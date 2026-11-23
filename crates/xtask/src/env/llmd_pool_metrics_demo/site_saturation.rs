//! Deployed proof that llm-d sites are read by their saturation and weighed by ready units.

#![expect(
    clippy::disallowed_methods,
    reason = "This synchronous Kind proof owns bounded polling and restores its run-owned mutations"
)]

use std::{
    process::Command,
    time::{Duration, Instant},
};

use super::{
    DemoContext, GRID_SERVING_GATEWAY, GRID_SERVING_METRICS_PORT, GRID_SERVING_SITE, GRID_SYSTEM_NS, MetricsTransport,
    ProofResult, VCR_MODEL,
    cross_site::{
        GatewayMetricsReader, RouteDistribution, RoutingAttributionDiagnostics, sample_value, start_ready_port_forward,
        verify_attributed_site_sample,
    },
    format_utc_iso, kind_context, kubectl_exec_epp_metrics, set_simulator_fake_metrics,
};

/// Both sites as the Gateway labels them; pool-B's EPP runs flow control and publishes its saturation.
const SITES: [(&str, &str); 2] = [("pool-a", "llmd-pool-a-provider"), ("pool-b", "llmd-pool-b-provider")];
/// The site whose EPP publishes `llm_d_epp_flow_control_pool_saturation`.
const PUBLISHING_SITE: &str = "pool-b";
/// Simulator endpoint scaled away to move pool-B's ready-unit weight.
const SHRINK_DEPLOYMENT: &str = "vcr-2";
/// KV share the llm-d utilization detector reads as a full unit by default.
const KV_FULL_SHARE: f64 = 0.8;
/// Waiting requests per unit the detector reads as a full unit by default.
const QUEUE_DEPTH: f64 = 5.0;
/// Agreement allowed between a Gateway gauge and the value computed from the EPP's own gauges.
const TOLERANCE: f64 = 0.02;
/// Poll interval for every wait in this proof.
const POLL: Duration = Duration::from_secs(2);
/// Deadline for any one transition, covering the Gateway's 30-second worst-in-window reading.
const TRANSITION_TIMEOUT: Duration = Duration::from_secs(180);
/// How long a KV-only site must stay not full: past the load window plus `full_after_ms`.
const NOT_FULL_SPAN: Duration = Duration::from_secs(45);
/// Requests in each bounded attribution sample for the signal-routing corroboration.
const SIGNAL_ROUTE_SAMPLE_REQUESTS: usize = 1024;
/// The same bounded request count for each ready-unit capacity-loss sample.
const CAPACITY_ROUTE_SAMPLE_REQUESTS: usize = SIGNAL_ROUTE_SAMPLE_REQUESTS;
/// A low-pressure baseline is expected to be an equal two-site share in this topology.
const BASELINE_SHARE: f64 = 0.5;
/// Allow about three binomial standard errors around the equal-share baseline and on recovery.
const ROUTE_SHARE_TOLERANCE: f64 = 0.05;
/// Require a pressured-but-not-full site to retain a meaningful positive share.
const MIN_PRESSURE_SITE_SHARE: f64 = 0.10;
/// Require a pressure reduction well above sampling noise for two 1,024-request samples.
const MIN_PRESSURE_SHARE_REDUCTION: f64 = 0.10;
/// Maximum rho for a recovered idle reading, matching the source-check tolerance.
const IDLE_RHO_MAXIMUM: f64 = TOLERANCE;
/// Per-endpoint fake metrics before capacity loss: two endpoints carry four running requests and 0.9 KV units.
const CAPACITY_BASE_LOAD: Stimulus = Stimulus {
    waiting: 0,
    running: 2,
    kv: 0.45,
};
/// After one endpoint is removed, its survivor carries the same aggregate load at rho 1.125.
const CAPACITY_REDUCED_LOAD: Stimulus = Stimulus {
    waiting: 0,
    running: 4,
    kv: 0.9,
};
/// Ready-unit weights for equal two-endpoint capacity before the loss and after restoration.
const CAPACITY_BEFORE_LOSS: CapacityPair = CapacityPair {
    baseline: 2.0,
    affected: 2.0,
    baseline_stimulus: CAPACITY_BASE_LOAD,
    affected_stimulus: CAPACITY_BASE_LOAD,
};
/// Ready-unit weights while pool-B has lost one of its two effective endpoints.
const CAPACITY_DURING_LOSS: CapacityPair = CapacityPair {
    baseline: 2.0,
    affected: 1.0,
    baseline_stimulus: CAPACITY_BASE_LOAD,
    affected_stimulus: CAPACITY_REDUCED_LOAD,
};

/// Ready-unit weights for unchanged pool-A and affected pool-B.
#[derive(Clone, Copy, Debug)]
struct CapacityPair {
    /// Pool-A stays at its original capacity throughout the scenario.
    baseline: f64,
    /// Pool-B's capacity is changed by scaling one of its two endpoints.
    affected: f64,
    /// Pool-A's stimulus, held fixed throughout the experiment.
    baseline_stimulus: Stimulus,
    /// Pool-B's stimulus, increased per remaining endpoint to keep aggregate work fixed.
    affected_stimulus: Stimulus,
}

/// The route metrics observed for one soft, non-full capacity-loss phase.
#[derive(Clone, Copy, Debug)]
struct CapacityRouteReadings {
    /// Grid Gateway weight for unchanged pool-A.
    baseline_weight: f64,
    /// Grid Gateway weight for pool-B, whose capacity is reduced and restored.
    affected_weight: f64,
    /// Soft pressure read for unchanged pool-A.
    baseline_rho: f64,
    /// Soft pressure read for pool-B, whose capacity is reduced and restored.
    affected_rho: f64,
}

impl CapacityRouteReadings {
    /// Whether weights and pressure remained stable across one bounded HTTP sample.
    fn stable_with(self, other: Self) -> bool {
        (self.baseline_weight - other.baseline_weight).abs() <= f64::EPSILON
            && (self.affected_weight - other.affected_weight).abs() <= f64::EPSILON
            && (self.baseline_rho - other.baseline_rho).abs() <= TOLERANCE
            && (self.affected_rho - other.affected_rho).abs() <= TOLERANCE
    }
}

/// One simulator stimulus applied to every run-owned endpoint of a pool.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Stimulus {
    /// Waiting requests per endpoint.
    waiting: u32,
    /// Running requests per endpoint.
    running: u32,
    /// KV-cache usage per endpoint.
    kv: f64,
}

/// Moderate load: both terms below full; the comparator reads 0.5.
const MODERATE: Stimulus = Stimulus {
    waiting: 1,
    running: 2,
    kv: 0.4,
};
/// A spent cache with nothing waiting: reads above 1, but is a site working flat out, not full.
const KV_ONLY: Stimulus = Stimulus {
    waiting: 0,
    running: 2,
    kv: 0.9,
};
/// A queue past the detector's depth: reads 1.2 with work waiting, so the site is full.
const BACKLOG: Stimulus = Stimulus {
    waiting: 6,
    running: 2,
    kv: 0.2,
};
/// The state the cross-site proof leaves behind and this proof restores.
const IDLE: Stimulus = Stimulus {
    waiting: 0,
    running: 0,
    kv: 0.0,
};

/// Run the saturation proof and restore simulator load and replica counts.
pub(super) fn proof(context: &DemoContext) -> ProofResult {
    let mut observations = vec![format!("[{}] site saturation proof started", format_utc_iso())];
    let mtls = context.metrics_transport == MetricsTransport::MtlsProxy;
    let original_replicas = deployment_replicas(PUBLISHING_SITE, SHRINK_DEPLOYMENT);

    let primary = match &original_replicas {
        Ok(replicas) => run(mtls, *replicas, &mut observations),
        Err(error) => Err(format!("could not read {SHRINK_DEPLOYMENT} replicas before mutation: {error}").into()),
    };
    if let Err(error) = &primary {
        observations.push(format!("[PROOF][FAIL] {error}"));
    }

    let mut restoration_errors = Vec::new();
    if let Ok(replicas) = original_replicas
        && let Err(error) = scale_deployment(PUBLISHING_SITE, SHRINK_DEPLOYMENT, replicas)
    {
        restoration_errors.push(format!("restore {SHRINK_DEPLOYMENT} replicas: {error}"));
    }
    for (site, _) in SITES {
        if let Err(error) = apply(site, IDLE, mtls) {
            restoration_errors.push(format!("restore idle simulator load on {site}: {error}"));
        }
    }
    for error in &restoration_errors {
        observations.push(format!("[RESTORE][FAIL] {error}"));
    }

    let success = primary.is_ok() && restoration_errors.is_empty();
    observations.push(if success {
        "site saturation proof passed; simulator load and replicas restored".to_owned()
    } else {
        "site saturation proof failed; see the failure and restoration lines above".to_owned()
    });
    ProofResult {
        success,
        description: "llm-d sites are weighed by ready units, read by EPP or Gateway saturation, full only with work waiting, and publish no learned ceiling; a synthetic KV-only rho>=1 signal reduces but does not zero its attributed route share and recovers after restoration".to_owned(),
        observations,
    }
}

/// Every proof step, in order; the caller restores state whatever this returns.
fn run(mtls: bool, original_replicas: u64, observations: &mut Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let local = kind_context(GRID_SERVING_SITE);
    let (gateway_http_port, _gateway_http_forward) =
        start_ready_port_forward(&local, GRID_SERVING_GATEWAY, super::GRID_SERVING_GATEWAY_PORT)?;
    let (port, _forward) = start_ready_port_forward(
        &local,
        &format!("{GRID_SERVING_GATEWAY}-metrics"),
        GRID_SERVING_METRICS_PORT,
    )?;
    let metrics = GatewayMetricsReader::for_context(&local, port)?;
    moderate_load(&metrics, mtls, observations)?;
    half_capacity_loss_reduces_share_and_recovers(gateway_http_port, &metrics, mtls, original_replicas, observations)?;
    spent_cache_is_not_full(&metrics, mtls, observations)?;
    saturated_not_full_site_keeps_traffic_and_recovers(gateway_http_port, &metrics, mtls, observations)?;
    backlog_is_full_then_recovers(&metrics, mtls, observations)
}

/// Steps 1 to 3 and 5: weight, source, comparator value and no learned ceiling under moderate load.
fn moderate_load(
    metrics: &GatewayMetricsReader,
    mtls: bool,
    observations: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    for (site, _) in SITES {
        apply(site, MODERATE, mtls)?;
    }
    for (site, cluster) in SITES {
        let reading = wait_for(&format!("{site} reads moderate load from its source"), || {
            let gateway = metrics.read()?;
            let epp = kubectl_exec_epp_metrics(site, mtls)?;
            check_source_and_value(&gateway, &epp, site, cluster)
        })?;
        observations.push(format!("[MODERATE] {site}: {reading}"));
    }
    Ok(())
}

/// Step 1: pool-B's weight follows its ready units down and back.
fn half_capacity_loss_reduces_share_and_recovers(
    gateway_http_port: u16,
    metrics: &GatewayMetricsReader,
    mtls: bool,
    original_replicas: u64,
    observations: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if original_replicas != 1 {
        return Err(format!(
            "the half-capacity topology requires {SHRINK_DEPLOYMENT} on {PUBLISHING_SITE} to start with one replica, got {original_replicas}"
        )
        .into());
    }
    if !capacity_loss_holds_aggregate_work_constant() {
        return Err("capacity-loss stimuli do not preserve pool-B aggregate running and KV load".into());
    }
    for (site, _) in SITES {
        apply(site, CAPACITY_BASE_LOAD, mtls)?;
    }
    let before = wait_for("both sites publish the fixed soft-load capacity baseline", || {
        read_capacity_route_state(metrics, mtls, CAPACITY_BEFORE_LOSS)
    })?;
    if (before.baseline_weight - CAPACITY_BEFORE_LOSS.baseline).abs() > f64::EPSILON
        || (before.affected_weight - CAPACITY_BEFORE_LOSS.affected).abs() > f64::EPSILON
    {
        return Err(format!("capacity-loss baseline is not two ready units per site: {before:?}").into());
    }
    let (baseline_share, baseline_distribution) = verify_capacity_route_sample(
        gateway_http_port,
        metrics,
        mtls,
        "half-capacity-baseline",
        CAPACITY_BEFORE_LOSS,
    )?;

    scale_deployment(PUBLISHING_SITE, SHRINK_DEPLOYMENT, 0)?;
    let during_result = (|| {
        apply(PUBLISHING_SITE, CAPACITY_REDUCED_LOAD, mtls)?;
        let during = wait_for(
            "pool-b loses exactly half its ready-unit weight while pool-a is fixed",
            || read_capacity_route_state(metrics, mtls, CAPACITY_DURING_LOSS),
        )?;
        let (share, distribution) = verify_capacity_route_sample(
            gateway_http_port,
            metrics,
            mtls,
            "pool-b-half-capacity-loss",
            CAPACITY_DURING_LOSS,
        )?;
        Ok::<_, Box<dyn std::error::Error>>((during, share, distribution))
    })();

    // Restore the mutation even when the pressure or request assertions fail.
    let restore_capacity = scale_deployment(PUBLISHING_SITE, SHRINK_DEPLOYMENT, original_replicas);
    let restore_load = apply(PUBLISHING_SITE, CAPACITY_BASE_LOAD, mtls);
    require_restorations(
        during_result.as_ref().err().map(ToString::to_string),
        restore_capacity.map_err(|error| error.to_string()),
        restore_load.map_err(|error| error.to_string()),
    )?;
    let recovery_result = (|| {
        let recovered = wait_for("pool-b ready-unit capacity returns while pool-a stays fixed", || {
            read_capacity_route_state(metrics, mtls, CAPACITY_BEFORE_LOSS)
        })?;
        let (share, distribution) = verify_capacity_route_sample(
            gateway_http_port,
            metrics,
            mtls,
            "pool-b-half-capacity-recovery",
            CAPACITY_BEFORE_LOSS,
        )?;
        Ok::<_, Box<dyn std::error::Error>>((recovered, share, distribution))
    })();

    let (during, during_share, during_distribution) = during_result?;
    let (recovered, recovered_share, recovery_distribution) = recovery_result?;
    let expected_during_share = predicted_pool_b_capacity_share(&during);
    let gate =
        capacity_loss_share_transition_gate(baseline_share, during_share, recovered_share, expected_during_share)?;
    observations.push(format!(
        "[CAPACITY-LOSS] fixed aggregate simulator load: pool-a stayed at running=2/KV=0.45 on two endpoints; pool-b aggregate running=4/KV=0.9 across two endpoints before and one after loss; waiting=0 throughout; exactly {CAPACITY_ROUTE_SAMPLE_REQUESTS} requests per phase; pool-a weight stayed 2, pool-b weight {} -> {} -> {}; pool-b rho {:.3} -> {:.3} -> {:.3}; share {:.3} -> {:.3} -> {:.3}; full=0 throughout and neither site had waiting/held requests; baseline {baseline_distribution}; half-capacity {during_distribution}; recovery {recovery_distribution}; {gate}",
        before.affected_weight,
        during.affected_weight,
        recovered.affected_weight,
        before.affected_rho,
        during.affected_rho,
        recovered.affected_rho,
        baseline_share,
        during_share,
        recovered_share
    ));
    Ok(())
}

/// Preserve primary and restoration errors before any recovery request is sent.
fn require_restorations(
    primary: Option<String>,
    capacity: Result<(), String>,
    load: Result<(), String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut errors: Vec<String> = primary.into_iter().collect();
    if let Err(error) = capacity {
        errors.push(format!("capacity restore: {error}"));
    }
    if let Err(error) = load {
        errors.push(format!("load restore: {error}"));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; ").into())
    }
}

/// Read the Gateway and EPP state required for a fixed-load, non-backlogged capacity sample.
fn read_capacity_route_state(
    metrics: &GatewayMetricsReader,
    mtls: bool,
    expected_units: CapacityPair,
) -> Result<CapacityRouteReadings, Box<dyn std::error::Error>> {
    let gateway = metrics.read()?;
    let baseline_epp = kubectl_exec_epp_metrics("pool-a", mtls)?;
    let affected_epp = kubectl_exec_epp_metrics("pool-b", mtls)?;
    check_source_and_value(&gateway, &baseline_epp, "pool-a", "llmd-pool-a-provider")?;
    check_source_and_value(&gateway, &affected_epp, "pool-b", "llmd-pool-b-provider")?;
    check_capacity_soft_load(&baseline_epp, expected_units.baseline_stimulus, "pool-a")?;
    check_capacity_soft_load(&affected_epp, expected_units.affected_stimulus, "pool-b")?;
    let baseline_units = epp_value(&baseline_epp, "llm_d_epp_ready_endpoints")?;
    let affected_units = epp_value(&affected_epp, "llm_d_epp_ready_endpoints")?;
    if (baseline_units - expected_units.baseline).abs() > f64::EPSILON
        || (affected_units - expected_units.affected).abs() > f64::EPSILON
    {
        return Err(format!(
            "capacity-loss ready units differ from the controlled topology: unchanged pool-a={baseline_units}, affected pool-b={affected_units}; expected {}/{}",
            expected_units.baseline, expected_units.affected
        )
        .into());
    }
    capacity_route_readings(&gateway, expected_units)
}

/// Require the fixed capacity experiment to remain soft, fresh, and unshed.
fn capacity_route_readings(
    gateway: &str,
    expected_units: CapacityPair,
) -> Result<CapacityRouteReadings, Box<dyn std::error::Error>> {
    let baseline_weight = gauge(gateway, "grid_route_site_weight", "pool-a", "llmd-pool-a-provider")
        .ok_or("no finite pool-a capacity weight")?;
    let affected_weight = gauge(gateway, "grid_route_site_weight", "pool-b", "llmd-pool-b-provider")
        .ok_or("no finite pool-b capacity weight")?;
    let baseline_rho = gauge(gateway, "grid_route_site_rho", "pool-a", "llmd-pool-a-provider")
        .ok_or("no finite pool-a pressure reading")?;
    let affected_rho = gauge(gateway, "grid_route_site_rho", "pool-b", "llmd-pool-b-provider")
        .ok_or("no finite pool-b pressure reading")?;
    for (site, cluster, weight, expected_weight) in [
        (
            "pool-a",
            "llmd-pool-a-provider",
            baseline_weight,
            expected_units.baseline,
        ),
        (
            "pool-b",
            "llmd-pool-b-provider",
            affected_weight,
            expected_units.affected,
        ),
    ] {
        if (weight - expected_weight).abs() > f64::EPSILON {
            return Err(format!("{site} Gateway weight {weight}, expected {expected_weight} ready endpoints").into());
        }
        if !matches!(gauge(gateway, "grid_route_site_full", site, cluster), Some(full) if full.abs() < f64::EPSILON) {
            return Err(format!("{site} is full during the capacity-loss sample").into());
        }
        if !matches!(gauge(gateway, "grid_route_site_reading_age_seconds", site, cluster), Some(age) if age <= MAX_READING_AGE_SECONDS)
        {
            return Err(format!("{site} pressure reading is stale during the capacity-loss sample").into());
        }
    }
    let reduced_capacity = expected_units.affected < CAPACITY_BEFORE_LOSS.affected;
    if baseline_rho >= 1.0 || (reduced_capacity && affected_rho < 1.0) || (!reduced_capacity && affected_rho >= 1.0) {
        return Err(format!(
            "capacity-loss pressure is on the wrong side of the soft non-backlogged state: pool-a rho={baseline_rho}, pool-b rho={affected_rho}, reduced_capacity={reduced_capacity}"
        )
        .into());
    }
    if let Some(shed) = sample_value(gateway, "grid_route_shedding", &[("model", VCR_MODEL)])
        && (!shed.is_finite() || shed >= 1.0)
    {
        return Err(
            format!("the model's shedding gauge is invalid or active during the capacity-loss sample: {shed}").into(),
        );
    }
    Ok(CapacityRouteReadings {
        baseline_weight,
        affected_weight,
        baseline_rho,
        affected_rho,
    })
}

/// Verify the zero-queue synthetic state independently of the route's observed metric source.
fn check_capacity_soft_load(epp: &str, stimulus: Stimulus, site: &str) -> Result<(), Box<dyn std::error::Error>> {
    check_epp_stimulus(epp, stimulus, site)?;
    let running = epp_value(epp, "llm_d_epp_average_running_requests")?;
    if (running - f64::from(stimulus.running)).abs() > TOLERANCE {
        return Err(format!(
            "EPP running load for {site} changed during the capacity-loss scenario: {running}, expected {}",
            stimulus.running
        )
        .into());
    }
    if stimulus.waiting == 0 {
        let held = sample_value(epp, "llm_d_epp_flow_control_queue_size", &[]).unwrap_or(0.0);
        if !held.is_finite() || held.abs() > TOLERANCE {
            return Err(
                format!("EPP reports held requests for {site} during the non-backlogged sample: {held}").into(),
            );
        }
    }
    Ok(())
}

/// Sample the actual two-site selector under a fixed synthetic load and require matching attribution.
fn verify_capacity_route_sample(
    gateway_http_port: u16,
    metrics: &GatewayMetricsReader,
    mtls: bool,
    phase: &str,
    expected_units: CapacityPair,
) -> Result<(f64, String), Box<dyn std::error::Error>> {
    let before = read_capacity_route_state(metrics, mtls, expected_units)?;
    let mut attribution = RoutingAttributionDiagnostics::default();
    let (distribution, selector_evidence) = verify_attributed_site_sample(
        gateway_http_port,
        metrics,
        phase,
        CAPACITY_ROUTE_SAMPLE_REQUESTS,
        128,
        &mut attribution,
        |before_metrics, after_metrics, batch, requests| {
            let before_window =
                capacity_route_readings(before_metrics, expected_units).map_err(|error| error.to_string())?;
            let after_window =
                capacity_route_readings(after_metrics, expected_units).map_err(|error| error.to_string())?;
            if before_window.stable_with(after_window) && before.stable_with(before_window) {
                capacity_interval_gate(batch, requests, predicted_pool_b_capacity_share(&before_window))
            } else {
                Err(format!(
                    "capacity or soft-pressure state changed during {phase}: initial={before:?}, before={before_window:?}, after={after_window:?}"
                ))
            }
        },
    )?;
    let after = read_capacity_route_state(metrics, mtls, expected_units)?;
    if !before.stable_with(after) {
        return Err(format!(
            "capacity or soft-pressure state changed across {phase}: before={before:?}, after={after:?}"
        )
        .into());
    }
    if let Some(error) = attribution.contract_error() {
        return Err(format!("{phase} response attribution contract failed: {error}").into());
    }
    let total = distribution.sites.values().sum::<usize>();
    let only_expected_sites = distribution
        .sites
        .keys()
        .all(|site| matches!(site.as_str(), "pool-a" | "pool-b"));
    if total != CAPACITY_ROUTE_SAMPLE_REQUESTS
        || distribution.provider_gateways.values().sum::<usize>() != total
        || distribution.backends.values().sum::<usize>() != total
        || !distribution.has_consistent_backends()
        || !only_expected_sites
        || distribution.sites.get("pool-a").copied().unwrap_or_default() == 0
        || distribution.sites.get("pool-b").copied().unwrap_or_default() == 0
    {
        return Err(format!(
            "{phase} did not retain complete per-response provider/backend attribution to both sites: {distribution}"
        )
        .into());
    }
    let pool_b_share = site_share(&distribution, "pool-b", total)?;
    let predicted = predicted_pool_b_capacity_share(&before);
    if (pool_b_share - predicted).abs() > ROUTE_SHARE_TOLERANCE {
        return Err(format!(
            "{phase} pool-b share {pool_b_share:.3} differs from its capacity-and-pressure share {predicted:.3} by more than {ROUTE_SHARE_TOLERANCE:.2}; {selector_evidence}; {distribution}"
        )
        .into());
    }
    Ok((
        pool_b_share,
        format!(
            "pool-b predicted share={predicted:.3}, observed={pool_b_share:.3} (±{ROUTE_SHARE_TOLERANCE:.2}); state={before:?}; {selector_evidence}; per-response attribution={distribution}"
        ),
    ))
}

/// Require a positive, non-full share in every observed 128-request interval.
/// The 0.20 interval tolerance is deliberately wider than the 1,024-request aggregate
/// gate; at this fixture's probabilities it exceeds four standard deviations.
fn capacity_interval_gate(distribution: &RouteDistribution, requests: usize, predicted: f64) -> Result<(), String> {
    let share = site_share(distribution, "pool-b", requests).map_err(|error| error.to_string())?;
    if requests < 128
        || !predicted.is_finite()
        || !(0.0..1.0).contains(&share)
        || share == 0.0
        || (share - predicted).abs() > 0.20
    {
        return Err(format!(
            "observed capacity interval lost its expected positive share: requests={requests}, pool-b share={share:.3}, predicted={predicted:.3}; {distribution}"
        ));
    }
    Ok(())
}

/// Pool-B's two-site pick probability from ready units and the same positive soft-pressure preference.
fn predicted_pool_b_capacity_share(readings: &CapacityRouteReadings) -> f64 {
    let preference = |rho: f64| 1.0 / (1.0 + rho);
    let baseline_score = readings.baseline_weight * preference(readings.baseline_rho);
    let affected_score = readings.affected_weight * preference(readings.affected_rho);
    (affected_score / (baseline_score + affected_score)).clamp(0.1, 0.9)
}

/// Whether the controlled ready-unit loss keeps pool-B's aggregate synthetic load unchanged.
fn capacity_loss_holds_aggregate_work_constant() -> bool {
    let aggregate = |units: f64, stimulus: Stimulus| (units * f64::from(stimulus.running), units * stimulus.kv);
    CAPACITY_BEFORE_LOSS.baseline_stimulus == CAPACITY_DURING_LOSS.baseline_stimulus
        && aggregate(CAPACITY_BEFORE_LOSS.affected, CAPACITY_BEFORE_LOSS.affected_stimulus)
            == aggregate(CAPACITY_DURING_LOSS.affected, CAPACITY_DURING_LOSS.affected_stimulus)
        && CAPACITY_BEFORE_LOSS.affected_stimulus.waiting == 0
        && CAPACITY_DURING_LOSS.affected_stimulus.waiting == 0
}

/// Require the affected site's share to fall to the half-capacity expectation and recover.
fn capacity_loss_share_transition_gate(
    baseline: f64,
    during_loss: f64,
    recovered: f64,
    expected_reduced_share: f64,
) -> Result<String, Box<dyn std::error::Error>> {
    if !expected_reduced_share.is_finite() || !(0.0..=1.0).contains(&expected_reduced_share) {
        return Err(format!("predicted half-capacity share is outside [0,1]: {expected_reduced_share}").into());
    }
    for (phase, share) in [
        ("baseline", baseline),
        ("half-capacity", during_loss),
        ("recovery", recovered),
    ] {
        if !share.is_finite() || !(0.0..=1.0).contains(&share) {
            return Err(format!("{phase} pool-b share is outside [0,1]: {share}").into());
        }
    }
    if (baseline - BASELINE_SHARE).abs() > ROUTE_SHARE_TOLERANCE {
        return Err(format!("baseline pool-b share {baseline:.3} is not near 0.500").into());
    }
    if !(0.10..0.90).contains(&during_loss) {
        return Err(format!(
            "pool-b's half-capacity share {during_loss:.3} is not positive and bounded away from the full-share-to-zero cliff"
        )
        .into());
    }
    if (during_loss - expected_reduced_share).abs() > ROUTE_SHARE_TOLERANCE {
        return Err(format!(
            "half-capacity pool-b share {during_loss:.3} differs from the capacity-and-pressure prediction {expected_reduced_share:.3} by more than {ROUTE_SHARE_TOLERANCE:.2}"
        )
        .into());
    }
    if baseline - during_loss < 0.10 {
        return Err(format!(
            "pool-b share did not fall meaningfully after losing half its capacity: {baseline:.3} -> {during_loss:.3}"
        )
        .into());
    }
    if (recovered - BASELINE_SHARE).abs() > ROUTE_SHARE_TOLERANCE
        || (recovered - baseline).abs() > ROUTE_SHARE_TOLERANCE
    {
        return Err(format!(
            "pool-b share did not recover after capacity restoration: baseline={baseline:.3}, recovered={recovered:.3}"
        )
        .into());
    }
    Ok(format!(
        "capacity-loss gate passed: baseline={baseline:.3}, half-capacity={during_loss:.3}, recovered={recovered:.3}; expected baseline=0.500, half-capacity={expected_reduced_share:.3}, recovery=0.500 within ±{ROUTE_SHARE_TOLERANCE:.2}, all phases positive"
    ))
}

/// Step 4a: a spent cache with nothing waiting reads above 1 but is never full and never sheds.
fn spent_cache_is_not_full(
    metrics: &GatewayMetricsReader,
    mtls: bool,
    observations: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    for (site, _) in SITES {
        apply(site, KV_ONLY, mtls)?;
    }
    for (site, cluster) in SITES {
        wait_for(&format!("{site} reads its spent cache above 1"), || {
            let gateway = metrics.read()?;
            let rho = gauge(&gateway, "grid_route_site_rho", site, cluster).ok_or("no grid_route_site_rho")?;
            if rho >= 1.0 {
                Ok(rho)
            } else {
                Err(format!("rho {rho} below 1").into())
            }
        })?;
    }
    hold_not_full(metrics)?;
    observations.push(format!(
        "[KV-ONLY] both sites read rho >= 1 with no queue and stayed not full and unshed for {}s",
        NOT_FULL_SPAN.as_secs()
    ));
    Ok(())
}

/// Prove live `grid_site_route` keeps a soft-saturated site selectable, then restores its baseline share.
fn saturated_not_full_site_keeps_traffic_and_recovers(
    gateway_port: u16,
    metrics: &GatewayMetricsReader,
    mtls: bool,
    observations: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    for (site, _) in SITES {
        apply(site, IDLE, mtls)?;
    }
    let (baseline_pool_b_share, baseline) = verify_signal_route_sample(&SignalRouteSample {
        gateway_port,
        metrics,
        mtls,
        expected_pressure: RoutePressure::Idle,
        expected_site: "pool-b",
        phase: "saturation-baseline",
    })?;
    observations.push(format!(
        "[SYNTHETIC-SIGNAL][BASELINE] low-pressure pool-b share={baseline_pool_b_share:.3}; {baseline}"
    ));

    let pressure_result = (|| {
        apply("pool-b", KV_ONLY, mtls)?;
        let (pressure_pool_a_share, pressure) = verify_signal_route_sample(&SignalRouteSample {
            gateway_port,
            metrics,
            mtls,
            expected_pressure: RoutePressure::PoolBSoftSaturated,
            expected_site: "pool-a",
            phase: "synthetic-kv-pressure",
        })?;
        Ok::<_, Box<dyn std::error::Error>>((pressure_pool_a_share, pressure))
    })();

    let mut restore_errors = Vec::new();
    for (site, _) in SITES {
        if let Err(error) = apply(site, IDLE, mtls) {
            restore_errors.push(format!("restore synthetic signal on {site}: {error}"));
        }
    }
    let recovery_result = if restore_errors.is_empty() {
        verify_signal_route_sample(&SignalRouteSample {
            gateway_port,
            metrics,
            mtls,
            expected_pressure: RoutePressure::Idle,
            expected_site: "pool-b",
            phase: "synthetic-kv-pressure-recovery",
        })
    } else {
        Err(format!(
            "simulator pressure restoration failed; recovery traffic was not run: {}",
            restore_errors.join("; ")
        )
        .into())
    };

    let mut errors = restore_errors;
    let pressure_observation = match pressure_result {
        Ok(value) => Some(value),
        Err(error) => {
            errors.push(format!("soft-saturation request sample failed: {error}"));
            None
        },
    };
    let recovery_observation = match recovery_result {
        Ok(value) => Some(value),
        Err(error) => {
            errors.push(format!("route recovery sample failed: {error}"));
            None
        },
    };
    if let (Some((pressure_pool_a_share, pressure)), Some((recovered_pool_b_share, recovered))) =
        (pressure_observation, recovery_observation)
    {
        let pressured_share = 1.0 - pressure_pool_a_share;
        match route_share_transition_gate(baseline_pool_b_share, pressured_share, recovered_pool_b_share) {
            Ok(gate) => {
                observations.push(format!(
                    "[SYNTHETIC-SIGNAL][RHO>=1] pool-b share={pressured_share:.3}; {pressure}"
                ));
                observations.push(format!(
                    "[SYNTHETIC-SIGNAL][RECOVERY] pool-b share={recovered_pool_b_share:.3}; {recovered}; {gate}"
                ));
            },
            Err(error) => errors.push(error.to_string()),
        }
    }
    if !errors.is_empty() {
        return Err(errors.join("; ").into());
    }
    Ok(())
}

/// Whether the live Gateway should be reading its low-pressure or pool-B saturated signal.
#[derive(Clone, Copy, Debug)]
enum RoutePressure {
    /// Both pools are idle and should be equally weighted.
    Idle,
    /// Pool-B reports rho at or above one but has no waiting request and is not full.
    PoolBSoftSaturated,
}

/// Rho readings held stable across a bounded request sample.
#[derive(Clone, Copy, Debug)]
struct RouteReadings {
    /// Pool-A's measured saturation.
    a_rho: f64,
    /// Pool-B's measured saturation.
    b_rho: f64,
    /// Pool-A's ready-unit weight at this checkpoint.
    a_weight: f64,
    /// Pool-B's ready-unit weight at this checkpoint.
    b_weight: f64,
}

/// Wait for fresh, stable Gateway readings and verify them against each EPP's live scrape.
fn wait_for_route_state(
    metrics: &GatewayMetricsReader,
    mtls: bool,
    expected: RoutePressure,
) -> Result<(RouteReadings, String), Box<dyn std::error::Error>> {
    wait_for(&format!("live Gateway reaches route state {expected:?}"), || {
        let gateway = metrics.read()?;
        let readings = readings_in_state(&gateway, expected)?;
        let a_epp = kubectl_exec_epp_metrics("pool-a", mtls)?;
        let b_epp = kubectl_exec_epp_metrics("pool-b", mtls)?;
        let a_source = check_source_and_value(&gateway, &a_epp, "pool-a", "llmd-pool-a-provider")?;
        let b_source = check_source_and_value(&gateway, &b_epp, "pool-b", "llmd-pool-b-provider")?;
        let (expected_a, expected_b) = match expected {
            RoutePressure::Idle => (IDLE, IDLE),
            RoutePressure::PoolBSoftSaturated => (IDLE, KV_ONLY),
        };
        let a_signal = check_epp_stimulus(&a_epp, expected_a, "pool-a")?;
        let b_signal = check_epp_stimulus(&b_epp, expected_b, "pool-b")?;
        let a_weight = gauge(&gateway, "grid_route_site_weight", "pool-a", "llmd-pool-a-provider")
            .ok_or("no finite pool-a weight")?;
        let b_weight = gauge(&gateway, "grid_route_site_weight", "pool-b", "llmd-pool-b-provider")
            .ok_or("no finite pool-b weight")?;
        if (a_weight - b_weight).abs() > f64::EPSILON {
            return Err(format!(
                "signal route proof requires equal ready-unit weights, got pool-a={a_weight}, pool-b={b_weight}"
            )
            .into());
        }
        Ok((
            readings,
            format!(
                "Gateway rho=({:.3}, {:.3}), full=0; {a_source}; {b_source}; {a_signal}; {b_signal}",
                readings.a_rho, readings.b_rho
            ),
        ))
    })
}

/// Verify that the EPP reports the synthetic runtime values applied to that pool's simulator.
fn check_epp_stimulus(epp: &str, stimulus: Stimulus, site: &str) -> Result<String, Box<dyn std::error::Error>> {
    let queue = epp_value(epp, "llm_d_epp_average_queue_size")?;
    let kv = epp_value(epp, "llm_d_epp_average_kv_cache_utilization")?;
    if (queue - f64::from(stimulus.waiting)).abs() > TOLERANCE || (kv - stimulus.kv).abs() > TOLERANCE {
        return Err(format!(
            "EPP signal for {site} does not match the applied synthetic simulator values: queue={queue}, expected={}, KV={kv}, expected={}",
            stimulus.waiting, stimulus.kv
        )
        .into());
    }
    Ok(format!("{site} EPP queue={queue:.3}, KV={kv:.3}"))
}

/// Whether successive rho readings describe the same pressure sample window.
fn route_readings_stable(left: &RouteReadings, right: &RouteReadings) -> bool {
    (left.a_rho - right.a_rho).abs() <= TOLERANCE
        && (left.b_rho - right.b_rho).abs() <= TOLERANCE
        && (left.a_weight - right.a_weight).abs() <= f64::EPSILON
        && (left.b_weight - right.b_weight).abs() <= f64::EPSILON
}

/// Input required to sample `grid_site_route` while checking live signals.
struct SignalRouteSample<'phase> {
    /// Forwarded HTTP listener for the run-owned Grid Gateway.
    gateway_port: u16,
    /// Forwarded metrics listener for that Gateway.
    metrics: &'phase GatewayMetricsReader,
    /// Whether EPP metrics are read through the run-owned mTLS proxy.
    mtls: bool,
    /// Signal state that must remain stable for the request window.
    expected_pressure: RoutePressure,
    /// Site whose share this request sample reports.
    expected_site: &'phase str,
    /// Evidence label for this routing phase.
    phase: &'phase str,
}

/// Sample `grid_site_route` while checking live signals; return the share for `expected_site`.
fn verify_signal_route_sample(sample: &SignalRouteSample<'_>) -> Result<(f64, String), Box<dyn std::error::Error>> {
    let gateway_port = sample.gateway_port;
    let metrics = sample.metrics;
    let mtls = sample.mtls;
    let expected_pressure = sample.expected_pressure;
    let expected_site = sample.expected_site;
    let phase = sample.phase;
    let (sample_state, signal_evidence) = wait_for_route_state(metrics, mtls, expected_pressure)?;
    let mut attribution = RoutingAttributionDiagnostics::default();
    let (distribution, selector_evidence) = verify_attributed_site_sample(
        gateway_port,
        metrics,
        phase,
        SIGNAL_ROUTE_SAMPLE_REQUESTS,
        32,
        &mut attribution,
        |before_metrics, after_metrics, _batch, _requests| {
            let before = readings_in_state(before_metrics, expected_pressure).map_err(|error| error.to_string())?;
            let after = readings_in_state(after_metrics, expected_pressure).map_err(|error| error.to_string())?;
            if route_readings_stable(&sample_state, &before) && route_readings_stable(&sample_state, &after) {
                Ok(())
            } else {
                Err(format!(
                    "signal changed during a bounded request batch: sample={sample_state:?}, before={before:?}, after={after:?}"
                ))
            }
        },
    )?;
    let (after_sample_state, after_signal_evidence) = wait_for_route_state(metrics, mtls, expected_pressure)?;
    if !route_readings_stable(&sample_state, &after_sample_state) {
        return Err(format!(
            "{phase} crossed Gateway signal windows: before={sample_state:?}, after={after_sample_state:?}"
        )
        .into());
    }
    if let Some(error) = attribution.contract_error() {
        return Err(format!("{phase} response attribution contract failed: {error}").into());
    }
    let total = distribution.sites.values().sum::<usize>();
    let known_sites = |site: &String| matches!(site.as_str(), "pool-a" | "pool-b");
    if total != SIGNAL_ROUTE_SAMPLE_REQUESTS
        || distribution.provider_gateways.values().sum::<usize>() != SIGNAL_ROUTE_SAMPLE_REQUESTS
        || distribution.backends.values().sum::<usize>() != SIGNAL_ROUTE_SAMPLE_REQUESTS
        || !distribution.has_consistent_backends()
        || !distribution.sites.keys().all(known_sites)
        || !distribution.provider_gateways.keys().all(known_sites)
    {
        return Err(format!(
            "{phase} had incomplete or unexpected provider/backend attribution: expected {SIGNAL_ROUTE_SAMPLE_REQUESTS} responses from pool-a/pool-b, got {distribution}"
        )
        .into());
    }
    let expected_site_share = site_share(&distribution, expected_site, total)?;
    let observed_pool_b_share = if expected_site == "pool-b" {
        expected_site_share
    } else {
        1.0 - expected_site_share
    };
    let predicted_pool_b_share = predicted_pool_b_share(&sample_state);
    if (observed_pool_b_share - predicted_pool_b_share).abs() > ROUTE_SHARE_TOLERANCE {
        return Err(format!(
            "{phase} pool-b route share {observed_pool_b_share:.3} differs from the signal-derived selector share {predicted_pool_b_share:.3} by more than {ROUTE_SHARE_TOLERANCE:.2}; {signal_evidence}; {selector_evidence}; {distribution}"
        )
        .into());
    }
    Ok((
        expected_site_share,
        format!(
            "predicted pool-b share={predicted_pool_b_share:.3}, observed={observed_pool_b_share:.3} (±{ROUTE_SHARE_TOLERANCE:.2}); {signal_evidence}; {selector_evidence}; {after_signal_evidence}; provider/backend attribution={distribution}"
        ),
    ))
}

/// Compute the two-site share used by `grid_site_route` when both sites have equal capacity.
fn predicted_pool_b_share(readings: &RouteReadings) -> f64 {
    let preference = |rho: f64| 1.0 / (1.0 + rho);
    (preference(readings.b_rho) / (preference(readings.a_rho) + preference(readings.b_rho))).clamp(0.1, 0.9)
}

/// Calculate one site's share from a complete attributed sample.
fn site_share(
    distribution: &RouteDistribution,
    site: &str,
    expected_requests: usize,
) -> Result<f64, Box<dyn std::error::Error>> {
    if !matches!(site, "pool-a" | "pool-b") {
        return Err(format!("unexpected expected site in signal sample: {site}").into());
    }
    let total = distribution.sites.values().sum::<usize>();
    if total != expected_requests || total == 0 {
        return Err(
            format!("cannot calculate {site} share from {total} responses; expected {expected_requests}").into(),
        );
    }
    let count = distribution.sites.get(site).copied().unwrap_or_default();
    Ok(
        f64::from(u32::try_from(count).map_err(|error| format!("site route count overflowed: {error}"))?)
            / f64::from(u32::try_from(total).map_err(|error| format!("route sample count overflowed: {error}"))?),
    )
}

/// Require a bounded signal reduction at rho >= 1 and return to the idle baseline after restoration.
fn route_share_transition_gate(
    baseline: f64,
    pressured: f64,
    recovered: f64,
) -> Result<String, Box<dyn std::error::Error>> {
    if !baseline.is_finite()
        || !pressured.is_finite()
        || !recovered.is_finite()
        || !(0.0..=1.0).contains(&baseline)
        || !(0.0..=1.0).contains(&pressured)
        || !(0.0..=1.0).contains(&recovered)
    {
        return Err(format!(
            "route shares must be finite and in [0, 1]: baseline={baseline}, pressure={pressured}, recovery={recovered}"
        )
        .into());
    }
    if (baseline - BASELINE_SHARE).abs() > ROUTE_SHARE_TOLERANCE {
        return Err(format!(
            "idle baseline pool-b share {baseline:.3} is not near the equal-weight expectation {BASELINE_SHARE:.3} (±{ROUTE_SHARE_TOLERANCE:.2})"
        )
        .into());
    }
    if pressured < MIN_PRESSURE_SITE_SHARE {
        return Err(format!(
            "pool-b received only {pressured:.3} of requests at soft saturation; minimum positive share is {MIN_PRESSURE_SITE_SHARE:.3}"
        )
        .into());
    }
    if pressured > baseline - MIN_PRESSURE_SHARE_REDUCTION {
        return Err(format!(
            "synthetic rho>=1 pressure did not reduce pool-b share enough: baseline={baseline:.3}, pressure={pressured:.3}, required reduction={MIN_PRESSURE_SHARE_REDUCTION:.3}"
        )
        .into());
    }
    if (recovered - baseline).abs() > ROUTE_SHARE_TOLERANCE {
        return Err(format!(
            "pool-b route share did not recover after simulator metrics returned to idle: baseline={baseline:.3}, recovered={recovered:.3}, tolerance={ROUTE_SHARE_TOLERANCE:.2}"
        )
        .into());
    }
    Ok(format!(
        "pool-b baseline={baseline:.3}, rho>=1 share={pressured:.3}, recovered={recovered:.3}; minimum positive share={MIN_PRESSURE_SITE_SHARE:.2}, required pressure reduction={MIN_PRESSURE_SHARE_REDUCTION:.2}, recovery tolerance=±{ROUTE_SHARE_TOLERANCE:.2}"
    ))
}

/// Read route metrics and verify the phase predicate between request batches.
fn readings_in_state(metrics: &str, expected: RoutePressure) -> Result<RouteReadings, Box<dyn std::error::Error>> {
    let readings = RouteReadings {
        a_rho: gauge(metrics, "grid_route_site_rho", "pool-a", "llmd-pool-a-provider")
            .ok_or("no finite pool-a rho during request sample")?,
        b_rho: gauge(metrics, "grid_route_site_rho", "pool-b", "llmd-pool-b-provider")
            .ok_or("no finite pool-b rho during request sample")?,
        a_weight: gauge(metrics, "grid_route_site_weight", "pool-a", "llmd-pool-a-provider")
            .filter(|weight| *weight > 0.0)
            .ok_or("no positive pool-a weight")?,
        b_weight: gauge(metrics, "grid_route_site_weight", "pool-b", "llmd-pool-b-provider")
            .filter(|weight| *weight > 0.0)
            .ok_or("no positive pool-b weight")?,
    };
    let expected_rhos = match expected {
        RoutePressure::Idle => readings.a_rho <= IDLE_RHO_MAXIMUM && readings.b_rho <= IDLE_RHO_MAXIMUM,
        RoutePressure::PoolBSoftSaturated => readings.a_rho <= IDLE_RHO_MAXIMUM && readings.b_rho >= 1.0,
    };
    let both_not_full = SITES.iter().all(
        |(site, cluster)| matches!(gauge(metrics, "grid_route_site_full", site, cluster), Some(full) if full.abs() < f64::EPSILON),
    );
    let both_fresh = SITES.iter().all(|(site, cluster)| {
        matches!(gauge(metrics, "grid_route_site_reading_age_seconds", site, cluster), Some(age) if age <= MAX_READING_AGE_SECONDS)
    });
    let not_shedding =
        sample_value(metrics, "grid_route_shedding", &[("model", VCR_MODEL)]).is_none_or(|shed| shed < 1.0);
    if !expected_rhos || !both_not_full || !both_fresh || !not_shedding {
        return Err(format!(
            "Grid route metric window changed during request sample: expected={expected:?}, readings={readings:?}, both_not_full={both_not_full}, both_fresh={both_fresh}, not_shedding={not_shedding}"
        )
        .into());
    }
    Ok(readings)
}

/// Step 4b: a queue past the detector depth with work waiting is full, then recovers when it drains.
fn backlog_is_full_then_recovers(
    metrics: &GatewayMetricsReader,
    mtls: bool,
    observations: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    for (site, _) in SITES {
        apply(site, BACKLOG, mtls)?;
    }
    for (site, cluster) in SITES {
        wait_for(&format!("{site} is full with work waiting"), || {
            let gateway = metrics.read()?;
            match gauge(&gateway, "grid_route_site_full", site, cluster) {
                Some(full) if full >= 1.0 => Ok(()),
                other => Err(format!("grid_route_site_full {other:?}, expected 1").into()),
            }
        })?;
    }
    for (site, _) in SITES {
        apply(site, IDLE, mtls)?;
    }
    for (site, cluster) in SITES {
        wait_for(&format!("{site} recovers once its queue drains"), || {
            let gateway = metrics.read()?;
            match gauge(&gateway, "grid_route_site_full", site, cluster) {
                Some(full) if full.abs() < f64::EPSILON => Ok(()),
                other => Err(format!("grid_route_site_full {other:?}, expected 0").into()),
            }
        })?;
    }
    wait_for("the model stops shedding once a site has room", || {
        let gateway = metrics.read()?;
        match sample_value(&gateway, "grid_route_shedding", &[("model", VCR_MODEL)]) {
            Some(shed) if shed >= 1.0 => Err("model still shedding".into()),
            _ => Ok(()),
        }
    })?;
    observations
        .push("[BACKLOG] both sites were full with a queue past depth 5 and recovered when it drained".to_owned());
    Ok(())
}

/// One site's moderate-load check: source, comparator value against the EPP's own gauges, weight, no learned ceiling.
fn check_source_and_value(
    gateway: &str,
    epp: &str,
    site: &str,
    cluster: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let derived =
        gauge(gateway, "grid_route_site_rho_derived", site, cluster).ok_or("no grid_route_site_rho_derived")?;
    let expected = expected_derived(epp)?;
    if (derived - expected).abs() > TOLERANCE {
        return Err(format!("rho_derived {derived}, the EPP's gauges give {expected}").into());
    }
    let rho = gauge(gateway, "grid_route_site_rho", site, cluster).ok_or("no grid_route_site_rho")?;
    let published =
        gauge(gateway, "grid_route_site_rho_published", site, cluster).ok_or("no grid_route_site_rho_published")?;
    let source = if site == PUBLISHING_SITE {
        let effective = sample_value(epp, "llm_d_epp_flow_control_pool_saturation", &[("stage", "effective")])
            .filter(|value| value.is_finite())
            .ok_or("pool-b EPP publishes no effective pool saturation; is flowControl on?")?;
        let reported =
            gauge(gateway, "grid_route_site_rho_reported", site, cluster).ok_or("no grid_route_site_rho_reported")?;
        if (published - 1.0).abs() > f64::EPSILON {
            return Err(format!("rho_published {published}, expected 1 for a flow-control EPP").into());
        }
        if (reported - effective).abs() > TOLERANCE {
            return Err(format!("rho_reported {reported}, the EPP publishes {effective}").into());
        }
        if (rho - reported).abs() > TOLERANCE {
            return Err(format!("rho {rho} does not follow the published {reported}").into());
        }
        format!("published {effective:.3}, rho {rho:.3}")
    } else {
        if published.abs() > f64::EPSILON {
            return Err(format!("rho_published {published}, expected 0 without flow control").into());
        }
        if (rho - derived).abs() > TOLERANCE {
            return Err(format!("rho {rho} does not follow rho_derived {derived}").into());
        }
        format!("derived {derived:.3}, rho {rho:.3}")
    };
    let units = epp_value(epp, "llm_d_epp_ready_endpoints")?;
    let weight = gauge(gateway, "grid_route_site_weight", site, cluster).ok_or("no grid_route_site_weight")?;
    if (weight - units).abs() > f64::EPSILON {
        return Err(format!("weight {weight}, the EPP reports {units} ready endpoints").into());
    }
    for name in [
        "grid_route_site_ceiling",
        "grid_route_site_ceiling_soft",
        "grid_route_site_ceiling_hard",
    ] {
        if let Some(value) = gauge(gateway, name, site, cluster) {
            return Err(format!("{name} {value} is learned for an llm-d site").into());
        }
    }
    Ok(format!(
        "{source}, comparator {expected:.3}, weight {weight} = ready units"
    ))
}

/// The Gateway's comparator computed from one EPP scrape: KV share plus spread over 0.8 against waiting plus held per
/// unit over 5.
fn expected_derived(epp: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let share = epp_value(epp, "llm_d_epp_average_kv_cache_utilization")?;
    let spread = sample_value(epp, "llm_d_epp_std_dev_kv_cache_utilization", &[])
        .filter(|value| value.is_finite())
        .unwrap_or(0.0);
    let queue = epp_value(epp, "llm_d_epp_average_queue_size")?;
    let units = epp_value(epp, "llm_d_epp_ready_endpoints")?;
    let held = sample_value(epp, "llm_d_epp_flow_control_queue_size", &[])
        .filter(|value| value.is_finite())
        .unwrap_or(0.0);
    let cache = (share + spread).clamp(0.0, 1.0) / KV_FULL_SHARE;
    let waiting = queue + if units > 0.0 { held / units } else { 0.0 };
    Ok(cache.max(waiting / QUEUE_DEPTH))
}

/// Newest sample the hold accepts: five of the gateway's one-second signal polls.
const MAX_READING_AGE_SECONDS: f64 = 5.0;

/// Require an unbroken `NOT_FULL_SPAN` of fresh readings at or above 1 with both sites not full
/// and the model unshed; a gap restarts the span, going full or shed fails.
fn hold_not_full(metrics: &GatewayMetricsReader) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + TRANSITION_TIMEOUT + NOT_FULL_SPAN;
    let mut since = Instant::now();
    let mut last = String::from("not checked");
    while Instant::now() < deadline {
        match not_full_now(metrics)? {
            None if since.elapsed() >= NOT_FULL_SPAN => return Ok(()),
            None => {},
            Some(gap) => {
                last = gap;
                since = Instant::now();
            },
        }
        std::thread::sleep(POLL);
    }
    Err(format!(
        "no unbroken {}s of fresh not-full readings within the deadline; last gap: {last}",
        NOT_FULL_SPAN.as_secs()
    )
    .into())
}

/// One poll of the hold: `Err` when a site went full or the model shed, `Some` gap when a
/// reading is missing, stale or below 1, else `None`.
fn not_full_now(metrics: &GatewayMetricsReader) -> Result<Option<String>, Box<dyn std::error::Error>> {
    match metrics.read() {
        Ok(gateway) => not_full_in(&gateway),
        Err(error) => Ok(Some(format!("gateway metrics unreadable: {error}"))),
    }
}

/// `not_full_now` over one scrape of the gateway's metrics.
fn not_full_in(gateway: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
    for (site, cluster) in SITES {
        let Some(full) = gauge(gateway, "grid_route_site_full", site, cluster) else {
            return Ok(Some(format!("{site} publishes no finite grid_route_site_full")));
        };
        if full >= 1.0 {
            return Err(format!("{site} went full with a spent cache and nothing waiting").into());
        }
        match gauge(gateway, "grid_route_site_reading_age_seconds", site, cluster) {
            Some(age) if age <= MAX_READING_AGE_SECONDS => {},
            age => {
                return Ok(Some(format!(
                    "{site} reading age {age:?}, over {MAX_READING_AGE_SECONDS}s"
                )));
            },
        }
        match gauge(gateway, "grid_route_site_rho", site, cluster) {
            Some(rho) if rho >= 1.0 => {},
            rho => return Ok(Some(format!("{site} rho {rho:?}, not at or above 1"))),
        }
    }
    // The gauge exists only once a model has been shed, so absent reads as never shed.
    match sample_value(gateway, "grid_route_shedding", &[("model", VCR_MODEL)]) {
        Some(shed) if shed >= 1.0 => Err("the model was shed with a spent cache and nothing waiting".into()),
        Some(shed) if !shed.is_finite() => Ok(Some(format!("grid_route_shedding reads {shed}"))),
        _ => Ok(None),
    }
}

/// Poll `check` until it succeeds or `TRANSITION_TIMEOUT` passes, reporting the last reason.
fn wait_for<T>(
    what: &str,
    mut check: impl FnMut() -> Result<T, Box<dyn std::error::Error>>,
) -> Result<T, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + TRANSITION_TIMEOUT;
    let mut last = String::from("not checked");
    while Instant::now() < deadline {
        match check() {
            Ok(value) => return Ok(value),
            Err(error) => last = error.to_string(),
        }
        std::thread::sleep(POLL);
    }
    Err(format!("{what}: not within {}s; last: {last}", TRANSITION_TIMEOUT.as_secs()).into())
}

/// One finite Gateway gauge for the exact pair; `None` when absent or `NaN`.
fn gauge(text: &str, name: &str, site: &str, cluster: &str) -> Option<f64> {
    sample_value(text, name, &[("site", site), ("cluster", cluster)]).filter(|value| value.is_finite())
}

/// One required finite EPP gauge.
fn epp_value(epp: &str, name: &str) -> Result<f64, Box<dyn std::error::Error>> {
    sample_value(epp, name, &[])
        .filter(|value| value.is_finite())
        .ok_or_else(|| format!("EPP publishes no finite {name}").into())
}

/// Apply one stimulus to every run-owned simulator endpoint of a pool.
fn apply(site: &str, stimulus: Stimulus, mtls: bool) -> Result<(), Box<dyn std::error::Error>> {
    set_simulator_fake_metrics(site, stimulus.waiting, stimulus.running, stimulus.kv, mtls)
}

/// Desired replicas of one run-owned simulator deployment.
fn deployment_replicas(site: &str, deployment: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kind_context(site),
            "-n",
            GRID_SYSTEM_NS,
            "get",
            &format!("deployment/{deployment}"),
            "-o",
            "jsonpath={.spec.replicas}",
        ])
        .output()?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned().into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().parse()?)
}

/// Scale one run-owned simulator deployment and wait for the rollout to settle.
fn scale_deployment(site: &str, deployment: &str, replicas: u64) -> Result<(), Box<dyn std::error::Error>> {
    let context = kind_context(site);
    for args in [
        vec![
            "scale".to_owned(),
            format!("deployment/{deployment}"),
            format!("--replicas={replicas}"),
        ],
        vec![
            "rollout".to_owned(),
            "status".to_owned(),
            format!("deployment/{deployment}"),
            "--timeout=120s".to_owned(),
        ],
    ] {
        let mut command = Command::new("kubectl");
        command.args(["--context", &context, "-n", GRID_SYSTEM_NS]).args(&args);
        let output = super::super::operator_routing_qualification::bounded_command(
            command,
            Duration::from_secs(130),
            "simulator replica restoration",
        )
        .map_err(std::io::Error::other)?;
        if !output.status.success() {
            return Err(format!(
                "kubectl {} on {site}: {}",
                args.join(" "),
                String::from_utf8_lossy(&output.stderr).trim()
            )
            .into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn recovery_requires_both_restorations_and_retains_all_errors() {
        let result = require_restorations(
            Some("pressure failed".to_owned()),
            Err("scale failed".to_owned()),
            Err("load failed".to_owned()),
        );
        let detail = result.err().map(|error| error.to_string()).unwrap_or_default();
        assert!(
            detail.contains("pressure failed") && detail.contains("scale failed") && detail.contains("load failed")
        );
        assert!(
            require_restorations(None, Ok(()), Ok(())).is_ok(),
            "successful restoration permits recovery"
        );
    }

    #[test]
    fn route_checkpoints_reject_temporary_weight_changes_even_after_recovery() {
        let initial = RouteReadings {
            a_rho: 0.0,
            b_rho: 1.125,
            a_weight: 2.0,
            b_weight: 2.0,
        };
        let changed = RouteReadings {
            b_weight: 1.0,
            ..initial
        };
        assert!(route_readings_stable(&initial, &initial));
        assert!(!route_readings_stable(&initial, &changed));
        assert!(!route_readings_stable(&changed, &initial));
    }

    #[test]
    fn an_aggregate_pass_cannot_hide_an_empty_capacity_interval() {
        let lost = RouteDistribution {
            sites: BTreeMap::from([("pool-a".to_owned(), 512)]),
            ..RouteDistribution::default()
        };
        let normal = RouteDistribution {
            sites: BTreeMap::from([("pool-a".to_owned(), 94), ("pool-b".to_owned(), 34)]),
            ..RouteDistribution::default()
        };
        assert!((225.0_f64 / 1024.0 - 0.269).abs() < ROUTE_SHARE_TOLERANCE);
        assert!(capacity_interval_gate(&lost, 512, 0.269).is_err());
        assert!(
            capacity_interval_gate(&normal, 128, 0.269).is_ok(),
            "a normal 128-request interval must pass"
        );
    }

    #[test]
    fn expected_derived_takes_the_larger_term_and_counts_held_per_unit() {
        let epp = "llm_d_epp_average_kv_cache_utilization 0.4\n\
                   llm_d_epp_std_dev_kv_cache_utilization 0\n\
                   llm_d_epp_average_queue_size 1\n\
                   llm_d_epp_ready_endpoints 2\n";
        assert!(
            matches!(expected_derived(epp), Ok(moderate) if (moderate - 0.5).abs() < 1e-9),
            "KV 0.4 over 0.8 outweighs queue 1 over 5"
        );

        let held = format!("{epp}llm_d_epp_flow_control_queue_size 8\n");
        assert!(
            matches!(expected_derived(&held), Ok(with_held) if (with_held - 1.0).abs() < 1e-9),
            "held 8 over 2 units adds 4 per unit to the queue of 1, over 5"
        );
        assert!(
            matches!(expected_derived("llm_d_epp_ready_endpoints 2\n"), Err(error) if error.to_string().contains("kv_cache")),
            "a missing KV share is an error, not a zero"
        );
    }

    /// A scrape where both sites read fresh, at or above 1, and not full.
    fn held_scrape(replace: &[(&str, &str)]) -> String {
        let mut text: String = SITES
            .iter()
            .flat_map(|(site, cluster)| {
                [
                    ("grid_route_site_full", "0"),
                    ("grid_route_site_reading_age_seconds", "1"),
                    ("grid_route_site_rho", "1.1"),
                ]
                .map(|(name, value)| format!("{name}{{site=\"{site}\",cluster=\"{cluster}\"}} {value}\n"))
            })
            .collect();
        for (from, to) in replace {
            text = text.replacen(from, to, 1);
        }
        text
    }

    #[test]
    fn the_hold_restarts_on_any_gap_and_fails_on_full() {
        assert!(
            matches!(not_full_in(&held_scrape(&[])), Ok(None)),
            "a fresh, full-reading, not-full scrape holds"
        );
        for (from, to, why) in [
            (
                "grid_route_site_full{site=\"pool-a\"",
                "other_full{site=\"pool-a\"",
                "a missing full gauge",
            ),
            (
                "grid_route_site_full{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 0",
                "grid_route_site_full{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} NaN",
                "a NaN full gauge",
            ),
            (
                "grid_route_site_reading_age_seconds{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 1",
                "grid_route_site_reading_age_seconds{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 30",
                "a stale reading",
            ),
            (
                "grid_route_site_rho{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 1.1",
                "grid_route_site_rho{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 0.4",
                "rho below 1",
            ),
        ] {
            let scrape = held_scrape(&[(from, to)]);
            assert!(
                scrape != held_scrape(&[]),
                "the fixture for {why} must change the scrape"
            );
            assert!(
                matches!(not_full_in(&scrape), Ok(Some(_))),
                "{why} restarts the span instead of passing"
            );
        }
        let full = held_scrape(&[(
            "grid_route_site_full{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 0",
            "grid_route_site_full{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 1",
        )]);
        assert!(not_full_in(&full).is_err(), "a site that went full fails the proof");
        let shed = format!("{}grid_route_shedding{{model=\"{VCR_MODEL}\"}} 1\n", held_scrape(&[]));
        assert!(not_full_in(&shed).is_err(), "a shed model fails the proof");
    }

    #[test]
    fn stimuli_land_on_the_intended_side_of_full() {
        let read = |stimulus: Stimulus| (stimulus.kv / KV_FULL_SHARE).max(f64::from(stimulus.waiting) / QUEUE_DEPTH);
        assert!(read(MODERATE) < 1.0, "moderate load must leave room");
        assert!(
            read(KV_ONLY) >= 1.0 && KV_ONLY.waiting == 0,
            "the KV-only case must read full with nothing waiting"
        );
        assert!(
            read(BACKLOG) >= 1.0 && BACKLOG.waiting > 0,
            "the backlog case must read full with work waiting"
        );
        assert!(read(IDLE) < f64::EPSILON, "idle must read empty");
    }

    #[test]
    fn soft_saturation_route_gate_requires_positive_reduced_share_and_recovery() {
        assert!(
            route_share_transition_gate(0.5, 0.3, 0.52).is_ok(),
            "a lower but nonzero pressure share and restored idle share pass"
        );
        assert!(
            route_share_transition_gate(0.5, 0.0, 0.5).is_err(),
            "the original all-or-nothing cliff fails"
        );
        assert!(
            route_share_transition_gate(0.5, 0.49, 0.5).is_err(),
            "soft saturation must reduce the share"
        );
        assert!(
            route_share_transition_gate(0.5, 0.41, 0.5).is_err(),
            "pressure reduction smaller than the frozen 10-point gate fails"
        );
        assert!(
            route_share_transition_gate(0.56, 0.3, 0.5).is_err(),
            "baseline drift beyond the frozen five-point tolerance fails"
        );
        assert!(
            route_share_transition_gate(0.5, 0.3, 0.56).is_err(),
            "restored simulator pressure must restore the idle route share"
        );
    }

    #[test]
    fn half_capacity_route_gate_requires_a_positive_reduction_and_recovery() {
        assert!(
            capacity_loss_share_transition_gate(0.50, 0.27, 0.51, 0.27).is_ok(),
            "a two-to-one ready-unit loss with fixed aggregate load should reduce pool-b's predicted share and recover"
        );
        assert!(
            capacity_loss_share_transition_gate(0.50, 0.0, 0.50, 0.27).is_err(),
            "losing half the capacity must not drop the affected site's sampled share to zero"
        );
        assert!(
            capacity_loss_share_transition_gate(0.50, 0.50, 0.50, 0.27).is_err(),
            "the affected site's share must fall when its capacity halves"
        );
        assert!(
            capacity_loss_share_transition_gate(0.50, 0.27, 0.27, 0.27).is_err(),
            "the affected site's share must recover after capacity returns"
        );
    }

    #[test]
    fn half_capacity_share_uses_ready_units_and_soft_pressure() {
        let baseline = CapacityRouteReadings {
            baseline_weight: 2.0,
            affected_weight: 2.0,
            baseline_rho: 0.5625,
            affected_rho: 0.5625,
        };
        let reduced = CapacityRouteReadings {
            affected_weight: 1.0,
            affected_rho: 1.125,
            ..baseline
        };
        assert!((predicted_pool_b_capacity_share(&baseline) - 0.5).abs() < f64::EPSILON);
        assert!(
            (predicted_pool_b_capacity_share(&reduced) - 0.268_817_204_301_075_25).abs() < 1e-12,
            "the selector uses both reduced capacity and the increased non-full soft pressure"
        );
        assert!(
            capacity_loss_holds_aggregate_work_constant(),
            "pool-B loses half its ready units while aggregate running and KV load remain fixed"
        );
    }

    #[test]
    fn route_sample_share_is_for_the_named_site() -> Result<(), Box<dyn std::error::Error>> {
        let distribution = RouteDistribution {
            sites: BTreeMap::from([("pool-a".to_owned(), 720), ("pool-b".to_owned(), 304)]),
            ..RouteDistribution::default()
        };
        let pool_a = site_share(&distribution, "pool-a", SIGNAL_ROUTE_SAMPLE_REQUESTS)?;
        let pool_b = site_share(&distribution, "pool-b", SIGNAL_ROUTE_SAMPLE_REQUESTS)?;
        assert!((pool_a - 720.0 / 1024.0).abs() < f64::EPSILON);
        assert!((pool_b - 304.0 / 1024.0).abs() < f64::EPSILON);
        assert!((pool_a + pool_b - 1.0).abs() < f64::EPSILON);
        Ok(())
    }

    #[test]
    fn pressure_sample_share_matches_the_live_two_site_selector_formula() {
        let idle = RouteReadings {
            a_rho: 0.0,
            a_weight: 2.0,
            b_weight: 2.0,
            b_rho: 0.0,
        };
        let pressure = RouteReadings {
            a_rho: 0.0,
            a_weight: 2.0,
            b_weight: 2.0,
            b_rho: 1.125,
        };
        assert!((predicted_pool_b_share(&idle) - 0.5).abs() < f64::EPSILON);
        assert!((predicted_pool_b_share(&pressure) - (1.0 / 2.125) / (1.0 + 1.0 / 2.125)).abs() < f64::EPSILON);
    }
}
