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
    cross_site::{read_gateway_metrics, sample_value, start_ready_port_forward},
    format_utc_iso, kind_context, kubectl_exec_epp_metrics, set_simulator_fake_metrics,
};

/// Both sites as the Gateway labels them; pool-B's EPP runs flow control and publishes its saturation.
const SITES: [(&str, &str); 2] = [("pool-a", "llmd-pool-a-provider"), ("pool-b", "llmd-pool-b-provider")];
/// The site whose EPP publishes `llm_d_epp_flow_control_pool_saturation`.
const PUBLISHING_SITE: &str = "pool-b";
/// The publishing site's provider, as the Gateway labels it.
const PUBLISHING_CLUSTER: &str = "llmd-pool-b-provider";
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

/// One simulator stimulus applied to every run-owned endpoint of a pool.
#[derive(Clone, Copy, Debug)]
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
        description: "llm-d sites are weighed by ready units, read by the EPP's published saturation where flow control publishes it and by the Gateway's comparator otherwise, are full only with work waiting, and publish no learned ceiling".to_owned(),
        observations,
    }
}

/// Every proof step, in order; the caller restores state whatever this returns.
fn run(mtls: bool, original_replicas: u64, observations: &mut Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let local = kind_context(GRID_SERVING_SITE);
    let (port, _forward) = start_ready_port_forward(
        &local,
        &format!("{GRID_SERVING_GATEWAY}-metrics"),
        GRID_SERVING_METRICS_PORT,
    )?;
    moderate_load(port, mtls, observations)?;
    weight_follows_units(port, mtls, original_replicas, observations)?;
    spent_cache_is_not_full(port, mtls, observations)?;
    backlog_is_full_then_recovers(port, mtls, observations)
}

/// Steps 1 to 3 and 5: weight, source, comparator value and no learned ceiling under moderate load.
fn moderate_load(port: u16, mtls: bool, observations: &mut Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    for (site, _) in SITES {
        apply(site, MODERATE, mtls)?;
    }
    for (site, cluster) in SITES {
        let reading = wait_for(&format!("{site} reads moderate load from its source"), || {
            let gateway = read_gateway_metrics(port)?;
            let epp = kubectl_exec_epp_metrics(site, mtls)?;
            check_source_and_value(&gateway, &epp, site, cluster)
        })?;
        observations.push(format!("[MODERATE] {site}: {reading}"));
    }
    Ok(())
}

/// Step 1: pool-B's weight follows its ready units down and back.
fn weight_follows_units(
    port: u16,
    mtls: bool,
    original_replicas: u64,
    observations: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if original_replicas == 0 {
        return Err(format!("{SHRINK_DEPLOYMENT} on {PUBLISHING_SITE} has no replicas to remove").into());
    }
    let before = weight_matching_units(port, PUBLISHING_SITE, PUBLISHING_CLUSTER, mtls)?;
    scale_deployment(PUBLISHING_SITE, SHRINK_DEPLOYMENT, 0)?;
    let shrunk = wait_for("pool-b weight follows one fewer ready unit", || {
        let weight = weight_matching_units(port, PUBLISHING_SITE, PUBLISHING_CLUSTER, mtls)?;
        if weight < before {
            Ok(weight)
        } else {
            Err(format!("weight {weight} has not dropped below {before}").into())
        }
    })?;
    scale_deployment(PUBLISHING_SITE, SHRINK_DEPLOYMENT, original_replicas)?;
    wait_for("pool-b weight returns with the restored unit", || {
        let weight = weight_matching_units(port, PUBLISHING_SITE, PUBLISHING_CLUSTER, mtls)?;
        if (weight - before).abs() < f64::EPSILON {
            Ok(weight)
        } else {
            Err(format!("weight {weight}, expected the original {before}").into())
        }
    })?;
    observations.push(format!(
        "[WEIGHT] pool-b weight {before} -> {shrunk} -> {before} with its ready units"
    ));
    Ok(())
}

/// Step 4a: a spent cache with nothing waiting reads above 1 but is never full and never sheds.
fn spent_cache_is_not_full(
    port: u16,
    mtls: bool,
    observations: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    for (site, _) in SITES {
        apply(site, KV_ONLY, mtls)?;
    }
    for (site, cluster) in SITES {
        wait_for(&format!("{site} reads its spent cache above 1"), || {
            let gateway = read_gateway_metrics(port)?;
            let rho = gauge(&gateway, "grid_route_site_rho", site, cluster).ok_or("no grid_route_site_rho")?;
            if rho >= 1.0 {
                Ok(rho)
            } else {
                Err(format!("rho {rho} below 1").into())
            }
        })?;
    }
    hold_not_full(port)?;
    observations.push(format!(
        "[KV-ONLY] both sites read rho >= 1 with no queue and stayed not full and unshed for {}s",
        NOT_FULL_SPAN.as_secs()
    ));
    Ok(())
}

/// Step 4b: a queue past the detector depth with work waiting is full, then recovers when it drains.
fn backlog_is_full_then_recovers(
    port: u16,
    mtls: bool,
    observations: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    for (site, _) in SITES {
        apply(site, BACKLOG, mtls)?;
    }
    for (site, cluster) in SITES {
        wait_for(&format!("{site} is full with work waiting"), || {
            let gateway = read_gateway_metrics(port)?;
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
            let gateway = read_gateway_metrics(port)?;
            match gauge(&gateway, "grid_route_site_full", site, cluster) {
                Some(full) if full.abs() < f64::EPSILON => Ok(()),
                other => Err(format!("grid_route_site_full {other:?}, expected 0").into()),
            }
        })?;
    }
    wait_for("the model stops shedding once a site has room", || {
        let gateway = read_gateway_metrics(port)?;
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

/// Read the site's weight and require it to equal the EPP's ready endpoints in the same moment.
fn weight_matching_units(port: u16, site: &str, cluster: &str, mtls: bool) -> Result<f64, Box<dyn std::error::Error>> {
    let gateway = read_gateway_metrics(port)?;
    let units = epp_value(&kubectl_exec_epp_metrics(site, mtls)?, "llm_d_epp_ready_endpoints")?;
    let weight = gauge(&gateway, "grid_route_site_weight", site, cluster).ok_or("no grid_route_site_weight")?;
    if (weight - units).abs() > f64::EPSILON {
        return Err(format!("{site} weight {weight}, the EPP reports {units} ready endpoints").into());
    }
    Ok(weight)
}

/// Newest sample the hold accepts: five of the gateway's one-second signal polls.
const MAX_READING_AGE_SECONDS: f64 = 5.0;

/// Require an unbroken `NOT_FULL_SPAN` of fresh readings at or above 1 with both sites not full
/// and the model unshed; a gap restarts the span, going full or shed fails.
fn hold_not_full(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + TRANSITION_TIMEOUT + NOT_FULL_SPAN;
    let mut since = Instant::now();
    let mut last = String::from("not checked");
    while Instant::now() < deadline {
        match not_full_now(port)? {
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
fn not_full_now(port: u16) -> Result<Option<String>, Box<dyn std::error::Error>> {
    match read_gateway_metrics(port) {
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
        let output = Command::new("kubectl")
            .args(["--context", &context, "-n", GRID_SYSTEM_NS])
            .args(&args)
            .output()?;
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
    use super::*;

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
}
