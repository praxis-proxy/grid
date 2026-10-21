//! Deployed proof for EPP signal relay and Grid Gateway site selection.

#![expect(
    clippy::disallowed_methods,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "This synchronous Kind proof owns bounded polling and one ordered live-signal lifecycle"
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use super::{
    CLUSTERS, CONSUMER_TLS_SECRET, CurlProbeDiagnostic, DemoContext, GRID_NETWORK_NAME, GRID_SERVING_CONFIGMAP,
    GRID_SERVING_GATEWAY, GRID_SERVING_GATEWAY_PORT, GRID_SERVING_METRICS_PORT, GRID_SERVING_SITE, GRID_SYSTEM_NS,
    MetricsTransport, OPERATOR_METRICS_PORT, OVERLAY_CONFIGMAP, PROVIDER_GATEWAY_PORT, PortForwardGuard, ProofResult,
    VCR_MODEL, curl_probe_diagnostics, find_free_port, format_utc_iso, get_configmap_data_key, kind_context, kubectl,
    run_cluster_prefix, set_simulator_waiting_requests, split_image_reference,
};

/// One live peer is sufficient for the two-site topology used by this proof.
const MIN_CONNECTED_SWIM_MEMBERS: u64 = 1;
/// Requests in one site-selection sample, large enough to compare the weighted selector with live metrics.
const WEIGHTED_ROUTE_SAMPLE_REQUESTS: usize = 256;
/// Bound each request batch so changing saturation is observed between samples.
const WEIGHTED_ROUTE_SAMPLE_BATCH_REQUESTS: usize = 32;
/// Maximum absolute difference between observed and metric-predicted two-site route share.
const WEIGHTED_ROUTE_SHARE_TOLERANCE: f64 = 0.15;
/// `grid_site_route` clamps a two-site weighted share to this range.
const TWO_SITE_SHARE_RANGE: (f64, f64) = (0.1, 0.9);
/// Provider identity retained while pool-a's EPP signal expires.
const POOL_A_PROVIDER: &str = "llmd-pool-a-provider";
/// Gateway gauges that a measured refresh owns and must clear when its reading disappears.
const MEASURED_SITE_GAUGES: [&str; 8] = [
    "grid_route_site_ceiling",
    "grid_route_site_ceiling_soft",
    "grid_route_site_ceiling_hard",
    "grid_route_site_rho_reported",
    "grid_route_site_rho_counted",
    "grid_route_site_in_flight",
    "grid_route_site_full",
    "grid_route_site_reading_age_seconds",
];
/// Live gauges required before testing the stale-signal cleanup path.
const ACTIVE_SIGNAL_GAUGES: [&str; 3] = [
    "grid_route_site_in_flight",
    "grid_route_site_full",
    "grid_route_site_reading_age_seconds",
];
/// Site and cluster identity in the Gateway's serving topology.
type ServingCandidatePair = (String, String);
/// Parsed site/cluster pairs from the operator-generated serving configuration.
type ServingCandidatePairsResult = Result<BTreeSet<ServingCandidatePair>, Box<dyn std::error::Error>>;
/// Process-wide request counter so separate routing samples never reuse a session identifier.
static SITE_REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// One exact node attachment to the run-owned cross-cluster Docker network.
struct NetworkAttachment {
    /// Kind node container name.
    container: String,
    /// IPv4 address to restore verbatim after the partition.
    ipv4: String,
    /// Docker network aliases to restore with the attachment.
    aliases: Vec<String>,
    /// Whether this proof disconnected the attachment.
    disconnected: bool,
}

/// Reversible partition of only the run-owned pool-b Kind nodes from the shared network.
struct DockerNetworkPartition {
    /// The unique network shared by this run's two Kind clusters.
    network: String,
    /// Immutable Docker network identity captured before disconnecting nodes.
    network_id: String,
    /// Pool-b Kind node network attachments.
    attachments: Vec<NetworkAttachment>,
}

/// Last observed samples and whether the wrong-pin pressure precondition stabilized.
#[derive(Default)]
struct WrongPinSignalWindow {
    /// Local pool-a signal at the end of the bounded observation.
    local_a: Option<f64>,
    /// Pool-b signal still accepted by pool-a.
    trusted_b: Option<f64>,
    /// Pool-b signal reported by the pool-b operator.
    origin_b: Option<f64>,
    /// Whether `origin_b < local_a < trusted_b` held for two consecutive polls.
    stable: bool,
    /// Whether pool-a ever accepted a value different from the trusted baseline.
    trusted_changed: bool,
}

impl DockerNetworkPartition {
    /// Disconnect all pool-b Kind nodes from the network shared with pool-a.
    fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let local_nodes = kind_node_names(&kind_context("pool-a"))?;
        let remote_nodes = kind_node_names(&kind_context("pool-b"))?;
        if local_nodes.is_empty() || remote_nodes.is_empty() {
            return Err("partition proof requires at least one Kind node at each site".into());
        }
        let local_networks = shared_kind_networks(&local_nodes)?;
        let remote_networks = shared_kind_networks(&remote_nodes)?;
        let run_prefix = run_cluster_prefix();
        let shared: Vec<_> = local_networks
            .intersection(&remote_networks)
            .filter(|name| name.starts_with(run_prefix))
            .cloned()
            .collect();
        let network = match shared.as_slice() {
            [network] => network.clone(),
            [] => return Err(format!("the run's Kind sites share no Docker network with prefix {run_prefix}").into()),
            _ => return Err(format!("multiple run-owned cross-cluster networks matched: {shared:?}").into()),
        };
        let network_id = docker_network_id(&network)?;
        let attachments = remote_nodes
            .iter()
            .map(|node| network_attachment(node, &network))
            .collect::<Result<Vec<_>, _>>()?;
        let mut partition = Self {
            network,
            network_id,
            attachments,
        };
        for attachment in &mut partition.attachments {
            let output = Command::new("docker")
                .args(["network", "disconnect", &partition.network, &attachment.container])
                .output()?;
            if !output.status.success() {
                return Err(format!(
                    "could not partition run-owned network {} from {}: {}",
                    partition.network,
                    attachment.container,
                    String::from_utf8_lossy(&output.stderr).trim()
                )
                .into());
            }
            attachment.disconnected = true;
        }
        Ok(partition)
    }

    /// Reattach every disconnected node to the original network address and aliases.
    fn restore(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.attachments.iter().all(|attachment| !attachment.disconnected) {
            return Ok(());
        }
        let current_id = docker_network_id(&self.network)?;
        if current_id != self.network_id {
            return Err(format!(
                "run network {} changed identity before rollback: {} -> {}",
                self.network, self.network_id, current_id
            )
            .into());
        }
        let mut errors = Vec::new();
        for attachment in &mut self.attachments {
            if !attachment.disconnected {
                continue;
            }
            if let Ok(current) = network_attachment(&attachment.container, &self.network) {
                if attachment_matches(&current, attachment) {
                    attachment.disconnected = false;
                    continue;
                }
                let output = Command::new("docker")
                    .args(["network", "disconnect", &self.network, &attachment.container])
                    .output();
                if !output.is_ok_and(|output| output.status.success()) {
                    errors.push(format!(
                        "could not replace the changed {} attachment on {}",
                        attachment.container, self.network
                    ));
                    continue;
                }
            }
            let mut command = Command::new("docker");
            command.args(["network", "connect", "--ip", &attachment.ipv4]);
            for alias in &attachment.aliases {
                command.args(["--alias", alias]);
            }
            let output = command.args([&self.network, &attachment.container]).output();
            match output {
                Ok(output) if output.status.success() => {
                    match network_attachment(&attachment.container, &self.network) {
                        Ok(restored) if attachment_matches(&restored, attachment) => {
                            attachment.disconnected = false;
                        },
                        Ok(restored) => errors.push(format!(
                            "{} returned with address {} and aliases {:?}, expected {} and {:?}",
                            attachment.container, restored.ipv4, restored.aliases, attachment.ipv4, attachment.aliases
                        )),
                        Err(error) => errors.push(format!(
                            "{} did not rejoin {}: {error}",
                            attachment.container, self.network
                        )),
                    }
                },
                Ok(output) => errors.push(format!(
                    "could not restore {} to {}: {}",
                    attachment.container,
                    self.network,
                    String::from_utf8_lossy(&output.stderr).trim()
                )),
                Err(error) => errors.push(format!("could not run Docker network restore: {error}")),
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(format!("run-owned network partition rollback failed: {}", errors.join("; ")).into())
        }
    }

    /// Bounded evidence for the partition's network and target containers.
    fn summary(&self) -> serde_json::Value {
        serde_json::json!({
            "network": self.network,
            "network_id": self.network_id,
            "disconnected_pool_b_nodes": self.attachments.iter().map(|node| serde_json::json!({
                "container": node.container,
                "restored_ipv4": node.ipv4,
                "restored_aliases": node.aliases,
            })).collect::<Vec<_>>(),
        })
    }
}

/// Compare a restored attachment with the saved address and aliases.
fn attachment_matches(current: &NetworkAttachment, expected: &NetworkAttachment) -> bool {
    current.ipv4 == expected.ipv4
        && current.aliases.iter().collect::<BTreeSet<_>>() == expected.aliases.iter().collect::<BTreeSet<_>>()
}

impl Drop for DockerNetworkPartition {
    fn drop(&mut self) {
        if self.attachments.iter().all(|attachment| !attachment.disconnected) {
            return;
        }
        eprintln!(
            "[CROSS-SITE][CLEANUP][{}] fallback rollback started for run-owned network {}",
            format_utc_iso(),
            self.network
        );
        match self.restore() {
            Ok(()) => eprintln!(
                "[CROSS-SITE][CLEANUP][{}] fallback rollback restored run-owned network {}",
                format_utc_iso(),
                self.network
            ),
            Err(error) => eprintln!(
                "[CROSS-SITE][CLEANUP][{}][FAIL] fallback rollback failed for run-owned network {}: {error}",
                format_utc_iso(),
                self.network
            ),
        }
    }
}

/// Kind node names currently registered in one run-owned cluster.
fn kind_node_names(context: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args(["--context", context, "get", "nodes", "-o", "json"])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "read Kind nodes for {context}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let nodes: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    Ok(nodes
        .get("items")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|node| node.pointer("/metadata/name").and_then(serde_json::Value::as_str))
        .map(str::to_owned)
        .collect())
}

/// Docker network names attached to every node in one Kind cluster.
fn shared_kind_networks(nodes: &[String]) -> Result<BTreeSet<String>, Box<dyn std::error::Error>> {
    let mut iter = nodes.iter();
    let Some(first) = iter.next() else {
        return Ok(BTreeSet::new());
    };
    let mut shared = docker_container_networks(first)?;
    for node in iter {
        let names = docker_container_networks(node)?;
        shared = shared.intersection(&names).cloned().collect();
    }
    Ok(shared)
}

/// Inspect one Docker container as JSON.
fn docker_container_json(container: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let output = Command::new("docker").args(["inspect", container]).output()?;
    if !output.status.success() {
        return Err(format!(
            "docker inspect {container}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let inspected: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    inspected
        .as_array()
        .and_then(|items| items.first())
        .cloned()
        .ok_or_else(|| format!("docker inspect {container} returned no object").into())
}

/// Network names attached to one Docker container.
fn docker_container_networks(container: &str) -> Result<BTreeSet<String>, Box<dyn std::error::Error>> {
    let inspected = docker_container_json(container)?;
    Ok(inspected
        .pointer("/NetworkSettings/Networks")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("Docker container {container} has no network map"))?
        .keys()
        .cloned()
        .collect())
}

/// Snapshot one node's exact network attachment for reversible disconnection.
fn network_attachment(container: &str, network: &str) -> Result<NetworkAttachment, Box<dyn std::error::Error>> {
    let inspected = docker_container_json(container)?;
    let attachment = inspected
        .pointer(&format!("/NetworkSettings/Networks/{network}"))
        .ok_or_else(|| format!("Docker node {container} is not attached to run network {network}"))?;
    let ipv4 = attachment
        .get("IPAddress")
        .and_then(serde_json::Value::as_str)
        .filter(|address| !address.is_empty())
        .ok_or_else(|| format!("Docker node {container} has no IPv4 address on run network {network}"))?;
    let aliases = attachment
        .get("Aliases")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_owned)
        .collect();
    Ok(NetworkAttachment {
        container: container.to_owned(),
        ipv4: ipv4.to_owned(),
        aliases,
        disconnected: false,
    })
}

/// Immutable Docker network ID used to verify that rollback reaches the same object.
fn docker_network_id(network: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("docker").args(["network", "inspect", network]).output()?;
    if !output.status.success() {
        return Err(format!(
            "docker network inspect {network}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let inspected: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    inspected
        .pointer("/0/Id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("Docker network {network} has no immutable ID").into())
}

/// Run the full-mode cross-site signal proof.
pub(super) fn proof(context: &DemoContext) -> ProofResult {
    let mut observations = Vec::new();
    let mut attribution = RoutingAttributionDiagnostics::default();
    record(&mut observations, "cross-site signal proof started".to_owned());
    let result = run(context, &mut observations, &mut attribution);
    if let Err(error) = &result {
        record(&mut observations, format!("primary proof assertion failed: {error}"));
    }

    record(
        &mut observations,
        "final restoration started for EPP and simulator pressure".to_owned(),
    );
    let restore_epp = scale_epp("pool-a", 1);
    let restore_a = set_simulator_waiting_requests(
        "pool-a",
        0,
        context.scoring_flavor,
        context.metrics_transport == MetricsTransport::MtlsProxy,
    );
    let restore_b = set_simulator_waiting_requests(
        "pool-b",
        0,
        context.scoring_flavor,
        context.metrics_transport == MetricsTransport::MtlsProxy,
    );
    for (name, restored) in [
        ("pool-a EPP", restore_epp),
        ("pool-a pressure", restore_a),
        ("pool-b pressure", restore_b),
    ] {
        match restored {
            Ok(()) => record(&mut observations, format!("final restoration succeeded for {name}")),
            Err(error) => record(
                &mut observations,
                format!("final restoration failed for {name}: {error}"),
            ),
        }
    }

    record(
        &mut observations,
        format!("Grid Gateway response attribution diagnostics: {attribution}"),
    );
    let attribution_error = attribution.contract_error();
    let result = match (result, attribution_error) {
        (Ok(()), Some(error)) => Err(error.into()),
        (result, _) => result,
    };
    let run_succeeded = result.is_ok();
    if let Err(error) = &result {
        record(&mut observations, format!("cross-site signal proof failed: {error}"));
    }
    let success = run_succeeded
        && observations
            .iter()
            .all(|item| !item.contains("final restoration failed"));
    record(
        &mut observations,
        if success {
            "cross-site signal proof passed".to_owned()
        } else {
            "cross-site signal proof failed; see the primary proof and restoration errors above".to_owned()
        },
    );
    ProofResult {
        success,
        description: "EPP signals are scraped, relayed over operator mTLS polling, consumed by the local Grid Gateway, and drive live site selection".to_owned(),
        observations,
    }
}

/// Execute the ordered signal, routing, outage, recovery, and identity checks.
#[expect(
    clippy::too_many_lines,
    reason = "The observations preserve one time-ordered live proof"
)]
fn run(
    context: &DemoContext,
    observations: &mut Vec<String>,
    attribution: &mut RoutingAttributionDiagnostics,
) -> Result<(), Box<dyn std::error::Error>> {
    let image = context
        .images
        .grid_gateway
        .as_deref()
        .ok_or("full mode did not resolve a Grid Gateway image")?;
    let mtls = context.metrics_transport == MetricsTransport::MtlsProxy;

    set_simulator_waiting_requests("pool-a", 0, context.scoring_flavor, mtls)?;
    set_simulator_waiting_requests("pool-b", 9, context.scoring_flavor, mtls)?;
    prepare_poll_gateway(image)?;

    let local_context = kind_context(GRID_SERVING_SITE);
    let (gateway_http_port, _gateway_forward) =
        start_ready_port_forward(&local_context, GRID_SERVING_GATEWAY, GRID_SERVING_GATEWAY_PORT)?;
    let (gateway_metrics_port, _gateway_metrics_forward) = start_ready_port_forward(
        &local_context,
        &format!("{GRID_SERVING_GATEWAY}-metrics"),
        GRID_SERVING_METRICS_PORT,
    )?;
    let (operator_metrics_port, _operator_metrics_forward) =
        start_ready_port_forward(&local_context, "grid-operator-metrics", OPERATOR_METRICS_PORT)?;
    let peer_context = kind_context("pool-b");
    let (peer_operator_metrics_port, _peer_operator_metrics_forward) =
        start_ready_port_forward(&peer_context, "grid-operator-metrics", OPERATOR_METRICS_PORT)?;

    let serving_peer = wait_for_grid_serving_snapshot(Duration::from_secs(120))?;
    record(
        observations,
        format!("Grid Gateway serving config names its local operator poll target: {serving_peer}"),
    );
    wait_for_gateway_config_observed(gateway_metrics_port, Duration::from_secs(90))?;
    record(
        observations,
        "Grid Gateway watcher observed its mounted serving configuration".to_owned(),
    );

    wait_for_relay_samples(operator_metrics_port, Duration::from_secs(120))?;
    let metrics = read_operator_metrics(operator_metrics_port)?;
    let local_a = sample_value(
        &metrics,
        "grid_provider_in_flight_requests",
        &[("grid_site", "pool-a"), ("grid_provider", "llmd-pool-a-provider")],
    )
    .ok_or("pool-a operator metrics contain no local EPP signal sample")?;
    let relayed_b = sample_value(
        &metrics,
        "grid_provider_in_flight_requests",
        &[("grid_site", "pool-b"), ("grid_provider", "llmd-pool-b-provider")],
    )
    .ok_or("pool-a operator metrics contain no relayed pool-b signal sample")?;
    if local_a >= relayed_b {
        return Err(format!(
            "expected pool-a to have lower measured pressure than pool-b, got {local_a} >= {relayed_b}"
        )
        .into());
    }
    record(
        observations,
        format!(
            "EPP scrape and operator-to-operator mTLS relay observed: pool-a in-flight={local_a}, pool-b in-flight={relayed_b}"
        ),
    );
    record(
        observations,
        format!(
            "pool-a peer-poll success counter: {}",
            metric_counter(
                &metrics,
                "grid_peer_poll_total",
                &[("peer", "pool-b"), ("outcome", "ok")]
            )
        ),
    );

    verify_weighted_site_distribution(
        gateway_http_port,
        gateway_metrics_port,
        "pool-a",
        false,
        "lower-pressure-baseline",
        attribution,
        Duration::from_secs(120),
    )
    .map(|distribution| record(observations, format!("lower-pressure baseline: {distribution}")))?;
    let gateway_uid = gateway_pod_uid(&local_context)?;
    let gateway_reloads_before_shift = applied_gateway_reload_count(gateway_metrics_port)?;

    set_simulator_waiting_requests("pool-a", 9, context.scoring_flavor, mtls)?;
    set_simulator_waiting_requests("pool-b", 0, context.scoring_flavor, mtls)?;
    wait_for_pressure_order(operator_metrics_port, "pool-b", Duration::from_secs(120))?;
    verify_weighted_site_distribution(
        gateway_http_port,
        gateway_metrics_port,
        "pool-b",
        true,
        "live-pressure-shift",
        attribution,
        Duration::from_secs(120),
    )
    .map(|distribution| record(observations, format!("live pressure shift: {distribution}")))?;
    let gateway_uid_after_shift = gateway_pod_uid(&local_context)?;
    if gateway_uid != gateway_uid_after_shift {
        return Err(format!(
            "Grid Gateway pod changed during the A-to-B signal shift: {gateway_uid} -> {gateway_uid_after_shift}"
        )
        .into());
    }
    if (applied_gateway_reload_count(gateway_metrics_port)? - gateway_reloads_before_shift).abs() > f64::EPSILON {
        return Err("Grid Gateway reloaded serving configuration during the signal-only pressure shift".into());
    }
    record(
        observations,
        format!("Grid Gateway pod UID remained {gateway_uid}; its serving-config reload count stayed unchanged"),
    );

    let metrics_before_signal_loss = read_gateway_metrics(gateway_metrics_port)?;
    let active_site_metrics = require_active_signal_gauges(&metrics_before_signal_loss, "pool-a", POOL_A_PROVIDER)?;
    if !serving_candidate_pairs(&local_context)?.contains(&("pool-a".to_owned(), POOL_A_PROVIDER.to_owned())) {
        return Err("pool-a provider is absent before the stale-signal gauge proof".into());
    }
    record(
        observations,
        format!(
            "Grid Gateway had live pool-a in-flight, full, and reading-age gauges before EPP signal removal: {active_site_metrics}"
        ),
    );

    let peer_polls_before_loss = metric_counter(
        &read_operator_metrics(operator_metrics_port)?,
        "grid_peer_poll_total",
        &[("peer", "pool-b"), ("outcome", "ok")],
    );
    scale_epp("pool-a", 0)?;
    wait_for_operator_signal_absence(operator_metrics_port, "pool-a", Duration::from_secs(90))?;
    let connected_sites = read_connected_sites(&local_context)?;
    if !has_connected_swim_peer(connected_sites) {
        return Err(format!(
            "pool-a lost SWIM membership while only its local EPP signal was removed: connectedSites={connected_sites}"
        )
        .into());
    }
    let after_loss = read_operator_metrics(operator_metrics_port)?;
    let peer_polls_after_loss = metric_counter(
        &after_loss,
        "grid_peer_poll_total",
        &[("peer", "pool-b"), ("outcome", "ok")],
    );
    if peer_polls_after_loss <= peer_polls_before_loss {
        return Err("pool-a lost its local EPP signal without continuing successful pool-b operator polls".into());
    }
    record(
        observations,
        format!(
            "pool-a EPP signal became stale while successful SWIM-era peer polling continued ({peer_polls_before_loss} -> {peer_polls_after_loss})"
        ),
    );
    if !serving_candidate_pairs(&local_context)?.contains(&("pool-a".to_owned(), POOL_A_PROVIDER.to_owned())) {
        return Err(
            "pool-a provider left the serving topology before stale-signal gauge cleanup could be verified".into(),
        );
    }
    wait_for_site_distribution(
        gateway_http_port,
        "pool-b",
        "stale-pool-a-signal-exclusion",
        attribution,
        Duration::from_secs(90),
    )
    .map(|distribution| {
        record(
            observations,
            format!("stale pool-a signal excluded while connectedSites={connected_sites}: {distribution}"),
        );
    })?;
    let cleared_site_metrics = wait_for_measured_site_gauges_cleared(
        gateway_metrics_port,
        "pool-a",
        POOL_A_PROVIDER,
        Duration::from_secs(90),
    )?;
    record(
        observations,
        format!(
            "Grid Gateway cleared every measured pool-a gauge while its provider remained in serving topology: {cleared_site_metrics}"
        ),
    );

    scale_epp("pool-a", 1)?;
    set_simulator_waiting_requests("pool-a", 0, context.scoring_flavor, mtls)?;
    set_simulator_waiting_requests("pool-b", 9, context.scoring_flavor, mtls)?;
    wait_for_relay_samples(operator_metrics_port, Duration::from_secs(120))?;
    verify_weighted_site_distribution(
        gateway_http_port,
        gateway_metrics_port,
        "pool-a",
        true,
        "signal-and-route-recovery",
        attribution,
        Duration::from_secs(120),
    )
    .map(|distribution| record(observations, format!("signal and route recovery: {distribution}")))?;
    record(
        observations,
        "pool-a provider signal returned after EPP recovery".to_owned(),
    );

    set_simulator_waiting_requests("pool-a", 0, context.scoring_flavor, mtls)?;
    set_simulator_waiting_requests("pool-b", 9, context.scoring_flavor, mtls)?;
    wait_for_pressure_order(operator_metrics_port, "pool-a", Duration::from_secs(120))?;
    let pre_pin_route = verify_weighted_site_distribution(
        gateway_http_port,
        gateway_metrics_port,
        "pool-a",
        true,
        "pre-pin-pressure-baseline",
        attribution,
        Duration::from_secs(120),
    )?;
    record(
        observations,
        format!("precondition before peer identity test: pool-a is lower pressure and routable: {pre_pin_route}"),
    );
    let trusted_metrics_before_bad_pin = read_operator_metrics(operator_metrics_port)?;
    let local_a_before_bad_pin = provider_in_flight_value(&trusted_metrics_before_bad_pin, "pool-a")
        .ok_or("pool-a operator has no local pool-a signal before the wrong-pin proof")?;
    let pool_b_before_bad_pin = provider_in_flight_value(&trusted_metrics_before_bad_pin, "pool-b")
        .ok_or("pool-a operator has no trusted pool-b signal before the wrong-pin proof")?;
    let pool_b_origin_before = provider_in_flight_value(&read_operator_metrics(peer_operator_metrics_port)?, "pool-b")
        .ok_or("pool-b operator has no local signal before the wrong-pin proof")?;
    let remote_site = format!("{GRID_NETWORK_NAME}-pool-b");
    let original_pins = read_site_pins(&local_context, &remote_site)?;
    let poll_failures_before_bad_pin = peer_poll_failure_counts(&trusted_metrics_before_bad_pin, "pool-b");
    let wrong_pin = "0000000000000000000000000000000000000000000000000000000000000000";
    record(
        observations,
        format!(
            "wrong-peer-pin phase starting: trusted pool-a sample={local_a_before_bad_pin}, trusted pool-b sample={pool_b_before_bad_pin}, pool-b origin sample={pool_b_origin_before}"
        ),
    );
    let invalid_identity_result = (|| -> Result<(), Box<dyn std::error::Error>> {
        record(
            observations,
            "injecting invalid pool-b fingerprint on pool-a".to_owned(),
        );
        patch_site_pins(&local_context, &remote_site, &[wrong_pin.to_owned()])?;
        if read_site_pins(&local_context, &remote_site)? != [wrong_pin.to_owned()] {
            return Err("pool-a did not retain the injected invalid pool-b fingerprint".into());
        }
        record(
            observations,
            "pool-a GridSite for pool-b now contains the injected invalid fingerprint".to_owned(),
        );
        let poll_failures_after_bad_pin = wait_for_peer_identity_rejection(
            operator_metrics_port,
            "pool-b",
            poll_failures_before_bad_pin,
            Duration::from_secs(60),
        )?;
        record(
            observations,
            format!(
                "pool-a rejected the changed pool-b identity; peer-poll outcome deltas: {}",
                poll_failures_after_bad_pin.delta_from(poll_failures_before_bad_pin)
            ),
        );

        let (phase_during_rejection, reason_during_rejection, site_resource_version) =
            wait_for_gridsite_phase_and_reason(
                &local_context,
                &remote_site,
                "Connecting",
                "PinMismatch",
                Duration::from_secs(30),
            )?;
        record(
            observations,
            format!(
                "pool-b became ineligible after failed identity verification: GridSite phase={phase_during_rejection}, reason={reason_during_rejection}, resourceVersion={site_resource_version}"
            ),
        );

        let connected_sites_during_tls_rejection = read_connected_sites(&local_context)?;
        if !has_connected_swim_peer(connected_sites_during_tls_rejection) {
            return Err(format!("SWIM membership did not remain connected during peer-signal TLS rejection: connectedSites={connected_sites_during_tls_rejection}").into());
        }

        // The remote endpoint advertises a lower load that would otherwise
        // change the pressure order. While its peer certificate is rejected,
        // pool-a must keep the exact trusted sample. The same failed identity
        // probe makes the remote GridSite Connecting, so fail-closed routing
        // must keep requests on pool-a until trust is restored.
        set_simulator_waiting_requests("pool-a", 6, context.scoring_flavor, mtls)?;
        set_simulator_waiting_requests("pool-b", 0, context.scoring_flavor, mtls)?;
        let pool_b_origin = wait_for_signal_decrease(
            peer_operator_metrics_port,
            "pool-b",
            pool_b_origin_before,
            Duration::from_secs(90),
        )?;
        record(
            observations,
            format!(
                "pool-b origin published the changed lower-pressure sample: {pool_b_origin} (trusted baseline was {pool_b_origin_before})"
            ),
        );
        let poll_failures_before_updated_signal =
            peer_poll_failure_counts(&read_operator_metrics(operator_metrics_port)?, "pool-b");
        let poll_failures_after_updated_signal = wait_for_peer_identity_rejection(
            operator_metrics_port,
            "pool-b",
            poll_failures_before_updated_signal,
            Duration::from_secs(30),
        )?;
        record(
            observations,
            format!(
                "pool-a continued rejecting the changed pool-b identity; peer-poll outcome deltas after the origin update: {}",
                poll_failures_after_updated_signal.delta_from(poll_failures_before_updated_signal)
            ),
        );
        let wrong_pin_window = wait_for_wrong_pin_signal_window(
            operator_metrics_port,
            peer_operator_metrics_port,
            "pool-b",
            pool_b_origin_before,
            pool_b_before_bad_pin,
            Duration::from_secs(30),
        );
        record(
            observations,
            format!(
                "wrong-peer-pin pressure observation: stable={}, local pool-a={:?}, accepted pool-b={:?}, pool-b origin={:?}, trusted sample changed={}",
                wrong_pin_window.stable,
                wrong_pin_window.local_a,
                wrong_pin_window.trusted_b,
                wrong_pin_window.origin_b,
                wrong_pin_window.trusted_changed
            ),
        );
        if !wrong_pin_window.stable {
            record(
                observations,
                "[PRECONDITION][FAIL] changed remote signal did not establish a stable ordering that would prefer pool-b within 30 seconds".to_owned(),
            );
        }
        let remote_matches_trusted_baseline = wrong_pin_window
            .trusted_b
            .is_some_and(|value| (value - pool_b_before_bad_pin).abs() <= f64::EPSILON);
        let trusted_sample_keeps_pool_a_preferred = wrong_pin_window
            .trusted_b
            .zip(wrong_pin_window.local_a)
            .is_some_and(|(trusted, local)| trusted > local);
        let changed_origin_would_prefer_pool_b = wrong_pin_window
            .origin_b
            .zip(wrong_pin_window.local_a)
            .is_some_and(|(origin, local)| origin < local);

        // Always execute the original routing assertion, even when the signal
        // ordering precondition failed. This preserves routing evidence while
        // still failing the proof below if the stimulus was insufficient.
        let wrong_pin_route = wait_for_site_distribution(
            gateway_http_port,
            "pool-a",
            "wrong-peer-pin-routing",
            attribution,
            Duration::from_secs(30),
        );
        if let Ok(distribution) = &wrong_pin_route {
            record(
                observations,
                format!(
                    "wrong peer pin left routing on pool-a while pool-b was ineligible (phase={phase_during_rejection}, reason={reason_during_rejection}): {distribution}; SWIM connectedSites={connected_sites_during_tls_rejection}"
                ),
            );
        }

        let mut assertion_errors = Vec::new();
        if !wrong_pin_window.stable {
            assertion_errors.push("wrong-pin pressure precondition did not stabilize within 30 seconds".to_owned());
        }
        if wrong_pin_window.trusted_changed
            || !remote_matches_trusted_baseline
            || !trusted_sample_keeps_pool_a_preferred
            || !changed_origin_would_prefer_pool_b
        {
            assertion_errors.push(format!("pool-a did not preserve the exact trusted pool-b sample while rejecting a changed signal that would otherwise prefer pool-b: local pool-a={:?}, current pool-b sample={:?}, trusted pool-b baseline={pool_b_before_bad_pin}, rejected pool-b origin={:?}, trusted signal ever changed={}", wrong_pin_window.local_a, wrong_pin_window.trusted_b, wrong_pin_window.origin_b, wrong_pin_window.trusted_changed));
        }
        if let Err(error) = wrong_pin_route {
            assertion_errors.push(format!("original wrong-pin routing assertion failed: {error}"));
        }
        if !assertion_errors.is_empty() {
            return Err(assertion_errors.join("; ").into());
        }
        record(
            observations,
            format!(
                "pool-a retained the exact trusted pool-b value during the rejected update: local pool-a={:?}, trusted pool-b={:?}, rejected origin={:?} would prefer pool-b",
                wrong_pin_window.local_a, wrong_pin_window.trusted_b, wrong_pin_window.origin_b
            ),
        );
        Ok(())
    })();
    record(
        observations,
        "restoring the original pool-b fingerprint on pool-a".to_owned(),
    );
    let pin_restore = patch_site_pins(&local_context, &remote_site, &original_pins);
    match &pin_restore {
        Ok(()) => record(observations, "original pool-b fingerprint restored".to_owned()),
        Err(error) => record(
            observations,
            format!("failed to restore the original pool-b fingerprint: {error}"),
        ),
    }
    match (invalid_identity_result, pin_restore) {
        (Ok(()), Ok(())) => {},
        (Err(proof_error), Ok(())) => return Err(proof_error),
        (Ok(()), Err(restore_error)) => {
            return Err(format!("peer-pin proof passed but pin restoration failed: {restore_error}").into());
        },
        (Err(proof_error), Err(restore_error)) => {
            return Err(format!(
                "peer-pin proof failed: {proof_error}; peer-pin restoration also failed: {restore_error}"
            )
            .into());
        },
    }

    let (phase_after_restore, reason_after_restore, restored_site_resource_version) =
        wait_for_gridsite_phase_and_reason(
            &local_context,
            &remote_site,
            "Active",
            "TlsVerified",
            Duration::from_secs(90),
        )?;
    record(
        observations,
        format!(
            "pool-b GridSite returned to an eligible state after restoring its trusted identity: phase={phase_after_restore}, reason={reason_after_restore}, resourceVersion={restored_site_resource_version}"
        ),
    );
    wait_for_relay_samples(operator_metrics_port, Duration::from_secs(90))?;
    let pool_b_restored = wait_for_signal_decrease(
        operator_metrics_port,
        "pool-b",
        pool_b_before_bad_pin,
        Duration::from_secs(90),
    )?;
    wait_for_pressure_order(operator_metrics_port, "pool-b", Duration::from_secs(90))?;
    verify_weighted_site_distribution(
        gateway_http_port,
        gateway_metrics_port,
        "pool-b",
        true,
        "restored-peer-identity-routing",
        attribution,
        Duration::from_secs(120),
    )
    .map(|distribution| {
        record(
            observations,
            format!("restored peer identity and lower-pressure pool-b routing: {distribution}"),
        );
    })?;
    record(
        observations,
        format!("pool-a accepted the restored peer's lower-pressure signal: {pool_b_restored}"),
    );
    prove_network_partition(
        context,
        gateway_http_port,
        gateway_metrics_port,
        operator_metrics_port,
        mtls,
        &gateway_uid,
        attribution,
        observations,
    )?;
    if gateway_pod_uid(&local_context)? != gateway_uid {
        return Err("Grid Gateway pod changed during the cross-site signal proof".into());
    }

    let gateway_metrics = read_gateway_metrics(gateway_metrics_port)?;
    record(
        observations,
        format!(
            "Grid Gateway route decision samples: pool-a={}, pool-b={}",
            metric_counter(
                &gateway_metrics,
                "grid_route_decisions_total",
                &[("site", "pool-a"), ("reason", "routed")]
            ),
            metric_counter(
                &gateway_metrics,
                "grid_route_decisions_total",
                &[("site", "pool-b"), ("reason", "routed")]
            )
        ),
    );
    Ok(())
}

/// Patch both run-owned networks into poll mode and install the Grid Gateway on pool-a.
fn prepare_poll_gateway(image: &str) -> Result<(), Box<dyn std::error::Error>> {
    let endpoints = [
        provider_gateway_endpoint("pool-a")?,
        provider_gateway_endpoint("pool-b")?,
    ];
    for cluster in CLUSTERS {
        patch_network_for_poll(cluster, &endpoints)?;
    }
    for cluster in CLUSTERS {
        restart_operator(cluster)?;
    }
    apply_grid_gateway_config(&endpoints)?;
    install_grid_gateway(image)?;
    wait_for_grid_serving_snapshot(Duration::from_secs(120))?;
    Ok(())
}

/// Start one diagnostic port-forward and retry if its local listener exits during startup.
pub(super) fn start_ready_port_forward(
    context: &str,
    service: &str,
    service_port: u16,
) -> Result<(u16, PortForwardGuard), Box<dyn std::error::Error>> {
    let mut failures = Vec::new();
    for attempt in 1..=3 {
        let local_port = find_free_port()?;
        let mut forward =
            PortForwardGuard::start_with_diagnostics(context, GRID_SYSTEM_NS, service, local_port, service_port)?;
        match forward.wait_until_ready(local_port) {
            Ok(()) => return Ok((local_port, forward)),
            Err(error) => {
                let message = error.to_string();
                failures.push(format!("attempt {attempt} on local port {local_port}: {message}"));
                if !message.contains("exited with") {
                    break;
                }
            },
        }
    }
    Err(format!(
        "could not start port-forward to {service}:{service_port}: {}",
        failures.join("; ")
    )
    .into())
}

/// Partition pool-b's run-owned Docker link, keep traffic flowing, and prove recovery.
#[expect(
    clippy::too_many_lines,
    reason = "partition, observed degradation, rollback, and recovery are one ordered proof"
)]
fn prove_network_partition(
    context: &DemoContext,
    gateway_port: u16,
    gateway_metrics_port: u16,
    operator_port: u16,
    mtls: bool,
    expected_gateway_uid: &str,
    attribution: &mut RoutingAttributionDiagnostics,
    observations: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    set_simulator_waiting_requests("pool-a", 9, context.scoring_flavor, mtls)?;
    set_simulator_waiting_requests("pool-b", 0, context.scoring_flavor, mtls)?;
    wait_for_pressure_order(operator_port, "pool-b", Duration::from_secs(120))?;
    verify_weighted_site_distribution(
        gateway_port,
        gateway_metrics_port,
        "pool-b",
        true,
        "network-partition-precondition",
        attribution,
        Duration::from_secs(120),
    )?;
    let local_context = kind_context(GRID_SERVING_SITE);
    let connected_before = read_connected_sites(&local_context)?;
    let remote_cluster = "llmd-pool-b-provider";
    let candidate_fresh_before = read_overlay_candidate_fresh(GRID_SERVING_SITE, remote_cluster)?;
    if !has_connected_swim_peer(connected_before) || candidate_fresh_before != Some(true) {
        return Err(format!(
            "partition must start with both sites connected and the remote candidate fresh: connectedSites={connected_before}, fresh={candidate_fresh_before:?}"
        )
        .into());
    }
    let successful_polls_before = metric_counter(
        &read_operator_metrics(operator_port)?,
        "grid_peer_poll_total",
        &[("peer", "pool-b"), ("outcome", "ok")],
    );
    let mut partition = DockerNetworkPartition::start()?;
    let partition_summary = partition.summary();
    record(
        observations,
        format!("run-owned cross-site network partition started: {partition_summary}"),
    );

    let degradation_result = wait_for_partition_degradation(
        gateway_port,
        &local_context,
        remote_cluster,
        Duration::from_secs(180),
        attribution,
        observations,
    );
    record(observations, "run-owned network rollback started".to_owned());
    let restoration_result = partition.restore();
    match &restoration_result {
        Ok(()) => record(observations, "run-owned network rollback succeeded".to_owned()),
        Err(error) => record(observations, format!("run-owned network rollback failed: {error}")),
    }
    match (degradation_result, restoration_result) {
        (Ok(()), Ok(())) => {},
        (Err(proof_error), Ok(())) => return Err(proof_error),
        (Ok(()), Err(restore_error)) => {
            return Err(format!("partition proof passed but network rollback failed: {restore_error}").into());
        },
        (Err(proof_error), Err(restore_error)) => {
            return Err(format!(
                "partition proof failed: {proof_error}; network rollback also failed: {restore_error}"
            )
            .into());
        },
    }
    record(
        observations,
        format!("run-owned Docker network restored with its original ID and node addresses: {partition_summary}"),
    );

    wait_for_connected_fresh_candidate(&local_context, remote_cluster, Duration::from_secs(180))?;
    let connected_after_recovery = read_connected_sites(&local_context)?;
    let successful_polls_after = wait_for_metric_counter_increase(
        operator_port,
        "grid_peer_poll_total",
        &[("peer", "pool-b"), ("outcome", "ok")],
        successful_polls_before,
        Duration::from_secs(90),
    )?;
    verify_weighted_site_distribution(
        gateway_port,
        gateway_metrics_port,
        "pool-b",
        true,
        "network-partition-recovery",
        attribution,
        Duration::from_secs(120),
    )?;
    if gateway_pod_uid(&local_context)? != expected_gateway_uid {
        return Err("Grid Gateway pod changed during network partition and reconvergence".into());
    }
    record(
        observations,
        format!(
            "cross-site partition reconverged: connectedSites={connected_after_recovery}, remote candidate fresh=true, peer polls={successful_polls_before}->{successful_polls_after}, attributed traffic returned to pool-b"
        ),
    );
    Ok(())
}

/// Send requests during a partition until membership and the remote route both become stale.
fn wait_for_partition_degradation(
    gateway_port: u16,
    local_context: &str,
    remote_cluster: &str,
    timeout: Duration,
    attribution: &mut RoutingAttributionDiagnostics,
    observations: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    let mut sequence = 0;
    let mut attempts = 0_usize;
    let mut transport_errors = 0_usize;
    let mut statuses = BTreeMap::new();
    let mut attributed_sites = BTreeMap::new();
    let mut last_request_observation = None;
    let mut last_membership_observation = None;
    let mut last_state = String::from("partition active; waiting for SWIM and overlay updates");

    while Instant::now() < deadline {
        attempts += 1;
        let request_observation =
            match send_site_request_with_timeout(gateway_port, sequence, "network-partition-traffic", 5) {
                Ok(response) => {
                    attribution.observe(&response);
                    *statuses.entry(response.status).or_insert(0_usize) += 1;
                    if let Some(site) = response.provider_site.as_ref().or(response.site.as_ref()) {
                        *attributed_sites.entry(site.clone()).or_insert(0_usize) += 1;
                    }
                    format!(
                        "partition traffic HTTP {}, site={:?}, backend={:?}, provider_gateway_site={:?}",
                        response.status, response.site, response.backend, response.provider_site
                    )
                },
                Err(error) => {
                    transport_errors += 1;
                    format!("partition traffic request error: {error}")
                },
            };
        sequence += 1;
        if last_request_observation.as_deref() != Some(request_observation.as_str()) {
            record(observations, request_observation.clone());
            last_request_observation = Some(request_observation);
        }

        let connected = read_connected_sites(local_context);
        let fresh = read_overlay_candidate_fresh(GRID_SERVING_SITE, remote_cluster);
        let state = format!("connectedSites={connected:?}, remote_candidate_fresh={fresh:?}");
        if last_membership_observation.as_deref() != Some(state.as_str()) {
            record(observations, format!("partition control-plane state: {state}"));
            last_membership_observation = Some(state.clone());
        }
        last_state.clone_from(&state);
        if connected.is_ok_and(|count| !has_connected_swim_peer(count)) && fresh.is_ok_and(|value| value != Some(true))
        {
            record(
                observations,
                format!(
                    "partition traffic summary: attempts={attempts}, transport_errors={transport_errors}, HTTP statuses={statuses:?}, attributed sites={attributed_sites:?}"
                ),
            );
            wait_for_site_distribution(
                gateway_port,
                GRID_SERVING_SITE,
                "network-partition-remote-site-exclusion",
                attribution,
                Duration::from_secs(60),
            )
            .map(|distribution| {
                record(
                    observations,
                    format!("remote site was excluded while the network remained partitioned: {distribution}"),
                );
            })?;
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(format!(
        "network partition did not make SWIM membership and the remote candidate stale within {timeout:?}: {last_state}"
    )
    .into())
}

/// Read one candidate's current freshness from the operator's distributed overlay.
fn read_overlay_candidate_fresh(
    cluster: &str,
    candidate_cluster: &str,
) -> Result<Option<bool>, Box<dyn std::error::Error>> {
    let context = kind_context(cluster);
    let text = get_configmap_data_key(&context, GRID_SYSTEM_NS, OVERLAY_CONFIGMAP, "routing-config.json")?;
    let overlay: serde_json::Value = serde_json::from_str(&text)?;
    let candidates = overlay
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("operator routing config has no candidates array")?;
    candidates
        .iter()
        .find(|candidate| candidate.get("cluster").and_then(serde_json::Value::as_str) == Some(candidate_cluster))
        .map(|candidate| {
            candidate
                .get("fresh")
                .and_then(serde_json::Value::as_bool)
                .ok_or_else(|| format!("candidate {candidate_cluster} has no freshness field").into())
        })
        .transpose()
}

/// Wait for the healed sites and candidate to return to current state.
fn wait_for_connected_fresh_candidate(
    context: &str,
    remote_cluster: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    let mut last = String::from("not observed");
    while Instant::now() < deadline {
        let connected = read_connected_sites(context).unwrap_or_default();
        let fresh = read_overlay_candidate_fresh(GRID_SERVING_SITE, remote_cluster)
            .ok()
            .flatten();
        last = format!("connectedSites={connected}, remote_candidate_fresh={fresh:?}");
        if has_connected_swim_peer(connected) && fresh == Some(true) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(format!("sites and remote candidate did not reconverge within {timeout:?}: {last}").into())
}

/// GridNetwork.status.connectedSites counts live SWIM members; one peer is healthy here.
pub(super) fn has_connected_swim_peer(connected_sites: u64) -> bool {
    connected_sites >= MIN_CONNECTED_SWIM_MEMBERS
}

/// Wait for a Prometheus counter to advance past a captured value.
fn wait_for_metric_counter_increase(
    port: u16,
    name: &str,
    labels: &[(&str, &str)],
    previous: f64,
    timeout: Duration,
) -> Result<f64, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    let mut last = previous;
    while Instant::now() < deadline {
        if let Ok(metrics) = read_operator_metrics(port) {
            last = metric_counter(&metrics, name, labels);
            if last > previous {
                return Ok(last);
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(format!("{name} did not increase from {previous}; last value={last}").into())
}

/// Resolve the mTLS endpoint used by a run-owned provider gateway.
fn provider_gateway_endpoint(site: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let ctx = kind_context(site);
    let service = kubectl_json(&ctx, "get", "service/provider-gateway")?;
    let ingress = service
        .pointer("/status/loadBalancer/ingress/0")
        .ok_or_else(|| format!("{site} provider-gateway Service has no load balancer address"))?;
    let host = ingress
        .get("ip")
        .or_else(|| ingress.get("hostname"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("{site} provider-gateway Service has no IP or hostname"))?;
    let cluster = format!("llmd-{site}-provider");
    let sni = format!("{site}.grid.internal");
    Ok(serde_json::json!({
        "cluster": cluster,
        "address": format!("{host}:{PROVIDER_GATEWAY_PORT}"),
        "sni": sni,
        "transport": { "mode": "mutual_tls", "sni": sni }
    }))
}

/// Add poll mode and the embedded serving gateway reference to a run-owned GridNetwork.
fn patch_network_for_poll(cluster: &str, endpoints: &[serde_json::Value]) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = kind_context(cluster);
    let network = kubectl_json(&ctx, "get", &format!("gridnetwork/{GRID_NETWORK_NAME}"))?;
    let mut gateway_refs = network
        .pointer("/spec/consumerGateways")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .ok_or_else(|| format!("{cluster}: GridNetwork has no consumerGateways array"))?;
    if cluster == GRID_SERVING_SITE {
        let provider_hop_endpoints: Vec<_> = endpoints
            .iter()
            .map(|endpoint| {
                serde_json::json!({
                    "cluster": endpoint.get("cluster"),
                    "transport": endpoint.get("transport")
                })
            })
            .collect();
        gateway_refs.push(serde_json::json!({
            "name": GRID_SERVING_GATEWAY,
            "namespace": GRID_SYSTEM_NS,
            "localSiteName": GRID_SERVING_SITE,
            "providerHopEndpoints": provider_hop_endpoints
        }));
    }
    let patch = serde_json::json!({
        "spec": {
            "signalTransport": { "mode": "poll" },
            "consumerGateways": gateway_refs
        }
    });
    kubectl_patch_json(&ctx, &format!("gridnetwork/{GRID_NETWORK_NAME}"), &patch)
}

/// Build static provider-hop config, then install the chart-managed Grid Gateway.
fn apply_grid_gateway_config(endpoints: &[serde_json::Value]) -> Result<(), Box<dyn std::error::Error>> {
    let clusters: Vec<_> = endpoints
        .iter()
        .map(|endpoint| {
            serde_json::json!({
                "name": endpoint.get("cluster"),
                "tls": {
                    "ca": { "ca_path": "/etc/praxis/tls/ca.crt" },
                    "client_cert": {
                        "cert_path": "/etc/praxis/tls/tls.crt",
                        "key_path": "/etc/praxis/tls/tls.key"
                    },
                    "sni": endpoint.get("sni"),
                    "verify": true
                },
                "endpoints": [endpoint.get("address")]
            })
        })
        .collect();
    let config = serde_json::json!({
        "insecure_options": {
            "allow_private_endpoints": true,
            "allow_private_upstreams": true
        },
        "listeners": [{
            "name": "proxy",
            "address": "0.0.0.0:8080",
            "filter_chains": ["main"]
        }],
        "filter_chains": [{
            "name": "main",
            "filters": [
                { "filter": "model_to_header", "header": "X-Gateway-Model-Name" },
                { "filter": "grid_site_route", "model_header": "X-Gateway-Model-Name" },
                { "filter": "load_balancer", "clusters": clusters }
            ]
        }],
        "admin": { "address": "127.0.0.1:9901" },
        "shutdown_timeout_secs": 5
    });
    let config_yaml = serde_yaml::to_string(&config)?;
    let config_map = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "grid-serving-gateway-config",
            "namespace": GRID_SYSTEM_NS,
            "labels": { "grid.praxis-proxy.io/run-prefix": run_cluster_prefix() }
        },
        "data": { "praxis.yaml": config_yaml }
    });
    apply_json(&kind_context(GRID_SERVING_SITE), &config_map)
}

/// Install the embedded Grid Gateway with a TLS metrics listener for proof evidence.
fn install_grid_gateway(image: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (repository, tag) = split_image_reference(image)?;
    let chart = crate::workspace_root().join("charts/praxis-gateway");
    let chart = chart.to_str().ok_or("Praxis Gateway chart path is not UTF-8")?;
    let kube_context = kind_context(GRID_SERVING_SITE);
    let mut command = Command::new("helm");
    command.args([
        "upgrade",
        "--install",
        GRID_SERVING_GATEWAY,
        chart,
        "--kube-context",
        &kube_context,
        "--namespace",
        GRID_SYSTEM_NS,
        "--wait",
        "--timeout",
        "180s",
    ]);
    for value in [
        format!("fullnameOverride={GRID_SERVING_GATEWAY}"),
        format!("image.repository={repository}"),
        format!("image.tag={tag}"),
        "image.flavor=grid-gateway".to_owned(),
        "image.pullPolicy=Never".to_owned(),
        "config.existingConfigMap=grid-serving-gateway-config".to_owned(),
        "service.type=ClusterIP".to_owned(),
        format!("gridServing.network={GRID_NETWORK_NAME}"),
        format!("gridServing.gatewayRef={GRID_SERVING_GATEWAY}"),
        format!("gridServing.configMap={GRID_SERVING_CONFIGMAP}"),
        format!("tls.existingSecret={CONSUMER_TLS_SECRET}"),
        format!("tls.caSecret={CONSUMER_TLS_SECRET}"),
        "networkPolicy.from[0].namespaceSelector.matchLabels.kubernetes\\.io/metadata\\.name=grid-system".to_owned(),
        "metricsListener.existingSecret=consumer-gateway-tls".to_owned(),
        "metricsListener.fromNamespaces[0]=grid-system".to_owned(),
    ] {
        command.args(["--set-string", &value]);
    }
    command.args([
        "--set",
        "gridServing.enabled=true",
        "--set",
        "tls.enabled=true",
        "--set",
        "networkPolicy.enabled=true",
        "--set",
        "metricsListener.enabled=true",
    ]);
    let output = command.output()?;
    if !output.status.success() {
        return Err(format!(
            "could not install the run-owned Grid Gateway: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(())
}

/// Wait for the operator's serving ConfigMap to publish both provider candidates.
fn wait_for_grid_serving_snapshot(timeout: Duration) -> Result<String, Box<dyn std::error::Error>> {
    let context = kind_context(GRID_SERVING_SITE);
    let deadline = Instant::now() + timeout;
    let expected = ["llmd-pool-a-provider", "llmd-pool-b-provider"];
    loop {
        if let Ok(config_map) = kubectl_json(&context, "get", &format!("configmap/{GRID_SERVING_CONFIGMAP}"))
            && let Some(text) = config_map
                .pointer("/data/serving-config.json")
                .and_then(serde_json::Value::as_str)
            && let Ok(snapshot) = serde_json::from_str::<serde_json::Value>(text)
        {
            let candidates = snapshot
                .get("candidates")
                .and_then(serde_json::Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.get("cluster").and_then(serde_json::Value::as_str))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let local_site = snapshot.get("local_site").and_then(serde_json::Value::as_str);
            let local_peers = snapshot
                .get("peers")
                .and_then(serde_json::Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let local_operator_peer =
                local_peers.len() == 1 && local_peers.first().is_some_and(is_expected_local_operator_peer);
            if local_site == Some(GRID_SERVING_SITE)
                && local_operator_peer
                && expected.iter().all(|cluster| candidates.contains(cluster))
            {
                let peer = local_peers.first().ok_or("validated local operator peer disappeared")?;
                return Ok(serde_json::json!({
                    "site": peer.get("site"),
                    "address": peer.get("addr"),
                    "path": peer.get("path"),
                    "interval_ms": peer.get("interval_ms"),
                    "mtls_identity": peer.get("server_name"),
                    "candidate_clusters": candidates,
                })
                .to_string());
            }
        }
        if Instant::now() >= deadline {
            return Err("timed out waiting for the Grid serving snapshot with both llm-d providers".into());
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// Read the exact site/cluster pairs still published to the Grid Gateway.
fn serving_candidate_pairs(context: &str) -> ServingCandidatePairsResult {
    let serving_config =
        get_configmap_data_key(context, GRID_SYSTEM_NS, GRID_SERVING_CONFIGMAP, "serving-config.json")?;
    let snapshot: serde_json::Value = serde_json::from_str(&serving_config)?;
    let candidates = snapshot
        .pointer("/candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("Grid Gateway serving configuration has no candidate array")?;
    let mut pairs = BTreeSet::new();
    for candidate in candidates {
        let site = candidate
            .get("site")
            .and_then(serde_json::Value::as_str)
            .ok_or("Grid Gateway serving candidate has no site")?;
        let cluster = candidate
            .get("cluster")
            .and_then(serde_json::Value::as_str)
            .ok_or("Grid Gateway serving candidate has no cluster")?;
        pairs.insert((site.to_owned(), cluster.to_owned()));
    }
    Ok(pairs)
}

/// Require the deployed Gateway to expose the metrics produced by an active EPP reading.
fn require_active_signal_gauges(
    metrics: &str,
    site: &str,
    cluster: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut values = Vec::with_capacity(ACTIVE_SIGNAL_GAUGES.len());
    for name in ACTIVE_SIGNAL_GAUGES {
        let value = sample_value(metrics, name, &[("site", site), ("cluster", cluster)])
            .filter(|value| value.is_finite())
            .ok_or_else(|| format!("Grid Gateway has no finite {name} gauge for ({site}, {cluster})"))?;
        values.push(format!("{name}={value:.3}"));
    }
    Ok(values.join(", "))
}

/// Require NaN samples for every measured gauge after a candidate's EPP reading disappears.
fn measured_site_gauges_are_cleared(metrics: &str, site: &str, cluster: &str) -> bool {
    MEASURED_SITE_GAUGES.iter().all(|name| {
        sample_value(metrics, name, &[("site", site), ("cluster", cluster)]).is_some_and(|value| !value.is_finite())
    })
}

/// Render all measured gauge samples for timeout diagnostics and qualification evidence.
fn measured_site_gauge_summary(metrics: &str, site: &str, cluster: &str) -> String {
    MEASURED_SITE_GAUGES
        .iter()
        .map(|name| {
            format!(
                "{name}={:?}",
                sample_value(metrics, name, &[("site", site), ("cluster", cluster)])
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Wait for every measured gauge to clear while the candidate remains published.
fn wait_for_measured_site_gauges_cleared(
    port: u16,
    site: &str,
    cluster: &str,
    timeout: Duration,
) -> Result<String, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    let mut last = String::from("Gateway metrics unavailable");
    while Instant::now() < deadline {
        match read_gateway_metrics(port) {
            Ok(metrics) => {
                last = measured_site_gauge_summary(&metrics, site, cluster);
                if measured_site_gauges_are_cleared(&metrics, site, cluster) {
                    return Ok(last);
                }
            },
            Err(error) => last = error.to_string(),
        }
        std::thread::sleep(Duration::from_secs(3));
    }
    Err(format!(
        "timed out waiting for Grid Gateway measured gauges to clear for ({site}, {cluster}); last values: {last}"
    )
    .into())
}

/// Whether one serving peer is exactly the Gateway's local operator mTLS poll target.
fn is_expected_local_operator_peer(peer: &serde_json::Value) -> bool {
    let identity = format!("{GRID_SERVING_SITE}.grid.internal");
    peer.get("site").and_then(serde_json::Value::as_str) == Some(GRID_SERVING_SITE)
        && peer
            .get("addr")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|address| !address.is_empty())
        && peer.get("server_name").and_then(serde_json::Value::as_str) == Some(identity.as_str())
        && peer.get("authority").and_then(serde_json::Value::as_str) == Some(identity.as_str())
        && peer.get("path").and_then(serde_json::Value::as_str) == Some("/v1/site/signals")
        && peer.get("interval_ms").and_then(serde_json::Value::as_u64) == Some(5_000)
        && peer
            .get("grid_ca_path")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|path| path.ends_with("/ca.crt"))
        && peer
            .get("client_cert_path")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|path| path.ends_with("/tls.crt"))
        && peer
            .get("client_key_path")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|path| path.ends_with("/tls.key"))
}

/// Wait until the deployed gateway watcher applies or reuses its mounted serving config.
fn wait_for_gateway_config_observed(port: u16, timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    let mut applied = 0.0;
    let mut reused = 0.0;
    while Instant::now() < deadline {
        if let Ok(metrics) = read_gateway_metrics(port) {
            applied = applied_gateway_reload_count_from_text(&metrics);
            reused = reused_gateway_config_count_from_text(&metrics);
            if applied + reused > 0.0 {
                return Ok(());
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(format!(
        "Grid Gateway watcher did not observe its serving config; applied reloads={applied}, reused configs={reused}"
    )
    .into())
}

/// Return the gateway's accepted serving-config reload count.
fn applied_gateway_reload_count(port: u16) -> Result<f64, Box<dyn std::error::Error>> {
    Ok(applied_gateway_reload_count_from_text(&read_gateway_metrics(port)?))
}

/// Parse the accepted serving-config reload counter from gateway metrics.
fn applied_gateway_reload_count_from_text(metrics: &str) -> f64 {
    metric_counter(metrics, "grid_serving_config_reload_total", &[("result", "applied")])
}

/// Parse the count of mounted serving configs the watcher found unchanged.
fn reused_gateway_config_count_from_text(metrics: &str) -> f64 {
    metric_counter(metrics, "grid_serving_config_reload_total", &[("result", "reused")])
}

/// Read the GridNetwork's number of active SWIM sites.
pub(super) fn read_connected_sites(context: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let network = kubectl_json(context, "get", &format!("gridnetwork/{GRID_NETWORK_NAME}"))?;
    network
        .pointer("/status/connectedSites")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "GridNetwork status has no connectedSites count".into())
}

/// Wait for the exact GridSite phase and reason, retaining the resource version for diagnostics.
fn wait_for_gridsite_phase_and_reason(
    context: &str,
    site_name: &str,
    expected_phase: &str,
    expected_reason: &str,
    timeout: Duration,
) -> Result<(String, String, String), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    let mut last = ("unknown".to_owned(), "unknown".to_owned(), "unknown".to_owned());
    while Instant::now() < deadline {
        let site = kubectl_json(context, "get", &format!("gridsite/{site_name}"))?;
        let phase = site
            .pointer("/status/phase")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let reason = site
            .pointer("/status/reason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let resource_version = site
            .pointer("/metadata/resourceVersion")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        last = (phase, reason, resource_version);
        if last.0 == expected_phase && last.1 == expected_reason {
            return Ok(last);
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    Err(format!(
        "timed out waiting for GridSite {site_name} phase={expected_phase:?} reason={expected_reason:?}; last observed phase={:?} reason={:?} resourceVersion={:?}",
        last.0, last.1, last.2
    )
    .into())
}

/// Add a UTC timestamp to one structured proof observation.
fn record(observations: &mut Vec<String>, message: impl Into<String>) {
    let message = message.into();
    let timestamped = format!("{} {message}", format_utc_iso());
    eprintln!("[CROSS-SITE] {timestamped}");
    observations.push(timestamped);
}

/// Wait until the local operator exports its own and its peer's live EPP signals.
fn wait_for_relay_samples(port: u16, timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    let mut last = String::from("no operator metrics collected");
    while Instant::now() < deadline {
        if let Ok(metrics) = read_operator_metrics(port) {
            let local = sample_value(
                &metrics,
                "grid_provider_in_flight_requests",
                &[("grid_site", "pool-a"), ("grid_provider", "llmd-pool-a-provider")],
            );
            let peer = sample_value(
                &metrics,
                "grid_provider_in_flight_requests",
                &[("grid_site", "pool-b"), ("grid_provider", "llmd-pool-b-provider")],
            );
            let polls = metric_counter(
                &metrics,
                "grid_peer_poll_total",
                &[("peer", "pool-b"), ("outcome", "ok")],
            );
            last = format!("local={local:?}, relayed_peer={peer:?}, peer_poll_success={polls}");
            if local.is_some() && peer.is_some() && polls > 0.0 {
                return Ok(());
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(format!("timed out waiting for local scrape and relayed peer samples: {last}").into())
}

/// Gateway metrics used by its two-site capacity-weighted selector.
#[derive(Clone, Copy, Debug)]
struct WeightedRouteMetrics {
    /// Queue score of the lower-pressure site.
    preferred_score: f64,
    /// Queue score of the other site.
    other_score: f64,
    /// Capacity weight of the lower-pressure site.
    preferred_weight: f64,
    /// Capacity weight of the other site.
    other_weight: f64,
    /// Smoothed saturation of the lower-pressure site.
    preferred_rho: f64,
    /// Smoothed saturation of the other site.
    other_rho: f64,
    /// Expected share for the lower-pressure site from Grid's two-site formula.
    preferred_share: f64,
}

/// Gateway selector paths observed before and after one two-site routing sample.
#[derive(Clone, Copy, Debug)]
struct RouteSelectionCounts {
    /// Requests selected through the weighted two-site path.
    between_two: f64,
    /// Requests selected through the two-choice path for at least three candidates.
    two_choices: f64,
    /// Requests selected because only one candidate had room.
    lone: f64,
    /// Requests selected from the fallback capacity band.
    by_capacity: f64,
}

/// Read the live selector path counters from the Grid Gateway.
fn route_selection_counts(metrics: &str) -> Result<RouteSelectionCounts, String> {
    let counter = |path: &str| {
        sample_value(metrics, "grid_route_selections_total", &[("path", path)])
            .filter(|value| value.is_finite() && *value >= 0.0)
            .ok_or_else(|| format!("Gateway has no finite selector counter for path={path}"))
    };
    Ok(RouteSelectionCounts {
        between_two: counter("between_two")?,
        two_choices: counter("two_choices")?,
        lone: counter("lone")?,
        by_capacity: counter("by_capacity")?,
    })
}

/// Require every response in a two-site sample to use the weighted selector path.
fn weighted_selector_sample_delta(
    before: RouteSelectionCounts,
    after: RouteSelectionCounts,
    requests: usize,
) -> Result<String, String> {
    let delta = RouteSelectionCounts {
        between_two: after.between_two - before.between_two,
        two_choices: after.two_choices - before.two_choices,
        lone: after.lone - before.lone,
        by_capacity: after.by_capacity - before.by_capacity,
    };
    let expected = f64::from(
        u32::try_from(requests).map_err(|error| format!("request count does not fit a metric sample: {error}"))?,
    );
    if (delta.between_two - expected).abs() > f64::EPSILON
        || delta.two_choices.abs() > f64::EPSILON
        || delta.lone.abs() > f64::EPSILON
        || delta.by_capacity.abs() > f64::EPSILON
    {
        return Err(format!(
            "Gateway selector counters do not match {requests} requests through the between_two path: before={before:?}, after={after:?}, delta={delta:?}"
        ));
    }
    Ok(format!("selector path delta={delta:?}"))
}

/// Wait for Gateway pressure metrics, then compare one attributed sample to the merged weighted selector.
fn verify_weighted_site_distribution(
    gateway_port: u16,
    metrics_port: u16,
    expected_site: &str,
    require_strict_pressure_preference: bool,
    phase: &str,
    attribution: &mut RoutingAttributionDiagnostics,
    timeout: Duration,
) -> Result<String, Box<dyn std::error::Error>> {
    let warmup = route_distribution(gateway_port, 1, phase, attribution)?;
    if warmup.sites.values().sum::<usize>() != 1 {
        return Err(format!("weighted route warmup did not return one attributed response: {warmup}").into());
    }

    let deadline = Instant::now() + timeout;
    let mut last = String::from("Gateway route metrics unavailable");
    let mut previous: Option<WeightedRouteMetrics> = None;
    let mut stable_samples = 0_u32;
    let mut pre_sample_retries = 0_u32;
    while Instant::now() < deadline {
        match read_gateway_metrics(metrics_port).and_then(|metrics| {
            weighted_route_metrics(&metrics, expected_site, require_strict_pressure_preference).map_err(Into::into)
        }) {
            Ok(current) => {
                last = format!(
                    "expected site score={:.3}, weight={:.3}, rho={:.6}, weighted share={:.1}%; other site score={:.3}, weight={:.3}, rho={:.6}",
                    current.preferred_score,
                    current.preferred_weight,
                    current.preferred_rho,
                    current.preferred_share * 100.0,
                    current.other_score,
                    current.other_weight,
                    current.other_rho
                );
                if previous.is_some_and(|before| weighted_route_metrics_stable(before, current)) {
                    stable_samples = stable_samples.saturating_add(1);
                } else {
                    stable_samples = 1;
                }
                previous = Some(current);
                if stable_samples >= 2 {
                    let before_snapshot: Result<_, Box<dyn std::error::Error>> = read_gateway_metrics(metrics_port)
                        .and_then(|metrics| {
                            let state =
                                weighted_route_metrics(&metrics, expected_site, require_strict_pressure_preference)
                                    .map_err(Box::<dyn std::error::Error>::from)?;
                            let counts =
                                route_selection_counts(&metrics).map_err(Box::<dyn std::error::Error>::from)?;
                            Ok((state, counts))
                        });
                    let (before_state, before_counts) = match before_snapshot {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            pre_sample_retries = pre_sample_retries.saturating_add(1);
                            last = format!(
                                "pre-sample route metrics unavailable on recheck {pre_sample_retries}: {error}"
                            );
                            previous = None;
                            stable_samples = 0;
                            std::thread::sleep(Duration::from_secs(2));
                            continue;
                        },
                    };
                    if !weighted_route_metrics_stable(current, before_state) {
                        pre_sample_retries = pre_sample_retries.saturating_add(1);
                        last = format!("Gateway route metrics changed during pre-sample recheck {pre_sample_retries}");
                        previous = None;
                        stable_samples = 0;
                        std::thread::sleep(Duration::from_secs(2));
                        continue;
                    }
                    let route_state = format!(
                        "scores=({:.3}, {:.3}), weights=({:.3}, {:.3}), rho=({:.6}, {:.6}), predicted share={:.1}%",
                        before_state.preferred_score,
                        before_state.other_score,
                        before_state.preferred_weight,
                        before_state.other_weight,
                        before_state.preferred_rho,
                        before_state.other_rho,
                        before_state.preferred_share * 100.0
                    );
                    let mut distribution = RouteDistribution::default();
                    let mut expected_share_windows = Vec::new();
                    let mut observed_metric_windows = Vec::new();
                    let mut requests_remaining = WEIGHTED_ROUTE_SAMPLE_REQUESTS;
                    let mut after_metrics = None;
                    while requests_remaining > 0 {
                        let batch_requests = requests_remaining.min(WEIGHTED_ROUTE_SAMPLE_BATCH_REQUESTS);
                        let before_metrics = read_gateway_metrics(metrics_port)?;
                        let before_batch =
                            weighted_route_metrics(&before_metrics, expected_site, require_strict_pressure_preference)?;
                        let batch = route_distribution(gateway_port, batch_requests, phase, attribution)?;
                        distribution.merge(batch);
                        let current_metrics = read_gateway_metrics(metrics_port)?;
                        let after_batch = weighted_route_metrics(
                            &current_metrics,
                            expected_site,
                            require_strict_pressure_preference,
                        )?;
                        expected_share_windows.push((
                            batch_requests,
                            before_batch.preferred_share,
                            after_batch.preferred_share,
                        ));
                        observed_metric_windows.push(format!(
                            "{batch_requests} requests: before={before_batch:?}, after={after_batch:?}"
                        ));
                        requests_remaining -= batch_requests;
                        after_metrics = Some(current_metrics);
                    }
                    let expected_share = weighted_route_expected_share(&expected_share_windows).map_err(|error| {
                        format!("{error}; {route_state}; {observed_metric_windows:?}; {distribution}")
                    })?;
                    let after_metrics = after_metrics.ok_or("weighted route sample produced no metric windows")?;
                    let after_counts = route_selection_counts(&after_metrics)?;
                    let selector_evidence =
                        weighted_selector_sample_delta(before_counts, after_counts, WEIGHTED_ROUTE_SAMPLE_REQUESTS)
                            .map_err(|error| format!("{error}; {route_state}; {distribution}"))?;
                    let total = distribution.sites.values().sum::<usize>();
                    if total != WEIGHTED_ROUTE_SAMPLE_REQUESTS {
                        return Err(format!(
                            "weighted route sample returned {total} attributed responses, expected {WEIGHTED_ROUTE_SAMPLE_REQUESTS}; {route_state}; {selector_evidence}; {distribution}"
                        )
                        .into());
                    }
                    let preferred = distribution.sites.get(expected_site).copied().unwrap_or_default();
                    let total = u32::try_from(total)
                        .map_err(|error| format!("weighted route count does not fit a metric sample: {error}"))?;
                    let preferred = u32::try_from(preferred)
                        .map_err(|error| format!("preferred route count does not fit a metric sample: {error}"))?;
                    let observed_share = f64::from(preferred) / f64::from(total);
                    let share_error = (observed_share - expected_share).abs();
                    if !weighted_route_share_matches(expected_share, observed_share) {
                        return Err(format!(
                            "observed {expected_site} route share {:.1}% differs from the metric-window-weighted Gateway prediction {:.1}% by {:.1} percentage points (limit {:.1}); {route_state}; metric windows={observed_metric_windows:?}; {selector_evidence}; {} requests: {distribution}",
                            observed_share * 100.0,
                            expected_share * 100.0,
                            share_error * 100.0,
                            WEIGHTED_ROUTE_SHARE_TOLERANCE * 100.0,
                            WEIGHTED_ROUTE_SAMPLE_REQUESTS
                        )
                        .into());
                    }
                    return Ok(format!(
                        "lower-pressure site={expected_site}; metric-window-weighted predicted share={:.1}%, observed={:.1}%, error={:.1} percentage points; scores=({:.3}, {:.3}), weights=({:.3}, {:.3}), rho=({:.6}, {:.6}); pre-sample metric retries={pre_sample_retries}; metric windows={observed_metric_windows:?}; {selector_evidence}; {distribution}",
                        expected_share * 100.0,
                        observed_share * 100.0,
                        share_error * 100.0,
                        before_state.preferred_score,
                        before_state.other_score,
                        before_state.preferred_weight,
                        before_state.other_weight,
                        before_state.preferred_rho,
                        before_state.other_rho
                    ));
                }
            },
            Err(error) => {
                last = error.to_string();
                previous = None;
                stable_samples = 0;
            },
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(format!(
        "Gateway did not expose a stable queue-score and pressure order with {expected_site} preferred within {timeout:?}: {last}"
    )
    .into())
}

/// Read the expected two-site selection share from the Gateway's current route snapshot metrics.
fn weighted_route_metrics(
    metrics: &str,
    preferred_site: &str,
    require_strict_pressure_preference: bool,
) -> Result<WeightedRouteMetrics, String> {
    let other_site = match preferred_site {
        "pool-a" => "pool-b",
        "pool-b" => "pool-a",
        _ => return Err(format!("unknown expected site in the two-site proof: {preferred_site}")),
    };
    let pair = |site: &str| {
        let cluster = format!("llmd-{site}-provider");
        let labels = [("site", site), ("cluster", cluster.as_str())];
        let metric = |name| {
            sample_value(metrics, name, &labels)
                .filter(|value| value.is_finite())
                .ok_or_else(|| {
                    format!(
                        "Gateway has no finite {name} metric for ({site}, {cluster}); current route metrics: {}",
                        route_metric_diagnostics(metrics)
                    )
                })
        };
        Ok::<_, String>((
            metric("grid_route_site_score")?,
            metric("grid_route_site_weight")?,
            metric("grid_route_site_rho")?,
        ))
    };
    let (preferred_score, preferred_weight, preferred_rho) = pair(preferred_site)?;
    let (other_score, other_weight, other_rho) = pair(other_site)?;
    let score_order_invalid = if require_strict_pressure_preference {
        preferred_score >= other_score
    } else {
        preferred_score > other_score
    };
    if score_order_invalid {
        let relation = if require_strict_pressure_preference {
            "is not strictly lower than"
        } else {
            "is higher than"
        };
        return Err(format!(
            "Gateway route score for {preferred_site} {relation} the other site: score={preferred_score}, {other_site} score={other_score}"
        ));
    }
    if preferred_weight <= 0.0 || other_weight <= 0.0 || preferred_rho < 0.0 || other_rho < 0.0 {
        return Err(format!(
            "Gateway route metrics are outside the weighted selector domain: {preferred_site} weight={preferred_weight}, rho={preferred_rho}; {other_site} weight={other_weight}, rho={other_rho}"
        ));
    }
    if require_strict_pressure_preference && preferred_rho >= other_rho {
        return Err(format!(
            "Gateway pressure rho for {preferred_site} is not strictly lower than {other_site}: rho={preferred_rho}, other rho={other_rho}"
        ));
    }
    // Keep this expectation aligned with `between_two` in the Grid Gateway filter.
    let preferred_rate = preferred_weight / (1.0 + preferred_rho);
    let other_rate = other_weight / (1.0 + other_rho);
    let preferred_share =
        (preferred_rate / (preferred_rate + other_rate)).clamp(TWO_SITE_SHARE_RANGE.0, TWO_SITE_SHARE_RANGE.1);
    Ok(WeightedRouteMetrics {
        preferred_score,
        other_score,
        preferred_weight,
        other_weight,
        preferred_rho,
        other_rho,
        preferred_share,
    })
}

/// Preserve the current two-site route gauges when a weighted sample is incomplete.
fn route_metric_diagnostics(metrics: &str) -> String {
    let samples = metrics
        .lines()
        .filter(|line| {
            line.starts_with("grid_route_site_")
                && (line.contains("site=\"pool-a\"") || line.contains("site=\"pool-b\""))
        })
        .collect::<Vec<_>>();
    if samples.is_empty() {
        "<no grid_route_site_* samples>".to_owned()
    } else {
        samples.join("; ")
    }
}

/// Whether repeated Gateway metrics describe the same weighted selection state.
fn weighted_route_metrics_stable(before: WeightedRouteMetrics, after: WeightedRouteMetrics) -> bool {
    [
        (before.preferred_score, after.preferred_score),
        (before.other_score, after.other_score),
        (before.preferred_weight, after.preferred_weight),
        (before.other_weight, after.other_weight),
        (before.preferred_rho, after.preferred_rho),
        (before.other_rho, after.other_rho),
        (before.preferred_share, after.preferred_share),
    ]
    .into_iter()
    .all(|(left, right)| (left - right).abs() <= f64::EPSILON)
}

/// Whether an attributed sample is within the explicit tolerance of the Gateway's weighted choice.
fn weighted_route_share_matches(expected: f64, observed: f64) -> bool {
    (expected - observed).abs() <= WEIGHTED_ROUTE_SHARE_TOLERANCE
}

/// Estimate the preferred route share over request-count-weighted metric windows.
fn weighted_route_expected_share(windows: &[(usize, f64, f64)]) -> Result<f64, String> {
    let mut total_requests = 0_u32;
    let mut expected_requests = 0.0;
    for (requests, before, after) in windows {
        if *requests == 0
            || !before.is_finite()
            || !after.is_finite()
            || !(0.0..=1.0).contains(before)
            || !(0.0..=1.0).contains(after)
        {
            return Err(format!(
                "weighted route sample contains an invalid metric window: requests={requests}, before={before}, after={after}"
            ));
        }
        let request_count = u32::try_from(*requests)
            .map_err(|error| format!("weighted route batch count does not fit a metric sample: {error}"))?;
        total_requests = total_requests
            .checked_add(request_count)
            .ok_or("weighted route request count overflowed its metric sample")?;
        expected_requests += f64::from(request_count) * ((before + after) / 2.0);
    }
    if total_requests == 0 {
        return Err("weighted route sample contains no metric windows".to_owned());
    }
    Ok(expected_requests / f64::from(total_requests))
}

/// Wait for a strong route preference while retaining each actual HTTP attribution.
fn wait_for_site_distribution(
    port: u16,
    expected_site: &str,
    phase: &str,
    attribution: &mut RoutingAttributionDiagnostics,
    timeout: Duration,
) -> Result<String, Box<dyn std::error::Error>> {
    const REQUESTS: usize = 16;
    const REQUIRED: usize = 12;
    let deadline = Instant::now() + timeout;
    let mut last = RouteDistribution::default();
    while Instant::now() < deadline {
        last = route_distribution(port, REQUESTS, phase, attribution)?;
        if last.proves_site(expected_site, REQUIRED) {
            return Ok(last.to_string());
        }
        std::thread::sleep(Duration::from_secs(3));
    }
    Err(
        format!("expected {expected_site} for at least {REQUIRED}/{REQUESTS} requests; last distribution: {last}")
            .into(),
    )
}

/// Wait for the operator's relayed EPP values to reflect the requested low-pressure site.
fn wait_for_pressure_order(
    port: u16,
    lower_pressure_site: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    let (expected_lower, expected_higher) = if lower_pressure_site == "pool-a" {
        ("pool-a", "pool-b")
    } else {
        ("pool-b", "pool-a")
    };
    let mut last = String::from("operator samples unavailable");
    while Instant::now() < deadline {
        if let Ok(metrics) = read_operator_metrics(port) {
            let a = sample_value(
                &metrics,
                "grid_provider_in_flight_requests",
                &[("grid_site", "pool-a"), ("grid_provider", "llmd-pool-a-provider")],
            );
            let b = sample_value(
                &metrics,
                "grid_provider_in_flight_requests",
                &[("grid_site", "pool-b"), ("grid_provider", "llmd-pool-b-provider")],
            );
            last = format!("pool-a={a:?}, pool-b={b:?}");
            let (low, high) = if expected_lower == "pool-a" { (a, b) } else { (b, a) };
            if low.zip(high).is_some_and(|(low, high)| low < high) {
                return Ok(());
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(format!(
        "operator did not relay the expected pressure order ({expected_lower} below {expected_higher}): {last}"
    )
    .into())
}

/// Read a provider's in-flight sample from operator exposition.
fn provider_in_flight_value(metrics: &str, site: &str) -> Option<f64> {
    let provider = format!("llmd-{site}-provider");
    sample_value(
        metrics,
        "grid_provider_in_flight_requests",
        &[("grid_site", site), ("grid_provider", provider.as_str())],
    )
}

/// Wait for a provider's in-flight signal to decrease below its previous value.
fn wait_for_signal_decrease(
    port: u16,
    site: &str,
    previous: f64,
    timeout: Duration,
) -> Result<f64, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    let mut last = None;
    while Instant::now() < deadline {
        if let Ok(metrics) = read_operator_metrics(port) {
            last = provider_in_flight_value(&metrics, site);
            if last.is_some_and(|value| value < previous) {
                return last.ok_or_else(|| "in-flight signal disappeared after matching decrease".into());
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(format!("{site} in-flight signal did not decrease below {previous}; last sample={last:?}").into())
}

/// Wait for a stable pressure window where the changed peer sample would alter
/// selection, while the last trusted sample still prefers the local site.
fn wait_for_wrong_pin_signal_window(
    local_port: u16,
    peer_port: u16,
    peer: &str,
    previous_origin: f64,
    trusted_sample: f64,
    timeout: Duration,
) -> WrongPinSignalWindow {
    let deadline = Instant::now() + timeout;
    let mut result = WrongPinSignalWindow::default();
    let mut previous_window: Option<(f64, f64, f64)> = None;
    while Instant::now() < deadline {
        if let (Ok(local_metrics), Ok(peer_metrics)) =
            (read_operator_metrics(local_port), read_operator_metrics(peer_port))
        {
            let local = provider_in_flight_value(&local_metrics, "pool-a");
            let trusted = provider_in_flight_value(&local_metrics, peer);
            let origin = provider_in_flight_value(&peer_metrics, peer);
            if let Some(trusted) = trusted
                && (trusted - trusted_sample).abs() > f64::EPSILON
            {
                result.trusted_changed = true;
            }
            result.local_a = local;
            result.trusted_b = trusted;
            result.origin_b = origin;

            if let (Some(local), Some(trusted), Some(origin)) = (local, trusted, origin)
                && !result.trusted_changed
                && origin < previous_origin
                && wrong_pin_signal_window(local, trusted, origin)
            {
                let current_window = (local, trusted, origin);
                if previous_window.as_ref() == Some(&current_window) {
                    result.stable = true;
                    return result;
                }
                previous_window = Some(current_window);
            } else {
                previous_window = None;
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    result
}

/// The changed signal must prefer the remote site while the trusted signal prefers local.
fn wrong_pin_signal_window(local: f64, trusted: f64, changed: f64) -> bool {
    changed < local && local < trusted
}

/// Wait until the scaled-down site's local EPP sample expires while peer polling remains active.
fn wait_for_operator_signal_absence(
    port: u16,
    site: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let provider = format!("llmd-{site}-provider");
    let deadline = Instant::now() + timeout;
    let mut last = String::from("operator metrics unavailable");
    while Instant::now() < deadline {
        if let Ok(metrics) = read_operator_metrics(port) {
            let sample = sample_value(
                &metrics,
                "grid_provider_in_flight_requests",
                &[("grid_site", site), ("grid_provider", &provider)],
            );
            let peer_success = metric_counter(
                &metrics,
                "grid_peer_poll_total",
                &[("peer", "pool-b"), ("outcome", "ok")],
            );
            last = format!("{site} sample={sample:?}, peer-poll success={peer_success}");
            if sample.is_none() && peer_success > 0.0 {
                return Ok(());
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(format!("the {site} EPP sample did not expire while peer polling remained live: {last}").into())
}

/// Poll failures that can report a peer-identity rejection across supported HTTP/TLS adapters.
#[derive(Clone, Copy, Debug, Default)]
struct PeerPollFailureCounts {
    /// Failures classified as a TLS error.
    tls: f64,
    /// Failures whose wrapped transport error does not expose its TLS source type.
    transport: f64,
    /// Non-success HTTP responses from the peer.
    client_error: f64,
}

impl PeerPollFailureCounts {
    /// Whether an identity-related poll failure occurred after the snapshot.
    fn advanced_from(self, previous: Self) -> bool {
        self.tls > previous.tls || self.transport > previous.transport || self.client_error > previous.client_error
    }

    /// Format the observed counter changes without hiding their classification.
    fn delta_from(self, previous: Self) -> String {
        format!(
            "tls={}->{} transport={}->{} client_error={}->{}",
            previous.tls, self.tls, previous.transport, self.transport, previous.client_error, self.client_error
        )
    }
}

/// Read identity-relevant failed poll outcomes for one peer.
fn peer_poll_failure_counts(metrics: &str, peer: &str) -> PeerPollFailureCounts {
    let count = |outcome| metric_counter(metrics, "grid_peer_poll_total", &[("peer", peer), ("outcome", outcome)]);
    PeerPollFailureCounts {
        tls: count("tls"),
        transport: count("transport"),
        client_error: count("client_error"),
    }
}

/// Wait for a poll failure after replacing the peer's trusted fingerprint.
///
/// Some HTTP/TLS adapters retain the TLS rejection only as a generic transport
/// error. The proof records that exact label and separately verifies that the
/// changed remote signal never replaces the last trusted value or route.
fn wait_for_peer_identity_rejection(
    port: u16,
    peer: &str,
    previous: PeerPollFailureCounts,
    timeout: Duration,
) -> Result<PeerPollFailureCounts, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    let mut last = previous;
    while Instant::now() < deadline {
        let metrics = read_operator_metrics(port)?;
        last = peer_poll_failure_counts(&metrics, peer);
        if last.advanced_from(previous) {
            return Ok(last);
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(format!(
        "pool-a recorded no TLS, transport, or client-error poll after pool-b's pin changed: {}",
        last.delta_from(previous)
    )
    .into())
}

/// A batch of real request attributions from the Grid Gateway.
#[derive(Default)]
pub(super) struct RouteDistribution {
    /// Requests attributed to each site.
    pub(super) sites: BTreeMap<String, usize>,
    /// Requests attributed to each provider cluster.
    pub(super) backends: BTreeMap<String, usize>,
    /// Requests whose response came from the trusted provider gateway at each site.
    pub(super) provider_gateways: BTreeMap<String, usize>,
}

impl RouteDistribution {
    /// Retain a successful response's provider marker and any Grid attribution headers.
    pub(super) fn observe(&mut self, response: &SiteRouteResponse) -> Result<(), String> {
        let provider_site = response.provider_site.clone();
        let site = provider_site.clone().or_else(|| response.site.clone()).ok_or_else(|| {
            "Grid Gateway response had neither a trusted provider marker nor x-grid-site attribution".to_owned()
        })?;
        *self.sites.entry(site).or_default() += 1;
        if let Some(provider_site) = provider_site {
            *self.provider_gateways.entry(provider_site).or_default() += 1;
        }
        if let Some(backend) = &response.backend {
            *self.backends.entry(backend.clone()).or_default() += 1;
        }
        Ok(())
    }

    /// Add counts from another attributed request batch.
    fn merge(&mut self, other: Self) {
        for (site, count) in other.sites {
            *self.sites.entry(site).or_default() += count;
        }
        for (backend, count) in other.backends {
            *self.backends.entry(backend).or_default() += count;
        }
        for (site, count) in other.provider_gateways {
            *self.provider_gateways.entry(site).or_default() += count;
        }
    }

    /// Whether enough responses name the expected site through a trusted provider marker or both Grid headers.
    fn proves_site(&self, expected_site: &str, required: usize) -> bool {
        let from_provider = self.provider_gateways.get(expected_site).copied().unwrap_or_default() >= required;
        let expected_backend = format!("llmd-{expected_site}-provider");
        let from_grid_headers = self.sites.get(expected_site).copied().unwrap_or_default() >= required
            && self.backends.get(&expected_backend).copied().unwrap_or_default() >= required;
        from_provider || from_grid_headers
    }
}

impl std::fmt::Display for RouteDistribution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "sites={:?}, backends={:?}, provider_gateways={:?}",
            self.sites, self.backends, self.provider_gateways
        )
    }
}

/// Response-header contract observed on successfully routed Gateway requests.
#[derive(Default)]
struct RoutingAttributionDiagnostics {
    /// Number of successful responses inspected.
    responses: usize,
    /// Successful responses without the documented `x-grid-site` header.
    missing_site_header: usize,
    /// Successful responses without the documented `x-grid-backend` header.
    missing_backend_header: usize,
    /// Responses where the Grid site and provider-gateway markers disagree.
    site_mismatches: usize,
    /// Bounded samples of actual header names from responses missing a Grid header.
    raw_header_name_samples: Vec<String>,
}

impl RoutingAttributionDiagnostics {
    /// Record the response-header contract without stopping independent routing proofs.
    fn observe(&mut self, response: &SiteRouteResponse) {
        if response.status != 200 {
            return;
        }
        self.responses += 1;
        if response.site.is_none() {
            self.missing_site_header += 1;
        }
        if response.backend.is_none() {
            self.missing_backend_header += 1;
        }
        if response
            .site
            .as_ref()
            .zip(response.provider_site.as_ref())
            .is_some_and(|(site, provider_site)| site != provider_site)
        {
            self.site_mismatches += 1;
        }
        if (response.site.is_none() || response.backend.is_none()) && self.raw_header_name_samples.len() < 3 {
            self.raw_header_name_samples.push(format!(
                "response {} missing_site={} missing_backend={} raw_header_names={:?}",
                self.responses,
                response.site.is_none(),
                response.backend.is_none(),
                response.raw_header_names
            ));
        }
    }

    /// Explain a missing or inconsistent documented response attribution contract.
    fn contract_error(&self) -> Option<String> {
        (self.missing_site_header > 0 || self.missing_backend_header > 0 || self.site_mismatches > 0).then(|| {
            format!(
                "Grid Gateway returned {} successful responses with missing or inconsistent attribution headers: missing x-grid-site={}, missing x-grid-backend={}, site mismatches={}",
                self.responses, self.missing_site_header, self.missing_backend_header, self.site_mismatches
            )
        })
    }
}

impl std::fmt::Display for RoutingAttributionDiagnostics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "successful_responses={}, missing_x_grid_site={}, missing_x_grid_backend={}, site_mismatches={}, raw_header_name_samples={:?}",
            self.responses,
            self.missing_site_header,
            self.missing_backend_header,
            self.site_mismatches,
            self.raw_header_name_samples
        )
    }
}

/// Send a fixed batch through `grid_site_route`, retaining only trusted response attribution.
fn route_distribution(
    port: u16,
    requests: usize,
    phase: &str,
    attribution: &mut RoutingAttributionDiagnostics,
) -> Result<RouteDistribution, Box<dyn std::error::Error>> {
    let mut distribution = RouteDistribution::default();
    for sequence in 0..requests {
        let response = send_site_request(port, sequence, phase)?;
        attribution.observe(&response);
        if response.status != 200 {
            return Err(format!("Grid Gateway request returned HTTP {}", response.status).into());
        }
        distribution.observe(&response)?;
    }
    Ok(distribution)
}

/// One HTTP status and the Grid Gateway's site/backend attribution headers.
pub(super) struct SiteRouteResponse {
    /// HTTP status.
    pub(super) status: u16,
    /// `x-grid-site` value.
    pub(super) site: Option<String>,
    /// `x-grid-backend` value.
    pub(super) backend: Option<String>,
    /// Site marker set by the trusted provider gateway reached by the request.
    pub(super) provider_site: Option<String>,
    /// Header names captured from the final raw HTTP response header block.
    pub(super) raw_header_names: Vec<String>,
}

/// Send one sessionless inference request to the local Grid Gateway listener.
pub(super) fn send_site_request(
    port: u16,
    sequence: usize,
    phase: &str,
) -> Result<SiteRouteResponse, Box<dyn std::error::Error>> {
    send_site_request_with_timeout(port, sequence, phase, 15)
}

/// Send one request with a bounded timeout for traffic that runs during a partition.
fn send_site_request_with_timeout(
    port: u16,
    sequence: usize,
    phase: &str,
    timeout_secs: u8,
) -> Result<SiteRouteResponse, Box<dyn std::error::Error>> {
    let timeout = timeout_secs.to_string();
    let request_id = unique_site_request_id(sequence);
    let body = serde_json::json!({
        "model": VCR_MODEL,
        "messages": [{"role": "user", "content": unique_site_prompt(&request_id)}],
        "max_tokens": 8
    });
    let output = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--max-time",
            &timeout,
            "--dump-header",
            "-",
            "--output",
            "/dev/null",
            "--write-out",
            "STATUS:%{http_code}\n",
            "--request",
            "POST",
            &format!("http://127.0.0.1:{port}/v1/chat/completions"),
            "--header",
            "Content-Type: application/json",
            "--header",
            &format!("X-Session-Id: {request_id}"),
            "--data",
            &body.to_string(),
        ])
        .output()
        .map_err(|error| {
            format!(
                "Grid Gateway inference probe failed: {}",
                curl_probe_diagnostics(&CurlProbeDiagnostic {
                    phase,
                    observed_at: &format_utc_iso(),
                    request_id: &request_id,
                    http_status: None,
                    curl_exit_code: None,
                    curl_stderr: "",
                    reason: &format!("could not start curl: {error}"),
                })
            )
        })?;
    let text = String::from_utf8_lossy(&output.stdout).into_owned();
    let response = parse_site_route_response(&text);
    if !output.status.success() || response.status == 0 {
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        return Err(format!(
            "Grid Gateway inference probe failed: {}",
            curl_probe_diagnostics(&CurlProbeDiagnostic {
                phase,
                observed_at: &format_utc_iso(),
                request_id: &request_id,
                http_status: Some(response.status),
                curl_exit_code: output.status.code(),
                curl_stderr: &stderr,
                reason: "curl returned a transport failure or HTTP status 000",
            })
        )
        .into());
    }
    Ok(response)
}

/// Generate a session and prompt identifier that is unique across every sample in this process.
fn unique_site_request_id(sequence: usize) -> String {
    let request = SITE_REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("grid-poll-{sequence}-{request}")
}

/// Start each prompt with a unique token so content-affinity cannot pin a sample batch.
fn unique_site_prompt(request_id: &str) -> String {
    format!("{request_id} cross-site signal qualification")
}

/// Parse curl's raw response-header dump and status output.
///
/// Header names are matched using HTTP's case-insensitive rules. If curl emits
/// multiple response blocks (for example, an interim response), only the final
/// block is retained. The raw names are preserved for bounded failure diagnostics.
fn parse_site_route_response(raw: &str) -> SiteRouteResponse {
    let mut response = SiteRouteResponse {
        status: 0,
        site: None,
        backend: None,
        provider_site: None,
        raw_header_names: Vec::new(),
    };

    for line in raw.lines().map(|line| line.trim_end_matches('\r')) {
        if line.starts_with("HTTP/") {
            response.site = None;
            response.backend = None;
            response.provider_site = None;
            response.raw_header_names.clear();
        } else if let Some(value) = line.strip_prefix("STATUS:") {
            response.status = value.trim().parse().unwrap_or(0);
        } else if let Some((name, value)) = line.split_once(':') {
            let name = name.trim();
            if name.is_empty() {
                continue;
            }
            let value = value.trim();
            response.raw_header_names.push(name.to_owned());
            if name.eq_ignore_ascii_case("x-grid-site") && !value.is_empty() {
                response.site = Some(value.to_owned());
            } else if name.eq_ignore_ascii_case("x-grid-backend") && !value.is_empty() {
                response.backend = Some(value.to_owned());
            } else if name.eq_ignore_ascii_case("x-grid-llmd-provider-gateway") && !value.is_empty() {
                response.provider_site = Some(value.to_owned());
            }
        }
    }
    response
}

/// Read an operator's `/metrics` through the run-owned Service.
pub(super) fn read_operator_metrics(port: u16) -> Result<String, Box<dyn std::error::Error>> {
    read_metrics_url(port, true)
}

/// Read the Grid Gateway's TLS-only `/metrics` endpoint.
pub(super) fn read_gateway_metrics(port: u16) -> Result<String, Box<dyn std::error::Error>> {
    read_metrics_url(port, false)
}

/// Fetch Prometheus exposition over the forwarded service port.
fn read_metrics_url(port: u16, tls_fallback: bool) -> Result<String, Box<dyn std::error::Error>> {
    let schemes = if tls_fallback {
        ["https", "http"]
    } else {
        ["https", "https"]
    };
    let mut last_error = String::from("no HTTP response");
    for scheme in schemes {
        let url = format!("{scheme}://127.0.0.1:{port}/metrics");
        let output = Command::new("curl")
            .args(["--silent", "--show-error", "--insecure", "--max-time", "5", &url])
            .output()?;
        if output.status.success() {
            return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
        }
        String::from_utf8_lossy(&output.stderr)
            .trim()
            .clone_into(&mut last_error);
    }
    Err(format!("could not read metrics service: {last_error}").into())
}

/// Return the counter value matching a metric name and its required labels.
pub(super) fn metric_counter(text: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    sample_value(text, name, labels).unwrap_or(0.0)
}

#[cfg(test)]
mod serving_peer_tests {
    use super::{
        MEASURED_SITE_GAUGES, POOL_A_PROVIDER, applied_gateway_reload_count_from_text, is_expected_local_operator_peer,
        measured_site_gauges_are_cleared, require_active_signal_gauges, reused_gateway_config_count_from_text,
    };

    #[test]
    fn serving_peer_must_target_the_local_operator_over_its_verified_mtls_path() {
        let valid = serde_json::json!({
            "site": "pool-a",
            "addr": "10.0.0.5:9091",
            "server_name": "pool-a.grid.internal",
            "authority": "pool-a.grid.internal",
            "path": "/v1/site/signals",
            "interval_ms": 5000,
            "grid_ca_path": "/etc/praxis/tls/ca.crt",
            "client_cert_path": "/etc/praxis/tls/tls.crt",
            "client_key_path": "/etc/praxis/tls/tls.key"
        });
        assert!(is_expected_local_operator_peer(&valid));

        let mut wrong_site = valid.clone();
        if let Some(site) = wrong_site.get_mut("site") {
            *site = serde_json::json!("pool-b");
        } else {
            std::process::abort();
        }
        assert!(!is_expected_local_operator_peer(&wrong_site));

        let mut wrong_path = valid.clone();
        if let Some(path) = wrong_path.get_mut("path") {
            *path = serde_json::json!("/metrics");
        } else {
            std::process::abort();
        }
        assert!(!is_expected_local_operator_peer(&wrong_path));

        let mut wrong_identity = valid;
        if let Some(server_name) = wrong_identity.get_mut("server_name") {
            *server_name = serde_json::json!("pool-b.grid.internal");
        } else {
            std::process::abort();
        }
        assert!(!is_expected_local_operator_peer(&wrong_identity));
    }

    #[test]
    fn accepted_gateway_reload_counter_requires_the_applied_label() {
        let metrics = "grid_serving_config_reload_total{result=\"applied\"} 2\n\
                       grid_serving_config_reload_total{result=\"refused\"} 8\n";
        assert!((applied_gateway_reload_count_from_text(metrics) - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn observed_gateway_config_counts_applied_and_reused_results_separately() {
        let metrics = "grid_serving_config_reload_total{result=\"applied\"} 2\n\
                       grid_serving_config_reload_total{result=\"reused\"} 3\n\
                       grid_serving_config_reload_total{result=\"refused\"} 8\n";
        assert!((applied_gateway_reload_count_from_text(metrics) - 2.0).abs() < f64::EPSILON);
        assert!((reused_gateway_config_count_from_text(metrics) - 3.0).abs() < f64::EPSILON);
        assert!(reused_gateway_config_count_from_text("").abs() < f64::EPSILON);
    }

    #[test]
    fn fresh_signal_exposes_finite_full_and_reading_age_gauges() {
        let metrics = "grid_route_site_in_flight{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 9\n\
                       grid_route_site_full{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 0\n\
                       grid_route_site_reading_age_seconds{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 1\n";
        assert!(
            require_active_signal_gauges(metrics, "pool-a", POOL_A_PROVIDER).is_ok(),
            "the active Gateway sample should expose finite in-flight, full, and reading-age gauges"
        );
        assert!(
            require_active_signal_gauges(
                &metrics.replace(
                    "grid_route_site_full{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 0",
                    "grid_route_site_full{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} NaN"
                ),
                "pool-a",
                POOL_A_PROVIDER
            )
            .is_err(),
            "a non-finite full gauge must not count as an active signal reading"
        );
    }

    #[test]
    fn stale_signal_clearing_requires_all_eight_measured_gauges_to_be_nan() {
        let cleared = MEASURED_SITE_GAUGES
            .iter()
            .map(|name| format!("{name}{{site=\"pool-a\",cluster=\"{POOL_A_PROVIDER}\"}} NaN"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            measured_site_gauges_are_cleared(&cleared, "pool-a", POOL_A_PROVIDER),
            "every measured gauge should have an explicit NaN sample after the signal disappears"
        );
        let stale = cleared.replace(
            "grid_route_site_reading_age_seconds{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} NaN",
            "grid_route_site_reading_age_seconds{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 4",
        );
        assert!(
            !measured_site_gauges_are_cleared(&stale, "pool-a", POOL_A_PROVIDER),
            "a stale reading-age gauge must fail the cleanup proof"
        );
        assert!(
            !measured_site_gauges_are_cleared(
                &cleared.replace(
                    "grid_route_site_full{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} NaN\n",
                    ""
                ),
                "pool-a",
                POOL_A_PROVIDER
            ),
            "a missing measured gauge must not pass as a cleared gauge"
        );
    }
}

/// Parse a Prometheus gauge/counter line by metric name and required labels.
pub(super) fn sample_value(text: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    text.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let series = fields.next()?;
        let metric_name = series.split('{').next().unwrap_or(series);
        let matches = metric_name == name
            && labels
                .iter()
                .all(|(label, value)| series.contains(&format!("{label}=\"{value}\"")));
        if matches { fields.next()?.parse().ok() } else { None }
    })
}

/// Capture the currently running Grid Gateway pod UID.
pub(super) fn gateway_pod_uid(context: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/instance=grid-serving-gateway",
            "-o",
            "json",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "read Grid Gateway pod identity: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let pods: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    pods.pointer("/items/0/metadata/uid")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| "Grid Gateway has no running pod UID".into())
}

/// Save the peer's current canonical fingerprint set for unconditional restoration.
fn read_site_pins(context: &str, name: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let site = kubectl_json(context, "get", &format!("gridsite/{name}"))?;
    site.pointer("/spec/trust/canonicalFingerprints")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("GridSite {name} has no canonicalFingerprints array"))?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("GridSite {name} has a non-string fingerprint").into())
        })
        .collect()
}

/// Replace the run-owned GridSite peer pins.
fn patch_site_pins(context: &str, name: &str, pins: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let patch = serde_json::json!({"spec": {"trust": {"canonicalFingerprints": pins}}});
    kubectl_patch_json(context, &format!("gridsite/{name}"), &patch)
}

/// Scale an EPP Deployment and wait for the requested replica state.
fn scale_epp(site: &str, replicas: u32) -> Result<(), Box<dyn std::error::Error>> {
    let context = kind_context(site);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "scale",
            "deployment/llmd-epp",
            "--replicas",
            &replicas.to_string(),
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "scale {site} EPP to {replicas} replicas: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        if let Ok(deployment) = kubectl_json(&context, "get", "deployment/llmd-epp") {
            let desired = deployment
                .pointer("/spec/replicas")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();
            let ready = deployment
                .pointer("/status/readyReplicas")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();
            let actual = deployment
                .pointer("/status/replicas")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();
            if desired == u64::from(replicas)
                && if replicas == 0 {
                    actual == 0
                } else {
                    ready >= u64::from(replicas)
                }
            {
                return Ok(());
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(format!("{site} EPP did not reach replicas={replicas} within 120 seconds").into())
}

/// Restart the run-owned operator after changing its grid-wide signal transport mode.
fn restart_operator(site: &str) -> Result<(), Box<dyn std::error::Error>> {
    let context = kind_context(site);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "rollout",
            "restart",
            "deployment/grid-operator",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "restart {site} Grid operator for signal polling: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    kubectl::wait_for_rollout_ns(&context, "grid-operator", GRID_SYSTEM_NS, site)
        .map_err(|error| format!("wait for {site} Grid operator poll-mode restart: {error}"))?;
    Ok(())
}

/// Get one Kubernetes JSON object.
fn kubectl_json(context: &str, verb: &str, resource: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args(["--context", context, "-n", GRID_SYSTEM_NS, verb, resource, "-o", "json"])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl {verb} {resource}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

/// Apply a JSON Kubernetes object from stdin.
fn apply_json(context: &str, object: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    let mut child = Command::new("kubectl")
        .args(["--context", context, "apply", "-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdin = child.stdin.take().ok_or("kubectl apply has no stdin")?;
    serde_json::to_writer(stdin, object)?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl apply run-owned object: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(())
}

/// Merge-patch a Kubernetes resource.
fn kubectl_patch_json(
    context: &str,
    resource: &str,
    patch: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "patch",
            resource,
            "--type=merge",
            "-p",
            &patch.to_string(),
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl patch {resource}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_sample_sequence_gets_a_unique_request_identity() {
        let first = unique_site_request_id(0);
        let second = unique_site_request_id(0);

        assert_ne!(first, second, "a later sample must not reuse a sticky session");
        assert!(
            unique_site_prompt(&first).starts_with(&first),
            "a sample prompt must begin with its unique token to avoid shared-prefix affinity"
        );
    }

    #[test]
    fn weighted_route_expectation_uses_current_scores_capacity_and_saturation() {
        let metrics = "grid_route_site_score{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 0\n\
                       grid_route_site_weight{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 8\n\
                       grid_route_site_rho{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 0\n\
                       grid_route_site_score{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 9\n\
                       grid_route_site_weight{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 18\n\
                       grid_route_site_rho{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 0.9999842224617964\n";

        let route = weighted_route_metrics(metrics, "pool-a", true);

        assert!(route.is_ok(), "both sites must expose complete Gateway route metrics");
        let route = route.unwrap_or_else(|_| std::process::abort());
        assert!(
            route.preferred_score.abs() < f64::EPSILON,
            "pool-A must have the lower queue score"
        );
        assert!(
            (route.other_score - 9.0).abs() < f64::EPSILON,
            "pool-B must have the higher queue score"
        );
        assert!(
            (route.preferred_share - (8.0 / (8.0 + 18.0 / (1.0 + 0.999_984_222_461_796_4)))).abs() < f64::EPSILON,
            "the expected share must use capacity divided by one plus smoothed saturation"
        );
    }

    #[test]
    fn weighted_route_expectation_allows_tied_finite_scores() {
        let metrics = "grid_route_site_score{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 0\n\
                       grid_route_site_weight{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 8\n\
                       grid_route_site_rho{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 0\n\
                       grid_route_site_score{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 0\n\
                       grid_route_site_weight{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 18\n\
                       grid_route_site_rho{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 0.5\n";

        let route = weighted_route_metrics(metrics, "pool-a", false);

        assert!(
            route.is_ok(),
            "equal finite queue scores still have a valid weighted share"
        );
        let route = route.unwrap_or_else(|_| std::process::abort());
        assert!(
            (route.preferred_share - (8.0 / (8.0 + 12.0))).abs() < f64::EPSILON,
            "the tied-score share must still use capacity divided by one plus saturation"
        );
    }

    #[test]
    fn signal_recovery_requires_a_strictly_lower_site_score() {
        let metrics = r#"grid_route_site_score{site="pool-a",cluster="llmd-pool-a-provider"} 0
grid_route_site_weight{site="pool-a",cluster="llmd-pool-a-provider"} 8
grid_route_site_rho{site="pool-a",cluster="llmd-pool-a-provider"} 0
grid_route_site_score{site="pool-b",cluster="llmd-pool-b-provider"} 0
grid_route_site_weight{site="pool-b",cluster="llmd-pool-b-provider"} 18
grid_route_site_rho{site="pool-b",cluster="llmd-pool-b-provider"} 0.5
"#;

        let route = weighted_route_metrics(metrics, "pool-a", true);

        assert!(
            route.is_err_and(|error| error.contains("is not strictly lower")),
            "a tied score must not satisfy the recovery preference assertion"
        );
    }

    #[test]
    fn pressure_transition_rejects_equal_rho_even_when_the_route_score_is_lower() {
        let metrics = r#"grid_route_site_score{site="pool-a",cluster="llmd-pool-a-provider"} 0
grid_route_site_weight{site="pool-a",cluster="llmd-pool-a-provider"} 18
grid_route_site_rho{site="pool-a",cluster="llmd-pool-a-provider"} 0
grid_route_site_score{site="pool-b",cluster="llmd-pool-b-provider"} 9
grid_route_site_weight{site="pool-b",cluster="llmd-pool-b-provider"} 18
grid_route_site_rho{site="pool-b",cluster="llmd-pool-b-provider"} 0
"#;

        let route = weighted_route_metrics(metrics, "pool-a", true);

        assert!(
            route.is_err_and(|error| error.contains("rho for pool-a is not strictly lower")),
            "a lower score must not prove the pressure signal changed when rho is tied"
        );
    }

    #[test]
    fn missing_weighted_route_metric_reports_the_current_site_gauges() {
        let metrics = "grid_route_site_score{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 0\n\
                       grid_route_site_weight{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 8\n\
                       grid_route_site_rho{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} NaN\n\
                       grid_route_site_score{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 9\n\
                       grid_route_site_weight{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 18\n\
                       grid_route_site_rho{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 0.5\n";
        let Err(error) = weighted_route_metrics(metrics, "pool-a", false) else {
            std::process::abort();
        };

        assert!(error.contains("no finite grid_route_site_rho metric for (pool-a, llmd-pool-a-provider)"));
        assert!(error.contains("current route metrics:"));
        assert!(error.contains("grid_route_site_rho{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} NaN"));
        assert!(error.contains("grid_route_site_rho{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 0.5"));
    }

    #[test]
    fn weighted_route_expectation_rejects_a_gateway_score_that_disagrees_with_pressure_order() {
        let metrics = "grid_route_site_score{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 9\n\
                       grid_route_site_weight{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 8\n\
                       grid_route_site_rho{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 0\n\
                       grid_route_site_score{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 0\n\
                       grid_route_site_weight{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 18\n\
                       grid_route_site_rho{site=\"pool-b\",cluster=\"llmd-pool-b-provider\"} 0\n";

        assert!(
            weighted_route_metrics(metrics, "pool-a", false).is_err(),
            "the test must fail while Gateway metrics prefer the opposite pressure order"
        );
    }

    #[test]
    fn weighted_route_sample_must_match_the_predicted_selector_share() {
        assert!(weighted_route_share_matches(0.47, 0.38));
        assert!(!weighted_route_share_matches(0.47, 0.70));
    }

    #[test]
    fn weighted_route_share_uses_metric_windows_when_saturation_changes_during_traffic() {
        let expected = weighted_route_expected_share(&[(256, 0.307_692_307_7, 0.424_110_570_8)]).unwrap_or(f64::NAN);

        assert!(
            (expected - 0.365_901_439_25).abs() < 0.000_001,
            "the request window should use both observed Gateway saturation states"
        );
        assert!(
            weighted_route_share_matches(expected, 101.0 / 256.0),
            "the observed 39.5% share should be checked against the measured route-metric window"
        );
        assert!(
            !weighted_route_share_matches(expected, 0.70),
            "metric changes must not make a clearly incorrect route share pass"
        );
    }

    #[test]
    fn weighted_route_share_weights_each_metric_window_by_its_request_count() {
        let expected = weighted_route_expected_share(&[(32, 0.2, 0.4), (96, 0.4, 0.8)]).unwrap_or(f64::NAN);

        assert!(
            (expected - 0.525).abs() < f64::EPSILON,
            "the longer route-metric interval should contribute proportionally more requests"
        );
    }

    #[test]
    fn weighted_route_sample_requires_every_request_to_use_between_two() {
        let before = "grid_route_selections_total{path=\"two_choices\"} 2\n\
                       grid_route_selections_total{path=\"by_capacity\"} 3\n\
                       grid_route_selections_total{path=\"lone\"} 4\n\
                       grid_route_selections_total{path=\"between_two\"} 5\n";
        let after = "grid_route_selections_total{path=\"two_choices\"} 2\n\
                      grid_route_selections_total{path=\"by_capacity\"} 3\n\
                      grid_route_selections_total{path=\"lone\"} 4\n\
                      grid_route_selections_total{path=\"between_two\"} 261\n";
        let before = route_selection_counts(before);
        let after = route_selection_counts(after);

        assert!(before.is_ok(), "all selector paths must be exposed before the sample");
        assert!(after.is_ok(), "all selector paths must be exposed after the sample");
        let before = before.unwrap_or_else(|_| std::process::abort());
        let after = after.unwrap_or_else(|_| std::process::abort());
        let delta = weighted_selector_sample_delta(before, after, 256);

        assert!(delta.is_ok(), "every request should use the two-site weighted selector");
        assert!(
            delta.is_ok_and(|evidence| evidence.contains("between_two: 256")),
            "the recorded path delta should identify all weighted selections"
        );
    }

    #[test]
    fn weighted_route_sample_rejects_a_changed_selector_path() {
        let before = RouteSelectionCounts {
            between_two: 5.0,
            two_choices: 2.0,
            lone: 4.0,
            by_capacity: 3.0,
        };
        let after = RouteSelectionCounts {
            between_two: 260.0,
            two_choices: 3.0,
            lone: 4.0,
            by_capacity: 3.0,
        };

        assert!(
            weighted_selector_sample_delta(before, after, 256).is_err(),
            "a sample that changed candidate count or selection tier must fail"
        );
    }

    #[test]
    fn one_live_swim_member_is_connected_for_the_two_site_topology() {
        assert!(has_connected_swim_peer(1));
        assert!(!has_connected_swim_peer(0));
    }

    #[test]
    fn wrong_pin_pressure_window_requires_the_changed_sample_to_flip_selection() {
        assert!(wrong_pin_signal_window(12.0, 18.0, 9.0));
        assert!(!wrong_pin_signal_window(6.0, 18.0, 9.0));
        assert!(!wrong_pin_signal_window(18.0, 18.0, 9.0));
        assert!(!wrong_pin_signal_window(12.0, 18.0, 18.0));
    }

    #[test]
    fn identity_rejection_observes_tls_transport_and_peer_client_errors() {
        let previous = PeerPollFailureCounts::default();
        for observed in [
            PeerPollFailureCounts { tls: 1.0, ..previous },
            PeerPollFailureCounts {
                transport: 1.0,
                ..previous
            },
            PeerPollFailureCounts {
                client_error: 1.0,
                ..previous
            },
        ] {
            assert!(observed.advanced_from(previous));
        }
        assert!(!previous.advanced_from(previous));
        assert!(
            PeerPollFailureCounts {
                transport: 1.0,
                ..previous
            }
            .delta_from(previous)
            .contains("transport=0->1")
        );
    }

    #[test]
    fn restored_network_attachment_must_preserve_address_and_aliases() {
        let expected = NetworkAttachment {
            container: "grid-llmd-pm-pool-b-control-plane".to_owned(),
            ipv4: "172.20.0.3".to_owned(),
            aliases: vec!["pool-b-control-plane".to_owned(), "kind".to_owned()],
            disconnected: true,
        };
        let restored = NetworkAttachment {
            container: expected.container.clone(),
            ipv4: expected.ipv4.clone(),
            aliases: vec!["kind".to_owned(), "pool-b-control-plane".to_owned()],
            disconnected: false,
        };
        assert!(attachment_matches(&restored, &expected));

        let changed_address = NetworkAttachment {
            ipv4: "172.20.0.4".to_owned(),
            ..restored
        };
        assert!(!attachment_matches(&changed_address, &expected));

        let changed_aliases = NetworkAttachment {
            container: expected.container.clone(),
            ipv4: expected.ipv4.clone(),
            aliases: vec!["pool-b-control-plane".to_owned()],
            disconnected: false,
        };
        assert!(!attachment_matches(&changed_aliases, &expected));
    }

    #[test]
    fn metric_sample_requires_the_exact_name_and_labels() {
        let exposition = "grid_provider_in_flight_requests{grid_site=\"pool-a\",grid_provider=\"llmd-pool-a-provider\"} 0\n\
                          grid_provider_in_flight_requests{grid_site=\"pool-b\",grid_provider=\"llmd-pool-b-provider\"} 18\n\
                          grid_route_site_rho_counted{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} NaN\n\
                          grid_route_site_rho{site=\"pool-a\",cluster=\"llmd-pool-a-provider\"} 0\n";
        assert_eq!(
            sample_value(
                exposition,
                "grid_provider_in_flight_requests",
                &[("grid_site", "pool-b"), ("grid_provider", "llmd-pool-b-provider")]
            ),
            Some(18.0)
        );
        assert_eq!(
            sample_value(
                exposition,
                "grid_provider_in_flight_requests",
                &[("grid_site", "pool-c")]
            ),
            None
        );
        assert_eq!(
            sample_value(
                exposition,
                "grid_route_site_rho",
                &[("site", "pool-a"), ("cluster", "llmd-pool-a-provider")]
            ),
            Some(0.0),
            "a longer metric name with the same prefix must not shadow the exact gauge"
        );
    }

    #[test]
    fn metric_counter_defaults_to_zero_when_a_series_is_absent() {
        assert!(
            (metric_counter(
                "grid_peer_poll_total{peer=\"pool-b\",outcome=\"ok\"} 3\n",
                "grid_peer_poll_total",
                &[("peer", "pool-a")]
            ) - 0.0)
                .abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn missing_grid_headers_are_reported_without_discarding_provider_site_evidence() {
        let response = SiteRouteResponse {
            status: 200,
            site: None,
            backend: None,
            provider_site: Some("pool-a".to_owned()),
            raw_header_names: vec!["x-grid-llmd-provider-gateway".to_owned()],
        };
        let mut attribution = RoutingAttributionDiagnostics::default();
        attribution.observe(&response);

        assert_eq!(response.provider_site.as_deref(), Some("pool-a"));
        assert!(
            attribution.contract_error().is_some_and(
                |error| error.contains("missing x-grid-site=1") && error.contains("missing x-grid-backend=1")
            )
        );
    }

    #[test]
    fn trusted_provider_marker_can_prove_site_when_grid_headers_are_missing() {
        let response = SiteRouteResponse {
            status: 200,
            site: None,
            backend: None,
            provider_site: Some("pool-a".to_owned()),
            raw_header_names: vec!["x-grid-llmd-provider-gateway".to_owned()],
        };
        let mut distribution = RouteDistribution::default();

        assert!(matches!(distribution.observe(&response), Ok(())));
        assert!(distribution.proves_site("pool-a", 1));
        assert!(!distribution.proves_site("pool-b", 1));
    }

    #[test]
    fn matching_grid_site_and_backend_headers_can_prove_site_without_provider_marker() {
        let response = SiteRouteResponse {
            status: 200,
            site: Some("pool-a".to_owned()),
            backend: Some("llmd-pool-a-provider".to_owned()),
            provider_site: None,
            raw_header_names: vec!["x-grid-site".to_owned(), "x-grid-backend".to_owned()],
        };
        let mut distribution = RouteDistribution::default();

        assert!(matches!(distribution.observe(&response), Ok(())));
        assert!(distribution.proves_site("pool-a", 1));
    }

    #[test]
    fn grid_and_provider_site_markers_must_agree() {
        let response = SiteRouteResponse {
            status: 200,
            site: Some("pool-a".to_owned()),
            backend: Some("llmd-pool-a-provider".to_owned()),
            provider_site: Some("pool-b".to_owned()),
            raw_header_names: vec!["x-grid-site".to_owned(), "x-grid-backend".to_owned()],
        };
        let mut attribution = RoutingAttributionDiagnostics::default();
        attribution.observe(&response);

        assert!(
            attribution
                .contract_error()
                .is_some_and(|error| error.contains("site mismatches=1"))
        );
    }

    #[test]
    fn raw_response_headers_are_matched_case_insensitively_from_the_final_block() {
        let response = parse_site_route_response(
            "HTTP/1.1 100 Continue\r\nX-Grid-Site: stale\r\n\r\nHTTP/2 200\r\nX-GRID-SITE: pool-b\r\nX-Grid-Backend: llmd-pool-b-provider\r\nX-Grid-Llmd-Provider-Gateway: pool-b\r\nContent-Type: application/json\r\n\r\nSTATUS:200\n",
        );

        assert_eq!(response.status, 200);
        assert_eq!(response.site.as_deref(), Some("pool-b"));
        assert_eq!(response.backend.as_deref(), Some("llmd-pool-b-provider"));
        assert_eq!(response.provider_site.as_deref(), Some("pool-b"));
        assert_eq!(
            response.raw_header_names,
            [
                "X-GRID-SITE",
                "X-Grid-Backend",
                "X-Grid-Llmd-Provider-Gateway",
                "Content-Type"
            ]
        );
    }
}
