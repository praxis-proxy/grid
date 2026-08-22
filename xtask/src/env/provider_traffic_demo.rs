//! Narrated, evidence-backed provider-traffic demo scenarios.
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use serde::Serialize;

use super::{DemoMode, GlbDemoOptions, certs, glb, kubectl, operator, safe_truncate_str};

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

/// Directory where generated TLS certificates are stored.
const CERTS_DIR: &str = "tests/env/certs";

/// Ordered provider-site cluster names in the provider-traffic scenario.
///
/// The consumer entrypoint is deployed only in `CONSUMER_SITE`; the other
/// clusters contain provider gateways and backends only.
const CLUSTERS: &[&str] = &["provider-a", "provider-b", "provider-c"];

/// The single consumer gateway used by the focused request-routing proof.
const CONSUMER_SITE: &str = "provider-a";

/// Consumer gateway TLS secret name (matches Helm `existingSecret` reference).
const CONSUMER_TLS_SECRET: &str = "consumer-gateway-tls";

/// Evidence JSON schema version.
const EVIDENCE_SCHEMA_VERSION: &str = "1";

/// Kubernetes namespace for all Grid components.
const GRID_SYSTEM_NS: &str = "grid-system";

/// Overlay `ConfigMap` name created by the Grid operator for consumer gateways.
const OVERLAY_CONFIGMAP: &str = "grid-overlay-grid-provider-traffic-consumer-gateway";

/// Provider credential secret name (matches Helm `credentials[0].name`).
const VCR_INFERENCE_CREDENTIAL: &str = "vcr-inference-credential";

/// Stable terminal separator that also remains readable in captured logs.
const OUTPUT_RULE: &str = "===============================================================================";

/// Provider gateway service name advertised via SWIM for cross-site discovery.
const PROVIDER_GATEWAY_SERVICE: &str = "provider-gateway";

/// Provider gateway port advertised via SWIM for cross-site discovery.
const PROVIDER_GATEWAY_PORT: &str = "8443";

/// Provider gateway TLS secret name (matches Helm `existingSecret` reference).
const PROVIDER_TLS_SECRET: &str = "provider-gateway-tls";

/// Same-CA client identity with an organization rejected by `peer_identity_trust`.
const WRONG_ORG_TLS_SECRET: &str = "wrong-org-client-tls";

/// Number of environment setup phases shown to the user.
const SETUP_PHASES: usize = 14;

/// Makes retry probe names unique while retaining a recognizable prefix.
static PROBE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

// -----------------------------------------------------------------------------
// Context
// -----------------------------------------------------------------------------

/// Provider-traffic demo execution context.
struct ProviderTrafficContext {
    /// Canonical demo root directory for resolving configs, resources, and
    /// other demo-relative assets.
    demo_root: PathBuf,
    /// Path to the resolved Forge config.
    resolved_config: PathBuf,
    /// Path to the forge binary.
    forge_bin: PathBuf,
}

// -----------------------------------------------------------------------------
// Overlay State
// -----------------------------------------------------------------------------

/// Per-site overlay snapshot captured from the `ConfigMap`.
#[derive(Clone, Debug, serde::Deserialize, Serialize)]
struct OverlayData {
    /// Kubernetes `ConfigMap` `resourceVersion` (per-cluster, never compared
    /// across clusters).
    resource_version: String,
    /// Content-addressed semantic revision from the
    /// `grid.praxis-proxy.io/overlay-revision` annotation. Changes only when
    /// routing-relevant fields change; safe to compare across clusters.
    semantic_revision: String,
    /// Candidate name to stable ID mapping.
    stable_ids: BTreeMap<String, String>,
    /// Full candidate details for evidence (kind, name, site, cluster, model).
    candidates: Vec<OverlayCandidate>,
}

/// Subset of `RoutingCandidate` fields kept for evidence and validation.
#[derive(Clone, Debug, serde::Deserialize, Serialize)]
struct OverlayCandidate {
    /// Candidate kind (e.g. `inference_model`).
    kind: String,
    /// Model name.
    name: String,
    /// Site name.
    site: String,
    /// Upstream cluster identifier (used as the candidate key).
    cluster: String,
    /// Deterministic stable ID for session binding.
    stable_id: String,
    /// Whether the candidate's metrics snapshot is fresh.
    fresh: Option<bool>,
    /// Admission state emitted by the operator, when present.
    admission_state: Option<String>,
    /// Explicit priority group emitted by the operator, when present.
    selection_group: Option<u32>,
}

/// Pre- and post-SWIM overlay snapshots for evidence.
#[derive(Clone, Debug, Default, serde::Deserialize, Serialize)]
struct OverlayState {
    /// Per-site overlays before SWIM seeding (local candidates only).
    pre_swim: BTreeMap<String, OverlayData>,
    /// Per-site overlays after global convergence.
    post_swim: BTreeMap<String, OverlayData>,
}

// -----------------------------------------------------------------------------
// Evidence
// -----------------------------------------------------------------------------

/// Evidence written to `results.json`.
#[derive(Debug, serde::Deserialize, Serialize)]
struct Evidence {
    /// Evidence schema version.
    schema_version: String,
    /// Demo mode that was executed.
    mode: String,
    /// Topology name.
    topology: String,
    /// List of cluster names.
    clusters: Vec<String>,
    /// Proof results for each assertion.
    proof_results: BTreeMap<String, ProofResult>,
    /// Exact image references used.
    images: BTreeMap<String, String>,
    /// Pre- and post-SWIM overlay snapshots.
    overlay_state: OverlayState,
    /// Cluster health status.
    cluster_health: Vec<ClusterHealth>,
    /// Component deployment status.
    components: Vec<ComponentStatus>,
    /// SWIM membership views.
    swim_membership: Vec<SwimMembership>,
    /// Provider response samples.
    provider_responses: Vec<ProviderResponse>,
    /// Security assertion results.
    security_results: Vec<SecurityResult>,
    /// Teardown success.
    teardown_success: bool,
}

/// Evidence for one proof assertion.
#[derive(Debug, serde::Deserialize, Serialize)]
struct ProofResult {
    /// Whether the proof passed.
    success: bool,
    /// Human-readable reason.
    reason: String,
    /// Observed facts that support this result.
    observed_facts: BTreeMap<String, serde_json::Value>,
    /// Duration of the assertion in milliseconds.
    duration_ms: u64,
}

/// Cluster health status.
#[derive(Debug, serde::Deserialize, Serialize)]
struct ClusterHealth {
    /// Cluster name.
    name: String,
    /// Whether the cluster is healthy.
    healthy: bool,
    /// API server response time in milliseconds.
    api_response_ms: Option<u64>,
    /// Number of ready nodes.
    ready_nodes: u32,
}

/// Component deployment status.
#[derive(Debug, serde::Deserialize, Serialize)]
struct ComponentStatus {
    /// Component name.
    name: String,
    /// Deployment namespace.
    namespace: String,
    /// Ready replicas.
    ready_replicas: u32,
    /// Desired replicas.
    desired_replicas: u32,
    /// Whether the component is ready.
    ready: bool,
}

/// SWIM membership view.
#[derive(Debug, serde::Deserialize, Serialize)]
struct SwimMembership {
    /// Site name.
    site: String,
    /// Local node ID.
    local_node: String,
    /// List of known peers.
    peers: Vec<String>,
    /// Membership convergence status.
    converged: bool,
}

/// Provider response metadata.
#[derive(Debug, serde::Deserialize, Serialize)]
struct ProviderResponse {
    /// Consumer site that made the request.
    consumer_site: String,
    /// Provider site that served the request.
    provider_site: String,
    /// Provider instance ID.
    provider_instance: String,
    /// Session ID.
    session_id: String,
    /// Serving revision.
    serving_revision: String,
    /// Response time in milliseconds.
    response_time_ms: u64,
    /// Whether the response was successful.
    success: bool,
}

/// Security assertion result.
#[derive(Debug, serde::Deserialize, Serialize)]
struct SecurityResult {
    /// Type of security test.
    test_type: String,
    /// Expected result (allow/deny).
    expected: String,
    /// Actual result.
    actual: String,
    /// Whether the test passed.
    passed: bool,
    /// Additional context.
    context: String,
}

// -----------------------------------------------------------------------------
// Utility Functions
// -----------------------------------------------------------------------------

/// Format current UTC timestamp for run IDs.
fn format_utc_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or_else(|_| "unknown".to_owned(), |duration| format!("{}", duration.as_secs()))
}

/// Format current UTC timestamp in ISO format.
fn format_utc_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map_or_else(
        |_| "unknown-utc".to_owned(),
        |duration| format!("{}-utc", duration.as_secs()),
    )
}

/// Resolve the evidence directory path.
fn resolve_evidence_dir(
    forge_config: &Path,
    options: &GlbDemoOptions,
    run_id: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(dir) = &options.evidence_dir {
        Ok(dir.clone())
    } else {
        let config_dir = forge_config
            .parent()
            .ok_or("forge config should have parent directory")?;
        Ok(config_dir.join(format!("evidence-{run_id}")))
    }
}

// -----------------------------------------------------------------------------
// Assertion Framework
// -----------------------------------------------------------------------------

/// Result of a runtime assertion.
type AssertionResult = Result<ProofResult, Box<dyn std::error::Error>>;

/// Create a successful proof result with observed facts.
fn proof_success(reason: &str, observed_facts: BTreeMap<String, serde_json::Value>, duration: Duration) -> ProofResult {
    ProofResult {
        success: true,
        reason: reason.to_owned(),
        observed_facts,
        duration_ms: u64::try_from(duration.as_millis().min(u128::from(u64::MAX))).unwrap_or(u64::MAX),
    }
}

/// Create a failed proof result with observed facts.
fn proof_failure(reason: &str, observed_facts: BTreeMap<String, serde_json::Value>, duration: Duration) -> ProofResult {
    ProofResult {
        success: false,
        reason: reason.to_owned(),
        observed_facts,
        duration_ms: u64::try_from(duration.as_millis().min(u128::from(u64::MAX))).unwrap_or(u64::MAX),
    }
}

/// Execute an assertion with timing and error handling.
fn run_assertion<F>(name: &str, assertion_fn: F) -> AssertionResult
where
    F: FnOnce() -> AssertionResult,
{
    let start = Instant::now();
    eprintln!("  [ASSERT] {name}");

    let result = assertion_fn();
    let _duration = start.elapsed();

    match &result {
        Ok(proof) => {
            if proof.success {
                eprintln!("  [OK] {name}: {}", proof.reason);
            } else {
                eprintln!("  [FAIL] {name}: {}", proof.reason);
            }
        },
        Err(e) => {
            eprintln!("  [ERROR] {name}: {e}");
        },
    }

    result.map_err(|e| {
        // Convert assertion errors to proof failures
        format!("Assertion {name} failed: {e}").into()
    })
}

/// Poll for a condition with bounded retries.
fn poll_until<F, T>(condition: F, timeout: Duration, interval: Duration) -> Result<T, Box<dyn std::error::Error>>
where
    F: Fn() -> Result<Option<T>, Box<dyn std::error::Error>>,
{
    let start = Instant::now();

    loop {
        match condition() {
            Ok(Some(result)) => return Ok(result),
            Ok(None) => {
                if start.elapsed() >= timeout {
                    return Err("Timeout waiting for condition".into());
                }
                std::thread::park_timeout(interval);
            },
            Err(e) => return Err(e),
        }
    }
}

/// Wait for a deployment to be ready.
fn wait_for_deployment(deployment: &str, namespace: &str, context: &str) -> Result<(), Box<dyn std::error::Error>> {
    kubectl::wait_for_rollout_ns(context, deployment, namespace, "deployment")?;
    Ok(())
}

/// Pod-security overrides for ephemeral curl pods in restricted namespaces.
///
/// `kubectl run` names the container after the pod, so the container name must
/// match. The curlimages/curl image runs as UID 100.
fn curl_pod_overrides(pod_name: &str, curl_args: &[&str]) -> String {
    let args = curl_args.strip_prefix(&["curl"]).unwrap_or(curl_args);
    serde_json::json!({
        "spec": {
            "automountServiceAccountToken": false,
            "securityContext": {
                "runAsNonRoot": true,
                "seccompProfile": { "type": "RuntimeDefault" }
            },
            "containers": [{
                "name": pod_name,
                "image": "curlimages/curl:8.12.1",
                "command": ["curl"],
                "args": args,
                "securityContext": {
                    "runAsUser": 100,
                    "allowPrivilegeEscalation": false,
                    "readOnlyRootFilesystem": true,
                    "capabilities": { "drop": ["ALL"] }
                }
            }]
        }
    })
    .to_string()
}

/// Run an ephemeral curl pod with restricted `PodSecurity` context.
fn run_curl_probe(context: &str, pod_name: &str, curl_args: &[&str]) -> Result<std::process::Output, std::io::Error> {
    let sequence = PROBE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let prefix = pod_name.get(..pod_name.len().min(40)).unwrap_or(pod_name);
    let unique_pod_name = format!("{prefix}-{sequence}");
    let overrides = curl_pod_overrides(&unique_pod_name, curl_args);
    Command::new("kubectl")
        .args([
            "run",
            &unique_pod_name,
            "--image=curlimages/curl:8.12.1",
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "--rm",
            "-i",
            "--restart=Never",
            "--overrides",
            &overrides,
        ])
        .output()
}

/// Run an ephemeral curl pod with additional kubectl flags (e.g. `--labels`).
fn response_header(output: &[u8], name: &str) -> Option<String> {
    let expected = name.to_ascii_lowercase();
    String::from_utf8_lossy(output).lines().find_map(|line| {
        let (header, value) = line.split_once(':')?;
        header.eq_ignore_ascii_case(&expected).then(|| value.trim().to_owned())
    })
}

/// Wait for the demo environment to be ready.
fn wait_for_environment_ready() -> Result<String, Box<dyn std::error::Error>> {
    // Wait for Grid operators to converge
    for cluster in CLUSTERS {
        let context = format!("kind-grid-provider-traffic-{cluster}");
        eprintln!("  [WAIT] {cluster}: Grid operator convergence");

        // Wait for deployment to be ready
        wait_for_deployment("grid-operator", "grid-system", &context)?;

        // Every site has a provider gateway; only the designated ingress site
        // has the consumer gateway used by the request proof.
        if *cluster == CONSUMER_SITE {
            wait_for_deployment("consumer-gateway", "grid-system", &context)?;
        }
        wait_for_deployment("provider-gateway", "grid-system", &context)?;

        eprintln!("  [OK] {cluster}: Gateways ready");
    }

    Ok("Three provider sites converged; one consumer entrypoint and all provider gateways are ready".to_owned())
}

// -----------------------------------------------------------------------------
// Runtime Assertions
// -----------------------------------------------------------------------------

/// Assert exactly three provider clusters exist and are healthy.
#[expect(
    clippy::too_many_lines,
    reason = "The proof reports one bounded fact set per cluster."
)]
fn assert_cluster_health() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut cluster_health = Vec::new();

    for cluster in CLUSTERS {
        let context = format!("kind-grid-provider-traffic-{cluster}");

        // Check API server responsiveness
        let api_start = Instant::now();
        let output = Command::new("kubectl")
            .args(["cluster-info", "--context", &context])
            .output()?;

        let api_response_ms =
            u64::try_from(api_start.elapsed().as_millis().min(u128::from(u64::MAX))).unwrap_or(u64::MAX);
        let healthy = output.status.success();

        // Get node count
        let nodes_output = Command::new("kubectl")
            .args([
                "get",
                "nodes",
                "--context",
                &context,
                "-o",
                "jsonpath={.items[*].status.conditions[?(@.type=='Ready')].status}",
            ])
            .output()?;

        let ready_nodes = if nodes_output.status.success() {
            u32::try_from(
                String::from_utf8_lossy(&nodes_output.stdout)
                    .split_whitespace()
                    .filter(|s| s == &"True")
                    .count()
                    .min(u32::MAX as usize),
            )
            .unwrap_or(u32::MAX)
        } else {
            0
        };

        cluster_health.push(ClusterHealth {
            name: cluster.to_string(),
            healthy,
            api_response_ms: Some(api_response_ms),
            ready_nodes,
        });

        observed_facts.insert(format!("{cluster}_healthy"), serde_json::Value::Bool(healthy));
        observed_facts.insert(
            format!("{cluster}_ready_nodes"),
            serde_json::Value::Number(ready_nodes.into()),
        );
    }

    let all_healthy = cluster_health.iter().all(|c| c.healthy && c.ready_nodes > 0);
    observed_facts.insert(
        "total_clusters".to_owned(),
        serde_json::Value::Number(CLUSTERS.len().into()),
    );

    if all_healthy {
        Ok(proof_success(
            &format!("All {} clusters are healthy with ready nodes", CLUSTERS.len()),
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "One or more clusters are unhealthy",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert one provider stack per site and one consumer entrypoint.
#[expect(
    clippy::too_many_lines,
    reason = "The proof checks the required deployed components together."
)]
fn assert_component_deployment() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut components = Vec::new();

    let static_components = ["grid-operator", "provider-gateway"];

    for cluster in CLUSTERS {
        let context = format!("kind-grid-provider-traffic-{cluster}");
        let mock_name = format!("vcr-inference-{cluster}");
        let cluster_components: Vec<&str> = static_components
            .iter()
            .copied()
            .chain(std::iter::once(mock_name.as_str()))
            .collect();
        let cluster_components = if *cluster == CONSUMER_SITE {
            cluster_components
                .into_iter()
                .chain(std::iter::once("consumer-gateway"))
                .collect::<Vec<_>>()
        } else {
            cluster_components
        };

        for component in &cluster_components {
            let output = Command::new("kubectl")
                .args([
                    "get",
                    "deployment",
                    component,
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "-o",
                    "jsonpath={.status.readyReplicas},{.status.replicas}",
                ])
                .output()?;

            if output.status.success() {
                let status_str = String::from_utf8_lossy(&output.stdout);
                let parts: Vec<&str> = status_str.trim().split(',').collect();

                let ready_replicas = parts.first().and_then(|s| s.parse::<u32>().ok()).unwrap_or(0);
                let desired_replicas = parts.get(1).and_then(|s| s.parse::<u32>().ok()).unwrap_or(0);
                let ready = ready_replicas > 0 && ready_replicas == desired_replicas;

                components.push(ComponentStatus {
                    name: format!("{cluster}-{component}"),
                    namespace: GRID_SYSTEM_NS.to_owned(),
                    ready_replicas,
                    desired_replicas,
                    ready,
                });

                observed_facts.insert(format!("{cluster}_{component}_ready"), serde_json::Value::Bool(ready));
                observed_facts.insert(
                    format!("{cluster}_{component}_replicas"),
                    serde_json::Value::Number(ready_replicas.into()),
                );
            } else {
                components.push(ComponentStatus {
                    name: format!("{cluster}-{component}"),
                    namespace: GRID_SYSTEM_NS.to_owned(),
                    ready_replicas: 0,
                    desired_replicas: 1,
                    ready: false,
                });
                observed_facts.insert(format!("{cluster}_{component}_ready"), serde_json::Value::Bool(false));
            }
        }
    }

    let expected_count = components.len();
    let all_ready = components.len() == expected_count && components.iter().all(|c| c.ready);
    observed_facts.insert(
        "total_components".to_owned(),
        serde_json::Value::Number(expected_count.into()),
    );

    if all_ready {
        Ok(proof_success(
            &format!(
                "{} components are ready across {} provider sites with one consumer entrypoint",
                expected_count,
                CLUSTERS.len()
            ),
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "One or more components are not ready",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert every site's `GridNetwork` reports all three remote sites connected.
#[expect(
    clippy::too_many_lines,
    clippy::unnecessary_wraps,
    reason = "The assertion framework requires a fallible, named proof boundary."
)]
fn assert_swim_convergence() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let expected_remote_sites = CLUSTERS.len() - 1;
    let mut all_converged = true;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-provider-traffic-{cluster}");
        let status = poll_until(
            || {
                let output = Command::new("kubectl")
                    .args([
                        "get",
                        "gridnetwork/grid-provider-traffic",
                        "--context",
                        &context,
                        "-o",
                        "jsonpath={.status.phase},{.status.connectedSites}",
                    ])
                    .output()?;
                if !output.status.success() {
                    return Ok(None);
                }
                let value = String::from_utf8_lossy(&output.stdout);
                let Some((phase, connected)) = value.trim().split_once(',') else {
                    return Ok(None);
                };
                let connected = connected.parse::<usize>().unwrap_or_default();
                Ok((phase == "Active" && connected == expected_remote_sites).then_some((phase.to_owned(), connected)))
            },
            Duration::from_secs(90),
            Duration::from_secs(3),
        );

        match status {
            Ok((phase, connected)) => {
                observed_facts.insert(format!("{cluster}_phase"), serde_json::Value::String(phase));
                observed_facts.insert(
                    format!("{cluster}_connected_sites"),
                    serde_json::Value::Number(connected.into()),
                );
            },
            Err(error) => {
                all_converged = false;
                observed_facts.insert(format!("{cluster}_error"), serde_json::Value::String(error.to_string()));
            },
        }
    }

    observed_facts.insert(
        "expected_remote_sites".to_owned(),
        serde_json::Value::Number(expected_remote_sites.into()),
    );

    if all_converged {
        Ok(proof_success(
            "Every GridNetwork is Active with all three remote sites connected",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "One or more GridNetworks did not report all three remote sites connected",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Verify that both remote sites are discovered and routing-eligible.
///
/// The locally declared placement site is not a remote SWIM discovery result
/// and is excluded from this assertion.
#[expect(
    clippy::too_many_lines,
    clippy::unnecessary_wraps,
    reason = "The assertion framework requires a fallible, named proof boundary."
)]
fn assert_site_auto_discovery() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let expected_remote_count = CLUSTERS.len() - 1;
    let mut all_ok = true;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-provider-traffic-{cluster}");

        let result = poll_until(
            || {
                let output = Command::new("kubectl")
                    .args([
                        "get", "gridsite",
                        "-l", "grid.praxis-proxy.io/auto-discovered=true",
                        "--context", &context,
                        "-n", GRID_SYSTEM_NS,
                        "-o", "jsonpath={range .items[*]}{.metadata.name}\t{.status.phase}\t{.status.reason}\t{.spec.egress.address}\t{.spec.egress.tls.serverName}\t{.spec.trust.canonicalFingerprints}\n{end}",
                    ])
                    .output()?;
                if !output.status.success() {
                    return Ok(None);
                }
                let body = String::from_utf8_lossy(&output.stdout);
                let mut verified = Vec::new();
                let port_suffix = format!(":{PROVIDER_GATEWAY_PORT}");
                for line in body.lines() {
                    let fields: Vec<&str> = line.trim().split('\t').collect();
                    let [name, phase, reason, addr, server_name, fingerprints, ..] = fields.as_slice() else {
                        continue;
                    };
                    if *phase == "Active"
                        && *reason == "TlsVerified"
                        && addr.ends_with(&port_suffix)
                        && !server_name.is_empty()
                        && !fingerprints.is_empty()
                    {
                        verified.push(serde_json::json!({
                            "name": name,
                            "phase": phase,
                            "reason": reason,
                            "egressAddress": addr,
                            "serverName": server_name,
                            "hasFingerprints": true,
                        }));
                    }
                }
                if verified.len() == expected_remote_count {
                    Ok(Some(verified))
                } else {
                    Ok(None)
                }
            },
            Duration::from_secs(120),
            Duration::from_secs(5),
        );

        match result {
            Ok(remotes) => {
                observed_facts.insert(format!("{cluster}_remote_sites"), serde_json::Value::Array(remotes));
            },
            Err(error) => {
                all_ok = false;
                observed_facts.insert(format!("{cluster}_error"), serde_json::Value::String(error.to_string()));
            },
        }
    }

    observed_facts.insert(
        "expected_remote_count".to_owned(),
        serde_json::Value::Number(expected_remote_count.into()),
    );

    if all_ok {
        Ok(proof_success(
            "Every cluster has two Active/TlsVerified remote `GridSites` with :8443 addresses and trust configuration",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "One or more clusters lack Active auto-discovered remote GridSites",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert each consumer receives and accepts a versioned overlay.
///
/// Validates per-site: `ConfigMap` exists with non-empty data, gateway deployment
/// is ready, and a request is routed through the accepted overlay.
#[expect(
    clippy::too_many_lines,
    reason = "The proof checks overlay, gateway, and routing acceptance together."
)]
fn assert_overlay_acceptance() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut all_accepted = true;

    {
        let cluster = CONSUMER_SITE;
        let context = format!("kind-grid-provider-traffic-{cluster}");

        // Step 1: Overlay ConfigMap exists
        let overlay_output = Command::new("kubectl")
            .args([
                "get",
                "configmap",
                "grid-overlay-grid-provider-traffic-consumer-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.metadata.resourceVersion}",
            ])
            .output()?;

        let resource_version = String::from_utf8_lossy(&overlay_output.stdout).trim().to_owned();
        let overlay_exists = overlay_output.status.success() && !resource_version.is_empty();

        // Step 2: Overlay ConfigMap has non-empty data
        let overlay_data_output = Command::new("kubectl")
            .args([
                "get",
                "configmap",
                "grid-overlay-grid-provider-traffic-consumer-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.data}",
            ])
            .output()?;

        let data_str = String::from_utf8_lossy(&overlay_data_output.stdout).trim().to_owned();
        let has_data = overlay_data_output.status.success() && !data_str.is_empty() && data_str != "{}";

        // Step 3: Consumer gateway deployment is ready
        let deploy_output = Command::new("kubectl")
            .args([
                "get",
                "deployment/consumer-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.status.readyReplicas}",
            ])
            .output()?;

        let gateway_ready = deploy_output.status.success()
            && String::from_utf8_lossy(&deploy_output.stdout)
                .trim()
                .parse::<u32>()
                .is_ok_and(|n| n > 0);

        // Step 4: A valid request proves that the accepted overlay is serving.
        // Praxis health endpoints are exposed only on the pod-local admin listener.
        let routing_output = run_curl_probe(
            &context,
            &format!("overlay-routing-{cluster}"),
            &[
                "curl",
                "--fail-with-body",
                "--silent",
                "--show-error",
                "--max-time",
                "10",
                "--header",
                "Content-Type: application/json",
                "--header",
                "Authorization: Bearer consumer-token",
                "--data",
                r#"{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"overlay probe"}],"max_tokens":16}"#,
                "http://consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
            ],
        )?;

        let routing_ok = routing_output.status.success();

        let overlay_accepted = overlay_exists && has_data && gateway_ready && routing_ok;
        if !overlay_accepted {
            all_accepted = false;
        }

        observed_facts.insert(
            format!("{cluster}_overlay_configmap_exists"),
            serde_json::Value::Bool(overlay_exists),
        );
        observed_facts.insert(format!("{cluster}_overlay_has_data"), serde_json::Value::Bool(has_data));
        observed_facts.insert(
            format!("{cluster}_gateway_ready"),
            serde_json::Value::Bool(gateway_ready),
        );
        observed_facts.insert(
            format!("{cluster}_overlay_routing_ok"),
            serde_json::Value::Bool(routing_ok),
        );
        observed_facts.insert(
            format!("{cluster}_resource_version"),
            serde_json::Value::String(resource_version),
        );
    }

    observed_facts.insert(
        "all_overlays_accepted".to_owned(),
        serde_json::Value::Bool(all_accepted),
    );

    if all_accepted {
        Ok(proof_success(
            "Consumer entrypoint overlay ConfigMap exists with data, gateway ready, and routing passes",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "One or more sites failed overlay acceptance (ConfigMap/data/ready/routing)",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert consumer and operator cannot access provider credentials.
fn require_local_image(image: &str) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("docker")
        .args(["image", "inspect", image])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?;
    if status.success() {
        return Ok(());
    }
    Err(format!(
        "required local image {image:?} is absent; build it or set \
         GRID_XTASK_IMAGE_PULL_POLICY=IfNotPresent with registry image overrides"
    )
    .into())
}

/// Load local container images into all Kind clusters.
///
/// Reads image references from the `GRID_XTASK_*_IMAGE` environment variables
/// (same source as `apply_image_overrides`). When `imagePullPolicy` is not
/// `Never`, this is a no-op.
#[expect(
    clippy::too_many_lines,
    reason = "Image loading is one bounded setup operation across the three clusters."
)]
fn load_images_into_clusters(forge_bin: &Path, resolved_config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let pull_policy = std::env::var("GRID_XTASK_IMAGE_PULL_POLICY").unwrap_or_else(|_| "Never".to_owned());
    if pull_policy != "Never" {
        eprintln!("  skipping Kind image loading (pull policy is {pull_policy})");
        return Ok(());
    }

    let gateway =
        std::env::var("GRID_XTASK_GATEWAY_IMAGE").unwrap_or_else(|_| "praxis-ai:provider-traffic-demo".to_owned());
    let operator =
        std::env::var("GRID_XTASK_OPERATOR_IMAGE").unwrap_or_else(|_| "grid-operator:provider-traffic-demo".to_owned());
    let overlay_sync = crate::env::image_overrides::overlay_sync_image();
    let vcr = crate::env::image_overrides::vcr_image();

    for image in [&gateway, &operator, &overlay_sync, &vcr] {
        require_local_image(image)?;
        eprintln!("  verified local image: {image}");
    }

    for cluster in CLUSTERS {
        for image in [&gateway, &operator, &overlay_sync, &vcr] {
            eprintln!("  loading {image} into {cluster}...");
            let output = Command::new(forge_bin.as_os_str())
                .arg("--config")
                .arg(resolved_config)
                .args(["--non-interactive", "cluster", "load-image", cluster, image])
                .output()?;
            if !output.status.success() {
                return Err(format!(
                    "failed to load {image} into {cluster}: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                )
                .into());
            }
        }
        eprintln!("  [OK] {cluster}: all images loaded");
    }
    Ok(())
}

/// Generate TLS certificates for all provider-traffic identities.
///
/// Must be called BEFORE `forge up` so the certificates exist on the host
/// when `install_provider_boundary` creates the Kubernetes Secrets.
fn stage_provider_boundary() -> Result<(), Box<dyn std::error::Error>> {
    let identities: Vec<String> = CLUSTERS.iter().map(|c| (*c).to_owned()).collect();
    certs::generate_all(&identities)?;

    let wrong_ca = ::certs::generate_ca("Combined Site untrusted test CA")?;
    fs::write(Path::new(CERTS_DIR).join("untrusted-ca.pem"), wrong_ca.cert_pem)?;

    eprintln!("  [OK] TLS certificates generated for provider-a, provider-b, provider-c");
    Ok(())
}

/// Create TLS and credential Secrets in every provider-traffic cluster.
///
/// Must be called AFTER `forge up` since the clusters must exist.  Gateway
/// deployments are restarted so pods pick up the new volume mounts.
fn install_provider_boundary() -> Result<(), Box<dyn std::error::Error>> {
    let certs_dir = Path::new(CERTS_DIR);

    for cluster in CLUSTERS {
        let context = format!("kind-grid-provider-traffic-{cluster}");

        apply_tls_secret(&context, cluster, CONSUMER_TLS_SECRET, certs_dir)?;
        apply_tls_secret(&context, cluster, PROVIDER_TLS_SECRET, certs_dir)?;
        apply_tls_secret(&context, "wrong-org-client", WRONG_ORG_TLS_SECRET, certs_dir)?;

        eprintln!("  [OK] {cluster}: TLS secrets installed");
    }

    Ok(())
}

/// Create a TLS secret from the generated cert, key, and CA files.
#[expect(
    clippy::too_many_lines,
    reason = "TLS secret creation is one bounded setup operation."
)]
fn apply_tls_secret(
    context: &str,
    identity: &str,
    secret_name: &str,
    certs_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "create",
            "secret",
            "generic",
            secret_name,
            &format!(
                "--from-file=tls.crt={}",
                certs_dir.join(format!("{identity}-cert.pem")).display()
            ),
            &format!(
                "--from-file=tls.key={}",
                certs_dir.join(format!("{identity}-key.pem")).display()
            ),
            &format!("--from-file=ca.crt={}", certs_dir.join("ca.pem").display()),
            "--dry-run=client",
            "-o",
            "yaml",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to render {identity} Secret/{secret_name}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    kubectl::apply_manifest(context, &String::from_utf8(output.stdout)?)
}

/// Generate a random 32-byte hex provider credential.
fn generate_provider_credential() -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("openssl").args(["rand", "-hex", "32"]).output()?;
    if !output.status.success() {
        return Err("openssl failed to generate provider credential".into());
    }
    let token = String::from_utf8(output.stdout)?.trim().to_owned();
    if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("openssl returned an invalid provider credential".into());
    }
    Ok(token)
}

/// Create an Opaque Secret with a `token` key.
fn apply_credential_secret(context: &str, secret_name: &str, token: &str) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = format!(
        r#"{{"apiVersion":"v1","kind":"Secret","metadata":{{"name":"{secret_name}","namespace":"{GRID_SYSTEM_NS}"}},"type":"Opaque","stringData":{{"token":"{token}"}}}}"#,
    );
    kubectl::apply_manifest(context, &manifest)
}

/// Data extracted from the operator-created overlay `ConfigMap`.
/// Read a single cluster's overlay `ConfigMap` and return structured data.
///
/// Captures both the Kubernetes `resourceVersion` (per-cluster) and the
/// semantic revision from the `grid.praxis-proxy.io/overlay-revision`
/// annotation (content-addressed, safe to compare across clusters).
#[expect(
    clippy::too_many_lines,
    reason = "The reader validates and parses one Kubernetes ConfigMap response."
)]
fn read_cluster_overlay(cluster: &str) -> Result<OverlayData, Box<dyn std::error::Error>> {
    let context = format!("kind-grid-provider-traffic-{cluster}");

    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "configmap",
            OVERLAY_CONFIGMAP,
            "-o",
            "json",
        ])
        .output()?;

    if !output.status.success() {
        return Err(format!("{cluster}: overlay ConfigMap not found").into());
    }

    let cm: serde_json::Value =
        serde_json::from_slice(&output.stdout).map_err(|e| format!("{cluster}: overlay ConfigMap invalid: {e}"))?;

    let resource_version = cm
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_owned();

    let semantic_revision = cm
        .pointer("/metadata/annotations/grid.praxis-proxy.io~1overlay-revision")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_owned();

    let routing_json = cm
        .pointer("/data/routing-config.json")
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("{cluster}: overlay missing routing-config.json"))?;

    let parsed: serde_json::Value =
        serde_json::from_str(routing_json).map_err(|e| format!("{cluster}: routing-config.json invalid: {e}"))?;

    let raw_candidates = parsed
        .get("candidates")
        .and_then(|v| v.as_array())
        .ok_or_else(|| format!("{cluster}: overlay missing candidates array"))?;

    let mut stable_ids = BTreeMap::new();
    let mut candidates = Vec::new();

    for c in raw_candidates {
        let cluster_field = c.get("cluster").and_then(|v| v.as_str()).unwrap_or("").to_owned();
        let stable_id = c.get("stable_id").and_then(|v| v.as_str()).unwrap_or("").to_owned();
        let kind = c.get("kind").and_then(|v| v.as_str()).unwrap_or("").to_owned();
        let name = c.get("name").and_then(|v| v.as_str()).unwrap_or("").to_owned();
        let site = c.get("site").and_then(|v| v.as_str()).unwrap_or("").to_owned();
        let fresh = c.get("fresh").and_then(serde_json::Value::as_bool);
        let admission_state = c
            .get("admission_state")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        let selection_group = c
            .get("selection_group")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| u32::try_from(value).ok());

        if !cluster_field.is_empty() && !stable_id.is_empty() {
            stable_ids.insert(cluster_field.clone(), stable_id.clone());
        }

        candidates.push(OverlayCandidate {
            kind,
            name,
            site,
            cluster: cluster_field,
            stable_id,
            fresh,
            admission_state,
            selection_group,
        });
    }

    Ok(OverlayData {
        resource_version,
        semantic_revision,
        stable_ids,
        candidates,
    })
}

/// Establish the non-traffic preconditions for the measured picker proof.
///
/// This gate deliberately performs no request. It verifies one consumer
/// replica, the explicit round-robin policy, three fresh `NewAndExisting`
/// candidates in group zero, and three consecutive identical semantic
/// revisions. The measured request window starts only after this gate passes.
#[expect(
    clippy::too_many_lines,
    reason = "The readiness barrier checks all non-traffic serving invariants."
)]
fn wait_for_round_robin_readiness() -> Result<BTreeMap<String, serde_json::Value>, Box<dyn std::error::Error>> {
    let context = "kind-grid-provider-traffic-provider-a";
    let replicas = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "deployment/consumer-gateway",
            "-o",
            "jsonpath={.spec.replicas},{.status.replicas},{.status.readyReplicas}",
        ])
        .output()?;
    let replica_text = String::from_utf8_lossy(&replicas.stdout).trim().to_owned();
    if !replicas.status.success() || replica_text != "1,1,1" {
        return Err(format!("consumer gateway is not exactly one ready replica: {replica_text}").into());
    }

    let mut stable_revision: Option<String> = None;
    let mut stable_signature: Option<String> = None;
    for observation in 1..=3 {
        let overlay = read_cluster_overlay("provider-a")?;
        let configmap = Command::new("kubectl")
            .args([
                "--context",
                context,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                "configmap",
                OVERLAY_CONFIGMAP,
                "-o",
                "json",
            ])
            .output()?;
        if !configmap.status.success() {
            return Err("consumer overlay ConfigMap could not be read".into());
        }
        let configmap_json: serde_json::Value = serde_json::from_slice(&configmap.stdout)?;
        let routing_text = configmap_json
            .pointer("/data/routing-config.json")
            .and_then(serde_json::Value::as_str)
            .ok_or("consumer overlay ConfigMap has no routing-config.json data")?;
        let routing: serde_json::Value = serde_json::from_str(routing_text)?;
        let mode = routing
            .pointer("/selection_policy/mode")
            .and_then(serde_json::Value::as_str)
            .ok_or("overlay does not publish selection_policy.mode")?;
        if mode != "roundRobin" {
            return Err(format!("overlay selection mode is {mode}, expected roundRobin").into());
        }

        let mut candidate_signature = Vec::new();
        for candidate in &overlay.candidates {
            if candidate.selection_group != Some(0) {
                return Err(format!(
                    "candidate {} is not in group 0: {:?}",
                    candidate.cluster, candidate.selection_group
                )
                .into());
            }
            if candidate.fresh == Some(false) {
                return Err(format!("candidate {} is stale", candidate.cluster).into());
            }
            if candidate
                .admission_state
                .as_deref()
                .is_some_and(|state| state != "new_and_existing")
            {
                return Err(format!(
                    "candidate {} is not NewAndExisting: {:?}",
                    candidate.cluster, candidate.admission_state
                )
                .into());
            }
            candidate_signature.push(format!(
                "{}:{}:{:?}:{:?}",
                candidate.cluster, candidate.stable_id, candidate.selection_group, candidate.admission_state
            ));
        }
        candidate_signature.sort();
        let signature = candidate_signature.join("|");
        if overlay.candidates.len() != 3 {
            return Err(format!("expected three candidates, found {}", overlay.candidates.len()).into());
        }
        if stable_revision
            .as_ref()
            .is_some_and(|revision| revision != &overlay.semantic_revision)
            || stable_signature.as_ref().is_some_and(|previous| previous != &signature)
        {
            return Err("overlay changed while establishing the measured proof precondition".into());
        }
        stable_revision = Some(overlay.semantic_revision);
        stable_signature = Some(signature);
        if observation < 3 {
            std::thread::park_timeout(Duration::from_secs(2));
        }
    }

    let serving_revision = stable_revision
        .as_deref()
        .ok_or("round-robin readiness did not establish a semantic revision")?;
    wait_for_consumer_gateway_revision(serving_revision)?;

    let mut facts = BTreeMap::new();
    facts.insert("consumer_gateway_replicas".to_owned(), serde_json::json!(replica_text));
    facts.insert("semantic_revision".to_owned(), serde_json::json!(stable_revision));
    facts.insert("candidate_signature".to_owned(), serde_json::json!(stable_signature));
    facts.insert("selection_policy".to_owned(), serde_json::json!("roundRobin"));
    facts.insert("candidate_count".to_owned(), serde_json::json!(3));
    facts.insert(
        "praxis_serving_revision".to_owned(),
        serde_json::json!(serving_revision),
    );
    Ok(facts)
}

/// Read bounded logs from the single consumer gateway used by the measured
/// provider-traffic proof.
fn consumer_gateway_logs() -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            "kind-grid-provider-traffic-provider-a",
            "-n",
            GRID_SYSTEM_NS,
            "logs",
            "deployment/consumer-gateway",
            "-c",
            "praxis",
            "--tail=300",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to read consumer gateway logs: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 160)
        )
        .into());
    }
    Ok(strip_csi_sgr(&String::from_utf8_lossy(&output.stdout)))
}

/// Strip the ANSI SGR sequences emitted by the gateway's tracing subscriber.
fn strip_csi_sgr(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        if character == '\x1b' {
            if chars.next() == Some('[') {
                for final_byte in chars.by_ref() {
                    if final_byte.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
        } else {
            output.push(character);
        }
    }
    output
}

/// Wait until Praxis reports that the exact final overlay revision is serving.
///
/// The `ConfigMap` and projected file can be ready before the gateway watcher
/// has accepted the file. Starting the measured window earlier would count
/// requests against the initial local-only snapshot and invalidate the
/// round-robin proof.
fn wait_for_consumer_gateway_revision(revision: &str) -> Result<(), Box<dyn std::error::Error>> {
    const TIMEOUT: Duration = Duration::from_secs(90);
    const POLL: Duration = Duration::from_secs(2);
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let logs = consumer_gateway_logs()?;
        let accepted = latest_log_field(&logs, "accepted_revision");
        let serving = latest_log_field(&logs, "serving_revision");
        if accepted.as_deref() == Some(revision) && serving.as_deref() == Some(revision) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let accepted_summary = accepted
                .as_deref()
                .map_or_else(|| "none".to_owned(), |value| safe_truncate_str(value, 16));
            let serving_summary = serving
                .as_deref()
                .map_or_else(|| "none".to_owned(), |value| safe_truncate_str(value, 16));
            return Err(format!(
                "consumer gateway did not report accepted/serving overlay revision {} within {TIMEOUT:?} (accepted={}, serving={})",
                safe_truncate_str(revision, 16),
                accepted_summary,
                serving_summary
            )
            .into());
        }
        std::thread::park_timeout(POLL);
    }
}

/// Return the latest exact tracing field value from bounded gateway logs.
fn latest_log_field(logs: &str, field: &str) -> Option<String> {
    logs.lines()
        .rev()
        .find_map(|line| {
            let prefix = format!("{field}=");
            line.match_indices(&prefix).find_map(|(index, _)| {
                let at_boundary = index == 0
                    || line
                        .get(..index)
                        .and_then(|value| value.chars().next_back())
                        .is_some_and(char::is_whitespace);
                if !at_boundary {
                    return None;
                }
                let value = line.get(index + prefix.len()..)?;
                Some(
                    value
                        .strip_prefix('"')
                        .map_or_else(
                            || value.split_whitespace().next().unwrap_or(""),
                            |value| value.split('"').next().unwrap_or(""),
                        )
                        .to_owned(),
                )
            })
        })
        .filter(|value| !value.is_empty())
}

/// Build the expected provider candidate name set.
fn expected_candidates() -> BTreeSet<String> {
    let mut expected = BTreeSet::new();
    for cluster in CLUSTERS {
        expected.insert(format!("vcr-{cluster}-provider"));
    }

    expected
}

/// Wait for each cluster's operator to produce its expected local candidate.
///
/// Pre-SWIM, each operator only knows its local `InferenceProvider` resources.
/// Global convergence (all candidates on every cluster) happens after SWIM
/// seeding in a separate phase.
#[expect(
    clippy::too_many_lines,
    reason = "The readiness poll checks each local cluster's overlay."
)]
fn wait_for_local_overlays() -> Result<BTreeMap<String, OverlayData>, Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(180);
    let interval = Duration::from_secs(5);
    let start = Instant::now();

    eprintln!("  Polling for local overlay ConfigMaps (timeout {timeout:?})...");

    while start.elapsed() < timeout {
        let mut overlays = BTreeMap::new();
        let mut all_ready = true;

        for cluster in CLUSTERS {
            let expected_local = format!("vcr-{cluster}-provider");
            match read_cluster_overlay(cluster) {
                Ok(data) if data.stable_ids.contains_key(&expected_local) => {
                    eprintln!(
                        "  {cluster}: {expected_local} present (semantic_rev={}, stable_id={})",
                        data.semantic_revision,
                        data.stable_ids.get(&expected_local).map_or("?", String::as_str)
                    );
                    overlays.insert((*cluster).to_owned(), data);
                },
                Ok(data) => {
                    eprintln!(
                        "  {cluster}: overlay present but missing {expected_local} (has: {:?})",
                        data.stable_ids.keys().collect::<Vec<_>>()
                    );
                    all_ready = false;
                },
                Err(_) => {
                    all_ready = false;
                },
            }
        }

        if all_ready && overlays.len() == CLUSTERS.len() {
            return Ok(overlays);
        }

        std::thread::park_timeout(interval);
    }

    collect_overlay_diagnostics();
    Err(format!("Local overlay ConfigMaps not ready after {timeout:?}").into())
}

/// Wait until every provider-traffic cluster serves the same candidate set.
#[expect(
    clippy::too_many_lines,
    reason = "The convergence poll compares the bounded three-cluster overlay set."
)]
fn wait_for_global_overlay_convergence(
    expected: &BTreeSet<String>,
) -> Result<BTreeMap<String, OverlayData>, Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(180);
    let interval = Duration::from_secs(5);
    let start = Instant::now();

    eprintln!(
        "  Waiting for global overlay convergence \
         ({} candidates on all clusters, timeout {timeout:?})...",
        expected.len()
    );

    while start.elapsed() < timeout {
        let mut overlays = BTreeMap::new();
        let mut all_converged = true;

        for cluster in CLUSTERS {
            match read_cluster_overlay(cluster) {
                Ok(data) => {
                    let missing: Vec<&String> = expected.iter().filter(|c| !data.stable_ids.contains_key(*c)).collect();
                    if missing.is_empty() {
                        overlays.insert((*cluster).to_owned(), data);
                    } else {
                        eprintln!("  {cluster}: missing candidates {missing:?}");
                        all_converged = false;
                    }
                },
                Err(_) => {
                    all_converged = false;
                },
            }
        }

        if all_converged && overlays.len() == CLUSTERS.len() {
            let reference_site = CLUSTERS.first().ok_or("CLUSTERS is empty")?;
            let reference = overlays
                .get(*reference_site)
                .ok_or("reference site missing from overlays")?;

            for cluster in CLUSTERS.iter().skip(1) {
                let site_data = overlays
                    .get(*cluster)
                    .ok_or_else(|| format!("{cluster} missing from overlays"))?;
                for candidate in expected {
                    let ref_id = reference
                        .stable_ids
                        .get(candidate)
                        .ok_or_else(|| format!("{reference_site}: missing {candidate}"))?;
                    let site_id = site_data
                        .stable_ids
                        .get(candidate)
                        .ok_or_else(|| format!("{cluster}: missing {candidate}"))?;
                    if site_id != ref_id {
                        return Err(format!(
                            "stable_id mismatch for {candidate}: \
                             {reference_site}={ref_id} vs {cluster}={site_id}"
                        )
                        .into());
                    }
                }
            }

            eprintln!(
                "  Global overlay converged: {} candidates on all {} clusters, stable_ids agree",
                expected.len(),
                CLUSTERS.len()
            );
            return Ok(overlays);
        }

        std::thread::park_timeout(interval);
    }

    collect_overlay_diagnostics();
    Err(format!("Global overlay convergence failed after {timeout:?}").into())
}

/// Collect diagnostic information when the overlay `ConfigMap` fails to converge.
#[expect(
    clippy::too_many_lines,
    reason = "Diagnostics intentionally report each bounded control-plane boundary."
)]
fn collect_overlay_diagnostics() {
    eprintln!("  [DIAG] Collecting overlay failure diagnostics...\n");

    for cluster in CLUSTERS {
        let context = format!("kind-grid-provider-traffic-{cluster}");
        eprintln!("  ======== {cluster} ========");

        eprintln!("  [DIAG] {cluster}: 1. Operator deployment SWIM env vars");
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "deployment",
                    "grid-operator",
                    "-o",
                    "jsonpath={range .spec.template.spec.containers[0].env[*]}{.name}={.value}{'\\n'}{end}",
                ])
                .status(),
        );

        eprintln!("\n  [DIAG] {cluster}: 2. SWIM service details");
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "svc",
                    "grid-operator-swim",
                    "-o",
                    "wide",
                ])
                .status(),
        );
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "endpoints",
                    "grid-operator-swim",
                    "-o",
                    "yaml",
                ])
                .status(),
        );

        eprintln!("  [DIAG] {cluster}: 3. Operator pod status");
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "pods",
                    "-l",
                    "app.kubernetes.io/name=grid-operator",
                    "-o",
                    "wide",
                ])
                .status(),
        );

        eprintln!("  [DIAG] {cluster}: 4. Operator logs (last 50 lines, unfiltered)");
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "logs",
                    "deployment/grid-operator",
                    "--tail=50",
                ])
                .status(),
        );

        eprintln!("\n  [DIAG] {cluster}: 5. GridNetwork CRD status");
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "gridnetwork",
                    "-o",
                    "yaml",
                ])
                .status(),
        );

        eprintln!("  [DIAG] {cluster}: 6. Overlay ConfigMap content");
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "configmap",
                    OVERLAY_CONFIGMAP,
                    "-o",
                    "json",
                ])
                .status(),
        );

        eprintln!("  [DIAG] {cluster}: 7. InferenceProvider CRs");
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "inferenceprovider",
                    "-o",
                    "yaml",
                ])
                .status(),
        );

        eprintln!("  [DIAG] {cluster}: 8. Helm values for grid-operator");
        drop(
            Command::new("helm")
                .args([
                    "get",
                    "values",
                    "grid-operator",
                    "--namespace",
                    GRID_SYSTEM_NS,
                    "--kube-context",
                    &context,
                    "-o",
                    "yaml",
                ])
                .status(),
        );

        eprintln!("  [DIAG] {cluster}: 9. NetworkPolicy in grid-system");
        drop(
            Command::new("kubectl")
                .args(["--context", &context, "-n", GRID_SYSTEM_NS, "get", "networkpolicy"])
                .status(),
        );

        eprintln!();
    }

    eprintln!("  [DIAG] 10. Cross-cluster SWIM connectivity check");
    for cluster in CLUSTERS {
        let context = format!("kind-grid-provider-traffic-{cluster}");
        for target in CLUSTERS {
            if *target == *cluster {
                continue;
            }
            let target_context = format!("kind-grid-provider-traffic-{target}");
            if let Ok(output) = Command::new("kubectl")
                .args([
                    "--context",
                    &target_context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "svc",
                    "grid-operator-swim",
                    "-o",
                    "jsonpath={.status.loadBalancer.ingress[0].ip}",
                ])
                .output()
            {
                let ip = String::from_utf8_lossy(&output.stdout);
                eprintln!("  [DIAG] {cluster} -> {target} (SWIM LB {ip}): testing TCP 7946");
                drop(
                    Command::new("kubectl")
                        .args([
                            "--context",
                            &context,
                            "-n",
                            GRID_SYSTEM_NS,
                            "exec",
                            "deployment/grid-operator",
                            "--",
                            "sh",
                            "-c",
                            &format!(
                                "timeout 3 sh -c 'echo | nc -w 2 {ip} 7946' && echo REACHABLE || echo UNREACHABLE"
                            ),
                        ])
                        .status(),
                );
            }
        }
    }
}

/// Materialize provider gateway configuration from the pre-SWIM overlay map.
///
/// For each cluster, extracts the `stable_id` for the local
/// `vcr-{cluster}-provider` candidate and renders the provider praxis.yaml
/// template with:
/// - `SITE_PLACEHOLDER` → cluster name
/// - `CANDIDATE_ID_PLACEHOLDER` → stable ID from the overlay
///
/// Creates the `provider-gateway-config` `ConfigMap` in each cluster.
#[expect(
    clippy::too_many_lines,
    reason = "Provider config materialization is one bounded setup phase."
)]
fn materialize_provider_config(
    overlays: &BTreeMap<String, OverlayData>,
    demo_root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let template_path = demo_root.join("configs/provider/praxis.yaml");
    let template =
        fs::read_to_string(template_path).map_err(|e| format!("failed to read provider config template: {e}"))?;

    for cluster in CLUSTERS {
        let provider_name = format!("vcr-{cluster}-provider");
        let overlay = overlays
            .get(*cluster)
            .ok_or_else(|| format!("no overlay data for cluster {cluster}"))?;
        let stable_id = overlay.stable_ids.get(&provider_name).ok_or_else(|| {
            format!(
                "{cluster}: no candidate {provider_name} in overlay (has: {:?})",
                overlay.stable_ids.keys().collect::<Vec<_>>()
            )
        })?;

        eprintln!("  {cluster}: {provider_name} -> {stable_id}");

        let rendered = template
            .replace("SITE_PLACEHOLDER", cluster)
            .replace("CANDIDATE_ID_PLACEHOLDER", stable_id);

        let context = format!("kind-grid-provider-traffic-{cluster}");

        let create = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "create",
                "configmap",
                "provider-gateway-config",
                &format!("--from-literal=praxis.yaml={rendered}"),
                "--dry-run=client",
                "-o",
                "yaml",
            ])
            .output()?;

        if !create.status.success() {
            return Err(format!(
                "failed to render provider-gateway-config for {cluster}: {}",
                String::from_utf8_lossy(&create.stderr).trim()
            )
            .into());
        }

        kubectl::apply_manifest(&context, &String::from_utf8(create.stdout)?)?;
        eprintln!("  [OK] {cluster}: provider-gateway-config created (stable_id={stable_id})");
    }

    Ok(())
}

/// Materialize the Forge configuration with image overrides.
fn materialize_config(source: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(source)?;
    let mut config: serde_yaml::Value = serde_yaml::from_str(&content)?;
    apply_image_overrides(&mut config);
    let rendered = serde_yaml::to_string(&config)?;
    let parent = source.parent().ok_or("source config must have parent directory")?;
    let output = parent.join(".forge.resolved.yaml");
    fs::write(&output, rendered)?;
    Ok(output)
}

/// Apply image overrides from environment variables to the Forge configuration.
#[expect(
    clippy::too_many_lines,
    clippy::collapsible_if,
    reason = "Image override application with structured YAML manipulation; nested ifs follow YAML structure hierarchy"
)]
fn apply_image_overrides(config: &mut serde_yaml::Value) {
    let gateway_image =
        std::env::var("GRID_XTASK_GATEWAY_IMAGE").unwrap_or_else(|_| "praxis-ai:provider-traffic-demo".to_owned());
    let operator_image =
        std::env::var("GRID_XTASK_OPERATOR_IMAGE").unwrap_or_else(|_| "grid-operator:provider-traffic-demo".to_owned());
    let overlay_sync_image = crate::env::image_overrides::overlay_sync_image();
    let vcr_image = crate::env::image_overrides::vcr_image();
    let image_pull_policy = std::env::var("GRID_XTASK_IMAGE_PULL_POLICY").unwrap_or_else(|_| "Never".to_owned());

    let (gateway_repo, gateway_tag) = parse_image_ref(&gateway_image);
    let (operator_repo, operator_tag) = parse_image_ref(&operator_image);
    let (overlay_sync_repo, overlay_sync_tag) = parse_image_ref(&overlay_sync_image);

    if let Some(spec) = config.get_mut("spec") {
        if let Some(clusters) = spec.get_mut("clusters") {
            if let Some(clusters_array) = clusters.as_sequence_mut() {
                for cluster in clusters_array {
                    if let Some(properties) = cluster.get_mut("properties") {
                        if let Some(props_map) = properties.as_mapping_mut() {
                            let pairs = [
                                ("gatewayImage", &gateway_image),
                                ("operatorImage", &operator_image),
                                ("vcrImage", &vcr_image),
                                ("imagePullPolicy", &image_pull_policy),
                                ("gatewayImageRepo", &gateway_repo),
                                ("gatewayImageTag", &gateway_tag),
                                ("operatorImageRepo", &operator_repo),
                                ("operatorImageTag", &operator_tag),
                                ("overlaySyncImage", &overlay_sync_image),
                                ("overlaySyncImageRepo", &overlay_sync_repo),
                                ("overlaySyncImageTag", &overlay_sync_tag),
                            ];
                            for (key, val) in pairs {
                                props_map.insert(
                                    serde_yaml::Value::String(key.to_owned()),
                                    serde_yaml::Value::String(val.clone()),
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Parse image reference into (repo, tag) components.
fn parse_image_ref(image: &str) -> (String, String) {
    if let Some(colon_pos) = image.rfind(':') {
        let (repo, tag) = image.split_at(colon_pos);
        // Skip the ':' character
        let tag = tag.strip_prefix(':').unwrap_or(tag);
        (repo.to_owned(), tag.to_owned())
    } else {
        (image.to_owned(), "latest".to_owned())
    }
}

/// Prepare setup context from configuration.
fn prepare_setup(forge_config: &Path) -> Result<ProviderTrafficContext, Box<dyn std::error::Error>> {
    let root = super::demo_root(forge_config);
    eprintln!("Forge config: {}", forge_config.display());
    eprintln!("Demo root:    {}", root.display());
    let resolved_config = materialize_config(forge_config)?;
    let forge_bin = glb::resolve_forge_binary()
        .ok_or("praxis-forge binary not found")?
        .into();

    Ok(ProviderTrafficContext {
        demo_root: root,
        resolved_config,
        forge_bin,
    })
}

/// Authorize auto-discovered remote `GridSites` with identity trust material.
///
/// For each local cluster, waits for the two remote auto-discovered `GridSites`,
/// verifies the SWIM-advertised certificate matches the staged identity, then
/// patches `spec.egress.tls.serverName` and `spec.trust.canonicalFingerprints`.
/// The controller transitions the site to Active naturally after the patch.
fn authorize_discovered_sites() -> Result<(), Box<dyn std::error::Error>> {
    const TRUST_TIMEOUT: Duration = Duration::from_secs(120);
    const GRID_NETWORK: &str = "grid-provider-traffic";

    for local in CLUSTERS {
        let context = format!("kind-grid-provider-traffic-{local}");
        eprintln!();
        eprintln!("  {local}: authorizing remote provider sites");
        for remote in CLUSTERS {
            if *remote == *local {
                continue;
            }
            let site_name = format!("{GRID_NETWORK}-{remote}");
            operator::wait_for_auto_gridsite(&context, &site_name, GRID_NETWORK, TRUST_TIMEOUT)?;
            let canonical_fp = certs::site_certificate_fingerprint(remote)?;
            operator::wait_for_expected_site_certificate(&context, &site_name, &canonical_fp, TRUST_TIMEOUT)?;
            let server_name = format!("{remote}.grid.internal");
            operator::patch_gridsite_identity_trust(&context, &site_name, &canonical_fp, &server_name)?;
            operator::wait_for_gridsite_phase(&context, &site_name, "Active", TRUST_TIMEOUT)?;
        }
    }
    eprintln!("  [OK] All auto-discovered remote GridSites authorized and Active");
    Ok(())
}

/// Deploy the provider-traffic environment.
#[expect(
    clippy::too_many_lines,
    reason = "sequential setup steps: each step depends on the previous; splitting obscures the setup flow"
)]
fn deploy_setup(context: &ProviderTrafficContext) -> Result<OverlayState, Box<dyn std::error::Error>> {
    let total_phases = SETUP_PHASES;
    let mut phase = 0;
    let mut next = || {
        phase += 1;
        phase
    };

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Resolving Forge config and building images",
        next(),
        total_phases
    );

    // Validate the resolved forge configuration
    let output = Command::new(&context.forge_bin)
        .args(["config", "validate", "--config"])
        .arg(&context.resolved_config)
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "Forge config validation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    eprintln!("  [OK] Forge config resolved to {}", context.resolved_config.display());

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Generating TLS certificates for all sites",
        next(),
        total_phases
    );

    stage_provider_boundary()?;

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Creating three provider Kind clusters: provider-a, provider-b, provider-c",
        next(),
        total_phases
    );

    let status = Command::new(&context.forge_bin)
        .args(["up", "--config"])
        .arg(&context.resolved_config)
        .status()?;

    if !status.success() {
        return Err("Failed to create provider-traffic clusters".into());
    }

    eprintln!("  [OK] All three provider clusters created");

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Loading container images into Kind clusters",
        next(),
        total_phases
    );

    load_images_into_clusters(&context.forge_bin, &context.resolved_config)?;

    eprintln!();
    eprintln!("[SETUP {}/{}] Deploying infrastructure stacks", next(), total_phases);

    let apply_stack = |forge_bin: &Path,
                       resolved_config: &Path,
                       cluster: &str,
                       stack: &str|
     -> Result<(), Box<dyn std::error::Error>> {
        eprintln!("  applying {stack} to {cluster}...");
        let status = Command::new(forge_bin)
            .arg("--config")
            .arg(resolved_config)
            .args(["--non-interactive", "stack", "apply", cluster, stack])
            .status()?;
        if !status.success() {
            return Err(format!("Failed to apply {stack} to {cluster}").into());
        }
        Ok(())
    };

    for cluster in CLUSTERS {
        apply_stack(&context.forge_bin, &context.resolved_config, cluster, "metallb")?;
    }
    for cluster in CLUSTERS {
        let op_stack = format!("{cluster}-operator-base");
        apply_stack(&context.forge_bin, &context.resolved_config, cluster, &op_stack)?;
    }
    eprintln!("  [OK] Infrastructure stacks applied");

    eprintln!();
    eprintln!("[SETUP {}/{}] Verifying Grid operators are ready", next(), total_phases);

    for cluster in CLUSTERS {
        let ctx = format!("kind-grid-provider-traffic-{cluster}");
        wait_for_deployment("grid-operator", GRID_SYSTEM_NS, &ctx)?;
        eprintln!("  [OK] {cluster}: Grid operator ready");
    }

    eprintln!();
    eprintln!("[SETUP {}/{}] Deploying VCR backends", next(), total_phases);

    for cluster in CLUSTERS {
        let ctx = format!("kind-grid-provider-traffic-{cluster}");
        let credential = generate_provider_credential()?;
        apply_credential_secret(&ctx, VCR_INFERENCE_CREDENTIAL, &credential)?;
        eprintln!("  [OK] {cluster}: vcr-inference-credential created");
        apply_stack(&context.forge_bin, &context.resolved_config, cluster, "vcr-backend")?;
    }
    eprintln!("  [OK] VCR backends deployed");

    eprintln!();
    eprintln!("[SETUP {}/{}] Deploying grid-site resources", next(), total_phases);

    for cluster in CLUSTERS {
        let site_stack = format!("{cluster}-site");
        apply_stack(&context.forge_bin, &context.resolved_config, cluster, &site_stack)?;
    }
    eprintln!("  [OK] Grid site resources deployed");

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Waiting for local overlay ConfigMaps",
        next(),
        total_phases
    );

    let pre_swim_overlays = wait_for_local_overlays()?;
    eprintln!("  [OK] Local overlay ConfigMaps ready");

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Materializing provider config and installing trust",
        next(),
        total_phases
    );

    install_provider_boundary()?;

    materialize_provider_config(&pre_swim_overlays, &context.demo_root)?;
    eprintln!("  [OK] Provider config materialized, trust installed");

    eprintln!();
    eprintln!("[SETUP {}/{}] Deploying provider gateways", next(), total_phases);

    for cluster in CLUSTERS {
        apply_stack(
            &context.forge_bin,
            &context.resolved_config,
            cluster,
            "provider-gateway",
        )?;
    }
    eprintln!("  [OK] Provider gateways deployed");

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Deploying the single consumer gateway",
        next(),
        total_phases
    );

    apply_stack(
        &context.forge_bin,
        &context.resolved_config,
        CONSUMER_SITE,
        "consumer-gateway",
    )?;
    eprintln!("  [OK] Consumer gateway deployed in {CONSUMER_SITE}");

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] SWIM discovery and trust authorization",
        next(),
        total_phases
    );

    configure_swim_peers(&context.forge_bin, &context.resolved_config)?;
    authorize_discovered_sites()?;

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Waiting for global overlay convergence",
        next(),
        total_phases
    );

    let expected = expected_candidates();
    let post_swim_overlays = wait_for_global_overlay_convergence(&expected)?;
    let environment_status = wait_for_environment_ready()?;
    eprintln!("  [OK] {environment_status}");

    Ok(OverlayState {
        pre_swim: pre_swim_overlays,
        post_swim: post_swim_overlays,
    })
}

// -----------------------------------------------------------------------------
// Demo Scenarios
// -----------------------------------------------------------------------------

/// Run the quick-mode proof scenarios using the assertion framework.
///
/// Run the six focused provider-traffic proof scenarios.
#[expect(
    clippy::too_many_lines,
    reason = "The focused demo presents its six proof phases in order."
)]
fn run_quick_scenarios() -> BTreeMap<String, ProofResult> {
    let mut results = BTreeMap::new();
    let mut scenario_num: usize = 0;
    let mut scenario = || {
        scenario_num += 1;
        scenario_num
    };

    eprintln!();
    eprintln!("=== QUICK MODE SCENARIOS ===");
    eprintln!();

    eprintln!("[SCENARIO {}] Verify three provider clusters are healthy", scenario());
    run_and_insert(&mut results, "cluster_health", assert_cluster_health);

    eprintln!();
    eprintln!("[SCENARIO {}] Verify component deployment", scenario());
    run_and_insert(&mut results, "component_deployment", assert_component_deployment);

    eprintln!();
    eprintln!("[SCENARIO {}] Verify SWIM convergence", scenario());
    run_and_insert(&mut results, "swim_convergence", assert_swim_convergence);

    eprintln!();
    eprintln!("[SCENARIO {}] Verify site auto-discovery", scenario());
    run_and_insert(&mut results, "site_auto_discovery", assert_site_auto_discovery);

    eprintln!();
    eprintln!("[SCENARIO {}] Verify overlay acceptance", scenario());
    run_and_insert(&mut results, "overlay_acceptance", assert_overlay_acceptance);

    eprintln!();
    eprintln!();
    eprintln!(
        "[SCENARIO {}] Prove equal round-robin across distinct provider gateways",
        scenario()
    );
    run_and_insert(
        &mut results,
        "provider_gateway_round_robin",
        assert_provider_gateway_round_robin,
    );

    results
}

/// Send serial, unbound requests through one consumer gateway and verify that
/// the active no-metrics round-robin picker distributes them across the
/// distinct provider gateways.  This is intentionally a request-path proof:
/// the request itself is the only source of the attribution counts.
#[expect(
    clippy::too_many_lines,
    clippy::unnecessary_wraps,
    reason = "The assertion framework requires a fallible, named traffic proof boundary."
)]
fn assert_provider_gateway_round_robin() -> AssertionResult {
    let start = Instant::now();
    let context = "kind-grid-provider-traffic-provider-a";
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut sequence = Vec::new();
    let mut failures = Vec::new();
    let mut overlay_changes = Vec::new();

    let readiness = match wait_for_round_robin_readiness() {
        Ok(facts) => facts,
        Err(error) => {
            return Ok(proof_failure(
                &format!("round-robin readiness gate failed: {error}"),
                BTreeMap::from([(String::from("readiness_error"), serde_json::json!(error.to_string()))]),
                start.elapsed(),
            ));
        },
    };
    let baseline_overlay = match read_cluster_overlay("provider-a") {
        Ok(overlay) => overlay,
        Err(error) => {
            return Ok(proof_failure(
                &format!("could not capture baseline overlay after readiness: {error}"),
                BTreeMap::new(),
                start.elapsed(),
            ));
        },
    };

    for request_number in 1..=60 {
        if let Ok(current_overlay) = read_cluster_overlay("provider-a")
            && (current_overlay.resource_version != baseline_overlay.resource_version
                || current_overlay.semantic_revision != baseline_overlay.semantic_revision)
        {
            overlay_changes.push(serde_json::json!({
                "request": request_number,
                "resource_version": current_overlay.resource_version,
                "semantic_revision": current_overlay.semantic_revision,
            }));
        }
        let request_label = format!("provider-traffic-rr-{request_number:03}");
        let output = run_curl_probe(
            context,
            &format!("rr-{request_number}"),
            &[
                "curl",
                "--fail-with-body",
                "--include",
                "--silent",
                "--show-error",
                "--header",
                "Content-Type: application/json",
                "--header",
                "Authorization: Bearer consumer-token",
                "--data",
                r#"{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"provider traffic proof"}],"max_tokens":8}"#,
                "http://consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
            ],
        );

        match output {
            Ok(output) if output.status.success() => {
                let provider = response_header(&output.stdout, "x-grid-combined-provider-gateway")
                    .or_else(|| response_header(&output.stdout, "x-grid-provider-traffic-provider-gateway"))
                    .or_else(|| response_header(&output.stdout, "x-grid-provider-gateway"));
                if let Some(provider) = provider {
                    *counts.entry(provider.clone()).or_default() += 1;
                    sequence.push(provider);
                } else {
                    failures.push(format!("{request_label}: provider attribution header missing"));
                }
            },
            Ok(output) => failures.push(format!(
                "{request_label}: HTTP probe failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )),
            Err(error) => failures.push(format!("{request_label}: probe execution failed: {error}")),
        }
    }

    let canonical = ["provider-a", "provider-b", "provider-c"];
    let exact_counts = canonical
        .iter()
        .all(|name| counts.get(*name).copied().unwrap_or(0) == 20);
    let cycle_start = sequence
        .first()
        .and_then(|first| canonical.iter().position(|name| *name == first));
    let repeating_cycle = sequence.len() == 60
        && cycle_start.is_some_and(|start_index| {
            sequence.iter().enumerate().all(|(index, provider)| {
                canonical
                    .get((start_index + index) % canonical.len())
                    .is_some_and(|expected| *expected == provider.as_str())
            })
        });
    let balanced = exact_counts && repeating_cycle && failures.is_empty();

    let mut facts = BTreeMap::new();
    facts.insert("request_count".to_owned(), serde_json::json!(sequence.len()));
    facts.insert("provider_counts".to_owned(), serde_json::json!(counts));
    facts.insert("ordered_provider_sequence".to_owned(), serde_json::json!(sequence));
    facts.insert("cycle_start_provider".to_owned(), serde_json::json!(sequence.first()));
    facts.insert("exact_20_each".to_owned(), serde_json::json!(exact_counts));
    facts.insert(
        "repeating_three_provider_cycle".to_owned(),
        serde_json::json!(repeating_cycle),
    );
    facts.insert("failures".to_owned(), serde_json::json!(failures));
    facts.insert("selection_policy".to_owned(), serde_json::json!("roundRobin"));
    facts.insert("scoring_strategy".to_owned(), serde_json::json!("noMetrics"));
    facts.insert("readiness".to_owned(), serde_json::json!(readiness));
    facts.insert(
        "baseline_resource_version".to_owned(),
        serde_json::json!(baseline_overlay.resource_version),
    );
    facts.insert(
        "baseline_semantic_revision".to_owned(),
        serde_json::json!(baseline_overlay.semantic_revision),
    );
    let overlay_stable = overlay_changes.is_empty();
    facts.insert(
        "overlay_changes_during_requests".to_owned(),
        serde_json::json!(overlay_changes),
    );

    if balanced && overlay_stable {
        Ok(proof_success(
            "60 serial requests distributed evenly across three distinct provider gateways",
            facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            &format!(
                "provider gateway distribution or overlay stability failed: counts={counts:?}, overlay_changes={overlay_changes:?}"
            ),
            facts,
            start.elapsed(),
        ))
    }
}

/// Run one proof assertion and retain failures as structured evidence.
fn run_and_insert(results: &mut BTreeMap<String, ProofResult>, name: &str, assertion_fn: fn() -> AssertionResult) {
    match run_assertion(name, assertion_fn) {
        Ok(proof) => {
            results.insert(name.to_owned(), proof);
        },
        Err(error) => {
            eprintln!("  [X] {name} failed: {error}");
            results.insert(
                name.to_owned(),
                proof_failure(&format!("{name} failed: {error}"), BTreeMap::new(), Duration::ZERO),
            );
        },
    }
}

/// Configure SWIM peer discovery by updating each operator with peer seed addresses.
fn configure_swim_peers(_forge_bin: &Path, _resolved_config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut swim_ips = BTreeMap::new();

    for cluster in CLUSTERS {
        let ip = read_swim_lb_ip(cluster)?;
        eprintln!("  {cluster}: SWIM LB IP = {ip}");
        swim_ips.insert(*cluster, ip);
    }

    for cluster in CLUSTERS {
        let this_ip = swim_ips
            .get(cluster)
            .ok_or_else(|| format!("{cluster}: missing SWIM IP"))?;
        let mut peer_parts = Vec::new();
        for c in CLUSTERS {
            if *c != *cluster {
                let ip = swim_ips.get(c).ok_or_else(|| format!("{c}: missing SWIM IP"))?;
                peer_parts.push(format!("{ip}:7946"));
            }
        }
        let peer_seeds = peer_parts.join(",");
        update_operator_swim_config(cluster, this_ip, &peer_seeds)?;
    }

    for cluster in CLUSTERS {
        let ctx = format!("kind-grid-provider-traffic-{cluster}");
        wait_for_deployment("grid-operator", GRID_SYSTEM_NS, &ctx)?;
        eprintln!("  [OK] {cluster}: operator restarted with SWIM config");
    }

    Ok(())
}

/// Read the SWIM `LoadBalancer` IP for a cluster directly from Kubernetes.
fn read_swim_lb_ip(cluster: &str) -> Result<String, Box<dyn std::error::Error>> {
    let context = format!("kind-grid-provider-traffic-{cluster}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "svc",
            "grid-operator-swim",
            "-o",
            "jsonpath={.status.loadBalancer.ingress[0].ip}",
        ])
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "{cluster}: cannot read SWIM service: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }

    let ip = String::from_utf8(output.stdout)?.trim().to_owned();
    if ip.is_empty() {
        return Err(format!("{cluster}: SWIM LoadBalancer has no ingress IP").into());
    }

    Ok(ip)
}

/// Update a single operator's SWIM configuration with peer addresses.
#[expect(
    clippy::too_many_lines,
    reason = "The Helm upgrade carries the bounded SWIM configuration contract."
)]
fn update_operator_swim_config(
    cluster: &str,
    advertise_ip: &str,
    seeds: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let context = format!("kind-grid-provider-traffic-{cluster}");

    let operator_image =
        std::env::var("GRID_XTASK_OPERATOR_IMAGE").unwrap_or_else(|_| "grid-operator:provider-traffic-demo".to_owned());
    let image_pull_policy = std::env::var("GRID_XTASK_IMAGE_PULL_POLICY").unwrap_or_else(|_| "Never".to_owned());
    let (operator_repo, operator_tag) = parse_image_ref(&operator_image);

    let seeds_escaped = seeds.replace(',', "\\,");

    let upgrade_output = Command::new("helm")
        .args([
            "upgrade",
            "grid-operator",
            "charts/grid-operator",
            "--version",
            "0.1.0",
            "--namespace",
            "grid-system",
            "--kube-context",
            &context,
            "--reuse-values",
            "--set",
            &format!("image.repository={operator_repo}"),
            "--set",
            &format!("image.tag={operator_tag}"),
            "--set",
            &format!("image.pullPolicy={image_pull_policy}"),
            "--set",
            &format!("swim.siteName={cluster}"),
            "--set",
            &format!("swim.advertiseAddress={advertise_ip}:7946"),
            "--set",
            &format!("swim.seeds={seeds_escaped}"),
            "--set",
            "swim.service.enabled=true",
            "--set",
            "swim.service.type=LoadBalancer",
            "--set",
            &format!("gateway.serviceName={PROVIDER_GATEWAY_SERVICE}"),
            "--set-string",
            &format!("gateway.port={PROVIDER_GATEWAY_PORT}"),
        ])
        .output()?;

    if !upgrade_output.status.success() {
        return Err(format!(
            "Failed to update SWIM config for {cluster}: {}",
            String::from_utf8_lossy(&upgrade_output.stderr)
        )
        .into());
    }

    eprintln!("  [OK] {cluster}: SWIM peers configured");

    assert_operator_gateway_env(&context, cluster)?;

    Ok(())
}

/// Post-upgrade assertion: the operator deployment must reflect the expected
/// gateway discovery contract after every Helm upgrade.
fn assert_operator_gateway_env(context: &str, cluster: &str) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "deployment/grid-operator",
            "-o",
            "jsonpath={.spec.template.spec.containers[0].env}",
        ])
        .output()?;

    let env_json = String::from_utf8_lossy(&output.stdout);

    let has_service = env_json.contains(&format!(
        "\"name\":\"GRID_GATEWAY_SERVICE_NAME\",\"value\":\"{PROVIDER_GATEWAY_SERVICE}\""
    ));
    let has_port = env_json.contains(&format!(
        "\"name\":\"GRID_GATEWAY_PORT\",\"value\":\"{PROVIDER_GATEWAY_PORT}\""
    ));

    if !has_service || !has_port {
        return Err(format!(
            "{cluster}: operator gateway env mismatch after helm upgrade \
             (expected GRID_GATEWAY_SERVICE_NAME={PROVIDER_GATEWAY_SERVICE}, \
             GRID_GATEWAY_PORT={PROVIDER_GATEWAY_PORT}); got: {env_json}"
        )
        .into());
    }

    Ok(())
}

/// Tear down only the provider-traffic Forge environment.
fn teardown_environment(context: &ProviderTrafficContext) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!();
    eprintln!("=== TEARDOWN ===");

    let status = Command::new(&context.forge_bin)
        .args(["down", "--config"])
        .arg(&context.resolved_config)
        .status()?;

    if !status.success() {
        return Err("failed to tear down provider-traffic environment".into());
    }

    eprintln!("  [OK] Environment torn down successfully");
    Ok(())
}

/// Run the focused provider-traffic demo.
#[expect(
    clippy::too_many_lines,
    reason = "The public demo entrypoint keeps setup, proof, evidence, and teardown visible."
)]
pub(crate) fn run(forge_config: &Path, options: &GlbDemoOptions) -> Result<(), Box<dyn std::error::Error>> {
    if options.mode() != DemoMode::Quick {
        return Err("provider-traffic supports only the focused quick proof".into());
    }
    let mode = DemoMode::Quick;
    let run_id = format_utc_timestamp();
    let wall_start = Instant::now();
    let _started_at = format_utc_iso();

    let evidence_dir = resolve_evidence_dir(forge_config, options, &run_id)?;
    fs::create_dir_all(&evidence_dir)?;

    let setup_ctx = prepare_setup(forge_config);
    let mut teardown_success = false;
    let mut run_error = None;
    let mut overlay_state = OverlayState::default();
    let mut images = BTreeMap::new();

    let proof_results = match &setup_ctx {
        Ok(context) => {
            eprintln!("{OUTPUT_RULE}");
            eprintln!("Grid Provider Traffic Demo");
            eprintln!("Mode: {}", if mode == DemoMode::Quick { "quick" } else { "full" });
            eprintln!("Config: {}", forge_config.display());
            eprintln!("{OUTPUT_RULE}");

            match deploy_setup(context) {
                Ok(state) => {
                    overlay_state = state;
                    images = match collect_image_evidence() {
                        Ok(images) => images,
                        Err(error) => {
                            run_error = Some(format!("image evidence collection failed: {error}"));
                            BTreeMap::new()
                        },
                    };
                    eprintln!();
                    eprintln!("{OUTPUT_RULE}");
                    eprintln!("ENVIRONMENT READY - Starting proof scenarios");
                    eprintln!("{OUTPUT_RULE}");

                    let scenario_results = run_quick_scenarios();

                    let failed_proofs: Vec<&str> = scenario_results
                        .iter()
                        .filter_map(|(name, proof)| (!proof.success).then_some(name.as_str()))
                        .collect();
                    if !failed_proofs.is_empty() {
                        run_error = Some(format!("runtime proofs failed: {}", failed_proofs.join(", ")));
                    }

                    // Teardown if requested
                    if options.teardown && (run_error.is_none() || !options.keep_on_failure) {
                        match teardown_environment(context) {
                            Ok(()) => teardown_success = true,
                            Err(error) => {
                                eprintln!("[WARN]  Teardown failed: {error}");
                                run_error = Some(match run_error {
                                    Some(previous) => format!("{previous}; teardown failed: {error}"),
                                    None => format!("teardown failed: {error}"),
                                });
                            },
                        }
                    }

                    scenario_results
                },
                Err(e) => {
                    eprintln!("[FAIL] Environment setup failed: {e}");
                    run_error = Some(format!("environment setup failed: {e}"));

                    if options.teardown && !options.keep_on_failure {
                        if let Err(cleanup_err) = teardown_environment(context) {
                            eprintln!("[WARN]  Cleanup after setup failure also failed: {cleanup_err}");
                            run_error = Some(format!("environment setup failed: {e}; cleanup failed: {cleanup_err}"));
                        } else {
                            teardown_success = true;
                        }
                    }

                    BTreeMap::new()
                },
            }
        },
        Err(e) => {
            eprintln!("[FAIL] Setup preparation failed: {e}");
            run_error = Some(format!("setup preparation failed: {e}"));
            BTreeMap::new()
        },
    };

    let evidence = Evidence {
        schema_version: EVIDENCE_SCHEMA_VERSION.to_owned(),
        mode: if mode == DemoMode::Quick { "quick" } else { "full" }.to_owned(),
        topology: "provider-traffic".to_owned(),
        clusters: CLUSTERS.iter().map(|&s| s.to_owned()).collect(),
        proof_results,
        images,
        overlay_state,
        cluster_health: Vec::new(),     // Will be populated during runtime assertions
        components: Vec::new(),         // Will be populated during runtime assertions
        swim_membership: Vec::new(),    // Will be populated during runtime assertions
        provider_responses: Vec::new(), // Will be populated during runtime assertions
        security_results: Vec::new(),   // Will be populated during runtime assertions
        teardown_success,
    };

    // Write evidence
    let evidence_file = evidence_dir.join("results.json");
    let evidence_json = serde_json::to_string_pretty(&evidence)?;
    fs::write(&evidence_file, evidence_json)?;

    eprintln!();
    eprintln!("{OUTPUT_RULE}");
    eprintln!("Demo completed in {:.1}s", wall_start.elapsed().as_secs_f64());
    eprintln!("Evidence: {}", evidence_file.display());
    eprintln!("{OUTPUT_RULE}");

    match run_error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

/// Collect actual image evidence from the deployed clusters.
#[expect(
    clippy::too_many_lines,
    reason = "Evidence collection queries the bounded set of deployed component images."
)]
fn collect_image_evidence() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let mut images = BTreeMap::new();

    for cluster in CLUSTERS {
        let context = format!("kind-grid-provider-traffic-{cluster}");

        // Get grid operator image
        let output = Command::new("kubectl")
            .args([
                "get",
                "deployment/grid-operator",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.spec.template.spec.containers[0].image}",
            ])
            .output()?;

        if output.status.success() {
            let operator_image = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            images.insert(format!("{cluster}_operator"), operator_image);
        }

        // Get consumer gateway image
        let output = Command::new("kubectl")
            .args([
                "get",
                "deployment/consumer-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.spec.template.spec.containers[0].image}",
            ])
            .output()?;

        if output.status.success() {
            let consumer_image = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            images.insert(format!("{cluster}_consumer_gateway"), consumer_image);
        }

        // Get provider gateway image
        let output = Command::new("kubectl")
            .args([
                "get",
                "deployment/provider-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.spec.template.spec.containers[0].image}",
            ])
            .output()?;

        if output.status.success() {
            let provider_image = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            images.insert(format!("{cluster}_provider_gateway"), provider_image);
        }

        // Get VCR inference image
        let output = Command::new("kubectl")
            .args([
                "get",
                &format!("deployment/vcr-inference-{cluster}"),
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.spec.template.spec.containers[0].image}",
            ])
            .output()?;

        if output.status.success() {
            let mock_image = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            images.insert(format!("{cluster}_vcr_inference"), mock_image);
        }
    }

    Ok(images)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proof_success_creation() {
        let mut facts = BTreeMap::new();
        facts.insert("cluster_count".to_owned(), serde_json::Value::Number(3.into()));
        facts.insert("all_healthy".to_owned(), serde_json::Value::Bool(true));

        let proof = proof_success("Test success", facts.clone(), Duration::from_millis(100));

        assert!(proof.success);
        assert_eq!(proof.reason, "Test success");
        assert_eq!(proof.duration_ms, 100);
        assert_eq!(proof.observed_facts.len(), 2);
        assert_eq!(
            proof.observed_facts.get("all_healthy"),
            Some(&serde_json::Value::Bool(true))
        );
    }

    #[test]
    fn test_proof_failure_creation() {
        let mut facts = BTreeMap::new();
        facts.insert("error_code".to_owned(), serde_json::Value::Number(500.into()));

        let proof = proof_failure("Test failure", facts.clone(), Duration::from_millis(50));

        assert!(!proof.success);
        assert_eq!(proof.reason, "Test failure");
        assert_eq!(proof.duration_ms, 50);
        assert_eq!(proof.observed_facts.len(), 1);
        assert_eq!(
            proof.observed_facts.get("error_code"),
            Some(&serde_json::Value::Number(500.into()))
        );
    }

    #[test]
    fn test_assertion_result_error_handling() {
        let assertion_fn = || -> AssertionResult { Err("Simulated assertion failure".into()) };

        let result = run_assertion("test_assertion", assertion_fn);
        assert!(result.is_err());

        let Err(error) = result else {
            std::process::abort();
        };
        let error_msg = error.to_string();
        assert!(error_msg.contains("Assertion test_assertion failed"));
        assert!(error_msg.contains("Simulated assertion failure"));
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "This test exercises the complete evidence schema.")]
    fn test_evidence_serialization() {
        let evidence = Evidence {
            schema_version: "test".to_owned(),
            mode: "quick".to_owned(),
            topology: "provider-traffic".to_owned(),
            clusters: vec![
                "provider-a".to_owned(),
                "provider-b".to_owned(),
                "provider-c".to_owned(),
            ],
            proof_results: BTreeMap::new(),
            images: BTreeMap::new(),
            overlay_state: OverlayState::default(),
            cluster_health: vec![ClusterHealth {
                name: "provider-a".to_owned(),
                healthy: true,
                api_response_ms: Some(100),
                ready_nodes: 1,
            }],
            components: vec![ComponentStatus {
                name: "provider-a-grid-operator".to_owned(),
                namespace: "grid-system".to_owned(),
                ready_replicas: 1,
                desired_replicas: 1,
                ready: true,
            }],
            swim_membership: vec![SwimMembership {
                site: "provider-a".to_owned(),
                local_node: "provider-a-operator".to_owned(),
                peers: vec!["provider-b-operator".to_owned(), "provider-c-operator".to_owned()],
                converged: true,
            }],
            provider_responses: vec![],
            security_results: vec![],
            teardown_success: true,
        };

        let Ok(json) = serde_json::to_string(&evidence) else {
            std::process::abort();
        };
        assert!(json.contains("\"schema_version\":\"test\""));
        assert!(json.contains("\"topology\":\"provider-traffic\""));
        assert!(json.contains("\"healthy\":true"));
        assert!(json.contains("\"ready_replicas\":1"));
        assert!(json.contains("\"converged\":true"));

        // Verify deserialization
        let Ok(_deserialized) = serde_json::from_str::<Evidence>(&json) else {
            std::process::abort();
        };
    }

    #[test]
    fn test_proof_count_validation() {
        let names = [
            "cluster_health",
            "component_deployment",
            "swim_convergence",
            "site_auto_discovery",
            "overlay_acceptance",
            "provider_gateway_round_robin",
        ];
        assert_eq!(names.len(), 6);
        assert_eq!(names[0], "cluster_health");
        assert_eq!(names[5], "provider_gateway_round_robin");
        assert_eq!(CLUSTERS.len(), 3);
        assert_eq!(CLUSTERS, &["provider-a", "provider-b", "provider-c"]);
    }

    #[test]
    fn test_evidence_schema_version() {
        assert_eq!(EVIDENCE_SCHEMA_VERSION, "1");
    }

    #[test]
    fn curl_pod_overrides_meets_restricted_pod_security() {
        let json = curl_pod_overrides("test-probe", &["curl", "--fail", "http://example.test"]);
        let actual: serde_json::Value = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            actual,
            serde_json::json!({
                "spec": {
                    "automountServiceAccountToken": false,
                    "securityContext": {
                        "runAsNonRoot": true,
                        "seccompProfile": { "type": "RuntimeDefault" }
                    },
                    "containers": [{
                        "name": "test-probe",
                        "image": "curlimages/curl:8.12.1",
                        "command": ["curl"],
                        "args": ["--fail", "http://example.test"],
                        "securityContext": {
                            "runAsUser": 100,
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": { "drop": ["ALL"] }
                        }
                    }]
                }
            })
        );
    }

    #[test]
    fn provider_traffic_constants_describe_focused_topology() {
        assert_eq!(CLUSTERS, &["provider-a", "provider-b", "provider-c"]);
        assert_eq!(CONSUMER_SITE, "provider-a");
        assert_eq!(EVIDENCE_SCHEMA_VERSION, "1");
    }
}
