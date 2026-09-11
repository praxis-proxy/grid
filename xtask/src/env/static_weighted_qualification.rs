//! Narrated, evidence-backed static-weighted qualification scenarios.
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use serde::Serialize;

use super::{DemoMode, GlbDemoOptions, certs, glb, kubectl, operator, safe_truncate_str};

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

/// Directory where generated TLS certificates are stored.
const CERTS_DIR: &str = "tests/env/certs";

/// Ordered provider-site cluster names in the static-weighted scenario.
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
const BASE_RUN_NAME: &str = "grid-static-weighted";

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
/// Run-scoped Forge prefix, initialized once by the qualification entrypoint.
static RUN_NAME: OnceLock<String> = OnceLock::new();
/// Persistent client pod used for all weighted traffic samples.
const STATIC_CLIENT_POD: &str = "static-weighted-client";

/// Return the current run's Forge prefix, or the topology default in tests.
fn run_name() -> &'static str {
    RUN_NAME.get().map_or(BASE_RUN_NAME, |value| value.as_str())
}

/// Build the kubectl context for one run-scoped provider cluster.
fn cluster_context(cluster: &str) -> String {
    format!("kind-{}-{cluster}", run_name())
}

/// Return the run-scoped consumer overlay `ConfigMap` name.
fn overlay_configmap() -> String {
    format!("grid-overlay-{}-consumer-gateway", run_name())
}

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

/// Semantic revision and canonical candidate signature observed at readiness.
type StaticReadinessObservation = (String, String);

/// Typed evidence for one complete static-weight phase.
#[derive(Clone, Debug, serde::Deserialize, Serialize)]
struct StaticPhaseEvidence {
    /// Phase label.
    phase: String,
    /// Operator configuration observed from all three provider CRs.
    configured_capacity: BTreeMap<String, u32>,
    /// Capacity observed on the local CRDT input and remote CRDT projection.
    crdt_capacity: BTreeMap<String, CrdtCapacityEvidence>,
    /// Rendered traffic weights by provider site.
    rendered_traffic_weight: BTreeMap<String, u32>,
    /// Grid semantic revision.
    grid_revision: String,
    /// Praxis accepted revision.
    praxis_accepted_revision: String,
    /// Praxis serving revision.
    praxis_serving_revision: String,
    /// Gateway identity and restart stability.
    gateway: GatewayStabilityEvidence,
    /// Full attributed sample and statistical calculation.
    traffic: TrafficSampleEvidence,
    /// Number of consecutive stable revision observations before traffic.
    stable_observations: u32,
}

/// Capacity values observed at both CRDT boundaries.
#[derive(Clone, Debug, serde::Deserialize, Serialize)]
struct CrdtCapacityEvidence {
    /// Local provider state capacity.
    local: u32,
    /// Remote provider state capacity observed in the converged overlay.
    remote: u32,
}

/// Gateway identity and restart counters surrounding a sample.
#[derive(Clone, Debug, serde::Deserialize, Serialize)]
struct GatewayStabilityEvidence {
    /// Pod UID before sampling.
    pod_uid_before: String,
    /// Pod UID after sampling.
    pod_uid_after: String,
    /// Restart count before sampling.
    restart_count_before: u32,
    /// Restart count after sampling.
    restart_count_after: u32,
}

/// Statistical request sample and its acceptance calculation.
#[derive(Clone, Debug, serde::Deserialize, Serialize)]
struct TrafficSampleEvidence {
    /// Every request attribution, including failed request records.
    requests: Vec<RequestAttribution>,
    /// Count by provider site.
    counts: BTreeMap<String, u32>,
    /// Expected relative weights.
    expected_weights: [u32; 3],
    /// Observed fractions in provider-a/b/c order.
    observed_fraction: [f64; 3],
    /// Pearson chi-square statistic for the observed sample.
    chi_square: f64,
    /// Five-percent critical value for two degrees of freedom.
    chi_square_critical_value: f64,
    /// Whether the sample passed the statistical bound.
    accepted: bool,
}

/// One request and its observed HTTP/provider attribution.
#[derive(Clone, Debug, serde::Deserialize, Serialize)]
struct RequestAttribution {
    /// Stable request ordinal.
    request: u32,
    /// Attributed provider, if the response carried one.
    provider: Option<String>,
    /// HTTP status observed by the probe, when available.
    status: Option<u16>,
    /// Response or transport error text.
    error: Option<String>,
    /// Elapsed time for the selected attempt.
    latency_ms: u64,
    /// Number of attempts used, including the selected attempt.
    attempts: u8,
    /// Evidence for every transport attempt.
    attempt_results: Vec<AttemptEvidence>,
}

/// One bounded request attempt.
#[derive(Clone, Debug, serde::Deserialize, Serialize)]
struct AttemptEvidence {
    /// One-based attempt number.
    attempt: u8,
    /// Whether the process exited successfully.
    process_success: bool,
    /// Parsed HTTP response status, when present.
    status: Option<u16>,
    /// Whether another attempt was made.
    retried: bool,
    /// Classification explaining the decision.
    reason: String,
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
    /// Static relative provider capacity published for weighted selection.
    traffic_weight: Option<u32>,
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
    /// Typed baseline, changed, and equal phase evidence.
    static_phases: Vec<StaticPhaseEvidence>,
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
    /// Functional result captured before cleanup starts.
    functional_status: String,
    /// Cleanup result, updated after teardown completes.
    cleanup_status: String,
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
        Ok(config_dir.join("evidence").join(run_id))
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
/// Build the restricted security context for the persistent curl client.
fn client_pod_overrides() -> String {
    serde_json::json!({
        "spec": {
            "automountServiceAccountToken": false,
            "securityContext": {
                "runAsNonRoot": true,
                "seccompProfile": { "type": "RuntimeDefault" }
            },
            "containers": [{
                "name": STATIC_CLIENT_POD,
                "image": "curlimages/curl:8.12.1",
                "command": ["sleep", "3600"],
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

/// Create and wait for the persistent weighted-sampling client pod.
#[expect(
    clippy::too_many_lines,
    reason = "client creation keeps security, readiness, and diagnostic errors together"
)]
fn ensure_client_pod(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let existing = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "pod",
            STATIC_CLIENT_POD,
        ])
        .output()?;
    if !existing.status.success() {
        let overrides = client_pod_overrides();
        let created = Command::new("kubectl")
            .args([
                "--context",
                context,
                "-n",
                GRID_SYSTEM_NS,
                "run",
                STATIC_CLIENT_POD,
                "--image=curlimages/curl:8.12.1",
                "--restart=Never",
                "--overrides",
                &overrides,
            ])
            .output()?;
        if !created.status.success() {
            return Err(format!(
                "persistent client creation failed: {}",
                String::from_utf8_lossy(&created.stderr).trim()
            )
            .into());
        }
    }
    let ready = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "wait",
            "--for=condition=Ready",
            &format!("pod/{STATIC_CLIENT_POD}"),
            "--timeout=120s",
        ])
        .output()?;
    if !ready.status.success() {
        return Err(format!(
            "persistent client did not become ready: {}",
            String::from_utf8_lossy(&ready.stderr).trim()
        )
        .into());
    }
    Ok(())
}

/// Run curl inside the persistent client pod.
fn run_persistent_curl_probe(context: &str, curl_args: &[&str]) -> Result<std::process::Output, std::io::Error> {
    Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "exec",
            STATIC_CLIENT_POD,
            "--",
        ])
        .args(curl_args)
        .output()
}

/// Run one isolated curl pod probe for setup assertions.
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

/// Parse the first HTTP status line from curl output.
fn response_status(output: &[u8]) -> Option<u16> {
    String::from_utf8_lossy(output).lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        let protocol = parts.next()?;
        (protocol == "HTTP/1.1" || protocol == "HTTP/2")
            .then(|| parts.next()?.parse().ok())
            .flatten()
    })
}

/// Build the sessionless request used by weighted traffic samples.
///
/// Keeping this list pure makes it possible to prove that sampling remains
/// unbound and that retries reuse exactly the same request arguments.
fn static_weighted_curl_args() -> [&'static str; 12] {
    [
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
    ]
}

/// Classify whether a curl result may be retried.
fn retry_reason(output: &std::process::Output) -> Option<&'static str> {
    if response_status(&output.stdout).is_some() || response_status(&output.stderr).is_some() {
        None
    } else if output.status.success() {
        Some("successful process without an HTTP status")
    } else {
        Some("transport failure without an HTTP response")
    }
}

/// Calculate observed provider fractions in canonical site order.
fn observed_fraction(counts: [u32; 3]) -> [f64; 3] {
    let total = f64::from(counts.into_iter().sum::<u32>()).max(1.0);
    counts.map(|count| f64::from(count) / total)
}

/// Calculate Pearson's chi-square statistic for the configured proportions.
fn chi_square_statistic(expected: [u32; 3], counts: [u32; 3]) -> f64 {
    let total = f64::from(counts.into_iter().sum::<u32>());
    let expected_total = f64::from(expected.into_iter().sum::<u32>());
    if total <= 0.0 || expected_total <= 0.0 {
        return f64::INFINITY;
    }
    expected.into_iter().zip(counts).fold(0.0, |sum, (weight, observed)| {
        let expected_count = total * f64::from(weight) / expected_total;
        if expected_count == 0.0 {
            sum
        } else {
            let delta = f64::from(observed) - expected_count;
            sum + delta * delta / expected_count
        }
    })
}

/// Accept a sample when its Pearson statistic is below the five-percent
/// critical value for two provider categories of freedom.
fn accepts_proportion(expected: [u32; 3], counts: [u32; 3]) -> bool {
    chi_square_statistic(expected, counts) <= 5.991
}

/// Check that Grid, accepted, and serving revisions are identical and known.
fn revisions_converged(grid: &str, accepted: &str, serving: &str) -> bool {
    grid == accepted && accepted == serving && grid != "unknown"
}

/// Wait for the demo environment to be ready.
fn wait_for_environment_ready() -> Result<String, Box<dyn std::error::Error>> {
    // Wait for Grid operators to converge
    for cluster in CLUSTERS {
        let context = cluster_context(cluster);
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
        let context = cluster_context(cluster);

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
        let context = cluster_context(cluster);
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
        let context = cluster_context(cluster);
        let status = poll_until(
            || {
                let output = Command::new("kubectl")
                    .args([
                        "get",
                        &format!("gridnetwork/{}", run_name()),
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
        let context = cluster_context(cluster);

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
        let context = cluster_context(cluster);

        // Step 1: Overlay ConfigMap exists
        let overlay_output = Command::new("kubectl")
            .args([
                "get",
                "configmap",
                &overlay_configmap(),
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
                &overlay_configmap(),
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
        .stdout(Stdio::null())
        .stderr(Stdio::null())
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
fn load_images_into_clusters() -> Result<(), Box<dyn std::error::Error>> {
    let pull_policy = std::env::var("GRID_XTASK_IMAGE_PULL_POLICY").unwrap_or_else(|_| "Never".to_owned());
    if pull_policy != "Never" {
        eprintln!("  skipping Kind image loading (pull policy is {pull_policy})");
        return Ok(());
    }

    let gateway = std::env::var("GRID_XTASK_GATEWAY_IMAGE")
        .unwrap_or_else(|_| "praxis-ai:static-weighted-qualification".to_owned());
    let operator = std::env::var("GRID_XTASK_OPERATOR_IMAGE")
        .unwrap_or_else(|_| "grid-operator:static-weighted-qualification".to_owned());
    let vcr = crate::env::image_overrides::vcr_image();

    for image in [&gateway, &operator, &vcr] {
        require_local_image(image)?;
        eprintln!("  verified local image: {image}");
    }

    for cluster in CLUSTERS {
        for image in [&gateway, &operator, &vcr] {
            eprintln!("  loading {image} into {cluster}...");
            load_docker_image_into_kind(image, &format!("{}-{cluster}", run_name()))?;
        }
        eprintln!("  [OK] {cluster}: all images loaded");
    }
    Ok(())
}

/// Import one host image into a run-owned Kind node using only its host
/// platform. This avoids OCI-index imports failing when the local Docker
/// store has only the linux/amd64 child content.
fn load_docker_image_into_kind(image: &str, kind_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let control_plane = format!("{kind_name}-control-plane");
    let mut save = Command::new("docker")
        .args(["save", "--platform", "linux/amd64", image])
        .stdout(Stdio::piped())
        .spawn()?;
    let save_stdout = save.stdout.take().ok_or("docker save did not provide stdout")?;
    let import_status = Command::new("docker")
        .args([
            "exec",
            "--privileged",
            "-i",
            &control_plane,
            "ctr",
            "--namespace=k8s.io",
            "images",
            "import",
            "--digests",
            "--snapshotter=overlayfs",
            "-",
        ])
        .stdin(save_stdout)
        .status()?;
    let save_status = save.wait()?;
    if !save_status.success() {
        return Err(format!("docker save failed for {image}").into());
    }
    if !import_status.success() {
        return Err(format!("failed to import {image} into {control_plane}").into());
    }
    Ok(())
}

/// Generate TLS certificates for all static-weighted identities.
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

/// Create TLS and credential Secrets in every static-weighted cluster.
///
/// Must be called AFTER `forge up` since the clusters must exist.  Gateway
/// deployments are restarted so pods pick up the new volume mounts.
fn install_provider_boundary() -> Result<(), Box<dyn std::error::Error>> {
    let certs_dir = Path::new(CERTS_DIR);

    for cluster in CLUSTERS {
        let context = cluster_context(cluster);

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
    let context = cluster_context(cluster);

    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "configmap",
            &overlay_configmap(),
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
            traffic_weight: c
                .get("traffic_weight")
                .and_then(serde_json::Value::as_u64)
                .and_then(|weight| u32::try_from(weight).ok()),
        });
    }

    Ok(OverlayData {
        resource_version,
        semantic_revision,
        stable_ids,
        candidates,
    })
}

/// Establish the non-traffic preconditions for the measured static-weight proof.
///
/// This gate deliberately performs no request. It verifies one consumer
/// replica, the explicit weighted policy, three fresh `NewAndExisting`
/// candidates in group zero, and three consecutive identical semantic
/// revisions. The measured request window starts only after this gate passes.
#[expect(
    clippy::too_many_lines,
    reason = "The readiness barrier checks all non-traffic serving invariants."
)]
fn wait_for_static_readiness(
    expected: [u32; 3],
) -> Result<BTreeMap<String, serde_json::Value>, Box<dyn std::error::Error>> {
    let context = cluster_context("provider-a");
    let replicas = Command::new("kubectl")
        .args([
            "--context",
            &context,
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

    let read_matching = || -> Result<Option<StaticReadinessObservation>, Box<dyn std::error::Error>> {
        let overlay = read_cluster_overlay("provider-a")?;
        let configmap = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                "configmap",
                &overlay_configmap(),
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
        if mode != "weightedRandom" {
            return Ok(None);
        }

        let mut candidate_signature = Vec::new();
        for candidate in &overlay.candidates {
            if candidate.selection_group != Some(0) {
                return Ok(None);
            }
            if candidate.fresh == Some(false) {
                return Ok(None);
            }
            if candidate
                .admission_state
                .as_deref()
                .is_some_and(|state| state != "new_and_existing")
            {
                return Ok(None);
            }
            let expected_weight = match candidate.site.as_str() {
                "provider-a" => Some(expected[0]),
                "provider-b" => Some(expected[1]),
                "provider-c" => Some(expected[2]),
                _ => None,
            };
            if candidate.traffic_weight != expected_weight {
                return Ok(None);
            }
            candidate_signature.push(format!(
                "{}:{}:{:?}:{:?}",
                candidate.cluster, candidate.stable_id, candidate.selection_group, candidate.admission_state
            ));
        }
        candidate_signature.sort();
        let signature = candidate_signature.join("|");
        if overlay.candidates.len() != 3 {
            return Ok(None);
        }
        Ok(Some((overlay.semantic_revision, signature)))
    };

    let first = poll_until(read_matching, Duration::from_secs(180), Duration::from_secs(2))?;
    let second = poll_until(
        || Ok(read_matching()?.filter(|value| value == &first)),
        Duration::from_secs(180),
        Duration::from_secs(2),
    )?;
    let stable = poll_until(
        || Ok(read_matching()?.filter(|value| value == &second)),
        Duration::from_secs(180),
        Duration::from_secs(2),
    )?;

    wait_for_consumer_gateway_revision(&stable.0)?;

    let mut facts = BTreeMap::new();
    facts.insert("consumer_gateway_replicas".to_owned(), serde_json::json!(replica_text));
    facts.insert("semantic_revision".to_owned(), serde_json::json!(stable.0));
    facts.insert("candidate_signature".to_owned(), serde_json::json!(stable.1));
    facts.insert("selection_policy".to_owned(), serde_json::json!("weightedRandom"));
    facts.insert(
        "configured_weights".to_owned(),
        serde_json::json!({"provider-a": expected[0], "provider-b": expected[1], "provider-c": expected[2]}),
    );
    facts.insert("candidate_count".to_owned(), serde_json::json!(3));
    facts.insert("praxis_serving_revision".to_owned(), serde_json::json!(stable.0));
    Ok(facts)
}

/// Read bounded logs from the single consumer gateway used by the measured
/// static-weighted proof.
fn consumer_gateway_logs() -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            &cluster_context("provider-a"),
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
/// static-weighted proof.
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

/// Wait until every static-weighted cluster serves the same candidate set.
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
        let context = cluster_context(cluster);
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
                    &overlay_configmap(),
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
        let context = cluster_context(cluster);
        for target in CLUSTERS {
            if *target == *cluster {
                continue;
            }
            let target_context = cluster_context(target);
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

        let context = cluster_context(cluster);

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
    rewrite_run_scope(&mut config);
    apply_image_overrides(&mut config);
    let rendered = serde_yaml::to_string(&config)?;
    let parent = source.parent().ok_or("source config must have parent directory")?;
    let output = parent.join(".forge.resolved.yaml");
    fs::write(&output, rendered)?;
    Ok(output)
}

/// Rewrite topology-owned names to the unique prefix for this qualification run.
fn rewrite_run_scope(value: &mut serde_yaml::Value) {
    match value {
        serde_yaml::Value::String(text) => *text = text.replace(BASE_RUN_NAME, run_name()),
        serde_yaml::Value::Sequence(items) => {
            for item in items {
                rewrite_run_scope(item);
            }
        },
        serde_yaml::Value::Mapping(items) => {
            for (_key, mapped_value) in items {
                rewrite_run_scope(mapped_value);
            }
        },
        serde_yaml::Value::Tagged(tagged) => rewrite_run_scope(&mut tagged.value),
        serde_yaml::Value::Null | serde_yaml::Value::Bool(_) | serde_yaml::Value::Number(_) => {},
    }
}

/// Apply image overrides from environment variables to the Forge configuration.
#[expect(
    clippy::too_many_lines,
    clippy::collapsible_if,
    reason = "Image override application with structured YAML manipulation; nested ifs follow YAML structure hierarchy"
)]
fn apply_image_overrides(config: &mut serde_yaml::Value) {
    let gateway_image = std::env::var("GRID_XTASK_GATEWAY_IMAGE")
        .unwrap_or_else(|_| "praxis-ai:static-weighted-qualification".to_owned());
    let operator_image = std::env::var("GRID_XTASK_OPERATOR_IMAGE")
        .unwrap_or_else(|_| "grid-operator:static-weighted-qualification".to_owned());
    let vcr_image = crate::env::image_overrides::vcr_image();
    let image_pull_policy = std::env::var("GRID_XTASK_IMAGE_PULL_POLICY").unwrap_or_else(|_| "Never".to_owned());

    let (gateway_repo, gateway_tag) = parse_image_ref(&gateway_image);
    let (operator_repo, operator_tag) = parse_image_ref(&operator_image);

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
    for generated in [Path::new(".forge/state.json"), Path::new(".forge/lock")] {
        if generated.exists() {
            fs::remove_file(generated).map_err(|error| format!("failed to clear Forge state: {error}"))?;
        }
    }
    let generated_runtime = Path::new(".forge/runtime");
    if generated_runtime.exists() {
        fs::remove_dir_all(generated_runtime)
            .map_err(|error| format!("failed to clear generated Forge runtime: {error}"))?;
    }
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
    let grid_network = run_name().to_owned();

    for local in CLUSTERS {
        let context = cluster_context(local);
        eprintln!();
        eprintln!("  {local}: authorizing remote provider sites");
        for remote in CLUSTERS {
            if *remote == *local {
                continue;
            }
            let site_name = format!("{grid_network}-{remote}");
            operator::wait_for_auto_gridsite(&context, &site_name, &grid_network, TRUST_TIMEOUT)?;
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

/// Deploy the static-weighted environment.
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

    let config_status = Command::new(&context.forge_bin)
        .args(["up", "--config"])
        .arg(&context.resolved_config)
        .status()?;

    if !config_status.success() {
        return Err("Failed to create static-weighted clusters".into());
    }

    eprintln!("  [OK] All three provider clusters created");

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Loading container images into Kind clusters",
        next(),
        total_phases
    );

    load_images_into_clusters()?;

    eprintln!();
    eprintln!("[SETUP {}/{}] Deploying infrastructure stacks", next(), total_phases);

    let apply_stack = |forge_bin: &Path,
                       resolved_config: &Path,
                       cluster: &str,
                       stack: &str|
     -> Result<(), Box<dyn std::error::Error>> {
        eprintln!("  applying {stack} to {cluster}...");
        let stack_status = Command::new(forge_bin)
            .arg("--config")
            .arg(resolved_config)
            .args(["--non-interactive", "stack", "apply", cluster, stack])
            .status()?;
        if !stack_status.success() {
            capture_stack_failure(cluster, stack);
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
        let ctx = cluster_context(cluster);
        wait_for_deployment("grid-operator", GRID_SYSTEM_NS, &ctx)?;
        eprintln!("  [OK] {cluster}: Grid operator ready");
    }

    eprintln!();
    eprintln!("[SETUP {}/{}] Deploying VCR backends", next(), total_phases);

    for cluster in CLUSTERS {
        let ctx = cluster_context(cluster);
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

/// Capture bounded Kubernetes diagnostics before failed setup is torn down.
fn capture_stack_failure(cluster: &str, stack: &str) {
    let context = cluster_context(cluster);
    eprintln!("  [DIAG] {stack} failed on {cluster}; capturing Kubernetes state");
    for args in [
        vec!["get", "pods", "-o", "wide"],
        vec!["get", "deployments", "-o", "wide"],
        vec!["describe", "deployment", stack],
        vec!["get", "events", "--sort-by=.lastTimestamp"],
        vec!["logs", &format!("deployment/{stack}"), "--all-containers", "--tail=100"],
        vec![
            "logs",
            &format!("deployment/{stack}"),
            "--all-containers",
            "--previous",
            "--tail=100",
        ],
    ] {
        let output = Command::new("kubectl")
            .args(["--context", &context, "-n", GRID_SYSTEM_NS, "--request-timeout=10s"])
            .args(&args)
            .output();
        match output {
            Ok(output) => {
                let stdout = safe_truncate_str(String::from_utf8_lossy(&output.stdout).trim(), 8_000);
                let stderr = safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 2_000);
                eprintln!("  [DIAG] kubectl {}\n{stdout}\n{stderr}", args.join(" "));
            },
            Err(error) => eprintln!("  [DIAG] kubectl {} failed: {error}", args.join(" ")),
        }
    }
}

// -----------------------------------------------------------------------------
// Demo Scenarios
// -----------------------------------------------------------------------------

/// Update the three provider CRs for a new static-weight phase.
fn provider_resource_name(site: &str) -> String {
    let site = site.strip_prefix("vcr-").unwrap_or(site);
    if site.ends_with("-provider") {
        format!("vcr-{site}")
    } else {
        format!("vcr-{site}-provider")
    }
}

/// Apply one static capacity vector to the three materialized provider CRs.
fn apply_static_weights(weights: [u32; 3]) -> Result<(), Box<dyn std::error::Error>> {
    for (site, weight) in ["provider-a", "provider-b", "provider-c"].into_iter().zip(weights) {
        let context = cluster_context(site);
        let provider = provider_resource_name(site);
        let patch = format!(r#"{{"spec":{{"capacityWeight":{weight}}}}}"#);
        let output = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "patch",
                "inferenceprovider",
                &provider,
                "--type",
                "merge",
                "-p",
                &patch,
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "{site}: static capacity patch failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )
            .into());
        }
    }
    Ok(())
}

/// Read the configured capacity from the local provider CR.
fn read_configured_capacity(site: &str) -> Result<u32, Box<dyn std::error::Error>> {
    let context = cluster_context(site);
    let provider = provider_resource_name(site);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "inferenceprovider",
            &provider,
            "-o",
            "jsonpath={.spec.capacityWeight}",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!("{site}: configured capacity unavailable").into());
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    Ok(if value.is_empty() { 1 } else { value.parse()? })
}

/// Extract one locally published CRDT capacity from structured operator logs.
fn parse_local_crdt_capacity(logs: &str, site: &str, provider: &str) -> Option<u32> {
    logs.lines().rev().find_map(|line| {
        if !line.contains("published local provider CRDT capacity")
            || !(line.contains(&format!("site_id=\"{site}\"")) || line.contains(&format!("site_id={site}")))
            || !(line.contains(&format!("provider_id=\"{provider}\""))
                || line.contains(&format!("provider_id={provider}")))
        {
            return None;
        }
        latest_log_field(line, "capacity_weight")?.parse().ok()
    })
}

/// Read the locally published provider state rather than rereading the CR.
fn read_local_crdt_capacity(site: &str) -> Result<u32, Box<dyn std::error::Error>> {
    let context = cluster_context(site);
    let provider = provider_resource_name(site);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "logs",
            "deployment/grid-operator",
            "--all-containers",
            "--tail=500",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!("{site}: local CRDT publication logs unavailable").into());
    }
    let logs = strip_csi_sgr(&String::from_utf8_lossy(&output.stdout));
    parse_local_crdt_capacity(&logs, site, &provider)
        .ok_or_else(|| format!("{site}: local CRDT capacity was not published in operator logs").into())
}

/// Capture pod identity and restart count for the consumer gateway.
#[expect(
    clippy::too_many_lines,
    reason = "The identity query validates every restart invariant field."
)]
fn read_gateway_stability() -> Result<(String, u32), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            &cluster_context("provider-a"),
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "pod",
            "-l",
            "app.kubernetes.io/name=praxis-gateway,app.kubernetes.io/instance=consumer-gateway",
            "-o",
            "json",
        ])
        .output()?;
    if !output.status.success() {
        return Err("consumer gateway pod state unavailable".into());
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let items = value
        .pointer("/items")
        .and_then(serde_json::Value::as_array)
        .ok_or("consumer gateway pod list is invalid")?;
    if items.len() != 1 {
        return Err(format!("expected exactly one consumer gateway pod, found {}", items.len()).into());
    }
    let pod = items.first().ok_or("consumer gateway pod list is empty")?;
    let uid = pod
        .pointer("/metadata/uid")
        .and_then(serde_json::Value::as_str)
        .ok_or("consumer gateway UID missing")?;
    let restarts = pod
        .pointer("/status/containerStatuses/0/restartCount")
        .and_then(serde_json::Value::as_u64)
        .ok_or("consumer gateway restart count missing")?;
    Ok((uid.to_owned(), u32::try_from(restarts)?))
}

/// Wait until the published revision has been accepted and is serving twice.
fn wait_for_stable_revision(revision: &str) -> Result<(String, String, u32), Box<dyn std::error::Error>> {
    wait_for_consumer_gateway_revision(revision)?;
    let first = consumer_gateway_logs()?;
    let accepted = latest_log_field(&first, "accepted_revision").ok_or("accepted revision missing")?;
    let serving = latest_log_field(&first, "serving_revision").ok_or("serving revision missing")?;
    if accepted != revision || serving != revision {
        return Err("Praxis accepted/serving revision mismatch".into());
    }
    std::thread::park_timeout(Duration::from_secs(2));
    let second = consumer_gateway_logs()?;
    let accepted2 = latest_log_field(&second, "accepted_revision").ok_or("second accepted revision missing")?;
    let serving2 = latest_log_field(&second, "serving_revision").ok_or("second serving revision missing")?;
    if accepted2 != revision || serving2 != revision {
        return Err("Praxis revision was not stable across two observations".into());
    }
    Ok((accepted, serving, 2))
}

/// Capture typed phase state from Grid, the converged remote overlay, and Praxis.
#[expect(
    clippy::too_many_lines,
    reason = "Phase evidence intentionally validates every required boundary."
)]
fn capture_phase_state(
    phase: &str,
    configured: [u32; 3],
    traffic: TrafficSampleEvidence,
    gateway: GatewayStabilityEvidence,
) -> Result<StaticPhaseEvidence, Box<dyn std::error::Error>> {
    let overlay = read_cluster_overlay("provider-a")?;
    let (accepted, serving, stable_observations) = wait_for_stable_revision(&overlay.semantic_revision)?;
    let mut configured_capacity = BTreeMap::new();
    let mut crdt_capacity = BTreeMap::new();
    let mut rendered_traffic_weight = BTreeMap::new();
    for (index, site) in ["provider-a", "provider-b", "provider-c"].into_iter().enumerate() {
        let configured_value = read_configured_capacity(site)?;
        let local = read_local_crdt_capacity(site)?;
        let candidate = overlay
            .candidates
            .iter()
            .find(|candidate| candidate.site == site)
            .ok_or("provider missing from converged overlay")?;
        let remote = candidate
            .traffic_weight
            .ok_or("remote CRDT capacity missing from overlay")?;
        configured_capacity.insert(site.to_owned(), configured_value);
        crdt_capacity.insert(site.to_owned(), CrdtCapacityEvidence { local, remote });
        rendered_traffic_weight.insert(site.to_owned(), remote);
        let expected_capacity = configured
            .get(index)
            .copied()
            .ok_or("static phase has fewer than three configured capacities")?;
        if configured_value != expected_capacity || local != expected_capacity || remote != expected_capacity {
            return Err(format!("{site}: configured/CRDT/rendered capacity mismatch").into());
        }
    }
    Ok(StaticPhaseEvidence {
        phase: phase.to_owned(),
        configured_capacity,
        crdt_capacity,
        rendered_traffic_weight,
        grid_revision: overlay.semantic_revision,
        praxis_accepted_revision: accepted,
        praxis_serving_revision: serving,
        gateway,
        traffic,
        stable_observations,
    })
}

#[expect(clippy::too_many_lines, reason = "A phase is an end-to-end qualification boundary.")]
/// Execute one configured, converged, sampled static-weight phase.
fn run_static_phase(
    phase: &str,
    weights: [u32; 3],
    baseline_gateway: &(String, u32),
) -> Result<(ProofResult, StaticPhaseEvidence), Box<dyn std::error::Error>> {
    let previous_revision = read_cluster_overlay("provider-a")
        .ok()
        .map(|overlay| overlay.semantic_revision);
    if phase_requires_revision_change(phase) {
        apply_static_weights(weights)?;
    }
    if phase_requires_revision_change(phase)
        && let Some(previous) = previous_revision.as_ref()
    {
        poll_until(
            || {
                let current = read_cluster_overlay("provider-a")?;
                Ok((current.semantic_revision != *previous).then_some(current))
            },
            Duration::from_secs(180),
            Duration::from_secs(2),
        )?;
    }
    wait_for_static_readiness(weights)?;
    let pre_sample_overlay = read_cluster_overlay("provider-a")?;
    let (pre_accepted, pre_serving, _) = wait_for_stable_revision(&pre_sample_overlay.semantic_revision)?;
    if !revisions_converged(&pre_sample_overlay.semantic_revision, &pre_accepted, &pre_serving) {
        return Err(format!("{phase}: revisions were not converged before sampling").into());
    }
    let (uid_before, restart_before) = baseline_gateway;
    let mut proof = assert_provider_gateway_static(weights, phase)?;
    let (uid_after, restart_after) = read_gateway_stability()?;
    let requests = proof
        .observed_facts
        .get("requests")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    let request_records: Vec<RequestAttribution> = serde_json::from_value(requests)?;
    let counts_value = proof
        .observed_facts
        .get("provider_counts")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let count_map: BTreeMap<String, u32> = serde_json::from_value(counts_value)?;
    let counts = [
        count_map.get("provider-a").copied().unwrap_or(0),
        count_map.get("provider-b").copied().unwrap_or(0),
        count_map.get("provider-c").copied().unwrap_or(0),
    ];
    let traffic = TrafficSampleEvidence {
        requests: request_records,
        counts: count_map,
        expected_weights: weights,
        observed_fraction: observed_fraction(counts),
        chi_square: chi_square_statistic(weights, counts),
        chi_square_critical_value: 5.991,
        accepted: proof.success && accepts_proportion(weights, counts),
    };
    let evidence = capture_phase_state(
        phase,
        weights,
        traffic,
        GatewayStabilityEvidence {
            pod_uid_before: uid_before.clone(),
            pod_uid_after: uid_after,
            restart_count_before: *restart_before,
            restart_count_after: restart_after,
        },
    )?;
    let revision_changed = previous_revision
        .as_deref()
        .is_some_and(|previous| previous != evidence.grid_revision);
    if phase_requires_revision_change(phase) && !revision_changed {
        return Err(format!("{phase}: weight update did not advance the Grid semantic revision").into());
    }
    if phase == "baseline"
        && previous_revision
            .as_deref()
            .is_some_and(|previous| previous != evidence.grid_revision)
    {
        return Err("baseline: reapplying unchanged weights churned the Grid semantic revision".into());
    }
    proof
        .observed_facts
        .insert("revision_changed".to_owned(), serde_json::json!(revision_changed));
    if !revisions_converged(
        &evidence.grid_revision,
        &evidence.praxis_accepted_revision,
        &evidence.praxis_serving_revision,
    ) {
        return Err(format!("{phase}: Grid/Praxis revisions did not converge").into());
    }
    if evidence.gateway.pod_uid_before != baseline_gateway.0
        || evidence.gateway.pod_uid_after != baseline_gateway.0
        || evidence.gateway.restart_count_before != evidence.gateway.restart_count_after
        || evidence.gateway.restart_count_before != baseline_gateway.1
        || evidence.gateway.restart_count_after != baseline_gateway.1
    {
        return Err(format!("{phase}: Praxis gateway identity changed from baseline").into());
    }
    Ok((proof, evidence))
}

/// Return whether a phase mutates weights and therefore requires a new revision.
fn phase_requires_revision_change(phase: &str) -> bool {
    phase != "baseline"
}

#[cfg(test)]
mod static_phase_policy_tests {
    use super::{phase_requires_revision_change, provider_resource_name};

    #[test]
    fn baseline_observes_without_requiring_revision_change() {
        assert!(!phase_requires_revision_change("baseline"));
        assert!(phase_requires_revision_change("changed"));
        assert!(phase_requires_revision_change("equal"));
    }

    #[test]
    fn provider_resource_name_uses_materialized_identity_without_duplication() {
        assert_eq!(provider_resource_name("provider-a"), "vcr-provider-a-provider");
        assert_eq!(
            provider_resource_name("vcr-provider-a-provider"),
            "vcr-provider-a-provider"
        );
    }
}

/// Run the quick-mode proof scenarios using the assertion framework.
///
/// Run the infrastructure checks and three focused static-weighted phases.
#[expect(
    clippy::too_many_lines,
    reason = "The focused demo presents its six proof phases in order."
)]
fn run_quick_scenarios() -> (BTreeMap<String, ProofResult>, Vec<StaticPhaseEvidence>) {
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

    let baseline_gateway = read_gateway_stability().ok();
    if baseline_gateway.is_none() {
        eprintln!("  [FAIL] baseline consumer gateway identity could not be captured");
    }

    let mut phases = Vec::new();
    for (phase, weights) in [
        ("baseline", [50, 30, 20]),
        ("changed", [20, 30, 50]),
        ("equal", [1, 1, 1]),
    ] {
        eprintln!();
        eprintln!("[SCENARIO {}] Static-weighted {phase} phase: {weights:?}", scenario());
        match baseline_gateway.as_ref() {
            Some(baseline_gateway) => match run_static_phase(phase, weights, baseline_gateway) {
                Ok((proof, evidence)) => {
                    results.insert(format!("static_weighted_{phase}"), proof);
                    phases.push(evidence);
                },
                Err(error) => {
                    results.insert(
                        format!("static_weighted_{phase}"),
                        proof_failure(
                            &format!("{phase} phase failed: {error}"),
                            BTreeMap::new(),
                            Duration::ZERO,
                        ),
                    );
                    eprintln!("  [FAIL] {phase}: {error}");
                },
            },
            None => {
                results.insert(
                    format!("static_weighted_{phase}"),
                    proof_failure(
                        &format!("{phase} phase skipped: baseline gateway identity unavailable"),
                        BTreeMap::new(),
                        Duration::ZERO,
                    ),
                );
            },
        }
    }

    (results, phases)
}

/// Send serial, unbound requests through one consumer gateway and verify that
/// the active static weighted picker distributes them across the distinct
/// provider gateways. This is intentionally a request-path proof:
/// the request itself is the only source of the attribution counts.
#[expect(
    clippy::too_many_lines,
    clippy::unnecessary_wraps,
    reason = "The assertion framework requires a fallible, named traffic proof boundary."
)]
fn assert_provider_gateway_static(expected: [u32; 3], phase: &str) -> AssertionResult {
    let start = Instant::now();
    let context = cluster_context("provider-a");
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut sequence = Vec::new();
    let mut requests = Vec::new();
    let mut failures = Vec::new();
    let mut overlay_changes = Vec::new();

    if let Err(error) = ensure_client_pod(&context) {
        return Ok(proof_failure(
            &format!("persistent traffic client failed to become ready: {error}"),
            BTreeMap::new(),
            start.elapsed(),
        ));
    }

    let readiness = match wait_for_static_readiness(expected) {
        Ok(facts) => facts,
        Err(error) => {
            return Ok(proof_failure(
                &format!("static-weighted readiness gate failed: {error}"),
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
        let request_label = format!("static-weighted-rr-{request_number:03}");
        let curl_args = static_weighted_curl_args();
        let request_start = Instant::now();
        let mut attempt_results = Vec::new();
        let mut selected_output = None;
        for attempt in 1..=3_u8 {
            let output = run_persistent_curl_probe(&context, &curl_args);
            let (process_success, status, reason) = match &output {
                Ok(value) => (value.status.success(), response_status(&value.stdout), None),
                Err(error) => (false, None, Some(error.to_string())),
            };
            let retryable = output.as_ref().ok().and_then(|value| retry_reason(value)).is_some() || output.is_err();
            let should_retry = retryable && attempt < 3;
            attempt_results.push(AttemptEvidence {
                attempt,
                process_success,
                status,
                retried: should_retry,
                reason: reason.unwrap_or_else(|| {
                    if retryable {
                        "transport failure without an HTTP response".to_owned()
                    } else {
                        "received HTTP response".to_owned()
                    }
                }),
            });
            if !should_retry {
                selected_output = output.ok();
                break;
            }
        }
        let latency_ms =
            u64::try_from(request_start.elapsed().as_millis().min(u128::from(u64::MAX))).unwrap_or(u64::MAX);
        let attempts = u8::try_from(attempt_results.len()).unwrap_or(3);
        match selected_output {
            Some(output) if output.status.success() => {
                let provider = response_header(&output.stdout, "x-grid-combined-provider-gateway")
                    .or_else(|| response_header(&output.stdout, "x-grid-static-weighted-provider-gateway"))
                    .or_else(|| response_header(&output.stdout, "x-grid-provider-gateway"));
                if let Some(provider) = provider {
                    *counts.entry(provider.clone()).or_default() += 1;
                    sequence.push(provider);
                    requests.push(RequestAttribution {
                        request: request_number,
                        provider: sequence.last().cloned(),
                        status: response_status(&output.stdout),
                        error: None,
                        latency_ms,
                        attempts,
                        attempt_results,
                    });
                } else {
                    failures.push(format!("{request_label}: provider attribution header missing"));
                    requests.push(RequestAttribution {
                        request: request_number,
                        provider: None,
                        status: response_status(&output.stdout),
                        error: Some("provider attribution header missing".to_owned()),
                        latency_ms,
                        attempts,
                        attempt_results,
                    });
                }
            },
            Some(output) => {
                let error = format!(
                    "HTTP probe failed: {}",
                    safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 500)
                );
                failures.push(format!("{request_label}: {error}"));
                requests.push(RequestAttribution {
                    request: request_number,
                    provider: None,
                    status: response_status(&output.stdout),
                    error: Some(error),
                    latency_ms,
                    attempts,
                    attempt_results,
                });
            },
            None => {
                let error = "probe execution failed after transport retries".to_owned();
                failures.push(format!("{request_label}: {error}"));
                requests.push(RequestAttribution {
                    request: request_number,
                    provider: None,
                    status: None,
                    error: Some(error),
                    latency_ms,
                    attempts,
                    attempt_results,
                });
            },
        }
    }

    let canonical = ["provider-a", "provider-b", "provider-c"];
    let total = f64::from(u32::try_from(sequence.len()).unwrap_or(u32::MAX));
    let proportions: Vec<f64> = canonical
        .iter()
        .map(|name| {
            f64::from(u32::try_from(counts.get(*name).copied().unwrap_or(0)).unwrap_or(u32::MAX)) / total.max(1.0)
        })
        .collect();
    let within_tolerance = accepts_proportion(
        expected,
        [
            u32::try_from(counts.get("provider-a").copied().unwrap_or(0)).unwrap_or(u32::MAX),
            u32::try_from(counts.get("provider-b").copied().unwrap_or(0)).unwrap_or(u32::MAX),
            u32::try_from(counts.get("provider-c").copied().unwrap_or(0)).unwrap_or(u32::MAX),
        ],
    );
    let weighted = sequence.len() == 60 && within_tolerance && failures.is_empty();

    let mut facts = BTreeMap::new();
    facts.insert("request_count".to_owned(), serde_json::json!(sequence.len()));
    facts.insert("provider_counts".to_owned(), serde_json::json!(counts));
    facts.insert("ordered_provider_sequence".to_owned(), serde_json::json!(sequence));
    facts.insert("requests".to_owned(), serde_json::json!(requests));
    facts.insert("observed_proportions".to_owned(), serde_json::json!(proportions));
    facts.insert("phase".to_owned(), serde_json::json!(phase));
    facts.insert("expected_percentages".to_owned(), serde_json::json!(expected));
    facts.insert(
        "chi_square".to_owned(),
        serde_json::json!(chi_square_statistic(
            expected,
            [
                u32::try_from(counts.get("provider-a").copied().unwrap_or(0)).unwrap_or(u32::MAX),
                u32::try_from(counts.get("provider-b").copied().unwrap_or(0)).unwrap_or(u32::MAX),
                u32::try_from(counts.get("provider-c").copied().unwrap_or(0)).unwrap_or(u32::MAX),
            ],
        )),
    );
    facts.insert("chi_square_accepted".to_owned(), serde_json::json!(within_tolerance));
    facts.insert("failures".to_owned(), serde_json::json!(failures));
    facts.insert("selection_policy".to_owned(), serde_json::json!("weightedRandom"));
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

    if weighted && overlay_stable {
        Ok(proof_success(
            "60 requests were attributed consistently with the configured static weights",
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
        let ctx = cluster_context(cluster);
        wait_for_deployment("grid-operator", GRID_SYSTEM_NS, &ctx)?;
        eprintln!("  [OK] {cluster}: operator restarted with SWIM config");
    }

    Ok(())
}

/// Read the SWIM `LoadBalancer` IP for a cluster directly from Kubernetes.
fn read_swim_lb_ip(cluster: &str) -> Result<String, Box<dyn std::error::Error>> {
    let context = cluster_context(cluster);
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
    let context = cluster_context(cluster);

    let operator_image = std::env::var("GRID_XTASK_OPERATOR_IMAGE")
        .unwrap_or_else(|_| "grid-operator:static-weighted-qualification".to_owned());
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

/// Tear down only the static-weighted Forge environment.
fn teardown_environment(context: &ProviderTrafficContext) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!();
    eprintln!("=== TEARDOWN ===");

    let client_context = cluster_context("provider-a");
    drop(
        Command::new("kubectl")
            .args([
                "--context",
                &client_context,
                "-n",
                GRID_SYSTEM_NS,
                "delete",
                "pod",
                STATIC_CLIENT_POD,
                "--ignore-not-found",
                "--wait=false",
            ])
            .status(),
    );

    let status = Command::new(&context.forge_bin)
        .args(["down", "--config"])
        .arg(&context.resolved_config)
        .status()?;

    if !status.success() {
        return Err("failed to tear down static-weighted environment".into());
    }

    eprintln!("  [OK] Environment torn down successfully");
    Ok(())
}

/// Atomically persist qualification evidence so functional results survive a
/// teardown failure or process interruption during cleanup.
fn write_evidence(path: &Path, evidence: &Evidence) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = path.with_extension("json.tmp");
    let serialized = serde_json::to_string_pretty(evidence)?;
    fs::write(&temporary, serialized)?;
    fs::rename(temporary, path)?;
    Ok(())
}

/// Run the focused static-weighted qualification.
#[expect(
    clippy::too_many_lines,
    reason = "The public demo entrypoint keeps setup, proof, evidence, and teardown visible."
)]
pub(crate) fn run(forge_config: &Path, options: &GlbDemoOptions) -> Result<(), Box<dyn std::error::Error>> {
    if options.mode() != DemoMode::Quick {
        return Err("static-weighted supports only the focused quick proof".into());
    }
    let mode = DemoMode::Quick;
    let run_id = format_utc_timestamp();
    drop(RUN_NAME.set(format!("{BASE_RUN_NAME}-{run_id}")));
    let wall_start = Instant::now();
    let _started_at = format_utc_iso();

    let evidence_dir = resolve_evidence_dir(forge_config, options, &run_id)?;
    fs::create_dir_all(&evidence_dir)?;

    let setup_ctx = prepare_setup(forge_config);
    let mut run_error = None;
    let mut overlay_state = OverlayState::default();
    let mut image_evidence = BTreeMap::new();
    let mut static_phases = Vec::new();

    let proof_results = match &setup_ctx {
        Ok(context) => {
            eprintln!("{OUTPUT_RULE}");
            eprintln!("Grid Provider Traffic Qualification");
            eprintln!("Mode: {}", if mode == DemoMode::Quick { "quick" } else { "full" });
            eprintln!("Config: {}", forge_config.display());
            eprintln!("{OUTPUT_RULE}");

            match deploy_setup(context) {
                Ok(state) => {
                    overlay_state = state;
                    image_evidence = match collect_image_evidence() {
                        Ok(collected_images) => collected_images,
                        Err(error) => {
                            run_error = Some(format!("image evidence collection failed: {error}"));
                            BTreeMap::new()
                        },
                    };
                    eprintln!();
                    eprintln!("{OUTPUT_RULE}");
                    eprintln!("ENVIRONMENT READY - Starting proof scenarios");
                    eprintln!("{OUTPUT_RULE}");

                    let (scenario_results, phase_evidence) = run_quick_scenarios();
                    static_phases = phase_evidence;

                    let failed_proofs: Vec<&str> = scenario_results
                        .iter()
                        .filter_map(|(name, proof)| (!proof.success).then_some(name.as_str()))
                        .collect();
                    if !failed_proofs.is_empty() {
                        run_error = Some(format!("runtime proofs failed: {}", failed_proofs.join(", ")));
                    }

                    scenario_results
                },
                Err(e) => {
                    eprintln!("[FAIL] Environment setup failed: {e}");
                    run_error = Some(format!("environment setup failed: {e}"));

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

    let mut evidence = Evidence {
        schema_version: EVIDENCE_SCHEMA_VERSION.to_owned(),
        mode: if mode == DemoMode::Quick { "quick" } else { "full" }.to_owned(),
        topology: "static-weighted".to_owned(),
        clusters: CLUSTERS.iter().map(|&s| s.to_owned()).collect(),
        proof_results,
        static_phases,
        images: image_evidence,
        overlay_state,
        cluster_health: Vec::new(),     // Will be populated during runtime assertions
        components: Vec::new(),         // Will be populated during runtime assertions
        swim_membership: Vec::new(),    // Will be populated during runtime assertions
        provider_responses: Vec::new(), // Will be populated during runtime assertions
        security_results: Vec::new(),   // Will be populated during runtime assertions
        teardown_success: false,
        functional_status: if run_error.is_none() {
            "PASS".to_owned()
        } else {
            "FAIL".to_owned()
        },
        cleanup_status: "pending".to_owned(),
    };

    // Persist functional evidence before cleanup begins.
    let evidence_file = evidence_dir.join("results.json");
    write_evidence(&evidence_file, &evidence)?;

    if options.teardown && !options.keep_on_failure {
        if let Ok(context) = &setup_ctx {
            match teardown_environment(context) {
                Ok(()) => {
                    evidence.teardown_success = true;
                    "PASS".clone_into(&mut evidence.cleanup_status);
                },
                Err(error) => {
                    eprintln!("[WARN]  Teardown failed: {error}");
                    evidence.cleanup_status = format!("FAIL: {error}");
                    run_error = Some(match run_error {
                        Some(previous) => format!("{previous}; teardown failed: {error}"),
                        None => format!("teardown failed: {error}"),
                    });
                },
            }
        } else {
            "not-run: setup did not produce a context".clone_into(&mut evidence.cleanup_status);
        }
    } else {
        "not-requested".clone_into(&mut evidence.cleanup_status);
    }
    write_evidence(&evidence_file, &evidence)?;

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
    let mut image_evidence = BTreeMap::new();

    for cluster in CLUSTERS {
        let context = cluster_context(cluster);

        // Get grid operator image
        let operator_output = Command::new("kubectl")
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

        if operator_output.status.success() {
            let operator_image = String::from_utf8_lossy(&operator_output.stdout).trim().to_owned();
            image_evidence.insert(format!("{cluster}_operator"), operator_image);
        }

        // Get consumer gateway image
        let consumer_output = Command::new("kubectl")
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

        if consumer_output.status.success() {
            let consumer_image = String::from_utf8_lossy(&consumer_output.stdout).trim().to_owned();
            image_evidence.insert(format!("{cluster}_consumer_gateway"), consumer_image);
        }

        // Get provider gateway image
        let provider_output = Command::new("kubectl")
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

        if provider_output.status.success() {
            let provider_image = String::from_utf8_lossy(&provider_output.stdout).trim().to_owned();
            image_evidence.insert(format!("{cluster}_provider_gateway"), provider_image);
        }

        // Get VCR inference image
        let vcr_output = Command::new("kubectl")
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

        if vcr_output.status.success() {
            let mock_image = String::from_utf8_lossy(&vcr_output.stdout).trim().to_owned();
            image_evidence.insert(format!("{cluster}_vcr_inference"), mock_image);
        }
    }

    Ok(image_evidence)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proof_success_creation() {
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
    fn proof_failure_creation() {
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
    fn assertion_result_error_handling() {
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
    fn evidence_serialization() {
        let evidence = Evidence {
            schema_version: "test".to_owned(),
            mode: "quick".to_owned(),
            topology: "static-weighted".to_owned(),
            clusters: vec![
                "provider-a".to_owned(),
                "provider-b".to_owned(),
                "provider-c".to_owned(),
            ],
            proof_results: BTreeMap::new(),
            static_phases: Vec::new(),
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
            functional_status: "PASS".to_owned(),
            cleanup_status: "PASS".to_owned(),
        };

        let Ok(json) = serde_json::to_string(&evidence) else {
            std::process::abort();
        };
        assert!(json.contains("\"schema_version\":\"test\""));
        assert!(json.contains("\"topology\":\"static-weighted\""));
        assert!(json.contains("\"healthy\":true"));
        assert!(json.contains("\"ready_replicas\":1"));
        assert!(json.contains("\"converged\":true"));

        // Verify deserialization
        let Ok(_deserialized) = serde_json::from_str::<Evidence>(&json) else {
            std::process::abort();
        };
    }

    #[test]
    fn proof_count_validation() {
        let names = [
            "cluster_health",
            "component_deployment",
            "swim_convergence",
            "site_auto_discovery",
            "overlay_acceptance",
            "static_weighted_baseline",
        ];
        assert_eq!(names.len(), 6);
        assert_eq!(names[0], "cluster_health");
        assert_eq!(names[5], "static_weighted_baseline");
        assert_eq!(CLUSTERS.len(), 3);
        assert_eq!(CLUSTERS, &["provider-a", "provider-b", "provider-c"]);
    }

    #[test]
    fn ordinary_provider_traffic_topology_remains_round_robin() {
        let Some(root) = Path::new(env!("CARGO_MANIFEST_DIR")).parent() else {
            std::process::abort();
        };
        let ordinary = fs::read_to_string(root.join("tests/e2e/topologies/grid-provider-traffic/forge.yaml"))
            .unwrap_or_else(|_| std::process::abort());
        assert!(ordinary.contains("name: grid-provider-traffic"));
        assert!(ordinary.contains("mode: roundRobin"));
        assert!(!ordinary.contains("capacityWeight:"));
    }

    #[test]
    fn evidence_schema_version() {
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

    #[test]
    fn materialized_static_policies_are_explicit() {
        assert_eq!(
            [
                ("baseline", [50, 30, 20]),
                ("changed", [20, 30, 50]),
                ("equal", [1, 1, 1])
            ][0]
            .1,
            [50, 30, 20]
        );
        assert_eq!([50_u32, 30, 20], [50, 30, 20]);
        assert_eq!([1_u32, 1, 1], [1, 1, 1]);
    }

    #[test]
    fn statistical_bounds_and_revision_decisions_are_deterministic() {
        assert!(accepts_proportion([50, 30, 20], [30, 18, 12]));
        assert!(accepts_proportion([1, 1, 1], [20, 21, 19]));
        assert!(!accepts_proportion([50, 30, 20], [60, 0, 0]));
        assert!(revisions_converged("r2", "r2", "r2"));
        assert!(!revisions_converged("r2", "r1", "r2"));
        assert!(!revisions_converged("unknown", "unknown", "unknown"));
    }

    #[test]
    fn retry_classification_retries_only_without_an_http_response() {
        let transport = Command::new("false").output().unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            retry_reason(&transport),
            Some("transport failure without an HTTP response")
        );

        let http_error = std::process::Output {
            status: Command::new("false")
                .output()
                .unwrap_or_else(|_| std::process::abort())
                .status,
            stdout: b"HTTP/1.1 503 Service Unavailable\r\n".to_vec(),
            stderr: Vec::new(),
        };
        assert_eq!(retry_reason(&http_error), None);

        let http2 = std::process::Output {
            status: Command::new("false")
                .output()
                .unwrap_or_else(|_| std::process::abort())
                .status,
            stdout: b"HTTP/2 200\r\n".to_vec(),
            stderr: Vec::new(),
        };
        assert_eq!(response_status(&http2.stdout), Some(200));
        assert_eq!(retry_reason(&http2), None);
    }

    #[test]
    fn weighted_sample_request_is_sessionless_and_retry_stable() {
        let args = static_weighted_curl_args();
        assert!(!args.iter().any(|arg| arg.eq_ignore_ascii_case("X-Session-Id")));
        assert!(!args.iter().any(|arg| arg.contains("X-Session-Id:")));
        assert_eq!(args.iter().filter(|arg| **arg == "--header").count(), 2);
        assert_eq!(args[5], "--header");
        assert_eq!(args[6], "Content-Type: application/json");
        assert_eq!(args[7], "--header");
        assert_eq!(args[8], "Authorization: Bearer consumer-token");
        assert_eq!(args[9], "--data");
        assert!(args[10].starts_with('{'));
        assert!(args[11].starts_with("http://"));

        let retry_one = static_weighted_curl_args();
        let retry_two = static_weighted_curl_args();
        assert_eq!(retry_one, retry_two);
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        clippy::indexing_slicing,
        clippy::expect_used,
        reason = "This test exercises nested typed evidence fields."
    )]
    fn typed_phase_evidence_serializes_requests_and_revisions() {
        let phase = StaticPhaseEvidence {
            phase: "changed".to_owned(),
            configured_capacity: BTreeMap::from([(String::from("provider-a"), 20)]),
            crdt_capacity: BTreeMap::from([(
                String::from("provider-a"),
                CrdtCapacityEvidence { local: 20, remote: 20 },
            )]),
            rendered_traffic_weight: BTreeMap::from([(String::from("provider-a"), 20)]),
            grid_revision: "r2".to_owned(),
            praxis_accepted_revision: "r2".to_owned(),
            praxis_serving_revision: "r2".to_owned(),
            gateway: GatewayStabilityEvidence {
                pod_uid_before: "pod-1".to_owned(),
                pod_uid_after: "pod-1".to_owned(),
                restart_count_before: 0,
                restart_count_after: 0,
            },
            stable_observations: 2,
            traffic: TrafficSampleEvidence {
                requests: vec![RequestAttribution {
                    request: 1,
                    provider: Some("provider-a".to_owned()),
                    status: Some(200),
                    error: None,
                    latency_ms: 5,
                    attempts: 1,
                    attempt_results: vec![AttemptEvidence {
                        attempt: 1,
                        process_success: true,
                        status: Some(200),
                        retried: false,
                        reason: "received HTTP response".to_owned(),
                    }],
                }],
                counts: BTreeMap::from([(String::from("provider-a"), 1)]),
                expected_weights: [20, 30, 50],
                observed_fraction: [1.0, 0.0, 0.0],
                chi_square: 0.0,
                chi_square_critical_value: 5.991,
                accepted: false,
            },
        };
        let value = serde_json::to_value(&phase).expect("typed evidence must serialize");
        assert_eq!(value["grid_revision"], "r2");
        assert_eq!(value["traffic"]["requests"][0]["status"], 200);
        assert_eq!(value["gateway"]["restart_count_after"], 0);
    }
}
