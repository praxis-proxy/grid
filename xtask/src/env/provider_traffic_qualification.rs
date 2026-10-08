//! Narrated, evidence-backed provider-traffic qualification scenarios.
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write as _,
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use serde::Serialize;
use sha2::{Digest as _, Sha256};

use super::{DemoMode, GlbDemoOptions, certs, glb, kubectl, operator, safe_truncate_str};

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

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

/// `GridNetwork` name in the focused provider-traffic topology.
const GRID_NETWORK_NAME: &str = "grid-provider-traffic";

/// Additional startup-configured grid-gateway consumer used to qualify serving-config reload.
const GRID_SERVING_GATEWAY: &str = "grid-serving-gateway";

/// Operator `ConfigMap` consumed by the embedded `grid-gateway` filter.
const GRID_SERVING_CONFIGMAP: &str = "grid-serving-grid-provider-traffic-grid-serving-gateway";

/// Overlay `ConfigMap` name created by the Grid operator for consumer gateways.
const OVERLAY_CONFIGMAP: &str = "grid-overlay-grid-provider-traffic-consumer-gateway";

/// Physical Kind cluster prefix assigned to this qualification run.
static RUN_CLUSTER_PREFIX: OnceLock<String> = OnceLock::new();

/// Provider credential secret name (matches Helm `credentials[0].name`).
const VCR_INFERENCE_CREDENTIAL: &str = "vcr-inference-credential";

/// Stable terminal separator that also remains readable in captured logs.
const OUTPUT_RULE: &str = "===============================================================================";

/// Marker appended by curl after the response body so probes do not parse HTTP header formatting.
const CURL_HTTP_STATUS_MARKER: &str = "GRID258_HTTP_STATUS:";

/// Maximum wait for Grid to publish a credential-bearing test overlay.
const PROJECTED_CREDENTIAL_OVERLAY_TIMEOUT: Duration = Duration::from_secs(180);

/// Provider gateway service name advertised via SWIM for cross-site discovery.
const PROVIDER_GATEWAY_SERVICE: &str = "provider-gateway";

/// Provider gateway port advertised via SWIM for cross-site discovery.
const PROVIDER_GATEWAY_PORT: &str = "8443";

/// Label used by the run-owned `InferenceProvider` selectors.
const PROVIDER_SITE_LABEL: &str = "grid.praxis-proxy.io/provider-site";

/// Provider `InferenceProvider` resources created by this topology, in origin-cluster order.
const PROVIDER_RESOURCES: &[(&str, &str)] = &[
    ("provider-a", "vcr-provider-a-provider"),
    ("provider-b", "vcr-provider-b-provider"),
    ("provider-c", "vcr-provider-c-provider"),
];

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
    /// Run-owned Forge state directory.
    forge_state_dir: PathBuf,
    /// Run-owned TLS material directory.
    certs_dir: PathBuf,
    /// Run-owned evidence directory.
    evidence_dir: PathBuf,
    /// Path to the forge binary.
    forge_bin: PathBuf,
}

/// Own a diagnostic-only backend metrics port-forward and stop it on every exit path.
struct BackendMetricsPortForward {
    /// Child process serving the local loopback tunnel.
    child: Option<Child>,
}

impl Drop for BackendMetricsPortForward {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _kill = child.kill();
            let _wait = child.wait();
        }
    }
}

/// Build the context name for one cluster in this qualification run.
fn cluster_context(cluster: &str) -> String {
    let prefix = RUN_CLUSTER_PREFIX.get().map_or("grid-provider-traffic", String::as_str);
    format!("kind-{prefix}-{cluster}")
}

/// Resolve Grid chart paths from the xtask crate's workspace root, not from a
/// topology directory or the caller's current working directory.
fn grid_repository_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "xtask manifest directory has no Grid repository parent".into())
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
    /// `grid.praxis.fast/overlay-revision` annotation. Changes only when
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
    /// Whether this overlay candidate carries a provider Secret reference.
    #[serde(default)]
    has_credential_reference: bool,
}

/// Sanitized result of one request used by the no-route lifecycle proof.
#[derive(Clone, Debug, Serialize)]
struct LifecycleRequest {
    /// Observed HTTP status.
    status: u16,
    /// Provider gateway attribution, absent when no provider served the request.
    provider: Option<String>,
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

/// `kubectl run`'s attached stream can lose output from a short-lived curl
/// container. Read logs only after Kubernetes reports a terminated container.
struct ProbeExitStatus {
    /// Terminated curl process exit code.
    code: i32,
}

impl ProbeExitStatus {
    /// Whether curl exited successfully.
    fn success(&self) -> bool {
        self.code == 0
    }
}

/// Completed curl probe output collected from its temporary Pod.
struct CurlProbeOutput {
    /// Curl process status.
    status: ProbeExitStatus,
    /// Captured standard output.
    stdout: Vec<u8>,
    /// Captured standard error.
    stderr: Vec<u8>,
}

/// Delete exactly the temporary probe Pod created by `run_curl_probe`.
struct ProbePodCleanup<'probe> {
    /// Kubernetes context containing the temporary Pod.
    context: &'probe str,
    /// Name of the temporary Pod.
    name: &'probe str,
}

impl Drop for ProbePodCleanup<'_> {
    fn drop(&mut self) {
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    self.context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "delete",
                    "pod",
                    self.name,
                    "--ignore-not-found=true",
                    "--wait=false",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status(),
        );
    }
}

/// Return the terminal curl-container exit code, if Kubernetes has observed it.
fn probe_pod_exit_code(pod: &serde_json::Value) -> Option<i32> {
    pod.pointer("/status/containerStatuses")
        .and_then(serde_json::Value::as_array)
        .and_then(|statuses| statuses.first())
        .and_then(|status| status.pointer("/state/terminated/exitCode"))
        .and_then(serde_json::Value::as_i64)
        .and_then(|code| i32::try_from(code).ok())
        .or_else(
            || match pod.pointer("/status/phase").and_then(serde_json::Value::as_str) {
                Some("Succeeded") => Some(0),
                Some("Failed") => Some(1),
                _ => None,
            },
        )
}

/// Run a restricted curl Pod, collect its completed logs, and then remove it.
#[expect(clippy::too_many_lines, reason = "probe lifecycle and cleanup are one operation")]
fn run_curl_probe(
    context: &str,
    pod_name: &str,
    curl_args: &[&str],
) -> Result<CurlProbeOutput, Box<dyn std::error::Error>> {
    let sequence = PROBE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let prefix = pod_name.get(..pod_name.len().min(40)).unwrap_or(pod_name);
    let unique_pod_name = format!("{prefix}-{sequence}");
    let overrides = curl_pod_overrides(&unique_pod_name, curl_args);
    let created = Command::new("kubectl")
        .args([
            "run",
            &unique_pod_name,
            "--image=curlimages/curl:8.12.1",
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "--restart=Never",
            "--overrides",
            &overrides,
        ])
        .output()?;
    if !created.status.success() {
        return Err(format!(
            "could not create curl probe Pod {unique_pod_name}: {}",
            safe_truncate_str(String::from_utf8_lossy(&created.stderr).trim(), 500)
        )
        .into());
    }
    let _cleanup = ProbePodCleanup {
        context,
        name: &unique_pod_name,
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let observed = Command::new("kubectl")
            .args([
                "--context",
                context,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                "pod",
                &unique_pod_name,
                "-o",
                "json",
            ])
            .output()?;
        if observed.status.success() {
            let pod: serde_json::Value = serde_json::from_slice(&observed.stdout)?;
            if let Some(code) = probe_pod_exit_code(&pod) {
                let logs = Command::new("kubectl")
                    .args(["--context", context, "-n", GRID_SYSTEM_NS, "logs", &unique_pod_name])
                    .output()?;
                if !logs.status.success() {
                    return Err(format!(
                        "could not collect completed curl probe Pod logs: {}",
                        safe_truncate_str(String::from_utf8_lossy(&logs.stderr).trim(), 500)
                    )
                    .into());
                }
                // Container logs combine stdout/stderr. Retain response bytes only
                // in memory; callers persist status and allowlisted headers only.
                let stderr = if code == 0 {
                    Vec::new()
                } else {
                    let diagnostics = String::from_utf8_lossy(&logs.stdout)
                        .lines()
                        .filter(|line| line.starts_with("curl: ("))
                        .collect::<Vec<_>>()
                        .join("\n");
                    if diagnostics.is_empty() {
                        format!("curl container exited with status {code}")
                    } else {
                        diagnostics
                    }
                    .into_bytes()
                };
                return Ok(CurlProbeOutput {
                    status: ProbeExitStatus { code },
                    stdout: logs.stdout,
                    stderr,
                });
            }
            if pod.pointer("/status/phase").and_then(serde_json::Value::as_str) == Some("Failed") {
                let reason = pod
                    .pointer("/status/reason")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("PodFailed");
                let message = pod
                    .pointer("/status/message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("curl probe Pod failed before the container reported an exit code");
                return Err(format!("curl probe Pod failed: {reason}: {}", safe_truncate_str(message, 500)).into());
            }
        } else if !String::from_utf8_lossy(&observed.stderr).contains("NotFound") {
            return Err(format!(
                "could not observe curl probe Pod {unique_pod_name}: {}",
                safe_truncate_str(String::from_utf8_lossy(&observed.stderr).trim(), 500)
            )
            .into());
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for curl probe Pod {unique_pod_name} to finish").into());
        }
        std::thread::park_timeout(Duration::from_millis(200));
    }
}

/// Run an ephemeral curl pod with additional kubectl flags (e.g. `--labels`).
fn response_header(output: &[u8], name: &str) -> Option<String> {
    let expected = name.to_ascii_lowercase();
    String::from_utf8_lossy(output).lines().find_map(|line| {
        let (header, value) = line.split_once(':')?;
        header.eq_ignore_ascii_case(&expected).then(|| value.trim().to_owned())
    })
}

/// Extract the final HTTP status line without retaining a response body.
fn response_status_line(output: &[u8]) -> Option<String> {
    String::from_utf8_lossy(output)
        .lines()
        .rev()
        .find(|line| line.starts_with("HTTP/"))
        .map(|line| safe_truncate_str(line.trim(), 120))
}

/// Return response header names only, for sanitized probe diagnostics.
fn response_header_names(output: &[u8]) -> Vec<String> {
    let mut names = BTreeSet::new();
    let mut in_headers = false;
    for line in String::from_utf8_lossy(output).lines() {
        if line.starts_with("HTTP/") {
            in_headers = true;
            continue;
        }
        if line.is_empty() {
            in_headers = false;
            continue;
        }
        if in_headers && let Some((name, _)) = line.split_once(':') {
            names.insert(name.trim().to_ascii_lowercase());
        }
    }
    names.into_iter().collect()
}

/// Parse curl's explicit HTTP status marker, rejecting transport failures (`000`).
fn curl_http_status(output: &[u8]) -> Result<u16, String> {
    let response = String::from_utf8_lossy(output);
    let status = response
        .lines()
        .find_map(|line| line.strip_prefix(CURL_HTTP_STATUS_MARKER))
        .ok_or_else(|| "curl output did not contain its HTTP status marker".to_owned())?
        .parse::<u16>()
        .map_err(|error| format!("curl emitted an invalid HTTP status: {error}"))?;
    if status == 0 {
        return Err("curl received no HTTP response (status 000)".to_owned());
    }
    Ok(status)
}

/// Remove ANSI CSI styling so tracing fields remain parseable in terminal output.
fn strip_ansi_csi(input: &str) -> String {
    let mut output = Vec::with_capacity(input.len());
    let mut bytes = input.bytes().peekable();
    while let Some(byte) = bytes.next() {
        if byte == 0x1B && bytes.peek() == Some(&b'[') {
            bytes.next();
            for control in bytes.by_ref() {
                if (0x40..=0x7E).contains(&control) {
                    break;
                }
            }
        } else {
            output.push(byte);
        }
    }
    String::from_utf8_lossy(&output).into_owned()
}

/// Return every exact serving-revision log observation after normalizing ANSI styling.
fn embedded_serving_revision_lines(logs: &str, digest: &str, count: u64) -> Vec<String> {
    let clean = strip_ansi_csi(logs);
    clean
        .lines()
        .filter(|line| {
            line.contains("grid_serving_revision_serving")
                && line.contains(digest)
                && line.contains(&format!("candidate_count={count}"))
        })
        .map(str::to_owned)
        .collect()
}

/// Require an exact serving event beyond those already observed for this digest.
fn has_new_embedded_serving_revision_observation(
    logs: &str,
    digest: &str,
    count: u64,
    prior_matching_observations: usize,
) -> bool {
    embedded_serving_revision_lines(logs, digest, count).len() > prior_matching_observations
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
        let context = cluster_context(cluster);

        let result = poll_until(
            || {
                let output = Command::new("kubectl")
                    .args([
                        "get", "gridsite",
                        "-l", "grid.praxis.fast/auto-discovered=true",
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

        // Step 4: A request proves routing works; the exact revision gate below
        // distinguishes the current overlay from an older last-known-good one.
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

        let current_overlay = read_cluster_overlay(cluster)?;
        let revision_accepted = current_overlay.semantic_revision != "unknown"
            && wait_for_consumer_gateway_revision(&current_overlay.semantic_revision).is_ok();
        let overlay_accepted = overlay_exists && has_data && gateway_ready && routing_ok && revision_accepted;
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
            format!("{cluster}_accepted_serving_revision_matches"),
            serde_json::Value::Bool(revision_accepted),
        );
        observed_facts.insert(
            format!("{cluster}_overlay_revision"),
            serde_json::Value::String(current_overlay.semantic_revision),
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
            "Consumer entrypoint accepted and serves the current overlay revision, with a ready gateway and successful routing",
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
        .unwrap_or_else(|_| "praxis-ai:provider-traffic-qualification".to_owned());
    let operator = std::env::var("GRID_XTASK_OPERATOR_IMAGE")
        .unwrap_or_else(|_| "grid-operator:provider-traffic-qualification".to_owned());
    let vcr = crate::env::image_overrides::sim_image();
    let overlay_sync = crate::env::image_overrides::overlay_sync_image();

    for image in [&gateway, &operator, &vcr, &overlay_sync] {
        require_local_image(image)?;
        eprintln!("  verified local image: {image}");
    }

    for cluster in CLUSTERS {
        for image in [&gateway, &operator, &vcr, &overlay_sync] {
            eprintln!("  loading {image} into {cluster}...");
            let kind_name = format!(
                "{}-{cluster}",
                RUN_CLUSTER_PREFIX
                    .get()
                    .ok_or("run-owned cluster prefix was not initialized")?
            );
            crate::env::image_overrides::load_docker_image_into_kind(image, &kind_name)?;
        }
        eprintln!("  [OK] {cluster}: all images loaded");
    }
    Ok(())
}

/// Generate TLS certificates for all provider-traffic identities.
///
/// Must be called BEFORE `forge up` so the certificates exist on the host
/// when `install_provider_boundary` creates the Kubernetes Secrets.
fn stage_provider_boundary(certs_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let identities: Vec<String> = CLUSTERS.iter().map(|c| (*c).to_owned()).collect();
    certs::generate_all_in_dir(&identities, certs_dir)?;

    let wrong_ca = ::certs::generate_ca("Combined Site untrusted test CA")?;
    fs::write(certs_dir.join("untrusted-ca.pem"), wrong_ca.cert_pem)?;

    eprintln!("  [OK] TLS certificates generated for provider-a, provider-b, provider-c");
    Ok(())
}

/// Create TLS and credential Secrets in every provider-traffic cluster.
///
/// Must be called AFTER `forge up` since the clusters must exist.  Gateway
/// deployments are restarted so pods pick up the new volume mounts.
fn install_provider_boundary(certs_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
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
/// semantic revision from the `grid.praxis.fast/overlay-revision`
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
        .pointer("/metadata/annotations/grid.praxis.fast~1overlay-revision")
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
        let has_credential_reference = c.get("credential").is_some_and(serde_json::Value::is_object);

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
            has_credential_reference,
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

    let mut stable_revision: Option<String> = None;
    let mut stable_signature: Option<String> = None;
    for observation in 1..=3 {
        let overlay = read_cluster_overlay("provider-a")?;
        let configmap = Command::new("kubectl")
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
    let context = cluster_context("provider-a");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
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

/// Materialize a run-owned Forge config beside its source so topology-relative
/// resource paths continue to resolve from the documented directory.
fn materialize_config(
    source: &Path,
    run_id: &str,
    cluster_prefix: &str,
    state_dir: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(source)?;
    let mut config: serde_yaml::Value = serde_yaml::from_str(&content)?;
    apply_image_overrides(&mut config)?;
    scope_environment_name(&mut config, run_id)?;
    let runtime = config
        .get_mut("spec")
        .and_then(|spec| spec.get_mut("runtime"))
        .and_then(serde_yaml::Value::as_mapping_mut)
        .ok_or("Forge config is missing spec.runtime")?;
    runtime.insert(
        serde_yaml::Value::String("clusterPrefix".to_owned()),
        serde_yaml::Value::String(cluster_prefix.to_owned()),
    );
    rewrite_run_scoped_paths(&mut config, cluster_prefix, state_dir);
    let rendered = serde_yaml::to_string(&config)?;
    let parent = source.parent().ok_or("source config must have parent directory")?;
    let output = parent.join(format!(".forge.resolved-{run_id}.yaml"));
    fs::write(&output, rendered)?;
    Ok(output)
}

/// Give Forge's environment and derived Docker network a unique identity.
fn scope_environment_name(config: &mut serde_yaml::Value, run_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    let metadata = config
        .get_mut("metadata")
        .and_then(serde_yaml::Value::as_mapping_mut)
        .ok_or("Forge config is missing metadata")?;
    let name_key = serde_yaml::Value::String("name".to_owned());
    let name = metadata
        .get(&name_key)
        .and_then(serde_yaml::Value::as_str)
        .ok_or("Forge config metadata is missing name")?;
    let scoped_name = format!("{name}-{run_id}");
    if scoped_name.len() > 63 {
        return Err(format!("run-scoped Forge environment name is too long: {scoped_name}").into());
    }
    metadata.insert(name_key, serde_yaml::Value::String(scoped_name));
    Ok(())
}

/// Rewrite fixed Kind contexts and `exec` paths to use this run's Forge state.
/// `template-file` targets intentionally keep their `.forge/runtime/` prefix:
/// Forge resolves those relative to `--state-dir` itself.
fn rewrite_run_scoped_paths(value: &mut serde_yaml::Value, cluster_prefix: &str, state_dir: &Path) {
    rewrite_cluster_contexts(value, cluster_prefix);
    rewrite_exec_state_references(value, state_dir);
}

/// Scope hard-coded Kind context names without changing logical cluster names.
fn rewrite_cluster_contexts(value: &mut serde_yaml::Value, cluster_prefix: &str) {
    match value {
        serde_yaml::Value::String(string) => {
            *string = string.replace("kind-grid-provider-traffic-", &format!("kind-{cluster_prefix}-"));
        },
        serde_yaml::Value::Sequence(sequence) => {
            for child in sequence {
                rewrite_cluster_contexts(child, cluster_prefix);
            }
        },
        serde_yaml::Value::Mapping(mapping) => {
            for child in mapping.values_mut() {
                rewrite_cluster_contexts(child, cluster_prefix);
            }
        },
        serde_yaml::Value::Null
        | serde_yaml::Value::Bool(_)
        | serde_yaml::Value::Number(_)
        | serde_yaml::Value::Tagged(_) => {},
    }
}

/// Rewrite `.forge/runtime/` only inside shell exec commands. Forge's own
/// `template-file` step maps that prefix into `<state_dir>/runtime`; shell
/// commands run from the repository root and must use the corresponding path.
fn rewrite_exec_state_references(value: &mut serde_yaml::Value, state_dir: &Path) {
    match value {
        serde_yaml::Value::Mapping(mapping) => {
            let is_exec = mapping
                .get(serde_yaml::Value::String("type".to_owned()))
                .and_then(serde_yaml::Value::as_str)
                == Some("exec");
            if is_exec {
                if let Some(command) = mapping.get_mut(serde_yaml::Value::String("command".to_owned())) {
                    rewrite_state_path_strings(command, state_dir);
                }
            } else {
                for child in mapping.values_mut() {
                    rewrite_exec_state_references(child, state_dir);
                }
            }
        },
        serde_yaml::Value::Sequence(sequence) => {
            for child in sequence {
                rewrite_exec_state_references(child, state_dir);
            }
        },
        serde_yaml::Value::Null
        | serde_yaml::Value::Bool(_)
        | serde_yaml::Value::Number(_)
        | serde_yaml::Value::String(_)
        | serde_yaml::Value::Tagged(_) => {},
    }
}

/// Rewrite generated Forge runtime paths to the current run's isolated state directory.
fn rewrite_state_path_strings(value: &mut serde_yaml::Value, state_dir: &Path) {
    match value {
        serde_yaml::Value::String(string) => {
            *string = string.replace(".forge/runtime/", &format!("{}/runtime/", state_dir.display()));
        },
        serde_yaml::Value::Sequence(sequence) => {
            for child in sequence {
                rewrite_state_path_strings(child, state_dir);
            }
        },
        serde_yaml::Value::Mapping(mapping) => {
            for child in mapping.values_mut() {
                rewrite_state_path_strings(child, state_dir);
            }
        },
        serde_yaml::Value::Null
        | serde_yaml::Value::Bool(_)
        | serde_yaml::Value::Number(_)
        | serde_yaml::Value::Tagged(_) => {},
    }
}

/// Apply the shared Forge image overrides, including the overlay-sync image.
fn apply_image_overrides(config: &mut serde_yaml::Value) -> Result<(), Box<dyn std::error::Error>> {
    let images = crate::env::forge_config::ImageOverrides {
        gateway: std::env::var("GRID_XTASK_GATEWAY_IMAGE")
            .unwrap_or_else(|_| "praxis-ai:provider-traffic-qualification".to_owned()),
        operator: std::env::var("GRID_XTASK_OPERATOR_IMAGE")
            .unwrap_or_else(|_| "grid-operator:provider-traffic-qualification".to_owned()),
        overlay_sync: crate::env::image_overrides::overlay_sync_image(),
        vcr: crate::env::image_overrides::sim_image(),
        pull_policy: std::env::var("GRID_XTASK_IMAGE_PULL_POLICY").unwrap_or_else(|_| "Never".to_owned()),
    };
    crate::env::forge_config::apply_image_values(config, &images)
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
fn prepare_setup(
    forge_config: &Path,
    evidence_dir: &Path,
    run_id: &str,
    cluster_prefix: &str,
) -> Result<ProviderTrafficContext, Box<dyn std::error::Error>> {
    let root = super::demo_root(forge_config);
    eprintln!("Forge config: {}", forge_config.display());
    eprintln!("Demo root:    {}", root.display());
    let evidence_dir = fs::canonicalize(evidence_dir)?;
    let forge_state_dir = evidence_dir.join("forge-state");
    let certs_dir = evidence_dir.join("certs");
    fs::create_dir_all(&forge_state_dir)?;
    fs::create_dir_all(&certs_dir)?;
    let resolved_config = materialize_config(forge_config, run_id, cluster_prefix, &forge_state_dir)?;
    fs::copy(&resolved_config, evidence_dir.join("resolved-forge.yaml"))?;
    let forge_bin = glb::resolve_forge_binary()
        .ok_or("praxis-forge binary not found")?
        .into();

    Ok(ProviderTrafficContext {
        demo_root: root,
        resolved_config,
        forge_state_dir,
        certs_dir,
        evidence_dir,
        forge_bin,
    })
}

/// Construct a Forge command with only this run's config and state.
fn forge_command(context: &ProviderTrafficContext) -> Command {
    let config = context.resolved_config.display().to_string();
    let state_dir = context.forge_state_dir.display().to_string();
    let mut command = Command::new(&context.forge_bin);
    command
        .args(["--config", &config, "--non-interactive", "--state-dir", &state_dir])
        .env("FORGE_STATE_DIR", &context.forge_state_dir);
    command
}

/// Authorize auto-discovered remote `GridSites` with identity trust material.
///
/// For each local cluster, waits for the two remote auto-discovered `GridSites`,
/// verifies the SWIM-advertised certificate matches the staged identity, then
/// patches `spec.egress.tls.serverName` and `spec.trust.canonicalFingerprints`.
/// The controller transitions the site to Active naturally after the patch.
fn authorize_discovered_sites(certs_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    const TRUST_TIMEOUT: Duration = Duration::from_secs(120);
    const GRID_NETWORK: &str = "grid-provider-traffic";

    for local in CLUSTERS {
        let context = cluster_context(local);
        eprintln!();
        eprintln!("  {local}: authorizing remote provider sites");
        for remote in CLUSTERS {
            if *remote == *local {
                continue;
            }
            let site_name = format!("{GRID_NETWORK}-{remote}");
            operator::wait_for_auto_gridsite(&context, &site_name, GRID_NETWORK, TRUST_TIMEOUT)?;
            let canonical_fp = certs::certificate_sha256(&certs_dir.join(format!("{remote}-cert.pem")))?;
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
    let output = forge_command(context).args(["config", "validate"]).output()?;

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

    stage_provider_boundary(&context.certs_dir)?;

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Creating three provider Kind clusters: provider-a, provider-b, provider-c",
        next(),
        total_phases
    );

    let config_status = forge_command(context).arg("up").status()?;

    if !config_status.success() {
        return Err("Failed to create provider-traffic clusters".into());
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

    let apply_stack = |cluster: &str, stack: &str| -> Result<(), Box<dyn std::error::Error>> {
        eprintln!("  applying {stack} to {cluster}...");
        let stack_status = forge_command(context)
            .args(["stack", "apply", cluster, stack])
            .status()?;
        if !stack_status.success() {
            capture_stack_failure(cluster, stack);
            return Err(format!("Failed to apply {stack} to {cluster}").into());
        }
        Ok(())
    };

    for cluster in CLUSTERS {
        apply_stack(cluster, "metallb")?;
    }
    for cluster in CLUSTERS {
        let op_stack = format!("{cluster}-operator-base");
        apply_stack(cluster, &op_stack)?;
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
        apply_stack(cluster, "vcr-backend")?;
    }
    eprintln!("  [OK] VCR backends deployed");

    eprintln!();
    eprintln!("[SETUP {}/{}] Deploying grid-site resources", next(), total_phases);

    for cluster in CLUSTERS {
        let site_stack = format!("{cluster}-site");
        apply_stack(cluster, &site_stack)?;
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

    install_provider_boundary(&context.certs_dir)?;

    materialize_provider_config(&pre_swim_overlays, &context.demo_root)?;
    eprintln!("  [OK] Provider config materialized, trust installed");

    eprintln!();
    eprintln!("[SETUP {}/{}] Deploying provider gateways", next(), total_phases);

    for cluster in CLUSTERS {
        apply_stack(cluster, "provider-gateway")?;
    }
    eprintln!("  [OK] Provider gateways deployed");

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Deploying the single consumer gateway",
        next(),
        total_phases
    );

    apply_stack(CONSUMER_SITE, "consumer-gateway")?;
    eprintln!("  [OK] Consumer gateway deployed in {CONSUMER_SITE}");

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] SWIM discovery and trust authorization",
        next(),
        total_phases
    );

    configure_swim_peers(&context.forge_bin, &context.resolved_config)?;
    authorize_discovered_sites(&context.certs_dir)?;

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
        vec!["logs", "deployment/provider-gateway", "--all-containers", "--tail=100"],
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

/// Run the provider-traffic proof scenarios using the assertion framework.
///
/// Return the scenario contract for one qualification mode.
fn provider_traffic_scenario_names(mode: DemoMode) -> Vec<&'static str> {
    let mut names = vec![
        "cluster_health",
        "component_deployment",
        "swim_convergence",
        "site_auto_discovery",
        "overlay_acceptance",
        "provider_gateway_round_robin",
    ];
    if mode == DemoMode::Full {
        names.push("generated_consumer_config_convergence");
        names.push("embedded_serving_gateway_convergence");
        names.push("provider_withdrawal_lifecycle");
    }
    names
}

/// Read the provider gateway's run-owned `LoadBalancer` IP for the generated
/// consumer endpoint inventory.
fn read_provider_gateway_address(site: &str) -> Result<String, Box<dyn std::error::Error>> {
    let context = cluster_context(site);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "service",
            "provider-gateway",
            "-o",
            "json",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!("{site}: could not read provider-gateway Service address").into());
    }
    let service: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let address = service
        .pointer("/status/loadBalancer/ingress/0/ip")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            service
                .pointer("/status/loadBalancer/ingress/0/hostname")
                .and_then(serde_json::Value::as_str)
        })
        .filter(|address| !address.trim().is_empty())
        .ok_or_else(|| format!("{site}: provider-gateway Service has no LoadBalancer address"))?;
    Ok(format!("{address}:{PROVIDER_GATEWAY_PORT}"))
}

/// Build the endpoint list for all providers while retaining the topology's
/// verified TLS identities.
fn generated_consumer_endpoints() -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    PROVIDER_RESOURCES
        .iter()
        .map(|(site, resource)| {
            Ok(serde_json::json!({
                "cluster": resource,
                "address": read_provider_gateway_address(site)?,
                "transport": {
                    "mode": "mutual_tls",
                    "sni": format!("{site}.grid.internal")
                }
            }))
        })
        .collect()
}

/// Opt the run-owned provider-a `GatewayRef` into the operator-generated config.
#[expect(
    clippy::too_many_lines,
    reason = "this applies the opt-in only after reading and validating the run-owned gateway reference"
)]
fn enable_generated_consumer_config() -> Result<(), Box<dyn std::error::Error>> {
    let context = cluster_context("provider-a");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "gridnetwork",
            GRID_NETWORK_NAME,
            "-o",
            "json",
        ])
        .output()?;
    if !output.status.success() {
        return Err("could not read provider-a GridNetwork before enabling consumerConfig".into());
    }
    let network: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let gateways = network
        .pointer("/spec/gatewayRefs")
        .and_then(serde_json::Value::as_array)
        .ok_or("provider-a GridNetwork has no gatewayRefs array")?;
    let index = gateways
        .iter()
        .position(|gateway| {
            gateway.get("name").and_then(serde_json::Value::as_str) == Some("consumer-gateway")
                && gateway.get("namespace").and_then(serde_json::Value::as_str) == Some(GRID_SYSTEM_NS)
        })
        .ok_or("provider-a GridNetwork does not declare its run-owned consumer-gateway")?;
    let gateway = gateways.get(index).ok_or("consumer-gateway index disappeared")?;
    if gateway.get("consumerConfig").is_some() {
        return Err("consumer-gateway already has consumerConfig; refusing to replace unexpected fixture state".into());
    }

    let consumer_config = serde_json::json!({
        "enabled": true,
        "enableProjectedCredentials": true,
        "supportsProjectedCredentials": false,
        "configMapName": "praxis-consumer-config",
        "clusterEndpoints": generated_consumer_endpoints()?,
        "tlsCertMountPath": "/etc/praxis/tls"
    });
    let patch = serde_json::json!([{
        "op": "add",
        "path": format!("/spec/gatewayRefs/{index}/consumerConfig"),
        "value": consumer_config
    }]);
    let patched = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "patch",
            "gridnetwork",
            GRID_NETWORK_NAME,
            "--type=json",
            "--patch",
            &patch.to_string(),
        ])
        .output()?;
    if !patched.status.success() {
        return Err(format!(
            "could not enable run-owned consumerConfig: {}",
            safe_truncate_str(String::from_utf8_lossy(&patched.stderr).trim(), 800)
        )
        .into());
    }
    Ok(())
}

/// Change only the run-owned Provider A consumer endpoint transport and return its previous mode.
#[expect(clippy::too_many_lines, reason = "reads and patches the run-owned consumer endpoint")]
fn set_generated_consumer_provider_a_transport_mode(mode: &str) -> Result<String, Box<dyn std::error::Error>> {
    let context = cluster_context("provider-a");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "gridnetwork",
            GRID_NETWORK_NAME,
            "-o",
            "json",
        ])
        .output()?;
    if !output.status.success() {
        return Err("could not read provider-a GridNetwork before changing consumerConfig".into());
    }
    let network: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let gateways = network
        .pointer("/spec/gatewayRefs")
        .and_then(serde_json::Value::as_array)
        .ok_or("provider-a GridNetwork has no gatewayRefs array")?;
    let gateway_index = gateways
        .iter()
        .position(|gateway| {
            gateway.get("name").and_then(serde_json::Value::as_str) == Some("consumer-gateway")
                && gateway.get("namespace").and_then(serde_json::Value::as_str) == Some(GRID_SYSTEM_NS)
        })
        .ok_or("provider-a GridNetwork does not declare its run-owned consumer-gateway")?;
    let endpoints = gateways
        .get(gateway_index)
        .and_then(|gateway| gateway.pointer("/consumerConfig/clusterEndpoints"))
        .and_then(serde_json::Value::as_array)
        .ok_or("run-owned consumerConfig has no clusterEndpoints array")?;
    let endpoint_index = endpoints
        .iter()
        .position(|endpoint| {
            endpoint.get("cluster").and_then(serde_json::Value::as_str) == Some("vcr-provider-a-provider")
        })
        .ok_or("run-owned consumerConfig has no Provider A endpoint")?;
    let previous_mode = endpoints
        .get(endpoint_index)
        .and_then(|endpoint| endpoint.pointer("/transport/mode"))
        .and_then(serde_json::Value::as_str)
        .ok_or("Provider A consumer endpoint has no transport mode")?
        .to_owned();
    let json_pointer =
        format!("/spec/gatewayRefs/{gateway_index}/consumerConfig/clusterEndpoints/{endpoint_index}/transport/mode");
    let patch = serde_json::json!([{"op":"replace","path":json_pointer,"value":mode}]);
    let patched = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "patch",
            "gridnetwork",
            GRID_NETWORK_NAME,
            "--type=json",
            "--patch",
            &patch.to_string(),
        ])
        .output()?;
    if !patched.status.success() {
        return Err(format!(
            "could not set run-owned Provider A consumer endpoint transport: {}",
            safe_truncate_str(String::from_utf8_lossy(&patched.stderr).trim(), 800)
        )
        .into());
    }
    Ok(previous_mode)
}

/// Wait for a generation-current Rendered status and validate its live config.
#[expect(
    clippy::too_many_lines,
    reason = "polling and validating the generated config is one bounded readiness gate"
)]
fn wait_for_generated_consumer_config() -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    const TIMEOUT: Duration = Duration::from_secs(180);
    let context = cluster_context("provider-a");
    let deadline = Instant::now() + TIMEOUT;
    let mut last_state = String::from("not observed");
    loop {
        let network_output = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                "gridnetwork",
                GRID_NETWORK_NAME,
                "-o",
                "json",
            ])
            .output()?;
        if network_output.status.success() {
            let network: serde_json::Value = serde_json::from_slice(&network_output.stdout)?;
            let generation = network
                .pointer("/metadata/generation")
                .and_then(serde_json::Value::as_i64);
            let status = network
                .pointer("/status/consumerConfigStatus")
                .and_then(serde_json::Value::as_array);
            let current = status.and_then(|items| {
                items.iter().find(|item| {
                    item.get("gatewayName").and_then(serde_json::Value::as_str) == Some("consumer-gateway")
                        && item.get("namespace").and_then(serde_json::Value::as_str) == Some(GRID_SYSTEM_NS)
                })
            });
            if let Some(current) = current {
                let observed = current.get("observedGeneration").and_then(serde_json::Value::as_i64);
                let phase = current
                    .get("phase")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                last_state = format!("phase={phase}, observed={observed:?}, generation={generation:?}");
                if phase == "Rendered" && observed == generation {
                    let config_output = Command::new("kubectl")
                        .args([
                            "--context",
                            &context,
                            "-n",
                            GRID_SYSTEM_NS,
                            "get",
                            "configmap",
                            "praxis-consumer-config",
                            "-o",
                            "json",
                        ])
                        .output()?;
                    if !config_output.status.success() {
                        return Err("GridNetwork reports consumer config Rendered but its ConfigMap is absent".into());
                    }
                    let config_map: serde_json::Value = serde_json::from_slice(&config_output.stdout)?;
                    validate_generated_consumer_yaml(&config_map)?;
                    return Ok(serde_json::json!({
                        "generation": generation,
                        "status": current,
                        "config_map": config_map,
                    }));
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for operator-generated consumer config: {last_state}").into());
        }
        std::thread::park_timeout(Duration::from_secs(2));
    }
}

/// Wait for a generation-current consumer-config render error on the run-owned `GatewayRef`.
#[expect(clippy::too_many_lines, reason = "polls status through the error transition")]
fn wait_for_generated_consumer_config_error(
    expected_reason: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    const TIMEOUT: Duration = Duration::from_secs(180);
    let context = cluster_context("provider-a");
    let deadline = Instant::now() + TIMEOUT;
    let mut last_state = String::from("not observed");
    loop {
        let output = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                "gridnetwork",
                GRID_NETWORK_NAME,
                "-o",
                "json",
            ])
            .output()?;
        if output.status.success() {
            let network: serde_json::Value = serde_json::from_slice(&output.stdout)?;
            let generation = network
                .pointer("/metadata/generation")
                .and_then(serde_json::Value::as_i64);
            let statuses = network
                .pointer("/status/consumerConfigStatus")
                .and_then(serde_json::Value::as_array);
            let current = statuses.and_then(|items| {
                items.iter().find(|item| {
                    item.get("gatewayName").and_then(serde_json::Value::as_str) == Some("consumer-gateway")
                        && item.get("namespace").and_then(serde_json::Value::as_str) == Some(GRID_SYSTEM_NS)
                })
            });
            if let Some(current) = current {
                let observed = current.get("observedGeneration").and_then(serde_json::Value::as_i64);
                let phase = current
                    .get("phase")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                let reason = current
                    .get("reason")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                last_state =
                    format!("phase={phase}, reason={reason}, observed={observed:?}, generation={generation:?}");
                if phase == "Error" && reason == expected_reason && observed == generation {
                    return Ok(current.clone());
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for generation-current consumer config error: {last_state}").into());
        }
        std::thread::park_timeout(Duration::from_secs(2));
    }
}

/// Check that generated YAML is dynamic, scoped, and contains the complete
/// endpoint inventory rather than startup-only candidates.
#[expect(
    clippy::too_many_lines,
    reason = "the schema assertion checks the complete generated routing and endpoint contract"
)]
fn validate_generated_consumer_yaml(config_map: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = config_map
        .pointer("/data/praxis.yaml")
        .and_then(serde_json::Value::as_str)
        .ok_or("praxis-consumer-config has no data.praxis.yaml")?;
    let config: serde_yaml::Value = serde_yaml::from_str(yaml)?;
    let filters = config
        .get("filter_chains")
        .and_then(serde_yaml::Value::as_sequence)
        .and_then(|chains| chains.first())
        .and_then(|chain| chain.get("filters"))
        .and_then(serde_yaml::Value::as_sequence)
        .ok_or("generated consumer config has no filter chain")?;
    let route = filters
        .iter()
        .find(|filter| filter.get("filter").and_then(serde_yaml::Value::as_str) == Some("intelligent_route"))
        .ok_or("generated consumer config has no intelligent_route filter")?;
    if route.get("overlay_file").and_then(serde_yaml::Value::as_str) != Some("/etc/praxis/routing/routing-overlay.json")
        || route.get("candidates").is_some()
        || route
            .get("expected_overlay_scope")
            .and_then(|scope| scope.get("network"))
            .and_then(serde_yaml::Value::as_str)
            != Some("grid-provider-traffic")
    {
        return Err("generated consumer config does not use the expected scoped dynamic overlay".into());
    }
    let expected_provider_hops: BTreeSet<_> = PROVIDER_RESOURCES
        .iter()
        .map(|(_, cluster)| (*cluster).to_owned())
        .collect();
    let actual_provider_hops: BTreeSet<_> = route
        .get("provider_hop_clusters")
        .and_then(serde_yaml::Value::as_sequence)
        .ok_or("generated consumer config has no provider_hop_clusters list")?
        .iter()
        .map(|cluster| {
            cluster
                .as_str()
                .map(str::to_owned)
                .ok_or("generated provider_hop_clusters contains a non-string entry")
        })
        .collect::<Result<_, _>>()?;
    if actual_provider_hops != expected_provider_hops {
        return Err(format!(
            "generated provider_hop_clusters mismatch: expected {expected_provider_hops:?}, got {actual_provider_hops:?}"
        )
        .into());
    }
    let credential_inject = filters
        .iter()
        .find(|filter| filter.get("filter").and_then(serde_yaml::Value::as_str) == Some("credential_inject"))
        .ok_or("generated consumer config has no fail-closed credential_inject filter")?;
    if credential_inject
        .get("projected_credential_mount_base")
        .and_then(serde_yaml::Value::as_str)
        != Some("/run/secrets/grid-credentials")
    {
        return Err("generated consumer credential filter has no projected Secret mount fallback".into());
    }
    if credential_inject
        .get("credentials")
        .and_then(serde_yaml::Value::as_sequence)
        .is_none_or(|credentials| !credentials.is_empty())
    {
        return Err("generated consumer credential filter must use an empty dynamic credential table".into());
    }
    let cluster_count = filters
        .iter()
        .find(|filter| filter.get("filter").and_then(serde_yaml::Value::as_str) == Some("load_balancer"))
        .and_then(|filter| filter.get("clusters"))
        .and_then(serde_yaml::Value::as_sequence)
        .map_or(0, Vec::len);
    if cluster_count != PROVIDER_RESOURCES.len() {
        return Err(format!(
            "generated load balancer has {cluster_count} clusters; expected {}",
            PROVIDER_RESOURCES.len()
        )
        .into());
    }
    Ok(())
}

/// Configure and exercise the chart-managed embedded `grid-gateway` consumer.
#[expect(
    clippy::too_many_lines,
    reason = "the qualification provisions one isolated gateway and verifies its baseline request"
)]
fn activate_embedded_serving_gateway() -> AssertionResult {
    const TIMEOUT: Duration = Duration::from_secs(180);
    let started = Instant::now();
    let image = std::env::var("GRID_XTASK_GRID_GATEWAY_IMAGE")
        .unwrap_or_else(|_| "grid-gateway:provider-traffic-qualification".to_owned());
    require_local_image(&image)?;
    let repository_root = grid_repository_root()?;
    let operator_chart = repository_root.join("charts/grid-operator");

    for cluster in CLUSTERS {
        let kube_context = cluster_context(cluster);
        let upgrade = Command::new("helm")
            .args([
                "upgrade",
                "grid-operator",
                operator_chart.to_str().ok_or("Grid Operator chart path is not UTF-8")?,
                "--kube-context",
                &kube_context,
                "--namespace",
                GRID_SYSTEM_NS,
                "--reuse-values",
                "--set",
                "signals.enabled=true",
                "--wait",
                "--timeout",
                "180s",
            ])
            .output()?;
        if !upgrade.status.success() {
            return Err(format!(
                "could not enable the run-owned Grid signals listener in {cluster}: {}",
                safe_truncate_str(String::from_utf8_lossy(&upgrade.stderr).trim(), 800)
            )
            .into());
        }
    }

    let endpoints = generated_consumer_endpoints()?;
    for cluster in CLUSTERS {
        patch_grid_network_for_serving(cluster, &endpoints)?;
    }
    for cluster in CLUSTERS {
        restart_grid_operator(cluster)?;
    }

    let all_clusters = BTreeSet::from([
        "vcr-provider-a-provider".to_owned(),
        "vcr-provider-b-provider".to_owned(),
        "vcr-provider-c-provider".to_owned(),
    ]);
    let initial_serving = wait_for_grid_serving_snapshot(&all_clusters, None, TIMEOUT)?;
    require_serving_provider_hops(&initial_serving)?;

    if std::env::var("GRID_XTASK_IMAGE_PULL_POLICY").unwrap_or_else(|_| "Never".to_owned()) == "Never" {
        let kind_name = format!(
            "{}-{CONSUMER_SITE}",
            RUN_CLUSTER_PREFIX
                .get()
                .ok_or("run-owned cluster prefix was not initialized")?
        );
        crate::env::image_overrides::load_docker_image_into_kind(&image, &kind_name)?;
    }

    apply_embedded_gateway_config(&cluster_context("provider-a"), &endpoints)?;
    install_embedded_gateway(&repository_root, &image)?;
    wait_for_deployment(GRID_SERVING_GATEWAY, GRID_SYSTEM_NS, &cluster_context("provider-a"))?;
    let serving_log = wait_for_embedded_serving_log(&initial_serving, &cluster_context("provider-a"), 0, TIMEOUT)?;
    let baseline = send_embedded_lifecycle_request(None)?;
    if baseline.status != 200 || baseline.provider.is_none() {
        return Err(
            format!("embedded grid-gateway baseline did not reach an attributed provider: {baseline:?}").into(),
        );
    }
    let runtime_image = deployment_runtime_image_evidence(&cluster_context("provider-a"), GRID_SERVING_GATEWAY)?;
    let facts = BTreeMap::from([
        ("gateway_image_reference".to_owned(), serde_json::json!(image)),
        (
            "gateway_local_image_id".to_owned(),
            serde_json::json!(docker_image_id(&image)?),
        ),
        ("gateway_runtime_image_ids".to_owned(), serde_json::json!(runtime_image)),
        (
            "serving_config_initial_revision".to_owned(),
            initial_serving
                .get("digest")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        ),
        (
            "serving_config_initial_candidates".to_owned(),
            initial_serving
                .get("candidate_clusters")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        ),
        ("praxis_serving_log".to_owned(), serde_json::json!(serving_log)),
        ("baseline_request".to_owned(), serde_json::json!(baseline)),
    ]);
    Ok(proof_success(
        "chart-managed embedded grid-gateway loaded the Grid serving revision and routed an attributed request",
        facts,
        started.elapsed(),
    ))
}

/// Add the run-owned embedded gateway reference and select poll-backed serving config.
fn patch_grid_network_for_serving(
    cluster: &str,
    endpoints: &[serde_json::Value],
) -> Result<(), Box<dyn std::error::Error>> {
    let context = cluster_context(cluster);
    let network = kubectl_get_json(&context, &format!("gridnetwork/{GRID_NETWORK_NAME}"))?;
    let mut gateway_refs = network
        .pointer("/spec/gatewayRefs")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .ok_or_else(|| format!("{cluster}: GridNetwork has no gatewayRefs array"))?;
    if cluster == CONSUMER_SITE {
        if gateway_refs
            .iter()
            .any(|gateway| gateway.get("name").and_then(serde_json::Value::as_str) == Some(GRID_SERVING_GATEWAY))
        {
            return Err("run-owned GridNetwork unexpectedly already declares the embedded gateway".into());
        }
        gateway_refs.push(embedded_serving_gateway_ref(endpoints));
    }
    let patch = serde_json::json!({
        "spec": {
            "signalTransport": { "mode": "poll" },
            "gatewayRefs": gateway_refs
        }
    });
    kubectl_patch_merge(&context, &format!("gridnetwork/{GRID_NETWORK_NAME}"), &patch)?;
    Ok(())
}

/// Replace only the run-owned embedded gateway's provider-hop declaration and
/// return its exact prior value for unconditional restoration.
fn patch_embedded_provider_hop_endpoints(
    endpoints: Option<&serde_json::Value>,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let context = cluster_context(CONSUMER_SITE);
    let network = kubectl_get_json(&context, &format!("gridnetwork/{GRID_NETWORK_NAME}"))?;
    let mut gateway_refs = network
        .pointer("/spec/gatewayRefs")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .ok_or("GridNetwork has no gatewayRefs array")?;
    let index = gateway_refs
        .iter()
        .position(|gateway| gateway.get("name").and_then(serde_json::Value::as_str) == Some(GRID_SERVING_GATEWAY))
        .ok_or("GridNetwork has no run-owned embedded gateway reference")?;
    let gateway = gateway_refs
        .get_mut(index)
        .ok_or("embedded gateway reference index is out of range")?;
    let previous = gateway
        .get("providerHopEndpoints")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    if let Some(endpoints) = endpoints {
        gateway["providerHopEndpoints"] = endpoints.clone();
    } else if let Some(object) = gateway.as_object_mut() {
        object.remove("providerHopEndpoints");
    }
    kubectl_patch_merge(
        &context,
        &format!("gridnetwork/{GRID_NETWORK_NAME}"),
        &serde_json::json!({"spec": {"gatewayRefs": gateway_refs}}),
    )?;
    Ok(previous)
}

/// Wait for the controller's warning that invalid optional hop metadata was
/// ignored while publishing an empty serving revision.
#[expect(clippy::too_many_lines, reason = "polls controller logs for the scoped warning")]
fn wait_for_empty_overlay_invalid_hop_warning(timeout: Duration) -> Result<String, Box<dyn std::error::Error>> {
    let context = cluster_context(CONSUMER_SITE);
    let deadline = Instant::now() + timeout;
    loop {
        let output = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "logs",
                "deployment/grid-operator",
                "--all-containers=true",
                "--since=5m",
            ])
            .output()?;
        if output.status.success() {
            let logs = String::from_utf8_lossy(&output.stdout);
            if let Some(line) = logs
                .lines()
                .find(|line| line.contains("invalid provider-hop declaration ignored for empty serving revision"))
            {
                return Ok(safe_truncate_str(line, 1_000));
            }
        }
        if Instant::now() >= deadline {
            return Err(
                "operator did not log that invalid provider-hop metadata was ignored for the empty revision".into(),
            );
        }
        std::thread::park_timeout(Duration::from_secs(2));
    }
}

/// Build the run-owned `GatewayRef` with the explicit mTLS provider-hop inventory.
fn embedded_serving_gateway_ref(endpoints: &[serde_json::Value]) -> serde_json::Value {
    let provider_hop_endpoints: Vec<_> = endpoints
        .iter()
        .map(|endpoint| {
            serde_json::json!({
                "cluster": endpoint.get("cluster"),
                "transport": endpoint.get("transport")
            })
        })
        .collect();
    serde_json::json!({
        "name": GRID_SERVING_GATEWAY,
        "namespace": GRID_SYSTEM_NS,
        "localSiteName": CONSUMER_SITE,
        "providerHopEndpoints": provider_hop_endpoints,
        "consumerConfig": {
            "enabled": false,
            "tlsCertMountPath": "/etc/praxis/tls",
            "clusterEndpoints": [
                { "cluster": "", "address": "", "transport": null },
                { "cluster": "", "address": "", "transport": null }
            ]
        }
    })
}

/// Require the embedded serving snapshot to carry the explicit provider-hop trust list.
fn require_serving_provider_hops(snapshot: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    let expected: BTreeSet<_> = PROVIDER_RESOURCES
        .iter()
        .map(|(_, resource)| (*resource).to_owned())
        .collect();
    let actual: BTreeSet<_> = snapshot
        .pointer("/serving_config/provider_hop_clusters")
        .and_then(serde_json::Value::as_array)
        .ok_or("embedded serving snapshot has no provider_hop_clusters allowlist")?
        .iter()
        .map(|cluster| {
            cluster
                .as_str()
                .map(str::to_owned)
                .ok_or("embedded provider-hop allowlist contains a non-string entry")
        })
        .collect::<Result<_, _>>()?;
    if actual != expected {
        return Err(
            format!("embedded serving provider-hop allowlist mismatch: expected {expected:?}, got {actual:?}").into(),
        );
    }
    Ok(())
}

/// An authoritative empty serving revision must not carry provider-hop trust.
fn require_empty_serving_provider_hops(snapshot: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    let serving = snapshot
        .get("serving_config")
        .ok_or("embedded serving snapshot has no serving_config")?;
    if serving
        .get("provider_hop_clusters")
        .is_some_and(|value| !value.as_array().is_some_and(Vec::is_empty))
    {
        return Err("empty embedded serving snapshot retained or malformed provider_hop_clusters".into());
    }
    if serving
        .get("provider_hop_sni")
        .is_some_and(|value| !value.as_object().is_some_and(serde_json::Map::is_empty))
    {
        return Err("empty embedded serving snapshot retained or malformed provider_hop_sni".into());
    }
    Ok(())
}

/// Return public site labels used by the provider-attribution response header.
fn expected_provider_attributions() -> BTreeSet<String> {
    PROVIDER_RESOURCES.iter().map(|(site, _)| (*site).to_owned()).collect()
}

/// Restart one run-owned operator after its `GridNetwork` declares poll mode.
fn restart_grid_operator(cluster: &str) -> Result<(), Box<dyn std::error::Error>> {
    let context = cluster_context(cluster);
    let restart = Command::new("kubectl")
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
    if !restart.status.success() {
        return Err(format!(
            "{cluster}: could not restart the run-owned Grid operator for poll mode: {}",
            safe_truncate_str(String::from_utf8_lossy(&restart.stderr).trim(), 500)
        )
        .into());
    }
    wait_for_deployment("grid-operator", GRID_SYSTEM_NS, &context)?;
    Ok(())
}

/// Apply the embedded gateway's static listener/backend plumbing; Grid owns the changing candidates.
#[expect(
    clippy::too_many_lines,
    reason = "this renders listener plumbing and static endpoint inventory in one run-owned ConfigMap"
)]
fn apply_embedded_gateway_config(
    context: &str,
    endpoints: &[serde_json::Value],
) -> Result<(), Box<dyn std::error::Error>> {
    let clusters: Vec<_> = endpoints
        .iter()
        .map(|endpoint| {
            let cluster = endpoint
                .get("cluster")
                .and_then(serde_json::Value::as_str)
                .ok_or("generated endpoint has no cluster name")?;
            let address = endpoint
                .get("address")
                .and_then(serde_json::Value::as_str)
                .ok_or("generated endpoint has no address")?;
            let sni = endpoint
                .pointer("/transport/sni")
                .and_then(serde_json::Value::as_str)
                .ok_or("generated endpoint has no TLS server name")?;
            Ok(serde_json::json!({
                "name": cluster,
                "tls": {
                    "ca": { "ca_path": "/etc/praxis/tls/ca.crt" },
                    "client_cert": {
                        "cert_path": "/etc/praxis/tls/tls.crt",
                        "key_path": "/etc/praxis/tls/tls.key"
                    },
                    "sni": sni,
                    "verify": true
                },
                "endpoints": [address]
            }))
        })
        .collect::<Result<_, &str>>()?;
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
    let run_id = RUN_CLUSTER_PREFIX
        .get()
        .ok_or("run-owned cluster prefix was not initialized")?;
    let config_map = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "grid-serving-gateway-config",
            "namespace": GRID_SYSTEM_NS,
            "labels": { "grid.praxis-proxy.io/run-id": run_id }
        },
        "data": { "praxis.yaml": config_yaml }
    });
    kubectl_apply_json(context, &config_map)?;
    Ok(())
}

/// Install the chart-managed embedded consumer against the Grid serving `ConfigMap`.
#[expect(
    clippy::too_many_lines,
    reason = "Helm inputs and run-owned rollout verification form one bounded install operation"
)]
fn install_embedded_gateway(root: &Path, image: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (repository, tag) = parse_image_ref(image);
    let chart = root.join("charts/praxis-gateway");
    let chart = chart.to_str().ok_or("Praxis Gateway chart path is not UTF-8")?;
    let context = cluster_context(CONSUMER_SITE);
    let mut command = Command::new("helm");
    command.args([
        "upgrade",
        "--install",
        GRID_SERVING_GATEWAY,
        chart,
        "--kube-context",
        &context,
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
    ] {
        command.args(["--set-string", &value]);
    }
    command.args(["--set", "gridServing.enabled=true", "--set", "tls.enabled=true"]);
    let output = command.output()?;
    if !output.status.success() {
        return Err(format!(
            "could not install the run-owned embedded grid-gateway chart: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 1_000)
        )
        .into());
    }
    Ok(())
}

/// Wait for the operator's serving snapshot to match a candidate set and, optionally, a new digest.
fn wait_for_grid_serving_snapshot(
    expected_clusters: &BTreeSet<String>,
    previous_digest: Option<&str>,
    timeout: Duration,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let context = cluster_context(CONSUMER_SITE);
    let deadline = Instant::now() + timeout;
    let mut last = String::from("ConfigMap has not appeared");
    loop {
        if let Ok(snapshot) = read_grid_serving_snapshot(&context) {
            let digest = snapshot.get("digest").and_then(serde_json::Value::as_str);
            let clusters = snapshot
                .get("candidate_clusters")
                .and_then(serde_json::Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::to_owned)
                        .collect::<BTreeSet<_>>()
                })
                .unwrap_or_default();
            let changed = previous_digest.is_none_or(|previous| digest != Some(previous));
            last = format!("digest={digest:?}, candidates={clusters:?}");
            if &clusters == expected_clusters && changed {
                return Ok(snapshot);
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for embedded serving snapshot: {last}").into());
        }
        std::thread::park_timeout(Duration::from_secs(2));
    }
}

/// Read and verify the content-addressed operator serving snapshot.
fn read_grid_serving_snapshot(context: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let config_map = kubectl_get_json(context, &format!("configmap/{GRID_SERVING_CONFIGMAP}"))?;
    let text = config_map
        .pointer("/data/serving-config.json")
        .and_then(serde_json::Value::as_str)
        .ok_or("Grid serving ConfigMap has no serving-config.json")?;
    let digest = config_map
        .pointer("/metadata/annotations/grid.praxis-proxy.io~1serving-digest")
        .and_then(serde_json::Value::as_str)
        .ok_or("Grid serving ConfigMap has no serving digest annotation")?;
    let actual_digest = format!("{:x}", Sha256::digest(text.as_bytes()));
    if digest != actual_digest {
        return Err(format!("Grid serving digest annotation {digest} differs from content {actual_digest}").into());
    }
    let parsed: serde_json::Value = serde_json::from_str(text)?;
    let candidates = parsed
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("serving config has no candidates array")?;
    let mut candidate_clusters: Vec<_> = candidates
        .iter()
        .filter_map(|candidate| candidate.get("cluster").and_then(serde_json::Value::as_str))
        .collect();
    candidate_clusters.sort_unstable();
    Ok(serde_json::json!({
        "digest": digest,
        "candidate_count": candidates.len(),
        "candidate_clusters": candidate_clusters,
        "serving_config": parsed,
    }))
}

/// Wait for the embedded gateway process to log acceptance of one exact serving digest.
#[expect(
    clippy::too_many_lines,
    reason = "this bounded poll requires the exact digest and candidate count in runtime logs"
)]
fn wait_for_embedded_serving_log(
    snapshot: &serde_json::Value,
    context: &str,
    prior_matching_observations: usize,
    timeout: Duration,
) -> Result<String, Box<dyn std::error::Error>> {
    let digest = snapshot
        .get("digest")
        .and_then(serde_json::Value::as_str)
        .ok_or("serving snapshot evidence has no digest")?;
    let count = snapshot
        .get("candidate_count")
        .and_then(serde_json::Value::as_u64)
        .ok_or("serving snapshot evidence has no candidate count")?;
    let deadline = Instant::now() + timeout;
    let mut last_logs = String::new();
    loop {
        let output = Command::new("kubectl")
            .args([
                "--context",
                context,
                "-n",
                GRID_SYSTEM_NS,
                "logs",
                &format!("deployment/{GRID_SERVING_GATEWAY}"),
                "--all-containers",
                "--tail=400",
            ])
            .output()?;
        if output.status.success() {
            let raw_logs = String::from_utf8_lossy(&output.stdout);
            let matches = embedded_serving_revision_lines(&raw_logs, digest, count);
            if has_new_embedded_serving_revision_observation(&raw_logs, digest, count, prior_matching_observations) {
                let matched_line = matches.last().ok_or("serving log match disappeared")?;
                return Ok(safe_truncate_str(matched_line.trim(), 1_000));
            }
            last_logs = strip_ansi_csi(&raw_logs);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "embedded gateway did not log serving digest {} with candidate_count={count}: {}",
                safe_truncate_str(digest, 16),
                safe_truncate_str(&last_logs, 1_000)
            )
            .into());
        }
        std::thread::park_timeout(Duration::from_secs(2));
    }
}

/// Count existing exact serving-log observations so a repeated digest must be newly accepted.
fn embedded_serving_log_observation_count(
    snapshot: &serde_json::Value,
    context: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let digest = snapshot
        .get("digest")
        .and_then(serde_json::Value::as_str)
        .ok_or("serving snapshot evidence has no digest")?;
    let count = snapshot
        .get("candidate_count")
        .and_then(serde_json::Value::as_u64)
        .ok_or("serving snapshot evidence has no candidate count")?;
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "logs",
            &format!("deployment/{GRID_SERVING_GATEWAY}"),
            "--all-containers",
            "--tail=400",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "could not read existing embedded serving logs: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 500)
        )
        .into());
    }
    Ok(embedded_serving_revision_lines(&String::from_utf8_lossy(&output.stdout), digest, count).len())
}

/// Send one request through the embedded `grid-gateway` Service and retain status plus provider attribution.
#[expect(
    clippy::too_many_lines,
    reason = "request headers, response status, and attribution are captured as one probe"
)]
fn send_embedded_lifecycle_request(session_id: Option<&str>) -> Result<LifecycleRequest, Box<dyn std::error::Error>> {
    let mut args = vec![
        "curl".to_owned(),
        "--include".to_owned(),
        "--silent".to_owned(),
        "--show-error".to_owned(),
        "--write-out".to_owned(),
        format!("\n{CURL_HTTP_STATUS_MARKER}%{{http_code}}\n"),
        "--header".to_owned(),
        "Content-Type: application/json".to_owned(),
    ];
    if let Some(session_id) = session_id {
        args.push("--header".to_owned());
        args.push(format!("X-Session-Id: {session_id}"));
    }
    args.extend([
        "--header".to_owned(),
        "x-ai-routing-candidate: caller-controlled-candidate".to_owned(),
        "--header".to_owned(),
        "x-ai-routing-request-id: caller-controlled-request".to_owned(),
        "--header".to_owned(),
        "x-ai-routing-revision: caller-controlled-revision".to_owned(),
        "--data".to_owned(),
        r#"{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"embedded no-route qualification"}],"max_tokens":8}"#
            .to_owned(),
        format!("http://{GRID_SERVING_GATEWAY}.{GRID_SYSTEM_NS}.svc.cluster.local:8080/v1/chat/completions"),
    ]);
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let output = run_curl_probe(&cluster_context(CONSUMER_SITE), "embedded-withdrawal-probe", &refs)?;
    if !output.status.success() {
        return Err(format!(
            "embedded gateway request command failed: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 500)
        )
        .into());
    }
    let status = curl_http_status(&output.stdout).map_err(|error| {
        format!(
            "embedded gateway response status unavailable: {error}; curl stderr: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 300)
        )
    })?;
    let provider = response_header(&output.stdout, "x-grid-provider-traffic-provider-gateway");
    Ok(LifecycleRequest { status, provider })
}

/// Get one run-cluster namespaced object as JSON.
fn kubectl_get_json(context: &str, resource: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            resource,
            "-o",
            "json",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "could not read {resource}: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 500)
        )
        .into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

/// Apply a merge patch to one run-cluster namespaced object.
fn kubectl_patch_merge(
    context: &str,
    resource: &str,
    patch: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let patch = serde_json::to_string(patch)?;
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "patch",
            resource,
            "--type=merge",
            "--patch",
            &patch,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "could not patch run-owned {resource}: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 800)
        )
        .into());
    }
    Ok(())
}

/// Apply JSON through stdin, so generated `ConfigMap` data is not shell-interpreted.
fn kubectl_apply_json(context: &str, resource: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    let mut child = Command::new("kubectl")
        .args(["--context", context, "-n", GRID_SYSTEM_NS, "apply", "-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let bytes = serde_json::to_vec(resource)?;
    child
        .stdin
        .take()
        .ok_or("kubectl stdin was not piped")?
        .write_all(&bytes)?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(format!(
            "could not apply run-owned ConfigMap: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 800)
        )
        .into());
    }
    Ok(())
}

/// Return the local container-engine image ID for an exact image reference.
fn docker_image_id(image: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("docker")
        .args(["image", "inspect", image, "--format", "{{.Id}}"])
        .output()?;
    if !output.status.success() {
        return Err(format!("could not inspect local image {image}").into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Roll the run-owned consumer onto the exact operator-generated `ConfigMap` and
/// wait for the pod to load its dynamic overlay filter before traffic tests.
#[expect(
    clippy::too_many_lines,
    reason = "the helper patches only its run-owned Deployment and checks the exact loaded config"
)]
fn activate_generated_consumer_config() -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    enable_generated_consumer_config()?;
    let evidence = wait_for_generated_consumer_config()?;
    let context = cluster_context("provider-a");
    let patch = serde_json::json!({
        "spec": {"template": {"spec": {"volumes": [{
            "name": "config",
            "configMap": {"name": "praxis-consumer-config"}
        }]}}}
    });
    let patched = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "patch",
            "deployment",
            "consumer-gateway",
            "--type=strategic",
            "--patch",
            &patch.to_string(),
        ])
        .output()?;
    if !patched.status.success() {
        return Err(format!(
            "could not point consumer-gateway at generated config: {}",
            safe_truncate_str(String::from_utf8_lossy(&patched.stderr).trim(), 800)
        )
        .into());
    }
    let rollout = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "rollout",
            "status",
            "deployment/consumer-gateway",
            "--timeout=180s",
        ])
        .output()?;
    if !rollout.status.success() {
        return Err(format!(
            "consumer-gateway did not load generated config: {}",
            safe_truncate_str(String::from_utf8_lossy(&rollout.stderr).trim(), 800)
        )
        .into());
    }
    let deployment = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "deployment",
            "consumer-gateway",
            "-o",
            "json",
        ])
        .output()?;
    if !deployment.status.success() {
        return Err("could not verify consumer-gateway config volume after rollout".into());
    }
    let deployment: serde_json::Value = serde_json::from_slice(&deployment.stdout)?;
    let uses_generated_config = deployment
        .pointer("/spec/template/spec/volumes")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|volumes| {
            volumes.iter().any(|volume| {
                volume.get("name").and_then(serde_json::Value::as_str) == Some("config")
                    && volume.pointer("/configMap/name").and_then(serde_json::Value::as_str)
                        == Some("praxis-consumer-config")
            })
        });
    if !uses_generated_config {
        return Err("consumer-gateway deployment is not mounted from praxis-consumer-config".into());
    }
    let has_projected_credential_mount = deployment
        .pointer("/spec/template/spec/containers")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|container| container.get("name").and_then(serde_json::Value::as_str) == Some("praxis"))
        .flat_map(|container| {
            container
                .pointer("/volumeMounts")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
        })
        .any(|mount| {
            mount.get("name").and_then(serde_json::Value::as_str) == Some("credential-vcr-inference-credential")
                && mount.get("mountPath").and_then(serde_json::Value::as_str)
                    == Some("/run/secrets/grid-credentials/grid-system/vcr-inference-credential")
                && mount.get("readOnly").and_then(serde_json::Value::as_bool) == Some(true)
        });
    if !has_projected_credential_mount {
        return Err(
            "consumer-gateway does not have the read-only provider Secret projection required by the generated filter"
                .into(),
        );
    }
    let readiness = declare_projected_credentials_ready()?;
    let ready_config = wait_for_generated_consumer_config()?;
    let credential_overlay = enable_provider_a_credential_candidate()?;
    let consumer_credential_request = verify_consumer_credential_request()?;
    Ok(serde_json::json!({
        "operator_generated_config": evidence,
        "readiness_attestation": readiness,
        "ready_config_status": ready_config.get("status"),
        "credential_overlay": credential_overlay,
        "consumer_credential_request": consumer_credential_request,
        "consumer_deployment_template": deployment.pointer("/spec/template/spec/volumes"),
        "projected_credential_mount": "read-only vcr-inference-credential Secret projection verified (value not captured)",
        "rollout": String::from_utf8_lossy(&rollout.stdout).trim(),
    }))
}

/// Prove the rolled-out consumer can route a credential-bearing provider-A
/// candidate without receiving the provider credential from the client.
fn verify_consumer_credential_request() -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    for attempt in 0..32 {
        let session_id = format!("consumer-projected-credential-{attempt}");
        let request = send_lifecycle_request(Some(&session_id))?;
        if request.provider.as_deref() == Some("provider-a") {
            if request.status != 200 {
                return Err(format!(
                    "consumer projected-credential request routed to provider-a but returned HTTP {}",
                    request.status
                )
                .into());
            }
            return Ok(serde_json::json!({
                "status": request.status,
                "provider": request.provider,
                "attempts_until_provider_a": attempt + 1,
                "client_supplied_provider_secret": false,
                "response_content_retained": false
            }));
        }
    }
    Err("no session-affinity probe selected credential-bearing provider-a within 32 attempts".into())
}

/// Attest the projected-credential capability only after the generated filter
/// and Secret volume have been confirmed in the rolled-out consumer Deployment.
#[expect(
    clippy::too_many_lines,
    reason = "one guarded read-patch-verify phase records a single readiness attestation"
)]
fn declare_projected_credentials_ready() -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let context = cluster_context("provider-a");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "gridnetwork",
            GRID_NETWORK_NAME,
            "-o",
            "json",
        ])
        .output()?;
    if !output.status.success() {
        return Err("could not read GridNetwork before acknowledging projected credentials".into());
    }
    let network: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let gateways = network
        .pointer("/spec/gatewayRefs")
        .and_then(serde_json::Value::as_array)
        .ok_or("GridNetwork has no gatewayRefs array")?;
    let index = gateways
        .iter()
        .position(|gateway| {
            gateway.get("name").and_then(serde_json::Value::as_str) == Some("consumer-gateway")
                && gateway.get("namespace").and_then(serde_json::Value::as_str) == Some(GRID_SYSTEM_NS)
        })
        .ok_or("GridNetwork does not contain the run-owned consumer-gateway")?;
    let gateway = gateways
        .get(index)
        .ok_or("GridNetwork consumer-gateway index is out of range")?;
    let already_ready = gateway
        .pointer("/consumerConfig/supportsProjectedCredentials")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if already_ready {
        return Err("consumer projected-credential readiness was already set before this rollout".into());
    }
    if gateway
        .pointer("/consumerConfig/enableProjectedCredentials")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        return Err("consumer projected credential filter was not enabled before rollout".into());
    }
    let patch = serde_json::json!([{
        "op": "replace",
        "path": format!("/spec/gatewayRefs/{index}/consumerConfig/supportsProjectedCredentials"),
        "value": true
    }]);
    let patched = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "patch",
            "gridnetwork",
            GRID_NETWORK_NAME,
            "--type=json",
            "--patch",
            &patch.to_string(),
        ])
        .output()?;
    if !patched.status.success() {
        return Err(format!(
            "could not acknowledge projected-credential rollout: {}",
            safe_truncate_str(String::from_utf8_lossy(&patched.stderr).trim(), 800)
        )
        .into());
    }
    Ok(serde_json::json!({
        "enableProjectedCredentials": true,
        "supportsProjectedCredentials": true,
        "proof": "operator generated filter and read-only Secret mount verified after rollout"
    }))
}

/// Add one credential-bearing local candidate after the generated consumer
/// filter is rolled out, then wait until Grid publishes its Secret reference.
#[expect(
    clippy::too_many_lines,
    reason = "patch, bounded publication wait, and evidence form one ordered qualification phase"
)]
fn enable_provider_a_credential_candidate() -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let previous = read_cluster_overlay("provider-a")?;
    let context = cluster_context("provider-a");
    let patch = serde_json::json!({
        "spec": {
            "auth": {
                "strategy": "bearer_token",
                "secretRef": {
                    "name": VCR_INFERENCE_CREDENTIAL,
                    "namespace": GRID_SYSTEM_NS,
                    "key": "token"
                }
            }
        }
    });
    let patched = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "patch",
            "inferenceproviders.grid.praxis-proxy.io",
            "vcr-provider-a-provider",
            "--type=merge",
            "--patch",
            &patch.to_string(),
        ])
        .output()?;
    if !patched.status.success() {
        return Err(format!(
            "could not add the run-owned consumer credential reference: {}",
            safe_truncate_str(String::from_utf8_lossy(&patched.stderr).trim(), 800)
        )
        .into());
    }

    let deadline = Instant::now() + PROJECTED_CREDENTIAL_OVERLAY_TIMEOUT;
    loop {
        let last_state = match read_cluster_overlay("provider-a") {
            Ok(overlay) => {
                let candidate = overlay
                    .candidates
                    .iter()
                    .find(|candidate| candidate.site == "provider-a" && candidate.has_credential_reference);
                if let Some(candidate) = candidate {
                    if overlay.semantic_revision != previous.semantic_revision {
                        return Ok(serde_json::json!({
                            "previous_revision": previous.semantic_revision,
                            "credential_revision": overlay.semantic_revision,
                            "candidate_site": candidate.site,
                            "credential_reference_present": true,
                            "secret_value_captured": false
                        }));
                    }
                    "credential candidate observed without a new overlay revision".to_owned()
                } else {
                    format!(
                        "revision={} has no credential-bearing provider-a candidate",
                        overlay.semantic_revision
                    )
                }
            },
            Err(error) => error.to_string(),
        };
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for credential-bearing consumer overlay: {last_state}").into());
        }
        std::thread::park_timeout(Duration::from_secs(2));
    }
}

/// Run the original six proofs and append lifecycle coverage in full mode.
#[expect(
    clippy::too_many_lines,
    reason = "scenario order and the original six proofs remain explicit and auditable"
)]
fn run_provider_traffic_scenarios(mode: DemoMode, context: &ProviderTrafficContext) -> BTreeMap<String, ProofResult> {
    let mut results = BTreeMap::new();
    let mut scenario_num: usize = 0;
    let mut scenario = || {
        scenario_num += 1;
        scenario_num
    };

    eprintln!();
    eprintln!("=== PROVIDER TRAFFIC SCENARIOS ===");
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

    if provider_traffic_scenario_names(mode).contains(&"provider_withdrawal_lifecycle") {
        eprintln!();
        eprintln!(
            "[SCENARIO {}] Prove the embedded grid-gateway serving-config path is live and revision-aware",
            scenario()
        );
        let embedded = run_assertion("embedded_serving_gateway_convergence", || {
            activate_embedded_serving_gateway()
        });
        let embedded_ready = match embedded {
            Ok(proof) => {
                let ready = proof.success;
                results.insert("embedded_serving_gateway_convergence".to_owned(), proof);
                ready
            },
            Err(error) => {
                results.insert(
                    "embedded_serving_gateway_convergence".to_owned(),
                    proof_failure(
                        &format!("embedded serving gateway setup failed: {error}"),
                        BTreeMap::new(),
                        Duration::ZERO,
                    ),
                );
                false
            },
        };

        eprintln!();
        eprintln!(
            "[SCENARIO {}] Prove provider withdrawal, no-route serving, and restoration",
            scenario()
        );
        let proof = run_assertion("provider_withdrawal_lifecycle", || {
            Ok(assert_provider_withdrawal_lifecycle(context, embedded_ready))
        });
        match proof {
            Ok(proof) => {
                results.insert("provider_withdrawal_lifecycle".to_owned(), proof);
            },
            Err(error) => {
                results.insert(
                    "provider_withdrawal_lifecycle".to_owned(),
                    proof_failure(
                        &format!("provider withdrawal lifecycle failed: {error}"),
                        BTreeMap::new(),
                        Duration::ZERO,
                    ),
                );
            },
        }
    }

    results
}

/// Send serial, unbound requests through one consumer gateway and verify that
/// the active no-metrics round-robin picker distributes them across the
/// distinct provider gateways.  This is intentionally a request-path proof:
/// the request itself is the only source of the attribution counts.
#[expect(
    clippy::too_many_lines,
    reason = "The assertion framework requires a fallible, named traffic proof boundary."
)]
fn assert_provider_gateway_round_robin() -> AssertionResult {
    let start = Instant::now();
    let context = cluster_context("provider-a");
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut sequence = Vec::new();
    let mut failures = Vec::new();
    let mut response_diagnostics = Vec::new();
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
    let gateway_posts_before: BTreeMap<String, u64> = PROVIDER_RESOURCES
        .iter()
        .map(|(site, _)| Ok(((*site).to_owned(), read_provider_gateway_post_count_cold(site)?)))
        .collect::<Result<_, Box<dyn std::error::Error>>>()?;

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
            &context,
            &format!("rr-{request_number}"),
            &[
                "curl",
                "--fail-with-body",
                "--include",
                "--silent",
                "--show-error",
                "--write-out",
                &format!("\n{CURL_HTTP_STATUS_MARKER}%{{http_code}}\n"),
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
                    response_diagnostics.push(serde_json::json!({
                        "request": request_label,
                        "curl_exit_code": output.status.code,
                        "http_status": curl_http_status(&output.stdout).ok(),
                        "http_status_line": response_status_line(&output.stdout),
                        "response_header_names": response_header_names(&output.stdout),
                        "response_bytes": output.stdout.len(),
                    }));
                }
            },
            Ok(output) => {
                failures.push(format!(
                    "{request_label}: HTTP probe failed: {}",
                    safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 300)
                ));
                response_diagnostics.push(serde_json::json!({
                    "request": request_label,
                    "curl_exit_code": output.status.code,
                    "http_status": curl_http_status(&output.stdout).ok(),
                    "http_status_line": response_status_line(&output.stdout),
                    "response_header_names": response_header_names(&output.stdout),
                    "response_bytes": output.stdout.len(),
                }));
            },
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
    let gateway_posts_after: BTreeMap<String, u64> = PROVIDER_RESOURCES
        .iter()
        .map(|(site, _)| Ok(((*site).to_owned(), read_provider_gateway_post_count(site)?)))
        .collect::<Result<_, Box<dyn std::error::Error>>>()?;

    let mut facts = BTreeMap::new();
    facts.insert(
        "provider_gateway_posts_before".to_owned(),
        serde_json::json!(gateway_posts_before),
    );
    facts.insert(
        "provider_gateway_posts_after".to_owned(),
        serde_json::json!(gateway_posts_after),
    );
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
    facts.insert(
        "response_diagnostics".to_owned(),
        serde_json::json!(response_diagnostics),
    );
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

/// Snapshot of the selector that the lifecycle test temporarily changes.
#[derive(Clone, Debug, Serialize)]
struct ProviderSelectorState {
    /// Provider site identity.
    site: String,
    /// Run-owned `InferenceProvider` name.
    resource: String,
    /// Original selector value restored in all exit paths.
    selector_value: String,
    /// Object UID captured before mutation.
    uid: String,
    /// Resource version captured before mutation.
    resource_version: String,
}

/// Capture the selector contract from one topology-owned `InferenceProvider`.
#[expect(
    clippy::too_many_lines,
    reason = "the helper validates identity and preserves every field needed for safe restoration"
)]
fn read_provider_selector(site: &str, resource: &str) -> Result<ProviderSelectorState, Box<dyn std::error::Error>> {
    let context = cluster_context(site);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "inferenceprovider",
            resource,
            "-o",
            "json",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!("{site}: could not read InferenceProvider/{resource}").into());
    }
    let provider: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let selector_value = provider
        .get("spec")
        .and_then(|spec| spec.get("siteSelector"))
        .and_then(|selector| selector.get("matchLabels"))
        .and_then(|labels| labels.get(PROVIDER_SITE_LABEL))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("{site}: InferenceProvider/{resource} has no expected site selector"))?
        .to_owned();
    if selector_value != site {
        return Err(format!("{site}: refusing to mutate unexpected site selector {selector_value:?}").into());
    }
    Ok(ProviderSelectorState {
        site: site.to_owned(),
        resource: resource.to_owned(),
        selector_value,
        uid: provider
            .pointer("/metadata/uid")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_owned(),
        resource_version: provider
            .pointer("/metadata/resourceVersion")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_owned(),
    })
}

/// Change one topology-owned provider selector without touching its workload.
#[expect(
    clippy::too_many_lines,
    reason = "selector mutation validates its target and applies only the narrowly owned label change"
)]
fn patch_provider_selector(
    state: &ProviderSelectorState,
    selector_value: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let context = cluster_context(&state.site);
    let mut patch = serde_json::json!({"spec": {"siteSelector": {"matchLabels": {}}}});
    let labels = patch
        .pointer_mut("/spec/siteSelector/matchLabels")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("failed to construct selector patch")?;
    labels.insert(PROVIDER_SITE_LABEL.to_owned(), serde_json::json!(selector_value));
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "patch",
            "inferenceprovider",
            &state.resource,
            "--type=merge",
            "--patch",
            &patch.to_string(),
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "{}: patching InferenceProvider/{} selector failed: {}",
            state.site,
            state.resource,
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 500)
        )
        .into());
    }
    Ok(())
}

/// Wait for a stable overlay containing exactly the requested clusters.
fn wait_for_overlay_clusters(
    expected: &BTreeSet<String>,
    previous_revision: Option<&str>,
) -> Result<OverlayData, Box<dyn std::error::Error>> {
    const TIMEOUT: Duration = Duration::from_secs(180);
    let deadline = Instant::now() + TIMEOUT;
    let mut stable_revision: Option<String> = None;
    loop {
        let overlay = read_cluster_overlay("provider-a")?;
        let clusters: BTreeSet<String> = overlay
            .candidates
            .iter()
            .map(|candidate| candidate.cluster.clone())
            .collect();
        let changed = previous_revision.is_none_or(|previous| overlay.semantic_revision != previous);
        if changed && &clusters == expected {
            if stable_revision.as_deref() == Some(overlay.semantic_revision.as_str()) {
                return Ok(overlay);
            }
            stable_revision = Some(overlay.semantic_revision.clone());
        } else {
            stable_revision = None;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for overlay clusters {expected:?}; last clusters={clusters:?}, revision={}",
                safe_truncate_str(&overlay.semantic_revision, 16)
            )
            .into());
        }
        std::thread::park_timeout(Duration::from_secs(2));
    }
}

/// Issue a request and retain only status plus provider attribution.
#[expect(
    clippy::too_many_lines,
    reason = "the lifecycle probe constructs a bounded request and parses only status/attribution"
)]
fn send_lifecycle_request(session_id: Option<&str>) -> Result<LifecycleRequest, Box<dyn std::error::Error>> {
    let mut args = vec![
        "curl".to_owned(),
        "--include".to_owned(),
        "--silent".to_owned(),
        "--show-error".to_owned(),
        "--write-out".to_owned(),
        format!("\n{CURL_HTTP_STATUS_MARKER}%{{http_code}}\n"),
        "--header".to_owned(),
        "Content-Type: application/json".to_owned(),
        "--header".to_owned(),
        "Authorization: Bearer consumer-token".to_owned(),
    ];
    if let Some(session_id) = session_id {
        args.push("--header".to_owned());
        args.push(format!("X-Session-Id: {session_id}"));
    }
    args.extend([
        "--data".to_owned(),
        r#"{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"no-route qualification"}],"max_tokens":8}"#
            .to_owned(),
        "http://consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions".to_owned(),
    ]);
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let output = run_curl_probe(&cluster_context("provider-a"), "withdrawal-probe", &refs)?;
    if !output.status.success() {
        return Err(format!(
            "consumer request command failed: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 500)
        )
        .into());
    }
    let status = curl_http_status(&output.stdout).map_err(|error| {
        format!(
            "consumer response status unavailable: {error}; curl stderr: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 300)
        )
    })?;
    let provider = response_header(&output.stdout, "x-grid-provider-traffic-provider-gateway");
    Ok(LifecycleRequest { status, provider })
}

/// Read a workload's pod-local metrics through a scoped port-forward.
#[expect(
    clippy::too_many_lines,
    reason = "the scoped port-forward lifecycle and bounded HTTP read form one observation contract"
)]
fn read_workload_metrics(site: &str, resource: &str, remote_port: u16) -> Result<String, Box<dyn std::error::Error>> {
    let context = cluster_context(site);
    let local_port = find_local_tcp_port()?;
    let mapping = format!("{local_port}:{remote_port}");
    let child = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "port-forward",
            resource,
            &mapping,
            "--address",
            "127.0.0.1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let _port_forward = BackendMetricsPortForward { child: Some(child) };
    if !wait_for_local_tcp_port(local_port, Duration::from_secs(10)) {
        return Err(format!("{site}: {resource} metrics port-forward did not become ready").into());
    }

    let url = format!("http://127.0.0.1:{local_port}/metrics");
    let output = Command::new("curl")
        .args([
            "--fail",
            "--silent",
            "--show-error",
            "--connect-timeout",
            "2",
            "--max-time",
            "5",
            &url,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "{site}: {resource} /metrics probe through the run-scoped port-forward failed: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 300)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?)
}

/// Read the deterministic backend's successful inference count.
fn read_backend_success_count(site: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let metrics = read_workload_metrics(site, &format!("service/vcr-inference-{site}"), 8000)?;
    let mut found = false;
    let mut total = 0_u64;
    for line in metrics
        .lines()
        .filter(|line| line.starts_with("vllm:request_success_total"))
    {
        let value = line
            .split_whitespace()
            .last()
            .ok_or_else(|| format!("{site}: malformed simulator success metric"))?
            .parse::<u64>()?;
        total = total.checked_add(value).ok_or("simulator metric sum overflow")?;
        found = true;
    }
    if !found {
        return Err(format!("{site}: simulator did not expose vllm:request_success_total").into());
    }
    Ok(total)
}

/// Count completed POSTs at the provider gateway, including failed inference.
fn read_provider_gateway_post_count(site: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let metrics = read_workload_metrics(site, "deployment/provider-gateway", 9901)?;
    parse_provider_gateway_post_count(&metrics)?
        .ok_or_else(|| format!("{site}: provider gateway did not expose a POST request counter").into())
}

/// Before the first request, Prometheus may not have a POST label series yet.
fn read_provider_gateway_post_count_cold(site: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let metrics = read_workload_metrics(site, "deployment/provider-gateway", 9901)?;
    Ok(parse_provider_gateway_post_count(&metrics)?.unwrap_or(0))
}

/// Sum provider-gateway POST ingress; `None` means no POST series has been created.
fn parse_provider_gateway_post_count(metrics: &str) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    if metrics.trim().is_empty() {
        return Err("provider gateway returned an empty metrics response".into());
    }
    let mut found = false;
    let mut total = 0_u64;
    for line in metrics
        .lines()
        .filter(|line| line.starts_with("praxis_http_requests_total{") && line.contains("method=\"POST\""))
    {
        let value = line
            .split_whitespace()
            .last()
            .ok_or("malformed provider gateway POST counter")?
            .parse::<u64>()?;
        total = total
            .checked_add(value)
            .ok_or("provider gateway POST counter overflow")?;
        found = true;
    }
    Ok(found.then_some(total))
}

/// Reserve and release an ephemeral loopback port for one metrics observation.
fn find_local_tcp_port() -> Result<u16, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

/// Wait until the run-scoped `kubectl port-forward` accepts a loopback connection.
fn wait_for_local_tcp_port(port: u16, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::park_timeout(Duration::from_millis(100));
    }
}

/// Write incremental no-route evidence so failed runs retain their last phase.
fn write_lifecycle_evidence(
    context: &ProviderTrafficContext,
    facts: &BTreeMap<String, serde_json::Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::write(
        context.evidence_dir.join("no-route-lifecycle.json"),
        serde_json::to_vec_pretty(facts)?,
    )?;
    Ok(())
}

/// Prove partial fallback, authoritative empty serving, backend non-contact,
/// session invalidation, and restoration on the running consumer gateway.
#[expect(
    clippy::too_many_lines,
    reason = "the assertion documents ordered mutations and unconditional restoration"
)]
#[expect(
    clippy::large_stack_frames,
    reason = "bounded lifecycle evidence and cleanup state live for one synchronous qualification"
)]
fn assert_provider_withdrawal_lifecycle(
    context: &ProviderTrafficContext,
    embedded_gateway_enabled: bool,
) -> ProofResult {
    let start = Instant::now();
    let mut facts = BTreeMap::new();
    let mut original_selectors = Vec::new();
    let mut original_consumer_endpoint_mode = None;
    let mut original_provider_hop_endpoints = None;
    let mut embedded_restore_prior_log_observations = 0;
    let body = (|| -> Result<(), Box<dyn std::error::Error>> {
        for (site, resource) in PROVIDER_RESOURCES {
            wait_for_deployment("provider-gateway", GRID_SYSTEM_NS, &cluster_context(site))?;
            wait_for_deployment(&format!("vcr-inference-{site}"), GRID_SYSTEM_NS, &cluster_context(site))?;
            original_selectors.push(read_provider_selector(site, resource)?);
        }
        facts.insert(
            "provider_selectors_before".to_owned(),
            serde_json::json!(original_selectors),
        );
        write_lifecycle_evidence(context, &facts)?;

        let baseline = read_cluster_overlay("provider-a")?;
        let all_clusters: BTreeSet<String> = [
            "vcr-provider-a-provider",
            "vcr-provider-b-provider",
            "vcr-provider-c-provider",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        if baseline
            .candidates
            .iter()
            .map(|candidate| candidate.cluster.clone())
            .collect::<BTreeSet<_>>()
            != all_clusters
        {
            return Err("lifecycle did not start from exactly providers A, B, and C".into());
        }
        wait_for_consumer_gateway_revision(&baseline.semantic_revision)?;
        let mut embedded_serving_digest = if embedded_gateway_enabled {
            let embedded_baseline = read_grid_serving_snapshot(&cluster_context(CONSUMER_SITE))?;
            embedded_restore_prior_log_observations =
                embedded_serving_log_observation_count(&embedded_baseline, &cluster_context(CONSUMER_SITE))?;
            if embedded_restore_prior_log_observations == 0 {
                return Err("embedded gateway has not logged acceptance of its baseline serving revision".into());
            }
            facts.insert("embedded_baseline_serving_config".to_owned(), embedded_baseline.clone());
            facts.insert(
                "embedded_baseline_log_observation_count".to_owned(),
                serde_json::json!(embedded_restore_prior_log_observations),
            );
            Some(
                embedded_baseline
                    .get("digest")
                    .and_then(serde_json::Value::as_str)
                    .ok_or("embedded baseline has no serving digest")?
                    .to_owned(),
            )
        } else {
            None
        };
        facts.insert(
            "baseline_revision".to_owned(),
            serde_json::json!(baseline.semantic_revision),
        );
        facts.insert("baseline_candidates".to_owned(), serde_json::json!(baseline.candidates));
        write_lifecycle_evidence(context, &facts)?;

        let mut bound_session = None;
        for attempt in 0..9 {
            let session_id = format!("grid258-bound-to-a-{attempt}");
            let response = send_lifecycle_request(Some(&session_id))?;
            if response.status != 200 {
                return Err(format!("baseline affinity request returned HTTP {}", response.status).into());
            }
            if response.provider.as_deref() == Some("provider-a") {
                bound_session = Some(session_id);
                break;
            }
        }
        let bound_session = bound_session.ok_or("could not bind a request session to provider A")?;
        facts.insert("provider_a_bound_session".to_owned(), serde_json::json!(bound_session));
        write_lifecycle_evidence(context, &facts)?;

        let a = original_selectors
            .iter()
            .find(|selector| selector.site == "provider-a")
            .ok_or("missing captured provider A selector")?;
        patch_provider_selector(a, "grid258-withdrawn-provider-a")?;
        facts.insert(
            "withdrawn_provider_a_selector".to_owned(),
            serde_json::json!("grid258-withdrawn-provider-a"),
        );
        write_lifecycle_evidence(context, &facts)?;
        let fallback_set = BTreeSet::from([
            "vcr-provider-b-provider".to_owned(),
            "vcr-provider-c-provider".to_owned(),
        ]);
        let fallback = wait_for_overlay_clusters(&fallback_set, Some(&baseline.semantic_revision))?;
        wait_for_consumer_gateway_revision(&fallback.semantic_revision)?;
        facts.insert(
            "fallback_revision".to_owned(),
            serde_json::json!(fallback.semantic_revision),
        );
        facts.insert("fallback_candidates".to_owned(), serde_json::json!(fallback.candidates));
        write_lifecycle_evidence(context, &facts)?;

        if embedded_gateway_enabled {
            let expected = BTreeSet::from([
                "vcr-provider-b-provider".to_owned(),
                "vcr-provider-c-provider".to_owned(),
            ]);
            let serving = wait_for_grid_serving_snapshot(
                &expected,
                embedded_serving_digest.as_deref(),
                Duration::from_secs(180),
            )?;
            let log =
                wait_for_embedded_serving_log(&serving, &cluster_context(CONSUMER_SITE), 0, Duration::from_secs(90))?;
            for attempt in 0..6 {
                let response = send_embedded_lifecycle_request(Some(&format!("grid258-embedded-fallback-{attempt}")))?;
                if response.status != 200 || response.provider.as_deref() == Some("provider-a") {
                    return Err(format!(
                        "embedded serving-config fallback reached a hard-withdrawn provider or failed: {response:?}"
                    )
                    .into());
                }
            }
            facts.insert("embedded_fallback_serving_config".to_owned(), serving.clone());
            facts.insert("embedded_fallback_serving_log".to_owned(), serde_json::json!(log));
            facts.insert("embedded_fallback_requests".to_owned(), serde_json::json!(6));
            embedded_serving_digest = Some(
                serving
                    .get("digest")
                    .and_then(serde_json::Value::as_str)
                    .ok_or("embedded fallback has no serving digest")?
                    .to_owned(),
            );
            write_lifecycle_evidence(context, &facts)?;
        }

        let mut fallback_providers = BTreeSet::new();
        for _ in 0..6 {
            let response = send_lifecycle_request(None)?;
            if response.status != 200 {
                return Err(format!("partial-withdrawal request returned HTTP {}", response.status).into());
            }
            let provider = response
                .provider
                .ok_or("fallback request had no provider attribution")?;
            if provider == "provider-a" {
                return Err("hard-withdrawn provider A still served a new request".into());
            }
            fallback_providers.insert(provider);
        }
        if fallback_providers != BTreeSet::from(["provider-b".to_owned(), "provider-c".to_owned()]) {
            return Err(format!(
                "partial withdrawal did not route across both eligible fallbacks: {fallback_providers:?}"
            )
            .into());
        }
        facts.insert("fallback_attribution".to_owned(), serde_json::json!(fallback_providers));
        write_lifecycle_evidence(context, &facts)?;

        for state in original_selectors.iter().filter(|state| state.site != "provider-a") {
            patch_provider_selector(state, &format!("grid258-withdrawn-{}", state.site))?;
        }
        facts.insert("withdrawn_provider_b_c_selectors".to_owned(), serde_json::json!(true));
        write_lifecycle_evidence(context, &facts)?;

        let previous_mode = set_generated_consumer_provider_a_transport_mode("plaintext")?;
        original_consumer_endpoint_mode = Some(previous_mode.clone());
        if previous_mode != "mutual_tls" {
            return Err(format!(
                "negative consumer-config probe expected Provider A mutual_tls before injection, got {previous_mode:?}"
            )
            .into());
        }
        facts.insert(
            "consumer_config_failure_injected".to_owned(),
            serde_json::json!({
                "provider_cluster": "vcr-provider-a-provider",
                "transport_mode": "plaintext",
                "sni_remains_configured": true
            }),
        );
        write_lifecycle_evidence(context, &facts)?;
        let consumer_config_failure = wait_for_generated_consumer_config_error("PlaintextWithSni")?;
        facts.insert("consumer_config_failure_status".to_owned(), consumer_config_failure);
        write_lifecycle_evidence(context, &facts)?;

        let invalid_hops = serde_json::json!([{
            "cluster": "vcr-provider-a-provider",
            "transport": {
                "mode": "plaintext",
                "sni": "provider-a.grid.internal"
            }
        }]);
        original_provider_hop_endpoints = Some(patch_embedded_provider_hop_endpoints(Some(&invalid_hops))?);
        facts.insert(
            "invalid_provider_hop_metadata_injected".to_owned(),
            serde_json::json!({
                "cluster": "vcr-provider-a-provider",
                "transport_mode": "plaintext",
                "sni": "provider-a.grid.internal"
            }),
        );
        write_lifecycle_evidence(context, &facts)?;

        let empty = wait_for_overlay_clusters(&BTreeSet::new(), Some(&fallback.semantic_revision))?;
        wait_for_consumer_gateway_revision(&empty.semantic_revision)?;
        facts.insert("empty_revision".to_owned(), serde_json::json!(empty.semantic_revision));
        facts.insert(
            "empty_candidate_count".to_owned(),
            serde_json::json!(empty.candidates.len()),
        );
        facts.insert(
            "empty_overlay_published_during_consumer_config_failure".to_owned(),
            serde_json::json!(true),
        );
        facts.insert(
            "praxis_empty_serving_revision".to_owned(),
            serde_json::json!(empty.semantic_revision),
        );
        if embedded_gateway_enabled {
            let serving = wait_for_grid_serving_snapshot(
                &BTreeSet::new(),
                embedded_serving_digest.as_deref(),
                Duration::from_secs(180),
            )?;
            require_empty_serving_provider_hops(&serving)?;
            let log =
                wait_for_embedded_serving_log(&serving, &cluster_context(CONSUMER_SITE), 0, Duration::from_secs(90))?;
            let warning = wait_for_empty_overlay_invalid_hop_warning(Duration::from_secs(45))?;
            facts.insert("embedded_empty_serving_config".to_owned(), serving);
            facts.insert("embedded_empty_serving_log".to_owned(), serde_json::json!(log));
            facts.insert(
                "invalid_provider_hop_metadata_ignored".to_owned(),
                serde_json::json!(warning),
            );
            embedded_serving_digest = Some(
                read_grid_serving_snapshot(&cluster_context(CONSUMER_SITE))?
                    .get("digest")
                    .and_then(serde_json::Value::as_str)
                    .ok_or("embedded empty snapshot has no serving digest")?
                    .to_owned(),
            );
        }
        if let Some(previous) = original_provider_hop_endpoints.take() {
            let prior = if previous.is_null() { None } else { Some(&previous) };
            let injected = patch_embedded_provider_hop_endpoints(prior)?;
            if injected
                != serde_json::json!([{
                    "cluster": "vcr-provider-a-provider",
                    "transport": {
                        "mode": "plaintext",
                        "sni": "provider-a.grid.internal"
                    }
                }])
            {
                return Err("embedded provider-hop declaration changed unexpectedly before restoration".into());
            }
            facts.insert(
                "provider_hop_metadata_restored_before_provider_restore".to_owned(),
                serde_json::json!(true),
            );
        }
        write_lifecycle_evidence(context, &facts)?;

        let restored_mode = set_generated_consumer_provider_a_transport_mode(
            original_consumer_endpoint_mode
                .as_deref()
                .ok_or("negative consumer-config probe lost its original transport mode")?,
        )?;
        if restored_mode != "plaintext" {
            return Err(
                format!("consumer-config probe expected plaintext before restore, got {restored_mode:?}").into(),
            );
        }
        original_consumer_endpoint_mode = None;
        let restored_consumer_config = wait_for_generated_consumer_config()?;
        facts.insert(
            "consumer_config_restored_after_negative_probe".to_owned(),
            serde_json::json!({
                "phase": restored_consumer_config.pointer("/status/phase"),
                "observed_generation": restored_consumer_config.pointer("/status/observedGeneration")
            }),
        );
        write_lifecycle_evidence(context, &facts)?;

        for (site, _) in PROVIDER_RESOURCES {
            wait_for_deployment("provider-gateway", GRID_SYSTEM_NS, &cluster_context(site))?;
            wait_for_deployment(&format!("vcr-inference-{site}"), GRID_SYSTEM_NS, &cluster_context(site))?;
        }
        let metrics_before: BTreeMap<String, u64> = PROVIDER_RESOURCES
            .iter()
            .map(|(site, _)| Ok(((*site).to_owned(), read_backend_success_count(site)?)))
            .collect::<Result<_, Box<dyn std::error::Error>>>()?;
        let gateway_posts_before: BTreeMap<String, u64> = PROVIDER_RESOURCES
            .iter()
            .map(|(site, _)| Ok(((*site).to_owned(), read_provider_gateway_post_count(site)?)))
            .collect::<Result<_, Box<dyn std::error::Error>>>()?;
        facts.insert(
            "backend_success_total_before_no_route".to_owned(),
            serde_json::json!(metrics_before),
        );
        facts.insert(
            "provider_gateway_posts_before_no_route".to_owned(),
            serde_json::json!(gateway_posts_before),
        );
        write_lifecycle_evidence(context, &facts)?;

        let mut no_route_results = Vec::new();
        for attempt in 0..3 {
            no_route_results.push(send_lifecycle_request(Some(&format!(
                "grid258-new-no-route-{attempt}"
            )))?);
        }
        no_route_results.push(send_lifecycle_request(Some(&bound_session))?);
        if no_route_results
            .iter()
            .any(|response| response.status != 404 || response.provider.is_some())
        {
            return Err(format!("no-route requests were not all unattributed HTTP 404: {no_route_results:?}").into());
        }
        facts.insert("no_route_requests".to_owned(), serde_json::json!(no_route_results));
        if embedded_gateway_enabled {
            let embedded_no_route: Vec<_> = (0..3)
                .map(|attempt| send_embedded_lifecycle_request(Some(&format!("grid258-embedded-no-route-{attempt}"))))
                .collect::<Result<_, _>>()?;
            if embedded_no_route
                .iter()
                .any(|response| response.status != 404 || response.provider.is_some())
            {
                return Err(format!(
                    "embedded gateway continued to route after serving empty revision: {embedded_no_route:?}"
                )
                .into());
            }
            let bound_probe = send_embedded_lifecycle_request(Some(&bound_session))?;
            if bound_probe.status != 404 || bound_probe.provider.is_some() {
                return Err(format!(
                    "embedded gateway routed a request with a session header despite an empty candidate snapshot: {bound_probe:?}"
                )
                .into());
            }
            facts.insert(
                "embedded_no_route_requests".to_owned(),
                serde_json::json!(embedded_no_route),
            );
            facts.insert(
                "embedded_session_header_no_route_probe".to_owned(),
                serde_json::json!(bound_probe),
            );
        }

        let metrics_after: BTreeMap<String, u64> = PROVIDER_RESOURCES
            .iter()
            .map(|(site, _)| Ok(((*site).to_owned(), read_backend_success_count(site)?)))
            .collect::<Result<_, Box<dyn std::error::Error>>>()?;
        let gateway_posts_after: BTreeMap<String, u64> = PROVIDER_RESOURCES
            .iter()
            .map(|(site, _)| Ok(((*site).to_owned(), read_provider_gateway_post_count(site)?)))
            .collect::<Result<_, Box<dyn std::error::Error>>>()?;
        if gateway_posts_before != gateway_posts_after {
            return Err(format!(
                "provider gateway POST counters changed while no route was serving: before={gateway_posts_before:?}, after={gateway_posts_after:?}"
            )
            .into());
        }
        if metrics_before != metrics_after {
            return Err(format!("backend request counters changed while no route was serving: before={metrics_before:?}, after={metrics_after:?}").into());
        }
        facts.insert(
            "backend_success_total_after_no_route".to_owned(),
            serde_json::json!(metrics_after),
        );
        facts.insert(
            "provider_gateway_posts_after_no_route".to_owned(),
            serde_json::json!(gateway_posts_after),
        );
        facts.insert("backend_non_contact_proven".to_owned(), serde_json::json!(true));
        if embedded_gateway_enabled {
            facts.insert(
                "embedded_backend_non_contact_proven".to_owned(),
                serde_json::json!(true),
            );
        }
        write_lifecycle_evidence(context, &facts)?;

        for state in &original_selectors {
            patch_provider_selector(state, &state.selector_value)?;
        }
        let restored = wait_for_overlay_clusters(&all_clusters, Some(&empty.semantic_revision))?;
        wait_for_consumer_gateway_revision(&restored.semantic_revision)?;
        facts.insert(
            "restored_revision".to_owned(),
            serde_json::json!(restored.semantic_revision),
        );
        facts.insert("restored_candidates".to_owned(), serde_json::json!(restored.candidates));
        if embedded_gateway_enabled {
            let serving = wait_for_grid_serving_snapshot(
                &all_clusters,
                embedded_serving_digest.as_deref(),
                Duration::from_secs(180),
            )?;
            let log = wait_for_embedded_serving_log(
                &serving,
                &cluster_context(CONSUMER_SITE),
                embedded_restore_prior_log_observations,
                Duration::from_secs(90),
            )?;
            facts.insert("embedded_restored_serving_config".to_owned(), serving);
            facts.insert("embedded_restored_serving_log".to_owned(), serde_json::json!(log));
        }
        let response = send_lifecycle_request(None)?;
        if response.status != 200
            || !expected_provider_attributions().contains(response.provider.as_deref().unwrap_or_default())
        {
            return Err(format!("restored provider request was not attributed to A/B/C: {response:?}").into());
        }
        facts.insert("restored_request".to_owned(), serde_json::json!(response));
        if embedded_gateway_enabled {
            let embedded_response = send_embedded_lifecycle_request(None)?;
            if embedded_response.status != 200
                || !expected_provider_attributions().contains(embedded_response.provider.as_deref().unwrap_or_default())
            {
                return Err(
                    format!("embedded gateway did not restore attributed traffic: {embedded_response:?}").into(),
                );
            }
            facts.insert(
                "embedded_restored_request".to_owned(),
                serde_json::json!(embedded_response),
            );
        }
        facts.insert("provider_workloads_remained_ready".to_owned(), serde_json::json!(true));
        write_lifecycle_evidence(context, &facts)?;
        Ok(())
    })();

    let mut consumer_config_restoration_error = None;
    if let Some(mode) = original_consumer_endpoint_mode.take()
        && let Err(error) = set_generated_consumer_provider_a_transport_mode(&mode)
            .and_then(|_| wait_for_generated_consumer_config().map(|_| ()))
    {
        consumer_config_restoration_error = Some(error.to_string());
    }
    let mut restoration_error = consumer_config_restoration_error.clone();
    if let Some(previous) = original_provider_hop_endpoints.take() {
        let prior = if previous.is_null() { None } else { Some(&previous) };
        if let Err(error) = patch_embedded_provider_hop_endpoints(prior) {
            restoration_error.get_or_insert_with(|| format!("provider-hop declaration: {error}"));
        }
    }
    if !original_selectors.is_empty() {
        let before_restore = read_cluster_overlay("provider-a").ok();
        for state in &original_selectors {
            if let Err(error) = patch_provider_selector(state, &state.selector_value) {
                restoration_error.get_or_insert_with(|| format!("{}: {error}", state.site));
            }
        }
        if restoration_error.is_none() {
            let all_clusters: BTreeSet<String> = PROVIDER_RESOURCES
                .iter()
                .map(|(_, resource)| (*resource).to_owned())
                .collect();
            let current_clusters = before_restore.as_ref().map(|overlay| {
                overlay
                    .candidates
                    .iter()
                    .map(|candidate| candidate.cluster.clone())
                    .collect::<BTreeSet<_>>()
            });
            if current_clusters.as_ref() != Some(&all_clusters) {
                let previous_revision = before_restore
                    .as_ref()
                    .map(|overlay| overlay.semantic_revision.as_str());
                if let Err(error) = wait_for_overlay_clusters(&all_clusters, previous_revision) {
                    restoration_error = Some(error.to_string());
                }
            }
        }
    }
    facts.insert(
        "selector_restoration_error".to_owned(),
        serde_json::json!(restoration_error),
    );
    facts.insert(
        "consumer_config_restoration_error".to_owned(),
        serde_json::json!(consumer_config_restoration_error),
    );
    facts.insert(
        "lifecycle_error".to_owned(),
        serde_json::json!(body.as_ref().err().map(ToString::to_string)),
    );
    let evidence_error = write_lifecycle_evidence(context, &facts)
        .err()
        .map(|error| error.to_string());
    facts.insert("evidence_write_error".to_owned(), serde_json::json!(evidence_error));

    if let Some(error) = body.as_ref().err() {
        return proof_failure(
            &format!("provider withdrawal lifecycle failed: {error}"),
            facts,
            start.elapsed(),
        );
    }
    if let Some(error) = restoration_error {
        return proof_failure(
            &format!("provider selectors did not restore: {error}"),
            facts,
            start.elapsed(),
        );
    }
    if let Some(error) = evidence_error {
        return proof_failure(
            &format!("could not persist lifecycle evidence: {error}"),
            facts,
            start.elapsed(),
        );
    }
    proof_success(
        "partial fallback, empty overlay serving, no backend contact, bound-session denial, and provider restoration passed",
        facts,
        start.elapsed(),
    )
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
        .unwrap_or_else(|_| "grid-operator:provider-traffic-qualification".to_owned());
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

    let status = forge_command(context).arg("down").status()?;

    if !status.success() {
        return Err("failed to tear down provider-traffic environment".into());
    }

    if context.resolved_config.exists() {
        fs::remove_file(&context.resolved_config)?;
    }

    eprintln!("  [OK] Environment torn down successfully");
    Ok(())
}

/// Run the focused provider-traffic qualification.
#[expect(
    clippy::too_many_lines,
    reason = "The public demo entrypoint keeps setup, proof, evidence, and teardown visible."
)]
pub(crate) fn run(forge_config: &Path, options: &GlbDemoOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mode = options.mode();
    let run_id = format!("{}-{}", format_utc_timestamp(), std::process::id());
    let cluster_prefix = format!("grid258-{run_id}");
    RUN_CLUSTER_PREFIX
        .set(cluster_prefix.clone())
        .map_err(|error| format!("provider-traffic cluster prefix was already initialized: {error:?}"))?;
    let wall_start = Instant::now();
    let _started_at = format_utc_iso();

    let evidence_dir = resolve_evidence_dir(forge_config, options, &run_id)?;
    fs::create_dir_all(&evidence_dir)?;

    let setup_ctx = prepare_setup(forge_config, &evidence_dir, &run_id, &cluster_prefix);
    let mut teardown_success = false;
    let mut run_error = None;
    let mut overlay_state = OverlayState::default();
    let mut image_evidence = BTreeMap::new();

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
                    eprintln!();
                    eprintln!("{OUTPUT_RULE}");
                    eprintln!("ENVIRONMENT READY - Starting proof scenarios");
                    eprintln!("{OUTPUT_RULE}");

                    let generated_config_result = (mode == DemoMode::Full
                        && provider_traffic_scenario_names(mode).contains(&"generated_consumer_config_convergence"))
                    .then(|| {
                        let assertion = run_assertion("generated_consumer_config_convergence", || {
                            let started = Instant::now();
                            match activate_generated_consumer_config() {
                                Ok(evidence) => Ok(proof_success(
                                    "live consumer loaded the generated overlay and projected credential filter, then routed a credential-bearing request without a client provider secret",
                                    BTreeMap::from([("runtime_evidence".to_owned(), evidence)]),
                                    started.elapsed(),
                                )),
                                Err(error) => Ok(proof_failure(
                                    &format!("operator-generated consumer config did not converge: {error}"),
                                    BTreeMap::from([(
                                        "configuration_error".to_owned(),
                                        serde_json::json!(error.to_string()),
                                    )]),
                                    started.elapsed(),
                                )),
                            }
                        });
                        match assertion {
                            Ok(result) => result,
                            Err(error) => proof_failure(
                                &format!("generated consumer config assertion errored: {error}"),
                                BTreeMap::new(),
                                Duration::ZERO,
                            ),
                        }
                    });

                    let mut scenario_results = run_provider_traffic_scenarios(mode, context);
                    if let Some(result) = generated_config_result {
                        scenario_results.insert("generated_consumer_config_convergence".to_owned(), result);
                    }

                    let failed_proofs: Vec<&str> = scenario_results
                        .iter()
                        .filter_map(|(name, proof)| (!proof.success).then_some(name.as_str()))
                        .collect();
                    if !failed_proofs.is_empty() {
                        run_error = Some(format!("runtime proofs failed: {}", failed_proofs.join(", ")));
                    }

                    image_evidence = match collect_image_evidence() {
                        Ok(collected_images) => collected_images,
                        Err(error) => {
                            let message = format!("image evidence collection failed: {error}");
                            run_error = Some(match run_error.take() {
                                Some(previous) => format!("{previous}; {message}"),
                                None => message,
                            });
                            BTreeMap::new()
                        },
                    };

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
        images: image_evidence,
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
fn collect_image_evidence() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let mut image_evidence = BTreeMap::new();

    for cluster in CLUSTERS {
        let context = cluster_context(cluster);
        for (key, deployment) in image_evidence_deployments(cluster) {
            image_evidence.insert(
                format!("{cluster}_{key}"),
                deployment_runtime_image_evidence(&context, &deployment)?,
            );
        }
    }

    if let Some(embedded_image) = collect_embedded_gateway_image_evidence()? {
        image_evidence.insert("provider-a_embedded_grid_gateway".to_owned(), embedded_image);
    }

    Ok(image_evidence)
}

/// Capture the optional embedded Grid gateway, which is installed only on the consumer site.
fn collect_embedded_gateway_image_evidence() -> Result<Option<String>, Box<dyn std::error::Error>> {
    let consumer_context = cluster_context(CONSUMER_SITE);
    if deployment_exists(&consumer_context, GRID_SERVING_GATEWAY)? {
        return Ok(Some(deployment_runtime_image_evidence(
            &consumer_context,
            GRID_SERVING_GATEWAY,
        )?));
    }

    Ok(None)
}

/// Read the platform image's config digest, which Kind reports as the pod imageID.
fn local_image_config_digest(image: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut docker = Command::new("docker")
        .args(["image", "save", "--platform", "linux/amd64", image])
        .stdout(Stdio::piped())
        .spawn()?;
    let archive = docker.stdout.take().ok_or("docker image save did not provide stdout")?;
    let manifest = Command::new("tar")
        .args(["-xOf", "-", "manifest.json"])
        .stdin(Stdio::from(archive))
        .output()?;
    let docker_status = docker.wait()?;
    if !manifest.status.success() || !docker_status.success() {
        return Err(format!("could not inspect the local linux/amd64 image config for {image}").into());
    }
    image_config_digest_from_manifest(&manifest.stdout)
}

/// Docker's OCI archive names the config blob separately from its index digest.
fn image_config_digest_from_manifest(manifest: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let entries: serde_json::Value = serde_json::from_slice(manifest)?;
    let config = entries
        .as_array()
        .and_then(|entries| entries.first())
        .and_then(|entry| entry.get("Config"))
        .and_then(serde_json::Value::as_str)
        .and_then(|path| path.strip_prefix("blobs/sha256/"))
        .filter(|digest| digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or("image archive has no valid OCI config digest")?;
    Ok(format!("sha256:{config}"))
}

/// Check whether a deployment exists without treating a missing optional workload as an error.
fn deployment_exists(context: &str, deployment: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "get",
            &format!("deployment/{deployment}"),
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "--ignore-not-found",
            "-o",
            "json",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "could not check for embedded gateway image evidence: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let deployment_json: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    Ok(deployment_json
        .pointer("/metadata/name")
        .and_then(serde_json::Value::as_str)
        .is_some())
}

/// Return deployments that belong to one provider-traffic cluster.
pub(super) fn image_evidence_deployments(cluster: &str) -> Vec<(&'static str, String)> {
    let mut deployments = vec![
        ("operator", "grid-operator".to_owned()),
        ("provider_gateway", "provider-gateway".to_owned()),
        ("simulator", format!("vcr-inference-{cluster}")),
    ];
    if cluster == CONSUMER_SITE {
        deployments.push(("consumer_gateway", "consumer-gateway".to_owned()));
    }
    deployments
}

/// Capture requested image references and Kubernetes-reported image IDs for every Ready pod of a deployment.
#[expect(
    clippy::too_many_lines,
    reason = "deployment identity, requested images, and every ready runtime image ID are captured together"
)]
pub(super) fn deployment_runtime_image_evidence(
    context: &str,
    deployment: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let deployment_output = Command::new("kubectl")
        .args([
            "get",
            &format!("deployment/{deployment}"),
            "--context",
            context,
            "-n",
            "grid-system",
            "-o",
            "json",
        ])
        .output()?;
    if !deployment_output.status.success() {
        return Err(format!(
            "could not read deployment {deployment}: {}",
            String::from_utf8_lossy(&deployment_output.stderr).trim()
        )
        .into());
    }
    let deployment_json: serde_json::Value = serde_json::from_slice(&deployment_output.stdout)?;
    let selector = deployment_json
        .pointer("/spec/selector/matchLabels")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("deployment {deployment} has no label selector"))?;
    let requested = deployment_json
        .pointer("/spec/template/spec/containers")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("deployment {deployment} has no container template"))?;
    let requested_by_name: BTreeMap<_, _> = requested
        .iter()
        .filter_map(|container| Some((container.get("name")?.as_str()?, container.get("image")?.as_str()?)))
        .collect();
    let verify_local = std::env::var("GRID_XTASK_IMAGE_PULL_POLICY").unwrap_or_else(|_| "Never".to_owned()) == "Never";
    let mut expected_by_image = BTreeMap::new();
    if verify_local {
        for image in requested_by_name.values() {
            expected_by_image.insert(*image, local_image_config_digest(image)?);
        }
    }
    let selector_arg = selector
        .iter()
        .filter_map(|(key, value)| Some(format!("{key}={}", value.as_str()?)))
        .collect::<Vec<_>>()
        .join(",");
    if selector_arg.is_empty() {
        return Err(format!("deployment {deployment} has an empty label selector").into());
    }
    let pod_output = Command::new("kubectl")
        .args([
            "get",
            "pods",
            "-l",
            &selector_arg,
            "--context",
            context,
            "-n",
            "grid-system",
            "-o",
            "json",
        ])
        .output()?;
    if !pod_output.status.success() {
        return Err(format!(
            "could not read pods for deployment {deployment}: {}",
            String::from_utf8_lossy(&pod_output.stderr).trim()
        )
        .into());
    }
    let pods_json: serde_json::Value = serde_json::from_slice(&pod_output.stdout)?;
    let pods = pods_json
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or("kubectl pods response has no items array")?;
    let mut ready_pods = Vec::new();
    for pod in pods {
        let ready = pod
            .pointer("/status/conditions")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|conditions| {
                conditions.iter().any(|condition| {
                    condition.get("type").and_then(serde_json::Value::as_str) == Some("Ready")
                        && condition.get("status").and_then(serde_json::Value::as_str) == Some("True")
                })
            });
        if !ready {
            continue;
        }
        let pod_name = pod
            .pointer("/metadata/name")
            .and_then(serde_json::Value::as_str)
            .ok_or("matching pod has no name")?;
        let statuses = pod
            .pointer("/status/containerStatuses")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| format!("Ready pod {pod_name} has no containerStatuses"))?;
        let containers: Vec<_> = statuses
            .iter()
            .map(|status| {
                let name = status
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| format!("pod {pod_name} has unnamed container status"))?;
                let image_id = status
                    .get("imageID")
                    .and_then(serde_json::Value::as_str)
                    .filter(|image_id| !image_id.is_empty())
                    .ok_or_else(|| format!("pod {pod_name} container {name} has no runtime imageID"))?;
                let requested_image = requested_by_name
                    .get(name)
                    .ok_or_else(|| format!("pod {pod_name} container {name} is absent from the Deployment template"))?;
                let expected = expected_by_image.get(requested_image);
                if let Some(expected) = expected
                    && image_id != expected
                {
                    return Err(format!(
                        "pod {pod_name} container {name} runs {image_id}, but local image {requested_image} has config digest {expected}"
                    ));
                }
                Ok(serde_json::json!({
                    "name": name,
                    "requested": requested_image,
                    "imageID": image_id,
                    "expectedLocalConfigDigest": expected,
                    "sourceMatched": expected.is_some(),
                }))
            })
            .collect::<Result<_, String>>()?;
        ready_pods.push(serde_json::json!({ "pod": pod_name, "containers": containers }));
    }
    if ready_pods.is_empty() {
        return Err(format!("deployment {deployment} has no Ready pod for runtime image evidence").into());
    }
    ready_pods.sort_by(|left, right| left["pod"].as_str().cmp(&right["pod"].as_str()));
    Ok(serde_json::to_string(&serde_json::json!({ "pods": ready_pods }))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_evidence_deployments_scope_consumer_to_consumer_site() {
        let provider_a = image_evidence_deployments(CONSUMER_SITE);
        assert!(
            provider_a
                .iter()
                .any(|(key, deployment)| { *key == "consumer_gateway" && deployment == "consumer-gateway" })
        );

        for site in ["provider-b", "provider-c"] {
            let deployments = image_evidence_deployments(site);
            assert!(!deployments.iter().any(|(key, _)| *key == "consumer_gateway"));
        }
    }

    #[test]
    fn provider_gateway_post_counter_counts_failed_and_successful_statuses() {
        let metrics = concat!(
            "praxis_http_requests_total{method=\"GET\",status_class=\"2xx\"} 8\n",
            "praxis_http_requests_total{method=\"POST\",status_class=\"2xx\"} 3\n",
            "praxis_http_requests_total{method=\"POST\",status_class=\"5xx\"} 2\n",
        );
        assert_eq!(parse_provider_gateway_post_count(metrics).ok(), Some(Some(5)));
        assert_eq!(
            parse_provider_gateway_post_count("praxis_http_requests_total{method=\"GET\"} 8").ok(),
            Some(None),
            "a cold gateway has no POST series until it handles a POST"
        );
        assert_eq!(parse_provider_gateway_post_count("").ok(), None);
    }

    #[test]
    fn image_manifest_config_digest_is_distinct_from_index_digest() {
        let config = "a".repeat(64);
        let manifest = format!(r#"[{{"Config":"blobs/sha256/{config}"}}]"#);
        assert_eq!(
            image_config_digest_from_manifest(manifest.as_bytes()).ok(),
            Some(format!("sha256:{config}"))
        );
        assert_eq!(
            image_config_digest_from_manifest(br#"[{"Config":"../escape"}]"#).ok(),
            None
        );
    }

    #[test]
    fn embedded_consumer_charts_resolve_from_grid_repository_root() {
        let root = grid_repository_root().unwrap_or_else(|_| std::process::abort());
        assert!(root.join("charts/grid-operator/Chart.yaml").is_file());
        assert!(root.join("charts/praxis-gateway/Chart.yaml").is_file());
        assert!(!root.join("tests/e2e/topologies/grid-provider-traffic/charts").exists());
    }

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
    fn proof_count_validation() {
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
    fn curl_status_parser_uses_explicit_marker_and_rejects_no_response() {
        assert_eq!(
            curl_http_status(b"HTTP/1.1 200 OK\r\n\r\nbody\nGRID258_HTTP_STATUS:200\n"),
            Ok(200)
        );
        assert_eq!(curl_http_status(b"GRID258_HTTP_STATUS:404\n"), Ok(404));
        assert_eq!(
            curl_http_status(b"GRID258_HTTP_STATUS:000\n"),
            Err("curl received no HTTP response (status 000)".to_owned())
        );
        assert_eq!(
            curl_http_status(b"curl: (7) connection refused"),
            Err("curl output did not contain its HTTP status marker".to_owned())
        );
    }

    #[test]
    fn serving_revision_matcher_handles_colored_tracing_fields() {
        let logs = "\x1b[2mINFO\x1b[0m grid_serving_revision_serving revision=abc123 \x1b[3m candidate_count\x1b[0m\x1b[3m=\x1b[0m3";
        let line = embedded_serving_revision_lines(logs, "abc123", 3)
            .into_iter()
            .next()
            .unwrap_or_default();
        assert!(line.contains("candidate_count=3"));
        assert!(embedded_serving_revision_lines(logs, "wrong", 3).is_empty());
        assert!(embedded_serving_revision_lines(logs, "abc123", 2).is_empty());
    }

    #[test]
    fn restored_revision_wait_requires_a_new_acceptance_after_its_old_log() {
        let old = "INFO grid_serving_revision_serving revision=abc123 candidate_count=3";
        assert!(!has_new_embedded_serving_revision_observation(old, "abc123", 3, 1));
        let updated = format!("{old}\nINFO grid_serving_revision_serving revision=abc123 candidate_count=3");
        assert!(has_new_embedded_serving_revision_observation(&updated, "abc123", 3, 1));
        assert!(!has_new_embedded_serving_revision_observation(&updated, "abc123", 2, 1));
    }

    #[test]
    fn full_mode_appends_lifecycle_without_changing_the_six_existing_scenarios() {
        let quick = provider_traffic_scenario_names(DemoMode::Quick);
        let full = provider_traffic_scenario_names(DemoMode::Full);
        assert_eq!(quick.len(), 6);
        assert_eq!(full.len(), 9);
        assert_eq!(full.get(..6), Some(quick.as_slice()));
        assert_eq!(full.get(6), Some(&"generated_consumer_config_convergence"));
        assert_eq!(full.get(7), Some(&"embedded_serving_gateway_convergence"));
        assert_eq!(full.last(), Some(&"provider_withdrawal_lifecycle"));
    }

    #[test]
    fn resolved_paths_and_kind_contexts_are_run_scoped() {
        let mut config: serde_yaml::Value = serde_yaml::from_str(
            "spec:\n  runtime:\n    clusterPrefix: old\n  stacks:\n    test:\n      steps:\n        - type: template-file\n          target: .forge/runtime/{{ cluster.name }}/consumer/praxis.yaml\n          source: configs/consumer/praxis.yaml\n        - type: exec\n          command: kubectl --context kind-grid-provider-traffic-{{ cluster.name }} --from-file=.forge/runtime/{{ cluster.name }}/consumer/praxis.yaml\n",
        )
        .unwrap_or_else(|_| std::process::abort());
        rewrite_run_scoped_paths(&mut config, "grid258-unique-run", Path::new("/tmp/run/forge-state"));
        let resolved = serde_yaml::to_string(&config).unwrap_or_else(|_| std::process::abort());
        assert!(resolved.contains("target: .forge/runtime/{{ cluster.name }}/consumer/praxis.yaml"));
        assert!(resolved.contains("/tmp/run/forge-state/runtime/{{ cluster.name }}/consumer/praxis.yaml"));
        assert!(resolved.contains("kind-grid258-unique-run-{{ cluster.name }}"));
        assert!(!resolved.contains("kind-grid-provider-traffic-"));
    }

    #[test]
    fn forge_environment_and_network_name_are_run_scoped() {
        let mut config: serde_yaml::Value = serde_yaml::from_str(
            "metadata:\n  name: grid-provider-traffic\nspec:\n  runtime:\n    clusterPrefix: old\n",
        )
        .unwrap_or_else(|_| std::process::abort());
        scope_environment_name(&mut config, "run-1234").unwrap_or_else(|_| std::process::abort());
        let name = config
            .get("metadata")
            .and_then(|metadata| metadata.get("name"))
            .and_then(serde_yaml::Value::as_str)
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(name, "grid-provider-traffic-run-1234");
        assert_eq!(format!("{name}-net"), "grid-provider-traffic-run-1234-net");
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
    #[expect(
        clippy::too_many_lines,
        reason = "keeps the declaration and disabled consumer config assertions together"
    )]
    fn embedded_gateway_declares_mtls_provider_hops_separately_from_disabled_consumer_config() {
        let endpoints = PROVIDER_RESOURCES
            .iter()
            .map(|(site, resource)| {
                serde_json::json!({
                    "cluster": resource,
                    "address": format!("{site}.grid.internal:8443"),
                    "transport": { "mode": "mutual_tls", "sni": format!("{site}.grid.internal") }
                })
            })
            .collect::<Vec<_>>();
        let gateway = embedded_serving_gateway_ref(&endpoints);
        assert_eq!(
            gateway
                .pointer("/consumerConfig/enabled")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
        let rendered_endpoints = gateway
            .pointer("/providerHopEndpoints")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(rendered_endpoints.len(), PROVIDER_RESOURCES.len());
        assert!(rendered_endpoints.iter().all(|endpoint| {
            endpoint.pointer("/transport/mode").and_then(serde_json::Value::as_str) == Some("mutual_tls")
                && endpoint
                    .pointer("/transport/sni")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
        }));
        assert_eq!(
            gateway
                .pointer("/consumerConfig/clusterEndpoints/0/cluster")
                .and_then(serde_json::Value::as_str),
            Some("")
        );
    }

    #[test]
    fn embedded_serving_snapshot_requires_the_exact_provider_hop_allowlist() {
        let expected: Vec<_> = PROVIDER_RESOURCES.iter().map(|(_, resource)| *resource).collect();
        let valid = serde_json::json!({ "serving_config": { "provider_hop_clusters": expected } });
        assert!(matches!(require_serving_provider_hops(&valid), Ok(())));

        let missing = serde_json::json!({ "serving_config": { "provider_hop_clusters": [] } });
        assert!(require_serving_provider_hops(&missing).is_err());
        let extra = serde_json::json!({
            "serving_config": {
                "provider_hop_clusters": ["vcr-provider-a-provider", "vcr-provider-b-provider", "vcr-provider-c-provider", "untrusted"]
            }
        });
        assert!(require_serving_provider_hops(&extra).is_err());
    }

    #[test]
    fn empty_embedded_serving_snapshot_omits_provider_hop_trust() {
        let omitted = serde_json::json!({"serving_config": {"candidates": []}});
        assert!(matches!(require_empty_serving_provider_hops(&omitted), Ok(())));

        let explicit_empty = serde_json::json!({
            "serving_config": {
                "candidates": [],
                "provider_hop_clusters": [],
                "provider_hop_sni": {}
            }
        });
        assert!(matches!(require_empty_serving_provider_hops(&explicit_empty), Ok(())));

        let stale_trust = serde_json::json!({
            "serving_config": {
                "candidates": [],
                "provider_hop_clusters": ["provider-a"],
                "provider_hop_sni": {"provider-a": "provider-a.example"}
            }
        });
        assert!(require_empty_serving_provider_hops(&stale_trust).is_err());

        let malformed = serde_json::json!({
            "serving_config": {"candidates": [], "provider_hop_clusters": {}}
        });
        assert!(require_empty_serving_provider_hops(&malformed).is_err());
    }

    #[test]
    fn curl_probe_waits_for_a_terminal_container_status() {
        let running = serde_json::json!({"status": {"phase": "Running"}});
        assert_eq!(probe_pod_exit_code(&running), None);

        let succeeded = serde_json::json!({"status": {"phase": "Succeeded"}});
        assert_eq!(probe_pod_exit_code(&succeeded), Some(0));

        let failed = serde_json::json!({
            "status": {
                "phase": "Failed",
                "containerStatuses": [{"state": {"terminated": {"exitCode": 22}}}]
            }
        });
        assert_eq!(probe_pod_exit_code(&failed), Some(22));
    }

    #[test]
    fn response_diagnostics_retain_status_and_header_names_not_values() {
        let response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Grid-Provider-Gateway: provider-a\r\n\r\n{\"private\":\"body\"}\nGRID258_HTTP_STATUS:200\n";
        assert_eq!(response_status_line(response).as_deref(), Some("HTTP/1.1 200 OK"));
        assert_eq!(
            response_header_names(response),
            vec!["content-type".to_owned(), "x-grid-provider-gateway".to_owned()]
        );
    }

    #[test]
    fn restored_provider_attribution_uses_site_labels_not_cluster_names() {
        let sites = expected_provider_attributions();
        assert_eq!(
            sites,
            BTreeSet::from([
                "provider-a".to_owned(),
                "provider-b".to_owned(),
                "provider-c".to_owned()
            ])
        );
        assert!(sites.contains("provider-a"));
        assert!(!sites.contains("vcr-provider-a-provider"));
    }

    #[test]
    fn provider_traffic_constants_describe_focused_topology() {
        assert_eq!(CLUSTERS, &["provider-a", "provider-b", "provider-c"]);
        assert_eq!(CONSUMER_SITE, "provider-a");
        assert_eq!(EVIDENCE_SCHEMA_VERSION, "1");
    }
}
