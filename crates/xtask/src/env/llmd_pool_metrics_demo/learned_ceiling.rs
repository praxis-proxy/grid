//! Deployed proof for the learned route-ceiling lifecycle.

#![expect(
    clippy::disallowed_methods,
    clippy::too_many_lines,
    reason = "This synchronous Kind proof owns bounded polling and restores its run-owned mutations"
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    process::Command,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};

use super::{
    DemoContext, GRID_NETWORK_NAME, GRID_SERVING_CONFIGMAP, GRID_SERVING_GATEWAY, GRID_SERVING_GATEWAY_PORT,
    GRID_SERVING_METRICS_PORT, GRID_SERVING_SITE, GRID_SYSTEM_NS, MetricsTransport, OVERLAY_CONFIGMAP, ProofResult,
    SIMULATOR_CONFIGMAPS, SIMULATOR_DEPLOYMENTS, SIMULATOR_KV_CACHE_ANNOTATION, SIMULATOR_RUNNING_REQUESTS_ANNOTATION,
    SIMULATOR_WAITING_REQUESTS_ANNOTATION,
    cross_site::{
        RouteDistribution, SiteRouteResponse, has_connected_swim_peer, read_connected_sites, read_gateway_metrics,
        read_operator_metrics, send_site_request, start_ready_port_forward,
    },
    format_utc_iso, get_configmap_data_key, kind_context, kubectl, kubectl_exec_epp_metrics, run_cluster_prefix,
    set_simulator_fake_metrics, set_simulator_fake_metrics_with_endpoint_kv,
};
use crate::env::verify::PortForwardGuard;

/// Remote site whose learned ceiling is exercised.
const SITE: &str = "pool-b";
/// Provider cluster that must be removed and then returned as the exact same pair.
const CLUSTER: &str = "llmd-pool-b-provider";
/// Local provider that remains eligible while the remote pair is withdrawn.
const POOL_A_PROVIDER: &str = "llmd-pool-a-provider";
/// Serving ConfigMap annotation carrying the operator's content-addressed revision.
const SERVING_DIGEST_ANNOTATION: &str = "grid.praxis.fast/serving-digest";
/// GridNetwork annotation used by the qualification harness to request prompt reconciliation.
const METRICS_REFRESH_ANNOTATION: &str = "grid.praxis.fast/metrics-refresh-at";
/// Unique access-policy label used to exclude the pool-A consumer during this run.
const RUN_EXCLUSION_LABEL: &str = "grid.praxis.fast/issue-374-run";
/// Gauge that reports the learned `(site, cluster)` ceiling.
const CEILING_METRIC: &str = "grid_route_site_ceiling";
/// Gateway gauge for the exact pair's queue-free soft ceiling.
const SOFT_CEILING_METRIC: &str = "grid_route_site_ceiling_soft";
/// Gateway gauge for the exact pair's demonstrated hard ceiling.
const HARD_CEILING_METRIC: &str = "grid_route_site_ceiling_hard";
/// Gateway gauge for provider-reported saturation.
const REPORTED_SATURATION_METRIC: &str = "grid_route_site_rho_reported";
/// Gateway gauge for saturation derived from in-flight requests and the hard ceiling.
const COUNTED_SATURATION_METRIC: &str = "grid_route_site_rho_counted";
/// Gateway gauge for the request count read by the route snapshot.
const IN_FLIGHT_METRIC: &str = "grid_route_site_in_flight";
/// Pinned llm-d EPP running-request metric.
const RUNNING_METRIC: &str = "llm_d_epp_average_running_requests";
/// Pinned llm-d EPP queue metric.
const QUEUE_METRIC: &str = "llm_d_epp_average_queue_size";
/// Pinned llm-d EPP pool-average KV-cache utilization metric.
const KV_CACHE_METRIC: &str = "llm_d_epp_average_kv_cache_utilization";
/// Pinned llm-d EPP KV-cache utilization spread metric, when exposed.
const KV_CACHE_SPREAD_METRIC: &str = "llm_d_epp_std_dev_kv_cache_utilization";
/// Pinned llm-d EPP ready-endpoint metric.
const READY_METRIC: &str = "llm_d_epp_ready_endpoints";
/// Pinned llm-d EPP per-endpoint queue metric.
const PER_ENDPOINT_QUEUE_METRIC: &str = "llm_d_epp_per_endpoint_queue_size";
/// Pinned llm-d EPP held-request count metric, when the EPP emits it.
const HELD_QUEUE_METRIC: &str = "llm_d_epp_flow_control_queue_size";
/// Pinned llm-d EPP request-capacity utilization metric used to prove zero held work.
const HELD_CAPACITY_METRIC: &str = "llm_d_epp_flow_control_capacity_utilization_requests";
/// Container in the Grid Gateway deployment.
const GATEWAY_CONTAINER: &str = "praxis";
/// One run-owned simulator endpoint deployment scaled down to test learned-bound retirement.
const SHRINK_TEST_DEPLOYMENT: &str = "vcr-2";
/// Mounted operator-generated configuration read from the running Gateway pod.
const MOUNTED_CONFIG_PATH: &str = "/etc/praxis/grid-serving/serving-config.json";
/// Learned ceilings start at this configured floor.
const DEFAULT_CEILING_FLOOR: f64 = 8.0;
/// Deterministic per-endpoint load used to teach a clearly higher ceiling.
const HIGH_RUNNING_REQUESTS: u32 = 20;
/// Deterministic per-endpoint load used after the old sample expires.
const LOW_RUNNING_REQUESTS: u32 = 2;
/// Per-endpoint running load used to teach a lower hard ceiling at high KV pressure.
const KV_HARD_BOUND_RUNNING_REQUESTS: u32 = 8;
/// Keep the provider-reported KV-routing sample queue-free.
const KV_REPORTED_ONLY_WAITING_REQUESTS: u32 = 0;
/// Require a real per-endpoint backlog when teaching a hard bound from saturated KV cache.
const KV_HARD_BOUND_WAITING_REQUESTS: u32 = 1;
/// Asymmetric per-endpoint cache stimulus with a pool mean of one half.
const KV_HARD_BOUND_CACHE_MEAN: f64 = 0.5;
/// Expected sample standard deviation from pinned EPP for endpoint values one and zero.
const KV_HARD_BOUND_CACHE_SPREAD: f64 = std::f64::consts::FRAC_1_SQRT_2;
/// Cache utilization configured on `vcr-1`, which remains after the shrink proof.
const KV_HARD_BOUND_CACHE_HIGH_ENDPOINT: f64 = 1.0;
/// Cache utilization configured on `vcr-2`, which the shrink proof removes.
const KV_HARD_BOUND_CACHE_LOW_ENDPOINT: f64 = 0.0;
/// Once the low-cache endpoint is removed, the high-cache endpoint remains alone.
const KV_SHRUNK_CACHE_MEAN: f64 = 1.0;
/// Provider-reported saturation is the bounded mean plus spread.
const KV_HARD_BOUND_CACHE_SATURATION: f64 = 1.0;
/// Tolerance for EPP's pool aggregation and standard-deviation output.
const KV_CACHE_SIGNAL_TOLERANCE: f64 = 0.02;
/// Larger matched samples reduce random routing noise without relaxing the share assertions.
const KV_HARD_BOUND_ROUTE_REQUESTS: usize = 2048;
/// The queue-free soft-ceiling sample must measurably prefer the higher-capacity pool B.
const MIN_SOFT_BASELINE_POOL_B_SHARE: f64 = 0.75;
/// Even at its hard bound, weighted selection may send some requests to pool B while smoothed rho is below 1.
const MAX_HARD_BOUND_POOL_B_SHARE: f64 = 0.65;
/// Require at least this absolute reduction in pool-B share between matched samples.
const MIN_HARD_BOUND_POOL_B_SHARE_REDUCTION: f64 = 0.15;
/// A KV-only signal must lower pool-B's share even when its request-count bound is absent.
const MIN_REPORTED_KV_POOL_B_SHARE_REDUCTION: f64 = 0.08;
/// A provider-reported KV signal must raise the live combined route saturation near full.
const MIN_REPORTED_KV_ROUTE_RHO: f64 = 0.90;
/// Required gap between the old learned ceiling and the returned ceiling.
const CEILING_MARGIN: f64 = 8.0;
/// Tolerance around the returned floor-or-measured-load ceiling.
const RETURNED_CEILING_TOLERANCE: f64 = 2.0;
/// Exact identity of one candidate in an operator-generated serving topology.
type CandidatePair = (String, String);
/// Requests sent in each explicit attribution phase.
const REQUESTS_PER_PHASE: usize = 5;
/// Poll interval for bounded deployed-state convergence checks.
const POLL: Duration = Duration::from_secs(2);
/// Minimum observation span for a state to survive multiple Gateway polling cycles.
const STABLE_SPAN: Duration = Duration::from_secs(12);
/// Maximum wait for signal, route-config, and restoration transitions.
const TRANSITION_TIMEOUT: Duration = Duration::from_secs(180);
/// Maximum wait for simulator rollout and metric restoration.
const SIMULATOR_RESTORE_TIMEOUT: Duration = Duration::from_secs(180);
/// Globally unique session sequence so affinity cannot pin probes to an older route.
static REQUEST_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

/// Diagnostic port-forwards owned by this proof.
struct PortForwards {
    /// Grid Gateway inference listener.
    gateway_http: u16,
    /// Grid Gateway TLS metrics listener.
    gateway_metrics: u16,
    /// Pool-A operator metrics listener.
    local_operator_metrics: u16,
    /// Pool-B operator metrics listener.
    remote_operator_metrics: u16,
    /// Child guards terminate all port-forward processes on every return path.
    _guards: Vec<PortForwardGuard>,
}

/// Exact run-owned state captured before the first mutation.
struct OriginalState {
    /// Pool-B provider access policy as it existed before the proof.
    access_policy: Value,
    /// Simulator ConfigMap text as it existed before the proof, keyed by ConfigMap name.
    simulator_configs: BTreeMap<String, String>,
    /// Pool-A operator-generated routing overlay before this proof mutates the run-owned provider.
    pool_a_overlay: String,
    /// Values of the simulator metric annotations on each run-owned deployment.
    simulator_annotations: BTreeMap<String, BTreeMap<String, Option<String>>>,
    /// Desired replica count of each run-owned simulator deployment.
    simulator_replicas: BTreeMap<String, u64>,
    /// Live pool-B EPP readings before the proof changes simulator metrics.
    simulator_readings: EppReadings,
    /// Previous values of the harness reconciliation annotation on both networks.
    reconcile_annotations: BTreeMap<String, Option<String>>,
    /// Serving state and pod identity captured before any mutation.
    baseline: RuntimeSnapshot,
}

/// Required direct observations from the pinned pool-B EPP.
#[derive(Clone)]
struct EppReadings {
    /// Average running requests per ready endpoint.
    running: f64,
    /// Average waiting requests per endpoint.
    queue: f64,
    /// Pool-average KV-cache utilization, normalized to 0 through 1.
    kv_cache_utilization: f64,
    /// Pool KV-cache utilization spread, when the pinned EPP exports it.
    kv_cache_spread: Option<f64>,
    /// Number of ready endpoints.
    ready: f64,
    /// Queue size reported for each ready endpoint.
    per_endpoint_queues: Vec<f64>,
    /// Sum of held requests when the EPP exposes a direct held-count series.
    held_queue_count: Option<f64>,
    /// Exact metric family used to establish no held work.
    held_source: String,
    /// Maximum request-capacity utilization across flow-control priorities.
    held_capacity_utilization: f64,
}

/// Capacity and saturation gauges for the exact Gateway `(site, cluster)` pair.
#[derive(Clone, Copy)]
struct GatewayCapacityMetrics {
    /// Requests held by the provider as read by the Gateway.
    in_flight: Option<f64>,
    /// Queue-free soft capacity bound.
    soft_ceiling: Option<f64>,
    /// Demonstrated hard capacity bound.
    hard_ceiling: Option<f64>,
    /// Provider-reported KV-cache saturation.
    reported_saturation: Option<f64>,
    /// Request-count saturation, present only after a hard bound is known.
    counted_saturation: Option<f64>,
}

/// One timestamped view of operator, mounted, Gateway, and EPP state.
#[derive(Clone)]
struct RuntimeSnapshot {
    /// Operator-generated serving JSON.
    operator_config: String,
    /// Operator ConfigMap content digest.
    operator_digest: String,
    /// Operator ConfigMap revision annotation.
    operator_digest_annotation: String,
    /// Kubernetes resourceVersion of the operator ConfigMap.
    operator_resource_version: String,
    /// Serving JSON read inside the exact running Gateway pod.
    mounted_config: String,
    /// SHA-256 digest of the mounted serving JSON.
    mounted_digest: String,
    /// Exact `(site, cluster)` candidate pairs in the operator JSON.
    candidates: Vec<CandidatePair>,
    /// Exact `(site, cluster)` candidate pairs in the mounted Gateway JSON.
    mounted_candidates: Vec<CandidatePair>,
    /// Direct EPP readings from the pinned pool-B metrics service.
    epp: EppReadings,
    /// Raw EPP metric lines relevant to this proof.
    epp_metric_samples: String,
    /// Pool-B operator's own published in-flight count.
    origin_in_flight: f64,
    /// Pool-B operator's own ready-endpoint count.
    origin_ready: f64,
    /// Pool-A operator's relayed pool-B in-flight count.
    local_in_flight: Option<f64>,
    /// Pool-A operator's relayed pool-B ready-endpoint count.
    local_ready: Option<f64>,
    /// Relevant pool-B operator metric lines from the origin site.
    origin_operator_metric_samples: String,
    /// Relevant pool-B operator metric lines relayed to pool-A.
    local_operator_metric_samples: String,
    /// Raw Grid Gateway metrics text.
    gateway_metrics: String,
    /// Grid Gateway's cumulative applied and reused serving-config watcher observations.
    gateway_reloads: f64,
    /// Finite ceiling value, if the Gateway exports one for this pair.
    ceiling: Option<f64>,
    /// Raw ceiling gauge value, including the literal `NaN` when cleared.
    ceiling_raw: Option<String>,
    /// Whether the Gateway exports the pair's cleared ceiling as NaN.
    ceiling_is_nan: bool,
    /// The exact pair's live Gateway capacity and saturation gauges.
    gateway_capacity: GatewayCapacityMetrics,
    /// Pod UID of the running Grid Gateway process.
    gateway_uid: String,
    /// Actual pod image reference, runtime image ID, and container ID for the Gateway.
    gateway_image_identity: Value,
    /// Freshness window configured in the mounted serving file.
    load_window_ms: u64,
    /// Gateway load-store horizon configured in the mounted serving file.
    horizon_ms: u64,
    /// Gateway load-store retention configured in the mounted serving file.
    window_secs: u64,
}

/// Evidence envelope written independently for this proof.
#[derive(Serialize)]
struct LearnedCeilingEvidence<'evidence> {
    /// Evidence schema version.
    schema_version: &'static str,
    /// Git commit checked out when this proof ran.
    source_sha: Option<&'evidence str>,
    /// Whether the tracked worktree was clean when the proof started.
    tracked_worktree_clean: Option<bool>,
    /// SHA-256 fingerprint of tracked diff content and non-ignored untracked files.
    source_worktree_sha256: Option<&'evidence str>,
    /// Run-scoped Kind cluster prefix.
    run_cluster_prefix: &'static str,
    /// Start timestamp in UTC.
    started_at: &'evidence str,
    /// Final proof status, including restoration.
    success: bool,
    /// Primary lifecycle failure, if any.
    primary_error: Option<&'evidence str>,
    /// Errors encountered while restoring run-owned state.
    restoration_errors: &'evidence [String],
    /// Original-state summary captured before mutation.
    original_state: Option<Value>,
    /// Requested image references, Docker IDs, repository digests, and live pod IDs.
    image_manifest: Option<Value>,
    /// Time-ordered phase and sample evidence.
    phases: &'evidence [Value],
    /// Failed HTTP probes and transport errors, distinguished by `kind`.
    probe_failures: &'evidence [Value],
}

/// Run the learned-ceiling proof and restore its run-owned provider and simulator state.
pub(super) fn proof(context: &DemoContext) -> ProofResult {
    let mut observations = Vec::new();
    let mut phases = Vec::new();
    let mut probe_failures = Vec::new();
    let mut restoration_errors = Vec::new();
    let started_at = format_utc_iso();
    observations.push(format!("[{started_at}] learned ceiling lifecycle proof started"));

    let (source_sha, tracked_worktree_clean, source_worktree_sha256) = match source_identity() {
        Ok(identity) => identity,
        Err(error) => {
            let message = format!("could not capture source revision: {error}");
            observations.push(format!("[SETUP][FAIL] {message}"));
            record_evidence_write(
                write_evidence(
                    context,
                    None,
                    None,
                    None,
                    &started_at,
                    false,
                    Some(&message),
                    &restoration_errors,
                    None,
                    None,
                    &phases,
                    &probe_failures,
                ),
                &mut observations,
            );
            return result(false, observations);
        },
    };

    let port_forwards = match open_port_forwards() {
        Ok(forwards) => forwards,
        Err(error) => {
            let message = format!("could not open run-owned diagnostic port-forwards: {error}");
            observations.push(format!("[SETUP][FAIL] {message}"));
            record_evidence_write(
                write_evidence(
                    context,
                    Some(&source_sha),
                    Some(tracked_worktree_clean),
                    Some(&source_worktree_sha256),
                    &started_at,
                    false,
                    Some(&message),
                    &restoration_errors,
                    None,
                    None,
                    &phases,
                    &probe_failures,
                ),
                &mut observations,
            );
            return result(false, observations);
        },
    };

    let baseline = match wait_for_baseline_state(context, &port_forwards, &mut phases, &mut observations) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            let message = format!("could not establish a stable original serving state before mutation: {error}");
            observations.push(format!("[SETUP][FAIL] {message}"));
            record_evidence_write(
                write_evidence(
                    context,
                    Some(&source_sha),
                    Some(tracked_worktree_clean),
                    Some(&source_worktree_sha256),
                    &started_at,
                    false,
                    Some(&message),
                    &restoration_errors,
                    None,
                    None,
                    &phases,
                    &probe_failures,
                ),
                &mut observations,
            );
            return result(false, observations);
        },
    };

    let original = match capture_original_state(context, baseline) {
        Ok(state) => state,
        Err(error) => {
            let message = format!("could not save original run-owned state before mutation: {error}");
            observations.push(format!("[SETUP][FAIL] {message}"));
            record_evidence_write(
                write_evidence(
                    context,
                    Some(&source_sha),
                    Some(tracked_worktree_clean),
                    Some(&source_worktree_sha256),
                    &started_at,
                    false,
                    Some(&message),
                    &restoration_errors,
                    None,
                    None,
                    &phases,
                    &probe_failures,
                ),
                &mut observations,
            );
            return result(false, observations);
        },
    };

    let original_summary = Some(original_state_summary(&original));
    let image_manifest = match collect_image_manifest(context) {
        Ok(manifest) => Some(manifest),
        Err(error) => {
            let message = format!("could not capture actual image identities before mutation: {error}");
            observations.push(format!("[SETUP][FAIL] {message}"));
            record_evidence_write(
                write_evidence(
                    context,
                    Some(&source_sha),
                    Some(tracked_worktree_clean),
                    Some(&source_worktree_sha256),
                    &started_at,
                    false,
                    Some(&message),
                    &restoration_errors,
                    original_summary,
                    None,
                    &phases,
                    &probe_failures,
                ),
                &mut observations,
            );
            return result(false, observations);
        },
    };

    observations.push(format!(
        "source SHA {source_sha}; tracked worktree clean={tracked_worktree_clean}; source worktree SHA-256={source_worktree_sha256}"
    ));
    observations.push("actual image manifest captured for the run-owned pods and local image store".to_owned());

    let primary = run_lifecycle(
        context,
        &port_forwards,
        &original,
        &mut phases,
        &mut observations,
        &mut probe_failures,
    );
    if let Err(error) = &primary {
        observations.push(format!("[PROOF][FAIL] {error}"));
        if let Ok(snapshot) = observe_runtime(context, &port_forwards) {
            if let Err(capture_error) = record_phase(context, "failure-capture", &snapshot, None, &mut phases) {
                observations.push(format!(
                    "[EVIDENCE][FAIL] could not record final live diagnostics: {capture_error}"
                ));
            }
        } else {
            observations.push("[EVIDENCE][WARN] final live snapshot was unavailable before restoration".to_owned());
        }
    }

    observations.push(format!("[{}] restoration of run-owned state started", format_utc_iso()));
    restore_original_state(
        context,
        &port_forwards,
        &original,
        &mut phases,
        &mut restoration_errors,
        &mut probe_failures,
    );
    for error in &restoration_errors {
        observations.push(format!("[RESTORE][FAIL] {error}"));
    }

    let mut success = qualification_succeeded(primary.is_ok(), &restoration_errors);
    let primary_error = primary.as_ref().err().map(ToString::to_string);
    let final_error = primary_error
        .as_deref()
        .or_else(|| (!restoration_errors.is_empty()).then_some("run-owned state restoration failed"));
    match write_evidence(
        context,
        Some(&source_sha),
        Some(tracked_worktree_clean),
        Some(&source_worktree_sha256),
        &started_at,
        success,
        final_error,
        &restoration_errors,
        original_summary,
        image_manifest,
        &phases,
        &probe_failures,
    ) {
        Ok(path) => observations.push(format!("learned-ceiling evidence: {}", path.display())),
        Err(error) => {
            success = false;
            observations.push(format!(
                "[EVIDENCE][FAIL] could not write learned-ceiling evidence: {error}"
            ));
        },
    }
    observations.push(if success {
        "learned ceiling lifecycle proof passed; provider policy, simulator settings, and reconcile annotations were restored".to_owned()
    } else {
        "learned ceiling lifecycle proof failed; see primary, restoration, and phase evidence".to_owned()
    });
    result(success, observations)
}

/// Build the enclosing xtask proof result.
fn result(success: bool, observations: Vec<String>) -> ProofResult {
    ProofResult {
        success,
        description: "a queue-free running-request ceiling persists while idle; exact-pair withdrawal clears the bounds; KV saturation without waiting lowers route share, while saturated KV plus backlog teaches a hard bound; endpoint shrink retires stale bounds".to_owned(),
        observations,
    }
}

/// Record whether an early proof exit successfully persisted its diagnostic artifact.
fn record_evidence_write(
    result: Result<std::path::PathBuf, Box<dyn std::error::Error>>,
    observations: &mut Vec<String>,
) {
    match result {
        Ok(path) => observations.push(format!("evidence: {}", path.display())),
        Err(error) => observations.push(format!("[EVIDENCE][FAIL] {error}")),
    }
}

/// Require the primary proof and every run-owned restoration to succeed.
fn qualification_succeeded(primary_succeeded: bool, restoration_errors: &[String]) -> bool {
    primary_succeeded && restoration_errors.is_empty()
}

/// Capture the source commit and a digest of all source changes present at proof start.
fn source_identity() -> Result<(String, bool, String), Box<dyn std::error::Error>> {
    let revision = Command::new("git").args(["rev-parse", "HEAD"]).output()?;
    if !revision.status.success() {
        return Err(format!(
            "git rev-parse HEAD: {}",
            String::from_utf8_lossy(&revision.stderr).trim()
        )
        .into());
    }
    let sha = String::from_utf8_lossy(&revision.stdout).trim().to_owned();
    if sha.len() != 40 || !sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("git returned an invalid source SHA: {sha:?}").into());
    }
    let status = Command::new("git")
        .args(["status", "--porcelain=v1", "--untracked-files=no"])
        .output()?;
    if !status.status.success() {
        return Err(format!("git status: {}", String::from_utf8_lossy(&status.stderr).trim()).into());
    }
    let tracked_diff = Command::new("git").args(["diff", "--binary", "HEAD", "--"]).output()?;
    if !tracked_diff.status.success() {
        return Err(format!(
            "git diff --binary HEAD: {}",
            String::from_utf8_lossy(&tracked_diff.stderr).trim()
        )
        .into());
    }
    let untracked = Command::new("git")
        .args(["ls-files", "--others", "--exclude-standard", "-z"])
        .output()?;
    if !untracked.status.success() {
        return Err(format!(
            "git ls-files --others: {}",
            String::from_utf8_lossy(&untracked.stderr).trim()
        )
        .into());
    }
    let mut untracked_paths = String::from_utf8_lossy(&untracked.stdout)
        .split('\0')
        .filter(|path| !path.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    untracked_paths.sort();
    let mut hasher = Sha256::new();
    hasher.update(b"tracked-diff\0");
    hasher.update(&tracked_diff.stdout);
    for path in untracked_paths {
        let contents = fs::read(&path)?;
        hasher.update(b"\0untracked-file\0");
        hasher.update(path.as_bytes());
        hasher.update(b"\0");
        hasher.update(contents);
    }
    let source_worktree_sha256 = hex::encode(hasher.finalize());
    Ok((sha, status.stdout.is_empty(), source_worktree_sha256))
}

/// Open the HTTP and metrics listeners used by this proof.
fn open_port_forwards() -> Result<PortForwards, Box<dyn std::error::Error>> {
    let local_context = kind_context(GRID_SERVING_SITE);
    let peer_context = kind_context(SITE);
    let (gateway_http, gateway_guard) =
        start_ready_port_forward(&local_context, GRID_SERVING_GATEWAY, GRID_SERVING_GATEWAY_PORT)?;
    let (gateway_metrics, gateway_metrics_guard) = start_ready_port_forward(
        &local_context,
        &format!("{GRID_SERVING_GATEWAY}-metrics"),
        GRID_SERVING_METRICS_PORT,
    )?;
    let (local_operator_metrics, local_operator_guard) =
        start_ready_port_forward(&local_context, "grid-operator-metrics", 9090)?;
    let (remote_operator_metrics, remote_operator_guard) =
        start_ready_port_forward(&peer_context, "grid-operator-metrics", 9090)?;
    Ok(PortForwards {
        gateway_http,
        gateway_metrics,
        local_operator_metrics,
        remote_operator_metrics,
        _guards: vec![
            gateway_guard,
            gateway_metrics_guard,
            local_operator_guard,
            remote_operator_guard,
        ],
    })
}

/// Capture all state that may be changed later, before allowing the first mutation.
fn capture_original_state(
    context: &DemoContext,
    baseline: RuntimeSnapshot,
) -> Result<OriginalState, Box<dyn std::error::Error>> {
    require_pair(&baseline.candidates, "operator's baseline serving config")?;
    require_pair(&baseline.mounted_candidates, "Gateway's baseline mounted config")?;
    require_same_gateway_config(&baseline)?;

    let peer_context = kind_context(SITE);
    let provider = kube_json(&peer_context, "get", &format!("inferenceprovider/{CLUSTER}"))?;
    let access_policy = provider
        .pointer("/spec/accessPolicy")
        .cloned()
        .ok_or("pool-B InferenceProvider has no spec.accessPolicy to preserve")?;
    let mut simulator_configs = BTreeMap::new();
    for (_, configmap) in SIMULATOR_CONFIGMAPS {
        simulator_configs.insert(
            (*configmap).to_owned(),
            get_configmap_data_key(&peer_context, GRID_SYSTEM_NS, configmap, "config.yaml")?,
        );
    }
    let pool_a_overlay = get_configmap_data_key(
        &kind_context(GRID_SERVING_SITE),
        GRID_SYSTEM_NS,
        OVERLAY_CONFIGMAP,
        "routing-config.json",
    )?;
    let mut simulator_annotations = BTreeMap::new();
    let mut simulator_replicas = BTreeMap::new();
    for deployment in SIMULATOR_DEPLOYMENTS {
        let object = kube_json(&peer_context, "get", &format!("deployment/{deployment}"))?;
        let replicas = object.pointer("/spec/replicas").and_then(Value::as_u64).unwrap_or(1);
        simulator_replicas.insert((*deployment).to_owned(), replicas);
        let annotations = object
            .pointer("/spec/template/metadata/annotations")
            .and_then(Value::as_object);
        let values = [
            SIMULATOR_WAITING_REQUESTS_ANNOTATION,
            SIMULATOR_RUNNING_REQUESTS_ANNOTATION,
            SIMULATOR_KV_CACHE_ANNOTATION,
        ]
        .into_iter()
        .map(|key| {
            (
                key.to_owned(),
                annotations
                    .and_then(|items| items.get(key))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            )
        })
        .collect();
        simulator_annotations.insert((*deployment).to_owned(), values);
    }
    let simulator_readings = parse_epp_readings(&kubectl_exec_epp_metrics(
        SITE,
        context.metrics_transport == MetricsTransport::MtlsProxy,
    )?)?;
    if !epp_readings_match(&simulator_readings, &baseline.epp) {
        return Err(format!(
            "pool-B EPP readings changed while saving original state: baseline={:?}, current={:?}",
            epp_value(&baseline.epp),
            epp_value(&simulator_readings)
        )
        .into());
    }
    require_signal_values(&baseline, simulator_readings.running)?;

    let reconcile_annotations = [SITE, GRID_SERVING_SITE]
        .into_iter()
        .map(|site| {
            let network = kube_json(&kind_context(site), "get", &format!("gridnetwork/{GRID_NETWORK_NAME}"))?;
            Ok((site.to_owned(), annotation_value(&network, METRICS_REFRESH_ANNOTATION)))
        })
        .collect::<Result<BTreeMap<_, _>, Box<dyn std::error::Error>>>()?;

    Ok(OriginalState {
        access_policy,
        simulator_configs,
        pool_a_overlay,
        simulator_annotations,
        simulator_replicas,
        simulator_readings,
        reconcile_annotations,
        baseline,
    })
}

/// Wait until the run's original operator config is mounted and stably accepted before mutation.
fn wait_for_baseline_state(
    context: &DemoContext,
    forwards: &PortForwards,
    phases: &mut Vec<Value>,
    observations: &mut Vec<String>,
) -> Result<RuntimeSnapshot, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + TRANSITION_TIMEOUT;
    let mut stable_signature: Option<(String, String, u64)> = None;
    let mut stable_since = None;
    let mut stable_samples = 0_u32;
    let mut last_recorded_signature: Option<(String, String, String, u64)> = None;
    let mut last_snapshot = None;
    let mut last = String::from("no runtime sample");
    let mut evidence_sequence = 0_u32;

    while Instant::now() < deadline {
        match observe_runtime(context, forwards) {
            Ok(snapshot) => {
                let diagnostic_signature = (
                    snapshot.operator_digest.clone(),
                    snapshot.mounted_digest.clone(),
                    snapshot.gateway_uid.clone(),
                    snapshot.gateway_reloads.to_bits(),
                );
                if last_recorded_signature.as_ref() != Some(&diagnostic_signature) {
                    evidence_sequence = evidence_sequence.saturating_add(1);
                    record_phase(
                        context,
                        &format!("baseline-convergence-{evidence_sequence}"),
                        &snapshot,
                        None,
                        phases,
                    )?;
                    last_recorded_signature = Some(diagnostic_signature);
                }

                let config_state = baseline_config_problem(&snapshot);
                let signal_state = require_signal_values(&snapshot, snapshot.epp.running);
                if config_state.is_none() && signal_state.is_ok() {
                    let signature = (
                        snapshot.operator_digest.clone(),
                        snapshot.gateway_uid.clone(),
                        snapshot.gateway_reloads.to_bits(),
                    );
                    if stable_signature.as_ref() == Some(&signature) {
                        stable_samples = stable_samples.saturating_add(1);
                    } else {
                        stable_signature = Some(signature);
                        stable_since = Some(Instant::now());
                        stable_samples = 1;
                    }
                    if stable_samples >= 3 && stable_since.is_some_and(|start| start.elapsed() >= STABLE_SPAN) {
                        record_phase(context, "baseline-serving-ready", &snapshot, None, phases)?;
                        observations.push(format!(
                            "original operator config is mounted and stable at revision {}; Gateway config-watcher observation count={}, pod UID={}",
                            snapshot.operator_digest, snapshot.gateway_reloads, snapshot.gateway_uid
                        ));
                        return Ok(snapshot);
                    }
                    last = format!(
                        "waiting for stable baseline observations: {}",
                        snapshot_summary(&snapshot)
                    );
                } else {
                    stable_signature = None;
                    stable_since = None;
                    stable_samples = 0;
                    last = config_state.unwrap_or_else(|| {
                        signal_state
                            .err()
                            .map_or_else(|| "baseline state is not ready".to_owned(), |error| error.to_string())
                    });
                    if !observations.last().is_some_and(|entry| entry.contains(&last)) {
                        observations.push(format!("baseline is not ready; waiting without mutation: {last}"));
                    }
                }
                last_snapshot = Some(snapshot);
            },
            Err(error) => {
                last = error.to_string();
                stable_signature = None;
                stable_since = None;
                stable_samples = 0;
            },
        }
        std::thread::sleep(POLL);
    }

    if let Some(snapshot) = last_snapshot {
        record_phase(context, "baseline-convergence-timeout", &snapshot, None, phases)?;
    }
    Err(format!("timed out waiting for an accepted original serving config; last observation: {last}").into())
}

/// Explain why an operator and mounted config are not yet a usable baseline.
fn baseline_config_problem(snapshot: &RuntimeSnapshot) -> Option<String> {
    if !pair_present(&snapshot.candidates) {
        return Some(format!(
            "exact candidate pair ({SITE}, {CLUSTER}) is absent from operator config: {:?}",
            snapshot.candidates
        ));
    }
    if !pair_present(&snapshot.mounted_candidates) {
        return Some(format!(
            "exact candidate pair ({SITE}, {CLUSTER}) is absent from mounted config: {:?}",
            snapshot.mounted_candidates
        ));
    }
    if !gateway_config_values_match(
        &snapshot.operator_digest,
        &snapshot.operator_digest_annotation,
        &snapshot.mounted_digest,
        &snapshot.operator_config,
        &snapshot.mounted_config,
    ) {
        return Some(format!(
            "operator and mounted serving revisions have not converged: operator={} annotation={} mounted={}",
            snapshot.operator_digest, snapshot.operator_digest_annotation, snapshot.mounted_digest
        ));
    }
    None
}

/// Execute all four learned-ceiling states in order.
#[expect(
    clippy::large_stack_frames,
    reason = "the synchronous lifecycle retains small diagnostic snapshots while making sequential bounded API calls"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "the lifecycle keeps its run state, phase evidence, user observations, and failed-probe evidence explicit"
)]
fn run_lifecycle(
    context: &DemoContext,
    forwards: &PortForwards,
    original: &OriginalState,
    phases: &mut Vec<Value>,
    observations: &mut Vec<String>,
    probe_failures: &mut Vec<Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    let baseline = &original.baseline;
    let original_uid = baseline.gateway_uid.clone();
    let original_candidates = candidate_set(&baseline.candidates);
    let baseline_record = record_phase(context, "baseline-pair-present", baseline, None, phases)?;
    observations.push(format!(
        "baseline pair is published and mounted at serving digest {} (resourceVersion={}); Gateway pod UID={original_uid}",
        baseline.operator_digest, baseline.operator_resource_version
    ));
    record_attributed_requests(
        context,
        forwards,
        "before-removal",
        None,
        baseline,
        phases,
        observations,
        probe_failures,
    )?;

    set_simulator_fake_metrics(
        SITE,
        0,
        HIGH_RUNNING_REQUESTS,
        0.0,
        context.metrics_transport == MetricsTransport::MtlsProxy,
    )?;
    observations.push(format!(
        "pool-B simulator set running-requests={HIGH_RUNNING_REQUESTS}, waiting-requests=0, KV-cache=0"
    ));
    let taught = wait_for_ceiling(
        context,
        forwards,
        &original_uid,
        HIGH_RUNNING_REQUESTS,
        |snapshot| {
            snapshot
                .ceiling
                .is_some_and(|value| value > DEFAULT_CEILING_FLOOR + CEILING_MARGIN)
        },
        "high-running-ceiling",
    )?;
    let high_ceiling = taught.ceiling.ok_or("Gateway did not publish a finite high ceiling")?;
    if high_ceiling <= DEFAULT_CEILING_FLOOR + CEILING_MARGIN {
        return Err(format!("running load did not teach a ceiling above the floor with margin: {high_ceiling}").into());
    }
    record_phase(context, "high-ceiling-learned", &taught, None, phases)?;
    observations.push(format!(
        "Gateway learned ceiling {high_ceiling:.2} for ({SITE}, {CLUSTER}) from running={} across {} ready endpoints; EPP queue, per-endpoint queues, and held work were clear",
        taught.epp.running, taught.epp.ready
    ));
    record_attributed_requests(
        context,
        forwards,
        "high-ceiling-learned",
        None,
        &taught,
        phases,
        observations,
        probe_failures,
    )?;

    set_simulator_fake_metrics(
        SITE,
        0,
        0,
        0.0,
        context.metrics_transport == MetricsTransport::MtlsProxy,
    )?;
    let idle = wait_for_ceiling(
        context,
        forwards,
        &original_uid,
        0,
        |snapshot| idle_ceiling_is_retained(snapshot.gateway_capacity.in_flight, snapshot.ceiling, high_ceiling),
        "idle-ceiling-retained",
    )?;
    record_phase(context, "idle-ceiling-retained", &idle, None, phases)?;
    observations.push(format!(
        "after fresh idle samples, ceiling remained {high_ceiling:.2}; the exact pair stayed published and the Gateway pod UID stayed {original_uid}"
    ));

    let connected_before = read_connected_sites(&kind_context(GRID_SERVING_SITE))?;
    if !has_connected_swim_peer(connected_before) {
        return Err(
            format!("cannot withdraw provider while SWIM is disconnected: connectedSites={connected_before}").into(),
        );
    }
    let excluded_policy = patch_access_policy_to_exclude_run_site(original)?;
    trigger_gridnetwork_reconcile_strict(SITE)?;
    trigger_gridnetwork_reconcile_strict(GRID_SERVING_SITE)?;
    let mut expected_removed = original_candidates.clone();
    expected_removed.remove(&(SITE.to_owned(), CLUSTER.to_owned()));
    if !expected_removed.contains(&("pool-a".to_owned(), POOL_A_PROVIDER.to_owned())) {
        return Err(
            "pool-A's local provider is missing from the original topology; cannot prove a safe pool-A-only route"
                .into(),
        );
    }
    let removed = wait_for_topology_state(
        context,
        forwards,
        &original_uid,
        false,
        &expected_removed,
        &idle,
        0,
        "pair-removed-and-accepted",
        phases,
        probe_failures,
    )?;
    let connected_after = read_connected_sites(&kind_context(GRID_SERVING_SITE))?;
    if !has_connected_swim_peer(connected_after) || connected_after != connected_before {
        return Err(format!(
            "access-policy withdrawal changed SWIM membership: {connected_before} -> {connected_after}"
        )
        .into());
    }
    if !removed.ceiling_is_nan {
        return Err(format!(
            "Gateway did not clear the withdrawn pair's ceiling to NaN: {:?}",
            removed.ceiling_raw
        )
        .into());
    }
    record_phase(context, "pair-removed-and-accepted", &removed, None, phases)?;
    observations.push(format!(
        "operator published digest {} at resourceVersion {}; Gateway accepted the mounted removal of ({SITE}, {CLUSTER}), cleared its ceiling, and kept SWIM at {connected_after}; temporary policy={}",
        removed.operator_digest, removed.operator_resource_version, excluded_policy
    ));
    record_attributed_requests(
        context,
        forwards,
        "only-pool-a-eligible",
        Some("pool-a"),
        &removed,
        phases,
        observations,
        probe_failures,
    )?;

    set_simulator_fake_metrics(
        SITE,
        KV_REPORTED_ONLY_WAITING_REQUESTS,
        LOW_RUNNING_REQUESTS,
        0.0,
        context.metrics_transport == MetricsTransport::MtlsProxy,
    )?;
    let low_signal = wait_for_signal(
        context,
        forwards,
        &original_uid,
        LOW_RUNNING_REQUESTS,
        &expected_removed,
        "lower-running-signal-while-absent",
    )?;
    if !low_signal.ceiling_is_nan {
        return Err(format!(
            "the removed pair's ceiling became finite before the pair was re-added: {:?}",
            low_signal.ceiling_raw
        )
        .into());
    }
    record_phase(context, "lower-running-signal-while-absent", &low_signal, None, phases)?;

    let exclusion_ms = low_signal
        .load_window_ms
        .max(low_signal.horizon_ms)
        .max(low_signal.window_secs.saturating_mul(1000))
        .saturating_add(12_000);
    let low_window = Duration::from_millis(exclusion_ms);
    observations.push(format!(
        "waiting {exclusion_ms}ms with a fresh low-only signal window before re-adding the pair (load_window_ms={}, horizon_ms={}, window_secs={})",
        low_signal.load_window_ms, low_signal.horizon_ms, low_signal.window_secs
    ));
    let low_window_end = Instant::now() + low_window;
    let mut window_sample = 0_u32;
    while Instant::now() < low_window_end {
        let snapshot = observe_runtime(context, forwards)?;
        require_signal_values(&snapshot, f64::from(LOW_RUNNING_REQUESTS))?;
        if candidate_set(&snapshot.candidates) != expected_removed
            || candidate_set(&snapshot.mounted_candidates) != expected_removed
        {
            return Err(
                "operator or mounted Gateway configuration re-added the pair before the high sample expired".into(),
            );
        }
        if !snapshot.ceiling_is_nan {
            return Err(format!(
                "the removed pair regained a ceiling during the low-only window: {:?}",
                snapshot.ceiling_raw
            )
            .into());
        }
        if snapshot.gateway_uid != original_uid {
            return Err(format!(
                "Gateway pod UID changed while pair was absent: {original_uid} -> {}",
                snapshot.gateway_uid
            )
            .into());
        }
        let sample_name = format!("lower-only-window-{window_sample:02}");
        let probe = send_probe(forwards.gateway_http, Some("pool-a"), &sample_name, probe_failures)?;
        observations.push(format!(
            "low-only window request {} returned HTTP {} attributed to pool-a/{}",
            window_sample.saturating_add(1),
            probe.status,
            probe.backend.as_deref().unwrap_or("missing")
        ));
        record_phase(context, &sample_name, &snapshot, None, phases)?;
        window_sample = window_sample.saturating_add(1);
        let remaining = low_window_end.saturating_duration_since(Instant::now());
        std::thread::sleep(POLL.min(remaining));
    }
    if window_sample < 2 {
        return Err(format!("low-only observation window produced only {window_sample} samples").into());
    }

    let before_return = observe_runtime(context, forwards)?;
    let mut expected_returned = original_candidates.clone();
    expected_returned.insert((SITE.to_owned(), CLUSTER.to_owned()));
    patch_access_policy(original)?;
    trigger_gridnetwork_reconcile_strict(SITE)?;
    trigger_gridnetwork_reconcile_strict(GRID_SERVING_SITE)?;
    let returned = wait_for_topology_state(
        context,
        forwards,
        &original_uid,
        true,
        &expected_returned,
        &before_return,
        LOW_RUNNING_REQUESTS,
        "same-pair-returned-under-low-load",
        phases,
        probe_failures,
    )?;
    let returned_local_in_flight = returned
        .local_in_flight
        .ok_or("returned candidate has no relayed in-flight observation")?;
    let new_ceiling = returned
        .ceiling
        .ok_or("returned candidate has no finite learned ceiling")?;
    if !lifecycle_ceiling_sequence_valid(
        high_ceiling,
        idle.ceiling.unwrap_or(f64::NAN),
        removed.ceiling_raw.as_deref(),
        new_ceiling,
        returned_local_in_flight,
    ) {
        return Err(format!(
            "returned ceiling {new_ceiling:.2} did not prove the old value was forgotten: old={high_ceiling:.2}, measured low in-flight={returned_local_in_flight}, floor={DEFAULT_CEILING_FLOOR}, margin={CEILING_MARGIN}"
        )
        .into());
    }
    require_same_gateway_config(&returned)?;
    record_phase(context, "same-pair-returned-under-low-load", &returned, None, phases)?;
    observations.push(format!(
        "the exact pair returned at serving digest {} (resourceVersion={}); its new ceiling was {new_ceiling:.2}, more than {CEILING_MARGIN:.2} below old ceiling {high_ceiling:.2}",
        returned.operator_digest, returned.operator_resource_version
    ));
    record_attributed_requests(
        context,
        forwards,
        "after-return",
        None,
        &returned,
        phases,
        observations,
        probe_failures,
    )?;

    set_simulator_fake_metrics(
        SITE,
        0,
        HIGH_RUNNING_REQUESTS,
        0.0,
        context.metrics_transport == MetricsTransport::MtlsProxy,
    )?;
    let high_soft = wait_for_ceiling(
        context,
        forwards,
        &original_uid,
        HIGH_RUNNING_REQUESTS,
        |snapshot| {
            snapshot
                .gateway_capacity
                .soft_ceiling
                .is_some_and(|value| value > DEFAULT_CEILING_FLOOR + CEILING_MARGIN)
                && snapshot
                    .gateway_capacity
                    .hard_ceiling
                    .is_none_or(|value| !value.is_finite())
                && counted_saturation_is_absent(snapshot.gateway_capacity)
        },
        "kv-cache-hard-bound-soft-baseline",
    )?;
    let high_soft_ceiling = high_soft
        .gateway_capacity
        .soft_ceiling
        .filter(|value| value.is_finite())
        .ok_or("Gateway did not publish a finite queue-free soft ceiling before the KV-cache sample")?;
    record_phase(context, "kv-cache-hard-bound-soft-baseline", &high_soft, None, phases)?;
    observations.push(format!(
        "queue-free running load relearned soft ceiling {high_soft_ceiling:.2} before the saturated KV-cache sample; Gateway count-based saturation remained absent without a hard ceiling"
    ));
    let soft_baseline_distribution = record_attributed_request_batch(
        context,
        forwards,
        "kv-cache-hard-bound-soft-baseline",
        None,
        &high_soft,
        phases,
        observations,
        probe_failures,
        KV_HARD_BOUND_ROUTE_REQUESTS,
    )?;

    set_simulator_fake_metrics_with_endpoint_kv(
        SITE,
        0,
        LOW_RUNNING_REQUESTS,
        KV_HARD_BOUND_CACHE_HIGH_ENDPOINT,
        KV_HARD_BOUND_CACHE_LOW_ENDPOINT,
        context.metrics_transport == MetricsTransport::MtlsProxy,
    )?;
    observations.push(format!(
        "pool-B simulator set low running-requests={LOW_RUNNING_REQUESTS}, waiting-requests={KV_REPORTED_ONLY_WAITING_REQUESTS}, vcr-1 KV-cache={KV_HARD_BOUND_CACHE_HIGH_ENDPOINT}, vcr-2 KV-cache={KV_HARD_BOUND_CACHE_LOW_ENDPOINT}; the provider signal must route as saturated without a learned hard ceiling"
    ));
    let reported_only =
        wait_for_reported_only_kv_cache(context, forwards, &original_uid, &high_soft, high_soft_ceiling, phases)?;
    observations.push(format!(
        "pool-B EPP reported KV mean={:.3}, spread={:.3}; the live Gateway reported saturation {:.3} and combined route rho {:.3} while request-count saturation and the hard ceiling remained absent; running={}, waiting={}, in-flight={}, soft ceiling={high_soft_ceiling:.2}",
        reported_only.epp.kv_cache_utilization,
        reported_only.epp.kv_cache_spread.unwrap_or(f64::NAN),
        reported_only.gateway_capacity.reported_saturation.unwrap_or(f64::NAN),
        required_metric(
            &reported_only.gateway_metrics,
            "grid_route_site_rho",
            &[("site", SITE), ("cluster", CLUSTER)],
            "pool-B combined route saturation"
        )?,
        reported_only.epp.running,
        reported_only.epp.queue,
        reported_only.gateway_capacity.in_flight.unwrap_or(f64::NAN),
    ));
    let reported_only_distribution = record_attributed_request_batch(
        context,
        forwards,
        "kv-cache-reported-saturation-with-low-count",
        None,
        &reported_only,
        phases,
        observations,
        probe_failures,
        KV_HARD_BOUND_ROUTE_REQUESTS,
    )?;
    let (reported_only_soft_pool_b_share, reported_only_pool_b_share) = kv_cache_reported_only_route_share_shift(
        &soft_baseline_distribution,
        &reported_only_distribution,
        KV_HARD_BOUND_ROUTE_REQUESTS,
    )
    .map_err(Into::<Box<dyn std::error::Error>>::into)?;
    observations.push(format!(
        "matched {KV_HARD_BOUND_ROUTE_REQUESTS}-request samples show pool-B routing share falling from {:.1}% under low KV pressure to {:.1}% from provider-reported KV saturation alone ({:.1} percentage-point reduction)",
        reported_only_soft_pool_b_share * 100.0,
        reported_only_pool_b_share * 100.0,
        (reported_only_soft_pool_b_share - reported_only_pool_b_share) * 100.0
    ));

    set_simulator_fake_metrics_with_endpoint_kv(
        SITE,
        KV_HARD_BOUND_WAITING_REQUESTS,
        KV_HARD_BOUND_RUNNING_REQUESTS,
        KV_HARD_BOUND_CACHE_HIGH_ENDPOINT,
        KV_HARD_BOUND_CACHE_LOW_ENDPOINT,
        context.metrics_transport == MetricsTransport::MtlsProxy,
    )?;
    observations.push(format!(
        "pool-B simulator set running-requests={KV_HARD_BOUND_RUNNING_REQUESTS}, waiting-requests={KV_HARD_BOUND_WAITING_REQUESTS} per endpoint, vcr-1 KV-cache={KV_HARD_BOUND_CACHE_HIGH_ENDPOINT}, vcr-2 KV-cache={KV_HARD_BOUND_CACHE_LOW_ENDPOINT}; expected pinned EPP mean={KV_HARD_BOUND_CACHE_MEAN}, sample standard deviation={KV_HARD_BOUND_CACHE_SPREAD}, and zero held-work capacity"
    ));
    let kv_hard_bound =
        wait_for_kv_cache_hard_bound(context, forwards, &original_uid, high_soft_ceiling, probe_failures)?;
    record_phase(context, "kv-cache-hard-bound-learned", &kv_hard_bound, None, phases)?;
    let hard_ceiling = kv_hard_bound
        .gateway_capacity
        .hard_ceiling
        .ok_or("Gateway did not publish a finite hard ceiling for the saturated KV-cache sample")?;
    observations.push(format!(
        "Gateway reported KV saturation {:.3} and learned hard ceiling {hard_ceiling:.2} below soft ceiling {high_soft_ceiling:.2}; EPP running={}, waiting={}, ready={}, held-capacity={}",
        kv_hard_bound.gateway_capacity.reported_saturation.unwrap_or(f64::NAN),
        kv_hard_bound.epp.running,
        kv_hard_bound.epp.queue,
        kv_hard_bound.epp.ready,
        kv_hard_bound.epp.held_capacity_utilization
    ));
    let hard_bound_distribution = record_attributed_request_batch(
        context,
        forwards,
        "kv-cache-hard-bound-learned",
        None,
        &kv_hard_bound,
        phases,
        observations,
        probe_failures,
        KV_HARD_BOUND_ROUTE_REQUESTS,
    )?;
    let (hard_bound_soft_pool_b_share, hard_pool_b_share) = kv_cache_route_share_shift(
        &soft_baseline_distribution,
        &hard_bound_distribution,
        KV_HARD_BOUND_ROUTE_REQUESTS,
    )
    .map_err(Into::<Box<dyn std::error::Error>>::into)?;
    observations.push(format!(
        "matched {KV_HARD_BOUND_ROUTE_REQUESTS}-request samples show pool-B routing share falling from {:.1}% under the queue-free soft ceiling to {:.1}% under the KV-cache hard bound ({:.1} percentage-point reduction)",
        hard_bound_soft_pool_b_share * 100.0,
        hard_pool_b_share * 100.0,
        (hard_bound_soft_pool_b_share - hard_pool_b_share) * 100.0
    ));

    let resized = wait_for_endpoint_shrink_retirement(
        context,
        forwards,
        &original_uid,
        &original.simulator_replicas,
        &kv_hard_bound,
        high_soft_ceiling,
        hard_ceiling,
        phases,
        observations,
    )?;
    record_phase(
        context,
        "kv-cache-bounds-retired-after-endpoint-shrink",
        &resized,
        None,
        phases,
    )?;

    if returned.gateway_uid != original_uid {
        return Err(format!(
            "Gateway pod UID changed during the lifecycle: {original_uid} -> {}",
            returned.gateway_uid
        )
        .into());
    }
    if candidate_set(&returned.candidates) != original_candidates {
        return Err("returned serving topology does not match the original candidate set".into());
    }
    if baseline_record
        .pointer("/operator_revision/content_digest")
        .and_then(Value::as_str)
        != Some(returned.operator_digest.as_str())
    {
        observations.push(
            "restored pair uses a new content-addressed serving revision after the intervening removal".to_owned(),
        );
    }
    run_hard_bound_without_soft(
        context,
        forwards,
        original,
        &original_uid,
        &original_candidates,
        &resized,
        phases,
        observations,
        probe_failures,
    )?;
    Ok(())
}

/// Prove that a re-added saturated pair learns a hard bound without a queue-free soft sample.
#[expect(
    clippy::too_many_arguments,
    reason = "this deployed phase ties topology removal, saturated EPP signals, Gateway evidence, and restoration together"
)]
fn run_hard_bound_without_soft(
    context: &DemoContext,
    forwards: &PortForwards,
    original: &OriginalState,
    gateway_uid: &str,
    original_candidates: &BTreeSet<CandidatePair>,
    current: &RuntimeSnapshot,
    phases: &mut Vec<Value>,
    observations: &mut Vec<String>,
    probe_failures: &mut Vec<Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    if (current.epp.ready - 1.0).abs() > f64::EPSILON
        || current.gateway_capacity.soft_ceiling.is_some_and(f64::is_finite)
    {
        return Err(format!(
            "hard-bound-without-soft phase requires the endpoint-shrunk state with one ready endpoint and no finite soft ceiling: ready={}, soft={:?}",
            current.epp.ready, current.gateway_capacity.soft_ceiling
        )
        .into());
    }
    let excluded_policy = patch_access_policy_to_exclude_run_site(original)?;
    trigger_gridnetwork_reconcile_strict(SITE)?;
    trigger_gridnetwork_reconcile_strict(GRID_SERVING_SITE)?;
    let mut expected_removed = original_candidates.clone();
    expected_removed.remove(&(SITE.to_owned(), CLUSTER.to_owned()));
    let removed =
        wait_for_pair_removal_and_bound_clear(context, forwards, gateway_uid, &expected_removed, current, phases)?;
    observations.push(format!(
        "before the hard-only sample, the exact pair was removed at operator revision {} / resourceVersion {}; the mounted Gateway accepted it and cleared soft, hard, and effective bounds; temporary policy={excluded_policy}",
        removed.operator_digest, removed.operator_resource_version
    ));

    set_simulator_fake_metrics(
        SITE,
        KV_HARD_BOUND_WAITING_REQUESTS,
        KV_HARD_BOUND_RUNNING_REQUESTS,
        KV_SHRUNK_CACHE_MEAN,
        context.metrics_transport == MetricsTransport::MtlsProxy,
    )?;
    let saturated_absent = wait_for_saturated_signal_while_pair_absent(
        context,
        forwards,
        gateway_uid,
        &expected_removed,
        &removed,
        phases,
    )?;
    observations.push(format!(
        "while ({SITE}, {CLUSTER}) was absent, the single ready EPP endpoint reported running={}, waiting={}, KV mean={}, and zero held work; origin and local operator signal evidence matched, and no Gateway ceiling existed",
        saturated_absent.epp.running,
        saturated_absent.epp.queue,
        saturated_absent.epp.kv_cache_utilization
    ));

    patch_access_policy(original)?;
    trigger_gridnetwork_reconcile_strict(SITE)?;
    trigger_gridnetwork_reconcile_strict(GRID_SERVING_SITE)?;
    let learned = wait_for_hard_bound_without_soft(
        context,
        forwards,
        gateway_uid,
        original_candidates,
        &saturated_absent,
        phases,
    )?;
    let hard = learned
        .gateway_capacity
        .hard_ceiling
        .filter(|value| value.is_finite())
        .ok_or("Gateway did not learn a hard ceiling without a queue-free soft sample")?;
    observations.push(format!(
        "re-adding ({SITE}, {CLUSTER}) under sustained full-cache backlog learned hard/effective ceiling {hard:.2} from in-flight={}, while the soft ceiling stayed absent; no queue-free signal occurred after the pair was re-added",
        learned.gateway_capacity.in_flight.unwrap_or(f64::NAN)
    ));
    record_attributed_requests(
        context,
        forwards,
        "kv-hard-bound-learned-without-soft",
        None,
        &learned,
        phases,
        observations,
        probe_failures,
    )?;
    Ok(())
}

/// Wait until the retracted candidate and every learned bound are absent from live Gateway state.
#[expect(
    clippy::too_many_arguments,
    reason = "the wait binds the exact removed topology to live Gateway and phase evidence"
)]
fn wait_for_pair_removal_and_bound_clear(
    context: &DemoContext,
    forwards: &PortForwards,
    gateway_uid: &str,
    expected_candidates: &BTreeSet<CandidatePair>,
    previous: &RuntimeSnapshot,
    phases: &mut Vec<Value>,
) -> Result<RuntimeSnapshot, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + TRANSITION_TIMEOUT;
    let mut stable_since = None;
    let mut stable_samples = 0_u32;
    let mut last = String::from("no runtime sample");
    while Instant::now() < deadline {
        match observe_runtime(context, forwards) {
            Ok(snapshot) => {
                last = snapshot_summary(&snapshot);
                if snapshot.gateway_uid != gateway_uid {
                    return Err(format!(
                        "Gateway pod UID changed while clearing learned bounds: {gateway_uid} -> {}",
                        snapshot.gateway_uid
                    )
                    .into());
                }
                let accepted_revision = candidate_set(&snapshot.candidates) == *expected_candidates
                    && candidate_set(&snapshot.mounted_candidates) == *expected_candidates
                    && snapshot.operator_digest != previous.operator_digest
                    && snapshot.operator_resource_version != previous.operator_resource_version
                    && snapshot.operator_digest_annotation == snapshot.operator_digest
                    && snapshot.mounted_digest == snapshot.operator_digest
                    && snapshot.gateway_reloads > previous.gateway_reloads;
                let bounds_cleared = snapshot.ceiling_is_nan && capacity_metrics_are_cleared(snapshot.gateway_capacity);
                let signal_state = require_hard_only_kv_signal_values(&snapshot, false);
                if accepted_revision && bounds_cleared && signal_state.is_ok() {
                    stable_since.get_or_insert_with(Instant::now);
                    stable_samples = stable_samples.saturating_add(1);
                    if stable_samples >= 3 && stable_since.is_some_and(|start| start.elapsed() >= STABLE_SPAN) {
                        record_phase(
                            context,
                            "kv-hard-bound-pair-removed-and-cleared",
                            &snapshot,
                            None,
                            phases,
                        )?;
                        return Ok(snapshot);
                    }
                } else {
                    last = signal_state.err().map_or_else(
                        || {
                            format!(
                                "candidate removal, accepted revision, or cleared bounds are incomplete: {}",
                                snapshot_summary(&snapshot)
                            )
                        },
                        |error| error.to_string(),
                    );
                    stable_since = None;
                    stable_samples = 0;
                }
            },
            Err(error) => {
                last = error.to_string();
                stable_since = None;
                stable_samples = 0;
            },
        }
        std::thread::sleep(POLL);
    }
    Err(format!("timed out waiting for the pair removal to clear all learned bounds; last observation: {last}").into())
}

/// Require a saturated EPP sample to remain published while its exact provider pair is absent.
#[expect(
    clippy::too_many_arguments,
    reason = "the wait binds the absent topology to the saturated origin and relay signal"
)]
fn wait_for_saturated_signal_while_pair_absent(
    context: &DemoContext,
    forwards: &PortForwards,
    gateway_uid: &str,
    expected_candidates: &BTreeSet<CandidatePair>,
    previous: &RuntimeSnapshot,
    phases: &mut Vec<Value>,
) -> Result<RuntimeSnapshot, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + TRANSITION_TIMEOUT;
    let mut stable_since = None;
    let mut stable_samples = 0_u32;
    let mut last = String::from("no runtime sample");
    while Instant::now() < deadline {
        match observe_runtime(context, forwards) {
            Ok(snapshot) => {
                last = snapshot_summary(&snapshot);
                if snapshot.gateway_uid != gateway_uid {
                    return Err(format!(
                        "Gateway pod UID changed while the saturated signal was published without its pair: {gateway_uid} -> {}",
                        snapshot.gateway_uid
                    )
                    .into());
                }
                let topology_is_absent = candidate_set(&snapshot.candidates) == *expected_candidates
                    && candidate_set(&snapshot.mounted_candidates) == *expected_candidates
                    && snapshot.operator_digest == previous.operator_digest
                    && snapshot.operator_digest_annotation == snapshot.operator_digest
                    && snapshot.mounted_digest == snapshot.operator_digest;
                let bounds_cleared = snapshot.ceiling_is_nan && capacity_metrics_are_cleared(snapshot.gateway_capacity);
                let signal_state = require_hard_only_kv_signal_values(&snapshot, false);
                if topology_is_absent && bounds_cleared && signal_state.is_ok() {
                    stable_since.get_or_insert_with(Instant::now);
                    stable_samples = stable_samples.saturating_add(1);
                    if stable_samples >= 3 && stable_since.is_some_and(|start| start.elapsed() >= STABLE_SPAN) {
                        record_phase(
                            context,
                            "kv-hard-bound-saturated-signal-while-absent",
                            &snapshot,
                            None,
                            phases,
                        )?;
                        return Ok(snapshot);
                    }
                } else {
                    last = signal_state.err().map_or_else(
                        || {
                            format!(
                                "the absent-pair topology, cleared bounds, or stable signal is incomplete: {}",
                                snapshot_summary(&snapshot)
                            )
                        },
                        |error| error.to_string(),
                    );
                    stable_since = None;
                    stable_samples = 0;
                }
            },
            Err(error) => {
                last = error.to_string();
                stable_since = None;
                stable_samples = 0;
            },
        }
        std::thread::sleep(POLL);
    }
    Err(format!(
        "timed out verifying the saturated signal while the provider pair was absent; last observation: {last}"
    )
    .into())
}

/// Wait for the restored pair to learn and serve a hard bound with no finite soft ceiling.
#[expect(
    clippy::too_many_arguments,
    reason = "the wait binds the restored serving revision to EPP, relay, and hard-bound evidence"
)]
fn wait_for_hard_bound_without_soft(
    context: &DemoContext,
    forwards: &PortForwards,
    gateway_uid: &str,
    expected_candidates: &BTreeSet<CandidatePair>,
    previous: &RuntimeSnapshot,
    phases: &mut Vec<Value>,
) -> Result<RuntimeSnapshot, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + TRANSITION_TIMEOUT;
    let mut stable_since = None;
    let mut stable_samples = 0_u32;
    let mut last = String::from("no runtime sample");
    while Instant::now() < deadline {
        match observe_runtime(context, forwards) {
            Ok(snapshot) => {
                last = snapshot_summary(&snapshot);
                if snapshot.gateway_uid != gateway_uid {
                    return Err(format!(
                        "Gateway pod UID changed while learning a hard bound without a soft sample: {gateway_uid} -> {}",
                        snapshot.gateway_uid
                    )
                    .into());
                }
                let accepted_revision = candidate_set(&snapshot.candidates) == *expected_candidates
                    && candidate_set(&snapshot.mounted_candidates) == *expected_candidates
                    && snapshot.operator_digest != previous.operator_digest
                    && snapshot.operator_resource_version != previous.operator_resource_version
                    && snapshot.operator_digest_annotation == snapshot.operator_digest
                    && snapshot.mounted_digest == snapshot.operator_digest
                    && snapshot.gateway_reloads > previous.gateway_reloads;
                let valid = require_hard_only_kv_signal_values(&snapshot, true).and_then(|expected_in_flight| {
                    hard_bound_without_soft_is_valid(snapshot.gateway_capacity, snapshot.ceiling, expected_in_flight)
                        .map_err(Into::<Box<dyn std::error::Error>>::into)
                });
                if accepted_revision && valid.is_ok() {
                    stable_since.get_or_insert_with(Instant::now);
                    stable_samples = stable_samples.saturating_add(1);
                    if stable_samples >= 3 && stable_since.is_some_and(|start| start.elapsed() >= STABLE_SPAN) {
                        record_phase(context, "kv-hard-bound-learned-without-soft", &snapshot, None, phases)?;
                        return Ok(snapshot);
                    }
                } else {
                    last = valid.err().map_or_else(
                        || {
                            format!(
                                "new serving revision or hard-only Gateway bounds are incomplete: {}",
                                snapshot_summary(&snapshot)
                            )
                        },
                        |error| error.to_string(),
                    );
                    stable_since = None;
                    stable_samples = 0;
                }
            },
            Err(error) => {
                last = error.to_string();
                stable_since = None;
                stable_samples = 0;
            },
        }
        std::thread::sleep(POLL);
    }
    Err(format!("timed out waiting for a hard bound without a soft sample; last observation: {last}").into())
}

/// Require the exact one-endpoint saturated stimulus and match its origin and relayed signals.
fn require_hard_only_kv_signal_values(
    snapshot: &RuntimeSnapshot,
    pair_published: bool,
) -> Result<f64, Box<dyn std::error::Error>> {
    let expected_in_flight = hard_only_epp_in_flight(&snapshot.epp)?;
    if (snapshot.origin_in_flight - expected_in_flight).abs() > 0.01
        || (snapshot.origin_ready - snapshot.epp.ready).abs() > f64::EPSILON
        || !local_relay_matches(
            pair_published,
            snapshot.local_in_flight,
            snapshot.local_ready,
            expected_in_flight,
            snapshot.epp.ready,
        )
        .map_err(Into::<Box<dyn std::error::Error>>::into)?
    {
        return Err(format!(
            "saturated EPP signal does not match pool-B origin and pool-A relay: expected in-flight={expected_in_flight}, ready={}, origin=({}, {}), relay=({:?}, {:?})",
            snapshot.epp.ready,
            snapshot.origin_in_flight,
            snapshot.origin_ready,
            snapshot.local_in_flight,
            snapshot.local_ready
        )
        .into());
    }
    Ok(expected_in_flight)
}

/// Validate one ready endpoint's saturated KV-cache and backed-up request sample.
fn hard_only_epp_in_flight(epp: &EppReadings) -> Result<f64, Box<dyn std::error::Error>> {
    if (epp.running - f64::from(KV_HARD_BOUND_RUNNING_REQUESTS)).abs() > f64::EPSILON
        || (epp.queue - f64::from(KV_HARD_BOUND_WAITING_REQUESTS)).abs() > f64::EPSILON
        || (epp.kv_cache_utilization - KV_SHRUNK_CACHE_MEAN).abs() > KV_CACHE_SIGNAL_TOLERANCE
        || epp
            .kv_cache_spread
            .is_some_and(|spread| !spread.is_finite() || spread.abs() > KV_CACHE_SIGNAL_TOLERANCE)
        || (epp.ready - 1.0).abs() > f64::EPSILON
        || epp.per_endpoint_queues.len() != 1
        || epp
            .per_endpoint_queues
            .first()
            .is_none_or(|queue| (*queue - f64::from(KV_HARD_BOUND_WAITING_REQUESTS)).abs() > f64::EPSILON)
        || !epp.held_queue_count.is_none_or(|held| held == 0.0)
        || epp.held_capacity_utilization != 0.0
    {
        return Err(format!(
            "the no-soft stimulus must be one ready endpoint with running={}, waiting={}, KV mean={}, no nonzero spread, and zero held work; observed running={}, waiting={}, KV mean={}, spread={:?}, ready={}, per-endpoint queues={:?}, held queue={:?}, held capacity={}",
            KV_HARD_BOUND_RUNNING_REQUESTS,
            KV_HARD_BOUND_WAITING_REQUESTS,
            KV_SHRUNK_CACHE_MEAN,
            epp.running,
            epp.queue,
            epp.kv_cache_utilization,
            epp.kv_cache_spread,
            epp.ready,
            epp.per_endpoint_queues,
            epp.held_queue_count,
            epp.held_capacity_utilization
        )
        .into());
    }
    Ok((epp.running + epp.queue) * epp.ready + epp.held_queue_count.unwrap_or(0.0))
}

/// Validate a hard ceiling learned from the saturated sample without a finite soft ceiling.
fn hard_bound_without_soft_is_valid(
    gateway: GatewayCapacityMetrics,
    ceiling: Option<f64>,
    expected_in_flight: f64,
) -> Result<(), String> {
    if gateway.soft_ceiling.is_some_and(f64::is_finite) {
        return Err(format!(
            "a queue-free soft ceiling was learned despite the saturated-only stimulus: {:?}",
            gateway.soft_ceiling
        ));
    }
    let reported = gateway
        .reported_saturation
        .filter(|value| value.is_finite())
        .ok_or("Gateway did not report a finite KV-cache saturation without a soft ceiling")?;
    if (reported - KV_HARD_BOUND_CACHE_SATURATION).abs() > KV_CACHE_SIGNAL_TOLERANCE {
        return Err(format!(
            "Gateway-reported saturation {reported:.3} does not match the one-endpoint full KV-cache sample"
        ));
    }
    let in_flight = gateway
        .in_flight
        .filter(|value| value.is_finite())
        .ok_or("Gateway did not publish in-flight requests for the saturated-only pair")?;
    if (in_flight - expected_in_flight).abs() > 0.01 {
        return Err(format!(
            "Gateway in-flight {in_flight:.2} does not match saturated EPP/origin/relay count {expected_in_flight:.2}"
        ));
    }
    let hard = gateway
        .hard_ceiling
        .filter(|value| value.is_finite())
        .ok_or("Gateway did not learn a hard ceiling without a queue-free soft sample")?;
    if (hard - expected_in_flight).abs() > 0.01 {
        return Err(format!(
            "Gateway hard ceiling {hard:.2} does not match saturated in-flight count {expected_in_flight:.2}"
        ));
    }
    let effective = ceiling
        .filter(|value| value.is_finite())
        .ok_or("Gateway did not use the hard ceiling as its effective ceiling without a soft sample")?;
    if (effective - hard).abs() > 0.01 {
        return Err(format!(
            "effective ceiling {effective:.2} does not use the hard-only ceiling {hard:.2}"
        ));
    }
    let counted = gateway
        .counted_saturation
        .filter(|value| value.is_finite())
        .ok_or("Gateway did not publish count-based saturation after learning the hard-only ceiling")?;
    if (counted - 1.0).abs() > 0.01 {
        return Err(format!(
            "counted saturation {counted:.3} does not match in-flight/hard-ceiling=1.000"
        ));
    }
    Ok(())
}

/// Wait for a finite ceiling while requiring current EPP and relayed signals to match.
#[expect(
    clippy::too_many_arguments,
    reason = "the wait predicate binds one proof phase to its live context and expected signal"
)]
fn wait_for_ceiling<F>(
    context: &DemoContext,
    forwards: &PortForwards,
    gateway_uid: &str,
    expected_running: u32,
    predicate: F,
    phase: &str,
) -> Result<RuntimeSnapshot, Box<dyn std::error::Error>>
where
    F: Fn(&RuntimeSnapshot) -> bool,
{
    let deadline = Instant::now() + TRANSITION_TIMEOUT;
    let mut stable_since = None;
    let mut stable_samples = 0_u32;
    let mut last = String::from("no runtime sample");
    while Instant::now() < deadline {
        match observe_runtime(context, forwards) {
            Ok(snapshot) => {
                last = snapshot_summary(&snapshot);
                if snapshot.gateway_uid != gateway_uid {
                    return Err(format!(
                        "Gateway pod UID changed during {phase}: {gateway_uid} -> {}",
                        snapshot.gateway_uid
                    )
                    .into());
                }
                let valid = require_pair(&snapshot.candidates, "operator serving config")
                    .and_then(|()| require_pair(&snapshot.mounted_candidates, "mounted Gateway config"))
                    .and_then(|()| require_same_gateway_config(&snapshot))
                    .and_then(|()| require_signal_values(&snapshot, f64::from(expected_running)));
                if valid.is_ok() && predicate(&snapshot) {
                    stable_since.get_or_insert_with(Instant::now);
                    stable_samples = stable_samples.saturating_add(1);
                    if stable_samples >= 3 && stable_since.is_some_and(|start| start.elapsed() >= STABLE_SPAN) {
                        return Ok(snapshot);
                    }
                } else {
                    last = valid.err().map_or_else(
                        || format!("ceiling predicate false: {}", snapshot_summary(&snapshot)),
                        |error| error.to_string(),
                    );
                    stable_since = None;
                    stable_samples = 0;
                }
            },
            Err(error) => {
                last = error.to_string();
                stable_since = None;
                stable_samples = 0;
            },
        }
        std::thread::sleep(POLL);
    }
    Err(format!("timed out waiting for {phase}; last observation: {last}").into())
}

/// Require the Gateway itself to observe zero in-flight load while retaining its learned ceiling.
fn idle_ceiling_is_retained(gateway_in_flight: Option<f64>, ceiling: Option<f64>, learned_ceiling: f64) -> bool {
    gateway_in_flight == Some(0.0) && ceiling.is_some_and(|value| (value - learned_ceiling).abs() < f64::EPSILON)
}

/// Require a deployed KV-cache sample to create a hard bound below a previously learned soft bound.
fn wait_for_kv_cache_hard_bound(
    context: &DemoContext,
    forwards: &PortForwards,
    gateway_uid: &str,
    high_soft_ceiling: f64,
    probe_failures: &mut Vec<Value>,
) -> Result<RuntimeSnapshot, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + TRANSITION_TIMEOUT;
    let mut stable_since = None;
    let mut stable_samples = 0_u32;
    let mut last = String::from("no runtime sample");
    while Instant::now() < deadline {
        // Keep transport failures as qualification failures so the wait does not hide underlay drops tracked in Grid
        // #370.
        let probe = send_probe(forwards.gateway_http, None, "kv-cache-hard-bound-wait", probe_failures)?;
        match observe_runtime(context, forwards) {
            Ok(snapshot) => {
                last = snapshot_summary(&snapshot);
                if snapshot.gateway_uid != gateway_uid {
                    return Err(format!(
                        "Gateway pod UID changed during the KV-cache hard-bound proof: {gateway_uid} -> {}",
                        snapshot.gateway_uid
                    )
                    .into());
                }
                let valid = require_pair(&snapshot.candidates, "operator serving config")
                    .and_then(|()| require_pair(&snapshot.mounted_candidates, "mounted Gateway config"))
                    .and_then(|()| require_same_gateway_config(&snapshot))
                    .and_then(|()| require_kv_cache_signal_values(&snapshot))
                    .and_then(|()| {
                        kv_cache_hard_bound_is_valid(
                            &snapshot.epp,
                            snapshot.gateway_capacity,
                            snapshot.ceiling,
                            high_soft_ceiling,
                        )
                        .map_err(Into::<Box<dyn std::error::Error>>::into)
                    });
                if let Err(error) = valid {
                    last = format!("{error}; {}", snapshot_summary(&snapshot));
                    stable_since = None;
                    stable_samples = 0;
                } else {
                    eprintln!(
                        "[LEARNED-CEILING] KV-cache hard-bound poll returned HTTP {} with attributed site {:?} and backend {:?}",
                        probe.status, probe.site, probe.backend
                    );
                    stable_since.get_or_insert_with(Instant::now);
                    stable_samples = stable_samples.saturating_add(1);
                    if stable_samples >= 3 && stable_since.is_some_and(|start| start.elapsed() >= STABLE_SPAN) {
                        eprintln!(
                            "[LEARNED-CEILING] KV-cache hard bound remained below the queue-free soft ceiling for {stable_samples} stable observations over at least {}s",
                            STABLE_SPAN.as_secs()
                        );
                        return Ok(snapshot);
                    }
                }
            },
            Err(error) => {
                last = error.to_string();
                stable_since = None;
                stable_samples = 0;
            },
        }
        std::thread::sleep(POLL);
    }
    Err(format!("timed out waiting for the KV-cache hard bound; last observation: {last}").into())
}

/// Wait for provider-reported KV saturation to affect routing with low in-flight count and no backlog.
#[expect(
    clippy::too_many_arguments,
    reason = "the live wait binds the low-count EPP sample, relay, Gateway bounds, and route saturation"
)]
fn wait_for_reported_only_kv_cache(
    context: &DemoContext,
    forwards: &PortForwards,
    gateway_uid: &str,
    baseline: &RuntimeSnapshot,
    high_soft_ceiling: f64,
    phases: &mut Vec<Value>,
) -> Result<RuntimeSnapshot, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + TRANSITION_TIMEOUT;
    let mut stable_since = None;
    let mut stable_samples = 0_u32;
    let mut last = String::from("no runtime sample");
    while Instant::now() < deadline {
        match observe_runtime(context, forwards) {
            Ok(snapshot) => {
                last = snapshot_summary(&snapshot);
                if snapshot.gateway_uid != gateway_uid {
                    return Err(format!(
                        "Gateway pod UID changed during the reported-only KV-cache proof: {gateway_uid} -> {}",
                        snapshot.gateway_uid
                    )
                    .into());
                }
                if snapshot.operator_digest != baseline.operator_digest
                    || snapshot.mounted_digest != baseline.mounted_digest
                    || snapshot.candidates != baseline.candidates
                    || snapshot.mounted_candidates != baseline.mounted_candidates
                {
                    return Err("serving topology changed during the reported-only KV-cache proof".into());
                }
                let valid = require_same_gateway_config(&snapshot)
                    .and_then(|()| require_reported_only_kv_cache_values(&snapshot, high_soft_ceiling));
                if valid.is_ok() {
                    stable_since.get_or_insert_with(Instant::now);
                    stable_samples = stable_samples.saturating_add(1);
                    if stable_samples >= 3 && stable_since.is_some_and(|start| start.elapsed() >= STABLE_SPAN) {
                        record_phase(
                            context,
                            "kv-cache-reported-saturation-with-low-count",
                            &snapshot,
                            None,
                            phases,
                        )?;
                        return Ok(snapshot);
                    }
                } else {
                    last = valid.err().map_or_else(
                        || {
                            format!(
                                "reported-only KV-cache requirements are incomplete: {}",
                                snapshot_summary(&snapshot)
                            )
                        },
                        |error| error.to_string(),
                    );
                    stable_since = None;
                    stable_samples = 0;
                }
            },
            Err(error) => {
                last = error.to_string();
                stable_since = None;
                stable_samples = 0;
            },
        }
        std::thread::sleep(POLL);
    }
    Err(format!(
        "timed out waiting for KV-cache saturation to drive routing without a hard bound; last observation: {last}"
    )
    .into())
}

/// Require a high provider KV reading to drive rho while count saturation and the hard bound are absent.
fn require_reported_only_kv_cache_values(
    snapshot: &RuntimeSnapshot,
    high_soft_ceiling: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    let route_rho = required_metric(
        &snapshot.gateway_metrics,
        "grid_route_site_rho",
        &[("site", SITE), ("cluster", CLUSTER)],
        "pool-B combined route saturation",
    )?;
    reported_only_kv_cache_is_valid(
        &snapshot.epp,
        snapshot.gateway_capacity,
        snapshot.ceiling,
        route_rho,
        high_soft_ceiling,
    )
    .map_err(Into::<Box<dyn std::error::Error>>::into)?;
    require_signal_values(snapshot, f64::from(LOW_RUNNING_REQUESTS))?;

    Ok(())
}

/// Shrink the EPP's ready endpoint count and require bounds for the old pool size to retire.
#[expect(
    clippy::too_many_arguments,
    reason = "the wait ties exact deployment scale, EPP signals, Gateway limits, and evidence together"
)]
fn wait_for_endpoint_shrink_retirement(
    context: &DemoContext,
    forwards: &PortForwards,
    gateway_uid: &str,
    original_replicas: &BTreeMap<String, u64>,
    hard_bound: &RuntimeSnapshot,
    old_soft_ceiling: f64,
    old_hard_ceiling: f64,
    phases: &mut Vec<Value>,
    observations: &mut Vec<String>,
) -> Result<RuntimeSnapshot, Box<dyn std::error::Error>> {
    let current_replicas = original_replicas
        .get(SHRINK_TEST_DEPLOYMENT)
        .copied()
        .ok_or_else(|| format!("no original replica count for {SHRINK_TEST_DEPLOYMENT}"))?;
    if current_replicas == 0 || hard_bound.epp.ready <= 1.0 {
        return Err(format!(
            "endpoint-shrink proof requires a ready endpoint to remove: deployment={SHRINK_TEST_DEPLOYMENT} replicas={current_replicas}, EPP ready={}",
            hard_bound.epp.ready
        )
        .into());
    }
    let reduced_replicas = current_replicas - 1;
    let expected_ready = hard_bound.epp.ready - 1.0;
    let peer_context = kind_context(SITE);
    set_simulator_deployment_replicas(&peer_context, SHRINK_TEST_DEPLOYMENT, reduced_replicas)?;
    observations.push(format!(
        "scaled run-owned {SHRINK_TEST_DEPLOYMENT} from {current_replicas} to {reduced_replicas} replicas; waiting for EPP readiness {expected_ready:.0} and retirement of soft={old_soft_ceiling:.2}/hard={old_hard_ceiling:.2} bounds"
    ));

    let shrink_window_ms = hard_bound.load_window_ms;
    if shrink_window_ms == 0 {
        return Err("Gateway load window is zero; cannot establish a sustained endpoint shrink".into());
    }
    let shrink_window = Duration::from_millis(shrink_window_ms);
    let deadline = Instant::now() + shrink_window.saturating_add(TRANSITION_TIMEOUT);
    let mut shrink_seen_at = None;
    let mut shrink_evidence_recorded = false;
    let mut stable_since = None;
    let mut stable_samples = 0_u32;
    let mut last = String::from("no post-scale EPP observation");
    while Instant::now() < deadline {
        match observe_runtime(context, forwards) {
            Ok(snapshot) => {
                last = snapshot_summary(&snapshot);
                if snapshot.gateway_uid != gateway_uid {
                    return Err(format!(
                        "Gateway pod UID changed during endpoint shrink: {gateway_uid} -> {}",
                        snapshot.gateway_uid
                    )
                    .into());
                }
                let valid = require_pair(&snapshot.candidates, "operator serving config")
                    .and_then(|()| require_pair(&snapshot.mounted_candidates, "mounted Gateway config"))
                    .and_then(|()| require_same_gateway_config(&snapshot))
                    .and_then(|()| require_resized_kv_cache_signal_values(&snapshot, expected_ready));
                match valid {
                    Ok(expected_in_flight) => {
                        let since = shrink_seen_at.get_or_insert_with(Instant::now);
                        if !shrink_evidence_recorded {
                            record_phase(context, "kv-cache-endpoint-count-reduced", &snapshot, None, phases)?;
                            observations.push(format!(
                                "after removing one simulator endpoint, EPP ready={:.0}, EPP-derived/origin/relay/Gateway in-flight={expected_in_flight:.2}; Gateway still reports soft={:?}, hard={:?}",
                                snapshot.epp.ready,
                                snapshot.gateway_capacity.soft_ceiling,
                                snapshot.gateway_capacity.hard_ceiling
                            ));
                            shrink_evidence_recorded = true;
                        }

                        let shrunk_for_window = since.elapsed() >= shrink_window;
                        let bounds_retired = shrunk_for_window
                            && snapshot
                                .gateway_capacity
                                .soft_ceiling
                                .is_none_or(|value| !value.is_finite())
                            && snapshot
                                .gateway_capacity
                                .hard_ceiling
                                .is_some_and(|value| (value - expected_in_flight).abs() <= 0.01)
                            && snapshot
                                .ceiling
                                .is_some_and(|value| (value - expected_in_flight).abs() <= 0.01)
                            && snapshot
                                .gateway_capacity
                                .reported_saturation
                                .is_some_and(|value| (value - KV_HARD_BOUND_CACHE_SATURATION).abs() <= 0.01)
                            && snapshot
                                .gateway_capacity
                                .counted_saturation
                                .is_some_and(|value| (value - 1.0).abs() <= 0.01);
                        if bounds_retired {
                            stable_since.get_or_insert_with(Instant::now);
                            stable_samples = stable_samples.saturating_add(1);
                            if stable_samples >= 3 && stable_since.is_some_and(|start| start.elapsed() >= STABLE_SPAN) {
                                observations.push(format!(
                                    "after a stable endpoint-count reduction lasting at least {}ms, the old soft ceiling was cleared and the hard/effective ceiling relearned at {expected_in_flight:.2} for ready={expected_ready:.0}; old soft={old_soft_ceiling:.2}, old hard={old_hard_ceiling:.2}",
                                    shrink_window.as_millis()
                                ));
                                return Ok(snapshot);
                            }
                        } else {
                            last = format!(
                                "endpoint count is reduced and signals agree, but old bounds have not retired; reduced for {}ms of {}ms; {}",
                                since.elapsed().as_millis(),
                                shrink_window.as_millis(),
                                snapshot_summary(&snapshot)
                            );
                            stable_since = None;
                            stable_samples = 0;
                        }
                    },
                    Err(error) => {
                        last = error.to_string();
                        stable_since = None;
                        stable_samples = 0;
                    },
                }
            },
            Err(error) => {
                last = error.to_string();
                stable_since = None;
                stable_samples = 0;
            },
        }
        std::thread::sleep(POLL);
    }
    Err(format!(
        "timed out waiting for learned soft/hard bounds to retire after reducing {SHRINK_TEST_DEPLOYMENT}; last observation: {last}"
    )
    .into())
}

/// Match the saturated EPP stimulus and relay after the ready endpoint count changes.
fn require_resized_kv_cache_signal_values(
    snapshot: &RuntimeSnapshot,
    expected_ready: f64,
) -> Result<f64, Box<dyn std::error::Error>> {
    let epp = &snapshot.epp;
    if (epp.ready - expected_ready).abs() > f64::EPSILON {
        return Err(format!(
            "EPP ready-endpoint count is {}, expected {expected_ready} after deployment scale",
            epp.ready
        )
        .into());
    }
    if (epp.running - f64::from(KV_HARD_BOUND_RUNNING_REQUESTS)).abs() > f64::EPSILON
        || (epp.queue - f64::from(KV_HARD_BOUND_WAITING_REQUESTS)).abs() > f64::EPSILON
        || (epp.kv_cache_utilization - KV_SHRUNK_CACHE_MEAN).abs() > f64::EPSILON
        || epp
            .kv_cache_spread
            .is_none_or(|spread| spread.abs() > KV_CACHE_SIGNAL_TOLERANCE)
        || epp.per_endpoint_queues.len() != usize_value(expected_ready)?
        || epp
            .per_endpoint_queues
            .iter()
            .any(|queue| (*queue - f64::from(KV_HARD_BOUND_WAITING_REQUESTS)).abs() > f64::EPSILON)
        || !epp.held_queue_count.is_none_or(|held| held == 0.0)
        || epp.held_capacity_utilization != 0.0
    {
        return Err(format!(
            "EPP no longer matches the KV-cache shrink stimulus: running={}, waiting={}, KV mean={}, spread={:?}, ready={}, per-endpoint queues={:?}, held queue={:?}, held capacity={}",
            epp.running,
            epp.queue,
            epp.kv_cache_utilization,
            epp.kv_cache_spread,
            epp.ready,
            epp.per_endpoint_queues,
            epp.held_queue_count,
            epp.held_capacity_utilization
        )
        .into());
    }
    let expected_in_flight = (epp.running + epp.queue) * epp.ready + epp.held_queue_count.unwrap_or(0.0);
    if (snapshot.origin_in_flight - expected_in_flight).abs() > 0.01
        || (snapshot.origin_ready - expected_ready).abs() > f64::EPSILON
        || !local_relay_matches(
            pair_present(&snapshot.candidates),
            snapshot.local_in_flight,
            snapshot.local_ready,
            expected_in_flight,
            expected_ready,
        )
        .map_err(Into::<Box<dyn std::error::Error>>::into)?
    {
        return Err(format!(
            "EPP shrink stimulus does not match the pool-B origin and pool-A relay: expected in-flight={expected_in_flight}, ready={expected_ready}, origin=({}, {}), relay=({:?}, {:?})",
            snapshot.origin_in_flight,
            snapshot.origin_ready,
            snapshot.local_in_flight,
            snapshot.local_ready
        )
        .into());
    }
    let gateway_in_flight = snapshot
        .gateway_capacity
        .in_flight
        .filter(|value| value.is_finite())
        .ok_or("Gateway has no finite in-flight sample after endpoint shrink")?;
    if (gateway_in_flight - expected_in_flight).abs() > 0.01 {
        return Err(format!(
            "Gateway in-flight sample {gateway_in_flight} does not match EPP-derived {expected_in_flight} after endpoint shrink"
        )
        .into());
    }
    Ok(expected_in_flight)
}

/// Match the exact deterministic high-cache, backlogged, and zero-held EPP stimulus at both operators.
fn require_kv_cache_signal_values(snapshot: &RuntimeSnapshot) -> Result<(), Box<dyn std::error::Error>> {
    let epp = &snapshot.epp;
    let expected_origin = expected_kv_cache_origin_in_flight(epp, KV_HARD_BOUND_WAITING_REQUESTS)?;
    if (snapshot.origin_in_flight - expected_origin).abs() > 0.01 {
        return Err(format!(
            "pool-B origin in-flight sample {} does not match EPP-derived {expected_origin}",
            snapshot.origin_in_flight
        )
        .into());
    }
    if (snapshot.origin_ready - epp.ready).abs() > f64::EPSILON {
        return Err(format!(
            "pool-B origin readiness {} does not match EPP {}",
            snapshot.origin_ready, epp.ready
        )
        .into());
    }
    let local_signal = local_relay_matches(
        pair_present(&snapshot.candidates),
        snapshot.local_in_flight,
        snapshot.local_ready,
        expected_origin,
        epp.ready,
    )
    .map_err(Into::<Box<dyn std::error::Error>>::into)?;
    if !local_signal {
        return Err(format!(
            "pool-A relay differs from the direct EPP and origin signals: expected in-flight={expected_origin}, ready={}, got in-flight={:?}, ready={:?}",
            epp.ready, snapshot.local_in_flight, snapshot.local_ready
        )
        .into());
    }
    Ok(())
}

/// Require the pinned EPP's queue and per-endpoint samples to match the test stimulus.
fn expected_kv_cache_origin_in_flight(
    epp: &EppReadings,
    expected_waiting_requests: u32,
) -> Result<f64, Box<dyn std::error::Error>> {
    if (epp.running - f64::from(KV_HARD_BOUND_RUNNING_REQUESTS)).abs() > f64::EPSILON {
        return Err(format!(
            "EPP running requests are {}, expected {}",
            epp.running, KV_HARD_BOUND_RUNNING_REQUESTS
        )
        .into());
    }
    if (epp.queue - f64::from(expected_waiting_requests)).abs() > f64::EPSILON {
        return Err(format!(
            "EPP average queue is {}, expected {}",
            epp.queue, expected_waiting_requests
        )
        .into());
    }
    if (epp.ready - 2.0).abs() > f64::EPSILON
        || (epp.kv_cache_utilization - KV_HARD_BOUND_CACHE_MEAN).abs() > KV_CACHE_SIGNAL_TOLERANCE
    {
        return Err(format!(
            "two-endpoint EPP KV-cache mean is {}, expected {} with two ready endpoints",
            epp.kv_cache_utilization, KV_HARD_BOUND_CACHE_MEAN
        )
        .into());
    }
    let spread = epp
        .kv_cache_spread
        .ok_or("pinned EPP did not expose KV-cache standard deviation for the asymmetric stimulus")?;
    if (spread - KV_HARD_BOUND_CACHE_SPREAD).abs() > KV_CACHE_SIGNAL_TOLERANCE {
        return Err(format!(
            "EPP KV-cache spread is {spread}, expected {KV_HARD_BOUND_CACHE_SPREAD} from endpoint values {KV_HARD_BOUND_CACHE_HIGH_ENDPOINT} and {KV_HARD_BOUND_CACHE_LOW_ENDPOINT}"
        )
        .into());
    }
    if epp.ready < 1.0 || epp.ready.fract() != 0.0 {
        return Err(format!("EPP ready-endpoint count is not a positive integer: {}", epp.ready).into());
    }
    if epp.per_endpoint_queues.len() != usize_value(epp.ready)? {
        return Err(format!(
            "EPP reports {} ready endpoints but {} per-endpoint queue series",
            epp.ready,
            epp.per_endpoint_queues.len()
        )
        .into());
    }
    if let Some((endpoint, queue)) = epp
        .per_endpoint_queues
        .iter()
        .enumerate()
        .find(|(_, queue)| (**queue - f64::from(expected_waiting_requests)).abs() > f64::EPSILON)
    {
        return Err(format!("EPP endpoint {endpoint} queue is {queue}, expected {expected_waiting_requests}").into());
    }
    if epp.held_queue_count.is_some_and(|held| held.abs() > f64::EPSILON)
        || epp.held_capacity_utilization.abs() > f64::EPSILON
    {
        return Err(format!(
            "EPP held work is nonzero: queue={:?}, capacity_utilization={}",
            epp.held_queue_count, epp.held_capacity_utilization
        )
        .into());
    }

    Ok((epp.running + epp.queue) * epp.ready + epp.held_queue_count.unwrap_or(0.0))
}

/// Require the live Gateway to publish the KV-derived limit and use it as the lower bound.
fn kv_cache_hard_bound_is_valid(
    epp: &EppReadings,
    gateway: GatewayCapacityMetrics,
    ceiling: Option<f64>,
    high_soft_ceiling: f64,
) -> Result<(), String> {
    let spread = epp
        .kv_cache_spread
        .ok_or("pinned EPP did not expose KV-cache spread for the Gateway saturation check")?;
    let reported = (epp.kv_cache_utilization + spread).clamp(0.0, 1.0);
    let actual_reported = gateway
        .reported_saturation
        .filter(|value| value.is_finite())
        .ok_or("Gateway has no finite provider-reported saturation for the pool-B pair")?;
    if (actual_reported - reported).abs() > 0.01 || actual_reported < 0.8 {
        return Err(format!(
            "Gateway reported saturation {actual_reported:.3} does not match EPP KV mean-plus-spread {reported:.3} or is below the full threshold"
        ));
    }

    let expected_in_flight =
        expected_kv_cache_origin_in_flight(epp, KV_HARD_BOUND_WAITING_REQUESTS).map_err(|error| error.to_string())?;
    let actual_in_flight = gateway
        .in_flight
        .filter(|value| value.is_finite())
        .ok_or("Gateway has no finite in-flight count for the pool-B pair")?;
    if (actual_in_flight - expected_in_flight).abs() > 0.01 {
        return Err(format!(
            "Gateway in-flight count {actual_in_flight:.2} does not match per-endpoint EPP count {expected_in_flight:.2}"
        ));
    }

    let hard = gateway
        .hard_ceiling
        .filter(|value| value.is_finite())
        .ok_or("Gateway did not learn a finite hard ceiling from the saturated EPP sample")?;
    if (hard - expected_in_flight).abs() > 0.01 {
        return Err(format!(
            "Gateway hard ceiling {hard:.2} does not match the saturated in-flight count {expected_in_flight:.2}"
        ));
    }

    let soft = gateway
        .soft_ceiling
        .filter(|value| value.is_finite())
        .ok_or("Gateway did not preserve a finite queue-free soft ceiling")?;
    if (soft - high_soft_ceiling).abs() > 0.01 {
        return Err(format!(
            "queue-free soft ceiling changed during the KV-cache sample: expected {high_soft_ceiling:.2}, got {soft:.2}"
        ));
    }
    if soft - hard < CEILING_MARGIN {
        return Err(format!(
            "hard ceiling {hard:.2} was not at least {CEILING_MARGIN:.2} below the soft ceiling {soft:.2}"
        ));
    }

    let actual_ceiling = ceiling
        .filter(|value| value.is_finite())
        .ok_or("Gateway has no finite effective ceiling for the pool-B pair")?;
    if (actual_ceiling - hard).abs() > 0.01 {
        return Err(format!(
            "effective ceiling {actual_ceiling:.2} does not use the lower hard ceiling {hard:.2}"
        ));
    }
    let expected_counted = (expected_in_flight / hard).clamp(0.0, 1.0);
    let actual_counted = gateway
        .counted_saturation
        .filter(|value| value.is_finite())
        .ok_or("Gateway has no finite count-based saturation after learning the hard ceiling")?;
    if (actual_counted - expected_counted).abs() > 0.01 {
        return Err(format!(
            "Gateway count-based saturation {actual_counted:.3} does not match in-flight/hard-ceiling {expected_counted:.3}"
        ));
    }
    Ok(())
}

/// Count-based saturation is meaningful only after the Gateway has learned a hard ceiling.
fn counted_saturation_is_absent(gateway: GatewayCapacityMetrics) -> bool {
    gateway.counted_saturation.is_none_or(|value| !value.is_finite())
}

/// Whether every capacity and saturation gauge for a withdrawn candidate is cleared.
fn capacity_metrics_are_cleared(gateway: GatewayCapacityMetrics) -> bool {
    [
        gateway.in_flight,
        gateway.soft_ceiling,
        gateway.hard_ceiling,
        gateway.reported_saturation,
        gateway.counted_saturation,
    ]
    .into_iter()
    .all(|value| value.is_none_or(|value| !value.is_finite()))
}

/// Wait for lower load to reach both operators while the exact pair remains absent.
#[expect(
    clippy::too_many_arguments,
    reason = "the wait binds one absent-topology phase to its expected signal and candidates"
)]
fn wait_for_signal(
    context: &DemoContext,
    forwards: &PortForwards,
    gateway_uid: &str,
    expected_running: u32,
    expected_candidates: &BTreeSet<CandidatePair>,
    phase: &str,
) -> Result<RuntimeSnapshot, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + TRANSITION_TIMEOUT;
    let mut stable_since = None;
    let mut stable_samples = 0_u32;
    let mut last = String::from("no runtime sample");
    while Instant::now() < deadline {
        match observe_runtime(context, forwards) {
            Ok(snapshot) => {
                last = snapshot_summary(&snapshot);
                if snapshot.gateway_uid != gateway_uid {
                    return Err(format!(
                        "Gateway pod UID changed during {phase}: {gateway_uid} -> {}",
                        snapshot.gateway_uid
                    )
                    .into());
                }
                let valid = if candidate_set(&snapshot.candidates) != *expected_candidates
                    || candidate_set(&snapshot.mounted_candidates) != *expected_candidates
                {
                    Err("operator or mounted candidate set differs from the required absent-pair set".to_owned())
                } else if !snapshot.ceiling_is_nan {
                    Err(format!("removed pair ceiling is not NaN: {:?}", snapshot.ceiling_raw))
                } else {
                    require_signal_values(&snapshot, f64::from(expected_running)).map_err(|error| error.to_string())
                };
                if valid.is_ok() {
                    stable_since.get_or_insert_with(Instant::now);
                    stable_samples = stable_samples.saturating_add(1);
                    if stable_samples >= 3 && stable_since.is_some_and(|start| start.elapsed() >= STABLE_SPAN) {
                        return Ok(snapshot);
                    }
                } else {
                    last = valid.err().unwrap_or_else(|| "signal predicate false".to_owned());
                    stable_since = None;
                    stable_samples = 0;
                }
            },
            Err(error) => {
                last = error.to_string();
                stable_since = None;
                stable_samples = 0;
            },
        }
        std::thread::sleep(POLL);
    }
    Err(format!("timed out waiting for {phase}; last observation: {last}").into())
}

/// Wait for a fresh operator serving revision and the Gateway's observation of it.
#[expect(
    clippy::too_many_arguments,
    reason = "the transition wait checks the prior revision, exact topology, signal, and live Gateway"
)]
fn wait_for_topology_state(
    context: &DemoContext,
    forwards: &PortForwards,
    gateway_uid: &str,
    pair_present_expected: bool,
    expected_candidates: &BTreeSet<CandidatePair>,
    previous: &RuntimeSnapshot,
    expected_running: u32,
    phase: &str,
    phases: &mut Vec<Value>,
    probe_failures: &mut Vec<Value>,
) -> Result<RuntimeSnapshot, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + TRANSITION_TIMEOUT;
    let mut stable_since = None;
    let mut stable_samples = 0_u32;
    let mut last = String::from("no runtime sample");
    while Instant::now() < deadline {
        match observe_runtime(context, forwards) {
            Ok(snapshot) => {
                last = snapshot_summary(&snapshot);
                if snapshot.gateway_uid != gateway_uid {
                    return Err(format!(
                        "Gateway pod UID changed during {phase}: {gateway_uid} -> {}",
                        snapshot.gateway_uid
                    )
                    .into());
                }
                let pairs = candidate_set(&snapshot.candidates);
                let mounted_pairs = candidate_set(&snapshot.mounted_candidates);
                let accepted_revision = pair_present(&snapshot.candidates) == pair_present_expected
                    && pair_present(&snapshot.mounted_candidates) == pair_present_expected
                    && pairs == *expected_candidates
                    && mounted_pairs == *expected_candidates
                    && snapshot.operator_digest != previous.operator_digest
                    && snapshot.operator_resource_version != previous.operator_resource_version
                    && snapshot.operator_digest_annotation == snapshot.operator_digest
                    && snapshot.mounted_digest == snapshot.operator_digest
                    && snapshot.gateway_reloads > previous.gateway_reloads;
                let signals_valid = require_signal_values(&snapshot, f64::from(expected_running));
                if accepted_revision && signals_valid.is_ok() {
                    let expected_site = (!pair_present_expected).then_some("pool-a");
                    let response = send_probe(forwards.gateway_http, expected_site, phase, probe_failures)?;
                    observations_for_topology_probe(phase, &response);
                    let reloaded = observe_runtime(context, forwards)?;
                    let response_evidence = response_value(&response);
                    record_phase(
                        context,
                        &format!("{phase}-accepted-probe"),
                        &reloaded,
                        Some(&response_evidence),
                        phases,
                    )?;
                    let still_state = reloaded.gateway_uid == gateway_uid
                        && require_topology_transition(previous, &reloaded, pair_present_expected, expected_candidates)
                        && require_signal_values(&reloaded, f64::from(expected_running)).is_ok();
                    if still_state {
                        stable_since.get_or_insert_with(Instant::now);
                        stable_samples = stable_samples.saturating_add(1);
                        if stable_samples >= 3 && stable_since.is_some_and(|start| start.elapsed() >= STABLE_SPAN) {
                            return Ok(reloaded);
                        }
                    } else {
                        last = format!(
                            "Gateway state changed after an accepted-revision request: {}",
                            snapshot_summary(&reloaded)
                        );
                        stable_since = None;
                        stable_samples = 0;
                    }
                } else {
                    last = signals_valid.err().map_or_else(
                        || {
                            format!(
                                "new revision/reload/topology predicate false: {}",
                                snapshot_summary(&snapshot)
                            )
                        },
                        |error| error.to_string(),
                    );
                    stable_since = None;
                    stable_samples = 0;
                }
            },
            Err(error) => {
                last = error.to_string();
                stable_since = None;
                stable_samples = 0;
            },
        }
        std::thread::sleep(POLL);
    }
    Err(format!("timed out waiting for {phase}; last observation: {last}").into())
}

/// Keep the accepted-revision probe in a concise human-readable observation.
fn observations_for_topology_probe(phase: &str, response: &SiteRouteResponse) {
    eprintln!(
        "[LEARNED-CEILING] {phase} accepted-revision probe: HTTP {}, site={:?}, backend={:?}, provider_site={:?}",
        response.status, response.site, response.backend, response.provider_site
    );
}

/// Send a uniquely sessioned request and require consistent Grid and provider attribution.
fn send_probe(
    port: u16,
    expected_site: Option<&str>,
    phase: &str,
    probe_failures: &mut Vec<Value>,
) -> Result<SiteRouteResponse, Box<dyn std::error::Error>> {
    let sequence = REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let response = match send_site_request(port, sequence, phase) {
        Ok(response) => response,
        Err(error) => {
            let failure = probe_failure_value(phase, sequence, expected_site, None, &error.to_string());
            eprintln!("[LEARNED-CEILING][PROBE][TRANSPORT-FAIL] {failure}");
            probe_failures.push(failure);
            return Err(error);
        },
    };
    if let Err(error) = validate_response_attribution(&response, expected_site) {
        let failure = probe_failure_value(phase, sequence, expected_site, Some(&response), &error);
        eprintln!("[LEARNED-CEILING][PROBE][FAIL] {failure}");
        probe_failures.push(failure);
        return Err(format!(
            "Grid Gateway request failed attribution validation (HTTP {}, headers={:?}): {error}",
            response.status, response.raw_header_names
        )
        .into());
    }
    Ok(response)
}

/// Capture an unsuccessful request without treating its HTTP status as transport failure.
fn probe_failure_value(
    phase: &str,
    sequence: usize,
    expected_site: Option<&str>,
    response: Option<&SiteRouteResponse>,
    error: &str,
) -> Value {
    let kind = response.map_or("transport_error", |response| {
        if response.status == 200 {
            "attribution_error"
        } else {
            "http_status"
        }
    });
    json!({
        "phase": phase,
        "observed_at": format_utc_iso(),
        "request_sequence": sequence,
        "kind": kind,
        "expected_site": expected_site,
        "status": response.map(|response| response.status),
        "x_grid_site": response.and_then(|response| response.site.as_deref()),
        "x_grid_backend": response.and_then(|response| response.backend.as_deref()),
        "provider_site": response.and_then(|response| response.provider_site.as_deref()),
        "raw_header_names": response.map(|response| &response.raw_header_names),
        "error": error,
    })
}

/// Require HTTP success and agreement among both Grid headers and the trusted provider marker.
fn validate_response_attribution(response: &SiteRouteResponse, expected_site: Option<&str>) -> Result<(), String> {
    if response.status != 200 {
        return Err(format!("expected HTTP 200, got {}", response.status));
    }
    let site = response.site.as_deref().ok_or("response has no x-grid-site header")?;
    let backend = response
        .backend
        .as_deref()
        .ok_or("response has no x-grid-backend header")?;
    let provider_site = response
        .provider_site
        .as_deref()
        .ok_or("response has no trusted provider marker")?;
    if site != provider_site {
        return Err(format!(
            "x-grid-site {site:?} disagrees with provider marker {provider_site:?}"
        ));
    }
    let expected_backend = format!("llmd-{site}-provider");
    if backend != expected_backend {
        return Err(format!(
            "x-grid-backend {backend:?} does not identify site {site:?} ({expected_backend})"
        ));
    }
    if expected_site.is_some_and(|expected| site != expected) {
        return Err(format!("expected route to {expected_site:?}, got {site:?}/{backend:?}"));
    }
    Ok(())
}

/// Compare direct EPP, both operator signal paths, and live Gateway state in one sample.
fn observe_runtime(
    context: &DemoContext,
    forwards: &PortForwards,
) -> Result<RuntimeSnapshot, Box<dyn std::error::Error>> {
    let local_context = kind_context(GRID_SERVING_SITE);
    let config_map = kube_json(&local_context, "get", &format!("configmap/{GRID_SERVING_CONFIGMAP}"))?;
    let operator_config = config_map
        .pointer("/data/serving-config.json")
        .and_then(Value::as_str)
        .ok_or("operator serving ConfigMap has no serving-config.json data")?
        .to_owned();
    let operator_value: Value = serde_json::from_str(&operator_config)?;
    let operator_candidates = candidate_pairs(&operator_value)?;
    let operator_digest = sha256_text(&operator_config);
    let operator_digest_annotation = annotation_value(&config_map, SERVING_DIGEST_ANNOTATION)
        .ok_or("operator serving ConfigMap has no content-addressed serving revision annotation")?;
    let operator_resource_version = config_map
        .pointer("/metadata/resourceVersion")
        .and_then(Value::as_str)
        .ok_or("operator serving ConfigMap has no resourceVersion")?
        .to_owned();

    let (gateway_pod, gateway_uid) = gateway_pod_identity(&local_context, context)?;
    let pod_name = gateway_pod
        .get("pod_name")
        .and_then(Value::as_str)
        .ok_or("Grid Gateway pod identity has no pod name")?;
    let output = Command::new("kubectl")
        .args([
            "--context",
            &local_context,
            "-n",
            GRID_SYSTEM_NS,
            "exec",
            pod_name,
            "-c",
            GATEWAY_CONTAINER,
            "--",
            "cat",
            MOUNTED_CONFIG_PATH,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "read mounted serving config from Gateway pod {pod_name}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let mounted_config = String::from_utf8_lossy(&output.stdout).into_owned();
    if mounted_config.trim().is_empty() {
        return Err("Gateway mounted serving config is empty".into());
    }
    let mounted_value: Value = serde_json::from_str(&mounted_config)?;
    let mounted_candidates = candidate_pairs(&mounted_value)?;
    let mounted_digest = sha256_text(&mounted_config);
    let load_window_ms = required_unsigned(&mounted_value, "/load_window_ms", "load_window_ms")?;
    let horizon_ms = mounted_value
        .pointer("/horizon_ms")
        .and_then(Value::as_u64)
        .unwrap_or(load_window_ms);
    let window_secs = required_unsigned(&mounted_value, "/window_secs", "window_secs")?;

    let epp_metrics = kubectl_exec_epp_metrics(SITE, context.metrics_transport == MetricsTransport::MtlsProxy)?;
    let epp = parse_epp_readings(&epp_metrics)?;
    let origin_operator_metrics = read_operator_metrics(forwards.remote_operator_metrics)?;
    let local_operator_metrics = read_operator_metrics(forwards.local_operator_metrics)?;
    let origin_in_flight = required_operator_sample(
        &origin_operator_metrics,
        "grid_provider_in_flight_requests",
        "pool-B origin in-flight",
    )?;
    let origin_ready = required_operator_sample(
        &origin_operator_metrics,
        "grid_provider_ready_endpoints",
        "pool-B origin readiness",
    )?;
    let local_in_flight = optional_operator_sample(
        &local_operator_metrics,
        "grid_provider_in_flight_requests",
        "pool-A relayed pool-B in-flight",
    )?;
    let local_ready = optional_operator_sample(
        &local_operator_metrics,
        "grid_provider_ready_endpoints",
        "pool-A relayed pool-B readiness",
    )?;
    let gateway_metrics = read_gateway_metrics(forwards.gateway_metrics)?;
    let gateway_reloads = gateway_config_observation_count(&gateway_metrics)?;
    let ceiling_raw = metric_sample(
        &gateway_metrics,
        CEILING_METRIC,
        &[("site", SITE), ("cluster", CLUSTER)],
    )
    .map(|(_, value)| value.to_owned());
    let ceiling = ceiling_raw.as_deref().and_then(|value| value.parse::<f64>().ok());
    let ceiling_is_nan = ceiling.is_some_and(f64::is_nan);
    let gateway_capacity = GatewayCapacityMetrics {
        in_flight: optional_metric_value(
            &gateway_metrics,
            IN_FLIGHT_METRIC,
            &[("site", SITE), ("cluster", CLUSTER)],
        ),
        soft_ceiling: optional_metric_value(
            &gateway_metrics,
            SOFT_CEILING_METRIC,
            &[("site", SITE), ("cluster", CLUSTER)],
        ),
        hard_ceiling: optional_metric_value(
            &gateway_metrics,
            HARD_CEILING_METRIC,
            &[("site", SITE), ("cluster", CLUSTER)],
        ),
        reported_saturation: optional_metric_value(
            &gateway_metrics,
            REPORTED_SATURATION_METRIC,
            &[("site", SITE), ("cluster", CLUSTER)],
        ),
        counted_saturation: optional_metric_value(
            &gateway_metrics,
            COUNTED_SATURATION_METRIC,
            &[("site", SITE), ("cluster", CLUSTER)],
        ),
    };

    Ok(RuntimeSnapshot {
        operator_config,
        operator_digest,
        operator_digest_annotation,
        operator_resource_version,
        mounted_config,
        mounted_digest,
        candidates: operator_candidates,
        mounted_candidates,
        epp,
        epp_metric_samples: relevant_epp_metric_lines(&epp_metrics),
        origin_in_flight,
        origin_ready,
        local_in_flight,
        local_ready,
        origin_operator_metric_samples: relevant_operator_metric_lines(&origin_operator_metrics),
        local_operator_metric_samples: relevant_operator_metric_lines(&local_operator_metrics),
        gateway_metrics: relevant_gateway_metric_lines(&gateway_metrics),
        gateway_reloads,
        ceiling,
        ceiling_raw,
        ceiling_is_nan,
        gateway_capacity,
        gateway_uid,
        gateway_image_identity: gateway_pod,
        load_window_ms,
        horizon_ms,
        window_secs,
    })
}

/// Read the exact Ready Grid Gateway pod identity and actual runtime image fields.
fn gateway_pod_identity(context: &str, demo: &DemoContext) -> Result<(Value, String), Box<dyn std::error::Error>> {
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
    let pods: Value = serde_json::from_slice(&output.stdout)?;
    let items = pods
        .get("items")
        .and_then(Value::as_array)
        .ok_or("Grid Gateway pod query returned no items array")?;
    let ready: Vec<&Value> = items
        .iter()
        .filter(|pod| {
            pod.pointer("/status/phase").and_then(Value::as_str) == Some("Running")
                && pod
                    .pointer("/status/conditions")
                    .and_then(Value::as_array)
                    .is_some_and(|conditions| {
                        conditions.iter().any(|condition| {
                            condition.get("type").and_then(Value::as_str) == Some("Ready")
                                && condition.get("status").and_then(Value::as_str) == Some("True")
                        })
                    })
        })
        .collect();
    if ready.len() != 1 {
        return Err(format!("expected one Ready Grid Gateway pod, found {}", ready.len()).into());
    }
    let pod = ready.first().ok_or("Ready Grid Gateway pod disappeared")?;
    let uid = pod
        .pointer("/metadata/uid")
        .and_then(Value::as_str)
        .ok_or("Ready Grid Gateway pod has no UID")?
        .to_owned();
    let name = pod
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .ok_or("Ready Grid Gateway pod has no name")?;
    let requested = pod
        .pointer("/spec/containers")
        .and_then(Value::as_array)
        .and_then(|containers| {
            containers
                .iter()
                .find(|container| container.get("name").and_then(Value::as_str) == Some(GATEWAY_CONTAINER))
        })
        .and_then(|container| container.get("image"))
        .and_then(Value::as_str)
        .ok_or("Ready Grid Gateway pod has no praxis container image reference")?;
    let expected = demo
        .images
        .grid_gateway
        .as_deref()
        .ok_or("full-mode Grid Gateway image is not configured")?;
    if requested != expected {
        return Err(format!(
            "running Grid Gateway requested image {requested:?}, expected the source-built image {expected:?}"
        )
        .into());
    }
    let container_status = pod
        .pointer("/status/containerStatuses")
        .and_then(Value::as_array)
        .and_then(|containers| {
            containers
                .iter()
                .find(|container| container.get("name").and_then(Value::as_str) == Some(GATEWAY_CONTAINER))
        })
        .ok_or("Ready Grid Gateway pod has no praxis container status")?;
    let image_id = container_status
        .get("imageID")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or("Ready Grid Gateway pod has no runtime imageID")?;
    let container_id = container_status
        .get("containerID")
        .and_then(Value::as_str)
        .unwrap_or_default();
    Ok((
        json!({
            "pod_name": name,
            "pod_uid": uid,
            "container": GATEWAY_CONTAINER,
            "requested_image": requested,
            "runtime_image_id": image_id,
            "container_id": container_id,
        }),
        uid,
    ))
}

/// Parse and validate the complete signal surface needed to teach a ceiling.
fn parse_epp_readings(metrics: &str) -> Result<EppReadings, Box<dyn std::error::Error>> {
    let running = required_metric(
        metrics,
        RUNNING_METRIC,
        &[("name", "pool-b")],
        "pool-B running requests",
    )?;
    let queue = required_metric(metrics, QUEUE_METRIC, &[("name", "pool-b")], "pool-B average queue")?;
    let kv_cache_utilization = required_metric(
        metrics,
        KV_CACHE_METRIC,
        &[("name", "pool-b")],
        "pool-B average KV-cache utilization",
    )?;
    let kv_cache_spread = metric_sample(metrics, KV_CACHE_SPREAD_METRIC, &[("name", "pool-b")])
        .map(|(_, raw)| raw.parse::<f64>())
        .transpose()?;
    let ready = required_metric(metrics, READY_METRIC, &[("name", "pool-b")], "pool-B ready endpoints")?;
    let per_endpoint_queues = metric_values(metrics, PER_ENDPOINT_QUEUE_METRIC, &[("name", "pool-b")]);
    if per_endpoint_queues.is_empty() {
        return Err("pinned EPP has no per-endpoint queue series for pool-b".into());
    }
    let held_capacity = metric_values(metrics, HELD_CAPACITY_METRIC, &[("inference_pool", "pool-b")]);
    if held_capacity.is_empty() {
        return Err("pinned EPP has no flow-control request-capacity observation for pool-b".into());
    }
    let held_queue = metric_values(metrics, HELD_QUEUE_METRIC, &[("inference_pool", "pool-b")]);
    let held_queue_count = (!held_queue.is_empty()).then(|| held_queue.iter().sum());
    let readings = EppReadings {
        running,
        queue,
        kv_cache_utilization,
        kv_cache_spread,
        ready,
        per_endpoint_queues,
        held_queue_count,
        held_source: if held_queue.is_empty() {
            HELD_CAPACITY_METRIC.to_owned()
        } else {
            HELD_QUEUE_METRIC.to_owned()
        },
        held_capacity_utilization: held_capacity.into_iter().fold(0.0_f64, f64::max),
    };
    for (name, value) in [
        ("running requests", readings.running),
        ("average queue", readings.queue),
        ("average KV-cache utilization", readings.kv_cache_utilization),
        ("ready endpoints", readings.ready),
        ("held capacity utilization", readings.held_capacity_utilization),
    ] {
        if !value.is_finite() || value < 0.0 {
            return Err(format!("EPP {name} is not a finite non-negative value: {value}").into());
        }
    }
    if !(0.0..=1.0).contains(&readings.kv_cache_utilization) {
        return Err(format!(
            "EPP average KV-cache utilization is outside the normalized range: {}",
            readings.kv_cache_utilization
        )
        .into());
    }
    if readings
        .kv_cache_spread
        .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
    {
        return Err(format!(
            "EPP KV-cache utilization spread is outside the normalized range: {:?}",
            readings.kv_cache_spread
        )
        .into());
    }
    if readings
        .per_endpoint_queues
        .iter()
        .any(|value| !value.is_finite() || *value < 0.0)
    {
        return Err("EPP per-endpoint queue contains a non-finite or negative value".into());
    }
    if readings
        .held_queue_count
        .is_some_and(|value| !value.is_finite() || value < 0.0)
    {
        return Err("EPP held queue count is non-finite or negative".into());
    }
    Ok(readings)
}

/// Check that all available waiting and held-work observations are exactly zero.
fn no_waiting_or_held_work(epp: &EppReadings) -> bool {
    epp.queue == 0.0
        && epp.per_endpoint_queues.iter().all(|queue| *queue == 0.0)
        && epp.held_queue_count.is_none_or(|held| held == 0.0)
        && epp.held_capacity_utilization == 0.0
}

/// Require zero waiting and held work and match both operator views to the raw EPP sample.
fn require_signal_values(snapshot: &RuntimeSnapshot, expected_running: f64) -> Result<(), Box<dyn std::error::Error>> {
    let epp = &snapshot.epp;
    if (epp.running - expected_running).abs() > f64::EPSILON {
        return Err(format!("EPP running requests are {}, expected {expected_running}", epp.running).into());
    }
    if epp.ready < 1.0 || epp.ready.fract() != 0.0 {
        return Err(format!("EPP ready-endpoint count is not a positive integer: {}", epp.ready).into());
    }
    if epp.per_endpoint_queues.len() != usize_value(epp.ready)? {
        return Err(format!(
            "EPP reports {} ready endpoints but {} per-endpoint queue series",
            epp.ready,
            epp.per_endpoint_queues.len()
        )
        .into());
    }
    if !no_waiting_or_held_work(epp) {
        return Err(format!(
            "EPP must report exactly zero waiting and held work: average_queue={}, per_endpoint_queues={:?}, held_queue={:?}, request_capacity_utilization={}",
            epp.queue, epp.per_endpoint_queues, epp.held_queue_count, epp.held_capacity_utilization
        )
        .into());
    }

    let expected_in_flight = epp.running * epp.ready + epp.queue + epp.held_queue_count.unwrap_or(0.0);
    for (name, actual, expected) in [("pool-B origin", snapshot.origin_in_flight, expected_in_flight)] {
        if (actual - expected).abs() > 0.01 {
            return Err(format!("{name} in-flight sample {actual} does not match EPP-derived {expected}").into());
        }
    }
    if (snapshot.origin_ready - epp.ready).abs() > f64::EPSILON {
        return Err(format!(
            "origin ready-endpoint signal differs from EPP: EPP={}, origin={}",
            epp.ready, snapshot.origin_ready
        )
        .into());
    }
    let local_signal = local_relay_matches(
        pair_present(&snapshot.candidates),
        snapshot.local_in_flight,
        snapshot.local_ready,
        expected_in_flight,
        epp.ready,
    )
    .map_err(Into::<Box<dyn std::error::Error>>::into)?;
    if !local_signal {
        return Err(format!(
            "pool-A relayed signal differs from EPP: EPP in-flight={expected_in_flight}, EPP ready={}, relay in-flight={:?}, relay ready={:?}",
            epp.ready, snapshot.local_in_flight, snapshot.local_ready
        )
        .into());
    }
    Ok(())
}

/// Match the local operator relay, requiring it while the provider is published.
fn local_relay_matches(
    pair_published: bool,
    local_in_flight: Option<f64>,
    local_ready: Option<f64>,
    expected_in_flight: f64,
    expected_ready: f64,
) -> Result<bool, String> {
    match (local_in_flight, local_ready) {
        (Some(in_flight), Some(ready)) => {
            Ok((in_flight - expected_in_flight).abs() <= 0.01 && (ready - expected_ready).abs() <= f64::EPSILON)
        },
        (None, None) if !pair_published => Ok(true),
        (None, None) => Err("pool-A relayed metrics are missing while the provider pair is published".to_owned()),
        (None, Some(_)) if pair_published => {
            Err("pool-A relayed in-flight metric is missing while the provider pair is published".to_owned())
        },
        (Some(_), None) if pair_published => {
            Err("pool-A relayed readiness metric is missing while the provider pair is published".to_owned())
        },
        (None, Some(ready)) => Ok((ready - expected_ready).abs() <= f64::EPSILON),
        (Some(in_flight), None) => Ok((in_flight - expected_in_flight).abs() <= 0.01),
    }
}

/// Convert an already-validated integral endpoint count without a lossy float cast.
fn usize_value(value: f64) -> Result<usize, Box<dyn std::error::Error>> {
    value
        .to_string()
        .parse::<usize>()
        .map_err(|error| format!("invalid endpoint count {value}: {error}").into())
}

/// Capture a batch of attributed real requests as a separate time-ordered evidence phase.
#[expect(
    clippy::too_many_arguments,
    reason = "each evidence sample is tied to its phase, expected site, runtime snapshot, and output collectors"
)]
fn record_attributed_requests(
    context: &DemoContext,
    forwards: &PortForwards,
    phase: &str,
    expected_site: Option<&str>,
    snapshot: &RuntimeSnapshot,
    phases: &mut Vec<Value>,
    observations: &mut Vec<String>,
    probe_failures: &mut Vec<Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    record_attributed_request_batch(
        context,
        forwards,
        phase,
        expected_site,
        snapshot,
        phases,
        observations,
        probe_failures,
        REQUESTS_PER_PHASE,
    )
    .map(|_| ())
}

/// Capture a fixed-size batch of successful, attributed Gateway responses.
#[expect(
    clippy::too_many_arguments,
    reason = "the paired routing samples retain their phase, state snapshot, and evidence collectors"
)]
fn record_attributed_request_batch(
    context: &DemoContext,
    forwards: &PortForwards,
    phase: &str,
    expected_site: Option<&str>,
    snapshot: &RuntimeSnapshot,
    phases: &mut Vec<Value>,
    observations: &mut Vec<String>,
    probe_failures: &mut Vec<Value>,
    request_count: usize,
) -> Result<RouteDistribution, Box<dyn std::error::Error>> {
    let mut distribution = RouteDistribution::default();
    let mut responses = Vec::new();
    for _ in 0..request_count {
        let response = send_probe(forwards.gateway_http, expected_site, phase, probe_failures)?;
        distribution
            .observe(&response)
            .map_err(|error| format!("record provider attribution: {error}"))?;
        responses.push(response_value(&response));
    }
    let attribution = json!({
        "requested": request_count,
        "successful": responses.len(),
        "expected_site": expected_site,
        "distribution": {
            "sites": distribution.sites,
            "backends": distribution.backends,
            "provider_gateways": distribution.provider_gateways,
        },
        "responses": responses,
    });
    record_phase(context, phase, snapshot, Some(&attribution), phases)?;
    observations.push(format!("{phase}: {attribution}"));
    Ok(distribution)
}

/// Calculate pool B's share from a complete attributed response sample.
fn pool_b_share(distribution: &RouteDistribution, expected_requests: usize, label: &str) -> Result<f64, String> {
    let observed = distribution.sites.values().sum::<usize>();
    if observed != expected_requests {
        return Err(format!(
            "{label} attributed {observed} requests, expected the full {expected_requests}-request sample"
        ));
    }
    let pool_b = distribution.sites.get(SITE).copied().unwrap_or_default();
    let observed = u32::try_from(observed)
        .map_err(|error| format!("{label} request count does not fit a metric sample: {error}"))?;
    let pool_b =
        u32::try_from(pool_b).map_err(|error| format!("{label} pool-B count does not fit a metric sample: {error}"))?;
    Ok(f64::from(pool_b) / f64::from(observed))
}

/// Require a matched route sample to reduce the saturated pool's share, not send every request to one site.
fn kv_cache_route_share_shift(
    queue_free: &RouteDistribution,
    hard_bound: &RouteDistribution,
    expected_requests: usize,
) -> Result<(f64, f64), String> {
    let soft_share = pool_b_share(queue_free, expected_requests, "queue-free soft-ceiling sample")?;
    let hard_share = pool_b_share(hard_bound, expected_requests, "KV-cache hard-bound sample")?;
    if soft_share < MIN_SOFT_BASELINE_POOL_B_SHARE {
        return Err(format!(
            "queue-free sample did not establish pool B's expected capacity preference: {:.1}% is below {:.1}%",
            soft_share * 100.0,
            MIN_SOFT_BASELINE_POOL_B_SHARE * 100.0
        ));
    }
    if hard_share > MAX_HARD_BOUND_POOL_B_SHARE {
        return Err(format!(
            "KV-cache hard-bound sample still routes too much traffic to pool B: {:.1}% exceeds {:.1}%",
            hard_share * 100.0,
            MAX_HARD_BOUND_POOL_B_SHARE * 100.0
        ));
    }
    let reduction = soft_share - hard_share;
    if reduction < MIN_HARD_BOUND_POOL_B_SHARE_REDUCTION {
        return Err(format!(
            "KV-cache hard bound reduced pool-B share by only {:.1} percentage points; at least {:.1} are required",
            reduction * 100.0,
            MIN_HARD_BOUND_POOL_B_SHARE_REDUCTION * 100.0
        ));
    }
    Ok((soft_share, hard_share))
}

/// Require provider-reported KV saturation alone to reduce the route share with a complete sample.
fn kv_cache_reported_only_route_share_shift(
    low_pressure: &RouteDistribution,
    reported_saturated: &RouteDistribution,
    expected_requests: usize,
) -> Result<(f64, f64), String> {
    let low_pressure_share = pool_b_share(low_pressure, expected_requests, "low-pressure soft-ceiling sample")?;
    let saturated_share = pool_b_share(reported_saturated, expected_requests, "reported-saturation sample")?;
    if low_pressure_share < MIN_SOFT_BASELINE_POOL_B_SHARE {
        return Err(format!(
            "low-pressure sample did not establish pool B's expected capacity preference: {:.1}% is below {:.1}%",
            low_pressure_share * 100.0,
            MIN_SOFT_BASELINE_POOL_B_SHARE * 100.0
        ));
    }
    let reduction = low_pressure_share - saturated_share;
    if reduction < MIN_REPORTED_KV_POOL_B_SHARE_REDUCTION {
        return Err(format!(
            "provider-reported KV saturation reduced pool-B share by only {:.1} percentage points; at least {:.1} are required",
            reduction * 100.0,
            MIN_REPORTED_KV_POOL_B_SHARE_REDUCTION * 100.0
        ));
    }
    Ok((low_pressure_share, saturated_share))
}

/// Validate the KV-only route signal independently of count saturation and backlog learning.
fn reported_only_kv_cache_is_valid(
    epp: &EppReadings,
    gateway: GatewayCapacityMetrics,
    effective_ceiling: Option<f64>,
    route_rho: f64,
    high_soft_ceiling: f64,
) -> Result<(), String> {
    if (epp.running - f64::from(LOW_RUNNING_REQUESTS)).abs() > f64::EPSILON
        || epp.queue.abs() > f64::EPSILON
        || (epp.kv_cache_utilization - KV_HARD_BOUND_CACHE_MEAN).abs() > KV_CACHE_SIGNAL_TOLERANCE
        || epp
            .kv_cache_spread
            .is_none_or(|spread| (spread - KV_HARD_BOUND_CACHE_SPREAD).abs() > KV_CACHE_SIGNAL_TOLERANCE)
        || (epp.ready - 2.0).abs() > f64::EPSILON
        || epp.per_endpoint_queues.len() != 2
        || !epp.per_endpoint_queues.iter().all(|queue| queue.abs() <= f64::EPSILON)
        || !no_waiting_or_held_work(epp)
    {
        return Err(format!(
            "reported-only KV proof requires low running={}, two ready endpoints, no queue or held work, and KV mean/spread=({KV_HARD_BOUND_CACHE_MEAN}, {KV_HARD_BOUND_CACHE_SPREAD}); observed running={}, queue={}, KV mean/spread=({:.3}, {:?}), ready={}, per-endpoint queues={:?}, held queue={:?}/{}",
            LOW_RUNNING_REQUESTS,
            epp.running,
            epp.queue,
            epp.kv_cache_utilization,
            epp.kv_cache_spread,
            epp.ready,
            epp.per_endpoint_queues,
            epp.held_queue_count,
            epp.held_capacity_utilization
        ));
    }
    let expected_in_flight = (epp.running + epp.queue) * epp.ready + epp.held_queue_count.unwrap_or(0.0);
    let actual_in_flight = gateway
        .in_flight
        .filter(|value| value.is_finite())
        .ok_or("Gateway did not publish in-flight requests for the reported-only KV sample")?;
    if (actual_in_flight - expected_in_flight).abs() > 0.01 {
        return Err(format!(
            "Gateway in-flight {actual_in_flight:.2} does not match the low-count EPP value {expected_in_flight:.2}"
        ));
    }
    let soft = gateway
        .soft_ceiling
        .filter(|value| value.is_finite())
        .ok_or("Gateway did not preserve the queue-free soft ceiling during the reported-only KV sample")?;
    if (soft - high_soft_ceiling).abs() > 0.01 {
        return Err(format!(
            "reported-only KV sample changed the soft ceiling: expected {high_soft_ceiling:.2}, got {soft:.2}"
        ));
    }
    if gateway.hard_ceiling.is_some_and(f64::is_finite) || !counted_saturation_is_absent(gateway) {
        return Err(format!(
            "the low-count KV sample must not learn a hard bound or count saturation: hard={:?}, counted={:?}",
            gateway.hard_ceiling, gateway.counted_saturation
        ));
    }
    let reported = gateway
        .reported_saturation
        .filter(|value| value.is_finite())
        .ok_or("Gateway did not publish provider-reported KV saturation")?;
    let spread = epp.kv_cache_spread.ok_or("pinned EPP sample has no KV-cache spread")?;
    let expected_reported = (epp.kv_cache_utilization + spread).clamp(0.0, 1.0);
    if (reported - expected_reported).abs() > KV_CACHE_SIGNAL_TOLERANCE {
        return Err(format!(
            "Gateway provider saturation {reported:.3} does not include the pinned EPP mean-plus-spread {expected_reported:.3}"
        ));
    }
    let effective = effective_ceiling
        .filter(|value| value.is_finite())
        .ok_or("Gateway has no finite effective ceiling while testing provider-reported KV saturation")?;
    if (effective - high_soft_ceiling).abs() > 0.01 {
        return Err(format!(
            "without a hard bound, the effective ceiling should remain the soft value {high_soft_ceiling:.2}, got {effective:.2}"
        ));
    }
    if !route_rho.is_finite() || !(MIN_REPORTED_KV_ROUTE_RHO..=1.0).contains(&route_rho) {
        return Err(format!(
            "combined route rho {route_rho:.3} did not reflect the full provider KV signal without count saturation"
        ));
    }
    Ok(())
}

/// Convert a validated route response to safe structured evidence.
fn response_value(response: &SiteRouteResponse) -> Value {
    json!({
        "status": response.status,
        "x_grid_site": response.site,
        "x_grid_backend": response.backend,
        "provider_site": response.provider_site,
        "raw_header_names": response.raw_header_names,
    })
}

/// Append one structured snapshot and persist the exact operator and mounted JSON contents.
fn record_phase(
    context: &DemoContext,
    name: &str,
    snapshot: &RuntimeSnapshot,
    attribution: Option<&Value>,
    phases: &mut Vec<Value>,
) -> Result<Value, Box<dyn std::error::Error>> {
    let revision_dir = context
        .evidence_dir
        .join("learned-ceiling-lifecycle")
        .join("serving-revisions");
    fs::create_dir_all(&revision_dir)?;
    let operator_name = format!("operator-{}.json", safe_digest(&snapshot.operator_digest));
    let mounted_name = format!("mounted-{}.json", safe_digest(&snapshot.mounted_digest));
    persist_content(&revision_dir.join(operator_name), &snapshot.operator_config)?;
    persist_content(&revision_dir.join(mounted_name), &snapshot.mounted_config)?;
    let finite_ceiling = snapshot.ceiling.filter(|value| value.is_finite());
    let record = json!({
        "phase": name,
        "observed_at": format_utc_iso(),
        "operator_revision": {
            "content_digest": snapshot.operator_digest,
            "annotation_digest": snapshot.operator_digest_annotation,
            "resource_version": snapshot.operator_resource_version,
            "config_file": revision_dir.join(format!("operator-{}.json", safe_digest(&snapshot.operator_digest))).display().to_string(),
        },
        "mounted_revision": {
            "content_digest": snapshot.mounted_digest,
            "matches_operator": snapshot.mounted_digest == snapshot.operator_digest,
            "config_file": revision_dir.join(format!("mounted-{}.json", safe_digest(&snapshot.mounted_digest))).display().to_string(),
        },
        "candidate_pairs": snapshot.candidates,
        "mounted_candidate_pairs": snapshot.mounted_candidates,
        "signals": {
            "epp": {
                "running_requests_per_ready_endpoint": snapshot.epp.running,
                "average_waiting_requests": snapshot.epp.queue,
                "average_kv_cache_utilization": snapshot.epp.kv_cache_utilization,
                "kv_cache_utilization_spread": snapshot.epp.kv_cache_spread,
                "ready_endpoints": snapshot.epp.ready,
                "per_endpoint_queues": snapshot.epp.per_endpoint_queues,
                "held_queue_count": snapshot.epp.held_queue_count,
                "held_observation_source": snapshot.epp.held_source,
                "held_request_capacity_utilization": snapshot.epp.held_capacity_utilization,
                "metric_samples": snapshot.epp_metric_samples,
            },
            "pool_b_origin": {
                "in_flight": snapshot.origin_in_flight,
                "ready_endpoints": snapshot.origin_ready,
                "metric_samples": snapshot.origin_operator_metric_samples,
            },
            "pool_a_relay": {
                "in_flight": snapshot.local_in_flight,
                "ready_endpoints": snapshot.local_ready,
                "metric_samples": snapshot.local_operator_metric_samples,
            },
        },
        "gateway": {
            "pod_uid": snapshot.gateway_uid,
            "image": snapshot.gateway_image_identity,
            "config_watcher_observation_count": snapshot.gateway_reloads,
            "ceiling_metric": CEILING_METRIC,
            "ceiling_value": finite_ceiling,
            "ceiling_raw": snapshot.ceiling_raw,
            "ceiling_is_nan": snapshot.ceiling_is_nan,
            "in_flight": snapshot.gateway_capacity.in_flight,
            "soft_ceiling": snapshot.gateway_capacity.soft_ceiling,
            "hard_ceiling": snapshot.gateway_capacity.hard_ceiling,
            "reported_saturation": snapshot.gateway_capacity.reported_saturation,
            "counted_saturation": snapshot.gateway_capacity.counted_saturation,
            "metric_samples": snapshot.gateway_metrics,
        },
        "load_store": {
            "load_window_ms": snapshot.load_window_ms,
            "horizon_ms": snapshot.horizon_ms,
            "window_secs": snapshot.window_secs,
        },
        "attribution": attribution,
    });
    phases.push(record.clone());
    Ok(record)
}

/// Write evidence once per content digest and reject inconsistent repeated content.
fn persist_content(path: &std::path::Path, contents: &str) -> Result<(), Box<dyn std::error::Error>> {
    if path.exists() {
        let existing = fs::read_to_string(path)?;
        if existing != contents {
            return Err(format!("evidence path {} already contains different content", path.display()).into());
        }
        return Ok(());
    }
    fs::write(path, contents)?;
    Ok(())
}

/// Write the standalone structured proof artifact.
#[expect(
    clippy::too_many_arguments,
    reason = "the standalone evidence envelope keeps each failure and source field explicit"
)]
fn write_evidence(
    context: &DemoContext,
    source_sha: Option<&str>,
    tracked_worktree_clean: Option<bool>,
    source_worktree_sha256: Option<&str>,
    started_at: &str,
    success: bool,
    primary_error: Option<&str>,
    restoration_errors: &[String],
    original_state: Option<Value>,
    image_manifest: Option<Value>,
    phases: &[Value],
    probe_failures: &[Value],
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let directory = context.evidence_dir.join("learned-ceiling-lifecycle");
    fs::create_dir_all(&directory)?;
    let path = directory.join("evidence.json");
    let evidence = LearnedCeilingEvidence {
        schema_version: "1",
        source_sha,
        tracked_worktree_clean,
        source_worktree_sha256,
        run_cluster_prefix: run_cluster_prefix(),
        started_at,
        success,
        primary_error,
        restoration_errors,
        original_state,
        image_manifest,
        phases,
        probe_failures,
    };
    fs::write(path.as_path(), serde_json::to_vec_pretty(&evidence)?)?;
    Ok(path)
}

/// Summarize original state without copying unneeded runtime data into the envelope.
fn original_state_summary(original: &OriginalState) -> Value {
    let simulator_annotations: BTreeMap<&str, &BTreeMap<String, Option<String>>> = original
        .simulator_annotations
        .iter()
        .map(|(deployment, annotations)| (deployment.as_str(), annotations))
        .collect();
    json!({
        "provider_access_policy": original.access_policy,
        "simulator_config_sha256": original
            .simulator_configs
            .iter()
            .map(|(name, config)| (name, sha256_text(config)))
            .collect::<BTreeMap<_, _>>(),
        "simulator_configs": original.simulator_configs,
        "pool_a_operator_overlay_sha256": sha256_text(&original.pool_a_overlay),
        "pool_a_operator_overlay": original.pool_a_overlay,
        "simulator_annotations": simulator_annotations,
        "simulator_replicas": original.simulator_replicas,
        "simulator_epp_readings": epp_value(&original.simulator_readings),
        "gridnetwork_metrics_refresh_annotations": original.reconcile_annotations,
        "serving_revision": original.baseline.operator_digest,
        "serving_resource_version": original.baseline.operator_resource_version,
        "gateway_pod_uid": original.baseline.gateway_uid,
        "gateway_image": original.baseline.gateway_image_identity,
    })
}

/// Serialize all direct EPP observations with non-finite values already rejected.
fn epp_value(epp: &EppReadings) -> Value {
    json!({
        "running": epp.running,
        "queue": epp.queue,
        "kv_cache_utilization": epp.kv_cache_utilization,
        "kv_cache_spread": epp.kv_cache_spread,
        "ready": epp.ready,
        "per_endpoint_queues": epp.per_endpoint_queues,
        "held_queue_count": epp.held_queue_count,
        "held_source": epp.held_source,
        "held_capacity_utilization": epp.held_capacity_utilization,
    })
}

/// Collect requested image references and actual Docker and pod-runtime image identities.
fn collect_image_manifest(context: &DemoContext) -> Result<Value, Box<dyn std::error::Error>> {
    let mut references = BTreeMap::from([
        ("gateway".to_owned(), context.images.gateway.clone()),
        ("operator".to_owned(), context.images.operator.clone()),
        ("epp".to_owned(), context.images.epp.clone()),
        ("simulator".to_owned(), context.images.vcr.clone()),
        ("overlay_sync".to_owned(), context.images.overlay_sync.clone()),
    ]);
    if let Some(image) = &context.images.grid_gateway {
        references.insert("grid_gateway".to_owned(), image.clone());
    }
    if let Some(image) = &context.images.nginx {
        references.insert("nginx".to_owned(), image.clone());
    }
    let mut docker_identities = BTreeMap::new();
    for (role, image) in &references {
        docker_identities.insert(role.clone(), docker_image_identity(image)?);
    }
    let mut pods = BTreeMap::new();
    for site in [GRID_SERVING_SITE, SITE] {
        let site_context = kind_context(site);
        let object = kube_json(&site_context, "get", "pods")?;
        let items = object
            .get("items")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("{site} pod listing has no items array"))?;
        let mut identities = Vec::new();
        for pod in items {
            if pod.pointer("/metadata/namespace").and_then(Value::as_str) != Some(GRID_SYSTEM_NS) {
                continue;
            }
            identities.push(pod_image_identity(pod));
        }
        pods.insert(site.to_owned(), identities);
    }
    Ok(json!({
        "requested_references": references,
        "docker_image_identities": docker_identities,
        "running_pod_images": pods,
    }))
}

/// Read a Docker image's immutable config ID and any repository digests.
fn docker_image_identity(reference: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let output = Command::new("docker").args(["image", "inspect", reference]).output()?;
    if !output.status.success() {
        return Err(format!(
            "docker image inspect {reference}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let images: Value = serde_json::from_slice(&output.stdout)?;
    let image = images
        .as_array()
        .and_then(|items| items.first())
        .ok_or_else(|| format!("docker image inspect returned no image for {reference}"))?;
    let id = image
        .get("Id")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("Docker image {reference} has no config ID"))?;
    Ok(json!({
        "reference": reference,
        "image_id": id,
        "repo_digests": image.get("RepoDigests").cloned().unwrap_or(Value::Null),
        "repo_tags": image.get("RepoTags").cloned().unwrap_or(Value::Null),
        "architecture": image.get("Architecture"),
        "os": image.get("Os"),
    }))
}

/// Extract requested and runtime image identities from one Kubernetes pod object.
fn pod_image_identity(pod: &Value) -> Value {
    let containers = pod
        .pointer("/spec/containers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|container| {
            let name = container.get("name").and_then(Value::as_str).unwrap_or("unknown");
            let status = pod
                .pointer("/status/containerStatuses")
                .and_then(Value::as_array)
                .and_then(|statuses| {
                    statuses
                        .iter()
                        .find(|status| status.get("name").and_then(Value::as_str) == Some(name))
                });
            json!({
                "name": name,
                "requested_image": container.get("image"),
                "runtime_image": status.and_then(|item| item.get("image")),
                "runtime_image_id": status.and_then(|item| item.get("imageID")),
                "container_id": status.and_then(|item| item.get("containerID")),
            })
        })
        .collect::<Vec<_>>();
    json!({
        "name": pod.pointer("/metadata/name"),
        "uid": pod.pointer("/metadata/uid"),
        "phase": pod.pointer("/status/phase"),
        "containers": containers,
    })
}

/// Change only the pool-B provider policy so the pool-A consumer cannot select it.
fn patch_access_policy_to_exclude_run_site(original: &OriginalState) -> Result<String, Box<dyn std::error::Error>> {
    let local_context = kind_context(GRID_SERVING_SITE);
    let consumer = kube_json(&local_context, "get", &format!("gridsite/{GRID_SERVING_SITE}"))?;
    let labels = consumer
        .pointer("/metadata/labels")
        .and_then(Value::as_object)
        .ok_or("pool-A GridSite has no metadata.labels object")?;
    let run_token = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let run_value = format!("exclude-{run_token}");
    if labels.get(RUN_EXCLUSION_LABEL).and_then(Value::as_str) == Some(run_value.as_str()) {
        return Err("run-unique exclusion label unexpectedly already matches pool-A GridSite".into());
    }
    let policy = policy_with_run_exclusion(&original.access_policy, &run_value)?;
    patch_access_policy_value(&policy)?;
    let applied = read_access_policy()?;
    if applied != policy {
        return Err("pool-B InferenceProvider did not retain the run-specific exclusion policy".into());
    }
    Ok(run_value)
}

/// Add only the run-owned selector label while preserving the saved policy's other selectors.
fn policy_with_run_exclusion(original: &Value, run_value: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let mut policy = original.clone();
    let policy_object = policy
        .as_object_mut()
        .ok_or("saved provider accessPolicy is not an object")?;
    let selector = policy_object
        .entry("siteSelector".to_owned())
        .or_insert_with(|| json!({}));
    let selector_object = selector
        .as_object_mut()
        .ok_or("saved provider accessPolicy.siteSelector is not an object")?;
    let match_labels = selector_object
        .entry("matchLabels".to_owned())
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or("saved provider accessPolicy.siteSelector.matchLabels is not an object")?;
    if match_labels.contains_key(RUN_EXCLUSION_LABEL) {
        return Err("saved provider access policy already uses the run-exclusion label".into());
    }
    match_labels.insert(RUN_EXCLUSION_LABEL.to_owned(), Value::String(run_value.to_owned()));
    Ok(policy)
}

/// Restore the provider's original access policy exactly.
fn patch_access_policy(original: &OriginalState) -> Result<(), Box<dyn std::error::Error>> {
    patch_access_policy_value(&original.access_policy)
}

/// Apply one complete accessPolicy object to the run-owned pool-B provider.
fn patch_access_policy_value(policy: &Value) -> Result<(), Box<dyn std::error::Error>> {
    let context = kind_context(SITE);
    let patch = access_policy_json_patch(policy).to_string();
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "patch",
            "inferenceprovider",
            CLUSTER,
            "--type=json",
            "-p",
            &patch,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "patch pool-B InferenceProvider accessPolicy: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(())
}

/// Replace the complete accessPolicy object so temporary nested labels cannot survive restoration.
fn access_policy_json_patch(policy: &Value) -> Value {
    json!([{"op":"replace","path":"/spec/accessPolicy","value":policy}])
}

/// Read the pool-B provider access policy exactly as Kubernetes stores it.
fn read_access_policy() -> Result<Value, Box<dyn std::error::Error>> {
    let provider = kube_json(&kind_context(SITE), "get", &format!("inferenceprovider/{CLUSTER}"))?;
    provider
        .pointer("/spec/accessPolicy")
        .cloned()
        .ok_or_else(|| "pool-B InferenceProvider has no spec.accessPolicy".into())
}

/// Trigger a checked reconcile without changing any resource outside this run's clusters.
fn trigger_gridnetwork_reconcile_strict(site: &str) -> Result<(), Box<dyn std::error::Error>> {
    let context = kind_context(site);
    let token = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos().to_string();
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "annotate",
            "gridnetwork",
            GRID_NETWORK_NAME,
            &format!("{METRICS_REFRESH_ANNOTATION}={token}"),
            "--overwrite",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "trigger {site} GridNetwork reconcile: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(())
}

/// Restore the provider policy, simulator config/annotations, and reconcile annotations on all exits.
#[expect(
    clippy::too_many_arguments,
    reason = "restoration must retain the run state and separate cleanup, phase, and probe diagnostics"
)]
fn restore_original_state(
    context: &DemoContext,
    forwards: &PortForwards,
    original: &OriginalState,
    phases: &mut Vec<Value>,
    errors: &mut Vec<String>,
    probe_failures: &mut Vec<Value>,
) {
    let before_restore = observe_runtime(context, forwards).ok();
    match read_access_policy() {
        Ok(current) if current == original.access_policy => {},
        Ok(_) | Err(_) => {
            if let Err(error) = patch_access_policy(original) {
                errors.push(format!("restore pool-B InferenceProvider accessPolicy: {error}"));
            }
        },
    }

    if let Err(error) = restore_simulator_state(context, original) {
        errors.push(format!("restore pool-B simulator state: {error}"));
    }

    for site in [SITE, GRID_SERVING_SITE] {
        if let Err(error) = trigger_gridnetwork_reconcile_strict(site) {
            errors.push(format!("reconcile {site} after restoring provider policy: {error}"));
        }
    }
    if let Err(error) = wait_for_restored_topology(
        context,
        forwards,
        original,
        phases,
        probe_failures,
        before_restore.as_ref(),
    ) {
        errors.push(format!("restore and verify the original serving topology: {error}"));
    }

    for (site, previous) in &original.reconcile_annotations {
        if let Err(error) = restore_reconcile_annotation(site, previous.as_deref()) {
            errors.push(format!("restore {site} GridNetwork reconcile annotation: {error}"));
        }
    }
    for (site, previous) in &original.reconcile_annotations {
        match current_reconcile_annotation(site) {
            Ok(actual) if actual.as_deref() == previous.as_deref() => {},
            Ok(actual) => errors.push(format!(
                "{site} GridNetwork reconcile annotation remained {actual:?}, expected {previous:?}"
            )),
            Err(error) => errors.push(format!("verify {site} GridNetwork reconcile annotation: {error}")),
        }
    }

    match wait_for_restored_topology(context, forwards, original, phases, probe_failures, None) {
        Ok(snapshot) => {
            if let Err(error) = record_phase(context, "restoration-verified", &snapshot, None, phases) {
                errors.push(format!("record restored serving state: {error}"));
            }
        },
        Err(error) => errors.push(format!(
            "final serving verification after annotation restoration: {error}"
        )),
    }
    if let Err(error) = verify_simulator_state(context, original) {
        errors.push(format!("verify restored pool-B simulator state: {error}"));
    }
    match read_access_policy() {
        Ok(actual) if actual == original.access_policy => {},
        Ok(actual) => errors.push(format!(
            "pool-B provider accessPolicy was not restored exactly: {actual}"
        )),
        Err(error) => errors.push(format!("verify restored pool-B provider accessPolicy: {error}")),
    }
}

/// Restore both simulator ConfigMaps and each mutated pod-template annotation independently.
fn restore_simulator_state(context: &DemoContext, original: &OriginalState) -> Result<(), Box<dyn std::error::Error>> {
    let peer_context = kind_context(SITE);
    let mut errors = Vec::new();
    for (_, configmap) in SIMULATOR_CONFIGMAPS {
        let Some(config_yaml) = original.simulator_configs.get(*configmap) else {
            errors.push(format!("no saved simulator config for ConfigMap {configmap}"));
            continue;
        };
        let config_patch = json!({"data": {"config.yaml": config_yaml}}).to_string();
        let config = Command::new("kubectl")
            .args([
                "--context",
                &peer_context,
                "-n",
                GRID_SYSTEM_NS,
                "patch",
                "configmap",
                configmap,
                "--type=merge",
                "-p",
                &config_patch,
            ])
            .output();
        match config {
            Ok(output) if output.status.success() => {},
            Ok(output) => errors.push(format!(
                "patch simulator ConfigMap {configmap}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )),
            Err(error) => errors.push(format!("patch simulator ConfigMap {configmap}: {error}")),
        }
    }

    for deployment in SIMULATOR_DEPLOYMENTS {
        match original.simulator_annotations.get(*deployment) {
            Some(annotations) => {
                if let Err(error) = patch_simulator_annotations(&peer_context, deployment, annotations) {
                    errors.push(format!("restore {deployment} simulator annotations: {error}"));
                }
            },
            None => errors.push(format!("no saved simulator annotations for {deployment}")),
        }
        match original.simulator_replicas.get(*deployment) {
            Some(replicas) => {
                if let Err(error) = set_simulator_deployment_replicas(&peer_context, deployment, *replicas) {
                    errors.push(format!("restore {deployment} desired replicas: {error}"));
                }
            },
            None => errors.push(format!("no saved replica count for {deployment}")),
        }
        match kubectl::wait_for_rollout_ns(&peer_context, deployment, GRID_SYSTEM_NS, SITE) {
            Ok(()) => {},
            Err(error) => errors.push(format!("wait for restored {deployment} rollout: {error}")),
        }
    }
    if errors.is_empty() {
        wait_for_original_simulator_readings(context, original)?;
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; ").into())
    }
}

/// Patch only the fake-metric annotations while preserving every unrelated pod-template field.
fn patch_simulator_annotations(
    context: &str,
    deployment: &str,
    annotations: &BTreeMap<String, Option<String>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut values = Map::new();
    for (key, value) in annotations {
        values.insert(key.clone(), value.clone().map_or(Value::Null, Value::String));
    }
    let patch = json!({"spec": {"template": {"metadata": {"annotations": values}}}}).to_string();
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "patch",
            &format!("deployment/{deployment}"),
            "--type=merge",
            "-p",
            &patch,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "patch deployment/{deployment}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(())
}

/// Scale one run-owned simulator deployment and wait until Kubernetes reports the exact replica state.
fn set_simulator_deployment_replicas(
    context: &str,
    deployment: &str,
    replicas: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "scale",
            &format!("deployment/{deployment}"),
            &format!("--replicas={replicas}"),
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "scale {deployment} to {replicas} replicas: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }

    let deadline = Instant::now() + Duration::from_secs(120);
    let mut last = String::from("deployment status unavailable");
    while Instant::now() < deadline {
        if let Ok(object) = kube_json(context, "get", &format!("deployment/{deployment}")) {
            let desired = object.pointer("/spec/replicas").and_then(Value::as_u64).unwrap_or(1);
            let generation = object
                .pointer("/metadata/generation")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let observed_generation = object
                .pointer("/status/observedGeneration")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let actual = object
                .pointer("/status/replicas")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let ready = object
                .pointer("/status/readyReplicas")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let updated = object
                .pointer("/status/updatedReplicas")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            last = format!(
                "desired={desired}, replicas={actual}, ready={ready}, updated={updated}, generation={generation}/{observed_generation}"
            );
            if desired == replicas
                && actual == replicas
                && ready == replicas
                && updated == replicas
                && observed_generation >= generation
            {
                return Ok(());
            }
        }
        std::thread::sleep(POLL);
    }
    Err(format!("{deployment} did not reach replicas={replicas} within 120 seconds: {last}").into())
}

/// Wait for the exact pre-proof EPP metrics after restoring simulator configuration.
fn wait_for_original_simulator_readings(
    context: &DemoContext,
    original: &OriginalState,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + SIMULATOR_RESTORE_TIMEOUT;
    let mut last = String::from("no EPP metrics sample");
    while Instant::now() < deadline {
        match kubectl_exec_epp_metrics(SITE, context.metrics_transport == MetricsTransport::MtlsProxy)
            .and_then(|text| parse_epp_readings(&text))
        {
            Ok(actual) if epp_readings_match(&actual, &original.simulator_readings) => return Ok(()),
            Ok(actual) => {
                last = format!(
                    "restored metrics differ: expected {:?}, got {:?}",
                    epp_value(&original.simulator_readings),
                    epp_value(&actual)
                );
            },
            Err(error) => last = error.to_string(),
        }
        std::thread::sleep(POLL);
    }
    Err(format!("simulator metrics did not return to their original state: {last}").into())
}

/// Verify the restored simulator ConfigMap, annotation values, and EPP readings.
fn verify_simulator_state(context: &DemoContext, original: &OriginalState) -> Result<(), Box<dyn std::error::Error>> {
    let peer_context = kind_context(SITE);
    for (_, configmap) in SIMULATOR_CONFIGMAPS {
        let actual = get_configmap_data_key(&peer_context, GRID_SYSTEM_NS, configmap, "config.yaml")?;
        let expected = original
            .simulator_configs
            .get(*configmap)
            .ok_or_else(|| format!("saved simulator config for {configmap} is missing"))?;
        if &actual != expected {
            return Err(format!("simulator ConfigMap {configmap} config.yaml differs from its original text").into());
        }
    }
    for deployment in SIMULATOR_DEPLOYMENTS {
        let object = kube_json(&peer_context, "get", &format!("deployment/{deployment}"))?;
        let actual_replicas = object.pointer("/spec/replicas").and_then(Value::as_u64).unwrap_or(1);
        let expected_replicas = original
            .simulator_replicas
            .get(*deployment)
            .copied()
            .ok_or("saved simulator replica count is missing")?;
        if actual_replicas != expected_replicas {
            return Err(format!(
                "{deployment} desired replicas were not restored: expected {expected_replicas}, got {actual_replicas}"
            )
            .into());
        }
        let actual = object
            .pointer("/spec/template/metadata/annotations")
            .and_then(Value::as_object);
        let expected = original
            .simulator_annotations
            .get(*deployment)
            .ok_or("saved simulator annotation map is missing")?;
        for (key, value) in expected {
            let actual_value = actual
                .and_then(|annotations| annotations.get(key))
                .and_then(Value::as_str);
            if actual_value != value.as_deref() {
                return Err(format!(
                    "deployment/{deployment} annotation {key} is {actual_value:?}, expected {value:?}"
                )
                .into());
            }
        }
    }
    let metrics = kubectl_exec_epp_metrics(SITE, context.metrics_transport == MetricsTransport::MtlsProxy)?;
    let readings = parse_epp_readings(&metrics)?;
    if !epp_readings_match(&readings, &original.simulator_readings) {
        return Err(format!(
            "restored EPP readings differ from original state: expected {:?}, got {:?}",
            epp_value(&original.simulator_readings),
            epp_value(&readings)
        )
        .into());
    }
    Ok(())
}

/// Compare every queue, readiness, and held-work reading while allowing no missing series.
fn epp_readings_match(actual: &EppReadings, expected: &EppReadings) -> bool {
    (actual.running - expected.running).abs() < f64::EPSILON
        && (actual.queue - expected.queue).abs() < f64::EPSILON
        && (actual.kv_cache_utilization - expected.kv_cache_utilization).abs() < f64::EPSILON
        && (actual.kv_cache_spread.unwrap_or(0.0) - expected.kv_cache_spread.unwrap_or(0.0)).abs() < f64::EPSILON
        && (actual.ready - expected.ready).abs() < f64::EPSILON
        && actual.per_endpoint_queues == expected.per_endpoint_queues
        && actual.held_queue_count == expected.held_queue_count
        && (actual.held_capacity_utilization - expected.held_capacity_utilization).abs() < f64::EPSILON
}

/// Wait for the original operator topology, Gateway mount, and same Gateway pod after restoration.
#[expect(
    clippy::too_many_arguments,
    reason = "restoration verification records the original state and any failed serving probes"
)]
fn wait_for_restored_topology(
    context: &DemoContext,
    forwards: &PortForwards,
    original: &OriginalState,
    phases: &mut Vec<Value>,
    probe_failures: &mut Vec<Value>,
    before_restore: Option<&RuntimeSnapshot>,
) -> Result<RuntimeSnapshot, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + TRANSITION_TIMEOUT;
    let expected = candidate_set(&original.baseline.candidates);
    let mut stable_since = None;
    let mut stable_samples = 0_u32;
    let mut last = String::from("no runtime sample");
    while Instant::now() < deadline {
        match observe_runtime(context, forwards) {
            Ok(snapshot) => {
                last = snapshot_summary(&snapshot);
                let stable = snapshot.gateway_uid == original.baseline.gateway_uid
                    && candidate_set(&snapshot.candidates) == expected
                    && candidate_set(&snapshot.mounted_candidates) == expected
                    && snapshot.operator_digest == snapshot.operator_digest_annotation
                    && snapshot.mounted_digest == snapshot.operator_digest
                    && before_restore.is_none_or(|previous| {
                        snapshot.operator_digest == previous.operator_digest
                            || snapshot.gateway_reloads > previous.gateway_reloads
                    })
                    && original_signals_restored(&snapshot, original);
                if stable {
                    let response =
                        match send_probe(forwards.gateway_http, None, "restoration-serving-probe", probe_failures) {
                            Ok(response) => response,
                            Err(error) => {
                                last = format!("restoration serving probe failed: {error}");
                                stable_since = None;
                                stable_samples = 0;
                                std::thread::sleep(POLL);
                                continue;
                            },
                        };
                    let post_request = observe_runtime(context, forwards)?;
                    if post_request.gateway_uid == original.baseline.gateway_uid
                        && candidate_set(&post_request.candidates) == expected
                        && candidate_set(&post_request.mounted_candidates) == expected
                        && post_request.operator_digest == post_request.operator_digest_annotation
                        && post_request.mounted_digest == post_request.operator_digest
                        && before_restore.is_none_or(|previous| {
                            post_request.operator_digest == previous.operator_digest
                                || post_request.gateway_reloads > previous.gateway_reloads
                        })
                        && original_signals_restored(&post_request, original)
                    {
                        let response_evidence = response_value(&response);
                        record_phase(
                            context,
                            "restoration-serving-probe",
                            &post_request,
                            Some(&response_evidence),
                            phases,
                        )?;
                        stable_since.get_or_insert_with(Instant::now);
                        stable_samples = stable_samples.saturating_add(1);
                        if stable_samples >= 3 && stable_since.is_some_and(|start| start.elapsed() >= STABLE_SPAN) {
                            return Ok(post_request);
                        }
                    } else {
                        last = format!(
                            "state changed after restoration request: {}",
                            snapshot_summary(&post_request)
                        );
                        stable_since = None;
                        stable_samples = 0;
                    }
                } else {
                    stable_since = None;
                    stable_samples = 0;
                }
            },
            Err(error) => {
                last = error.to_string();
                stable_since = None;
                stable_samples = 0;
            },
        }
        std::thread::sleep(POLL);
    }
    let failure = format!("timed out restoring the original serving topology: {last}");
    if let Ok(snapshot) = observe_runtime(context, forwards)
        && let Err(error) = record_phase(context, "restoration-timeout-capture", &snapshot, None, phases)
    {
        return Err(format!("{failure}; could not save timeout snapshot: {error}").into());
    }
    Err(failure.into())
}

/// Require the original EPP and operator sample values after the simulator settings are restored.
fn original_signals_restored(snapshot: &RuntimeSnapshot, original: &OriginalState) -> bool {
    let local_in_flight_restored = match (snapshot.local_in_flight, original.baseline.local_in_flight) {
        (Some(actual), Some(expected)) => (actual - expected).abs() <= 0.01,
        _ => false,
    };
    let local_ready_restored = match (snapshot.local_ready, original.baseline.local_ready) {
        (Some(actual), Some(expected)) => (actual - expected).abs() <= f64::EPSILON,
        _ => false,
    };
    epp_readings_match(&snapshot.epp, &original.simulator_readings)
        && (snapshot.origin_in_flight - original.baseline.origin_in_flight).abs() <= 0.01
        && (snapshot.origin_ready - original.baseline.origin_ready).abs() <= f64::EPSILON
        && local_in_flight_restored
        && local_ready_restored
}

/// Restore or remove only the harness reconciliation annotation on one run-owned network.
fn restore_reconcile_annotation(site: &str, previous: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let context = kind_context(site);
    let mut annotations = Map::new();
    annotations.insert(
        METRICS_REFRESH_ANNOTATION.to_owned(),
        previous.map_or(Value::Null, |value| Value::String(value.to_owned())),
    );
    let patch = json!({"metadata": {"annotations": annotations}}).to_string();
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "patch",
            "gridnetwork",
            GRID_NETWORK_NAME,
            "--type=merge",
            "-p",
            &patch,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "restore {site} GridNetwork annotation: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(())
}

/// Read the current harness reconciliation annotation from one GridNetwork.
fn current_reconcile_annotation(site: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let network = kube_json(&kind_context(site), "get", &format!("gridnetwork/{GRID_NETWORK_NAME}"))?;
    Ok(annotation_value(&network, METRICS_REFRESH_ANNOTATION))
}

/// Read a string value from `metadata.annotations` without interpreting slashes in annotation keys.
fn annotation_value(object: &Value, name: &str) -> Option<String> {
    object
        .pointer("/metadata/annotations")
        .and_then(Value::as_object)
        .and_then(|annotations| annotations.get(name))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// Require both semantic states, a changed operator revision, mount equality, and a Gateway apply.
fn require_topology_transition(
    previous: &RuntimeSnapshot,
    current: &RuntimeSnapshot,
    pair_present_expected: bool,
    expected_candidates: &BTreeSet<CandidatePair>,
) -> bool {
    candidate_set(&current.candidates) == *expected_candidates
        && candidate_set(&current.mounted_candidates) == *expected_candidates
        && pair_present(&current.candidates) == pair_present_expected
        && pair_present(&current.mounted_candidates) == pair_present_expected
        && current.operator_digest != previous.operator_digest
        && current.operator_resource_version != previous.operator_resource_version
        && current.operator_digest_annotation == current.operator_digest
        && current.mounted_digest == current.operator_digest
        && current.gateway_reloads > previous.gateway_reloads
        && if pair_present_expected {
            current.ceiling.is_some_and(f64::is_finite)
        } else {
            current.ceiling_is_nan
        }
}

/// Check whether the old learned ceiling was cleared and the returned pair learned a lower value.
fn lifecycle_ceiling_sequence_valid(
    high: f64,
    idle: f64,
    removed_raw: Option<&str>,
    returned: f64,
    measured_low: f64,
) -> bool {
    high > DEFAULT_CEILING_FLOOR + CEILING_MARGIN
        && (idle - high).abs() < f64::EPSILON
        && removed_raw.is_some_and(|raw| raw.eq_ignore_ascii_case("nan"))
        && returned_ceiling_is_valid(high, returned, measured_low)
}

/// Require the returned ceiling to match the floor-or-current-load and reject retained high state.
fn returned_ceiling_is_valid(high: f64, returned: f64, measured_low: f64) -> bool {
    if !high.is_finite() || !returned.is_finite() || !measured_low.is_finite() {
        return false;
    }
    let expected = DEFAULT_CEILING_FLOOR.max(measured_low);
    high > DEFAULT_CEILING_FLOOR + CEILING_MARGIN
        && returned < high - CEILING_MARGIN
        && (returned - expected).abs() <= RETURNED_CEILING_TOLERANCE
}

/// Require a specific pair in one serving candidate list.
fn require_pair(candidates: &[CandidatePair], source: &str) -> Result<(), Box<dyn std::error::Error>> {
    if pair_present(candidates) {
        Ok(())
    } else {
        Err(format!("exact candidate pair ({SITE}, {CLUSTER}) is absent from {source}: {candidates:?}").into())
    }
}

/// Whether a candidate list contains the exact expected site and cluster.
fn pair_present(candidates: &[CandidatePair]) -> bool {
    candidates
        .iter()
        .any(|(site, cluster)| site == SITE && cluster == CLUSTER)
}

/// Parse and sort exact site/cluster pairs from one serving JSON document.
fn candidate_pairs(config: &Value) -> Result<Vec<CandidatePair>, Box<dyn std::error::Error>> {
    let items = config
        .get("candidates")
        .and_then(Value::as_array)
        .ok_or("serving config has no candidates array")?;
    let mut candidates = Vec::with_capacity(items.len());
    for item in items {
        let site = item
            .get("site")
            .and_then(Value::as_str)
            .ok_or("serving candidate has no site")?;
        let cluster = item
            .get("cluster")
            .and_then(Value::as_str)
            .ok_or("serving candidate has no cluster")?;
        candidates.push((site.to_owned(), cluster.to_owned()));
    }
    candidates.sort();
    Ok(candidates)
}

/// Make a stable set for exact topology comparisons.
fn candidate_set(candidates: &[CandidatePair]) -> BTreeSet<CandidatePair> {
    candidates.iter().cloned().collect()
}

/// Require identical operator and mounted contents before using a serving revision.
fn require_same_gateway_config(snapshot: &RuntimeSnapshot) -> Result<(), Box<dyn std::error::Error>> {
    if gateway_config_values_match(
        &snapshot.operator_digest,
        &snapshot.operator_digest_annotation,
        &snapshot.mounted_digest,
        &snapshot.operator_config,
        &snapshot.mounted_config,
    ) {
        Ok(())
    } else {
        Err(format!(
            "Gateway mount does not match operator config: operator={} mounted={}",
            snapshot.operator_digest, snapshot.mounted_digest
        )
        .into())
    }
}

/// Check that the mounted bytes and content-addressed revision match the current operator config.
fn gateway_config_values_match(
    operator_digest: &str,
    annotation_digest: &str,
    mounted_digest: &str,
    operator_config: &str,
    mounted_config: &str,
) -> bool {
    operator_digest == annotation_digest && operator_digest == mounted_digest && operator_config == mounted_config
}

/// Require a finite metric sample with one exact label set.
fn required_metric(
    text: &str,
    name: &str,
    labels: &[(&str, &str)],
    description: &str,
) -> Result<f64, Box<dyn std::error::Error>> {
    let value = metric_sample(text, name, labels)
        .ok_or_else(|| format!("required metric {name} {labels:?} for {description} is missing"))?
        .1
        .parse::<f64>()?;
    if !value.is_finite() {
        return Err(format!("required metric {name} for {description} is not finite: {value}").into());
    }
    Ok(value)
}

/// Sum accepted Gateway config-watcher observations, whether a config was applied or reused.
fn gateway_config_observation_count(text: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let mut found_accepted_result = false;
    let mut observations = 0.0;
    for result in ["applied", "reused"] {
        if let Some((_, raw)) = metric_sample(text, "grid_serving_config_reload_total", &[("result", result)]) {
            found_accepted_result = true;
            let count = raw.parse::<f64>()?;
            if !count.is_finite() || count < 0.0 {
                return Err(format!("Gateway config-watcher counter for result={result} is invalid: {count}").into());
            }
            observations += count;
        }
    }
    if !found_accepted_result {
        return Err("Gateway has no applied or reused config-watcher counter".into());
    }
    if !observations.is_finite() || observations <= 0.0 {
        return Err(
            format!("Gateway has no accepted config-watcher observation: applied + reused = {observations}").into(),
        );
    }
    Ok(observations)
}

/// Read one optional Gateway gauge for the exact pool-B candidate pair.
fn optional_metric_value(text: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    metric_sample(text, name, labels)
        .and_then(|(_, raw)| raw.parse::<f64>().ok())
        .filter(|value| value.is_finite())
}

/// Require one operator series labeled for pool-B's exact provider identity.
fn required_operator_sample(text: &str, name: &str, description: &str) -> Result<f64, Box<dyn std::error::Error>> {
    required_metric(
        text,
        name,
        &[("grid_site", SITE), ("grid_provider", CLUSTER)],
        description,
    )
}

/// Read an optional operator series labeled for pool-B's exact provider identity.
fn optional_operator_sample(
    text: &str,
    name: &str,
    description: &str,
) -> Result<Option<f64>, Box<dyn std::error::Error>> {
    let Some((_, raw)) = metric_sample(text, name, &[("grid_site", SITE), ("grid_provider", CLUSTER)]) else {
        return Ok(None);
    };
    let value = raw.parse::<f64>()?;
    if !value.is_finite() {
        return Err(format!("operator metric {name} for {description} is not finite: {value}").into());
    }
    Ok(Some(value))
}

/// Find one exact Prometheus metric sample matching every requested label.
fn metric_sample<'metrics>(
    text: &'metrics str,
    name: &str,
    labels: &[(&str, &str)],
) -> Option<(&'metrics str, &'metrics str)> {
    text.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let series = fields.next()?;
        let metric_name = series.split('{').next()?;
        if metric_name != name
            || !labels
                .iter()
                .all(|(key, value)| series.contains(&format!("{key}=\"{value}\"")))
        {
            return None;
        }
        Some((series, fields.next()?))
    })
}

/// Collect every numeric sample of one metric with the required labels.
fn metric_values(text: &str, name: &str, labels: &[(&str, &str)]) -> Vec<f64> {
    text.lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let series = fields.next()?;
            let metric_name = series.split('{').next()?;
            if metric_name == name
                && labels
                    .iter()
                    .all(|(key, value)| series.contains(&format!("{key}=\"{value}\"")))
            {
                fields.next()?.parse::<f64>().ok()
            } else {
                None
            }
        })
        .collect()
}

/// Select safe EPP lines for this pool and the metric families needed by the proof.
fn relevant_epp_metric_lines(text: &str) -> String {
    text.lines()
        .filter(|line| {
            !line.starts_with('#')
                && (line.contains("name=\"pool-b\"") || line.contains("inference_pool=\"pool-b\""))
                && (line.starts_with(RUNNING_METRIC)
                    || line.starts_with(QUEUE_METRIC)
                    || line.starts_with(KV_CACHE_METRIC)
                    || line.starts_with(KV_CACHE_SPREAD_METRIC)
                    || line.starts_with(READY_METRIC)
                    || line.starts_with(PER_ENDPOINT_QUEUE_METRIC)
                    || line.starts_with(HELD_QUEUE_METRIC)
                    || line.starts_with(HELD_CAPACITY_METRIC))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Select operator lines for the exact remote provider without copying unrelated metrics.
fn relevant_operator_metric_lines(text: &str) -> String {
    text.lines()
        .filter(|line| {
            !line.starts_with('#')
                && line.contains(&format!("grid_site=\"{SITE}\""))
                && line.contains(&format!("grid_provider=\"{CLUSTER}\""))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Select serving-config, signal-poll, and per-site ceiling series from live Gateway metrics.
fn relevant_gateway_metric_lines(text: &str) -> String {
    text.lines()
        .filter(|line| {
            !line.starts_with('#')
                && (line.starts_with("grid_serving_config_reload_total")
                    || line.starts_with("grid_signals_")
                    || line.starts_with("grid_route_site_")
                    || line.starts_with("grid_route_snapshot_"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Parse one required unsigned serving-config field.
fn required_unsigned(config: &Value, pointer: &str, name: &str) -> Result<u64, Box<dyn std::error::Error>> {
    config
        .pointer(pointer)
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("serving config has no positive {name}").into())
}

/// Read one namespaced Kubernetes JSON object.
fn kube_json(context: &str, verb: &str, resource: &str) -> Result<Value, Box<dyn std::error::Error>> {
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

/// Hash one exact UTF-8 serving document using the operator's sha256 revision format.
fn sha256_text(text: &str) -> String {
    hex::encode(Sha256::digest(text.as_bytes()))
}

/// Convert a digest to a filesystem-safe suffix.
fn safe_digest(digest: &str) -> String {
    digest
        .strip_prefix("sha256:")
        .unwrap_or(digest)
        .chars()
        .filter(char::is_ascii_hexdigit)
        .collect()
}

/// Render a compact last-observation summary for timeout diagnostics.
fn snapshot_summary(snapshot: &RuntimeSnapshot) -> String {
    format!(
        "revision={} rv={} mounted={} candidates={:?} running={} queue={} kv_cache={}+/-{:?} ready={} per_endpoint={:?} held={:?}/{} origin={} relay={:?} reloads={} ceiling={:?}/{:?} soft={:?} hard={:?} rho_reported={:?} rho_counted={:?} in_flight={:?} pod={}",
        snapshot.operator_digest,
        snapshot.operator_resource_version,
        snapshot.mounted_digest,
        snapshot.candidates,
        snapshot.epp.running,
        snapshot.epp.queue,
        snapshot.epp.kv_cache_utilization,
        snapshot.epp.kv_cache_spread,
        snapshot.epp.ready,
        snapshot.epp.per_endpoint_queues,
        snapshot.epp.held_queue_count,
        snapshot.epp.held_capacity_utilization,
        snapshot.origin_in_flight,
        snapshot.local_in_flight,
        snapshot.gateway_reloads,
        snapshot.ceiling,
        snapshot.ceiling_raw,
        snapshot.gateway_capacity.soft_ceiling,
        snapshot.gateway_capacity.hard_ceiling,
        snapshot.gateway_capacity.reported_saturation,
        snapshot.gateway_capacity.counted_saturation,
        snapshot.gateway_capacity.in_flight,
        snapshot.gateway_uid
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{
        EppReadings, GatewayCapacityMetrics, HELD_CAPACITY_METRIC, HIGH_RUNNING_REQUESTS, KV_CACHE_METRIC,
        KV_CACHE_SIGNAL_TOLERANCE, KV_CACHE_SPREAD_METRIC, KV_HARD_BOUND_CACHE_MEAN, KV_HARD_BOUND_CACHE_SPREAD,
        KV_HARD_BOUND_ROUTE_REQUESTS, KV_HARD_BOUND_WAITING_REQUESTS, KV_REPORTED_ONLY_WAITING_REQUESTS,
        LOW_RUNNING_REQUESTS, MIN_REPORTED_KV_POOL_B_SHARE_REDUCTION, MIN_SOFT_BASELINE_POOL_B_SHARE, QUEUE_METRIC,
        READY_METRIC, RUN_EXCLUSION_LABEL, RUNNING_METRIC, RouteDistribution, SITE, access_policy_json_patch,
        capacity_metrics_are_cleared, counted_saturation_is_absent, expected_kv_cache_origin_in_flight,
        gateway_config_observation_count, gateway_config_values_match, hard_bound_without_soft_is_valid,
        hard_only_epp_in_flight, idle_ceiling_is_retained, kv_cache_hard_bound_is_valid,
        kv_cache_reported_only_route_share_shift, kv_cache_route_share_shift, lifecycle_ceiling_sequence_valid,
        local_relay_matches, metric_values, no_waiting_or_held_work, parse_epp_readings, policy_with_run_exclusion,
        probe_failure_value, qualification_succeeded, reported_only_kv_cache_is_valid, returned_ceiling_is_valid,
        validate_response_attribution,
    };

    fn route_distribution(pool_a: usize, pool_b: usize) -> RouteDistribution {
        RouteDistribution {
            sites: BTreeMap::from([("pool-a".to_owned(), pool_a), (SITE.to_owned(), pool_b)]),
            ..RouteDistribution::default()
        }
    }

    #[test]
    fn pinned_epp_parser_requires_running_queue_readiness_per_unit_and_flow_control_observations() {
        let text = format!(
            "{RUNNING_METRIC}{{name=\"pool-b\"}} 20\n{QUEUE_METRIC}{{name=\"pool-b\"}} 0\n{KV_CACHE_METRIC}{{name=\"pool-b\"}} 0\n{KV_CACHE_SPREAD_METRIC}{{name=\"pool-b\"}} 0\n{READY_METRIC}{{name=\"pool-b\"}} 2\nllm_d_epp_per_endpoint_queue_size{{model_server_endpoint=\"vcr-1\",name=\"pool-b\"}} 0\nllm_d_epp_per_endpoint_queue_size{{model_server_endpoint=\"vcr-2\",name=\"pool-b\"}} 0\n{HELD_CAPACITY_METRIC}{{inference_pool=\"pool-b\",priority=\"0\"}} 0\n"
        );
        let parsed = parse_epp_readings(&text).unwrap_or_else(|_| std::process::abort());
        assert!(
            (parsed.running - f64::from(HIGH_RUNNING_REQUESTS)).abs() < f64::EPSILON,
            "running metric should preserve the pinned EPP value"
        );
        assert!(
            parsed.queue.abs() < f64::EPSILON,
            "average queue must be observed as zero"
        );
        assert!(
            parsed.kv_cache_utilization.abs() < f64::EPSILON,
            "average KV-cache utilization must be observed"
        );
        assert_eq!(
            parsed.kv_cache_spread,
            Some(0.0),
            "KV-cache spread should be retained when present"
        );
        assert!(
            (parsed.ready - 2.0).abs() < f64::EPSILON,
            "the ready endpoint count must be present"
        );
        assert_eq!(
            parsed.per_endpoint_queues,
            vec![0.0, 0.0],
            "every endpoint queue must be observed"
        );
        assert!(
            parsed.held_capacity_utilization.abs() < f64::EPSILON,
            "flow-control capacity metric must prove there is no held work"
        );
        assert!(
            parsed.held_queue_count.is_none(),
            "an absent held-count series must remain unknown"
        );
        assert_eq!(
            parsed.held_source, HELD_CAPACITY_METRIC,
            "evidence should name the present held-work signal"
        );
    }

    #[test]
    fn pinned_epp_parser_preserves_asymmetric_kv_cache_mean_and_spread() {
        let text = format!(
            "{RUNNING_METRIC}{{name=\"pool-b\"}} 8\n{QUEUE_METRIC}{{name=\"pool-b\"}} 0\n{KV_CACHE_METRIC}{{name=\"pool-b\"}} 0.5\n{KV_CACHE_SPREAD_METRIC}{{name=\"pool-b\"}} 0.71\n{READY_METRIC}{{name=\"pool-b\"}} 2\nllm_d_epp_per_endpoint_queue_size{{model_server_endpoint=\"vcr-1\",name=\"pool-b\"}} 0\nllm_d_epp_per_endpoint_queue_size{{model_server_endpoint=\"vcr-2\",name=\"pool-b\"}} 0\n{HELD_CAPACITY_METRIC}{{inference_pool=\"pool-b\",priority=\"0\"}} 0\n"
        );
        let parsed = parse_epp_readings(&text).unwrap_or_else(|_| std::process::abort());

        assert!(
            (parsed.kv_cache_utilization - KV_HARD_BOUND_CACHE_MEAN).abs() < f64::EPSILON,
            "the EPP must retain the configured endpoint mean"
        );
        assert!(
            parsed
                .kv_cache_spread
                .is_some_and(|spread| { (spread - KV_HARD_BOUND_CACHE_SPREAD).abs() <= KV_CACHE_SIGNAL_TOLERANCE }),
            "the pinned EPP's rounded sample standard deviation should match the asymmetric endpoint values"
        );
        assert!(
            expected_kv_cache_origin_in_flight(&parsed, KV_REPORTED_ONLY_WAITING_REQUESTS).is_ok(),
            "the full proof's asymmetric endpoint sample must satisfy its exact EPP stimulus"
        );
    }

    #[test]
    fn idle_ceiling_requires_gateway_zero_in_flight_and_the_learned_ceiling() {
        assert!(
            idle_ceiling_is_retained(Some(0.0), Some(12.0), 12.0),
            "an idle Gateway with the learned ceiling should satisfy the wait"
        );
        assert!(
            !idle_ceiling_is_retained(Some(4.0), Some(12.0), 12.0),
            "a retained ceiling while the Gateway still sees in-flight work is not idle"
        );
        assert!(
            !idle_ceiling_is_retained(None, Some(12.0), 12.0),
            "a missing Gateway in-flight metric must not count as idle"
        );
        assert!(
            !idle_ceiling_is_retained(Some(0.0), Some(11.0), 12.0),
            "a changed ceiling must not satisfy retention"
        );
    }

    #[test]
    fn pinned_epp_parser_does_not_treat_missing_held_observations_as_zero() {
        let text = format!(
            "{RUNNING_METRIC}{{name=\"pool-b\"}} 20\n{QUEUE_METRIC}{{name=\"pool-b\"}} 0\n{KV_CACHE_METRIC}{{name=\"pool-b\"}} 0\n{READY_METRIC}{{name=\"pool-b\"}} 2\nllm_d_epp_per_endpoint_queue_size{{model_server_endpoint=\"vcr-1\",name=\"pool-b\"}} 0\nllm_d_epp_per_endpoint_queue_size{{model_server_endpoint=\"vcr-2\",name=\"pool-b\"}} 0\n"
        );
        assert!(
            parse_epp_readings(&text).is_err(),
            "missing flow-control observation must fail closed"
        );
    }

    #[test]
    fn pinned_epp_parser_requires_a_pool_kv_cache_observation() {
        let text = format!(
            "{RUNNING_METRIC}{{name=\"pool-b\"}} 8\n{QUEUE_METRIC}{{name=\"pool-b\"}} 1\n{READY_METRIC}{{name=\"pool-b\"}} 2\nllm_d_epp_per_endpoint_queue_size{{model_server_endpoint=\"vcr-1\",name=\"pool-b\"}} 1\nllm_d_epp_per_endpoint_queue_size{{model_server_endpoint=\"vcr-2\",name=\"pool-b\"}} 1\n{HELD_CAPACITY_METRIC}{{inference_pool=\"pool-b\",priority=\"0\"}} 0\n"
        );
        assert!(
            parse_epp_readings(&text).is_err(),
            "a missing provider-reported KV-cache reading must not be treated as zero saturation"
        );
    }

    #[test]
    fn held_queue_metric_requires_exact_pool_labels() {
        let text = "llm_d_epp_flow_control_queue_size{inference_pool=\"pool-b\",priority=\"0\"} 0\nllm_d_epp_flow_control_queue_size{inference_pool=\"pool-a\",priority=\"0\"} 3\n";
        assert_eq!(
            metric_values(
                text,
                "llm_d_epp_flow_control_queue_size",
                &[("inference_pool", "pool-b")]
            ),
            vec![0.0]
        );
        assert_eq!(
            metric_values(
                text,
                "llm_d_epp_flow_control_queue_size",
                &[("inference_pool", "pool-a")]
            ),
            vec![3.0]
        );
        assert!(
            metric_values(
                text,
                "llm_d_epp_flow_control_queue_size",
                &[("inference_pool", "pool-c")]
            )
            .is_empty()
        );
    }

    #[test]
    fn ceiling_learning_rejects_any_fractional_waiting_or_held_work() {
        let clear = EppReadings {
            running: 20.0,
            queue: 0.0,
            kv_cache_utilization: 0.0,
            kv_cache_spread: Some(0.0),
            ready: 2.0,
            per_endpoint_queues: vec![0.0, 0.0],
            held_queue_count: Some(0.0),
            held_source: "test".to_owned(),
            held_capacity_utilization: 0.0,
        };
        assert!(
            no_waiting_or_held_work(&clear),
            "zero readings should permit ceiling learning"
        );

        let average_waiting = EppReadings {
            queue: 0.5,
            ..clear.clone()
        };
        assert!(
            !no_waiting_or_held_work(&average_waiting),
            "a fractional average queue still represents waiting work"
        );

        let endpoint_waiting = EppReadings {
            per_endpoint_queues: vec![0.5, 0.0],
            ..clear.clone()
        };
        assert!(
            !no_waiting_or_held_work(&endpoint_waiting),
            "a fractional per-endpoint queue still represents waiting work"
        );

        let held_queue = EppReadings {
            held_queue_count: Some(0.5),
            ..clear.clone()
        };
        assert!(
            !no_waiting_or_held_work(&held_queue),
            "a fractional held queue still represents held work"
        );

        let held_capacity = EppReadings {
            held_capacity_utilization: f64::EPSILON,
            ..clear
        };
        assert!(
            !no_waiting_or_held_work(&held_capacity),
            "nonzero held request capacity must not be treated as empty"
        );
    }

    #[test]
    fn saturated_kv_cache_must_teach_a_hard_ceiling_below_the_soft_bound() {
        let epp = EppReadings {
            running: 8.0,
            queue: f64::from(KV_HARD_BOUND_WAITING_REQUESTS),
            kv_cache_utilization: KV_HARD_BOUND_CACHE_MEAN,
            kv_cache_spread: Some(KV_HARD_BOUND_CACHE_SPREAD),
            ready: 2.0,
            per_endpoint_queues: vec![1.0, 1.0],
            held_queue_count: Some(0.0),
            held_source: HELD_CAPACITY_METRIC.to_owned(),
            held_capacity_utilization: 0.0,
        };
        let gateway = GatewayCapacityMetrics {
            in_flight: Some(18.0),
            soft_ceiling: Some(40.0),
            hard_ceiling: Some(18.0),
            reported_saturation: Some(1.0),
            counted_saturation: Some(1.0),
        };
        assert!(
            kv_cache_hard_bound_is_valid(&epp, gateway, Some(18.0), 40.0).is_ok(),
            "the live Gateway should learn a lower hard bound from saturated KV with one waiting request per endpoint and no held work"
        );
        let queue_free = EppReadings {
            queue: 0.0,
            per_endpoint_queues: vec![0.0, 0.0],
            ..epp.clone()
        };
        assert!(
            kv_cache_hard_bound_is_valid(&queue_free, gateway, Some(18.0), 40.0).is_err(),
            "a queue-free saturated KV sample must not be mistaken for the backlogged hard-bound proof"
        );
        let fractional_backlog = EppReadings {
            queue: 0.5,
            per_endpoint_queues: vec![0.5, 0.5],
            ..epp.clone()
        };
        assert!(
            kv_cache_hard_bound_is_valid(&fractional_backlog, gateway, Some(18.0), 40.0).is_err(),
            "fractional waiting must not satisfy the one-request backlog threshold"
        );
        assert!(
            kv_cache_hard_bound_is_valid(
                &epp,
                GatewayCapacityMetrics {
                    reported_saturation: Some(KV_HARD_BOUND_CACHE_MEAN),
                    ..gateway
                },
                Some(16.0),
                40.0,
            )
            .is_err(),
            "the Gateway must include endpoint spread instead of reporting only the EPP mean"
        );
        let missing_spread = EppReadings {
            kv_cache_spread: None,
            ..epp.clone()
        };
        assert!(
            kv_cache_hard_bound_is_valid(&missing_spread, gateway, Some(16.0), 40.0).is_err(),
            "the EPP sample must expose the spread used to derive provider-reported saturation"
        );
        let zero_spread = EppReadings {
            kv_cache_spread: Some(0.0),
            ..epp.clone()
        };
        assert!(
            kv_cache_hard_bound_is_valid(&zero_spread, gateway, Some(16.0), 40.0).is_err(),
            "a same-value KV-cache sample must not satisfy the nonzero-spread proof"
        );
        assert!(
            kv_cache_hard_bound_is_valid(
                &epp,
                GatewayCapacityMetrics {
                    hard_ceiling: None,
                    ..gateway
                },
                Some(40.0),
                40.0,
            )
            .is_err(),
            "the old soft ceiling must not pass when no hard bound was learned"
        );
        assert!(
            kv_cache_hard_bound_is_valid(
                &epp,
                GatewayCapacityMetrics {
                    hard_ceiling: Some(40.0),
                    ..gateway
                },
                Some(40.0),
                40.0,
            )
            .is_err(),
            "a retained old ceiling must not pass as the newly demonstrated hard limit"
        );
    }

    #[test]
    fn provider_reported_kv_saturation_drives_rho_without_waiting_or_a_hard_bound() {
        let epp = EppReadings {
            running: f64::from(LOW_RUNNING_REQUESTS),
            queue: 0.0,
            kv_cache_utilization: KV_HARD_BOUND_CACHE_MEAN,
            kv_cache_spread: Some(KV_HARD_BOUND_CACHE_SPREAD),
            ready: 2.0,
            per_endpoint_queues: vec![0.0, 0.0],
            held_queue_count: Some(0.0),
            held_source: HELD_CAPACITY_METRIC.to_owned(),
            held_capacity_utilization: 0.0,
        };
        let gateway = GatewayCapacityMetrics {
            in_flight: Some(4.0),
            soft_ceiling: Some(40.0),
            hard_ceiling: None,
            reported_saturation: Some(1.0),
            counted_saturation: None,
        };
        assert!(
            reported_only_kv_cache_is_valid(&epp, gateway, Some(40.0), 0.95, 40.0).is_ok(),
            "the full provider KV reading should drive route rho even though request-count saturation is absent"
        );
        assert!(
            reported_only_kv_cache_is_valid(
                &epp,
                GatewayCapacityMetrics {
                    reported_saturation: Some(KV_HARD_BOUND_CACHE_MEAN),
                    ..gateway
                },
                Some(40.0),
                0.95,
                40.0,
            )
            .is_err(),
            "the report must include endpoint spread instead of only the pool mean"
        );
        assert!(
            reported_only_kv_cache_is_valid(
                &epp,
                GatewayCapacityMetrics {
                    hard_ceiling: Some(4.0),
                    counted_saturation: Some(1.0),
                    ..gateway
                },
                Some(4.0),
                0.95,
                40.0,
            )
            .is_err(),
            "this phase must not pass because a queue-derived hard bound made the count saturated"
        );
        assert!(
            reported_only_kv_cache_is_valid(&epp, gateway, Some(40.0), 0.5, 40.0).is_err(),
            "the combined route rho must reflect provider KV saturation"
        );
        assert!(
            reported_only_kv_cache_is_valid(
                &EppReadings {
                    queue: 1.0,
                    per_endpoint_queues: vec![0.0, 1.0],
                    ..epp
                },
                gateway,
                Some(40.0),
                0.95,
                40.0,
            )
            .is_err(),
            "waiting work must not be used to satisfy the KV-only route proof"
        );
    }

    #[test]
    fn provider_reported_kv_route_share_requires_a_complete_material_shift() {
        let low_pressure = route_distribution(408, 1640);
        let reported_saturated = route_distribution(680, 1368);
        let (low_share, saturated_share) =
            kv_cache_reported_only_route_share_shift(&low_pressure, &reported_saturated, KV_HARD_BOUND_ROUTE_REQUESTS)
                .unwrap_or_else(|_| std::process::abort());
        assert!(low_share >= MIN_SOFT_BASELINE_POOL_B_SHARE);
        assert!(low_share - saturated_share >= MIN_REPORTED_KV_POOL_B_SHARE_REDUCTION);

        assert!(
            kv_cache_reported_only_route_share_shift(&low_pressure, &low_pressure, KV_HARD_BOUND_ROUTE_REQUESTS,)
                .is_err(),
            "unchanged traffic share must not pass the provider-saturation proof"
        );
        assert!(
            kv_cache_reported_only_route_share_shift(
                &low_pressure,
                &route_distribution(571, 1477),
                KV_HARD_BOUND_ROUTE_REQUESTS,
            )
            .is_err(),
            "a shift smaller than the required eight percentage points must fail"
        );
        assert!(
            kv_cache_reported_only_route_share_shift(
                &low_pressure,
                &reported_saturated,
                KV_HARD_BOUND_ROUTE_REQUESTS + 1,
            )
            .is_err(),
            "incomplete traffic samples must not be compared as if they were complete"
        );
    }

    #[test]
    fn saturated_kv_cache_can_teach_a_hard_ceiling_without_a_soft_sample() {
        let epp = EppReadings {
            running: 8.0,
            queue: f64::from(KV_HARD_BOUND_WAITING_REQUESTS),
            kv_cache_utilization: 1.0,
            kv_cache_spread: None,
            ready: 1.0,
            per_endpoint_queues: vec![1.0],
            held_queue_count: Some(0.0),
            held_source: HELD_CAPACITY_METRIC.to_owned(),
            held_capacity_utilization: 0.0,
        };
        let hard_only_gateway = GatewayCapacityMetrics {
            in_flight: Some(9.0),
            soft_ceiling: None,
            hard_ceiling: Some(9.0),
            reported_saturation: Some(1.0),
            counted_saturation: Some(1.0),
        };

        assert!(
            hard_only_epp_in_flight(&epp).is_ok_and(|in_flight| (in_flight - 9.0).abs() < f64::EPSILON),
            "the full KV-cache sample uses running and waiting work without held requests"
        );
        assert!(
            hard_bound_without_soft_is_valid(hard_only_gateway, Some(9.0), 9.0).is_ok(),
            "a saturated KV sample with a backlog should establish the effective hard ceiling without any soft value"
        );
        assert!(
            hard_only_epp_in_flight(&EppReadings {
                queue: 0.0,
                per_endpoint_queues: vec![0.0],
                ..epp.clone()
            })
            .is_err(),
            "the hard-only proof must fail when no request is waiting at the saturated cache"
        );
        assert!(
            hard_only_epp_in_flight(&EppReadings {
                queue: 0.5,
                per_endpoint_queues: vec![0.5],
                ..epp
            })
            .is_err(),
            "fractional waiting must not satisfy the one-request backlog threshold"
        );
        assert!(
            hard_bound_without_soft_is_valid(
                GatewayCapacityMetrics {
                    soft_ceiling: Some(40.0),
                    ..hard_only_gateway
                },
                Some(9.0),
                9.0
            )
            .is_err(),
            "a finite soft value must fail this hard-only proof"
        );
        assert!(
            hard_bound_without_soft_is_valid(
                GatewayCapacityMetrics {
                    hard_ceiling: None,
                    counted_saturation: None,
                    ..hard_only_gateway
                },
                Some(8.0),
                9.0
            )
            .is_err(),
            "an absent hard bound cannot be replaced by a floor or omitted value"
        );
    }

    #[test]
    fn soft_only_phase_rejects_counted_saturation_without_a_hard_bound() {
        let soft_only = GatewayCapacityMetrics {
            in_flight: None,
            soft_ceiling: None,
            hard_ceiling: None,
            reported_saturation: None,
            counted_saturation: None,
        };
        assert!(
            counted_saturation_is_absent(soft_only),
            "counted saturation should be absent before a hard ceiling is learned"
        );
        assert!(
            counted_saturation_is_absent(GatewayCapacityMetrics {
                counted_saturation: Some(f64::NAN),
                ..soft_only
            }),
            "a non-finite cleared metric is equivalent to an absent hard-bound saturation"
        );
        assert!(
            !counted_saturation_is_absent(GatewayCapacityMetrics {
                counted_saturation: Some(0.0),
                ..soft_only
            }),
            "even a zero counted-saturation value must not appear before a hard bound exists"
        );
    }

    #[test]
    fn withdrawn_pair_requires_all_capacity_and_saturation_gauges_to_clear() {
        let cleared = GatewayCapacityMetrics {
            in_flight: None,
            soft_ceiling: None,
            hard_ceiling: None,
            reported_saturation: None,
            counted_saturation: None,
        };
        assert!(
            capacity_metrics_are_cleared(cleared),
            "a withdrawn pair with no finite capacity or saturation gauges is clear"
        );
        assert!(
            capacity_metrics_are_cleared(GatewayCapacityMetrics {
                in_flight: Some(f64::NAN),
                soft_ceiling: Some(f64::NAN),
                hard_ceiling: Some(f64::NAN),
                reported_saturation: Some(f64::NAN),
                counted_saturation: Some(f64::NAN),
            }),
            "NaN is the Gateway's cleared value for a withdrawn candidate"
        );
        for (metric, stale) in [
            (
                "in-flight",
                GatewayCapacityMetrics {
                    in_flight: Some(1.0),
                    ..cleared
                },
            ),
            (
                "soft ceiling",
                GatewayCapacityMetrics {
                    soft_ceiling: Some(1.0),
                    ..cleared
                },
            ),
            (
                "hard ceiling",
                GatewayCapacityMetrics {
                    hard_ceiling: Some(1.0),
                    ..cleared
                },
            ),
            (
                "provider saturation",
                GatewayCapacityMetrics {
                    reported_saturation: Some(0.5),
                    ..cleared
                },
            ),
            (
                "counted saturation",
                GatewayCapacityMetrics {
                    counted_saturation: Some(0.5),
                    ..cleared
                },
            ),
        ] {
            assert!(
                !capacity_metrics_are_cleared(stale),
                "a withdrawn pair must not retain its {metric} gauge"
            );
        }
    }

    #[test]
    fn saturated_kv_cache_counts_running_and_waiting_requests_per_ready_endpoint() {
        let epp = EppReadings {
            running: 8.0,
            queue: 0.0,
            kv_cache_utilization: KV_HARD_BOUND_CACHE_MEAN,
            kv_cache_spread: Some(KV_HARD_BOUND_CACHE_SPREAD),
            ready: 2.0,
            per_endpoint_queues: vec![0.0, 0.0],
            held_queue_count: Some(0.0),
            held_source: HELD_CAPACITY_METRIC.to_owned(),
            held_capacity_utilization: 0.0,
        };
        assert!(
            (expected_kv_cache_origin_in_flight(&epp, KV_REPORTED_ONLY_WAITING_REQUESTS)
                .unwrap_or_else(|_| std::process::abort())
                - 16.0)
                .abs()
                < f64::EPSILON,
            "queue-free running requests must be counted across both ready endpoints"
        );
        let backlogged = EppReadings {
            queue: 1.0,
            per_endpoint_queues: vec![1.0, 1.0],
            ..epp
        };
        assert!(
            (expected_kv_cache_origin_in_flight(&backlogged, KV_HARD_BOUND_WAITING_REQUESTS)
                .unwrap_or_else(|_| std::process::abort())
                - 18.0)
                .abs()
                < f64::EPSILON,
            "running and waiting requests must both be counted across ready endpoints"
        );

        let mismatched_endpoint_queue = EppReadings {
            per_endpoint_queues: vec![1.0, 0.0],
            ..backlogged
        };
        assert!(
            expected_kv_cache_origin_in_flight(&mismatched_endpoint_queue, KV_HARD_BOUND_WAITING_REQUESTS).is_err(),
            "the sample must fail when waiting work appears on an endpoint"
        );
    }

    #[test]
    fn saturated_kv_cache_must_materially_reduce_pool_b_route_share() {
        let queue_free = route_distribution(408, 1640);
        let hard_bound = route_distribution(968, 1080);
        let (soft_share, hard_share) =
            kv_cache_route_share_shift(&queue_free, &hard_bound, KV_HARD_BOUND_ROUTE_REQUESTS)
                .unwrap_or_else(|_| std::process::abort());
        assert!(
            soft_share >= 0.80,
            "queue-free high-capacity pool should receive most requests, got {soft_share:.3}"
        );
        assert!(
            hard_share < 0.55,
            "the hard-bound sample should route materially less traffic to pool B, got {hard_share:.3}"
        );
    }

    #[test]
    fn saturated_kv_cache_route_sample_rejects_no_shift_or_undersized_batches() {
        let queue_free = route_distribution(408, 1640);
        let unchanged = route_distribution(512, 1536);
        assert!(
            kv_cache_route_share_shift(&queue_free, &unchanged, KV_HARD_BOUND_ROUTE_REQUESTS).is_err(),
            "a high hard-bound pool-B share must not pass"
        );

        let undersized = route_distribution(516, 1532);
        assert!(
            kv_cache_route_share_shift(&queue_free, &undersized, KV_HARD_BOUND_ROUTE_REQUESTS + 1).is_err(),
            "partial traffic samples must not be compared as if they were complete"
        );

        let unproven_baseline = route_distribution(520, 1528);
        let low_hard_bound = route_distribution(1024, 1024);
        assert!(
            kv_cache_route_share_shift(&unproven_baseline, &low_hard_bound, KV_HARD_BOUND_ROUTE_REQUESTS,).is_err(),
            "the paired proof must first establish the queue-free capacity preference"
        );
    }

    #[test]
    fn restoration_errors_fail_the_proof_without_erasing_the_primary_result() {
        assert!(
            qualification_succeeded(true, &[]),
            "proof and cleanup success should pass"
        );
        assert!(
            !qualification_succeeded(false, &[]),
            "successful cleanup must not turn a failed proof into a pass"
        );
        assert!(
            !qualification_succeeded(true, &["policy restore failed".to_owned()]),
            "a failed restoration must fail an otherwise passing proof"
        );
        assert!(
            !qualification_succeeded(false, &["policy restore failed".to_owned()]),
            "a restoration error must remain a failure when the primary proof also failed"
        );
    }

    #[test]
    fn excluded_policy_preserves_original_selectors_and_adds_one_run_label() {
        let original = json!({
            "siteSelector": {"matchLabels": {"provider-site": "pool-b"}},
            "trustedDomains": ["pool-a.grid.internal"]
        });
        let excluded =
            policy_with_run_exclusion(&original, "exclude-this-run").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            excluded.pointer("/siteSelector/matchLabels/provider-site"),
            Some(&json!("pool-b")),
            "the temporary selector should preserve the original site restriction"
        );
        let exclusion_value = excluded
            .get("siteSelector")
            .and_then(|value| value.as_object())
            .and_then(|selector| selector.get("matchLabels"))
            .and_then(|value| value.as_object())
            .and_then(|labels| labels.get(RUN_EXCLUSION_LABEL));
        assert_eq!(
            exclusion_value,
            Some(&json!("exclude-this-run")),
            "the temporary selector should add its unique exclusion value"
        );
        assert_eq!(
            excluded.get("trustedDomains"),
            original.get("trustedDomains"),
            "unrelated access policy fields must remain unchanged"
        );
    }

    #[test]
    fn restoring_policy_replaces_the_complete_nested_object() {
        let original = json!({"siteSelector": {"matchLabels": {"provider-site": "pool-b"}}});
        let patch = access_policy_json_patch(&original);
        let operation = patch.as_array().and_then(|operations| operations.first());
        assert_eq!(
            operation.and_then(|operation| operation.get("op")),
            Some(&json!("replace")),
            "restoration must replace, not merge, accessPolicy"
        );
        assert_eq!(
            operation.and_then(|operation| operation.get("path")),
            Some(&json!("/spec/accessPolicy")),
            "the patch must target the exact policy field"
        );
        assert_eq!(
            operation.and_then(|operation| operation.get("value")),
            Some(&original),
            "the replacement value must be the saved full policy"
        );
    }

    #[test]
    fn missing_local_relay_is_allowed_only_while_pair_is_absent() {
        assert_eq!(
            local_relay_matches(false, None, None, 40.0, 2.0),
            Ok(true),
            "an absent provider pair can stop publishing the local relay series"
        );
        assert!(
            local_relay_matches(true, None, None, 40.0, 2.0).is_err(),
            "a published pair requires both local relay metrics"
        );
        assert_eq!(
            local_relay_matches(false, Some(4.0), Some(2.0), 4.0, 2.0),
            Ok(true),
            "an available absent-pair relay must still agree with the source"
        );
        assert_eq!(
            local_relay_matches(false, Some(40.0), None, 4.0, 2.0),
            Ok(false),
            "an available stale relay value must not be accepted as the lower signal"
        );
    }

    #[test]
    fn four_state_sequence_accepts_clear_and_lower_relearning_but_rejects_retained_old_ceiling() {
        assert!(lifecycle_ceiling_sequence_valid(40.0, 40.0, Some("NaN"), 8.0, 4.0));
        assert!(!lifecycle_ceiling_sequence_valid(40.0, 40.0, Some("NaN"), 40.0, 4.0));
        assert!(!lifecycle_ceiling_sequence_valid(40.0, 39.0, Some("NaN"), 8.0, 4.0));
        assert!(!lifecycle_ceiling_sequence_valid(40.0, 40.0, None, 8.0, 4.0));
        assert!(returned_ceiling_is_valid(40.0, 9.0, 8.0));
        assert!(!returned_ceiling_is_valid(40.0, 40.0, 8.0));
    }

    #[test]
    fn gateway_config_watcher_observations_accept_applied_and_reused_results() {
        let applied = "grid_serving_config_reload_total{result=\"applied\"} 2\n";
        let reused = "grid_serving_config_reload_total{result=\"reused\"} 3\n";
        let both = format!("{applied}{reused}grid_serving_config_reload_total{{result=\"refused\"}} 8\n");

        assert!(
            (gateway_config_observation_count(applied).unwrap_or_else(|_| std::process::abort()) - 2.0).abs()
                < f64::EPSILON,
            "an applied config is an accepted watcher observation"
        );
        assert!(
            (gateway_config_observation_count(reused).unwrap_or_else(|_| std::process::abort()) - 3.0).abs()
                < f64::EPSILON,
            "a mounted config the watcher confirms unchanged is an accepted observation"
        );
        assert!(
            (gateway_config_observation_count(&both).unwrap_or_else(|_| std::process::abort()) - 5.0).abs()
                < f64::EPSILON,
            "applied and reused counters both advance the cumulative observation count"
        );
    }

    #[test]
    fn gateway_config_watcher_observations_reject_missing_zero_and_invalid_counters() {
        for metrics in [
            "grid_serving_config_reload_total{result=\"refused\"} 1\n",
            "grid_serving_config_reload_total{result=\"applied\"} 0\n",
            "grid_serving_config_reload_total{result=\"reused\"} -1\n",
            "grid_serving_config_reload_total{result=\"reused\"} invalid\n",
        ] {
            assert!(
                gateway_config_observation_count(metrics).is_err(),
                "invalid or non-accepted watcher metrics must not establish readiness: {metrics:?}"
            );
        }
    }

    #[test]
    fn gateway_evidence_keeps_signal_poll_and_feedback_counters() {
        let metrics = "grid_signals_poll_total{result=\"ok\"} 83\n\
                       grid_signals_feedback_failures_total{reason=\"status\"} 83\n\
                       unrelated_metric 5\n";
        let evidence = super::relevant_gateway_metric_lines(metrics);

        assert!(evidence.contains("grid_signals_poll_total{result=\"ok\"} 83"));
        assert!(evidence.contains("grid_signals_feedback_failures_total{reason=\"status\"} 83"));
        assert!(!evidence.contains("unrelated_metric"));
    }

    #[test]
    fn original_serving_baseline_requires_the_exact_annotated_config_in_the_gateway_mount() {
        assert!(gateway_config_values_match(
            "current", "current", "current", "same", "same"
        ));
        assert!(
            !gateway_config_values_match("current", "current", "previous", "current bytes", "previous bytes"),
            "a stale mounted overlay must remain unready until projection catches up"
        );
        assert!(
            !gateway_config_values_match("current", "previous", "current", "same", "same"),
            "the ConfigMap revision annotation must identify its serving content"
        );
        assert!(
            !gateway_config_values_match("current", "current", "current", "operator bytes", "mount bytes"),
            "equal digest labels must not hide different operator and mounted bytes"
        );
    }

    #[test]
    fn response_attribution_requires_grid_headers_and_trusted_provider_to_agree() {
        let valid = super::SiteRouteResponse {
            status: 200,
            site: Some("pool-a".to_owned()),
            backend: Some("llmd-pool-a-provider".to_owned()),
            provider_site: Some("pool-a".to_owned()),
            raw_header_names: vec!["x-grid-site".to_owned(), "x-grid-backend".to_owned()],
        };
        assert_eq!(
            validate_response_attribution(&valid, Some("pool-a")),
            Ok(()),
            "the consistent route should pass attribution validation"
        );
        assert!(validate_response_attribution(&valid, Some("pool-b")).is_err());
        let mut missing_backend = valid;
        missing_backend.backend = None;
        assert!(validate_response_attribution(&missing_backend, None).is_err());
    }

    #[test]
    fn failed_http_responses_and_transport_errors_are_recorded_separately() {
        let response = super::SiteRouteResponse {
            status: 502,
            site: Some("pool-b".to_owned()),
            backend: Some("llmd-pool-b-provider".to_owned()),
            provider_site: None,
            raw_header_names: vec!["x-grid-site".to_owned(), "x-grid-backend".to_owned()],
        };
        let http_failure = probe_failure_value("idle", 19, Some("pool-b"), Some(&response), "expected HTTP 200");
        assert_eq!(
            http_failure.get("kind"),
            Some(&json!("http_status")),
            "an HTTP 502 is a response failure, not a transport failure"
        );
        assert_eq!(http_failure.get("status"), Some(&json!(502)));
        assert_eq!(
            http_failure.get("x_grid_backend"),
            Some(&json!("llmd-pool-b-provider")),
            "failed response attribution should be retained"
        );

        let transport_failure = probe_failure_value("idle", 20, Some("pool-b"), None, "curl timed out");
        assert_eq!(
            transport_failure.get("kind"),
            Some(&json!("transport_error")),
            "a failed curl request has no HTTP response"
        );
        assert_eq!(
            transport_failure.get("status"),
            Some(&json!(null)),
            "transport failures should not be assigned an HTTP status"
        );
    }
}
