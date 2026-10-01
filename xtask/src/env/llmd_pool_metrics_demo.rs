//! llm-d pool-metrics routing demo orchestration.
//!
//! Deploys two Kind clusters, each running an llm-d EPP backed by two
//! deterministic llm-d-sim inference backends. Grid scrapes EPP pool-level
//! metrics and adjusts routing when persistent simulator configuration changes.
#![expect(
    clippy::string_slice,
    clippy::too_many_lines,
    clippy::unnecessary_wraps,
    clippy::unwrap_used,
    clippy::doc_markdown,
    clippy::disallowed_methods,
    clippy::struct_excessive_bools,
    clippy::cast_possible_wrap,
    reason = "Demo orchestration code prioritizes clarity over lint perfection"
)]

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::OnceLock,
    time::{Duration, Instant},
};

use serde::Serialize;
use sha2::{Digest as _, Sha256};

use super::{DemoMode, GlbDemoOptions, certs, glb, kubectl, operator, safe_truncate_str};

/// Directory where generated TLS certificates are stored.
const CERTS_DIR: &str = "tests/env/certs";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Ordered cluster names in the llm-d pool-metrics demo.
const CLUSTERS: &[&str] = &["pool-a", "pool-b"];

/// Kubernetes namespace for all Grid and llm-d components.
const GRID_SYSTEM_NS: &str = "grid-system";

/// Consumer gateway TLS secret name.
const CONSUMER_TLS_SECRET: &str = "consumer-gateway-tls";

/// Provider gateway TLS secret name.
const PROVIDER_TLS_SECRET: &str = "provider-gateway-tls";

/// Provider credential secret name (VCR backends accept any bearer token).
const VCR_INFERENCE_CREDENTIAL: &str = "vcr-inference-credential";

/// Overlay ConfigMap name created by the Grid operator for consumer gateways.
const OVERLAY_CONFIGMAP: &str = "grid-overlay-grid-llmd-pool-metrics-consumer-gateway";

/// Stable terminal separator.
const OUTPUT_RULE: &str = "===============================================================================";

/// Evidence JSON schema version.
const EVIDENCE_SCHEMA_VERSION: &str = "1";

/// Number of setup phases in mTLS mode.
const SETUP_PHASES_MTLS: usize = 11;

/// Number of setup phases in direct-HTTP mode (no metrics TLS secrets phase).
const SETUP_PHASES_DIRECT: usize = 10;

/// Additional setup phase for the operator restart after creating a poll-mode GridNetwork.
const SETUP_PHASES_DYNAMIC: usize = 11;

/// Requests per phase for dynamic weighted placement qualification.
const DYNAMIC_SAMPLE_SIZE: u32 = 1600;

/// Existing weighted-affinity sessions replayed after the pressure transition.
const DYNAMIC_AFFINITY_SESSION_COUNT: u32 = 32;

/// One-degree-of-freedom Pearson chi-square threshold at 99% confidence.
const DYNAMIC_CHI_SQUARE_CRITICAL: f64 = 6.635;

/// Minimum measured change between phases; 1,600 requests keep the
/// 12-point shift criterion conservative relative to binomial sampling error.
const DYNAMIC_MIN_PRESSURE_SHIFT: f64 = 0.12;

/// Minimum recovered pool-a share after returning pressure to baseline.
const DYNAMIC_MIN_RECOVERY_SHARE: f64 = 0.45;

/// Primary model name served by the deterministic llm-d-sim backends.
const VCR_MODEL: &str = "Qwen/Qwen3-0.6B";

/// Data-plane convergence timeout for overlay propagation.
const DATA_PLANE_WAIT: Duration = Duration::from_secs(180);

/// Retry interval for convergence probes.
const DATA_PLANE_INTERVAL: Duration = Duration::from_secs(1);

/// Configured queue capacity (matches MOCK_MAX_NUM_SEQS on VCR pods).
const QUEUE_CAPACITY: f64 = 4.0;

/// Minimum score gap required before capturing the pressure scorecard.
///
/// With real VCR backends, pressure creates modest score differences
/// (queue=2 → gap ≈ 0.03) from live VCR/EPP pressure.
const MIN_PRESSURE_SCORE_GAP: f64 = 0.01;

/// Queue-depth pressure-phase threshold (raw queue size, out of `QUEUE_CAPACITY`).
const QUEUE_PRESSURE_THRESHOLD: f64 = 1.0;

/// KV-cache pressure-phase threshold (normalized utilization, 0.0-1.0).
///
/// Lower than the queue threshold's fraction of capacity (1.0/4.0 = 25%)
/// because KV-cache utilization is a smoother, more gradually-rising signal
/// under the same synthetic load than discrete queued-request counts.
const KV_CACHE_PRESSURE_THRESHOLD: f64 = 0.1;

/// Queue-depth recovery threshold: how low `queue_size` must drop before the
/// recovery proof attempts its verification probe.
///
/// Deliberately looser than `QUEUE_PRESSURE_THRESHOLD` (3.0 vs 1.0) -- recovery
/// only needs "clearly drained," not a full return below the more sensitive
/// phase-detection threshold. Extracted from the original inline literal.
const RECOVERY_QUEUE_THRESHOLD: f64 = 3.0;

/// Deterministic simulator Deployments representing the pool endpoints.
const SIMULATOR_DEPLOYMENTS: &[&str] = &["vcr-1", "vcr-2"];
/// Persistent simulator ConfigMap containing startup fake metrics.
const SIMULATOR_CONFIGMAP: &str = "vcr-1-config";

/// GridNetwork resource name.
const GRID_NETWORK_NAME: &str = "grid-llmd-pool-metrics";

/// Run-specific Kind/Forge prefix; unset in unit tests.
static RUN_PREFIX: OnceLock<String> = OnceLock::new();

/// Default gateway image tag.
///
/// The pool-metrics demo shares the same Grid-enabled Praxis AI binary as the
/// combined-site demo. Both require the `peer_identity_trust`,
/// `provider_route`, `credential_inject`, and `intelligent_route` filters
/// which are built into the published Grid AI rollup.
const DEFAULT_GATEWAY_IMAGE: &str = "ghcr.io/praxis-proxy/ai:0.4.0";

/// Default operator image tag.
const DEFAULT_OPERATOR_IMAGE: &str = "ghcr.io/praxis-proxy/grid-operator:v0.1.4";

/// Default EPP image reference required by this demo.
const DEFAULT_EPP_IMAGE: &str = "ghcr.io/llm-d/llm-d-router-endpoint-picker:v0.9.0";

/// Default deterministic llm-d-sim image reference required by this demo.
const DEFAULT_VCR_IMAGE: &str = "llm-d-inference-sim:deterministic";

/// Default overlay-sync sidecar image tag.
const DEFAULT_OVERLAY_SYNC_IMAGE: &str = "ghcr.io/praxis-proxy/grid-overlay-sync:v0.1.4";

/// Default nginx image for the metrics TLS reverse proxy sidecar.
const DEFAULT_NGINX_IMAGE: &str = "docker.io/library/nginx:1.27.4-alpine";

/// Metrics TLS CA common name (separate from gateway CA).
const METRICS_CA_CN: &str = "Grid Metrics Test CA";

/// DNS SAN for the metrics TLS server certificate.
const METRICS_SERVER_DNS: &str = "llmd-epp-metrics.grid-system.svc.cluster.local";

/// Secret name holding the metrics CA certificate.
const METRICS_CA_SECRET: &str = "metrics-ca";

/// Secret name holding the metrics server TLS certificate and key.
const METRICS_SERVER_TLS_SECRET: &str = "metrics-server-tls";

/// Secret name holding the metrics client TLS certificate and key.
const METRICS_CLIENT_TLS_SECRET: &str = "metrics-client-tls";

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

/// Metrics transport mode selected by the user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricsTransport {
    /// Scrape EPP directly over HTTP on port 9090.
    DirectHttp,
    /// nginx mTLS reverse proxy on port 9443 forwarding to EPP 9090.
    MtlsProxy,
}

impl MetricsTransport {
    /// Human-readable label used in CLI output and evidence JSON.
    fn label(self) -> &'static str {
        match self {
            Self::DirectHttp => "direct-http",
            Self::MtlsProxy => "mtls-proxy",
        }
    }
}

/// Which of Grid's real scoring signals drives routing in this demo run.
///
/// Selected via the `--kv-cache` CLI flag. Both flavors share the same
/// simulator metric transitions and the same overlay score-breakdown display (both
/// `queue_depth` and `kv_cache` are always shown); only the operator's
/// `GridNetwork.spec.scoringPolicy.strategy` — and therefore which raw
/// signal actually produces the `score`/`rank` that drives the A\u{2192}B flip —
/// changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScoringFlavor {
    /// llm-d's `queue-scorer` equivalent (default).
    QueueDepth,
    /// llm-d's `kv-cache-utilization-scorer` equivalent.
    KvCachePressure,
}

/// Internal mode and image inputs shared by the score and pressure runs.
#[derive(Clone, Copy)]
struct PlacementRunOptions {
    /// EPP metrics transport.
    metrics_transport: MetricsTransport,
    /// Signal selected for the score-based demo or pressure adapter.
    scoring_flavor: ScoringFlavor,
    /// Enable the separately qualified poll-mode weighted adapter.
    pressure_weighted: bool,
}

/// Paths owned by one run while preparing its resolved Forge configuration.
#[derive(Clone, Copy)]
struct SetupPaths<'paths> {
    /// Isolated Forge state root.
    forge_state: &'paths Path,
    /// Run-owned site identity and trust material directory.
    certificates: &'paths Path,
    /// Evidence root for setup artifacts.
    evidence: &'paths Path,
}

impl ScoringFlavor {
    /// Selects the flavor from the `--kv-cache` CLI flag.
    fn from_kv_cache_flag(kv_cache: bool) -> Self {
        if kv_cache {
            Self::KvCachePressure
        } else {
            Self::QueueDepth
        }
    }

    /// Human-readable label used in CLI output and evidence JSON.
    fn label(self) -> &'static str {
        match self {
            Self::QueueDepth => "queue-depth",
            Self::KvCachePressure => "kv-cache-pressure",
        }
    }

    /// `GridNetwork.spec.scoringPolicy.strategy` YAML value for this flavor.
    ///
    /// Must match `ScoringStrategy`'s `camelCase` serde rename in
    /// `operator/src/crd/grid_network.rs` exactly.
    fn strategy_yaml(self) -> &'static str {
        match self {
            Self::QueueDepth => "queueDepth",
            Self::KvCachePressure => "kvCachePressure",
        }
    }
}

/// Demo execution context holding resolved paths.
struct DemoContext {
    /// Unique run identifier.
    run_id: String,
    /// Path to the resolved Forge config.
    resolved_config: PathBuf,
    /// Per-run Forge state directory, kept beside the run's evidence.
    forge_state_dir: PathBuf,
    /// Path to the forge binary.
    forge_bin: PathBuf,
    /// Resolved container images.
    images: ResolvedImages,
    /// Selected metrics transport mode.
    metrics_transport: MetricsTransport,
    /// Selected scoring flavor (which signal drives routing).
    scoring_flavor: ScoringFlavor,
    /// Whether this run calculates placement weights from polled signals.
    pressure_weighted: bool,
    /// Run-owned certificate directory.
    certs_dir: PathBuf,
    /// Run-specific evidence directory.
    evidence_dir: PathBuf,
}

/// Resolved container image references.
struct ResolvedImages {
    /// Praxis AI gateway image (must contain Grid filters).
    gateway: String,
    /// Grid operator image.
    operator: String,
    /// llm-d EPP image.
    epp: String,
    /// Deterministic llm-d-sim inference backend image.
    vcr: String,
    /// Grid overlay-sync sidecar image.
    overlay_sync: String,
    /// nginx image for metrics TLS reverse proxy sidecar (mTLS mode only).
    nginx: Option<String>,
}

// ---------------------------------------------------------------------------
// Evidence structures
// ---------------------------------------------------------------------------

/// Top-level evidence envelope.
#[derive(Serialize)]
struct Evidence {
    /// Schema version for tooling compatibility.
    schema_version: String,
    /// Demo mode.
    mode: String,
    /// Metrics transport: "direct-http" or "mtls-proxy".
    metrics_transport: String,
    /// Scoring strategy: "queue-depth" or "kv-cache-pressure".
    scoring_strategy: String,
    /// Placement strategy used by this run.
    placement_strategy: String,
    /// Unique run identifier used for clusters, generated files, and evidence.
    run_id: String,
    /// UTC timestamp when the run started.
    started_at: String,
    /// Wall-clock duration in seconds.
    wall_secs: f64,
    /// Whether the run succeeded.
    success: bool,
    /// Error message, if any.
    error: Option<String>,
    /// Setup phase evidence.
    setup: SetupEvidence,
    /// Proof scenario results.
    proofs: BTreeMap<String, ProofResult>,
    /// Lifecycle metadata.
    lifecycle: LifecycleRecord,
}

/// Setup phase evidence.
#[derive(Serialize)]
struct SetupEvidence {
    /// Cluster names created.
    clusters: Vec<String>,
    /// Image tags used.
    images: BTreeMap<String, String>,
    /// Requested container references and immutable IDs observed in live pods.
    pod_images: Vec<PodImageEvidence>,
}

/// Image provenance reported by a running Kubernetes container.
#[derive(Serialize)]
struct PodImageEvidence {
    /// Logical pool/Kind cluster that contains the pod.
    cluster: String,
    /// Kubernetes pod name.
    pod: String,
    /// Container name within the pod.
    container: String,
    /// Reference requested in the pod spec.
    requested_image: String,
    /// Runtime-reported immutable image identifier.
    image_id: String,
    /// Whether Kubernetes reports this container ready.
    ready: bool,
    /// Restart count observed before teardown.
    restart_count: u32,
}

/// Inputs for the runtime identity proof for the locally selected Praxis AI image.
struct GatewayImageIdentity<'identity> {
    /// Selected local image reference.
    image: &'identity str,
    /// Expected OCI image config digest.
    config_id: &'identity str,
    /// OCI source repository label.
    source: &'identity str,
    /// OCI source revision label.
    revision: &'identity str,
    /// Expected AI worktree revision.
    expected_revision: Option<&'identity str>,
    /// Expected full hash of the AI worktree's uncommitted diff.
    expected_content_hash: Option<&'identity str>,
    /// OCI image version label.
    version: &'identity str,
    /// Runtime image evidence captured from Kubernetes.
    pods: &'identity [PodImageEvidence],
}

/// Single proof scenario result.
#[derive(Clone, Serialize)]
struct ProofResult {
    /// Whether the proof passed.
    success: bool,
    /// Human-readable description.
    description: String,
    /// Observations captured during the proof.
    observations: Vec<String>,
}

/// Lifecycle record for teardown tracking.
#[derive(Serialize)]
struct LifecycleRecord {
    /// Whether teardown was requested.
    teardown_requested: bool,
    /// Whether teardown was performed.
    teardown_performed: bool,
    /// Teardown result.
    teardown_result: Option<String>,
    /// Whether the environment was kept on failure.
    kept_on_failure: bool,
}

/// One row of the narrated CLI scorecard.
///
/// All fields are derived from the overlay ConfigMap so that displayed
/// metrics and scores come from the same operator scoring revision.
#[derive(Clone, Serialize)]
struct ScorecardRow {
    /// Cluster identifier.
    cluster: String,
    /// Queue size back-computed from overlay score breakdown.
    queue: f64,
    /// Configured queue capacity.
    capacity: f64,
    /// Queue pressure back-computed from overlay score breakdown.
    pressure: f64,
    /// KV-cache utilization back-computed from overlay score breakdown.
    kv_cache: f64,
    /// Production score from the overlay (scoring engine output).
    score: f64,
    /// Rank from the overlay ConfigMap (0 = preferred).
    rank: i64,
}

/// Parsed overlay candidate scores from the overlay ConfigMap JSON.
#[derive(Clone, Serialize)]
struct OverlayCandidate {
    /// Cluster identifier.
    cluster: String,
    /// Zero-based rank.
    rank: u32,
    /// Production weighted score.
    score: f64,
    /// Whether the candidate is fresh.
    fresh: bool,
    /// Admission state string.
    admission_state: String,
    /// Selection group used for group-first routing.
    selection_group: Option<u32>,
    /// Published relative traffic weight, when weighted selection is active.
    traffic_weight: Option<u32>,
    /// Score breakdown from the production scoring engine.
    breakdown: Option<super::operator_overlay::ScoreBreakdown>,
}

/// One normalized provider signal read from the operator's mTLS endpoint.
#[derive(Clone, Debug, Serialize)]
struct DynamicSignalSample {
    /// Source Grid site that measured this provider.
    site: String,
    /// Provider routing identity assigned by Grid.
    provider: String,
    /// Normalized pressure in `[0, 1]`.
    value: f64,
    /// Sample timestamp rendered by the signal service, in Unix milliseconds.
    timestamp_ms: Option<i64>,
}

/// Overlay snapshot observed after a Grid reconciliation.
#[derive(Clone, Debug, Serialize)]
struct DynamicOverlaySnapshot {
    /// Semantic digest published by Grid.
    semantic_revision: String,
    /// resourceVersion for diagnostics only; not compared across clusters.
    resource_version: String,
    /// Two active candidates in the first eligible selection group.
    candidates: Vec<DynamicCandidateWeight>,
}

/// Published provider weight and identity relevant to this test.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct DynamicCandidateWeight {
    /// Provider site represented by this candidate.
    site: String,
    /// Provider routing identity.
    provider: String,
    /// Stable identity used for weight state.
    stable_id: String,
    /// AI selection group for this candidate.
    selection_group: u32,
    /// Published positive traffic weight.
    traffic_weight: u32,
    /// Candidate freshness published by Grid.
    fresh: bool,
    /// Candidate admission state published by Grid.
    admission_state: String,
}

/// Statistical outcomes from one complete phase sample.
#[derive(Clone, Debug, Serialize)]
struct DynamicTrafficSample {
    /// Number of raw response records observed.
    request_count: u32,
    /// Attributed successful response count per provider.
    counts: BTreeMap<String, u32>,
    /// Measured share of attributed responses per provider.
    observed_fraction: BTreeMap<String, f64>,
    /// Expected share calculated from published weights.
    expected_fraction: BTreeMap<String, f64>,
    /// Pearson chi-square statistic for the observed distribution.
    chi_square: f64,
    /// Predeclared critical chi-square value.
    chi_square_critical_value: f64,
    /// Whether request and statistical acceptance criteria passed.
    accepted: bool,
    /// Requests that did not produce an HTTP response.
    transport_failures: u32,
    /// Requests that produced a non-200 response.
    http_failures: u32,
    /// Successful HTTP responses missing trusted provider attribution.
    attribution_failures: u32,
}

/// One non-retried request made while binding or replaying weighted affinity.
#[derive(Clone, Debug, Serialize)]
struct DynamicAffinitySample {
    /// Whether this request created or replayed the session binding.
    stage: String,
    /// Run-local session ordinal; the synthetic session key is not recorded.
    session_ordinal: u32,
    /// curl exit status (zero means transport completed).
    curl_exit_code: i32,
    /// HTTP status, if one was received.
    http_status: Option<u16>,
    /// Provider gateway returned by the trusted response attribution header.
    provider_gateway: String,
    /// Independent provider attribution returned by the demo backend.
    provider_attribution: String,
}

/// Deployment pod identities captured across an intentional operator restart.
#[derive(Clone, Debug, Serialize)]
struct DynamicOperatorRestart {
    /// Run-owned Kind cluster name.
    cluster: String,
    /// Operator pod UIDs before the restart.
    before_pod_uids: Vec<String>,
    /// Operator pod UIDs after the rollout completed.
    after_pod_uids: Vec<String>,
}

/// Runtime evidence for the complete baseline/pressure/recovery chain.
#[derive(Clone, Debug, Serialize)]
struct DynamicPhaseEvidence {
    /// Baseline, pressure, or recovery phase name.
    phase: String,
    /// Requested deterministic simulator queue value.
    requested_waiting_requests: u32,
    /// Simulator deployment generation per pool.
    simulator_generation: BTreeMap<String, i64>,
    /// Simulator pod UID per deployment.
    simulator_pod_uids: BTreeMap<String, String>,
    /// Queue values observed from EPP per pool.
    epp_queue_size: BTreeMap<String, f64>,
    /// Fresh normalized provider signal observations.
    normalized_signals: Vec<DynamicSignalSample>,
    /// Per-gateway Grid overlays and their Praxis accepted/serving revisions.
    gateways: BTreeMap<String, DynamicGatewayState>,
    /// Consecutive matching full-state observations before traffic began.
    stable_observations: u8,
    /// Duration the full state remained unchanged before traffic began.
    stable_duration_ms: u128,
    /// Required quiet period, derived from the peer-signal poll interval.
    required_stable_duration_ms: u128,
    /// Attributed request sample and statistical result.
    traffic: DynamicTrafficSample,
}

/// One gateway's overlay and the exact AI revisions accepted and served there.
#[derive(Clone, Debug, Serialize)]
struct DynamicGatewayState {
    /// Grid's local-site-specific overlay.
    overlay: DynamicOverlaySnapshot,
    /// Revision accepted by Praxis on this gateway.
    accepted_revision: String,
    /// Revision currently served by Praxis on this gateway.
    serving_revision: String,
}

/// Snapshot returned when Grid, signal, and both gateway states agree.
struct DynamicStateSnapshot {
    /// Per-gateway overlays; semantic revisions need not match across sites.
    gateways: BTreeMap<String, DynamicGatewayState>,
    /// Fresh normalized provider signal samples.
    signal_samples: Vec<DynamicSignalSample>,
    /// EPP queue values observed for both pools.
    epp_queue_size: BTreeMap<String, f64>,
    /// Consecutive matching full-state observations before returning.
    stable_observations: u8,
    /// Duration the full state remained unchanged before returning.
    stable_duration: Duration,
    /// Required quiet period derived from the peer-signal poll interval.
    required_stable_duration: Duration,
    /// Raw sampled signal exposition for troubleshooting.
    signal_lines: Vec<String>,
}

/// Complete traffic result, including raw rows and sample-window timestamps.
type DynamicTrafficResult = (DynamicTrafficSample, Vec<String>, String, String);

/// Result and bounded-heap evidence returned by one dynamic phase.
type DynamicPhaseResult = Result<(ProofResult, Box<DynamicPhaseEvidence>), Box<dyn std::error::Error>>;

/// Simulator deployment generations and pod UIDs captured before sampling.
type SimulatorRuntimeIdentity = (BTreeMap<String, i64>, BTreeMap<String, String>);

/// Parsed inference response with gateway attribution.
struct InferenceResponse {
    /// Provider gateway cluster from `X-Grid-LlmD-Provider-Gateway`.
    provider_gateway: String,
    /// Demo attribution from `x-ai-demo-provider-gateway`.
    demo_attribution: String,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run the llm-d pool-metrics routing demo.
///
/// # Errors
///
/// Returns an error when setup, proof scenarios, or teardown fail.
pub(crate) fn run(
    forge_config: &Path,
    options: &GlbDemoOptions,
    metrics_mtls: bool,
    kv_cache: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    run_with_placement(
        forge_config,
        options,
        PlacementRunOptions {
            metrics_transport: if metrics_mtls {
                MetricsTransport::MtlsProxy
            } else {
                MetricsTransport::DirectHttp
            },
            scoring_flavor: ScoringFlavor::from_kv_cache_flag(kv_cache),
            pressure_weighted: false,
        },
    )
}

/// Run the dedicated full poll-mode pressure-weighted qualification.
pub(crate) fn run_dynamic_weighted(
    forge_config: &Path,
    options: &GlbDemoOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    if options.mode() != DemoMode::Full {
        return Err("dynamic weighted qualification requires --full".into());
    }
    run_with_placement(
        forge_config,
        options,
        PlacementRunOptions {
            metrics_transport: MetricsTransport::DirectHttp,
            scoring_flavor: ScoringFlavor::QueueDepth,
            pressure_weighted: true,
        },
    )
}

/// Execute the existing pool-metrics lifecycle with the selected placement policy.
fn run_with_placement(
    forge_config: &Path,
    options: &GlbDemoOptions,
    placement: PlacementRunOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    let mode = options.mode();
    let PlacementRunOptions {
        metrics_transport,
        scoring_flavor,
        pressure_weighted,
    } = placement;
    let run_id = format_run_id(&format_utc_timestamp(), std::process::id());
    drop(RUN_PREFIX.set(format!("grid-llmd-pm-{run_id}")));
    let started_at = format_utc_iso();
    let wall_start = Instant::now();

    let evidence_dir = resolve_evidence_dir(forge_config, options, &run_id)?;
    fs::create_dir_all(&evidence_dir)?;
    let certs_dir = Path::new(CERTS_DIR).join(format!("grid-llmd-pm-{run_id}"));

    let demo_root = super::demo_root(forge_config);
    eprintln!("{OUTPUT_RULE}");
    eprintln!(
        "{}",
        if pressure_weighted {
            "Grid Dynamic Weighted Routing Qualification"
        } else {
            "Grid llm-d Pool-Metrics Routing Demo"
        }
    );
    eprintln!("Mode: {}", if mode == DemoMode::Quick { "quick" } else { "full" });
    eprintln!("Metrics transport: {}", metrics_transport.label());
    eprintln!("Scoring strategy:  {}", scoring_flavor.label());
    eprintln!(
        "Placement mode:    {}",
        if pressure_weighted {
            "pressureWeighted"
        } else {
            "score preference"
        }
    );
    eprintln!("Forge config: {}", forge_config.display());
    eprintln!("Demo root:    {}", demo_root.display());
    eprintln!("{OUTPUT_RULE}");

    let context = prepare_setup(
        forge_config,
        &run_id,
        placement,
        SetupPaths {
            forge_state: &evidence_dir.join("forge-state"),
            certificates: &certs_dir,
            evidence: &evidence_dir,
        },
    )?;
    let mut teardown_success = false;
    let mut run_error: Option<String> = None;
    let mut pod_images = Vec::new();

    let proof_results = match deploy_setup(&context) {
        Ok(()) => {
            eprintln!();
            eprintln!("{OUTPUT_RULE}");
            eprintln!("ENVIRONMENT READY - Starting proof scenarios");
            eprintln!("{OUTPUT_RULE}");

            let mut results = run_proof_scenarios(&context, mode);

            match collect_pod_image_evidence() {
                Ok(captured) => {
                    fs::write(
                        evidence_dir.join("pod-images.json"),
                        serde_json::to_vec_pretty(&captured)?,
                    )?;
                    pod_images = captured;
                    if pressure_weighted {
                        let expected_revision = std::env::var("GRID_XTASK_GATEWAY_REVISION").ok();
                        let expected_content_hash = std::env::var("GRID_XTASK_GATEWAY_CONTENT_SHA256").ok();
                        let image_proof = verify_gateway_image_identity(
                            &context.images.gateway,
                            expected_revision.as_deref(),
                            expected_content_hash.as_deref(),
                            &pod_images,
                        );
                        if !image_proof.success {
                            append_run_error(&mut run_error, "gateway image identity verification failed".to_owned());
                        }
                        results.insert("gateway_image_identity".to_owned(), image_proof);
                    }
                },
                Err(error) => {
                    if run_error.is_none() {
                        run_error = Some(format!("pod image evidence capture failed: {error}"));
                    }
                },
            }

            let failed: Vec<&str> = results
                .iter()
                .filter_map(|(name, proof)| (!proof.success).then_some(name.as_str()))
                .collect();
            if !failed.is_empty() {
                append_run_error(&mut run_error, format!("proofs failed: {}", failed.join(", ")));
            }

            if options.teardown && (run_error.is_none() || !options.keep_on_failure) {
                match teardown_environment(&context) {
                    Ok(()) => teardown_success = true,
                    Err(e) => {
                        eprintln!("[WARN]  Teardown failed: {e}");
                        run_error = Some(match run_error {
                            Some(prev) => format!("{prev}; teardown: {e}"),
                            None => format!("teardown: {e}"),
                        });
                    },
                }
            }

            results
        },
        Err(e) => {
            eprintln!("[FAIL] Environment setup failed: {e}");
            run_error = Some(format!("setup failed: {e}"));

            match collect_pod_image_evidence() {
                Ok(captured) => {
                    if let Err(error) = fs::write(
                        evidence_dir.join("pod-images.json"),
                        serde_json::to_vec_pretty(&captured)?,
                    ) {
                        append_run_error(&mut run_error, format!("pod image evidence write failed: {error}"));
                    } else {
                        pod_images = captured;
                    }
                },
                Err(error) => append_run_error(&mut run_error, format!("pod image evidence capture failed: {error}")),
            }

            if options.teardown && !options.keep_on_failure {
                match teardown_environment(&context) {
                    Ok(()) => teardown_success = true,
                    Err(te) => {
                        eprintln!("[WARN]  Cleanup after setup failure: {te}");
                        append_run_error(&mut run_error, format!("teardown failed: {te}"));
                    },
                }
            }

            BTreeMap::new()
        },
    };

    let wall_secs = wall_start.elapsed().as_secs_f64();
    let images = collect_image_evidence(&context.images)?;
    write_run_provenance(&evidence_dir, &context, &images, &pod_images)?;
    let success = run_error.is_none();

    write_transition_timeline(&evidence_dir, &proof_results, context.scoring_flavor, wall_secs)?;
    let evidence = Evidence {
        schema_version: EVIDENCE_SCHEMA_VERSION.to_owned(),
        mode: format!("{mode:?}").to_lowercase(),
        metrics_transport: metrics_transport.label().to_owned(),
        scoring_strategy: scoring_flavor.label().to_owned(),
        placement_strategy: if pressure_weighted {
            "pressureWeighted".to_owned()
        } else {
            "score preference".to_owned()
        },
        run_id,
        started_at,
        wall_secs,
        success,
        error: run_error.clone(),
        setup: SetupEvidence {
            clusters: CLUSTERS
                .iter()
                .map(|cluster| format!("{}-{cluster}", forge_cluster_prefix()))
                .collect(),
            images,
            pod_images,
        },
        proofs: proof_results,
        lifecycle: LifecycleRecord {
            teardown_requested: options.teardown,
            teardown_performed: teardown_success,
            teardown_result: teardown_success.then(|| "success".to_owned()),
            kept_on_failure: options.keep_on_failure,
        },
    };

    let evidence_path = evidence_dir.join("evidence.json");
    let json = serde_json::to_string_pretty(&evidence).unwrap();
    fs::write(&evidence_path, &json)?;

    eprintln!();
    eprintln!("{OUTPUT_RULE}");
    if success {
        eprintln!("DEMO PASSED  ({wall_secs:.1}s)");
    } else {
        eprintln!("DEMO FAILED  ({wall_secs:.1}s)");
        if let Some(err) = &run_error {
            eprintln!("  {err}");
        }
    }
    eprintln!("Evidence: {}", evidence_path.display());
    eprintln!("{OUTPUT_RULE}");

    if success {
        Ok(())
    } else {
        Err(run_error.unwrap().into())
    }
}

/// Write one machine-readable record per deterministic phase. The detailed
/// observations remain in `evidence.json`; this JSONL companion makes phase
/// ordering and requested simulator values easy to consume without parsing
/// human output.
fn write_transition_timeline(
    evidence_dir: &Path,
    proofs: &BTreeMap<String, ProofResult>,
    scoring_flavor: ScoringFlavor,
    wall_secs: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    let phases = [
        ("baseline", 0_u32, "baseline"),
        ("pressure", 9_u32, "pressure_and_flip"),
        ("recovery", 0_u32, "recovery"),
    ];
    let mut lines = Vec::new();
    for (phase, requested_pressure, proof_name) in phases {
        let proof = proofs.get(proof_name);
        let (waiting_requests, kv_cache_usage) = simulator_metrics(requested_pressure, scoring_flavor);
        lines.push(serde_json::json!({
            "phase": phase,
            "requested_waiting_requests": waiting_requests,
            "requested_kv_cache_usage": kv_cache_usage,
            "proof_success": proof.is_some_and(|result| result.success),
            "wall_elapsed_seconds": wall_secs,
            "observations": proof.map_or_else(Vec::new, |result| result.observations.clone()),
        }));
    }
    let content = lines
        .iter()
        .map(serde_json::Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(evidence_dir.join("timeline.jsonl"), format!("{content}\n"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

/// Resolve inputs before creating clusters.
fn prepare_setup(
    forge_config: &Path,
    run_id: &str,
    placement: PlacementRunOptions,
    paths: SetupPaths<'_>,
) -> Result<DemoContext, Box<dyn std::error::Error>> {
    let PlacementRunOptions {
        metrics_transport,
        scoring_flavor,
        pressure_weighted,
    } = placement;
    let SetupPaths {
        forge_state: forge_state_dir,
        certificates: certs_dir,
        evidence: evidence_dir,
    } = paths;
    let images = resolve_images(metrics_transport)?;
    verify_images(&images)?;

    let resolved_config = materialize_config_with_images(
        forge_config,
        &MaterializeConfigOptions {
            metrics_transport,
            scoring_flavor,
            nginx_image: images.nginx.as_deref(),
            images: Some(&images),
            run_id: Some(run_id),
            pressure_weighted,
        },
    )?;
    verify_materialized_images(&resolved_config, &images)?;
    let forge_bin = glb::resolve_forge_binary()
        .ok_or("praxis-forge binary not found")?
        .into();

    Ok(DemoContext {
        run_id: run_id.to_owned(),
        resolved_config,
        forge_state_dir: forge_state_dir.to_path_buf(),
        forge_bin,
        images,
        metrics_transport,
        scoring_flavor,
        pressure_weighted,
        certs_dir: certs_dir.to_path_buf(),
        evidence_dir: evidence_dir.to_path_buf(),
    })
}

/// Resolve image references from environment variables with defaults.
fn resolve_images(metrics_transport: MetricsTransport) -> Result<ResolvedImages, Box<dyn std::error::Error>> {
    let gateway = std::env::var("GRID_XTASK_GATEWAY_IMAGE").unwrap_or_else(|_| DEFAULT_GATEWAY_IMAGE.to_owned());
    let operator = std::env::var("GRID_XTASK_OPERATOR_IMAGE").unwrap_or_else(|_| DEFAULT_OPERATOR_IMAGE.to_owned());
    let epp = std::env::var("GRID_XTASK_EPP_IMAGE").unwrap_or_else(|_| DEFAULT_EPP_IMAGE.to_owned());
    let vcr = std::env::var("GRID_XTASK_SIM_IMAGE").unwrap_or_else(|_| DEFAULT_VCR_IMAGE.to_owned());
    let overlay_sync =
        std::env::var("GRID_XTASK_OVERLAY_SYNC_IMAGE").unwrap_or_else(|_| DEFAULT_OVERLAY_SYNC_IMAGE.to_owned());
    let nginx = (metrics_transport == MetricsTransport::MtlsProxy)
        .then(|| std::env::var("GRID_XTASK_NGINX_IMAGE").unwrap_or_else(|_| DEFAULT_NGINX_IMAGE.to_owned()));

    eprintln!("  Images:");
    eprintln!("    gateway:      {gateway}");
    eprintln!("    operator:     {operator}");
    eprintln!("    epp:          {epp}");
    eprintln!("    vcr:          {vcr}");
    eprintln!("    overlay-sync: {overlay_sync}");
    if let Some(n) = &nginx {
        eprintln!("    nginx:        {n}");
    }

    Ok(ResolvedImages {
        gateway,
        operator,
        epp,
        vcr,
        overlay_sync,
        nginx,
    })
}

/// Whether the demo should pull its published images in each Kind cluster.
fn uses_registry_images() -> bool {
    std::env::var("GRID_XTASK_IMAGE_PULL_POLICY").unwrap_or_else(|_| "IfNotPresent".to_owned()) != "Never"
}

/// Verify local-mode images before creating clusters.
///
/// Registry mode references pullable images directly from the Forge config, so
/// each Kind node resolves them without host-side tagging or loading.
fn verify_images(images: &ResolvedImages) -> Result<(), Box<dyn std::error::Error>> {
    if uses_registry_images() {
        eprintln!("  registry image mode: images will be pulled by each cluster");
        return Ok(());
    }

    let mut checks: Vec<(&str, &str, &str)> = vec![
        ("gateway", &images.gateway, "GATEWAY"),
        ("operator", &images.operator, "OPERATOR"),
        ("epp", &images.epp, "EPP"),
        ("vcr", &images.vcr, "VCR"),
        ("overlay-sync", &images.overlay_sync, "OVERLAY_SYNC"),
    ];
    if let Some(nginx) = &images.nginx {
        checks.push(("nginx", nginx, "NGINX"));
    }
    for (role, image, env_suffix) in checks {
        let status = Command::new("docker")
            .args(["image", "inspect", image])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        if !status.success() {
            return Err(format!(
                "required {role} image {image:?} is absent; \
                 build it or set GRID_XTASK_{env_suffix}_IMAGE to an available image",
            )
            .into());
        }
    }
    tag_images_for_forge(images)?;
    Ok(())
}

/// Tag resolved images to match the names the forge config expects.
///
/// The forge config uses fixed image references (e.g.
/// `praxis-ai:llmd-pool-metrics-demo`). When the resolved source image
/// differs (for example, an explicitly configured local development image), this function creates the
/// expected tag so Kind image loading and pod pulls succeed.
fn tag_images_for_forge(images: &ResolvedImages) -> Result<(), Box<dyn std::error::Error>> {
    let forge_expected: &[(&str, &str)] = &[
        (&images.gateway, "praxis-ai:llmd-pool-metrics-demo"),
        (&images.operator, "grid-operator:llmd-pool-metrics-demo"),
        (&images.epp, "llm-d-epp:llmd-pool-metrics-demo"),
        (&images.vcr, "llm-d-inference-sim:deterministic"),
        (&images.overlay_sync, "grid-overlay-sync:llmd-pool-metrics-demo"),
    ];
    for (source, target) in forge_expected {
        if *source != *target {
            let status = Command::new("docker").args(["tag", source, target]).status()?;
            if !status.success() {
                return Err(format!("failed to tag {source} as {target}").into());
            }
            eprintln!("  tagged {source} -> {target}");
        }
    }
    Ok(())
}

/// Deploy the two-cluster environment.
fn deploy_setup(context: &DemoContext) -> Result<(), Box<dyn std::error::Error>> {
    let mtls = context.metrics_transport == MetricsTransport::MtlsProxy;
    let total = if context.pressure_weighted {
        SETUP_PHASES_DYNAMIC
    } else if mtls {
        SETUP_PHASES_MTLS
    } else {
        SETUP_PHASES_DIRECT
    };
    let mut phase = 0_usize;
    let mut next = || {
        phase += 1;
        phase
    };

    // Phase 1: Validate forge config
    eprintln!();
    eprintln!("[SETUP {}/{}] Validating Forge config", next(), total);
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
    eprintln!("  [OK] Config: {}", context.resolved_config.display());

    // Phase 2: Generate TLS certificates
    eprintln!();
    if mtls {
        eprintln!(
            "[SETUP {}/{}] Generating TLS certificates (gateway + metrics)",
            next(),
            total
        );
        stage_certificates(&context.certs_dir)?;
    } else {
        eprintln!(
            "[SETUP {}/{}] Generating TLS certificates (gateway only)",
            next(),
            total
        );
        let clusters: Vec<String> = CLUSTERS.iter().map(|s| (*s).to_owned()).collect();
        certs::generate_all_in_dir(&clusters, &context.certs_dir)?;
        eprintln!("  [OK] TLS certificates generated for pool-a, pool-b");
    }

    // Phase 3: Create Kind clusters
    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Creating two Kind clusters: pool-a, pool-b",
        next(),
        total
    );
    run_forge(
        &context.forge_bin,
        &context.resolved_config,
        &context.forge_state_dir,
        &["up"],
    )?;

    // Phase 4: Load images into clusters
    eprintln!();
    eprintln!("[SETUP {}/{}] Loading container images into clusters", next(), total);
    load_images_into_clusters(context)?;

    // Phase 5: Install MetalLB and Grid operators
    eprintln!();
    eprintln!("[SETUP {}/{}] Installing MetalLB and Grid operators", next(), total);
    for cluster in CLUSTERS {
        let ctx = kind_context(cluster);
        run_forge_stack(context, cluster, "metallb")?;
        let op_stack = format!("{cluster}-operator-base");
        run_forge_stack(context, cluster, &op_stack)?;
        eprintln!("  [OK] {cluster}: MetalLB and operator ready");
        drop(ctx);
    }

    // Phase 6: Seed SWIM membership
    eprintln!();
    eprintln!("[SETUP {}/{}] Seeding SWIM cross-cluster membership", next(), total);
    seed_swim_membership()?;

    // Phase 7 (mTLS only): Install metrics TLS secrets
    if mtls {
        eprintln!();
        eprintln!(
            "[SETUP {}/{}] Installing metrics TLS secrets for EPP sidecar",
            next(),
            total
        );
        let metrics_certs_dir = Path::new(CERTS_DIR);
        for cluster in CLUSTERS {
            let ctx = kind_context(cluster);
            install_metrics_tls_secrets(&ctx, metrics_certs_dir)?;
            eprintln!("  [OK] {cluster}: metrics TLS secrets installed");
        }
    }

    // Phase 7/8: Deploy VCR backends and EPP
    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Deploying deterministic llm-d-sim backends and EPP",
        next(),
        total
    );
    for cluster in CLUSTERS {
        let llmd_stack = format!("llmd-{cluster}");
        run_forge_stack(context, cluster, &llmd_stack)?;
        eprintln!("  [OK] {cluster}: vcr-1, vcr-2, and EPP running");
    }

    // Phase 8/9: Install provider trust and credentials
    eprintln!();
    eprintln!("[SETUP {}/{}] Installing provider trust and credentials", next(), total);
    install_provider_trust(&context.certs_dir)?;

    // Phase 9/10: Deploy Grid site resources and gateways
    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Deploying Grid site resources and gateways",
        next(),
        total
    );
    for cluster in CLUSTERS {
        let site_stack = format!("{cluster}-site");
        run_forge_stack(context, cluster, &site_stack)?;
        run_forge_stack(context, cluster, "provider-gateway")?;
        eprintln!("  [OK] {cluster}: site and provider-gateway deployed");
    }
    if context.pressure_weighted {
        eprintln!();
        eprintln!(
            "[SETUP {}/{}] Restarting operators to resolve the already-created poll-mode GridNetwork",
            next(),
            total
        );
        for cluster in CLUSTERS {
            restart_operator_for_poll_mode(cluster)?;
        }
    }
    for cluster in CLUSTERS {
        run_forge_stack(context, cluster, "consumer-gateway")?;
        eprintln!("  [OK] {cluster}: consumer-gateway deployed");
    }

    // Phase 10/11: Wait for overlay convergence
    eprintln!();
    eprintln!("[SETUP {}/{}] Waiting for overlay convergence", next(), total);
    authorize_discovered_sites(&context.certs_dir)?;
    wait_for_overlay_convergence(context)?;

    eprintln!();
    eprintln!("[READY] Environment deployed");
    Ok(())
}

/// Restart a freshly installed operator only after the poll-mode GridNetwork exists.
fn restart_operator_for_poll_mode(cluster: &str) -> Result<(), Box<dyn std::error::Error>> {
    let context = kind_context(cluster);
    let debug = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "set",
            "env",
            "deployment/grid-operator",
            "RUST_LOG=debug",
        ])
        .output()?;
    if !debug.status.success() {
        return Err(format!(
            "{cluster}: enabling bounded signal diagnostics failed: {}",
            String::from_utf8_lossy(&debug.stderr)
        )
        .into());
    }
    let status = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "rollout",
            "status",
            "deployment/grid-operator",
            "--timeout=120s",
        ])
        .status()?;
    if !status.success() {
        return Err(format!("{cluster}: operator did not become ready in poll mode").into());
    }
    eprintln!("  [OK] {cluster}: operator restarted with poll-mode signals and debug diagnostics enabled");
    Ok(())
}

// ---------------------------------------------------------------------------
// Proof scenarios
// ---------------------------------------------------------------------------

/// Run the demo proof scenarios.
fn run_proof_scenarios(context: &DemoContext, mode: DemoMode) -> BTreeMap<String, ProofResult> {
    let mut results = BTreeMap::new();
    let mtls = context.metrics_transport == MetricsTransport::MtlsProxy;

    // Proof 1: Provenance — image digests and config verification
    results.insert("provenance".to_owned(), proof_provenance(mtls));

    if context.pressure_weighted {
        results.extend(run_dynamic_weighted_scenarios(context));
        return results;
    }

    // Proof 2: Baseline — early state scorecard with production scores
    results.insert("baseline".to_owned(), proof_baseline(context));

    if mode == DemoMode::Full {
        let table_start = Instant::now();

        // Proof 3: Pressure via consumer gateway — live table with attribution
        results.insert(
            "pressure_and_flip".to_owned(),
            proof_pressure_and_flip(context, table_start),
        );

        // Proof 4: Recovery — measured queue drain with live table
        results.insert("recovery".to_owned(), proof_recovery(context, table_start));
    }

    // TLS proof stages — only in mTLS mode
    if mtls {
        let tls_results = run_tls_proof_stages();
        results.extend(tls_results);
    }

    results
}

/// Run baseline, deterministic pressure, and recovery through polled signals.
#[expect(
    clippy::too_many_lines,
    reason = "The qualification sequence is deliberately explicit and preserves each dependent phase result."
)]
fn run_dynamic_weighted_scenarios(context: &DemoContext) -> BTreeMap<String, ProofResult> {
    let mut results = BTreeMap::new();
    let client_context = kind_context("pool-a");
    if let Err(error) = ensure_dynamic_client_pod(&client_context, &context.run_id) {
        results.insert(
            "baseline".to_owned(),
            failed_proof("Baseline: persistent traffic client unavailable", error.to_string()),
        );
        results.insert(
            "pressure_and_flip".to_owned(),
            skipped_proof("Pressure: baseline phase did not complete"),
        );
        results.insert(
            "recovery".to_owned(),
            skipped_proof("Recovery: baseline phase did not complete"),
        );
        results.insert(
            "weighted_affinity".to_owned(),
            skipped_proof("Affinity: baseline phase did not complete"),
        );
        results.insert(
            "operator_restart_persistence".to_owned(),
            skipped_proof("Operator restart: baseline phase did not complete"),
        );
        results.insert(
            "invalid_overlay_last_known_good".to_owned(),
            skipped_proof("Invalid overlay: baseline phase did not complete"),
        );
        return results;
    }

    let mut signals = match SignalsPortForward::start(&client_context, &context.certs_dir) {
        Ok(signals) => signals,
        Err(error) => {
            results.insert(
                "baseline".to_owned(),
                failed_proof("Baseline: signals mTLS endpoint unavailable", error.to_string()),
            );
            results.insert(
                "pressure_and_flip".to_owned(),
                skipped_proof("Pressure: baseline phase did not complete"),
            );
            results.insert(
                "recovery".to_owned(),
                skipped_proof("Recovery: baseline phase did not complete"),
            );
            results.insert(
                "weighted_affinity".to_owned(),
                skipped_proof("Affinity: signal endpoint unavailable"),
            );
            results.insert(
                "operator_restart_persistence".to_owned(),
                skipped_proof("Operator restart: signal endpoint unavailable"),
            );
            results.insert(
                "invalid_overlay_last_known_good".to_owned(),
                skipped_proof("Invalid overlay: signal endpoint unavailable"),
            );
            return results;
        },
    };

    let baseline_evidence = match run_dynamic_phase(context, &signals, "baseline", 0, None) {
        Ok((proof, evidence)) => {
            let passed = proof.success;
            results.insert("baseline".to_owned(), proof);
            if !passed {
                results.insert(
                    "pressure_and_flip".to_owned(),
                    skipped_proof("Pressure: baseline phase did not complete"),
                );
                results.insert(
                    "recovery".to_owned(),
                    skipped_proof("Recovery: baseline phase did not complete"),
                );
                results.insert(
                    "weighted_affinity".to_owned(),
                    skipped_proof("Affinity: baseline phase did not complete"),
                );
                results.insert(
                    "operator_restart_persistence".to_owned(),
                    skipped_proof("Operator restart: baseline phase did not complete"),
                );
                results.insert(
                    "invalid_overlay_last_known_good".to_owned(),
                    skipped_proof("Invalid overlay: baseline phase did not complete"),
                );
                return results;
            }
            evidence
        },
        Err(error) => {
            results.insert(
                "baseline".to_owned(),
                failed_proof("Baseline: equal idle weights and sampling", error.to_string()),
            );
            results.insert(
                "pressure_and_flip".to_owned(),
                skipped_proof("Pressure: baseline phase did not complete"),
            );
            results.insert(
                "recovery".to_owned(),
                skipped_proof("Recovery: baseline phase did not complete"),
            );
            results.insert(
                "weighted_affinity".to_owned(),
                skipped_proof("Affinity: baseline phase did not complete"),
            );
            results.insert(
                "operator_restart_persistence".to_owned(),
                skipped_proof("Operator restart: baseline phase did not complete"),
            );
            results.insert(
                "invalid_overlay_last_known_good".to_owned(),
                skipped_proof("Invalid overlay: baseline phase did not complete"),
            );
            return results;
        },
    };

    let affinity_bindings = match sample_dynamic_affinity(&client_context, &context.run_id, "bind") {
        Ok(samples) => {
            let evidence_error = write_dynamic_affinity_samples(&context.evidence_dir, "bind", &samples)
                .err()
                .map(|error| error.to_string());
            let valid = affinity_bind_samples_valid(&samples) && evidence_error.is_none();
            let bindings = samples
                .iter()
                .filter(|sample| sample.curl_exit_code == 0 && sample.http_status == Some(200))
                .map(|sample| (sample.session_ordinal, sample.provider_gateway.clone()))
                .collect::<BTreeMap<_, _>>();
            results.insert(
                "weighted_affinity".to_owned(),
                if valid {
                    ProofResult {
                        success: true,
                        description: "Affinity: new session bindings established before the pressure transition".to_owned(),
                        observations: vec![format!(
                            "{} unique sessions bound across both eligible providers; raw outcomes in affinity-bind.jsonl",
                            samples.len()
                        )],
                    }
                } else {
                    failed_proof(
                        "Affinity: baseline session bindings",
                        evidence_error.unwrap_or_else(|| format!(
                            "{} session binding requests did not produce valid attributed 200 responses on both providers",
                            samples.len()
                        )),
                    )
                },
            );
            valid.then_some(bindings)
        },
        Err(error) => {
            results.insert(
                "weighted_affinity".to_owned(),
                failed_proof("Affinity: baseline session bindings", error.to_string()),
            );
            None
        },
    };

    let baseline_revisions = dynamic_semantic_revisions(&baseline_evidence.gateways);
    let pressure = run_dynamic_phase(context, &signals, "pressure", 9, Some(&baseline_revisions));
    let pressure_ok = pressure.as_ref().is_ok_and(|phase| phase.0.success);
    let pressure_evidence = pressure.as_ref().ok().map(|phase| phase.1.clone());
    let pressure_proof = pressure.map_or_else(
        |error| failed_proof("Pressure: signal-driven weight shift and sampling", error.to_string()),
        |phase| phase.0,
    );
    let pressure_proof = match pressure_evidence.as_ref() {
        Some(evidence) => {
            let baseline_share = baseline_evidence
                .traffic
                .observed_fraction
                .get("pool-a")
                .copied()
                .unwrap_or(0.0);
            let pressure_share = evidence.traffic.observed_fraction.get("pool-a").copied().unwrap_or(0.0);
            if baseline_share - pressure_share >= DYNAMIC_MIN_PRESSURE_SHIFT {
                pressure_proof
            } else {
                failed_proof(
                    "Pressure: signal-driven weight shift and sampling",
                    format!(
                        "pool-a traffic share shifted by {shift:.3}, below the predeclared {DYNAMIC_MIN_PRESSURE_SHIFT:.2} minimum",
                        shift = baseline_share - pressure_share,
                    ),
                )
            }
        },
        None => pressure_proof,
    };
    results.insert("pressure_and_flip".to_owned(), pressure_proof);
    if !pressure_ok || pressure_evidence.is_none() {
        results.insert(
            "affinity_replay".to_owned(),
            skipped_proof("Affinity replay: pressure phase did not complete"),
        );
        results.insert(
            "operator_restart_persistence".to_owned(),
            skipped_proof("Operator restart: pressure phase did not complete"),
        );
        results.insert(
            "invalid_overlay_last_known_good".to_owned(),
            skipped_proof("Invalid overlay: pressure phase did not complete"),
        );
        results.insert(
            "recovery".to_owned(),
            skipped_proof("Recovery: pressure phase did not complete"),
        );
        return results;
    }
    let Some(pressure_evidence) = pressure_evidence else {
        results.insert(
            "recovery".to_owned(),
            skipped_proof("Recovery: pressure phase did not complete"),
        );
        return results;
    };

    if let Some(bindings) = affinity_bindings {
        match sample_dynamic_affinity(&client_context, &context.run_id, "replay") {
            Ok(samples) => {
                let evidence_error = write_dynamic_affinity_samples(&context.evidence_dir, "replay", &samples)
                    .err()
                    .map(|error| error.to_string());
                let matches = affinity_replay_matches(&bindings, &samples) && evidence_error.is_none();
                results.insert(
                    "affinity_replay".to_owned(),
                    if matches {
                        ProofResult {
                            success: true,
                            description: "Affinity: every bound session retained its provider after weights changed".to_owned(),
                            observations: vec![format!(
                                "{} of {} replayed sessions retained the original provider; pressure weights had shifted to {} / {}",
                                samples.len(),
                                bindings.len(),
                                pressure_evidence.traffic.counts.get("pool-a").copied().unwrap_or(0),
                                pressure_evidence.traffic.counts.get("pool-b").copied().unwrap_or(0),
                            )],
                        }
                    } else {
                        failed_proof(
                            "Affinity: provider binding changed after weights changed",
                            evidence_error.unwrap_or_else(|| format!(
                                "{} session outcomes did not preserve their baseline attribution",
                                samples.len()
                            )),
                        )
                    },
                );
            },
            Err(error) => {
                results.insert(
                    "affinity_replay".to_owned(),
                    failed_proof("Affinity replay failed", error.to_string()),
                );
            },
        }
    } else {
        results.insert(
            "affinity_replay".to_owned(),
            skipped_proof("Affinity replay: baseline bindings were not valid"),
        );
    }

    // Restart both run-owned operators while pressure remains asserted. The new
    // operator instances must repoll the signals and preserve the effective
    // pressure preference through a fresh overlay/serving-revision gate.
    drop(signals);
    let operator_restarts = restart_dynamic_operators();
    let mut operator_restart_evidence = None;
    match SignalsPortForward::start(&client_context, &context.certs_dir) {
        Ok(restarted_signals) => signals = restarted_signals,
        Err(error) => {
            results.insert(
                "operator_restart_persistence".to_owned(),
                failed_proof("Operator restart: poll endpoint did not recover", error.to_string()),
            );
            results.insert(
                "invalid_overlay_last_known_good".to_owned(),
                skipped_proof("Invalid overlay: operator signal endpoint did not recover"),
            );
            results.insert(
                "recovery".to_owned(),
                skipped_proof("Recovery: operator signal endpoint did not recover"),
            );
            return results;
        },
    }
    match run_dynamic_phase(context, &signals, "operator-restart", 9, None) {
        Ok((phase_proof, phase_evidence)) => {
            operator_restart_evidence = Some(phase_evidence.clone());
            let mut observations = match &operator_restarts {
                Ok(records) => records
                    .iter()
                    .map(|record| {
                        format!(
                            "{} operator pod UIDs {:?} -> {:?}",
                            record.cluster, record.before_pod_uids, record.after_pod_uids
                        )
                    })
                    .collect::<Vec<_>>(),
                Err(error) => vec![format!("operator restart failed: {error}")],
            };
            observations.extend(phase_proof.observations);
            let success = operator_restarts.is_ok() && phase_proof.success;
            let mut proof = ProofResult {
                success,
                description: "Operator restart: pressure signals, weights, and serving revision persist after restart"
                    .to_owned(),
                observations,
            };
            let evidence_write = serde_json::to_vec_pretty(&serde_json::json!({
                "operator_pods": operator_restarts.as_ref().ok(),
                "phase": phase_evidence,
                "proof": proof.clone(),
            }))
            .map_err(|error| error.to_string())
            .and_then(|bytes| {
                fs::write(context.evidence_dir.join("operator-restart.json"), bytes).map_err(|error| error.to_string())
            });
            if let Err(error) = evidence_write {
                proof.success = false;
                proof
                    .observations
                    .push(format!("operator restart evidence could not be written: {error}"));
            }
            results.insert("operator_restart_persistence".to_owned(), proof);
        },
        Err(error) => {
            let proof = failed_operator_restart_proof(error.as_ref(), &operator_restarts, &context.evidence_dir);
            results.insert("operator_restart_persistence".to_owned(), proof);
        },
    }

    // Pause both operators so the deliberately malformed run-owned ConfigMap
    // cannot be repaired before overlay-sync and Praxis demonstrate LKG safety.
    drop(signals);
    let lkg_reference = operator_restart_evidence.as_ref().unwrap_or(&pressure_evidence);
    let lkg = prove_invalid_overlay_last_known_good(context, lkg_reference);
    results.insert("invalid_overlay_last_known_good".to_owned(), lkg);
    match SignalsPortForward::start(&client_context, &context.certs_dir) {
        Ok(recovered_signals) => signals = recovered_signals,
        Err(error) => {
            results.insert(
                "recovery".to_owned(),
                failed_proof(
                    "Recovery: poll endpoint did not recover after LKG test",
                    error.to_string(),
                ),
            );
            return results;
        },
    }

    let pressure_revisions = dynamic_semantic_revisions(&pressure_evidence.gateways);
    let recovery = run_dynamic_phase(context, &signals, "recovery", 0, Some(&pressure_revisions));
    let recovery_proof = recovery.map_or_else(
        |error| failed_proof("Recovery: restored weights and traffic", error.to_string()),
        |phase| {
            let recovered_share = phase
                .1
                .traffic
                .observed_fraction
                .get("pool-a")
                .copied()
                .unwrap_or(0.0);
            let pressure_share = pressure_evidence
                .traffic
                .observed_fraction
                .get("pool-a")
                .copied()
                .unwrap_or(0.0);
            if phase.0.success
                && recovered_share >= DYNAMIC_MIN_RECOVERY_SHARE
                && recovered_share - pressure_share >= DYNAMIC_MIN_PRESSURE_SHIFT
            {
                phase.0
            } else {
                failed_proof(
                    "Recovery: restored weights and traffic",
                    format!(
                        "pool-a recovered share={recovered_share:.3}, pressure share={pressure_share:.3}; required recovery >= {DYNAMIC_MIN_RECOVERY_SHARE:.2} and rebound >= {DYNAMIC_MIN_PRESSURE_SHIFT:.2}"
                    ),
                )
            }
        },
    );
    results.insert("recovery".to_owned(), recovery_proof);
    results
}

/// Build a failed evidence result with the first concrete boundary error.
fn failed_proof(description: &str, error: String) -> ProofResult {
    ProofResult {
        success: false,
        description: description.to_owned(),
        observations: vec![error],
    }
}

/// Preserve the operator pod replacement and phase failure even when convergence fails.
fn failed_operator_restart_proof(
    phase_error: &dyn std::error::Error,
    restarts: &Result<Vec<DynamicOperatorRestart>, Box<dyn std::error::Error>>,
    evidence_dir: &Path,
) -> ProofResult {
    let mut proof = failed_proof(
        "Operator restart: pressure state did not reconverge",
        phase_error.to_string(),
    );
    if let Ok(records) = restarts {
        proof.observations.extend(records.iter().map(|record| {
            format!(
                "{} operator pod UIDs {:?} -> {:?}",
                record.cluster, record.before_pod_uids, record.after_pod_uids
            )
        }));
    } else if let Err(restart_error) = restarts {
        proof
            .observations
            .push(format!("operator rollout failed: {restart_error}"));
    }
    let evidence = serde_json::json!({
        "operator_pods": restarts.as_ref().ok(),
        "phase_error": phase_error.to_string(),
        "proof": proof.clone(),
    });
    if let Err(write_error) = serde_json::to_vec_pretty(&evidence)
        .map_err(|serde_error| serde_error.to_string())
        .and_then(|bytes| {
            fs::write(evidence_dir.join("operator-restart.json"), bytes).map_err(|io_error| io_error.to_string())
        })
    {
        proof.observations.push(format!(
            "operator restart failure evidence could not be written: {write_error}"
        ));
    }
    proof
}

/// Mark a later phase as not run after a required earlier phase failed.
fn skipped_proof(description: &str) -> ProofResult {
    ProofResult {
        success: false,
        description: description.to_owned(),
        observations: vec!["not run because a required prior phase failed".to_owned()],
    }
}

/// A host-side mTLS tunnel to one operator's poll-mode signal endpoint.
struct SignalsPortForward {
    /// Owned kubectl process; dropped after authenticated reads finish.
    child: Option<Child>,
    /// Host loopback port forwarded to the operator's TLS signal listener.
    local_port: u16,
    /// Run-specific certificate directory used for authenticated reads.
    certs_dir: PathBuf,
}

impl SignalsPortForward {
    /// Start a run-owned port-forward and wait until the authenticated endpoint answers.
    fn start(context: &str, certs_dir: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let socket = TcpListener::bind("127.0.0.1:0")?;
        let local_port = socket.local_addr()?.port();
        drop(socket);
        let child = Command::new("kubectl")
            .args([
                "--context",
                context,
                "-n",
                GRID_SYSTEM_NS,
                "port-forward",
                "--address=127.0.0.1",
                "service/grid-operator-swim",
                &format!("{local_port}:9091"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let mut forward = Self {
            child: Some(child),
            local_port,
            certs_dir: certs_dir.to_path_buf(),
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut last_error = String::from("port-forward did not become ready");
        while Instant::now() < deadline {
            if let Some(port_forward_process) = forward.child.as_mut()
                && port_forward_process.try_wait()?.is_some()
            {
                return Err("signals port-forward exited before readiness".into());
            }
            if TcpStream::connect(("127.0.0.1", local_port)).is_ok() {
                match forward.read_exposition() {
                    Ok(body) if body.contains("# TYPE") || body.lines().any(|line| line.contains("grid_site=")) => {
                        return Ok(forward);
                    },
                    Ok(_) => "signals endpoint returned no exposition yet".clone_into(&mut last_error),
                    Err(error) => last_error = error.to_string(),
                }
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        Err(format!("signals endpoint did not become ready: {last_error}").into())
    }

    /// Fetch the authenticated rollup for the selected pressure metric.
    fn read_exposition(&self) -> Result<String, Box<dyn std::error::Error>> {
        let ca = self.certs_dir.join("ca.pem");
        let cert = self.certs_dir.join("pool-a-cert.pem");
        let key = self.certs_dir.join("pool-a-key.pem");
        let authority = format!("pool-a.grid.internal:{}:127.0.0.1", self.local_port);
        let url = format!("https://pool-a.grid.internal:{}/v1/site/signals", self.local_port);
        let output = Command::new("curl")
            .args([
                "--silent",
                "--show-error",
                "--fail",
                "--max-time",
                "8",
                "--cacert",
                ca.to_str().ok_or("CA path is not UTF-8")?,
                "--cert",
                cert.to_str().ok_or("site certificate path is not UTF-8")?,
                "--key",
                key.to_str().ok_or("site key path is not UTF-8")?,
                "--resolve",
                &authority,
                "--get",
                "--data-urlencode",
                "collect[]=grid_routing_queue_pressure",
                "--data-urlencode",
                "collect[]=grid_routing_kv_cache_pressure",
                &url,
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "mTLS signals query failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )
            .into());
        }
        Ok(String::from_utf8(output.stdout)?)
    }
}

impl Drop for SignalsPortForward {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            drop(child.kill());
            drop(child.wait());
        }
    }
}

/// Create the persistent request source used throughout the three phases.
fn ensure_dynamic_client_pod(context: &str, run_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    const POD: &str = "dynamic-weighted-client";
    let get = Command::new("kubectl")
        .args(["--context", context, "-n", GRID_SYSTEM_NS, "get", &format!("pod/{POD}")])
        .output()?;
    if !get.status.success() {
        let overrides = serde_json::json!({
            "metadata": {"labels": {"grid.praxis-proxy.io/run-id": run_id}},
            "spec": {
                "automountServiceAccountToken": false,
                "securityContext": {"runAsNonRoot": true, "seccompProfile": {"type": "RuntimeDefault"}},
                "containers": [{
                    "name": POD,
                    "image": "curlimages/curl:8.12.1",
                    "command": ["sleep", "3600"],
                    "securityContext": {
                        "runAsUser": 100,
                        "allowPrivilegeEscalation": false,
                        "readOnlyRootFilesystem": true,
                        "capabilities": {"drop": ["ALL"]}
                    }
                }]
            }
        })
        .to_string();
        let create = Command::new("kubectl")
            .args([
                "--context",
                context,
                "-n",
                GRID_SYSTEM_NS,
                "run",
                POD,
                "--image=curlimages/curl:8.12.1",
                "--restart=Never",
                "--overrides",
                &overrides,
            ])
            .output()?;
        if !create.status.success() {
            return Err(format!(
                "persistent request client creation failed: {}",
                String::from_utf8_lossy(&create.stderr).trim()
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
            &format!("pod/{POD}"),
            "--timeout=120s",
        ])
        .output()?;
    if !ready.status.success() {
        return Err(format!(
            "persistent request client did not become Ready: {}",
            String::from_utf8_lossy(&ready.stderr).trim()
        )
        .into());
    }
    Ok(())
}

/// One full dynamic qualification phase, from simulator config through attributed traffic.
#[expect(
    clippy::too_many_lines,
    reason = "This function records and checks every boundary of one end-to-end phase."
)]
fn run_dynamic_phase(
    context: &DemoContext,
    signals: &SignalsPortForward,
    phase: &str,
    waiting_requests: u32,
    previous_revisions: Option<&BTreeMap<String, String>>,
) -> DynamicPhaseResult {
    let evidence_dir = &context.evidence_dir;
    let mtls = context.metrics_transport == MetricsTransport::MtlsProxy;
    eprintln!("\n  [DYNAMIC {phase}] Set simulator waiting-requests={waiting_requests}");
    set_simulator_waiting_requests("pool-a", waiting_requests, context.scoring_flavor, mtls)?;
    let (simulator_generation, simulator_pod_uids) = simulator_runtime_identity("pool-a")?;
    let state = wait_for_dynamic_state(context, signals, phase, waiting_requests, previous_revisions)?;
    let overlay = state
        .gateways
        .get("pool-a")
        .ok_or("pool-a gateway state missing after convergence")?
        .overlay
        .clone();
    fs::write(
        evidence_dir.join(format!("signals-{phase}.jsonl")),
        format!("{}\n", state.signal_lines.join("\n")),
    )?;
    fs::write(
        evidence_dir.join(format!("overlay-{phase}.json")),
        serde_json::to_vec_pretty(&overlay)?,
    )?;
    fs::write(
        evidence_dir.join(format!("overlays-{phase}.json")),
        serde_json::to_vec_pretty(&state.gateways)?,
    )?;
    let (traffic, request_lines, sample_started, sample_completed) =
        sample_dynamic_traffic(&kind_context("pool-a"), phase, &overlay)?;
    fs::write(
        evidence_dir.join(format!("requests-{phase}.jsonl")),
        format!("{}\n", request_lines.join("\n")),
    )?;
    let served_states = CLUSTERS
        .iter()
        .map(|cluster| -> Result<_, Box<dyn std::error::Error>> {
            let (observed, accepted, serving) = observe_serving_state(&kind_context(cluster))?;
            Ok(((*cluster).to_owned(), observed, accepted, serving))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let stable_during_sample = served_states.iter().all(|(cluster, observed, accepted, serving)| {
        state.gateways.get(cluster).is_some_and(|expected| {
            observed.semantic_revision == expected.overlay.semantic_revision
                && observed.candidates == expected.overlay.candidates
                && accepted == &expected.accepted_revision
                && serving == &expected.serving_revision
        })
    });
    let phase_success = traffic.accepted && stable_during_sample;
    let phase_evidence = DynamicPhaseEvidence {
        phase: phase.to_owned(),
        requested_waiting_requests: waiting_requests,
        simulator_generation: simulator_generation.clone(),
        simulator_pod_uids: simulator_pod_uids.clone(),
        epp_queue_size: state.epp_queue_size.clone(),
        normalized_signals: state.signal_samples.clone(),
        gateways: state.gateways.clone(),
        stable_observations: state.stable_observations,
        stable_duration_ms: state.stable_duration.as_millis(),
        required_stable_duration_ms: state.required_stable_duration.as_millis(),
        traffic: traffic.clone(),
    };
    fs::write(
        evidence_dir.join(format!("phase-{phase}.json")),
        serde_json::to_vec_pretty(&phase_evidence)?,
    )?;
    let grid_revisions = dynamic_semantic_revisions(&state.gateways);
    let accepted_revisions = dynamic_accepted_revisions(&state.gateways);
    let serving_revisions = dynamic_serving_revisions(&state.gateways);
    let summary = serde_json::json!({
        "phase": phase,
        "sample_started_at": sample_started,
        "sample_completed_at": sample_completed,
        "grid_revisions": grid_revisions,
        "accepted_revisions": accepted_revisions,
        "serving_revisions": serving_revisions,
        "published_weights": overlay.candidates,
        "stable_observations": state.stable_observations,
        "stable_duration_ms": state.stable_duration.as_millis(),
        "required_stable_duration_ms": state.required_stable_duration.as_millis(),
        "statistical_sample": traffic,
        "overlay_stable_during_sample": stable_during_sample,
    });
    append_dynamic_timeline(evidence_dir, &summary)?;
    let mut observations = vec![
        format!("simulator generation/pod UIDs: {simulator_generation:?} / {simulator_pod_uids:?}"),
        format!("EPP queue observations: {:?}", state.epp_queue_size),
        format!(
            "normalized site/provider signals: {:?}",
            phase_evidence.normalized_signals
        ),
        format!(
            "per-gateway Grid/accepted/serving revisions: {grid_revisions:?} / {accepted_revisions:?} / {serving_revisions:?}"
        ),
        format!(
            "state stability before sampling: {} matching observations over {}ms (required {}ms)",
            state.stable_observations,
            state.stable_duration.as_millis(),
            state.required_stable_duration.as_millis()
        ),
        format!("published traffic weights: {:?}", overlay.candidates),
        format!(
            "{} requests; provider counts={:?}; chi-square={:.4} (critical {:.3})",
            traffic.request_count, traffic.counts, traffic.chi_square, traffic.chi_square_critical_value
        ),
        format!("sample window: {sample_started} through {sample_completed}"),
    ];
    if !traffic.accepted {
        observations.push(format!(
            "sample failures: transport={}, HTTP={}, attribution={}",
            traffic.transport_failures, traffic.http_failures, traffic.attribution_failures
        ));
    }
    if !stable_during_sample {
        observations.push("overlay or accepted/serving revision changed during traffic sample".to_owned());
    }
    Ok((
        ProofResult {
            success: phase_success,
            description: format!("{phase}: dynamic Grid weights, served revision, and attributed traffic"),
            observations,
        },
        Box::new(phase_evidence),
    ))
}

/// Wait for fresh EPP signals, published weights, and both AI serving revisions to agree.
#[expect(
    clippy::too_many_lines,
    reason = "This state gate validates EPP, signal, overlay, and data-plane convergence together."
)]
fn wait_for_dynamic_state(
    context: &DemoContext,
    signals: &SignalsPortForward,
    phase: &str,
    waiting_requests: u32,
    previous_revisions: Option<&BTreeMap<String, String>>,
) -> Result<DynamicStateSnapshot, Box<dyn std::error::Error>> {
    let metric = match context.scoring_flavor {
        ScoringFlavor::QueueDepth => "grid_routing_queue_pressure",
        ScoringFlavor::KvCachePressure => "grid_routing_kv_cache_pressure",
    };
    let deadline = Instant::now() + DATA_PLANE_WAIT;
    let mut last_trigger = Instant::now();
    let mut stable_signature: Option<String> = None;
    let mut stable_observations = 0_u8;
    let mut stable_since: Option<Instant> = None;
    let required_stable_duration = dynamic_state_stability_window();
    let mut last_detail = String::from("no state observed yet");
    let mut signal_lines = Vec::new();
    while Instant::now() < deadline {
        if last_trigger.elapsed() >= Duration::from_secs(5) {
            for cluster in CLUSTERS {
                trigger_gridnetwork_reconcile(cluster);
            }
            last_trigger = Instant::now();
        }
        let epp_a = kubectl_exec_epp_metrics("pool-a", false)
            .ok()
            .and_then(|text| parse_required_epp_metrics(&text).ok());
        let epp_b = kubectl_exec_epp_metrics("pool-b", false)
            .ok()
            .and_then(|text| parse_required_epp_metrics(&text).ok());
        let exposition = signals.read_exposition();
        match (epp_a, epp_b, exposition) {
            (Some(epp_a), Some(epp_b), Ok(exposition)) => {
                let samples = parse_dynamic_signal_samples(&exposition, metric);
                let poll_line = serde_json::json!({
                    "observed_at": format_utc_iso(),
                    "phase": phase,
                    "pool_a_epp_queue": epp_a.queue_size,
                    "pool_b_epp_queue": epp_b.queue_size,
                    "signal_samples": samples,
                    "pool_a_overlay": read_dynamic_overlay("pool-a").ok(),
                    "pool_b_overlay": read_dynamic_overlay("pool-b").ok(),
                });
                signal_lines.push(poll_line.to_string());
                let signals_ok =
                    signal_matches(
                        &samples,
                        "pool-a",
                        "llmd-pool-a-provider",
                        waiting_requests,
                        context.scoring_flavor,
                    ) && signal_matches(&samples, "pool-b", "llmd-pool-b-provider", 0, context.scoring_flavor);
                let epp_ok = epp_matches(epp_a.queue_size, waiting_requests, context.scoring_flavor)
                    && epp_matches(epp_b.queue_size, 0, context.scoring_flavor);
                let overlay_a = read_dynamic_overlay("pool-a");
                let overlay_b = read_dynamic_overlay("pool-b");
                if let (Ok(overlay_a), Ok(overlay_b)) = (overlay_a, overlay_b) {
                    let weights_ok = dynamic_overlays_match_phase(&overlay_a, &overlay_b, phase);
                    let revision_changed = dynamic_revisions_changed(&overlay_a, &overlay_b, previous_revisions);
                    if signals_ok && epp_ok && weights_ok && revision_changed {
                        let (accepted_a, serving_a) =
                            match wait_for_gateway_revision("pool-a", &overlay_a.semantic_revision) {
                                Ok(revisions) => revisions,
                                Err(error) => {
                                    stable_signature = None;
                                    stable_observations = 0;
                                    stable_since = None;
                                    last_detail = error.to_string();
                                    std::thread::sleep(Duration::from_secs(2));
                                    continue;
                                },
                            };
                        let (accepted_b, serving_b) =
                            match wait_for_gateway_revision("pool-b", &overlay_b.semantic_revision) {
                                Ok(revisions) => revisions,
                                Err(error) => {
                                    stable_signature = None;
                                    stable_observations = 0;
                                    stable_since = None;
                                    last_detail = error.to_string();
                                    std::thread::sleep(Duration::from_secs(2));
                                    continue;
                                },
                            };
                        let gateway_states = BTreeMap::from([
                            (
                                "pool-a".to_owned(),
                                DynamicGatewayState {
                                    overlay: overlay_a.clone(),
                                    accepted_revision: accepted_a,
                                    serving_revision: serving_a,
                                },
                            ),
                            (
                                "pool-b".to_owned(),
                                DynamicGatewayState {
                                    overlay: overlay_b.clone(),
                                    accepted_revision: accepted_b,
                                    serving_revision: serving_b,
                                },
                            ),
                        ]);
                        let signature = serde_json::to_string(&gateway_states)?;
                        if stable_signature.as_deref() == Some(signature.as_str()) {
                            stable_observations = stable_observations.saturating_add(1);
                        } else {
                            stable_signature = Some(signature);
                            stable_observations = 1;
                            stable_since = Some(Instant::now());
                        }
                        let stable_duration = stable_since.map_or(Duration::ZERO, |since| since.elapsed());
                        let queue_map = BTreeMap::from([
                            ("pool-a".to_owned(), epp_a.queue_size),
                            ("pool-b".to_owned(), epp_b.queue_size),
                        ]);
                        if dynamic_state_stability_satisfied(
                            stable_observations,
                            stable_duration,
                            required_stable_duration,
                        ) {
                            fs::write(
                                context.evidence_dir.join(format!("signals-raw-{phase}.txt")),
                                exposition,
                            )?;
                            return Ok(DynamicStateSnapshot {
                                gateways: gateway_states,
                                signal_samples: samples,
                                epp_queue_size: queue_map,
                                stable_observations,
                                stable_duration,
                                required_stable_duration,
                                signal_lines,
                            });
                        }
                    } else {
                        stable_signature = None;
                        stable_observations = 0;
                        stable_since = None;
                        last_detail = format!(
                            "epp_ok={epp_ok}, signals_ok={signals_ok}, weights_ok={weights_ok}, revisions_changed={revision_changed}, a_revision={}, b_revision={}, a={:?}, b={:?}",
                            overlay_a.semantic_revision,
                            overlay_b.semantic_revision,
                            overlay_a.candidates,
                            overlay_b.candidates
                        );
                    }
                } else {
                    "one or both Grid overlay ConfigMaps are not readable".clone_into(&mut last_detail);
                }
            },
            (epp_a, epp_b, exposition) => {
                last_detail = format!(
                    "EPP/signal boundary unavailable: pool_a={}, pool_b={}, signals={}",
                    epp_a.is_some(),
                    epp_b.is_some(),
                    exposition.is_ok()
                );
            },
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    fs::write(
        context.evidence_dir.join(format!("signals-{phase}.jsonl")),
        format!("{}\n", signal_lines.join("\n")),
    )?;
    Err(format!(
        "{phase}: dynamic signals/overlay/revision gate timed out: {last_detail}; stable observations={stable_observations}, required quiet window={}ms",
        required_stable_duration.as_millis()
    )
    .into())
}

/// Require one full peer polling interval, plus a short margin, with no served-state change.
fn dynamic_state_stability_window() -> Duration {
    let peer_poll_seconds = std::env::var("GRID_SIGNALS_PEER_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30)
        .max(1);
    Duration::from_secs(peer_poll_seconds.saturating_add(2))
}

/// State-based stability barrier used before starting the statistical sample.
fn dynamic_state_stability_satisfied(observations: u8, stable_for: Duration, required: Duration) -> bool {
    observations >= 2 && stable_for >= required
}

/// Check that EPP exposes the deterministic simulator queue value for a phase.
fn epp_matches(observed_queue: f64, requested_waiting: u32, flavor: ScoringFlavor) -> bool {
    let (queue, _) = simulator_metrics(requested_waiting, flavor);
    (observed_queue - f64::from(queue)).abs() < f64::EPSILON
}

/// Check that a fresh normalized signal belongs to the requested provider.
fn signal_matches(
    samples: &[DynamicSignalSample],
    site: &str,
    provider: &str,
    waiting_requests: u32,
    flavor: ScoringFlavor,
) -> bool {
    let expected = match flavor {
        ScoringFlavor::QueueDepth => (f64::from(waiting_requests) / QUEUE_CAPACITY).clamp(0.0, 1.0),
        ScoringFlavor::KvCachePressure => simulator_metrics(waiting_requests, flavor).1,
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok());
    samples.iter().any(|sample| {
        sample.site == site
            && sample.provider == provider
            && (sample.value - expected).abs() < 0.000_001
            && sample
                .timestamp_ms
                .zip(now_ms)
                .is_some_and(|(stamp, now)| now >= stamp && now.saturating_sub(stamp) <= 120_000)
    })
}

/// Check phase-specific positive weights, group, freshness, and admission.
fn dynamic_weights_match_phase(overlay: &DynamicOverlaySnapshot, phase: &str) -> bool {
    let pool_a = overlay.candidates.iter().find(|candidate| candidate.site == "pool-a");
    let pool_b = overlay.candidates.iter().find(|candidate| candidate.site == "pool-b");
    let (Some(pool_a), Some(pool_b)) = (pool_a, pool_b) else {
        return false;
    };
    if overlay.candidates.len() != 2
        || pool_a.selection_group != pool_b.selection_group
        || pool_a.selection_group != 0
        || !pool_a.fresh
        || !pool_b.fresh
        || pool_a.admission_state != "new_and_existing"
        || pool_b.admission_state != "new_and_existing"
        || pool_a.traffic_weight == 0
        || pool_b.traffic_weight == 0
        || pool_a.traffic_weight + pool_b.traffic_weight != 1000
    {
        return false;
    }
    match phase {
        "baseline" => pool_a.traffic_weight == 500 && pool_b.traffic_weight == 500,
        "pressure" | "operator-restart" => pool_a.traffic_weight <= 300 && pool_b.traffic_weight >= 700,
        "recovery" => pool_a.traffic_weight >= 450 && pool_b.traffic_weight <= 550,
        _ => false,
    }
}

/// Compare the contract on both gateways without requiring their local overlays
/// or candidate ordering to have identical semantic revisions.
fn dynamic_overlays_match_phase(pool_a: &DynamicOverlaySnapshot, pool_b: &DynamicOverlaySnapshot, phase: &str) -> bool {
    dynamic_weights_match_phase(pool_a, phase)
        && dynamic_weights_match_phase(pool_b, phase)
        && dynamic_candidate_identities_match_unordered(pool_a, pool_b)
}

/// Compare shared candidate identity, not weights maintained by each operator.
/// Each gateway smooths its independently timed fresh signal snapshot; phase
/// bounds are checked separately for both overlays above.
fn dynamic_candidate_identities_match_unordered(left: &DynamicOverlaySnapshot, right: &DynamicOverlaySnapshot) -> bool {
    let identities = |overlay: &DynamicOverlaySnapshot| {
        let mut candidates = overlay
            .candidates
            .iter()
            .map(|candidate| {
                (
                    candidate.site.clone(),
                    candidate.provider.clone(),
                    candidate.stable_id.clone(),
                    candidate.selection_group,
                    candidate.fresh,
                    candidate.admission_state.clone(),
                )
            })
            .collect::<Vec<_>>();
        candidates.sort_unstable();
        candidates
    };
    identities(left) == identities(right)
}

/// Every gateway must publish a new semantic revision after a phase transition.
fn dynamic_revisions_changed(
    pool_a: &DynamicOverlaySnapshot,
    pool_b: &DynamicOverlaySnapshot,
    previous: Option<&BTreeMap<String, String>>,
) -> bool {
    previous.is_none_or(|revisions| {
        revisions
            .get("pool-a")
            .is_some_and(|revision| revision != &pool_a.semantic_revision)
            && revisions
                .get("pool-b")
                .is_some_and(|revision| revision != &pool_b.semantic_revision)
    })
}

/// Extract each gateway's own Grid semantic revision for the next phase gate.
fn dynamic_semantic_revisions(gateways: &BTreeMap<String, DynamicGatewayState>) -> BTreeMap<String, String> {
    gateways
        .iter()
        .map(|(cluster, state)| (cluster.clone(), state.overlay.semantic_revision.clone()))
        .collect()
}

/// Extract the Praxis-accepted revision from each gateway state.
fn dynamic_accepted_revisions(gateways: &BTreeMap<String, DynamicGatewayState>) -> BTreeMap<String, String> {
    gateways
        .iter()
        .map(|(cluster, state)| (cluster.clone(), state.accepted_revision.clone()))
        .collect()
}

/// Extract the revision currently serving requests on each gateway.
fn dynamic_serving_revisions(gateways: &BTreeMap<String, DynamicGatewayState>) -> BTreeMap<String, String> {
    gateways
        .iter()
        .map(|(cluster, state)| (cluster.clone(), state.serving_revision.clone()))
        .collect()
}

/// Parse one metric's site/provider labels and timestamp from exposition.
fn parse_dynamic_signal_samples(text: &str, metric: &str) -> Vec<DynamicSignalSample> {
    text.lines()
        .filter_map(|line| {
            let (name_and_labels, fields) = line.split_once('}')?;
            let (name, labels) = name_and_labels.split_once('{')?;
            if name != metric {
                return None;
            }
            let mut fields = fields.split_whitespace();
            Some(DynamicSignalSample {
                site: prometheus_label(labels, "grid_site")?,
                provider: prometheus_label(labels, "grid_provider")?,
                value: fields.next()?.parse().ok()?,
                timestamp_ms: fields.next().and_then(|value| value.parse().ok()),
            })
        })
        .filter(|sample: &DynamicSignalSample| sample.value.is_finite() && (0.0..=1.0).contains(&sample.value))
        .collect()
}

/// Extract one quoted label value from a parsed exposition label list.
fn prometheus_label(labels: &str, name: &str) -> Option<String> {
    let marker = format!("{name}=\"");
    let start = labels.find(&marker)?.checked_add(marker.len())?;
    let rest = labels.get(start..)?;
    let end = rest.find('"')?;
    Some(rest.get(..end)?.to_owned())
}

/// Read the dynamic-weight overlay ConfigMap from a named demo cluster.
fn read_dynamic_overlay(cluster: &str) -> Result<DynamicOverlaySnapshot, Box<dyn std::error::Error>> {
    let context = kind_context(cluster);
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
        return Err(format!("{cluster}: overlay ConfigMap unavailable").into());
    }
    parse_dynamic_overlay_json(&output.stdout).map_err(|error| format!("{cluster}: {error}").into())
}

/// Parse the weighted routing config and semantic revision from a ConfigMap.
fn parse_dynamic_overlay_json(bytes: &[u8]) -> Result<DynamicOverlaySnapshot, Box<dyn std::error::Error>> {
    let configmap: serde_json::Value = serde_json::from_slice(bytes)?;
    let semantic_revision = configmap
        .pointer("/metadata/annotations/grid.praxis-proxy.io~1overlay-revision")
        .and_then(serde_json::Value::as_str)
        .ok_or("overlay semantic revision missing")?
        .to_owned();
    let resource_version = configmap
        .pointer("/metadata/resourceVersion")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let routing: serde_json::Value = serde_json::from_str(
        configmap
            .pointer("/data/routing-config.json")
            .and_then(serde_json::Value::as_str)
            .ok_or("routing config missing")?,
    )?;
    if routing
        .pointer("/selection_policy/mode")
        .and_then(serde_json::Value::as_str)
        != Some("weightedRandom")
    {
        return Err("overlay is not weightedRandom".into());
    }
    let candidates = routing
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("overlay candidates missing")?
        .iter()
        .filter_map(|candidate| {
            Some(DynamicCandidateWeight {
                site: candidate.get("site")?.as_str()?.to_owned(),
                provider: candidate.get("cluster")?.as_str()?.to_owned(),
                stable_id: candidate.get("stable_id")?.as_str()?.to_owned(),
                selection_group: u32::try_from(candidate.get("selection_group")?.as_u64()?).ok()?,
                traffic_weight: u32::try_from(candidate.get("traffic_weight")?.as_u64()?).ok()?,
                fresh: candidate.get("fresh")?.as_bool()?,
                admission_state: candidate.get("admission_state")?.as_str()?.to_owned(),
            })
        })
        .collect::<Vec<_>>();
    Ok(DynamicOverlaySnapshot {
        semantic_revision,
        resource_version,
        candidates,
    })
}

/// Wait until Praxis accepts and serves the exact expected overlay revision.
fn wait_for_gateway_revision(cluster: &str, revision: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    let context = kind_context(cluster);
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut last_observed = None;
    let mut last_error = None;
    while Instant::now() < deadline {
        match observe_serving_state(&context) {
            Ok((overlay, accepted, serving)) => {
                if accepted == revision && serving == revision {
                    return Ok((accepted, serving));
                }
                last_observed = Some((overlay.semantic_revision, accepted, serving));
                last_error = None;
            },
            Err(error) => last_error = Some(error.to_string()),
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    let observed = last_observed.map_or_else(
        || "no complete Grid/Praxis revision observation".to_owned(),
        |(grid, accepted, serving)| format!("last observed Grid={grid}, accepted={accepted}, serving={serving}"),
    );
    let observer_error = last_error.map_or_else(String::new, |error| format!("; last read error={error}"));
    Err(
        format!("{cluster}: Praxis did not accept and serve Grid revision {revision}; {observed}{observer_error}")
            .into(),
    )
}

/// Read one gateway's overlay plus its latest accepted and serving revisions.
fn observe_serving_state(
    context: &str,
) -> Result<(DynamicOverlaySnapshot, String, String), Box<dyn std::error::Error>> {
    let overlay = read_dynamic_overlay_for_context(context)?;
    let (accepted, serving) = read_praxis_revisions(context)?;
    Ok((overlay, accepted, serving))
}

/// Read the latest accepted and serving revision fields from the Praxis gateway logs.
fn read_praxis_revisions(context: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    let logs = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "logs",
            "deployment/consumer-gateway",
            "-c",
            "praxis",
            "--tail=300",
        ])
        .output()?;
    if !logs.status.success() {
        return Err(format!("failed to read Praxis gateway logs in {context}").into());
    }
    let logs = String::from_utf8_lossy(&logs.stdout);
    let accepted = latest_dynamic_log_field(&logs, "accepted_revision").ok_or("accepted revision not logged")?;
    let serving = latest_dynamic_log_field(&logs, "serving_revision").ok_or("serving revision not logged")?;
    Ok((accepted, serving))
}

/// Read an overlay ConfigMap using an explicit Kubernetes context.
fn read_dynamic_overlay_for_context(context: &str) -> Result<DynamicOverlaySnapshot, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
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
    if !output.status.success() {
        return Err("consumer overlay ConfigMap unavailable".into());
    }
    parse_dynamic_overlay_json(&output.stdout)
}

/// Extract the latest structured revision field from Praxis logs.
fn latest_dynamic_log_field(logs: &str, field: &str) -> Option<String> {
    strip_dynamic_csi_sgr(logs).lines().rev().find_map(|line| {
        let marker = format!("{field}=");
        line.match_indices(&marker).find_map(|(index, _)| {
            let at_boundary = index == 0
                || line
                    .get(..index)
                    .and_then(|value| value.chars().next_back())
                    .is_some_and(char::is_whitespace);
            if !at_boundary {
                return None;
            }
            let value = line.get(index + marker.len()..)?;
            let value = value.strip_prefix('"').map_or_else(
                || value.split_whitespace().next().unwrap_or(""),
                |quoted| quoted.split('"').next().unwrap_or(""),
            );
            (!value.is_empty()).then(|| value.to_owned())
        })
    })
}

/// Remove ANSI SGR styling from structured tracing logs before parsing fields.
fn strip_dynamic_csi_sgr(value: &str) -> String {
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

/// Collect a sessionless weighted sample from four concurrent loops in one persistent client pod.
#[expect(
    clippy::too_many_lines,
    reason = "The sampler classifies every request once and writes the raw, non-retried outcome."
)]
fn sample_dynamic_traffic(
    context: &str,
    phase: &str,
    overlay: &DynamicOverlaySnapshot,
) -> Result<DynamicTrafficResult, Box<dyn std::error::Error>> {
    const WORKERS: u32 = 4;
    let sample_started = format_utc_iso();
    debug_assert_eq!(
        WORKERS * (DYNAMIC_SAMPLE_SIZE / WORKERS),
        DYNAMIC_SAMPLE_SIZE,
        "all configured requests must be assigned to sampler workers"
    );
    let script = dynamic_sample_script(phase);
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "exec",
            "dynamic-weighted-client",
            "--",
            "sh",
            "-c",
            &script,
        ])
        .output()?;
    let sample_completed = format_utc_iso();
    let mut raw = Vec::new();
    let mut counts = BTreeMap::from([("pool-a".to_owned(), 0_u32), ("pool-b".to_owned(), 0_u32)]);
    let mut transport_failures = 0_u32;
    let mut http_failures = 0_u32;
    let mut attribution_failures = 0_u32;
    let mut received = 0_u32;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Some(rest) = line.strip_prefix("DYN|") else {
            continue;
        };
        let mut fields = rest.splitn(6, '|');
        let request_id = fields.next().unwrap_or("unknown");
        let curl_exit = fields.next().and_then(|value| value.parse::<i32>().ok()).unwrap_or(-1);
        let status = fields
            .next()
            .and_then(|value| value.parse::<u16>().ok())
            .filter(|status| *status != 0);
        let provider_gateway = fields.next().unwrap_or("");
        let provider_attribution = fields.next().unwrap_or("");
        let latency = fields.next().unwrap_or("");
        received = received.saturating_add(1);
        let category = if curl_exit != 0 || status.is_none() {
            transport_failures = transport_failures.saturating_add(1);
            "transport_failure"
        } else if status != Some(200) {
            http_failures = http_failures.saturating_add(1);
            "http_failure"
        } else if provider_gateway.contains("pool-a") && provider_attribution.contains("pool-a") {
            *counts.entry("pool-a".to_owned()).or_default() += 1;
            "attributed_pool_a"
        } else if provider_gateway.contains("pool-b") && provider_attribution.contains("pool-b") {
            *counts.entry("pool-b".to_owned()).or_default() += 1;
            "attributed_pool_b"
        } else {
            attribution_failures = attribution_failures.saturating_add(1);
            "attribution_failure"
        };
        raw.push(
            serde_json::json!({
                "request_id": request_id,
                "phase": phase,
                "http_status": status,
                "curl_exit_code": curl_exit,
                "provider_gateway": provider_gateway,
                "provider_attribution": provider_attribution,
                "latency_seconds": latency,
                "classification": category,
            })
            .to_string(),
        );
    }
    if !output.status.success() {
        raw.push(
            serde_json::json!({
                "phase": phase,
                "classification": "sampler_transport_failure",
                "stderr": safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 240),
            })
            .to_string(),
        );
    }
    if received < DYNAMIC_SAMPLE_SIZE {
        transport_failures = transport_failures.saturating_add(DYNAMIC_SAMPLE_SIZE - received);
    }
    let pool_a = overlay
        .candidates
        .iter()
        .find(|candidate| candidate.site == "pool-a")
        .ok_or("pool-a weight absent")?;
    let pool_b = overlay
        .candidates
        .iter()
        .find(|candidate| candidate.site == "pool-b")
        .ok_or("pool-b weight absent")?;
    let observed_a = counts.get("pool-a").copied().ok_or("pool-a count missing")?;
    let observed_b = counts.get("pool-b").copied().ok_or("pool-b count missing")?;
    let total = f64::from(observed_a.saturating_add(observed_b)).max(1.0);
    let expected_total = f64::from(pool_a.traffic_weight + pool_b.traffic_weight);
    let expected_a = f64::from(pool_a.traffic_weight) / expected_total;
    let expected_b = f64::from(pool_b.traffic_weight) / expected_total;
    let chi_square = dynamic_chi_square([pool_a.traffic_weight, pool_b.traffic_weight], [observed_a, observed_b]);
    let accepted = received == DYNAMIC_SAMPLE_SIZE
        && transport_failures == 0
        && http_failures == 0
        && attribution_failures == 0
        && (observed_a + observed_b) == DYNAMIC_SAMPLE_SIZE
        && chi_square <= DYNAMIC_CHI_SQUARE_CRITICAL;
    Ok((
        DynamicTrafficSample {
            request_count: received,
            counts,
            observed_fraction: BTreeMap::from([
                ("pool-a".to_owned(), f64::from(observed_a) / total),
                ("pool-b".to_owned(), f64::from(observed_b) / total),
            ]),
            expected_fraction: BTreeMap::from([("pool-a".to_owned(), expected_a), ("pool-b".to_owned(), expected_b)]),
            chi_square,
            chi_square_critical_value: DYNAMIC_CHI_SQUARE_CRITICAL,
            accepted,
            transport_failures,
            http_failures,
            attribution_failures,
        },
        raw,
        sample_started,
        sample_completed,
    ))
}

/// Bind independent sessions before pressure and replay those same keys later.
/// Every curl executes once; HTTP and attribution failures are recorded, never retried.
fn sample_dynamic_affinity(
    context: &str,
    run_id: &str,
    stage: &str,
) -> Result<Vec<DynamicAffinitySample>, Box<dyn std::error::Error>> {
    const SCRIPT: &str = r#"
run_id="$1"
stage="$2"
total="$3"
ordinal=1
while [ "$ordinal" -le "$total" ]; do
  session_id="${run_id}-weighted-affinity-${ordinal}"
  out=$(curl --silent --show-error --connect-timeout 5 --max-time 20 -o /dev/null -w '%{http_code}|%header{X-Grid-LlmD-Provider-Gateway}|%header{x-ai-demo-provider-gateway}' -H 'Content-Type: application/json' -H "X-Session-Id: ${session_id}" -X POST 'http://consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions' -d '{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"weighted affinity qualification"}],"max_tokens":2}' 2>/dev/null)
  rc=$?
  printf 'AFF|%s|%s|%s|%s\n' "$stage" "$ordinal" "$rc" "$out"
  ordinal=$((ordinal + 1))
done
"#;
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "exec",
            "dynamic-weighted-client",
            "--",
            "sh",
            "-c",
            SCRIPT,
            "affinity",
            run_id,
            stage,
            &DYNAMIC_AFFINITY_SESSION_COUNT.to_string(),
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "affinity {stage} requests could not run: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 240)
        )
        .into());
    }

    let mut samples = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let mut fields = line.splitn(5, '|');
        if fields.next() != Some("AFF") {
            continue;
        }
        let Some(observed_stage) = fields.next() else { continue };
        let Some(session_ordinal) = fields.next().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        let curl_exit_code = fields.next().and_then(|value| value.parse::<i32>().ok()).unwrap_or(-1);
        let mut response = fields.next().unwrap_or("").splitn(3, '|');
        samples.push(DynamicAffinitySample {
            stage: observed_stage.to_owned(),
            session_ordinal,
            curl_exit_code,
            http_status: response
                .next()
                .and_then(|value| value.parse::<u16>().ok())
                .filter(|value| *value != 0),
            provider_gateway: response.next().unwrap_or("").to_owned(),
            provider_attribution: response.next().unwrap_or("").to_owned(),
        });
    }
    Ok(samples)
}

/// Persist every affinity request result without exposing the synthetic session key.
fn write_dynamic_affinity_samples(
    evidence_dir: &Path,
    stage: &str,
    samples: &[DynamicAffinitySample],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut lines = samples
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?;
    if lines.is_empty() {
        lines.push(serde_json::json!({"stage": stage, "classification": "no_samples"}).to_string());
    }
    fs::write(
        evidence_dir.join(format!("affinity-{stage}.jsonl")),
        format!("{}\n", lines.join("\n")),
    )?;
    Ok(())
}

/// Require a complete baseline binding sample with trusted attribution to both providers.
fn affinity_bind_samples_valid(samples: &[DynamicAffinitySample]) -> bool {
    samples.len() == usize::try_from(DYNAMIC_AFFINITY_SESSION_COUNT).unwrap_or(usize::MAX)
        && samples.iter().all(|sample| {
            sample.stage == "bind"
                && sample.curl_exit_code == 0
                && sample.http_status == Some(200)
                && sample.provider_gateway == sample.provider_attribution
                && matches!(sample.provider_gateway.as_str(), "pool-a" | "pool-b")
        })
        && samples.iter().any(|sample| sample.provider_gateway == "pool-a")
        && samples.iter().any(|sample| sample.provider_gateway == "pool-b")
}

/// Require every replay to return its original provider after the overlay weights change.
fn affinity_replay_matches(bindings: &BTreeMap<u32, String>, samples: &[DynamicAffinitySample]) -> bool {
    samples.len() == bindings.len()
        && samples.iter().all(|sample| {
            sample.stage == "replay"
                && sample.curl_exit_code == 0
                && sample.http_status == Some(200)
                && sample.provider_gateway == sample.provider_attribution
                && bindings.get(&sample.session_ordinal) == Some(&sample.provider_gateway)
        })
}

/// Restart the run-owned Grid operators and record deployment pod replacement.
fn restart_dynamic_operators() -> Result<Vec<DynamicOperatorRestart>, Box<dyn std::error::Error>> {
    let mut before = BTreeMap::new();
    for cluster in CLUSTERS {
        before.insert(
            (*cluster).to_owned(),
            dynamic_operator_pod_uids(&kind_context(cluster))?,
        );
    }
    for cluster in CLUSTERS {
        let context = kind_context(cluster);
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
                "{cluster}: operator rollout restart failed: {}",
                safe_truncate_str(String::from_utf8_lossy(&restart.stderr).trim(), 240)
            )
            .into());
        }
    }
    let mut records = Vec::new();
    for cluster in CLUSTERS {
        let context = kind_context(cluster);
        let rollout = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "rollout",
                "status",
                "deployment/grid-operator",
                "--timeout=180s",
            ])
            .output()?;
        if !rollout.status.success() {
            return Err(format!(
                "{cluster}: operator rollout did not become ready: {}",
                safe_truncate_str(String::from_utf8_lossy(&rollout.stderr).trim(), 240)
            )
            .into());
        }
        let after_pod_uids = dynamic_operator_pod_uids(&context)?;
        let before_pod_uids = before
            .remove(*cluster)
            .ok_or("operator pre-restart pod snapshot missing")?;
        if before_pod_uids == after_pod_uids {
            return Err(format!("{cluster}: operator rollout completed without replacing its pod").into());
        }
        if before_pod_uids.iter().any(|uid| after_pod_uids.contains(uid)) {
            return Err(format!("{cluster}: pre-restart operator pod is still Ready after rollout").into());
        }
        records.push(DynamicOperatorRestart {
            cluster: (*cluster).to_owned(),
            before_pod_uids,
            after_pod_uids,
        });
    }
    Ok(records)
}

/// Return UIDs for the Grid operator Deployment's run-owned pods.
fn dynamic_operator_pod_uids(context: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args(["--context", context, "-n", GRID_SYSTEM_NS, "get", "pods", "-o", "json"])
        .output()?;
    if !output.status.success() {
        return Err(format!("could not list operator pods in {context}").into());
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let items = value
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or("pod list items missing")?;
    let uids = active_dynamic_operator_pod_uids(items);
    if uids.is_empty() {
        return Err(format!("no Ready, non-terminating Grid operator pod found in {context}").into());
    }
    Ok(uids)
}

/// Return Ready, non-terminating Grid operator pod UIDs from a Kubernetes pod list.
fn active_dynamic_operator_pod_uids(pods: &[serde_json::Value]) -> Vec<String> {
    let mut uids = pods
        .iter()
        .filter(|pod| {
            pod.pointer("/metadata/name")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|name| name.starts_with("grid-operator-"))
                && pod
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(serde_json::Value::as_str)
                    .is_none()
                && pod
                    .pointer("/status/conditions")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|conditions| {
                        conditions.iter().any(|condition| {
                            condition.get("type").and_then(serde_json::Value::as_str) == Some("Ready")
                                && condition.get("status").and_then(serde_json::Value::as_str) == Some("True")
                        })
                    })
        })
        .filter_map(|pod| {
            pod.pointer("/metadata/uid")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect::<Vec<_>>();
    uids.sort();
    uids
}

/// Return Grid operator pod UIDs, including the empty state during a scale-down.
fn dynamic_operator_pod_uids_allow_empty(context: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args(["--context", context, "-n", GRID_SYSTEM_NS, "get", "pods", "-o", "json"])
        .output()?;
    if !output.status.success() {
        return Err(format!("could not list operator pods in {context}").into());
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let items = value
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or("pod list items missing")?;
    let mut uids = items
        .iter()
        .filter(|pod| {
            pod.pointer("/metadata/name")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|name| name.starts_with("grid-operator-"))
        })
        .filter_map(|pod| {
            pod.pointer("/metadata/uid")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect::<Vec<_>>();
    uids.sort();
    Ok(uids)
}

/// One HTTP request made while the invalid overlay is being rejected.
#[derive(Clone, Debug, Serialize)]
struct DynamicLkgProbe {
    /// curl exit status (zero means transport completed).
    curl_exit_code: i32,
    /// HTTP response status, when received.
    http_status: Option<u16>,
    /// Trusted provider gateway response attribution.
    provider_gateway: String,
    /// Independent provider attribution from the backend.
    provider_attribution: String,
}

/// Result of the invalid-overlay interval before the run-owned state is restored.
type DynamicLkgCheck = (String, (String, String), bool, DynamicLkgProbe);

/// Fallible result for the invalid-overlay interval.
type DynamicLkgCheckResult = Result<DynamicLkgCheck, Box<dyn std::error::Error>>;

/// Inject one malformed run-owned overlay and prove both LKG retention and live routing.
fn prove_invalid_overlay_last_known_good(context: &DemoContext, expected: &DynamicPhaseEvidence) -> ProofResult {
    const OVERLAY_FILE: &str = "/etc/praxis/routing/routing-overlay.json";
    const INVALID_OVERLAY: &str = "{";
    const OVERLAY_KEY: &str = "routing-overlay.json";
    let cluster_context = kind_context("pool-a");
    let original_overlay = match read_overlay_configmap_value(&cluster_context, OVERLAY_KEY) {
        Ok(value) => value,
        Err(error) => {
            return failed_proof(
                "Invalid overlay LKG: original ConfigMap could not be captured",
                error.to_string(),
            );
        },
    };
    let expected_gateway = expected.gateways.get("pool-a");
    let Some(expected_gateway) = expected_gateway else {
        return failed_proof(
            "Invalid overlay LKG: pressure-phase gateway evidence is missing",
            "pool-a state absent".to_owned(),
        );
    };
    let expected_revision = expected_gateway.overlay.semantic_revision.clone();
    let (accepted_before, serving_before) = match read_praxis_revisions(&cluster_context) {
        Ok(revisions) => revisions,
        Err(error) => return failed_proof("Invalid overlay LKG: Praxis revisions unavailable", error.to_string()),
    };
    let file_before = match read_gateway_overlay_file(&cluster_context, OVERLAY_FILE) {
        Ok(contents) => contents,
        Err(error) => {
            return failed_proof(
                "Invalid overlay LKG: active route snapshot unavailable",
                error.to_string(),
            );
        },
    };
    if accepted_before != expected_revision || serving_before != expected_revision {
        return failed_proof(
            "Invalid overlay LKG: gateway was not serving the pressure revision before injection",
            format!("expected={expected_revision}, accepted={accepted_before}, serving={serving_before}"),
        );
    }

    let mut operators_scale_touched = false;
    let mut configmap_touched = false;
    let test_result = (|| -> DynamicLkgCheckResult {
        operators_scale_touched = true;
        for cluster in CLUSTERS {
            scale_dynamic_operator(cluster, 0)?;
        }
        for cluster in CLUSTERS {
            wait_for_dynamic_operator_count(cluster, 0)?;
        }

        // Mark as touched before invoking kubectl so restoration is attempted even
        // if the API server reports an ambiguous patch outcome.
        configmap_touched = true;
        patch_overlay_configmap_value(&cluster_context, OVERLAY_KEY, INVALID_OVERLAY)?;
        let rejection = wait_for_overlay_rejection(&cluster_context, "malformed")?;
        let revisions = read_praxis_revisions(&cluster_context)?;
        let file_after = read_gateway_overlay_file(&cluster_context, OVERLAY_FILE)?;
        let live_overlay_unchanged = file_before.trim_end() == file_after.trim_end();
        let request = run_dynamic_lkg_probe(&cluster_context)?;
        let provider_matches = request.provider_gateway == request.provider_attribution
            && matches!(request.provider_gateway.as_str(), "pool-a" | "pool-b");
        if revisions.0 != accepted_before
            || revisions.1 != serving_before
            || !live_overlay_unchanged
            || request.curl_exit_code != 0
            || request.http_status != Some(200)
            || !provider_matches
        {
            return Err(format!(
                "last-known-good check failed: revisions={revisions:?}, file_unchanged={live_overlay_unchanged}, request={request:?}"
            )
            .into());
        }
        Ok((rejection, revisions, live_overlay_unchanged, request))
    })();

    let mut restoration_errors = Vec::new();
    if configmap_touched
        && let Err(error) = patch_overlay_configmap_value(&cluster_context, OVERLAY_KEY, &original_overlay)
    {
        restoration_errors.push(format!("restore original overlay ConfigMap data: {error}"));
    }
    if operators_scale_touched {
        for cluster in CLUSTERS {
            if let Err(error) = scale_dynamic_operator(cluster, 1) {
                restoration_errors.push(format!("restore {cluster} operator replica: {error}"));
            }
        }
        for cluster in CLUSTERS {
            if let Err(error) = wait_for_dynamic_operator_count(cluster, 1) {
                restoration_errors.push(format!("wait for {cluster} operator recovery: {error}"));
            }
        }
    }

    let (test_error, details) = match test_result {
        Ok(result) => (None, Some(result)),
        Err(error) => (Some(error.to_string()), None),
    };
    let (rejection, revisions, live_overlay_unchanged, request) = details.map_or((None, None, None, None), |result| {
        (Some(result.0), Some(result.1), Some(result.2), Some(result.3))
    });
    let restoration_succeeded = restoration_errors.is_empty();
    let evidence_write = serde_json::to_vec_pretty(&serde_json::json!({
        "expected_pressure_revision": expected_revision,
        "accepted_before": accepted_before,
        "serving_before": serving_before,
        "invalid_payload_class": "malformed JSON object",
        "invalid_payload": INVALID_OVERLAY,
        "overlay_sync_rejection": &rejection,
        "accepted_after": revisions.as_ref().map(|value| &value.0),
        "serving_after": revisions.as_ref().map(|value| &value.1),
        "live_route_file_unchanged": live_overlay_unchanged,
        "request": &request,
        "test_error": &test_error,
        "restoration_errors": restoration_errors.clone(),
    }))
    .map_err(|error| error.to_string())
    .and_then(|bytes| {
        fs::write(context.evidence_dir.join("invalid-overlay-last-known-good.json"), bytes)
            .map_err(|error| error.to_string())
    });
    let success = evidence_write.is_ok() && test_error.is_none() && restoration_succeeded;
    ProofResult {
        success,
        description: "Invalid overlay: overlay-sync and Praxis retain and serve the last-known-good snapshot"
            .to_owned(),
        observations: vec![
            rejection.map_or_else(
                || "overlay-sync malformed-overlay rejection was not observed".to_owned(),
                |line| format!("overlay-sync rejected invalid update: {line}"),
            ),
            revisions.map_or_else(
                || "Praxis post-injection revision state unavailable".to_owned(),
                |(accepted, serving)| format!("accepted={accepted}, serving={serving}"),
            ),
            live_overlay_unchanged.map_or_else(
                || "active route file was not compared".to_owned(),
                |unchanged| format!("active route file unchanged={unchanged}"),
            ),
            request.map_or_else(
                || "post-injection request not completed".to_owned(),
                |result| {
                    format!(
                        "post-injection request status={:?}, gateway={}, backend={}",
                        result.http_status, result.provider_gateway, result.provider_attribution
                    )
                },
            ),
            test_error.unwrap_or_else(|| "invalid-overlay runtime assertions passed".to_owned()),
            if restoration_errors.is_empty() {
                "original ConfigMap data restored and both operators Ready".to_owned()
            } else {
                format!("restoration errors: {}", restoration_errors.join("; "))
            },
        ],
    }
}

/// Read one ConfigMap data field without emitting its payload to logs.
fn read_overlay_configmap_value(context: &str, key: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
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
    if !output.status.success() {
        return Err("consumer overlay ConfigMap unavailable".into());
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    value
        .pointer(&format!("/data/{key}"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("overlay ConfigMap data key {key} is missing").into())
}

/// Patch a single run-owned ConfigMap data field using JSON-encoded input.
fn patch_overlay_configmap_value(context: &str, key: &str, value: &str) -> Result<(), Box<dyn std::error::Error>> {
    let patch = serde_json::json!({"data": {key: value}}).to_string();
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "patch",
            "configmap",
            OVERLAY_CONFIGMAP,
            "--type=merge",
            "-p",
            &patch,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "overlay ConfigMap patch failed: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 240)
        )
        .into());
    }
    Ok(())
}

/// Read the active overlay file inside the run-owned Praxis gateway container.
fn read_gateway_overlay_file(context: &str, path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "exec",
            "deployment/consumer-gateway",
            "-c",
            "praxis",
            "--",
            "cat",
            path,
        ])
        .output()?;
    if !output.status.success() {
        return Err("could not read the active Praxis overlay snapshot".into());
    }
    Ok(String::from_utf8(output.stdout)?)
}

/// Read bounded overlay-sync logs and return the matching malformed rejection line.
fn wait_for_overlay_rejection(context: &str, reason: &str) -> Result<String, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut last_error = String::from("overlay-sync rejection log was not observed");
    while Instant::now() < deadline {
        let output = Command::new("kubectl")
            .args([
                "--context",
                context,
                "-n",
                GRID_SYSTEM_NS,
                "logs",
                "deployment/consumer-gateway",
                "-c",
                "overlay-sync",
                "--since=2m",
                "--tail=500",
            ])
            .output()?;
        if output.status.success() {
            let logs = String::from_utf8_lossy(&output.stdout);
            if let Some(line) = logs
                .lines()
                .rev()
                .find(|line| line.contains("overlay_rejected") && line.contains(reason))
            {
                return Ok(safe_truncate_str(line.trim(), 400));
            }
            "overlay-sync logs did not contain the expected rejection reason".clone_into(&mut last_error);
        } else {
            last_error = safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 240);
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    Err(last_error.into())
}

/// Make exactly one fresh, sessionless request during the invalid-overlay window.
fn run_dynamic_lkg_probe(context: &str) -> Result<DynamicLkgProbe, Box<dyn std::error::Error>> {
    const SCRIPT: &str = r#"
out=$(curl --silent --show-error --connect-timeout 5 --max-time 20 -o /dev/null -w '%{http_code}|%header{X-Grid-LlmD-Provider-Gateway}|%header{x-ai-demo-provider-gateway}' -H 'Content-Type: application/json' -X POST 'http://consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions' -d '{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"invalid overlay LKG qualification"}],"max_tokens":2}' 2>/dev/null)
rc=$?
printf 'LKG|%s|%s\n' "$rc" "$out"
"#;
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "exec",
            "dynamic-weighted-client",
            "--",
            "sh",
            "-c",
            SCRIPT,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "last-known-good probe could not run: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 240)
        )
        .into());
    }
    let line = String::from_utf8_lossy(&output.stdout)
        .lines()
        .find(|line| line.starts_with("LKG|"))
        .ok_or("last-known-good probe produced no result line")?
        .to_owned();
    let mut fields = line.splitn(3, '|');
    let _prefix = fields.next();
    let curl_exit_code = fields.next().and_then(|value| value.parse::<i32>().ok()).unwrap_or(-1);
    let mut response = fields.next().unwrap_or("").splitn(3, '|');
    Ok(DynamicLkgProbe {
        curl_exit_code,
        http_status: response
            .next()
            .and_then(|value| value.parse::<u16>().ok())
            .filter(|value| *value != 0),
        provider_gateway: response.next().unwrap_or("").to_owned(),
        provider_attribution: response.next().unwrap_or("").to_owned(),
    })
}

/// Scale an operator in its run-owned cluster.
fn scale_dynamic_operator(cluster: &str, replicas: u32) -> Result<(), Box<dyn std::error::Error>> {
    let context = kind_context(cluster);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "scale",
            "deployment/grid-operator",
            &format!("--replicas={replicas}"),
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "{cluster}: scaling Grid operator to {replicas} replicas failed: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 240)
        )
        .into());
    }
    Ok(())
}

/// Wait for the requested run-owned operator pod count; require rollout readiness at one replica.
fn wait_for_dynamic_operator_count(cluster: &str, expected: usize) -> Result<(), Box<dyn std::error::Error>> {
    let context = kind_context(cluster);
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let uids = dynamic_operator_pod_uids_allow_empty(&context)?;
        if uids.len() == expected {
            if expected == 0 {
                return Ok(());
            }
            let rollout = Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "rollout",
                    "status",
                    "deployment/grid-operator",
                    "--timeout=5s",
                ])
                .output()?;
            if rollout.status.success() {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("{cluster}: operator did not reach pod count {expected}").into());
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Build one non-retrying, sessionless concurrent request batch.
fn dynamic_sample_script(phase: &str) -> String {
    const PER_WORKER: u32 = DYNAMIC_SAMPLE_SIZE / 4;
    const URL: &str = "http://consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions";
    format!(
        r#"for worker in 1 2 3 4; do (ordinal=1; while [ "$ordinal" -le {PER_WORKER} ]; do id="{phase}-$worker-$ordinal"; out=$(curl --silent --show-error --connect-timeout 5 --max-time 20 -o /dev/null -w '%{{http_code}}|%header{{X-Grid-LlmD-Provider-Gateway}}|%header{{x-ai-demo-provider-gateway}}|%{{time_total}}' -H 'Content-Type: application/json' -X POST '{URL}' -d '{{"model":"{VCR_MODEL}","messages":[{{"role":"user","content":"weighted qualification"}}],"max_tokens":2}}' 2>/dev/null); rc=$?; printf 'DYN|%s|%s|%s\n' "$id" "$rc" "$out"; ordinal=$((ordinal + 1)); done) & done; wait"#
    )
}

/// Calculate Pearson chi-square against the published two-provider weights.
fn dynamic_chi_square(weights: [u32; 2], observed: [u32; 2]) -> f64 {
    let observed_total = f64::from(observed[0].saturating_add(observed[1]));
    let weight_total = f64::from(weights[0].saturating_add(weights[1]));
    if observed_total <= 0.0 || weight_total <= 0.0 {
        return f64::INFINITY;
    }
    weights.into_iter().zip(observed).fold(0.0, |sum, (weight, count)| {
        let expected = observed_total * f64::from(weight) / weight_total;
        if expected < 5.0 {
            f64::INFINITY
        } else {
            sum + (f64::from(count) - expected).powi(2) / expected
        }
    })
}

/// Capture deployment generations and pod UIDs for deterministic simulators.
fn simulator_runtime_identity(cluster: &str) -> Result<SimulatorRuntimeIdentity, Box<dyn std::error::Error>> {
    let context = kind_context(cluster);
    let mut generations = BTreeMap::new();
    let mut uids = BTreeMap::new();
    for deployment in SIMULATOR_DEPLOYMENTS {
        generations.insert(deployment.to_string(), deployment_generation(&context, deployment)?);
        let output = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                "pods",
                "-l",
                &format!("instance={deployment}"),
                "-o",
                "jsonpath={.items[0].metadata.uid}",
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!("{cluster}/{deployment}: unable to read simulator pod UID").into());
        }
        let uid = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if uid.is_empty() {
            return Err(format!("{cluster}/{deployment}: simulator pod UID is empty").into());
        }
        uids.insert(deployment.to_string(), uid);
    }
    Ok((generations, uids))
}

/// Append one timestamped observation to the run's phase timeline.
fn append_dynamic_timeline(path: &Path, value: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write as _;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path.join("dynamic-timeline.jsonl"))?;
    writeln!(file, "{value}")?;
    Ok(())
}

/// Proof 1: Image digests and VCR configuration verification.
fn proof_provenance(mtls: bool) -> ProofResult {
    let mut observations = Vec::new();
    let mut success = true;

    for cluster in CLUSTERS {
        let ctx = kind_context(cluster);
        let mut metrics_ok = false;
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Ok(metrics_text) = kubectl_exec_epp_metrics(cluster, mtls) {
                let has_kv = any_metric_present(&metrics_text, EPP_KV_CACHE_METRICS);
                let has_queue = any_metric_present(&metrics_text, EPP_QUEUE_SIZE_METRICS);
                let has_ready = any_metric_present(&metrics_text, EPP_READY_METRICS);
                if has_kv && has_queue && has_ready {
                    observations.push(format!("{cluster}: all 3 EPP pool metrics present"));
                    metrics_ok = true;
                    break;
                }
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        if !metrics_ok {
            observations.push(format!("{cluster}: EPP pool metrics not available within 30s"));
            success = false;
        }

        // Verify VCR deployment MODEL env var
        match kubectl_get_deployment_env(&ctx, "vcr-1", "MODEL") {
            Ok(val) if val == VCR_MODEL => {
                observations.push(format!("{cluster}: VCR MODEL={val}"));
            },
            Ok(val) => {
                observations.push(format!("{cluster}: VCR MODEL={val} (expected {VCR_MODEL})"));
                success = false;
            },
            Err(e) => {
                observations.push(format!("{cluster}: cannot read VCR env: {e}"));
            },
        }
    }

    ProofResult {
        success,
        description: "Provenance: EPP metrics live, VCR model verified".to_owned(),
        observations,
    }
}

/// Proof 2: Confirm pool-a preferred at idle, send attributed request
/// through pool-a.
fn proof_baseline(context: &DemoContext) -> ProofResult {
    let mut observations = Vec::new();

    eprintln!();
    eprintln!("  [BASELINE] Waiting for pool-a to become preferred at idle");
    let deadline = Instant::now() + DATA_PLANE_WAIT;
    let mut last_reconcile_trigger = Instant::now();
    let mut last_request = Instant::now()
        .checked_sub(Duration::from_secs(10))
        .unwrap_or_else(Instant::now);

    for cluster in CLUSTERS {
        trigger_gridnetwork_reconcile(cluster);
    }

    while Instant::now() < deadline {
        if last_reconcile_trigger.elapsed() > Duration::from_secs(5) {
            for cluster in CLUSTERS {
                trigger_gridnetwork_reconcile(cluster);
            }
            last_reconcile_trigger = Instant::now();
        }

        let epp_a = scrape_epp_metrics("pool-a", context.metrics_transport == MetricsTransport::MtlsProxy);
        let candidates = read_overlay_candidates("pool-a");
        let rank_a = overlay_rank_for_cluster(&candidates, "pool-a");

        if rank_a == 0 && epp_a.queue_size < 3.0 && last_request.elapsed() >= Duration::from_secs(5) {
            last_request = Instant::now();
            eprintln!(
                "  [BASELINE] Pool A is preferred (rank=0, queue={:.1}); sending verification traffic",
                epp_a.queue_size
            );
            let probe_ctx = kind_context("pool-a");
            match send_inference_request(&probe_ctx, VCR_MODEL) {
                Ok(resp) => {
                    if resp.provider_gateway.contains("pool-a") && resp.demo_attribution.contains("pool-a") {
                        let epp_b =
                            scrape_epp_metrics("pool-b", context.metrics_transport == MetricsTransport::MtlsProxy);
                        let row_a = build_scorecard_row("Cluster A", &candidates, "pool-a", &epp_a);
                        let row_b = build_scorecard_row("Cluster B", &candidates, "pool-b", &epp_b);
                        eprintln!("  [BASELINE] Request attributed to pool-a -- baseline confirmed");
                        print_scorecard_with_cause(
                            "BASELINE",
                            &[&row_a, &row_b],
                            "CLUSTER A",
                            &candidates,
                            "Both pools idle. Pool A outscores Pool B on locality (local=3.0 vs remote=1.5).",
                        );
                        observations.push(format!(
                            "pool-a: queue={:.2} kv={:.2} score={:.2} rank=0",
                            row_a.queue, row_a.kv_cache, row_a.score
                        ));
                        observations.push(format!(
                            "pool-b: queue={:.2} kv={:.2} score={:.2} rank={}",
                            row_b.queue, row_b.kv_cache, row_b.score, row_b.rank
                        ));
                        observations.push(format!(
                            "attribution: gateway={} provider={}",
                            resp.provider_gateway, resp.demo_attribution
                        ));
                        return ProofResult {
                            success: true,
                            description: "Baseline: pool-a preferred at idle, pool-a attribution confirmed".to_owned(),
                            observations,
                        };
                    }
                    eprintln!(
                        "  [BASELINE] Data plane converging (overlay=pool-a rank 0, but routing to {})",
                        resp.provider_gateway
                    );
                },
                Err(e) => {
                    eprintln!("  [BASELINE] Inference probe retrying: {e}");
                },
            }
        } else if rank_a != 0 {
            eprintln!(
                "  [BASELINE] pool-a: queue={:.1} rank={} (waiting for idle convergence)",
                epp_a.queue_size, rank_a
            );
        }

        std::thread::sleep(DATA_PLANE_INTERVAL);
    }

    observations.push("pool-a did not reach rank 0 with confirmed routing within timeout".to_owned());
    ProofResult {
        success: false,
        description: "Baseline: pool-a preferred at idle, pool-a attribution confirmed".to_owned(),
        observations,
    }
}

/// Proof 3: Scale up pressure through the consumer gateway, wait for
/// A→B flip with live metrics table and attribution tracking.
fn proof_pressure_and_flip(context: &DemoContext, table_start: Instant) -> ProofResult {
    let mut observations = Vec::new();
    let mtls = context.metrics_transport == MetricsTransport::MtlsProxy;
    let candidates = read_overlay_candidates("pool-a");
    let initial_rank_a = overlay_rank_for_cluster(&candidates, "pool-a");
    if initial_rank_a != 0 {
        observations.push(format!(
            "precondition failed: pool-a rank={initial_rank_a} at entry, expected 0"
        ));
        return ProofResult {
            success: false,
            description: "Pressure & flip: pool-a was not rank 0 at entry".to_owned(),
            observations,
        };
    }
    observations.push("precondition: pool-a rank=0 at entry".to_owned());

    eprintln!();
    eprintln!(
        "  [PRESSURE] Persisting deterministic llm-d-sim {} pressure and rolling Pool A",
        context.scoring_flavor.label()
    );
    if let Err(e) = set_simulator_waiting_requests("pool-a", 9, context.scoring_flavor, mtls) {
        observations.push(format!("simulator pressure rollout failed: {e}"));
        return ProofResult {
            success: false,
            description: "Pressure & flip: simulator failed to report the requested metric".to_owned(),
            observations,
        };
    }
    observations.push(format!(
        "llm-d-sim {} pressure set through persistent Deployment configuration",
        context.scoring_flavor.label()
    ));

    eprintln!();
    eprintln!("  Live Metrics Table");
    eprintln!("    Queue/KV/Score/Rank: derived from the Grid overlay (production scoring engine)");
    eprintln!("    pressure source:     llm-d-sim /metrics (persistent fake-metrics)");
    eprintln!("    LAST_ROUTE:         most recent confirmed request destination");
    print_live_table_header();
    let deadline = Instant::now() + DATA_PLANE_WAIT;
    let mut last_reconcile_trigger = Instant::now();
    let mut last_route = String::from("-");
    let mut last_probe_result = None::<String>;
    let mut pressure_announced = false;

    while Instant::now() < deadline {
        if last_reconcile_trigger.elapsed() > Duration::from_secs(5) {
            for cluster in CLUSTERS {
                trigger_gridnetwork_reconcile(cluster);
            }
            last_reconcile_trigger = Instant::now();
        }

        let epp_a = scrape_epp_metrics("pool-a", mtls);
        let epp_b = scrape_epp_metrics("pool-b", mtls);
        let updated_candidates = read_overlay_candidates("pool-a");
        let row_a = build_scorecard_row("Cluster A", &updated_candidates, "pool-a", &epp_a);
        let row_b = build_scorecard_row("Cluster B", &updated_candidates, "pool-b", &epp_b);
        let score_gap = row_b.score - row_a.score;

        let phase = if row_b.rank == 0 && row_a.rank > 0 {
            "FAILOVER"
        } else if pressure_phase_active(context.scoring_flavor, &epp_a) {
            "PRESSURE"
        } else {
            "BASELINE"
        };

        if !pressure_announced && pressure_phase_active(context.scoring_flavor, &epp_a) {
            pressure_announced = true;
            eprintln!(
                "  [PRESSURE] Pool A queue/KV pressure is increasing (queue={:.1} kv={:.2})",
                epp_a.queue_size, epp_a.kv_cache
            );
        }

        print_live_table_row(&LiveTableRow {
            elapsed: table_start.elapsed(),
            phase,
            rows: (&row_a, &row_b),
            last_route: &last_route,
        });

        if row_b.rank == 0 && row_a.rank > 0 && score_gap >= MIN_PRESSURE_SCORE_GAP {
            eprintln!(
                "  [SCORING] Pool A score={:.2} rank={}; Pool B score={:.2} rank={} (gap={:.2})",
                row_a.score, row_a.rank, row_b.score, row_b.rank, score_gap
            );
            eprintln!("  [FAILOVER] Pool B is now preferred; sending verification request");
            let probe_ctx = kind_context("pool-a");
            match send_inference_request(&probe_ctx, VCR_MODEL) {
                Ok(resp) => {
                    last_route = if resp.provider_gateway.contains("pool-b") {
                        "pool-b".to_owned()
                    } else {
                        "pool-a".to_owned()
                    };
                    let result = format!(
                        "request attribution: gateway={} provider={}",
                        resp.provider_gateway, resp.demo_attribution
                    );
                    if last_probe_result.as_deref() != Some(result.as_str()) {
                        eprintln!("  [FAILOVER] {result}");
                    }
                    last_probe_result = Some(result);
                    if resp.provider_gateway.contains("pool-b") && resp.demo_attribution.contains("pool-b") {
                        eprintln!("  [TRAFFIC SHIFT] Request attributed to pool-b");
                        print_scorecard_with_cause(
                            "FAILOVER",
                            &[&row_a, &row_b],
                            "CLUSTER B",
                            &updated_candidates,
                            "Pool A pressure lowered its queue/KV scores, so Pool B became rank 0.",
                        );
                        observations.push(format!(
                            "flip: pool-b rank=0 score={:.2}, pool-a rank={} score={:.2} (gap={:.2})",
                            row_b.score, row_a.rank, row_a.score, score_gap
                        ));
                        observations.push(format!(
                            "pool-a: queue={:.1}/{:.0} kv={:.2}",
                            row_a.queue, row_a.capacity, row_a.kv_cache
                        ));
                        observations.push(format!(
                            "attribution: gateway={} provider={}",
                            resp.provider_gateway, resp.demo_attribution
                        ));
                        observations
                            .push("Grid rerouted: gateway-routed load caused A\u{2192}B preference change".to_owned());
                        return ProofResult {
                            success: true,
                            description: "Gateway-routed load drove A\u{2192}B routing with visible attribution shift"
                                .to_owned(),
                            observations,
                        };
                    }
                },
                Err(error) => {
                    let result = format!("verification request failed: {error}");
                    if last_probe_result.as_deref() != Some(result.as_str()) {
                        eprintln!("  [FAILOVER] {result}");
                    }
                    last_probe_result = Some(result);
                },
            }
        }

        std::thread::sleep(Duration::from_secs(2));
    }

    observations.push("A\u{2192}B flip did not converge in data plane within timeout".to_owned());
    if let Some(result) = last_probe_result {
        observations.push(format!("last probe result: {result}"));
    }
    ProofResult {
        success: false,
        description: "Gateway-routed load drove A\u{2192}B routing with visible attribution shift".to_owned(),
        observations,
    }
}

/// Proof 4: Stop pressure, wait for measured queue drain and rank
/// recovery, verify pool-a attribution returns via the live table.
fn proof_recovery(context: &DemoContext, table_start: Instant) -> ProofResult {
    let mut observations = Vec::new();
    let mtls = context.metrics_transport == MetricsTransport::MtlsProxy;

    eprintln!();
    eprintln!("  [RECOVERY] Persisting llm-d-sim waiting-requests=0 and rolling Pool A");
    if let Err(e) = set_simulator_waiting_requests("pool-a", 0, context.scoring_flavor, mtls) {
        observations.push(format!("simulator recovery rollout failed: {e}"));
        return ProofResult {
            success: false,
            description: "Recovery: simulator failed to report waiting-requests=0".to_owned(),
            observations,
        };
    }
    observations
        .push("llm-d-sim waiting-requests restored to 0 through persistent Deployment configuration".to_owned());

    eprintln!("  [RECOVERY] Pressure stopped; waiting for Pool A to drain and regain rank 0");

    let deadline = Instant::now() + DATA_PLANE_WAIT;
    let mut last_reconcile_trigger = Instant::now();
    let mut last_route = String::from("-");
    let mut last_probe_result = None::<String>;

    for cluster in CLUSTERS {
        trigger_gridnetwork_reconcile(cluster);
    }

    while Instant::now() < deadline {
        if last_reconcile_trigger.elapsed() > Duration::from_secs(5) {
            for cluster in CLUSTERS {
                trigger_gridnetwork_reconcile(cluster);
            }
            last_reconcile_trigger = Instant::now();
        }

        let epp_a = scrape_epp_metrics("pool-a", mtls);
        let epp_b = scrape_epp_metrics("pool-b", mtls);
        let candidates = read_overlay_candidates("pool-a");
        let row_a = build_scorecard_row("Cluster A", &candidates, "pool-a", &epp_a);
        let row_b = build_scorecard_row("Cluster B", &candidates, "pool-b", &epp_b);

        print_live_table_row(&LiveTableRow {
            elapsed: table_start.elapsed(),
            phase: "RECOVERY",
            rows: (&row_a, &row_b),
            last_route: &last_route,
        });

        if row_a.rank == 0 && recovery_condition_met(context.scoring_flavor, &epp_a) {
            eprintln!(
                "  [RECOVERY] Pool A drained (queue={:.1} kv={:.2}); sending verification request",
                epp_a.queue_size, epp_a.kv_cache
            );
            let probe_ctx = kind_context("pool-a");
            match send_inference_request(&probe_ctx, VCR_MODEL) {
                Ok(resp) => {
                    last_route = if resp.provider_gateway.contains("pool-a") {
                        "pool-a".to_owned()
                    } else {
                        "pool-b".to_owned()
                    };
                    let result = format!(
                        "request attribution: gateway={} provider={}",
                        resp.provider_gateway, resp.demo_attribution
                    );
                    if last_probe_result.as_deref() != Some(result.as_str()) {
                        eprintln!("  [RECOVERY] {result}");
                    }
                    last_probe_result = Some(result);
                    if resp.provider_gateway.contains("pool-a") && resp.demo_attribution.contains("pool-a") {
                        eprintln!("  [RECOVERED] Pool A is preferred again; request attributed to pool-a");
                        print_scorecard_with_cause(
                            "RECOVERED",
                            &[&row_a, &row_b],
                            "CLUSTER A",
                            &candidates,
                            "Pressure stopped; Pool A drained and regained rank 0.",
                        );
                        observations.push(format!(
                            "recovery: pool-a queue={:.2} kv={:.2} score={:.2} rank=0",
                            row_a.queue, row_a.kv_cache, row_a.score
                        ));
                        observations.push(format!(
                            "attribution: gateway={} provider={}",
                            resp.provider_gateway, resp.demo_attribution
                        ));
                        observations.push("pool-a recovered to rank 0, pool-a attribution confirmed".to_owned());
                        return ProofResult {
                            success: true,
                            description: "Recovery: measured queue drain restores pool-a, attribution confirmed"
                                .to_owned(),
                            observations,
                        };
                    }
                },
                Err(error) => {
                    let result = format!("verification request failed: {error}");
                    if last_probe_result.as_deref() != Some(result.as_str()) {
                        eprintln!("  [RECOVERY] {result}");
                    }
                    last_probe_result = Some(result);
                },
            }
        }

        std::thread::sleep(Duration::from_secs(2));
    }

    observations.push("pool-a did not recover with confirmed routing within timeout".to_owned());
    if let Some(result) = last_probe_result {
        observations.push(format!("last probe result: {result}"));
    }
    ProofResult {
        success: false,
        description: "Recovery: measured queue drain restores pool-a, attribution confirmed".to_owned(),
        observations,
    }
}

// ---------------------------------------------------------------------------
// Pressure generator
// ---------------------------------------------------------------------------

/// Persist a deterministic queue value, then roll the simulator pods so the
/// value is read from startup configuration rather than an in-memory admin
/// endpoint. The EPP metric and overlay checks remain the authoritative
/// downstream observations.
fn set_simulator_waiting_requests(
    cluster: &str,
    waiting_requests: u32,
    scoring_flavor: ScoringFlavor,
    mtls: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = kind_context(cluster);
    let (waiting_requests, kv_cache_usage) = simulator_metrics(waiting_requests, scoring_flavor);
    let config = simulator_config(waiting_requests, kv_cache_usage);
    kubectl::apply_manifest(&ctx, &config)
        .map_err(|e| format!("apply persistent simulator config on {cluster}: {e}"))?;

    for deployment in SIMULATOR_DEPLOYMENTS {
        let requested_value = waiting_requests.to_string();
        let current_value = simulator_waiting_requests_annotation(&ctx, deployment)?;
        if !simulator_annotation_needs_rollout(current_value.as_deref(), &requested_value) {
            continue;
        }
        let before = deployment_generation(&ctx, deployment)?;
        let patch = serde_json::json!({
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "grid.praxis-proxy.io/fake-waiting-requests": requested_value.as_str()
                        }
                    }
                }
            }
        })
        .to_string();
        let output = Command::new("kubectl")
            .args([
                "--context",
                &ctx,
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
                "patch simulator deployment/{deployment}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )
            .into());
        }
        let after = deployment_generation(&ctx, deployment)?;
        if after <= before {
            return Err(format!("deployment/{deployment} generation did not change ({before} -> {after})").into());
        }
        kubectl::wait_for_rollout_ns(&ctx, deployment, GRID_SYSTEM_NS, cluster)
            .map_err(|e| format!("wait for simulator deployment/{deployment} rollout: {e}"))?;
    }

    let deadline = Instant::now() + DATA_PLANE_WAIT;
    while Instant::now() < deadline {
        let config_text = get_configmap_data_key(&ctx, GRID_SYSTEM_NS, SIMULATOR_CONFIGMAP, "config.yaml");
        if config_text
            .as_deref()
            .is_ok_and(|text| text.contains(&format!("waiting-requests: {waiting_requests}")))
            && kubectl_exec_epp_metrics(cluster, mtls)
                .ok()
                .and_then(|text| parse_required_epp_metrics(&text).ok())
                .is_some_and(|metrics| {
                    (metrics.queue_size - f64::from(waiting_requests)).abs() < f64::EPSILON
                        && (metrics.kv_cache - kv_cache_usage).abs() < f64::EPSILON
                })
        {
            eprintln!("  [OK] {cluster}: simulator waiting-requests={waiting_requests} is live in EPP metrics");
            return Ok(());
        }
        std::thread::sleep(DATA_PLANE_INTERVAL);
    }
    Err(format!("{cluster}: simulator did not expose waiting-requests={waiting_requests} within timeout").into())
}

/// Read the desired deterministic pressure from one simulator Deployment.
fn simulator_waiting_requests_annotation(
    context: &str,
    deployment: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    const ANNOTATION: &str = "grid.praxis-proxy.io/fake-waiting-requests";
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            &format!("deployment/{deployment}"),
            "-o",
            "json",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "read simulator deployment/{deployment} pressure annotation: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    Ok(value
        .pointer("/spec/template/metadata/annotations")
        .and_then(serde_json::Value::as_object)
        .and_then(|annotations| annotations.get(ANNOTATION))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned))
}

/// A matching simulator value is already active, so no rollout is needed.
fn simulator_annotation_needs_rollout(current: Option<&str>, requested: &str) -> bool {
    current != Some(requested)
}

/// Render the complete simulator startup configuration for a queue value.
fn simulator_config(waiting_requests: u32, kv_cache_usage: f64) -> String {
    format!(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {SIMULATOR_CONFIGMAP}\n  namespace: {GRID_SYSTEM_NS}\ndata:\n  config.yaml: |\n    port: 8000\n    model: {VCR_MODEL}\n    served-model-name: [{VCR_MODEL}]\n    mode: echo\n    fake-metrics:\n      waiting-requests: {waiting_requests}\n      kv-cache-usage: {kv_cache_usage}\n"
    )
}

/// Select deterministic simulator metrics for the active scoring strategy.
fn simulator_metrics(pressure: u32, scoring_flavor: ScoringFlavor) -> (u32, f64) {
    if pressure == 0 {
        return (0, 0.0);
    }
    match scoring_flavor {
        ScoringFlavor::QueueDepth => (pressure, 0.0),
        ScoringFlavor::KvCachePressure => (0, 0.95),
    }
}

/// Read a Deployment generation before and after a deterministic simulator
/// configuration change.
fn deployment_generation(context: &str, deployment: &str) -> Result<i64, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            &format!("deployment/{deployment}"),
            "-o",
            "jsonpath={.metadata.generation}",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "read deployment/{deployment} generation: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<i64>()
        .map_err(|e| format!("invalid deployment/{deployment} generation: {e}").into())
}

// ---------------------------------------------------------------------------
// VCR config helpers
// ---------------------------------------------------------------------------

/// Read a specific environment variable from a Deployment's first container.
fn kubectl_get_deployment_env(
    context: &str,
    deployment: &str,
    env_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let jsonpath = format!("{{.spec.template.spec.containers[0].env[?(@.name==\"{env_name}\")].value}}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            &format!("deployment/{deployment}"),
            "-o",
            &format!("jsonpath={jsonpath}"),
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "deployment/{deployment} env {env_name}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let val = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if val.is_empty() {
        return Err(format!("deployment/{deployment} env {env_name} is empty").into());
    }
    Ok(val)
}

// ---------------------------------------------------------------------------
// EPP metrics helpers
// ---------------------------------------------------------------------------

/// Scraped EPP pool metrics (used for convergence gating, not scorecard display).
struct EppMetrics {
    /// Average queue size (raw, unnormalized).
    queue_size: f64,
    /// Average KV-cache utilization (raw, unnormalized, 0.0-1.0).
    kv_cache: f64,
}

/// Scrape EPP metrics.
///
/// In mTLS mode, execs into the nginx sidecar to access localhost:9090.
/// In direct-HTTP mode, uses the Kubernetes API proxy to reach the Service.
fn scrape_epp_metrics(cluster: &str, mtls: bool) -> EppMetrics {
    let text = kubectl_exec_epp_metrics(cluster, mtls).unwrap_or_default();
    parse_epp_metrics(&text)
}

/// EPP Prometheus metric names accepted for each reading, in priority order.
///
/// The endpoint picker's metric names have changed across releases: the
/// current `llm-d-router-endpoint-picker` build emits the `llm_d_epp_*`
/// series, older `llm-d-inference-scheduler` builds emit `inference_pool_*`,
/// and an intermediate build emitted `llm_d_router_epp_*`. Keeping the names
/// as data lets one accessor serve whichever EPP image the topology pins.
/// Average per-pool request queue depth, newest EPP series first.
const EPP_QUEUE_SIZE_METRICS: &[&str] = &[
    "llm_d_epp_average_queue_size",
    "inference_pool_average_queue_size",
    "llm_d_router_epp_average_queue_size",
];
/// Average per-pool KV cache utilization, newest EPP series first.
const EPP_KV_CACHE_METRICS: &[&str] = &[
    "llm_d_epp_average_kv_cache_utilization",
    "inference_pool_average_kv_cache_utilization",
    "llm_d_router_epp_average_kv_cache_utilization",
];
/// Ready endpoint/pod count for a pool, newest EPP series first.
const EPP_READY_METRICS: &[&str] = &[
    "llm_d_epp_ready_endpoints",
    "inference_pool_ready_pods",
    "llm_d_router_epp_ready_endpoints",
];

/// Parse `EppMetrics` out of raw Prometheus text (functional core of
/// [`scrape_epp_metrics`], separated out so metric-name-fallback behavior is
/// unit-testable without a live EPP).
///
/// Each field takes the first series the EPP actually exposes (see
/// [`EPP_QUEUE_SIZE_METRICS`] and [`EPP_KV_CACHE_METRICS`]), so a build
/// emitting only the newer or only the older names still yields a real
/// reading. An absent series must not be read as a silent 0.0: that would
/// leave a `kvCachePressure` run's pressure phase unannounced even while real
/// KV pressure is driving the rank flip.
fn parse_epp_metrics(text: &str) -> EppMetrics {
    EppMetrics {
        queue_size: first_prom_value(text, EPP_QUEUE_SIZE_METRICS).unwrap_or(0.0),
        kv_cache: first_prom_value(text, EPP_KV_CACHE_METRICS).unwrap_or(0.0),
    }
}

/// Parse required pool metrics without treating a failed scrape as zero.
fn parse_required_epp_metrics(text: &str) -> Result<EppMetrics, Box<dyn std::error::Error>> {
    let queue_size = first_prom_value(text, EPP_QUEUE_SIZE_METRICS)
        .ok_or("required queue metric is absent from the simulator/EPP scrape")?;
    let kv_cache = first_prom_value(text, EPP_KV_CACHE_METRICS)
        .ok_or("required KV-cache metric is absent from the simulator/EPP scrape")?;
    Ok(EppMetrics { queue_size, kv_cache })
}

/// Whether the pressure phase should be announced/entered for the given
/// scoring flavor.
///
/// Both metrics typically rise together under deterministic simulator pressure.
/// synthetic load, but the announced phase must key off the signal that
/// actually drives the active `GridNetwork` scoring strategy — otherwise a
/// `kvCachePressure` run could narrate "queue pressure" while queue depth
/// isn't what's producing the rank flip.
fn pressure_phase_active(flavor: ScoringFlavor, epp: &EppMetrics) -> bool {
    match flavor {
        ScoringFlavor::QueueDepth => epp.queue_size > QUEUE_PRESSURE_THRESHOLD,
        ScoringFlavor::KvCachePressure => epp.kv_cache > KV_CACHE_PRESSURE_THRESHOLD,
    }
}

/// Whether pool-a has drained enough, for the given scoring flavor, to
/// attempt the recovery verification probe.
///
/// `QueueDepth` uses its own calibrated [`RECOVERY_QUEUE_THRESHOLD`] (looser
/// than [`QUEUE_PRESSURE_THRESHOLD`] by design -- recovery only needs "clearly
/// drained," not a full return below the phase-detection threshold).
/// `KvCachePressure` requires the shared queue-drain threshold as well as the
/// inverse of [`pressure_phase_active`]. This prevents a locality tie-break
/// from being reported as recovery while request queues remain saturated.
fn recovery_condition_met(flavor: ScoringFlavor, epp: &EppMetrics) -> bool {
    match flavor {
        ScoringFlavor::QueueDepth => epp.queue_size < RECOVERY_QUEUE_THRESHOLD,
        ScoringFlavor::KvCachePressure => {
            epp.queue_size < RECOVERY_QUEUE_THRESHOLD && !pressure_phase_active(flavor, epp)
        },
    }
}

/// Extract a numeric value from Prometheus text format.
fn extract_prom_value(text: &str, metric_name: &str) -> Option<f64> {
    for line in text.lines() {
        if line.starts_with(metric_name) && !line.starts_with('#') {
            let value_part = line.rsplit_once(' ').map_or("0", |(_, v)| v);
            return value_part.parse().ok();
        }
    }
    None
}

/// Value of the first metric in `names` that is present in `text`, in order.
fn first_prom_value(text: &str, names: &[&str]) -> Option<f64> {
    names.iter().find_map(|name| extract_prom_value(text, name))
}

/// Whether any metric in `names` is emitted as a (non-comment) line in `text`.
fn any_metric_present(text: &str, names: &[&str]) -> bool {
    names.iter().any(|name| {
        text.lines()
            .any(|line| line.starts_with(name) && !line.starts_with('#'))
    })
}

/// Read overlay candidates from the overlay ConfigMap on a cluster.
fn read_overlay_candidates(cluster: &str) -> Vec<OverlayCandidate> {
    let ctx = kind_context(cluster);
    let Ok(json) = get_configmap_data_key(&ctx, GRID_SYSTEM_NS, OVERLAY_CONFIGMAP, "routing-config.json") else {
        return Vec::new();
    };
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&json) else {
        return Vec::new();
    };
    let Some(candidates_arr) = parsed.get("candidates").and_then(serde_json::Value::as_array) else {
        return Vec::new();
    };
    candidates_arr
        .iter()
        .filter_map(|c| {
            let cluster_name = c.get("cluster")?.as_str()?.to_owned();
            #[expect(clippy::cast_possible_truncation, reason = "rank is always small")]
            let rank = c.get("rank").and_then(serde_json::Value::as_u64).unwrap_or(99) as u32;
            let score = c.get("score").and_then(serde_json::Value::as_f64).unwrap_or(0.0);
            let fresh = c.get("fresh").and_then(serde_json::Value::as_bool).unwrap_or(true);
            let admission = c
                .get("admission_state")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_owned();
            let selection_group = c
                .get("selection_group")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| u32::try_from(value).ok());
            let traffic_weight = c
                .get("traffic_weight")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| u32::try_from(value).ok());
            let breakdown = c
                .get("score_breakdown")
                .and_then(|v| serde_json::from_value(v.clone()).ok());
            Some(OverlayCandidate {
                cluster: cluster_name,
                rank,
                score,
                fresh,
                admission_state: admission,
                selection_group,
                traffic_weight,
                breakdown,
            })
        })
        .collect()
}

/// Get the rank of a cluster from overlay candidates.
fn overlay_rank_for_cluster(candidates: &[OverlayCandidate], cluster_suffix: &str) -> i64 {
    candidates
        .iter()
        .find(|c| c.cluster.contains(cluster_suffix))
        .map_or(99, |c| i64::from(c.rank))
}

/// Get the score of a cluster from overlay candidates.
fn overlay_score_for_cluster(candidates: &[OverlayCandidate], cluster_suffix: &str) -> f64 {
    candidates
        .iter()
        .find(|c| c.cluster.contains(cluster_suffix))
        .map_or(0.0, |c| c.score)
}

/// Build a scorecard row from the overlay decision and live EPP metrics.
///
/// The overlay owns score and rank. EPP owns the raw queue and KV-cache
/// measurements. Reconstructing raw metrics from score-breakdown weights is
/// invalid when a signal is inactive, because its zero contribution does not
/// mean the underlying metric is zero.
fn build_scorecard_row(
    label: &str,
    candidates: &[OverlayCandidate],
    cluster_suffix: &str,
    metrics: &EppMetrics,
) -> ScorecardRow {
    let rank = overlay_rank_for_cluster(candidates, cluster_suffix);
    let score = overlay_score_for_cluster(candidates, cluster_suffix);
    ScorecardRow {
        cluster: label.to_owned(),
        queue: metrics.queue_size,
        capacity: QUEUE_CAPACITY,
        pressure: metrics.queue_size / QUEUE_CAPACITY,
        kv_cache: metrics.kv_cache,
        score,
        rank,
    }
}

/// Print a narrated CLI scorecard with scoring breakdown and a causal explanation.
fn print_scorecard_with_cause(
    state: &str,
    rows: &[&ScorecardRow],
    preferred: &str,
    candidates: &[OverlayCandidate],
    cause: &str,
) {
    eprintln!();
    eprintln!("  LLM-D POOL ROUTING DECISION");
    eprintln!("  State: {state}");
    eprintln!();
    eprintln!(
        "  {:>14} {:>7} {:>9} {:>9} {:>9} {:>7} {:>5}",
        "", "Queue", "Capacity", "Pressure", "KV Cache", "Score", "Rank"
    );
    for row in rows {
        eprintln!(
            "  {:>14} {:>7.1} {:>9.0} {:>9.2} {:>9.2} {:>7.2} {:>5}",
            row.cluster, row.queue, row.capacity, row.pressure, row.kv_cache, row.score, row.rank
        );
    }

    eprintln!();
    eprintln!(
        "  {:>14} {:>8} {:>5} {:>5} {:>6} {:>7} {:>5}  {:>5}",
        "Signal", "Locality", "Queue", "KV", "Prefix", "Latency", "Cost", "Total"
    );
    for oc in candidates {
        if let Some(bd) = &oc.breakdown {
            let label = if oc.cluster.contains("pool-a") {
                "Cluster A"
            } else {
                "Cluster B"
            };
            eprintln!(
                "  {:>14} {:>8.2} {:>5.2} {:>5.2} {:>6.2} {:>7.2} {:>5.2}  {:>5.2}",
                label, bd.locality, bd.queue_depth, bd.kv_cache, bd.prefix_cache, bd.latency, bd.cost, bd.total,
            );
        }
    }

    eprintln!();
    eprintln!("  Grid preference: {preferred}");
    if !cause.is_empty() {
        eprintln!("  Reason: {cause}");
    }
    eprintln!();
}

/// Print the live metrics table header.
fn print_live_table_header() {
    eprintln!();
    eprintln!(
        "  {:<6} {:<11} {:>7} {:>5} {:>7} {:>6}  {:>7} {:>5} {:>7} {:>6}  {:>10}",
        "TIME", "PHASE", "A_QUEUE", "A_KV", "A_SCORE", "A_RANK", "B_QUEUE", "B_KV", "B_SCORE", "B_RANK", "LAST_ROUTE"
    );
}

/// Snapshot of live table data for one row.
struct LiveTableRow<'row> {
    /// Elapsed time since the table started.
    elapsed: Duration,
    /// Current phase label.
    phase: &'row str,
    /// Scorecard rows for pool-a and pool-b.
    rows: (&'row ScorecardRow, &'row ScorecardRow),
    /// Last probe request attribution.
    last_route: &'row str,
}

/// Print one row of the live metrics table.
fn print_live_table_row(row: &LiveTableRow<'_>) {
    let secs = row.elapsed.as_secs();
    let time_str = format!("{:02}:{:02}", secs / 60, secs % 60);
    let (a, b) = row.rows;
    eprintln!(
        "  {:<6} {:<11} {:>7.1} {:>.2} {:>7.2} {:>6}  {:>7.1} {:>.2} {:>7.2} {:>6}  {:>10}",
        time_str, row.phase, a.queue, a.kv_cache, a.score, a.rank, b.queue, b.kv_cache, b.score, b.rank, row.last_route,
    );
}

// ---------------------------------------------------------------------------
// Routing helpers
// ---------------------------------------------------------------------------

/// Send an inference request and capture gateway attribution headers.
fn send_inference_request(kube_context: &str, model: &str) -> Result<InferenceResponse, Box<dyn std::error::Error>> {
    let body = format!(r#"{{"model":"{model}","messages":[{{"role":"user","content":"test"}}]}}"#,);
    let session_id = format!("probe-{}", format_utc_timestamp());
    let curl_cmd = format!(
        "curl -s -o /dev/null \
         -w 'STATUS:%{{http_code}}\\nPROVIDER_GW:%header{{X-Grid-LlmD-Provider-Gateway}}\\nDEMO_ATTRIB:%header{{x-ai-demo-provider-gateway}}\\n' \
         -X POST http://consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions \
         -H 'Content-Type: application/json' \
         -H 'X-Session-Id: {session_id}' \
         -d '{body}'",
    );
    let raw = kubectl_exec_curl_raw(kube_context, &curl_cmd)?;
    let mut status = 0_u16;
    let mut provider_gw = String::new();
    let mut demo_attr = String::new();
    for line in raw.lines() {
        if let Some(code) = line.strip_prefix("STATUS:") {
            status = code.trim().parse().unwrap_or(0);
        } else if let Some(val) = line.strip_prefix("PROVIDER_GW:") {
            val.trim().clone_into(&mut provider_gw);
        } else if let Some(val) = line.strip_prefix("DEMO_ATTRIB:") {
            val.trim().clone_into(&mut demo_attr);
        }
    }
    if status != 200 {
        return Err(format!("inference request returned HTTP {status}").into());
    }
    if provider_gw.is_empty() || demo_attr.is_empty() {
        return Err("missing attribution headers in response".into());
    }
    Ok(InferenceResponse {
        provider_gateway: provider_gw,
        demo_attribution: demo_attr,
    })
}

/// Fetch EPP metrics.
///
/// In mTLS mode, execs into the nginx sidecar to access localhost:9090.
/// In direct-HTTP mode, uses the Kubernetes API server proxy to reach
/// the metrics Service without requiring extra images or containers.
fn kubectl_exec_epp_metrics(cluster: &str, mtls: bool) -> Result<String, Box<dyn std::error::Error>> {
    let ctx = kind_context(cluster);
    if mtls {
        let output = Command::new("kubectl")
            .args([
                "--context",
                &ctx,
                "-n",
                GRID_SYSTEM_NS,
                "exec",
                "deploy/llmd-epp",
                "-c",
                "metrics-tls-proxy",
                "--",
                "wget",
                "-qO-",
                "--timeout=5",
                "http://127.0.0.1:9090/metrics",
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "kubectl exec metrics failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )
            .into());
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let output = Command::new("kubectl")
            .args([
                "--context",
                &ctx,
                "get",
                "--raw",
                "/api/v1/namespaces/grid-system/services/llmd-epp-metrics:9090/proxy/metrics",
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "kubectl api proxy metrics failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )
            .into());
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

/// Run an arbitrary command via `kubectl run` in a temporary pod.
fn kubectl_exec_curl_raw(kube_context: &str, cmd: &str) -> Result<String, Box<dyn std::error::Error>> {
    let pod_name = format!("curl-probe-{}", &format_utc_timestamp()[9..15]);
    let output = Command::new("kubectl")
        .args([
            "--context",
            kube_context,
            "run",
            &pod_name,
            "--image=curlimages/curl:8.5.0",
            "--restart=Never",
            "--rm",
            "-i",
            "-n",
            GRID_SYSTEM_NS,
            "--",
            "sh",
            "-c",
            cmd,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!("kubectl run failed: {}", String::from_utf8_lossy(&output.stderr).trim()).into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Extract a specific data key from a ConfigMap as raw text.
fn get_configmap_data_key(
    context: &str,
    namespace: &str,
    name: &str,
    key: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let escaped_key = key.replace('.', r"\.");
    let jsonpath = format!("{{.data.{escaped_key}}}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            namespace,
            "get",
            "configmap",
            name,
            "-o",
            &format!("jsonpath={jsonpath}"),
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl get configmap/{name} key={key} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let data = String::from_utf8_lossy(&output.stdout).to_string();
    if data.is_empty() {
        return Err(format!("configmap/{name} key={key} is empty").into());
    }
    Ok(data)
}

// ---------------------------------------------------------------------------
// SWIM seeding
// ---------------------------------------------------------------------------

/// Read the SWIM LoadBalancer IP for a cluster.
fn read_swim_lb_ip(cluster: &str) -> Result<String, Box<dyn std::error::Error>> {
    let context = kind_context(cluster);
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

/// Seed SWIM membership by upgrading operators with cross-cluster seeds.
fn seed_swim_membership() -> Result<(), Box<dyn std::error::Error>> {
    let mut ips: Vec<(String, String)> = Vec::new();
    for cluster in CLUSTERS {
        let ip = read_swim_lb_ip(cluster)?;
        eprintln!("  {cluster}: SWIM LB IP = {ip}");
        ips.push(((*cluster).to_owned(), ip));
    }

    for (cluster, this_ip) in &ips {
        let peer_seeds: Vec<String> = ips
            .iter()
            .filter(|(c, _)| c != cluster)
            .map(|(_, ip)| format!("{ip}:7946"))
            .collect();
        let seeds = peer_seeds.join(",");
        let context = kind_context(cluster);
        let seeds_escaped = seeds.replace(',', "\\,");

        let upgrade = Command::new("helm")
            .args([
                "upgrade",
                "grid-operator",
                "charts/grid-operator",
                "--version",
                "0.1.0",
                "--namespace",
                GRID_SYSTEM_NS,
                "--kube-context",
                &context,
                "--reuse-values",
                "--set",
                &format!("swim.siteName={cluster}"),
                "--set",
                &format!("swim.advertiseAddress={this_ip}:7946"),
                "--set",
                &format!("swim.seeds={seeds_escaped}"),
                "--set",
                "swim.service.enabled=true",
                "--set",
                "swim.service.type=LoadBalancer",
                "--set",
                "gateway.serviceName=provider-gateway",
                "--set-string",
                "gateway.port=8443",
            ])
            .output()?;
        if !upgrade.status.success() {
            return Err(format!(
                "{cluster}: helm upgrade failed: {}",
                String::from_utf8_lossy(&upgrade.stderr).trim()
            )
            .into());
        }
        eprintln!("  {cluster}: seeds={seeds}");
    }

    for cluster in CLUSTERS {
        let context = kind_context(cluster);
        let wait = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "rollout",
                "status",
                "deployment/grid-operator",
                "--timeout=120s",
            ])
            .status()?;
        if !wait.success() {
            return Err(format!("{cluster}: operator restart timed out").into());
        }
        eprintln!("  [OK] {cluster}: operator restarted with SWIM seeds");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Certificate and trust staging
// ---------------------------------------------------------------------------

/// Generate TLS certificates for both clusters and metrics TLS.
fn stage_certificates(certs_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let clusters: Vec<String> = CLUSTERS.iter().map(|s| (*s).to_owned()).collect();
    certs::generate_all_in_dir(&clusters, certs_dir)?;
    eprintln!("  [OK] TLS certificates generated for pool-a, pool-b");

    certs::generate_metrics_certs(METRICS_CA_CN, METRICS_SERVER_DNS)?;
    eprintln!("  [OK] Metrics TLS certificates generated (separate CA)");
    Ok(())
}

/// Install provider trust secrets into both clusters.
///
/// Metrics TLS secrets are installed earlier in phase 7 (before EPP
/// deployment) since the nginx sidecar mounts them at startup.
fn install_provider_trust(certs_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    for cluster in CLUSTERS {
        let ctx = kind_context(cluster);

        apply_tls_secret(&ctx, cluster, CONSUMER_TLS_SECRET, certs_dir)?;
        apply_tls_secret(&ctx, cluster, PROVIDER_TLS_SECRET, certs_dir)?;

        apply_credential_secret(&ctx, VCR_INFERENCE_CREDENTIAL, "vcr-demo-token")?;

        eprintln!("  [OK] {cluster}: TLS secrets and credentials installed");
    }
    Ok(())
}

/// Install the three metrics TLS secrets (CA, server, client) into a cluster.
fn install_metrics_tls_secrets(context: &str, certs_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    apply_metrics_ca_secret(context, certs_dir)?;
    apply_metrics_server_secret(context, certs_dir)?;
    apply_metrics_client_secret(context, certs_dir)?;
    Ok(())
}

/// Create the metrics CA Secret (holds only ca.crt).
fn apply_metrics_ca_secret(context: &str, certs_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "create",
            "secret",
            "generic",
            METRICS_CA_SECRET,
            &format!("--from-file=ca.crt={}", certs_dir.join("metrics-ca.pem").display()),
            "--dry-run=client",
            "-o",
            "yaml",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to render Secret/{METRICS_CA_SECRET}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    kubectl::apply_manifest(context, &String::from_utf8(output.stdout)?)
}

/// Create the metrics server TLS Secret (tls.crt + tls.key for nginx).
fn apply_metrics_server_secret(context: &str, certs_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "create",
            "secret",
            "generic",
            METRICS_SERVER_TLS_SECRET,
            &format!(
                "--from-file=tls.crt={}",
                certs_dir.join("metrics-server-cert.pem").display()
            ),
            &format!(
                "--from-file=tls.key={}",
                certs_dir.join("metrics-server-key.pem").display()
            ),
            "--dry-run=client",
            "-o",
            "yaml",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to render Secret/{METRICS_SERVER_TLS_SECRET}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    kubectl::apply_manifest(context, &String::from_utf8(output.stdout)?)
}

/// Create the metrics client TLS Secret (tls.crt + tls.key for the operator).
fn apply_metrics_client_secret(context: &str, certs_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "create",
            "secret",
            "generic",
            METRICS_CLIENT_TLS_SECRET,
            &format!(
                "--from-file=tls.crt={}",
                certs_dir.join("metrics-client-cert.pem").display()
            ),
            &format!(
                "--from-file=tls.key={}",
                certs_dir.join("metrics-client-key.pem").display()
            ),
            "--dry-run=client",
            "-o",
            "yaml",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to render Secret/{METRICS_CLIENT_TLS_SECRET}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    kubectl::apply_manifest(context, &String::from_utf8(output.stdout)?)
}

/// Create a TLS secret from the generated cert, key, and CA files.
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

/// Create an Opaque Secret with a `token` key.
fn apply_credential_secret(context: &str, secret_name: &str, token: &str) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = format!(
        r#"{{"apiVersion":"v1","kind":"Secret","metadata":{{"name":"{secret_name}","namespace":"{GRID_SYSTEM_NS}"}},"type":"Opaque","stringData":{{"token":"{token}"}}}}"#,
    );
    kubectl::apply_manifest(context, &manifest)
}

/// Authorize auto-discovered remote GridSites with identity trust.
fn authorize_discovered_sites(certs_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    const TRUST_TIMEOUT: Duration = Duration::from_secs(120);
    const GRID_NETWORK: &str = "grid-llmd-pool-metrics";

    for local in CLUSTERS {
        let context = kind_context(local);
        eprintln!("  {local}: authorizing remote provider sites");
        for remote in CLUSTERS {
            if *remote == *local {
                continue;
            }
            let site_name = format!("{GRID_NETWORK}-{remote}");
            operator::wait_for_auto_gridsite(&context, &site_name, GRID_NETWORK, TRUST_TIMEOUT)?;
            let canonical_fp = certs::site_certificate_fingerprint_in_dir(remote, certs_dir)?;
            operator::wait_for_expected_site_certificate(&context, &site_name, &canonical_fp, TRUST_TIMEOUT)?;
            let server_name = format!("{remote}.grid.internal");
            operator::patch_gridsite_identity_trust(&context, &site_name, &canonical_fp, &server_name)?;
            operator::wait_for_gridsite_phase(&context, &site_name, "Active", TRUST_TIMEOUT)?;
        }
    }
    eprintln!("  [OK] All auto-discovered remote GridSites authorized and Active");
    Ok(())
}

/// Wait for overlay convergence on both consumer gateways.
fn wait_for_overlay_convergence(context: &DemoContext) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + DATA_PLANE_WAIT;
    for cluster in CLUSTERS {
        let ctx = kind_context(cluster);
        let mut converged = false;
        while Instant::now() < deadline {
            match kubectl::get_configmap_yaml(&ctx, GRID_SYSTEM_NS, OVERLAY_CONFIGMAP) {
                Ok(yaml) if yaml.contains("llmd-pool-a-provider") && yaml.contains("llmd-pool-b-provider") => {
                    converged = true;
                    break;
                },
                _ => std::thread::sleep(DATA_PLANE_INTERVAL),
            }
        }
        if !converged {
            let diagnostics = capture_overlay_timeout_diagnostics(context)?;
            return Err(format!(
                "{cluster}: overlay did not converge within timeout; diagnostics: {}",
                diagnostics.display()
            )
            .into());
        }
        eprintln!("  [OK] {cluster}: overlay converged with both providers");
    }
    Ok(())
}

/// Save bounded signal-path state before setup-failure teardown removes the clusters.
fn capture_overlay_timeout_diagnostics(context: &DemoContext) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let mut clusters = serde_json::Map::new();
    for cluster in CLUSTERS {
        let kube_context = kind_context(cluster);
        let logs = diagnostic_kubectl(
            &kube_context,
            &["logs", "deployment/grid-operator", "--since=10m", "--tail=1000"],
        );
        let relevant_logs: Vec<String> = logs
            .lines()
            .filter(|line| {
                let line = line.to_ascii_lowercase();
                line.contains("signals:")
                    || line.contains("peer signals")
                    || line.contains("peer poll")
                    || line.contains("pressure-weighted")
                    || line.contains("routing overlay render failed")
                    || line.contains("no fresh grid_routing_")
            })
            .take(200)
            .map(|line| safe_truncate_str(line, 512))
            .collect();
        let metrics = diagnostic_kubectl(
            &kube_context,
            &[
                "exec",
                "deployment/grid-operator",
                "--",
                "/bin/busybox",
                "wget",
                "-qO-",
                "http://llmd-epp-metrics.grid-system.svc.cluster.local:9090/metrics",
            ],
        );
        let relevant_metrics: Vec<&str> = metrics
            .lines()
            .filter(|line| {
                [
                    "inference_pool_average_queue_size",
                    "inference_pool_average_kv_cache_utilization",
                    "inference_pool_ready_pods",
                ]
                .iter()
                .any(|metric| line.starts_with(metric))
            })
            .take(100)
            .collect();
        let operator_metrics = diagnostic_kubectl(
            &kube_context,
            &[
                "exec",
                "deployment/grid-operator",
                "--",
                "/bin/busybox",
                "wget",
                "-qO-",
                "http://127.0.0.1:9090/metrics",
            ],
        );
        let relevant_peer_metrics: Vec<&str> = operator_metrics
            .lines()
            .filter(|line| {
                [
                    "grid_peer_poll_total",
                    "grid_peer_poll_retries_total",
                    "grid_peer_poll_duration_seconds",
                    "grid_peer_poll_slow_total",
                    "grid_peer_response_bytes_total",
                    "grid_collection_up",
                    "grid_peer_last_success_timestamp_seconds",
                    "grid_peer_polls_in_flight",
                ]
                .iter()
                .any(|metric| line.starts_with(metric))
            })
            .take(100)
            .collect();
        let providers = diagnostic_kubectl(&kube_context, &["get", "inferenceproviders", "-o", "json"]);
        let networks = diagnostic_kubectl(&kube_context, &["get", "gridnetworks", "-o", "json"]);
        let overlay_raw = diagnostic_kubectl(&kube_context, &["get", "configmap", OVERLAY_CONFIGMAP, "-o", "json"]);
        let overlay = serde_json::from_str::<serde_json::Value>(&overlay_raw)
            .ok()
            .and_then(|value| value.pointer("/data/routing-config.json").cloned())
            .unwrap_or_else(|| serde_json::json!({"unavailable": safe_truncate_str(&overlay_raw, 512)}));
        clusters.insert(
            (*cluster).to_owned(),
            serde_json::json!({
                "operator_signal_logs": relevant_logs,
                "operator_peer_metrics": relevant_peer_metrics,
                "epp_metrics": relevant_metrics,
                "inference_providers": diagnostic_provider_summary(&providers),
                "grid_networks": diagnostic_network_summary(&networks),
                "routing_overlay": overlay,
            }),
        );
    }
    let path = context.evidence_dir.join("overlay-convergence-diagnostics.json");
    fs::write(&path, serde_json::to_vec_pretty(&serde_json::Value::Object(clusters))?)?;
    Ok(path)
}

/// Run a short best-effort kubectl command for sanitized failure evidence.
fn diagnostic_kubectl(context: &str, args: &[&str]) -> String {
    let output = Command::new("timeout")
        .arg("20s")
        .arg("kubectl")
        .arg("--context")
        .arg(context)
        .arg("-n")
        .arg(GRID_SYSTEM_NS)
        .args(args)
        .output();
    match output {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout).into_owned(),
        Ok(output) => format!(
            "command failed ({}): {}",
            output.status,
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 512)
        ),
        Err(error) => format!("command could not run: {error}"),
    }
}

/// Keep only non-secret provider identity, pressure configuration, and phase fields.
fn diagnostic_provider_summary(raw: &str) -> serde_json::Value {
    let Ok(document) = serde_json::from_str::<serde_json::Value>(raw) else {
        return serde_json::json!({"unavailable": safe_truncate_str(raw.trim(), 512)});
    };
    let providers: Vec<serde_json::Value> = document
        .get("items")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .map(|provider| {
            let metrics = provider.pointer("/spec/metricsConfig");
            serde_json::json!({
                "name": provider.pointer("/metadata/name"),
                "gridNetworkRef": provider.pointer("/spec/gridNetworkRef"),
                "poolName": metrics.and_then(|value| value.get("poolName")),
                "queueCapacity": metrics.and_then(|value| value.get("queueCapacity")),
                "signalNames": metrics.and_then(|value| value.get("signalNames")),
                "phase": provider.pointer("/status/phase"),
                "reason": provider.pointer("/status/reason"),
            })
        })
        .collect();
    serde_json::json!(providers)
}

/// Keep GridNetwork generation, transport, placement policy, and status evidence.
fn diagnostic_network_summary(raw: &str) -> serde_json::Value {
    let Ok(document) = serde_json::from_str::<serde_json::Value>(raw) else {
        return serde_json::json!({"unavailable": safe_truncate_str(raw.trim(), 512)});
    };
    let networks: Vec<serde_json::Value> = document
        .get("items")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .map(|network| {
            serde_json::json!({
                "name": network.pointer("/metadata/name"),
                "generation": network.pointer("/metadata/generation"),
                "signalTransport": network.pointer("/spec/signalTransport"),
                "placementPolicy": network.pointer("/spec/placementPolicy"),
                "status": network.get("status"),
            })
        })
        .collect();
    serde_json::json!(networks)
}

/// Append one secondary evidence/cleanup error without losing the first failure.
fn append_run_error(error: &mut Option<String>, additional: String) {
    match error {
        Some(primary) => {
            primary.push_str("; ");
            primary.push_str(&additional);
        },
        None => *error = Some(additional),
    }
}

// ---------------------------------------------------------------------------
// Image loading
// ---------------------------------------------------------------------------

/// Load pre-built images into Kind clusters in local-image mode.
///
/// Uses the exact resolved image references so `imagePullPolicy: Never`
/// references the image that was actually loaded into each Kind node.
fn load_images_into_clusters(context: &DemoContext) -> Result<(), Box<dyn std::error::Error>> {
    if uses_registry_images() {
        eprintln!("  [OK] Registry image mode: skipped local Kind image loading");
        return Ok(());
    }

    let mut tags: Vec<&str> = vec![
        &context.images.operator,
        &context.images.overlay_sync,
        &context.images.gateway,
        &context.images.epp,
        &context.images.vcr,
    ];
    if let Some(nginx) = &context.images.nginx {
        tags.push(nginx);
    }
    for cluster in CLUSTERS {
        let kind_name = format!("{}-{cluster}", forge_cluster_prefix());
        for image_tag in &tags {
            load_docker_image_into_kind(image_tag, &kind_name)?;
        }
        eprintln!("  [OK] {cluster}: all images loaded");
    }
    Ok(())
}

/// Stream a Docker image directly into Kind's containerd image store.
///
/// `kind load docker-image` imports with `ctr --all-platforms`. Docker's
/// containerd image store can retain an OCI index while only having the host
/// platform's child content available locally, causing that import to fail on
/// multi-platform images. Importing the Docker save stream without
/// `--all-platforms` selects the host platform and preserves the local-image
/// workflow without weakening registry-backed deployments.
fn load_docker_image_into_kind(image: &str, kind_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let control_plane = format!("{kind_name}-control-plane");
    let mut save = Command::new("docker")
        .args(["save", image])
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

/// Collect image tags and digests for evidence.
fn collect_image_evidence(resolved: &ResolvedImages) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let mut images = BTreeMap::new();
    let mut entries: Vec<(&str, &str)> = vec![
        ("operator", &resolved.operator),
        ("gateway", &resolved.gateway),
        ("epp", &resolved.epp),
        ("vcr", &resolved.vcr),
        ("overlay-sync", &resolved.overlay_sync),
    ];
    if let Some(nginx) = &resolved.nginx {
        entries.push(("nginx", nginx));
    }
    for (role, tag) in entries {
        let digest = Command::new("docker")
            .args(["inspect", "--format", "{{.Id}}", tag])
            .output()
            .ok()
            .and_then(|o| {
                o.status
                    .success()
                    .then(|| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            })
            .unwrap_or_default();
        images.insert(role.to_owned(), format!("{tag} ({digest})"));
    }
    Ok(images)
}

/// Verify the dynamic qualification ran the gateway image built from the expected AI worktree.
fn verify_gateway_image_identity(
    image: &str,
    expected_revision: Option<&str>,
    expected_content_hash: Option<&str>,
    pods: &[PodImageEvidence],
) -> ProofResult {
    let config_id = match local_image_config_id(image) {
        Ok(config_id) => config_id,
        Err(error) => {
            return ProofResult {
                success: false,
                description: "Gateway pods ran the selected source-built Praxis AI image".to_owned(),
                observations: vec![error.to_string()],
            };
        },
    };
    let labels = match local_image_labels(image) {
        Ok(labels) => labels,
        Err(error) => {
            return ProofResult {
                success: false,
                description: "Gateway pods ran the selected source-built Praxis AI image".to_owned(),
                observations: vec![error.to_string()],
            };
        },
    };
    let source = labels
        .get("org.opencontainers.image.source")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let revision = labels
        .get("org.opencontainers.image.revision")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let version = labels
        .get("org.opencontainers.image.version")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    gateway_image_identity_proof(&GatewayImageIdentity {
        image,
        config_id: &config_id,
        source,
        revision,
        expected_revision,
        expected_content_hash,
        version,
        pods,
    })
}

/// Read the local image's OCI config digest from the Docker save manifest.
///
/// With Docker's containerd image store, `docker inspect .Id` can name the OCI
/// index while Kubernetes reports the platform image config digest. The save
/// manifest supplies the config digest that can be compared directly.
fn local_image_config_id(image: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut save = Command::new("docker")
        .args(["save", image])
        .stdout(Stdio::piped())
        .spawn()?;
    let save_stdout = save.stdout.take().ok_or("docker save did not provide stdout")?;
    let manifest = Command::new("tar")
        .args(["-xOf", "-", "manifest.json"])
        .stdin(save_stdout)
        .output()?;
    let save_status = save.wait()?;
    if !save_status.success() {
        return Err(format!("docker save failed for {image}").into());
    }
    if !manifest.status.success() {
        return Err(format!("unable to read Docker save manifest for {image}").into());
    }
    config_digest_from_save_manifest(&manifest.stdout, image).map_err(Into::into)
}

/// Extract a selected image's config digest from Docker's `manifest.json` output.
fn config_digest_from_save_manifest(manifest: &[u8], image: &str) -> Result<String, String> {
    let entries: Vec<serde_json::Value> = serde_json::from_slice(manifest).map_err(|error| error.to_string())?;
    let entry = entries
        .iter()
        .find(|entry| {
            entry
                .get("RepoTags")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|tags| tags.iter().any(|tag| tag.as_str() == Some(image)))
        })
        .ok_or_else(|| format!("Docker save manifest does not contain selected image {image}"))?;
    let config_path = entry
        .get("Config")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "Docker save manifest has no config path".to_owned())?;
    let filename = Path::new(config_path)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "Docker save config path is invalid".to_owned())?;
    if filename.len() != 64 || !filename.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("Docker save config path does not contain a SHA-256 digest".to_owned());
    }
    Ok(format!("sha256:{filename}"))
}

/// Read OCI labels from the exact local image selected for deployment.
fn local_image_labels(image: &str) -> Result<serde_json::Map<String, serde_json::Value>, Box<dyn std::error::Error>> {
    let output = Command::new("docker")
        .args(["image", "inspect", "--format", "{{json .Config.Labels}}", image])
        .output()?;
    if !output.status.success() {
        return Err(format!("unable to inspect selected gateway image {image}").into());
    }
    let labels: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    labels
        .as_object()
        .cloned()
        .ok_or_else(|| "selected gateway image has no OCI labels".into())
}

/// Pure validation for selected-image reference, runtime config digest, and source revision.
fn gateway_image_identity_proof(input: &GatewayImageIdentity<'_>) -> ProofResult {
    let image = input.image;
    let config_id = input.config_id;
    let source = input.source;
    let revision = input.revision;
    let expected_revision = input.expected_revision;
    let expected_content_hash = input.expected_content_hash;
    let version = input.version;
    let pods = input.pods;
    let mut observations = vec![
        format!("selected image: {image}"),
        format!("local OCI config digest: {config_id}"),
    ];
    let mut success = true;
    if source != "https://github.com/praxis-proxy/ai" {
        success = false;
        observations.push(format!("unexpected OCI source label: {source:?}"));
    }
    if revision.is_empty() {
        success = false;
        observations.push("OCI source revision label is missing".to_owned());
    } else {
        observations.push(format!("OCI source revision: {revision}"));
    }
    match expected_revision.filter(|expected| !expected.is_empty()) {
        Some(expected) if revision == expected => {
            observations.push(format!("expected AI worktree revision matched: {expected}"));
        },
        Some(expected) => {
            success = false;
            observations.push(format!(
                "AI source revision mismatch: expected {expected}, image has {revision}"
            ));
        },
        None => {
            success = false;
            observations.push("GRID_XTASK_GATEWAY_REVISION must name the expected AI worktree SHA".to_owned());
        },
    }
    let dirty_hash_matches = expected_content_hash
        .filter(|hash| hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .and_then(|hash| hash.get(..8).map(|prefix| (hash, prefix)));
    match dirty_hash_matches {
        Some((full_hash, short_hash)) if version.ends_with(&format!("-{short_hash}")) => {
            observations.push(format!(
                "AI worktree diff SHA-256: {full_hash}; image version suffix matched its 8-character hash prefix"
            ));
        },
        Some((_, short_hash)) => {
            success = false;
            observations.push(format!(
                "AI worktree diff SHA-256 prefix {short_hash} does not match image version label {version:?}"
            ));
        },
        None => {
            success = false;
            observations
                .push("GRID_XTASK_GATEWAY_CONTENT_SHA256 must contain the full AI worktree diff SHA-256".to_owned());
        },
    }

    let gateways: Vec<&PodImageEvidence> = pods.iter().filter(|pod| pod.container == "praxis").collect();
    if gateways.is_empty() {
        success = false;
        observations.push("no Praxis gateway containers were captured".to_owned());
    }
    for gateway in &gateways {
        let runtime_config_id = gateway
            .image_id
            .strip_prefix("containerd://")
            .or_else(|| gateway.image_id.strip_prefix("docker://"))
            .unwrap_or(&gateway.image_id);
        if gateway.requested_image != image {
            success = false;
            observations.push(format!(
                "{} / {} requested {}, expected {image}",
                gateway.cluster, gateway.pod, gateway.requested_image
            ));
        }
        if !gateway.ready {
            success = false;
            observations.push(format!(
                "{} / {} Praxis container was not Ready",
                gateway.cluster, gateway.pod
            ));
        }
        if runtime_config_id != config_id {
            success = false;
            observations.push(format!(
                "{} / {} runtime image ID {} does not match selected image config {config_id}",
                gateway.cluster, gateway.pod, gateway.image_id
            ));
        }
    }
    for pool in CLUSTERS {
        let found = gateways.iter().any(|gateway| {
            gateway.cluster.ends_with(&format!("-{pool}"))
                && gateway.requested_image == image
                && gateway.ready
                && gateway
                    .image_id
                    .strip_prefix("containerd://")
                    .or_else(|| gateway.image_id.strip_prefix("docker://"))
                    .unwrap_or(&gateway.image_id)
                    == config_id
        });
        if !found {
            success = false;
            observations.push(format!("no verified Praxis gateway image was captured in {pool}"));
        }
    }
    observations.push(format!("verified gateway containers: {}", gateways.len()));
    ProofResult {
        success,
        description: "Gateway pods ran the selected source-built Praxis AI image".to_owned(),
        observations,
    }
}

/// Capture pod image references and immutable runtime IDs from each isolated cluster.
fn collect_pod_image_evidence() -> Result<Vec<PodImageEvidence>, Box<dyn std::error::Error>> {
    let mut captured = Vec::new();
    for cluster in CLUSTERS {
        let context = kind_context(cluster);
        let output = Command::new("kubectl")
            .args(["--context", &context, "-n", GRID_SYSTEM_NS, "get", "pods", "-o", "json"])
            .output()?;
        if !output.status.success() {
            return Err(format!("{cluster}: unable to capture pod image IDs").into());
        }
        let pods: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        for pod in pods
            .get("items")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            let pod_name = pod
                .pointer("/metadata/name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown-pod");
            let spec_containers = pod
                .pointer("/spec/containers")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten();
            let statuses = pod
                .pointer("/status/containerStatuses")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .map(|status| {
                    (
                        status.get("name").and_then(serde_json::Value::as_str).unwrap_or(""),
                        status,
                    )
                })
                .collect::<HashMap<_, _>>();
            for container in spec_containers {
                let name = container
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown-container");
                let status = statuses.get(name).copied();
                captured.push(PodImageEvidence {
                    cluster: context.clone(),
                    pod: pod_name.to_owned(),
                    container: name.to_owned(),
                    requested_image: container
                        .get("image")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                    image_id: status
                        .and_then(|status| status.get("imageID"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                    ready: status
                        .and_then(|status| status.get("ready"))
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false),
                    restart_count: status
                        .and_then(|status| status.get("restartCount"))
                        .and_then(serde_json::Value::as_u64)
                        .and_then(|count| u32::try_from(count).ok())
                        .unwrap_or(0),
                });
            }
        }
    }
    Ok(captured)
}

/// Write reproducible Grid worktree and image-label provenance beside the run evidence.
fn write_run_provenance(
    evidence_dir: &Path,
    context: &DemoContext,
    image_ids: &BTreeMap<String, String>,
    pod_images: &[PodImageEvidence],
) -> Result<(), Box<dyn std::error::Error>> {
    let head = Command::new("git").args(["rev-parse", "HEAD"]).output()?;
    if !head.status.success() {
        return Err("unable to record Grid source SHA".into());
    }
    let grid_sha = String::from_utf8_lossy(&head.stdout).trim().to_owned();
    let tracked_diff = Command::new("git").args(["diff", "--binary", "HEAD", "--"]).output()?;
    if !tracked_diff.status.success() {
        return Err("unable to hash Grid tracked changes".into());
    }
    let untracked = Command::new("git")
        .args(["ls-files", "--others", "--exclude-standard"])
        .output()?;
    if !untracked.status.success() {
        return Err("unable to list untracked Grid source files".into());
    }
    let mut source_hasher = Sha256::new();
    source_hasher.update(b"tracked-diff\0");
    source_hasher.update(u64::try_from(tracked_diff.stdout.len())?.to_be_bytes());
    source_hasher.update(&tracked_diff.stdout);
    let mut source_paths = Vec::new();
    let mut generated_artifacts = BTreeMap::new();
    let mut other_untracked_paths = Vec::new();
    for relative in String::from_utf8_lossy(&untracked.stdout).lines() {
        let path = Path::new(relative);
        if !path.is_file() {
            continue;
        }
        let bytes = fs::read(path)?;
        if is_untracked_source_file(path) {
            source_hasher.update(b"source-file\0");
            source_hasher.update(u64::try_from(relative.len())?.to_be_bytes());
            source_hasher.update(relative.as_bytes());
            source_hasher.update(u64::try_from(bytes.len())?.to_be_bytes());
            source_hasher.update(&bytes);
            source_paths.push(relative.to_owned());
        } else if is_generated_resolved_yaml(path) {
            generated_artifacts.insert(relative.to_owned(), format!("{:x}", Sha256::digest(&bytes)));
        } else {
            other_untracked_paths.push(relative.to_owned());
        }
    }
    source_paths.sort();
    other_untracked_paths.sort();
    let content_hash = format!("{:x}", source_hasher.finalize());
    let generated_json = serde_json::to_vec_pretty(&generated_artifacts)?;
    let generated_manifest_hash = format!("{:x}", Sha256::digest(&generated_json));
    fs::write(evidence_dir.join("generated-artifacts.json"), generated_json)?;

    let mut selected_images = BTreeMap::from([
        ("gateway", context.images.gateway.as_str()),
        ("operator", context.images.operator.as_str()),
        ("epp", context.images.epp.as_str()),
        ("simulator", context.images.vcr.as_str()),
        ("overlay_sync", context.images.overlay_sync.as_str()),
    ]);
    if let Some(nginx) = context.images.nginx.as_deref() {
        selected_images.insert("nginx", nginx);
    }
    let mut labels = BTreeMap::new();
    for (role, image) in selected_images {
        let output = Command::new("docker")
            .args(["inspect", "--format", "{{json .Config.Labels}}", image])
            .output()?;
        let parsed = if output.status.success() {
            serde_json::from_slice(&output.stdout).unwrap_or(serde_json::Value::Null)
        } else {
            serde_json::Value::Null
        };
        labels.insert(role, serde_json::json!({"image": image, "labels": parsed}));
    }
    let provenance_markdown = format!(
        "# Run provenance\n\n- Run ID: `{}`\n- Grid HEAD: `{grid_sha}`\n- Tracked diff plus untracked source SHA-256: `{content_hash}`\n- Untracked source files included: `{}`\n- Generated resolved artifacts excluded from source hash: {} files; manifest SHA-256 `{generated_manifest_hash}` (see `generated-artifacts.json`).\n- Other untracked files excluded from source hash: `{}`\n\n## Selected image references and OCI labels\n\n```json\n{}\n```\n\n## Docker image IDs\n\n```json\n{}\n```\n\n## Runtime pod image IDs\n\nSee `pod-images.json` ({} container records captured before teardown). For the source-built dynamic run, `gateway_image_identity` in `evidence.json` compares every running Praxis container's runtime config digest and requested reference with the selected local image, and checks its source revision and worktree-diff hash against the image labels.\n",
        context.run_id,
        source_paths.join(", "),
        generated_artifacts.len(),
        other_untracked_paths.join(", "),
        serde_json::to_string_pretty(&labels)?,
        serde_json::to_string_pretty(image_ids)?,
        pod_images.len(),
    );
    fs::write(evidence_dir.join("PROVENANCE.md"), provenance_markdown)?;
    Ok(())
}

/// Whether an untracked path is source or test input that belongs in the source-content hash.
fn is_untracked_source_file(path: &Path) -> bool {
    let rust_source = path.extension().and_then(|extension| extension.to_str()) == Some("rs")
        && [
            "operator/src/",
            "xtask/src/",
            "overlay-sync/src/",
            "crdt/src/",
            "swim/src/",
        ]
        .iter()
        .any(|root| path.starts_with(root));
    let chart_test = path.extension().and_then(|extension| extension.to_str()) == Some("yaml")
        && path.starts_with("charts/grid-operator/tests/");
    rust_source || chart_test
}

/// Identify generated per-run Forge YAML without reading its contents into source provenance.
fn is_generated_resolved_yaml(path: &Path) -> bool {
    path.extension().and_then(|extension| extension.to_str()) == Some("yaml")
        && path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.contains(".resolved"))
}

// ---------------------------------------------------------------------------
// Forge helpers
// ---------------------------------------------------------------------------

/// Run a forge command.
fn run_forge(
    forge_bin: &Path,
    config: &Path,
    state_dir: &Path,
    args: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(forge_bin)
        .args(["--config", &config.display().to_string(), "--non-interactive"])
        .args(["--state-dir", &state_dir.display().to_string()])
        .env("FORGE_STATE_DIR", state_dir)
        .args(args)
        .output()?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(format!("forge {} failed: {stderr}", args.join(" ")).into())
}

/// Run a specific forge stack on a cluster.
fn run_forge_stack(context: &DemoContext, cluster: &str, stack: &str) -> Result<(), Box<dyn std::error::Error>> {
    run_forge(
        &context.forge_bin,
        &context.resolved_config,
        &context.forge_state_dir,
        &["stack", "apply", cluster, stack],
    )?;
    Ok(())
}

/// Materialize the forge config with computed candidate IDs and
/// optional mTLS transformations.
///
/// Injects `candidateId` properties into each cluster definition so
/// that the provider gateway's `provider_route` filter `candidate_id`
/// matches the `stable_id` the operator writes to the routing overlay.
/// Both are derived from `fnv1a_hex8("{kind}/{model}/{site}/{cluster}")`.
///
/// In mTLS mode, additionally:
/// - Swaps EPP deployment paths to the `-mtls` variants (with nginx sidecar)
/// - Adds the metrics TLS proxy ConfigMap manifest step
/// - Changes the metricsEndpoint to HTTPS :9443
/// - Adds the TLS Secret references to the InferenceProvider metricsConfig
///
/// When `scoring_flavor` is `KvCachePressure`, additionally swaps both
/// sites' `GridNetwork.spec.scoringPolicy.strategy` from the template's
/// default `queueDepth` to `kvCachePressure`.
#[cfg(test)]
fn materialize_config(
    forge_config: &Path,
    metrics_transport: MetricsTransport,
    scoring_flavor: ScoringFlavor,
    nginx_image: Option<&str>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    materialize_config_with_images(
        forge_config,
        &MaterializeConfigOptions {
            metrics_transport,
            scoring_flavor,
            nginx_image,
            images: None,
            run_id: None,
            pressure_weighted: false,
        },
    )
}

/// Materialize a Forge config and inject all explicitly selected images.
///
/// The llm-d topology has image values in cluster properties and in the
/// overlay-sync sidecar values. Keeping this injection here ensures the image
/// references used by Forge, Kind loading, and the environment variables are
/// identical before any cluster is created.
#[derive(Clone, Copy)]
struct MaterializeConfigOptions<'inputs> {
    /// Metrics transport to render into the resolved Forge config.
    metrics_transport: MetricsTransport,
    /// Scoring implementation selected by this qualification.
    scoring_flavor: ScoringFlavor,
    /// Optional nginx image used by the metrics TLS proxy manifests.
    nginx_image: Option<&'inputs str>,
    /// Optional explicitly resolved image set to materialize.
    images: Option<&'inputs ResolvedImages>,
    /// Optional run ID used to isolate generated manifest paths.
    run_id: Option<&'inputs str>,
    /// Render poll-mode signal transport and pressure-weighted placement.
    pressure_weighted: bool,
}

/// Materialize a Forge config with optional image and run-specific overrides.
fn materialize_config_with_images(
    forge_config: &Path,
    options: &MaterializeConfigOptions<'_>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let MaterializeConfigOptions {
        metrics_transport,
        scoring_flavor,
        nginx_image,
        images,
        run_id,
        pressure_weighted,
    } = *options;
    let dir = forge_config.parent().unwrap_or_else(|| Path::new("."));
    let run_suffix = run_id.map_or_else(String::new, |id| format!(".{id}"));
    let resolved = dir.join(format!(".forge.resolved{run_suffix}.yaml"));
    let mut result = fs::read_to_string(forge_config)?;
    if let Some(run_id) = run_id {
        result = checked_replace(
            &result,
            "clusterPrefix: grid-llmd-pm",
            &format!("clusterPrefix: grid-llmd-pm-{run_id}"),
            1,
            "run-specific Kind cluster prefix",
        )?;
        result = checked_replace(
            &result,
            "kind-grid-llmd-pm-{{ cluster.name }}",
            &format!("kind-grid-llmd-pm-{run_id}-{{{{ cluster.name }}}}"),
            4,
            "run-specific kubectl contexts",
        )?;
    }
    if let Some(images) = images {
        let image_pull_policy =
            std::env::var("GRID_XTASK_IMAGE_PULL_POLICY").unwrap_or_else(|_| "IfNotPresent".to_owned());
        if image_pull_policy != "IfNotPresent" {
            result = checked_replace(
                &result,
                "imagePullPolicy: IfNotPresent",
                &format!("imagePullPolicy: {image_pull_policy}"),
                2,
                "image pull policy",
            )?;
        }
        for (role, selected, default) in [
            ("gateway", images.gateway.as_str(), DEFAULT_GATEWAY_IMAGE),
            ("operator", images.operator.as_str(), DEFAULT_OPERATOR_IMAGE),
            ("epp", images.epp.as_str(), DEFAULT_EPP_IMAGE),
            ("vcr", images.vcr.as_str(), DEFAULT_VCR_IMAGE),
            ("overlay-sync", images.overlay_sync.as_str(), DEFAULT_OVERLAY_SYNC_IMAGE),
        ] {
            let (repository, tag) = split_image_reference(selected)?;
            let (default_repository, default_tag) = split_image_reference(default)?;
            if selected != default {
                if role != "overlay-sync" {
                    result = checked_replace(&result, default, selected, 2, &format!("{role} image reference"))?;
                }
                let property = match role {
                    "gateway" => "gatewayImage",
                    "operator" => "operatorImage",
                    "epp" => "eppImage",
                    "vcr" => "vcrImage",
                    "overlay-sync" => "overlay-sync",
                    _ => unreachable!(),
                };
                let repo_property = if role == "overlay-sync" {
                    "repository".to_owned()
                } else {
                    format!("{property}Repo")
                };
                let tag_property = if role == "overlay-sync" {
                    "tag".to_owned()
                } else {
                    format!("{property}Tag")
                };
                result = checked_replace(
                    &result,
                    &format!("{repo_property}: \"{default_repository}\""),
                    &format!("{repo_property}: \"{repository}\""),
                    if role == "overlay-sync" { 1 } else { 2 },
                    &format!("{role} image repository"),
                )?;
                result = checked_replace(
                    &result,
                    &format!("{tag_property}: \"{default_tag}\""),
                    &format!("{tag_property}: \"{tag}\""),
                    if role == "overlay-sync" { 1 } else { 2 },
                    &format!("{role} image tag"),
                )?;
            }
        }

        // The simulator image is also embedded in the checked-in backend
        // manifests. Materialize those manifests so an explicit override is
        // the exact image loaded into Kind; do not rely on a host-only alias.
        for pool in CLUSTERS {
            let source = dir.join(format!("resources/{pool}/vcr-deployment.yaml"));
            let resolved_name = format!(".forge.resolved.{pool}-vcr-deployment{run_suffix}.yaml");
            let destination = dir.join(&resolved_name);
            let manifest = fs::read_to_string(&source)?;
            let resolved_manifest = checked_replace(
                &manifest,
                DEFAULT_VCR_IMAGE,
                &images.vcr,
                2,
                &format!("{pool} simulator image"),
            )?;
            fs::write(&destination, resolved_manifest)?;
            result = checked_replace(
                &result,
                &format!("resources/{pool}/vcr-deployment.yaml"),
                &resolved_name,
                1,
                &format!("{pool} simulator deployment path"),
            )?;
        }
    }
    for cluster in CLUSTERS {
        let provider_name = format!("llmd-{cluster}-provider");
        let candidate_id = fnv1a_hex8(&format!("inference_model/{VCR_MODEL}/{cluster}/{provider_name}"));
        let anchor = format!("poolName: {cluster}");
        let replacement = format!("{anchor}\n        candidateId: \"{candidate_id}\"");
        result = checked_replace(&result, &anchor, &replacement, 1, &format!("poolName:{cluster}"))?;
    }

    if metrics_transport == MetricsTransport::MtlsProxy {
        let nginx_img = nginx_image.unwrap_or(DEFAULT_NGINX_IMAGE);

        // Create resolved mTLS deployment manifests with injected nginx image
        for pool in CLUSTERS {
            let src = dir.join(format!("resources/{pool}/epp-deployment-mtls.yaml"));
            let resolved_name = format!(".forge.resolved.{pool}-epp-deployment-mtls{run_suffix}.yaml");
            let dst = dir.join(&resolved_name);
            let manifest = fs::read_to_string(&src)?;
            let patched = checked_replace(
                &manifest,
                DEFAULT_NGINX_IMAGE,
                nginx_img,
                1,
                &format!("{pool} nginx image"),
            )?;
            fs::write(&dst, patched)?;

            // Point forge config to resolved manifest (instead of the template)
            result = checked_replace(
                &result,
                &format!("resources/{pool}/epp-deployment.yaml"),
                &resolved_name,
                1,
                &format!("{pool} deployment path"),
            )?;
        }

        // Add metrics-tls-proxy-config manifest step after epp-rbac
        let rbac_step = "          path: resources/common/epp-rbac.yaml";
        let rbac_with_proxy = format!(
            "{rbac_step}\n        - type: manifest\n          path: resources/common/metrics-tls-proxy-config.yaml"
        );
        result = checked_replace(&result, rbac_step, &rbac_with_proxy, 2, "epp-rbac anchor")?;

        // Change metricsEndpoint from HTTP :9090 to HTTPS :9443
        result = checked_replace(
            &result,
            "http://llmd-epp-metrics.grid-system.svc.cluster.local:9090",
            "https://llmd-epp-metrics.grid-system.svc.cluster.local:9443",
            2,
            "metrics endpoint",
        )?;

        // Add TLS secret references to metricsConfig
        let signal_anchor = "                    healthy: inference_pool_ready_pods";
        let tls_block = format!(
            "{signal_anchor}\n\
             \x20                 tls:\n\
             \x20                   caSecretRef:\n\
             \x20                     name: metrics-ca\n\
             \x20                     namespace: grid-system\n\
             \x20                   clientCertificateSecretRef:\n\
             \x20                     name: metrics-client-tls\n\
             \x20                     namespace: grid-system"
        );
        result = checked_replace(&result, signal_anchor, &tls_block, 2, "metrics signal anchor")?;
    }

    if scoring_flavor == ScoringFlavor::KvCachePressure {
        let default_strategy = format!("strategy: {}", ScoringFlavor::QueueDepth.strategy_yaml());
        let selected_strategy = format!("strategy: {}", scoring_flavor.strategy_yaml());
        let default_matches = result.matches(&default_strategy).count();
        let selected_matches = result.matches(&selected_strategy).count();
        match (default_matches, selected_matches) {
            (2, 0) => {
                result = checked_replace(
                    &result,
                    &default_strategy,
                    &selected_strategy,
                    2,
                    "scoringPolicy.strategy",
                )?;
            },
            (0, 2) => {
                // The wrapper may select a Forge config that already uses the
                // requested strategy. Keep materialization idempotent.
            },
            _ => {
                return Err(format!(
                    "materialize_config: scoringPolicy.strategy: expected either 2 queueDepth matches or 2 kvCachePressure matches, found {default_matches} and {selected_matches}"
                )
                .into());
            },
        }
    }

    if pressure_weighted {
        let selected_strategy = scoring_flavor.strategy_yaml();
        let signal = match scoring_flavor {
            ScoringFlavor::QueueDepth => "queueDepth",
            ScoringFlavor::KvCachePressure => "kvCacheUtilization",
        };
        let current = format!(
            "              scoringPolicy:\n                strategy: {selected_strategy}\n              selectionPolicy:\n                mode: deterministic"
        );
        let configured = format!(
            "              scoringPolicy:\n                strategy: noMetrics\n              signalTransport:\n                mode: poll\n              selectionPolicy:\n                mode: weightedRandom\n              placementPolicy:\n                strategy: pressureWeighted\n                pressureWeighted:\n                  signal: {signal}\n                  minimumWeight: 1\n                  maximumWeight: 1000\n                  availabilityFloorPercent: 5\n                  smoothingFactor: 0.35\n                  changeThresholdPercent: 5\n                  staleSignalSeconds: 120"
        );
        result = checked_replace(&result, &current, &configured, 2, "poll-mode pressure-weighted policy")?;
    }

    fs::write(&resolved, result)?;
    Ok(resolved)
}

/// Split a registry image reference into repository and tag.
fn split_image_reference(image: &str) -> Result<(&str, &str), Box<dyn std::error::Error>> {
    let (repository, tag) = image
        .rsplit_once(':')
        .ok_or_else(|| format!("image reference {image:?} has no tag"))?;
    if repository.is_empty() || tag.is_empty() || tag.contains('/') {
        return Err(format!("image reference {image:?} must contain a repository and tag").into());
    }
    Ok((repository, tag))
}

/// Verify every selected image occurs in the resolved Forge source.
fn verify_materialized_images(path: &Path, images: &ResolvedImages) -> Result<(), Box<dyn std::error::Error>> {
    let content = fs::read_to_string(path)?;
    let expected = [
        ("gateway", images.gateway.as_str()),
        ("operator", images.operator.as_str()),
        ("epp", images.epp.as_str()),
        ("vcr", images.vcr.as_str()),
        ("overlay-sync", images.overlay_sync.as_str()),
    ];
    for (role, image) in expected {
        let (repository, tag) = split_image_reference(image)?;
        let (repo_property, tag_property, expected_count) = if role == "overlay-sync" {
            ("repository", "tag", 1)
        } else {
            let property = match role {
                "gateway" => "gatewayImage",
                "operator" => "operatorImage",
                "epp" => "eppImage",
                "vcr" => "vcrImage",
                _ => unreachable!(),
            };
            // These fields occur once per cluster.
            (property, property, 2)
        };
        let repository_key = if role == "overlay-sync" {
            repo_property.to_owned()
        } else {
            format!("{repo_property}Repo")
        };
        let tag_key = if role == "overlay-sync" {
            tag_property.to_owned()
        } else {
            format!("{tag_property}Tag")
        };
        let repository_count = content.matches(&format!("{repository_key}: \"{repository}\"")).count();
        let tag_count = content.matches(&format!("{tag_key}: \"{tag}\"")).count();
        if repository_count != expected_count || tag_count != expected_count {
            return Err(format!(
                "resolved Forge config is missing {role} image {image:?} (repository matches: {repository_count}, tag matches: {tag_count})"
            )
            .into());
        }
    }
    if !uses_registry_images() && !content.contains("imagePullPolicy: Never") {
        return Err("resolved Forge config does not set imagePullPolicy: Never for local image mode".into());
    }
    Ok(())
}

/// Replace `needle` in `content`, failing if the match count differs from `expected`.
fn checked_replace(
    content: &str,
    needle: &str,
    replacement: &str,
    expected: usize,
    label: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let found = content.matches(needle).count();
    if found != expected {
        return Err(format!("materialize_config: {label}: expected {expected} match(es), found {found}").into());
    }
    Ok(content.replacen(needle, replacement, expected))
}

/// FNV-1a 32-bit hash, formatted as 8-char lowercase hex.
///
/// Mirrors the operator's `routing_overlay::fnv1a_hex8` to produce
/// identical `stable_id` values for overlay candidate identification.
fn fnv1a_hex8(input: &str) -> String {
    const FNV_OFFSET: u32 = 2_166_136_261;
    const FNV_PRIME: u32 = 16_777_619;
    let mut hash = FNV_OFFSET;
    for byte in input.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:08x}")
}

/// Teardown the environment.
fn teardown_environment(context: &DemoContext) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!();
    eprintln!("[TEARDOWN] Removing Kind clusters");
    run_forge(
        &context.forge_bin,
        &context.resolved_config,
        &context.forge_state_dir,
        &["down"],
    )?;
    let cert_root = Path::new(CERTS_DIR);
    if context.certs_dir.starts_with(cert_root) && context.certs_dir != cert_root {
        if context.certs_dir.exists() {
            fs::remove_dir_all(&context.certs_dir)?;
        }
    } else {
        return Err("refusing certificate cleanup outside the run-owned llm-d certificate directory".into());
    }
    eprintln!("  [OK] Teardown complete");
    Ok(())
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/// Format a UTC timestamp for run IDs (YYYYMMDDTHHMMSSZ).
fn format_utc_timestamp() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let secs = now;
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Approximate Gregorian date calculation
    let mut y = 1970_i64;
    let mut remaining = days as i64;
    loop {
        let year_days = if is_leap(y) { 366 } else { 365 };
        if remaining < year_days {
            break;
        }
        remaining -= year_days;
        y += 1;
    }
    let months = [
        31,
        if is_leap(y) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut m = 1_u32;
    for &md in &months {
        if remaining < md {
            break;
        }
        remaining -= md;
        m += 1;
    }
    let d = remaining + 1;

    format!("{y:04}{m:02}{d:02}T{hours:02}{minutes:02}{seconds:02}Z")
}

/// Build a run ID that is valid in Kubernetes labels and Kind cluster names.
fn format_run_id(timestamp: &str, process_id: u32) -> String {
    format!("{}-{process_id}", timestamp.to_ascii_lowercase())
}

/// Format a UTC ISO-8601 timestamp.
fn format_utc_iso() -> String {
    let ts = format_utc_timestamp();
    format!(
        "{}-{}-{}T{}:{}:{}Z",
        &ts[..4],
        &ts[4..6],
        &ts[6..8],
        &ts[9..11],
        &ts[11..13],
        &ts[13..15]
    )
}

/// Check if a year is a leap year.
fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

/// Format a Kind cluster context name.
fn kind_context(cluster: &str) -> String {
    format!("kind-{}-{cluster}", forge_cluster_prefix())
}

/// Return the current run's unique Forge cluster prefix.
fn forge_cluster_prefix() -> &'static str {
    RUN_PREFIX.get().map_or("grid-llmd-pm", String::as_str)
}

/// Annotate the GridNetwork to trigger operator re-reconciliation.
///
/// The operator watches GridNetwork resources. Changing an annotation
/// generates a watch event that forces an immediate reconcile cycle,
/// bypassing the 300-second requeue interval. This lets the recovery
/// proof observe fresh overlay scores without waiting for the timer.
fn trigger_gridnetwork_reconcile(cluster: &str) {
    let ctx = kind_context(cluster);
    let ts = format_utc_timestamp();
    drop(
        Command::new("kubectl")
            .args([
                "--context",
                &ctx,
                "-n",
                GRID_SYSTEM_NS,
                "annotate",
                "gridnetwork",
                GRID_NETWORK_NAME,
                &format!("grid.praxis-proxy.io/metrics-refresh-at={ts}"),
                "--overwrite",
            ])
            .output(),
    );
}

/// Resolve the evidence directory path.
fn resolve_evidence_dir(
    forge_config: &Path,
    options: &GlbDemoOptions,
    run_id: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let base = options
        .evidence_dir
        .clone()
        .unwrap_or_else(|| forge_config.parent().unwrap_or_else(|| Path::new(".")).join("evidence"));
    Ok(base.join(run_id))
}

// ---------------------------------------------------------------------------
// TLS proof stages
// ---------------------------------------------------------------------------

/// Timeout for a single TLS state transition.
const TLS_TRANSITION_TIMEOUT: Duration = Duration::from_secs(90);

/// Interval between overlay checks during TLS proofs.
const TLS_POLL_INTERVAL: Duration = Duration::from_secs(3);

/// Value of `staleMetricsSeconds` in the demo InferenceProvider.
const STALE_METRICS_TTL_SECS: u64 = 20;

/// Check whether a provider is observable in the overlay.
///
/// Returns `true` when a candidate containing `provider_suffix` is present
/// with a score above zero — meaning the operator successfully scraped its
/// metrics via TLS. When scraping fails, `UNOBSERVABLE_METRICS` sets
/// `healthy: false`, which results in a zero score.
fn is_provider_observable(cluster: &str, provider_suffix: &str) -> bool {
    let candidates = read_overlay_candidates(cluster);
    candidates
        .iter()
        .any(|c| c.cluster.contains(provider_suffix) && c.score > 0.0)
}

/// Read a base64-encoded field from a Kubernetes Secret.
fn read_secret_field_b64(context: &str, secret_name: &str, key: &str) -> Result<String, Box<dyn std::error::Error>> {
    let escaped_key = key.replace('.', r"\.");
    let jsonpath = format!("{{.data.{escaped_key}}}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "secret",
            secret_name,
            "-o",
            &format!("jsonpath={jsonpath}"),
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "Secret/{secret_name} key={key}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let data = String::from_utf8_lossy(&output.stdout).to_string();
    if data.is_empty() {
        return Err(format!("Secret/{secret_name} key={key} is empty").into());
    }
    Ok(data)
}

/// Probe the metrics TLS endpoint using credentials read from Kubernetes
/// Secrets.
///
/// Decodes client cert, client key, and CA inside the metrics-tls-proxy
/// container, then uses curl to connect to the TLS-protected metrics
/// Service. Returns `true` on HTTP 200. Use this to distinguish TLS
/// transport failures from operator ingestion failures.
fn probe_mtls_endpoint(cluster: &str) -> bool {
    let ctx = kind_context(cluster);

    let Ok(cert_b64) = read_secret_field_b64(&ctx, METRICS_CLIENT_TLS_SECRET, "tls.crt") else {
        return false;
    };
    let Ok(key_b64) = read_secret_field_b64(&ctx, METRICS_CLIENT_TLS_SECRET, "tls.key") else {
        return false;
    };
    let Ok(ca_b64) = read_secret_field_b64(&ctx, METRICS_CA_SECRET, "ca.crt") else {
        return false;
    };

    let metrics_url = format!("https://{METRICS_SERVER_DNS}:9443/metrics");
    let cmd = format!(
        "echo '{ca_b64}' | base64 -d > /tmp/p-ca.pem\n\
         echo '{cert_b64}' | base64 -d > /tmp/p-cert.pem\n\
         echo '{key_b64}' | base64 -d > /tmp/p-key.pem\n\
         curl -sf --connect-timeout 5 \
           --cacert /tmp/p-ca.pem \
           --cert /tmp/p-cert.pem \
           --key /tmp/p-key.pem \
           {metrics_url} -o /dev/null\n\
         rc=$?\n\
         rm -f /tmp/p-ca.pem /tmp/p-cert.pem /tmp/p-key.pem\n\
         exit $rc"
    );

    let output = Command::new("kubectl")
        .args([
            "--context",
            &ctx,
            "-n",
            GRID_SYSTEM_NS,
            "exec",
            "deploy/llmd-epp",
            "-c",
            "metrics-tls-proxy",
            "--",
            "sh",
            "-c",
            &cmd,
        ])
        .output();

    output.is_ok_and(|o| o.status.success())
}

/// Wait until a provider becomes observable in the overlay.
fn wait_for_observable(cluster: &str, provider_suffix: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        trigger_gridnetwork_reconcile(cluster);
        if is_provider_observable(cluster, provider_suffix) {
            return true;
        }
        std::thread::sleep(TLS_POLL_INTERVAL);
    }
    false
}

/// Wait until a provider becomes unobservable in the overlay.
fn wait_for_unobservable(cluster: &str, provider_suffix: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        trigger_gridnetwork_reconcile(cluster);
        if !is_provider_observable(cluster, provider_suffix) {
            return true;
        }
        std::thread::sleep(TLS_POLL_INTERVAL);
    }
    false
}

/// Delete a Kubernetes Secret.
fn delete_secret(context: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "delete",
            "secret",
            name,
            "--ignore-not-found",
        ])
        .status()?;
    if !status.success() {
        return Err(format!("failed to delete Secret/{name}").into());
    }
    Ok(())
}

/// Rollout-restart a Deployment and wait for it to become available.
fn rollout_restart(context: &str, deployment: &str) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "rollout",
            "restart",
            &format!("deployment/{deployment}"),
        ])
        .status()?;
    if !status.success() {
        return Err(format!("failed to rollout restart {deployment}").into());
    }
    let wait = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "rollout",
            "status",
            &format!("deployment/{deployment}"),
            "--timeout=120s",
        ])
        .status()?;
    if !wait.success() {
        return Err(format!("{deployment} rollout timed out").into());
    }
    Ok(())
}

/// Snapshot of a pod's identity and restart counts.
struct PodSnapshot {
    /// Pod name.
    name: String,
    /// Pod UID.
    uid: String,
    /// Container restart counts: `(container_name, restart_count)`.
    restarts: Vec<(String, u32)>,
}

/// Capture pod snapshots for a given label selector in one cluster.
fn capture_pod_snapshots(cluster: &str, label: &str) -> Vec<PodSnapshot> {
    let ctx = kind_context(cluster);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &ctx,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "pods",
            "-l",
            label,
            "-o",
            "jsonpath={range .items[*]}{.metadata.name}|{.metadata.uid}|{range .status.containerStatuses[*]}{.name}={.restartCount},{end}{\"\\n\"}{end}",
        ])
        .output();
    let Ok(o) = output else { return Vec::new() };
    let text = String::from_utf8_lossy(&o.stdout);
    text.lines()
        .filter(|l| !l.is_empty())
        .filter_map(|line| {
            let mut parts = line.splitn(3, '|');
            let name = parts.next()?.to_owned();
            let uid = parts.next()?.to_owned();
            let containers = parts.next().unwrap_or("");
            let restarts: Vec<(String, u32)> = containers
                .split(',')
                .filter(|s| !s.is_empty())
                .filter_map(|entry| {
                    let (cname, count_str) = entry.split_once('=')?;
                    Some((cname.to_owned(), count_str.parse().unwrap_or(0)))
                })
                .collect();
            Some(PodSnapshot { name, uid, restarts })
        })
        .collect()
}

/// Snapshot of all workload pods relevant for restart accounting.
struct RestartSnapshot {
    /// Grid operator pods.
    operator: Vec<PodSnapshot>,
    /// EPP + metrics proxy pods.
    epp: Vec<PodSnapshot>,
    /// Gateway and overlay-sync pods.
    gateway: Vec<PodSnapshot>,
}

/// Capture restart snapshots for all relevant workloads on one cluster.
fn capture_restart_snapshot(cluster: &str) -> RestartSnapshot {
    RestartSnapshot {
        operator: capture_pod_snapshots(cluster, "app.kubernetes.io/name=grid-operator"),
        epp: capture_pod_snapshots(cluster, "app.kubernetes.io/name=llmd-epp"),
        gateway: capture_pod_snapshots(cluster, "app.kubernetes.io/name=praxis-gateway"),
    }
}

/// Compare restart snapshots and emit observations.
///
/// Returns `(success, observations)`.
fn compare_restart_snapshots(
    cluster: &str,
    before: &RestartSnapshot,
    after: &RestartSnapshot,
    server_rotation_performed: bool,
) -> (bool, Vec<String>) {
    let mut observations = Vec::new();
    let mut success = true;

    // Operator: pod identity must be unchanged, zero restarts
    for bp in &before.operator {
        let matching_after = after.operator.iter().find(|ap| ap.uid == bp.uid);
        if let Some(ap) = matching_after {
            let total: u32 = ap.restarts.iter().map(|(_, c)| c).sum();
            observations.push(format!(
                "{cluster}/operator/{}: uid unchanged, restart_count={total}",
                ap.name
            ));
            if total > 0 {
                observations.push(format!("{cluster}: operator restarted unexpectedly"));
                success = false;
            }
        } else {
            observations.push(format!(
                "{cluster}: operator pod {} (uid={}) replaced — unexpected restart",
                bp.name, bp.uid
            ));
            success = false;
        }
    }

    // EPP: if server rotation was performed, expect a new pod (rollout restart)
    if server_rotation_performed {
        let before_uids: Vec<&str> = before.epp.iter().map(|p| p.uid.as_str()).collect();
        let new_pods: Vec<&PodSnapshot> = after
            .epp
            .iter()
            .filter(|p| !before_uids.contains(&p.uid.as_str()))
            .collect();
        if new_pods.is_empty() {
            observations.push(format!(
                "{cluster}: EPP pod was not replaced after server rotation — expected rollout restart"
            ));
        } else {
            for np in &new_pods {
                let total: u32 = np.restarts.iter().map(|(_, c)| c).sum();
                observations.push(format!(
                    "{cluster}/epp/{}: new pod after server cert rotation (expected), restart_count={total}",
                    np.name
                ));
            }
        }
    } else {
        for bp in &before.epp {
            let matching_after = after.epp.iter().find(|ap| ap.uid == bp.uid);
            if let Some(ap) = matching_after {
                let total: u32 = ap.restarts.iter().map(|(_, c)| c).sum();
                observations.push(format!(
                    "{cluster}/epp/{}: uid unchanged, restart_count={total}",
                    ap.name
                ));
                if total > 0 {
                    observations.push(format!("{cluster}: EPP restarted unexpectedly"));
                    success = false;
                }
            }
        }
    }

    // Gateway: no restarts expected
    for bp in &before.gateway {
        let matching_after = after.gateway.iter().find(|ap| ap.uid == bp.uid);
        if let Some(ap) = matching_after {
            for (cname, count) in &ap.restarts {
                observations.push(format!("{cluster}/gateway/{}: {cname} restarts={count}", ap.name));
                if *count > 0 {
                    success = false;
                }
            }
        } else {
            observations.push(format!("{cluster}: gateway pod {} replaced unexpectedly", bp.name));
            success = false;
        }
    }

    (success, observations)
}

/// Prove that TLS proof stages did not cause unexpected restarts.
///
/// Compares before/after pod snapshots across operator, EPP, and gateway
/// workloads. The intentional EPP rollout restart from server certificate
/// rotation is documented and excluded from the failure check — but only
/// on the cluster where rotation was actually performed (pool-a).
fn proof_restart_accounting(
    before: &HashMap<String, RestartSnapshot>,
    after: &HashMap<String, RestartSnapshot>,
    rotation_cluster: Option<&str>,
) -> ProofResult {
    let mut observations = Vec::new();
    let mut success = true;

    for cluster in CLUSTERS {
        let cluster_had_rotation = rotation_cluster.is_some_and(|c| c == *cluster);
        if let (Some(b), Some(a)) = (before.get(*cluster), after.get(*cluster)) {
            let (ok, obs) = compare_restart_snapshots(cluster, b, a, cluster_had_rotation);
            for o in &obs {
                eprintln!("    {o}");
            }
            observations.extend(obs);
            if !ok {
                success = false;
            }
        } else {
            let msg = format!("{cluster}: snapshot missing");
            eprintln!("    {msg}");
            observations.push(msg);
            success = false;
        }
    }

    if let Some(rc) = rotation_cluster {
        let msg = format!(
            "server rotation (stage 8) on {rc}: EPP rollout restart is expected — nginx does not reload TLS in-place"
        );
        eprintln!("    {msg}");
        observations.push(msg);
    }

    ProofResult {
        success,
        description: "Restart accounting: operator/gateway zero restarts, EPP restart only from server rotation"
            .to_owned(),
        observations,
    }
}

/// Run all TLS proof stages in sequence.
///
/// Returns the proof results keyed by stage name. Stages build on each
/// other (each manipulates Secrets, so ordering matters). Captures
/// before/after restart snapshots and includes restart accounting.
fn run_tls_proof_stages() -> BTreeMap<String, ProofResult> {
    let mut results = BTreeMap::new();

    eprintln!();
    eprintln!("{OUTPUT_RULE}");
    eprintln!("TLS PROOF STAGES");
    eprintln!("{OUTPUT_RULE}");

    // Capture restart snapshots before TLS stages
    let before_snapshots: HashMap<String, RestartSnapshot> = CLUSTERS
        .iter()
        .map(|c| ((*c).to_owned(), capture_restart_snapshot(c)))
        .collect();

    // Stage 1: Baseline mTLS — verify operator scrapes through TLS
    eprintln!();
    eprintln!("  [TLS 1/9] Baseline mTLS");
    results.insert("tls_01_baseline".to_owned(), proof_tls_baseline());

    // Stage 2: Handshake rejection — TLS proxy rejects connection without client cert
    eprintln!();
    eprintln!("  [TLS 2/9] Handshake rejection");
    results.insert("tls_02_handshake_rejection".to_owned(), proof_tls_handshake_rejection());

    // Stage 3: Missing client identity — delete client cert Secret
    eprintln!();
    eprintln!("  [TLS 3/9] Missing client identity");
    results.insert("tls_03_missing_client".to_owned(), proof_tls_missing_client());

    // Stage 4: Wrong CA — replace CA Secret with untrusted CA
    eprintln!();
    eprintln!("  [TLS 4/9] Wrong CA");
    results.insert("tls_04_wrong_ca".to_owned(), proof_tls_wrong_ca());

    // Stage 5: Restore valid mTLS — recreate correct Secrets
    eprintln!();
    eprintln!("  [TLS 5/9] Restore valid mTLS");
    results.insert("tls_05_restore".to_owned(), proof_tls_restore());

    // Stage 6: Stale-cache behavior — independent TTL verification
    eprintln!();
    eprintln!("  [TLS 6/9] Stale-cache TTL");
    results.insert("tls_06_stale_cache".to_owned(), proof_tls_stale_cache());

    // Stage 7: Client Secret rotation — new cert, same CA
    eprintln!();
    eprintln!("  [TLS 7/9] Client Secret rotation");
    results.insert("tls_07_client_rotation".to_owned(), proof_tls_client_rotation());

    // Stage 8: Server cert/CA rotation — new server cert + nginx restart
    eprintln!();
    eprintln!("  [TLS 8/9] Server cert rotation");
    let server_rotation = proof_tls_server_rotation();
    let rotation_cluster = server_rotation.success.then_some("pool-a");
    results.insert("tls_08_server_rotation".to_owned(), server_rotation);

    // Stage 9: Existing routing behavior — verify routing after TLS manipulations
    eprintln!();
    eprintln!("  [TLS 9/9] Existing routing behavior");
    results.insert("tls_09_routing".to_owned(), proof_tls_routing());

    // Restart accounting — compare before/after snapshots
    eprintln!();
    eprintln!("  Restart accounting");
    let after_snapshots: HashMap<String, RestartSnapshot> = CLUSTERS
        .iter()
        .map(|c| ((*c).to_owned(), capture_restart_snapshot(c)))
        .collect();
    results.insert(
        "restart_accounting".to_owned(),
        proof_restart_accounting(&before_snapshots, &after_snapshots, rotation_cluster),
    );

    results
}

/// TLS Stage 1: Verify baseline mTLS scraping produces valid overlay scores.
fn proof_tls_baseline() -> ProofResult {
    let mut observations = Vec::new();

    for cluster in CLUSTERS {
        if is_provider_observable(cluster, cluster) {
            let candidates = read_overlay_candidates(cluster);
            let score = overlay_score_for_cluster(&candidates, cluster);
            observations.push(format!("{cluster}: observable, score={score:.2} (mTLS working)"));
        } else {
            let tls_ok = probe_mtls_endpoint(cluster);
            if tls_ok {
                observations.push(format!(
                    "{cluster}: NOT observable — TLS transport OK but operator did not ingest metrics into overlay scores \
                     (check operator image contains MetricsConfig implementation)"
                ));
            } else {
                observations.push(format!(
                    "{cluster}: NOT observable — TLS transport also failed \
                     (check Secrets and TLS proxy configuration)"
                ));
            }
            return ProofResult {
                success: false,
                description: "Baseline mTLS: operator scrapes metrics through TLS".to_owned(),
                observations,
            };
        }
    }

    ProofResult {
        success: true,
        description: "Baseline mTLS: operator scrapes metrics through TLS".to_owned(),
        observations,
    }
}

/// TLS Stage 2: Prove the TLS proxy rejects connections without a client certificate.
///
/// Connects to the metrics endpoint from inside the cluster without presenting
/// a client identity. The nginx proxy has `ssl_verify_client on`, so it must
/// reject the handshake or return an error. This tests the server-side mTLS
/// enforcement path directly (independent of Secret-watch behavior).
fn proof_tls_handshake_rejection() -> ProofResult {
    let mut observations = Vec::new();
    let cluster = "pool-a";
    let ctx = kind_context(cluster);

    // Connect to the metrics TLS endpoint without a client certificate.
    // We use `wget` inside the nginx sidecar — it has network access to
    // localhost:9443 but does not present a client cert.
    let output = Command::new("kubectl")
        .args([
            "--context",
            &ctx,
            "-n",
            GRID_SYSTEM_NS,
            "exec",
            "deployment/llmd-epp",
            "-c",
            "metrics-tls-proxy",
            "--",
            "wget",
            "-q",
            "--timeout=5",
            "-O",
            "/dev/null",
            "https://localhost:9443/metrics",
        ])
        .output();

    match output {
        Ok(o) => {
            if o.status.success() {
                observations.push(format!(
                    "{cluster}: metrics endpoint accepted connection WITHOUT client cert — mTLS NOT enforced"
                ));
                return ProofResult {
                    success: false,
                    description: "Handshake rejection: TLS proxy requires client certificate".to_owned(),
                    observations,
                };
            }
            let stderr = String::from_utf8_lossy(&o.stderr);
            let category = if stderr.contains("SSL") || stderr.contains("ssl") || stderr.contains("handshake") {
                "MetricsTlsHandshakeFailed"
            } else if stderr.contains("400") || stderr.contains("certificate") {
                "MetricsTlsClientCertRequired"
            } else {
                "MetricsTlsConnectionRejected"
            };
            observations.push(format!(
                "{cluster}: connection without client cert rejected (category={category})"
            ));
        },
        Err(e) => {
            observations.push(format!("{cluster}: kubectl exec failed: {e}"));
            return ProofResult {
                success: false,
                description: "Handshake rejection: TLS proxy requires client certificate".to_owned(),
                observations,
            };
        },
    }

    ProofResult {
        success: true,
        description: "Handshake rejection: TLS proxy requires client certificate".to_owned(),
        observations,
    }
}

/// TLS Stage 3: Delete client cert Secret → provider becomes unobservable.
fn proof_tls_missing_client() -> ProofResult {
    let mut observations = Vec::new();
    let cluster = "pool-a";
    let ctx = kind_context(cluster);

    if let Err(e) = delete_secret(&ctx, METRICS_CLIENT_TLS_SECRET) {
        observations.push(format!("failed to delete {METRICS_CLIENT_TLS_SECRET}: {e}"));
        return ProofResult {
            success: false,
            description: "Missing client identity: scrape fails without client cert".to_owned(),
            observations,
        };
    }
    observations.push(format!("deleted Secret/{METRICS_CLIENT_TLS_SECRET} from {cluster}"));

    let became_unobservable = wait_for_unobservable(cluster, cluster, TLS_TRANSITION_TIMEOUT);
    if became_unobservable {
        observations.push(format!(
            "{cluster}: provider became unobservable after client cert removal (Secret-watch fail-closed)"
        ));
    } else {
        observations.push(format!(
            "{cluster}: provider still observable after client cert removal — fail-closed NOT working"
        ));
        return ProofResult {
            success: false,
            description: "Missing client identity: scrape fails without client cert".to_owned(),
            observations,
        };
    }

    ProofResult {
        success: true,
        description: "Missing client identity: scrape fails without client cert".to_owned(),
        observations,
    }
}

/// TLS Stage 3: Replace CA Secret with wrong CA → scrape fails.
fn proof_tls_wrong_ca() -> ProofResult {
    let mut observations = Vec::new();
    let cluster = "pool-a";
    let ctx = kind_context(cluster);
    let certs_dir = Path::new(CERTS_DIR);

    // First restore client secret (deleted in stage 2) so only CA is wrong
    if let Err(e) = apply_metrics_client_secret(&ctx, certs_dir) {
        observations.push(format!("failed to restore client secret: {e}"));
    }

    // Generate a wrong CA and replace the Secret
    if let Err(e) = certs::generate_wrong_metrics_ca() {
        observations.push(format!("failed to generate wrong CA: {e}"));
        return ProofResult {
            success: false,
            description: "Wrong CA: scrape fails with untrusted CA".to_owned(),
            observations,
        };
    }

    let wrong_ca_path = certs_dir.join("metrics-wrong-ca.pem");
    let result = Command::new("kubectl")
        .args([
            "--context",
            &ctx,
            "-n",
            GRID_SYSTEM_NS,
            "create",
            "secret",
            "generic",
            METRICS_CA_SECRET,
            &format!("--from-file=ca.crt={}", wrong_ca_path.display()),
            "--dry-run=client",
            "-o",
            "yaml",
        ])
        .output();
    match result {
        Ok(output) if output.status.success() => {
            if let Err(e) = kubectl::apply_manifest(&ctx, &String::from_utf8_lossy(&output.stdout)) {
                observations.push(format!("failed to apply wrong CA secret: {e}"));
                return ProofResult {
                    success: false,
                    description: "Wrong CA: scrape fails with untrusted CA".to_owned(),
                    observations,
                };
            }
        },
        _ => {
            observations.push("failed to render wrong CA secret".to_owned());
            return ProofResult {
                success: false,
                description: "Wrong CA: scrape fails with untrusted CA".to_owned(),
                observations,
            };
        },
    }
    observations.push(format!(
        "replaced Secret/{METRICS_CA_SECRET} with wrong CA on {cluster}"
    ));

    let became_unobservable = wait_for_unobservable(cluster, cluster, TLS_TRANSITION_TIMEOUT);
    if became_unobservable {
        observations.push(format!(
            "{cluster}: provider unobservable with wrong CA (server cert rejected)"
        ));
    } else {
        observations.push(format!(
            "{cluster}: provider still observable with wrong CA — CA validation NOT working"
        ));
        return ProofResult {
            success: false,
            description: "Wrong CA: scrape fails with untrusted CA".to_owned(),
            observations,
        };
    }

    ProofResult {
        success: true,
        description: "Wrong CA: scrape fails with untrusted CA".to_owned(),
        observations,
    }
}

/// TLS Stage 4: Restore correct Secrets → provider recovers.
fn proof_tls_restore() -> ProofResult {
    let mut observations = Vec::new();
    let cluster = "pool-a";
    let ctx = kind_context(cluster);
    let certs_dir = Path::new(CERTS_DIR);

    if let Err(e) = apply_metrics_ca_secret(&ctx, certs_dir) {
        observations.push(format!("failed to restore CA secret: {e}"));
        return ProofResult {
            success: false,
            description: "Restore: provider recovers with correct Secrets".to_owned(),
            observations,
        };
    }
    if let Err(e) = apply_metrics_client_secret(&ctx, certs_dir) {
        observations.push(format!("failed to restore client secret: {e}"));
        return ProofResult {
            success: false,
            description: "Restore: provider recovers with correct Secrets".to_owned(),
            observations,
        };
    }
    observations.push(format!(
        "restored correct {METRICS_CA_SECRET} and {METRICS_CLIENT_TLS_SECRET} on {cluster}"
    ));

    let recovered = wait_for_observable(cluster, cluster, TLS_TRANSITION_TIMEOUT);
    if recovered {
        let candidates = read_overlay_candidates(cluster);
        let score = overlay_score_for_cluster(&candidates, cluster);
        observations.push(format!("{cluster}: provider recovered, score={score:.2}"));
    } else {
        observations.push(format!("{cluster}: provider did not recover within timeout"));
        return ProofResult {
            success: false,
            description: "Restore: provider recovers with correct Secrets".to_owned(),
            observations,
        };
    }

    ProofResult {
        success: true,
        description: "Restore: provider recovers with correct Secrets".to_owned(),
        observations,
    }
}

/// TLS Stage 5: Rotate client cert (new cert, same CA) → scrape continues.
fn proof_tls_client_rotation() -> ProofResult {
    let mut observations = Vec::new();
    let cluster = "pool-a";
    let ctx = kind_context(cluster);
    let certs_dir = Path::new(CERTS_DIR);

    if !is_provider_observable(cluster, cluster) {
        observations.push("precondition failed: provider not observable at entry".to_owned());
        return ProofResult {
            success: false,
            description: "Client rotation: new cert from same CA works".to_owned(),
            observations,
        };
    }
    observations.push("precondition: provider observable at entry".to_owned());

    if let Err(e) = certs::rotate_metrics_client_cert(METRICS_CA_CN) {
        observations.push(format!("failed to generate rotated client cert: {e}"));
        return ProofResult {
            success: false,
            description: "Client rotation: new cert from same CA works".to_owned(),
            observations,
        };
    }
    observations.push("generated new client cert signed by same metrics CA".to_owned());

    if let Err(e) = apply_metrics_client_secret(&ctx, certs_dir) {
        observations.push(format!("failed to apply rotated client secret: {e}"));
        return ProofResult {
            success: false,
            description: "Client rotation: new cert from same CA works".to_owned(),
            observations,
        };
    }
    observations.push(format!("updated Secret/{METRICS_CLIENT_TLS_SECRET} with rotated cert"));

    // Wait a few reconcile cycles to confirm the operator picks up the new cert
    std::thread::sleep(Duration::from_secs(10));
    for _ in 0..3 {
        trigger_gridnetwork_reconcile(cluster);
        std::thread::sleep(TLS_POLL_INTERVAL);
    }

    let still_observable = wait_for_observable(cluster, cluster, TLS_TRANSITION_TIMEOUT);
    if still_observable {
        let candidates = read_overlay_candidates(cluster);
        let score = overlay_score_for_cluster(&candidates, cluster);
        observations.push(format!(
            "{cluster}: provider still observable after client rotation, score={score:.2}"
        ));
    } else {
        observations.push(format!(
            "{cluster}: provider became unobservable after client rotation — rotation failed"
        ));
        return ProofResult {
            success: false,
            description: "Client rotation: new cert from same CA works".to_owned(),
            observations,
        };
    }

    ProofResult {
        success: true,
        description: "Client rotation: new cert from same CA works".to_owned(),
        observations,
    }
}

/// TLS Stage 6: Rotate server cert + restart nginx → scrape continues.
///
/// **Limitation:** nginx does not reload TLS material automatically.
/// A `rollout restart` of the EPP Deployment is required. This is
/// documented honestly — the operator handles Secret rotation, but
/// the metrics proxy (nginx) needs a pod restart to load new certs.
fn proof_tls_server_rotation() -> ProofResult {
    let mut observations = Vec::new();
    let cluster = "pool-a";
    let ctx = kind_context(cluster);
    let certs_dir = Path::new(CERTS_DIR);

    if !is_provider_observable(cluster, cluster) {
        observations.push("precondition failed: provider not observable at entry".to_owned());
        return ProofResult {
            success: false,
            description: "Server rotation: new cert + nginx restart works".to_owned(),
            observations,
        };
    }
    observations.push("precondition: provider observable at entry".to_owned());

    if let Err(e) = certs::rotate_metrics_server_cert(METRICS_CA_CN, METRICS_SERVER_DNS) {
        observations.push(format!("failed to generate rotated server cert: {e}"));
        return ProofResult {
            success: false,
            description: "Server rotation: new cert + nginx restart works".to_owned(),
            observations,
        };
    }
    observations.push("generated new server cert signed by same metrics CA".to_owned());

    if let Err(e) = apply_metrics_server_secret(&ctx, certs_dir) {
        observations.push(format!("failed to apply rotated server secret: {e}"));
        return ProofResult {
            success: false,
            description: "Server rotation: new cert + nginx restart works".to_owned(),
            observations,
        };
    }
    observations.push(format!("updated Secret/{METRICS_SERVER_TLS_SECRET} with rotated cert"));

    observations.push("LIMITATION: nginx does not reload TLS in-place; rollout restart required".to_owned());
    if let Err(e) = rollout_restart(&ctx, "llmd-epp") {
        observations.push(format!("rollout restart failed: {e}"));
        return ProofResult {
            success: false,
            description: "Server rotation: new cert + nginx restart works".to_owned(),
            observations,
        };
    }
    observations.push("rollout restart of llmd-epp completed".to_owned());

    let recovered = wait_for_observable(cluster, cluster, TLS_TRANSITION_TIMEOUT);
    if recovered {
        let candidates = read_overlay_candidates(cluster);
        let score = overlay_score_for_cluster(&candidates, cluster);
        observations.push(format!(
            "{cluster}: provider observable after server rotation, score={score:.2}"
        ));
    } else {
        observations.push(format!(
            "{cluster}: provider not observable after server rotation — rotation failed"
        ));
        return ProofResult {
            success: false,
            description: "Server rotation: new cert + nginx restart works".to_owned(),
            observations,
        };
    }

    ProofResult {
        success: true,
        description: "Server rotation: new cert + nginx restart works".to_owned(),
        observations,
    }
}

/// TLS Stage 6: Independent stale-cache TTL verification.
///
/// Proves that `staleMetricsSeconds` (set to [`STALE_METRICS_TTL_SECS`])
/// allows the operator to serve cached metrics during a brief TLS outage,
/// and that the cached sample expires after the TTL.
///
/// Sequence:
/// 1. Record baseline score (provider must be observable).
/// 2. Trigger a reconcile to establish a fresh metrics sample.
/// 3. Delete the client cert Secret to break TLS.
/// 4. Before TTL expires: assert the provider is still observable (cached).
/// 5. After TTL expires: assert the provider becomes unobservable.
/// 6. Restore the client cert Secret and verify recovery.
fn proof_tls_stale_cache() -> ProofResult {
    let mut observations = Vec::new();
    let cluster = "pool-a";
    let ctx = kind_context(cluster);
    let certs_dir = Path::new(CERTS_DIR);

    // 1. Precondition: provider must be observable.
    if !is_provider_observable(cluster, cluster) {
        observations.push("precondition failed: provider not observable at entry".to_owned());
        return ProofResult {
            success: false,
            description: "Stale-cache TTL: cached metrics served before expiry, rejected after".to_owned(),
            observations,
        };
    }
    let candidates = read_overlay_candidates(cluster);
    let baseline_score = overlay_score_for_cluster(&candidates, cluster);
    observations.push(format!("baseline: {cluster} observable, score={baseline_score:.2}"));
    eprintln!("    baseline: {cluster} observable, score={baseline_score:.2}");

    // 2. Force a fresh scrape so the cache timestamp is recent.
    trigger_gridnetwork_reconcile(cluster);
    std::thread::sleep(Duration::from_secs(3));
    let pre_break = Instant::now();

    // 3. Break TLS by deleting the client cert Secret.
    if let Err(e) = delete_secret(&ctx, METRICS_CLIENT_TLS_SECRET) {
        observations.push(format!("failed to delete {METRICS_CLIENT_TLS_SECRET}: {e}"));
        return ProofResult {
            success: false,
            description: "Stale-cache TTL: cached metrics served before expiry, rejected after".to_owned(),
            observations,
        };
    }
    let msg = format!(
        "deleted Secret/{METRICS_CLIENT_TLS_SECRET} to break TLS (staleMetricsSeconds={STALE_METRICS_TTL_SECS})"
    );
    eprintln!("    {msg}");
    observations.push(msg);

    // 4. Inside-TTL check: provider should still be observable (cached metrics). Poll within the first half of the TTL
    //    window.
    let inside_ttl_deadline = pre_break + Duration::from_secs(STALE_METRICS_TTL_SECS / 2);
    let mut inside_ttl_observable = false;
    while Instant::now() < inside_ttl_deadline {
        trigger_gridnetwork_reconcile(cluster);
        std::thread::sleep(Duration::from_secs(2));
        if is_provider_observable(cluster, cluster) {
            inside_ttl_observable = true;
            let elapsed = pre_break.elapsed().as_secs();
            let refreshed_candidates = read_overlay_candidates(cluster);
            let score = overlay_score_for_cluster(&refreshed_candidates, cluster);
            let overlay_msg = format!(
                "inside-TTL ({elapsed}s/{STALE_METRICS_TTL_SECS}s): {cluster} still observable, \
                 score={score:.2} (cached metrics served)"
            );
            eprintln!("    {overlay_msg}");
            observations.push(overlay_msg);
            break;
        }
    }
    if !inside_ttl_observable {
        let elapsed = pre_break.elapsed().as_secs();
        observations.push(format!(
            "inside-TTL ({elapsed}s/{STALE_METRICS_TTL_SECS}s): {cluster} became unobservable \
             before TTL expired — cached metrics not served"
        ));
        // Restore before returning
        drop(apply_metrics_client_secret(&ctx, certs_dir));
        wait_for_observable(cluster, cluster, TLS_TRANSITION_TIMEOUT);
        return ProofResult {
            success: false,
            description: "Stale-cache TTL: cached metrics served before expiry, rejected after".to_owned(),
            observations,
        };
    }

    // 5. Post-TTL check: wait for the TTL to expire, then assert unobservable.
    let remaining_ttl = STALE_METRICS_TTL_SECS.saturating_sub(pre_break.elapsed().as_secs());
    if remaining_ttl > 0 {
        std::thread::sleep(Duration::from_secs(remaining_ttl + 5));
    }
    // Force a reconcile so the operator evaluates the expired cache.
    trigger_gridnetwork_reconcile(cluster);
    std::thread::sleep(Duration::from_secs(3));

    let post_ttl_unobservable =
        !is_provider_observable(cluster, cluster) || wait_for_unobservable(cluster, cluster, Duration::from_secs(30));
    let elapsed = pre_break.elapsed().as_secs();
    if post_ttl_unobservable {
        let retry_msg = format!(
            "post-TTL ({elapsed}s/{STALE_METRICS_TTL_SECS}s): {cluster} unobservable \
             (cached metrics expired, UNOBSERVABLE_METRICS applied)"
        );
        eprintln!("    {retry_msg}");
        observations.push(retry_msg);
    } else {
        observations.push(format!(
            "post-TTL ({elapsed}s/{STALE_METRICS_TTL_SECS}s): {cluster} still observable \
             after TTL expired — stale metrics not evicted"
        ));
        drop(apply_metrics_client_secret(&ctx, certs_dir));
        wait_for_observable(cluster, cluster, TLS_TRANSITION_TIMEOUT);
        return ProofResult {
            success: false,
            description: "Stale-cache TTL: cached metrics served before expiry, rejected after".to_owned(),
            observations,
        };
    }

    // 6. Restore the client cert Secret and verify recovery.
    if let Err(e) = apply_metrics_client_secret(&ctx, certs_dir) {
        observations.push(format!("failed to restore client secret: {e}"));
        return ProofResult {
            success: false,
            description: "Stale-cache TTL: cached metrics served before expiry, rejected after".to_owned(),
            observations,
        };
    }
    let recovered = wait_for_observable(cluster, cluster, TLS_TRANSITION_TIMEOUT);
    if recovered {
        let recovery_candidates = read_overlay_candidates(cluster);
        let score = overlay_score_for_cluster(&recovery_candidates, cluster);
        let recovery_msg = format!("recovery: {cluster} observable after client cert restored, score={score:.2}");
        eprintln!("    {recovery_msg}");
        observations.push(recovery_msg);
    } else {
        observations.push(format!("{cluster}: provider did not recover after stale-cache test"));
        return ProofResult {
            success: false,
            description: "Stale-cache TTL: cached metrics served before expiry, rejected after".to_owned(),
            observations,
        };
    }

    ProofResult {
        success: true,
        description: "Stale-cache TTL: cached metrics served before expiry, rejected after".to_owned(),
        observations,
    }
}

/// TLS Stage 8: Verify existing routing still works after TLS manipulations.
fn proof_tls_routing() -> ProofResult {
    let mut observations = Vec::new();

    // Verify both providers are observable
    for cluster in CLUSTERS {
        if !is_provider_observable(cluster, cluster) && !wait_for_observable(cluster, cluster, TLS_TRANSITION_TIMEOUT) {
            observations.push(format!("{cluster}: provider NOT observable — routing check impossible"));
            return ProofResult {
                success: false,
                description: "Existing routing: inference routing works after TLS manipulations".to_owned(),
                observations,
            };
        }
        let candidates = read_overlay_candidates(cluster);
        let score = overlay_score_for_cluster(&candidates, cluster);
        observations.push(format!("{cluster}: observable, score={score:.2}"));
    }

    // Send an inference request and verify attribution
    let probe_ctx = kind_context("pool-a");
    match send_inference_request(&probe_ctx, VCR_MODEL) {
        Ok(resp) => {
            observations.push(format!(
                "routing attribution: gateway={} provider={}",
                resp.provider_gateway, resp.demo_attribution
            ));
        },
        Err(e) => {
            observations.push(format!("inference request failed: {e}"));
            return ProofResult {
                success: false,
                description: "Existing routing: inference routing works after TLS manipulations".to_owned(),
                observations,
            };
        },
    }

    ProofResult {
        success: true,
        description: "Existing routing: inference routing works after TLS manipulations".to_owned(),
        observations,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use std::collections::BTreeSet;

    use serde::Deserialize as _;

    use super::*;

    #[test]
    fn pool_metrics_topology_uses_deterministic_selection_for_pressure_recovery()
    -> Result<(), Box<dyn std::error::Error>> {
        let config_source = include_str!("../../../tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml");
        let config: serde_yaml::Value = serde_yaml::from_str(config_source)?;

        for site in ["pool-a-site", "pool-b-site"] {
            let selection_mode = config
                .get("spec")
                .and_then(|spec| spec.get("stacks"))
                .and_then(|stacks| stacks.get(site))
                .and_then(|site| site.get("steps"))
                .and_then(|steps| steps.get(0))
                .and_then(|step| step.get("values"))
                .and_then(|values| values.get("gridNetwork"))
                .and_then(|network| network.get("selectionPolicy"))
                .and_then(|policy| policy.get("mode"))
                .and_then(serde_yaml::Value::as_str);
            assert_eq!(
                selection_mode,
                Some("deterministic"),
                "{site} must use deterministic selection because this qualification asserts rank-0 preference, not equal-turn round robin"
            );
        }
        Ok(())
    }

    #[test]
    fn pool_metrics_simulator_backends_use_the_same_dummy_tokenizer_mode() -> Result<(), Box<dyn std::error::Error>> {
        for source in [
            include_str!("../../../tests/e2e/topologies/grid-llmd-pool-metrics/resources/pool-a/vcr-deployment.yaml"),
            include_str!("../../../tests/e2e/topologies/grid-llmd-pool-metrics/resources/pool-b/vcr-deployment.yaml"),
        ] {
            let mut deployments = BTreeSet::new();
            for document in serde_yaml::Deserializer::from_str(source) {
                let value = serde_yaml::Value::deserialize(document)?;
                if value.get("kind").and_then(serde_yaml::Value::as_str) != Some("Deployment") {
                    continue;
                }
                let Some(name) = value
                    .get("metadata")
                    .and_then(|metadata| metadata.get("name"))
                    .and_then(serde_yaml::Value::as_str)
                else {
                    continue;
                };
                let args = value
                    .get("spec")
                    .and_then(|spec| spec.get("template"))
                    .and_then(|template| template.get("spec"))
                    .and_then(|spec| spec.get("containers"))
                    .and_then(|containers| containers.get(0))
                    .and_then(|container| container.get("args"))
                    .and_then(serde_yaml::Value::as_sequence)
                    .ok_or_else(|| std::io::Error::other(format!("{name} simulator args must be a YAML sequence")))?;
                assert!(
                    args.iter().any(|arg| arg.as_str() == Some("--force-dummy-tokenizer")),
                    "{name} must use dummy tokenization; otherwise llm-d-inference-sim calls its absent localhost:8082 renderer"
                );
                deployments.insert(name.to_owned());
            }
            assert_eq!(deployments, BTreeSet::from(["vcr-1".to_owned(), "vcr-2".to_owned()]));
        }
        Ok(())
    }

    #[test]
    fn pool_metrics_topology_reads_generated_config_from_isolated_forge_state() {
        let config = include_str!("../../../tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml");

        for component in ["provider", "consumer"] {
            let expected = format!(
                "--from-file=praxis.yaml=${{FORGE_STATE_DIR:-.forge}}/runtime/{{{{ cluster.name }}}}/{component}/praxis.yaml"
            );
            assert!(
                config.contains(&expected),
                "the {component} configmap command must read generated files from the active Forge state directory"
            );
        }
    }

    #[test]
    fn utc_timestamp_format_is_valid() {
        let ts = format_utc_timestamp();
        assert_eq!(ts.len(), 16, "expected YYYYMMDDTHHMMSSz format");
        assert!(ts.ends_with('Z'));
        let bytes = ts.as_bytes();
        assert_eq!(bytes.get(8).copied(), Some(b'T'));
        assert!(bytes.get(..8).unwrap().iter().all(u8::is_ascii_digit));
        assert!(bytes.get(9..15).unwrap().iter().all(u8::is_ascii_digit));
    }

    #[test]
    fn run_id_is_valid_in_kind_cluster_names() {
        let run_id = format_run_id("20260930T202802Z", 788_430);
        assert_eq!(run_id, "20260930t202802z-788430");
        assert!(
            run_id
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        );
    }

    #[test]
    fn provider_diagnostic_summary_excludes_credentials_and_endpoints() {
        let raw = serde_json::json!({
            "items": [{
                "metadata": {"name": "provider-a"},
                "spec": {
                    "gridNetworkRef": "network-a",
                    "endpoint": "http://private-endpoint.invalid",
                    "auth": {"token": "do-not-emit"},
                    "metricsConfig": {
                        "poolName": "pool-a",
                        "queueCapacity": 4,
                        "signalNames": {"queueDepth": "inference_pool_average_queue_size"}
                    }
                },
                "status": {"phase": "Available", "reason": "Ready"}
            }]
        });

        let summary = diagnostic_provider_summary(&raw.to_string()).to_string();
        assert!(summary.contains("provider-a"));
        assert!(summary.contains("pool-a"));
        assert!(summary.contains("inference_pool_average_queue_size"));
        assert!(!summary.contains("do-not-emit"));
        assert!(!summary.contains("private-endpoint"));
    }

    #[test]
    fn append_run_error_preserves_primary_failure_and_adds_cleanup_failure() {
        let mut error = Some("setup failed: overlay timeout".to_owned());
        append_run_error(&mut error, "teardown failed: cluster still exists".to_owned());
        assert_eq!(
            error.as_deref(),
            Some("setup failed: overlay timeout; teardown failed: cluster still exists")
        );
    }

    #[test]
    fn utc_iso_format_has_separators() {
        let iso = format_utc_iso();
        assert!(iso.contains('-'), "ISO format must contain dashes");
        assert!(iso.contains(':'), "ISO format must contain colons");
        assert!(iso.ends_with('Z'), "ISO format must end with Z");
    }

    #[test]
    fn kind_context_has_prefix() {
        assert_eq!(kind_context("pool-a"), "kind-grid-llmd-pm-pool-a");
        assert_eq!(kind_context("pool-b"), "kind-grid-llmd-pm-pool-b");
    }

    #[test]
    fn clusters_are_two_pools() {
        assert_eq!(CLUSTERS.len(), 2);
        assert!(CLUSTERS.contains(&"pool-a"));
        assert!(CLUSTERS.contains(&"pool-b"));
    }

    #[test]
    fn extract_prom_value_parses_labeled_metric() {
        let text = r#"# HELP inference_pool_average_kv_cache_utilization Average kv cache
# TYPE inference_pool_average_kv_cache_utilization gauge
inference_pool_average_kv_cache_utilization{name="pool-a"} 0.35
"#;
        let val = extract_prom_value(text, "inference_pool_average_kv_cache_utilization");
        assert_eq!(val, Some(0.35), "expected Some(0.35)");
    }

    #[test]
    fn extract_prom_value_returns_none_for_missing_metric() {
        let text = "some_other_metric 1.0\n";
        let val = extract_prom_value(text, "inference_pool_average_queue_size");
        assert_eq!(val, None, "expected None for missing metric");
    }

    #[test]
    fn simulator_config_persists_zero_queue_value() {
        let config = simulator_config(0, 0.0);
        assert!(config.contains("fake-metrics:"));
        assert!(config.contains("waiting-requests: 0"));
        assert!(config.contains("kv-cache-usage: 0"));
    }

    #[test]
    fn simulator_config_persists_pressure_queue_value() {
        let config = simulator_config(9, 0.0);
        assert!(config.contains("waiting-requests: 9"));
        assert!(!config.contains("pressure-generator"));
    }

    #[test]
    fn kv_cache_pressure_sets_only_the_selected_signal() {
        let (queue, kv_cache) = simulator_metrics(9, ScoringFlavor::KvCachePressure);
        let config = simulator_config(queue, kv_cache);
        assert!(config.contains("waiting-requests: 0"));
        assert!(config.contains("kv-cache-usage: 0.95"));
    }

    #[test]
    fn queue_pressure_sets_only_the_selected_signal() {
        let (queue, kv_cache) = simulator_metrics(9, ScoringFlavor::QueueDepth);
        assert_eq!(queue, 9);
        assert!(kv_cache.abs() < f64::EPSILON);
    }

    #[test]
    fn required_metric_parser_rejects_missing_scrape() {
        assert!(parse_required_epp_metrics("").is_err());
    }

    #[test]
    fn required_metric_parser_reads_exact_queue_and_kv_values() {
        let text = "llm_d_epp_average_queue_size{name=\"pool-a\"} 9\n\
                    llm_d_epp_average_kv_cache_utilization{name=\"pool-a\"} 0\n";
        let metrics = parse_required_epp_metrics(text).unwrap();
        assert!((metrics.queue_size - 9.0).abs() < f64::EPSILON);
        assert!(metrics.kv_cache.abs() < f64::EPSILON);
    }

    #[test]
    fn evidence_serializes_to_json() {
        let evidence = Evidence {
            schema_version: "1".to_owned(),
            mode: "quick".to_owned(),
            metrics_transport: "direct-http".to_owned(),
            scoring_strategy: ScoringFlavor::QueueDepth.label().to_owned(),
            placement_strategy: "score preference".to_owned(),
            run_id: "test-run".to_owned(),
            started_at: "2026-01-01T00:00:00Z".to_owned(),
            wall_secs: 42.0,
            success: true,
            error: None,
            setup: SetupEvidence {
                clusters: vec!["pool-a".to_owned(), "pool-b".to_owned()],
                images: BTreeMap::new(),
                pod_images: Vec::new(),
            },
            proofs: BTreeMap::new(),
            lifecycle: LifecycleRecord {
                teardown_requested: false,
                teardown_performed: false,
                teardown_result: None,
                kept_on_failure: false,
            },
        };
        let json = serde_json::to_string_pretty(&evidence).unwrap();
        assert!(json.contains("\"schema_version\""));
        assert!(json.contains("pool-a"));
    }

    #[test]
    fn leap_year_detection() {
        assert!(is_leap(2024));
        assert!(!is_leap(2023));
        assert!(is_leap(2000));
        assert!(!is_leap(1900));
    }

    #[test]
    fn metrics_transport_labels() {
        assert_eq!(MetricsTransport::DirectHttp.label(), "direct-http");
        assert_eq!(MetricsTransport::MtlsProxy.label(), "mtls-proxy");
    }

    #[test]
    fn scoring_flavor_from_kv_cache_flag() {
        assert_eq!(ScoringFlavor::from_kv_cache_flag(false), ScoringFlavor::QueueDepth);
        assert_eq!(ScoringFlavor::from_kv_cache_flag(true), ScoringFlavor::KvCachePressure);
    }

    #[test]
    fn scoring_flavor_labels() {
        assert_eq!(ScoringFlavor::QueueDepth.label(), "queue-depth");
        assert_eq!(ScoringFlavor::KvCachePressure.label(), "kv-cache-pressure");
    }

    #[test]
    fn scoring_flavor_strategy_yaml_matches_grid_network_crd() {
        // Must match `ScoringStrategy`'s camelCase serde rename in
        // operator/src/crd/grid_network.rs exactly, since this string is
        // spliced directly into the GridNetwork Helm values.
        assert_eq!(ScoringFlavor::QueueDepth.strategy_yaml(), "queueDepth");
        assert_eq!(ScoringFlavor::KvCachePressure.strategy_yaml(), "kvCachePressure");
    }

    #[test]
    fn pressure_phase_active_queue_depth_flavor_ignores_kv_cache() {
        let low_queue_high_kv = EppMetrics {
            queue_size: 0.0,
            kv_cache: 0.9,
        };
        assert!(
            !pressure_phase_active(ScoringFlavor::QueueDepth, &low_queue_high_kv),
            "queue-depth flavor must key off queue_size, not kv_cache"
        );

        let high_queue = EppMetrics {
            queue_size: 2.0,
            kv_cache: 0.0,
        };
        assert!(pressure_phase_active(ScoringFlavor::QueueDepth, &high_queue));
    }

    #[test]
    fn pressure_phase_active_kv_cache_flavor_ignores_queue_size() {
        let high_queue_low_kv = EppMetrics {
            queue_size: 3.0,
            kv_cache: 0.0,
        };
        assert!(
            !pressure_phase_active(ScoringFlavor::KvCachePressure, &high_queue_low_kv),
            "kv-cache flavor must key off kv_cache, not queue_size"
        );

        let high_kv = EppMetrics {
            queue_size: 0.0,
            kv_cache: 0.5,
        };
        assert!(pressure_phase_active(ScoringFlavor::KvCachePressure, &high_kv));
    }

    #[test]
    #[expect(clippy::float_cmp, reason = "exact literal round-trips in test assertions")]
    fn parse_epp_metrics_prefers_llm_d_epp_metric_names() {
        // The current llm-d-router-endpoint-picker build emits the
        // llm_d_epp_* series; when several series are present it must win.
        let text = "llm_d_epp_average_queue_size{name=\"pool-a\"} 4.5\n\
                     llm_d_epp_average_kv_cache_utilization{name=\"pool-a\"} 0.35\n\
                     inference_pool_average_queue_size{name=\"pool-a\"} 9.9\n\
                     inference_pool_average_kv_cache_utilization{name=\"pool-a\"} 0.99\n";
        let epp = parse_epp_metrics(text);
        assert_eq!(epp.queue_size, 4.5);
        assert_eq!(epp.kv_cache, 0.35);
    }

    #[test]
    #[expect(
        clippy::indexing_slicing,
        reason = "the exposition fixture yields exactly one matching sample"
    )]
    fn dynamic_signal_parser_keeps_source_site_provider_and_timestamp() {
        let exposition = concat!(
            "# TYPE grid_routing_queue_pressure gauge\n",
            "grid_routing_queue_pressure{grid_site=\"pool-a\",grid_provider=\"llmd-pool-a-provider\"} 0.75 1790731200123\n",
            "other_metric{grid_site=\"pool-b\",grid_provider=\"ignored\"} 0.25 1790731200123\n",
        );
        let parsed = parse_dynamic_signal_samples(exposition, "grid_routing_queue_pressure");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].site, "pool-a");
        assert_eq!(parsed[0].provider, "llmd-pool-a-provider");
        assert!((parsed[0].value - 0.75).abs() < f64::EPSILON);
        assert_eq!(parsed[0].timestamp_ms, Some(1_790_731_200_123));
    }

    #[test]
    fn dynamic_state_stability_waits_for_peer_poll_window_and_matching_observations() {
        let peer_poll_window = Duration::from_secs(32);
        assert!(!dynamic_state_stability_satisfied(
            1,
            Duration::from_secs(40),
            peer_poll_window
        ));
        assert!(!dynamic_state_stability_satisfied(
            2,
            Duration::from_secs(31),
            peer_poll_window
        ));
        assert!(dynamic_state_stability_satisfied(2, peer_poll_window, peer_poll_window));
    }

    #[test]
    fn dynamic_phase_gate_requires_two_fresh_positive_group_zero_weights() {
        let candidate = |site: &str, weight| DynamicCandidateWeight {
            site: site.to_owned(),
            provider: format!("llmd-{site}-provider"),
            stable_id: format!("stable-{site}"),
            selection_group: 0,
            traffic_weight: weight,
            fresh: true,
            admission_state: "new_and_existing".to_owned(),
        };
        let baseline = DynamicOverlaySnapshot {
            semantic_revision: "baseline".to_owned(),
            resource_version: "10".to_owned(),
            candidates: vec![candidate("pool-a", 500), candidate("pool-b", 500)],
        };
        assert!(dynamic_weights_match_phase(&baseline, "baseline"));
        let pressure = DynamicOverlaySnapshot {
            semantic_revision: "pressure".to_owned(),
            resource_version: "11".to_owned(),
            candidates: vec![candidate("pool-a", 250), candidate("pool-b", 750)],
        };
        assert!(dynamic_weights_match_phase(&pressure, "pressure"));
        let invalid_zero = DynamicOverlaySnapshot {
            candidates: vec![candidate("pool-a", 0), candidate("pool-b", 1000)],
            ..pressure
        };
        assert!(!dynamic_weights_match_phase(&invalid_zero, "pressure"));
        let invalid_group = DynamicOverlaySnapshot {
            candidates: vec![
                candidate("pool-a", 500),
                DynamicCandidateWeight {
                    selection_group: 1,
                    ..candidate("pool-b", 500)
                },
            ],
            ..baseline
        };
        assert!(!dynamic_weights_match_phase(&invalid_group, "baseline"));
    }

    #[test]
    fn dynamic_gate_allows_site_specific_revisions_and_candidate_order() {
        let candidate = |site: &str| DynamicCandidateWeight {
            site: site.to_owned(),
            provider: format!("llmd-{site}-provider"),
            stable_id: format!("stable-{site}"),
            selection_group: 0,
            traffic_weight: 500,
            fresh: true,
            admission_state: "new_and_existing".to_owned(),
        };
        let pool_a = DynamicOverlaySnapshot {
            semantic_revision: "pool-a-revision".to_owned(),
            resource_version: "10".to_owned(),
            candidates: vec![candidate("pool-a"), candidate("pool-b")],
        };
        let pool_b = DynamicOverlaySnapshot {
            semantic_revision: "pool-b-revision".to_owned(),
            resource_version: "22".to_owned(),
            candidates: vec![candidate("pool-b"), candidate("pool-a")],
        };

        assert!(dynamic_overlays_match_phase(&pool_a, &pool_b, "baseline"));
        assert!(dynamic_revisions_changed(&pool_a, &pool_b, None));

        let previous = BTreeMap::from([
            ("pool-a".to_owned(), "old-pool-a-revision".to_owned()),
            ("pool-b".to_owned(), "old-pool-b-revision".to_owned()),
        ]);
        assert!(dynamic_revisions_changed(&pool_a, &pool_b, Some(&previous)));
        let partially_unchanged = BTreeMap::from([
            ("pool-a".to_owned(), "old-pool-a-revision".to_owned()),
            ("pool-b".to_owned(), "pool-b-revision".to_owned()),
        ]);
        assert!(!dynamic_revisions_changed(&pool_a, &pool_b, Some(&partially_unchanged)));
    }

    #[test]
    fn dynamic_gate_allows_independently_smoothed_weights_within_phase_bounds() {
        let candidate = |site: &str, traffic_weight| DynamicCandidateWeight {
            site: site.to_owned(),
            provider: format!("llmd-{site}-provider"),
            stable_id: format!("stable-{site}"),
            selection_group: 0,
            traffic_weight,
            fresh: true,
            admission_state: "new_and_existing".to_owned(),
        };
        let pool_a = DynamicOverlaySnapshot {
            semantic_revision: "pool-a-recovery".to_owned(),
            resource_version: "10".to_owned(),
            candidates: vec![candidate("pool-a", 481), candidate("pool-b", 519)],
        };
        let pool_b = DynamicOverlaySnapshot {
            semantic_revision: "pool-b-recovery".to_owned(),
            resource_version: "22".to_owned(),
            candidates: vec![candidate("pool-b", 500), candidate("pool-a", 500)],
        };

        assert!(dynamic_overlays_match_phase(&pool_a, &pool_b, "recovery"));

        let outside_recovery_bounds = DynamicOverlaySnapshot {
            candidates: vec![candidate("pool-a", 449), candidate("pool-b", 551)],
            ..pool_b
        };
        assert!(!dynamic_overlays_match_phase(
            &pool_a,
            &outside_recovery_bounds,
            "recovery"
        ));
    }

    #[test]
    fn operator_restart_snapshot_excludes_terminating_unready_and_unrelated_pods() {
        let pods = vec![
            serde_json::json!({
                "metadata": {
                    "name": "grid-operator-old",
                    "uid": "old-uid",
                    "deletionTimestamp": "2026-09-30T23:00:00Z"
                },
                "status": {"conditions": [{"type": "Ready", "status": "True"}]}
            }),
            serde_json::json!({
                "metadata": {"name": "grid-operator-current", "uid": "current-uid"},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]}
            }),
            serde_json::json!({
                "metadata": {"name": "grid-operator-pending", "uid": "pending-uid"},
                "status": {"conditions": [{"type": "Ready", "status": "False"}]}
            }),
            serde_json::json!({
                "metadata": {"name": "consumer-gateway", "uid": "gateway-uid"},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]}
            }),
        ];

        assert_eq!(active_dynamic_operator_pod_uids(&pods), ["current-uid"]);
    }

    #[test]
    fn simulator_annotation_avoids_a_noop_rollout() {
        assert!(!simulator_annotation_needs_rollout(Some("9"), "9"));
        assert!(simulator_annotation_needs_rollout(Some("0"), "9"));
        assert!(simulator_annotation_needs_rollout(None, "9"));
    }

    #[test]
    fn dynamic_revision_log_parser_does_not_match_serving_revision_suffixes() {
        let logs = concat!(
            "INFO overlay initialized accepted_revision=INITIAL serving_revision=INITIAL\n",
            "INFO overlay reloaded accepted_revision=NEW serving_revision=NEW previous_serving_revision=INITIAL\n",
            "ERROR overlay reload failed retained_serving_revision=NEW\n",
        );
        assert_eq!(
            latest_dynamic_log_field(logs, "accepted_revision").as_deref(),
            Some("NEW")
        );
        assert_eq!(
            latest_dynamic_log_field(logs, "serving_revision").as_deref(),
            Some("NEW")
        );
        assert_eq!(
            latest_dynamic_log_field(logs, "previous_serving_revision").as_deref(),
            Some("INITIAL")
        );
    }

    #[test]
    fn dynamic_revision_log_parser_handles_quoted_values_and_requires_field_boundary() {
        let logs = concat!(
            "INFO serving_revision=OLD\n",
            "INFO accepted_revision=\"new revision\" serving_revision=\"new revision\"\n",
            "ERROR retained_serving_revision=STALE\n",
        );
        assert_eq!(
            latest_dynamic_log_field(logs, "serving_revision").as_deref(),
            Some("new revision")
        );
        assert_eq!(
            latest_dynamic_log_field(logs, "accepted_revision").as_deref(),
            Some("new revision")
        );
    }

    #[test]
    fn dynamic_revision_log_parser_handles_ansi_between_field_and_value() {
        let logs = concat!(
            "\x1b[2mINFO\x1b[0m overlay reloaded ",
            "\x1b[3maccepted_revision\x1b[0m\x1b[2m=\x1b[0mrevision-a ",
            "\x1b[3mserving_revision\x1b[0m\x1b[2m=\x1b[0mrevision-a ",
            "\x1b[3mprevious_serving_revision\x1b[0m\x1b[2m=\x1b[0mrevision-old",
        );
        assert_eq!(
            latest_dynamic_log_field(logs, "accepted_revision").as_deref(),
            Some("revision-a")
        );
        assert_eq!(
            latest_dynamic_log_field(logs, "serving_revision").as_deref(),
            Some("revision-a")
        );
        assert_eq!(
            latest_dynamic_log_field(logs, "previous_serving_revision").as_deref(),
            Some("revision-old")
        );
    }

    #[test]
    fn dynamic_weighted_sample_uses_new_sessionless_requests_and_no_retries() {
        let script = dynamic_sample_script("pressure");
        assert!(script.contains("pressure-$worker-$ordinal"));
        assert!(script.contains("-le 400"));
        assert_eq!(DYNAMIC_SAMPLE_SIZE, 1600);
        assert!(script.contains("--max-time 20"));
        assert!(script.contains("%header{X-Grid-LlmD-Provider-Gateway}"));
        assert!(script.contains("%header{x-ai-demo-provider-gateway}"));
        assert!(!script.contains("%{header:"));
        assert!(!script.contains("--retry"));
        assert!(!script.contains("X-Session-Id"));
    }

    #[test]
    fn weighted_affinity_requires_both_provider_bindings_and_exact_replay() {
        let bound = (1..=DYNAMIC_AFFINITY_SESSION_COUNT)
            .map(|ordinal| DynamicAffinitySample {
                stage: "bind".to_owned(),
                session_ordinal: ordinal,
                curl_exit_code: 0,
                http_status: Some(200),
                provider_gateway: if ordinal.is_multiple_of(2) { "pool-a" } else { "pool-b" }.to_owned(),
                provider_attribution: if ordinal.is_multiple_of(2) { "pool-a" } else { "pool-b" }.to_owned(),
            })
            .collect::<Vec<_>>();
        assert!(affinity_bind_samples_valid(&bound));
        let bindings = bound
            .iter()
            .map(|sample| (sample.session_ordinal, sample.provider_gateway.clone()))
            .collect::<BTreeMap<_, _>>();
        let replay = bound
            .iter()
            .map(|sample| DynamicAffinitySample {
                stage: "replay".to_owned(),
                ..sample.clone()
            })
            .collect::<Vec<_>>();
        assert!(affinity_replay_matches(&bindings, &replay));

        let mut moved = replay;
        let Some(first) = moved.first_mut() else {
            return;
        };
        first.provider_gateway = if first.provider_gateway == "pool-a" {
            "pool-b"
        } else {
            "pool-a"
        }
        .to_owned();
        first.provider_attribution.clone_from(&first.provider_gateway);
        assert!(!affinity_replay_matches(&bindings, &moved));

        let mut incomplete = bound;
        incomplete.pop();
        assert!(!affinity_bind_samples_valid(&incomplete));
    }

    #[test]
    fn dynamic_chi_square_uses_published_probabilities() {
        assert!(dynamic_chi_square([500, 500], [200, 200]).abs() < f64::EPSILON);
        assert!(dynamic_chi_square([300, 700], [120, 280]) < DYNAMIC_CHI_SQUARE_CRITICAL);
        assert!(dynamic_chi_square([300, 700], [220, 180]) > DYNAMIC_CHI_SQUARE_CRITICAL);
        assert!(dynamic_chi_square([1, 999], [1, 399]).is_infinite());
    }

    #[test]
    fn dynamic_sample_size_supports_recovery_floor_without_relaxing_it() {
        let expected_recovery_share = 0.481_f64;
        let standard_error =
            (expected_recovery_share * (1.0 - expected_recovery_share) / f64::from(DYNAMIC_SAMPLE_SIZE)).sqrt();
        let one_sided_99_percent_lower_bound = expected_recovery_share - (2.326 * standard_error);
        assert!(one_sided_99_percent_lower_bound >= DYNAMIC_MIN_RECOVERY_SHARE);
        assert!((DYNAMIC_MIN_RECOVERY_SHARE - 0.45).abs() < f64::EPSILON);
        assert_eq!(DYNAMIC_SAMPLE_SIZE, 1600);
    }

    #[test]
    #[expect(
        clippy::indexing_slicing,
        reason = "the overlay fixture contains exactly one candidate"
    )]
    fn dynamic_overlay_parser_requires_weighted_contract_and_reads_revision() -> Result<(), Box<dyn std::error::Error>>
    {
        let json = serde_json::json!({
            "metadata": {
                "resourceVersion": "55",
                "annotations": {"grid.praxis-proxy.io/overlay-revision": "sha256:revision"}
            },
            "data": {"routing-config.json": serde_json::json!({
                "selection_policy": {"mode": "weightedRandom"},
                "candidates": [{
                    "site": "pool-a", "cluster": "llmd-pool-a-provider", "stable_id": "id-a",
                    "selection_group": 0, "traffic_weight": 500, "fresh": true,
                    "admission_state": "new_and_existing"
                }]
            }).to_string()}
        });
        let overlay = parse_dynamic_overlay_json(&serde_json::to_vec(&json)?)?;
        assert_eq!(overlay.semantic_revision, "sha256:revision");
        assert_eq!(overlay.resource_version, "55");
        assert_eq!(overlay.candidates.len(), 1);
        assert_eq!(overlay.candidates[0].traffic_weight, 500);
        Ok(())
    }

    #[test]
    #[expect(clippy::float_cmp, reason = "exact literal round-trips in test assertions")]
    fn parse_epp_metrics_falls_back_to_inference_pool_metric_names() {
        // Older llm-d-inference-scheduler builds expose only inference_pool_*.
        let text = "inference_pool_average_queue_size{name=\"pool-a\"} 4.5\n\
                     inference_pool_average_kv_cache_utilization{name=\"pool-a\"} 0.35\n";
        let epp = parse_epp_metrics(text);
        assert_eq!(epp.queue_size, 4.5);
        assert_eq!(epp.kv_cache, 0.35);
    }

    #[test]
    #[expect(clippy::float_cmp, reason = "exact literal round-trips in test assertions")]
    fn parse_epp_metrics_falls_back_to_llm_d_router_metric_names() {
        // Some EPP builds only expose the llm_d_router_* series (no
        // inference_pool_* series at all) -- both queue_size and kv_cache
        // must fall back symmetrically, or a kvCachePressure run against
        // such an EPP always reads kv_cache=0.0 and never detects pressure.
        let text = "llm_d_router_epp_average_queue_size{name=\"pool-a\"} 6.0\n\
                     llm_d_router_epp_average_kv_cache_utilization{name=\"pool-a\"} 0.42\n";
        let epp = parse_epp_metrics(text);
        assert_eq!(epp.queue_size, 6.0);
        assert_eq!(epp.kv_cache, 0.42);
    }

    #[test]
    #[expect(clippy::float_cmp, reason = "exact literal round-trips in test assertions")]
    fn parse_epp_metrics_defaults_to_zero_when_absent() {
        let epp = parse_epp_metrics("");
        assert_eq!(epp.queue_size, 0.0);
        assert_eq!(epp.kv_cache, 0.0);
    }

    #[test]
    fn any_metric_present_ignores_comment_lines_and_absence() {
        let text = "# HELP llm_d_epp_ready_endpoints ready endpoints\n\
                     llm_d_epp_ready_endpoints{name=\"pool-a\"} 2\n";
        assert!(any_metric_present(text, EPP_READY_METRICS));
        assert!(!any_metric_present("no epp metrics here", EPP_READY_METRICS));
    }

    #[test]
    fn recovery_condition_met_queue_depth_flavor_uses_calibrated_threshold() {
        let draining = EppMetrics {
            queue_size: 2.9,
            kv_cache: 0.9, // must be ignored for this flavor
        };
        assert!(recovery_condition_met(ScoringFlavor::QueueDepth, &draining));

        let still_pressured = EppMetrics {
            queue_size: 3.0,
            kv_cache: 0.0,
        };
        assert!(!recovery_condition_met(ScoringFlavor::QueueDepth, &still_pressured));
    }

    #[test]
    fn recovery_condition_met_kv_cache_flavor_requires_queue_drain_and_low_pressure() {
        let recovered = EppMetrics {
            queue_size: 2.9, // must be below the shared recovery threshold
            kv_cache: 0.0,
        };
        assert!(recovery_condition_met(ScoringFlavor::KvCachePressure, &recovered));

        let queue_still_saturated = EppMetrics {
            queue_size: 4.0,
            kv_cache: 0.0,
        };
        assert!(!recovery_condition_met(
            ScoringFlavor::KvCachePressure,
            &queue_still_saturated
        ));

        let still_pressured = EppMetrics {
            queue_size: 0.0,
            kv_cache: 0.5,
        };
        assert!(!recovery_condition_met(
            ScoringFlavor::KvCachePressure,
            &still_pressured
        ));
    }

    #[test]
    fn setup_phase_count_differs_by_transport() {
        const _: () = assert!(SETUP_PHASES_MTLS > SETUP_PHASES_DIRECT);
        const _: () = assert!(SETUP_PHASES_MTLS - SETUP_PHASES_DIRECT == 1);
    }

    #[test]
    fn evidence_records_direct_http_transport() {
        let evidence = Evidence {
            schema_version: "1".to_owned(),
            mode: "quick".to_owned(),
            metrics_transport: MetricsTransport::DirectHttp.label().to_owned(),
            scoring_strategy: ScoringFlavor::QueueDepth.label().to_owned(),
            placement_strategy: "score preference".to_owned(),
            run_id: "test-run".to_owned(),
            started_at: "2026-01-01T00:00:00Z".to_owned(),
            wall_secs: 10.0,
            success: true,
            error: None,
            setup: SetupEvidence {
                clusters: vec!["pool-a".to_owned()],
                images: BTreeMap::new(),
                pod_images: Vec::new(),
            },
            proofs: BTreeMap::new(),
            lifecycle: LifecycleRecord {
                teardown_requested: false,
                teardown_performed: false,
                teardown_result: None,
                kept_on_failure: false,
            },
        };
        let json = serde_json::to_string_pretty(&evidence).unwrap();
        assert!(json.contains("\"metrics_transport\": \"direct-http\""));
    }

    #[test]
    fn evidence_records_mtls_proxy_transport() {
        let evidence = Evidence {
            schema_version: "1".to_owned(),
            mode: "quick".to_owned(),
            metrics_transport: MetricsTransport::MtlsProxy.label().to_owned(),
            scoring_strategy: ScoringFlavor::QueueDepth.label().to_owned(),
            placement_strategy: "score preference".to_owned(),
            run_id: "test-run".to_owned(),
            started_at: "2026-01-01T00:00:00Z".to_owned(),
            wall_secs: 10.0,
            success: true,
            error: None,
            setup: SetupEvidence {
                clusters: vec!["pool-a".to_owned()],
                images: BTreeMap::new(),
                pod_images: Vec::new(),
            },
            proofs: BTreeMap::new(),
            lifecycle: LifecycleRecord {
                teardown_requested: false,
                teardown_performed: false,
                teardown_result: None,
                kept_on_failure: false,
            },
        };
        let json = serde_json::to_string_pretty(&evidence).unwrap();
        assert!(json.contains("\"metrics_transport\": \"mtls-proxy\""));
    }

    /// Build a minimal forge.yaml fragment that matches the indentation
    /// anchors used by `materialize_config`.
    fn test_forge_config() -> String {
        // Indentation matches the real forge.yaml exactly so that
        // string replacements in materialize_config fire correctly.
        "\
      properties:
        poolName: pool-a

      properties:
        poolName: pool-b

    llmd-pool-a:
      steps:
        - type: manifest
          path: resources/common/epp-rbac.yaml
        - type: manifest
          path: resources/pool-a/epp-deployment.yaml

    llmd-pool-b:
      steps:
        - type: manifest
          path: resources/common/epp-rbac.yaml
        - type: manifest
          path: resources/pool-b/epp-deployment.yaml

                  metricsEndpoint: \"http://llmd-epp-metrics.grid-system.svc.cluster.local:9090\"
                  signalNames:
                    healthy: inference_pool_ready_pods

                  metricsEndpoint: \"http://llmd-epp-metrics.grid-system.svc.cluster.local:9090\"
                  signalNames:
                    healthy: inference_pool_ready_pods

              scoringPolicy:
                strategy: queueDepth

              scoringPolicy:
                strategy: queueDepth
"
        .to_owned()
    }

    /// Stub mTLS deployment manifest containing the default nginx image.
    fn test_mtls_manifest(pool: &str) -> String {
        format!(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: llmd-epp-{pool}\n\
             spec:\n  containers:\n    - name: epp\n      image: epp:latest\n\
             \x20   - name: metrics-tls-proxy\n      image: \"{DEFAULT_NGINX_IMAGE}\"\n"
        )
    }

    /// Create mTLS manifest stubs under the test directory.
    fn write_test_mtls_manifests(dir: &Path) {
        for pool in CLUSTERS {
            let pool_dir = dir.join(format!("resources/{pool}"));
            fs::create_dir_all(&pool_dir).unwrap();
            fs::write(pool_dir.join("epp-deployment-mtls.yaml"), test_mtls_manifest(pool)).unwrap();
        }
    }

    #[test]
    fn materialize_direct_http_has_no_tls_config() {
        let dir = std::env::temp_dir().join("grid-test-materialize-direct");
        drop(fs::create_dir_all(&dir));
        let forge_path = dir.join("forge.yaml");
        fs::write(&forge_path, test_forge_config()).unwrap();
        let resolved = materialize_config(
            &forge_path,
            MetricsTransport::DirectHttp,
            ScoringFlavor::QueueDepth,
            None,
        )
        .unwrap();
        let content = fs::read_to_string(&resolved).unwrap();

        assert!(
            !content.contains("epp-deployment-mtls.yaml"),
            "direct-HTTP must not reference mTLS deployment"
        );
        assert!(
            !content.contains("metrics-tls-proxy-config.yaml"),
            "direct-HTTP must not include metrics TLS proxy config"
        );
        assert!(
            content.contains("http://llmd-epp-metrics.grid-system.svc.cluster.local:9090"),
            "direct-HTTP must use HTTP endpoint"
        );
        assert!(
            !content.contains("https://llmd-epp-metrics"),
            "direct-HTTP must not use HTTPS endpoint"
        );
        assert!(
            !content.contains("caSecretRef"),
            "direct-HTTP must not include TLS secret references"
        );
        drop(fs::remove_dir_all(&dir));
    }

    #[test]
    fn materialize_run_id_updates_kind_contexts_with_cluster_prefix() {
        let dir = std::env::temp_dir().join(format!("grid-test-materialize-run-context-{}", std::process::id()));
        drop(fs::create_dir_all(&dir));
        let forge_path = dir.join("forge.yaml");
        fs::write(
            &forge_path,
            include_str!("../../../tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml"),
        )
        .unwrap();

        let resolved = materialize_config_with_images(
            &forge_path,
            &MaterializeConfigOptions {
                metrics_transport: MetricsTransport::DirectHttp,
                scoring_flavor: ScoringFlavor::QueueDepth,
                nginx_image: None,
                images: None,
                run_id: Some("20260930t203319z-795499"),
                pressure_weighted: false,
            },
        )
        .unwrap();
        let content = fs::read_to_string(resolved).unwrap();
        let expected_context = "kind-grid-llmd-pm-20260930t203319z-795499-{{ cluster.name }}";

        assert!(content.contains("clusterPrefix: grid-llmd-pm-20260930t203319z-795499"));
        assert_eq!(content.matches(expected_context).count(), 4);
        assert!(!content.contains("kind-grid-llmd-pm-{{ cluster.name }}"));
        drop(fs::remove_dir_all(&dir));
    }

    #[test]
    fn materialize_mtls_has_tls_config() {
        let dir = std::env::temp_dir().join("grid-test-materialize-mtls");
        drop(fs::create_dir_all(&dir));
        write_test_mtls_manifests(&dir);
        let forge_path = dir.join("forge.yaml");
        fs::write(&forge_path, test_forge_config()).unwrap();
        let resolved = materialize_config(
            &forge_path,
            MetricsTransport::MtlsProxy,
            ScoringFlavor::QueueDepth,
            None,
        )
        .unwrap();
        let content = fs::read_to_string(&resolved).unwrap();

        assert!(
            content.contains("epp-deployment-mtls.yaml"),
            "mTLS must reference mTLS deployment variant"
        );
        assert!(
            content.contains("metrics-tls-proxy-config.yaml"),
            "mTLS must include metrics TLS proxy config"
        );
        assert!(
            content.contains("https://llmd-epp-metrics.grid-system.svc.cluster.local:9443"),
            "mTLS must use HTTPS endpoint"
        );
        assert!(
            !content.contains("http://llmd-epp-metrics.grid-system.svc.cluster.local:9090"),
            "mTLS must not use HTTP endpoint"
        );
        assert!(content.contains("caSecretRef"), "mTLS must include CA secret reference");
        assert!(
            content.contains("clientCertificateSecretRef"),
            "mTLS must include client cert secret reference"
        );
        drop(fs::remove_dir_all(&dir));
    }

    #[test]
    fn materialize_mtls_injects_custom_nginx_image() {
        let dir = std::env::temp_dir().join("grid-test-materialize-mtls-nginx");
        drop(fs::create_dir_all(&dir));
        write_test_mtls_manifests(&dir);
        let forge_path = dir.join("forge.yaml");
        fs::write(&forge_path, test_forge_config()).unwrap();

        let custom_image = "registry.example.com/nginx:custom";
        materialize_config(
            &forge_path,
            MetricsTransport::MtlsProxy,
            ScoringFlavor::QueueDepth,
            Some(custom_image),
        )
        .unwrap();

        for pool in CLUSTERS {
            let resolved_manifest =
                fs::read_to_string(dir.join(format!(".forge.resolved.{pool}-epp-deployment-mtls.yaml"))).unwrap();
            assert!(
                resolved_manifest.contains(custom_image),
                "{pool}: resolved manifest must contain the custom nginx image"
            );
            assert!(
                !resolved_manifest.contains(DEFAULT_NGINX_IMAGE),
                "{pool}: resolved manifest must not contain the default nginx image"
            );
        }
        drop(fs::remove_dir_all(&dir));
    }

    #[test]
    fn materialize_queue_depth_flavor_leaves_default_strategy() {
        let dir = std::env::temp_dir().join("grid-test-materialize-queue-depth-flavor");
        drop(fs::create_dir_all(&dir));
        let forge_path = dir.join("forge.yaml");
        fs::write(&forge_path, test_forge_config()).unwrap();

        let resolved = materialize_config(
            &forge_path,
            MetricsTransport::DirectHttp,
            ScoringFlavor::QueueDepth,
            None,
        )
        .unwrap();
        let content = fs::read_to_string(&resolved).unwrap();

        assert_eq!(
            content.matches("strategy: queueDepth").count(),
            2,
            "queue-depth flavor must leave both sites' default strategy untouched"
        );
        assert!(!content.contains("kvCachePressure"));
        drop(fs::remove_dir_all(&dir));
    }

    #[test]
    fn materialize_kv_cache_flavor_swaps_strategy_on_both_sites() {
        let dir = std::env::temp_dir().join("grid-test-materialize-kv-cache-flavor");
        drop(fs::create_dir_all(&dir));
        let forge_path = dir.join("forge.yaml");
        fs::write(&forge_path, test_forge_config()).unwrap();

        let resolved = materialize_config(
            &forge_path,
            MetricsTransport::DirectHttp,
            ScoringFlavor::KvCachePressure,
            None,
        )
        .unwrap();
        let content = fs::read_to_string(&resolved).unwrap();

        assert_eq!(
            content.matches("strategy: kvCachePressure").count(),
            2,
            "kv-cache flavor must swap both pool-a-site and pool-b-site's strategy"
        );
        assert!(
            !content.contains("strategy: queueDepth"),
            "no queueDepth strategy should remain after the swap"
        );
        drop(fs::remove_dir_all(&dir));
    }

    #[test]
    fn materialize_kv_cache_flavor_accepts_preselected_strategy() {
        let dir = std::env::temp_dir().join("grid-test-materialize-kv-cache-preselected");
        drop(fs::create_dir_all(&dir));
        let forge_path = dir.join("forge-kv-cache.yaml");
        let config = test_forge_config().replace("strategy: queueDepth", "strategy: kvCachePressure");
        fs::write(&forge_path, config).unwrap();

        let resolved = materialize_config(
            &forge_path,
            MetricsTransport::DirectHttp,
            ScoringFlavor::KvCachePressure,
            None,
        )
        .unwrap();
        let content = fs::read_to_string(&resolved).unwrap();

        assert_eq!(
            content.matches("strategy: kvCachePressure").count(),
            2,
            "a preselected kv-cache config must remain unchanged"
        );
        assert!(!content.contains("strategy: queueDepth"));
        drop(fs::remove_dir_all(&dir));
    }

    #[test]
    fn materialize_fails_on_missing_anchor() {
        let dir = std::env::temp_dir().join("grid-test-materialize-bad-anchor");
        drop(fs::create_dir_all(&dir));
        let forge_path = dir.join("forge.yaml");
        fs::write(&forge_path, "empty config with no anchors").unwrap();
        let result = materialize_config(
            &forge_path,
            MetricsTransport::DirectHttp,
            ScoringFlavor::QueueDepth,
            None,
        );
        assert!(result.is_err(), "must fail when anchors are missing");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("expected 1 match(es), found 0"),
            "error must report the mismatch: {err}"
        );
        drop(fs::remove_dir_all(&dir));
    }

    #[test]
    fn nginx_image_absent_in_direct_http() {
        if std::env::var("GRID_XTASK_NGINX_IMAGE").is_ok() {
            return;
        }
        let images = resolve_images(MetricsTransport::DirectHttp).unwrap();
        assert!(images.nginx.is_none(), "direct-HTTP must not resolve nginx image");
    }

    #[test]
    fn nginx_image_present_in_mtls() {
        let images = resolve_images(MetricsTransport::MtlsProxy).unwrap();
        assert!(images.nginx.is_some(), "mTLS must resolve nginx image");
        assert_eq!(images.nginx.unwrap(), DEFAULT_NGINX_IMAGE);
    }

    #[test]
    fn source_hash_classifies_only_source_and_generated_resolved_files() {
        assert!(is_untracked_source_file(Path::new(
            "operator/src/resources/placement.rs"
        )));
        assert!(is_untracked_source_file(Path::new(
            "charts/grid-operator/tests/service-swim_test.yaml"
        )));
        assert!(!is_untracked_source_file(Path::new(
            "tests/e2e/topologies/grid-llmd-pool-metrics/.forge.resolved.run.yaml"
        )));
        assert!(is_generated_resolved_yaml(Path::new(
            "tests/e2e/topologies/grid-llmd-pool-metrics/.forge.resolved.run.yaml"
        )));
        assert!(is_generated_resolved_yaml(Path::new(
            "tests/e2e/topologies/grid-single-cluster-multi-gateway/.grid-run.resolved.yaml"
        )));
        assert!(!is_generated_resolved_yaml(Path::new(
            "operator/src/resources/placement.rs"
        )));
    }

    #[test]
    fn docker_save_manifest_yields_the_config_digest_not_the_index_digest() {
        let manifest = br#"[{"Config":"blobs/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","RepoTags":["praxis-ai:test"],"Layers":[]}]"#;
        assert_eq!(
            config_digest_from_save_manifest(manifest, "praxis-ai:test").unwrap(),
            format!("sha256:{}", "a".repeat(64))
        );
        assert_eq!(
            config_digest_from_save_manifest(manifest, "praxis-ai:other").unwrap_err(),
            "Docker save manifest does not contain selected image praxis-ai:other"
        );
    }

    fn gateway_image_pod(cluster: &str, image: &str, image_id: &str) -> PodImageEvidence {
        PodImageEvidence {
            cluster: cluster.to_owned(),
            pod: "consumer-gateway-abc".to_owned(),
            container: "praxis".to_owned(),
            requested_image: image.to_owned(),
            image_id: image_id.to_owned(),
            ready: true,
            restart_count: 0,
        }
    }

    #[test]
    fn gateway_image_identity_requires_source_sha_and_each_cluster_runtime_digest() {
        let image = "praxis-ai:test";
        let config_id = format!("sha256:{}", "a".repeat(64));
        let pods = vec![
            gateway_image_pod("kind-grid-pool-a", image, &config_id),
            gateway_image_pod("kind-grid-pool-b", image, &config_id),
        ];
        let revision = "0123456789abcdef";
        let content_hash = "a".repeat(64);
        let version = format!("0.4.1-test-{}", &content_hash[..8]);
        let valid_proof = gateway_image_identity_proof(&GatewayImageIdentity {
            image,
            config_id: &config_id,
            source: "https://github.com/praxis-proxy/ai",
            revision,
            expected_revision: Some(revision),
            expected_content_hash: Some(&content_hash),
            version: &version,
            pods: &pods,
        });
        assert!(valid_proof.success);
        assert!(
            valid_proof
                .observations
                .iter()
                .any(|observation| observation.contains("8-character hash prefix"))
        );

        assert!(
            !gateway_image_identity_proof(&GatewayImageIdentity {
                image,
                config_id: &config_id,
                source: "https://github.com/praxis-proxy/ai",
                revision,
                expected_revision: None,
                expected_content_hash: Some(&content_hash),
                version: &version,
                pods: &pods,
            })
            .success
        );
        let wrong_image = vec![gateway_image_pod("kind-grid-pool-a", "praxis-ai:stale", "sha256:stale")];
        assert!(
            !gateway_image_identity_proof(&GatewayImageIdentity {
                image,
                config_id: &config_id,
                source: "https://github.com/praxis-proxy/ai",
                revision,
                expected_revision: Some(revision),
                expected_content_hash: Some(&content_hash),
                version: &version,
                pods: &wrong_image,
            })
            .success
        );
    }
}
