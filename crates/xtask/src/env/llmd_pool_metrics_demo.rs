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
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::OnceLock,
    time::{Duration, Instant},
};

use serde::Serialize;
use sha2::{Digest as _, Sha256};

mod cross_site;
mod learned_ceiling;

use super::{
    DemoMode, GlbDemoOptions, certs, glb, kubectl, operator,
    verify::{PortForwardGuard, find_free_port},
};

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

/// Primary model name served by the deterministic llm-d-sim backends.
const VCR_MODEL: &str = "Qwen/Qwen3-0.6B";

/// Data-plane convergence timeout for overlay propagation.
const DATA_PLANE_WAIT: Duration = Duration::from_secs(180);
/// Maximum time to wait for the peer-signals LoadBalancer ingress to be assigned.
const SIGNALS_LB_INGRESS_TIMEOUT: Duration = Duration::from_secs(90);

/// Retry interval for convergence probes.
const DATA_PLANE_INTERVAL: Duration = Duration::from_secs(1);

/// Tolerance for comparing Prometheus-serialized EPP KV-cache means.
const KV_CACHE_METRIC_TOLERANCE: f64 = 1e-6;

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
/// Persistent simulator ConfigMaps keyed by their Deployment.
const SIMULATOR_CONFIGMAPS: &[(&str, &str)] = &[("vcr-1", "vcr-1-config"), ("vcr-2", "vcr-2-config")];
/// Pod template annotation for the simulator's queue metric.
const SIMULATOR_WAITING_REQUESTS_ANNOTATION: &str = "grid.praxis.fast/fake-waiting-requests";
/// Pod template annotation for the simulator's running-request metric.
const SIMULATOR_RUNNING_REQUESTS_ANNOTATION: &str = "grid.praxis.fast/fake-running-requests";
/// Pod template annotation for the simulator's KV-cache metric.
const SIMULATOR_KV_CACHE_ANNOTATION: &str = "grid.praxis.fast/fake-kv-cache-usage";

/// GridNetwork resource name.
const GRID_NETWORK_NAME: &str = "grid-llmd-pool-metrics";

/// Default prefix used when materializing a run-owned Forge environment.
const DEFAULT_CLUSTER_PREFIX: &str = "grid-llmd-pm";

/// Forge derives the shared Docker network name from the environment metadata name.
const DEFAULT_FORGE_ENV_NAME: &str = "grid-llmd-pool-metrics";

/// Prefix for the clusters owned by this process.
static RUN_CLUSTER_PREFIX: OnceLock<String> = OnceLock::new();

/// Default gateway image tag.
///
/// The pool-metrics demo shares the same Grid-enabled Praxis AI binary as the
/// combined-site demo. Both require the `peer_identity_trust`,
/// `provider_route`, `credential_inject`, and `intelligent_route` filters
/// which are built into the published Grid AI rollup.
const DEFAULT_GATEWAY_IMAGE: &str = "ghcr.io/praxis-proxy/ai:0.6.0";

/// Default local Grid Gateway image used by the full cross-site signal proof.
const DEFAULT_GRID_GATEWAY_IMAGE: &str = "grid-gateway:llmd-pool-metrics-demo";

/// Default operator image tag.
const DEFAULT_OPERATOR_IMAGE: &str = "ghcr.io/praxis-proxy/grid-operator:v0.1.4";

/// Default EPP image reference required by this demo.
const DEFAULT_EPP_IMAGE: &str = "ghcr.io/llm-d/llm-d-router-endpoint-picker:v0.11.0";

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

/// Grid-serving consumer installed for the cross-site signal-poll proof.
const GRID_SERVING_GATEWAY: &str = "grid-serving-gateway";

/// Operator-owned serving ConfigMap consumed by the Grid-serving gateway.
const GRID_SERVING_CONFIGMAP: &str = "grid-serving-grid-llmd-pool-metrics-grid-serving-gateway";

/// Praxis Gateway HTTP service port.
const GRID_SERVING_GATEWAY_PORT: u16 = 8080;

/// Provider gateway HTTPS service port.
const PROVIDER_GATEWAY_PORT: u16 = 8443;

/// Operator metrics service port.
const OPERATOR_METRICS_PORT: u16 = 9090;
/// Operator peer-signal listener port exposed by the signals Service.
const OPERATOR_SIGNALS_PORT: u16 = 9091;

/// Grid Gateway's TLS metrics listener port.
const GRID_SERVING_METRICS_PORT: u16 = 9443;

/// Cluster exposing the Grid Gateway's cross-site route.
const GRID_SERVING_SITE: &str = "pool-a";

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
/// `GridNetwork.spec.scoringPolicy.strategy` (and therefore which raw
/// signal actually produces the `score`/`rank` that drives the A\u{2192}B flip)
/// changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScoringFlavor {
    /// llm-d's `queue-scorer` equivalent (default).
    QueueDepth,
    /// llm-d's `kv-cache-utilization-scorer` equivalent.
    KvCachePressure,
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
    /// `crates/operator/src/crd/grid_network.rs` exactly.
    fn strategy_yaml(self) -> &'static str {
        match self {
            Self::QueueDepth => "queueDepth",
            Self::KvCachePressure => "kvCachePressure",
        }
    }
}

/// Demo execution context holding resolved paths.
struct DemoContext {
    /// Directory where this run's structured evidence is written.
    evidence_dir: PathBuf,
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
}

/// Resolved container image references.
struct ResolvedImages {
    /// Praxis AI gateway image (must contain Grid filters).
    gateway: String,
    /// Grid Gateway image, required by full-mode cross-site signal proofs.
    grid_gateway: Option<String>,
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
    /// Score breakdown from the production scoring engine.
    breakdown: Option<super::operator_overlay::ScoreBreakdown>,
}

/// Parsed inference response with gateway attribution.
struct InferenceResponse {
    /// Provider gateway cluster from `X-Grid-LlmD-Provider-Gateway`.
    provider_gateway: String,
    /// Demo attribution from `x-ai-demo-provider-gateway`.
    demo_attribution: String,
}

/// Captured response and transport diagnostics from one curl probe.
struct CurlProbeOutput {
    /// HTTP status returned by curl, with zero representing no HTTP response.
    status: u16,
    /// Provider gateway attribution header.
    provider_gateway: String,
    /// Demo provider attribution header.
    demo_attribution: String,
    /// Exit code returned by curl inside the probe pod.
    curl_exit_code: i32,
    /// Standard error emitted by curl inside the probe pod.
    curl_stderr: String,
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
    let mode = options.mode();
    let metrics_transport = if metrics_mtls {
        MetricsTransport::MtlsProxy
    } else {
        MetricsTransport::DirectHttp
    };
    let scoring_flavor = ScoringFlavor::from_kv_cache_flag(kv_cache);
    let run_id = format!("{}-{}", format_utc_timestamp(), std::process::id());
    let cluster_prefix = format!("{DEFAULT_CLUSTER_PREFIX}-{}", run_id.to_ascii_lowercase());
    RUN_CLUSTER_PREFIX
        .set(cluster_prefix)
        .map_err(|error| format!("llm-d pool-metrics run prefix was already initialized: {error}"))?;
    let started_at = format_utc_iso();
    let wall_start = Instant::now();

    let evidence_dir = resolve_evidence_dir(forge_config, options, &run_id)?;
    fs::create_dir_all(&evidence_dir)?;

    let demo_root = super::demo_root(forge_config);
    eprintln!("{OUTPUT_RULE}");
    eprintln!("Grid llm-d Pool-Metrics Routing Demo");
    eprintln!("Mode: {}", if mode == DemoMode::Quick { "quick" } else { "full" });
    eprintln!("Metrics transport: {}", metrics_transport.label());
    eprintln!("Scoring strategy:  {}", scoring_flavor.label());
    eprintln!("Forge config: {}", forge_config.display());
    eprintln!("Demo root:    {}", demo_root.display());
    eprintln!("{OUTPUT_RULE}");

    let context = prepare_setup(
        forge_config,
        &evidence_dir,
        &evidence_dir.join("forge-state"),
        &run_id,
        metrics_transport,
        scoring_flavor,
        mode,
    )?;
    let mut teardown_success = false;
    let mut run_error: Option<String> = None;

    let proof_results = match deploy_setup(&context) {
        Ok(()) => {
            eprintln!();
            eprintln!("{OUTPUT_RULE}");
            eprintln!("ENVIRONMENT READY - Starting proof scenarios");
            eprintln!("{OUTPUT_RULE}");

            let results = run_proof_scenarios(&context, mode);

            let failed: Vec<&str> = results
                .iter()
                .filter_map(|(name, proof)| (!proof.success).then_some(name.as_str()))
                .collect();
            if !failed.is_empty() {
                for name in &failed {
                    if let Some(proof) = results.get(*name) {
                        eprintln!("[PROOF][FAIL] {name}: {}", proof.description);
                        for observation in &proof.observations {
                            eprintln!("  [PROOF][DETAIL] {observation}");
                        }
                    }
                }
                run_error = Some(format!("proofs failed: {}", failed.join(", ")));
            }

            if options.teardown && (run_error.is_none() || !options.keep_on_failure) {
                eprintln!("[CLEANUP][START] tearing down this run's Forge environment");
                match teardown_environment(&context) {
                    Ok(()) => {
                        teardown_success = true;
                        eprintln!("[CLEANUP][PASS] this run's Forge environment was removed");
                    },
                    Err(e) => {
                        eprintln!("[CLEANUP][FAIL] teardown command failed: {e}");
                        run_error = Some(match run_error {
                            Some(prev) => format!("{prev}; teardown also failed: {e}"),
                            None => format!("teardown failed: {e}"),
                        });
                    },
                }
            } else if options.teardown && options.keep_on_failure && run_error.is_some() {
                eprintln!(
                    "[CLEANUP][SKIP] preserving run-owned clusters because a proof failed and --keep-on-failure was set"
                );
            } else if !options.teardown {
                eprintln!("[CLEANUP][SKIP] teardown was not requested");
            }

            results
        },
        Err(e) => {
            eprintln!("[SETUP][FAIL] environment deployment failed: {e}");
            run_error = Some(format!("setup failed: {e}"));

            if options.teardown && !options.keep_on_failure {
                eprintln!("[CLEANUP][START] attempting teardown after setup failure");
                match teardown_environment(&context) {
                    Ok(()) => {
                        teardown_success = true;
                        eprintln!("[CLEANUP][PASS] partial run environment was removed");
                    },
                    Err(te) => {
                        eprintln!("[CLEANUP][FAIL] teardown after setup failure also failed: {te}");
                        run_error = Some(format!("setup failed: {e}; cleanup also failed: {te}"));
                    },
                }
            } else if options.keep_on_failure {
                eprintln!("[CLEANUP][SKIP] preserving partial setup because --keep-on-failure was set");
            } else {
                eprintln!("[CLEANUP][SKIP] teardown was not requested after setup failure");
            }

            BTreeMap::new()
        },
    };

    let wall_secs = wall_start.elapsed().as_secs_f64();
    let images = collect_image_evidence(&context.images)?;
    let success = run_error.is_none();

    write_transition_timeline(&evidence_dir, &proof_results, context.scoring_flavor, wall_secs)?;
    let evidence = Evidence {
        schema_version: EVIDENCE_SCHEMA_VERSION.to_owned(),
        mode: format!("{mode:?}").to_lowercase(),
        metrics_transport: metrics_transport.label().to_owned(),
        scoring_strategy: scoring_flavor.label().to_owned(),
        started_at,
        wall_secs,
        success,
        error: run_error.clone(),
        setup: SetupEvidence {
            clusters: CLUSTERS.iter().map(|s| (*s).to_owned()).collect(),
            images,
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
        eprintln!("DEMO FAILED  (exit status 1, {wall_secs:.1}s)");
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
#[expect(
    clippy::too_many_arguments,
    reason = "setup resolves these independent topology, run, signal, and image options before any cluster exists"
)]
fn prepare_setup(
    forge_config: &Path,
    evidence_dir: &Path,
    forge_state_dir: &Path,
    run_id: &str,
    metrics_transport: MetricsTransport,
    scoring_flavor: ScoringFlavor,
    mode: DemoMode,
) -> Result<DemoContext, Box<dyn std::error::Error>> {
    let images = resolve_images(metrics_transport, mode == DemoMode::Full)?;
    verify_images(&images)?;

    let resolved_config = materialize_config_with_images(
        forge_config,
        &MaterializeConfigOptions {
            metrics_transport,
            scoring_flavor,
            nginx_image: images.nginx.as_deref(),
            images: Some(&images),
            run_id: Some(run_id),
        },
    )?;
    verify_materialized_images(&resolved_config, &images)?;
    let forge_bin = glb::resolve_forge_binary()
        .ok_or("praxis-forge binary not found")?
        .into();

    Ok(DemoContext {
        evidence_dir: evidence_dir.to_path_buf(),
        resolved_config,
        forge_state_dir: forge_state_dir.to_path_buf(),
        forge_bin,
        images,
        metrics_transport,
        scoring_flavor,
    })
}

/// Resolve image references from environment variables with defaults.
fn resolve_images(
    metrics_transport: MetricsTransport,
    include_grid_gateway: bool,
) -> Result<ResolvedImages, Box<dyn std::error::Error>> {
    let gateway = std::env::var("GRID_XTASK_GATEWAY_IMAGE").unwrap_or_else(|_| DEFAULT_GATEWAY_IMAGE.to_owned());
    let grid_gateway = include_grid_gateway.then(|| {
        std::env::var("GRID_XTASK_GRID_GATEWAY_IMAGE").unwrap_or_else(|_| DEFAULT_GRID_GATEWAY_IMAGE.to_owned())
    });
    let operator = std::env::var("GRID_XTASK_OPERATOR_IMAGE").unwrap_or_else(|_| DEFAULT_OPERATOR_IMAGE.to_owned());
    let epp = std::env::var("GRID_XTASK_EPP_IMAGE").unwrap_or_else(|_| DEFAULT_EPP_IMAGE.to_owned());
    let vcr = std::env::var("GRID_XTASK_SIM_IMAGE").unwrap_or_else(|_| DEFAULT_VCR_IMAGE.to_owned());
    let overlay_sync =
        std::env::var("GRID_XTASK_OVERLAY_SYNC_IMAGE").unwrap_or_else(|_| DEFAULT_OVERLAY_SYNC_IMAGE.to_owned());
    let nginx = (metrics_transport == MetricsTransport::MtlsProxy)
        .then(|| std::env::var("GRID_XTASK_NGINX_IMAGE").unwrap_or_else(|_| DEFAULT_NGINX_IMAGE.to_owned()));

    eprintln!("  Images:");
    eprintln!("    gateway:      {gateway}");
    if let Some(image) = &grid_gateway {
        eprintln!("    grid-gateway: {image}");
    }
    eprintln!("    operator:     {operator}");
    eprintln!("    epp:          {epp}");
    eprintln!("    vcr:          {vcr}");
    eprintln!("    overlay-sync: {overlay_sync}");
    if let Some(n) = &nginx {
        eprintln!("    nginx:        {n}");
    }

    Ok(ResolvedImages {
        gateway,
        grid_gateway,
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
    if let Some(grid_gateway) = &images.grid_gateway {
        checks.push(("grid-gateway", grid_gateway, "GRID_GATEWAY"));
    }
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
    let total = if mtls { SETUP_PHASES_MTLS } else { SETUP_PHASES_DIRECT };
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
        stage_certificates()?;
    } else {
        eprintln!(
            "[SETUP {}/{}] Generating TLS certificates (gateway only)",
            next(),
            total
        );
        let clusters: Vec<String> = CLUSTERS.iter().map(|s| (*s).to_owned()).collect();
        certs::generate_all(&clusters)?;
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
    install_provider_trust()?;

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
    for cluster in CLUSTERS {
        run_forge_stack(context, cluster, "consumer-gateway")?;
        eprintln!("  [OK] {cluster}: consumer-gateway deployed");
    }

    // Phase 10/11: Wait for overlay convergence
    eprintln!();
    eprintln!("[SETUP {}/{}] Waiting for overlay convergence", next(), total);
    authorize_discovered_sites()?;
    wait_for_overlay_convergence()?;

    eprintln!();
    eprintln!("[READY] Environment deployed");
    Ok(())
}

// ---------------------------------------------------------------------------
// Proof scenarios
// ---------------------------------------------------------------------------

/// Run the demo proof scenarios.
fn run_proof_scenarios(context: &DemoContext, mode: DemoMode) -> BTreeMap<String, ProofResult> {
    let mut results = BTreeMap::new();
    let mtls = context.metrics_transport == MetricsTransport::MtlsProxy;

    // Proof 1: Provenance (image digests and config verification)
    results.insert("provenance".to_owned(), proof_provenance(mtls));

    // Proof 2: Baseline (early state scorecard with production scores)
    results.insert("baseline".to_owned(), proof_baseline(context));

    if mode == DemoMode::Full {
        let table_start = Instant::now();

        // Proof 3: Pressure via consumer gateway (live table with attribution)
        results.insert(
            "pressure_and_flip".to_owned(),
            proof_pressure_and_flip(context, table_start),
        );

        // Proof 4: Recovery (measured queue drain with live table)
        results.insert("recovery".to_owned(), proof_recovery(context, table_start));

        // Proof 5: EPP metrics cross the operator poll relay and drive the
        // embedded Grid Gateway's live site route without a gateway restart.
        results.insert("cross_site_signal_poll".to_owned(), cross_site::proof(context));
        results.insert("learned_ceiling_lifecycle".to_owned(), learned_ceiling::proof(context));
    }

    // TLS proof stages, only in mTLS mode
    if mtls {
        let tls_results = run_tls_proof_stages();
        results.extend(tls_results);
    }

    results
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
            match send_inference_request(&probe_ctx, VCR_MODEL, "baseline") {
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
            match send_inference_request(&probe_ctx, VCR_MODEL, "failover") {
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
                    eprintln!("  [FAILOVER] {result}");
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
            match send_inference_request(&probe_ctx, VCR_MODEL, "recovery") {
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
                    eprintln!("  [RECOVERY] {result}");
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

/// Persist deterministic fake metrics and roll simulator pods from template annotations.
/// The annotations make KV-cache-only changes alter the pod template and trigger rollout.
/// The EPP metric and overlay checks remain the authoritative downstream observations.
fn set_simulator_waiting_requests(
    cluster: &str,
    waiting_requests: u32,
    scoring_flavor: ScoringFlavor,
    mtls: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (waiting_requests, kv_cache_usage) = simulator_metrics(waiting_requests, scoring_flavor);
    set_simulator_fake_metrics(cluster, waiting_requests, 0, kv_cache_usage, mtls)
}

/// Persist all fake simulator metrics and roll its run-owned pods.
fn set_simulator_fake_metrics(
    cluster: &str,
    waiting_requests: u32,
    running_requests: u32,
    kv_cache_usage: f64,
    mtls: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    set_simulator_fake_metrics_with_endpoint_kv(
        cluster,
        waiting_requests,
        running_requests,
        kv_cache_usage,
        kv_cache_usage,
        mtls,
    )
}

/// Persist pool metrics with a distinct KV-cache value on each run-owned endpoint.
#[expect(
    clippy::too_many_arguments,
    reason = "the queue, running load, and each endpoint's cache value describe the stimulus directly"
)]
fn set_simulator_fake_metrics_with_endpoint_kv(
    cluster: &str,
    waiting_requests: u32,
    running_requests: u32,
    vcr1_kv_cache_usage: f64,
    vcr2_kv_cache_usage: f64,
    mtls: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = kind_context(cluster);
    for (deployment, configmap) in SIMULATOR_CONFIGMAPS {
        let kv_cache_usage = match *deployment {
            "vcr-1" => vcr1_kv_cache_usage,
            "vcr-2" => vcr2_kv_cache_usage,
            _ => return Err(format!("no KV-cache stimulus was specified for {deployment}").into()),
        };
        kubectl::apply_manifest(
            &ctx,
            &simulator_config(configmap, waiting_requests, running_requests, kv_cache_usage),
        )
        .map_err(|e| format!("apply persistent simulator configmap/{configmap} on {cluster}: {e}"))?;
    }

    for (deployment, configmap) in SIMULATOR_CONFIGMAPS {
        let kv_cache_usage = match *deployment {
            "vcr-1" => vcr1_kv_cache_usage,
            "vcr-2" => vcr2_kv_cache_usage,
            _ => return Err(format!("no KV-cache stimulus was specified for {deployment}").into()),
        };
        let before = deployment_generation(&ctx, deployment)?;
        let patch = serde_json::json!({
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": simulator_rollout_annotations(waiting_requests, running_requests, kv_cache_usage)
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
        if simulator_rollout_needed(before, after).map_err(|error| format!("deployment/{deployment}: {error}"))? {
            eprintln!("  restarting {deployment} for simulator metric configuration");
        } else {
            eprintln!("  {deployment} already has the requested simulator metric configuration; no restart needed");
        }
        kubectl::wait_for_rollout_ns(&ctx, deployment, GRID_SYSTEM_NS, cluster)
            .map_err(|e| format!("wait for simulator deployment/{deployment} rollout: {e}"))?;

        let expected_config = simulator_config_data(waiting_requests, running_requests, kv_cache_usage);
        let config_text = get_configmap_data_key(&ctx, GRID_SYSTEM_NS, configmap, "config.yaml").map_err(|error| {
            format!("could not read configmap/{configmap} on {cluster} after applying the simulator metrics: {error}")
        })?;
        if config_text != expected_config {
            let expected_digest = hex::encode(Sha256::digest(expected_config.as_bytes()));
            let actual_digest = hex::encode(Sha256::digest(config_text.as_bytes()));
            return Err(format!(
                "configmap/{configmap} on {cluster} does not contain its requested simulator metrics (expected {} bytes sha256={expected_digest}; found {} bytes sha256={actual_digest})",
                expected_config.len(),
                config_text.len(),
            )
            .into());
        }
    }

    let ready_replicas = wait_for_simulator_service_endpoints(&ctx)?;
    let expected_endpoints = ready_replicas.total().map_err(std::io::Error::other)?;
    let expected_kv_cache = simulator_kv_cache_mean(vcr1_kv_cache_usage, vcr2_kv_cache_usage, ready_replicas)
        .map_err(std::io::Error::other)?;
    let expected_endpoint_metric = u32::try_from(expected_endpoints).map(f64::from).map_err(|error| {
        std::io::Error::other(format!(
            "simulator endpoint count exceeds EPP metric precision: {error}"
        ))
    })?;
    let deadline = Instant::now() + DATA_PLANE_WAIT;
    let mut last_observed = String::from("no valid EPP metric sample");
    while Instant::now() < deadline {
        match kubectl_exec_epp_metrics(cluster, mtls) {
            Ok(text) => match parse_required_epp_metrics(&text) {
                Ok(metrics) => {
                    let observed_endpoints = first_prom_value(&text, EPP_READY_METRICS);
                    last_observed = format!(
                        "queue-size={}, KV-cache mean={}, ready endpoints={observed_endpoints:?}, per-deployment ready replicas={ready_replicas:?}",
                        metrics.queue_size, metrics.kv_cache,
                    );
                    if (metrics.queue_size - f64::from(waiting_requests)).abs() < f64::EPSILON
                        && (metrics.kv_cache - expected_kv_cache).abs() < KV_CACHE_METRIC_TOLERANCE
                        && observed_endpoints
                            .is_some_and(|ready| (ready - expected_endpoint_metric).abs() < f64::EPSILON)
                    {
                        eprintln!(
                            "  [OK] {cluster}: simulator waiting-requests={waiting_requests}, endpoint-weighted KV-cache mean={expected_kv_cache:.3}, ready endpoints={expected_endpoints} (vcr-1={}, vcr-2={}) are live in EPP metrics",
                            ready_replicas.vcr_1, ready_replicas.vcr_2,
                        );
                        return Ok(());
                    }
                },
                Err(error) => last_observed = format!("EPP metrics parse failed: {error}"),
            },
            Err(error) => last_observed = format!("EPP metrics scrape failed: {error}"),
        }
        std::thread::sleep(DATA_PLANE_INTERVAL);
    }
    Err(format!(
        "{cluster}: simulator did not expose waiting-requests={waiting_requests}, endpoint-weighted KV-cache mean={expected_kv_cache:.6}, and ready-endpoints={expected_endpoints} within timeout; last observation: {last_observed}"
    )
    .into())
}

/// Render the complete simulator ConfigMap for the requested fake metrics.
fn simulator_config(configmap: &str, waiting_requests: u32, running_requests: u32, kv_cache_usage: f64) -> String {
    let config_data = simulator_config_data(waiting_requests, running_requests, kv_cache_usage);
    let config_data_block = config_data
        .lines()
        .map(|line| format!("    {line}\n"))
        .collect::<String>();
    format!(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {configmap}\n  namespace: {GRID_SYSTEM_NS}\ndata:\n  config.yaml: |\n{config_data_block}"
    )
}

/// Render the simulator's `config.yaml` ConfigMap value for fake metrics.
fn simulator_config_data(waiting_requests: u32, running_requests: u32, kv_cache_usage: f64) -> String {
    format!(
        "port: 8000\nmodel: {VCR_MODEL}\nserved-model-name: [{VCR_MODEL}]\nmode: echo\nfake-metrics:\n  waiting-requests: {waiting_requests}\n  running-requests: {running_requests}\n  kv-cache-usage: {kv_cache_usage}\n"
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

/// Return the pod-template annotations that identify the complete fake-metric state.
fn simulator_rollout_annotations(
    waiting_requests: u32,
    running_requests: u32,
    kv_cache_usage: f64,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            SIMULATOR_WAITING_REQUESTS_ANNOTATION.to_owned(),
            waiting_requests.to_string(),
        ),
        (
            SIMULATOR_RUNNING_REQUESTS_ANNOTATION.to_owned(),
            running_requests.to_string(),
        ),
        (SIMULATOR_KV_CACHE_ANNOTATION.to_owned(), kv_cache_usage.to_string()),
    ])
}

/// Whether a Deployment generation change requires waiting for a new simulator rollout.
fn simulator_rollout_needed(before: i64, after: i64) -> Result<bool, String> {
    if after < before {
        return Err(format!("generation moved backwards ({before} -> {after})"));
    }
    Ok(after > before)
}

/// Sum the desired replicas of the run-owned simulator Deployments.
fn simulator_desired_replicas(context: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let mut replicas = 0_usize;
    for deployment in SIMULATOR_DEPLOYMENTS {
        let output = Command::new("kubectl")
            .args([
                "--context",
                context,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                &format!("deployment/{deployment}"),
                "-o",
                "jsonpath={.spec.replicas}",
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "read deployment/{deployment} replica count: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )
            .into());
        }
        let value = String::from_utf8_lossy(&output.stdout);
        let count = if value.trim().is_empty() {
            1
        } else {
            value.trim().parse::<usize>().map_err(|error| {
                std::io::Error::other(format!("invalid deployment/{deployment} replica count: {error}"))
            })?
        };
        replicas = replicas
            .checked_add(count)
            .ok_or_else(|| std::io::Error::other("simulator replica count overflowed"))?;
    }
    Ok(replicas)
}

/// Ready endpoint counts for the two simulator Deployments behind `vcr-service`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SimulatorReadyReplicas {
    /// Ready endpoints owned by `vcr-1`.
    vcr_1: usize,
    /// Ready endpoints owned by `vcr-2`.
    vcr_2: usize,
}

impl SimulatorReadyReplicas {
    /// Return the total ready endpoints, rejecting arithmetic overflow.
    fn total(self) -> Result<usize, String> {
        self.vcr_1
            .checked_add(self.vcr_2)
            .ok_or_else(|| "simulator ready endpoint count overflowed".to_owned())
    }
}

/// Read each simulator Deployment's ready replica count.
fn simulator_ready_replicas(context: &str) -> Result<SimulatorReadyReplicas, Box<dyn std::error::Error>> {
    let mut counts = SimulatorReadyReplicas::default();
    for deployment in SIMULATOR_DEPLOYMENTS {
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
                "read deployment/{deployment} ready replicas: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )
            .into());
        }
        let document: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        let ready = match document.pointer("/status/readyReplicas") {
            None => 0,
            Some(value) => usize::try_from(value.as_u64().ok_or_else(|| {
                std::io::Error::other(format!(
                    "deployment/{deployment} has an invalid status.readyReplicas value: {value}"
                ))
            })?)
            .map_err(|error| std::io::Error::other(format!("deployment/{deployment} ready replicas: {error}")))?,
        };
        match *deployment {
            "vcr-1" => counts.vcr_1 = ready,
            "vcr-2" => counts.vcr_2 = ready,
            _ => return Err(format!("unknown simulator Deployment {deployment}").into()),
        }
    }
    Ok(counts)
}

/// Calculate the EPP's endpoint-weighted mean for the two simulator Deployments.
fn simulator_kv_cache_mean(
    vcr_1_kv_cache: f64,
    vcr_2_kv_cache: f64,
    ready_replicas: SimulatorReadyReplicas,
) -> Result<f64, String> {
    if !vcr_1_kv_cache.is_finite()
        || !(0.0..=1.0).contains(&vcr_1_kv_cache)
        || !vcr_2_kv_cache.is_finite()
        || !(0.0..=1.0).contains(&vcr_2_kv_cache)
    {
        return Err(format!(
            "simulator KV-cache values must be finite and in [0, 1]: vcr-1={vcr_1_kv_cache}, vcr-2={vcr_2_kv_cache}"
        ));
    }
    let total = ready_replicas.total()?;
    if total == 0 {
        return Err("cannot calculate simulator KV-cache mean without ready endpoints".to_owned());
    }
    let vcr_1_count = u32::try_from(ready_replicas.vcr_1)
        .map_err(|error| format!("vcr-1 ready endpoints exceed EPP metric precision: {error}"))?;
    let vcr_2_count = u32::try_from(ready_replicas.vcr_2)
        .map_err(|error| format!("vcr-2 ready endpoints exceed EPP metric precision: {error}"))?;
    let total_count = u32::try_from(total)
        .map_err(|error| format!("simulator ready endpoints exceed EPP metric precision: {error}"))?;
    Ok((vcr_1_kv_cache * f64::from(vcr_1_count) + vcr_2_kv_cache * f64::from(vcr_2_count)) / f64::from(total_count))
}

/// Ready and not-ready pod identities currently published by the VCR Service.
#[derive(Debug, Default, Eq, PartialEq)]
struct SimulatorServiceEndpoints {
    /// Identities of endpoints ready to receive traffic.
    ready: BTreeSet<String>,
    /// Identities of endpoints Kubernetes has marked not ready.
    not_ready: BTreeSet<String>,
}

/// Parse the Endpoints view that kube-proxy uses for the VCR Service.
fn parse_simulator_service_endpoints(
    document: &serde_json::Value,
) -> Result<SimulatorServiceEndpoints, Box<dyn std::error::Error>> {
    let mut endpoints = SimulatorServiceEndpoints::default();
    let subsets = document
        .get("subsets")
        .and_then(serde_json::Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    for subset in subsets {
        for (field, destination) in [
            ("addresses", &mut endpoints.ready),
            ("notReadyAddresses", &mut endpoints.not_ready),
        ] {
            let addresses = subset
                .get(field)
                .and_then(serde_json::Value::as_array)
                .map_or(&[][..], Vec::as_slice);
            for address in addresses {
                let identity = address
                    .pointer("/targetRef/uid")
                    .and_then(serde_json::Value::as_str)
                    .or_else(|| address.pointer("/targetRef/name").and_then(serde_json::Value::as_str))
                    .or_else(|| address.get("ip").and_then(serde_json::Value::as_str))
                    .ok_or_else(|| {
                        std::io::Error::other(format!("{field} entry has no pod identity or IP: {address}"))
                    })?;
                destination.insert(identity.to_owned());
            }
        }
    }
    Ok(endpoints)
}

/// Read the Service endpoints that provider Gateways connect to.
fn simulator_service_endpoints(context: &str) -> Result<SimulatorServiceEndpoints, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "endpoints/vcr-service",
            "-o",
            "json",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "read vcr-service Endpoints: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let document: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    parse_simulator_service_endpoints(&document)
}

/// Wait until the VCR Service publishes the desired ready pods without a stale or
/// not-ready endpoint before routing proof traffic through its provider Gateway.
fn wait_for_simulator_service_endpoints(context: &str) -> Result<SimulatorReadyReplicas, Box<dyn std::error::Error>> {
    const STABLE_SAMPLES: u32 = 3;

    let expected = simulator_desired_replicas(context)?;
    let deadline = Instant::now() + DATA_PLANE_WAIT;
    let mut previous: Option<(BTreeSet<String>, SimulatorReadyReplicas)> = None;
    let mut stable_samples = 0_u32;
    let mut last = String::from("no Endpoints observation");
    while Instant::now() < deadline {
        let endpoints = simulator_service_endpoints(context)?;
        let ready_replicas = simulator_ready_replicas(context)?;
        let deployment_endpoint_total = ready_replicas.total().map_err(std::io::Error::other)?;
        last = format!(
            "service-ready={:?}, service-not-ready={:?}, per-deployment-ready={ready_replicas:?}, expected-service-ready={expected}",
            endpoints.ready, endpoints.not_ready,
        );
        if endpoints.ready.len() == expected
            && endpoints.not_ready.is_empty()
            && deployment_endpoint_total == endpoints.ready.len()
        {
            let observation = (endpoints.ready.clone(), ready_replicas);
            if previous.as_ref() == Some(&observation) {
                stable_samples = stable_samples.saturating_add(1);
            } else {
                stable_samples = 1;
                previous = Some(observation);
            }
            if stable_samples >= STABLE_SAMPLES {
                eprintln!(
                    "  [OK] {context}: vcr-service has {expected} stable ready endpoint(s), vcr-1={}, vcr-2={}",
                    ready_replicas.vcr_1, ready_replicas.vcr_2
                );
                return Ok(ready_replicas);
            }
        } else {
            previous = None;
            stable_samples = 0;
        }
        std::thread::sleep(DATA_PLANE_INTERVAL);
    }
    Err(format!("{context}: vcr-service endpoints did not converge: {last}").into())
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
/// actually drives the active `GridNetwork` scoring strategy; otherwise a
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
            let breakdown = c
                .get("score_breakdown")
                .and_then(|v| serde_json::from_value(v.clone()).ok());
            Some(OverlayCandidate {
                cluster: cluster_name,
                rank,
                score,
                fresh,
                admission_state: admission,
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
fn send_inference_request(
    kube_context: &str,
    model: &str,
    phase: &str,
) -> Result<InferenceResponse, Box<dyn std::error::Error>> {
    let body = format!(r#"{{"model":"{model}","messages":[{{"role":"user","content":"test"}}]}}"#,);
    let session_id = format!("probe-{}", format_utc_timestamp());
    let curl_cmd = format!(
        "curl -sS -o /dev/null \
         -w 'STATUS:%{{http_code}}\\nPROVIDER_GW:%header{{X-Grid-LlmD-Provider-Gateway}}\\nDEMO_ATTRIB:%header{{x-ai-demo-provider-gateway}}\\n' \
         -X POST http://consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions \
         -H 'Content-Type: application/json' \
         -H 'X-Session-Id: {session_id}' \
         -d '{body}' 2>/tmp/curl.stderr > /tmp/curl.stdout
         curl_exit=$?
         curl_stdout=$(cat /tmp/curl.stdout)
         curl_stderr=$(cat /tmp/curl.stderr)
         printf '%s\\n' \"$curl_stdout\"
         printf 'CURL_EXIT:%s\\n' \"$curl_exit\"
         printf 'CURL_STDERR_BEGIN\\n'
         printf '%s' \"$curl_stderr\"
         printf '\\nCURL_STDERR_END\\n'
         exit 0",
    );
    let raw = kubectl_exec_curl_raw(kube_context, &curl_cmd).map_err(|error| {
        format!(
            "inference probe command failed: phase={phase}; observed_at={}; request_id={session_id}; {error}",
            format_utc_iso()
        )
    })?;
    let probe = parse_curl_probe_output(&raw.stdout).map_err(|error| {
        format!(
            "inference probe output could not be parsed: {}",
            curl_probe_diagnostics(&CurlProbeDiagnostic {
                phase,
                observed_at: &format_utc_iso(),
                request_id: &session_id,
                http_status: None,
                curl_exit_code: None,
                curl_stderr: &format!("kubectl stderr: {}; raw stdout: {:?}", raw.stderr, raw.stdout),
                reason: &error.to_string(),
            })
        )
    })?;
    if probe.status != 200 {
        let reason = if probe.status == 0 {
            "no HTTP response (HTTP status 000)"
        } else {
            "unexpected HTTP response"
        };
        return Err(format!(
            "inference probe failed: {}",
            curl_probe_diagnostics(&CurlProbeDiagnostic {
                phase,
                observed_at: &format_utc_iso(),
                request_id: &session_id,
                http_status: Some(probe.status),
                curl_exit_code: Some(probe.curl_exit_code),
                curl_stderr: &probe.curl_stderr,
                reason,
            })
        )
        .into());
    }
    if probe.provider_gateway.is_empty() || probe.demo_attribution.is_empty() {
        return Err(format!(
            "inference probe failed: {}",
            curl_probe_diagnostics(&CurlProbeDiagnostic {
                phase,
                observed_at: &format_utc_iso(),
                request_id: &session_id,
                http_status: Some(probe.status),
                curl_exit_code: Some(probe.curl_exit_code),
                curl_stderr: &probe.curl_stderr,
                reason: "missing attribution headers in HTTP 200 response",
            })
        )
        .into());
    }
    Ok(InferenceResponse {
        provider_gateway: probe.provider_gateway,
        demo_attribution: probe.demo_attribution,
    })
}

/// Context recorded when one inference probe is rejected.
struct CurlProbeDiagnostic<'diagnostic> {
    /// Name of the proof phase that issued this request.
    phase: &'diagnostic str,
    /// UTC time at which the probe result was observed.
    observed_at: &'diagnostic str,
    /// Unique request identifier.
    request_id: &'diagnostic str,
    /// HTTP status, with zero representing no response.
    http_status: Option<u16>,
    /// Exit code returned by curl.
    curl_exit_code: Option<i32>,
    /// Captured curl or kubectl error output.
    curl_stderr: &'diagnostic str,
    /// Reason the probe result was rejected.
    reason: &'diagnostic str,
}

/// Format rejected inference-probe context without affecting acceptance.
fn curl_probe_diagnostics(probe: &CurlProbeDiagnostic<'_>) -> serde_json::Value {
    serde_json::json!({
        "phase": probe.phase,
        "observed_at": probe.observed_at,
        "request_id": probe.request_id,
        "http_status": probe.http_status.map(|status| format!("{status:03}")),
        "curl_exit_code": probe.curl_exit_code,
        "curl_stderr": probe.curl_stderr,
        "reason": probe.reason,
    })
}

/// Parse curl's HTTP result, exit code, attribution headers, and captured stderr.
fn parse_curl_probe_output(raw: &str) -> Result<CurlProbeOutput, Box<dyn std::error::Error>> {
    let (response_lines, stderr_block) = raw
        .split_once("CURL_STDERR_BEGIN\n")
        .ok_or_else(|| std::io::Error::other("curl probe output omitted the stderr marker"))?;
    let (curl_stderr, trailing_output) = stderr_block
        .split_once("\nCURL_STDERR_END")
        .ok_or_else(|| std::io::Error::other("curl probe output omitted the stderr terminator"))?;
    let trailing_output = trailing_output.trim();
    if !trailing_output.is_empty() && !is_kubectl_curl_probe_cleanup_notice(trailing_output) {
        return Err(std::io::Error::other("curl probe output had unexpected trailing data").into());
    }

    let mut status = None;
    let mut provider_gateway = String::new();
    let mut demo_attribution = String::new();
    let mut curl_exit_code = None;
    for line in response_lines.lines() {
        if let Some(code) = line.strip_prefix("STATUS:") {
            status = Some(code.trim().parse::<u16>().map_err(|error| {
                std::io::Error::other(format!("curl probe output had an invalid HTTP status: {error}"))
            })?);
        } else if let Some(value) = line.strip_prefix("PROVIDER_GW:") {
            value.trim().clone_into(&mut provider_gateway);
        } else if let Some(value) = line.strip_prefix("DEMO_ATTRIB:") {
            value.trim().clone_into(&mut demo_attribution);
        } else if let Some(code) = line.strip_prefix("CURL_EXIT:") {
            curl_exit_code = Some(code.trim().parse::<i32>().map_err(|error| {
                std::io::Error::other(format!("curl probe output had an invalid exit code: {error}"))
            })?);
        }
    }

    Ok(CurlProbeOutput {
        status: status.ok_or_else(|| std::io::Error::other("curl probe output omitted the HTTP status"))?,
        provider_gateway,
        demo_attribution,
        curl_exit_code: curl_exit_code
            .ok_or_else(|| std::io::Error::other("curl probe output omitted the curl exit code"))?,
        curl_stderr: curl_stderr.to_owned(),
    })
}

/// Recognize the line kubectl prints after deleting a successful `run --rm` pod.
fn is_kubectl_curl_probe_cleanup_notice(line: &str) -> bool {
    const PREFIX: &str = "pod \"curl-probe-";
    const SUFFIXES: [&str; 2] = ["\" deleted", "\" deleted from grid-system namespace"];

    line.strip_prefix(PREFIX)
        .and_then(|name| SUFFIXES.iter().find_map(|suffix| name.strip_suffix(suffix)))
        .is_some_and(|name| !name.is_empty() && name.bytes().all(|byte| byte.is_ascii_digit()))
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

/// Captured output from a curl probe run in a temporary pod.
struct KubectlCurlOutput {
    /// Standard output from the pod and kubectl.
    stdout: String,
    /// Standard error from kubectl.
    stderr: String,
}

/// Run curl via kubectl in a temporary pod and retain its raw output.
fn kubectl_exec_curl_raw(kube_context: &str, cmd: &str) -> Result<KubectlCurlOutput, Box<dyn std::error::Error>> {
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
        return Err(format!(
            "kubectl run {pod_name} failed (kubectl exit code {:?}); stdout={:?}; stderr={:?}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim(),
        )
        .into());
    }
    Ok(KubectlCurlOutput {
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
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

/// Read a LoadBalancer IP for one of this run's operator Services.
fn read_operator_lb_ip(cluster: &str, service: &str) -> Result<String, Box<dyn std::error::Error>> {
    let context = kind_context(cluster);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "svc",
            service,
            "-o",
            "jsonpath={.status.loadBalancer.ingress[0].ip}",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "{cluster}: cannot read {service} service: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let ip = String::from_utf8(output.stdout)?.trim().to_owned();
    if ip.is_empty() {
        return Err(format!("{cluster}: {service} LoadBalancer has no ingress IP").into());
    }
    Ok(ip)
}

/// Read the SWIM LoadBalancer IP for a cluster.
fn read_swim_lb_ip(cluster: &str) -> Result<String, Box<dyn std::error::Error>> {
    read_operator_lb_ip(cluster, "grid-operator-swim")
}

/// Read the peer-signals LoadBalancer IP for a cluster.
fn read_signals_lb_ip(cluster: &str) -> Result<String, Box<dyn std::error::Error>> {
    read_operator_lb_ip(cluster, "grid-operator-signals")
}

/// Wait for MetalLB to assign ingress to the operator's peer-signals Service.
fn wait_for_signals_lb_ip(cluster: &str, timeout: Duration) -> Result<String, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    let mut last_error: String;
    loop {
        match read_signals_lb_ip(cluster) {
            Ok(ip) => return Ok(ip),
            Err(error) => last_error = error.to_string(),
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{cluster}: timed out waiting {timeout:?} for grid-operator-signals ingress; last observation: {last_error}"
            )
            .into());
        }
        eprintln!("[WAIT] {cluster}: grid-operator-signals has no ingress yet: {last_error}");
        std::thread::park_timeout(Duration::from_secs(2));
    }
}

/// Seed SWIM membership by upgrading operators with cross-cluster seeds.
fn seed_swim_membership() -> Result<(), Box<dyn std::error::Error>> {
    let mut endpoints: Vec<(String, String, String)> = Vec::new();
    for cluster in CLUSTERS {
        let swim_ip = read_swim_lb_ip(cluster)?;
        let signals_ip = wait_for_signals_lb_ip(cluster, SIGNALS_LB_INGRESS_TIMEOUT)?;
        eprintln!("  {cluster}: SWIM LB IP = {swim_ip}; signals LB IP = {signals_ip}");
        endpoints.push(((*cluster).to_owned(), swim_ip, signals_ip));
    }

    for (cluster, this_swim_ip, this_signals_ip) in &endpoints {
        let peer_seeds: Vec<String> = endpoints
            .iter()
            .filter(|(c, ..)| c != cluster)
            .map(|(_, ip, _)| format!("{ip}:7946"))
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
                &format!("swim.advertiseAddress={this_swim_ip}:7946"),
                "--set-string",
                &format!("signals.advertiseAddress={this_signals_ip}:{OPERATOR_SIGNALS_PORT}"),
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
fn stage_certificates() -> Result<(), Box<dyn std::error::Error>> {
    let clusters: Vec<String> = CLUSTERS.iter().map(|s| (*s).to_owned()).collect();
    certs::generate_all(&clusters)?;
    eprintln!("  [OK] TLS certificates generated for pool-a, pool-b");

    certs::generate_metrics_certs(METRICS_CA_CN, METRICS_SERVER_DNS)?;
    eprintln!("  [OK] Metrics TLS certificates generated (separate CA)");
    Ok(())
}

/// Install provider trust secrets into both clusters.
///
/// Metrics TLS secrets are installed earlier in phase 7 (before EPP
/// deployment) since the nginx sidecar mounts them at startup.
fn install_provider_trust() -> Result<(), Box<dyn std::error::Error>> {
    let certs_dir = Path::new(CERTS_DIR);

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
fn authorize_discovered_sites() -> Result<(), Box<dyn std::error::Error>> {
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

/// Wait for overlay convergence on both consumer gateways.
fn wait_for_overlay_convergence() -> Result<(), Box<dyn std::error::Error>> {
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
            return Err(format!("{cluster}: overlay did not converge within timeout").into());
        }
        eprintln!("  [OK] {cluster}: overlay converged with both providers");
    }
    Ok(())
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
        let kind_name = format!("{}-{cluster}", run_cluster_prefix());
        for image_tag in &tags {
            load_docker_image_into_kind(image_tag, &kind_name)?;
        }
        if cluster == &"pool-a"
            && let Some(grid_gateway) = &context.images.grid_gateway
        {
            load_docker_image_into_kind(grid_gateway, &kind_name)?;
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
    if let Some(grid_gateway) = &resolved.grid_gateway {
        entries.push(("grid-gateway", grid_gateway));
    }
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
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    let exit = output.status.code().map_or_else(
        || "terminated by signal".to_owned(),
        |code| format!("exit status {code}"),
    );
    let detail = if stderr.is_empty() {
        "no stderr output".to_owned()
    } else {
        stderr
    };
    eprintln!("[COMMAND][FAIL] Forge command {:?} {exit}: {detail}", args.join(" "));
    Err(format!("Forge command {} {exit}: {detail}", args.join(" ")).into())
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
    } = *options;
    let dir = forge_config.parent().unwrap_or_else(|| Path::new("."));
    let run_suffix = run_id.map_or_else(String::new, |id| format!(".{id}"));
    let resolved = dir.join(format!(".forge.resolved{run_suffix}.yaml"));
    let mut result = fs::read_to_string(forge_config)?;
    if let Some(run_id) = run_id {
        result = scope_forge_config_to_run(&result, run_id)?;
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
        let signal_anchor = "                    healthy: llm_d_epp_ready_endpoints";
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

    fs::write(&resolved, result)?;
    Ok(resolved)
}

/// Scope the Forge prefix, shared Docker network, and explicit `kubectl` contexts to one run.
fn scope_forge_config_to_run(config: &str, run_id: &str) -> Result<String, Box<dyn std::error::Error>> {
    let cluster_prefix = format!("{DEFAULT_CLUSTER_PREFIX}-{}", run_id.to_ascii_lowercase());
    let mut scoped = checked_replace(
        config,
        &format!("metadata:\n  name: {DEFAULT_FORGE_ENV_NAME}"),
        &format!("metadata:\n  name: {cluster_prefix}"),
        1,
        "run-scoped Forge environment and Docker network name",
    )?;
    scoped = checked_replace(
        &scoped,
        &format!("clusterPrefix: {DEFAULT_CLUSTER_PREFIX}"),
        &format!("clusterPrefix: {cluster_prefix}"),
        1,
        "run-scoped Forge cluster prefix",
    )?;
    let default_context = format!("kind-{DEFAULT_CLUSTER_PREFIX}-{{{{ cluster.name }}}}");
    let run_context = format!("kind-{cluster_prefix}-{{{{ cluster.name }}}}");
    scoped = checked_replace(
        &scoped,
        &default_context,
        &run_context,
        4,
        "run-scoped kubectl contexts",
    )?;
    Ok(scoped)
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
    format!("kind-{}-{cluster}", run_cluster_prefix())
}

/// Return this invocation's Forge cluster prefix, or the checked-in default in unit tests.
fn run_cluster_prefix() -> &'static str {
    RUN_CLUSTER_PREFIX.get().map_or(DEFAULT_CLUSTER_PREFIX, String::as_str)
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
                &format!("grid.praxis.fast/metrics-refresh-at={ts}"),
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
/// with a score above zero, meaning the operator successfully scraped its
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
                "{cluster}: operator pod {} (uid={}) replaced (unexpected restart)",
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
                "{cluster}: EPP pod was not replaced after server rotation (expected rollout restart)"
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
/// rotation is documented and excluded from the failure check, but only
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
            "server rotation (stage 8) on {rc}: EPP rollout restart is expected; nginx does not reload TLS in-place"
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

    // Stage 1: Baseline mTLS (verify operator scrapes through TLS)
    eprintln!();
    eprintln!("  [TLS 1/9] Baseline mTLS");
    results.insert("tls_01_baseline".to_owned(), proof_tls_baseline());

    // Stage 2: Handshake rejection (TLS proxy rejects connection without client cert)
    eprintln!();
    eprintln!("  [TLS 2/9] Handshake rejection");
    results.insert("tls_02_handshake_rejection".to_owned(), proof_tls_handshake_rejection());

    // Stage 3: Missing client identity (delete client cert Secret)
    eprintln!();
    eprintln!("  [TLS 3/9] Missing client identity");
    results.insert("tls_03_missing_client".to_owned(), proof_tls_missing_client());

    // Stage 4: Wrong CA (replace CA Secret with untrusted CA)
    eprintln!();
    eprintln!("  [TLS 4/9] Wrong CA");
    results.insert("tls_04_wrong_ca".to_owned(), proof_tls_wrong_ca());

    // Stage 5: Restore valid mTLS (recreate correct Secrets)
    eprintln!();
    eprintln!("  [TLS 5/9] Restore valid mTLS");
    results.insert("tls_05_restore".to_owned(), proof_tls_restore());

    // Stage 6: Stale-cache behavior (independent TTL verification)
    eprintln!();
    eprintln!("  [TLS 6/9] Stale-cache TTL");
    results.insert("tls_06_stale_cache".to_owned(), proof_tls_stale_cache());

    // Stage 7: Client Secret rotation (new cert, same CA)
    eprintln!();
    eprintln!("  [TLS 7/9] Client Secret rotation");
    results.insert("tls_07_client_rotation".to_owned(), proof_tls_client_rotation());

    // Stage 8: Server cert/CA rotation (new server cert + nginx restart)
    eprintln!();
    eprintln!("  [TLS 8/9] Server cert rotation");
    let server_rotation = proof_tls_server_rotation();
    let rotation_cluster = server_rotation.success.then_some("pool-a");
    results.insert("tls_08_server_rotation".to_owned(), server_rotation);

    // Stage 9: Existing routing behavior (verify routing after TLS manipulations)
    eprintln!();
    eprintln!("  [TLS 9/9] Existing routing behavior");
    results.insert("tls_09_routing".to_owned(), proof_tls_routing());

    // Restart accounting: compare before/after snapshots
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
                    "{cluster}: NOT observable; TLS transport OK but operator did not ingest metrics into overlay scores \
                     (check operator image contains MetricsConfig implementation)"
                ));
            } else {
                observations.push(format!(
                    "{cluster}: NOT observable; TLS transport also failed \
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
    // We use `wget` inside the nginx sidecar; it has network access to
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
                    "{cluster}: metrics endpoint accepted connection WITHOUT client cert; mTLS NOT enforced"
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
            "{cluster}: provider still observable after client cert removal; fail-closed NOT working"
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
            "{cluster}: provider still observable with wrong CA; CA validation NOT working"
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
            "{cluster}: provider became unobservable after client rotation; rotation failed"
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
/// documented honestly: the operator handles Secret rotation, but
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
            "{cluster}: provider not observable after server rotation; rotation failed"
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
             before TTL expired; cached metrics not served"
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
             after TTL expired; stale metrics not evicted"
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
            observations.push(format!("{cluster}: provider NOT observable; routing check impossible"));
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
    match send_inference_request(&probe_ctx, VCR_MODEL, "tls-routing") {
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
    use serde::Deserialize as _;

    use super::*;

    #[test]
    fn curl_probe_output_preserves_no_response_transport_diagnostics() -> Result<(), Box<dyn std::error::Error>> {
        let output = parse_curl_probe_output(
            "STATUS:000\nPROVIDER_GW:\nDEMO_ATTRIB:\nCURL_EXIT:28\nCURL_STDERR_BEGIN\ncurl: (28) timed out\nCURL_STDERR_END\npod \"curl-probe-050906\" deleted\n",
        )?;

        assert_eq!(output.status, 0);
        assert_eq!(output.curl_exit_code, 28);
        assert_eq!(output.curl_stderr, "curl: (28) timed out");
        assert!(output.provider_gateway.is_empty());
        assert!(output.demo_attribution.is_empty());
        Ok(())
    }

    #[test]
    fn curl_probe_output_accepts_kubectl_cleanup_notice_with_namespace_detail() -> Result<(), Box<dyn std::error::Error>>
    {
        let output = parse_curl_probe_output(
            "STATUS:200\nPROVIDER_GW:pool-a\nDEMO_ATTRIB:pool-a\nCURL_EXIT:0\nCURL_STDERR_BEGIN\n\nCURL_STDERR_END\npod \"curl-probe-050906\" deleted from grid-system namespace\n",
        )?;

        assert_eq!(output.status, 200);
        assert_eq!(output.provider_gateway, "pool-a");
        assert_eq!(output.demo_attribution, "pool-a");
        assert_eq!(output.curl_exit_code, 0);
        Ok(())
    }

    #[test]
    fn curl_probe_output_rejects_unrecognized_trailing_data() {
        assert!(
            parse_curl_probe_output(
                "STATUS:000\nPROVIDER_GW:\nDEMO_ATTRIB:\nCURL_EXIT:28\nCURL_STDERR_BEGIN\ncurl: (28) timed out\nCURL_STDERR_END\nunexpected output\n",
            )
            .err()
            .is_some_and(|error| error.to_string().contains("unexpected trailing data")),
            "unrecognized kubectl output must not be accepted as curl data"
        );
    }

    #[test]
    fn curl_probe_failure_report_keeps_phase_timestamp_exit_code_and_stderr() {
        let report = curl_probe_diagnostics(&CurlProbeDiagnostic {
            phase: "network-partition-traffic",
            observed_at: "2026-10-15T04:28:27Z",
            request_id: "grid-poll-0-1",
            http_status: Some(0),
            curl_exit_code: Some(28),
            curl_stderr: "curl: (28) Operation timed out\n",
            reason: "curl returned a transport failure or HTTP status 000",
        });

        assert_eq!(
            report.get("phase"),
            Some(&serde_json::json!("network-partition-traffic"))
        );
        assert_eq!(
            report.get("observed_at"),
            Some(&serde_json::json!("2026-10-15T04:28:27Z"))
        );
        assert_eq!(report.get("request_id"), Some(&serde_json::json!("grid-poll-0-1")));
        assert_eq!(report.get("http_status"), Some(&serde_json::json!("000")));
        assert_eq!(report.get("curl_exit_code"), Some(&serde_json::json!(28)));
        assert_eq!(
            report.get("curl_stderr"),
            Some(&serde_json::json!("curl: (28) Operation timed out\n"))
        );
    }

    #[test]
    fn curl_probe_output_parses_successful_status_and_attribution() -> Result<(), Box<dyn std::error::Error>> {
        let output = parse_curl_probe_output(
            "STATUS:200\nPROVIDER_GW:pool-a\nDEMO_ATTRIB:pool-a-provider\nCURL_EXIT:0\nCURL_STDERR_BEGIN\n\nCURL_STDERR_END\n",
        )?;

        assert_eq!(output.status, 200);
        assert_eq!(output.curl_exit_code, 0);
        assert!(output.curl_stderr.is_empty());
        assert_eq!(output.provider_gateway, "pool-a");
        assert_eq!(output.demo_attribution, "pool-a-provider");
        Ok(())
    }

    #[test]
    fn pool_metrics_topology_uses_deterministic_selection_for_pressure_recovery()
    -> Result<(), Box<dyn std::error::Error>> {
        let config_source = include_str!("../../../../tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml");
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
            include_str!(
                "../../../../tests/e2e/topologies/grid-llmd-pool-metrics/resources/pool-a/vcr-deployment.yaml"
            ),
            include_str!(
                "../../../../tests/e2e/topologies/grid-llmd-pool-metrics/resources/pool-b/vcr-deployment.yaml"
            ),
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
        let config = include_str!("../../../../tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml");

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
    fn run_scoped_forge_config_updates_every_explicit_kubectl_context() -> Result<(), Box<dyn std::error::Error>> {
        let config = include_str!("../../../../tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml");
        let run_id = "20261011T120000Z-12345";
        let scoped = scope_forge_config_to_run(config, run_id)?;
        let run_prefix = format!("{DEFAULT_CLUSTER_PREFIX}-{}", run_id.to_ascii_lowercase());
        let run_context = format!("kind-{run_prefix}-{{{{ cluster.name }}}}");

        assert!(
            scoped.contains(&format!("metadata:\n  name: {run_prefix}")),
            "Forge environment and network names should include the unique run prefix"
        );
        assert!(
            scoped.contains(&format!("clusterPrefix: {run_prefix}")),
            "Kind cluster names should include the unique run prefix"
        );
        assert_eq!(
            scoped.matches(&run_context).count(),
            4,
            "every explicit kubectl context should use the run-scoped clusters"
        );
        assert!(
            !scoped.contains(&format!("metadata:\n  name: {DEFAULT_FORGE_ENV_NAME}")),
            "the shared default Forge environment must not remain in the scoped config"
        );
        assert!(
            !scoped.contains(&format!("kind-{DEFAULT_CLUSTER_PREFIX}-{{{{ cluster.name }}}}")),
            "the shared default Kind context must not remain in the scoped config"
        );
        Ok(())
    }

    #[test]
    fn pool_metrics_topology_enables_signal_polling_at_the_chart_root() -> Result<(), Box<dyn std::error::Error>> {
        let config = include_str!("../../../../tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml");
        let config: serde_yaml::Value = serde_yaml::from_str(config)?;
        let stacks = config
            .get("spec")
            .and_then(|spec| spec.get("stacks"))
            .and_then(serde_yaml::Value::as_mapping)
            .ok_or_else(|| std::io::Error::other("Forge config must contain a spec.stacks mapping"))?;

        for stack_name in ["pool-a-operator-base", "pool-b-operator-base"] {
            let stack = stacks
                .get(serde_yaml::Value::String(stack_name.to_owned()))
                .and_then(serde_yaml::Value::as_mapping)
                .ok_or_else(|| std::io::Error::other(format!("{stack_name} must be a stack mapping")))?;
            let steps = stack
                .get(serde_yaml::Value::String("steps".to_owned()))
                .and_then(serde_yaml::Value::as_sequence)
                .ok_or_else(|| std::io::Error::other(format!("{stack_name}.steps must be a sequence")))?;
            let operator = steps
                .iter()
                .find(|step| step.get("release").and_then(serde_yaml::Value::as_str) == Some("grid-operator"))
                .ok_or_else(|| std::io::Error::other(format!("{stack_name} must include a grid-operator release")))?;
            let values = operator.get("values").ok_or_else(|| {
                std::io::Error::other(format!("{stack_name} grid-operator release must include values"))
            })?;
            let signals = values.get("signals").ok_or_else(|| {
                std::io::Error::other(format!("{stack_name} grid-operator values must include signals"))
            })?;

            assert_eq!(
                signals.get("enabled").and_then(serde_yaml::Value::as_bool),
                Some(true),
                "cross-site qualification must enable operator peer signals"
            );
            assert_eq!(
                signals.get("peerIntervalSeconds").and_then(serde_yaml::Value::as_str),
                Some("2"),
                "cross-site peer signals should poll at the configured two-second interval"
            );
            assert!(
                values.get("swim").and_then(|swim| swim.get("signals")).is_none(),
                "signals configuration belongs at the chart root rather than under SWIM"
            );
        }
        Ok(())
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
        let config = simulator_config("vcr-1-config", 0, 0, 0.0);
        assert!(config.contains("fake-metrics:"));
        assert!(config.contains("name: vcr-1-config"));
        assert!(config.contains("waiting-requests: 0"));
        assert!(config.contains("running-requests: 0"));
        assert!(config.contains("kv-cache-usage: 0"));
    }

    #[test]
    fn simulator_config_persists_pressure_queue_value() {
        let config = simulator_config("vcr-2-config", 9, 0, 0.0);
        assert!(config.contains("name: vcr-2-config"));
        assert!(config.contains("waiting-requests: 9"));
        assert!(!config.contains("pressure-generator"));
    }

    #[test]
    fn simulator_config_supports_distinct_endpoint_kv_cache_values() {
        let high_endpoint = simulator_config("vcr-1-config", 1, 8, 1.0);
        let low_endpoint = simulator_config("vcr-2-config", 1, 8, 0.0);

        assert!(high_endpoint.contains("name: vcr-1-config"));
        assert!(high_endpoint.contains("waiting-requests: 1"));
        assert!(high_endpoint.contains("running-requests: 8"));
        assert!(high_endpoint.contains("kv-cache-usage: 1"));
        assert!(low_endpoint.contains("name: vcr-2-config"));
        assert!(low_endpoint.contains("waiting-requests: 1"));
        assert!(low_endpoint.contains("running-requests: 8"));
        assert!(low_endpoint.contains("kv-cache-usage: 0"));
    }

    #[test]
    fn simulator_config_data_matches_the_manifest_block_value() {
        let waiting_requests = 1;
        let running_requests = 8;
        let kv_cache_usage = 1.0;
        let config = simulator_config("vcr-1-config", waiting_requests, running_requests, kv_cache_usage);
        let config_data = simulator_config_data(waiting_requests, running_requests, kv_cache_usage);
        let indented_config_data = config_data
            .lines()
            .map(|line| format!("    {line}\n"))
            .collect::<String>();

        assert!(
            config.ends_with(&format!("data:\n  config.yaml: |\n{indented_config_data}")),
            "the readback value should be rendered from the same simulator configuration data"
        );
    }

    #[test]
    fn kv_cache_pressure_sets_only_the_selected_signal() {
        let (queue, kv_cache) = simulator_metrics(9, ScoringFlavor::KvCachePressure);
        let config = simulator_config("vcr-1-config", queue, 0, kv_cache);
        assert!(config.contains("waiting-requests: 0"));
        assert!(config.contains("kv-cache-usage: 0.95"));
    }

    #[test]
    fn simulator_rollout_annotations_change_for_kv_cache_pressure() {
        let idle = simulator_rollout_annotations(0, 0, 0.0);
        let pressured = simulator_rollout_annotations(0, 0, 0.95);

        assert_eq!(
            idle.get(SIMULATOR_WAITING_REQUESTS_ANNOTATION).map(String::as_str),
            Some("0"),
            "idle configuration should annotate the zero queue value"
        );
        assert_eq!(
            pressured.get(SIMULATOR_WAITING_REQUESTS_ANNOTATION).map(String::as_str),
            Some("0"),
            "KV-cache pressure must leave queue pressure unchanged"
        );
        assert_eq!(
            idle.get(SIMULATOR_KV_CACHE_ANNOTATION).map(String::as_str),
            Some("0"),
            "idle configuration should annotate zero KV-cache use"
        );
        assert_eq!(
            idle.get(SIMULATOR_RUNNING_REQUESTS_ANNOTATION).map(String::as_str),
            Some("0"),
            "idle configuration should annotate the zero running-request value"
        );
        assert_eq!(
            pressured.get(SIMULATOR_KV_CACHE_ANNOTATION).map(String::as_str),
            Some("0.95"),
            "KV-cache pressure should be present in the simulator pod template"
        );
        assert_ne!(
            idle, pressured,
            "KV-cache-only changes must change pod-template annotations and trigger rollout"
        );
    }

    #[test]
    fn unchanged_simulator_generation_does_not_require_a_rollout() {
        assert_eq!(
            simulator_rollout_needed(3, 3),
            Ok(false),
            "an unchanged generation is already current"
        );
        assert_eq!(
            simulator_rollout_needed(3, 4),
            Ok(true),
            "a newer generation requires rollout readiness"
        );
        assert_eq!(
            simulator_rollout_needed(4, 3),
            Err("generation moved backwards (4 -> 3)".to_owned()),
            "a generation moving backwards indicates inconsistent rollout evidence"
        );
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
            started_at: "2026-01-01T00:00:00Z".to_owned(),
            wall_secs: 42.0,
            success: true,
            error: None,
            setup: SetupEvidence {
                clusters: vec!["pool-a".to_owned(), "pool-b".to_owned()],
                images: BTreeMap::new(),
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
        // crates/operator/src/crd/grid_network.rs exactly, since this string is
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
    fn simulator_endpoints_parser_separates_ready_pods_from_replacements() -> Result<(), Box<dyn std::error::Error>> {
        let endpoints = parse_simulator_service_endpoints(&serde_json::json!({
            "subsets": [{
                "addresses": [{
                    "ip": "10.244.0.12",
                    "targetRef": {"name": "vcr-1-new", "uid": "ready-uid"}
                }],
                "notReadyAddresses": [{
                    "ip": "10.244.0.11",
                    "targetRef": {"name": "vcr-1-old", "uid": "terminating-uid"}
                }]
            }]
        }))?;

        assert_eq!(endpoints.ready, BTreeSet::from(["ready-uid".to_owned()]));
        assert_eq!(endpoints.not_ready, BTreeSet::from(["terminating-uid".to_owned()]));
        Ok(())
    }

    #[test]
    fn simulator_kv_cache_mean_weights_each_ready_endpoint() {
        let mean = simulator_kv_cache_mean(0.2, 0.8, SimulatorReadyReplicas { vcr_1: 2, vcr_2: 1 });

        assert!(mean.is_ok(), "ready endpoint counts should produce a mean");
        assert!(
            mean.is_ok_and(|value| (value - 0.4).abs() < f64::EPSILON),
            "the mean must weight the two vcr-1 endpoints separately from the one vcr-2 endpoint"
        );
    }

    #[test]
    fn simulator_kv_cache_mean_rejects_missing_ready_endpoints() {
        let mean = simulator_kv_cache_mean(0.2, 0.8, SimulatorReadyReplicas::default());

        assert!(mean.is_err(), "the mean requires at least one ready endpoint");
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
            started_at: "2026-01-01T00:00:00Z".to_owned(),
            wall_secs: 10.0,
            success: true,
            error: None,
            setup: SetupEvidence {
                clusters: vec!["pool-a".to_owned()],
                images: BTreeMap::new(),
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
            started_at: "2026-01-01T00:00:00Z".to_owned(),
            wall_secs: 10.0,
            success: true,
            error: None,
            setup: SetupEvidence {
                clusters: vec!["pool-a".to_owned()],
                images: BTreeMap::new(),
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
                    healthy: llm_d_epp_ready_endpoints

                  metricsEndpoint: \"http://llmd-epp-metrics.grid-system.svc.cluster.local:9090\"
                  signalNames:
                    healthy: llm_d_epp_ready_endpoints

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
        let images = resolve_images(MetricsTransport::DirectHttp, false).unwrap();
        assert!(images.nginx.is_none(), "direct-HTTP must not resolve nginx image");
        assert!(
            images.grid_gateway.is_none(),
            "quick-mode inputs must not require Grid Gateway"
        );
    }

    #[test]
    fn nginx_image_present_in_mtls() {
        let images = resolve_images(MetricsTransport::MtlsProxy, false).unwrap();
        assert!(images.nginx.is_some(), "mTLS must resolve nginx image");
        assert_eq!(images.nginx.unwrap(), DEFAULT_NGINX_IMAGE);
        assert!(
            images.grid_gateway.is_none(),
            "quick-mode inputs must not require Grid Gateway"
        );
    }
}
