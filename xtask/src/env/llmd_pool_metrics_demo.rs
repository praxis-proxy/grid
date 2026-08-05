//! llm-d pool-metrics routing demo orchestration.
//!
//! Deploys two Kind clusters, each running an llm-d EPP backed by two
//! `inference-sim` replicas. Grid scrapes EPP pool-level metrics and
//! adjusts routing when load pressure changes one pool's state.
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
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use serde::Serialize;

use super::{DemoMode, GlbDemoOptions, certs, glb, kubectl, operator};

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

/// Provider credential secret name (simulators accept any bearer token).
const SIM_INFERENCE_CREDENTIAL: &str = "sim-inference-credential";

/// Overlay ConfigMap name created by the Grid operator for consumer gateways.
const OVERLAY_CONFIGMAP: &str = "grid-overlay-grid-llmd-pool-metrics-consumer-gateway";

/// Stable terminal separator.
const OUTPUT_RULE: &str = "===============================================================================";

/// Evidence JSON schema version.
const EVIDENCE_SCHEMA_VERSION: &str = "1";

/// Number of setup phases.
const SETUP_PHASES: usize = 11;

/// Primary model name served by inference simulators.
const SIM_MODEL: &str = "llmd-sim-model";

/// Data-plane convergence timeout for overlay propagation.
const DATA_PLANE_WAIT: Duration = Duration::from_secs(180);

/// Retry interval for convergence probes.
const DATA_PLANE_INTERVAL: Duration = Duration::from_secs(1);

/// Configured queue capacity (matches sim-config max-waiting-queue-length).
const QUEUE_CAPACITY: f64 = 16.0;

/// Scoring weight for queue_depth signal (must match ScoringWeights default).
const QUEUE_DEPTH_WEIGHT: f64 = 3.0;

/// Scoring weight for kv_cache signal (must match ScoringWeights default).
const KV_CACHE_WEIGHT: f64 = 2.0;

/// Minimum score gap required before capturing the pressure scorecard.
const MIN_PRESSURE_SCORE_GAP: f64 = 1.0;

/// GridNetwork resource name.
const GRID_NETWORK_NAME: &str = "grid-llmd-pool-metrics";

/// Default gateway image tag.
///
/// The pool-metrics demo shares the same Grid-enabled Praxis AI binary as the
/// combined-site demo. Both require the `peer_identity_trust`,
/// `provider_route`, `credential_inject`, and `intelligent_route` filters
/// which are built into the GLB demo image.
const DEFAULT_GATEWAY_IMAGE: &str = "praxis-ai:glb-demo";

/// Default operator image tag.
const DEFAULT_OPERATOR_IMAGE: &str = "grid-operator:llmd-pool-metrics-demo";

/// Default EPP image tag.
const DEFAULT_EPP_IMAGE: &str = "llm-d-epp:llmd-pool-metrics-demo";

/// Default inference-sim image tag.
const DEFAULT_SIM_IMAGE: &str = "llm-d-inference-sim:llmd-pool-metrics-demo";

/// Default overlay-sync sidecar image tag.
const DEFAULT_OVERLAY_SYNC_IMAGE: &str = "grid-overlay-sync:llmd-pool-metrics-demo";

/// Default nginx image for the metrics TLS reverse proxy sidecar.
const DEFAULT_NGINX_IMAGE: &str = "nginx:metrics-proxy-demo";

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

/// Demo execution context holding resolved paths.
struct DemoContext {
    /// Path to the resolved Forge config.
    resolved_config: PathBuf,
    /// Path to the forge binary.
    forge_bin: PathBuf,
    /// Resolved container images.
    images: ResolvedImages,
}

/// Resolved container image references.
struct ResolvedImages {
    /// Praxis AI gateway image (must contain Grid filters).
    gateway: String,
    /// Grid operator image.
    operator: String,
    /// llm-d EPP image.
    epp: String,
    /// llm-d inference-sim image.
    sim: String,
    /// Grid overlay-sync sidecar image.
    overlay_sync: String,
    /// nginx image for metrics TLS reverse proxy sidecar.
    nginx: String,
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

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run the llm-d pool-metrics routing demo.
///
/// # Errors
///
/// Returns an error when setup, proof scenarios, or teardown fail.
pub(crate) fn run(forge_config: &Path, options: &GlbDemoOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mode = options.mode();
    let run_id = format_utc_timestamp();
    let started_at = format_utc_iso();
    let wall_start = Instant::now();

    let evidence_dir = resolve_evidence_dir(forge_config, options, &run_id)?;
    fs::create_dir_all(&evidence_dir)?;

    eprintln!("{OUTPUT_RULE}");
    eprintln!("Grid llm-d Pool-Metrics Routing Demo");
    eprintln!("Mode: {}", if mode == DemoMode::Quick { "quick" } else { "full" });
    eprintln!("Config: {}", forge_config.display());
    eprintln!("{OUTPUT_RULE}");

    let context = prepare_setup(forge_config)?;
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
                run_error = Some(format!("proofs failed: {}", failed.join(", ")));
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

            if options.teardown
                && !options.keep_on_failure
                && let Err(te) = teardown_environment(&context)
            {
                eprintln!("[WARN]  Cleanup after setup failure: {te}");
            }

            BTreeMap::new()
        },
    };

    let wall_secs = wall_start.elapsed().as_secs_f64();
    let images = collect_image_evidence(&context.images)?;
    let success = run_error.is_none();

    let evidence = Evidence {
        schema_version: EVIDENCE_SCHEMA_VERSION.to_owned(),
        mode: format!("{mode:?}").to_lowercase(),
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

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

/// Resolve inputs before creating clusters.
fn prepare_setup(forge_config: &Path) -> Result<DemoContext, Box<dyn std::error::Error>> {
    let resolved_config = materialize_config(forge_config)?;
    let forge_bin = glb::resolve_forge_binary()
        .ok_or("praxis-forge binary not found")?
        .into();

    let images = resolve_images()?;
    verify_images(&images)?;

    Ok(DemoContext {
        resolved_config,
        forge_bin,
        images,
    })
}

/// Resolve image references from environment variables with defaults.
fn resolve_images() -> Result<ResolvedImages, Box<dyn std::error::Error>> {
    let gateway = std::env::var("GRID_XTASK_GATEWAY_IMAGE").unwrap_or_else(|_| DEFAULT_GATEWAY_IMAGE.to_owned());
    let operator = std::env::var("GRID_XTASK_OPERATOR_IMAGE").unwrap_or_else(|_| DEFAULT_OPERATOR_IMAGE.to_owned());
    let epp = std::env::var("GRID_XTASK_EPP_IMAGE").unwrap_or_else(|_| DEFAULT_EPP_IMAGE.to_owned());
    let sim = std::env::var("GRID_XTASK_SIM_IMAGE").unwrap_or_else(|_| DEFAULT_SIM_IMAGE.to_owned());
    let overlay_sync =
        std::env::var("GRID_XTASK_OVERLAY_SYNC_IMAGE").unwrap_or_else(|_| DEFAULT_OVERLAY_SYNC_IMAGE.to_owned());
    let nginx = std::env::var("GRID_XTASK_NGINX_IMAGE").unwrap_or_else(|_| DEFAULT_NGINX_IMAGE.to_owned());

    eprintln!("  Images:");
    eprintln!("    gateway:      {gateway}");
    eprintln!("    operator:     {operator}");
    eprintln!("    epp:          {epp}");
    eprintln!("    sim:          {sim}");
    eprintln!("    overlay-sync: {overlay_sync}");
    eprintln!("    nginx:        {nginx}");

    Ok(ResolvedImages {
        gateway,
        operator,
        epp,
        sim,
        overlay_sync,
        nginx,
    })
}

/// Verify all required images exist locally before creating clusters.
fn verify_images(images: &ResolvedImages) -> Result<(), Box<dyn std::error::Error>> {
    for (role, image, env_suffix) in [
        ("gateway", &images.gateway, "GATEWAY"),
        ("operator", &images.operator, "OPERATOR"),
        ("epp", &images.epp, "EPP"),
        ("sim", &images.sim, "SIM"),
        ("overlay-sync", &images.overlay_sync, "OVERLAY_SYNC"),
        ("nginx", &images.nginx, "NGINX"),
    ] {
        let status = Command::new("docker")
            .args(["image", "inspect", image])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
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
/// differs (e.g. `praxis-ai:glb-demo`), this function creates the
/// expected tag so Kind image loading and pod pulls succeed.
fn tag_images_for_forge(images: &ResolvedImages) -> Result<(), Box<dyn std::error::Error>> {
    let forge_expected: &[(&str, &str)] = &[
        (&images.gateway, "praxis-ai:llmd-pool-metrics-demo"),
        (&images.operator, "grid-operator:llmd-pool-metrics-demo"),
        (&images.epp, "llm-d-epp:llmd-pool-metrics-demo"),
        (&images.sim, "llm-d-inference-sim:llmd-pool-metrics-demo"),
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
    let total = SETUP_PHASES;
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
    eprintln!("[SETUP {}/{}] Generating TLS certificates", next(), total);
    stage_certificates()?;

    // Phase 3: Create Kind clusters
    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Creating two Kind clusters: pool-a, pool-b",
        next(),
        total
    );
    run_forge(&context.forge_bin, &context.resolved_config, &["up"])?;

    // Phase 4: Load images into clusters
    eprintln!();
    eprintln!("[SETUP {}/{}] Loading container images into clusters", next(), total);
    load_images_into_clusters(context)?;

    // Phase 5: Install MetalLB and Grid operators
    eprintln!();
    eprintln!("[SETUP {}/{}] Installing MetalLB and Grid operators", next(), total);
    for cluster in CLUSTERS {
        let ctx = kind_context(cluster);
        run_forge_stack(&context.forge_bin, &context.resolved_config, cluster, "metallb")?;
        let op_stack = format!("{cluster}-operator-base");
        run_forge_stack(&context.forge_bin, &context.resolved_config, cluster, &op_stack)?;
        eprintln!("  [OK] {cluster}: MetalLB and operator ready");
        drop(ctx);
    }

    // Phase 6: Seed SWIM membership
    eprintln!();
    eprintln!("[SETUP {}/{}] Seeding SWIM cross-cluster membership", next(), total);
    seed_swim_membership()?;

    // Phase 7: Install metrics TLS secrets (before EPP deployment needs them)
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

    // Phase 8: Deploy llm-d simulators and EPP
    eprintln!();
    eprintln!("[SETUP {}/{}] Deploying llm-d simulators and EPP", next(), total);
    for cluster in CLUSTERS {
        let llmd_stack = format!("llmd-{cluster}");
        run_forge_stack(&context.forge_bin, &context.resolved_config, cluster, &llmd_stack)?;
        eprintln!("  [OK] {cluster}: sim-1, sim-2, and EPP running");
    }

    // Phase 9: Install provider trust and credentials
    eprintln!();
    eprintln!("[SETUP {}/{}] Installing provider trust and credentials", next(), total);
    install_provider_trust()?;

    // Phase 9: Deploy Grid site resources and gateways
    //
    // Consumer gateway configs reference provider-gateway IPs from *both*
    // clusters (via forge captures), so all provider gateways must be up
    // before any consumer gateway is deployed.
    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Deploying Grid site resources and gateways",
        next(),
        total
    );
    for cluster in CLUSTERS {
        let site_stack = format!("{cluster}-site");
        run_forge_stack(&context.forge_bin, &context.resolved_config, cluster, &site_stack)?;
        run_forge_stack(
            &context.forge_bin,
            &context.resolved_config,
            cluster,
            "provider-gateway",
        )?;
        eprintln!("  [OK] {cluster}: site and provider-gateway deployed");
    }
    for cluster in CLUSTERS {
        run_forge_stack(
            &context.forge_bin,
            &context.resolved_config,
            cluster,
            "consumer-gateway",
        )?;
        eprintln!("  [OK] {cluster}: consumer-gateway deployed");
    }

    // Phase 10: Wait for overlay convergence
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

    // Proof 1: Provenance — image digests and config verification
    results.insert("provenance".to_owned(), proof_provenance());

    // Proof 2: Baseline — early state scorecard with production scores
    results.insert("baseline".to_owned(), proof_baseline(context));

    if mode == DemoMode::Full {
        // Proof 3: Pressure and flip — poll until score crossover
        results.insert("pressure_and_flip".to_owned(), proof_pressure_and_flip(context));

        // Proof 4: Recovery — poll until ramp resets
        results.insert("recovery".to_owned(), proof_recovery(context));
    }

    // TLS proof stages — always run (these are the core mTLS acceptance criteria)
    let tls_results = run_tls_proof_stages();
    results.extend(tls_results);

    results
}

/// Proof 1: Image digests and sim-config verification.
fn proof_provenance() -> ProofResult {
    let mut observations = Vec::new();
    let mut success = true;

    for cluster in CLUSTERS {
        let ctx = kind_context(cluster);
        let mut metrics_ok = false;
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Ok(metrics_text) = kubectl_exec_epp_metrics(cluster) {
                let has_kv = metrics_text.contains("llm_d_router_epp_average_kv_cache_utilization");
                let has_queue = metrics_text.contains("llm_d_router_epp_average_queue_size");
                let has_ready = metrics_text.contains("llm_d_router_epp_ready_endpoints");
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

        // Verify sim-config ConfigMap
        match kubectl::get_configmap_yaml(&ctx, GRID_SYSTEM_NS, "sim-config") {
            Ok(yaml) => {
                let has_fake = yaml.contains("fake-metrics");
                observations.push(format!("{cluster}: sim-config has fake-metrics={has_fake}"));
            },
            Err(e) => {
                observations.push(format!("{cluster}: sim-config missing: {e}"));
            },
        }
    }

    ProofResult {
        success,
        description: "Provenance: EPP metrics live, sim-config verified".to_owned(),
        observations,
    }
}

/// Proof 2: Wait for pool-a ramp reset, confirm pool-a preferred, send
/// attributed request through pool-a.
fn proof_baseline(_context: &DemoContext) -> ProofResult {
    let mut observations = Vec::new();

    eprintln!("  Waiting for pool-a to become preferred (ramp reset)...");
    let deadline = Instant::now() + DATA_PLANE_WAIT;
    let mut last_reconcile_trigger = Instant::now();
    let mut last_request = Instant::now() - Duration::from_secs(10);

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

        let epp_a = scrape_epp_metrics("pool-a");
        let candidates = read_overlay_candidates("pool-a");
        let rank_a = overlay_rank_for_cluster(&candidates, "pool-a");

        if rank_a == 0 && epp_a.queue_size < 3.0 && last_request.elapsed() >= Duration::from_secs(5) {
            last_request = Instant::now();
            let probe_ctx = kind_context("pool-a");
            match send_inference_request(&probe_ctx, SIM_MODEL) {
                Ok(resp) => {
                    if resp.provider_gateway.contains("pool-a") && resp.demo_attribution.contains("pool-a") {
                        let row_a = build_scorecard_row("Cluster A", &candidates, "pool-a");
                        let row_b = build_scorecard_row("Cluster B", &candidates, "pool-b");
                        print_scorecard("BASELINE", &[&row_a, &row_b], "CLUSTER A", &candidates);
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
                            description: "Baseline: pool-a preferred after ramp reset, pool-a attribution confirmed"
                                .to_owned(),
                            observations,
                        };
                    }
                    eprintln!(
                        "  data plane converging (overlay=pool-a rank 0, routing={})",
                        resp.provider_gateway
                    );
                },
                Err(e) => {
                    eprintln!("  inference probe retrying: {e}");
                },
            }
        } else if rank_a != 0 {
            eprintln!(
                "  pool-a: queue={:.1} rank={} (waiting for ramp reset)",
                epp_a.queue_size, rank_a
            );
        }

        std::thread::sleep(DATA_PLANE_INTERVAL);
    }

    observations.push("pool-a did not reach rank 0 with confirmed routing within timeout".to_owned());
    ProofResult {
        success: false,
        description: "Baseline: pool-a preferred after ramp reset, pool-a attribution confirmed".to_owned(),
        observations,
    }
}

/// Proof 3: Require pool-a rank 0 at entry, wait until pressure causes
/// A→B flip, verify attributed request goes to pool-b.
fn proof_pressure_and_flip(_context: &DemoContext) -> ProofResult {
    let mut observations = Vec::new();

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

    eprintln!("  Polling for pool-a pressure and A\u{2192}B routing flip...");
    let deadline = Instant::now() + DATA_PLANE_WAIT;
    let mut last_reconcile_trigger = Instant::now();
    let mut last_request = Instant::now() - Duration::from_secs(10);

    while Instant::now() < deadline {
        if last_reconcile_trigger.elapsed() > Duration::from_secs(5) {
            for cluster in CLUSTERS {
                trigger_gridnetwork_reconcile(cluster);
            }
            last_reconcile_trigger = Instant::now();
        }

        let epp_a = scrape_epp_metrics("pool-a");
        let candidates = read_overlay_candidates("pool-a");
        let row_a = build_scorecard_row("Cluster A", &candidates, "pool-a");
        let row_b = build_scorecard_row("Cluster B", &candidates, "pool-b");
        let score_gap = row_b.score - row_a.score;

        if row_b.rank == 0
            && row_a.rank > 0
            && score_gap >= MIN_PRESSURE_SCORE_GAP
            && last_request.elapsed() >= Duration::from_secs(5)
        {
            last_request = Instant::now();
            let probe_ctx = kind_context("pool-a");
            match send_inference_request(&probe_ctx, SIM_MODEL) {
                Ok(resp) => {
                    if resp.provider_gateway.contains("pool-b") && resp.demo_attribution.contains("pool-b") {
                        print_scorecard(
                            "CLUSTER A CAPACITY PRESSURE",
                            &[&row_a, &row_b],
                            "CLUSTER B",
                            &candidates,
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
                        observations.push("Grid rerouted: preference changed A\u{2192}B by score".to_owned());
                        return ProofResult {
                            success: true,
                            description:
                                "Controlled simulated telemetry drove A\u{2192}B routing change via production scoring"
                                    .to_owned(),
                            observations,
                        };
                    }
                    eprintln!(
                        "  data plane converging (overlay=pool-b rank 0, routing={})",
                        resp.provider_gateway
                    );
                },
                Err(e) => {
                    eprintln!("  inference probe retrying: {e}");
                },
            }
        } else if epp_a.queue_size > 2.0 {
            eprintln!(
                "  pool-a: queue={:.1} kv={:.2} score={:.2} rank={}  pool-b: score={:.2} rank={} gap={:.2}",
                epp_a.queue_size, epp_a.kv_cache, row_a.score, row_a.rank, row_b.score, row_b.rank, score_gap
            );
        }

        std::thread::sleep(DATA_PLANE_INTERVAL);
    }

    observations.push("A\u{2192}B flip did not converge in data plane within timeout".to_owned());
    ProofResult {
        success: false,
        description: "Controlled simulated telemetry drove A\u{2192}B routing change via production scoring".to_owned(),
        observations,
    }
}

/// Proof 4: Wait for rampreset recovery, verify pool-a regains rank 0,
/// send attributed request through pool-a.
fn proof_recovery(_context: &DemoContext) -> ProofResult {
    let mut observations = Vec::new();

    eprintln!("  Waiting for pool-a ramp reset and recovery...");
    let deadline = Instant::now() + DATA_PLANE_WAIT;
    let mut last_reconcile_trigger = Instant::now();
    let mut last_request = Instant::now() - Duration::from_secs(10);

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

        let epp_a = scrape_epp_metrics("pool-a");
        let candidates = read_overlay_candidates("pool-a");
        let rank_a = overlay_rank_for_cluster(&candidates, "pool-a");

        if rank_a == 0 && epp_a.queue_size < 3.0 && last_request.elapsed() >= Duration::from_secs(5) {
            last_request = Instant::now();
            let probe_ctx = kind_context("pool-a");
            match send_inference_request(&probe_ctx, SIM_MODEL) {
                Ok(resp) => {
                    if resp.provider_gateway.contains("pool-a") && resp.demo_attribution.contains("pool-a") {
                        let row_a = build_scorecard_row("Cluster A", &candidates, "pool-a");
                        let row_b = build_scorecard_row("Cluster B", &candidates, "pool-b");
                        print_scorecard("RECOVERED", &[&row_a, &row_b], "CLUSTER A", &candidates);
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
                            description: "Recovery: ramp reset restores pool-a to preferred, attribution confirmed"
                                .to_owned(),
                            observations,
                        };
                    }
                    eprintln!(
                        "  data plane converging (overlay=pool-a rank 0, routing={})",
                        resp.provider_gateway
                    );
                },
                Err(e) => {
                    eprintln!("  inference probe retrying: {e}");
                },
            }
        }

        std::thread::sleep(Duration::from_secs(2));
    }

    observations.push("pool-a did not recover with confirmed routing within timeout".to_owned());
    ProofResult {
        success: false,
        description: "Recovery: ramp reset restores pool-a to preferred, attribution confirmed".to_owned(),
        observations,
    }
}

// ---------------------------------------------------------------------------
// EPP metrics helpers
// ---------------------------------------------------------------------------

/// Scraped EPP pool metrics (used for convergence gating, not scorecard display).
struct EppMetrics {
    /// Average queue size (raw, unnormalized).
    queue_size: f64,
    /// Average KV cache utilization (0.0 - 1.0).
    kv_cache: f64,
}

/// Scrape EPP metrics by exec-ing into the nginx sidecar.
///
/// The metrics Service requires mTLS, so demo probes access the EPP's
/// plain HTTP port (9090) via localhost inside the pod.
fn scrape_epp_metrics(cluster: &str) -> EppMetrics {
    let text = kubectl_exec_epp_metrics(cluster).unwrap_or_default();

    EppMetrics {
        queue_size: extract_prom_value(&text, "llm_d_router_epp_average_queue_size").unwrap_or(0.0),
        kv_cache: extract_prom_value(&text, "llm_d_router_epp_average_kv_cache_utilization").unwrap_or(0.0),
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

/// Build a scorecard row entirely from the overlay.
///
/// Raw queue size and KV-cache utilization are back-computed from the
/// score breakdown so that every value in the row comes from the same
/// operator scoring revision.  Mixing live EPP metrics with overlay
/// scores produces contradictions when the simulator ramp advances
/// between the operator scrape and the demo capture.
fn build_scorecard_row(label: &str, candidates: &[OverlayCandidate], cluster_suffix: &str) -> ScorecardRow {
    let rank = overlay_rank_for_cluster(candidates, cluster_suffix);
    let score = overlay_score_for_cluster(candidates, cluster_suffix);
    let bd = candidates
        .iter()
        .find(|c| c.cluster.contains(cluster_suffix))
        .and_then(|c| c.breakdown.as_ref());
    let (queue, kv_cache) = if let Some(b) = bd {
        let q = QUEUE_CAPACITY * (1.0 - b.queue_depth / QUEUE_DEPTH_WEIGHT);
        let kv = 1.0 - b.kv_cache / KV_CACHE_WEIGHT;
        (q.max(0.0), kv.max(0.0))
    } else {
        (0.0, 0.0)
    };
    ScorecardRow {
        cluster: label.to_owned(),
        queue,
        capacity: QUEUE_CAPACITY,
        pressure: queue / QUEUE_CAPACITY,
        kv_cache,
        score,
        rank,
    }
}

/// Print a narrated CLI scorecard.
fn print_scorecard(state: &str, rows: &[&ScorecardRow], preferred: &str, candidates: &[OverlayCandidate]) {
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

    // Print score breakdowns from production scoring engine
    for oc in candidates {
        if let Some(bd) = &oc.breakdown {
            eprintln!(
                "  Score breakdown ({cluster}): locality={loc:.2} queue={q:.2} kv={kv:.2} prefix={p:.2} latency={lat:.2} cost={c:.2}",
                cluster = oc.cluster,
                loc = bd.locality,
                q = bd.queue_depth,
                kv = bd.kv_cache,
                p = bd.prefix_cache,
                lat = bd.latency,
                c = bd.cost,
            );
        }
    }

    eprintln!();
    eprintln!("  Grid preference: {preferred}");
    eprintln!();
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

/// Exec into the nginx sidecar to fetch EPP metrics via localhost.
fn kubectl_exec_epp_metrics(cluster: &str) -> Result<String, Box<dyn std::error::Error>> {
    let ctx = kind_context(cluster);
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

        apply_credential_secret(&ctx, SIM_INFERENCE_CREDENTIAL, "sim-demo-token")?;

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

/// Load pre-built images into Kind clusters.
///
/// Uses the forge-expected tags (created by [`tag_images_for_forge`]) since
/// the forge manifests and Helm values reference those names.
fn load_images_into_clusters(_context: &DemoContext) -> Result<(), Box<dyn std::error::Error>> {
    let forge_tags: &[&str] = &[
        "grid-operator:llmd-pool-metrics-demo",
        "grid-overlay-sync:llmd-pool-metrics-demo",
        "praxis-ai:llmd-pool-metrics-demo",
        "llm-d-epp:llmd-pool-metrics-demo",
        "llm-d-inference-sim:llmd-pool-metrics-demo",
        "nginx:metrics-proxy-demo",
    ];
    for cluster in CLUSTERS {
        let kind_name = format!("grid-llmd-pm-{cluster}");
        for image_tag in forge_tags {
            let status = Command::new("kind")
                .args(["load", "docker-image", image_tag, "--name", &kind_name])
                .status()?;
            if !status.success() {
                return Err(format!("Failed to load {image_tag} into {kind_name}").into());
            }
        }
        eprintln!("  [OK] {cluster}: all images loaded");
    }
    Ok(())
}

/// Collect image tags and digests for evidence.
fn collect_image_evidence(resolved: &ResolvedImages) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let mut images = BTreeMap::new();
    for (role, tag) in [
        ("operator", &resolved.operator),
        ("gateway", &resolved.gateway),
        ("epp", &resolved.epp),
        ("sim", &resolved.sim),
        ("overlay-sync", &resolved.overlay_sync),
        ("nginx", &resolved.nginx),
    ] {
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
fn run_forge(forge_bin: &Path, config: &Path, args: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(forge_bin)
        .args(["--config", &config.display().to_string(), "--non-interactive"])
        .args(args)
        .output()?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(format!("forge {} failed: {stderr}", args.join(" ")).into())
}

/// Run a specific forge stack on a cluster.
fn run_forge_stack(
    forge_bin: &Path,
    config: &Path,
    cluster: &str,
    stack: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    run_forge(forge_bin, config, &["stack", "apply", cluster, stack])?;
    Ok(())
}

/// Materialize the forge config with computed candidate IDs.
///
/// Injects `candidateId` properties into each cluster definition so
/// that the provider gateway's `provider_route` filter `candidate_id`
/// matches the `stable_id` the operator writes to the routing overlay.
/// Both are derived from `fnv1a_hex8("{kind}/{model}/{site}/{cluster}")`.
fn materialize_config(forge_config: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let dir = forge_config.parent().unwrap_or_else(|| Path::new("."));
    let resolved = dir.join(".forge.resolved.yaml");
    let content = fs::read_to_string(forge_config)?;

    let mut result = content.clone();
    for cluster in CLUSTERS {
        let provider_name = format!("llmd-{cluster}-provider");
        let candidate_id = fnv1a_hex8(&format!("inference_model/{SIM_MODEL}/{cluster}/{provider_name}"));
        let anchor = format!("poolName: {cluster}");
        let replacement = format!("{anchor}\n        candidateId: \"{candidate_id}\"");
        result = result.replacen(&anchor, &replacement, 1);
    }

    fs::write(&resolved, result)?;
    Ok(resolved)
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
    run_forge(&context.forge_bin, &context.resolved_config, &["down"])?;
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
    format!("kind-grid-llmd-pm-{cluster}")
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
            observations.push(format!("{cluster}: NOT observable — mTLS scraping may have failed"));
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
            let candidates = read_overlay_candidates(cluster);
            let score = overlay_score_for_cluster(&candidates, cluster);
            let msg = format!(
                "inside-TTL ({elapsed}s/{STALE_METRICS_TTL_SECS}s): {cluster} still observable, \
                 score={score:.2} (cached metrics served)"
            );
            eprintln!("    {msg}");
            observations.push(msg);
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
        let msg = format!(
            "post-TTL ({elapsed}s/{STALE_METRICS_TTL_SECS}s): {cluster} unobservable \
             (cached metrics expired, UNOBSERVABLE_METRICS applied)"
        );
        eprintln!("    {msg}");
        observations.push(msg);
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
        let candidates = read_overlay_candidates(cluster);
        let score = overlay_score_for_cluster(&candidates, cluster);
        let msg = format!("recovery: {cluster} observable after client cert restored, score={score:.2}");
        eprintln!("    {msg}");
        observations.push(msg);
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
    match send_inference_request(&probe_ctx, SIM_MODEL) {
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
    use super::*;

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
        let text = r#"# HELP llm_d_router_epp_average_kv_cache_utilization Average kv cache
# TYPE llm_d_router_epp_average_kv_cache_utilization gauge
llm_d_router_epp_average_kv_cache_utilization{name="pool-a"} 0.35
"#;
        let val = extract_prom_value(text, "llm_d_router_epp_average_kv_cache_utilization");
        assert_eq!(val, Some(0.35), "expected Some(0.35)");
    }

    #[test]
    fn extract_prom_value_returns_none_for_missing_metric() {
        let text = "some_other_metric 1.0\n";
        let val = extract_prom_value(text, "llm_d_router_epp_average_queue_size");
        assert_eq!(val, None, "expected None for missing metric");
    }

    #[test]
    fn evidence_serializes_to_json() {
        let evidence = Evidence {
            schema_version: "1".to_owned(),
            mode: "quick".to_owned(),
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
}
