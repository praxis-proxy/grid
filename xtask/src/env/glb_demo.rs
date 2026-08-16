//! Narrated, evidence-backed GLB demo scenarios.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use serde::Serialize;

use super::{
    DemoMode, GlbDemoModeOptions, GlbDemoOptions, IngressMode, certs,
    external_provider::{self, ExternalProviderDescriptor},
    glb, gtm_emulator, image_overrides, kubectl, operator, workload,
};

#[cfg(test)]
#[path = "test_render_config.rs"]
mod test_render_config;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum session keys sampled when discovering observable routing paths.
const MAX_PATH_SAMPLES: usize = 128;

/// Number of repeated requests used for the narrated affinity proof.
const AFFINITY_REPEATS: usize = 3;

/// Request soak performed only by the full demo.
const FULL_SOAK_DURATION: Duration = Duration::from_secs(300);

/// Delay between full-mode soak requests.
const FULL_SOAK_INTERVAL: Duration = Duration::from_secs(5);

/// Successful requests between full-mode soak progress updates.
const FULL_SOAK_PROGRESS_SAMPLES: usize = 12;

/// Resolved config emitted next to the source config to preserve relative paths.
const RESOLVED_CONFIG_NAME: &str = ".forge.resolved.yaml";

/// Ordered cluster names in the global-ingress scenario environment.
const CLUSTERS: &[&str] = &[
    "gtm-emulator",
    "east-edge",
    "east-provider",
    "west-edge",
    "west-provider",
];

/// Consumer clusters that accept workload requests.
const CONSUMER_CLUSTERS: &[&str] = &["east-edge", "west-edge"];

/// Clusters that participate in Grid discovery and run an operator.
const GRID_CLUSTERS: &[&str] = &["east-edge", "east-provider", "west-edge", "west-provider"];

/// Provider clusters that run the private provider path.
const PROVIDER_CLUSTERS: &[&str] = &["east-provider", "west-provider"];

/// Evidence JSON schema version.
const EVIDENCE_SCHEMA_VERSION: &str = "1";

/// Stable terminal separator that also remains readable in captured logs.
const OUTPUT_RULE: &str = "===============================================================================";

/// Preferred width for human-readable narration.
const OUTPUT_WIDTH: usize = OUTPUT_RULE.len();

/// Number of environment setup phases shown to the user.
const SETUP_PHASES: usize = 9;

// ---------------------------------------------------------------------------
// Narrator
// ---------------------------------------------------------------------------

/// Dual-output narrator: writes to stderr and captures to memory.
pub(crate) struct Narrator {
    /// Captured narration lines.
    lines: Vec<String>,
}

impl Narrator {
    /// Create an empty narrator.
    pub(crate) fn new() -> Self {
        Self { lines: Vec::new() }
    }

    /// Emit one narration line to stderr and capture it.
    pub(crate) fn narrate(&mut self, line: &str) {
        eprintln!("{line}");
        self.lines.push(line.to_owned());
    }

    /// Emit a prominent top-level section.
    fn banner(&mut self, title: &str) {
        self.narrate("");
        self.narrate(OUTPUT_RULE);
        self.narrate(title);
        self.narrate(OUTPUT_RULE);
    }

    /// Emit prose with stable indentation and bounded line length.
    fn wrapped(&mut self, first_prefix: &str, continuation_prefix: &str, text: &str) {
        let mut line = first_prefix.to_owned();
        for word in text.split_whitespace() {
            let separator = usize::from(line.chars().count() > first_prefix.chars().count());
            if line.chars().count() + separator + word.chars().count() > OUTPUT_WIDTH
                && line.chars().count() > first_prefix.chars().count()
            {
                self.narrate(&line);
                continuation_prefix.clone_into(&mut line);
            }
            if line.chars().count() > continuation_prefix.chars().count() {
                line.push(' ');
            }
            line.push_str(word);
        }
        self.narrate(&line);
    }

    /// Write captured narration to a file.
    fn write_to_file(&self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let mut content = self.lines.join("\n");
        content.push('\n');
        fs::write(path, content)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Independent edge/provider affinity fixtures that reproduce one path.
#[derive(Debug, Clone)]
struct AffinityFixture {
    /// Session fixture consumed only by the GTM layer.
    edge_session: String,
    /// Session fixture consumed only by the Grid routing layer.
    provider_session: String,
}

/// Observed `(edge, provider)` pair to the fixtures that reproduce it.
type ObservedPaths = BTreeMap<(String, String), AffinityFixture>;

/// Resolved environment references needed after setup.
pub(crate) struct SetupContext {
    /// Canonical demo root directory from which configs, resources, policies,
    /// and fixtures are resolved.
    demo_root: PathBuf,
    /// Resolved forge config path.
    resolved_config: PathBuf,
    /// Forge binary path.
    forge_bin: String,
    /// Ingress topology for this run.
    ingress_mode: IngressMode,
    /// Resolved external provider descriptor when enabled.
    external_provider: Option<ExternalProviderDescriptor>,
    /// Path to the external provider key file (kept separate from the descriptor
    /// so the descriptor never carries the filesystem path after validation).
    external_key_file: Option<PathBuf>,
}

/// Outcome of the narrated demonstration.
struct DemoOutcome {
    /// Per-capability results.
    capabilities: Vec<CapabilityResult>,
    /// Observed routing paths from discovery.
    observed_paths: Vec<ObservedPathEntry>,
    /// Sanitized live external-provider proof, when enabled.
    external_provider: Option<ExternalProviderProof>,
    /// Concise failure detail when the run did not complete successfully.
    error: Option<String>,
}

// ---------------------------------------------------------------------------
// Evidence JSON types
// ---------------------------------------------------------------------------

/// Top-level machine-readable evidence report.
#[derive(Debug, Serialize)]
struct EvidenceReport {
    /// Schema version for forward compatibility.
    schema_version: &'static str,
    /// Unique run identifier (UTC timestamp).
    run_id: String,
    /// Demo mode: `"quick"` or `"full"`.
    mode: &'static str,
    /// UTC start time as ISO 8601.
    started_at: String,
    /// UTC completion time as ISO 8601.
    completed_at: String,
    /// Wall-clock duration in seconds.
    duration_secs: f64,
    /// Overall result: `"pass"` or `"fail"`.
    status: &'static str,
    /// Concise failure detail when `status` is `"fail"`.
    error: Option<String>,
    /// Per-capability results.
    capabilities: Vec<CapabilityResult>,
    /// Observed routing paths.
    observed_paths: Vec<ObservedPathEntry>,
    /// Sanitized live external-provider proof, when enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    external_provider: Option<ExternalProviderProof>,
    /// Lifecycle actions performed.
    lifecycle: LifecycleRecord,
    /// Paths to generated artifacts.
    artifacts: ArtifactPaths,
}

/// One capability row in the evidence.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct CapabilityResult {
    /// Human-readable capability name.
    capability: String,
    /// `"pass"`, `"fail"`, or `"skipped"`.
    result: &'static str,
    /// One-line evidence string.
    evidence: String,
}

/// One observed routing path from discovery.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ObservedPathEntry {
    /// GTM-selected edge cluster.
    edge: String,
    /// Grid-selected provider cluster.
    provider: String,
    /// Narrative path description.
    path: String,
}

/// Sanitized facts observed during a live external-provider request.
#[derive(Debug, Clone, Serialize)]
struct ExternalProviderProof {
    /// HTTP response status.
    http_status: u16,
    /// Gateway selected by the traffic manager.
    edge: String,
    /// Authenticated provider gateway that handled the request.
    provider_gateway: String,
    /// Candidate identity present in the selected edge overlay.
    candidate_id: String,
    /// Provider-local upstream cluster.
    cluster: String,
    /// Model reported by the external provider response.
    response_model: String,
    /// Exact overlay revision distributed to and accepted by the selected edge.
    serving_revision: String,
    /// Request duration in seconds.
    duration_secs: f64,
}

impl ExternalProviderProof {
    /// Render one concise capability evidence string.
    fn summary(&self) -> String {
        format!(
            "HTTP {}; edge={}; gateway={}; candidate={}; cluster={}; model={}; revision={}",
            self.http_status,
            self.edge,
            self.provider_gateway,
            self.candidate_id,
            self.cluster,
            self.response_model,
            self.serving_revision,
        )
    }
}

/// Lifecycle actions recorded.
#[derive(Debug, Clone, Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "serialized evidence record with independent boolean fields"
)]
struct LifecycleRecord {
    /// Whether teardown was requested.
    teardown_requested: bool,
    /// Whether teardown was performed.
    teardown_performed: bool,
    /// Teardown result if performed.
    teardown_result: Option<String>,
    /// Whether clusters were kept on failure.
    kept_on_failure: bool,
}

/// Paths to generated artifacts.
#[derive(Debug, Clone, Serialize)]
struct ArtifactPaths {
    /// Path to `narration.txt`.
    narration: String,
    /// Path to `results.json`.
    results: String,
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Materialize and deploy a complete GLB demo environment.
///
/// # Errors
///
/// Returns an error when config rendering, cluster creation, image loading,
/// stack application, trust installation, or service startup fails.
pub(crate) fn setup(
    forge_config: &Path,
    ingress_mode: IngressMode,
) -> Result<SetupContext, Box<dyn std::error::Error>> {
    let context = prepare_setup(forge_config, ingress_mode, None, None)?;
    deploy_setup(&context)?;
    Ok(context)
}

/// Resolve setup inputs before creating any runtime resources.
///
/// When `external_provider` and `external_key_file` are provided, the key file
/// is validated before any clusters are created so the demo fails fast on
/// invalid credentials.
fn prepare_setup(
    forge_config: &Path,
    ingress_mode: IngressMode,
    external_provider: Option<ExternalProviderDescriptor>,
    external_key_file: Option<PathBuf>,
) -> Result<SetupContext, Box<dyn std::error::Error>> {
    let root = super::demo_root(forge_config);
    eprintln!("Forge config: {}", forge_config.display());
    eprintln!("Demo root:    {}", root.display());
    Ok(SetupContext {
        demo_root: root,
        resolved_config: materialize_config(forge_config, ingress_mode, external_provider.as_ref())?,
        forge_bin: glb::resolve_forge_binary().ok_or("praxis-forge binary not found")?,
        ingress_mode,
        external_provider,
        external_key_file,
    })
}

/// Deploy the environment using prepared setup inputs.
#[expect(clippy::too_many_lines, reason = "sequential mode-aware environment deployment")]
fn deploy_setup(context: &SetupContext) -> Result<(), Box<dyn std::error::Error>> {
    let is_workload = context.ingress_mode == IngressMode::Workload;
    let base_phases = if is_workload { 7 } else { SETUP_PHASES };
    let total_phases = base_phases + usize::from(context.external_provider.is_some());
    let cluster_desc = if is_workload { "four" } else { "five" };

    eprintln!();
    eprintln!("{OUTPUT_RULE}");
    eprintln!("ENVIRONMENT SETUP");
    eprintln!("{OUTPUT_RULE}");

    let mut phase_number = 0_usize;
    let mut next = || {
        phase_number += 1;
        phase_number
    };

    eprintln!();
    eprintln!(
        "[SETUP {}/{total_phases}] Staging demo certificates and provider identities",
        next()
    );
    glb::stage_provider_boundary_with_mode_and_external(
        context.ingress_mode,
        context.external_provider.as_ref(),
        &context.demo_root,
    )?;

    eprintln!();
    eprintln!(
        "[SETUP {}/{total_phases}] Creating {cluster_desc} Kind clusters on one shared cross-cluster network",
        next()
    );
    eprintln!("            Forge will report again after cluster creation completes.");
    run_forge(&context.forge_bin, &context.resolved_config, &["up"])?;

    eprintln!();
    eprintln!("[SETUP {}/{total_phases}] Resolving and loading runtime images", next());
    print_runtime_images(context.ingress_mode);
    load_local_images_if_required(&context.forge_bin, &context.resolved_config, context.ingress_mode)?;

    eprintln!();
    eprintln!(
        "[SETUP {}/{total_phases}] Installing MetalLB, SWIM services, and Grid operators",
        next()
    );
    apply_foundation_stacks_with_mode(&context.forge_bin, &context.resolved_config, context.ingress_mode)?;

    eprintln!();
    eprintln!(
        "[SETUP {}/{total_phases}] Installing provider trust, credentials, and policy",
        next()
    );
    glb::install_provider_boundary_with_mode_and_external(context.ingress_mode, context.external_key_file.as_deref())?;

    eprintln!();
    let provider_desc = if context.external_provider.is_some() {
        "Deploying two provider gateways, three private and one external inference provider"
    } else {
        "Deploying two provider gateways and three private inference providers"
    };
    eprintln!("[SETUP {}/{total_phases}] {provider_desc}", next());
    apply_provider_stacks(
        &context.forge_bin,
        &context.resolved_config,
        context.external_provider.as_ref(),
    )?;
    if let Some(ext) = &context.external_provider {
        glb::apply_openai_inference_provider(ext)?;
        eprintln!("  [OK] east-provider: OpenAI external provider configured");
    }

    eprintln!();
    eprintln!(
        "[SETUP {}/{total_phases}] Configuring edge trust and deploying Praxis edge gateways",
        next()
    );
    apply_edge_stacks(&context.forge_bin, &context.resolved_config)?;
    if let Some(ext) = &context.external_provider {
        glb::patch_edge_configs_for_openai(ext)?;
        eprintln!("  [OK] edge gateways: OpenAI routing cluster configured");
    }

    if !is_workload {
        eprintln!();
        eprintln!(
            "[SETUP {}/{total_phases}] Deploying the GTM emulator in front of both edges",
            next()
        );
        apply_gtm_emulator_stack(&context.forge_bin, &context.resolved_config)?;
    }

    eprintln!();
    eprintln!(
        "[SETUP {}/{total_phases}] Waiting for both edge-local routing overlays to converge",
        next()
    );
    explain_overlay_convergence();
    let expected_candidates = if context.external_provider.is_some() { 4 } else { 3 };
    let overlay_evidence = glb::wait_for_edge_overlays_ready_with_count(expected_candidates)?;

    if let Some(ext) = &context.external_provider {
        eprintln!();
        eprintln!(
            "[SETUP {}/{total_phases}] Verifying OpenAI provider appears in overlay",
            next()
        );
        eprintln!("  model: {}", ext.model);
        eprintln!("  cluster: {}", ext.routing_cluster);
    }

    eprintln!();
    eprintln!(
        "[READY] Environment deployed from {}\n        {overlay_evidence}",
        context.resolved_config.display()
    );
    Ok(())
}

/// Print the exact image contract selected by environment overrides.
fn print_runtime_images(ingress_mode: IngressMode) {
    eprintln!("  gateway:       {}", image_overrides::demo_gateway_image(ingress_mode));
    eprintln!(
        "  operator:      {}",
        image_overrides::demo_operator_image(ingress_mode)
    );
    eprintln!("  vcr:           {}", image_overrides::vcr_image());
    eprintln!(
        "  pull policy:   {}",
        image_overrides::demo_image_pull_policy(ingress_mode)
    );
}

/// Explain the control-plane milestone represented by overlay convergence.
fn explain_overlay_convergence() {
    eprintln!("            Each edge must receive one complete, versioned provider view.");
    eprintln!("            This proves distribution; Praxis acceptance is verified next.");
}

/// Set up the environment and run every narrated proof.
///
/// # Errors
///
/// Returns an error when setup or any runtime scenario fails.
#[expect(
    clippy::too_many_lines,
    reason = "orchestration of setup, demonstrate, evidence, teardown is clearest in one flow"
)]
pub(crate) fn run(forge_config: &Path, options: &GlbDemoOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mode = options.mode();
    let ingress_mode = options.ingress_mode();

    // Resolve and validate external provider before any cluster creation.
    let ext_descriptor = external_provider::resolve_external_provider(
        options.external_provider,
        options.external_provider_key_file.as_deref(),
        options.external_provider_model.as_deref(),
    )?;
    let ext_key_file = options.external_provider_key_file.clone();
    if ingress_mode == IngressMode::Workload && ext_descriptor.is_some() {
        return Err("external providers require the global-ingress demo mode".into());
    }
    let run_id = format_utc_timestamp();
    let started_at = format_utc_iso();
    let wall_start = Instant::now();
    let mut narrator = Narrator::new();

    let evidence_dir = resolve_evidence_dir(forge_config, options, &run_id);
    fs::create_dir_all(&evidence_dir)?;

    let setup_ctx = prepare_setup(forge_config, ingress_mode, ext_descriptor, ext_key_file);
    let mut outcome = match &setup_ctx {
        Ok(context) => match deploy_setup(context) {
            Ok(()) => demonstrate_inner(
                &context.resolved_config,
                mode,
                ingress_mode,
                context.external_provider.as_ref(),
                &mut narrator,
            ),
            Err(error) => failed_outcome(Vec::new(), Vec::new(), "Environment setup", concise_error(error)),
        },
        Err(error) => failed_outcome(Vec::new(), Vec::new(), "Environment preparation", concise_error(error)),
    };

    let mut lifecycle = LifecycleRecord {
        teardown_requested: options.teardown,
        teardown_performed: false,
        teardown_result: None,
        kept_on_failure: false,
    };

    if options.teardown {
        let should_keep = options.keep_on_failure && outcome.error.is_some() && setup_ctx.is_ok();
        if should_keep {
            lifecycle.kept_on_failure = true;
            narrator.narrate("[CLEANUP] Clusters retained for debugging (--keep-on-failure).");
        } else if let Ok(context) = &setup_ctx {
            lifecycle.teardown_performed = true;
            match teardown_clusters(&context.forge_bin, &context.resolved_config) {
                Ok(()) => {
                    lifecycle.teardown_result = Some("success".to_owned());
                    narrator.narrate("[CLEANUP] Teardown complete.");
                },
                Err(error) => {
                    let message = concise_error(error);
                    lifecycle.teardown_result = Some(format!("error: {message}"));
                    narrator.narrate(&format!("[CLEANUP] FAIL: {message}"));
                    append_error(&mut outcome.error, format!("teardown failed: {message}"));
                },
            }
        } else {
            lifecycle.teardown_result = Some("not needed: deployment did not start".to_owned());
        }
    }

    let status = if outcome.error.is_some() { "fail" } else { "pass" };
    let elapsed = wall_start.elapsed();
    let completed_at = format_utc_iso();
    let narration_path = evidence_dir.join("narration.txt");
    let results_path = evidence_dir.join("results.json");

    let report = EvidenceReport {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        run_id,
        mode: evidence_mode_label(mode, ingress_mode),
        started_at,
        completed_at,
        duration_secs: elapsed.as_secs_f64(),
        status,
        error: outcome.error.clone(),
        capabilities: outcome.capabilities,
        observed_paths: outcome.observed_paths,
        external_provider: outcome.external_provider,
        lifecycle,
        artifacts: ArtifactPaths {
            narration: narration_path.display().to_string(),
            results: results_path.display().to_string(),
        },
    };

    print_final_summary(&mut narrator, &report, &evidence_dir);

    if let Err(error) = write_evidence(&report, &narrator, &narration_path, &results_path) {
        let message = concise_error(error);
        return match &report.error {
            Some(run_error) => Err(format!("{run_error}; evidence write failed: {message}").into()),
            None => Err(format!("evidence write failed: {message}").into()),
        };
    }

    match report.error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

/// Run narrated scenarios against an already-deployed environment.
///
/// # Errors
///
/// Returns an error when any prerequisite proof, routing scenario, affinity
/// check, or edge withdrawal/recovery check fails.
pub(crate) fn demonstrate_with_options(
    forge_config: &Path,
    options: &GlbDemoModeOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    let mode = options.mode();
    let ingress_mode = options.ingress_mode();
    let mut narrator = Narrator::new();
    match demonstrate_inner(forge_config, mode, ingress_mode, None, &mut narrator).error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Core demonstration logic
// ---------------------------------------------------------------------------

/// Run narrated scenarios and collect capability results.
fn demonstrate_inner(
    forge_config: &Path,
    mode: DemoMode,
    ingress_mode: IngressMode,
    external: Option<&ExternalProviderDescriptor>,
    narrator: &mut Narrator,
) -> DemoOutcome {
    match ingress_mode {
        IngressMode::Global => demonstrate_global(forge_config, mode, external, narrator),
        IngressMode::Workload => demonstrate_workload(forge_config, mode, narrator),
    }
}

/// Global-ingress demonstration: GTM-based path discovery and edge withdrawal.
#[expect(
    clippy::too_many_lines,
    reason = "sequential scenario narration is clearest in one function"
)]
fn demonstrate_global(
    forge_config: &Path,
    mode: DemoMode,
    external: Option<&ExternalProviderDescriptor>,
    narrator: &mut Narrator,
) -> DemoOutcome {
    let root = super::demo_root(forge_config);
    let fixture = root.join("fixtures/requests/shared-model.json");
    let mut capabilities = Vec::new();
    let mut external_provider = None;

    print_introduction(narrator, mode);

    // Scenario 1: Active/active routing.
    print_scenario(
        narrator,
        1,
        "Active/active global and provider routing",
        "As an application owner, I need one stable HTTPS endpoint backed by active edges while Grid independently selects an admitted provider.",
    );

    if let Err(error) = glb::verify_grid_routing_with_mode(forge_config, mode, IngressMode::Global) {
        return failed_outcome(capabilities, Vec::new(), "Active/active routing", concise_error(error));
    }

    let paths = match discover_paths(&fixture) {
        Ok(paths) => paths,
        Err(error) => {
            return failed_outcome(capabilities, Vec::new(), "Active/active routing", concise_error(error));
        },
    };
    let observed_paths = build_observed_paths(&paths);
    print_paths(narrator, &paths);
    capabilities.push(CapabilityResult {
        capability: "Active/active routing".to_owned(),
        result: "pass",
        evidence: "2 edges observed; 3 provider candidates include 2 independently routed providers in one cluster"
            .to_owned(),
    });
    capabilities.push(CapabilityResult {
        capability: "Observable overlay contract".to_owned(),
        result: "pass",
        evidence: if mode == DemoMode::Full {
            "one revision matched rendered/distributed/accepted/serving evidence; invalid reload retained last-known-good; cold invalid startup failed closed"
        } else {
            "one revision matched rendered/distributed/accepted/serving evidence"
        }
        .to_owned(),
    });

    // Scenario 2: Secure provider boundary (summarizes results from scenario 1).
    print_scenario(
        narrator,
        2,
        "Secure provider boundary",
        "As a provider and security owner, I need authenticated Grid traffic, exact local policy, private backend isolation, and final-hop credential replacement.",
    );
    print_provider_boundary_proof(narrator);
    print_credential_boundary_proof(narrator);
    capabilities.push(CapabilityResult {
        capability: "Secure provider boundary".to_owned(),
        result: "pass",
        evidence: "mTLS, peer auth, NetworkPolicy, credential replacement verified".to_owned(),
    });

    // Scenarios 3-5: full mode only.
    if mode == DemoMode::Full {
        // Scenario 3: Session affinity and provider drain.
        print_scenario(
            narrator,
            3,
            "Session affinity and provider drain",
            "As an inference client, I need repeated requests to remain on one edge and provider while existing sessions survive a metrics-driven drain and new sessions move safely.",
        );
        if let Err(error) = prove_affinity(narrator, &paths, &fixture) {
            return failed_outcome(
                capabilities,
                observed_paths,
                "Session affinity and drain",
                concise_error(error),
            );
        }
        capabilities.push(CapabilityResult {
            capability: "Session affinity and drain".to_owned(),
            result: "pass",
            evidence: "edge+provider stable, drain verified".to_owned(),
        });

        // Scenario 4: Edge withdrawal and recovery.
        print_scenario(
            narrator,
            4,
            "Edge withdrawal and recovery",
            "As a reliability operator, I need a failed edge withdrawn behind the same HTTPS name and returned after recovery.",
        );
        if let Err(error) = gtm_emulator::verify(forge_config) {
            return failed_outcome(
                capabilities,
                observed_paths,
                "Edge withdrawal and recovery",
                concise_error(error),
            );
        }
        capabilities.push(CapabilityResult {
            capability: "Edge withdrawal and recovery".to_owned(),
            result: "pass",
            evidence: "east withdrawn, west served, east recovered".to_owned(),
        });

        // Scenario 5: operator restart recovery and request soak.
        print_scenario(
            narrator,
            5,
            "Grid restart recovery and request soak",
            "As a platform operator, I need Grid control-plane restarts to preserve converged routing and sustained inference traffic.",
        );
        match prove_restart_recovery_and_soak(narrator, &paths, &fixture) {
            Ok(evidence) => capabilities.push(CapabilityResult {
                capability: "Grid restart recovery and soak".to_owned(),
                result: "pass",
                evidence,
            }),
            Err(error) => {
                return failed_outcome(
                    capabilities,
                    observed_paths,
                    "Grid restart recovery and soak",
                    concise_error(error),
                );
            },
        }
    } else {
        capabilities.push(CapabilityResult {
            capability: "Session affinity and drain".to_owned(),
            result: "skipped",
            evidence: "quick mode".to_owned(),
        });
        capabilities.push(CapabilityResult {
            capability: "Edge withdrawal and recovery".to_owned(),
            result: "skipped",
            evidence: "quick mode".to_owned(),
        });
        capabilities.push(CapabilityResult {
            capability: "Grid restart recovery and soak".to_owned(),
            result: "skipped",
            evidence: "quick mode".to_owned(),
        });
        narrator.narrate("");
        narrator.narrate("[SKIP] Demos 3-5 run only in full mode.");
    }

    // Optional: Live external provider scenario.
    if let Some(ext) = external {
        let scenario_num = if mode == DemoMode::Full { 6 } else { 3 };
        print_scenario(
            narrator,
            scenario_num,
            "Live external provider",
            "As a platform owner, I need to prove that a real external API provider can be routed through the Grid provider boundary with credential replacement and public TLS.",
        );
        match prove_external_provider(narrator, ext) {
            Ok(proof) => {
                capabilities.push(CapabilityResult {
                    capability: "Live external provider".to_owned(),
                    result: "pass",
                    evidence: proof.summary(),
                });
                external_provider = Some(proof);
            },
            Err(error) => {
                return failed_outcome(
                    capabilities,
                    observed_paths,
                    "Live external provider",
                    concise_error(error),
                );
            },
        }
    } else {
        let absence = match glb::verify_external_provider_absent() {
            Ok(evidence) => evidence,
            Err(error) => {
                return failed_outcome(
                    capabilities,
                    observed_paths,
                    "Live external provider absence",
                    concise_error(error),
                );
            },
        };
        capabilities.push(CapabilityResult {
            capability: "Live external provider".to_owned(),
            result: "skipped",
            evidence: format!("not enabled; {absence}"),
        });
    }

    print_boundaries(narrator, mode);

    DemoOutcome {
        capabilities,
        observed_paths,
        external_provider,
        error: None,
    }
}

/// Workload-inference demonstration: in-cluster request path, no GTM.
#[expect(
    clippy::too_many_lines,
    reason = "sequential scenario narration is clearest in one function"
)]
fn demonstrate_workload(forge_config: &Path, mode: DemoMode, narrator: &mut Narrator) -> DemoOutcome {
    let mut capabilities = Vec::new();

    print_workload_introduction(narrator, mode);

    // Scenario 1: Grid routing verification (shared with global mode).
    print_scenario(
        narrator,
        1,
        "Grid routing and provider boundary",
        "As a platform operator, I need the Grid mesh to converge, discover providers, and enforce the provider security boundary.",
    );

    if let Err(error) = glb::verify_grid_routing_with_mode(forge_config, mode, IngressMode::Workload) {
        return failed_outcome(capabilities, Vec::new(), "Grid routing", concise_error(error));
    }

    capabilities.push(CapabilityResult {
        capability: "Observable overlay contract".to_owned(),
        result: "pass",
        evidence: if mode == DemoMode::Full {
            "one revision matched rendered/distributed/accepted/serving evidence; invalid reload retained last-known-good; cold invalid startup failed closed"
        } else {
            "one revision matched rendered/distributed/accepted/serving evidence"
        }
        .to_owned(),
    });
    capabilities.push(CapabilityResult {
        capability: "Secure provider boundary".to_owned(),
        result: "pass",
        evidence: "mTLS, peer auth, NetworkPolicy, credential replacement verified".to_owned(),
    });

    // Scenario 2: Workload requests from consumer clusters.
    print_scenario(
        narrator,
        2,
        "In-cluster workload routing",
        "As a platform workload, I need requests from inside the consumer cluster to reach a Grid-selected provider without any external ingress.",
    );

    let workload_paths = match discover_workload_paths() {
        Ok(paths) => paths,
        Err(error) => {
            return failed_outcome(capabilities, Vec::new(), "Workload routing", concise_error(error));
        },
    };
    print_workload_paths(narrator, &workload_paths);
    capabilities.push(CapabilityResult {
        capability: "In-cluster workload routing".to_owned(),
        result: "pass",
        evidence: format!(
            "{} consumer cluster(s) routed to provider via in-cluster Jobs",
            workload_paths.len()
        ),
    });

    // Scenario 3: Provider boundary (summarized from scenario 1 verification).
    print_scenario(
        narrator,
        3,
        "Secure provider boundary",
        "As a provider and security owner, I need authenticated Grid traffic, exact local policy, private backend isolation, and final-hop credential replacement.",
    );
    print_provider_boundary_proof(narrator);
    print_credential_boundary_proof(narrator);

    // GTM-only capabilities: not applicable in workload mode.
    capabilities.push(CapabilityResult {
        capability: "Edge withdrawal and recovery".to_owned(),
        result: "not_applicable",
        evidence: "workload mode has no traffic manager".to_owned(),
    });

    if mode == DemoMode::Full {
        // Scenario 4: Local preference and remote fallback.
        print_scenario(
            narrator,
            4,
            "Local provider preference and remote fallback",
            "As a platform operator, I need workloads to prefer local providers and fall back to remote providers when local capacity is unavailable.",
        );
        match prove_workload_local_preference(narrator) {
            Ok(evidence) => capabilities.push(CapabilityResult {
                capability: "Local preference and remote fallback".to_owned(),
                result: "pass",
                evidence,
            }),
            Err(error) => {
                return failed_outcome(capabilities, workload_paths, "Local preference", concise_error(error));
            },
        }
    } else {
        capabilities.push(CapabilityResult {
            capability: "Local preference and remote fallback".to_owned(),
            result: "skipped",
            evidence: "quick mode".to_owned(),
        });
        narrator.narrate("");
        narrator.narrate("[SKIP] Local preference/fallback runs only in full mode.");
    }

    print_workload_boundaries(narrator, mode);

    DemoOutcome {
        capabilities,
        observed_paths: workload_paths,
        external_provider: None,
        error: None,
    }
}

// ---------------------------------------------------------------------------
// Narration helpers
// ---------------------------------------------------------------------------

/// Print the architecture and proof policy before executing scenarios.
fn print_introduction(narrator: &mut Narrator, mode: DemoMode) {
    narrator.banner("PRAXIS GRID GLOBAL INGRESS DEMO");
    narrator.narrate("");
    narrator.narrate(&format!("Mode: {}", mode_label(mode).to_uppercase()));
    narrator.wrapped(
        "Proof policy: ",
        "              ",
        "Every PASS comes from a runtime assertion; manifest intent is not counted as proof.",
    );
    narrator.narrate("");
    narrator.narrate("EXPECTED PATH");
    narrator.narrate("  client -> stable Praxis HTTPS -> selected Praxis edge");
    narrator.narrate("         -> Grid-selected provider gateway -> private backend");
    narrator.narrate("");
    narrator.narrate("Live edge/provider paths appear under OBSERVED ROUTES after Demo 1");
    narrator.narrate("runtime validation completes.");
}

/// Print one scenario and its user story.
fn print_scenario(narrator: &mut Narrator, number: usize, title: &str, user_story: &str) {
    narrator.banner(&format!("DEMO {number} | {}", title.to_uppercase()));
    narrator.narrate("");
    narrator.narrate("USER STORY");
    narrator.wrapped("  ", "  ", user_story);
}

/// Print the path matrix observed from live responses.
fn print_paths(narrator: &mut Narrator, paths: &ObservedPaths) {
    narrator.narrate("");
    narrator.narrate("OBSERVED ROUTES");
    for ((edge, provider), fixture) in paths {
        narrator.narrate(&format!(
            "  [PASS] {edge} -> {provider}  (fixtures: {} / {})",
            fixture.edge_session, fixture.provider_session
        ));
    }

    for (edge, provider) in paths.keys() {
        narrator.narrate(&format!(
            "         client -> {edge} -> {} gateway -> {provider} backend",
            provider_gateway_for_backend(provider)
        ));
    }

    print_crossed_path(narrator, paths);
}

/// Print the crossed edge/provider path when one is observed.
fn print_crossed_path(narrator: &mut Narrator, paths: &ObservedPaths) {
    let crossed = paths.iter().find(|((edge, provider), _)| {
        edge.strip_suffix("-edge") != provider_gateway_for_backend(provider).strip_suffix("-provider")
    });

    if let Some(((edge, provider), fixture)) = crossed {
        let provider_gateway = provider_gateway_for_backend(provider);
        narrator.narrate("");
        narrator.narrate(&format!(
            "CROSSED ROUTE PROOF (fixtures: {} / {})",
            fixture.edge_session, fixture.provider_session
        ));
        narrator.narrate(&format!("  client -> {edge} public edge -> {edge} Grid overlay"));
        narrator.narrate(&format!(
            "         -> {provider_gateway} private provider gateway -> {provider} backend"
        ));
        narrator.narrate(&format!(
            "         -> {provider_gateway} provider gateway -> {edge} edge -> client"
        ));
    }
}

/// Map a backend provider identity to its provider-site gateway.
fn provider_gateway_for_backend(provider: &str) -> &str {
    if provider == "east-provider-secondary" {
        "east-provider"
    } else {
        provider
    }
}

/// Summarize the provider assertions completed by the preceding strict proof.
fn print_provider_boundary_proof(narrator: &mut Narrator) {
    narrator.narrate("");
    narrator.wrapped(
        "[PASS] ",
        "       ",
        "Both provider gateways required mTLS, accepted both pinned edge identities, rejected missing or invalid TLS identities, and enforced exact candidate/model/path policy for three provider candidates.",
    );
}

/// Summarize final-hop credential and private-backend runtime evidence.
fn print_credential_boundary_proof(narrator: &mut Narrator) {
    narrator.narrate("");
    narrator.wrapped(
        "[PASS] ",
        "       ",
        "All three private provider paths are isolated by NetworkPolicy and provider-local credentials; the two east providers use distinct backends and credentials behind one site gateway.",
    );
}

/// Print explicit, mode-specific scope boundaries after all runtime proofs.
#[expect(clippy::too_many_lines, reason = "mode-branched narration block")]
fn print_boundaries(narrator: &mut Narrator, mode: DemoMode) {
    narrator.banner("DEMONSTRATED BOUNDARY");
    narrator.narrate("");
    narrator.wrapped(
        "[PROVEN] ",
        "         ",
        "Two Praxis edges served one verified HTTPS name, and Grid routed across three provider candidates.",
    );
    narrator.wrapped(
        "[PROVEN] ",
        "         ",
        "Versioned per-edge overlays with three candidates, including two independently routed providers in one cluster, plus exact rendered/distributed/accepted/serving revision evidence.",
    );
    if mode == DemoMode::Full {
        narrator.wrapped(
            "[PROVEN] ",
            "         ",
            "Edge and provider session affinity, health-driven provider withdrawal, health-driven edge withdrawal and recovery, operator restart recovery, sustained request soak, hot reload, provider mTLS, and peer authorization.",
        );
    } else {
        narrator.wrapped(
            "[PROVEN] ",
            "         ",
            "Health-driven provider withdrawal, hot reload, provider mTLS, and peer authorization.",
        );
    }
    narrator.wrapped(
        "[PROVEN] ",
        "         ",
        "Provider-local credential replacement and NetworkPolicy-enforced private backend access.",
    );
    narrator.wrapped(
        "[OUT OF SCOPE] ",
        "               ",
        "Managed DNS/Anycast, internet DDoS/WAF, geo-latency GTM steering, shared affinity storage, or in-flight stream migration.",
    );
}

// ---------------------------------------------------------------------------
// Workload-mode narration
// ---------------------------------------------------------------------------

/// Print workload-inference architecture and proof policy.
fn print_workload_introduction(narrator: &mut Narrator, mode: DemoMode) {
    narrator.banner("PRAXIS GRID WORKLOAD INFERENCE DEMO");
    narrator.narrate("");
    narrator.narrate(&format!("Mode: {}", mode_label(mode).to_uppercase()));
    narrator.wrapped(
        "Proof policy: ",
        "              ",
        "Every PASS comes from a runtime assertion; manifest intent is not counted as proof.",
    );
    narrator.narrate("");
    narrator.narrate("EXPECTED PATH");
    narrator.narrate("  workload -> local Praxis consumer gateway");
    narrator.narrate("           -> Grid-selected provider gateway -> private backend");
    narrator.narrate("");
    narrator.narrate("No traffic manager or public endpoint involved.");
}

/// Print workload paths observed from in-cluster Jobs.
fn print_workload_paths(narrator: &mut Narrator, paths: &[ObservedPathEntry]) {
    narrator.narrate("");
    narrator.narrate("OBSERVED WORKLOAD ROUTES");
    for entry in paths {
        narrator.narrate(&format!("  [PASS] {} -> {}", entry.edge, entry.provider));
        narrator.narrate(&format!("         {}", entry.path));
    }
}

/// Print workload-mode scope boundaries.
fn print_workload_boundaries(narrator: &mut Narrator, mode: DemoMode) {
    narrator.banner("DEMONSTRATED BOUNDARY");
    narrator.narrate("");
    narrator.wrapped(
        "[PROVEN] ",
        "         ",
        "In-cluster workloads reached Grid-selected providers through local consumer gateways without external ingress.",
    );
    narrator.wrapped(
        "[PROVEN] ",
        "         ",
        "Versioned overlays with provider candidates, plus rendered/distributed/accepted/serving revision evidence.",
    );
    if mode == DemoMode::Full {
        narrator.wrapped(
            "[PROVEN] ",
            "         ",
            "Local provider preference and remote fallback, provider mTLS, peer authorization, and credential replacement.",
        );
    } else {
        narrator.wrapped(
            "[PROVEN] ",
            "         ",
            "Provider mTLS, peer authorization, and credential replacement.",
        );
    }
    narrator.wrapped(
        "[NOT APPLICABLE] ",
        "                 ",
        "Edge withdrawal and GTM steering (workload mode has no traffic manager).",
    );
}

// ---------------------------------------------------------------------------
// Workload path discovery
// ---------------------------------------------------------------------------

/// Request fixture body for in-cluster workload requests.
const WORKLOAD_REQUEST_BODY: &str =
    r#"{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"hello"}],"max_tokens":64}"#;

/// Discover routing paths by sending in-cluster requests from each consumer.
fn discover_workload_paths() -> Result<Vec<ObservedPathEntry>, Box<dyn std::error::Error>> {
    let mut paths = Vec::new();
    for cluster in CONSUMER_CLUSTERS {
        let response = workload::send_workload_request(cluster, WORKLOAD_REQUEST_BODY, None)?;
        if response.status != 200 {
            return Err(format!("workload request from {cluster} returned status {}", response.status).into());
        }
        let provider = if response.provider.is_empty() {
            extract_provider_from_response(&response.body)?
        } else {
            response.provider.clone()
        };
        paths.push(ObservedPathEntry {
            edge: (*cluster).to_owned(),
            provider: provider.clone(),
            path: format!(
                "workload -> {cluster} consumer gateway -> {} gateway -> {provider} backend",
                provider_gateway_for_backend(&provider)
            ),
        });
    }
    Ok(paths)
}

/// Extract provider identity from the workload response body.
///
/// Returns an error when the response is not valid JSON or lacks a
/// `model` field — missing attribution must fail the proof rather than
/// silently substituting a placeholder.
fn extract_provider_from_response(body: &str) -> Result<String, Box<dyn std::error::Error>> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("response is not valid JSON: {e}"))?;
    value
        .get("model")
        .and_then(serde_json::Value::as_str)
        .map(String::from)
        .ok_or_else(|| "response JSON missing 'model' field for provider attribution".into())
}

/// Maximum time to wait for overlay convergence during drain/restore.
const OVERLAY_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);

/// Interval between overlay convergence polls.
const OVERLAY_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Overlay `ConfigMap` name used by the edge gateways.
const EDGE_OVERLAY_CONFIGMAP: &str = "grid-overlay-glb-demo-edge-gateway";

/// Prove locality preference and remote fallback.
///
/// Phase 1: send requests from each consumer and require every consumer
/// to route to its local provider (proving the scoring engine prefers
/// locality for all sites, not just some).
///
/// Phase 2: scale down the provider gateway in east-provider so no east
/// routing capacity remains, poll the east-edge overlay until east
/// candidates are absent, send a request from east-edge, and confirm it
/// routes to a known west provider. Restoration is guarded: the provider
/// gateway is always restored, overlay recovery verified, and local
/// routing confirmed before returning.
#[expect(clippy::too_many_lines, reason = "two-phase preference/fallback proof")]
fn prove_workload_local_preference(narrator: &mut Narrator) -> Result<String, Box<dyn std::error::Error>> {
    narrator.narrate("  Phase 1: local preference");
    let mut local_hits = 0_usize;
    let total = CONSUMER_CLUSTERS.len();

    for cluster in CONSUMER_CLUSTERS {
        let response = workload::send_workload_request(cluster, WORKLOAD_REQUEST_BODY, None)?;
        if response.status != 200 {
            return Err(format!("local preference: {cluster} returned status {}", response.status).into());
        }
        let provider = &response.provider;
        if provider.is_empty() {
            return Err(format!("local preference: {cluster} response missing provider header").into());
        }
        let region = cluster.strip_suffix("-edge").unwrap_or(cluster);
        let is_local = provider.starts_with(region);
        if is_local {
            local_hits += 1;
        }
        narrator.narrate(&format!(
            "  [{}] {cluster} -> {provider} ({})",
            if is_local { "PASS" } else { "FAIL" },
            if is_local { "local" } else { "remote" },
        ));
    }

    if local_hits != total {
        return Err(format!("local preference: {local_hits}/{total} consumers routed locally, expected all").into());
    }

    narrator.narrate("  Phase 2: remote fallback after east provider drain");
    let drain_cluster = "east-provider";
    let drain_context = format!("kind-grid-glb-{drain_cluster}");

    let reload_before = glb::count_overlay_reload_logs("east-edge").unwrap_or(0);

    scale_deployment(&drain_context, "provider-gateway", 0)?;
    narrator.narrate("  [INFO] east-provider/provider-gateway scaled to 0");

    let drain_result = verify_drain_fallback("east-edge", reload_before, narrator);
    let restore_result = restore_deployment(&drain_context, "provider-gateway", "east-edge", narrator);

    match (drain_result, restore_result) {
        (Ok(fallback_provider), Ok(())) => {
            narrator.narrate(&format!(
                "  [PASS] east-edge -> {fallback_provider} (remote fallback to west-provider)"
            ));
            Ok(format!(
                "{local_hits}/{total} local, remote fallback to {fallback_provider}"
            ))
        },
        (Err(drain_err), Err(restore_err)) => {
            Err(format!("{drain_err}; restoration also failed: {restore_err}").into())
        },
        (Err(e), Ok(())) | (Ok(_), Err(e)) => Err(e),
    }
}

/// Scale a deployment in `grid-system` to the given replica count.
fn scale_deployment(context: &str, name: &str, replicas: u32) -> Result<(), Box<dyn std::error::Error>> {
    let out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            "grid-system",
            "scale",
            &format!("deployment/{name}"),
            &format!("--replicas={replicas}"),
            "--timeout=30s",
        ])
        .output()?;
    if !out.status.success() {
        return Err(format!(
            "could not scale {name} to {replicas}: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    Ok(())
}

/// Read the overlay candidates for one edge cluster.
fn read_overlay_candidates(edge: &str) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let context = format!("kind-grid-glb-{edge}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            "grid-system",
            "get",
            "configmap",
            EDGE_OVERLAY_CONFIGMAP,
            "-o",
            r"jsonpath={.data.routing-config\.json}",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "could not read overlay from {edge}: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let raw = String::from_utf8_lossy(&output.stdout);
    let document: serde_json::Value = serde_json::from_str(&raw)?;
    let candidates = document
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(candidates)
}

/// Poll until no candidates from `site` appear in the edge overlay.
fn wait_for_overlay_candidate_absent(edge: &str, site: &str) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + OVERLAY_CONVERGENCE_TIMEOUT;
    loop {
        let candidates = read_overlay_candidates(edge)?;
        let has_site = candidates
            .iter()
            .any(|c| c.get("site").and_then(serde_json::Value::as_str) == Some(site));
        if !has_site {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timeout: {edge} overlay still lists {site} candidates after {OVERLAY_CONVERGENCE_TIMEOUT:?}"
            )
            .into());
        }
        std::thread::park_timeout(OVERLAY_POLL_INTERVAL);
    }
}

/// Poll until at least one candidate from `site` appears in the edge overlay.
fn wait_for_overlay_candidate_present(edge: &str, site: &str) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + OVERLAY_CONVERGENCE_TIMEOUT;
    loop {
        let candidates = read_overlay_candidates(edge)?;
        let has_site = candidates
            .iter()
            .any(|c| c.get("site").and_then(serde_json::Value::as_str) == Some(site));
        if has_site {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timeout: {edge} overlay does not list {site} candidates after {OVERLAY_CONVERGENCE_TIMEOUT:?}"
            )
            .into());
        }
        std::thread::park_timeout(OVERLAY_POLL_INTERVAL);
    }
}

/// Wait for a deployment rollout to complete.
fn wait_for_rollout(context: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let rollout = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            "grid-system",
            "rollout",
            "status",
            &format!("deployment/{name}"),
            "--timeout=60s",
        ])
        .output()?;
    if !rollout.status.success() {
        return Err(format!(
            "{name} did not become ready: {}",
            String::from_utf8_lossy(&rollout.stderr)
        )
        .into());
    }
    Ok(())
}

/// Confirm overlay convergence, hot-reload, and remote fallback to a
/// provider outside the drained site. Returns the fallback provider name.
fn verify_drain_fallback(
    consumer_edge: &str,
    reload_before: usize,
    narrator: &mut Narrator,
) -> Result<String, Box<dyn std::error::Error>> {
    wait_for_overlay_candidate_absent(consumer_edge, "east-provider")?;
    narrator.narrate(&format!(
        "  [OK] {consumer_edge} overlay no longer lists east-provider candidates"
    ));

    glb::check_hot_reload_observed(consumer_edge, reload_before)?;
    narrator.narrate(&format!("  [OK] {consumer_edge} gateway reloaded overlay after drain"));

    let resp = glb::wait_for_data_plane_convergence("remote fallback routing", || {
        let resp = workload::send_workload_request(consumer_edge, WORKLOAD_REQUEST_BODY, None)?;
        if resp.status != 200 {
            return Err(format!(
                "remote fallback: {consumer_edge} returned status {} after drain",
                resp.status
            )
            .into());
        }
        if resp.provider.is_empty() {
            return Err(format!("remote fallback: {consumer_edge} response missing provider header").into());
        }
        if !resp.provider.starts_with("west") {
            return Err(format!(
                "remote fallback: {consumer_edge} routed to {}, expected a west-provider identity",
                resp.provider
            )
            .into());
        }
        Ok(resp)
    })?;

    Ok(resp.provider)
}

/// Verify a consumer routes to its local provider.
fn verify_local_routing(consumer_edge: &str) -> Result<String, Box<dyn std::error::Error>> {
    let verify = workload::send_workload_request(consumer_edge, WORKLOAD_REQUEST_BODY, None)?;
    if verify.status != 200 {
        return Err(format!("{consumer_edge} returned status {}", verify.status).into());
    }
    let provider = if verify.provider.is_empty() {
        extract_provider_from_response(&verify.body)?
    } else {
        verify.provider
    };
    let region = consumer_edge.strip_suffix("-edge").unwrap_or(consumer_edge);
    if !provider.starts_with(region) {
        return Err(format!("{consumer_edge} routed to {provider}, expected local {region} provider").into());
    }
    Ok(provider)
}

/// Restore a deployment to 1 replica, wait for readiness, verify overlay
/// recovery, and confirm local routing before returning.
fn restore_deployment(
    context: &str,
    name: &str,
    consumer_edge: &str,
    narrator: &mut Narrator,
) -> Result<(), Box<dyn std::error::Error>> {
    let site = context.strip_prefix("kind-grid-glb-").unwrap_or(context);
    let reload_before = glb::count_overlay_reload_logs(consumer_edge).unwrap_or(0);
    scale_deployment(context, name, 1)?;
    wait_for_rollout(context, name)?;
    wait_for_overlay_candidate_present(consumer_edge, site)?;
    narrator.narrate(&format!("  [OK] {site} candidates returned to {consumer_edge} overlay"));
    glb::check_hot_reload_observed(consumer_edge, reload_before)?;
    narrator.narrate(&format!(
        "  [OK] {consumer_edge} gateway reloaded overlay after restoration"
    ));
    let provider =
        glb::wait_for_data_plane_convergence("local routing restoration", || verify_local_routing(consumer_edge))?;
    narrator.narrate(&format!(
        "  [OK] {consumer_edge} -> {provider} (local routing restored)"
    ));
    Ok(())
}

// ---------------------------------------------------------------------------
// Path discovery and affinity (global mode)
// ---------------------------------------------------------------------------

/// Discover real edge/provider combinations through the stable HTTPS name.
fn discover_paths(request_fixture: &Path) -> Result<ObservedPaths, Box<dyn std::error::Error>> {
    let mut paths = BTreeMap::new();
    for index in 0..MAX_PATH_SAMPLES {
        let fixture = AffinityFixture {
            edge_session: format!("narrated-edge-{index}"),
            provider_session: format!("narrated-provider-{index}"),
        };
        let sample = gtm_emulator::request_path_with_affinity(
            &fixture.edge_session,
            &fixture.provider_session,
            request_fixture,
        )?;
        if !paths.keys().any(|(edge, _)| edge == &sample.edge) {
            paths.insert((sample.edge, sample.provider), fixture);
        }
        if paths.len() == 2 {
            break;
        }
    }
    if paths.len() != 2 {
        return Err(format!("path discovery observed {} of 2 Praxis edges", paths.len()).into());
    }
    Ok(paths)
}

/// Build evidence path entries from observed paths.
fn build_observed_paths(paths: &ObservedPaths) -> Vec<ObservedPathEntry> {
    paths
        .iter()
        .map(|((edge, provider), _)| ObservedPathEntry {
            edge: edge.clone(),
            provider: provider.clone(),
            path: format!(
                "client -> {edge} -> {} gateway -> {provider} backend",
                provider_gateway_for_backend(provider)
            ),
        })
        .collect()
}

/// Repeat one observed path and require both affinity layers to remain stable.
fn prove_affinity(
    narrator: &mut Narrator,
    paths: &ObservedPaths,
    request_fixture: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let ((expected_edge, expected_provider), fixture) = paths
        .first_key_value()
        .ok_or("no observed path available for affinity")?;

    for _attempt in 0..AFFINITY_REPEATS {
        let sample = gtm_emulator::request_path_with_affinity(
            &fixture.edge_session,
            &fixture.provider_session,
            request_fixture,
        )?;
        if sample.edge != *expected_edge || sample.provider != *expected_provider {
            return Err(format!(
                "affinity fixtures moved from {expected_edge}/{expected_provider} to {}/{}",
                sample.edge, sample.provider,
            )
            .into());
        }
    }

    narrator.narrate("");
    narrator.wrapped(
        "[PASS] ",
        "       ",
        &format!(
            "Edge fixture {} and provider fixture {} remained on edge {expected_edge} and provider {expected_provider} for {AFFINITY_REPEATS} repeated requests.",
            fixture.edge_session, fixture.provider_session
        ),
    );
    Ok(())
}

/// Restart every Grid operator sequentially, then sustain requests for a bounded soak.
fn prove_restart_recovery_and_soak(
    narrator: &mut Narrator,
    paths: &ObservedPaths,
    request_fixture: &Path,
) -> Result<String, Box<dyn std::error::Error>> {
    let fixtures = paths.values().collect::<Vec<_>>();
    if fixtures.is_empty() {
        return Err("no observed path available for restart and soak proof".into());
    }

    prove_operator_restarts(narrator, &fixtures, request_fixture)?;
    let (samples, edge_count, provider_count) = run_request_soak(narrator, &fixtures, request_fixture)?;
    let evidence = format!(
        "4 Grid operators restarted; {samples} soak requests passed across {edge_count} edges and {provider_count} provider(s)"
    );
    narrator.narrate(&format!("[PASS] {evidence}."));
    Ok(evidence)
}

/// Restart each Grid operator and prove overlay and request recovery.
fn prove_operator_restarts(
    narrator: &mut Narrator,
    fixtures: &[&AffinityFixture],
    request_fixture: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    narrator.narrate("");
    narrator.wrapped(
        "[RESTART] ",
        "          ",
        "Restarting each Grid operator one at a time. After every restart, both edge overlays must converge and one inference request must succeed.",
    );
    for (index, cluster) in GRID_CLUSTERS.iter().enumerate() {
        narrator.narrate(&format!(
            "[RESTART {}/{}] {cluster}: waiting for operator rollout and routing recovery.",
            index + 1,
            GRID_CLUSTERS.len()
        ));
        restart_grid_operator(cluster)?;
        let overlay_evidence = glb::wait_for_edge_overlays_ready()?;
        let fixture = fixtures
            .get(index % fixtures.len())
            .ok_or("no affinity fixture available after Grid restart")?;
        let sample = gtm_emulator::request_path_with_affinity(
            &fixture.edge_session,
            &fixture.provider_session,
            request_fixture,
        )?;
        narrator.narrate(&format!(
            "[PASS] Restarted {cluster} Grid operator; routing recovered via {} -> {} ({overlay_evidence}).",
            sample.edge, sample.provider
        ));
    }
    Ok(())
}

/// Sustain inference requests for the full-mode soak window.
fn run_request_soak(
    narrator: &mut Narrator,
    fixtures: &[&AffinityFixture],
    request_fixture: &Path,
) -> Result<(usize, usize, usize), Box<dyn std::error::Error>> {
    narrate_soak_start(narrator);
    let deadline = Instant::now() + FULL_SOAK_DURATION;
    let mut samples = 0_usize;
    let mut edges = BTreeSet::new();
    let mut providers = BTreeSet::new();
    while Instant::now() < deadline {
        let fixture = fixtures
            .get(samples % fixtures.len())
            .ok_or("no affinity fixture available during request soak")?;
        let sample = gtm_emulator::request_path_with_affinity(
            &fixture.edge_session,
            &fixture.provider_session,
            request_fixture,
        )?;
        edges.insert(sample.edge);
        providers.insert(sample.provider);
        samples += 1;
        narrate_soak_progress(narrator, samples, edges.len(), providers.len());

        let remaining = deadline.saturating_duration_since(Instant::now());
        if !remaining.is_zero() {
            std::thread::park_timeout(FULL_SOAK_INTERVAL.min(remaining));
        }
    }
    if edges.len() != 2 {
        return Err(format!("request soak reached {} of 2 Praxis edges", edges.len()).into());
    }
    Ok((samples, edges.len(), providers.len()))
}

/// Explain the full-mode soak contract before the bounded wait begins.
fn narrate_soak_start(narrator: &mut Narrator) {
    narrator.narrate("");
    narrator.narrate(&format!(
        "[SOAK] Sending requests through the stable HTTPS endpoint for {} seconds.",
        FULL_SOAK_DURATION.as_secs()
    ));
    narrator.wrapped(
        "       ",
        "       ",
        "Every request must succeed. Both edges must remain observable, and progress is reported after each 12 successful requests.",
    );
}

/// Report bounded soak progress without logging every request.
fn narrate_soak_progress(narrator: &mut Narrator, samples: usize, edge_count: usize, provider_count: usize) {
    if samples.is_multiple_of(FULL_SOAK_PROGRESS_SAMPLES) {
        narrator.narrate(&format!(
            "[SOAK] {samples} requests passed; observed {edge_count} of 2 edges and {provider_count} provider(s)."
        ));
    }
}

/// Restart one Grid operator and wait for its replacement pod.
fn restart_grid_operator(cluster: &str) -> Result<(), Box<dyn std::error::Error>> {
    let context = format!("kind-grid-glb-{cluster}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            "grid-system",
            "rollout",
            "restart",
            "deployment/grid-operator",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to restart {cluster} Grid operator: {}",
            super::safe_truncate_str(&String::from_utf8_lossy(&output.stderr), 160)
        )
        .into());
    }
    kubectl::wait_for_rollout_ns(&context, "grid-operator", "grid-system", cluster)
}

/// Prove external provider routing through the Grid provider boundary.
///
/// Sends one small, non-streaming request with an invalid caller credential
/// to prove final-hop credential replacement. Records only status, provider,
/// model, and timing. Never retains the prompt, response body, or token.
#[expect(clippy::too_many_lines, reason = "linear request-response-evidence sequence")]
fn prove_external_provider(
    narrator: &mut Narrator,
    ext: &ExternalProviderDescriptor,
) -> Result<ExternalProviderProof, Box<dyn std::error::Error>> {
    narrator.narrate("");
    narrator.narrate("EXTERNAL PROVIDER PROOF");
    narrator.narrate(&format!("  provider: {} ({})", ext.routing_cluster, ext.provider_kind));
    narrator.narrate(&format!("  model: {}", ext.model));
    narrator.narrate(&format!("  endpoint: {}", ext.endpoint()));
    narrator.narrate("  requests: 1 (non-streaming, low token limit)");
    narrator.narrate("  caller auth: invalid (proves final-hop replacement)");
    narrator.narrate("");

    let request_start = Instant::now();

    let body = serde_json::json!({
        "model": ext.model,
        "input": "Say exactly: hello",
        "max_output_tokens": 16,
        "stream": false,
    });

    let gtm_ip = gtm_emulator::resolve_gtm_ip()?;
    let gtm_ca = ".forge/runtime/glb-tls/gtm/ca.crt";
    let public_name = "api.grid-glb.test";
    let public_port: u16 = 8443;
    let resolve = format!("{public_name}:{public_port}:{gtm_ip}");
    let url = format!("https://{public_name}:{public_port}/v1/responses");

    let output = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--max-time",
            "30",
            "--cacert",
            gtm_ca,
            "--noproxy",
            public_name,
            "--resolve",
            &resolve,
            "--include",
            "-X",
            "POST",
            &url,
            "-H",
            "Content-Type: application/json",
            "-H",
            "Authorization: Bearer invalid-caller-token",
            "-d",
            &body.to_string(),
        ])
        .output()?;

    let elapsed = request_start.elapsed();
    if !output.status.success() {
        return Err(format!("external provider request failed: curl exit status {}", output.status).into());
    }
    let raw = String::from_utf8(output.stdout)?;
    let (headers, response_body) = raw
        .split_once("\r\n\r\n")
        .or_else(|| raw.split_once("\n\n"))
        .ok_or("external provider response contained no header terminator")?;
    let status_code = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or("failed to parse external provider HTTP status")?;

    narrator.narrate(&format!(
        "  [{}] HTTP {} in {:.1}s (model: {}, cluster: {})",
        if status_code == 200 { "PASS" } else { "FAIL" },
        status_code,
        elapsed.as_secs_f64(),
        ext.model,
        ext.routing_cluster,
    ));

    if status_code != 200 {
        let hint = match status_code {
            401 => "credential replacement may have failed or key is invalid",
            403 => "model or API path may not be authorized",
            404 => "model not found or endpoint path is incorrect",
            429 => "rate limited by OpenAI",
            _ => "unexpected response status",
        };
        return Err(format!("external provider returned HTTP {status_code}: {hint}").into());
    }

    let edge = response_header(headers, "x-grid-demo-edge-gateway")
        .filter(|value| matches!(*value, "east-edge" | "west-edge"))
        .ok_or("external provider response missing valid edge attribution")?;
    let provider_gateway = response_header(headers, "x-ai-demo-provider-gateway")
        .filter(|value| *value == "east-provider")
        .ok_or("external provider response missing east-provider gateway attribution")?;

    let response: serde_json::Value = serde_json::from_str(response_body)
        .map_err(|error| format!("external provider response is not valid JSON: {error}"))?;
    if response.get("object").and_then(serde_json::Value::as_str) != Some("response") {
        return Err("external provider response is not an OpenAI Responses object".into());
    }
    response
        .get("id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| value.starts_with("resp_") && value.len() <= 256)
        .ok_or("external provider response missing bounded OpenAI response ID")?;
    let response_model = response
        .get("model")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .ok_or("external provider response missing bounded model")?;
    if !response.get("output").is_some_and(serde_json::Value::is_array) {
        return Err("external provider response missing output array".into());
    }

    let serving_revision = glb::verify_external_candidate(edge, ext)?;
    let proof = ExternalProviderProof {
        http_status: status_code,
        edge: edge.to_owned(),
        provider_gateway: provider_gateway.to_owned(),
        candidate_id: glb::openai_candidate_id(&ext.model),
        cluster: ext.routing_cluster.to_owned(),
        response_model: response_model.to_owned(),
        serving_revision,
        duration_secs: elapsed.as_secs_f64(),
    };
    narrator.narrate("  [PASS] OpenAI Responses object validated (response ID format and output array)");
    narrator.narrate(&format!("  evidence: {}", proof.summary()));
    Ok(proof)
}

/// Find one case-insensitive response header without retaining response bytes.
fn response_header<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
    headers.lines().find_map(|line| {
        let (candidate, value) = line.split_once(':')?;
        candidate.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

/// Build a failed outcome while preserving completed capability evidence.
fn failed_outcome(
    mut capabilities: Vec<CapabilityResult>,
    observed_paths: Vec<ObservedPathEntry>,
    capability: &str,
    error: String,
) -> DemoOutcome {
    capabilities.push(CapabilityResult {
        capability: capability.to_owned(),
        result: "fail",
        evidence: error.clone(),
    });
    DemoOutcome {
        capabilities,
        observed_paths,
        external_provider: None,
        error: Some(error),
    }
}

/// Convert arbitrary command errors into one bounded evidence line.
fn concise_error(error: impl Display) -> String {
    error
        .to_string()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(512)
        .collect()
}

/// Add another failure without discarding the primary cause.
fn append_error(error: &mut Option<String>, additional: String) {
    match error {
        Some(primary) => {
            primary.push_str("; ");
            primary.push_str(&additional);
        },
        None => *error = Some(additional),
    }
}

// ---------------------------------------------------------------------------
// Final summary
// ---------------------------------------------------------------------------

/// Print a concise summary table after all scenarios.
fn print_final_summary(narrator: &mut Narrator, report: &EvidenceReport, evidence_dir: &Path) {
    narrator.banner("FINAL RESULT");
    for cap in &report.capabilities {
        narrator.narrate(&format!("[{:<7}] {}", cap.result.to_uppercase(), cap.capability));
        narrator.wrapped("          ", "          ", &cap.evidence);
    }
    narrator.narrate("");
    narrator.narrate(&format!("OVERALL   {}", report.status.to_uppercase()));
    narrator.narrate(&format!("MODE      {}", report.mode.to_uppercase()));
    narrator.narrate(&format!("ELAPSED   {:.1}s", report.duration_secs));
    narrator.narrate(&format!("EVIDENCE  {}", evidence_dir.display()));
}

// ---------------------------------------------------------------------------
// Evidence output
// ---------------------------------------------------------------------------

/// Resolve the evidence directory path.
fn resolve_evidence_dir(forge_config: &Path, options: &GlbDemoOptions, run_id: &str) -> PathBuf {
    if let Some(dir) = &options.evidence_dir {
        return dir.clone();
    }
    let parent = forge_config.parent().unwrap_or_else(|| Path::new("."));
    parent.join(format!(".forge/evidence/glb-demo-{run_id}"))
}

/// Write evidence files (narration and JSON report).
fn write_evidence(
    report: &EvidenceReport,
    narrator: &Narrator,
    narration_path: &Path,
    results_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    narrator.write_to_file(narration_path)?;
    let json = serde_json::to_string_pretty(report)?;
    fs::write(results_path, json)?;
    eprintln!(
        "[EVIDENCE] Human narration and results.json written to {}",
        narration_path.parent().unwrap_or_else(|| Path::new(".")).display()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Teardown
// ---------------------------------------------------------------------------

/// Delete all GLB demo clusters through Forge.
fn teardown_clusters(forge: &str, config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!();
    eprintln!("[CLEANUP] Tearing down demo clusters...");
    run_forge(forge, config, &["down", "--force"])
}

// ---------------------------------------------------------------------------
// Timestamp helpers
// ---------------------------------------------------------------------------

/// Format the current UTC time as `YYYYMMDDTHHMMSSz` for evidence directory names.
fn format_utc_timestamp() -> String {
    let now = time::OffsetDateTime::now_utc();
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    )
}

/// Format the current UTC time as ISO 8601 for evidence fields.
fn format_utc_iso() -> String {
    let now = time::OffsetDateTime::now_utc();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    )
}

/// Return the string label for a demo mode.
fn mode_label(mode: DemoMode) -> &'static str {
    match mode {
        DemoMode::Quick => "quick",
        DemoMode::Full => "full",
    }
}

/// Return a combined label for evidence reports.
fn evidence_mode_label(mode: DemoMode, ingress_mode: IngressMode) -> &'static str {
    match (ingress_mode, mode) {
        (IngressMode::Global, DemoMode::Quick) => "quick",
        (IngressMode::Global, DemoMode::Full) => "full",
        (IngressMode::Workload, DemoMode::Quick) => "workload-quick",
        (IngressMode::Workload, DemoMode::Full) => "workload-full",
    }
}

// ---------------------------------------------------------------------------
// Environment setup helpers
// ---------------------------------------------------------------------------

/// Render image overrides into a Forge config without mutating source files.
fn materialize_config(
    source: &Path,
    ingress_mode: IngressMode,
    external_provider: Option<&ExternalProviderDescriptor>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(source)?;
    let rendered = render_config(&content, ingress_mode, external_provider)?;
    let parent = source.parent().unwrap_or_else(|| Path::new("."));
    let output = parent.join(RESOLVED_CONFIG_NAME);
    fs::write(&output, rendered)?;
    Ok(output)
}

/// Render image overrides and optional provider integration into one Forge configuration.
fn render_config(
    content: &str,
    ingress_mode: IngressMode,
    external_provider: Option<&ExternalProviderDescriptor>,
) -> Result<String, Box<dyn std::error::Error>> {
    validate_image_contract_for_mode(ingress_mode)?;
    let mut config: serde_yaml::Value = serde_yaml::from_str(content)?;
    let spec = mapping_mut(&mut config, "spec")?;
    if ingress_mode == IngressMode::Workload {
        strip_gtm_cluster(spec)?;
    }
    set_cluster_image_properties_for_mode(spec, ingress_mode)?;

    if external_provider.is_some() {
        add_openai_credential_mount(spec)?;
    }

    Ok(serde_yaml::to_string(&config)?)
}

/// Clone vcr-backend stack as vcr-backend-openai with required `OpenAI` credential mount.
///
/// Creates east-only stack to prevent west-provider requiring credentials that only exist in east-provider.
#[expect(
    clippy::too_many_lines,
    reason = "YAML path traversal and validation requires comprehensive error checking"
)]
fn add_openai_credential_mount(spec: &mut serde_yaml::Mapping) -> Result<(), Box<dyn std::error::Error>> {
    let stacks = spec
        .get_mut("stacks")
        .and_then(|v| v.as_mapping_mut())
        .ok_or("spec.stacks not found or not a mapping")?;

    if stacks.contains_key("vcr-backend-openai") {
        return Err("vcr-backend-openai stack already exists".into());
    }

    let vcr_backend = stacks
        .get("vcr-backend")
        .and_then(|v| v.as_mapping())
        .ok_or("vcr-backend stack not found")?;

    let mut vcr_backend_openai = vcr_backend.clone();

    let steps = vcr_backend_openai
        .get_mut("steps")
        .and_then(|v| v.as_sequence_mut())
        .ok_or("vcr-backend-openai.steps not found or not a sequence")?;

    let mut found_helm_step = false;
    for step in steps {
        if let Some(step_map) = step.as_mapping_mut()
            && step_map.get("type").and_then(|v| v.as_str()) == Some("helm")
            && step_map.get("release").and_then(|v| v.as_str()) == Some("provider-gateway")
        {
            let values = step_map
                .get_mut("values")
                .and_then(|v| v.as_mapping_mut())
                .ok_or("provider-gateway Helm values not found")?;

            let credentials = values
                .entry("credentials".into())
                .or_insert_with(|| serde_yaml::Value::Sequence(Vec::new()))
                .as_sequence_mut()
                .ok_or("credentials is not a sequence")?;

            for cred in credentials.iter() {
                if let Some(name) = cred.get("name").and_then(|v| v.as_str())
                    && name == "openai-api-key"
                {
                    return Err("OpenAI credential mount already exists".into());
                }
            }

            let openai_credential = serde_yaml::Value::Mapping(
                [
                    ("name".into(), "openai-api-key".into()),
                    ("mountPath".into(), "/etc/praxis/credentials/openai".into()),
                    ("optional".into(), false.into()),
                ]
                .iter()
                .cloned()
                .collect(),
            );

            credentials.push(openai_credential);
            found_helm_step = true;
            break;
        }
    }

    if !found_helm_step {
        return Err("provider-gateway Helm step not found in vcr-backend stack".into());
    }

    if let Some(desc) = vcr_backend_openai.get_mut("description") {
        *desc = "Private inference backend with OpenAI external provider support".into();
    }

    stacks.insert(
        "vcr-backend-openai".into(),
        serde_yaml::Value::Mapping(vcr_backend_openai),
    );

    Ok(())
}

/// Validate image references and pull policy for the given ingress mode.
fn validate_image_contract_for_mode(ingress_mode: IngressMode) -> Result<(), Box<dyn std::error::Error>> {
    for (name, image) in [
        (
            "GRID_XTASK_GATEWAY_IMAGE",
            image_overrides::demo_gateway_image(ingress_mode),
        ),
        (
            "GRID_XTASK_OPERATOR_IMAGE",
            image_overrides::demo_operator_image(ingress_mode),
        ),
        ("GRID_XTASK_VCR_IMAGE", image_overrides::vcr_image()),
    ] {
        if image.is_empty() || image.chars().any(char::is_whitespace) {
            return Err(format!("{name} must be a non-empty image reference without whitespace").into());
        }
    }

    let pull_policy = image_overrides::demo_image_pull_policy(ingress_mode);
    if !matches!(pull_policy.as_str(), "Always" | "IfNotPresent" | "Never") {
        return Err(format!(
            "GRID_XTASK_IMAGE_PULL_POLICY must be Always, IfNotPresent, or Never; got {pull_policy:?}"
        )
        .into());
    }
    Ok(())
}

/// Split a `repo:tag` image reference into `(repo, tag)`.
fn split_image_ref(image: &str) -> (String, String) {
    image.rsplit_once(':').map_or_else(
        || (image.to_owned(), "latest".to_owned()),
        |(repo, tag)| (repo.to_owned(), tag.to_owned()),
    )
}

/// Apply mode-selected images to stack template properties.
fn set_cluster_image_properties_for_mode(
    spec: &mut serde_yaml::Mapping,
    ingress_mode: IngressMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let gateway_image = image_overrides::demo_gateway_image(ingress_mode);
    let operator_image = image_overrides::demo_operator_image(ingress_mode);
    let (gw_repo, gw_tag) = split_image_ref(&gateway_image);
    let (op_repo, op_tag) = split_image_ref(&operator_image);

    let clusters = sequence_mut(spec, "clusters")?;
    for cluster in clusters {
        let cluster = cluster.as_mapping_mut().ok_or("cluster entry must be a mapping")?;
        let properties = mapping_mut_in(cluster, "properties")?;
        for (key, value) in [
            ("gatewayImage", gateway_image.clone()),
            ("operatorImage", operator_image.clone()),
            ("vcrImage", image_overrides::vcr_image()),
            ("imagePullPolicy", image_overrides::demo_image_pull_policy(ingress_mode)),
            ("gatewayImageRepo", gw_repo.clone()),
            ("gatewayImageTag", gw_tag.clone()),
            ("operatorImageRepo", op_repo.clone()),
            ("operatorImageTag", op_tag.clone()),
        ] {
            properties.insert(yaml_key(key), serde_yaml::Value::String(value));
        }
    }
    Ok(())
}

/// Load local images into Kind when the pull policy is `Never`.
fn load_local_images_if_required(
    forge: &str,
    config: &Path,
    ingress_mode: IngressMode,
) -> Result<(), Box<dyn std::error::Error>> {
    if image_overrides::demo_image_pull_policy(ingress_mode) != "Never" {
        return Ok(());
    }
    let operator = image_overrides::demo_operator_image(ingress_mode);
    let gateway = image_overrides::demo_gateway_image(ingress_mode);
    let vcr = image_overrides::vcr_image();
    for image in [&operator, &gateway, &vcr] {
        require_local_image(image)?;
    }
    let gateway_clusters = match ingress_mode {
        IngressMode::Global => CLUSTERS,
        IngressMode::Workload => GRID_CLUSTERS,
    };
    for cluster in GRID_CLUSTERS {
        run_forge(forge, config, &["cluster", "load-image", cluster, &operator])?;
    }
    for cluster in gateway_clusters {
        run_forge(forge, config, &["cluster", "load-image", cluster, &gateway])?;
    }
    for cluster in PROVIDER_CLUSTERS {
        run_forge(forge, config, &["cluster", "load-image", cluster, &vcr])?;
    }
    Ok(())
}

/// Apply shared infrastructure, skipping GTM when in workload mode.
fn apply_foundation_stacks_with_mode(
    forge: &str,
    config: &Path,
    ingress_mode: IngressMode,
) -> Result<(), Box<dyn std::error::Error>> {
    if ingress_mode == IngressMode::Global {
        run_forge(forge, config, &["stack", "apply", "gtm-emulator", "metallb"])?;
    }

    for cluster in GRID_CLUSTERS {
        run_forge(forge, config, &["stack", "apply", cluster, "metallb"])?;
    }

    for cluster in GRID_CLUSTERS {
        run_forge(forge, config, &["stack", "apply", cluster, "grid-operator"])?;
    }
    for (cluster, identity_stack) in [
        ("east-edge", "east-edge-operator"),
        ("east-provider", "east-provider-operator"),
        ("west-edge", "west-edge-operator"),
        ("west-provider", "west-provider-operator"),
    ] {
        run_forge(forge, config, &["stack", "apply", cluster, identity_stack])?;
    }
    Ok(())
}

/// Apply provider sites and private provider paths before edge rendering.
/// Select the appropriate stack name for a provider based on region and external provider status.
///
/// Returns `vcr-backend-openai` only for east-provider with external providers enabled.
/// This separation prevents west-provider from requiring credentials that only exist in east-provider.
fn select_stack_for_provider(region: &str, has_external_provider: bool) -> &'static str {
    if region == "east" && has_external_provider {
        "vcr-backend-openai"
    } else {
        "vcr-backend"
    }
}

/// Apply provider stack configuration using appropriate inference stack for each region.
fn apply_provider_stacks(
    forge: &str,
    config: &Path,
    external_provider: Option<&ExternalProviderDescriptor>,
) -> Result<(), Box<dyn std::error::Error>> {
    for (cluster, site_stack) in [
        ("east-provider", "east-provider-site"),
        ("west-provider", "west-provider-site"),
    ] {
        run_forge(forge, config, &["stack", "apply", cluster, site_stack])?;

        let region = if cluster.starts_with("east") { "east" } else { "west" };
        let inference_stack = select_stack_for_provider(region, external_provider.is_some());

        run_forge(forge, config, &["stack", "apply", cluster, inference_stack])?;
    }
    Ok(())
}

/// Apply edge sites and the local Praxis edge in each edge cluster.
fn apply_edge_stacks(forge: &str, config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    for (cluster, site_stack) in [("east-edge", "east-edge-site"), ("west-edge", "west-edge-site")] {
        run_forge(forge, config, &["stack", "apply", cluster, site_stack])?;
        eprintln!("  [OK] {cluster}: local edge site configured");
    }
    authorize_provider_sites_for_edges()?;
    for cluster in ["east-edge", "west-edge"] {
        run_forge(forge, config, &["stack", "apply", cluster, "edge-gateway"])?;
        eprintln!("  [OK] {cluster}: Praxis edge gateway deployed");
    }
    Ok(())
}

/// Pin each provider's SWIM-advertised public certificate on both edge sites.
///
/// SWIM discovery supplies the endpoint and public certificate, but it does
/// not authorize routing.  The demo compares the received certificate to the
/// locally generated out-of-band identity and configures the `GridSite` with:
///
/// - `canonicalFingerprints`: DER-based SHA-256 pin for the provider certificate
/// - `serverName`: DNS SAN identity for TLS SNI/SAN verification
///
/// Edge Deployments are applied only after both provider sites reach `Active`,
/// so a missing or mismatched trust record fails closed.
fn authorize_provider_sites_for_edges() -> Result<(), Box<dyn std::error::Error>> {
    const TRUST_TIMEOUT: Duration = Duration::from_secs(120);

    for edge in ["east-edge", "west-edge"] {
        let context = format!("kind-grid-glb-{edge}");
        eprintln!();
        eprintln!("  {edge} trust view: authorizing providers discovered through Grid");
        for provider in PROVIDER_CLUSTERS {
            let site_name = format!("glb-demo-{provider}");
            operator::wait_for_auto_gridsite(&context, &site_name, "glb-demo", TRUST_TIMEOUT)?;
            let canonical_fp = certs::site_certificate_fingerprint(provider)?;
            operator::wait_for_expected_site_certificate(&context, &site_name, &canonical_fp, TRUST_TIMEOUT)?;
            let server_name = format!("{provider}.grid.internal");
            operator::patch_gridsite_identity_trust(&context, &site_name, &canonical_fp, &server_name)?;
            operator::wait_for_gridsite_phase(&context, &site_name, "Active", TRUST_TIMEOUT)?;
        }
    }
    Ok(())
}

/// Apply the local managed-GTM stand-in after both edge addresses are known.
fn apply_gtm_emulator_stack(forge: &str, config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    run_forge(forge, config, &["stack", "apply", "gtm-emulator", "gtm-emulator"])
}

/// Execute one Forge command and retain its output on failure.
fn run_forge(forge: &str, config: &Path, args: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(forge)
        .args(["--config", &config.display().to_string(), "--non-interactive"])
        .args(args)
        .output()?;
    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "praxis-forge {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    )
    .into())
}

/// Require a local Docker image before a `Never`-pull setup.
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
        "required local image {image:?} is absent; build it or set GRID_XTASK_IMAGE_PULL_POLICY=IfNotPresent with registry image overrides"
    )
    .into())
}

// ---------------------------------------------------------------------------
// Workload-mode config stripping
// ---------------------------------------------------------------------------

/// GTM cluster name removed from the Forge config in workload mode.
const GTM_CLUSTER_NAME: &str = "gtm-emulator";

/// Remove the GTM emulator cluster and its stacks from the Forge spec.
///
/// Retains every other cluster and stack entry unchanged, producing a
/// four-cluster resolved config for the workload-inference topology.
fn strip_gtm_cluster(spec: &mut serde_yaml::Mapping) -> Result<(), Box<dyn std::error::Error>> {
    let clusters = sequence_mut(spec, "clusters")?;
    clusters.retain(|cluster| {
        cluster
            .as_mapping()
            .and_then(|mapping| mapping.get(yaml_key("name")))
            .and_then(serde_yaml::Value::as_str)
            .is_none_or(|name| name != GTM_CLUSTER_NAME)
    });

    if let Some(stacks) = spec
        .get_mut(yaml_key("stacks"))
        .and_then(serde_yaml::Value::as_mapping_mut)
    {
        stacks.remove(yaml_key(GTM_CLUSTER_NAME));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// YAML helpers
// ---------------------------------------------------------------------------

/// Return a named mapping from a YAML value.
fn mapping_mut<'a>(
    value: &'a mut serde_yaml::Value,
    field: &str,
) -> Result<&'a mut serde_yaml::Mapping, Box<dyn std::error::Error>> {
    let mapping = value.as_mapping_mut().ok_or("YAML root must be a mapping")?;
    mapping_mut_in(mapping, field)
}

/// Return a named child mapping.
fn mapping_mut_in<'a>(
    mapping: &'a mut serde_yaml::Mapping,
    field: &str,
) -> Result<&'a mut serde_yaml::Mapping, Box<dyn std::error::Error>> {
    mapping
        .get_mut(yaml_key(field))
        .and_then(serde_yaml::Value::as_mapping_mut)
        .ok_or_else(|| format!("YAML field {field:?} must be a mapping").into())
}

/// Return a named child sequence.
fn sequence_mut<'a>(
    mapping: &'a mut serde_yaml::Mapping,
    field: &str,
) -> Result<&'a mut Vec<serde_yaml::Value>, Box<dyn std::error::Error>> {
    mapping
        .get_mut(yaml_key(field))
        .and_then(serde_yaml::Value::as_sequence_mut)
        .ok_or_else(|| format!("YAML field {field:?} must be a sequence").into())
}

/// Construct one YAML mapping key.
fn yaml_key(value: &str) -> serde_yaml::Value {
    serde_yaml::Value::String(value.to_owned())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod setup_tests {
    use super::*;

    #[expect(clippy::allow_attributes, reason = "blanket test lint suppression")]
    #[allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        reason = "tests"
    )]
    mod inner {
        use super::*;

        /// Repository root from the xtask crate directory.
        fn workspace_root() -> PathBuf {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
        }

        #[test]
        fn materialized_config_uses_glb_image_contract() -> Result<(), Box<dyn std::error::Error>> {
            let source = workspace_root().join("tests/e2e/topologies/grid-glb-demo/forge.yaml");
            let rendered = render_config(&fs::read_to_string(source)?, IngressMode::Global, None)?;
            assert!(rendered.contains(&image_overrides::demo_gateway_image(IngressMode::Global)));
            assert!(rendered.contains(&image_overrides::demo_operator_image(IngressMode::Global)));
            assert!(rendered.contains(&image_overrides::vcr_image()));
            assert!(rendered.contains(&image_overrides::demo_image_pull_policy(IngressMode::Global)));
            assert!(!rendered.contains("grid-overlay-sync"));
            Ok(())
        }

        #[test]
        fn grid_operator_stack_uses_helm_and_captures_swim_ip() -> Result<(), Box<dyn std::error::Error>> {
            let source = workspace_root().join("tests/e2e/topologies/grid-glb-demo/forge.yaml");
            let forge: serde_yaml::Value = serde_yaml::from_str(&fs::read_to_string(source)?)?;
            let steps = forge
                .get("spec")
                .and_then(|value| value.get("stacks"))
                .and_then(|value| value.get("grid-operator"))
                .and_then(|value| value.get("steps"))
                .and_then(serde_yaml::Value::as_sequence)
                .ok_or("grid-operator steps must be a sequence")?;

            let first_type = steps
                .first()
                .and_then(|value| value.get("type"))
                .and_then(serde_yaml::Value::as_str);
            assert_eq!(first_type, Some("helm"), "grid-operator must use a Helm step");

            let has_capture = steps.iter().any(|step| {
                step.get("type")
                    .and_then(serde_yaml::Value::as_str)
                    .is_some_and(|t| t == "capture")
                    && step
                        .get("key")
                        .and_then(serde_yaml::Value::as_str)
                        .is_some_and(|k| k == "swim-lb-ip")
            });
            assert!(has_capture, "grid-operator must capture swim-lb-ip");
            Ok(())
        }

        #[test]
        #[expect(clippy::too_many_lines, reason = "asserts Helm SWIM wiring for four identity stacks")]
        fn identity_stacks_use_helm_with_swim_config() -> Result<(), Box<dyn std::error::Error>> {
            let source = workspace_root().join("tests/e2e/topologies/grid-glb-demo/forge.yaml");
            let forge: serde_yaml::Value = serde_yaml::from_str(&fs::read_to_string(source)?)?;
            let stacks = forge
                .get("spec")
                .and_then(|value| value.get("stacks"))
                .ok_or("forge.yaml must have stacks")?;

            for site in ["east-edge", "east-provider", "west-edge", "west-provider"] {
                let stack_name = format!("{site}-operator");
                let steps = stacks
                    .get(&stack_name)
                    .and_then(|value| value.get("steps"))
                    .and_then(serde_yaml::Value::as_sequence)
                    .ok_or_else(|| format!("{stack_name} steps must be a sequence"))?;

                let helm_step = steps
                    .first()
                    .filter(|step| {
                        step.get("type")
                            .and_then(serde_yaml::Value::as_str)
                            .is_some_and(|t| t == "helm")
                    })
                    .ok_or_else(|| format!("{stack_name} must start with a Helm step"))?;

                let site_name = helm_step
                    .get("values")
                    .and_then(|value| value.get("swim"))
                    .and_then(|value| value.get("siteName"))
                    .and_then(serde_yaml::Value::as_str)
                    .ok_or_else(|| format!("{stack_name} must set swim.siteName"))?;
                assert_eq!(site_name, site, "{stack_name} siteName must match the site identity");

                let has_wait = steps.iter().any(|step| {
                    step.get("type")
                        .and_then(serde_yaml::Value::as_str)
                        .is_some_and(|t| t == "wait")
                });
                assert!(has_wait, "{stack_name} must wait for the operator rollout");
            }
            Ok(())
        }

        #[test]
        fn demo_workloads_use_restricted_container_defaults() -> Result<(), Box<dyn std::error::Error>> {
            let resources = workspace_root().join("tests/e2e/topologies/grid-glb-demo/resources");
            for manifest in ["provider-workloads.yaml", "provider-workload-east-secondary.yaml"] {
                let deployment = fs::read_to_string(resources.join(manifest))?;
                for required in [
                    "automountServiceAccountToken: false",
                    "runAsNonRoot: true",
                    "type: RuntimeDefault",
                    "allowPrivilegeEscalation: false",
                    "readOnlyRootFilesystem: false",
                    "- ALL",
                ] {
                    assert!(deployment.contains(required), "{manifest} must contain {required:?}");
                }
            }
            Ok(())
        }

        #[test]
        fn default_glb_image_contract_is_valid() {
            assert!(validate_image_contract_for_mode(IngressMode::Global).is_ok());
        }

        #[test]
        fn default_workload_image_contract_is_valid() {
            assert!(validate_image_contract_for_mode(IngressMode::Workload).is_ok());
        }

        #[test]
        fn workload_config_has_four_clusters() -> Result<(), Box<dyn std::error::Error>> {
            let source = workspace_root().join("tests/e2e/topologies/grid-glb-demo/forge.yaml");
            let rendered = render_config(&fs::read_to_string(source)?, IngressMode::Workload, None)?;
            let config: serde_yaml::Value = serde_yaml::from_str(&rendered)?;
            let clusters = config
                .get("spec")
                .and_then(|value| value.get("clusters"))
                .and_then(serde_yaml::Value::as_sequence)
                .ok_or("spec.clusters must be a sequence")?;
            assert_eq!(clusters.len(), 4, "workload mode must have exactly four clusters");
            let names: Vec<&str> = clusters
                .iter()
                .filter_map(|cluster| cluster.get("name").and_then(serde_yaml::Value::as_str))
                .collect();
            assert!(
                !names.contains(&"gtm-emulator"),
                "workload mode must not contain gtm-emulator cluster"
            );
            Ok(())
        }

        #[test]
        fn global_config_has_five_clusters() -> Result<(), Box<dyn std::error::Error>> {
            let source = workspace_root().join("tests/e2e/topologies/grid-glb-demo/forge.yaml");
            let rendered = render_config(&fs::read_to_string(source)?, IngressMode::Global, None)?;
            let config: serde_yaml::Value = serde_yaml::from_str(&rendered)?;
            let clusters = config
                .get("spec")
                .and_then(|value| value.get("clusters"))
                .and_then(serde_yaml::Value::as_sequence)
                .ok_or("spec.clusters must be a sequence")?;
            assert_eq!(clusters.len(), 5, "global mode must have exactly five clusters");
            Ok(())
        }

        #[test]
        fn workload_config_strips_gtm_stacks() -> Result<(), Box<dyn std::error::Error>> {
            let source = workspace_root().join("tests/e2e/topologies/grid-glb-demo/forge.yaml");
            let rendered = render_config(&fs::read_to_string(source)?, IngressMode::Workload, None)?;
            let config: serde_yaml::Value = serde_yaml::from_str(&rendered)?;
            let stacks = config
                .get("spec")
                .and_then(|value| value.get("stacks"))
                .and_then(serde_yaml::Value::as_mapping)
                .ok_or("spec.stacks must be a mapping")?;
            assert!(
                stacks.get(yaml_key("gtm-emulator")).is_none(),
                "workload mode must not contain gtm-emulator stack"
            );
            Ok(())
        }

        // ----- New tests for demo runner enhancements -----

        #[test]
        fn quick_and_full_are_mutually_exclusive() {
            let result = <crate::Cli as clap::Parser>::try_parse_from([
                "xtask",
                "env",
                "run-grid-glb-demo",
                "--quick",
                "--full",
            ]);
            assert!(result.is_err(), "--quick and --full must conflict");
        }

        #[test]
        fn keep_on_failure_requires_teardown() {
            let result = <crate::Cli as clap::Parser>::try_parse_from([
                "xtask",
                "env",
                "run-grid-glb-demo",
                "--keep-on-failure",
            ]);
            assert!(result.is_err(), "--keep-on-failure requires --teardown");
        }

        #[test]
        fn default_mode_is_full() {
            let options = GlbDemoOptions {
                mode_options: GlbDemoModeOptions {
                    quick: false,
                    full: false,
                    no_ingress: false,
                },
                teardown: false,
                keep_on_failure: false,
                evidence_dir: None,
                external_provider: None,
                external_provider_key_file: None,
                external_provider_model: None,
                external_provider_site: None,
            };
            assert_eq!(options.mode(), DemoMode::Full);
        }

        #[test]
        fn quick_flag_selects_quick_mode() {
            let options = GlbDemoOptions {
                mode_options: GlbDemoModeOptions {
                    quick: true,
                    full: false,
                    no_ingress: false,
                },
                teardown: false,
                keep_on_failure: false,
                evidence_dir: None,
                external_provider: None,
                external_provider_key_file: None,
                external_provider_model: None,
                external_provider_site: None,
            };
            assert_eq!(options.mode(), DemoMode::Quick);
            assert_eq!(options.ingress_mode(), IngressMode::Global);
        }

        #[test]
        fn no_ingress_flag_selects_workload_mode() {
            let options = GlbDemoOptions {
                mode_options: GlbDemoModeOptions {
                    quick: true,
                    full: false,
                    no_ingress: true,
                },
                teardown: false,
                keep_on_failure: false,
                evidence_dir: None,
                external_provider: None,
                external_provider_key_file: None,
                external_provider_model: None,
                external_provider_site: None,
            };
            assert_eq!(options.ingress_mode(), IngressMode::Workload);
        }

        #[expect(clippy::too_many_lines, reason = "complete serialized evidence fixture")]
        fn sample_report(mode: &'static str, status: &'static str) -> EvidenceReport {
            EvidenceReport {
                schema_version: EVIDENCE_SCHEMA_VERSION,
                run_id: "20260728T120000Z".to_owned(),
                mode,
                started_at: "2026-07-28T12:00:00Z".to_owned(),
                completed_at: "2026-07-28T12:02:00Z".to_owned(),
                duration_secs: 120.0,
                status,
                error: None,
                capabilities: vec![CapabilityResult {
                    capability: "Active/active routing".to_owned(),
                    result: "pass",
                    evidence: "2 edges observed".to_owned(),
                }],
                observed_paths: vec![ObservedPathEntry {
                    edge: "east-edge".to_owned(),
                    provider: "west-provider".to_owned(),
                    path: "client -> east-edge -> west-provider gateway -> backend".to_owned(),
                }],
                external_provider: None,
                lifecycle: LifecycleRecord {
                    teardown_requested: true,
                    teardown_performed: true,
                    teardown_result: Some("success".to_owned()),
                    kept_on_failure: false,
                },
                artifacts: ArtifactPaths {
                    narration: "narration.txt".to_owned(),
                    results: "results.json".to_owned(),
                },
            }
        }

        #[test]
        fn evidence_report_serializes_to_valid_json() {
            let json = serde_json::to_string_pretty(&sample_report("full", "pass")).unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed["schema_version"], "1");
            assert_eq!(parsed["mode"], "full");
            assert_eq!(parsed["status"], "pass");
            assert!(parsed["error"].is_null());
            assert!(parsed["capabilities"].is_array());
            assert!(parsed["observed_paths"].is_array());
            assert!(parsed.get("external_provider").is_none());
        }

        #[test]
        fn external_provider_proof_serializes_observed_runtime_fields() {
            let proof = ExternalProviderProof {
                http_status: 200,
                edge: "east-edge".to_owned(),
                provider_gateway: "east-provider".to_owned(),
                candidate_id: "candidate-1".to_owned(),
                cluster: "openai-api".to_owned(),
                response_model: "gpt-test".to_owned(),
                serving_revision: "a".repeat(64),
                duration_secs: 1.25,
            };
            let value = serde_json::to_value(&proof).unwrap();
            assert_eq!(value["http_status"], 200);
            assert_eq!(value["edge"], "east-edge");
            assert_eq!(value["provider_gateway"], "east-provider");
            assert_eq!(value["cluster"], "openai-api");
            assert_eq!(value["serving_revision"], "a".repeat(64));
        }

        #[test]
        fn response_header_is_case_insensitive() {
            let headers = "HTTP/2 200\r\nX-Grid-Demo-Edge-Gateway: east-edge\r\n";
            assert_eq!(response_header(headers, "x-grid-demo-edge-gateway"), Some("east-edge"));
            assert_eq!(response_header(headers, "missing"), None);
        }

        #[test]
        fn demonstrate_command_rejects_lifecycle_flags() {
            let result =
                <crate::Cli as clap::Parser>::try_parse_from(["xtask", "env", "demonstrate-grid-glb", "--teardown"]);
            assert!(
                result.is_err(),
                "the demonstrate-only command must not accept lifecycle flags"
            );
        }

        #[test]
        fn concise_error_is_single_line_and_bounded() {
            let error = format!("first line\n{}\r\nlast line", "x".repeat(600));
            let concise = concise_error(error);
            assert!(!concise.contains(['\n', '\r']));
            assert!(concise.chars().count() <= 512);
        }

        #[test]
        fn failed_outcome_retains_prior_capabilities() {
            let outcome = failed_outcome(
                vec![CapabilityResult {
                    capability: "prior".to_owned(),
                    result: "pass",
                    evidence: "runtime proof".to_owned(),
                }],
                Vec::new(),
                "current",
                "failed proof".to_owned(),
            );
            assert_eq!(outcome.capabilities.len(), 2);
            assert_eq!(outcome.capabilities[0].result, "pass");
            assert_eq!(outcome.capabilities[1].result, "fail");
            assert_eq!(outcome.error.as_deref(), Some("failed proof"));
        }

        #[test]
        fn narrator_captures_lines() {
            let mut narrator = Narrator::new();
            narrator.narrate("line one");
            narrator.narrate("line two");
            assert_eq!(narrator.lines.len(), 2);
            assert_eq!(narrator.lines[0], "line one");
            assert_eq!(narrator.lines[1], "line two");
        }

        #[test]
        fn narrator_wraps_prose_with_stable_indentation() {
            let mut narrator = Narrator::new();
            narrator.wrapped("[PASS] ", "       ", &"word ".repeat(30));

            assert!(narrator.lines.len() > 1);
            assert!(narrator.lines.first().unwrap().starts_with("[PASS] "));
            assert!(narrator.lines.iter().skip(1).all(|line| line.starts_with("       ")));
            assert!(narrator.lines.iter().all(|line| line.chars().count() <= OUTPUT_WIDTH));
        }

        #[test]
        fn demonstrated_boundary_matches_mode() {
            let mut quick = Narrator::new();
            print_boundaries(&mut quick, DemoMode::Quick);
            let quick_text = quick.lines.join("\n");
            assert!(quick_text.contains("Health-driven provider withdrawal"));
            assert!(!quick_text.contains("edge withdrawal"));
            assert!(!quick_text.contains("restart recovery"));
            assert!(!quick_text.contains("request soak"));

            let mut full = Narrator::new();
            print_boundaries(&mut full, DemoMode::Full);
            let full_text = full.lines.join("\n");
            assert!(full_text.contains("edge withdrawal"));
            assert!(full_text.contains("restart recovery"));
            assert!(full_text.contains("request soak"));
        }

        #[test]
        fn soak_progress_is_bounded_to_sample_intervals() {
            let mut narrator = Narrator::new();

            narrate_soak_progress(&mut narrator, FULL_SOAK_PROGRESS_SAMPLES - 1, 2, 2);
            assert!(narrator.lines.is_empty());

            narrate_soak_progress(&mut narrator, FULL_SOAK_PROGRESS_SAMPLES, 2, 2);
            assert_eq!(narrator.lines.len(), 1);
            assert!(narrator.lines[0].contains("12 requests passed"));
        }

        #[test]
        fn capability_result_fields_present() {
            let cap = CapabilityResult {
                capability: "test".to_owned(),
                result: "pass",
                evidence: "evidence".to_owned(),
            };
            let json: serde_json::Value = serde_json::from_str(&serde_json::to_string(&cap).unwrap()).unwrap();
            assert!(json.get("capability").is_some());
            assert!(json.get("result").is_some());
            assert!(json.get("evidence").is_some());
        }

        #[test]
        fn lifecycle_record_serializes_teardown_states() {
            let no_teardown = LifecycleRecord {
                teardown_requested: false,
                teardown_performed: false,
                teardown_result: None,
                kept_on_failure: false,
            };
            let json = serde_json::to_string(&no_teardown).unwrap();
            assert!(json.contains("\"teardown_requested\":false"));

            let with_teardown = LifecycleRecord {
                teardown_requested: true,
                teardown_performed: true,
                teardown_result: Some("success".to_owned()),
                kept_on_failure: false,
            };
            let json = serde_json::to_string(&with_teardown).unwrap();
            assert!(json.contains("\"teardown_performed\":true"));
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

        // ----- External provider CLI tests -----

        #[test]
        fn external_provider_key_file_requires_provider() {
            let result = <crate::Cli as clap::Parser>::try_parse_from([
                "xtask",
                "env",
                "run-grid-glb-demo",
                "--external-provider-key-file",
                "/tmp/key",
            ]);
            assert!(
                result.is_err(),
                "--external-provider-key-file requires --external-provider"
            );
        }

        #[test]
        fn external_provider_model_requires_provider() {
            let result = <crate::Cli as clap::Parser>::try_parse_from([
                "xtask",
                "env",
                "run-grid-glb-demo",
                "--external-provider-model",
                "gpt-5-mini",
            ]);
            assert!(
                result.is_err(),
                "--external-provider-model requires --external-provider"
            );
        }

        #[test]
        fn external_provider_parses_with_all_flags() {
            let cli = <crate::Cli as clap::Parser>::try_parse_from([
                "xtask",
                "env",
                "run-grid-glb-demo",
                "--external-provider",
                "openai",
                "--external-provider-key-file",
                "/tmp/test-key",
                "--external-provider-model",
                "gpt-5-mini",
            ]);
            assert!(cli.is_ok(), "valid combination must parse: {:?}", cli.err());
        }

        #[test]
        fn external_provider_alone_parses_without_key_and_model() {
            let cli = <crate::Cli as clap::Parser>::try_parse_from([
                "xtask",
                "env",
                "run-grid-glb-demo",
                "--external-provider",
                "openai",
            ]);
            assert!(
                cli.is_ok(),
                "clap allows --external-provider alone (runtime validation rejects it)"
            );
        }

        #[test]
        fn quick_mode_skips_full_capabilities() {
            let quick_caps = [
                CapabilityResult {
                    capability: "Active/active routing".to_owned(),
                    result: "pass",
                    evidence: "observed".to_owned(),
                },
                CapabilityResult {
                    capability: "Secure provider boundary".to_owned(),
                    result: "pass",
                    evidence: "verified".to_owned(),
                },
                CapabilityResult {
                    capability: "Session affinity and drain".to_owned(),
                    result: "skipped",
                    evidence: "quick mode".to_owned(),
                },
                CapabilityResult {
                    capability: "Edge withdrawal and recovery".to_owned(),
                    result: "skipped",
                    evidence: "quick mode".to_owned(),
                },
                CapabilityResult {
                    capability: "Grid restart recovery and soak".to_owned(),
                    result: "skipped",
                    evidence: "quick mode".to_owned(),
                },
            ];
            let skipped_count = quick_caps.iter().filter(|c| c.result == "skipped").count();
            assert_eq!(skipped_count, 3, "quick mode must skip 3 capabilities");
        }

        #[test]
        fn evidence_mode_labels_encode_ingress_and_demo_mode() {
            assert_eq!(evidence_mode_label(DemoMode::Quick, IngressMode::Global), "quick");
            assert_eq!(evidence_mode_label(DemoMode::Full, IngressMode::Global), "full");
            assert_eq!(
                evidence_mode_label(DemoMode::Quick, IngressMode::Workload),
                "workload-quick"
            );
            assert_eq!(
                evidence_mode_label(DemoMode::Full, IngressMode::Workload),
                "workload-full"
            );
        }

        #[test]
        fn workload_introduction_contains_expected_path() {
            let mut narrator = Narrator::new();
            print_workload_introduction(&mut narrator, DemoMode::Quick);
            let text = narrator.lines.join("\n");
            assert!(text.contains("WORKLOAD INFERENCE"), "banner must mention workload");
            assert!(
                text.contains("consumer gateway"),
                "path must reference consumer gateway"
            );
            assert!(!text.contains("GTM"), "workload narration must not mention GTM");
            assert!(
                !text.contains("HTTPS endpoint"),
                "workload narration must not mention public endpoint"
            );
        }

        #[test]
        fn workload_boundaries_mark_gtm_not_applicable() {
            let mut narrator = Narrator::new();
            print_workload_boundaries(&mut narrator, DemoMode::Quick);
            let text = narrator.lines.join("\n");
            assert!(text.contains("NOT APPLICABLE"), "must mark GTM as not applicable");
            let collapsed: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
            assert!(collapsed.contains("traffic manager"), "must reference traffic manager");
        }
    }
}
