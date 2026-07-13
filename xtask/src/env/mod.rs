//! Multi-cluster test environment management.

pub(crate) mod certs;
pub(crate) mod config;
pub(crate) mod consumer;
pub(crate) mod gateway;
pub(crate) mod images;
pub(crate) mod kind;
pub(crate) mod operator;
pub(crate) mod operator_overlay;
pub(crate) mod providers;
pub(crate) mod trust;
pub(crate) mod verify;

use std::path::{Path, PathBuf};

use clap::Subcommand;

use self::config::EnvConfig;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default path to the environment configuration file.
const DEFAULT_CONFIG_PATH: &str = "tests/env/config.toml";

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// Actions for the `env` subcommand.
#[derive(Debug, Subcommand)]
pub(crate) enum Action {
    /// Create or update the test environment.
    Up {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Tear down the test environment.
    Down {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Report the status of all environment components.
    Status {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Verify provider inference endpoints are reachable.
    VerifyProviders {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Build gateway images from the AI repository.
    BuildGatewayImages {
        /// Path to the AI repository. Can also be provided via `AI_REPO_PATH`.
        #[arg(long)]
        ai_repo: Option<PathBuf>,
    },

    /// Load gateway images into all kind clusters.
    LoadGatewayImages {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Deploy provider gateways into provider clusters.
    DeployProviderGateways {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Verify provider gateways through the configured provider backend request path.
    VerifyProviderGateways {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Probe cross-kind network connectivity from consumer to providers.
    ProbeGatewayNetwork {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Deploy the consumer gateway.
    ///
    /// Without `--overlay-config`, generates a static `grid_route` config from
    /// provider sites declared in the environment config file.
    ///
    /// With `--overlay-config`, reads a routing overlay `grid-config.json`
    /// from the given path. The overlay `local_site` and candidates
    /// drive the `grid_route` stanza; the `load_balancer` section is still
    /// generated from provider endpoints in the environment config.
    DeployConsumerGateway {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,

        /// Path to a `grid-config.json` routing overlay.
        ///
        /// When provided, `grid_route.local_site` and candidates are taken
        /// from the overlay file.  When absent, the static config derived from
        /// `config.toml` provider sites is used.
        #[arg(long)]
        overlay_config: Option<PathBuf>,
    },

    /// Verify consumer-to-provider gateway routing end-to-end.
    VerifyGatewayE2e {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Verify gateway-to-gateway mTLS trust (positive + negative cases).
    VerifyMtlsTrust {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Install Grid CRDs (`GridNetwork`, `GridSite`, `InferenceProvider`) into a cluster.
    ///
    /// Generates CRD manifests from the Rust type definitions and applies them
    /// via `kubectl apply`.  Run after `env up` and before `verify-operator-reconcile`.
    InstallGridCrds {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,

        /// Site name from the config to install CRDs into (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Validate Grid operator reconciliation: install CRDs, apply test fixtures,
    /// run the operator locally, and verify health-aware overlay generation.
    ///
    /// Runs the operator binary **out-of-cluster** using the current kubeconfig.
    /// The operator must be compiled (`cargo build -p operator`) before this command.
    VerifyOperatorReconcile {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,

        /// Site name from the config to run the validation against.
        #[arg(long)]
        site: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Run the requested environment action.
///
/// # Errors
///
/// Returns an error if the configuration cannot be loaded or the
/// action fails.
pub(crate) fn run(action: &Action) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        Action::Up { config } => env_up(config),
        Action::Down { config } => env_down(config),
        Action::Status { config } => env_status(config),
        Action::VerifyProviders { config } => env_verify_providers(config),
        Action::BuildGatewayImages { ai_repo } => env_build_gateway_images(ai_repo.as_deref()),
        Action::LoadGatewayImages { config } => env_load_gateway_images(config),
        Action::DeployProviderGateways { config } => env_deploy_provider_gateways(config),
        Action::VerifyProviderGateways { config } => env_verify_provider_gateways(config),
        Action::ProbeGatewayNetwork { config } => env_probe_gateway_network(config),
        Action::DeployConsumerGateway { config, overlay_config } => {
            env_deploy_consumer_gateway(config, overlay_config.as_deref())
        },
        Action::VerifyGatewayE2e { config } => env_verify_gateway_e2e(config),
        Action::VerifyMtlsTrust { config } => env_verify_mtls_trust(config),
        Action::InstallGridCrds { config, site } => env_install_grid_crds(config, site.as_deref()),
        Action::VerifyOperatorReconcile { config, site } => env_verify_operator_reconcile(config, site.as_deref()),
    }
}

/// Create or update the test environment.
fn env_up(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    print_topology(&cfg);

    for name in &cfg.clusters.names {
        if let Some(def) = cfg.clusters.definitions.get(name) {
            kind::create_cluster(name, def)?;
        }
    }

    certs::generate_all(&cfg.clusters.names)?;

    if let Err(e) = providers::start_all(&cfg.providers) {
        eprintln!("warning: mock providers failed to start: {e}");
        eprintln!("         (build the grid-mock-providers image first if needed)");
        eprintln!("         provider inference baseline does not require mock providers");
    }

    eprintln!("env up: clusters and certs ready");
    Ok(())
}

/// Tear down the test environment.
fn env_down(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;

    for name in &cfg.clusters.names {
        kind::delete_cluster(name)?;
    }

    providers::stop_all()?;
    certs::cleanup()?;
    eprintln!("env down: clusters, providers, and certs removed");
    Ok(())
}

/// Report the status of all environment components.
fn env_status(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    let mut all_ok = true;

    all_ok = report_cluster_status(&cfg, all_ok);
    all_ok = report_provider_status(all_ok);
    all_ok = report_cert_status(all_ok);

    let summary = if all_ok {
        "all components healthy"
    } else {
        "some components not ready"
    };
    eprintln!("status: {summary}");
    Ok(())
}

/// Report cluster and deployment status.
fn report_cluster_status(cfg: &EnvConfig, mut all_ok: bool) -> bool {
    eprintln!("clusters:");
    for name in &cfg.clusters.names {
        let ok = kind::is_cluster_running(name);
        all_ok = all_ok && ok;
        eprintln!("  grid-{name}: {}", status_label(ok));
        if ok
            && let Some(def) = cfg.clusters.definitions.get(name)
            && def.role == config::ClusterRole::Provider
        {
            let deploy_ok = kind::is_provider_backend_ready(name, def);
            all_ok = all_ok && deploy_ok;
            let deploy = kind::provider_backend_deployment_name(def);
            eprintln!("    {deploy}: {}", status_label(deploy_ok));
        }
    }
    all_ok
}

/// Report mock provider container status.
fn report_provider_status(mut all_ok: bool) -> bool {
    eprintln!("providers:");
    for provider in &["openai", "anthropic", "bedrock", "vertex"] {
        let ok = providers::is_running(provider);
        all_ok = all_ok && ok;
        eprintln!("  mock-{provider}: {}", status_label(ok));
    }
    all_ok
}

/// Report certificate status.
fn report_cert_status(mut all_ok: bool) -> bool {
    eprintln!("certificates:");
    let ok = certs::certs_exist();
    all_ok = all_ok && ok;
    eprintln!("  CA + site certs: {}", status_label(ok));
    all_ok
}

/// Verify provider inference endpoints.
fn env_verify_providers(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    verify::verify_providers(&cfg)
}

/// Build gateway images from the AI repository.
fn env_build_gateway_images(ai_repo: Option<&Path>) -> Result<(), Box<dyn std::error::Error>> {
    let resolved = images::resolve_ai_repo_path(ai_repo)?;
    eprintln!("building gateway images from {}...", resolved.display());
    images::build_all(&resolved)?;
    eprintln!("env build-gateway-images: done");
    Ok(())
}

/// Load gateway images into all kind clusters.
fn env_load_gateway_images(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    eprintln!("loading gateway images into kind clusters...");
    images::load_all(&cfg)?;
    eprintln!("env load-gateway-images: done");
    Ok(())
}

/// Deploy provider gateways into provider clusters.
fn env_deploy_provider_gateways(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    eprintln!("deploying provider gateways...");
    gateway::deploy_all(&cfg)?;
    eprintln!("env deploy-provider-gateways: done");
    Ok(())
}

/// Verify provider gateways.
fn env_verify_provider_gateways(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    gateway::verify_all(&cfg)
}

/// Probe cross-kind network connectivity from the consumer cluster to all
/// provider gateways.
fn env_probe_gateway_network(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    consumer::probe_network(&cfg)
}

/// Deploy the consumer gateway.
///
/// When `overlay_config` is `Some`, reads a routing overlay `grid-config.json`
/// and uses it to drive the `grid_route` stanza.
/// When `overlay_config` is `None`, generates a static config from the
/// environment config provider sites (existing behaviour).
fn env_deploy_consumer_gateway(config: &Path, overlay_config: Option<&Path>) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    consumer::deploy_consumer(&cfg, overlay_config)
}

/// Verify consumer-to-provider gateway routing end-to-end.
fn env_verify_gateway_e2e(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    consumer::verify_e2e(&cfg)
}

/// Verify gateway-to-gateway mTLS trust (positive + negative cases).
fn env_verify_mtls_trust(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    trust::verify_mtls_trust(&cfg)
}

/// Format a status label.
fn status_label(ok: bool) -> &'static str {
    if ok { "ready" } else { "not ready" }
}

/// Print the configured topology summary.
fn print_topology(cfg: &EnvConfig) {
    eprintln!("env up: {} clusters, 4 providers", cfg.clusters.names.len(),);
    for name in &cfg.clusters.names {
        if let Some(def) = cfg.clusters.definitions.get(name) {
            eprintln!("  {name}: {:?}, models: {}", def.role, def.models.join(", "),);
        }
    }
    eprintln!("  openai:    port {}", cfg.providers.openai.port);
    eprintln!("  anthropic: port {}", cfg.providers.anthropic.port);
    eprintln!(
        "  bedrock:   port {} ({})",
        cfg.providers.bedrock.port, cfg.providers.bedrock.region,
    );
    eprintln!(
        "  vertex:    port {} ({})",
        cfg.providers.vertex.port, cfg.providers.vertex.project,
    );
}

// ---------------------------------------------------------------------------
// Grid operator commands
// ---------------------------------------------------------------------------

/// Resolve the kubectl context for the target site.
///
/// Uses the first provider site in the config when `site` is `None`.
fn resolve_operator_context(cfg: &EnvConfig, site: Option<&str>) -> Result<String, Box<dyn std::error::Error>> {
    let name = site
        .or_else(|| cfg.clusters.names.first().map(String::as_str))
        .ok_or("no sites in config")?;
    Ok(kind::kubectl_context(name))
}

/// Install Grid CRDs into the selected kind cluster.
fn env_install_grid_crds(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("install-grid-crds: context={context}");
    operator::install_grid_crds(&context)?;
    eprintln!("install-grid-crds: done");
    Ok(())
}

/// Run the full Grid operator reconciliation validation.
#[expect(
    clippy::too_many_lines,
    reason = "sequential E2E steps: CRD install, fixtures, operator spawn, poll, verify, cleanup"
)]
fn env_verify_operator_reconcile(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        CONFIGMAP_POLL_TIMEOUT, ERROR_ENDPOINT_LOCAL_PORT, ERROR_ENDPOINT_NAME, POD_READY_TIMEOUT, STATUS_POLL_TIMEOUT,
        TEST_DEGRADED_ROUTING_CLUSTER, TEST_GATEWAY_NAME, TEST_GATEWAY_NS, TEST_HEALTHY_ROUTING_CLUSTER, TEST_NETWORK,
        TEST_PROVIDER_API, TEST_PROVIDER_DEGRADED, TEST_PROVIDER_HEALTHY, TEST_PROVIDER_INVALID,
    };

    // Guard that kills a process on drop, even on early return.
    struct ProcGuard(Option<std::process::Child>, &'static str);
    impl Drop for ProcGuard {
        fn drop(&mut self) {
            if let Some(mut c) = self.0.take() {
                drop(c.kill());
                drop(c.wait());
                eprintln!("  {} stopped", self.1);
            }
        }
    }

    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("verify-operator-reconcile: context={context}");

    // Step 1: install Grid CRDs.
    operator::install_grid_crds(&context)?;
    operator::cleanup_validation_resources(&context)?;

    // Step 2: deploy the HTTP 503 error-endpoint Pod and wait for it to be ready.
    // This provides the health probe target for the Degraded provider.
    operator::apply_error_endpoint_fixture(&context)?;
    operator::wait_for_error_endpoint_ready(&context, POD_READY_TIMEOUT)?;

    // Step 3: port-forward the error endpoint to the operator host so the
    // out-of-cluster operator can reach it at 127.0.0.1:18503.
    let pf_child = operator::start_error_endpoint_port_forward(&context)?;
    let mut pf_guard = ProcGuard(Some(pf_child), ERROR_ENDPOINT_NAME);

    // Step 4: spawn the operator out-of-cluster.
    let op_child = operator::spawn_operator(&context)?;
    eprintln!("  operator spawned (PID {})", op_child.id());
    let mut op_guard = ProcGuard(Some(op_child), "operator");

    let degraded_endpoint = format!("http://127.0.0.1:{ERROR_ENDPOINT_LOCAL_PORT}");

    // Step 5: apply provider fixtures.
    // GridNetwork is created first (inside apply_test_fixtures); all providers are
    // applied after the network so the operator can resolve gridNetworkRef immediately.
    // api_provider is applied last to prove scoring order is score-driven, not input-order.
    let healthy_endpoint = "http://mock-openai-provider.default.svc:8080";
    let api_endpoint = "https://api.anthropic.com"; // static, not probed (no healthCheck)
    operator::apply_test_fixtures(&context, healthy_endpoint)?;
    operator::apply_degraded_provider_fixture(&context, &degraded_endpoint)?;
    operator::apply_api_provider_fixture(&context, api_endpoint)?;

    // Step 6: wait for all providers to reconcile.
    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        operator::wait_for_provider_phase(&context, TEST_PROVIDER_INVALID, "Unavailable", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(&context, TEST_PROVIDER_HEALTHY, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(&context, TEST_PROVIDER_DEGRADED, "Degraded", STATUS_POLL_TIMEOUT)?;
        // api provider has no healthCheck and no matching sites → Pending
        operator::wait_for_provider_phase(&context, TEST_PROVIDER_API, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_overlay_configmap(
            &context,
            TEST_NETWORK,
            TEST_GATEWAY_NAME,
            TEST_GATEWAY_NS,
            CONFIGMAP_POLL_TIMEOUT,
        )?;

        // Step 7: verify overlay contents.
        // NOTE: candidate cluster/site values use routingClusterRef when set.
        // - op-e2e-healthy has routingClusterRef="site-a" → candidate.cluster="site-a"
        // - op-e2e-degraded has routingClusterRef="site-a" → candidate.cluster="site-a"
        // - op-e2e-invalid (blank endpoint) → excluded (Unavailable)
        // - op-e2e-api-fallback (no routingClusterRef) → cluster="op-e2e-api-fallback"
        let overlay = operator::read_overlay_configmap(&context, TEST_NETWORK, TEST_GATEWAY_NAME, TEST_GATEWAY_NS)?;
        operator::verify_overlay(&overlay, TEST_HEALTHY_ROUTING_CLUSTER, TEST_PROVIDER_INVALID)?;
        operator::verify_degraded_candidate(&overlay, TEST_DEGRADED_ROUTING_CLUSTER)?;
        // Verify scoring order: local (site-a) before api_provider.
        operator::verify_scoring_order(&overlay, TEST_HEALTHY_ROUTING_CLUSTER, TEST_PROVIDER_API)?;

        // Step 8: export overlay for Praxis handoff attempt.
        let overlay_path =
            operator::export_overlay_to_file(&context, TEST_NETWORK, TEST_GATEWAY_NAME, TEST_GATEWAY_NS)?;
        eprintln!("  overlay exported: {}", overlay_path.display());
        Ok(())
    })();

    // Step 8: stop port-forward and operator before returning.
    if let Some(c) = op_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(mut c) = pf_guard.0.take() {
        drop(c.kill());
        drop(c.wait());
    }

    result?;
    eprintln!("verify-operator-reconcile: PASS");
    Ok(())
}
