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

use self::config::{ClusterRole, EnvConfig};

// ---------------------------------------------------------------------------
// Shared infrastructure helpers
// ---------------------------------------------------------------------------

/// RAII guard that kills a subprocess on drop.
///
/// Used to ensure the operator and port-forward processes are always stopped
/// when the reconcile function returns, even on error.
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

    /// Validate the full operator-to-consumer routing flow in kind.
    ///
    /// Orchestrates in one command:
    ///
    /// 1. Deploy provider gateways (idempotent).
    /// 2. Install Grid CRDs and apply `InferenceProvider` fixtures.
    /// 3. Run the Grid operator out-of-cluster (spawned via `cargo run`).
    /// 4. Wait for provider reconciliation:
    ///    - healthy → `Pending`
    ///    - invalid → `Unavailable`
    ///    - degraded → `Degraded`
    ///    - api fallback → `Pending`
    /// 5. Verify the overlay `ConfigMap` (healthy present, unavailable excluded, scoring order).
    /// 6. Export the overlay to a temp file.
    /// 7. Deploy the consumer gateway from the operator-exported overlay.
    /// 8. Verify end-to-end routing: locally routable model returns 200, unknown model fails cleanly.
    ///
    /// Requires kind clusters and gateway images to be ready.  Run `env up` and
    /// `env load-gateway-images` first.  Safe to rerun: owned test resources are
    /// deleted at the start of each run.
    ValidateOperatorRouting {
        /// Path to the environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing.toml")]
        config: PathBuf,

        /// Site name from the config to run the operator against (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Prove live SWIM membership reaches `GridNetwork` status.
    ///
    /// Starts two out-of-cluster operator processes each with a distinct SWIM
    /// identity, has the secondary announce to the primary, waits for gossip
    /// convergence, then applies a `GridNetwork` resource and polls until:
    ///
    /// - `status.phase = Active` (derived from live `MembershipSnapshot`)
    /// - `status.connectedSites ≥ 1` (one SWIM peer confirmed alive)
    ///
    /// Uses available localhost UDP ports selected at runtime. Both operators
    /// connect to the same kind cluster (context resolved from `config` via
    /// `--site`). Safe to rerun: the `GridNetwork` fixture is deleted before
    /// and after the run.
    ///
    /// Requires a kind cluster with Grid CRDs installable (`env up` +
    /// `env load-gateway-images` are **not** required — this command installs
    /// the CRDs itself).
    VerifySwimMembership {
        /// Path to the environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing.toml")]
        config: PathBuf,

        /// Kind cluster context to run against (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Prove live CRDT state propagation over SWIM.
    ///
    /// Starts two SWIM-enabled operator processes against the same kind
    /// cluster.  Each operator publishes its own site-presence as a CRDT
    /// `GridStateSnapshot` on every `GridNetwork` reconcile.  After SWIM
    /// gossip convergence the remote operator's broadcast arrives and
    /// `GridNetwork.status.distributedProviderCount` becomes ≥ 1.
    ///
    /// Proves that:
    /// - Operators use real foca UDP custom broadcasts (not direct injection).
    /// - The `StateBroadcastHandler` receives and merges remote state.
    /// - `GridNetworkStatus.distributedProviderCount` reflects the merged state.
    ///
    /// Requires a kind cluster.  Run `env up` + `env load-gateway-images` first.
    /// Safe to rerun: the `GridNetwork` fixture is deleted at the start of each run.
    VerifySwimState {
        /// Path to the environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing.toml")]
        config: PathBuf,

        /// Kind cluster context to run against (first provider site by default).
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
        Action::ValidateOperatorRouting { config, site } => env_validate_operator_routing(config, site.as_deref()),
        Action::VerifySwimMembership { config, site } => env_verify_swim_membership(config, site.as_deref()),
        Action::VerifySwimState { config, site } => env_verify_swim_state(config, site.as_deref()),
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
            && def.role == ClusterRole::Provider
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
    let name = if let Some(site) = site {
        site
    } else {
        cfg.clusters
            .names
            .iter()
            .find(|name| {
                cfg.clusters
                    .definitions
                    .get(*name)
                    .is_some_and(|d| d.role == ClusterRole::Provider)
            })
            .map(String::as_str)
            .ok_or("no provider site in config")?
    };
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

/// Install CRDs, apply operator test fixtures, run the operator, verify the overlay,
/// and export it to a temp file.
///
/// Returns the path of the exported `grid-config.json` overlay file.
/// The caller is responsible for killing the operator and port-forward processes
/// before this function returns — both are wrapped in [`ProcGuard`] so they are
/// stopped on drop even on early return.
///
/// This is the shared core of both [`env_verify_operator_reconcile`] and
/// [`env_validate_operator_routing`].
#[expect(
    clippy::too_many_lines,
    reason = "sequential reconcile steps: CRD install, fixtures, operator spawn, poll, verify, export"
)]
fn run_operator_reconcile(context: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    use operator::{
        CONFIGMAP_POLL_TIMEOUT, ERROR_ENDPOINT_LOCAL_PORT, ERROR_ENDPOINT_NAME, METRICS_BUSY_LOCAL_PORT,
        METRICS_IDLE_LOCAL_PORT, POD_READY_TIMEOUT, STATUS_POLL_TIMEOUT, TEST_DEGRADED_ROUTING_CLUSTER,
        TEST_GATEWAY_NAME, TEST_GATEWAY_NS, TEST_HEALTHY_ROUTING_CLUSTER, TEST_METRICS_BUSY_PROVIDER,
        TEST_METRICS_BUSY_ROUTING_CLUSTER, TEST_METRICS_IDLE_PROVIDER, TEST_METRICS_IDLE_ROUTING_CLUSTER, TEST_NETWORK,
        TEST_PROVIDER_API, TEST_PROVIDER_DEGRADED, TEST_PROVIDER_HEALTHY, TEST_PROVIDER_INVALID,
    };

    // Step 1: install Grid CRDs and remove stale owned resources.
    operator::install_grid_crds(context)?;
    operator::cleanup_validation_resources(context)?;

    // Step 2: deploy the HTTP 503 error-endpoint Pod.
    // The operator health probe reaches this endpoint to classify the provider as Degraded.
    operator::apply_error_endpoint_fixture(context)?;
    operator::wait_for_error_endpoint_ready(context, POD_READY_TIMEOUT)?;

    // Step 2b: deploy metrics endpoint Pods.
    // Each Pod serves a fixed Prometheus gauge for queue depth.  They are deployed
    // before spawning the operator so they are ready when the first reconcile fires.
    // - idle Pod → provider_queue_depth_normalized 0.1 (low queue → high score)
    // - busy Pod → provider_queue_depth_normalized 0.9 (high queue → low score)
    operator::apply_and_wait_for_metrics_pods(context, POD_READY_TIMEOUT)?;

    // Step 3: port-forward all endpoints so the out-of-cluster operator can reach them.
    let pf_child = operator::start_error_endpoint_port_forward(context)?;
    let mut pf_guard = ProcGuard(Some(pf_child), ERROR_ENDPOINT_NAME);

    let pf_idle_child =
        operator::start_named_pod_port_forward(context, TEST_METRICS_IDLE_PROVIDER, METRICS_IDLE_LOCAL_PORT)?;
    let mut pf_idle_guard = ProcGuard(Some(pf_idle_child), TEST_METRICS_IDLE_PROVIDER);

    let pf_busy_child =
        operator::start_named_pod_port_forward(context, TEST_METRICS_BUSY_PROVIDER, METRICS_BUSY_LOCAL_PORT)?;
    let mut pf_busy_guard = ProcGuard(Some(pf_busy_child), TEST_METRICS_BUSY_PROVIDER);

    // Step 4: spawn the operator out-of-cluster.
    let op_child = operator::spawn_operator(context)?;
    eprintln!("  operator spawned (PID {})", op_child.id());
    let mut op_guard = ProcGuard(Some(op_child), "operator");

    let degraded_endpoint = format!("http://127.0.0.1:{ERROR_ENDPOINT_LOCAL_PORT}");
    let metrics_idle_endpoint = format!("http://127.0.0.1:{METRICS_IDLE_LOCAL_PORT}");
    let metrics_busy_endpoint = format!("http://127.0.0.1:{METRICS_BUSY_LOCAL_PORT}");

    // Step 5: apply InferenceProvider fixtures.
    // GridNetwork is created first; providers follow so the operator resolves gridNetworkRef immediately.
    // api_provider is last to prove scoring is score-driven, not input-order-driven.
    //
    // routingClusterRef controls overlay candidate identity:
    // - op-e2e-healthy:       routingClusterRef="site-a"           → candidate.site="site-a"
    // - op-e2e-degraded:      routingClusterRef="site-a"           → fresh=false
    // - op-e2e-invalid:       blank endpoint                       → Unavailable, excluded
    // - op-e2e-api-fallback:  no routingClusterRef                 → cluster="op-e2e-api-fallback"
    // - op-e2e-metrics-idle:  routingClusterRef="site-metrics-idle" → metrics scraped (queue=0.1)
    // - op-e2e-metrics-busy:  routingClusterRef="site-metrics-busy" → metrics scraped (queue=0.9)
    let healthy_endpoint = "http://mock-openai-provider.default.svc:8080";
    let api_endpoint = "https://api.anthropic.com";
    operator::apply_test_fixtures(context, healthy_endpoint)?;
    operator::apply_degraded_provider_fixture(context, &degraded_endpoint)?;
    operator::apply_api_provider_fixture(context, api_endpoint)?;
    operator::apply_metrics_provider_fixtures(context, &metrics_idle_endpoint, &metrics_busy_endpoint)?;

    // Step 6–7: wait for reconciliation and verify overlay.
    let result = (|| -> Result<PathBuf, Box<dyn std::error::Error>> {
        operator::wait_for_provider_phase(context, TEST_PROVIDER_INVALID, "Unavailable", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(context, TEST_PROVIDER_HEALTHY, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(context, TEST_PROVIDER_DEGRADED, "Degraded", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(context, TEST_PROVIDER_API, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(context, TEST_METRICS_IDLE_PROVIDER, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(context, TEST_METRICS_BUSY_PROVIDER, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_overlay_configmap(
            context,
            TEST_NETWORK,
            TEST_GATEWAY_NAME,
            TEST_GATEWAY_NS,
            CONFIGMAP_POLL_TIMEOUT,
        )?;

        let overlay = operator::read_overlay_configmap(context, TEST_NETWORK, TEST_GATEWAY_NAME, TEST_GATEWAY_NS)?;
        operator::verify_overlay(&overlay, TEST_HEALTHY_ROUTING_CLUSTER, TEST_PROVIDER_INVALID)?;
        operator::verify_degraded_candidate(&overlay, TEST_DEGRADED_ROUTING_CLUSTER)?;
        operator::verify_scoring_order(&overlay, TEST_HEALTHY_ROUTING_CLUSTER, TEST_PROVIDER_API)?;
        // Verify that live scraped metrics reordered the equal-locality providers:
        // idle (queue=0.1, high score) must appear before busy (queue=0.9, low score).
        operator::verify_metrics_ordering(
            &overlay,
            TEST_METRICS_IDLE_ROUTING_CLUSTER,
            TEST_METRICS_BUSY_ROUTING_CLUSTER,
        )?;

        // Step 8: export overlay for consumer gateway handoff.
        let path = operator::export_overlay_to_file(context, TEST_NETWORK, TEST_GATEWAY_NAME, TEST_GATEWAY_NS)?;
        eprintln!("  overlay exported: {}", path.display());
        Ok(path)
    })();

    // Always stop the operator and all port-forwards before returning.
    if let Some(c) = op_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(mut c) = pf_guard.0.take() {
        drop(c.kill());
        drop(c.wait());
    }
    if let Some(mut c) = pf_idle_guard.0.take() {
        drop(c.kill());
        drop(c.wait());
    }
    if let Some(mut c) = pf_busy_guard.0.take() {
        drop(c.kill());
        drop(c.wait());
    }

    result
}

/// Verify Grid operator reconciliation only (CRD install → overlay export).
fn env_verify_operator_reconcile(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("verify-operator-reconcile: context={context}");
    run_operator_reconcile(&context)?;
    eprintln!("verify-operator-reconcile: PASS");
    Ok(())
}

/// Run the full operator-to-consumer routing validation in kind.
///
/// Orchestrates provider gateway deployment, operator reconcile + overlay export,
/// consumer gateway deployment from the operator overlay, and end-to-end routing
/// verification in a single idempotent command.
fn env_validate_operator_routing(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("validate-operator-routing: context={context}");

    eprintln!("validate-operator-routing: [1/4] deploying provider gateways...");
    gateway::deploy_all(&cfg)?;

    eprintln!("validate-operator-routing: [2/4] operator reconcile + overlay export...");
    let overlay_path = run_operator_reconcile(&context)?;

    eprintln!("validate-operator-routing: [3/4] deploying consumer gateway from operator overlay...");
    consumer::deploy_consumer(&cfg, Some(&overlay_path))?;

    eprintln!("validate-operator-routing: [4/4] verifying end-to-end routing...");
    consumer::verify_e2e(&cfg)?;

    eprintln!("validate-operator-routing: PASS");
    Ok(())
}

/// Prove that live SWIM membership reaches `GridNetwork` status.
///
/// Starts two SWIM-enabled operator processes, waits for gossip convergence,
/// applies a `GridNetwork` fixture, then polls until `phase = Active` and
/// `connectedSites ≥ 1`.
///
/// Both operators connect to the same kind cluster (the first provider site from
/// `config`). They bind on available localhost UDP ports chosen at runtime.
fn env_verify_swim_membership(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        SWIM_CONVERGENCE_WAIT, SWIM_NODE_PRIMARY_NAME, SWIM_NODE_SECONDARY_NAME, SWIM_STATUS_POLL_TIMEOUT,
        SWIM_TEST_NETWORK,
    };

    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("verify-swim-membership: context={context}");

    // Step 1: install Grid CRDs and remove any stale SWIM test resources.
    operator::install_grid_crds(&context)?;
    operator::cleanup_swim_test_resources(&context)?;

    let (bind1, bind2) = reserve_swim_bind_addrs()?;

    // Step 2: start the primary SWIM operator (no seeds — it is the first member).
    let op1 = operator::spawn_operator_with_swim(&context, &bind1, &bind1, SWIM_NODE_PRIMARY_NAME, "")?;
    let mut op1_guard = ProcGuard(Some(op1), "operator-primary");

    // Step 3: start the secondary operator with the primary as its seed.
    // spawn_operator_with_swim includes a 3-second post-spawn settle sleep, so
    // the primary's SWIM listener is ready before the secondary announces.
    let op2 = operator::spawn_operator_with_swim(&context, &bind2, &bind2, SWIM_NODE_SECONDARY_NAME, &bind1)?;
    let mut op2_guard = ProcGuard(Some(op2), "operator-secondary");

    // Step 4: wait for SWIM gossip to converge (both nodes see each other as Alive).
    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);

    // Step 5: apply the GridNetwork fixture.
    // Both SWIM-enabled operators are watching for GridNetwork resources.
    // The watch event from the new resource triggers an immediate reconcile in
    // both operators; by this point SWIM has converged, so each operator's live
    // MembershipSnapshot already contains the other as an Alive peer.
    operator::apply_swim_test_network(&context)?;
    eprintln!("  GridNetwork {SWIM_TEST_NETWORK} applied; awaiting Active status from live SWIM snapshot...");

    // Step 6: poll until the GridNetwork status reflects the SWIM membership.
    let result = operator::wait_for_gridnetwork_active(&context, SWIM_TEST_NETWORK, SWIM_STATUS_POLL_TIMEOUT);

    // Always stop both operators before propagating errors.
    if let Some(c) = op1_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op2_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_swim_test_resources(&context)?;

    // Step 7: verify the result.
    let connected_sites = result?;
    operator::verify_swim_status("Active", connected_sites)?;

    eprintln!("verify-swim-membership: PASS (connectedSites={connected_sites})");
    Ok(())
}

/// Prove that live CRDT state propagates between two SWIM-enabled operators via foca broadcast.
///
/// Both operators publish their own site-presence as a `GridStateSnapshot` on each reconcile.
/// After SWIM gossip convergence, each operator's `state_snapshot()` includes the remote site's
/// provider entry, and `GridNetworkStatus.distributedProviderCount` becomes ≥ 1.
fn env_verify_swim_state(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        SWIM_CONVERGENCE_WAIT, SWIM_NODE_PRIMARY_NAME, SWIM_NODE_SECONDARY_NAME, SWIM_STATUS_POLL_TIMEOUT,
        SWIM_TEST_NETWORK,
    };

    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("verify-swim-state: context={context}");

    operator::install_grid_crds(&context)?;
    operator::cleanup_swim_test_resources(&context)?;

    // Start primary operator (no seeds).
    let (bind1, bind2) = reserve_swim_bind_addrs()?;
    let op1 = operator::spawn_operator_with_swim(&context, &bind1, &bind1, SWIM_NODE_PRIMARY_NAME, "")?;
    let mut op1_guard = ProcGuard(Some(op1), "operator-primary");

    // Start secondary operator; seeds point to primary.
    let op2 = operator::spawn_operator_with_swim(&context, &bind2, &bind2, SWIM_NODE_SECONDARY_NAME, &bind1)?;
    let mut op2_guard = ProcGuard(Some(op2), "operator-secondary");

    // Wait for SWIM convergence.
    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);

    // Apply the GridNetwork fixture; both operators reconcile immediately.
    // Each operator publishes a site-presence StateBroadcast and gossips to its peer.
    // After convergence, each operator has the other's provider in its merged state.
    operator::apply_swim_test_network(&context)?;
    eprintln!("  GridNetwork {SWIM_TEST_NETWORK} applied; operators will reconcile and exchange CRDT state...");

    // Poll for distributedProviderCount > 0 (proves real distributed state arrived via SWIM broadcast).
    let distributed_result =
        operator::wait_for_gridnetwork_distributed_state(&context, SWIM_TEST_NETWORK, SWIM_STATUS_POLL_TIMEOUT);

    // Cleanup.
    if let Some(c) = op1_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op2_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_swim_test_resources(&context)?;

    let distributed_count = distributed_result?;
    operator::verify_distributed_state_received(distributed_count)?;

    eprintln!("verify-swim-state: PASS (distributedProviderCount={distributed_count})");
    Ok(())
}

/// Reserve two distinct localhost UDP addresses for the SWIM membership check.
fn reserve_swim_bind_addrs() -> Result<(String, String), Box<dyn std::error::Error>> {
    let bind1 = operator::reserve_local_udp_addr()?.to_string();
    let mut bind2 = operator::reserve_local_udp_addr()?.to_string();
    while bind2 == bind1 {
        bind2 = operator::reserve_local_udp_addr()?.to_string();
    }
    Ok((bind1, bind2))
}
