//! Multi-cluster test environment management.

pub(crate) mod certs;
pub(crate) mod config;
pub(crate) mod kind;
pub(crate) mod providers;
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

    /// Verify provider inference-sim endpoints are reachable.
    VerifyProviders {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
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
            for model in &def.models {
                let deploy_ok = kind::is_model_deployment_ready(name, model);
                all_ok = all_ok && deploy_ok;
                let deploy = kind::deployment_name(model);
                eprintln!("    {deploy}: {}", status_label(deploy_ok));
            }
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

/// Verify provider inference-sim endpoints.
fn env_verify_providers(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    verify::verify_providers(&cfg)
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
