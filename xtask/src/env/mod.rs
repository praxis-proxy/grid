//! Multi-cluster test environment management.

pub(crate) mod certs;
pub(crate) mod config;
pub(crate) mod kind;
pub(crate) mod providers;

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

    providers::start_all(&cfg.providers)?;
    certs::generate_all(&cfg.clusters.names)?;
    eprintln!("env up: clusters, providers, and certs ready");
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

    eprintln!("clusters:");
    for name in &cfg.clusters.names {
        let ok = kind::is_cluster_running(name);
        all_ok = all_ok && ok;
        let label = status_label(ok);
        eprintln!("  grid-{name}: {label}");
    }

    eprintln!("providers:");
    for provider in &["openai", "anthropic", "bedrock", "vertex"] {
        let ok = providers::is_running(provider);
        all_ok = all_ok && ok;
        let label = status_label(ok);
        eprintln!("  mock-{provider}: {label}");
    }

    eprintln!("certificates:");
    let certs_ok = certs::certs_exist();
    all_ok = all_ok && certs_ok;
    eprintln!("  CA + site certs: {}", status_label(certs_ok));

    if all_ok {
        eprintln!("status: all components healthy");
    } else {
        eprintln!("status: some components not ready");
    }

    Ok(())
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
