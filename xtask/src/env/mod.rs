//! Multi-cluster test environment management.

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
    eprintln!("env up: clusters and providers ready");
    Ok(())
}

/// Tear down the test environment.
fn env_down(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;

    for name in &cfg.clusters.names {
        kind::delete_cluster(name)?;
    }

    providers::stop_all()?;
    eprintln!("env down: clusters and providers removed");
    Ok(())
}

/// Report the status of all environment components.
fn env_status(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;

    for name in &cfg.clusters.names {
        let running = kind::is_cluster_running(name);
        let status = if running { "running" } else { "stopped" };
        eprintln!("  cluster grid-{name}: {status}");
    }

    for provider in &["openai", "anthropic", "bedrock", "vertex"] {
        let running = providers::is_running(provider);
        let status = if running { "running" } else { "stopped" };
        eprintln!("  mock-{provider}: {status}");
    }
    Ok(())
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
