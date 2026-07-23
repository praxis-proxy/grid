//! Command-line interface definition using `clap`.
//!
//! Defines the global options and subcommand tree for `praxis-forge`.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::{config::RuntimeProvider, output::OutputFormat};

/// Generic development-environment orchestrator for Kubernetes.
#[derive(Debug, Parser)]
#[command(name = "praxis-forge", version, about)]
pub struct Cli {
    /// Global options shared by all subcommands.
    #[command(flatten)]
    pub global: GlobalOptions,
    /// Subcommand to execute.
    #[command(subcommand)]
    pub command: Command,
}

/// Global options available to every subcommand.
#[derive(Debug, Parser)]
pub struct GlobalOptions {
    /// Path to the Forge configuration file.
    #[arg(long, env = "FORGE_CONFIG", default_value = "forge.yaml", global = true)]
    pub config: PathBuf,

    /// Directory for Forge state files.
    #[arg(long, env = "FORGE_STATE_DIR", default_value = ".forge", global = true)]
    pub state_dir: PathBuf,

    /// Container runtime to use.
    #[arg(long, default_value = "auto", global = true)]
    pub runtime: RuntimeProvider,

    /// Log level (e.g. `info`, `debug`, `trace`).
    #[arg(long, env = "FORGE_LOG", default_value = "info", global = true)]
    pub log: String,

    /// Output format.
    #[arg(long, default_value = "text", global = true)]
    pub output: OutputFormat,

    /// Show what would happen without making changes.
    #[arg(long, global = true)]
    pub dry_run: bool,

    /// Suppress interactive prompts.
    #[arg(long, global = true)]
    pub non_interactive: bool,
}

/// Top-level subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Check availability of required external tools.
    Doctor,
    /// Show what the environment would look like.
    Plan,
    /// Configuration management subcommands.
    #[command(subcommand)]
    Config(ConfigCommand),
}

/// Configuration subcommands.
#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Validate the configuration file.
    Validate,
    /// Show the parsed configuration.
    Show {
        /// Show the fully resolved configuration (no-op in F1).
        #[arg(long)]
        resolved: bool,
    },
    /// Create a minimal configuration file.
    Init {
        /// Overwrite an existing configuration file.
        #[arg(long)]
        force: bool,
    },
    /// Emit the JSON Schema for the configuration format.
    Schema,
}

// -----------------------------------------------------------------
// Trait impls for clap integration
// -----------------------------------------------------------------

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Text => f.write_str("text"),
            Self::Json => f.write_str("json"),
        }
    }
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            other => Err(format!("unknown output format {other:?} (expected text or json)")),
        }
    }
}

impl std::fmt::Display for RuntimeProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => f.write_str("auto"),
            Self::Docker => f.write_str("docker"),
            Self::Podman => f.write_str("podman"),
        }
    }
}

impl std::str::FromStr for RuntimeProvider {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "auto" => Ok(Self::Auto),
            "docker" => Ok(Self::Docker),
            "podman" => Ok(Self::Podman),
            other => Err(format!("unknown runtime {other:?} (expected auto, docker, or podman)")),
        }
    }
}
