//! Development task runner for the AI Grid workspace.
#![expect(
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::exit,
    reason = "xtask is a CLI tool that prints to the terminal"
)]
#![expect(
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    clippy::min_ident_chars,
    reason = "xtask config generators use short closure params, port arithmetic, and index casts pervasively"
)]

mod api_codegen;
mod env;

use clap::{Parser, Subcommand};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// AI Grid development tasks.
#[derive(Debug, Parser)]
#[command(name = "xtask", about = "AI Grid development tasks")]
pub(crate) struct Cli {
    /// Subcommand to run.
    #[command(subcommand)]
    command: Command,
}

/// Top-level subcommands.
#[derive(Debug, Subcommand)]
#[expect(
    clippy::large_enum_variant,
    reason = "top-level CLI enum parsed once; variant size does not matter"
)]
enum Command {
    /// Manage the multi-cluster test environment.
    Env {
        /// Environment action to perform.
        #[command(subcommand)]
        action: env::Action,
    },

    /// Generate the enrollment wire types from the `OpenAPI` spec.
    GenerateApiTypes,

    /// Verify the generated enrollment wire types match the spec.
    CheckApiTypes,
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Env { action } => env::run(&action),
        Command::GenerateApiTypes => api_codegen::generate(),
        Command::CheckApiTypes => api_codegen::check(),
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
