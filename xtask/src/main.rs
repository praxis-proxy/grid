//! Development task runner for the AI Grid workspace.
#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::exit,
    reason = "xtask is a CLI tool that prints to the terminal"
)]

mod env;

use clap::{Parser, Subcommand};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// AI Grid development tasks.
#[derive(Debug, Parser)]
#[command(name = "xtask", about = "AI Grid development tasks")]
struct Cli {
    /// Subcommand to run.
    #[command(subcommand)]
    command: Command,
}

/// Top-level subcommands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Manage the multi-cluster test environment.
    Env {
        /// Environment action to perform.
        #[command(subcommand)]
        action: env::Action,
    },
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Env { action } => env::run(&action),
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
