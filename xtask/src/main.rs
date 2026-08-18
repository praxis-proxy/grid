//! Development task runner for the AI Grid workspace.
#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::exit,
    reason = "xtask is a CLI tool that prints to the terminal"
)]

mod env;
mod lint_extended;

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
enum Command {
    /// Manage the multi-cluster test environment.
    Env {
        /// Environment action to perform.
        #[command(subcommand)]
        action: env::Action,
    },

    /// Diff-scoped heuristic checks for common low-quality-code patterns.
    ///
    /// Flags leftover work-marker comments and commented-out code
    /// (blocking), plus narrating comments, repeated literals, weak
    /// identifier names, and new clippy suppressions (warnings only), scoped
    /// to lines added/changed versus a diff base. See [`lint_extended`] for
    /// the full check descriptions and diff-base resolution order.
    LintExtended {
        /// Git diff base ref or SHA to scope the check against.
        ///
        /// Falls back to `$EXTENDED_LINT_BASE`, then `origin/$GITHUB_BASE_REF`
        /// inside a GitHub Actions PR, then `origin/main`.
        #[arg(long)]
        base: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Env { action } => env::run(&action),
        Command::LintExtended { base } => lint_extended::run(base.as_deref()).map(|clean| {
            if !clean {
                std::process::exit(1);
            }
        }),
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
