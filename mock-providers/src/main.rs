//! Mock AI provider servers for integration testing.
//!
//! A single binary that runs one of four provider mocks based on
//! the `--provider` CLI argument: `openai`, `anthropic`, `bedrock`,
//! or `vertex`.
#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::exit,
    reason = "CLI binary that prints to the terminal"
)]

mod common;
mod openai;

use clap::Parser;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// Mock AI provider server for integration testing.
#[derive(Debug, Parser)]
#[command(name = "mock-providers")]
struct Cli {
    /// Which provider API to simulate.
    #[arg(short, long)]
    provider: ProviderKind,

    /// Port to listen on.
    #[arg(long, default_value = "8080")]
    port: u16,
}

/// Supported provider kinds.
#[derive(Debug, Clone, clap::ValueEnum)]
enum ProviderKind {
    /// `OpenAI` chat completions API.
    Openai,
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let router = match cli.provider {
        ProviderKind::Openai => openai::router(),
    };

    let addr = format!("0.0.0.0:{}", cli.port);
    eprintln!("mock-{:?} listening on {addr}", cli.provider);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap_or_else(|e| {
        eprintln!("failed to bind {addr}: {e}");
        std::process::exit(1);
    });

    axum::serve(listener, router).await.unwrap_or_else(|e| {
        eprintln!("server error: {e}");
        std::process::exit(1);
    });
}
