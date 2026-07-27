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

use std::{sync::Arc, time::Duration};

use clap::Parser;
use mock_providers::{AppState, anthropic, bedrock, openai, vertex};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// Mock AI provider server for integration testing.
#[derive(Debug, Parser)]
#[command(name = "mock-providers")]
struct Cli {
    /// Which provider API to simulate.
    #[arg(
        short,
        long,
        required_unless_present_any = ["tcp_probe", "http_probe"],
        conflicts_with_all = ["tcp_probe", "http_probe"]
    )]
    provider: Option<ProviderKind>,

    /// Port to listen on.
    #[arg(long, default_value = "8080")]
    port: u16,

    /// Run one bounded TCP connectivity probe and exit.
    #[arg(long, value_name = "HOST:PORT", conflicts_with = "provider")]
    tcp_probe: Option<String>,

    /// TCP probe timeout in milliseconds.
    #[arg(long, default_value_t = 2_000, requires = "tcp_probe")]
    tcp_probe_timeout_ms: u64,

    /// Send one bounded OpenAI-compatible HTTP authentication probe and exit.
    #[arg(
        long,
        value_name = "HOST:PORT",
        conflicts_with_all = ["provider", "tcp_probe"]
    )]
    http_probe: Option<String>,

    /// Optional Authorization value for the HTTP probe.
    #[arg(long, requires = "http_probe")]
    http_probe_authorization: Option<String>,

    /// HTTP probe timeout in milliseconds.
    #[arg(long, default_value_t = 2_000, requires = "http_probe")]
    http_probe_timeout_ms: u64,
}

/// Supported provider kinds.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum ProviderKind {
    /// `OpenAI` chat completions API.
    Openai,

    /// `Anthropic` Messages API.
    Anthropic,

    /// AWS `Bedrock` Converse API.
    Bedrock,

    /// Google `Vertex` AI `generateContent` API.
    Vertex,
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    if run_selected_probe(&cli).await {
        return;
    }

    let Some(provider) = cli.provider else {
        eprintln!("either --provider, --tcp-probe, or --http-probe is required");
        std::process::exit(2);
    };
    let state = app_state();
    let router = match provider {
        ProviderKind::Openai => openai::router(state),
        ProviderKind::Anthropic => anthropic::router(state),
        ProviderKind::Bedrock => bedrock::router(state),
        ProviderKind::Vertex => vertex::router(state),
    };

    let addr = format!("0.0.0.0:{}", cli.port);
    eprintln!("mock-{provider:?} listening on {addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap_or_else(|e| {
        eprintln!("failed to bind {addr}: {e}");
        std::process::exit(1);
    });

    axum::serve(listener, router).await.unwrap_or_else(|e| {
        eprintln!("server error: {e}");
        std::process::exit(1);
    });
}

/// Build server state from bounded demo environment values.
fn app_state() -> AppState {
    let provider_site = std::env::var("MOCK_PROVIDER_SITE").unwrap_or_else(|_| "unknown".to_owned());
    let queue_depth_env = std::env::var("MOCK_QUEUE_DEPTH").ok();
    let queue_depth = parse_queue_depth(queue_depth_env.as_deref());
    AppState {
        provider_site: Arc::<str>::from(provider_site),
        queue_depth,
    }
}

/// Parse a normalized queue-depth metric, falling back to a ready provider.
fn parse_queue_depth(value: Option<&str>) -> f64 {
    value
        .and_then(|candidate| candidate.parse::<f64>().ok())
        .filter(|candidate| candidate.is_finite() && (0.0..=1.0).contains(candidate))
        .unwrap_or(0.1)
}

/// Run the selected one-shot probe, returning whether server startup should stop.
async fn run_selected_probe(cli: &Cli) -> bool {
    if let Some(target) = cli.tcp_probe.as_deref() {
        run_tcp_probe(target, Duration::from_millis(cli.tcp_probe_timeout_ms)).await;
        return true;
    }
    if let Some(target) = cli.http_probe.as_deref() {
        run_http_probe(
            target,
            cli.http_probe_authorization.as_deref(),
            Duration::from_millis(cli.http_probe_timeout_ms),
        )
        .await;
        return true;
    }
    false
}

/// Run a single TCP probe for `NetworkPolicy` verification.
async fn run_tcp_probe(target: &str, timeout: Duration) {
    if target.is_empty() || target.len() > 512 || timeout.is_zero() || timeout > Duration::from_secs(30) {
        eprintln!("tcp-probe=invalid");
        std::process::exit(2);
    }

    match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(target)).await {
        Ok(Ok(_stream)) => println!("tcp-probe=connected target={target}"),
        Ok(Err(_error)) => {
            eprintln!("tcp-probe=connect-failed target={target}");
            std::process::exit(3);
        },
        Err(_elapsed) => {
            eprintln!("tcp-probe=timeout target={target}");
            std::process::exit(4);
        },
    }
}

/// Run one bounded HTTP probe without printing credential material.
async fn run_http_probe(target: &str, authorization: Option<&str>, timeout: Duration) {
    if !valid_probe_input(target, timeout)
        || authorization.is_some_and(|value| value.len() > 512 || value.contains(['\r', '\n']))
    {
        eprintln!("http-probe=invalid");
        std::process::exit(2);
    }

    let result = tokio::time::timeout(timeout, send_http_probe(target, authorization)).await;
    match result {
        Ok(Ok(status)) => println!("http-probe=status status={status}"),
        Ok(Err(_error)) => {
            eprintln!("http-probe=request-failed");
            std::process::exit(3);
        },
        Err(_elapsed) => {
            eprintln!("http-probe=timeout");
            std::process::exit(4);
        },
    }
}

/// Validate common probe bounds.
fn valid_probe_input(target: &str, timeout: Duration) -> bool {
    !target.is_empty()
        && target.len() <= 512
        && !target.contains(['\r', '\n'])
        && !timeout.is_zero()
        && timeout <= Duration::from_secs(30)
}

/// Send the fixed HTTP request and return its response status.
async fn send_http_probe(target: &str, authorization: Option<&str>) -> Result<u16, Box<dyn std::error::Error>> {
    let mut stream = tokio::net::TcpStream::connect(target).await?;
    let body = r#"{"model":"sim-model-v1","messages":[{"role":"user","content":"auth probe"}]}"#;
    let authorization = authorization.map_or_else(String::new, |value| format!("Authorization: {value}\r\n"));
    let request = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: {target}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n{authorization}\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await?;

    let mut response = Vec::with_capacity(4096);
    stream.take(16_384).read_to_end(&mut response).await?;
    let status = String::from_utf8(response)?
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("HTTP probe response has no status")?
        .parse()?;
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_depth_accepts_only_normalized_finite_values() {
        assert_eq!(parse_queue_depth(Some("0.95")), 0.95);
        for invalid in [None, Some("-0.1"), Some("1.1"), Some("NaN"), Some("invalid")] {
            assert_eq!(parse_queue_depth(invalid), 0.1);
        }
    }

    #[test]
    fn provider_mode_parses() {
        let cli = Cli::try_parse_from(["mock-providers", "--provider", "openai", "--port", "9090"])
            .unwrap_or_else(|_| std::process::abort());
        assert!(matches!(cli.provider, Some(ProviderKind::Openai)));
        assert_eq!(cli.port, 9090);
        assert!(cli.tcp_probe.is_none());
    }

    #[test]
    fn tcp_probe_mode_parses_without_provider() {
        let cli = Cli::try_parse_from([
            "mock-providers",
            "--tcp-probe",
            "mock-inference.grid-system.svc:8080",
            "--tcp-probe-timeout-ms",
            "1500",
        ])
        .unwrap_or_else(|_| std::process::abort());
        assert!(cli.provider.is_none());
        assert_eq!(cli.tcp_probe.as_deref(), Some("mock-inference.grid-system.svc:8080"));
        assert_eq!(cli.tcp_probe_timeout_ms, 1500);
        assert!(cli.http_probe.is_none());
    }

    #[test]
    fn http_probe_mode_parses_without_provider() {
        let cli = Cli::try_parse_from([
            "mock-providers",
            "--http-probe",
            "mock-inference.grid-system.svc:8080",
            "--http-probe-authorization",
            "Bearer test-token",
        ])
        .unwrap_or_else(|_| std::process::abort());
        assert!(cli.provider.is_none());
        assert_eq!(cli.http_probe.as_deref(), Some("mock-inference.grid-system.svc:8080"));
        assert_eq!(cli.http_probe_authorization.as_deref(), Some("Bearer test-token"));
        assert!(cli.tcp_probe.is_none());
    }

    #[test]
    fn provider_and_tcp_probe_conflict() {
        let result = Cli::try_parse_from([
            "mock-providers",
            "--provider",
            "openai",
            "--tcp-probe",
            "mock-inference:8080",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn one_mode_is_required() {
        assert!(Cli::try_parse_from(["mock-providers"]).is_err());
    }

    #[test]
    fn http_probe_rejects_header_injection() {
        assert!(!valid_probe_input(
            "mock-inference:8080\r\nX-Bad: yes",
            Duration::from_secs(1)
        ));
    }
}
