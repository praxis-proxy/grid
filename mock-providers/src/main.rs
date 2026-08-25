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

use std::{path::PathBuf, sync::Arc, time::Duration};

use clap::Parser;
use mock_providers::{AppState, anthropic, bedrock, openai, vertex};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _};
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
        required_unless_present_any = ["tcp_probe", "http_probe", "tls_probe_server", "mcp_server"],
        conflicts_with_all = ["tcp_probe", "http_probe", "tls_probe_server", "mcp_server"]
    )]
    provider: Option<ProviderKind>,

    /// Port to listen on.
    #[arg(long, default_value = "8080")]
    port: u16,

    /// Run a mock MCP (Model Context Protocol) `tools/list` server instead
    /// of an AI provider mock.
    #[arg(long, conflicts_with_all = ["provider", "tcp_probe", "http_probe", "tls_probe_server"])]
    mcp_server: bool,

    /// Comma-separated tool names the MCP mock reports from `tools/list`.
    #[arg(long, default_value = "search", requires = "mcp_server")]
    mcp_tools: String,

    /// If set, the MCP mock rejects `tools/list` calls whose bearer token
    /// does not match this value.
    #[arg(long, requires = "mcp_server")]
    mcp_bearer: Option<String>,

    /// Run a TLS-only probe server that accepts mTLS connections.
    #[arg(
        long,
        conflicts_with_all = ["provider", "tcp_probe", "http_probe", "mcp_server"],
        requires_all = ["tls_cert", "tls_key", "tls_ca"]
    )]
    tls_probe_server: bool,

    /// PEM certificate chain file for TLS probe server.
    #[arg(long, requires = "tls_probe_server")]
    tls_cert: Option<PathBuf>,

    /// PEM private key file for TLS probe server.
    #[arg(long, requires = "tls_probe_server")]
    tls_key: Option<PathBuf>,

    /// PEM CA certificate file for client verification.
    #[arg(long, requires = "tls_probe_server")]
    tls_ca: Option<PathBuf>,

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

#[expect(
    clippy::too_many_lines,
    reason = "CLI dispatch: TLS probe, one-shot probes, or provider server"
)]
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    if cli.tls_probe_server {
        let cert_path = cli.tls_cert.as_deref().unwrap_or_else(|| std::process::exit(2));
        let key_path = cli.tls_key.as_deref().unwrap_or_else(|| std::process::exit(2));
        let ca_path = cli.tls_ca.as_deref().unwrap_or_else(|| std::process::exit(2));
        run_tls_probe_server(cli.port, cert_path, key_path, ca_path).await;
        return;
    }

    if run_selected_probe(&cli).await {
        return;
    }

    if cli.mcp_server {
        run_mcp_server(cli.port, &cli.mcp_tools, cli.mcp_bearer.as_deref()).await;
        return;
    }

    let Some(provider) = cli.provider else {
        eprintln!("either --provider, --mcp-server, --tcp-probe, or --http-probe is required");
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

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap_or_else(|err| {
        eprintln!("failed to bind {addr}: {err}");
        std::process::exit(1);
    });

    axum::serve(listener, router).await.unwrap_or_else(|err| {
        eprintln!("server error: {err}");
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

/// Run a TLS-only probe server that accepts mTLS connections.
///
/// Loads the certificate chain, private key, and CA from PEM files, builds a
/// `rustls` server configuration with mutual TLS client verification, and
/// accepts connections in a loop. Each accepted connection completes the TLS
/// handshake and is then dropped — no HTTP is served.
#[expect(
    clippy::too_many_lines,
    reason = "linear TLS config: load PEM → build ServerConfig → accept loop"
)]
#[expect(clippy::infinite_loop, reason = "server runs forever until killed by parent process")]
async fn run_tls_probe_server(
    port: u16,
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
    ca_path: &std::path::Path,
) {
    let cert_pem = std::fs::read(cert_path).unwrap_or_else(|err| {
        eprintln!("tls-probe-server: failed to read cert {}: {err}", cert_path.display());
        std::process::exit(1);
    });
    let key_pem = std::fs::read(key_path).unwrap_or_else(|err| {
        eprintln!("tls-probe-server: failed to read key {}: {err}", key_path.display());
        std::process::exit(1);
    });
    let ca_pem = std::fs::read(ca_path).unwrap_or_else(|err| {
        eprintln!("tls-probe-server: failed to read CA {}: {err}", ca_path.display());
        std::process::exit(1);
    });

    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|err| {
            eprintln!("tls-probe-server: invalid cert PEM: {err}");
            std::process::exit(1);
        });
    let key = PrivateKeyDer::from_pem_slice(&key_pem).unwrap_or_else(|err| {
        eprintln!("tls-probe-server: invalid key PEM: {err}");
        std::process::exit(1);
    });

    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(&ca_pem) {
        let cert = cert.unwrap_or_else(|err| {
            eprintln!("tls-probe-server: invalid CA PEM: {err}");
            std::process::exit(1);
        });
        roots.add(cert).unwrap_or_else(|err| {
            eprintln!("tls-probe-server: failed to add CA cert: {err}");
            std::process::exit(1);
        });
    }

    let provider = rustls::crypto::ring::default_provider();
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .unwrap_or_else(|err| {
            eprintln!("tls-probe-server: failed to build client verifier: {err}");
            std::process::exit(1);
        });
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .unwrap_or_else(|err| {
            eprintln!("tls-probe-server: failed to build TLS config: {err}");
            std::process::exit(1);
        })
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .unwrap_or_else(|err| {
            eprintln!("tls-probe-server: failed to set cert/key: {err}");
            std::process::exit(1);
        });

    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap_or_else(|err| {
        eprintln!("tls-probe-server: failed to bind {addr}: {err}");
        std::process::exit(1);
    });
    let local_port = listener.local_addr().map_or(port, |local| local.port());
    eprintln!("tls-probe-server=listening port={local_port}");

    loop {
        let Ok((stream, _peer)) = listener.accept().await else {
            continue;
        };
        let acc = acceptor.clone();
        tokio::spawn(async move {
            drop(acc.accept(stream).await);
        });
    }
}

/// Run the mock MCP `tools/list` server (see `mock_providers::mcp`).
async fn run_mcp_server(port: u16, tools_csv: &str, required_bearer: Option<&str>) {
    let tools: Vec<String> = tools_csv
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
        .collect();
    let router = mock_providers::mcp::router(tools, required_bearer.map(str::to_owned));

    let addr = format!("0.0.0.0:{port}");
    eprintln!("mock-mcp-server listening on {addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap_or_else(|error| {
        eprintln!("failed to bind {addr}: {error}");
        std::process::exit(1);
    });

    axum::serve(listener, router).await.unwrap_or_else(|error| {
        eprintln!("server error: {error}");
        std::process::exit(1);
    });
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

    #[expect(clippy::float_cmp, reason = "exact equality valid for parsed f64 test literals")]
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
        assert!(result.is_err(), "provider and tcp-probe must conflict");
    }

    #[test]
    fn one_mode_is_required() {
        assert!(
            Cli::try_parse_from(["mock-providers"]).is_err(),
            "at least one mode must be specified"
        );
    }

    #[test]
    fn mcp_server_mode_parses_without_provider() {
        let cli = Cli::try_parse_from([
            "mock-providers",
            "--mcp-server",
            "--port",
            "9091",
            "--mcp-tools",
            "search,read_file",
            "--mcp-bearer",
            "s3cr3t",
        ])
        .unwrap_or_else(|_| std::process::abort());
        assert!(cli.provider.is_none());
        assert!(cli.mcp_server);
        assert_eq!(cli.mcp_tools, "search,read_file");
        assert_eq!(cli.mcp_bearer.as_deref(), Some("s3cr3t"));
    }

    #[test]
    fn mcp_server_conflicts_with_provider() {
        let result = Cli::try_parse_from(["mock-providers", "--provider", "openai", "--mcp-server"]);
        assert!(result.is_err(), "--mcp-server must conflict with --provider");
    }

    #[test]
    fn mcp_tools_defaults_to_search_when_unset() {
        let cli = Cli::try_parse_from(["mock-providers", "--mcp-server"]).unwrap_or_else(|_| std::process::abort());
        assert_eq!(cli.mcp_tools, "search");
        assert!(cli.mcp_bearer.is_none());
    }

    #[test]
    fn tls_probe_server_mode_parses_without_provider() {
        let cli = Cli::try_parse_from([
            "mock-providers",
            "--tls-probe-server",
            "--tls-cert",
            "/tmp/cert.pem",
            "--tls-key",
            "/tmp/key.pem",
            "--tls-ca",
            "/tmp/ca.pem",
            "--port",
            "9443",
        ])
        .unwrap_or_else(|_| std::process::abort());
        assert!(cli.provider.is_none());
        assert!(cli.tls_probe_server);
        assert_eq!(cli.port, 9443);
    }

    #[test]
    fn tls_probe_server_requires_cert_paths() {
        let result = Cli::try_parse_from(["mock-providers", "--tls-probe-server"]);
        assert!(result.is_err(), "tls-probe-server without cert paths must fail");
    }

    #[test]
    fn tls_probe_server_conflicts_with_provider() {
        let result = Cli::try_parse_from([
            "mock-providers",
            "--provider",
            "openai",
            "--tls-probe-server",
            "--tls-cert",
            "/tmp/c.pem",
            "--tls-key",
            "/tmp/k.pem",
            "--tls-ca",
            "/tmp/ca.pem",
        ]);
        assert!(result.is_err(), "tls-probe-server must conflict with --provider");
    }

    #[test]
    fn http_probe_rejects_header_injection() {
        assert!(!valid_probe_input(
            "mock-inference:8080\r\nX-Bad: yes",
            Duration::from_secs(1)
        ));
    }
}
