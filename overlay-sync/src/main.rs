// SPDX-License-Identifier: MIT

//! Grid overlay sync sidecar.
//!
//! Watches a single named `ConfigMap` through the Kubernetes API,
//! validates the content-addressed overlay envelope, and atomically
//! writes the latest valid overlay to a shared `emptyDir` volume
//! consumed by Praxis.

#![expect(
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    clippy::min_ident_chars,
    reason = "overlay-sync uses short closure params and index arithmetic pervasively"
)]

mod atomic_file;
mod metrics;
mod status;
mod types;
mod validation;
mod watcher;

use std::{net::SocketAddr, path::Path, sync::Arc};

use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use clap::Parser;

use crate::{metrics::Metrics, status::SharedStatus, validation::ExpectedScope, watcher::WatcherConfig};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// Grid overlay sync sidecar — watches a `ConfigMap` and atomically
/// publishes validated overlays.
#[derive(Parser, Debug)]
#[command(name = "grid-overlay-sync")]
struct Cli {
    /// Kubernetes namespace.
    #[arg(long, env = "OVERLAY_SYNC_NAMESPACE", default_value = "grid-system")]
    namespace: String,

    /// `ConfigMap` name to watch.
    #[arg(long, env = "OVERLAY_SYNC_CONFIGMAP")]
    config_map: String,

    /// Data key within the `ConfigMap`.
    #[arg(long, env = "OVERLAY_SYNC_DATA_KEY", default_value = "routing-overlay.json")]
    data_key: String,

    /// Output file path.
    #[arg(
        long,
        env = "OVERLAY_SYNC_OUTPUT",
        default_value = "/run/praxis-routing/routing-overlay.json"
    )]
    output: std::path::PathBuf,

    /// Expected network name for scope validation.
    #[arg(long, env = "OVERLAY_SYNC_EXPECTED_NETWORK")]
    expected_network: String,

    /// Expected gateway name for scope validation.
    #[arg(long, env = "OVERLAY_SYNC_EXPECTED_GATEWAY")]
    expected_gateway: String,

    /// Expected namespace for scope validation.
    #[arg(long, env = "OVERLAY_SYNC_EXPECTED_NAMESPACE")]
    expected_namespace: Option<String>,

    /// Expected local site for scope validation.
    #[arg(long, env = "OVERLAY_SYNC_EXPECTED_LOCAL_SITE")]
    expected_local_site: String,

    /// Maximum payload size in bytes.
    #[arg(long, env = "OVERLAY_SYNC_MAX_BYTES", default_value_t = 1_048_576)]
    max_bytes: usize,

    /// Health server listen address.
    #[arg(long, env = "OVERLAY_SYNC_HEALTH_ADDR", default_value = "0.0.0.0:9091")]
    health_addr: String,

    /// Fetch and publish one valid operator overlay, then exit.
    #[arg(long, env = "OVERLAY_SYNC_ONCE", default_value_t = false)]
    once: bool,
}

// ---------------------------------------------------------------------------
// Shared application state
// ---------------------------------------------------------------------------

/// Application state shared between the health server and watcher.
#[derive(Clone)]
struct AppState {
    /// Sidecar status.
    status: SharedStatus,
    /// Prometheus metrics.
    metrics: Arc<Metrics>,
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Top-level error for the sidecar binary.
#[derive(Debug)]
enum StartupError {
    /// Cannot create a Kubernetes client.
    KubeClient(kube::Error),
}

impl std::fmt::Display for StartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KubeClient(e) => write!(f, "cannot create Kubernetes client: {e}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), StartupError> {
    let cli = Cli::parse();
    init_tracing();
    run(cli).await
}

/// Run the sidecar in one-shot initialization or continuous watch mode.
async fn run(cli: Cli) -> Result<(), StartupError> {
    tracing::info!(
        namespace = %cli.namespace,
        config_map = %cli.config_map,
        data_key = %cli.data_key,
        output = %cli.output.display(),
        "grid-overlay-sync starting"
    );

    let (watcher_config, metrics, status) = build_config(&cli);
    ensure_output_dir(&cli.output);
    watcher::restore_last_known_good(&watcher_config, &status, &metrics);
    let client = kube::Client::try_default().await.map_err(StartupError::KubeClient)?;

    if cli.once {
        watcher::initial_fetch_until_ready(&client, &watcher_config, &status, &metrics).await;
        return Ok(());
    }

    run_continuous(cli.health_addr, client, watcher_config, status, metrics).await;
    Ok(())
}

/// Run the health endpoint and Kubernetes watcher until shutdown.
#[expect(
    clippy::cognitive_complexity,
    reason = "supervising the two long-running tasks and shutdown signal is intentionally centralized"
)]
async fn run_continuous(
    health_addr: String,
    client: kube::Client,
    watcher_config: WatcherConfig,
    status: SharedStatus,
    metrics: Arc<Metrics>,
) {
    watcher::initial_fetch(&client, &watcher_config, &status, &metrics).await;

    let app_state = AppState {
        status: status.clone(),
        metrics: Arc::clone(&metrics),
    };

    let health_handle = tokio::spawn(serve_health(health_addr, app_state));
    let watch_handle = tokio::spawn(async move {
        watcher::run_watch_loop(&client, &watcher_config, &status, &metrics).await;
    });

    tokio::select! {
        r = health_handle => { if let Err(e) = r { tracing::error!(error = %e, "health server failed"); } }
        r = watch_handle => { if let Err(e) = r { tracing::error!(error = %e, "watch loop failed"); } }
        _ = tokio::signal::ctrl_c() => { tracing::info!("received shutdown signal"); }
    }
}

// ---------------------------------------------------------------------------
// Initialization helpers
// ---------------------------------------------------------------------------

/// Initialize structured JSON tracing.
fn init_tracing() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

/// Build the watcher configuration, metrics, and status from CLI args.
fn build_config(cli: &Cli) -> (WatcherConfig, Arc<Metrics>, SharedStatus) {
    let expected_ns = cli.expected_namespace.clone().unwrap_or_else(|| cli.namespace.clone());
    let expected_scope = ExpectedScope {
        network: cli.expected_network.clone(),
        gateway: cli.expected_gateway.clone(),
        namespace: expected_ns,
        local_site: cli.expected_local_site.clone(),
    };
    let watcher_config = WatcherConfig {
        namespace: cli.namespace.clone(),
        config_map_name: cli.config_map.clone(),
        data_key: cli.data_key.clone(),
        output_path: cli.output.clone(),
        expected_scope,
        max_payload_bytes: cli.max_bytes,
    };
    let metrics = Arc::new(Metrics::new());
    let status = SharedStatus::new(&cli.namespace, &cli.config_map, &cli.data_key);
    (watcher_config, metrics, status)
}

/// Ensure the parent directory of the output path exists.
fn ensure_output_dir(output: &Path) {
    if let Some(parent) = output.parent()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        tracing::error!(path = %parent.display(), error = %e, "cannot create output directory");
    }
}

// ---------------------------------------------------------------------------
// Health server
// ---------------------------------------------------------------------------

/// Parse the listen address and bind the health server.
async fn serve_health(addr: String, state: AppState) {
    let Ok(parsed) = addr.parse::<SocketAddr>() else {
        tracing::error!(addr = %addr, "invalid health server address");
        return;
    };
    tracing::info!(addr = %parsed, "health server listening");
    bind_and_serve(parsed, state).await;
}

/// Bind a TCP listener and serve the health router.
async fn bind_and_serve(addr: SocketAddr, state: AppState) {
    let app = build_health_router(state);
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(addr = %addr, error = %e, "cannot bind health server");
            return;
        },
    };
    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!(error = %e, "health server error");
    }
}

/// Build the axum router for health endpoints.
fn build_health_router(state: AppState) -> Router {
    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/status", get(status_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

/// Liveness probe — always healthy if the process is running.
async fn livez() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Readiness probe — healthy after first valid overlay is written.
async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    if state.status.is_ready() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

/// Status endpoint — JSON with current sidecar state.
async fn status_handler(State(state): State<AppState>) -> impl IntoResponse {
    let response = state.status.to_response();
    (
        StatusCode::OK,
        [(http::header::CONTENT_TYPE, "application/json")],
        serde_json::to_string_pretty(&response).unwrap_or_default(),
    )
}

/// Prometheus metrics endpoint.
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let body = state.metrics.encode();
    (
        StatusCode::OK,
        [(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        body,
    )
}
