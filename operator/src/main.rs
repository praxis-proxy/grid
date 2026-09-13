//! AI Grid operator binary.
//!
//! Runs Kubernetes controllers for [`GridNetwork`], [`GridSite`], and
//! [`InferenceProvider`] resources, and optionally starts a live SWIM
//! membership runtime for peer-to-peer mesh formation.
//!
//! # SWIM configuration
//!
//! Set `GRID_SWIM_BIND_ADDR` (e.g. `"0.0.0.0:7946"`) to enable the SWIM
//! runtime. Set `GRID_SWIM_ADVERTISE_ADDR` when the bind address is not
//! directly reachable by peers, and set `GRID_SWIM_SEEDS` to a comma-separated
//! list of `host:port` seed endpoints. Both literal IP addresses and DNS
//! hostnames are accepted; DNS is resolved once, with a bounded timeout, before
//! the runtime starts. When `GRID_SWIM_BIND_ADDR` is absent
//! the operator runs in static mode (`membership = None`);
//! `GridNetwork.status.connectedSites` and `distributedProviderCount` remain
//! 0, and the phase stays `Pending`/`Initializing` based on TLS configuration
//! only.
//!
//! # SWIM encryption (environment variable)
//!
//! Set `GRID_SWIM_ENCRYPT_KEY` to a 64-character lowercase hex string (32 bytes)
//! to enable AES-256-GCM encryption for all SWIM gossip packets.  When set,
//! packets from peers without the same key are silently dropped.
//!
//! This is the environment-variable path, intended for local development and
//! Kind-based testing.  Environment variables are visible to same-host process
//! inspectors, so the production configuration path uses
//! `GridNetwork.spec.tls.swimKeyRef` to source the key from a Kubernetes
//! Secret; the `GridNetwork` controller loads it and calls
//! `SwimHandle::set_swim_key` at reconcile time.
//!
//! The key value is **never** written to logs or tracing spans.
//!
//! [`GridNetwork`]: operator::crd::grid_network::GridNetwork
//! [`GridSite`]: operator::crd::grid_site::GridSite
//! [`InferenceProvider`]: operator::crd::inference_provider::InferenceProvider

#![deny(unsafe_code)]
#![expect(
    clippy::arithmetic_side_effects,
    clippy::min_ident_chars,
    reason = "operator uses short closure params and index arithmetic pervasively"
)]

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::response::IntoResponse as _;
use clap::Parser as _;
use futures::StreamExt as _;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
    Api, Client,
    api::{ObjectMeta, PostParams},
    runtime::{controller::Controller, watcher},
};
use operator::{
    cli::Cli,
    controller::{
        agent_tool_provider,
        grid_network::{self, OperatorCtx},
        grid_site, inference_provider,
    },
    crd::{
        agent_tool_provider::AgentToolProvider, grid_network::GridNetwork, grid_site::GridSite,
        inference_provider::InferenceProvider,
    },
    gateway,
    swim_endpoint::{SwimEndpoint, resolve_endpoint, resolve_endpoint_list_partial},
    swim_runtime::{self, RevisionLease, SwimConfig},
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
#[expect(
    clippy::large_stack_frames,
    clippy::too_many_lines,
    reason = "top-level binary with tokio runtime; startup sequence (crypto provider, CLI parsing, \
              SWIM bootstrap, controller fan-out) reads clearer sequential than split further"
)]
async fn main() {
    tracing_subscriber::fmt::init();
    tracing::info!("starting grid-operator");

    // Explicit process-wide rustls crypto-provider choice.
    //
    // `InferenceProvider`'s probe (`inference_provider.rs`) builds a
    // `hyper-rustls` client via `.with_native_roots()`, which relies on
    // `rustls` auto-detecting a single process-wide `CryptoProvider`. The
    // `AgentToolProvider` MCP probe (PR 2 of grid#41) links in `reqwest`
    // (via `rmcp`'s reqwest-backed transport) using its `rustls-no-provider`
    // feature specifically to avoid pulling in `aws-lc-rs` alongside `ring`
    // (see the workspace `Cargo.toml` comment on the `reqwest`/`rmcp`
    // entries) — but that feature means `reqwest` will no longer install a
    // default provider on our behalf either, so `Client::builder().build()`
    // panics with "No rustls crypto provider is configured" unless one is
    // installed explicitly first. Installing `ring` here, once, up front,
    // covers both `hyper-rustls` and `reqwest` for every reconciler in this
    // binary, regardless of which one runs first.
    if rustls::crypto::ring::default_provider().install_default().is_err() {
        tracing::warn!("rustls default CryptoProvider already installed; continuing");
    }

    let config = Cli::parse();

    let client = match Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to create kube client");
            return;
        },
    };

    let swim = match maybe_start_swim(&client, &config.gateway).await {
        Ok(swim) => swim,
        Err(error) => {
            tracing::error!(%error, "SWIM startup failed");
            std::process::exit(1);
        },
    };

    if let Some(handle) = &swim {
        tokio::spawn(gateway::run_discovery_poller(
            client.clone(),
            Arc::clone(handle),
            config.gateway.clone(),
        ));
    }

    let signals_enabled = config.polling_metrics_signals;
    let swim_for_poller = swim.clone();
    let ctx = Arc::new(OperatorCtx::new(client.clone(), swim));

    // A round of peer polls can be in flight when the pod is told to terminate;
    // the trigger lets it stand down cleanly rather than being dropped mid-await.
    // Only installed with the feature, so the default path keeps today's signal
    // disposition unchanged.
    let (trigger, shutdown) = operator::shutdown::Trigger::new();
    if signals_enabled {
        tokio::spawn(watch_for_termination(trigger));
    } else {
        drop(trigger);
    }

    let result = tokio::try_join!(
        run_network_controller(client.clone(), Arc::clone(&ctx)),
        run_site_controller(client.clone()),
        run_provider_controller(client.clone()),
        run_agent_tool_provider_controller(client.clone()),
        run_metrics_server(),
        run_signals_server(
            signals_enabled,
            client.clone(),
            Published {
                site: ctx.signals(),
                peers: ctx.peers(),
                local_labels: Arc::new(grid_network::local_site_labels()),
            },
            ctx.peer_identities(),
        ),
        run_peer_poller(
            signals_enabled,
            Arc::clone(&ctx),
            swim_for_poller,
            client.clone(),
            shutdown.clone(),
        ),
        run_local_scraper(signals_enabled, Arc::clone(&ctx), client.clone()),
    );

    if let Err(e) = result {
        tracing::error!(error = %e, "controller error");
    }
}

// ---------------------------------------------------------------------------
// Hostname helper
// ---------------------------------------------------------------------------

/// Optionally start the SWIM runtime from environment variables.
///
/// Returns `Some(handle)` if `GRID_SWIM_BIND_ADDR` is set and the runtime
/// starts successfully, `None` when SWIM is not configured or a non-contract
/// startup dependency is unavailable, and an error when an explicitly
/// configured bind or advertise endpoint is invalid or cannot be resolved.
///
/// Gateway address resolution uses [`operator::gateway::resolve`]:
/// `GRID_GATEWAY_ADDRESS` env var wins; otherwise the operator discovers
/// its own provider gateway Service `LoadBalancer` IP from Kubernetes.
#[expect(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::large_stack_frames,
    reason = "sequential env-var parsing + runtime startup; splitting would obscure the startup sequence"
)]
async fn maybe_start_swim(
    client: &Client,
    config: &gateway::Config,
) -> Result<Option<Arc<swim_runtime::SwimHandle>>, String> {
    let Some(addr_str) = std::env::var("GRID_SWIM_BIND_ADDR").ok() else {
        return Ok(None);
    };
    let bind_addr = match addr_str.parse() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(addr = %addr_str, error = %e, "GRID_SWIM_BIND_ADDR not a valid socket address");
            return Err(format!("GRID_SWIM_BIND_ADDR is not a valid socket address: {e}"));
        },
    };
    let advertise_addr = match std::env::var("GRID_SWIM_ADVERTISE_ADDR") {
        Ok(value) => {
            let endpoint = match value.parse::<SwimEndpoint>() {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    tracing::error!(env = "GRID_SWIM_ADVERTISE_ADDR", value = %value, %error, "invalid SWIM endpoint");
                    return Err(format!("GRID_SWIM_ADVERTISE_ADDR is invalid: {error}"));
                },
            };
            match resolve_endpoint(&endpoint, "GRID_SWIM_ADVERTISE_ADDR").await {
                Ok(addresses) => {
                    let resolved = addresses.first().copied();
                    if let Some(address) = resolved {
                        tracing::info!(configured = %endpoint.as_text(), %address, "resolved SWIM advertise endpoint");
                    }
                    resolved
                },
                Err(error) => return Err(format!("cannot resolve GRID_SWIM_ADVERTISE_ADDR: {error}")),
            }
        },
        Err(_) => None,
    };
    let seed_values = std::env::var("GRID_SWIM_SEEDS").unwrap_or_default();
    let seed_values: Vec<String> = seed_values.split(',').map(str::to_owned).collect();
    let seed_resolution = resolve_endpoint_list_partial(&seed_values, "GRID_SWIM_SEEDS").await;
    for failure in &seed_resolution.failures {
        tracing::warn!(
            source = %failure.source,
            endpoint = %failure.endpoint,
            reason = %failure.reason,
            "ignoring unusable SWIM seed"
        );
    }
    if seed_resolution.configured && seed_resolution.addresses.is_empty() {
        tracing::warn!(
            source = "GRID_SWIM_SEEDS",
            failures = seed_resolution.failures.len(),
            "no configured SWIM seeds resolved; keeping SWIM active with a seedless bootstrap"
        );
    }
    let seeds = seed_resolution.addresses;
    let site_name = std::env::var("GRID_SWIM_SITE_NAME").unwrap_or_else(|_| hostname_or_default());
    let gateway_address = match gateway::resolve(client, config).await {
        Ok(addr) => addr,
        Err(e) => {
            tracing::error!(error = %e, "gateway address discovery failed; continuing without");
            None
        },
    };
    let swim_key = parse_swim_key_env("GRID_SWIM_ENCRYPT_KEY");
    let revision_lease = match reserve_revision_lease(client, &site_name).await {
        Ok(lease) => lease,
        Err(error) => {
            tracing::error!(%error, "failed to reserve SWIM revisions; running in static mode");
            return Ok(None);
        },
    };
    let cfg = SwimConfig {
        bind_addr,
        advertise_addr,
        site_name: site_name.clone(),
        seeds,
        gateway_address,
        swim_key,
        revision_lease,
    };
    match swim_runtime::start(cfg).await {
        Ok(handle) => {
            tracing::info!(addr = %addr_str, "SWIM runtime started");
            Ok(Some(handle))
        },
        Err(e) => {
            tracing::error!(error = %e, "SWIM runtime failed to start; running in static mode");
            Ok(None)
        },
    }
}

/// Number of revisions reserved durably for each operator process.
///
/// At the current one-second metadata repair rate this covers more than a
/// century. Exhaustion still causes the runtime to stop rather than reuse a
/// published revision.
const REVISION_LEASE_SIZE: u64 = 1_u64 << 32;
/// Maximum in-process foca identity renewals reserved for one operator.
const NODE_GENERATION_LEASE_SIZE: u64 = 1_u64 << 20;
/// Maximum resource-version conflicts retried during one reservation.
const REVISION_RESERVATION_ATTEMPTS: usize = 8;
/// `ConfigMap` data key containing the last reserved transport revision.
const REVISION_HIGH_KEY: &str = "revisionHighWatermark";
/// `ConfigMap` data key containing the last reserved identity generation.
const NODE_GENERATION_HIGH_KEY: &str = "nodeGenerationHighWatermark";

/// Reserve a disjoint transport-revision range and node generation.
///
/// The upper bound is written before any revision in the range can be
/// published. `replace` includes the `ConfigMap`'s `resourceVersion`, so
/// concurrent operator starts conflict and retry instead of overwriting one
/// another.
#[expect(
    clippy::too_many_lines,
    clippy::large_stack_frames,
    reason = "the Kubernetes read/create/replace CAS loop keeps each conflict and fail-closed path explicit"
)]
async fn reserve_revision_lease(client: &Client, site_name: &str) -> Result<RevisionLease, String> {
    let api: Api<ConfigMap> = Api::default_namespaced(client.clone());
    let cm_name = format!("grid-swim-revision-hwm-{site_name}");
    for _attempt in 0..REVISION_RESERVATION_ATTEMPTS {
        match api.get(&cm_name).await {
            Ok(mut cm) => {
                let data = cm.data.as_ref().ok_or_else(|| format!("{cm_name} has no data"))?;
                let current_high = parse_revision_value(data, REVISION_HIGH_KEY)
                    .or_else(|| parse_revision_value(data, "revision"))
                    .ok_or_else(|| format!("{cm_name} has no valid revision high-water mark"))?;
                let current_generation_high = parse_revision_value(data, NODE_GENERATION_HIGH_KEY)
                    .or_else(|| parse_revision_value(data, "nodeGeneration"))
                    .unwrap_or(0);
                let lease = next_revision_lease(current_high, current_generation_high)?;
                cm.data = Some(revision_lease_data(&lease));
                match api.replace(&cm_name, &PostParams::default(), &cm).await {
                    Ok(_) => {
                        tracing::info!(
                            first_revision = lease.first_revision,
                            last_revision = lease.last_revision,
                            first_node_generation = lease.first_node_generation,
                            last_node_generation = lease.last_node_generation,
                            cm = %cm_name,
                            "reserved SWIM revision range"
                        );
                        return Ok(lease);
                    },
                    Err(kube::Error::Api(conflict)) if conflict.code == 409 => {},
                    Err(replace_err) => return Err(format!("replace {cm_name}: {replace_err}")),
                }
            },
            Err(kube::Error::Api(not_found)) if not_found.code == 404 => {
                let lease = initial_revision_lease()?;
                let cm = ConfigMap {
                    metadata: ObjectMeta {
                        name: Some(cm_name.clone()),
                        ..ObjectMeta::default()
                    },
                    data: Some(revision_lease_data(&lease)),
                    ..ConfigMap::default()
                };
                match api.create(&PostParams::default(), &cm).await {
                    Ok(_) => {
                        tracing::info!(
                            first_revision = lease.first_revision,
                            last_revision = lease.last_revision,
                            first_node_generation = lease.first_node_generation,
                            last_node_generation = lease.last_node_generation,
                            cm = %cm_name,
                            "created SWIM revision reservation"
                        );
                        return Ok(lease);
                    },
                    Err(kube::Error::Api(conflict)) if conflict.code == 409 => {},
                    Err(create_err) => return Err(format!("create {cm_name}: {create_err}")),
                }
            },
            Err(read_err) => return Err(format!("read {cm_name}: {read_err}")),
        }
    }
    Err(format!(
        "could not reserve SWIM revisions in {cm_name} after {REVISION_RESERVATION_ATTEMPTS} conflicts"
    ))
}

/// Parse an unsigned value from `ConfigMap` data.
fn parse_revision_value(data: &BTreeMap<String, String>, key: &str) -> Option<u64> {
    data.get(key).and_then(|value| value.parse().ok())
}

/// Render the durable high-water marks for one reservation.
fn revision_lease_data(lease: &RevisionLease) -> BTreeMap<String, String> {
    BTreeMap::from([
        (REVISION_HIGH_KEY.to_owned(), lease.last_revision.to_string()),
        (
            NODE_GENERATION_HIGH_KEY.to_owned(),
            lease.last_node_generation.to_string(),
        ),
    ])
}

/// Build the first durable lease from wall-clock seeds.
fn initial_revision_lease() -> Result<RevisionLease, String> {
    let revision_seed = unix_millis()?;
    let node_generation = unix_nanos()?;
    lease_from_seeds(revision_seed, node_generation)
}

/// Build the next lease strictly after persisted high-water marks.
fn next_revision_lease(current_high: u64, current_generation_high: u64) -> Result<RevisionLease, String> {
    let revision_seed = current_high
        .checked_add(1)
        .ok_or_else(|| "SWIM revision high-water mark exhausted".to_owned())?
        .max(unix_millis()?);
    let node_generation = current_generation_high
        .checked_add(1)
        .ok_or_else(|| "SWIM node generation exhausted".to_owned())?
        .max(unix_nanos()?);
    lease_from_seeds(revision_seed, node_generation)
}

/// Build bounded revision and generation ranges from inclusive first values.
fn lease_from_seeds(first_revision: u64, node_generation: u64) -> Result<RevisionLease, String> {
    let last_revision = first_revision
        .checked_add(REVISION_LEASE_SIZE - 1)
        .ok_or_else(|| "SWIM revision range exhausted".to_owned())?;
    let last_node_generation = node_generation
        .checked_add(NODE_GENERATION_LEASE_SIZE - 1)
        .ok_or_else(|| "SWIM node generation range exhausted".to_owned())?;
    Ok(RevisionLease {
        first_revision,
        last_revision,
        first_node_generation: node_generation,
        last_node_generation,
    })
}

/// Return milliseconds since the Unix epoch as `u64`.
fn unix_millis() -> Result<u64, String> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock before Unix epoch: {error}"))?
        .as_millis();
    u64::try_from(millis).map_err(|_error| "Unix millisecond value exceeds u64".to_owned())
}

/// Return nanoseconds since the Unix epoch as `u64`.
fn unix_nanos() -> Result<u64, String> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock before Unix epoch: {error}"))?
        .as_nanos();
    u64::try_from(nanos).map_err(|_error| "Unix nanosecond value exceeds u64".to_owned())
}

/// Parse `GRID_SWIM_ENCRYPT_KEY` as a 32-byte AES-256-GCM key from 64 hex characters.
///
/// Returns `None` when the env var is absent (no encryption).
/// Logs an error and returns `None` when the value is present but malformed.
///
/// # Security invariant
///
/// The decoded key bytes are never written to logs or tracing spans.
fn parse_swim_key_env(name: &str) -> Option<swim::crypto::SwimKey> {
    let hex = std::env::var(name).ok()?;
    let hex = hex.trim();
    if hex.len() != 64 {
        tracing::error!(
            env = name,
            len = hex.len(),
            "SWIM encryption key must be a 64-character hex string (32 bytes); ignoring"
        );
        return None;
    }
    // Parse hex byte-by-byte using char::to_digit to avoid string slice indexing.
    // to_digit(16) returns 0..=15 as u32; cast to u8 is safe and done immediately.
    let hex_nibbles: Vec<u8> = hex
        .chars()
        .filter_map(|c| c.to_digit(16).and_then(|n| u8::try_from(n).ok()))
        .collect();
    if hex_nibbles.len() != 64 {
        tracing::error!(
            env = name,
            "SWIM encryption key contains invalid hex character; ignoring"
        );
        return None;
    }
    let mut key = [0_u8; 32];
    for (i, byte) in key.iter_mut().enumerate() {
        let hi = hex_nibbles.get(i * 2).copied().unwrap_or(0);
        let lo = hex_nibbles.get(i * 2 + 1).copied().unwrap_or(0);
        *byte = (hi << 4) | lo;
    }
    tracing::info!(env = name, "SWIM encryption key loaded from environment");
    Some(key)
}

/// Return the machine hostname or a safe fallback.
fn hostname_or_default() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "grid-operator".to_owned())
}

// ---------------------------------------------------------------------------
// Controller Setup
// ---------------------------------------------------------------------------

/// Run the [`GridNetwork`] controller.
///
/// In addition to watching `GridNetwork` resources, this controller watches
/// `InferenceProvider`, `GridSite`, and `Secret` resources.  Secret changes
/// trigger reconciliation of affected `GridNetwork`s when providers change.
///
/// Metrics TLS rotation is detected by bounded requeue rather than a
/// cluster-wide Secret watch — the operator only reads referenced
/// Secrets by explicit namespace/name during reconciliation.
#[expect(
    clippy::too_many_lines,
    reason = "controller setup with two cross-resource watches and optional SWIM"
)]
async fn run_network_controller(
    client: Client,
    ctx: Arc<OperatorCtx>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<GridNetwork>::all(client.clone());
    let provider_api = Api::<InferenceProvider>::all(client.clone());
    let site_api = Api::<GridSite>::all(client.clone());

    let controller = Controller::new(api, watcher::Config::default())
        .watches(
            provider_api,
            watcher::Config::default(),
            grid_network::network_refs_from_inference_provider,
        )
        .watches(
            site_api,
            watcher::Config::default(),
            grid_network::network_refs_from_grid_site,
        );
    let controller = if let Some(swim) = ctx.swim.as_ref() {
        controller.reconcile_all_on(swim.reconciliation_events())
    } else {
        controller
    };
    controller
        .run(grid_network::reconcile, grid_network::error_policy, ctx)
        .for_each(|result| async {
            match result {
                Ok((obj, _action)) => tracing::info!(%obj, "reconciled GridNetwork"),
                Err(e) => tracing::error!(error = ?e, "GridNetwork watch error"),
            }
        })
        .await;

    Ok(())
}

/// Run the [`GridSite`] controller.
async fn run_site_controller(client: Client) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<GridSite>::all(client.clone());

    Controller::new(api, watcher::Config::default())
        .with_config(kube::runtime::controller::Config::default().concurrency(16))
        .run(grid_site::reconcile, grid_site::error_policy, Arc::new(client))
        .for_each(|result| async {
            match result {
                Ok((obj, _action)) => tracing::info!(%obj, "reconciled GridSite"),
                Err(e) => tracing::error!(error = ?e, "GridSite watch error"),
            }
        })
        .await;

    Ok(())
}

/// Run the [`InferenceProvider`] controller (OP-02).
///
/// Watches `InferenceProvider` resources.  Metrics TLS rotation is detected
/// by bounded requeue rather than a cluster-wide Secret watch — the operator
/// only reads referenced Secrets by explicit namespace/name during
/// reconciliation.
///
/// [`InferenceProvider`]: operator::crd::inference_provider::InferenceProvider
async fn run_provider_controller(client: Client) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<InferenceProvider>::all(client.clone());

    Controller::new(api, watcher::Config::default())
        .run(
            inference_provider::reconcile,
            inference_provider::error_policy,
            Arc::new(client),
        )
        .for_each(|result| async {
            match result {
                Ok((obj, _action)) => tracing::info!(%obj, "reconciled InferenceProvider"),
                Err(e) => tracing::error!(error = ?e, "InferenceProvider watch error"),
            }
        })
        .await;

    Ok(())
}

/// Run the [`AgentToolProvider`] controller (grid#41).
///
/// Watches `AgentToolProvider` resources. Mirrors
/// [`run_provider_controller`]'s structure; cross-resource watches for
/// `GridNetwork`/`GridSite` changes are a follow-up, matching
/// [`InferenceProvider`]'s own documented watch limitation.
async fn run_agent_tool_provider_controller(client: Client) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<AgentToolProvider>::all(client.clone());

    Controller::new(api, watcher::Config::default())
        .run(
            agent_tool_provider::reconcile,
            agent_tool_provider::error_policy,
            Arc::new(client),
        )
        .for_each(|result| async {
            match result {
                Ok((obj, _action)) => tracing::info!(%obj, "reconciled AgentToolProvider"),
                Err(e) => tracing::error!(error = ?e, "AgentToolProvider watch error"),
            }
        })
        .await;

    Ok(())
}

/// Serve Prometheus metrics and health endpoints.
async fn run_metrics_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = std::env::var("GRID_METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:9090".to_owned());
    let app = axum::Router::new()
        .route("/metrics", axum::routing::get(metrics_handler))
        .route("/healthz", axum::routing::get(health_handler))
        .route("/readyz", axum::routing::get(health_handler));
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let bound_addr = listener.local_addr().map_or_else(|_| addr.clone(), |a| a.to_string());
    tracing::info!(addr = %bound_addr, "metrics server started");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Prometheus text-format metrics handler.
async fn metrics_handler() -> impl axum::response::IntoResponse {
    let body = operator::metrics::encode_metrics();
    (
        [(http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

/// Health check handler for liveness and readiness probes.
async fn health_handler() -> &'static str {
    "ok"
}

// ---------------------------------------------------------------------------
// Signals serving and peer polling (polling-metrics-signals feature)
// ---------------------------------------------------------------------------
//
// PR1 wires the mTLS serve path and the peer poller behind the opt-in flag and
// folds the rollup onto one wire path. It is additive: when the flag is on this
// site also serves and polls signals; the SWIM-carried signals and local
// scoring paths are untouched.
//
// PR2 SEAM: commit 8b25c3a ("take signals out of gossip and out of the
// overlay") is where the flag also *stops* carrying metrics signals through
// SWIM gossip and *disables* local scoring. That gating is deliberately NOT in
// this PR. It attaches in the GridNetwork reconcile/gossip path, not here.

/// How often the listener and poller re-read their own certificate.
///
/// Material rarely arrives with the process: cert-manager writes the Secret
/// after the operator rolls and rewrites it on renewal. Resolving once at
/// startup left a listener that came up early stuck without TLS for the process
/// lifetime, and one that came up after a renewal serving the old key.
const SIGNALS_TLS_POLL: std::time::Duration = std::time::Duration::from_secs(30);

/// Trigger cooperative shutdown on the first termination signal.
async fn watch_for_termination(trigger: operator::shutdown::Trigger) {
    let signal = first_termination_signal().await;
    tracing::info!(signal, "standing down");
    trigger.trigger();
}

/// Resolve on SIGTERM, or SIGINT, whichever arrives first.
async fn first_termination_signal() -> &'static str {
    let Ok(mut term) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) else {
        tracing::warn!("cannot watch for SIGTERM; only an interrupt will stand down cleanly");
        drop(tokio::signal::ctrl_c().await);
        return "SIGINT";
    };
    tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = tokio::signal::ctrl_c() => "SIGINT",
    }
}

/// Who is asking, which decides what they are served.
///
/// Decided from the certificate a connection presented, before any request
/// parameter is read, so a caller cannot widen its scope by asking differently.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Caller {
    /// Another site, named by the certificate it presented.
    ///
    /// Carries the labels this site holds for that peer, matched against the
    /// local `GridSite` record rather than the peer's own claim. `None` when
    /// this site holds no record for the name, holds one that refuses reads, or
    /// holds pins the presented key does not match, or when no certificate was
    /// presented at all. A `None` peer is served nothing.
    Peer(Option<BTreeMap<String, String>>),
    /// The co-located gateway, proven by this site's own certificate.
    Local,
}

/// How a caller is named on the signals listener.
#[derive(Clone)]
struct SignalsIdentity {
    /// Keys peers have declared, and the labels held for each.
    peers: operator::signals::PeerIdentities,
    /// This site's own certificate fingerprint, when it has one.
    own_key: Option<String>,
}

/// Everything this operator publishes on the signals path.
#[derive(Clone)]
struct Published {
    /// This site's own signals.
    site: operator::signals::SignalStore,
    /// What peers reported about themselves.
    peers: operator::signals::SignalStore,
    /// Labels a local consumer reads as, which are this site's own.
    local_labels: Arc<BTreeMap<String, String>>,
}

/// Serve the coarse signal rollup on the single mTLS wire path, fail closed.
///
/// There is no plaintext branch: without verified TLS material the rollup is
/// not exposed at all, rather than served to every caller as `Local`. The
/// listener rebinds whenever this site's certificate changes, because rustls
/// fixes the verifier at build time.
async fn run_signals_server(
    enabled: bool,
    client: Client,
    published: Published,
    peer_identities: operator::signals::PeerIdentities,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !enabled {
        return Ok(());
    }
    let addr = std::env::var("GRID_SIGNALS_ADDR").unwrap_or_else(|_| "0.0.0.0:9091".to_owned());
    let app = axum::Router::new()
        .route(operator::signals::SIGNALS_PATH, axum::routing::get(signals_handler))
        .with_state(published);

    // The opt-in listener rebinds on every material change, so a transient
    // bind blip (a socket in TIME_WAIT across a rebind) must not be fatal to
    // reconcile and the metrics server through `try_join!`: a serve error logs
    // and retries rather than propagating.
    #[expect(
        clippy::infinite_loop,
        reason = "serves for the process lifetime alongside the controllers"
    )]
    loop {
        if let Err(error) = serve_signals_once(&addr, &app, &client, &peer_identities).await {
            tracing::error!(%error, %addr, "signals listener error; retrying");
            tokio::time::sleep(SIGNALS_TLS_POLL).await;
        }
    }
}

/// One resolve-and-serve cycle: serve until this site's material changes, or
/// wait when material is unavailable. Fails closed, never serving plaintext.
async fn serve_signals_once(
    addr: &str,
    app: &axum::Router,
    client: &Client,
    peer_identities: &operator::signals::PeerIdentities,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (tls, own_key) = Box::pin(signals_identity(client)).await;
    let Some(tls) = tls else {
        tracing::warn!(%addr, "signals: TLS material unavailable; not serving the rollup (fail closed)");
        tokio::time::sleep(SIGNALS_TLS_POLL).await;
        return Ok(());
    };
    let identity = SignalsIdentity {
        peers: peer_identities.clone(),
        own_key: own_key.clone(),
    };
    serve_signals(
        addr,
        app.clone(),
        tls,
        identity,
        material_changed(client.clone(), own_key),
    )
    .await?;
    tracing::info!("signals TLS material changed; serving again");
    Ok(())
}

/// Bind and serve the mTLS listener until `changed` resolves.
async fn serve_signals(
    addr: &str,
    app: axum::Router,
    tls: Arc<rustls::ServerConfig>,
    identity: SignalsIdentity,
    changed: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener
        .local_addr()
        .map_or_else(|_| addr.to_owned(), |a| a.to_string());
    tracing::info!(addr = %bound, tls = true, "signals server started");
    serve_signals_tls(listener, app, tls, identity, changed).await
}

/// Resolves when this site's certificate definitely stops matching `serving`.
///
/// A transient read failure is not a change: a momentary API-server hiccup must
/// not tear down a healthy mTLS listener (or the poller) for a poll interval
/// when the serving certificate never actually moved. Only a successfully
/// observed key that differs, including a genuine removal, resolves.
async fn material_changed(client: Client, serving: Option<String>) {
    loop {
        tokio::time::sleep(SIGNALS_TLS_POLL).await;
        if let Some(observed) = Box::pin(observe_own_key(&client)).await
            && observed != serving
        {
            return;
        }
    }
}

/// This site's own key as a change sentinel.
///
/// `None` means "could not read, treat as unchanged"; `Some(opt)` is a definite
/// observation, with `opt` itself `None` when the material is genuinely absent.
/// Separating the two is what keeps a transient read error from masquerading as
/// a certificate change.
async fn observe_own_key(client: &Client) -> Option<Option<String>> {
    let networks: Api<GridNetwork> = Api::all(client.clone());
    let list = networks.list(&kube::api::ListParams::default()).await.ok()?;
    let Some(network) = list.items.into_iter().next() else {
        return Some(None);
    };
    grid_network::signals_own_key(&network, client).await.ok()
}

/// Accept signals connections, deciding scope from the certificate presented.
async fn serve_signals_tls(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    tls: Arc<rustls::ServerConfig>,
    identity: SignalsIdentity,
    changed: impl Future<Output = ()> + Send,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    let mut changed = std::pin::pin!(changed);
    loop {
        let accepted = tokio::select! {
            () = &mut changed => return Ok(()),
            accepted = listener.accept() => accepted,
        };
        let Ok((stream, remote)) = accepted else {
            continue;
        };
        tokio::spawn(serve_signals_connection(
            acceptor.clone(),
            app.clone(),
            stream,
            remote,
            identity.clone(),
        ));
    }
}

/// Who a connection speaks for, decided from the key it presented.
///
/// Nothing is trusted for presenting nothing: a caller with no certificate, or
/// one carrying a key nobody declared, is named so it is served nothing. Only
/// this site's own certificate earns `Local`, so "no cert" is never as
/// privileged as the site's own workload.
fn caller_for(
    presented: Option<&[rustls::pki_types::CertificateDer<'static>]>,
    identity: &SignalsIdentity,
    remote: SocketAddr,
) -> Caller {
    let Some(leaf) = presented.and_then(<[_]>::first) else {
        tracing::debug!(%remote, "signals caller presented no certificate");
        return Caller::Peer(None);
    };
    let fingerprint = operator::signals::leaf_fingerprint(leaf);
    if identity.own_key.as_deref() == Some(fingerprint.as_str()) {
        return Caller::Local;
    }
    let named = identity.peers.resolve_by_key(&fingerprint);
    if named.is_none() {
        tracing::debug!(%remote, "signals caller presented a key this site has not declared");
    }
    Caller::Peer(named)
}

/// Handshake one connection, then serve it with the scope its certificate earns.
#[expect(clippy::large_stack_frames, reason = "async future over a rustls handshake")]
async fn serve_signals_connection(
    acceptor: tokio_rustls::TlsAcceptor,
    app: axum::Router,
    stream: tokio::net::TcpStream,
    remote: SocketAddr,
    identity: SignalsIdentity,
) {
    let Ok(stream) = acceptor.accept(stream).await else {
        tracing::debug!(%remote, "signals handshake failed");
        return;
    };
    let caller = caller_for(stream.get_ref().1.peer_certificates(), &identity, remote);
    let service = hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
        use tower::Service as _;
        let mut request = request;
        request.extensions_mut().insert(caller.clone());
        app.clone().call(request)
    });
    let io = hyper_util::rt::TokioIo::new(stream);
    if let Err(error) = hyper::server::conn::http1::Builder::new()
        .serve_connection(io, service)
        .await
    {
        tracing::debug!(%remote, %error, "signals connection ended");
    }
}

/// Serve the coarse rollup, following the multi-target exporter pattern.
///
/// `target` names one provider, `collect[]` names the signals wanted. Scope
/// comes from the connection, so no parameter can widen it.
async fn signals_handler(
    axum::extract::State(published): axum::extract::State<Published>,
    axum::Extension(caller): axum::Extension<Caller>,
    axum::extract::Query(params): axum::extract::Query<Vec<(String, String)>>,
) -> axum::response::Response {
    let target = params.iter().find(|(k, _)| k == "target").map(|(_, v)| v.as_str());
    let collect: Vec<String> = params
        .iter()
        .filter(|(k, _)| k == "collect[]" || k == "collect")
        .map(|(_, v)| v.clone())
        .collect();

    let reader = match &caller {
        Caller::Local => &*published.local_labels,
        Caller::Peer(Some(labels)) => labels,
        Caller::Peer(None) => return refused(),
    };
    let reader = Some(reader);
    let (mut body, mut oldest) = published.site.render(target, &collect, reader);
    if caller == Caller::Local {
        // Relayed peer signals are served only to Local (this site's own data
        // plane), so the peers store carries no access map. Relaying peers to a
        // `Peer(Some)` caller in future must add scoping here, or it would
        // bypass the per-target access policy the site store enforces.
        let (relayed, relayed_age) = published.peers.render(target, &collect, reader);
        body.push_str(&relayed);
        oldest = oldest.max(relayed_age);
    }
    served(body, oldest)
}

/// Refuse a caller this site will not serve.
///
/// A status rather than an empty body: an empty exposition says nothing is held
/// right now, which a peer would chase as a fault; a refusal says the answer
/// will not change until an administrator changes it. The reason is the same
/// for every cause, so a caller learns only that it is refused.
fn refused() -> axum::response::Response {
    (
        http::StatusCode::FORBIDDEN,
        "signals: caller is not permitted to read this site\n",
    )
        .into_response()
}

/// One exposition response, with `Age` bounding the whole body.
fn served(body: String, oldest: std::time::Duration) -> axum::response::Response {
    (
        [
            (http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8"),
            (http::header::AGE, &*oldest.as_secs().to_string()),
        ],
        body,
    )
        .into_response()
}

/// TLS material for the signals listener, and this site's own fingerprint.
///
/// Both resolve to `None` when the network declares no TLS or the material
/// cannot be read; the serve loop treats either as "do not expose", so a load
/// failure fails closed rather than downgrading to plaintext.
async fn signals_identity(client: &Client) -> (Option<Arc<rustls::ServerConfig>>, Option<String>) {
    let networks: Api<GridNetwork> = Api::all(client.clone());
    let network = networks
        .list(&kube::api::ListParams::default())
        .await
        .ok()
        .and_then(|list| list.items.into_iter().next());
    let Some(network) = network else {
        return (None, None);
    };
    (
        Box::pin(signals_listener_tls(&network, client)).await,
        Box::pin(signals_own_key(&network, client)).await,
    )
}

/// TLS for the listener, or `None` when unconfigured or unreadable.
async fn signals_listener_tls(network: &GridNetwork, client: &Client) -> Option<Arc<rustls::ServerConfig>> {
    match grid_network::signals_server_config(network, client).await {
        Ok(config) => {
            if config.is_none() {
                tracing::info!("signals TLS not configured; rollup not exposed");
            }
            config
        },
        Err(error) => {
            // Configured but unreadable: fail closed rather than serve plaintext.
            tracing::error!(%error, "signals TLS configured but unavailable; not serving (fail closed)");
            None
        },
    }
}

/// This site's own certificate fingerprint, for recognising its own workloads.
async fn signals_own_key(network: &GridNetwork, client: &Client) -> Option<String> {
    match grid_network::signals_own_key(network, client).await {
        Ok(key) => key,
        Err(error) => {
            tracing::warn!(%error, "this site's own certificate is unreadable; its workloads cannot be recognised");
            None
        },
    }
}

/// Scrape this site's providers on their own interval and publish the result.
///
/// Separate from reconcile, which runs on an interval sized for declarations
/// and is two orders of magnitude slower than these values move.
async fn run_local_scraper(
    enabled: bool,
    ctx: Arc<OperatorCtx>,
    client: Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !enabled {
        return Ok(());
    }
    let interval = scrape_interval();
    tracing::info!(interval_ms = interval.as_millis(), "local signals scraper started");
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    #[expect(
        clippy::infinite_loop,
        reason = "runs for the process lifetime alongside the controllers"
    )]
    loop {
        ticker.tick().await;
        let networks: Api<GridNetwork> = Api::all(client.clone());
        let Ok(list) = networks.list(&kube::api::ListParams::default()).await else {
            continue;
        };
        for network in list.items.iter().filter_map(|n| n.metadata.name.as_deref()) {
            if let Err(error) = grid_network::refresh_signals(&ctx, &client, network).await {
                tracing::warn!(network, %error, "local signals refresh failed");
            }
        }
    }
}

/// How often this site reads its own providers.
///
/// `GRID_SIGNALS_SCRAPE_INTERVAL_MS` wins when set, since the seconds form
/// bottoms out at one and this scrape never leaves the node. Floors at 50ms.
fn scrape_interval() -> std::time::Duration {
    let ms = parse_env_or(
        "GRID_SIGNALS_SCRAPE_INTERVAL_MS",
        parse_env_or("GRID_SIGNALS_SCRAPE_INTERVAL_SECS", 5_u64).saturating_mul(1_000),
    );
    std::time::Duration::from_millis(ms.max(50))
}

/// Poll every alive peer's signals endpoint on a coarse interval, fail closed.
///
/// The wire path is mTLS only, so a round requires verified client material.
/// Without it the poller idles rather than polling peers in plaintext, and it
/// rebuilds when this site's certificate changes.
async fn run_peer_poller(
    enabled: bool,
    ctx: Arc<OperatorCtx>,
    swim: Option<Arc<swim_runtime::SwimHandle>>,
    client: Client,
    shutdown: operator::shutdown::Shutdown,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !enabled {
        return Ok(());
    }
    let Some(swim) = swim else {
        tracing::info!("peer signals poller disabled: SWIM is not running");
        return Ok(());
    };
    PeerPoller {
        ctx,
        swim,
        client,
        port: parse_env_or("GRID_SIGNALS_PEER_PORT", 9091_u16),
        interval: std::time::Duration::from_secs(parse_env_or("GRID_SIGNALS_PEER_INTERVAL_SECS", 30_u64)),
        shutdown,
    }
    .run()
    .await;
    Ok(())
}

/// The invariant state one peer-polling session threads through every round.
struct PeerPoller {
    /// Where polled peer signals are published and peer identities are read.
    ctx: Arc<OperatorCtx>,
    /// Membership, for the alive peers and their advertised addresses.
    swim: Arc<swim_runtime::SwimHandle>,
    /// Kube client, for resolving this site's TLS material.
    client: Client,
    /// Port each peer's signals endpoint is dialled on.
    port: u16,
    /// How often a round runs.
    interval: std::time::Duration,
    /// Lets an in-flight round stand down when the process is stopping.
    shutdown: operator::shutdown::Shutdown,
}

impl PeerPoller {
    /// Rebuild and poll until shutdown.
    async fn run(&self) {
        while !self.shutdown.is_triggered() && self.cycle().await {}
        tracing::info!("peer signals poller stopped");
    }

    /// One build-and-poll cycle; `false` means shutdown arrived while idle.
    async fn cycle(&self) -> bool {
        let own_key = Box::pin(signals_identity(&self.client)).await.1;
        let Some(source) = Box::pin(peer_source(&self.client, self.shutdown.clone())).await else {
            tracing::warn!("peer signals poller idle: TLS material unavailable (fail closed)");
            return self.idle_until_retry().await;
        };
        tracing::info!(
            self.port,
            interval_secs = self.interval.as_secs(),
            source = source.name(),
            "peer signals poller started"
        );
        self.poll_until_changed(source.as_ref(), material_changed(self.client.clone(), own_key))
            .await;
        true
    }

    /// Wait out the TLS poll interval; `false` if shutdown arrives first.
    async fn idle_until_retry(&self) -> bool {
        tokio::select! {
            biased;
            () = self.shutdown.triggered() => false,
            () = tokio::time::sleep(SIGNALS_TLS_POLL) => true,
        }
    }

    /// Poll every interval until this site's material changes, or shutdown.
    async fn poll_until_changed(
        &self,
        source: &dyn operator::signals::PeerSignals,
        changed: impl Future<Output = ()> + Send,
    ) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut changed = std::pin::pin!(changed);
        loop {
            tokio::select! {
                biased;
                () = self.shutdown.triggered() => return,
                () = &mut changed => {
                    tracing::info!("peer signals TLS material changed; rebuilding the client");
                    return;
                },
                _ = ticker.tick() => {},
            }
            self.poll_once(source).await;
        }
    }

    /// Collect from every alive peer and replace what is published for them.
    async fn poll_once(&self, source: &dyn operator::signals::PeerSignals) {
        // A refusal is symmetric: one we will not answer is one we do not read.
        let identities = self.ctx.peer_identities();
        let snapshot = self.swim.snapshot();
        let alive = snapshot
            .members
            .iter()
            .filter(|m| m.status == operator::swim::MemberStatus::Alive)
            .filter(|m| !identities.refuses(&m.site_id))
            .map(|m| (m.site_id.as_str(), m.endpoint.as_str()));
        // Verified against the keys declared for that peer, not any key the CA signed.
        let mut sites = operator::signals::peer_sites(alive, self.swim.site_name(), self.port, "https");
        for site in &mut sites {
            site.pins = identities.pins_for(&site.name);
        }
        if sites.is_empty() {
            return;
        }
        let collected = source.collect(&sites).await;
        self.ctx.peers().refresh(collected, peer_signals_ttl());
    }
}

/// Build the peer poller source from verified TLS material, or `None` when absent.
async fn peer_source(
    client: &Client,
    shutdown: operator::shutdown::Shutdown,
) -> Option<Box<dyn operator::signals::PeerSignals>> {
    let tls = peer_tls(client).await?;
    Some(Box::new(operator::signals::PollPeers {
        timeout: std::time::Duration::from_secs(parse_env_or("GRID_SIGNALS_PEER_TIMEOUT_SECS", 5_u64)),
        tls: Some(tls),
        collect: parse_peer_collect(),
        concurrency: parse_env_or("GRID_SIGNALS_PEER_CONCURRENCY", 8_usize),
        attempts: parse_env_or("GRID_SIGNALS_PEER_ATTEMPTS", 3_u32),
        backoff: std::time::Duration::from_millis(parse_env_or("GRID_SIGNALS_PEER_BACKOFF_MS", 50_u64)),
        budget: std::time::Duration::from_secs(parse_env_or("GRID_SIGNALS_PEER_BUDGET_SECS", 10_u64)),
        slow_after: std::time::Duration::from_millis(parse_env_or("GRID_SIGNALS_PEER_SLOW_MS", 1_000_u64)),
        shutdown,
    }))
}

/// How long a polled peer signal is served before it expires.
///
/// Coarser than the local store because a peer is polled across a cluster
/// boundary and on a longer interval, so a couple of missed rounds must not
/// erase it.
fn peer_signals_ttl() -> std::time::Duration {
    std::time::Duration::from_secs(parse_env_or("GRID_SIGNALS_PEER_TTL_SECS", 120_u64).max(1))
}

/// Client TLS for peer polling, or `None` when unconfigured or unreadable.
///
/// Fails closed: configured-but-unreadable material returns `None` so the
/// poller idles rather than dialing peers in plaintext.
async fn peer_tls(client: &Client) -> Option<Arc<operator::signals::PeerTlsMaterial>> {
    let networks: Api<GridNetwork> = Api::all(client.clone());
    let list = networks.list(&kube::api::ListParams::default()).await.ok()?;
    let network = list.items.into_iter().next()?;
    match grid_network::peer_tls_config(&network, client).await {
        Ok(Some(material)) => Some(material),
        Ok(None) => {
            tracing::info!("peer signals: no TLS configured; peers not polled");
            None
        },
        Err(error) => {
            tracing::warn!(%error, "peer signals TLS configured but unavailable; not polling (fail closed)");
            None
        },
    }
}

/// Signals asked of each peer, from `GRID_SIGNALS_PEER_COLLECT`.
///
/// Newline-separated metric names. Empty asks a peer for everything it holds.
fn parse_peer_collect() -> Vec<String> {
    std::env::var("GRID_SIGNALS_PEER_COLLECT")
        .map(|raw| {
            raw.lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// Read an environment variable, falling back when unset or unparseable.
fn parse_env_or<T: std::str::FromStr + std::fmt::Display + Copy>(name: &str, fallback: T) -> T {
    match std::env::var(name) {
        Ok(raw) => raw.parse().unwrap_or_else(|_| {
            tracing::warn!(var = name, value = raw, default = %fallback, "unparseable; using default");
            fallback
        }),
        Err(_) => fallback,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A caller is scoped from its certificate, and nothing is trusted for
    /// presenting nothing: only this site's own certificate earns `Local`, and
    /// a missing or undeclared certificate is served nothing.
    #[test]
    fn signals_caller_scope_requires_a_positive_credential() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let leaf = rustls::pki_types::CertificateDer::from(vec![1_u8, 2, 3, 4]);
        let own_key = operator::signals::leaf_fingerprint(&leaf);

        let owner = SignalsIdentity {
            peers: operator::signals::PeerIdentities::new(),
            own_key: Some(own_key),
        };
        // No certificate and an empty chain are both served nothing, never Local.
        assert_eq!(caller_for(None, &owner, addr), Caller::Peer(None));
        assert_eq!(caller_for(Some(&[]), &owner, addr), Caller::Peer(None));
        // This site's own certificate is the only key that earns Local.
        assert_eq!(
            caller_for(Some(std::slice::from_ref(&leaf)), &owner, addr),
            Caller::Local
        );

        // A certificate this site has not declared is served nothing, not Local.
        let stranger = SignalsIdentity {
            peers: operator::signals::PeerIdentities::new(),
            own_key: Some("0".repeat(64)),
        };
        assert_eq!(
            caller_for(Some(std::slice::from_ref(&leaf)), &stranger, addr),
            Caller::Peer(None)
        );
    }

    #[test]
    fn lease_from_seeds_reserves_full_disjoint_block() {
        let first = lease_from_seeds(100, 7).unwrap_or_else(|_| std::process::abort());
        let second = lease_from_seeds(
            first
                .last_revision
                .checked_add(1)
                .unwrap_or_else(|| std::process::abort()),
            first
                .last_node_generation
                .checked_add(1)
                .unwrap_or_else(|| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(first.first_revision, 100);
        assert_eq!(first.last_revision - first.first_revision + 1, REVISION_LEASE_SIZE);
        assert!(second.first_revision > first.last_revision);
        assert!(second.first_node_generation > first.last_node_generation);
    }

    #[test]
    fn persisted_values_win_when_clock_is_behind() {
        let future_revision = 10_000_000_000_000_u64;
        let future_generation = 10_000_000_000_000_000_000_u64;
        let lease = next_revision_lease(future_revision, future_generation).unwrap_or_else(|_| std::process::abort());
        assert_eq!(lease.first_revision, future_revision + 1);
        assert_eq!(lease.first_node_generation, future_generation + 1);
    }

    #[test]
    fn exhausted_revision_or_generation_fails_closed() {
        assert!(
            next_revision_lease(u64::MAX, 1).is_err(),
            "u64::MAX revision must overflow"
        );
        assert!(
            next_revision_lease(1, u64::MAX).is_err(),
            "u64::MAX generation must overflow"
        );
        assert!(lease_from_seeds(u64::MAX, 1).is_err(), "u64::MAX seed must overflow");
    }

    #[test]
    fn reservation_data_round_trips() {
        let lease = RevisionLease {
            first_revision: 10,
            last_revision: 20,
            first_node_generation: 30,
            last_node_generation: 40,
        };
        let data = revision_lease_data(&lease);
        assert_eq!(parse_revision_value(&data, REVISION_HIGH_KEY), Some(20));
        assert_eq!(parse_revision_value(&data, NODE_GENERATION_HIGH_KEY), Some(40));
    }
}
