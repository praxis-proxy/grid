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
//! list of seed peer socket addresses. When `GRID_SWIM_BIND_ADDR` is absent
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

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

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
        grid_network::{self, OperatorCtx},
        grid_site, inference_provider,
    },
    crd::{grid_network::GridNetwork, grid_site::GridSite, inference_provider::InferenceProvider},
    gateway,
    swim_runtime::{self, RevisionLease, SwimConfig},
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
#[expect(clippy::large_stack_frames, reason = "top-level binary with tokio runtime")]
async fn main() {
    tracing_subscriber::fmt::init();
    tracing::info!("starting grid-operator");

    let config = Cli::parse();

    let client = match Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to create kube client");
            return;
        },
    };

    let swim = maybe_start_swim(&client, &config.gateway).await;

    if let Some(handle) = &swim {
        tokio::spawn(gateway::run_discovery_poller(
            client.clone(),
            Arc::clone(handle),
            config.gateway.clone(),
        ));
    }

    let ctx = Arc::new(OperatorCtx::new(client.clone(), swim));

    let result = tokio::try_join!(
        run_network_controller(client.clone(), Arc::clone(&ctx)),
        run_site_controller(client.clone()),
        run_provider_controller(client.clone()),
        run_metrics_server(),
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
/// starts successfully.  Returns `None` when the variable is absent,
/// unparseable, or the bind fails (all logged at error level).
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
async fn maybe_start_swim(client: &Client, config: &gateway::Config) -> Option<Arc<swim_runtime::SwimHandle>> {
    let addr_str = std::env::var("GRID_SWIM_BIND_ADDR").ok()?;
    let bind_addr = match addr_str.parse() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(addr = %addr_str, error = %e, "GRID_SWIM_BIND_ADDR not a valid socket address");
            return None;
        },
    };
    let advertise_addr = parse_optional_socket_addr_env("GRID_SWIM_ADVERTISE_ADDR");
    let seeds = parse_socket_addr_list_env("GRID_SWIM_SEEDS");
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
            return None;
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
            Some(handle)
        },
        Err(e) => {
            tracing::error!(error = %e, "SWIM runtime failed to start; running in static mode");
            None
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
                    Err(kube::Error::Api(error)) if error.code == 409 => {},
                    Err(error) => return Err(format!("replace {cm_name}: {error}")),
                }
            },
            Err(kube::Error::Api(error)) if error.code == 404 => {
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
                    Err(kube::Error::Api(error)) if error.code == 409 => {},
                    Err(error) => return Err(format!("create {cm_name}: {error}")),
                }
            },
            Err(error) => return Err(format!("read {cm_name}: {error}")),
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

/// Parse an optional socket address environment variable.
fn parse_optional_socket_addr_env(name: &str) -> Option<SocketAddr> {
    let value = std::env::var(name).ok()?;
    match value.parse() {
        Ok(addr) => Some(addr),
        Err(e) => {
            tracing::error!(env = name, value = %value, error = %e, "SWIM socket address env var is invalid");
            None
        },
    }
}

/// Parse a comma-separated socket address environment variable.
fn parse_socket_addr_list_env(name: &str) -> Vec<SocketAddr> {
    let Ok(value) = std::env::var(name) else {
        return Vec::new();
    };

    value
        .split(',')
        .filter_map(|raw| {
            let item = raw.trim();
            if item.is_empty() {
                return None;
            }
            match item.parse() {
                Ok(addr) => Some(addr),
                Err(e) => {
                    tracing::error!(env = name, value = %item, error = %e, "SWIM seed address is invalid");
                    None
                },
            }
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(next_revision_lease(u64::MAX, 1).is_err());
        assert!(next_revision_lease(1, u64::MAX).is_err());
        assert!(lease_from_seeds(u64::MAX, 1).is_err());
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
