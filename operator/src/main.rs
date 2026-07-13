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
//! [`GridNetwork`]: operator::crd::grid_network::GridNetwork
//! [`GridSite`]: operator::crd::grid_site::GridSite
//! [`InferenceProvider`]: operator::crd::inference_provider::InferenceProvider

#![deny(unsafe_code)]

use std::{net::SocketAddr, sync::Arc};

use futures::StreamExt as _;
use kube::{
    Api, Client,
    runtime::{controller::Controller, watcher},
};
use operator::{
    controller::{
        grid_network::{self, OperatorCtx},
        grid_site, inference_provider,
    },
    crd::{grid_network::GridNetwork, grid_site::GridSite, inference_provider::InferenceProvider},
    swim_runtime::{self, SwimConfig},
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
#[expect(clippy::large_stack_frames, reason = "top-level binary with tokio runtime")]
async fn main() {
    tracing_subscriber::fmt::init();
    tracing::info!("starting grid-operator");

    let client = match Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to create kube client");
            return;
        },
    };

    // Start the SWIM runtime if GRID_SWIM_BIND_ADDR is set.
    // The runtime runs in the background; failures are logged and the
    // operator continues in static mode.
    let swim = maybe_start_swim().await;

    let ctx = Arc::new(OperatorCtx {
        client: client.clone(),
        swim,
    });

    let result = tokio::try_join!(
        run_network_controller(client.clone(), Arc::clone(&ctx)),
        run_site_controller(client.clone()),
        run_provider_controller(client.clone()),
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
async fn maybe_start_swim() -> Option<Arc<swim_runtime::SwimHandle>> {
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
    let cfg = SwimConfig {
        bind_addr,
        advertise_addr,
        site_name,
        seeds,
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
/// `InferenceProvider` and `GridSite` resources.  Changes to either trigger
/// reconciliation of the owning [`GridNetwork`] (identified by
/// `spec.gridNetworkRef`), keeping routing overlay `ConfigMap`s consistent
/// with provider availability and site membership.
async fn run_network_controller(
    client: Client,
    ctx: Arc<OperatorCtx>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<GridNetwork>::all(client.clone());
    let provider_api = Api::<InferenceProvider>::all(client.clone());
    let site_api = Api::<GridSite>::all(client.clone());

    Controller::new(api, watcher::Config::default())
        .watches(
            provider_api,
            watcher::Config::default(),
            grid_network::network_refs_from_inference_provider,
        )
        .watches(
            site_api,
            watcher::Config::default(),
            grid_network::network_refs_from_grid_site,
        )
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
