//! AI Grid operator binary.
//!
//! Runs Kubernetes controllers for [`GridNetwork`] and
//! [`GridSite`] resources. In future phases, also runs the
//! SWIM runtime for peer-to-peer mesh formation.
//!
//! [`GridNetwork`]: operator::crd::grid_network::GridNetwork
//! [`GridSite`]: operator::crd::grid_site::GridSite

#![deny(unsafe_code)]

use std::sync::Arc;

use futures::StreamExt as _;
use kube::{
    Api, Client,
    runtime::{controller::Controller, watcher},
};
use operator::{
    controller::{grid_network, grid_site},
    crd::{grid_network::GridNetwork, grid_site::GridSite},
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
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

    let result = tokio::try_join!(
        run_network_controller(client.clone()),
        run_site_controller(client.clone()),
    );

    if let Err(e) = result {
        tracing::error!(error = %e, "controller error");
    }
}

// ---------------------------------------------------------------------------
// Controller Setup
// ---------------------------------------------------------------------------

/// Run the [`GridNetwork`] controller.
async fn run_network_controller(client: Client) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<GridNetwork>::all(client.clone());

    Controller::new(api, watcher::Config::default())
        .run(grid_network::reconcile, grid_network::error_policy, Arc::new(client))
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
