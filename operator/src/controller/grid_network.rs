//! [`GridNetwork`] controller.
//!
//! Reconciles [`GridNetwork`] resources: generates the grid CA
//! and site certificate, manages TLS secrets, generates the
//! grid ID, and signals the SWIM runtime to start.
//!
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork

use std::sync::Arc;

use kube::{
    Client,
    api::{Api, Patch, PatchParams},
    runtime::controller::Action,
};
use tokio::time::Duration;
use tracing::info;

use crate::{
    crd::grid_network::{GridNetwork, GridNetworkPhase, GridNetworkStatus},
    error::OperatorError,
    resources::secret,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Requeue interval after a successful reconciliation.
const REQUEUE_INTERVAL: Duration = Duration::from_secs(300);

/// Field manager name for server-side apply.
const FIELD_MANAGER: &str = "grid-operator";

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

/// Reconcile a [`GridNetwork`] resource.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API or certificate
/// generation failures.
#[expect(clippy::large_stack_frames, reason = "async future with kube API types")]
pub async fn reconcile(network: Arc<GridNetwork>, client: Arc<Client>) -> Result<Action, OperatorError> {
    let name = network
        .metadata
        .name
        .as_deref()
        .unwrap_or_else(|| std::process::abort());

    info!(name, "reconciling GridNetwork");

    ensure_tls_secrets(&network, &client).await?;

    let grid_id = resolve_grid_id(&network);
    let phase = determine_phase(&network, &grid_id);
    update_status(&network, client.as_ref(), &grid_id, &phase).await?;

    Ok(Action::requeue(REQUEUE_INTERVAL))
}

/// Error policy for the [`GridNetwork`] controller.
pub fn error_policy(_network: Arc<GridNetwork>, error: &OperatorError, _ctx: Arc<Client>) -> Action {
    tracing::error!(%error, "GridNetwork reconciliation failed");
    Action::requeue(Duration::from_secs(30))
}

// ---------------------------------------------------------------------------
// TLS Secrets
// ---------------------------------------------------------------------------

/// Ensure CA and site certificate secrets exist.
///
/// Generates both together so the CA is available for
/// signing the site certificate without needing to
/// reconstruct it from PEM.
#[expect(clippy::large_stack_frames, reason = "async future with kube API types")]
async fn ensure_tls_secrets(network: &GridNetwork, client: &Client) -> Result<(), OperatorError> {
    let tls = &network.spec.tls;
    let (Some(ca_ref), Some(site_ref)) = (&tls.ca_secret_ref, &tls.site_secret_ref) else {
        return Ok(());
    };

    let ca_api: Api<k8s_openapi::api::core::v1::Secret> = Api::namespaced(client.clone(), &ca_ref.namespace);
    let site_api: Api<k8s_openapi::api::core::v1::Secret> = Api::namespaced(client.clone(), &site_ref.namespace);

    let ca_exists = ca_api.get_opt(&ca_ref.name).await?.is_some();
    let site_exists = site_api.get_opt(&site_ref.name).await?.is_some();

    if ca_exists && site_exists {
        return Ok(());
    }

    let site_name = network_site_name(network);
    let ca = certs::generate_ca("grid-ca")?;
    let site_cert = certs::generate_site_cert(&ca, &site_name)?;

    apply_ca_secret(&ca_api, ca_ref, &ca).await?;
    apply_site_secret(&site_api, site_ref, &site_cert).await?;

    info!("created grid TLS secrets");
    Ok(())
}

/// Apply the CA secret via server-side apply.
async fn apply_ca_secret(
    api: &Api<k8s_openapi::api::core::v1::Secret>,
    ca_ref: &crate::crd::grid_network::SecretRef,
    ca: &certs::CaCert,
) -> Result<(), OperatorError> {
    let data = secret::ca_secret_data(ca);
    let s = secret::build(&ca_ref.name, &ca_ref.namespace, data);
    api.patch(
        &ca_ref.name,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(&s),
    )
    .await?;
    Ok(())
}

/// Apply the site certificate secret via server-side apply.
async fn apply_site_secret(
    api: &Api<k8s_openapi::api::core::v1::Secret>,
    site_ref: &crate::crd::grid_network::SecretRef,
    site_cert: &certs::SiteCertOutput,
) -> Result<(), OperatorError> {
    let data = secret::site_cert_secret_data(site_cert);
    let s = secret::build(&site_ref.name, &site_ref.namespace, data);
    api.patch(
        &site_ref.name,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(&s),
    )
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Grid ID
// ---------------------------------------------------------------------------

/// Resolve the grid ID: use spec if set, or status if
/// previously generated, or generate a new one.
fn resolve_grid_id(network: &GridNetwork) -> String {
    if !network.spec.grid_id.is_empty() {
        return network.spec.grid_id.clone();
    }
    if let Some(status) = &network.status
        && !status.grid_id.is_empty()
    {
        return status.grid_id.clone();
    }
    uuid::Uuid::new_v4().to_string()
}

/// Determine the lifecycle phase.
fn determine_phase(network: &GridNetwork, grid_id: &str) -> GridNetworkPhase {
    if grid_id.is_empty() {
        return GridNetworkPhase::Pending;
    }
    let has_tls = network.spec.tls.ca_secret_ref.is_some();
    if has_tls {
        GridNetworkPhase::Initializing
    } else {
        GridNetworkPhase::Pending
    }
}

// ---------------------------------------------------------------------------
// Status Update
// ---------------------------------------------------------------------------

/// Patch the `GridNetwork` status subresource.
async fn update_status(
    network: &GridNetwork,
    client: &Client,
    grid_id: &str,
    phase: &GridNetworkPhase,
) -> Result<(), OperatorError> {
    let name = network
        .metadata
        .name
        .as_deref()
        .unwrap_or_else(|| std::process::abort());

    let api: Api<GridNetwork> = Api::all(client.clone());
    let status = GridNetworkStatus {
        connected_sites: 0,
        grid_id: grid_id.to_owned(),
        observed_generation: network.metadata.generation.unwrap_or(0),
        phase: phase.clone(),
    };

    let patch = serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "status": status
    });

    api.patch_status(name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(patch))
        .await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Derive the site name from the `GridNetwork` metadata.
fn network_site_name(network: &GridNetwork) -> String {
    network
        .metadata
        .name
        .clone()
        .unwrap_or_else(|| "unknown-site".to_owned())
}
