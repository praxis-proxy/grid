//! [`GridNetwork`] controller.
//!
//! Reconciles [`GridNetwork`] resources: generates the grid CA
//! and site certificate, manages TLS secrets, generates the
//! grid ID, signals the SWIM runtime to start, and renders
//! routing overlay ConfigMaps for each gateway reference.
//!
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork

use std::sync::Arc;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
    Client,
    api::{Api, ListParams, Patch, PatchParams},
    runtime::controller::Action,
};
use tokio::time::Duration;
use tracing::info;

use crate::{
    crd::{
        grid_network::{GatewayRef, GridNetwork, GridNetworkPhase, GridNetworkStatus},
        inference_provider::InferenceProvider,
    },
    error::OperatorError,
    resources::{routing_overlay, secret},
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
    reconcile_routing_overlay(&network, &client).await?;

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
// Routing Overlay
// ---------------------------------------------------------------------------

/// Reconcile routing overlay `ConfigMap`s for a [`GridNetwork`].
///
/// Lists all [`InferenceProvider`]s and [`GridSite`]s cluster-wide, then
/// renders one overlay `ConfigMap` per `gatewayRef`.  Each gateway may
/// declare its own `localSiteName` — the `local_site` in the overlay for
/// gateway G is `G.localSiteName ?? network_name`.  This ensures that in a
/// multi-gateway network each gateway's overlay identifies the correct local
/// site.  A network with no `gatewayRefs` is a no-op.
///
/// **Watch limitation:** [`InferenceProvider`] and [`GridSite`] changes do
/// not currently trigger a [`GridNetwork`] reconcile.  Overlays are only
/// refreshed when the [`GridNetwork`] itself changes.  Adding cross-resource
/// watch/index from [`InferenceProvider`] and [`GridSite`] to their owning
/// [`GridNetwork`] is future work.
///
/// [`GridSite`]: crate::crd::grid_site::GridSite
#[expect(
    clippy::large_stack_frames,
    reason = "async future with kube API types and overlay data"
)]
async fn reconcile_routing_overlay(network: &GridNetwork, client: &Client) -> Result<(), OperatorError> {
    let network_name = network
        .metadata
        .name
        .as_deref()
        .unwrap_or_else(|| std::process::abort());

    let providers = list_all_inference_providers(client).await?;
    let sites = list_all_grid_sites(client).await?;

    for gw_ref in &network.spec.gateway_refs {
        // Each gateway identifies its own local site.  Fall back to the
        // network name for single-site deployments where the two are equal.
        let local_site = gw_ref.local_site_name.as_deref().unwrap_or(network_name);
        let overlay = routing_overlay::render_routing_overlay(network, &sites, &providers, local_site)
            .map_err(OperatorError::OverlayRender)?;
        apply_overlay_for_gateway(&overlay, network, gw_ref, client).await?;
    }
    Ok(())
}

/// List all [`InferenceProvider`] resources cluster-wide.
async fn list_all_inference_providers(client: &Client) -> Result<Vec<InferenceProvider>, OperatorError> {
    let api: Api<InferenceProvider> = Api::all(client.clone());
    let list = api.list(&ListParams::default()).await?;
    Ok(list.items)
}

/// List all [`GridSite`] resources cluster-wide.
///
/// [`GridSite`]: crate::crd::grid_site::GridSite
async fn list_all_grid_sites(client: &Client) -> Result<Vec<crate::crd::grid_site::GridSite>, OperatorError> {
    let api: Api<crate::crd::grid_site::GridSite> = Api::all(client.clone());
    let list = api.list(&ListParams::default()).await?;
    Ok(list.items)
}

/// Server-side apply one routing overlay `ConfigMap` for a single gateway.
async fn apply_overlay_for_gateway(
    overlay: &routing_overlay::RoutingOverlay,
    network: &GridNetwork,
    gw_ref: &GatewayRef,
    client: &Client,
) -> Result<(), OperatorError> {
    let network_name = network
        .metadata
        .name
        .as_deref()
        .unwrap_or_else(|| std::process::abort());

    let cm = routing_overlay::build_overlay_configmap(overlay, network_name, &gw_ref.name, &gw_ref.namespace)
        .map_err(OperatorError::Json)?;
    let cm_name = cm.metadata.name.as_deref().unwrap_or_else(|| std::process::abort());

    let api: Api<ConfigMap> = Api::namespaced(client.clone(), &gw_ref.namespace);
    api.patch(cm_name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(&cm))
        .await?;

    info!(cm_name, "applied routing overlay ConfigMap");
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
