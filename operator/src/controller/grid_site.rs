//! [`GridSite`] controller.
//!
//! Reconciles [`GridSite`] resources: validates the grid network
//! reference, manages lifecycle phase transitions, and maintains
//! the trust bundle secret. In Phase 1, lifecycle is manual
//! (no SWIM events).
//!
//! [`GridSite`]: crate::crd::grid_site::GridSite

use std::sync::Arc;

use kube::{
    Client,
    api::{Api, Patch, PatchParams},
    runtime::controller::Action,
};
use tokio::time::Duration;
use tracing::info;

use crate::{
    crd::{
        grid_network::GridNetwork,
        grid_site::{GridSite, GridSitePhase, GridSiteStatus},
    },
    error::OperatorError,
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

/// Reconcile a [`GridSite`] resource.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures.
pub async fn reconcile(site: Arc<GridSite>, client: Arc<Client>) -> Result<Action, OperatorError> {
    let name = site.metadata.name.as_deref().unwrap_or_else(|| std::process::abort());

    info!(name, "reconciling GridSite");

    validate_network_ref(&site, client.as_ref()).await?;

    let current_phase = site.status.as_ref().map_or(&GridSitePhase::Pending, |s| &s.phase);

    let next_phase = determine_phase(current_phase);
    update_status(&site, client.as_ref(), &next_phase).await?;

    Ok(Action::requeue(REQUEUE_INTERVAL))
}

/// Error policy for the [`GridSite`] controller.
pub fn error_policy(_site: Arc<GridSite>, error: &OperatorError, _ctx: Arc<Client>) -> Action {
    tracing::error!(%error, "GridSite reconciliation failed");
    Action::requeue(Duration::from_secs(30))
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Validate that the referenced `GridNetwork` exists.
async fn validate_network_ref(site: &GridSite, client: &Client) -> Result<(), OperatorError> {
    let api: Api<GridNetwork> = Api::all(client.clone());
    let network_name = &site.spec.grid_network_ref;
    api.get(network_name).await.map_err(|e| {
        tracing::warn!(error = %e, network = %network_name, "lookup failed");
        OperatorError::NotFound(format!("GridNetwork {network_name}"))
    })?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase Determination
// ---------------------------------------------------------------------------

/// Determine the next phase based on current state.
///
/// In Phase 1 (manual lifecycle), sites stay in their current
/// phase. SWIM-driven transitions come in Phase 2.
fn determine_phase(current: &GridSitePhase) -> GridSitePhase {
    current.clone()
}

// ---------------------------------------------------------------------------
// Status Update
// ---------------------------------------------------------------------------

/// Patch the `GridSite` status subresource.
async fn update_status(site: &GridSite, client: &Client, phase: &GridSitePhase) -> Result<(), OperatorError> {
    let name = site.metadata.name.as_deref().unwrap_or_else(|| std::process::abort());

    let api: Api<GridSite> = Api::all(client.clone());
    let status = GridSiteStatus {
        phase: phase.clone(),
        observed_generation: site.metadata.generation.unwrap_or(0),
        ..Default::default()
    };

    let patch = serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridSite",
        "status": status
    });

    api.patch_status(name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(patch))
        .await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn determine_phase_preserves_pending() {
        let result = determine_phase(&GridSitePhase::Pending);
        assert_eq!(result, GridSitePhase::Pending, "Pending must stay Pending in Phase 1");
    }

    #[test]
    fn determine_phase_preserves_discovered() {
        let result = determine_phase(&GridSitePhase::Discovered);
        assert_eq!(
            result,
            GridSitePhase::Discovered,
            "Discovered must stay Discovered in Phase 1"
        );
    }

    #[test]
    fn determine_phase_preserves_connecting() {
        let result = determine_phase(&GridSitePhase::Connecting);
        assert_eq!(
            result,
            GridSitePhase::Connecting,
            "Connecting must stay Connecting in Phase 1"
        );
    }

    #[test]
    fn determine_phase_preserves_active() {
        let result = determine_phase(&GridSitePhase::Active);
        assert_eq!(result, GridSitePhase::Active, "Active must stay Active in Phase 1");
    }

    #[test]
    fn determine_phase_preserves_unreachable() {
        let result = determine_phase(&GridSitePhase::Unreachable);
        assert_eq!(
            result,
            GridSitePhase::Unreachable,
            "Unreachable must stay Unreachable in Phase 1"
        );
    }

    #[test]
    fn determine_phase_preserves_left() {
        let result = determine_phase(&GridSitePhase::Left);
        assert_eq!(result, GridSitePhase::Left, "Left must stay Left in Phase 1");
    }
}
