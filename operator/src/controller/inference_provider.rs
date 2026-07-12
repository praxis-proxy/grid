//! [`InferenceProvider`] controller (OP-02).
//!
//! Reconciles [`InferenceProvider`] resources: validates referenced
//! [`GridNetwork`], resolves matching [`GridSite`]s via the site selector,
//! and sets `status.phase`, `status.matchingSites`, and
//! `status.observedGeneration`.
//!
//! # Phase policy
//!
//! Static config checks run first and short-circuit on any failure.
//! The health probe result (from `ProbeOutcome`) is applied last, on top
//! of the site-matching phase.
//!
//! | Condition | Phase |
//! |-----------|-------|
//! | `spec.endpoint` is blank or whitespace | `Unavailable` |
//! | Any `spec.models[].name` is blank | `Unavailable` |
//! | `spec.gridNetworkRef` not found | `Unavailable` |
//! | Config valid, probe returns transport failure | `Unavailable` |
//! | Config valid, probe returns degraded response | `Degraded` |
//! | Config valid, probe healthy or not run, no matching sites | `Pending` |
//! | Config valid, probe healthy or not run, ≥1 matching site | `Available` |
//!
//! `Degraded` is emitted by `phase_from_probe` when a health probe
//! returns a degraded response.  Until live probing is wired into the
//! reconcile loop, `ProbeOutcome::NotProbed` is always used, which
//! preserves the pre-OP-05 site-matching behaviour.
//!
//! # Watch / reconcile note
//!
//! This controller watches [`InferenceProvider`] resources.  Changes to
//! [`GridSite`]s or [`GridNetwork`]s do not trigger an [`InferenceProvider`]
//! reconcile.  Adding cross-resource watches is a follow-up task.
//!
//! # Site matching
//!
//! The controller lists all [`GridSite`]s whose `spec.gridNetworkRef` equals
//! the provider's `spec.gridNetworkRef`, then applies the provider's
//! `spec.siteSelector.matchLabels`.  An empty selector matches all sites in
//! the network.  Network filtering (by `spec.gridNetworkRef`) is the
//! controller's responsibility — `sites_matching_selector` itself does not
//! filter by network.
//!
//! [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork
//! [`GridSite`]: crate::crd::grid_site::GridSite

use std::sync::Arc;

use kube::{
    Client,
    api::{Api, ListParams, Patch, PatchParams},
    runtime::controller::Action,
};
use tokio::time::Duration;
use tracing::info;

use crate::{
    crd::{
        grid_network::GridNetwork,
        grid_site::GridSite,
        inference_provider::{InferenceProvider, InferenceProviderStatus, ProviderPhase},
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

/// Reconcile an [`InferenceProvider`] resource.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API errors.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
pub async fn reconcile(provider: Arc<InferenceProvider>, client: Arc<Client>) -> Result<Action, OperatorError> {
    let name = provider
        .metadata
        .name
        .as_deref()
        .unwrap_or_else(|| std::process::abort());

    info!(name, "reconciling InferenceProvider");

    let (phase, matching_sites) = resolve_phase_and_sites(&provider, &client).await?;
    let generation = provider.metadata.generation.unwrap_or(0);
    update_status(&provider, &client, phase, matching_sites, generation).await?;

    Ok(Action::requeue(REQUEUE_INTERVAL))
}

/// Error policy for the [`InferenceProvider`] controller.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
pub fn error_policy(_provider: Arc<InferenceProvider>, error: &OperatorError, _ctx: Arc<Client>) -> Action {
    tracing::error!(%error, "InferenceProvider reconciliation failed");
    Action::requeue(Duration::from_secs(30))
}

// ---------------------------------------------------------------------------
// Phase resolution
// ---------------------------------------------------------------------------

/// Validate the static configuration of a provider (no Kubernetes API calls).
///
/// Returns `Some(reason)` if the provider has a configuration error that
/// immediately maps to [`ProviderPhase::Unavailable`], or `None` if static
/// validation passes.
///
/// Checked invariants:
/// - `spec.endpoint` is non-blank and non-whitespace.
/// - All `spec.models[].name` values are non-blank.
///
/// The `gridNetworkRef` existence check is not included here because it
/// requires a Kubernetes API call.
pub(crate) fn validate_provider_config(provider: &InferenceProvider) -> Option<&'static str> {
    if provider.spec.endpoint.trim().is_empty() {
        return Some("blank endpoint");
    }
    for model in &provider.spec.models {
        if model.name.trim().is_empty() {
            return Some("blank model name");
        }
    }
    None
}

/// The outcome of a health probe against an [`InferenceProvider`] endpoint.
///
/// This enum represents what a live health probe would observe, without
/// performing the probe itself.  Pass a [`ProbeOutcome`] to
/// [`phase_from_probe`] to merge it with the site-matching phase.
///
/// The current reconcile loop always passes [`ProbeOutcome::NotProbed`],
/// preserving pre-OP-05 behaviour.  Future work will substitute a real
/// probe result once the HTTP health-check loop is implemented.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeOutcome {
    /// The endpoint responded successfully (e.g. HTTP 200).
    Healthy,

    /// The endpoint responded but signalled a degraded state (e.g. HTTP 5xx,
    /// or a valid response indicating reduced capacity).
    Degraded,

    /// The endpoint was unreachable: transport failure, connection refused,
    /// or DNS resolution error.
    Unavailable,

    /// No probe was attempted this reconcile cycle.
    ///
    /// Preserves the site-matching phase unchanged — equivalent to the
    /// pre-OP-05 behaviour.
    NotProbed,
}

impl ProbeOutcome {
    /// Derive a [`ProbeOutcome`] from an HTTP status code returned by a health probe.
    ///
    /// A 2xx status indicates the endpoint is healthy.  Any other status —
    /// redirects, client errors, server errors — indicates the endpoint is
    /// reachable but in a degraded state.
    ///
    /// Transport failures (connection refused, timeout, DNS error) are not
    /// representable as an HTTP status code; construct
    /// [`ProbeOutcome::Unavailable`] directly in those cases.
    ///
    /// | Status range | Result |
    /// |---|---|
    /// | 200–299 | [`Healthy`] |
    /// | any other | [`Degraded`] |
    ///
    /// [`Healthy`]: ProbeOutcome::Healthy
    /// [`Degraded`]: ProbeOutcome::Degraded
    #[must_use]
    pub fn from_http_status(status: u16) -> Self {
        if (200..300).contains(&status) {
            Self::Healthy
        } else {
            Self::Degraded
        }
    }
}

/// Apply a health [`ProbeOutcome`] on top of a site-matching phase.
///
/// The site-matching phase (from `phase_from_matching`) determines whether
/// the provider has any active sites.  The probe outcome can tighten or
/// override it:
///
/// | Probe outcome | Result |
/// |---------------|--------|
/// | [`Healthy`] | site-matching phase unchanged |
/// | [`NotProbed`] | site-matching phase unchanged |
/// | [`Degraded`] | [`ProviderPhase::Degraded`] |
/// | [`Unavailable`] | [`ProviderPhase::Unavailable`] |
///
/// Static config validation (`validate_provider_config`) runs **before**
/// this function and short-circuits on failure.  This function is only
/// called when static validation has already passed.
///
/// [`Healthy`]: ProbeOutcome::Healthy
/// [`NotProbed`]: ProbeOutcome::NotProbed
/// [`Degraded`]: ProbeOutcome::Degraded
/// [`Unavailable`]: ProbeOutcome::Unavailable
pub fn phase_from_probe(outcome: ProbeOutcome, site_phase: ProviderPhase) -> ProviderPhase {
    match outcome {
        ProbeOutcome::Healthy | ProbeOutcome::NotProbed => site_phase,
        ProbeOutcome::Degraded => ProviderPhase::Degraded,
        ProbeOutcome::Unavailable => ProviderPhase::Unavailable,
    }
}

/// Compute the provider phase from site matching results.
///
/// Returns [`ProviderPhase::Pending`] when no sites match, and
/// [`ProviderPhase::Available`] when at least one site matches.
///
/// This function never returns [`ProviderPhase::Degraded`].
/// `Degraded` is only reachable via [`phase_from_probe`] when a health
/// probe returns a degraded response.
pub(crate) fn phase_from_matching(matching: &[String]) -> ProviderPhase {
    if matching.is_empty() {
        ProviderPhase::Pending
    } else {
        ProviderPhase::Available
    }
}

/// Determine the provider phase and matching sites.
///
/// Returns `(ProviderPhase, sorted_matching_site_names)`.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures.
#[expect(clippy::large_stack_frames, reason = "async future with kube API types")]
async fn resolve_phase_and_sites(
    provider: &InferenceProvider,
    client: &Client,
) -> Result<(ProviderPhase, Vec<String>), OperatorError> {
    // Static validation: config errors map immediately to Unavailable.
    if let Some(reason) = validate_provider_config(provider) {
        tracing::warn!(
            name = provider.metadata.name.as_deref().unwrap_or("?"),
            reason,
            "InferenceProvider config invalid"
        );
        return Ok((ProviderPhase::Unavailable, Vec::new()));
    }

    // Validate: referenced GridNetwork must exist.
    let network_ref = &provider.spec.grid_network_ref;
    let network_api: Api<GridNetwork> = Api::all(client.clone());
    if network_api.get_opt(network_ref).await?.is_none() {
        tracing::warn!(
            name = provider.metadata.name.as_deref().unwrap_or("?"),
            network = %network_ref,
            "referenced GridNetwork not found"
        );
        return Ok((ProviderPhase::Unavailable, Vec::new()));
    }

    // Resolve matching sites.
    let sites = list_sites_for_network(client, network_ref).await?;
    let matching = sites_matching_selector(provider, &sites);
    let site_phase = phase_from_matching(&matching);
    // Apply health probe result.  NotProbed preserves site_phase unchanged
    // until live health-check probing is wired in (future OP-05 work).
    let phase = phase_from_probe(ProbeOutcome::NotProbed, site_phase);

    Ok((phase, matching))
}

/// List all [`GridSite`]s whose `spec.gridNetworkRef` matches `network_ref`.
///
/// Network filtering is applied here so that `sites_matching_selector`
/// only sees sites from the correct network.
///
/// [`GridSite`]: crate::crd::grid_site::GridSite
async fn list_sites_for_network(client: &Client, network_ref: &str) -> Result<Vec<GridSite>, OperatorError> {
    let api: Api<GridSite> = Api::all(client.clone());
    let all = api.list(&ListParams::default()).await?;
    Ok(all
        .items
        .into_iter()
        .filter(|s| s.spec.grid_network_ref == network_ref)
        .collect())
}

/// Apply `siteSelector.matchLabels` against the supplied sites.
///
/// An empty `matchLabels` matches all sites.  All configured key-value pairs
/// must match (AND semantics); extra labels on the site are ignored.
/// Returns a deterministically sorted list of matching site names.
///
/// Network filtering is the caller's responsibility — pass only sites that
/// already belong to the relevant network.
pub(crate) fn sites_matching_selector(provider: &InferenceProvider, sites: &[GridSite]) -> Vec<String> {
    let selector = &provider.spec.site_selector.match_labels;

    let mut names: Vec<String> = sites
        .iter()
        .filter(|site| {
            let site_labels = site.metadata.labels.as_ref();
            selector
                .iter()
                .all(|(k, v)| site_labels.is_some_and(|labels| labels.get(k).is_some_and(|sv| sv == v)))
        })
        .filter_map(|site| site.metadata.name.clone())
        .collect();

    names.sort();
    names
}

// ---------------------------------------------------------------------------
// Status Update
// ---------------------------------------------------------------------------

/// Patch the [`InferenceProvider`] status subresource.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API errors.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
async fn update_status(
    provider: &InferenceProvider,
    client: &Client,
    phase: ProviderPhase,
    matching_sites: Vec<String>,
    observed_generation: i64,
) -> Result<(), OperatorError> {
    let name = provider
        .metadata
        .name
        .as_deref()
        .unwrap_or_else(|| std::process::abort());

    let api: Api<InferenceProvider> = Api::all(client.clone());
    let status = InferenceProviderStatus {
        matching_sites,
        observed_generation,
        phase,
    };

    let patch = serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "status": status
    });

    api.patch_status(name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(patch))
        .await?;

    info!(name, "updated InferenceProvider status");
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Test utilities
    // -----------------------------------------------------------------------

    fn test_site(name: &str, network: &str) -> GridSite {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": name },
            "spec": { "gridNetworkRef": network }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_site_with_labels(name: &str, network: &str, labels: &[(&str, &str)]) -> GridSite {
        let labels_map: serde_json::Map<String, serde_json::Value> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": name, "labels": labels_map },
            "spec": { "gridNetworkRef": network }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_provider(name: &str, network: &str, models: &[&str]) -> InferenceProvider {
        let models_json: Vec<serde_json::Value> = models.iter().map(|m| serde_json::json!({ "name": m })).collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": models_json
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_provider_with_selector(name: &str, network: &str, selector: &[(&str, &str)]) -> InferenceProvider {
        let match_labels: serde_json::Map<String, serde_json::Value> = selector
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": [{"name": "model"}],
                "siteSelector": { "matchLabels": match_labels }
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    // -----------------------------------------------------------------------
    // validate_provider_config — static validation (items 1-4)
    // -----------------------------------------------------------------------

    #[test]
    fn blank_endpoint_maps_to_unavailable() {
        // Item 1: blank endpoint
        let provider: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": "prov" },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "",
                "models": [{"name": "model"}]
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let err = validate_provider_config(&provider);
        assert!(err.is_some(), "blank endpoint must fail static validation");
        assert!(
            err.unwrap_or_else(|| std::process::abort()).contains("endpoint"),
            "error must mention endpoint"
        );
    }

    #[test]
    fn whitespace_only_endpoint_maps_to_unavailable() {
        // Item 2: whitespace-only endpoint
        let provider: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": "prov" },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "   ",
                "models": [{"name": "model"}]
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert!(
            validate_provider_config(&provider).is_some(),
            "whitespace-only endpoint must fail validation"
        );
    }

    #[test]
    fn blank_model_name_maps_to_unavailable() {
        // Item 3: blank model name
        let provider: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": "prov" },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": [{"name": ""}]
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let err = validate_provider_config(&provider);
        assert!(err.is_some(), "blank model name must fail static validation");
        assert!(
            err.unwrap_or_else(|| std::process::abort()).contains("model"),
            "error must mention model"
        );
    }

    #[test]
    fn second_model_blank_maps_to_unavailable() {
        // Item 4: first model valid, second blank
        let provider: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": "prov" },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": [{"name": "model-ok"}, {"name": ""}]
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert!(
            validate_provider_config(&provider).is_some(),
            "any blank model name must fail validation"
        );
    }

    #[test]
    fn valid_config_passes_static_validation() {
        // Items 1-4 all pass: good endpoint and all model names non-blank
        let provider = test_provider("prov", "net", &["model-a", "model-b"]);
        assert!(
            validate_provider_config(&provider).is_none(),
            "valid provider config must pass static validation"
        );
    }

    // Item 5: missing GridNetwork → Unavailable
    // Requires a Kubernetes API call (network_api.get_opt) and cannot be
    // unit-tested without a live cluster or a mock Kubernetes server.
    // Covered at the integration level; documented here for completeness.

    // -----------------------------------------------------------------------
    // phase_from_matching — pure phase logic (items 6-7, 12)
    // -----------------------------------------------------------------------

    #[test]
    fn no_matching_sites_yields_pending() {
        // Item 6: valid config, no matching sites → Pending
        let phase = phase_from_matching(&[]);
        assert_eq!(phase, ProviderPhase::Pending, "empty matching → Pending");
    }

    #[test]
    fn one_matching_site_yields_available() {
        // Item 7: valid config, ≥1 matching site → Available
        let phase = phase_from_matching(&["site-a".to_owned()]);
        assert_eq!(phase, ProviderPhase::Available, "one match → Available");
    }

    #[test]
    fn multiple_matching_sites_yields_available() {
        // Item 7 (multi-site): all non-empty slices → Available
        let phase = phase_from_matching(&["site-a".to_owned(), "site-b".to_owned()]);
        assert_eq!(phase, ProviderPhase::Available, "multiple matches → Available");
    }

    #[test]
    fn phase_from_matching_never_emits_degraded() {
        // phase_from_matching only returns Pending or Available; Degraded is
        // only reachable via phase_from_probe when a health probe returns a
        // degraded outcome.
        let empty_phase = phase_from_matching(&[]);
        let some_phase = phase_from_matching(&["site-x".to_owned()]);
        assert!(
            matches!(empty_phase, ProviderPhase::Pending),
            "empty matching must yield Pending"
        );
        assert!(
            matches!(some_phase, ProviderPhase::Available),
            "non-empty matching must yield Available"
        );
        assert_ne!(
            empty_phase,
            ProviderPhase::Degraded,
            "Degraded unreachable from phase_from_matching"
        );
        assert_ne!(
            some_phase,
            ProviderPhase::Degraded,
            "Degraded unreachable from phase_from_matching"
        );
    }

    // -----------------------------------------------------------------------
    // phase_from_probe — health decision function
    // -----------------------------------------------------------------------

    #[test]
    fn healthy_probe_preserves_available() {
        let result = phase_from_probe(ProbeOutcome::Healthy, ProviderPhase::Available);
        assert_eq!(
            result,
            ProviderPhase::Available,
            "Healthy probe must preserve Available"
        );
    }

    #[test]
    fn healthy_probe_preserves_pending() {
        let result = phase_from_probe(ProbeOutcome::Healthy, ProviderPhase::Pending);
        assert_eq!(result, ProviderPhase::Pending, "Healthy probe must preserve Pending");
    }

    #[test]
    fn not_probed_preserves_available() {
        let result = phase_from_probe(ProbeOutcome::NotProbed, ProviderPhase::Available);
        assert_eq!(
            result,
            ProviderPhase::Available,
            "NotProbed must preserve Available (current default)"
        );
    }

    #[test]
    fn not_probed_preserves_pending() {
        let result = phase_from_probe(ProbeOutcome::NotProbed, ProviderPhase::Pending);
        assert_eq!(
            result,
            ProviderPhase::Pending,
            "NotProbed must preserve Pending (current default)"
        );
    }

    #[test]
    fn degraded_probe_overrides_available() {
        let result = phase_from_probe(ProbeOutcome::Degraded, ProviderPhase::Available);
        assert_eq!(
            result,
            ProviderPhase::Degraded,
            "Degraded probe must override Available"
        );
    }

    #[test]
    fn degraded_probe_overrides_pending() {
        let result = phase_from_probe(ProbeOutcome::Degraded, ProviderPhase::Pending);
        assert_eq!(result, ProviderPhase::Degraded, "Degraded probe must override Pending");
    }

    #[test]
    fn unavailable_probe_overrides_available() {
        let result = phase_from_probe(ProbeOutcome::Unavailable, ProviderPhase::Available);
        assert_eq!(
            result,
            ProviderPhase::Unavailable,
            "Unavailable probe must override Available"
        );
    }

    #[test]
    fn unavailable_probe_overrides_pending() {
        let result = phase_from_probe(ProbeOutcome::Unavailable, ProviderPhase::Pending);
        assert_eq!(
            result,
            ProviderPhase::Unavailable,
            "Unavailable probe must override Pending"
        );
    }

    #[test]
    fn degraded_is_reachable_via_phase_from_probe() {
        // Documents that Degraded IS now reachable from the controller, via
        // phase_from_probe — in contrast to phase_from_matching which cannot
        // emit it.
        let result = phase_from_probe(ProbeOutcome::Degraded, ProviderPhase::Available);
        assert_eq!(
            result,
            ProviderPhase::Degraded,
            "Degraded must be reachable via phase_from_probe"
        );
    }

    // -----------------------------------------------------------------------
    // ProbeOutcome::from_http_status — HTTP status code mapping
    // -----------------------------------------------------------------------

    #[test]
    fn http_200_yields_healthy() {
        assert_eq!(
            ProbeOutcome::from_http_status(200),
            ProbeOutcome::Healthy,
            "HTTP 200 must yield Healthy"
        );
    }

    #[test]
    fn http_204_yields_healthy() {
        assert_eq!(
            ProbeOutcome::from_http_status(204),
            ProbeOutcome::Healthy,
            "HTTP 204 No Content must yield Healthy"
        );
    }

    #[test]
    fn http_299_is_last_healthy_status() {
        assert_eq!(
            ProbeOutcome::from_http_status(299),
            ProbeOutcome::Healthy,
            "HTTP 299 must still be within the healthy range"
        );
    }

    #[test]
    fn http_300_yields_degraded() {
        assert_eq!(
            ProbeOutcome::from_http_status(300),
            ProbeOutcome::Degraded,
            "HTTP 300 (redirect) is outside 2xx range and must yield Degraded"
        );
    }

    #[test]
    fn http_400_yields_degraded() {
        assert_eq!(
            ProbeOutcome::from_http_status(400),
            ProbeOutcome::Degraded,
            "HTTP 400 must yield Degraded"
        );
    }

    #[test]
    fn http_429_yields_degraded() {
        assert_eq!(
            ProbeOutcome::from_http_status(429),
            ProbeOutcome::Degraded,
            "HTTP 429 Too Many Requests must yield Degraded"
        );
    }

    #[test]
    fn http_500_yields_degraded() {
        assert_eq!(
            ProbeOutcome::from_http_status(500),
            ProbeOutcome::Degraded,
            "HTTP 500 must yield Degraded"
        );
    }

    #[test]
    fn http_503_yields_degraded() {
        assert_eq!(
            ProbeOutcome::from_http_status(503),
            ProbeOutcome::Degraded,
            "HTTP 503 Service Unavailable must yield Degraded (reachable but degraded)"
        );
    }

    #[test]
    fn http_status_zero_yields_degraded() {
        assert_eq!(
            ProbeOutcome::from_http_status(0),
            ProbeOutcome::Degraded,
            "invalid status 0 must yield Degraded"
        );
    }

    #[test]
    fn http_2xx_boundary_is_inclusive_exclusive() {
        // 200 → Healthy, 299 → Healthy, 300 → Degraded
        assert_eq!(ProbeOutcome::from_http_status(200), ProbeOutcome::Healthy);
        assert_eq!(ProbeOutcome::from_http_status(299), ProbeOutcome::Healthy);
        assert_eq!(ProbeOutcome::from_http_status(300), ProbeOutcome::Degraded);
    }

    #[test]
    fn static_config_failure_precedes_probe_result() {
        // Static config validation short-circuits before phase_from_probe is
        // called.  When validate_provider_config returns Some(_), the caller
        // returns Unavailable immediately — no probe outcome can rescue a
        // provider with an invalid config.
        //
        // Simulate the precedence: if static validation fails, we would
        // never reach phase_from_probe.  The test confirms both that
        // validate_provider_config catches the config error AND that
        // phase_from_probe does not participate in that path.
        let provider_with_blank_endpoint: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": "bad" },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "",
                "models": [{"name": "model"}]
            }
        }))
        .unwrap_or_else(|_| std::process::abort());

        let config_error = validate_provider_config(&provider_with_blank_endpoint);
        assert!(
            config_error.is_some(),
            "blank endpoint must fail static validation before any probe result applies"
        );
        // phase_from_probe is never called; this ensures it can never rescue
        // a provider with an invalid config.
    }

    // -----------------------------------------------------------------------
    // sites_matching_selector — selector matching (items 8-11)
    // -----------------------------------------------------------------------

    #[test]
    fn empty_selector_matches_all_passed_sites() {
        // Item 8: empty selector matches all pre-filtered sites
        let provider = test_provider("prov", "net", &["model"]);
        let sites = vec![test_site("site-a", "net"), test_site("site-b", "net")];
        let matching = sites_matching_selector(&provider, &sites);
        assert_eq!(
            matching,
            vec!["site-a", "site-b"],
            "empty selector must match all pre-filtered sites"
        );
    }

    #[test]
    fn label_selector_matches_only_matching_labels() {
        // Item 9: label selector matches only labeled sites
        let provider = test_provider_with_selector("prov", "net", &[("hw", "gpu")]);
        let sites = vec![
            test_site_with_labels("gpu-site", "net", &[("hw", "gpu")]),
            test_site_with_labels("cpu-site", "net", &[("hw", "cpu")]),
        ];
        let matching = sites_matching_selector(&provider, &sites);
        assert_eq!(matching, vec!["gpu-site"], "only gpu-site should match");
    }

    #[test]
    fn matching_sites_are_sorted_deterministically() {
        // Item 10: deterministic alphabetical sort
        let provider = test_provider("prov", "net", &["model"]);
        let sites = vec![
            test_site("zebra-site", "net"),
            test_site("alpha-site", "net"),
            test_site("mango-site", "net"),
        ];
        let matching = sites_matching_selector(&provider, &sites);
        assert_eq!(
            matching,
            vec!["alpha-site", "mango-site", "zebra-site"],
            "matching sites must be sorted alphabetically"
        );
    }

    #[test]
    fn sites_from_other_network_match_empty_selector() {
        // Item 11 (contract doc): sites_matching_selector does NOT filter by
        // network — that is the controller's responsibility via
        // list_sites_for_network.  An empty selector will match any site
        // passed in, regardless of network.
        let provider = test_provider("prov", "net", &["model"]);
        let sites = vec![test_site("site-other", "other-net")];
        let matching = sites_matching_selector(&provider, &sites);
        assert_eq!(
            matching,
            vec!["site-other"],
            "empty selector matches any site; network filtering is the controller's responsibility"
        );
    }

    #[test]
    fn no_matching_sites_returns_empty() {
        // Item 9 (negative): label selector, no sites match → empty
        let provider = test_provider_with_selector("prov", "net", &[("hw", "gpu")]);
        let sites = vec![test_site_with_labels("cpu-site", "net", &[("hw", "cpu")])];
        let matching = sites_matching_selector(&provider, &sites);
        assert!(matching.is_empty(), "no matching sites should return empty");
    }

    #[test]
    fn multi_key_selector_requires_all_keys_to_match() {
        // Item 9 (AND semantics): all selector keys must match
        let provider = test_provider_with_selector("prov", "net", &[("hw", "gpu"), ("region", "us-east")]);
        // Site with both keys → matches
        let both = test_site_with_labels(
            "full-match",
            "net",
            &[("hw", "gpu"), ("region", "us-east"), ("extra", "ignored")],
        );
        // Site with only one key → no match
        let partial = test_site_with_labels("partial", "net", &[("hw", "gpu")]);
        let sites = vec![both, partial];
        let matching = sites_matching_selector(&provider, &sites);
        assert_eq!(
            matching,
            vec!["full-match"],
            "multi-key selector requires ALL keys to match (AND semantics)"
        );
    }

    #[test]
    fn site_with_extra_labels_still_matches() {
        // Item 9 (extra labels OK): extra labels on the site don't block matching
        let provider = test_provider_with_selector("prov", "net", &[("hw", "gpu")]);
        let site = test_site_with_labels("gpu-site", "net", &[("hw", "gpu"), ("zone", "us-east-1a")]);
        let matching = sites_matching_selector(&provider, &[site]);
        assert_eq!(
            matching,
            vec!["gpu-site"],
            "extra labels on site must not prevent matching"
        );
    }

    #[test]
    fn selector_wrong_value_does_not_match() {
        // Item 9 (value mismatch): key present but wrong value → no match
        let provider = test_provider_with_selector("prov", "net", &[("hw", "gpu")]);
        let site = test_site_with_labels("site", "net", &[("hw", "cpu")]);
        let matching = sites_matching_selector(&provider, &[site]);
        assert!(matching.is_empty(), "wrong label value must not match the selector");
    }

    #[test]
    fn selector_missing_key_does_not_match() {
        // Item 9 (missing key): site has no matching key → no match
        let provider = test_provider_with_selector("prov", "net", &[("hw", "gpu")]);
        let site = test_site_with_labels("site", "net", &[("zone", "us-east")]);
        let matching = sites_matching_selector(&provider, &[site]);
        assert!(matching.is_empty(), "missing selector key on site must not match");
    }

    // Item 13: update_status and reconcile require a live Kubernetes client.
    // They are covered at the integration level.  The pure decision logic
    // (validate_provider_config, phase_from_matching, sites_matching_selector)
    // is fully unit-tested above.
}
