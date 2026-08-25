//! [`AgentToolProvider`] controller.
//!
//! Reconciles [`AgentToolProvider`] resources: validates the static spec,
//! resolves the referenced [`GridNetwork`], resolves matching [`GridSite`]s
//! via the site selector, live-probes the endpoint's MCP `tools/list`
//! contract, and sets `status.phase`, `status.matchingSites`,
//! `status.discoveredTools`, `status.reason`, and `status.observedGeneration`.
//!
//! Structured exactly like [`inference_provider`](crate::controller::inference_provider):
//! static validation short-circuits first, then `GridNetwork`/site
//! resolution, then the live probe outcome merges on top.
//!
//! [`AgentToolProvider`]: crate::crd::agent_tool_provider::AgentToolProvider
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork
//! [`GridSite`]: crate::crd::grid_site::GridSite

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use k8s_openapi::api::core::v1::ObjectReference;
use kube::{
    Client, Resource as _,
    api::{Api, ListParams, Patch, PatchParams},
    runtime::{
        controller::Action,
        events::{Event, EventType, Recorder, Reporter},
    },
};
use tracing::info;

use crate::{
    crd::{
        agent_tool_provider::{AgentToolProvider, AgentToolProviderStatus},
        grid_network::GridNetwork,
        grid_site::GridSite,
        inference_provider::ProviderPhase,
    },
    error::OperatorError,
    resources::{
        credentials::{self, CredentialPlan, CredentialResolver as _, KubernetesSecretResolver},
        mcp_probe,
    },
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Requeue interval after a successful reconciliation.
///
/// Matches [`inference_provider`](crate::controller::inference_provider)'s
/// default — no `healthCheck.interval`-equivalent config exists on
/// [`AgentToolProviderSpec`](crate::crd::agent_tool_provider::AgentToolProviderSpec) yet.
const REQUEUE_INTERVAL: Duration = Duration::from_secs(300);

/// Total bounded wall-clock budget for the live MCP probe: DNS resolution,
/// TLS Secret material reads, connect/handshake, and the `tools/list` call
/// combined — enforced as a single outer timeout in
/// [`mcp_probe::probe_agent_tool_provider`], not summed/multiplied across
/// phases.
///
/// [`AgentToolProviderSpec`](crate::crd::agent_tool_provider::AgentToolProviderSpec)
/// has no `healthCheck.timeout`-equivalent field yet, so this is a fixed
/// constant rather than a per-resource override — revisit if a future CRD
/// revision adds one.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Field manager name for server-side apply.
const FIELD_MANAGER: &str = "grid-operator";

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

/// Reconcile an [`AgentToolProvider`] resource.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API errors.
pub async fn reconcile(provider: Arc<AgentToolProvider>, client: Arc<Client>) -> Result<Action, OperatorError> {
    let name = provider
        .metadata
        .name
        .as_deref()
        .unwrap_or_else(|| std::process::abort());

    info!(name, "reconciling AgentToolProvider");

    let reporter = Reporter {
        controller: "agent-tool-provider-controller".into(),
        instance: None,
    };
    let object_ref = provider.object_ref(&());
    let recorder = Recorder::new(client.as_ref().clone(), reporter);

    let (phase, matching_sites, reason, discovered_tools) =
        Box::pin(resolve_phase_and_sites(&provider, &client)).await?;
    let generation = provider.metadata.generation.unwrap_or(0);
    update_status(
        &provider,
        &client,
        phase,
        matching_sites,
        discovered_tools,
        generation,
        reason,
        &recorder,
        &object_ref,
    )
    .await?;

    Ok(Action::requeue(REQUEUE_INTERVAL))
}

/// Error policy for the [`AgentToolProvider`] controller.
pub fn error_policy(_provider: Arc<AgentToolProvider>, error: &OperatorError, _ctx: Arc<Client>) -> Action {
    tracing::error!(%error, "AgentToolProvider reconciliation failed");
    Action::requeue(Duration::from_secs(30))
}

// ---------------------------------------------------------------------------
// Static validation
// ---------------------------------------------------------------------------

/// Validate the static configuration of a provider (no Kubernetes API calls).
///
/// Returns `Some(reason)` if the provider has a configuration error that
/// immediately maps to `Unavailable`, or `None` if static validation passes.
///
/// Business rule: a provider must have both a non-blank `endpoint` and a
/// non-blank `gridNetworkRef` to be considered configured. The
/// `gridNetworkRef` *existence* check (does the referenced `GridNetwork`
/// actually exist) requires a Kubernetes API call and is not part of this
/// pure function — see resolution in `resolve_phase_and_sites`.
pub(crate) fn validate_provider_config(provider: &AgentToolProvider) -> Option<&'static str> {
    if provider.spec.endpoint.trim().is_empty() {
        return Some("blank endpoint");
    }
    if provider.spec.grid_network_ref.trim().is_empty() {
        return Some("blank gridNetworkRef");
    }
    None
}

// ---------------------------------------------------------------------------
// Site resolution and matching
// ---------------------------------------------------------------------------

/// Compute the provider phase from site matching results.
///
/// Business rule: a provider is only actionable once at least one site
/// matches its selector. Returns [`ProviderPhase::Pending`] when no sites
/// match, and [`ProviderPhase::Available`] when at least one site matches.
///
/// This function never returns [`ProviderPhase::Degraded`] or
/// [`ProviderPhase::Unavailable`] — those are only reachable via the
/// config/`GridNetwork`-missing short-circuits in `resolve_phase_and_sites`,
/// or the live probe outcome merge (`phase_and_reason_from_probe`).
pub(crate) fn phase_from_matching(matching: &[String]) -> ProviderPhase {
    if matching.is_empty() {
        ProviderPhase::Pending
    } else {
        ProviderPhase::Available
    }
}

/// Apply `siteSelector.matchLabels` against the supplied sites.
///
/// An empty `matchLabels` matches all sites. All configured key-value pairs
/// must match (AND semantics); extra labels on the site are ignored.
/// Returns a deterministically sorted list of matching site names.
///
/// Network filtering (by `spec.gridNetworkRef`) is the caller's
/// responsibility — this function does not filter by network, mirroring
/// [`inference_provider::sites_matching_selector`](crate::controller::inference_provider::sites_matching_selector).
pub(crate) fn sites_matching_selector(provider: &AgentToolProvider, sites: &[GridSite]) -> Vec<String> {
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

/// List all [`GridSite`]s whose `spec.gridNetworkRef` matches `network_ref`.
///
/// Network filtering is applied here so that [`sites_matching_selector`]
/// only sees sites from the correct network.
async fn list_sites_for_network(client: &Client, network_ref: &str) -> Result<Vec<GridSite>, OperatorError> {
    let api: Api<GridSite> = Api::all(client.clone());
    let all = api.list(&ListParams::default()).await?;
    Ok(all
        .items
        .into_iter()
        .filter(|s| s.spec.grid_network_ref == network_ref)
        .collect())
}

/// Static config validation plus the `GridNetwork`-existence check, both of
/// which short-circuit straight to `Unavailable` before any site or probe
/// work runs.
///
/// Unlike `inference_provider.rs` (which leaves status.reason as None for
/// its equivalent checks), `AgentToolProvider` populates a stable reason
/// here: grid#9 requires transition evidence with bounded cardinality
/// across Events/metrics/logs, and a stable reason string is the shared
/// label all three channels key off in `update_status`.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures.
async fn static_config_failure_reason(
    provider: &AgentToolProvider,
    client: &Client,
    name: &str,
) -> Result<Option<&'static str>, OperatorError> {
    if let Some(config_error) = validate_provider_config(provider) {
        tracing::warn!(name, reason = config_error, "AgentToolProvider config invalid");
        return Ok(Some("ProviderConfigInvalid"));
    }

    let network_ref = &provider.spec.grid_network_ref;
    let network_api: Api<GridNetwork> = Api::all(client.clone());
    if network_api.get_opt(network_ref).await?.is_none() {
        tracing::warn!(name, network = %network_ref, "referenced GridNetwork not found");
        return Ok(Some("GridNetworkNotFound"));
    }

    Ok(None)
}

/// Determine the provider phase, matching sites, optional failure reason,
/// and discovered tool names.
///
/// Returns `(ProviderPhase, sorted_matching_site_names, Option<status_reason>, discovered_tools)`.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures.
async fn resolve_phase_and_sites(
    provider: &AgentToolProvider,
    client: &Client,
) -> Result<(ProviderPhase, Vec<String>, Option<String>, Vec<String>), OperatorError> {
    let name = provider.metadata.name.as_deref().unwrap_or("?");
    let previous_tools = provider
        .status
        .as_ref()
        .map_or_else(Vec::new, |s| s.discovered_tools.clone());

    if let Some(reason) = static_config_failure_reason(provider, client, name).await? {
        return Ok((
            ProviderPhase::Unavailable,
            Vec::new(),
            Some(reason.to_owned()),
            previous_tools,
        ));
    }

    let sites = list_sites_for_network(client, &provider.spec.grid_network_ref).await?;
    let matching = sites_matching_selector(provider, &sites);
    let site_phase = phase_from_matching(&matching);

    if site_phase != ProviderPhase::Available {
        // No sites match (yet) — nothing to probe. Mirrors
        // inference_provider's semantics: the health probe never runs
        // before the resource has anything to reach.
        return Ok((site_phase, matching, None, previous_tools));
    }

    Box::pin(probe_and_merge(provider, client, name, matching, previous_tools)).await
}

/// Outcome of resolving `spec.auth` into a probe-ready credential, before
/// the live probe itself runs.
enum CredentialProbeInput {
    /// Credentials (if any) resolved cleanly; the probe may proceed.
    Ready(Option<credentials::BearerToken>),
    /// Config or Secret resolution failed; carries the stable `status.reason`.
    Failed(String),
}

/// Resolve `spec.auth` into a [`CredentialProbeInput`]: parse the plan,
/// verify the referenced Secret is accessible, and (for `bearer_token`)
/// resolve the token value.
///
/// Split out of [`probe_and_merge`] purely to keep both functions within the
/// project's complexity lints — this is the credential half of what was
/// previously one larger function.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures.
async fn resolve_probe_credentials(
    provider: &AgentToolProvider,
    client: &Client,
    name: &str,
) -> Result<CredentialProbeInput, OperatorError> {
    let plan = match credentials::credential_plan_from_auth(provider.spec.auth.as_ref()) {
        Ok(plan) => plan,
        Err(_parse_err) => {
            let cr = credentials::credential_failure_reason_for_auth(provider.spec.auth.as_ref());
            tracing::warn!(name, reason = cr.as_str(), "AgentToolProvider auth config invalid");
            return Ok(CredentialProbeInput::Failed(cr.as_str().to_owned()));
        },
    };

    if let Some(cr) = credentials::verify_credential_accessible(client, &plan).await? {
        tracing::warn!(
            name,
            reason = cr.as_str(),
            "AgentToolProvider credential Secret inaccessible"
        );
        return Ok(CredentialProbeInput::Failed(cr.as_str().to_owned()));
    }

    let CredentialPlan::Bearer(bearer_ref) = &plan else {
        return Ok(CredentialProbeInput::Ready(None));
    };

    let resolver = KubernetesSecretResolver::new(client.clone());
    match resolver.resolve(bearer_ref).await {
        Ok(token) => Ok(CredentialProbeInput::Ready(Some(token))),
        Err(error) => {
            tracing::warn!(name, %error, "AgentToolProvider bearer token resolution failed");
            Ok(CredentialProbeInput::Failed("CredentialSecretMissing".to_owned()))
        },
    }
}

/// Resolve credentials and run the live MCP probe, merging its outcome into
/// the final `(phase, matching_sites, reason, discovered_tools)` tuple.
///
/// Split out of [`resolve_phase_and_sites`] to keep that function within the
/// project's complexity lints once credential resolution and probing are
/// both inline.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures.
async fn probe_and_merge(
    provider: &AgentToolProvider,
    client: &Client,
    name: &str,
    matching: Vec<String>,
    previous_tools: Vec<String>,
) -> Result<(ProviderPhase, Vec<String>, Option<String>, Vec<String>), OperatorError> {
    let token = match Box::pin(resolve_probe_credentials(provider, client, name)).await? {
        CredentialProbeInput::Ready(token) => token,
        CredentialProbeInput::Failed(reason) => {
            return Ok((ProviderPhase::Unavailable, matching, Some(reason), previous_tools));
        },
    };

    let probe_started = Instant::now();
    let outcome = Box::pin(mcp_probe::probe_agent_tool_provider(
        client,
        mcp_probe::ProbeRequest {
            endpoint: &provider.spec.endpoint,
            timeout: PROBE_TIMEOUT,
            tls_config: provider.spec.tls.as_ref(),
            provider_identity: name,
            auth_token: token.as_ref(),
        },
    ))
    .await;
    crate::metrics::record_mcp_probe(mcp_probe::mcp_probe_outcome_label(&outcome), probe_started.elapsed());

    let (probe_phase, probe_reason) = mcp_probe::phase_and_reason_from_probe(&outcome);
    let discovered = mcp_probe::discovered_tools_after_probe(&previous_tools, &outcome);

    Ok((probe_phase, matching, probe_reason, discovered))
}

// ---------------------------------------------------------------------------
// Status Update
// ---------------------------------------------------------------------------

/// Patch the [`AgentToolProvider`] status subresource.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API errors.
#[expect(
    clippy::too_many_arguments,
    reason = "all parameters are distinct reconcile outputs plus telemetry sinks; no logical grouping reduces them"
)]
#[expect(
    clippy::large_stack_frames,
    reason = "async future with kube API types; mirrors grid_site.rs precedent"
)]
#[expect(
    clippy::too_many_lines,
    reason = "status construction, patch-if-changed short-circuit, and the transition-telemetry call each need to \
              stay inline for the flow to read top-to-bottom; the telemetry body itself is already split out into \
              emit_transition_telemetry"
)]
async fn update_status(
    provider: &AgentToolProvider,
    client: &Client,
    phase: ProviderPhase,
    matching_sites: Vec<String>,
    discovered_tools: Vec<String>,
    observed_generation: i64,
    reason: Option<String>,
    recorder: &Recorder,
    object_ref: &ObjectReference,
) -> Result<(), OperatorError> {
    let name = provider
        .metadata
        .name
        .as_deref()
        .unwrap_or_else(|| std::process::abort());

    let existing = provider.status.as_ref();
    let current_phase = existing.map(|s| &s.phase);
    let phase_changed = current_phase != Some(&phase);
    let reason_changed = existing.map(|s| &s.reason) != Some(&reason);

    let api: Api<AgentToolProvider> = Api::all(client.clone());
    let status = AgentToolProviderStatus {
        discovered_tools,
        matching_sites,
        observed_generation,
        phase,
        reason,
    };

    if !agent_tool_provider_status_needs_update(existing, &status) {
        return Ok(());
    }

    let patch = serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "AgentToolProvider",
        "status": status
    });

    api.patch_status(name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(patch))
        .await?;

    info!(name, "updated AgentToolProvider status");

    // Events, metrics, and transition-level logs only fire on a real phase
    // or reason transition — never on a matchingSites/discoveredTools-only
    // patch — so a converged provider being repeatedly re-listed doesn't
    // spam the Event feed or inflate the phase-transition counter.
    if is_real_transition(phase_changed, reason_changed) {
        emit_transition_telemetry(name, current_phase, &status, recorder, object_ref).await;
    }

    Ok(())
}

/// Emit the Event, metric, and transition-level log for a real phase or
/// reason transition.
///
/// Split out of [`update_status`] purely to keep that function within the
/// project's line/complexity lints — this is the transition-telemetry half
/// of what was previously one larger function, called only once, from the
/// `is_real_transition` branch.
async fn emit_transition_telemetry(
    name: &str,
    current_phase: Option<&ProviderPhase>,
    status: &AgentToolProviderStatus,
    recorder: &Recorder,
    object_ref: &ObjectReference,
) {
    let label = telemetry_reason_label(&status.phase, status.reason.as_deref());
    let from_label = current_phase.map_or("None", phase_label);
    let to_label = phase_label(&status.phase);

    tracing::info!(name, previous_phase = ?current_phase, phase = ?status.phase, reason = label, "AgentToolProvider phase transition");

    if let Err(e) = recorder
        .publish(
            &Event {
                type_: event_type_for_reason(label),
                reason: label.to_owned(),
                note: status.reason.clone(),
                action: "Reconcile".to_owned(),
                secondary: None,
            },
            object_ref,
        )
        .await
    {
        tracing::warn!(error = %e, "failed to publish AgentToolProvider event");
    }

    crate::metrics::record_agent_tool_provider_phase_transition(from_label, to_label, label);
}

/// Render a [`ProviderPhase`] as its bounded-cardinality metric/log label.
fn phase_label(phase: &ProviderPhase) -> &'static str {
    match phase {
        ProviderPhase::Pending => "Pending",
        ProviderPhase::Available => "Available",
        ProviderPhase::Degraded => "Degraded",
        ProviderPhase::Unavailable => "Unavailable",
    }
}

/// Synthesize a bounded telemetry reason label for Events, metrics, and logs.
///
/// Business rule: when `status.reason` is set (an unhealthy phase with a
/// diagnostic code — see [`AgentToolProviderStatus`]'s doc comment), that
/// code *is* the telemetry label, keeping a single source of truth between
/// what a user reads on the CR and what appears in Events/metrics. When
/// `status.reason` is `None` (a healthy phase), this synthesizes one of two
/// bounded labels from the phase alone, since [`AgentToolProviderStatus`]
/// deliberately never sets `reason` while healthy.
fn telemetry_reason_label<'reason>(phase: &ProviderPhase, status_reason: Option<&'reason str>) -> &'reason str {
    match status_reason {
        Some(reason) => reason,
        None if *phase == ProviderPhase::Available => "SitesMatched",
        None => "AwaitingSiteMatch",
    }
}

/// Map a telemetry reason label to a Kubernetes [`EventType`].
///
/// [`Normal`] for the two healthy-phase labels synthesized by
/// [`telemetry_reason_label`]; [`Warning`] for every diagnostic
/// `status.reason` code (config, `GridNetwork`, and probe/TLS failures),
/// including any future code not yet in this list,
/// since an unrecognized reason is safer treated as a `Warning` than
/// silently downgraded to `Normal`.
///
/// [`Normal`]: EventType::Normal
/// [`Warning`]: EventType::Warning
fn event_type_for_reason(reason: &str) -> EventType {
    match reason {
        "SitesMatched" | "AwaitingSiteMatch" => EventType::Normal,
        _ => EventType::Warning,
    }
}

/// Whether a phase or reason change is a "real" transition worth surfacing
/// via Event, metric, and transition-level log — as opposed to a status
/// patch driven solely by `matchingSites`/`discoveredTools` churn.
fn is_real_transition(phase_changed: bool, reason_changed: bool) -> bool {
    phase_changed || reason_changed
}

/// Return whether the status subresource differs from the desired status.
///
/// Business rule: the status subresource is only patched when `phase`,
/// `reason`, `matchingSites`, or `discoveredTools` materially changed —
/// never on a no-op reconcile.
fn agent_tool_provider_status_needs_update(
    current: Option<&AgentToolProviderStatus>,
    desired: &AgentToolProviderStatus,
) -> bool {
    current != Some(desired)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn test_provider(endpoint: &str, grid_network_ref: &str) -> AgentToolProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "AgentToolProvider",
            "metadata": { "name": "prov" },
            "spec": {
                "gridNetworkRef": grid_network_ref,
                "endpoint": endpoint
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    // -----------------------------------------------------------------------
    // validate_provider_config — static validation
    // -----------------------------------------------------------------------

    #[test]
    fn blank_endpoint_maps_to_unavailable() {
        let provider = test_provider("", "net");
        let err = validate_provider_config(&provider);
        assert!(err.is_some(), "blank endpoint must fail static validation");
        assert!(
            err.unwrap_or_else(|| std::process::abort()).contains("endpoint"),
            "error must mention endpoint"
        );
    }

    #[test]
    fn whitespace_only_endpoint_maps_to_unavailable() {
        let provider = test_provider("   ", "net");
        assert!(
            validate_provider_config(&provider).is_some(),
            "whitespace-only endpoint must fail static validation"
        );
    }

    #[test]
    fn blank_grid_network_ref_maps_to_unavailable() {
        let provider = test_provider("http://tools:8080", "");
        let err = validate_provider_config(&provider);
        assert!(err.is_some(), "blank gridNetworkRef must fail static validation");
        assert!(
            err.unwrap_or_else(|| std::process::abort()).contains("gridNetworkRef"),
            "error must mention gridNetworkRef"
        );
    }

    #[test]
    fn whitespace_only_grid_network_ref_maps_to_unavailable() {
        let provider = test_provider("http://tools:8080", "   ");
        assert!(
            validate_provider_config(&provider).is_some(),
            "whitespace-only gridNetworkRef must fail static validation"
        );
    }

    #[test]
    fn valid_config_passes_static_validation() {
        let provider = test_provider("http://tools:8080", "net");
        assert!(
            validate_provider_config(&provider).is_none(),
            "valid provider config must pass static validation"
        );
    }

    // -----------------------------------------------------------------------
    // static_config_failure_reason — async wiring around validate_provider_config
    // and the GridNetwork-existence check, against a mocked Kubernetes API.
    // -----------------------------------------------------------------------

    /// Build a `kube::Client` backed by an in-memory `GridNetwork` map keyed
    /// by name, mirroring `mcp_probe::tests::mock_kube_client_with_secrets`'
    /// pattern for a different resource type.
    #[expect(
        clippy::too_many_lines,
        reason = "test mock builder: 404-vs-200 branches are the whole point"
    )]
    fn mock_kube_client_with_grid_networks(networks: HashMap<&'static str, GridNetwork>) -> Client {
        let service = tower::service_fn(move |req: http::Request<kube::client::Body>| {
            let networks = networks.clone();
            async move {
                let name = req.uri().path().rsplit('/').next().unwrap_or_default().to_owned();
                let response = networks.get(name.as_str()).map_or_else(
                    || {
                        let not_found = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": format!("gridnetworks.grid.praxis-proxy.io \"{name}\" not found"),
                            "reason": "NotFound",
                            "code": 404,
                        });
                        http::Response::builder()
                            .status(404)
                            .body(kube::client::Body::from(
                                serde_json::to_vec(&not_found).unwrap_or_else(|_| std::process::abort()),
                            ))
                            .unwrap_or_else(|_| std::process::abort())
                    },
                    |network| {
                        http::Response::builder()
                            .status(200)
                            .body(kube::client::Body::from(
                                serde_json::to_vec(network).unwrap_or_else(|_| std::process::abort()),
                            ))
                            .unwrap_or_else(|_| std::process::abort())
                    },
                );
                Ok::<_, std::convert::Infallible>(response)
            }
        });
        Client::new(service, "default")
    }

    fn test_grid_network(name: &str) -> GridNetwork {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": name },
            "spec": {}
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    /// A `kube::Client` that panics if a request is ever sent through it —
    /// used to prove `static_config_failure_reason` short-circuits on a
    /// config error *before* making any Kubernetes API call.
    fn unused_kube_client() -> Client {
        let service = tower::service_fn(|_req: http::Request<kube::client::Body>| async {
            Ok::<_, std::convert::Infallible>(http::Response::new(kube::client::Body::empty()))
        });
        Client::new(service, "default")
    }

    #[tokio::test]
    async fn static_config_failure_reason_short_circuits_on_invalid_config_without_calling_kubernetes() {
        let provider = test_provider("", "net");
        let result = static_config_failure_reason(&provider, &unused_kube_client(), "prov").await;
        assert_eq!(
            result.unwrap_or_else(|_| std::process::abort()),
            Some("ProviderConfigInvalid"),
            "an invalid static config must short-circuit to ProviderConfigInvalid before any GridNetwork lookup"
        );
    }

    #[tokio::test]
    async fn static_config_failure_reason_returns_grid_network_not_found_when_absent() {
        let provider = test_provider("http://tools:8080", "absent-net");
        let client = mock_kube_client_with_grid_networks(HashMap::new());
        let result = static_config_failure_reason(&provider, &client, "prov").await;
        assert_eq!(
            result.unwrap_or_else(|_| std::process::abort()),
            Some("GridNetworkNotFound"),
            "a gridNetworkRef that doesn't resolve to an existing GridNetwork must yield GridNetworkNotFound"
        );
    }

    #[tokio::test]
    async fn static_config_failure_reason_returns_none_when_grid_network_exists() {
        let provider = test_provider("http://tools:8080", "net");
        let client = mock_kube_client_with_grid_networks(HashMap::from([("net", test_grid_network("net"))]));
        let result = static_config_failure_reason(&provider, &client, "prov").await;
        assert_eq!(
            result.unwrap_or_else(|_| std::process::abort()),
            None,
            "a valid config with an existing GridNetwork must pass both static checks"
        );
    }

    // -----------------------------------------------------------------------
    // phase_from_matching — pure phase logic
    // -----------------------------------------------------------------------

    #[test]
    fn no_matching_sites_yields_pending() {
        let phase = phase_from_matching(&[]);
        assert_eq!(phase, ProviderPhase::Pending, "empty matching → Pending");
    }

    #[test]
    fn one_matching_site_yields_available() {
        let phase = phase_from_matching(&["site-a".to_owned()]);
        assert_eq!(phase, ProviderPhase::Available, "one match → Available");
    }

    #[test]
    fn multiple_matching_sites_yields_available() {
        let phase = phase_from_matching(&["site-a".to_owned(), "site-b".to_owned()]);
        assert_eq!(phase, ProviderPhase::Available, "multiple matches → Available");
    }

    #[test]
    fn phase_from_matching_never_emits_degraded_or_unavailable() {
        let empty_phase = phase_from_matching(&[]);
        let some_phase = phase_from_matching(&["site-x".to_owned()]);
        assert_ne!(
            empty_phase,
            ProviderPhase::Degraded,
            "Degraded unreachable from site matching alone"
        );
        assert_ne!(
            some_phase,
            ProviderPhase::Degraded,
            "Degraded unreachable from site matching alone"
        );
        assert_ne!(
            empty_phase,
            ProviderPhase::Unavailable,
            "site matching alone never yields Unavailable"
        );
        assert_ne!(
            some_phase,
            ProviderPhase::Unavailable,
            "site matching alone never yields Unavailable"
        );
    }

    // -----------------------------------------------------------------------
    // sites_matching_selector — selector matching
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

    fn test_provider_with_selector(network: &str, selector: &[(&str, &str)]) -> AgentToolProvider {
        let match_labels: serde_json::Map<String, serde_json::Value> = selector
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "AgentToolProvider",
            "metadata": { "name": "prov" },
            "spec": {
                "gridNetworkRef": network,
                "endpoint": "http://tools:8080",
                "siteSelector": { "matchLabels": match_labels }
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn empty_selector_matches_all_passed_sites() {
        let provider = test_provider("http://tools:8080", "net");
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
        let provider = test_provider_with_selector("net", &[("hw", "gpu")]);
        let sites = vec![
            test_site_with_labels("gpu-site", "net", &[("hw", "gpu")]),
            test_site_with_labels("cpu-site", "net", &[("hw", "cpu")]),
        ];
        let matching = sites_matching_selector(&provider, &sites);
        assert_eq!(matching, vec!["gpu-site"], "only gpu-site should match");
    }

    #[test]
    fn matching_sites_are_sorted_deterministically() {
        let provider = test_provider("http://tools:8080", "net");
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
    fn no_matching_sites_returns_empty() {
        let provider = test_provider_with_selector("net", &[("hw", "gpu")]);
        let sites = vec![test_site_with_labels("cpu-site", "net", &[("hw", "cpu")])];
        let matching = sites_matching_selector(&provider, &sites);
        assert!(matching.is_empty(), "no matching sites should return empty");
    }

    #[test]
    fn multi_key_selector_requires_all_keys_to_match() {
        let provider = test_provider_with_selector("net", &[("hw", "gpu"), ("region", "us-east")]);
        let both = test_site_with_labels(
            "full-match",
            "net",
            &[("hw", "gpu"), ("region", "us-east"), ("extra", "ignored")],
        );
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
    fn empty_selector_with_no_sites_returns_empty() {
        let provider = test_provider("http://tools:8080", "net");
        let matching = sites_matching_selector(&provider, &[]);
        assert!(
            matching.is_empty(),
            "passing an empty sites slice must return an empty result"
        );
    }

    // -----------------------------------------------------------------------
    // agent_tool_provider_status_needs_update — patch-if-changed decision
    // -----------------------------------------------------------------------

    fn baseline_status() -> AgentToolProviderStatus {
        AgentToolProviderStatus {
            discovered_tools: vec!["search".to_owned()],
            matching_sites: vec!["site-a".to_owned()],
            observed_generation: 2,
            phase: ProviderPhase::Available,
            reason: None,
        }
    }

    #[test]
    fn no_op_reconcile_does_not_need_update() {
        let baseline = baseline_status();
        assert!(
            !agent_tool_provider_status_needs_update(Some(&baseline), &baseline),
            "identical current and desired status must never require a patch"
        );
    }

    #[test]
    fn phase_change_needs_update() {
        let baseline = baseline_status();
        let changed = AgentToolProviderStatus {
            phase: ProviderPhase::Degraded,
            ..baseline.clone()
        };
        assert!(
            agent_tool_provider_status_needs_update(Some(&baseline), &changed),
            "a phase change must require a status patch"
        );
    }

    #[test]
    fn reason_change_needs_update() {
        let baseline = baseline_status();
        let changed = AgentToolProviderStatus {
            reason: Some("McpEndpointUnreachable".to_owned()),
            ..baseline.clone()
        };
        assert!(
            agent_tool_provider_status_needs_update(Some(&baseline), &changed),
            "a reason change must require a status patch, even when phase is unchanged"
        );
    }

    #[test]
    fn matching_sites_change_needs_update() {
        let baseline = baseline_status();
        let changed = AgentToolProviderStatus {
            matching_sites: vec!["site-a".to_owned(), "site-b".to_owned()],
            ..baseline.clone()
        };
        assert!(
            agent_tool_provider_status_needs_update(Some(&baseline), &changed),
            "a matchingSites change must require a status patch"
        );
    }

    #[test]
    fn discovered_tools_change_needs_update() {
        let baseline = baseline_status();
        let changed = AgentToolProviderStatus {
            discovered_tools: vec!["search".to_owned(), "fetch".to_owned()],
            ..baseline.clone()
        };
        assert!(
            agent_tool_provider_status_needs_update(Some(&baseline), &changed),
            "a discoveredTools change must require a status patch"
        );
    }

    #[test]
    fn absent_current_status_needs_update() {
        let desired = baseline_status();
        assert!(
            agent_tool_provider_status_needs_update(None, &desired),
            "an AgentToolProvider with no prior status must always be patched on first reconcile"
        );
    }

    // -----------------------------------------------------------------------
    // telemetry_reason_label — synthesize a bounded telemetry label
    // -----------------------------------------------------------------------

    #[test]
    fn telemetry_label_passes_through_status_reason_when_set() {
        assert_eq!(
            telemetry_reason_label(&ProviderPhase::Unavailable, Some("ProviderConfigInvalid")),
            "ProviderConfigInvalid",
            "an explicit status.reason must be used verbatim as the telemetry label"
        );
    }

    #[test]
    fn telemetry_label_passes_through_grid_network_not_found_reason() {
        assert_eq!(
            telemetry_reason_label(&ProviderPhase::Unavailable, Some("GridNetworkNotFound")),
            "GridNetworkNotFound"
        );
    }

    #[test]
    fn telemetry_label_synthesizes_sites_matched_for_available_with_no_reason() {
        assert_eq!(
            telemetry_reason_label(&ProviderPhase::Available, None),
            "SitesMatched",
            "Available with no status.reason (the healthy case) must synthesize a bounded label"
        );
    }

    #[test]
    fn telemetry_label_synthesizes_awaiting_site_match_for_pending_with_no_reason() {
        assert_eq!(
            telemetry_reason_label(&ProviderPhase::Pending, None),
            "AwaitingSiteMatch",
            "Pending with no status.reason must synthesize a bounded label distinct from Available's"
        );
    }

    #[test]
    fn telemetry_label_falls_back_to_awaiting_site_match_for_any_other_healthy_phase() {
        assert_eq!(
            telemetry_reason_label(&ProviderPhase::Degraded, None),
            "AwaitingSiteMatch",
            "any non-Available phase with no explicit reason falls back to the Pending-style label"
        );
    }

    // -----------------------------------------------------------------------
    // event_type_for_reason — bounded Event severity mapping
    // -----------------------------------------------------------------------

    #[test]
    fn sites_matched_reason_is_a_normal_event() {
        assert!(matches!(event_type_for_reason("SitesMatched"), EventType::Normal));
    }

    #[test]
    fn awaiting_site_match_reason_is_a_normal_event() {
        assert!(matches!(event_type_for_reason("AwaitingSiteMatch"), EventType::Normal));
    }

    #[test]
    fn provider_config_invalid_reason_is_a_warning_event() {
        assert!(matches!(
            event_type_for_reason("ProviderConfigInvalid"),
            EventType::Warning
        ));
    }

    #[test]
    fn grid_network_not_found_reason_is_a_warning_event() {
        assert!(matches!(
            event_type_for_reason("GridNetworkNotFound"),
            EventType::Warning
        ));
    }

    #[test]
    fn unrecognized_reason_defaults_to_warning_event() {
        assert!(matches!(
            event_type_for_reason("SomeFutureProbeReason"),
            EventType::Warning
        ));
    }

    // -----------------------------------------------------------------------
    // is_real_transition — gates Event emission, metric recording, and
    // transition-level logging so a no-op reconcile never fires any of them
    // -----------------------------------------------------------------------

    #[test]
    fn no_change_is_not_a_real_transition() {
        assert!(
            !is_real_transition(false, false),
            "neither phase nor reason changed: not a real transition"
        );
    }

    #[test]
    fn phase_change_alone_is_a_real_transition() {
        assert!(
            is_real_transition(true, false),
            "a phase change alone must count as a real transition"
        );
    }

    #[test]
    fn reason_change_alone_is_a_real_transition() {
        assert!(
            is_real_transition(false, true),
            "a reason change alone (phase steady) must still count as a real transition"
        );
    }

    #[test]
    fn both_changing_is_a_real_transition() {
        assert!(is_real_transition(true, true));
    }

    // -----------------------------------------------------------------------
    // reconcile — full orchestration against a mocked Kubernetes API.
    //
    // Everything above exercises resolve_phase_and_sites/update_status's
    // constituent resolve_*/pure-logic functions in isolation. These tests
    // drive the public reconcile() entrypoint itself end-to-end, proving the
    // resolved (phase, reason, matchingSites, discoveredTools) tuple actually
    // reaches the Kubernetes API as the status PATCH body a real controller
    // would send — the seam none of the functions above individually cover.
    // -----------------------------------------------------------------------

    /// A `kube::Client` that serves `GridNetwork` GETs from an in-memory map
    /// and captures every status-subresource PATCH body sent through it into
    /// `captured`. Any other request (a missing `GridNetwork`, the `Event`
    /// POST from `Recorder::publish`, a `GridSite` LIST) 404s: `update_status`
    /// and `emit_transition_telemetry` both already tolerate a failed event
    /// publish by design (logged, not propagated — see `emit_transition_telemetry`),
    /// and the two scenarios below never reach `GridSite` listing at all,
    /// since both short-circuit inside `static_config_failure_reason`.
    #[expect(
        clippy::too_many_lines,
        reason = "test mock builder: PATCH-capture vs GridNetwork-GET vs catch-all-404 branches are the whole point"
    )]
    fn mock_kube_client_capturing_status_patch(
        networks: HashMap<&'static str, GridNetwork>,
        captured: Arc<std::sync::Mutex<Option<serde_json::Value>>>,
    ) -> Client {
        let service = tower::service_fn(move |req: http::Request<kube::client::Body>| {
            let networks = networks.clone();
            let captured = Arc::clone(&captured);
            async move {
                if req.method() == http::Method::PATCH && req.uri().path().ends_with("/status") {
                    let name = req
                        .uri()
                        .path()
                        .trim_end_matches("/status")
                        .rsplit('/')
                        .next()
                        .unwrap_or_default()
                        .to_owned();
                    let bytes = http_body_util::BodyExt::collect(req.into_body())
                        .await
                        .unwrap_or_else(|_| std::process::abort())
                        .to_bytes();
                    let body: serde_json::Value =
                        serde_json::from_slice(&bytes).unwrap_or_else(|_| std::process::abort());
                    *captured.lock().unwrap_or_else(|_| std::process::abort()) = Some(body.clone());

                    let echo = serde_json::json!({
                        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
                        "kind": "AgentToolProvider",
                        "metadata": { "name": name },
                        "spec": { "gridNetworkRef": "net", "endpoint": "http://tools:8080" },
                        "status": body["status"],
                    });
                    return Ok::<_, std::convert::Infallible>(
                        http::Response::builder()
                            .status(200)
                            .body(kube::client::Body::from(
                                serde_json::to_vec(&echo).unwrap_or_else(|_| std::process::abort()),
                            ))
                            .unwrap_or_else(|_| std::process::abort()),
                    );
                }

                if req.method() == http::Method::GET {
                    let name = req.uri().path().rsplit('/').next().unwrap_or_default().to_owned();
                    if let Some(network) = networks.get(name.as_str()) {
                        return Ok(http::Response::builder()
                            .status(200)
                            .body(kube::client::Body::from(
                                serde_json::to_vec(network).unwrap_or_else(|_| std::process::abort()),
                            ))
                            .unwrap_or_else(|_| std::process::abort()));
                    }
                }

                let not_found = serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "status": "Failure",
                    "message": "not found",
                    "reason": "NotFound",
                    "code": 404,
                });
                Ok(http::Response::builder()
                    .status(404)
                    .body(kube::client::Body::from(
                        serde_json::to_vec(&not_found).unwrap_or_else(|_| std::process::abort()),
                    ))
                    .unwrap_or_else(|_| std::process::abort()))
            }
        });
        Client::new(service, "default")
    }

    #[tokio::test]
    async fn reconcile_patches_unavailable_provider_config_invalid_without_any_grid_network_lookup() {
        let provider = test_provider("", "net");
        let captured = Arc::new(std::sync::Mutex::new(None));
        let client = mock_kube_client_capturing_status_patch(HashMap::new(), Arc::clone(&captured));

        let action = Box::pin(reconcile(Arc::new(provider), Arc::new(client)))
            .await
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            action,
            Action::requeue(REQUEUE_INTERVAL),
            "reconcile must always requeue on a successful (non-erroring) pass, even when the provider is Unavailable"
        );

        let patched = captured
            .lock()
            .unwrap_or_else(|_| std::process::abort())
            .clone()
            .expect("reconcile must PATCH the status subresource for a first-seen config-invalid provider");
        assert_eq!(
            patched["status"]["phase"], "Unavailable",
            "a blank endpoint must surface as Unavailable all the way through to the persisted status"
        );
        assert_eq!(
            patched["status"]["reason"], "ProviderConfigInvalid",
            "the specific static-validation failure reason must reach the persisted status"
        );
        assert_eq!(
            patched["status"]["matchingSites"],
            serde_json::json!([]),
            "a config-invalid provider must never report matching sites"
        );
    }

    #[tokio::test]
    async fn reconcile_patches_unavailable_grid_network_not_found_when_referenced_network_absent() {
        let provider = test_provider("http://tools:8080", "missing-net");
        let captured = Arc::new(std::sync::Mutex::new(None));
        let client = mock_kube_client_capturing_status_patch(HashMap::new(), Arc::clone(&captured));

        let action = Box::pin(reconcile(Arc::new(provider), Arc::new(client)))
            .await
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            action,
            Action::requeue(REQUEUE_INTERVAL),
            "reconcile must requeue even when the referenced GridNetwork cannot be found"
        );

        let patched = captured
            .lock()
            .unwrap_or_else(|_| std::process::abort())
            .clone()
            .expect("reconcile must PATCH the status subresource once the GridNetwork lookup 404s");
        assert_eq!(
            patched["status"]["phase"], "Unavailable",
            "an unresolvable gridNetworkRef must surface as Unavailable through the full reconcile path"
        );
        assert_eq!(
            patched["status"]["reason"], "GridNetworkNotFound",
            "the GridNetwork-lookup failure reason must reach the persisted status, proving reconcile actually \
             performed the live GET rather than short-circuiting on static config alone"
        );
    }
}
