//! [`GridNetwork`] controller.
//!
//! Reconciles [`GridNetwork`] resources: generates the grid CA
//! and site certificate, manages TLS secrets, generates the
//! grid ID, signals the SWIM runtime to start, and renders
//! routing overlay ConfigMaps for each gateway reference.
//!
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork

use std::{
    collections::{BTreeSet, HashMap},
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};

use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
    Client,
    api::{Api, ListParams, Patch, PatchParams},
    runtime::{controller::Action, reflector::ObjectRef},
};
use tokio::{sync::Mutex, time::Duration};
use tracing::info;

use crate::{
    crd::{
        grid_network::{GatewayRef, GridNetwork, GridNetworkPhase, GridNetworkStatus},
        grid_site::GridSite,
        inference_provider::InferenceProvider,
    },
    error::OperatorError,
    resources::{provider_metrics, routing_overlay, secret},
    swim::{MemberStatus, MembershipSnapshot},
    swim_runtime::SwimHandle,
};

// ---------------------------------------------------------------------------
// Operator context
// ---------------------------------------------------------------------------

/// Shared context passed to the [`GridNetwork`] controller's reconcile loop.
///
/// Bundles the Kubernetes client with an optional live SWIM handle.
/// When `swim` is `Some`, each reconcile obtains a fresh
/// [`MembershipSnapshot`] to feed into `determine_phase` and
/// `update_status`.  When `swim` is `None`, the controller falls back
/// to its existing static phase logic.
pub struct OperatorCtx {
    /// Kubernetes API client.
    pub client: Client,

    /// Optional handle to the live SWIM membership runtime.
    ///
    /// `None` when the operator is started without a SWIM bind address
    /// configured (e.g. in single-node or test environments).
    pub swim: Option<Arc<SwimHandle>>,

    /// Cross-reconcile cache of recently-scraped provider metrics.
    ///
    /// Keyed by `(network_name, provider_routing_identity)`.  Each successful
    /// Prometheus scrape updates this cache.  When a subsequent scrape fails
    /// and the provider's `metricsConfig.stale_metrics_seconds` grace period is
    /// configured, the cached sample is used instead of falling back to neutral
    /// scoring immediately.
    ///
    /// The cache is shared across concurrent reconcile invocations via the
    /// wrapping `Arc`; the inner [`Mutex`] ensures safe concurrent access.
    pub(crate) metrics_cache: Mutex<provider_metrics::MetricsCache>,
}

impl OperatorCtx {
    /// Create a new [`OperatorCtx`] with an empty metrics cache.
    ///
    /// This is the canonical constructor used by the operator binary so that
    /// the internal metrics cache type does not need to be exported from the
    /// library crate.
    pub fn new(client: Client, swim: Option<Arc<SwimHandle>>) -> Self {
        Self {
            client,
            swim,
            metrics_cache: Mutex::new(provider_metrics::MetricsCache::new()),
        }
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Requeue interval after a successful reconciliation.
const REQUEUE_INTERVAL: Duration = Duration::from_secs(300);

/// Field manager name for server-side apply.
const FIELD_MANAGER: &str = "grid-operator";

/// Label key that opts a `GridNetwork` into automatic `GridSite` discovery.
///
/// When this label is present with value `"true"`, the `GridNetwork` controller
/// creates `GridSite` resources for remote Alive SWIM members automatically.
/// Networks without this label are unaffected — their overlay generation uses
/// the existing `routingClusterRef`-based (Phase 1) fallback.
///
/// This opt-in gate prevents auto-discovery from changing the overlay generation
/// semantics for networks that were not designed with it in mind.
pub const LABEL_AUTO_DISCOVER_SITES: &str = "grid.praxis-proxy.io/auto-discover-sites";

// ---------------------------------------------------------------------------
// Cross-resource watch mappers
// ---------------------------------------------------------------------------

/// Map an [`InferenceProvider`] change to the [`GridNetwork`] it belongs to.
///
/// Returns `Some(ObjectRef)` for the `GridNetwork` named by
/// `spec.gridNetworkRef`, or `None` when the field is blank (which would
/// indicate a malformed resource — we silently skip rather than panic or
/// trigger spurious reconciles).
///
/// Used by the [`GridNetwork`] controller's cross-resource watch so that
/// changes to any `InferenceProvider` trigger immediate overlay refresh of
/// the owning `GridNetwork`.
pub fn network_refs_from_inference_provider(ip: InferenceProvider) -> Option<ObjectRef<GridNetwork>> {
    let name = ip.spec.grid_network_ref;
    if name.trim().is_empty() {
        None
    } else {
        Some(ObjectRef::new(&name))
    }
}

/// Map a [`GridSite`] change to the [`GridNetwork`] it belongs to.
///
/// Returns `Some(ObjectRef)` for the `GridNetwork` named by
/// `spec.gridNetworkRef`, or `None` when the field is blank.
///
/// Used by the [`GridNetwork`] controller's cross-resource watch so that
/// changes to any `GridSite` (e.g. label updates affecting site selector
/// matching) trigger immediate overlay refresh of the owning `GridNetwork`.
pub fn network_refs_from_grid_site(site: GridSite) -> Option<ObjectRef<GridNetwork>> {
    let name = site.spec.grid_network_ref;
    if name.trim().is_empty() {
        None
    } else {
        Some(ObjectRef::new(&name))
    }
}

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
#[expect(
    clippy::too_many_lines,
    reason = "sequential reconcile steps: TLS, providers fetch, overlay, CRDT broadcast, status update"
)]
pub async fn reconcile(network: Arc<GridNetwork>, ctx: Arc<OperatorCtx>) -> Result<Action, OperatorError> {
    let name = network
        .metadata
        .name
        .as_deref()
        .unwrap_or_else(|| std::process::abort());

    info!(name, "reconciling GridNetwork");

    let client = &ctx.client;
    ensure_tls_secrets(&network, client).await?;

    // Announce CRD-declared seeds to the SWIM runtime so peers can be reached
    // without requiring the GRID_SWIM_SEEDS environment variable.
    // Re-announcing on each reconcile is idempotent (foca ignores existing members).
    if let Some(swim) = ctx.swim.as_ref() {
        announce_crd_seeds(&network, swim);
    }

    // List providers once; share between routing overlay rendering and CRDT publishing.
    let providers = list_all_inference_providers(client).await?;
    let raw_metrics =
        provider_metrics::collect_provider_metrics(name, &providers, &ctx.metrics_cache, Instant::now()).await;

    let remote_crdt_providers: Vec<crdt::ProviderState> = ctx
        .swim
        .as_ref()
        .map(|swim| collect_remote_crdt_providers(swim, name))
        .unwrap_or_default();

    // Obtain a live membership snapshot here - used both for staleness override below
    // and for phase determination after the overlay step.
    // When swim is None (runtime not configured), falls through to static phase logic.
    let membership = ctx.swim.as_ref().map(|h| h.snapshot());

    // Downgrade providers from Dead/Suspect SWIM members to Degraded so the overlay
    // emits fresh=false for their candidates.  The record is kept (not excluded) so
    // Praxis can observe the stale-but-known state while preferring healthy fallbacks.
    let remote_crdt_providers = apply_swim_staleness_override(&remote_crdt_providers, membership.as_ref());

    // Apply stale candidate GC policy: omit remote providers whose Dead/Suspect age
    // exceeds the configured TTL.  With the default policy (TTL=None, absent field)
    // this is a no-op — runtime behaviour is unchanged from pre-GC.
    let stale_policy = routing_overlay::stale_policy_from_spec(network.spec.stale_candidate_ttl_seconds);
    let remote_crdt_providers =
        routing_overlay::apply_stale_gc_filter(&remote_crdt_providers, membership.as_ref(), &stale_policy);

    reconcile_routing_overlay_inner(&network, client, &providers, &remote_crdt_providers, &raw_metrics).await?;

    let grid_id = resolve_grid_id(&network);
    let phase = determine_phase(&network, &grid_id, membership.as_ref());

    // Publish real InferenceProvider-derived CRDT state so peers learn this site's providers.
    let distributed_provider_count = if let Some(swim) = ctx.swim.as_ref() {
        publish_real_provider_state(swim, name, &providers, &raw_metrics);
        count_remote_provider_records(swim, name)
    } else {
        0
    };

    update_status(
        &network,
        client,
        &grid_id,
        &phase,
        membership.as_ref(),
        distributed_provider_count,
    )
    .await?;

    // Auto-create or update GridSite records for remote Alive SWIM members.
    // Only runs when the GridNetwork explicitly opts in via LABEL_AUTO_DISCOVER_SITES.
    // This gate prevents auto-discovery from changing overlay generation semantics
    // for networks that use the existing routingClusterRef-based (Phase 1) path.
    let auto_discover_enabled = network
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(LABEL_AUTO_DISCOVER_SITES))
        .is_some_and(|v| v == "true");
    if auto_discover_enabled && let (Some(swim), Some(snapshot)) = (ctx.swim.as_ref(), membership.as_ref()) {
        reconcile_discovered_sites(name, swim.site_name(), snapshot, client).await?;
    }

    Ok(Action::requeue(REQUEUE_INTERVAL))
}

/// Error policy for the [`GridNetwork`] controller.
pub fn error_policy(_network: Arc<GridNetwork>, error: &OperatorError, _ctx: Arc<OperatorCtx>) -> Action {
    tracing::error!(%error, "GridNetwork reconciliation failed");
    Action::requeue(Duration::from_secs(30))
}

// ---------------------------------------------------------------------------
// CRD-driven SWIM seeds
// ---------------------------------------------------------------------------

/// Parse and normalize SWIM seed addresses from `GridNetwork.spec.seeds`.
///
/// Each string is trimmed and parsed as a [`SocketAddr`].  Invalid entries
/// are logged at `warn` level and skipped; they do not fail the reconcile.
/// If `local_addr` is `Some`, any address equal to it is removed (self-seed).
/// Duplicates are removed; the result is deterministically sorted.
///
/// Returns an empty `Vec` when `raw` is empty or all entries fail to parse.
pub(crate) fn parse_crd_seeds(raw: &[String], local_addr: Option<SocketAddr>) -> Vec<SocketAddr> {
    let mut seen = BTreeSet::new();
    for s in raw {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            continue;
        }
        match trimmed.parse::<SocketAddr>() {
            Ok(addr) => {
                if local_addr.is_some_and(|local| addr == local) {
                    tracing::debug!(addr = %addr, "GridNetwork spec.seeds: skipping self-address");
                    continue;
                }
                seen.insert(addr);
            },
            Err(e) => {
                tracing::warn!(
                    seed = trimmed,
                    error = %e,
                    "GridNetwork spec.seeds contains invalid socket address, skipping"
                );
            },
        }
    }
    seen.into_iter().collect()
}

/// Announce `network.spec.seeds` to the live SWIM runtime.
///
/// Called once per reconcile.  Re-announcing to existing members is
/// idempotent — foca ignores redundant joins.
///
/// # Global-runtime semantics
///
/// The SWIM runtime is **process-global**: one UDP listener per operator
/// process, shared by all `GridNetwork` reconciles in that process.  Seeds
/// from any `GridNetwork.spec.seeds` are announced to the same SWIM node.
/// This makes `spec.seeds` a site-membership bootstrap mechanism, not a
/// per-network membership isolation control.  CRDT provider records remain
/// network-scoped separately (filtered by `network_id` in `collect_remote_crdt_providers`).
///
/// # Channel-full behavior
///
/// If the SWIM runtime seed channel is full (capacity 16 batches), the
/// announce is skipped for this reconcile cycle and retried on the next
/// reconcile (default interval 300 s).  This means CRD seeds are not
/// guaranteed to be announced immediately when the runtime is under heavy
/// broadcast load, but they will be applied on the next reconcile.
///
/// Channel errors are logged at `warn` level and do not fail the reconcile.
fn announce_crd_seeds(network: &GridNetwork, swim: &SwimHandle) {
    if network.spec.seeds.is_empty() {
        return;
    }
    let seeds = parse_crd_seeds(&network.spec.seeds, Some(swim.local_addr()));
    if seeds.is_empty() {
        return;
    }
    let name = network.metadata.name.as_deref().unwrap_or("?");
    tracing::info!(name, seeds = seeds.len(), "announcing CRD seeds to SWIM runtime");
    if let Err(e) = swim.announce_seeds(seeds) {
        // Channel-full or closed: log and continue. Seeds will be re-queued on
        // the next reconcile cycle (REQUEUE_INTERVAL = 300 s by default).
        tracing::warn!(name, error = %e, "failed to queue CRD seeds for SWIM announcement; will retry on next reconcile");
    }
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
/// Changes to [`InferenceProvider`] and [`GridSite`] resources trigger a
/// [`GridNetwork`] reconcile via cross-resource watches in the controller
/// (see [`network_refs_from_inference_provider`] and
/// [`network_refs_from_grid_site`]).  Overlays stay consistent with provider
/// availability and site membership without waiting for the next periodic
/// requeue.
///
/// [`GridSite`]: crate::crd::grid_site::GridSite
#[expect(
    clippy::large_stack_frames,
    reason = "async future with kube API types and overlay data"
)]
#[expect(
    clippy::too_many_lines,
    reason = "sequential reconcile steps: metrics collection, overlay render, ConfigMap apply"
)]
/// Reconcile routing overlay `ConfigMap`s using pre-fetched provider and metrics data.
///
/// Receives the provider list, remote CRDT providers, and metrics map from
/// [`reconcile`] so both the routing overlay and the CRDT state broadcast share
/// a single kube API fetch.  Remote CRDT providers are passed through to
/// [`routing_overlay::render_routing_overlay`] so cross-site candidates appear
/// in the overlay.
async fn reconcile_routing_overlay_inner(
    network: &GridNetwork,
    client: &Client,
    providers: &[InferenceProvider],
    remote_crdt_providers: &[crdt::ProviderState],
    raw_metrics: &HashMap<String, scoring::BackendMetrics>,
) -> Result<(), OperatorError> {
    let network_name = network
        .metadata
        .name
        .as_deref()
        .unwrap_or_else(|| std::process::abort());

    let sites = list_all_grid_sites(client).await?;

    let metrics_by_str: HashMap<&str, scoring::BackendMetrics> =
        raw_metrics.iter().map(|(k, v)| (k.as_str(), *v)).collect();
    let metrics_arg = if metrics_by_str.is_empty() {
        None
    } else {
        Some(&metrics_by_str)
    };

    for gw_ref in &network.spec.gateway_refs {
        // Each gateway identifies its own local site.  Fall back to the
        // network name for single-site deployments where the two are equal.
        let local_site = gw_ref.local_site_name.as_deref().unwrap_or(network_name);
        let overlay = routing_overlay::render_routing_overlay(
            network,
            &sites,
            providers,
            remote_crdt_providers,
            local_site,
            metrics_arg,
        )
        .map_err(OperatorError::OverlayRender)?;
        // Praxis grid_route rejects an empty candidates list at config load
        // time, which would cause a hot-reload error rather than a clean
        // "no routes" state.  Skip the apply and warn so the previous
        // (non-empty) ConfigMap remains in place until a provider becomes
        // available again.
        if overlay.candidates.is_empty() {
            tracing::warn!(
                network = network_name,
                gateway = %gw_ref.name,
                "routing overlay has no candidates; skipping ConfigMap apply \
                 to prevent invalid Praxis grid_route config"
            );
            continue;
        }
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
async fn list_all_grid_sites(client: &Client) -> Result<Vec<GridSite>, OperatorError> {
    let api: Api<GridSite> = Api::all(client.clone());
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
///
/// When a [`MembershipSnapshot`] is provided, the live membership state takes
/// precedence:
/// - ≥1 [`Alive`] member → [`Active`].
/// - Members present but all [`Suspect`]/[`Dead`] → [`Degraded`].
/// - Empty snapshot → falls through to the existing TLS-based logic.
///
/// When `membership` is `None` (no SWIM runtime wired yet), the existing
/// `Pending`/`Initializing` logic is unchanged.
///
/// [`Alive`]: MemberStatus::Alive
/// [`Suspect`]: MemberStatus::Suspect
/// [`Dead`]: MemberStatus::Dead
/// [`Active`]: GridNetworkPhase::Active
/// [`Degraded`]: GridNetworkPhase::Degraded
fn determine_phase(network: &GridNetwork, grid_id: &str, membership: Option<&MembershipSnapshot>) -> GridNetworkPhase {
    if grid_id.is_empty() {
        return GridNetworkPhase::Pending;
    }
    // Live membership takes precedence when available and non-empty.
    if let Some(snap) = membership
        && let Some(hint) = snap.phase_hint()
    {
        return hint;
    }
    // Existing static phase logic (no live membership yet).
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

// ---------------------------------------------------------------------------
// Provider → CRDT state mapping
// ---------------------------------------------------------------------------

/// Map a Kubernetes `ProviderPhase` to the CRDT `ProviderPhase`.
///
/// All variants are preserved so remote sites know about unavailable providers
/// and can avoid routing to them.  Absent status (not yet reconciled) maps to
/// `Pending`.
fn crdt_phase_from_provider(
    status_phase: Option<&crate::crd::inference_provider::ProviderPhase>,
) -> crdt::ProviderPhase {
    use crate::crd::inference_provider::ProviderPhase as Op;
    match status_phase {
        Some(Op::Available) => crdt::ProviderPhase::Available,
        Some(Op::Degraded) => crdt::ProviderPhase::Degraded,
        Some(Op::Unavailable) => crdt::ProviderPhase::Unavailable,
        Some(Op::Pending) | None => crdt::ProviderPhase::Pending,
    }
}

/// Convert a [`scoring::BackendMetrics`] to a CRDT [`crdt::ProviderMetricsSnapshot`].
///
/// When `metrics` is `None` (no live scrape configured or scrape failed) all
/// fields default to `None` so remote sites apply neutral scoring.
fn metrics_to_crdt(metrics: Option<scoring::BackendMetrics>) -> crdt::ProviderMetricsSnapshot {
    metrics.map_or_else(crdt::ProviderMetricsSnapshot::default, |m| {
        crdt::ProviderMetricsSnapshot {
            queue_depth: Some(m.queue_depth),
            kv_cache_utilization: Some(m.kv_cache_utilization),
            latency_p99_ms: Some(m.latency_p99_ms),
            prefix_cache_hit_ratio: Some(m.prefix_cache_hit_ratio),
            error_rate: Some(m.error_rate),
            healthy: Some(m.healthy),
        }
    })
}

/// Map one Kubernetes [`InferenceProvider`] to a CRDT [`crdt::ProviderState`].
///
/// Returns `None` when the provider has no metadata name (invalid resource).
///
/// **Revision strategy**: prefers Kubernetes `metadata.resourceVersion`, which
/// advances on spec and status writes, and falls back to `metadata.generation`
/// when no parseable resource version is present.  Equal revisions break ties
/// via `writer_id`, which is the advertising SWIM site identity.
fn provider_state_from_kube(
    provider: &InferenceProvider,
    network_id: &str,
    site_id: &str,
    metrics: Option<scoring::BackendMetrics>,
) -> Option<crdt::ProviderState> {
    let provider_id = provider.metadata.name.as_deref()?;
    let routing_cluster = routing_overlay::routing_identity(provider)?.to_owned();
    let models = provider.spec.models.iter().map(|m| m.name.clone()).collect();
    let phase = crdt_phase_from_provider(provider.status.as_ref().map(|s| &s.phase));
    let revision = provider_revision(provider);

    Some(crdt::ProviderState {
        network_id: network_id.to_owned(),
        site_id: site_id.to_owned(),
        provider_id: provider_id.to_owned(),
        routing_cluster,
        models,
        backend_kind: provider.spec.backend_kind.clone(),
        phase,
        metrics: metrics_to_crdt(metrics),
        revision,
        writer_id: site_id.to_owned(),
    })
}

/// Return the monotonic-ish Kubernetes revision used for CRDT provider records.
///
/// `resourceVersion` is preferred because it advances for status changes and
/// metrics-bearing reconciles, not only spec changes.  Unit tests and malformed
/// fixtures may lack a parseable resource version, so fall back to generation.
fn provider_revision(provider: &InferenceProvider) -> u64 {
    provider
        .metadata
        .resource_version
        .as_deref()
        .and_then(|rv| rv.parse::<u64>().ok())
        .or_else(|| provider.metadata.generation.and_then(|g| u64::try_from(g).ok()))
        .unwrap_or(0)
}

/// Publish real [`InferenceProvider`] records as a CRDT state broadcast over SWIM.
///
/// Builds a [`crdt::GridStateSnapshot`] from all providers belonging to
/// `network_name`, attaches live metrics where configured, and sends the
/// snapshot to SWIM peers via [`SwimHandle::publish_state_broadcast`].
///
/// Providers are included regardless of their phase (even `Unavailable`) so
/// remote sites can learn which providers exist and avoid routing to unhealthy
/// ones.  The routing overlay layer already filters `Unavailable` providers
/// from local routing decisions.
fn publish_real_provider_state(
    swim: &SwimHandle,
    network_name: &str,
    providers: &[InferenceProvider],
    raw_metrics: &HashMap<String, scoring::BackendMetrics>,
) {
    use crdt::{Capability, GridStateSnapshot};
    use swim::StateBroadcast;

    let site_name = swim.site_name();
    let mut snap = GridStateSnapshot::new(site_name.to_owned());
    let mut max_revision: u64 = 0;

    for provider in providers {
        if provider.spec.grid_network_ref != network_name {
            continue;
        }
        // Key by routing identity so the metrics map lookup matches.
        let routing_id = routing_overlay::routing_identity(provider).unwrap_or("");
        let metrics = raw_metrics.get(routing_id).copied();
        if let Some(state) = provider_state_from_kube(provider, network_name, site_name, metrics) {
            max_revision = max_revision.max(state.revision);
            for model in &state.models {
                if !model.is_empty() {
                    snap.add_capability(Capability::Model(model.clone()));
                }
            }
            snap.upsert_provider(state);
        }
    }

    if snap.providers.is_empty() {
        // No providers in this network — nothing to broadcast.
        return;
    }

    // Use the highest provider revision as this origin's broadcast revision.
    // Duplicate unchanged broadcasts are idempotent; newer Kubernetes writes
    // advance resourceVersion and therefore advance the broadcast revision.
    let bc = StateBroadcast::new(site_name.to_owned(), max_revision, snap);
    if let Err(e) = swim.publish_state_broadcast(bc) {
        tracing::debug!(error = %e, "CRDT broadcast channel unavailable — runtime not yet receiving");
    }
}

/// Count provider records learned from remote sites through distributed state.
fn count_remote_provider_records(swim: &SwimHandle, network_name: &str) -> u32 {
    count_remote_provider_records_in_snapshot(swim.site_name(), network_name, &swim.state_snapshot())
}

/// Collect remote CRDT provider records from the SWIM state snapshot.
///
/// Filters to providers that:
/// - have `network_id == network_name` (belong to this [`GridNetwork`]);
/// - have `site_id != swim.site_name()` (originate from a remote site).
///
/// [`crdt::ProviderPhase::Unavailable`] providers are retained here —
/// [`routing_overlay::crdt_phase_to_fresh`] applies phase-based exclusion
/// during candidate generation, keeping the boundary clear between collection
/// and rendering.
///
/// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
pub(crate) fn collect_remote_crdt_providers(swim: &SwimHandle, network_name: &str) -> Vec<crdt::ProviderState> {
    collect_remote_providers_from_snapshot(swim.site_name(), network_name, &swim.state_snapshot())
}

/// Pure filtering logic for remote CRDT provider records.
///
/// Extracts [`crdt::ProviderState`] entries from `snapshot` whose
/// `network_id` matches `network_name` and `site_id` differs from
/// `local_site`.  Designed as a separately-testable inner function
/// following the same pattern as [`count_remote_provider_records_in_snapshot`].
fn collect_remote_providers_from_snapshot(
    local_site: &str,
    network_name: &str,
    snapshot: &crdt::GridStateSnapshot,
) -> Vec<crdt::ProviderState> {
    snapshot
        .providers
        .values()
        .filter(|p| p.network_id == network_name && p.site_id != local_site)
        .cloned()
        .collect()
}

/// Count provider records whose owner differs from the local site.
fn count_remote_provider_records_in_snapshot(
    local_site: &str,
    network_name: &str,
    snapshot: &crdt::GridStateSnapshot,
) -> u32 {
    let count = snapshot
        .providers
        .values()
        .filter(|provider| provider.network_id == network_name && provider.site_id != local_site)
        .count();
    u32::try_from(count).unwrap_or(u32::MAX)
}

/// Override CRDT provider phases based on current SWIM membership status.
///
/// Providers from `Dead` or `Suspect` SWIM members are downgraded to
/// [`crdt::ProviderPhase::Degraded`] so the routing overlay emits them with
/// `fresh = false`.  The record is kept rather than excluded so the data plane
/// can observe the stale-but-known state and prefer a healthy fallback candidate
/// when one exists.
///
/// Providers from `Alive` members, or from sites absent from the membership
/// snapshot (e.g. seed-only peers not yet tracked), are returned unchanged.
/// When `membership` is `None` (SWIM not configured), all providers are returned
/// unchanged.
pub(crate) fn apply_swim_staleness_override(
    providers: &[crdt::ProviderState],
    membership: Option<&MembershipSnapshot>,
) -> Vec<crdt::ProviderState> {
    let Some(snapshot) = membership else {
        return providers.to_vec();
    };
    providers
        .iter()
        .map(|p| {
            let is_degraded = snapshot
                .members
                .iter()
                .any(|m| m.site_id == p.site_id && matches!(m.status, MemberStatus::Dead | MemberStatus::Suspect));
            if is_degraded {
                crdt::ProviderState {
                    phase: crdt::ProviderPhase::Degraded,
                    ..p.clone()
                }
            } else {
                p.clone()
            }
        })
        .collect()
}

/// Patch the `GridNetwork` status subresource.
///
/// `connected_sites` is derived from `membership`: count of peers with
/// [`Alive`] status.  `distributed_provider_count` reflects providers received via
/// CRDT state broadcasts.  Both are `0` when SWIM is disabled.
///
/// [`Alive`]: MemberStatus::Alive
#[expect(
    clippy::too_many_arguments,
    reason = "all six arguments are distinct status fields; a wrapper struct would obscure the data flow"
)]
async fn update_status(
    network: &GridNetwork,
    client: &Client,
    grid_id: &str,
    phase: &GridNetworkPhase,
    membership: Option<&MembershipSnapshot>,
    distributed_provider_count: u32,
) -> Result<(), OperatorError> {
    let name = network
        .metadata
        .name
        .as_deref()
        .unwrap_or_else(|| std::process::abort());

    let connected_sites = membership.map_or(0, MembershipSnapshot::connected_count);

    let api: Api<GridNetwork> = Api::all(client.clone());
    let status = GridNetworkStatus {
        connected_sites,
        distributed_provider_count,
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

// ---------------------------------------------------------------------------
// Automatic GridSite discovery
// ---------------------------------------------------------------------------

/// A [`GridSite`] that the operator should auto-create or update from SWIM membership.
///
/// Produced by [`discovered_sites_from_swim`] and consumed by
/// [`reconcile_discovered_sites`].  Using a named struct instead of a tuple
/// makes unit tests and the reconcile loop unambiguous.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiscoveredSite {
    /// Kubernetes resource name derived deterministically from the SWIM `site_id`.
    pub name: String,
    /// The `GridNetwork` this site belongs to.
    pub grid_network_ref: String,
    /// Egress address for data-plane connectivity, sourced from the SWIM advertised address.
    pub egress_address: String,
}

/// Derive the set of remote [`GridSite`]s the operator should maintain from the SWIM snapshot.
///
/// Returns one [`DiscoveredSite`] per remote Alive SWIM member.  The local site
/// and non-Alive (Suspect, Dead) members are excluded — only confirmed Alive
/// peers should produce a `Discovered` record.
///
/// Name derivation is deterministic: the SWIM `site_id` is sanitised to a valid
/// Kubernetes resource name.
///
/// This is a **pure function** — no Kubernetes API calls — and is
/// suitable for unit testing in isolation.
pub(crate) fn discovered_sites_from_swim(
    network_name: &str,
    local_site: &str,
    snapshot: &MembershipSnapshot,
) -> Vec<DiscoveredSite> {
    snapshot
        .members
        .iter()
        .filter(|m| m.status == MemberStatus::Alive && m.site_id != local_site)
        .filter(|m| !m.site_id.trim().is_empty())
        .map(|m| DiscoveredSite {
            name: discovered_site_k8s_name(network_name, &m.site_id),
            grid_network_ref: network_name.to_owned(),
            egress_address: m.endpoint.clone(),
        })
        .collect()
}

/// Derive a Kubernetes resource name for an auto-discovered `GridSite`.
///
/// The name is `"{network}-{site_id}"` (both sanitised).  Using the composite
/// `(network, site_id)` key avoids name collisions when the same SWIM peer
/// appears as a member across multiple `GridNetwork` objects.  Each
/// `(network, site)` pair gets its own distinct `GridSite` resource.
///
/// Rules: lowercase, non-alphanumeric characters replaced with `-`,
/// leading/trailing hyphens stripped, truncated at 253 characters.
pub(crate) fn discovered_site_k8s_name(network_name: &str, site_id: &str) -> String {
    let sanitise = |s: &str| -> String {
        let raw: String = s
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect();
        raw.trim_matches('-').to_owned()
    };

    let net = sanitise(network_name);
    let site = sanitise(site_id);

    let candidate = match (net.is_empty(), site.is_empty()) {
        (false, false) => format!("{net}-{site}"),
        (false, true) => net,
        (true, false) => site,
        (true, true) => "discovered-site".to_owned(),
    };
    candidate.chars().take(253).collect()
}

/// Create or update `GridSite` resources for remote Alive SWIM members.
///
/// Uses server-side apply, so the call is idempotent: applying an already-existing
/// `GridSite` with the same spec is a no-op.  After the spec is applied, the
/// `status.phase` is set to `Discovered` to reflect that the member is observed
/// Alive via SWIM.
///
/// The `GridSite` controller's `determine_phase` is an identity function (Phase 1),
/// so subsequent `GridSite` reconciles preserve the `Discovered` phase.
///
/// # Phase 2 follow-up
///
/// This creates a site record at `Discovered`.  Advancing to `Active` (which
/// requires mTLS cert exchange, capability negotiation, and a data-plane ping)
/// is out of scope for Phase 1.
#[expect(
    clippy::too_many_lines,
    reason = "sequential spec-apply + status-patch per discovered site; splitting would hide the per-site K8s transaction"
)]
#[expect(
    clippy::large_stack_frames,
    reason = "async future over Kubernetes API types with serde_json values"
)]
async fn reconcile_discovered_sites(
    network_name: &str,
    local_site: &str,
    snapshot: &MembershipSnapshot,
    client: &Client,
) -> Result<(), OperatorError> {
    let sites = discovered_sites_from_swim(network_name, local_site, snapshot);
    if sites.is_empty() {
        return Ok(());
    }

    let api: Api<GridSite> = Api::all(client.clone());

    for site in &sites {
        // Server-side apply the spec.  Creating on first call; updating on subsequent
        // calls is a no-op when the spec has not changed.
        let spec_doc = serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridSite",
            "metadata": {
                "name": site.name,
                "labels": {
                    "grid.praxis-proxy.io/network": network_name,
                    "grid.praxis-proxy.io/auto-discovered": "true"
                }
            },
            "spec": {
                "gridNetworkRef": site.grid_network_ref,
                "egress": {
                    "address": site.egress_address,
                    "tls": { "mode": "Mutual" }
                }
            }
        });

        api.patch(
            &site.name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&spec_doc),
        )
        .await?;

        // Set status.phase = Discovered.  The GridSite controller's determine_phase
        // is an identity function, so it will preserve this on subsequent reconciles.
        let status_doc = serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridSite",
            "status": { "phase": "Discovered" }
        });

        api.patch_status(
            &site.name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&status_doc),
        )
        .await?;

        tracing::info!(
            name = %site.name,
            network = %network_name,
            egress = %site.egress_address,
            "reconciled auto-discovered GridSite from SWIM Alive member"
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::MemberRecord;

    fn make_inference_provider(name: &str, network_ref: &str) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network_ref,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": []
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn make_grid_site(name: &str, network_ref: &str) -> GridSite {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": name },
            "spec": { "gridNetworkRef": network_ref }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn ref_name(refs: Option<ObjectRef<GridNetwork>>) -> String {
        refs.unwrap_or_else(|| std::process::abort()).name
    }

    // -----------------------------------------------------------------------
    // network_refs_from_inference_provider
    // -----------------------------------------------------------------------

    #[test]
    fn inference_provider_maps_to_owning_grid_network() {
        let ip = make_inference_provider("provider-a", "net-a");
        let name = ref_name(network_refs_from_inference_provider(ip));
        assert_eq!(name, "net-a", "ObjectRef name must match gridNetworkRef");
    }

    #[test]
    fn inference_provider_blank_network_ref_returns_none() {
        let ip = make_inference_provider("provider-blank", "");
        let refs = network_refs_from_inference_provider(ip);
        assert!(
            refs.is_none(),
            "blank gridNetworkRef must return None (no spurious reconcile)"
        );
    }

    #[test]
    fn inference_provider_whitespace_network_ref_returns_none() {
        let mut ip = make_inference_provider("provider-ws", "net-a");
        ip.spec.grid_network_ref = "   ".to_owned();
        let refs = network_refs_from_inference_provider(ip);
        assert!(refs.is_none(), "whitespace-only gridNetworkRef must return None");
    }

    #[test]
    fn inference_provider_different_networks_map_correctly() {
        let ip_a = make_inference_provider("prov-1", "net-x");
        let ip_b = make_inference_provider("prov-2", "net-y");
        let name_a = ref_name(network_refs_from_inference_provider(ip_a));
        let name_b = ref_name(network_refs_from_inference_provider(ip_b));
        assert_ne!(name_a, name_b, "different providers must map to different networks");
        assert_eq!(name_a, "net-x", "first provider maps to net-x");
        assert_eq!(name_b, "net-y", "second provider maps to net-y");
    }

    // -----------------------------------------------------------------------
    // network_refs_from_grid_site
    // -----------------------------------------------------------------------

    #[test]
    fn grid_site_maps_to_owning_grid_network() {
        let site = make_grid_site("site-a", "net-a");
        let name = ref_name(network_refs_from_grid_site(site));
        assert_eq!(name, "net-a", "ObjectRef name must match gridNetworkRef");
    }

    #[test]
    fn grid_site_blank_network_ref_returns_none() {
        let site = make_grid_site("site-blank", "");
        let refs = network_refs_from_grid_site(site);
        assert!(
            refs.is_none(),
            "blank gridNetworkRef must return None (no spurious reconcile)"
        );
    }

    #[test]
    fn grid_site_whitespace_network_ref_returns_none() {
        let mut site = make_grid_site("site-ws", "net-a");
        site.spec.grid_network_ref = "  ".to_owned();
        let refs = network_refs_from_grid_site(site);
        assert!(refs.is_none(), "whitespace-only gridNetworkRef must return None");
    }

    #[test]
    fn grid_site_different_networks_map_correctly() {
        let site_a = make_grid_site("site-1", "net-x");
        let site_b = make_grid_site("site-2", "net-y");
        let name_a = ref_name(network_refs_from_grid_site(site_a));
        let name_b = ref_name(network_refs_from_grid_site(site_b));
        assert_ne!(name_a, name_b, "different sites must map to different networks");
        assert_eq!(name_a, "net-x", "first site maps to net-x");
        assert_eq!(name_b, "net-y", "second site maps to net-y");
    }

    // -----------------------------------------------------------------------
    // determine_phase with membership seam
    // -----------------------------------------------------------------------

    fn base_network() -> GridNetwork {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "net" },
            "spec": { "seeds": [], "gridId": "test-id" }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn alive_snapshot(count: usize) -> MembershipSnapshot {
        MembershipSnapshot {
            members: (0..count)
                .map(|i| MemberRecord {
                    site_id: format!("site-{i}"),
                    endpoint: format!("10.0.0.{i}:7946"),
                    incarnation: 1,
                    status: MemberStatus::Alive,
                    age_secs: 0,
                })
                .collect(),
        }
    }

    fn suspect_snapshot() -> MembershipSnapshot {
        MembershipSnapshot {
            members: vec![MemberRecord {
                site_id: "site-suspect".to_owned(),
                endpoint: "10.0.0.1:7946".to_owned(),
                incarnation: 1,
                status: MemberStatus::Suspect,
                age_secs: 5,
            }],
        }
    }

    #[test]
    fn determine_phase_none_membership_preserves_tls_logic() {
        let network = base_network();
        // Without TLS config, phase is Pending regardless of grid_id.
        let phase = determine_phase(&network, "some-id", None);
        assert_eq!(
            phase,
            GridNetworkPhase::Pending,
            "None membership and no TLS must yield Pending"
        );
    }

    #[test]
    fn determine_phase_empty_snapshot_preserves_tls_logic() {
        let network = base_network();
        let empty = MembershipSnapshot::default();
        let phase = determine_phase(&network, "some-id", Some(&empty));
        assert_eq!(
            phase,
            GridNetworkPhase::Pending,
            "empty snapshot must fall through to existing phase logic"
        );
    }

    #[test]
    fn determine_phase_with_alive_member_is_active() {
        let network = base_network();
        let snap = alive_snapshot(2);
        let phase = determine_phase(&network, "some-id", Some(&snap));
        assert_eq!(
            phase,
            GridNetworkPhase::Active,
            "Alive members must produce Active phase"
        );
    }

    #[test]
    fn determine_phase_with_suspect_only_is_degraded() {
        let network = base_network();
        let snap = suspect_snapshot();
        let phase = determine_phase(&network, "some-id", Some(&snap));
        assert_eq!(
            phase,
            GridNetworkPhase::Degraded,
            "all-Suspect members must produce Degraded phase"
        );
    }

    #[test]
    fn determine_phase_active_overrides_tls_initializing() {
        // When TLS would make the phase Initializing, an Alive membership still
        // promotes to Active because live peers are the authoritative signal.
        let mut network = base_network();
        network.spec.tls.ca_secret_ref = Some(crate::crd::grid_network::SecretRef {
            name: "ca".to_owned(),
            namespace: "default".to_owned(),
            key: None,
        });
        let snap = alive_snapshot(1);
        let phase = determine_phase(&network, "some-id", Some(&snap));
        assert_eq!(
            phase,
            GridNetworkPhase::Active,
            "Alive membership must override TLS-Initializing phase"
        );
    }

    #[test]
    fn connected_sites_is_zero_without_membership() {
        // Verify the update_status path: no membership → connected_sites = 0.
        let count = None::<MembershipSnapshot>
            .as_ref()
            .map_or(0, MembershipSnapshot::connected_count);
        assert_eq!(count, 0, "None membership must produce connected_sites=0");
    }

    #[test]
    fn connected_sites_counts_alive_members_from_snapshot() {
        let snap = alive_snapshot(3);
        let count = snap.connected_count();
        assert_eq!(count, 3, "three Alive members must give connected_sites=3");
    }

    fn provider_state(site_id: &str, provider_id: &str) -> crdt::ProviderState {
        crdt::ProviderState {
            network_id: "net".to_owned(),
            site_id: site_id.to_owned(),
            provider_id: provider_id.to_owned(),
            routing_cluster: site_id.to_owned(),
            models: vec!["model-x".to_owned()],
            backend_kind: "local".to_owned(),
            phase: crdt::ProviderPhase::Available,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            revision: 1,
            writer_id: site_id.to_owned(),
        }
    }

    fn remote_provider_state_with_phase(
        site_id: &str,
        provider_id: &str,
        phase: crdt::ProviderPhase,
    ) -> crdt::ProviderState {
        crdt::ProviderState {
            network_id: "net".to_owned(),
            site_id: site_id.to_owned(),
            provider_id: provider_id.to_owned(),
            routing_cluster: site_id.to_owned(),
            models: vec!["model-x".to_owned()],
            backend_kind: "local".to_owned(),
            phase,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            revision: 1,
            writer_id: site_id.to_owned(),
        }
    }

    // -----------------------------------------------------------------------
    // collect_remote_crdt_providers (via collect_remote_providers_from_snapshot)
    // -----------------------------------------------------------------------

    #[test]
    fn collect_remote_crdt_providers_excludes_local_site() {
        let mut snap = crdt::GridStateSnapshot::new("site-local".to_owned());
        snap.upsert_provider(remote_provider_state_with_phase(
            "site-local",
            "local-prov",
            crdt::ProviderPhase::Available,
        ));
        snap.upsert_provider(remote_provider_state_with_phase(
            "site-remote",
            "remote-prov",
            crdt::ProviderPhase::Available,
        ));
        let result = collect_remote_providers_from_snapshot("site-local", "net", &snap);
        assert_eq!(result.len(), 1, "only remote site records must be collected");
        assert_eq!(
            result.first().unwrap_or_else(|| std::process::abort()).site_id,
            "site-remote",
            "collected record must be from remote site"
        );
    }

    #[test]
    fn collect_remote_crdt_providers_excludes_wrong_network() {
        let mut snap = crdt::GridStateSnapshot::new("site-local".to_owned());
        let mut other_net =
            remote_provider_state_with_phase("site-remote", "remote-prov", crdt::ProviderPhase::Available);
        other_net.network_id = "other-net".to_owned();
        snap.upsert_provider(other_net);
        let result = collect_remote_providers_from_snapshot("site-local", "net", &snap);
        assert!(
            result.is_empty(),
            "providers from a different GridNetwork must be excluded"
        );
    }

    #[test]
    fn collect_remote_crdt_providers_includes_degraded() {
        let mut snap = crdt::GridStateSnapshot::new("site-local".to_owned());
        snap.upsert_provider(remote_provider_state_with_phase(
            "site-remote",
            "remote-prov",
            crdt::ProviderPhase::Degraded,
        ));
        let result = collect_remote_providers_from_snapshot("site-local", "net", &snap);
        assert_eq!(result.len(), 1, "Degraded remote providers must be collected");
        assert_eq!(
            result.first().unwrap_or_else(|| std::process::abort()).phase,
            crdt::ProviderPhase::Degraded,
            "Degraded phase must be preserved in collected record"
        );
    }

    #[test]
    fn collect_remote_crdt_providers_retains_unavailable_for_phase_filter() {
        // Unavailable providers are collected here; crdt_phase_to_fresh excludes them
        // during overlay candidate generation.  This test proves collection does not filter
        // by phase so the rendering layer has full control over inclusion decisions.
        let mut snap = crdt::GridStateSnapshot::new("site-local".to_owned());
        snap.upsert_provider(remote_provider_state_with_phase(
            "site-remote",
            "remote-prov",
            crdt::ProviderPhase::Unavailable,
        ));
        let result = collect_remote_providers_from_snapshot("site-local", "net", &snap);
        assert_eq!(
            result.len(),
            1,
            "Unavailable remote providers must be retained by collection; rendering layer applies phase filter"
        );
        assert_eq!(
            result.first().unwrap_or_else(|| std::process::abort()).phase,
            crdt::ProviderPhase::Unavailable,
            "phase must be preserved so rendering layer can apply crdt_phase_to_fresh"
        );
    }

    #[test]
    fn distributed_provider_count_ignores_local_records() {
        let mut snap = crdt::GridStateSnapshot::new("site-local".to_owned());
        snap.upsert_provider(provider_state("site-local", "local-provider"));
        let count = count_remote_provider_records_in_snapshot("site-local", "net", &snap);
        assert_eq!(
            count, 0,
            "local self-published records must not count as distributed state"
        );
    }

    #[test]
    fn distributed_provider_count_counts_remote_records() {
        let mut snap = crdt::GridStateSnapshot::new("site-local".to_owned());
        snap.upsert_provider(provider_state("site-local", "local-provider"));
        snap.upsert_provider(provider_state("site-remote", "remote-provider"));
        let count = count_remote_provider_records_in_snapshot("site-local", "net", &snap);
        assert_eq!(count, 1, "only remote provider records count as distributed state");
    }

    #[test]
    fn distributed_provider_count_ignores_other_network_records() {
        let mut snap = crdt::GridStateSnapshot::new("site-local".to_owned());
        let mut remote_other_network = provider_state("site-remote", "remote-provider");
        remote_other_network.network_id = "other-net".to_owned();
        snap.upsert_provider(remote_other_network);
        let count = count_remote_provider_records_in_snapshot("site-local", "net", &snap);
        assert_eq!(
            count, 0,
            "distributedProviderCount for one GridNetwork must not include records from another GridNetwork"
        );
    }

    // -----------------------------------------------------------------------
    // InferenceProvider → crdt::ProviderState mapping
    // -----------------------------------------------------------------------

    fn make_provider(name: &str, network: &str, backend_kind: &str, generation: i64) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name, "generation": generation },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "self_hosted",
                "backendKind": backend_kind,
                "endpoint": "http://localhost:8080",
                "models": [{ "name": "model-a" }, { "name": "model-b" }]
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn make_provider_with_routing_ref(name: &str, network: &str, routing_ref: &str) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8080",
                "models": [{ "name": "model-x" }],
                "routingClusterRef": routing_ref
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn make_provider_with_status(name: &str, network: &str, phase: &str) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8080",
                "models": [{ "name": "model-x" }]
            },
            "status": { "phase": phase }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn provider_state_from_kube_maps_basic_fields() {
        let p = make_provider("my-provider", "net", "local", 3);
        let state = provider_state_from_kube(&p, "net", "site-a", None);
        let state = state.unwrap_or_else(|| std::process::abort());
        assert_eq!(state.network_id, "net", "network_id from owning GridNetwork");
        assert_eq!(state.provider_id, "my-provider", "provider_id from metadata.name");
        assert_eq!(state.site_id, "site-a", "site_id from swim site name");
        assert_eq!(state.writer_id, "site-a", "writer_id from SWIM site name");
        assert_eq!(state.backend_kind, "local", "backend_kind from spec");
        assert_eq!(state.models, vec!["model-a", "model-b"], "models from spec");
        assert_eq!(state.revision, 3, "revision from generation");
    }

    #[test]
    fn provider_state_from_kube_uses_metadata_name_as_routing_cluster_by_default() {
        let p = make_provider("prov-a", "net", "api_provider", 0);
        let state = provider_state_from_kube(&p, "net", "site-a", None).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            state.routing_cluster, "prov-a",
            "routing_cluster defaults to metadata.name"
        );
    }

    #[test]
    fn provider_state_from_kube_uses_routing_cluster_ref_when_set() {
        let p = make_provider_with_routing_ref("prov-x", "net", "site-override");
        let state = provider_state_from_kube(&p, "net", "site-a", None).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            state.routing_cluster, "site-override",
            "routingClusterRef must override metadata.name"
        );
    }

    #[test]
    fn provider_state_from_kube_returns_none_for_missing_name() {
        let p: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": {},
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8080",
                "models": []
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert!(
            provider_state_from_kube(&p, "net", "site-a", None).is_none(),
            "provider with no metadata.name must yield None"
        );
    }

    #[test]
    fn crdt_phase_from_provider_maps_all_variants() {
        use crate::crd::inference_provider::ProviderPhase as Op;

        assert_eq!(
            crdt_phase_from_provider(None),
            crdt::ProviderPhase::Pending,
            "absent status → Pending"
        );
        assert_eq!(
            crdt_phase_from_provider(Some(&Op::Pending)),
            crdt::ProviderPhase::Pending
        );
        assert_eq!(
            crdt_phase_from_provider(Some(&Op::Available)),
            crdt::ProviderPhase::Available
        );
        assert_eq!(
            crdt_phase_from_provider(Some(&Op::Degraded)),
            crdt::ProviderPhase::Degraded
        );
        assert_eq!(
            crdt_phase_from_provider(Some(&Op::Unavailable)),
            crdt::ProviderPhase::Unavailable
        );
    }

    #[test]
    fn provider_state_from_kube_propagates_provider_phase_via_status() {
        let p = make_provider_with_status("prov-a", "net", "Degraded");
        let state = provider_state_from_kube(&p, "net", "s", None).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            state.phase,
            crdt::ProviderPhase::Degraded,
            "Degraded must propagate to CRDT phase"
        );
    }

    #[test]
    fn provider_state_from_kube_unavailable_is_included_not_skipped() {
        let p = make_provider_with_status("prov-a", "net", "Unavailable");
        let state = provider_state_from_kube(&p, "net", "s", None);
        assert!(
            state.is_some(),
            "Unavailable providers must be published so remote sites know to avoid them"
        );
        let state = state.unwrap_or_else(|| std::process::abort());
        assert_eq!(state.phase, crdt::ProviderPhase::Unavailable);
    }

    #[test]
    fn metrics_to_crdt_maps_all_signals() {
        let bm = scoring::BackendMetrics::new(0.1, true, 0.4, 120.0, 0.7, 0.3);
        let m = metrics_to_crdt(Some(bm));
        assert_eq!(m.error_rate, Some(0.1), "error_rate");
        assert_eq!(m.healthy, Some(true), "healthy");
        assert_eq!(m.kv_cache_utilization, Some(0.4), "kv_cache");
        assert_eq!(m.latency_p99_ms, Some(120.0), "latency_p99_ms");
        assert_eq!(m.prefix_cache_hit_ratio, Some(0.7), "prefix_cache");
        assert_eq!(m.queue_depth, Some(0.3), "queue_depth");
    }

    #[test]
    fn metrics_to_crdt_returns_all_none_when_no_metrics() {
        let m = metrics_to_crdt(None);
        assert!(m.error_rate.is_none(), "no metrics → error_rate=None");
        assert!(m.queue_depth.is_none(), "no metrics → queue_depth=None");
        assert!(m.healthy.is_none(), "no metrics → healthy=None");
    }

    #[test]
    fn revision_falls_back_to_generation_field() {
        let p = make_provider("prov-g", "net", "local", 42);
        let state = provider_state_from_kube(&p, "net", "s", None).unwrap_or_else(|| std::process::abort());
        assert_eq!(state.revision, 42, "revision must fall back to Kubernetes generation");
    }

    #[test]
    fn revision_prefers_resource_version_over_generation() {
        let mut p = make_provider("prov-rv", "net", "local", 42);
        p.metadata.resource_version = Some("99".to_owned());
        let state = provider_state_from_kube(&p, "net", "s", None).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            state.revision, 99,
            "resourceVersion advances on status writes and must win over generation"
        );
    }

    #[test]
    fn revision_defaults_to_zero_when_no_generation() {
        let p: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": "prov-no-gen" },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8080",
                "models": []
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let state = provider_state_from_kube(&p, "net", "s", None).unwrap_or_else(|| std::process::abort());
        assert_eq!(state.revision, 0, "missing generation must default to revision=0");
    }

    // -----------------------------------------------------------------------
    // apply_swim_staleness_override - pure function tests
    // -----------------------------------------------------------------------

    fn make_crdt_provider(site_id: &str, phase: crdt::ProviderPhase) -> crdt::ProviderState {
        crdt::ProviderState {
            network_id: "test-net".to_owned(),
            site_id: site_id.to_owned(),
            provider_id: "prov-1".to_owned(),
            routing_cluster: site_id.to_owned(),
            models: vec!["model-x".to_owned()],
            backend_kind: "remote".to_owned(),
            phase,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            revision: 1,
            writer_id: "writer-1".to_owned(),
        }
    }

    fn make_swim_membership(site_id: &str, status: MemberStatus) -> MembershipSnapshot {
        MembershipSnapshot {
            members: vec![MemberRecord {
                site_id: site_id.to_owned(),
                endpoint: "127.0.0.1:7946".to_owned(),
                incarnation: 0,
                status,
                age_secs: 0,
            }],
        }
    }

    #[test]
    fn staleness_override_dead_site_becomes_degraded() {
        let provider = make_crdt_provider("site-west", crdt::ProviderPhase::Available);
        let membership = make_swim_membership("site-west", MemberStatus::Dead);
        let result = apply_swim_staleness_override(&[provider], Some(&membership));
        assert_eq!(
            result.first().map(|p| &p.phase),
            Some(&crdt::ProviderPhase::Degraded),
            "Dead SWIM member must cause provider phase to become Degraded"
        );
    }

    #[test]
    fn staleness_override_suspect_site_becomes_degraded() {
        let provider = make_crdt_provider("site-west", crdt::ProviderPhase::Available);
        let membership = make_swim_membership("site-west", MemberStatus::Suspect);
        let result = apply_swim_staleness_override(&[provider], Some(&membership));
        assert_eq!(
            result.first().map(|p| &p.phase),
            Some(&crdt::ProviderPhase::Degraded),
            "Suspect SWIM member must cause provider phase to become Degraded"
        );
    }

    #[test]
    fn staleness_override_alive_site_unchanged() {
        let provider = make_crdt_provider("site-west", crdt::ProviderPhase::Available);
        let membership = make_swim_membership("site-west", MemberStatus::Alive);
        let result = apply_swim_staleness_override(&[provider], Some(&membership));
        assert_eq!(
            result.first().map(|p| &p.phase),
            Some(&crdt::ProviderPhase::Available),
            "Alive SWIM member must not degrade provider phase"
        );
    }

    #[test]
    fn staleness_override_unknown_site_unchanged() {
        let provider = make_crdt_provider("site-unknown", crdt::ProviderPhase::Available);
        let membership = make_swim_membership("site-west", MemberStatus::Dead);
        let result = apply_swim_staleness_override(&[provider], Some(&membership));
        assert_eq!(
            result.first().map(|p| &p.phase),
            Some(&crdt::ProviderPhase::Available),
            "Provider from a site not in SWIM snapshot must not be degraded"
        );
    }

    #[test]
    fn staleness_override_no_swim_unchanged() {
        let provider = make_crdt_provider("site-west", crdt::ProviderPhase::Available);
        let result = apply_swim_staleness_override(&[provider], None);
        assert_eq!(
            result.first().map(|p| &p.phase),
            Some(&crdt::ProviderPhase::Available),
            "No SWIM configured (membership=None) must preserve all provider phases"
        );
    }

    #[test]
    fn staleness_override_dead_then_alive_restores_phase() {
        // Recovery: provider was Degraded when west was Dead; after rejoin west is Alive
        // and the override must no longer apply — phase returns to Available.
        // This is the pure-function equivalent of the rejoin recovery proof.
        let provider = make_crdt_provider("site-west", crdt::ProviderPhase::Available);

        // Partition: Dead → Degraded
        let dead_membership = make_swim_membership("site-west", MemberStatus::Dead);
        let degraded = apply_swim_staleness_override(std::slice::from_ref(&provider), Some(&dead_membership));
        assert_eq!(
            degraded.first().map(|p| &p.phase),
            Some(&crdt::ProviderPhase::Degraded),
            "Dead peer must produce Degraded phase (partition)"
        );

        // Recovery: Alive → Available (override lifted)
        let alive_membership = make_swim_membership("site-west", MemberStatus::Alive);
        let recovered = apply_swim_staleness_override(std::slice::from_ref(&provider), Some(&alive_membership));
        assert_eq!(
            recovered.first().map(|p| &p.phase),
            Some(&crdt::ProviderPhase::Available),
            "Alive peer after rejoin must restore Available phase (recovery)"
        );
    }

    #[test]
    fn staleness_override_suspect_then_alive_restores_phase() {
        // Same recovery path but starting from Suspect rather than Dead.
        let provider = make_crdt_provider("site-west", crdt::ProviderPhase::Available);
        let suspect_membership = make_swim_membership("site-west", MemberStatus::Suspect);
        let degraded = apply_swim_staleness_override(std::slice::from_ref(&provider), Some(&suspect_membership));
        assert_eq!(
            degraded.first().map(|p| &p.phase),
            Some(&crdt::ProviderPhase::Degraded),
            "Suspect peer must produce Degraded phase"
        );
        let alive_membership = make_swim_membership("site-west", MemberStatus::Alive);
        let recovered = apply_swim_staleness_override(&[provider], Some(&alive_membership));
        assert_eq!(
            recovered.first().map(|p| &p.phase),
            Some(&crdt::ProviderPhase::Available),
            "Alive peer must restore Available phase after Suspect"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "two-provider membership fixture with inline vec construction"
    )]
    fn staleness_override_multiple_providers_only_dead_site_degraded() {
        // Multi-provider recovery: west is Dead, east is Alive.
        // Only west's provider becomes Degraded; east's provider stays Available.
        let west_provider = make_crdt_provider("site-west", crdt::ProviderPhase::Available);
        let east_provider = make_crdt_provider("site-east", crdt::ProviderPhase::Available);
        let membership = MembershipSnapshot {
            members: vec![
                MemberRecord {
                    site_id: "site-west".to_owned(),
                    endpoint: "10.0.0.2:7946".to_owned(),
                    incarnation: 0,
                    status: MemberStatus::Dead,
                    age_secs: 0,
                },
                MemberRecord {
                    site_id: "site-east".to_owned(),
                    endpoint: "10.0.0.1:7946".to_owned(),
                    incarnation: 0,
                    status: MemberStatus::Alive,
                    age_secs: 0,
                },
            ],
        };
        let result = apply_swim_staleness_override(&[west_provider, east_provider], Some(&membership));
        let west = result
            .iter()
            .find(|p| p.site_id == "site-west")
            .unwrap_or_else(|| std::process::abort());
        let east = result
            .iter()
            .find(|p| p.site_id == "site-east")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(west.phase, crdt::ProviderPhase::Degraded, "Dead west must be Degraded");
        assert_eq!(
            east.phase,
            crdt::ProviderPhase::Available,
            "Alive east must stay Available"
        );
    }

    // -----------------------------------------------------------------------
    // resolve_grid_id — pure ID resolution (three branches)
    // -----------------------------------------------------------------------

    #[test]
    fn resolve_grid_id_prefers_spec_grid_id() {
        let network = base_network();
        let id = resolve_grid_id(&network);
        assert_eq!(
            id, "test-id",
            "spec.gridId must be returned verbatim when non-empty, with no status lookup or UUID generation"
        );
    }

    #[test]
    fn resolve_grid_id_falls_back_to_status_grid_id_when_spec_is_empty() {
        let mut network = base_network();
        network.spec.grid_id = String::new();
        network.status = Some(GridNetworkStatus {
            grid_id: "persisted-id".to_owned(),
            ..Default::default()
        });
        let id = resolve_grid_id(&network);
        assert_eq!(
            id, "persisted-id",
            "status.gridId must be returned when spec.gridId is empty, \
             preserving a previously negotiated ID across operator restarts"
        );
    }

    #[test]
    fn resolve_grid_id_generates_uuid_when_both_spec_and_status_are_empty() {
        let mut network = base_network();
        network.spec.grid_id = String::new();
        network.status = None;
        let id = resolve_grid_id(&network);
        assert!(!id.is_empty(), "a freshly generated grid ID must not be empty");
        assert!(
            uuid::Uuid::parse_str(&id).is_ok(),
            "generated grid ID must be a valid UUID, got: {id}"
        );
    }

    // -----------------------------------------------------------------------
    // network_site_name — fallback helper
    // -----------------------------------------------------------------------

    #[test]
    fn network_site_name_returns_metadata_name_when_present() {
        let network = base_network();
        let name = network_site_name(&network);
        assert_eq!(name, "net", "metadata.name must be returned verbatim when present");
    }

    #[test]
    fn network_site_name_falls_back_to_unknown_site_when_metadata_name_absent() {
        let network: GridNetwork = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridNetwork",
            "metadata": {},
            "spec": { "seeds": [] }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let name = network_site_name(&network);
        assert_eq!(
            name, "unknown-site",
            "absent metadata.name must yield the safe fallback site name to prevent panics in TLS secret generation"
        );
    }

    // -----------------------------------------------------------------------
    // discovered_sites_from_swim — pure helper
    // -----------------------------------------------------------------------

    fn make_member(site_id: &str, endpoint: &str, status: MemberStatus) -> MemberRecord {
        MemberRecord {
            site_id: site_id.to_owned(),
            endpoint: endpoint.to_owned(),
            incarnation: 0,
            status,
            age_secs: 0,
        }
    }

    fn make_snapshot(members: Vec<MemberRecord>) -> MembershipSnapshot {
        MembershipSnapshot { members }
    }

    #[test]
    fn discovered_sites_includes_alive_remote_member() {
        let snap = make_snapshot(vec![
            make_member("local", "127.0.0.1:7946", MemberStatus::Alive),
            make_member("remote", "10.0.0.2:7946", MemberStatus::Alive),
        ]);
        let sites = discovered_sites_from_swim("net", "local", &snap);
        assert_eq!(sites.len(), 1, "exactly one remote Alive member");
        let site = sites.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(
            site.name, "net-remote",
            "site name must be composite network-site to avoid collisions across networks"
        );
        assert_eq!(
            site.grid_network_ref, "net",
            "grid_network_ref must match the network name"
        );
        assert_eq!(
            site.egress_address, "10.0.0.2:7946",
            "egress_address must match the SWIM advertised address"
        );
    }

    #[test]
    fn discovered_sites_excludes_local_site() {
        let snap = make_snapshot(vec![make_member("local", "127.0.0.1:7946", MemberStatus::Alive)]);
        let sites = discovered_sites_from_swim("net", "local", &snap);
        assert!(sites.is_empty(), "local site must never produce a DiscoveredSite");
    }

    #[test]
    fn discovered_sites_excludes_suspect_members() {
        let snap = make_snapshot(vec![make_member("remote", "10.0.0.3:7946", MemberStatus::Suspect)]);
        let sites = discovered_sites_from_swim("net", "local", &snap);
        assert!(sites.is_empty(), "Suspect member must not produce a DiscoveredSite");
    }

    #[test]
    fn discovered_sites_excludes_dead_members() {
        let snap = make_snapshot(vec![make_member("remote", "10.0.0.4:7946", MemberStatus::Dead)]);
        let sites = discovered_sites_from_swim("net", "local", &snap);
        assert!(sites.is_empty(), "Dead member must not produce a DiscoveredSite");
    }

    #[test]
    fn discovered_sites_empty_snapshot_returns_empty() {
        let sites = discovered_sites_from_swim("net", "local", &make_snapshot(vec![]));
        assert!(sites.is_empty(), "empty snapshot must produce no sites");
    }

    #[test]
    fn discovered_sites_name_is_deterministic() {
        let snap = make_snapshot(vec![make_member("site-west", "127.0.0.1:9999", MemberStatus::Alive)]);
        let a = discovered_sites_from_swim("net", "local", &snap);
        let b = discovered_sites_from_swim("net", "local", &snap);
        let a_name = a.first().unwrap_or_else(|| std::process::abort()).name.as_str();
        let b_name = b.first().unwrap_or_else(|| std::process::abort()).name.as_str();
        assert_eq!(a_name, b_name, "name must be deterministic across calls");
    }

    #[test]
    fn discovered_site_k8s_name_lowercases_and_sanitises_underscores() {
        assert_eq!(discovered_site_k8s_name("net", "Site_West"), "net-site-west");
        assert_eq!(discovered_site_k8s_name("net", "SITE.EAST"), "net-site-east");
    }

    #[test]
    fn discovered_site_k8s_name_strips_leading_trailing_hyphens() {
        assert_eq!(discovered_site_k8s_name("net", "--valid--"), "net-valid");
    }

    #[test]
    fn discovered_site_k8s_name_both_empty_yields_fallback() {
        assert_eq!(
            discovered_site_k8s_name("", ""),
            "discovered-site",
            "both empty must produce the safe fallback name"
        );
        assert_eq!(
            discovered_site_k8s_name("---", "---"),
            "discovered-site",
            "all-hyphen input must produce the safe fallback name"
        );
    }

    #[test]
    fn discovered_site_k8s_name_truncates_at_253_chars() {
        let long_net = "n".repeat(150);
        let long_site = "s".repeat(150);
        let result = discovered_site_k8s_name(&long_net, &long_site);
        assert_eq!(result.len(), 253, "composite name must be truncated to 253 chars");
    }

    #[test]
    fn discovered_site_k8s_name_is_unique_per_network() {
        let name_net1 = discovered_site_k8s_name("network-a", "site-west");
        let name_net2 = discovered_site_k8s_name("network-b", "site-west");
        assert_ne!(
            name_net1, name_net2,
            "same site_id in different networks must produce different names"
        );
    }

    // -----------------------------------------------------------------------
    // parse_crd_seeds — pure seed normalization
    // -----------------------------------------------------------------------

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn parse_crd_seeds_empty_input_returns_empty() {
        let result = parse_crd_seeds(&[], None);
        assert!(result.is_empty(), "empty input must produce empty output");
    }

    #[test]
    fn parse_crd_seeds_valid_address_parsed() {
        let raw = vec!["10.0.0.1:7946".to_owned()];
        let result = parse_crd_seeds(&raw, None);
        assert_eq!(result, vec![addr("10.0.0.1:7946")], "valid address must be included");
    }

    #[test]
    fn parse_crd_seeds_invalid_address_skipped() {
        let raw = vec!["not-an-address".to_owned()];
        let result = parse_crd_seeds(&raw, None);
        assert!(result.is_empty(), "invalid address must be skipped without panic");
    }

    #[test]
    fn parse_crd_seeds_mixed_valid_and_invalid() {
        let raw = vec![
            "10.0.0.1:7946".to_owned(),
            "bad-addr".to_owned(),
            "10.0.0.2:7946".to_owned(),
        ];
        let result = parse_crd_seeds(&raw, None);
        assert_eq!(result.len(), 2, "only valid addresses must appear");
        assert!(result.contains(&addr("10.0.0.1:7946")));
        assert!(result.contains(&addr("10.0.0.2:7946")));
    }

    #[test]
    fn parse_crd_seeds_deduplicates() {
        let raw = vec!["10.0.0.1:7946".to_owned(), "10.0.0.1:7946".to_owned()];
        let result = parse_crd_seeds(&raw, None);
        assert_eq!(result.len(), 1, "duplicates must be removed");
    }

    #[test]
    fn parse_crd_seeds_filters_self_addr() {
        let local = addr("10.0.0.1:7946");
        let raw = vec!["10.0.0.1:7946".to_owned(), "10.0.0.2:7946".to_owned()];
        let result = parse_crd_seeds(&raw, Some(local));
        assert_eq!(result.len(), 1, "self-address must be filtered out");
        assert_eq!(
            result.first().copied(),
            Some(addr("10.0.0.2:7946")),
            "non-self address must remain"
        );
    }

    #[test]
    fn parse_crd_seeds_no_local_filter_when_none() {
        let raw = vec!["10.0.0.1:7946".to_owned()];
        let result = parse_crd_seeds(&raw, None);
        assert_eq!(result.len(), 1, "without local filter all valid addresses are kept");
    }

    #[test]
    fn parse_crd_seeds_whitespace_trimmed() {
        let raw = vec!["  10.0.0.1:7946  ".to_owned()];
        let result = parse_crd_seeds(&raw, None);
        assert_eq!(
            result,
            vec![addr("10.0.0.1:7946")],
            "leading/trailing whitespace must be trimmed"
        );
    }

    #[test]
    fn parse_crd_seeds_empty_string_skipped() {
        let raw = vec![String::new(), "  ".to_owned()];
        let result = parse_crd_seeds(&raw, None);
        assert!(result.is_empty(), "blank strings must be skipped");
    }

    #[test]
    fn parse_crd_seeds_result_is_sorted() {
        let raw = vec!["10.0.0.2:7946".to_owned(), "10.0.0.1:7946".to_owned()];
        let result = parse_crd_seeds(&raw, None);
        let mut expected = result.clone();
        expected.sort();
        assert_eq!(result, expected, "result must be sorted for deterministic ordering");
    }
}
