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
    time::{Instant, SystemTime, UNIX_EPOCH},
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
        grid_network::{
            ConsumerConfig, ConsumerConfigPhase, ConsumerConfigStatus, GatewayRef, GridNetwork, GridNetworkPhase,
            GridNetworkStatus, OverlayPhase, OverlayRevisionStatus, TenantBudgetStatus, TransportMode,
        },
        grid_site::{GridSite, GridSitePhase, GridSiteStatus},
        inference_provider::InferenceProvider,
    },
    error::OperatorError,
    resources::{
        consumer_config::{self, ConsumerConfigError},
        overlay_envelope, provider_admission, provider_metrics, routing_overlay, secret,
        trust_bundle::{self, CertPemStatus},
    },
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
#[expect(
    clippy::partial_pub_fields,
    reason = "client and swim are public API; metrics_cache and last_seeds are crate-internal"
)]
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

    /// Stateful admission memory keyed by provider routing identity.
    ///
    /// Admission is evaluated in the control plane and the resulting wire
    /// state is copied into the overlay. It is never consulted by a request.
    pub(crate) admission_memory: Mutex<provider_admission::AdmissionMemory>,

    /// Tracks the seed set announced on the last reconcile per `GridNetwork`.
    ///
    /// Keyed by `GridNetwork` name.  On each reconcile, the new seed set is
    /// compared against the stored set via [`diff_seed_sets`] to log additions
    /// and removals.  Seeds are always announced in full (idempotent); this
    /// state is used only for diagnostics.
    ///
    /// Uses [`std::sync::Mutex`] because [`announce_crd_seeds`] is a synchronous
    /// function called from within the async reconcile loop.
    pub(crate) last_seeds: std::sync::Mutex<HashMap<String, Vec<SocketAddr>>>,
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
            admission_memory: Mutex::new(provider_admission::AdmissionMemory::default()),
            last_seeds: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Requeue interval after a successful reconciliation.
const REQUEUE_INTERVAL: Duration = Duration::from_secs(300);

/// Shorter requeue interval when any provider in the network has
/// `metricsConfig.tls` configured.  Without a cluster-wide Secret watch,
/// this bounded interval detects TLS material rotation for the metrics
/// collection and overlay publication that happen in the `GridNetwork`
/// reconcile loop.
const TLS_REQUEUE_INTERVAL: Duration = Duration::from_secs(60);

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
// Resource name helpers
// ---------------------------------------------------------------------------

/// Extract the resource name from a watched [`GridNetwork`].
///
/// kube-rs guarantees `metadata.name` is present on watched resources.
/// Returns an error for defensive requeue instead of aborting the process.
fn grid_network_name(network: &GridNetwork) -> Result<&str, OperatorError> {
    network
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| OperatorError::InvalidResource("GridNetwork missing metadata.name".into()))
}

/// Reject a [`GridNetwork`] whose `budgetPolicy` fails validation, before any
/// other reconcile work begins.
///
/// Pure and I/O-free (network fields only), so the reconcile-time wiring this
/// guards is exercised directly by unit tests without a live or mocked
/// Kubernetes client, per this repo's convention of preferring pure decision
/// functions for reconciliation logic (`docs/conventions.md`). The CRD
/// schema's numeric minimum on `capUsd` already rejects negative values at
/// admission time; this is the defensive second layer for `NaN`/infinite
/// caps and blank/duplicate `tenantId`s that the schema cannot express.
fn reject_invalid_budget_policy(network: &GridNetwork) -> Result<(), OperatorError> {
    let Some(policy) = network.spec.budget_policy.as_ref() else {
        return Ok(());
    };
    crate::crd::grid_network::validate_budget_policy(policy)
        .map_err(|error| OperatorError::InvalidResource(format!("invalid budgetPolicy: {error}")))
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
#[expect(
    clippy::cognitive_complexity,
    reason = "sequential reconcile steps with cert broadcast; extracting cert into a helper would obscure the security boundary"
)]
pub async fn reconcile(network: Arc<GridNetwork>, ctx: Arc<OperatorCtx>) -> Result<Action, OperatorError> {
    let name = grid_network_name(&network)?;
    reject_invalid_budget_policy(&network)?;

    info!(name, "reconciling GridNetwork");

    let client = &ctx.client;
    ensure_tls_secrets(&network, client).await?;

    if let Some(swim) = ctx.swim.as_ref() {
        // When a GridNetwork configures a SWIM key Secret, resolve and apply it
        // before any reconcile-triggered SWIM send.  If the key cannot be
        // loaded, fail this reconcile before announcing CRD seeds or publishing
        // cert/provider state so the configured network does not silently send
        // plaintext traffic.
        apply_configured_swim_key(&network, client, swim).await?;

        // Announce CRD-declared seeds to the SWIM runtime so peers can be reached
        // without requiring the GRID_SWIM_SEEDS environment variable.
        // Re-announcing on each reconcile is idempotent (foca ignores existing members).
        // diff_seed_sets tracks additions/removals for diagnostic logging.
        announce_crd_seeds(&network, swim, &ctx.last_seeds);

        // Broadcast the local site's public certificate PEM so remote peers can
        // populate GridSite.status.publicCertPem.  Only the public cert is read —
        // the private key (tls.key) is never accessed by this code path.
        if let Ok(Some(cert_pem)) = secret::read_site_cert_pem(client, network.spec.tls.site_secret_ref.as_ref()).await
        {
            let cert_broadcast = swim::StateBroadcast::new(
                swim.site_name().to_owned(),
                cert_broadcast_revision(),
                crdt::GridStateSnapshot::new(swim.site_name().to_owned()),
                None,
            )
            .with_cert(Some(cert_pem));
            if let Err(e) = swim.publish_state_broadcast(cert_broadcast) {
                tracing::warn!(network = name, error = %e, "failed to publish site cert broadcast");
            }
        }
    }

    // List providers once; share between routing overlay rendering and CRDT publishing.
    let providers = list_all_inference_providers(client).await?;
    let requeue_interval = requeue_interval_for_network(&network, &providers)?;
    let collected = provider_metrics::collect_provider_metrics_with_refresh_interval(
        name,
        &providers,
        &ctx.metrics_cache,
        Instant::now(),
        requeue_interval,
        Some(client),
    )
    .await;
    let raw_metrics = collected.metrics;

    let scoring_strategy = network
        .spec
        .scoring_policy
        .as_ref()
        .map_or(crate::crd::grid_network::ScoringStrategy::NoMetrics, |policy| {
            policy.strategy
        });
    let admission_policy =
        provider_admission::Policy::from_config(network.spec.admission_policy.as_ref(), scoring_strategy.into())
            .map_err(OperatorError::InvalidResource)?;
    let now = Instant::now();
    let mut admission_states = HashMap::new();
    let mut admission_keys = Vec::new();
    {
        let mut memory = ctx.admission_memory.lock().await;
        for provider in providers
            .iter()
            .filter(|provider| provider.spec.grid_network_ref == name)
        {
            let Some(identity) = routing_overlay::routing_identity(provider) else {
                continue;
            };
            let identity = identity.to_owned();
            let memory_key = format!(
                "{name}/{}/{}",
                provider.metadata.uid.as_deref().map_or(identity.as_str(), |uid| uid),
                identity
            );
            let signal_configured = match scoring_strategy {
                crate::crd::grid_network::ScoringStrategy::QueueDepth => provider
                    .spec
                    .metrics_config
                    .as_ref()
                    .and_then(|config| config.signal_names.queue_depth.as_ref())
                    .is_some(),
                crate::crd::grid_network::ScoringStrategy::KvCachePressure => provider
                    .spec
                    .metrics_config
                    .as_ref()
                    .and_then(|config| config.signal_names.kv_cache_utilization.as_ref())
                    .is_some(),
                crate::crd::grid_network::ScoringStrategy::NoMetrics => false,
            };
            let observation = if signal_configured {
                raw_metrics
                    .get(&identity)
                    .copied()
                    .map_or(provider_admission::Observation::Missing, |metrics| {
                        provider_admission::Observation::Fresh {
                            revision: collected.generations.get(&identity).copied().unwrap_or(0),
                            metrics,
                        }
                    })
            } else {
                provider_admission::Observation::NotConfigured
            };
            let state = memory.evaluate(&memory_key, observation, admission_policy, now);
            admission_keys.push(memory_key);
            admission_states.insert(identity, state);
        }
        memory.retain_network_keys(name, admission_keys.iter().cloned());
    }

    let remote_crdt_providers: Vec<crdt::ProviderState> = ctx
        .swim
        .as_ref()
        .map(|swim| collect_remote_crdt_providers(swim, name))
        .unwrap_or_default();

    // Obtain a live membership snapshot here - used both for staleness override below
    // and for phase determination after the overlay step.
    // When swim is None (runtime not configured), falls through to static phase logic.
    let swim_runtime_running = ctx.swim.as_ref().is_none_or(|handle| handle.is_running());
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

    let scoring_weights = crate::crd::grid_network::resolve_scoring_weights(network.spec.scoring_policy.as_ref());

    let (consumer_config_statuses, overlay_statuses) = reconcile_routing_overlay_inner(
        &network,
        client,
        &providers,
        &remote_crdt_providers,
        &raw_metrics,
        &scoring_weights,
        &admission_states,
    )
    .await?;

    let grid_id = resolve_grid_id(&network);
    let phase = if swim_runtime_running {
        determine_phase(&network, &grid_id, membership.as_ref())
    } else {
        GridNetworkPhase::Degraded
    };

    // Publish real InferenceProvider-derived CRDT state so peers learn this site's providers.
    let distributed_provider_count = if let Some(swim) = ctx.swim.as_ref().filter(|handle| handle.is_running()) {
        publish_real_provider_state(swim, name, &providers, &raw_metrics);
        count_remote_provider_records(swim, name)
    } else {
        0
    };

    // Resolve per-tenant budget status from the merged CRDT spend state, if any.
    // Empty tenant_spend (SWIM disabled, or no spend broadcast received yet) is
    // indistinguishable here from "no spend recorded" — resolve_budget_statuses
    // still emits a zero-spend entry for every policy-declared tenant.
    let tenant_spend = ctx
        .swim
        .as_ref()
        .map(|swim| swim.state_snapshot().tenant_spend)
        .unwrap_or_default();
    let budget_statuses =
        crate::crd::grid_network::resolve_budget_statuses(network.spec.budget_policy.as_ref(), &tenant_spend);

    update_status(
        &network,
        client,
        &grid_id,
        &phase,
        membership.as_ref(),
        distributed_provider_count,
        consumer_config_statuses,
        overlay_statuses,
        budget_statuses,
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
        let plaintext = network_uses_plaintext_egress(&network);
        reconcile_discovered_sites(name, swim.site_name(), snapshot, client, plaintext).await?;
    }

    Ok(Action::requeue(requeue_interval))
}

/// Apply `spec.tls.swimKeyRef` before any reconcile-triggered SWIM send.
///
/// A configured Secret reference is mandatory for that reconcile: missing
/// Secret, missing key field, invalid key length, RBAC denial, or a stopped
/// runtime all return an error.  This prevents the configured network from
/// announcing CRD seeds or publishing certificate/provider broadcasts in
/// plaintext after the operator has observed the `GridNetwork`.
async fn apply_configured_swim_key(
    network: &GridNetwork,
    client: &Client,
    swim: &SwimHandle,
) -> Result<(), OperatorError> {
    let Some(swim_key_ref) = &network.spec.tls.swim_key_ref else {
        return Ok(());
    };
    let network_name = network.metadata.name.as_deref().unwrap_or("<unknown>");

    let key = secret::read_swim_key(client, swim_key_ref)
        .await
        .map_err(|e| OperatorError::SwimKeyConfig(format!("failed to read swimKeyRef for {network_name}: {e}")))?
        .ok_or_else(|| {
            OperatorError::SwimKeyConfig(format!(
                "swimKeyRef for {network_name} did not resolve to a valid 32-byte key \
                 (secret={}/{}, key={})",
                swim_key_ref.namespace,
                swim_key_ref.name,
                swim_key_ref.key.as_deref().unwrap_or("key")
            ))
        })?;

    swim.set_swim_key(key)
        .map_err(|e| OperatorError::SwimKeyConfig(format!("failed to apply swimKeyRef for {network_name}: {e}")))?;
    Ok(())
}

/// Return a monotonic-ish revision for public-cert metadata broadcasts.
///
/// Current UTC time as an RFC 3339 string.
///
/// Returns `None` on format failure rather than panicking.
fn rfc3339_now() -> Option<String> {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .ok()
}

/// Cert rotation updates the Kubernetes Secret, not necessarily the `GridNetwork`
/// generation.  Use wall-clock nanoseconds so a reconcile after rotation is not
/// suppressed as a duplicate metadata broadcast.
fn cert_broadcast_revision() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
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

/// Compute the difference between two seed sets.
///
/// Returns `(added, removed)` as sorted `Vec<SocketAddr>` slices.
///
/// - `added`: seeds in `desired` that are not in `previous`.
/// - `removed`: seeds in `previous` that are not in `desired`.
///
/// Both sides are sorted deterministically.  This is a pure function with no
/// I/O — suitable for unit tests and for logging seed changes between reconciles.
///
/// # Removal semantics
///
/// A seed appearing in `removed` will **not** be actively disconnected from the
/// SWIM runtime.  The SWIM protocol's own failure detection (probe → Suspect →
/// Dead) handles peers that stop responding.  Use the `removed` list only for
/// diagnostics and logging.
pub(crate) fn diff_seed_sets(previous: &[SocketAddr], desired: &[SocketAddr]) -> (Vec<SocketAddr>, Vec<SocketAddr>) {
    let prev_set: BTreeSet<SocketAddr> = previous.iter().copied().collect();
    let next_set: BTreeSet<SocketAddr> = desired.iter().copied().collect();
    let mut added: Vec<SocketAddr> = next_set.difference(&prev_set).copied().collect();
    let mut removed: Vec<SocketAddr> = prev_set.difference(&next_set).copied().collect();
    added.sort();
    removed.sort();
    (added, removed)
}

/// Announce `network.spec.seeds` to the live SWIM runtime.
///
/// Called once per reconcile.  Re-announcing to existing members is
/// idempotent — foca ignores redundant joins.
///
/// # Runtime update semantics
///
/// Seeds added to `spec.seeds` since the last reconcile are logged as
/// additions via [`diff_seed_sets`].  Seeds removed from `spec.seeds` are
/// logged as removals but are **not actively disconnected** — the SWIM
/// protocol's own failure detection handles peers that stop responding.
/// The full current seed set is always announced to ensure resilience against
/// channel-full drops from previous reconciles.
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
#[expect(
    clippy::cognitive_complexity,
    reason = "conditional logging paths for add/remove/announce; splitting would obscure the announce sequence"
)]
#[expect(
    clippy::too_many_lines,
    reason = "linear announce sequence: parse → diff → log → announce → update tracker"
)]
fn announce_crd_seeds(
    network: &GridNetwork,
    swim: &SwimHandle,
    last_seeds: &std::sync::Mutex<HashMap<String, Vec<SocketAddr>>>,
) {
    let seeds = parse_crd_seeds(&network.spec.seeds, Some(swim.local_addr()));
    let name = network.metadata.name.as_deref().unwrap_or("?");

    // Log what changed since the last reconcile using diff_seed_sets.
    // Always announce the full set for robustness (idempotent, handles channel-full retries).
    let prev = last_seeds
        .lock()
        .unwrap_or_else(|e| {
            tracing::warn!("last_seeds lock poisoned, recovering");
            e.into_inner()
        })
        .get(name)
        .cloned()
        .unwrap_or_default();
    let (added, removed) = diff_seed_sets(&prev, &seeds);
    if !added.is_empty() {
        let addrs: Vec<String> = added.iter().map(ToString::to_string).collect();
        tracing::info!(
            network = name,
            count = added.len(),
            ?addrs,
            "new CRD seeds added; announcing to SWIM runtime"
        );
    }
    if !removed.is_empty() {
        let addrs: Vec<String> = removed.iter().map(ToString::to_string).collect();
        tracing::info!(
            network = name,
            count = removed.len(),
            ?addrs,
            "CRD seeds removed from spec; no active disconnect — SWIM failure detection handles stale peers"
        );
    }

    if seeds.is_empty() {
        last_seeds
            .lock()
            .unwrap_or_else(|e| {
                tracing::warn!("last_seeds lock poisoned, recovering");
                e.into_inner()
            })
            .insert(name.to_owned(), seeds);
        return;
    }

    tracing::debug!(
        network = name,
        seeds = seeds.len(),
        "announcing CRD seeds to SWIM runtime"
    );
    if let Err(e) = swim.announce_seeds(seeds.clone()) {
        // Channel-full or closed: log and continue. Seeds will be re-queued on
        // the next reconcile cycle (REQUEUE_INTERVAL = 300 s by default).
        tracing::warn!(network = name, error = %e, "failed to queue CRD seeds for SWIM announcement; will retry on next reconcile");
        return;
    }

    // Update tracked seed set only on successful queue.
    last_seeds
        .lock()
        .unwrap_or_else(|e| {
            tracing::warn!("last_seeds lock poisoned, recovering");
            e.into_inner()
        })
        .insert(name.to_owned(), seeds);
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
#[expect(
    clippy::cognitive_complexity,
    reason = "sequential overlay render loop with per-gateway eligibility filter, consumer config, and status; splitting obscures the pipeline"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "scoring_weights is threaded through overlay rendering without hiding the selected strategy"
)]
async fn reconcile_routing_overlay_inner(
    network: &GridNetwork,
    client: &Client,
    providers: &[InferenceProvider],
    remote_crdt_providers: &[crdt::ProviderState],
    raw_metrics: &HashMap<String, scoring::BackendMetrics>,
    scoring_weights: &scoring::ScoringWeights,
    admission_states: &HashMap<String, crate::resources::geography::AdmissionState>,
) -> Result<(Vec<ConsumerConfigStatus>, Vec<OverlayRevisionStatus>), OperatorError> {
    let network_name = grid_network_name(network)?;

    let sites = list_all_grid_sites(client).await?;

    let metrics_by_str: HashMap<&str, scoring::BackendMetrics> =
        raw_metrics.iter().map(|(k, v)| (k.as_str(), *v)).collect();
    let metrics_arg = if metrics_by_str.is_empty() {
        None
    } else {
        Some(&metrics_by_str)
    };

    let observed_generation = network.metadata.generation.unwrap_or(0);
    let mut consumer_statuses: Vec<ConsumerConfigStatus> = Vec::new();
    let mut overlay_statuses: Vec<OverlayRevisionStatus> = Vec::new();

    for gw_ref in &network.spec.gateway_refs {
        // Each gateway identifies its own local site.  Fall back to the
        // network name for single-site deployments where the two are equal.
        let local_site = gw_ref.local_site_name.as_deref().unwrap_or(network_name);

        // Filter remote CRDT providers to those whose source GridSite is Active.
        // Providers from sites in any other phase (Discovered, Connecting, Pending,
        // Unreachable, Left, or missing) are excluded before the overlay is rendered.
        // This is the routing eligibility gate: SWIM membership alone does not
        // make a remote site routable.
        let eligible_remote: Vec<&crdt::ProviderState> =
            filter_eligible_remote_crdt_providers(network_name, &sites, remote_crdt_providers);
        let eligible_remote_owned: Vec<crdt::ProviderState> = eligible_remote.into_iter().cloned().collect();
        let filtered_count = remote_crdt_providers.len() - eligible_remote_owned.len();
        if filtered_count > 0 {
            tracing::debug!(
                network = network_name,
                gateway = %gw_ref.name,
                total_remote = remote_crdt_providers.len(),
                filtered = filtered_count,
                eligible = eligible_remote_owned.len(),
                "filtered remote CRDT providers: source GridSite not Active"
            );
        }

        let timestamp = rfc3339_now();
        let overlay = match routing_overlay::render_routing_overlay_with_admission(
            network,
            &sites,
            providers,
            &eligible_remote_owned,
            local_site,
            metrics_arg,
            timestamp.as_deref(),
            scoring_weights,
            Some(admission_states),
        ) {
            Ok(overlay) => overlay,
            Err(error) => {
                tracing::warn!(
                    network = network_name,
                    gateway = %gw_ref.name,
                    error = %error,
                    "routing overlay render failed; retaining any previously distributed revision"
                );
                overlay_statuses.push(retained_overlay_status(
                    network,
                    gw_ref,
                    observed_generation,
                    None,
                    "OverlayRenderFailed",
                    "overlay render failed",
                ));
                continue;
            },
        };
        let render = match render_overlay_for_gateway(&overlay, network, gw_ref) {
            Ok(r) => r,
            Err(error) => {
                tracing::warn!(
                    network = network_name,
                    gateway = %gw_ref.name,
                    error = %error,
                    "overlay envelope build failed; retaining any previously distributed revision"
                );
                overlay_statuses.push(retained_overlay_status(
                    network,
                    gw_ref,
                    observed_generation,
                    None,
                    "OverlayRenderFailed",
                    "overlay envelope build failed",
                ));
                continue;
            },
        };
        // Praxis intelligent_route rejects an empty candidates list at config load
        // time, which would cause a hot-reload error rather than a clean
        // "no routes" state.  Skip the apply and warn so the previous
        // (non-empty) ConfigMap remains in place until a provider becomes
        // available again.
        if overlay.candidates.is_empty() {
            tracing::warn!(
                network = network_name,
                gateway = %gw_ref.name,
                "routing overlay has no candidates; skipping ConfigMap apply \
                 to prevent invalid Praxis intelligent_route config"
            );
            overlay_statuses.push(retained_overlay_status(
                network,
                gw_ref,
                observed_generation,
                Some(&render),
                "EmptyCandidates",
                "no candidates available",
            ));
            continue;
        }
        let resource_version = match distribute_overlay_configmap(&overlay, &render, network_name, gw_ref, client).await
        {
            Ok(rv) => rv,
            Err(error) => {
                tracing::warn!(
                    network = network_name,
                    gateway = %gw_ref.name,
                    error = %error,
                    "routing overlay distribution failed; retaining any previously distributed revision"
                );
                overlay_statuses.push(retained_overlay_status(
                    network,
                    gw_ref,
                    observed_generation,
                    Some(&render),
                    "OverlayApplyFailed",
                    "overlay ConfigMap apply failed",
                ));
                continue;
            },
        };
        let rendered_at = stable_rendered_at(
            find_prior_overlay(network, gw_ref),
            &render.revision_hex,
            &resource_version,
            &render.rendered_at,
        );
        overlay_statuses.push(OverlayRevisionStatus {
            gateway_name: gw_ref.name.clone(),
            namespace: gw_ref.namespace.clone(),
            config_map_name: render.config_map_name,
            schema_version: render.schema_version,
            rendered_revision: render.revision_hex.clone(),
            distributed_revision: render.revision_hex.clone(),
            content_digest: render.revision_hex,
            config_map_resource_version: resource_version,
            rendered_at,
            candidate_count: render.candidate_count,
            phase: OverlayPhase::Distributed,
            reason: String::new(),
            message: String::new(),
            observed_generation,
        });

        // Opt-in: generate and apply the consumer Praxis config when enabled.
        // Render/apply errors are recorded as per-gateway status and do NOT
        // abort the reconcile loop — other gateways continue to be processed.
        // Gateways with consumerConfig.enabled=false get a Disabled entry.
        // Gateways without a consumerConfig block are omitted from status.
        if let Some(cc) = gw_ref.consumer_config.as_ref().filter(|cc| cc.enabled) {
            match apply_consumer_config_for_gateway(&overlay, network_name, gw_ref, cc, client).await {
                Ok(()) => {
                    consumer_statuses.push(consumer_config_status_rendered(gw_ref, cc, observed_generation));
                },
                Err(e) => {
                    tracing::warn!(
                        network = network_name,
                        gateway = %gw_ref.name,
                        namespace = %gw_ref.namespace,
                        error = %e,
                        "consumer Praxis config render/apply failed; recorded in status"
                    );
                    consumer_statuses.push(consumer_config_status_error(gw_ref, cc, &e, observed_generation));
                },
            }
        } else if let Some(cc) = gw_ref.consumer_config.as_ref().filter(|cc| !cc.enabled) {
            consumer_statuses.push(consumer_config_status_disabled(gw_ref, cc, observed_generation));
        }
    }
    Ok((consumer_statuses, overlay_statuses))
}

/// Find the last successfully distributed overlay status for a gateway.
fn find_prior_overlay<'net>(network: &'net GridNetwork, gw_ref: &GatewayRef) -> Option<&'net OverlayRevisionStatus> {
    network.status.as_ref().and_then(|status| {
        status
            .overlay_status
            .iter()
            .find(|e| e.gateway_name == gw_ref.name && e.namespace == gw_ref.namespace)
            .filter(|e| !e.distributed_revision.is_empty())
    })
}

/// Decide the `rendered_at` timestamp to record for a freshly-successful
/// overlay distribution.
///
/// `fresh_rendered_at` is derived from a new timestamp taken on every
/// reconcile tick (see `rfc3339_now` in `reconcile_overlays_and_consumer_configs`),
/// so using it unconditionally would make every `OverlayRevisionStatus`
/// compare as changed even when the overlay's actual content (distributed
/// revision, `ConfigMap` `resourceVersion`) is byte-for-byte identical to
/// what is already recorded — silently defeating
/// [`grid_network_status_needs_update`]'s equality check and reproducing the
/// same class of reconcile hot-loop as grid#42, just against the
/// `GridNetwork` object's own status subresource instead of `GridSite` or
/// the overlay `ConfigMap`. Reuse the prior timestamp whenever nothing
/// observable changed; only advance it when the distributed revision or
/// `ConfigMap` `resourceVersion` actually moved.
fn stable_rendered_at(
    prior: Option<&OverlayRevisionStatus>,
    revision_hex: &str,
    resource_version: &str,
    fresh_rendered_at: &str,
) -> String {
    match prior {
        Some(p) if p.distributed_revision == revision_hex && p.config_map_resource_version == resource_version => {
            p.rendered_at.clone()
        },
        _ => fresh_rendered_at.to_owned(),
    }
}

/// Resolve rendered-side evidence from render result, prior status, or defaults.
fn resolve_overlay_evidence(
    render: Option<&OverlayRenderResult>,
    prior: Option<&OverlayRevisionStatus>,
    fallback_cm_name: String,
) -> OverlayRevisionStatus {
    let r = |rf: fn(&OverlayRenderResult) -> &str, pf: fn(&OverlayRevisionStatus) -> &str| {
        render.map_or_else(
            || prior.map_or_else(String::new, |p| pf(p).to_owned()),
            |v| rf(v).to_owned(),
        )
    };
    OverlayRevisionStatus {
        gateway_name: String::new(),
        namespace: String::new(),
        config_map_name: render
            .map(|v| v.config_map_name.clone())
            .or_else(|| prior.map(|p| p.config_map_name.clone()))
            .unwrap_or(fallback_cm_name),
        schema_version: r(|v| &v.schema_version, |p| &p.schema_version),
        rendered_revision: r(|v| &v.revision_hex, |p| &p.rendered_revision),
        distributed_revision: prior.map_or_else(String::new, |p| p.distributed_revision.clone()),
        content_digest: r(|v| &v.revision_hex, |p| &p.content_digest),
        config_map_resource_version: prior.map_or_else(String::new, |p| p.config_map_resource_version.clone()),
        rendered_at: r(|v| &v.rendered_at, |p| &p.rendered_at),
        candidate_count: render.map_or_else(|| prior.map_or(0, |p| p.candidate_count), |v| v.candidate_count),
        phase: OverlayPhase::default(),
        reason: String::new(),
        message: String::new(),
        observed_generation: 0,
    }
}

/// Build status for a failed overlay update without discarding evidence of
/// the last successfully distributed revision.
///
/// When `render` is `Some`, rendered-side fields (revision, digest,
/// timestamp, count) reflect the new render; distributed-side fields are
/// taken from any prior successful distribution. When `render` is `None`
/// (render failure), all evidence is taken from the prior status.
#[expect(
    clippy::too_many_arguments,
    reason = "failure context requires render result, reason, and message alongside lookup params"
)]
fn retained_overlay_status(
    network: &GridNetwork,
    gw_ref: &GatewayRef,
    observed_generation: i64,
    render: Option<&OverlayRenderResult>,
    reason: &str,
    failure_message: &str,
) -> OverlayRevisionStatus {
    let prior = find_prior_overlay(network, gw_ref);
    let has_prior = prior.is_some();
    let fallback_cm =
        routing_overlay::overlay_configmap_name(network.metadata.name.as_deref().unwrap_or("unknown"), &gw_ref.name);
    let mut status = resolve_overlay_evidence(render, prior, fallback_cm);
    status.gateway_name.clone_from(&gw_ref.name);
    status.namespace.clone_from(&gw_ref.namespace);
    status.observed_generation = observed_generation;
    reason.clone_into(&mut status.reason);
    status.phase = if has_prior {
        OverlayPhase::Retained
    } else {
        OverlayPhase::Error
    };
    status.message = if has_prior {
        format!("{failure_message}; previous valid overlay retained")
    } else {
        format!("{failure_message}; no valid overlay has been distributed")
    };
    status
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

/// Server-side apply the operator-generated consumer Praxis config `ConfigMap`.
///
/// Only called when `gw_ref.consumer_config.enabled` is `true`.  Renders the
/// consumer Praxis YAML from the routing overlay and applies it to the gateway
/// namespace.  The generated config never contains credential token bytes.
async fn apply_consumer_config_for_gateway(
    overlay: &routing_overlay::RoutingOverlay,
    network_name: &str,
    gw_ref: &GatewayRef,
    cc: &ConsumerConfig,
    client: &Client,
) -> Result<(), OperatorError> {
    let config_yaml = consumer_config::generate_consumer_praxis_config(
        overlay,
        &cc.credential_mount_base,
        &cc.cluster_endpoints,
        &cc.tls_cert_mount_path,
        cc.listener_port,
    )?;
    let cm = consumer_config::build_consumer_config_map(
        &config_yaml,
        &cc.config_map_name,
        &gw_ref.namespace,
        network_name,
        &gw_ref.name,
    );

    let api: Api<ConfigMap> = Api::namespaced(client.clone(), &gw_ref.namespace);
    api.patch(
        &cc.config_map_name,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(&cm),
    )
    .await?;

    info!(
        config_map = %cc.config_map_name,
        namespace = %gw_ref.namespace,
        "applied consumer Praxis config ConfigMap"
    );
    Ok(())
}

/// Result of applying one routing overlay `ConfigMap` for a single gateway.
/// Result of rendering an overlay envelope before distribution.
pub(crate) struct OverlayRenderResult {
    /// `ConfigMap` name.
    pub(crate) config_map_name: String,
    /// Semantic revision hex from the envelope.
    pub(crate) revision_hex: String,
    /// Schema version from the envelope.
    pub(crate) schema_version: String,
    /// RFC 3339 timestamp when the overlay was rendered.
    pub(crate) rendered_at: String,
    /// Number of candidates in the overlay.
    pub(crate) candidate_count: u32,
    /// The built envelope, carried forward for distribution.
    pub(crate) envelope: overlay_envelope::OverlayEnvelope,
}

/// Build the overlay envelope without distributing it.
fn render_overlay_for_gateway(
    overlay: &routing_overlay::RoutingOverlay,
    network: &GridNetwork,
    gw_ref: &GatewayRef,
) -> Result<OverlayRenderResult, OperatorError> {
    let network_name = grid_network_name(network)?;
    let network_uid = network.metadata.uid.as_deref().unwrap_or("");
    let network_generation = network.metadata.generation.unwrap_or(0);
    let rendered_at = overlay.generated_at.as_deref().unwrap_or("");

    let build_result = overlay_envelope::build_overlay_envelope(
        overlay,
        &gw_ref.name,
        &gw_ref.namespace,
        network_uid,
        network_generation,
        rendered_at,
    )
    .map_err(OperatorError::Json)?;

    #[expect(
        clippy::cast_possible_truncation,
        reason = "candidate count is bounded by provider count; u32 overflow is unreachable"
    )]
    let candidate_count = overlay.candidates.len() as u32;

    Ok(OverlayRenderResult {
        config_map_name: routing_overlay::overlay_configmap_name(network_name, &gw_ref.name),
        revision_hex: build_result.revision_hex,
        schema_version: build_result.envelope.schema_version.clone(),
        rendered_at: build_result.envelope.provenance.rendered_at.clone(),
        candidate_count,
        envelope: build_result.envelope,
    })
}

/// True when the existing overlay has the expected semantic content and scope.
///
/// Provenance and timestamps are intentionally ignored: they explain when and
/// where an overlay was rendered, but do not change request routing. The
/// semantic digest and parsed payload still protect against a corrupted or
/// partially modified `ConfigMap` retaining an old revision annotation.
fn overlay_configmap_matches(existing: &ConfigMap, desired: &ConfigMap, revision: &str) -> bool {
    configmap_revision_matches(existing, desired, revision)
        && overlay_envelope_payload_matches(existing, desired, revision)
        && legacy_overlay_payload_matches(existing, desired, revision)
}

/// Check all content-addressed annotations before parsing the stored payload.
fn configmap_revision_matches(existing: &ConfigMap, desired: &ConfigMap, revision: &str) -> bool {
    let (Some(existing_annotations), Some(desired_annotations)) = (
        existing.metadata.annotations.as_ref(),
        desired.metadata.annotations.as_ref(),
    ) else {
        return false;
    };
    let annotation_matches = |key: &str| {
        existing_annotations
            .get(key)
            .zip(desired_annotations.get(key))
            .is_some_and(|(cm_existing, cm_desired)| cm_existing == cm_desired)
    };

    annotation_matches(overlay_envelope::ANNOTATION_SCHEMA_VERSION)
        && annotation_matches(overlay_envelope::ANNOTATION_REVISION)
        && annotation_matches(overlay_envelope::ANNOTATION_CONTENT_DIGEST)
        && desired_annotations
            .get(overlay_envelope::ANNOTATION_REVISION)
            .is_some_and(|value| value == revision)
        && desired_annotations
            .get(overlay_envelope::ANNOTATION_CONTENT_DIGEST)
            .is_some_and(|value| value == revision)
}

/// Parse the content-addressed envelope stored in a routing `ConfigMap`.
fn overlay_envelope_from_configmap(configmap: &ConfigMap) -> Option<overlay_envelope::OverlayEnvelope> {
    configmap
        .data
        .as_ref()
        .and_then(|data| data.get(overlay_envelope::ENVELOPE_KEY))
        .and_then(|payload| serde_json::from_str(payload).ok())
}

/// Parse the compatibility routing payload stored in a routing `ConfigMap`.
fn routing_overlay_from_configmap(configmap: &ConfigMap) -> Option<routing_overlay::RoutingOverlay> {
    configmap
        .data
        .as_ref()
        .and_then(|data| data.get("routing-config.json"))
        .and_then(|payload| serde_json::from_str(payload).ok())
}

/// Validate the semantic envelope and its gateway scope.
fn overlay_envelope_payload_matches(existing: &ConfigMap, desired: &ConfigMap, revision: &str) -> bool {
    let (Some(existing_envelope), Some(desired_envelope)) = (
        overlay_envelope_from_configmap(existing),
        overlay_envelope_from_configmap(desired),
    ) else {
        return false;
    };

    existing_envelope.schema_version == desired_envelope.schema_version
        && existing_envelope.revision.kind == desired_envelope.revision.kind
        && existing_envelope.revision.algorithm == desired_envelope.revision.algorithm
        && existing_envelope.content_digest.algorithm == desired_envelope.content_digest.algorithm
        && existing_envelope.revision.value == revision
        && existing_envelope.content_digest.value == revision
        && existing_envelope.scope.network == desired_envelope.scope.network
        && existing_envelope.scope.gateway == desired_envelope.scope.gateway
        && existing_envelope.scope.namespace == desired_envelope.scope.namespace
        && existing_envelope.scope.local_site == desired_envelope.scope.local_site
        && overlay_envelope::compute_semantic_digest(&existing_envelope.overlay)
            .ok()
            .as_deref()
            == Some(revision)
}

/// Validate the compatibility routing payload and its semantic digest.
fn legacy_overlay_payload_matches(existing: &ConfigMap, desired: &ConfigMap, revision: &str) -> bool {
    let (Some(existing_overlay), Some(desired_overlay)) = (
        routing_overlay_from_configmap(existing),
        routing_overlay_from_configmap(desired),
    ) else {
        return false;
    };

    existing_overlay.network == desired_overlay.network
        && existing_overlay.local_site == desired_overlay.local_site
        && overlay_envelope::compute_semantic_digest(&existing_overlay)
            .ok()
            .as_deref()
            == Some(revision)
}

/// Server-side apply a pre-rendered overlay `ConfigMap` for a single gateway,
/// skipping the apply when [`overlay_configmap_matches`] says it would be a
/// no-op.
///
/// Returns the Kubernetes `resourceVersion` of the (applied or pre-existing)
/// `ConfigMap`.
#[expect(
    clippy::too_many_lines,
    reason = "fetch-guard, apply, and logging is a single cohesive sequence"
)]
#[expect(
    clippy::large_stack_frames,
    reason = "async future over Kubernetes API types with serde_json values"
)]
async fn distribute_overlay_configmap(
    overlay: &routing_overlay::RoutingOverlay,
    render: &OverlayRenderResult,
    network_name: &str,
    gw_ref: &GatewayRef,
    client: &Client,
) -> Result<String, OperatorError> {
    let cm = routing_overlay::build_overlay_configmap(
        overlay,
        Some(&render.envelope),
        network_name,
        &gw_ref.name,
        &gw_ref.namespace,
    )
    .map_err(OperatorError::Json)?;

    let api: Api<ConfigMap> = Api::namespaced(client.clone(), &gw_ref.namespace);

    if let Ok(existing) = api.get(&render.config_map_name).await
        && overlay_configmap_matches(&existing, &cm, &render.revision_hex)
    {
        tracing::debug!(
            cm_name = %render.config_map_name,
            revision = %render.revision_hex,
            "routing overlay ConfigMap already at this revision; skipping no-op apply"
        );
        return Ok(existing.metadata.resource_version.unwrap_or_default());
    }

    let applied = api
        .patch(
            &render.config_map_name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&cm),
        )
        .await?;

    let resource_version = applied.metadata.resource_version.unwrap_or_default();

    info!(
        cm_name = %render.config_map_name,
        revision = %render.revision_hex,
        resource_version = %resource_version,
        "applied routing overlay ConfigMap with envelope"
    );

    Ok(resource_version)
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
    // No live phase hint. `phase_hint` returns `Some` only when at least one
    // Alive/Degraded peer exists, so we reach here when the network has no peers
    // yet — either the SWIM runtime is not up (`membership` is `None`) or it is
    // up but no peers have joined (`Some`, empty snapshot).
    let has_tls = network.spec.tls.ca_secret_ref.is_some();
    if !has_tls {
        return GridNetworkPhase::Pending;
    }
    // A single-site / combined deployment legitimately has zero SWIM peers —
    // peers are other *sites*, not intra-site gateways or pods. When no seeds
    // are configured, this network is standalone, so a running SWIM runtime
    // (`membership.is_some()`) with TLS trust material is a locally operational
    // control plane and reports `Active` instead of pinning `Initializing`
    // forever. Peer connectivity is reported separately via
    // `status.connectedSites`.
    //
    // When seeds ARE configured the network expects peers, so a peerless
    // snapshot stays `Initializing` until at least one peer is observed (handled
    // by `phase_hint` above). `membership.is_none()` means the SWIM runtime is
    // not up yet, which also stays `Initializing`.
    if membership.is_some() && network.spec.seeds.is_empty() {
        GridNetworkPhase::Active
    } else {
        GridNetworkPhase::Initializing
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

/// Convert an operator `AccessPolicy` to a CRDT `ProviderAccessPolicy`.
fn access_policy_to_crdt(access_policy: &crate::crd::auth::AccessPolicy) -> crdt::ProviderAccessPolicy {
    crdt::ProviderAccessPolicy {
        match_labels: access_policy.site_selector.match_labels.clone(),
    }
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
        access_policy: access_policy_to_crdt(&provider.spec.access_policy),
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
/// snapshot to SWIM peers via [`SwimHandle::publish_state_broadcast`].  When a
/// gateway address is configured, a broadcast is sent even if the snapshot has
/// no providers so peers can discover the data-plane address before providers
/// are created.
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

    let gateway_address = swim.gateway_address();
    if snap.providers.is_empty() && gateway_address.is_none() {
        // No providers and no gateway address — nothing to broadcast.
        return;
    }

    // Use the highest provider revision as this origin's broadcast revision.
    // Duplicate unchanged broadcasts are idempotent; newer Kubernetes writes
    // advance resourceVersion and therefore advance the broadcast revision.
    let bc = StateBroadcast::new(site_name.to_owned(), max_revision, snap, gateway_address);
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
/// `consumer_config_statuses` holds per-gateway render/apply outcomes for
/// gateways with `consumerConfig.enabled: true`; empty when no gateways opted in.
/// `budget_statuses` holds per-tenant spend status derived from
/// `spec.budgetPolicy` and merged CRDT spend state; empty when `budgetPolicy`
/// is absent.
///
/// [`Alive`]: MemberStatus::Alive
#[expect(
    clippy::too_many_arguments,
    reason = "all arguments are distinct status fields; a wrapper struct would obscure the data flow"
)]
async fn update_status(
    network: &GridNetwork,
    client: &Client,
    grid_id: &str,
    phase: &GridNetworkPhase,
    membership: Option<&MembershipSnapshot>,
    distributed_provider_count: u32,
    consumer_config_statuses: Vec<ConsumerConfigStatus>,
    overlay_statuses: Vec<OverlayRevisionStatus>,
    budget_statuses: Vec<TenantBudgetStatus>,
) -> Result<(), OperatorError> {
    let name = grid_network_name(network)?;

    let connected_sites = membership.map_or(0, MembershipSnapshot::connected_count);

    let api: Api<GridNetwork> = Api::all(client.clone());
    let status = GridNetworkStatus {
        connected_sites,
        distributed_provider_count,
        grid_id: grid_id.to_owned(),
        observed_generation: network.metadata.generation.unwrap_or(0),
        phase: phase.clone(),
        consumer_config_status: consumer_config_statuses,
        overlay_status: overlay_statuses,
        budget_status: budget_statuses,
    };

    if !grid_network_status_needs_update(network.status.as_ref(), &status) {
        return Ok(());
    }

    let patch = serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "status": status
    });

    api.patch_status(name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(patch))
        .await?;

    Ok(())
}

/// Return whether the status subresource differs from the desired status.
fn grid_network_status_needs_update(current: Option<&GridNetworkStatus>, desired: &GridNetworkStatus) -> bool {
    current != Some(desired)
}

// ---------------------------------------------------------------------------
// Consumer config status builders
// ---------------------------------------------------------------------------

/// Build a `Rendered` [`ConsumerConfigStatus`] for a successfully applied consumer config.
pub(crate) fn consumer_config_status_rendered(
    gw_ref: &GatewayRef,
    cc: &ConsumerConfig,
    observed_generation: i64,
) -> ConsumerConfigStatus {
    ConsumerConfigStatus {
        gateway_name: gw_ref.name.clone(),
        namespace: gw_ref.namespace.clone(),
        config_map_name: cc.config_map_name.clone(),
        phase: ConsumerConfigPhase::Rendered,
        reason: String::new(),
        message: format!(
            "consumer config rendered and applied to {}/{}",
            gw_ref.namespace, cc.config_map_name
        ),
        observed_generation,
    }
}

/// Build a `Disabled` [`ConsumerConfigStatus`] for a gateway whose
/// `consumerConfig.enabled` is `false`.
pub(crate) fn consumer_config_status_disabled(
    gw_ref: &GatewayRef,
    cc: &ConsumerConfig,
    observed_generation: i64,
) -> ConsumerConfigStatus {
    ConsumerConfigStatus {
        gateway_name: gw_ref.name.clone(),
        namespace: gw_ref.namespace.clone(),
        config_map_name: cc.config_map_name.clone(),
        phase: ConsumerConfigPhase::Disabled,
        reason: "ConsumerConfigDisabled".to_owned(),
        message: "consumerConfig.enabled is false; no ConfigMap generated".to_owned(),
        observed_generation,
    }
}

/// Build an `Error` [`ConsumerConfigStatus`] from a render or apply failure.
///
/// # Security
///
/// `message` is derived from the `OperatorError` `Display` impl only.  That
/// impl never includes credential token bytes — error messages describe
/// structural failures (blank fields, JSON errors, Kubernetes API errors).
pub(crate) fn consumer_config_status_error(
    gw_ref: &GatewayRef,
    cc: &ConsumerConfig,
    err: &OperatorError,
    observed_generation: i64,
) -> ConsumerConfigStatus {
    let reason = match err {
        OperatorError::ConsumerConfigRender(ConsumerConfigError::MissingClusterEndpoint { .. }) => {
            "MissingClusterEndpoint"
        },
        OperatorError::ConsumerConfigRender(ConsumerConfigError::MissingTransport { .. }) => "MissingTransport",
        OperatorError::ConsumerConfigRender(ConsumerConfigError::MissingSni { .. }) => "MissingSni",
        OperatorError::ConsumerConfigRender(ConsumerConfigError::PlaintextWithSni { .. }) => "PlaintextWithSni",
        OperatorError::ConsumerConfigRender(_) => "ConsumerConfigRenderFailed",
        OperatorError::Kube(_) => "ConsumerConfigApplyFailed",
        OperatorError::Certificate(_)
        | OperatorError::Json(_)
        | OperatorError::NotFound(_)
        | OperatorError::OverlayRender(_)
        | OperatorError::SwimKeyConfig(_)
        | OperatorError::InvalidResource(_) => "ConsumerConfigError",
    };
    ConsumerConfigStatus {
        gateway_name: gw_ref.name.clone(),
        namespace: gw_ref.namespace.clone(),
        config_map_name: cc.config_map_name.clone(),
        phase: ConsumerConfigPhase::Error,
        reason: reason.to_owned(),
        message: format!("{err}"),
        observed_generation,
    }
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
// Routing eligibility: CRDT provider → GridSite phase
// ---------------------------------------------------------------------------

/// Filter remote CRDT provider records to those whose source `GridSite` is
/// routing-eligible.
///
/// A remote provider is eligible when a `GridSite` with:
/// - resource name matching `discovered_site_k8s_name(provider.network_id, provider.site_id)`
/// - `spec.gridNetworkRef == network_name`
/// - `status.phase == Active`
///
/// exists in `sites`.
///
/// Local providers (`provider.site_id == local_site`) are excluded from this
/// function's input by the caller — they are always eligible and use a separate
/// rendering path.
///
/// Providers with no matching `GridSite`, a `GridSite` in any phase other than
/// `Active`, or a `GridSite` in a different network are excluded.  This is the
/// fail-closed contract: a SWIM-discovered site must not become routable solely
/// because it gossiped.
pub(crate) fn filter_eligible_remote_crdt_providers<'ctx>(
    network_name: &str,
    sites: &[GridSite],
    remote_providers: &'ctx [crdt::ProviderState],
) -> Vec<&'ctx crdt::ProviderState> {
    remote_providers
        .iter()
        .filter(|p| is_crdt_provider_routing_eligible(network_name, sites, p))
        .collect()
}

/// Return `true` when the `GridSite` corresponding to `provider` is routing-eligible.
///
/// Eligibility requires an `Active` `GridSite` matching the provider's network and site
/// identity.  All other outcomes — missing `GridSite`, wrong network, wrong phase —
/// are ineligible.
///
/// This is a pure function with no I/O, suitable for unit testing.
pub(crate) fn is_crdt_provider_routing_eligible(
    network_name: &str,
    sites: &[GridSite],
    provider: &crdt::ProviderState,
) -> bool {
    if provider.network_id != network_name {
        return false;
    }
    let expected_name = discovered_site_k8s_name(&provider.network_id, &provider.site_id);
    sites.iter().any(|s| {
        s.metadata.name.as_deref() == Some(expected_name.as_str())
            && s.spec.grid_network_ref == network_name
            && s.status.as_ref().is_some_and(|st| st.phase == GridSitePhase::Active)
    })
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
    /// Data-plane gateway address for egress connectivity.
    ///
    /// When the remote peer advertises a gateway address via a SWIM state broadcast,
    /// this field carries that address.  Otherwise it is empty and the `egress`
    /// section should be omitted from the `GridSite` spec to allow the `GridSite`
    /// controller to hold the site in `Discovered` until a gateway address arrives.
    pub egress_address: String,
    /// Public site certificate PEM received from this peer via SWIM broadcast.
    ///
    /// Contains only the public certificate — never a private key.
    /// `None` when the remote peer has not yet broadcast its certificate.
    pub site_cert_pem: Option<String>,
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
            egress_address: m.gateway_address.clone().unwrap_or_default(),
            site_cert_pem: m.site_cert_pem.clone(),
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

/// Whether auto-discovered remote `GridSite` egress should use plaintext.
///
/// Explicit `gatewayRefs[].consumerConfig.clusterEndpoints[].transport.mode`
/// declarations are the source of truth when present.  If any endpoint is
/// declared plaintext, auto-discovered `GridSite` egress is plaintext for this
/// network.  This supports local/dev GLB demos where provider gateways are
/// intentionally plain HTTP.
///
/// When no explicit plaintext endpoint exists, fall back to the top-level grid
/// TLS references: a network with no CA or site certificate refs is treated as
/// plaintext, while a network with either TLS ref keeps mutual TLS.
fn network_uses_plaintext_egress(network: &GridNetwork) -> bool {
    let has_plaintext_endpoint = network.spec.gateway_refs.iter().any(|gw| {
        gw.consumer_config.as_ref().is_some_and(|cc| {
            cc.cluster_endpoints.iter().any(|ep| {
                ep.transport
                    .as_ref()
                    .is_some_and(|transport| transport.mode == TransportMode::Plaintext)
            })
        })
    });

    has_plaintext_endpoint || (network.spec.tls.ca_secret_ref.is_none() && network.spec.tls.site_secret_ref.is_none())
}

/// `GridSite.status.reason` recorded for every cert-PEM validation failure.
///
/// Single source of truth for both [`decide_cert_pem_write`]'s write (via
/// [`reconcile_site_cert_pem`]) and [`already_recorded_invalid`]'s read, so
/// the two can never drift apart the way a hand-duplicated literal could.
const REASON_TRUST_MATERIAL_INVALID: &str = "TrustMaterialInvalid";

/// Diagnostic message for [`CertPemStatus::ContainsPrivateKey`].
const CERT_PEM_MSG_CONTAINS_PRIVATE_KEY: &str =
    "received trust material from remote site contained private-key markers; discarded";

/// Diagnostic message for [`CertPemStatus::NotACertificate`].
const CERT_PEM_MSG_NOT_A_CERTIFICATE: &str = "received cert PEM from remote site is not a valid certificate; check \
                                               GRID_TLS_SITE_SECRET_REF configuration on the remote operator";

/// Diagnostic message for [`CertPemStatus::TooLarge`].
const CERT_PEM_MSG_TOO_LARGE: &str = "received cert PEM from remote site exceeds the configured size bound";

/// What (if anything) [`reconcile_site_cert_pem`] should write to `GridSite`
/// status for a received site cert PEM.
///
/// Produced by the pure [`decide_cert_pem_write`] so the branching logic is
/// unit-testable without a live Kubernetes API — see grid#42, where writing
/// unconditionally on every branch turned a stable, unchanged site into an
/// infinite reconcile hot-loop.
#[derive(Debug, Eq, PartialEq)]
enum CertPemWrite {
    /// `existing_status` already reflects this outcome; nothing to do.
    NoOp,
    /// Store the structurally-valid cert PEM.
    StoreValid,
    /// Reject with `TrustMaterialInvalid`, recording this diagnostic message.
    RejectInvalid {
        /// Diagnostic message to record in `status.message`.
        message: &'static str,
        /// Selects `error!` (private-key leak) vs `warn!` (malformed or
        /// oversized) logging in the caller.
        security_violation: bool,
    },
}

/// Pure decision: given the current `GridSite` status and a freshly-checked
/// [`CertPemStatus`], decide what (if anything) to write.
///
/// Never itself touches the Kubernetes API — see [`CertPemWrite`].
fn decide_cert_pem_write(
    existing_status: Option<&GridSiteStatus>,
    cert_pem: &str,
    check: &CertPemStatus,
) -> CertPemWrite {
    match check {
        CertPemStatus::ValidStructure => {
            if existing_status.and_then(|s| s.public_cert_pem.as_deref()) == Some(cert_pem) {
                CertPemWrite::NoOp
            } else {
                CertPemWrite::StoreValid
            }
        },
        CertPemStatus::ContainsPrivateKey => {
            decide_reject_invalid(existing_status, CERT_PEM_MSG_CONTAINS_PRIVATE_KEY, true)
        },
        CertPemStatus::NotACertificate => decide_reject_invalid(existing_status, CERT_PEM_MSG_NOT_A_CERTIFICATE, false),
        CertPemStatus::TooLarge => decide_reject_invalid(existing_status, CERT_PEM_MSG_TOO_LARGE, false),
    }
}

/// Shared decision logic for the three invalid-cert-PEM outcomes: skip when
/// `existing_status` already records this exact `message`, otherwise reject.
fn decide_reject_invalid(
    existing_status: Option<&GridSiteStatus>,
    message: &'static str,
    security_violation: bool,
) -> CertPemWrite {
    if already_recorded_invalid(existing_status, message) {
        CertPemWrite::NoOp
    } else {
        CertPemWrite::RejectInvalid {
            message,
            security_violation,
        }
    }
}

/// True when `existing` already records the given invalid-cert `message` with
/// no stored `publicCertPem`, meaning a re-patch with the same content would
/// be a redundant write.
fn already_recorded_invalid(existing: Option<&GridSiteStatus>, message: &str) -> bool {
    existing.is_some_and(|s| {
        s.public_cert_pem.is_none() && s.reason == REASON_TRUST_MATERIAL_INVALID && s.message == message
    })
}

/// Validate a received site cert PEM and store (or reject) it in `GridSite`
/// status, per [`decide_cert_pem_write`]. Purely an imperative shell around
/// that pure decision: no branching logic lives here, only I/O and logging.
#[expect(
    clippy::cognitive_complexity,
    reason = "three sequential match arms, each a distinct security invariant (store/reject/security-log); splitting further would fragment cohesive I/O steps rather than reduce complexity"
)]
#[expect(
    clippy::too_many_lines,
    reason = "three JSON-patch-plus-log branches read clearer inline than split further"
)]
#[expect(
    clippy::large_stack_frames,
    reason = "async future over Kubernetes API types with serde_json values"
)]
async fn reconcile_site_cert_pem(
    api: &Api<GridSite>,
    site_name: &str,
    existing_status: Option<&GridSiteStatus>,
    cert_pem: &str,
) -> Result<(), OperatorError> {
    match decide_cert_pem_write(existing_status, cert_pem, &trust_bundle::check_cert_pem(cert_pem)) {
        CertPemWrite::NoOp => {
            tracing::debug!(name = %site_name, "cert PEM status already up to date; skipping no-op status patch");
        },
        CertPemWrite::StoreValid => {
            // Use strategic merge patch (not SSA) so only publicCertPem is
            // updated; SSA with a partial payload would clear other status
            // fields managed by "grid-operator" (e.g., reason, message).
            let cert_merge = serde_json::json!({ "status": { "publicCertPem": cert_pem } });
            api.patch_status(site_name, &PatchParams::default(), &Patch::Merge(&cert_merge))
                .await?;
            tracing::info!(
                name = %site_name,
                "received and stored public site certificate PEM (structure valid; not chain-verified)"
            );
        },
        CertPemWrite::RejectInvalid {
            message,
            security_violation,
        } => {
            // Write a status marker so operators can see the invalid material.
            // Do not store the raw PEM; record only the invalid status.
            let invalid_status_doc = serde_json::json!({
                "apiVersion": "grid.praxis-proxy.io/v1alpha1",
                "kind": "GridSite",
                "status": {
                    "publicCertPem": null,
                    "reason": REASON_TRUST_MATERIAL_INVALID,
                    "message": message
                }
            });
            api.patch_status(site_name, &PatchParams::default(), &Patch::Merge(&invalid_status_doc))
                .await?;
            if security_violation {
                tracing::error!(
                    name = %site_name,
                    "SECURITY: received cert PEM contains private key markers from remote SWIM peer; \
                     discarding — private keys must never appear in SWIM broadcasts"
                );
            } else {
                tracing::warn!(name = %site_name, %message, "rejected invalid cert PEM from remote site");
            }
        },
    }
    Ok(())
}

/// Create or update `GridSite` resources for remote Alive SWIM members.
///
/// Uses server-side apply, so the call is idempotent: applying an already-existing
/// `GridSite` with the same spec is a no-op.  After the spec is applied, the
/// `status.phase` is set to `Discovered` **only if the current phase is `Pending`**,
/// preventing this controller from regressing a site that the `GridSite` controller
/// has already advanced to `Connecting` or beyond.
///
/// Phase ownership:
/// - Pending → Discovered: this function (`GridNetwork` controller), based on SWIM Alive
/// - Discovered → Connecting: `GridSite` controller, based on data-plane gateway address presence.
/// - Connecting → Active: only an identity-verified TLS probe can promote a site. Plaintext probes report reachability
///   but remain in Connecting.
#[expect(
    clippy::too_many_lines,
    reason = "sequential spec-apply + conditional status-patch per discovered site"
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
    plaintext: bool,
) -> Result<(), OperatorError> {
    let sites = discovered_sites_from_swim(network_name, local_site, snapshot);
    if sites.is_empty() {
        return Ok(());
    }

    let api: Api<GridSite> = Api::all(client.clone());

    for site in &sites {
        // Server-side apply the spec.  Creating on first call; updating on subsequent
        // calls is a no-op when the spec has not changed.
        let mut spec_obj = serde_json::json!({
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
            }
        });
        if !site.egress_address.is_empty() {
            let tls_mode = if plaintext { "Plaintext" } else { "Mutual" };
            spec_obj.get_mut("spec").and_then(|s| {
                s.as_object_mut().map(|o| {
                    o.insert(
                        "egress".to_owned(),
                        serde_json::json!({
                            "address": site.egress_address,
                            "tls": { "mode": tls_mode }
                        }),
                    );
                })
            });
        }
        let spec_doc = spec_obj;

        api.patch(
            &site.name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&spec_doc),
        )
        .await?;

        // Fetch current status once and reuse it below for every write in this
        // iteration. Every status patch bumps the GridSite's resourceVersion,
        // which fires a watch event that re-triggers a GridNetwork reconcile
        // (related object updated) — re-entering this same loop. Writing
        // unconditionally therefore turns a stable, unchanged site into an
        // infinite reconcile hot-loop; checking against current state first
        // makes each write idempotent in practice, not just in intent (see
        // grid#42).
        let existing_status = api.get(&site.name).await.ok().and_then(|s| s.status);

        // Only write Discovered when the current phase is Pending.
        // If the GridSite controller has already advanced the phase (e.g. to
        // Connecting), we must not regress it.
        let should_write_discovered = matches!(
            existing_status.as_ref().map(|s| &s.phase),
            None | Some(GridSitePhase::Pending)
        );

        if should_write_discovered {
            let status_doc = serde_json::json!({
                "apiVersion": "grid.praxis-proxy.io/v1alpha1",
                "kind": "GridSite",
                "status": {
                    "phase": "Discovered",
                    "reason": "SWIMDiscovered",
                    "message": "site observed as Alive SWIM member"
                }
            });

            api.patch_status(
                &site.name,
                &PatchParams::apply(FIELD_MANAGER).force(),
                &Patch::Apply(&status_doc),
            )
            .await?;
        }

        // Write received public cert PEM to status after structure validation.
        // Private key material must never be written to status; invalid PEM is
        // also rejected and recorded as TrustMaterialInvalid. Skips any patch
        // that would be a no-op given `existing_status` — otherwise every
        // reconcile re-issues an unconditional write, which (per the comment
        // above `existing_status`) becomes an infinite reconcile hot-loop even
        // when the remote site's cert hasn't changed (grid#42).
        if let Some(cert_pem) = &site.site_cert_pem {
            reconcile_site_cert_pem(&api, &site.name, existing_status.as_ref(), cert_pem).await?;
        }

        tracing::info!(
            name = %site.name,
            network = %network_name,
            egress = %site.egress_address,
            cert = site.site_cert_pem.is_some(),
            "reconciled auto-discovered GridSite from SWIM Alive member"
        );
    }

    Ok(())
}

/// Compute the requeue interval for a [`GridNetwork`] reconcile.
///
/// When any [`InferenceProvider`] in the network has `metricsConfig.tls`
/// configured, returns [`TLS_REQUEUE_INTERVAL`] (60 s) so the metrics
/// collection and overlay publication in this reconcile loop detect
/// certificate rotation without a cluster-wide Secret watch.
///
/// An explicit `spec.metricsRefreshInterval` is used when valid. TLS
/// networks are capped at [`TLS_REQUEUE_INTERVAL`] so certificate rotation is
/// not delayed by an unsafe long custom interval. An absent value uses the
/// appropriate safe default; an invalid value fails reconciliation.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
fn requeue_interval_for_network(
    network: &GridNetwork,
    providers: &[InferenceProvider],
) -> Result<Duration, OperatorError> {
    let network_name = grid_network_name(network)?;
    let any_has_tls = providers
        .iter()
        .filter(|provider| provider.spec.grid_network_ref == network_name)
        .any(|provider| provider.spec.metrics_config.as_ref().is_some_and(|mc| mc.tls.is_some()));
    let default = if any_has_tls {
        TLS_REQUEUE_INTERVAL
    } else {
        REQUEUE_INTERVAL
    };
    let configured = network
        .spec
        .metrics_refresh_interval
        .as_deref()
        .map(parse_metrics_refresh_interval)
        .transpose()?;

    Ok(match (configured, any_has_tls) {
        (Some(interval), true) => interval.min(TLS_REQUEUE_INTERVAL),
        (Some(interval), false) => interval,
        (None, _) => default,
    })
}

/// Parse the deliberately small duration format accepted by
/// `metricsRefreshInterval`: seconds or milliseconds, with a one-second
/// minimum.
fn parse_metrics_refresh_interval(value: &str) -> Result<Duration, OperatorError> {
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 1_u64)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000_u64)
    } else {
        return Err(OperatorError::InvalidResource(format!(
            "spec.metricsRefreshInterval must use seconds or milliseconds, got {value:?}"
        )));
    };
    if number.is_empty() || number.starts_with('0') || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(OperatorError::InvalidResource(format!(
            "spec.metricsRefreshInterval contains an invalid duration: {value:?}"
        )));
    }
    let millis = number
        .parse::<u64>()
        .ok()
        .and_then(|number| number.checked_mul(multiplier))
        .ok_or_else(|| {
            OperatorError::InvalidResource(format!(
                "spec.metricsRefreshInterval contains an invalid duration: {value:?}"
            ))
        })?;
    if millis < 1_000 {
        return Err(OperatorError::InvalidResource(
            "spec.metricsRefreshInterval must be at least one second".to_owned(),
        ));
    }
    Ok(Duration::from_millis(millis))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crd::grid_network::{BudgetPolicyConfig, TenantBudgetConfig},
        swim::MemberRecord,
    };

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

    // -----------------------------------------------------------------------
    // reject_invalid_budget_policy
    // -----------------------------------------------------------------------

    fn network_with_budget_policy(tenants: Vec<TenantBudgetConfig>) -> GridNetwork {
        let mut network = base_network();
        network.spec.budget_policy = Some(BudgetPolicyConfig { tenants });
        network
    }

    fn tenant(tenant_id: &str, cap_usd: f64) -> TenantBudgetConfig {
        TenantBudgetConfig {
            tenant_id: tenant_id.to_owned(),
            cap_usd,
        }
    }

    #[test]
    fn reject_invalid_budget_policy_accepts_absent_policy() {
        let network = base_network();
        assert!(
            reject_invalid_budget_policy(&network).is_ok(),
            "a GridNetwork with no budgetPolicy at all must not be rejected"
        );
    }

    #[test]
    fn reject_invalid_budget_policy_accepts_valid_policy() {
        let network = network_with_budget_policy(vec![tenant("tenant-a", 100.0), tenant("tenant-b", 250.0)]);
        assert!(
            reject_invalid_budget_policy(&network).is_ok(),
            "distinct positive caps and non-empty tenant ids must be accepted"
        );
    }

    #[test]
    fn reject_invalid_budget_policy_rejects_blank_tenant_id() {
        let network = network_with_budget_policy(vec![tenant("", 100.0)]);
        let Err(error) = reject_invalid_budget_policy(&network) else {
            std::process::abort()
        };
        assert!(
            error.to_string().contains("budgetPolicy"),
            "error must identify the budgetPolicy as the invalid field, got: {error}"
        );
    }

    #[test]
    fn reject_invalid_budget_policy_rejects_duplicate_tenant_id() {
        let network = network_with_budget_policy(vec![tenant("tenant-a", 100.0), tenant("tenant-a", 200.0)]);
        let Err(error) = reject_invalid_budget_policy(&network) else {
            std::process::abort()
        };
        assert!(
            error.to_string().contains("tenant-a"),
            "error must name the offending tenant_id, got: {error}"
        );
    }

    #[test]
    fn reject_invalid_budget_policy_rejects_negative_cap() {
        let network = network_with_budget_policy(vec![tenant("tenant-a", -5.0)]);
        assert!(
            reject_invalid_budget_policy(&network).is_err(),
            "negative capUsd must be rejected even though the CRD schema minimum should already catch this before reconcile"
        );
    }

    #[test]
    fn reject_invalid_budget_policy_rejects_non_finite_cap() {
        for bad_cap in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let network = network_with_budget_policy(vec![tenant("tenant-a", bad_cap)]);
            assert!(
                reject_invalid_budget_policy(&network).is_err(),
                "non-finite capUsd ({bad_cap}) must be rejected"
            );
        }
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
                    gateway_address: None,
                    site_cert_pem: None,
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
                gateway_address: None,
                site_cert_pem: None,
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
    fn determine_phase_standalone_single_site_reaches_active() {
        // Single-site / combined deployment: no seeds, no SWIM peers. With TLS
        // trust material and the SWIM runtime up (Some, but empty membership),
        // the local control plane is operational and reports Active rather than
        // staying Initializing forever.
        let mut network = base_network();
        network.spec.tls.ca_secret_ref = Some(crate::crd::grid_network::SecretRef {
            name: "ca".to_owned(),
            namespace: "default".to_owned(),
            key: None,
        });
        network.spec.seeds.clear();
        let empty = MembershipSnapshot::default();
        let phase = determine_phase(&network, "some-id", Some(&empty));
        assert_eq!(
            phase,
            GridNetworkPhase::Active,
            "peerless single-site (no seeds) with SWIM up and TLS must reach Active"
        );
    }

    #[test]
    fn determine_phase_seeded_but_peerless_stays_initializing() {
        // With seeds configured the network expects peers; until at least one is
        // observed it must stay Initializing and not prematurely claim Active.
        let mut network = base_network();
        network.spec.tls.ca_secret_ref = Some(crate::crd::grid_network::SecretRef {
            name: "ca".to_owned(),
            namespace: "default".to_owned(),
            key: None,
        });
        network.spec.seeds = vec!["grid.peer:7946".to_owned()];
        let empty = MembershipSnapshot::default();
        let phase = determine_phase(&network, "some-id", Some(&empty));
        assert_eq!(
            phase,
            GridNetworkPhase::Initializing,
            "seeded network with no peers observed yet must stay Initializing"
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
            access_policy: crdt::ProviderAccessPolicy::default(),
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
            access_policy: crdt::ProviderAccessPolicy::default(),
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
            access_policy: crdt::ProviderAccessPolicy::default(), // Empty policy = allow all
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
                gateway_address: None,
                site_cert_pem: None,
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
                    gateway_address: None,
                    site_cert_pem: None,
                },
                MemberRecord {
                    site_id: "site-east".to_owned(),
                    endpoint: "10.0.0.1:7946".to_owned(),
                    incarnation: 0,
                    status: MemberStatus::Alive,
                    age_secs: 0,
                    gateway_address: None,
                    site_cert_pem: None,
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
    fn grid_network_status_update_is_skipped_when_semantically_unchanged() {
        let baseline = GridNetworkStatus {
            connected_sites: 2,
            distributed_provider_count: 2,
            grid_id: "grid-id".to_owned(),
            observed_generation: 3,
            phase: GridNetworkPhase::Active,
            consumer_config_status: Vec::new(),
            overlay_status: Vec::new(),
            budget_status: Vec::new(),
        };
        assert!(!grid_network_status_needs_update(Some(&baseline), &baseline));

        let changed = GridNetworkStatus {
            distributed_provider_count: 1,
            ..baseline.clone()
        };
        assert!(grid_network_status_needs_update(Some(&baseline), &changed));
        assert!(grid_network_status_needs_update(None, &baseline));
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
            gateway_address: None,
            site_cert_pem: None,
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
        assert!(
            site.egress_address.is_empty(),
            "egress_address must be empty when member has no gateway_address"
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
    fn discovered_site_uses_gateway_address_when_present() {
        let snap = make_snapshot(vec![MemberRecord {
            site_id: "remote".to_owned(),
            endpoint: "10.0.0.2:7946".to_owned(),
            incarnation: 0,
            status: MemberStatus::Alive,
            age_secs: 0,
            gateway_address: Some("10.0.0.2:19080".to_owned()),
            site_cert_pem: None,
        }]);
        let sites = discovered_sites_from_swim("net", "local", &snap);
        assert_eq!(sites.len(), 1, "exactly one remote Alive member");
        let site = sites.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(
            site.egress_address, "10.0.0.2:19080",
            "egress_address must use gateway_address when present"
        );
    }

    #[test]
    fn discovered_site_egress_empty_when_no_gateway_address() {
        let snap = make_snapshot(vec![MemberRecord {
            site_id: "remote".to_owned(),
            endpoint: "10.0.0.2:7946".to_owned(),
            incarnation: 0,
            status: MemberStatus::Alive,
            age_secs: 0,
            gateway_address: None,
            site_cert_pem: None,
        }]);
        let sites = discovered_sites_from_swim("net", "local", &snap);
        assert_eq!(sites.len(), 1, "exactly one remote Alive member");
        let site = sites.first().unwrap_or_else(|| std::process::abort());
        assert!(
            site.egress_address.is_empty(),
            "egress_address must be empty when no gateway_address is set"
        );
    }

    #[test]
    fn discovered_site_carries_site_cert_pem_when_present() {
        let sentinel_cert = "-----BEGIN CERTIFICATE-----\nMIIBIjANBgkqhkiG9\n-----END CERTIFICATE-----\n";
        let snap = make_snapshot(vec![MemberRecord {
            site_id: "remote".to_owned(),
            endpoint: "10.0.0.2:7946".to_owned(),
            incarnation: 0,
            status: MemberStatus::Alive,
            age_secs: 0,
            gateway_address: Some("10.0.0.2:8080".to_owned()),
            site_cert_pem: Some(sentinel_cert.to_owned()),
        }]);
        let sites = discovered_sites_from_swim("net", "local", &snap);
        let site = sites.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(
            site.site_cert_pem.as_deref(),
            Some(sentinel_cert),
            "site_cert_pem must propagate from MemberRecord to DiscoveredSite"
        );
    }

    #[test]
    fn discovered_site_cert_pem_none_when_not_received() {
        let snap = make_snapshot(vec![MemberRecord {
            site_id: "remote".to_owned(),
            endpoint: "10.0.0.2:7946".to_owned(),
            incarnation: 0,
            status: MemberStatus::Alive,
            age_secs: 0,
            gateway_address: None,
            site_cert_pem: None,
        }]);
        let sites = discovered_sites_from_swim("net", "local", &snap);
        let site = sites.first().unwrap_or_else(|| std::process::abort());
        assert!(
            site.site_cert_pem.is_none(),
            "site_cert_pem must be None when member has no cert"
        );
    }

    #[test]
    fn discovered_site_cert_does_not_contain_private_key_marker() {
        // Defensive: prove that whatever appears in site_cert_pem does not
        // look like a PEM private key.  This is a code-level invariant proof,
        // not an exhaustive crypto check.
        let sentinel_cert = "-----BEGIN CERTIFICATE-----\nMIIBIjANBgkqhkiG9\n-----END CERTIFICATE-----\n";
        let snap = make_snapshot(vec![MemberRecord {
            site_id: "remote".to_owned(),
            endpoint: "10.0.0.2:7946".to_owned(),
            incarnation: 0,
            status: MemberStatus::Alive,
            age_secs: 0,
            gateway_address: Some("10.0.0.2:8080".to_owned()),
            site_cert_pem: Some(sentinel_cert.to_owned()),
        }]);
        let sites = discovered_sites_from_swim("net", "local", &snap);
        let site = sites.first().unwrap_or_else(|| std::process::abort());
        if let Some(pem) = &site.site_cert_pem {
            assert!(
                !pem.contains("BEGIN RSA PRIVATE KEY") && !pem.contains("BEGIN PRIVATE KEY"),
                "site_cert_pem must never contain private key material"
            );
        }
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

    fn network_with_endpoint_transport(mode: &str, sni: Option<&str>) -> GridNetwork {
        let transport = match sni {
            Some(sni) => serde_json::json!({"mode": mode, "sni": sni}),
            None => serde_json::json!({"mode": mode}),
        };
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "glb-demo" },
            "spec": {
                "gridId": "id",
                "gatewayRefs": [{
                    "name": "consumer-gateway",
                    "namespace": "grid-system",
                    "consumerConfig": {
                        "enabled": true,
                        "clusterEndpoints": [{
                            "cluster": "sim-provider-us-west",
                            "address": "172.19.255.212:8080",
                            "transport": transport
                        }]
                    }
                }],
                "tls": {
                    "caSecretRef": {"name": "ca", "namespace": "grid-system"},
                    "siteSecretRef": {"name": "site", "namespace": "grid-system"},
                    "swimKeyRef": null
                }
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn network_uses_plaintext_egress_when_endpoint_transport_plaintext() {
        let network = network_with_endpoint_transport("plaintext", None);
        assert!(
            network_uses_plaintext_egress(&network),
            "explicit plaintext endpoint transport must drive discovered GridSite egress"
        );
    }

    #[test]
    fn network_uses_mutual_egress_when_endpoint_transport_mtls() {
        let network = network_with_endpoint_transport("mutual_tls", Some("provider.example.com"));
        assert!(
            !network_uses_plaintext_egress(&network),
            "mTLS endpoint transport plus TLS refs must keep discovered GridSite egress mutual"
        );
    }

    #[test]
    fn network_uses_plaintext_egress_when_no_tls_refs_present() {
        let network = base_network();
        assert!(
            network_uses_plaintext_egress(&network),
            "network with no CA/site TLS refs should fall back to plaintext"
        );
    }

    // -----------------------------------------------------------------------
    // already_recorded_invalid — grid#42 reconcile-hot-loop regression guard
    // -----------------------------------------------------------------------

    fn invalid_cert_status(message: &str) -> GridSiteStatus {
        GridSiteStatus {
            public_cert_pem: None,
            reason: REASON_TRUST_MATERIAL_INVALID.to_owned(),
            message: message.to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn already_recorded_invalid_true_when_reason_message_and_absence_all_match() {
        let existing = Some(invalid_cert_status("private key detected"));
        assert!(
            already_recorded_invalid(existing.as_ref(), "private key detected"),
            "identical reason/message/absent-cert must be recognized as already recorded"
        );
    }

    #[test]
    fn already_recorded_invalid_false_when_status_is_none() {
        assert!(
            !already_recorded_invalid(None, "private key detected"),
            "a GridSite with no status yet has nothing recorded"
        );
    }

    #[test]
    fn already_recorded_invalid_false_when_message_differs() {
        let existing = Some(invalid_cert_status("private key detected"));
        assert!(
            !already_recorded_invalid(existing.as_ref(), "cert exceeds size bound"),
            "a different rejection reason must not be treated as already recorded"
        );
    }

    #[test]
    fn already_recorded_invalid_false_when_reason_is_not_trust_material_invalid() {
        let existing = Some(GridSiteStatus {
            public_cert_pem: None,
            reason: "AwaitingDiscovery".to_owned(),
            message: "private key detected".to_owned(),
            ..Default::default()
        });
        assert!(
            !already_recorded_invalid(existing.as_ref(), "private key detected"),
            "a status recorded for an unrelated reason must not suppress the write"
        );
    }

    #[test]
    fn already_recorded_invalid_false_when_public_cert_pem_still_present() {
        let existing = Some(GridSiteStatus {
            public_cert_pem: Some("stale cert".to_owned()),
            reason: REASON_TRUST_MATERIAL_INVALID.to_owned(),
            message: "private key detected".to_owned(),
            ..Default::default()
        });
        assert!(
            !already_recorded_invalid(existing.as_ref(), "private key detected"),
            "a leftover publicCertPem means the invalid status was never actually applied yet"
        );
    }

    // -----------------------------------------------------------------------
    // decide_cert_pem_write — grid#42 acceptance criterion:
    //
    //   "reconciling an unchanged remote site must decide NoOp for every
    //    possible cert-PEM outcome" — i.e. a stable GridSite never causes a
    //    write, which is precisely the condition that stops the infinite
    //    reconcile hot-loop (repeated no-op writes bumping resourceVersion
    //    and re-triggering the reconciler). These tests assert that
    //    business-level property directly, across all four outcomes, rather
    //    than only exercising the internal `already_recorded_invalid` guard.
    // -----------------------------------------------------------------------

    #[test]
    fn decide_cert_pem_write_is_noop_for_every_outcome_when_site_is_already_stable() {
        // ValidStructure: publicCertPem already stored verbatim.
        let stored = Some(GridSiteStatus {
            public_cert_pem: Some("cert-a".to_owned()),
            ..Default::default()
        });
        assert_eq!(
            decide_cert_pem_write(stored.as_ref(), "cert-a", &CertPemStatus::ValidStructure),
            CertPemWrite::NoOp,
            "an unchanged valid cert must never be re-patched (grid#42)"
        );

        // Every invalid outcome: already recorded with its exact message.
        for (check, message) in [
            (CertPemStatus::ContainsPrivateKey, CERT_PEM_MSG_CONTAINS_PRIVATE_KEY),
            (CertPemStatus::NotACertificate, CERT_PEM_MSG_NOT_A_CERTIFICATE),
            (CertPemStatus::TooLarge, CERT_PEM_MSG_TOO_LARGE),
        ] {
            let recorded = Some(invalid_cert_status(message));
            assert_eq!(
                decide_cert_pem_write(recorded.as_ref(), "irrelevant-pem", &check),
                CertPemWrite::NoOp,
                "an unchanged rejection ({check:?}) must never be re-patched (grid#42)"
            );
        }
    }

    #[test]
    fn decide_cert_pem_write_stores_valid_cert_on_first_sight() {
        assert_eq!(
            decide_cert_pem_write(None, "cert-a", &CertPemStatus::ValidStructure),
            CertPemWrite::StoreValid,
            "a GridSite with no prior status must store the first valid cert seen"
        );
    }

    #[test]
    fn decide_cert_pem_write_stores_valid_cert_when_it_rotates() {
        let stale = Some(GridSiteStatus {
            public_cert_pem: Some("cert-old".to_owned()),
            ..Default::default()
        });
        assert_eq!(
            decide_cert_pem_write(stale.as_ref(), "cert-new", &CertPemStatus::ValidStructure),
            CertPemWrite::StoreValid,
            "a rotated cert (different from what's stored) must still be written"
        );
    }

    #[test]
    fn decide_cert_pem_write_rejects_private_key_as_security_violation() {
        assert_eq!(
            decide_cert_pem_write(None, "leaked-key", &CertPemStatus::ContainsPrivateKey),
            CertPemWrite::RejectInvalid {
                message: CERT_PEM_MSG_CONTAINS_PRIVATE_KEY,
                security_violation: true
            },
            "private-key leakage must be flagged as a security violation, not a routine rejection"
        );
    }

    #[test]
    fn decide_cert_pem_write_rejects_malformed_cert_as_non_security() {
        assert_eq!(
            decide_cert_pem_write(None, "garbage", &CertPemStatus::NotACertificate),
            CertPemWrite::RejectInvalid {
                message: CERT_PEM_MSG_NOT_A_CERTIFICATE,
                security_violation: false
            },
            "a malformed cert is an operator misconfiguration, not a security violation"
        );
    }

    #[test]
    fn decide_cert_pem_write_rejects_oversized_cert_as_non_security() {
        assert_eq!(
            decide_cert_pem_write(None, "huge", &CertPemStatus::TooLarge),
            CertPemWrite::RejectInvalid {
                message: CERT_PEM_MSG_TOO_LARGE,
                security_violation: false
            },
            "an oversized cert is a bound violation, not a security violation"
        );
    }

    #[test]
    fn decide_cert_pem_write_re_rejects_when_recorded_reason_no_longer_matches() {
        // Status shows a *different* rejection (or none) — must not be
        // mistaken for "already handled".
        let recorded_other_reason = Some(invalid_cert_status(CERT_PEM_MSG_TOO_LARGE));
        assert_eq!(
            decide_cert_pem_write(
                recorded_other_reason.as_ref(),
                "leaked-key",
                &CertPemStatus::ContainsPrivateKey
            ),
            CertPemWrite::RejectInvalid {
                message: CERT_PEM_MSG_CONTAINS_PRIVATE_KEY,
                security_violation: true
            },
            "a newly-observed private-key leak must be recorded even if a different rejection was previously stored"
        );
    }

    // -----------------------------------------------------------------------
    // overlay_configmap_matches — grid#42 no-op write guard
    // -----------------------------------------------------------------------

    fn overlay_configmaps_for_test() -> (ConfigMap, ConfigMap, String) {
        let overlay: routing_overlay::RoutingOverlay = serde_json::from_value(serde_json::json!({
            "network": "net",
            "local_site": "site",
            "candidates": []
        }))
        .unwrap_or_else(|_| std::process::abort());
        let built = overlay_envelope::build_overlay_envelope(&overlay, "gateway", "grid-system", "uid", 1, "now")
            .unwrap_or_else(|_| std::process::abort());
        let desired =
            routing_overlay::build_overlay_configmap(&overlay, Some(&built.envelope), "net", "gateway", "grid-system")
                .unwrap_or_else(|_| std::process::abort());
        (desired.clone(), desired, built.revision_hex)
    }

    fn mutate_envelope(configmap: &mut ConfigMap, mutate: impl FnOnce(&mut overlay_envelope::OverlayEnvelope)) {
        let payload = configmap
            .data
            .as_mut()
            .and_then(|data| data.get_mut(overlay_envelope::ENVELOPE_KEY))
            .unwrap_or_else(|| std::process::abort());
        let mut envelope = serde_json::from_str(payload).unwrap_or_else(|_| std::process::abort());
        mutate(&mut envelope);
        *payload = serde_json::to_string_pretty(&envelope).unwrap_or_else(|_| std::process::abort());
    }

    #[test]
    fn identical_overlay_configmap_is_safe_to_skip() {
        let (existing, desired, revision) = overlay_configmaps_for_test();
        assert!(overlay_configmap_matches(&existing, &desired, &revision));
    }

    #[test]
    fn provenance_only_overlay_changes_are_safe_to_skip() {
        let (mut existing, desired, revision) = overlay_configmaps_for_test();
        let envelope_payload = existing
            .data
            .as_mut()
            .and_then(|data| data.get_mut(overlay_envelope::ENVELOPE_KEY))
            .unwrap_or_else(|| std::process::abort());
        let mut envelope: overlay_envelope::OverlayEnvelope =
            serde_json::from_str(envelope_payload).unwrap_or_else(|_| std::process::abort());
        envelope.provenance.rendered_at = "later-render-time".to_owned();
        envelope.overlay.generated_at = Some("later-overlay-time".to_owned());
        *envelope_payload = serde_json::to_string_pretty(&envelope).unwrap_or_else(|_| std::process::abort());

        let legacy_payload = existing
            .data
            .as_mut()
            .and_then(|data| data.get_mut("routing-config.json"))
            .unwrap_or_else(|| std::process::abort());
        *legacy_payload = serde_json::to_string_pretty(&envelope.overlay).unwrap_or_else(|_| std::process::abort());

        assert!(overlay_configmap_matches(&existing, &desired, &revision));
    }

    #[test]
    fn corrupted_overlay_configmap_is_repaired_even_with_matching_annotation() {
        let (mut existing, desired, revision) = overlay_configmaps_for_test();
        if let Some(payload) = existing
            .data
            .as_mut()
            .and_then(|data| data.get_mut(overlay_envelope::ENVELOPE_KEY))
        {
            *payload = "not-json".to_owned();
        }
        assert!(!overlay_configmap_matches(&existing, &desired, &revision));
    }

    #[test]
    fn missing_overlay_payload_is_repaired_even_with_matching_annotation() {
        let (mut existing, desired, revision) = overlay_configmaps_for_test();
        existing.data = None;
        assert!(!overlay_configmap_matches(&existing, &desired, &revision));
    }

    #[test]
    fn annotation_payload_disagreement_is_repaired() {
        let (mut existing, desired, revision) = overlay_configmaps_for_test();
        if let Some(value) = existing
            .metadata
            .annotations
            .as_mut()
            .and_then(|annotations| annotations.get_mut(overlay_envelope::ANNOTATION_REVISION))
        {
            *value = "stale-revision".to_owned();
        }
        assert!(!overlay_configmap_matches(&existing, &desired, &revision));
    }

    #[test]
    fn contract_annotation_disagreement_is_repaired() {
        let (_, desired, revision) = overlay_configmaps_for_test();
        for key in [
            overlay_envelope::ANNOTATION_SCHEMA_VERSION,
            overlay_envelope::ANNOTATION_REVISION,
            overlay_envelope::ANNOTATION_CONTENT_DIGEST,
        ] {
            let mut existing = desired.clone();
            existing
                .metadata
                .annotations
                .as_mut()
                .unwrap_or_else(|| std::process::abort())
                .insert(key.to_owned(), "corrupted".to_owned());
            assert!(!overlay_configmap_matches(&existing, &desired, &revision));
        }
    }

    #[test]
    fn envelope_revision_contract_disagreement_is_repaired() {
        let (_, desired, revision) = overlay_configmaps_for_test();
        for field in ["schema", "kind", "revision_algorithm", "digest_algorithm"] {
            let mut existing = desired.clone();
            mutate_envelope(&mut existing, |envelope| match field {
                "schema" => envelope.schema_version = "corrupted".to_owned(),
                "kind" => envelope.revision.kind = "corrupted".to_owned(),
                "revision_algorithm" => envelope.revision.algorithm = "corrupted".to_owned(),
                "digest_algorithm" => envelope.content_digest.algorithm = "corrupted".to_owned(),
                _ => std::process::abort(),
            });
            assert!(!overlay_configmap_matches(&existing, &desired, &revision));
        }
    }

    #[test]
    fn corrupted_legacy_payload_is_repaired_even_when_envelope_is_valid() {
        let (mut existing, desired, revision) = overlay_configmaps_for_test();
        let payload = existing
            .data
            .as_mut()
            .and_then(|data| data.get_mut("routing-config.json"))
            .unwrap_or_else(|| std::process::abort());
        *payload = "not-json".to_owned();
        assert!(!overlay_configmap_matches(&existing, &desired, &revision));
    }

    #[test]
    fn semantic_payload_disagreement_is_repaired() {
        let (mut existing, desired, revision) = overlay_configmaps_for_test();
        mutate_envelope(&mut existing, |envelope| {
            envelope.overlay.local_site = "other-site".to_owned();
        });
        assert!(!overlay_configmap_matches(&existing, &desired, &revision));
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

    // -----------------------------------------------------------------------
    // diff_seed_sets
    // -----------------------------------------------------------------------

    #[test]
    fn diff_seed_sets_empty_to_empty_is_no_op() {
        let (added, removed) = diff_seed_sets(&[], &[]);
        assert!(added.is_empty(), "empty→empty must produce no additions");
        assert!(removed.is_empty(), "empty→empty must produce no removals");
    }

    #[test]
    fn diff_seed_sets_adding_a_seed() {
        let prev = vec![addr("10.0.0.1:7946")];
        let next = vec![addr("10.0.0.1:7946"), addr("10.0.0.2:7946")];
        let (added, removed) = diff_seed_sets(&prev, &next);
        assert_eq!(added, vec![addr("10.0.0.2:7946")], "new seed must appear in added");
        assert!(removed.is_empty(), "no removals when only adding");
    }

    #[test]
    fn diff_seed_sets_removing_a_seed() {
        let prev = vec![addr("10.0.0.1:7946"), addr("10.0.0.2:7946")];
        let next = vec![addr("10.0.0.1:7946")];
        let (added, removed) = diff_seed_sets(&prev, &next);
        assert!(added.is_empty(), "no additions when only removing");
        assert_eq!(
            removed,
            vec![addr("10.0.0.2:7946")],
            "removed seed must appear in removed"
        );
    }

    #[test]
    fn diff_seed_sets_unchanged_set_is_no_op() {
        let seeds = vec![addr("10.0.0.1:7946"), addr("10.0.0.2:7946")];
        let (added, removed) = diff_seed_sets(&seeds, &seeds);
        assert!(added.is_empty(), "no additions when set is unchanged");
        assert!(removed.is_empty(), "no removals when set is unchanged");
    }

    #[test]
    fn diff_seed_sets_reorder_is_no_op() {
        let prev = vec![addr("10.0.0.1:7946"), addr("10.0.0.2:7946")];
        let next = vec![addr("10.0.0.2:7946"), addr("10.0.0.1:7946")];
        let (added, removed) = diff_seed_sets(&prev, &next);
        assert!(added.is_empty(), "reordering must not produce additions");
        assert!(removed.is_empty(), "reordering must not produce removals");
    }

    #[test]
    fn diff_seed_sets_from_empty_adds_all() {
        let next = vec![addr("10.0.0.1:7946"), addr("10.0.0.2:7946")];
        let (added, removed) = diff_seed_sets(&[], &next);
        assert_eq!(added, vec![addr("10.0.0.1:7946"), addr("10.0.0.2:7946")]);
        assert!(removed.is_empty());
    }

    #[test]
    fn diff_seed_sets_to_empty_removes_all() {
        let prev = vec![addr("10.0.0.1:7946"), addr("10.0.0.2:7946")];
        let (added, removed) = diff_seed_sets(&prev, &[]);
        assert!(added.is_empty());
        assert_eq!(removed, vec![addr("10.0.0.1:7946"), addr("10.0.0.2:7946")]);
    }

    #[test]
    fn diff_seed_sets_results_are_sorted() {
        let prev = vec![addr("10.0.0.3:7946"), addr("10.0.0.1:7946")];
        let next = vec![addr("10.0.0.2:7946"), addr("10.0.0.1:7946")];
        let (added, removed) = diff_seed_sets(&prev, &next);
        // added: 10.0.0.2 only; removed: 10.0.0.3 only
        assert_eq!(added, vec![addr("10.0.0.2:7946")], "added must be sorted");
        assert_eq!(removed, vec![addr("10.0.0.3:7946")], "removed must be sorted");
    }

    #[test]
    fn diff_seed_sets_simultaneous_add_and_remove() {
        let prev = vec![addr("10.0.0.1:7946"), addr("10.0.0.2:7946")];
        let next = vec![addr("10.0.0.1:7946"), addr("10.0.0.3:7946")];
        let (added, removed) = diff_seed_sets(&prev, &next);
        assert_eq!(added, vec![addr("10.0.0.3:7946")]);
        assert_eq!(removed, vec![addr("10.0.0.2:7946")]);
    }

    // -----------------------------------------------------------------------
    // consumer config status builders
    // -----------------------------------------------------------------------

    fn make_gw_ref(name: &str, ns: &str) -> GatewayRef {
        GatewayRef {
            name: name.to_owned(),
            namespace: ns.to_owned(),
            local_site_name: None,
            consumer_config: None,
        }
    }

    fn make_consumer_config(cm_name: &str) -> ConsumerConfig {
        ConsumerConfig {
            enabled: true,
            config_map_name: cm_name.to_owned(),
            ..ConsumerConfig::default()
        }
    }

    fn rendered_overlay_status(gw: &GatewayRef) -> OverlayRevisionStatus {
        OverlayRevisionStatus {
            gateway_name: gw.name.clone(),
            namespace: gw.namespace.clone(),
            config_map_name: "grid-overlay-net-gw".to_owned(),
            schema_version: "1.0.0".to_owned(),
            rendered_revision: "a".repeat(64),
            distributed_revision: "a".repeat(64),
            content_digest: "a".repeat(64),
            config_map_resource_version: "42".to_owned(),
            rendered_at: "2026-07-29T00:00:00Z".to_owned(),
            candidate_count: 2,
            phase: OverlayPhase::Distributed,
            reason: String::new(),
            message: String::new(),
            observed_generation: 4,
        }
    }

    #[test]
    fn retained_overlay_status_preserves_last_distributed_revision() {
        let gw = make_gw_ref("gw", "grid-system");
        let prior = rendered_overlay_status(&gw);
        let mut network = base_network();
        network.status = Some(GridNetworkStatus {
            overlay_status: vec![prior.clone()],
            ..GridNetworkStatus::default()
        });

        let status = retained_overlay_status(&network, &gw, 5, None, "EmptyCandidates", "no candidates available");

        assert_eq!(status.phase, OverlayPhase::Retained);
        assert_eq!(status.rendered_revision, prior.rendered_revision);
        assert_eq!(status.distributed_revision, prior.distributed_revision);
        assert_eq!(status.content_digest, prior.content_digest);
        assert_eq!(status.config_map_resource_version, prior.config_map_resource_version);
        assert_eq!(status.candidate_count, prior.candidate_count);
        assert_eq!(status.rendered_at, prior.rendered_at);
        assert_eq!(status.reason, "EmptyCandidates");
        assert!(status.message.contains("previous valid overlay retained"));
        assert_eq!(status.observed_generation, 5);
    }

    #[test]
    fn retained_overlay_status_reports_error_without_prior_revision() {
        let gw = make_gw_ref("gw", "grid-system");
        let network = base_network();

        let status = retained_overlay_status(
            &network,
            &gw,
            1,
            None,
            "OverlayApplyFailed",
            "overlay ConfigMap apply failed",
        );

        assert_eq!(status.phase, OverlayPhase::Error);
        assert!(status.rendered_revision.is_empty());
        assert!(status.distributed_revision.is_empty());
        assert!(status.content_digest.is_empty());
        assert!(status.config_map_resource_version.is_empty());
        assert_eq!(status.reason, "OverlayApplyFailed");
        assert!(status.message.contains("no valid overlay has been distributed"));
        assert_eq!(status.observed_generation, 1);
    }

    #[test]
    fn retained_overlay_status_does_not_retain_an_error_without_revision() {
        let gw = make_gw_ref("gw", "grid-system");
        let mut network = base_network();
        network.status = Some(GridNetworkStatus {
            overlay_status: vec![OverlayRevisionStatus {
                gateway_name: gw.name.clone(),
                namespace: gw.namespace.clone(),
                config_map_name: "grid-overlay-net-gw".to_owned(),
                schema_version: String::new(),
                rendered_revision: String::new(),
                distributed_revision: String::new(),
                content_digest: String::new(),
                config_map_resource_version: String::new(),
                rendered_at: String::new(),
                candidate_count: 0,
                phase: OverlayPhase::Error,
                reason: "OverlayApplyFailed".to_owned(),
                message: "no valid overlay has been distributed".to_owned(),
                observed_generation: 1,
            }],
            ..GridNetworkStatus::default()
        });

        let status = retained_overlay_status(&network, &gw, 2, None, "EmptyCandidates", "no candidates available");

        assert_eq!(status.phase, OverlayPhase::Error);
        assert!(status.rendered_revision.is_empty());
        assert!(status.distributed_revision.is_empty());
        assert_eq!(status.reason, "EmptyCandidates");
    }

    // -----------------------------------------------------------------------
    // stable_rendered_at (grid#42: GridNetwork status resourceVersion churn)
    // -----------------------------------------------------------------------

    #[test]
    fn stable_rendered_at_reuses_prior_when_nothing_changed() {
        let gw = make_gw_ref("gw", "grid-system");
        let prior = rendered_overlay_status(&gw);

        let rendered_at = stable_rendered_at(
            Some(&prior),
            &prior.distributed_revision,
            &prior.config_map_resource_version,
            "2026-08-12T04:37:55.488097906Z",
        );

        assert_eq!(
            rendered_at, prior.rendered_at,
            "identical revision and resourceVersion must not advance rendered_at, or every \
             reconcile tick bumps GridNetwork's own resourceVersion forever (grid#42)"
        );
    }

    #[test]
    fn stable_rendered_at_advances_when_revision_changes() {
        let gw = make_gw_ref("gw", "grid-system");
        let prior = rendered_overlay_status(&gw);
        let fresh = "2026-08-12T04:37:55.488097906Z";

        let rendered_at = stable_rendered_at(Some(&prior), &"b".repeat(64), &prior.config_map_resource_version, fresh);

        assert_eq!(
            rendered_at, fresh,
            "a genuinely new distributed revision must advance rendered_at"
        );
    }

    #[test]
    fn stable_rendered_at_advances_when_configmap_resource_version_changes() {
        let gw = make_gw_ref("gw", "grid-system");
        let prior = rendered_overlay_status(&gw);
        let fresh = "2026-08-12T04:37:55.488097906Z";

        let rendered_at = stable_rendered_at(Some(&prior), &prior.distributed_revision, "43", fresh);

        assert_eq!(
            rendered_at, fresh,
            "a genuinely new ConfigMap resourceVersion must advance rendered_at"
        );
    }

    #[test]
    fn stable_rendered_at_uses_fresh_value_with_no_prior() {
        let fresh = "2026-08-12T04:37:55.488097906Z";
        let rendered_at = stable_rendered_at(None, &"a".repeat(64), "42", fresh);
        assert_eq!(
            rendered_at, fresh,
            "first-ever distribution has no prior to compare against"
        );
    }

    #[expect(
        clippy::too_many_lines,
        reason = "test helper constructing deeply nested envelope struct"
    )]
    fn make_render_result(revision: &str, candidate_count: u32) -> OverlayRenderResult {
        OverlayRenderResult {
            config_map_name: "grid-overlay-net-gw".to_owned(),
            revision_hex: revision.to_owned(),
            schema_version: "1.0.0".to_owned(),
            rendered_at: "2026-07-29T01:00:00Z".to_owned(),
            candidate_count,
            envelope: overlay_envelope::OverlayEnvelope {
                schema_version: "1.0.0".to_owned(),
                revision: overlay_envelope::ContentRevision {
                    kind: "content_addressed".to_owned(),
                    algorithm: "sha256".to_owned(),
                    value: revision.to_owned(),
                },
                content_digest: overlay_envelope::ContentDigest {
                    algorithm: "sha256".to_owned(),
                    value: revision.to_owned(),
                },
                scope: overlay_envelope::OverlayScope {
                    network: "net".to_owned(),
                    gateway: "gw".to_owned(),
                    namespace: "grid-system".to_owned(),
                    local_site: "site".to_owned(),
                },
                provenance: overlay_envelope::OverlayProvenance {
                    producer: "grid-operator".to_owned(),
                    producer_version: "0.1.0".to_owned(),
                    source_name: "net".to_owned(),
                    source_uid: "uid".to_owned(),
                    source_generation: 1,
                    rendered_at: "2026-07-29T01:00:00Z".to_owned(),
                },
                overlay: routing_overlay::RoutingOverlay {
                    network: "net".to_owned(),
                    local_site: "site".to_owned(),
                    candidates: Vec::new(),
                    generated_at: Some("2026-07-29T01:00:00Z".to_owned()),
                },
            },
        }
    }

    #[test]
    fn retained_status_rendered_b_distributed_a_after_apply_failure() {
        let gw = make_gw_ref("gw", "grid-system");
        let prior = rendered_overlay_status(&gw);
        let mut network = base_network();
        network.status = Some(GridNetworkStatus {
            overlay_status: vec![prior.clone()],
            ..GridNetworkStatus::default()
        });
        let render = make_render_result(&"b".repeat(64), 3);

        let status = retained_overlay_status(&network, &gw, 5, Some(&render), "OverlayApplyFailed", "apply failed");

        assert_eq!(
            status.rendered_revision,
            "b".repeat(64),
            "must show newly rendered revision"
        );
        assert_eq!(
            status.distributed_revision, prior.distributed_revision,
            "must retain prior distribution"
        );
        assert_eq!(status.config_map_resource_version, prior.config_map_resource_version);
        assert_eq!(status.candidate_count, 3, "must show new render candidate count");
        assert_eq!(
            status.rendered_at, "2026-07-29T01:00:00Z",
            "must show new render timestamp"
        );
        assert_eq!(status.phase, OverlayPhase::Retained);
    }

    #[test]
    fn retained_status_first_apply_failure_no_prior_distribution() {
        let gw = make_gw_ref("gw", "grid-system");
        let network = base_network();
        let render = make_render_result(&"b".repeat(64), 2);

        let status = retained_overlay_status(&network, &gw, 1, Some(&render), "OverlayApplyFailed", "apply failed");

        assert_eq!(
            status.rendered_revision,
            "b".repeat(64),
            "must show newly rendered revision"
        );
        assert!(status.distributed_revision.is_empty(), "no prior distribution exists");
        assert!(status.config_map_resource_version.is_empty());
        assert_eq!(status.phase, OverlayPhase::Error);
    }

    #[test]
    fn retained_status_empty_candidates_retains_prior_distribution() {
        let gw = make_gw_ref("gw", "grid-system");
        let prior = rendered_overlay_status(&gw);
        let mut network = base_network();
        network.status = Some(GridNetworkStatus {
            overlay_status: vec![prior.clone()],
            ..GridNetworkStatus::default()
        });
        let render = make_render_result(&"c".repeat(64), 0);

        let status = retained_overlay_status(&network, &gw, 6, Some(&render), "EmptyCandidates", "no candidates");

        assert_eq!(status.rendered_revision, "c".repeat(64));
        assert_eq!(status.distributed_revision, prior.distributed_revision);
        assert_eq!(status.candidate_count, 0);
        assert_eq!(status.phase, OverlayPhase::Retained);
    }

    #[test]
    fn retained_status_render_failure_preserves_all_prior_evidence() {
        let gw = make_gw_ref("gw", "grid-system");
        let prior = rendered_overlay_status(&gw);
        let mut network = base_network();
        network.status = Some(GridNetworkStatus {
            overlay_status: vec![prior.clone()],
            ..GridNetworkStatus::default()
        });

        let status = retained_overlay_status(&network, &gw, 7, None, "OverlayRenderFailed", "render failed");

        assert_eq!(
            status.rendered_revision, prior.rendered_revision,
            "must preserve prior rendered"
        );
        assert_eq!(
            status.distributed_revision, prior.distributed_revision,
            "must preserve prior distributed"
        );
        assert_eq!(status.content_digest, prior.content_digest);
        assert_eq!(status.rendered_at, prior.rendered_at);
        assert_eq!(status.candidate_count, prior.candidate_count);
        assert_eq!(status.phase, OverlayPhase::Retained);
    }

    #[test]
    fn distributed_status_rendered_equals_distributed() {
        let rev = "d".repeat(64);
        let status = OverlayRevisionStatus {
            gateway_name: "gw".to_owned(),
            namespace: "grid-system".to_owned(),
            config_map_name: "grid-overlay-net-gw".to_owned(),
            schema_version: "1.0.0".to_owned(),
            rendered_revision: rev.clone(),
            distributed_revision: rev.clone(),
            content_digest: rev,
            config_map_resource_version: "100".to_owned(),
            rendered_at: "2026-07-29T01:00:00Z".to_owned(),
            candidate_count: 2,
            phase: OverlayPhase::Distributed,
            reason: String::new(),
            message: String::new(),
            observed_generation: 1,
        };
        assert_eq!(
            status.rendered_revision, status.distributed_revision,
            "success path must set rendered == distributed"
        );
    }

    #[test]
    fn consumer_config_status_rendered_has_rendered_phase() {
        let gw = make_gw_ref("inference-gw", "praxis-system");
        let cc = make_consumer_config("praxis-consumer-config");
        let status = consumer_config_status_rendered(&gw, &cc, 5);
        assert_eq!(
            status.phase,
            ConsumerConfigPhase::Rendered,
            "rendered must set phase=Rendered"
        );
        assert_eq!(
            status.gateway_name, "inference-gw",
            "gateway_name must match gw_ref.name"
        );
        assert_eq!(
            status.namespace, "praxis-system",
            "namespace must match gw_ref.namespace"
        );
        assert_eq!(
            status.config_map_name, "praxis-consumer-config",
            "config_map_name must match cc"
        );
        assert_eq!(status.observed_generation, 5, "observed_generation must propagate");
        assert!(status.reason.is_empty(), "Rendered status must have empty reason");
        assert!(
            status.message.contains("praxis-consumer-config"),
            "message must name the ConfigMap"
        );
    }

    #[test]
    fn consumer_config_status_error_has_error_phase() {
        let gw = make_gw_ref("inference-gw", "praxis-system");
        let cc = make_consumer_config("praxis-consumer-config");
        let err = OperatorError::OverlayRender("structural failure".to_owned());
        let status = consumer_config_status_error(&gw, &cc, &err, 3);
        assert_eq!(status.phase, ConsumerConfigPhase::Error, "error must set phase=Error");
        assert!(!status.reason.is_empty(), "Error status must have non-empty reason");
        assert!(
            status.message.contains("structural failure"),
            "message must include error detail"
        );
        assert_eq!(status.observed_generation, 3, "observed_generation must propagate");
    }

    #[test]
    fn consumer_config_status_render_failed_reason() {
        use crate::resources::consumer_config::ConsumerConfigError;
        let gw = make_gw_ref("gw", "ns");
        let cc = make_consumer_config("cm");
        let err = OperatorError::ConsumerConfigRender(ConsumerConfigError::BlankLocalSite);
        let status = consumer_config_status_error(&gw, &cc, &err, 1);
        assert_eq!(
            status.reason, "ConsumerConfigRenderFailed",
            "render error must map to ConsumerConfigRenderFailed reason"
        );
    }

    #[test]
    fn consumer_config_status_missing_endpoint_reason() {
        let gw = make_gw_ref("gw", "ns");
        let cc = make_consumer_config("cm");
        let err = OperatorError::ConsumerConfigRender(ConsumerConfigError::MissingClusterEndpoint {
            cluster: "site-a".to_owned(),
        });
        let status = consumer_config_status_error(&gw, &cc, &err, 1);
        assert_eq!(
            status.reason, "MissingClusterEndpoint",
            "missing endpoint topology must map to a specific operator-facing reason"
        );
        assert!(
            status.message.contains("site-a"),
            "missing endpoint message must identify the cluster"
        );
    }

    #[test]
    fn consumer_config_status_missing_transport_reason() {
        let gw = make_gw_ref("gw", "ns");
        let cc = make_consumer_config("cm");
        let err = OperatorError::ConsumerConfigRender(ConsumerConfigError::MissingTransport {
            cluster: "site-b".to_owned(),
        });
        let status = consumer_config_status_error(&gw, &cc, &err, 1);
        assert_eq!(
            status.reason, "MissingTransport",
            "missing transport must map to MissingTransport reason"
        );
        assert!(
            status.message.contains("site-b"),
            "missing transport message must identify the cluster"
        );
    }

    #[test]
    fn consumer_config_status_missing_sni_reason() {
        let gw = make_gw_ref("gw", "ns");
        let cc = make_consumer_config("cm");
        let err = OperatorError::ConsumerConfigRender(ConsumerConfigError::MissingSni {
            cluster: "site-c".to_owned(),
        });
        let status = consumer_config_status_error(&gw, &cc, &err, 1);
        assert_eq!(
            status.reason, "MissingSni",
            "missing sni on mutual_tls must map to MissingSni reason"
        );
        assert!(
            status.message.contains("site-c"),
            "missing sni message must identify the cluster"
        );
    }

    #[test]
    fn consumer_config_status_plaintext_with_sni_reason() {
        let gw = make_gw_ref("gw", "ns");
        let cc = make_consumer_config("cm");
        let err = OperatorError::ConsumerConfigRender(ConsumerConfigError::PlaintextWithSni {
            cluster: "site-d".to_owned(),
        });
        let status = consumer_config_status_error(&gw, &cc, &err, 1);
        assert_eq!(
            status.reason, "PlaintextWithSni",
            "plaintext with sni must map to PlaintextWithSni reason"
        );
        assert!(
            status.message.contains("site-d"),
            "plaintext with sni message must identify the cluster"
        );
    }

    #[test]
    fn consumer_config_status_error_message_does_not_contain_sentinel_token() {
        let sentinel = "sk-super-secret-token-do-not-emit";
        let gw = make_gw_ref("gw", "ns");
        let cc = make_consumer_config("cm");
        // OverlayRender error message must not include the token (it never sees it).
        let err = OperatorError::OverlayRender("render failed: blank field".to_owned());
        let status = consumer_config_status_error(&gw, &cc, &err, 1);
        assert!(
            !status.message.contains(sentinel),
            "error status message must not contain token bytes"
        );
    }

    #[test]
    fn consumer_config_status_disabled_has_disabled_phase() {
        let gw = make_gw_ref("gw", "ns");
        let mut cc = make_consumer_config("cm");
        cc.enabled = false;
        let status = consumer_config_status_disabled(&gw, &cc, 2);
        assert_eq!(status.phase, ConsumerConfigPhase::Disabled, "must set phase=Disabled");
        assert_eq!(
            status.reason, "ConsumerConfigDisabled",
            "must have ConsumerConfigDisabled reason"
        );
        assert!(!status.message.is_empty(), "must have a non-empty diagnostic message");
        assert_eq!(status.observed_generation, 2);
    }

    #[test]
    fn consumer_config_status_disabled_message_does_not_contain_sentinel_token() {
        let sentinel = "sk-super-secret-token-must-not-appear";
        let gw = make_gw_ref("gw", "ns");
        let cc = make_consumer_config("cm");
        let status = consumer_config_status_disabled(&gw, &cc, 1);
        assert!(
            !status.message.contains(sentinel),
            "disabled message must not contain token bytes"
        );
        assert!(
            !status.reason.contains(sentinel),
            "disabled reason must not contain token bytes"
        );
    }

    #[test]
    fn consumer_config_status_serde_round_trip() {
        use crate::crd::grid_network::ConsumerConfigStatus;
        let original = ConsumerConfigStatus {
            gateway_name: "inference-gw".to_owned(),
            namespace: "praxis-system".to_owned(),
            config_map_name: "praxis-consumer-config".to_owned(),
            phase: ConsumerConfigPhase::Rendered,
            reason: String::new(),
            message: "consumer config rendered and applied".to_owned(),
            observed_generation: 42,
        };
        let json = serde_json::to_string(&original).unwrap_or_else(|_| std::process::abort());
        let round_tripped: ConsumerConfigStatus = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            original, round_tripped,
            "ConsumerConfigStatus must survive a JSON round-trip unchanged"
        );
    }

    #[test]
    fn consumer_config_status_serde_includes_all_fields_in_camel_case() {
        let status = ConsumerConfigStatus {
            gateway_name: "gw".to_owned(),
            namespace: "ns".to_owned(),
            config_map_name: "cm".to_owned(),
            phase: ConsumerConfigPhase::Error,
            reason: "ConsumerConfigRenderFailed".to_owned(),
            message: "error".to_owned(),
            observed_generation: 1,
        };
        let json = serde_json::to_string(&status).unwrap_or_else(|_| std::process::abort());
        assert!(json.contains("gatewayName"), "must serialize as camelCase gatewayName");
        assert!(
            json.contains("configMapName"),
            "must serialize as camelCase configMapName"
        );
        assert!(
            json.contains("observedGeneration"),
            "must serialize as camelCase observedGeneration"
        );
    }

    #[test]
    fn consumer_config_status_multiple_gateways_produce_separate_entries() {
        let gw_a = make_gw_ref("gw-a", "ns-a");
        let gw_b = make_gw_ref("gw-b", "ns-b");
        let cc_a = make_consumer_config("cm-a");
        let cc_b = make_consumer_config("cm-b");
        let status_a = consumer_config_status_rendered(&gw_a, &cc_a, 1);
        let status_b = consumer_config_status_rendered(&gw_b, &cc_b, 1);
        assert_eq!(status_a.gateway_name, "gw-a");
        assert_eq!(status_b.gateway_name, "gw-b");
        assert_eq!(status_a.config_map_name, "cm-a");
        assert_eq!(status_b.config_map_name, "cm-b");
        assert_eq!(status_a.phase, ConsumerConfigPhase::Rendered);
        assert_eq!(status_b.phase, ConsumerConfigPhase::Rendered);
    }

    // -----------------------------------------------------------------------
    // Routing eligibility: is_crdt_provider_routing_eligible
    // -----------------------------------------------------------------------

    fn make_eligible_crdt_provider(network_id: &str, site_id: &str) -> crdt::ProviderState {
        crdt::ProviderState {
            network_id: network_id.to_owned(),
            site_id: site_id.to_owned(),
            provider_id: "prov".to_owned(),
            routing_cluster: site_id.to_owned(),
            models: vec!["model-x".to_owned()],
            backend_kind: "local".to_owned(),
            phase: crdt::ProviderPhase::Available,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            access_policy: crdt::ProviderAccessPolicy::default(),
            revision: 1,
            writer_id: site_id.to_owned(),
        }
    }

    fn make_active_grid_site(k8s_name: &str, network_ref: &str) -> GridSite {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": k8s_name },
            "spec": { "gridNetworkRef": network_ref },
            "status": { "phase": "Active" }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn make_phase_grid_site(k8s_name: &str, network_ref: &str, phase: &str) -> GridSite {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": k8s_name },
            "spec": { "gridNetworkRef": network_ref },
            "status": { "phase": phase }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn active_grid_site_makes_crdt_provider_eligible() {
        // GridSite name = discovered_site_k8s_name("net", "site-west") = "net-site-west"
        let sites = vec![make_active_grid_site("net-site-west", "net")];
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            is_crdt_provider_routing_eligible("net", &sites, &provider),
            "Active GridSite must make CRDT provider eligible"
        );
    }

    #[test]
    fn connecting_grid_site_excludes_crdt_provider() {
        let sites = vec![make_phase_grid_site("net-site-west", "net", "Connecting")];
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            !is_crdt_provider_routing_eligible("net", &sites, &provider),
            "Connecting GridSite must NOT make CRDT provider eligible"
        );
    }

    #[test]
    fn discovered_grid_site_excludes_crdt_provider() {
        let sites = vec![make_phase_grid_site("net-site-west", "net", "Discovered")];
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            !is_crdt_provider_routing_eligible("net", &sites, &provider),
            "Discovered GridSite must NOT make CRDT provider eligible"
        );
    }

    #[test]
    fn pending_grid_site_excludes_crdt_provider() {
        let sites = vec![make_phase_grid_site("net-site-west", "net", "Pending")];
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            !is_crdt_provider_routing_eligible("net", &sites, &provider),
            "Pending GridSite must NOT make CRDT provider eligible"
        );
    }

    #[test]
    fn unreachable_grid_site_excludes_crdt_provider() {
        let sites = vec![make_phase_grid_site("net-site-west", "net", "Unreachable")];
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            !is_crdt_provider_routing_eligible("net", &sites, &provider),
            "Unreachable GridSite must NOT make CRDT provider eligible"
        );
    }

    #[test]
    fn missing_grid_site_excludes_crdt_provider() {
        let sites: Vec<GridSite> = vec![];
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            !is_crdt_provider_routing_eligible("net", &sites, &provider),
            "No matching GridSite must NOT make CRDT provider eligible (fail-closed)"
        );
    }

    #[test]
    fn wrong_network_grid_site_excludes_crdt_provider() {
        // GridSite is for a different network
        let sites = vec![make_active_grid_site("net-site-west", "other-net")];
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            !is_crdt_provider_routing_eligible("net", &sites, &provider),
            "Wrong-network GridSite must NOT make CRDT provider eligible"
        );
    }

    #[test]
    fn wrong_network_provider_excludes_crdt_provider() {
        let sites = vec![make_active_grid_site("other-net-site-west", "net")];
        let provider = make_eligible_crdt_provider("other-net", "site-west");
        assert!(
            !is_crdt_provider_routing_eligible("net", &sites, &provider),
            "Provider from another network must NOT become eligible even if a matching-name Active GridSite exists"
        );
    }

    #[test]
    fn filter_keeps_only_active_site_providers() {
        let sites = vec![
            make_active_grid_site("net-site-a", "net"),
            make_phase_grid_site("net-site-b", "net", "Connecting"),
        ];
        let providers = vec![
            make_eligible_crdt_provider("net", "site-a"), // Active → eligible
            make_eligible_crdt_provider("net", "site-b"), // Connecting → ineligible
            make_eligible_crdt_provider("net", "site-c"), // Missing → ineligible
        ];
        let eligible = filter_eligible_remote_crdt_providers("net", &sites, &providers);
        assert_eq!(eligible.len(), 1, "only Active site provider must pass filter");
        let first = eligible.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(first.site_id, "site-a");
    }

    #[test]
    fn filter_is_deterministic() {
        let sites = vec![make_active_grid_site("net-site-a", "net")];
        let providers = vec![make_eligible_crdt_provider("net", "site-a")];
        let r1 = filter_eligible_remote_crdt_providers("net", &sites, &providers);
        let r2 = filter_eligible_remote_crdt_providers("net", &sites, &providers);
        assert_eq!(r1.len(), r2.len(), "filter must be deterministic");
    }

    #[test]
    fn crdt_provider_identity_preserved_through_filter() {
        // Remote CRDT providers never carry credential data (ProviderState has no credential field).
        // The filter must not alter provider identity.
        let sites = vec![make_active_grid_site("net-site-a", "net")];
        let providers = vec![make_eligible_crdt_provider("net", "site-a")];
        let eligible = filter_eligible_remote_crdt_providers("net", &sites, &providers);
        assert_eq!(eligible.len(), 1, "eligible provider must pass filter");
        let first = eligible.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(first.site_id, "site-a", "filter must not alter provider identity");
        assert_eq!(first.network_id, "net", "filter must not alter provider network");
    }

    // -----------------------------------------------------------------------
    // requeue_interval_for_network
    // -----------------------------------------------------------------------

    fn make_provider_with_tls(name: &str, network_ref: &str) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network_ref,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": [],
                "metricsConfig": {
                    "endpoint": "https://localhost:9090/metrics",
                    "tls": {
                        "caSecretRef": { "namespace": "ns", "name": "ca" }
                    }
                }
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn make_provider_with_tls_and_health_interval(name: &str, network_ref: &str, interval: &str) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network_ref,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": [],
                "healthCheck": { "interval": interval },
                "metricsConfig": {
                    "endpoint": "https://localhost:9090/metrics",
                    "tls": {
                        "caSecretRef": { "namespace": "ns", "name": "ca" }
                    }
                }
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn network_requeue_no_tls_uses_default() {
        let providers = vec![
            make_inference_provider("p1", "net"),
            make_inference_provider("p2", "net"),
        ];
        assert_eq!(
            requeue_interval_for_network(&base_network(), &providers).ok(),
            Some(REQUEUE_INTERVAL),
            "networks with no TLS providers should use 300s"
        );
    }

    #[test]
    fn network_requeue_tls_provider_uses_tls_interval() {
        let providers = vec![make_provider_with_tls("p1", "net")];
        assert_eq!(
            requeue_interval_for_network(&base_network(), &providers).ok(),
            Some(TLS_REQUEUE_INTERVAL),
            "network with a TLS provider should use 60s"
        );
    }

    #[test]
    fn network_requeue_mixed_providers_uses_tls_interval() {
        let providers = vec![
            make_inference_provider("plain", "net"),
            make_provider_with_tls("secure", "net"),
        ];
        assert_eq!(
            requeue_interval_for_network(&base_network(), &providers).ok(),
            Some(TLS_REQUEUE_INTERVAL),
            "mixed plaintext/TLS providers should use 60s"
        );
    }

    #[test]
    fn network_requeue_longer_health_interval_does_not_delay_tls() {
        let providers = vec![make_provider_with_tls_and_health_interval("p1", "net", "600s")];
        assert_eq!(
            requeue_interval_for_network(&base_network(), &providers).ok(),
            Some(TLS_REQUEUE_INTERVAL),
            "a longer healthCheck.interval must not delay TLS rotation detection"
        );
    }

    #[test]
    fn network_requeue_uses_configured_seconds_interval() {
        let providers = vec![make_inference_provider("p1", "net")];
        let mut network = base_network();
        network.spec.metrics_refresh_interval = Some("10s".to_owned());
        assert_eq!(
            requeue_interval_for_network(&network, &providers).ok(),
            Some(Duration::from_secs(10))
        );
    }

    #[test]
    fn network_requeue_uses_configured_milliseconds_interval() {
        let providers = vec![make_inference_provider("p1", "net")];
        let mut network = base_network();
        network.spec.metrics_refresh_interval = Some("1500ms".to_owned());
        assert_eq!(
            requeue_interval_for_network(&network, &providers).ok(),
            Some(Duration::from_millis(1_500))
        );
    }

    #[test]
    fn network_requeue_invalid_config_fails() {
        let providers = vec![make_inference_provider("p1", "net")];
        let mut network = base_network();
        network.spec.metrics_refresh_interval = Some("5m".to_owned());
        assert!(
            requeue_interval_for_network(&network, &providers).is_err(),
            "unsupported duration unit '5m' must be rejected"
        );
    }

    #[test]
    fn network_requeue_tls_caps_long_configured_interval() {
        let providers = vec![make_provider_with_tls("p1", "net")];
        let mut network = base_network();
        network.spec.metrics_refresh_interval = Some("600s".to_owned());
        assert_eq!(
            requeue_interval_for_network(&network, &providers).ok(),
            Some(TLS_REQUEUE_INTERVAL)
        );
    }

    #[test]
    fn network_requeue_tls_allows_short_configured_interval() {
        let providers = vec![make_provider_with_tls("p1", "net")];
        let mut network = base_network();
        network.spec.metrics_refresh_interval = Some("10s".to_owned());
        assert_eq!(
            requeue_interval_for_network(&network, &providers).ok(),
            Some(Duration::from_secs(10))
        );
    }

    #[test]
    fn network_requeue_ignores_tls_providers_from_other_networks() {
        let providers = vec![
            make_inference_provider("local", "net"),
            make_provider_with_tls("unrelated-secure", "other-net"),
        ];
        let mut network = base_network();
        network.spec.metrics_refresh_interval = Some("120s".to_owned());
        assert_eq!(
            requeue_interval_for_network(&network, &providers).ok(),
            Some(Duration::from_secs(120)),
            "TLS providers from another network must not cap this network's interval"
        );
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "exhaustive rejection table")]
    fn metrics_refresh_duration_parser_rejects_unsupported_values() {
        assert!(
            parse_metrics_refresh_interval("").is_err(),
            "empty string must be rejected"
        );
        assert!(
            parse_metrics_refresh_interval("10").is_err(),
            "bare number without unit must be rejected"
        );
        assert!(
            parse_metrics_refresh_interval("0s").is_err(),
            "zero-second interval must be rejected"
        );
        assert!(
            parse_metrics_refresh_interval("500ms").is_err(),
            "sub-second interval must be rejected"
        );
        assert!(
            parse_metrics_refresh_interval("-1s").is_err(),
            "negative interval must be rejected"
        );
        assert!(
            parse_metrics_refresh_interval("1m").is_err(),
            "minute unit must be rejected"
        );
        assert!(
            parse_metrics_refresh_interval("01s").is_err(),
            "leading-zero numeric must be rejected"
        );
        assert!(
            parse_metrics_refresh_interval(" 10s").is_err(),
            "leading whitespace must be rejected"
        );
        assert!(
            parse_metrics_refresh_interval("18446744073709551615s").is_err(),
            "overflow value must be rejected"
        );
    }
}
