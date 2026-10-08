//! [`GridNetwork`] controller.
//!
//! Reconciles [`GridNetwork`] resources: generates the grid CA
//! and site certificate, manages TLS secrets, generates the
//! grid ID, signals the SWIM runtime to start, and renders
//! routing overlay ConfigMaps for each gateway reference.
//!
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    net::{Ipv6Addr, SocketAddr, SocketAddrV6},
    path::Path,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{ConfigMap, Secret},
};
use kube::{
    Client,
    api::{Api, DeleteParams, ListParams, Patch, PatchParams, Preconditions},
    runtime::{controller::Action, reflector::ObjectRef},
};
use serde_json::{Value, json};
use tokio::{
    sync::Mutex,
    time::{Duration, timeout},
};
use tracing::info;

use crate::{
    crd::{
        agent_tool_provider::AgentToolProvider,
        grid_network::{
            ConsumerConfig, ConsumerConfigPhase, ConsumerConfigStatus, GatewayRef, GridNetwork, GridNetworkPhase,
            GridNetworkStatus, MountReconciliation, MountReconciliationPhase, MountReconciliationStatus, OverlayPhase,
            OverlayRevisionStatus, PeerTrustMode, SignalMode, SiteIdentityStatus, TenantBudgetStatus, TransportMode,
        },
        grid_site::{GridSite, GridSitePhase, GridSiteStatus},
        inference_provider::{InferenceProvider, InferenceProviderStatus},
    },
    error::OperatorError,
    readiness,
    resources::{
        consumer_config::{self, ConsumerConfigError},
        gateway_mounts, overlay_envelope, provider_admission, provider_metrics, routing_overlay, secret,
        serving_config::{self, ServingInputs, WriteDecision, WriteGate},
        tls_backend::ServerTlsConfig,
        trust_bundle::{self, CertPemStatus},
    },
    served_models, signals,
    swim::{MemberStatus, MembershipSnapshot},
    swim_endpoint::{SeedResolution, resolve_endpoint_list_partial},
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
    reason = "client is public API; the SWIM handle and caches are reached through methods"
)]
pub struct OperatorCtx {
    /// Kubernetes API client.
    pub client: Client,

    /// The live SWIM runtime once started, empty without a SWIM bind address.
    swim: std::sync::OnceLock<Arc<SwimHandle>>,

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
    /// Uses [`std::sync::Mutex`] because seed tracking is updated synchronously
    /// after async DNS resolution and the SWIM channel announcement.
    pub(crate) last_seeds: std::sync::Mutex<HashMap<String, Vec<SocketAddr>>>,

    /// Sites each `GridNetwork`'s serving config last refused, for warning on change.
    pub(crate) refused_sites: serving_config::RefusedSites,

    /// Who may read the signals endpoint, keyed by presented-cert fingerprint.
    /// Set by reconcile from each `GridSite`'s trust pins.
    pub(crate) peer_identities: signals::PeerIdentities,

    /// Signals polled from peers, keyed by site name. Kept apart from local so a
    /// scoped read returns only what this site observed, never a second-hand copy.
    pub(crate) peers: signals::SignalStore,

    /// This site's scraped signals, served to gateways and peers.
    pub(crate) signals: signals::SignalStore,

    /// Served-model sets discovered from this site's providers.
    pub(crate) served_models: served_models::ServedModelStore,

    /// Each provider's latest scrape, from which its readiness is resolved.
    pub(crate) readiness: readiness::ReadinessStore,

    /// How often the signals loop scrapes providers; a readiness window is at least two.
    scrape_interval: Duration,

    /// Signal transport resolved once at startup: gossip or poll.
    ///
    /// Under poll the operator stops carrying metrics in gossip and scoring the
    /// overlay, and the gateway ranks from the signal it pulls instead.
    pub(crate) signal_mode: SignalMode,

    /// Last serving config write per `ConfigMap`, to coalesce membership churn.
    pub(crate) serving_writes: WriteGate,

    /// Peer addressing and trust, resolved once at startup.
    pub(crate) peer_settings: PeerSettings,

    /// Whether membership-derived writes may run, cleared while SWIM converges.
    membership_ready: std::sync::atomic::AtomicBool,

    /// Where the declared peer trust goes, so site identity rotation follows it.
    declared_trust: Option<tokio::sync::watch::Sender<PeerTrustMode>>,

    /// Whether this process runs the site identity rotation loop.
    rotation: bool,

    /// This site's name, which a certificate the operator issues itself carries.
    site_name: Option<String>,

    /// The last value logged per state, so a pass logs at INFO only what changed.
    pub(crate) logged: ChangeLog,

    /// When this operator started, so discovery never judges a site absent before gossip could vouch for it.
    started: Instant,
}

/// The last value logged per key, so INFO fires on a change rather than on every pass.
#[derive(Debug, Default)]
pub(crate) struct ChangeLog(std::sync::Mutex<HashMap<String, String>>);

impl ChangeLog {
    /// Whether `value` differs from the last one recorded for `key`, recording it.
    pub(crate) fn changed(&self, key: &str, value: String) -> bool {
        let mut held = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if held.get(key) == Some(&value) {
            return false;
        }
        held.insert(key.to_owned(), value);
        true
    }

    /// Forget every key under `prefix` not in `keep`, so departed objects do not grow the log.
    pub(crate) fn retain_under(&self, prefix: &str, keep: &std::collections::HashSet<String>) {
        let mut held = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        held.retain(|key, _| !key.starts_with(prefix) || keep.contains(key));
    }
}

/// Send `declared` to `sender`, returning whether it changed, so a repeat wakes nobody.
fn send_declared_trust(sender: &tokio::sync::watch::Sender<PeerTrustMode>, declared: PeerTrustMode) -> bool {
    sender.send_if_modified(|trust| {
        let changed = *trust != declared;
        *trust = declared;
        changed
    })
}

/// Grid-wide modes, fixed for the life of the process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GridModes {
    /// Signal propagation.
    pub signal: SignalMode,
    /// Peer authorization on the signals path.
    pub trust: PeerTrustMode,
}

impl GridModes {
    /// Without a `GridNetwork`: SPIFFE trust in the Grid CA identity, never an implicit pin.
    pub const WITHOUT_NETWORK: Self = Self {
        signal: SignalMode::Gossip,
        trust: PeerTrustMode::Spiffe,
    };

    /// The modes to start in with no `GridNetwork` yet: the install's declared modes,
    /// else [`Self::WITHOUT_NETWORK`]. Matching the network the install will create
    /// means a fresh install never restarts once it appears.
    #[must_use]
    pub fn without_network(signal: Option<SignalMode>, trust: Option<PeerTrustMode>) -> Self {
        Self {
            signal: signal.unwrap_or(Self::WITHOUT_NETWORK.signal),
            trust: trust.unwrap_or(Self::WITHOUT_NETWORK.trust),
        }
    }

    /// The modes `network` declares, defaults for absent fields.
    #[must_use]
    pub fn of(network: &GridNetwork) -> Self {
        Self {
            signal: network
                .spec
                .signal_transport
                .as_ref()
                .map(|t| t.mode)
                .unwrap_or_default(),
            trust: network.spec.peer_trust.as_ref().map(|t| t.mode).unwrap_or_default(),
        }
    }

    /// Whether the site identity renews: a pinned peer would refuse the renewed leaf.
    #[must_use]
    pub const fn renews(self) -> bool {
        matches!(self.trust, PeerTrustMode::Spiffe)
    }

    /// The modes to restart into when `network` declares other than `self`, the running modes.
    #[must_use]
    pub fn restart_for(self, network: Option<&GridNetwork>) -> Option<Self> {
        // Only the poll path reads trust, so a trust change under gossip needs no restart.
        network.map(Self::of).filter(|declared| {
            declared.signal != self.signal || (declared.signal == SignalMode::Poll && declared.trust != self.trust)
        })
    }
}

/// Peer addressing and trust, resolved once at startup.
#[derive(Clone, Debug)]
pub struct PeerSettings {
    /// This site's own signals endpoint, for its gateway.
    pub local_signals_addr: Option<String>,
    /// How peers prove their identity.
    pub trust: PeerTrustMode,
    /// Port dialed for a peer that gossips no signals endpoint.
    pub peer_port: u16,
}

impl Default for PeerSettings {
    fn default() -> Self {
        Self {
            local_signals_addr: None,
            trust: PeerTrustMode::default(),
            peer_port: signals::DEFAULT_PEER_PORT,
        }
    }
}

/// Requeue for a reconcile held while SWIM membership converges.
const MEMBERSHIP_HOLD_REQUEUE: Duration = Duration::from_secs(5);

/// Requeue after a failed serving config apply.
const SERVING_RETRY_REQUEUE: Duration = Duration::from_secs(30);

impl OperatorCtx {
    /// Create a new [`OperatorCtx`] with an empty metrics cache.
    ///
    /// This is the canonical constructor used by the operator binary so that
    /// the internal metrics cache type does not need to be exported from the
    /// library crate.
    pub fn new(client: Client, swim: Option<Arc<SwimHandle>>, signal_mode: SignalMode) -> Self {
        Self {
            client,
            swim: swim.map(std::sync::OnceLock::from).unwrap_or_default(),
            metrics_cache: Mutex::new(provider_metrics::MetricsCache::new()),
            admission_memory: Mutex::new(provider_admission::AdmissionMemory::default()),
            last_seeds: std::sync::Mutex::new(HashMap::new()),
            refused_sites: std::sync::Mutex::new(HashMap::new()),
            peer_identities: signals::PeerIdentities::new(),
            peers: signals::SignalStore::new(),
            signals: signals::SignalStore::new(),
            served_models: served_models::ServedModelStore::new(),
            readiness: readiness::ReadinessStore::default(),
            scrape_interval: Duration::ZERO,
            signal_mode,
            serving_writes: WriteGate::default(),
            peer_settings: PeerSettings::default(),
            membership_ready: std::sync::atomic::AtomicBool::new(true),
            declared_trust: None,
            rotation: false,
            site_name: None,
            logged: ChangeLog::default(),
            started: Instant::now(),
        }
    }

    /// Report a rotation schedule only when this process runs the rotation loop.
    #[must_use]
    pub const fn with_rotation(mut self, rotation: bool) -> Self {
        self.rotation = rotation;
        self
    }

    /// Send the declared peer trust here whenever a `GridNetwork` changes it.
    #[must_use]
    pub fn with_declared_trust(mut self, declared_trust: tokio::sync::watch::Sender<PeerTrustMode>) -> Self {
        self.declared_trust = Some(declared_trust);
        self
    }

    /// Publish the trust `network` declares, waking its watchers only on a change.
    fn publish_declared_trust(&self, network: &GridNetwork) {
        if let Some(sender) = &self.declared_trust {
            send_declared_trust(sender, GridModes::of(network).trust);
        }
    }

    /// Hold membership-derived writes until [`release_membership`](Self::release_membership).
    #[must_use]
    pub fn hold_membership(self) -> Self {
        self.membership_ready.store(false, std::sync::atomic::Ordering::Release);
        self
    }

    /// Let membership-derived writes run once SWIM has settled and converged.
    pub fn release_membership(&self) {
        self.membership_ready.store(true, std::sync::atomic::Ordering::Release);
    }

    /// The requeue for a reconcile held while membership converges, `None` once released.
    #[must_use]
    pub fn membership_hold(&self) -> Option<Action> {
        (!self.membership_ready.load(std::sync::atomic::Ordering::Acquire))
            .then(|| Action::requeue(MEMBERSHIP_HOLD_REQUEUE))
    }

    /// The SWIM runtime, `None` until it starts or without one.
    #[must_use]
    pub fn swim(&self) -> Option<&Arc<SwimHandle>> {
        self.swim.get()
    }

    /// Install the SWIM runtime once it starts, `false` if one already was.
    pub fn set_swim(&self, handle: Arc<SwimHandle>) -> bool {
        self.swim.set(handle).is_ok()
    }

    /// Peer settings resolved at startup.
    #[must_use]
    pub fn peer_settings(&self) -> &PeerSettings {
        &self.peer_settings
    }

    /// Replace the peer settings resolved at startup.
    #[must_use]
    pub fn with_peer_settings(mut self, settings: PeerSettings) -> Self {
        self.peer_settings = settings;
        self
    }

    /// The configured site name, when the install sets a valid one.
    #[must_use]
    pub fn site_name(&self) -> Option<&str> {
        self.site_name.as_deref()
    }

    /// Record how often the signals loop scrapes providers.
    #[must_use]
    pub const fn with_scrape_interval(mut self, interval: Duration) -> Self {
        self.scrape_interval = interval;
        self
    }

    /// Name the certificate the operator issues itself after this site, not the network.
    #[must_use]
    pub fn with_site_name(mut self, site_name: Option<String>) -> Self {
        self.site_name = site_name.filter(|name| match certs::validate_site_name(name) {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(site = %name, %error, "site name is not a valid label; certificates name the network");
                false
            },
        });
        self
    }

    /// A handle to what this site publishes, for the signals listener.
    #[must_use]
    pub fn signals(&self) -> signals::SignalStore {
        self.signals.clone()
    }

    /// A handle to what peers publish, for the signals listener and poller.
    #[must_use]
    pub fn peers(&self) -> signals::SignalStore {
        self.peers.clone()
    }

    /// A handle to who may read, for the signals listener and poller.
    #[must_use]
    pub fn peer_identities(&self) -> signals::PeerIdentities {
        self.peer_identities.clone()
    }
}

// ---------------------------------------------------------------------------
// Signals (poll mode)
// ---------------------------------------------------------------------------

/// How long a published site signal is served before it expires.
///
/// Several scrape intervals, so a couple of missed scrapes do not erase what is
/// known; absence, not a stale flag, is what tells a reader a writer stopped.
fn site_signals_ttl() -> Duration {
    let secs = std::env::var("GRID_SIGNALS_TTL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(30);
    Duration::from_secs(secs.max(1))
}

/// Scrape this site's providers once and publish the coarse signals observed.
///
/// Called from its own loop rather than from reconcile. Reconcile runs on an
/// interval sized for declarations, two orders of magnitude slower than these
/// values move; driving publication from it would leave a reader re-reading one
/// observation for minutes.
///
/// # Errors
///
/// Returns [`OperatorError`] when providers or sites cannot be listed.
pub async fn refresh_signals(ctx: &OperatorCtx, client: &Client, network_name: &str) -> Result<(), OperatorError> {
    let providers = list_all_inference_providers(client).await?;
    ctx.signals.set_access(signal_access(&providers));
    Box::pin(register_peers(ctx, client)).await?;
    Box::pin(provider_metrics::collect_provider_signals(
        network_name,
        &providers,
        Some(client),
        &ctx.readiness,
    ))
    .await;
    let collected = resolve_readiness(ctx, client, &providers, network_name);
    publish_signals(ctx, collected);
    Ok(())
}

/// Resolve each provider's readiness, write its `Ready` condition, and return what to publish.
fn resolve_readiness(
    ctx: &OperatorCtx,
    client: &Client,
    providers: &[InferenceProvider],
    network_name: &str,
) -> HashMap<String, Vec<signals::Observation>> {
    ctx.readiness.retain(
        &providers
            .iter()
            .filter(|p| provider_metrics::signal_scrape_plan(p).is_some())
            .filter_map(|p| {
                p.metadata
                    .name
                    .as_deref()
                    .map(|name| readiness::key(&p.spec.grid_network_ref, name))
            })
            .collect(),
    );
    let now = Instant::now();
    let mut collected: HashMap<String, Vec<signals::Observation>> = HashMap::new();
    let mut writes = Vec::new();
    for provider in providers.iter().filter(|p| p.spec.grid_network_ref == network_name) {
        let Some(entry) = readiness_entry(ctx, provider, now) else {
            continue;
        };
        writes.push((provider.clone(), entry.verdict));
        if let Some(published) = entry.published {
            publish_strictest(&mut collected, entry.identity, published);
        }
    }
    write_ready_conditions(client.clone(), writes);
    collected
}

/// Keep one entry per routing identity: the stricter verdict wins, so one provider
/// reporting no endpoints is not hidden by another's ready behind the same cluster.
fn publish_strictest(
    collected: &mut HashMap<String, Vec<signals::Observation>>,
    identity: &str,
    published: Vec<signals::Observation>,
) {
    match collected.entry(identity.to_owned()) {
        std::collections::hash_map::Entry::Vacant(slot) => {
            slot.insert(published);
        },
        std::collections::hash_map::Entry::Occupied(mut slot) => {
            if excludes(&published) && !excludes(slot.get()) {
                slot.insert(published);
            }
        },
    }
}

/// Whether a published entry carries a not-ready verdict.
fn excludes(published: &[signals::Observation]) -> bool {
    published
        .iter()
        .any(|o| o.metric == readiness::READY_SIGNAL && o.value < 1.0)
}

/// How long one `Ready` condition write may take before it is abandoned.
const READY_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// One condition-write pass at a time.
///
/// Each pass can take a deadline per provider, so a stalled apiserver would otherwise let a
/// pass per scrape interval pile up, each holding a clone of every provider it writes. Worse,
/// overlapping passes have no order: an older one could apply its stale verdict after a newer
/// one. Skipping a pass costs nothing, because the next derives every verdict again.
static WRITE_SLOT: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

/// Write each provider's `Ready` condition off the publication path, each under a deadline,
/// so a stalled status PATCH (the default client has no read timeout) cannot hold every
/// provider's signals from publishing and let them expire.
fn write_ready_conditions(client: Client, writes: Vec<(InferenceProvider, readiness::Verdict)>) {
    if writes.is_empty() {
        return;
    }
    let Ok(slot) = WRITE_SLOT.try_acquire() else {
        tracing::debug!("a Ready condition pass is still running; this one is skipped");
        return;
    };
    tokio::spawn(async move {
        // Moved in, so the slot frees when the pass ends however it ends.
        let _slot = slot;
        for (provider, verdict) in writes {
            let name = provider.metadata.name.as_deref().unwrap_or("?");
            match timeout(
                READY_WRITE_TIMEOUT,
                Box::pin(apply_ready_condition(&client, &provider, &verdict)),
            )
            .await
            {
                Ok(Ok(())) => {},
                Ok(Err(error)) => tracing::warn!(provider = name, %error, "provider Ready condition not written"),
                Err(_elapsed) => tracing::warn!(provider = name, "provider Ready condition write timed out"),
            }
        }
    });
}

/// One provider's readiness and the signals to publish for it.
struct ReadinessEntry<'provider> {
    /// Routing identity.
    identity: &'provider str,
    /// The verdict.
    verdict: readiness::Verdict,
    /// Signals to publish, `None` for a provider without metrics: absent reads as ready.
    published: Option<Vec<signals::Observation>>,
}

/// A provider's readiness entry, `None` for no identity or not yet judged.
///
/// A provider not scraped for readiness, with no metrics config or no usable
/// endpoint, is `Unknown` and publishes nothing: absent reads as ready.
fn readiness_entry<'provider>(
    ctx: &OperatorCtx,
    provider: &'provider InferenceProvider,
    now: Instant,
) -> Option<ReadinessEntry<'provider>> {
    let identity = routing_overlay::routing_identity(provider)?;
    let key = readiness::key(&provider.spec.grid_network_ref, provider.metadata.name.as_deref()?);
    if provider_metrics::signal_scrape_plan(provider).is_none() {
        return Some(ReadinessEntry {
            identity,
            verdict: readiness::Verdict {
                reason: readiness::Reason::MetricsNotConfigured,
                message: "no metricsConfig endpoint to read readiness from".to_owned(),
            },
            published: None,
        });
    }
    let verdict = provider_readiness(ctx, provider, now)?;
    let ready = ready_sample(&verdict);
    let endpoints = ctx.readiness.ready_endpoints(&key).map(|count| signals::Observation {
        metric: readiness::READY_ENDPOINTS_SIGNAL.to_owned(),
        labels: BTreeMap::new(),
        value: count,
        timestamp_ms: None,
    });
    // A fresh scrape republishes with the verdict. Without one, the last published
    // entry ages on its own, unless the verdict turned not ready.
    let fresh = ctx.readiness.take_fresh(&key);
    Some(ReadinessEntry {
        identity,
        published: published_signals(fresh, ready, endpoints, verdict.reason.excludes()),
        verdict,
    })
}

/// What to publish for a scraped provider: a fresh scrape with the verdict, or the
/// verdict alone when it is not ready. Without either, the last entry ages on its own.
fn published_signals(
    fresh: Option<Vec<signals::Observation>>,
    ready: signals::Observation,
    endpoints: Option<signals::Observation>,
    excluded: bool,
) -> Option<Vec<signals::Observation>> {
    match fresh {
        Some(mut held) => {
            held.push(ready);
            held.extend(endpoints);
            Some(held)
        },
        None if excluded => Some(std::iter::once(ready).chain(endpoints).collect()),
        None => None,
    }
}


/// The `grid_provider_ready` sample for `verdict`: 1 when it serves, 0 when not.
fn ready_sample(verdict: &readiness::Verdict) -> signals::Observation {
    signals::Observation {
        metric: readiness::READY_SIGNAL.to_owned(),
        labels: BTreeMap::new(),
        value: if verdict.reason.excludes() { 0.0 } else { 1.0 },
        timestamp_ms: None,
    }
}

/// How long a provider's last good scrape stands.
///
/// `staleMetricsSeconds`, else half the signal TTL, and never under two scrape
/// intervals, so a provider is not judged stale between successful scrapes.
fn readiness_stale_after(ctx: &OperatorCtx, provider: &InferenceProvider) -> Duration {
    provider
        .spec
        .metrics_config
        .as_ref()
        .and_then(|mc| mc.stale_metrics_seconds)
        .filter(|secs| *secs > 0)
        .map_or_else(|| site_signals_ttl() / 2, |secs| Duration::from_secs(secs.into()))
        .max(ctx.scrape_interval.saturating_mul(2))
}

/// The provider's readiness verdict, `None` when it is not scraped for readiness or not yet judged.
pub(crate) fn provider_readiness(
    ctx: &OperatorCtx,
    provider: &InferenceProvider,
    now: Instant,
) -> Option<readiness::Verdict> {
    provider_metrics::signal_scrape_plan(provider)?;
    let unavailable = provider
        .status
        .as_ref()
        .is_some_and(|status| status.phase == crate::crd::inference_provider::ProviderPhase::Unavailable);
    ctx.readiness.verdict(
        &readiness::key(&provider.spec.grid_network_ref, provider.metadata.name.as_deref()?),
        unavailable,
        readiness_stale_after(ctx, provider),
        now,
    )
}

/// Write the provider's `Ready` condition when its status or reason changed.
async fn apply_ready_condition(
    client: &Client,
    provider: &InferenceProvider,
    verdict: &readiness::Verdict,
) -> Result<(), OperatorError> {
    let Some((name, patch)) = ready_condition_patch(provider, verdict) else {
        return Ok(());
    };
    let api: Api<InferenceProvider> = Api::all(client.clone());
    Box::pin(api.patch_status(
        name,
        &PatchParams::apply(READINESS_FIELD_MANAGER).force(),
        &Patch::Apply(patch),
    ))
    .await?;
    // A turn away from Ready warns; a return to Ready or a wait is informational.
    if verdict.reason.excludes() {
        tracing::warn!(
            provider = name,
            status = verdict.reason.status(),
            reason = verdict.reason.as_str(),
            message = %verdict.message,
            "provider readiness changed"
        );
    } else {
        tracing::info!(
            provider = name,
            status = verdict.reason.status(),
            reason = verdict.reason.as_str(),
            message = %verdict.message,
            "provider readiness changed"
        );
    }
    Ok(())
}

/// The status patch carrying the provider's new `Ready` condition, `None` when unchanged.
fn ready_condition_patch<'provider>(
    provider: &'provider InferenceProvider,
    verdict: &readiness::Verdict,
) -> Option<(&'provider str, Value)> {
    let name = provider.metadata.name.as_deref()?;
    let status = provider.status.as_ref();
    let current = status.map_or(&[][..], |status| status.conditions.as_slice());
    let now = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .ok()?;
    let condition = readiness::ready_condition(current, verdict, &now, provider.metadata.generation)?;
    Some((
        name,
        serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "status": { "conditions": [condition] }
        }),
    ))
}

/// Server-side apply manager for the `Ready` condition, apart from provider reconciliation.
const READINESS_FIELD_MANAGER: &str = "grid-operator-readiness";

/// Poll this site's providers once and hold the models they serve.
///
/// Called from its own loop rather than from reconcile, like
/// [`refresh_signals`]: a served set must expire on its own cadence, not wait
/// for a declaration change.
///
/// # Errors
///
/// Returns [`OperatorError`] when providers cannot be listed.
pub async fn refresh_served_models(
    ctx: &OperatorCtx,
    client: &Client,
    config: &served_models::DiscoveryConfig,
) -> Result<(), OperatorError> {
    served_models::discover(&ctx.served_models, client, config).await
}

/// Refresh who may read from the currently approved sites.
async fn register_peers(ctx: &OperatorCtx, client: &Client) -> Result<(), OperatorError> {
    let sites = list_all_grid_sites(client).await?;
    ctx.peer_identities
        .set(peer_identities(&sites, ctx.peer_settings.trust));
    Ok(())
}

/// What this site holds about each peer it knows.
///
/// Keyed by the bare `site_id` both poll and serve look a peer up by, which an
/// auto-discovered object's `{network}-{site_id}` name is not. When two objects
/// name one site, a pinned record outranks an unpinned one, then an enrolled
/// object outranks a discovered stub. `status` is deliberately not read: it is
/// populated from gossip, and a member could advertise its own certificate under
/// another site's name. The labels are the local object's for the same reason.
fn peer_identities(sites: &[GridSite], trust: PeerTrustMode) -> BTreeMap<String, signals::PeerRecord> {
    let pinned = trust == PeerTrustMode::Pin;
    let mut ranked = BTreeMap::<String, ((bool, bool), signals::PeerRecord)>::new();
    for site in sites {
        let Some((key, enrolled)) = peer_site_key(site) else {
            continue;
        };
        let record = signals::PeerRecord {
            labels: site.metadata.labels.clone().unwrap_or_default(),
            pins: site
                .spec
                .trust
                .as_ref()
                .filter(|_| pinned)
                .and_then(|site_trust| site_trust.canonical_fingerprints.as_deref())
                .unwrap_or_default()
                .iter()
                .map(|fp| signals::canonical_fingerprint(fp))
                .collect(),
        };
        let rank = (!record.pins.is_empty(), enrolled);
        if ranked.get(&key).is_none_or(|(held, _)| rank > *held) {
            ranked.insert(key, (rank, record));
        }
    }
    ranked.into_iter().map(|(key, (_, record))| (key, record)).collect()
}

/// The bare `site_id` a peer is keyed by, and whether the object is enrolled
/// rather than a discovered stub carrying [`ANNOTATION_SITE_ID`].
pub(crate) fn peer_site_key(site: &GridSite) -> Option<(String, bool)> {
    let discovered = site
        .metadata
        .labels
        .as_ref()
        .is_some_and(|labels| labels.get(LABEL_AUTO_DISCOVERED).is_some_and(|v| v == "true"));
    // Only discovery's own stubs speak for another id; any other object is its name.
    site.metadata
        .annotations
        .as_ref()
        .filter(|_| discovered)
        .and_then(|annotations| annotations.get(ANNOTATION_SITE_ID))
        .filter(|id| !id.trim().is_empty())
        .map(|id| (id.clone(), false))
        .or_else(|| site.metadata.name.clone().map(|name| (name, true)))
}

/// What a reader must satisfy to be served each provider's signals.
///
/// `accessPolicy.siteSelector` says who may route to a provider, and reading
/// its load is not a wider right than using it, so the same selector bounds
/// both.
fn signal_access(providers: &[InferenceProvider]) -> signals::AccessMap {
    providers
        .iter()
        .filter_map(|provider| {
            let target = routing_overlay::routing_identity(provider)?.to_owned();
            let required = provider.spec.access_policy.site_selector.match_labels.clone();
            (!required.is_empty()).then_some((target, vec![required]))
        })
        .collect()
}

/// Attribute this cycle's observations to this site and publish them.
///
/// The site label is applied here because this is where the membership identity
/// is known. A provider that set it itself keeps its value under an exported
/// name, so what a reader sees as the origin is what this site says it is.
fn publish_signals(ctx: &OperatorCtx, collected: HashMap<String, Vec<signals::Observation>>) {
    let site = ctx.swim().map(|s| s.site_name().to_owned()).unwrap_or_default();
    let attributed = collected
        .into_iter()
        .map(|(provider, observations)| {
            let attributed = signals::attribute(observations, &site, &provider);
            (provider, attributed)
        })
        .collect();
    ctx.signals.refresh(attributed, site_signals_ttl());
}

/// Client TLS for polling peer operators, built from the network's own trust.
///
/// The same CA and site identity the gateways already use between sites, so a
/// peer proves which site it is rather than only that it holds a key the mesh
/// shares.
///
/// # Errors
///
/// `Ok(None)` when the network declares no TLS. `Err` when the referenced
/// Secrets are declared but missing or unusable, so a caller can fail closed
/// rather than poll in plaintext.
pub async fn peer_tls_config(
    network: &GridNetwork,
    client: &Client,
) -> Result<Option<Arc<signals::PeerTlsMaterial>>, String> {
    let (Some(ca), Some(site)) = (&network.spec.tls.ca_secret_ref, &network.spec.tls.site_secret_ref) else {
        return Ok(None);
    };
    let ca_pem = read_signals_pem(client, ca, "ca.crt").await?;
    let cert_pem = read_signals_pem(client, site, "tls.crt").await?;
    let key_pem = read_signals_pem(client, site, "tls.key").await?;
    Ok(Some(Arc::new(signals::PeerTlsMaterial {
        ca: ca_pem,
        identity: Some(signals::PeerClientIdentity {
            cert: cert_pem,
            key: zeroize::Zeroizing::new(key_pem),
        }),
    })))
}

/// TLS for the signals listener, from the same material peers dial with.
///
/// Client auth is optional on purpose: the co-located gateway connects with
/// this site's own certificate and a peer presents its own, and that difference
/// is what the scope rule reads.
///
/// # Errors
///
/// `Ok(None)` when the network declares no TLS. `Err` when the configured
/// material cannot be read or parsed, so the listener fails closed rather than
/// serving every caller unauthenticated.
pub async fn signals_server_config(network: &GridNetwork, client: &Client) -> Result<Option<ServerTlsConfig>, String> {
    let (Some(ca), Some(site)) = (&network.spec.tls.ca_secret_ref, &network.spec.tls.site_secret_ref) else {
        return Ok(None);
    };
    let ca_pem = read_signals_pem(client, ca, "ca.crt").await?;
    let cert_pem = read_signals_pem(client, site, "tls.crt").await?;
    let key_pem = read_signals_pem(client, site, "tls.key").await?;
    crate::resources::tls_backend::build_server_config(&ca_pem, &cert_pem, &key_pem).map(Some)
}

/// This site's own leaf, as its co-located gateway presents it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnLeaf {
    /// SHA-256 of the leaf DER.
    pub fingerprint: String,
    /// The leaf's grid SPIFFE ID, when it carries one.
    pub spiffe: Option<String>,
    /// SHA-256 of the leaf a renewal replaced, while it is still valid.
    pub previous: Option<String>,
}

impl OwnLeaf {
    /// Whether `fingerprint` is this site's current or still-valid previous leaf.
    #[must_use]
    pub fn is_own(&self, fingerprint: &str) -> bool {
        self.fingerprint == fingerprint || self.previous.as_deref() == Some(fingerprint)
    }
}

/// This site's own leaf, for recognising its own workloads, `None` without TLS.
///
/// # Errors
///
/// Returns a message when the configured material cannot be read or parsed.
pub async fn own_leaf_identity(network: &GridNetwork, client: &Client) -> Result<Option<OwnLeaf>, String> {
    let Some(site) = &network.spec.tls.site_secret_ref else {
        return Ok(None);
    };
    let cert_pem = read_signals_pem(client, site, "tls.crt").await?;
    let pem = std::str::from_utf8(&cert_pem).map_err(|_e| "signals TLS: certificate is not valid UTF-8".to_owned())?;
    let der = crate::resources::tls_backend::first_cert_der_from_pem(pem).map_err(str::to_owned)?;
    // The co-located gateway may present the replaced leaf until it reloads.
    let previous = read_signals_pem(client, site, crate::enroll::renew::PREVIOUS_CERT)
        .await
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .filter(|replaced| certs::cert_expires_within(replaced, time::Duration::ZERO) == Ok(false))
        .and_then(|replaced| crate::resources::tls_backend::first_cert_der_from_pem(&replaced).ok())
        .map(|replaced| signals::leaf_fingerprint(&replaced));
    Ok(Some(OwnLeaf {
        fingerprint: signals::leaf_fingerprint(&der),
        spiffe: certs::leaf_spiffe_id(&der),
        previous,
    }))
}

/// Read one PEM value out of a Secret.
///
/// The structured `TlsFailureReason` is dropped deliberately: every signals
/// consumer only logs the message and fails closed, none sets a CRD
/// `status.reason`. Return `OperatorError`/`TlsFailureReason` instead the day a
/// consumer needs to branch on the cause.
async fn read_signals_pem(
    client: &Client,
    secret: &crate::crd::grid_network::SecretRef,
    key: &str,
) -> Result<Vec<u8>, String> {
    crate::resources::endpoint_tls::read_secret_bytes_for_tls(client, secret, key, "signals", "signals TLS")
        .await
        .map_err(|(_, message)| message)
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

/// Requeue while delegated gateway mounts may need rollout or Secret rotation detection.
const MOUNT_RECONCILIATION_REQUEUE: Duration = Duration::from_secs(60);

/// Field manager name for server-side apply.
const FIELD_MANAGER: &str = "grid-operator";

/// Finalizer that keeps a `GridNetwork` present until its SWIM scope withdraws.
const GRID_NETWORK_WITHDRAWAL_FINALIZER: &str = "grid.praxis.fast/gridnetwork-withdrawal";

/// Bound retries when another reconcile updates a `GridNetwork` finalizer at the same time.
const FINALIZER_PATCH_ATTEMPTS: usize = 4;

/// Retry interval while a deleting network awaits local SWIM withdrawal.
const WITHDRAWAL_RETRY_REQUEUE: Duration = Duration::from_secs(5);

/// Longest one reconcile waits for the runtime to publish a withdrawal.
const WITHDRAWAL_PUBLICATION_TIMEOUT: Duration = Duration::from_secs(5);

/// Label key that opts a `GridNetwork` into automatic `GridSite` discovery.
///
/// When this label is present with value `"true"`, the `GridNetwork` controller
/// creates `GridSite` resources for remote Alive SWIM members automatically.
/// Networks without this label are unaffected — their overlay generation uses
/// the existing `routingClusterRef`-based (Phase 1) fallback.
///
/// This opt-in gate prevents auto-discovery from changing the overlay generation
/// semantics for networks that were not designed with it in mind.
pub const LABEL_AUTO_DISCOVER_SITES: &str = "grid.praxis.fast/auto-discover-sites";

/// Marks a `GridSite` that discovery created from SWIM membership.
pub const LABEL_AUTO_DISCOVERED: &str = "grid.praxis.fast/auto-discovered";

/// Bare SWIM `site_id` on an auto-discovered `GridSite`, whose name carries a network prefix.
///
/// An annotation, not a label: the gossiped id is unvalidated, and a label the API server rejects fails the apply.
pub const ANNOTATION_SITE_ID: &str = "grid.praxis.fast/site-id";

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

/// Map an [`AgentToolProvider`] change to the [`GridNetwork`] it belongs to.
///
/// Returns `Some(ObjectRef)` for the `GridNetwork` named by
/// `spec.gridNetworkRef`, or `None` when the field is blank.
///
/// Used by the [`GridNetwork`] controller's cross-resource watch so that
/// changes to any `AgentToolProvider` trigger immediate overlay refresh of
/// the owning `GridNetwork`.
pub fn network_refs_from_agent_tool_provider(atp: AgentToolProvider) -> Option<ObjectRef<GridNetwork>> {
    let name = atp.spec.grid_network_ref;
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

/// Whether `network` carries the operator's SWIM-withdrawal finalizer.
fn has_withdrawal_finalizer(network: &GridNetwork) -> bool {
    network.metadata.finalizers.as_ref().is_some_and(|finalizers| {
        finalizers
            .iter()
            .any(|finalizer| finalizer == GRID_NETWORK_WITHDRAWAL_FINALIZER)
    })
}

/// The persisted grid ID used to scope state that may have reached SWIM peers.
///
/// Unlike [`resolve_grid_id`], this never generates a fallback UUID: an
/// unpersisted UUID cannot identify a scope that another reconciliation may
/// have published.
fn persisted_grid_id(network: &GridNetwork) -> Option<&str> {
    (!network.spec.grid_id.is_empty())
        .then_some(network.spec.grid_id.as_str())
        .or_else(|| {
            network
                .status
                .as_ref()
                .map(|status| status.grid_id.as_str())
                .filter(|grid_id| !grid_id.is_empty())
        })
}

/// Add or remove the SWIM-withdrawal finalizer without changing other finalizers.
#[expect(
    clippy::too_many_lines,
    reason = "bounded conflict retry must keep its rebase invariants together"
)]
#[expect(
    clippy::large_stack_frames,
    reason = "async Kubernetes API calls carry large request and response values"
)]
async fn patch_withdrawal_finalizer(
    network: &GridNetwork,
    client: &Client,
    present: bool,
) -> Result<(), OperatorError> {
    let name = grid_network_name(network)?;
    let api: Api<GridNetwork> = Api::all(client.clone());
    let mut resource_version = network.metadata.resource_version.clone();
    let mut finalizers = network.metadata.finalizers.clone().unwrap_or_default();

    for attempt in 0..FINALIZER_PATCH_ATTEMPTS {
        if !set_withdrawal_finalizer(&mut finalizers, present) {
            return Ok(());
        }

        let patch = serde_json::json!({
            "metadata": {
                "resourceVersion": resource_version,
                "finalizers": finalizers,
            }
        });
        match api.patch(name, &PatchParams::default(), &Patch::Merge(&patch)).await {
            Ok(_) => return Ok(()),
            Err(kube::Error::Api(error)) if error.code == 409 => {
                if attempt + 1 == FINALIZER_PATCH_ATTEMPTS {
                    return Err(kube::Error::Api(error).into());
                }

                // A normal reconcile can update status while this finalizer patch is in flight.
                // Rebase the narrow metadata change onto the latest finalizer list so we neither
                // surface a transient conflict as an operator error nor overwrite another finalizer.
                let Some(latest) = api.get_opt(name).await? else {
                    return Ok(());
                };
                resource_version.clone_from(&latest.metadata.resource_version);
                finalizers = latest.metadata.finalizers.unwrap_or_default();
                let already_present = finalizers
                    .iter()
                    .any(|finalizer| finalizer == GRID_NETWORK_WITHDRAWAL_FINALIZER);
                if already_present == present {
                    return Ok(());
                }
            },
            Err(error) => return Err(error.into()),
        }
    }

    Ok(())
}

/// Finalizer patches replace the list, so retain entries owned by other controllers
/// to avoid bypassing their cleanup during a conflicting reconcile.
fn set_withdrawal_finalizer(finalizers: &mut Vec<String>, present: bool) -> bool {
    let had_finalizer = finalizers
        .iter()
        .any(|finalizer| finalizer == GRID_NETWORK_WITHDRAWAL_FINALIZER);
    match (present, had_finalizer) {
        (true, false) => {
            finalizers.push(GRID_NETWORK_WITHDRAWAL_FINALIZER.to_owned());
            true
        },
        (false, true) => {
            finalizers.retain(|finalizer| finalizer != GRID_NETWORK_WITHDRAWAL_FINALIZER);
            true
        },
        _ => false,
    }
}

/// Complete a deleting network's local SWIM withdrawal before Kubernetes removes it.
#[expect(
    clippy::too_many_lines,
    reason = "finalizer lifecycle keeps the retry and confirmation decisions adjacent"
)]
#[expect(
    clippy::cognitive_complexity,
    reason = "finalizer lifecycle has explicit branches for every safe deletion outcome"
)]
#[expect(
    clippy::large_stack_frames,
    reason = "async Kubernetes finalizer patching carries API request and response values"
)]
async fn reconcile_deleting_grid_network(
    network: &GridNetwork,
    ctx: &OperatorCtx,
    name: &str,
) -> Result<Action, OperatorError> {
    if !has_withdrawal_finalizer(network) {
        return Ok(Action::await_change());
    }
    let Some(grid_id) = persisted_grid_id(network) else {
        tracing::info!(
            network = name,
            "GridNetwork has no persisted SWIM scope; removing withdrawal finalizer"
        );
        patch_withdrawal_finalizer(network, &ctx.client, false).await?;
        return Ok(Action::await_change());
    };
    let Some(swim) = ctx.swim() else {
        tracing::info!(network = name, "SWIM is disabled; removing withdrawal finalizer");
        patch_withdrawal_finalizer(network, &ctx.client, false).await?;
        return Ok(Action::await_change());
    };
    let receipt = match swim.withdraw_scope(grid_id.to_owned()) {
        Ok(receipt) => receipt,
        Err(error) => {
            tracing::warn!(network = name, %grid_id, %error, "failed to queue GridNetwork SWIM withdrawal");
            return Ok(Action::requeue(WITHDRAWAL_RETRY_REQUEUE));
        },
    };
    match timeout(WITHDRAWAL_PUBLICATION_TIMEOUT, receipt).await {
        Ok(Ok(())) => {
            patch_withdrawal_finalizer(network, &ctx.client, false).await?;
            tracing::info!(network = name, %grid_id, "published GridNetwork SWIM withdrawal; removed finalizer");
            Ok(Action::await_change())
        },
        Ok(Err(_)) => {
            tracing::warn!(network = name, %grid_id, "SWIM runtime exited before publishing GridNetwork withdrawal");
            Ok(Action::requeue(WITHDRAWAL_RETRY_REQUEUE))
        },
        Err(_) => {
            tracing::warn!(network = name, %grid_id, "timed out waiting for local GridNetwork SWIM withdrawal publication");
            Ok(Action::requeue(WITHDRAWAL_RETRY_REQUEUE))
        },
    }
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

    if network.metadata.deletion_timestamp.is_some() {
        return reconcile_deleting_grid_network(&network, &ctx, name).await;
    }
    if ctx.swim().is_some() && !has_withdrawal_finalizer(&network) {
        patch_withdrawal_finalizer(&network, &ctx.client, true).await?;
        return Ok(Action::await_change());
    }

    reject_invalid_budget_policy(&network)?;

    tracing::debug!(name, "reconciling GridNetwork");

    // Modes are fixed at startup, so a change restarts the pod to apply it.
    let running = GridModes {
        signal: ctx.signal_mode,
        trust: ctx.peer_settings.trust,
    };
    if let Some(next) = running.restart_for(Some(&network)) {
        tracing::info!(
            "GridNetwork {name} sets signalTransport={:?} peerTrust={:?}; running {:?}/{:?}; restarting to apply",
            next.signal,
            next.trust,
            running.signal,
            running.trust
        );
        #[expect(clippy::exit, reason = "modes apply only at startup; Kubernetes restarts the pod")]
        std::process::exit(0);
    }
    ctx.publish_declared_trust(&network);

    let client = &ctx.client;
    ensure_tls_secrets(&network, client, ctx.site_name.as_deref()).await?;

    if let Some(swim) = ctx.swim() {
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
        announce_crd_seeds(&network, swim, &ctx.last_seeds).await;

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

    // Rendering before membership converges would drop remote entries.
    if let Some(hold) = ctx.membership_hold() {
        tracing::debug!(network = name, "holding membership-derived writes until SWIM converges");
        return Ok(hold);
    }

    // List providers once; share between routing overlay rendering and CRDT publishing.
    let providers = list_all_inference_providers(client).await?;
    let mut requeue_interval = requeue_interval_for_network(&network, &providers)?;
    if network.spec.gateway_refs.iter().any(|gateway| {
        gateway.consumer_config.as_ref().is_some_and(|config| {
            config.enabled
                && config
                    .mount_reconciliation
                    .as_ref()
                    .is_some_and(|mounts| mounts.enabled)
        })
    }) {
        requeue_interval = requeue_interval.min(MOUNT_RECONCILIATION_REQUEUE);
    }
    // In poll mode, load travels the signals path:
    // the operator neither scrapes providers for scoring nor lets a metric
    // sample reach gossip or the overlay. An empty collection makes the overlay
    // score neutrally (no load-driven reordering, no ConfigMap churn per scrape)
    // and makes `publish_real_provider_state` carry no metrics into the gossiped
    // record, so a scrape no longer mutates replicated state.
    let collected = if ctx.signal_mode == SignalMode::Poll {
        provider_metrics::CollectedMetrics::default()
    } else {
        provider_metrics::collect_provider_metrics_with_refresh_interval(
            name,
            &providers,
            &ctx.metrics_cache,
            Instant::now(),
            requeue_interval,
            Some(client),
        )
        .await
    };
    let raw_metrics = collected.metrics;

    // Admission is a controller decision held over time, not re-derived from a
    // metric on every render. With the feature on there are no live metrics, so
    // the strategy is `NoMetrics`: a provider present and not Unavailable is
    // offered, and the gateway picks among them from the signal it polls.
    let scoring_strategy = if ctx.signal_mode == SignalMode::Poll {
        crate::crd::grid_network::ScoringStrategy::NoMetrics
    } else {
        network
            .spec
            .scoring_policy
            .as_ref()
            .map_or(crate::crd::grid_network::ScoringStrategy::NoMetrics, |policy| {
                policy.strategy
            })
    };
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
            let state = crate::resources::geography::apply_administrative_drain(
                memory.evaluate(&memory_key, observation, admission_policy, now),
                provider.spec.traffic_policy.as_ref().is_some_and(|p| p.drain),
            );
            // Readiness gates admission whatever the scoring strategy, NoMetrics included.
            let not_ready = provider_readiness(&ctx, provider, now).is_some_and(|verdict| verdict.reason.excludes());
            let state = if not_ready {
                crate::resources::geography::AdmissionState::Excluded
            } else {
                state
            };
            admission_keys.push(memory_key);
            admission_states.insert(identity, state);
        }
        memory.retain_network_keys(name, admission_keys.iter().cloned());
    }

    let remote_crdt_providers: Vec<crdt::ProviderState> = ctx
        .swim()
        .map(|swim| collect_remote_crdt_providers(swim, name))
        .unwrap_or_default();

    // Obtain a live membership snapshot here - used both for staleness override below
    // and for phase determination after the overlay step.
    // When swim is None (runtime not configured), falls through to static phase logic.
    let swim_runtime_running = ctx.swim().is_none_or(|handle| handle.is_running());
    let membership = ctx.swim().map(|h| h.snapshot());

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
    let serving = serving_source(&ctx, membership.as_ref());

    // List tool providers once; shared between overlay rendering and CRDT publishing.
    let tool_providers = list_all_agent_tool_providers(client).await?;

    let OverlayOutcome {
        consumer_statuses: consumer_config_statuses,
        mount_statuses: mount_reconciliation_statuses,
        overlay_statuses,
        serving_retry,
    } = Box::pin(reconcile_routing_overlay_inner(
        &network,
        client,
        &providers,
        &tool_providers,
        &remote_crdt_providers,
        &raw_metrics,
        &scoring_weights,
        &admission_states,
        serving.as_ref(),
        ctx.site_name(),
    ))
    .await?;

    let grid_id = resolve_grid_id(&network);
    let identity = site_identity_status(
        &network,
        client,
        time::OffsetDateTime::now_utc(),
        ctx.rotation && GridModes::of(&network).renews(),
    )
    .await;
    // An expired or unreadable identity degrades the network.
    let identity_failed = identity.as_ref().is_some_and(|status| !status.reason.is_empty());
    let phase = if swim_runtime_running && !identity_failed {
        determine_phase(&network, &grid_id, membership.as_ref())
    } else {
        GridNetworkPhase::Degraded
    };

    // Publish provider CRDT state so peers learn this site's providers.
    let distributed_provider_count = if let Some(swim) = ctx.swim().filter(|handle| handle.is_running()) {
        publish_real_provider_state(swim, name, &grid_id, &providers, &tool_providers, &raw_metrics);
        log_capacity_changes(&ctx.logged, name, &providers);
        count_remote_provider_records(swim, name)
    } else {
        0
    };

    // Resolve per-tenant budget status from the merged CRDT spend state, if any.
    // Empty tenant_spend (SWIM disabled, or no spend broadcast received yet) is
    // indistinguishable here from "no spend recorded" — resolve_budget_statuses
    // still emits a zero-spend entry for every policy-declared tenant.
    let tenant_spend = ctx
        .swim()
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
        mount_reconciliation_statuses,
        overlay_statuses,
        budget_statuses,
        identity,
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
    if auto_discover_enabled && let (Some(swim), Some(snapshot)) = (ctx.swim(), membership.as_ref()) {
        reconcile_local_site(name, swim.site_name(), client).await?;
        reconcile_discovered_sites(&ctx, &network, name, swim.site_name(), snapshot).await?;
    }

    // A deferred serving write lands as soon as its spacing allows.
    Ok(Action::requeue(
        serving_retry.map_or(requeue_interval, |wait| requeue_interval.min(wait)),
    ))
}

/// Advance the explicitly configured local [`GridSite`] into discovery.
///
/// Remote sites are created from SWIM membership, but the local site is
/// intentionally excluded from that discovery list. When a topology provides
/// its local `GridSite` declaratively, this controller must still hand it to the
/// `GridSite` controller for gateway probing. The status write is idempotent and
/// only applies while the site is still pending.
#[expect(
    clippy::too_many_lines,
    reason = "status patch keeps the local-site transition explicit"
)]
async fn reconcile_local_site(network_name: &str, local_site: &str, client: &Client) -> Result<(), OperatorError> {
    let api: Api<GridSite> = Api::all(client.clone());
    let Ok(site) = api.get(local_site).await else {
        return Ok(());
    };
    if site.spec.grid_network_ref != network_name {
        return Ok(());
    }
    let pending = site
        .status
        .as_ref()
        .is_none_or(|status| status.phase == GridSitePhase::Pending);
    if !pending {
        return Ok(());
    }
    let status_doc = serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridSite",
        "status": {
            "phase": "Discovered",
            "reason": "SWIMDiscovered",
            "message": "local site configured and ready for gateway probing"
        }
    });
    api.patch_status(
        local_site,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(&status_doc),
    )
    .await?;
    Ok(())
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
    let network_name = network.metadata.name.as_deref().unwrap_or("<unknown>");
    let Some(swim_key_ref) = &network.spec.tls.swim_key_ref else {
        return release_plain_if_undeclared(client, swim, network_name).await;
    };

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

/// Release a pending hold to plaintext once no `GridNetwork` declares a SWIM key.
async fn release_plain_if_undeclared(client: &Client, swim: &SwimHandle, network: &str) -> Result<(), OperatorError> {
    if !swim.is_key_pending() {
        return Ok(());
    }
    let networks = Api::<GridNetwork>::all(client.clone())
        .list(&ListParams::default())
        .await?
        .items;
    if !declares_swim_key(&networks) && swim.release_plain() {
        tracing::warn!(
            network,
            "no GridNetwork declares a swimKeyRef; SWIM gossip is plaintext"
        );
    }
    Ok(())
}

/// Whether any of `networks` declares a SWIM key.
#[must_use]
pub fn declares_swim_key(networks: &[GridNetwork]) -> bool {
    networks.iter().any(|network| network.spec.tls.swim_key_ref.is_some())
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
    if error.is_conflict() {
        // Another writer won the race. Re-read and reapply rather than wait out the backoff.
        tracing::debug!(%error, "GridNetwork moved underneath the write; reapplying");
        return Action::requeue(Duration::from_secs(1));
    }
    tracing::error!(%error, "GridNetwork reconciliation failed");
    Action::requeue(Duration::from_secs(30))
}

// ---------------------------------------------------------------------------
// CRD-driven SWIM seeds
// ---------------------------------------------------------------------------

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
    reason = "linear announce sequence: resolve → diff → log → announce → update tracker"
)]
async fn announce_crd_seeds(
    network: &GridNetwork,
    swim: &SwimHandle,
    last_seeds: &std::sync::Mutex<HashMap<String, Vec<SocketAddr>>>,
) {
    let name = network.metadata.name.as_deref().unwrap_or("?");
    let prev = last_seeds
        .lock()
        .unwrap_or_else(|e| {
            tracing::warn!("last_seeds lock poisoned, recovering");
            e.into_inner()
        })
        .get(name)
        .cloned()
        .unwrap_or_default();
    let resolution = resolve_endpoint_list_partial(&network.spec.seeds, "GridNetwork.spec.seeds").await;
    for failure in &resolution.failures {
        tracing::warn!(
            network = name,
            source = %failure.source,
            endpoint = %failure.endpoint,
            reason = %failure.reason,
            "ignoring unusable GridNetwork seed"
        );
    }
    let mut seeds = match crd_seed_decision(&resolution, &prev) {
        CrdSeedDecision::Announce(seeds) => seeds,
        CrdSeedDecision::Retain => {
            tracing::warn!(
                network = name,
                "all configured GridNetwork seeds failed; retaining the last-known-good seed set"
            );
            return;
        },
        CrdSeedDecision::Seedless => {
            tracing::warn!(
                network = name,
                "no configured GridNetwork seed resolved and no last-known-good set exists; keeping SWIM seedless"
            );
            return;
        },
    };
    seeds.retain(|addr| *addr != swim.local_addr());

    // Log what changed since the last reconcile using diff_seed_sets.
    // Always announce the full set for robustness (idempotent, handles channel-full retries).
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

/// Action for a CRD seed update after partial DNS resolution.
enum CrdSeedDecision {
    /// Announce the supplied resolved set.
    Announce(Vec<SocketAddr>),
    /// Keep the previous announced set because all current lookups failed.
    Retain,
    /// Keep SWIM active without seeds because no prior set exists.
    Seedless,
}

/// Decide whether a CRD seed reconciliation may replace the announced set.
fn crd_seed_decision(resolution: &SeedResolution, previous: &[SocketAddr]) -> CrdSeedDecision {
    if !resolution.addresses.is_empty() {
        CrdSeedDecision::Announce(resolution.addresses.clone())
    } else if resolution.configured && !previous.is_empty() {
        CrdSeedDecision::Retain
    } else if resolution.configured {
        CrdSeedDecision::Seedless
    } else {
        CrdSeedDecision::Announce(Vec::new())
    }
}

// ---------------------------------------------------------------------------
// TLS Secrets
// ---------------------------------------------------------------------------

/// What to do about the grid TLS Secrets a `GridNetwork` names.
#[derive(Debug, PartialEq, Eq)]
enum TlsSecrets {
    /// Both exist.
    Present,
    /// Neither exists: create a self-signed CA and a site certificate from it.
    Create,
    /// Only one exists. A new CA would replace the grid's, so nothing is written.
    Inconsistent,
}

/// Decide from which of the CA and site Secrets exist.
fn tls_secrets_action(ca_exists: bool, site_exists: bool) -> TlsSecrets {
    match (ca_exists, site_exists) {
        (true, true) => TlsSecrets::Present,
        (false, false) => TlsSecrets::Create,
        _ => TlsSecrets::Inconsistent,
    }
}

/// Create a self-signed CA and site certificate when neither Secret exists.
///
/// Generates both together so the CA is available for signing the site
/// certificate. Never replaces an existing CA: one that enrollment or bootstrap
/// wrote is the grid's, and a new one would split it.
#[expect(
    clippy::large_stack_frames,
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    reason = "decide, then create the CA and the identity it signs, as one step"
)]
async fn ensure_tls_secrets(
    network: &GridNetwork,
    client: &Client,
    this_site: Option<&str>,
) -> Result<(), OperatorError> {
    let tls = &network.spec.tls;
    let (Some(ca_ref), Some(site_ref)) = (&tls.ca_secret_ref, &tls.site_secret_ref) else {
        return Ok(());
    };

    let ca_api: Api<Secret> = Api::namespaced(client.clone(), &ca_ref.namespace);
    let site_api: Api<Secret> = Api::namespaced(client.clone(), &site_ref.namespace);

    let ca_exists = ca_api.get_opt(&ca_ref.name).await?.is_some();
    let site_exists = site_api.get_opt(&site_ref.name).await?.is_some();

    let action = tls_secrets_action(ca_exists, site_exists);
    if note_inconsistent(
        ObjectRef::new(&network_site_name(network)),
        action == TlsSecrets::Inconsistent,
    ) {
        tracing::warn!(
            ca = %ca_ref.name,
            site = %site_ref.name,
            "only one of the grid CA and site identity Secrets exists; not replacing the CA. Re-enroll \
             the site, or delete both to start a self-signed grid"
        );
    }
    if action != TlsSecrets::Create {
        return Ok(());
    }

    let Some(site_name) = issued_site_name(network, this_site) else {
        // The network name is a Kubernetes object name, which may carry dots and run to 253
        // characters, so it is not always a site name. Self-signing is a convenience; refusing
        // it leaves the site to enroll rather than failing every later step in this reconcile.
        tracing::warn!(
            network = %network_site_name(network),
            "no valid site name for a self-signed identity; enroll this site instead"
        );
        return Ok(());
    };
    let ca = certs::generate_ca("grid-ca")?;
    let site_cert = certs::generate_site_cert(&ca, &site_name)?;

    // Another writer, such as enrollment, stored its CA first: it is the grid's, and a
    // site identity from this one would not chain to it.
    if !create_secret(
        &ca_api,
        secret::build(&ca_ref.name, &ca_ref.namespace, secret::ca_secret_data(&ca)),
    )
    .await?
    {
        tracing::debug!(ca = %ca_ref.name, "another writer created the grid CA; not self-signing a site identity");
        return Ok(());
    }
    let site = secret::build(
        &site_ref.name,
        &site_ref.namespace,
        secret::site_cert_secret_data(&site_cert),
    );
    if create_secret(&site_api, site).await? {
        info!(ca = %ca_ref.name, site = %site_ref.name, "created a self-signed grid CA and site identity");
    } else {
        tracing::warn!(
            ca = %ca_ref.name,
            site = %site_ref.name,
            "created a self-signed grid CA, but another writer created the site identity, which may not \
             chain to it"
        );
    }
    Ok(())
}

/// Create `secret`, returning whether this call created it: a Secret another writer
/// created first is never overwritten.
async fn create_secret(api: &Api<Secret>, secret: Secret) -> Result<bool, OperatorError> {
    match api.create(&kube::api::PostParams::default(), &secret).await {
        Ok(_) => Ok(true),
        Err(kube::Error::Api(response)) if response.code == 409 => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// Networks whose TLS Secrets were last seen inconsistent, so the warning fires once
/// per transition rather than every reconcile.
static INCONSISTENT_TLS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<ObjectRef<GridNetwork>>>> =
    std::sync::LazyLock::new(Default::default);

/// Record whether `network`'s TLS Secrets are inconsistent, returning whether it just became so.
fn note_inconsistent(network: ObjectRef<GridNetwork>, inconsistent: bool) -> bool {
    let mut seen = INCONSISTENT_TLS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if inconsistent {
        seen.insert(network)
    } else {
        seen.remove(&network);
        false
    }
}

// ---------------------------------------------------------------------------
// Routing Overlay
// ---------------------------------------------------------------------------

/// Publish each site's phase as `grid_site_phase`.
fn publish_site_phases(sites: &[GridSite]) {
    crate::metrics::set_site_phases(site_phases(sites));
}

/// Each site's name and the phase its printer column shows. No status yet is Pending,
/// the phase a new `GridSite` starts in.
fn site_phases(sites: &[GridSite]) -> impl Iterator<Item = (&str, &'static str)> {
    sites.iter().filter_map(|site| {
        let phase = site.status.as_ref().map_or("Pending", |status| {
            crate::controller::grid_site::phase_label(&status.phase)
        });
        Some((site.metadata.name.as_deref()?, phase))
    })
}

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
    tool_providers: &[AgentToolProvider],
    remote_crdt_providers: &[crdt::ProviderState],
    raw_metrics: &HashMap<String, scoring::BackendMetrics>,
    scoring_weights: &scoring::ScoringWeights,
    admission_states: &HashMap<String, crate::resources::geography::AdmissionState>,
    serving: Option<&ServingSource<'_>>,
    owning_site: Option<&str>,
) -> Result<OverlayOutcome, OperatorError> {
    let network_name = grid_network_name(network)?;

    // Every reconcile lists the sites, so the phase gauge follows each status change and deletion.
    let sites = list_all_grid_sites(client)
        .await
        .inspect_err(|_error| crate::metrics::clear_site_phases())?;
    publish_site_phases(&sites);

    let metrics_by_str: HashMap<&str, scoring::BackendMetrics> =
        raw_metrics.iter().map(|(k, v)| (k.as_str(), *v)).collect();
    let metrics_arg = if metrics_by_str.is_empty() {
        None
    } else {
        Some(&metrics_by_str)
    };

    let observed_generation = network.metadata.generation.unwrap_or(0);
    let mut consumer_statuses: Vec<ConsumerConfigStatus> = Vec::new();
    let mut mount_statuses: Vec<MountReconciliationStatus> = Vec::new();
    let mut overlay_statuses: Vec<OverlayRevisionStatus> = Vec::new();
    let mut serving_retry: Option<Duration> = None;

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
            tool_providers,
            &eligible_remote_owned,
            local_site,
            owning_site,
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
        let no_candidates = overlay.candidates.is_empty();
        if no_candidates {
            tracing::warn!(
                network = network_name,
                gateway = %gw_ref.name,
                "routing overlay has no candidates; distributing authoritative no-route state"
            );
        }
        let credential_bearing = overlay
            .candidates
            .iter()
            .any(|candidate| candidate.credential.is_some());
        if credential_bearing
            && let Some(cc) = gw_ref.consumer_config.as_ref().filter(|cc| cc.enabled)
            && !cc.enable_projected_credentials
        {
            let error = OperatorError::ConsumerConfigRender(ConsumerConfigError::ProjectedCredentialsUnsupported);
            consumer_statuses.push(consumer_config_status_error(gw_ref, cc, &error, observed_generation));
            overlay_statuses.push(retained_overlay_status(
                network,
                gw_ref,
                observed_generation,
                Some(&render),
                "ProjectedCredentialsUnsupported",
                "credential-bearing overlay requires a rolled-out projected credential filter and mounted Secrets",
            ));
            continue;
        }
        // For delegated gateways, make the generated config and all of its
        // Secret mounts available before publishing candidates that can use
        // them. A pending or failed mount/config rollout retains the previous
        // routing overlay. An empty overlay is published first so a mount or
        // config failure cannot retain a previously serving route.
        let mut delegated_mount_reconciled = false;
        if !no_candidates
            && let Some(cc) = gw_ref
                .consumer_config
                .as_ref()
                .filter(|cc| cc.enabled && cc.mount_reconciliation.as_ref().is_some_and(|mounts| mounts.enabled))
        {
            match Box::pin(apply_consumer_config_for_gateway(
                &overlay,
                network_name,
                gw_ref,
                cc,
                &network.spec.tls,
                observed_generation,
                client,
            ))
            .await
            {
                Ok(outcome) => {
                    if outcome.config_applied && (!credential_bearing || cc.supports_projected_credentials) {
                        consumer_statuses.push(consumer_config_status_rendered(gw_ref, cc, observed_generation));
                    }
                    if let Some(status) = outcome.mount_status {
                        delegated_mount_reconciled = !status.applied_revision.is_empty();
                        mount_statuses.push(status);
                    }
                },
                Err(error) => {
                    tracing::warn!(
                        network = network_name,
                        gateway = %gw_ref.name,
                        namespace = %gw_ref.namespace,
                        error = %error,
                        "delegated gateway mounts are not ready; retaining the previously distributed routing overlay"
                    );
                    consumer_statuses.push(consumer_config_status_error(gw_ref, cc, &error, observed_generation));
                    let delegation = cc.mount_reconciliation.as_ref().filter(|mounts| mounts.enabled);
                    mount_statuses.push(mount_reconciliation_status_error(
                        gw_ref,
                        delegation,
                        &error,
                        observed_generation,
                    ));
                },
            }
            if !delegated_mount_reconciled {
                overlay_statuses.push(retained_overlay_status(
                    network,
                    gw_ref,
                    observed_generation,
                    Some(&render),
                    "GatewayMountsNotReady",
                    "routing overlay publication is waiting for mounted Secrets and a ready gateway rollout",
                ));
                continue;
            }
        }

        if credential_bearing
            && let Some(cc) = gw_ref.consumer_config.as_ref().filter(|cc| cc.enabled)
            && !cc.supports_projected_credentials
        {
            let error = OperatorError::ConsumerConfigRender(ConsumerConfigError::ProjectedCredentialsUnsupported);
            consumer_statuses.push(consumer_config_status_error(gw_ref, cc, &error, observed_generation));
            overlay_statuses.push(retained_overlay_status(
                network,
                gw_ref,
                observed_generation,
                Some(&render),
                "ProjectedCredentialsUnsupported",
                "credential-bearing overlay requires a rolled-out projected credential filter and mounted Secrets",
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
        overlay_statuses.push(OverlayRevisionStatus {
            gateway_name: gw_ref.name.clone(),
            namespace: gw_ref.namespace.clone(),
            config_map_name: render.config_map_name,
            schema_version: render.schema_version,
            rendered_revision: render.revision_hex.clone(),
            distributed_revision: render.revision_hex.clone(),
            content_digest: render.revision_hex,
            config_map_resource_version: resource_version,
            rendered_at: render.rendered_at,
            candidate_count: render.candidate_count,
            phase: OverlayPhase::Distributed,
            reason: String::new(),
            message: String::new(),
            observed_generation,
        });

        // The embedded gateway watches valid serving revisions independently.
        if let Some(source) = serving {
            match apply_serving_config(&overlay, source, network_name, gw_ref, client).await {
                Ok(retry) => serving_retry = serving_retry.into_iter().chain(retry).min(),
                Err(error) => {
                    tracing::warn!(network = network_name, gateway = %gw_ref.name, %error, "serving config apply failed");
                    serving_retry = serving_retry.into_iter().chain([SERVING_RETRY_REQUEUE]).min();
                },
            }
        }

        // Gateways with consumerConfig.enabled=false get a Disabled entry.
        // Gateways without a consumerConfig block are omitted from status.
        if !delegated_mount_reconciled && let Some(cc) = gw_ref.consumer_config.as_ref().filter(|cc| cc.enabled) {
            match Box::pin(apply_consumer_config_for_gateway(
                &overlay,
                network_name,
                gw_ref,
                cc,
                &network.spec.tls,
                observed_generation,
                client,
            ))
            .await
            {
                Ok(outcome) => {
                    if outcome.config_applied && (!credential_bearing || cc.supports_projected_credentials) {
                        consumer_statuses.push(consumer_config_status_rendered(gw_ref, cc, observed_generation));
                    }
                    if let Some(status) = outcome.mount_status {
                        mount_statuses.push(status);
                    }
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
                    let delegation = cc.mount_reconciliation.as_ref().filter(|mounts| mounts.enabled);
                    mount_statuses.push(mount_reconciliation_status_error(
                        gw_ref,
                        delegation,
                        &e,
                        observed_generation,
                    ));
                },
            }
        } else if let Some(cc) = gw_ref.consumer_config.as_ref().filter(|cc| !cc.enabled) {
            consumer_statuses.push(consumer_config_status_disabled(gw_ref, cc, observed_generation));
        }
    }
    Ok(OverlayOutcome {
        consumer_statuses,
        mount_statuses,
        overlay_statuses,
        serving_retry,
    })
}

/// What one routing overlay pass produced, per gateway.
struct OverlayOutcome {
    /// Consumer config render and apply results.
    consumer_statuses: Vec<ConsumerConfigStatus>,
    /// Delegated gateway Secret mount outcomes.
    mount_statuses: Vec<MountReconciliationStatus>,
    /// Overlay distribution results.
    overlay_statuses: Vec<OverlayRevisionStatus>,
    /// Soonest a deferred serving config write can land.
    serving_retry: Option<Duration>,
}

/// Gossip the serving config renders from, present only under poll.
struct ServingSource<'src> {
    /// Dialable `(site, signals endpoint)` members.
    members: Vec<(&'src str, String)>,
    /// Declared leaf digests per member, empty outside pin trust.
    pins: BTreeMap<String, Vec<String>>,
    /// Write coalescing state.
    gate: &'src WriteGate,
    /// Peer addressing resolved at startup.
    settings: &'src PeerSettings,
    /// How often this operator scrapes its providers.
    scrape_interval: Duration,
    /// Sites each network's serving config last refused.
    refused: &'src serving_config::RefusedSites,
}

/// Build the serving source from membership, `None` outside poll mode.
fn serving_source<'src>(
    ctx: &'src OperatorCtx,
    membership: Option<&'src MembershipSnapshot>,
) -> Option<ServingSource<'src>> {
    if ctx.signal_mode != SignalMode::Poll {
        return None;
    }
    let encrypted = ctx.swim().is_some_and(|swim| swim.is_encrypted());
    let identities = ctx.peer_identities();
    let settings = &ctx.peer_settings;
    let dialable = membership
        .map(|snapshot| {
            serving_config::dialable_members(snapshot, &identities, settings.trust, encrypted, settings.peer_port)
        })
        .unwrap_or_default();
    let mut pins = BTreeMap::new();
    let mut members = Vec::with_capacity(dialable.len());
    for (site, endpoint, declared) in dialable {
        if !declared.is_empty() {
            pins.insert(site.to_owned(), declared);
        }
        members.push((site, endpoint));
    }
    Some(ServingSource {
        members,
        pins,
        gate: &ctx.serving_writes,
        settings: &ctx.peer_settings,
        scrape_interval: ctx.scrape_interval,
        refused: &ctx.refused_sites,
    })
}

/// Render the serving config text for one gateway, including authoritative no-route state.
fn render_serving_text(
    overlay: &routing_overlay::RoutingOverlay,
    source: &ServingSource<'_>,
    gw_ref: &GatewayRef,
) -> Result<String, OperatorError> {
    let tls_mount = gw_ref
        .consumer_config
        .as_ref()
        .map_or(crate::crd::grid_network::DEFAULT_TLS_CERT_MOUNT_PATH, |cc| {
            cc.tls_cert_mount_path.as_str()
        });
    let provider_hop_sni = serving_provider_hop_sni_for_overlay(overlay, gw_ref)?;
    let provider_hop_clusters: BTreeSet<String> = provider_hop_sni.keys().cloned().collect();
    let inputs = ServingInputs {
        provider_hop_clusters: &provider_hop_clusters,
        provider_hop_sni: &provider_hop_sni,
        tls_mount,
        local_signals_addr: source.settings.local_signals_addr.as_deref(),
        pins: &source.pins,
        scrape_interval: source.scrape_interval,
    };
    let members = source.members.iter().map(|(site, endpoint)| (*site, endpoint.as_str()));
    serving_config::to_text(&serving_config::render(overlay, members, &inputs)).map_err(OperatorError::Json)
}

/// No candidate can receive hop context after an authoritative withdrawal.
fn serving_provider_hop_sni_for_overlay(
    overlay: &routing_overlay::RoutingOverlay,
    gw_ref: &GatewayRef,
) -> Result<BTreeMap<String, String>, OperatorError> {
    if overlay.candidates.is_empty() {
        if let Err(error) = serving_provider_hop_clusters(gw_ref) {
            tracing::warn!(gateway = %gw_ref.name, %error,
                "invalid provider-hop declaration ignored for empty serving revision");
        }
        Ok(BTreeMap::new())
    } else {
        serving_provider_hop_clusters(gw_ref)?;
        Ok(gw_ref
            .provider_hop_endpoints
            .iter()
            .map(|endpoint| {
                (
                    endpoint.cluster.clone(),
                    endpoint.transport.sni.clone().unwrap_or_default(),
                )
            })
            .collect())
    }
}

/// Resolve embedded-gateway provider hops from their dedicated `GatewayRef`
/// contract, not from the optional generated consumer Praxis config.
#[expect(
    clippy::too_many_lines,
    reason = "each provider-hop declaration is checked before routing"
)]
fn serving_provider_hop_clusters(gw_ref: &GatewayRef) -> Result<BTreeSet<String>, OperatorError> {
    let mut clusters = BTreeSet::new();
    for endpoint in &gw_ref.provider_hop_endpoints {
        if endpoint.cluster.trim().is_empty() {
            return Err(OperatorError::InvalidResource(
                "providerHopEndpoints cluster must not be blank".to_owned(),
            ));
        }
        if !clusters.insert(endpoint.cluster.clone()) {
            return Err(OperatorError::InvalidResource(format!(
                "providerHopEndpoints contains duplicate cluster {:?}",
                endpoint.cluster
            )));
        }
        if endpoint.transport.mode != TransportMode::MutualTls {
            return Err(OperatorError::InvalidResource(format!(
                "providerHopEndpoints cluster {:?} must use mutual_tls",
                endpoint.cluster
            )));
        }
        if endpoint
            .transport
            .sni
            .as_deref()
            .is_none_or(|sni| sni.trim().is_empty())
        {
            return Err(OperatorError::InvalidResource(format!(
                "providerHopEndpoints cluster {:?} requires a nonblank SNI",
                endpoint.cluster
            )));
        }
    }
    Ok(clusters)
}

/// Apply the serving config `ConfigMap` when changed, returning when to retry a deferred write.
async fn apply_serving_config(
    overlay: &routing_overlay::RoutingOverlay,
    source: &ServingSource<'_>,
    network_name: &str,
    gw_ref: &GatewayRef,
    client: &Client,
) -> Result<Option<Duration>, OperatorError> {
    serving_config::warn_refused_sites(overlay, network_name, source.refused);
    let text = render_serving_text(overlay, source, gw_ref)?;
    let name = serving_config::configmap_name(network_name, &gw_ref.name);
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), &gw_ref.namespace);
    let existing = api.get_opt(&name).await?;
    let stored = existing
        .as_ref()
        .and_then(|cm| cm.data.as_ref())
        .and_then(|data| data.get(serving_config::SERVING_CONFIG_KEY))
        .map(String::as_str);
    let key = format!("{}/{name}", gw_ref.namespace);
    let now = Instant::now();
    let decision = if overlay.candidates.is_empty() {
        serving_config::decide_empty_withdrawal(stored, &text)
    } else {
        serving_config::decide_write(stored, &text, source.gate.last(&key), now)
    };
    match decision {
        WriteDecision::Unchanged => return Ok(None),
        WriteDecision::Deferred(wait) => return Ok(Some(wait)),
        WriteDecision::Write => {},
    }
    let cm = serving_config::build_configmap(&text, network_name, &gw_ref.name, &gw_ref.namespace);
    api.patch(&name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(&cm))
        .await?;
    source.gate.record(&key, now);
    info!(cm_name = %name, digest = %serving_config::digest(&text), "applied grid serving config");
    Ok(None)
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

/// Keep each prior `rendered_at` when only timestamps changed, so a status write never retriggers reconcile.
fn keep_rendered_at(
    current: Option<&GridNetworkStatus>,
    desired: Vec<OverlayRevisionStatus>,
) -> Vec<OverlayRevisionStatus> {
    let prior = current.map_or(&[][..], |status| status.overlay_status.as_slice());
    desired
        .into_iter()
        .map(|mut entry| {
            if let Some(p) = prior
                .iter()
                .find(|p| p.gateway_name == entry.gateway_name && p.namespace == entry.namespace)
                && same_rendered_content(p, &entry)
            {
                entry.rendered_at.clone_from(&p.rendered_at);
            }
            entry
        })
        .collect()
}

/// Keep last applied consumer status for gateways whose empty candidate set
/// deliberately retained their previous working config.
#[expect(
    clippy::too_many_lines,
    reason = "preserving last-good per-gateway status is one compact selection pass"
)]
fn keep_consumer_config_status_at(
    network: &GridNetwork,
    desired: &[ConsumerConfigStatus],
) -> Vec<ConsumerConfigStatus> {
    let prior = network
        .status
        .as_ref()
        .map_or(&[][..], |status| status.consumer_config_status.as_slice());
    let mut kept = Vec::new();
    for gw_ref in &network.spec.gateway_refs {
        let Some(cc) = gw_ref.consumer_config.as_ref() else {
            continue;
        };
        if !cc.enabled {
            kept.push(consumer_config_status_disabled(
                gw_ref,
                cc,
                network.metadata.generation.unwrap_or(0),
            ));
            continue;
        }
        if let Some(status) = desired
            .iter()
            .find(|status| status.gateway_name == gw_ref.name && status.namespace == gw_ref.namespace)
            .or_else(|| {
                prior.iter().find(|status| {
                    status.gateway_name == gw_ref.name
                        && status.namespace == gw_ref.namespace
                        && status.phase != ConsumerConfigPhase::Disabled
                })
            })
        {
            kept.push(status.clone());
        }
    }
    kept
}

/// Keep the last requirements/readiness evidence while an empty overlay
/// intentionally retains the corresponding last working consumer config.
fn keep_mount_reconciliation_status_at(
    network: &GridNetwork,
    desired: &[MountReconciliationStatus],
) -> Vec<MountReconciliationStatus> {
    let prior = network
        .status
        .as_ref()
        .map_or(&[][..], |status| status.mount_reconciliation_status.as_slice());
    let mut kept = Vec::new();
    for gw_ref in &network.spec.gateway_refs {
        let Some(cc) = gw_ref.consumer_config.as_ref().filter(|cc| cc.enabled) else {
            continue;
        };
        let expected_deployment = cc
            .mount_reconciliation
            .as_ref()
            .filter(|mounts| mounts.enabled)
            .and_then(|mounts| mounts.deployment_name.as_deref());
        if let Some(status) = desired
            .iter()
            .find(|status| status.gateway_name == gw_ref.name && status.namespace == gw_ref.namespace)
            .or_else(|| {
                prior.iter().find(|status| {
                    status.gateway_name == gw_ref.name
                        && status.namespace == gw_ref.namespace
                        && status.deployment_name.as_deref() == expected_deployment
                })
            })
        {
            kept.push(status.clone());
        }
    }
    kept
}

/// Whether two entries differ at most in `rendered_at` and `observed_generation`.
fn same_rendered_content(prior: &OverlayRevisionStatus, desired: &OverlayRevisionStatus) -> bool {
    let normalized = OverlayRevisionStatus {
        rendered_at: desired.rendered_at.clone(),
        observed_generation: desired.observed_generation,
        ..prior.clone()
    };
    normalized == *desired
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

/// List all [`AgentToolProvider`] resources cluster-wide.
async fn list_all_agent_tool_providers(client: &Client) -> Result<Vec<AgentToolProvider>, OperatorError> {
    let api: Api<AgentToolProvider> = Api::all(client.clone());
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

/// Outcome of rendering and optionally reconciling a consumer gateway.
struct ConsumerApplyOutcome {
    /// Whether the generated Praxis `ConfigMap` was applied in this pass.
    config_applied: bool,
    /// Mount lifecycle status when explicit Deployment delegation is enabled.
    mount_status: Option<MountReconciliationStatus>,
}

/// Explicit opt-in annotation required on a delegated Deployment.
const MOUNT_OPT_IN_ANNOTATION: &str = "grid.praxis-proxy.io/mount-reconciliation";
/// `GridNetwork` identity annotation required on a delegated Deployment.
const MOUNT_NETWORK_ANNOTATION: &str = "grid.praxis-proxy.io/network";
/// `GatewayRef` identity annotation required on a delegated Deployment.
const MOUNT_GATEWAY_ANNOTATION: &str = "grid.praxis-proxy.io/gateway";
/// Chart marker for the Grid-serving TLS projection that remains Helm-owned.
const GRID_SERVING_TLS_ANNOTATION: &str = "grid.praxis-proxy.io/grid-serving-tls";
/// Fixed path read by the Grid-serving peer pollers.
const GRID_SERVING_TLS_MOUNT_PATH: &str = "/etc/praxis/tls";
/// Annotation recording the volume names currently owned by Grid.
const OWNED_MOUNTS_ANNOTATION: &str = "grid.praxis-proxy.io/owned-mounts";
/// Pod-template annotation recording the desired mount-requirement revision.
const MOUNT_REVISION_ANNOTATION: &str = "grid.praxis-proxy.io/mount-revision";
/// Pod-template annotation recording the rendered Praxis config revision.
const CONFIG_REVISION_ANNOTATION: &str = "grid.praxis-proxy.io/config-revision";
/// Pod-template annotation recording the hashed Secret revision.
const SECRET_REVISION_ANNOTATION: &str = "grid.praxis-proxy.io/secret-revision";
/// Deployment annotation recording Secret resource versions, without Secret data.
const SECRET_RESOURCE_VERSIONS_ANNOTATION: &str = "grid.praxis-proxy.io/secret-resource-versions";
/// Reserved prefix for projected volume names owned by Grid.
const GRID_MOUNT_PREFIX: &str = "grid-mount-";

/// A second, bounded config slot lets old and new Pod revisions retain their matching files.
fn alternate_consumer_config_map_name(base: &str) -> String {
    let digest = gateway_mounts::config_revision(base);
    format!("grid-consumer-config-{}", digest.chars().take(16).collect::<String>())
}

/// Reuse only a config slot that the completed Deployment rollout no longer mounts.
fn inactive_consumer_config_map_name(base: &str, active: &str) -> String {
    if active == base {
        alternate_consumer_config_map_name(base)
    } else {
        base.to_owned()
    }
}

/// Render Praxis config and requirements, optionally reconciling a delegated gateway.
#[expect(
    clippy::too_many_arguments,
    reason = "the exact GridNetwork/gateway inputs define this immutable render"
)]
#[expect(
    clippy::too_many_lines,
    reason = "render, publish requirements, and apply config as one gateway operation"
)]
#[expect(
    clippy::large_stack_frames,
    reason = "render result crosses the optional delegated async boundary"
)]
async fn apply_consumer_config_for_gateway(
    overlay: &routing_overlay::RoutingOverlay,
    network_name: &str,
    gw_ref: &GatewayRef,
    cc: &ConsumerConfig,
    tls: &crate::crd::grid_network::TlsConfig,
    observed_generation: i64,
    client: &Client,
) -> Result<ConsumerApplyOutcome, OperatorError> {
    let rendered = consumer_config::render_consumer_config_with_projected(
        overlay,
        &cc.credential_mount_base,
        &cc.cluster_endpoints,
        &cc.tls_cert_mount_path,
        cc.listener_port,
        tls,
        &gw_ref.name,
        &gw_ref.namespace,
        cc.telemetry.as_ref(),
        cc.enable_projected_credentials,
        cc.mount_reconciliation.as_ref().is_some_and(|mounts| mounts.enabled),
    )?;
    if let Some(mounts) = cc.mount_reconciliation.as_ref().filter(|mounts| mounts.enabled) {
        let mut attempts = 0;
        let (status, config_applied) = loop {
            let result = Box::pin(reconcile_delegated_gateway(
                &rendered,
                network_name,
                gw_ref,
                cc,
                mounts,
                tls,
                observed_generation,
                client,
            ))
            .await;
            if result.as_ref().is_err_and(OperatorError::is_conflict) && attempts < 2 {
                attempts += 1;
                continue;
            }
            break result?;
        };
        return Ok(ConsumerApplyOutcome {
            config_applied,
            mount_status: Some(status),
        });
    }

    let requirements = mount_requirements_document(&rendered, network_name, gw_ref);
    let requirements_revision = gateway_mounts::requirements_revision(&requirements)?;
    apply_mount_requirements_document(&requirements, network_name, gw_ref, cc, client).await?;
    apply_consumer_config_map(&rendered.config_yaml, network_name, gw_ref, cc, client).await?;
    Ok(ConsumerApplyOutcome {
        config_applied: true,
        mount_status: Some(requirements_rendered_status(
            gw_ref,
            requirements_revision,
            observed_generation,
        )),
    })
}

/// Assemble the identity and reference-only input for projected mounts.
fn mount_requirements_document(
    rendered: &consumer_config::ConsumerRenderResult,
    network_name: &str,
    gw_ref: &GatewayRef,
) -> consumer_config::MountRequirementsDocument {
    consumer_config::MountRequirementsDocument {
        schema_version: "v1".to_owned(),
        network: network_name.to_owned(),
        gateway: consumer_config::RequirementGateway {
            name: gw_ref.name.clone(),
            namespace: gw_ref.namespace.clone(),
        },
        requirements: rendered.requirements.clone(),
    }
}

/// Add the chart-owned Grid-serving TLS projection to delegated requirements.
///
/// The chart keeps this projection when `gridServing` is enabled. Recording its
/// references still validates the files and Secret revisions, while the
/// `GridServingTls` purpose prevents the operator from claiming the chart mount.
#[expect(
    clippy::too_many_arguments,
    reason = "the helper combines one render with its exact owner configuration"
)]
#[expect(
    clippy::too_many_lines,
    reason = "all delegated requirements must be canonicalized as one document"
)]
fn delegated_mount_requirements_document(
    rendered: &consumer_config::ConsumerRenderResult,
    network_name: &str,
    gw_ref: &GatewayRef,
    cc: &ConsumerConfig,
    tls: &crate::crd::grid_network::TlsConfig,
    chart_managed_serving_tls: bool,
) -> Result<consumer_config::MountRequirementsDocument, OperatorError> {
    let mut document = mount_requirements_document(rendered, network_name, gw_ref);
    if !chart_managed_serving_tls {
        return Ok(document);
    }
    if cc.tls_cert_mount_path != GRID_SERVING_TLS_MOUNT_PATH {
        return Err(mount_failure(
            "GridServingTlsPathMismatch",
            format!("gridServing requires consumerConfig.tlsCertMountPath={GRID_SERVING_TLS_MOUNT_PATH:?}"),
        )
        .into());
    }
    let ca_ref = tls.ca_secret_ref.as_ref().ok_or_else(|| {
        mount_failure(
            "GridServingTlsReferenceMissing",
            "Grid serving requires tls.caSecretRef",
        )
    })?;
    let site_ref = tls.site_secret_ref.as_ref().ok_or_else(|| {
        mount_failure(
            "GridServingTlsReferenceMissing",
            "Grid serving requires tls.siteSecretRef",
        )
    })?;

    for requirement in &mut document.requirements {
        if matches!(
            requirement.purpose,
            consumer_config::MountPurpose::GridPeerCa | consumer_config::MountPurpose::GridSiteIdentity
        ) {
            requirement.purpose = consumer_config::MountPurpose::GridServingTls;
        }
    }
    add_chart_serving_tls_item(&mut document, gw_ref, &ca_ref.namespace, &ca_ref.name, "ca.crt");
    for key in ["tls.crt", "tls.key"] {
        add_chart_serving_tls_item(&mut document, gw_ref, &site_ref.namespace, &site_ref.name, key);
    }
    for requirement in &mut document.requirements {
        requirement.items.sort();
        requirement.items.dedup();
    }
    document.requirements.sort_by(|left, right| {
        (
            &left.purpose,
            &left.final_hop,
            &left.secret.namespace,
            &left.secret.name,
        )
            .cmp(&(
                &right.purpose,
                &right.final_hop,
                &right.secret.namespace,
                &right.secret.name,
            ))
    });
    Ok(document)
}

/// Merge one chart-owned serving TLS key into the stable requirements list.
fn add_chart_serving_tls_item(
    document: &mut consumer_config::MountRequirementsDocument,
    gw_ref: &GatewayRef,
    namespace: &str,
    name: &str,
    key: &str,
) {
    let item = consumer_config::RequirementItem {
        key: key.to_owned(),
        path: format!("{GRID_SERVING_TLS_MOUNT_PATH}/{key}"),
    };
    if let Some(requirement) = document.requirements.iter_mut().find(|requirement| {
        requirement.purpose == consumer_config::MountPurpose::GridServingTls
            && requirement.secret.namespace == namespace
            && requirement.secret.name == name
    }) {
        if !requirement.items.contains(&item) {
            requirement.items.push(item);
        }
        return;
    }
    document.requirements.push(consumer_config::MountRequirement {
        purpose: consumer_config::MountPurpose::GridServingTls,
        final_hop: gw_ref.name.clone(),
        secret: consumer_config::RequirementSecret {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        },
        items: vec![item],
    });
}

/// Confirm that the chart's read-only TLS mount supplies each serving file.
#[expect(
    clippy::too_many_lines,
    reason = "validate every external TLS file against its mounted Secret source"
)]
fn validate_chart_managed_serving_tls(
    requirements: &consumer_config::MountRequirementsDocument,
    deployment: &Deployment,
    delegation: &MountReconciliation,
) -> Result<(), OperatorError> {
    let template = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .ok_or_else(|| mount_failure("DeploymentInvalid", "the delegated Deployment has no pod spec"))?;
    let container = template
        .containers
        .iter()
        .find(|container| container.name == delegation.container_name)
        .ok_or_else(|| mount_failure("ContainerMissing", "the named Praxis container is absent"))?;
    let volumes = template.volumes.as_deref().unwrap_or_default();
    let mounts = container.volume_mounts.as_deref().unwrap_or_default();

    for requirement in requirements
        .requirements
        .iter()
        .filter(|requirement| requirement.purpose == consumer_config::MountPurpose::GridServingTls)
    {
        for item in &requirement.items {
            let path = Path::new(&item.path);
            let parent = path.parent().and_then(Path::to_str).unwrap_or_default();
            let relative = path
                .strip_prefix(parent)
                .ok()
                .and_then(Path::to_str)
                .unwrap_or_default();
            let mount = mounts
                .iter()
                .find(|mount| mount.mount_path == parent && mount.read_only == Some(true));
            let supplied = mount
                .and_then(|mount| volumes.iter().find(|volume| volume.name == mount.name))
                .and_then(|volume| serde_json::to_value(volume).ok())
                .is_some_and(|volume| {
                    volume_projects_secret_key(&volume, &requirement.secret.name, &item.key, relative)
                });
            if parent != GRID_SERVING_TLS_MOUNT_PATH || !supplied {
                return Err(mount_failure(
                    "ChartTlsMountMissing",
                    format!(
                        "the chart-managed TLS mount does not provide {}/{} at {:?}",
                        requirement.secret.name, item.key, item.path
                    ),
                )
                .into());
            }
        }
    }
    Ok(())
}

/// Check a serialized Secret or projected volume for one key-to-file mapping.
fn volume_projects_secret_key(volume: &Value, secret_name: &str, key: &str, path: &str) -> bool {
    let matches_source = |source: &Value, name_field: &str| {
        if source.get(name_field).and_then(Value::as_str) != Some(secret_name) {
            return false;
        }
        let Some(items) = source.get("items").and_then(Value::as_array) else {
            return path == key;
        };
        items.iter().any(|item| {
            item.get("key").and_then(Value::as_str) == Some(key)
                && item.get("path").and_then(Value::as_str) == Some(path)
        })
    };

    if volume
        .get("secret")
        .is_some_and(|source| matches_source(source, "secretName"))
    {
        return true;
    }
    volume
        .pointer("/projected/sources")
        .and_then(Value::as_array)
        .is_some_and(|sources| {
            sources
                .iter()
                .filter_map(|source| source.get("secret"))
                .any(|source| matches_source(source, "name"))
        })
}

/// Publish the reference-only requirements document as a `ConfigMap`.
async fn apply_mount_requirements_document(
    requirements: &consumer_config::MountRequirementsDocument,
    network_name: &str,
    gw_ref: &GatewayRef,
    cc: &ConsumerConfig,
    client: &Client,
) -> Result<(), OperatorError> {
    let requirements_config_map_name = gateway_mounts::requirements_config_map_name(&cc.config_map_name);
    let config_map = consumer_config::build_mount_requirements_config_map(
        requirements,
        &requirements_config_map_name,
        &gw_ref.namespace,
        network_name,
        &gw_ref.name,
    )?;
    let config_maps: Api<ConfigMap> = Api::namespaced(client.clone(), &gw_ref.namespace);
    config_maps
        .patch(
            &requirements_config_map_name,
            &PatchParams::apply(FIELD_MANAGER),
            &Patch::Apply(&config_map),
        )
        .await
        .map_err(|_error| {
            mount_failure(
                "RequirementsApplyFailed",
                "could not apply the reference-only requirements ConfigMap",
            )
        })?;
    Ok(())
}

/// Build status for requirements published without Deployment delegation.
fn requirements_rendered_status(
    gw_ref: &GatewayRef,
    requirements_revision: String,
    observed_generation: i64,
) -> MountReconciliationStatus {
    MountReconciliationStatus {
        gateway_name: gw_ref.name.clone(),
        namespace: gw_ref.namespace.clone(),
        deployment_name: None,
        phase: MountReconciliationPhase::RequirementsRendered,
        requirements_revision,
        applied_revision: String::new(),
        reason: String::new(),
        message: "reference-only mount requirements were published; Deployment remains owner-managed".to_owned(),
        observed_generation,
        deployment_generation: 0,
    }
}

/// Apply the operator-owned Praxis config `ConfigMap` after mounts are staged.
async fn apply_consumer_config_map(
    config_yaml: &str,
    network_name: &str,
    gw_ref: &GatewayRef,
    cc: &ConsumerConfig,
    client: &Client,
) -> Result<(), OperatorError> {
    apply_consumer_config_map_named(config_yaml, network_name, gw_ref, &cc.config_map_name, client).await
}

/// Publish a config into a selected slot before switching the delegated Pod template to it.
async fn apply_consumer_config_map_named(
    config_yaml: &str,
    network_name: &str,
    gw_ref: &GatewayRef,
    config_map_name: &str,
    client: &Client,
) -> Result<(), OperatorError> {
    let cm = consumer_config::build_consumer_config_map(
        config_yaml,
        config_map_name,
        &gw_ref.namespace,
        network_name,
        &gw_ref.name,
    );
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), &gw_ref.namespace);
    if Box::pin(config_map_current(&api, config_map_name, &cm)).await? {
        return Ok(());
    }
    api.patch(
        config_map_name,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(&cm),
    )
    .await?;
    info!(
        config_map = %config_map_name,
        namespace = %gw_ref.namespace,
        "applied consumer Praxis config ConfigMap"
    );
    Ok(())
}

/// Whether `name` already holds `desired`'s data, so applying it would change nothing.
async fn config_map_current(api: &Api<ConfigMap>, name: &str, desired: &ConfigMap) -> Result<bool, OperatorError> {
    let current = api
        .get_opt(name)
        .await?
        .is_some_and(|current| current.data == desired.data);
    if current {
        tracing::debug!(config_map = %name, "consumer Praxis config unchanged");
    }
    Ok(current)
}

/// Keep a strategic patch tied to the Deployment version used to compute mount ownership.
fn guarded_deployment_patch(mut patch: Value, deployment: &Deployment) -> Result<Value, OperatorError> {
    let version = deployment
        .metadata
        .resource_version
        .as_deref()
        .ok_or_else(|| mount_failure("DeploymentInvalid", "the delegated Deployment has no resourceVersion"))?;
    let metadata = patch
        .get_mut("metadata")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| mount_failure("DeploymentInvalid", "the Deployment patch has no metadata"))?;
    metadata.insert("resourceVersion".to_owned(), json!(version));
    Ok(patch)
}

/// Preserve conflict errors so the caller can retry from a fresh Deployment read.
async fn patch_delegated_deployment(
    deployments: &Api<Deployment>,
    name: &str,
    deployment: &Deployment,
    patch: Value,
    failure_message: &'static str,
) -> Result<(), OperatorError> {
    let patch = guarded_deployment_patch(patch, deployment)?;
    match deployments
        .patch(name, &PatchParams::default(), &Patch::Strategic(patch))
        .await
    {
        Ok(_) => Ok(()),
        Err(error) if matches!(&error, kube::Error::Api(status) if status.code == 409) => Err(error.into()),
        Err(_) => Err(mount_failure("DeploymentPatchFailed", failure_message).into()),
    }
}

/// Switch config source, Secret projections, and their revisions in one Pod-template update.
#[expect(
    clippy::too_many_arguments,
    reason = "the staged patch keeps config, mounts, and revisions in one write"
)]
fn staged_config_mount_patch(
    mut volume_mutations: Vec<Value>,
    mount_additions: &[Value],
    config_volume_name: &str,
    next_config_name: &str,
    container_name: &str,
    deployment_annotations: &Value,
    template_annotations: &Value,
) -> Value {
    volume_mutations.push(json!({"name": config_volume_name, "configMap": {"name": next_config_name}}));
    let mut pod_spec_patch = serde_json::Map::new();
    pod_spec_patch.insert("volumes".to_owned(), Value::Array(volume_mutations));
    if !mount_additions.is_empty() {
        pod_spec_patch.insert(
            "containers".to_owned(),
            json!([{"name": container_name, "volumeMounts": mount_additions}]),
        );
    }
    json!({
        "metadata": {"annotations": deployment_annotations},
        "spec": {"template": {
            "metadata": {"annotations": template_annotations},
            "spec": pod_spec_patch
        }}
    })
}

/// Validate and reconcile one explicitly delegated gateway Deployment.
#[expect(
    clippy::too_many_lines,
    reason = "the ordered mount, config, rollout, and prune steps are one lifecycle state machine"
)]
#[expect(
    clippy::large_stack_frames,
    reason = "Kubernetes Deployment state and strategic patch payloads cross async API calls"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "the lifecycle uses exact render, ownership, rollout, and API inputs"
)]
async fn reconcile_delegated_gateway(
    rendered: &consumer_config::ConsumerRenderResult,
    network_name: &str,
    gw_ref: &GatewayRef,
    cc: &ConsumerConfig,
    delegation: &MountReconciliation,
    tls: &crate::crd::grid_network::TlsConfig,
    observed_generation: i64,
    client: &Client,
) -> Result<(MountReconciliationStatus, bool), OperatorError> {
    let Some(deployment_name) = delegation
        .deployment_name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
    else {
        return Err(mount_failure(
            "InvalidDelegation",
            "deploymentName is required when mount reconciliation is enabled",
        )
        .into());
    };
    if delegation.container_name.trim().is_empty() {
        return Err(mount_failure(
            "InvalidDelegation",
            "containerName is required when mount reconciliation is enabled",
        )
        .into());
    }
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), &gw_ref.namespace);
    let deployment = deployments
        .get_opt(deployment_name)
        .await
        .map_err(|error| {
            mount_failure(
                "DeploymentReadFailed",
                format!("could not read the delegated Deployment: {error}"),
            )
        })?
        .ok_or_else(|| mount_failure("DeploymentMissing", "the delegated Deployment does not exist"))?;
    let chart_managed_serving_tls =
        validate_deployment_delegation(&deployment, network_name, gw_ref, delegation, &cc.config_map_name)?;
    let requirements =
        delegated_mount_requirements_document(rendered, network_name, gw_ref, cc, tls, chart_managed_serving_tls)?;
    let requirements_revision = gateway_mounts::requirements_revision(&requirements)?;
    let resource_versions = validate_required_secrets(&requirements, &gw_ref.namespace, client).await?;
    if chart_managed_serving_tls {
        validate_chart_managed_serving_tls(&requirements, &deployment, delegation)?;
    }
    apply_mount_requirements_document(&requirements, network_name, gw_ref, cc, client).await?;
    let secret_revision = gateway_mounts::secret_revision(&resource_versions)?;
    let desired_mounts = gateway_mounts::desired_mounts(&requirements)?;
    let pod_template = deployment
        .spec
        .as_ref()
        .map(|spec| &spec.template)
        .ok_or_else(|| mount_failure("DeploymentInvalid", "the delegated Deployment has no pod template"))?;
    let template = pod_template
        .spec
        .as_ref()
        .ok_or_else(|| mount_failure("DeploymentInvalid", "the delegated Deployment has no pod spec"))?;
    let target_container = template
        .containers
        .iter()
        .find(|container| container.name == delegation.container_name)
        .ok_or_else(|| mount_failure("ContainerMissing", "the named Praxis container is absent"))?;
    let volumes = template.volumes.as_deref().unwrap_or_default();
    let target_mounts = target_container.volume_mounts.as_deref().unwrap_or_default();
    let config_volume_name = target_mounts
        .iter()
        .find(|mount| mount.mount_path == "/etc/praxis")
        .map(|mount| mount.name.as_str())
        .ok_or_else(|| mount_failure("ConfigSourceMismatch", "the Praxis config mount is absent"))?;
    let active_config_name = volumes
        .iter()
        .find(|volume| volume.name == config_volume_name)
        .and_then(|volume| volume.config_map.as_ref())
        .map(|source| source.name.as_str())
        .ok_or_else(|| mount_failure("ConfigSourceMismatch", "the Praxis config source is absent"))?;
    let mut owned_mounts = read_owned_mounts(&deployment)?;
    let expected_names: BTreeSet<String> = desired_mounts.iter().map(|mount| mount.volume_name.clone()).collect();
    let mut additions = Vec::new();
    let mut mount_additions = Vec::new();
    for desired in &desired_mounts {
        let desired_volume = &desired.volume;
        let desired_mount = &desired.volume_mount;
        let current_volume = volumes.iter().find(|volume| volume.name == desired.volume_name);
        let current_mount = target_mounts
            .iter()
            .find(|mount| mount.mount_path == desired.mount_path);
        if current_volume.is_some() && !owned_mounts.contains(&desired.volume_name) {
            return Err(mount_failure(
                "OwnershipConflict",
                format!(
                    "reserved volume name {:?} is already used outside Grid ownership",
                    desired.volume_name
                ),
            )
            .into());
        }
        if current_mount.is_some() && !owned_mounts.contains(&desired.volume_name) {
            return Err(mount_failure(
                "OwnershipConflict",
                format!(
                    "mount path {:?} is already used outside Grid ownership",
                    desired.mount_path
                ),
            )
            .into());
        }
        if let Some(volume) = current_volume {
            if serde_json::to_value(volume)? != *desired_volume {
                // The owned volume name is derived from its mount path. When a
                // Secret reference changes at that same path (for example the
                // Grid CA reference), replace only the Grid-owned volume entry.
                additions.extend(owned_volume_mutation_patch(
                    template,
                    &delegation.container_name,
                    &desired.volume_name,
                    Some(desired_volume),
                )?);
            }
        } else {
            additions.push(desired_volume.clone());
        }
        if let Some(volume_mount) = current_mount {
            if serde_json::to_value(volume_mount)? != *desired_mount {
                return Err(mount_failure(
                    "OwnershipConflict",
                    format!("Grid-owned mount path {:?} has unexpected settings", desired.mount_path),
                )
                .into());
            }
        } else {
            if target_mounts.iter().any(|mount| mount.name == desired.volume_name) {
                return Err(mount_failure(
                    "OwnershipConflict",
                    format!(
                        "reserved volume name {:?} is mounted at another path",
                        desired.volume_name
                    ),
                )
                .into());
            }
            mount_additions.push(desired_mount.clone());
        }
        owned_mounts.insert(desired.volume_name.clone());
    }
    if volumes
        .iter()
        .any(|volume| volume.name.starts_with(GRID_MOUNT_PREFIX) && !owned_mounts.contains(&volume.name))
    {
        return Err(mount_failure(
            "OwnershipConflict",
            "a reserved Grid mount volume is present without Grid ownership state",
        )
        .into());
    }

    let template_annotations = pod_template
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.annotations.as_ref())
        .cloned()
        .unwrap_or_default();
    let has_mount_revision = template_annotations
        .get(MOUNT_REVISION_ANNOTATION)
        .is_some_and(|revision| revision == &requirements_revision);
    let config_revision = gateway_mounts::config_revision(&rendered.config_yaml);
    let resource_versions_json = serde_json::to_string(&resource_versions)?;
    let owned_mounts_json = serde_json::to_string(&owned_mounts.iter().collect::<Vec<_>>())?;
    let current_owned_mounts_json = deployment
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(OWNED_MOUNTS_ANNOTATION));
    let active_cm = consumer_config::build_consumer_config_map(
        &rendered.config_yaml,
        active_config_name,
        &gw_ref.namespace,
        network_name,
        &gw_ref.name,
    );
    let config_api: Api<ConfigMap> = Api::namespaced(client.clone(), &gw_ref.namespace);
    let active_config_current = Box::pin(config_map_current(&config_api, active_config_name, &active_cm)).await?;
    let needs_revision_rollout = !additions.is_empty()
        || !mount_additions.is_empty()
        || !has_mount_revision
        || current_owned_mounts_json != Some(&owned_mounts_json)
        || template_annotations.get(CONFIG_REVISION_ANNOTATION) != Some(&config_revision)
        || template_annotations.get(SECRET_REVISION_ANNOTATION) != Some(&secret_revision)
        || deployment
            .metadata
            .annotations
            .as_ref()
            .and_then(|values| values.get(SECRET_RESOURCE_VERSIONS_ANNOTATION))
            != Some(&resource_versions_json)
        || !active_config_current;
    if !deployment_rollout_ready(&deployment) {
        return Ok((
            mount_status(
                gw_ref,
                Some(delegation),
                MountReconciliationPhase::WaitingForRollout,
                &requirements_revision,
                "",
                "waiting for the previous Deployment revision to finish before changing config and mounts",
                "",
                observed_generation,
                deployment.metadata.generation.unwrap_or(0),
            ),
            false,
        ));
    }
    if needs_revision_rollout {
        let next_config_name = inactive_consumer_config_map_name(&cc.config_map_name, active_config_name);
        let config_shared = template
            .containers
            .iter()
            .filter(|container| container.name != delegation.container_name)
            .any(|container| {
                container
                    .volume_mounts
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .any(|mount| mount.name == config_volume_name)
            })
            || template
                .init_containers
                .as_deref()
                .unwrap_or_default()
                .iter()
                .any(|container| {
                    container
                        .volume_mounts
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .any(|mount| mount.name == config_volume_name)
                });
        if config_shared {
            return Err(mount_failure(
                "OwnershipConflict",
                "the delegated Praxis config volume is also mounted outside the target container",
            )
            .into());
        }
        apply_consumer_config_map_named(&rendered.config_yaml, network_name, gw_ref, &next_config_name, client).await?;
        let patch = staged_config_mount_patch(
            additions,
            &mount_additions,
            config_volume_name,
            &next_config_name,
            &delegation.container_name,
            &json!({
                OWNED_MOUNTS_ANNOTATION: owned_mounts_json,
                SECRET_RESOURCE_VERSIONS_ANNOTATION: resource_versions_json
            }),
            &json!({
                MOUNT_REVISION_ANNOTATION: requirements_revision,
                CONFIG_REVISION_ANNOTATION: config_revision,
                SECRET_REVISION_ANNOTATION: secret_revision
            }),
        );
        patch_delegated_deployment(
            &deployments,
            deployment_name,
            &deployment,
            patch,
            "could not stage the matching Praxis config and Secret mounts",
        )
        .await?;
        return Ok((
            mount_status(
                gw_ref,
                Some(delegation),
                MountReconciliationPhase::WaitingForRollout,
                &requirements_revision,
                "",
                "the matching Praxis config and Secret mounts were staged; waiting for the Deployment rollout",
                "",
                observed_generation,
                deployment.metadata.generation.unwrap_or(0),
            ),
            true,
        ));
    }

    let stale_names: BTreeSet<String> = owned_mounts.difference(&expected_names).cloned().collect();
    let mut mount_deletions = Vec::new();
    let mut volume_deletions = Vec::new();
    for name in &stale_names {
        for volume_mount in target_mounts.iter().filter(|mount| &mount.name == name) {
            mount_deletions.push(json!({
                "name": volume_mount.name,
                "mountPath": volume_mount.mount_path,
                "$patch": "delete"
            }));
        }
        if volumes.iter().any(|volume| &volume.name == name) {
            volume_deletions.extend(owned_volume_mutation_patch(
                template,
                &delegation.container_name,
                name,
                None,
            )?);
        }
    }
    if !stale_names.is_empty() {
        let owned_json = serde_json::to_string(&expected_names.iter().collect::<Vec<_>>())?;
        let mut pod_spec_patch = serde_json::Map::new();
        if !volume_deletions.is_empty() {
            pod_spec_patch.insert("volumes".to_owned(), Value::Array(volume_deletions));
        }
        if !mount_deletions.is_empty() {
            pod_spec_patch.insert(
                "containers".to_owned(),
                json!([{"name": delegation.container_name, "volumeMounts": mount_deletions}]),
            );
        }
        let patch = json!({
            "metadata": {"annotations": {OWNED_MOUNTS_ANNOTATION: owned_json}},
            "spec": {"template": {"spec": pod_spec_patch}}
        });
        patch_delegated_deployment(
            &deployments,
            deployment_name,
            &deployment,
            patch,
            "could not remove obsolete Grid-owned mounts",
        )
        .await?;
        return Ok((
            mount_status(
                gw_ref,
                Some(delegation),
                MountReconciliationPhase::MountsReconciling,
                &requirements_revision,
                &config_revision,
                "the config rollout is available; obsolete Grid-owned mounts were removed",
                "",
                observed_generation,
                deployment.metadata.generation.unwrap_or(0),
            ),
            true,
        ));
    }

    Ok((
        mount_status(
            gw_ref,
            Some(delegation),
            MountReconciliationPhase::Ready,
            &requirements_revision,
            &config_revision,
            "required Secret files are mounted in available pods with the matching config revision",
            "",
            observed_generation,
            deployment.metadata.generation.unwrap_or(0),
        ),
        true,
    ))
}

/// Build a Grid-owned volume replacement or deletion patch after checking all
/// consumers outside the target container.
#[expect(
    clippy::too_many_lines,
    reason = "keep ownership checks and the matching mutation patch in one policy helper"
)]
fn owned_volume_mutation_patch(
    pod_spec: &k8s_openapi::api::core::v1::PodSpec,
    target_container_name: &str,
    volume_name: &str,
    replacement: Option<&Value>,
) -> Result<Vec<Value>, OperatorError> {
    let mounts_volume = |container: &k8s_openapi::api::core::v1::Container| {
        container
            .volume_mounts
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|mount| mount.name == volume_name)
    };
    let shared_regular = pod_spec
        .containers
        .iter()
        .filter(|container| container.name != target_container_name)
        .any(&mounts_volume);
    let shared_init = pod_spec
        .init_containers
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(mounts_volume);
    if shared_regular || shared_init {
        return Err(mount_failure(
            "OwnershipConflict",
            format!("Grid-owned volume {volume_name:?} is also mounted outside the target container"),
        )
        .into());
    }

    match replacement {
        Some(volume) if volume.is_object() => {
            Ok(vec![json!({"name": volume_name, "$patch": "delete"}), volume.clone()])
        },
        Some(_) => Err(mount_failure("DeploymentInvalid", "desired volume was not an object").into()),
        None => Ok(vec![json!({"name": volume_name, "$patch": "delete"})]),
    }
}

/// Confirm the Deployment's explicit opt-in and selected container identity.
#[expect(
    clippy::too_many_lines,
    reason = "validate explicit owner annotations and selected container together"
)]
fn validate_deployment_delegation(
    deployment: &Deployment,
    network_name: &str,
    gw_ref: &GatewayRef,
    delegation: &MountReconciliation,
    config_map_name: &str,
) -> Result<bool, OperatorError> {
    let annotations = deployment.metadata.annotations.as_ref();
    let owns = annotations.is_some_and(|annotations| {
        annotations
            .get(MOUNT_OPT_IN_ANNOTATION)
            .is_some_and(|value| value == "enabled")
            && annotations
                .get(MOUNT_NETWORK_ANNOTATION)
                .is_some_and(|value| value == network_name)
            && annotations
                .get(MOUNT_GATEWAY_ANNOTATION)
                .is_some_and(|value| value == &gw_ref.name)
    });
    if !owns {
        return Err(mount_failure(
            "OwnershipMismatch",
            "Deployment opt-in annotations do not name this GridNetwork and gatewayRef",
        )
        .into());
    }
    let pod_spec = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .ok_or_else(|| mount_failure("DeploymentInvalid", "the delegated Deployment has no pod spec"))?;
    let container = pod_spec
        .containers
        .iter()
        .find(|container| container.name == delegation.container_name)
        .ok_or_else(|| mount_failure("ContainerMissing", "the named Praxis container is absent"))?;
    validate_deployment_config_source(pod_spec, container, config_map_name)?;
    Ok(annotations.is_some_and(|annotations| {
        annotations
            .get(GRID_SERVING_TLS_ANNOTATION)
            .is_some_and(|value| value == "chart-managed")
    }))
}

/// Verify that the selected container receives Grid's generated praxis.yaml.
#[expect(
    clippy::too_many_lines,
    reason = "mount path, volume source, and key projection form one fail-closed config-source check"
)]
fn validate_deployment_config_source(
    pod_spec: &k8s_openapi::api::core::v1::PodSpec,
    container: &k8s_openapi::api::core::v1::Container,
    config_map_name: &str,
) -> Result<(), OperatorError> {
    let mut config_mounts = container
        .volume_mounts
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|mount| mount.mount_path == "/etc/praxis");
    let config_mount = config_mounts.next().ok_or_else(|| {
        mount_failure(
            "ConfigSourceMismatch",
            "the Praxis container has no /etc/praxis config mount",
        )
    })?;
    if config_mounts.next().is_some()
        || config_mount.sub_path.is_some()
        || config_mount.sub_path_expr.is_some()
        || container
            .volume_mounts
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|mount| mount.mount_path == "/etc/praxis/praxis.yaml")
    {
        return Err(mount_failure(
            "ConfigSourceMismatch",
            "the Praxis config mount is ambiguous or shadowed",
        )
        .into());
    }
    let config_map = pod_spec
        .volumes
        .as_deref()
        .unwrap_or_default()
        .iter()
        .find(|volume| volume.name == config_mount.name)
        .and_then(|volume| volume.config_map.as_ref())
        .ok_or_else(|| {
            mount_failure(
                "ConfigSourceMismatch",
                "the Praxis config mount is not a ConfigMap volume",
            )
        })?;
    let projects_config = config_map.items.as_deref().is_none_or(|items| {
        items.is_empty()
            || items
                .iter()
                .any(|item| item.key == "praxis.yaml" && item.path == "praxis.yaml")
    });
    let valid_name =
        config_map.name == config_map_name || config_map.name == alternate_consumer_config_map_name(config_map_name);
    if !valid_name || config_map.optional == Some(true) || !projects_config {
        return Err(mount_failure(
            "ConfigSourceMismatch",
            "the Praxis config mount does not project the generated praxis.yaml",
        )
        .into());
    }
    Ok(())
}

/// Read only Secret key presence and resource versions for a requirements document.
#[expect(
    clippy::too_many_lines,
    reason = "validate same-namespace Secret references, keys, and versions in one pass"
)]
async fn validate_required_secrets(
    document: &consumer_config::MountRequirementsDocument,
    gateway_namespace: &str,
    client: &Client,
) -> Result<BTreeMap<String, String>, OperatorError> {
    let mut required = BTreeMap::<(String, String), BTreeSet<String>>::new();
    for requirement in &document.requirements {
        if requirement.secret.namespace != gateway_namespace {
            return Err(mount_failure(
                "SecretNamespaceMismatch",
                format!(
                    "Secret {}/{} is outside gateway namespace {gateway_namespace:?}",
                    requirement.secret.namespace, requirement.secret.name
                ),
            )
            .into());
        }
        for item in &requirement.items {
            required
                .entry((requirement.secret.namespace.clone(), requirement.secret.name.clone()))
                .or_default()
                .insert(item.key.clone());
        }
    }
    let mut resource_versions = BTreeMap::new();
    for ((namespace, name), keys) in required {
        let api: Api<Secret> = Api::namespaced(client.clone(), &namespace);
        let secret = api
            .get_opt(&name)
            .await
            .map_err(|error| {
                mount_failure(
                    "SecretReadFailed",
                    format!("could not read Secret {namespace}/{name}: {error}"),
                )
            })?
            .ok_or_else(|| mount_failure("MissingSecret", format!("Secret {namespace}/{name} does not exist")))?;
        for key in keys {
            if secret
                .data
                .as_ref()
                .and_then(|data| data.get(&key))
                .is_none_or(|bytes| bytes.0.is_empty())
            {
                return Err(mount_failure(
                    "MissingSecretKey",
                    format!("Secret {namespace}/{name} has no nonempty key {key:?}"),
                )
                .into());
            }
        }
        let resource_version = secret.metadata.resource_version.ok_or_else(|| {
            mount_failure(
                "SecretReadFailed",
                format!("Secret {namespace}/{name} has no resourceVersion"),
            )
        })?;
        resource_versions.insert(format!("{namespace}/{name}"), resource_version);
    }
    Ok(resource_versions)
}

/// Parse the exact set of Grid-owned volumes persisted on the Deployment.
fn read_owned_mounts(deployment: &Deployment) -> Result<BTreeSet<String>, OperatorError> {
    let Some(encoded) = deployment
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(OWNED_MOUNTS_ANNOTATION))
    else {
        return Ok(BTreeSet::new());
    };
    let names: Vec<String> = serde_json::from_str(encoded).map_err(|_error| {
        mount_failure(
            "OwnershipConflict",
            "Grid-owned mount state annotation is not valid JSON",
        )
    })?;
    if names.iter().any(|name| !name.starts_with(GRID_MOUNT_PREFIX)) {
        return Err(mount_failure(
            "OwnershipConflict",
            "Grid-owned mount state contains an unreserved volume name",
        )
        .into());
    }
    Ok(names.into_iter().collect())
}

/// Check whether the current Deployment generation has fully replaced old replicas.
fn deployment_rollout_ready(deployment: &Deployment) -> bool {
    let replicas = deployment.spec.as_ref().and_then(|spec| spec.replicas).unwrap_or(1);
    let generation = deployment.metadata.generation.unwrap_or(0);
    deployment.status.as_ref().is_some_and(|status| {
        replicas > 0
            && status.observed_generation.unwrap_or(0) >= generation
            && status.updated_replicas.unwrap_or(0) == replicas
            && status.available_replicas.unwrap_or(0) >= replicas
            && status.replicas.unwrap_or(0) == replicas
            && status.unavailable_replicas.unwrap_or(0) == 0
    })
}

/// Build one per-gateway mount status with no Secret payload data.
#[expect(
    clippy::too_many_arguments,
    reason = "status fields are assembled explicitly to keep lifecycle facts visible"
)]
fn mount_status(
    gw_ref: &GatewayRef,
    delegation: Option<&MountReconciliation>,
    phase: MountReconciliationPhase,
    requirements_revision: &str,
    applied_revision: &str,
    message: &str,
    reason: &str,
    observed_generation: i64,
    deployment_generation: i64,
) -> MountReconciliationStatus {
    MountReconciliationStatus {
        gateway_name: gw_ref.name.clone(),
        namespace: gw_ref.namespace.clone(),
        deployment_name: delegation.and_then(|mounts| mounts.deployment_name.clone()),
        phase,
        requirements_revision: requirements_revision.to_owned(),
        applied_revision: applied_revision.to_owned(),
        reason: reason.to_owned(),
        message: message.to_owned(),
        observed_generation,
        deployment_generation,
    }
}

/// Create a mount error whose message is safe to surface in status and logs.
fn mount_failure(reason: &'static str, message: impl Into<String>) -> crate::error::GatewayMountFailure {
    crate::error::GatewayMountFailure::new(reason, message)
}

/// Build a sanitized status entry after mount reconciliation failed.
#[expect(
    clippy::too_many_lines,
    reason = "maps stable failure categories to the public mount lifecycle status"
)]
fn mount_reconciliation_status_error(
    gw_ref: &GatewayRef,
    delegation: Option<&MountReconciliation>,
    err: &OperatorError,
    observed_generation: i64,
) -> MountReconciliationStatus {
    let (reason, message) = match err {
        OperatorError::MountReconciliation(failure) => (failure.reason, failure.message.clone()),
        OperatorError::ConsumerConfigRender(ConsumerConfigError::MountPathConflict { .. }) => {
            ("MountPathConflict", err.to_string())
        },
        OperatorError::ConsumerConfigRender(ConsumerConfigError::InvalidMountPath { .. }) => {
            ("InvalidMountPath", err.to_string())
        },
        OperatorError::Certificate(_)
        | OperatorError::Kube(_)
        | OperatorError::Json(_)
        | OperatorError::NotFound(_)
        | OperatorError::OverlayRender(_)
        | OperatorError::ConsumerConfigRender(_)
        | OperatorError::SwimKeyConfig(_)
        | OperatorError::InvalidResource(_) => (
            "MountReconciliationFailed",
            "gateway mount reconciliation failed".to_owned(),
        ),
    };
    let phase = if matches!(reason, "MissingSecret" | "MissingSecretKey") {
        MountReconciliationPhase::WaitingForSecret
    } else {
        MountReconciliationPhase::Error
    };
    mount_status(
        gw_ref,
        delegation,
        phase,
        "",
        "",
        &message,
        reason,
        observed_generation,
        0,
    )
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
fn crdt_phase_from_provider(status: Option<&InferenceProviderStatus>, generation: i64) -> crdt::ProviderPhase {
    use crate::crd::inference_provider::ProviderPhase as Op;
    if status.is_some_and(|status| {
        status.phase == Op::Pending
            && status.reason.as_deref() == Some("NoMatchingSites")
            && status.observed_generation == generation
    }) {
        // The provider controller has observed this generation and confirmed
        // that its selector matches no known site. Preserve that withdrawal in
        // the CRDT instead of publishing Pending as routable capacity.
        return crdt::ProviderPhase::Unavailable;
    }
    match status.map(|status| &status.phase) {
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

/// The capacity weight a provider publishes: its declared weight when valid, else the minimum.
fn effective_capacity_weight(provider: &InferenceProvider) -> u32 {
    provider
        .spec
        .capacity_weight
        .filter(|weight| crdt::is_valid_capacity_weight(*weight))
        .unwrap_or(crdt::MIN_CAPACITY_WEIGHT)
}

/// Log at INFO each of `network`'s providers whose published capacity is new or changed.
fn log_capacity_changes(logged: &ChangeLog, network: &str, providers: &[InferenceProvider]) {
    let prefix = format!("capacity/{network}/");
    let mut current = std::collections::HashSet::new();
    for provider in providers.iter().filter(|p| p.spec.grid_network_ref == network) {
        let Some(provider_id) = provider.metadata.name.as_deref() else {
            continue;
        };
        let weight = effective_capacity_weight(provider);
        let key = format!("{prefix}{provider_id}");
        let changed = logged.changed(&key, weight.to_string());
        current.insert(key);
        if changed {
            info!(
                network,
                provider_id,
                capacity_weight = weight,
                "published local provider CRDT capacity"
            );
        }
    }
    logged.retain_under(&prefix, &current);
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
    let phase = crdt_phase_from_provider(
        provider.status.as_ref(),
        provider.metadata.generation.unwrap_or_default(),
    );
    let revision = provider_revision(&provider.metadata);
    let capacity_weight = effective_capacity_weight(provider);
    Some(crdt::ProviderState {
        network_id: network_id.to_owned(),
        site_id: site_id.to_owned(),
        provider_id: provider_id.to_owned(),
        routing_cluster,
        models,
        tools: Vec::new(),
        backend_kind: provider.spec.backend_kind.clone(),
        capacity_weight,
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
///
/// Accepts `&ObjectMeta` so it works for both `InferenceProvider` and
/// `AgentToolProvider` resources.
fn provider_revision(meta: &kube::api::ObjectMeta) -> u64 {
    meta.resource_version
        .as_deref()
        .and_then(|rv| rv.parse::<u64>().ok())
        .or_else(|| meta.generation.and_then(|g| u64::try_from(g).ok()))
        .unwrap_or(0)
}

/// Convert an [`AgentToolProvider`] into a [`crdt::ProviderState`] for SWIM broadcast.
///
/// Returns `None` when the provider lacks a `metadata.name` (shouldn't happen
/// for server-generated resources).
///
/// Tool providers carry no models and no metrics.  `backend_kind` is empty
/// because `AgentToolProvider` has no `spec.backendKind` field.
/// The `tools` field is populated only from a current, available
/// `status.discoveredTools` result. `spec.tools` is an allowlist, not a source
/// of unverified routing data.
fn tool_provider_state_from_kube(
    provider: &AgentToolProvider,
    network_id: &str,
    site_id: &str,
) -> Option<crdt::ProviderState> {
    let name = provider.metadata.name.as_deref()?;
    let tools = tool_names_from_agent_tool_provider(provider);
    let phase = provider
        .status
        .as_ref()
        .map_or(crdt::ProviderPhase::Pending, |status| match status.phase {
            crate::crd::inference_provider::ProviderPhase::Available => crdt::ProviderPhase::Available,
            crate::crd::inference_provider::ProviderPhase::Degraded => crdt::ProviderPhase::Degraded,
            crate::crd::inference_provider::ProviderPhase::Unavailable => crdt::ProviderPhase::Unavailable,
            crate::crd::inference_provider::ProviderPhase::Pending => crdt::ProviderPhase::Pending,
        });
    let revision = provider_revision(&provider.metadata);
    // Prefix with "tool/" to distinguish from InferenceProvider names in the
    // CRDT key (network/site/provider_id). Without this, an InferenceProvider
    // and AgentToolProvider with the same name would collide, and one record
    // would silently displace the other.
    let provider_id = routing_overlay::tool_routing_cluster(name);
    Some(crdt::ProviderState {
        network_id: network_id.to_owned(),
        site_id: site_id.to_owned(),
        provider_id: provider_id.clone(),
        routing_cluster: provider_id,
        models: Vec::new(),
        tools,
        backend_kind: String::new(),
        capacity_weight: crdt::MIN_CAPACITY_WEIGHT,
        phase,
        metrics: crdt::ProviderMetricsSnapshot::default(),
        access_policy: access_policy_to_crdt(&provider.spec.access_policy),
        revision,
        writer_id: site_id.to_owned(),
    })
}

/// Maximum number of tool names retained per [`AgentToolProvider`].
///
/// Large MCP catalogs can inflate both the SWIM broadcast byte budget and
/// the routing overlay size. Excess tools are truncated and a warning is
/// logged.
const MAX_TOOLS_PER_PROVIDER: usize = 128;

/// Maximum encoded length of one MCP tool name accepted by the routing path.
const MAX_TOOL_NAME_LEN: usize = 256;

/// Return the current available discovery status, if routing may consume it.
fn current_tool_status(
    provider: &AgentToolProvider,
) -> Option<&crate::crd::agent_tool_provider::AgentToolProviderStatus> {
    let status = provider.status.as_ref()?;
    let generation = provider.metadata.generation.unwrap_or(0);
    (status.phase == crate::crd::inference_provider::ProviderPhase::Available
        && status.observed_generation == generation)
        .then_some(status)
}

/// Extract tool names from an [`AgentToolProvider`].
///
/// Tools are emitted only from a current-generation `Available` status. If
/// `spec.tools` is non-empty it acts as an allowlist over live discovery.
/// A successful probe that discovers zero tools is authoritative and remains
/// empty; spec entries are never treated as unverified routing data.
///
/// Results are capped at [`MAX_TOOLS_PER_PROVIDER`] to bound SWIM byte
/// budget and overlay size.
pub(crate) fn tool_names_from_agent_tool_provider(provider: &AgentToolProvider) -> Vec<String> {
    let Some(status) = current_tool_status(provider) else {
        return Vec::new();
    };

    let allowed: std::collections::HashSet<&str> = provider.spec.tools.iter().map(|tool| tool.name.as_str()).collect();
    let mut seen = std::collections::HashSet::new();
    let mut tools: Vec<String> = status
        .discovered_tools
        .iter()
        .filter(|tool| {
            !tool.trim().is_empty()
                && tool.len() <= MAX_TOOL_NAME_LEN
                && (allowed.is_empty() || allowed.contains(tool.as_str()))
                && seen.insert((*tool).clone())
        })
        .cloned()
        .collect();

    if tools.len() > MAX_TOOLS_PER_PROVIDER {
        tracing::warn!(
            provider = provider.metadata.name.as_deref().unwrap_or("?"),
            total = tools.len(),
            cap = MAX_TOOLS_PER_PROVIDER,
            "tool provider exceeds per-provider tool cap; truncating"
        );
        tools.truncate(MAX_TOOLS_PER_PROVIDER);
    }

    tools
}

/// Publish real provider records as a CRDT state broadcast over SWIM.
///
/// Builds a [`crdt::GridStateSnapshot`] from all [`InferenceProvider`] and
/// [`AgentToolProvider`] resources belonging to `network_name`, attaches
/// live metrics where configured (inference only), and sends the snapshot
/// to SWIM peers via [`SwimHandle::publish_state_broadcast`]. An empty
/// provider snapshot is still sent as an authoritative withdrawal so peers
/// remove any retained provider records for this site.
///
/// Providers are included regardless of their phase (even `Unavailable`) so
/// remote sites can learn which providers exist and avoid routing to unhealthy
/// ones.  The routing overlay layer already filters `Unavailable` providers
/// from local routing decisions.
///
/// `grid_id` (the caller's already-[`resolve_grid_id`]d value) is attached to
/// the broadcast so a signature over this `GridNetwork`'s state cannot be
/// replayed as valid for a different `GridNetwork` sharing the same
/// cluster's `SwimHandle` — see [`swim::StateBroadcast::grid_id`].
/// Upsert a CRDT provider state, registering model capabilities and advancing
/// `max_revision`. Tool names remain only on the provider record.
fn upsert_provider_with_capabilities(
    snap: &mut crdt::GridStateSnapshot,
    max_revision: &mut u64,
    state: crdt::ProviderState,
) {
    *max_revision = (*max_revision).max(state.revision);
    for model in &state.models {
        if !model.is_empty() {
            snap.add_capability(crdt::Capability::Model(model.clone()));
        }
    }
    // Tool names are NOT registered as Capability::Tool in the OR-set.
    // They travel in the BroadcastExtension's `provider_tools` map and are
    // stored directly on the ProviderState. Duplicating them as capabilities
    // would bloat the SWIM byte budget for providers with large tool catalogs.
    snap.upsert_provider(state);
}

/// Append one tool only when the resulting broadcast remains transport-safe.
fn append_tool_within_budget(
    broadcast: &mut swim::StateBroadcast,
    provider_key: &str,
    tool: String,
    byte_budget: usize,
) -> bool {
    let Some(provider) = broadcast.snapshot.providers.get_mut(provider_key) else {
        return false;
    };
    provider.tools.push(tool);
    if broadcast.encode().is_ok_and(|encoded| encoded.len() <= byte_budget) {
        return true;
    }
    if let Some(oversized_provider) = broadcast.snapshot.providers.get_mut(provider_key) {
        oversized_provider.tools.pop();
    }
    false
}

/// One tool-provider record temporarily removed while fitting the transport.
struct ToolCatalog {
    /// Snapshot map key for the provider.
    key: String,
    /// Provider metadata, with `tools` cleared while detached.
    provider: crdt::ProviderState,
    /// Offered tool names in their discovery order.
    tools: Vec<String>,
}

/// Detach tool-provider records so inference state forms the immutable baseline.
fn detach_tool_catalogs(broadcast: &mut swim::StateBroadcast) -> Vec<ToolCatalog> {
    let keys: Vec<String> = broadcast
        .snapshot
        .providers
        .iter()
        .filter(|(_, provider)| provider.provider_id.starts_with("tool/") && provider.models.is_empty())
        .map(|(key, _)| key.clone())
        .collect();
    keys.into_iter()
        .filter_map(|key| {
            let mut provider = broadcast.snapshot.providers.remove(&key)?;
            let tools = std::mem::take(&mut provider.tools);
            Some(ToolCatalog { key, provider, tools })
        })
        .collect()
}

/// Reinsert catalog entries round-robin while each encoded snapshot still fits.
///
/// Exits early once a full round produces no new retained tools — further
/// rounds cannot fit either (inference baseline is fixed, only tools grow).
fn retain_tool_catalogs_within_budget(
    broadcast: &mut swim::StateBroadcast,
    catalogs: &[ToolCatalog],
    byte_budget: usize,
) -> usize {
    let rounds = catalogs.iter().map(|catalog| catalog.tools.len()).max().unwrap_or(0);
    let mut retained = 0_usize;
    for tool_index in 0..rounds {
        let round_start = retained;
        for catalog in catalogs {
            let Some(tool) = catalog.tools.get(tool_index) else {
                continue;
            };
            if tool_index == 0 {
                broadcast
                    .snapshot
                    .providers
                    .insert(catalog.key.clone(), catalog.provider.clone());
            }
            if append_tool_within_budget(broadcast, &catalog.key, tool.clone(), byte_budget) {
                retained = retained.saturating_add(1);
            } else if tool_index == 0 {
                broadcast.snapshot.providers.remove(&catalog.key);
            }
        }
        // If no tools were added this round, no future round can fit either.
        if retained == round_start {
            break;
        }
    }
    retained
}

/// Deterministically trim tool catalogs until the encoded state fits SWIM.
///
/// Inference provider records and capabilities are never removed. Tool
/// providers are added back round-robin only when their provider record and at
/// least one tool fit. If the inference-only baseline already exceeds the
/// transport budget, all tool providers remain omitted and the runtime reports
/// the oversized inference snapshot.
#[expect(
    clippy::too_many_lines,
    reason = "transport preflight, deterministic trimming, and its summary log form one bounded operation"
)]
fn fit_provider_tools_to_swim_budget(broadcast: &mut swim::StateBroadcast) {
    // The runtime's advertised address may be IPv6, so preflight against the
    // largest valid source identity for this site name. This cannot exceed the
    // runtime's real transport budget.
    let local_id = worst_case_swim_identity(&broadcast.origin_site);
    let Ok(byte_budget) = swim::node::state_broadcast_byte_budget(&local_id) else {
        tracing::warn!("failed to calculate SWIM state-broadcast byte budget");
        return;
    };
    // The runtime replaces the CRD-derived revision with a leased transport
    // revision. Fit against the largest bincode representation so that rewrite
    // cannot push an edge-sized snapshot over the node's byte limit.
    let original_revision = std::mem::replace(&mut broadcast.revision, u64::MAX);
    if broadcast.encode().is_ok_and(|encoded| encoded.len() <= byte_budget) {
        broadcast.revision = original_revision;
        return;
    }

    let catalogs = detach_tool_catalogs(broadcast);
    let providers_offered = catalogs.len();
    let offered = catalogs.iter().map(|catalog| catalog.tools.len()).sum::<usize>();
    let baseline_fits = broadcast.encode().is_ok_and(|encoded| encoded.len() <= byte_budget);
    let retained = if baseline_fits {
        retain_tool_catalogs_within_budget(broadcast, &catalogs, byte_budget)
    } else {
        tracing::warn!(
            byte_budget,
            "inference provider snapshot exceeds the SWIM broadcast budget"
        );
        0
    };
    if broadcast.snapshot.providers.is_empty() {
        broadcast.authoritative_provider_state = true;
    }
    let providers_retained = catalogs
        .iter()
        .filter(|catalog| broadcast.snapshot.providers.contains_key(&catalog.key))
        .count();
    broadcast.revision = original_revision;
    tracing::warn!(
        providers_offered,
        providers_retained,
        providers_dropped = providers_offered.saturating_sub(providers_retained),
        offered,
        retained,
        dropped = offered.saturating_sub(retained),
        byte_budget,
        "trimmed MCP tool providers and catalogs to fit the SWIM broadcast budget"
    );
}

/// Return the largest valid local SWIM identity for `site_name`.
///
/// Tool-catalog fitting occurs before the operator can inspect the runtime's
/// private foca identity. Using IPv6 and the largest generation makes the
/// resulting payload budget safe for either IPv4 or IPv6 runtime addresses.
fn worst_case_swim_identity(site_name: &str) -> swim::NodeId {
    let address = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, u16::MAX, u32::MAX, u32::MAX));
    swim::NodeId::with_generation(site_name.to_owned(), address, u64::MAX)
}

/// Build an authoritative provider snapshot, including an explicit empty marker.
fn provider_state_broadcast(
    site_name: &str,
    grid_id: &str,
    revision: u64,
    snap: crdt::GridStateSnapshot,
    gateway_address: Option<String>,
) -> swim::StateBroadcast {
    let authoritative_empty = snap.providers.is_empty();
    let broadcast = swim::StateBroadcast::new(site_name.to_owned(), revision, snap, gateway_address)
        .with_grid_id(Some(grid_id.to_owned()));
    if authoritative_empty {
        broadcast.with_authoritative_provider_state()
    } else {
        broadcast
    }
}

/// Publish provider records as a CRDT state broadcast over SWIM.
///
/// Builds a [`crdt::GridStateSnapshot`] from all [`InferenceProvider`] and
/// [`AgentToolProvider`] resources belonging to `network_name`, attaches
/// live metrics where configured (inference only), and sends the snapshot
/// to SWIM peers via [`SwimHandle::publish_state_broadcast`].
#[expect(
    clippy::too_many_arguments,
    reason = "tool_providers is the new distinct input alongside existing inference parameters"
)]
fn publish_real_provider_state(
    swim: &SwimHandle,
    network_name: &str,
    grid_id: &str,
    providers: &[InferenceProvider],
    tool_providers: &[AgentToolProvider],
    raw_metrics: &HashMap<String, scoring::BackendMetrics>,
) {
    let site_name = swim.site_name();
    let mut snap = crdt::GridStateSnapshot::new(site_name.to_owned());
    let mut max_revision: u64 = 0;

    for provider in providers {
        if provider.spec.grid_network_ref != network_name {
            continue;
        }
        let routing_id = routing_overlay::routing_identity(provider).unwrap_or("");
        let metrics = raw_metrics.get(routing_id).copied();
        if let Some(state) = provider_state_from_kube(provider, network_name, site_name, metrics) {
            upsert_provider_with_capabilities(&mut snap, &mut max_revision, state);
        }
    }

    for tool_provider in tool_providers {
        if tool_provider.spec.grid_network_ref != network_name {
            continue;
        }
        if let Some(state) = tool_provider_state_from_kube(tool_provider, network_name, site_name)
            && !state.tools.is_empty()
        {
            upsert_provider_with_capabilities(&mut snap, &mut max_revision, state);
        }
    }

    let mut bc = provider_state_broadcast(site_name, grid_id, max_revision, snap, swim.gateway_address());
    fit_provider_tools_to_swim_budget(&mut bc);
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
/// Available or pending providers from `Dead` or `Suspect` SWIM members are
/// downgraded to [`crdt::ProviderPhase::Degraded`] so the routing overlay emits
/// them with `fresh = false`. Explicitly unavailable providers remain
/// unavailable; membership staleness must never make a failed provider routable.
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
            if is_degraded && p.phase != crdt::ProviderPhase::Unavailable {
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
    mount_reconciliation_statuses: Vec<MountReconciliationStatus>,
    overlay_statuses: Vec<OverlayRevisionStatus>,
    budget_statuses: Vec<TenantBudgetStatus>,
    identity: Option<SiteIdentityStatus>,
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
        consumer_config_status: keep_consumer_config_status_at(network, &consumer_config_statuses),
        mount_reconciliation_status: keep_mount_reconciliation_status_at(network, &mount_reconciliation_statuses),
        overlay_status: keep_rendered_at(network.status.as_ref(), overlay_statuses),
        budget_status: budget_statuses,
        identity,
    };

    if !grid_network_status_needs_update(network.status.as_ref(), &status) {
        return Ok(());
    }

    let patch = serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridNetwork",
        "status": status
    });

    api.patch_status(name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(patch))
        .await?;

    Ok(())
}

/// Reason a site identity past its `notAfter` reports.
pub const IDENTITY_EXPIRED: &str = "IdentityExpired";

/// `status.identity.reason` when the identity Secret holds no readable certificate.
pub const IDENTITY_UNREADABLE: &str = "IdentityUnreadable";

/// This site's identity status from its certificate, `None` without one.
async fn site_identity_status(
    network: &GridNetwork,
    client: &Client,
    now: time::OffsetDateTime,
    renews: bool,
) -> Option<SiteIdentityStatus> {
    use crate::resources::endpoint_tls::TlsFailureReason;

    let site = network.spec.tls.site_secret_ref.as_ref()?;
    let read =
        crate::resources::endpoint_tls::read_secret_bytes_for_tls(client, site, "tls.crt", "signals", "site identity")
            .await;
    let bytes = match read {
        Ok(bytes) => bytes,
        // Not written yet: enrollment or the self-signed path writes it.
        Err((TlsFailureReason::SecretMissing, _)) => return None,
        Err((TlsFailureReason::KeyMissing, message)) => return Some(unreadable_identity(&message)),
        // A failed read says nothing new about the certificate, so the last status stands.
        Err(_) => return network.status.as_ref().and_then(|status| status.identity.clone()),
    };
    let status = String::from_utf8(bytes)
        .ok()
        .and_then(|pem| identity_status(&pem, now, renews));
    Some(status.unwrap_or_else(|| unreadable_identity("tls.crt is not a certificate")))
}

/// The identity status of a Secret that holds no usable certificate.
fn unreadable_identity(detail: &str) -> SiteIdentityStatus {
    // Zero, not the last good notAfter, so an expiry alert fires on broken material.
    crate::metrics::set_site_identity_expiry(0);
    SiteIdentityStatus {
        not_after: String::new(),
        rotate_after: String::new(),
        fingerprint: String::new(),
        reason: IDENTITY_UNREADABLE.to_owned(),
        message: format!("the site identity cannot be read ({detail}); restore the identity Secret or re-enroll"),
    }
}

/// The identity status of `cert_pem` at `now`; without renewal, no renewal time.
fn identity_status(cert_pem: &str, now: time::OffsetDateTime, renews: bool) -> Option<SiteIdentityStatus> {
    use time::format_description::well_known::Rfc3339;
    let (not_before, not_after) = certs::cert_validity(cert_pem).ok()?;
    crate::metrics::set_site_identity_expiry(not_after.unix_timestamp());
    let renew_after = crate::enroll::renew::renew_after(not_before, not_after);
    let expired = now >= not_after;
    Some(SiteIdentityStatus {
        not_after: not_after.format(&Rfc3339).ok()?,
        rotate_after: if renews {
            renew_after.format(&Rfc3339).ok()?
        } else {
            String::new()
        },
        fingerprint: certs::canonical_fingerprint(cert_pem).ok()?,
        reason: if expired {
            IDENTITY_EXPIRED.to_owned()
        } else {
            String::new()
        },
        message: if expired {
            "the site identity expired and cannot renew: a grid-admin deletes this site's enrollment, mints a new \
             site token, and the site re-enrolls"
                .to_owned()
        } else if renews {
            String::new()
        } else {
            "rotation is off under pin peer trust: re-enroll and re-pin this site before notAfter".to_owned()
        },
    })
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
        OperatorError::ConsumerConfigRender(ConsumerConfigError::NoInferenceCandidates) => "NoInferenceCandidates",
        OperatorError::ConsumerConfigRender(ConsumerConfigError::ProjectedCredentialsUnsupported) => {
            "ProjectedCredentialsUnsupported"
        },
        OperatorError::ConsumerConfigRender(_) => "ConsumerConfigRenderFailed",
        OperatorError::MountReconciliation(failure) => failure.reason,
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

/// The site a self-issued certificate names: this site when known, else the network.
///
/// `None` when neither is a valid site name. The name becomes the certificate's SPIFFE
/// path segment and its subject organization, which a peer authorizes on, so a network
/// name carrying dots or exceeding a DNS label cannot stand in for it.
fn issued_site_name(network: &GridNetwork, site: Option<&str>) -> Option<String> {
    let name = site.map_or_else(|| network_site_name(network), str::to_owned);
    certs::is_valid_site_name(&name).then_some(name)
}

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
    /// Bare SWIM `site_id`, stamped as an annotation so the poll path can key by it.
    ///
    /// The name is `{network}-{site_id}`, so it is not the key the poller uses.
    pub site_id: String,
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
        // Only a name the grid CA could have issued, so one gossiped id cannot fail the apply or collide.
        .filter(|m| certs::validate_site_name(&m.site_id).is_ok())
        .map(|m| DiscoveredSite {
            name: discovered_site_k8s_name(network_name, &m.site_id),
            site_id: m.site_id.clone(),
            grid_network_ref: network_name.to_owned(),
            egress_address: m.gateway_address.clone().unwrap_or_default(),
            site_cert_pem: m.site_cert_pem.clone(),
        })
        .collect()
}

/// Most `GridSite` objects auto discovery creates for one network, bounding what a gossiping peer can mint.
const MAX_AUTO_CREATED_SITES: usize = 256;

/// The auto-discovered `GridSite` objects that already belong to `network_name`.
async fn auto_discovered_stubs(api: &Api<GridSite>, network_name: &str) -> Result<Vec<GridSite>, OperatorError> {
    let selector = format!("{LABEL_AUTO_DISCOVERED}=true,grid.praxis.fast/network={network_name}");
    let mut stubs = api.list(&ListParams::default().labels(&selector)).await?.items;
    // A label is not ownership: only discovery's own objects for this network.
    stubs.retain(|stub| stub.spec.grid_network_ref == network_name && is_stub(stub));
    Ok(stubs)
}

/// Whether `site` is a stub discovery wrote: labeled auto-discovered and keyed by its site-id annotation.
fn is_stub(site: &GridSite) -> bool {
    peer_site_key(site).is_some_and(|(_, enrolled)| !enrolled)
}

/// How long a departed stub is kept, independent of overlay pruning so a short
/// `staleCandidateTtlSeconds` never deletes a `GridSite`.
const STUB_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// A gossiped record's lifetime without a refresh.
const GOSSIP_RECORD_EXPIRY: Duration = Duration::from_secs(600);

/// Time for membership to converge once records can expire.
const GOSSIP_CONVERGENCE: Duration = Duration::from_secs(120);

/// Uptime before discovery judges any site absent: one full verification window.
const STUB_GC_WARMUP: Duration = GOSSIP_RECORD_EXPIRY.saturating_add(GOSSIP_CONVERGENCE);

/// Whether gossip vouches for one site this pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Vouch {
    /// Gossip vouches for it.
    Vouched,
    /// Gossip has had a full window to vouch for it and has not.
    Absent,
    /// Too soon after a start to tell, so it is neither marked nor collected.
    Unchecked,
}

/// Who gossip vouches for this pass.
struct Vouching<'snap> {
    /// Every member not `Dead`.
    vouched: std::collections::HashSet<&'snap str>,
    /// Whether the operator has been up a full [`STUB_GC_WARMUP`].
    settled: bool,
}

impl Vouching<'_> {
    /// Whether gossip vouches for `site`.
    fn of(&self, site: &str) -> Vouch {
        if self.vouched.contains(site) {
            Vouch::Vouched
        } else if self.settled {
            Vouch::Absent
        } else {
            Vouch::Unchecked
        }
    }
}

/// Who gossip vouches for after `uptime`.
///
/// The one place absence is judged, so per-origin verified-record times can replace SWIM liveness here.
fn vouched_sites(snapshot: &MembershipSnapshot, uptime: Duration) -> Vouching<'_> {
    Vouching {
        vouched: snapshot
            .members
            .iter()
            .filter(|member| member.status != MemberStatus::Dead)
            .map(|member| member.site_id.as_str())
            .collect(),
        settled: uptime >= STUB_GC_WARMUP,
    }
}

/// What the collection pass does with one stub.
#[derive(Debug, Eq, PartialEq)]
enum StubGc {
    /// Nothing to write; it counts against the cap.
    Keep,
    /// Gossip stopped vouching for it; start the clock.
    MarkAbsent,
    /// Its site is back; stop the clock.
    ClearAbsent,
    /// Gone past the TTL; delete it. It no longer counts against the cap.
    Collect,
}

/// Decide what to do with stub `site` given whether gossip vouches for it.
fn stub_gc(site: &GridSite, vouch: Vouch, ttl: Duration, now: time::OffsetDateTime) -> StubGc {
    let absent_since = site.status.as_ref().and_then(|status| status.absent_since.as_deref());
    match (vouch, absent_since) {
        (Vouch::Vouched, None) | (Vouch::Unchecked, _) => StubGc::Keep,
        (Vouch::Vouched, Some(_)) => StubGc::ClearAbsent,
        (Vouch::Absent, None) => StubGc::MarkAbsent,
        (Vouch::Absent, Some(since)) => {
            match time::OffsetDateTime::parse(since, &time::format_description::well_known::Rfc3339) {
                Ok(since) if now - since >= ttl => StubGc::Collect,
                Ok(_) => StubGc::Keep,
                // An unreadable clock restarts rather than collects.
                Err(_) => StubGc::MarkAbsent,
            }
        },
    }
}

/// Each named stub with what the collection pass does to it.
fn plan_stub_gc<'stub>(
    stubs: &'stub [GridSite],
    vouching: &Vouching<'_>,
    ttl: Duration,
    now: time::OffsetDateTime,
) -> Vec<(&'stub GridSite, &'stub str, StubGc)> {
    let mut plan: Vec<_> = stubs
        .iter()
        .filter_map(|stub| {
            let name = stub.metadata.name.as_deref()?;
            let (site_id, _) = peer_site_key(stub)?;
            Some((stub, name, stub_gc(stub, vouching.of(&site_id), ttl, now)))
        })
        .collect();
    brake(&mut plan);
    plan
}

/// Collections a pass may always make, however few stubs there are.
const COLLECT_BRAKE_FLOOR: usize = 8;

/// Hold every collection in `plan` when it would delete more than half the stubs past the
/// floor, or every stub of several.
///
/// A partition or a bug looks like mass departure; the held stubs are judged again next pass.
fn brake(plan: &mut [(&GridSite, &str, StubGc)]) -> bool {
    let collect = plan.iter().filter(|(_, _, gc)| *gc == StubGc::Collect).count();
    let most = collect > COLLECT_BRAKE_FLOOR && collect.saturating_mul(2) > plan.len();
    let all = plan.len() > 1 && collect == plan.len();
    let braked = most || all;
    if braked {
        tracing::warn!(
            collect,
            stubs = plan.len(),
            "refusing to collect most auto-discovered GridSites at once"
        );
        for (_, _, gc) in plan.iter_mut().filter(|(_, _, gc)| *gc == StubGc::Collect) {
            *gc = StubGc::Keep;
        }
    }
    braked
}

/// The stubs that count against [`MAX_AUTO_CREATED_SITES`]: every one not past the TTL.
fn counted_stubs(plan: &[(&GridSite, &str, StubGc)]) -> BTreeSet<String> {
    plan.iter()
        .filter(|(_, _, gc)| *gc != StubGc::Collect)
        .map(|(_, name, _)| (*name).to_owned())
        .collect()
}

/// Collect stubs gone past `ttl` and track absence on the rest; returns the names that count against the cap.
///
/// Absence lives in status, written only on a transition, so a restart keeps the clock and an
/// unchanged stub writes nothing.
async fn collect_stale_stubs(
    api: &Api<GridSite>,
    network_name: &str,
    vouching: &Vouching<'_>,
    ttl: Duration,
    stubs: &[GridSite],
) -> Result<BTreeSet<String>, OperatorError> {
    let plan = plan_stub_gc(stubs, vouching, ttl, time::OffsetDateTime::now_utc());
    for (stub, name, gc) in &plan {
        let since = match gc {
            StubGc::Keep => continue,
            StubGc::Collect => {
                if delete_unchanged_stub(api, stub, name).await? {
                    tracing::info!(
                        name,
                        network = network_name,
                        "collected departed auto-discovered GridSite"
                    );
                }
                continue;
            },
            StubGc::MarkAbsent => rfc3339_now(),
            StubGc::ClearAbsent => None,
        };
        let patch = serde_json::json!({ "status": { "absentSince": since } });
        api.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await?;
    }
    Ok(counted_stubs(&plan))
}

/// Delete `stub` only if it is still the object judged stale, leaving finalizers to the API server.
///
/// Discovery re-applying a returning site bumps the resourceVersion, so the precondition fails and
/// the stub survives instead of being deleted under it.
async fn delete_unchanged_stub(api: &Api<GridSite>, stub: &GridSite, name: &str) -> Result<bool, OperatorError> {
    if stub.metadata.deletion_timestamp.is_some() {
        return Ok(false);
    }
    let params = DeleteParams::default().preconditions(Preconditions {
        resource_version: stub.metadata.resource_version.clone(),
        uid: stub.metadata.uid.clone(),
    });
    match api.delete(name, &params).await {
        Ok(_) => Ok(true),
        Err(kube::Error::Api(error)) if error.code == 404 || error.code == 409 => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// Log the members [`admit_stub`] left out this round.
fn warn_capped(network_name: &str, capped: usize) {
    if capped > 0 {
        tracing::warn!(
            network = %network_name,
            capped,
            "SWIM members not adopted: {MAX_AUTO_CREATED_SITES} auto-discovered GridSites reached"
        );
    }
}

/// Whether discovery may apply `name`: an existing stub always, a new one only under [`MAX_AUTO_CREATED_SITES`].
fn admit_stub(stubs: &mut BTreeSet<String>, name: &str) -> bool {
    if stubs.contains(name) {
        return true;
    }
    if stubs.len() >= MAX_AUTO_CREATED_SITES {
        return false;
    }
    stubs.insert(name.to_owned())
}

/// Log a reconciled stub at INFO when `noteworthy`, else at debug.
fn log_stub(site: &DiscoveredSite, network_name: &str, noteworthy: bool) {
    if noteworthy {
        tracing::info!(
            name = %site.name,
            network = %network_name,
            egress = %site.egress_address,
            cert = site.site_cert_pem.is_some(),
            "reconciled auto-discovered GridSite from SWIM Alive member"
        );
    } else {
        tracing::debug!(name = %site.name, network = %network_name, "auto-discovered GridSite unchanged");
    }
}

/// Whether applying `site` creates its stub or changes the egress or certificate the stub carries.
fn stub_changed(existing: Option<&GridSite>, site: &DiscoveredSite) -> bool {
    let Some(existing) = existing else {
        return true;
    };
    let egress = existing
        .spec
        .egress
        .as_ref()
        .map_or("", |egress| egress.address.as_str());
    let cert = existing
        .status
        .as_ref()
        .and_then(|status| status.public_cert_pem.as_deref());
    egress != site.egress_address || (site.site_cert_pem.is_some() && cert != site.site_cert_pem.as_deref())
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
                "apiVersion": "grid.praxis.fast/v1alpha1",
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

/// The applied `GridSite` for a discovered peer, named for the identity the probe verifies.
fn discovered_site_spec(site: &DiscoveredSite, network_name: &str, plaintext: bool) -> Value {
    let mut spec = serde_json::json!({ "gridNetworkRef": site.grid_network_ref });
    if !site.egress_address.is_empty()
        && let Some(fields) = spec.as_object_mut()
    {
        let tls = if plaintext {
            serde_json::json!({ "mode": "Plaintext" })
        } else {
            serde_json::json!({
                "mode": "Mutual",
                "serverName": format!("{}.{}", site.site_id, certs::SPIFFE_TRUST_DOMAIN),
            })
        };
        fields.insert(
            "egress".to_owned(),
            serde_json::json!({ "address": site.egress_address, "tls": tls }),
        );
    }
    serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridSite",
        "metadata": {
            "name": site.name,
            "labels": {
                "grid.praxis.fast/network": network_name,
                LABEL_AUTO_DISCOVERED: "true"
            },
            "annotations": { ANNOTATION_SITE_ID: site.site_id }
        },
        "spec": spec,
    })
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
    ctx: &OperatorCtx,
    network: &GridNetwork,
    network_name: &str,
    local_site: &str,
    snapshot: &MembershipSnapshot,
) -> Result<(), OperatorError> {
    let client = &ctx.client;
    let plaintext = network_uses_plaintext_egress(network);
    let ttl = STUB_TTL;
    let api: Api<GridSite> = Api::all(client.clone());
    let present = auto_discovered_stubs(&api, network_name).await?;
    let vouching = vouched_sites(snapshot, ctx.started.elapsed());
    let mut stubs = collect_stale_stubs(&api, network_name, &vouching, ttl, &present).await?;

    let sites = discovered_sites_from_swim(network_name, local_site, snapshot);
    let mut capped = 0_usize;

    for site in &sites {
        if !admit_stub(&mut stubs, &site.name) {
            capped += 1;
            continue;
        }
        // Server-side apply the spec.  Creating on first call; updating on subsequent
        // calls is a no-op when the spec has not changed.
        let spec_doc = discovered_site_spec(site, network_name, plaintext);
        // Read before the apply, which writes only spec, so status is the same either side of it.
        let existing = api.get_opt(&site.name).await?;
        let noteworthy = stub_changed(existing.as_ref(), site);

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
        let existing_status = existing.and_then(|s| s.status);

        // Only write Discovered when the current phase is Pending.
        // If the GridSite controller has already advanced the phase (e.g. to
        // Connecting), we must not regress it.
        let should_write_discovered = matches!(
            existing_status.as_ref().map(|s| &s.phase),
            None | Some(GridSitePhase::Pending)
        );

        if should_write_discovered {
            let status_doc = serde_json::json!({
                "apiVersion": "grid.praxis.fast/v1alpha1",
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

        log_stub(site, network_name, noteworthy);
    }

    warn_capped(network_name, capped);
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
    use crate::swim_endpoint::EndpointResolutionFailure;

    #[test]
    fn a_provider_not_ready_publishes_ready_zero_without_a_fresh_scrape() {
        let ready = |value: f64| signals::Observation {
            metric: readiness::READY_SIGNAL.to_owned(),
            labels: BTreeMap::new(),
            value,
            timestamp_ms: None,
        };
        assert!(
            matches!(
                published_signals(None, ready(0.0), None, true).as_deref(),
                Some([only]) if only.metric == readiness::READY_SIGNAL && only.value == 0.0
            ),
            "a failed or stale provider keeps a ready=0 row, so an alert sees it"
        );
        assert!(
            published_signals(None, ready(1.0), None, false).is_none(),
            "a waiting or unconfigured provider publishes nothing"
        );
    }


    #[expect(clippy::expect_used, reason = "test fixture")]
    fn provider_with_status(status: &Value) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": "qwen3-site-b", "generation": 1 },
            "spec": {
                "gridNetworkRef": "grid",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": []
            },
            "status": status
        }))
        .expect("provider")
    }

    fn ready_condition_json(status: &str, reason: &str) -> Value {
        serde_json::json!({
            "type": "Ready", "status": status, "reason": reason, "message": "m",
            "lastTransitionTime": "2026-10-03T00:00:00Z", "observedGeneration": 1
        })
    }

    #[test]
    #[expect(
        clippy::expect_used,
        clippy::indexing_slicing,
        reason = "test assertions on a JSON patch"
    )]
    fn the_ready_condition_is_patched_only_on_a_change() {
        let down = readiness::Verdict {
            reason: readiness::Reason::NoEndpointsReady,
            message: "0 ready endpoints".to_owned(),
        };
        let current = provider_with_status(&serde_json::json!({
            "conditions": [ready_condition_json("False", "NoEndpointsReady")]
        }));
        assert!(
            ready_condition_patch(&current, &down).is_none(),
            "the condition already says so, so there is nothing to write"
        );
        let up = readiness::Verdict {
            reason: readiness::Reason::Ready,
            message: "1 ready endpoints".to_owned(),
        };
        let (_, patch) = ready_condition_patch(&current, &up).expect("a transition is written");
        assert_eq!(patch["status"]["conditions"][0]["status"], "True");
        assert!(
            patch["status"].get("state").is_none(),
            "the column reads the condition, so status carries no copy of it"
        );
    }

    fn network_with_modes(spec: &Value) -> GridNetwork {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": {"name": "grid"},
            "spec": spec,
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn modes_restart_only_when_a_grid_network_declares_others() {
        let running = GridModes::WITHOUT_NETWORK;
        assert_eq!(running.restart_for(None), None, "no GridNetwork, no restart");

        let same = network_with_modes(&serde_json::json!({"peerTrust": {"mode": "spiffe"}}));
        assert_eq!(running.restart_for(Some(&same)), None, "same modes, no restart");

        let poll = network_with_modes(&serde_json::json!({
            "peerTrust": {"mode": "spiffe"},
            "signalTransport": {"mode": "poll"},
        }));
        assert_eq!(
            running.restart_for(Some(&poll)),
            Some(GridModes {
                signal: SignalMode::Poll,
                trust: PeerTrustMode::Spiffe,
            })
        );
        assert_eq!(
            GridModes::of(&poll).restart_for(Some(&poll)),
            None,
            "the restarted process runs what it declares, so it never loops"
        );
    }

    #[test]
    fn a_trust_change_restarts_only_under_poll() {
        let pin = network_with_modes(&serde_json::json!({"peerTrust": {"mode": "pin"}}));
        assert_eq!(
            GridModes::WITHOUT_NETWORK.restart_for(Some(&pin)),
            None,
            "gossip reads no trust, so a fresh install declaring pin does not restart"
        );
        let poll = network_with_modes(&serde_json::json!({
            "peerTrust": {"mode": "spiffe"},
            "signalTransport": {"mode": "poll"},
        }));
        let poll_pin = network_with_modes(&serde_json::json!({
            "peerTrust": {"mode": "pin"},
            "signalTransport": {"mode": "poll"},
        }));
        assert_eq!(
            GridModes::of(&poll).restart_for(Some(&poll_pin)),
            Some(GridModes::of(&poll_pin)),
            "under poll a trust change restarts"
        );
    }

    #[test]
    fn a_poll_install_starts_in_poll_before_its_grid_network_exists() {
        let startup = GridModes::without_network(Some(SignalMode::Poll), Some(PeerTrustMode::Pin));
        assert_eq!(startup.signal, SignalMode::Poll, "the chart's grid.signals, not gossip");
        let poll_pin = network_with_modes(&serde_json::json!({
            "peerTrust": {"mode": "pin"},
            "signalTransport": {"mode": "poll"},
        }));
        assert_eq!(
            startup.restart_for(Some(&poll_pin)),
            None,
            "the network it declared needs no restart"
        );
        assert!(
            GridModes::WITHOUT_NETWORK.restart_for(Some(&poll_pin)).is_some(),
            "without the declared modes the same install restarts"
        );
        assert_eq!(
            GridModes::without_network(Some(SignalMode::Poll), None).trust,
            PeerTrustMode::Spiffe,
            "undeclared trust keeps its default"
        );
        assert_eq!(
            GridModes::without_network(None, None),
            GridModes::WITHOUT_NETWORK,
            "an install declaring nothing keeps today's defaults"
        );
    }

    fn seed_addr(value: &str) -> SocketAddr {
        value.parse().unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn crd_seed_resolution_retains_last_known_good_when_all_current_lookups_fail() {
        let resolution = SeedResolution {
            configured: true,
            addresses: Vec::new(),
            failures: vec![EndpointResolutionFailure {
                source: "GridNetwork.spec.seeds".to_owned(),
                endpoint: "missing.example:7946".to_owned(),
                reason: "DNS resolution failed".to_owned(),
            }],
        };
        assert!(matches!(
            crd_seed_decision(&resolution, &[seed_addr("10.0.0.1:7946")]),
            CrdSeedDecision::Retain
        ));
    }

    #[test]
    fn crd_seed_resolution_announces_recovered_addresses() {
        let resolution = SeedResolution {
            configured: true,
            addresses: vec![seed_addr("10.0.0.2:7946")],
            failures: Vec::new(),
        };
        assert!(matches!(
            crd_seed_decision(&resolution, &[seed_addr("10.0.0.1:7946")]),
            CrdSeedDecision::Announce(addresses) if addresses == vec![seed_addr("10.0.0.2:7946")]
        ));
    }

    #[test]
    fn crd_seed_resolution_without_previous_seeds_stays_seedless() {
        let resolution = SeedResolution {
            configured: true,
            addresses: Vec::new(),
            failures: Vec::new(),
        };
        assert!(matches!(crd_seed_decision(&resolution, &[]), CrdSeedDecision::Seedless));
    }
    use crate::{
        crd::grid_network::{BudgetPolicyConfig, TenantBudgetConfig},
        swim::MemberRecord,
    };

    fn make_inference_provider(name: &str, network_ref: &str) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
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
            "apiVersion": "grid.praxis.fast/v1alpha1",
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
    // network_refs_from_agent_tool_provider
    // -----------------------------------------------------------------------

    #[test]
    fn agent_tool_provider_maps_to_owning_grid_network() {
        let atp = make_agent_tool_provider("mcp-server", "net-a", &["search"]);
        let name = ref_name(network_refs_from_agent_tool_provider(atp));
        assert_eq!(name, "net-a", "ObjectRef name must match gridNetworkRef");
    }

    #[test]
    fn agent_tool_provider_blank_network_ref_returns_none() {
        let atp = make_agent_tool_provider("mcp-blank", "", &["search"]);
        let refs = network_refs_from_agent_tool_provider(atp);
        assert!(
            refs.is_none(),
            "blank gridNetworkRef must return None (no spurious reconcile)"
        );
    }

    #[test]
    fn agent_tool_provider_whitespace_network_ref_returns_none() {
        let mut atp = make_agent_tool_provider("mcp-ws", "net-a", &["search"]);
        atp.spec.grid_network_ref = "   ".to_owned();
        let refs = network_refs_from_agent_tool_provider(atp);
        assert!(refs.is_none(), "whitespace-only gridNetworkRef must return None");
    }

    // -----------------------------------------------------------------------
    // determine_phase with membership seam
    // -----------------------------------------------------------------------

    fn base_network() -> GridNetwork {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "net" },
            "spec": { "seeds": [], "gridId": "test-id" }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    // -----------------------------------------------------------------------
    // reject_invalid_budget_policy
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn membership_writes_hold_until_released() {
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::new());
        let open = OperatorCtx::new(client.clone(), None, SignalMode::default());
        assert!(open.membership_hold().is_none(), "tests and static mode write at once");
        let held = OperatorCtx::new(client, None, SignalMode::default()).hold_membership();
        assert_eq!(held.membership_hold(), Some(Action::requeue(MEMBERSHIP_HOLD_REQUEUE)));
        held.release_membership();
        assert!(held.membership_hold().is_none(), "released");
    }

    #[test]
    fn a_keyless_network_releases_plaintext_only_when_no_network_declares_a_key() {
        let keyless = base_network();
        let mut keyed = base_network();
        keyed.spec.tls.swim_key_ref = Some(crate::crd::grid_network::SecretRef {
            name: "swim-key".to_owned(),
            namespace: "grid".to_owned(),
            key: None,
        });
        assert!(
            !declares_swim_key(std::slice::from_ref(&keyless)),
            "alone it may release"
        );
        assert!(declares_swim_key(&[keyless, keyed]), "a keyed sibling keeps the hold");
        assert!(!declares_swim_key(&[]), "no network declares one");
    }

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
                    signals_address: None,
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
                signals_address: None,
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
            tools: Vec::new(),
            backend_kind: "local".to_owned(),
            capacity_weight: 1,
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
            tools: Vec::new(),
            backend_kind: "local".to_owned(),
            capacity_weight: 1,
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
            "apiVersion": "grid.praxis.fast/v1alpha1",
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
            "apiVersion": "grid.praxis.fast/v1alpha1",
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
            "apiVersion": "grid.praxis.fast/v1alpha1",
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
            "apiVersion": "grid.praxis.fast/v1alpha1",
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
    #[expect(
        clippy::too_many_lines,
        reason = "this table proves current and stale withdrawal behavior for every provider phase"
    )]
    fn crdt_phase_from_provider_maps_all_variants() {
        assert_eq!(
            crdt_phase_from_provider(None, 1),
            crdt::ProviderPhase::Pending,
            "absent status → Pending"
        );
        let pending: InferenceProviderStatus = serde_json::from_value(serde_json::json!({
            "phase": "Pending", "observedGeneration": 3
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            crdt_phase_from_provider(Some(&pending), 3),
            crdt::ProviderPhase::Pending
        );
        let withdrawn: InferenceProviderStatus = serde_json::from_value(serde_json::json!({
            "phase": "Pending", "reason": "NoMatchingSites", "matchingSites": [], "observedGeneration": 3
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            crdt_phase_from_provider(Some(&withdrawn), 3),
            crdt::ProviderPhase::Unavailable,
            "current-generation selector withdrawal must not be advertised as routable capacity"
        );
        assert_eq!(
            crdt_phase_from_provider(Some(&withdrawn), 4),
            crdt::ProviderPhase::Pending,
            "stale withdrawal status must not withdraw a newer provider generation"
        );
        let available: InferenceProviderStatus = serde_json::from_value(serde_json::json!({
            "phase": "Available", "observedGeneration": 3
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            crdt_phase_from_provider(Some(&available), 3),
            crdt::ProviderPhase::Available
        );
        let degraded: InferenceProviderStatus = serde_json::from_value(serde_json::json!({
            "phase": "Degraded", "observedGeneration": 3
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            crdt_phase_from_provider(Some(&degraded), 3),
            crdt::ProviderPhase::Degraded
        );
        let unavailable: InferenceProviderStatus = serde_json::from_value(serde_json::json!({
            "phase": "Unavailable", "observedGeneration": 3
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            crdt_phase_from_provider(Some(&unavailable), 3),
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
            "apiVersion": "grid.praxis.fast/v1alpha1",
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
            tools: Vec::new(),
            backend_kind: "remote".to_owned(),
            capacity_weight: 1,
            phase,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            access_policy: crdt::ProviderAccessPolicy::default(),
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
                signals_address: None,
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
    fn staleness_override_dead_site_preserves_unavailable() {
        let provider = make_crdt_provider("site-west", crdt::ProviderPhase::Unavailable);
        let membership = make_swim_membership("site-west", MemberStatus::Dead);
        let result = apply_swim_staleness_override(&[provider], Some(&membership));
        assert_eq!(
            result.first().map(|p| &p.phase),
            Some(&crdt::ProviderPhase::Unavailable)
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
                    signals_address: None,
                },
                MemberRecord {
                    site_id: "site-east".to_owned(),
                    endpoint: "10.0.0.1:7946".to_owned(),
                    incarnation: 0,
                    status: MemberStatus::Alive,
                    age_secs: 0,
                    gateway_address: None,
                    site_cert_pem: None,
                    signals_address: None,
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
    fn persisted_grid_id_uses_spec_without_generating_a_fallback() {
        let network = base_network();
        assert_eq!(persisted_grid_id(&network), Some("test-id"));
    }

    #[test]
    fn persisted_grid_id_uses_status_when_spec_is_empty() {
        let mut network = base_network();
        network.spec.grid_id.clear();
        network.status = Some(GridNetworkStatus {
            grid_id: "persisted-id".to_owned(),
            ..GridNetworkStatus::default()
        });
        assert_eq!(persisted_grid_id(&network), Some("persisted-id"));
    }

    #[test]
    fn persisted_grid_id_is_none_without_spec_or_status_value() {
        let mut network = base_network();
        network.spec.grid_id.clear();
        assert_eq!(persisted_grid_id(&network), None);
    }

    #[test]
    fn withdrawal_finalizer_detection_ignores_unrelated_finalizers() {
        let mut network = base_network();
        network.metadata.finalizers = Some(vec!["other.example/finalizer".to_owned()]);
        assert!(!has_withdrawal_finalizer(&network));
        network
            .metadata
            .finalizers
            .get_or_insert_default()
            .push(GRID_NETWORK_WITHDRAWAL_FINALIZER.to_owned());
        assert!(has_withdrawal_finalizer(&network));
    }

    #[test]
    fn withdrawal_finalizer_updates_preserve_other_finalizers_and_are_idempotent() {
        let mut finalizers = vec!["other.example/finalizer".to_owned()];

        assert!(
            set_withdrawal_finalizer(&mut finalizers, true),
            "adding the withdrawal finalizer must change the list"
        );
        assert_eq!(
            finalizers,
            ["other.example/finalizer", GRID_NETWORK_WITHDRAWAL_FINALIZER,],
            "adding the withdrawal finalizer must preserve unrelated entries"
        );
        assert!(
            !set_withdrawal_finalizer(&mut finalizers, true),
            "adding an existing withdrawal finalizer must be idempotent"
        );

        assert!(
            set_withdrawal_finalizer(&mut finalizers, false),
            "removing the withdrawal finalizer must change the list"
        );
        assert_eq!(
            finalizers,
            ["other.example/finalizer"],
            "removing the withdrawal finalizer must preserve unrelated entries"
        );
        assert!(
            !set_withdrawal_finalizer(&mut finalizers, false),
            "removing an absent withdrawal finalizer must be idempotent"
        );
    }

    /// The operator self-signs only a grid with neither Secret, never over an existing CA.
    #[test]
    fn the_operator_never_replaces_an_existing_grid_ca() {
        assert_eq!(tls_secrets_action(true, true), TlsSecrets::Present);
        assert_eq!(tls_secrets_action(false, false), TlsSecrets::Create);
        assert_eq!(
            tls_secrets_action(true, false),
            TlsSecrets::Inconsistent,
            "an enrolled CA, identity gone"
        );
        assert_eq!(tls_secrets_action(false, true), TlsSecrets::Inconsistent);
    }

    /// The inconsistency warning fires on the transition, not on every reconcile.
    #[test]
    fn a_declared_trust_wakes_watchers_only_when_it_changes() {
        let (sender, mut changes) = tokio::sync::watch::channel(PeerTrustMode::Spiffe);
        assert!(!send_declared_trust(&sender, PeerTrustMode::Spiffe), "unchanged");
        assert!(!changes.has_changed().unwrap_or(true), "nobody woken");
        assert!(send_declared_trust(&sender, PeerTrustMode::Pin), "pin declared");
        assert!(changes.has_changed().unwrap_or(false), "the rotation loop wakes");
        assert_eq!(*changes.borrow_and_update(), PeerTrustMode::Pin);
    }

    #[test]
    fn an_inconsistent_grid_is_warned_once() {
        // Names no other test uses: the set is process-wide.
        let network = || ObjectRef::new("inconsistent-tls-test-warn-once");
        let other = || ObjectRef::new("inconsistent-tls-test-other");
        assert!(note_inconsistent(network(), true), "the first time");
        assert!(!note_inconsistent(network(), true), "not again");
        assert!(!note_inconsistent(network(), false), "fixed");
        assert!(note_inconsistent(network(), true), "and again after it recurs");
        assert!(note_inconsistent(other(), true), "per network");
        // Leave the process-wide set as it was.
        assert!(!note_inconsistent(network(), false) && !note_inconsistent(other(), false));
    }

    /// The identity status reports expiry and the renewal window, and an expired identity says how to recover.
    #[test]
    #[expect(clippy::expect_used, reason = "test")]
    fn the_identity_status_reports_expiry_and_the_renewal_window() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let csr = certs::generate_csr("east").expect("csr");
        let start = time::OffsetDateTime::now_utc().saturating_sub(time::Duration::days(1));
        let validity = certs::Validity {
            not_before: start,
            not_after: start.saturating_add(time::Duration::days(30)),
        };
        let leaf = certs::sign_csr(&ca, "east", &csr.csr_pem, validity).expect("leaf");

        let current = identity_status(&leaf.cert_pem, time::OffsetDateTime::now_utc(), true).expect("status");
        assert!(current.reason.is_empty(), "a current identity reports no reason");
        let (not_before, not_after) = certs::cert_validity(&leaf.cert_pem).expect("validity");
        let renew_after = crate::enroll::renew::renew_after(not_before, not_after);
        assert_eq!(
            current.rotate_after,
            renew_after
                .format(&time::format_description::well_known::Rfc3339)
                .expect("format")
        );
        assert_eq!(
            current.fingerprint,
            certs::canonical_fingerprint(&leaf.cert_pem).expect("fp")
        );

        let expired = identity_status(&leaf.cert_pem, not_after, true).expect("status");
        assert_eq!(expired.reason, IDENTITY_EXPIRED);
        assert!(expired.message.contains("re-enrolls"), "names the recovery");

        let pinned = identity_status(&leaf.cert_pem, time::OffsetDateTime::now_utc(), false).expect("status");
        assert!(pinned.rotate_after.is_empty(), "pin trust schedules no renewal");
        assert!(pinned.reason.is_empty(), "a current pinned identity is not degraded");
        assert!(pinned.message.contains("re-pin"), "names the manual step");
    }

    #[test]
    fn only_spiffe_trust_renews() {
        let modes = |trust| GridModes {
            signal: SignalMode::Gossip,
            trust,
        };
        assert!(
            !modes(PeerTrustMode::Pin).renews(),
            "a pinned peer refuses a renewed leaf"
        );
        assert!(modes(PeerTrustMode::Spiffe).renews());
        assert!(
            GridModes::WITHOUT_NETWORK.renews(),
            "no GridNetwork trusts by SPIFFE ID"
        );
    }

    #[test]
    fn an_unreadable_identity_degrades_with_its_recovery() {
        let status = unreadable_identity("tls.crt is not a certificate");
        assert_eq!(status.reason, IDENTITY_UNREADABLE);
        assert!(status.message.contains("re-enroll"), "names the recovery");
        assert!(status.not_after.is_empty() && status.fingerprint.is_empty());
    }

    #[test]
    fn a_site_reports_the_phase_its_status_shows_and_pending_before_any() {
        let site = |name: &str, phase: Option<&str>| -> GridSite {
            let mut object = serde_json::json!({
                "apiVersion": "grid.praxis.fast/v1alpha1",
                "kind": "GridSite",
                "metadata": { "name": name },
                "spec": { "gridNetworkRef": "grid" },
            });
            if let Some(phase) = phase
                && let Some(map) = object.as_object_mut()
            {
                map.insert("status".to_owned(), serde_json::json!({ "phase": phase }));
            }
            serde_json::from_value(object).unwrap_or_else(|_| std::process::abort())
        };
        let sites = [
            site("hub", Some("Active")),
            site("east", None),
            site("west", Some("Unreachable")),
        ];
        assert_eq!(
            site_phases(&sites).collect::<Vec<_>>(),
            [("hub", "Active"), ("east", "Pending"), ("west", "Unreachable")]
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
            identity: None,
            mount_reconciliation_status: Vec::new(),
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
    fn a_self_issued_certificate_names_this_site_not_the_network() {
        let network = base_network();
        assert_eq!(issued_site_name(&network, Some("east")).as_deref(), Some("east"));
        assert_eq!(
            issued_site_name(&network, None).as_deref(),
            Some("net"),
            "no site name falls back to the network"
        );
    }

    /// A network named like a Kubernetes object, not like a site, issues nothing.
    #[test]
    fn a_network_name_that_is_not_a_site_name_issues_no_identity() {
        // The name becomes the SPIFFE path segment and the subject organization. A
        // Kubernetes object name may carry dots and run past a DNS label, so the fallback
        // has to decline rather than mint an identity nothing can authorize.
        let mut network = base_network();
        network.metadata.name = Some("grid.example.internal".to_owned());
        assert_eq!(
            issued_site_name(&network, None),
            None,
            "a dotted network name is not a site name"
        );
        assert_eq!(
            issued_site_name(&network, Some("east")).as_deref(),
            Some("east"),
            "an explicit site name is still used"
        );
    }

    #[test]
    fn network_site_name_returns_metadata_name_when_present() {
        let network = base_network();
        let name = network_site_name(&network);
        assert_eq!(name, "net", "metadata.name must be returned verbatim when present");
    }

    #[test]
    fn network_site_name_falls_back_to_unknown_site_when_metadata_name_absent() {
        let network: GridNetwork = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
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
            signals_address: None,
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
    fn discovered_sites_carry_bare_site_id() {
        let snap = make_snapshot(vec![make_member("remote", "10.0.0.2:7946", MemberStatus::Alive)]);
        let sites = discovered_sites_from_swim("net", "local", &snap);
        let site = sites.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(site.name, "net-remote", "name is the network-prefixed composite");
        assert_eq!(
            site.site_id, "remote",
            "site_id is the bare SWIM id the poll path keys by"
        );
    }

    // -----------------------------------------------------------------------
    // peer_identities keying (grid#peer-identity: poll path keys by site_id)
    // -----------------------------------------------------------------------

    fn peer_grid_site(name: &str, site_id_annotation: Option<&str>, pins: &[&str]) -> GridSite {
        let mut metadata = serde_json::Map::new();
        metadata.insert("name".to_owned(), name.into());
        if let Some(id) = site_id_annotation {
            let mut annotations = serde_json::Map::new();
            annotations.insert(ANNOTATION_SITE_ID.to_owned(), id.into());
            metadata.insert("annotations".to_owned(), Value::Object(annotations));
            metadata.insert(
                "labels".to_owned(),
                serde_json::json!({ LABEL_AUTO_DISCOVERED: "true" }),
            );
        }
        let mut spec = serde_json::Map::new();
        spec.insert("gridNetworkRef".to_owned(), "net".into());
        if !pins.is_empty() {
            spec.insert("trust".to_owned(), serde_json::json!({ "canonicalFingerprints": pins }));
        }
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridSite",
            "metadata": Value::Object(metadata),
            "spec": Value::Object(spec),
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn peer_identities_keys_auto_discovered_by_site_id_annotation() {
        let sites = [peer_grid_site("net-remote", Some("remote"), &["pin-a"])];
        let identities = peer_identities(&sites, PeerTrustMode::Pin);
        assert!(
            identities.contains_key("remote"),
            "keyed by the bare site_id annotation"
        );
        assert!(
            !identities.contains_key("net-remote"),
            "never keyed by the prefixed name"
        );
    }

    #[test]
    fn peer_identities_falls_back_to_name_for_local_site() {
        let sites = [peer_grid_site("site-a", None, &["pin-a"])];
        let identities = peer_identities(&sites, PeerTrustMode::Pin);
        assert!(
            identities.contains_key("site-a"),
            "a bare-named site falls back to metadata.name"
        );
    }

    #[test]
    fn peer_identities_blank_annotation_falls_back_to_name() {
        let sites = [peer_grid_site("site-a", Some("   "), &["pin-a"])];
        let identities = peer_identities(&sites, PeerTrustMode::Pin);
        assert!(
            identities.contains_key("site-a"),
            "a blank annotation is ignored, name is used"
        );
    }

    #[test]
    fn peer_identities_prefers_pinned_record_on_collision() {
        // Two objects name one site: an empty auto-discovered stub and a pinned
        // object. The pinned one wins in either order, so it is never shadowed.
        let empty = peer_grid_site("net-remote", Some("remote"), &[]);
        let pinned = peer_grid_site("remote", None, &["pin-a"]);
        for order in [vec![empty.clone(), pinned.clone()], vec![pinned, empty]] {
            let identities = peer_identities(&order, PeerTrustMode::Pin);
            let record = identities.get("remote").unwrap_or_else(|| std::process::abort());
            assert_eq!(
                record.pins,
                vec!["pin-a".to_owned()],
                "the pinned record wins on collision"
            );
        }
    }

    #[test]
    fn peer_identities_unblocks_poll_for_pinned_auto_discovered_peer() {
        // The bug: an auto-discovered pinned peer was refused because the poll
        // path looked it up by the bare site_id and found nothing.
        let sites = [peer_grid_site("net-remote", Some("remote"), &["pin-a"])];
        let identities = signals::PeerIdentities::new();
        identities.set(peer_identities(&sites, PeerTrustMode::Pin));
        assert!(
            !identities.refuses("remote"),
            "the poll path reaches the peer by its bare site_id"
        );
        assert_eq!(
            identities.pins_for("remote"),
            vec!["pin-a".to_owned()],
            "pins resolve under the bare id"
        );
    }

    #[test]
    fn peer_identities_keeps_unpinned_peer_refused() {
        // Membership is not trust: an un-pinned peer stays refused even when it
        // is now keyed correctly.
        let sites = [peer_grid_site("net-remote", Some("remote"), &[])];
        let identities = signals::PeerIdentities::new();
        identities.set(peer_identities(&sites, PeerTrustMode::Pin));
        assert!(
            identities.refuses("remote"),
            "an un-pinned peer stays refused (membership != trust)"
        );
    }

    #[test]
    fn peer_identities_pinned_stub_outranks_unpinned_enrolled() {
        let stub = peer_grid_site("net-remote", Some("remote"), &["pin-a"]);
        let enrolled = peer_grid_site("remote", None, &[]);
        for order in [vec![stub.clone(), enrolled.clone()], vec![enrolled, stub]] {
            let identities = peer_identities(&order, PeerTrustMode::Pin);
            let record = identities.get("remote").unwrap_or_else(|| std::process::abort());
            assert_eq!(record.pins, vec!["pin-a".to_owned()]);
        }
    }

    #[test]
    fn discovery_skips_ids_the_ca_could_not_issue() {
        let long = "a".repeat(240);
        let snap = make_snapshot(
            ["Site_B", "site-b", long.as_str(), "-x"]
                .into_iter()
                .map(|id| make_member(id, "10.0.0.2:7946", MemberStatus::Alive))
                .collect(),
        );
        let ids: Vec<_> = discovered_sites_from_swim("net", "local", &snap)
            .into_iter()
            .map(|site| site.site_id)
            .collect();
        assert_eq!(ids, vec!["site-b".to_owned()]);
    }

    #[test]
    fn discovered_site_spec_plaintext_has_no_server_name() {
        let site = DiscoveredSite {
            name: "net-remote".to_owned(),
            site_id: "remote".to_owned(),
            grid_network_ref: "net".to_owned(),
            egress_address: "10.0.0.2:19080".to_owned(),
            site_cert_pem: None,
        };
        let spec = discovered_site_spec(&site, "net", true);
        assert_eq!(
            spec.pointer("/spec/egress/tls"),
            Some(&serde_json::json!({ "mode": "Plaintext" }))
        );
    }

    #[test]
    fn a_hand_made_site_cannot_claim_another_sites_key_by_annotation() {
        let mut impostor = peer_grid_site("impostor", Some("victim"), &["pin-x"]);
        impostor.metadata.labels = None;
        let victim = peer_grid_site("victim", None, &[]);
        assert_eq!(peer_site_key(&impostor), Some(("impostor".to_owned(), true)));
        for (trust, order) in [
            (PeerTrustMode::Pin, vec![impostor.clone(), victim.clone()]),
            (PeerTrustMode::Spiffe, vec![victim, impostor]),
        ] {
            let identities = peer_identities(&order, trust);
            let record = identities.get("victim").unwrap_or_else(|| std::process::abort());
            assert!(
                record.pins.is_empty(),
                "{trust:?}: the impostor's pins never land on victim"
            );
            assert!(
                identities.contains_key("impostor"),
                "{trust:?}: it keys by its own name"
            );
        }
    }

    #[test]
    fn an_auto_discovered_stub_still_keys_by_its_bare_id() {
        let stub = peer_grid_site("net-remote", Some("remote"), &[]);
        assert_eq!(peer_site_key(&stub), Some(("remote".to_owned(), false)));
    }

    /// A stub for `site`, absent since `since` when given.
    fn stub(site: &str, since: Option<&str>) -> GridSite {
        let mut stub = peer_grid_site(&format!("net-{site}"), Some(site), &[]);
        stub.status = since.map(|since| GridSiteStatus {
            absent_since: Some(since.to_owned()),
            ..GridSiteStatus::default()
        });
        stub
    }

    fn at(rfc3339: &str) -> time::OffsetDateTime {
        time::OffsetDateTime::parse(rfc3339, &time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| std::process::abort())
    }

    /// [`stub_gc`] at noon for a stub absent since `since`, with a one-hour TTL, or a year when `ttl` is false.
    fn gc(since: Option<&str>, vouched: bool, ttl: bool) -> StubGc {
        let ttl = Duration::from_secs(if ttl { 3600 } else { 365 * 24 * 3600 });
        let vouch = if vouched { Vouch::Vouched } else { Vouch::Absent };
        stub_gc(&stub("a", since), vouch, ttl, at("2026-10-02T12:00:00Z"))
    }

    #[test]
    fn the_absence_clock_starts_when_gossip_stops_vouching_and_clears_on_return() {
        assert_eq!(gc(None, true, true), StubGc::Keep, "live");
        assert_eq!(gc(None, false, true), StubGc::MarkAbsent, "just left");
        assert_eq!(
            gc(Some("2026-10-02T09:00:00Z"), true, true),
            StubGc::ClearAbsent,
            "back"
        );
        assert_eq!(
            gc(Some("yesterday"), false, true),
            StubGc::MarkAbsent,
            "bad clock restarts"
        );
    }

    #[test]
    fn nothing_is_marked_or_collected_before_a_full_verification_window() {
        let snapshot = make_snapshot(vec![make_member("here", "10.0.0.2:7946", MemberStatus::Alive)]);
        let fresh = vouched_sites(&snapshot, STUB_GC_WARMUP.saturating_sub(Duration::from_secs(1)));
        assert_eq!(fresh.of("here"), Vouch::Vouched, "presence is evidence at any uptime");
        assert_eq!(fresh.of("gone"), Vouch::Unchecked);
        assert_eq!(vouched_sites(&snapshot, STUB_GC_WARMUP).of("gone"), Vouch::Absent);

        let now = at("2026-10-02T12:00:00Z");
        let hour = Duration::from_secs(3600);
        let old = stub("gone", Some("2026-01-01T00:00:00Z"));
        assert_eq!(
            stub_gc(&stub("gone", None), Vouch::Unchecked, hour, now),
            StubGc::Keep,
            "no clock starts"
        );
        assert_eq!(
            stub_gc(&old, Vouch::Unchecked, hour, now),
            StubGc::Keep,
            "no collection"
        );
    }

    #[test]
    fn a_pass_never_collects_most_stubs_at_once() {
        let now = at("2026-10-02T12:00:00Z");
        let hour = Duration::from_secs(3600);
        let gone = |n: usize| (0..n).map(|i| stub(&format!("g{i}"), Some("2026-10-02T09:00:00Z")));
        let live = |n: usize| (0..n).map(|i| stub(&format!("l{i}"), None));
        let snapshot = make_snapshot(
            (0..20)
                .map(|i| make_member(&format!("l{i}"), "10.0.0.2:7946", MemberStatus::Alive))
                .collect(),
        );
        let vouching = vouched_sites(&snapshot, STUB_GC_WARMUP);
        let collected = |stubs: &[GridSite]| {
            plan_stub_gc(stubs, &vouching, hour, now)
                .iter()
                .filter(|(_, _, gc)| *gc == StubGc::Collect)
                .count()
        };
        let partition: Vec<GridSite> = gone(9).chain(live(1)).collect();
        assert_eq!(collected(&partition), 0, "9 of 10 departing at once is held");
        let lone: Vec<GridSite> = gone(1).collect();
        assert_eq!(collected(&lone), 1, "a small grid still collects");
        let floor: Vec<GridSite> = gone(COLLECT_BRAKE_FLOOR).chain(live(1)).collect();
        assert_eq!(collected(&floor), COLLECT_BRAKE_FLOOR, "up to the floor collects");
        let minority: Vec<GridSite> = gone(9).chain(live(11)).collect();
        assert_eq!(collected(&minority), 9, "under half collects");
        let held = counted_stubs(&plan_stub_gc(&partition, &vouching, hour, now));
        assert_eq!(held.len(), 10, "held stubs still count against the cap");
    }

    #[test]
    fn a_long_partition_never_collects_every_stub_of_several() {
        let now = at("2026-10-02T12:00:00Z");
        let snapshot = make_snapshot(Vec::new());
        let vouching = vouched_sites(&snapshot, STUB_GC_WARMUP);
        let every: Vec<GridSite> = (0..COLLECT_BRAKE_FLOOR)
            .map(|i| stub(&format!("g{i}"), Some("2026-10-02T09:00:00Z")))
            .collect();
        let plan = plan_stub_gc(&every, &vouching, Duration::from_secs(3600), now);
        assert!(plan.iter().all(|(_, _, gc)| *gc == StubGc::Keep), "every stub is held");
    }

    #[test]
    fn a_departed_stub_is_collected_only_past_the_ttl() {
        assert_eq!(
            gc(Some("2026-10-02T11:30:00Z"), false, true),
            StubGc::Keep,
            "inside ttl"
        );
        assert_eq!(
            gc(Some("2026-10-02T10:59:59Z"), false, true),
            StubGc::Collect,
            "past ttl"
        );
        assert_eq!(gc(Some("2026-01-01T00:00:00Z"), false, false), StubGc::Keep, "no ttl");
    }

    #[test]
    fn declared_sites_are_never_stubs() {
        assert!(is_stub(&stub("a", None)));
        assert!(!is_stub(&peer_grid_site("a", None, &[])), "declared, no label");
        let mut annotated = peer_grid_site("net-a", Some("a"), &[]);
        annotated.metadata.labels = None;
        assert!(
            !is_stub(&annotated),
            "an annotation without the label is a declared site"
        );
    }

    #[test]
    fn a_new_peer_is_admitted_once_a_stale_stub_is_collected() {
        let now = at("2026-10-02T12:00:00Z");
        let hour = Duration::from_secs(3600);
        let mut stubs: Vec<GridSite> = (1..MAX_AUTO_CREATED_SITES)
            .map(|i| stub(&format!("s{i}"), None))
            .collect();
        stubs.push(stub("gone", Some("2026-10-02T09:00:00Z")));
        let snapshot = make_snapshot(
            (1..MAX_AUTO_CREATED_SITES)
                .map(|i| make_member(&format!("s{i}"), "10.0.0.2:7946", MemberStatus::Alive))
                .collect(),
        );
        let vouching = vouched_sites(&snapshot, STUB_GC_WARMUP);
        let mut counted = counted_stubs(&plan_stub_gc(&stubs, &vouching, hour, now));
        assert!(!counted.contains("net-gone"), "a collected stub no longer counts");
        assert!(counted.contains("net-s1"), "a live stub still counts");
        assert!(admit_stub(&mut counted, "net-new"), "the freed slot admits a new peer");
    }

    #[test]
    fn peer_identities_spiffe_keys_discovered_peer_by_bare_id() {
        let sites = [peer_grid_site("net-remote", Some("remote"), &["pin-a"])];
        let identities = signals::PeerIdentities::new();
        identities.set(peer_identities(&sites, PeerTrustMode::Spiffe));
        assert!(
            identities.labels_for("remote").is_some(),
            "poll and serve find it by bare id"
        );
        assert!(identities.pins_for("remote").is_empty(), "spiffe mode carries no pins");
    }

    #[test]
    fn peer_identities_spiffe_enrolled_outranks_discovered_stub() {
        let mut stub = peer_grid_site("net-remote", Some("remote"), &[]);
        stub.metadata.labels = Some(BTreeMap::from([("from".to_owned(), "stub".to_owned())]));
        let mut enrolled = peer_grid_site("remote", None, &[]);
        enrolled.metadata.labels = Some(BTreeMap::from([("from".to_owned(), "enrolled".to_owned())]));
        for order in [vec![stub.clone(), enrolled.clone()], vec![enrolled, stub]] {
            let identities = peer_identities(&order, PeerTrustMode::Spiffe);
            let record = identities.get("remote").unwrap_or_else(|| std::process::abort());
            assert_eq!(record.labels.get("from").map(String::as_str), Some("enrolled"));
        }
    }

    #[test]
    fn discovery_creates_no_stub_past_the_cap_but_keeps_reconciling_existing_ones() {
        let mut stubs: BTreeSet<String> = (0..MAX_AUTO_CREATED_SITES).map(|i| format!("net-s{i}")).collect();
        assert!(admit_stub(&mut stubs, "net-s0"), "an existing stub still reconciles");
        assert!(!admit_stub(&mut stubs, "net-new"), "no new stub past the cap");
        assert_eq!(stubs.len(), MAX_AUTO_CREATED_SITES);

        let mut room = BTreeSet::new();
        assert!(admit_stub(&mut room, "net-a"));
        assert!(room.contains("net-a"), "an admitted stub counts against the cap");
    }

    #[test]
    fn a_second_identical_pass_logs_nothing_new() {
        let logged = ChangeLog::default();
        assert!(
            logged.changed("capacity/net/qwen3", "1".to_owned()),
            "first publish is news"
        );
        assert!(
            !logged.changed("capacity/net/qwen3", "1".to_owned()),
            "an identical pass is not"
        );
        assert!(
            logged.changed("capacity/net/qwen3", "2".to_owned()),
            "a capacity change is"
        );
        assert!(
            logged.changed("capacity/net/other", "2".to_owned()),
            "keys are independent"
        );
        let keep = std::collections::HashSet::from(["capacity/net/other".to_owned()]);
        logged.retain_under("capacity/net/", &keep);
        assert!(
            logged.changed("capacity/net/qwen3", "2".to_owned()),
            "a departed key is forgotten"
        );
        assert!(
            !logged.changed("capacity/net/other", "2".to_owned()),
            "a kept key is not"
        );
    }

    #[test]
    fn only_a_new_stub_or_a_changed_egress_or_cert_is_news() {
        let site = DiscoveredSite {
            name: "net-remote".to_owned(),
            site_id: "remote".to_owned(),
            grid_network_ref: "net".to_owned(),
            egress_address: "10.0.0.2:19080".to_owned(),
            site_cert_pem: Some("CERT".to_owned()),
        };
        let mut applied: GridSite =
            serde_json::from_value(discovered_site_spec(&site, "net", false)).unwrap_or_else(|_| std::process::abort());
        applied.status = Some(GridSiteStatus {
            public_cert_pem: Some("CERT".to_owned()),
            ..GridSiteStatus::default()
        });
        assert!(stub_changed(None, &site), "created");
        assert!(!stub_changed(Some(&applied), &site), "an idle pass");
        let moved = DiscoveredSite {
            egress_address: "10.0.0.3:19080".to_owned(),
            ..site.clone()
        };
        assert!(stub_changed(Some(&applied), &moved), "egress changed");
        let renewed = DiscoveredSite {
            site_cert_pem: Some("NEW".to_owned()),
            ..site
        };
        assert!(stub_changed(Some(&applied), &renewed), "cert changed");
    }

    #[test]
    fn discovered_site_spec_names_the_identity_the_probe_verifies() {
        let site = DiscoveredSite {
            name: "net-remote".to_owned(),
            site_id: "remote".to_owned(),
            grid_network_ref: "net".to_owned(),
            egress_address: "10.0.0.2:19080".to_owned(),
            site_cert_pem: None,
        };
        let spec = discovered_site_spec(&site, "net", false);
        assert_eq!(
            spec.pointer("/spec/egress/tls/serverName").and_then(Value::as_str),
            Some("remote.grid.internal")
        );
        assert_eq!(
            spec.pointer("/metadata/annotations")
                .and_then(|a| a.get(ANNOTATION_SITE_ID))
                .and_then(Value::as_str),
            Some("remote")
        );
        let applied: GridSite = serde_json::from_value(spec).unwrap_or_else(|_| std::process::abort());
        assert_eq!(peer_site_key(&applied), Some(("remote".to_owned(), false)));
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
            signals_address: None,
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
            signals_address: None,
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
            signals_address: None,
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
            signals_address: None,
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
            signals_address: None,
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
            "apiVersion": "grid.praxis.fast/v1alpha1",
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
    // diff_seed_sets
    // -----------------------------------------------------------------------

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap_or_else(|_| std::process::abort())
    }

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
            provider_hop_endpoints: Vec::new(),
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

    fn make_serving_tls() -> crate::crd::grid_network::TlsConfig {
        crate::crd::grid_network::TlsConfig {
            ca_secret_ref: Some(crate::crd::grid_network::SecretRef {
                name: "grid-ca".to_owned(),
                namespace: "praxis-system".to_owned(),
                key: None,
            }),
            site_secret_ref: Some(crate::crd::grid_network::SecretRef {
                name: "grid-site".to_owned(),
                namespace: "praxis-system".to_owned(),
                key: None,
            }),
            swim_key_ref: None,
        }
    }

    #[test]
    fn grid_serving_tls_is_a_chart_owned_requirement_without_consumer_candidates() {
        let gw = make_gw_ref("inference-gw", "praxis-system");
        let cc = make_consumer_config("consumer-config");
        let rendered = consumer_config::ConsumerRenderResult {
            config_yaml: "listeners: []".to_owned(),
            requirements: Vec::new(),
        };
        let requirements =
            delegated_mount_requirements_document(&rendered, "production", &gw, &cc, &make_serving_tls(), true)
                .unwrap_or_else(|_| std::process::abort());

        assert_eq!(requirements.requirements.len(), 2);
        assert!(
            requirements
                .requirements
                .iter()
                .all(|requirement| { requirement.purpose == consumer_config::MountPurpose::GridServingTls })
        );
        let desired = gateway_mounts::desired_mounts(&requirements).unwrap_or_else(|_| std::process::abort());
        assert!(desired.is_empty());
        assert!(requirements.requirements.iter().any(|requirement| {
            requirement.secret.name == "grid-ca"
                && requirement
                    .items
                    .iter()
                    .any(|item| item.path == "/etc/praxis/tls/ca.crt")
        }));
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "the chart TLS fixture exercises every required projected file"
    )]
    fn chart_managed_serving_tls_must_match_the_secret_refs_and_files() {
        let deployment: Deployment = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "inference-gw"},
            "spec": {"template": {"spec": {
                "containers": [{"name": "praxis", "volumeMounts": [
                    {"name": "tls", "mountPath": "/etc/praxis/tls", "readOnly": true}
                ]}],
                "volumes": [{"name": "tls", "projected": {"sources": [
                    {"secret": {"name": "grid-site", "items": [
                        {"key": "tls.crt", "path": "tls.crt"},
                        {"key": "tls.key", "path": "tls.key"}
                    ]}},
                    {"secret": {"name": "grid-ca", "items": [
                        {"key": "ca.crt", "path": "ca.crt"}
                    ]}}
                ]}}]
            }}}
        }))
        .unwrap_or_else(|_| std::process::abort());
        let requirements = delegated_mount_requirements_document(
            &consumer_config::ConsumerRenderResult {
                config_yaml: "listeners: []".to_owned(),
                requirements: Vec::new(),
            },
            "production",
            &make_gw_ref("inference-gw", "praxis-system"),
            &make_consumer_config("consumer-config"),
            &make_serving_tls(),
            true,
        )
        .unwrap_or_else(|_| std::process::abort());
        let delegation = MountReconciliation {
            enabled: true,
            deployment_name: Some("inference-gw".to_owned()),
            container_name: "praxis".to_owned(),
        };

        validate_chart_managed_serving_tls(&requirements, &deployment, &delegation)
            .unwrap_or_else(|_| std::process::abort());
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
    fn failed_overlay_status_preserves_last_distributed_revision() {
        let gw = make_gw_ref("gw", "grid-system");
        let prior = rendered_overlay_status(&gw);
        let mut network = base_network();
        network.status = Some(GridNetworkStatus {
            overlay_status: vec![prior.clone()],
            ..GridNetworkStatus::default()
        });

        let status = retained_overlay_status(&network, &gw, 5, None, "OverlayApplyFailed", "overlay apply failed");

        assert_eq!(status.phase, OverlayPhase::Retained);
        assert_eq!(status.rendered_revision, prior.rendered_revision);
        assert_eq!(status.distributed_revision, prior.distributed_revision);
        assert_eq!(status.content_digest, prior.content_digest);
        assert_eq!(status.config_map_resource_version, prior.config_map_resource_version);
        assert_eq!(status.candidate_count, prior.candidate_count);
        assert_eq!(status.rendered_at, prior.rendered_at);
        assert_eq!(status.reason, "OverlayApplyFailed");
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

        let status = retained_overlay_status(&network, &gw, 2, None, "OverlayRenderFailed", "overlay render failed");

        assert_eq!(status.phase, OverlayPhase::Error);
        assert!(status.rendered_revision.is_empty());
        assert!(status.distributed_revision.is_empty());
        assert_eq!(status.reason, "OverlayRenderFailed");
    }

    // -----------------------------------------------------------------------
    // keep_rendered_at: status-only churn must not retrigger reconcile
    // -----------------------------------------------------------------------

    const FRESH: &str = "2026-10-01T12:00:00Z";

    fn only_overlay(status: &GridNetworkStatus) -> &OverlayRevisionStatus {
        status.overlay_status.first().unwrap_or_else(|| std::process::abort())
    }

    fn network_with_overlay(prior: OverlayRevisionStatus) -> GridNetwork {
        let mut network = base_network();
        network.status = Some(GridNetworkStatus {
            overlay_status: vec![prior],
            ..GridNetworkStatus::default()
        });
        network
    }

    fn desired_with_overlay(network: &GridNetwork, overlay: OverlayRevisionStatus) -> GridNetworkStatus {
        GridNetworkStatus {
            overlay_status: keep_rendered_at(network.status.as_ref(), vec![overlay]),
            ..network.status.clone().unwrap_or_default()
        }
    }

    #[test]
    fn unchanged_distributed_overlay_writes_no_status() {
        let gw = make_gw_ref("gw", "grid-system");
        let prior = rendered_overlay_status(&gw);
        let network = network_with_overlay(prior.clone());
        let rerendered = OverlayRevisionStatus {
            rendered_at: FRESH.to_owned(),
            ..prior.clone()
        };

        let desired = desired_with_overlay(&network, rerendered);

        assert_eq!(only_overlay(&desired).rendered_at, prior.rendered_at);
        assert!(!grid_network_status_needs_update(network.status.as_ref(), &desired));
    }

    #[test]
    fn content_change_advances_rendered_at() {
        let gw = make_gw_ref("gw", "grid-system");
        let prior = rendered_overlay_status(&gw);
        let network = network_with_overlay(prior.clone());
        let new_digest = OverlayRevisionStatus {
            content_digest: "b".repeat(64),
            rendered_revision: "b".repeat(64),
            distributed_revision: "b".repeat(64),
            rendered_at: FRESH.to_owned(),
            ..prior.clone()
        };
        let new_resource_version = OverlayRevisionStatus {
            config_map_resource_version: "43".to_owned(),
            rendered_at: FRESH.to_owned(),
            ..prior.clone()
        };
        let new_reason = OverlayRevisionStatus {
            phase: OverlayPhase::Error,
            reason: "OverlayApplyFailed".to_owned(),
            message: "apply failed".to_owned(),
            rendered_at: FRESH.to_owned(),
            ..prior
        };

        for changed in [new_digest, new_resource_version, new_reason] {
            let desired = desired_with_overlay(&network, changed);
            assert_eq!(only_overlay(&desired).rendered_at, FRESH);
            assert!(grid_network_status_needs_update(network.status.as_ref(), &desired));
        }
    }

    #[test]
    fn generation_bump_keeps_rendered_at() {
        let gw = make_gw_ref("gw", "grid-system");
        let prior = rendered_overlay_status(&gw);
        let network = network_with_overlay(prior.clone());
        let bumped = OverlayRevisionStatus {
            rendered_at: FRESH.to_owned(),
            observed_generation: prior.observed_generation + 1,
            ..prior.clone()
        };

        let desired = desired_with_overlay(&network, bumped);

        assert_eq!(only_overlay(&desired).rendered_at, prior.rendered_at);
        assert!(grid_network_status_needs_update(network.status.as_ref(), &desired));
    }

    #[test]
    fn first_render_uses_fresh_rendered_at() {
        let gw = make_gw_ref("gw", "grid-system");
        let fresh = OverlayRevisionStatus {
            rendered_at: FRESH.to_owned(),
            ..rendered_overlay_status(&gw)
        };
        assert_eq!(
            keep_rendered_at(None, vec![fresh])
                .first()
                .map(|e| e.rendered_at.as_str()),
            Some(FRESH)
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
                    excluded: Vec::new(),
                    selection_policy: None,
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
    #[expect(clippy::expect_used, reason = "serializing a fixed status fixture cannot fail")]
    fn requirements_only_status_does_not_claim_a_deployment_is_ready() {
        let gw = make_gw_ref("inference-gw", "praxis-system");
        let status = requirements_rendered_status(&gw, "requirements-digest".to_owned(), 7);
        assert_eq!(status.phase, MountReconciliationPhase::RequirementsRendered);
        assert_eq!(status.requirements_revision, "requirements-digest");
        assert!(status.applied_revision.is_empty());
        assert!(status.deployment_name.is_none());
        assert_eq!(status.observed_generation, 7);
        let serialized = serde_json::to_string(&status).expect("serialize status");
        assert!(!serialized.contains("deploymentName"));
        assert!(!serialized.contains("private-key"));
    }

    #[test]
    fn secret_namespace_mismatch_requires_a_spec_change() {
        let gateway = make_gw_ref("inference-gw", "praxis-system");
        for (reason, expected_phase) in [
            ("MissingSecret", MountReconciliationPhase::WaitingForSecret),
            ("MissingSecretKey", MountReconciliationPhase::WaitingForSecret),
            ("SecretNamespaceMismatch", MountReconciliationPhase::Error),
        ] {
            let error = OperatorError::MountReconciliation(mount_failure(reason, "test failure"));
            let status = mount_reconciliation_status_error(&gateway, None, &error, 7);
            assert_eq!(status.phase, expected_phase, "incorrect phase for {reason}");
            assert_eq!(status.reason, reason, "status must retain the specific failure reason");
        }
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "the rollout status transitions are exercised against one Deployment fixture"
    )]
    #[expect(
        clippy::expect_used,
        reason = "deserializing a fixed Deployment fixture must succeed"
    )]
    fn delegated_gateway_readiness_requires_current_generation_and_all_replicas() {
        let mut deployment: Deployment = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "inference-gw", "generation": 5},
            "spec": {
                "replicas": 2,
                "selector": {"matchLabels": {"app": "inference-gw"}},
                "template": {
                    "metadata": {"labels": {"app": "inference-gw"}},
                    "spec": {"containers": [{"name": "praxis", "image": "praxis"}]}
                }
            },
            "status": {"observedGeneration": 5, "replicas": 2, "updatedReplicas": 2, "availableReplicas": 2}
        }))
        .expect("valid deployment fixture");
        assert!(deployment_rollout_ready(&deployment));

        if let Some(status) = deployment.status.as_mut() {
            status.available_replicas = Some(1);
        }
        assert!(!deployment_rollout_ready(&deployment));
        if let Some(status) = deployment.status.as_mut() {
            status.available_replicas = Some(2);
            status.observed_generation = Some(4);
        }
        assert!(!deployment_rollout_ready(&deployment));
        if let Some(status) = deployment.status.as_mut() {
            status.observed_generation = Some(5);
            status.replicas = Some(3);
        }
        assert!(
            !deployment_rollout_ready(&deployment),
            "an old surge replica must block rollout completion"
        );
        if let Some(status) = deployment.status.as_mut() {
            status.replicas = Some(2);
            status.unavailable_replicas = Some(1);
        }
        assert!(
            !deployment_rollout_ready(&deployment),
            "unavailable replicas must block rollout completion"
        );
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the Deployment fixture keeps the config volume and target container together"
    )]
    fn delegated_config_source_fixture() -> Value {
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "gateway",
                "annotations": {
                    (MOUNT_OPT_IN_ANNOTATION): "enabled",
                    (MOUNT_NETWORK_ANNOTATION): "production",
                    (MOUNT_GATEWAY_ANNOTATION): "gateway"
                }
            },
            "spec": {
                "selector": {"matchLabels": {"app": "gateway"}},
                "template": {
                    "metadata": {"labels": {"app": "gateway"}},
                    "spec": {
                        "containers": [{
                            "name": "praxis",
                            "image": "praxis:test",
                            "volumeMounts": [{"name": "config", "mountPath": "/etc/praxis", "readOnly": true}]
                        }],
                        "volumes": [{
                            "name": "config",
                            "configMap": {
                                "name": "praxis-consumer-config",
                                "items": [{"key": "praxis.yaml", "path": "praxis.yaml"}]
                            }
                        }]
                    }
                }
            }
        })
    }

    fn delegated_config_source_check(value: Value) -> Result<bool, OperatorError> {
        let deployment: Deployment = serde_json::from_value(value).unwrap_or_else(|_| std::process::abort());
        let delegation = MountReconciliation {
            enabled: true,
            deployment_name: Some("gateway".to_owned()),
            container_name: "praxis".to_owned(),
        };
        validate_deployment_delegation(
            &deployment,
            "production",
            &make_gw_ref("gateway", "praxis-system"),
            &delegation,
            "praxis-consumer-config",
        )
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "config source validation cases share one Deployment fixture"
    )]
    fn delegated_config_source_requires_the_expected_config_map_and_projection() {
        let valid = delegated_config_source_fixture();
        assert!(matches!(delegated_config_source_check(valid.clone()), Ok(false)));
        let mut alternate = valid.clone();
        let source_name = alternate
            .pointer_mut("/spec/template/spec/volumes/0/configMap/name")
            .unwrap_or_else(|| std::process::abort());
        *source_name = json!(alternate_consumer_config_map_name("praxis-consumer-config"));
        assert!(
            matches!(delegated_config_source_check(alternate), Ok(false)),
            "a Grid-managed alternate config slot must remain a valid delegated source"
        );

        for (pointer, replacement) in [
            (
                "/spec/template/spec/volumes/0/configMap/name",
                json!("old-praxis-config"),
            ),
            ("/spec/template/spec/containers/0/volumeMounts", json!([])),
            (
                "/spec/template/spec/volumes/0/configMap/items/0/key",
                json!("other.yaml"),
            ),
            (
                "/spec/template/spec/volumes/0/configMap/items/0/path",
                json!("other.yaml"),
            ),
        ] {
            let mut invalid = valid.clone();
            let target = invalid.pointer_mut(pointer).unwrap_or_else(|| std::process::abort());
            *target = replacement;
            assert!(
                matches!(
                    delegated_config_source_check(invalid),
                    Err(OperatorError::MountReconciliation(failure)) if failure.reason == "ConfigSourceMismatch"
                ),
                "invalid config source at {pointer} must fail closed"
            );
        }
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one patch must carry config, Secret volume, revision, and resourceVersion together"
    )]
    fn config_and_secret_projection_switch_in_one_version_guarded_patch() {
        let mut fixture = delegated_config_source_fixture();
        let metadata = fixture
            .get_mut("metadata")
            .and_then(Value::as_object_mut)
            .unwrap_or_else(|| std::process::abort());
        metadata.insert("resourceVersion".to_owned(), json!("42"));
        let deployment: Deployment = serde_json::from_value(fixture).unwrap_or_else(|_| std::process::abort());
        let base = "praxis-consumer-config";
        let alternate = inactive_consumer_config_map_name(base, base);
        assert_eq!(
            inactive_consumer_config_map_name(base, &alternate),
            base,
            "the next complete rollout reuses the now-inactive base slot"
        );
        let patch = staged_config_mount_patch(
            vec![json!({"name": "grid-mount-ca", "projected": {"sources": [{"secret": {"name": "new-ca"}}]}})],
            &[json!({"name": "grid-mount-ca", "mountPath": "/etc/praxis/tls"})],
            "config",
            &alternate,
            "praxis",
            &json!({OWNED_MOUNTS_ANNOTATION: "[\"grid-mount-ca\"]"}),
            &json!({MOUNT_REVISION_ANNOTATION: "mount-2", CONFIG_REVISION_ANNOTATION: "config-2"}),
        );
        let guarded = guarded_deployment_patch(patch, &deployment).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            guarded.pointer("/metadata/resourceVersion"),
            Some(&json!("42")),
            "a concurrent Deployment writer must cause the whole stage to conflict"
        );
        let mut unversioned = deployment;
        unversioned.metadata.resource_version = None;
        assert!(
            matches!(
                guarded_deployment_patch(guarded.clone(), &unversioned),
                Err(OperatorError::MountReconciliation(failure)) if failure.reason == "DeploymentInvalid"
            ),
            "the operator must not patch a Deployment without a concurrency precondition"
        );
        let volumes = guarded
            .pointer("/spec/template/spec/volumes")
            .and_then(Value::as_array)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            volumes.iter().any(|volume| {
                volume.get("name") == Some(&json!("config"))
                    && volume.pointer("/configMap/name") == Some(&json!(alternate))
            }),
            "the new Pod revision must mount the inactive config slot"
        );
        assert!(
            volumes
                .iter()
                .any(|volume| volume.get("name") == Some(&json!("grid-mount-ca"))),
            "the matching Secret projection must be in the same Pod-template patch"
        );
        assert_eq!(
            guarded.pointer(&format!(
                "/spec/template/metadata/annotations/{}",
                MOUNT_REVISION_ANNOTATION.replace('/', "~1")
            )),
            Some(&json!("mount-2")),
            "the staged Pod revision must carry the matching mount revision"
        );
    }

    #[expect(
        clippy::too_many_lines,
        reason = "keep the preserved Deployment and all of its consumers visible as one regression fixture"
    )]
    fn mount_ownership_test_deployment_value() -> Value {
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "gateway",
                "labels": {"app.kubernetes.io/managed-by": "helm"},
                "annotations": {"helm.sh/release": "gateway"}
            },
            "spec": {
                "replicas": 2,
                "strategy": {
                    "type": "RollingUpdate",
                    "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1}
                },
                "selector": {"matchLabels": {"app": "gateway"}},
                "template": {
                    "metadata": {
                        "labels": {"app": "gateway"},
                        "annotations": {"unrelated": "preserve"}
                    },
                    "spec": {
                        "serviceAccountName": "gateway",
                        "terminationGracePeriodSeconds": 30,
                        "containers": [
                            {
                                "name": "praxis",
                                "image": "praxis:test",
                                "volumeMounts": [
                                    {"name": "grid-credential", "mountPath": "/run/secrets/grid", "readOnly": true},
                                    {"name": "praxis-config", "mountPath": "/etc/praxis", "readOnly": true}
                                ]
                            },
                            {
                                "name": "metrics-sidecar",
                                "image": "metrics:test",
                                "volumeMounts": [
                                    {"name": "metrics-data", "mountPath": "/var/metrics"}
                                ]
                            }
                        ],
                        "initContainers": [
                            {
                                "name": "init-config",
                                "image": "busybox:test",
                                "command": ["sh", "-c", "true"],
                                "volumeMounts": [
                                    {"name": "init-data", "mountPath": "/init"}
                                ]
                            }
                        ],
                        "volumes": [
                            {"name": "grid-credential", "secret": {"secretName": "credential-old"}},
                            {"name": "praxis-config", "configMap": {"name": "praxis-config"}},
                            {"name": "metrics-data", "emptyDir": {}},
                            {"name": "init-data", "configMap": {"name": "init-config"}}
                        ]
                    }
                }
            }
        })
    }

    fn mount_ownership_test_deployment() -> Deployment {
        serde_json::from_value(mount_ownership_test_deployment_value()).unwrap_or_else(|_| std::process::abort())
    }

    fn mount_ownership_test_pod_spec(deployment: &Deployment) -> &k8s_openapi::api::core::v1::PodSpec {
        let Some(pod_spec) = deployment.spec.as_ref().and_then(|spec| spec.template.spec.as_ref()) else {
            std::process::abort();
        };
        pod_spec
    }

    fn append_mount_to_test_container(value: &mut Value, container_list: &str, container_index: usize, mount: Value) {
        let Some(containers) = value
            .get_mut("spec")
            .and_then(|spec| spec.get_mut("template"))
            .and_then(|template| template.get_mut("spec"))
            .and_then(|spec| spec.get_mut(container_list))
            .and_then(Value::as_array_mut)
        else {
            std::process::abort();
        };
        let Some(container) = containers.get_mut(container_index) else {
            std::process::abort();
        };
        let Some(mounts) = container.get_mut("volumeMounts").and_then(Value::as_array_mut) else {
            std::process::abort();
        };
        mounts.push(mount);
    }

    fn assert_mount_ownership_test_deployment_unchanged(deployment: &Deployment, original: &Value) {
        let current = serde_json::to_value(deployment).unwrap_or_else(|_| std::process::abort());
        assert_eq!(current, *original, "the ownership check must not mutate the Deployment");
        assert_eq!(current.pointer("/spec/strategy/type"), Some(&json!("RollingUpdate")));
        assert_eq!(
            current.pointer("/spec/template/metadata/annotations/unrelated"),
            Some(&json!("preserve"))
        );
        assert_eq!(
            current.pointer("/spec/template/spec/containers/1/volumeMounts/0/name"),
            Some(&json!("metrics-data"))
        );
        assert_eq!(
            current.pointer("/spec/template/spec/initContainers/0/volumeMounts/0/name"),
            Some(&json!("init-data"))
        );
    }

    fn is_mount_ownership_conflict(error: &OperatorError) -> bool {
        matches!(error, OperatorError::MountReconciliation(failure) if failure.reason == "OwnershipConflict")
    }

    #[test]
    fn replacing_shared_grid_volume_with_sidecar_is_rejected_without_deployment_changes() {
        let mut value = mount_ownership_test_deployment_value();
        append_mount_to_test_container(
            &mut value,
            "containers",
            1,
            json!({"name": "grid-credential", "mountPath": "/run/shared/grid", "readOnly": true}),
        );
        let deployment: Deployment = serde_json::from_value(value).unwrap_or_else(|_| std::process::abort());
        let original = serde_json::to_value(&deployment).unwrap_or_else(|_| std::process::abort());
        let replacement = json!({"name": "grid-credential", "secret": {"secretName": "credential-new"}});
        let result = owned_volume_mutation_patch(
            mount_ownership_test_pod_spec(&deployment),
            "praxis",
            "grid-credential",
            Some(&replacement),
        );

        let error = result.err().unwrap_or_else(|| std::process::abort());
        assert!(is_mount_ownership_conflict(&error));
        assert_mount_ownership_test_deployment_unchanged(&deployment, &original);
    }

    #[test]
    fn replacing_shared_grid_volume_with_init_container_is_rejected_without_deployment_changes() {
        let mut value = mount_ownership_test_deployment_value();
        append_mount_to_test_container(
            &mut value,
            "initContainers",
            0,
            json!({"name": "grid-credential", "mountPath": "/run/shared/grid", "readOnly": true}),
        );
        let deployment: Deployment = serde_json::from_value(value).unwrap_or_else(|_| std::process::abort());
        let original = serde_json::to_value(&deployment).unwrap_or_else(|_| std::process::abort());
        let replacement = json!({"name": "grid-credential", "secret": {"secretName": "credential-new"}});
        let result = owned_volume_mutation_patch(
            mount_ownership_test_pod_spec(&deployment),
            "praxis",
            "grid-credential",
            Some(&replacement),
        );

        let error = result.err().unwrap_or_else(|| std::process::abort());
        assert!(is_mount_ownership_conflict(&error));
        assert_mount_ownership_test_deployment_unchanged(&deployment, &original);
    }

    #[test]
    fn deleting_stale_grid_volume_with_init_container_is_rejected_without_deployment_changes() {
        let mut value = mount_ownership_test_deployment_value();
        append_mount_to_test_container(
            &mut value,
            "initContainers",
            0,
            json!({"name": "grid-credential", "mountPath": "/run/shared/grid", "readOnly": true}),
        );
        let deployment: Deployment = serde_json::from_value(value).unwrap_or_else(|_| std::process::abort());
        let original = serde_json::to_value(&deployment).unwrap_or_else(|_| std::process::abort());
        let result = owned_volume_mutation_patch(
            mount_ownership_test_pod_spec(&deployment),
            "praxis",
            "grid-credential",
            None,
        );

        let error = result.err().unwrap_or_else(|| std::process::abort());
        assert!(is_mount_ownership_conflict(&error));
        assert_mount_ownership_test_deployment_unchanged(&deployment, &original);
    }

    #[test]
    fn unshared_grid_volume_rotation_and_deletion_preserve_unrelated_deployment_state() {
        let deployment = mount_ownership_test_deployment();
        let original = serde_json::to_value(&deployment).unwrap_or_else(|_| std::process::abort());
        let replacement = json!({"name": "grid-credential", "secret": {"secretName": "credential-new"}});
        let replacement_patch = owned_volume_mutation_patch(
            mount_ownership_test_pod_spec(&deployment),
            "praxis",
            "grid-credential",
            Some(&replacement),
        )
        .unwrap_or_else(|_| std::process::abort());
        let deletion_patch = owned_volume_mutation_patch(
            mount_ownership_test_pod_spec(&deployment),
            "praxis",
            "grid-credential",
            None,
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            replacement_patch,
            vec![json!({"name": "grid-credential", "$patch": "delete"}), replacement]
        );
        assert_eq!(
            deletion_patch,
            vec![json!({"name": "grid-credential", "$patch": "delete"})]
        );
        assert_mount_ownership_test_deployment_unchanged(&deployment, &original);
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
    fn consumer_config_status_projected_credentials_reason_is_specific() {
        let gw = make_gw_ref("gw", "ns");
        let cc = make_consumer_config("cm");
        let err = OperatorError::ConsumerConfigRender(ConsumerConfigError::ProjectedCredentialsUnsupported);
        let status = consumer_config_status_error(&gw, &cc, &err, 1);
        assert_eq!(status.reason, "ProjectedCredentialsUnsupported");
        assert!(status.message.contains("supportsProjectedCredentials=true"));
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
            tools: Vec::new(),
            backend_kind: "local".to_owned(),
            capacity_weight: 1,
            phase: crdt::ProviderPhase::Available,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            access_policy: crdt::ProviderAccessPolicy::default(),
            revision: 1,
            writer_id: site_id.to_owned(),
        }
    }

    fn make_tool_crdt_provider(site_id: &str) -> crdt::ProviderState {
        let mut provider = make_eligible_crdt_provider("net", site_id);
        provider.provider_id = "tool/mcp-server".to_owned();
        provider.routing_cluster = "tool/mcp-server".to_owned();
        provider.models.clear();
        provider.tools = vec!["read_file".to_owned(), "list_dir".to_owned()];
        provider
    }

    fn make_active_grid_site(k8s_name: &str, network_ref: &str) -> GridSite {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": k8s_name },
            "spec": { "gridNetworkRef": network_ref },
            "status": { "phase": "Active" }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn make_phase_grid_site(k8s_name: &str, network_ref: &str, phase: &str) -> GridSite {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
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

    /// Remote `mcp_tool` CRDT providers must be gated by the same active `GridSite`
    /// requirement that inference providers are. A tool provider whose source site is
    /// not active must produce zero routing candidates even when the provider itself
    /// is available and carries non-empty tools.
    #[test]
    fn remote_tool_crdt_provider_excluded_when_source_site_not_active() {
        let sites = vec![
            make_active_grid_site("net-site-a", "net"),
            make_phase_grid_site("net-site-b", "net", "Connecting"),
        ];
        let active_provider = make_tool_crdt_provider("site-a");
        let inactive_provider = make_tool_crdt_provider("site-b");
        let providers = vec![active_provider, inactive_provider.clone()];
        let eligible = filter_eligible_remote_crdt_providers("net", &sites, &providers);
        assert_eq!(
            eligible.len(),
            1,
            "only the active-site tool provider must pass the filter"
        );
        let provider = eligible.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(
            (provider.site_id.as_str(), provider.provider_id.as_str()),
            ("site-a", "tool/mcp-server"),
            "the eligible provider must retain its source site and tool identity"
        );
        let excluded_candidates = routing_overlay::remote_crdt_provider_to_candidates(&inactive_provider);
        assert!(
            !excluded_candidates.is_empty(),
            "the inactive-site provider carries tools so it would produce candidates if not filtered"
        );
        assert!(
            excluded_candidates.iter().all(|c| c.kind == "mcp_tool"),
            "candidates from a tool provider must be mcp_tool kind"
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
            "apiVersion": "grid.praxis.fast/v1alpha1",
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
            "apiVersion": "grid.praxis.fast/v1alpha1",
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

    // -----------------------------------------------------------------------
    // tool_provider_state_from_kube
    // -----------------------------------------------------------------------

    fn make_agent_tool_provider(name: &str, network: &str, tools: &[&str]) -> AgentToolProvider {
        let tools_json: Vec<Value> = tools.iter().map(|t| serde_json::json!({ "name": t })).collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "AgentToolProvider",
            "metadata": { "name": name, "resourceVersion": "100" },
            "spec": {
                "gridNetworkRef": network,
                "endpoint": "http://localhost:9090",
                "tools": tools_json
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn make_agent_tool_provider_with_discovered(
        name: &str,
        network: &str,
        spec_tools: &[&str],
        discovered: &[&str],
    ) -> AgentToolProvider {
        let tools_json: Vec<Value> = spec_tools.iter().map(|t| serde_json::json!({ "name": t })).collect();
        let discovered_json: Vec<&str> = discovered.to_vec();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "AgentToolProvider",
            "metadata": { "name": name, "resourceVersion": "200", "generation": 1 },
            "spec": {
                "gridNetworkRef": network,
                "endpoint": "http://localhost:9090",
                "tools": tools_json
            },
            "status": {
                "discoveredTools": discovered_json,
                "phase": "Available",
                "matchingSites": [],
                "observedGeneration": 1
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn tool_provider_state_from_kube_does_not_route_unprobed_spec_tools() {
        let provider = make_agent_tool_provider("mcp-server", "net", &["search", "translate"]);
        let state = tool_provider_state_from_kube(&provider, "net", "site-a").unwrap_or_else(|| std::process::abort());
        assert!(
            state.tools.is_empty(),
            "spec tools are an allowlist, not discovered routes"
        );
        assert!(state.models.is_empty(), "tool provider must have no models");
        assert_eq!(
            state.provider_id, "tool/mcp-server",
            "provider_id must have tool/ prefix"
        );
        assert_eq!(state.network_id, "net");
        assert_eq!(state.site_id, "site-a");
        assert!(
            state.backend_kind.is_empty(),
            "tool provider backend_kind must be empty"
        );
    }

    #[test]
    fn tool_provider_spec_tools_filters_discovered_tools() {
        let provider = make_agent_tool_provider_with_discovered(
            "mcp-server",
            "net",
            &["discovered-a"],
            &["discovered-a", "discovered-b"],
        );
        let state = tool_provider_state_from_kube(&provider, "net", "site-a").unwrap_or_else(|| std::process::abort());
        assert_eq!(
            state.tools,
            vec!["discovered-a"],
            "spec.tools must act as allowlist over discoveredTools"
        );
    }

    #[test]
    fn tool_provider_empty_spec_tools_passes_all_discovered() {
        let provider =
            make_agent_tool_provider_with_discovered("mcp-server", "net", &[], &["discovered-a", "discovered-b"]);
        let state = tool_provider_state_from_kube(&provider, "net", "site-a").unwrap_or_else(|| std::process::abort());
        assert_eq!(
            state.tools,
            vec!["discovered-a", "discovered-b"],
            "empty spec.tools must pass all discovered tools through"
        );
    }

    #[test]
    fn tool_provider_state_from_kube_keeps_successful_empty_discovery_authoritative() {
        let provider = make_agent_tool_provider_with_discovered("mcp-server", "net", &["fallback-tool"], &[]);
        let state = tool_provider_state_from_kube(&provider, "net", "site-a").unwrap_or_else(|| std::process::abort());
        assert!(state.tools.is_empty());
    }

    #[test]
    fn tool_provider_state_from_kube_rejects_stale_observed_generation() {
        let mut provider = make_agent_tool_provider_with_discovered("mcp-server", "net", &[], &["search"]);
        provider.metadata.generation = Some(2);
        let state = tool_provider_state_from_kube(&provider, "net", "site-a").unwrap_or_else(|| std::process::abort());
        assert!(state.tools.is_empty());
    }

    #[test]
    fn tool_provider_state_filters_invalid_and_duplicate_discovery_names() {
        let long_name = "x".repeat(MAX_TOOL_NAME_LEN + 1);
        let discovered = ["search", " ", "search", long_name.as_str()];
        let provider = make_agent_tool_provider_with_discovered("mcp-server", "net", &[], &discovered);
        let state = tool_provider_state_from_kube(&provider, "net", "site-a").unwrap_or_else(|| std::process::abort());
        assert_eq!(state.tools, vec!["search"]);
    }

    #[test]
    fn tool_provider_state_from_kube_parses_resource_version_as_revision() {
        let provider = make_agent_tool_provider("mcp-server", "net", &["tool-a"]);
        let state = tool_provider_state_from_kube(&provider, "net", "site-a").unwrap_or_else(|| std::process::abort());
        assert_eq!(state.revision, 100, "resourceVersion=100 must parse to revision=100");
    }

    #[test]
    fn tool_provider_state_routing_cluster_uses_tool_prefix() {
        let provider = make_agent_tool_provider("my-mcp", "net", &["tool-a"]);
        let state = tool_provider_state_from_kube(&provider, "net", "site-a").unwrap_or_else(|| std::process::abort());
        assert_eq!(
            state.routing_cluster, "tool/my-mcp",
            "routing_cluster must use tool/ prefix to avoid collision with inference providers"
        );
    }

    // -----------------------------------------------------------------------
    // upsert_provider_with_capabilities
    // -----------------------------------------------------------------------

    /// Tool names are stored on the provider record, NOT as `Capability::Tool`
    /// in the OR-set. They travel in the `BroadcastExtension`'s `provider_tools`
    /// map. This test verifies the provider is upserted with tools intact and
    /// that no `Capability::Tool` entries are created.
    #[test]
    fn upsert_tool_provider_stores_tools_on_provider_not_as_capabilities() {
        let mut snap = crdt::GridStateSnapshot::new("site-a".to_owned());
        let mut max_rev = 0_u64;
        let state = crdt::ProviderState {
            network_id: "net".to_owned(),
            site_id: "site-a".to_owned(),
            provider_id: "tool/tool-prov".to_owned(),
            routing_cluster: "tool/tool-prov".to_owned(),
            models: Vec::new(),
            tools: vec!["search".to_owned(), "calc".to_owned()],
            backend_kind: String::new(),
            capacity_weight: 1,
            phase: crdt::ProviderPhase::Available,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            access_policy: crdt::ProviderAccessPolicy::default(),
            revision: 5,
            writer_id: "site-a".to_owned(),
        };
        upsert_provider_with_capabilities(&mut snap, &mut max_rev, state);
        assert_eq!(max_rev, 5);
        let key = "net/site-a/tool/tool-prov";
        let provider = snap.providers.get(key).unwrap_or_else(|| std::process::abort());
        assert_eq!(provider.tools, vec!["search", "calc"]);
        assert!(
            !snap.capabilities.contains(&crdt::Capability::Tool("search".to_owned())),
            "tool names must NOT be registered as Capability::Tool (byte budget)"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "constructs a mixed provider and verifies both capability classes"
    )]
    fn upsert_mixed_provider_registers_models_but_not_tools_as_capabilities() {
        let mut snap = crdt::GridStateSnapshot::new("site-a".to_owned());
        let mut max_rev = 0_u64;
        let state = crdt::ProviderState {
            network_id: "net".to_owned(),
            site_id: "site-a".to_owned(),
            provider_id: "hybrid".to_owned(),
            routing_cluster: "hybrid".to_owned(),
            models: vec!["llama".to_owned()],
            tools: vec!["search".to_owned()],
            backend_kind: "local".to_owned(),
            capacity_weight: 1,
            phase: crdt::ProviderPhase::Available,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            access_policy: crdt::ProviderAccessPolicy::default(),
            revision: 3,
            writer_id: "site-a".to_owned(),
        };
        upsert_provider_with_capabilities(&mut snap, &mut max_rev, state);
        assert!(
            snap.capabilities.contains(&crdt::Capability::Model("llama".to_owned())),
            "model capabilities must still be registered"
        );
        assert!(
            !snap.capabilities.contains(&crdt::Capability::Tool("search".to_owned())),
            "tool names must NOT be registered as Capability::Tool (byte budget)"
        );
        let provider = snap
            .providers
            .get("net/site-a/hybrid")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            provider.tools,
            vec!["search"],
            "tools must be stored on provider record"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "constructs an oversized provider snapshot and verifies both transport and retention invariants"
    )]
    fn tool_catalog_is_trimmed_to_the_serialized_swim_budget() {
        let mut snap = crdt::GridStateSnapshot::new("site-a".to_owned());
        let offered = 128_usize;
        snap.upsert_provider(crdt::ProviderState {
            network_id: "net".to_owned(),
            site_id: "site-a".to_owned(),
            provider_id: "tool/mcp-server".to_owned(),
            routing_cluster: "tool/mcp-server".to_owned(),
            models: Vec::new(),
            tools: (0..offered)
                .map(|index| format!("tool-{index:03}-{}", "x".repeat(240)))
                .collect(),
            backend_kind: String::new(),
            capacity_weight: 1,
            phase: crdt::ProviderPhase::Available,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            access_policy: crdt::ProviderAccessPolicy::default(),
            revision: 1,
            writer_id: "site-a".to_owned(),
        });
        let mut broadcast = swim::StateBroadcast::new("site-a".to_owned(), 1, snap, None)
            .with_grid_id(Some("grid".to_owned()))
            .with_authoritative_provider_state();

        fit_provider_tools_to_swim_budget(&mut broadcast);

        assert_eq!(
            broadcast.revision, 1,
            "budget fitting must preserve the caller revision"
        );
        broadcast.revision = u64::MAX;
        let encoded = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        let local_id = worst_case_swim_identity("site-a");
        let budget = swim::node::state_broadcast_byte_budget(&local_id).unwrap_or_else(|_| std::process::abort());
        let retained = broadcast
            .snapshot
            .providers
            .get("net/site-a/tool/mcp-server")
            .map_or(0, |provider| provider.tools.len());
        assert!(encoded.len() <= budget);
        assert!(retained < offered);
        assert!(retained > 0);
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "constructs many tool providers and verifies inference survives whole-record trimming"
    )]
    fn many_tool_provider_records_cannot_evict_inference_or_overfill_swim() {
        let mut snap = crdt::GridStateSnapshot::new("site-a".to_owned());
        snap.upsert_provider(crdt::ProviderState {
            network_id: "net".to_owned(),
            site_id: "site-a".to_owned(),
            provider_id: "inference".to_owned(),
            routing_cluster: "inference".to_owned(),
            models: vec!["llama".to_owned()],
            tools: Vec::new(),
            backend_kind: "local".to_owned(),
            capacity_weight: 1,
            phase: crdt::ProviderPhase::Available,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            access_policy: crdt::ProviderAccessPolicy::default(),
            revision: 1,
            writer_id: "site-a".to_owned(),
        });
        for index in 0..32 {
            snap.upsert_provider(crdt::ProviderState {
                network_id: "net".to_owned(),
                site_id: "site-a".to_owned(),
                provider_id: format!("tool/provider-{index:02}"),
                routing_cluster: format!("tool/provider-{index:02}"),
                models: Vec::new(),
                tools: vec![format!("tool-{index:02}-{}", "x".repeat(240))],
                backend_kind: String::new(),
                capacity_weight: 1,
                phase: crdt::ProviderPhase::Available,
                metrics: crdt::ProviderMetricsSnapshot::default(),
                access_policy: crdt::ProviderAccessPolicy::default(),
                revision: 1,
                writer_id: "site-a".to_owned(),
            });
        }
        let mut broadcast =
            swim::StateBroadcast::new("site-a".to_owned(), 1, snap, None).with_grid_id(Some("grid".to_owned()));

        fit_provider_tools_to_swim_budget(&mut broadcast);

        broadcast.revision = u64::MAX;
        let encoded = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        let local_id = worst_case_swim_identity("site-a");
        let budget = swim::node::state_broadcast_byte_budget(&local_id).unwrap_or_else(|_| std::process::abort());
        assert!(encoded.len() <= budget);
        assert!(broadcast.snapshot.providers.contains_key("net/site-a/inference"));
        assert!(
            broadcast.snapshot.providers.len() < 33,
            "whole tool records must be bounded"
        );
        assert!(
            broadcast
                .snapshot
                .providers
                .values()
                .any(|provider| !provider.tools.is_empty()),
            "the fair fit should retain at least one reachable tool provider"
        );
    }
}
