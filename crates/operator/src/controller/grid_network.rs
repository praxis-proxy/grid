//! [`GridNetwork`] controller.
//!
//! Reconciles [`GridNetwork`] resources: generates the grid CA
//! and site certificate, manages TLS secrets, generates the
//! grid ID, signals the SWIM runtime to start, and renders
//! routing overlay ConfigMaps for each gateway reference.
//!
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork

use std::{
    borrow::Cow,
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
            ClusterEndpointConfig, ConsumerConfig, ConsumerConfigPhase, ConsumerConfigStatus, GatewayRef, GridNetwork,
            GridNetworkPhase, GridNetworkStatus, MembershipMode, MountReconciliation, MountReconciliationPhase,
            MountReconciliationStatus, OverlayPhase, OverlayRevisionStatus, PeerTrustMode, SignalMode, SignedStateMode,
            SiteRouter, TenantBudgetStatus, TransportMode,
        },
        grid_site::{GridSite, GridSitePhase, GridSiteStatus},
        inference_provider::{Condition, ConditionStatus, InferenceProvider, InferenceProviderStatus},
    },
    error::OperatorError,
    readiness,
    resources::{
        consumer_config::{self, ConsumerConfigError, SiteRoutePrerequisite},
        derived_topology, gateway_mounts, overlay_envelope, provider_admission, provider_metrics, routing_overlay,
        secret,
        serving_config::{self, ServingInputs, WriteDecision, WriteGate},
        tls_backend::ServerTlsConfig,
        trust_bundle::{self, CertPemStatus},
    },
    rollup, served_models, signals, site_providers,
    swim::{MemberStatus, MembershipSnapshot},
    swim_endpoint::{SeedResolution, resolve_endpoint_list_partial},
    swim_runtime::{PeerPins, SwimHandle},
};


// ---------------------------------------------------------------------------
// Operator context
// ---------------------------------------------------------------------------

/// Shared context passed to the [`GridNetwork`] controller's reconcile loop.
///
/// Bundles the Kubernetes client with an optional live SWIM handle.
/// When `swim` is `Some`, each reconcile obtains a fresh
/// [`MembershipSnapshot`] to feed into `determine_phase` and
/// `update_status`. When `swim` is `None`, phase follows the configured
/// membership mode and local identity prerequisites.
#[expect(
    clippy::partial_pub_fields,
    reason = "client is public API; the SWIM handle and caches are reached through methods"
)]
pub struct OperatorCtx {
    /// One provider collection/publication pass at a time, across networks.
    signal_pass: tokio::sync::Semaphore,
    /// Monotonic batch sequence within this process's immutable publisher epoch.
    signal_sequence: std::sync::atomic::AtomicU64,
    /// A superseded publisher never reacquires authority in the same process.
    signal_fenced: std::sync::atomic::AtomicBool,
    /// Publication ordering is scoped to the single supported network.
    signal_network: std::sync::OnceLock<String>,
    /// Kubernetes API client.
    pub client: Client,

    /// The live SWIM runtime once started, empty without a SWIM bind address.
    swim: std::sync::OnceLock<Arc<SwimHandle>>,

    /// Who publishes this site's signals, set once its revision lease is reserved.
    publisher: std::sync::OnceLock<Publisher>,

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
    /// When the poller's last round that included each peer ended.
    pub(crate) peer_rounds: signals::PeerRounds,
    /// Whether each polled peer has caught up since this process started.
    pub(crate) peer_catch_up: signals::CatchUp,
    /// Each peer provider the poller has received load rows for.
    pub(crate) load_heard: signals::LoadHeard,

    /// This site's scraped signals, served to gateways and peers.
    pub(crate) signals: signals::SignalStore,

    /// This site's providers, served to peers on the providers path.
    local_providers: site_providers::LocalProviders,
    /// Providers polled from peers, the poll-mode source of remote candidates.
    peer_providers: site_providers::PeerProviders,

    /// Served-model sets discovered from this site's providers.
    pub(crate) served_models: served_models::ServedModelStore,

    /// Each provider's latest scrape, from which its readiness is resolved.
    pub(crate) readiness: readiness::ReadinessStore,

    /// How often the signals loop scrapes providers; a readiness window is at least two.
    scrape_interval: Duration,

    /// Duration of each network's latest provider-scrape pass.
    ///
    /// Used to widen the readiness window when a pass takes longer than one
    /// scrape interval, so healthy providers are not marked stale before the
    /// next pass can refresh them. The measurement covers only that network's
    /// `collect_provider_signals` call; provider listing, peer registration,
    /// and other networks' passes are excluded.
    last_scrape_passes: std::sync::Mutex<HashMap<String, Duration>>,

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

    /// This operator's own settings, which say whether this site enrolls.
    operator_config: crate::operator_config::ConfigWatch,

    /// Where a mode change asks for the graceful restart that applies it.
    mode_restart: Option<tokio::sync::watch::Sender<Option<GridModes>>>,

    /// This site's name, which a certificate the operator issues itself carries.
    site_name: Option<String>,

    /// How this site learns its members.
    membership: crate::cli::Membership,

    /// Membership selected by the install when the network omits its override.
    membership_fallback: MembershipMode,

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
    /// Membership discovery.
    pub membership: MembershipMode,
    /// Signal propagation.
    pub signal: SignalMode,
    /// Peer authorization on the signals path.
    pub trust: PeerTrustMode,
}

impl GridModes {
    /// Without a `GridNetwork`: SPIFFE trust in the Grid CA identity, never an implicit pin.
    pub const WITHOUT_NETWORK: Self = Self {
        membership: MembershipMode::Swim,
        signal: SignalMode::Gossip,
        trust: PeerTrustMode::Spiffe,
    };

    /// The modes to start in with no `GridNetwork` yet: the install's declared modes,
    /// else [`Self::WITHOUT_NETWORK`]. Matching the network the install will create
    /// means a fresh install never restarts once it appears.
    #[must_use]
    pub fn without_network(signal: Option<SignalMode>, trust: Option<PeerTrustMode>) -> Self {
        Self {
            membership: Self::WITHOUT_NETWORK.membership,
            signal: signal.unwrap_or(Self::WITHOUT_NETWORK.signal),
            trust: trust.unwrap_or(Self::WITHOUT_NETWORK.trust),
        }
    }

    /// Select the install's membership before a network overrides it.
    #[must_use]
    pub const fn with_membership(mut self, membership: MembershipMode) -> Self {
        self.membership = membership;
        self
    }

    /// The modes `network` declares, using `membership` when its override is absent.
    #[must_use]
    pub fn of(network: &GridNetwork, membership: MembershipMode) -> Self {
        Self {
            membership: network
                .spec
                .membership
                .as_ref()
                .map_or(membership, |policy| policy.mode),
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
    pub fn restart_for(self, network: Option<&GridNetwork>, membership_fallback: MembershipMode) -> Option<Self> {
        // Only the poll path reads trust, so a trust change under gossip needs no restart.
        network
            .map(|network| Self::of(network, membership_fallback))
            .filter(|declared| {
                declared.membership != self.membership
                    || declared.signal != self.signal
                    || (declared.signal == SignalMode::Poll && declared.trust != self.trust)
            })
    }
}

/// The site publishing signals, and the immutable node-generation range fencing it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publisher {
    /// The site the revision lease is reserved for.
    pub site: String,
    /// First and last node generation of the lease.
    pub reservation: (u64, u64),
}

impl Publisher {
    /// The publisher a running SWIM runtime reserved for.
    #[must_use]
    pub fn of(handle: &SwimHandle) -> Self {
        Self {
            site: handle.site_name().to_owned(),
            reservation: handle.publisher_reservation(),
        }
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
    /// Port a declared peer serves signals on at its gateway host. Unset, the gateway's
    /// own port, behind the same router.
    pub signals_port: Option<u16>,
    /// How often this operator polls each peer.
    pub peer_interval: Duration,
    /// The most one peer poll may take, attempts included.
    pub peer_budget: Duration,
}

impl Default for PeerSettings {
    fn default() -> Self {
        Self {
            local_signals_addr: None,
            trust: PeerTrustMode::default(),
            peer_port: signals::DEFAULT_PEER_PORT,
            signals_port: None,
            peer_interval: Duration::from_secs(30),
            peer_budget: Duration::from_secs(10),
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
            signal_pass: tokio::sync::Semaphore::new(1),
            signal_sequence: std::sync::atomic::AtomicU64::new(0),
            signal_fenced: std::sync::atomic::AtomicBool::new(false),
            signal_network: std::sync::OnceLock::new(),
            publisher: swim
                .as_ref()
                .map(|handle| std::sync::OnceLock::from(Publisher::of(handle)))
                .unwrap_or_default(),
            swim: swim.map(std::sync::OnceLock::from).unwrap_or_default(),
            metrics_cache: Mutex::new(provider_metrics::MetricsCache::new()),
            admission_memory: Mutex::new(provider_admission::AdmissionMemory::default()),
            last_seeds: std::sync::Mutex::new(HashMap::new()),
            refused_sites: std::sync::Mutex::new(HashMap::new()),
            peer_identities: signals::PeerIdentities::new(),
            peers: signals::SignalStore::new(),
            peer_rounds: signals::PeerRounds::default(),
            peer_catch_up: signals::CatchUp::default(),
            load_heard: signals::LoadHeard::default(),
            signals: signals::SignalStore::new(),
            local_providers: site_providers::LocalProviders::default(),
            peer_providers: site_providers::PeerProviders::default(),
            served_models: served_models::ServedModelStore::new(),
            readiness: readiness::ReadinessStore::default(),
            scrape_interval: Duration::ZERO,
            last_scrape_passes: std::sync::Mutex::new(HashMap::new()),
            signal_mode,
            serving_writes: WriteGate::default(),
            peer_settings: PeerSettings::default(),
            membership_ready: std::sync::atomic::AtomicBool::new(true),
            declared_trust: None,
            operator_config: crate::operator_config::ConfigWatch::fixed(crate::operator_config::Snapshot::absent()),
            mode_restart: None,
            site_name: None,
            membership: crate::cli::Membership::Swim,
            membership_fallback: MembershipMode::Swim,
            logged: ChangeLog::default(),
            started: Instant::now(),
        }
    }

    /// Read this operator's own settings from `config`.
    #[must_use]
    pub fn with_operator_config(mut self, config: crate::operator_config::ConfigWatch) -> Self {
        self.operator_config = config;
        self
    }

    /// Send the declared peer trust here whenever a `GridNetwork` changes it.
    #[must_use]
    pub fn with_declared_trust(mut self, declared_trust: tokio::sync::watch::Sender<PeerTrustMode>) -> Self {
        self.declared_trust = Some(declared_trust);
        self
    }

    /// Ask here for a graceful restart when a `GridNetwork` declares modes other than the running ones.
    #[must_use]
    pub fn with_mode_restart(mut self, mode_restart: tokio::sync::watch::Sender<Option<GridModes>>) -> Self {
        self.mode_restart = Some(mode_restart);
        self
    }

    /// Ask for the graceful restart that starts the process in `next`.
    fn owe_restart(&self, next: GridModes) {
        if let Some(sender) = &self.mode_restart {
            sender.send_replace(Some(next));
        } else {
            tracing::warn!(?next, "no restart is wired; the declared modes apply at the next start");
        }
    }

    /// Publish the trust `network` declares, waking its watchers only on a change.
    fn publish_declared_trust(&self, network: &GridNetwork) {
        if let Some(sender) = &self.declared_trust {
            send_declared_trust(sender, GridModes::of(network, self.membership_fallback).trust);
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
        // A publisher reserved without SWIM keeps its fence.
        let _held = self.publisher.set(Publisher::of(&handle));
        self.swim.set(handle).is_ok()
    }

    /// Who publishes this site's signals, `None` until a revision lease is reserved.
    #[must_use]
    pub fn publisher(&self) -> Option<&Publisher> {
        self.publisher.get()
    }

    /// Install the publisher reserved without SWIM, `false` if one already was.
    pub fn set_publisher(&self, publisher: Publisher) -> bool {
        self.publisher.set(publisher).is_ok()
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

    /// The signal transport this operator runs, fixed for its lifetime.
    #[must_use]
    pub const fn signal_mode(&self) -> SignalMode {
        self.signal_mode
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

    /// Record how long one network's latest provider-scrape pass took.
    fn record_scrape_pass(&self, network: &str, duration: Duration) {
        let mut passes = self
            .last_scrape_passes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        passes.insert(network.to_owned(), duration);
    }

    /// The latest measured provider-scrape pass for `network`.
    fn last_scrape_pass(&self, network: &str) -> Duration {
        self.last_scrape_passes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(network)
            .copied()
            .unwrap_or(Duration::ZERO)
    }

    /// Learn members the way `membership` says.
    #[must_use]
    pub const fn with_membership(mut self, membership: crate::cli::Membership) -> Self {
        self.membership = membership;
        self.membership_fallback = membership;
        self
    }

    /// Restore the install's membership if the network override is removed.
    #[must_use]
    pub const fn with_membership_fallback(mut self, membership: MembershipMode) -> Self {
        self.membership_fallback = membership;
        self
    }

    /// How this site learns its members.
    #[must_use]
    pub const fn membership(&self) -> crate::cli::Membership {
        self.membership
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

    /// A handle to this site's providers, for the signals listener.
    #[must_use]
    pub fn local_providers(&self) -> site_providers::LocalProviders {
        self.local_providers.clone()
    }

    /// A handle to the providers polled from peers, for the poller and reconcile.
    #[must_use]
    pub fn peer_providers(&self) -> site_providers::PeerProviders {
        self.peer_providers.clone()
    }

    /// Whether each polled peer has caught up since start, for the poller and the restart hold.
    #[must_use]
    pub fn peer_catch_up(&self) -> signals::CatchUp {
        self.peer_catch_up.clone()
    }

    /// A handle to the poller's rounds, for the poller and this site's own gateway.
    #[must_use]
    pub fn peer_rounds(&self) -> signals::PeerRounds {
        self.peer_rounds.clone()
    }

    /// A handle to the peer providers the poller has heard load for.
    #[must_use]
    pub fn load_heard(&self) -> signals::LoadHeard {
        self.load_heard.clone()
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

/// Scrape this site's providers once, write their readiness conditions, and, in Poll mode,
/// publish the coarse signals observed.
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
    // Under gossip, load travels the CRDT path, so this pass only judges readiness.
    let polling = ctx.signal_mode == SignalMode::Poll;
    let publisher = ctx.publisher().filter(|_| polling);
    let Some(_pass) = signal_pass(ctx, network_name, publisher) else {
        return Ok(());
    };
    let collected_at = Instant::now();
    let providers = list_all_inference_providers(client).await?;
    if polling {
        ctx.signals.set_access(signal_access(&providers));
        Box::pin(register_peers(ctx, client)).await?;
    }
    Box::pin(scrape_providers(ctx, client, &providers, network_name)).await;
    let mut collected = resolve_readiness(ctx, client, &providers, network_name);
    if polling && !ctx.signal_fenced.load(std::sync::atomic::Ordering::Relaxed) {
        if let Some(publisher) = publisher {
            let Some((epoch, sequence)) = publication_order(ctx, client, publisher).await? else {
                return Ok(());
            };
            crate::provider_revision::qualify(&mut collected, &providers, network_name, epoch, sequence);
        }
        publish_signals(ctx, collected, collected_at);
    }
    Ok(())
}

/// Scrape `providers` once into the readiness store and record how long the pass took.
async fn scrape_providers(ctx: &OperatorCtx, client: &Client, providers: &[InferenceProvider], network_name: &str) {
    let scrape_started = Instant::now();
    Box::pin(provider_metrics::collect_provider_signals(
        network_name,
        providers,
        Some(client),
        &ctx.readiness,
    ))
    .await;
    ctx.record_scrape_pass(network_name, scrape_started.elapsed());
}

/// Serialize collection and bind ordered publication to one network.
fn signal_pass<'context>(
    ctx: &'context OperatorCtx,
    network: &str,
    publisher: Option<&Publisher>,
) -> Option<tokio::sync::SemaphorePermit<'context>> {
    let pass = ctx.signal_pass.try_acquire().ok()?;
    if publisher.is_some() && ctx.signal_network.get_or_init(|| network.to_owned()) != network {
        tracing::warn!(network, "skipping signals for a different GridNetwork");
        return None;
    }
    Some(pass)
}

/// Allocate a publication order only while the pre-collection publisher still owns its fence.
async fn publication_order(
    ctx: &OperatorCtx,
    client: &Client,
    publisher: &Publisher,
) -> Result<Option<(u64, u64)>, OperatorError> {
    let (epoch, high) = publisher.reservation;
    let running = ctx.swim().is_none_or(|swim| swim.is_running());
    if !running || !crate::provider_revision::holds_fence(client, &publisher.site, high).await? {
        if !ctx.signal_fenced.swap(true, std::sync::atomic::Ordering::Relaxed) {
            tracing::error!(
                site = publisher.site,
                epoch,
                "signals publisher lost its reservation; publication is fenced until restart"
            );
        }
        return Ok(None);
    }
    Ok(ctx
        .signal_sequence
        .fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |value| value.checked_add(1),
        )
        .ok()
        .map(|previous| (epoch, previous.saturating_add(1))))
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
                message: "metrics.endpoint is not set".to_owned(),
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
        age: Duration::ZERO,
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
        age: Duration::ZERO,
    }
}

/// How long a provider's last good scrape stands.
///
/// `staleMetricsSeconds`, else half the signal TTL. The floor is one scrape
/// interval plus the larger of one interval or the latest measured pass, so a
/// slow pass does not make providers queued behind it stale before the next pass.
fn readiness_stale_after(ctx: &OperatorCtx, provider: &InferenceProvider) -> Duration {
    let interval = ctx.scrape_interval;
    let pass = ctx.last_scrape_pass(&provider.spec.grid_network_ref);
    let floor = interval.saturating_add(interval.max(pass));
    provider
        .spec
        .metrics_config
        .as_ref()
        .and_then(|mc| mc.stale_metrics_seconds)
        .filter(|secs| *secs > 0)
        .map_or_else(|| site_signals_ttl() / 2, |secs| Duration::from_secs(secs.into()))
        .max(floor)
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
#[expect(
    clippy::large_stack_frames,
    reason = "the Kubernetes status patch future carries a full InferenceProvider response"
)]
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
            status = %verdict.reason.status(),
            reason = verdict.reason.as_str(),
            message = %verdict.message,
            "provider readiness changed"
        );
    } else {
        tracing::info!(
            provider = name,
            status = %verdict.reason.status(),
            reason = verdict.reason.as_str(),
            message = %verdict.message,
            "provider readiness changed"
        );
    }
    Ok(())
}

/// The status patch carrying the conditions the readiness pass owns, `None` when none changed.
/// Every one is sent, since an apply that leaves one out removes it.
fn ready_condition_patch<'provider>(
    provider: &'provider InferenceProvider,
    verdict: &readiness::Verdict,
) -> Option<(&'provider str, Value)> {
    let name = provider.metadata.name.as_deref()?;
    let now = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .ok()?;
    let conditions = readiness_conditions(provider, verdict, &now)?;
    Some((
        name,
        serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "status": { "conditions": conditions }
        }),
    ))
}

/// `Ready`, `MetricsAvailable`, `EndpointLive` and `MetricsReachable` for `verdict`, `None`
/// when the live status already says them.
fn readiness_conditions(
    provider: &InferenceProvider,
    verdict: &readiness::Verdict,
    now: &str,
) -> Option<Vec<Condition>> {
    let current = provider
        .status
        .as_ref()
        .map_or(&[][..], |status| status.conditions.as_slice());
    let generation = provider.metadata.generation;
    let stamp = rollup::Stamp { generation, now };
    let ready = readiness::ready_condition(current, verdict, now, generation);
    let metrics = readiness::metrics_condition(current, verdict, now, generation);
    let live = readiness::endpoint_live(verdict, liveness_fallback(provider));
    let live = rollup::condition(current, readiness::ENDPOINT_LIVE_CONDITION, live, stamp);
    let reachable = readiness::metrics_reachable(verdict)
        .map(|state| rollup::condition(current, readiness::METRICS_REACHABLE_CONDITION, state, stamp));
    let held = |type_: &str| current.iter().find(|c| c.type_ == type_).cloned();
    // Left out of the apply, a held MetricsReachable is removed once no endpoint is set.
    let reachable_moved = reachable.as_ref().map_or_else(
        || held(readiness::METRICS_REACHABLE_CONDITION).is_some(),
        |reachable| rollup::differs(current, std::slice::from_ref(reachable)),
    );
    let live_moved = rollup::differs(current, std::slice::from_ref(&live));
    if ready.is_none() && metrics.is_none() && !reachable_moved && !live_moved {
        return None;
    }
    let conditions = [
        ready.or_else(|| held(readiness::READY_CONDITION)),
        metrics.or_else(|| held(readiness::METRICS_CONDITION)),
        Some(live),
        reachable,
    ];
    Some(conditions.into_iter().flatten().collect())
}

/// What stands in for metrics when judging a provider's liveness.
fn liveness_fallback(provider: &InferenceProvider) -> readiness::Fallback {
    let unavailable = provider
        .status
        .as_ref()
        .is_some_and(|status| status.phase == crate::crd::inference_provider::ProviderPhase::Unavailable);
    if unavailable {
        readiness::Fallback::Unavailable
    } else if provider.spec.health_check.is_some() {
        readiness::Fallback::HealthChecked
    } else {
        readiness::Fallback::Unchecked
    }
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
    let identities = peer_identities(&sites, ctx.peer_settings.trust, derived_signals(ctx));
    // The overlay's ceiling is shared among the declared peers, this site left out.
    let own = ctx.site_name();
    let peers = identities.keys().filter(|site| own != Some(site.as_str())).count();
    ctx.peer_providers.declare(peers);
    ctx.peer_identities.set(identities);
    Ok(())
}

/// Where a peer's signals are polled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SignalsAt {
    /// Where SWIM says the peer serves them.
    Advertised,
    /// At the endpoint derived from its `GridSite`, at this port or the gateway's.
    Derived(Option<u16>),
}

/// Where this site polls its peers' signals: derived under Local membership, advertised under Swim.
fn derived_signals(ctx: &OperatorCtx) -> SignalsAt {
    if ctx.membership() == crate::cli::Membership::Local {
        SignalsAt::Derived(ctx.peer_settings.signals_port)
    } else {
        SignalsAt::Advertised
    }
}

/// What this site holds about each peer it knows.
///
/// Keyed by the bare `site_id` both poll and serve look a peer up by, which an
/// auto-discovered object's `{network}-{site_id}` name is not. When two objects
/// name one site, a pinned record outranks an unpinned one, then an enrolled
/// object outranks a discovered stub. `status` is deliberately not read: it is
/// populated from gossip, and a member could advertise its own certificate under
/// another site's name. The labels are the local object's for the same reason.
fn peer_identities(
    sites: &[GridSite],
    trust: PeerTrustMode,
    signals_at: SignalsAt,
) -> BTreeMap<String, signals::PeerRecord> {
    let pinned = trust == PeerTrustMode::Pin;
    let mut ranked = BTreeMap::<String, ((bool, bool), signals::PeerRecord)>::new();
    for site in sites {
        let Some((key, enrolled)) = peer_site_key(site) else {
            continue;
        };
        // Under Local membership nothing discovers a peer, so a stub left from Swim names
        // no member and is not counted.
        if matches!(signals_at, SignalsAt::Derived(_)) && !enrolled {
            continue;
        }
        let record = signals::PeerRecord {
            labels: site.metadata.labels.clone().unwrap_or_default(),
            pins: site_pins(site, pinned),
            // A stub never gets here under Local, and under Swim a peer is polled where SWIM says.
            signals: match signals_at {
                SignalsAt::Derived(signals_port) => declared_signals(site, &key, signals_port),
                SignalsAt::Advertised => None,
            },
        };
        let rank = (!record.pins.is_empty(), enrolled);
        if ranked.get(&key).is_none_or(|(held, _)| rank > *held) {
            ranked.insert(key, (rank, record));
        }
    }
    ranked.into_iter().map(|(key, (_, record))| (key, record)).collect()
}

/// The fingerprints `site` is pinned to under pin trust, normalised; none under SPIFFE trust.
fn site_pins(site: &GridSite, pinned: bool) -> Vec<String> {
    site.spec
        .trust
        .as_ref()
        .filter(|_| pinned)
        .and_then(|site_trust| site_trust.canonical_fingerprints.as_deref())
        .unwrap_or_default()
        .iter()
        .map(|fp| signals::canonical_fingerprint(fp))
        .collect()
}

/// The cluster names this site owns: its providers' routing identities and the clusters its
/// gateways name explicitly, none of which a peer's record may claim.
fn reserved_clusters(network: &GridNetwork, providers: &[InferenceProvider]) -> std::collections::HashSet<String> {
    let explicit = network
        .spec
        .consumer_gateways
        .iter()
        .filter_map(|gateway| gateway.praxis_config.as_ref())
        .flat_map(|config| config.cluster_endpoints.iter().map(|entry| entry.cluster.clone()));
    providers
        .iter()
        .filter_map(|provider| routing_overlay::routing_identity(provider).map(str::to_owned))
        .chain(explicit)
        .collect()
}

/// Each provider-hop cluster this site's gateways declare, bound to the site its SNI names, the
/// only peer whose records may route through it.
fn hop_sites(network: &GridNetwork) -> site_providers::ClusterSites {
    network
        .spec
        .consumer_gateways
        .iter()
        .flat_map(|gateway| gateway.provider_hop_endpoints.iter())
        .filter_map(|hop| {
            let sni = hop.transport.sni.as_deref()?.trim().to_ascii_lowercase();
            let site = sni.strip_suffix(&format!(".{}", certs::SPIFFE_TRUST_DOMAIN))?;
            Some((hop.cluster.clone(), site.to_owned()))
        })
        .collect()
}

/// Where `site` serves signals, as its `spec.signalsEndpoint` says or derived from its gateway.
fn declared_signals(site: &GridSite, name: &str, signals_port: Option<u16>) -> Option<signals::DeclaredSignals> {
    Some(signals::DeclaredSignals {
        endpoint: site.signals_endpoint(signals_port)?,
        server_name: Some(site.signals_server_name(name)),
    })
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
fn publish_signals(ctx: &OperatorCtx, collected: HashMap<String, Vec<signals::Observation>>, collected_at: Instant) {
    let site = ctx.site_name().unwrap_or_default();
    let age = collected_at.elapsed();
    let attributed = collected
        .into_iter()
        .map(|(provider, mut observations)| {
            for observation in &mut observations {
                observation.age = observation.age.max(age);
            }
            let attributed = signals::attribute(observations, site, &provider);
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
    crate::metrics::set_signals_ca(&ca_pem, SystemTime::now());
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
    /// SHA-256 of the grid CA bundle it is served and verified under, so a CA change alone
    /// rebuilds what holds the material.
    pub anchors: Option<[u8; 32]>,
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
    let cert_pem = Box::pin(read_signals_pem(client, site, "tls.crt")).await?;
    let pem = std::str::from_utf8(&cert_pem).map_err(|_e| "signals TLS: certificate is not valid UTF-8".to_owned())?;
    let der = crate::resources::tls_backend::first_cert_der_from_pem(pem).map_err(str::to_owned)?;
    // The co-located gateway may present the replaced leaf until it reloads.
    let previous = Box::pin(read_signals_pem(client, site, crate::enroll::renew::PREVIOUS_CERT))
        .await
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .filter(|replaced| certs::cert_expires_within(replaced, time::Duration::ZERO) == Ok(false))
        .and_then(|replaced| crate::resources::tls_backend::first_cert_der_from_pem(&replaced).ok())
        .map(|replaced| signals::leaf_fingerprint(&replaced));
    let anchors = match &network.spec.tls.ca_secret_ref {
        Some(ca) => Some(certs::sha256(&Box::pin(read_signals_pem(client, ca, "ca.crt")).await?)),
        None => None,
    };
    Ok(Some(OwnLeaf {
        fingerprint: signals::leaf_fingerprint(&der),
        spiffe: certs::leaf_spiffe_id(&der),
        previous,
        anchors,
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

/// Load the local join-issued leaf and return its validated P-256 signing key.
async fn load_gossip_signing_key(
    network: &GridNetwork,
    client: &Client,
    site: &str,
) -> Result<zeroize::Zeroizing<Vec<u8>>, String> {
    let ca_ref = network
        .spec
        .tls
        .ca_secret_ref
        .as_ref()
        .ok_or_else(|| "Grid CA Secret is not configured".to_owned())?;
    let site_ref = network
        .spec
        .tls
        .site_secret_ref
        .as_ref()
        .ok_or_else(|| "site identity Secret is not configured".to_owned())?;
    let ca = read_signals_pem(client, ca_ref, "ca.crt").await?;
    let cert = read_signals_pem(client, site_ref, "tls.crt").await?;
    let key = zeroize::Zeroizing::new(read_signals_pem(client, site_ref, "tls.key").await?);
    validated_gossip_key(&ca, &cert, &key, site)
}

/// PEM tag of a PKCS8 private key.
const PKCS8_PEM_TAG: &str = "PRIVATE KEY";

/// Length of an uncompressed SEC1 P-256 point: a tag, then two 32-byte coordinates.
const P256_POINT_LEN: usize = 65;

/// SEC1 tag of an uncompressed point.
const SEC1_UNCOMPRESSED_TAG: u8 = 0x04;

/// Whether `public_key` has the shape of an uncompressed P-256 point.
///
/// The curve itself is enforced when a signature verifies against it.
fn is_p256_point(public_key: &[u8]) -> bool {
    public_key.len() == P256_POINT_LEN && public_key.first() == Some(&SEC1_UNCOMPRESSED_TAG)
}

/// Validate the site-bound leaf and its matching P-256 PKCS8 key, returning the key DER.
fn validated_gossip_key(ca: &[u8], cert: &[u8], key: &[u8], site: &str) -> Result<zeroize::Zeroizing<Vec<u8>>, String> {
    let ca = std::str::from_utf8(ca).map_err(|_error| "Grid CA certificate is not UTF-8".to_owned())?;
    let cert = std::str::from_utf8(cert).map_err(|_error| "site certificate is not UTF-8".to_owned())?;
    if trust_bundle::check_cert_pem(cert) != CertPemStatus::ValidStructure {
        return Err("site certificate is not public certificate PEM".to_owned());
    }
    let key_pem = std::str::from_utf8(key).map_err(|_error| "site private key is not UTF-8".to_owned())?;
    let public_key = certs::verify_site_cert(ca, cert, site).map_err(|error| error.to_string())?;

    let block = pem::parse(key_pem).map_err(|_error| "site private key is not PEM".to_owned())?;
    if block.tag() != PKCS8_PEM_TAG {
        return Err("site private key is not PKCS8".to_owned());
    }
    let der = zeroize::Zeroizing::new(block.into_contents());

    // Loading the key as the runtime will checks its curve and proves it matches the leaf.
    let derived = swim::signing::public_key_ecdsa_p256(&der)
        .map_err(|_error| "site private key is not a P-256 PKCS8 key".to_owned())?;
    if derived != public_key {
        return Err("site private key does not match its certificate".to_owned());
    }
    Ok(der)
}

/// Load and validate this site's signed-gossip identity.
async fn load_gossip_signing_identity(
    network: &GridNetwork,
    client: &Client,
    site: &str,
) -> Option<zeroize::Zeroizing<Vec<u8>>> {
    match load_gossip_signing_key(network, client, site).await {
        Ok(key) => Some(key),
        Err(reason) => {
            tracing::warn!(%reason, "signed gossip disabled: local site identity is unusable");
            None
        },
    }
}

/// Validate one peer certificate against this `GridNetwork`'s CA and trust mode.
fn verified_peer_gossip_key(site: &GridSite, ca_pem: &str, trust: PeerTrustMode) -> Result<Option<Vec<u8>>, String> {
    let Some((site_name, _)) = peer_site_key(site) else {
        return Ok(None);
    };
    let Some(cert_pem) = site
        .status
        .as_ref()
        .and_then(|status| status.public_cert_pem.as_deref())
    else {
        return Ok(None);
    };
    if trust_bundle::check_cert_pem(cert_pem) != CertPemStatus::ValidStructure {
        return Err("peer certificate is not public certificate PEM".to_owned());
    }
    let public_key = certs::verify_site_cert(ca_pem, cert_pem, &site_name).map_err(|error| error.to_string())?;
    if !is_p256_point(&public_key) {
        return Err("peer certificate does not carry an uncompressed P-256 key".to_owned());
    }
    if trust == PeerTrustMode::Pin
        && !peer_has_declared_fingerprint(
            site,
            &certs::canonical_fingerprint(cert_pem).map_err(|error| error.to_string())?,
        )
    {
        return Err("peer certificate does not match a declared canonical fingerprint".to_owned());
    }
    Ok(Some(public_key))
}

/// Whether `site` declares this certificate's canonical SHA-256 fingerprint.
fn peer_has_declared_fingerprint(site: &GridSite, fingerprint: &str) -> bool {
    site.spec
        .trust
        .as_ref()
        .and_then(|policy| policy.canonical_fingerprints.as_ref())
        .is_some_and(|pins| {
            pins.iter()
                .any(|pin| signals::canonical_fingerprint(pin) == fingerprint)
        })
}

/// This grid's signed-state mode, `off` when `peerTrust` is absent.
fn signed_state_mode(network: &GridNetwork) -> SignedStateMode {
    network
        .spec
        .peer_trust
        .as_ref()
        .map_or_else(SignedStateMode::default, |trust| trust.signed_state)
}

/// Apply this grid's signed-state mode to the SWIM runtime.
///
/// `off` drops any trust installed while enforcing, so state flows unsigned
/// again. `enforce` installs the local signer and verified peer pins.
/// Returns whether this site can sign its grid-scoped state.
async fn apply_signed_state(
    network: &GridNetwork,
    ctx: &OperatorCtx,
    swim: &SwimHandle,
    grid_id: &str,
    signed_state: SignedStateMode,
) -> Result<bool, OperatorError> {
    if signed_state == SignedStateMode::Off {
        swim.relax_grid_trust(grid_id.to_owned())
            .map_err(|error| OperatorError::InvalidResource(error.to_string()))?;
        return Ok(false);
    }

    let client = &ctx.client;

    // Peers that cannot be re-verified keep their pins: a transient read failure must not drop all state.
    let peer_pins = match collect_gossip_peer_pins(network, client, swim.site_name()).await {
        Ok(peer_pins) => Some(peer_pins),
        Err(error) => {
            tracing::warn!(%grid_id, %error, "keeping installed SWIM peer pins: peers could not be re-verified");
            None
        },
    };
    let local_signing_key = load_gossip_signing_identity(network, client, swim.site_name()).await;
    let signing_ready = local_signing_key.is_some();
    swim.set_grid_trust_snapshot(grid_id.to_owned(), local_signing_key, peer_pins)
        .map_err(|error| OperatorError::InvalidResource(error.to_string()))?;
    Ok(signing_ready)
}

/// Apply this grid's signed-state mode, then publish the certificate bootstrap it allows.
async fn sign_grid_state(
    network: &GridNetwork,
    ctx: &OperatorCtx,
    swim: &SwimHandle,
    name: &str,
) -> Result<(), OperatorError> {
    let signed_state = signed_state_mode(network);
    let Some(grid_id) = persisted_grid_id(network) else {
        // Enrollment owns grid IDs. Reconciliation waits for `spec.grid_id`
        // rather than minting a local fallback.
        return Ok(());
    };
    let signing_ready = Box::pin(apply_signed_state(network, ctx, swim, grid_id, signed_state)).await?;

    match signed_state {
        SignedStateMode::Off => Box::pin(publish_site_cert(network, &ctx.client, swim, name)).await,
        SignedStateMode::Enforce if signing_ready => {
            Box::pin(publish_signed_site_cert(network, &ctx.client, swim, grid_id, name)).await;
        },
        // An unusable local identity cannot sign; stay silent rather than send unsigned.
        SignedStateMode::Enforce => {},
    }
    Ok(())
}

/// Empty-state broadcast carrying this site's public certificate PEM.
fn site_cert_broadcast(swim: &SwimHandle, cert_pem: String) -> swim::StateBroadcast {
    swim::StateBroadcast::new(
        swim.site_name().to_owned(),
        cert_broadcast_revision(),
        crdt::GridStateSnapshot::new(swim.site_name().to_owned()),
        None,
    )
    .with_cert(Some(cert_pem))
}

/// Broadcast this site's public certificate, unsigned, on every reconcile.
///
/// Remote peers populate `GridSite.status.publicCertPem` from it. Only the
/// public cert is read; the private key (`tls.key`) is never accessed here.
async fn publish_site_cert(network: &GridNetwork, client: &Client, swim: &SwimHandle, name: &str) {
    let Ok(Some(cert_pem)) = secret::read_site_cert_pem(client, network.spec.tls.site_secret_ref.as_ref()).await else {
        return;
    };
    if let Err(error) = swim.publish_state_broadcast(site_cert_broadcast(swim, cert_pem)) {
        tracing::warn!(network = name, %error, "failed to publish site cert broadcast");
    }
}

/// Publish this site's grid-scoped certificate bootstrap on every reconcile.
///
/// The runtime signs it; peers validate the certificate, then pin its key.
/// Republishing reaches peers that joined, restarted, or evicted it since.
async fn publish_signed_site_cert(
    network: &GridNetwork,
    client: &Client,
    swim: &SwimHandle,
    grid_id: &str,
    name: &str,
) {
    let Ok(Some(cert_pem)) = secret::read_site_cert_pem(client, network.spec.tls.site_secret_ref.as_ref()).await else {
        return;
    };
    let broadcast = site_cert_broadcast(swim, cert_pem).with_grid_id(Some(grid_id.to_owned()));
    if let Err(error) = swim.publish_state_broadcast(broadcast) {
        tracing::warn!(network = name, %error, "failed to publish site cert broadcast");
    }
}


/// Collect the currently verified peer certificates as a complete trust snapshot.
async fn collect_gossip_peer_pins(
    network: &GridNetwork,
    client: &Client,
    local_site_name: &str,
) -> Result<PeerPins, OperatorError> {
    let Some(ca_ref) = network.spec.tls.ca_secret_ref.as_ref() else {
        return Ok(BTreeMap::new());
    };
    let ca = read_signals_pem(client, ca_ref, "ca.crt")
        .await
        .map_err(OperatorError::InvalidResource)?;
    let ca = std::str::from_utf8(&ca)
        .map_err(|_error| OperatorError::InvalidResource("Grid CA certificate is not UTF-8".to_owned()))?;
    let network_name = network.metadata.name.as_deref().unwrap_or_default();
    // The declared mode, not the startup one: a trust change under gossip does not restart.
    let trust = GridModes::of(network, MembershipMode::Swim).trust;
    let mut verified = PeerPins::new();
    for site in list_all_grid_sites(client)
        .await?
        .iter()
        .filter(|site| site.spec.grid_network_ref == network_name)
    {
        let Some((site_name, _)) = peer_site_key(site) else {
            continue;
        };
        if site_name == local_site_name {
            continue;
        }
        match verified_peer_gossip_key(site, ca, trust) {
            Ok(Some(key)) => {
                let keys = verified.entry(site_name).or_default();
                if !keys.contains(&key) {
                    keys.push(key);
                }
            },
            Ok(None) => {},
            Err(reason) => tracing::warn!(site = %site_name, %reason, "peer gossip signing pin refused"),
        }
    }
    Ok(verified)
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
/// the existing `clusterName`-based (Phase 1) fallback.
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
/// indicate a malformed resource; we silently skip rather than panic or
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
/// Legacy status-only IDs remain valid for withdrawing state published before migration.
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

/// Field manager for the operator's one write to `spec.gridId`.
const GRID_ID_FIELD_MANAGER: &str = "grid-operator-grid-id";

/// Tries at copying a status-only grid ID into spec before the reconcile reports the conflict.
const GRID_ID_PATCH_ATTEMPTS: usize = 3;

/// Copy a status-only grid ID into an empty `spec.gridId`, never replacing a set one.
///
/// Operators before #474 minted the ID into status only; rc.2 then stopped running such members.
/// Returns whether spec now holds a grid ID, the copied one or one another writer set first.
///
/// # Errors
///
/// [`OperatorError::InvalidResource`] when the API refuses the status ID as `spec.gridId`.
#[expect(
    clippy::large_stack_frames,
    reason = "async Kubernetes API calls carry large request and response values"
)]
async fn adopt_status_grid_id(network: &GridNetwork, client: &Client) -> Result<bool, OperatorError> {
    let name = grid_network_name(network)?;
    let api: Api<GridNetwork> = Api::all(client.clone());
    let mut latest = network.clone();
    for attempt in 0..GRID_ID_PATCH_ATTEMPTS {
        if !latest.spec.grid_id.is_empty() {
            return Ok(true);
        }
        // Spec is empty here, so this is the status ID.
        let Some(grid_id) = persisted_grid_id(&latest) else {
            return Ok(false);
        };
        let Some(resource_version) = latest.metadata.resource_version.as_deref() else {
            return Ok(false);
        };
        // The resource version makes this a compare-and-swap against a writer that sets spec first.
        let patch = serde_json::json!({
            "metadata": { "resourceVersion": resource_version },
            "spec": { "gridId": grid_id },
        });
        let params = PatchParams {
            field_manager: Some(GRID_ID_FIELD_MANAGER.to_owned()),
            ..PatchParams::default()
        };
        match api.patch(name, &params, &Patch::Merge(&patch)).await {
            Ok(_) => {
                tracing::info!(network = name, %grid_id, "copied the status grid ID into spec.gridId");
                return Ok(true);
            },
            Err(kube::Error::Api(error)) if error.code == 409 && attempt + 1 < GRID_ID_PATCH_ATTEMPTS => {
                let Some(current) = api.get_opt(name).await? else {
                    return Ok(false);
                };
                latest = current;
            },
            // The same patch cannot pass again, so only adopt a spec another writer set meanwhile.
            Err(kube::Error::Api(error)) if error.code == 422 => {
                return match api.get_opt(name).await? {
                    Some(current) if !current.spec.grid_id.is_empty() => Ok(true),
                    _ => Err(OperatorError::InvalidResource(format!(
                        "GridNetwork {name} status.gridId {grid_id:?} cannot become spec.gridId: {}; set spec.gridId \
                         by hand",
                        error.message
                    ))),
                };
            },
            Err(error) => return Err(error.into()),
        }
    }
    // Not reached: the last attempt returns from its match.
    Ok(false)
}

/// Add or remove the SWIM-withdrawal finalizer without changing other finalizers.
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
    ctx.local_providers.retain_networks(|held| held != name);
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
            // The withdrawal was the grid's last signed message; drop its signer and pins.
            if let Err(error) = swim.relax_grid_trust(grid_id.to_owned()) {
                tracing::warn!(network = name, %grid_id, %error, "failed to drop deleted GridNetwork's gossip trust");
            }
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

    // Ahead of every spec check, so an unrelated invalid field cannot keep a member status-only.
    let client = &ctx.client;
    if network.spec.grid_id.is_empty() && Box::pin(adopt_status_grid_id(&network, client)).await? {
        // The spec write triggers the next reconcile, which runs with spec as the only source.
        return Ok(Action::await_change());
    }

    reject_invalid_budget_policy(&network)?;

    tracing::debug!(name, "reconciling GridNetwork");

    // Modes are fixed at startup, so a change stands the process down and its restart applies it,
    // and nothing here runs under the old modes meanwhile.
    let running = GridModes {
        membership: ctx.membership(),
        signal: ctx.signal_mode,
        trust: ctx.peer_settings.trust,
    };
    let declared = GridModes::of(&network, ctx.membership_fallback);
    if declared.membership == MembershipMode::Local && declared.signal != SignalMode::Poll {
        return Err(OperatorError::InvalidResource(
            "Local membership requires signalTransport.mode poll".to_owned(),
        ));
    }
    if let Some(next) = running.restart_for(Some(&network), ctx.membership_fallback) {
        tracing::info!(
            "GridNetwork {name} sets membership={:?} signalTransport={:?} peerTrust={:?}; running {:?}; restarting to apply",
            next.membership,
            next.signal,
            next.trust,
            running
        );
        ctx.owe_restart(next);
        return Ok(Action::await_change());
    }
    ctx.publish_declared_trust(&network);

    if network.spec.grid_id.is_empty() {
        tracing::info!(name, "spec.gridId is not set; the grid runtime waits until it is");
        Box::pin(apply_rollup(client, &network)).await;
        return Ok(Action::await_change());
    }
    let enrolls = config_enrolls(&ctx.operator_config.current());
    let tls_retry = Box::pin(ensure_tls_secrets(&network, client, ctx.site_name.as_deref(), enrolls)).await?;

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

        Box::pin(sign_grid_state(&network, &ctx, swim, name)).await?;
    }

    // List providers and tools once; shared between the catalog, the overlay and CRDT publishing.
    let providers = list_all_inference_providers(client).await?;
    let tool_providers = list_all_agent_tool_providers(client).await?;

    // Published before any hold or render, so peers can read this site while it waits for
    // theirs, and two sites restarting together do not wait out the grace on each other.
    ctx.local_providers.replace(
        name,
        local_wire_providers(name, &providers, &tool_providers, &ctx.served_models),
    );

    // Rendering before membership converges would drop remote entries.
    if let Some(hold) = ctx.membership_hold() {
        tracing::debug!(network = name, "holding membership-derived writes until SWIM converges");
        return Ok(hold);
    }

    let mut requeue_interval = requeue_interval_for_network(&network, &providers)?;
    if network.spec.consumer_gateways.iter().any(|gateway| {
        gateway.praxis_config.as_ref().is_some_and(|config| {
            config.generate
                && config
                    .mount_reconciliation
                    .as_ref()
                    .is_some_and(|mounts| mounts.enabled)
        })
    }) {
        requeue_interval = requeue_interval.min(MOUNT_RECONCILIATION_REQUEUE);
    }
    if let Some(wait) = tls_retry {
        requeue_interval = requeue_interval.min(wait);
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

    let grid_id = network.spec.grid_id.clone();
    let remote_crdt_providers: Vec<crdt::ProviderState> = if reads_polled_catalogs(ctx.membership()) {
        ctx.peer_providers()
            .admit(name, &reserved_clusters(&network, &providers), &hop_sites(&network))
    } else {
        ctx.swim()
            .map(|swim| collect_remote_crdt_providers(swim, name))
            .unwrap_or_default()
    };
    let remote_model_inventories = ctx.swim().map(|swim| swim.model_inventories()).unwrap_or_default();

    // Obtain a live membership snapshot here - used both for staleness override below
    // and for phase determination after the overlay step.
    // When swim is None (runtime not configured), falls through to static phase logic.
    let swim_runtime_running = ctx.swim().is_none_or(|handle| handle.is_running());
    let membership = ctx.swim().map(|h| h.snapshot());

    let remote_crdt_providers = membership_staleness(&network, remote_crdt_providers, membership.as_ref());

    let scoring_weights = crate::crd::grid_network::resolve_scoring_weights(network.spec.scoring_policy.as_ref());
    let serving = serving_source(&ctx, &providers);

    let OverlayOutcome {
        consumer_statuses: consumer_config_statuses,
        mount_statuses: mount_reconciliation_statuses,
        overlay_statuses,
        serving_retry,
        inventory_expiry_deadline,
    } = Box::pin(reconcile_routing_overlay_inner(
        &network,
        &grid_id,
        client,
        &providers,
        &tool_providers,
        &remote_crdt_providers,
        &remote_model_inventories,
        &ctx.served_models,
        &raw_metrics,
        &scoring_weights,
        &admission_states,
        serving.as_ref(),
        ctx.site_name(),
        reads_polled_catalogs(ctx.membership()),
    ))
    .await?;

    // An expired or unreadable identity, as IdentityValid reports it, degrades the network.
    let identity_failed = network.status.as_ref().is_some_and(|status| {
        held_condition(&status.conditions, crate::enroll::local::IDENTITY_VALID)
            .is_some_and(|valid| matches!(valid.reason.as_str(), "CertificateExpired" | "CertificateUnreadable"))
    });
    let phase = if swim_runtime_running && !identity_failed {
        determine_phase(&network, &grid_id, membership.as_ref(), ctx.membership())
    } else {
        GridNetworkPhase::Degraded
    };

    // Publish provider CRDT state so peers learn this site's providers.
    let remote_provider_count = if let Some(swim) = ctx.swim().filter(|handle| handle.is_running()) {
        publish_real_provider_state(
            swim,
            name,
            &grid_id,
            &providers,
            &tool_providers,
            &raw_metrics,
            &ctx.served_models,
        );
        log_capacity_changes(&ctx.logged, name, &providers);
        count_remote_provider_records(swim, name)
    } else {
        0
    };

    // Resolve per-tenant budget status from the merged CRDT spend state, if any.
    // Empty tenant_spend (SWIM disabled, or no spend broadcast received yet) is
    // indistinguishable here from "no spend recorded"; resolve_budget_statuses
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
        remote_provider_count,
        consumer_config_statuses,
        mount_reconciliation_statuses,
        overlay_statuses,
        budget_statuses,
    )
    .await?;
    Box::pin(apply_rollup(client, &network)).await;

    // Auto-create or update GridSite records for remote Alive SWIM members.
    // Only runs when the GridNetwork explicitly opts in via LABEL_AUTO_DISCOVER_SITES.
    // This gate prevents auto-discovery from changing overlay generation semantics
    // for networks that use the existing clusterName-based (Phase 1) path.
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

    // A deferred serving write lands as soon as its spacing allows. Discovered inventory
    // candidates also set a deadline so both generated router configs are refreshed on expiry.
    let retry = soonest_reconcile_retry(
        requeue_interval,
        serving_retry,
        inventory_expiry_deadline,
        time::OffsetDateTime::now_utc(),
    );
    Ok(Action::requeue(retry))
}

/// Requeue at the earliest discovered-model deadline still present in a gateway overlay.
///
/// The next reconcile re-renders each router config after the inventory renderer has dropped
/// expired models. This covers both the generated Praxis router and the embedded Grid router.
fn next_discovered_model_expiry(overlay: &routing_overlay::RoutingOverlay) -> Option<time::OffsetDateTime> {
    overlay
        .candidates
        .iter()
        .chain(&overlay.excluded)
        .filter_map(|candidate| {
            let expires_at = candidate.expires_at.as_deref()?;
            time::OffsetDateTime::parse(expires_at, &time::format_description::well_known::Rfc3339).ok()
        })
        .min()
}

/// Convert an absolute inventory deadline to a positive controller requeue delay.
fn wait_until_deadline(deadline: time::OffsetDateTime, now: time::OffsetDateTime) -> Duration {
    let wait_nanos = deadline
        .unix_timestamp_nanos()
        .saturating_sub(now.unix_timestamp_nanos())
        .max(1);
    let wait_millis = wait_nanos.saturating_add(999_999) / 1_000_000;
    Duration::from_millis(u64::try_from(wait_millis).unwrap_or(u64::MAX))
}

/// Wake well before the gateway's 30-second expiry cutoff, retrying briefly when inside the lead.
fn inventory_expiry_retry(deadline: time::OffsetDateTime, now: time::OffsetDateTime) -> Duration {
    const RECONCILE_LEAD: Duration = Duration::from_secs(60);
    const LATE_RETRY_MAX: Duration = Duration::from_secs(10);

    if deadline <= now {
        return Duration::from_secs(1);
    }
    let until_deadline = wait_until_deadline(deadline, now);
    if until_deadline > RECONCILE_LEAD {
        return until_deadline.saturating_sub(RECONCILE_LEAD);
    }
    let half_wait = Duration::from_nanos(u64::try_from(until_deadline.as_nanos() / 2).unwrap_or(0));
    half_wait.min(LATE_RETRY_MAX).max(Duration::from_nanos(1))
}

/// Choose the earliest normal, deferred-write, or discovered-inventory requeue.
fn soonest_reconcile_retry(
    normal_retry: Duration,
    serving_retry: Option<Duration>,
    inventory_expiry_deadline: Option<time::OffsetDateTime>,
    now: time::OffsetDateTime,
) -> Duration {
    let expiry_retry = inventory_expiry_deadline.map(|deadline| inventory_expiry_retry(deadline, now));
    [Some(normal_retry), serving_retry, expiry_retry]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(normal_retry)
}

/// Manager for the `GridNetwork`'s `Available`, `Progressing` and `Degraded`, written only here.
pub const ROLLUP_FIELD_MANAGER: &str = "grid-operator-rollup";

/// What raises the `GridNetwork`'s `Degraded`, each with the status that does, the first held
/// read: this site's own identity and clocks first, then its view of its peers. None gates
/// `Available`, since peers being down leaves this site available.
const DEGRADED_BY: [(&str, ConditionStatus); 7] = [
    (crate::enroll::local::ROTATION_ON_SCHEDULE, ConditionStatus::False),
    (crate::peer_ages::PEER_TIME_VALID, ConditionStatus::False),
    (crate::gateway_ages::GATEWAY_AGES_REJECTED, ConditionStatus::True),
    (crate::peer_reach::PEER_SIGNALS_REACHABLE, ConditionStatus::False),
    (crate::peer_reach::PEER_CATALOGS_ACCEPTED, ConditionStatus::False),
    (crate::peer_membership::PEER_MEMBERSHIP, ConditionStatus::False),
    (crate::peer_reach::PEER_POLLS_COMPLETE, ConditionStatus::False),
];

/// The network's roll-up: the enrollment step's and the renewal loop's conditions gate it, and
/// whatever [`DEGRADED_BY`] holds degrades it.
pub(crate) fn network_rollup(network: &GridNetwork) -> Vec<Condition> {
    let Ok(now) = time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339) else {
        return Vec::new();
    };
    let held = network
        .status
        .as_ref()
        .map_or(&[][..], |status| status.conditions.as_slice());
    let stamp = rollup::Stamp {
        generation: network.metadata.generation,
        now: &now,
    };
    let enrollment = enrollment_gates(held, stamp);
    let gates: Vec<&Condition> = enrollment.iter().collect();
    let degraded = DEGRADED_BY
        .iter()
        .find_map(|(type_, bad)| held_condition(held, type_).filter(|condition| condition.status == *bad))
        .map(|condition| (condition.reason.as_str(), condition.message.as_str()));
    rollup::trio(held, &gates, (degraded, crate::enroll::local::in_flight(held)), stamp).into()
}

/// The enrollment step's and the renewal loop's gates as `held` has them, one not written
/// yet reading `Unknown`.
fn enrollment_gates(held: &[Condition], stamp: rollup::Stamp<'_>) -> Vec<Condition> {
    crate::enroll::local::GATES
        .iter()
        .map(|type_| {
            held_condition(held, type_).cloned().unwrap_or_else(|| {
                let unwritten = (
                    ConditionStatus::Unknown,
                    "AwaitingFirstCheck",
                    "not evaluated yet".to_owned(),
                );
                rollup::condition(&[], type_, unwritten, stamp)
            })
        })
        .collect()
}

/// Write `network`'s roll-up under [`ROLLUP_FIELD_MANAGER`] when it changed. A failure is
/// logged and retried on the next reconcile: the roll-up reports, it never gates.
pub async fn apply_rollup(client: &Client, network: &GridNetwork) {
    let Some(name) = network.metadata.name.as_deref() else {
        return;
    };
    let trio = network_rollup(network);
    let held = network
        .status
        .as_ref()
        .map_or(&[][..], |status| status.conditions.as_slice());
    if !rollup::differs(held, &trio) {
        return;
    }
    let patch = serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": name },
        "status": { "conditions": trio }
    });
    let params = PatchParams::apply(ROLLUP_FIELD_MANAGER).force();
    let api: Api<GridNetwork> = Api::all(client.clone());
    if let Err(error) = Box::pin(api.patch_status(name, &params, &Patch::Apply(patch))).await {
        tracing::warn!(grid_network = name, %error, "writing the roll-up failed; retrying next reconcile");
    }
}

/// The condition of `type_` in `held`.
fn held_condition<'held>(held: &'held [Condition], type_: &str) -> Option<&'held Condition> {
    held.iter().find(|condition| condition.type_ == type_)
}

/// Advance the explicitly configured local [`GridSite`] into discovery.
///
/// Remote sites are created from SWIM membership, but the local site is
/// intentionally excluded from that discovery list. When a topology provides
/// its local `GridSite` declaratively, this controller must still hand it to the
/// `GridSite` controller for gateway probing. The status write is idempotent and
/// only applies while the site is still pending.
async fn reconcile_local_site(network_name: &str, local_site: &str, client: &Client) -> Result<(), OperatorError> {
    let api: Api<GridSite> = Api::all(client.clone());
    let Ok(site) = Box::pin(api.get(local_site)).await.map(Box::new) else {
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
    Box::pin(api.patch_status(
        local_site,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(&status_doc),
    ))
    .await?;
    Ok(())
}

/// Apply `spec.tls.swimKeySecretRef` before any reconcile-triggered SWIM send.
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
    let Some(swim_key_secret_ref) = &network.spec.tls.swim_key_secret_ref else {
        return release_plain_if_undeclared(client, swim, network_name).await;
    };

    let key = secret::read_swim_key(client, swim_key_secret_ref)
        .await
        .map_err(|e| OperatorError::SwimKeyConfig(format!("failed to read swimKeySecretRef for {network_name}: {e}")))?
        .ok_or_else(|| {
            OperatorError::SwimKeyConfig(format!(
                "swimKeySecretRef for {network_name} did not resolve to a valid 32-byte key \
                 (secret={}/{}, key={})",
                swim_key_secret_ref.namespace,
                swim_key_secret_ref.name,
                swim_key_secret_ref.key.as_deref().unwrap_or("key")
            ))
        })?;

    swim.set_swim_key(key).map_err(|e| {
        OperatorError::SwimKeyConfig(format!("failed to apply swimKeySecretRef for {network_name}: {e}"))
    })?;
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
            "no GridNetwork declares a swimKeySecretRef; SWIM gossip is plaintext"
        );
    }
    Ok(())
}

/// Whether any of `networks` declares a SWIM key.
#[must_use]
pub fn declares_swim_key(networks: &[GridNetwork]) -> bool {
    networks
        .iter()
        .any(|network| network.spec.tls.swim_key_secret_ref.is_some())
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
/// I/O, suitable for unit tests and for logging seed changes between reconciles.
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
/// idempotent: foca ignores redundant joins.
///
/// # Runtime update semantics
///
/// Seeds added to `spec.seeds` since the last reconcile are logged as
/// additions via [`diff_seed_sets`].  Seeds removed from `spec.seeds` are
/// logged as removals but are **not actively disconnected**; the SWIM
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
            "CRD seeds removed from spec; no active disconnect (SWIM failure detection handles stale peers)"
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
    /// Neither exists, and the operator config names this site, so enrollment writes both.
    Enrolls,
}

/// Decide from which of the CA and site Secrets exist.
fn tls_secrets_action(ca_exists: bool, site_exists: bool) -> TlsSecrets {
    match (ca_exists, site_exists) {
        (true, true) => TlsSecrets::Present,
        (false, false) => TlsSecrets::Create,
        _ => TlsSecrets::Inconsistent,
    }
}

/// How long a new `GridNetwork` waits before the operator self-signs for it, so an operator
/// config applied with it, naming this site, is seen first.
const SELF_SIGN_GRACE: Duration = Duration::from_secs(30);

/// The wait left before self-signing for a `GridNetwork` created at `created_s`, `None` once
/// the grace has passed. Both in Unix seconds.
fn self_sign_wait(created_s: Option<i64>, now_s: i64) -> Option<Duration> {
    let age = u64::try_from(now_s.saturating_sub(created_s?)).unwrap_or(0);
    SELF_SIGN_GRACE
        .checked_sub(Duration::from_secs(age))
        .filter(|left| !left.is_zero())
}

/// `action`, unless it would create Secrets enrollment writes: a placeholder would read
/// as enrolled, and the site would never enroll.
const fn unless_enrolling(action: TlsSecrets, enrolls: bool) -> TlsSecrets {
    match action {
        TlsSecrets::Create if enrolls => TlsSecrets::Enrolls,
        held @ (TlsSecrets::Present | TlsSecrets::Create | TlsSecrets::Inconsistent | TlsSecrets::Enrolls) => held,
    }
}

/// Whether the operator config names this site, so enrollment writes its Secrets. A refused
/// config holds off too, and so does one that named the site earlier in this process: a
/// placeholder would read as enrolled once the config is fixed or restored.
fn config_enrolls(config: &crate::operator_config::Snapshot) -> bool {
    config.site_seen
}

/// Create a self-signed CA and site certificate when neither Secret exists.
///
/// Generates both together so the CA is available for signing the site
/// certificate. Never replaces an existing CA: one that enrollment or bootstrap
/// wrote is the grid's, and a new one would split it.
#[expect(
    clippy::large_stack_frames,
    clippy::cognitive_complexity,
    reason = "decide, then create the CA and the identity it signs, as one step"
)]
async fn ensure_tls_secrets(
    network: &GridNetwork,
    client: &Client,
    this_site: Option<&str>,
    enrolls: bool,
) -> Result<Option<Duration>, OperatorError> {
    let tls = &network.spec.tls;
    let (Some(ca_ref), Some(site_ref)) = (&tls.ca_secret_ref, &tls.site_secret_ref) else {
        return Ok(None);
    };

    let ca_api: Api<Secret> = Api::namespaced(client.clone(), &ca_ref.namespace);
    let site_api: Api<Secret> = Api::namespaced(client.clone(), &site_ref.namespace);

    let ca_exists = ca_api.get_opt(&ca_ref.name).await?.is_some();
    let site_exists = site_api.get_opt(&site_ref.name).await?.is_some();

    let mut action = tls_secrets_action(ca_exists, site_exists);
    if action == TlsSecrets::Create {
        let created = network.metadata.creation_timestamp.as_ref().map(|at| at.0.as_second());
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        if let Some(wait) = self_sign_wait(created, now) {
            tracing::debug!(
                wait_s = wait.as_secs(),
                "waiting for the operator config before self-signing"
            );
            return Ok(Some(wait));
        }
        // A request enrollment kept outlives a restart that lost the config, so it holds too.
        let enrolling = enrolls || crate::enroll::in_flight_for(&site_api, &site_ref.name).await?;
        action = unless_enrolling(action, enrolling);
    }
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
        return Ok(None);
    }

    let Some(site_name) = issued_site_name(network, this_site) else {
        // The network name is a Kubernetes object name, which may carry dots and run to 253
        // characters, so it is not always a site name. Self-signing is a convenience; refusing
        // it leaves the site to enroll rather than failing every later step in this reconcile.
        tracing::warn!(
            network = %network_site_name(network),
            "no valid site name for a self-signed identity; enroll this site instead"
        );
        return Ok(None);
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
        return Ok(None);
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
    Ok(None)
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
/// renders one overlay `ConfigMap` per `consumerGateways` entry.  Each gateway may
/// declare its own `siteName` — the `local_site` in the overlay for
/// gateway G is `G.siteName ?? network_name`.  This ensures that in a
/// multi-gateway network each gateway's overlay identifies the correct local
/// site.  A network with no `consumerGateways` is a no-op.
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
    reason = "scoring weights and served-model state are threaded through overlay rendering without hiding their use"
)]
async fn reconcile_routing_overlay_inner(
    network: &GridNetwork,
    grid_id: &str,
    client: &Client,
    providers: &[InferenceProvider],
    tool_providers: &[AgentToolProvider],
    remote_crdt_providers: &[crdt::ProviderState],
    remote_model_inventories: &[swim::ModelInventory],
    served_model_store: &served_models::ServedModelStore,
    raw_metrics: &HashMap<String, scoring::BackendMetrics>,
    scoring_weights: &scoring::ScoringWeights,
    admission_states: &HashMap<String, crate::resources::geography::AdmissionState>,
    serving: Option<&ServingSource<'_>>,
    owning_site: Option<&str>,
    member_catalogs: bool,
) -> Result<OverlayOutcome, OperatorError> {
    let network_name = grid_network_name(network)?;
    let signal = GridModes::of(network, MembershipMode::Swim).signal;

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
    let mut inventory_expiry_deadline: Option<time::OffsetDateTime> = None;
    // Peer records are the same for every gateway; only Local reads them from verified peers.
    let peer_catalog = member_catalogs.then(|| {
        derived_topology::peer_catalog(filter_eligible_remote_crdt_providers(
            network_name,
            &sites,
            remote_crdt_providers,
            signal,
        ))
    });

    for gw_ref in &network.spec.consumer_gateways {
        // Each gateway identifies its own local site.  Fall back to the
        // network name for single-site deployments where the two are equal.
        let local_site = gw_ref.site_name.as_deref().unwrap_or(network_name);

        // Filter remote CRDT providers to those whose source GridSite is Active.
        // Providers from sites in any other phase (Discovered, Connecting, Pending,
        // Unreachable, Left, or missing) are excluded before the overlay is rendered.
        // This is the routing eligibility gate: SWIM membership alone does not
        // make a remote site routable.
        let eligible_remote: Vec<&crdt::ProviderState> =
            filter_eligible_remote_crdt_providers(network_name, &sites, remote_crdt_providers, signal);
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
        let mut overlay = match routing_overlay::render_routing_overlay_with_admission(
            network,
            &sites,
            providers,
            tool_providers,
            &eligible_remote_owned,
            remote_model_inventories,
            grid_id,
            local_site,
            owning_site,
            metrics_arg,
            timestamp.as_deref(),
            scoring_weights,
            Some(admission_states),
            served_model_store,
            if gw_ref.supports_model_discovery_contract() {
                routing_overlay::CandidateExpirySupport::Supported
            } else {
                routing_overlay::CandidateExpirySupport::Unsupported
            },
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
                    "RoutingMapRenderFailed",
                    "overlay render failed",
                ));
                continue;
            },
        };
        if let Some(deadline) = next_discovered_model_expiry(&overlay) {
            inventory_expiry_deadline =
                Some(inventory_expiry_deadline.map_or(deadline, |current| current.min(deadline)));
        }
        let (cluster_endpoints, derived_summary, refused) = gateway_cluster_endpoints(
            gw_ref,
            &overlay.candidates,
            &derived_topology::Declarations {
                providers,
                sites: &sites,
                local_site,
                network_name,
                from_providers: &[],
                transport: None,
                signal,
                peer_catalog: peer_catalog.as_ref(),
            },
        );
        if !refused.is_empty() {
            tracing::warn!(
                network = network_name,
                gateway = %gw_ref.name,
                refused = refused.len(),
                "derivation refused candidates; withdrawing them from this gateway"
            );
        }
        withdraw_refused(&mut overlay, &refused);
        let site_route = gw_ref
            .praxis_config
            .as_ref()
            .filter(|cc| cc.generate && cc.site_router == SiteRouter::GridSiteRoute);
        let hops = site_route.map_or(HopSource::Declared, |_| HopSource::Rendered(&cluster_endpoints));
        if let Some(cc) = site_route
            && let Some(missing) = site_route_unmet(gw_ref, cc, serving.is_some())
        {
            let error = OperatorError::ConsumerConfigRender(ConsumerConfigError::SiteRouteUnsupported(missing));
            tracing::warn!(network = network_name, gateway = %gw_ref.name, %error, "consumer Praxis config not rendered");
            consumer_statuses.push(consumer_config_status_error(gw_ref, cc, &error, observed_generation));
            overlay_statuses.push(retained_overlay_status(
                network,
                gw_ref,
                observed_generation,
                None,
                "SiteRouteUnsupported",
                &missing.to_string(),
            ));
            continue;
        }
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
                    "RoutingMapRenderFailed",
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
            && let Some(cc) = gw_ref.praxis_config.as_ref().filter(|cc| cc.generate)
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
        let mut awaiting_rollout = false;
        if !no_candidates
            && let Some(cc) = gw_ref
                .praxis_config
                .as_ref()
                .filter(|cc| cc.generate && cc.mount_reconciliation.as_ref().is_some_and(|mounts| mounts.enabled))
        {
            match Box::pin(apply_consumer_config_for_gateway(
                &overlay,
                network_name,
                gw_ref,
                cc,
                &cluster_endpoints,
                &network.spec.tls,
                observed_generation,
                client,
            ))
            .await
            {
                Ok(outcome) => {
                    if outcome.config_applied && (!credential_bearing || cc.supports_projected_credentials) {
                        consumer_statuses.push(consumer_config_status_rendered(
                            gw_ref,
                            cc,
                            observed_generation,
                            &derived_summary,
                        ));
                    }
                    if let Some(status) = outcome.mount_status {
                        delegated_mount_reconciled = !status.applied_revision.is_empty();
                        awaiting_rollout = status.phase == MountReconciliationPhase::WaitingForRollout;
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
                // A serving file no pod has started on serves nothing, and the gateway cannot
                // start, nor its rollout finish, without one, so a missing one is written now.
                if let Some(source) = serving.filter(|_| awaiting_rollout) {
                    let target = ServingTarget {
                        gw_ref,
                        hops,
                        bootstrap: true,
                    };
                    let written = apply_serving_config(&overlay, source, network_name, target, client).await;
                    fold_serving_write(&mut serving_retry, written, network_name, gw_ref);
                }
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
            && let Some(cc) = gw_ref.praxis_config.as_ref().filter(|cc| cc.generate)
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
                    "RoutingMapApplyFailed",
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
            let target = ServingTarget {
                gw_ref,
                hops,
                bootstrap: false,
            };
            let written = apply_serving_config(&overlay, source, network_name, target, client).await;
            fold_serving_write(&mut serving_retry, written, network_name, gw_ref);
        }

        // Gateways with praxisConfig.generate=false get a Disabled entry.
        // Gateways without a praxisConfig block are omitted from status.
        if !delegated_mount_reconciled && let Some(cc) = gw_ref.praxis_config.as_ref().filter(|cc| cc.generate) {
            match Box::pin(apply_consumer_config_for_gateway(
                &overlay,
                network_name,
                gw_ref,
                cc,
                &cluster_endpoints,
                &network.spec.tls,
                observed_generation,
                client,
            ))
            .await
            {
                Ok(outcome) => {
                    if outcome.config_applied && (!credential_bearing || cc.supports_projected_credentials) {
                        consumer_statuses.push(consumer_config_status_rendered(
                            gw_ref,
                            cc,
                            observed_generation,
                            &derived_summary,
                        ));
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
                    consumer_statuses.push(with_derived_summary(
                        consumer_config_status_error(gw_ref, cc, &e, observed_generation),
                        &derived_summary,
                    ));
                    let delegation = cc.mount_reconciliation.as_ref().filter(|mounts| mounts.enabled);
                    mount_statuses.push(mount_reconciliation_status_error(
                        gw_ref,
                        delegation,
                        &e,
                        observed_generation,
                    ));
                },
            }
        } else if let Some(cc) = gw_ref.praxis_config.as_ref().filter(|cc| !cc.generate) {
            consumer_statuses.push(consumer_config_status_disabled(gw_ref, cc, observed_generation));
        }
    }
    Ok(OverlayOutcome {
        consumer_statuses,
        mount_statuses,
        overlay_statuses,
        serving_retry,
        inventory_expiry_deadline,
    })
}

/// Legacy status reason used by retained-status compatibility tests.
#[cfg(test)]
const EMPTY_CANDIDATES: &str = "EmptyCandidates";

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
    /// Soonest a discovered model inventory expires.
    inventory_expiry_deadline: Option<time::OffsetDateTime>,
}

/// What the serving config renders from, present only under poll.
struct ServingSource<'src> {
    /// Write coalescing state.
    gate: &'src WriteGate,
    /// Peer addressing resolved at startup.
    settings: &'src PeerSettings,
    /// How often this operator scrapes its providers.
    scrape_interval: Duration,
    /// Sites each network's serving config last refused.
    refused: &'src serving_config::RefusedSites,
    /// Peer providers the poller has heard load for.
    load_heard: &'src signals::LoadHeard,
    /// This site's provider clusters with no metrics to scrape.
    quiet_local: BTreeSet<String>,
}

/// Build the serving source, `None` outside poll mode.
fn serving_source<'ctx>(ctx: &'ctx OperatorCtx, providers: &[InferenceProvider]) -> Option<ServingSource<'ctx>> {
    if ctx.signal_mode != SignalMode::Poll {
        return None;
    }
    let quiet_local = providers
        .iter()
        // The same plan signal publication scrapes by, so a provider it cannot scrape publishes none.
        .filter(|provider| provider_metrics::signal_scrape_plan(provider).is_none())
        .filter_map(|provider| routing_overlay::routing_identity(provider))
        .map(str::to_owned)
        .collect();
    Some(ServingSource {
        gate: &ctx.serving_writes,
        settings: &ctx.peer_settings,
        scrape_interval: ctx.scrape_interval,
        refused: &ctx.refused_sites,
        load_heard: &ctx.load_heard,
        quiet_local,
    })
}

/// The first `grid_site_route` prerequisite this gateway does not meet.
fn site_route_unmet(gw_ref: &GatewayRef, cc: &ConsumerConfig, polling: bool) -> Option<SiteRoutePrerequisite> {
    if !polling {
        Some(SiteRoutePrerequisite::PollTransport)
    } else if !cc.mount_reconciliation.as_ref().is_some_and(|mounts| mounts.enabled) {
        Some(SiteRoutePrerequisite::MountReconciliation)
    } else if !gw_ref.provider_hop_endpoints.is_empty() {
        Some(SiteRoutePrerequisite::DeclaredProviderHops)
    } else {
        None
    }
}

/// Render the serving config text for one gateway, including authoritative no-route state.
fn render_serving_text(
    overlay: &routing_overlay::RoutingOverlay,
    source: &ServingSource<'_>,
    gw_ref: &GatewayRef,
    hops: HopSource<'_>,
) -> Result<String, OperatorError> {
    let tls_mount = gw_ref
        .praxis_config
        .as_ref()
        .map_or(crate::crd::grid_network::DEFAULT_TLS_CERT_MOUNT_PATH, |cc| {
            cc.tls_cert_mount_path.as_str()
        });
    let provider_hop_sni = serving_hop_sni(overlay, gw_ref, hops)?;
    let provider_hop_clusters: BTreeSet<String> = provider_hop_sni.keys().cloned().collect();
    let heard = |site: &str, cluster: &str| source.load_heard.heard(site, cluster);
    let inputs = ServingInputs {
        provider_hop_clusters: &provider_hop_clusters,
        provider_hop_sni: &provider_hop_sni,
        tls_mount,
        local_signals_addr: source.settings.local_signals_addr.as_deref(),
        scrape_interval: source.scrape_interval,
        peer_interval: source.settings.peer_interval,
        peer_budget: source.settings.peer_budget,
        heard: &heard,
        quiet_local: &source.quiet_local,
        inject_local_credentials: matches!(hops, HopSource::Rendered(_)),
    };
    serving_config::to_text(&serving_config::render(overlay, &inputs)).map_err(OperatorError::Json)
}

/// Where a gateway's serving config takes its provider hops from.
#[derive(Clone, Copy)]
enum HopSource<'endpoints> {
    /// The declared `providerHopEndpoints`.
    Declared,
    /// The mutual TLS endpoints a generated `grid_site_route` config renders, so the gateway
    /// can bind each one. Its local credentials ride along for the router to select.
    Rendered(&'endpoints [ClusterEndpointConfig]),
}

/// The serving config's provider hops, each with the SNI its backend verifies.
fn serving_hop_sni(
    overlay: &routing_overlay::RoutingOverlay,
    gw_ref: &GatewayRef,
    hops: HopSource<'_>,
) -> Result<BTreeMap<String, String>, OperatorError> {
    match hops {
        HopSource::Rendered(endpoints) if !overlay.candidates.is_empty() => {
            consumer_config::provider_hops(endpoints).map_err(OperatorError::ConsumerConfigRender)
        },
        HopSource::Rendered(_) => Ok(BTreeMap::new()),
        HopSource::Declared => serving_provider_hop_sni_for_overlay(overlay, gw_ref),
    }
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

/// One gateway's serving config write.
#[derive(Clone, Copy)]
struct ServingTarget<'target> {
    /// The gateway it is written for.
    gw_ref: &'target GatewayRef,
    /// Where its provider hops come from.
    hops: HopSource<'target>,
    /// Write only a missing one, for a gateway whose first pod needs it to start.
    bootstrap: bool,
}

/// Fold one serving write into the soonest retry, requeueing a failed one.
fn fold_serving_write(
    retry: &mut Option<Duration>,
    written: Result<Option<Duration>, OperatorError>,
    network_name: &str,
    gw_ref: &GatewayRef,
) {
    let next = written.unwrap_or_else(|error| {
        tracing::warn!(network = network_name, gateway = %gw_ref.name, %error, "serving config apply failed");
        Some(SERVING_RETRY_REQUEUE)
    });
    *retry = retry.iter().copied().chain(next).min();
}

/// Apply the serving config `ConfigMap` when changed, returning when to retry a deferred write.
async fn apply_serving_config(
    overlay: &routing_overlay::RoutingOverlay,
    source: &ServingSource<'_>,
    network_name: &str,
    target: ServingTarget<'_>,
    client: &Client,
) -> Result<Option<Duration>, OperatorError> {
    let ServingTarget {
        gw_ref,
        hops,
        bootstrap,
    } = target;
    serving_config::warn_refused_sites(overlay, network_name, source.refused);
    let text = render_serving_text(overlay, source, gw_ref, hops)?;
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
    let decision = if bootstrap && existing.is_some() {
        WriteDecision::Unchanged
    } else if overlay.candidates.is_empty() {
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

/// Whether the last recorded status for this gateway already had no candidates.
#[cfg(test)]
fn already_empty(network: &GridNetwork, gw_ref: &GatewayRef) -> bool {
    network.status.as_ref().is_some_and(|status| {
        status.routing_map_status.iter().any(|entry| {
            entry.gateway_name == gw_ref.name && entry.namespace == gw_ref.namespace && entry.reason == EMPTY_CANDIDATES
        })
    })
}

/// Find the last successfully distributed overlay status for a gateway.
fn find_prior_overlay<'net>(network: &'net GridNetwork, gw_ref: &GatewayRef) -> Option<&'net OverlayRevisionStatus> {
    network.status.as_ref().and_then(|status| {
        status
            .routing_map_status
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
    let prior = current.map_or(&[][..], |status| status.routing_map_status.as_slice());
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
fn keep_consumer_config_status_at(
    network: &GridNetwork,
    desired: &[ConsumerConfigStatus],
) -> Vec<ConsumerConfigStatus> {
    let prior = network
        .status
        .as_ref()
        .map_or(&[][..], |status| status.praxis_config_status.as_slice());
    let mut kept = Vec::new();
    for gw_ref in &network.spec.consumer_gateways {
        let Some(cc) = gw_ref.praxis_config.as_ref() else {
            continue;
        };
        if !cc.generate {
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
    for gw_ref in &network.spec.consumer_gateways {
        let Some(cc) = gw_ref.praxis_config.as_ref().filter(|cc| cc.generate) else {
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

/// The 503 config a delegated gateway's first pod starts on.
fn bootstrap_config_map(name: &str, cc: &ConsumerConfig, network_name: &str, gw_ref: &GatewayRef) -> ConfigMap {
    consumer_config::build_consumer_config_map(
        &consumer_config::bootstrap_praxis_config(cc.listener_port),
        name,
        &gw_ref.namespace,
        network_name,
        &gw_ref.name,
    )
}

/// Create the bootstrap config, keeping one another writer created first.
async fn create_bootstrap_config(
    api: &Api<ConfigMap>,
    name: &str,
    cc: &ConsumerConfig,
    network_name: &str,
    gw_ref: &GatewayRef,
) -> Result<(), OperatorError> {
    match api
        .create(
            &kube::api::PostParams::default(),
            &bootstrap_config_map(name, cc, network_name, gw_ref),
        )
        .await
    {
        Ok(_) => info!(config_map = %name, "bootstrapped the gateway's first Praxis config"),
        Err(kube::Error::Api(response)) if response.code == 409 => {},
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

/// Replace a bootstrap left in the chart's own slot after the first flip, so a revert to that
/// slot serves `config_yaml` rather than 503.
async fn retire_bootstrap_slot(
    api: &Api<ConfigMap>,
    cc: &ConsumerConfig,
    config_yaml: &str,
    network_name: &str,
    gw_ref: &GatewayRef,
) -> Result<(), OperatorError> {
    let base = &cc.config_map_name;
    let bootstrap = bootstrap_config_map(base, cc, network_name, gw_ref);
    if api
        .get_opt(base)
        .await?
        .is_some_and(|current| current.data == bootstrap.data)
    {
        let cm = consumer_config::build_consumer_config_map(
            config_yaml,
            base,
            &gw_ref.namespace,
            network_name,
            &gw_ref.name,
        );
        api.patch(base, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(&cm))
            .await?;
        info!(config_map = %base, "replaced the retired bootstrap Praxis config");
    }
    Ok(())
}

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
    clippy::large_stack_frames,
    reason = "render result crosses the optional delegated async boundary"
)]
async fn apply_consumer_config_for_gateway(
    overlay: &routing_overlay::RoutingOverlay,
    network_name: &str,
    gw_ref: &GatewayRef,
    cc: &ConsumerConfig,
    cluster_endpoints: &[ClusterEndpointConfig],
    tls: &crate::crd::grid_network::TlsConfig,
    observed_generation: i64,
    client: &Client,
) -> Result<ConsumerApplyOutcome, OperatorError> {
    let rendered = consumer_config::render_consumer_config_with_projected(
        overlay,
        &cc.credential_mount_path,
        cluster_endpoints,
        &cc.tls_cert_mount_path,
        cc.listener_port,
        tls,
        &gw_ref.name,
        &gw_ref.namespace,
        cc.telemetry.as_ref(),
        cc.enable_projected_credentials,
        cc.mount_reconciliation.as_ref().is_some_and(|mounts| mounts.enabled),
        cc.site_router,
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
            format!("gridServing requires praxisConfig.tlsCertMountPath={GRID_SERVING_TLS_MOUNT_PATH:?}"),
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

/// Map an apiserver error on the delegated Deployment to what it means for the mount status.
///
/// A conflict stays a kube error so the caller re-reads and retries. A 403 preserves the
/// apiserver refusal so authorization and admission denials remain distinguishable.
/// Anything else is `fallback`.
fn delegated_deployment_failure(
    error: kube::Error,
    verb: &'static str,
    fallback: crate::error::GatewayMountFailure,
) -> OperatorError {
    if matches!(&error, kube::Error::Api(status) if status.code == 409) {
        return error.into();
    }
    if matches!(&error, kube::Error::Api(status) if status.code == 403) {
        return mount_failure(
            "DeploymentForbidden",
            format!(
                "the apiserver refused {verb} on the delegated Deployment: {error}. Inspect the refusal for \
                 authorization or admission-policy denial. If RBAC denied access, verify the operator's \
                 ServiceAccount has get and patch on that one Deployment (the praxis-gateway chart renders \
                 the Role when mountReconciliation is enabled; otherwise create one in the gateway namespace), \
                 and check that deploymentName is included in the name-restricted grant"
            ),
        )
        .into();
    }
    fallback.into()
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
    deployments
        .patch(name, &PatchParams::default(), &Patch::Strategic(patch))
        .await
        .map(drop)
        .map_err(|error| {
            delegated_deployment_failure(error, "patch", mount_failure("DeploymentPatchFailed", failure_message))
        })
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
            let fallback = mount_failure(
                "DeploymentReadFailed",
                format!("could not read the delegated Deployment: {error}"),
            );
            delegated_deployment_failure(error, "get", fallback)
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
    let active = config_api.get_opt(active_config_name).await?;
    if active.is_none() && !template_annotations.contains_key(CONFIG_REVISION_ANNOTATION) {
        // Never staged, so no pod serves a config yet, and the first cannot start, nor the
        // rollout finish, without one.
        create_bootstrap_config(&config_api, active_config_name, cc, network_name, gw_ref).await?;
    }
    let active_config_current = active.is_some_and(|current| current.data == active_cm.data);
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

    if active_config_name != cc.config_map_name {
        retire_bootstrap_slot(&config_api, cc, &rendered.config_yaml, network_name, gw_ref).await?;
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

/// Determine the lifecycle phase.
///
/// When a [`MembershipSnapshot`] is provided, the live membership state takes
/// precedence:
/// - ≥1 [`Alive`] member → [`Active`].
/// - Members present but all [`Suspect`]/[`Dead`] → [`Degraded`].
/// - Empty snapshot → falls through to the existing TLS-based logic.
///
/// Local membership needs a grid identity and TLS trust, but no SWIM runtime.
/// Peer reachability is reported by the polling conditions independently.
/// In Swim mode, an absent runtime retains the `Pending`/`Initializing` logic.
///
/// [`Alive`]: MemberStatus::Alive
/// [`Suspect`]: MemberStatus::Suspect
/// [`Dead`]: MemberStatus::Dead
/// [`Active`]: GridNetworkPhase::Active
/// [`Degraded`]: GridNetworkPhase::Degraded
fn determine_phase(
    network: &GridNetwork,
    grid_id: &str,
    membership: Option<&MembershipSnapshot>,
    mode: MembershipMode,
) -> GridNetworkPhase {
    if grid_id.is_empty() {
        return GridNetworkPhase::Pending;
    }
    let has_tls = network.spec.tls.ca_secret_ref.is_some();
    if mode == MembershipMode::Local {
        return if has_tls {
            GridNetworkPhase::Active
        } else {
            GridNetworkPhase::Pending
        };
    }
    // Live membership takes precedence when available and non-empty.
    if let Some(snap) = membership
        && let Some(hint) = snap.phase_hint()
    {
        return hint;
    }
    // No live phase hint. `phase_hint` returns `Some` only when at least one
    // Alive/Degraded peer exists, so we reach here when the network has no peers
    // yet: either the SWIM runtime is not up (`membership` is `None`) or it is
    // up but no peers have joined (`Some`, empty snapshot).
    if !has_tls {
        return GridNetworkPhase::Pending;
    }
    // A single-site / combined deployment legitimately has zero SWIM peers;
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
pub(crate) fn provider_state_from_kube(
    provider: &InferenceProvider,
    network_id: &str,
    site_id: &str,
    metrics: Option<scoring::BackendMetrics>,
    served_model_store: &served_models::ServedModelStore,
) -> Option<crdt::ProviderState> {
    let provider_id = provider.metadata.name.as_deref()?;
    let routing_cluster = routing_overlay::routing_identity(provider)?.to_owned();
    let models = match served_model_store.select_for(provider) {
        // FIXME: we are currently only wiring static config, even if we support discovery.
        Some(inventory) if inventory.is_static() => inventory.models().to_vec(),
        _ => Vec::new(),
    };
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
/// `grid_id` (the caller's declared grid identity) is attached to
/// the broadcast so a signature over this `GridNetwork`'s state cannot be
/// replayed as valid for a different `GridNetwork` sharing the same
/// cluster's `SwimHandle`; see [`swim::StateBroadcast::grid_id`].
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
    // They travel in the versioned provider-state extension and are
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
/// Exits early once a full round produces no new retained tools, since further
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

/// Maximum DER-encoded ECDSA P-256 signature size.
const MAX_P256_DER_SIGNATURE_BYTES: usize = 72;

/// Retain whole provider UID records only while the inference snapshot remains transport-safe.
///
/// UID values are identity tokens and are never truncated. Providers whose UID metadata does not
/// fit still remain in the provider snapshot; their discovered inventories will be ignored until
/// a later snapshot can carry the matching UID.
fn fit_provider_uids_to_swim_budget(broadcast: &mut swim::StateBroadcast, byte_budget: usize) -> usize {
    let mut offered = std::mem::take(&mut broadcast.provider_uids);
    offered.sort_by(|left, right| left.identity.cmp(&right.identity));
    let mut dropped = 0_usize;
    for provider_uid in offered {
        broadcast.provider_uids.push(provider_uid);
        if !broadcast.encode().is_ok_and(|encoded| encoded.len() <= byte_budget)
            && broadcast.provider_uids.pop().is_some()
        {
            dropped = dropped.saturating_add(1);
        }
    }
    dropped
}

/// Deterministically trim tool catalogs until the encoded state fits SWIM.
///
/// Inference provider records and capabilities are never removed. Tool
/// providers are added back round-robin only when their provider record and at
/// least one tool fit. If the inference-only baseline already exceeds the
/// transport budget, all tool providers remain omitted and the runtime reports
/// the oversized inference snapshot.
fn fit_provider_tools_to_swim_budget(broadcast: &mut swim::StateBroadcast) {
    let Some(byte_budget) = provider_tool_byte_budget(broadcast) else {
        return;
    };

    let catalogs = detach_tool_catalogs(broadcast);
    let providers_offered = catalogs.len();
    let offered = catalogs.iter().map(|catalog| catalog.tools.len()).sum::<usize>();
    let provider_uids_dropped = fit_provider_uids_to_swim_budget(broadcast, byte_budget);
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
    tracing::warn!(
        providers_offered,
        providers_retained,
        providers_dropped = providers_offered.saturating_sub(providers_retained),
        offered,
        retained,
        dropped = offered.saturating_sub(retained),
        provider_uids_dropped,
        byte_budget,
        "trimmed provider UID metadata and MCP tool catalogs to fit the SWIM broadcast budget"
    );
}

/// Returns the payload budget when this broadcast needs its provider tools trimmed.
fn provider_tool_byte_budget(broadcast: &swim::StateBroadcast) -> Option<usize> {
    // The runtime's advertised address may be IPv6, so preflight against the
    // largest valid source identity for this site name. This cannot exceed the
    // runtime's real transport budget.
    let local_id = worst_case_swim_identity(&broadcast.origin_site);
    let Ok(maximum_bytes) = swim::node::state_broadcast_byte_budget(&local_id) else {
        tracing::warn!("failed to calculate SWIM state-broadcast byte budget");
        return None;
    };
    // The runtime replaces the CRD-derived revision with a leased transport
    // revision. Fit against the largest bincode representation so that rewrite
    // cannot push an edge-sized snapshot over the node's byte limit.
    // P-256 ECDSA signatures use DER encoding, whose largest valid signature is
    // 72 bytes. Include the maximum signature and timestamp representation when
    // measuring so the runtime's later signing step cannot exceed foca's limit.
    let mut maximum_signed = broadcast.clone();
    maximum_signed.revision = u64::MAX;
    maximum_signed.signature = Some(vec![0; MAX_P256_DER_SIGNATURE_BYTES]);
    maximum_signed.signed_at_ms = Some(u64::MAX);
    let Ok(current_len) = broadcast.encode().map(|encoded| encoded.len()) else {
        tracing::warn!("failed to encode SWIM state broadcast for size preflight");
        return None;
    };
    let Ok(signed_maximum_len) = maximum_signed.encode().map(|encoded| encoded.len()) else {
        tracing::warn!("failed to encode maximum signed SWIM state broadcast for size preflight");
        return None;
    };
    if signed_maximum_len <= maximum_bytes {
        return None;
    }
    // Covers both leased-revision growth and the signature fields.
    let signing_overhead = signed_maximum_len.saturating_sub(current_len);
    Some(maximum_bytes.saturating_sub(signing_overhead))
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
    reason = "tool providers and served-model state are distinct inputs alongside existing inference parameters"
)]
fn publish_real_provider_state(
    swim: &SwimHandle,
    network_name: &str,
    grid_id: &str,
    providers: &[InferenceProvider],
    tool_providers: &[AgentToolProvider],
    raw_metrics: &HashMap<String, scoring::BackendMetrics>,
    served_model_store: &served_models::ServedModelStore,
) {
    let site_name = swim.site_name();
    let mut snap = crdt::GridStateSnapshot::new(site_name.to_owned());
    let mut max_revision: u64 = 0;
    let mut provider_uids = Vec::new();

    for provider in providers {
        if provider.spec.grid_network_ref != network_name {
            continue;
        }
        let routing_id = routing_overlay::routing_identity(provider).unwrap_or("");
        let metrics = raw_metrics.get(routing_id).copied();
        let provider_state = provider_state_from_kube(provider, network_name, site_name, metrics, served_model_store);
        if let Some(state) = provider_state {
            if provider.spec.model_discovery.is_some()
                && let Some(uid) = provider.metadata.uid.as_deref()
            {
                provider_uids.push(provider_uid_from_state(uid.to_owned(), &state));
            }
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

    let mut bc = provider_state_broadcast(site_name, grid_id, max_revision, snap, swim.gateway_address())
        .with_provider_uids(provider_uids);
    fit_provider_tools_to_swim_budget(&mut bc);
    if let Err(e) = swim.publish_state_broadcast(bc) {
        tracing::debug!(error = %e, "CRDT broadcast channel unavailable (runtime not yet receiving)");
    }
}

/// This site's providers in `network_name`, with the models discovered on them, as served
/// to peers on the providers path.
fn local_wire_providers(
    network_name: &str,
    providers: &[InferenceProvider],
    tool_providers: &[AgentToolProvider],
    served_model_store: &served_models::ServedModelStore,
) -> Vec<site_providers::WireProvider> {
    let inference = providers
        .iter()
        .filter(|provider| provider.spec.grid_network_ref == network_name)
        .filter_map(|provider| provider_state_from_kube(provider, network_name, "", None, served_model_store));
    let tools = tool_providers
        .iter()
        .filter(|provider| provider.spec.grid_network_ref == network_name)
        .filter_map(|provider| tool_provider_state_from_kube(provider, network_name, ""))
        .filter(|state| !state.tools.is_empty());
    inference.chain(tools).map(site_providers::WireProvider::from).collect()
}

/// Associate a Kubernetes provider UID with the identity published in CRDT state.
fn provider_uid_from_state(uid: String, state: &crdt::ProviderState) -> swim::ProviderUid {
    let identity = swim::ProviderIdentity {
        network: state.network_id.clone(),
        site: state.site_id.clone(),
        provider: state.provider_id.clone(),
    };
    swim::ProviderUid { identity, uid }
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
/// [`crdt::ProviderPhase::Unavailable`] providers are retained here;
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

/// Apply SWIM liveness to remote providers under gossip only.
///
/// Under poll the gateway reads liveness from signal staleness, so a membership change
/// must not rewrite the serving config.
fn membership_staleness(
    network: &GridNetwork,
    providers: Vec<crdt::ProviderState>,
    membership: Option<&MembershipSnapshot>,
) -> Vec<crdt::ProviderState> {
    if GridModes::of(network, MembershipMode::Swim).signal == SignalMode::Poll {
        return providers;
    }
    // Dead or Suspect members' providers go Degraded, so their candidates render fresh=false.
    let providers = apply_swim_staleness_override(&providers, membership);
    let stale_policy = routing_overlay::stale_policy_from_spec(network.spec.stale_candidate_ttl_seconds);
    routing_overlay::apply_stale_gc_filter(&providers, membership, &stale_policy)
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
/// [`Alive`] status.  `remote_provider_count` reflects providers received via
/// CRDT state broadcasts.  Both are `0` when SWIM is disabled.
/// `consumer_config_statuses` holds per-gateway render/apply outcomes for
/// gateways with `praxisConfig.generate: true`; empty when no gateways opted in.
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
    remote_provider_count: u32,
    consumer_config_statuses: Vec<ConsumerConfigStatus>,
    mount_reconciliation_statuses: Vec<MountReconciliationStatus>,
    overlay_statuses: Vec<OverlayRevisionStatus>,
    budget_statuses: Vec<TenantBudgetStatus>,
) -> Result<(), OperatorError> {
    let name = grid_network_name(network)?;

    let api: Api<GridNetwork> = Api::all(client.clone());
    let status = GridNetworkStatus {
        // None: each condition type belongs to its own field manager.
        conditions: Vec::new(),
        gateway_age_reports: Vec::new(),
        gateway_age_report_overflow: false,
        connected_sites: membership.map_or(0, MembershipSnapshot::connected_count),
        remote_provider_count,
        grid_id: grid_id.to_owned(),
        observed_generation: network.metadata.generation.unwrap_or(0),
        phase: phase.clone(),
        praxis_config_status: keep_consumer_config_status_at(network, &consumer_config_statuses),
        mount_reconciliation_status: keep_mount_reconciliation_status_at(network, &mount_reconciliation_statuses),
        routing_map_status: keep_rendered_at(network.status.as_ref(), overlay_statuses),
        budget_status: budget_statuses,
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

/// Return whether reconciliation-owned status differs from the desired status.
/// Ignore conditions and gateway evidence managed by the signal publishers.
fn grid_network_status_needs_update(current: Option<&GridNetworkStatus>, desired: &GridNetworkStatus) -> bool {
    current.is_none_or(|current| {
        rollup::differs(&current.conditions, &desired.conditions)
            || *desired
                != GridNetworkStatus {
                    conditions: desired.conditions.clone(),
                    gateway_age_reports: desired.gateway_age_reports.clone(),
                    gateway_age_report_overflow: desired.gateway_age_report_overflow,
                    ..current.clone()
                }
    })
}

// ---------------------------------------------------------------------------
// Consumer config status builders
// ---------------------------------------------------------------------------

/// One gateway's endpoint topology, status note, and refusals.
fn gateway_cluster_endpoints<'gw>(
    gw_ref: &'gw GatewayRef,
    candidates: &[routing_overlay::RoutingCandidate],
    declarations: &derived_topology::Declarations<'_>,
) -> (
    Cow<'gw, [ClusterEndpointConfig]>,
    String,
    Vec<derived_topology::Refused>,
) {
    // A disabled block renders nothing, so its gateway's overlay is not ours to withdraw from.
    let Some(cc) = gw_ref.praxis_config.as_ref().filter(|cc| cc.generate) else {
        return (Cow::Borrowed(&[]), String::new(), Vec::new());
    };
    // Resolve even with nothing allowlisted, so a revoked derivation is withdrawn, not kept stale.
    let derive = cc.derive_topology.as_ref();
    let resolution = derived_topology::resolve(
        candidates,
        &cc.cluster_endpoints,
        &derived_topology::Declarations {
            from_providers: derive.map_or(&[][..], |derive| derive.from_providers.as_slice()),
            transport: derive.and_then(|derive| derive.transport.as_ref()),
            ..*declarations
        },
    );
    let summary = derived_topology::derived_summary(&resolution);
    let derived_topology::Resolution { resolved, refused } = resolution;
    let mut derived = resolved
        .into_values()
        .filter(|entry| entry.origin != derived_topology::Origin::Explicit)
        .map(|entry| entry.endpoint)
        .peekable();
    // Typed entries pass through whole, including ones no candidate names yet.
    let endpoints = if derived.peek().is_none() {
        Cow::Borrowed(cc.cluster_endpoints.as_slice())
    } else {
        Cow::Owned(cc.cluster_endpoints.iter().cloned().chain(derived).collect())
    };
    (endpoints, summary, refused)
}

/// Move refused candidates to `excluded` so one refusal does not fail the whole render.
fn withdraw_refused(overlay: &mut routing_overlay::RoutingOverlay, refused: &[derived_topology::Refused]) {
    if refused.is_empty() {
        return;
    }
    let (withdrawn, kept): (Vec<_>, Vec<_>) = overlay.candidates.drain(..).partition(|candidate| {
        candidate.kind == routing_overlay::CANDIDATE_KIND
            && refused
                .iter()
                .any(|refusal| refusal.cluster == candidate.cluster && refusal.site == candidate.site)
    });
    overlay.candidates = kept;
    overlay.excluded.extend(withdrawn);
}

/// Append what derivation resolved and withdrew, so a failed render says which clusters and why.
fn with_derived_summary(mut status: ConsumerConfigStatus, derived_summary: &str) -> ConsumerConfigStatus {
    if !derived_summary.is_empty() {
        status.message = format!("{}; {derived_summary}", status.message);
    }
    status
}

/// Build a `Rendered` [`ConsumerConfigStatus`] for a successfully applied consumer config.
pub(crate) fn consumer_config_status_rendered(
    gw_ref: &GatewayRef,
    cc: &ConsumerConfig,
    observed_generation: i64,
    derived_summary: &str,
) -> ConsumerConfigStatus {
    ConsumerConfigStatus {
        gateway_name: gw_ref.name.clone(),
        namespace: gw_ref.namespace.clone(),
        config_map_name: cc.config_map_name.clone(),
        phase: ConsumerConfigPhase::Rendered,
        reason: String::new(),
        message: if derived_summary.is_empty() {
            format!(
                "consumer config rendered and applied to {}/{}",
                gw_ref.namespace, cc.config_map_name
            )
        } else {
            format!(
                "consumer config rendered and applied to {}/{}; {derived_summary}",
                gw_ref.namespace, cc.config_map_name
            )
        },
        observed_generation,
    }
}

/// Build a `Disabled` [`ConsumerConfigStatus`] for a gateway whose
/// `praxisConfig.generate` is `false`.
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
        reason: "PraxisConfigDisabled".to_owned(),
        message: "praxisConfig.generate is false; no ConfigMap generated".to_owned(),
        observed_generation,
    }
}

/// Build an `Error` [`ConsumerConfigStatus`] from a render or apply failure.
///
/// # Security
///
/// `message` is derived from the `OperatorError` `Display` impl only.  That
/// impl never includes credential token bytes; error messages describe
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
        OperatorError::ConsumerConfigRender(ConsumerConfigError::SiteRouteUnsupported(_)) => "SiteRouteUnsupported",
        OperatorError::ConsumerConfigRender(_) => "PraxisConfigRenderFailed",
        OperatorError::MountReconciliation(failure) => failure.reason,
        OperatorError::Kube(_) => "PraxisConfigApplyFailed",
        OperatorError::Certificate(_)
        | OperatorError::Json(_)
        | OperatorError::NotFound(_)
        | OperatorError::OverlayRender(_)
        | OperatorError::SwimKeyConfig(_)
        | OperatorError::InvalidResource(_) => "PraxisConfigError",
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
/// function's input by the caller; they are always eligible and use a separate
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
    signal: SignalMode,
) -> Vec<&'ctx crdt::ProviderState> {
    remote_providers
        .iter()
        .filter(|p| is_crdt_provider_routing_eligible(network_name, sites, p, signal))
        .collect()
}

/// Return `true` when the `GridSite` corresponding to `provider` is routing-eligible.
///
/// Eligibility requires a routable `GridSite` keyed by the provider's site, as
/// [`peer_site_key`] keys it: a declared object by name, a discovered stub by its
/// annotation. All other outcomes (missing `GridSite`, wrong network, wrong phase)
/// are ineligible.
///
/// This is a pure function with no I/O, suitable for unit testing.
pub(crate) fn is_crdt_provider_routing_eligible(
    network_name: &str,
    sites: &[GridSite],
    provider: &crdt::ProviderState,
    signal: SignalMode,
) -> bool {
    if provider.network_id != network_name {
        return false;
    }
    sites.iter().any(|s| {
        peer_site_key(s).is_some_and(|(key, _)| key == provider.site_id)
            && s.spec.grid_network_ref == network_name
            && s.status.as_ref().is_some_and(|st| st.phase.routes(signal))
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
    /// Contains only the public certificate, never a private key.
    /// `None` when the remote peer has not yet broadcast its certificate.
    pub site_cert_pem: Option<String>,
}

/// Derive the set of remote [`GridSite`]s the operator should maintain from the SWIM snapshot.
///
/// Returns one [`DiscoveredSite`] per remote Alive SWIM member.  The local site
/// and non-Alive (Suspect, Dead) members are excluded; only confirmed Alive
/// peers should produce a `Discovered` record.
///
/// Name derivation is deterministic: the SWIM `site_id` is sanitised to a valid
/// Kubernetes resource name.
///
/// This is a **pure function** (no Kubernetes API calls) and is
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

/// The auto-discovered `GridSite` objects among `sites` that belong to `network_name`.
fn auto_discovered_stubs(sites: &[GridSite], network_name: &str) -> Vec<GridSite> {
    sites
        .iter()
        // A label is not ownership: only discovery's own objects for this network.
        .filter(|site| {
            site.metadata
                .labels
                .as_ref()
                .is_some_and(|labels| labels.get("grid.praxis.fast/network").is_some_and(|n| n == network_name))
                && site.spec.grid_network_ref == network_name
                && is_stub(site)
        })
        .cloned()
        .collect()
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

/// Whether applying `site` creates its stub or changes the gateway endpoint or certificate the stub carries.
fn stub_changed(existing: Option<&GridSite>, site: &DiscoveredSite) -> bool {
    let Some(existing) = existing else {
        return true;
    };
    let gateway_endpoint = existing
        .spec
        .gateway_endpoint
        .as_ref()
        .map_or("", |endpoint| endpoint.address.as_str());
    let cert = existing
        .status
        .as_ref()
        .and_then(|status| status.public_cert_pem.as_deref());
    gateway_endpoint != site.egress_address || (site.site_cert_pem.is_some() && cert != site.site_cert_pem.as_deref())
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
/// Explicit `consumerGateways[].praxisConfig.clusterEndpoints[].transport.mode`
/// declarations are the source of truth when present.  If any endpoint is
/// declared plaintext, auto-discovered `GridSite` egress is plaintext for this
/// network.  This supports local/dev GLB demos where provider gateways are
/// intentionally plain HTTP.
///
/// When no explicit plaintext endpoint exists, fall back to the top-level grid
/// TLS references: a network with no CA or site certificate refs is treated as
/// plaintext, while a network with either TLS ref keeps mutual TLS.
fn network_uses_plaintext_egress(network: &GridNetwork) -> bool {
    let has_plaintext_endpoint = network.spec.consumer_gateways.iter().any(|gw| {
        gw.praxis_config.as_ref().is_some_and(|cc| {
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
/// unit-testable without a live Kubernetes API; see grid#42, where writing
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
/// Never itself touches the Kubernetes API; see [`CertPemWrite`].
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
                     discarding: private keys must never appear in SWIM broadcasts"
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
            "gatewayEndpoint".to_owned(),
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
    let all = api.list(&ListParams::default()).await?.items;
    let present = auto_discovered_stubs(&all, network_name);
    let vouching = vouched_sites(snapshot, ctx.started.elapsed());
    let mut stubs = collect_stale_stubs(&api, network_name, &vouching, ttl, &present).await?;

    let sites = discovered_sites_from_swim(network_name, local_site, snapshot);
    let mut capped = 0_usize;

    // Discovery runs only under Swim, where the chart names a declared peer as discovery does,
    // so it advances that object rather than a stub beside it.
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

        if existing.is_some() && !site.egress_address.is_empty() {
            let clear_legacy_endpoint = serde_json::json!({"spec": {"egress": null}});
            api.patch(
                &site.name,
                &PatchParams::default(),
                &Patch::Merge(&clear_legacy_endpoint),
            )
            .await?;
        }

        api.patch(
            &site.name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&spec_doc),
        )
        .await?;

        // Fetch current status once and reuse it below for every write in this
        // iteration. Every status patch bumps the GridSite's resourceVersion,
        // which fires a watch event that re-triggers a GridNetwork reconcile
        // (related object updated), re-entering this same loop. Writing
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
        // that would be a no-op given `existing_status`; otherwise every
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

/// Whether remote providers come from the peers' polled catalogs: only under Local
/// membership, which has no gossip. Under Swim gossip carries them, as on main, whatever the
/// signal transport.
const fn reads_polled_catalogs(membership: crate::cli::Membership) -> bool {
    matches!(membership, crate::cli::Membership::Local)
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
            age: Duration::ZERO,
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
        let metrics = serde_json::json!({
            "type": "MetricsAvailable", "status": "True", "reason": "ScrapeSucceeded", "message": "metrics scraped",
            "lastTransitionTime": "2026-10-03T00:00:00Z", "observedGeneration": 1
        });
        let live = serde_json::json!({
            "type": "EndpointLive", "status": "False", "reason": "NoEndpointsReady", "message": "0 ready endpoints",
            "lastTransitionTime": "2026-10-03T00:00:00Z", "observedGeneration": 1
        });
        let reachable = serde_json::json!({
            "type": "MetricsReachable", "status": "True", "reason": "MetricsReachable", "message": "",
            "lastTransitionTime": "2026-10-03T00:00:00Z", "observedGeneration": 1
        });
        let current = provider_with_status(&serde_json::json!({
            "conditions": [ready_condition_json("False", "NoEndpointsReady"), metrics, live, reachable]
        }));
        assert!(
            ready_condition_patch(&current, &down).is_none(),
            "the conditions already say so, so there is nothing to write"
        );
        let up = readiness::Verdict {
            reason: readiness::Reason::Ready,
            message: "1 ready endpoints".to_owned(),
        };
        let (_, patch) = ready_condition_patch(&current, &up).expect("a transition is written");
        assert_eq!(patch["status"]["conditions"][0]["status"], "True");
        assert_eq!(
            patch["status"]["conditions"][1]["type"], "MetricsAvailable",
            "the unchanged condition is sent too, or the apply would remove it"
        );

        let failing = readiness::Verdict {
            reason: readiness::Reason::ScrapeTimedOut,
            message: "no successful metrics scrape in the last 30s; the latest failed: timeout".to_owned(),
        };
        let (_, failed) = ready_condition_patch(&current, &failing).expect("a scrape failure is written");
        let written = &failed["status"]["conditions"][1];
        assert_eq!(written["status"], "False");
        assert_eq!(written["reason"], "ScrapeTimedOut");
        assert_eq!(written["message"], failing.message.as_str());
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

    #[tokio::test]
    #[expect(clippy::expect_used, reason = "test")]
    async fn a_mode_change_asks_for_a_graceful_restart_and_applies_nothing_meanwhile() {
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::new());
        let (owed, mut restart) = tokio::sync::watch::channel(None);
        let ctx = Arc::new(OperatorCtx::new(client, None, SignalMode::Gossip).with_mode_restart(owed));
        let poll = network_with_modes(&serde_json::json!({"signalTransport": {"mode": "poll"}}));
        let action = Box::pin(reconcile(Arc::new(poll), Arc::clone(&ctx)))
            .await
            .expect("reconciled");
        assert_eq!(action, Action::await_change(), "the old modes reconcile nothing");
        assert_eq!(
            restart.borrow_and_update().map(|next| next.signal),
            Some(SignalMode::Poll),
            "the restart names the declared transport"
        );
    }

    #[tokio::test]
    #[expect(clippy::expect_used, reason = "test: two Secrets, then three reads")]
    async fn a_new_grid_ca_alone_changes_the_material_a_listener_was_built_from() {
        crate::init_process_crypto();
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let leaf = certs::generate_site_cert(&ca, "site-d").expect("leaf");
        let api = status_harness::FakeApi::new();
        let put = |name: &str, key: &str, pem: &str| {
            let secret = Secret {
                metadata: kube::api::ObjectMeta {
                    name: Some(name.to_owned()),
                    namespace: Some("grid-system".to_owned()),
                    ..Default::default()
                },
                data: Some(BTreeMap::from([(
                    key.to_owned(),
                    k8s_openapi::ByteString(pem.as_bytes().to_vec()),
                )])),
                ..Default::default()
            };
            api.put(
                &format!("/api/v1/namespaces/grid-system/secrets/{name}"),
                serde_json::to_value(secret).expect("secret"),
            );
        };
        put("site-tls", "tls.crt", &leaf.cert_pem);
        put("grid-ca", "ca.crt", &ca.cert_pem);
        let network = network_with_modes(&serde_json::json!({"tls": {
            "caSecretRef": {"name": "grid-ca", "namespace": "grid-system"},
            "siteSecretRef": {"name": "site-tls", "namespace": "grid-system"},
        }}));
        let client = api.client("grid-system");
        let before = own_leaf_identity(&network, &client).await.expect("read");
        assert_eq!(
            own_leaf_identity(&network, &client).await.expect("read"),
            before,
            "unchanged"
        );
        let next = certs::generate_ca("grid-ca").expect("ca");
        put("grid-ca", "ca.crt", &format!("{}{}", ca.cert_pem, next.cert_pem));
        let after = own_leaf_identity(&network, &client).await.expect("read");
        assert_eq!(
            after.as_ref().map(|own| &own.fingerprint),
            before.as_ref().map(|own| &own.fingerprint),
            "the leaf is the same"
        );
        assert_ne!(after, before, "a rotated CA bundle is a change");
    }

    #[test]
    fn modes_restart_only_when_a_grid_network_declares_others() {
        let running = GridModes::WITHOUT_NETWORK;
        assert_eq!(
            running.restart_for(None, MembershipMode::Swim),
            None,
            "no GridNetwork, no restart"
        );

        let same = network_with_modes(&serde_json::json!({"peerTrust": {"mode": "spiffe"}}));
        assert_eq!(
            running.restart_for(Some(&same), MembershipMode::Swim),
            None,
            "same modes, no restart"
        );

        let poll = network_with_modes(&serde_json::json!({
            "peerTrust": {"mode": "spiffe"},
            "signalTransport": {"mode": "poll"},
        }));
        assert_eq!(
            running.restart_for(Some(&poll), MembershipMode::Swim),
            Some(GridModes {
                membership: MembershipMode::Swim,
                signal: SignalMode::Poll,
                trust: PeerTrustMode::Spiffe,
            })
        );
        assert_eq!(
            GridModes::of(&poll, MembershipMode::Swim).restart_for(Some(&poll), MembershipMode::Swim),
            None,
            "the restarted process runs what it declares, so it never loops"
        );
    }

    #[test]
    fn a_trust_change_restarts_only_under_poll() {
        let pin = network_with_modes(&serde_json::json!({"peerTrust": {"mode": "pin"}}));
        assert_eq!(
            GridModes::WITHOUT_NETWORK.restart_for(Some(&pin), MembershipMode::Swim),
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
            GridModes::of(&poll, MembershipMode::Swim).restart_for(Some(&poll_pin), MembershipMode::Swim),
            Some(GridModes::of(&poll_pin, MembershipMode::Swim)),
            "under poll a trust change restarts"
        );
    }

    #[test]
    fn membership_override_and_removal_resolve_against_the_install() {
        let legacy = network_with_modes(&serde_json::json!({"signalTransport": {"mode": "poll"}}));
        for mode in [MembershipMode::Swim, MembershipMode::Local] {
            assert_eq!(GridModes::of(&legacy, mode).membership, mode);
            assert_eq!(GridModes::of(&legacy, mode).restart_for(Some(&legacy), mode), None);
        }
        let declared = network_with_modes(&serde_json::json!({
            "membership": {"mode": "Local"}, "signalTransport": {"mode": "poll"}
        }));
        let running = GridModes::of(&declared, MembershipMode::Swim);
        assert_eq!(running.membership, MembershipMode::Local);
        assert_eq!(running.restart_for(Some(&declared), MembershipMode::Swim), None);
        assert_eq!(
            running
                .restart_for(Some(&legacy), MembershipMode::Swim)
                .map(|next| next.membership),
            Some(MembershipMode::Swim),
            "removing the override restores the install setting"
        );
        assert_eq!(running.restart_for(Some(&legacy), MembershipMode::Local), None);
    }

    #[tokio::test]
    async fn membership_changes_restart_before_reconciling_other_resources() -> Result<(), OperatorError> {
        for (running, wanted) in [
            (MembershipMode::Swim, MembershipMode::Local),
            (MembershipMode::Local, MembershipMode::Swim),
        ] {
            let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::new());
            let (owed, mut restart) = tokio::sync::watch::channel(None);
            let ctx = Arc::new(
                OperatorCtx::new(client, None, SignalMode::Poll)
                    .with_membership(running)
                    .with_mode_restart(owed),
            );
            let network = network_with_modes(&serde_json::json!({
                "membership": {"mode": wanted.as_str()},
                "signalTransport": {"mode": "poll"}, "peerTrust": {"mode": "pin"}
            }));
            assert_eq!(
                Box::pin(reconcile(Arc::new(network), ctx)).await?,
                Action::await_change()
            );
            assert_eq!(restart.borrow_and_update().map(|next| next.membership), Some(wanted));
        }
        Ok(())
    }

    #[tokio::test]
    async fn local_membership_rejects_gossip_before_requesting_restart() {
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::new());
        let (owed, restart) = tokio::sync::watch::channel(None);
        let ctx = Arc::new(OperatorCtx::new(client, None, SignalMode::Gossip).with_mode_restart(owed));
        let network = network_with_modes(&serde_json::json!({"membership": {"mode": "Local"}}));
        assert!(matches!(Box::pin(reconcile(Arc::new(network), ctx)).await,
                         Err(OperatorError::InvalidResource(message)) if message.contains("requires signalTransport.mode poll")));
        assert!(
            restart.borrow().is_none(),
            "invalid modes must not cause a restart loop"
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
            startup.restart_for(Some(&poll_pin), MembershipMode::Swim),
            None,
            "the network it declared needs no restart"
        );
        assert!(
            GridModes::WITHOUT_NETWORK
                .restart_for(Some(&poll_pin), MembershipMode::Swim)
                .is_some(),
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

    /// Build a scraped provider whose configured staleness age is one second.
    fn provider_with_stale_metrics(name: &str, network_ref: &str) -> InferenceProvider {
        let mut provider = make_inference_provider(name, network_ref);
        provider.spec.metrics_config = Some(crate::crd::inference_provider::MetricsConfig {
            stale_metrics_seconds: Some(1),
            ..Default::default()
        });
        provider
    }

    /// Record a successful readiness probe at `age` before `now`.
    fn record_success_at_age(ctx: &OperatorCtx, key: &str, age: Duration, now: Instant) {
        let at = now.checked_sub(age).unwrap_or_else(|| std::process::abort());
        ctx.readiness.record_success(key, Some(1.0), None, Vec::new(), at);
    }

    /// Assert one provider's readiness floor.
    #[track_caller]
    fn assert_readiness_floor(ctx: &OperatorCtx, provider: &InferenceProvider, expected: Duration, message: &str) {
        assert_eq!(readiness_stale_after(ctx, provider), expected, "{message}");
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

    /// API path of [`stored_grid_id_network`]'s network.
    const GRID_ID_PATH: &str = "/apis/grid.praxis.fast/v1alpha1/gridnetworks/net";

    /// A fake API holding a network with these spec and status grid IDs, and the network as read back.
    fn stored_grid_id_network(
        spec: &str,
        status: &str,
    ) -> Result<(status_harness::FakeApi, GridNetwork), OperatorError> {
        let api = status_harness::FakeApi::new();
        let mut network = base_network();
        spec.clone_into(&mut network.spec.grid_id);
        network.status = (!status.is_empty()).then(|| GridNetworkStatus {
            grid_id: status.to_owned(),
            ..GridNetworkStatus::default()
        });
        api.put(GRID_ID_PATH, serde_json::to_value(&network)?);
        let stored = serde_json::from_value(api.get(GRID_ID_PATH).unwrap_or_default())?;
        Ok((api, stored))
    }

    /// Writes to the network's main resource, which is where `spec.gridId` lives.
    fn grid_id_spec_writes(api: &status_harness::FakeApi) -> Vec<status_harness::Write> {
        api.writes()
            .into_iter()
            .filter(|write| write.path == GRID_ID_PATH && !write.status)
            .collect()
    }

    fn base_network() -> GridNetwork {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "net" },
            "spec": { "seeds": [], "gridId": "test-id" }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn empty_overlay_warns_only_on_entering_the_state() {
        let gw: GatewayRef = serde_json::from_value(serde_json::json!({"name": "gw", "namespace": "ns"}))
            .unwrap_or_else(|_| std::process::abort());
        let with_reason = |reason: &str| {
            let mut network = base_network();
            network.status = Some(
                serde_json::from_value(serde_json::json!({"routingMapStatus": [{
                    "gatewayName": "gw", "namespace": "ns", "configMapName": "c", "schemaVersion": "v",
                    "renderedRevision": "r", "distributedRevision": "r", "contentDigest": "r", "reason": reason,
                }]}))
                .unwrap_or_else(|_| std::process::abort()),
            );
            network
        };
        let cases = [
            ("no status yet", base_network(), false),
            ("was distributed", with_reason(""), false),
            ("was already empty", with_reason("EmptyCandidates"), true),
        ];
        for (label, network, want) in cases {
            assert_eq!(already_empty(&network, &gw), want, "{label}");
        }
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

    #[tokio::test]
    async fn a_quick_scrape_pass_keeps_the_staleness_floor() {
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::new());
        let ctx = OperatorCtx::new(client, None, SignalMode::default()).with_scrape_interval(Duration::from_secs(5));
        let provider = provider_with_stale_metrics("provider-a", "net-a");

        assert_readiness_floor(
            &ctx,
            &provider,
            Duration::from_secs(10),
            "before a pass is measured, two intervals remain the floor",
        );
        ctx.record_scrape_pass("net-a", Duration::from_secs(3));
        assert_readiness_floor(
            &ctx,
            &provider,
            Duration::from_secs(10),
            "a pass shorter than the interval keeps the old floor",
        );
        ctx.record_scrape_pass("net-a", Duration::from_secs(5));
        assert_readiness_floor(
            &ctx,
            &provider,
            Duration::from_secs(10),
            "a pass equal to the interval keeps the old floor",
        );
    }

    #[tokio::test]
    async fn a_slow_scrape_pass_widens_the_staleness_floor() {
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::new());
        let ctx = OperatorCtx::new(client, None, SignalMode::default()).with_scrape_interval(Duration::from_secs(5));
        let provider = provider_with_stale_metrics("provider-a", "net-a");
        let sibling_network = provider_with_stale_metrics("provider-b", "net-b");

        ctx.record_scrape_pass("net-a", Duration::from_secs(17));
        assert_readiness_floor(
            &ctx,
            &provider,
            Duration::from_secs(22),
            "the floor covers one interval plus the measured pass",
        );
        assert_readiness_floor(
            &ctx,
            &sibling_network,
            Duration::from_secs(10),
            "another network keeps its own measured pass",
        );
    }

    #[tokio::test]
    async fn a_timed_out_provider_leaves_a_sibling_inside_the_widened_floor_ready() {
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::new());
        let ctx = OperatorCtx::new(client, None, SignalMode::default()).with_scrape_interval(Duration::from_secs(5));
        let timed_out = provider_with_stale_metrics("slow", "net");
        let sibling = provider_with_stale_metrics("fast", "net");
        let now = Instant::now();
        record_success_at_age(&ctx, &readiness::key("net", "slow"), Duration::from_secs(23), now);
        record_success_at_age(&ctx, &readiness::key("net", "fast"), Duration::from_secs(12), now);
        ctx.readiness
            .record_failure(&readiness::key("net", "slow"), readiness::ScrapeClass::Timeout, now);
        ctx.record_scrape_pass("net", Duration::from_secs(17));

        let slow_verdict = provider_readiness(&ctx, &timed_out, now).unwrap_or_else(|| std::process::abort());
        let fast_verdict = provider_readiness(&ctx, &sibling, now).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            slow_verdict.reason,
            readiness::Reason::ScrapeTimedOut,
            "{slow_verdict:?}"
        );
        assert_eq!(fast_verdict.reason, readiness::Reason::Ready, "{fast_verdict:?}");
        assert!(slow_verdict.reason.excludes(), "the timed-out provider is excluded");
        assert!(!fast_verdict.reason.excludes(), "the healthy sibling stays ready");
    }

    #[tokio::test]
    async fn refresh_signals_records_the_measured_scrape_pass() {
        let response = "HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 0\r\n\r\n"
            .as_bytes()
            .to_vec();
        let endpoint =
            crate::resources::test_doubles::start_delayed_http_server(response, Duration::from_millis(25)).await;
        let mut provider = make_inference_provider("provider-a", "net");
        provider.spec.endpoint = endpoint;
        provider.spec.metrics_config = Some(crate::crd::inference_provider::MetricsConfig {
            timeout: "1s".to_owned(),
            ..Default::default()
        });
        let client = crate::resources::test_doubles::mock_kube_client_with_providers(vec![provider]);
        let ctx =
            OperatorCtx::new(client.clone(), None, SignalMode::default()).with_scrape_interval(Duration::from_secs(5));

        refresh_signals(&ctx, &client, "net")
            .await
            .unwrap_or_else(|_| std::process::abort());

        assert!(
            ctx.last_scrape_pass("net") >= Duration::from_millis(25),
            "the scrape pass records its measured duration"
        );
    }

    #[tokio::test]
    async fn fenced_publication_keeps_scraping_readiness_and_refreshing_peers() -> Result<(), Box<dyn std::error::Error>>
    {
        let response = "HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 0\r\n\r\n"
            .as_bytes()
            .to_vec();
        let mut provider = make_inference_provider("provider-a", "net");
        provider.spec.endpoint =
            crate::resources::test_doubles::start_delayed_http_server(response, Duration::from_millis(25)).await;
        provider.spec.metrics_config = Some(crate::crd::inference_provider::MetricsConfig {
            timeout: "1s".to_owned(),
            ..Default::default()
        });
        let client = crate::resources::test_doubles::mock_kube_client_with_providers(vec![provider.clone()]);
        let ctx = OperatorCtx::new(client.clone(), None, SignalMode::Poll);
        ctx.signal_fenced.store(true, std::sync::atomic::Ordering::Relaxed);
        ctx.peer_identities.set(BTreeMap::from([(
            "removed".to_owned(),
            signals::PeerRecord {
                labels: BTreeMap::new(),
                pins: vec!["previous-pin".to_owned()],
                signals: None,
            },
        )]));
        assert!(
            !ctx.peer_identities.refuses("removed"),
            "the old peer is present before refresh"
        );
        refresh_signals(&ctx, &client, "net").await?;
        assert!(
            ctx.last_scrape_pass("net") >= Duration::from_millis(25),
            "fencing publication cannot skip collection"
        );
        let verdict = provider_readiness(&ctx, &provider, Instant::now()).ok_or("readiness was not refreshed")?;
        assert!(
            ready_condition_patch(&provider, &verdict).is_some(),
            "new readiness remains eligible for a Ready condition write"
        );
        assert!(
            ctx.signals.current().is_empty(),
            "the fenced process still cannot publish"
        );
        assert!(
            ctx.peer_identities.refuses("removed"),
            "fencing publication cannot freeze the peer inventory"
        );
        Ok(())
    }

    #[tokio::test]
    async fn delayed_publication_retains_the_collection_pass_age() {
        let client = crate::resources::test_doubles::mock_kube_client_with_providers(Vec::new());
        let ctx = OperatorCtx::new(client, None, SignalMode::default());
        let observations = signals::parse("queue 2\nready 1\n");
        let started = Instant::now()
            .checked_sub(Duration::from_secs(40))
            .unwrap_or_else(|| std::process::abort());
        publish_signals(&ctx, HashMap::from([("model".to_owned(), observations)]), started);
        let published = ctx.signals.current();
        assert_eq!(published.len(), 2);
        assert!(
            published.iter().all(|row| row.age >= Duration::from_secs(40)),
            "collection and API delays cannot rejuvenate observations"
        );
        assert_eq!(
            published.first().map(|row| row.age),
            published.last().map(|row| row.age),
            "one batch keeps one age"
        );
    }

    #[test]
    fn a_keyless_network_releases_plaintext_only_when_no_network_declares_a_key() {
        let keyless = base_network();
        let mut keyed = base_network();
        keyed.spec.tls.swim_key_secret_ref = Some(crate::crd::grid_network::SecretRef {
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
    fn determine_phase_local_does_not_wait_for_swim() {
        for (grid_id, trust, mode, expected) in [
            ("some-id", true, MembershipMode::Local, GridNetworkPhase::Active),
            ("", true, MembershipMode::Local, GridNetworkPhase::Pending),
            ("some-id", false, MembershipMode::Local, GridNetworkPhase::Pending),
            ("some-id", true, MembershipMode::Swim, GridNetworkPhase::Initializing),
        ] {
            let mut network = base_network();
            network.spec.seeds = vec!["legacy-peer:7946".to_owned()];
            network.spec.tls.ca_secret_ref = trust.then(|| crate::crd::grid_network::SecretRef {
                name: "ca".to_owned(),
                namespace: "default".to_owned(),
                key: None,
            });
            assert_eq!(
                determine_phase(&network, grid_id, None, mode),
                expected,
                "mode={mode:?}, grid_id={grid_id:?}, trust={trust} without a SWIM runtime"
            );
        }
    }

    #[test]
    fn determine_phase_none_membership_preserves_tls_logic() {
        let network = base_network();
        // Without TLS config, phase is Pending regardless of grid_id.
        let phase = determine_phase(&network, "some-id", None, MembershipMode::Swim);
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
        let phase = determine_phase(&network, "some-id", Some(&empty), MembershipMode::Swim);
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
        let phase = determine_phase(&network, "some-id", Some(&snap), MembershipMode::Swim);
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
        let phase = determine_phase(&network, "some-id", Some(&snap), MembershipMode::Swim);
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
        let phase = determine_phase(&network, "some-id", Some(&snap), MembershipMode::Swim);
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
        let phase = determine_phase(&network, "some-id", Some(&empty), MembershipMode::Swim);
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
        let phase = determine_phase(&network, "some-id", Some(&empty), MembershipMode::Swim);
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
            "remoteProviderCount for one GridNetwork must not include records from another GridNetwork"
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
                "clusterName": routing_ref
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
        let state = provider_state_from_kube(&p, "net", "site-a", None, &served_models::ServedModelStore::new());
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
    fn provider_state_from_kube_does_not_gossip_dynamic_spec_models() {
        let p: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": "dynamic-provider", "uid": "provider-uid", "generation": 1 },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8080",
                "models": [{ "name": "configured-model" }],
                "modelDiscovery": { "openAiModels": {} }
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let state = provider_state_from_kube(&p, "net", "site-a", None, &served_models::ServedModelStore::new())
            .unwrap_or_else(|| std::process::abort());

        assert!(
            state.models.is_empty(),
            "dynamic providers without a fresh inventory must not gossip spec.models"
        );
    }

    #[test]
    fn provider_state_from_kube_uses_metadata_name_as_routing_cluster_by_default() {
        let p = make_provider("prov-a", "net", "api_provider", 0);
        let state = provider_state_from_kube(&p, "net", "site-a", None, &served_models::ServedModelStore::new())
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            state.routing_cluster, "prov-a",
            "routing_cluster defaults to metadata.name"
        );
    }

    #[test]
    fn provider_state_from_kube_uses_routing_cluster_ref_when_set() {
        let p = make_provider_with_routing_ref("prov-x", "net", "site-override");
        let state = provider_state_from_kube(&p, "net", "site-a", None, &served_models::ServedModelStore::new())
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            state.routing_cluster, "site-override",
            "clusterName must override metadata.name"
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
            provider_state_from_kube(&p, "net", "site-a", None, &served_models::ServedModelStore::new()).is_none(),
            "provider with no metadata.name must yield None"
        );
    }

    #[test]
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
        let state = provider_state_from_kube(&p, "net", "s", None, &served_models::ServedModelStore::new())
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            state.phase,
            crdt::ProviderPhase::Degraded,
            "Degraded must propagate to CRDT phase"
        );
    }

    #[test]
    fn provider_state_from_kube_unavailable_is_included_not_skipped() {
        let p = make_provider_with_status("prov-a", "net", "Unavailable");
        let state = provider_state_from_kube(&p, "net", "s", None, &served_models::ServedModelStore::new());
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
        let state = provider_state_from_kube(&p, "net", "s", None, &served_models::ServedModelStore::new())
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(state.revision, 42, "revision must fall back to Kubernetes generation");
    }

    #[test]
    fn revision_prefers_resource_version_over_generation() {
        let mut p = make_provider("prov-rv", "net", "local", 42);
        p.metadata.resource_version = Some("99".to_owned());
        let state = provider_state_from_kube(&p, "net", "s", None, &served_models::ServedModelStore::new())
            .unwrap_or_else(|| std::process::abort());
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
        let state = provider_state_from_kube(&p, "net", "s", None, &served_models::ServedModelStore::new())
            .unwrap_or_else(|| std::process::abort());
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
    fn poll_ignores_membership_and_gossip_degrades_on_it() {
        let provider = make_crdt_provider("site-west", crdt::ProviderPhase::Available);
        for status in [MemberStatus::Dead, MemberStatus::Suspect] {
            let membership = make_swim_membership("site-west", status.clone());
            let poll = network_with_modes(&serde_json::json!({"signalTransport": {"mode": "poll"}}));
            let kept = membership_staleness(&poll, vec![provider.clone()], Some(&membership));
            assert_eq!(
                kept,
                vec![provider.clone()],
                "poll must not rewrite a provider on {status:?}"
            );

            let gossip = network_with_modes(&serde_json::json!({}));
            let degraded = membership_staleness(&gossip, vec![provider.clone()], Some(&membership));
            assert_eq!(
                degraded.first().map(|p| &p.phase),
                Some(&crdt::ProviderPhase::Degraded),
                "gossip still degrades on {status:?}"
            );
        }
    }

    #[test]
    fn poll_ignores_the_stale_candidate_ttl() {
        let provider = make_crdt_provider("site-west", crdt::ProviderPhase::Degraded);
        let mut membership = make_swim_membership("site-west", MemberStatus::Dead);
        if let Some(member) = membership.members.first_mut() {
            member.age_secs = 3_600;
        }
        let poll = network_with_modes(&serde_json::json!({
            "signalTransport": {"mode": "poll"},
            "staleCandidateTtlSeconds": 60
        }));
        assert_eq!(
            membership_staleness(&poll, vec![provider.clone()], Some(&membership)).len(),
            1,
            "a dead member's age must not evict its providers under poll"
        );
        let gossip = network_with_modes(&serde_json::json!({"staleCandidateTtlSeconds": 60}));
        assert!(
            membership_staleness(&gossip, vec![provider], Some(&membership)).is_empty(),
            "gossip evicts past the TTL"
        );
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
        // and the override must no longer apply, so phase returns to Available.
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
        assert_eq!(
            unless_enrolling(tls_secrets_action(false, false), true),
            TlsSecrets::Enrolls,
            "a site that enrolls never gets a self-signed placeholder"
        );
        assert_eq!(
            unless_enrolling(tls_secrets_action(false, false), false),
            TlsSecrets::Create
        );
        assert_eq!(
            unless_enrolling(tls_secrets_action(true, false), true),
            TlsSecrets::Inconsistent
        );
    }

    /// Only an operator config naming this site holds off self-signing, a refused one and one
    /// seen earlier in this process included.
    #[test]
    fn only_an_operator_config_naming_this_site_holds_off_self_signing() {
        use crate::operator_config::{OperatorConfig, Snapshot, parse};
        assert!(
            !config_enrolls(&Snapshot::absent()),
            "no config leaves the grid to self-sign"
        );
        assert!(
            !config_enrolls(&Snapshot::of_config(OperatorConfig::default())),
            "a config without a site section does not enroll"
        );
        let named = parse("site:\n  name: site-a\n")
            .unwrap_or_else(|_| std::process::abort())
            .config;
        assert!(
            config_enrolls(&Snapshot::of_config(named)),
            "a config naming this site enrolls"
        );
        assert!(
            config_enrolls(&Snapshot::unreadable("site.name: bad".to_owned())),
            "a refused config holds off until it is fixed"
        );
        let mut deleted = Snapshot::absent();
        deleted.site_seen = true;
        assert!(
            config_enrolls(&deleted),
            "a config deleted after it named this site still holds off"
        );
    }

    /// What `ensure_tls_secrets` writes, after a restart that lost the config, beside a
    /// pending Secret labelled `labels`.
    async fn writes_beside_pending(labels: &Value) -> Vec<status_harness::Write> {
        let api = status_harness::FakeApi::new();
        api.put(
            "/api/v1/namespaces/grid-system/secrets/site-tls-pending",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "type": "Opaque",
                "metadata": {"name": "site-tls-pending", "namespace": "grid-system", "labels": labels},
            }),
        );
        let network: GridNetwork = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": {"name": "grid", "creationTimestamp": "2020-01-01T00:00:00Z"},
            "spec": {"tls": {
                "caSecretRef": {"name": "grid-ca", "namespace": "grid-system"},
                "siteSecretRef": {"name": "site-tls", "namespace": "grid-system"},
            }},
        }))
        .unwrap_or_else(|_| std::process::abort());
        let client = api.client("grid-system");
        let held = Box::pin(ensure_tls_secrets(&network, &client, Some("site-d"), false)).await;
        assert!(held.is_ok(), "{held:?}");
        api.writes()
    }

    #[tokio::test]
    async fn a_restart_with_no_config_beside_enrollments_pending_secret_self_signs_nothing() {
        let ours = serde_json::json!({"app.kubernetes.io/managed-by": "grid-operator"});
        assert!(
            writes_beside_pending(&ours).await.is_empty(),
            "a site was configured here: enrollment writes the Secrets"
        );
        let planted = serde_json::json!({"app.kubernetes.io/managed-by": "someone-else"});
        assert!(
            !writes_beside_pending(&planted).await.is_empty(),
            "a pending Secret enrollment did not write holds nothing off"
        );
    }

    /// A new `GridNetwork` waits for an operator config applied with it before it self-signs.
    #[test]
    fn self_signing_waits_out_the_grace_for_a_new_grid_network() {
        assert_eq!(self_sign_wait(Some(100), 100), Some(SELF_SIGN_GRACE));
        assert_eq!(self_sign_wait(Some(100), 110), Some(Duration::from_secs(20)));
        assert_eq!(self_sign_wait(Some(100), 130), None, "the grace has passed");
        assert_eq!(self_sign_wait(Some(100), 10_000), None);
        assert_eq!(self_sign_wait(None, 100), None, "no timestamp, no wait");
        assert_eq!(
            self_sign_wait(Some(200), 100),
            Some(SELF_SIGN_GRACE),
            "a clock behind the server waits the full grace"
        );
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

    #[test]
    #[expect(clippy::expect_used, reason = "certificate trust boundary test fixture")]
    fn validated_gossip_key_requires_grid_ca_site_identity_and_matching_private_key() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let leaf = certs::generate_site_cert(&ca, "site-east").expect("leaf");
        let der = pem::parse(&leaf.key_pem).expect("key pem").into_contents();
        let validated = validated_gossip_key(
            ca.cert_pem.as_bytes(),
            leaf.cert_pem.as_bytes(),
            leaf.key_pem.as_bytes(),
            "site-east",
        )
        .expect("valid local identity");
        let wrong_name = validated_gossip_key(
            ca.cert_pem.as_bytes(),
            leaf.cert_pem.as_bytes(),
            leaf.key_pem.as_bytes(),
            "site-west",
        );
        let other_ca = certs::generate_ca("other-ca").expect("other ca");
        let wrong_ca = validated_gossip_key(
            other_ca.cert_pem.as_bytes(),
            leaf.cert_pem.as_bytes(),
            leaf.key_pem.as_bytes(),
            "site-east",
        );
        let other_leaf = certs::generate_site_cert(&ca, "site-east").expect("other leaf");
        let mismatched_key = validated_gossip_key(
            ca.cert_pem.as_bytes(),
            leaf.cert_pem.as_bytes(),
            other_leaf.key_pem.as_bytes(),
            "site-east",
        );

        assert_eq!(
            validated.as_slice(),
            der.as_slice(),
            "the installed signer is the validated PKCS8 key"
        );
        assert!(wrong_name.is_err(), "another site's leaf must not identify this site");
        assert!(wrong_ca.is_err(), "a leaf outside this grid CA must not be trusted");
        assert!(
            mismatched_key.is_err(),
            "the private signer must match the existing site certificate"
        );
    }

    #[test]
    #[expect(clippy::expect_used, reason = "peer certificate trust test fixture")]
    fn peer_gossip_pin_obeys_grid_ca_and_declared_fingerprint_policy() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let leaf = certs::generate_site_cert(&ca, "site-peer").expect("leaf");
        let fingerprint = certs::canonical_fingerprint(&leaf.cert_pem).expect("fingerprint");
        let peer = |fingerprints: Vec<String>| {
            serde_json::from_value::<GridSite>(serde_json::json!({
                "apiVersion": "grid.praxis.fast/v1alpha1",
                "kind": "GridSite",
                "metadata": { "name": "site-peer" },
                "spec": {
                    "gridNetworkRef": "grid-a",
                    "trust": { "canonicalFingerprints": fingerprints }
                },
                "status": { "publicCertPem": leaf.cert_pem.clone() }
            }))
            .expect("GridSite")
        };
        let formatted_fingerprint: String = fingerprint
            .char_indices()
            .flat_map(|(index, character)| {
                let character = character.to_ascii_uppercase();
                if index > 0 && index.is_multiple_of(2) {
                    vec![':', character]
                } else {
                    vec![character]
                }
            })
            .collect();
        let pinned = peer(vec![fingerprint]);
        let unpinned = peer(Vec::new());
        let other_ca = certs::generate_ca("other-ca").expect("other ca");
        assert!(
            verified_peer_gossip_key(&pinned, &ca.cert_pem, PeerTrustMode::Pin)
                .expect("verified certificate")
                .is_some(),
            "matching CA and declared fingerprint yield a peer pin"
        );
        assert!(
            verified_peer_gossip_key(&peer(vec![formatted_fingerprint]), &ca.cert_pem, PeerTrustMode::Pin)
                .expect("non-canonical declared fingerprint")
                .is_some(),
            "uppercase colon-separated pins authorize the same certificate"
        );
        assert!(
            verified_peer_gossip_key(&unpinned, &ca.cert_pem, PeerTrustMode::Pin).is_err(),
            "pin trust refuses a certificate with no declared fingerprint"
        );
        assert!(
            verified_peer_gossip_key(&pinned, &other_ca.cert_pem, PeerTrustMode::Spiffe).is_err(),
            "SPIFFE trust still requires the Grid CA"
        );
    }

    #[tokio::test]
    async fn an_unpersisted_grid_id_waits_without_installing_signed_state() {
        let mut network = network_with_endpoint_transport("plaintext", None);
        network.spec.grid_id = String::new();
        network.spec.peer_trust = Some(crate::crd::grid_network::PeerTrustConfig {
            mode: PeerTrustMode::Pin,
            signed_state: SignedStateMode::Enforce,
        });
        let client = crate::resources::test_doubles::mock_kube_client_with_grid_networks(HashMap::from([(
            "glb-demo",
            network.clone(),
        )]));
        let ctx = OperatorCtx::new(client, None, SignalMode::default());
        let (swim, mut runtime) = SwimHandle::detached("site-a");

        let result = sign_grid_state(&network, &ctx, &swim, "glb-demo").await;

        assert!(
            result.is_ok(),
            "missing enrollment-owned grid ID waits without failing: {result:?}"
        );
        assert_eq!(
            runtime.grids_enforced(),
            Vec::<String>::new(),
            "signed state is not enabled before enrollment provides a grid ID"
        );
    }

    #[tokio::test]
    #[expect(clippy::expect_used, reason = "certificate and API server fixture")]
    async fn peer_pins_follow_the_declared_trust_not_the_startup_one() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let leaf = certs::generate_site_cert(&ca, "site-peer").expect("leaf");
        let sites = serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridSiteList",
            "metadata": { "resourceVersion": "1" },
            "items": [{
                "apiVersion": "grid.praxis.fast/v1alpha1",
                "kind": "GridSite",
                "metadata": { "name": "site-peer" },
                "spec": { "gridNetworkRef": "glb-demo" },
                "status": { "publicCertPem": leaf.cert_pem }
            }]
        });
        let ca_secret = serde_json::to_value(crate::resources::test_doubles::secret_with_key(
            "ca.crt",
            ca.cert_pem.as_bytes(),
        ))
        .expect("Secret");
        let client = crate::resources::test_doubles::mock_kube_client_with_json(HashMap::from([
            ("ca", ca_secret),
            ("gridsites", sites),
        ]));
        let mut network = network_with_endpoint_transport("plaintext", None);
        network.spec.peer_trust = Some(crate::crd::grid_network::PeerTrustConfig {
            mode: PeerTrustMode::Pin,
            signed_state: SignedStateMode::Enforce,
        });

        let pins = collect_gossip_peer_pins(&network, &client, "site-local")
            .await
            .expect("pins");

        assert!(
            pins.is_empty(),
            "declared pin trust refuses a peer with no declared fingerprint: {pins:?}"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::expect_used,
        reason = "multiple certificate fixtures verify key accumulation and deduplication"
    )]
    async fn peer_gossip_pins_accumulate_distinct_keys_for_one_site() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let first = certs::generate_site_cert(&ca, "site-peer").expect("first leaf");
        let second = certs::generate_site_cert(&ca, "site-peer").expect("second leaf");
        let first_fingerprint = certs::canonical_fingerprint(&first.cert_pem).expect("first fingerprint");
        let second_fingerprint = certs::canonical_fingerprint(&second.cert_pem).expect("second fingerprint");
        let discovered = |name: &str, cert_pem: &str, fingerprint: &str| {
            serde_json::json!({
                "apiVersion": "grid.praxis.fast/v1alpha1",
                "kind": "GridSite",
                "metadata": {
                    "name": name,
                    "labels": { "grid.praxis.fast/auto-discovered": "true" },
                    "annotations": { "grid.praxis.fast/site-id": "site-peer" }
                },
                "spec": {
                    "gridNetworkRef": "glb-demo",
                    "trust": { "canonicalFingerprints": [fingerprint] }
                },
                "status": { "publicCertPem": cert_pem }
            })
        };
        let enrolled = serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": "site-peer" },
            "spec": {
                "gridNetworkRef": "glb-demo",
                "trust": { "canonicalFingerprints": [first_fingerprint] }
            },
            "status": { "publicCertPem": first.cert_pem }
        });
        let sites = serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridSiteList",
            "metadata": { "resourceVersion": "1" },
            "items": [
                enrolled,
                discovered("glb-demo-site-peer", &second.cert_pem, &second_fingerprint),
                discovered("glb-demo-site-peer-copy", &second.cert_pem, &second_fingerprint)
            ]
        });
        let ca_secret = serde_json::to_value(crate::resources::test_doubles::secret_with_key(
            "ca.crt",
            ca.cert_pem.as_bytes(),
        ))
        .expect("Secret");
        let client = crate::resources::test_doubles::mock_kube_client_with_json(HashMap::from([
            ("ca", ca_secret),
            ("gridsites", sites),
        ]));
        let network = network_with_endpoint_transport("plaintext", None);
        let pins = collect_gossip_peer_pins(&network, &client, "site-local")
            .await
            .expect("pins");
        let first_key = certs::verify_site_cert(&ca.cert_pem, &first.cert_pem, "site-peer").expect("first key");
        let second_key = certs::verify_site_cert(&ca.cert_pem, &second.cert_pem, "site-peer").expect("second key");

        assert!(
            matches!(pins.get("site-peer"), Some(keys) if keys.len() == 2
                && keys.contains(&first_key) && keys.contains(&second_key)),
            "both distinct certificates are retained once: {pins:?}"
        );
    }

    #[tokio::test]
    #[expect(clippy::expect_used, reason = "certificate fixture")]
    async fn signed_cert_bootstrap_republishes_on_every_reconcile() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let leaf = certs::generate_site_cert(&ca, "site-a").expect("leaf");
        let site = crate::resources::test_doubles::secret_with_key("tls.crt", leaf.cert_pem.as_bytes());
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::from([("site", site)]));
        let ctx = OperatorCtx::new(client, None, SignalMode::default());
        let (swim, mut runtime) = SwimHandle::detached("site-a");
        let network = network_with_endpoint_transport("plaintext", None);

        publish_signed_site_cert(&network, &ctx.client, &swim, "grid-a", "net").await;
        publish_signed_site_cert(&network, &ctx.client, &swim, "grid-a", "net").await;

        for reconcile in ["first", "second"] {
            let bootstrap = runtime.broadcasts.try_recv().expect(reconcile);
            assert_eq!(
                bootstrap.grid_id.as_deref(),
                Some("grid-a"),
                "{reconcile} reconcile publishes a grid-scoped bootstrap"
            );
            assert_eq!(
                bootstrap.site_cert_pem.as_deref(),
                Some(leaf.cert_pem.as_str()),
                "{reconcile} reconcile carries the site certificate for late joiners"
            );
        }
    }

    #[tokio::test]
    async fn unreadable_grid_ca_keeps_installed_pins_and_reconciles() {
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(HashMap::new());
        let ctx = OperatorCtx::new(client, None, SignalMode::default());
        let (swim, mut runtime) = SwimHandle::detached("site-a");
        let network = network_with_endpoint_transport("plaintext", None);

        let ready = apply_signed_state(&network, &ctx, &swim, "grid-a", SignedStateMode::Enforce).await;

        assert!(
            matches!(ready, Ok(false)),
            "a missing Grid CA must not fail the reconcile, nor yield a signer"
        );
        assert_eq!(
            runtime.peer_pins_sent("grid-a"),
            vec![None],
            "the grid still enforces, keeping the pins it has"
        );
    }

    #[tokio::test]
    async fn deleting_a_network_relaxes_its_trust_once_withdrawn() {
        let mut network = network_with_endpoint_transport("plaintext", None);
        network.metadata.finalizers = Some(vec![GRID_NETWORK_WITHDRAWAL_FINALIZER.to_owned()]);
        let client = crate::resources::test_doubles::mock_kube_client_with_grid_networks(HashMap::from([(
            "glb-demo",
            network.clone(),
        )]));
        let (swim, mut runtime) = SwimHandle::detached("site-a");
        let ctx = OperatorCtx::new(client, Some(Arc::new(swim)), SignalMode::default());

        let (action, withdrawn) = tokio::join!(
            reconcile_deleting_grid_network(&network, &ctx, "glb-demo"),
            runtime.publish_withdrawal()
        );

        assert!(action.is_ok(), "the finalizer is removed: {action:?}");
        assert_eq!(withdrawn.as_deref(), Some("id"));
        assert_eq!(
            runtime.grids_relaxed(),
            vec!["id".to_owned()],
            "a withdrawn grid keeps no signer or pins"
        );
    }

    #[test]
    fn only_spiffe_trust_renews() {
        let modes = |trust| GridModes {
            membership: MembershipMode::Swim,
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
            conditions: Vec::new(),
            gateway_age_reports: Vec::new(),
            gateway_age_report_overflow: false,
            connected_sites: 2,
            remote_provider_count: 2,
            grid_id: "grid-id".to_owned(),
            observed_generation: 3,
            phase: GridNetworkPhase::Active,
            praxis_config_status: Vec::new(),
            routing_map_status: Vec::new(),
            budget_status: Vec::new(),
            mount_reconciliation_status: Vec::new(),
        };
        assert!(!grid_network_status_needs_update(Some(&baseline), &baseline));

        let changed = GridNetworkStatus {
            remote_provider_count: 1,
            ..baseline.clone()
        };
        assert!(grid_network_status_needs_update(Some(&baseline), &changed));
        assert!(grid_network_status_needs_update(None, &baseline));
    }

    #[test]
    fn independently_owned_evidence_is_omitted_and_does_not_trigger_reconcile() -> Result<(), serde_json::Error> {
        let baseline = GridNetworkStatus::default();
        let mut independently_owned = baseline.clone();
        independently_owned.conditions = vec![Condition {
            type_: crate::peer_ages::PEER_TIME_VALID.to_owned(),
            status: ConditionStatus::False,
            reason: "TimestampOutOfRange".to_owned(),
            ..Default::default()
        }];
        independently_owned.gateway_age_report_overflow = true;
        independently_owned.gateway_age_reports = vec![crate::crd::grid_network::GatewayAgeReport {
            replica: "gateway".to_owned(),
            epoch: "a".repeat(32),
            sequence: 1,
            skew: 1,
            rejected: true,
            reported_rejected: true,
            received_at: "t0".to_owned(),
        }];
        assert!(
            !grid_network_status_needs_update(Some(&independently_owned), &baseline),
            "separate evidence ownership cannot cause reconcile loops"
        );
        let serialized = serde_json::to_value(&baseline)?;
        assert!(
            serialized.get("conditions").is_none()
                && serialized.get("gatewayAgeReports").is_none()
                && serialized.get("gatewayAgeReportOverflow").is_none(),
            "normal reconciliation never claims separately managed status"
        );
        Ok(())
    }

    /// A network holding `conditions` as `(type, status, reason)`.
    fn network_holding(conditions: &[(&str, ConditionStatus, &str)]) -> GridNetwork {
        let mut network = base_network();
        network.status = Some(GridNetworkStatus {
            conditions: conditions
                .iter()
                .map(|(type_, status, reason)| Condition {
                    type_: (*type_).to_owned(),
                    status: *status,
                    reason: (*reason).to_owned(),
                    ..Condition::default()
                })
                .collect(),
            ..GridNetworkStatus::default()
        });
        network
    }

    /// `(status, reason)` of `type_` in the network's roll-up.
    fn rolled_up(network: &GridNetwork, type_: &str) -> Option<(String, String)> {
        network_rollup(network)
            .into_iter()
            .find(|condition| condition.type_ == type_)
            .map(|condition| (condition.status.to_string(), condition.reason))
    }

    #[test]
    fn rejected_peer_ages_degrade_and_an_unwritten_enrollment_gate_reads_unknown() {
        let network = network_holding(&[
            (
                crate::gateway_ages::GATEWAY_AGES_REJECTED,
                ConditionStatus::True,
                "GatewayAgesRejected",
            ),
            (
                crate::peer_ages::PEER_TIME_VALID,
                ConditionStatus::False,
                "TimestampOutOfRange",
            ),
        ]);
        let pair = |status: &str, reason: &str| Some((status.to_owned(), reason.to_owned()));
        assert_eq!(
            rolled_up(&network, "Available"),
            pair("Unknown", "AwaitingFirstCheck"),
            "a gate not written yet is not evaluated, never True"
        );
        assert_eq!(rolled_up(&network, "Degraded"), pair("True", "TimestampOutOfRange"));
    }

    #[test]
    fn available_copies_the_first_failing_enrollment_gate_in_order() {
        use crate::enroll::local::{ACCEPTED, ENROLLED, IDENTITY_VALID, RESOLVED_REFS, ROTATION_ON_SCHEDULE};
        let pair = |status: &str, reason: &str| Some((status.to_owned(), reason.to_owned()));
        let gates = |identity: (ConditionStatus, &'static str)| {
            network_holding(&[
                (ROTATION_ON_SCHEDULE, ConditionStatus::False, "RenewalFailing"),
                (ENROLLED, ConditionStatus::True, "Issued"),
                (ACCEPTED, ConditionStatus::True, ACCEPTED),
                (RESOLVED_REFS, ConditionStatus::False, "RefNotFound"),
                (IDENTITY_VALID, identity.0, identity.1),
            ])
        };
        let held = gates((ConditionStatus::Unknown, "AwaitingFirstAttempt"));
        assert_eq!(
            rolled_up(&held, "Available"),
            pair("False", "RefNotFound"),
            "ResolvedRefs comes first"
        );
        let mut valid = gates((ConditionStatus::True, IDENTITY_VALID));
        if let Some(status) = valid.status.as_mut() {
            for condition in &mut status.conditions {
                if condition.type_ == RESOLVED_REFS {
                    condition.status = ConditionStatus::True;
                    condition.reason = RESOLVED_REFS.to_owned();
                }
            }
        }
        assert_eq!(
            rolled_up(&valid, "Available"),
            pair("True", "Available"),
            "RotationOnSchedule does not gate"
        );
        assert_eq!(
            rolled_up(&valid, "Degraded"),
            pair("True", "RenewalFailing"),
            "a rotation that will not happen degrades"
        );
    }

    #[test]
    fn progressing_reads_only_enrolling_or_retrying_off_enrolled() {
        use crate::enroll::local::{ENROLLED, ENROLLING, RETRYING};
        let pair = |status: &str, reason: &str| Some((status.to_owned(), reason.to_owned()));
        let enrolled = |status, reason| network_holding(&[(ENROLLED, status, reason)]);
        assert_eq!(
            rolled_up(&enrolled(ConditionStatus::Unknown, ENROLLING), "Progressing"),
            pair("True", ENROLLING)
        );
        assert_eq!(
            rolled_up(&enrolled(ConditionStatus::False, "InviteMaybeSpent"), "Progressing"),
            pair("True", RETRYING)
        );
        for (status, reason) in [
            (ConditionStatus::False, "AwaitingInvite"),
            (ConditionStatus::False, "InviteInvalid"),
            (ConditionStatus::True, "Issued"),
        ] {
            assert_eq!(
                rolled_up(&enrolled(status, reason), "Progressing").map(|(status, _)| status),
                Some("False".to_owned()),
                "{reason} is nothing in flight"
            );
        }
    }

    #[test]
    fn the_peer_conditions_degrade_the_network_after_its_own_and_never_gate_it() {
        use crate::{peer_membership::PEER_MEMBERSHIP, peer_reach};
        let pair = |status: &str, reason: &str| Some((status.to_owned(), reason.to_owned()));
        for (type_, reason) in [
            (peer_reach::PEER_SIGNALS_REACHABLE, peer_reach::PEERS_UNREACHABLE),
            (peer_reach::PEER_CATALOGS_ACCEPTED, peer_reach::CATALOGS_REFUSED),
            (PEER_MEMBERSHIP, crate::peer_membership::PEER_MEMBERSHIP_MISMATCH),
            (peer_reach::PEER_POLLS_COMPLETE, peer_reach::POLL_ROUND_OVERRUN),
        ] {
            let failing = network_holding(&[(type_, ConditionStatus::False, reason)]);
            assert_eq!(
                rolled_up(&failing, "Degraded"),
                pair("True", reason),
                "{type_} degrades"
            );
            assert_ne!(
                rolled_up(&failing, "Available").map(|(_, gate)| gate).as_deref(),
                Some(reason),
                "{type_} never gates Available"
            );
            let clear = network_holding(&[(type_, ConditionStatus::True, type_)]);
            assert_eq!(
                rolled_up(&clear, "Degraded").map(|(status, _)| status).as_deref(),
                Some("False"),
                "{type_} True clears it"
            );
        }
        let both = network_holding(&[
            (
                peer_reach::PEER_SIGNALS_REACHABLE,
                ConditionStatus::False,
                peer_reach::PEERS_UNREACHABLE,
            ),
            (
                crate::peer_ages::PEER_TIME_VALID,
                ConditionStatus::False,
                "TimestampOutOfRange",
            ),
        ]);
        assert_eq!(
            rolled_up(&both, "Degraded"),
            pair("True", "TimestampOutOfRange"),
            "this site's own inputs read first"
        );
        assert_eq!(
            rolled_up(&network_holding(&[]), "Degraded")
                .map(|(status, _)| status)
                .as_deref(),
            Some("False"),
            "removed, as under Swim, they degrade nothing"
        );
    }

    #[test]
    fn rejected_gateway_ages_degrade() {
        let network = network_holding(&[(
            crate::gateway_ages::GATEWAY_AGES_REJECTED,
            ConditionStatus::True,
            "GatewayAgesRejected",
        )]);
        let degraded = rolled_up(&network, "Degraded");
        assert_eq!(degraded, Some(("True".to_owned(), "GatewayAgesRejected".to_owned())));
    }

    #[test]
    fn a_condition_alone_never_calls_for_a_status_write() {
        let baseline = GridNetworkStatus {
            grid_id: "grid-id".to_owned(),
            phase: GridNetworkPhase::Active,
            ..GridNetworkStatus::default()
        };
        let with_condition = GridNetworkStatus {
            conditions: vec![Condition {
                type_: crate::peer_ages::PEER_TIME_VALID.to_owned(),
                status: ConditionStatus::True,
                ..Condition::default()
            }],
            ..baseline.clone()
        };
        assert!(
            !grid_network_status_needs_update(Some(&with_condition), &baseline),
            "a condition the peer poller wrote is no reason to write status, or reconcile would fight it"
        );
        assert!(
            serde_json::to_value(&baseline).is_ok_and(|value| value.get("conditions").is_none()),
            "empty conditions are left out of the reconcile's apply, so it never claims the field"
        );
    }

    #[tokio::test]
    async fn an_unconfigured_operator_waits_for_bootstrap_instead_of_minting_a_grid_id() -> Result<(), OperatorError> {
        let (api, network) = stored_grid_id_network("", "")?;
        let ctx = Arc::new(OperatorCtx::new(api.client("grid"), None, SignalMode::Gossip));
        assert_eq!(
            Box::pin(reconcile(Arc::new(network), ctx)).await?,
            Action::await_change()
        );
        let writes = api.writes();
        assert!(
            !writes.is_empty(),
            "the waiting reconcile still applies its status rollup"
        );
        assert!(
            writes
                .iter()
                .all(|write| write.status && write.body.pointer("/status/gridId").is_none()),
            "neither spec nor status gains a minted grid ID"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_refused_status_grid_id_is_a_terminal_invalid_resource() -> Result<(), OperatorError> {
        let (api, network) = stored_grid_id_network("", "legacy-grid")?;
        api.reject_next_patch(GRID_ID_PATH, None);
        let result = Box::pin(adopt_status_grid_id(&network, &api.client("grid"))).await;
        assert!(
            matches!(&result, Err(OperatorError::InvalidResource(message)) if message.contains("\"legacy-grid\"")),
            "a 422 names the status grid ID and does not retry: {result:?}"
        );
        assert_eq!(grid_id_spec_writes(&api).len(), 1, "a 422 is never retried");
        Ok(())
    }

    #[tokio::test]
    async fn a_refused_copy_adopts_a_spec_another_writer_set_meanwhile() -> Result<(), OperatorError> {
        let (api, network) = stored_grid_id_network("", "legacy-grid")?;
        let mut winner = api.get(GRID_ID_PATH).unwrap_or_default();
        if let Some(spec) = winner.get_mut("spec") {
            spec["gridId"] = serde_json::json!("bootstrap-grid");
        }
        api.reject_next_patch(GRID_ID_PATH, Some(winner));
        assert!(Box::pin(adopt_status_grid_id(&network, &api.client("grid"))).await?);
        assert_eq!(
            grid_id_spec_writes(&api).len(),
            1,
            "the refused patch is the only write"
        );
        Ok(())
    }

    #[tokio::test]
    async fn an_invalid_budget_policy_does_not_keep_a_member_status_only() -> Result<(), OperatorError> {
        let (api, mut network) = stored_grid_id_network("", "legacy-grid")?;
        network.spec.budget_policy = Some(BudgetPolicyConfig {
            tenants: vec![tenant("", 100.0)],
        });
        let ctx = Arc::new(OperatorCtx::new(api.client("grid"), None, SignalMode::Gossip));
        assert_eq!(
            Box::pin(reconcile(Arc::new(network), ctx)).await?,
            Action::await_change()
        );
        assert_eq!(
            api.get(GRID_ID_PATH)
                .and_then(|stored| stored.pointer("/spec/gridId").cloned()),
            Some(serde_json::json!("legacy-grid")),
            "the copy runs before spec validation"
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_reconcile_after_the_copy_runs_with_the_copied_spec() -> Result<(), OperatorError> {
        let (api, mut declared) = stored_grid_id_network("", "legacy-grid")?;
        // A mode the operator is not running, so reaching the mode check is observable.
        declared.spec.signal_transport = Some(serde_json::from_value(serde_json::json!({ "mode": "poll" }))?);
        api.put(GRID_ID_PATH, serde_json::to_value(&declared)?);
        let network: GridNetwork = serde_json::from_value(api.get(GRID_ID_PATH).unwrap_or_default())?;
        let (restart, restarts) = tokio::sync::watch::channel(None);
        let ctx = Arc::new(OperatorCtx::new(api.client("grid"), None, SignalMode::Gossip).with_mode_restart(restart));
        assert_eq!(
            Box::pin(reconcile(Arc::new(network), Arc::clone(&ctx))).await?,
            Action::await_change()
        );
        assert!(restarts.borrow().is_none(), "the copy returns before the mode check");
        let copied: GridNetwork = serde_json::from_value(api.get(GRID_ID_PATH).unwrap_or_default())?;
        assert_eq!(copied.spec.grid_id, "legacy-grid");
        assert_eq!(
            Box::pin(reconcile(Arc::new(copied), ctx)).await?,
            Action::await_change()
        );
        assert_eq!(
            grid_id_spec_writes(&api).len(),
            1,
            "a set spec.gridId is not written again"
        );
        assert_eq!(
            restarts.borrow().map(|next| next.signal),
            Some(SignalMode::Poll),
            "the next reconcile reaches the mode check"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_status_only_grid_id_is_copied_into_spec() -> Result<(), OperatorError> {
        let (api, network) = stored_grid_id_network("", "legacy-grid")?;
        let ctx = Arc::new(OperatorCtx::new(api.client("grid"), None, SignalMode::Gossip));
        assert_eq!(
            Box::pin(reconcile(Arc::new(network), ctx)).await?,
            Action::await_change()
        );
        let spec_writes = grid_id_spec_writes(&api);
        assert_eq!(spec_writes.len(), 1, "one spec write: {spec_writes:?}");
        assert!(
            spec_writes.iter().all(|write| {
                write.manager.as_deref() == Some(GRID_ID_FIELD_MANAGER)
                    && write.body.get("spec") == Some(&serde_json::json!({ "gridId": "legacy-grid" }))
            }),
            "the write carries spec.gridId alone under its own manager: {spec_writes:?}"
        );
        let stored = api.get(GRID_ID_PATH).unwrap_or_default();
        assert_eq!(stored.pointer("/spec/gridId"), Some(&serde_json::json!("legacy-grid")));
        assert_eq!(
            stored.pointer("/status/gridId"),
            Some(&serde_json::json!("legacy-grid")),
            "status keeps the ID it already mirrored"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_set_spec_grid_id_is_never_patched() -> Result<(), OperatorError> {
        let (api, network) = stored_grid_id_network("declared", "legacy-grid")?;
        assert!(Box::pin(adopt_status_grid_id(&network, &api.client("grid"))).await?);
        assert!(api.writes().is_empty(), "a set spec.gridId is never written");
        Ok(())
    }

    #[tokio::test]
    async fn a_grid_id_another_writer_sets_first_is_adopted_without_a_second_write() -> Result<(), OperatorError> {
        let (api, network) = stored_grid_id_network("", "legacy-grid")?;
        let mut winner = api.get(GRID_ID_PATH).unwrap_or_default();
        if let Some(spec) = winner.get_mut("spec") {
            spec["gridId"] = serde_json::json!("bootstrap-grid");
        }
        api.win_patch_race(GRID_ID_PATH, winner);
        assert!(Box::pin(adopt_status_grid_id(&network, &api.client("grid"))).await?);
        assert_eq!(
            grid_id_spec_writes(&api).len(),
            1,
            "the losing patch is the only write; the re-read adopts what is there"
        );
        assert_eq!(
            api.get(GRID_ID_PATH)
                .and_then(|stored| stored.pointer("/spec/gridId").cloned()),
            Some(serde_json::json!("bootstrap-grid")),
            "the other writer's ID stands"
        );
        Ok(())
    }

    // -----------------------------------------------------------------------
    // network_site_name: fallback helper
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
    // discovered_sites_from_swim: pure helper
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
        let identities = peer_identities(&sites, PeerTrustMode::Pin, SignalsAt::Advertised);
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
    fn under_local_a_discovered_stub_is_not_a_peer() {
        let sites = [peer_grid_site("net-remote", Some("remote"), &["pin-a"])];
        assert!(
            peer_identities(&sites, PeerTrustMode::Pin, SignalsAt::Derived(None)).is_empty(),
            "nothing discovers under Local, so a stub left from Swim names no member"
        );
        assert!(
            peer_identities(&sites, PeerTrustMode::Pin, SignalsAt::Advertised).contains_key("remote"),
            "under Swim it still counts"
        );
    }

    #[test]
    fn peer_identities_falls_back_to_name_for_local_site() {
        let sites = [peer_grid_site("site-a", None, &["pin-a"])];
        let identities = peer_identities(&sites, PeerTrustMode::Pin, SignalsAt::Advertised);
        assert!(
            identities.contains_key("site-a"),
            "a bare-named site falls back to metadata.name"
        );
    }

    #[test]
    fn peer_identities_blank_annotation_falls_back_to_name() {
        let sites = [peer_grid_site("site-a", Some("   "), &["pin-a"])];
        let identities = peer_identities(&sites, PeerTrustMode::Pin, SignalsAt::Advertised);
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
            let identities = peer_identities(&order, PeerTrustMode::Pin, SignalsAt::Advertised);
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
        identities.set(peer_identities(&sites, PeerTrustMode::Pin, SignalsAt::Advertised));
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
        identities.set(peer_identities(&sites, PeerTrustMode::Pin, SignalsAt::Advertised));
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
            let identities = peer_identities(&order, PeerTrustMode::Pin, SignalsAt::Advertised);
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
            spec.pointer("/spec/gatewayEndpoint/tls"),
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
            let identities = peer_identities(&order, trust, SignalsAt::Advertised);
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
    fn under_swim_a_peer_is_polled_where_swim_advertises() {
        let mut site = peer_grid_site("east", None, &[]);
        site.spec.gateway_endpoint = Some(crate::crd::grid_site::EgressConfig {
            address: "east.example.com:443".to_owned(),
            tls: crate::crd::grid_site::EgressTls::default(),
        });
        let swim = peer_identities(
            std::slice::from_ref(&site),
            PeerTrustMode::Spiffe,
            SignalsAt::Advertised,
        );
        assert!(
            swim.get("east").is_some_and(|record| record.signals.is_none()),
            "under Swim a peer is polled where SWIM says, not at a derived endpoint"
        );
    }

    #[test]
    fn a_declared_peer_is_polled_at_its_gateway_host_under_its_signals_name() {
        let mut site = peer_grid_site("east", None, &[]);
        site.spec.gateway_endpoint = Some(crate::crd::grid_site::EgressConfig {
            address: "east.example.com:443".to_owned(),
            tls: crate::crd::grid_site::EgressTls::default(),
        });
        for (port, authority) in [(None, "east.example.com:443"), (Some(9091), "east.example.com:9091")] {
            let identities = peer_identities(
                std::slice::from_ref(&site),
                PeerTrustMode::Spiffe,
                SignalsAt::Derived(port),
            );
            let signals = identities
                .get("east")
                .and_then(|record| record.signals.clone())
                .unwrap_or_else(|| std::process::abort());
            assert_eq!(signals.endpoint.authority(), authority, "{port:?}");
            assert_eq!(signals.server_name.as_deref(), Some("east-signals.grid.internal"));
        }
        let mut stub = peer_grid_site("net-east", Some("east"), &[]);
        stub.spec.gateway_endpoint.clone_from(&site.spec.gateway_endpoint);
        let identities = peer_identities(&[stub], PeerTrustMode::Spiffe, SignalsAt::Derived(None));
        assert!(
            !identities.contains_key("east"),
            "a stub is no peer under Local, so its gossiped address is never polled"
        );
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
        identities.set(peer_identities(&sites, PeerTrustMode::Spiffe, SignalsAt::Advertised));
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
            let identities = peer_identities(&order, PeerTrustMode::Spiffe, SignalsAt::Advertised);
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
            spec.pointer("/spec/gatewayEndpoint/tls/serverName")
                .and_then(Value::as_str),
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

    #[test]
    fn a_site_reserves_its_providers_and_its_explicit_clusters() {
        let network = network_with_endpoint_transport("plaintext", None);
        let provider = make_provider("vllm-a", "glb-demo", "local_model", 1);
        let reserved = reserved_clusters(&network, std::slice::from_ref(&provider));
        let identity = routing_overlay::routing_identity(&provider).unwrap_or_else(|| std::process::abort());
        assert!(reserved.contains(identity), "{reserved:?}");
        assert!(reserved.contains("sim-provider-us-west"), "{reserved:?}");
        assert_eq!(reserved.len(), 2);
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
                "consumerGateways": [{
                    "name": "consumer-gateway",
                    "namespace": "grid-system",
                    "praxisConfig": {
                        "generate": true,
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
                    "swimKeySecretRef": null
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
    // already_recorded_invalid: grid#42 reconcile-hot-loop regression guard
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
    // decide_cert_pem_write (grid#42 acceptance criterion):
    //
    //   "reconciling an unchanged remote site must decide NoOp for every
    //    possible cert-PEM outcome", i.e. a stable GridSite never causes a
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
        // Status shows a *different* rejection (or none) and must not be
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
    // overlay_configmap_matches: grid#42 no-op write guard
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
            site_name: None,
            model_discovery_contract_version: None,
            provider_hop_endpoints: Vec::new(),
            praxis_config: None,
        }
    }

    /// Supply controlled kube errors to test status mapping without an apiserver.
    fn api_error(code: u16) -> kube::Error {
        kube::Error::Api(Box::new(
            kube::core::Status::failure("scripted", "Scripted").with_code(code),
        ))
    }

    /// Supply scripted apiserver responses to test error mapping through the kube client.
    fn apiserver_answering(code: u16) -> Client {
        let service = tower::service_fn(move |_req: http::Request<kube::client::Body>| async move {
            let status = json!({
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "message": "scripted",
                "reason": "Scripted",
                "code": code,
            });
            let response = http::Response::builder()
                .status(code)
                .body(kube::client::Body::from(
                    serde_json::to_vec(&status).unwrap_or_else(|_| std::process::abort()),
                ))
                .unwrap_or_else(|_| std::process::abort());
            Ok::<_, std::convert::Infallible>(response)
        });
        Client::new(service, "default")
    }

    /// Exercise delegated patch error mapping with a scripted apiserver refusal.
    async fn patch_refused_by_apiserver(code: u16) -> Result<OperatorError, Box<dyn std::error::Error>> {
        let deployment: Deployment = serde_json::from_value(json!({
            "metadata": {"name": "inference-gw", "namespace": "praxis-system", "resourceVersion": "7"}
        }))?;
        let deployments: Api<Deployment> = Api::namespaced(apiserver_answering(code), "praxis-system");
        match patch_delegated_deployment(
            &deployments,
            "inference-gw",
            &deployment,
            json!({"metadata": {}}),
            "the staged patch was refused",
        )
        .await
        {
            Ok(()) => Err(format!("a {code} from the apiserver must not look like a successful patch").into()),
            Err(error) => Ok(error),
        }
    }

    fn make_consumer_config(cm_name: &str) -> ConsumerConfig {
        ConsumerConfig {
            generate: true,
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
            swim_key_secret_ref: None,
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
            routing_map_status: vec![prior.clone()],
            ..GridNetworkStatus::default()
        });

        let status = retained_overlay_status(&network, &gw, 5, None, "RoutingMapApplyFailed", "overlay apply failed");

        assert_eq!(status.phase, OverlayPhase::Retained);
        assert_eq!(status.rendered_revision, prior.rendered_revision);
        assert_eq!(status.distributed_revision, prior.distributed_revision);
        assert_eq!(status.content_digest, prior.content_digest);
        assert_eq!(status.config_map_resource_version, prior.config_map_resource_version);
        assert_eq!(status.candidate_count, prior.candidate_count);
        assert_eq!(status.rendered_at, prior.rendered_at);
        assert_eq!(status.reason, "RoutingMapApplyFailed");
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
            "RoutingMapApplyFailed",
            "overlay ConfigMap apply failed",
        );

        assert_eq!(status.phase, OverlayPhase::Error);
        assert!(status.rendered_revision.is_empty());
        assert!(status.distributed_revision.is_empty());
        assert!(status.content_digest.is_empty());
        assert!(status.config_map_resource_version.is_empty());
        assert_eq!(status.reason, "RoutingMapApplyFailed");
        assert!(status.message.contains("no valid overlay has been distributed"));
        assert_eq!(status.observed_generation, 1);
    }

    #[test]
    fn retained_overlay_status_does_not_retain_an_error_without_revision() {
        let gw = make_gw_ref("gw", "grid-system");
        let mut network = base_network();
        network.status = Some(GridNetworkStatus {
            routing_map_status: vec![OverlayRevisionStatus {
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
                reason: "RoutingMapApplyFailed".to_owned(),
                message: "no valid overlay has been distributed".to_owned(),
                observed_generation: 1,
            }],
            ..GridNetworkStatus::default()
        });

        let status = retained_overlay_status(
            &network,
            &gw,
            2,
            None,
            "RoutingMapRenderFailed",
            "overlay render failed",
        );

        assert_eq!(status.phase, OverlayPhase::Error);
        assert!(status.rendered_revision.is_empty());
        assert!(status.distributed_revision.is_empty());
        assert_eq!(status.reason, "RoutingMapRenderFailed");
    }

    // -----------------------------------------------------------------------
    // keep_rendered_at: status-only churn must not retrigger reconcile
    // -----------------------------------------------------------------------

    const FRESH: &str = "2026-10-01T12:00:00Z";

    fn only_overlay(status: &GridNetworkStatus) -> &OverlayRevisionStatus {
        status
            .routing_map_status
            .first()
            .unwrap_or_else(|| std::process::abort())
    }

    fn network_with_overlay(prior: OverlayRevisionStatus) -> GridNetwork {
        let mut network = base_network();
        network.status = Some(GridNetworkStatus {
            routing_map_status: vec![prior],
            ..GridNetworkStatus::default()
        });
        network
    }

    fn desired_with_overlay(network: &GridNetwork, overlay: OverlayRevisionStatus) -> GridNetworkStatus {
        GridNetworkStatus {
            routing_map_status: keep_rendered_at(network.status.as_ref(), vec![overlay]),
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
            reason: "RoutingMapApplyFailed".to_owned(),
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
            routing_map_status: vec![prior.clone()],
            ..GridNetworkStatus::default()
        });
        let render = make_render_result(&"b".repeat(64), 3);

        let status = retained_overlay_status(&network, &gw, 5, Some(&render), "RoutingMapApplyFailed", "apply failed");

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

        let status = retained_overlay_status(&network, &gw, 1, Some(&render), "RoutingMapApplyFailed", "apply failed");

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
            routing_map_status: vec![prior.clone()],
            ..GridNetworkStatus::default()
        });

        let status = retained_overlay_status(&network, &gw, 7, None, "RoutingMapRenderFailed", "render failed");

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

    /// A candidate naming one cluster at one site, for the topology arm.
    fn topology_candidate(cluster: &str, site: &str) -> routing_overlay::RoutingCandidate {
        serde_json::from_value(serde_json::json!({
            "kind": "inference_model",
            "name": "model-x",
            "site": site,
            "cluster": cluster,
            "fresh": true
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    /// An explicit entry a human typed, transport and all.
    fn typed_endpoint(cluster: &str, address: &str) -> ClusterEndpointConfig {
        ClusterEndpointConfig {
            cluster: cluster.to_owned(),
            address: address.to_owned(),
            transport: Some(crate::crd::grid_network::EndpointTransport {
                mode: TransportMode::Plaintext,
                sni: None,
                ca_secret_ref: None,
            }),
        }
    }

    /// Declarations with nothing to derive from, so only the arm is under test.
    fn empty_declarations() -> derived_topology::Declarations<'static> {
        derived_topology::Declarations {
            providers: &[],
            sites: &[],
            local_site: "site-a",
            network_name: "net",
            from_providers: &[],
            transport: None,
            signal: SignalMode::Gossip,
            peer_catalog: None,
        }
    }

    #[test]
    fn without_derive_topology_typed_entries_pass_through_and_an_untyped_candidate_is_withdrawn() {
        let mut gw = make_gw_ref("inference-gw", "praxis-system");
        let typed = vec![typed_endpoint("prov-a", "a.example.invalid:8080")];
        gw.praxis_config = Some(ConsumerConfig {
            cluster_endpoints: typed.clone(),
            ..make_consumer_config("praxis-consumer-config")
        });
        let candidates = vec![topology_candidate("prov-b", "site-b")];
        let (endpoints, summary, refused) = gateway_cluster_endpoints(&gw, &candidates, &empty_declarations());
        assert_eq!(endpoints, typed, "typed entries pass through whole");
        assert_eq!(
            refused.iter().map(|r| r.reason).collect::<Vec<_>>(),
            [derived_topology::Refusal::NotAllowlisted],
            "a candidate with no entry is withdrawn rather than failing the whole render"
        );
        assert_eq!(summary, "withdrew 1 (prov-b: not allowlisted)");
    }

    #[test]
    fn a_disabled_consumer_config_withdraws_nothing() {
        let mut gw = make_gw_ref("inference-gw", "praxis-system");
        gw.praxis_config = Some(ConsumerConfig {
            generate: false,
            ..make_consumer_config("praxis-consumer-config")
        });
        let candidates = vec![topology_candidate("prov-b", "site-b")];
        let (endpoints, summary, refused) = gateway_cluster_endpoints(&gw, &candidates, &empty_declarations());
        assert!(endpoints.is_empty() && summary.is_empty());
        assert!(
            refused.is_empty(),
            "nothing renders, so nothing is withdrawn from its overlay"
        );
    }

    /// A delegated gateway generating config with `router`.
    fn site_route_gateway(router: SiteRouter) -> GatewayRef {
        let mut gw = make_gw_ref("inference-gw", "praxis-system");
        gw.praxis_config = Some(ConsumerConfig {
            cluster_endpoints: vec![
                typed_endpoint("prov-a", "a.example.invalid:8080"),
                typed_endpoint("prov-b", "b.example.invalid:8080"),
                typed_endpoint("prov-c", "c.example.invalid:8080"),
            ],
            mount_reconciliation: Some(
                serde_json::from_value(json!({"enabled": true})).unwrap_or_else(|_| std::process::abort()),
            ),
            site_router: router,
            ..make_consumer_config("praxis-consumer-config")
        });
        gw
    }

    #[test]
    fn site_route_prerequisites_fail_closed() {
        let gw = site_route_gateway(SiteRouter::GridSiteRoute);
        let cc = gw.praxis_config.clone().unwrap_or_else(|| std::process::abort());
        assert_eq!(site_route_unmet(&gw, &cc, true), None, "every prerequisite met");
        assert_eq!(
            site_route_unmet(&gw, &cc, false),
            Some(SiteRoutePrerequisite::PollTransport),
            "no serving config is written outside poll"
        );
        for mounts in [None, Some(json!({"enabled": false}))] {
            let cc = ConsumerConfig {
                mount_reconciliation: mounts
                    .map(|value| serde_json::from_value(value).unwrap_or_else(|_| std::process::abort())),
                ..cc.clone()
            };
            assert_eq!(
                site_route_unmet(&gw, &cc, true),
                Some(SiteRoutePrerequisite::MountReconciliation),
                "without a rollout per revision the gateway never re-reads its hop backends"
            );
        }
        let mut declared = gw;
        declared.provider_hop_endpoints = vec![
            serde_json::from_value(json!({
                "cluster": "prov-b", "transport": {"mode": "mutual_tls", "sni": "site-b.grid.internal"}
            }))
            .unwrap_or_else(|_| std::process::abort()),
        ];
        assert_eq!(
            site_route_unmet(&declared, &cc, true),
            Some(SiteRoutePrerequisite::DeclaredProviderHops)
        );
    }

    #[test]
    fn site_route_serving_hops_are_the_rendered_mutual_tls_endpoints() {
        let mtls = |cluster: &str, sni: &str| ClusterEndpointConfig {
            cluster: cluster.to_owned(),
            address: "203.0.113.20:8443".to_owned(),
            transport: Some(crate::crd::grid_network::EndpointTransport {
                mode: TransportMode::MutualTls,
                sni: Some(sni.to_owned()),
                ca_secret_ref: None,
            }),
        };
        let endpoints = vec![
            mtls("site-b", " site-b.grid.internal "),
            typed_endpoint("prov-c", "c.example.invalid:8080"),
        ];
        let mut gw = make_gw_ref("inference-gw", "praxis-system");
        gw.provider_hop_endpoints = vec![
            serde_json::from_value(json!({
                "cluster": "declared", "transport": {"mode": "mutual_tls", "sni": "declared.grid.internal"}
            }))
            .unwrap_or_else(|_| std::process::abort()),
        ];
        let overlay = routing_overlay::RoutingOverlay {
            network: "net".to_owned(),
            local_site: "site-a".to_owned(),
            candidates: vec![topology_candidate("site-b", "site-b")],
            excluded: Vec::new(),
            selection_policy: None,
            generated_at: None,
        };
        let derived =
            serving_hop_sni(&overlay, &gw, HopSource::Rendered(&endpoints)).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            derived.into_iter().collect::<Vec<_>>(),
            [("site-b".to_owned(), "site-b.grid.internal".to_owned())],
            "the hop is the rendered mTLS endpoint at its rendered SNI, never the hand list"
        );
        let declared = serving_hop_sni(&overlay, &gw, HopSource::Declared).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            declared.keys().collect::<Vec<_>>(),
            ["declared"],
            "intelligentRoute keeps the hand list"
        );
        let empty = routing_overlay::RoutingOverlay {
            candidates: Vec::new(),
            ..overlay
        };
        assert!(
            serving_hop_sni(&empty, &gw, HopSource::Rendered(&endpoints))
                .unwrap_or_else(|_| std::process::abort())
                .is_empty(),
            "no candidate receives hop context after an authoritative withdrawal"
        );
    }

    #[test]
    fn an_mcp_tool_candidate_is_never_withdrawn() {
        let mut gw = make_gw_ref("inference-gw", "praxis-system");
        gw.praxis_config = Some(make_consumer_config("praxis-consumer-config"));
        let mut tool = topology_candidate("prov-b", "site-b");
        tool.kind = "mcp_tool".to_owned();
        let mut overlay = routing_overlay::RoutingOverlay {
            network: "net".to_owned(),
            local_site: "site-a".to_owned(),
            candidates: vec![tool, topology_candidate("prov-b", "site-b")],
            excluded: Vec::new(),
            selection_policy: None,
            generated_at: None,
        };
        let (_, _, refused) = gateway_cluster_endpoints(&gw, &overlay.candidates, &empty_declarations());
        withdraw_refused(&mut overlay, &refused);
        let kinds: Vec<&str> = overlay.candidates.iter().map(|c| c.kind.as_str()).collect();
        assert_eq!(
            kinds,
            ["mcp_tool"],
            "tools render no load balancer cluster, so they need no endpoint"
        );
    }

    #[test]
    fn with_an_empty_allowlist_a_gateways_own_entries_pass_through_untouched() {
        // An empty allowlist reads as no provider; typed entries pass through.
        let mut gw = make_gw_ref("inference-gw", "praxis-system");
        let typed = vec![typed_endpoint("prov-a", "a.example.invalid:8080")];
        gw.praxis_config = Some(ConsumerConfig {
            derive_topology: Some(crate::crd::grid_network::DeriveTopology {
                from_providers: Vec::new(),
                transport: None,
            }),
            cluster_endpoints: typed.clone(),
            ..make_consumer_config("praxis-consumer-config")
        });
        let candidates = vec![topology_candidate("prov-a", "site-a")];
        let (endpoints, summary, _) = gateway_cluster_endpoints(&gw, &candidates, &empty_declarations());
        assert_eq!(
            endpoints, typed,
            "an empty allowlist derives nothing and keeps the typed entries"
        );
        assert!(summary.is_empty(), "nothing derived, nothing to report");
    }

    #[test]
    fn with_derive_topology_an_unresolvable_candidate_leaves_the_topology_empty() {
        let mut gw = make_gw_ref("inference-gw", "praxis-system");
        gw.praxis_config = Some(ConsumerConfig {
            derive_topology: Some(crate::crd::grid_network::DeriveTopology {
                from_providers: vec!["prov-a".to_owned(), "prov-b".to_owned(), "prov-ghost".to_owned()],
                transport: None,
            }),
            ..make_consumer_config("praxis-consumer-config")
        });
        let candidates = vec![topology_candidate("prov-ghost", "site-ghost")];
        let (endpoints, summary, refused) = gateway_cluster_endpoints(&gw, &candidates, &empty_declarations());
        assert!(endpoints.is_empty(), "nothing resolves");
        assert_eq!(
            refused,
            [derived_topology::Refused {
                cluster: "prov-ghost".to_owned(),
                site: "site-ghost".to_owned(),
                reason: derived_topology::Refusal::SiteUnknown,
            }],
            "the refusal names the cluster and why"
        );
        assert_eq!(summary, "withdrew 1 (prov-ghost: site unknown)");
    }

    #[test]
    fn a_refused_candidate_is_withdrawn_into_excluded_rather_than_failing_the_render() {
        let mut overlay = routing_overlay::RoutingOverlay {
            network: "net".to_owned(),
            local_site: "site-a".to_owned(),
            candidates: vec![
                topology_candidate("prov-a", "site-a"),
                topology_candidate("prov-ghost", "site-ghost"),
            ],
            excluded: Vec::new(),
            selection_policy: None,
            generated_at: None,
        };
        let refused = vec![derived_topology::Refused {
            cluster: "prov-ghost".to_owned(),
            site: "site-ghost".to_owned(),
            reason: derived_topology::Refusal::NotAllowlisted,
        }];
        withdraw_refused(&mut overlay, &refused);
        let routed: Vec<&str> = overlay.candidates.iter().map(|c| c.cluster.as_str()).collect();
        assert_eq!(routed, ["prov-a"], "the refused candidate leaves the route list");
        assert_eq!(
            overlay.excluded.len(),
            1,
            "and stays known, so the model answers 503 not 404"
        );
        assert_eq!(overlay.excluded.first().map(|c| c.cluster.as_str()), Some("prov-ghost"));
    }

    #[test]
    fn with_derive_topology_a_typed_entry_for_no_candidate_is_kept() {
        // The renderer emits every entry, so a later overlay can route to it without a new config.
        let mut gw = make_gw_ref("inference-gw", "praxis-system");
        let typed = vec![typed_endpoint("retired", "retired.example.invalid:8080")];
        gw.praxis_config = Some(ConsumerConfig {
            derive_topology: Some(crate::crd::grid_network::DeriveTopology {
                from_providers: vec!["prov-a".to_owned(), "prov-b".to_owned(), "prov-ghost".to_owned()],
                transport: None,
            }),
            cluster_endpoints: typed.clone(),
            ..make_consumer_config("praxis-consumer-config")
        });
        let (endpoints, ..) = gateway_cluster_endpoints(&gw, &[], &empty_declarations());
        assert_eq!(endpoints, typed);
    }

    /// A local `http` provider at `site-a` that derivation can resolve.
    fn local_http_provider(name: &str, host: &str) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local_model",
                "endpoint": format!("http://{host}:8000"),
                "models": [{ "name": "model-x" }]
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn removing_a_provider_withdraws_its_derived_endpoint_from_the_render() {
        // The typed entry keeps the inventory non-empty once every provider is gone.
        let steps: [&[&str]; 3] = [&["prov-a", "prov-b"], &["prov-a"], &[]];
        for live in steps {
            let providers: Vec<InferenceProvider> = live
                .iter()
                .map(|name| local_http_provider(name, &format!("{name}.models.svc")))
                .collect();
            let declarations = derived_topology::Declarations {
                providers: &providers,
                ..empty_declarations()
            };
            let mut gw = make_gw_ref("inference-gw", "praxis-system");
            gw.site_name = Some("site-a".to_owned());
            let cc = ConsumerConfig {
                cluster_endpoints: vec![typed_endpoint("typed-c", "typed-c.models.svc:8000")],
                ..make_consumer_config("praxis-consumer-config")
            };
            gw.praxis_config = Some(cc.clone());
            let mut overlay = routing_overlay::RoutingOverlay {
                network: "net".to_owned(),
                local_site: "site-a".to_owned(),
                candidates: vec![
                    topology_candidate("prov-a", "site-a"),
                    topology_candidate("prov-b", "site-a"),
                    topology_candidate("typed-c", "site-a"),
                ],
                excluded: Vec::new(),
                selection_policy: None,
                generated_at: None,
            };
            let (endpoints, summary, refused) = gateway_cluster_endpoints(&gw, &overlay.candidates, &declarations);
            withdraw_refused(&mut overlay, &refused);
            let render = consumer_config::render_consumer_config_with_projected(
                &overlay,
                &cc.credential_mount_path,
                &endpoints,
                &cc.tls_cert_mount_path,
                cc.listener_port,
                &crate::crd::grid_network::TlsConfig::default(),
                &gw.name,
                &gw.namespace,
                None,
                false,
                false,
                cc.site_router,
            );
            assert!(render.is_ok(), "{live:?}: the render must succeed: {render:?}");
            let rendered = render.unwrap_or_else(|_| std::process::abort());
            for cluster in ["prov-a", "prov-b"] {
                let address = format!("{cluster}.models.svc:8000");
                assert_eq!(
                    rendered.config_yaml.contains(&address),
                    live.contains(&cluster),
                    "{live:?}: {address} is live only while its provider exists"
                );
                assert_eq!(
                    overlay.candidates.iter().any(|c| c.cluster == cluster),
                    live.contains(&cluster),
                    "{live:?}: {cluster} routes only while its provider exists"
                );
                assert_eq!(
                    summary.contains(&format!("{cluster}: no provider")),
                    !live.contains(&cluster),
                    "{live:?}: the status names {cluster} once withdrawn: {summary}"
                );
            }
            assert!(
                rendered.config_yaml.contains("typed-c.models.svc:8000"),
                "typed entries stay"
            );
        }
    }

    #[test]
    fn a_gateway_with_no_consumer_config_has_no_topology() {
        let gw = make_gw_ref("inference-gw", "praxis-system");
        let candidates = vec![topology_candidate("prov-a", "site-a")];
        let (endpoints, summary, _) = gateway_cluster_endpoints(&gw, &candidates, &empty_declarations());
        assert!(endpoints.is_empty());
        assert!(summary.is_empty());
    }

    #[test]
    fn consumer_config_status_rendered_has_rendered_phase() {
        let gw = make_gw_ref("inference-gw", "praxis-system");
        let cc = make_consumer_config("praxis-consumer-config");
        let status = consumer_config_status_rendered(&gw, &cc, 5, "");
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
    fn a_forbidden_delegated_deployment_preserves_refusal_and_grant_guidance() {
        let gateway = make_gw_ref("inference-gw", "praxis-system");
        for verb in ["get", "patch"] {
            let fallback = mount_failure("DeploymentReadFailed", "unused");
            let error = delegated_deployment_failure(api_error(403), verb, fallback);
            let status = mount_reconciliation_status_error(&gateway, None, &error, 7);
            assert_eq!(
                status.reason, "DeploymentForbidden",
                "a 403 on {verb} must report the apiserver refusal"
            );
            assert!(
                status.message.contains(&format!("refused {verb}")) && status.message.contains("scripted"),
                "the message must preserve the refused verb and apiserver explanation: {}",
                status.message
            );
            assert!(
                status.message.contains("If RBAC denied access") && status.message.contains("deploymentName"),
                "the grant guidance must be conditional on RBAC denying access"
            );
            assert_eq!(status.phase, MountReconciliationPhase::Error, "a refusal must not wait");
        }
    }

    #[test]
    fn a_conflicting_delegated_deployment_write_still_retries() {
        let fallback = mount_failure("DeploymentPatchFailed", "unused");
        let error = delegated_deployment_failure(api_error(409), "patch", fallback);
        assert!(
            error.is_conflict(),
            "a 409 must stay a kube conflict so the caller re-reads and retries"
        );
    }

    #[test]
    fn other_delegated_deployment_errors_keep_the_callers_reason() {
        let gateway = make_gw_ref("inference-gw", "praxis-system");
        for code in [401, 404, 422, 500] {
            let fallback = mount_failure("DeploymentPatchFailed", "the staged patch was refused");
            let error = delegated_deployment_failure(api_error(code), "patch", fallback);
            let status = mount_reconciliation_status_error(&gateway, None, &error, 7);
            assert_eq!(
                status.reason, "DeploymentPatchFailed",
                "a {code} is the caller's own failure"
            );
            assert_eq!(
                status.message, "the staged patch was refused",
                "the caller's message is kept for a {code}"
            );
        }
    }

    #[tokio::test]
    async fn a_refused_delegated_deployment_read_reports_forbidden() {
        let gateway = make_gw_ref("inference-gw", "praxis-system");
        let delegation = MountReconciliation {
            enabled: true,
            deployment_name: Some("inference-gw".to_owned()),
            container_name: "praxis".to_owned(),
        };
        let rendered = consumer_config::ConsumerRenderResult {
            config_yaml: "listeners: []".to_owned(),
            requirements: Vec::new(),
        };
        let result = Box::pin(reconcile_delegated_gateway(
            &rendered,
            "production",
            &gateway,
            &make_consumer_config("consumer-config"),
            &delegation,
            &make_serving_tls(),
            7,
            &apiserver_answering(403),
        ))
        .await;
        // A 403 on the Deployment read must not reconcile.
        let error = result.err().unwrap_or_else(|| std::process::abort());
        let status = mount_reconciliation_status_error(&gateway, None, &error, 7);
        assert_eq!(status.reason, "DeploymentForbidden", "{}", status.message);
        assert!(status.message.contains("refused get"), "{}", status.message);
    }

    #[tokio::test]
    async fn a_refused_delegated_deployment_patch_reports_forbidden() -> Result<(), Box<dyn std::error::Error>> {
        let gateway = make_gw_ref("inference-gw", "praxis-system");
        for (code, reason) in [(403, "DeploymentForbidden"), (500, "DeploymentPatchFailed")] {
            let error = Box::pin(patch_refused_by_apiserver(code)).await?;
            let status = mount_reconciliation_status_error(&gateway, None, &error, 7);
            assert_eq!(status.reason, reason, "a {code} on patch must surface as {reason}");
        }
        let error = Box::pin(patch_refused_by_apiserver(409)).await?;
        assert!(
            error.is_conflict(),
            "a 409 on patch must stay a conflict for the retry loop"
        );
        Ok(())
    }

    #[test]
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
    fn a_failed_render_status_names_the_withdrawn_clusters() {
        let gw = make_gw_ref("inference-gw", "praxis-system");
        let cc = make_consumer_config("praxis-consumer-config");
        let err = OperatorError::ConsumerConfigRender(ConsumerConfigError::NoClusterEndpoints);
        let status = with_derived_summary(
            consumer_config_status_error(&gw, &cc, &err, 1),
            "withdrew 1 (prov-a: not allowlisted)",
        );
        assert!(status.message.contains("prov-a: not allowlisted"), "{}", status.message);
        let bare = with_derived_summary(consumer_config_status_error(&gw, &cc, &err, 1), "");
        assert!(!bare.message.ends_with("; "), "no empty suffix: {}", bare.message);
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
            status.reason, "PraxisConfigRenderFailed",
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
        cc.generate = false;
        let status = consumer_config_status_disabled(&gw, &cc, 2);
        assert_eq!(status.phase, ConsumerConfigPhase::Disabled, "must set phase=Disabled");
        assert_eq!(
            status.reason, "PraxisConfigDisabled",
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
            reason: "PraxisConfigRenderFailed".to_owned(),
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
        let status_a = consumer_config_status_rendered(&gw_a, &cc_a, 1, "");
        let status_b = consumer_config_status_rendered(&gw_b, &cc_b, 1, "");
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
        // A declared GridSite is keyed by its own name.
        let sites = vec![make_active_grid_site("site-west", "net")];
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            is_crdt_provider_routing_eligible("net", &sites, &provider, SignalMode::Gossip),
            "Active GridSite must make CRDT provider eligible"
        );
    }

    #[test]
    fn a_discovered_stub_names_its_site_by_annotation() {
        let stub: GridSite = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridSite",
            "metadata": {
                "name": "net-site-west",
                "labels": { LABEL_AUTO_DISCOVERED: "true" },
                "annotations": { ANNOTATION_SITE_ID: "site-west" }
            },
            "spec": { "gridNetworkRef": "net" },
            "status": { "phase": "Active" }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            is_crdt_provider_routing_eligible("net", &[stub], &provider, SignalMode::Gossip),
            "a stub is keyed by its annotation"
        );
        let composite = vec![make_active_grid_site("net-site-west", "net")];
        assert!(
            !is_crdt_provider_routing_eligible("net", &composite, &provider, SignalMode::Gossip),
            "a declared object is keyed by its name alone"
        );
    }

    #[test]
    fn under_poll_an_unreachable_site_keeps_its_providers() {
        let provider = make_eligible_crdt_provider("net", "site-west");
        for (phase, kept) in [
            ("Unreachable", true),
            ("Active", true),
            ("Connecting", false),
            ("Discovered", false),
            ("Pending", false),
            ("Left", false),
        ] {
            let sites = vec![make_phase_grid_site("site-west", "net", phase)];
            assert_eq!(
                is_crdt_provider_routing_eligible("net", &sites, &provider, SignalMode::Poll),
                kept,
                "{phase} under poll"
            );
        }
        let unreachable = vec![make_phase_grid_site("site-west", "net", "Unreachable")];
        assert!(
            !is_crdt_provider_routing_eligible("net", &unreachable, &provider, SignalMode::Gossip),
            "gossip still withdraws an unreachable site"
        );
        assert!(
            !is_crdt_provider_routing_eligible("net", &[], &provider, SignalMode::Poll),
            "a deleted GridSite withdraws its providers under poll"
        );
    }

    #[test]
    fn connecting_grid_site_excludes_crdt_provider() {
        let sites = vec![make_phase_grid_site("site-west", "net", "Connecting")];
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            !is_crdt_provider_routing_eligible("net", &sites, &provider, SignalMode::Gossip),
            "Connecting GridSite must NOT make CRDT provider eligible"
        );
    }

    #[test]
    fn discovered_grid_site_excludes_crdt_provider() {
        let sites = vec![make_phase_grid_site("site-west", "net", "Discovered")];
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            !is_crdt_provider_routing_eligible("net", &sites, &provider, SignalMode::Gossip),
            "Discovered GridSite must NOT make CRDT provider eligible"
        );
    }

    #[test]
    fn pending_grid_site_excludes_crdt_provider() {
        let sites = vec![make_phase_grid_site("site-west", "net", "Pending")];
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            !is_crdt_provider_routing_eligible("net", &sites, &provider, SignalMode::Gossip),
            "Pending GridSite must NOT make CRDT provider eligible"
        );
    }

    #[test]
    fn unreachable_grid_site_excludes_crdt_provider() {
        let sites = vec![make_phase_grid_site("site-west", "net", "Unreachable")];
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            !is_crdt_provider_routing_eligible("net", &sites, &provider, SignalMode::Gossip),
            "Unreachable GridSite must NOT make CRDT provider eligible"
        );
    }

    #[test]
    fn missing_grid_site_excludes_crdt_provider() {
        let sites: Vec<GridSite> = vec![];
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            !is_crdt_provider_routing_eligible("net", &sites, &provider, SignalMode::Gossip),
            "No matching GridSite must NOT make CRDT provider eligible (fail-closed)"
        );
    }

    #[test]
    fn wrong_network_grid_site_excludes_crdt_provider() {
        // GridSite is for a different network
        let sites = vec![make_active_grid_site("site-west", "other-net")];
        let provider = make_eligible_crdt_provider("net", "site-west");
        assert!(
            !is_crdt_provider_routing_eligible("net", &sites, &provider, SignalMode::Gossip),
            "Wrong-network GridSite must NOT make CRDT provider eligible"
        );
    }

    #[test]
    fn wrong_network_provider_excludes_crdt_provider() {
        let sites = vec![make_active_grid_site("site-west", "net")];
        let provider = make_eligible_crdt_provider("other-net", "site-west");
        assert!(
            !is_crdt_provider_routing_eligible("net", &sites, &provider, SignalMode::Gossip),
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
            make_active_grid_site("site-a", "net"),
            make_phase_grid_site("site-b", "net", "Connecting"),
        ];
        let active_provider = make_tool_crdt_provider("site-a");
        let inactive_provider = make_tool_crdt_provider("site-b");
        let providers = vec![active_provider, inactive_provider.clone()];
        let eligible = filter_eligible_remote_crdt_providers("net", &sites, &providers, SignalMode::Gossip);
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
        let excluded_candidates = routing_overlay::remote_crdt_provider_to_candidates(&inactive_provider, &[], "");
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
            make_active_grid_site("site-a", "net"),
            make_phase_grid_site("site-b", "net", "Connecting"),
        ];
        let providers = vec![
            make_eligible_crdt_provider("net", "site-a"), // Active → eligible
            make_eligible_crdt_provider("net", "site-b"), // Connecting → ineligible
            make_eligible_crdt_provider("net", "site-c"), // Missing → ineligible
        ];
        let eligible = filter_eligible_remote_crdt_providers("net", &sites, &providers, SignalMode::Gossip);
        assert_eq!(eligible.len(), 1, "only Active site provider must pass filter");
        let first = eligible.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(first.site_id, "site-a");
    }

    #[test]
    fn filter_is_deterministic() {
        let sites = vec![make_active_grid_site("site-a", "net")];
        let providers = vec![make_eligible_crdt_provider("net", "site-a")];
        let r1 = filter_eligible_remote_crdt_providers("net", &sites, &providers, SignalMode::Gossip);
        let r2 = filter_eligible_remote_crdt_providers("net", &sites, &providers, SignalMode::Gossip);
        assert_eq!(r1.len(), r2.len(), "filter must be deterministic");
    }

    #[test]
    fn crdt_provider_identity_preserved_through_filter() {
        // Remote CRDT providers never carry credential data (ProviderState has no credential field).
        // The filter must not alter provider identity.
        let sites = vec![make_active_grid_site("site-a", "net")];
        let providers = vec![make_eligible_crdt_provider("net", "site-a")];
        let eligible = filter_eligible_remote_crdt_providers("net", &sites, &providers, SignalMode::Gossip);
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
    fn a_hop_cluster_is_bound_to_the_site_its_sni_names() {
        let mut network = base_network();
        let mut gw = make_gw_ref("inference-gw", "praxis-system");
        gw.provider_hop_endpoints = vec![
            serde_json::from_value(json!({
                "cluster": "prov-b", "transport": {"mode": "mutual_tls", "sni": " Site-B.grid.internal "}
            }))
            .unwrap_or_else(|_| std::process::abort()),
            serde_json::from_value(json!({
                "cluster": "elsewhere", "transport": {"mode": "mutual_tls", "sni": "gateway.example.com"}
            }))
            .unwrap_or_else(|_| std::process::abort()),
        ];
        network.spec.consumer_gateways = vec![gw];
        assert_eq!(
            hop_sites(&network),
            site_providers::ClusterSites::from([("prov-b".to_owned(), "site-b".to_owned())]),
            "a hop whose SNI names no grid site binds nothing"
        );
    }

    #[test]
    fn only_local_membership_takes_remote_providers_from_polled_catalogs() {
        assert!(reads_polled_catalogs(crate::cli::Membership::Local));
        assert!(
            !reads_polled_catalogs(crate::cli::Membership::Swim),
            "under Swim gossip carries them, as on main, even with poll signals"
        );
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
    fn discovered_model_expiry_uses_the_earliest_routable_or_excluded_candidate() {
        let mut routable = topology_candidate("cluster-a", "site-a");
        routable.expires_at = Some("2030-01-01T00:00:30Z".to_owned());
        let mut excluded = topology_candidate("cluster-b", "site-b");
        excluded.expires_at = Some("2030-01-01T00:00:10Z".to_owned());
        let overlay = routing_overlay::RoutingOverlay {
            network: "net".to_owned(),
            local_site: "site-a".to_owned(),
            candidates: vec![routable],
            excluded: vec![excluded],
            selection_policy: None,
            generated_at: None,
        };
        let expected =
            time::OffsetDateTime::parse("2030-01-01T00:00:10Z", &time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            next_discovered_model_expiry(&overlay),
            Some(expected),
            "excluded candidates also remain in serving config until their inventory expires"
        );
    }

    #[test]
    fn discovered_model_expiry_retries_before_gateway_cutoff() {
        let now = time::OffsetDateTime::parse("2030-01-01T00:00:00Z", &time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| std::process::abort());
        let normal_retry = Duration::from_secs(300);
        let serving_retry = Some(Duration::from_secs(10));

        assert_eq!(
            soonest_reconcile_retry(normal_retry, serving_retry, Some(now + time::Duration::seconds(5)), now,),
            Duration::from_secs(2) + Duration::from_millis(500),
            "a deadline inside the lead window must retry before the deadline"
        );
        assert_eq!(
            soonest_reconcile_retry(normal_retry, serving_retry, Some(now - time::Duration::seconds(1)), now,),
            Duration::from_secs(1),
            "a deadline reached during reconciliation must still schedule a bounded short retry"
        );
        assert_eq!(
            inventory_expiry_retry(now + time::Duration::seconds(120), now),
            Duration::from_secs(60),
            "a deadline 120 seconds away must requeue no later than 60 seconds"
        );
        let inside_lead = inventory_expiry_retry(now + time::Duration::seconds(20), now);
        assert!(
            inside_lead < Duration::from_secs(20),
            "a deadline inside the lead window must requeue before the deadline"
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
    fn local_wire_providers_serve_this_network_and_only_tool_providers_with_discovered_tools() {
        let providers = [
            make_provider_with_routing_ref("pool-a", "net", "pool-a"),
            make_provider_with_routing_ref("pool-b", "other", "pool-b"),
        ];
        let tools = [
            make_agent_tool_provider_with_discovered("mcp", "net", &[], &["search"]),
            make_agent_tool_provider("unprobed", "net", &["search"]),
        ];
        let served: Vec<String> =
            local_wire_providers("net", &providers, &tools, &served_models::ServedModelStore::new())
                .into_iter()
                .map(|provider| provider.provider_id)
                .collect();
        assert_eq!(
            served,
            ["pool-a", "tool/mcp"],
            "another network's provider and a tool provider with no discovered tools are not served"
        );
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
    /// in the OR-set. They travel in the versioned provider-state extension.
    /// This test verifies the provider is upserted with tools intact and
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

    #[test]
    fn uid_metadata_cannot_overflow_an_otherwise_fitting_inference_snapshot() {
        let local_id = worst_case_swim_identity("site-a");
        let budget = swim::node::state_broadcast_byte_budget(&local_id).unwrap_or_else(|_| std::process::abort());
        let uid = swim::ProviderUid {
            identity: swim::ProviderIdentity {
                network: "net".to_owned(),
                site: "site-a".to_owned(),
                provider: "inference".to_owned(),
            },
            uid: "uid-entire-and-never-truncated".to_owned(),
        };
        let make_broadcast = |model: &str| {
            let mut snapshot = crdt::GridStateSnapshot::new("site-a".to_owned());
            let mut state = provider_state("site-a", "inference");
            state.models = vec![model.to_owned()];
            let mut max_revision = 0;
            upsert_provider_with_capabilities(&mut snapshot, &mut max_revision, state);
            provider_state_broadcast("site-a", "grid", max_revision, snapshot, None)
        };
        let mut model = String::new();
        let mut baseline = None;
        for _ in 0..budget {
            let candidate = make_broadcast(&model);
            let candidate_fits = candidate.encode().is_ok_and(|encoded| encoded.len() <= budget);
            let with_uid_fits = candidate
                .clone()
                .with_provider_uids(vec![uid.clone()])
                .encode()
                .is_ok_and(|encoded| encoded.len() <= budget);
            if candidate_fits && !with_uid_fits {
                baseline = Some(candidate);
                break;
            }
            model.push('x');
        }
        let mut broadcast = baseline
            .unwrap_or_else(|| std::process::abort())
            .with_provider_uids(vec![uid]);

        assert!(
            !broadcast.encode().is_ok_and(|encoded| encoded.len() <= budget),
            "the UID metadata pushes this inference snapshot past the transport budget"
        );
        fit_provider_tools_to_swim_budget(&mut broadcast);

        let encoded = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        assert!(encoded.len() <= budget);
        assert!(broadcast.snapshot.providers.contains_key("net/site-a/inference"));
        assert!(
            broadcast.provider_uids.is_empty(),
            "the complete UID is omitted rather than truncated"
        );
    }
}
