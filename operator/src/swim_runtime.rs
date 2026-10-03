//! Live SWIM membership runtime for the Grid Operator.
//!
//! Starts a `SwimNode` event loop over a UDP socket and exposes a cheap
//! `SwimHandle::snapshot` method so the `GridNetwork` reconcile loop can
//! read the current membership view without blocking.
//!
//! # Lifecycle
//!
//! Call `start` once at operator startup.  It returns an `Arc<SwimHandle>`
//! that is shared across all `GridNetwork` reconciles via `OperatorCtx`.
//! The event loop runs as a background tokio task for the lifetime of the
//! process.
//!
//! # Relationship to `operator::swim`
//!
//! [`operator::swim`] is the pure data layer (`MembershipSnapshot`, phase
//! hints, etc.).  This module is the async I/O layer that produces those
//! snapshots.
//!
//! # Error handling
//!
//! Bind failures are returned as `SwimRuntimeError`.  After startup, all
//! I/O errors (UDP send/recv, foca protocol errors) are logged at `warn` level
//! and do not terminate the loop.
//!
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork
//! [`operator::swim`]: crate::swim
//! [`OperatorCtx`]: crate::controller::grid_network::OperatorCtx

use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use crdt::GridStateSnapshot;
use futures::{Stream, future::BoxFuture, stream};
use swim::{MemberEvent, NodeId, SwimNode, runtime::TimerEvent};
use tokio::{
    net::UdpSocket,
    sync::{
        mpsc::{self, error::TrySendError},
        oneshot, watch,
    },
};

use crate::swim::{MemberRecord, MemberStatus, MembershipSnapshot};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Durably reserved revision range and node generation for one runtime.
///
/// The operator must persist the range's upper bound and node generation
/// before calling [`start`]. A crash can therefore waste revisions, but a
/// replacement process cannot reuse a revision already visible to peers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RevisionLease {
    /// First transport revision available to this process.
    pub first_revision: u64,
    /// Last transport revision available to this process.
    pub last_revision: u64,
    /// First foca identity generation reserved for this process.
    pub first_node_generation: u64,
    /// Last foca identity generation reserved for this process.
    pub last_node_generation: u64,
}

/// Configuration for the SWIM runtime.
#[derive(Clone)]
pub struct SwimConfig {
    /// UDP address to bind for SWIM gossip (e.g. `"0.0.0.0:7946"`).
    pub bind_addr: SocketAddr,

    /// Address advertised to peers in SWIM membership messages.
    ///
    /// When `None`, the runtime advertises the socket's local address after
    /// binding.  Operators that bind a wildcard address such as `0.0.0.0:7946`
    /// should set this to a routable address or DNS-resolved pod address.
    pub advertise_addr: Option<SocketAddr>,

    /// Stable site name advertised to peers (should match `GridSite.metadata.name`).
    pub site_name: String,

    /// Known seed peers to announce to at startup.
    ///
    /// Each address must be reachable and running a compatible SWIM node.
    /// An empty list starts a single-node cluster; other peers must
    /// announce to this node to join.
    pub seeds: Vec<SocketAddr>,

    /// Signals `host:port` advertised to peers, `None` when this site serves none.
    pub signals_address: Option<String>,

    /// Data-plane gateway address to advertise in SWIM state broadcasts.
    ///
    /// When set, this address is included in outbound `StateBroadcast` messages
    /// as the `gateway_address` field, and remote peers will use it for
    /// `GridSite.spec.egress.address` instead of the SWIM UDP endpoint.
    pub gateway_address: Option<String>,

    /// SWIM packet protection at startup, never logged.
    pub key: KeyState,

    /// Revision range and node generation reserved durably before startup.
    pub revision_lease: RevisionLease,

    /// Persists the next revision range as the lease runs low, `None` to stop at its end.
    pub revision_renewer: Option<RevisionRenewer>,
}

/// Persists a revision range past the given last revision before any of it is used, returning its bounds.
pub type RevisionRenewer = Arc<dyn Fn(u64) -> BoxFuture<'static, RenewedRange> + Send + Sync>;

/// Inclusive bounds of a persisted revision range, or why it was not.
pub type RenewedRange = Result<(u64, u64), String>;

/// SWIM packet protection.
#[derive(Clone)]
pub enum KeyState {
    /// A key is required but not loaded: nothing is sent or received.
    Pending,
    /// No key configured: plaintext.
    Plain,
    /// Encrypt and authenticate with this key.
    Key(Arc<swim::crypto::SwimKey>),
}

impl std::fmt::Debug for KeyState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Pending => "Pending",
            Self::Plain => "Plain",
            Self::Key(_) => "Key(<redacted>)",
        })
    }
}

/// The bytes to send for `data` under `key`, `None` when the packet is dropped.
fn wire_payload<'data>(key: &KeyState, data: &'data [u8], addr: SocketAddr) -> Option<std::borrow::Cow<'data, [u8]>> {
    match key {
        KeyState::Key(key) => swim::crypto::encrypt(key, data)
            .inspect_err(|e| tracing::warn!(error = %e, %addr, "SWIM encrypt failed, dropping packet"))
            .ok()
            .map(std::borrow::Cow::Owned),
        KeyState::Plain => Some(std::borrow::Cow::Borrowed(data)),
        KeyState::Pending => {
            tracing::debug!(%addr, "SWIM key pending; holding send");
            None
        },
    }
}

/// The plaintext of an inbound packet under `key`.
fn wire_plaintext<'data>(key: &KeyState, data: &'data [u8]) -> Result<std::borrow::Cow<'data, [u8]>, Rejected> {
    match key {
        KeyState::Key(key) => swim::crypto::decrypt(key, data)
            .map(std::borrow::Cow::Owned)
            .map_err(|_e| Rejected::Unauthenticated),
        KeyState::Plain => Ok(std::borrow::Cow::Borrowed(data)),
        KeyState::Pending => Err(Rejected::Pending),
    }
}

/// Inbound drops while the key is pending, warned once per hold.
#[derive(Default)]
struct PendingDrops {
    /// Whether this hold has warned.
    warned: bool,
}

impl PendingDrops {
    /// Track the hold gauge, rearming the warning once a hold ends.
    fn observe(&mut self, key: &KeyState) {
        let pending = matches!(key, KeyState::Pending);
        crate::metrics::set_swim_key_pending(pending);
        if !pending {
            self.warned = false;
        }
    }

    /// Count one dropped packet.
    fn dropped(&mut self, from: SocketAddr) {
        crate::metrics::record_swim_pending_drop();
        if std::mem::replace(&mut self.warned, true) {
            tracing::debug!(addr = %from, "SWIM key pending; dropped packet");
        } else {
            tracing::warn!(addr = %from, "SWIM key pending; dropping packets until it loads");
        }
    }
}

/// Inbound packets that failed authentication, warned at most once per [`UNAUTHENTICATED_WARN_EVERY`].
#[derive(Default)]
struct UnauthenticatedDrops {
    /// When the last warning was logged.
    warned_at: Option<Instant>,
    /// Drops since that warning.
    suppressed: u64,
}

/// Least time between warnings about unauthenticated packets.
const UNAUTHENTICATED_WARN_EVERY: Duration = Duration::from_secs(60);

impl UnauthenticatedDrops {
    /// Count one dropped packet, returning whether it was warned.
    fn dropped(&mut self, from: SocketAddr, bytes: usize, now: Instant) -> bool {
        let quiet = self
            .warned_at
            .is_some_and(|at| now.saturating_duration_since(at) < UNAUTHENTICATED_WARN_EVERY);
        if quiet {
            self.suppressed = self.suppressed.saturating_add(1);
            tracing::debug!(addr = %from, bytes, "SWIM: dropped packet (authentication failed)");
            return false;
        }
        let suppressed = std::mem::take(&mut self.suppressed);
        self.warned_at = Some(now);
        tracing::warn!(addr = %from, bytes, suppressed, "SWIM: dropped packet (authentication failed)");
        true
    }
}

/// Why an inbound packet was dropped.
#[derive(Debug, PartialEq, Eq)]
enum Rejected {
    /// The key is not loaded yet.
    Pending,
    /// The packet did not authenticate under the key.
    Unauthenticated,
}

impl std::fmt::Debug for SwimConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwimConfig")
            .field("bind_addr", &self.bind_addr)
            .field("advertise_addr", &self.advertise_addr)
            .field("site_name", &self.site_name)
            .field("seeds", &self.seeds)
            .field("gateway_address", &self.gateway_address)
            .field("signals_address", &self.signals_address)
            .field("key", &self.key)
            .field("revision_lease", &self.revision_lease)
            .field("revision_renewer", &self.revision_renewer.is_some())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Internal runtime member tracking
// ---------------------------------------------------------------------------

/// One site's mirrored identities, Alive while any is, reached at the highest live generation.
struct TrackedMember {
    /// Opaque site identity, mirrors [`MemberRecord::site_id`].
    site_id: String,
    /// Each identity by `(generation, address)`: `None` while up, else when it went down.
    identities: BTreeMap<(u64, SocketAddr), Option<Instant>>,
    /// When the site lost its last live identity, `None` while Alive.
    status_changed_at: Option<Instant>,

    /// Data-plane gateway address for this member.
    ///
    /// Populated from the `gateway_addrs` map at snapshot time rather than
    /// from membership events.  Always `None` until
    /// [`members_snapshot`] enriches it.
    gateway_address: Option<String>,

    /// Public site certificate PEM for this member.
    ///
    /// Populated from the `cert_pems` map at snapshot time.
    /// Contains only the public certificate — never a private key.
    site_cert_pem: Option<String>,
}

impl TrackedMember {
    /// A site with no identities yet.
    fn new(site_id: String) -> Self {
        Self {
            site_id,
            identities: BTreeMap::new(),
            status_changed_at: None,
            gateway_address: None,
            site_cert_pem: None,
        }
    }

    /// Whether any identity is up.
    fn is_alive(&self) -> bool {
        self.identities.values().any(Option::is_none)
    }

    /// Alive or Dead, from the identities held.
    fn status(&self) -> MemberStatus {
        if self.is_alive() {
            MemberStatus::Alive
        } else {
            MemberStatus::Dead
        }
    }

    /// The identity peers reach: the highest live generation, else the highest held.
    fn current(&self) -> Option<(u64, SocketAddr)> {
        self.identities
            .iter()
            .rev()
            .find(|(_, down)| down.is_none())
            .or_else(|| self.identities.iter().next_back())
            .map(|(identity, _)| *identity)
    }

    /// Mark `identity` up or down at `now`, `false` when it is new and the site is full.
    fn record(&mut self, identity: (u64, SocketAddr), up: bool, now: Instant) -> bool {
        if !self.identities.contains_key(&identity) && self.identities.len() >= MAX_IDENTITIES_PER_SITE {
            let oldest_down = self
                .identities
                .iter()
                .filter_map(|(held, down)| down.map(|at| (at, *held)))
                .min()
                .map(|(_, held)| held);
            let Some(oldest_down) = oldest_down else {
                return false;
            };
            self.identities.remove(&oldest_down);
        }
        let down = self.identities.entry(identity).or_insert(Some(now));
        if up {
            *down = None;
        } else if down.is_none() {
            *down = Some(now);
        }
        self.refresh_status(now);
        true
    }

    /// Start the Dead clock when the last identity goes down, clear it on any up.
    fn refresh_status(&mut self, now: Instant) {
        if self.is_alive() {
            self.status_changed_at = None;
        } else if self.status_changed_at.is_none() {
            self.status_changed_at = Some(now);
        }
    }

    /// Convert to a public [`MemberRecord`], aging only a Dead site.
    fn to_member_record(&self, now: Instant) -> MemberRecord {
        MemberRecord {
            site_id: self.site_id.clone(),
            endpoint: self.current().map(|(_, addr)| addr.to_string()).unwrap_or_default(),
            incarnation: 0,
            status: self.status(),
            age_secs: self
                .status_changed_at
                .map_or(0, |t| now.saturating_duration_since(t).as_secs()),
            gateway_address: self.gateway_address.clone(),
            site_cert_pem: self.site_cert_pem.clone(),
            signals_address: None,
        }
    }
}

/// Per-site metadata received over gossip, keyed by site, borrowed from the node.
#[derive(Clone, Copy)]
struct PeerMetadata<'meta> {
    /// Data-plane gateway addresses.
    gateway_addrs: &'meta BTreeMap<String, String>,
    /// Public site certificate PEMs.
    cert_pems: &'meta BTreeMap<String, String>,
    /// Signals addresses.
    signals_addrs: &'meta BTreeMap<String, String>,
}

/// No metadata, for a snapshot before any gossip.
static NO_METADATA: BTreeMap<String, String> = BTreeMap::new();

impl Default for PeerMetadata<'static> {
    fn default() -> Self {
        Self {
            gateway_addrs: &NO_METADATA,
            cert_pems: &NO_METADATA,
            signals_addrs: &NO_METADATA,
        }
    }
}

/// Build a [`MembershipSnapshot`] from the tracked members and gossiped metadata, ordered by site.
///
/// `now` is injected so tests can use a fixed [`Instant`].
fn members_snapshot(
    tracked: &HashMap<String, TrackedMember>,
    now: Instant,
    metadata: &PeerMetadata<'_>,
) -> MembershipSnapshot {
    let mut members: Vec<MemberRecord> = tracked
        .values()
        .map(|t| {
            let mut record = t.to_member_record(now);
            record.gateway_address = metadata.gateway_addrs.get(&t.site_id).cloned();
            record.site_cert_pem = metadata.cert_pems.get(&t.site_id).cloned();
            record.signals_address = metadata.signals_addrs.get(&t.site_id).cloned();
            record
        })
        .collect();
    members.sort_by(|left, right| left.site_id.cmp(&right.site_id));
    MembershipSnapshot { members }
}

/// Publish the membership view, waking readers only when it changed.
fn publish_members(
    snapshot_tx: &watch::Sender<MembershipSnapshot>,
    tracked: &HashMap<String, TrackedMember>,
    now: Instant,
    node: &SwimNode,
) {
    let snapshot = node.with_peer_metadata(|gateway_addrs, cert_pems, signals_addrs| {
        let metadata = PeerMetadata {
            gateway_addrs,
            cert_pems,
            signals_addrs,
        };
        members_snapshot(tracked, now, &metadata)
    });
    snapshot_tx.send_if_modified(|current| {
        let changed = *current != snapshot;
        if changed {
            *current = snapshot;
        }
        changed
    });
}

/// Publish the merged grid state, cloning and waking readers only when it changed.
fn publish_state(state_tx: &watch::Sender<GridStateSnapshot>, node: &SwimNode) {
    node.with_state(|state| {
        state_tx.send_if_modified(|current| {
            let changed = current != state;
            if changed {
                current.clone_from(state);
            }
            changed
        })
    });
}

/// Return true when any tracked member needs age recomputation in snapshots.
fn has_aging_members(tracked: &HashMap<String, TrackedMember>) -> bool {
    tracked.values().any(|t| t.status_changed_at.is_some())
}

/// Internal channels owned by the SWIM runtime loop.
struct RuntimeChannels {
    /// Publishes membership snapshots to readers.
    snapshot_tx: watch::Sender<MembershipSnapshot>,

    /// Schedules foca timer callbacks.
    timer_tx: mpsc::Sender<TimerEvent>,

    /// Receives due foca timer callbacks.
    timer_rx: mpsc::Receiver<TimerEvent>,

    /// Receives CRDT state broadcasts to publish over SWIM.
    ///
    /// When a `StateBroadcast` is received here the runtime calls
    /// `SwimNode::publish_state_broadcast` and immediately gossips so that
    /// peers receive the broadcast on the next outbound message.
    broadcast_rx: mpsc::Receiver<swim::StateBroadcast>,

    /// Receives batches of seed addresses to announce at runtime.
    ///
    /// Populated by [`SwimHandle::announce_seeds`].  Each batch is
    /// announced via [`SwimNode::announce`] on the next event loop turn.
    seed_rx: mpsc::Receiver<Vec<SocketAddr>>,

    /// Asks the loop to leave the cluster and stop.
    leave_rx: mpsc::Receiver<()>,
}

/// Period between bounded anti-entropy publications of local provider state.
const STATE_REPUBLISH_INTERVAL: Duration = Duration::from_secs(30);

/// Default TTL for dead SWIM members before eviction from the tracked table.
///
/// 300 seconds = 10 × foca's WAN `suspect_to_down_after` (30 s), giving
/// ample time for the full Alive → Suspect → Dead lifecycle and peer
/// convergence before cleanup.  Override with `GRID_SWIM_DEAD_MEMBER_TTL_SECS`.
const DEFAULT_DEAD_MEMBER_TTL_SECS: u64 = 300;
/// Maximum membership records retained by the operator-side mirror.
const MAX_TRACKED_MEMBERS: usize = 1_024;
/// Maximum dead records retained inside the total member bound.
const MAX_NON_ALIVE_MEMBERS: usize = 512;
/// Identities held per site, enough for a rolling update and its leftovers.
const MAX_IDENTITIES_PER_SITE: usize = 16;

/// Wait before retrying a failed revision renewal.
const RENEWAL_RETRY: Duration = Duration::from_secs(5);

/// Process-local allocator over durably reserved ranges, renewed before the current one runs out.
struct RevisionClock {
    /// Next unused revision in the current range.
    next: u64,
    /// Inclusive upper bound of the current range.
    last: u64,
    /// Persists the next range, `None` to stop at the end of this one.
    renewer: Option<RevisionRenewer>,
    /// The renewal in flight.
    renewal: Option<oneshot::Receiver<RenewedRange>>,
    /// No renewal starts before this, after a failure.
    retry_at: Option<Instant>,
}

impl RevisionClock {
    /// Validate and initialize an allocator over a durable lease.
    fn new(lease: &RevisionLease, renewer: Option<RevisionRenewer>) -> Result<Self, SwimRuntimeError> {
        if lease.first_revision == 0 || lease.first_revision > lease.last_revision {
            return Err(SwimRuntimeError::InvalidRevisionLease {
                first: lease.first_revision,
                last: lease.last_revision,
            });
        }
        Ok(Self {
            next: lease.first_revision,
            last: lease.last_revision,
            renewer,
            renewal: None,
            retry_at: None,
        })
    }

    /// Return the next reserved revision, or `None` while no range has one left.
    fn take(&mut self) -> Option<u64> {
        self.adopt_renewal();
        let revision = (self.next <= self.last).then_some(self.next);
        self.next = self.next.saturating_add(u64::from(revision.is_some()));
        if self.last.saturating_sub(self.next) < swim::state_broadcast::REVISION_LEASE_SPAN / 2 {
            self.renew();
        }
        revision
    }

    /// Switch to a renewed range once it is persisted.
    fn adopt_renewal(&mut self) {
        let Some(renewal) = self.renewal.as_mut() else {
            return;
        };
        let outcome = match renewal.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => return,
            Ok(Ok((first, last))) if first != 0 && first <= last => {
                (self.next, self.last) = (first, last);
                None
            },
            Ok(Ok((first, last))) => Some(format!("invalid range {first}..={last}")),
            Ok(Err(error)) => Some(error),
            Err(oneshot::error::TryRecvError::Closed) => Some("renewal task stopped".to_owned()),
        };
        self.renewal = None;
        if let Some(error) = outcome {
            tracing::warn!(%error, "SWIM revision renewal failed; retrying");
            self.retry_at = Some(Instant::now() + RENEWAL_RETRY);
        }
    }

    /// Start persisting the next range unless one is in flight or a retry is pending.
    fn renew(&mut self) {
        if self.renewal.is_some() || self.retry_at.is_some_and(|at| Instant::now() < at) {
            return;
        }
        let Some(renewer) = &self.renewer else {
            return;
        };
        let (done, renewal) = oneshot::channel();
        let pending = renewer(self.last);
        tokio::spawn(async move { drop(done.send(pending.await)) });
        self.renewal = Some(renewal);
    }
}

/// Retains the latest local provider-state payload for bounded anti-entropy.
///
/// Foca intentionally removes a custom broadcast after its transmission
/// budget is exhausted. A peer that joins later therefore needs a new
/// transport revision even when the underlying provider state is unchanged.
struct RetainedStateBroadcast {
    /// Canonical encoded payload with its transport revision normalized to zero.
    canonical_payload: Vec<u8>,
    /// Latest provider-state payload to republish.
    broadcast: swim::StateBroadcast,
    /// Last origin-local transport revision assigned by this process.
    last_revision: u64,
}

impl RetainedStateBroadcast {
    /// Retain a changed state payload and assign a restart-safe transport revision.
    ///
    /// Returns `None` when the submitted provider state is byte-identical to the
    /// retained state. Metadata-only broadcasts are not accepted by this helper.
    fn update(
        current: Option<Self>,
        mut broadcast: swim::StateBroadcast,
        revision: u64,
    ) -> Result<(Self, Option<swim::StateBroadcast>), String> {
        debug_assert!(
            broadcast.carries_grid_state(),
            "retained anti-entropy accepts provider/capability state only"
        );
        let canonical_payload = canonical_state_payload(&broadcast)?;
        if let Some(retained) = current
            && retained.canonical_payload == canonical_payload
        {
            return Ok((retained, None));
        }

        broadcast.revision = revision;
        let outbound = broadcast.clone();
        Ok((
            Self {
                canonical_payload,
                broadcast,
                last_revision: revision,
            },
            Some(outbound),
        ))
    }

    /// Create a fresh transport revision for periodic anti-entropy.
    fn republish(&mut self, revision: u64) -> swim::StateBroadcast {
        self.last_revision = revision;
        self.broadcast.revision = revision;
        self.broadcast.clone()
    }
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors from starting the SWIM runtime.
#[derive(Debug, thiserror::Error)]
pub enum SwimRuntimeError {
    /// Failed to bind the UDP socket.
    #[error("SWIM runtime failed to bind {addr}: {source}")]
    Bind {
        /// The address that failed.
        addr: SocketAddr,
        /// The underlying IO error.
        #[source]
        source: std::io::Error,
    },

    /// Failed to read the bound socket address.
    #[error("SWIM runtime failed to read local socket address: {source}")]
    LocalAddr {
        /// The underlying IO error.
        #[source]
        source: std::io::Error,
    },

    /// The reserved transport-revision range is empty or invalid.
    #[error("invalid SWIM revision lease: first={first}, last={last}")]
    InvalidRevisionLease {
        /// First reserved revision.
        first: u64,
        /// Last reserved revision.
        last: u64,
    },

    /// The reserved node-generation range is empty or invalid.
    #[error("invalid SWIM node-generation lease: first={first}, last={last}")]
    InvalidNodeGenerationLease {
        /// First reserved generation.
        first: u64,
        /// Last reserved generation.
        last: u64,
    },
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// Error returned when a CRDT state broadcast cannot be queued.
#[derive(Debug, thiserror::Error)]
pub enum BroadcastError {
    /// The runtime broadcast queue is full.
    #[error("SWIM runtime broadcast channel full")]
    ChannelFull,

    /// The runtime task has exited and the channel is closed.
    #[error("SWIM runtime broadcast channel closed")]
    ChannelClosed,
}

/// Error returned when [`SwimHandle::set_swim_key`] cannot apply the key.
#[derive(Debug, thiserror::Error)]
pub enum SetKeyError {
    /// The SWIM runtime has exited and the key channel is closed.
    #[error("SWIM runtime has exited; key not applied")]
    RuntimeGone,
}

/// Error returned when [`SwimHandle::set_gateway_address`] fails.
#[derive(Debug, thiserror::Error)]
pub enum SetGatewayError {
    /// The SWIM runtime has exited and the gateway channel is closed.
    #[error("SWIM runtime has exited; gateway address not applied")]
    RuntimeGone,
}

/// Error returned when seed addresses cannot be queued for announcement.
#[derive(Debug, thiserror::Error)]
pub enum SeedAnnounceError {
    /// The runtime seed queue is full; the caller should retry later.
    #[error("SWIM runtime seed channel full")]
    ChannelFull,

    /// The runtime task has exited and the channel is closed.
    #[error("SWIM runtime seed channel closed")]
    ChannelClosed,
}

/// A handle to the live SWIM runtime.
///
/// Returned by `start`; shared across all `GridNetwork` reconciles via
/// `OperatorCtx`.  Produces snapshots on each call to [`SwimHandle::snapshot`]
/// and [`SwimHandle::state_snapshot`] by cloning the most recent watch value
/// without blocking.
pub struct SwimHandle {
    /// Stable local site identity advertised by this SWIM runtime.
    site_name: String,

    /// Address this runtime advertises to SWIM peers.
    ///
    /// Callers may use this to filter the local address from `spec.seeds`
    /// before calling [`SwimHandle::announce_seeds`].
    advertise_addr: SocketAddr,

    /// Signals endpoint gossiped to peers, if this site serves one.
    signals_address: Option<String>,

    /// Watch channel receiver for SWIM membership snapshots.
    snapshot_rx: watch::Receiver<MembershipSnapshot>,

    /// Watch channel receiver for the merged CRDT grid-state snapshot.
    ///
    /// Updated whenever a peer delivers a `swim::StateBroadcast` over SWIM
    /// custom broadcasts.
    state_rx: watch::Receiver<GridStateSnapshot>,

    /// Channel for sending CRDT state broadcasts to the runtime loop.
    broadcast_tx: mpsc::Sender<swim::StateBroadcast>,

    /// Channel for queuing seed addresses to announce at runtime.
    seed_tx: mpsc::Sender<Vec<SocketAddr>>,

    /// SWIM packet protection, a key never logged.
    key_tx: watch::Sender<KeyState>,

    /// Watch sender for updating the data-plane gateway address at runtime.
    ///
    /// Used by the background discovery poller to push a newly-discovered
    /// address to the SWIM run loop.  The run loop detects changes and
    /// publishes a fresh gateway-address broadcast to peers.
    gateway_tx: watch::Sender<Option<String>>,

    /// Runtime liveness published by the task monitor.
    runtime_tx: watch::Sender<bool>,

    /// Asks the runtime to leave the cluster and stop.
    leave_tx: mpsc::Sender<()>,
}

impl SwimHandle {
    /// Return the signals endpoint gossiped to peers, if any.
    #[must_use]
    pub fn signals_address(&self) -> Option<&str> {
        self.signals_address.as_deref()
    }

    /// Return the local site identity advertised to SWIM peers.
    #[must_use]
    pub fn site_name(&self) -> &str {
        &self.site_name
    }

    /// Return the address this runtime advertises to SWIM peers.
    ///
    /// Use this to filter the local address from `spec.seeds` before
    /// calling [`SwimHandle::announce_seeds`] — announcing to self is harmless but
    /// generates unnecessary noise.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.advertise_addr
    }

    /// Return the current data-plane gateway address, if any.
    #[must_use]
    pub fn gateway_address(&self) -> Option<String> {
        self.gateway_tx.borrow().clone()
    }

    /// Return whether the SWIM runtime task is still running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        *self.runtime_tx.borrow()
    }

    /// Update the data-plane gateway address at runtime.
    ///
    /// The SWIM run loop detects the change and publishes a fresh
    /// gateway-address broadcast to peers.
    ///
    /// [`SwimHandle::gateway_address()`] reflects the last value accepted by
    /// this channel, so provider-state broadcasts do not regress to the startup
    /// address after discovery updates.
    ///
    /// # Errors
    ///
    /// Returns [`SetGatewayError::RuntimeGone`] if the SWIM runtime has exited.
    pub fn set_gateway_address(&self, addr: Option<String>) -> Result<(), SetGatewayError> {
        self.gateway_tx.send(addr).map_err(|_e| SetGatewayError::RuntimeGone)
    }

    /// Queue seed addresses for announcement to the SWIM runtime.
    ///
    /// Each address in `seeds` is announced as a new SWIM peer on the next
    /// event loop turn via [`SwimNode::announce`].  Announcing to a peer that
    /// is already a live member is idempotent — foca ignores redundant joins.
    ///
    /// An empty `seeds` slice is a no-op and always returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns [`SeedAnnounceError::ChannelFull`] if the bounded runtime queue
    /// is full (capacity 16 batches), or [`SeedAnnounceError::ChannelClosed`]
    /// if the runtime task has exited.
    pub fn announce_seeds(&self, seeds: Vec<SocketAddr>) -> Result<(), SeedAnnounceError> {
        if seeds.is_empty() {
            return Ok(());
        }
        self.seed_tx.try_send(seeds).map_err(|e| match e {
            TrySendError::Full(_) => SeedAnnounceError::ChannelFull,
            TrySendError::Closed(_) => SeedAnnounceError::ChannelClosed,
        })
    }

    /// Clone the most recently published [`MembershipSnapshot`].
    ///
    /// Returns the snapshot without blocking. If the runtime task has stopped,
    /// every retained peer is reported as [`MemberStatus::Dead`] so callers
    /// cannot mistake the last received view for live membership.
    pub fn snapshot(&self) -> MembershipSnapshot {
        let mut snapshot = self.snapshot_rx.borrow().clone();
        if !self.is_running() {
            for member in &mut snapshot.members {
                member.status = MemberStatus::Dead;
            }
        }
        snapshot
    }

    /// Clone the most recently merged CRDT [`GridStateSnapshot`].
    ///
    /// Updated by the `swim::StateBroadcastHandler` as peers deliver state
    /// broadcasts over SWIM gossip.  Returns the last-known value without
    /// blocking; callers should tolerate a brief lag after startup while the
    /// first broadcasts arrive.
    pub fn state_snapshot(&self) -> GridStateSnapshot {
        self.state_rx.borrow().clone()
    }

    /// Return a stream that emits when SWIM state relevant to a
    /// [`GridNetwork`] reconciliation changes.
    ///
    /// Repeated gossip packets commonly republish identical watch values. The
    /// returned stream compares a semantic view of membership, gateway and
    /// certificate metadata, and distributed provider state so those duplicate
    /// packets do not cause Kubernetes API churn. Suspect/dead member age is
    /// retained because stale-candidate TTL processing depends on it.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub fn reconciliation_events(&self) -> impl Stream<Item = ()> + Send + Sync + use<> {
        let membership_rx = self.snapshot_rx.clone();
        let state_rx = self.state_rx.clone();
        let runtime_rx = self.runtime_tx.subscribe();
        let previous = reconciliation_view(&membership_rx.borrow(), &state_rx.borrow(), *runtime_rx.borrow());
        stream::unfold(
            (membership_rx, state_rx, runtime_rx, previous),
            |(mut membership_rx, mut state_rx, mut runtime_rx, mut previous)| async move {
                loop {
                    tokio::select! {
                        result = membership_rx.changed() => result.ok()?,
                        result = state_rx.changed() => result.ok()?,
                        result = runtime_rx.changed() => result.ok()?,
                    }
                    let current =
                        reconciliation_view(&membership_rx.borrow(), &state_rx.borrow(), *runtime_rx.borrow());
                    if current != previous {
                        previous = current;
                        return Some(((), (membership_rx, state_rx, runtime_rx, previous)));
                    }
                }
            },
        )
    }

    /// Queue a CRDT state broadcast for delivery to SWIM peers.
    ///
    /// The runtime task encodes the broadcast and calls
    /// `SwimNode::publish_state_broadcast` followed immediately by a gossip
    /// round so peers receive the data on the next outbound message.
    ///
    /// # Errors
    ///
    /// Returns [`BroadcastError::ChannelFull`] if the bounded runtime queue is
    /// currently full, or [`BroadcastError::ChannelClosed`] if the runtime task
    /// has exited.
    pub fn publish_state_broadcast(&self, broadcast: swim::StateBroadcast) -> Result<(), BroadcastError> {
        self.broadcast_tx.try_send(broadcast).map_err(|e| match e {
            TrySendError::Full(_) => BroadcastError::ChannelFull,
            TrySendError::Closed(_) => BroadcastError::ChannelClosed,
        })
    }

    /// Configure the AES-256-GCM encryption key for SWIM traffic.
    ///
    /// After this call, the runtime encrypts every outgoing SWIM packet with the
    /// given key and drops any incoming packet that does not authenticate.
    ///
    /// This method is idempotent: if the same key bytes are sent again, the
    /// runtime receives the same value and behavior does not change.
    ///
    /// # Key change warning
    ///
    /// Changing to a different key while the cluster is live technically takes
    /// effect immediately, but it is not a safe rotation mechanism: there is no
    /// keyring, so peers that still hold the old key can no longer communicate.
    /// Use a coordinated restart or simultaneous replacement across sites.
    ///
    /// # Security invariant
    ///
    /// The key value is never logged, traced, or exposed in status fields.
    ///
    /// # Errors
    ///
    /// Returns [`SetKeyError::RuntimeGone`] if the SWIM runtime has exited.
    pub fn set_swim_key(&self, key: swim::crypto::SwimKey) -> Result<(), SetKeyError> {
        self.key_tx
            .send(KeyState::Key(Arc::new(key)))
            .map_err(|_e| SetKeyError::RuntimeGone)
    }

    /// Leave the cluster so peers drop this site at once, waiting up to `wait` for the runtime to stop.
    pub async fn leave(&self, wait: Duration) {
        let mut running = self.runtime_tx.subscribe();
        if self.leave_tx.try_send(()).is_err() {
            return;
        }
        drop(tokio::time::timeout(wait, running.wait_for(|running| !running)).await);
    }

    /// Release a pending hold to plaintext, never a loaded key, returning whether it was pending.
    pub fn release_plain(&self) -> bool {
        self.key_tx.send_if_modified(|state| {
            let pending = matches!(state, KeyState::Pending);
            if pending {
                *state = KeyState::Plain;
            }
            pending
        })
    }

    /// Whether SWIM is held for a key not yet decided.
    #[must_use]
    pub fn is_key_pending(&self) -> bool {
        matches!(*self.key_tx.borrow(), KeyState::Pending)
    }

    /// Whether gossip is encrypted.
    #[must_use]
    pub fn is_encrypted(&self) -> bool {
        matches!(*self.key_tx.borrow(), KeyState::Key(_))
    }
}

/// Stable SWIM data that can change rendered Grid resources.
#[derive(Debug, PartialEq)]
struct ReconciliationView {
    /// Whether the runtime task is still serving membership updates.
    runtime_alive: bool,
    /// Membership and peer metadata, sorted by site identity.
    members: Vec<ReconciliationMember>,
    /// Distributed provider and capability state.
    state: GridStateSnapshot,
}

/// Membership fields consumed by Grid reconciliation.
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ReconciliationMember {
    /// Peer site identity.
    site_id: String,
    /// Peer SWIM endpoint.
    endpoint: String,
    /// Peer incarnation.
    incarnation: u64,
    /// Peer lifecycle state.
    status: u8,
    /// Suspect/dead age used by stale-candidate policy.
    non_alive_age_secs: Option<u64>,
    /// Peer data-plane gateway address.
    gateway_address: Option<String>,
    /// Peer public certificate.
    site_cert_pem: Option<String>,
    /// Peer signals address.
    signals_address: Option<String>,
}

/// Build the deduplicated view used by [`SwimHandle::reconciliation_events`].
fn reconciliation_view(
    membership: &MembershipSnapshot,
    state: &GridStateSnapshot,
    runtime_alive: bool,
) -> ReconciliationView {
    let mut members: Vec<ReconciliationMember> = membership
        .members
        .iter()
        .map(|member| ReconciliationMember {
            site_id: member.site_id.clone(),
            endpoint: member.endpoint.clone(),
            incarnation: member.incarnation,
            status: match member.status {
                MemberStatus::Alive => 0,
                MemberStatus::Suspect => 1,
                MemberStatus::Dead => 2,
            },
            non_alive_age_secs: (member.status != MemberStatus::Alive).then_some(member.age_secs / 5),
            gateway_address: member.gateway_address.clone(),
            site_cert_pem: member.site_cert_pem.clone(),
            signals_address: member.signals_address.clone(),
        })
        .collect();
    members.sort();

    ReconciliationView {
        runtime_alive,
        members,
        state: state.clone(),
    }
}

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

/// Start the SWIM membership runtime and return a handle.
///
/// Binds a UDP socket on `config.bind_addr`, starts the foca event loop as a
/// background task, and announces to all configured seed peers.
///
/// The returned [`Arc<SwimHandle>`] provides cheap snapshot reads.  Dropping
/// it does **not** stop the background task; the task runs for the lifetime
/// of the process.
///
/// # Errors
///
/// Returns [`SwimRuntimeError::Bind`] if the socket cannot be bound.
#[expect(
    clippy::too_many_lines,
    reason = "channel setup, socket bind, runtime spawn — linear startup sequence"
)]
pub async fn start(config: SwimConfig) -> Result<Arc<SwimHandle>, SwimRuntimeError> {
    let revisions = RevisionClock::new(&config.revision_lease, config.revision_renewer.clone())?;
    if config.revision_lease.first_node_generation == 0
        || config.revision_lease.first_node_generation > config.revision_lease.last_node_generation
    {
        return Err(SwimRuntimeError::InvalidNodeGenerationLease {
            first: config.revision_lease.first_node_generation,
            last: config.revision_lease.last_node_generation,
        });
    }
    let socket = UdpSocket::bind(config.bind_addr)
        .await
        .map_err(|source| SwimRuntimeError::Bind {
            addr: config.bind_addr,
            source,
        })?;
    let local_addr = socket
        .local_addr()
        .map_err(|source| SwimRuntimeError::LocalAddr { source })?;
    let advertise_addr = config.advertise_addr.unwrap_or(local_addr);

    let site_name = config.site_name.clone();
    let gateway_address = config.gateway_address.clone();
    let (snapshot_tx, snapshot_rx) = watch::channel(MembershipSnapshot::default());
    let (state_tx, state_rx) = watch::channel(GridStateSnapshot::new(site_name.clone()));
    let (timer_tx, timer_rx) = mpsc::channel::<TimerEvent>(256);
    let (broadcast_tx, broadcast_rx) = mpsc::channel::<swim::StateBroadcast>(32);
    let (seed_tx, seed_rx) = mpsc::channel::<Vec<SocketAddr>>(16);
    let (leave_tx, leave_rx) = mpsc::channel::<()>(1);
    let (key_tx, key_rx) = watch::channel(config.key.clone());
    let (gateway_tx, gateway_loop_rx) = watch::channel(gateway_address);
    let (runtime_tx, _) = watch::channel(true);
    let channels = RuntimeChannels {
        snapshot_tx,
        timer_tx,
        timer_rx,
        broadcast_rx,
        seed_rx,
        leave_rx,
    };

    tracing::info!(
        bind_addr = %config.bind_addr,
        advertise_addr = %advertise_addr,
        site_name = %config.site_name,
        seeds = config.seeds.len(),
        key = ?config.key,
        "SWIM runtime starting"
    );

    let signals_address = config.signals_address.clone();
    let run_loop_handle = tokio::spawn(run_loop(
        Arc::new(socket),
        config,
        advertise_addr,
        channels,
        state_tx,
        key_rx,
        gateway_loop_rx,
        revisions,
    ));
    let runtime_monitor_tx = runtime_tx.clone();
    tokio::spawn(async move {
        match run_loop_handle.await {
            Ok(()) => tracing::info!("SWIM runtime exited"),
            Err(e) => tracing::error!(error = %e, "SWIM runtime panicked"),
        }
        runtime_monitor_tx.send_replace(false);
    });

    Ok(Arc::new(SwimHandle {
        site_name,
        advertise_addr,
        signals_address,
        snapshot_rx,
        state_rx,
        broadcast_tx,
        seed_tx,
        key_tx,
        gateway_tx,
        runtime_tx,
        leave_tx,
    }))
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

/// `seeds` without this node's own addresses, so a site never announces to itself.
///
/// A seed list shared by every site, or a load balancer in front of this node,
/// can name it; foca then refuses the reply as data from itself.
fn foreign_seeds(mut seeds: Vec<SocketAddr>, own: &[Option<SocketAddr>]) -> Vec<SocketAddr> {
    let before = seeds.len();
    seeds.retain(|seed| !own.contains(&Some(*seed)));
    if seeds.len() < before {
        tracing::debug!(
            dropped = before - seeds.len(),
            "ignoring SWIM seeds that name this node"
        );
    }
    seeds
}

/// Drive the SWIM node until the process exits.
///
/// All errors are logged and the loop continues.
#[expect(
    clippy::too_many_lines,
    reason = "sequential startup steps (seed announces) + select! event loop; splitting would obscure the data flow"
)]
#[expect(
    clippy::cognitive_complexity,
    reason = "select! with four arms plus nested drain_output; extracting arms would hide the I/O ownership pattern"
)]
#[expect(clippy::large_stack_frames, reason = "async future over UDP socket + foca node")]
#[expect(
    clippy::too_many_arguments,
    reason = "runtime channels and state are passed explicitly to preserve single-task ownership"
)]
async fn run_loop(
    socket: Arc<UdpSocket>,
    config: SwimConfig,
    advertise_addr: SocketAddr,
    mut channels: RuntimeChannels,
    state_tx: watch::Sender<GridStateSnapshot>,
    mut key_rx: watch::Receiver<KeyState>,
    mut gateway_loop_rx: watch::Receiver<Option<String>>,
    mut revisions: RevisionClock,
) {
    let site_name = config.site_name.clone();
    let identity = NodeId::with_generation_range(
        site_name.clone(),
        advertise_addr,
        config.revision_lease.first_node_generation,
        config.revision_lease.last_node_generation,
    );
    let mut node = SwimNode::with_origin_capacity(identity, MAX_TRACKED_MEMBERS);
    let mut tracked: HashMap<String, TrackedMember> = HashMap::new();
    let mut buf = vec![0_u8; 65_536];
    let mut age_tick = tokio::time::interval(Duration::from_secs(1));
    let mut pending_drops = PendingDrops::default();
    let mut unauthenticated_drops = UnauthenticatedDrops::default();
    let own = [Some(advertise_addr), socket.local_addr().ok()];
    let mut seed_addrs = foreign_seeds(config.seeds.clone(), &own);
    let mut next_seed_announce_at = Instant::now() + Duration::from_secs(5);
    let mut gateway_address = config.gateway_address.clone();
    let Some(mut gateway_address_revision) = revisions.take() else {
        tracing::error!("SWIM revision lease exhausted during startup");
        return;
    };
    let mut next_gateway_republish_at = Instant::now();
    let mut retained_state_broadcast: Option<RetainedStateBroadcast> = None;
    let mut next_state_republish_at = Instant::now() + STATE_REPUBLISH_INTERVAL;
    let dead_member_ttl = Duration::from_secs(
        std::env::var("GRID_SWIM_DEAD_MEMBER_TTL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_DEAD_MEMBER_TTL_SECS),
    );

    publish_address_broadcast(
        &mut node,
        &site_name,
        gateway_address_revision,
        gateway_address.as_deref(),
        config.signals_address.as_deref(),
    );

    // Announce to seed peers.  Errors are logged inside SwimNode::announce.
    let startup_key = key_rx.borrow_and_update().clone();
    for &seed_addr in &seed_addrs {
        let seed_id = NodeId::seed(seed_addr);
        let output = node.announce(seed_id);
        drain_output(
            output,
            &socket,
            &channels.timer_tx,
            &mut tracked,
            &channels.snapshot_tx,
            &node,
            &startup_key,
        )
        .await;
    }

    loop {
        let key = key_rx.borrow_and_update().clone();
        pending_drops.observe(&key);

        tokio::select! {
            result = socket.recv_from(&mut buf) => {
                match result {
                    Ok((n, from)) => {
                        let raw = buf.get(..n).unwrap_or(&[]);
                        let data = match wire_plaintext(&key, raw) {
                            Ok(data) => data,
                            Err(Rejected::Pending) => {
                                pending_drops.dropped(from);
                                continue;
                            },
                            Err(Rejected::Unauthenticated) => {
                                unauthenticated_drops.dropped(from, n, Instant::now());
                                continue;
                            },
                        };

                        let output = node.handle_data(&data);
                        tracing::trace!(from = %from, bytes = n, "SWIM UDP received");
                        drain_output(
                            output,
                            &socket,
                            &channels.timer_tx,
                            &mut tracked,
                            &channels.snapshot_tx,
                            &node,
                            &key,
                        )
                        .await;
                        // Publish updated CRDT state after every incoming UDP packet.
                        // Broadcasts are received inside handle_data, so the snapshot
                        // may have advanced.
                        publish_state(&state_tx, &node);
                        // Gateway-address-only broadcasts update the node's gateway-address
                        // map but may not emit a membership event.  Republish the
                        // membership snapshot after every inbound packet so callers see the
                        // latest gateway address attached to already-known members.
                        publish_members(&channels.snapshot_tx, &tracked, Instant::now(), &node);
                        let now = Instant::now();
                        let advertises = gateway_address.is_some() || config.signals_address.is_some();
                        if advertises && now >= next_gateway_republish_at {
                            let Some(revision) = revisions.take() else {
                                tracing::warn!("SWIM revisions exhausted; address republish waits for renewal");
                                next_gateway_republish_at = now + Duration::from_secs(1);
                                continue;
                            };
                            gateway_address_revision = revision;
                            publish_address_broadcast(
                                &mut node,
                                &site_name,
                                gateway_address_revision,
                                gateway_address.as_deref(),
                                config.signals_address.as_deref(),
                            );
                            let gossip_output = node.gossip();
                            drain_output(
                                gossip_output,
                                &socket,
                                &channels.timer_tx,
                                &mut tracked,
                                &channels.snapshot_tx,
                                &node,
                                &key,
                            )
                            .await;
                            publish_state(&state_tx, &node);
                            next_gateway_republish_at = now + Duration::from_secs(1);
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "SWIM UDP recv error"),
                }
            }
            Some(event) = channels.timer_rx.recv() => {
                let output = node.handle_timer(event);
                drain_output(
                    output,
                    &socket,
                    &channels.timer_tx,
                    &mut tracked,
                    &channels.snapshot_tx,
                    &node,
                    &key,
                )
                .await;
            }
            Some(mut bc) = channels.broadcast_rx.recv() => {
                if bc.carries_grid_state() {
                    let Some(revision) = revisions.take() else {
                        tracing::warn!("SWIM revisions exhausted; state publishes on repair after renewal");
                        if let Ok((retained, _)) = RetainedStateBroadcast::update(retained_state_broadcast.take(), bc, 0) {
                            retained_state_broadcast = Some(retained);
                        }
                        next_state_republish_at = Instant::now();
                        continue;
                    };
                    match RetainedStateBroadcast::update(retained_state_broadcast.take(), bc, revision) {
                        Ok((retained, Some(outbound))) => {
                            retained_state_broadcast = Some(retained);
                            bc = outbound;
                        }
                        Ok((retained, None)) => {
                            retained_state_broadcast = Some(retained);
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to encode state broadcast");
                            continue;
                        }
                    }
                }
                // Publish through foca's dedicated custom-broadcast path so
                // every currently available candidate receives the update.
                if let Err(e) = node.publish_state_broadcast(&bc) {
                    tracing::warn!(error = %e, "failed to encode state broadcast");
                } else {
                    let broadcast_out = node.broadcast();
                    drain_output(
                        broadcast_out,
                        &socket,
                        &channels.timer_tx,
                        &mut tracked,
                        &channels.snapshot_tx,
                        &node,
                        &key,
                    )
                    .await;
                }
                publish_state(&state_tx, &node);
            }
            Some(()) = channels.leave_rx.recv() => {
                let output = node.leave();
                drain_output(
                    output,
                    &socket,
                    &channels.timer_tx,
                    &mut tracked,
                    &channels.snapshot_tx,
                    &node,
                    &key,
                )
                .await;
                tracing::info!("SWIM left the cluster");
                return;
            }
            Some(seeds) = channels.seed_rx.recv() => {
                // Announce to CRD-declared seed peers at runtime.
                // Re-announcing to existing members is idempotent (foca ignores them).
                seed_addrs = foreign_seeds(seeds, &own);
                for &addr in &seed_addrs {
                    let seed_id = NodeId::seed(addr);
                    let output = node.announce(seed_id);
                    drain_output(
                        output,
                        &socket,
                        &channels.timer_tx,
                        &mut tracked,
                        &channels.snapshot_tx,
                        &node,
                        &key,
                    )
                    .await;
                }
            }
            _ = age_tick.tick() => {
                let now = Instant::now();
                if now >= next_state_republish_at {
                    let revision = retained_state_broadcast.as_ref().and_then(|_| revisions.take());
                    if let (Some(retained), Some(revision)) = (retained_state_broadcast.as_mut(), revision) {
                        let bc = retained.republish(revision);
                        if let Err(e) = node.publish_state_broadcast(&bc) {
                            tracing::warn!(error = %e, "failed to encode retained state broadcast");
                        } else {
                            let output = node.broadcast();
                            drain_output(
                                output,
                                &socket,
                                &channels.timer_tx,
                                &mut tracked,
                                &channels.snapshot_tx,
                                &node,
                                &key,
                            )
                            .await;
                        }
                    }
                    // Without a revision the repair retries on the next tick.
                    if retained_state_broadcast.is_none() || revision.is_some() {
                        next_state_republish_at = now + STATE_REPUBLISH_INTERVAL;
                    }
                }
                if now >= next_seed_announce_at {
                    // Seed discovery is retried so serial startup, transient
                    // packet loss, and a later MemberDown do not leave a site
                    // permanently isolated. Announcing an active member is
                    // idempotent in foca.
                    for &addr in &seed_addrs {
                        let output = node.announce(NodeId::seed(addr));
                        drain_output(
                            output,
                            &socket,
                            &channels.timer_tx,
                            &mut tracked,
                            &channels.snapshot_tx,
                            &node,
                            &key,
                        )
                        .await;
                    }
                    next_seed_announce_at = now + Duration::from_secs(5);
                }
                let evicted = prune_tracked_members(&mut tracked, now, dead_member_ttl);
                let changed = !evicted.is_empty();
                let live: Vec<&NodeId> = node.live_identities().collect();
                let gone = restore_from_foca(evicted, &mut tracked, &live, now);
                let adopted = adopt_live_identities(&mut tracked, &live, now);
                for origin in &gone {
                    node.evict_origin(origin);
                }
                // Republish while age changes or after eviction so readers see
                // a coherent bounded membership view.
                if has_aging_members(&tracked) || changed || adopted {
                    publish_members(&channels.snapshot_tx, &tracked, now, &node);
                }
            }
            Ok(()) = gateway_loop_rx.changed() => {
                let previous = gateway_address.clone();
                gateway_address.clone_from(&gateway_loop_rx.borrow_and_update());
                if let Some(addr) = gateway_address.as_deref() {
                    let Some(revision) = revisions.take() else {
                        tracing::warn!("SWIM revisions exhausted; the address change waits for renewal");
                        continue;
                    };
                    gateway_address_revision = revision;
                    publish_address_broadcast(
                        &mut node,
                        &site_name,
                        gateway_address_revision,
                        Some(addr),
                        config.signals_address.as_deref(),
                    );
                    let gossip_out = node.gossip();
                    drain_output(
                        gossip_out,
                        &socket,
                        &channels.timer_tx,
                        &mut tracked,
                        &channels.snapshot_tx,
                        &node,
                        &key,
                    )
                    .await;
                    publish_state(&state_tx, &node);
                    // Discovery re-announces an unchanged address for late joiners; only a change is news.
                    if previous.as_deref() == Some(addr) {
                        tracing::debug!(addr = %addr, "gateway address re-announced");
                    } else {
                        tracing::info!(addr = %addr, "gateway address updated at runtime");
                    }
                }
            }
        }
    }
}

/// Encode provider-state content without its replaceable transport revision.
fn canonical_state_payload(broadcast: &swim::StateBroadcast) -> Result<Vec<u8>, String> {
    let mut canonical = broadcast.clone();
    canonical.revision = 0;
    canonical.encode().map_err(|error| error.to_string())
}

/// Queue a broadcast of this site's gateway and signals addresses, without `grid_id`.
fn publish_address_broadcast(
    node: &mut SwimNode,
    site_name: &str,
    revision: u64,
    gateway_address: Option<&str>,
    signals_address: Option<&str>,
) {
    if gateway_address.is_none() && signals_address.is_none() {
        return;
    }
    let broadcast = swim::StateBroadcast::new(
        site_name.to_owned(),
        revision,
        GridStateSnapshot::new(site_name.to_owned()),
        gateway_address.map(str::to_owned),
    )
    .with_signals_address(signals_address.map(str::to_owned));
    if let Err(e) = node.publish_state_broadcast(&broadcast) {
        tracing::warn!(error = %e, "failed to encode address broadcast");
    }
}

/// Send outbound messages, schedule timers, apply membership events.
///
/// Uses [`Instant::now()`] for age tracking so Dead transitions record
/// an accurate wall-clock start time.  The same `now` value is used for all
/// events processed in a single call, ensuring consistency within one gossip round.
///
/// Each packet is encrypted, sent plain, or held per [`KeyState`].
#[expect(
    clippy::too_many_arguments,
    reason = "distinct runtime state pointers; a wrapper struct would obscure the data-flow"
)]
async fn drain_output(
    output: swim::AccumulatedOutput,
    socket: &UdpSocket,
    timer_tx: &mpsc::Sender<TimerEvent>,
    tracked: &mut HashMap<String, TrackedMember>,
    snapshot_tx: &watch::Sender<MembershipSnapshot>,
    node: &SwimNode,
    key: &KeyState,
) {
    for msg in output.messages {
        let Some(payload) = wire_payload(key, &msg.data, msg.addr) else {
            continue;
        };
        if let Err(e) = socket.send_to(&payload, msg.addr).await {
            tracing::warn!(error = %e, addr = %msg.addr, "SWIM UDP send error");
        }
    }

    for scheduled in output.timers {
        let tx = timer_tx.clone();
        let event = scheduled.event;
        let delay = scheduled.delay;
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            drop(tx.send(event).await);
        });
    }

    let mut changed = false;
    // Capture a single `now` for all events in this batch so age is consistent.
    let now = Instant::now();
    for event in output.events {
        apply_member_event_bounded(event, tracked, now);
        changed = true;
    }
    if changed {
        publish_members(snapshot_tx, tracked, now, node);
    }
}

/// Apply a membership event to the tracked table, bounded by [`MAX_TRACKED_MEMBERS`].
fn apply_member_event_bounded(event: MemberEvent, tracked: &mut HashMap<String, TrackedMember>, now: Instant) {
    let (site_name, ..) = event_identity(&event);
    if !tracked.contains_key(site_name) && tracked.len() >= MAX_TRACKED_MEMBERS {
        tracing::warn!(
            max_members = MAX_TRACKED_MEMBERS,
            "SWIM tracked-member capacity reached; ignoring event for an unknown member"
        );
        return;
    }
    apply_member_event(event, tracked, now);
}

/// Mark the named identity up or down, ignoring a down for an identity a known site never reported.
fn apply_member_event(event: MemberEvent, tracked: &mut HashMap<String, TrackedMember>, now: Instant) {
    let (up, site, addr, generation) = into_identity(event);
    if is_from_the_future(generation, unix_nanos_now()) {
        tracing::warn!(site, %addr, generation, "ignoring a SWIM identity with a generation from the future");
        return;
    }
    let outcome = if let Some(member) = tracked.get_mut(&site) {
        if !up && !member.identities.contains_key(&(generation, addr)) {
            "ignored an untracked down"
        } else if member.record((generation, addr), up, now) {
            "applied"
        } else {
            "ignored a join past the identity bound"
        }
    } else {
        let mut member = TrackedMember::new(site.clone());
        member.record((generation, addr), up, now);
        tracked.insert(site.clone(), member);
        "applied"
    };
    tracing::info!(site, %addr, generation, up, outcome, "SWIM membership event");
}

/// Whether the event is a join, and the site, address, and generation it names.
fn into_identity(event: MemberEvent) -> (bool, String, SocketAddr, u64) {
    match event {
        MemberEvent::Joined {
            site_name,
            addr,
            generation,
        } => (true, site_name, addr, generation),
        MemberEvent::Left {
            site_name,
            addr,
            generation,
        } => (false, site_name, addr, generation),
    }
}

/// The site, address, and generation an event names.
fn event_identity(event: &MemberEvent) -> (&str, SocketAddr, u64) {
    match event {
        MemberEvent::Joined {
            site_name,
            addr,
            generation,
        }
        | MemberEvent::Left {
            site_name,
            addr,
            generation,
        } => (site_name, *addr, *generation),
    }
}

/// Whether `generation` lies past `now_nanos` by more than [`swim::identity::MAX_LEASE_SKEW`].
fn is_from_the_future(generation: u64, now_nanos: u64) -> bool {
    swim::identity::is_future_generation(generation, now_nanos)
}

/// Wall-clock nanoseconds, the unit generations are reserved in.
fn unix_nanos_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| u64::try_from(since.as_nanos()).unwrap_or(u64::MAX))
}

/// Drop identities down past `dead_ttl` and excess Dead sites, returning the sites emptied.
fn prune_tracked_members(
    tracked: &mut HashMap<String, TrackedMember>,
    now: Instant,
    dead_ttl: Duration,
) -> Vec<String> {
    let mut emptied: Vec<(Instant, String)> = tracked
        .iter_mut()
        .filter_map(|(site, member)| {
            member
                .identities
                .retain(|_, down| down.is_none_or(|at| now.saturating_duration_since(at) < dead_ttl));
            member
                .identities
                .is_empty()
                .then(|| (member.status_changed_at.unwrap_or(now), site.clone()))
        })
        .collect();

    let mut dead: Vec<(Instant, String)> = tracked
        .iter()
        .filter(|(_, member)| !member.identities.is_empty() && !member.is_alive())
        .map(|(site, member)| (member.status_changed_at.unwrap_or(now), site.clone()))
        .collect();
    let excess = dead.len().saturating_sub(MAX_NON_ALIVE_MEMBERS);
    if excess > 0 {
        dead.sort();
        emptied.extend(dead.into_iter().take(excess));
    }

    emptied.sort();
    let evicted: Vec<String> = emptied.into_iter().map(|(_, site)| site).collect();
    for site in &evicted {
        tracked.remove(site);
    }
    evicted
}

/// Rebuild each evicted site foca still holds live, returning the ones to evict for good.
fn restore_from_foca(
    evicted: Vec<String>,
    tracked: &mut HashMap<String, TrackedMember>,
    live: &[&NodeId],
    now: Instant,
) -> Vec<String> {
    let now_nanos = unix_nanos_now();
    evicted
        .into_iter()
        .filter(|site| {
            let mut member = TrackedMember::new(site.clone());
            for id in live.iter().filter(|id| id.site_name() == site) {
                if !is_from_the_future(id.generation(), now_nanos) {
                    member.record((id.generation(), id.socket_addr()), true, now);
                }
            }
            if member.identities.is_empty() {
                return true;
            }
            tracing::info!(site = %site, "SWIM mirror lost a site foca still holds live; rebuilt it");
            tracked.insert(site.clone(), member);
            false
        })
        .collect()
}

/// Mirror each foca-live identity the mirror lacks, such as a join refused while from the future.
fn adopt_live_identities(tracked: &mut HashMap<String, TrackedMember>, live: &[&NodeId], now: Instant) -> bool {
    let now_nanos = unix_nanos_now();
    let mut adopted = false;
    for id in live {
        let identity = (id.generation(), id.socket_addr());
        let known = tracked.get(id.site_name());
        if is_from_the_future(identity.0, now_nanos)
            || known.is_some_and(|member| member.identities.contains_key(&identity))
            || (known.is_none() && tracked.len() >= MAX_TRACKED_MEMBERS)
        {
            continue;
        }
        let member = tracked
            .entry(id.site_name().to_owned())
            .or_insert_with(|| TrackedMember::new(id.site_name().to_owned()));
        if member.record(identity, true, now) {
            tracing::info!(site = %id.site_name(), generation = identity.0, "SWIM mirror adopted a live identity it missed");
            adopted = true;
        }
    }
    adopted
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::expect_used, reason = "tests")]
mod tests {
    use futures::StreamExt as _;

    use super::*;

    #[test]
    fn a_pending_key_holds_both_directions() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 7946));
        let key = KeyState::Key(Arc::new([7_u8; 32]));
        let sealed = wire_payload(&key, b"ping", addr).expect("encrypts").into_owned();
        assert!(wire_payload(&KeyState::Pending, b"ping", addr).is_none(), "send held");
        assert_eq!(
            wire_plaintext(&KeyState::Pending, &sealed).map(|_| ()),
            Err(Rejected::Pending),
            "receive dropped"
        );
        assert_eq!(wire_plaintext(&key, &sealed).expect("opens").as_ref(), b"ping");
        assert_eq!(
            wire_plaintext(&key, b"ping").map(|_| ()),
            Err(Rejected::Unauthenticated)
        );
        assert_eq!(
            wire_plaintext(&KeyState::Plain, b"ping").expect("plain").as_ref(),
            b"ping"
        );
    }

    #[tokio::test]
    async fn a_pending_hold_releases_to_plain_but_never_drops_a_key() {
        let handle = start(SwimConfig {
            key: KeyState::Pending,
            ..test_config(110_000)
        })
        .await
        .expect("start");
        assert!(handle.release_plain(), "pending released");
        assert!(!handle.release_plain(), "already plain");
        handle.set_swim_key([7_u8; 32]).expect("runtime alive");
        assert!(!handle.release_plain(), "a loaded key stays");
        assert!(handle.is_encrypted());
    }

    #[tokio::test]
    async fn a_pending_hold_stays_until_the_key_loads() {
        let handle = start(SwimConfig {
            key: KeyState::Pending,
            ..test_config(120_000)
        })
        .await
        .expect("start");
        assert!(!handle.is_encrypted(), "held, not encrypted");
        handle.set_swim_key([7_u8; 32]).expect("runtime alive");
        assert!(handle.is_encrypted());
    }

    // -----------------------------------------------------------------------
    // apply_member_event
    // -----------------------------------------------------------------------

    const POD_ADDR: &str = "10.0.0.1:7946";

    fn joined(site_name: &str) -> MemberEvent {
        joined_at(site_name, POD_ADDR, 1)
    }

    fn left(site_name: &str) -> MemberEvent {
        left_at(site_name, POD_ADDR, 1)
    }

    fn joined_at(site_name: &str, addr: &str, generation: u64) -> MemberEvent {
        MemberEvent::Joined {
            site_name: site_name.to_owned(),
            addr: addr.parse().expect("addr"),
            generation,
        }
    }

    fn left_at(site_name: &str, addr: &str, generation: u64) -> MemberEvent {
        MemberEvent::Left {
            site_name: site_name.to_owned(),
            addr: addr.parse().expect("addr"),
            generation,
        }
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "case table")]
    fn a_stale_identity_never_downs_the_current_one() {
        let lb = "203.0.113.7:7946";
        let status = |events: Vec<MemberEvent>| {
            let mut tracked = HashMap::new();
            for event in events {
                apply_member_event(event, &mut tracked, now());
            }
            let record = tracked.get("site-a").expect("tracked").to_member_record(now());
            (record.status, record.endpoint)
        };
        let alive_at_lb = (MemberStatus::Alive, lb.to_owned());
        let cases = [
            (
                "old pod identity leaves after the LB one joins",
                vec![
                    joined_at("site-a", POD_ADDR, 1),
                    joined_at("site-a", lb, 2),
                    left_at("site-a", POD_ADDR, 1),
                ],
                alive_at_lb.clone(),
            ),
            (
                "a late join for the old identity is ignored",
                vec![joined_at("site-a", lb, 2), joined_at("site-a", POD_ADDR, 1)],
                alive_at_lb.clone(),
            ),
            (
                "a leave for another address is ignored",
                vec![joined_at("site-a", lb, 2), left_at("site-a", POD_ADDR, 3)],
                alive_at_lb,
            ),
            (
                "the last identity leaving downs the site",
                vec![
                    joined_at("site-a", POD_ADDR, 1),
                    joined_at("site-a", lb, 2),
                    left_at("site-a", POD_ADDR, 1),
                    left_at("site-a", lb, 2),
                ],
                (MemberStatus::Dead, lb.to_owned()),
            ),
            (
                "a rolling update whose new pod dies keeps the old one",
                vec![
                    joined_at("site-a", POD_ADDR, 1),
                    joined_at("site-a", lb, 2),
                    left_at("site-a", lb, 2),
                ],
                (MemberStatus::Alive, POD_ADDR.to_owned()),
            ),
            (
                "a forged identity going silent leaves the real one",
                vec![
                    joined_at("site-a", POD_ADDR, 5),
                    joined_at("site-a", lb, 9),
                    left_at("site-a", lb, 9),
                ],
                (MemberStatus::Alive, POD_ADDR.to_owned()),
            ),
            (
                "a forged generation from the future is ignored",
                vec![joined_at("site-a", POD_ADDR, 1), joined_at("site-a", lb, u64::MAX)],
                (MemberStatus::Alive, POD_ADDR.to_owned()),
            ),
        ];
        for (label, events, want) in cases {
            assert_eq!(status(events), want, "{label}");
        }
    }

    async fn reserve_local_addr() -> SocketAddr {
        let socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|_| std::process::abort());
        let addr = socket.local_addr().unwrap_or_else(|_| std::process::abort());
        drop(socket);
        addr
    }

    async fn wait_until_member_alive(handle: &SwimHandle, site_id: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let found = handle
                .snapshot()
                .members
                .iter()
                .any(|m| m.site_id == site_id && m.status == MemberStatus::Alive);
            if found {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "member {site_id} must become Alive through seed announcement"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn now() -> Instant {
        Instant::now()
    }

    /// A loopback, seedless, plaintext config.
    fn test_config(seed: u64) -> SwimConfig {
        SwimConfig {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            advertise_addr: None,
            site_name: "test-node".to_owned(),
            seeds: Vec::new(),
            signals_address: None,
            gateway_address: None,
            key: KeyState::Plain,
            revision_lease: test_revision_lease(seed),
            revision_renewer: None,
        }
    }

    fn test_revision_lease(seed: u64) -> RevisionLease {
        RevisionLease {
            first_revision: seed,
            last_revision: seed + 10_000,
            first_node_generation: seed,
            last_node_generation: seed + 100,
        }
    }

    #[test]
    fn joined_event_inserts_alive_member() {
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, now());
        assert!(tracked.contains_key("site-a"), "member must be inserted");
        assert_eq!(
            tracked.get("site-a").unwrap_or_else(|| std::process::abort()).status(),
            MemberStatus::Alive,
            "joined member must be Alive"
        );
    }

    #[test]
    fn left_event_marks_member_dead() {
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, now());
        apply_member_event(left("site-a"), &mut tracked, now());
        assert_eq!(
            tracked.get("site-a").unwrap_or_else(|| std::process::abort()).status(),
            MemberStatus::Dead,
            "member must be marked Dead after Left event"
        );
    }

    #[test]
    fn left_event_for_unknown_member_inserts_dead_tombstone() {
        let mut tracked = HashMap::new();
        apply_member_event(left("site-a"), &mut tracked, now());
        assert_eq!(
            tracked.get("site-a").unwrap_or_else(|| std::process::abort()).status(),
            MemberStatus::Dead,
            "unknown Left event must preserve a Dead tombstone"
        );
    }

    #[test]
    fn multiple_joins_produce_correct_connected_count() {
        let t = now();
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t);
        apply_member_event(joined("site-b"), &mut tracked, t);
        let snap = members_snapshot(&tracked, t, &PeerMetadata::default());
        assert_eq!(snap.connected_count(), 2, "two Alive members must give count=2");
    }

    #[test]
    fn has_aging_members_false_for_empty_and_alive_members() {
        let t = now();
        let mut tracked = HashMap::new();
        assert!(
            !has_aging_members(&tracked),
            "empty table must not require age republish"
        );
        apply_member_event(joined("site-a"), &mut tracked, t);
        assert!(
            !has_aging_members(&tracked),
            "Alive members must not require age republish"
        );
    }

    #[test]
    fn an_unchanged_view_wakes_no_reader() {
        let node = SwimNode::new(NodeId::with_generation(
            "local".to_owned(),
            POD_ADDR.parse().expect("addr"),
            1,
        ));
        let (snapshot_tx, mut snapshot_rx) = watch::channel(MembershipSnapshot::default());
        let (state_tx, mut state_rx) = watch::channel(GridStateSnapshot::new("local".to_owned()));
        let t0 = now();
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-b"), &mut tracked, t0);
        apply_member_event(joined("site-a"), &mut tracked, t0);

        publish_members(&snapshot_tx, &tracked, t0, &node);
        assert!(snapshot_rx.has_changed().expect("open"), "a new member wakes");
        let sites: Vec<String> = snapshot_rx
            .borrow_and_update()
            .members
            .iter()
            .map(|m| m.site_id.clone())
            .collect();
        assert_eq!(sites, ["site-a", "site-b"], "ordered by site");
        publish_members(&snapshot_tx, &tracked, t0, &node);
        assert!(!snapshot_rx.has_changed().expect("open"), "the same view stays quiet");

        state_rx.mark_unchanged();
        publish_state(&state_tx, &node);
        assert!(!state_rx.has_changed().expect("open"), "the same state stays quiet");
    }

    #[test]
    fn has_aging_members_true_for_dead_members() {
        let mut dead_tracked = HashMap::new();
        apply_member_event(left("site-b"), &mut dead_tracked, now());
        assert!(
            has_aging_members(&dead_tracked),
            "Dead member must require age republish"
        );
    }

    #[test]
    fn a_generation_past_the_skew_is_from_the_future() {
        let now_nanos = 1_000_000_000_000_000_000;
        let skew = u64::try_from(swim::identity::MAX_LEASE_SKEW.as_nanos()).expect("fits");
        let cases = [
            ("now", now_nanos, false),
            ("inside the skew", now_nanos + skew, false),
            ("past the skew", now_nanos + skew + 1, true),
            ("forged maximum", u64::MAX, true),
        ];
        for (label, generation, want) in cases {
            assert_eq!(is_from_the_future(generation, now_nanos), want, "{label}");
        }
        assert!(
            !is_from_the_future(unix_nanos_now(), unix_nanos_now()),
            "the live clock"
        );
    }

    #[test]
    fn repeated_left_preserves_the_original_timestamp() {
        let t0 = now();
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t0);
        apply_member_event(left("site-a"), &mut tracked, t0 + Duration::from_secs(10));
        apply_member_event(left("site-a"), &mut tracked, t0 + Duration::from_secs(50));
        let snap = members_snapshot(&tracked, t0 + Duration::from_secs(70), &PeerMetadata::default());
        let m = snap.members.first().expect("member");
        assert_eq!(m.age_secs, 60, "a repeated leave must not reset the age clock");
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "fills a site, then frees one slot")]
    fn identities_per_site_are_bounded() {
        let t0 = now();
        let mut tracked = HashMap::new();
        for index in 0..MAX_IDENTITIES_PER_SITE {
            let generation = u64::try_from(index).expect("small");
            apply_member_event(joined_at("site-a", POD_ADDR, generation), &mut tracked, t0);
        }
        apply_member_event(joined_at("site-a", POD_ADDR, 1_000), &mut tracked, t0);
        let member = tracked.get("site-a").expect("tracked");
        assert_eq!(
            member.identities.len(),
            MAX_IDENTITIES_PER_SITE,
            "full of live identities"
        );
        assert!(
            !member
                .identities
                .contains_key(&(1_000, POD_ADDR.parse().expect("addr")))
        );

        apply_member_event(left_at("site-a", POD_ADDR, 0), &mut tracked, t0);
        apply_member_event(joined_at("site-a", POD_ADDR, 1_000), &mut tracked, t0);
        let made_room = tracked.get("site-a").expect("tracked");
        assert_eq!(
            made_room.identities.len(),
            MAX_IDENTITIES_PER_SITE,
            "a down identity made room"
        );
        assert!(
            made_room
                .identities
                .contains_key(&(1_000, POD_ADDR.parse().expect("addr")))
        );
    }

    #[test]
    fn pruning_expires_each_identity_and_keeps_a_live_site() {
        let ttl = Duration::from_secs(300);
        let t0 = now();
        let lb = "203.0.113.7:7946";
        let mut tracked = HashMap::new();
        apply_member_event(joined_at("site-a", POD_ADDR, 1), &mut tracked, t0);
        apply_member_event(joined_at("site-a", lb, 2), &mut tracked, t0);
        apply_member_event(left_at("site-a", POD_ADDR, 1), &mut tracked, t0);
        assert!(
            prune_tracked_members(&mut tracked, t0 + ttl, ttl).is_empty(),
            "the site stays"
        );
        let member = tracked.get("site-a").expect("tracked");
        assert_eq!(member.identities.len(), 1, "only the dead identity expired");
        assert_eq!(member.to_member_record(t0).endpoint, lb);
    }

    #[test]
    fn an_emptied_site_foca_still_holds_is_rebuilt_not_evicted() {
        let t0 = now();
        let addr: SocketAddr = POD_ADDR.parse().expect("addr");
        let live_a = NodeId::with_generation("site-a".to_owned(), addr, 7);
        let forged = NodeId::with_generation("site-a".to_owned(), "10.0.0.9:7946".parse().expect("addr"), u64::MAX);
        let other = NodeId::with_generation("site-c".to_owned(), addr, 1);
        let mut tracked = HashMap::new();
        let gone = restore_from_foca(
            vec!["site-a".to_owned(), "site-b".to_owned()],
            &mut tracked,
            &[&live_a, &forged, &other],
            t0,
        );
        assert_eq!(gone, vec!["site-b".to_owned()], "only the site foca no longer holds");
        let record = tracked.get("site-a").expect("rebuilt").to_member_record(t0);
        assert_eq!(
            (record.status, record.endpoint),
            (MemberStatus::Alive, POD_ADDR.to_owned())
        );
        assert_eq!(
            tracked.get("site-a").expect("rebuilt").identities.len(),
            1,
            "forged skipped"
        );
    }

    #[test]
    fn a_join_refused_from_the_future_is_adopted_once_it_passes_the_cap() {
        let t0 = now();
        let addr: SocketAddr = POD_ADDR.parse().expect("addr");
        let soon = unix_nanos_now() + u64::try_from(swim::identity::MAX_LEASE_SKEW.as_nanos()).expect("fits") * 2;
        let mut tracked = HashMap::new();
        apply_member_event(joined_at("site-a", POD_ADDR, soon), &mut tracked, t0);
        assert!(tracked.is_empty(), "refused while from the future");
        let ahead = NodeId::with_generation("site-a".to_owned(), addr, soon);
        assert!(!adopt_live_identities(&mut tracked, &[&ahead], t0), "still ahead");

        let caught_up = NodeId::with_generation("site-a".to_owned(), addr, unix_nanos_now());
        let forged = NodeId::with_generation("site-b".to_owned(), addr, u64::MAX);
        assert!(
            adopt_live_identities(&mut tracked, &[&caught_up, &forged], t0),
            "adopted"
        );
        let record = tracked.get("site-a").expect("adopted").to_member_record(t0);
        assert_eq!(
            (record.status, record.endpoint),
            (MemberStatus::Alive, POD_ADDR.to_owned())
        );
        assert!(!tracked.contains_key("site-b"), "a forged identity stays out");
        assert!(
            !adopt_live_identities(&mut tracked, &[&caught_up], t0),
            "already mirrored"
        );
    }

    #[test]
    fn unauthenticated_drops_warn_once_per_window() {
        let t0 = now();
        let from = SocketAddr::from(([10, 0, 0, 1], 7946));
        let mut drops = UnauthenticatedDrops::default();
        assert!(drops.dropped(from, 10, t0), "first drop warns");
        assert!(!drops.dropped(from, 10, t0 + Duration::from_secs(1)), "then quiet");
        assert!(!drops.dropped(from, 10, t0 + Duration::from_secs(59)));
        assert!(drops.dropped(from, 10, t0 + UNAUTHENTICATED_WARN_EVERY), "warns again");
        assert_eq!(drops.suppressed, 0, "the count resets on a warning");
    }

    // -----------------------------------------------------------------------
    // SWIM age tracking with synthetic instants
    // -----------------------------------------------------------------------

    #[test]
    fn joined_member_has_zero_age() {
        let t = now();
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t);
        let snap = members_snapshot(&tracked, t, &PeerMetadata::default());
        let m = snap.members.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(m.age_secs, 0, "Alive member must have age_secs=0");
    }

    #[test]
    fn dead_event_starts_age_clock() {
        let t0 = now();
        let t_dead = t0 + Duration::from_secs(20);
        let t_snap = t0 + Duration::from_secs(80);
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t0);
        apply_member_event(left("site-a"), &mut tracked, t_dead);
        let snap = members_snapshot(&tracked, t_snap, &PeerMetadata::default());
        let m = snap.members.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(m.status, MemberStatus::Dead);
        assert_eq!(m.age_secs, 60, "dead age must be 80s - 20s = 60s");
    }

    #[test]
    fn alive_after_dead_resets_age_to_zero() {
        let t0 = now();
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t0);
        apply_member_event(left("site-a"), &mut tracked, t0 + Duration::from_secs(10));
        // Rejoin clears status_changed_at → age=0.
        apply_member_event(joined("site-a"), &mut tracked, t0 + Duration::from_secs(50));
        let snap = members_snapshot(&tracked, t0 + Duration::from_secs(70), &PeerMetadata::default());
        let m = snap.members.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(m.status, MemberStatus::Alive);
        assert_eq!(m.age_secs, 0, "rejoined Alive member must have age=0");
    }

    #[test]
    fn unknown_left_creates_dead_tombstone_with_age() {
        let t0 = now();
        let t_dead = t0 + Duration::from_secs(15);
        let t_snap = t0 + Duration::from_secs(75);
        let mut tracked = HashMap::new();
        apply_member_event(left("unknown-site"), &mut tracked, t_dead);
        let snap = members_snapshot(&tracked, t_snap, &PeerMetadata::default());
        let m = snap.members.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(m.status, MemberStatus::Dead);
        assert_eq!(m.age_secs, 60, "unknown Left tombstone age must be 75s - 15s = 60s");
    }

    // -----------------------------------------------------------------------
    // SwimHandle
    // -----------------------------------------------------------------------

    fn make_test_handle() -> (
        SwimHandle,
        watch::Sender<MembershipSnapshot>,
        watch::Sender<GridStateSnapshot>,
    ) {
        let (snapshot_tx, snapshot_rx) = watch::channel(MembershipSnapshot::default());
        let (state_tx, state_rx) = watch::channel(GridStateSnapshot::new("test".to_owned()));
        let (broadcast_tx, _broadcast_rx) = mpsc::channel(1);
        let (seed_tx, _seed_rx) = mpsc::channel(16);
        let (key_tx, _key_rx) = watch::channel(KeyState::Plain);
        let (gateway_tx, _gateway_rx) = watch::channel(None);
        let handle = SwimHandle {
            site_name: "test".to_owned(),
            advertise_addr: "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
            signals_address: None,
            snapshot_rx,
            state_rx,
            broadcast_tx,
            seed_tx,
            key_tx,
            gateway_tx,
            runtime_tx: watch::channel(true).0,
            leave_tx: mpsc::channel(1).0,
        };
        (handle, snapshot_tx, state_tx)
    }

    fn reconciliation_member(status: MemberStatus, age_secs: u64) -> MemberRecord {
        MemberRecord {
            site_id: "site-x".to_owned(),
            endpoint: "10.0.0.1:7946".to_owned(),
            incarnation: 1,
            status,
            age_secs,
            gateway_address: Some("10.0.0.1:8443".to_owned()),
            site_cert_pem: Some("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----".to_owned()),
            signals_address: None,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one ordered test proves deduplication across membership, health, and provider-state changes"
    )]
    async fn reconciliation_events_emit_only_for_semantic_changes() {
        let (handle, snapshot_tx, state_tx) = make_test_handle();
        let mut events = Box::pin(handle.reconciliation_events());

        drop(snapshot_tx.send(MembershipSnapshot::default()));
        assert!(
            tokio::time::timeout(Duration::from_millis(25), events.next())
                .await
                .is_err(),
            "an identical membership snapshot must not trigger reconciliation"
        );

        let alive = MembershipSnapshot {
            members: vec![reconciliation_member(MemberStatus::Alive, 0)],
        };
        drop(snapshot_tx.send(alive.clone()));
        assert!(
            tokio::time::timeout(Duration::from_secs(1), events.next())
                .await
                .is_ok_and(|event| event.is_some()),
            "a new member must trigger reconciliation"
        );

        let alive_with_new_age = MembershipSnapshot {
            members: vec![reconciliation_member(MemberStatus::Alive, 30)],
        };
        drop(snapshot_tx.send(alive_with_new_age));
        assert!(
            tokio::time::timeout(Duration::from_millis(25), events.next())
                .await
                .is_err(),
            "age updates for a healthy member must not trigger reconciliation"
        );

        let suspect = MembershipSnapshot {
            members: vec![reconciliation_member(MemberStatus::Suspect, 1)],
        };
        drop(snapshot_tx.send(suspect));
        assert!(
            tokio::time::timeout(Duration::from_secs(1), events.next())
                .await
                .is_ok_and(|event| event.is_some()),
            "a health-state change must trigger reconciliation"
        );

        let suspect_same_age_bucket = MembershipSnapshot {
            members: vec![reconciliation_member(MemberStatus::Suspect, 4)],
        };
        drop(snapshot_tx.send(suspect_same_age_bucket));
        assert!(
            tokio::time::timeout(Duration::from_millis(25), events.next())
                .await
                .is_err(),
            "suspect age changes inside one five-second bucket must be deduplicated"
        );

        let suspect_next_age_bucket = MembershipSnapshot {
            members: vec![reconciliation_member(MemberStatus::Suspect, 5)],
        };
        drop(snapshot_tx.send(suspect_next_age_bucket));
        assert!(
            tokio::time::timeout(Duration::from_secs(1), events.next())
                .await
                .is_ok_and(|event| event.is_some()),
            "crossing a suspect age bucket must trigger stale-policy reconciliation"
        );

        let mut provider_state = GridStateSnapshot::new("test".to_owned());
        provider_state.add_capability(crdt::Capability::Model("model-x".to_owned()));
        provider_state.upsert_provider(crdt::ProviderState {
            network_id: "net".to_owned(),
            site_id: "site-x".to_owned(),
            provider_id: "provider-x".to_owned(),
            routing_cluster: "site-x".to_owned(),
            models: vec!["model-x".to_owned()],
            backend_kind: "local".to_owned(),
            capacity_weight: 1,
            phase: crdt::ProviderPhase::Available,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            access_policy: crdt::ProviderAccessPolicy::default(),
            revision: 1,
            writer_id: "site-x".to_owned(),
        });
        drop(state_tx.send(provider_state.clone()));
        assert!(
            tokio::time::timeout(Duration::from_secs(1), events.next())
                .await
                .is_ok_and(|event| event.is_some()),
            "a distributed provider-state change must trigger reconciliation"
        );

        drop(state_tx.send(provider_state));
        assert!(
            tokio::time::timeout(Duration::from_millis(25), events.next())
                .await
                .is_err(),
            "identical provider gossip must not trigger reconciliation"
        );

        handle.runtime_tx.send_replace(false);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), events.next())
                .await
                .is_ok_and(|event| event.is_some()),
            "runtime termination must trigger reconciliation"
        );
    }

    #[test]
    fn stopped_runtime_never_reports_retained_members_alive() {
        let (handle, snapshot_tx, _state_tx) = make_test_handle();
        drop(snapshot_tx.send(MembershipSnapshot {
            members: vec![reconciliation_member(MemberStatus::Alive, 0)],
        }));
        handle.runtime_tx.send_replace(false);

        let snapshot = handle.snapshot();
        assert!(!handle.is_running());
        assert_eq!(
            snapshot.members.first().unwrap_or_else(|| std::process::abort()).status,
            MemberStatus::Dead
        );
        assert_eq!(snapshot.connected_count(), 0);
    }

    #[test]
    fn handle_exposes_gateway_address() {
        let (snapshot_tx, snapshot_rx) = watch::channel(MembershipSnapshot::default());
        let (state_tx, state_rx) = watch::channel(GridStateSnapshot::new("test".to_owned()));
        let (broadcast_tx, _broadcast_rx) = mpsc::channel(1);
        let (seed_tx, _seed_rx) = mpsc::channel(16);
        let (key_tx, _key_rx) = watch::channel(KeyState::Plain);
        let (gateway_tx, _gateway_rx) = watch::channel(Some("127.0.0.1:19080".to_owned()));
        let handle = SwimHandle {
            site_name: "test".to_owned(),
            advertise_addr: "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
            signals_address: None,
            snapshot_rx,
            state_rx,
            broadcast_tx,
            seed_tx,
            key_tx,
            gateway_tx,
            runtime_tx: watch::channel(true).0,
            leave_tx: mpsc::channel(1).0,
        };
        drop((snapshot_tx, state_tx));

        assert_eq!(
            handle.gateway_address(),
            Some("127.0.0.1:19080".to_owned()),
            "handle must expose configured gateway address for state broadcasts"
        );
    }

    #[test]
    fn set_gateway_address_sends_to_channel() {
        let (_snapshot_tx, snapshot_rx) = watch::channel(MembershipSnapshot::default());
        let (_state_tx, state_rx) = watch::channel(GridStateSnapshot::new("test".to_owned()));
        let (broadcast_tx, _broadcast_rx) = mpsc::channel(1);
        let (seed_tx, _seed_rx) = mpsc::channel(16);
        let (key_tx, _key_rx) = watch::channel(KeyState::Plain);
        let (gateway_tx, _gateway_rx) = watch::channel(None);
        let handle = SwimHandle {
            site_name: "test".to_owned(),
            advertise_addr: "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
            signals_address: None,
            snapshot_rx,
            state_rx,
            broadcast_tx,
            seed_tx,
            key_tx,
            gateway_tx,
            runtime_tx: watch::channel(true).0,
            leave_tx: mpsc::channel(1).0,
        };

        let result = handle.set_gateway_address(Some("10.0.0.5:8080".to_owned()));
        assert!(result.is_ok(), "set_gateway_address must succeed when channel is open");
        assert_eq!(
            handle.gateway_address(),
            Some("10.0.0.5:8080".to_owned()),
            "handle must expose runtime-updated address via watch"
        );
    }

    #[test]
    fn set_gateway_address_returns_error_without_runtime_receiver() {
        let (_snapshot_tx, snapshot_rx) = watch::channel(MembershipSnapshot::default());
        let (_state_tx, state_rx) = watch::channel(GridStateSnapshot::new("test".to_owned()));
        let (broadcast_tx, _broadcast_rx) = mpsc::channel(1);
        let (seed_tx, _seed_rx) = mpsc::channel(16);
        let (key_tx, _key_rx) = watch::channel(KeyState::Plain);
        let (gateway_tx, gateway_rx) = watch::channel(None);
        let handle = SwimHandle {
            site_name: "test".to_owned(),
            advertise_addr: "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
            signals_address: None,
            snapshot_rx,
            state_rx,
            broadcast_tx,
            seed_tx,
            key_tx,
            gateway_tx,
            runtime_tx: watch::channel(false).0,
            leave_tx: mpsc::channel(1).0,
        };
        drop(gateway_rx);

        let result = handle.set_gateway_address(Some("10.0.0.5:8080".to_owned()));
        assert!(
            matches!(result, Err(SetGatewayError::RuntimeGone)),
            "set_gateway_address must report a stopped runtime"
        );
        assert_eq!(
            handle.gateway_address(),
            None,
            "failed update must not change the sender's retained value"
        );
    }

    #[test]
    fn revision_clock_uses_reserved_range_once() {
        let lease = RevisionLease {
            first_revision: 41,
            last_revision: 42,
            first_node_generation: 7,
            last_node_generation: 8,
        };
        let mut clock = RevisionClock::new(&lease, None).unwrap_or_else(|_| std::process::abort());
        assert_eq!(clock.take(), Some(41));
        assert_eq!(clock.take(), Some(42));
        assert_eq!(clock.take(), None);
        assert_eq!(clock.take(), None);
    }

    #[tokio::test]
    async fn revision_clock_renews_before_the_range_runs_out() {
        let span = swim::state_broadcast::REVISION_LEASE_SPAN;
        let asked = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = Arc::clone(&asked);
        let renewer: RevisionRenewer = Arc::new(move |last| {
            seen.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(last);
            Box::pin(async move { Ok((last + 1_000, last + 1_000 + span - 1)) })
        });
        let lease = RevisionLease {
            first_revision: 1,
            last_revision: span,
            first_node_generation: 7,
            last_node_generation: 8,
        };
        let mut clock = RevisionClock::new(&lease, Some(renewer)).unwrap_or_else(|_| std::process::abort());
        let mut taken = Vec::new();
        for _ in 0..span {
            taken.extend(clock.take());
            tokio::task::yield_now().await;
        }
        assert_eq!(
            asked.lock().unwrap_or_else(std::sync::PoisonError::into_inner).first(),
            Some(&span),
            "renewed past the persisted end before running out"
        );
        assert!(taken.is_sorted_by(|a, b| a < b), "strictly increasing");
        assert!(taken.contains(&(span + 1_000)), "moved to the renewed range");
    }

    #[tokio::test]
    async fn revision_clock_waits_out_a_failed_renewal() {
        let renewer: RevisionRenewer = Arc::new(|_| Box::pin(async { Err("apiserver down".to_owned()) }));
        let lease = RevisionLease {
            first_revision: 5,
            last_revision: 5,
            first_node_generation: 7,
            last_node_generation: 8,
        };
        let mut clock = RevisionClock::new(&lease, Some(renewer)).unwrap_or_else(|_| std::process::abort());
        assert_eq!(clock.take(), Some(5));
        tokio::task::yield_now().await;
        assert_eq!(clock.take(), None, "exhausted, not reused");
        assert!(clock.retry_at.is_some(), "retries later");
    }

    #[test]
    fn revision_clock_rejects_invalid_range() {
        let lease = RevisionLease {
            first_revision: 42,
            last_revision: 41,
            first_node_generation: 7,
            last_node_generation: 8,
        };
        assert!(
            matches!(
                RevisionClock::new(&lease, None),
                Err(SwimRuntimeError::InvalidRevisionLease { .. })
            ),
            "runtime must reject an empty reserved range"
        );
    }

    fn provider_broadcast(queue_depth: f64) -> swim::StateBroadcast {
        let mut snapshot = GridStateSnapshot::new("site-a".to_owned());
        snapshot.add_capability(crdt::Capability::Model("model-x".to_owned()));
        snapshot.upsert_provider(crdt::ProviderState {
            network_id: "net".to_owned(),
            site_id: "site-a".to_owned(),
            provider_id: "provider-a".to_owned(),
            routing_cluster: "site-a".to_owned(),
            models: vec!["model-x".to_owned()],
            backend_kind: "local".to_owned(),
            capacity_weight: 1,
            phase: crdt::ProviderPhase::Available,
            metrics: crdt::ProviderMetricsSnapshot {
                queue_depth: Some(queue_depth),
                ..crdt::ProviderMetricsSnapshot::default()
            },
            access_policy: crdt::ProviderAccessPolicy::default(),
            revision: 7,
            writer_id: "site-a".to_owned(),
        });
        swim::StateBroadcast::new("site-a".to_owned(), 7, snapshot, Some("10.0.0.1:8443".to_owned()))
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "ordered state-machine proof covers initial, duplicate, changed, and repair publications"
    )]
    fn retained_state_deduplicates_updates_and_republishes_with_new_revision() {
        let original = provider_broadcast(0.1);
        let (retained, first) =
            RetainedStateBroadcast::update(None, original.clone(), 100).unwrap_or_else(|_| std::process::abort());
        let first = first.unwrap_or_else(|| std::process::abort());
        assert!(
            first.revision > original.revision,
            "first publication must use a restart-safe transport revision"
        );

        let (retained, duplicate) =
            RetainedStateBroadcast::update(Some(retained), original, 101).unwrap_or_else(|_| std::process::abort());
        assert!(
            duplicate.is_none(),
            "an identical controller reconcile must not reset the foca transmission budget"
        );

        let changed = provider_broadcast(0.8);
        let (mut retained, changed_outbound) =
            RetainedStateBroadcast::update(Some(retained), changed, 102).unwrap_or_else(|_| std::process::abort());
        let changed_outbound = changed_outbound.unwrap_or_else(|| std::process::abort());
        assert!(
            changed_outbound.revision > first.revision,
            "changed provider state must advance the transport revision"
        );
        assert_eq!(
            changed_outbound
                .snapshot
                .provider("net", "site-a", "provider-a")
                .and_then(|provider| provider.metrics.queue_depth),
            Some(0.8),
            "changed provider state must publish immediately"
        );

        let repair = retained.republish(103);
        assert!(
            repair.revision > changed_outbound.revision,
            "anti-entropy must use a fresh transport revision"
        );
        assert_eq!(
            canonical_state_payload(&repair).unwrap_or_else(|_| std::process::abort()),
            canonical_state_payload(&changed_outbound).unwrap_or_else(|_| std::process::abort()),
            "anti-entropy must preserve the retained provider-state content"
        );
    }

    #[test]
    fn handle_snapshot_starts_empty() {
        let (handle, snapshot_tx, _state_tx) = make_test_handle();
        let snap = handle.snapshot();
        assert!(snap.members.is_empty(), "initial snapshot must be empty");
        drop(snapshot_tx);
    }

    #[test]
    fn handle_snapshot_reflects_published_update() {
        let (handle, snapshot_tx, _state_tx) = make_test_handle();

        let snap_with_member = MembershipSnapshot {
            members: vec![MemberRecord {
                site_id: "site-x".to_owned(),
                endpoint: "10.0.0.1:7946".to_owned(),
                incarnation: 0,
                status: MemberStatus::Alive,
                age_secs: 0,
                gateway_address: None,
                site_cert_pem: None,
                signals_address: None,
            }],
        };
        drop(snapshot_tx.send(snap_with_member));

        let snap = handle.snapshot();
        assert_eq!(snap.connected_count(), 1, "snapshot must reflect published member");
    }

    #[test]
    fn handle_state_snapshot_starts_empty() {
        let (handle, _snap_tx, _state_tx) = make_test_handle();
        let state = handle.state_snapshot();
        assert!(state.providers.is_empty(), "initial CRDT state must have no providers");
    }

    #[test]
    fn handle_state_snapshot_reflects_published_update() {
        let (handle, _snap_tx, state_tx) = make_test_handle();

        let mut snap = GridStateSnapshot::new("site-a".to_owned());
        snap.upsert_provider(crdt::ProviderState {
            network_id: "net".to_owned(),
            site_id: "site-a".to_owned(),
            provider_id: "p1".to_owned(),
            routing_cluster: "site-a".to_owned(),
            models: vec!["model-x".to_owned()],
            backend_kind: "local".to_owned(),
            capacity_weight: 1,
            phase: crdt::ProviderPhase::Available,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            access_policy: crdt::ProviderAccessPolicy::default(),
            revision: 1,
            writer_id: "site-a".to_owned(),
        });
        drop(state_tx.send(snap));

        let state = handle.state_snapshot();
        assert!(
            state.provider("net", "site-a", "p1").is_some(),
            "CRDT state handle must reflect published provider"
        );
    }

    #[test]
    fn none_swim_handle_gives_zero_connected_sites() {
        let no_swim: Option<Arc<SwimHandle>> = None;
        let count = no_swim.as_ref().map_or(0, |h| h.snapshot().connected_count());
        assert_eq!(count, 0, "None swim handle must give zero connected_sites");
    }

    // -----------------------------------------------------------------------
    #[test]
    fn a_seed_naming_this_node_is_never_announced() {
        let own_lb: SocketAddr = "192.168.1.204:7946".parse().unwrap_or_else(|_| std::process::abort());
        let bind: SocketAddr = "10.128.0.5:7946".parse().unwrap_or_else(|_| std::process::abort());
        let peer: SocketAddr = "192.168.1.150:7946".parse().unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            foreign_seeds(vec![own_lb, peer, bind], &[Some(own_lb), Some(bind)]),
            vec![peer],
            "only the peer seed survives, never this node's advertised or bound address"
        );
        assert_eq!(
            foreign_seeds(vec![own_lb], &[Some(own_lb), None]),
            Vec::<SocketAddr>::new(),
            "a seed list naming only this node announces to nobody"
        );
    }

    // SwimHandle::local_addr and announce_seeds
    // -----------------------------------------------------------------------

    #[test]
    fn local_addr_returns_advertise_addr() {
        let (handle, _snap_tx, _state_tx) = make_test_handle();
        let addr: SocketAddr = "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            handle.local_addr(),
            addr,
            "local_addr must return the configured advertise_addr"
        );
    }

    #[test]
    fn announce_seeds_empty_is_noop() {
        let (handle, _snap_tx, _state_tx) = make_test_handle();
        let result = handle.announce_seeds(Vec::new());
        assert!(result.is_ok(), "announce_seeds with empty vec must return Ok");
    }

    #[test]
    fn announce_seeds_sends_to_channel() {
        let (snapshot_tx, snapshot_rx) = watch::channel(MembershipSnapshot::default());
        let (state_tx, state_rx) = watch::channel(GridStateSnapshot::new("test".to_owned()));
        let (broadcast_tx, _broadcast_rx) = mpsc::channel(1);
        let (seed_tx, mut seed_rx) = mpsc::channel(16);
        let (key_tx, _key_rx) = watch::channel(KeyState::Plain);
        let (gateway_tx, _gateway_rx) = watch::channel(None);
        let handle = SwimHandle {
            site_name: "test".to_owned(),
            advertise_addr: "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
            signals_address: None,
            snapshot_rx,
            state_rx,
            broadcast_tx,
            seed_tx,
            key_tx,
            gateway_tx,
            runtime_tx: watch::channel(true).0,
            leave_tx: mpsc::channel(1).0,
        };
        drop((snapshot_tx, state_tx));

        let addr: SocketAddr = "10.0.0.2:7946".parse().unwrap_or_else(|_| std::process::abort());
        let result = handle.announce_seeds(vec![addr]);
        assert!(result.is_ok(), "announce_seeds must succeed when channel has capacity");
        // Verify the seeds were sent to the channel.
        let received = seed_rx.try_recv().unwrap_or_else(|_| std::process::abort());
        assert_eq!(received, vec![addr], "seed batch must arrive at runtime channel");
    }

    #[test]
    fn announce_seeds_returns_closed_when_receiver_dropped() {
        let (_snapshot_tx, snapshot_rx) = watch::channel(MembershipSnapshot::default());
        let (_state_tx, state_rx) = watch::channel(GridStateSnapshot::new("test".to_owned()));
        let (broadcast_tx, _broadcast_rx) = mpsc::channel(1);
        let (seed_tx, seed_rx) = mpsc::channel::<Vec<SocketAddr>>(16);
        let (key_tx, _key_rx) = watch::channel(KeyState::Plain);
        let (gateway_tx, _gateway_rx) = watch::channel(None);
        let handle = SwimHandle {
            site_name: "test".to_owned(),
            advertise_addr: "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
            signals_address: None,
            snapshot_rx,
            state_rx,
            broadcast_tx,
            seed_tx,
            key_tx,
            gateway_tx,
            runtime_tx: watch::channel(true).0,
            leave_tx: mpsc::channel(1).0,
        };
        drop(seed_rx);

        let addr: SocketAddr = "10.0.0.2:7946".parse().unwrap_or_else(|_| std::process::abort());
        let result = handle.announce_seeds(vec![addr]);
        assert!(
            matches!(result, Err(SeedAnnounceError::ChannelClosed)),
            "announce_seeds must return ChannelClosed when receiver is dropped"
        );
    }

    // -----------------------------------------------------------------------
    // start (integration smoke test — requires tokio runtime)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn start_binds_and_returns_handle() {
        let cfg = SwimConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap_or_else(|_| std::process::abort()),
            advertise_addr: None,
            site_name: "test-node".to_owned(),
            seeds: Vec::new(),
            signals_address: None,
            gateway_address: None,
            key: KeyState::Plain,
            revision_lease: test_revision_lease(1),
            revision_renewer: None,
        };
        let handle = start(cfg).await;
        assert!(handle.is_ok(), "start must succeed with an available port");
        let handle = handle.unwrap_or_else(|_| std::process::abort());
        let snap = handle.snapshot();
        assert!(snap.members.is_empty(), "initial snapshot must be empty (no peers yet)");
    }

    #[tokio::test]
    async fn start_fails_on_already_bound_port() {
        // Bind a socket first, then try to start a runtime on the same port.
        let socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|_| std::process::abort());
        let addr = socket.local_addr().unwrap_or_else(|_| std::process::abort());
        let cfg = SwimConfig {
            bind_addr: addr,
            advertise_addr: None,
            site_name: "test".to_owned(),
            seeds: Vec::new(),
            signals_address: None,
            gateway_address: None,
            key: KeyState::Plain,
            revision_lease: test_revision_lease(20_000),
            revision_renewer: None,
        };
        let result = start(cfg).await;
        assert!(result.is_err(), "start on an already-bound port must fail");
    }

    #[tokio::test]
    async fn two_local_nodes_exchange_membership() {
        // Start two SWIM nodes on deterministic local addresses, then have
        // node-2 announce to node-1 through its seed list.
        let addr1 = reserve_local_addr().await;
        let addr2 = reserve_local_addr().await;

        let cfg1 = SwimConfig {
            bind_addr: addr1,
            advertise_addr: Some(addr1),
            site_name: "node-1".to_owned(),
            seeds: Vec::new(),
            signals_address: None,
            gateway_address: None,
            key: KeyState::Plain,
            revision_lease: test_revision_lease(40_000),
            revision_renewer: None,
        };
        let handle1 = start(cfg1).await.unwrap_or_else(|_| std::process::abort());

        let cfg2 = SwimConfig {
            bind_addr: addr2,
            advertise_addr: Some(addr2),
            site_name: "node-2".to_owned(),
            seeds: vec![addr1],
            signals_address: None,
            gateway_address: None,
            key: KeyState::Plain,
            revision_lease: test_revision_lease(60_000),
            revision_renewer: None,
        };
        let handle2 = start(cfg2).await.unwrap_or_else(|_| std::process::abort());

        wait_until_member_alive(&handle1, "node-2").await;
        drop(handle2);
    }

    #[tokio::test]
    #[expect(clippy::too_many_lines, reason = "two real runtimes, a join, and a leave")]
    async fn a_leaving_runtime_stops_and_its_peer_sees_it_dead() {
        let addr1 = reserve_local_addr().await;
        let addr2 = reserve_local_addr().await;
        let handle1 = start(SwimConfig {
            bind_addr: addr1,
            advertise_addr: Some(addr1),
            site_name: "node-1".to_owned(),
            ..test_config(130_000)
        })
        .await
        .expect("start");
        let handle2 = start(SwimConfig {
            bind_addr: addr2,
            advertise_addr: Some(addr2),
            site_name: "node-2".to_owned(),
            seeds: vec![addr1],
            ..test_config(140_000)
        })
        .await
        .expect("start");
        wait_until_member_alive(&handle1, "node-2").await;

        handle2.leave(Duration::from_secs(5)).await;
        assert!(!handle2.is_running(), "the runtime stopped");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while handle1
            .snapshot()
            .members
            .iter()
            .any(|m| m.site_id == "node-2" && m.status == MemberStatus::Alive)
        {
            assert!(tokio::time::Instant::now() < deadline, "the peer saw the leave");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Poll a handle's `state_snapshot()` until a tenant's converged spend
    /// reaches `expected_cents`, or panic at the deadline.
    ///
    /// Exercises the real production merge path: [`GridStateSnapshot::merge_tenant_spend`]
    /// as invoked by [`swim::state_broadcast::StateBroadcastHandler::receive_item`] on
    /// every SWIM gossip round — no CRDT logic is duplicated here.
    async fn wait_until_tenant_spend_converges(handle: &SwimHandle, tenant_id: &str, expected_cents: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let total = handle
                .state_snapshot()
                .tenant_spend
                .get(tenant_id)
                .map(crdt::GCounter::total)
                .unwrap_or_default();
            if total == expected_cents {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "tenant '{tenant_id}' spend must converge to {expected_cents} cents via real SWIM gossip \
                 (last observed: {total} cents)"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "three-node real-UDP gossip proof: join, dual increment, convergence, late join, status tie-in"
    )]
    async fn tenant_spend_converges_at_late_joining_site_after_partition_heals() {
        // grid#40 AC3, live network proof: sites A and B accumulate real per-request
        // spend for the same tenant *before* site C ever joins the mesh (a stand-in
        // for C being partitioned away while A and B kept serving traffic). C then
        // joins ("the partition heals") and must converge to the true cross-site sum
        // purely through the real UDP-bound SWIM runtime — no manual message shuttling,
        // unlike the lower-tier unit-level proof in `swim::node::tests`.
        let addr_a = reserve_local_addr().await;
        let addr_b = reserve_local_addr().await;
        let addr_c = reserve_local_addr().await;

        let handle_a = start(SwimConfig {
            bind_addr: addr_a,
            advertise_addr: Some(addr_a),
            site_name: "site-a".to_owned(),
            seeds: Vec::new(),
            signals_address: None,
            gateway_address: None,
            key: KeyState::Plain,
            revision_lease: test_revision_lease(80_000),
            revision_renewer: None,
        })
        .await
        .unwrap_or_else(|_| std::process::abort());

        let handle_b = start(SwimConfig {
            bind_addr: addr_b,
            advertise_addr: Some(addr_b),
            site_name: "site-b".to_owned(),
            seeds: vec![addr_a],
            signals_address: None,
            gateway_address: None,
            key: KeyState::Plain,
            revision_lease: test_revision_lease(90_000),
            revision_renewer: None,
        })
        .await
        .unwrap_or_else(|_| std::process::abort());
        wait_until_member_alive(&handle_a, "site-b").await;

        // Real per-request cost values, matching `scoring::BackendConfig::cost_per_1k_input`
        // shape rather than an arbitrary test constant.
        let a_cents = cost_cents_for_tokens(0.03, 1_800);
        let mut a_snap = GridStateSnapshot::new("site-a".to_owned());
        a_snap.increment_tenant_spend("tenant-acme", a_cents);
        handle_a
            .publish_state_broadcast(swim::StateBroadcast::new("site-a".to_owned(), 1, a_snap, None))
            .unwrap_or_else(|_| std::process::abort());

        let b_cents = cost_cents_for_tokens(0.06, 2_500);
        let mut b_snap = GridStateSnapshot::new("site-b".to_owned());
        b_snap.increment_tenant_spend("tenant-acme", b_cents);
        handle_b
            .publish_state_broadcast(swim::StateBroadcast::new("site-b".to_owned(), 1, b_snap, None))
            .unwrap_or_else(|_| std::process::abort());

        let total_cents = a_cents + b_cents;
        wait_until_tenant_spend_converges(&handle_a, "tenant-acme", total_cents).await;
        wait_until_tenant_spend_converges(&handle_b, "tenant-acme", total_cents).await;

        // The partition "heals": site-c joins the already-converged A/B mesh for
        // the first time, having missed every prior broadcast.
        let handle_c = start(SwimConfig {
            bind_addr: addr_c,
            advertise_addr: Some(addr_c),
            site_name: "site-c".to_owned(),
            seeds: vec![addr_a],
            signals_address: None,
            gateway_address: None,
            key: KeyState::Plain,
            revision_lease: test_revision_lease(100_000),
            revision_renewer: None,
        })
        .await
        .unwrap_or_else(|_| std::process::abort());
        wait_until_member_alive(&handle_a, "site-c").await;

        wait_until_tenant_spend_converges(&handle_c, "tenant-acme", total_cents).await;

        // End-to-end tie-in: the same status-derivation function the reconciler
        // calls on every reconcile must report the correct spendRatio from C's
        // independently-converged view, proving the full CRDT -> status pipeline.
        let policy = crate::crd::grid_network::BudgetPolicyConfig {
            tenants: vec![crate::crd::grid_network::TenantBudgetConfig {
                tenant_id: "tenant-acme".to_owned(),
                cap_usd: 1.00,
            }],
        };
        let statuses =
            crate::crd::grid_network::resolve_budget_statuses(Some(&policy), &handle_c.state_snapshot().tenant_spend);
        let status = statuses.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(
            status.tenant_id, "tenant-acme",
            "status must be keyed by the policy's tenant_id"
        );
        assert!(
            (status.spend_usd - crate::crd::grid_network::cents_to_usd(total_cents)).abs() < f64::EPSILON,
            "spend_usd must reflect the fully-converged cross-site total observed at the late-joining site"
        );

        drop((handle_b, handle_c));
    }

    /// Real per-request USD cost for `tokens` at `cost_per_1k`, in integer cents —
    /// mirrors how a gateway-side policy filter would size a spend increment from
    /// `scoring::BackendConfig::cost_per_1k_input` (AC5 non-goal: that filter does
    /// not exist yet, so this helper stands in for it here).
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "test-only cost simulation over small constant token counts, well within f64 exact-integer range"
    )]
    fn cost_cents_for_tokens(cost_per_1k: f64, tokens: u64) -> u64 {
        (cost_per_1k * (tokens as f64 / 1000.0) * 100.0).round() as u64
    }

    // -----------------------------------------------------------------------
    // Dead-member eviction
    // -----------------------------------------------------------------------

    #[test]
    fn dead_member_evicted_after_ttl() {
        let ttl = Duration::from_secs(300);
        let t0 = now();
        let t_dead = t0 + Duration::from_secs(10);

        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t0);
        apply_member_event(left("site-a"), &mut tracked, t_dead);
        let just_before_ttl = (t_dead + ttl).checked_sub(Duration::from_secs(1)).unwrap_or(t0);
        assert!(
            prune_tracked_members(&mut tracked, just_before_ttl, ttl).is_empty(),
            "dead member must remain before TTL"
        );
        assert_eq!(
            prune_tracked_members(&mut tracked, t_dead + ttl, ttl),
            vec!["site-a".to_owned()],
            "dead member must be evicted at TTL"
        );
        assert!(tracked.is_empty(), "evicted member must be removed");
    }

    #[test]
    fn alive_member_not_evicted() {
        let ttl = Duration::from_secs(300);
        let t0 = now();
        let t_check = t0 + ttl + Duration::from_secs(100);

        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t0);

        assert!(
            prune_tracked_members(&mut tracked, t_check, ttl).is_empty(),
            "alive member must never be evicted by non-alive retention"
        );
    }

    #[test]
    fn evicted_site_can_rejoin() {
        let ttl = Duration::from_secs(300);
        let t0 = now();
        let t_dead = t0 + Duration::from_secs(10);
        let t_evict = t0 + Duration::from_secs(10) + ttl + Duration::from_secs(1);
        let t_rejoin = t_evict + Duration::from_secs(5);

        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t0);
        apply_member_event(left("site-a"), &mut tracked, t_dead);

        prune_tracked_members(&mut tracked, t_evict, ttl);
        assert!(tracked.is_empty(), "site must be evicted");

        // Rejoin creates fresh entry.
        apply_member_event(joined("site-a"), &mut tracked, t_rejoin);
        assert_eq!(tracked.len(), 1, "rejoined site must create fresh entry");
        let member = tracked.get("site-a").unwrap_or_else(|| std::process::abort());
        assert_eq!(member.status(), MemberStatus::Alive, "rejoined member must be Alive");
        assert!(
            member.status_changed_at.is_none(),
            "rejoined member must have no age tracking"
        );
    }

    #[test]
    fn unknown_left_churn_is_hard_bounded() {
        let mut tracked = HashMap::new();
        let now = now();
        for index in 0..(MAX_TRACKED_MEMBERS * 4) {
            apply_member_event_bounded(left(&format!("site-{index:05}")), &mut tracked, now);
        }
        assert_eq!(
            tracked.len(),
            MAX_TRACKED_MEMBERS,
            "unknown dead-member events must not grow the mirror past its hard bound"
        );
    }

    #[test]
    fn non_alive_capacity_evicts_oldest_deterministically() {
        let now = now();
        let mut tracked = HashMap::new();
        for index in 0..(MAX_NON_ALIVE_MEMBERS + 2) {
            apply_member_event(
                left(&format!("site-{index:05}")),
                &mut tracked,
                now + Duration::from_secs(u64::try_from(index).unwrap_or(u64::MAX)),
            );
        }
        let evicted = prune_tracked_members(&mut tracked, now, Duration::from_secs(10_000));
        assert_eq!(evicted, vec!["site-00000".to_owned(), "site-00001".to_owned()]);
        assert_eq!(tracked.len(), MAX_NON_ALIVE_MEMBERS);
    }
}
