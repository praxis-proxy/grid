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
use swim::{MemberEvent, NodeId, SwimNode, runtime::TimerEvent};
use tokio::{
    net::UdpSocket,
    sync::{
        mpsc::{self, error::TrySendError},
        watch,
    },
};

use crate::swim::{MemberRecord, MemberStatus, MembershipSnapshot};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

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

    /// Data-plane gateway address to advertise in SWIM state broadcasts.
    ///
    /// When set, this address is included in outbound `StateBroadcast` messages
    /// as the `gateway_address` field, and remote peers will use it for
    /// `GridSite.spec.egress.address` instead of the SWIM UDP endpoint.
    pub gateway_address: Option<String>,

    /// AES-256-GCM encryption key for SWIM UDP packets.
    ///
    /// When `Some`, every outgoing SWIM packet is encrypted and authenticated
    /// with this key.  Incoming packets that do not authenticate with this key
    /// are silently dropped — the SWIM state machine never sees them.
    ///
    /// When `None`, SWIM traffic is sent as plaintext (backward-compatible local
    /// and development behavior).
    ///
    /// Configuring a key via `GridNetwork.spec.tls.swimKeyRef` is the preferred
    /// production path.  This field is also used for testing: the operator reads
    /// the env var `GRID_SWIM_ENCRYPT_KEY` (64 hex chars = 32 bytes) at startup
    /// and stores the decoded key here.
    ///
    /// # Security invariant
    ///
    /// The key value must **never** appear in logs, tracing spans, error messages,
    /// Kubernetes resources, or process output.
    pub swim_key: Option<swim::crypto::SwimKey>,
}

impl std::fmt::Debug for SwimConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwimConfig")
            .field("bind_addr", &self.bind_addr)
            .field("advertise_addr", &self.advertise_addr)
            .field("site_name", &self.site_name)
            .field("seeds", &self.seeds)
            .field("gateway_address", &self.gateway_address)
            .field("swim_key", &self.swim_key.map(|_| "<redacted>"))
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Internal runtime member tracking
// ---------------------------------------------------------------------------

/// Runtime-internal member state that extends [`MemberRecord`] with an
/// age-tracking instant.
///
/// The public [`MemberRecord`] type carries an `age_secs` field but the SWIM
/// runtime previously always set it to `0`.  This private struct holds the
/// `status_changed_at` instant required to compute a real elapsed age at
/// snapshot time.
///
/// Conversion to [`MemberRecord`] happens in [`members_snapshot`] by computing
/// `now.saturating_duration_since(status_changed_at).as_secs()`.
struct TrackedMember {
    /// Opaque site identity — mirrors [`MemberRecord::site_id`].
    site_id: String,
    /// Advertised SWIM listener address — mirrors [`MemberRecord::endpoint`].
    endpoint: String,
    /// Incarnation counter — mirrors [`MemberRecord::incarnation`].
    incarnation: u64,
    /// Membership status — mirrors [`MemberRecord::status`].
    status: MemberStatus,
    /// Instant when `status` first transitioned to [`MemberStatus::Dead`] or
    /// [`MemberStatus::Suspect`].  `None` for [`MemberStatus::Alive`] members
    /// and for members that have never been dead/suspect.
    ///
    /// Semantics:
    /// - Set (or preserved) on the **first** Dead/Suspect transition.
    /// - Cleared on a [`MemberStatus::Alive`] (Joined) event.
    /// - Repeated Dead/Suspect events with an existing timestamp are ignored so the age grows monotonically from the
    ///   initial transition time.
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
    /// Convert to a public [`MemberRecord`], computing `age_secs` from
    /// `status_changed_at` relative to `now`.
    ///
    /// `age_secs` is non-zero only for Dead/Suspect members; Alive members
    /// always report `0`.
    fn to_member_record(&self, now: Instant) -> MemberRecord {
        MemberRecord {
            site_id: self.site_id.clone(),
            endpoint: self.endpoint.clone(),
            incarnation: self.incarnation,
            status: self.status.clone(),
            age_secs: self
                .status_changed_at
                .map_or(0, |t| now.saturating_duration_since(t).as_secs()),
            gateway_address: self.gateway_address.clone(),
            site_cert_pem: self.site_cert_pem.clone(),
        }
    }
}

/// Build a [`MembershipSnapshot`] from the current tracked member table.
///
/// `now` is injected so callers can use a fixed [`Instant`] for deterministic
/// testing without sleeps.  In production, pass [`Instant::now()`].
///
/// `gateway_addrs` is a map of site names to their data-plane gateway addresses,
/// obtained from the [`StateBroadcastHandler`].  `cert_pems` is a map of site
/// names to their public site certificate PEMs.  Both maps are used to enrich
/// each [`MemberRecord`] in the returned snapshot.
///
/// [`StateBroadcastHandler`]: swim::StateBroadcastHandler
fn members_snapshot(
    tracked: &HashMap<String, TrackedMember>,
    now: Instant,
    gateway_addrs: &BTreeMap<String, String>,
    cert_pems: &BTreeMap<String, String>,
) -> MembershipSnapshot {
    MembershipSnapshot {
        members: tracked
            .values()
            .map(|t| {
                let mut record = t.to_member_record(now);
                record.gateway_address = gateway_addrs.get(&t.site_id).cloned();
                record.site_cert_pem = cert_pems.get(&t.site_id).cloned();
                record
            })
            .collect(),
    }
}

/// Return true when any tracked member needs age recomputation in snapshots.
fn has_aging_members(tracked: &HashMap<String, TrackedMember>) -> bool {
    tracked
        .values()
        .any(|t| t.status_changed_at.is_some() && matches!(t.status, MemberStatus::Dead | MemberStatus::Suspect))
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

    /// Data-plane gateway address advertised in CRDT state broadcasts.
    gateway_address: Option<String>,

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

    /// Watch sender for the SWIM encryption key.
    ///
    /// Send `Some(key)` to enable encryption; the run loop starts encrypting
    /// outgoing packets and drops non-authenticating inbound packets.
    ///
    /// The initial value is `None` (no encryption) when no env-var key is
    /// configured.  `set_swim_key` sends `Some(key)` once at reconcile time;
    /// sending `None` after a key has been set is not an intended runtime
    /// path and no production code does so.
    ///
    /// Key changes after the initial send require an operator restart because the
    /// old key immediately loses the ability to decrypt in-flight packets from
    /// peers that have not yet received the new key.
    ///
    /// # Security invariant
    ///
    /// The key value must **never** appear in logs, spans, error messages, or
    /// status fields.
    key_tx: watch::Sender<Option<Arc<swim::crypto::SwimKey>>>,
}

impl SwimHandle {
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

    /// Return the configured data-plane gateway address, if any.
    #[must_use]
    pub fn gateway_address(&self) -> Option<&str> {
        self.gateway_address.as_deref()
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
    /// Returns the snapshot without blocking.
    pub fn snapshot(&self) -> MembershipSnapshot {
        self.snapshot_rx.borrow().clone()
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
            .send(Some(Arc::new(key)))
            .map_err(|_e| SetKeyError::RuntimeGone)
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
    let initial_key: Option<Arc<swim::crypto::SwimKey>> = config.swim_key.map(Arc::new);
    let (snapshot_tx, snapshot_rx) = watch::channel(MembershipSnapshot::default());
    let (state_tx, state_rx) = watch::channel(GridStateSnapshot::new(site_name.clone()));
    let (timer_tx, timer_rx) = mpsc::channel::<TimerEvent>(256);
    let (broadcast_tx, broadcast_rx) = mpsc::channel::<swim::StateBroadcast>(32);
    let (seed_tx, seed_rx) = mpsc::channel::<Vec<SocketAddr>>(16);
    let (key_tx, key_rx) = watch::channel(initial_key);
    let channels = RuntimeChannels {
        snapshot_tx,
        timer_tx,
        timer_rx,
        broadcast_rx,
        seed_rx,
    };

    tracing::info!(
        bind_addr = %config.bind_addr,
        advertise_addr = %advertise_addr,
        site_name = %config.site_name,
        seeds = config.seeds.len(),
        encrypted = config.swim_key.is_some(),
        "SWIM runtime starting"
    );

    tokio::spawn(run_loop(
        Arc::new(socket),
        config,
        advertise_addr,
        channels,
        state_tx,
        key_rx,
    ));

    Ok(Arc::new(SwimHandle {
        site_name,
        advertise_addr,
        gateway_address,
        snapshot_rx,
        state_rx,
        broadcast_tx,
        seed_tx,
        key_tx,
    }))
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

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
    reason = "key_rx added to support runtime encryption key loading; separate struct would obscure the startup sequence"
)]
async fn run_loop(
    socket: Arc<UdpSocket>,
    config: SwimConfig,
    advertise_addr: SocketAddr,
    mut channels: RuntimeChannels,
    state_tx: watch::Sender<GridStateSnapshot>,
    mut key_rx: watch::Receiver<Option<Arc<swim::crypto::SwimKey>>>,
) {
    let site_name = config.site_name.clone();
    let identity = NodeId::new(site_name.clone(), advertise_addr);
    let (event_tx, _event_rx) = mpsc::channel(1);
    let mut node = SwimNode::new(identity, event_tx);
    let mut tracked: HashMap<String, TrackedMember> = HashMap::new();
    let mut buf = vec![0_u8; 65_536];
    let mut age_tick = tokio::time::interval(Duration::from_secs(1));
    let gateway_address = config.gateway_address.clone();
    let mut gateway_address_revision = 0_u64;
    let mut next_gateway_republish_at = Instant::now();

    if let Some(gateway_address) = gateway_address.as_deref() {
        publish_gateway_address_broadcast(&mut node, &site_name, gateway_address_revision, gateway_address);
    }

    // Announce to seed peers.  Errors are logged inside SwimNode::announce.
    // Read the initial key (from SwimConfig, if any) for the startup seed sends.
    let startup_key: Option<Arc<swim::crypto::SwimKey>> = key_rx.borrow_and_update().clone();
    for &seed_addr in &config.seeds {
        let seed_id = NodeId::new(format!("seed-{seed_addr}"), seed_addr);
        let output = node.announce(seed_id);
        drain_output(
            output,
            &socket,
            &channels.timer_tx,
            &mut tracked,
            &channels.snapshot_tx,
            &node.gateway_addrs(),
            &node.cert_pems(),
            startup_key.as_deref(),
        )
        .await;
    }

    loop {
        // Snapshot the current key for this iteration.  The key is set once and
        // never changes, so this borrow-and-clone is cheap and branch-free after
        // the first successful key load.
        let current_key: Option<Arc<swim::crypto::SwimKey>> = key_rx.borrow_and_update().clone();

        tokio::select! {
            result = socket.recv_from(&mut buf) => {
                match result {
                    Ok((n, from)) => {
                        let raw = buf.get(..n).unwrap_or(&[]);

                        // Decrypt and authenticate when a key is configured.
                        // On failure, drop the packet silently — do not pass
                        // unauthenticated bytes to foca.
                        let plaintext_buf: Vec<u8>;
                        let data: &[u8] = if let Some(key) = &current_key {
                            if let Ok(plain) = swim::crypto::decrypt(key.as_ref(), raw) {
                                plaintext_buf = plain;
                                &plaintext_buf
                            } else {
                                tracing::warn!(
                                    addr = %from,
                                    bytes = n,
                                    "SWIM: dropped packet (authentication failed)"
                                );
                                continue;
                            }
                        } else {
                            raw
                        };

                        let output = node.handle_data(data);
                        tracing::trace!(from = %from, bytes = n, "SWIM UDP received");
                        drain_output(
                            output,
                            &socket,
                            &channels.timer_tx,
                            &mut tracked,
                            &channels.snapshot_tx,
                            &node.gateway_addrs(),
                            &node.cert_pems(),
                            current_key.as_deref(),
                        )
                        .await;
                        // Publish updated CRDT state after every incoming UDP packet.
                        // Broadcasts are received inside handle_data, so the snapshot
                        // may have advanced.
                        drop(state_tx.send(node.state_snapshot()));
                        // Gateway-address-only broadcasts update the node's gateway-address
                        // map but may not emit a membership event.  Republish the
                        // membership snapshot after every inbound packet so callers see the
                        // latest gateway address attached to already-known members.
                        drop(channels.snapshot_tx.send(members_snapshot(&tracked, Instant::now(), &node.gateway_addrs(), &node.cert_pems())));
                        if let Some(gateway_address) = gateway_address.as_deref() {
                            let now = Instant::now();
                            if now >= next_gateway_republish_at {
                                gateway_address_revision = gateway_address_revision.saturating_add(1);
                                publish_gateway_address_broadcast(
                                    &mut node,
                                    &site_name,
                                    gateway_address_revision,
                                    gateway_address,
                                );
                                let output = node.gossip();
                                drain_output(
                                    output,
                                    &socket,
                                    &channels.timer_tx,
                                    &mut tracked,
                                    &channels.snapshot_tx,
                                    &node.gateway_addrs(),
                                    &node.cert_pems(),
                                    current_key.as_deref(),
                                )
                                .await;
                                drop(state_tx.send(node.state_snapshot()));
                                next_gateway_republish_at = now + Duration::from_secs(1);
                            }
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
                    &node.gateway_addrs(),
                    &node.cert_pems(),
                    current_key.as_deref(),
                )
                .await;
            }
            Some(bc) = channels.broadcast_rx.recv() => {
                // Publish the broadcast then immediately gossip so peers
                // receive it on the next outbound message.
                if let Err(e) = node.publish_state_broadcast(&bc) {
                    tracing::warn!(error = %e, "failed to encode state broadcast");
                } else {
                    let gossip_out = node.gossip();
                    drain_output(
                        gossip_out,
                        &socket,
                        &channels.timer_tx,
                        &mut tracked,
                        &channels.snapshot_tx,
                        &node.gateway_addrs(),
                        &node.cert_pems(),
                        current_key.as_deref(),
                    )
                    .await;
                }
                drop(state_tx.send(node.state_snapshot()));
            }
            Some(seeds) = channels.seed_rx.recv() => {
                // Announce to CRD-declared seed peers at runtime.
                // Re-announcing to existing members is idempotent (foca ignores them).
                for addr in seeds {
                    let seed_id = NodeId::new(format!("seed-{addr}"), addr);
                    let output = node.announce(seed_id);
                    drain_output(
                        output,
                        &socket,
                        &channels.timer_tx,
                        &mut tracked,
                        &channels.snapshot_tx,
                        &node.gateway_addrs(),
                        &node.cert_pems(),
                        current_key.as_deref(),
                    )
                    .await;
                }
            }
            _ = age_tick.tick() => {
                // Age is derived from an internal Instant, but SwimHandle readers
                // only see the latest published MembershipSnapshot.  Republish
                // while any member is Dead/Suspect so age_secs advances even when
                // no new membership event arrives.
                if has_aging_members(&tracked) {
                    drop(channels.snapshot_tx.send(members_snapshot(&tracked, Instant::now(), &node.gateway_addrs(), &node.cert_pems())));
                }
            }
        }
    }
}

/// Queue a gateway-address-only state broadcast.
///
/// This is published at startup and re-published after SWIM activity so a node
/// that joins before it has any local `GridNetwork` can still advertise its
/// data-plane gateway address to peers.  The broadcast carries an empty CRDT
/// snapshot and only updates the peer gateway-address map.
fn publish_gateway_address_broadcast(node: &mut SwimNode, site_name: &str, revision: u64, gateway_address: &str) {
    let broadcast = swim::StateBroadcast::new(
        site_name.to_owned(),
        revision,
        GridStateSnapshot::new(site_name.to_owned()),
        Some(gateway_address.to_owned()),
    );
    if let Err(e) = node.publish_state_broadcast(&broadcast) {
        tracing::warn!(error = %e, "failed to encode gateway address broadcast");
    }
}

/// Send outbound messages, schedule timers, apply membership events.
///
/// Uses [`Instant::now()`] for age tracking so Dead/Suspect transitions record
/// an accurate wall-clock start time.  The same `now` value is used for all
/// events processed in a single call, ensuring consistency within one gossip round.
///
/// When `swim_key` is `Some`, every outgoing SWIM packet is encrypted with
/// AES-256-GCM before being written to the socket.
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
    gateway_addrs: &BTreeMap<String, String>,
    cert_pems: &BTreeMap<String, String>,
    swim_key: Option<&swim::crypto::SwimKey>,
) {
    for msg in output.messages {
        // Encrypt outgoing packet when a key is configured.
        let payload: std::borrow::Cow<'_, [u8]> = if let Some(key) = swim_key {
            std::borrow::Cow::Owned(swim::crypto::encrypt(key, &msg.data))
        } else {
            std::borrow::Cow::Borrowed(&msg.data)
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
        apply_member_event(event, tracked, now);
        changed = true;
    }
    if changed {
        drop(snapshot_tx.send(members_snapshot(tracked, now, gateway_addrs, cert_pems)));
    }
}

/// Apply a membership event to the internal tracked-member table.
///
/// `now` is the instant at which the event is being processed.  Pass
/// [`Instant::now()`] in production; pass a synthetic past instant in tests to
/// verify age computation without sleeping.
///
/// # Age-tracking semantics
///
/// | Event | `status_changed_at` | `status` |
/// |-------|---------------------|---------|
/// | `Joined` | Cleared (`None`) | `Alive` |
/// | `Suspect` (first time or was `Alive`) | Set to `now` | `Suspect` |
/// | `Suspect` (already `Suspect`/`Dead`) | Preserved (age grows monotonically) | `Suspect` |
/// | `Left` / unknown (`Dead`) | Set to `now` if not already set | `Dead` |
#[expect(
    clippy::too_many_lines,
    reason = "exhaustive match over all member-event variants with per-event age-tracking logic; splitting would obscure the status-change semantics"
)]
fn apply_member_event(event: MemberEvent, tracked: &mut HashMap<String, TrackedMember>, now: Instant) {
    match event {
        MemberEvent::Joined { site_name, addr } => {
            tracing::info!(site = %site_name, addr = %addr, "SWIM member joined");
            // Joined always creates/replaces with Alive and clears age tracking.
            tracked.insert(
                site_name.clone(),
                TrackedMember {
                    site_id: site_name,
                    endpoint: addr.to_string(),
                    incarnation: 0,
                    status: MemberStatus::Alive,
                    status_changed_at: None,
                    gateway_address: None,
                    site_cert_pem: None,
                },
            );
        },
        MemberEvent::Left { site_name } => {
            tracing::info!(site = %site_name, "SWIM member left");
            apply_left_event(site_name, tracked, now);
        },
        MemberEvent::Suspect { site_name } => {
            tracing::warn!(site = %site_name, "SWIM member suspected");
            if let Some(t) = tracked.get_mut(&site_name) {
                let was_healthy = t.status == MemberStatus::Alive;
                t.status = MemberStatus::Suspect;
                // Only record status_changed_at on the first transition from Alive.
                // Repeated Suspect events preserve the original timestamp so age
                // grows monotonically from the initial failure.
                if was_healthy {
                    t.status_changed_at = Some(now);
                }
            }
        },
    }
}

/// Apply a member-left/down event as a Dead tombstone with age tracking.
fn apply_left_event(site_name: String, tracked: &mut HashMap<String, TrackedMember>, now: Instant) {
    if let Some(t) = tracked.get_mut(&site_name) {
        let was_not_dead = t.status != MemberStatus::Dead;
        t.status = MemberStatus::Dead;
        // Only set status_changed_at on the first Dead transition; preserve
        // the original Suspect timestamp if already suspect so age is continuous.
        if was_not_dead && t.status_changed_at.is_none() {
            t.status_changed_at = Some(now);
        }
        return;
    }
    // Unknown member declared Dead — create a tombstone with age tracking from now.
    tracked.insert(
        site_name.clone(),
        TrackedMember {
            site_id: site_name,
            endpoint: String::new(),
            incarnation: 0,
            status: MemberStatus::Dead,
            status_changed_at: Some(now),
            gateway_address: None,
            site_cert_pem: None,
        },
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // apply_member_event
    // -----------------------------------------------------------------------

    fn joined(site_name: &str) -> MemberEvent {
        MemberEvent::Joined {
            site_name: site_name.to_owned(),
            addr: "10.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
        }
    }

    fn left(site_name: &str) -> MemberEvent {
        MemberEvent::Left {
            site_name: site_name.to_owned(),
        }
    }

    fn suspect(site_name: &str) -> MemberEvent {
        MemberEvent::Suspect {
            site_name: site_name.to_owned(),
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

    #[test]
    fn joined_event_inserts_alive_member() {
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, now());
        assert!(tracked.contains_key("site-a"), "member must be inserted");
        assert_eq!(
            tracked.get("site-a").unwrap_or_else(|| std::process::abort()).status,
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
            tracked.get("site-a").unwrap_or_else(|| std::process::abort()).status,
            MemberStatus::Dead,
            "member must be marked Dead after Left event"
        );
    }

    #[test]
    fn left_event_for_unknown_member_inserts_dead_tombstone() {
        let mut tracked = HashMap::new();
        apply_member_event(left("site-a"), &mut tracked, now());
        assert_eq!(
            tracked.get("site-a").unwrap_or_else(|| std::process::abort()).status,
            MemberStatus::Dead,
            "unknown Left event must preserve a Dead tombstone"
        );
    }

    #[test]
    fn suspect_event_marks_member_suspect() {
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, now());
        apply_member_event(suspect("site-a"), &mut tracked, now());
        assert_eq!(
            tracked.get("site-a").unwrap_or_else(|| std::process::abort()).status,
            MemberStatus::Suspect,
            "member status must be Suspect after suspect event"
        );
    }

    #[test]
    fn suspect_event_for_unknown_member_is_ignored() {
        let mut tracked = HashMap::new();
        apply_member_event(suspect("nonexistent"), &mut tracked, now());
        assert!(tracked.is_empty(), "suspect for unknown member must not insert");
    }

    #[test]
    fn multiple_joins_produce_correct_connected_count() {
        let t = now();
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t);
        apply_member_event(joined("site-b"), &mut tracked, t);
        let snap = members_snapshot(&tracked, t, &BTreeMap::new(), &BTreeMap::new());
        assert_eq!(snap.connected_count(), 2, "two Alive members must give count=2");
    }

    #[test]
    fn suspect_member_not_counted_as_connected() {
        let t = now();
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t);
        apply_member_event(suspect("site-a"), &mut tracked, t);
        let snap = members_snapshot(&tracked, t, &BTreeMap::new(), &BTreeMap::new());
        assert_eq!(snap.connected_count(), 0, "Suspect member must not count as connected");
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
    fn has_aging_members_true_for_suspect_and_dead_members() {
        let t = now();
        let mut suspect_tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut suspect_tracked, t);
        apply_member_event(suspect("site-a"), &mut suspect_tracked, t);
        assert!(
            has_aging_members(&suspect_tracked),
            "Suspect member must require age republish"
        );

        let mut dead_tracked = HashMap::new();
        apply_member_event(left("site-b"), &mut dead_tracked, t);
        assert!(
            has_aging_members(&dead_tracked),
            "Dead member must require age republish"
        );
    }

    // -----------------------------------------------------------------------
    // SWIM age tracking — deterministic tests using synthetic instants
    // -----------------------------------------------------------------------

    #[test]
    fn joined_member_has_zero_age() {
        let t = now();
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t);
        let snap = members_snapshot(&tracked, t, &BTreeMap::new(), &BTreeMap::new());
        let m = snap.members.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(m.age_secs, 0, "Alive member must have age_secs=0");
    }

    #[test]
    fn suspect_event_starts_age_clock() {
        // Simulate: join at t0, become suspect at t0+30s, snapshot at t0+30s.
        let t0 = now();
        let t_suspect = t0 + Duration::from_secs(30);
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t0);
        apply_member_event(suspect("site-a"), &mut tracked, t_suspect);
        let snap = members_snapshot(&tracked, t_suspect, &BTreeMap::new(), &BTreeMap::new());
        let m = snap.members.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(m.status, MemberStatus::Suspect);
        assert_eq!(m.age_secs, 0, "age at the moment of transition must be 0");
    }

    #[test]
    fn suspect_age_grows_over_time() {
        let t0 = now();
        let t_suspect = t0 + Duration::from_secs(10);
        let t_snap = t0 + Duration::from_secs(70);
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t0);
        apply_member_event(suspect("site-a"), &mut tracked, t_suspect);
        let snap = members_snapshot(&tracked, t_snap, &BTreeMap::new(), &BTreeMap::new());
        let m = snap.members.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(m.status, MemberStatus::Suspect);
        assert_eq!(m.age_secs, 60, "age must be elapsed since transition (70s - 10s = 60s)");
    }

    #[test]
    fn repeated_suspect_preserves_original_timestamp() {
        // First Suspect at t+10s, second at t+50s. Age at t+70s must be 60s (t+70 - t+10).
        let t0 = now();
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t0);
        apply_member_event(suspect("site-a"), &mut tracked, t0 + Duration::from_secs(10));
        apply_member_event(suspect("site-a"), &mut tracked, t0 + Duration::from_secs(50));
        let snap = members_snapshot(
            &tracked,
            t0 + Duration::from_secs(70),
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        let m = snap.members.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(m.age_secs, 60, "repeated Suspect must not reset age clock");
    }

    #[test]
    fn dead_event_starts_age_clock() {
        let t0 = now();
        let t_dead = t0 + Duration::from_secs(20);
        let t_snap = t0 + Duration::from_secs(80);
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t0);
        apply_member_event(left("site-a"), &mut tracked, t_dead);
        let snap = members_snapshot(&tracked, t_snap, &BTreeMap::new(), &BTreeMap::new());
        let m = snap.members.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(m.status, MemberStatus::Dead);
        assert_eq!(m.age_secs, 60, "dead age must be 80s - 20s = 60s");
    }

    #[test]
    fn suspect_to_dead_preserves_original_suspect_timestamp() {
        // Suspect at t+10s, then Dead at t+40s. Age at t+70s = 70-10 = 60s.
        let t0 = now();
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t0);
        apply_member_event(suspect("site-a"), &mut tracked, t0 + Duration::from_secs(10));
        apply_member_event(left("site-a"), &mut tracked, t0 + Duration::from_secs(40));
        let snap = members_snapshot(
            &tracked,
            t0 + Duration::from_secs(70),
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        let m = snap.members.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(m.status, MemberStatus::Dead);
        assert_eq!(
            m.age_secs, 60,
            "dead after suspect must use original suspect timestamp (60s), not dead transition time (30s)"
        );
    }

    #[test]
    fn alive_after_dead_resets_age_to_zero() {
        let t0 = now();
        let mut tracked = HashMap::new();
        apply_member_event(joined("site-a"), &mut tracked, t0);
        apply_member_event(left("site-a"), &mut tracked, t0 + Duration::from_secs(10));
        // Rejoin clears status_changed_at → age=0.
        apply_member_event(joined("site-a"), &mut tracked, t0 + Duration::from_secs(50));
        let snap = members_snapshot(
            &tracked,
            t0 + Duration::from_secs(70),
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
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
        let snap = members_snapshot(&tracked, t_snap, &BTreeMap::new(), &BTreeMap::new());
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
        let (key_tx, _key_rx) = watch::channel(None);
        let handle = SwimHandle {
            site_name: "test".to_owned(),
            advertise_addr: "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
            gateway_address: None,
            snapshot_rx,
            state_rx,
            broadcast_tx,
            seed_tx,
            key_tx,
        };
        (handle, snapshot_tx, state_tx)
    }

    #[test]
    fn handle_exposes_gateway_address() {
        let (snapshot_tx, snapshot_rx) = watch::channel(MembershipSnapshot::default());
        let (state_tx, state_rx) = watch::channel(GridStateSnapshot::new("test".to_owned()));
        let (broadcast_tx, _broadcast_rx) = mpsc::channel(1);
        let (seed_tx, _seed_rx) = mpsc::channel(16);
        let (key_tx, _key_rx) = watch::channel(None);
        let handle = SwimHandle {
            site_name: "test".to_owned(),
            advertise_addr: "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
            gateway_address: Some("127.0.0.1:19080".to_owned()),
            snapshot_rx,
            state_rx,
            broadcast_tx,
            seed_tx,
            key_tx,
        };
        drop((snapshot_tx, state_tx));

        assert_eq!(
            handle.gateway_address(),
            Some("127.0.0.1:19080"),
            "handle must expose configured gateway address for state broadcasts"
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
            phase: crdt::ProviderPhase::Available,
            metrics: crdt::ProviderMetricsSnapshot::default(),
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
        let (key_tx, _key_rx) = watch::channel(None);
        let handle = SwimHandle {
            site_name: "test".to_owned(),
            advertise_addr: "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
            gateway_address: None,
            snapshot_rx,
            state_rx,
            broadcast_tx,
            seed_tx,
            key_tx,
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
        let (key_tx, _key_rx) = watch::channel(None);
        let handle = SwimHandle {
            site_name: "test".to_owned(),
            advertise_addr: "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
            gateway_address: None,
            snapshot_rx,
            state_rx,
            broadcast_tx,
            seed_tx,
            key_tx,
        };
        // Drop the receiver to simulate the runtime loop having exited.
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
            gateway_address: None,
            swim_key: None,
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
            gateway_address: None,
            swim_key: None,
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
            gateway_address: None,
            swim_key: None,
        };
        let handle1 = start(cfg1).await.unwrap_or_else(|_| std::process::abort());

        let cfg2 = SwimConfig {
            bind_addr: addr2,
            advertise_addr: Some(addr2),
            site_name: "node-2".to_owned(),
            seeds: vec![addr1],
            gateway_address: None,
            swim_key: None,
        };
        let handle2 = start(cfg2).await.unwrap_or_else(|_| std::process::abort());

        wait_until_member_alive(&handle1, "node-2").await;
        drop(handle2);
    }
}
