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

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

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
#[derive(Clone, Debug)]
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
    let (snapshot_tx, snapshot_rx) = watch::channel(MembershipSnapshot::default());
    let (state_tx, state_rx) = watch::channel(GridStateSnapshot::new(site_name.clone()));
    let (timer_tx, timer_rx) = mpsc::channel::<TimerEvent>(256);
    let (broadcast_tx, broadcast_rx) = mpsc::channel::<swim::StateBroadcast>(32);
    let (seed_tx, seed_rx) = mpsc::channel::<Vec<SocketAddr>>(16);
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
        "SWIM runtime starting"
    );

    tokio::spawn(run_loop(Arc::new(socket), config, advertise_addr, channels, state_tx));

    Ok(Arc::new(SwimHandle {
        site_name,
        advertise_addr,
        snapshot_rx,
        state_rx,
        broadcast_tx,
        seed_tx,
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
async fn run_loop(
    socket: Arc<UdpSocket>,
    config: SwimConfig,
    advertise_addr: SocketAddr,
    mut channels: RuntimeChannels,
    state_tx: watch::Sender<GridStateSnapshot>,
) {
    let identity = NodeId::new(config.site_name, advertise_addr);
    let (event_tx, _event_rx) = mpsc::channel(1);
    let mut node = SwimNode::new(identity, event_tx);
    let mut members: HashMap<String, MemberRecord> = HashMap::new();
    let mut buf = vec![0_u8; 65_536];

    // Announce to seed peers.  Errors are logged inside SwimNode::announce.
    for &seed_addr in &config.seeds {
        let seed_id = NodeId::new(format!("seed-{seed_addr}"), seed_addr);
        let output = node.announce(seed_id);
        drain_output(output, &socket, &channels.timer_tx, &mut members, &channels.snapshot_tx).await;
    }

    loop {
        tokio::select! {
            result = socket.recv_from(&mut buf) => {
                match result {
                    Ok((n, from)) => {
                        let data = buf.get(..n).unwrap_or(&[]);
                        let output = node.handle_data(data);
                        tracing::trace!(from = %from, bytes = n, "SWIM UDP received");
                        drain_output(
                            output,
                            &socket,
                            &channels.timer_tx,
                            &mut members,
                            &channels.snapshot_tx,
                        )
                        .await;
                        // Publish updated CRDT state after every incoming UDP packet.
                        // Broadcasts are received inside handle_data, so the snapshot
                        // may have advanced.
                        drop(state_tx.send(node.state_snapshot()));
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
                    &mut members,
                    &channels.snapshot_tx,
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
                        &mut members,
                        &channels.snapshot_tx,
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
                        &mut members,
                        &channels.snapshot_tx,
                    )
                    .await;
                }
            }
        }
    }
}

/// Send outbound messages, schedule timers, apply membership events.
async fn drain_output(
    output: swim::AccumulatedOutput,
    socket: &UdpSocket,
    timer_tx: &mpsc::Sender<TimerEvent>,
    members: &mut HashMap<String, MemberRecord>,
    snapshot_tx: &watch::Sender<MembershipSnapshot>,
) {
    for msg in output.messages {
        if let Err(e) = socket.send_to(&msg.data, msg.addr).await {
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
    for event in output.events {
        apply_member_event(event, members);
        changed = true;
    }
    if changed {
        let snapshot = MembershipSnapshot {
            members: members.values().cloned().collect(),
        };
        drop(snapshot_tx.send(snapshot));
    }
}

/// Apply a membership event to the local member map.
fn apply_member_event(event: MemberEvent, members: &mut HashMap<String, MemberRecord>) {
    match event {
        MemberEvent::Joined { site_name, addr } => {
            tracing::info!(site = %site_name, addr = %addr, "SWIM member joined");
            members.insert(
                site_name.clone(),
                MemberRecord {
                    site_id: site_name,
                    endpoint: addr.to_string(),
                    incarnation: 0,
                    status: MemberStatus::Alive,
                    age_secs: 0,
                },
            );
        },
        MemberEvent::Left { site_name } => {
            tracing::info!(site = %site_name, "SWIM member left");
            apply_left_event(site_name, members);
        },
        MemberEvent::Suspect { site_name } => {
            tracing::warn!(site = %site_name, "SWIM member suspected");
            if let Some(r) = members.get_mut(&site_name) {
                r.status = MemberStatus::Suspect;
            }
        },
    }
}

/// Apply a member-left/down event as a dead tombstone.
fn apply_left_event(site_name: String, members: &mut HashMap<String, MemberRecord>) {
    if let Some(r) = members.get_mut(&site_name) {
        r.status = MemberStatus::Dead;
        return;
    }
    members.insert(
        site_name.clone(),
        MemberRecord {
            site_id: site_name,
            endpoint: String::new(),
            incarnation: 0,
            status: MemberStatus::Dead,
            age_secs: 0,
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
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
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
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    #[test]
    fn joined_event_inserts_alive_member() {
        let mut members = HashMap::new();
        apply_member_event(joined("site-a"), &mut members);
        assert!(members.contains_key("site-a"), "member must be inserted");
        assert_eq!(
            members.get("site-a").unwrap_or_else(|| std::process::abort()).status,
            MemberStatus::Alive,
            "joined member must be Alive"
        );
    }

    #[test]
    fn left_event_marks_member_dead() {
        let mut members = HashMap::new();
        apply_member_event(joined("site-a"), &mut members);
        apply_member_event(left("site-a"), &mut members);
        assert_eq!(
            members.get("site-a").unwrap_or_else(|| std::process::abort()).status,
            MemberStatus::Dead,
            "member must be marked Dead after Left event"
        );
    }

    #[test]
    fn left_event_for_unknown_member_inserts_dead_tombstone() {
        let mut members = HashMap::new();
        apply_member_event(left("site-a"), &mut members);
        assert_eq!(
            members.get("site-a").unwrap_or_else(|| std::process::abort()).status,
            MemberStatus::Dead,
            "unknown Left event must preserve a Dead tombstone"
        );
    }

    #[test]
    fn suspect_event_marks_member_suspect() {
        let mut members = HashMap::new();
        apply_member_event(joined("site-a"), &mut members);
        apply_member_event(suspect("site-a"), &mut members);
        assert_eq!(
            members.get("site-a").unwrap_or_else(|| std::process::abort()).status,
            MemberStatus::Suspect,
            "member status must be Suspect after suspect event"
        );
    }

    #[test]
    fn suspect_event_for_unknown_member_is_ignored() {
        let mut members = HashMap::new();
        apply_member_event(suspect("nonexistent"), &mut members);
        assert!(members.is_empty(), "suspect for unknown member must not insert");
    }

    #[test]
    fn multiple_joins_produce_correct_connected_count() {
        let mut members = HashMap::new();
        apply_member_event(joined("site-a"), &mut members);
        apply_member_event(joined("site-b"), &mut members);
        let snap = MembershipSnapshot {
            members: members.values().cloned().collect(),
        };
        assert_eq!(snap.connected_count(), 2, "two Alive members must give count=2");
    }

    #[test]
    fn suspect_member_not_counted_as_connected() {
        let mut members = HashMap::new();
        apply_member_event(joined("site-a"), &mut members);
        apply_member_event(suspect("site-a"), &mut members);
        let snap = MembershipSnapshot {
            members: members.values().cloned().collect(),
        };
        assert_eq!(snap.connected_count(), 0, "Suspect member must not count as connected");
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
        let handle = SwimHandle {
            site_name: "test".to_owned(),
            advertise_addr: "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
            snapshot_rx,
            state_rx,
            broadcast_tx,
            seed_tx,
        };
        (handle, snapshot_tx, state_tx)
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
        let handle = SwimHandle {
            site_name: "test".to_owned(),
            advertise_addr: "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
            snapshot_rx,
            state_rx,
            broadcast_tx,
            seed_tx,
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
        let handle = SwimHandle {
            site_name: "test".to_owned(),
            advertise_addr: "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
            snapshot_rx,
            state_rx,
            broadcast_tx,
            seed_tx,
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
        };
        let handle1 = start(cfg1).await.unwrap_or_else(|_| std::process::abort());

        let cfg2 = SwimConfig {
            bind_addr: addr2,
            advertise_addr: Some(addr2),
            site_name: "node-2".to_owned(),
            seeds: vec![addr1],
        };
        let handle2 = start(cfg2).await.unwrap_or_else(|_| std::process::abort());

        wait_until_member_alive(&handle1, "node-2").await;
        drop(handle2);
    }
}
