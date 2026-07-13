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

use swim::{MemberEvent, NodeId, SwimNode, runtime::TimerEvent};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, watch},
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

/// A handle to the live SWIM runtime.
///
/// Returned by `start`; shared across all `GridNetwork` reconciles via
/// `OperatorCtx`.  Produces a [`MembershipSnapshot`] on each call to
/// [`SwimHandle::snapshot`] by cloning the most recent watch value — no blocking.
pub struct SwimHandle {
    /// Watch channel receiver for membership snapshots.
    snapshot_rx: watch::Receiver<MembershipSnapshot>,
}

impl SwimHandle {
    /// Clone the most recently published [`MembershipSnapshot`].
    ///
    /// Returns the snapshot without blocking.  The snapshot reflects
    /// all membership events processed since the runtime started.
    pub fn snapshot(&self) -> MembershipSnapshot {
        self.snapshot_rx.borrow().clone()
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

    let (snapshot_tx, snapshot_rx) = watch::channel(MembershipSnapshot::default());
    let (timer_tx, timer_rx) = mpsc::channel::<TimerEvent>(256);
    let channels = RuntimeChannels {
        snapshot_tx,
        timer_tx,
        timer_rx,
    };

    tracing::info!(
        bind_addr = %config.bind_addr,
        advertise_addr = %advertise_addr,
        site_name = %config.site_name,
        seeds = config.seeds.len(),
        "SWIM runtime starting"
    );

    tokio::spawn(run_loop(Arc::new(socket), config, advertise_addr, channels));

    Ok(Arc::new(SwimHandle { snapshot_rx }))
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
async fn run_loop(
    socket: Arc<UdpSocket>,
    config: SwimConfig,
    advertise_addr: SocketAddr,
    mut channels: RuntimeChannels,
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

    #[test]
    fn handle_snapshot_starts_empty() {
        let (snapshot_tx, snapshot_rx) = watch::channel(MembershipSnapshot::default());
        let handle = SwimHandle { snapshot_rx };
        let snap = handle.snapshot();
        assert!(snap.members.is_empty(), "initial snapshot must be empty");
        drop(snapshot_tx);
    }

    #[test]
    fn handle_snapshot_reflects_published_update() {
        let (snapshot_tx, snapshot_rx) = watch::channel(MembershipSnapshot::default());
        let handle = SwimHandle { snapshot_rx };

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
    fn none_swim_handle_gives_zero_connected_sites() {
        let no_swim: Option<Arc<SwimHandle>> = None;
        let count = no_swim.as_ref().map_or(0, |h| h.snapshot().connected_count());
        assert_eq!(count, 0, "None swim handle must give zero connected_sites");
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
