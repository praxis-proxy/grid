//! High-level SWIM node wrapping a [`foca::Foca`] instance.
//!
//! [`SwimNode`] encapsulates foca internals — codec, RNG, broadcast handler,
//! and the [`GridRuntime`] adapter — so callers interact only with Grid-specific
//! types: [`AccumulatedOutput`], [`MemberEvent`], and `GridStateSnapshot`.
//!
//! The runtime is **not** thread-safe; run the node from a single task and pass
//! only [`AccumulatedOutput`] across task boundaries.

use std::{collections::BTreeMap, time::Duration};

use crdt::GridStateSnapshot;
use rand::{SeedableRng as _, rngs::SmallRng};
use tokio::sync::{mpsc, watch};

use crate::{
    AccumulatedOutput, GridRuntime, MemberEvent, NodeId,
    runtime::TimerEvent,
    state_broadcast::{StateBroadcast, StateBroadcastHandler},
};

// ---------------------------------------------------------------------------
// Internal type alias
// ---------------------------------------------------------------------------

/// Concrete foca type used by the Grid, wired with the CRDT broadcast handler.
///
/// Using `BincodeCodec` with the standard bincode 2.x config provides compact,
/// backward-compatible serialization.  `SmallRng` is adequate for gossip-target
/// randomization (not a cryptographic use).
type GridFoca = foca::Foca<NodeId, foca::BincodeCodec<bincode::config::Configuration>, SmallRng, StateBroadcastHandler>;

// ---------------------------------------------------------------------------
// SwimNode
// ---------------------------------------------------------------------------

/// A ready-to-drive SWIM node with live CRDT state broadcast support.
///
/// Callers drive the node by feeding incoming UDP bytes and timer events; after
/// each call, [`AccumulatedOutput`] contains UDP messages to send and timers to
/// schedule.
pub struct SwimNode {
    /// The foca membership instance (owns the [`StateBroadcastHandler`]).
    foca: GridFoca,

    /// Runtime adapter accumulating foca's side effects.
    runtime: GridRuntime,

    /// Watch receiver for the merged CRDT state snapshot.
    ///
    /// Written by the [`StateBroadcastHandler`] inside foca whenever a
    /// broadcast is received; read via [`SwimNode::state_snapshot`].
    state_rx: watch::Receiver<GridStateSnapshot>,

    /// Watch receiver for the gateway address map.
    ///
    /// Updated by the [`StateBroadcastHandler`] inside foca when a broadcast
    /// with a gateway address extension is received.
    gateway_addrs_rx: watch::Receiver<BTreeMap<String, String>>,

    /// Watch receiver for the public site certificate PEM map.
    ///
    /// Updated by the [`StateBroadcastHandler`] inside foca when a broadcast
    /// carrying a `site_cert_pem` extension is received.
    cert_pems_rx: watch::Receiver<BTreeMap<String, String>>,
}

impl SwimNode {
    /// Create a new SWIM node with the given identity.
    ///
    /// Membership events are forwarded to `event_tx`.  Callers may also
    /// process events from [`AccumulatedOutput::events`] directly.
    pub fn new(identity: NodeId, event_tx: mpsc::Sender<MemberEvent>) -> Self {
        let seed = {
            // Truncate nanoseconds to u64; we want spread, not precision.
            #[expect(
                clippy::cast_possible_truncation,
                reason = "intentional truncation for entropy mixing"
            )]
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos() as u64;
            let port = u64::from(identity.socket_addr().port());
            nanos ^ port.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        };
        let rng = SmallRng::seed_from_u64(seed);
        let codec = foca::BincodeCodec(bincode::config::standard());

        let site_id = identity.site_name().to_owned();
        let handler = StateBroadcastHandler::new(site_id);
        let state_rx = handler.subscribe();
        let gateway_addrs_rx = handler.subscribe_gateway_addrs();
        let cert_pems_rx = handler.subscribe_cert_pems();

        Self {
            foca: foca::Foca::with_custom_broadcast(identity, foca::Config::simple(), rng, codec, handler),
            runtime: GridRuntime::new(event_tx),
            state_rx,
            gateway_addrs_rx,
            cert_pems_rx,
        }
    }

    /// Feed an incoming UDP packet to foca.
    ///
    /// Returns accumulated side effects: outbound messages, scheduled timers,
    /// membership events, and any CRDT state broadcast payloads received.
    /// Protocol errors are logged at `warn` level and do not abort the output.
    pub fn handle_data(&mut self, data: &[u8]) -> AccumulatedOutput {
        if let Err(e) = self.foca.handle_data(data, &mut self.runtime) {
            tracing::warn!(error = %e, len = data.len(), "foca handle_data error");
        }
        self.runtime.take_output()
    }

    /// Deliver a scheduled timer event to foca.
    ///
    /// Only [`TimerEvent::Token`] events are forwarded; others are silently
    /// ignored.
    pub fn handle_timer(&mut self, event: TimerEvent) -> AccumulatedOutput {
        if let TimerEvent::Token(t) = event
            && let Err(e) = self.foca.handle_timer(t, &mut self.runtime)
        {
            tracing::warn!(error = %e, "foca handle_timer error");
        }
        self.runtime.take_output()
    }

    /// Announce this node to a known peer, requesting membership inclusion.
    ///
    /// Any pending CRDT state broadcasts are piggybacked on the announce probe
    /// message — call [`publish_state_broadcast`] before announcing to a new
    /// peer to propagate state eagerly.
    ///
    /// [`publish_state_broadcast`]: SwimNode::publish_state_broadcast
    pub fn announce(&mut self, dst: NodeId) -> AccumulatedOutput {
        if let Err(e) = self.foca.announce(dst, &mut self.runtime) {
            tracing::warn!(error = %e, "foca announce error");
        }
        self.runtime.take_output()
    }

    /// Trigger an explicit gossip round.
    ///
    /// foca sends membership updates — including any queued CRDT state broadcasts
    /// — to a random subset of known members.  Call this after
    /// [`publish_state_broadcast`] to propagate state without waiting for a
    /// periodic probe timer.
    ///
    /// Returns an empty [`AccumulatedOutput`] when no members are known yet.
    ///
    /// [`publish_state_broadcast`]: SwimNode::publish_state_broadcast
    pub fn gossip(&mut self) -> AccumulatedOutput {
        if let Err(e) = self.foca.gossip(&mut self.runtime) {
            tracing::warn!(error = %e, "foca gossip error");
        }
        self.runtime.take_output()
    }

    /// Queue a CRDT state broadcast for piggybacking on the next probe/gossip message.
    ///
    /// foca attaches queued broadcasts to outbound probe and gossip messages
    /// automatically.  Stale broadcasts (lower revision than what foca's
    /// peer already acknowledged) are silently dropped by the invalidation
    /// mechanism.
    ///
    /// # Errors
    ///
    /// Returns an error if the broadcast payload cannot be encoded.
    pub fn publish_state_broadcast(&mut self, broadcast: &StateBroadcast) -> Result<(), bincode::error::EncodeError> {
        let bytes = broadcast.encode()?;
        match self.foca.add_broadcast(&bytes) {
            Ok(true) => {
                tracing::debug!(origin = %broadcast.origin_site, rev = broadcast.revision, "state broadcast queued");
            },
            Ok(false) => {
                tracing::debug!(origin = %broadcast.origin_site, "state broadcast rejected (stale or duplicate)");
            },
            Err(e) => tracing::warn!(error = %e, "foca add_broadcast failed"),
        }
        Ok(())
    }

    /// Return the current merged CRDT grid-state snapshot.
    ///
    /// The snapshot is updated each time a [`StateBroadcast`] is received from
    /// a peer.  Reading is non-blocking — the value is cloned from a watch channel
    /// maintained by the internal [`StateBroadcastHandler`].
    #[must_use]
    pub fn state_snapshot(&self) -> GridStateSnapshot {
        self.state_rx.borrow().clone()
    }

    /// Return the current gateway address map from all received broadcasts.
    ///
    /// Keyed by origin site name.  Updated whenever a broadcast carrying a
    /// gateway address extension is received from a peer.
    #[must_use]
    pub fn gateway_addrs(&self) -> BTreeMap<String, String> {
        self.gateway_addrs_rx.borrow().clone()
    }

    /// Return the current public site certificate PEM map from all received broadcasts.
    ///
    /// Keyed by origin site name.  Contains only public certificate material —
    /// never private keys.  Updated whenever a broadcast carrying a
    /// `site_cert_pem` extension is received from a peer.
    #[must_use]
    pub fn cert_pems(&self) -> BTreeMap<String, String> {
        self.cert_pems_rx.borrow().clone()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crdt::{Capability, GridStateSnapshot, ProviderMetricsSnapshot, ProviderPhase, ProviderState};
    use tokio::sync::mpsc;

    use super::*;
    use crate::state_broadcast::StateBroadcastError;

    // -----------------------------------------------------------------------
    // Test utilities
    // -----------------------------------------------------------------------

    fn local_id(site: &str, port: u16) -> NodeId {
        NodeId::new(
            site.to_owned(),
            format!("127.0.0.1:{port}")
                .parse()
                .unwrap_or_else(|_| std::process::abort()),
        )
    }

    fn make_node(site: &str, port: u16) -> (SwimNode, mpsc::Receiver<MemberEvent>) {
        let (tx, rx) = mpsc::channel(16);
        (SwimNode::new(local_id(site, port), tx), rx)
    }

    fn provider_snap(site: &str, queue: f64) -> GridStateSnapshot {
        let mut snap = GridStateSnapshot::new(site.to_owned());
        snap.add_capability(Capability::Model("model-x".to_owned()));
        snap.upsert_provider(ProviderState {
            network_id: "net".to_owned(),
            site_id: site.to_owned(),
            provider_id: "provider-1".to_owned(),
            routing_cluster: site.to_owned(),
            models: vec!["model-x".to_owned()],
            backend_kind: "local".to_owned(),
            phase: ProviderPhase::Available,
            metrics: ProviderMetricsSnapshot {
                queue_depth: Some(queue),
                ..Default::default()
            },
            access_policy: crdt::ProviderAccessPolicy::default(),
            revision: 1,
            writer_id: site.to_owned(),
        });
        snap
    }

    // -----------------------------------------------------------------------
    // Basic node construction
    // -----------------------------------------------------------------------

    #[test]
    fn new_creates_node_without_panic() {
        let (tx, _rx) = mpsc::channel(1);
        let _node = SwimNode::new(local_id("test", 19_101), tx);
    }

    #[test]
    fn initial_state_snapshot_is_empty() {
        let (node, _) = make_node("site-a", 19_102);
        let snap = node.state_snapshot();
        assert!(
            snap.capabilities.is_empty(),
            "initial snapshot must have no capabilities"
        );
        assert!(snap.providers.is_empty(), "initial snapshot must have no providers");
    }

    #[test]
    fn handle_data_with_garbage_produces_no_messages() {
        let (mut node, _) = make_node("site-a", 19_103);
        let output = node.handle_data(b"not-swim-data");
        assert!(
            output.messages.is_empty(),
            "garbage data must produce no outbound messages"
        );
    }

    #[test]
    fn handle_timer_with_unrecognised_variant_is_noop() {
        let (mut node, _) = make_node("site-a", 19_104);
        let output = node.handle_timer(TimerEvent::PeriodicAnnounce);
        assert!(output.is_empty(), "non-Token timer must produce no output");
    }

    // -----------------------------------------------------------------------
    // Broadcast publishing
    // -----------------------------------------------------------------------

    #[test]
    fn publish_state_broadcast_does_not_error() {
        let (mut node, _) = make_node("site-a", 19_105);
        let snap = provider_snap("site-a", 0.2);
        let bc = StateBroadcast::new("site-a".to_owned(), 1, snap, None);
        node.publish_state_broadcast(&bc)
            .unwrap_or_else(|_| std::process::abort());
    }

    // -----------------------------------------------------------------------
    // Real foca CRDT broadcast propagation
    // -----------------------------------------------------------------------

    /// Exchange JOIN/ALIVE to establish bidirectional SWIM membership between two nodes.
    ///
    /// Returns a) everything A generated during the exchange so callers can continue
    /// processing timers, and b) the most recent set of messages A received from B.
    fn establish_membership(
        node_a: &mut SwimNode,
        node_b: &mut SwimNode,
        id_a: &NodeId,
        id_b: &NodeId,
    ) -> (AccumulatedOutput, Vec<crate::runtime::OutboundMessage>) {
        // A announces to B.
        let out_a = node_a.announce(id_b.clone());

        // B processes A's announce (receives JOIN, sends ALIVE back).
        let mut from_b: Vec<crate::runtime::OutboundMessage> = Vec::new();
        for msg in &out_a.messages {
            let ob = node_b.handle_data(&msg.data);
            from_b.extend(ob.messages);
        }

        // A processes B's responses (receives ALIVE — B is now in A's member list).
        for msg in &from_b {
            if msg.addr == id_a.socket_addr() {
                let oa = node_a.handle_data(&msg.data);
                // Pass any A→B follow-ups to B (acknowledgements etc.)
                for m in &oa.messages {
                    if m.addr == id_b.socket_addr() {
                        drop(node_b.handle_data(&m.data));
                    }
                }
            }
        }
        (out_a, from_b)
    }

    /// Prove that foca carries the CRDT state payload to a peer via gossip.
    ///
    /// Flow:
    /// 1. Establish bidirectional SWIM membership (announce + ALIVE exchange).
    /// 2. A publishes a `StateBroadcast` (queued in foca's custom broadcast backlog).
    /// 3. A calls `gossip()` — foca includes queued broadcasts in the gossip message.
    /// 4. B processes the gossip message → `StateBroadcastHandler::receive_item` fires.
    /// 5. B's `state_snapshot()` reflects A's CRDT state.
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "multi-step gossip broadcast proof: membership + publish + gossip + receive"
    )]
    fn crdt_state_propagates_to_peer_via_foca_gossip_broadcast() {
        let id_a = local_id("site-a", 19_201);
        let id_b = local_id("site-b", 19_202);
        let (mut node_a, _) = make_node("site-a", 19_201);
        let (mut node_b, _) = make_node("site-b", 19_202);

        // Step 1: establish membership so A knows B and gossip will target B.
        establish_membership(&mut node_a, &mut node_b, &id_a, &id_b);

        // Step 2: queue a CRDT broadcast on A.
        let bc = StateBroadcast::new("site-a".to_owned(), 1, provider_snap("site-a", 0.2), None);
        node_a
            .publish_state_broadcast(&bc)
            .unwrap_or_else(|_| std::process::abort());

        // Step 3: gossip from A — the pending broadcast is piggybacked.
        let out_gossip = node_a.gossip();
        assert!(
            !out_gossip.messages.is_empty(),
            "gossip must produce outbound messages when B is known"
        );

        // Step 4: B processes A's gossip messages.
        for msg in &out_gossip.messages {
            if msg.addr == id_b.socket_addr() {
                drop(node_b.handle_data(&msg.data));
            }
        }

        // Step 5: verify B has A's CRDT state.
        let b_snap = node_b.state_snapshot();
        assert!(
            b_snap.provider("net", "site-a", "provider-1").is_some(),
            "B must receive A's provider state via SWIM custom gossip broadcast"
        );
        let received = b_snap
            .provider("net", "site-a", "provider-1")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            received.metrics.queue_depth,
            Some(0.2),
            "B must receive the correct queue depth from A's CRDT state"
        );
        assert!(
            !b_snap.capabilities.is_empty(),
            "B must receive A's capabilities via SWIM custom gossip broadcast"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "sends two broadcasts (rev=2 then rev=1) and verifies stale rejection at receiver"
    )]
    fn stale_broadcast_does_not_overwrite_newer_state_at_receiver() {
        let id_a = local_id("site-a", 19_203);
        let id_b = local_id("site-b", 19_204);
        let (mut node_a, _) = make_node("site-a", 19_203);
        let (mut node_b, _) = make_node("site-b", 19_204);
        establish_membership(&mut node_a, &mut node_b, &id_a, &id_b);

        // Send rev=2 (newer).
        node_a
            .publish_state_broadcast(&StateBroadcast::new(
                "site-a".to_owned(),
                2,
                provider_snap("site-a", 0.1),
                None,
            ))
            .unwrap_or_else(|_| std::process::abort());
        for msg in &node_a.gossip().messages {
            if msg.addr == id_b.socket_addr() {
                drop(node_b.handle_data(&msg.data));
            }
        }
        assert_eq!(
            node_b
                .state_snapshot()
                .provider("net", "site-a", "provider-1")
                .map(|p| p.metrics.queue_depth),
            Some(Some(0.1)),
            "B should have queue_depth=0.1 from rev=2"
        );

        // Send rev=1 (stale) — B must reject it.
        node_a
            .publish_state_broadcast(&StateBroadcast::new(
                "site-a".to_owned(),
                1,
                provider_snap("site-a", 0.9),
                None,
            ))
            .unwrap_or_else(|_| std::process::abort());
        for msg in &node_a.gossip().messages {
            if msg.addr == id_b.socket_addr() {
                drop(node_b.handle_data(&msg.data));
            }
        }
        assert_eq!(
            node_b
                .state_snapshot()
                .provider("net", "site-a", "provider-1")
                .map(|p| p.metrics.queue_depth),
            Some(Some(0.1)),
            "stale rev=1 must not overwrite newer rev=2 state"
        );
    }

    #[test]
    fn malformed_broadcast_does_not_panic_or_corrupt_state() {
        let id_a = local_id("site-a", 19_205);
        let id_b = local_id("site-b", 19_206);
        let (mut node_a, _) = make_node("site-a", 19_205);
        let (mut node_b, _) = make_node("site-b", 19_206);
        establish_membership(&mut node_a, &mut node_b, &id_a, &id_b);

        // Send a valid broadcast so B has some state.
        node_a
            .publish_state_broadcast(&StateBroadcast::new(
                "site-a".to_owned(),
                1,
                provider_snap("site-a", 0.3),
                None,
            ))
            .unwrap_or_else(|_| std::process::abort());
        for msg in &node_a.gossip().messages {
            if msg.addr == id_b.socket_addr() {
                drop(node_b.handle_data(&msg.data));
            }
        }
        let before = node_b.state_snapshot();

        // Feed a garbage packet — foca parses it, the handler decode fails gracefully.
        drop(node_b.handle_data(b"totally-invalid-foca-packet-garbage"));

        let after = node_b.state_snapshot();
        assert_eq!(
            before.providers.len(),
            after.providers.len(),
            "malformed packet must not corrupt B's CRDT state"
        );
    }

    #[test]
    fn gateway_address_propagates_to_peer_via_gossip_broadcast() {
        let id_a = local_id("site-a", 19_210);
        let id_b = local_id("site-b", 19_211);
        let (mut node_a, _) = make_node("site-a", 19_210);
        let (mut node_b, _) = make_node("site-b", 19_211);
        establish_membership(&mut node_a, &mut node_b, &id_a, &id_b);

        node_a
            .publish_state_broadcast(&StateBroadcast::new(
                "site-a".to_owned(),
                1,
                GridStateSnapshot::new("site-a".to_owned()),
                Some("10.0.0.2:19080".to_owned()),
            ))
            .unwrap_or_else(|_| std::process::abort());

        for msg in &node_a.gossip().messages {
            if msg.addr == id_b.socket_addr() {
                drop(node_b.handle_data(&msg.data));
            }
        }

        assert_eq!(
            node_b.gateway_addrs().get("site-a").map(String::as_str),
            Some("10.0.0.2:19080"),
            "B must receive A's gateway address via SWIM custom broadcast"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "establishes two independent memberships (A–C and B–C), gossips from each, verifies merged state"
    )]
    fn two_independent_origins_merge_correctly_at_receiver() {
        let id_a = local_id("site-a", 19_207);
        let id_b = local_id("site-b", 19_208);
        let id_c = local_id("site-c", 19_209);
        let (mut node_a, _) = make_node("site-a", 19_207);
        let (mut node_b, _) = make_node("site-b", 19_208);
        let (mut node_c, _) = make_node("site-c", 19_209);

        // Establish A–C and B–C membership.
        establish_membership(&mut node_a, &mut node_c, &id_a, &id_c);
        establish_membership(&mut node_b, &mut node_c, &id_b, &id_c);

        // A gossips its state to C.
        node_a
            .publish_state_broadcast(&StateBroadcast::new(
                "site-a".to_owned(),
                1,
                provider_snap("site-a", 0.2),
                None,
            ))
            .unwrap_or_else(|_| std::process::abort());
        for msg in &node_a.gossip().messages {
            if msg.addr == id_c.socket_addr() {
                drop(node_c.handle_data(&msg.data));
            }
        }

        // B gossips its state to C.
        node_b
            .publish_state_broadcast(&StateBroadcast::new(
                "site-b".to_owned(),
                1,
                provider_snap("site-b", 0.8),
                None,
            ))
            .unwrap_or_else(|_| std::process::abort());
        for msg in &node_b.gossip().messages {
            if msg.addr == id_c.socket_addr() {
                drop(node_c.handle_data(&msg.data));
            }
        }

        let c_snap = node_c.state_snapshot();
        assert!(
            c_snap.provider("net", "site-a", "provider-1").is_some(),
            "C must have received A's state via SWIM gossip broadcast"
        );
        assert!(
            c_snap.provider("net", "site-b", "provider-1").is_some(),
            "C must have received B's state via SWIM gossip broadcast"
        );
    }

    // -----------------------------------------------------------------------
    // StateBroadcastError display
    // -----------------------------------------------------------------------

    #[test]
    fn state_broadcast_error_formats_correctly() {
        let err = StateBroadcastError::UnsupportedVersion {
            expected: 1,
            actual: 99,
        };
        let msg = err.to_string();
        assert!(msg.contains("99"), "error message must include the actual version");
    }
}
