//! High-level SWIM node wrapping a [`foca::Foca`] instance.
//!
//! [`SwimNode`] encapsulates foca internals — codec, RNG, broadcast handler,
//! and the [`GridRuntime`] adapter — so callers interact only with Grid-specific
//! types: [`AccumulatedOutput`], [`MemberEvent`], and `GridStateSnapshot`.
//!
//! The runtime is **not** thread-safe; run the node from a single task and pass
//! only [`AccumulatedOutput`] across task boundaries.

use std::{
    collections::BTreeMap,
    net::{Ipv6Addr, SocketAddr, SocketAddrV6},
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use crdt::GridStateSnapshot;
use rand::{SeedableRng as _, rngs::SmallRng};
use tokio::sync::watch;

use crate::{
    AccumulatedOutput, GridRuntime, NodeId,
    runtime::TimerEvent,
    state_broadcast::{
        DEFAULT_MAX_RETAINED_ORIGINS, MAX_PINNED_KEYS_PER_ORIGIN, OriginStateHandle, StateBroadcast,
        StateBroadcastHandler, TooManyPinnedKeys, TrustStore,
    },
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
// Public types and constants
// ---------------------------------------------------------------------------

/// Maximum complete datagram emitted by the SWIM transport.
pub const MAX_SWIM_PACKET_BYTES: usize = 1_400;

/// Maximum Kubernetes DNS-subdomain length used for a site identity.
const MAX_SITE_NAME_BYTES: usize = 253;

/// Network-order length prefix foca adds before each custom broadcast.
const CUSTOM_BROADCAST_LENGTH_PREFIX_BYTES: usize = 2;

/// Failure to encode or queue a state broadcast.
#[derive(Debug, thiserror::Error)]
pub enum PublishStateBroadcastError {
    /// Bincode could not encode the state payload.
    #[error("failed to encode state broadcast: {0}")]
    Encode(#[from] bincode::error::EncodeError),
    /// Foca rejected the encoded payload before queueing it.
    #[error("failed to queue state broadcast: {0}")]
    Queue(#[from] foca::Error),
    /// The payload cannot fit beside a worst-case SWIM broadcast header.
    #[error("state broadcast is {actual_bytes} bytes, exceeding safe SWIM payload budget of {maximum_bytes} bytes")]
    TooLarge {
        /// Encoded payload length.
        actual_bytes: usize,
        /// Maximum payload length safe for every valid site identity.
        maximum_bytes: usize,
    },
}

/// Compute the safe payload budget for a SWIM state broadcast.
///
/// `local_id` must be the identity of the publishing node. The calculation
/// reserves space for that source identity, a worst-case destination identity,
/// the foca broadcast header, and its custom-item length prefix. A payload
/// within this bound can be emitted even when ordinary piggyback messages lack
/// enough remaining space.
///
/// # Errors
///
/// Returns an encode error if the synthetic header cannot be serialized with
/// the configured foca codec.
pub fn state_broadcast_byte_budget(local_id: &NodeId) -> Result<usize, bincode::error::EncodeError> {
    let worst_case_address = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, u16::MAX, u32::MAX, u32::MAX));
    let worst_case_dst = NodeId::with_generation("x".repeat(MAX_SITE_NAME_BYTES), worst_case_address, u64::MAX);
    let header = foca::Header {
        src: local_id.clone(),
        src_incarnation: u16::MAX,
        dst: worst_case_dst,
        message: foca::Message::Broadcast,
    };
    let header_length = bincode::serde::encode_to_vec(header, bincode::config::standard())?.len();
    Ok(MAX_SWIM_PACKET_BYTES
        .saturating_sub(header_length)
        .saturating_sub(CUSTOM_BROADCAST_LENGTH_PREFIX_BYTES))
}

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

    /// Watch receiver for the signals address map, keyed by origin site.
    signals_addrs_rx: watch::Receiver<BTreeMap<String, String>>,

    /// Immediate control path for coordinated per-origin state eviction.
    origin_state: OriginStateHandle,

    /// Sender for the pinned-identity trust store read by the broadcast
    /// handler inside `foca`. See [`SwimNode::pin_origin`].
    trust_store_tx: watch::Sender<TrustStore>,

    /// Whether data claiming this node's identity has been warned about once.
    warned_self_data: bool,
}

impl SwimNode {
    /// Create a new SWIM node with the given identity.
    ///
    /// Uses foca's WAN configuration with periodic announce, down-member
    /// recovery, and gossip enabled.
    ///
    /// Membership events are available in [`AccumulatedOutput::events`]
    /// after each foca interaction.
    pub fn new(identity: NodeId) -> Self {
        Self::with_origin_capacity(identity, DEFAULT_MAX_RETAINED_ORIGINS)
    }

    /// Create a node with an explicit hard bound for retained state origins.
    #[expect(clippy::too_many_lines, reason = "seed + foca setup is one logical step")]
    pub fn with_origin_capacity(identity: NodeId, max_origins: usize) -> Self {
        let seed = {
            // Truncate nanoseconds to u64; we want spread, not precision.
            #[expect(
                clippy::cast_possible_truncation,
                reason = "intentional truncation for entropy mixing"
            )]
            #[expect(
                clippy::as_conversions,
                reason = "u128 -> u64 truncation is intentional for entropy mixing"
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
        let (handler, origin_state) = StateBroadcastHandler::with_capacity(site_id, max_origins);
        let state_rx = handler.subscribe();
        let gateway_addrs_rx = handler.subscribe_gateway_addrs();
        let cert_pems_rx = handler.subscribe_cert_pems();
        let signals_addrs_rx = handler.subscribe_signals_addrs();
        let trust_store_tx = handler.trust_store_sender();

        Self {
            foca: foca::Foca::with_custom_broadcast(identity, grid_config(), rng, codec, handler),
            runtime: GridRuntime::new(),
            state_rx,
            gateway_addrs_rx,
            cert_pems_rx,
            signals_addrs_rx,
            origin_state,
            trust_store_tx,
            warned_self_data: false,
        }
    }

    /// Immediately remove provider, metadata, and revision state for one origin.
    pub fn evict_origin(&self, origin: &str) {
        self.origin_state.remove_origin(origin);
    }

    /// Identities foca currently holds as active, suspect ones included.
    pub fn live_identities(&self) -> impl Iterator<Item = &NodeId> {
        self.foca.iter_members().map(foca::Member::id)
    }

    /// Announce that this node is leaving.
    pub fn leave(&mut self) -> AccumulatedOutput {
        if let Err(err) = self.foca.leave_cluster(&mut self.runtime) {
            tracing::warn!(error = %err, "foca leave error");
        }
        self.runtime.take_output()
    }

    /// Pin `origin` to a bounded set of accepted raw ECDSA P-256 public keys.
    ///
    /// If `origin` has no existing pin (or its existing pin is empty),
    /// immediately purges any state already merged from that origin via
    /// [`evict_origin`](Self::evict_origin) before installing the new pin —
    /// so state accepted from `origin` while it was unauthenticated cannot
    /// silently remain trusted once signature enforcement begins for it.
    /// Updating an *existing* non-empty pin (e.g. adding a next key during
    /// rotation, or dropping a retired one) does not purge state, since
    /// that update never crosses the unauthenticated-to-authenticated
    /// boundary.
    ///
    /// # Errors
    ///
    /// Returns [`TooManyPinnedKeys`] if `keys` has more than
    /// [`MAX_PINNED_KEYS_PER_ORIGIN`] entries, without changing the trust
    /// store.
    pub fn pin_origin(&self, origin: String, keys: Vec<Vec<u8>>) -> Result<(), TooManyPinnedKeys> {
        if keys.len() > MAX_PINNED_KEYS_PER_ORIGIN {
            return Err(TooManyPinnedKeys {
                origin,
                supplied: keys.len(),
            });
        }
        let was_unpinned = self.trust_store_tx.borrow().get(&origin).is_none_or(Vec::is_empty);
        if was_unpinned {
            self.evict_origin(&origin);
        }
        self.trust_store_tx.send_modify(|store| {
            store.insert(origin, keys);
        });
        Ok(())
    }

    /// Remove `origin`'s pin, returning it to unenforced (pass-through) status.
    ///
    /// Does not purge `origin`'s currently merged state — unpinning is a
    /// deliberate relaxation, not a security event, and the state was
    /// already accepted under whatever enforcement applied when it arrived.
    pub fn unpin_origin(&self, origin: &str) {
        self.trust_store_tx.send_modify(|store| {
            store.remove(origin);
        });
    }

    /// Feed an incoming UDP packet to foca.
    ///
    /// Returns accumulated side effects: outbound messages, scheduled timers,
    /// membership events, and any CRDT state broadcast payloads received.
    /// Protocol errors are logged at `warn` level, and data carrying this node's identity
    /// warns once, then logs at `debug`; neither aborts the output.
    pub fn handle_data(&mut self, data: &[u8]) -> AccumulatedOutput {
        match self.foca.handle_data(data, &mut self.runtime) {
            Ok(()) => {},
            // Self-seeds are filtered, so this is most likely two sites sharing one SWIM identity.
            Err(foca::Error::DataFromOurselves) if !self.warned_self_data => {
                self.warned_self_data = true;
                tracing::warn!(
                    len = data.len(),
                    "foca received data carrying this node's identity; check that no two sites share a SWIM identity or address"
                );
            },
            Err(foca::Error::DataFromOurselves) => {
                tracing::debug!(len = data.len(), "foca ignored data from this node");
            },
            Err(err) => tracing::warn!(error = %err, len = data.len(), "foca handle_data error"),
        }
        self.runtime.take_output()
    }

    /// Deliver a scheduled timer event to foca.
    ///
    /// Only [`TimerEvent::Token`] events are forwarded; others are silently
    /// ignored.
    pub fn handle_timer(&mut self, event: TimerEvent) -> AccumulatedOutput {
        if let TimerEvent::Token(token) = event
            && let Err(err) = self.foca.handle_timer(token, &mut self.runtime)
        {
            tracing::warn!(error = %err, "foca handle_timer error");
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
        if let Err(err) = self.foca.announce(dst, &mut self.runtime) {
            tracing::warn!(error = %err, "foca announce error");
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
        if let Err(err) = self.foca.gossip(&mut self.runtime) {
            tracing::warn!(error = %err, "foca gossip error");
        }
        self.runtime.take_output()
    }

    /// Send queued custom broadcasts to all available broadcast candidates.
    ///
    /// Unlike a general gossip round, this uses foca's dedicated custom
    /// broadcast path. It is used for retained state repair so a healthy peer
    /// cannot remain permanently missing an origin's provider state.
    pub fn broadcast(&mut self) -> AccumulatedOutput {
        if let Err(err) = self.foca.broadcast(&mut self.runtime) {
            tracing::warn!(error = %err, "foca broadcast error");
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
    /// Returns an error if the payload cannot be encoded or if foca rejects
    /// it before queueing, including when it exceeds the safe payload returned
    /// by [`state_broadcast_byte_budget`].
    pub fn publish_state_broadcast(&mut self, broadcast: &StateBroadcast) -> Result<(), PublishStateBroadcastError> {
        let bytes = broadcast.encode()?;
        let maximum_bytes = state_broadcast_byte_budget(self.foca.identity())?;
        if bytes.len() > maximum_bytes {
            return Err(PublishStateBroadcastError::TooLarge {
                actual_bytes: bytes.len(),
                maximum_bytes,
            });
        }
        match self.foca.add_broadcast(&bytes) {
            Ok(true) => {
                tracing::debug!(origin = %broadcast.origin_site, rev = broadcast.revision, "state broadcast queued");
            },
            Ok(false) => {
                tracing::debug!(origin = %broadcast.origin_site, "state broadcast rejected (stale or duplicate)");
            },
            Err(error) => {
                tracing::warn!(error = %error, bytes = bytes.len(), "foca add_broadcast failed");
                return Err(PublishStateBroadcastError::Queue(error));
            },
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

    /// Run `read` over the gateway, certificate, and signals maps without cloning them.
    pub fn with_peer_metadata<R, Read>(&self, read: Read) -> R
    where
        Read: FnOnce(&BTreeMap<String, String>, &BTreeMap<String, String>, &BTreeMap<String, String>) -> R,
    {
        read(
            &self.gateway_addrs_rx.borrow(),
            &self.cert_pems_rx.borrow(),
            &self.signals_addrs_rx.borrow(),
        )
    }

    /// Run `read` over the merged grid state without cloning it.
    pub fn with_state<R, Read: FnOnce(&GridStateSnapshot) -> R>(&self, read: Read) -> R {
        read(&self.state_rx.borrow())
    }

    /// Return the signals address each peer advertised, keyed by origin site.
    #[must_use]
    pub fn signals_addrs(&self) -> BTreeMap<String, String> {
        self.signals_addrs_rx.borrow().clone()
    }
}

// ---------------------------------------------------------------------------
// Foca configuration
// ---------------------------------------------------------------------------

/// Foca WAN configuration for cross-site SWIM.
///
/// Three is the minimum supported multi-site Grid topology. Capacity-specific
/// tuning belongs in an explicit operator configuration rather than an
/// assumed cluster size compiled into the transport.
fn grid_config() -> foca::Config {
    let expected_sites = NonZeroU32::new(3).unwrap_or(NonZeroU32::MIN);
    let mut config = foca::Config::new_wan(expected_sites);
    config.max_packet_size = NonZeroUsize::new(MAX_SWIM_PACKET_BYTES).unwrap_or(NonZeroUsize::MIN);
    config
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crdt::{Capability, ProviderMetricsSnapshot, ProviderPhase, ProviderState};

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

    fn make_node(site: &str, port: u16) -> (SwimNode, ()) {
        (SwimNode::new(local_id(site, port)), ())
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
            tools: Vec::new(),
            backend_kind: "local".to_owned(),
            capacity_weight: 1,
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
    fn grid_config_uses_foca_wan_profile() {
        let config = grid_config();
        assert_eq!(config.probe_period, Duration::from_secs(5));
        assert_eq!(config.probe_rtt, Duration::from_secs(3));
        assert_eq!(config.suspect_to_down_after, Duration::from_secs(30));
        assert!(config.periodic_announce.is_some());
        assert!(config.periodic_announce_to_down_members.is_some());
        assert!(config.periodic_gossip.is_some());
        assert!(
            config.notify_down_members,
            "notify_down_members enables auto-rejoin with new generation"
        );
    }

    #[test]
    fn new_creates_node_without_panic() {
        let _node = SwimNode::new(local_id("test", 19_101));
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

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "boundary proof finds both catalogs, exercises foca delivery, and checks the rejection"
    )]
    fn publish_state_broadcast_accepts_the_largest_fitting_catalog_and_rejects_the_next() {
        let id = local_id("site-a", 7946);
        let byte_budget = state_broadcast_byte_budget(&id).unwrap_or_else(|_| std::process::abort());
        let candidates = (1_u16..=128)
            .map(|tool_count| {
                let mut snap = provider_snap("site-a", 0.2);
                snap.providers.values_mut().for_each(|provider| {
                    provider.tools = (0_u16..tool_count)
                        .map(|index| format!("tool-{index:03}-{}", "x".repeat(48)))
                        .collect();
                });
                StateBroadcast::new("site-a".to_owned(), 1, snap, None)
            })
            .collect::<Vec<_>>();
        let fitting = candidates
            .iter()
            .rfind(|broadcast| broadcast.encode().is_ok_and(|bytes| bytes.len() <= byte_budget))
            .unwrap_or_else(|| std::process::abort());
        let overflowing = candidates
            .iter()
            .find(|broadcast| broadcast.encode().is_ok_and(|bytes| bytes.len() > byte_budget))
            .unwrap_or_else(|| std::process::abort());
        let fitting_length = fitting.encode().map_or(usize::MAX, |bytes| bytes.len());
        let overflowing_length = overflowing.encode().map_or(0, |bytes| bytes.len());
        let fitting_tool_count = fitting
            .snapshot
            .provider("net", "site-a", "provider-1")
            .map_or(0, |provider| provider.tools.len());
        let id_a = local_id("site-a", 19_106);
        let id_b = local_id("site-b", 19_107);
        let (mut accepted_node, _) = make_node("site-a", 19_106);
        let (mut receiving_node, _) = make_node("site-b", 19_107);
        let (mut rejecting_node, _) = make_node("site-a", 19_108);
        drop(establish_membership(
            &mut accepted_node,
            &mut receiving_node,
            &id_a,
            &id_b,
        ));

        accepted_node
            .publish_state_broadcast(fitting)
            .unwrap_or_else(|_| std::process::abort());
        let outbound = accepted_node.broadcast();
        for message in &outbound.messages {
            if message.addr == id_b.socket_addr() {
                drop(receiving_node.handle_data(&message.data));
            }
        }
        let rejected = rejecting_node.publish_state_broadcast(overflowing);

        assert!(
            fitting_length <= byte_budget,
            "the accepted catalog must fit the configured packet budget"
        );
        assert!(
            overflowing_length > byte_budget,
            "the rejected catalog must exceed the configured packet budget"
        );
        assert!(
            matches!(
                rejected,
                Err(PublishStateBroadcastError::TooLarge {
                    actual_bytes,
                    maximum_bytes,
                }) if actual_bytes == overflowing_length && maximum_bytes == byte_budget
            ),
            "an oversized catalog must surface its exact byte-budget error, got {rejected:?}"
        );
        assert!(
            outbound
                .messages
                .iter()
                .any(|message| { message.addr == id_b.socket_addr() && message.data.len() <= MAX_SWIM_PACKET_BYTES }),
            "the largest fitting catalog must be emitted in a bounded dedicated broadcast"
        );
        assert_eq!(
            receiving_node
                .state_snapshot()
                .provider("net", "site-a", "provider-1")
                .map_or(0, |provider| provider.tools.len()),
            fitting_tool_count,
            "the receiving peer must recover every tool in the largest fitting catalog"
        );
    }

    // -----------------------------------------------------------------------
    // Trust-store pinning
    // -----------------------------------------------------------------------

    /// Generate an ECDSA P-256 signing key plus the raw SPKI EC point a
    /// verifier needs, independent of *how* a real deployment would source
    /// or pin this key material (grid#75, still open).
    fn generate_signing_key_and_pubkey() -> (Vec<u8>, Vec<u8>) {
        let key_pair = rcgen::KeyPair::generate().unwrap_or_else(|_| std::process::abort());
        let pkcs8_der = key_pair.serialize_der();
        let params = rcgen::CertificateParams::new(vec!["spike.grid.internal".to_owned()])
            .unwrap_or_else(|_| std::process::abort());
        let cert = params.self_signed(&key_pair).unwrap_or_else(|_| std::process::abort());
        let (_, parsed) = x509_parser::parse_x509_certificate(cert.der()).unwrap_or_else(|_| std::process::abort());
        let raw_pubkey = parsed.public_key().subject_public_key.as_ref().to_vec();
        (pkcs8_der, raw_pubkey)
    }

    /// Return the current wall-clock time in milliseconds since the Unix
    /// epoch, for constructing test broadcasts with a fresh `signed_at_ms`.
    fn now_ms() -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_else(|_| std::process::abort())
                .as_millis(),
        )
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn pin_origin_rejects_more_than_the_max_pinned_keys() {
        let (node, _) = make_node("site-b", 19_230);

        let result = node.pin_origin("site-p".to_owned(), vec![vec![1], vec![2], vec![3]]);

        assert!(
            matches!(&result, Err(TooManyPinnedKeys { origin, supplied }) if origin == "site-p" && *supplied == 3),
            "pinning 3 keys (over MAX_PINNED_KEYS_PER_ORIGIN=2) must be rejected, got {result:?}"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "establishes membership, publishes unpinned state, then pins and re-checks purge in one proof"
    )]
    fn pin_origin_purges_previously_unauthenticated_state_from_that_origin() {
        let id_a = local_id("site-a", 19_231);
        let id_b = local_id("site-b", 19_232);
        let (mut node_a, _) = make_node("site-a", 19_231);
        let (mut node_b, _) = make_node("site-b", 19_232);
        establish_membership(&mut node_a, &mut node_b, &id_a, &id_b);

        // A publishes unsigned state; B has no pin for A yet, so it merges.
        node_a
            .publish_state_broadcast(&StateBroadcast::new(
                "site-a".to_owned(),
                1,
                provider_snap("site-a", 0.4),
                None,
            ))
            .unwrap_or_else(|_| std::process::abort());
        for msg in &node_a.gossip().messages {
            if msg.addr == id_b.socket_addr() {
                drop(node_b.handle_data(&msg.data));
            }
        }
        assert!(
            node_b
                .state_snapshot()
                .provider("net", "site-a", "provider-1")
                .is_some(),
            "B must have accepted A's unsigned, unpinned state"
        );

        let (_key, pubkey) = generate_signing_key_and_pubkey();
        node_b
            .pin_origin("site-a".to_owned(), vec![pubkey])
            .unwrap_or_else(|_| std::process::abort());

        assert!(
            node_b
                .state_snapshot()
                .provider("net", "site-a", "provider-1")
                .is_none(),
            "pinning a previously-unpinned origin for the first time must purge its unauthenticated state"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "establishes membership, pins, signs+publishes, and re-pins with a rotated key set in one proof"
    )]
    fn pin_origin_rotation_update_does_not_purge_already_authenticated_state() {
        let id_a = local_id("site-a", 19_233);
        let id_b = local_id("site-b", 19_234);
        let (mut node_a, _) = make_node("site-a", 19_233);
        let (mut node_b, _) = make_node("site-b", 19_234);
        establish_membership(&mut node_a, &mut node_b, &id_a, &id_b);

        let (signing_key, current_pubkey) = generate_signing_key_and_pubkey();
        node_b
            .pin_origin("site-a".to_owned(), vec![current_pubkey.clone()])
            .unwrap_or_else(|_| std::process::abort());

        let unsigned = StateBroadcast::new("site-a".to_owned(), 1, provider_snap("site-a", 0.5), None)
            .with_signed_at(Some(now_ms()));
        let signature = crate::signing::sign_ecdsa_p256(
            &signing_key,
            &unsigned.signable_bytes().unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        node_a
            .publish_state_broadcast(&unsigned.with_signature(Some(signature)))
            .unwrap_or_else(|_| std::process::abort());
        for msg in &node_a.gossip().messages {
            if msg.addr == id_b.socket_addr() {
                drop(node_b.handle_data(&msg.data));
            }
        }
        assert!(
            node_b
                .state_snapshot()
                .provider("net", "site-a", "provider-1")
                .is_some(),
            "B must have accepted A's correctly signed, pinned state"
        );

        // Rotation: add a next key alongside the current one. This is an
        // update to an existing non-empty pin, not a first-time pin.
        let (_next_key, next_pubkey) = generate_signing_key_and_pubkey();
        node_b
            .pin_origin("site-a".to_owned(), vec![current_pubkey, next_pubkey])
            .unwrap_or_else(|_| std::process::abort());

        assert!(
            node_b
                .state_snapshot()
                .provider("net", "site-a", "provider-1")
                .is_some(),
            "rotating an already-pinned origin's key set must not purge its already-authenticated state"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "establishes membership, publishes state, then unpins and re-checks retention in one proof"
    )]
    fn unpin_origin_does_not_purge_existing_state() {
        let id_a = local_id("site-a", 19_235);
        let id_b = local_id("site-b", 19_236);
        let (mut node_a, _) = make_node("site-a", 19_235);
        let (mut node_b, _) = make_node("site-b", 19_236);
        establish_membership(&mut node_a, &mut node_b, &id_a, &id_b);

        node_a
            .publish_state_broadcast(&StateBroadcast::new(
                "site-a".to_owned(),
                1,
                provider_snap("site-a", 0.6),
                None,
            ))
            .unwrap_or_else(|_| std::process::abort());
        for msg in &node_a.gossip().messages {
            if msg.addr == id_b.socket_addr() {
                drop(node_b.handle_data(&msg.data));
            }
        }
        assert!(
            node_b
                .state_snapshot()
                .provider("net", "site-a", "provider-1")
                .is_some()
        );

        node_b.unpin_origin("site-a");

        assert!(
            node_b
                .state_snapshot()
                .provider("net", "site-a", "provider-1")
                .is_some(),
            "unpinning an origin must not purge its currently merged state"
        );
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
                for followup in &oa.messages {
                    if followup.addr == id_b.socket_addr() {
                        drop(node_b.handle_data(&followup.data));
                    }
                }
            }
        }
        (out_a, from_b)
    }

    #[test]
    fn a_leaving_node_is_dropped_from_the_peer_live_identities() {
        let id_a = local_id("site-a", 19_240);
        let id_b = local_id("site-b", 19_241);
        let (mut node_a, _) = make_node("site-a", 19_240);
        let (mut node_b, _) = make_node("site-b", 19_241);
        establish_membership(&mut node_a, &mut node_b, &id_a, &id_b);
        let live = |node: &SwimNode| node.live_identities().any(|id| id.site_name() == "site-a");
        assert!(live(&node_b), "joined");

        let mut left = false;
        for msg in node_a.leave().messages {
            if msg.addr == id_b.socket_addr() {
                left |=
                    node_b.handle_data(&msg.data).events.iter().any(
                        |event| matches!(event, crate::MemberEvent::Left { site_name, .. } if site_name == "site-a"),
                    );
            }
        }
        assert!(left, "the peer saw the leave");
        assert!(!live(&node_b), "no longer live");
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
        reason = "three-node proof establishes membership, broadcasts once, and verifies both receivers"
    )]
    fn dedicated_broadcast_reaches_all_available_peers() {
        let id_a = local_id("site-a", 19_210);
        let id_b = local_id("site-b", 19_211);
        let id_c = local_id("site-c", 19_212);
        let (mut node_a, _) = make_node("site-a", 19_210);
        let (mut node_b, _) = make_node("site-b", 19_211);
        let (mut node_c, _) = make_node("site-c", 19_212);

        establish_membership(&mut node_a, &mut node_b, &id_a, &id_b);
        establish_membership(&mut node_a, &mut node_c, &id_a, &id_c);

        node_a
            .publish_state_broadcast(&StateBroadcast::new(
                "site-a".to_owned(),
                1,
                provider_snap("site-a", 0.2),
                None,
            ))
            .unwrap_or_else(|_| std::process::abort());

        let outbound = node_a.broadcast();
        let destinations: std::collections::BTreeSet<_> =
            outbound.messages.iter().map(|message| message.addr).collect();
        assert!(destinations.contains(&id_b.socket_addr()));
        assert!(destinations.contains(&id_c.socket_addr()));

        for message in &outbound.messages {
            if message.addr == id_b.socket_addr() {
                drop(node_b.handle_data(&message.data));
            } else if message.addr == id_c.socket_addr() {
                drop(node_c.handle_data(&message.data));
            }
        }

        assert!(
            node_b
                .state_snapshot()
                .provider("net", "site-a", "provider-1")
                .is_some()
        );
        assert!(
            node_c
                .state_snapshot()
                .provider("net", "site-a", "provider-1")
                .is_some()
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
                .map(|prov| prov.metrics.queue_depth),
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
                .map(|prov| prov.metrics.queue_depth),
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
    fn signals_address_propagates_to_peer_via_gossip_broadcast() {
        let id_a = local_id("site-a", 19_212);
        let id_b = local_id("site-b", 19_213);
        let (mut node_a, _) = make_node("site-a", 19_212);
        let (mut node_b, _) = make_node("site-b", 19_213);
        establish_membership(&mut node_a, &mut node_b, &id_a, &id_b);

        let broadcast = StateBroadcast::new(
            "site-a".to_owned(),
            1,
            GridStateSnapshot::new("site-a".to_owned()),
            None,
        )
        .with_signals_address(Some("[2001:db8::7]:9091".to_owned()));
        node_a
            .publish_state_broadcast(&broadcast)
            .unwrap_or_else(|_| std::process::abort());
        for msg in &node_a.gossip().messages {
            if msg.addr == id_b.socket_addr() {
                drop(node_b.handle_data(&msg.data));
            }
        }

        assert_eq!(
            node_b.signals_addrs().get("site-a").map(String::as_str),
            Some("[2001:db8::7]:9091")
        );
        assert!(node_b.gateway_addrs().is_empty(), "no gateway address was sent");
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
    // Tenant budget spend convergence (grid#40 AC3)
    // -----------------------------------------------------------------------

    /// Compute a spend increment in cents from a real product cost field
    /// ([`scoring::BackendConfig::cost_per_1k_input`], mirrored here without a
    /// crate dependency to keep `swim` free of the `scoring` crate) and a
    /// request's input token count — the same unit conversion
    /// `operator::crd::grid_network::spend_ratio` expects on the read side.
    fn cost_cents_for_request(cost_per_1k_input_usd: f64, input_tokens: u64) -> u64 {
        #[expect(
            clippy::as_conversions,
            clippy::cast_precision_loss,
            reason = "test-only cost simulation, not the production conversion path"
        )]
        let tokens = input_tokens as f64;
        let usd = cost_per_1k_input_usd * (tokens / 1000.0);
        #[expect(
            clippy::as_conversions,
            clippy::cast_sign_loss,
            clippy::cast_possible_truncation,
            reason = "usd is always non-negative in this test fixture"
        )]
        let cents = (usd * 100.0).round() as u64;
        cents
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "two-origin gossip proof establishes membership, broadcasts twice, and verifies convergence"
    )]
    fn tenant_spend_from_two_origin_sites_converges_at_third_via_gossip() {
        let id_a = local_id("site-a", 19_216);
        let id_b = local_id("site-b", 19_217);
        let id_c = local_id("site-c", 19_218);
        let (mut node_a, _) = make_node("site-a", 19_216);
        let (mut node_b, _) = make_node("site-b", 19_217);
        let (mut node_c, _) = make_node("site-c", 19_218);

        establish_membership(&mut node_a, &mut node_c, &id_a, &id_c);
        establish_membership(&mut node_b, &mut node_c, &id_b, &id_c);

        // Site A serves a request for tenant-acme against a $0.03/1k-input-token backend.
        let a_cents = cost_cents_for_request(0.03, 4_000);
        let mut a_snap = GridStateSnapshot::new("site-a".to_owned());
        a_snap.increment_tenant_spend("tenant-acme", a_cents);
        node_a
            .publish_state_broadcast(&StateBroadcast::new("site-a".to_owned(), 1, a_snap, None))
            .unwrap_or_else(|_| std::process::abort());
        for msg in &node_a.gossip().messages {
            if msg.addr == id_c.socket_addr() {
                drop(node_c.handle_data(&msg.data));
            }
        }

        // Site B independently serves a request for the same tenant against a pricier backend.
        let b_cents = cost_cents_for_request(0.06, 2_500);
        let mut b_snap = GridStateSnapshot::new("site-b".to_owned());
        b_snap.increment_tenant_spend("tenant-acme", b_cents);
        node_b
            .publish_state_broadcast(&StateBroadcast::new("site-b".to_owned(), 1, b_snap, None))
            .unwrap_or_else(|_| std::process::abort());
        for msg in &node_b.gossip().messages {
            if msg.addr == id_c.socket_addr() {
                drop(node_c.handle_data(&msg.data));
            }
        }

        let c_snap = node_c.state_snapshot();
        let converged_total = c_snap
            .tenant_spend
            .get("tenant-acme")
            .unwrap_or_else(|| std::process::abort())
            .total();
        assert_eq!(
            converged_total,
            a_cents + b_cents,
            "tenant spend from two independent origin sites must converge to the true sum \
             at a third site via real SWIM gossip broadcast, proving AC3 (cross-site convergence)"
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
