//! State snapshot payloads carried by SWIM custom broadcasts.
//!
//! Defines the wire envelope, the foca [`BroadcastHandler`] implementation,
//! and helper types for CRDT grid-state propagation over SWIM gossip.
//!
//! [`BroadcastHandler`]: foca::BroadcastHandler

use std::collections::BTreeMap;

use crdt::GridStateSnapshot;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::NodeId;

// ---------------------------------------------------------------------------
// State broadcast envelope
// ---------------------------------------------------------------------------

/// Wire-format version for [`StateBroadcast`].
///
/// Gateway address support is encoded as optional trailing extension data while
/// keeping this version unchanged.  Peers that only understand the base v1
/// payload decode the prefix and ignore the trailing bytes; newer peers decode
/// the extension when present.
pub const STATE_BROADCAST_VERSION_V1: u16 = 1;

/// Current wire-format version.
pub const STATE_BROADCAST_VERSION: u16 = STATE_BROADCAST_VERSION_V1;

/// Broadcast envelope carrying one CRDT grid-state snapshot.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StateBroadcast {
    /// Wire-format version.
    pub version: u16,

    /// Site that originated the broadcast.
    pub origin_site: String,

    /// Monotonic origin-local revision.
    pub revision: u64,

    /// Mergeable grid-state snapshot.
    pub snapshot: GridStateSnapshot,

    /// Data-plane gateway address advertised by this site.
    ///
    /// Carried as optional trailing extension data.  `None` when the originating
    /// operator has no configured gateway address.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_address: Option<String>,

    /// Public site certificate PEM advertised by this site.
    ///
    /// Contains only the public certificate (never a private key).  Used to
    /// populate `GridSite.status.publicCertPem` on the receiving operator.
    /// `None` when the originating operator has no TLS certificate configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub site_cert_pem: Option<String>,
}

/// Base wire-format struct.
///
/// New optional fields are appended after this base payload.  Older peers decode
/// the base payload and ignore trailing extension bytes.
#[derive(Serialize, Deserialize)]
struct StateBroadcastV1 {
    /// Wire-format version.
    version: u16,
    /// Site that originated the broadcast.
    origin_site: String,
    /// Monotonic origin-local revision.
    revision: u64,
    /// Mergeable grid-state snapshot.
    snapshot: GridStateSnapshot,
}

/// Extension data appended after the base v1 payload.
///
/// Encoded as a single serialized struct following the base payload.
/// Older peers that do not understand extensions decode only the base payload;
/// the trailing bytes are ignored by `bincode::serde::decode_from_slice`.
#[derive(Serialize, Deserialize)]
struct BroadcastExtension {
    /// Optional data-plane gateway address.
    gateway_address: Option<String>,
    /// Optional public site certificate PEM — never a private key.
    site_cert_pem: Option<String>,
}

impl StateBroadcast {
    /// Create a versioned state broadcast.
    ///
    /// Extensions (`gateway_address`, `site_cert_pem`) are appended as trailing
    /// data after the base v1 payload.  Older peers decode only the base payload;
    /// the trailing bytes are silently ignored.
    #[must_use]
    pub fn new(
        origin_site: String,
        revision: u64,
        snapshot: GridStateSnapshot,
        gateway_address: Option<String>,
    ) -> Self {
        Self {
            version: STATE_BROADCAST_VERSION,
            origin_site,
            revision,
            snapshot,
            gateway_address,
            site_cert_pem: None,
        }
    }

    /// Create a broadcast that also carries a public site certificate PEM.
    ///
    /// The certificate must be the public certificate only — never a private key.
    #[must_use]
    pub fn with_cert(mut self, site_cert_pem: Option<String>) -> Self {
        self.site_cert_pem = site_cert_pem;
        self
    }

    /// Return this broadcast's invalidation key.
    #[must_use]
    pub fn key(&self) -> StateBroadcastKey {
        StateBroadcastKey {
            origin_site: self.origin_site.clone(),
            revision: self.revision,
            kind: self.key_kind(),
        }
    }

    /// Return true when this payload only advertises side-channel metadata
    /// (gateway address and/or site cert PEM) with no CRDT state.
    #[must_use]
    fn is_metadata_only(&self) -> bool {
        self.snapshot.providers.is_empty() && self.snapshot.capabilities.is_empty()
    }

    /// Return true when this payload only advertises a gateway address.
    #[must_use]
    fn is_gateway_address_only(&self) -> bool {
        self.gateway_address.is_some() && self.site_cert_pem.is_none() && self.is_metadata_only()
    }

    /// Return true when this payload only carries site certificate PEM.
    #[must_use]
    fn is_cert_only(&self) -> bool {
        self.site_cert_pem.is_some() && self.gateway_address.is_none() && self.is_metadata_only()
    }

    /// Return the foca invalidation key kind for this payload.
    #[must_use]
    fn key_kind(&self) -> StateBroadcastKeyKind {
        if self.is_cert_only() {
            StateBroadcastKeyKind::Cert
        } else if self.is_gateway_address_only() {
            StateBroadcastKeyKind::GatewayAddress
        } else if self.is_metadata_only() {
            StateBroadcastKeyKind::Metadata
        } else {
            StateBroadcastKeyKind::State
        }
    }

    /// Encode this broadcast as bincode bytes.
    ///
    /// The base v1 payload is always encoded first.  When any extension field
    /// is present, a `BroadcastExtension` struct is appended as trailing data.
    /// Older peers decode only the base payload and ignore the extension bytes.
    ///
    /// # Errors
    ///
    /// Returns a bincode encode error if the snapshot cannot be serialized.
    pub fn encode(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        let v1 = StateBroadcastV1 {
            version: self.version,
            origin_site: self.origin_site.clone(),
            revision: self.revision,
            snapshot: self.snapshot.clone(),
        };
        let mut bytes = bincode::serde::encode_to_vec(&v1, bincode::config::standard())?;
        if self.gateway_address.is_some() || self.site_cert_pem.is_some() {
            let ext = BroadcastExtension {
                gateway_address: self.gateway_address.clone(),
                site_cert_pem: self.site_cert_pem.clone(),
            };
            let ext_bytes = bincode::serde::encode_to_vec(&ext, bincode::config::standard())?;
            bytes.extend_from_slice(&ext_bytes);
        }
        Ok(bytes)
    }

    /// Decode this broadcast from bincode bytes.
    ///
    /// Decodes the base v1 payload, then tries to decode any trailing bytes as
    /// a `BroadcastExtension` struct.  Falls back to the previous bare-`String`
    /// format for `gateway_address` when the struct decode fails, ensuring
    /// interoperability with older peers that use the first extension format.
    ///
    /// Payloads without any extension decode with `gateway_address = None` and
    /// `site_cert_pem = None`.
    ///
    /// # Errors
    ///
    /// Returns a bincode decode error if `bytes` is not a valid
    /// [`StateBroadcast`] payload.
    pub fn decode(bytes: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        let (v1, consumed): (StateBroadcastV1, usize) =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;

        let remaining = bytes.get(consumed..).unwrap_or(&[]);
        let (gateway_address, site_cert_pem) = if remaining.is_empty() {
            (None, None)
        } else {
            // Try the current extension struct format.
            match bincode::serde::decode_from_slice::<BroadcastExtension, _>(remaining, bincode::config::standard()) {
                Ok((ext, _)) => (ext.gateway_address, ext.site_cert_pem),
                Err(_) => {
                    // Compatibility fallback: bare String encoding for gateway_address only.
                    match bincode::serde::decode_from_slice::<String, _>(remaining, bincode::config::standard()) {
                        Ok((gw, _)) => (Some(gw), None),
                        Err(_) => (None, None),
                    }
                },
            }
        };

        Ok(Self {
            version: v1.version,
            origin_site: v1.origin_site,
            revision: v1.revision,
            snapshot: v1.snapshot,
            gateway_address,
            site_cert_pem,
        })
    }
}

/// Key used to replace stale queued broadcasts in foca.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateBroadcastKey {
    /// Site that originated the broadcast.
    pub origin_site: String,

    /// Monotonic origin-local revision.
    pub revision: u64,

    /// Independent invalidation lane.
    ///
    /// Gateway-address-only broadcasts must not invalidate provider/capability
    /// state broadcasts from the same origin.
    kind: StateBroadcastKeyKind,
}

/// Invalidation lane for SWIM state broadcasts.
#[derive(Clone, Debug, Eq, PartialEq)]
enum StateBroadcastKeyKind {
    /// Provider/capability CRDT state.
    State,

    /// Gateway-address-only side-channel update.
    GatewayAddress,

    /// Public site certificate PEM side-channel update.
    ///
    /// Cert broadcasts must not invalidate provider/capability state or
    /// gateway-address broadcasts from the same origin.
    Cert,

    /// Combined side-channel metadata update.
    ///
    /// Combined metadata broadcasts must not invalidate provider/capability
    /// state broadcasts from the same origin.
    Metadata,
}

impl foca::Invalidates for StateBroadcastKey {
    fn invalidates(&self, other: &Self) -> bool {
        self.origin_site == other.origin_site && self.kind == other.kind && self.revision >= other.revision
    }
}

// ---------------------------------------------------------------------------
// Broadcast handler
// ---------------------------------------------------------------------------

/// Errors produced while decoding state broadcasts.
#[derive(Debug, thiserror::Error)]
pub enum StateBroadcastError {
    /// The payload could not be decoded.
    #[error("state broadcast decode failed: {0}")]
    Decode(#[from] bincode::error::DecodeError),

    /// The payload version is not supported.
    #[error("unsupported state broadcast version {actual}; expected {expected}")]
    UnsupportedVersion {
        /// Expected version.
        expected: u16,
        /// Actual version.
        actual: u16,
    },
}

/// foca custom broadcast handler for CRDT grid-state snapshots.
///
/// Merges incoming [`StateBroadcast`] payloads into a shared
/// [`GridStateSnapshot`] that callers can observe via the watch receiver
/// returned by [`StateBroadcastHandler::subscribe`].
pub struct StateBroadcastHandler {
    /// Shared merged state — written here, read by all subscribers.
    state_tx: watch::Sender<GridStateSnapshot>,

    /// Highest revision received from each origin.
    latest_by_origin: BTreeMap<String, u64>,

    /// Gateway addresses received from each origin site.
    gateway_addrs: BTreeMap<String, String>,

    /// Watch channel for broadcasting gateway address updates to observers.
    ///
    /// Updated whenever a broadcast with a gateway address extension is received.
    /// Subscribers observe the full map keyed by origin site name.
    gateway_addrs_tx: watch::Sender<BTreeMap<String, String>>,

    /// Public site certificate PEMs received from each origin site.
    ///
    /// Contains only public certificate material — never private keys.
    cert_pems: BTreeMap<String, String>,

    /// Watch channel for broadcasting public cert PEM updates to observers.
    cert_pems_tx: watch::Sender<BTreeMap<String, String>>,
}

impl StateBroadcastHandler {
    /// Create a handler with an empty local state snapshot.
    ///
    /// Call [`subscribe`] before moving `self` into foca to obtain a
    /// [`watch::Receiver`] for reading the merged state.
    ///
    /// [`subscribe`]: StateBroadcastHandler::subscribe
    #[must_use]
    pub fn new(site_id: String) -> Self {
        let (tx, _) = watch::channel(GridStateSnapshot::new(site_id));
        let (gw_tx, _) = watch::channel(BTreeMap::new());
        let (cert_tx, _) = watch::channel(BTreeMap::new());
        Self {
            state_tx: tx,
            latest_by_origin: BTreeMap::new(),
            gateway_addrs: BTreeMap::new(),
            gateway_addrs_tx: gw_tx,
            cert_pems: BTreeMap::new(),
            cert_pems_tx: cert_tx,
        }
    }

    /// Return a receiver for the live merged grid-state snapshot.
    ///
    /// Create the receiver **before** moving `self` into foca.  Multiple
    /// receivers share the same underlying channel; each sees all updates.
    pub fn subscribe(&self) -> watch::Receiver<GridStateSnapshot> {
        self.state_tx.subscribe()
    }

    /// Return a receiver for the live gateway address map.
    ///
    /// Create the receiver **before** moving `self` into foca.  The map is
    /// keyed by origin site name and updated whenever a broadcast carrying a
    /// gateway address extension is received.
    pub fn subscribe_gateway_addrs(&self) -> watch::Receiver<BTreeMap<String, String>> {
        self.gateway_addrs_tx.subscribe()
    }

    /// Clone and return the currently merged grid-state snapshot.
    #[must_use]
    pub fn snapshot(&self) -> GridStateSnapshot {
        self.state_tx.borrow().clone()
    }

    /// Return the gateway address advertised by `site`, if any.
    #[must_use]
    pub fn gateway_address_for_site(&self, site: &str) -> Option<&str> {
        self.gateway_addrs.get(site).map(String::as_str)
    }

    /// Return a snapshot of all known gateway addresses, keyed by site name.
    #[must_use]
    pub fn gateway_addrs(&self) -> &BTreeMap<String, String> {
        &self.gateway_addrs
    }

    /// Return a receiver for the live public cert PEM map.
    ///
    /// Create the receiver **before** moving `self` into foca.
    pub fn subscribe_cert_pems(&self) -> watch::Receiver<BTreeMap<String, String>> {
        self.cert_pems_tx.subscribe()
    }

    /// Return the public site certificate PEM received from `site`, if any.
    ///
    /// The returned PEM is the public certificate only — never a private key.
    #[must_use]
    pub fn cert_pem_for_site(&self, site: &str) -> Option<&str> {
        self.cert_pems.get(site).map(String::as_str)
    }

    /// Return a snapshot of all known public cert PEMs, keyed by site name.
    #[must_use]
    pub fn cert_pems(&self) -> &BTreeMap<String, String> {
        &self.cert_pems
    }

    /// Store and publish the gateway address carried by a broadcast, if any.
    fn store_gateway_address(&mut self, broadcast: &StateBroadcast) {
        if let Some(gw) = &broadcast.gateway_address {
            self.gateway_addrs.insert(broadcast.origin_site.clone(), gw.clone());
            self.gateway_addrs_tx.send_modify(|m| {
                m.insert(broadcast.origin_site.clone(), gw.clone());
            });
        }
    }

    /// Store and publish the public site cert PEM carried by a broadcast, if any.
    ///
    /// Only the public certificate PEM is stored — private key material must
    /// never appear in a `StateBroadcast` payload.
    fn store_site_cert_pem(&mut self, broadcast: &StateBroadcast) {
        if let Some(pem) = &broadcast.site_cert_pem {
            self.cert_pems.insert(broadcast.origin_site.clone(), pem.clone());
            self.cert_pems_tx.send_modify(|m| {
                m.insert(broadcast.origin_site.clone(), pem.clone());
            });
        }
    }
}

impl foca::BroadcastHandler<NodeId> for StateBroadcastHandler {
    type Error = StateBroadcastError;
    type Key = StateBroadcastKey;

    fn receive_item(&mut self, data: &[u8], _sender: Option<&NodeId>) -> Result<Option<Self::Key>, Self::Error> {
        let broadcast = StateBroadcast::decode(data)?;
        if broadcast.version != STATE_BROADCAST_VERSION {
            return Err(StateBroadcastError::UnsupportedVersion {
                expected: STATE_BROADCAST_VERSION,
                actual: broadcast.version,
            });
        }

        let latest = self.latest_by_origin.get(&broadcast.origin_site).copied();
        if latest.is_some_and(|latest| latest > broadcast.revision) {
            return Ok(None);
        }

        // Store side-channel metadata regardless of whether this is
        // a state update.  Gateway address and cert PEM use independent
        // invalidation lanes and must not block provider/capability state.
        self.store_gateway_address(&broadcast);
        self.store_site_cert_pem(&broadcast);

        // Metadata-only broadcasts (gateway address or cert PEM, empty CRDT
        // snapshot) are always disseminated but never merge CRDT state.
        if broadcast.is_metadata_only() {
            return Ok(Some(broadcast.key()));
        }

        if latest.is_some_and(|latest| latest == broadcast.revision) {
            return Ok(None);
        }

        self.state_tx.send_modify(|snap| snap.merge(&broadcast.snapshot));
        self.latest_by_origin
            .insert(broadcast.origin_site.clone(), broadcast.revision);
        Ok(Some(broadcast.key()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crdt::{Capability, ProviderMetricsSnapshot, ProviderPhase, ProviderState};
    use foca::{BroadcastHandler as _, Invalidates as _};

    use super::*;

    fn snapshot(site: &str, revision: u64, queue_depth: f64) -> GridStateSnapshot {
        let mut snap = GridStateSnapshot::new(site.to_owned());
        snap.add_capability(Capability::Model("model-x".to_owned()));
        snap.upsert_provider(ProviderState {
            network_id: "net".to_owned(),
            site_id: site.to_owned(),
            provider_id: "provider".to_owned(),
            routing_cluster: site.to_owned(),
            models: vec!["model-x".to_owned()],
            backend_kind: "local".to_owned(),
            phase: ProviderPhase::Available,
            metrics: ProviderMetricsSnapshot {
                queue_depth: Some(queue_depth),
                ..ProviderMetricsSnapshot::default()
            },
            revision,
            writer_id: site.to_owned(),
        });
        snap
    }

    fn receive(handler: &mut StateBroadcastHandler, broadcast: &StateBroadcast) -> Option<StateBroadcastKey> {
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        handler
            .receive_item(&bytes, None)
            .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn new_sets_version_origin_and_revision() {
        let broadcast = StateBroadcast::new("site-p".to_owned(), 7, snapshot("site-p", 7, 0.1), None);
        assert_eq!(broadcast.version, STATE_BROADCAST_VERSION_V1, "version without gateway");
        assert_eq!(broadcast.origin_site, "site-p", "origin");
        assert_eq!(broadcast.revision, 7, "revision");
    }

    #[test]
    fn encode_decode_round_trip_preserves_snapshot() {
        let broadcast = StateBroadcast::new("site-p".to_owned(), 7, snapshot("site-p", 7, 0.1), None);
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());
        let provider = decoded
            .snapshot
            .provider("net", "site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(decoded.version, STATE_BROADCAST_VERSION_V1, "version without gateway");
        assert_eq!(provider.metrics.queue_depth, Some(0.1), "metric value");
    }

    #[test]
    fn newer_key_invalidates_older_from_same_origin() {
        let old = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 1,
            kind: StateBroadcastKeyKind::State,
        };
        let new = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 2,
            kind: StateBroadcastKeyKind::State,
        };
        assert!(new.invalidates(&old), "newer same-origin broadcast must invalidate old");
        assert!(!old.invalidates(&new), "older broadcast must not invalidate newer");
    }

    #[test]
    fn same_key_invalidates_duplicate() {
        let left = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 1,
            kind: StateBroadcastKeyKind::State,
        };
        let right = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 1,
            kind: StateBroadcastKeyKind::State,
        };
        assert!(left.invalidates(&right), "same key must invalidate duplicate");
    }

    #[test]
    fn different_origins_do_not_invalidate_each_other() {
        let left = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 9,
            kind: StateBroadcastKeyKind::State,
        };
        let right = StateBroadcastKey {
            origin_site: "site-q".to_owned(),
            revision: 1,
            kind: StateBroadcastKeyKind::State,
        };
        assert!(
            !left.invalidates(&right),
            "different origins must not invalidate each other"
        );
    }

    #[test]
    fn gateway_address_key_does_not_invalidate_state_key() {
        let gateway = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 99,
            kind: StateBroadcastKeyKind::GatewayAddress,
        };
        let state = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 1,
            kind: StateBroadcastKeyKind::State,
        };

        assert!(
            !gateway.invalidates(&state),
            "gateway-address updates must not invalidate provider/capability state"
        );
        assert!(
            !state.invalidates(&gateway),
            "provider/capability state must not invalidate gateway-address updates"
        );
    }

    #[test]
    fn decoded_broadcast_merges_with_local_snapshot() {
        let broadcast = StateBroadcast::new("site-p".to_owned(), 2, snapshot("site-p", 2, 0.1), None);
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());

        let mut local = snapshot("site-p", 1, 0.9);
        local.merge(&decoded.snapshot);

        let provider = local
            .provider("net", "site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(provider.revision, 2, "newer broadcast snapshot must win");
        assert_eq!(provider.metrics.queue_depth, Some(0.1), "newer metric must win");
    }

    #[test]
    fn handler_accepts_new_broadcast_and_merges_snapshot() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.2), None);
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());

        let key = handler
            .receive_item(&bytes, None)
            .unwrap_or_else(|_| std::process::abort());

        assert!(key.is_some(), "new broadcast must be disseminated");
        let snap = handler.snapshot();
        let provider = snap
            .provider("net", "site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(provider.metrics.queue_depth, Some(0.2), "snapshot must merge");
    }

    #[test]
    fn handler_rejects_duplicate_broadcast() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.2), None);
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());

        assert!(
            handler
                .receive_item(&bytes, None)
                .unwrap_or_else(|_| std::process::abort())
                .is_some(),
            "first broadcast is new"
        );
        assert!(
            handler
                .receive_item(&bytes, None)
                .unwrap_or_else(|_| std::process::abort())
                .is_none(),
            "duplicate broadcast is stale"
        );
    }

    #[test]
    fn handler_rejects_older_broadcast_after_newer_one() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let newer = StateBroadcast::new("site-p".to_owned(), 2, snapshot("site-p", 2, 0.1), None);
        let older = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.9), None);
        let newer_bytes = newer.encode().unwrap_or_else(|_| std::process::abort());
        let older_bytes = older.encode().unwrap_or_else(|_| std::process::abort());

        assert!(
            handler
                .receive_item(&newer_bytes, None)
                .unwrap_or_else(|_| std::process::abort())
                .is_some(),
            "newer broadcast is accepted"
        );
        assert!(
            handler
                .receive_item(&older_bytes, None)
                .unwrap_or_else(|_| std::process::abort())
                .is_none(),
            "older broadcast is stale"
        );
        let snap = handler.snapshot();
        let provider = snap
            .provider("net", "site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(provider.metrics.queue_depth, Some(0.1), "newer state must remain");
    }

    // -----------------------------------------------------------------------
    // Gateway address extension tests
    // -----------------------------------------------------------------------

    #[test]
    fn v1_broadcast_decoded_has_no_gateway_address() {
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.5), None);
        assert_eq!(broadcast.version, STATE_BROADCAST_VERSION_V1, "v1 version");
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());
        assert_eq!(decoded.version, STATE_BROADCAST_VERSION_V1, "decoded version");
        assert!(
            decoded.gateway_address.is_none(),
            "v1 broadcast must have no gateway address"
        );
    }

    #[test]
    fn extended_broadcast_with_gateway_address_round_trips() {
        let broadcast = StateBroadcast::new(
            "site-p".to_owned(),
            3,
            snapshot("site-p", 3, 0.7),
            Some("10.0.0.1:19080".to_owned()),
        );
        assert_eq!(
            broadcast.version, STATE_BROADCAST_VERSION_V1,
            "gateway extension must keep the base wire version"
        );
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());
        assert_eq!(decoded.version, STATE_BROADCAST_VERSION, "decoded version");
        assert_eq!(
            decoded.gateway_address.as_deref(),
            Some("10.0.0.1:19080"),
            "gateway address must round-trip"
        );
    }

    #[test]
    fn broadcast_without_gateway_address_encodes_base_payload_only() {
        let broadcast = StateBroadcast::new("site-p".to_owned(), 5, snapshot("site-p", 5, 0.3), None);
        assert_eq!(
            broadcast.version, STATE_BROADCAST_VERSION_V1,
            "version must be v1 when no gateway"
        );
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());

        // Decode as v1 directly to prove the wire format is v1.
        let (v1, _): (StateBroadcastV1, usize) = bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(v1.version, STATE_BROADCAST_VERSION_V1, "wire version must be v1");
        assert_eq!(v1.origin_site, "site-p", "origin site must match");

        // And it decodes via the public API too.
        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());
        assert!(decoded.gateway_address.is_none(), "must have no gateway address");
    }

    #[test]
    fn extended_broadcast_preserves_base_payload_for_older_decoders() {
        let broadcast = StateBroadcast::new(
            "site-p".to_owned(),
            3,
            snapshot("site-p", 3, 0.7),
            Some("10.0.0.1:19080".to_owned()),
        );
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());

        let (base, consumed): (StateBroadcastV1, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                .unwrap_or_else(|_| std::process::abort());
        assert_eq!(base.version, STATE_BROADCAST_VERSION_V1, "base version must remain v1");
        assert_eq!(base.origin_site, "site-p", "base origin must decode");
        assert_eq!(base.revision, 3, "base revision must decode");
        assert!(
            consumed < bytes.len(),
            "gateway extension must be trailing data after the base payload"
        );
    }

    #[test]
    fn handler_stores_gateway_address_from_extended_broadcast() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let broadcast = StateBroadcast::new(
            "site-p".to_owned(),
            1,
            snapshot("site-p", 1, 0.4),
            Some("10.0.0.2:19080".to_owned()),
        );
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());

        let key = handler
            .receive_item(&bytes, None)
            .unwrap_or_else(|_| std::process::abort());
        assert!(key.is_some(), "extended broadcast must be accepted");
        assert_eq!(
            handler.gateway_address_for_site("site-p"),
            Some("10.0.0.2:19080"),
            "gateway address must be stored"
        );
    }

    #[test]
    fn handler_accepts_gateway_address_update_at_equal_revision() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let without_gateway = StateBroadcast::new("site-p".to_owned(), 7, snapshot("site-p", 7, 0.4), None);
        let with_gateway = StateBroadcast::new(
            "site-p".to_owned(),
            7,
            snapshot("site-p", 7, 0.4),
            Some("10.0.0.2:19080".to_owned()),
        );

        let first_bytes = without_gateway.encode().unwrap_or_else(|_| std::process::abort());
        assert!(
            handler
                .receive_item(&first_bytes, None)
                .unwrap_or_else(|_| std::process::abort())
                .is_some(),
            "initial broadcast must be accepted"
        );

        let second_bytes = with_gateway.encode().unwrap_or_else(|_| std::process::abort());
        assert!(
            handler
                .receive_item(&second_bytes, None)
                .unwrap_or_else(|_| std::process::abort())
                .is_none(),
            "equal-revision gateway-only update must not re-merge state"
        );
        assert_eq!(
            handler.gateway_address_for_site("site-p"),
            Some("10.0.0.2:19080"),
            "gateway address must update even when provider revision is unchanged"
        );
    }

    #[test]
    fn handler_gateway_only_revision_does_not_block_later_state() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let gateway_only = StateBroadcast::new(
            "site-p".to_owned(),
            99,
            GridStateSnapshot::new("site-p".to_owned()),
            Some("10.0.0.2:19080".to_owned()),
        );
        assert!(receive(&mut handler, &gateway_only).is_some());

        let state = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.8), None);
        assert!(
            receive(&mut handler, &state).is_some(),
            "state broadcast must not be blocked by higher gateway-only revision"
        );

        let merged = handler.snapshot();
        let provider = merged
            .provider("net", "site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            provider.metrics.queue_depth,
            Some(0.8),
            "provider state must merge after gateway-only update"
        );
        assert_eq!(
            handler.gateway_address_for_site("site-p"),
            Some("10.0.0.2:19080"),
            "gateway address must remain available"
        );
    }

    #[test]
    fn cert_extension_round_trips() {
        let cert = "-----BEGIN CERTIFICATE-----\npublic\n-----END CERTIFICATE-----\n";
        let broadcast = StateBroadcast::new("site-p".to_owned(), 4, snapshot("site-p", 4, 0.6), None)
            .with_cert(Some(cert.to_owned()));
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());

        assert_eq!(decoded.site_cert_pem.as_deref(), Some(cert));
        assert!(decoded.gateway_address.is_none());
    }

    #[test]
    fn handler_stores_public_cert_from_extension() {
        let cert = "-----BEGIN CERTIFICATE-----\npublic\n-----END CERTIFICATE-----\n";
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.4), None)
            .with_cert(Some(cert.to_owned()));

        assert!(receive(&mut handler, &broadcast).is_some());
        assert_eq!(handler.cert_pem_for_site("site-p"), Some(cert));
    }

    #[test]
    fn cert_key_does_not_invalidate_gateway_or_state_keys() {
        let cert = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 99,
            kind: StateBroadcastKeyKind::Cert,
        };
        let gateway = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 99,
            kind: StateBroadcastKeyKind::GatewayAddress,
        };
        let state = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 1,
            kind: StateBroadcastKeyKind::State,
        };

        assert!(!cert.invalidates(&gateway));
        assert!(!cert.invalidates(&state));
        assert!(!state.invalidates(&cert));
    }

    #[test]
    fn handler_cert_only_revision_does_not_block_later_state() {
        let cert = "-----BEGIN CERTIFICATE-----\npublic\n-----END CERTIFICATE-----\n";
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let cert_only = StateBroadcast::new(
            "site-p".to_owned(),
            99,
            GridStateSnapshot::new("site-p".to_owned()),
            None,
        )
        .with_cert(Some(cert.to_owned()));
        assert!(receive(&mut handler, &cert_only).is_some());

        let state = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.8), None);
        assert!(
            receive(&mut handler, &state).is_some(),
            "state broadcast must not be blocked by higher cert-only revision"
        );

        assert!(handler.snapshot().provider("net", "site-p", "provider").is_some());
        assert_eq!(handler.cert_pem_for_site("site-p"), Some(cert));
    }

    #[test]
    fn handler_combined_metadata_revision_does_not_block_later_state() {
        let cert = "-----BEGIN CERTIFICATE-----\npublic\n-----END CERTIFICATE-----\n";
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let metadata_only = StateBroadcast::new(
            "site-p".to_owned(),
            99,
            GridStateSnapshot::new("site-p".to_owned()),
            Some("10.0.0.2:19080".to_owned()),
        )
        .with_cert(Some(cert.to_owned()));
        assert!(receive(&mut handler, &metadata_only).is_some());

        let state = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.8), None);
        assert!(
            receive(&mut handler, &state).is_some(),
            "state broadcast must not be blocked by higher combined metadata revision"
        );

        assert!(handler.snapshot().provider("net", "site-p", "provider").is_some());
        assert_eq!(handler.gateway_address_for_site("site-p"), Some("10.0.0.2:19080"));
        assert_eq!(handler.cert_pem_for_site("site-p"), Some(cert));
    }

    #[test]
    fn handler_accepts_v1_broadcast_without_gateway_address() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.4), None);
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());

        let key = handler
            .receive_item(&bytes, None)
            .unwrap_or_else(|_| std::process::abort());
        assert!(key.is_some(), "v1 broadcast must be accepted");
        assert!(
            handler.gateway_address_for_site("site-p").is_none(),
            "v1 broadcast must not set gateway address"
        );
    }
}
