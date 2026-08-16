//! State snapshot payloads carried by SWIM custom broadcasts.
//!
//! Defines the wire envelope, the foca [`BroadcastHandler`] implementation,
//! and helper types for CRDT grid-state propagation over SWIM gossip.
//!
//! [`BroadcastHandler`]: foca::BroadcastHandler

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

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

/// Default hard bound for distinct origins retained by one broadcast handler.
pub const DEFAULT_MAX_RETAINED_ORIGINS: usize = 1_024;

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

    /// Return true when this broadcast carries provider or capability state.
    ///
    /// Metadata-only broadcasts use independent invalidation lanes and are
    /// refreshed separately by the operator runtime.
    #[must_use]
    pub fn carries_grid_state(&self) -> bool {
        !self.is_metadata_only()
    }

    /// Return true when this payload only advertises side-channel metadata
    /// (gateway address and/or site cert PEM) with no CRDT state.
    #[must_use]
    fn is_metadata_only(&self) -> bool {
        self.snapshot.providers.is_empty()
            && self.snapshot.capabilities.is_empty()
            && self.snapshot.tenant_spend.is_empty()
    }

    /// Return true when this payload carries provider or capability records.
    ///
    /// Distinct from [`carries_grid_state`](Self::carries_grid_state): a
    /// tenant-spend-only broadcast carries grid state (must not be treated as
    /// metadata-only) but must **not** trigger `replace_origin_providers`,
    /// which performs a destructive retain-then-replace of the origin's
    /// provider set. Only a broadcast that actually carries provider or
    /// capability data represents an authoritative provider-state sync for
    /// its origin.
    #[must_use]
    fn carries_provider_state(&self) -> bool {
        !self.snapshot.providers.is_empty() || !self.snapshot.capabilities.is_empty()
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

/// Per-origin metadata retained by the broadcast handler.
#[derive(Default)]
struct RetainedOrigins {
    /// Highest provider-state revision received from each origin.
    latest_by_origin: BTreeMap<String, u64>,
    /// Gateway addresses received from each origin site.
    gateway_addrs: BTreeMap<String, String>,
    /// Highest gateway-address revision received from each origin.
    latest_gateway_revision_by_origin: BTreeMap<String, u64>,
    /// Public site certificate PEMs received from each origin site.
    cert_pems: BTreeMap<String, String>,
    /// Highest certificate revision received from each origin.
    latest_cert_revision_by_origin: BTreeMap<String, u64>,
}

impl RetainedOrigins {
    /// Return every origin represented by any retained map.
    fn known_origins(&self) -> BTreeSet<String> {
        self.latest_by_origin
            .keys()
            .chain(self.gateway_addrs.keys())
            .chain(self.latest_gateway_revision_by_origin.keys())
            .chain(self.cert_pems.keys())
            .chain(self.latest_cert_revision_by_origin.keys())
            .cloned()
            .collect()
    }

    /// Remove every revision and metadata value associated with one origin.
    fn remove(&mut self, origin: &str) {
        self.latest_by_origin.remove(origin);
        self.gateway_addrs.remove(origin);
        self.latest_gateway_revision_by_origin.remove(origin);
        self.cert_pems.remove(origin);
        self.latest_cert_revision_by_origin.remove(origin);
    }
}

/// Shared control path for immediate, coordinated origin eviction.
///
/// Operations take a synchronous lock only while updating in-memory maps. The
/// lock is never held across an async suspension point.
#[derive(Clone)]
pub(crate) struct OriginStateHandle {
    /// Per-origin revisions and metadata shared with the foca handler.
    retained: Arc<Mutex<RetainedOrigins>>,
    /// Merged provider-state publisher.
    state_tx: watch::Sender<GridStateSnapshot>,
    /// Gateway-address publisher.
    gateway_addrs_tx: watch::Sender<BTreeMap<String, String>>,
    /// Public-certificate publisher.
    cert_pems_tx: watch::Sender<BTreeMap<String, String>>,
}

impl OriginStateHandle {
    /// Acquire retained state, recovering a poisoned lock without hiding it.
    fn lock(&self) -> MutexGuard<'_, RetainedOrigins> {
        self.retained
            .lock()
            .unwrap_or_else(|error: PoisonError<MutexGuard<'_, RetainedOrigins>>| {
                tracing::warn!("SWIM retained-origin lock poisoned; recovering guarded state");
                error.into_inner()
            })
    }

    /// Remove provider and transport state for a departed origin, and
    /// publish the result.
    ///
    /// Deliberately does **not** touch `tenant_spend`: this method fires on
    /// ordinary SWIM membership churn (a site marked `Suspect`/`Dead` past
    /// its suspect/dead TTL, e.g. a pod restart or a transient partition —
    /// see `operator::swim_runtime::prune_tracked_members`), not on
    /// permanent tenant-budget retirement. `tenant_spend` is a cumulative
    /// (grow-only) ledger; wiping a site's slot here would let a tenant's
    /// `spendRatio` drop on a restart or blip and reopen an
    /// already-exhausted budget. If spend ever needs to expire, that must be
    /// an explicit budget-epoch/window reset, not a side effect of
    /// membership eviction — tracked in
    /// [grid#52](https://github.com/praxis-proxy/grid/issues/52), which also
    /// covers bounding per-tenant site-slot growth now that this path no
    /// longer prunes it.
    pub(crate) fn remove_origin(&self, origin: &str) {
        self.lock().remove(origin);
        self.state_tx.send_modify(|snapshot| {
            snapshot.remove_origin_providers(origin);
        });
        self.gateway_addrs_tx.send_modify(|addresses| {
            addresses.remove(origin);
        });
        self.cert_pems_tx.send_modify(|certs| {
            certs.remove(origin);
        });
    }
}

/// foca custom broadcast handler for CRDT grid-state snapshots.
///
/// Merges incoming [`StateBroadcast`] payloads into a shared
/// [`GridStateSnapshot`] that callers can observe via the watch receiver
/// returned by [`StateBroadcastHandler::subscribe`].
pub struct StateBroadcastHandler {
    /// Shared merged state — written here, read by all subscribers.
    state_tx: watch::Sender<GridStateSnapshot>,

    /// Per-origin revisions and metadata shared with the eviction control path.
    retained: Arc<Mutex<RetainedOrigins>>,

    /// Watch channel for broadcasting gateway address updates to observers.
    ///
    /// Updated whenever a broadcast with a gateway address extension is received.
    /// Subscribers observe the full map keyed by origin site name.
    gateway_addrs_tx: watch::Sender<BTreeMap<String, String>>,

    /// Watch channel for broadcasting public cert PEM updates to observers.
    cert_pems_tx: watch::Sender<BTreeMap<String, String>>,

    /// Hard bound for per-origin revision and metadata maps.
    max_origins: usize,
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
        Self::with_capacity(site_id, DEFAULT_MAX_RETAINED_ORIGINS).0
    }

    /// Create a bounded handler and a sender for coordinated origin eviction.
    ///
    /// `max_origins` is clamped to at least one. When the bound is reached, a
    /// previously retained origin is removed deterministically before a new
    /// origin is accepted.
    #[must_use]
    pub(crate) fn with_capacity(site_id: String, max_origins: usize) -> (Self, OriginStateHandle) {
        let (tx, _) = watch::channel(GridStateSnapshot::new(site_id));
        let (gw_tx, _) = watch::channel(BTreeMap::new());
        let (cert_tx, _) = watch::channel(BTreeMap::new());
        let max_origins = max_origins.max(1);
        let retained = Arc::new(Mutex::new(RetainedOrigins::default()));
        let control = OriginStateHandle {
            retained: Arc::clone(&retained),
            state_tx: tx.clone(),
            gateway_addrs_tx: gw_tx.clone(),
            cert_pems_tx: cert_tx.clone(),
        };
        (
            Self {
                state_tx: tx,
                retained,
                gateway_addrs_tx: gw_tx,
                cert_pems_tx: cert_tx,
                max_origins,
            },
            control,
        )
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
    pub fn gateway_address_for_site(&self, site: &str) -> Option<String> {
        self.retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .gateway_addrs
            .get(site)
            .cloned()
    }

    /// Return a snapshot of all known gateway addresses, keyed by site name.
    #[must_use]
    pub fn gateway_addrs(&self) -> BTreeMap<String, String> {
        self.retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .gateway_addrs
            .clone()
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
    pub fn cert_pem_for_site(&self, site: &str) -> Option<String> {
        self.retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cert_pems
            .get(site)
            .cloned()
    }

    /// Return a snapshot of all known public cert PEMs, keyed by site name.
    #[must_use]
    pub fn cert_pems(&self) -> BTreeMap<String, String> {
        self.retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cert_pems
            .clone()
    }

    /// Store and publish the gateway address carried by a broadcast, if any.
    fn store_gateway_address(&self, broadcast: &StateBroadcast) {
        if let Some(gw) = &broadcast.gateway_address {
            let mut retained = self.retained.lock().unwrap_or_else(PoisonError::into_inner);
            let latest = retained
                .latest_gateway_revision_by_origin
                .get(&broadcast.origin_site)
                .copied();
            if latest.is_some_and(|revision| revision > broadcast.revision) {
                return;
            }
            retained
                .latest_gateway_revision_by_origin
                .insert(broadcast.origin_site.clone(), broadcast.revision);
            retained.gateway_addrs.insert(broadcast.origin_site.clone(), gw.clone());
            drop(retained);
            self.gateway_addrs_tx.send_modify(|m| {
                m.insert(broadcast.origin_site.clone(), gw.clone());
            });
        }
    }

    /// Store and publish the public site cert PEM carried by a broadcast, if any.
    ///
    /// Only the public certificate PEM is stored — private key material must
    /// never appear in a `StateBroadcast` payload.
    fn store_site_cert_pem(&self, broadcast: &StateBroadcast) {
        if let Some(pem) = &broadcast.site_cert_pem {
            let mut retained = self.retained.lock().unwrap_or_else(PoisonError::into_inner);
            let latest = retained
                .latest_cert_revision_by_origin
                .get(&broadcast.origin_site)
                .copied();
            if latest.is_some_and(|revision| revision > broadcast.revision) {
                return;
            }
            retained
                .latest_cert_revision_by_origin
                .insert(broadcast.origin_site.clone(), broadcast.revision);
            retained.cert_pems.insert(broadcast.origin_site.clone(), pem.clone());
            drop(retained);
            self.cert_pems_tx.send_modify(|m| {
                m.insert(broadcast.origin_site.clone(), pem.clone());
            });
        }
    }

    /// Return every origin represented by the handler's retained maps.
    fn known_origins(&self) -> BTreeSet<String> {
        self.retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .known_origins()
    }

    /// Remove one origin from provider state, metadata, and revision guards.
    fn remove_origin(&self, origin: &str) {
        self.retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(origin);
        self.state_tx
            .send_modify(|snapshot| snapshot.remove_origin_providers(origin));
        self.gateway_addrs_tx.send_modify(|addresses| {
            addresses.remove(origin);
        });
        self.cert_pems_tx.send_modify(|certs| {
            certs.remove(origin);
        });
    }

    /// Enforce the hard origin bound before accepting an unknown origin.
    fn make_room_for(&self, incoming_origin: &str) {
        let origins = self.known_origins();
        if origins.contains(incoming_origin) || origins.len() < self.max_origins {
            return;
        }
        if let Some(origin) = origins.into_iter().next() {
            tracing::warn!(
                evicted_origin = %origin,
                max_origins = self.max_origins,
                "SWIM state origin capacity reached; evicting retained origin"
            );
            self.remove_origin(&origin);
        }
    }
}

impl foca::BroadcastHandler<NodeId> for StateBroadcastHandler {
    type Error = StateBroadcastError;
    type Key = StateBroadcastKey;

    #[expect(
        clippy::too_many_lines,
        reason = "decode, independent metadata lanes, and provider-state revision checks form one atomic receive path"
    )]
    fn receive_item(&mut self, data: &[u8], _sender: Option<&NodeId>) -> Result<Option<Self::Key>, Self::Error> {
        let broadcast = StateBroadcast::decode(data)?;
        if broadcast.version != STATE_BROADCAST_VERSION {
            return Err(StateBroadcastError::UnsupportedVersion {
                expected: STATE_BROADCAST_VERSION,
                actual: broadcast.version,
            });
        }
        self.make_room_for(&broadcast.origin_site);

        // Metadata-only broadcasts (gateway address or cert PEM, empty CRDT
        // snapshot) have independent revision lanes. They must not be rejected
        // by an unrelated provider-state revision.
        if broadcast.is_metadata_only() {
            self.store_gateway_address(&broadcast);
            self.store_site_cert_pem(&broadcast);
            return Ok(Some(broadcast.key()));
        }

        let latest = self
            .retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .latest_by_origin
            .get(&broadcast.origin_site)
            .copied();
        if latest.is_some_and(|latest| latest > broadcast.revision) {
            return Ok(None);
        }

        // Extensions on a non-stale state broadcast remain authoritative.
        self.store_gateway_address(&broadcast);
        self.store_site_cert_pem(&broadcast);

        if latest.is_some_and(|latest| latest == broadcast.revision) {
            return Ok(None);
        }

        let carries_provider_state = broadcast.carries_provider_state();
        self.state_tx.send_modify(|snap| {
            snap.capabilities.merge(&broadcast.snapshot.capabilities);
            snap.merge_tenant_spend_from_origin(&broadcast.origin_site, &broadcast.snapshot.tenant_spend);
            // A spend-only broadcast must not run the destructive
            // origin-provider replace below — it doesn't carry an
            // authoritative provider list for this cycle at all.
            if carries_provider_state {
                snap.replace_origin_providers(&broadcast.origin_site, broadcast.revision, &broadcast.snapshot);
            }
        });
        self.retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .latest_by_origin
            .insert(broadcast.origin_site.clone(), broadcast.revision);
        Ok(Some(broadcast.key()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crdt::{Capability, GCounter, ProviderMetricsSnapshot, ProviderPhase, ProviderState};
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
            access_policy: crdt::ProviderAccessPolicy::default(),
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
    fn grid_state_and_metadata_broadcasts_are_distinguished() {
        let state = StateBroadcast::new("site-p".to_owned(), 7, snapshot("site-p", 7, 0.1), None);
        let metadata = StateBroadcast::new(
            "site-p".to_owned(),
            8,
            GridStateSnapshot::new("site-p".to_owned()),
            Some("10.0.0.1:8443".to_owned()),
        );

        assert!(state.carries_grid_state(), "provider state must be retained for repair");
        assert!(
            !metadata.carries_grid_state(),
            "metadata uses its existing independent refresh path"
        );
    }

    #[test]
    fn tenant_spend_only_snapshot_carries_grid_state() {
        // A broadcast can carry only a tenant_spend increment (no provider or
        // capability change this gossip cycle) — this must NOT be classified
        // as metadata-only, or receive_item's merge path is skipped entirely
        // (regression: `is_metadata_only` originally only checked
        // providers/capabilities, silently dropping spend-only broadcasts).
        let mut snap = GridStateSnapshot::new("site-p".to_owned());
        snap.increment_tenant_spend("tenant-x", 500);
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snap, None);

        assert!(
            broadcast.carries_grid_state(),
            "tenant_spend-only snapshot must be treated as carrying grid state, not metadata-only"
        );
    }

    #[test]
    fn receive_item_merges_tenant_spend_only_broadcast_with_no_providers_or_capabilities() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let mut snap = GridStateSnapshot::new("site-p".to_owned());
        snap.increment_tenant_spend("tenant-x", 500);
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snap, None);

        receive(&mut handler, &broadcast);

        assert_eq!(
            handler
                .snapshot()
                .tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            500,
            "a broadcast with only tenant_spend set (no providers/capabilities) must still be merged, \
             not dropped as metadata-only"
        );
    }

    #[test]
    fn receive_item_spend_only_broadcast_does_not_wipe_origins_existing_providers() {
        // Bugbot regression: a later spend-only broadcast from an origin that
        // already has real providers must not erase those providers.
        // `replace_origin_providers` performs a destructive retain-then-replace
        // for the origin's provider set; it must only run when the broadcast
        // actually carries provider/capability data, not merely because
        // `carries_grid_state()` is true (which tenant_spend alone satisfies).
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let full = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.2), None);
        receive(&mut handler, &full);
        assert!(
            handler.snapshot().provider("net", "site-p", "provider").is_some(),
            "precondition: origin's provider must be present after the first broadcast"
        );

        let mut spend_only = GridStateSnapshot::new("site-p".to_owned());
        spend_only.increment_tenant_spend("tenant-x", 500);
        let spend_broadcast = StateBroadcast::new("site-p".to_owned(), 2, spend_only, None);
        receive(&mut handler, &spend_broadcast);

        assert!(
            handler.snapshot().provider("net", "site-p", "provider").is_some(),
            "a spend-only broadcast from the same origin must not wipe that origin's provider records"
        );
        assert_eq!(
            handler
                .snapshot()
                .tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            500,
            "the spend-only broadcast's tenant_spend must still be merged"
        );
    }

    #[test]
    fn origin_state_handle_remove_origin_preserves_that_origins_tenant_spend() {
        // Correctness (grid#47 review): membership eviction fires on ordinary
        // SWIM churn (a restart or a transient partition exceeding the
        // suspect/dead TTL), not on permanent tenant-budget retirement.
        // Wiping a site's cumulative spend contribution here would let
        // `spendRatio` drop and reopen an already-exhausted budget on a mere
        // restart, unlike provider records (which are membership-scoped and
        // correctly pruned).
        let (mut handler, control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 8);
        let mut snap = GridStateSnapshot::new("site-p".to_owned());
        snap.increment_tenant_spend("tenant-x", 500);
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snap, None);
        receive(&mut handler, &broadcast);
        assert_eq!(
            control
                .state_tx
                .borrow()
                .tenant_spend
                .get("tenant-x")
                .map(GCounter::total),
            Some(500),
            "precondition: tenant spend merged before eviction"
        );

        control.remove_origin("site-p");

        assert_eq!(
            control
                .state_tx
                .borrow()
                .tenant_spend
                .get("tenant-x")
                .map(GCounter::total),
            Some(500),
            "evicting an origin from membership must not erase its cumulative spend contribution"
        );
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
    fn newer_transport_revision_replaces_equal_revision_provider_metrics() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let initial = StateBroadcast::new("site-p".to_owned(), 10, snapshot("site-p", 1, 0.9), None);
        let changed = StateBroadcast::new("site-p".to_owned(), 11, snapshot("site-p", 1, 0.1), None);

        assert!(receive(&mut handler, &initial).is_some());
        assert!(receive(&mut handler, &changed).is_some());

        let snap = handler.snapshot();
        let provider = snap
            .provider("net", "site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            provider.revision, 11,
            "transport revision must become the provider LWW revision"
        );
        assert_eq!(
            provider.metrics.queue_depth,
            Some(0.1),
            "newer origin snapshot must replace metric-only state"
        );
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
            handler.gateway_address_for_site("site-p").as_deref(),
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
            handler.gateway_address_for_site("site-p").as_deref(),
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
            handler.gateway_address_for_site("site-p").as_deref(),
            Some("10.0.0.2:19080"),
            "gateway address must remain available"
        );
    }

    #[test]
    fn handler_state_revision_does_not_block_gateway_only_update() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let state = StateBroadcast::new("site-p".to_owned(), 99, snapshot("site-p", 99, 0.8), None);
        assert!(receive(&mut handler, &state).is_some());

        let gateway_only = StateBroadcast::new(
            "site-p".to_owned(),
            1,
            GridStateSnapshot::new("site-p".to_owned()),
            Some("10.0.0.2:8443".to_owned()),
        );
        assert!(
            receive(&mut handler, &gateway_only).is_some(),
            "gateway-only update must use its independent revision lane"
        );
        assert_eq!(
            handler.gateway_address_for_site("site-p").as_deref(),
            Some("10.0.0.2:8443"),
            "provider-state revision must not leave the gateway address stale"
        );
    }

    #[test]
    fn handler_rejects_out_of_order_gateway_rollback() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let newer = StateBroadcast::new(
            "site-p".to_owned(),
            8,
            GridStateSnapshot::new("site-p".to_owned()),
            Some("10.0.0.8:8443".to_owned()),
        );
        let older = StateBroadcast::new(
            "site-p".to_owned(),
            7,
            GridStateSnapshot::new("site-p".to_owned()),
            Some("10.0.0.7:8443".to_owned()),
        );

        assert!(receive(&mut handler, &newer).is_some());
        assert!(receive(&mut handler, &older).is_some());
        assert_eq!(
            handler.gateway_address_for_site("site-p").as_deref(),
            Some("10.0.0.8:8443")
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
        assert_eq!(handler.cert_pem_for_site("site-p").as_deref(), Some(cert));
    }

    #[test]
    fn handler_rejects_out_of_order_certificate_rollback() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let newer = StateBroadcast::new(
            "site-p".to_owned(),
            8,
            GridStateSnapshot::new("site-p".to_owned()),
            None,
        )
        .with_cert(Some("new-cert".to_owned()));
        let older = StateBroadcast::new(
            "site-p".to_owned(),
            7,
            GridStateSnapshot::new("site-p".to_owned()),
            None,
        )
        .with_cert(Some("old-cert".to_owned()));

        assert!(receive(&mut handler, &newer).is_some());
        assert!(receive(&mut handler, &older).is_some());
        assert_eq!(handler.cert_pem_for_site("site-p").as_deref(), Some("new-cert"));
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
        assert_eq!(handler.cert_pem_for_site("site-p").as_deref(), Some(cert));
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
        assert_eq!(
            handler.gateway_address_for_site("site-p").as_deref(),
            Some("10.0.0.2:19080")
        );
        assert_eq!(handler.cert_pem_for_site("site-p").as_deref(), Some(cert));
    }

    // -----------------------------------------------------------------------
    // tenant_spend broadcast wiring tests (F1-F2)
    // -----------------------------------------------------------------------

    #[test]
    fn receive_item_merges_tenant_spend_from_broadcast() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let mut snap = snapshot("site-p", 1, 0.2);
        snap.increment_tenant_spend("tenant-x", 500);
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snap, None);

        receive(&mut handler, &broadcast);

        let merged = handler.snapshot();
        assert_eq!(
            merged
                .tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            500,
            "receive_item must merge the broadcast's tenant_spend into the handler's snapshot"
        );
    }

    #[test]
    fn receive_item_sums_tenant_spend_across_origin_sites() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());

        let mut snap_a = snapshot("site-a", 1, 0.2);
        snap_a.increment_tenant_spend("tenant-x", 300);
        receive(&mut handler, &StateBroadcast::new("site-a".to_owned(), 1, snap_a, None));

        let mut snap_b = snapshot("site-b", 1, 0.2);
        snap_b.increment_tenant_spend("tenant-x", 700);
        receive(&mut handler, &StateBroadcast::new("site-b".to_owned(), 1, snap_b, None));

        let merged = handler.snapshot();
        assert_eq!(
            merged
                .tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            1000,
            "tenant spend from two different origin sites must sum, proving cross-site convergence at the wiring layer"
        );
    }

    // -----------------------------------------------------------------------
    // tenant_spend origin-slot cap tests (grid#52)
    // -----------------------------------------------------------------------

    /// Deliver one origin's tenant-spend-only broadcast through the real
    /// SWIM ingest entry point (`receive_item`, via the `receive` helper).
    ///
    /// `revision` must be unique-and-increasing per call for the same
    /// `origin` — `receive_item` drops a same-or-lower-revision broadcast
    /// from an origin it has already seen as stale, independent of this
    /// module's own cap logic (see `receive_item`'s `latest_by_origin` check).
    fn receive_tenant_spend_broadcast(
        handler: &mut StateBroadcastHandler,
        origin: &str,
        revision: u64,
        tenant_id: &str,
        amount: u64,
    ) {
        let mut snap = GridStateSnapshot::new(origin.to_owned());
        snap.increment_tenant_spend(tenant_id, amount);
        receive(handler, &StateBroadcast::new(origin.to_owned(), revision, snap, None));
    }

    #[test]
    fn receive_item_caps_distinct_origin_slots_for_a_tenant() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        for i in 0..crdt::grid_state::MAX_TENANT_SPEND_ORIGINS {
            receive_tenant_spend_broadcast(&mut handler, &format!("site-{i}"), 1, "tenant-x", 1);
        }

        receive_tenant_spend_broadcast(&mut handler, "site-overflow", 1, "tenant-x", 1);

        let merged = handler.snapshot();
        let counter = merged
            .tenant_spend
            .get("tenant-x")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            counter.slot_count(),
            crdt::grid_state::MAX_TENANT_SPEND_ORIGINS,
            "a brand-new origin's broadcast must be dropped once the tenant's counter is at capacity -- \
             exercised at the real SWIM ingest boundary (receive_item), not just the internal merge function \
             directly, to prove the bound actually applies to gossip as delivered"
        );
        assert_eq!(
            counter.total(),
            u64::try_from(crdt::grid_state::MAX_TENANT_SPEND_ORIGINS).unwrap_or(u64::MAX),
            "the overflow origin's amount must not be reflected in the merged total"
        );
    }

    #[test]
    fn receive_item_still_merges_updates_from_an_already_tracked_origin_once_capped() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        for i in 0..crdt::grid_state::MAX_TENANT_SPEND_ORIGINS {
            receive_tenant_spend_broadcast(&mut handler, &format!("site-{i}"), 1, "tenant-x", 1);
        }

        receive_tenant_spend_broadcast(&mut handler, "site-0", 2, "tenant-x", 500);

        let merged = handler.snapshot();
        let counter = merged
            .tenant_spend
            .get("tenant-x")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            counter.total(),
            u64::try_from(crdt::grid_state::MAX_TENANT_SPEND_ORIGINS - 1).unwrap_or(u64::MAX) + 500,
            "site-0 already has a slot, so a higher-revision, higher-amount broadcast from it must still merge \
             at the origin-slot cap -- the cap only blocks brand-new origins, not ongoing updates from origins \
             already being tracked"
        );
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

    #[test]
    fn coordinated_eviction_removes_all_origin_state_before_next_item() {
        let (mut handler, origin_state) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 4);
        let cert = "public-cert";
        let original = StateBroadcast::new(
            "site-a".to_owned(),
            10,
            snapshot("site-a", 10, 0.4),
            Some("10.0.0.1:8443".to_owned()),
        )
        .with_cert(Some(cert.to_owned()));
        assert!(receive(&mut handler, &original).is_some());
        origin_state.remove_origin("site-a");

        assert!(handler.snapshot().provider("net", "site-a", "provider").is_none());
        assert!(handler.gateway_address_for_site("site-a").is_none());
        assert!(handler.cert_pem_for_site("site-a").is_none());

        let restarted = StateBroadcast::new(
            "site-a".to_owned(),
            1,
            snapshot("site-a", 1, 0.8),
            Some("10.0.0.9:8443".to_owned()),
        );
        assert!(
            receive(&mut handler, &restarted).is_some(),
            "eviction must clear the old revision watermark so a restarted origin can rejoin"
        );
        assert_eq!(
            handler.gateway_address_for_site("site-a").as_deref(),
            Some("10.0.0.9:8443")
        );
    }

    #[test]
    fn origin_maps_are_hard_bounded() {
        let (mut handler, _origin_state) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 2);
        for (origin, revision) in [("site-b", 1), ("site-c", 2), ("site-a", 3)] {
            let broadcast = StateBroadcast::new(
                origin.to_owned(),
                revision,
                snapshot(origin, revision, 0.3),
                Some(format!("10.0.0.{revision}:8443")),
            )
            .with_cert(Some(format!("cert-{origin}")));
            assert!(receive(&mut handler, &broadcast).is_some());
        }

        let origins = handler.known_origins();
        assert_eq!(origins.len(), 2);
        assert!(origins.contains("site-a"));
        assert!(origins.contains("site-c"));
        assert!(
            handler.snapshot().provider("net", "site-b", "provider").is_none(),
            "capacity eviction must remove provider state with metadata"
        );
    }
}
