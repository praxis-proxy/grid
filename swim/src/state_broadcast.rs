//! State snapshot payloads carried by SWIM custom broadcasts.
//!
//! Defines the wire envelope, the foca [`BroadcastHandler`] implementation,
//! and helper types for CRDT grid-state propagation over SWIM gossip.
//!
//! [`BroadcastHandler`]: foca::BroadcastHandler

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crdt::GridStateSnapshot;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::{NodeId, identity::MAX_LEASE_SKEW};

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

/// Maximum provider-state scopes retained for each configured origin slot.
const RETAINED_SCOPE_MULTIPLIER: usize = 16;

/// Magic prefix for the framed provider-state extension appended after the
/// legacy signals-address slot.
const PROVIDER_STATE_EXTENSION_MAGIC: [u8; 8] = *b"AGNPSEXT";

/// First framed provider-state extension version.
const PROVIDER_STATE_EXTENSION_VERSION_V1: u8 = 1;

/// Domain-separation prefix mixed into every state-broadcast signature.
///
/// Binds a signature to this exact protocol, message type, and wire-format
/// version, so it can never be replayed as valid input to a different
/// signing context even if the same ECDSA key were ever reused elsewhere.
///
/// Scopes a signature to *this protocol*, not to a particular
/// `GridNetwork`: that narrower scoping is [`StateBroadcast::grid_id`]'s
/// job, since a node can publish a broadcast before joining any
/// `GridNetwork` and so cannot always supply one. Anything broader than a
/// single cluster's `GridNetwork`s — cross-deployment or per-peer mesh
/// identity — remains out of scope for this constant and for `grid_id`
/// alike.
const SIGNATURE_DOMAIN: &[u8] = b"praxis-grid/swim/state-broadcast/v1";

/// Bounded log message shared by signature verification failures.
const REJECTED_BROADCAST_LOG: &str = "rejecting pinned-origin state broadcast";

/// Maximum age, in milliseconds, of a pinned origin's signed broadcast
/// timestamp before it is rejected as stale.
///
/// Set to several orders of magnitude above the default 5-second SWIM probe
/// interval (`default_probe_interval` in `operator/src/crd/grid_network.rs`),
/// comfortably covering ordinary gossip fan-out delay while still bounding a
/// captured signature's replay window to minutes rather than leaving it
/// unbounded. See [`StateBroadcast::signed_at_ms`] for what this window does
/// and does not defend against.
pub const MAX_BROADCAST_AGE_MS: u64 = 5 * 60 * 1_000;

/// Maximum amount, in milliseconds, a pinned origin's signed broadcast
/// timestamp may be ahead of this node's own clock before it is rejected.
///
/// Tolerates ordinary clock drift between SWIM peers without opening a
/// window for a broadcast timestamped far in the future to keep outrunning
/// [`MAX_BROADCAST_AGE_MS`] on every receiving peer.
pub const MAX_CLOCK_SKEW_AHEAD_MS: u64 = 30_000;

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

    /// Whether this snapshot authoritatively replaces the origin's providers.
    ///
    /// This distinguishes an intentional empty provider inventory from a
    /// metadata-only broadcast. Peers that predate this field safely ignore
    /// its framed trailing extension.
    #[serde(default)]
    pub authoritative_provider_state: bool,

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

    /// ECDSA P-256 signature (ASN.1 DER) over [`signable_bytes`](Self::signable_bytes).
    ///
    /// `None` when the originating site has no signing key configured, or
    /// during the rollout window before every peer signs broadcasts. A
    /// receiver only requires this field once it holds a pinned identity for
    /// `origin_site`; see [`StateBroadcastHandler::receive_item`].
    ///
    /// [`StateBroadcastHandler::receive_item`]: foca::BroadcastHandler::receive_item
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<Vec<u8>>,

    /// Wall-clock time this broadcast was signed, in milliseconds since the
    /// Unix epoch.
    ///
    /// `None` under the same conditions as [`signature`](Self::signature)
    /// — no signing key configured, or the pre-rollout window. Included in
    /// [`signable_bytes`](Self::signable_bytes) so a captured signature
    /// cannot be re-attached to a forged, more-recent timestamp. A receiver
    /// holding a pinned identity for `origin_site` rejects a signature whose
    /// timestamp is more than [`MAX_BROADCAST_AGE_MS`] in the past, or more
    /// than [`MAX_CLOCK_SKEW_AHEAD_MS`] in the future, bounding how long a
    /// captured, validly signed broadcast can be replayed as current.
    ///
    /// Bounds only the *replay window*, not full replay elimination: this
    /// value is never persisted, so a process restart resets every
    /// receiver's notion of "now" relative to nothing durable. Full replay
    /// elimination needs a persisted, monotonic revision floor surviving
    /// restarts, which this in-memory crate does not provide.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signed_at_ms: Option<u64>,

    /// Identifier of the `GridNetwork` this broadcast's state belongs to.
    ///
    /// Included in [`signable_bytes`](Self::signable_bytes) so a signature
    /// is scoped to one `GridNetwork`: issue [#48] documents that a single
    /// cluster can run multiple `GridNetwork`s as separate tenants,
    /// environments, or trust domains with independent provider
    /// inventories, yet one `SwimHandle` is shared across every
    /// `GridNetwork` reconcile on that cluster. Without this field, a
    /// signature valid for one `GridNetwork` would also verify, bit for
    /// bit, as a broadcast claiming to belong to another `GridNetwork` on
    /// the same cluster — a cross-tenant replay that would defeat #48's
    /// stated isolation guarantee.
    ///
    /// `None` for broadcasts published before a node has joined any
    /// `GridNetwork` (e.g. a bare gateway-address advertisement — see
    /// `publish_gateway_address_broadcast` in the operator crate), and for
    /// broadcasts from operators old enough to predate this field.
    ///
    /// [#48]: https://github.com/praxis-proxy/grid/issues/48
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grid_id: Option<String>,

    /// Signals `host:port` this site serves peers on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signals_address: Option<String>,
}

/// Maximum number of raw public keys retained per pinned origin.
///
/// Mirrors `GridSiteTrustPolicy.canonical_fingerprints`'s dual-pin bound for
/// mTLS certificate rotation (`operator/src/crd/grid_site.rs`): one slot for
/// the current signing key and one for a next key during a bounded rotation
/// overlap window.
pub const MAX_PINNED_KEYS_PER_ORIGIN: usize = 2;

/// Bounded set of accepted raw ECDSA P-256 public keys, keyed by origin site
/// name.
///
/// Each value holds up to [`MAX_PINNED_KEYS_PER_ORIGIN`] raw uncompressed EC
/// points. A broadcast's signature verifies if it is valid under **any** key
/// in its origin's pinned set — this is what makes bounded key rotation
/// possible without an instantaneous flag-day cutover: a site publishes a
/// next key alongside its current one, callers add the next key to the pin
/// set, and once every peer has observed the rotation the old key is
/// dropped. Deliberately opaque to *how* a pinned identity was established —
/// that is an operator-level concern (see [`crate::signing`]). An origin
/// with no entry, or an empty entry, is not yet enforced against a
/// signature. Prefer [`crate::node::SwimNode::pin_origin`] over mutating
/// this map directly through [`StateBroadcastHandler::trust_store_sender`]:
/// `pin_origin` also purges any state accepted from an origin before it had
/// a pin, which a raw `watch::Sender::send`/`send_modify` call does not.
pub type TrustStore = BTreeMap<String, Vec<Vec<u8>>>;

/// A caller supplied more than [`MAX_PINNED_KEYS_PER_ORIGIN`] keys for one origin.
#[derive(Debug, thiserror::Error)]
#[error("origin {origin} was pinned with {supplied} keys, exceeding the max of {MAX_PINNED_KEYS_PER_ORIGIN}")]
pub struct TooManyPinnedKeys {
    /// Origin site the caller attempted to pin.
    pub origin: String,
    /// Number of keys the caller supplied.
    pub supplied: usize,
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

/// Legacy extension after the base v1 payload.
///
/// This six-field layout is immutable because deployed peers decode it
/// positionally, followed by the optional signals-address slot.
#[derive(Default, Serialize, Deserialize)]
struct BroadcastExtension {
    /// Optional data-plane gateway address.
    gateway_address: Option<String>,
    /// Optional public site certificate PEM — never a private key.
    site_cert_pem: Option<String>,
    /// Optional ECDSA P-256 signature over the base payload plus the other
    /// extension fields. Absent on older peers and pre-rollout broadcasts.
    #[serde(default)]
    signature: Option<Vec<u8>>,
    /// Optional wall-clock signing timestamp, milliseconds since the Unix
    /// epoch. Absent on older peers and pre-rollout broadcasts.
    #[serde(default)]
    signed_at_ms: Option<u64>,
    /// Optional owning `GridNetwork` identifier. Absent on older peers,
    /// pre-rollout broadcasts, and broadcasts published before a node has
    /// joined any `GridNetwork`.
    #[serde(default)]
    grid_id: Option<String>,
    /// Provider capacities keyed by stable network/site/provider identity.
    #[serde(default)]
    provider_capacity_weights: BTreeMap<String, u32>,
}

/// Header for a length-delimited provider-state extension frame.
#[derive(Deserialize, Serialize)]
struct ProviderStateExtensionHeader {
    /// Fixed marker distinguishing the frame from other trailing data.
    magic: [u8; 8],
    /// Encoded payload length in bytes.
    payload_length: u32,
    /// Payload schema version.
    version: u8,
}

/// Provider-state extension payload understood by current peers.
#[derive(Default, Deserialize, Serialize)]
struct ProviderStateExtensionV1 {
    /// Whether an empty provider set is an authoritative withdrawal.
    authoritative_provider_state: bool,
    /// MCP tool lists keyed by stable network/site/provider identity.
    provider_tools: BTreeMap<String, Vec<String>>,
}

/// Extension format used by peers that predate provider capacity weights.
#[derive(Serialize, Deserialize)]
struct PreCapacityBroadcastExtension {
    /// Optional data-plane gateway address.
    gateway_address: Option<String>,
    /// Optional public site certificate PEM.
    site_cert_pem: Option<String>,
    /// Optional ECDSA P-256 signature.
    signature: Option<Vec<u8>>,
    /// Optional wall-clock signing timestamp.
    signed_at_ms: Option<u64>,
    /// Optional owning `GridNetwork` identifier.
    grid_id: Option<String>,
}

/// Extension format used by peers that predate both the `signed_at_ms` and
/// `grid_id` fields.
///
/// bincode is not self-describing, so decoding a three-field payload as the
/// current six-field [`BroadcastExtension`] and the preceding five-field
/// [`PreCapacityBroadcastExtension`] fail partway through the
/// fourth field rather than falling back to `#[serde(default)]` — `decode`
/// tries this shape before falling further back through every prior wire
/// format, so a rolling update does not silently drop or misdecode
/// `gateway_address`/`site_cert_pem`/`signature` from not-yet-upgraded peers.
#[derive(Serialize, Deserialize)]
struct PreTimestampBroadcastExtension {
    /// Optional data-plane gateway address.
    gateway_address: Option<String>,
    /// Optional public site certificate PEM — never a private key.
    site_cert_pem: Option<String>,
    /// Optional ECDSA P-256 signature over the base payload plus the other
    /// extension fields. Absent on older peers and pre-rollout broadcasts.
    #[serde(default)]
    signature: Option<Vec<u8>>,
}

/// Extension format used by peers that predate the `signature` field.
///
/// bincode is not self-describing, so decoding a two-field payload as
/// [`PreTimestampBroadcastExtension`] fails partway through the third field
/// rather than falling back to `#[serde(default)]` — `decode` tries that
/// shape before falling further back to this one, then to the original
/// bare-`String` format, so a rolling update does not silently drop or
/// misdecode `gateway_address`/`site_cert_pem` from not-yet-upgraded peers.
#[derive(Serialize, Deserialize)]
struct PreSignatureBroadcastExtension {
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
            authoritative_provider_state: false,
            gateway_address,
            site_cert_pem: None,
            signature: None,
            signed_at_ms: None,
            grid_id: None,
            signals_address: None,
        }
    }

    /// Mark this snapshot as an authoritative replacement of the origin's
    /// provider inventory.
    ///
    /// Use this for provider-state publications, including an empty snapshot
    /// that withdraws every provider previously advertised by the origin.
    #[must_use]
    pub fn with_authoritative_provider_state(mut self) -> Self {
        self.authoritative_provider_state = true;
        self
    }

    /// Attach the signals `host:port` this site serves peers on.
    #[must_use]
    pub fn with_signals_address(mut self, signals_address: Option<String>) -> Self {
        self.signals_address = signals_address;
        self
    }

    /// Create a broadcast that also carries a public site certificate PEM.
    ///
    /// The certificate must be the public certificate only — never a private key.
    #[must_use]
    pub fn with_cert(mut self, site_cert_pem: Option<String>) -> Self {
        self.site_cert_pem = site_cert_pem;
        self
    }

    /// Attach a signature computed over [`signable_bytes`](Self::signable_bytes).
    ///
    /// Callers are responsible for computing `signature` (typically via
    /// [`crate::signing::sign_ecdsa_p256`] over `self.signable_bytes()`)
    /// before attaching it; this method performs no verification.
    #[must_use]
    pub fn with_signature(mut self, signature: Option<Vec<u8>>) -> Self {
        self.signature = signature;
        self
    }

    /// Attach the wall-clock signing timestamp (milliseconds since the Unix
    /// epoch).
    ///
    /// Set this **before** computing [`signable_bytes`](Self::signable_bytes)
    /// so the timestamp itself is covered by the signature — see
    /// [`signed_at_ms`](Self::signed_at_ms)'s doc comment for why an
    /// unsigned timestamp would defeat the freshness check it exists to
    /// support.
    #[must_use]
    pub fn with_signed_at(mut self, signed_at_ms: Option<u64>) -> Self {
        self.signed_at_ms = signed_at_ms;
        self
    }

    /// Attach the owning `GridNetwork` identifier.
    ///
    /// Set this **before** computing [`signable_bytes`](Self::signable_bytes)
    /// so the identifier is covered by the signature — see
    /// [`grid_id`](Self::grid_id)'s doc comment for the cross-`GridNetwork`
    /// replay this scoping closes.
    #[must_use]
    pub fn with_grid_id(mut self, grid_id: Option<String>) -> Self {
        self.grid_id = grid_id;
        self
    }

    /// Return the canonical bytes this broadcast's signature covers.
    ///
    /// Prefixed with a fixed domain-separation tag followed by
    /// [`encode`](Self::encode) of this broadcast with `signature` cleared,
    /// so a signature can never cover itself and can never be replayed as
    /// valid input to a different signing context even if the same key were
    /// ever reused elsewhere.
    ///
    /// Covers the signals address, which older verifiers drop: pin no origin gossiping one until all decode it.
    ///
    /// # Errors
    ///
    /// Returns a bincode encode error if the snapshot cannot be serialized.
    pub fn signable_bytes(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        let mut unsigned = self.clone();
        unsigned.signature = None;
        let mut signable = SIGNATURE_DOMAIN.to_vec();
        signable.extend(unsigned.encode()?);
        Ok(signable)
    }

    /// Return this broadcast's invalidation key.
    #[must_use]
    pub fn key(&self) -> StateBroadcastKey {
        let kind = self.key_kind();
        StateBroadcastKey {
            origin_site: self.origin_site.clone(),
            revision: self.revision,
            grid_id: (kind == StateBroadcastKeyKind::State)
                .then(|| self.grid_id.clone())
                .flatten(),
            kind,
        }
    }

    /// Return the origin and Grid ID that scope provider-state replacement.
    fn provider_state_scope(&self) -> ProviderStateScope {
        ProviderStateScope {
            origin_site: self.origin_site.clone(),
            grid_id: self.grid_id.clone(),
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
        !self.authoritative_provider_state
            && self.snapshot.providers.is_empty()
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
        self.authoritative_provider_state
            || !self.snapshot.providers.is_empty()
            || !self.snapshot.capabilities.is_empty()
    }

    /// Return true when this payload only advertises gateway or signals addresses.
    #[must_use]
    fn is_address_only(&self) -> bool {
        (self.gateway_address.is_some() || self.signals_address.is_some())
            && self.site_cert_pem.is_none()
            && self.is_metadata_only()
    }

    /// Return true when this payload only carries site certificate PEM.
    #[must_use]
    fn is_cert_only(&self) -> bool {
        self.site_cert_pem.is_some()
            && self.gateway_address.is_none()
            && self.signals_address.is_none()
            && self.is_metadata_only()
    }

    /// Return the foca invalidation key kind for this payload.
    #[must_use]
    fn key_kind(&self) -> StateBroadcastKeyKind {
        if self.is_cert_only() {
            StateBroadcastKeyKind::Cert
        } else if self.is_address_only() {
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
    #[expect(
        clippy::too_many_lines,
        reason = "base and trailing extension must be encoded as one atomic wire operation"
    )]
    pub fn encode(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        let v1 = StateBroadcastV1 {
            version: self.version,
            origin_site: self.origin_site.clone(),
            revision: self.revision,
            snapshot: self.snapshot.clone(),
        };
        let mut bytes = bincode::serde::encode_to_vec(&v1, bincode::config::standard())?;
        let provider_capacity_weights = self
            .snapshot
            .providers
            .values()
            .filter(|provider| provider.capacity_weight != 1)
            .map(|provider| (provider_capacity_key(provider), provider.capacity_weight))
            .collect::<BTreeMap<_, _>>();
        let provider_tools = self
            .snapshot
            .providers
            .values()
            .filter(|provider| !provider.tools.is_empty())
            .map(|provider| (provider_capacity_key(provider), provider.tools.clone()))
            .collect::<BTreeMap<_, _>>();
        let provider_state_extension = ProviderStateExtensionV1 {
            authoritative_provider_state: self.authoritative_provider_state,
            provider_tools,
        };
        let carries_provider_state_extension = provider_state_extension.authoritative_provider_state
            || !provider_state_extension.provider_tools.is_empty();
        let carries_extension = self.gateway_address.is_some()
            || self.site_cert_pem.is_some()
            || self.signature.is_some()
            || self.signed_at_ms.is_some()
            || self.grid_id.is_some()
            || !provider_capacity_weights.is_empty()
            || self.signals_address.is_some()
            || carries_provider_state_extension;
        if carries_extension {
            let ext = (
                &self.gateway_address,
                &self.site_cert_pem,
                &self.signature,
                &self.signed_at_ms,
                &self.grid_id,
                &provider_capacity_weights,
            );
            bincode::serde::encode_into_std_write(ext, &mut bytes, bincode::config::standard())?;
            if self.signals_address.is_some() || carries_provider_state_extension {
                bincode::serde::encode_into_std_write(&self.signals_address, &mut bytes, bincode::config::standard())?;
            }
            if carries_provider_state_extension {
                let payload = bincode::serde::encode_to_vec(&provider_state_extension, bincode::config::standard())?;
                let payload_length = u32::try_from(payload.len()).map_err(|_error| {
                    bincode::error::EncodeError::Other("provider-state extension exceeds the frame length limit")
                })?;
                let header = ProviderStateExtensionHeader {
                    magic: PROVIDER_STATE_EXTENSION_MAGIC,
                    payload_length,
                    version: PROVIDER_STATE_EXTENSION_VERSION_V1,
                };
                bincode::serde::encode_into_std_write(header, &mut bytes, bincode::config::standard())?;
                bytes.extend_from_slice(&payload);
            }
        }
        Ok(bytes)
    }

    /// Decode this broadcast from bincode bytes.
    ///
    /// Decodes the base v1 payload, then tries to decode any trailing bytes as
    /// a `BroadcastExtension` struct.  Because bincode is not self-describing,
    /// a newer extension decode does not simply come back `Ok` with
    /// `grid_id: None` when reading bytes from an older, three-field
    /// peer — it fails partway through the missing fields.  Falls back in
    /// turn to the three-field pre-timestamp extension format, then the
    /// two-field pre-signature extension format, then to the
    /// original bare-`String` format for `gateway_address`, ensuring
    /// interoperability with peers running any prior wire format during a
    /// rolling update.
    ///
    /// Payloads without any extension decode with `gateway_address = None` and
    /// `site_cert_pem = None`.
    ///
    /// # Errors
    ///
    /// Returns a bincode decode error if `bytes` is not a valid
    /// [`StateBroadcast`] payload.
    #[expect(
        clippy::too_many_lines,
        reason = "extension fields (capacity weights, tools) must be applied to providers in one decode pass"
    )]
    pub fn decode(bytes: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        let (mut v1, consumed): (StateBroadcastV1, usize) =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;

        let remaining = bytes.get(consumed..).unwrap_or(&[]);
        let (ext, signals_address, provider_state_extension) = Self::decode_extension(remaining);
        for provider in v1.snapshot.providers.values_mut() {
            let key = provider_capacity_key(provider);
            if let Some(weight) = ext.provider_capacity_weights.get(&key)
                && !apply_capacity_weight(provider, *weight)
            {
                tracing::warn!(
                    provider = %key,
                    capacity_weight = *weight,
                    "ignoring out-of-range remote provider capacity weight"
                );
            }
            if let Some(tools) = provider_state_extension.provider_tools.get(&key) {
                provider.tools.clone_from(tools);
            }
        }

        Ok(Self {
            version: v1.version,
            origin_site: v1.origin_site,
            revision: v1.revision,
            snapshot: v1.snapshot,
            authoritative_provider_state: provider_state_extension.authoritative_provider_state,
            gateway_address: ext.gateway_address,
            site_cert_pem: ext.site_cert_pem,
            signature: ext.signature,
            signed_at_ms: ext.signed_at_ms,
            grid_id: ext.grid_id,
            signals_address,
        })
    }

    /// Decode the trailing extension bytes, falling back across every prior
    /// wire format in turn. See [`decode`](Self::decode) for why a naive
    /// single-shot struct decode is not enough during a rolling update.
    #[expect(
        clippy::too_many_lines,
        reason = "ordered decoding of every historical extension shape is one compatibility chain"
    )]
    fn decode_extension(remaining: &[u8]) -> (BroadcastExtension, Option<String>, ProviderStateExtensionV1) {
        /// Decode `remaining` as one historical extension shape and the bytes it used.
        fn shape<T: serde::de::DeserializeOwned>(remaining: &[u8]) -> Option<(T, usize)> {
            bincode::serde::decode_from_slice::<T, _>(remaining, bincode::config::standard()).ok()
        }
        if remaining.is_empty() {
            return (BroadcastExtension::default(), None, ProviderStateExtensionV1::default());
        }
        if let Some((ext, used)) = shape::<BroadcastExtension>(remaining) {
            let after_extension = remaining.get(used..).unwrap_or(&[]);
            let Some((signals, signals_used)) = after_extension
                .is_empty()
                .then_some((None, 0))
                .or_else(|| shape::<Option<String>>(after_extension))
            else {
                return (ext, None, ProviderStateExtensionV1::default());
            };
            let framed = after_extension.get(signals_used..).unwrap_or(&[]);
            let provider_state = decode_provider_state_extension(framed);
            return (ext, signals, provider_state);
        }
        let ext = if let Some((ext, _)) = shape::<PreCapacityBroadcastExtension>(remaining) {
            BroadcastExtension {
                gateway_address: ext.gateway_address,
                site_cert_pem: ext.site_cert_pem,
                signature: ext.signature,
                signed_at_ms: ext.signed_at_ms,
                grid_id: ext.grid_id,
                ..BroadcastExtension::default()
            }
        } else if let Some((ext, _)) = shape::<PreTimestampBroadcastExtension>(remaining) {
            BroadcastExtension {
                gateway_address: ext.gateway_address,
                site_cert_pem: ext.site_cert_pem,
                signature: ext.signature,
                ..BroadcastExtension::default()
            }
        } else if let Some((ext, _)) = shape::<PreSignatureBroadcastExtension>(remaining) {
            BroadcastExtension {
                gateway_address: ext.gateway_address,
                site_cert_pem: ext.site_cert_pem,
                ..BroadcastExtension::default()
            }
        } else {
            // Compatibility fallback: bare String encoding for gateway_address only.
            BroadcastExtension {
                gateway_address: shape::<String>(remaining).map(|(gateway, _)| gateway),
                ..BroadcastExtension::default()
            }
        };
        (ext, None, ProviderStateExtensionV1::default())
    }
}

/// Decode a framed provider-state extension, ignoring unknown or malformed
/// frames so legacy metadata remains usable.
fn decode_provider_state_extension(remaining: &[u8]) -> ProviderStateExtensionV1 {
    if remaining.is_empty() {
        return ProviderStateExtensionV1::default();
    }
    let Ok((header, header_length)) =
        bincode::serde::decode_from_slice::<ProviderStateExtensionHeader, _>(remaining, bincode::config::standard())
    else {
        return ProviderStateExtensionV1::default();
    };
    if header.magic != PROVIDER_STATE_EXTENSION_MAGIC || header.version != PROVIDER_STATE_EXTENSION_VERSION_V1 {
        return ProviderStateExtensionV1::default();
    }
    let Ok(payload_length) = usize::try_from(header.payload_length) else {
        return ProviderStateExtensionV1::default();
    };
    let Some(payload_end) = header_length.checked_add(payload_length) else {
        return ProviderStateExtensionV1::default();
    };
    let Some(payload) = remaining.get(header_length..payload_end) else {
        return ProviderStateExtensionV1::default();
    };
    let Ok((extension, consumed)) =
        bincode::serde::decode_from_slice::<ProviderStateExtensionV1, _>(payload, bincode::config::standard())
    else {
        return ProviderStateExtensionV1::default();
    };
    if consumed == payload.len() {
        extension
    } else {
        ProviderStateExtensionV1::default()
    }
}

/// Apply a remote capacity extension only when it satisfies the shared
/// operator/CRDT contract. Invalid signed state must not overwrite a valid
/// local value or silently become a different capacity.
fn apply_capacity_weight(provider: &mut crdt::ProviderState, weight: u32) -> bool {
    if crdt::is_valid_capacity_weight(weight) {
        provider.capacity_weight = weight;
        true
    } else {
        false
    }
}

/// Build the stable extension key for one provider capacity.
fn provider_capacity_key(provider: &crdt::ProviderState) -> String {
    format!("{}/{}/{}", provider.network_id, provider.site_id, provider.provider_id)
}

/// Key used to replace stale queued broadcasts in foca.
#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::partial_pub_fields,
    reason = "grid and lane scope are internal; callers use origin_site + revision"
)]
pub struct StateBroadcastKey {
    /// Site that originated the broadcast.
    pub origin_site: String,

    /// Monotonic origin-local revision.
    pub revision: u64,

    /// Grid ID that owns a provider-state broadcast.
    ///
    /// Metadata lanes are site-global and therefore leave this unset.
    grid_id: Option<String>,

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
        self.origin_site == other.origin_site
            && self.grid_id == other.grid_id
            && self.kind == other.kind
            && self.revision >= other.revision
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

    /// The origin has a pinned identity but the broadcast carries no signature.
    #[error("state broadcast from pinned origin {origin_site} carries no signature")]
    MissingSignature {
        /// Site that originated the broadcast.
        origin_site: String,
    },

    /// The broadcast's signature does not verify against the pinned identity.
    #[error("state broadcast from pinned origin {origin_site} failed signature verification")]
    SignatureInvalid {
        /// Site that originated the broadcast.
        origin_site: String,
    },

    /// The broadcast's own bytes could not be re-encoded to check its signature.
    #[error("state broadcast from origin {origin_site} could not be re-encoded for signature verification: {source}")]
    SignableEncode {
        /// Site that originated the broadcast.
        origin_site: String,
        /// Underlying encode error.
        source: bincode::error::EncodeError,
    },

    /// The origin has a pinned identity but the broadcast carries no signing timestamp.
    #[error("state broadcast from pinned origin {origin_site} carries no signing timestamp")]
    MissingTimestamp {
        /// Site that originated the broadcast.
        origin_site: String,
    },

    /// The broadcast's signing timestamp falls outside the accepted freshness window.
    #[error(
        "state broadcast from pinned origin {origin_site} has a signing timestamp outside the accepted freshness window"
    )]
    TimestampOutOfWindow {
        /// Site that originated the broadcast.
        origin_site: String,
    },
}

/// Provider-state revision and ownership scope.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProviderStateScope {
    /// Site that originated the state.
    origin_site: String,
    /// Grid ID that owns the provider snapshot.
    grid_id: Option<String>,
}

/// Per-origin metadata retained by the broadcast handler.
#[derive(Default)]
struct RetainedOrigins {
    /// Highest provider-state revision received from each origin and Grid ID.
    latest_by_scope: BTreeMap<ProviderStateScope, u64>,
    /// Provider network IDs last associated with each origin and Grid ID.
    provider_networks_by_scope: BTreeMap<ProviderStateScope, BTreeSet<String>>,
    /// Gateway addresses received from each origin site.
    gateway_addrs: BTreeMap<String, String>,
    /// Highest gateway-address revision received from each origin.
    latest_gateway_revision_by_origin: BTreeMap<String, u64>,
    /// Public site certificate PEMs received from each origin site.
    cert_pems: BTreeMap<String, String>,
    /// Highest certificate revision received from each origin.
    latest_cert_revision_by_origin: BTreeMap<String, u64>,
    /// Signals addresses received from each origin site.
    signals_addrs: BTreeMap<String, String>,
    /// Highest signals-address revision received from each origin.
    latest_signals_revision_by_origin: BTreeMap<String, u64>,
}

/// One metadata lane, each with its own revision guard.
#[derive(Clone, Copy, Debug)]
enum Lane {
    /// Data-plane gateway address.
    Gateway,
    /// Public site certificate PEM.
    Cert,
    /// Signals address.
    Signals,
}

/// Revisions in one durable operator lease.
pub const REVISION_LEASE_SPAN: u64 = 256;

/// Highest leased revision a peer accepts at `now_ms`: one lease past the clock plus [`MAX_LEASE_SKEW`].
#[must_use]
pub fn max_leased_revision(now_ms: u64) -> u64 {
    let skew = u64::try_from(MAX_LEASE_SKEW.as_millis()).unwrap_or(u64::MAX);
    now_ms.saturating_add(REVISION_LEASE_SPAN).saturating_add(skew)
}

/// Highest revision `lane` accepts at `now`, leased milliseconds for addresses and nanoseconds for certificates.
fn max_metadata_revision(lane: Lane, now: Duration) -> u64 {
    match lane {
        Lane::Gateway | Lane::Signals => max_leased_revision(u64::try_from(now.as_millis()).unwrap_or(u64::MAX)),
        Lane::Cert => u64::try_from(now.saturating_add(MAX_LEASE_SKEW).as_nanos()).unwrap_or(u64::MAX),
    }
}

/// Whether a `held` revision outranks `incoming`, a held one past `cap` yielding to any within it.
fn outranks(held: u64, incoming: u64, cap: u64) -> bool {
    held > incoming && (held <= cap || incoming > cap)
}

/// Time since the Unix epoch, zero for a clock before it.
fn wall_clock() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO)
}

/// Wall-clock milliseconds.
fn wall_ms() -> u64 {
    u64::try_from(wall_clock().as_millis()).unwrap_or(u64::MAX)
}

impl RetainedOrigins {
    /// The values and revision guard for `lane`.
    fn lane_mut(&mut self, lane: Lane) -> (&mut BTreeMap<String, String>, &mut BTreeMap<String, u64>) {
        match lane {
            Lane::Gateway => (&mut self.gateway_addrs, &mut self.latest_gateway_revision_by_origin),
            Lane::Cert => (&mut self.cert_pems, &mut self.latest_cert_revision_by_origin),
            Lane::Signals => (&mut self.signals_addrs, &mut self.latest_signals_revision_by_origin),
        }
    }

    /// Return every origin represented by any retained map.
    fn known_origins(&self) -> BTreeSet<String> {
        self.latest_by_scope
            .keys()
            .map(|scope| &scope.origin_site)
            .chain(self.provider_networks_by_scope.keys().map(|scope| &scope.origin_site))
            .chain(self.gateway_addrs.keys())
            .chain(self.latest_gateway_revision_by_origin.keys())
            .chain(self.cert_pems.keys())
            .chain(self.latest_cert_revision_by_origin.keys())
            .chain(self.signals_addrs.keys())
            .chain(self.latest_signals_revision_by_origin.keys())
            .cloned()
            .collect()
    }

    /// Remove every revision and metadata value associated with one origin.
    fn remove(&mut self, origin: &str) {
        self.latest_by_scope.retain(|scope, _| scope.origin_site != origin);
        self.provider_networks_by_scope
            .retain(|scope, _| scope.origin_site != origin);
        self.gateway_addrs.remove(origin);
        self.latest_gateway_revision_by_origin.remove(origin);
        self.cert_pems.remove(origin);
        self.latest_cert_revision_by_origin.remove(origin);
        self.signals_addrs.remove(origin);
        self.latest_signals_revision_by_origin.remove(origin);
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
    /// Signals-address publisher.
    signals_addrs_tx: watch::Sender<BTreeMap<String, String>>,
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
        self.signals_addrs_tx.send_modify(|addresses| {
            addresses.remove(origin);
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

    /// Watch channel for signals address updates, keyed by origin site.
    signals_addrs_tx: watch::Sender<BTreeMap<String, String>>,

    /// Sender half of the pinned-identity trust store.
    ///
    /// Exposed via [`trust_store_sender`](Self::trust_store_sender) so a
    /// caller (e.g. the operator, once it has established which certificate
    /// pins an origin site's signing identity) can populate or update
    /// entries at runtime, after this handler has been moved into foca.
    /// Prefer [`crate::node::SwimNode::pin_origin`] over mutating this
    /// sender directly: pinning through the raw sender does not purge state
    /// merged from an origin before it was pinned, which is a correctness
    /// gap for a signature to close, not just an inconvenience.
    trust_store_tx: watch::Sender<TrustStore>,

    /// Receiver half read synchronously by [`receive_item`](foca::BroadcastHandler::receive_item).
    trust_store_rx: watch::Receiver<TrustStore>,

    /// Hard bound for per-origin revision and metadata maps.
    max_origins: usize,

    /// Hard bound for provider-state revision and ownership scopes.
    max_scopes: usize,
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
    /// previously retained unpinned origin is removed deterministically before
    /// a new origin is accepted. If all retained origins are pinned, new origins
    /// are refused without disturbing existing state or revision guards.
    #[must_use]
    pub(crate) fn with_capacity(site_id: String, max_origins: usize) -> (Self, OriginStateHandle) {
        let (tx, _) = watch::channel(GridStateSnapshot::new(site_id));
        let (gw_tx, _) = watch::channel(BTreeMap::new());
        let (cert_tx, _) = watch::channel(BTreeMap::new());
        let (signals_tx, _) = watch::channel(BTreeMap::new());
        let (trust_tx, trust_rx) = watch::channel(TrustStore::new());
        let max_origins = max_origins.max(1);
        let max_scopes = max_origins.saturating_mul(RETAINED_SCOPE_MULTIPLIER).max(1);
        let retained = Arc::new(Mutex::new(RetainedOrigins::default()));
        let control = OriginStateHandle {
            retained: Arc::clone(&retained),
            state_tx: tx.clone(),
            gateway_addrs_tx: gw_tx.clone(),
            cert_pems_tx: cert_tx.clone(),
            signals_addrs_tx: signals_tx.clone(),
        };
        (
            Self {
                state_tx: tx,
                retained,
                gateway_addrs_tx: gw_tx,
                cert_pems_tx: cert_tx,
                signals_addrs_tx: signals_tx,
                trust_store_tx: trust_tx,
                trust_store_rx: trust_rx,
                max_origins,
                max_scopes,
            },
            control,
        )
    }

    /// Return a sender for updating the pinned-identity trust store.
    ///
    /// Clone and hold this to push pinned identities in after `self` has
    /// been moved into foca. An origin site with no entry is not yet
    /// enforced against a signature — see [`receive_item`]'s doc comment for
    /// the rollout-transition rationale.
    ///
    /// This is the low-level primitive [`crate::node::SwimNode::pin_origin`]
    /// is built on. Prefer `pin_origin` for real use: sending directly
    /// through this sender skips the purge of state merged from an origin
    /// before it had a pin. This accessor stays public for tests and for
    /// callers that hold a bare [`StateBroadcastHandler`] outside a
    /// [`SwimNode`](crate::node::SwimNode).
    ///
    /// [`receive_item`]: foca::BroadcastHandler::receive_item
    #[must_use]
    pub fn trust_store_sender(&self) -> watch::Sender<TrustStore> {
        self.trust_store_tx.clone()
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

    /// Return a receiver for the signals address map. Subscribe before moving `self` into foca.
    pub fn subscribe_signals_addrs(&self) -> watch::Receiver<BTreeMap<String, String>> {
        self.signals_addrs_tx.subscribe()
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

    /// Store and publish `value` on `lane` unless a newer revision holds it, `true` when refused as from the future.
    fn store_latest(&self, lane: Lane, broadcast: &StateBroadcast, value: &String) -> bool {
        let origin = &broadcast.origin_site;
        let cap = max_metadata_revision(lane, wall_clock());
        if broadcast.revision > cap {
            tracing::warn!(origin = %origin, revision = broadcast.revision, ?lane, "refusing a future metadata revision");
            return true;
        }
        let mut retained = self.retained.lock().unwrap_or_else(PoisonError::into_inner);
        let (values, revisions) = retained.lane_mut(lane);
        if revisions
            .get(origin)
            .is_some_and(|held| outranks(*held, broadcast.revision, cap))
        {
            return false;
        }
        revisions.insert(origin.clone(), broadcast.revision);
        values.insert(origin.clone(), value.clone());
        drop(retained);
        // Republishing an unchanged value must not wake every reader.
        self.lane_tx(lane).send_if_modified(|map| {
            if map.get(origin) == Some(value) {
                return false;
            }
            map.insert(origin.clone(), value.clone());
            true
        });
        false
    }

    /// The watch publishing `lane`.
    fn lane_tx(&self, lane: Lane) -> &watch::Sender<BTreeMap<String, String>> {
        match lane {
            Lane::Gateway => &self.gateway_addrs_tx,
            Lane::Cert => &self.cert_pems_tx,
            Lane::Signals => &self.signals_addrs_tx,
        }
    }

    /// Store every metadata value a broadcast carries, `true` when each was refused as from the future.
    fn store_metadata(&self, broadcast: &StateBroadcast) -> bool {
        let (carried, refused) = [
            (Lane::Gateway, broadcast.gateway_address.as_ref()),
            (Lane::Cert, broadcast.site_cert_pem.as_ref()),
            (Lane::Signals, broadcast.signals_address.as_ref()),
        ]
        .into_iter()
        .filter_map(|(lane, value)| value.map(|value| self.store_latest(lane, broadcast, value)))
        .fold((false, true), |(_, all), refused| (true, all && refused));
        carried && refused
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
        self.signals_addrs_tx.send_modify(|addresses| {
            addresses.remove(origin);
        });
    }

    /// Reject a broadcast that fails signature verification against a
    /// pinned identity.
    ///
    /// An origin with **no** entry (or an empty entry) in the trust store
    /// passes through unchecked — this is deliberate: it lets a
    /// signed-broadcast rollout proceed incrementally as origins are pinned
    /// one at a time, rather than requiring a synchronized flag-day
    /// cutover. Once nerdalert's key-source question (grid#75) is resolved
    /// and the operator starts populating pins, this becomes the
    /// enforcement point; until then it is a no-op for every unpinned
    /// origin. An origin pinned to more than one key (rotation overlap)
    /// verifies against **any** key in its set.
    fn verify_signature_if_pinned(&self, broadcast: &StateBroadcast) -> Result<(), StateBroadcastError> {
        let pinned_keys = self
            .trust_store_rx
            .borrow()
            .get(&broadcast.origin_site)
            .cloned()
            .unwrap_or_default();
        if pinned_keys.is_empty() {
            return Ok(());
        }
        let (signature, signed_at_ms) = Self::required_signature_fields(broadcast)?;
        let signable = Self::signable_for_verification(broadcast)?;
        let verified = pinned_keys
            .iter()
            .any(|key| crate::signing::verify_ecdsa_p256(key, &signable, signature).is_ok());
        if !verified {
            tracing::warn!(origin_site = %broadcast.origin_site, reason = "signature_invalid", REJECTED_BROADCAST_LOG);
            return Err(StateBroadcastError::SignatureInvalid {
                origin_site: broadcast.origin_site.clone(),
            });
        }
        if !Self::within_freshness_window(signed_at_ms) {
            tracing::warn!(origin_site = %broadcast.origin_site, reason = "timestamp_out_of_window", REJECTED_BROADCAST_LOG);
            return Err(StateBroadcastError::TimestampOutOfWindow {
                origin_site: broadcast.origin_site.clone(),
            });
        }
        Ok(())
    }

    /// Return the signature and timestamp required for a pinned origin.
    fn required_signature_fields(broadcast: &StateBroadcast) -> Result<(&[u8], u64), StateBroadcastError> {
        let Some(signature) = broadcast.signature.as_ref() else {
            tracing::warn!(origin_site = %broadcast.origin_site, reason = "missing_signature", REJECTED_BROADCAST_LOG);
            return Err(StateBroadcastError::MissingSignature {
                origin_site: broadcast.origin_site.clone(),
            });
        };
        let Some(signed_at_ms) = broadcast.signed_at_ms else {
            tracing::warn!(origin_site = %broadcast.origin_site, reason = "missing_timestamp", REJECTED_BROADCAST_LOG);
            return Err(StateBroadcastError::MissingTimestamp {
                origin_site: broadcast.origin_site.clone(),
            });
        };
        Ok((signature, signed_at_ms))
    }

    /// Encode the bytes authenticated by a pinned origin's signature.
    fn signable_for_verification(broadcast: &StateBroadcast) -> Result<Vec<u8>, StateBroadcastError> {
        match broadcast.signable_bytes() {
            Ok(bytes) => Ok(bytes),
            Err(source) => {
                tracing::warn!(origin_site = %broadcast.origin_site, reason = "signable_encode", REJECTED_BROADCAST_LOG);
                Err(StateBroadcastError::SignableEncode {
                    origin_site: broadcast.origin_site.clone(),
                    source,
                })
            },
        }
    }

    /// Return true when `signed_at_ms` falls within the accepted freshness
    /// window relative to this node's own clock.
    ///
    /// A [`SystemTime::now`] that somehow predates the Unix epoch (a
    /// pathologically misconfigured clock) is treated as epoch zero rather
    /// than propagating an error — every positive `signed_at_ms` then falls
    /// outside the window and is rejected, failing closed rather than
    /// disabling the freshness check entirely.
    fn within_freshness_window(signed_at_ms: u64) -> bool {
        let now_ms = wall_ms();
        let not_too_old = now_ms.saturating_sub(signed_at_ms) <= MAX_BROADCAST_AGE_MS;
        let not_too_far_future = signed_at_ms.saturating_sub(now_ms) <= MAX_CLOCK_SKEW_AHEAD_MS;
        not_too_old && not_too_far_future
    }

    /// Enforce the hard origin bound before accepting an unknown origin.
    ///
    /// Never evicts a pinned origin's retained state: a signature pin is an
    /// authenticated fact about that origin, and evicting it would silently
    /// discard the anti-replay value of the origin's tracked revision (see
    /// [`verify_signature_if_pinned`](Self::verify_signature_if_pinned)) the
    /// moment memory pressure forces a choice. If every retained origin is
    /// pinned, returns false and refuses the unknown origin. Existing origins
    /// may still update their state while the capacity is full.
    fn make_room_for(&self, incoming_origin: &str) -> bool {
        let origins = self.known_origins();
        if origins.contains(incoming_origin) || origins.len() < self.max_origins {
            return true;
        }
        let trust_store = self.trust_store_rx.borrow();
        let Some(origin) = origins
            .into_iter()
            .find(|origin| trust_store.get(origin).is_none_or(Vec::is_empty))
        else {
            tracing::warn!(
                max_origins = self.max_origins,
                "SWIM state origin capacity reached and every retained origin is pinned; \
                 refusing the unknown origin"
            );
            return false;
        };
        drop(trust_store);
        tracing::warn!(
            evicted_origin = %origin,
            max_origins = self.max_origins,
            "SWIM state origin capacity reached; evicting retained origin"
        );
        self.remove_origin(&origin);
        true
    }

    /// Return whether the bounded provider-state maps can retain `scope`.
    fn has_room_for_scope(&self, scope: &ProviderStateScope) -> bool {
        let retained = self.retained.lock().unwrap_or_else(PoisonError::into_inner);
        retained.latest_by_scope.contains_key(scope) || retained.latest_by_scope.len() < self.max_scopes
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
        self.verify_signature_if_pinned(&broadcast)?;
        if !self.make_room_for(&broadcast.origin_site) {
            return Ok(None);
        }

        // Metadata-only broadcasts (gateway address or cert PEM, empty CRDT
        // snapshot) have independent revision lanes. They must not be rejected
        // by an unrelated provider-state revision.
        if broadcast.is_metadata_only() {
            // Gossiping on a revision every peer refuses would only spread it.
            return Ok((!self.store_metadata(&broadcast)).then(|| broadcast.key()));
        }

        let provider_scope = broadcast.provider_state_scope();
        if !self.has_room_for_scope(&provider_scope) {
            tracing::warn!(
                origin = %broadcast.origin_site,
                grid_id = ?broadcast.grid_id,
                max_scopes = self.max_scopes,
                "SWIM provider-state scope capacity reached; ignoring unknown scope"
            );
            return Ok(None);
        }
        let (latest, previous_networks) = {
            let retained = self.retained.lock().unwrap_or_else(PoisonError::into_inner);
            (
                retained.latest_by_scope.get(&provider_scope).copied(),
                retained
                    .provider_networks_by_scope
                    .get(&provider_scope)
                    .cloned()
                    .unwrap_or_default(),
            )
        };
        if latest.is_some_and(|held| outranks(held, broadcast.revision, max_leased_revision(wall_ms()))) {
            return Ok(None);
        }

        // Extensions on a non-stale state broadcast remain authoritative.
        self.store_metadata(&broadcast);

        if latest.is_some_and(|latest| latest == broadcast.revision) {
            return Ok(None);
        }

        let carries_provider_state = broadcast.carries_provider_state();
        let incoming_networks: BTreeSet<String> = broadcast
            .snapshot
            .providers
            .values()
            .filter(|provider| provider.site_id == broadcast.origin_site)
            .map(|provider| provider.network_id.clone())
            .collect();
        let networks_to_replace: BTreeSet<String> = previous_networks.union(&incoming_networks).cloned().collect();
        let mut accepted_networks = BTreeSet::new();
        self.state_tx.send_modify(|snap| {
            snap.capabilities.merge(&broadcast.snapshot.capabilities);
            snap.merge_tenant_spend_from_origin(&broadcast.origin_site, &broadcast.snapshot.tenant_spend);
            // A spend-only broadcast must not run the destructive
            // origin-provider replace below — it doesn't carry an
            // authoritative provider list for this cycle at all.
            if carries_provider_state {
                accepted_networks = snap.replace_origin_providers_in_networks(
                    &broadcast.origin_site,
                    broadcast.revision,
                    &networks_to_replace,
                    &broadcast.snapshot,
                );
            }
        });
        let mut retained = self.retained.lock().unwrap_or_else(PoisonError::into_inner);
        retained
            .latest_by_scope
            .insert(provider_scope.clone(), broadcast.revision);
        if !accepted_networks.is_empty() {
            retained
                .provider_networks_by_scope
                .insert(provider_scope, accepted_networks);
        }
        drop(retained);
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
            tools: Vec::new(),
            backend_kind: "local".to_owned(),
            capacity_weight: 1,
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

    /// Encode a base v1 payload plus a two-field pre-signature extension,
    /// mirroring the wire bytes a peer running the extension format that
    /// predates the `signature` field would have sent.
    fn encode_v1_plus_pre_signature_extension(
        origin_site: &str,
        revision: u64,
        gateway_address: Option<String>,
        site_cert_pem: Option<String>,
    ) -> Vec<u8> {
        let v1 = StateBroadcastV1 {
            version: STATE_BROADCAST_VERSION,
            origin_site: origin_site.to_owned(),
            revision,
            snapshot: snapshot(origin_site, revision, 0.4),
        };
        let mut bytes =
            bincode::serde::encode_to_vec(&v1, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        let ext = PreSignatureBroadcastExtension {
            gateway_address,
            site_cert_pem,
        };
        let ext_bytes =
            bincode::serde::encode_to_vec(&ext, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        bytes.extend_from_slice(&ext_bytes);
        bytes
    }

    /// Encode a base v1 payload plus a three-field pre-timestamp extension,
    /// mirroring the wire bytes a peer running the extension format that
    /// predates the `signed_at_ms` field would have sent.
    fn encode_v1_plus_pre_timestamp_extension(
        origin_site: &str,
        revision: u64,
        gateway_address: Option<String>,
        site_cert_pem: Option<String>,
        signature: Option<Vec<u8>>,
    ) -> Vec<u8> {
        let v1 = StateBroadcastV1 {
            version: STATE_BROADCAST_VERSION,
            origin_site: origin_site.to_owned(),
            revision,
            snapshot: snapshot(origin_site, revision, 0.4),
        };
        let mut bytes =
            bincode::serde::encode_to_vec(&v1, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        let ext = PreTimestampBroadcastExtension {
            gateway_address,
            site_cert_pem,
            signature,
        };
        let ext_bytes =
            bincode::serde::encode_to_vec(&ext, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        bytes.extend_from_slice(&ext_bytes);
        bytes
    }

    /// Encode the extension shape used immediately before capacity weights.
    fn encode_v1_plus_pre_capacity_extension(origin_site: &str, revision: u64) -> Vec<u8> {
        let v1 = StateBroadcastV1 {
            version: STATE_BROADCAST_VERSION,
            origin_site: origin_site.to_owned(),
            revision,
            snapshot: snapshot(origin_site, revision, 0.4),
        };
        let mut bytes =
            bincode::serde::encode_to_vec(&v1, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        let ext = PreCapacityBroadcastExtension {
            gateway_address: Some("10.0.0.8:8443".to_owned()),
            site_cert_pem: None,
            signature: None,
            signed_at_ms: Some(1_700_000_000_000),
            grid_id: Some("net".to_owned()),
        };
        let ext_bytes =
            bincode::serde::encode_to_vec(&ext, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        bytes.extend_from_slice(&ext_bytes);
        bytes
    }

    /// Reproduce the bytes an immediately pre-capacity peer signs.
    fn pre_capacity_signable_bytes(broadcast: &StateBroadcast) -> Vec<u8> {
        let v1 = StateBroadcastV1 {
            version: broadcast.version,
            origin_site: broadcast.origin_site.clone(),
            revision: broadcast.revision,
            snapshot: broadcast.snapshot.clone(),
        };
        let mut encoded =
            bincode::serde::encode_to_vec(&v1, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        let extension = PreCapacityBroadcastExtension {
            gateway_address: broadcast.gateway_address.clone(),
            site_cert_pem: broadcast.site_cert_pem.clone(),
            signature: None,
            signed_at_ms: broadcast.signed_at_ms,
            grid_id: broadcast.grid_id.clone(),
        };
        encoded.extend(
            bincode::serde::encode_to_vec(&extension, bincode::config::standard())
                .unwrap_or_else(|_| std::process::abort()),
        );
        let mut signable = SIGNATURE_DOMAIN.to_vec();
        signable.extend(encoded);
        signable
    }

    /// Return the current wall-clock time in milliseconds since the Unix
    /// epoch, for constructing test broadcasts with a fresh `signed_at_ms`.
    fn now_ms() -> u64 {
        u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| std::process::abort())
                .as_millis(),
        )
        .unwrap_or_else(|_| std::process::abort())
    }

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

    #[test]
    fn stored_pre_signature_extension_bytes_remain_decodable() -> Result<(), bincode::error::DecodeError> {
        let historical = [1, 1, b'a', 1, 1, b'a', 0, 0, 1, b'a', 0, 0, 0, 1, 1, b'g', 0];
        let decoded = StateBroadcast::decode(&historical)?;

        assert_eq!(decoded.origin_site, "a", "the historical origin identity must survive");
        assert_eq!(decoded.revision, 1, "the historical origin revision must survive");
        assert_eq!(
            decoded.snapshot,
            GridStateSnapshot::new("a".to_owned()),
            "historical bytes must preserve the empty origin snapshot"
        );
        assert_eq!(
            decoded.gateway_address.as_deref(),
            Some("g"),
            "the historical extension must preserve its gateway address"
        );
        assert!(
            decoded.site_cert_pem.is_none(),
            "the historical extension must preserve an absent site certificate"
        );
        assert!(
            decoded.signature.is_none(),
            "old extension bytes must not manufacture a signature"
        );
        assert!(
            !decoded.authoritative_provider_state,
            "old metadata cannot become a withdrawal"
        );
        Ok(())
    }

    #[test]
    fn receive_item_accepts_a_correctly_signed_broadcast_from_a_pinned_origin() {
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 8);
        let (pkcs8_der, raw_pubkey) = generate_signing_key_and_pubkey();
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-p".to_owned(), vec![raw_pubkey])));

        let unsigned = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None)
            .with_signed_at(Some(now_ms()));
        let signature = crate::signing::sign_ecdsa_p256(
            &pkcs8_der,
            &unsigned.signable_bytes().unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        let signed = unsigned.with_signature(Some(signature));

        let key = receive(&mut handler, &signed);

        assert!(
            key.is_some(),
            "a correctly signed broadcast from a pinned origin must be accepted"
        );
        assert!(
            handler.snapshot().provider("net", "site-p", "provider").is_some(),
            "the signed broadcast's provider state must be merged"
        );
    }

    #[test]
    fn receive_item_rejects_an_unsigned_broadcast_from_a_pinned_origin() {
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 8);
        let (_pkcs8_der, raw_pubkey) = generate_signing_key_and_pubkey();
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-p".to_owned(), vec![raw_pubkey])));
        let unsigned = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None);
        let bytes = unsigned.encode().unwrap_or_else(|_| std::process::abort());

        let result = handler.receive_item(&bytes, None);

        assert!(
            matches!(&result, Err(StateBroadcastError::MissingSignature { origin_site }) if origin_site.as_str() == "site-p"),
            "an unsigned broadcast from a pinned origin must be rejected, got {result:?}"
        );
        assert!(
            handler.snapshot().provider("net", "site-p", "provider").is_none(),
            "a rejected broadcast must not be merged into the snapshot"
        );
    }

    #[test]
    fn receive_item_rejects_a_broadcast_signed_by_the_wrong_key_for_a_pinned_origin() {
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 8);
        let (_correct_key, pinned_pubkey) = generate_signing_key_and_pubkey();
        let (wrong_key, _wrong_pubkey) = generate_signing_key_and_pubkey();
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-p".to_owned(), vec![pinned_pubkey])));

        let unsigned = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None)
            .with_signed_at(Some(now_ms()));
        let signature = crate::signing::sign_ecdsa_p256(
            &wrong_key,
            &unsigned.signable_bytes().unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        let bytes = unsigned
            .with_signature(Some(signature))
            .encode()
            .unwrap_or_else(|_| std::process::abort());

        let result = handler.receive_item(&bytes, None);

        assert!(
            matches!(&result, Err(StateBroadcastError::SignatureInvalid { origin_site }) if origin_site.as_str() == "site-p"),
            "a broadcast signed by a key other than the pinned one must be rejected, got {result:?}"
        );
    }

    #[test]
    fn receive_item_rejects_a_broadcast_whose_revision_was_tampered_after_signing() {
        // A relay that forwards someone else's broadcast is a legitimate path
        // (A -> B -> C); a relay that *edits* the payload in transit is not.
        // Sign with the correct key, then mutate the struct before encoding
        // to simulate exactly that: bytes that left the origin correctly
        // signed but arrived changed.
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 8);
        let (signing_key, pubkey) = generate_signing_key_and_pubkey();
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-p".to_owned(), vec![pubkey])));

        let unsigned = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None)
            .with_signed_at(Some(now_ms()));
        let signature = crate::signing::sign_ecdsa_p256(
            &signing_key,
            &unsigned.signable_bytes().unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        let mut tampered = unsigned.with_signature(Some(signature));
        tampered.revision = 99;
        let bytes = tampered.encode().unwrap_or_else(|_| std::process::abort());

        let result = handler.receive_item(&bytes, None);

        assert!(
            matches!(&result, Err(StateBroadcastError::SignatureInvalid { origin_site }) if origin_site.as_str() == "site-p"),
            "a broadcast edited after signing must fail verification even though the signature itself is well-formed, got {result:?}"
        );
    }

    #[test]
    fn receive_item_rejects_a_broadcast_whose_tenant_spend_was_tampered_after_signing() {
        // The concrete attack grid#75 is worried about: a relay (or a peer
        // holding only the shared SWIM key) inflates a tenant's reported
        // spend on someone else's correctly-signed broadcast. Proves that
        // attack lands as a rejected broadcast, not a silently-merged one.
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 8);
        let (signing_key, pubkey) = generate_signing_key_and_pubkey();
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-p".to_owned(), vec![pubkey])));

        let mut snap = snapshot("site-p", 1, 0.1);
        snap.increment_tenant_spend("tenant-x", 500);
        let unsigned = StateBroadcast::new("site-p".to_owned(), 1, snap, None).with_signed_at(Some(now_ms()));
        let signature = crate::signing::sign_ecdsa_p256(
            &signing_key,
            &unsigned.signable_bytes().unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        let mut tampered = unsigned.with_signature(Some(signature));
        tampered.snapshot.increment_tenant_spend("tenant-x", 1_000_000);
        let bytes = tampered.encode().unwrap_or_else(|_| std::process::abort());

        let result = handler.receive_item(&bytes, None);

        assert!(
            matches!(&result, Err(StateBroadcastError::SignatureInvalid { origin_site }) if origin_site.as_str() == "site-p"),
            "an inflated tenant_spend added after signing must be rejected, got {result:?}"
        );
        assert!(
            !handler.snapshot().tenant_spend.contains_key("tenant-x"),
            "a rejected tampered broadcast must not merge its forged tenant_spend into the snapshot"
        );
    }

    #[test]
    fn receive_item_rejects_a_broadcast_signed_by_a_key_dropped_from_a_completed_rotation() {
        // Same rejection path as the wrong-key test, but exercised as a
        // rotation *completion*: the old key is fully retired (not just
        // outnumbered by a new one), so a straggler still signing with it
        // must be rejected exactly like an impostor would be.
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 8);
        let (retired_key, retired_pubkey) = generate_signing_key_and_pubkey();
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-p".to_owned(), vec![retired_pubkey])));
        let (_current_key, current_pubkey) = generate_signing_key_and_pubkey();
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-p".to_owned(), vec![current_pubkey])));

        let unsigned = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None)
            .with_signed_at(Some(now_ms()));
        let signature = crate::signing::sign_ecdsa_p256(
            &retired_key,
            &unsigned.signable_bytes().unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        let bytes = unsigned
            .with_signature(Some(signature))
            .encode()
            .unwrap_or_else(|_| std::process::abort());

        let result = handler.receive_item(&bytes, None);

        assert!(
            matches!(&result, Err(StateBroadcastError::SignatureInvalid { origin_site }) if origin_site.as_str() == "site-p"),
            "a broadcast signed by a key retired at the end of rotation must be rejected, got {result:?}"
        );
    }

    #[test]
    fn receive_item_rejects_a_malformed_signature_without_panicking() {
        // Distinct from "signed by the wrong key": this signature isn't a
        // well-formed ECDSA signature at all, proving the verifier fails
        // closed on garbage input instead of panicking on it.
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 8);
        let (_signing_key, pubkey) = generate_signing_key_and_pubkey();
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-p".to_owned(), vec![pubkey])));

        let unsigned = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None)
            .with_signed_at(Some(now_ms()));
        let bytes = unsigned
            .with_signature(Some(vec![0xFF; 4]))
            .encode()
            .unwrap_or_else(|_| std::process::abort());

        let result = handler.receive_item(&bytes, None);

        assert!(
            matches!(&result, Err(StateBroadcastError::SignatureInvalid { origin_site }) if origin_site.as_str() == "site-p"),
            "a malformed signature must be rejected cleanly, not panic, got {result:?}"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "verifies acceptance under both the 'next' and 'current' pinned keys in one proof"
    )]
    fn receive_item_accepts_a_broadcast_signed_by_either_key_in_a_rotated_pin_set() {
        // Bounded key rotation: an origin pinned to [current, next] must
        // verify against a signature made with *either* key, mirroring
        // canonical_fingerprints' current+next mTLS pin overlap window.
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 8);
        let (current_key, current_pubkey) = generate_signing_key_and_pubkey();
        let (next_key, next_pubkey) = generate_signing_key_and_pubkey();
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-p".to_owned(), vec![current_pubkey, next_pubkey])));

        let unsigned_from_next = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None)
            .with_signed_at(Some(now_ms()));
        let signature_from_next = crate::signing::sign_ecdsa_p256(
            &next_key,
            &unsigned_from_next
                .signable_bytes()
                .unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        let signed_by_next = unsigned_from_next.with_signature(Some(signature_from_next));

        assert!(
            receive(&mut handler, &signed_by_next).is_some(),
            "a broadcast signed by the 'next' key in a rotated pin set must be accepted"
        );

        let unsigned_from_current = StateBroadcast::new("site-p".to_owned(), 2, snapshot("site-p", 2, 0.2), None)
            .with_signed_at(Some(now_ms()));
        let signature_from_current = crate::signing::sign_ecdsa_p256(
            &current_key,
            &unsigned_from_current
                .signable_bytes()
                .unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        let signed_by_current = unsigned_from_current.with_signature(Some(signature_from_current));

        assert!(
            receive(&mut handler, &signed_by_current).is_some(),
            "a broadcast signed by the 'current' key in a rotated pin set must also be accepted"
        );
    }

    #[test]
    fn receive_item_rejects_a_signed_broadcast_missing_a_timestamp_from_a_pinned_origin() {
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 8);
        let (pkcs8_der, raw_pubkey) = generate_signing_key_and_pubkey();
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-p".to_owned(), vec![raw_pubkey])));

        let unsigned = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None);
        let signature = crate::signing::sign_ecdsa_p256(
            &pkcs8_der,
            &unsigned.signable_bytes().unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        let bytes = unsigned
            .with_signature(Some(signature))
            .encode()
            .unwrap_or_else(|_| std::process::abort());

        let result = handler.receive_item(&bytes, None);

        assert!(
            matches!(&result, Err(StateBroadcastError::MissingTimestamp { origin_site }) if origin_site.as_str() == "site-p"),
            "a signed broadcast with no signing timestamp from a pinned origin must be rejected, got {result:?}"
        );
    }

    #[test]
    fn receive_item_rejects_a_signed_broadcast_with_a_stale_timestamp_from_a_pinned_origin() {
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 8);
        let (pkcs8_der, raw_pubkey) = generate_signing_key_and_pubkey();
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-p".to_owned(), vec![raw_pubkey])));
        let stale_at = now_ms().saturating_sub(MAX_BROADCAST_AGE_MS + 1_000);

        let unsigned = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None)
            .with_signed_at(Some(stale_at));
        let signature = crate::signing::sign_ecdsa_p256(
            &pkcs8_der,
            &unsigned.signable_bytes().unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        let bytes = unsigned
            .with_signature(Some(signature))
            .encode()
            .unwrap_or_else(|_| std::process::abort());

        let result = handler.receive_item(&bytes, None);

        assert!(
            matches!(&result, Err(StateBroadcastError::TimestampOutOfWindow { origin_site }) if origin_site.as_str() == "site-p"),
            "a signed broadcast with a stale timestamp from a pinned origin must be rejected, got {result:?}"
        );
    }

    #[test]
    fn receive_item_rejects_a_signed_broadcast_with_a_far_future_timestamp_from_a_pinned_origin() {
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 8);
        let (pkcs8_der, raw_pubkey) = generate_signing_key_and_pubkey();
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-p".to_owned(), vec![raw_pubkey])));
        let future_at = now_ms() + MAX_CLOCK_SKEW_AHEAD_MS + 1_000;

        let unsigned = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None)
            .with_signed_at(Some(future_at));
        let signature = crate::signing::sign_ecdsa_p256(
            &pkcs8_der,
            &unsigned.signable_bytes().unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        let bytes = unsigned
            .with_signature(Some(signature))
            .encode()
            .unwrap_or_else(|_| std::process::abort());

        let result = handler.receive_item(&bytes, None);

        assert!(
            matches!(&result, Err(StateBroadcastError::TimestampOutOfWindow { origin_site }) if origin_site.as_str() == "site-p"),
            "a signed broadcast with a far-future timestamp from a pinned origin must be rejected, got {result:?}"
        );
    }

    #[test]
    fn within_freshness_window_accepts_a_timestamp_comfortably_within_max_age() {
        let now = now_ms();
        assert!(
            StateBroadcastHandler::within_freshness_window(now.saturating_sub(MAX_BROADCAST_AGE_MS - 1_000)),
            "a timestamp just inside MAX_BROADCAST_AGE_MS old must be within the window"
        );
    }

    #[test]
    fn within_freshness_window_rejects_a_timestamp_older_than_max_age() {
        let now = now_ms();
        assert!(
            !StateBroadcastHandler::within_freshness_window(now.saturating_sub(MAX_BROADCAST_AGE_MS + 1_000)),
            "a timestamp older than MAX_BROADCAST_AGE_MS must fall outside the window"
        );
    }

    #[test]
    fn within_freshness_window_accepts_a_timestamp_comfortably_ahead_within_clock_skew() {
        let now = now_ms();
        assert!(
            StateBroadcastHandler::within_freshness_window(now + MAX_CLOCK_SKEW_AHEAD_MS - 1_000),
            "a timestamp just inside MAX_CLOCK_SKEW_AHEAD_MS ahead must be within the window"
        );
    }

    #[test]
    fn within_freshness_window_rejects_a_timestamp_further_ahead_than_clock_skew() {
        let now = now_ms();
        assert!(
            !StateBroadcastHandler::within_freshness_window(now + MAX_CLOCK_SKEW_AHEAD_MS + 1_000),
            "a timestamp further ahead than MAX_CLOCK_SKEW_AHEAD_MS must fall outside the window"
        );
    }

    #[test]
    fn missing_timestamp_error_message_names_the_origin() {
        let err = StateBroadcastError::MissingTimestamp {
            origin_site: "site-p".to_owned(),
        };
        assert!(
            err.to_string().contains("site-p"),
            "error message must name the origin site"
        );
    }

    #[test]
    fn timestamp_out_of_window_error_message_names_the_origin() {
        let err = StateBroadcastError::TimestampOutOfWindow {
            origin_site: "site-p".to_owned(),
        };
        assert!(
            err.to_string().contains("site-p"),
            "error message must name the origin site"
        );
    }

    /// Build and receive a correctly signed broadcast for a pinned origin.
    fn receive_signed(
        handler: &mut StateBroadcastHandler,
        origin: &str,
        revision: u64,
        signing_key: &[u8],
    ) -> Option<StateBroadcastKey> {
        let unsigned = StateBroadcast::new(origin.to_owned(), revision, snapshot(origin, revision, 0.1), None)
            .with_signed_at(Some(now_ms()));
        let signature = crate::signing::sign_ecdsa_p256(
            signing_key,
            &unsigned.signable_bytes().unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        receive(handler, &unsigned.with_signature(Some(signature)))
    }

    #[test]
    fn make_room_for_never_evicts_a_pinned_origin() {
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 2);
        let (signing_key, pubkey) = generate_signing_key_and_pubkey();
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-pinned".to_owned(), vec![pubkey])));

        drop(receive_signed(&mut handler, "site-pinned", 1, &signing_key));
        for (origin, revision) in [("site-unpinned", 2), ("site-new", 3)] {
            let broadcast = StateBroadcast::new(origin.to_owned(), revision, snapshot(origin, revision, 0.1), None);
            drop(receive(&mut handler, &broadcast));
        }

        let origins = handler.known_origins();
        assert!(
            origins.contains("site-pinned"),
            "a pinned origin must never be evicted for capacity, got {origins:?}"
        );
        assert!(
            !origins.contains("site-unpinned"),
            "an unpinned origin must be evicted ahead of a pinned one, got {origins:?}"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "capacity regression exercises signed and unsigned admissions, every metadata lane, and existing replay guards"
    )]
    fn full_pinned_capacity_refuses_unknown_state_without_mutating_any_lane() {
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 2);
        let (signing_key, public_key) = generate_signing_key_and_pubkey();
        for origin in ["site-pinned-a", "site-pinned-b"] {
            handler
                .trust_store_sender()
                .send_modify(|store| drop(store.insert(origin.to_owned(), vec![public_key.clone()])));
            drop(receive_signed(&mut handler, origin, 1, &signing_key));
        }
        let before = handler.subscribe().borrow().clone();
        for index in 0..4 {
            let origin = format!("site-new-{index}");
            let mut state = snapshot(&origin, 1, 0.1);
            state.increment_tenant_spend("tenant-new", 100);
            let broadcast = StateBroadcast::new(origin.clone(), 1, state, Some("gateway:8443".to_owned()))
                .with_cert(Some("certificate".to_owned()))
                .with_signals_address(Some("signals:9443".to_owned()));
            assert!(
                receive(&mut handler, &broadcast).is_none(),
                "unsigned new origins cannot bypass capacity"
            );
            handler
                .trust_store_sender()
                .send_modify(|store| drop(store.insert(origin.clone(), vec![public_key.clone()])));
            assert!(
                receive_signed(&mut handler, &origin, 1, &signing_key).is_none(),
                "new pins cannot bypass capacity"
            );
        }

        assert_eq!(
            *handler.subscribe().borrow(),
            before,
            "refusal must preserve providers, capabilities, and spend"
        );
        assert_eq!(
            handler.known_origins().len(),
            2,
            "repeated admission attempts cannot exceed the hard bound"
        );
        assert!(
            handler.subscribe_gateway_addrs().borrow().is_empty(),
            "refused metadata must not escape through a lane"
        );
        assert!(
            handler.subscribe_cert_pems().borrow().is_empty(),
            "refused certificate metadata must not escape through its lane"
        );
        assert!(
            handler.subscribe_signals_addrs().borrow().is_empty(),
            "refused signals addresses must not escape through their lane"
        );
        assert!(
            receive_signed(&mut handler, "site-pinned-a", 2, &signing_key).is_some(),
            "existing origins can advance at capacity"
        );
        assert!(
            receive_signed(&mut handler, "site-pinned-a", 1, &signing_key).is_none(),
            "retained replay guards remain active"
        );
    }

    #[test]
    fn signable_bytes_domain_prefix_is_load_bearing_not_decorative() {
        // Proves the domain-separation prefix actually participates in what
        // gets signed: a signature computed over signable_bytes() (prefix +
        // payload) must not verify against the bare, unprefixed encode() of
        // the same broadcast.
        let (key, pubkey) = generate_signing_key_and_pubkey();
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None);
        let signature = crate::signing::sign_ecdsa_p256(
            &key,
            &broadcast.signable_bytes().unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        let bare_encoded = broadcast.encode().unwrap_or_else(|_| std::process::abort());

        let result = crate::signing::verify_ecdsa_p256(&pubkey, &bare_encoded, &signature);

        assert!(
            result.is_err(),
            "a signature over the domain-prefixed signable_bytes() must not verify against the bare encoded payload"
        );
    }

    #[test]
    fn signable_bytes_cover_the_signals_address() {
        let plain = StateBroadcast::new("a".to_owned(), 1, GridStateSnapshot::new("a".to_owned()), None)
            .with_signed_at(Some(1));
        let with_signals = plain.clone().with_signals_address(Some("10.0.0.1:9091".to_owned()));
        assert_ne!(
            plain.signable_bytes().unwrap_or_else(|_| std::process::abort()),
            with_signals.signable_bytes().unwrap_or_else(|_| std::process::abort()),
            "a verifier that drops the field computes other bytes"
        );
    }

    #[test]
    fn signable_bytes_differ_for_broadcasts_that_differ_only_by_grid_id() {
        // Same origin_site, revision, and snapshot -- the only difference is
        // which GridNetwork the broadcast claims to belong to. If these
        // produced identical signable bytes, a signature valid for one
        // GridNetwork would silently double as valid for another sharing
        // the same cluster's SwimHandle (issue #48's per-tenant isolation).
        let base = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None);
        let for_grid_a = base.clone().with_grid_id(Some("grid-a".to_owned()));
        let for_grid_b = base.with_grid_id(Some("grid-b".to_owned()));

        let bytes_a = for_grid_a.signable_bytes().unwrap_or_else(|_| std::process::abort());
        let bytes_b = for_grid_b.signable_bytes().unwrap_or_else(|_| std::process::abort());

        assert_ne!(
            bytes_a, bytes_b,
            "signable_bytes() must depend on grid_id, or a signature would be replayable across GridNetworks"
        );
    }

    #[test]
    fn signed_pre_capacity_and_capacity_extensions_fail_closed_across_versions() {
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None)
            .with_grid_id(Some("net".to_owned()))
            .with_signed_at(Some(now_ms()));
        let legacy = pre_capacity_signable_bytes(&broadcast);
        let current = broadcast.signable_bytes().unwrap_or_else(|_| std::process::abort());
        assert_ne!(legacy, current, "wire extensions must have distinct signed bytes");

        let (key, public_key) = generate_signing_key_and_pubkey();
        let old_signature = crate::signing::sign_ecdsa_p256(&key, &legacy).unwrap_or_else(|_| std::process::abort());
        let new_signature = crate::signing::sign_ecdsa_p256(&key, &current).unwrap_or_else(|_| std::process::abort());

        assert!(
            crate::signing::verify_ecdsa_p256(&public_key, &current, &old_signature).is_err(),
            "new peers must reject signatures that do not cover the capacity extension"
        );
        assert!(
            crate::signing::verify_ecdsa_p256(&public_key, &legacy, &new_signature).is_err(),
            "old peers must reject signatures over an unknown extension"
        );
    }

    #[test]
    fn receive_item_rejects_a_signature_computed_for_a_different_grid_id() {
        // A signature made over a broadcast claiming grid_id="grid-a" must
        // not verify once the broadcast is re-labeled grid_id="grid-b" --
        // the cross-GridNetwork replay this field exists to prevent.
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 8);
        let (pkcs8_der, raw_pubkey) = generate_signing_key_and_pubkey();
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-p".to_owned(), vec![raw_pubkey])));

        let signed_at_ms = Some(now_ms());
        let for_grid_a = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None)
            .with_grid_id(Some("grid-a".to_owned()))
            .with_signed_at(signed_at_ms);
        let signature = crate::signing::sign_ecdsa_p256(
            &pkcs8_der,
            &for_grid_a.signable_bytes().unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());

        // Re-label as grid-b, reusing grid-a's signature over grid-a's bytes.
        let relabeled_as_grid_b = for_grid_a.with_grid_id(Some("grid-b".to_owned()));
        let bytes = relabeled_as_grid_b
            .with_signature(Some(signature))
            .encode()
            .unwrap_or_else(|_| std::process::abort());

        let result = handler.receive_item(&bytes, None);

        assert!(
            matches!(&result, Err(StateBroadcastError::SignatureInvalid { origin_site }) if origin_site.as_str() == "site-p"),
            "a broadcast re-labeled with a different grid_id than it was signed for must be rejected, got {result:?}"
        );
    }

    #[test]
    fn too_many_pinned_keys_error_message_is_descriptive() {
        let err = TooManyPinnedKeys {
            origin: "site-p".to_owned(),
            supplied: 3,
        };
        assert!(err.to_string().contains("site-p"), "error message must name the origin");
        assert!(
            err.to_string().contains('3'),
            "error message must name the supplied count"
        );
    }

    #[test]
    fn missing_signature_error_message_names_the_origin() {
        let err = StateBroadcastError::MissingSignature {
            origin_site: "site-p".to_owned(),
        };
        assert!(
            err.to_string().contains("site-p"),
            "error message must name the origin site"
        );
    }

    #[test]
    fn signature_invalid_error_message_names_the_origin() {
        let err = StateBroadcastError::SignatureInvalid {
            origin_site: "site-p".to_owned(),
        };
        assert!(
            err.to_string().contains("site-p"),
            "error message must name the origin site"
        );
    }

    #[test]
    fn signable_encode_error_message_names_the_origin_and_wraps_the_source() {
        // A genuine bincode encode failure on `StateBroadcast`'s own fields
        // (String/u64/u16/GridStateSnapshot, encoded to an in-memory Vec<u8>
        // with no writer I/O) has no reachable trigger through this crate's
        // public API; constructed directly to verify the message wording,
        // matching this codebase's `..._error_formats_correctly` convention
        // for defensive error variants (see e.g.
        // `node::tests::state_broadcast_error_formats_correctly`).
        let err = StateBroadcastError::SignableEncode {
            origin_site: "site-p".to_owned(),
            source: bincode::error::EncodeError::UnexpectedEnd,
        };
        assert!(
            err.to_string().contains("site-p"),
            "error message must name the origin site"
        );
    }

    #[test]
    fn receive_item_still_merges_an_unsigned_broadcast_from_an_origin_with_no_pinned_identity() {
        // Guards the incremental-rollout property documented on
        // `verify_signature_if_pinned`: no synchronized flag-day cutover.
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 8);
        let unsigned = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None);

        let key = receive(&mut handler, &unsigned);

        assert!(
            key.is_some(),
            "an unsigned broadcast from an unpinned origin must still be accepted"
        );
        assert!(
            handler.snapshot().provider("net", "site-p", "provider").is_some(),
            "the unsigned broadcast's provider state must be merged while the origin is unpinned"
        );
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
        let mut weighted = snapshot("site-p", 7, 0.1);
        weighted
            .providers
            .values_mut()
            .for_each(|provider| provider.capacity_weight = 70);
        let broadcast = StateBroadcast::new("site-p".to_owned(), 7, weighted, None);
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());
        let provider = decoded
            .snapshot
            .provider("net", "site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(decoded.version, STATE_BROADCAST_VERSION_V1, "version without gateway");
        assert_eq!(provider.metrics.queue_depth, Some(0.1), "metric value");
        assert_eq!(
            provider.capacity_weight, 70,
            "capacity extension must hydrate provider state"
        );
    }

    #[test]
    fn invalid_remote_capacity_cannot_overwrite_valid_provider_capacity() {
        let mut provider = snapshot("site-p", 7, 0.1)
            .provider("net", "site-p", "provider")
            .cloned()
            .unwrap_or_else(|| std::process::abort());
        provider.capacity_weight = 70;

        for invalid in [0, 1_001, u32::MAX] {
            assert!(!apply_capacity_weight(&mut provider, invalid));
            assert_eq!(provider.capacity_weight, 70);
        }
        assert!(apply_capacity_weight(&mut provider, 1));
        assert_eq!(provider.capacity_weight, 1);
        assert!(apply_capacity_weight(&mut provider, 1_000));
        assert_eq!(provider.capacity_weight, 1_000);
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "encodes the same broadcast in both wire shapes")]
    fn signals_address_rides_a_new_extension_older_peers_skip() {
        let base = StateBroadcast::new(
            "a".to_owned(),
            3,
            GridStateSnapshot::new("a".to_owned()),
            Some("gw:8080".to_owned()),
        );
        let with_signals = base.clone().with_signals_address(Some("[fd00::1]:9091".to_owned()));
        let round = StateBroadcast::decode(&with_signals.encode().unwrap_or_else(|_| std::process::abort()))
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(round.signals_address.as_deref(), Some("[fd00::1]:9091"));
        assert_eq!(round.gateway_address.as_deref(), Some("gw:8080"));

        let new_bytes = with_signals.encode().unwrap_or_else(|_| std::process::abort());
        let (v1, consumed): (StateBroadcastV1, usize) =
            bincode::serde::decode_from_slice(&new_bytes, bincode::config::standard())
                .unwrap_or_else(|_| std::process::abort());
        let (old, _): (BroadcastExtension, usize) =
            bincode::serde::decode_from_slice(new_bytes.get(consumed..).unwrap_or(&[]), bincode::config::standard())
                .unwrap_or_else(|_| std::process::abort());
        assert_eq!(v1.origin_site, "a");
        assert_eq!(
            old.gateway_address.as_deref(),
            Some("gw:8080"),
            "an older peer keeps the gateway"
        );

        let plain = base.encode().unwrap_or_else(|_| std::process::abort());
        let pre_signals = BroadcastExtension {
            gateway_address: Some("gw:8080".to_owned()),
            site_cert_pem: None,
            signature: None,
            signed_at_ms: None,
            grid_id: None,
            provider_capacity_weights: BTreeMap::new(),
        };
        let mut expected =
            bincode::serde::encode_to_vec(&v1, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        expected.extend(
            bincode::serde::encode_to_vec(&pre_signals, bincode::config::standard())
                .unwrap_or_else(|_| std::process::abort()),
        );
        assert_eq!(plain, expected, "without a signals address the bytes are unchanged");
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "encodes every extension field in both layouts")]
    fn encoding_keeps_the_previous_layout_byte_for_byte() {
        let mut snap = snapshot("a", 3, 0.5);
        for provider in snap.providers.values_mut() {
            provider.capacity_weight = 7;
        }
        let full = StateBroadcast::new("a".to_owned(), 3, snap, Some("gw:8080".to_owned()))
            .with_cert(Some("pem".to_owned()))
            .with_signature(Some(vec![1, 2, 3]))
            .with_signed_at(Some(42))
            .with_grid_id(Some("grid".to_owned()));
        let previous = |broadcast: &StateBroadcast| {
            let v1 = StateBroadcastV1 {
                version: broadcast.version,
                origin_site: broadcast.origin_site.clone(),
                revision: broadcast.revision,
                snapshot: broadcast.snapshot.clone(),
            };
            let ext = BroadcastExtension {
                gateway_address: broadcast.gateway_address.clone(),
                site_cert_pem: broadcast.site_cert_pem.clone(),
                signature: broadcast.signature.clone(),
                signed_at_ms: broadcast.signed_at_ms,
                grid_id: broadcast.grid_id.clone(),
                provider_capacity_weights: BTreeMap::from([("net/a/provider".to_owned(), 7)]),
            };
            let config = bincode::config::standard();
            let mut bytes = bincode::serde::encode_to_vec(&v1, config).unwrap_or_else(|_| std::process::abort());
            bytes.extend(bincode::serde::encode_to_vec(&ext, config).unwrap_or_else(|_| std::process::abort()));
            bytes
        };
        let encoded = full.encode().unwrap_or_else(|_| std::process::abort());
        assert_eq!(encoded, previous(&full), "no signals address, no new bytes");

        let with_signals = full.clone().with_signals_address(Some("[fd00::1]:9091".to_owned()));
        let mut expected = previous(&full);
        expected.extend(
            bincode::serde::encode_to_vec(Some("[fd00::1]:9091"), bincode::config::standard())
                .unwrap_or_else(|_| std::process::abort()),
        );
        assert_eq!(
            with_signals.encode().unwrap_or_else(|_| std::process::abort()),
            expected,
            "the address trails the extension"
        );

        let signals_only = StateBroadcast::new("a".to_owned(), 1, GridStateSnapshot::new("a".to_owned()), None)
            .with_signals_address(Some("10.0.0.1:9091".to_owned()));
        let round = StateBroadcast::decode(&signals_only.encode().unwrap_or_else(|_| std::process::abort()))
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(round.signals_address.as_deref(), Some("10.0.0.1:9091"));
        assert_eq!(round.gateway_address, None);
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "forged then real revisions on all three lanes")]
    fn a_metadata_revision_from_the_future_is_refused() {
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("local".to_owned(), 8);
        let empty = || GridStateSnapshot::new("a".to_owned());
        let addresses = |revision: u64, value: &str| {
            StateBroadcast::new("a".to_owned(), revision, empty(), Some(value.to_owned()))
                .with_signals_address(Some(value.to_owned()))
        };
        let cert = |revision: u64, value: &str| {
            StateBroadcast::new("a".to_owned(), revision, empty(), None).with_cert(Some(value.to_owned()))
        };
        let lanes = |held: &StateBroadcastHandler| {
            (
                held.gateway_addrs().get("a").cloned(),
                held.signals_addrs_tx.borrow().get("a").cloned(),
                held.cert_pems().get("a").cloned(),
            )
        };
        let now = wall_clock();
        let ms = |at: Duration| u64::try_from(at.as_millis()).unwrap_or(u64::MAX);
        let nanos = |at: Duration| u64::try_from(at.as_nanos()).unwrap_or(u64::MAX);
        assert!(
            receive(&mut handler, &addresses(u64::MAX, "forged")).is_none(),
            "not re-gossiped"
        );
        assert!(
            receive(
                &mut handler,
                &addresses(max_leased_revision(ms(now)) + 60_000, "forged")
            )
            .is_none(),
            "past one lease and the skew"
        );
        assert!(receive(&mut handler, &cert(u64::MAX, "forged")).is_none());
        assert!(
            receive(
                &mut handler,
                &cert(nanos(now + MAX_LEASE_SKEW + Duration::from_secs(60)), "forged")
            )
            .is_none(),
            "the cert lane shares the skew"
        );
        assert_eq!(lanes(&handler), (None, None, None), "no lane took a forged revision");

        drop(receive(&mut handler, &addresses(ms(now), "real")));
        drop(receive(&mut handler, &cert(nanos(now), "real")));
        let real = Some("real".to_owned());
        assert_eq!(
            lanes(&handler),
            (real.clone(), real.clone(), real),
            "real revisions land"
        );
    }

    #[test]
    fn a_held_revision_past_the_cap_yields_to_one_within_it() {
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("local".to_owned(), 8);
        let now = wall_ms();
        let old_scheme = now + (1 << 33);
        handler
            .retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .latest_gateway_revision_by_origin
            .insert("a".to_owned(), old_scheme);
        let gateway = |revision: u64, value: &str| {
            StateBroadcast::new(
                "a".to_owned(),
                revision,
                GridStateSnapshot::new("a".to_owned()),
                Some(value.to_owned()),
            )
        };
        assert!(receive(&mut handler, &gateway(now, "reseeded")).is_some(), "accepted");
        assert_eq!(handler.gateway_addrs().get("a").map(String::as_str), Some("reseeded"));
        assert!(receive(&mut handler, &gateway(now - 1, "older")).is_some());
        assert_eq!(
            handler.gateway_addrs().get("a").map(String::as_str),
            Some("reseeded"),
            "ordering within the cap holds"
        );
    }

    #[test]
    fn a_forged_revision_pins_a_lane_for_at_most_one_lease_and_the_skew() {
        let now = wall_ms();
        let forged = max_leased_revision(now);
        let skew = u64::try_from(MAX_LEASE_SKEW.as_millis()).unwrap_or(u64::MAX);
        assert_eq!(forged - now, REVISION_LEASE_SPAN + skew);
        assert!(outranks(forged, now, forged), "pins the lane while ahead of the clock");
        assert!(
            !outranks(forged, forged, max_leased_revision(forged)),
            "released once the clock reaches it"
        );
        let old_scheme = now + (1 << 33);
        assert!(
            !outranks(old_scheme, now, forged),
            "a far-ahead revision yields to one within the cap"
        );
        assert!(
            outranks(old_scheme + 1, old_scheme, forged),
            "far-ahead revisions keep their order"
        );
    }

    #[test]
    fn provider_state_from_a_reseeded_origin_replaces_a_far_ahead_one() {
        let mut handler = StateBroadcastHandler::new("local".to_owned());
        let now = wall_ms();
        assert!(
            receive(
                &mut handler,
                &StateBroadcast::new("a".to_owned(), now + (1 << 33), snapshot("a", 0, 1.0), None)
            )
            .is_some()
        );
        assert!(
            receive(
                &mut handler,
                &StateBroadcast::new("a".to_owned(), now, snapshot("a", 0, 2.0), None)
            )
            .is_some(),
            "a restarted origin under bounded leases is not locked out"
        );
    }

    #[test]
    fn a_broadcast_with_any_accepted_lane_is_gossiped() {
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("local".to_owned(), 8);
        let now = wall_clock();
        let ms = u64::try_from(now.as_millis()).unwrap_or(u64::MAX);
        let mixed = StateBroadcast::new(
            "a".to_owned(),
            ms,
            GridStateSnapshot::new("a".to_owned()),
            Some("gw".to_owned()),
        )
        .with_cert(Some("pem".to_owned()));
        assert!(receive(&mut handler, &mixed).is_some(), "the address lane took it");
    }

    #[test]
    fn an_unchanged_republish_wakes_no_reader() {
        let (mut handler, _control) = StateBroadcastHandler::with_capacity("local".to_owned(), 8);
        let mut gateway = handler.subscribe_gateway_addrs();
        let mut signals = handler.subscribe_signals_addrs();
        let addressed = |revision: u64| {
            StateBroadcast::new(
                "a".to_owned(),
                revision,
                GridStateSnapshot::new("a".to_owned()),
                Some("gw:8080".to_owned()),
            )
            .with_signals_address(Some("10.0.0.1:9091".to_owned()))
        };
        drop(receive(&mut handler, &addressed(1)));
        assert!(gateway.has_changed().unwrap_or(false) && signals.has_changed().unwrap_or(false));
        gateway.mark_unchanged();
        signals.mark_unchanged();
        drop(receive(&mut handler, &addressed(2)));
        assert!(!gateway.has_changed().unwrap_or(true), "same gateway, no wake");
        assert!(!signals.has_changed().unwrap_or(true), "same signals, no wake");
    }

    #[test]
    fn decode_pre_capacity_extension_preserves_old_fields_and_defaults_capacity() {
        let bytes = encode_v1_plus_pre_capacity_extension("site-old-peer", 6);

        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());
        let provider = decoded
            .snapshot
            .provider("net", "site-old-peer", "provider")
            .unwrap_or_else(|| std::process::abort());

        assert_eq!(decoded.gateway_address.as_deref(), Some("10.0.0.8:8443"));
        assert_eq!(decoded.signed_at_ms, Some(1_700_000_000_000));
        assert_eq!(decoded.grid_id.as_deref(), Some("net"));
        assert_eq!(
            provider.capacity_weight, 1,
            "older extension must default provider capacity"
        );
    }

    #[test]
    fn decode_recovers_gateway_and_cert_from_a_pre_signature_two_field_extension() {
        // Simulates a broadcast sent by a peer running the two-field
        // `BroadcastExtension` (gateway_address, site_cert_pem) that predates
        // the `signature` field added in this PR. bincode is not
        // self-describing: decoding those bytes as the current three-field
        // struct hits `UnexpectedEnd` while reading `signature`, and
        // `#[serde(default)]` never gets a chance to apply because the `?`
        // inside serde's generated `visit_seq` propagates that error first.
        // A rolling update must not silently drop `gateway_address`/
        // `site_cert_pem` for every broadcast from a not-yet-upgraded peer.
        let bytes = encode_v1_plus_pre_signature_extension(
            "site-old-peer",
            3,
            Some("10.0.0.9:8443".to_owned()),
            Some("-----BEGIN CERTIFICATE-----legacy-----END CERTIFICATE-----".to_owned()),
        );

        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            decoded.gateway_address.as_deref(),
            Some("10.0.0.9:8443"),
            "gateway_address from a pre-signature peer must survive decode, not be silently dropped"
        );
        assert_eq!(
            decoded.site_cert_pem.as_deref(),
            Some("-----BEGIN CERTIFICATE-----legacy-----END CERTIFICATE-----"),
            "site_cert_pem from a pre-signature peer must survive decode, not be silently dropped"
        );
        assert_eq!(decoded.signature, None, "a pre-signature peer never sends a signature");
    }

    #[test]
    fn decode_recovers_gateway_signature_and_cert_from_a_pre_timestamp_three_field_extension() {
        // Simulates a broadcast sent by a peer running the three-field
        // `PreTimestampBroadcastExtension` (gateway_address, site_cert_pem,
        // signature) that predates the `signed_at_ms` field added in this
        // fix. Same bincode non-self-describing failure mode as the
        // pre-signature case above, one field further down the chain.
        let signature = vec![9_u8, 8, 7, 6];
        let bytes = encode_v1_plus_pre_timestamp_extension(
            "site-old-peer",
            5,
            Some("10.0.0.4:8443".to_owned()),
            Some("-----BEGIN CERTIFICATE-----legacy-----END CERTIFICATE-----".to_owned()),
            Some(signature.clone()),
        );

        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            decoded.gateway_address.as_deref(),
            Some("10.0.0.4:8443"),
            "gateway_address from a pre-timestamp peer must survive decode, not be silently dropped"
        );
        assert_eq!(
            decoded.site_cert_pem.as_deref(),
            Some("-----BEGIN CERTIFICATE-----legacy-----END CERTIFICATE-----"),
            "site_cert_pem from a pre-timestamp peer must survive decode, not be silently dropped"
        );
        assert_eq!(
            decoded.signature,
            Some(signature),
            "signature from a pre-timestamp peer must survive decode, not be silently dropped"
        );
        assert_eq!(
            decoded.signed_at_ms, None,
            "a pre-timestamp peer never sends a signing timestamp"
        );
        assert_eq!(decoded.grid_id, None, "a pre-timestamp peer never sends a grid_id");
    }

    #[test]
    fn signed_at_ms_round_trips_through_encode_and_decode() {
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None)
            .with_signed_at(Some(1_700_000_000_000));
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());

        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            decoded.signed_at_ms,
            Some(1_700_000_000_000),
            "signed_at_ms must round-trip"
        );
    }

    #[test]
    fn grid_id_round_trips_through_encode_and_decode() {
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.1), None)
            .with_grid_id(Some("grid-a".to_owned()));
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());

        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());

        assert_eq!(decoded.grid_id.as_deref(), Some("grid-a"), "grid_id must round-trip");
    }

    #[test]
    fn decode_recovers_gateway_from_the_original_bare_string_extension() {
        // The oldest wire format, predating even the two-field
        // `PreSignatureBroadcastExtension`: a bare bincode-encoded `String`
        // for `gateway_address`, with no `site_cert_pem` or `signature`.
        // `decode_extension` falls all the way through to this tier only
        // after both newer struct shapes fail to decode, so it must stay
        // covered whenever that fallback chain is touched.
        let v1 = StateBroadcastV1 {
            version: STATE_BROADCAST_VERSION,
            origin_site: "site-ancient-peer".to_owned(),
            revision: 2,
            snapshot: snapshot("site-ancient-peer", 2, 0.2),
        };
        let mut bytes =
            bincode::serde::encode_to_vec(&v1, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        let gw_bytes = bincode::serde::encode_to_vec("10.0.0.5:8443".to_owned(), bincode::config::standard())
            .unwrap_or_else(|_| std::process::abort());
        bytes.extend_from_slice(&gw_bytes);

        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            decoded.gateway_address.as_deref(),
            Some("10.0.0.5:8443"),
            "gateway_address from the original bare-String wire format must survive decode"
        );
        assert_eq!(
            decoded.site_cert_pem, None,
            "the bare-String format never carries a cert"
        );
        assert_eq!(
            decoded.signature, None,
            "the bare-String format never carries a signature"
        );
    }

    #[test]
    fn decode_extension_returns_no_extension_fields_for_undecodable_trailing_bytes() {
        // Bytes that match none of the three known extension shapes -- the
        // final fallback in `decode_extension`'s chain must degrade to no
        // extension data rather than propagating a decode error, since the
        // trailing bytes might belong to a future format this peer doesn't
        // understand yet.
        let v1 = StateBroadcastV1 {
            version: STATE_BROADCAST_VERSION,
            origin_site: "site-p".to_owned(),
            revision: 1,
            snapshot: snapshot("site-p", 1, 0.1),
        };
        let mut bytes =
            bincode::serde::encode_to_vec(&v1, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        // A single 0xFF byte is not a valid bincode length prefix for a
        // `String`, `PreSignatureBroadcastExtension`, or `BroadcastExtension`.
        bytes.push(0xFF);

        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());

        assert_eq!(decoded.gateway_address, None);
        assert_eq!(decoded.site_cert_pem, None);
        assert_eq!(decoded.signature, None);
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
            grid_id: None,
            kind: StateBroadcastKeyKind::State,
        };
        let new = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 2,
            grid_id: None,
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
            grid_id: None,
            kind: StateBroadcastKeyKind::State,
        };
        let right = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 1,
            grid_id: None,
            kind: StateBroadcastKeyKind::State,
        };
        assert!(left.invalidates(&right), "same key must invalidate duplicate");
    }

    #[test]
    fn different_origins_do_not_invalidate_each_other() {
        let left = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 9,
            grid_id: None,
            kind: StateBroadcastKeyKind::State,
        };
        let right = StateBroadcastKey {
            origin_site: "site-q".to_owned(),
            revision: 1,
            grid_id: None,
            kind: StateBroadcastKeyKind::State,
        };
        assert!(
            !left.invalidates(&right),
            "different origins must not invalidate each other"
        );
    }

    #[test]
    fn different_grid_ids_do_not_invalidate_each_other() {
        let grid_a = StateBroadcast::new("site-p".to_owned(), 9, snapshot("site-p", 9, 0.4), None)
            .with_grid_id(Some("grid-a".to_owned()))
            .key();
        let grid_b = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.4), None)
            .with_grid_id(Some("grid-b".to_owned()))
            .key();

        assert!(
            !grid_a.invalidates(&grid_b),
            "one GridNetwork's state must not invalidate another GridNetwork's state"
        );
        assert!(
            !grid_b.invalidates(&grid_a),
            "GridNetwork state invalidation must remain scope-local in both revision directions"
        );
    }

    #[test]
    fn gateway_address_key_does_not_invalidate_state_key() {
        let gateway = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 99,
            grid_id: None,
            kind: StateBroadcastKeyKind::GatewayAddress,
        };
        let state = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 1,
            grid_id: None,
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
            grid_id: None,
            kind: StateBroadcastKeyKind::Cert,
        };
        let gateway = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 99,
            grid_id: None,
            kind: StateBroadcastKeyKind::GatewayAddress,
        };
        let state = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 1,
            grid_id: None,
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
    /// module's own cap logic (see `receive_item`'s `latest_by_scope` check).
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
    #[expect(
        clippy::too_many_lines,
        reason = "the eviction proof covers two GridNetwork scopes, metadata, and a lower-revision restart"
    )]
    fn coordinated_eviction_removes_all_origin_state_before_next_item() {
        let (mut handler, origin_state) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 4);
        let cert = "public-cert";
        let original = StateBroadcast::new(
            "site-a".to_owned(),
            10,
            snapshot("site-a", 10, 0.4),
            Some("10.0.0.1:8443".to_owned()),
        )
        .with_grid_id(Some("grid-a".to_owned()))
        .with_cert(Some(cert.to_owned()));
        let mut other_snapshot = snapshot("site-a", 11, 0.5);
        other_snapshot.providers.values_mut().for_each(|provider| {
            provider.network_id = "net-b".to_owned();
        });
        let other =
            StateBroadcast::new("site-a".to_owned(), 11, other_snapshot, None).with_grid_id(Some("grid-b".to_owned()));
        assert!(receive(&mut handler, &original).is_some());
        assert!(receive(&mut handler, &other).is_some());
        origin_state.remove_origin("site-a");

        assert!(handler.snapshot().provider("net", "site-a", "provider").is_none());
        assert!(handler.snapshot().provider("net-b", "site-a", "provider").is_none());
        assert!(handler.gateway_address_for_site("site-a").is_none());
        assert!(handler.cert_pem_for_site("site-a").is_none());

        let restarted = StateBroadcast::new(
            "site-a".to_owned(),
            1,
            snapshot("site-a", 1, 0.8),
            Some("10.0.0.9:8443".to_owned()),
        )
        .with_grid_id(Some("grid-a".to_owned()));
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

    #[test]
    fn provider_state_scope_maps_are_hard_bounded() {
        let (mut handler, _origin_state) = StateBroadcastHandler::with_capacity("site-local".to_owned(), 1);
        for index in 0..=RETAINED_SCOPE_MULTIPLIER {
            let network = format!("net-{index}");
            let mut snap = GridStateSnapshot::new("site-a".to_owned());
            let mut provider = provider_state("provider", vec!["model".to_owned()], Vec::new(), 1);
            provider.network_id.clone_from(&network);
            snap.upsert_provider(provider);
            let broadcast =
                StateBroadcast::new("site-a".to_owned(), 1, snap, None).with_grid_id(Some(format!("grid-{index}")));
            drop(receive(&mut handler, &broadcast));
        }

        let retained = handler.retained.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(retained.latest_by_scope.len(), RETAINED_SCOPE_MULTIPLIER);
        assert_eq!(retained.provider_networks_by_scope.len(), RETAINED_SCOPE_MULTIPLIER);
        drop(retained);
        assert!(
            handler
                .snapshot()
                .provider(&format!("net-{RETAINED_SCOPE_MULTIPLIER}"), "site-a", "provider")
                .is_none(),
            "the first scope past the configured bound must not merge provider state"
        );
    }

    #[test]
    fn provider_tools_survive_broadcast_round_trip() {
        let mut snap = GridStateSnapshot::new("site-a".to_owned());
        snap.upsert_provider(provider_state(
            "tool/my-mcp",
            Vec::new(),
            vec!["web-search".to_owned(), "code-exec".to_owned()],
            7,
        ));
        let broadcast = StateBroadcast::new("site-a".to_owned(), 7, snap, Some("10.0.0.1:8443".to_owned()))
            .with_signals_address(Some("10.0.0.1:9091".to_owned()));

        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());
        let provider = decoded
            .snapshot
            .provider("net", "site-a", "tool/my-mcp")
            .unwrap_or_else(|| std::process::abort());

        assert_eq!(provider.tools, vec!["web-search", "code-exec"]);
        assert_eq!(decoded.signals_address.as_deref(), Some("10.0.0.1:9091"));
    }

    #[test]
    fn pre_tools_peer_broadcast_decodes_with_empty_tools() {
        let v1 = StateBroadcastV1 {
            version: STATE_BROADCAST_VERSION_V1,
            origin_site: "site-old".to_owned(),
            revision: 5,
            snapshot: snapshot("site-old", 5, 0.4),
        };
        let mut bytes =
            bincode::serde::encode_to_vec(&v1, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        let extension = BroadcastExtension {
            gateway_address: Some("10.0.0.2:8443".to_owned()),
            site_cert_pem: None,
            signature: None,
            signed_at_ms: None,
            grid_id: Some("grid-1".to_owned()),
            provider_capacity_weights: BTreeMap::new(),
        };
        bincode::serde::encode_into_std_write(extension, &mut bytes, bincode::config::standard())
            .unwrap_or_else(|_| std::process::abort());
        bincode::serde::encode_into_std_write(Some("10.0.0.2:9091"), &mut bytes, bincode::config::standard())
            .unwrap_or_else(|_| std::process::abort());

        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());
        let provider = decoded
            .snapshot
            .provider("net", "site-old", "provider")
            .unwrap_or_else(|| std::process::abort());

        assert_eq!(decoded.gateway_address.as_deref(), Some("10.0.0.2:8443"));
        assert_eq!(decoded.grid_id.as_deref(), Some("grid-1"));
        assert_eq!(decoded.signals_address.as_deref(), Some("10.0.0.2:9091"));
        assert!(provider.tools.is_empty(), "pre-tools peer must have empty tools");
    }

    #[test]
    fn pre_tools_decoder_keeps_signals_address_when_tools_follow_it() {
        let mut snap = GridStateSnapshot::new("site-a".to_owned());
        snap.upsert_provider(provider_state("tool/my-mcp", Vec::new(), vec!["search".to_owned()], 3));
        let broadcast = StateBroadcast::new("site-a".to_owned(), 3, snap, None)
            .with_signals_address(Some("10.0.0.1:9091".to_owned()));
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        let (_, base_used): (StateBroadcastV1, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                .unwrap_or_else(|_| std::process::abort());
        let after_base = bytes.get(base_used..).unwrap_or(&[]);
        let (_, extension_used): (BroadcastExtension, usize) =
            bincode::serde::decode_from_slice(after_base, bincode::config::standard())
                .unwrap_or_else(|_| std::process::abort());
        let after_extension = after_base.get(extension_used..).unwrap_or(&[]);
        let (signals, signals_used): (Option<String>, usize) =
            bincode::serde::decode_from_slice(after_extension, bincode::config::standard())
                .unwrap_or_else(|_| std::process::abort());

        assert_eq!(signals.as_deref(), Some("10.0.0.1:9091"));
        assert!(
            after_extension.get(signals_used..).is_some_and(|tail| !tail.is_empty()),
            "the pre-tools decoder must leave the framed tool extension as ignorable trailing bytes"
        );
    }

    #[test]
    fn tool_free_signable_bytes_keep_the_pre_tools_layout() {
        let broadcast = StateBroadcast::new("site-a".to_owned(), 3, snapshot("site-a", 3, 0.4), None)
            .with_grid_id(Some("grid-1".to_owned()))
            .with_signed_at(Some(42))
            .with_signals_address(Some("10.0.0.1:9091".to_owned()));

        assert_eq!(
            broadcast.signable_bytes().unwrap_or_else(|_| std::process::abort()),
            pre_tools_signable_bytes(&broadcast),
            "tool-free signed broadcasts must stay verifiable across the rolling upgrade"
        );
    }

    #[test]
    fn signed_tool_extension_fails_closed_across_pre_tools_peers() {
        let mut snap = GridStateSnapshot::new("site-a".to_owned());
        snap.upsert_provider(provider_state("tool/my-mcp", Vec::new(), vec!["search".to_owned()], 3));
        let broadcast = StateBroadcast::new("site-a".to_owned(), 3, snap, None)
            .with_grid_id(Some("grid-1".to_owned()))
            .with_signed_at(Some(42));
        let legacy = pre_tools_signable_bytes(&broadcast);
        let current = broadcast.signable_bytes().unwrap_or_else(|_| std::process::abort());
        let (key, public_key) = generate_signing_key_and_pubkey();
        let old_signature = crate::signing::sign_ecdsa_p256(&key, &legacy).unwrap_or_else(|_| std::process::abort());
        let new_signature = crate::signing::sign_ecdsa_p256(&key, &current).unwrap_or_else(|_| std::process::abort());

        assert_ne!(legacy, current, "tools must participate in signed bytes");
        assert!(
            crate::signing::verify_ecdsa_p256(&public_key, &current, &old_signature).is_err(),
            "new peers must reject a legacy signature that does not authenticate tools"
        );
        assert!(
            crate::signing::verify_ecdsa_p256(&public_key, &legacy, &new_signature).is_err(),
            "pre-tools peers must reject a signature over an extension they cannot authenticate"
        );
    }

    #[test]
    fn signed_tool_extension_verifies_and_restores_tools_on_current_peers() {
        let mut snap = GridStateSnapshot::new("site-a".to_owned());
        snap.upsert_provider(provider_state("tool/my-mcp", Vec::new(), vec!["search".to_owned()], 3));
        let unsigned = StateBroadcast::new("site-a".to_owned(), 3, snap, None)
            .with_grid_id(Some("grid-1".to_owned()))
            .with_signed_at(Some(now_ms()));
        let (key, public_key) = generate_signing_key_and_pubkey();
        let signature = crate::signing::sign_ecdsa_p256(
            &key,
            &unsigned.signable_bytes().unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        handler
            .trust_store_sender()
            .send_modify(|store| drop(store.insert("site-a".to_owned(), vec![public_key])));

        let accepted = receive(&mut handler, &unsigned.with_signature(Some(signature)));
        let tools = handler
            .snapshot()
            .provider("net", "site-a", "tool/my-mcp")
            .map(|provider| provider.tools.clone());

        assert!(accepted.is_some(), "the authenticated tool broadcast must be accepted");
        assert_eq!(tools, Some(vec!["search".to_owned()]));
    }

    #[test]
    fn authoritative_empty_snapshot_withdraws_every_origin_provider() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let initial = StateBroadcast::new("site-a".to_owned(), 1, snapshot("site-a", 1, 0.4), None);
        let withdrawal = StateBroadcast::new(
            "site-a".to_owned(),
            2,
            GridStateSnapshot::new("site-a".to_owned()),
            None,
        )
        .with_authoritative_provider_state();

        drop(receive(&mut handler, &initial));
        let withdrawal_key = receive(&mut handler, &withdrawal);

        assert!(
            withdrawal.carries_grid_state(),
            "the empty withdrawal must use the state lane"
        );
        assert!(withdrawal_key.is_some(), "the empty withdrawal must be re-gossiped");
        assert!(
            handler.snapshot().provider("net", "site-a", "provider").is_none(),
            "an authoritative empty snapshot must remove the origin's retained provider"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "two scoped snapshots and one withdrawal form one cross-network regression proof"
    )]
    fn authoritative_empty_snapshot_withdraws_only_its_grid_scope() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let mut snapshot_a = GridStateSnapshot::new("site-a".to_owned());
        let mut provider_a = provider_state("provider-a", vec!["model-a".to_owned()], Vec::new(), 1);
        provider_a.network_id = "net-a".to_owned();
        snapshot_a.upsert_provider(provider_a);
        let mut snapshot_b = GridStateSnapshot::new("site-a".to_owned());
        let mut provider_b = provider_state("provider-b", vec!["model-b".to_owned()], Vec::new(), 1);
        provider_b.network_id = "net-b".to_owned();
        snapshot_b.upsert_provider(provider_b);
        let initial_a =
            StateBroadcast::new("site-a".to_owned(), 9, snapshot_a, None).with_grid_id(Some("grid-a".to_owned()));
        let initial_b =
            StateBroadcast::new("site-a".to_owned(), 1, snapshot_b, None).with_grid_id(Some("grid-b".to_owned()));
        let withdrawal_a = StateBroadcast::new(
            "site-a".to_owned(),
            10,
            GridStateSnapshot::new("site-a".to_owned()),
            None,
        )
        .with_grid_id(Some("grid-a".to_owned()))
        .with_authoritative_provider_state();

        drop(receive(&mut handler, &initial_a));
        drop(receive(&mut handler, &initial_b));
        drop(receive(&mut handler, &withdrawal_a));

        assert!(
            handler.snapshot().provider("net-a", "site-a", "provider-a").is_none(),
            "an empty GridNetwork snapshot must withdraw that scope's provider"
        );
        assert!(
            handler.snapshot().provider("net-b", "site-a", "provider-b").is_some(),
            "an empty GridNetwork snapshot must preserve the origin's other network"
        );
    }

    #[test]
    fn inference_and_tool_providers_coexist_in_broadcast() {
        let mut snap = GridStateSnapshot::new("site-a".to_owned());
        snap.add_capability(Capability::Model("llama".to_owned()));
        snap.upsert_provider(provider_state("my-server", vec!["llama".to_owned()], Vec::new(), 3));
        snap.upsert_provider(provider_state(
            "tool/my-server",
            Vec::new(),
            vec!["search".to_owned()],
            4,
        ));
        let broadcast = StateBroadcast::new("site-a".to_owned(), 4, snap, Some("10.0.0.1:8443".to_owned()));
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());
        let inference = decoded
            .snapshot
            .provider("net", "site-a", "my-server")
            .unwrap_or_else(|| std::process::abort());
        let tool = decoded
            .snapshot
            .provider("net", "site-a", "tool/my-server")
            .unwrap_or_else(|| std::process::abort());

        assert_eq!(inference.models, vec!["llama"]);
        assert!(inference.tools.is_empty());
        assert_eq!(tool.tools, vec!["search"]);
        assert!(tool.models.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    #[expect(
        clippy::too_many_lines,
        reason = "the utility reproduces every field of the immutable pre-tools wire layout"
    )]
    fn pre_tools_signable_bytes(broadcast: &StateBroadcast) -> Vec<u8> {
        let v1 = StateBroadcastV1 {
            version: broadcast.version,
            origin_site: broadcast.origin_site.clone(),
            revision: broadcast.revision,
            snapshot: broadcast.snapshot.clone(),
        };
        let mut encoded =
            bincode::serde::encode_to_vec(&v1, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        let provider_capacity_weights = broadcast
            .snapshot
            .providers
            .values()
            .filter(|provider| provider.capacity_weight != 1)
            .map(|provider| (provider_capacity_key(provider), provider.capacity_weight))
            .collect::<BTreeMap<_, _>>();
        let carries_extension = broadcast.gateway_address.is_some()
            || broadcast.site_cert_pem.is_some()
            || broadcast.signed_at_ms.is_some()
            || broadcast.grid_id.is_some()
            || !provider_capacity_weights.is_empty()
            || broadcast.signals_address.is_some();
        if carries_extension {
            let extension = BroadcastExtension {
                gateway_address: broadcast.gateway_address.clone(),
                site_cert_pem: broadcast.site_cert_pem.clone(),
                signature: None,
                signed_at_ms: broadcast.signed_at_ms,
                grid_id: broadcast.grid_id.clone(),
                provider_capacity_weights,
            };
            bincode::serde::encode_into_std_write(extension, &mut encoded, bincode::config::standard())
                .unwrap_or_else(|_| std::process::abort());
            if broadcast.signals_address.is_some() {
                bincode::serde::encode_into_std_write(
                    &broadcast.signals_address,
                    &mut encoded,
                    bincode::config::standard(),
                )
                .unwrap_or_else(|_| std::process::abort());
            }
        }
        let mut signable = SIGNATURE_DOMAIN.to_vec();
        signable.extend(encoded);
        signable
    }

    fn provider_state(id: &str, models: Vec<String>, tools: Vec<String>, revision: u64) -> ProviderState {
        ProviderState {
            network_id: "net".to_owned(),
            site_id: "site-a".to_owned(),
            provider_id: id.to_owned(),
            routing_cluster: id.to_owned(),
            models,
            tools,
            backend_kind: "local".to_owned(),
            capacity_weight: 1,
            phase: ProviderPhase::Available,
            metrics: ProviderMetricsSnapshot::default(),
            access_policy: crdt::ProviderAccessPolicy::default(),
            revision,
            writer_id: "site-a".to_owned(),
        }
    }
}
