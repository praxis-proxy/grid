//! Signed, bounded model inventory frames carried by SWIM broadcasts.

use std::collections::BTreeMap;

#[cfg(feature = "fips")]
use openssl::hash::{MessageDigest, hash};
use serde::{Deserialize, Serialize};
#[cfg(not(feature = "fips"))]
use sha2::{Digest as _, Sha256};

use crate::{
    signing,
    state_broadcast::{MAX_BROADCAST_AGE_MS, MAX_CLOCK_SKEW_AHEAD_MS},
};

/// Wire version for [`ModelInventoryFrame`].
pub const MODEL_INVENTORY_FRAME_VERSION: u16 = 1;

/// Maximum models in one provider inventory.
pub const MAX_INVENTORY_MODELS: usize = 256;

/// Maximum UTF-8 bytes in one model name.
pub const MAX_MODEL_NAME_BYTES: usize = 256;

/// Maximum frames in one inventory.
pub const MAX_INVENTORY_FRAMES: usize = 256;

/// Maximum decoded model-name bytes retained for one origin.
pub const MAX_ORIGIN_INVENTORY_NAME_BYTES: usize = 128 * 1024;

/// Maximum incomplete inventory frames retained for one origin.
pub const MAX_IN_PROGRESS_FRAMES_PER_ORIGIN: usize = 256;

/// Maximum provider inventories retained for one origin.
pub const MAX_ORIGIN_PROVIDER_INVENTORIES: usize = 256;

/// Maximum provider and model pairs retained for one origin.
pub const MAX_ORIGIN_MODEL_PAIRS: usize = 256;

/// Maximum UTF-8 byte length of a Kubernetes resource identity.
pub const MAX_INVENTORY_ID_BYTES: usize = 253;

/// Maximum UTF-8 byte length of a Kubernetes UID.
pub const MAX_PROVIDER_UID_BYTES: usize = 128;

/// Signature domain for model inventory frames.
const SIGNATURE_DOMAIN: &[u8] = b"praxis-grid/swim/model-inventory-frame/v1";

/// Binary prefix distinguishing inventory broadcasts from state broadcasts.
const MODEL_INVENTORY_BROADCAST_MAGIC: &[u8; 8] = b"AGNMIF01";

/// One signed part of a complete provider model inventory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelInventoryFrame {
    /// Wire version.
    pub version: u16,

    /// `GridNetwork` metadata name containing the provider.
    pub network_id: String,

    /// Inference provider metadata name.
    pub provider_id: String,

    /// Kubernetes UID of the provider instance.
    pub provider_uid: String,

    /// Monotonic provider resource revision.
    pub inventory_revision: u64,

    /// Inventory deadline as Unix milliseconds.
    pub expires_at_ms: u64,

    /// Zero-based frame position.
    pub frame_index: u16,

    /// Number of frames in the inventory.
    pub frame_count: u16,

    /// SHA-256 digest of the sorted complete model list.
    pub inventory_digest: [u8; 32],

    /// Model names carried by this frame.
    pub models: Vec<String>,

    /// Wall-clock time the frame was signed, in Unix milliseconds.
    pub signed_at_ms: Option<u64>,

    /// ECDSA P-256 signature over the frame and its origin/grid scope.
    pub signature: Option<Vec<u8>>,
}

impl ModelInventoryFrame {
    /// Attach the signing time before signing the frame.
    #[must_use]
    pub fn with_signed_at(mut self, signed_at_ms: u64) -> Self {
        self.signed_at_ms = Some(signed_at_ms);
        self
    }

    /// Attach an ECDSA signature computed over [`signable_bytes`](Self::signable_bytes).
    #[must_use]
    pub fn with_signature(mut self, signature: Vec<u8>) -> Self {
        self.signature = Some(signature);
        self
    }

    /// Sign this frame for the supplied origin site and grid.
    ///
    /// # Errors
    ///
    /// Returns an encoding error if the signable frame cannot be encoded, or
    /// a signing error if `pkcs8_der` is not a valid ECDSA P-256 private key.
    pub fn sign(
        mut self,
        origin_site: &str,
        grid_id: &str,
        pkcs8_der: &[u8],
        signed_at_ms: u64,
    ) -> Result<Self, FrameSigningError> {
        self.signed_at_ms = Some(signed_at_ms);
        let bytes = self.signable_bytes(origin_site, grid_id)?;
        self.signature = Some(signing::sign_ecdsa_p256(pkcs8_der, &bytes)?);
        Ok(self)
    }

    /// Return bytes authenticated by this frame's signature.
    ///
    /// # Errors
    ///
    /// Returns an encoding error if the unsigned frame cannot be serialized.
    pub fn signable_bytes(&self, origin_site: &str, grid_id: &str) -> Result<Vec<u8>, bincode::error::EncodeError> {
        let mut unsigned = self.clone();
        unsigned.signature = None;
        let mut bytes = SIGNATURE_DOMAIN.to_vec();
        bytes.extend(bincode::serde::encode_to_vec(
            (origin_site, grid_id, unsigned),
            bincode::config::standard(),
        )?);
        Ok(bytes)
    }

    /// Verify the frame's origin/grid-bound ECDSA signature against pinned keys.
    #[must_use]
    pub fn verifies(&self, origin_site: &str, grid_id: &str, keys: &[Vec<u8>]) -> bool {
        let (Some(signature), Some(signed_at_ms)) = (&self.signature, self.signed_at_ms) else {
            return false;
        };
        let now_ms = wall_ms();
        if now_ms.saturating_sub(signed_at_ms) > MAX_BROADCAST_AGE_MS
            || signed_at_ms.saturating_sub(now_ms) > MAX_CLOCK_SKEW_AHEAD_MS
        {
            return false;
        }
        self.signable_bytes(origin_site, grid_id).is_ok_and(|bytes| {
            keys.iter()
                .any(|key| signing::verify_ecdsa_p256(key, &bytes, signature).is_ok())
        })
    }
}

/// One origin-bound inventory frame as transported by SWIM.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelInventoryBroadcast {
    /// Wire version.
    pub version: u16,

    /// Origin site name, authenticated by the frame signature.
    pub origin_site: String,

    /// Grid ID, authenticated by the frame signature.
    pub grid_id: String,

    /// Inventory frame.
    pub frame: ModelInventoryFrame,
}

impl ModelInventoryBroadcast {
    /// Create an origin-bound inventory frame broadcast.
    #[must_use]
    pub fn new(origin_site: String, grid_id: String, frame: ModelInventoryFrame) -> Self {
        Self {
            version: MODEL_INVENTORY_FRAME_VERSION,
            origin_site,
            grid_id,
            frame,
        }
    }

    /// Encode this frame with its protocol discriminator.
    ///
    /// # Errors
    ///
    /// Returns a bincode encoding error if the broadcast cannot be encoded.
    pub fn encode(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        let mut bytes = MODEL_INVENTORY_BROADCAST_MAGIC.to_vec();
        bytes.extend(bincode::serde::encode_to_vec(self, bincode::config::standard())?);
        Ok(bytes)
    }

    /// Decode an inventory frame broadcast.
    ///
    /// # Errors
    ///
    /// Returns a bincode decode error for malformed bytes or an unsupported
    /// inventory broadcast version.
    pub fn decode(bytes: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        if !Self::recognizes(bytes) {
            return Err(bincode::error::DecodeError::Other(
                "invalid model inventory discriminator",
            ));
        }
        let payload = bytes.get(MODEL_INVENTORY_BROADCAST_MAGIC.len()..).unwrap_or(&[]);
        let (broadcast, consumed) =
            bincode::serde::decode_from_slice::<Self, _>(payload, bincode::config::standard().with_limit::<1_400>())?;
        if consumed != payload.len() || broadcast.version != MODEL_INVENTORY_FRAME_VERSION {
            return Err(bincode::error::DecodeError::Other("invalid model inventory broadcast"));
        }
        Ok(broadcast)
    }

    /// Return whether these bytes use the model inventory broadcast format.
    #[must_use]
    pub fn recognizes(bytes: &[u8]) -> bool {
        bytes.starts_with(MODEL_INVENTORY_BROADCAST_MAGIC)
    }

    /// Return this frame's SWIM invalidation key.
    #[must_use]
    pub fn key(&self) -> crate::StateBroadcastKey {
        crate::StateBroadcastKey::model_inventory(self)
    }
}

/// Errors produced while signing an inventory frame.
#[derive(Debug, thiserror::Error)]
pub enum FrameSigningError {
    /// The signable inventory frame could not be encoded.
    #[error("failed to encode signable model inventory frame: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    /// The frame could not be signed.
    #[error("failed to sign model inventory frame: {0}")]
    Signing(#[from] signing::SigningError),
}

/// One complete, verified model inventory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelInventory {
    /// Site that signed the inventory.
    pub origin_site: String,

    /// Owning `GridNetwork` ID.
    pub grid_id: String,

    /// `GridNetwork` metadata name.
    pub network_id: String,

    /// Provider metadata name.
    pub provider_id: String,

    /// Kubernetes provider UID.
    pub provider_uid: String,

    /// Monotonic provider resource revision.
    pub revision: u64,

    /// Inventory expiry in Unix milliseconds.
    pub expires_at_ms: u64,

    /// Complete sorted model list.
    pub models: Vec<String>,
}

/// Errors from validating or assembling signed inventory frames.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InventoryFrameError {
    /// Frame wire version is unsupported.
    #[error("unsupported model inventory frame version {actual}")]
    UnsupportedVersion {
        /// Received version.
        actual: u16,
    },

    /// Inventory contains more than the supported model limit.
    #[error("model inventory has {actual} models, maximum is {MAX_INVENTORY_MODELS}")]
    TooManyModels {
        /// Number of model names received.
        actual: usize,
    },

    /// Inventory contains a blank model name.
    #[error("model inventory contains a blank model name")]
    BlankModelName,

    /// A model name exceeds its byte limit.
    #[error("model inventory name has {actual} bytes, maximum is {MAX_MODEL_NAME_BYTES}")]
    ModelNameTooLong {
        /// Name length in bytes.
        actual: usize,
    },

    /// Inventory repeats a model name.
    #[error("model inventory contains a duplicate model name")]
    DuplicateModelName,

    /// Inventory has too many decoded name bytes.
    #[error("model inventory has {actual} name bytes, maximum is {MAX_ORIGIN_INVENTORY_NAME_BYTES}")]
    TooManyNameBytes {
        /// Decoded name bytes.
        actual: usize,
    },

    /// The digest provider failed to compute the model inventory digest.
    #[error("failed to compute model inventory digest: {0}")]
    DigestComputation(String),

    /// The digest provider returned a value with an unexpected length.
    #[error("model inventory digest provider returned an unexpected output length")]
    InvalidDigestLength,

    /// Frame index or count is invalid.
    #[error("model inventory frame index/count is invalid")]
    InvalidFramePosition,

    /// The inventory requires more frames than the supported bound.
    #[error("model inventory exceeds the maximum frame count")]
    TooManyFrames,

    /// Incomplete frame capacity is exhausted for the origin.
    #[error("model inventory frame capacity is exhausted for this origin")]
    InProgressCapacity,

    /// The provider inventory capacity is exhausted for the origin.
    #[error("model inventory capacity is exhausted for this origin")]
    InventoryCapacity,

    /// The frame contains an invalid or oversized Kubernetes identity.
    #[error("model inventory identity is invalid or exceeds its byte limit")]
    InvalidIdentity,

    /// A frame carries more than one model name.
    #[error("model inventory frame exceeds its one-name payload limit")]
    TooManyModelsInFrame,

    /// A duplicate frame number carried different content.
    #[error("duplicate model inventory frame conflicts with the held frame")]
    ConflictingDuplicate,

    /// Frame metadata differs within one inventory revision.
    #[error("model inventory frames disagree on revision metadata")]
    InconsistentFrames,

    /// Complete inventory digest did not match its frame metadata.
    #[error("model inventory digest does not match the complete model list")]
    DigestMismatch,

    /// A frame refers to an inventory that has already expired.
    #[error("model inventory frame is expired")]
    Expired,

    /// A newer complete inventory already supersedes this one.
    #[error("model inventory revision is stale")]
    StaleRevision,
}

impl ModelInventory {
    /// Split the complete inventory into one-name frames in sorted order.
    ///
    /// An empty inventory produces one authoritative empty frame.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identities, blank or duplicate names,
    /// inventories that exceed the name or model limits, or digest failures.
    pub fn frames(&self) -> Result<Vec<ModelInventoryFrame>, InventoryFrameError> {
        let mut sorted = self.models.clone();
        sorted.sort();
        let digest = digest_models(&sorted)?;
        let canonical = Self {
            models: sorted.clone(),
            ..self.clone()
        };
        validate_complete(&canonical, digest)?;
        let scope = InventoryScope::from_inventory(self);
        scope.validate()?;
        let frame_count = u16::try_from(sorted.len().max(1)).map_err(|_error| InventoryFrameError::TooManyFrames)?;
        let names = if sorted.is_empty() {
            vec![Vec::new()]
        } else {
            sorted.into_iter().map(|name| vec![name]).collect()
        };
        names
            .into_iter()
            .enumerate()
            .map(|(index, models)| {
                Ok(ModelInventoryFrame {
                    version: MODEL_INVENTORY_FRAME_VERSION,
                    network_id: self.network_id.clone(),
                    provider_id: self.provider_id.clone(),
                    provider_uid: self.provider_uid.clone(),
                    inventory_revision: self.revision,
                    expires_at_ms: self.expires_at_ms,
                    frame_index: u16::try_from(index).map_err(|_error| InventoryFrameError::TooManyFrames)?,
                    frame_count,
                    inventory_digest: digest,
                    models,
                    signed_at_ms: None,
                    signature: None,
                })
            })
            .collect()
    }
}

/// Identity shared by every frame of one provider inventory revision.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct InventoryKey {
    /// Provider instance and origin/grid scope.
    scope: InventoryScope,
    /// Monotonic inventory revision.
    revision: u64,
}

/// Provider instance, scoped to its authenticated origin and grid.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct InventoryScope {
    /// Site owning the provider.
    pub(crate) origin_site: String,
    /// Grid tenant ID.
    pub(crate) grid_id: String,
    /// `GridNetwork` metadata name.
    pub(crate) network_id: String,
    /// `InferenceProvider` metadata name.
    pub(crate) provider_id: String,
    /// Kubernetes provider instance UID.
    pub(crate) provider_uid: String,
}

impl InventoryScope {
    /// Extract provider identity from a complete inventory.
    fn from_inventory(inventory: &ModelInventory) -> Self {
        Self {
            origin_site: inventory.origin_site.clone(),
            grid_id: inventory.grid_id.clone(),
            network_id: inventory.network_id.clone(),
            provider_id: inventory.provider_id.clone(),
            provider_uid: inventory.provider_uid.clone(),
        }
    }

    /// Refuse invalid or oversized origin, grid, and provider identifiers.
    fn validate(&self) -> Result<(), InventoryFrameError> {
        if [&self.origin_site, &self.grid_id, &self.network_id, &self.provider_id]
            .into_iter()
            .all(|value| valid_identifier(value))
            && !self.provider_uid.trim().is_empty()
            && self.provider_uid.len() <= MAX_PROVIDER_UID_BYTES
        {
            Ok(())
        } else {
            Err(InventoryFrameError::InvalidIdentity)
        }
    }
}

/// A partial inventory kept until every frame and the whole-list digest verify.
#[derive(Clone, Debug)]
struct PendingInventory {
    /// Original signed expiry, identical on every frame.
    expires_at_ms: u64,
    /// Number of required frames.
    frame_count: u16,
    /// Expected digest of the complete model list.
    digest: [u8; 32],
    /// Unique frames indexed by their position.
    frames: BTreeMap<u16, Vec<String>>,
    /// Time of the first frame, bounding the assembly lifetime.
    started_at_ms: u64,
}

impl PendingInventory {
    /// Insert a frame without allowing conflicting duplicate content or metadata.
    fn insert(&mut self, frame: ModelInventoryFrame) -> Result<(), InventoryFrameError> {
        if self.expires_at_ms != frame.expires_at_ms
            || self.frame_count != frame.frame_count
            || self.digest != frame.inventory_digest
        {
            return Err(InventoryFrameError::InconsistentFrames);
        }
        if let Some(held) = self.frames.get(&frame.frame_index) {
            if held != &frame.models {
                return Err(InventoryFrameError::ConflictingDuplicate);
            }
        } else {
            self.frames.insert(frame.frame_index, frame.models);
        }
        Ok(())
    }
}

/// Completed or withdrawn revision retained through the signed replay window.
struct RevisionFloor {
    /// Last accepted complete revision.
    revision: u64,
    /// Deadline after which expired inventories can release their revision state.
    retain_until_ms: u64,
}

/// Outcome of retaining one authenticated inventory frame.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum InventoryAssemblyResult {
    /// A new frame was retained, but the inventory is incomplete.
    Incomplete,
    /// The same frame was already retained for an incomplete inventory.
    Duplicate,
    /// The frame completed a verified inventory.
    Complete(Box<ModelInventory>),
}

/// Bounded assembly state; callers authenticate origins before inserting frames.
#[derive(Default)]
pub(crate) struct ModelInventoryAssembler {
    /// Incomplete inventories keyed by provider and revision.
    pending: BTreeMap<InventoryKey, PendingInventory>,
    /// Last complete inventory for each provider instance.
    complete: BTreeMap<InventoryScope, ModelInventory>,
    /// Revision tombstones preventing replay after empty inventories and withdrawals.
    revision_floor: BTreeMap<InventoryScope, RevisionFloor>,
    /// Origins that exceeded a limit, blocked until all prior signed frames age out.
    blocked_origins: BTreeMap<String, u64>,
}

impl ModelInventoryAssembler {
    /// Accept an authenticated frame and replace the inventory only on completion.
    pub(crate) fn push(
        &mut self,
        broadcast: ModelInventoryBroadcast,
        now_ms: u64,
    ) -> Result<InventoryAssemblyResult, InventoryFrameError> {
        self.prune(now_ms);
        let ModelInventoryBroadcast {
            origin_site,
            grid_id,
            frame,
            ..
        } = broadcast;
        validate_frame(&frame, now_ms)?;
        if self.blocked_origins.contains_key(&origin_site) {
            return Err(InventoryFrameError::InventoryCapacity);
        }
        let scope = InventoryScope {
            origin_site,
            grid_id,
            network_id: frame.network_id.clone(),
            provider_id: frame.provider_id.clone(),
            provider_uid: frame.provider_uid.clone(),
        };
        scope.validate()?;
        if self
            .revision_floor
            .get(&scope)
            .is_some_and(|held| held.revision >= frame.inventory_revision)
        {
            return Err(InventoryFrameError::StaleRevision);
        }
        self.pending
            .retain(|held, _| held.scope != scope || held.revision >= frame.inventory_revision);
        let key = InventoryKey {
            scope: scope.clone(),
            revision: frame.inventory_revision,
        };
        let duplicate = self
            .pending
            .get(&key)
            .is_some_and(|pending| pending.frames.contains_key(&frame.frame_index));
        if !duplicate && self.pending_frames_for_origin(&scope.origin_site) >= MAX_IN_PROGRESS_FRAMES_PER_ORIGIN {
            self.block_origin(&scope.origin_site, now_ms);
            return Err(InventoryFrameError::InProgressCapacity);
        }
        let pending = self.pending.entry(key.clone()).or_insert_with(|| PendingInventory {
            expires_at_ms: frame.expires_at_ms,
            frame_count: frame.frame_count,
            digest: frame.inventory_digest,
            frames: BTreeMap::new(),
            started_at_ms: now_ms,
        });
        if let Err(error) = pending.insert(frame) {
            self.pending.remove(&key);
            return Err(error);
        }
        if pending.frames.len() != usize::from(pending.frame_count) {
            return Ok(if duplicate {
                InventoryAssemblyResult::Duplicate
            } else {
                InventoryAssemblyResult::Incomplete
            });
        }
        let inventory = ModelInventory {
            origin_site: scope.origin_site.clone(),
            grid_id: scope.grid_id.clone(),
            network_id: scope.network_id.clone(),
            provider_id: scope.provider_id.clone(),
            provider_uid: scope.provider_uid.clone(),
            revision: key.revision,
            expires_at_ms: pending.expires_at_ms,
            models: pending.frames.values().flatten().cloned().collect(),
        };
        let expected_digest = pending.digest;
        self.pending.remove(&key);
        validate_complete(&inventory, expected_digest)?;
        if !self.can_retain_inventory(&scope, &inventory) {
            self.block_origin(&scope.origin_site, now_ms);
            return Err(InventoryFrameError::InventoryCapacity);
        }
        self.pending
            .retain(|held, _| held.scope != scope || held.revision > key.revision);
        self.revision_floor.insert(
            scope.clone(),
            RevisionFloor {
                revision: inventory.revision,
                retain_until_ms: now_ms
                    .saturating_add(MAX_BROADCAST_AGE_MS)
                    .saturating_add(MAX_CLOCK_SKEW_AHEAD_MS),
            },
        );
        self.complete.insert(scope, inventory.clone());
        Ok(InventoryAssemblyResult::Complete(Box::new(inventory)))
    }

    /// Return fresh completed inventories; metadata matching happens before publication.
    pub(crate) fn inventories(&self, now_ms: u64) -> Vec<ModelInventory> {
        self.complete
            .values()
            .filter(|inventory| inventory.expires_at_ms > now_ms)
            .cloned()
            .collect()
    }

    /// Remove a withdrawn provider instance while retaining its completed revision floor.
    pub(crate) fn withdraw_provider(&mut self, scope: &InventoryScope) {
        self.pending.retain(|key, _| &key.scope != scope);
        self.complete.remove(scope);
    }

    /// Remove all buffered and completed state owned by one origin.
    pub(crate) fn remove_origin(&mut self, origin: &str) {
        self.pending.retain(|key, _| key.scope.origin_site != origin);
        self.complete.retain(|scope, _| scope.origin_site != origin);
        self.revision_floor.retain(|scope, _| scope.origin_site != origin);
        self.blocked_origins.remove(origin);
    }

    /// Drop an over-limit origin's entire inventory and refuse replays until they age out.
    fn block_origin(&mut self, origin: &str, now_ms: u64) {
        self.remove_origin(origin);
        self.blocked_origins.insert(
            origin.to_owned(),
            now_ms
                .saturating_add(MAX_BROADCAST_AGE_MS)
                .saturating_add(MAX_CLOCK_SKEW_AHEAD_MS),
        );
    }

    /// Return the number of held frame slots for one origin.
    fn pending_frames_for_origin(&self, origin: &str) -> usize {
        self.pending
            .iter()
            .filter(|(key, _)| key.scope.origin_site == origin)
            .map(|(_, pending)| pending.frames.len())
            .sum()
    }

    /// Enforce provider/model pair, identity, and decoded name-byte limits per origin.
    fn can_retain_inventory(&self, scope: &InventoryScope, incoming: &ModelInventory) -> bool {
        let others = self
            .complete
            .iter()
            .filter(|(held, _)| held.origin_site == scope.origin_site && *held != scope);
        let held_names: usize = others.clone().map(|(_, inventory)| inventory.models.len()).sum();
        let held_bytes: usize = others
            .flat_map(|(_, inventory)| &inventory.models)
            .map(String::len)
            .sum();
        let held_scopes = self
            .revision_floor
            .keys()
            .filter(|held| held.origin_site == scope.origin_site && *held != scope)
            .count();
        let pending_bytes: usize = self
            .pending
            .iter()
            .filter(|(key, _)| key.scope.origin_site == scope.origin_site)
            .flat_map(|(_, pending)| pending.frames.values().flatten())
            .map(String::len)
            .sum();
        held_scopes < MAX_ORIGIN_PROVIDER_INVENTORIES
            && held_names.saturating_add(incoming.models.len()) <= MAX_ORIGIN_MODEL_PAIRS
            && held_bytes
                .saturating_add(incoming.models.iter().map(String::len).sum::<usize>())
                .saturating_add(pending_bytes)
                <= MAX_ORIGIN_INVENTORY_NAME_BYTES
    }

    /// Release expired assemblies and replay floors after the signature freshness window.
    fn prune(&mut self, now_ms: u64) {
        self.pending.retain(|_, pending| {
            pending.expires_at_ms > now_ms && now_ms.saturating_sub(pending.started_at_ms) <= MAX_BROADCAST_AGE_MS
        });
        self.complete.retain(|_, inventory| inventory.expires_at_ms > now_ms);
        self.revision_floor
            .retain(|scope, held| held.retain_until_ms >= now_ms || self.complete.contains_key(scope));
        self.blocked_origins.retain(|_, until_ms| *until_ms >= now_ms);
    }
}

/// Validate the independent bounds and frame position before retaining a frame.
fn validate_frame(frame: &ModelInventoryFrame, now_ms: u64) -> Result<(), InventoryFrameError> {
    if frame.version != MODEL_INVENTORY_FRAME_VERSION {
        return Err(InventoryFrameError::UnsupportedVersion { actual: frame.version });
    }
    if frame.frame_count == 0
        || usize::from(frame.frame_count) > MAX_INVENTORY_FRAMES
        || frame.frame_index >= frame.frame_count
        || (frame.models.is_empty() && frame.frame_count != 1)
    {
        return Err(InventoryFrameError::InvalidFramePosition);
    }
    if frame.models.len() > 1 {
        return Err(InventoryFrameError::TooManyModelsInFrame);
    }
    if frame.expires_at_ms <= now_ms {
        return Err(InventoryFrameError::Expired);
    }
    for model in &frame.models {
        if model.trim().is_empty() {
            return Err(InventoryFrameError::BlankModelName);
        }
        if model.len() > MAX_MODEL_NAME_BYTES {
            return Err(InventoryFrameError::ModelNameTooLong { actual: model.len() });
        }
    }
    Ok(())
}

/// Validate a DNS-subdomain-style resource identity length and content.
fn valid_identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_INVENTORY_ID_BYTES && !value.trim().is_empty()
}

/// Validate the complete, ordered name set against its digest and site limits.
fn validate_complete(inventory: &ModelInventory, expected_digest: [u8; 32]) -> Result<(), InventoryFrameError> {
    if inventory.models.len() > MAX_INVENTORY_MODELS {
        return Err(InventoryFrameError::TooManyModels {
            actual: inventory.models.len(),
        });
    }
    let mut name_bytes = 0_usize;
    for pair in inventory.models.windows(2) {
        if pair.first() >= pair.get(1) {
            return Err(InventoryFrameError::DuplicateModelName);
        }
    }
    for model in &inventory.models {
        if model.trim().is_empty() {
            return Err(InventoryFrameError::BlankModelName);
        }
        if model.len() > MAX_MODEL_NAME_BYTES {
            return Err(InventoryFrameError::ModelNameTooLong { actual: model.len() });
        }
        name_bytes = name_bytes.saturating_add(model.len());
    }
    if name_bytes > MAX_ORIGIN_INVENTORY_NAME_BYTES {
        return Err(InventoryFrameError::TooManyNameBytes { actual: name_bytes });
    }
    if digest_models(&inventory.models)? != expected_digest {
        return Err(InventoryFrameError::DigestMismatch);
    }
    Ok(())
}

/// Hash sorted model names with explicit length prefixes.
#[cfg_attr(
    not(feature = "fips"),
    expect(
        clippy::unnecessary_wraps,
        reason = "the FIPS OpenSSL digest provider is fallible, so keep one result API across builds"
    )
)]
fn digest_models(models: &[String]) -> Result<[u8; 32], InventoryFrameError> {
    let mut bytes = Vec::new();
    for model in models {
        let length = u32::try_from(model.len()).unwrap_or(u32::MAX).to_be_bytes();
        bytes.extend_from_slice(&length);
        bytes.extend_from_slice(model.as_bytes());
    }
    #[cfg(feature = "fips")]
    {
        let digest = hash(MessageDigest::sha256(), &bytes)
            .map_err(|error| InventoryFrameError::DigestComputation(error.to_string()))?;
        digest
            .as_ref()
            .try_into()
            .map_err(|_error| InventoryFrameError::InvalidDigestLength)
    }
    #[cfg(not(feature = "fips"))]
    {
        Ok(Sha256::digest(bytes).into())
    }
}

/// Wall-clock milliseconds since the Unix epoch, or zero before it.
fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use crate::model_inventory::{
        InventoryAssemblyResult, InventoryFrameError, MAX_IN_PROGRESS_FRAMES_PER_ORIGIN, MAX_INVENTORY_FRAMES,
        MAX_INVENTORY_MODELS, MAX_MODEL_NAME_BYTES, MAX_ORIGIN_INVENTORY_NAME_BYTES, MAX_ORIGIN_MODEL_PAIRS,
        ModelInventory, ModelInventoryAssembler, ModelInventoryBroadcast, digest_models,
    };

    #[test]
    fn frames_carry_one_sorted_name_and_share_a_digest() -> Result<(), InventoryFrameError> {
        let inventory = inventory(1, &["z", "a"]);
        let frames = inventory.frames()?;
        let names: Vec<_> = frames
            .iter()
            .flat_map(|frame| frame.models.iter().map(String::as_str))
            .collect();
        assert_eq!(names, ["a", "z"]);
        for (index, frame) in frames.iter().enumerate() {
            assert_eq!(usize::from(frame.frame_index), index);
            assert_eq!(frame.frame_count, 2);
            assert_eq!(frame.models.len(), 1);
            assert_eq!(
                frame.inventory_digest,
                digest_models(&["a".to_owned(), "z".to_owned()])?
            );
        }
        Ok(())
    }

    #[test]
    fn reverse_order_and_duplicates_publish_only_when_complete() -> Result<(), InventoryFrameError> {
        let expected = inventory(1, &["a", "b", "c"]);
        let mut frames = broadcasts(&expected)?;
        let mut assembler = ModelInventoryAssembler::default();
        let first = frames.pop().unwrap_or_else(|| std::process::abort());
        assert_eq!(assembler.push(first.clone(), 1)?, InventoryAssemblyResult::Incomplete);
        assert_eq!(assembler.push(first, 2)?, InventoryAssemblyResult::Duplicate);
        assert_eq!(assembler.inventories(2), []);
        assert_eq!(
            assembler.push(frames.pop().unwrap_or_else(|| std::process::abort()), 3)?,
            InventoryAssemblyResult::Incomplete
        );
        let result = assembler.push(frames.pop().unwrap_or_else(|| std::process::abort()), 4)?;
        assert_eq!(result, InventoryAssemblyResult::Complete(Box::new(expected.clone())));
        assert_eq!(assembler.inventories(4), [expected]);
        Ok(())
    }

    #[test]
    fn conflicting_duplicate_and_bad_digest_keep_the_previous_inventory() -> Result<(), InventoryFrameError> {
        let mut assembler = ModelInventoryAssembler::default();
        let held = inventory(1, &["old"]);
        push_inventory(&mut assembler, &held)?;
        let mut next = broadcasts(&inventory(2, &["a", "b"]))?;
        let first = next.pop().unwrap_or_else(|| std::process::abort());
        assembler.push(first.clone(), 2)?;
        let mut conflicting = first;
        conflicting.frame.models = vec!["wrong".to_owned()];
        assert_eq!(
            assembler.push(conflicting, 2),
            Err(InventoryFrameError::ConflictingDuplicate)
        );
        let mut corrupt = broadcasts(&inventory(3, &["a"]))?
            .pop()
            .unwrap_or_else(|| std::process::abort());
        corrupt.frame.inventory_digest = [0; 32];
        assert_eq!(assembler.push(corrupt, 3), Err(InventoryFrameError::DigestMismatch));
        assert_eq!(assembler.inventories(3), [held]);
        Ok(())
    }

    #[test]
    fn authoritative_empty_inventory_prevents_old_frames_from_restoring_names() -> Result<(), InventoryFrameError> {
        let mut assembler = ModelInventoryAssembler::default();
        let previous = inventory(1, &["old"]);
        push_inventory(&mut assembler, &previous)?;
        let empty = inventory(2, &[]);
        let frames = broadcasts(&empty)?;
        assert_eq!(frames.len(), 1);
        assert!(frames.first().is_some_and(|frame| frame.frame.models.is_empty()));
        push_inventory(&mut assembler, &empty)?;
        for frame in broadcasts(&previous)? {
            assert_eq!(assembler.push(frame, 3), Err(InventoryFrameError::StaleRevision));
        }
        assert_eq!(assembler.inventories(3), [empty]);
        Ok(())
    }

    #[test]
    fn incomplete_update_does_not_extend_the_previous_expiry() -> Result<(), InventoryFrameError> {
        let mut assembler = ModelInventoryAssembler::default();
        let mut previous = inventory(1, &["old"]);
        previous.expires_at_ms = 10;
        push_inventory(&mut assembler, &previous)?;
        let frame = broadcasts(&inventory(2, &["a", "b"]))?
            .pop()
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(assembler.push(frame, 9)?, InventoryAssemblyResult::Incomplete);
        assert_eq!(assembler.inventories(9), [previous]);
        assert!(assembler.inventories(10).is_empty());
        Ok(())
    }

    #[test]
    fn pending_frame_limit_applies_when_adding_to_an_existing_inventory() -> Result<(), InventoryFrameError> {
        let mut assembler = ModelInventoryAssembler::default();
        push_inventory(&mut assembler, &inventory(1, &["old"]))?;
        for revision in 2..=u64::try_from(MAX_IN_PROGRESS_FRAMES_PER_ORIGIN).unwrap_or(u64::MAX) + 1 {
            let mut pending_inventory = inventory(revision, &["a", "b"]);
            pending_inventory.provider_id = format!("provider-{revision}");
            let frame = broadcasts(&pending_inventory)?
                .pop()
                .unwrap_or_else(|| std::process::abort());
            assert_eq!(assembler.push(frame, 2)?, InventoryAssemblyResult::Incomplete);
        }
        let frame = broadcasts(&inventory(2, &["a", "b"]))?
            .into_iter()
            .next()
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(assembler.push(frame, 3), Err(InventoryFrameError::InProgressCapacity));
        assert!(
            assembler.inventories(3).is_empty(),
            "a site limit violation withdraws all dynamic names"
        );
        Ok(())
    }

    #[test]
    fn newer_revision_replaces_older_pending_frames_before_capacity_checks() -> Result<(), InventoryFrameError> {
        let old_models: Vec<String> = (0..MAX_INVENTORY_FRAMES)
            .map(|index| format!("old-{index:03}-{}", "x".repeat(240)))
            .collect();
        let old_names: Vec<&str> = old_models.iter().map(String::as_str).collect();
        let mut assembler = ModelInventoryAssembler::default();
        let old_frames = broadcasts(&inventory(1, &old_names))?;
        for frame in old_frames.into_iter().take(MAX_IN_PROGRESS_FRAMES_PER_ORIGIN - 1) {
            assert_eq!(assembler.push(frame, 2)?, InventoryAssemblyResult::Incomplete);
        }
        assert_eq!(
            assembler.pending_frames_for_origin("site-a"),
            MAX_IN_PROGRESS_FRAMES_PER_ORIGIN - 1
        );
        let old_pending_bytes: usize = assembler
            .pending
            .values()
            .flat_map(|pending| pending.frames.values().flatten())
            .map(String::len)
            .sum();
        assert!(old_pending_bytes > MAX_ORIGIN_INVENTORY_NAME_BYTES / 4);

        let mut new_frames = broadcasts(&inventory(2, &["new-a", "new-b"]))?.into_iter();
        assert_eq!(
            assembler.push(new_frames.next().unwrap_or_else(|| std::process::abort()), 3)?,
            InventoryAssemblyResult::Incomplete
        );
        assert_eq!(assembler.pending_frames_for_origin("site-a"), 1);
        let new_pending_bytes: usize = assembler
            .pending
            .values()
            .flat_map(|pending| pending.frames.values().flatten())
            .map(String::len)
            .sum();
        assert_eq!(new_pending_bytes, "new-a".len());
        assert_eq!(
            assembler.push(new_frames.next().unwrap_or_else(|| std::process::abort()), 4)?,
            InventoryAssemblyResult::Complete(Box::new(inventory(2, &["new-a", "new-b"])))
        );
        assert_eq!(assembler.inventories(4), [inventory(2, &["new-a", "new-b"])]);
        Ok(())
    }

    #[test]
    fn provider_model_pair_limit_withdraws_the_entire_origin() -> Result<(), InventoryFrameError> {
        let mut assembler = ModelInventoryAssembler::default();
        let mut full = inventory(1, &[]);
        full.models = (0..MAX_ORIGIN_MODEL_PAIRS)
            .map(|index| format!("model-{index:03}"))
            .collect();
        push_inventory(&mut assembler, &full)?;
        let mut extra = inventory(1, &["extra"]);
        extra.provider_id = "second-provider".to_owned();
        let frame = broadcasts(&extra)?.pop().unwrap_or_else(|| std::process::abort());
        assert_eq!(assembler.push(frame, 2), Err(InventoryFrameError::InventoryCapacity));
        assert!(assembler.inventories(2).is_empty());
        Ok(())
    }

    #[test]
    fn oversized_blank_and_duplicate_names_are_rejected() {
        assert_eq!(
            inventory(1, &["a", "a"]).frames(),
            Err(InventoryFrameError::DuplicateModelName)
        );
        assert_eq!(inventory(1, &[" "]).frames(), Err(InventoryFrameError::BlankModelName));
        let mut oversized = inventory(1, &[]);
        oversized.models = vec!["x".repeat(MAX_MODEL_NAME_BYTES + 1)];
        assert_eq!(
            oversized.frames(),
            Err(InventoryFrameError::ModelNameTooLong {
                actual: MAX_MODEL_NAME_BYTES + 1
            })
        );
        oversized.models = (0..=MAX_INVENTORY_MODELS)
            .map(|index| format!("model-{index}"))
            .collect();
        assert_eq!(
            oversized.frames(),
            Err(InventoryFrameError::TooManyModels {
                actual: MAX_INVENTORY_MODELS + 1
            })
        );
    }

    #[test]
    fn decode_rejects_a_wrong_discriminator_or_trailing_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let broadcast = broadcasts(&inventory(1, &["a"]))?
            .pop()
            .unwrap_or_else(|| std::process::abort());
        let mut bytes = broadcast.encode()?;
        assert_eq!(ModelInventoryBroadcast::decode(&bytes)?, broadcast);
        bytes.push(0);
        assert!(matches!(
            ModelInventoryBroadcast::decode(&bytes),
            Err(bincode::error::DecodeError::Other(_))
        ));
        let mut wrong_magic = broadcast.encode()?;
        if let Some(first) = wrong_magic.first_mut() {
            *first = 0;
        }
        assert!(matches!(
            ModelInventoryBroadcast::decode(&wrong_magic),
            Err(bincode::error::DecodeError::Other(_))
        ));
        Ok(())
    }

    // Test Utilities

    /// Build a provider inventory with a deterministic deadline and scope.
    fn inventory(revision: u64, names: &[&str]) -> ModelInventory {
        ModelInventory {
            origin_site: "site-a".to_owned(),
            grid_id: "grid-a".to_owned(),
            network_id: "net".to_owned(),
            provider_id: "provider".to_owned(),
            provider_uid: "uid-a".to_owned(),
            revision,
            expires_at_ms: 1_000_000,
            models: names.iter().map(|name| (*name).to_owned()).collect(),
        }
    }

    /// Wrap the inventory frames with their origin and grid scope.
    fn broadcasts(inventory: &ModelInventory) -> Result<Vec<ModelInventoryBroadcast>, InventoryFrameError> {
        Ok(inventory
            .frames()?
            .into_iter()
            .map(|frame| ModelInventoryBroadcast::new(inventory.origin_site.clone(), inventory.grid_id.clone(), frame))
            .collect())
    }

    /// Complete one inventory using the authenticated assembly boundary.
    fn push_inventory(
        assembler: &mut ModelInventoryAssembler,
        inventory: &ModelInventory,
    ) -> Result<(), InventoryFrameError> {
        for frame in broadcasts(inventory)? {
            assembler.push(frame, 1)?;
        }
        Ok(())
    }
}
