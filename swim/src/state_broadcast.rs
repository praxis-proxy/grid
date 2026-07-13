//! State snapshot payloads suitable for SWIM custom broadcasts.
//!
//! This module defines the envelope that will be carried by foca custom
//! broadcasts.  It does not wire the envelope into the live runtime yet; it
//! keeps encoding, decoding, and invalidation semantics isolated and tested.

use std::collections::BTreeMap;

use crdt::GridStateSnapshot;
use serde::{Deserialize, Serialize};

use crate::NodeId;

// ---------------------------------------------------------------------------
// State broadcast envelope
// ---------------------------------------------------------------------------

/// Wire-format version for [`StateBroadcast`].
pub const STATE_BROADCAST_VERSION: u16 = 1;

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
}

impl StateBroadcast {
    /// Create a versioned state broadcast.
    #[must_use]
    pub fn new(origin_site: String, revision: u64, snapshot: GridStateSnapshot) -> Self {
        Self {
            version: STATE_BROADCAST_VERSION,
            origin_site,
            revision,
            snapshot,
        }
    }

    /// Return this broadcast's invalidation key.
    #[must_use]
    pub fn key(&self) -> StateBroadcastKey {
        StateBroadcastKey {
            origin_site: self.origin_site.clone(),
            revision: self.revision,
        }
    }

    /// Encode this broadcast as bincode bytes.
    ///
    /// # Errors
    ///
    /// Returns a bincode encode error if the snapshot cannot be serialized.
    pub fn encode(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
    }

    /// Decode this broadcast from bincode bytes.
    ///
    /// # Errors
    ///
    /// Returns a bincode decode error if `bytes` is not a valid
    /// [`StateBroadcast`] payload.
    pub fn decode(bytes: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        let (value, _len) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
        Ok(value)
    }
}

/// Key used to replace stale queued broadcasts in foca.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateBroadcastKey {
    /// Site that originated the broadcast.
    pub origin_site: String,

    /// Monotonic origin-local revision.
    pub revision: u64,
}

impl foca::Invalidates for StateBroadcastKey {
    fn invalidates(&self, other: &Self) -> bool {
        self.origin_site == other.origin_site && self.revision >= other.revision
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
#[derive(Debug)]
pub struct StateBroadcastHandler {
    /// Locally merged distributed state.
    snapshot: GridStateSnapshot,

    /// Highest revision received from each origin.
    latest_by_origin: BTreeMap<String, u64>,
}

impl StateBroadcastHandler {
    /// Create a handler with an empty local state snapshot.
    #[must_use]
    pub fn new(site_id: String) -> Self {
        Self {
            snapshot: GridStateSnapshot::new(site_id),
            latest_by_origin: BTreeMap::new(),
        }
    }

    /// Return the currently merged grid-state snapshot.
    #[must_use]
    pub fn snapshot(&self) -> &GridStateSnapshot {
        &self.snapshot
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

        if self
            .latest_by_origin
            .get(&broadcast.origin_site)
            .is_some_and(|latest| *latest >= broadcast.revision)
        {
            return Ok(None);
        }

        self.snapshot.merge(&broadcast.snapshot);
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

    #[test]
    fn new_sets_version_origin_and_revision() {
        let broadcast = StateBroadcast::new("site-p".to_owned(), 7, snapshot("site-p", 7, 0.1));
        assert_eq!(broadcast.version, STATE_BROADCAST_VERSION, "version");
        assert_eq!(broadcast.origin_site, "site-p", "origin");
        assert_eq!(broadcast.revision, 7, "revision");
    }

    #[test]
    fn encode_decode_round_trip_preserves_snapshot() {
        let broadcast = StateBroadcast::new("site-p".to_owned(), 7, snapshot("site-p", 7, 0.1));
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());
        let provider = decoded
            .snapshot
            .provider("site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(decoded.version, STATE_BROADCAST_VERSION, "version");
        assert_eq!(provider.metrics.queue_depth, Some(0.1), "metric value");
    }

    #[test]
    fn newer_key_invalidates_older_from_same_origin() {
        let old = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 1,
        };
        let new = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 2,
        };
        assert!(new.invalidates(&old), "newer same-origin broadcast must invalidate old");
        assert!(!old.invalidates(&new), "older broadcast must not invalidate newer");
    }

    #[test]
    fn same_key_invalidates_duplicate() {
        let left = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 1,
        };
        let right = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 1,
        };
        assert!(left.invalidates(&right), "same key must invalidate duplicate");
    }

    #[test]
    fn different_origins_do_not_invalidate_each_other() {
        let left = StateBroadcastKey {
            origin_site: "site-p".to_owned(),
            revision: 9,
        };
        let right = StateBroadcastKey {
            origin_site: "site-q".to_owned(),
            revision: 1,
        };
        assert!(
            !left.invalidates(&right),
            "different origins must not invalidate each other"
        );
    }

    #[test]
    fn decoded_broadcast_merges_with_local_snapshot() {
        let broadcast = StateBroadcast::new("site-p".to_owned(), 2, snapshot("site-p", 2, 0.1));
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());
        let decoded = StateBroadcast::decode(&bytes).unwrap_or_else(|_| std::process::abort());

        let mut local = snapshot("site-p", 1, 0.9);
        local.merge(&decoded.snapshot);

        let provider = local
            .provider("site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(provider.revision, 2, "newer broadcast snapshot must win");
        assert_eq!(provider.metrics.queue_depth, Some(0.1), "newer metric must win");
    }

    #[test]
    fn handler_accepts_new_broadcast_and_merges_snapshot() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.2));
        let bytes = broadcast.encode().unwrap_or_else(|_| std::process::abort());

        let key = handler
            .receive_item(&bytes, None)
            .unwrap_or_else(|_| std::process::abort());

        assert!(key.is_some(), "new broadcast must be disseminated");
        let provider = handler
            .snapshot()
            .provider("site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(provider.metrics.queue_depth, Some(0.2), "snapshot must merge");
    }

    #[test]
    fn handler_rejects_duplicate_broadcast() {
        let mut handler = StateBroadcastHandler::new("site-local".to_owned());
        let broadcast = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.2));
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
        let newer = StateBroadcast::new("site-p".to_owned(), 2, snapshot("site-p", 2, 0.1));
        let older = StateBroadcast::new("site-p".to_owned(), 1, snapshot("site-p", 1, 0.9));
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
        let provider = handler
            .snapshot()
            .provider("site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(provider.metrics.queue_depth, Some(0.1), "newer state must remain");
    }
}
