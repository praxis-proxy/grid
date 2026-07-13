//! Mergeable grid state snapshots for cross-site propagation.
//!
//! A [`GridStateSnapshot`] is the unit of distributed state that can be
//! exchanged between sites.  It intentionally contains only lightweight control
//! plane facts: provider capabilities, provider lifecycle phase, and normalized
//! scoring metrics.
//!
//! Merge semantics are deterministic, commutative, associative, and idempotent:
//! - provider records are last-writer-wins by `(revision, writer_id)`;
//! - capabilities use the existing add-wins [`OrSet`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::OrSet;

// ---------------------------------------------------------------------------
// Capability
// ---------------------------------------------------------------------------

/// A capability advertised by a grid site.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum Capability {
    /// A model identifier accepted by an inference provider.
    Model(String),

    /// A tool identifier accepted by a tool provider.
    Tool(String),

    /// An agent identifier accepted by an agent provider.
    Agent(String),
}

// ---------------------------------------------------------------------------
// Provider state
// ---------------------------------------------------------------------------

/// Observed lifecycle phase for a provider advertised in a snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ProviderPhase {
    /// Provider is accepting traffic.
    Available,

    /// Provider exists but has not proven healthy yet.
    Pending,

    /// Provider is reachable but degraded.
    Degraded,

    /// Provider should not receive traffic.
    Unavailable,
}

/// Normalized provider metrics used by routing/scoring.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ProviderMetricsSnapshot {
    /// Normalized pending queue depth, 0.0 idle to 1.0 saturated.
    pub queue_depth: Option<f64>,

    /// Normalized KV-cache utilization, 0.0 empty to 1.0 saturated.
    pub kv_cache_utilization: Option<f64>,

    /// P99 latency in milliseconds.
    pub latency_p99_ms: Option<f64>,

    /// Prefix-cache hit ratio, 0.0 to 1.0.
    pub prefix_cache_hit_ratio: Option<f64>,

    /// Error rate, 0.0 to 1.0.
    pub error_rate: Option<f64>,

    /// Explicit health signal when available.
    pub healthy: Option<bool>,
}

/// One provider record advertised by a site.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ProviderState {
    /// Site that produced this record.
    pub site_id: String,

    /// Provider identity within the advertising site.
    pub provider_id: String,

    /// Routing cluster identity used in Praxis overlays.
    pub routing_cluster: String,

    /// Models served by this provider.
    pub models: Vec<String>,

    /// Backend locality kind (`local`, `remote`, `cloud_managed`, `api_provider`).
    pub backend_kind: String,

    /// Lifecycle phase observed by the advertising site.
    pub phase: ProviderPhase,

    /// Optional normalized metrics.
    pub metrics: ProviderMetricsSnapshot,

    /// Monotonic per-writer revision for this provider record.
    pub revision: u64,

    /// Stable writer identity used to break equal-revision ties.
    pub writer_id: String,
}

impl ProviderState {
    /// Return true when `self` should replace `other` during merge.
    #[must_use]
    fn supersedes(&self, other: &Self) -> bool {
        (self.revision, &self.writer_id) > (other.revision, &other.writer_id)
    }
}

// ---------------------------------------------------------------------------
// Grid state snapshot
// ---------------------------------------------------------------------------

/// Mergeable distributed state for one grid view.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GridStateSnapshot {
    /// Site that created this snapshot replica.
    pub site_id: String,

    /// Add-wins capability catalog.
    pub capabilities: OrSet<Capability>,

    /// Provider records keyed by a stable `site_id/provider_id` string.
    pub providers: BTreeMap<String, ProviderState>,
}

impl GridStateSnapshot {
    /// Create an empty snapshot replica for `site_id`.
    #[must_use]
    pub fn new(site_id: String) -> Self {
        Self {
            capabilities: OrSet::new(site_id.clone()),
            providers: BTreeMap::new(),
            site_id,
        }
    }

    /// Add a capability to this snapshot.
    pub fn add_capability(&mut self, capability: Capability) {
        self.capabilities.add(capability);
    }

    /// Upsert one provider record.
    pub fn upsert_provider(&mut self, provider: ProviderState) {
        let key = provider_key(&provider.site_id, &provider.provider_id);
        match self.providers.get(&key) {
            Some(existing) if !provider.supersedes(existing) => {},
            _ => {
                self.providers.insert(key, provider);
            },
        }
    }

    /// Merge `other` into this snapshot.
    pub fn merge(&mut self, other: &Self) {
        self.capabilities.merge(&other.capabilities);
        for provider in other.providers.values() {
            self.upsert_provider(provider.clone());
        }
    }

    /// Return a provider by site/provider identity.
    #[must_use]
    pub fn provider(&self, site_id: &str, provider_id: &str) -> Option<&ProviderState> {
        self.providers.get(&provider_key(site_id, provider_id))
    }
}

/// Build a stable provider map key.
fn provider_key(site_id: &str, provider_id: &str) -> String {
    format!("{site_id}/{provider_id}")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(site: &str, provider_id: &str, revision: u64, queue_depth: f64) -> ProviderState {
        ProviderState {
            site_id: site.to_owned(),
            provider_id: provider_id.to_owned(),
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
        }
    }

    #[test]
    fn upsert_keeps_newer_provider_revision() {
        let mut snap = GridStateSnapshot::new("site-p".to_owned());
        snap.upsert_provider(provider("site-p", "provider", 1, 0.9));
        snap.upsert_provider(provider("site-p", "provider", 2, 0.1));
        let got = snap
            .provider("site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(got.revision, 2, "newer revision must win");
        assert_eq!(got.metrics.queue_depth, Some(0.1), "newer metrics must win");
    }

    #[test]
    fn upsert_ignores_older_provider_revision() {
        let mut snap = GridStateSnapshot::new("site-p".to_owned());
        snap.upsert_provider(provider("site-p", "provider", 2, 0.1));
        snap.upsert_provider(provider("site-p", "provider", 1, 0.9));
        let got = snap
            .provider("site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(got.revision, 2, "older revision must not replace newer state");
        assert_eq!(got.metrics.queue_depth, Some(0.1), "newer metrics must remain");
    }

    #[test]
    fn equal_revision_tie_breaks_by_writer_id() {
        let mut left = provider("site-p", "provider", 1, 0.9);
        left.writer_id = "writer-a".to_owned();
        let mut right = provider("site-p", "provider", 1, 0.1);
        right.writer_id = "writer-b".to_owned();

        let mut snap = GridStateSnapshot::new("site-p".to_owned());
        snap.upsert_provider(left);
        snap.upsert_provider(right);

        let got = snap
            .provider("site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(got.writer_id, "writer-b", "lexicographically larger writer wins tie");
    }

    #[test]
    fn merge_is_idempotent_for_duplicate_snapshot() {
        let mut snap = GridStateSnapshot::new("site-p".to_owned());
        snap.add_capability(Capability::Model("model-x".to_owned()));
        snap.upsert_provider(provider("site-p", "provider", 1, 0.2));

        let duplicate = snap.clone();
        snap.merge(&duplicate);

        assert_eq!(snap.capabilities.len(), 1, "duplicate capabilities must collapse");
        assert_eq!(snap.providers.len(), 1, "duplicate providers must collapse");
    }

    #[test]
    fn merge_order_does_not_change_result() {
        let mut a = GridStateSnapshot::new("site-p".to_owned());
        a.add_capability(Capability::Model("model-p".to_owned()));
        a.upsert_provider(provider("site-p", "provider", 1, 0.8));

        let mut b = GridStateSnapshot::new("site-q".to_owned());
        b.add_capability(Capability::Model("model-q".to_owned()));
        b.upsert_provider(provider("site-p", "provider", 2, 0.2));

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b;
        ba.merge(&a);

        assert_eq!(
            ab.capabilities.len(),
            ba.capabilities.len(),
            "capability merge must converge"
        );
        let ab_provider = ab
            .provider("site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        let ba_provider = ba
            .provider("site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            ab_provider.revision, ba_provider.revision,
            "provider revision must converge"
        );
        assert_eq!(
            ab_provider.metrics.queue_depth, ba_provider.metrics.queue_depth,
            "provider metrics must converge"
        );
    }

    #[test]
    fn merge_is_associative_for_provider_records() {
        let mut a = GridStateSnapshot::new("site-p".to_owned());
        a.upsert_provider(provider("site-p", "provider", 1, 0.8));
        let mut b = GridStateSnapshot::new("site-q".to_owned());
        b.upsert_provider(provider("site-p", "provider", 2, 0.4));
        let mut c = GridStateSnapshot::new("site-r".to_owned());
        c.upsert_provider(provider("site-p", "provider", 3, 0.1));

        let mut ab_then_c = a.clone();
        ab_then_c.merge(&b);
        ab_then_c.merge(&c);

        let mut bc = b;
        bc.merge(&c);
        let mut a_then_bc = a;
        a_then_bc.merge(&bc);

        let left = ab_then_c
            .provider("site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        let right = a_then_bc
            .provider("site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            left.revision, right.revision,
            "associative merge must choose same revision"
        );
        assert_eq!(
            left.metrics.queue_depth, right.metrics.queue_depth,
            "associative merge must choose same metrics"
        );
    }

    #[test]
    fn snapshot_serde_round_trip() {
        let mut snap = GridStateSnapshot::new("site-p".to_owned());
        snap.add_capability(Capability::Model("model-x".to_owned()));
        snap.upsert_provider(provider("site-p", "provider", 1, 0.3));

        let bytes =
            bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        let (restored, _len): (GridStateSnapshot, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                .unwrap_or_else(|_| std::process::abort());

        assert_eq!(restored.capabilities.len(), 1, "capabilities must survive serde");
        assert!(
            restored.provider("site-p", "provider").is_some(),
            "provider must survive serde"
        );
    }
}
