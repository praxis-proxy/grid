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
//!
//! # Provider Access Policy
//!
//! Provider access policy enforcement allows providers to restrict which consumer
//! sites may use them. Existing unrestricted-provider behavior is preserved: an
//! empty accessPolicy still means allow all. This pre-release CRDT wire-shape
//! change assumes all Grid operators in a Kind/test mesh run the same build.
//! Mixed-version SWIM/CRDT compatibility is not currently defined or tested.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{GCounter, OrSet};

/// Hard bound on the number of distinct `tenant_id`s tracked in
/// [`GridStateSnapshot::tenant_spend`].
///
/// Bounds memory growth from gossip: a malicious or buggy origin could
/// otherwise flood arbitrary `tenant_id` keys indefinitely. Mirrors the
/// `max_origins` bound already used for retained-origin maps in `swim`.
pub const MAX_TRACKED_TENANTS: usize = 1_024;

/// Hard bound on the number of distinct site-slots accumulated within a
/// single tenant's [`GridStateSnapshot::tenant_spend`] counter.
///
/// A different axis from [`MAX_TRACKED_TENANTS`], which bounds the number of
/// distinct `tenant_id` keys: this bounds the number of distinct *origins*
/// (sites) contributing to any one tenant's counter. Without this bound, a
/// compromised or churning origin repeatedly claiming new site identities
/// could grow a single tenant's [`GCounter`] slot map without limit, since
/// nothing currently removes a site's slot once recorded (dead-member
/// eviction deliberately does not clear `tenant_spend` — see
/// [`GridStateSnapshot::remove_origin_tenant_spend`]'s doc comment).
///
/// 256 is generous headroom for any real Grid deployment's site count (Grid's
/// SWIM layer is documented to scale to 50,000+ nodes, but that bounds
/// membership gossip fan-out, not the number of *distinct sites a customer
/// actually deploys* — realistically tens, not thousands) while still
/// bounding pathological/adversarial growth.
pub const MAX_TENANT_SPEND_ORIGINS: usize = 256;

// ---------------------------------------------------------------------------
// Access policy
// ---------------------------------------------------------------------------

/// CRDT-compatible representation of provider access policy.
///
/// Mirrors the operator's `AccessPolicy` but uses basic types suitable
/// for CRDT propagation. Empty `match_labels` means allow all consumers.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderAccessPolicy {
    /// Labels that consumer sites must match to use this provider.
    ///
    /// Empty map means allow all consumers (backward compatible default).
    /// Non-empty requires exact label matching.
    pub match_labels: BTreeMap<String, String>,
}

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
    /// Grid network this provider belongs to.
    pub network_id: String,

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

    /// Access policy for consumer authorization.
    ///
    /// When empty `match_labels`, the provider allows all consumers (preserving
    /// existing behavior). When non-empty labels, consumer sites must match
    /// exactly to access this provider.
    #[serde(default)]
    pub access_policy: ProviderAccessPolicy,

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
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GridStateSnapshot {
    /// Site that created this snapshot replica.
    pub site_id: String,

    /// Add-wins capability catalog.
    pub capabilities: OrSet<Capability>,

    /// Provider records keyed by a stable `network_id/site_id/provider_id` string.
    pub providers: BTreeMap<String, ProviderState>,

    /// Per-tenant cumulative spend, keyed by tenant identifier.
    ///
    /// Each [`GCounter`] total is denominated in **cents** (`u64`) to keep
    /// the CRDT free of float-merge precision concerns; consumers convert to
    /// USD at the edge (see `operator::crd::grid_network::spend_ratio`).
    /// This is a cross-site *visibility* signal only — Grid does not enforce
    /// budget limits; that is a gateway-side policy-filter concern.
    ///
    /// `#[serde(default)]` so snapshots serialized before this field existed
    /// (or peers running an older build) deserialize to an empty map instead
    /// of failing.
    #[serde(default)]
    pub tenant_spend: BTreeMap<String, GCounter>,
}

impl GridStateSnapshot {
    /// Create an empty snapshot replica for `site_id`.
    #[must_use]
    pub fn new(site_id: String) -> Self {
        Self {
            capabilities: OrSet::new(site_id.clone()),
            providers: BTreeMap::new(),
            tenant_spend: BTreeMap::new(),
            site_id,
        }
    }

    /// Add a capability to this snapshot.
    pub fn add_capability(&mut self, capability: Capability) {
        self.capabilities.add(capability);
    }

    /// Upsert one provider record.
    pub fn upsert_provider(&mut self, provider: ProviderState) {
        let key = provider_key(&provider.network_id, &provider.site_id, &provider.provider_id);
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
        self.merge_tenant_spend(&other.tenant_spend);
    }

    /// Merge tenant spend counters from another snapshot's `tenant_spend` map.
    ///
    /// Mirrors the add-wins semantics of `capabilities.merge`: each tenant's
    /// [`GCounter`] merge takes the max of every site's slot, so this is safe
    /// to call repeatedly, out of order, or with disjoint tenant sets.
    pub fn merge_tenant_spend(&mut self, other: &BTreeMap<String, GCounter>) {
        for (tenant_id, counter) in other {
            self.tenant_spend
                .entry(tenant_id.clone())
                .or_insert_with(|| GCounter::new(self.site_id.clone()))
                .merge(counter);
        }
    }

    /// Merge tenant spend from a single wire broadcast, trusting only the
    /// slot attributable to its claimed `origin_site`.
    ///
    /// This is the trust boundary for gossip ingest (called from
    /// `StateBroadcastHandler::receive_item`). Unlike [`merge_tenant_spend`]
    /// — used for trusted full-snapshot-to-full-snapshot merges where every
    /// slot is already locally attested — a single broadcast should only
    /// ever carry its own origin's contribution. Any other slot present in
    /// the payload is dropped rather than merged, so a compromised or buggy
    /// peer cannot forge another site's recorded spend by embedding extra
    /// slots in its own broadcast.
    ///
    /// Also enforces two independent bounds:
    ///
    /// - [`MAX_TRACKED_TENANTS`]: once that many distinct `tenant_id`s are tracked, brand-new `tenant_id`s are silently
    ///   refused.
    /// - [`MAX_TENANT_SPEND_ORIGINS`]: once a single tenant's counter has that many distinct origin slots, a brand-new
    ///   origin's contribution to that tenant is silently refused.
    ///
    /// In both cases, already-tracked tenants/origins keep accepting
    /// updates — only brand-new keys are refused at capacity — to bound
    /// memory growth from gossip carrying arbitrary attacker-supplied
    /// `tenant_id`s or an unbounded number of claimed origin identities.
    ///
    /// [`merge_tenant_spend`]: Self::merge_tenant_spend
    pub fn merge_tenant_spend_from_origin(&mut self, origin_site: &str, other: &BTreeMap<String, GCounter>) {
        for (tenant_id, counter) in other {
            let origin_only = counter.retain_origin(origin_site);
            if origin_only.total() == 0 {
                continue;
            }
            if !self.tenant_spend.contains_key(tenant_id) && self.tenant_spend.len() >= MAX_TRACKED_TENANTS {
                continue;
            }
            let entry = self
                .tenant_spend
                .entry(tenant_id.clone())
                .or_insert_with(|| GCounter::new(self.site_id.clone()));
            if !entry.has_slot(origin_site) && entry.slot_count() >= MAX_TENANT_SPEND_ORIGINS {
                continue;
            }
            entry.merge(&origin_only);
        }
    }

    /// Remove `origin_site`'s contribution from every tenant's spend counter.
    ///
    /// Used by the SWIM runtime's dead-member eviction sweep (mirrors
    /// [`remove_origin_providers`](Self::remove_origin_providers)) so an
    /// evicted site's slot doesn't linger forever. A tenant whose spend
    /// counter becomes entirely empty as a result is pruned from the map, to
    /// bound its long-term growth as origins churn.
    pub fn remove_origin_tenant_spend(&mut self, origin_site: &str) {
        self.tenant_spend.retain(|_, counter| {
            counter.remove_slot(origin_site);
            counter.total() > 0
        });
    }

    /// Increment this site's local slot of a tenant's spend counter.
    ///
    /// Creates the tenant's counter if this is the first spend recorded for
    /// it. `amount_cents` is in cents (see [`GridStateSnapshot::tenant_spend`]).
    pub fn increment_tenant_spend(&mut self, tenant_id: &str, amount_cents: u64) {
        self.tenant_spend
            .entry(tenant_id.to_owned())
            .or_insert_with(|| GCounter::new(self.site_id.clone()))
            .increment(amount_cents);
    }

    /// Replace provider records owned by one authoritative origin snapshot.
    ///
    /// SWIM state envelopes carry an origin-local transport revision that
    /// advances for metric-only changes and periodic repairs. Provider records
    /// from other origins are retained. Records whose embedded `site_id` does
    /// not match `origin_site` are ignored.
    pub fn replace_origin_providers(&mut self, origin_site: &str, revision: u64, authoritative: &Self) {
        self.providers.retain(|_, provider| provider.site_id != origin_site);
        for provider in authoritative
            .providers
            .values()
            .filter(|provider| provider.site_id == origin_site)
        {
            let mut provider = provider.clone();
            provider.revision = revision;
            origin_site.clone_into(&mut provider.writer_id);
            self.upsert_provider(provider);
        }
    }

    /// Remove all provider records originating from `origin_site`.
    ///
    /// Used by the SWIM runtime's dead-member eviction sweep to clean up
    /// state from a site that has been dead longer than the eviction TTL.
    pub fn remove_origin_providers(&mut self, origin_site: &str) {
        self.providers.retain(|_, p| p.site_id != origin_site);
    }

    /// Return a provider by network/site/provider identity.
    #[must_use]
    pub fn provider(&self, network_id: &str, site_id: &str, provider_id: &str) -> Option<&ProviderState> {
        self.providers.get(&provider_key(network_id, site_id, provider_id))
    }
}

/// Build a stable provider map key.
fn provider_key(network_id: &str, site_id: &str, provider_id: &str) -> String {
    format!("{network_id}/{site_id}/{provider_id}")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(site: &str, provider_id: &str, revision: u64, queue_depth: f64) -> ProviderState {
        ProviderState {
            network_id: "net".to_owned(),
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
            access_policy: ProviderAccessPolicy::default(), // Empty policy = allow all
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
            .provider("net", "site-p", "provider")
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
            .provider("net", "site-p", "provider")
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
            .provider("net", "site-p", "provider")
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
    fn authoritative_origin_replacement_updates_and_removes_provider_records() {
        let mut current = GridStateSnapshot::new("consumer".to_owned());
        current.upsert_provider(provider("site-p", "stale", 9, 0.9));
        current.upsert_provider(provider("site-p", "current", 9, 0.9));
        current.upsert_provider(provider("site-q", "preserved", 9, 0.4));

        let mut authoritative = GridStateSnapshot::new("site-p".to_owned());
        authoritative.upsert_provider(provider("site-p", "current", 1, 0.1));
        authoritative.upsert_provider(provider("site-q", "foreign", 20, 0.2));

        current.replace_origin_providers("site-p", 10, &authoritative);

        assert!(
            current.provider("net", "site-p", "stale").is_none(),
            "provider absent from the authoritative origin snapshot must be removed"
        );
        let updated = current
            .provider("net", "site-p", "current")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(updated.revision, 10, "transport revision must govern the origin record");
        assert_eq!(updated.metrics.queue_depth, Some(0.1), "metric-only change must apply");
        assert!(
            current.provider("net", "site-q", "preserved").is_some(),
            "records owned by other origins must remain"
        );
        assert!(
            current.provider("net", "site-q", "foreign").is_none(),
            "an origin cannot authoritatively replace another site's provider"
        );
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
            .provider("net", "site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        let ba_provider = ba
            .provider("net", "site-p", "provider")
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
            .provider("net", "site-p", "provider")
            .unwrap_or_else(|| std::process::abort());
        let right = a_then_bc
            .provider("net", "site-p", "provider")
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
            restored.provider("net", "site-p", "provider").is_some(),
            "provider must survive serde"
        );
    }

    #[test]
    fn snapshot_bincode_round_trip_with_default_access_policy() {
        // Test bincode round-trip with default (empty) access policy
        let mut snap = GridStateSnapshot::new("site-a".to_owned());
        let mut provider_state = provider("site-a", "provider-default", 1, 0.5);
        provider_state.access_policy = ProviderAccessPolicy::default(); // Empty policy
        snap.upsert_provider(provider_state);

        let bytes =
            bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        let (restored, _len): (GridStateSnapshot, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                .unwrap_or_else(|_| std::process::abort());

        let restored_provider = restored
            .provider("net", "site-a", "provider-default")
            .unwrap_or_else(|| std::process::abort());
        assert!(
            restored_provider.access_policy.match_labels.is_empty(),
            "default access policy must survive bincode round-trip"
        );
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "test needs comprehensive assertions")]
    fn snapshot_bincode_round_trip_with_restricted_access_policy() {
        // Test bincode round-trip with restricted access policy
        let mut snap = GridStateSnapshot::new("site-b".to_owned());
        let mut provider_state = provider("site-b", "provider-restricted", 2, 0.8);
        provider_state.access_policy = ProviderAccessPolicy {
            match_labels: [
                ("env".to_owned(), "prod".to_owned()),
                ("team".to_owned(), "platform".to_owned()),
            ]
            .into_iter()
            .collect(),
        };
        snap.upsert_provider(provider_state);

        let bytes =
            bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        let (restored, _len): (GridStateSnapshot, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                .unwrap_or_else(|_| std::process::abort());

        let restored_provider = restored
            .provider("net", "site-b", "provider-restricted")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            restored_provider.access_policy.match_labels.len(),
            2,
            "restricted access policy labels must survive bincode round-trip"
        );
        assert_eq!(
            restored_provider.access_policy.match_labels.get("env"),
            Some(&"prod".to_owned()),
            "env label must survive bincode round-trip"
        );
        assert_eq!(
            restored_provider.access_policy.match_labels.get("team"),
            Some(&"platform".to_owned()),
            "team label must survive bincode round-trip"
        );
    }

    #[test]
    fn provider_access_policy_json_decode_missing_field_defaults_to_allow_all() {
        // Test JSON deserialization when access_policy field is missing
        // This ensures that #[serde(default)] works for JSON compatibility
        let json_without_access_policy = r#"{
            "network_id": "net",
            "site_id": "site-c",
            "provider_id": "provider-json",
            "routing_cluster": "site-c",
            "models": ["model-y"],
            "backend_kind": "local",
            "phase": "Available",
            "metrics": {},
            "revision": 5,
            "writer_id": "site-c"
        }"#;

        let provider: ProviderState =
            serde_json::from_str(json_without_access_policy).unwrap_or_else(|_| std::process::abort());

        assert!(
            provider.access_policy.match_labels.is_empty(),
            "missing access_policy field must default to empty (allow all)"
        );
    }

    #[test]
    fn remove_origin_providers_clears_only_matching_site() {
        let mut snap = GridStateSnapshot::new("consumer".to_owned());
        snap.upsert_provider(provider("site-a", "p1", 1, 0.5));
        snap.upsert_provider(provider("site-a", "p2", 1, 0.6));
        snap.upsert_provider(provider("site-b", "p1", 1, 0.4));

        snap.remove_origin_providers("site-a");

        assert!(
            snap.provider("net", "site-a", "p1").is_none(),
            "evicted origin's providers must be removed"
        );
        assert!(
            snap.provider("net", "site-a", "p2").is_none(),
            "all providers from evicted origin must be removed"
        );
        assert!(
            snap.provider("net", "site-b", "p1").is_some(),
            "other origins' providers must be preserved"
        );
    }

    // -----------------------------------------------------------------------
    // tenant_spend tests (C1-C8)
    // -----------------------------------------------------------------------

    #[test]
    fn new_snapshot_has_empty_tenant_spend() {
        let snap = GridStateSnapshot::new("site-p".to_owned());
        assert!(
            snap.tenant_spend.is_empty(),
            "new snapshot must start with no tenant spend"
        );
    }

    #[test]
    fn merge_adds_tenant_present_only_in_other() {
        let mut a = GridStateSnapshot::new("site-a".to_owned());
        let mut b = GridStateSnapshot::new("site-b".to_owned());
        b.increment_tenant_spend("tenant-x", 500);

        a.merge(&b);

        assert_eq!(
            a.tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            500,
            "tenant absent from self before merge must be added from other"
        );
    }

    #[test]
    fn merge_tenant_spend_takes_per_site_max() {
        let mut a = GridStateSnapshot::new("site-a".to_owned());
        a.increment_tenant_spend("tenant-x", 100);
        let mut b = GridStateSnapshot::new("site-b".to_owned());
        b.increment_tenant_spend("tenant-x", 200);

        a.merge(&b);

        assert_eq!(
            a.tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            300,
            "per-site slots must sum via GCounter's max-per-slot merge rule"
        );
    }

    #[test]
    fn merge_tenant_spend_is_idempotent() {
        let mut a = GridStateSnapshot::new("site-a".to_owned());
        a.increment_tenant_spend("tenant-x", 100);
        let duplicate = a.clone();

        a.merge(&duplicate);

        assert_eq!(
            a.tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            100,
            "merging a duplicate snapshot must not double-count tenant spend"
        );
    }

    #[test]
    fn merge_tenant_spend_is_commutative() {
        let mut a = GridStateSnapshot::new("site-a".to_owned());
        a.increment_tenant_spend("tenant-x", 100);
        let mut b = GridStateSnapshot::new("site-b".to_owned());
        b.increment_tenant_spend("tenant-x", 200);

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b;
        ba.merge(&a);

        assert_eq!(
            ab.tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            ba.tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            "tenant spend merge must be commutative"
        );
    }

    #[test]
    fn merge_tenant_spend_is_associative() {
        let mut a = GridStateSnapshot::new("site-a".to_owned());
        a.increment_tenant_spend("tenant-x", 100);
        let mut b = GridStateSnapshot::new("site-b".to_owned());
        b.increment_tenant_spend("tenant-x", 200);
        let mut c = GridStateSnapshot::new("site-c".to_owned());
        c.increment_tenant_spend("tenant-x", 300);

        let mut ab_then_c = a.clone();
        ab_then_c.merge(&b);
        ab_then_c.merge(&c);

        let mut bc = b;
        bc.merge(&c);
        let mut a_then_bc = a;
        a_then_bc.merge(&bc);

        assert_eq!(
            ab_then_c
                .tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            a_then_bc
                .tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            "tenant spend merge must be associative"
        );
    }

    #[test]
    fn tenant_spend_survives_bincode_round_trip() {
        let mut snap = GridStateSnapshot::new("site-p".to_owned());
        snap.increment_tenant_spend("tenant-x", 4200);

        let bytes =
            bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap_or_else(|_| std::process::abort());
        let (restored, _len): (GridStateSnapshot, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            restored
                .tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            4200,
            "tenant_spend must survive bincode round-trip"
        );
    }

    #[test]
    fn tenant_spend_json_decode_missing_field_defaults_to_empty() {
        // Simulates a peer running an older build whose wire payload predates
        // this field: start from a real serialized snapshot (avoiding any
        // guesswork about OrSet's/ProviderState's internal JSON shape), strip
        // the key, and confirm deserialization still succeeds and defaults.
        let mut snap = GridStateSnapshot::new("site-p".to_owned());
        snap.increment_tenant_spend("tenant-x", 100);
        let mut json = serde_json::to_value(&snap).unwrap_or_else(|_| std::process::abort());
        json.as_object_mut()
            .unwrap_or_else(|| std::process::abort())
            .remove("tenant_spend");

        let restored: GridStateSnapshot = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());

        assert!(
            restored.tenant_spend.is_empty(),
            "missing tenant_spend field must default to an empty map, not fail to deserialize"
        );
    }

    // -----------------------------------------------------------------------
    // merge_tenant_spend_from_origin: wire-ingest trust boundary (security)
    // -----------------------------------------------------------------------

    #[test]
    fn merge_tenant_spend_from_origin_accepts_the_claimed_origins_own_slot() {
        let mut local = GridStateSnapshot::new("site-local".to_owned());
        let mut incoming = BTreeMap::new();
        let mut counter = GCounter::new("site-p".to_owned());
        counter.increment(500);
        incoming.insert("tenant-x".to_owned(), counter);

        local.merge_tenant_spend_from_origin("site-p", &incoming);

        assert_eq!(
            local
                .tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            500,
            "the claimed origin's own slot must be merged normally"
        );
    }

    #[test]
    fn merge_tenant_spend_from_origin_drops_forged_foreign_slots() {
        // Security: a broadcast claiming origin "site-a" must not be able to
        // smuggle in an inflated slot for "site-b" and have it accepted as
        // site-b's real contribution — that would let one compromised/buggy
        // peer forge another site's recorded spend mesh-wide.
        let mut local = GridStateSnapshot::new("site-local".to_owned());
        let mut forged = BTreeMap::new();
        let mut counter = GCounter::new("site-a".to_owned());
        counter.increment(10); // site-a's genuine contribution
        let mut fake_site_b = GCounter::new("site-b".to_owned());
        fake_site_b.increment(u64::MAX);
        counter.merge(&fake_site_b); // origin locally folds in a forged foreign slot
        forged.insert("tenant-x".to_owned(), counter);

        local.merge_tenant_spend_from_origin("site-a", &forged);

        assert_eq!(
            local
                .tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            10,
            "only the claimed origin's own slot must be accepted; the forged site-b slot must be dropped"
        );
    }

    #[test]
    fn merge_tenant_spend_from_origin_ignores_tenant_the_claimed_origin_never_contributed_to() {
        // The incoming counter carries only a foreign slot (no slot at all
        // for the claimed origin) — after stripping the forged foreign slot
        // there is nothing genuine left to merge, so the tenant must not be
        // created locally at all (not even as a zero entry).
        let mut local = GridStateSnapshot::new("site-local".to_owned());
        let mut forged = BTreeMap::new();
        let mut foreign_only = GCounter::new("site-b".to_owned());
        foreign_only.increment(999);
        forged.insert("tenant-x".to_owned(), foreign_only);

        local.merge_tenant_spend_from_origin("site-a", &forged);

        assert!(
            !local.tenant_spend.contains_key("tenant-x"),
            "a tenant with zero genuine contribution from the claimed origin must not be created"
        );
    }

    #[test]
    fn merge_tenant_spend_from_origin_repeated_calls_are_idempotent() {
        let mut local = GridStateSnapshot::new("site-local".to_owned());
        let mut incoming = BTreeMap::new();
        let mut counter = GCounter::new("site-p".to_owned());
        counter.increment(500);
        incoming.insert("tenant-x".to_owned(), counter);

        local.merge_tenant_spend_from_origin("site-p", &incoming);
        local.merge_tenant_spend_from_origin("site-p", &incoming);

        assert_eq!(
            local
                .tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            500,
            "re-merging the same origin broadcast must not double-count (max-per-slot semantics)"
        );
    }

    // -----------------------------------------------------------------------
    // remove_origin_tenant_spend + tenant-count bound (security: unbounded growth)
    // -----------------------------------------------------------------------

    #[test]
    fn remove_origin_tenant_spend_strips_only_that_origins_slot() {
        let mut snap = GridStateSnapshot::new("site-local".to_owned());
        let mut incoming_a = BTreeMap::new();
        let mut counter_a = GCounter::new("site-a".to_owned());
        counter_a.increment(100);
        incoming_a.insert("tenant-x".to_owned(), counter_a);
        snap.merge_tenant_spend_from_origin("site-a", &incoming_a);

        let mut incoming_b = BTreeMap::new();
        let mut counter_b = GCounter::new("site-b".to_owned());
        counter_b.increment(200);
        incoming_b.insert("tenant-x".to_owned(), counter_b);
        snap.merge_tenant_spend_from_origin("site-b", &incoming_b);

        snap.remove_origin_tenant_spend("site-a");

        assert_eq!(
            snap.tenant_spend
                .get("tenant-x")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            200,
            "evicting site-a must remove only its own contribution, leaving site-b's spend intact"
        );
    }

    #[test]
    fn remove_origin_tenant_spend_prunes_tenant_entry_once_fully_empty() {
        let mut snap = GridStateSnapshot::new("site-local".to_owned());
        let mut incoming = BTreeMap::new();
        let mut counter = GCounter::new("site-a".to_owned());
        counter.increment(100);
        incoming.insert("tenant-x".to_owned(), counter);
        snap.merge_tenant_spend_from_origin("site-a", &incoming);

        snap.remove_origin_tenant_spend("site-a");

        assert!(
            !snap.tenant_spend.contains_key("tenant-x"),
            "a tenant with no remaining site contributions must be pruned entirely, not left as a zero entry"
        );
    }

    #[test]
    fn merge_tenant_spend_from_origin_refuses_new_tenants_once_at_capacity() {
        // Security: bound the number of distinct tenant_id keys accepted from
        // gossip so a malicious/buggy origin flooding arbitrary tenant_ids
        // cannot grow tenant_spend without bound. Already-tracked tenants may
        // still accumulate; only brand-new tenant_ids are refused at capacity.
        let mut snap = GridStateSnapshot::new("site-local".to_owned());
        for i in 0..MAX_TRACKED_TENANTS {
            let mut incoming = BTreeMap::new();
            let mut counter = GCounter::new("site-a".to_owned());
            counter.increment(1);
            incoming.insert(format!("tenant-{i}"), counter);
            snap.merge_tenant_spend_from_origin("site-a", &incoming);
        }
        assert_eq!(
            snap.tenant_spend.len(),
            MAX_TRACKED_TENANTS,
            "precondition: at capacity"
        );

        let mut overflow = BTreeMap::new();
        let mut counter = GCounter::new("site-a".to_owned());
        counter.increment(1);
        overflow.insert("tenant-overflow".to_owned(), counter);
        snap.merge_tenant_spend_from_origin("site-a", &overflow);

        assert_eq!(
            snap.tenant_spend.len(),
            MAX_TRACKED_TENANTS,
            "a brand-new tenant_id must be refused once the map is at its hard bound"
        );
        assert!(
            !snap.tenant_spend.contains_key("tenant-overflow"),
            "the refused tenant_id must not be present at all"
        );
    }

    #[test]
    fn merge_tenant_spend_from_origin_still_updates_already_tracked_tenant_at_capacity() {
        let mut snap = GridStateSnapshot::new("site-local".to_owned());
        for i in 0..MAX_TRACKED_TENANTS {
            let mut incoming = BTreeMap::new();
            let mut counter = GCounter::new("site-a".to_owned());
            counter.increment(1);
            incoming.insert(format!("tenant-{i}"), counter);
            snap.merge_tenant_spend_from_origin("site-a", &incoming);
        }

        let mut more = BTreeMap::new();
        let mut counter = GCounter::new("site-a".to_owned());
        counter.increment(99);
        more.insert("tenant-0".to_owned(), counter);
        snap.merge_tenant_spend_from_origin("site-a", &more);

        assert_eq!(
            snap.tenant_spend
                .get("tenant-0")
                .unwrap_or_else(|| std::process::abort())
                .total(),
            99,
            "an already-tracked tenant must still accept updates while the map is at capacity"
        );
    }

    // -----------------------------------------------------------------------
    // MAX_TENANT_SPEND_ORIGINS bound (grid#52: distinct site-slots per tenant)
    // -----------------------------------------------------------------------

    /// Merge one origin's `amount`-cent contribution to `tenant_id` into `snap`.
    fn merge_one_origin_spend(snap: &mut GridStateSnapshot, origin: &str, tenant_id: &str, amount: u64) {
        let mut incoming = BTreeMap::new();
        let mut counter = GCounter::new(origin.to_owned());
        counter.increment(amount);
        incoming.insert(tenant_id.to_owned(), counter);
        snap.merge_tenant_spend_from_origin(origin, &incoming);
    }

    /// Fill `tenant_id`'s counter to exactly `count` distinct origin slots
    /// (`site-0..site-{count - 1}`, one cent each).
    fn fill_tenant_origins(snap: &mut GridStateSnapshot, tenant_id: &str, count: usize) {
        for i in 0..count {
            merge_one_origin_spend(snap, &format!("site-{i}"), tenant_id, 1);
        }
    }

    /// Look up `tenant_id`'s counter, aborting the test if it's missing.
    fn tenant_counter<'a>(snap: &'a GridStateSnapshot, tenant_id: &str) -> &'a GCounter {
        snap.tenant_spend
            .get(tenant_id)
            .unwrap_or_else(|| std::process::abort())
    }

    #[test]
    fn merge_tenant_spend_from_origin_refuses_new_origin_once_tenant_at_capacity() {
        let mut snap = GridStateSnapshot::new("site-local".to_owned());
        fill_tenant_origins(&mut snap, "tenant-x", MAX_TENANT_SPEND_ORIGINS);

        merge_one_origin_spend(&mut snap, "site-overflow", "tenant-x", 1);

        let counter = tenant_counter(&snap, "tenant-x");
        assert_eq!(
            counter.slot_count(),
            MAX_TENANT_SPEND_ORIGINS,
            "a brand-new origin must be refused once this tenant's counter is at its hard bound -- this caps \
             distinct site-slots per tenant, independent of MAX_TRACKED_TENANTS (which bounds tenant_id count)"
        );
        assert!(
            !counter.has_slot("site-overflow"),
            "the refused origin's slot must not be present at all"
        );
        assert_eq!(
            counter.total(),
            u64::try_from(MAX_TENANT_SPEND_ORIGINS).unwrap_or(u64::MAX),
            "the refused origin's contribution must not be reflected in the total"
        );
    }

    #[test]
    fn merge_tenant_spend_from_origin_still_updates_already_tracked_origin_at_capacity() {
        let mut snap = GridStateSnapshot::new("site-local".to_owned());
        fill_tenant_origins(&mut snap, "tenant-x", MAX_TENANT_SPEND_ORIGINS);

        merge_one_origin_spend(&mut snap, "site-0", "tenant-x", 99);

        let counter = tenant_counter(&snap, "tenant-x");
        assert_eq!(
            counter.slot_count(),
            MAX_TENANT_SPEND_ORIGINS,
            "site-0 already has a slot, so a higher-amount update from it must still merge even at the \
             origin-slot cap; updating an already-tracked origin must not change the slot count"
        );
        assert_eq!(
            counter.total(),
            u64::try_from(MAX_TENANT_SPEND_ORIGINS - 1).unwrap_or(u64::MAX) + 99,
            "an already-tracked origin must still accept updates (max-per-slot) while the tenant is at capacity"
        );
    }

    #[test]
    fn merge_tenant_spend_from_origin_admits_new_origin_one_below_capacity() {
        let mut snap = GridStateSnapshot::new("site-local".to_owned());
        fill_tenant_origins(&mut snap, "tenant-x", MAX_TENANT_SPEND_ORIGINS - 1);

        merge_one_origin_spend(&mut snap, "site-last", "tenant-x", 1);

        let counter = tenant_counter(&snap, "tenant-x");
        assert_eq!(
            counter.slot_count(),
            MAX_TENANT_SPEND_ORIGINS,
            "pins the exact boundary: with MAX_TENANT_SPEND_ORIGINS - 1 origins already tracked, the \
             (MAX_TENANT_SPEND_ORIGINS)-th distinct origin must still be admitted -- the cap itself, not \
             cap - 1, is the refusal point"
        );
        assert!(
            counter.has_slot("site-last"),
            "the newly-admitted origin's slot must be present"
        );
    }

    #[test]
    fn merge_tenant_spend_from_origin_caps_are_independent_per_tenant() {
        let mut snap = GridStateSnapshot::new("site-local".to_owned());
        fill_tenant_origins(&mut snap, "tenant-x", MAX_TENANT_SPEND_ORIGINS);

        merge_one_origin_spend(&mut snap, "site-0", "tenant-y", 5);

        let tenant_y = tenant_counter(&snap, "tenant-y");
        assert_eq!(
            tenant_y.slot_count(),
            1,
            "tenant-y's cap is independent of tenant-x's; another tenant reaching its origin-slot cap must \
             not affect this one"
        );
        assert_eq!(tenant_y.total(), 5);
    }
}
