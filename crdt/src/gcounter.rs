//! Grow-only Counter (G-Counter).
//!
//! A CRDT counter where each site increments its own slot.
//! The total is the sum across all slots. Under partition,
//! each site sees a lower bound (may overspend) rather than
//! hard-rejecting. Used for per-tenant budget tracking.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// G-Counter
// ---------------------------------------------------------------------------

/// A grow-only counter with per-site slots.
///
/// Each site increments its own slot. The total value is
/// the sum across all slots. Merge takes the max of each
/// slot. This provides monotonically increasing counts
/// with bounded error under partition.
///
/// ```
/// use crdt::GCounter;
///
/// let mut c = GCounter::new("site-a".to_owned());
/// c.increment(10);
/// assert_eq!(c.total(), 10);
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GCounter {
    /// Site identifier for this replica.
    site_id: String,

    /// Per-site counters.
    slots: BTreeMap<String, u64>,
}

impl GCounter {
    /// Create a new counter for the given site.
    #[must_use]
    pub fn new(site_id: String) -> Self {
        Self {
            site_id,
            slots: BTreeMap::new(),
        }
    }

    /// Increment this site's counter by the given amount.
    pub fn increment(&mut self, amount: u64) {
        let slot = self.slots.entry(self.site_id.clone()).or_default();
        *slot = slot.saturating_add(amount);
    }

    /// Return the total count across all sites.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.slots.values().sum()
    }

    /// Return this site's local count.
    #[must_use]
    pub fn local(&self) -> u64 {
        self.slots.get(&self.site_id).copied().unwrap_or(0)
    }

    /// Merge another counter into this one.
    ///
    /// Takes the max of each site's slot.
    pub fn merge(&mut self, other: &Self) {
        for (site, count) in &other.slots {
            let slot = self.slots.entry(site.clone()).or_default();
            *slot = (*slot).max(*count);
        }
    }

    /// Return a copy of this counter containing only the slot for `origin_site`.
    ///
    /// Used at trust boundaries (e.g. gossip wire-ingest) where a payload's
    /// claimed origin should only ever be believed for its own contribution.
    /// Any other slot present in `self` — legitimate or forged — is dropped,
    /// mirroring how provider records are scoped to their claimed origin
    /// before being accepted.
    #[must_use]
    pub fn retain_origin(&self, origin_site: &str) -> Self {
        let mut retained = Self::new(origin_site.to_owned());
        if let Some(&value) = self.slots.get(origin_site) {
            retained.slots.insert(origin_site.to_owned(), value);
        }
        retained
    }

    /// Remove the slot belonging to `site`, if present.
    ///
    /// Used when evicting a dead site so its contribution doesn't linger in
    /// other tenants' counters forever. A no-op if `site` never contributed.
    pub fn remove_slot(&mut self, site: &str) {
        self.slots.remove(site);
    }

    /// Return the number of distinct sites with a recorded slot.
    ///
    /// Used to enforce a hard cap on distinct origins contributing to a
    /// single counter (see `grid_state::MAX_TENANT_SPEND_ORIGINS`), as
    /// defense-in-depth against a compromised or churning origin claiming
    /// unbounded new site identities.
    #[must_use]
    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    /// Return whether `site` already has a recorded slot.
    #[must_use]
    pub fn has_slot(&self, site: &str) -> bool {
        self.slots.contains_key(site)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_counter_is_zero() {
        let c = GCounter::new("a".to_owned());
        assert_eq!(c.total(), 0, "new counter should be zero");
    }

    #[test]
    fn increment_adds_to_local() {
        let mut c = GCounter::new("a".to_owned());
        c.increment(5);
        c.increment(3);
        assert_eq!(c.total(), 8, "should sum increments");
        assert_eq!(c.local(), 8, "local should match");
    }

    #[test]
    fn merge_takes_max() {
        let mut a = GCounter::new("a".to_owned());
        let mut b = GCounter::new("b".to_owned());
        a.increment(10);
        b.increment(20);
        a.merge(&b);
        assert_eq!(a.total(), 30, "should sum both sites");
    }

    #[test]
    fn merge_max_per_site() {
        let mut a = GCounter::new("a".to_owned());
        a.increment(10);

        let mut b = GCounter::new("b".to_owned());
        b.slots.insert("a".to_owned(), 5);
        b.increment(20);

        a.merge(&b);
        assert_eq!(a.local(), 10, "should keep own higher value");
        assert_eq!(a.total(), 30, "total = max(10,5) + 20");
    }

    #[test]
    fn merge_is_commutative() {
        let mut a = GCounter::new("a".to_owned());
        let mut b = GCounter::new("b".to_owned());
        a.increment(10);
        b.increment(20);

        let a2 = a.clone();
        a.merge(&b);
        b.merge(&a2);

        assert_eq!(a.total(), b.total(), "merge should be commutative");
    }

    #[test]
    fn merge_is_idempotent() {
        let mut a = GCounter::new("a".to_owned());
        a.increment(10);
        let snapshot = a.clone();
        a.merge(&snapshot);
        assert_eq!(a.total(), 10, "merge with self should be idempotent");
    }

    #[test]
    fn saturating_increment() {
        let mut c = GCounter::new("a".to_owned());
        c.increment(u64::MAX);
        c.increment(1);
        assert_eq!(c.total(), u64::MAX, "should saturate");
    }

    #[test]
    fn merge_is_associative() {
        let mut a = GCounter::new("a".to_owned());
        let mut b = GCounter::new("b".to_owned());
        let mut c = GCounter::new("c".to_owned());
        a.increment(10);
        b.increment(20);
        c.increment(30);

        let mut ab_then_c = a.clone();
        ab_then_c.merge(&b);
        ab_then_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_then_bc = a.clone();
        a_then_bc.merge(&bc);

        assert_eq!(
            ab_then_c.total(),
            a_then_bc.total(),
            "(a merge b) merge c == a merge (b merge c)"
        );
    }

    #[test]
    fn retain_origin_keeps_only_the_named_slot() {
        let mut c = GCounter::new("site-a".to_owned());
        c.increment(10);
        c.slots.insert("site-b".to_owned(), 999);
        c.slots.insert("site-c".to_owned(), 42);

        let retained = c.retain_origin("site-a");

        assert_eq!(retained.total(), 10, "only site-a's slot must survive");
        assert_eq!(
            retained.local(),
            10,
            "the retained counter is keyed by site-a, so local() reflects its slot"
        );
    }

    #[test]
    fn retain_origin_for_absent_slot_is_zero() {
        let mut c = GCounter::new("site-a".to_owned());
        c.increment(10);

        let retained = c.retain_origin("site-b");

        assert_eq!(
            retained.total(),
            0,
            "a slot the origin never wrote must retain as zero, not forged"
        );
    }

    #[test]
    fn remove_slot_drops_only_the_named_site() {
        let mut c = GCounter::new("site-a".to_owned());
        c.increment(10);
        c.slots.insert("site-b".to_owned(), 20);

        c.remove_slot("site-a");

        assert_eq!(
            c.total(),
            20,
            "removing site-a's slot must leave site-b's contribution intact"
        );
    }

    #[test]
    fn remove_slot_for_absent_site_is_a_no_op() {
        let mut c = GCounter::new("site-a".to_owned());
        c.increment(10);

        c.remove_slot("site-never-contributed");

        assert_eq!(c.total(), 10, "removing an absent slot must not change the total");
    }

    #[test]
    fn slot_count_reflects_distinct_sites_after_merge() {
        let mut a = GCounter::new("site-a".to_owned());
        a.increment(10);
        assert_eq!(a.slot_count(), 1, "one increment from one site is one slot");

        let mut b = GCounter::new("site-b".to_owned());
        b.increment(5);
        a.merge(&b);

        assert_eq!(a.slot_count(), 2, "merging in a second site's slot must be counted");
    }

    #[test]
    fn slot_count_is_zero_for_a_fresh_counter() {
        let c = GCounter::new("site-a".to_owned());
        assert_eq!(c.slot_count(), 0, "a counter with no increments yet has no slots");
    }

    #[test]
    fn has_slot_reports_presence_per_site() {
        let mut c = GCounter::new("site-a".to_owned());
        c.increment(10);

        assert!(c.has_slot("site-a"), "site-a incremented, so it must have a slot");
        assert!(
            !c.has_slot("site-b"),
            "site-b never contributed, so it must not have a slot"
        );
    }

    #[test]
    fn gcounter_serde_round_trip() {
        let mut c = GCounter::new("site-x".to_owned());
        c.increment(42);
        let json = serde_json::to_string(&c).unwrap_or_else(|_| std::process::abort());
        let restored: GCounter = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(restored.total(), 42, "serde round-trip must preserve total");
        assert_eq!(restored.local(), 42, "serde round-trip must preserve local");
    }
}
