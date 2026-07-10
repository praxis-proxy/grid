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
/// use grid_crdt::GCounter;
///
/// let mut c = GCounter::new("site-a".to_owned());
/// c.increment(10);
/// assert_eq!(c.total(), 10);
/// ```
#[derive(Clone, Debug, Deserialize, Serialize)]
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
}
