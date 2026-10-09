//! Observed-Remove Set (OR-Set).
//!
//! A CRDT set where items can be added and removed.
//! Concurrent add and remove of the same item resolves
//! with add-wins semantics. Used for capabilities:
//! models, tools, and agents.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// OR-Set
// ---------------------------------------------------------------------------

/// An Observed-Remove Set with add-wins semantics.
///
/// Each add is tagged with a unique counter. A remove
/// only removes the specific add instances that were
/// observed. Concurrent add/remove resolves to the
/// item being present (add wins).
///
/// Each site identity must have only one active writer. Before reusing a site
/// identity, restore its complete serialized state or merge all its prior
/// additions and removals. Clones are snapshots, not independent writers with
/// the same identity. Causal tombstones must be retained for delayed messages.
///
/// ```
/// use crdt::OrSet;
///
/// let mut set = OrSet::new("site-a".to_owned());
/// set.add("model-x".to_owned());
/// assert!(set.contains(&"model-x".to_owned()));
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OrSet<T: Ord> {
    /// Counter for generating unique add tags.
    counter: u64,

    /// Active entries: item → set of (site, counter) tags.
    entries: BTreeMap<T, BTreeSet<Tag>>,

    /// Site identifier for this replica.
    site_id: String,

    /// Tombstones: removed (site, counter) tags.
    tombstones: BTreeSet<Tag>,
}

/// A unique tag identifying a specific add operation.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct Tag {
    /// Counter value when the add occurred.
    counter: u64,

    /// Site that performed the add.
    site_id: String,
}

impl<T: Clone + Ord> OrSet<T> {
    /// Create a new empty OR-Set for the given site.
    #[must_use]
    pub fn new(site_id: String) -> Self {
        Self {
            counter: 0,
            entries: BTreeMap::new(),
            site_id,
            tombstones: BTreeSet::new(),
        }
    }

    /// Add an item, returning false without changing the set if tags are exhausted.
    ///
    /// An exhausted writer must use a new site identity for further additions.
    pub fn add(&mut self, item: T) -> bool {
        let Some(counter) = self.counter.checked_add(1) else {
            return false;
        };
        self.counter = counter;
        let tag = Tag {
            counter: self.counter,
            site_id: self.site_id.clone(),
        };
        self.entries.entry(item).or_default().insert(tag);
        true
    }

    /// Remove an item from the set.
    ///
    /// Only removes the currently observed add tags. A
    /// concurrent add from another site will re-add the item.
    pub fn remove(&mut self, item: &T) {
        if let Some(tags) = self.entries.remove(item) {
            self.tombstones.extend(tags);
        }
    }

    /// Check if an item is in the set.
    #[must_use]
    pub fn contains(&self, item: &T) -> bool {
        self.entries.get(item).is_some_and(|tags| !tags.is_empty())
    }

    /// Return all items in the set.
    #[must_use]
    pub fn items(&self) -> Vec<&T> {
        self.entries
            .iter()
            .filter(|(_, tags)| !tags.is_empty())
            .map(|(item, _)| item)
            .collect()
    }

    /// Return the number of items in the set.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.iter().filter(|(_, tags)| !tags.is_empty()).count()
    }

    /// Check if the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Merge another OR-Set into this one.
    ///
    /// Adds all items with tags not in our tombstones.
    /// Removes items whose tags are all tombstoned.
    pub fn merge(&mut self, other: &Self) {
        if self.site_id == other.site_id {
            self.counter = self.counter.max(other.counter);
        }
        for tag in other.entries.values().flatten().chain(&other.tombstones) {
            if tag.site_id == self.site_id {
                self.counter = self.counter.max(tag.counter);
            }
        }
        for (item, other_tags) in &other.entries {
            let local = self.entries.entry(item.clone()).or_default();
            for tag in other_tags {
                if !self.tombstones.contains(tag) {
                    local.insert(tag.clone());
                }
            }
        }
        self.tombstones.extend(other.tombstones.iter().cloned());
        self.remove_tombstoned_tags();
    }
}

impl<T: Ord> OrSet<T> {
    /// Remove tags that appear in the tombstone set.
    fn remove_tombstoned_tags(&mut self) {
        self.entries.retain(|_, tags| {
            tags.retain(|tag| !self.tombstones.contains(tag));
            !tags.is_empty()
        });
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_contains() {
        let mut set = OrSet::new("a".to_owned());
        set.add("x".to_owned());
        assert!(set.contains(&"x".to_owned()), "should contain x");
        assert!(!set.contains(&"y".to_owned()), "should not contain y");
    }

    #[test]
    fn remove_deletes_item() {
        let mut set = OrSet::new("a".to_owned());
        set.add("x".to_owned());
        set.remove(&"x".to_owned());
        assert!(!set.contains(&"x".to_owned()), "should be removed");
    }

    #[test]
    fn add_wins_over_concurrent_remove() {
        let mut site_a = OrSet::new("a".to_owned());
        site_a.add("x".to_owned());

        let mut site_b = site_a.clone();
        site_b.site_id = "b".to_owned();

        site_a.remove(&"x".to_owned());
        site_b.add("x".to_owned());

        site_a.merge(&site_b);
        assert!(site_a.contains(&"x".to_owned()), "add should win");
    }

    #[test]
    fn merge_combines_items() {
        let mut set_a = OrSet::new("a".to_owned());
        let mut set_b = OrSet::new("b".to_owned());
        set_a.add("x".to_owned());
        set_b.add("y".to_owned());
        set_a.merge(&set_b);
        assert!(set_a.contains(&"x".to_owned()), "should have x");
        assert!(set_a.contains(&"y".to_owned()), "should have y");
    }

    #[test]
    fn len_counts_active() {
        let mut set = OrSet::new("a".to_owned());
        set.add("x".to_owned());
        set.add("y".to_owned());
        assert_eq!(set.len(), 2, "should have 2 items");
        set.remove(&"x".to_owned());
        assert_eq!(set.len(), 1, "should have 1 item after remove");
    }

    #[test]
    fn items_returns_active() {
        let mut set = OrSet::new("a".to_owned());
        set.add("x".to_owned());
        set.add("y".to_owned());
        let items = set.items();
        assert_eq!(items.len(), 2, "should return 2 items");
    }

    #[test]
    fn is_empty_on_new() {
        let set: OrSet<String> = OrSet::new("a".to_owned());
        assert!(set.is_empty(), "new set should be empty");
    }

    #[test]
    fn add_same_item_twice_counts_once() {
        let mut set = OrSet::new("a".to_owned());
        set.add("x".to_owned());
        set.add("x".to_owned());
        assert_eq!(set.len(), 1, "duplicate adds must count as one item");
        assert!(set.contains(&"x".to_owned()), "item must still be present");
    }

    #[test]
    fn merge_is_commutative() {
        let mut set_a = OrSet::new("a".to_owned());
        let mut set_b = OrSet::new("b".to_owned());
        set_a.add("x".to_owned());
        set_b.add("y".to_owned());

        let mut ab = set_a.clone();
        ab.merge(&set_b);

        let mut ba = set_b.clone();
        ba.merge(&set_a);

        assert_eq!(ab.len(), ba.len(), "merge must be commutative");
        assert_eq!(
            ab.contains(&"x".to_owned()),
            ba.contains(&"x".to_owned()),
            "x present in both"
        );
        assert_eq!(
            ab.contains(&"y".to_owned()),
            ba.contains(&"y".to_owned()),
            "y present in both"
        );
    }

    #[test]
    fn merge_is_idempotent() {
        let mut set_a = OrSet::new("a".to_owned());
        set_a.add("x".to_owned());
        let snapshot = set_a.clone();
        set_a.merge(&snapshot);
        assert_eq!(set_a.len(), 1, "merge with self must be idempotent");
    }

    #[test]
    fn merge_is_associative() {
        let mut set_a = OrSet::new("a".to_owned());
        let mut set_b = OrSet::new("b".to_owned());
        let mut set_c = OrSet::new("c".to_owned());
        set_a.add("x".to_owned());
        set_b.add("y".to_owned());
        set_c.add("z".to_owned());

        let mut ab_then_c = set_a.clone();
        ab_then_c.merge(&set_b);
        ab_then_c.merge(&set_c);

        let mut bc = set_b.clone();
        bc.merge(&set_c);
        let mut a_then_bc = set_a.clone();
        a_then_bc.merge(&bc);

        assert_eq!(
            ab_then_c.len(),
            a_then_bc.len(),
            "(a merge b) merge c == a merge (b merge c)"
        );
    }

    #[test]
    fn orset_serde_round_trip() {
        let mut set = OrSet::new("site-x".to_owned());
        set.add("model-a".to_owned());
        set.add("model-b".to_owned());
        let json = serde_json::to_string(&set).unwrap_or_else(|_| std::process::abort());
        let restored: OrSet<String> = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(restored.len(), 2, "serde round-trip must preserve item count");
        assert!(
            restored.contains(&"model-a".to_owned()),
            "model-a must survive round-trip"
        );
        assert!(
            restored.contains(&"model-b".to_owned()),
            "model-b must survive round-trip"
        );
    }

    #[test]
    fn restored_writer_advances_past_active_and_removed_tags() -> Result<(), serde_json::Error> {
        let mut original = OrSet::new("site-a".to_owned());
        assert!(
            original.add("kept".to_owned()),
            "the original writer must allocate the retained tag"
        );
        assert!(
            original.add("returned".to_owned()),
            "the original writer must allocate the tag that will be removed"
        );
        original.remove(&"returned".to_owned());
        let bytes = serde_json::to_string(&original)?;
        let persisted: OrSet<String> = serde_json::from_str(&bytes)?;
        let mut relay = OrSet::new("relay".to_owned());
        relay.merge(&persisted);
        let mut restarted = OrSet::new("site-a".to_owned());
        restarted.merge(&relay);
        assert!(
            restarted.add("returned".to_owned()),
            "the restored writer must allocate a fresh tag for the removed value"
        );
        restarted.merge(&persisted);
        relay.merge(&restarted);

        assert!(
            restarted.contains(&"returned".to_owned()),
            "old tombstones cannot erase a fresh addition"
        );
        assert_eq!(restarted.items(), relay.items(), "relayed restoration must converge");
        assert_eq!(restarted.len(), 2, "both the original and fresh additions survive");
        Ok(())
    }

    #[test]
    fn merging_same_site_preserves_a_reserved_counter_without_tags() {
        let mut saved = OrSet::<String>::new("a".to_owned());
        saved.counter = 12;
        let mut restored = OrSet::new("a".to_owned());
        restored.merge(&saved);
        assert!(
            restored.add("x".to_owned()),
            "the restored reserved counter must permit a fresh tag"
        );
        assert_eq!(restored.counter, 13, "restoration must honor the serialized counter");
    }

    #[test]
    fn exhausted_writer_refuses_additions_without_reusing_tombstones() {
        let mut set = OrSet::new("a".to_owned());
        set.counter = u64::MAX - 1;
        assert!(set.add("x".to_owned()), "the final unique tag is usable");
        set.remove(&"x".to_owned());
        let exhausted = set.clone();

        assert!(!set.add("x".to_owned()), "exhaustion must be observable");
        assert!(!set.add("y".to_owned()), "exhaustion cannot reset on a later call");
        assert_eq!(set, exhausted, "refused additions leave causal history intact");
    }

    #[test]
    fn removed_buckets_are_pruned_but_delayed_additions_remain_removed() {
        let mut source = OrSet::new("a".to_owned());
        assert!(
            source.add("x".to_owned()),
            "the source must allocate the tag replayed after removal"
        );
        let delayed = source.clone();
        source.remove(&"x".to_owned());
        let mut receiver = OrSet::new("b".to_owned());
        receiver.merge(&delayed);
        receiver.merge(&source);
        receiver.merge(&delayed);

        assert!(
            receiver.entries.is_empty(),
            "removed values must not retain empty buckets"
        );
        assert_eq!(
            receiver.tombstones, source.tombstones,
            "replay protection must survive pruning"
        );
        assert!(
            !receiver.contains(&"x".to_owned()),
            "delayed traffic cannot resurrect a removed value"
        );
    }
}
