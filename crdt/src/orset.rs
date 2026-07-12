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
/// ```
/// use crdt::OrSet;
///
/// let mut set = OrSet::new("site-a".to_owned());
/// set.add("model-x".to_owned());
/// assert!(set.contains(&"model-x".to_owned()));
/// ```
#[derive(Clone, Debug, Deserialize, Serialize)]
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

    /// Add an item to the set.
    pub fn add(&mut self, item: T) {
        self.counter = self.counter.wrapping_add(1);
        let tag = Tag {
            counter: self.counter,
            site_id: self.site_id.clone(),
        };
        self.entries.entry(item).or_default().insert(tag);
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
        for tags in self.entries.values_mut() {
            tags.retain(|tag| !self.tombstones.contains(tag));
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
        let mut a = OrSet::new("a".to_owned());
        let mut b = OrSet::new("b".to_owned());
        a.add("x".to_owned());
        b.add("y".to_owned());
        a.merge(&b);
        assert!(a.contains(&"x".to_owned()), "should have x");
        assert!(a.contains(&"y".to_owned()), "should have y");
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
        let mut a = OrSet::new("a".to_owned());
        let mut b = OrSet::new("b".to_owned());
        a.add("x".to_owned());
        b.add("y".to_owned());

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

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
        let mut a = OrSet::new("a".to_owned());
        a.add("x".to_owned());
        let snapshot = a.clone();
        a.merge(&snapshot);
        assert_eq!(a.len(), 1, "merge with self must be idempotent");
    }

    #[test]
    fn merge_is_associative() {
        let mut a = OrSet::new("a".to_owned());
        let mut b = OrSet::new("b".to_owned());
        let mut c = OrSet::new("c".to_owned());
        a.add("x".to_owned());
        b.add("y".to_owned());
        c.add("z".to_owned());

        let mut ab_then_c = a.clone();
        ab_then_c.merge(&b);
        ab_then_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_then_bc = a.clone();
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
}
