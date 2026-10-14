//! Last-Writer-Wins Register.
//!
//! A CRDT register where concurrent writes are resolved by
//! timestamp: the write with the higher timestamp wins, with the greater
//! value breaking timestamp ties.
//! Used for metrics like queue depth, KV cache utilization,
//! latency, cost, and health state.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// LWW Register
// ---------------------------------------------------------------------------

/// A Last-Writer-Wins register holding a value with a timestamp.
///
/// Merge semantics: the register with the higher timestamp
/// wins. Equal timestamps are resolved by comparing values
/// (deterministic tie-break).
/// Values must implement a total order. Floating-point metrics require a
/// wrapper with a consistent total ordering, including NaN and signed zero.
///
/// ```
/// use crdt::LwwRegister;
///
/// let mut r = LwwRegister::new(42, 1);
/// r.merge(&LwwRegister::new(99, 2));
/// assert_eq!(r.value(), 99);
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LwwRegister<T> {
    /// The current timestamp.
    timestamp: u64,

    /// The current value.
    value: T,
}

impl<T: Clone + Ord> LwwRegister<T> {
    /// Create a new register with the given value and timestamp.
    #[must_use]
    pub fn new(value: T, timestamp: u64) -> Self {
        Self { timestamp, value }
    }

    /// Return the current value.
    #[must_use]
    pub fn value(&self) -> T
    where
        T: Copy,
    {
        self.value
    }

    /// Return a reference to the current value.
    #[must_use]
    pub fn value_ref(&self) -> &T {
        &self.value
    }

    /// Return the current timestamp.
    #[must_use]
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Update when the timestamp is newer or an equal timestamp has a greater value.
    pub fn set(&mut self, value: T, timestamp: u64) {
        if (timestamp, &value) > (self.timestamp, &self.value) {
            self.value = value;
            self.timestamp = timestamp;
        }
    }

    /// Merge another register into this one.
    ///
    /// The greater `(timestamp, value)` pair wins.
    pub fn merge(&mut self, other: &Self) {
        if (other.timestamp, &other.value) > (self.timestamp, &self.value) {
            self.value.clone_from(&other.value);
            self.timestamp = other.timestamp;
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
    fn new_creates_register() {
        let reg = LwwRegister::new(42, 1);
        assert_eq!(reg.value(), 42, "initial value");
        assert_eq!(reg.timestamp(), 1, "initial timestamp");
    }

    #[test]
    fn set_updates_on_newer_timestamp() {
        let mut reg = LwwRegister::new(1, 1);
        reg.set(2, 2);
        assert_eq!(reg.value(), 2, "should update");
    }

    #[test]
    fn set_ignores_older_timestamp() {
        let mut reg = LwwRegister::new(1, 5);
        reg.set(2, 3);
        assert_eq!(reg.value(), 1, "should not update");
    }

    #[test]
    fn merge_takes_newer() {
        let mut reg_a = LwwRegister::new(1, 1);
        let reg_b = LwwRegister::new(2, 2);
        reg_a.merge(&reg_b);
        assert_eq!(reg_a.value(), 2, "should take newer");
    }

    #[test]
    fn merge_keeps_newer_self() {
        let mut reg_a = LwwRegister::new(1, 5);
        let reg_b = LwwRegister::new(2, 3);
        reg_a.merge(&reg_b);
        assert_eq!(reg_a.value(), 1, "should keep self");
    }

    #[test]
    fn merge_equal_timestamps_takes_greater_value() {
        let mut reg_a = LwwRegister::new(1, 1);
        let reg_b = LwwRegister::new(2, 1);
        reg_a.merge(&reg_b);
        assert_eq!(reg_a.value(), 2, "the greater value breaks a timestamp tie");
    }

    #[test]
    fn works_with_strings() {
        let mut reg = LwwRegister::new("old".to_owned(), 1);
        reg.merge(&LwwRegister::new("new".to_owned(), 2));
        assert_eq!(reg.value_ref(), "new", "string merge");
    }

    #[test]
    fn set_equal_timestamp_takes_greater_value() {
        let mut reg = LwwRegister::new(1, 5);
        reg.set(99, 5);
        assert_eq!(reg.value(), 99, "set uses the same tie-break as merge");
        reg.set(50, 5);
        assert_eq!(reg.value(), 99, "a smaller tied value cannot undo a write");
        assert_eq!(reg.timestamp(), 5, "set with equal timestamp must not change timestamp");
    }

    #[test]
    fn merge_equal_timestamps_is_commutative() {
        let mut reg_a = LwwRegister::new(1, 1);
        let reg_b = LwwRegister::new(2, 1);
        reg_a.merge(&reg_b);
        assert_eq!(reg_a.value(), 2, "a takes the greater tied value");

        let mut other_b = LwwRegister::new(2, 1);
        let other_a = LwwRegister::new(1, 1);
        other_b.merge(&other_a);
        assert_eq!(reg_a, other_b, "arrival order cannot change the winner");
    }

    #[test]
    fn lww_register_serde_round_trip() {
        let reg = LwwRegister::new(425, 100);
        let json = serde_json::to_string(&reg).unwrap_or_else(|_| std::process::abort());
        let restored: LwwRegister<u64> = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(restored.value(), 425, "serde round-trip must preserve value");
        assert_eq!(restored.timestamp(), 100, "serde round-trip must preserve timestamp");
    }

    #[test]
    fn merges_obey_semilattice_laws_across_timestamps_and_values() {
        let writes = [
            LwwRegister::new("a", 1),
            LwwRegister::new("z", 1),
            LwwRegister::new("b", 2),
        ];
        for first in &writes {
            let mut idempotent = first.clone();
            idempotent.merge(first);
            assert_eq!(&idempotent, first, "duplicate delivery cannot change a register");
            for second in &writes {
                let mut forward = first.clone();
                forward.merge(second);
                let mut reverse = second.clone();
                reverse.merge(first);
                assert_eq!(forward, reverse, "merge must be commutative");
                for third in &writes {
                    let mut left = forward.clone();
                    left.merge(third);
                    let mut right = second.clone();
                    right.merge(third);
                    right.merge(first);
                    assert_eq!(left, right, "grouping deliveries cannot change the winner");
                }
            }
        }
    }
}
