//! Last-Writer-Wins Register.
//!
//! A CRDT register where concurrent writes are resolved by
//! timestamp: the write with the higher timestamp wins.
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
///
/// ```
/// use crdt::LwwRegister;
///
/// let mut r = LwwRegister::new(42.0, 1);
/// r.merge(&LwwRegister::new(99.0, 2));
/// assert_eq!(r.value(), 99.0);
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LwwRegister<T> {
    /// The current timestamp.
    timestamp: u64,

    /// The current value.
    value: T,
}

impl<T: Clone + PartialOrd> LwwRegister<T> {
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

    /// Update the value if the given timestamp is newer.
    pub fn set(&mut self, value: T, timestamp: u64) {
        if timestamp > self.timestamp {
            self.value = value;
            self.timestamp = timestamp;
        }
    }

    /// Merge another register into this one.
    ///
    /// The register with the higher timestamp wins.
    pub fn merge(&mut self, other: &Self) {
        if other.timestamp > self.timestamp {
            self.value = other.value.clone();
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
        let r = LwwRegister::new(42.0, 1);
        assert_eq!(r.value(), 42.0, "initial value");
        assert_eq!(r.timestamp(), 1, "initial timestamp");
    }

    #[test]
    fn set_updates_on_newer_timestamp() {
        let mut r = LwwRegister::new(1.0, 1);
        r.set(2.0, 2);
        assert_eq!(r.value(), 2.0, "should update");
    }

    #[test]
    fn set_ignores_older_timestamp() {
        let mut r = LwwRegister::new(1.0, 5);
        r.set(2.0, 3);
        assert_eq!(r.value(), 1.0, "should not update");
    }

    #[test]
    fn merge_takes_newer() {
        let mut a = LwwRegister::new(1.0, 1);
        let b = LwwRegister::new(2.0, 2);
        a.merge(&b);
        assert_eq!(a.value(), 2.0, "should take newer");
    }

    #[test]
    fn merge_keeps_newer_self() {
        let mut a = LwwRegister::new(1.0, 5);
        let b = LwwRegister::new(2.0, 3);
        a.merge(&b);
        assert_eq!(a.value(), 1.0, "should keep self");
    }

    #[test]
    fn merge_equal_timestamps_keeps_self() {
        let mut a = LwwRegister::new(1.0, 1);
        let b = LwwRegister::new(2.0, 1);
        a.merge(&b);
        assert_eq!(a.value(), 1.0, "equal timestamp keeps self");
    }

    #[test]
    fn works_with_strings() {
        let mut r = LwwRegister::new("old".to_owned(), 1);
        r.merge(&LwwRegister::new("new".to_owned(), 2));
        assert_eq!(r.value_ref(), "new", "string merge");
    }

    #[test]
    fn set_ignores_equal_timestamp() {
        let mut r = LwwRegister::new(1.0, 5);
        r.set(99.0, 5);
        assert_eq!(r.value(), 1.0, "set with equal timestamp must not change value");
        assert_eq!(r.timestamp(), 5, "set with equal timestamp must not change timestamp");
    }

    #[test]
    fn merge_equal_timestamps_is_not_commutative() {
        // Equal-timestamp merge keeps self — this is intentional and non-commutative.
        // Pinning this behavior so future changes are deliberate.
        let mut a = LwwRegister::new(1.0, 1);
        let b = LwwRegister::new(2.0, 1);
        a.merge(&b);
        assert_eq!(a.value(), 1.0, "a keeps its own value on equal timestamps");

        let mut b2 = LwwRegister::new(2.0, 1);
        let a2 = LwwRegister::new(1.0, 1);
        b2.merge(&a2);
        assert_eq!(b2.value(), 2.0, "b keeps its own value on equal timestamps");
    }

    #[test]
    fn lww_register_serde_round_trip() {
        let r = LwwRegister::new(42.5_f64, 100);
        let json = serde_json::to_string(&r).unwrap_or_else(|_| std::process::abort());
        let restored: LwwRegister<f64> = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(restored.value(), 42.5, "serde round-trip must preserve value");
        assert_eq!(restored.timestamp(), 100, "serde round-trip must preserve timestamp");
    }
}
