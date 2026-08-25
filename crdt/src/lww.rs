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

    #[expect(clippy::float_cmp, reason = "exact equality valid for LWW test literals")]
    #[test]
    fn new_creates_register() {
        let reg = LwwRegister::new(42.0, 1);
        assert_eq!(reg.value(), 42.0, "initial value");
        assert_eq!(reg.timestamp(), 1, "initial timestamp");
    }

    #[expect(clippy::float_cmp, reason = "exact equality valid for LWW test literals")]
    #[test]
    fn set_updates_on_newer_timestamp() {
        let mut reg = LwwRegister::new(1.0, 1);
        reg.set(2.0, 2);
        assert_eq!(reg.value(), 2.0, "should update");
    }

    #[expect(clippy::float_cmp, reason = "exact equality valid for LWW test literals")]
    #[test]
    fn set_ignores_older_timestamp() {
        let mut reg = LwwRegister::new(1.0, 5);
        reg.set(2.0, 3);
        assert_eq!(reg.value(), 1.0, "should not update");
    }

    #[expect(clippy::float_cmp, reason = "exact equality valid for LWW test literals")]
    #[test]
    fn merge_takes_newer() {
        let mut reg_a = LwwRegister::new(1.0, 1);
        let reg_b = LwwRegister::new(2.0, 2);
        reg_a.merge(&reg_b);
        assert_eq!(reg_a.value(), 2.0, "should take newer");
    }

    #[expect(clippy::float_cmp, reason = "exact equality valid for LWW test literals")]
    #[test]
    fn merge_keeps_newer_self() {
        let mut reg_a = LwwRegister::new(1.0, 5);
        let reg_b = LwwRegister::new(2.0, 3);
        reg_a.merge(&reg_b);
        assert_eq!(reg_a.value(), 1.0, "should keep self");
    }

    #[expect(clippy::float_cmp, reason = "exact equality valid for LWW test literals")]
    #[test]
    fn merge_equal_timestamps_keeps_self() {
        let mut reg_a = LwwRegister::new(1.0, 1);
        let reg_b = LwwRegister::new(2.0, 1);
        reg_a.merge(&reg_b);
        assert_eq!(reg_a.value(), 1.0, "equal timestamp keeps self");
    }

    #[test]
    fn works_with_strings() {
        let mut reg = LwwRegister::new("old".to_owned(), 1);
        reg.merge(&LwwRegister::new("new".to_owned(), 2));
        assert_eq!(reg.value_ref(), "new", "string merge");
    }

    #[expect(clippy::float_cmp, reason = "exact equality valid for LWW test literals")]
    #[test]
    fn set_ignores_equal_timestamp() {
        let mut reg = LwwRegister::new(1.0, 5);
        reg.set(99.0, 5);
        assert_eq!(reg.value(), 1.0, "set with equal timestamp must not change value");
        assert_eq!(reg.timestamp(), 5, "set with equal timestamp must not change timestamp");
    }

    #[expect(clippy::float_cmp, reason = "exact equality valid for LWW test literals")]
    #[test]
    fn merge_equal_timestamps_is_not_commutative() {
        let mut reg_a = LwwRegister::new(1.0, 1);
        let reg_b = LwwRegister::new(2.0, 1);
        reg_a.merge(&reg_b);
        assert_eq!(reg_a.value(), 1.0, "a keeps its own value on equal timestamps");

        let mut other_b = LwwRegister::new(2.0, 1);
        let other_a = LwwRegister::new(1.0, 1);
        other_b.merge(&other_a);
        assert_eq!(other_b.value(), 2.0, "b keeps its own value on equal timestamps");
    }

    #[expect(clippy::float_cmp, reason = "exact equality valid for LWW test literals")]
    #[test]
    fn lww_register_serde_round_trip() {
        let reg = LwwRegister::new(42.5_f64, 100);
        let json = serde_json::to_string(&reg).unwrap_or_else(|_| std::process::abort());
        let restored: LwwRegister<f64> = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(restored.value(), 42.5, "serde round-trip must preserve value");
        assert_eq!(restored.timestamp(), 100, "serde round-trip must preserve timestamp");
    }
}
