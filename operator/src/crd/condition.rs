//! Status conditions in the `metav1.Condition` shape.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Whether a condition holds.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum ConditionStatus {
    /// The condition holds.
    True,
    /// The condition does not hold.
    False,
    /// The operator cannot tell yet.
    Unknown,
}

/// One observed condition, keyed by `type`.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Condition type, unique within the list.
    #[serde(rename = "type")]
    pub type_: String,

    /// Whether the condition holds.
    pub status: ConditionStatus,

    /// The `metadata.generation` this condition was computed from.
    #[serde(default)]
    pub observed_generation: i64,

    /// RFC 3339 time the status last changed.
    pub last_transition_time: String,

    /// Machine-readable reason.
    pub reason: String,

    /// Human-readable detail.
    #[serde(default)]
    pub message: String,
}

/// The status fields of a [`Condition`], without its transition time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Observed {
    /// Condition type.
    pub type_: &'static str,
    /// Whether it holds.
    pub status: ConditionStatus,
    /// Machine-readable reason.
    pub reason: String,
    /// Human-readable detail.
    pub message: String,
}

impl Observed {
    /// An observed condition of `type_` with `status` and `reason`.
    #[must_use]
    pub fn new(type_: &'static str, status: ConditionStatus, reason: &str) -> Self {
        Self {
            type_,
            status,
            reason: reason.to_owned(),
            message: String::new(),
        }
    }

    /// Attach a human-readable message.
    #[must_use]
    pub fn with_message(mut self, message: String) -> Self {
        self.message = message;
        self
    }
}

/// Build the condition list for `observed`, keeping each previous transition time
/// whose status did not change, so an unchanged reconcile writes identical status.
#[must_use]
pub fn reconcile_conditions(
    previous: &[Condition],
    observed: Vec<Observed>,
    generation: i64,
    now: &str,
) -> Vec<Condition> {
    observed
        .into_iter()
        .map(|next| {
            let last_transition_time = previous
                .iter()
                .find(|old| old.type_ == next.type_ && old.status == next.status)
                .map_or_else(|| now.to_owned(), |old| old.last_transition_time.clone());
            Condition {
                type_: next.type_.to_owned(),
                status: next.status,
                observed_generation: generation,
                last_transition_time,
                reason: next.reason,
                message: next.message,
            }
        })
        .collect()
}

/// Conditions for `observed` at `generation`, stamped now.
#[must_use]
pub fn refresh(previous: Option<&[Condition]>, observed: Vec<Observed>, generation: i64) -> Vec<Condition> {
    reconcile_conditions(previous.unwrap_or_default(), observed, generation, &now_rfc3339())
}

/// Current time as RFC 3339, the format conditions record, empty on a format failure.
#[must_use]
pub fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
#[expect(clippy::indexing_slicing, reason = "tests")]
mod tests {
    use super::*;

    fn available(status: ConditionStatus, reason: &str) -> Observed {
        Observed::new("Available", status, reason)
    }

    #[test]
    fn an_unchanged_status_keeps_its_transition_time() {
        let first = reconcile_conditions(&[], vec![available(ConditionStatus::True, "Ready")], 1, "t1");
        let second = reconcile_conditions(&first, vec![available(ConditionStatus::True, "Ready")], 2, "t2");
        assert_eq!(second[0].last_transition_time, "t1", "no transition, no new time");
        assert_eq!(second[0].observed_generation, 2, "the generation still advances");
    }

    #[test]
    fn a_status_change_records_a_new_transition_time() {
        let first = reconcile_conditions(&[], vec![available(ConditionStatus::True, "Ready")], 1, "t1");
        let second = reconcile_conditions(&first, vec![available(ConditionStatus::False, "NoneReady")], 2, "t2");
        assert_eq!(second[0].last_transition_time, "t2");
        assert_eq!(second[0].status, ConditionStatus::False);
        assert_eq!(second[0].reason, "NoneReady");
    }
}
