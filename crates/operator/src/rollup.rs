//! The `Available`, `Progressing` and `Degraded` roll-up every kind writes.
//!
//! `Available` copies the first gate that is not `True`, `Degraded` copies the first
//! concern that holds, and `Progressing` names what moves without a person.

use crate::crd::inference_provider::{Condition, ConditionStatus};

/// Whether the object serves what it declares.
pub const AVAILABLE: &str = "Available";

/// Whether a change is moving that needs no person to finish.
pub const PROGRESSING: &str = "Progressing";

/// Whether a non-gating condition is bad.
pub const DEGRADED: &str = "Degraded";

/// The reason an inverted condition carries in its normal state.
pub const AS_EXPECTED: &str = "AsExpected";

/// The `Progressing` reason while a gate describes an older generation.
pub const NEW_GENERATION: &str = "NewGeneration";

/// A cause in `(reason, message)` form, for `Degraded` or `Progressing`.
pub type Cause<'cause> = (&'cause str, &'cause str);

/// The generation a condition describes and the time a new status starts.
#[derive(Clone, Copy, Debug)]
pub struct Stamp<'now> {
    /// The object's `metadata.generation`.
    pub generation: Option<i64>,
    /// Now, RFC 3339.
    pub now: &'now str,
}

/// The three conditions for `gates` in order, `degraded` when a concern holds, and
/// `in_flight` when a change is moving.
///
/// Each keeps the `lastTransitionTime` it holds in `current` while its status is unchanged.
#[must_use]
pub fn trio(
    current: &[Condition],
    gates: &[&Condition],
    (degraded, in_flight): (Option<Cause<'_>>, Option<Cause<'_>>),
    stamp: Stamp<'_>,
) -> [Condition; 3] {
    let generation = stamp.generation;
    let available = gates
        .iter()
        .find(|gate| gate.status != ConditionStatus::True)
        .map_or_else(
            || (ConditionStatus::True, AVAILABLE, String::new()),
            |gate| (gate.status, gate.reason.as_str(), gate.message.clone()),
        );
    let behind = generation.and_then(|want| {
        gates.iter().find_map(|gate| {
            gate.observed_generation
                .filter(|seen| *seen < want)
                .map(|seen| (gate, seen, want))
        })
    });
    let progressing = match (behind, in_flight) {
        (Some((gate, seen, want)), _) => (
            ConditionStatus::True,
            NEW_GENERATION,
            format!("{} describes generation {seen}, the spec is at {want}", gate.type_),
        ),
        (None, Some((reason, message))) => (ConditionStatus::True, reason, message.to_owned()),
        (None, None) => (ConditionStatus::False, AS_EXPECTED, String::new()),
    };
    let degraded = degraded.map_or_else(
        || (ConditionStatus::False, AS_EXPECTED, String::new()),
        |(reason, message)| (ConditionStatus::True, reason, message.to_owned()),
    );
    [
        condition(current, AVAILABLE, available, stamp),
        condition(current, PROGRESSING, progressing, stamp),
        condition(current, DEGRADED, degraded, stamp),
    ]
}

/// Whether `desired` says something `current` does not, ignoring `lastTransitionTime`.
#[must_use]
pub fn differs(current: &[Condition], desired: &[Condition]) -> bool {
    desired.iter().any(|want| {
        current.iter().find(|held| held.type_ == want.type_).is_none_or(|held| {
            held.status != want.status
                || held.reason != want.reason
                || held.message != want.message
                || held.observed_generation != want.observed_generation
        })
    })
}

/// The condition `type_` reading `(status, reason, message)` at `stamp`, keeping the
/// `lastTransitionTime` `current` holds while its status is unchanged.
#[must_use]
pub fn condition(
    current: &[Condition],
    type_: &str,
    (status, reason, message): (ConditionStatus, &str, String),
    stamp: Stamp<'_>,
) -> Condition {
    let last_transition_time = current
        .iter()
        .find(|held| held.type_ == type_ && held.status == status)
        .map_or_else(|| stamp.now.to_owned(), |held| held.last_transition_time.clone());
    Condition {
        type_: type_.to_owned(),
        status,
        reason: reason.to_owned(),
        message,
        last_transition_time,
        observed_generation: stamp.generation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: &str = "2026-10-26T12:00:00Z";
    const EARLIER: &str = "2026-10-25T12:00:00Z";

    const fn at(generation: i64) -> Stamp<'static> {
        Stamp {
            generation: Some(generation),
            now: NOW,
        }
    }

    fn gate(type_: &str, status: ConditionStatus, reason: &str, generation: i64) -> Condition {
        Condition {
            type_: type_.to_owned(),
            status,
            reason: reason.to_owned(),
            message: format!("{type_} says {reason}"),
            last_transition_time: EARLIER.to_owned(),
            observed_generation: Some(generation),
        }
    }

    #[test]
    fn available_reads_its_own_name_when_every_gate_is_true() {
        let accepted = gate("Accepted", ConditionStatus::True, "Accepted", 1);
        let [available, progressing, degraded] = trio(&[], &[&accepted], (None, None), at(1));
        assert_eq!(
            (available.status, available.reason.as_str()),
            (ConditionStatus::True, AVAILABLE)
        );
        assert_eq!(
            (progressing.status, progressing.reason.as_str()),
            (ConditionStatus::False, AS_EXPECTED)
        );
        assert_eq!(
            (degraded.status, degraded.reason.as_str()),
            (ConditionStatus::False, AS_EXPECTED)
        );
    }

    #[test]
    fn available_copies_the_first_gate_that_is_not_true() {
        let accepted = gate("Accepted", ConditionStatus::True, "Accepted", 1);
        let refs = gate("ResolvedRefs", ConditionStatus::False, "BackendNotFound", 1);
        let live = gate("EndpointLive", ConditionStatus::Unknown, "AwaitingFirstCheck", 1);
        let [available, ..] = trio(&[], &[&accepted, &refs, &live], (None, None), at(1));
        assert_eq!(
            (available.status, available.reason.as_str(), available.message.as_str()),
            (
                ConditionStatus::False,
                "BackendNotFound",
                "ResolvedRefs says BackendNotFound"
            )
        );
    }

    #[test]
    fn a_gate_behind_the_generation_is_progressing_before_anything_in_flight() {
        let live = gate("EndpointLive", ConditionStatus::True, "EndpointLive", 1);
        let [_, progressing, _] = trio(&[], &[&live], (None, Some(("Probing", "probing"))), at(2));
        assert_eq!(progressing.reason, NEW_GENERATION);
        let [_, in_flight, _] = trio(&[], &[&live], (None, Some(("Probing", "probing"))), at(1));
        assert_eq!(
            (in_flight.status, in_flight.reason.as_str()),
            (ConditionStatus::True, "Probing")
        );
    }

    #[test]
    fn a_concern_degrades() {
        let live = gate("EndpointLive", ConditionStatus::True, "EndpointLive", 1);
        let [_, _, degraded] = trio(&[], &[&live], (Some(("ScrapeFailed", "scrape failed")), None), at(1));
        assert_eq!(
            (degraded.status, degraded.reason.as_str()),
            (ConditionStatus::True, "ScrapeFailed")
        );
    }

    #[test]
    fn the_transition_time_moves_only_with_the_status() {
        let held = vec![gate(AVAILABLE, ConditionStatus::False, "Invalid", 1)];
        let refs = gate("ResolvedRefs", ConditionStatus::False, "BackendNotFound", 1);
        let [same_status, ..] = trio(&held, &[&refs], (None, None), at(1));
        assert_eq!(
            same_status.last_transition_time, EARLIER,
            "a reason change alone keeps the time"
        );
        let ok = gate("ResolvedRefs", ConditionStatus::True, "ResolvedRefs", 1);
        let [new_status, ..] = trio(&held, &[&ok], (None, None), at(1));
        assert_eq!(new_status.last_transition_time, NOW);
    }

    #[test]
    fn differs_ignores_the_transition_time_only() {
        let held = vec![gate(AVAILABLE, ConditionStatus::True, AVAILABLE, 1)];
        let mut moved = gate(AVAILABLE, ConditionStatus::True, AVAILABLE, 1);
        moved.last_transition_time = NOW.to_owned();
        assert!(!differs(&held, std::slice::from_ref(&moved)));
        moved.message = "new".to_owned();
        assert!(differs(&held, std::slice::from_ref(&moved)));
        assert!(differs(&[], &held), "an absent condition differs");
    }
}
