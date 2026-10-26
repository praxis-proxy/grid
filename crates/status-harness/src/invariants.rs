//! The status invariants, each a function over what the rounds observed. Each returns
//! why it failed, naming the round and the condition, so a test reads as its number.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::{Failure, Observed, Write, diff};

/// A condition's identity across rounds: its status, reason and message.
fn verdict(condition: &Value) -> (String, String, String) {
    let field = |key: &str| {
        condition
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    (field("status"), field("reason"), field("message"))
}

/// The condition `type_` in `observed`, as its verdict.
fn verdict_of(observed: &Observed, type_: &str) -> Option<(String, String, String)> {
    observed.condition(type_).map(verdict)
}

/// A condition's type, status, reason, message and observed generation.
type Identity = (String, String, String, String, Option<i64>);

/// Each condition's [`Identity`]: what invariant 1 holds still, leaving `lastTransitionTime`
/// to invariant 2.
fn identities(observed: &Observed) -> Vec<Identity> {
    let mut held: Vec<Identity> = observed
        .conditions
        .iter()
        .map(|condition| {
            let (status, reason, message) = verdict(condition);
            let type_ = condition.get("type").and_then(Value::as_str).unwrap_or_default();
            let generation = condition.get("observedGeneration").and_then(Value::as_i64);
            (type_.to_owned(), status, reason, message, generation)
        })
        .collect();
    // A rewrite moves a condition to the end of the list, which is not a change.
    held.sort();
    held
}

/// Every condition type any round held.
fn types(rounds: &[Observed]) -> Vec<String> {
    let mut seen = Vec::new();
    for round in rounds {
        for condition in &round.conditions {
            if let Some(type_) = condition.get("type").and_then(Value::as_str)
                && !seen.iter().any(|held: &String| held == type_)
            {
                seen.push(type_.to_owned());
            }
        }
    }
    seen
}

/// The phase, reason and message `status` carries, for kinds that still write a phase.
fn phase_of(observed: &Observed) -> (String, String, String) {
    (
        observed.field("phase").to_owned(),
        observed.field("reason").to_owned(),
        observed.field("message").to_owned(),
    )
}

/// Invariant 1 (and 12): with inputs unchanged, every round from `from` on reads as the
/// round before it: each condition's status, reason, message and observed generation, and
/// the phase triple. `lastTransitionTime` is invariant 2's.
///
/// # Errors
///
/// Names the first round and field that changed.
pub fn unchanged_from(rounds: &[Observed], from: usize) -> Result<(), Failure> {
    for (index, pair) in rounds.windows(2).enumerate() {
        let (Some(earlier), Some(later)) = (pair.first(), pair.get(1)) else {
            continue;
        };
        let round = index.saturating_add(1);
        if round < from {
            continue;
        }
        if identities(earlier) != identities(later) {
            return Err(Failure::from(format!(
                "round {round} changed a condition with no input change\n{}",
                diff::between(earlier, later)
            )));
        }
        if phase_of(earlier) != phase_of(later) {
            return Err(Failure::from(format!(
                "round {round} changed the phase triple with no input change\n{}",
                diff::between(earlier, later)
            )));
        }
    }
    Ok(())
}

/// Invariant 2: a condition's `lastTransitionTime` changes only when its status does.
///
/// # Errors
///
/// Names the round and type where the time moved without a status change.
pub fn transition_time_moves_only_with_status(rounds: &[Observed]) -> Result<(), Failure> {
    for type_ in types(rounds) {
        for (index, pair) in rounds.windows(2).enumerate() {
            let (Some(before), Some(after)) = (pair.first(), pair.get(1)) else {
                continue;
            };
            let (Some(earlier), Some(later)) = (before.condition(&type_), after.condition(&type_)) else {
                continue;
            };
            let time = |condition: &Value| condition.get("lastTransitionTime").cloned();
            let status = |condition: &Value| condition.get("status").cloned();
            if time(earlier) != time(later) && status(earlier) == status(later) {
                return Err(Failure::from(format!(
                    "round {}: {type_} moved lastTransitionTime without a status change\n{}",
                    index.saturating_add(1),
                    diff::between(before, after)
                )));
            }
        }
    }
    Ok(())
}

/// Invariant 2, for a kind that still writes a phase: `status.lastTransitionTime` changes
/// only when `status.phase` does.
///
/// # Errors
///
/// Names the round where the time moved without a phase change.
pub fn phase_transition_time_moves_only_with_phase(rounds: &[Observed]) -> Result<(), Failure> {
    for (index, pair) in rounds.windows(2).enumerate() {
        let (Some(earlier), Some(later)) = (pair.first(), pair.get(1)) else {
            continue;
        };
        if earlier.field("lastTransitionTime") != later.field("lastTransitionTime")
            && earlier.field("phase") == later.field("phase")
        {
            return Err(Failure::from(format!(
                "round {}: lastTransitionTime moved while the phase stayed {}",
                index.saturating_add(1),
                later.field("phase")
            )));
        }
    }
    Ok(())
}

/// Invariant 3: after every round, `status.observedGeneration` and each condition's
/// `observedGeneration` equal `metadata.generation`.
///
/// # Errors
///
/// Names the round and field that lags.
pub fn observed_generation_tracks(rounds: &[Observed]) -> Result<(), Failure> {
    for (index, round) in rounds.iter().enumerate() {
        let want = round.generation;
        if let Some(held) = round.status.get("observedGeneration").and_then(Value::as_i64)
            && held != want
        {
            return Err(Failure::from(format!(
                "round {index}: status.observedGeneration {held} lags generation {want}"
            )));
        }
        for condition in &round.conditions {
            let held = condition.get("observedGeneration").and_then(Value::as_i64);
            if held != Some(want) {
                return Err(Failure::from(format!(
                    "round {index}: {} observedGeneration {held:?} lags generation {want}",
                    condition.get("type").and_then(Value::as_str).unwrap_or_default()
                )));
            }
        }
    }
    Ok(())
}

/// Invariant 4: a condition is `Unknown` only with an `AwaitingFirst` reason before its
/// first evaluation, or with `NotEvaluated`, and a type that has been `True` or `False`
/// is never `Unknown` for any other reason.
///
/// # Errors
///
/// Names the round, type and reason that broke the rule.
pub fn unknown_only_before_first_evaluation(rounds: &[Observed]) -> Result<(), Failure> {
    for type_ in types(rounds) {
        let mut evaluated = false;
        for (index, round) in rounds.iter().enumerate() {
            let Some((status, reason, _)) = verdict_of(round, &type_) else {
                continue;
            };
            match status.as_str() {
                "True" | "False" => evaluated = true,
                "Unknown" if reason == "NotEvaluated" => {},
                "Unknown" if !evaluated && reason.starts_with("AwaitingFirst") => {},
                "Unknown" => {
                    return Err(Failure::from(format!(
                        "round {index}: {type_} is Unknown with {reason}\n{}",
                        diff::table(round)
                    )));
                },
                other => return Err(Failure::from(format!("round {index}: {type_} has status {other}"))),
            }
        }
    }
    Ok(())
}

/// Invariant 5: each condition type on `path` is written by exactly one field manager,
/// the one `expected` names for it.
///
/// # Errors
///
/// Names the type written by a second manager, or by the wrong one.
pub fn one_manager_per_type(writes: &[Write], path: &str, expected: &BTreeMap<&str, &str>) -> Result<(), Failure> {
    let mut seen: BTreeMap<String, String> = BTreeMap::new();
    for (type_, manager) in writes
        .iter()
        .filter(|write| write.path == path && write.status)
        .flat_map(written_types)
    {
        if let Some(held) = seen.get(&type_)
            && held != &manager
        {
            return Err(Failure::from(format!("{type_} written by {held} and {manager}")));
        }
        if let Some(want) = expected.get(type_.as_str())
            && *want != manager
        {
            return Err(Failure::from(format!("{type_} written by {manager}, expected {want}")));
        }
        seen.insert(type_, manager);
    }
    Ok(())
}

/// The condition types `write` carries, each with the manager that wrote it.
fn written_types(write: &Write) -> Vec<(String, String)> {
    let manager = write
        .manager
        .clone()
        .unwrap_or_else(|| format!("<{}>", write.content_type));
    write
        .body
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|condition| {
            let type_ = condition.get("type").and_then(Value::as_str).unwrap_or_default();
            (type_.to_owned(), manager.clone())
        })
        .collect()
}

/// Invariant 6: every condition or phase change in round `k` is explained by an input
/// the case changed at round `k` (listed in `changed_at`); a change with no input fails.
///
/// # Errors
///
/// Names the round that changed without an input change.
pub fn changes_explained(rounds: &[Observed], changed_at: &[usize]) -> Result<(), Failure> {
    for (index, pair) in rounds.windows(2).enumerate() {
        let round = index.saturating_add(1);
        let (Some(earlier), Some(later)) = (pair.first(), pair.get(1)) else {
            continue;
        };
        let changed = types(rounds)
            .into_iter()
            .any(|type_| verdict_of(earlier, &type_) != verdict_of(later, &type_))
            || phase_of(earlier) != phase_of(later);
        if changed && !changed_at.contains(&round) {
            return Err(Failure::from(format!(
                "round {round} changed status with no input change"
            )));
        }
    }
    Ok(())
}

/// Invariant 7: after round 1, `Available`, `Progressing` and `Degraded` are present and
/// `Available` equals the roll-up of `gates`, in order: `True` `Available` when every
/// gate is `True`, else the first other gate's status, reason and message.
///
/// # Errors
///
/// Names the missing or wrong roll-up.
pub fn rollup_present_and_equal(observed: &Observed, gates: &[&str]) -> Result<(), Failure> {
    for type_ in ["Available", "Progressing", "Degraded"] {
        if observed.condition(type_).is_none() {
            return Err(Failure::from(format!("{type_} is absent\n{}", diff::table(observed))));
        }
    }
    available_equals_rollup(observed, gates)
}

/// Invariant 7, `Available` alone: it equals the roll-up of `gates`, in order.
///
/// # Errors
///
/// Names an absent gate or the wrong roll-up.
pub fn available_equals_rollup(observed: &Observed, gates: &[&str]) -> Result<(), Failure> {
    let mut present = Vec::with_capacity(gates.len());
    for gate in gates {
        // An absent gate would roll up as True and pass a writer that sets only the trio.
        let held = verdict_of(observed, gate)
            .ok_or_else(|| Failure::from(format!("gate {gate} is absent\n{}", diff::table(observed))))?;
        present.push(held);
    }
    let want = present
        .iter()
        .find(|(status, ..)| status != "True")
        .cloned()
        .unwrap_or_else(|| ("True".to_owned(), "Available".to_owned(), String::new()));
    let got = verdict_of(observed, "Available").unwrap_or_default();
    if want.0 != got.0 || want.1 != got.1 {
        return Err(Failure::from(format!(
            "Available reads {got:?}, the gates say {want:?}\n{}",
            diff::table(observed)
        )));
    }
    Ok(())
}

/// Invariant 8: every reason written is in the closed set `allowed` for its type and
/// status.
///
/// # Errors
///
/// Names the first reason outside the set.
pub fn reasons_within(rounds: &[Observed], allowed: &[(&str, &str, &str)]) -> Result<(), Failure> {
    for (index, round) in rounds.iter().enumerate() {
        for condition in &round.conditions {
            let type_ = condition.get("type").and_then(Value::as_str).unwrap_or_default();
            let (status, reason, _) = verdict(condition);
            if !allowed.iter().any(|(want_type, want_status, want_reason)| {
                *want_type == type_ && *want_status == status && *want_reason == reason
            }) {
                return Err(Failure::from(format!(
                    "round {index}: {type_} {status} {reason} is outside the closed set\n{}",
                    diff::table(round)
                )));
            }
        }
    }
    Ok(())
}

/// Invariant 9: every printer column of `crd` reads a condition, a spec field or an
/// identity field, and none reads `status.phase`, `status.reason` or `status.message`,
/// except a wide-only (priority 1) phase, kept while v1alpha1 derives it.
///
/// # Errors
///
/// Names the column that reads a phase field.
pub fn printer_columns_read_conditions(crd: &Value) -> Result<(), Failure> {
    let versions = crd
        .pointer("/spec/versions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for version in versions {
        let columns = version
            .get("additionalPrinterColumns")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for column in columns {
            let path = column.get("jsonPath").and_then(Value::as_str).unwrap_or_default();
            let name = column.get("name").and_then(Value::as_str).unwrap_or_default();
            let wide = column
                .get("priority")
                .and_then(Value::as_i64)
                .is_some_and(|priority| priority >= 1);
            if wide && path == ".status.phase" {
                continue;
            }
            if [".status.phase", ".status.reason", ".status.message"]
                .iter()
                .any(|phase| path.starts_with(phase))
            {
                return Err(Failure::from(format!("column {name} reads {path}")));
            }
        }
    }
    Ok(())
}

/// Invariant 10: on v1alpha1, the phase after a round equals `derive` applied to that
/// round's conditions.
///
/// # Errors
///
/// Names the round where the phase disagrees with its derivation.
pub fn phase_derived_from_conditions<F: Fn(&[Value]) -> Option<String>>(
    rounds: &[Observed],
    derive: F,
) -> Result<(), Failure> {
    for (index, round) in rounds.iter().enumerate() {
        let Some(want) = derive(&round.conditions) else {
            continue;
        };
        let got = round.field("phase");
        if got != want {
            return Err(Failure::from(format!(
                "round {index}: phase {got} but the conditions derive {want}"
            )));
        }
    }
    Ok(())
}

/// Invariant 11: once a writer is off, by the round `off_from`, every type in `types`
/// reads `Unknown` `NotEvaluated` and none keeps its last value.
///
/// # Errors
///
/// Names the type that kept a value.
pub fn not_evaluated_once_off(rounds: &[Observed], off_from: usize, types: &[&str]) -> Result<(), Failure> {
    for (index, round) in rounds.iter().enumerate().skip(off_from) {
        for type_ in types {
            match verdict_of(round, type_) {
                Some((status, reason, _)) if status == "Unknown" && reason == "NotEvaluated" => {},
                Some((status, reason, _)) => {
                    return Err(Failure::from(format!(
                        "round {index}: {type_} reads {status} {reason} after its writer stopped\n{}",
                        diff::table(round)
                    )));
                },
                None => {
                    return Err(Failure::from(format!(
                        "round {index}: {type_} is absent after its writer stopped\n{}",
                        diff::table(round)
                    )));
                },
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use serde_json::json;

    use super::*;

    /// An object holding `conditions` as `(type, status, reason)`.
    fn observed(conditions: &[(&str, &str, &str)]) -> Observed {
        Observed {
            generation: 1,
            status: json!({}),
            conditions: conditions
                .iter()
                .map(|(type_, status, reason)| json!({ "type": type_, "status": status, "reason": reason, "message": "" }))
                .collect(),
        }
    }

    #[test]
    fn an_absent_gate_fails_the_rollup() {
        let trio_only = observed(&[
            ("Available", "True", "Available"),
            ("Progressing", "False", "AsExpected"),
            ("Degraded", "False", "AsExpected"),
        ]);
        assert!(
            rollup_present_and_equal(&trio_only, &["Connected"]).is_err(),
            "an absent gate fails"
        );
    }

    #[test]
    fn available_reads_its_own_name_when_every_gate_is_true() {
        let gates = [
            ("Connected", "True", "Connected"),
            ("Progressing", "False", "AsExpected"),
            ("Degraded", "False", "AsExpected"),
        ];
        let mut held = gates.to_vec();
        held.push(("Available", "True", "Available"));
        rollup_present_and_equal(&observed(&held), &["Connected"]).expect("rolls up");
        let mut old = gates.to_vec();
        old.push(("Available", "True", "AsExpected"));
        assert!(
            rollup_present_and_equal(&observed(&old), &["Connected"]).is_err(),
            "Available True reads reason Available, not AsExpected"
        );
    }

    #[test]
    fn available_copies_the_first_gate_that_is_not_true() {
        let held = observed(&[
            ("Accepted", "True", "Accepted"),
            ("ResolvedRefs", "False", "BackendNotFound"),
            ("Available", "False", "BackendNotFound"),
            ("Progressing", "False", "AsExpected"),
            ("Degraded", "False", "AsExpected"),
        ]);
        rollup_present_and_equal(&held, &["Accepted", "ResolvedRefs"]).expect("rolls up");
    }

    /// A CRD whose one version prints `columns`.
    fn crd(columns: &Value) -> Value {
        json!({ "spec": { "versions": [{ "name": "v1alpha1", "additionalPrinterColumns": columns }] } })
    }

    #[test]
    fn a_default_phase_column_fails() {
        let columns = json!([{ "name": "Phase", "jsonPath": ".status.phase" }]);
        assert!(
            printer_columns_read_conditions(&crd(&columns)).is_err(),
            "a default-priority phase column fails"
        );
    }

    #[test]
    fn a_wide_phase_column_passes_and_a_wide_reason_does_not() {
        let phase = json!([{ "name": "Phase", "jsonPath": ".status.phase", "priority": 1 }]);
        printer_columns_read_conditions(&crd(&phase)).expect("a wide phase is derived");
        let reason = json!([{ "name": "Reason", "jsonPath": ".status.reason", "priority": 1 }]);
        assert!(
            printer_columns_read_conditions(&crd(&reason)).is_err(),
            "a reason column fails"
        );
    }
}
