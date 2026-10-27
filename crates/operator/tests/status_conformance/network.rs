//! A `GridNetwork` in Gossip mode without a SWIM runtime, reconciled by
//! `controller::grid_network`, which writes the phase and rolls up the enrollment step's and
//! the renewal loop's conditions.

use std::{collections::BTreeMap, sync::Arc};

use operator::{
    controller::grid_network::{self, OperatorCtx},
    crd::grid_network::{GridNetwork, SignalMode},
};
use status_harness::{FakeApi, invariants};

use crate::fixtures::{self, NETWORK};

/// The gates the enrollment step and the renewal loop write, in roll-up order.
const GATES: [&str; 4] = ["Accepted", "ResolvedRefs", "Enrolled", "IdentityValid"];

/// An API holding the network alone.
fn seeded() -> (FakeApi, String) {
    let api = FakeApi::new();
    let path = fixtures::network_path(NETWORK);
    api.put(&path, fixtures::network(NETWORK));
    (api, path)
}

/// One network reconcile.
async fn reconcile(api: &FakeApi, path: &str) {
    let network: GridNetwork = fixtures::stored(api, path);
    let ctx = OperatorCtx::new(api.client("default"), None, SignalMode::Gossip);
    grid_network::reconcile(Arc::new(network), Arc::new(ctx))
        .await
        .expect("network reconcile succeeds");
}

/// Add every gate `True` to `path`'s status, as the enrollment step and renewal loop write them.
fn gates_true(api: &FakeApi, path: &str) {
    api.edit(path, false, |object| {
        let gates = GATES
            .iter()
            .map(|type_| {
                serde_json::json!({
                    "type": type_, "status": "True", "reason": type_, "message": "",
                    "lastTransitionTime": "2026-10-27T10:00:00Z", "observedGeneration": 1
                })
            })
            .collect::<Vec<_>>();
        let mut held = object
            .pointer("/status/conditions")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        held.extend(gates);
        object["status"]["conditions"] = serde_json::Value::Array(held);
    });
}

/// The field manager each network condition type the reconcile writes belongs to.
fn managers() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        ("Available", grid_network::ROLLUP_FIELD_MANAGER),
        ("Progressing", grid_network::ROLLUP_FIELD_MANAGER),
        ("Degraded", grid_network::ROLLUP_FIELD_MANAGER),
    ])
}

/// The closed reason set of the roll-up, and the gates seeded `True`.
fn reasons() -> Vec<(&'static str, &'static str, &'static str)> {
    let mut allowed: Vec<_> = GATES.iter().map(|type_| (*type_, "True", *type_)).collect();
    allowed.extend([
        ("Available", "Unknown", "AwaitingFirstCheck"),
        ("Available", "True", "Available"),
        ("Progressing", "False", "AsExpected"),
        ("Degraded", "False", "AsExpected"),
    ]);
    allowed
}

#[tokio::test]
async fn invariant_1_pending_network_is_unchanged_across_five_rounds() {
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 5, |_round| reconcile(&api, &path)).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::observed_generation_tracks(&rounds).expect("invariant 3");
    invariants::one_manager_per_type(&api.writes(), &path, &managers()).expect("invariant 5");
    invariants::changes_explained(&rounds, &[]).expect("invariant 6");
    invariants::reasons_within(&rounds, &reasons()).expect("invariant 8");
    assert_eq!(
        rounds.first().expect("five rounds").field("phase"),
        "Pending",
        "a network nothing has enrolled stays Pending"
    );
}

#[tokio::test]
async fn invariant_2_available_follows_the_enrollment_gates_and_moves_only_with_its_status() {
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 5, |round| {
        // Round 2: the enrollment step and the renewal loop have written every gate.
        if round == 2 {
            gates_true(&api, &path);
        }
        reconcile(&api, &path)
    })
    .await;

    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::changes_explained(&rounds, &[2]).expect("invariant 6");
    invariants::one_manager_per_type(&api.writes(), &path, &managers()).expect("invariant 5");
    invariants::reasons_within(&rounds, &reasons()).expect("invariant 8");
    assert_eq!(
        available(&rounds, 1),
        ("Unknown", "AwaitingFirstCheck"),
        "no gate written yet"
    );
    assert_eq!(available(&rounds, 2), ("True", "Available"), "every gate True");
}

/// The `Available` status and reason after `round`.
fn available(rounds: &[status_harness::Observed], round: usize) -> (&str, &str) {
    let condition = rounds
        .get(round)
        .and_then(|observed| observed.condition("Available"))
        .expect("condition held");
    (
        condition["status"].as_str().unwrap_or_default(),
        condition["reason"].as_str().unwrap_or_default(),
    )
}

#[tokio::test]
async fn invariant_3_network_observed_generation_follows_a_spec_edit() {
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 4, |round| {
        if round == 2 {
            api.edit(&path, true, |object| {
                object["spec"]["region"] = serde_json::json!("eu");
            });
        }
        reconcile(&api, &path)
    })
    .await;

    invariants::observed_generation_tracks(&rounds).expect("invariant 3");
    assert_eq!(
        rounds.get(3).expect("four rounds").generation,
        2,
        "the spec edit bumps the generation"
    );
}

#[tokio::test]
async fn invariant_7_network_rollup_trio_is_present_after_round_one() {
    let (api, path) = seeded();
    gates_true(&api, &path);
    let rounds = fixtures::rounds(&api, &path, 1, |_round| reconcile(&api, &path)).await;
    invariants::rollup_present_and_equal(rounds.first().expect("one round"), &GATES).expect("invariant 7");
}
