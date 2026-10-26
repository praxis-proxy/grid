//! A `GridNetwork` in Gossip mode without a SWIM runtime, reconciled by
//! `controller::grid_network`, which writes the phase and the `LocalSiteDeclared`
//! condition.

use std::{collections::BTreeMap, sync::Arc};

use operator::{
    controller::grid_network::{self, OperatorCtx},
    crd::grid_network::{GridNetwork, SignalMode},
};
use status_harness::{FakeApi, invariants};

use crate::fixtures::{self, NETWORK};

/// The local site, once the operator knows its name.
const LOCAL_SITE: &str = "site-a";

/// An API holding the network alone.
fn seeded() -> (FakeApi, String) {
    let api = FakeApi::new();
    let path = fixtures::network_path(NETWORK);
    api.put(&path, fixtures::network(NETWORK));
    (api, path)
}

/// One network reconcile, run as `site_name`.
async fn reconcile(api: &FakeApi, path: &str, site_name: Option<&str>) {
    let network: GridNetwork = fixtures::stored(api, path);
    let ctx =
        OperatorCtx::new(api.client("default"), None, SignalMode::Gossip).with_site_name(site_name.map(str::to_owned));
    grid_network::reconcile(Arc::new(network), Arc::new(ctx))
        .await
        .expect("network reconcile succeeds");
}

/// The field manager each network condition type belongs to.
fn managers() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        ("LocalSiteDeclared", "grid-operator-local-site"),
        ("Available", "grid-operator"),
        ("Progressing", "grid-operator"),
        ("Degraded", "grid-operator"),
    ])
}

/// The closed reason set: each `LocalSiteDeclared` verdict, and `Available` copying it.
fn reasons() -> Vec<(&'static str, &'static str, &'static str)> {
    let declared = [
        ("False", "NoSiteIdentity"),
        ("False", "LocalSiteMissing"),
        ("False", "LocalSiteUnlabelled"),
        ("True", "LocalSiteFound"),
    ];
    let mut allowed: Vec<_> = declared
        .iter()
        .map(|(status, reason)| ("LocalSiteDeclared", *status, *reason))
        .chain(
            declared
                .iter()
                .filter(|(status, _)| *status == "False")
                .map(|(status, reason)| ("Available", *status, *reason)),
        )
        .collect();
    allowed.extend([
        ("Available", "True", "Available"),
        ("Progressing", "False", "AsExpected"),
        ("Degraded", "False", "AsExpected"),
    ]);
    allowed
}

#[tokio::test]
async fn invariant_1_pending_network_is_unchanged_across_five_rounds() {
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 5, |_round| reconcile(&api, &path, None)).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::observed_generation_tracks(&rounds).expect("invariant 3");
    invariants::one_manager_per_type(&api.writes(), &path, &managers()).expect("invariant 5");
    invariants::changes_explained(&rounds, &[]).expect("invariant 6");
    invariants::reasons_within(&rounds, &reasons()).expect("invariant 8");
    assert_eq!(
        rounds.first().expect("five rounds").field("phase"),
        "Pending",
        "a network with no local site stays Pending"
    );
}

#[tokio::test]
async fn invariant_2_local_site_declared_transition_time_moves_only_with_its_status() {
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 5, |round| {
        // Round 3: the operator learns its name. Round 4: the labelled GridSite appears.
        if round == 3 {
            api.put(
                &fixtures::site_path(LOCAL_SITE),
                fixtures::site(LOCAL_SITE, NETWORK, &[("grid.praxis.fast/provider-site", LOCAL_SITE)]),
            );
        }
        reconcile(&api, &path, (round >= 2).then_some(LOCAL_SITE))
    })
    .await;

    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::changes_explained(&rounds, &[2, 3]).expect("invariant 6");
    invariants::one_manager_per_type(&api.writes(), &path, &managers()).expect("invariant 5");
    invariants::reasons_within(&rounds, &reasons()).expect("invariant 8");
    assert_eq!(declared(&rounds, 1), ("False", "NoSiteIdentity"), "no identity yet");
    assert_eq!(
        declared(&rounds, 2),
        ("False", "LocalSiteMissing"),
        "identity, no GridSite"
    );
    assert_eq!(
        declared(&rounds, 4),
        ("True", "LocalSiteFound"),
        "the GridSite is declared"
    );
}

/// The `LocalSiteDeclared` status and reason after `round`.
fn declared(rounds: &[status_harness::Observed], round: usize) -> (&str, &str) {
    let condition = rounds
        .get(round)
        .and_then(|observed| observed.condition("LocalSiteDeclared"))
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
        reconcile(&api, &path, None)
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
    let rounds = fixtures::rounds(&api, &path, 1, |_round| reconcile(&api, &path, None)).await;
    invariants::rollup_present_and_equal(rounds.first().expect("one round"), &["LocalSiteDeclared"])
        .expect("invariant 7");
}
