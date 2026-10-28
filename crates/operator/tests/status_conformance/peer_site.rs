//! A peer `GridSite`, reconciled by `controller::grid_site`.
//!
//! The phase stays a merge patch with no manager. `Connected` and the roll-up are applied
//! under their own managers.

use std::{collections::BTreeMap, sync::Arc};

use kube::api::{Patch, PatchParams};
use operator::{
    cli::Membership,
    controller::grid_site,
    crd::grid_site::GridSite,
    peer_reach::{CATALOGS, REACH},
};
use serde_json::json;
use status_harness::{FakeApi, invariants};

use crate::fixtures::{self, NETWORK};

/// An API holding the network and a declared peer site named `site-b`.
fn seeded() -> (FakeApi, String) {
    let api = FakeApi::new();
    api.put(&fixtures::network_path(NETWORK), fixtures::network(NETWORK));
    let path = fixtures::site_path("site-b");
    api.put(&path, fixtures::site("site-b", NETWORK, &[]));
    (api, path)
}

/// One reconcile of the site at `path` under `membership`.
async fn reconcile_under(api: &FakeApi, path: &str, membership: Membership) {
    let site: GridSite = fixtures::stored(api, path);
    let ctx = Arc::new(grid_site::SiteContext {
        client: Arc::new(api.client("default")),
        membership,
    });
    Box::pin(grid_site::reconcile(Arc::new(site), ctx))
        .await
        .expect("peer site reconcile succeeds");
}

/// One reconcile of the site at `path` under Swim membership.
async fn reconcile(api: &FakeApi, path: &str) {
    reconcile_under(api, path, Membership::Swim).await;
}

#[tokio::test]
async fn invariant_1_pending_peer_site_is_unchanged_across_five_rounds() {
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 5, |_round| reconcile(&api, &path)).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::observed_generation_tracks(&rounds, &[]).expect("invariant 3");
    invariants::changes_explained(&rounds, &[]).expect("invariant 6");
    let first = rounds.first().expect("five rounds");
    status_harness::diff::status_matches(
        first,
        &[
            ("phase", "Pending", "AwaitingDiscovery"),
            ("Connected", "False", "AwaitingDiscovery"),
            ("Available", "False", "AwaitingDiscovery"),
            ("Progressing", "False", "AsExpected"),
            ("Degraded", "False", "AsExpected"),
        ],
    )
    .expect("invariant 8");
}

#[tokio::test]
async fn invariant_2_peer_site_transition_time_moves_only_with_the_phase() {
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 5, |round| {
        // Round 3: discovery advances the site, as the GridNetwork controller does from gossip.
        if round == 2 {
            api.edit(&path, false, |object| {
                object["status"]["phase"] = json!("Discovered");
            });
        }
        reconcile(&api, &path)
    })
    .await;

    invariants::phase_transition_time_moves_only_with_phase(&rounds).expect("invariant 2");
    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::changes_explained(&rounds, &[2]).expect("invariant 6");
    invariants::unchanged_from(&rounds, 3).expect("invariant 1 after the change");
    let after = rounds.get(2).expect("five rounds");
    status_harness::diff::status_matches(
        after,
        &[
            ("phase", "Discovered", "GatewayAddressMissing"),
            ("Connected", "False", "AddressMissing"),
            ("Available", "False", "AddressMissing"),
            ("Progressing", "False", "AsExpected"),
            ("Degraded", "False", "AsExpected"),
        ],
    )
    .expect("invariant 8");
}

#[tokio::test]
async fn invariant_3_peer_site_observed_generation_follows_a_spec_edit() {
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 4, |round| {
        if round == 2 {
            api.edit(&path, true, |object| {
                object["spec"]["region"] = json!("eu");
            });
        }
        reconcile(&api, &path)
    })
    .await;

    invariants::observed_generation_tracks(&rounds, &[]).expect("invariant 3");
    assert_eq!(
        rounds.get(3).expect("four rounds").generation,
        2,
        "the spec edit bumps the generation"
    );
}

#[tokio::test]
async fn invariant_5_peer_site_conditions_have_one_manager_each() {
    // The phase stays an unmanaged merge patch; each condition type has one manager.
    let (api, path) = seeded();
    drop(fixtures::rounds(&api, &path, 2, |_round| reconcile(&api, &path)).await);

    let managers = BTreeMap::from([
        ("Connected", "grid-operator-probe"),
        ("Available", "grid-operator-site"),
        ("Progressing", "grid-operator-site"),
        ("Degraded", "grid-operator-site"),
    ]);
    invariants::one_manager_per_type(&api.writes(), &path, &managers).expect("invariant 5");
    let phase_writes: Vec<_> = api
        .writes()
        .into_iter()
        .filter(|write| write.path == path && write.status && write.body.pointer("/status/phase").is_some())
        .collect();
    assert!(
        !phase_writes.is_empty()
            && phase_writes
                .iter()
                .all(|write| write.manager.is_none() && write.content_type.starts_with("application/merge-patch")),
        "the phase is an unmanaged merge patch: {phase_writes:?}"
    );
}

#[tokio::test]
async fn invariant_7_peer_site_rollup_trio_is_present_after_round_one() {
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 1, |_round| reconcile(&api, &path)).await;
    invariants::rollup_present_and_equal(rounds.first().expect("one round"), &["Connected"]).expect("invariant 7");
}

/// Apply `conditions` on `site-b` under `manager`, as the peer poller does: an empty list
/// gives up what the manager held.
async fn poller_applies(api: &FakeApi, manager: &str, conditions: serde_json::Value) {
    let sites: kube::Api<GridSite> = kube::Api::all(api.client("default"));
    let patch = json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridSite",
        "metadata": { "name": "site-b" },
        "status": { "conditions": conditions }
    });
    sites
        .patch_status("site-b", &PatchParams::apply(manager).force(), &Patch::Apply(patch))
        .await
        .expect("poller apply succeeds");
}

/// A False peer verdict of `type_` with `reason`.
fn refused(type_: &str, reason: &str) -> serde_json::Value {
    json!([{
        "type": type_, "status": "False", "reason": reason, "message": "the poller says so",
        "lastTransitionTime": "2026-10-26T12:00:00Z", "observedGeneration": 1
    }])
}

/// `type_` on the site at `path`, as (status, reason, message).
fn reads(api: &FakeApi, path: &str, type_: &str) -> (String, String, String) {
    let observed = fixtures::observe(api, path);
    let condition = observed.condition(type_).cloned().unwrap_or_default();
    let field = |key: &str| condition[key].as_str().unwrap_or_default().to_owned();
    (field("status"), field("reason"), field("message"))
}

/// `Degraded` on the site at `path`, as (status, reason).
fn degraded(api: &FakeApi, path: &str) -> (String, String) {
    let (status, reason, _message) = reads(api, path, "Degraded");
    (status, reason)
}

/// The peer with both verdicts False, reconciled once after each write under Local.
async fn refused_peer() -> (FakeApi, String) {
    let (api, path) = seeded();
    reconcile_under(&api, &path, Membership::Local).await;
    poller_applies(&api, REACH.field_manager, refused(REACH.site_type, "Timeout")).await;
    reconcile_under(&api, &path, Membership::Local).await;
    poller_applies(&api, CATALOGS.field_manager, refused(CATALOGS.site_type, "OwnCluster")).await;
    reconcile_under(&api, &path, Membership::Local).await;
    (api, path)
}

#[tokio::test]
async fn a_peer_poller_verdict_degrades_the_peer_on_the_next_reconcile() {
    let (api, path) = seeded();
    reconcile_under(&api, &path, Membership::Local).await;
    let available = reads(&api, &path, "Available");
    poller_applies(&api, REACH.field_manager, refused(REACH.site_type, "Timeout")).await;
    reconcile_under(&api, &path, Membership::Local).await;

    let (status, reason, message) = reads(&api, &path, "Degraded");
    assert_eq!((status.as_str(), reason.as_str()), ("True", "Timeout"));
    assert!(
        message.starts_with("SignalsReachable: "),
        "the message names the source: {message}"
    );
    assert_eq!(reads(&api, &path, "Available"), available, "a peer concern never gates");
}

#[tokio::test]
async fn signals_degrade_before_the_catalog_and_clearing_both_restores_the_peer() {
    let (api, path) = refused_peer().await;
    assert_eq!(degraded(&api, &path).1, "Timeout", "signals come before the catalog");

    // Swim clears both, and the next reconcile returns Degraded to False.
    poller_applies(&api, REACH.field_manager, json!([])).await;
    poller_applies(&api, CATALOGS.field_manager, json!([])).await;
    reconcile_under(&api, &path, Membership::Swim).await;
    assert_eq!(degraded(&api, &path), ("False".to_owned(), "AsExpected".to_owned()));

    let managers = BTreeMap::from([
        ("Connected", "grid-operator-probe"),
        ("Available", "grid-operator-site"),
        ("Progressing", "grid-operator-site"),
        ("Degraded", "grid-operator-site"),
        ("SignalsReachable", REACH.field_manager),
        ("CatalogAccepted", CATALOGS.field_manager),
    ]);
    invariants::one_manager_per_type(&api.writes(), &path, &managers).expect("invariant 5");
}
