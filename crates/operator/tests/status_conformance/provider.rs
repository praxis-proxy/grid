//! An `InferenceProvider`, reconciled by `controller::inference_provider`, with its
//! `Ready` and `MetricsAvailable` conditions written by the signals pass in Poll mode.

use std::{collections::BTreeMap, sync::Arc};

use operator::{
    controller::{
        grid_network::{self, OperatorCtx},
        inference_provider::{self, ProviderCtx},
    },
    crd::{grid_network::SignalMode, inference_provider::InferenceProvider},
};
use status_harness::{FakeApi, invariants};

use crate::fixtures::{self, NETWORK};

/// The site this operator runs as.
const LOCAL_SITE: &str = "site-a";

/// An API holding the network, the local site and a provider named `prov`.
fn seeded() -> (FakeApi, String) {
    let api = FakeApi::new();
    api.put(&fixtures::network_path(NETWORK), fixtures::network(NETWORK));
    api.put(
        &fixtures::site_path(LOCAL_SITE),
        fixtures::site(LOCAL_SITE, NETWORK, &[("grid.praxis.fast/provider-site", LOCAL_SITE)]),
    );
    let path = fixtures::provider_path("prov");
    api.put(&path, fixtures::provider("prov", NETWORK));
    (api, path)
}

/// One provider reconcile at `path`.
async fn reconcile(api: &FakeApi, path: &str) {
    let provider: InferenceProvider = fixtures::stored(api, path);
    let ctx = Arc::new(ProviderCtx {
        client: api.client("default"),
        local_site: Some(LOCAL_SITE.to_owned()),
    });
    inference_provider::reconcile(Arc::new(provider), ctx)
        .await
        .expect("provider reconcile succeeds");
}

/// Serializes the signals passes: the readiness write slot is one per process, and a
/// pass that finds it held skips its writes.
static PASS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// One signals pass in Poll mode, the only writer of provider conditions, awaited.
async fn signals_pass(api: &FakeApi, path: &str) {
    let _pass = PASS.lock().await;
    let client = api.client("default");
    let ctx = OperatorCtx::new(client.clone(), None, SignalMode::Poll);
    grid_network::refresh_signals(&ctx, &client, NETWORK)
        .await
        .expect("signals pass succeeds");
    fixtures::until_condition(api, path, "Ready").await;
}

/// The field manager each provider condition type belongs to.
fn managers() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        ("Ready", "grid-operator-readiness"),
        ("MetricsAvailable", "grid-operator-readiness"),
    ])
}

#[tokio::test]
async fn invariant_1_available_provider_is_unchanged_across_five_rounds() {
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 5, |_round| reconcile(&api, &path)).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::observed_generation_tracks(&rounds).expect("invariant 3");
    invariants::changes_explained(&rounds, &[]).expect("invariant 6");
    let first = rounds.first().expect("five rounds");
    assert_eq!(first.field("phase"), "Available");
    assert_eq!(first.field("reason"), "", "Available carries no reason");
    assert_eq!(first.status["matchingSites"], serde_json::json!([LOCAL_SITE]));
}

#[tokio::test]
async fn invariant_5_provider_conditions_belong_to_the_readiness_manager_alone() {
    let (api, path) = seeded();
    signals_pass(&api, &path).await;
    let rounds = fixtures::rounds(&api, &path, 3, |_round| reconcile(&api, &path)).await;

    invariants::one_manager_per_type(&api.writes(), &path, &managers()).expect("invariant 5");
    let last = rounds.last().expect("three rounds");
    assert!(
        last.condition("Ready").is_some() && last.condition("MetricsAvailable").is_some(),
        "the reconciler's apply leaves the readiness manager's conditions in place: {:?}",
        last.conditions
    );
    assert_eq!(
        api.owners(&path).get("Ready").map(String::as_str),
        Some("grid-operator-readiness")
    );
}

#[tokio::test]
async fn invariant_1_provider_conditions_are_unchanged_across_repeated_signals_passes() {
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 4, |_round| async {
        signals_pass(&api, &path).await;
        reconcile(&api, &path).await;
    })
    .await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::observed_generation_tracks(&rounds).expect("invariant 3");
    invariants::reasons_within(
        &rounds,
        &[
            ("Ready", "Unknown", "MetricsNotConfigured"),
            ("MetricsAvailable", "False", "MetricsNotConfigured"),
        ],
    )
    .expect("invariant 8");
}

#[tokio::test]
// Expected to fail: Ready reads Unknown MetricsNotConfigured, an Unknown that is neither AwaitingFirst nor
// NotEvaluated (readiness.rs:124); STATUS-REVIEW ratifies MetricsReachable absent without an endpoint
async fn invariant_4_provider_ready_is_unknown_only_before_its_first_evaluation() {
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 2, |_round| async {
        signals_pass(&api, &path).await;
        reconcile(&api, &path).await;
    })
    .await;
    invariants::unknown_only_before_first_evaluation(&rounds).expect("invariant 4");
}

#[tokio::test]
// Expected to fail: only the Poll-mode signals pass writes provider conditions (main.rs:2503-2505), so a provider
// keeps Ready Unknown MetricsNotConfigured for good once the mode turns Gossip; STATUS-REVIEW rule 9 wants Unknown
// NotEvaluated by the next round
async fn invariant_11_provider_conditions_read_not_evaluated_once_gossip_turns_the_writer_off() {
    let (api, path) = seeded();
    signals_pass(&api, &path).await;
    let rounds = fixtures::rounds(&api, &path, 3, |_round| async {
        let client = api.client("default");
        let ctx = Arc::new(OperatorCtx::new(client, None, SignalMode::Gossip));
        let network = fixtures::stored(&api, &fixtures::network_path(NETWORK));
        grid_network::reconcile(Arc::new(network), ctx)
            .await
            .expect("network reconcile succeeds");
        reconcile(&api, &path).await;
    })
    .await;
    invariants::not_evaluated_once_off(&rounds, 1, &["Ready", "MetricsAvailable"]).expect("invariant 11");
}

#[tokio::test]
// Expected to fail: an InferenceProvider writes no Available, Progressing or Degraded; STATUS-REVIEW part 3 adds the
// trio on v1alpha1
async fn invariant_7_provider_rollup_trio_is_present_after_round_one() {
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 1, |_round| reconcile(&api, &path)).await;
    invariants::rollup_present_and_equal(rounds.first().expect("one round"), &["EndpointLive", "Programmed"])
        .expect("invariant 7");
}
