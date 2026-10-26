//! An `InferenceProvider`, reconciled by `controller::inference_provider`, which writes
//! `Accepted`, `ResolvedRefs` and the roll-up, with `Ready`, `MetricsAvailable` and
//! `EndpointLive` written by the signals pass.

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
        ("EndpointLive", "grid-operator-readiness"),
        ("MetricsReachable", "grid-operator-readiness"),
        ("Accepted", "grid-operator"),
        ("ResolvedRefs", "grid-operator"),
        ("Available", "grid-operator"),
        ("Progressing", "grid-operator"),
        ("Degraded", "grid-operator"),
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
    assert_eq!(first.field("phase"), "Available", "a resolved provider is Available");
    assert_eq!(first.field("reason"), "", "Available carries no reason");
    assert_eq!(
        first.status["matchingSites"],
        serde_json::json!([LOCAL_SITE]),
        "the provider matches the local site"
    );
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
        Some("grid-operator-readiness"),
        "Ready stays with the readiness manager"
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
            ("Ready", "Unknown", "NotEvaluated"),
            ("MetricsAvailable", "Unknown", "NotEvaluated"),
            ("EndpointLive", "True", "NoHealthSignal"),
            ("Accepted", "True", "Accepted"),
            ("ResolvedRefs", "True", "ResolvedRefs"),
            ("Available", "True", "Available"),
            ("Progressing", "False", "AsExpected"),
            ("Degraded", "False", "AsExpected"),
        ],
    )
    .expect("invariant 8");
}

#[tokio::test]
async fn invariant_4_provider_ready_is_unknown_only_before_its_first_evaluation() {
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 2, |_round| async {
        signals_pass(&api, &path).await;
        reconcile(&api, &path).await;
    })
    .await;
    invariants::unknown_only_before_first_evaluation(&rounds).expect("invariant 4");
}

/// A provider named `prov` whose metrics endpoint refuses every scrape.
fn seeded_with_metrics() -> (FakeApi, String) {
    let (api, path) = seeded();
    api.edit(&path, false, |object| {
        object["spec"]["metricsConfig"] = serde_json::json!({ "endpoint": "http://127.0.0.1:1" });
    });
    (api, path)
}

/// Signals passes in `mode` until `type_` reads `reason`, or fail after a bounded wait.
///
/// A pass that finds the write slot held skips its writes, so the pass is retried.
async fn signals_pass_until(api: &FakeApi, path: &str, mode: SignalMode, type_: &str, reason: &str) {
    for _attempt in 0..50 {
        {
            let _pass = PASS.lock().await;
            let client = api.client("default");
            let ctx = OperatorCtx::new(client.clone(), None, mode);
            grid_network::refresh_signals(&ctx, &client, NETWORK)
                .await
                .expect("signals pass succeeds");
        }
        for _poll in 0..10 {
            if fixtures::reason_of(api, path, type_) == reason {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
    panic!("{type_} never read {reason}");
}

/// One Gossip-mode round: round 1 removes the metrics endpoint, then a readiness pass and a
/// reconcile run.
async fn gossip_round(api: &FakeApi, path: &str, round: usize) {
    if round == 1 {
        api.edit(path, true, |object| {
            if let Some(spec) = object["spec"].as_object_mut() {
                spec.remove("metricsConfig");
            }
        });
    }
    let want = if round == 0 {
        "AwaitingFirstScrape"
    } else {
        "NotEvaluated"
    };
    signals_pass_until(api, path, SignalMode::Gossip, "Ready", want).await;
    reconcile(api, path).await;
}

#[tokio::test]
async fn invariant_11_provider_conditions_read_not_evaluated_once_metrics_endpoint_is_removed() {
    // Gossip mode: the readiness pass runs in every signal mode.
    let (api, path) = seeded_with_metrics();
    let rounds = fixtures::rounds(&api, &path, 3, |round| gossip_round(&api, &path, round)).await;
    let first = rounds.first().expect("three rounds");
    assert!(
        first.condition("MetricsReachable").is_some(),
        "a metrics endpoint is scraped in Gossip mode"
    );
    invariants::not_evaluated_once_off(&rounds, 1, &["Ready", "MetricsAvailable"]).expect("invariant 11");
    assert!(
        rounds
            .iter()
            .skip(1)
            .all(|round| round.condition("MetricsReachable").is_none()),
        "MetricsReachable is absent without a metrics endpoint"
    );
}

#[tokio::test]
async fn invariant_7_provider_rollup_trio_is_present_after_round_one() {
    // Programmed joins the gates when model discovery writes it.
    let (api, path) = seeded();
    let rounds = fixtures::rounds(&api, &path, 1, |_round| async {
        signals_pass(&api, &path).await;
        reconcile(&api, &path).await;
    })
    .await;
    invariants::rollup_present_and_equal(
        rounds.first().expect("one round"),
        &["Accepted", "ResolvedRefs", "EndpointLive"],
    )
    .expect("invariant 7");
}
