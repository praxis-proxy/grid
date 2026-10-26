//! Status conformance for `SiteInvite`, over the invariants in `status_harness::invariants`.
//!
//! The reconciler is private to the parent module, so the cases live beside it.

#![expect(clippy::expect_used, clippy::indexing_slicing, reason = "tests")]

use std::{collections::BTreeMap, sync::Arc};

use k8s_openapi::api::core::v1::Secret;
use kube::{Api, runtime::controller::Action};
use site_invite_api::SiteInvite;
use status_harness::{FakeApi, Observed, invariants};

use super::{Context, reconcile};
use crate::{AppState, GridAdmins, SharedCa, auth::digest, authz::Authorizer, store::Store};

/// The namespace invite Secrets land in.
const NAMESPACE: &str = "grid-enrollment";

/// The invite's path.
const PATH: &str = "/apis/grid.praxis.fast/v1alpha1/siteinvites/site-d";

/// The path of the Secret the invite checkpoints its token in.
const SECRET: &str = "/api/v1/namespaces/grid-enrollment/secrets/grid-invite-site-d";

/// A memory-backed enrollment service and an API holding one invite for `site-d`.
fn seeded() -> (FakeApi, Arc<Context>) {
    let api = FakeApi::new();
    api.put(
        PATH,
        serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "SiteInvite",
            "metadata": { "name": "site-d", "generation": 1, "uid": "uid-site-d" },
            "spec": { "siteName": "site-d", "jurisdictions": [], "lifetime": "1h" }
        }),
    );
    let ca = certs::generate_ca("grid-ca").expect("ca");
    let state = Arc::new(AppState {
        store: Store::memory(),
        ca: SharedCa::new(ca),
        authorizer: Authorizer::Local(GridAdmins::from_table("admin: good-admin\n").expect("token table")),
        cert_lifetime: certs::DEFAULT_SITE_CERT_LIFETIME,
        reserved_sites: Vec::new(),
        renewals_enabled: true,
        #[cfg(feature = "csr-record")]
        recorder: None,
    });
    let ctx = Arc::new(Context {
        client: api.client(NAMESPACE),
        state,
        namespace: NAMESPACE.to_owned(),
    });
    (api, ctx)
}

/// One reconcile of the stored invite.
async fn round(api: &FakeApi, ctx: &Arc<Context>) -> Action {
    let invite: SiteInvite = serde_json::from_value(api.get(PATH).expect("invite stored")).expect("invite parses");
    reconcile(Arc::new(invite), Arc::clone(ctx))
        .await
        .expect("invite reconcile succeeds")
}

/// Run `count` rounds, observing the invite after each.
async fn rounds(api: &FakeApi, ctx: &Arc<Context>, count: usize) -> Vec<Observed> {
    let mut observed = Vec::with_capacity(count);
    for _round in 0..count {
        let _action = round(api, ctx).await;
        observed.push(api.observe(PATH).expect("invite still stored"));
    }
    observed
}

/// The field manager each invite condition type belongs to.
fn managers() -> BTreeMap<&'static str, &'static str> {
    [
        "Minted",
        "Redeemed",
        "Issued",
        "Available",
        "Progressing",
        "Degraded",
        "Ready",
    ]
    .into_iter()
    .map(|type_| (type_, "grid-enrollment"))
    .collect()
}

/// Redeem the minted token in the store, as an enrollment would.
async fn redeem(api: &FakeApi, ctx: &Context) {
    let secret = Api::<Secret>::namespaced(ctx.client.clone(), NAMESPACE)
        .get("grid-invite-site-d")
        .await
        .expect("checkpoint Secret");
    let token = secret
        .data
        .as_ref()
        .and_then(|data| data.get(super::SECRET_KEY))
        .expect("token");
    let token = std::str::from_utf8(&token.0).expect("token is text").to_owned();
    let ca = certs::generate_ca("grid-ca").expect("ca");
    let leaf = certs::generate_dns_cert(&ca, "site-d", "site-d").expect("leaf");
    ctx.state
        .store
        .redeem_and_issue(&digest(&token), |pin| {
            Ok(crate::store::Issued {
                site_name: pin.site_name.clone(),
                certificate: leaf.cert_pem,
                spiffe_id: "spiffe://grid/site-d".to_owned(),
                public_key_sha256: "0".repeat(64),
            })
        })
        .await
        .expect("token redeems");
    assert!(
        api.get(SECRET).is_some(),
        "the checkpoint Secret stays until the controller reads the redemption"
    );
}

#[tokio::test]
async fn invariant_1_ready_invite_is_unchanged_across_five_rounds() {
    let (api, ctx) = seeded();
    // Round 1 adds the finalizer and returns; the status starts in round 2.
    let rounds = rounds(&api, &ctx, 6).await;
    let since_ready = rounds.get(1..).expect("six rounds");

    invariants::unchanged_from(since_ready, 1).expect("invariant 1");
    invariants::transition_time_moves_only_with_status(since_ready).expect("invariant 2");
    invariants::observed_generation_tracks(since_ready).expect("invariant 3");
    invariants::unknown_only_before_first_evaluation(since_ready).expect("invariant 4");
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    invariants::changes_explained(since_ready, &[]).expect("invariant 6");
    invariants::reasons_within(
        since_ready,
        &[
            ("Minted", "True", "Minted"),
            ("Redeemed", "False", "AwaitingRequest"),
            ("Issued", "False", "AwaitingRequest"),
            ("Available", "True", "Available"),
            ("Progressing", "False", "AsExpected"),
            ("Degraded", "False", "AsExpected"),
            ("Ready", "True", "Ready"),
        ],
    )
    .expect("invariant 8");
    invariants::rollup_present_and_equal(&since_ready[0], &["Minted"]).expect("invariant 7");
    let ready = since_ready[0].condition("Ready").expect("Ready after round 2");
    assert_eq!(ready["status"], "True", "a minted invite reads Ready True");
    assert_eq!(ready["reason"], "Ready", "Ready keeps its shipped reason");
    assert!(api.get(SECRET).is_some(), "the token Secret is checkpointed");
}

#[tokio::test]
async fn a_ready_invite_does_not_pass_through_pending_on_a_requeue() {
    let (api, ctx) = seeded();
    drop(rounds(&api, &ctx, 6).await);
    let minting = api.writes().iter().filter(|write| writes_minting(write)).count();
    assert_eq!(
        minting, 1,
        "only the first mint writes Minting, not each requeue of a Ready invite"
    );
}

#[tokio::test]
async fn invariant_6_redemption_changes_ready_in_the_round_it_is_observed() {
    let (api, ctx) = seeded();
    let mut rounds = rounds(&api, &ctx, 3).await;
    redeem(&api, &ctx).await;
    rounds.extend(rounds_more(&api, &ctx, 3).await);
    let since_ready = rounds.get(1..).expect("six rounds");

    // The redemption is the input change before the fourth observed round.
    invariants::changes_explained(since_ready, &[2]).expect("invariant 6");
    invariants::unchanged_from(since_ready, 3).expect("invariant 1 after redemption");
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    let redeemed = since_ready[2].condition("Ready").expect("Ready after redemption");
    assert_eq!(
        redeemed["reason"], "Redeemed",
        "Ready names the redemption in the round it is seen"
    );
    assert!(api.get(SECRET).is_none(), "a redeemed invite drops its token Secret");
}

#[tokio::test]
async fn invariant_2_redemption_moves_each_transition_time_only_with_its_status() {
    let (api, ctx) = seeded();
    let mut rounds = rounds(&api, &ctx, 3).await;
    redeem(&api, &ctx).await;
    rounds.extend(rounds_more(&api, &ctx, 2).await);
    invariants::transition_time_moves_only_with_status(rounds.get(1..).expect("five rounds")).expect("invariant 2");
}

/// More rounds on an invite that already holds a status.
async fn rounds_more(api: &FakeApi, ctx: &Arc<Context>, count: usize) -> Vec<Observed> {
    rounds(api, ctx, count).await
}

#[tokio::test]
async fn invariant_7_redeemed_invite_reads_available_false_with_redeemed() {
    let (api, ctx) = seeded();
    drop(rounds(&api, &ctx, 2).await);
    redeem(&api, &ctx).await;
    let after = rounds(&api, &ctx, 1).await;
    let observed = after.first().expect("one round");

    // Once spent, Available is False Redeemed, the facts stay True, and Ready mirrors Available.
    status_harness::diff::status_matches(
        observed,
        &[
            ("Minted", "True", "Minted"),
            ("Redeemed", "True", "Redeemed"),
            ("Issued", "True", "Issued"),
            ("Available", "False", "Redeemed"),
            ("Progressing", "False", "AsExpected"),
            ("Degraded", "False", "AsExpected"),
            ("Ready", "False", "Redeemed"),
        ],
    )
    .expect("a redeemed invite");
}

/// Whether `write` carries a condition reading `Minting`.
fn writes_minting(write: &status_harness::Write) -> bool {
    write
        .body
        .pointer("/status/conditions")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|conditions| conditions.iter().any(|condition| condition["reason"] == "Minting"))
}

/// Mint another live token pinning `site-d`, so the invite's own mint is refused.
async fn pin_site_d(ctx: &Context) {
    ctx.state
        .store
        .mint_site_token(crate::store::NewSiteToken {
            token_id: None,
            token_sha256: "1".repeat(64),
            public_key_sha256: None,
            site_name: "site-d".to_owned(),
            grid_network_ref: "site-invite".to_owned(),
            issued_by: "test".to_owned(),
            expires_at: time::OffsetDateTime::now_utc().saturating_add(time::Duration::hours(1)),
            allow_deleted_name: false,
        })
        .await
        .expect("the first token mints");
}

#[tokio::test]
async fn a_refused_mint_reads_minted_false_with_the_signers_reason() {
    let (api, ctx) = seeded();
    pin_site_d(&ctx).await;
    let mut rounds = Vec::new();
    for round in 0..4 {
        let invite: SiteInvite = serde_json::from_value(api.get(PATH).expect("invite stored")).expect("invite parses");
        let result = reconcile(Arc::new(invite), Arc::clone(&ctx)).await;
        // Round 1 adds the finalizer; every later round must surface the refusal, not swallow it.
        assert_eq!(result.is_err(), round > 0, "round {round}: {result:?}");
        rounds.push(api.observe(PATH).expect("invite stored"));
    }
    // Round 1 adds the finalizer; each retry after the refusal writes nothing new.
    invariants::unchanged_from(&rounds[1..], 1).expect("invariant 1");
    let pending_writes = api.writes().iter().filter(|write| writes_minting(write)).count();
    assert_eq!(pending_writes, 1, "only the first attempt writes Minting");
    let observed = rounds.last().expect("four rounds");
    status_harness::diff::status_matches(
        observed,
        &[
            ("Minted", "False", "TokenOutstanding"),
            ("Redeemed", "False", "AwaitingRequest"),
            ("Issued", "False", "AwaitingRequest"),
            ("Available", "False", "TokenOutstanding"),
            ("Progressing", "False", "AsExpected"),
            ("Degraded", "False", "AsExpected"),
            ("Ready", "False", "Pending"),
        ],
    )
    .expect("a refused mint");
}

#[tokio::test]
async fn a_redeemed_invite_is_terminal_and_writes_nothing_more() {
    let (api, ctx) = seeded();
    drop(rounds(&api, &ctx, 2).await);
    redeem(&api, &ctx).await;
    drop(rounds(&api, &ctx, 1).await);
    let written = api.writes().len();
    drop(rounds(&api, &ctx, 3).await);
    assert_eq!(api.writes().len(), written, "a redeemed invite is not reconciled again");
}
