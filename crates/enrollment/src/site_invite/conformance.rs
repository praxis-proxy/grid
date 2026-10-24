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
    BTreeMap::from([("Ready", "grid-enrollment")])
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
        &[("Ready", "False", "Pending"), ("Ready", "True", "Ready")],
    )
    .expect("invariant 8");
    let ready = since_ready[0].condition("Ready").expect("Ready after round 2");
    assert_eq!(ready["status"], "True");
    assert_eq!(ready["reason"], "Ready");
    assert!(api.get(SECRET).is_some(), "the token Secret is checkpointed");
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
    assert_eq!(redeemed["reason"], "Redeemed");
    assert!(api.get(SECRET).is_none(), "a redeemed invite drops its token Secret");
}

#[tokio::test]
// Expected to fail: invite_status keeps lastTransitionTime only when status and reason both match (site_invite.rs
// prior_transition), so Ready True Ready to Ready True Redeemed moves the time with no status change
async fn invariant_2_redemption_keeps_the_transition_time_while_ready_stays_true() {
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
// Expected to fail: a redeemed SiteInvite reads Ready True Redeemed (site_invite.rs invite_status), so the Ready
// printer column shows True for a spent invite; STATUS-REVIEW ratifies Available False Redeemed with Minted, Redeemed
// and Issued beside it
async fn invariant_7_redeemed_invite_reads_available_false_with_redeemed() {
    let (api, ctx) = seeded();
    drop(rounds(&api, &ctx, 2).await);
    redeem(&api, &ctx).await;
    let after = rounds(&api, &ctx, 1).await;
    let observed = after.first().expect("one round");

    // API-SITEINVITE: once spent, Available is False Redeemed and the facts stay True.
    status_harness::diff::status_matches(
        observed,
        &[
            ("Minted", "True", "AsExpected"),
            ("Redeemed", "True", "AsExpected"),
            ("Issued", "True", "AsExpected"),
            ("Available", "False", "Redeemed"),
            ("Progressing", "False", "AsExpected"),
            ("Degraded", "False", "AsExpected"),
        ],
    )
    .expect("a redeemed invite");
}
