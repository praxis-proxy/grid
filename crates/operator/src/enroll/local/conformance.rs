//! Status conformance for this cluster's `GridNetwork`: the enrollment step through
//! [`Enrollment::step`], the renewal loop through `renew::Rounds` and the controller's
//! roll-up, against `status_harness::FakeApi`.

use std::{collections::BTreeMap, sync::Arc};

use serde_json::json;
use status_harness::{FakeApi, Observed, invariants};

use super::*;
use crate::{
    crd::grid_site::GridSite,
    enroll::{
        CA_SECRET, IDENTITY_SECRET, SITE_LABEL,
        tests::{Reply, SITE, TOKEN_SECRET, serve},
    },
    operator_config::{OperatorConfig, parse},
    rollup::{AVAILABLE, DEGRADED, PROGRESSING},
};

/// The operator namespace.
const NAMESPACE: &str = "grid-system";

/// The `GridNetwork`'s path, where the operator reports.
const PATH: &str = "/apis/grid.praxis.fast/v1alpha1/gridnetworks/grid";

/// The own `GridSite`'s path, which carries the peer roll-up only.
const SITE_PATH: &str = "/apis/grid.praxis.fast/v1alpha1/gridsites/site-d";

/// The types that read no spec field and so carry no `observedGeneration`.
const UNSTAMPED: [&str; 3] = [ACCEPTED, RESOLVED_REFS, ROTATION_ON_SCHEDULE];

/// The reasons the enrollment step writes, by type and status. `Available` copies the
/// first failing gate's reason, so it carries the gates' reasons.
const REASONS: [(&str, &str, &str); 47] = [
    (AVAILABLE, "True", "Available"),
    (AVAILABLE, "False", "AwaitingInvite"),
    (AVAILABLE, "False", "InviteMaybeSpent"),
    (AVAILABLE, "Unknown", "AwaitingFirstAttempt"),
    (ACCEPTED, "True", "Accepted"),
    (RESOLVED_REFS, "True", "ResolvedRefs"),
    (ENROLLED, "Unknown", "Enrolling"),
    (ENROLLED, "False", "AwaitingInvite"),
    (ENROLLED, "False", "InviteMaybeSpent"),
    (ENROLLED, "False", "InviteRefusedAfterLostAnswer"),
    (AVAILABLE, "False", "InviteRefusedAfterLostAnswer"),
    (ENROLLED, "True", "Enrolled"),
    (IDENTITY_VALID, "Unknown", "AwaitingFirstAttempt"),
    (IDENTITY_VALID, "True", "IdentityValid"),
    (IDENTITY_VALID, "False", "IdentityMissing"),
    (IDENTITY_VALID, "False", "CertificateUntrusted"),
    (IDENTITY_VALID, "False", "CertificateNameMismatch"),
    (IDENTITY_VALID, "False", "CertificateKeyMismatch"),
    (IDENTITY_VALID, "False", "CertificateDnsNameMissing"),
    (IDENTITY_VALID, "False", "CertificateOrganizationMismatch"),
    (AVAILABLE, "False", "CertificateDnsNameMissing"),
    (AVAILABLE, "False", "CertificateOrganizationMismatch"),
    (AVAILABLE, "False", "CertificateUntrusted"),
    (AVAILABLE, "False", "CertificateNameMismatch"),
    (AVAILABLE, "False", "CertificateKeyMismatch"),
    (PROGRESSING, "False", "AsExpected"),
    (PROGRESSING, "True", "Enrolling"),
    (PROGRESSING, "True", "Retrying"),
    (DEGRADED, "False", "AsExpected"),
    (ROTATION_ON_SCHEDULE, "True", "RotationOnSchedule"),
    (ROTATION_ON_SCHEDULE, "False", "SignerUntrusted"),
    (DEGRADED, "True", "SignerUntrusted"),
    (ROTATION_ON_SCHEDULE, "False", "RenewalNeedsPassthrough"),
    (DEGRADED, "True", "RenewalNeedsPassthrough"),
    (ROTATION_ON_SCHEDULE, "False", "SignerLapsed"),
    (DEGRADED, "True", "SignerLapsed"),
    (ENROLLED, "False", "EnrollmentOff"),
    (AVAILABLE, "False", "EnrollmentOff"),
    (ENROLLED, "False", "IdentityMissing"),
    (AVAILABLE, "False", "IdentityMissing"),
    (ACCEPTED, "False", "RotationUnsupportedUnderPinTrust"),
    (AVAILABLE, "False", "RotationUnsupportedUnderPinTrust"),
    (ACCEPTED, "False", "OperatorConfigInvalid"),
    (AVAILABLE, "False", "OperatorConfigInvalid"),
    (ROTATION_ON_SCHEDULE, "Unknown", "NotEvaluated"),
    (ROTATION_ON_SCHEDULE, "False", "RenewalFailing"),
    (DEGRADED, "True", "RenewalFailing"),
];

/// The path of a Secret in the operator namespace.
fn secret_path(name: &str) -> String {
    format!("/api/v1/namespaces/{NAMESPACE}/secrets/{name}")
}

/// A Secret named `name` with `data` and `labels`, as the API serves it.
fn secret(name: &str, data: &[(&str, &str)], labels: &[(&str, &str)]) -> Value {
    let secret = Secret {
        metadata: kube::api::ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(NAMESPACE.to_owned()),
            labels: Some(
                labels
                    .iter()
                    .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                    .collect(),
            ),
            ..kube::api::ObjectMeta::default()
        },
        data: Some(
            data.iter()
                .map(|(key, value)| ((*key).to_owned(), k8s_openapi::ByteString(value.as_bytes().to_vec())))
                .collect(),
        ),
        ..Secret::default()
    };
    serde_json::to_value(secret).expect("secret serializes")
}

/// An API holding this cluster's `GridNetwork` and its own `GridSite`.
fn seeded() -> FakeApi {
    let api = FakeApi::new();
    api.put(
        PATH,
        json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "grid", "generation": 1, "uid": "uid-grid" },
            "spec": { "gridId": "grid", "peerTrust": { "mode": "spiffe" } }
        }),
    );
    api.put(
        SITE_PATH,
        json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": SITE, "uid": "uid-site-d", "generation": 1 },
            "spec": { "gridNetworkRef": "grid" }
        }),
    );
    api
}

/// Write the controller's roll-up over the `GridNetwork` as it stands, as its reconcile does
/// on every status change, and observe it.
async fn rolled(api: &FakeApi, client: &Client) -> Observed {
    let network: GridNetwork = serde_json::from_value(api.get(PATH).expect("network stored")).expect("a GridNetwork");
    Box::pin(crate::controller::grid_network::apply_rollup(client, &network)).await;
    api.observe(PATH).expect("network stored")
}

/// The operator config naming this site, with `rest` under its site section.
fn config(rest: &str) -> Snapshot {
    let config: OperatorConfig = parse(&format!("site:\n  name: {SITE}\n{rest}"))
        .expect("config parses")
        .config;
    Snapshot::of_config(config)
}

/// Run `count` enrollment steps, observing the site after each.
async fn rounds(api: &FakeApi, config: &Snapshot, count: usize, mut before: impl FnMut(usize)) -> Vec<Observed> {
    let client = api.client(NAMESPACE);
    let mut enrollment = Enrollment::default();
    let mut observed = Vec::with_capacity(count);
    for round in 0..count {
        before(round);
        let _step = enrollment.step(&client, None, config).await.expect("networks list");
        observed.push(rolled(api, &client).await);
    }
    observed
}

/// The field manager each `GridNetwork` condition type belongs to, the controller's roll-up
/// among them.
fn managers() -> BTreeMap<&'static str, &'static str> {
    TYPES
        .iter()
        .copied()
        .chain(
            [AVAILABLE, PROGRESSING, DEGRADED]
                .map(|type_| (type_, crate::controller::grid_network::ROLLUP_FIELD_MANAGER)),
        )
        .collect()
}

/// `Available` on `observed`, as (status, reason).
fn available_of(observed: &Observed) -> (&str, &str) {
    let condition = observed.condition(AVAILABLE).expect("Available written");
    (
        condition["status"].as_str().unwrap_or_default(),
        condition["reason"].as_str().unwrap_or_default(),
    )
}

#[tokio::test]
async fn invariant_1_own_site_awaiting_an_invite_is_unchanged_across_five_rounds() {
    let api = seeded();
    let rounds = rounds(&api, &config(""), 5, |_round| {}).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::observed_generation_tracks(&rounds, &UNSTAMPED).expect("invariant 3");
    invariants::unknown_only_before_first_evaluation(&rounds).expect("invariant 4");
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    invariants::changes_explained(&rounds, &[]).expect("invariant 6");
    invariants::reasons_within(&rounds, &REASONS).expect("invariant 8");
    let first = rounds.first().expect("five rounds");
    invariants::available_equals_rollup(first, &GATES).expect("invariant 7, Available");
    assert_eq!(available_of(first), ("False", "AwaitingInvite"));
    assert!(first.condition(PROGRESSING).is_some(), "Progressing is written");
}

#[tokio::test]
async fn invariant_1_own_site_with_an_identity_is_unchanged_across_five_rounds() {
    let api = seeded();
    store_identity(&api);
    let rounds = renewal_rounds(&api, 5, |_round| {}).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::observed_generation_tracks(&rounds, &UNSTAMPED).expect("invariant 3");
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    invariants::reasons_within(&rounds, &REASONS).expect("invariant 8");
    let first = rounds.first().expect("five rounds");
    invariants::available_equals_rollup(first, &GATES).expect("invariant 7, Available");
    assert_eq!(available_of(first), ("True", "Available"));
    let valid = first.condition(IDENTITY_VALID).expect("IdentityValid");
    let message = valid["message"].as_str().unwrap_or_default();
    let (expiry, rest) = message.split_once(' ').unwrap_or_default();
    assert!(
        time::OffsetDateTime::parse(expiry, &time::format_description::well_known::Rfc3339).is_ok()
            && rest.starts_with("is the expiry; fingerprint sha256:"),
        "IdentityValid leads with the RFC 3339 expiry and names the fingerprint: {message}"
    );
}

/// Five rounds with an identity, deleted before the third.
async fn deleted_identity_rounds() -> (FakeApi, Vec<Observed>) {
    let api = seeded();
    store_identity(&api);
    let rounds = renewal_rounds(&api, 5, |round| {
        if round == 2 {
            api.remove(&secret_path(IDENTITY_SECRET));
        }
    })
    .await;
    (api, rounds)
}

#[tokio::test]
async fn invariant_1_deleting_the_identity_secret_settles_within_two_rounds() {
    let (api, rounds) = deleted_identity_rounds().await;

    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::unchanged_from(&rounds, 4).expect("invariant 1 two rounds after the deletion");
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    assert_eq!(available_of(&rounds[1]), ("True", "Available"));
    assert_ne!(rounds[2].condition(ENROLLED).expect("Enrolled")["status"], "True");
    assert_eq!(available_of(&rounds[3]).0, "False");
}

#[tokio::test]
async fn invariant_6_deleting_the_identity_secret_turns_available_false_in_the_same_round() {
    let (_api, rounds) = deleted_identity_rounds().await;

    invariants::changes_explained(&rounds, &[2]).expect("invariant 6");
    assert_eq!(
        available_of(&rounds[2]).0,
        "False",
        "Available falls in the round that sees the Secret gone"
    );
}

/// Store an identity the renewal loop can read: the leaf in the identity Secret, the CA in
/// the grid CA Secret.
fn store_identity(api: &FakeApi) {
    let ca = certs::generate_ca("grid-ca").expect("ca");
    let leaf = certs::generate_site_cert(&ca, SITE).expect("leaf");
    store_pair(api, &ca.cert_pem, &leaf.cert_pem, &leaf.key_pem);
}

/// Store `cert` and `key` as the identity and `ca` as the grid CA.
fn store_pair(api: &FakeApi, ca: &str, cert: &str, key: &str) {
    api.put(
        &secret_path(IDENTITY_SECRET),
        secret(IDENTITY_SECRET, &[("tls.crt", cert), ("tls.key", key)], &[]),
    );
    api.put(&secret_path(CA_SECRET), secret(CA_SECRET, &[("ca.crt", ca)], &[]));
}

/// Three renewal rounds over an identity that fails `reason`'s check, holding the
/// invariants, and the message `IdentityValid` gives.
async fn refused_identity(store: impl FnOnce(&FakeApi), reason: &str) -> String {
    let api = seeded();
    store(&api);
    let rounds = renewal_rounds(&api, 3, |_round| {}).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    invariants::reasons_within(&rounds, &REASONS).expect("invariant 8");
    invariants::available_equals_rollup(&rounds[1], &GATES).expect("invariant 7, Available");
    let valid = rounds[1].condition(IDENTITY_VALID).expect("IdentityValid");
    assert_eq!((&valid["status"], &valid["reason"]), (&json!("False"), &json!(reason)));
    assert_eq!(available_of(&rounds[1]), ("False", reason));
    valid["message"].as_str().unwrap_or_default().to_owned()
}

#[tokio::test]
async fn invariant_8_an_identity_another_ca_issued_is_certificate_untrusted() {
    let message = refused_identity(
        |api| {
            let held = certs::generate_ca("grid-ca").expect("ca");
            let other = certs::generate_ca("other-ca").expect("ca");
            let leaf = certs::generate_site_cert(&other, SITE).expect("leaf");
            store_pair(api, &held.cert_pem, &leaf.cert_pem, &leaf.key_pem);
        },
        "CertificateUntrusted",
    )
    .await;
    assert!(
        message.contains(IDENTITY_SECRET) && message.contains(CA_SECRET),
        "names both Secrets: {message}"
    );
}

#[tokio::test]
async fn invariant_8_an_identity_for_another_site_is_certificate_name_mismatch() {
    let message = refused_identity(
        |api| {
            let ca = certs::generate_ca("grid-ca").expect("ca");
            let leaf = certs::generate_site_cert(&ca, "site-e").expect("leaf");
            store_pair(api, &ca.cert_pem, &leaf.cert_pem, &leaf.key_pem);
        },
        "CertificateNameMismatch",
    )
    .await;
    assert!(
        message.contains(&certs::spiffe_id(SITE)) && message.contains("site.name"),
        "says what to issue or set: {message}"
    );
}

#[tokio::test]
async fn invariant_8_an_identity_with_another_key_is_certificate_key_mismatch() {
    let message = refused_identity(
        |api| {
            let ca = certs::generate_ca("grid-ca").expect("ca");
            let leaf = certs::generate_site_cert(&ca, SITE).expect("leaf");
            let other = certs::generate_site_cert(&ca, SITE).expect("leaf");
            store_pair(api, &ca.cert_pem, &leaf.cert_pem, &other.key_pem);
        },
        "CertificateKeyMismatch",
    )
    .await;
    assert!(message.contains("tls.key"), "names the key: {message}");
}

/// Run `count` rounds of the enrollment step and the renewal loop, as the operator does
/// once the identity names the site, observing the site after each.
async fn renewal_rounds(api: &FakeApi, count: usize, before: impl FnMut(usize)) -> Vec<Observed> {
    renewal_rounds_under(api, "", count, before).await
}

/// [`renewal_rounds`] with `rest` under the site section.
async fn renewal_rounds_under(api: &FakeApi, rest: &str, count: usize, mut before: impl FnMut(usize)) -> Vec<Observed> {
    let client = api.client(NAMESPACE);
    let settings = crate::enroll::renew::Settings::new()
        .with_site(Some(SITE.to_owned()))
        .with_config(ConfigWatch::fixed(config(rest)));
    let renewal = crate::enroll::renew::Rounds::new(client.clone(), &settings);
    let mut enrollment = Enrollment::default();
    let mut observed = Vec::with_capacity(count);
    for round in 0..count {
        before(round);
        let _step = enrollment
            .step(&client, Some(SITE), &config(rest))
            .await
            .expect("networks list");
        let _renewed = renewal.round(&settings).await;
        observed.push(rolled(api, &client).await);
    }
    observed
}

#[tokio::test(start_paused = true)]
async fn a_write_to_the_identity_secret_wakes_a_renewal_loop_with_no_signer() {
    use crate::enroll::renew::IDENTITY_POLL;
    let api = seeded();
    store_identity(&api);
    let client = api.client(NAMESPACE);
    let settings = crate::enroll::renew::Settings::new()
        .with_site(Some(SITE.to_owned()))
        .with_config(ConfigWatch::fixed(config("")));
    let renewal = crate::enroll::renew::Rounds::new(client, &settings);
    let _renewed = renewal.round(&settings).await;
    let seen = renewal.identity_version().await.expect("read");
    assert!(seen.is_some(), "the round named the identity Secret");
    let moved = renewal.identity_moved(seen);
    tokio::pin!(moved);
    assert!(
        tokio::time::timeout(IDENTITY_POLL * 3, &mut moved).await.is_err(),
        "an unwritten Secret does not wake the loop"
    );
    store_identity(&api);
    tokio::time::timeout(IDENTITY_POLL * 2, moved)
        .await
        .expect("a write by another manager wakes the loop within one poll");
}

#[tokio::test]
async fn invariant_4_identity_valid_reads_false_identity_missing_once_the_secret_is_deleted() {
    let api = seeded();
    store_identity(&api);
    let rounds = renewal_rounds(&api, 4, |round| {
        if round == 2 {
            api.remove(&secret_path(IDENTITY_SECRET));
        }
    })
    .await;

    invariants::unknown_only_before_first_evaluation(&rounds).expect("invariant 4");
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    invariants::changes_explained(&rounds, &[2]).expect("invariant 6");
    invariants::unchanged_from(&rounds, 3).expect("invariant 1 after the deletion");
    let valid = |round: usize| {
        let condition = rounds[round].condition(IDENTITY_VALID).expect("IdentityValid");
        (condition["status"].clone(), condition["reason"].clone())
    };
    assert_eq!(valid(1), (json!("True"), json!("IdentityValid")));
    assert_eq!(valid(2), (json!("False"), json!("IdentityMissing")));
    assert_eq!(available_of(&rounds[1]), ("True", "Available"));
    assert_eq!(available_of(&rounds[2]).0, "False");
}

/// Three rounds against a signer that answers 408, with the invite and pin in place.
async fn timeout_rounds() -> (Arc<crate::enroll::tests::Mock>, FakeApi, Vec<Observed>) {
    let (mock, settings) = serve(Reply::Timeout).await;
    let api = seeded();
    let config = config(&format!(
        "  signerEndpoint:\n    url: {}\n    caBundleRef: {{kind: Secret, name: grid-ca-bundle}}\n  inviteSecretRef:\n    name: {TOKEN_SECRET}\n",
        settings.base.as_str()
    ));
    api.put(
        &secret_path("grid-ca-bundle"),
        secret(
            "grid-ca-bundle",
            &[("ca.crt", &mock.ca.cert_pem)],
            &[(SIGNER_BUNDLE_LABEL, "true")],
        ),
    );
    api.put(
        &secret_path(TOKEN_SECRET),
        secret(TOKEN_SECRET, &[("token", "tok")], &[(SITE_LABEL, SITE)]),
    );
    let rounds = rounds(&api, &config, 3, |_round| {}).await;
    (mock, api, rounds)
}

#[tokio::test]
async fn invariant_8_a_408_from_the_signer_is_invite_maybe_spent_and_is_sent_again() {
    let (mock, api, rounds) = timeout_rounds().await;

    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    for round in [0, 2] {
        let enrolled = rounds[round].condition(ENROLLED).expect("Enrolled");
        assert_eq!(enrolled["status"], "False", "round {round}");
        assert_eq!(enrolled["reason"], "InviteMaybeSpent", "round {round}");
    }
    assert_eq!(mock.calls(), 3, "a 408 is not definitive, so every round sends again");
    assert_eq!(available_of(&rounds[0]), ("False", "InviteMaybeSpent"));
}

#[tokio::test]
async fn invariant_1_a_resent_attempt_holds_progressing_retrying_from_the_first_round() {
    let (_mock, _api, rounds) = timeout_rounds().await;

    invariants::reasons_within(&rounds, &REASONS).expect("invariant 8");
    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    let progressing = rounds[2].condition(PROGRESSING).expect("Progressing");
    assert_eq!(
        (&progressing["status"], &progressing["reason"]),
        (&json!("True"), &json!("Retrying"))
    );
}

#[tokio::test]
async fn invariant_2_a_resent_attempt_does_not_move_progressing_transition_time_every_round() {
    let (_mock, _api, rounds) = timeout_rounds().await;
    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
}

#[tokio::test]
async fn invariant_8_no_signer_reads_rotation_on_schedule_no_signer_and_never_blames_pin_trust() {
    let api = seeded();
    store_identity(&api);
    let rounds = renewal_rounds(&api, 3, |_round| {}).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    invariants::rollup_present_and_equal(&rounds[0], &GATES).expect("invariant 7");
    let rotation = rounds[1].condition(ROTATION_ON_SCHEDULE).expect("RotationOnSchedule");
    assert_eq!(rotation["status"], "True");
    assert_eq!(rotation["reason"], "RotationOnSchedule");
    assert!(
        rotation["message"]
            .as_str()
            .unwrap_or_default()
            .contains("nothing is known to renew this identity"),
        "{}",
        rotation["message"]
    );
    assert!(
        !rotation["message"].as_str().unwrap_or_default().contains("pin"),
        "no identity message names pin peer trust: {}",
        rotation["message"]
    );
    let degraded = rounds[1].condition(DEGRADED).expect("Degraded");
    assert_eq!(degraded["status"], "False");
}

#[tokio::test]
#[expect(clippy::too_many_lines, reason = "three writers across two objects")]
async fn invariant_5_the_own_grid_site_carries_only_the_peer_rollup_and_the_network_one_writer_each() {
    let api = seeded();
    let client = api.client(NAMESPACE);
    let mut enrollment = Enrollment::default();
    let mut observed = Vec::with_capacity(3);
    for _round in 0..3 {
        let _step = enrollment
            .step(&client, None, &config(""))
            .await
            .expect("networks list");
        observed.push(rolled(&api, &client).await);
        let site: GridSite = serde_json::from_value(api.get(SITE_PATH).expect("own site stored")).expect("a GridSite");
        Box::pin(crate::controller::grid_site::reconcile(
            Arc::new(site),
            Arc::new(client.clone()),
        ))
        .await
        .expect("the GridSite controller reconciles the own site");
    }

    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    invariants::unchanged_from(&observed, 1).expect("invariant 1: the writers do not flap");
    let site = api.observe(SITE_PATH).expect("own site stored");
    for (type_, _) in TYPES {
        assert!(
            site.condition(type_).is_none(),
            "{type_} is the GridNetwork's, never the GridSite's"
        );
    }
    let owners = api.owners(SITE_PATH);
    for type_ in [AVAILABLE, PROGRESSING, DEGRADED] {
        assert_eq!(
            owners.get(type_).map(String::as_str),
            Some("grid-operator-site"),
            "{type_} on the own GridSite is the peer roll-up, as on any GridSite"
        );
    }
}

#[tokio::test]
async fn invariant_8_an_identity_cert_manager_issued_reads_renewed_by_its_certificate() {
    let api = seeded();
    store_identity(&api);
    let mut held = api.get(&secret_path(IDENTITY_SECRET)).expect("identity stored");
    held["metadata"]["annotations"] = json!({ "cert-manager.io/certificate-name": "site-d-identity" });
    api.put(&secret_path(IDENTITY_SECRET), held);
    let rounds = renewal_rounds(&api, 3, |_round| {}).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    invariants::reasons_within(&rounds, &REASONS).expect("invariant 8");
    let rotation = rounds[1].condition(ROTATION_ON_SCHEDULE).expect("RotationOnSchedule");
    assert_eq!(
        (&rotation["status"], &rotation["reason"], &rotation["message"]),
        (
            &json!("True"),
            &json!("RotationOnSchedule"),
            &json!("renewed by cert-manager Certificate site-d-identity")
        )
    );
    assert_eq!(rounds[1].condition(DEGRADED).expect("Degraded")["status"], "False");
}

/// Store an identity five days from expiry, so renewal is due, returning the grid CA.
fn store_due_identity(api: &FakeApi) -> String {
    let ca = certs::generate_ca("grid-ca").expect("ca");
    let csr = certs::generate_csr(SITE).expect("csr");
    let now = time::OffsetDateTime::now_utc();
    let validity = certs::Validity {
        not_before: now.saturating_sub(time::Duration::days(25)),
        not_after: now.saturating_add(time::Duration::days(5)),
    };
    let leaf = certs::sign_csr(&ca, SITE, &csr.csr_pem, validity).expect("a leaf due for renewal");
    store_pair(api, &ca.cert_pem, &leaf.cert_pem, &csr.key_pem);
    ca.cert_pem
}

#[tokio::test]
async fn invariant_8_a_signer_neither_the_grid_ca_nor_a_pin_trusts_is_signer_untrusted() {
    crate::init_process_crypto();
    let api = seeded();
    let ca = store_due_identity(&api);
    let (base, _pin) = crate::enroll::tests::byo_serving_signer(&ca, axum::Router::new(), false).await;
    let rest = format!("  signerEndpoint:\n    url: {}\n", base.as_str());
    let rounds = renewal_rounds_under(&api, &rest, 3, |_round| {}).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    invariants::reasons_within(&rounds, &REASONS).expect("invariant 8");
    let rotation = rounds[1].condition(ROTATION_ON_SCHEDULE).expect("RotationOnSchedule");
    assert_eq!(
        (&rotation["status"], &rotation["reason"]),
        (&json!("False"), &json!("SignerUntrusted"))
    );
    assert!(
        rotation["message"]
            .as_str()
            .unwrap_or_default()
            .contains("site.signerEndpoint.caBundleRef"),
        "says to pin the signer: {}",
        rotation["message"]
    );
    assert_eq!(
        available_of(&rounds[1]),
        ("True", "Available"),
        "the identity still serves"
    );
}


/// A signer answering through `app`, its serving CA pinned by a bundle in `api`, accepting
/// client certificates `ca` issued, as the site section naming it.
async fn pinned_signer(api: &FakeApi, ca: &str, app: axum::Router, lapsed: bool) -> String {
    let (base, pin) = crate::enroll::tests::byo_serving_signer(ca, app, lapsed).await;
    api.put(
        &secret_path("signer-pin"),
        secret("signer-pin", &[("ca.crt", &pin)], &[(SIGNER_BUNDLE_LABEL, "true")]),
    );
    format!(
        "  signerEndpoint:\n    url: {}\n    caBundleRef: {{kind: Secret, name: signer-pin}}\n",
        base.as_str()
    )
}

#[tokio::test]
async fn invariant_8_a_signer_that_never_sees_the_client_certificate_needs_passthrough() {
    crate::init_process_crypto();
    let api = seeded();
    let ca = store_due_identity(&api);
    let stripped = axum::Router::new().route(
        crate::enroll::signer::RENEW_PATH,
        axum::routing::post(|| async {
            (
                http::StatusCode::UNAUTHORIZED,
                axum::Json(json!({ "error": "identity_required", "message": "rotation requires mutual TLS" })),
            )
        }),
    );
    let rest = pinned_signer(&api, &ca, stripped, false).await;
    let rounds = renewal_rounds_under(&api, &rest, 3, |_round| {}).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    invariants::reasons_within(&rounds, &REASONS).expect("invariant 8");
    let rotation = rounds[1].condition(ROTATION_ON_SCHEDULE).expect("RotationOnSchedule");
    assert_eq!(
        (&rotation["status"], &rotation["reason"]),
        (&json!("False"), &json!("RenewalNeedsPassthrough"))
    );
}

#[tokio::test]
async fn invariant_8_a_signer_serving_a_lapsed_certificate_is_signer_lapsed() {
    crate::init_process_crypto();
    let api = seeded();
    let ca = store_due_identity(&api);
    let rest = pinned_signer(&api, &ca, axum::Router::new(), true).await;
    let rounds = renewal_rounds_under(&api, &rest, 3, |_round| {}).await;
    // A second on, as rounds are in production: the handshake error names the verification time.
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let later = renewal_rounds_under(&api, &rest, 1, |_round| {}).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    for type_ in [ROTATION_ON_SCHEDULE, DEGRADED] {
        assert_eq!(
            rounds[2].condition(type_).map(|held| held["message"].clone()),
            later[0].condition(type_).map(|held| held["message"].clone()),
            "{type_} says the same of the same lapsed signer"
        );
    }
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    invariants::reasons_within(&rounds, &REASONS).expect("invariant 8");
    let rotation = rounds[1].condition(ROTATION_ON_SCHEDULE).expect("RotationOnSchedule");
    assert_eq!(
        (&rotation["status"], &rotation["reason"]),
        (&json!("False"), &json!("SignerLapsed"))
    );
    assert!(
        rotation["message"]
            .as_str()
            .unwrap_or_default()
            .contains("must rotate its serving certificate"),
        "names the signer's fix, not a pin: {}",
        rotation["message"]
    );
}

/// Hold this cluster's `GridNetwork` under pin peer trust.
fn pinned_trust(api: &FakeApi) {
    api.edit(PATH, true, |network| {
        network["spec"]["peerTrust"] = json!({ "mode": "pin" });
    });
}

#[tokio::test]
async fn invariant_1_rotation_enabled_under_pin_holds_accepted_false_without_a_second_writer_flapping_it() {
    let api = seeded();
    pinned_trust(&api);
    store_identity(&api);
    let rounds = renewal_rounds_under(
        &api,
        "  signerEndpoint:\n    url: https://signer.example.com\n",
        4,
        |_round| {},
    )
    .await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1: the enrollment step and the renewal loop agree");
    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    invariants::reasons_within(&rounds, &REASONS).expect("invariant 8");
    for round in &rounds {
        let accepted = round.condition(ACCEPTED).expect("Accepted");
        assert_eq!(
            (&accepted["status"], &accepted["reason"]),
            (&json!("False"), &json!("RotationUnsupportedUnderPinTrust"))
        );
    }
}

#[tokio::test]
async fn invariant_8_without_an_operator_config_the_network_says_enrollment_is_off() {
    let api = seeded();
    let rounds = rounds(&api, &Snapshot::absent(), 3, |_round| {}).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    invariants::reasons_within(&rounds, &REASONS).expect("invariant 8");
    invariants::available_equals_rollup(&rounds[1], &GATES).expect("invariant 7, Available");
    let accepted = rounds[1].condition(ACCEPTED).expect("Accepted");
    assert_eq!(accepted["message"], "no operator config, enrollment off");
    assert_eq!(
        rounds[1].condition(RESOLVED_REFS).expect("ResolvedRefs")["status"],
        "True"
    );
    assert_eq!(available_of(&rounds[1]), ("False", "EnrollmentOff"));
}

#[tokio::test]
async fn invariant_8_an_identity_written_outside_a_signer_reads_enrolled_with_what_renews_it() {
    let api = seeded();
    store_identity(&api);
    let rounds = renewal_rounds(&api, 3, |_round| {}).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::reasons_within(&rounds, &REASONS).expect("invariant 8");
    let enrolled = rounds[1].condition(ENROLLED).expect("Enrolled");
    assert_eq!(
        (&enrolled["status"], &enrolled["message"]),
        (
            &json!("True"),
            &json!("identity provisioned outside the signer; nothing is known to renew it")
        )
    );
    assert_eq!(available_of(&rounds[1]), ("True", "Available"));
}

#[tokio::test]
async fn invariant_4_a_restart_after_the_identity_is_deleted_never_keeps_identity_valid_true() {
    let api = seeded();
    store_identity(&api);
    let held = renewal_rounds(&api, 2, |_round| {}).await;
    assert_eq!(
        held[1].condition(IDENTITY_VALID).expect("IdentityValid")["status"],
        "True"
    );

    api.remove(&secret_path(IDENTITY_SECRET));
    let restarted = rounds(&api, &config(""), 2, |_round| {}).await;
    let valid = restarted[0].condition(IDENTITY_VALID).expect("IdentityValid");
    assert_eq!(
        (&valid["status"], &valid["reason"]),
        (&json!("Unknown"), &json!("AwaitingFirstAttempt")),
        "a restart without the identity rewrites what an earlier process held"
    );
    invariants::unchanged_from(&restarted, 1).expect("invariant 1");
}

#[tokio::test]
async fn invariant_2_a_refused_config_stays_refused_through_a_round_that_cannot_read() {
    let api = seeded();
    store_identity(&api);
    let client = api.client(NAMESPACE);
    let refused = Snapshot {
        loaded: crate::operator_config::Loaded::Invalid("site.extra: unknown field".to_owned()),
        ..Snapshot::absent()
    };
    let settings = crate::enroll::renew::Settings::new()
        .with_site(Some(SITE.to_owned()))
        .with_config(ConfigWatch::fixed(refused.clone()));
    let renewal = crate::enroll::renew::Rounds::new(client.clone(), &settings);
    let mut enrollment = Enrollment::default();
    let mut observed = Vec::new();
    for round in 0..4 {
        if round == 2 {
            api.fail_reads(&secret_path(IDENTITY_SECRET), 2);
        }
        drop(enrollment.step(&client, Some(SITE), &refused).await);
        let _renewed = renewal.round(&settings).await;
        observed.push(rolled(&api, &client).await);
    }

    invariants::transition_time_moves_only_with_status(&observed).expect("invariant 2");
    invariants::unchanged_from(&observed, 1).expect("invariant 1: a failed read changes nothing");
    for round in &observed {
        let accepted = round.condition(ACCEPTED).expect("Accepted");
        assert_eq!(
            (&accepted["status"], &accepted["reason"]),
            (&json!("False"), &json!("OperatorConfigInvalid"))
        );
    }
}

#[tokio::test]
async fn invariant_8_a_refused_config_with_no_identity_is_accepted_false_on_the_network_and_touches_no_grid_site() {
    let api = seeded();
    let refused = Snapshot {
        loaded: crate::operator_config::Loaded::Invalid("site.signerEndpont: unknown field".to_owned()),
        ..Snapshot::absent()
    };
    let rounds = rounds(&api, &refused, 3, |_round| {}).await;

    invariants::unchanged_from(&rounds, 1).expect("invariant 1");
    invariants::reasons_within(&rounds, &REASONS).expect("invariant 8");
    let accepted = rounds[1].condition(ACCEPTED).expect("Accepted");
    assert_eq!(
        (&accepted["status"], &accepted["reason"]),
        (&json!("False"), &json!("OperatorConfigInvalid"))
    );
    assert!(
        accepted["message"]
            .as_str()
            .unwrap_or_default()
            .contains("signerEndpont"),
        "names the field at fault: {}",
        accepted["message"]
    );
    assert!(
        api.writes().iter().all(|write| write.path != SITE_PATH),
        "nothing is written to any GridSite, and nothing is cleared"
    );
}

/// Store a 30-day identity whose renewal fell due `past` ago, annotated with the cert-manager
/// Certificate `renewer` when one is given.
fn store_lapsing_identity(api: &FakeApi, past: time::Duration, renewer: Option<&str>) {
    let ca = certs::generate_ca("grid-ca").expect("ca");
    let csr = certs::generate_csr(SITE).expect("csr");
    let now = time::OffsetDateTime::now_utc();
    let not_before = now.saturating_sub(time::Duration::days(20)).saturating_sub(past);
    let validity = certs::Validity {
        not_before,
        not_after: not_before.saturating_add(time::Duration::days(30)),
    };
    let leaf = certs::sign_csr(&ca, SITE, &csr.csr_pem, validity).expect("leaf");
    store_pair(api, &ca.cert_pem, &leaf.cert_pem, &csr.key_pem);
    if let Some(renewer) = renewer {
        let mut held = api.get(&secret_path(IDENTITY_SECRET)).expect("identity stored");
        held["metadata"]["annotations"] = json!({ "cert-manager.io/certificate-name": renewer });
        api.put(&secret_path(IDENTITY_SECRET), held);
    }
}

#[tokio::test]
async fn invariant_8_cert_manager_replacing_the_identity_just_after_it_falls_due_never_reads_overdue() {
    let api = seeded();
    store_lapsing_identity(&api, time::Duration::minutes(5), Some("site-d-identity"));
    let rounds = renewal_rounds(&api, 4, |round| {
        if round == 2 {
            tracing::info!("cert-manager issues the next certificate into the Secret");
            store_lapsing_identity(&api, time::Duration::days(-10), Some("site-d-identity"));
        }
    })
    .await;

    invariants::transition_time_moves_only_with_status(&rounds).expect("invariant 2");
    invariants::reasons_within(&rounds, &REASONS).expect("invariant 8");
    for round in &rounds {
        let rotation = round.condition(ROTATION_ON_SCHEDULE).expect("RotationOnSchedule");
        assert_eq!(
            (&rotation["status"], &rotation["message"]),
            (
                &json!("True"),
                &json!("renewed by cert-manager Certificate site-d-identity")
            ),
            "within the grace past due an external renewer is not overdue"
        );
        assert_eq!(
            round.condition(DEGRADED).expect("Degraded")["status"],
            "False",
            "no false alarm"
        );
    }
}

#[tokio::test]
async fn invariant_8_an_unrenewed_identity_reads_overdue_once_the_grace_runs_out() {
    let api = seeded();
    store_lapsing_identity(&api, time::Duration::hours(1), Some("site-d-identity"));
    let rounds = renewal_rounds(&api, 2, |_round| {}).await;
    let rotation = rounds[1].condition(ROTATION_ON_SCHEDULE).expect("RotationOnSchedule");
    assert_eq!(
        (&rotation["status"], &rotation["reason"]),
        (&json!("False"), &json!("RotationOverdue"))
    );
    assert!(
        rotation["message"]
            .as_str()
            .unwrap_or_default()
            .contains("site-d-identity"),
        "names the Certificate that has not replaced it: {}",
        rotation["message"]
    );
}

#[tokio::test]
async fn invariant_8_an_identity_without_the_dns_name_peers_dial_is_certificate_dns_name_missing() {
    let message = refused_identity(
        |api| {
            let ca = certs::generate_ca("grid-ca").expect("ca");
            let leaf = certs::generate_dns_cert(&ca, SITE, "site-d.example.com").expect("leaf");
            store_pair(api, &ca.cert_pem, &leaf.cert_pem, &leaf.key_pem);
        },
        "CertificateDnsNameMissing",
    )
    .await;
    assert!(
        message.contains("site-d.grid.internal"),
        "names the DNS name to issue: {message}"
    );
}

#[tokio::test]
async fn invariant_8_an_identity_naming_another_organization_is_certificate_organization_mismatch() {
    let message = refused_identity(
        |api| {
            let ca = certs::generate_ca("grid-ca").expect("ca");
            let leaf = certs::generate_cert_with_org(&ca, SITE, "acme").expect("leaf");
            store_pair(api, &ca.cert_pem, &leaf.cert_pem, &leaf.key_pem);
        },
        "CertificateOrganizationMismatch",
    )
    .await;
    assert!(
        message.contains("O=site-d"),
        "names the organization to issue: {message}"
    );
}

#[tokio::test]
async fn invariant_5_a_site_name_change_writes_nothing_to_any_grid_site() {
    let api = seeded();
    let mut observed = rounds(&api, &config(""), 2, |_round| {}).await;
    let renamed: OperatorConfig = parse("site:\n  name: site-e\n").expect("config parses").config;
    observed.extend(rounds(&api, &Snapshot::of_config(renamed), 2, |_round| {}).await);

    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    assert!(
        api.writes().iter().all(|write| !write.path.contains("/gridsites")),
        "the operator's conditions live on the GridNetwork, so a rename clears nothing elsewhere"
    );
    assert_eq!(
        available_of(&observed[3]).0,
        "False",
        "the renamed site still waits for its invite"
    );
}

/// The operator config naming the signer at `base`, pinned to `ca_pem`, and the invite, with
/// the pin and the invite stored in `api`.
fn invited_through(api: &FakeApi, base: &str, ca_pem: &str) -> Snapshot {
    api.put(
        &secret_path("grid-ca-bundle"),
        secret(
            "grid-ca-bundle",
            &[("ca.crt", ca_pem)],
            &[(SIGNER_BUNDLE_LABEL, "true")],
        ),
    );
    api.put(
        &secret_path(TOKEN_SECRET),
        secret(TOKEN_SECRET, &[("token", "tok")], &[(SITE_LABEL, SITE)]),
    );
    config(&format!(
        "  signerEndpoint:\n    url: {base}\n    caBundleRef: {{kind: Secret, name: grid-ca-bundle}}\n  inviteSecretRef:\n    name: {TOKEN_SECRET}\n"
    ))
}

#[tokio::test]
#[expect(clippy::too_many_lines, reason = "three restarts and what each must read")]
async fn invariant_8_a_restart_after_a_lost_answer_reads_invite_maybe_spent_when_the_token_is_refused() {
    let (mock, settings) = serve(Reply::Timeout).await;
    let api = seeded();
    let config = invited_through(&api, settings.base.as_str(), &mock.ca.cert_pem);
    let client = api.client(NAMESPACE);
    let mut observed = Vec::new();
    for round in 0..3 {
        if round == 1 {
            tracing::info!("the signer spent the token on the answer that was lost");
            mock.answer(Reply::Spent);
        }
        tracing::info!(round, "a fresh process, as after a kill");
        let _step = Enrollment::default()
            .step(&client, None, &config)
            .await
            .expect("networks list");
        observed.push(rolled(&api, &client).await);
    }

    invariants::transition_time_moves_only_with_status(&observed).expect("invariant 2");
    invariants::reasons_within(&observed, &REASONS).expect("invariant 8");
    for round in &observed[1..] {
        let enrolled = round.condition(ENROLLED).expect("Enrolled");
        assert_eq!(
            (&enrolled["status"], &enrolled["reason"]),
            (&json!("False"), &json!("InviteRefusedAfterLostAnswer")),
            "a refusal after an unanswered request is never read as an invalid invite"
        );
        assert!(
            enrolled["message"]
                .as_str()
                .unwrap_or_default()
                .contains("a new invite may be needed"),
            "{}",
            enrolled["message"]
        );
    }
    assert!(
        api.get(&secret_path(&format!("{IDENTITY_SECRET}-pending"))).is_some(),
        "the request is kept across the restarts"
    );
}
