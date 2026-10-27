//! Status conformance for this cluster's own `GridSite`: the enrollment step through
//! [`Enrollment::step`] and the renewal loop through `renew::Rounds`, against
//! `status_harness::FakeApi`.

use std::{collections::BTreeMap, sync::Arc};

use serde_json::json;
use status_harness::{FakeApi, Observed, invariants};

use super::*;
use crate::{
    enroll::{
        CA_SECRET, IDENTITY_SECRET, SITE_LABEL,
        tests::{Reply, SITE, TOKEN_SECRET, serve},
    },
    operator_config::{OperatorConfig, parse},
    rollup::{AVAILABLE, DEGRADED, PROGRESSING},
};

/// The operator namespace.
const NAMESPACE: &str = "grid-system";

/// The own site's path.
const PATH: &str = "/apis/grid.praxis.fast/v1alpha1/gridsites/site-d";

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

/// An API holding the own site.
fn seeded() -> FakeApi {
    let api = FakeApi::new();
    api.put(
        PATH,
        json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": SITE, "uid": "uid-site-d", "generation": 1 },
            "spec": { "gridNetworkRef": "grid" }
        }),
    );
    api
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
        let _step = enrollment.step(&client, None, config).await.expect("sites list");
        observed.push(api.observe(PATH).expect("own site stored"));
    }
    observed
}

/// The field manager each own-site condition type belongs to, the roll-up's apart from a
/// peer's.
fn managers() -> BTreeMap<&'static str, &'static str> {
    TYPES
        .iter()
        .copied()
        .chain([AVAILABLE, PROGRESSING, DEGRADED].map(|type_| (type_, OWN_ROLLUP_FIELD_MANAGER)))
        .collect()
}

/// The reasons the enrollment step writes, by type and status. `Available` copies the
/// first failing gate's reason, so it carries the gates' reasons.
const REASONS: [(&str, &str, &str); 28] = [
    (AVAILABLE, "True", "Available"),
    (AVAILABLE, "False", "AwaitingInvite"),
    (AVAILABLE, "False", "InviteMaybeSpent"),
    (AVAILABLE, "Unknown", "AwaitingFirstAttempt"),
    (ACCEPTED, "True", "Accepted"),
    (RESOLVED_REFS, "True", "ResolvedRefs"),
    (ENROLLED, "Unknown", "AwaitingFirstAttempt"),
    (ENROLLED, "False", "AwaitingInvite"),
    (ENROLLED, "False", "InviteMaybeSpent"),
    (ENROLLED, "True", "IdentityPresent"),
    (ENROLLED, "True", "Issued"),
    (IDENTITY_VALID, "Unknown", "AwaitingFirstAttempt"),
    (IDENTITY_VALID, "True", "IdentityValid"),
    (IDENTITY_VALID, "False", "IdentityMissing"),
    (IDENTITY_VALID, "False", "CertificateUntrusted"),
    (IDENTITY_VALID, "False", "CertificateNameMismatch"),
    (IDENTITY_VALID, "False", "CertificateKeyMismatch"),
    (AVAILABLE, "False", "CertificateUntrusted"),
    (AVAILABLE, "False", "CertificateNameMismatch"),
    (AVAILABLE, "False", "CertificateKeyMismatch"),
    (PROGRESSING, "False", "AsExpected"),
    (PROGRESSING, "True", "Enrolling"),
    (PROGRESSING, "True", "Retrying"),
    (DEGRADED, "False", "AsExpected"),
    (ROTATION_ON_SCHEDULE, "True", "RotationDisabled"),
    (ROTATION_ON_SCHEDULE, "True", "RotationOnSchedule"),
    (ROTATION_ON_SCHEDULE, "False", "SignerUntrusted"),
    (DEGRADED, "True", "SignerUntrusted"),
];

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
    invariants::observed_generation_tracks(&rounds).expect("invariant 3");
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
    invariants::observed_generation_tracks(&rounds).expect("invariant 3");
    invariants::one_manager_per_type(&api.writes(), PATH, &managers()).expect("invariant 5");
    invariants::reasons_within(&rounds, &REASONS).expect("invariant 8");
    let first = rounds.first().expect("five rounds");
    invariants::available_equals_rollup(first, &GATES).expect("invariant 7, Available");
    assert_eq!(available_of(first), ("True", "Available"));
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
    let leaf = certs::generate_dns_cert(&ca, SITE, SITE).expect("leaf");
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
            let leaf = certs::generate_dns_cert(&other, SITE, SITE).expect("leaf");
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
            let leaf = certs::generate_dns_cert(&ca, "site-e", "site-e").expect("leaf");
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
            let leaf = certs::generate_dns_cert(&ca, SITE, SITE).expect("leaf");
            let other = certs::generate_dns_cert(&ca, SITE, SITE).expect("leaf");
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
            .expect("sites list");
        let _renewed = renewal.round(&settings).await;
        observed.push(api.observe(PATH).expect("own site stored"));
    }
    observed
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
    assert_eq!(rotation["reason"], "RotationDisabled");
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

/// The `GridNetwork` the own site names, so the `GridSite` controller reconciles it in full.
fn with_network(api: &FakeApi) {
    api.put(
        "/apis/grid.praxis.fast/v1alpha1/gridnetworks/grid",
        json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "grid", "generation": 1, "uid": "uid-grid" },
            "spec": {}
        }),
    );
}

#[tokio::test]
async fn invariant_5_the_own_sites_rollup_has_exactly_one_manager_beside_the_grid_site_controller() {
    let api = seeded();
    with_network(&api);
    let client = api.client(NAMESPACE);
    let mut enrollment = Enrollment::default();
    let mut observed = Vec::with_capacity(3);
    for _round in 0..3 {
        let _step = enrollment.step(&client, None, &config("")).await.expect("sites list");
        let site: GridSite = serde_json::from_value(api.get(PATH).expect("own site stored")).expect("a GridSite");
        Box::pin(crate::controller::grid_site::reconcile(
            Arc::new(site),
            Arc::new(client.clone()),
        ))
        .await
        .expect("the GridSite controller reconciles the own site");
        observed.push(api.observe(PATH).expect("own site stored"));
    }

    let mut expected = managers();
    expected.insert(crate::controller::grid_site::CONNECTED, "grid-operator-probe");
    invariants::one_manager_per_type(&api.writes(), PATH, &expected).expect("invariant 5");
    let owners = api.owners(PATH);
    for type_ in [AVAILABLE, PROGRESSING, DEGRADED] {
        assert_eq!(
            owners.get(type_).map(String::as_str),
            Some(OWN_ROLLUP_FIELD_MANAGER),
            "{type_} has the own-site roll-up as its one manager"
        );
    }
    invariants::unchanged_from(&observed, 1).expect("invariant 1: the two writers do not flap");
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
    let (base, _pin) = crate::enroll::tests::byo_serving_signer(&ca).await;
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
