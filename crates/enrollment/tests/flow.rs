//! Site-token enrollment, end to end over the HTTP interface.

#![allow(clippy::tests_outside_test_module, reason = "integration tests live in tests/")]
#![expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "tests, and serde_json::Value indexing yields Null rather than panicking"
)]

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use enrollment::{AppState, GridAdmins, NewSiteToken, SeedRecord, SharedCa, Store, authz::Authorizer, router};
use http_body_util::BodyExt as _;
use rcgen::{CertificateParams, DnType, KeyPair, SanType};
use serde_json::{Value, json};
use tower::ServiceExt as _;

/// The grid-admin credential the tests mint with.
const TOKEN: &str = "t0ken";

/// A service with a fresh CA, an empty store, and one grid-admin that may also delete.
fn service() -> axum::Router {
    service_with_store(Store::memory())
}

/// A service over the supplied store.
fn service_with_store(store: Store) -> axum::Router {
    let ca = certs::generate_ca("test-grid-ca").expect("ca");
    router(Arc::new(AppState {
        store,
        ca: SharedCa::new(ca),
        authorizer: Authorizer::Local(
            GridAdmins::from_table("tester: t0ken\n")
                .expect("token table")
                .with_roles("tester: grid-admin,enrollment-admin\n")
                .expect("roles"),
        ),
        cert_lifetime: certs::DEFAULT_SITE_CERT_LIFETIME,
        reserved_sites: Vec::new(),
        renewals_enabled: true,
        #[cfg(feature = "csr-record")]
        recorder: None,
    }))
}

/// A service over `store` that reserves `reserved`, the hub's own name.
fn service_reserving(store: Store, reserved: &str) -> axum::Router {
    let ca = certs::generate_ca("test-grid-ca").expect("ca");
    router(Arc::new(AppState {
        store,
        ca: SharedCa::new(ca),
        authorizer: Authorizer::Local(
            GridAdmins::from_table("tester: t0ken\n")
                .expect("token table")
                .with_roles("tester: grid-admin,enrollment-admin\n")
                .expect("roles"),
        ),
        cert_lifetime: certs::DEFAULT_SITE_CERT_LIFETIME,
        reserved_sites: vec![reserved.to_owned()],
        renewals_enabled: true,
        #[cfg(feature = "csr-record")]
        recorder: None,
    }))
}

/// A request the way a site would make one, asking for `requested`.
fn csr_asking_for(requested: &[SanType]) -> String {
    let key = KeyPair::generate().expect("key");
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, "whatever");
    params.subject_alt_names = requested.to_vec();
    params.serialize_request(&key).expect("csr").pem().expect("pem")
}

fn plain_csr() -> String {
    csr_asking_for(&[])
}

/// A valid CSR and its public key digest.
fn csr_and_key_hash() -> (String, String) {
    let csr = plain_csr();
    let key = certs::verify_csr(&csr).expect("valid CSR");
    (csr, key)
}

async fn call(app: &axum::Router, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
    send(app, method, path, body, &[]).await
}

/// A call carrying a grid-admin credential.
async fn call_as_admin(app: &axum::Router, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
    send(app, method, path, body, &[("authorization", format!("Bearer {TOKEN}"))]).await
}

async fn send(
    app: &axum::Router,
    method: &str,
    path: &str,
    body: Option<Value>,
    headers: &[(&str, String)],
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    for (name, value) in headers {
        builder = builder.header(*name, value);
    }
    let request = builder
        .body(body.map_or_else(Body::empty, |value| Body::from(value.to_string())))
        .expect("request");

    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = response.into_body().collect().await.expect("body").to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

/// Ask to mint a token pinning `site`, returning the response as is.
async fn mint_status(app: &axum::Router, site: &str) -> (StatusCode, Value) {
    call_as_admin(
        app,
        "POST",
        "/v1alpha1/enrollmenttokens",
        Some(json!({ "siteName": site, "gridNetworkRef": "demo-grid" })),
    )
    .await
}

/// Mint a token pinning `site`, returning the token and its id.
async fn mint(app: &axum::Router, site: &str) -> (String, String) {
    let (status, body) = mint_status(app, site).await;
    assert_eq!(status, StatusCode::CREATED, "a grid-admin can mint a token");
    assert_eq!(body["siteName"], site, "the token pins the name");
    (
        body["token"].as_str().expect("token").to_owned(),
        body["tokenId"].as_str().expect("token id").to_owned(),
    )
}

/// Enroll under a token with the given CSR. The token rides in `Authorization:
/// Bearer`, the same header shape the grid-admin routes use.
async fn enroll(app: &axum::Router, token: &str, csr: &str) -> (StatusCode, Value) {
    send(
        app,
        "POST",
        "/v1alpha1/enrollments",
        Some(json!({ "csr": csr })),
        &[("authorization", format!("Bearer {token}"))],
    )
    .await
}

#[tokio::test]
async fn a_site_enrolls_and_gets_a_certificate() {
    let app = service();
    let (token, _id) = mint(&app, "site-d").await;

    let (status, issued) = enroll(&app, &token, &plain_csr()).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "enrolling under a token issues a certificate"
    );
    assert_eq!(issued["spiffeId"], "spiffe://grid.internal/site/site-d");
    assert!(
        issued["certificate"]
            .as_str()
            .is_some_and(|pem| pem.contains("BEGIN CERTIFICATE")),
        "the certificate comes back inline"
    );
    assert!(
        issued["caCertificate"]
            .as_str()
            .is_some_and(|pem| pem.contains("BEGIN CERTIFICATE")),
        "the grid CA comes back with the certificate"
    );
    assert!(
        issued["id"].as_str().is_some_and(|id| !id.is_empty()),
        "the issued-enrollment row id comes back"
    );
    assert!(
        issued["publicKeySha256"].as_str().is_some_and(|hex| hex.len() == 64),
        "the key fingerprint comes back"
    );
}

#[tokio::test]
#[expect(clippy::too_many_lines, reason = "exercises the invite key-binding retry flow")]
async fn a_bound_invite_refuses_another_key_without_spending_the_token() {
    let (expected_csr, expected_key) = csr_and_key_hash();
    let (other_csr, other_key) = csr_and_key_hash();
    assert_ne!(expected_key, other_key, "the test requests use distinct keys");
    let token = "bound-invite-token";
    let token_sha256 = certs::sha256(token.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let store = Store::memory();
    store
        .mint_site_token(NewSiteToken {
            token_id: Some(uuid::Uuid::new_v4()),
            token_sha256,
            public_key_sha256: Some(expected_key),
            site_name: "site-bound".to_owned(),
            grid_network_ref: "demo-grid".to_owned(),
            issued_by: "siteinvite/test".to_owned(),
            expires_at: time::OffsetDateTime::now_utc().saturating_add(time::Duration::hours(1)),
            allow_deleted_name: false,
        })
        .await
        .expect("mint bound invite");
    let app = service_with_store(store);

    let (mismatch_status, body) = enroll(&app, token, &other_csr).await;
    assert_eq!(
        mismatch_status,
        StatusCode::CONFLICT,
        "another key cannot use the invite"
    );
    assert_eq!(body["error"], "key_mismatch");

    let (enrollment_status, _) = enroll(&app, token, &expected_csr).await;
    assert_eq!(
        enrollment_status,
        StatusCode::CREATED,
        "a mismatched request leaves the invite available"
    );
}

#[tokio::test]
async fn enroll_without_a_token_is_refused() {
    let app = service();
    let (status, body) = call(
        &app,
        "POST",
        "/v1alpha1/enrollments",
        Some(json!({ "csr": plain_csr() })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "the endpoint is closed without a token"
    );
    assert_eq!(body["error"], "invalid_token");
}

#[tokio::test]
async fn an_unknown_token_is_refused() {
    let app = service();
    let (status, body) = enroll(&app, "deadbeef", &plain_csr()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "a token nobody minted is refused");
    assert_eq!(body["error"], "invalid_token", "the same error as a missing one");
}

#[tokio::test]
async fn a_token_is_one_shot() {
    let app = service();
    let (token, _id) = mint(&app, "site-once").await;

    let (first, _first_body) = enroll(&app, &token, &plain_csr()).await;
    assert_eq!(first, StatusCode::CREATED, "the first enrollment redeems the token");

    let (second, second_body) = enroll(&app, &token, &plain_csr()).await;
    assert_eq!(
        second,
        StatusCode::UNAUTHORIZED,
        "the token cannot enroll a second site"
    );
    assert_eq!(second_body["error"], "invalid_token");
}

#[tokio::test]
async fn a_revoked_token_cannot_enroll() {
    let app = service();
    let (token, token_id) = mint(&app, "site-revoked").await;

    let (revoke_status, _body) =
        call_as_admin(&app, "DELETE", &format!("/v1alpha1/enrollmenttokens/{token_id}"), None).await;
    assert_eq!(revoke_status, StatusCode::NO_CONTENT, "a grid-admin can revoke a token");

    let (status, body) = enroll(&app, &token, &plain_csr()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "a revoked token is unusable");
    assert_eq!(body["error"], "invalid_token");
}

#[tokio::test]
async fn a_redeemed_token_cannot_be_revoked() {
    let app = service();
    let (token, token_id) = mint(&app, "site-redeemed").await;

    let (enroll_status, _issued) = enroll(&app, &token, &plain_csr()).await;
    assert_eq!(enroll_status, StatusCode::CREATED, "the token redeems");

    // Revoke is a pre-redemption kill switch. A redeemed token's row stays as
    // issuance provenance, so revoking it reports not found rather than deleting.
    let (status, body) = call_as_admin(&app, "DELETE", &format!("/v1alpha1/enrollmenttokens/{token_id}"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "a redeemed token is no longer revocable");
    assert_eq!(body["error"], "not_found");
}

#[tokio::test]
async fn an_expiry_outside_one_second_to_seven_days_is_refused() {
    let app = service();
    for secs in [0, 604_801] {
        let (status, body) = call_as_admin(
            &app,
            "POST",
            "/v1alpha1/enrollmenttokens",
            Some(json!({ "siteName": "site-ttl", "gridNetworkRef": "demo-grid", "expiresInSecs": secs })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{secs}s is refused, not defaulted");
        assert_eq!(body["error"], "invalid_token_ttl", "{secs}s");
    }
}

#[tokio::test]
async fn minting_requires_a_grid_admin() {
    let app = service();
    let (status, body) = call(
        &app,
        "POST",
        "/v1alpha1/enrollmenttokens",
        Some(json!({ "siteName": "site-x", "gridNetworkRef": "demo-grid" })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "minting a name pin is not self-service"
    );
    assert_eq!(body["error"], "unauthorized");
}

#[tokio::test]
async fn an_unknown_grid_admin_mints_nothing() {
    let app = service();
    let (status, _body) = send(
        &app,
        "POST",
        "/v1alpha1/enrollmenttokens",
        Some(json!({ "siteName": "site-x", "gridNetworkRef": "demo-grid" })),
        &[("authorization", "Bearer not-the-token".to_owned())],
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "an unknown grid-admin token mints nothing"
    );
}

#[tokio::test]
async fn a_bad_pin_is_refused_at_mint() {
    let app = service();
    let (status, body) = call_as_admin(
        &app,
        "POST",
        "/v1alpha1/enrollmenttokens",
        Some(json!({ "siteName": "Site-D", "gridNetworkRef": "demo-grid" })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a bad pin fails at mint, not at enroll"
    );
    assert_eq!(body["error"], "invalid_site_name");
}

#[tokio::test]
async fn the_certificate_carries_the_pinned_name_not_one_the_csr_asked_for() {
    let app = service();
    let (token, _id) = mint(&app, "site-d").await;
    let csr = csr_asking_for(&[SanType::URI(
        "spiffe://grid.internal/site/site-a".to_owned().try_into().expect("ia5"),
    )]);

    let (_status, issued) = enroll(&app, &token, &csr).await;
    assert_eq!(
        issued["spiffeId"], "spiffe://grid.internal/site/site-d",
        "a CSR asking to be site-a must still receive the pinned name"
    );
}

#[tokio::test]
async fn a_held_name_cannot_be_minted() {
    let app = service();
    let (first_token, _) = mint(&app, "site-d").await;
    let (first_status, _first) = enroll(&app, &first_token, &plain_csr()).await;
    assert_eq!(first_status, StatusCode::CREATED);

    let (status, body) = mint_status(&app, "site-d").await;
    assert_eq!(status, StatusCode::CONFLICT, "an enrollment holds the name");
    assert_eq!(body["error"], "name_taken");
}

#[tokio::test]
async fn a_name_holds_one_live_token() {
    let app = service();
    let (_token, token_id) = mint(&app, "site-d").await;

    let (status, body) = mint_status(&app, "site-d").await;
    assert_eq!(status, StatusCode::CONFLICT, "a live token already pins the name");
    assert_eq!(body["error"], "token_outstanding");

    let (revoked, _) = call_as_admin(&app, "DELETE", &format!("/v1alpha1/enrollmenttokens/{token_id}"), None).await;
    assert_eq!(revoked, StatusCode::NO_CONTENT);
    let (reminted, _body) = mint_status(&app, "site-d").await;
    assert_eq!(
        reminted,
        StatusCode::CREATED,
        "revoking the live token frees the name to mint"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_mints_for_one_name_leave_one_live_token() {
    let app = service();
    let racers: Vec<_> = std::iter::repeat_with(|| {
        let app = app.clone();
        tokio::spawn(async move { mint_status(&app, "site-race").await })
    })
    .take(16)
    .collect();
    let mut created = 0_usize;
    for racer in racers {
        let (status, body) = racer.await.expect("join");
        if status == StatusCode::CREATED {
            created = created.saturating_add(1);
        } else {
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
            assert_eq!(body["error"], "token_outstanding");
        }
    }
    assert_eq!(created, 1, "exactly one racing mint wins the name");
}

#[tokio::test]
async fn an_expired_token_frees_the_name_but_a_live_one_holds_it() {
    let store = Store::memory();
    let mut expired = new_token("site-x", "expired-token");
    expired.expires_at = time::OffsetDateTime::now_utc().saturating_sub(time::Duration::seconds(1));
    store.mint_site_token(expired).await.expect("mint expired");
    let app = service_reserving(store, "hub");

    let (first, body) = mint_status(&app, "site-x").await;
    assert_eq!(first, StatusCode::CREATED, "an expired token holds nothing: {body}");
    let (second, refused) = mint_status(&app, "site-x").await;
    assert_eq!(second, StatusCode::CONFLICT, "the fresh token does");
    assert_eq!(refused["error"], "token_outstanding");
}

#[tokio::test]
async fn a_spelling_variant_does_not_mint_a_second_token_for_a_name() {
    let app = service();
    let _live = mint(&app, "site-d").await;
    for variant in ["Site-D", "SITE-D", " site-d", "site-d ", "site-d\n", "site\u{2010}d"] {
        let (status, body) = mint_status(&app, variant).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{variant:?} is not a second name: {body}"
        );
        assert_eq!(body["error"], "invalid_site_name", "{variant:?}");
    }
}

/// A token whose name another record took cannot claim it once that record is deleted.
#[tokio::test]
async fn a_token_that_loses_its_name_cannot_claim_it_after_a_delete() {
    let store = Store::memory();
    let token = "loser-token";
    store.mint_site_token(new_token("east", token)).await.expect("mint");
    // A record with no token takes the name, the only way after mint refuses a held one.
    store
        .seed_reserved(&SeedRecord {
            site_name: "east".to_owned(),
            key_sha256: "e".repeat(64),
            generation: 1,
            issued_at: time::OffsetDateTime::now_utc(),
        })
        .await
        .expect("seed");
    let app = service_reserving(store, "hub");

    let (status, body) = enroll(&app, token, &plain_csr()).await;
    assert_eq!(status, StatusCode::CONFLICT, "the name is held");
    assert_eq!(body["error"], "name_taken");

    let (deleted, _) = call_as_admin(&app, "DELETE", "/v1alpha1/enrollments/east", None).await;
    assert_eq!(deleted, StatusCode::NO_CONTENT, "the holder is deleted");
    let (replayed, replay) = enroll(&app, token, &plain_csr()).await;
    assert_eq!(
        replayed,
        StatusCode::CONFLICT,
        "the deleted name refuses the losing token"
    );
    assert_eq!(replay["error"], "name_deleted");
}

#[tokio::test]
async fn a_malformed_csr_is_refused_on_enroll() {
    let app = service();
    let (token, _id) = mint(&app, "site-d").await;

    let (status, body) = enroll(&app, &token, "not a csr").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "a malformed request is refused");
    assert_eq!(body["error"], "invalid_csr");

    // The CSR is verified before the token is touched, so a bad one cannot spend it.
    let (retry, _) = enroll(&app, &token, &plain_csr()).await;
    assert_eq!(retry, StatusCode::CREATED, "a malformed CSR does not spend the token");
}

#[tokio::test]
async fn an_rsa_key_is_refused_on_enroll_without_spending_the_token() {
    let app = service();
    let (token, _id) = mint(&app, "site-d").await;
    for rsa in [
        include_str!("../../certs/tests/fixtures/requests/rsa-1024.csr"),
        include_str!("../../certs/tests/fixtures/requests/rsa-2048.csr"),
    ] {
        let (status, body) = enroll(&app, &token, rsa).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"], "invalid_csr");
    }
    let (retry, _) = enroll(&app, &token, &plain_csr()).await;
    assert_eq!(retry, StatusCode::CREATED, "the token is still unspent");
}

/// An issued site certificate must not be a CA, or a compromised site could mint
/// sub-certificates for names it was never granted.
#[tokio::test]
async fn an_issued_site_cert_is_not_a_ca() {
    let app = service();
    let (token, _id) = mint(&app, "site-leaf").await;

    let (_status, issued) = enroll(&app, &token, &plain_csr()).await;
    let pem = issued["certificate"].as_str().expect("certificate");
    let (_rest, block) = x509_parser::pem::parse_x509_pem(pem.as_bytes()).expect("pem");
    let cert = block.parse_x509().expect("x509");
    let is_ca = cert.basic_constraints().ok().flatten().is_some_and(|ext| ext.value.ca);
    assert!(!is_ca, "an issued site cert must not be a CA");
}

/// Liveness and readiness both report OK against the in-memory store.
#[tokio::test]
async fn health_and_readiness_report_ok() {
    let app = service();

    let (health, _) = call(&app, "GET", "/healthz", None).await;
    assert_eq!(health, StatusCode::OK, "liveness is up");

    let (ready, _) = call(&app, "GET", "/readyz", None).await;
    assert_eq!(ready, StatusCode::OK, "the in-memory store is always ready");
}

#[tokio::test]
async fn the_ca_endpoint_is_gone() {
    let app = service();
    let (status, _body) = call(&app, "GET", "/v1alpha1/ca", None).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "the CA rides back in the enroll response, so there is no standalone CA endpoint"
    );
}

#[tokio::test]
async fn a_reserved_name_cannot_be_minted() {
    let app = service_reserving(Store::memory(), "hub");
    let (status, body) = call_as_admin(
        &app,
        "POST",
        "/v1alpha1/enrollmenttokens",
        Some(json!({ "siteName": "hub", "gridNetworkRef": "demo-grid" })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "the hub's name is issued outside enrollment"
    );
    assert_eq!(body["error"], "name_taken");
}

#[tokio::test]
async fn a_token_minted_before_the_reservation_cannot_claim_it() {
    let store = Store::memory();
    let token = "pre-reservation-token";
    store.mint_site_token(new_token("hub", token)).await.expect("mint");
    let app = service_reserving(store, "hub");

    let (status, body) = enroll(&app, token, &plain_csr()).await;
    assert_eq!(status, StatusCode::CONFLICT, "no second identity for the hub");
    assert_eq!(body["error"], "name_taken");
    let (replayed, replay) = enroll(&app, token, &plain_csr()).await;
    assert_eq!(replayed, StatusCode::CONFLICT, "the token stays unspent and refused");
    assert_eq!(replay["error"], "name_taken");
}

/// A store-level token pinning `site`, presented as `token`.
fn new_token(site: &str, token: &str) -> NewSiteToken {
    NewSiteToken {
        token_id: None,
        token_sha256: certs::sha256(token.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        public_key_sha256: None,
        site_name: site.to_owned(),
        grid_network_ref: "demo-grid".to_owned(),
        issued_by: "tester".to_owned(),
        expires_at: time::OffsetDateTime::now_utc().saturating_add(time::Duration::hours(1)),
        allow_deleted_name: false,
    }
}

/// A CSR for `key`, so a test can present one key more than once.
fn csr_for(key: &KeyPair) -> String {
    CertificateParams::default()
        .serialize_request(key)
        .expect("csr")
        .pem()
        .expect("pem")
}

/// Mint a token for `site` that opts in to a deleted name, returning the token.
async fn mint_deleted(app: &axum::Router, site: &str) -> String {
    let (status, body) = call_as_admin(
        app,
        "POST",
        "/v1alpha1/enrollmenttokens",
        Some(json!({ "siteName": site, "gridNetworkRef": "demo-grid", "allowDeletedName": true })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body["token"].as_str().expect("token").to_owned()
}

/// Delete `site`'s enrollment as a grid-admin.
async fn delete(app: &axum::Router, site: &str) {
    let (status, body) = call_as_admin(app, "DELETE", &format!("/v1alpha1/enrollments/{site}"), None).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
}

#[tokio::test]
async fn a_deleted_sites_key_cannot_enroll_again_under_any_name() {
    let app = service();
    let key = KeyPair::generate().expect("key");
    let (token, _) = mint(&app, "site-a").await;
    let (enrolled, _) = enroll(&app, &token, &csr_for(&key)).await;
    assert_eq!(enrolled, StatusCode::CREATED);
    delete(&app, "site-a").await;

    let (other, _) = mint(&app, "site-b").await;
    let (reused, body) = enroll(&app, &other, &csr_for(&key)).await;
    assert_eq!(reused, StatusCode::CONFLICT, "the key under another name: {body}");
    assert_eq!(body["error"], "key_reused");

    let same = mint_deleted(&app, "site-a").await;
    let (again, renamed) = enroll(&app, &same, &csr_for(&key)).await;
    assert_eq!(again, StatusCode::CONFLICT, "the key under its old name: {renamed}");
    assert_eq!(renamed["error"], "key_reused");

    let (fresh, _) = enroll(&app, &other, &plain_csr()).await;
    assert_eq!(
        fresh,
        StatusCode::CREATED,
        "a key_reused refusal leaves the token unspent"
    );
}

#[tokio::test]
async fn a_live_sites_key_cannot_enroll_a_second_name() {
    let app = service();
    let key = KeyPair::generate().expect("key");
    let (first, _) = mint(&app, "site-a").await;
    let (second, _) = mint(&app, "site-b").await;
    let (enrolled, _) = enroll(&app, &first, &csr_for(&key)).await;
    assert_eq!(enrolled, StatusCode::CREATED);
    let (reused, body) = enroll(&app, &second, &csr_for(&key)).await;
    assert_eq!(reused, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"], "key_reused");
}

#[tokio::test]
async fn a_deleted_name_enrolls_again_only_with_the_opt_in() {
    let app = service();
    let (token, _) = mint(&app, "site-a").await;
    let (enrolled, _) = enroll(&app, &token, &plain_csr()).await;
    assert_eq!(enrolled, StatusCode::CREATED);
    delete(&app, "site-a").await;

    let (refused, body) = call_as_admin(
        &app,
        "POST",
        "/v1alpha1/enrollmenttokens",
        Some(json!({ "siteName": "site-a", "gridNetworkRef": "demo-grid" })),
    )
    .await;
    assert_eq!(refused, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"], "name_deleted");
    let (explicit_false, declined) = call_as_admin(
        &app,
        "POST",
        "/v1alpha1/enrollmenttokens",
        Some(json!({ "siteName": "site-a", "gridNetworkRef": "demo-grid", "allowDeletedName": false })),
    )
    .await;
    assert_eq!(explicit_false, StatusCode::CONFLICT, "{declined}");

    let opted_in = mint_deleted(&app, "site-a").await;
    let (reenrolled, issued) = enroll(&app, &opted_in, &plain_csr()).await;
    assert_eq!(reenrolled, StatusCode::CREATED, "{issued}");
    assert_eq!(issued["spiffeId"], "spiffe://grid.internal/site/site-a");
}

#[tokio::test]
async fn a_token_minted_before_the_delete_cannot_claim_the_deleted_name() {
    let store = Store::memory();
    let waiting = "waiting-token";
    store.mint_site_token(new_token("site-a", waiting)).await.expect("mint");
    // A record with no token holds the name, the only way after mint refuses a held one.
    store
        .seed_reserved(&SeedRecord {
            site_name: "site-a".to_owned(),
            key_sha256: "a".repeat(64),
            generation: 1,
            issued_at: time::OffsetDateTime::now_utc(),
        })
        .await
        .expect("seed");
    let app = service_reserving(store, "hub");
    delete(&app, "site-a").await;

    let (claimed, body) = enroll(&app, waiting, &plain_csr()).await;
    assert_eq!(claimed, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"], "name_deleted");
    let (replayed, _body) = enroll(&app, waiting, &plain_csr()).await;
    assert_eq!(replayed, StatusCode::UNAUTHORIZED, "the refusal spent the token");
    let opted_in = mint_deleted(&app, "site-a").await;
    let (reenrolled, issued) = enroll(&app, &opted_in, &plain_csr()).await;
    assert_eq!(reenrolled, StatusCode::CREATED, "{issued}");
}

#[tokio::test]
async fn a_spelling_variant_of_a_deleted_name_is_not_a_new_name() {
    let app = service();
    let (token, _) = mint(&app, "site-a").await;
    let (enrolled, _) = enroll(&app, &token, &plain_csr()).await;
    assert_eq!(enrolled, StatusCode::CREATED);
    delete(&app, "site-a").await;
    for variant in ["Site-A", "SITE-A", " site-a", "site-a ", "site-a\n"] {
        let (status, body) = call_as_admin(
            &app,
            "POST",
            "/v1alpha1/enrollmenttokens",
            Some(json!({ "siteName": variant, "gridNetworkRef": "demo-grid" })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{variant:?}: {body}");
        assert_eq!(body["error"], "invalid_site_name", "{variant:?}");
    }
}

/// A table with a default grid-admin, a full admin, and a deleter-only entry.
const ROLE_TABLE: &str = "\
ops:t0ken
root:full-token
security:delete-token
";

const ROLES: &str = "\
root:grid-admin,enrollment-admin
security:enrollment-admin
";

fn service_with_roles() -> axum::Router {
    let ca = certs::generate_ca("test-grid-ca").expect("ca");
    router(Arc::new(AppState {
        store: Store::memory(),
        ca: SharedCa::new(ca),
        authorizer: Authorizer::Local(
            GridAdmins::from_table(ROLE_TABLE)
                .expect("token table")
                .with_roles(ROLES)
                .expect("roles"),
        ),
        cert_lifetime: certs::DEFAULT_SITE_CERT_LIFETIME,
        reserved_sites: Vec::new(),
        renewals_enabled: true,
        #[cfg(feature = "csr-record")]
        recorder: None,
    }))
}

async fn call_with(app: &axum::Router, bearer: &str, method: &str, path: &str, body: Option<Value>) -> StatusCode {
    send(
        app,
        method,
        path,
        body,
        &[("authorization", format!("Bearer {bearer}"))],
    )
    .await
    .0
}

/// Mint and enroll `site` as the default grid-admin.
async fn enrolled(app: &axum::Router, site: &str) {
    let (token, _token_id) = mint(app, site).await;
    let (status, _issued) = enroll(app, &token, &plain_csr()).await;
    assert_eq!(status, StatusCode::CREATED, "{site} enrolls");
}

#[tokio::test]
async fn a_grid_admin_without_enrollment_admin_cannot_delete_an_enrollment() {
    let app = service_with_roles();
    enrolled(&app, "site-kept").await;

    let (refused, body) = call_as_admin(&app, "DELETE", "/v1alpha1/enrollments/site-kept", None).await;
    assert_eq!(refused, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"], "forbidden");
    for (method, path) in [
        ("DELETE", "/v1alpha1/enrollments/site-kept/"),
        ("DELETE", "/v1alpha1//enrollments/site-kept"),
        ("DELETE", "/v1alpha1/enrollments/%73ite-kept"),
        ("DELETE", "/V1ALPHA1/enrollments/site-kept"),
        ("DELETE", "/v1alpha1/enrollments/site-kept?force=true"),
        ("PUT", "/v1alpha1/enrollments/site-kept"),
        ("PATCH", "/v1alpha1/enrollments/site-kept"),
        ("POST", "/v1alpha1/enrollments/site-kept"),
    ] {
        let status = call_with(&app, TOKEN, method, path, None).await;
        assert!(
            status.is_client_error() && status != StatusCode::NO_CONTENT,
            "{method} {path}: {status}"
        );
    }
    let (read, record) = call_as_admin(&app, "GET", "/v1alpha1/enrollments/site-kept", None).await;
    assert_eq!(
        read,
        StatusCode::OK,
        "a grid-admin still reads, and the record survived: {record}"
    );
}

#[tokio::test]
async fn a_default_entry_keeps_mint_revoke_and_read() {
    let app = service_with_roles();
    let (_token, token_id) = mint(&app, "site-ops").await;
    let status = call_with(
        &app,
        TOKEN,
        "DELETE",
        &format!("/v1alpha1/enrollmenttokens/{token_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "a default entry revokes");
    enrolled(&app, "site-read").await;
    let read = call_with(&app, TOKEN, "GET", "/v1alpha1/enrollments/site-read", None).await;
    assert_eq!(read, StatusCode::OK, "a default entry reads enrollments");
}

#[tokio::test]
async fn an_enrollment_admin_can_delete_an_enrollment() {
    let app = service_with_roles();
    enrolled(&app, "site-a").await;
    enrolled(&app, "site-b").await;
    assert_eq!(
        call_with(&app, "full-token", "DELETE", "/v1alpha1/enrollments/site-a", None).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call_with(&app, "delete-token", "DELETE", "/v1alpha1/enrollments/site-b", None).await,
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn an_enrollment_admin_alone_cannot_mint_or_revoke() {
    let app = service_with_roles();
    let (_token, token_id) = mint(&app, "site-z").await;
    let mint_body = json!({ "siteName": "site-y", "gridNetworkRef": "demo-grid" });
    assert_eq!(
        call_with(
            &app,
            "delete-token",
            "POST",
            "/v1alpha1/enrollmenttokens",
            Some(mint_body)
        )
        .await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call_with(
            &app,
            "delete-token",
            "DELETE",
            &format!("/v1alpha1/enrollmenttokens/{token_id}"),
            None
        )
        .await,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn re_admitting_a_deleted_name_needs_the_right_to_delete() {
    let app = service_with_roles();
    enrolled(&app, "site-gone").await;
    assert_eq!(
        call_with(&app, "full-token", "DELETE", "/v1alpha1/enrollments/site-gone", None).await,
        StatusCode::NO_CONTENT
    );
    let readmit = json!({ "siteName": "site-gone", "gridNetworkRef": "demo-grid", "allowDeletedName": true });
    assert_eq!(
        call_with(&app, TOKEN, "POST", "/v1alpha1/enrollmenttokens", Some(readmit.clone())).await,
        StatusCode::FORBIDDEN,
        "a grid-admin alone cannot undo a delete"
    );
    assert_eq!(
        call_with(&app, "full-token", "POST", "/v1alpha1/enrollmenttokens", Some(readmit)).await,
        StatusCode::CREATED,
        "grid-admin with enrollment-admin re-admits the name"
    );
}
