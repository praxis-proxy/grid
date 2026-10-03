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
use enrollment::{AppState, GridAdmins, NewSiteToken, SharedCa, Store, authz::Authorizer, router};
use http_body_util::BodyExt as _;
use rcgen::{CertificateParams, DnType, KeyPair, SanType};
use serde_json::{Value, json};
use tower::ServiceExt as _;

/// The grid-admin credential the tests mint with.
const TOKEN: &str = "t0ken";

/// A service with a fresh CA, an empty store, and one grid-admin.
fn service() -> axum::Router {
    let ca = certs::generate_ca("test-grid-ca").expect("ca");
    router(Arc::new(AppState {
        store: Store::memory(),
        ca: SharedCa::new(ca),
        authorizer: Authorizer::Local(GridAdmins::from_table("tester: t0ken\n")),
        cert_lifetime: certs::DEFAULT_SITE_CERT_LIFETIME,
        reserved_sites: Vec::new(),
    }))
}

/// A service over `store` that reserves `reserved`, the hub's own name.
fn service_reserving(store: Store, reserved: &str) -> axum::Router {
    let ca = certs::generate_ca("test-grid-ca").expect("ca");
    router(Arc::new(AppState {
        store,
        ca: SharedCa::new(ca),
        authorizer: Authorizer::Local(GridAdmins::from_table("tester: t0ken\n")),
        cert_lifetime: certs::DEFAULT_SITE_CERT_LIFETIME,
        reserved_sites: vec![reserved.to_owned()],
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

/// Mint a token pinning `site`, returning the token and its id.
async fn mint(app: &axum::Router, site: &str) -> (String, String) {
    let (status, body) = call_as_admin(
        app,
        "POST",
        "/v1alpha1/enrollmenttokens",
        Some(json!({ "siteName": site, "gridNetworkRef": "demo-grid" })),
    )
    .await;
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
async fn two_sites_cannot_hold_one_name() {
    let app = service();
    let (first_token, _) = mint(&app, "site-d").await;
    let (first_status, _first) = enroll(&app, &first_token, &plain_csr()).await;
    assert_eq!(first_status, StatusCode::CREATED);

    let (second_token, _) = mint(&app, "site-d").await;
    let (second_status, second_body) = enroll(&app, &second_token, &plain_csr()).await;
    assert_eq!(second_status, StatusCode::CONFLICT, "the name is already held");
    assert_eq!(second_body["error"], "name_taken");
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
    let token_sha256: String = certs::sha256(token.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    store
        .mint_site_token(NewSiteToken {
            token_sha256,
            site_name: "hub".to_owned(),
            grid_network_ref: "demo-grid".to_owned(),
            issued_by: "tester".to_owned(),
            expires_at: time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        })
        .await
        .expect("mint");
    let app = service_reserving(store, "hub");

    let (status, body) = enroll(&app, token, &plain_csr()).await;
    assert_eq!(status, StatusCode::CONFLICT, "no second identity for the hub");
    assert_eq!(body["error"], "name_taken");
}
