//! Renewing a site identity with its current certificate.

#![expect(
    clippy::tests_outside_test_module,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "integration tests live in tests/, and serde_json::Value indexing yields Null rather than panicking"
)]

use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use enrollment::{AppState, GridAdmins, SeedRecord, SharedCa, Store, api::PeerLeaf, authz::Authorizer, router};
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use time::{Duration, OffsetDateTime};
use tower::ServiceExt as _;

/// A grid: the service, its state, and a copy of its CA to issue leaves outside enrollment.
struct Grid {
    app: axum::Router,
    state: Arc<AppState>,
    ca: certs::CaCert,
}

/// A site's identity: its leaf and the key behind it.
struct Identity {
    cert_pem: String,
    key_pem: String,
}

impl Identity {
    fn der(&self) -> Vec<u8> {
        x509_parser::pem::parse_x509_pem(self.cert_pem.as_bytes())
            .expect("pem")
            .1
            .contents
    }

    fn key_sha256(&self) -> String {
        certs::cert_public_key_sha256(&self.cert_pem).expect("key digest")
    }
}

fn grid(reserved: &[&str]) -> Grid {
    grid_with(reserved, true)
}

fn grid_with(reserved: &[&str], renewals_enabled: bool) -> Grid {
    let ca = certs::generate_ca("test-grid-ca").expect("ca");
    let copy = certs::load_ca("test-grid-ca", &ca.key_pem, &ca.cert_pem).expect("copy");
    let state = Arc::new(AppState {
        store: Store::memory(),
        ca: SharedCa::new(ca),
        authorizer: Authorizer::Local(GridAdmins::from_table("tester: t0ken\n")),
        cert_lifetime: certs::DEFAULT_SITE_CERT_LIFETIME,
        reserved_sites: reserved.iter().map(|site| (*site).to_owned()).collect(),
        renewals_enabled,
    });
    Grid {
        app: router(Arc::clone(&state)),
        state,
        ca: copy,
    }
}

/// A leaf for `site` from `ca`, outside enrollment, as bootstrap issues the hub's.
fn issue(ca: &certs::CaCert, site: &str, validity: certs::Validity) -> Identity {
    let csr = certs::generate_csr(site).expect("csr");
    let cert = certs::sign_csr(ca, site, &csr.csr_pem, validity).expect("sign");
    Identity {
        cert_pem: cert.cert_pem,
        key_pem: csr.key_pem.to_string(),
    }
}

/// Register `identity` as bootstrap would, at `generation`.
async fn seed(grid: &Grid, site: &str, identity: &Identity, generation: u64) {
    let seed = SeedRecord {
        site_name: site.to_owned(),
        key_sha256: identity.key_sha256(),
        generation,
        issued_at: certs::cert_validity(&identity.cert_pem).expect("validity").0,
    };
    grid.state.store.seed_reserved(&seed).await.expect("seed");
}

async fn send(
    app: &axum::Router,
    (method, path): (&str, &str),
    body: &str,
    headers: &[(&str, String)],
    peer: Option<&Identity>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    for (name, value) in headers {
        builder = builder.header(*name, value);
    }
    let mut request = builder.body(Body::from(body.to_owned())).expect("request");
    if let Some(peer) = peer {
        request.extensions_mut().insert(PeerLeaf(Some(Arc::from(peer.der()))));
    }
    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = response.into_body().collect().await.expect("body").to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

fn admin() -> [(&'static str, String); 1] {
    [("authorization", "Bearer t0ken".to_owned())]
}

/// Enroll `site` under a fresh token, returning its identity.
async fn enroll(grid: &Grid, site: &str) -> Identity {
    let mint = json!({ "siteName": site, "gridNetworkRef": "demo-grid" }).to_string();
    let (minted, token) = send(&grid.app, ("POST", "/v1alpha1/enrollmenttokens"), &mint, &admin(), None).await;
    assert_eq!(minted, StatusCode::CREATED, "mint");
    let csr = certs::generate_csr(site).expect("csr");
    let bearer = [(
        "authorization",
        format!("Bearer {}", token["token"].as_str().expect("token")),
    )];
    let body = json!({ "csr": csr.csr_pem }).to_string();
    let (enrolled, issued) = send(&grid.app, ("POST", "/v1alpha1/enrollments"), &body, &bearer, None).await;
    assert_eq!(enrolled, StatusCode::CREATED, "enroll: {issued}");
    Identity {
        cert_pem: issued["certificate"].as_str().expect("certificate").to_owned(),
        key_pem: csr.key_pem.to_string(),
    }
}

/// A renewal's response and the key its request asked for.
struct Reply {
    status: StatusCode,
    body: Value,
    key_pem: String,
}

impl Reply {
    /// The renewed identity.
    fn identity(&self) -> Identity {
        Identity {
            cert_pem: self.body["certificate"].as_str().expect("certificate").to_owned(),
            key_pem: self.key_pem.clone(),
        }
    }
}

/// Renew presenting `peer` with a CSR for a fresh key.
async fn renew(grid: &Grid, site: &str, peer: Option<&Identity>) -> Reply {
    let csr = certs::generate_csr(site).expect("csr");
    let (status, body) = renew_with(grid, peer, &csr.csr_pem).await;
    Reply {
        status,
        body,
        key_pem: csr.key_pem.to_string(),
    }
}

async fn renew_with(grid: &Grid, peer: Option<&Identity>, csr_pem: &str) -> (StatusCode, Value) {
    let body = json!({ "csr": csr_pem }).to_string();
    send(&grid.app, ("POST", "/v1alpha1/rotations"), &body, &[], peer).await
}

/// A CSR for the key in `key_pem`.
fn csr_for(key_pem: &str) -> String {
    let key = rcgen::KeyPair::from_pem(key_pem).expect("key");
    rcgen::CertificateParams::default()
        .serialize_request(&key)
        .expect("csr")
        .pem()
        .expect("pem")
}

#[tokio::test]
async fn a_site_renews_with_its_current_certificate() {
    let grid = grid(&[]);
    let first = enroll(&grid, "site-a").await;

    let renewal = renew(&grid, "site-a", Some(&first)).await;
    assert_eq!(
        renewal.status,
        StatusCode::OK,
        "the current certificate renews: {}",
        renewal.body
    );
    assert_eq!(renewal.body["spiffeId"], certs::spiffe_id("site-a"));
    assert_eq!(
        renewal.body["caCertificate"],
        grid.ca.cert_pem.as_str(),
        "the CA rides back"
    );
    let second = renewal.identity();
    certs::verify_site_cert(&grid.ca.cert_pem, &second.cert_pem, "site-a").expect("same site, same CA");
    assert_ne!(second.key_sha256(), first.key_sha256(), "a new key");

    let again = renew(&grid, "site-a", Some(&second)).await;
    assert_eq!(again.status, StatusCode::OK, "the renewed certificate renews in turn");
}

#[tokio::test]
async fn a_grid_with_renewals_off_refuses_every_renewal_and_keeps_enrolling() {
    let grid = grid_with(&[], false);
    let first = enroll(&grid, "site-a").await;
    let renewal = renew(&grid, "site-a", Some(&first)).await;
    assert_eq!(renewal.status, StatusCode::SERVICE_UNAVAILABLE, "{}", renewal.body);
    assert_eq!(renewal.body["error"], "rotation_disabled");
}

#[tokio::test]
async fn a_grid_admin_reads_an_enrollment_record_without_key_material() {
    let grid = grid(&[]);
    let path = "/v1alpha1/enrollments/site-a";
    let (missing, body) = send(&grid.app, ("GET", path), "", &admin(), None).await;
    assert_eq!(
        (missing, body["error"].clone()),
        (StatusCode::NOT_FOUND, json!("not_found"))
    );

    let first = enroll(&grid, "site-a").await;
    let (read, fresh) = send(&grid.app, ("GET", path), "", &admin(), None).await;
    assert_eq!(read, StatusCode::OK, "{fresh}");
    assert_eq!(fresh["state"], "active");
    assert_eq!(fresh["publicKeySha256"], first.key_sha256());
    assert!(fresh.get("rotatedAt").is_none(), "never renewed: {fresh}");
    assert!(fresh["notAfter"].is_string(), "the issued certificate's expiry");

    let second = renew(&grid, "site-a", Some(&first)).await.identity();
    let (_read, renewed) = send(&grid.app, ("GET", path), "", &admin(), None).await;
    assert_eq!(renewed["publicKeySha256"], second.key_sha256());
    assert_eq!(renewed["previousPublicKeySha256"], first.key_sha256());
    assert!(renewed["rotatedAt"].is_string());
    let text = renewed.to_string();
    assert!(!text.contains("BEGIN"), "no certificate or key material: {text}");

    let (anonymous, _) = send(&grid.app, ("GET", path), "", &[], None).await;
    assert_eq!(anonymous, StatusCode::UNAUTHORIZED, "reading needs a grid-admin");
}

#[tokio::test]
async fn a_disabled_renewal_says_when_to_retry() {
    let grid = grid_with(&[], false);
    let first = enroll(&grid, "site-a").await;
    let csr = certs::generate_csr("site-a").expect("csr");
    let mut request = Request::builder()
        .method("POST")
        .uri("/v1alpha1/rotations")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "csr": csr.csr_pem }).to_string()))
        .expect("request");
    request.extensions_mut().insert(PeerLeaf(Some(Arc::from(first.der()))));
    let response = grid.app.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("300")
    );
}

#[tokio::test]
async fn a_lost_response_retries_and_the_replaced_key_cannot_pick_another() {
    let grid = grid(&[]);
    let first = enroll(&grid, "site-a").await;
    let renewal = renew(&grid, "site-a", Some(&first)).await;
    assert_eq!(renewal.status, StatusCode::OK);
    let current = renewal.identity();

    let (retried, body) = renew_with(&grid, Some(&first), &csr_for(&current.key_pem)).await;
    assert_eq!(retried, StatusCode::OK, "a lost response retries: {body}");
    assert_eq!(
        certs::cert_public_key_sha256(body["certificate"].as_str().expect("certificate")),
        Ok(current.key_sha256()),
        "re-signed for the key already recorded"
    );
}

#[tokio::test]
#[expect(clippy::too_many_lines, reason = "a fork, the freeze it leaves, and the recovery")]
async fn a_fork_freezes_the_site_until_a_grid_admin_deletes_its_enrollment() {
    let grid = grid(&[]);
    let first = enroll(&grid, "site-a").await;
    let current = renew(&grid, "site-a", Some(&first)).await.identity();

    let forked = renew(&grid, "site-a", Some(&first)).await;
    assert_eq!(
        forked.status,
        StatusCode::FORBIDDEN,
        "the replaced key asks for another key"
    );
    let frozen = renew(&grid, "site-a", Some(&current)).await;
    assert_eq!(
        frozen.status,
        StatusCode::FORBIDDEN,
        "now even the current key is refused"
    );

    let path = "/v1alpha1/enrollments/site-a";
    let (_read, record) = send(&grid.app, ("GET", path), "", &admin(), None).await;
    assert_eq!(record["state"], "frozen", "a grid-admin sees the freeze: {record}");
    let (anonymous, _) = send(&grid.app, ("DELETE", path), "", &[], None).await;
    assert_eq!(anonymous, StatusCode::UNAUTHORIZED, "deleting needs a grid-admin");
    let (deleted, _) = send(&grid.app, ("DELETE", path), "", &admin(), None).await;
    assert_eq!(deleted, StatusCode::NO_CONTENT, "a grid-admin deletes the enrollment");
    let (again, _) = send(&grid.app, ("DELETE", path), "", &admin(), None).await;
    assert_eq!(again, StatusCode::NOT_FOUND, "nothing left to delete");

    let gone = renew(&grid, "site-a", Some(&current)).await;
    assert_eq!(gone.status, StatusCode::FORBIDDEN, "no record, no renewal");
    let reenrolled = enroll(&grid, "site-a").await;
    assert_eq!(
        renew(&grid, "site-a", Some(&reenrolled)).await.status,
        StatusCode::OK,
        "the released name re-enrolls and renews"
    );
}

#[tokio::test]
async fn renewal_without_a_usable_certificate_is_refused() {
    let grid = grid(&[]);
    let enrolled = enroll(&grid, "site-a").await;
    let now = OffsetDateTime::now_utc();

    let anonymous = renew(&grid, "site-a", None).await;
    assert_eq!(anonymous.status, StatusCode::UNAUTHORIZED, "no certificate");
    assert_eq!(anonymous.body["error"], "identity_required");

    let expired = issue(
        &grid.ca,
        "site-a",
        certs::Validity {
            not_before: now.saturating_sub(Duration::days(40)),
            not_after: now.saturating_sub(Duration::days(10)),
        },
    );
    let lapsed = renew(&grid, "site-a", Some(&expired)).await;
    assert_eq!(lapsed.status, StatusCode::UNAUTHORIZED, "an expired certificate");

    let other_ca = certs::generate_ca("test-grid-ca").expect("other ca");
    let foreign = issue(&other_ca, "site-a", certs::Validity::default());
    let outsider = renew(&grid, "site-a", Some(&foreign)).await;
    assert_eq!(outsider.status, StatusCode::UNAUTHORIZED, "another grid's certificate");

    let (reused, body) = renew_with(&grid, Some(&enrolled), &csr_for(&enrolled.key_pem)).await;
    assert_eq!(reused, StatusCode::FORBIDDEN, "a request for the presented key: {body}");
}

#[tokio::test]
async fn an_unauthenticated_renewal_is_refused_before_its_body_is_read() {
    let grid = grid(&[]);
    let (status, body) = send(&grid.app, ("POST", "/v1alpha1/rotations"), "not json", &[], None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "refused, not a body error: {body}");
}

#[tokio::test]
async fn a_valid_leaf_without_a_record_is_refused() {
    let grid = grid(&[]);
    let unrecorded = issue(&grid.ca, "ghost", certs::Validity::default());
    let refused = renew(&grid, "ghost", Some(&unrecorded)).await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN, "only an enrolled name renews");
    assert_eq!(refused.body["error"], "identity_refused");
}

#[tokio::test]
async fn a_reserved_name_renews_only_once_bootstrap_seeded_it() {
    let grid = grid(&["hub"]);
    let bootstrap = issue(&grid.ca, "hub", certs::Validity::default());
    let unseeded = renew(&grid, "hub", Some(&bootstrap)).await;
    assert_eq!(unseeded.status, StatusCode::FORBIDDEN, "no seed, no record");

    seed(&grid, "hub", &bootstrap, 1).await;
    let renewed = renew(&grid, "hub", Some(&bootstrap)).await;
    assert_eq!(
        renewed.status,
        StatusCode::OK,
        "the seeded identity renews: {}",
        renewed.body
    );
    let current = renewed.identity();
    assert_eq!(
        renew(&grid, "hub", Some(&current)).await.status,
        StatusCode::OK,
        "then renews like any site"
    );
}

#[tokio::test]
async fn a_newer_seed_resets_a_reserved_record_and_an_older_one_is_ignored() {
    let grid = grid(&["hub"]);
    let first = issue(&grid.ca, "hub", certs::Validity::default());
    seed(&grid, "hub", &first, 5).await;
    let renewed = renew(&grid, "hub", Some(&first)).await.identity();

    let reissued = issue(&grid.ca, "hub", certs::Validity::default());
    seed(&grid, "hub", &reissued, 4).await;
    assert_eq!(
        renew(&grid, "hub", Some(&reissued)).await.status,
        StatusCode::FORBIDDEN,
        "an older generation changes nothing"
    );
    seed(&grid, "hub", &reissued, 6).await;
    assert_eq!(
        renew(&grid, "hub", Some(&reissued)).await.status,
        StatusCode::OK,
        "a newer generation resets the record to bootstrap's key"
    );
    assert_eq!(
        renew(&grid, "hub", Some(&renewed)).await.status,
        StatusCode::FORBIDDEN,
        "the key the reset replaced no longer renews"
    );
}

#[tokio::test]
async fn a_reserved_enrollment_is_not_deleted_here() {
    let grid = grid(&["hub"]);
    let (status, body) = send(&grid.app, ("DELETE", "/v1alpha1/enrollments/hub"), "", &admin(), None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"], "reserved_site");
}

#[tokio::test]
async fn a_seed_for_a_site_that_is_not_reserved_is_refused_and_warned_once() {
    let grid = grid(&["hub"]);
    let spoke = issue(&grid.ca, "east", certs::Validity::default());
    let dir = std::env::temp_dir().join(format!("seeds-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).expect("dir");
    let record = SeedRecord {
        site_name: "east".to_owned(),
        key_sha256: spoke.key_sha256(),
        generation: 1,
        issued_at: OffsetDateTime::now_utc(),
    };
    let (body, signature) = enrollment::seed::sign(&record, &grid.ca).expect("sign");
    std::fs::write(dir.join("east.seed"), body).expect("seed");
    std::fs::write(dir.join("east.sig"), signature).expect("sig");

    let mut reported = HashMap::new();
    let first = enrollment::seed::apply(&grid.state, &dir, &mut reported).await;
    assert_eq!(first.len(), 1, "warned: {first:?}");
    assert!(
        first.iter().all(|(_, problem)| problem.contains("not a reserved site")),
        "{first:?}"
    );
    let second = enrollment::seed::apply(&grid.state, &dir, &mut reported).await;
    assert!(second.is_empty(), "the same problem is not warned again");
    assert_eq!(
        renew(&grid, "east", Some(&spoke)).await.status,
        StatusCode::FORBIDDEN,
        "the refused seed created no record"
    );
}

#[tokio::test]
async fn a_leaf_two_renewals_old_still_freezes_the_site() {
    let grid = grid(&[]);
    let stolen = enroll(&grid, "site-a").await;
    let second = renew(&grid, "site-a", Some(&stolen)).await.identity();
    let third = renew(&grid, "site-a", Some(&second)).await.identity();

    let behind = renew(&grid, "site-a", Some(&stolen)).await;
    assert_eq!(
        behind.status,
        StatusCode::FORBIDDEN,
        "a leaf older than the replaced one"
    );
    assert_eq!(
        renew(&grid, "site-a", Some(&third)).await.status,
        StatusCode::FORBIDDEN,
        "it froze the site rather than only refusing itself"
    );
}

/// A leaf for `site` issued `days` ago, valid for 30 days from then.
fn issued_days_ago(grid: &Grid, site: &str, days: i64) -> Identity {
    let start = OffsetDateTime::now_utc().saturating_sub(Duration::days(days));
    issue(
        &grid.ca,
        site,
        certs::Validity {
            not_before: start,
            not_after: start.saturating_add(Duration::days(30)),
        },
    )
}

#[tokio::test]
async fn a_stolen_leaf_from_before_a_recovery_cannot_freeze_the_recovered_site() {
    let grid = grid(&[]);
    let _first = enroll(&grid, "site-a").await;
    let (deleted, _) = send(
        &grid.app,
        ("DELETE", "/v1alpha1/enrollments/site-a"),
        "",
        &admin(),
        None,
    )
    .await;
    assert_eq!(deleted, StatusCode::NO_CONTENT);
    let recovered = enroll(&grid, "site-a").await;

    let stolen = issued_days_ago(&grid, "site-a", 1);
    let refused = renew(&grid, "site-a", Some(&stolen)).await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN, "the stolen leaf is refused");
    assert_eq!(
        renew(&grid, "site-a", Some(&recovered)).await.status,
        StatusCode::OK,
        "and the recovered site still renews"
    );
}

#[tokio::test]
async fn a_hub_leaf_from_before_a_seed_reset_cannot_freeze_the_hub() {
    let grid = grid(&["hub"]);
    let before = issued_days_ago(&grid, "hub", 2);
    seed(&grid, "hub", &before, 1).await;
    let reissued = issue(&grid.ca, "hub", certs::Validity::default());
    seed(&grid, "hub", &reissued, 2).await;

    let refused = renew(&grid, "hub", Some(&before)).await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN, "the pre-reset leaf is refused");
    assert_eq!(
        renew(&grid, "hub", Some(&reissued)).await.status,
        StatusCode::OK,
        "and the re-issued hub still renews"
    );
}

#[tokio::test]
async fn a_hub_seeded_long_after_issue_still_freezes_on_a_fork() {
    let grid = grid(&["hub"]);
    // Bootstrap issued this leaf two hours before the service applied its seed.
    let issued_early = {
        let start = OffsetDateTime::now_utc().saturating_sub(Duration::hours(2));
        issue(
            &grid.ca,
            "hub",
            certs::Validity {
                not_before: start,
                not_after: start.saturating_add(Duration::days(30)),
            },
        )
    };
    seed(&grid, "hub", &issued_early, 1).await;

    let thief = renew(&grid, "hub", Some(&issued_early)).await.identity();
    let displaced = renew(&grid, "hub", Some(&issued_early)).await;
    assert_eq!(displaced.status, StatusCode::FORBIDDEN, "the real hub is refused");
    assert_eq!(
        renew(&grid, "hub", Some(&thief)).await.status,
        StatusCode::FORBIDDEN,
        "and the hub is frozen, so the thief cannot keep renewing"
    );

    seed(&grid, "hub", &thief, 2).await;
    assert_eq!(
        renew(&grid, "hub", Some(&thief)).await.status,
        StatusCode::FORBIDDEN,
        "re-seeding the key the record holds leaves the freeze in place"
    );
}

#[tokio::test]
async fn a_re_seed_of_the_held_key_is_not_applied_again_after_the_hub_renews() {
    let grid = grid(&["hub"]);
    let first = issue(&grid.ca, "hub", certs::Validity::default());
    seed(&grid, "hub", &first, 1).await;
    let current = renew(&grid, "hub", Some(&first)).await.identity();

    // Bootstrap re-signs the kept identity, the key the record already holds.
    seed(&grid, "hub", &current, 2).await;
    let renewed = renew(&grid, "hub", Some(&current)).await;
    assert_eq!(renewed.status, StatusCode::OK, "the hub renews past the re-seed");
    let latest = renewed.identity();

    // The next reapply tick reads the same seed again.
    seed(&grid, "hub", &current, 2).await;
    assert_eq!(
        renew(&grid, "hub", Some(&latest)).await.status,
        StatusCode::OK,
        "the re-seed is not applied again, so the hub keeps renewing"
    );
}

#[tokio::test]
#[expect(clippy::too_many_lines, reason = "apply, roll back, and check the record held")]
async fn a_rolled_back_seed_is_warned_once_and_changes_nothing() {
    let grid = grid(&["hub"]);
    let first = issue(&grid.ca, "hub", certs::Validity::default());
    let second = issue(&grid.ca, "hub", certs::Validity::default());
    let signed = |identity: &Identity, generation| SeedRecord {
        site_name: "hub".to_owned(),
        key_sha256: identity.key_sha256(),
        generation,
        issued_at: certs::cert_validity(&identity.cert_pem).expect("validity").0,
    };
    let dir = std::env::temp_dir().join(format!("seeds-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).expect("dir");
    let mount = |seed: &SeedRecord| {
        let (body, signature) = enrollment::seed::sign(seed, &grid.ca).expect("sign");
        std::fs::write(dir.join("hub.seed"), body).expect("seed");
        std::fs::write(dir.join("hub.sig"), signature).expect("sig");
    };
    let mut reported = HashMap::new();
    mount(&signed(&first, 1));
    assert!(
        enrollment::seed::apply(&grid.state, &dir, &mut reported)
            .await
            .is_empty()
    );
    mount(&signed(&second, 2));
    assert!(
        enrollment::seed::apply(&grid.state, &dir, &mut reported)
            .await
            .is_empty()
    );

    mount(&signed(&first, 1));
    let warned = enrollment::seed::apply(&grid.state, &dir, &mut reported).await;
    assert!(
        warned.iter().any(|(_, problem)| problem.contains("rollback")),
        "{warned:?}"
    );
    assert!(
        enrollment::seed::apply(&grid.state, &dir, &mut reported)
            .await
            .is_empty(),
        "warned once"
    );
    assert_eq!(
        renew(&grid, "hub", Some(&second)).await.status,
        StatusCode::OK,
        "the applied seed still holds"
    );
}
