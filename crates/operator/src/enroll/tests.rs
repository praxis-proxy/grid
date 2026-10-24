use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use axum::{Json, Router, extract::State, routing::post};
use serde_json::{Value, json};

use super::*;

pub(super) const SITE: &str = "site-d";

pub(super) const TOKEN_SECRET: &str = "grid-invite-site-d";

const FAST: Backoff = Backoff {
    initial: Duration::from_millis(1),
    max: Duration::from_millis(5),
};

/// How the mock enrollment service answers.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Reply {
    /// Sign the request for [`SITE`].
    Sign,
    /// Sign for another site.
    WrongSite,
    /// Sign a different key than the one requested.
    OtherKey,
    /// Sign under a CA outside the pin.
    OtherCa,
    /// Refuse the token as spent.
    Spent,
    /// Fail with 503.
    ServerError,
    /// Answer 200 with a body past the cap.
    Huge,
    /// Sign, then append another certificate and junk.
    Padded,
}

/// Mock enrollment service state.
pub(super) struct Mock {
    /// Serves TLS and is the pinned grid CA.
    pub(super) ca: certs::CaCert,
    /// A CA nobody pinned.
    stranger: certs::CaCert,
    /// Scripted reply.
    reply: Reply,
    /// Requests seen.
    calls: AtomicUsize,
}

impl Mock {
    fn new(reply: Reply) -> Arc<Self> {
        crate::init_process_crypto();
        Arc::new(Self {
            ca: certs::generate_ca("grid-ca").expect("ca"),
            stranger: certs::generate_ca("grid-ca").expect("stranger ca"),
            reply,
            calls: AtomicUsize::new(0),
        })
    }

    pub(super) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// Settings pinned to this mock at `port`.
    fn settings(&self, port: u16) -> Settings {
        Settings {
            base: Url::parse(&format!("https://localhost:{port}")).expect("url"),
            pin_pem: self.ca.cert_pem.clone(),
            anchor_pem: self.ca.cert_pem.clone(),
            site_name: SITE.to_owned(),
            token_secret: TOKEN_SECRET.to_owned(),
            token_key: "token".to_owned(),
            backoff: FAST,
            defaults: target(),
        }
    }
}

fn error(status: StatusCode, code: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({"error": code, "message": "scripted"})))
}

/// The mock enroll handler.
async fn enroll_handler(State(mock): State<Arc<Mock>>, Json(request): Json<Value>) -> (StatusCode, Json<Value>) {
    mock.calls.fetch_add(1, Ordering::SeqCst);
    match mock.reply {
        Reply::Spent => return error(StatusCode::UNAUTHORIZED, "invalid_token"),
        Reply::ServerError => return error(StatusCode::SERVICE_UNAVAILABLE, "internal"),
        Reply::Huge => return (StatusCode::OK, Json(json!({"pad": "x".repeat(MAX_RESPONSE_BYTES)}))),
        Reply::Sign | Reply::WrongSite | Reply::OtherKey | Reply::OtherCa | Reply::Padded => {},
    }
    (StatusCode::CREATED, Json(signed(&mock, &request)))
}

/// Sign the request the way `mock.reply` scripts.
fn signed(mock: &Mock, request: &Value) -> Value {
    let site = if mock.reply == Reply::WrongSite { "site-e" } else { SITE };
    let issuer = if mock.reply == Reply::OtherCa {
        &mock.stranger
    } else {
        &mock.ca
    };
    let requested = request
        .get("csr")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let csr = if mock.reply == Reply::OtherKey {
        certs::generate_csr(SITE).expect("other csr").csr_pem
    } else {
        requested
    };
    let cert = certs::sign_csr(issuer, site, &csr, certs::Validity::default()).expect("mock signs");
    let certificate = if mock.reply == Reply::Padded {
        format!("{}{}junk\n", cert.cert_pem, mock.stranger.cert_pem)
    } else {
        cert.cert_pem
    };
    json!({
        "id": "00000000-0000-0000-0000-000000000000",
        "certificate": certificate,
        "caCertificate": issuer.cert_pem,
        "spiffeId": "spiffe://grid.internal/site/not-from-the-server",
        "publicKeySha256": cert.public_key_sha256,
    })
}

/// Serve one TLS connection with `app`.
async fn serve_conn(tls: tls_backend::ServerTlsConfig, app: Router, stream: tokio::net::TcpStream) {
    let Ok(stream) = tls_backend::accept(stream, &tls).await else {
        return;
    };
    let service = hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
        use tower::Service as _;
        app.clone().call(request)
    });
    let io = hyper_util::rt::TokioIo::new(stream);
    if let Err(error) = hyper::server::conn::http1::Builder::new()
        .serve_connection(io, service)
        .await
    {
        tracing::debug!(%error, "mock connection ended");
    }
}

/// Serve `mock` on `listener` in the background.
fn serve_on(mock: &Arc<Mock>, listener: tokio::net::TcpListener) {
    let server = certs::generate_dns_cert(&mock.ca, "localhost", "localhost").expect("server cert");
    let tls = tls_backend::build_server_config_optional_client(
        mock.ca.cert_pem.as_bytes(),
        server.cert_pem.as_bytes(),
        server.key_pem.as_bytes(),
    )
    .expect("server TLS");
    let app = Router::new()
        .route(signer::ENROLL_PATH, post(enroll_handler))
        .with_state(Arc::clone(mock));
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(serve_conn(Arc::clone(&tls), app.clone(), stream));
        }
    });
}

/// Start a mock, returning it and settings pinned to it.
pub(super) async fn serve(reply: Reply) -> (Arc<Mock>, Settings) {
    let mock = Mock::new(reply);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let settings = mock.settings(listener.local_addr().expect("addr").port());
    serve_on(&mock, listener);
    (mock, settings)
}

/// An address nothing listens on.
async fn closed_addr() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    listener.local_addr().expect("addr")
}

/// In-memory Secrets, with scripted create failures.
#[derive(Default)]
struct FakeStore {
    /// Secrets by name.
    secrets: Mutex<BTreeMap<String, Secret>>,
    /// Names whose data was read.
    reads: Mutex<Vec<String>>,
    /// Every create reports a transient failure.
    creates_fail: bool,
    /// Creating this name reports a conflict.
    conflict_on: Option<&'static str>,
    /// Status every dry-run create answers.
    dry_run_status: Option<u16>,
    /// Attempts at a write, so a retry is countable.
    create_calls: Mutex<u32>,
    /// Attempts at a dry-run write, so a retry is countable.
    check_calls: Mutex<u32>,
}

impl FakeStore {
    /// Attempts at a write.
    fn create_calls(&self) -> u32 {
        *self.create_calls.lock().expect("lock")
    }

    /// Attempts at a dry-run write.
    fn check_calls(&self) -> u32 {
        *self.check_calls.lock().expect("lock")
    }

    /// A store holding the site token.
    fn invited() -> Self {
        let store = Self::default();
        store.put(TOKEN_SECRET, "token", "site-token");
        store.label(TOKEN_SECRET, SITE_LABEL, SITE);
        store
    }

    fn put(&self, name: &str, key: &str, value: &str) {
        let data = BTreeMap::from([(key.to_owned(), ByteString(value.as_bytes().to_vec()))]);
        let mut map = self.secrets.lock().expect("lock");
        map.insert(name.to_owned(), secret(name, "Opaque", data));
    }

    fn label(&self, name: &str, key: &str, value: &str) {
        self.secrets
            .lock()
            .expect("lock")
            .get_mut(name)
            .expect("secret")
            .metadata
            .labels
            .get_or_insert_default()
            .insert(key.to_owned(), value.to_owned());
    }

    fn value(&self, name: &str, key: &str) -> Option<String> {
        let secret = self.secrets.lock().expect("lock").get(name).cloned()?;
        String::from_utf8(secret.data?.get(key)?.0.clone()).ok()
    }
}

impl Store for FakeStore {
    async fn exists(&self, name: &str) -> Result<bool, EnrollError> {
        Ok(self.secrets.lock().expect("lock").contains_key(name))
    }

    async fn get(&self, name: &str) -> Result<Option<Secret>, EnrollError> {
        self.reads.lock().expect("lock").push(name.to_owned());
        Ok(self.secrets.lock().expect("lock").get(name).cloned())
    }

    async fn create(&self, secret: &Secret) -> Result<Created, EnrollError> {
        let name = secret.metadata.name.clone().unwrap_or_default();
        *self.create_calls.lock().expect("lock") += 1;
        if self.creates_fail {
            return Ok(Created::Retry("apiserver unavailable".to_owned()));
        }
        if self.conflict_on == Some(name.as_str()) {
            return Ok(Created::AlreadyExists);
        }
        let inserted = match self.secrets.lock().expect("lock").entry(name) {
            std::collections::btree_map::Entry::Occupied(_) => false,
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(secret.clone());
                true
            },
        };
        Ok(if inserted { Created::Yes } else { Created::AlreadyExists })
    }

    async fn check_create(&self, secret: &Secret) -> Result<Attempt<()>, EnrollError> {
        let name = secret.metadata.name.as_deref().unwrap_or_default();
        *self.check_calls.lock().expect("lock") += 1;
        dry_run_result(
            "grid-system",
            name,
            self.dry_run_status.map_or(Ok(()), |code| Err(api_error(code))),
        )
    }
}

/// A Kubernetes API error with `code`.
fn api_error(code: u16) -> kube::Error {
    kube::Error::Api(Box::new(kube::core::Status::failure("scripted", "r").with_code(code)))
}

fn target() -> Target {
    Target {
        site_secret: "site-tls".to_owned(),
        ca_secret: "grid-ca".to_owned(),
    }
}

/// Run the flow into [`target`].
async fn run_flow(store: &FakeStore, settings: &Settings) -> Result<Outcome, EnrollError> {
    let clients = signer::Clients::default();
    Box::pin(enroll(store, &settings.signer(&clients), settings, &target())).await
}

#[tokio::test]
async fn enrolls_and_stores_a_verified_identity() {
    let (mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore::invited();
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(&outcome, Ok(Outcome::Enrolled(id)) if *id == certs::spiffe_id(SITE)),
        "a valid token enrolls the site: {outcome:?}"
    );
    let ca = store.value("grid-ca", "ca.crt").expect("CA stored");
    let cert = store.value("site-tls", "tls.crt").expect("leaf stored");
    let key = store.value("site-tls", "tls.key").expect("key stored");
    assert!(
        certs::verify_site_cert(&ca, &cert, SITE).is_ok(),
        "the leaf verifies under the stored CA"
    );
    assert_eq!(ca, mock.ca.cert_pem, "the pinned CA is stored");
    assert!(key.contains("PRIVATE KEY"), "the private key is stored");
}

#[tokio::test]
async fn only_the_issued_leaf_is_stored() {
    let (_mock, settings) = serve(Reply::Padded).await;
    let store = FakeStore::invited();
    let outcome = run_flow(&store, &settings).await;
    assert!(matches!(outcome, Ok(Outcome::Enrolled(_))), "enrolls: {outcome:?}");
    let stored = store.value("site-tls", "tls.crt").expect("leaf stored");
    assert_eq!(
        stored.matches("BEGIN CERTIFICATE").count(),
        1,
        "one certificate is stored"
    );
    assert!(!stored.contains("junk"), "trailing bytes are dropped");
}

#[tokio::test]
async fn one_secret_holds_everything_when_site_and_ca_share_it() {
    let (_mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore::invited();
    let shared = Target {
        site_secret: "site-tls".to_owned(),
        ca_secret: "site-tls".to_owned(),
    };
    let clients = signer::Clients::default();
    let outcome = Box::pin(enroll(&store, &settings.signer(&clients), &settings, &shared)).await;
    assert!(matches!(outcome, Ok(Outcome::Enrolled(_))), "enrolls: {outcome:?}");
    for key in ["tls.crt", "tls.key", "ca.crt"] {
        assert!(store.value("site-tls", key).is_some(), "{key} is in the shared Secret");
    }
}

#[tokio::test]
async fn a_spent_token_fails_once_with_the_recovery() {
    let (mock, settings) = serve(Reply::Spent).await;
    let store = FakeStore::invited();
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(&outcome, Err(e @ EnrollError::TokenRejected(_)) if e.to_string().contains("mint a new invite")),
        "a spent token is a hard error naming the fix: {outcome:?}"
    );
    assert_eq!(mock.calls(), 1, "a rejected token is not retried");
    assert!(store.value("site-tls", "tls.crt").is_none(), "nothing is stored");
}

#[tokio::test]
async fn a_server_error_after_sending_is_not_retried() {
    let (mock, settings) = serve(Reply::ServerError).await;
    let outcome = run_flow(&FakeStore::invited(), &settings).await;
    assert!(
        matches!(&outcome, Err(e @ EnrollError::MaybeSpent(_)) if e.to_string().contains("may be spent")),
        "a sent request is never replayed: {outcome:?}"
    );
    assert_eq!(mock.calls(), 1, "one request only");
}

#[tokio::test]
async fn an_oversized_response_is_refused() {
    let (_mock, settings) = serve(Reply::Huge).await;
    let outcome = run_flow(&FakeStore::invited(), &settings).await;
    assert!(
        matches!(&outcome, Err(EnrollError::MaybeSpent(m)) if m.contains("size limit")),
        "the body is capped: {outcome:?}"
    );
}

#[tokio::test]
async fn a_refused_connection_is_retried_until_the_service_is_up() {
    let mock = Mock::new(Reply::Sign);
    let addr = closed_addr().await;
    let mut settings = mock.settings(addr.port());
    settings.backoff = Backoff {
        initial: Duration::from_millis(5),
        max: Duration::from_millis(25),
    };
    let late = Arc::clone(&mock);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        serve_on(&late, tokio::net::TcpListener::bind(addr).await.expect("rebind"));
    });
    let outcome = run_flow(&FakeStore::invited(), &settings).await;
    assert!(
        matches!(outcome, Ok(Outcome::Enrolled(_))),
        "enrolls once up: {outcome:?}"
    );
    assert_eq!(mock.calls(), 1, "the token is sent once");
}

#[tokio::test]
async fn an_unreachable_service_is_retried_without_a_limit() {
    let mock = Mock::new(Reply::Sign);
    let settings = mock.settings(closed_addr().await.port());
    // Giving up would leave the site unenrolled until something restarted the
    // pod, so a retryable failure must never resolve to an error.
    let outcome = tokio::time::timeout(
        Duration::from_millis(60),
        Box::pin(run_flow(&FakeStore::invited(), &settings)),
    )
    .await;
    assert!(
        outcome.is_err(),
        "enrollment stopped retrying a reachable-again service: {outcome:?}"
    );
}

#[tokio::test]
async fn an_existing_identity_skips_enrollment_without_reading_the_token() {
    let (mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore::default();
    store.put("site-tls", "tls.crt", "present");
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(outcome, Ok(Outcome::AlreadyEnrolled)),
        "restart is a no-op: {outcome:?}"
    );
    assert_eq!(mock.calls(), 0, "no token is spent on restart");
    assert!(store.reads.lock().expect("lock").is_empty(), "no Secret data is read");
}

#[tokio::test]
async fn a_grid_network_naming_other_secrets_after_enrollment_is_refused() {
    let (mock, mut settings) = serve(Reply::Sign).await;
    settings.defaults = Target {
        site_secret: "grid-site-identity".to_owned(),
        ca_secret: "grid-ca".to_owned(),
    };
    let store = FakeStore::invited();
    store.put("grid-site-identity", "tls.crt", "present");
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(&outcome, Err(EnrollError::Config(m)) if m.contains("grid-site-identity")),
        "the enrolled Secret is named: {outcome:?}"
    );
    assert_eq!(mock.calls(), 0, "the spent token is not redeemed again");
    assert!(store.reads.lock().expect("lock").is_empty(), "the token is not read");
}

/// Replies that must not be stored, with the reason.
#[tokio::test]
async fn an_unusable_certificate_is_refused_and_not_stored() {
    for (reply, reason) in [
        (Reply::WrongSite, "not valid for site-d"),
        (Reply::OtherKey, "does not carry this site's key, possible interception"),
        (Reply::OtherCa, "not the pinned grid CA"),
    ] {
        let (_mock, settings) = serve(reply).await;
        let store = FakeStore::invited();
        let outcome = run_flow(&store, &settings).await;
        assert!(
            matches!(&outcome, Err(e @ EnrollError::InvalidResponse(m)) if m.contains(reason) && e.to_string().contains("spent")),
            "{reason}: {outcome:?}"
        );
        assert!(store.value("grid-ca", "ca.crt").is_none(), "{reason}: no CA stored");
        assert!(store.value("site-tls", "tls.crt").is_none(), "{reason}: no leaf stored");
    }
}

#[tokio::test]
async fn transient_write_failures_keep_retrying_rather_than_lose_the_identity() {
    let (_mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore {
        creates_fail: true,
        ..FakeStore::invited()
    };
    // The token is spent and the certificate is only in memory, so giving up
    // would cost a new invite. A refused write still fails fast, which
    // kube_write_errors_retry_unless_refused covers.
    // Wait on attempts, not a clock: building the TLS client first takes longer on some runners.
    let flow = Box::pin(run_flow(&store, &settings));
    let retried = async {
        while store.create_calls() < 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    let raced = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::select! {
            outcome = flow => Err(outcome),
            () = retried => Ok(()),
        }
    })
    .await;
    assert!(
        matches!(raced, Ok(Ok(()))),
        "a transient write failure must keep retrying: {raced:?} after {} attempts",
        store.create_calls()
    );
}

#[tokio::test]
async fn an_existing_matching_ca_secret_is_kept() {
    let (mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore::invited();
    store.put("grid-ca", "ca.crt", &mock.ca.cert_pem);
    let outcome = run_flow(&store, &settings).await;
    assert!(matches!(outcome, Ok(Outcome::Enrolled(_))), "enrolls: {outcome:?}");
    assert_eq!(
        store.value("grid-ca", "ca.crt").as_deref(),
        Some(mock.ca.cert_pem.as_str()),
        "the existing CA Secret is left as it was"
    );
}

#[tokio::test]
async fn a_ca_secret_holding_another_ca_stops_before_the_token_is_spent() {
    let (mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore::invited();
    store.put("grid-ca", "ca.crt", &mock.stranger.cert_pem);
    let outcome = run_flow(&store, &settings).await;
    assert!(matches!(outcome, Err(EnrollError::Config(_))), "refused: {outcome:?}");
    assert_eq!(mock.calls(), 0, "the token is not sent");
    assert_eq!(
        store.value("grid-ca", "ca.crt").as_deref(),
        Some(mock.stranger.cert_pem.as_str()),
        "the CA Secret is not overwritten"
    );
}

#[tokio::test]
async fn a_ca_secret_holding_an_extra_ca_stops_before_the_token_is_spent() {
    let (mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore::invited();
    let both = format!("{}{}", mock.ca.cert_pem, mock.stranger.cert_pem);
    store.put("grid-ca", "ca.crt", &both);
    let outcome = run_flow(&store, &settings).await;
    assert!(matches!(outcome, Err(EnrollError::Config(_))), "refused: {outcome:?}");
    assert_eq!(mock.calls(), 0, "the token is not sent");
}

#[tokio::test]
async fn a_ca_secret_missing_a_pinned_ca_stops_before_the_token_is_spent() {
    let (mock, mut settings) = serve(Reply::Sign).await;
    settings.anchor_pem = format!("{}{}", mock.ca.cert_pem, mock.stranger.cert_pem);
    let store = FakeStore::invited();
    store.put("grid-ca", "ca.crt", &mock.stranger.cert_pem);
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(&outcome, Err(EnrollError::Config(m)) if m.contains("misses a pinned grid CA")),
        "refused: {outcome:?}"
    );
    assert_eq!(mock.calls(), 0, "the token is not sent");
}

#[tokio::test]
async fn a_refused_dry_run_stops_before_the_token_is_spent() {
    let (mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore {
        dry_run_status: Some(403),
        ..FakeStore::invited()
    };
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(&outcome, Err(EnrollError::Config(m)) if m.contains("cannot write Secret grid-system/grid-ca")),
        "refused: {outcome:?}"
    );
    assert_eq!(mock.calls(), 0, "the token is not sent");
}

#[tokio::test]
async fn a_dry_run_that_keeps_failing_transiently_never_spends_the_token() {
    let (mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore {
        dry_run_status: Some(503),
        ..FakeStore::invited()
    };
    let outcome = tokio::time::timeout(Duration::from_millis(60), Box::pin(run_flow(&store, &settings))).await;
    assert!(
        outcome.is_err(),
        "a transient dry-run failure must keep retrying rather than fail: {outcome:?}"
    );
    assert!(
        store.check_calls() > 1,
        "pending alone would hold for a stall too: {} attempts",
        store.check_calls()
    );
    assert_eq!(mock.calls(), 0, "the token is not sent");
}

#[tokio::test]
async fn a_token_minted_for_another_site_is_not_spent() {
    let (mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore::invited();
    store.label(TOKEN_SECRET, SITE_LABEL, "site-e");
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(&outcome, Err(EnrollError::NameMismatch(m)) if m.contains("site-e")),
        "refused: {outcome:?}"
    );
    assert_eq!(mock.calls(), 0, "the token is not sent");
}

#[tokio::test]
async fn a_server_outside_the_tls_pin_fails_fast() {
    let (mock, mut settings) = serve(Reply::Sign).await;
    settings.pin_pem = mock.stranger.cert_pem.clone();
    let outcome = run_flow(&FakeStore::invited(), &settings).await;
    assert!(
        matches!(&outcome, Err(EnrollError::SignerUntrusted(_))),
        "a pin mismatch is not retried: {outcome:?}"
    );
    assert_eq!(mock.calls(), 0, "the token is not sent");
}

#[tokio::test]
async fn an_identity_stored_concurrently_wins_and_ours_is_discarded() {
    let (_mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore {
        conflict_on: Some("site-tls"),
        ..FakeStore::invited()
    };
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(outcome, Ok(Outcome::Discarded)),
        "the other writer wins: {outcome:?}"
    );
    assert!(
        store.value("site-tls", "tls.crt").is_none(),
        "ours is not written over it"
    );
}

#[test]
fn recovery_messages_name_both_roles_for_a_new_invite() {
    for error in [
        EnrollError::TokenRejected("spent".into()),
        EnrollError::NameDeleted("deleted".into()),
    ] {
        let message = error.to_string();
        assert!(
            message.contains("allowDeletedName as a grid-admin that also holds enrollment-admin"),
            "recovery must describe both required roles: {message}"
        );
    }
}

#[test]
fn responses_classify_as_done_or_hard_failures() {
    let spent = br#"{"error":"invalid_token","message":"spent"}"#;
    let taken = br#"{"error":"name_taken","message":"held"}"#;
    let deleted = br#"{"error":"name_deleted","message":"deleted"}"#;
    let cases: [(u16, &[u8], &str); 8] = [
        (401, spent, "rejected"),
        (409, taken, "already enrolled"),
        (409, deleted, "allowDeletedName"),
        (500, b"", "may be spent"),
        (503, b"", "may be spent"),
        (400, b"{}", "refused"),
        (409, b"", "refused"),
        (201, b"not json", "invalid"),
    ];
    for (code, body, want) in cases {
        let status = StatusCode::from_u16(code).expect("status");
        let got = signer::classify(status, body)
            .err()
            .map(|e| enroll_error(e).to_string())
            .unwrap_or_default();
        assert!(got.contains(want), "{code}: want {want:?}, got {got:?}");
    }
    let ok = br#"{"certificate":"c","caCertificate":"a"}"#;
    assert!(
        signer::classify(StatusCode::CREATED, ok).is_ok(),
        "a decodable 201 is done"
    );
}

#[test]
fn a_lapsed_signer_certificate_is_retried_and_a_pin_mismatch_is_not() {
    let untrusted = |window: bool| SignerError::Untrusted {
        lapsed: window,
        detail: String::new(),
    };
    assert!(matches!(enroll_error(untrusted(true)), EnrollError::SignerLapsed(_)));
    assert!(matches!(
        enroll_error(untrusted(false)),
        EnrollError::SignerUntrusted(_)
    ));
}

#[cfg(not(feature = "fips"))]
#[test]
fn rustls_reads_only_a_validity_window_as_lapsed() {
    use rustls::{CertificateError, Error};
    let lapsed = |error: Error| tls_backend::is_lapsed_certificate(&error);
    assert!(lapsed(Error::InvalidCertificate(CertificateError::Expired)));
    assert!(lapsed(Error::InvalidCertificate(CertificateError::NotValidYet)));
    assert!(!lapsed(Error::InvalidCertificate(CertificateError::UnknownIssuer)));
    assert!(!lapsed(Error::InvalidCertificate(CertificateError::NotValidForName)));
}

#[cfg(feature = "fips")]
#[test]
fn openssl_reads_only_a_validity_window_as_lapsed() {
    let lapsed = |text: &str| tls_backend::is_lapsed_certificate(&std::io::Error::other(text.to_owned()));
    assert!(lapsed("certificate verify failed (certificate has expired)"));
    assert!(lapsed("certificate verify failed (certificate is not yet valid)"));
    assert!(!lapsed(
        "certificate verify failed (unable to get local issuer certificate)"
    ));
    assert!(!lapsed("certificate verify failed (hostname mismatch)"));
}

#[test]
fn kube_write_errors_retry_unless_refused() {
    for (code, retries) in [(403, false), (422, false), (429, true), (500, true), (503, true)] {
        assert_eq!(transient("s", &api_error(code)).is_ok(), retries, "status {code}");
    }
    assert!(
        transient("s", &kube::Error::TlsRequired).is_ok(),
        "a transport error retries"
    );
}

#[test]
fn dry_runs_pass_on_success_or_conflict_and_retry_like_writes() {
    let verdict = |result| match dry_run_result("ns", "s", result) {
        Ok(Attempt::Done(())) => "done",
        Ok(Attempt::Retry(_)) => "retry",
        Err(EnrollError::Config(_)) => "config",
        Err(_) => "other",
    };
    for (code, want) in [
        (409, "done"),
        (403, "config"),
        (422, "config"),
        (429, "retry"),
        (500, "retry"),
        (503, "retry"),
    ] {
        assert_eq!(verdict(Err(api_error(code))), want, "status {code}");
    }
    assert_eq!(verdict(Ok(())), "done", "an accepted dry run passes");
    assert_eq!(
        verdict(Err(kube::Error::TlsRequired)),
        "retry",
        "a transport error retries"
    );
}

fn invite() -> InviteSecretRef {
    InviteSecretRef {
        name: TOKEN_SECRET.to_owned(),
        key: "token".to_owned(),
    }
}

/// Signer URLs, and whether a signer may answer there: a host under the grid domain is
/// refused, since any site leaf names one, and so are credentials, which the status
/// would publish. The CRD's CEL rules on `local.signerEndpoint` and [`signer_url`] both
/// follow this table. CEL is the front gate over the raw text; [`signer_url`] is the
/// check that counts, and it parses the URL, so an IDNA host under the domain passes
/// CEL and is still refused here. A percent-encoded authority is refused by both, since
/// parsing would decode it past the host rule.
const SIGNER_URL_CASES: &[(&str, bool)] = &[
    ("https://grid-enrollment.grid-enrollment.svc:8443", true),
    ("https://enroll.example.com", true),
    ("https://enroll.example.com:8443/", true),
    ("https://enroll.example.com:8443/est", true),
    ("https://10.0.0.1:8443", true),
    ("https://[2001:db8::1]:8443", true),
    ("https://[::1]:8443", true),
    ("https://[::1]", true),
    ("https://grid.internal.example.com", true),
    ("https://gridxinternal", true),
    ("https://enroll.example.com/grid.internal", true),
    ("https://enroll.example.com/%2e", true),
    ("https://enroll.example.com?host=grid.internal", false),
    ("https://enroll.example.com/%2e?x=%25", false),
    ("https://enroll.example.com#f", false),
    ("https://enroll.example.com:abc", false),
    ("https://enroll.example.com:/", false),
    ("https://enroll.example.com:8443:1/", false),
    ("https://[::1]:abc", false),
    ("https://user@enroll.example.com", false),
    ("https://user:secret@enroll.example.com", false),
    ("http://enroll.example.com", false),
    ("not a url", false),
    ("https://", false),
    ("https:///path", false),
    ("https://:8443", false),
    ("https://grid%2einternal", false),
    ("https://hub.grid%2Einternal:8443/", false),
    ("https://enroll%2eexample.com", false),
    ("https://hub.grid.internal", false),
    ("https://hub.grid.internal:8443", false),
    ("https://HUB.GRID.INTERNAL.:8443", false),
    ("https://grid.internal", false),
    ("https://grid.internal/", false),
    ("https://enroll.example.com@hub.grid.internal", false),
    ("https://a.b.grid.internal/path?x#y", false),
];

#[test]
fn the_signer_refuses_a_url_a_site_certificate_could_answer() {
    for (url, accepted) in SIGNER_URL_CASES {
        assert_eq!(signer_url(url).is_ok(), *accepted, "{url}");
    }
}

/// The regex of a CEL `matches` rule that starts with `prefix`. A raw CEL string carries
/// the regex escapes verbatim.
fn cel_regex(rule: &str, prefix: &str) -> regex::Regex {
    let re = rule
        .strip_prefix(&format!("{prefix}matches("))
        .and_then(|rest| rest.strip_prefix("r'").or_else(|| rest.strip_prefix('\'')))
        .and_then(|rest| rest.strip_suffix("')"))
        .expect("a matches rule");
    regex::Regex::new(re).expect("RE2 syntax")
}

/// The `matches` rules on `local.signerEndpoint` as RE2, in CRD order: the negated query
/// or fragment, the authority, the port, the negated credentials, and the negated
/// lower-cased host under the grid domain.
fn cel_gates() -> [regex::Regex; 5] {
    use kube::CustomResourceExt as _;
    let crd = serde_json::to_value(crate::crd::grid_site::GridSite::crd()).expect("crd");
    let rules: Vec<&str> = crd
        .pointer(
            "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/local/properties/signerEndpoint/x-kubernetes-validations",
        )
        .and_then(Value::as_array)
        .expect("rules")
        .iter()
        .filter_map(|rule| rule.get("rule").and_then(Value::as_str))
        .collect();
    assert_eq!(rules.len(), 6, "{rules:?}");
    assert_eq!(rules.first().copied(), Some("self.url.startsWith('https://')"));
    let gates = [
        cel_regex(rules.get(1).expect("query rule"), "!self.url."),
        cel_regex(rules.get(2).expect("authority rule"), "self.url."),
        cel_regex(rules.get(3).expect("port rule"), "self.url."),
        cel_regex(rules.get(4).expect("credentials rule"), "!self.url."),
        cel_regex(rules.get(5).expect("host rule"), "!self.url.lowerAscii()."),
    ];
    assert!(
        gates[4]
            .as_str()
            .contains(&certs::SPIFFE_TRUST_DOMAIN.replace('.', "[.]")),
        "the rule spells out the grid domain: {}",
        gates[4]
    );
    gates
}

/// The CEL rules on `local.signerEndpoint`, evaluated as the API server would.
#[test]
fn the_crd_refuses_the_same_signer_urls() {
    let [query, authority, port, credentials, host] = cel_gates();
    for (url, accepted) in SIGNER_URL_CASES {
        let cel = url.starts_with("https://")
            && !query.is_match(url)
            && authority.is_match(url)
            && port.is_match(url)
            && !credentials.is_match(url)
            && !host.is_match(&url.to_ascii_lowercase());
        assert_eq!(cel, *accepted, "{url}");
    }
}

#[test]
fn settings_name_the_field_a_refused_url_came_from() {
    crate::init_process_crypto();
    let ca = certs::generate_ca("grid-ca").expect("ca").cert_pem;
    for (url, want) in [
        ("http://enroll.example.com", "https"),
        ("not a url", "signerEndpoint.url"),
        ("https://hub.grid.internal", "grid.internal"),
    ] {
        let got = Settings::new(SITE, url, &invite(), &ca, None)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(got.contains(want), "{url}: {got}");
    }
}

#[test]
fn bundles_without_a_certificate_are_refused_before_anything_is_sent() {
    crate::init_process_crypto();
    let ca = certs::generate_ca("grid-ca").expect("ca").cert_pem;
    let url = "https://enroll.example.com";
    let settings = Settings::new(SITE, url, &invite(), &ca, None).expect("a PEM CA is accepted");
    assert_eq!(settings.anchor_pem, ca, "the pin anchors the grid CA by default");
    let other = certs::generate_ca("grid-ca").expect("other").cert_pem;
    let anchored = Settings::new(SITE, url, &invite(), &ca, Some(&other)).expect("separate anchors");
    assert_eq!(anchored.anchor_pem, other, "gridCaBundleRef anchors the grid CA");
    for (pin, anchors, name) in [
        ("not a certificate\n", None, "signerEndpoint.caBundleRef"),
        (
            ca.as_str(),
            Some("not a certificate\n"),
            "signerEndpoint.gridCaBundleRef",
        ),
    ] {
        let got = Settings::new(SITE, url, &invite(), pin, anchors);
        assert!(
            matches!(&got, Err(EnrollError::Config(m)) if m.contains(name)),
            "{name}: {:?}",
            got.err()
        );
    }
}

#[test]
fn the_site_name_is_the_grid_site_name_and_must_be_valid() {
    crate::init_process_crypto();
    let ca = certs::generate_ca("grid-ca").expect("ca").cert_pem;
    let got = Settings::new("Not_A_Label", "https://enroll.example.com", &invite(), &ca, None);
    assert!(
        matches!(&got, Err(EnrollError::Config(m)) if m.contains("Not_A_Label")),
        "{:?}",
        got.err()
    );
}

fn network(tls: &Value) -> GridNetwork {
    serde_json::from_value(json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridNetwork",
        "metadata": {"name": "grid"},
        "spec": {"tls": tls},
    }))
    .expect("fixture")
}

#[test]
fn enrolling_without_a_grid_network_uses_the_configured_secrets() {
    let defaults = Target {
        site_secret: "grid-site-identity".to_owned(),
        ca_secret: "grid-ca".to_owned(),
    };
    assert_eq!(target_for(&[], "grid-system", &defaults).ok(), Some(defaults.clone()));
    assert_eq!(
        target_for(&[network(&json!({}))], "grid-system", &defaults).ok(),
        Some(defaults),
        "a GridNetwork naming no Secrets keeps the defaults"
    );
}

#[test]
fn a_grid_network_names_the_target_secrets() {
    let refs = |ns: &str| {
        network(&json!({
            "siteSecretRef": {"name": "site-tls", "namespace": ns},
            "caSecretRef": {"name": "grid-ca", "namespace": "grid-system"},
        }))
    };
    let defaults = Target {
        site_secret: "unused".to_owned(),
        ca_secret: "unused".to_owned(),
    };
    assert_eq!(
        target_for(&[refs("grid-system")], "grid-system", &defaults).ok(),
        Some(target()),
        "refs in the operator namespace resolve"
    );
    assert!(
        matches!(
            target_for(&[refs("elsewhere")], "grid-system", &defaults),
            Err(EnrollError::Config(_))
        ),
        "a ref outside the operator namespace is refused"
    );
    assert!(
        matches!(
            target_for(&[refs("grid-system"), refs("grid-system")], "grid-system", &defaults),
            Err(EnrollError::Config(_))
        ),
        "one operator serves one grid"
    );
}

#[test]
fn backoff_doubles_to_the_cap_and_never_shortens_a_delay() {
    // Jitter is added, never subtracted, so each delay sits in [base, base + a tenth].
    for (attempt, base) in (1..=5_u32).zip([2_u64, 4, 8, 16, 32]) {
        let delay = STEP_BACKOFF.delay(attempt).as_millis();
        let floor = u128::from(base) * 1_000;
        assert!(
            (floor..=floor + floor / 10).contains(&delay),
            "attempt {attempt} waited {delay}ms, outside [{floor}, {}]",
            floor + floor / 10
        );
    }
    let capped = STEP_BACKOFF.delay(9).as_millis();
    assert!((60_000..=66_000).contains(&capped), "capped, with jitter: {capped}ms");
}

#[test]
fn the_schedule_has_no_attempt_limit() {
    // A retryable failure must never run out of attempts: the delay keeps
    // coming back capped rather than the caller giving up.
    assert!((60..=66).contains(&STEP_BACKOFF.delay(u32::MAX).as_secs()));
}

#[tokio::test]
async fn an_unlabelled_secret_is_never_sent_as_a_token() {
    let (mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore::default();
    store.put(TOKEN_SECRET, "token", "provider-api-key");
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(&outcome, Err(EnrollError::NameMismatch(m)) if m.contains(SITE_LABEL)),
        "refused: {outcome:?}"
    );
    assert_eq!(mock.calls(), 0, "nothing is sent");
}
