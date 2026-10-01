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

const SITE: &str = "site-d";

const TOKEN_SECRET: &str = "grid-invite-site-d";

const FAST: Backoff = Backoff {
    attempts: 3,
    initial: Duration::from_millis(1),
    max: Duration::from_millis(5),
};

/// How the mock enrollment service answers.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Reply {
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
struct Mock {
    /// Serves TLS and is the pinned grid CA.
    ca: certs::CaCert,
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

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// Settings pinned to this mock at `port`.
    fn settings(&self, port: u16) -> Settings {
        Settings {
            base: Url::parse(&format!("https://localhost:{port}")).expect("url"),
            http: pinned(&self.ca.cert_pem),
            anchor_pem: self.ca.cert_pem.clone(),
            site_name: SITE.to_owned(),
            token_secret: TOKEN_SECRET.to_owned(),
            token_key: "token".to_owned(),
            backoff: FAST,
        }
    }
}

/// A client pinned to `ca_pem`.
fn pinned(ca_pem: &str) -> reqwest::Client {
    http_client(pem_roots(ca_pem, "test CA").expect("roots")).expect("client")
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
    let tls = tls_backend::build_server_config(
        mock.ca.cert_pem.as_bytes(),
        server.cert_pem.as_bytes(),
        server.key_pem.as_bytes(),
    )
    .expect("server TLS");
    let app = Router::new()
        .route(ENROLL_PATH, post(enroll_handler))
        .with_state(Arc::clone(mock));
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(serve_conn(Arc::clone(&tls), app.clone(), stream));
        }
    });
}

/// Start a mock, returning it and settings pinned to it.
async fn serve(reply: Reply) -> (Arc<Mock>, Settings) {
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
}

impl FakeStore {
    /// A store holding the site token.
    fn invited() -> Self {
        let store = Self::default();
        store.put(TOKEN_SECRET, "token", "site-token");
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
    Box::pin(enroll(store, settings, &target())).await
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
    let outcome = Box::pin(enroll(&store, &settings, &shared)).await;
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
        matches!(&outcome, Err(e @ EnrollError::TokenRejected(_)) if e.to_string().contains("new site name")),
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
        attempts: 400,
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
async fn an_unreachable_service_exhausts_the_retry_budget() {
    let mock = Mock::new(Reply::Sign);
    let settings = mock.settings(closed_addr().await.port());
    let outcome = run_flow(&FakeStore::invited(), &settings).await;
    assert!(
        matches!(outcome, Err(EnrollError::Exhausted { attempts: 3, .. })),
        "a dead endpoint is retried to the budget, then fails: {outcome:?}"
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
async fn persistent_write_failures_end_in_not_stored() {
    let (_mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore {
        creates_fail: true,
        ..FakeStore::invited()
    };
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(&outcome, Err(e @ EnrollError::NotStored(_)) if e.to_string().contains("new site name")),
        "writes retry to the budget, then say the token is spent: {outcome:?}"
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
async fn a_dry_run_that_keeps_failing_transiently_exhausts_before_the_token_is_spent() {
    let (mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore {
        dry_run_status: Some(503),
        ..FakeStore::invited()
    };
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(outcome, Err(EnrollError::Exhausted { attempts: 3, .. })),
        "a transient dry-run failure retries to the budget: {outcome:?}"
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
        matches!(&outcome, Err(EnrollError::Config(m)) if m.contains("site-e")),
        "refused: {outcome:?}"
    );
    assert_eq!(mock.calls(), 0, "the token is not sent");
}

#[tokio::test]
async fn a_server_outside_the_tls_pin_fails_fast() {
    let (mock, mut settings) = serve(Reply::Sign).await;
    settings.http = pinned(&mock.stranger.cert_pem);
    let outcome = run_flow(&FakeStore::invited(), &settings).await;
    assert!(
        matches!(&outcome, Err(EnrollError::Config(m)) if m.contains("GRID_ENROLL_CA_FILE")),
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
fn responses_classify_as_done_or_hard_failures() {
    let spent = br#"{"error":"invalid_token","message":"spent"}"#;
    let taken = br#"{"error":"name_taken","message":"held"}"#;
    let cases: [(u16, &[u8], &str); 7] = [
        (401, spent, "rejected"),
        (409, taken, "already enrolled"),
        (500, b"", "may be spent"),
        (503, b"", "may be spent"),
        (400, b"{}", "refused"),
        (409, b"", "refused"),
        (201, b"not json", "invalid"),
    ];
    for (code, body, want) in cases {
        let status = StatusCode::from_u16(code).expect("status");
        let got = classify(status, body).err().map(|e| e.to_string()).unwrap_or_default();
        assert!(got.contains(want), "{code}: want {want:?}, got {got:?}");
    }
    let ok = br#"{"certificate":"c","caCertificate":"a"}"#;
    assert!(classify(StatusCode::CREATED, ok).is_ok(), "a decodable 201 is done");
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

#[test]
fn urls_must_parse_and_be_https() {
    let config = |url: &str| Config {
        enabled: true,
        url: Some(url.to_owned()),
        ca_file: None,
        grid_ca_file: None,
        site_name: Some(SITE.to_owned()),
        token_secret: Some(TOKEN_SECRET.to_owned()),
        token_secret_key: "token".to_owned(),
    };
    for (url, want) in [("http://enroll.example.com", "https"), ("not a url", "GRID_ENROLL_URL")] {
        let got = Settings::from_config(&config(url))
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(got.contains(want), "{url}: {got}");
    }
}

/// A temp file, removed on drop.
struct TempFile(PathBuf);

impl TempFile {
    fn new(contents: &str) -> Self {
        let path = std::env::temp_dir().join(format!("grid-enroll-{}", uuid::Uuid::new_v4()));
        std::fs::write(&path, contents).expect("write");
        Self(path)
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        drop(std::fs::remove_file(&self.0));
    }
}

#[test]
fn bundles_without_a_certificate_are_refused_before_anything_is_sent() {
    crate::init_process_crypto();
    let ca = TempFile::new(&certs::generate_ca("grid-ca").expect("ca").cert_pem);
    let junk = TempFile::new("not a certificate\n");
    let with = |ca_file: &TempFile, grid_ca_file: Option<&TempFile>| Config {
        enabled: true,
        url: Some("https://enroll.example.com".to_owned()),
        ca_file: Some(ca_file.0.clone()),
        grid_ca_file: grid_ca_file.map(|file| file.0.clone()),
        site_name: Some(SITE.to_owned()),
        token_secret: Some(TOKEN_SECRET.to_owned()),
        token_secret_key: "token".to_owned(),
    };
    assert!(Settings::from_config(&with(&ca, None)).is_ok(), "a PEM CA is accepted");
    for (config, name) in [
        (with(&junk, None), "GRID_ENROLL_CA_FILE"),
        (with(&ca, Some(&junk)), "GRID_ENROLL_GRID_CA_FILE"),
    ] {
        let got = Settings::from_config(&config);
        assert!(
            matches!(&got, Err(EnrollError::Config(m)) if m.contains(name)),
            "{name}: {:?}",
            got.err()
        );
    }
}

#[test]
fn target_secrets_must_be_in_the_operator_namespace() {
    let network = |ns: &str| {
        serde_json::from_value::<GridNetwork>(json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridNetwork",
            "metadata": {"name": "grid"},
            "spec": {"tls": {
                "siteSecretRef": {"name": "site-tls", "namespace": ns},
                "caSecretRef": {"name": "grid-ca", "namespace": "grid-system"},
            }},
        }))
        .expect("fixture")
    };
    assert_eq!(
        target_from(&network("grid-system"), "grid-system").ok(),
        Some(target()),
        "refs in the operator namespace resolve"
    );
    assert!(
        matches!(
            target_from(&network("elsewhere"), "grid-system"),
            Err(EnrollError::Config(_))
        ),
        "a ref outside the operator namespace is refused"
    );
}

#[test]
fn backoff_doubles_to_the_cap() {
    let delays: Vec<u64> = (1..=5).map(|n| STEP_BACKOFF.delay(n).as_secs()).collect();
    assert_eq!(delays, [2, 4, 8, 16, 32], "doubling from the initial delay");
    assert_eq!(STEP_BACKOFF.delay(9), Duration::from_secs(60), "capped");
}
