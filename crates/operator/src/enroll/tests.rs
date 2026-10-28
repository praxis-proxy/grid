use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use axum::{Json, Router, extract::State, routing::post};
use reqwest::StatusCode;
use serde_json::{Value, json};

use super::*;

pub(super) const SITE: &str = "site-d";

pub(super) const TOKEN_SECRET: &str = "grid-invite-site-d";

const FAST: Backoff = Backoff {
    initial: Duration::from_millis(1),
    max: Duration::from_millis(5),
    budget: Duration::from_millis(40),
    started: None,
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
    /// Answer 408, so the token may have been spent.
    Timeout,
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
    /// Scripted reply, changed with [`Mock::answer`].
    reply: Mutex<Reply>,
    /// Requests seen.
    calls: AtomicUsize,
}

impl Mock {
    fn new(reply: Reply) -> Arc<Self> {
        crate::init_process_crypto();
        Arc::new(Self {
            ca: certs::generate_ca("grid-ca").expect("ca"),
            stranger: certs::generate_ca("grid-ca").expect("stranger ca"),
            reply: Mutex::new(reply),
            calls: AtomicUsize::new(0),
        })
    }

    /// Answer `reply` from now on.
    pub(super) fn answer(&self, reply: Reply) {
        *self.reply.lock().expect("lock") = reply;
    }

    pub(super) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// Settings pinned to this mock at `port`.
    fn settings(&self, port: u16) -> Settings {
        Settings {
            base: signer::SignerUrl::parse(&format!("https://localhost:{port}")).expect("url"),
            pin_pem: self.ca.cert_pem.clone(),
            anchor_pem: self.ca.cert_pem.clone(),
            site_name: SITE.to_owned(),
            token_secret: TOKEN_SECRET.to_owned(),
            token_key: "token".to_owned(),
            backoff: FAST,
            defaults: target(),
            read_secrets: vec![("site.inviteSecretRef", TOKEN_SECRET.to_owned())],
        }
    }
}

fn error(status: StatusCode, code: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({"error": code, "message": "scripted"})))
}

/// The mock enroll handler.
async fn enroll_handler(State(mock): State<Arc<Mock>>, Json(request): Json<Value>) -> (StatusCode, Json<Value>) {
    mock.calls.fetch_add(1, Ordering::SeqCst);
    let reply = *mock.reply.lock().expect("lock");
    match reply {
        Reply::Spent => return error(StatusCode::UNAUTHORIZED, "invalid_token"),
        Reply::ServerError => return error(StatusCode::SERVICE_UNAVAILABLE, "internal"),
        Reply::Timeout => return error(StatusCode::REQUEST_TIMEOUT, "timeout"),
        Reply::Huge => return (StatusCode::OK, Json(json!({"pad": "x".repeat(MAX_RESPONSE_BYTES)}))),
        Reply::Sign | Reply::WrongSite | Reply::OtherKey | Reply::OtherCa | Reply::Padded => {},
    }
    (StatusCode::CREATED, Json(signed(&mock, &request)))
}

/// Sign the request the way `mock.reply` scripts.
fn signed(mock: &Mock, request: &Value) -> Value {
    let reply = *mock.reply.lock().expect("lock");
    let site = if reply == Reply::WrongSite { "site-e" } else { SITE };
    let issuer = if reply == Reply::OtherCa {
        &mock.stranger
    } else {
        &mock.ca
    };
    let requested = request
        .get("csr")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let csr = if reply == Reply::OtherKey {
        certs::generate_csr(SITE).expect("other csr").csr_pem
    } else {
        requested
    };
    let cert = certs::sign_csr(issuer, site, &csr, certs::Validity::default()).expect("mock signs");
    let certificate = if reply == Reply::Padded {
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
pub(super) async fn serve_conn(tls: tls_backend::ServerTlsConfig, app: Router, stream: tokio::net::TcpStream) {
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

/// A signer on localhost serving a certificate from a CA of its own, expired when `lapsed`,
/// accepting client certificates `client_ca` issued and answering through `app`, and that CA.
pub(super) async fn byo_serving_signer(client_ca: &str, app: Router, lapsed: bool) -> (Url, String) {
    let serving = certs::generate_ca("corporate-ca").expect("ca");
    let server = if lapsed {
        certs::generate_expired_dns_cert(&serving, "localhost", "localhost")
    } else {
        certs::generate_dns_cert(&serving, "localhost", "localhost")
    }
    .expect("server cert");
    let tls = tls_backend::build_server_config_optional_client(
        client_ca.as_bytes(),
        server.cert_pem.as_bytes(),
        server.key_pem.as_bytes(),
    )
    .expect("server TLS");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            drop(tokio::spawn(serve_conn(Arc::clone(&tls), app.clone(), stream)));
        }
    }));
    let base = Url::parse(&format!("https://localhost:{port}")).expect("url");
    (base, serving.cert_pem)
}

/// A copy of `settings`.
fn mock_settings(settings: &Settings) -> Settings {
    Settings {
        base: settings.base.clone(),
        pin_pem: settings.pin_pem.clone(),
        anchor_pem: settings.anchor_pem.clone(),
        site_name: settings.site_name.clone(),
        token_secret: settings.token_secret.clone(),
        token_key: settings.token_key.clone(),
        backoff: settings.backoff,
        defaults: settings.defaults.clone(),
        read_secrets: settings.read_secrets.clone(),
    }
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

/// Which of a store's writes fail transiently.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum WriteFailure {
    /// None.
    #[default]
    None,
    /// Creates of the identity Secrets only: the enrollment kept beside them is still written.
    IdentityCreates,
    /// Creates and patches: the apiserver is away.
    CreatesAndPatches,
}

/// In-memory Secrets, with scripted create failures.
#[derive(Default)]
struct FakeStore {
    /// Secrets by name.
    secrets: Mutex<BTreeMap<String, Secret>>,
    /// Names whose data was read.
    reads: Mutex<Vec<String>>,
    /// Every create reports a transient failure.
    /// Which writes fail transiently.
    writes_fail: WriteFailure,
    /// Creating this name reports a conflict.
    conflict_on: Option<&'static str>,
    /// Status every dry-run create answers.
    dry_run_status: Option<u16>,
    /// Attempts at a write, so a retry is countable.
    create_calls: Mutex<u32>,
    /// Attempts at a dry-run write, so a retry is countable.
    check_calls: Mutex<u32>,
    /// Every read fails, as a refused or timed-out apiserver read does.
    reads_fail: bool,
    /// Patches that write data fail transiently this many more times.
    data_patches_fail: Mutex<u32>,
    /// A Secret another writer puts in place of the named one right after it is read, as a
    /// replacement racing a write would.
    replace_after_read: Mutex<Option<(String, Secret)>>,
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

    /// Give the Secret `name` the resource version `version` and UID `uid`.
    fn stamp(&self, name: &str, version: &str, uid: &str) {
        let mut secrets = self.secrets.lock().expect("lock");
        let metadata = &mut secrets.get_mut(name).expect("secret").metadata;
        metadata.resource_version = Some(version.to_owned());
        metadata.uid = Some(uid.to_owned());
        drop(secrets);
    }

    /// Give the Secret `name` the type `type_`, keeping everything else.
    fn set_type(&self, name: &str, type_: &str) {
        self.secrets.lock().expect("lock").get_mut(name).expect("secret").type_ = Some(type_.to_owned());
    }

    /// Set `key` of the Secret `name`, keeping its metadata.
    fn put_value(&self, name: &str, key: &str, value: &str) {
        self.secrets
            .lock()
            .expect("lock")
            .get_mut(name)
            .expect("secret")
            .data
            .get_or_insert_default()
            .insert(key.to_owned(), ByteString(value.as_bytes().to_vec()));
    }

    fn value(&self, name: &str, key: &str) -> Option<String> {
        let secret = self.secrets.lock().expect("lock").get(name).cloned()?;
        String::from_utf8(secret.data?.get(key)?.0.clone()).ok()
    }
}

impl Store for FakeStore {
    async fn exists(&self, name: &str) -> Result<bool, EnrollError> {
        if self.reads_fail {
            return Err(EnrollError::Kube(format!("reading Secret {name}: forbidden")));
        }
        Ok(self.secrets.lock().expect("lock").contains_key(name))
    }

    async fn metadata(&self, name: &str) -> Result<Option<ObjectMeta>, EnrollError> {
        if self.reads_fail {
            return Err(EnrollError::Kube(format!("reading Secret {name}: forbidden")));
        }
        Ok(self
            .secrets
            .lock()
            .expect("lock")
            .get(name)
            .map(|secret| secret.metadata.clone()))
    }

    async fn get(&self, name: &str) -> Result<Option<Secret>, EnrollError> {
        if self.reads_fail {
            return Err(EnrollError::Kube(format!("reading Secret {name}: forbidden")));
        }
        self.reads.lock().expect("lock").push(name.to_owned());
        let read = self.secrets.lock().expect("lock").get(name).cloned();
        let racing = self
            .replace_after_read
            .lock()
            .expect("lock")
            .take_if(|(named, _)| named == name);
        if let Some((_, replacement)) = racing {
            self.secrets.lock().expect("lock").insert(name.to_owned(), replacement);
        }
        Ok(read)
    }

    async fn create(&self, secret: &Secret) -> Result<Created, EnrollError> {
        let name = secret.metadata.name.clone().unwrap_or_default();
        *self.create_calls.lock().expect("lock") += 1;
        let identity = name == target().site_secret || name == target().ca_secret;
        if self.writes_fail == WriteFailure::CreatesAndPatches
            || (self.writes_fail == WriteFailure::IdentityCreates && identity)
        {
            return Ok(Created::Retry("apiserver unavailable".to_owned()));
        }
        if self.conflict_on == Some(name.as_str()) {
            return Ok(Created::AlreadyExists);
        }
        let inserted = match self.secrets.lock().expect("lock").entry(name) {
            std::collections::btree_map::Entry::Occupied(_) => false,
            std::collections::btree_map::Entry::Vacant(slot) => {
                let mut stored = secret.clone();
                stored.metadata.resource_version = Some("1".to_owned());
                slot.insert(stored);
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

    async fn patch(&self, name: &str, patch: &Value) -> Result<Attempt<()>, EnrollError> {
        if self.writes_fail == WriteFailure::CreatesAndPatches {
            return Ok(Attempt::Retry("apiserver unavailable".to_owned()));
        }
        if patch.get("data").is_some() {
            let mut left = self.data_patches_fail.lock().expect("lock");
            let failing = *left > 0;
            *left = left.saturating_sub(1);
            drop(left);
            if failing {
                return Ok(Attempt::Retry("apiserver unavailable".to_owned()));
            }
        }
        let mut secrets = self.secrets.lock().expect("lock");
        let held = secrets
            .get_mut(name)
            .ok_or_else(|| EnrollError::Kube(format!("Secret {name} not found")))?;
        let merged = merge_patch(held, patch);
        drop(secrets);
        Ok(if merged {
            Attempt::Done(())
        } else {
            Attempt::Retry(format!("Secret {name} changed under the write"))
        })
    }
}

/// Merge `from` into `held`'s annotations, a null removing one.
fn merge_annotations(held: &mut Secret, from: Option<&Value>) {
    let annotations = held.metadata.annotations.get_or_insert_default();
    for (key, value) in from.and_then(Value::as_object).into_iter().flatten() {
        match value.as_str() {
            Some(text) => drop(annotations.insert(key.clone(), text.to_owned())),
            None => drop(annotations.remove(key)),
        }
    }
}

/// Whether the resource version and UID `patch` names, if any, are `held`'s.
fn preconditions_hold(held: &Secret, patch: &Value, version: &str) -> bool {
    let named = |pointer: &str| patch.pointer(pointer).and_then(Value::as_str);
    named("/metadata/resourceVersion").is_none_or(|rv| rv == version)
        && named("/metadata/uid").is_none_or(|uid| Some(uid) == held.metadata.uid.as_deref())
}

/// Merge `patch` into `held` as the API server does, `false` when the resource version or
/// UID it names is not `held`'s.
pub(super) fn merge_patch(held: &mut Secret, patch: &Value) -> bool {
    let version: u64 = held
        .metadata
        .resource_version
        .as_deref()
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    if !preconditions_hold(held, patch, &version.to_string()) {
        return false;
    }
    merge_annotations(held, patch.pointer("/metadata/annotations"));
    let data = held.data.get_or_insert_default();
    for (key, value) in patch.pointer("/data").and_then(Value::as_object).into_iter().flatten() {
        match serde_json::from_value::<ByteString>(value.clone()) {
            Ok(bytes) if !value.is_null() => drop(data.insert(key.clone(), bytes)),
            _ => drop(data.remove(key)),
        }
    }
    held.metadata.resource_version = Some((version + 1).to_string());
    true
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

/// The pending Secret beside [`target`]'s identity.
fn pending_name() -> String {
    pending::name(&target())
}

#[tokio::test]
async fn a_request_that_got_no_answer_is_sent_again_with_the_same_key() {
    let (signer, settings) = serve(Reply::Timeout).await;
    let store = FakeStore::invited();
    let lost = run_flow(&store, &settings).await;
    assert!(
        matches!(lost, Err(EnrollError::MaybeSpent { refused: false, .. })),
        "{lost:?}"
    );
    let kept = store
        .value(&pending_name(), "tls.key")
        .expect("the key is kept before the request goes out");

    signer.answer(Reply::Sign);
    let outcome = run_flow(&store, &settings).await;
    assert!(matches!(outcome, Ok(Outcome::Enrolled(_))), "{outcome:?}");
    assert_eq!(signer.calls(), 2);
    assert_eq!(
        store.value("site-tls", "tls.key").as_deref(),
        Some(kept.as_str()),
        "the restart sends the request the signer may have recorded"
    );
    assert_eq!(store.value(&pending_name(), "tls.key"), None, "no key is left behind");
    assert_eq!(store.value(&pending_name(), "csr.pem"), None);
}

#[tokio::test]
async fn a_token_refused_after_an_unanswered_request_may_have_been_spent_by_it() {
    let (signer, settings) = serve(Reply::Timeout).await;
    let store = FakeStore::invited();
    drop(run_flow(&store, &settings).await);

    signer.answer(Reply::Spent);
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(&outcome, Err(e @ EnrollError::MaybeSpent { refused: true, .. }) if e.to_string().contains("a new invite may be needed")),
        "the earlier request may have spent it, so it is not called invalid, and not sent again: {outcome:?}"
    );
}

#[tokio::test]
async fn a_planted_pending_secret_is_refused_and_never_sent() {
    let (signer, settings) = serve(Reply::Sign).await;
    let store = FakeStore::invited();
    store.put(&pending_name(), "csr.pem", "planted");
    store.label(&pending_name(), "app.kubernetes.io/managed-by", "someone-else");
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(&outcome, Err(e @ EnrollError::PendingForeign(_)) if e.to_string().contains(&pending_name())),
        "names the Secret to delete: {outcome:?}"
    );
    assert_eq!(
        signer.calls(),
        0,
        "nothing is sent beside a Secret enrollment did not write"
    );
    assert_eq!(
        store.value(&pending_name(), "csr.pem").as_deref(),
        Some("planted"),
        "it is left alone"
    );
}

#[tokio::test]
async fn an_enrolled_site_clears_only_a_pending_secret_that_is_wholly_its_own() {
    let (signer, settings) = serve(Reply::Sign).await;
    for (type_, cleared) in [("kubernetes.io/tls", false), ("Opaque", true)] {
        let store = FakeStore::invited();
        store.put("site-tls", "tls.crt", "enrolled");
        store.put(&pending_name(), "csr.pem", "kept");
        store.set_type(&pending_name(), type_);
        let outcome = run_flow(&store, &settings).await;
        assert!(matches!(outcome, Ok(Outcome::AlreadyEnrolled)), "{type_}: {outcome:?}");
        assert_eq!(
            store.value(&pending_name(), "csr.pem").is_none(),
            cleared,
            "{type_}: enrollment's labels on a Secret of another type do not make it enrollment's"
        );
    }
    assert_eq!(signer.calls(), 0);
}

#[tokio::test]
async fn a_pending_secret_replaced_between_the_read_and_the_write_keeps_its_keys() {
    let (signer, settings) = serve(Reply::Sign).await;
    let store = FakeStore::invited();
    store.put("site-tls", "tls.crt", "enrolled");
    store.put(&pending_name(), "csr.pem", "ours");
    store.stamp(&pending_name(), "1", "uid-ours");
    *store.replace_after_read.lock().expect("lock") = Some((pending_name(), another_controllers(&pending_name())));

    let outcome = run_flow(&store, &settings).await;
    assert!(matches!(outcome, Ok(Outcome::AlreadyEnrolled)), "{outcome:?}");
    assert_eq!(signer.calls(), 0);
    let left = store
        .secrets
        .lock()
        .expect("lock")
        .get(&pending_name())
        .cloned()
        .expect("their Secret");
    assert_eq!(
        left.metadata.resource_version.as_deref(),
        Some("999"),
        "the write named the object it judged, so the replacement was never written"
    );
    assert!(
        left.data.as_ref().is_some_and(|data| data.contains_key("payload")),
        "their keys are intact: {:?}",
        left.data
    );
}

/// An Opaque Secret `name` another controller owns, at resource version 999.
fn another_controllers(name: &str) -> Secret {
    let mut theirs = secret(
        name,
        "Opaque",
        BTreeMap::from([("payload".to_owned(), ByteString(b"theirs".to_vec()))]),
    );
    theirs.metadata.resource_version = Some("999".to_owned());
    theirs.metadata.uid = Some("uid-theirs".to_owned());
    theirs.metadata.labels = Some(BTreeMap::from([(
        "app.kubernetes.io/managed-by".to_owned(),
        "another-controller".to_owned(),
    )]));
    theirs
}

/// A request kept for `settings` with the test invite, and its key.
async fn kept_request(store: &FakeStore, settings: &Settings) -> String {
    let csr = certs::generate_csr(SITE).expect("csr");
    let binding = pending::Binding::of(settings, "site-token");
    drop(
        pending::stash(store, &target(), (csr.csr_pem, csr.key_pem.clone()), (&binding, FAST))
            .await
            .expect("stashed"),
    );
    csr.key_pem.to_string()
}

#[tokio::test]
async fn a_request_kept_for_another_site_grid_ca_or_invite_is_discarded() {
    let (_signer, settings) = serve(Reply::Sign).await;
    let (_other, elsewhere) = serve(Reply::Sign).await;
    let renamed = Settings {
        site_name: "site-e".to_owned(),
        ..mock_settings(&settings)
    };
    for (name, binding) in [
        ("site name", pending::Binding::of(&renamed, "site-token")),
        ("grid CA", pending::Binding::of(&elsewhere, "site-token")),
        ("invite", pending::Binding::of(&settings, "another-token")),
    ] {
        let store = FakeStore::invited();
        drop(kept_request(&store, &settings).await);
        let read = pending::read(&store, &target(), &binding, FAST).await.expect("read");
        assert!(read.is_none(), "a {name} change discards the request");
        assert_eq!(
            store.value(&pending_name(), "tls.key"),
            None,
            "{name}: no key is left behind"
        );
    }
}

#[tokio::test]
async fn a_request_whose_key_is_not_its_own_is_discarded() {
    let (_signer, settings) = serve(Reply::Sign).await;
    let store = FakeStore::invited();
    drop(kept_request(&store, &settings).await);
    let other = certs::generate_csr(SITE).expect("other");
    store.put_value(&pending_name(), "tls.key", &other.key_pem);
    let binding = pending::Binding::of(&settings, "site-token");
    let read = pending::read(&store, &target(), &binding, FAST).await.expect("read");
    assert!(read.is_none(), "a key that is not the request's is never sent");
}

#[tokio::test]
async fn a_certificate_issued_before_a_restart_is_stored_without_asking_again() {
    let (issuer, settings) = serve(Reply::Sign).await;
    let store = FakeStore::invited();
    let csr = certs::generate_csr(SITE).expect("csr");
    let binding = pending::Binding::of(&settings, "site-token");
    let mut held = pending::stash(
        &store,
        &target(),
        (csr.csr_pem.clone(), csr.key_pem.clone()),
        (&binding, FAST),
    )
    .await
    .expect("stashed");
    let cert = certs::sign_csr(&issuer.ca, SITE, &csr.csr_pem, certs::Validity::default()).expect("signed");
    let answer = Issued {
        certificate: cert.cert_pem.clone(),
        ca_certificate: issuer.ca.cert_pem.clone(),
    };
    held.issued(&store, &answer, FAST).await.expect("kept");

    let outcome = run_flow(&store, &settings).await;
    assert!(matches!(outcome, Ok(Outcome::Enrolled(_))), "{outcome:?}");
    assert_eq!(issuer.calls(), 0, "the signer is not asked again for what it issued");
    assert_eq!(
        store.value("site-tls", "tls.crt").as_deref(),
        Some(cert.cert_pem.as_str())
    );
    assert_eq!(
        store.value("site-tls", "tls.key").as_deref(),
        Some(csr.key_pem.as_str())
    );
}

#[tokio::test]
async fn a_server_error_after_sending_is_not_retried() {
    let (mock, settings) = serve(Reply::ServerError).await;
    let outcome = run_flow(&FakeStore::invited(), &settings).await;
    assert!(
        matches!(&outcome, Err(e @ EnrollError::MaybeSpent { .. }) if e.to_string().contains("may be spent")),
        "a sent request is never replayed: {outcome:?}"
    );
    assert_eq!(mock.calls(), 1, "one request only");
}

#[tokio::test]
async fn an_oversized_response_is_refused() {
    let (_mock, settings) = serve(Reply::Huge).await;
    let outcome = run_flow(&FakeStore::invited(), &settings).await;
    assert!(
        matches!(&outcome, Err(EnrollError::MaybeSpent { detail, .. }) if detail.contains("size limit")),
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
        budget: Duration::from_secs(5),
        started: None,
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
async fn an_unreachable_service_is_given_up_within_the_step_budget_for_the_next_poll() {
    let mock = Mock::new(Reply::Sign);
    let settings = mock.settings(closed_addr().await.port());
    // The step returns within its budget, so a config edit is seen and the controllers
    // start; the next poll tries again, and the site is not reported as spent.
    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        Box::pin(run_flow(&FakeStore::invited(), &settings)),
    )
    .await
    .expect("the step gives up within its budget");
    assert!(
        matches!(outcome, Err(EnrollError::Unreachable(_))),
        "a service never reached is unreachable, not spent: {outcome:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "gave up near the budget, not after it: {:?}",
        started.elapsed()
    );
    assert_eq!(mock.calls(), 0, "nothing was sent");
}

#[tokio::test(start_paused = true)]
async fn calls_that_share_a_started_budget_spend_it_together() {
    let shared = Backoff {
        initial: Duration::from_millis(10),
        max: Duration::from_millis(10),
        budget: Duration::from_millis(25),
        started: None,
    }
    .started_now();
    let attempts = AtomicUsize::new(0);
    let failing = || async {
        attempts.fetch_add(1, Ordering::SeqCst);
        Ok(Attempt::<()>::Retry("down".to_owned()))
    };
    assert!(with_backoff(shared, "first", EnrollError::Kube, failing).await.is_err());
    let first = attempts.load(Ordering::SeqCst);
    assert!(first > 1, "the first call retries within the budget: {first}");
    assert!(
        with_backoff(shared, "second", EnrollError::Kube, failing)
            .await
            .is_err()
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        first + 1,
        "a later call on the spent budget makes one attempt and gives up"
    );
}

#[tokio::test]
async fn an_answer_after_the_step_budget_is_spent_is_still_kept_through_a_transient_write_failure() {
    let (mock, mut settings) = serve(Reply::Sign).await;
    settings.backoff = Backoff {
        initial: Duration::from_millis(5),
        max: Duration::from_millis(25),
        budget: Duration::from_secs(2),
        started: tokio::time::Instant::now().checked_sub(Duration::from_secs(2)),
    };
    let store = FakeStore {
        data_patches_fail: Mutex::new(1),
        ..FakeStore::invited()
    };
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(outcome, Ok(Outcome::Enrolled(_))),
        "the writes after the answer retry on their own budget: {outcome:?}"
    );
    assert_eq!(mock.calls(), 1, "the token is sent once");
    assert!(
        store.value("site-tls", "tls.crt").is_some(),
        "the issued certificate is stored"
    );
}

#[tokio::test]
async fn a_signer_url_corrected_mid_retry_is_used_on_the_next_poll_with_the_kept_request() {
    let mock = Mock::new(Reply::Sign);
    let wrong = mock.settings(closed_addr().await.port());
    let store = FakeStore::invited();
    let unreachable = run_flow(&store, &wrong).await;
    assert!(
        matches!(unreachable, Err(EnrollError::Unreachable(_))),
        "{unreachable:?}"
    );
    let csr = store
        .value(&pending_name(), "csr.pem")
        .expect("the request is kept before it goes out");
    assert_eq!(mock.calls(), 0, "nothing reached the wrong URL");

    // The admin corrects site.signerEndpoint.url: the next poll builds its settings from
    // the edit, with no restart, and the kept request is still bound to the same site,
    // grid CA and invite.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let corrected = mock.settings(listener.local_addr().expect("addr").port());
    serve_on(&mock, listener);
    let outcome = run_flow(&store, &corrected).await;
    assert!(matches!(outcome, Ok(Outcome::Enrolled(_))), "{outcome:?}");
    assert_eq!(mock.calls(), 1, "the corrected URL is used");
    let stored = store.value("site-tls", "tls.crt").expect("stored");
    assert_eq!(
        certs::cert_public_key(&stored).expect("leaf key"),
        certs::csr_public_key(&csr).expect("csr key"),
        "the kept request went out, not a new one"
    );
    assert_eq!(store.value(&pending_name(), "csr.pem"), None, "the stash is cleared");
}

#[tokio::test]
async fn a_step_with_no_budget_makes_one_attempt_then_gives_up() {
    let none = Backoff {
        initial: Duration::from_millis(1),
        max: Duration::from_millis(1),
        budget: Duration::ZERO,
        started: None,
    };
    let attempts = AtomicUsize::new(0);
    let outcome: Result<(), EnrollError> = with_backoff(none, "testing", EnrollError::Kube, || async {
        attempts.fetch_add(1, Ordering::SeqCst);
        Ok(Attempt::Retry("down".to_owned()))
    })
    .await;
    assert!(matches!(outcome, Err(EnrollError::Kube(reason)) if reason == "down"));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
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
    let reads = store.reads.lock().expect("lock").clone();
    assert!(
        reads.iter().all(|name| *name == pending_name()),
        "only the pending enrollment is read, never the token: {reads:?}"
    );
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
        writes_fail: WriteFailure::CreatesAndPatches,
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
async fn an_identity_write_the_budget_gave_up_on_is_the_stores_and_the_next_poll_stores_the_kept_answer() {
    let (mock, settings) = serve(Reply::Sign).await;
    let store = FakeStore {
        writes_fail: WriteFailure::IdentityCreates,
        ..FakeStore::invited()
    };
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(outcome, Err(EnrollError::Kube(_))),
        "a write given up on is StoreUnavailable, not a spent token: {outcome:?}"
    );
    assert!(
        store.value(&pending_name(), "tls.crt").is_some(),
        "the issued answer stays kept beside the identity"
    );
    assert_eq!(mock.calls(), 1);
    let store = FakeStore {
        secrets: Mutex::new(store.secrets.into_inner().expect("lock")),
        ..FakeStore::default()
    };
    let stored_later = run_flow(&store, &settings).await;
    assert!(matches!(stored_later, Ok(Outcome::Enrolled(_))), "{stored_later:?}");
    assert_eq!(
        mock.calls(),
        1,
        "the kept answer is stored without asking the signer again"
    );
    assert!(store.value("site-tls", "tls.crt").is_some());
}

#[tokio::test]
async fn a_lost_answer_stays_lost_through_a_failure_the_signer_did_not_answer() {
    let (mock, settings) = serve(Reply::Timeout).await;
    let store = FakeStore::invited();
    let lost = run_flow(&store, &settings).await;
    assert!(
        matches!(lost, Err(EnrollError::MaybeSpent { refused: false, .. })),
        "{lost:?}"
    );

    // The signer's serving certificate lapses: no request reaches it, so the earlier one is
    // still unanswered.
    let (url, pin) = byo_serving_signer(&mock.ca.cert_pem, Router::new(), true).await;
    let mut lapsed = mock_settings(&settings);
    lapsed.base = signer::SignerUrl::parse(url.as_str()).expect("url");
    lapsed.pin_pem = pin;
    let tls = run_flow(&store, &lapsed).await;
    assert!(matches!(tls, Err(EnrollError::SignerLapsed(_))), "{tls:?}");

    mock.answer(Reply::Spent);
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(outcome, Err(EnrollError::MaybeSpent { refused: true, .. })),
        "a refusal after the lost answer still reads as one, whatever failed in between: {outcome:?}"
    );
    assert_eq!(mock.calls(), 2, "the lapsed signer never saw the request");
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
    let outcome = tokio::time::timeout(Duration::from_secs(5), Box::pin(run_flow(&store, &settings)))
        .await
        .expect("the step gives up within its budget");
    assert!(
        matches!(outcome, Err(EnrollError::Kube(_))),
        "a transient dry-run failure is the store's, retried on the next poll: {outcome:?}"
    );
    assert!(
        store.check_calls() > 1,
        "the step retries within its budget before giving up: {} attempts",
        store.check_calls()
    );
    assert_eq!(mock.calls(), 0, "the token is not sent");
}

#[tokio::test]
async fn a_grid_ca_secret_named_for_the_pending_enrollment_is_refused_before_the_invite_is_spent() {
    let (mock, settings) = serve(Reply::Sign).await;
    let colliding = Target {
        site_secret: "site-tls".to_owned(),
        ca_secret: pending_name(),
    };
    let store = FakeStore::invited();
    let clients = signer::Clients::default();
    let outcome = Box::pin(enroll(&store, &settings.signer(&clients), &settings, &colliding)).await;
    assert!(
        matches!(&outcome, Err(EnrollError::Config(why)) if why.contains(&pending_name()) && why.contains("grid CA")),
        "names the colliding Secret and both roles: {outcome:?}"
    );
    assert_eq!(mock.calls(), 0, "the token is not sent");
    assert!(store.value(&pending_name(), "csr.pem").is_none(), "nothing is kept");
}

#[tokio::test]
async fn an_invite_or_bundle_secret_named_for_what_enrollment_writes_is_refused() {
    let (mock, mut settings) = serve(Reply::Sign).await;
    settings
        .read_secrets
        .push(("site.signerEndpoint.caBundleRef", "site-tls".to_owned()));
    let store = FakeStore::invited();
    let outcome = run_flow(&store, &settings).await;
    assert!(
        matches!(&outcome, Err(EnrollError::Config(why)) if why.contains("site-tls") && why.contains("caBundleRef") && why.contains("identity")),
        "names the colliding Secret and both fields: {outcome:?}"
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

/// The variant `error` is, for a table test.
fn signer_variant(error: &SignerError) -> &'static str {
    match error {
        SignerError::NameTaken(_) => "NameTaken",
        SignerError::NameDeleted(_) => "NameDeleted",
        SignerError::Forbidden(_) => "Forbidden",
        SignerError::Refused(_) => "Refused",
        SignerError::NoClientCertificate(_) => "NoClientCertificate",
        SignerError::Unauthenticated(_) => "Unauthenticated",
        SignerError::Disabled(_) => "Disabled",
        SignerError::Transport { sent: true, .. } => "Transport sent",
        SignerError::Transport { sent: false, .. } | SignerError::Untrusted { .. } => "never sent",
        SignerError::Request(_) | SignerError::Invalid(_) => "local",
    }
}

#[test]
fn the_signer_classifies_a_refusal_by_what_it_means() {
    let cases: [(u16, &str, &str); 9] = [
        (409, "name_taken", "NameTaken"),
        (409, "name_deleted", "NameDeleted"),
        (409, "other", "Refused"),
        (403, "identity_refused", "Forbidden"),
        (400, "bad", "Refused"),
        (401, "identity_required", "NoClientCertificate"),
        (401, "invalid_token", "Unauthenticated"),
        (503, "rotation_disabled", "Disabled"),
        (503, "internal", "Transport sent"),
    ];
    for (status, code, want) in cases {
        let body = format!(r#"{{"error":"{code}","message":"m"}}"#);
        let got = signer::classify(StatusCode::from_u16(status).expect("status"), body.as_bytes())
            .as_ref()
            .err()
            .map(signer_variant);
        assert_eq!(got, Some(want), "{status} {code}");
    }
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
/// would publish. A percent-encoded authority is refused, since parsing would decode it
/// past the host rule.
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

/// The operator config refuses the same signer URLs, before anything reads them.
#[test]
fn the_operator_config_refuses_the_same_signer_urls() {
    for (url, accepted) in SIGNER_URL_CASES {
        let document = format!("site:\n  name: {SITE}\n  signerEndpoint:\n    url: '{url}'\n");
        let parsed = crate::operator_config::parse(&document);
        assert_eq!(parsed.is_ok(), *accepted, "{url}: {parsed:?}");
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

/// A site whose identity is definitely gone, with an invite minted for it, restarts to enroll;
/// anything less certain decides nothing.
#[tokio::test]
#[expect(clippy::too_many_lines, reason = "one case per standing")]
async fn a_lost_identity_restarts_only_on_a_definite_absence_and_an_invite_for_this_site() {
    let site_secret = target().site_secret;
    let gone_invited = FakeStore::invited();
    gone_invited.label(TOKEN_SECRET, SITE_LABEL, SITE);
    assert!(
        identity_lost(&gone_invited, &site_secret, TOKEN_SECRET, SITE).await,
        "gone, and invited"
    );

    let gone_uninvited = FakeStore::default();
    assert!(
        !identity_lost(&gone_uninvited, &site_secret, TOKEN_SECRET, SITE).await,
        "no invite"
    );

    let unlabelled = FakeStore::default();
    unlabelled.put(TOKEN_SECRET, "token", "site-token");
    assert!(
        !identity_lost(&unlabelled, &site_secret, TOKEN_SECRET, SITE).await,
        "an invite not labelled for this site"
    );
    let other_site = FakeStore::invited();
    other_site.label(TOKEN_SECRET, SITE_LABEL, "site-e");
    assert!(
        !identity_lost(&other_site, &site_secret, TOKEN_SECRET, SITE).await,
        "an invite for another site"
    );

    let present = FakeStore::invited();
    present.label(TOKEN_SECRET, SITE_LABEL, SITE);
    present.put(&site_secret, "tls.crt", "cert");
    assert!(
        !identity_lost(&present, &site_secret, TOKEN_SECRET, SITE).await,
        "the identity is present"
    );

    let unreadable = FakeStore {
        reads_fail: true,
        ..FakeStore::default()
    };
    assert!(
        !identity_lost(&unreadable, &site_secret, TOKEN_SECRET, SITE).await,
        "a failed read is not an absence"
    );
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
