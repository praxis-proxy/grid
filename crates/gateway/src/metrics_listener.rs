//! Opt-in TLS listener that serves only Kubernetes-authorized `GET /metrics`.
//!
//! The Praxis admin listener also serves kv, pipelines, and log-level with no
//! auth, and refuses a non-loopback `Host`, so an in-cluster scraper cannot use
//! it. This listener renders the same Prometheus registry on its own port and checks each
//! scraper token with Kubernetes `TokenReview` and `SubjectAccessReview`.

use std::{
    borrow::Cow,
    collections::HashMap,
    io,
    net::{SocketAddr, TcpListener as StdTcpListener},
    sync::Arc,
    thread::JoinHandle,
    time::{Duration, Instant},
};

use metrics::{counter, gauge};
use praxis_core::config::Config;
use praxis_tls::{CertKeyPair, reload::ReloadableCertResolver, watcher::CertWatcher};
use reqwest::Certificate;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Semaphore},
    time::timeout,
};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{ServerConfig, crypto::CryptoProvider},
};
use tracing::{debug, info, warn};

/// Env var naming the listener's bind address. Unset leaves the listener off.
const ADDR_ENV: &str = "GRID_METRICS_ADDR";

/// Env var naming the serving certificate (PEM chain).
const CERT_ENV: &str = "GRID_METRICS_TLS_CERT";

/// Env var naming the serving key (PEM).
const KEY_ENV: &str = "GRID_METRICS_TLS_KEY";

/// Mounted projected Kubernetes API token for `TokenReview` and `SubjectAccessReview`.
const API_TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";

/// Mounted Kubernetes API CA bundle.
const API_CA_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";

/// Kubernetes API endpoint used for in-cluster authorization reviews.
const API_URL: &str = "https://kubernetes.default.svc";

/// Namespace whose synthetic scrape permission is checked by `SubjectAccessReview`.
const AUTH_NAMESPACE_ENV: &str = "GRID_METRICS_AUTH_NAMESPACE";

/// Release-specific synthetic scrape permission name.
const AUTH_RESOURCE_NAME_ENV: &str = "GRID_METRICS_AUTH_RESOURCE_NAME";

/// Concurrent connections accepted by the metrics listener.
const MAX_CONNECTIONS: usize = 16;

/// Concurrent uncached Kubernetes authorization reviews.
const MAX_AUTHORIZATION_REVIEWS: usize = 8;

/// Maximum cached token decisions held by this gateway process.
const MAX_CACHED_AUTHORIZATIONS: usize = 1024;

/// Lifetime for an allowed token decision.
const ALLOWED_AUTHORIZATION_TTL: Duration = Duration::from_secs(60);

/// Lifetime for a denied token decision.
const DENIED_AUTHORIZATION_TTL: Duration = Duration::from_secs(10);

/// Authorization request outcomes exposed on the metrics registry.
const AUTHORIZATION_METRIC: &str = "grid_metrics_auth_total";

/// Concurrent Kubernetes authorization reviews exposed on the metrics registry.
const AUTHORIZATION_IN_FLIGHT_METRIC: &str = "grid_metrics_auth_in_flight";

/// Deadline for the TLS handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);

/// Deadline for a whole connection, handshake to close, so no client holds a slot longer.
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

/// Largest request head read. A longer one gets 431.
const MAX_HEAD: usize = 8 * 1024;

/// Most request headers parsed.
const MAX_HEADERS: usize = 32;

/// Pause after a failed accept, so a persistent error such as EMFILE does not spin.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// The listener's settings, read from the environment.
#[derive(Debug)]
pub(crate) struct MetricsListener {
    /// Bind address.
    addr: SocketAddr,

    /// Serving certificate and key, reloaded on change.
    pair: CertKeyPair,
}

impl MetricsListener {
    /// The listener settings from `lookup`, or `None` when the address is unset.
    ///
    /// # Errors
    ///
    /// Returns a message for an unparsable address or a missing cert or key.
    pub(crate) fn from_env<F: Fn(&str) -> Option<String>>(lookup: F) -> Result<Option<Self>, String> {
        let Some(addr) = lookup(ADDR_ENV).filter(|addr| !addr.is_empty()) else {
            return Ok(None);
        };
        let addr = addr
            .parse()
            .map_err(|err| format!("{ADDR_ENV} {addr:?} is not an ip:port: {err}"))?;
        let path = |name: &str| {
            lookup(name)
                .filter(|path| !path.is_empty())
                .ok_or_else(|| format!("{ADDR_ENV} needs {name}: the metrics listener serves TLS only"))
        };
        Ok(Some(Self {
            addr,
            pair: CertKeyPair {
                cert_path: path(CERT_ENV)?,
                default: false,
                key_path: path(KEY_ENV)?,
                server_names: Vec::new(),
            },
        }))
    }

    /// Refuse a port that a Praxis data, admin, or metrics listener already binds.
    ///
    /// # Errors
    ///
    /// Returns a message naming the colliding address, or one with no readable port.
    pub(crate) fn check_ports(&self, config: &Config) -> Result<(), String> {
        let taken = config
            .listeners
            .iter()
            .map(|listener| listener.address.as_str())
            .chain(config.admin.address.as_deref())
            .chain(config.admin.metrics_address.as_deref());
        for address in taken {
            let port = address
                .rsplit_once(':')
                .and_then(|(_, port)| port.parse::<u16>().ok())
                .ok_or_else(|| format!("Praxis address {address:?} has no port to check {ADDR_ENV} against"))?;
            if port == self.addr.port() {
                return Err(format!(
                    "{ADDR_ENV} {} collides with Praxis address {address}",
                    self.addr
                ));
            }
        }
        Ok(())
    }

    /// Bind the listener and serve it on its own thread.
    ///
    /// # Errors
    ///
    /// Returns a message when the cert or Kubernetes API CA does not load, no
    /// crypto provider is installed, or the address does not bind.
    pub(crate) fn spawn(self) -> Result<JoinHandle<()>, String> {
        let resolver = ReloadableCertResolver::new(&self.pair)
            .map_err(|err| format!("metrics listener certificate {}: {err}", self.pair.cert_path))?;
        let current = resolver.arc();
        let acceptor = TlsAcceptor::from(Arc::new(server_config(resolver)?));
        let authorizer = KubernetesAuthorizer::new()?;
        let listener = StdTcpListener::bind(self.addr)
            .and_then(|listener| listener.set_nonblocking(true).map(|()| listener))
            .map_err(|err| format!("metrics listener bind {}: {err}", self.addr))?;
        // A dropped sender leaves the watcher running for the life of the process.
        let (_, shutdown) = tokio::sync::watch::channel(false);
        drop(CertWatcher::spawn(current, self.pair, None, shutdown));
        let addr = self.addr;
        std::thread::Builder::new()
            .name("grid-metrics".to_owned())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        warn!(error = %err, "metrics listener runtime failed to start");
                        return;
                    },
                };
                info!(%addr, "metrics listener serving");
                runtime.block_on(serve(listener, acceptor, authorizer));
            })
            .map_err(|err| format!("metrics listener thread: {err}"))
    }
}

/// The listener's TLS config, over the process crypto provider so FIPS mode carries over.
///
/// # Errors
///
/// Returns a message when no provider is installed, it rejects the protocol versions,
/// or FIPS is required and the config is not FIPS.
fn server_config(resolver: ReloadableCertResolver) -> Result<ServerConfig, String> {
    let provider = CryptoProvider::get_default()
        .ok_or_else(|| "metrics listener: no rustls crypto provider installed".to_owned())?;
    let mut config = ServerConfig::builder_with_provider(Arc::clone(provider))
        .with_safe_default_protocol_versions()
        .map_err(|err| format!("metrics listener TLS: {err}"))?
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(resolver));
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    // As Praxis sets on its own listeners: SP 800-52r2 needs EMS for TLS 1.2.
    config.require_ems = true;
    if praxis_tls::provider::required() && !config.fips() {
        return Err("metrics listener: FIPS is required but its TLS config is not FIPS".to_owned());
    }
    Ok(config)
}

/// Register the bound socket with the runtime and serve it.
async fn serve(listener: StdTcpListener, acceptor: TlsAcceptor, authorizer: KubernetesAuthorizer) {
    match TcpListener::from_std(listener) {
        Ok(listener) => accept_loop(&listener, &acceptor, &authorizer).await,
        Err(err) => warn!(error = %err, "metrics listener failed to register its socket"),
    }
}

/// Accept connections forever, closing any beyond [`MAX_CONNECTIONS`].
#[expect(clippy::infinite_loop, reason = "serves for the life of the process")]
async fn accept_loop(listener: &TcpListener, acceptor: &TlsAcceptor, authorizer: &KubernetesAuthorizer) {
    let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                warn!(error = %err, "metrics listener accept failed");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
                continue;
            },
        };
        let Ok(permit) = Arc::clone(&slots).try_acquire_owned() else {
            debug!(%peer, "metrics listener at capacity, closing connection");
            drop(stream);
            continue;
        };
        let acceptor = acceptor.clone();
        let authorizer = authorizer.clone();
        drop(tokio::spawn(async move {
            if let Err(err) = Box::pin(handle(stream, &acceptor, &authorizer)).await {
                debug!(%peer, error = %err, "metrics connection failed");
            }
            drop(permit);
        }));
    }
}

/// Serve one request on `stream` and close it.
///
/// # Errors
///
/// Returns the I/O error or timeout that ended the connection.
async fn handle(stream: TcpStream, acceptor: &TlsAcceptor, authorizer: &KubernetesAuthorizer) -> io::Result<()> {
    // Boxed: the TLS stream makes this future several KiB.
    within(
        CONNECTION_TIMEOUT,
        Box::pin(async {
            let mut tls = within(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await?;
            let request = read_request_details(&mut tls).await?;
            let reply = match (request.reply, request.bearer_token) {
                (Reply::Metrics, Some(token)) => authorize_request(authorizer, &token).await,
                (Reply::Metrics, None) => {
                    record_authorization_result("missing");
                    Reply::Unauthorized
                },
                (reply, _) => reply,
            };
            let (head, body) = reply.into_parts();
            tls.write_all(head.as_bytes()).await?;
            tls.write_all(body.as_bytes()).await?;
            tls.shutdown().await
        }),
    )
    .await
}

/// Resolve one scrape credential to an HTTP response and record its outcome.
async fn authorize_request(authorizer: &KubernetesAuthorizer, token: &str) -> Reply {
    match Box::pin(authorizer.authorize(token)).await {
        Ok(Authorization::Allowed) => {
            record_authorization_result("allowed");
            Reply::Metrics
        },
        Ok(Authorization::Unauthenticated) => {
            record_authorization_result("unauthenticated");
            Reply::Unauthorized
        },
        Ok(Authorization::Forbidden) => {
            record_authorization_result("forbidden");
            Reply::Forbidden
        },
        Err(AuthorizationError::Overloaded) => {
            record_authorization_result("overloaded");
            Reply::ServiceUnavailable
        },
        Err(AuthorizationError::Unavailable(err)) => {
            record_authorization_result("api_error");
            warn!(error = %err, "metrics authorization API unavailable");
            Reply::ServiceUnavailable
        },
    }
}

/// Record a bounded authorization outcome label.
fn record_authorization_result(result: &'static str) {
    counter!(AUTHORIZATION_METRIC, "result" => result).increment(1);
}

/// Keep the authorization review gauge balanced on every return and cancellation path.
struct AuthorizationReviewGuard;

impl AuthorizationReviewGuard {
    /// Increment the in-flight review gauge.
    fn new() -> Self {
        gauge!(AUTHORIZATION_IN_FLIGHT_METRIC).increment(1.0);
        Self
    }
}

impl Drop for AuthorizationReviewGuard {
    fn drop(&mut self) {
        gauge!(AUTHORIZATION_IN_FLIGHT_METRIC).decrement(1.0);
    }
}

/// Kubernetes API client for the metrics listener's authentication checks.
#[derive(Clone)]
struct KubernetesAuthorizer {
    /// API client trusting the in-cluster CA bundle.
    client: reqwest::Client,
    /// Namespace containing the gateway's synthetic scrape permission.
    namespace: String,
    /// Release-specific name of the scrape permission.
    resource_name: String,
    /// Bounded cache of token hashes and their recent review decisions.
    cache: Arc<Mutex<AuthorizationCache>>,
    /// Limit concurrent API review work independently of accepted connections.
    review_slots: Arc<Semaphore>,
}

impl KubernetesAuthorizer {
    /// Build a client using the projected service-account CA.
    ///
    /// # Errors
    ///
    /// Returns a message when the CA, authorization scope, or HTTP client cannot be loaded.
    fn new() -> Result<Self, String> {
        let namespace = std::env::var(AUTH_NAMESPACE_ENV)
            .ok()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("metrics listener needs {AUTH_NAMESPACE_ENV}"))?;
        let resource_name = std::env::var(AUTH_RESOURCE_NAME_ENV)
            .ok()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("metrics listener needs {AUTH_RESOURCE_NAME_ENV}"))?;
        let ca = std::fs::read(API_CA_PATH).map_err(|err| format!("metrics listener Kubernetes CA: {err}"))?;
        let certificate = Certificate::from_pem(&ca).map_err(|err| format!("metrics listener Kubernetes CA: {err}"))?;
        let client = reqwest::Client::builder()
            .tls_certs_only([certificate])
            .timeout(Duration::from_secs(2))
            .build()
            .map_err(|err| format!("metrics listener Kubernetes API client: {err}"))?;
        Ok(Self {
            client,
            namespace,
            resource_name,
            cache: Arc::new(Mutex::new(AuthorizationCache::default())),
            review_slots: Arc::new(Semaphore::new(MAX_AUTHORIZATION_REVIEWS)),
        })
    }

    /// Authenticate a scraper token and authorize `GET /metrics` for its user.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the Kubernetes API cannot complete either review.
    async fn authorize(&self, token: &str) -> Result<Authorization, AuthorizationError> {
        let token_hash = token_hash(token);
        let cached = self.cache.lock().await.get(token_hash, Instant::now());
        if let Some(cached) = cached {
            return Ok(cached);
        }
        let _permit = Arc::clone(&self.review_slots)
            .try_acquire_owned()
            .map_err(|_error| AuthorizationError::Overloaded)?;
        let _in_flight = AuthorizationReviewGuard::new();
        let authorization = self.review(token).await.map_err(AuthorizationError::Unavailable)?;
        self.cache
            .lock()
            .await
            .insert(token_hash, authorization, Instant::now());
        Ok(authorization)
    }

    /// Authenticate a scraper token and authorize its user with Kubernetes.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the Kubernetes API cannot complete either review.
    async fn review(&self, token: &str) -> io::Result<Authorization> {
        let token_review: TokenReview = self
            .post(
                "/apis/authentication.k8s.io/v1/tokenreviews",
                &serde_json::json!({
                    "apiVersion": "authentication.k8s.io/v1",
                    "kind": "TokenReview",
                    "spec": {"token": token}
                }),
            )
            .await?;
        if !token_review.status.authenticated {
            return Ok(Authorization::Unauthenticated);
        }
        let Some(user) = token_review.status.user else {
            return Ok(Authorization::Unauthenticated);
        };
        let mut spec = serde_json::json!({
            "user": user.username,
            "groups": user.groups,
            "extra": user.extra,
            "resourceAttributes": {
                "namespace": self.namespace.as_str(),
                "verb": "get",
                "group": "metrics.grid.praxis-proxy.io",
                "resource": "scrapes",
                "name": self.resource_name.as_str()
            }
        });
        if let (Some(uid), Some(spec)) = (user.uid, spec.as_object_mut()) {
            spec.insert("uid".to_owned(), serde_json::Value::String(uid));
        }
        let authorization_review: SubjectAccessReview = self
            .post(
                "/apis/authorization.k8s.io/v1/subjectaccessreviews",
                &serde_json::json!({
                    "apiVersion": "authorization.k8s.io/v1",
                    "kind": "SubjectAccessReview",
                    "spec": spec
                }),
            )
            .await?;
        Ok(if authorization_review.status.allowed {
            Authorization::Allowed
        } else {
            Authorization::Forbidden
        })
    }

    /// Post a review request using the gateway's projected API token.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when credentials, transport, API status, or response data fail.
    async fn post<T: serde::de::DeserializeOwned>(&self, path: &str, body: &serde_json::Value) -> io::Result<T> {
        let api_token = std::fs::read_to_string(API_TOKEN_PATH).map_err(|_error| auth_api_error())?;
        let response = self
            .client
            .post(format!("{API_URL}{path}"))
            .bearer_auth(api_token.trim())
            .json(&body)
            .send()
            .await
            .map_err(|_error| auth_api_error())?
            .error_for_status()
            .map_err(|_error| auth_api_error())?;
        response.json().await.map_err(|_error| auth_api_error())
    }
}

/// A bounded cache of authorization decisions keyed only by a token digest.
#[derive(Default)]
struct AuthorizationCache {
    /// Cached decisions, each expiring after its allow or deny TTL.
    entries: HashMap<[u8; 32], CachedAuthorization>,
}

impl AuthorizationCache {
    /// Return a cached decision and discard it when its lifetime has elapsed.
    fn get(&mut self, token_hash: [u8; 32], now: Instant) -> Option<Authorization> {
        let cached = self
            .entries
            .get(&token_hash)
            .filter(|entry| entry.expires_at > now)
            .map(|entry| entry.authorization);
        if cached.is_none() {
            self.entries.remove(&token_hash);
        }
        cached
    }

    /// Insert a decision while expiring old entries and keeping the map bounded.
    fn insert(&mut self, token_hash: [u8; 32], authorization: Authorization, now: Instant) {
        self.entries.retain(|_hash, entry| entry.expires_at > now);
        if self.entries.len() >= MAX_CACHED_AUTHORIZATIONS {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_hash, entry)| entry.expires_at)
                .map(|(hash, _entry)| *hash);
            if let Some(hash) = oldest {
                self.entries.remove(&hash);
            }
        }
        let ttl = match authorization {
            Authorization::Allowed => ALLOWED_AUTHORIZATION_TTL,
            Authorization::Unauthenticated | Authorization::Forbidden => DENIED_AUTHORIZATION_TTL,
        };
        let Some(expires_at) = now.checked_add(ttl) else {
            return;
        };
        self.entries.insert(
            token_hash,
            CachedAuthorization {
                authorization,
                expires_at,
            },
        );
    }
}

/// A cached decision with its expiration instant.
struct CachedAuthorization {
    /// Authentication and authorization decision.
    authorization: Authorization,
    /// Instant after which the decision must be reviewed again.
    expires_at: Instant,
}

/// SHA-256 digest for a bearer token, so the cache never retains credentials.
fn token_hash(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// Failure to authorize a scrape because checks are busy or Kubernetes is unavailable.
enum AuthorizationError {
    /// The bounded concurrent review limit has been reached.
    Overloaded,
    /// A Kubernetes authentication or authorization review failed.
    Unavailable(io::Error),
}

/// The result of the authentication and authorization reviews.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Authorization {
    /// The scraper token is valid and may read metrics.
    Allowed,
    /// The scraper did not present a valid Kubernetes token.
    Unauthenticated,
    /// The token is valid but is not authorized for `/metrics`.
    Forbidden,
}

/// A failure to consult the Kubernetes authorization API.
fn auth_api_error() -> io::Error {
    io::Error::other("Kubernetes metrics authorization check failed")
}

/// `TokenReview` response data used to construct the `SubjectAccessReview`.
#[derive(Deserialize)]
struct TokenReview {
    /// Authentication result.
    status: TokenReviewStatus,
}

/// `TokenReview` authentication status.
#[derive(Deserialize)]
struct TokenReviewStatus {
    /// Whether the token has a valid identity.
    authenticated: bool,
    /// The authenticated Kubernetes identity.
    user: Option<KubernetesUser>,
}

/// Kubernetes identity returned by `TokenReview`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct KubernetesUser {
    /// User name.
    username: String,
    /// Optional stable user identifier.
    uid: Option<String>,
    /// Groups associated with the identity.
    #[serde(default)]
    groups: Vec<String>,
    /// Additional identity attributes.
    #[serde(default)]
    extra: HashMap<String, Vec<String>>,
}

/// `SubjectAccessReview` response data.
#[derive(Deserialize)]
struct SubjectAccessReview {
    /// Authorization decision.
    status: SubjectAccessReviewStatus,
}

/// `SubjectAccessReview` authorization decision.
#[derive(Deserialize)]
struct SubjectAccessReviewStatus {
    /// Whether the identity may get `/metrics`.
    allowed: bool,
}

/// `future`'s result, or a `TimedOut` error after `limit`.
///
/// # Errors
///
/// Returns `future`'s error, or `TimedOut`.
async fn within<T, F: Future<Output = io::Result<T>>>(limit: Duration, future: F) -> io::Result<T> {
    timeout(limit, future)
        .await
        .unwrap_or_else(|_elapsed| Err(io::ErrorKind::TimedOut.into()))
}

/// Read a request head of at most [`MAX_HEAD`] bytes and decide the reply.
///
/// # Errors
///
/// Returns the read error, or `UnexpectedEof` when the peer closes mid-head.
#[cfg(test)]
async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<Reply> {
    Ok(read_request_details(stream).await?.reply)
}

/// The parsed route and optional bearer credential from one HTTP request.
struct RequestDetails {
    /// The route result before authentication.
    reply: Reply,
    /// Valid bearer token from a single Authorization header.
    bearer_token: Option<String>,
}

/// Read a request head and retain its bearer credential for authorization.
///
/// # Errors
///
/// Returns the read error, or `UnexpectedEof` when the peer closes mid-head.
async fn read_request_details<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<RequestDetails> {
    let mut head = vec![0_u8; MAX_HEAD];
    let mut filled = 0_usize;
    loop {
        let Some(spare) = head.get_mut(filled..).filter(|spare| !spare.is_empty()) else {
            return Ok(RequestDetails {
                reply: Reply::HeadTooLarge,
                bearer_token: None,
            });
        };
        let read = stream.read(spare).await?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        filled = filled.checked_add(read).unwrap_or(MAX_HEAD);
        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut request = httparse::Request::new(&mut headers);
        match request.parse(head.get(..filled).unwrap_or_default()) {
            Ok(httparse::Status::Complete(_)) => {
                return Ok(RequestDetails {
                    reply: Reply::route(request.method, request.path),
                    bearer_token: bearer_token(request.headers),
                });
            },
            Ok(httparse::Status::Partial) => {},
            Err(httparse::Error::TooManyHeaders) => {
                return Ok(RequestDetails {
                    reply: Reply::HeadTooLarge,
                    bearer_token: None,
                });
            },
            Err(_) => {
                return Ok(RequestDetails {
                    reply: Reply::BadRequest,
                    bearer_token: None,
                });
            },
        }
    }
}

/// Parse one unambiguous RFC-style bearer authorization value.
fn bearer_token(headers: &[httparse::Header<'_>]) -> Option<String> {
    let mut authorization = None;
    for header in headers {
        if header.name.eq_ignore_ascii_case("authorization") {
            if authorization.is_some() {
                return None;
            }
            authorization = Some(header.value);
        }
    }
    let value = std::str::from_utf8(authorization?).ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer") || token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return None;
    }
    Some(token.to_owned())
}

/// The listener's possible answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reply {
    /// 200 with the Prometheus exposition, or 503 before the recorder exists.
    Metrics,
    /// 404 for any path but `/metrics`.
    NotFound,
    /// 405 for `/metrics` with a method other than GET.
    MethodNotAllowed,
    /// 401 when no valid bearer token is presented.
    Unauthorized,
    /// 403 when the authenticated identity is not authorized to scrape.
    Forbidden,
    /// 503 when Kubernetes authorization reviews cannot be completed.
    ServiceUnavailable,
    /// 400 for an unparsable head.
    BadRequest,
    /// 431 for a head over [`MAX_HEAD`] or [`MAX_HEADERS`].
    HeadTooLarge,
}

impl Reply {
    /// The reply for `method` and `path`, ignoring a query string on `/metrics`.
    fn route(method: Option<&str>, path: Option<&str>) -> Self {
        let path = path.map(|path| path.split_once('?').map_or(path, |(path, _)| path));
        match (method, path) {
            (Some("GET"), Some("/metrics")) => Self::Metrics,
            (_, Some("/metrics")) => Self::MethodNotAllowed,
            _ => Self::NotFound,
        }
    }

    /// The HTTP/1.1 response head and body, closing the connection.
    fn into_parts(self) -> (String, Cow<'static, str>) {
        let (status, body) = match self {
            Self::Metrics => {
                return match praxis_protocol::http::pingora::metrics::render_prometheus() {
                    Some(body) => response("200 OK", "", "text/plain; version=0.0.4; charset=utf-8", body.into()),
                    None => response(
                        "503 Service Unavailable",
                        "",
                        "text/plain",
                        "metrics recorder not installed\n".into(),
                    ),
                };
            },
            Self::NotFound => ("404 Not Found", "not found\n"),
            Self::MethodNotAllowed => ("405 Method Not Allowed", "method not allowed\n"),
            Self::Unauthorized => ("401 Unauthorized", "unauthorized\n"),
            Self::Forbidden => ("403 Forbidden", "forbidden\n"),
            Self::ServiceUnavailable => ("503 Service Unavailable", "authorization unavailable\n"),
            Self::BadRequest => ("400 Bad Request", "bad request\n"),
            Self::HeadTooLarge => ("431 Request Header Fields Too Large", "request head too large\n"),
        };
        let extra = match self {
            Self::MethodNotAllowed => "Allow: GET\r\n",
            Self::Unauthorized => "WWW-Authenticate: Bearer\r\n",
            Self::Metrics
            | Self::NotFound
            | Self::Forbidden
            | Self::ServiceUnavailable
            | Self::BadRequest
            | Self::HeadTooLarge => "",
        };
        response(status, extra, "text/plain", body.into())
    }
}

/// An HTTP/1.1 response head with `extra` header lines, closing the connection, and its body.
fn response(status: &str, extra: &str, content_type: &str, body: Cow<'static, str>) -> (String, Cow<'static, str>) {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n",
        body.len()
    );
    (head, body)
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
        }
    }

    #[test]
    fn an_unset_address_leaves_the_listener_off() {
        assert!(MetricsListener::from_env(env(&[])).unwrap().is_none(), "unset");
        assert!(
            MetricsListener::from_env(env(&[(ADDR_ENV, "")])).unwrap().is_none(),
            "empty"
        );
    }

    #[test]
    fn an_address_without_a_cert_or_key_is_refused() {
        let no_key = MetricsListener::from_env(env(&[(ADDR_ENV, "0.0.0.0:9443"), (CERT_ENV, "/c")])).unwrap_err();
        assert!(no_key.contains(KEY_ENV), "{no_key}");
        let no_cert = MetricsListener::from_env(env(&[(ADDR_ENV, "0.0.0.0:9443"), (KEY_ENV, "/k")])).unwrap_err();
        assert!(no_cert.contains(CERT_ENV), "{no_cert}");
        let hostname = MetricsListener::from_env(env(&[(ADDR_ENV, "metrics:9443")])).unwrap_err();
        assert!(hostname.contains("ip:port"), "{hostname}");
    }

    #[test]
    fn a_port_praxis_binds_is_refused() {
        let listener = MetricsListener::from_env(env(&[(ADDR_ENV, "0.0.0.0:9443"), (CERT_ENV, "/c"), (KEY_ENV, "/k")]))
            .unwrap()
            .unwrap();
        let config = |yaml: &str| Config::from_yaml(yaml).expect("config");
        let base = "listeners:\n  - name: web\n    address: \"0.0.0.0:8080\"\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters: []\n";
        listener.check_ports(&config(base)).unwrap();
        let clash = base.replace("8080", "9443");
        assert!(
            listener.check_ports(&config(&clash)).unwrap_err().contains("collides"),
            "listener"
        );
        let admin = format!("{base}admin:\n  address: \"127.0.0.1:9443\"\n");
        assert!(
            listener.check_ports(&config(&admin)).unwrap_err().contains("collides"),
            "admin"
        );
        let metrics = format!("{base}admin:\n  metrics_address: \"127.0.0.1:9443\"\n");
        assert!(
            listener
                .check_ports(&config(&metrics))
                .unwrap_err()
                .contains("collides"),
            "metrics"
        );
    }

    async fn reply_to(request: &[u8]) -> Reply {
        let mut reader = request;
        read_request(&mut reader).await.unwrap()
    }

    #[tokio::test]
    async fn only_get_metrics_is_served() {
        assert_eq!(
            reply_to(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").await,
            Reply::Metrics
        );
        assert_eq!(reply_to(b"GET /metrics?x=1 HTTP/1.1\r\n\r\n").await, Reply::Metrics);
        assert_eq!(
            reply_to(b"POST /metrics HTTP/1.1\r\n\r\n").await,
            Reply::MethodNotAllowed
        );
        assert_eq!(
            reply_to(b"HEAD /metrics HTTP/1.1\r\n\r\n").await,
            Reply::MethodNotAllowed
        );
        for path in ["/", "/api/kv", "/ready", "/metrics/", "/metricsx"] {
            let request = format!("GET {path} HTTP/1.1\r\n\r\n");
            assert_eq!(reply_to(request.as_bytes()).await, Reply::NotFound, "{path}");
        }
        assert_eq!(reply_to(b"\x00garbage\r\n\r\n").await, Reply::BadRequest);
    }

    #[tokio::test]
    async fn an_oversized_head_gets_431_without_reading_past_the_cap() {
        let mut long = b"GET /metrics HTTP/1.1\r\nX: ".to_vec();
        long.resize(MAX_HEAD.saturating_mul(2), b'a');
        assert_eq!(reply_to(&long).await, Reply::HeadTooLarge);
        let many: String = std::iter::once("GET /metrics HTTP/1.1\r\n".to_owned())
            .chain((0..=MAX_HEADERS).map(|index| format!("H{index}: v\r\n")))
            .chain(std::iter::once("\r\n".to_owned()))
            .collect();
        assert_eq!(reply_to(many.as_bytes()).await, Reply::HeadTooLarge);
    }

    #[test]
    fn authorization_cache_uses_shorter_lifetimes_for_denials() {
        let now = Instant::now();
        let allowed_hash = token_hash("allowed-token");
        let denied_hash = token_hash("denied-token");
        let mut cache = AuthorizationCache::default();
        cache.insert(allowed_hash, Authorization::Allowed, now);
        cache.insert(denied_hash, Authorization::Forbidden, now);

        assert_eq!(
            cache.get(allowed_hash, now + Duration::from_secs(59)),
            Some(Authorization::Allowed),
            "allow remains cached for 60 seconds"
        );
        assert_eq!(
            cache.get(denied_hash, now + Duration::from_secs(9)),
            Some(Authorization::Forbidden),
            "deny remains cached for 10 seconds"
        );
        assert_eq!(
            cache.get(denied_hash, now + Duration::from_secs(10)),
            None,
            "deny expires at 10 seconds"
        );
        assert_eq!(
            cache.get(allowed_hash, now + Duration::from_secs(60)),
            None,
            "allow expires at 60 seconds"
        );
    }

    #[test]
    fn authorization_cache_does_not_exceed_its_capacity() {
        let now = Instant::now();
        let mut cache = AuthorizationCache::default();
        for index in 0..=MAX_CACHED_AUTHORIZATIONS {
            let hash: [u8; 32] = Sha256::digest(index.to_be_bytes()).into();
            cache.insert(hash, Authorization::Unauthenticated, now);
        }

        assert_eq!(cache.entries.len(), MAX_CACHED_AUTHORIZATIONS, "bounded cache");
    }

    #[tokio::test]
    async fn a_peer_closing_mid_head_is_an_error() {
        let mut reader: &[u8] = b"GET /metrics HTTP/1.1\r\n";
        let err = read_request(&mut reader).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "{err}");
    }

    #[test]
    fn the_tls_config_requires_extended_master_secret() {
        praxis_tls::provider::install();
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/tmp/metrics-listener-ems");
        std::fs::create_dir_all(&dir).unwrap();
        let ca = certs::generate_ca("ems test CA").unwrap();
        let leaf = certs::generate_site_cert(&ca, "metrics").unwrap();
        let pair = CertKeyPair {
            cert_path: dir.join("tls.crt").display().to_string(),
            default: false,
            key_path: dir.join("tls.key").display().to_string(),
            server_names: Vec::new(),
        };
        std::fs::write(&pair.cert_path, &leaf.cert_pem).unwrap();
        std::fs::write(&pair.key_path, &leaf.key_pem).unwrap();
        let config = server_config(ReloadableCertResolver::new(&pair).unwrap()).unwrap();
        assert!(config.require_ems, "TLS 1.2 without EMS is outside SP 800-52r2");
        assert_eq!(config.alpn_protocols, vec![b"http/1.1".to_vec()], "HTTP/1.1 only");
    }

    #[test]
    fn a_405_names_get_and_every_reply_closes() {
        let (response, _) = Reply::MethodNotAllowed.into_parts();
        assert!(response.starts_with("HTTP/1.1 405 "), "{response}");
        assert!(response.contains("\r\nAllow: GET\r\n"), "{response}");
        assert!(response.contains("\r\nConnection: close\r\n"), "{response}");
    }
}
