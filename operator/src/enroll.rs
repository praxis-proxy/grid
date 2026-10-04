//! Site auto-enroll: redeem a one-time token for the grid identity.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

use clap::Args;
use k8s_openapi::{ByteString, api::core::v1::Secret};
use kube::{
    Api, Client,
    api::{ListParams, ObjectMeta, PostParams},
};
use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize as _, Zeroizing};

use crate::{crd::grid_network::GridNetwork, resources::tls_backend};

/// The enroll route on the enrollment service.
const ENROLL_PATH: &str = "/v1alpha1/enrollments";

/// Response size cap.
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// Retry schedule per startup step.
const STEP_BACKOFF: Backoff = Backoff {
    attempts: 10,
    initial: Duration::from_secs(2),
    max: Duration::from_secs(60),
};

/// Deadline for the whole run.
const ENROLL_DEADLINE: Duration = Duration::from_secs(15 * 60);

/// `app.kubernetes.io/managed-by` on the Secrets enrollment writes.
const MANAGED_BY: &str = "grid-operator";

/// Label naming the site an invite Secret was minted for.
const SITE_LABEL: &str = "grid.praxis.fast/site";

/// Recovery hint for a spent token.
const SPENT: &str = "A spent token holds the site name until the hub releases it, which is not yet supported. \
                     Reinstall the hub or enroll under a new site name.";

/// Auto-enroll configuration.
#[derive(Args, Debug, Clone)]
// clap panics on a duplicate default group id.
#[group(id = "enrollment")]
pub struct Config {
    /// Enroll on startup when the site identity Secret is absent.
    #[arg(long = "enroll", env = "GRID_ENROLL_ENABLED")]
    pub enabled: bool,

    /// Enrollment service base URL (https).
    #[arg(long = "enroll-url", env = "GRID_ENROLL_URL")]
    pub url: Option<String>,

    /// PEM bundle pinning the enrollment server.
    #[arg(long = "enroll-ca-file", env = "GRID_ENROLL_CA_FILE")]
    pub ca_file: Option<PathBuf>,

    /// Grid CA anchors, defaulting to the TLS pin.
    #[arg(long = "enroll-grid-ca-file", env = "GRID_ENROLL_GRID_CA_FILE")]
    pub grid_ca_file: Option<PathBuf>,

    /// Site name the token pins.
    #[arg(long = "enroll-site-name", env = "GRID_ENROLL_SITE_NAME")]
    pub site_name: Option<String>,

    /// Secret in the operator namespace holding the site token.
    #[arg(long = "enroll-token-secret", env = "GRID_ENROLL_TOKEN_SECRET")]
    pub token_secret: Option<String>,

    /// Key of the site token within that Secret.
    #[arg(
        long = "enroll-token-secret-key",
        env = "GRID_ENROLL_TOKEN_SECRET_KEY",
        default_value = "token"
    )]
    pub token_secret_key: String,

    /// Secret the site identity is written to, unless the `GridNetwork` names one.
    #[arg(
        long = "enroll-identity-secret",
        env = "GRID_ENROLL_IDENTITY_SECRET",
        default_value = "grid-site-identity"
    )]
    pub identity_secret: String,

    /// Secret the grid CA is written to, unless the `GridNetwork` names one.
    #[arg(long = "enroll-ca-secret", env = "GRID_ENROLL_CA_SECRET", default_value = "grid-ca")]
    pub ca_secret: String,
}

/// Why enrollment did not complete.
#[derive(Debug, thiserror::Error)]
pub enum EnrollError {
    /// The configuration cannot work as given.
    #[error("enrollment misconfigured: {0}")]
    Config(String),

    /// The service refused the token.
    #[error("site token rejected ({0}). If it expired or was revoked, mint a new one. {SPENT}")]
    TokenRejected(String),

    /// The site name is already enrolled.
    #[error("site name already enrolled ({0}). {SPENT}")]
    NameTaken(String),

    /// The service rejected the request.
    #[error("enrollment refused: {0}")]
    Refused(String),

    /// The request was sent but no usable answer came back.
    #[error("enrollment failed after the request was sent ({0}). The token may be spent. {SPENT}")]
    MaybeSpent(String),

    /// A pre-send step exhausted its retries.
    #[error("{what} failed after {attempts} attempts: {last}")]
    Exhausted {
        /// The step that failed.
        what: &'static str,
        /// Attempts made.
        attempts: u32,
        /// The last failure.
        last: String,
    },

    /// The service answered with an unusable certificate.
    #[error("enrollment response invalid ({0}). The token is spent. {SPENT}")]
    InvalidResponse(String),

    /// The certificate was issued but could not be stored.
    #[error("enrolled but the identity was not stored ({0}). The token is spent. {SPENT}")]
    NotStored(String),

    /// A Kubernetes call failed in a way retrying cannot fix.
    #[error("kubernetes: {0}")]
    Kube(String),

    /// The whole run passed its deadline, in seconds.
    #[error("enrollment did not finish within {0}s. A token already sent may be spent. {SPENT}")]
    TimedOut(u64),
}

/// Capped exponential retry schedule.
#[derive(Clone, Copy, Debug)]
struct Backoff {
    /// Total attempts, including the first.
    attempts: u32,
    /// Delay after the first failure.
    initial: Duration,
    /// Delay cap.
    max: Duration,
}

impl Backoff {
    /// Delay after failed attempt `attempt` (1-based).
    fn delay(self, attempt: u32) -> Duration {
        self.initial
            .saturating_mul(2_u32.saturating_pow(attempt.saturating_sub(1)))
            .min(self.max)
    }
}

/// One attempt's result.
enum Attempt<T> {
    /// Succeeded.
    Done(T),
    /// Failed retryably, with the reason.
    Retry(String),
}

/// Run `attempt` under `backoff`.
async fn with_backoff<T, F, Fut>(backoff: Backoff, what: &'static str, mut attempt: F) -> Result<T, EnrollError>
where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<Attempt<T>, EnrollError>> + Send,
    T: Send,
{
    let mut last = String::new();
    for n in 1..=backoff.attempts {
        match attempt().await? {
            Attempt::Done(value) => return Ok(value),
            Attempt::Retry(reason) => last = reason,
        }
        if n < backoff.attempts {
            let delay = backoff.delay(n);
            tracing::warn!(what, attempt = n, retry_in_ms = delay.as_millis(), error = %last, "retrying");
            tokio::time::sleep(delay).await;
        }
    }
    Err(EnrollError::Exhausted {
        what,
        attempts: backoff.attempts,
        last,
    })
}

/// Validated settings.
struct Settings {
    /// Service base URL, https.
    base: Url,
    /// Client pinned to the service's CA bundle.
    http: reqwest::Client,
    /// PEM bundle the returned grid CA must match.
    anchor_pem: String,
    /// Site name the token pins.
    site_name: String,
    /// Secret holding the token.
    token_secret: String,
    /// Key within that Secret.
    token_key: String,
    /// Retry schedule per step.
    backoff: Backoff,
    /// Where the identity goes when no `GridNetwork` names its Secrets.
    defaults: Target,
}

impl Settings {
    /// Validate `config`, reading the CA bundles and building the client.
    fn from_config(config: &Config) -> Result<Self, EnrollError> {
        let raw = required(config.url.as_deref(), "GRID_ENROLL_URL")?;
        let base = Url::parse(&raw).map_err(|e| EnrollError::Config(format!("GRID_ENROLL_URL: {e}")))?;
        // Plaintext would expose the token.
        if base.scheme() != "https" {
            return Err(EnrollError::Config("GRID_ENROLL_URL must be an https URL".to_owned()));
        }
        let site_name = required(config.site_name.as_deref(), "GRID_ENROLL_SITE_NAME")?;
        certs::validate_site_name(&site_name)
            .map_err(|e| EnrollError::Config(format!("GRID_ENROLL_SITE_NAME: {e}")))?;
        let ca_file = config
            .ca_file
            .as_deref()
            .ok_or_else(|| EnrollError::Config("GRID_ENROLL_CA_FILE is required".to_owned()))?;
        let ca_pem = read_pem(ca_file)?;
        let http = http_client(pem_roots(&ca_pem, "GRID_ENROLL_CA_FILE")?)?;
        let anchor_pem = config.grid_ca_file.as_deref().map_or(Ok(ca_pem), read_anchors)?;
        Ok(Self {
            base,
            http,
            anchor_pem,
            site_name,
            token_secret: required(config.token_secret.as_deref(), "GRID_ENROLL_TOKEN_SECRET")?,
            token_key: config.token_secret_key.clone(),
            backoff: STEP_BACKOFF,
            defaults: Target {
                site_secret: required(Some(&config.identity_secret), "GRID_ENROLL_IDENTITY_SECRET")?,
                ca_secret: required(Some(&config.ca_secret), "GRID_ENROLL_CA_SECRET")?,
            },
        })
    }

    /// `path` under the base URL.
    fn endpoint(&self, path: &str) -> String {
        format!("{}{path}", self.base.as_str().trim_end_matches('/'))
    }
}

/// Read a PEM file.
fn read_pem(path: &Path) -> Result<String, EnrollError> {
    std::fs::read_to_string(path).map_err(|e| EnrollError::Config(format!("reading {}: {e}", path.display())))
}

/// Read the grid CA anchors, refusing a bundle with no certificate.
fn read_anchors(path: &Path) -> Result<String, EnrollError> {
    let pem = read_pem(path)?;
    pem_roots(&pem, "GRID_ENROLL_GRID_CA_FILE")?;
    Ok(pem)
}

/// The certificates in `pem`, refusing a bundle with none.
fn pem_roots(pem: &str, name: &str) -> Result<Vec<reqwest::Certificate>, EnrollError> {
    let roots = reqwest::Certificate::from_pem_bundle(pem.as_bytes())
        .map_err(|e| EnrollError::Config(format!("{name}: {e}")))?;
    if roots.is_empty() {
        return Err(EnrollError::Config(format!("{name} holds no certificate")));
    }
    Ok(roots)
}

/// A non-blank setting, or a config error naming it.
fn required(value: Option<&str>, name: &str) -> Result<String, EnrollError> {
    value
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| EnrollError::Config(format!("{name} is required")))
}

/// The site identity Secrets a `GridNetwork` names.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Target {
    /// `kubernetes.io/tls` Secret: `tls.crt`, `tls.key`.
    site_secret: String,
    /// Secret holding `ca.crt`.
    ca_secret: String,
}

/// The identity Secrets to enroll into. The token pins the grid, so no `GridNetwork` is needed.
async fn resolve_target(
    client: &Client,
    namespace: &str,
    defaults: &Target,
    backoff: Backoff,
) -> Result<Target, EnrollError> {
    let networks: Api<GridNetwork> = Api::all(client.clone());
    let networks = &networks;
    with_backoff(backoff, "listing GridNetworks", || async move {
        match networks.list(&ListParams::default()).await {
            Ok(list) => target_for(&list.items, namespace, defaults).map(Attempt::Done),
            Err(e) => Ok(Attempt::Retry(e.to_string())),
        }
    })
    .await
}

/// The Secrets the sole `GridNetwork` names, each falling back to `defaults`, all in `namespace`.
fn target_for(networks: &[GridNetwork], namespace: &str, defaults: &Target) -> Result<Target, EnrollError> {
    let tls = match networks {
        [] => return Ok(defaults.clone()),
        [network] => &network.spec.tls,
        many => {
            return Err(EnrollError::Config(format!(
                "{} GridNetworks found, but one operator serves one grid",
                many.len()
            )));
        },
    };
    let (site, ca) = (tls.site_secret_ref.as_ref(), tls.ca_secret_ref.as_ref());
    if let Some(other) = [site, ca].into_iter().flatten().find(|r| r.namespace != namespace) {
        return Err(EnrollError::Config(format!(
            "Secret {}/{} is outside the operator namespace {namespace}",
            other.namespace, other.name
        )));
    }
    Ok(Target {
        site_secret: site.map_or_else(|| defaults.site_secret.clone(), |r| r.name.clone()),
        ca_secret: ca.map_or_else(|| defaults.ca_secret.clone(), |r| r.name.clone()),
    })
}

/// Result of a create.
#[derive(Debug)]
enum Created {
    /// Created.
    Yes,
    /// Already there.
    AlreadyExists,
    /// Transient failure.
    Retry(String),
}

/// Secret access.
trait Store {
    /// Whether Secret `name` exists, without reading its data.
    fn exists(&self, name: &str) -> impl Future<Output = Result<bool, EnrollError>> + Send;
    /// Secret `name`, if present.
    fn get(&self, name: &str) -> impl Future<Output = Result<Option<Secret>, EnrollError>> + Send;
    /// Create `secret`, reporting a conflict.
    fn create(&self, secret: &Secret) -> impl Future<Output = Result<Created, EnrollError>> + Send;
    /// Dry-run creating `secret`, refusing what the API server would refuse.
    fn check_create(&self, secret: &Secret) -> impl Future<Output = Result<Attempt<()>, EnrollError>> + Send;
}

/// Kubernetes-backed [`Store`] in the operator namespace.
struct KubeStore(Api<Secret>);

impl Store for KubeStore {
    async fn exists(&self, name: &str) -> Result<bool, EnrollError> {
        self.0
            .get_metadata_opt(name)
            .await
            .map(|meta| meta.is_some())
            .map_err(|e| EnrollError::Kube(format!("reading Secret {name}: {e}")))
    }

    async fn get(&self, name: &str) -> Result<Option<Secret>, EnrollError> {
        self.0
            .get_opt(name)
            .await
            .map_err(|e| EnrollError::Kube(format!("reading Secret {name}: {e}")))
    }

    async fn create(&self, secret: &Secret) -> Result<Created, EnrollError> {
        let name = secret.metadata.name.as_deref().unwrap_or_default();
        match self.0.create(&PostParams::default(), secret).await {
            Ok(_) => Ok(Created::Yes),
            Err(kube::Error::Api(status)) if status.code == 409 => Ok(Created::AlreadyExists),
            Err(e) => transient(name, &e).map(Created::Retry),
        }
    }

    async fn check_create(&self, secret: &Secret) -> Result<Attempt<()>, EnrollError> {
        let params = PostParams {
            dry_run: true,
            ..PostParams::default()
        };
        let name = secret.metadata.name.as_deref().unwrap_or_default();
        let namespace = self.0.namespace().unwrap_or_default();
        dry_run_result(namespace, name, self.0.create(&params, secret).await.map(drop))
    }
}

/// Accept a dry-run create or a conflict, retry what [`transient`] retries, and refuse the rest as config.
fn dry_run_result(namespace: &str, name: &str, result: Result<(), kube::Error>) -> Result<Attempt<()>, EnrollError> {
    match result {
        Ok(()) => Ok(Attempt::Done(())),
        Err(kube::Error::Api(status)) if status.code == 409 => Ok(Attempt::Done(())),
        Err(e) => transient(name, &e).map(Attempt::Retry).map_err(|_refused| {
            EnrollError::Config(format!(
                "cannot write Secret {namespace}/{name}: {e}, fix RBAC or admission before enrolling"
            ))
        }),
    }
}

/// Classify a Kubernetes error as retryable or hard.
fn transient(name: &str, error: &kube::Error) -> Result<String, EnrollError> {
    let message = format!("writing Secret {name}: {error}");
    if let kube::Error::Api(status) = error
        && (400..500).contains(&status.code)
        && status.code != 429
    {
        return Err(EnrollError::Kube(message));
    }
    Ok(message)
}

/// Value `key` of `secret`.
fn secret_value(secret: Secret, key: &str) -> Option<Zeroizing<Vec<u8>>> {
    secret
        .data
        .and_then(|mut data| data.remove(key))
        .map(|bytes| Zeroizing::new(bytes.0))
}

/// The site token, refused if minted for another site.
async fn site_token<S: Store + Sync>(store: &S, settings: &Settings) -> Result<Zeroizing<String>, EnrollError> {
    let name = &settings.token_secret;
    let secret = store
        .get(name)
        .await?
        .ok_or_else(|| EnrollError::Config(format!("site token Secret {name} not found")))?;
    let site = &settings.site_name;
    if let Some(minted_for) = secret
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(SITE_LABEL))
        && minted_for != site
    {
        return Err(EnrollError::Config(format!(
            "Secret {name} holds the token for site {minted_for}, not {site}"
        )));
    }
    let raw = secret_value(secret, &settings.token_key)
        .ok_or_else(|| EnrollError::Config(format!("Secret {name} has no key {}", settings.token_key)))?;
    let token = std::str::from_utf8(&raw)
        .map_err(|_e| EnrollError::Config(format!("Secret {name}: token is not UTF-8")))?
        .trim();
    if token.is_empty() {
        return Err(EnrollError::Config(format!("Secret {name}: token is empty")));
    }
    Ok(Zeroizing::new(token.to_owned()))
}

/// Refuse an existing CA Secret that does not hold exactly the anchors, reporting whether one exists.
async fn check_existing_ca<S: Store + Sync>(store: &S, target: &Target, anchors: &str) -> Result<bool, EnrollError> {
    let name = &target.ca_secret;
    if *name == target.site_secret {
        return Ok(false);
    }
    let Some(secret) = store.get(name).await? else {
        return Ok(false);
    };
    let held = held_ca(secret).unwrap_or_default();
    if !within(&held, anchors) {
        return Err(EnrollError::Config(format!(
            "CA Secret {name} holds a CA outside the pinned grid CAs, fix or delete it before enrolling"
        )));
    }
    if !within(anchors, &held) {
        return Err(EnrollError::Config(format!(
            "CA Secret {name} misses a pinned grid CA the hub may return, add every pinned CA to it or delete it before enrolling"
        )));
    }
    Ok(true)
}

/// Dry-run the Secret writes so a refusal surfaces before the token is spent.
async fn check_writable<S: Store + Sync>(
    store: &S,
    target: &Target,
    ca_present: bool,
    backoff: Backoff,
) -> Result<(), EnrollError> {
    let empty = Verified {
        cert: String::new(),
        ca: String::new(),
    };
    let placeholder = &prepare(target, empty, Zeroizing::default());
    with_backoff(backoff, "dry-running the identity Secret writes", || async move {
        if let Some((ca_secret, _)) = &placeholder.ca
            && !ca_present
            && let Attempt::Retry(reason) = store.check_create(ca_secret).await?
        {
            return Ok(Attempt::Retry(reason));
        }
        store.check_create(&placeholder.site).await
    })
    .await
}

/// The `ca.crt` bundle `secret` holds.
fn held_ca(secret: Secret) -> Option<String> {
    let bytes = secret.data?.remove("ca.crt")?;
    String::from_utf8(bytes.0).ok()
}

/// Whether every certificate in `bundle` is in `anchors`.
fn within(bundle: &str, anchors: &str) -> bool {
    certs::bundle_within(bundle, anchors).unwrap_or(false)
}

/// How a run ended.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// The identity Secret already existed.
    AlreadyEnrolled,
    /// Enrolled under this SPIFFE ID.
    Enrolled(String),
    /// Another writer stored an identity first.
    Discarded,
}

/// Refuse a target other than `defaults` when the identity already sits in `defaults`: its token is spent.
async fn check_not_enrolled_elsewhere<S: Store + Sync>(
    store: &S,
    defaults: &Target,
    target: &Target,
) -> Result<(), EnrollError> {
    let enrolled = &defaults.site_secret;
    if enrolled != &target.site_secret && store.exists(enrolled).await? {
        return Err(EnrollError::Config(format!(
            "the site already enrolled into Secret {enrolled}, but the GridNetwork names {}; point spec.tls.siteSecretRef and caSecretRef at the enrolled Secrets",
            target.site_secret
        )));
    }
    Ok(())
}

/// Enroll unless already enrolled, then store the identity.
async fn enroll<S: Store + Sync>(store: &S, settings: &Settings, target: &Target) -> Result<Outcome, EnrollError> {
    if store.exists(&target.site_secret).await? {
        return Ok(Outcome::AlreadyEnrolled);
    }
    check_not_enrolled_elsewhere(store, &settings.defaults, target).await?;
    let ca_present = check_existing_ca(store, target, &settings.anchor_pem).await?;
    check_writable(store, target, ca_present, settings.backoff).await?;
    let token = site_token(store, settings).await?;
    let certs::GeneratedCsr { csr_pem, key_pem } =
        certs::generate_csr(&settings.site_name).map_err(|e| EnrollError::Config(format!("generating a CSR: {e}")))?;
    let enrollment = Box::pin(redeem(settings, &token, &csr_pem)).await?;
    let verified = checked_identity(&enrollment, &csr_pem, settings)?;
    if !certs::has_svid_profile(&verified.cert).unwrap_or(false) {
        tracing::warn!("issued certificate lacks the X.509-SVID profile, peers in spiffe trust mode will reject it");
    }
    let prepared = prepare(target, verified, key_pem);
    let anchors = &settings.anchor_pem;
    let written = with_backoff(settings.backoff, "writing the identity Secrets", || {
        write(store, &prepared, anchors)
    })
    .await
    .map_err(|e| EnrollError::NotStored(e.to_string()))?;
    Ok(match written {
        Written::Stored => Outcome::Enrolled(certs::spiffe_id(&settings.site_name)),
        Written::Discarded => Outcome::Discarded,
    })
}

/// Enroll request body.
#[derive(Serialize)]
struct EnrollmentRequest<'csr> {
    /// PKCS#10 request, PEM.
    csr: &'csr str,
}

/// The enroll response fields a site keeps.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Enrollment {
    /// Issued leaf, PEM.
    certificate: String,
    /// Grid CA, PEM.
    ca_certificate: String,
}

/// Error body the service returns.
#[derive(Deserialize)]
struct ErrorBody {
    /// Machine-readable code.
    error: String,
    /// Human message.
    message: String,
}

/// POST the CSR, retrying only pre-send failures.
async fn redeem(settings: &Settings, token: &str, csr_pem: &str) -> Result<Enrollment, EnrollError> {
    let url = settings.endpoint(ENROLL_PATH);
    let (http, url, body) = (&settings.http, &url, &EnrollmentRequest { csr: csr_pem });
    with_backoff(settings.backoff, "reaching the enrollment service", || async move {
        let response = match http.post(url).bearer_auth(token).json(body).send().await {
            Ok(response) => response,
            Err(e) => return pre_send(&e),
        };
        let status = response.status();
        let bytes = read_capped(response).await.map_err(EnrollError::MaybeSpent)?;
        classify(status, &bytes).map(Attempt::Done)
    })
    .await
}

/// Retry connect failures, but not TLS failures.
fn pre_send<T>(error: &reqwest::Error) -> Result<Attempt<T>, EnrollError> {
    if error.is_builder() {
        return Err(EnrollError::Config(error_chain(error)));
    }
    if is_tls_failure(error) {
        return Err(EnrollError::Config(format!(
            "TLS to the enrollment service failed, check GRID_ENROLL_CA_FILE: {}",
            error_chain(error)
        )));
    }
    if error.is_connect() {
        return Ok(Attempt::Retry(error_chain(error)));
    }
    Err(EnrollError::MaybeSpent(error_chain(error)))
}

/// Whether `error`'s chain holds a TLS failure.
fn is_tls_failure(error: &(dyn std::error::Error + 'static)) -> bool {
    std::iter::successors(Some(error), |err| {
        // io::Error::source skips its wrapped error.
        err.downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            .map(|inner| -> &(dyn std::error::Error + 'static) { inner })
            .or_else(|| err.source())
    })
    .any(tls_backend::is_tls_error)
}

/// Decode an enroll response.
fn classify(status: StatusCode, body: &[u8]) -> Result<Enrollment, EnrollError> {
    if status.is_success() {
        return serde_json::from_slice(body)
            .map_err(|e| EnrollError::InvalidResponse(format!("decoding the enrollment: {e}")));
    }
    let error = serde_json::from_slice::<ErrorBody>(body).ok();
    let detail = error.as_ref().map_or_else(
        || status.to_string(),
        |err| format!("{status} {}: {}", err.error, err.message),
    );
    Err(match status {
        StatusCode::UNAUTHORIZED => EnrollError::TokenRejected(detail),
        StatusCode::CONFLICT if error.is_some_and(|err| err.error == "name_taken") => EnrollError::NameTaken(detail),
        _ if status.is_server_error() => EnrollError::MaybeSpent(detail),
        _ => EnrollError::Refused(detail),
    })
}

/// Read a body, refusing one past [`MAX_RESPONSE_BYTES`].
async fn read_capped(mut response: reqwest::Response) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| error_chain(&e))? {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err("enrollment response exceeds the size limit".to_owned());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// An error with its source chain.
fn error_chain(error: &dyn std::error::Error) -> String {
    let mut out = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

/// A leaf and CA that passed [`checked_identity`].
struct Verified {
    /// Issued leaf, PEM.
    cert: String,
    /// Pinned grid CA, PEM.
    ca: String,
}

/// Check the identity against the pin, site name, and key.
fn checked_identity(enrollment: &Enrollment, csr_pem: &str, settings: &Settings) -> Result<Verified, EnrollError> {
    let invalid = EnrollError::InvalidResponse;
    let ca = certs::anchored_ca(&enrollment.ca_certificate, &settings.anchor_pem).map_err(|e| {
        if e == certs::VerifyError::NotAnchored {
            invalid("returned CA is not the pinned grid CA".to_owned())
        } else {
            invalid(format!("returned CA: {e}"))
        }
    })?;
    let cert = certs::leaf_only(&enrollment.certificate).map_err(|e| invalid(format!("issued certificate: {e}")))?;
    let site = &settings.site_name;
    certs::verify_site_cert(&ca, &cert, site).map_err(|e| {
        invalid(format!(
            "issued certificate is not valid for {site} ({e}), possible interception, contact the hub"
        ))
    })?;
    let leaf_key = certs::cert_public_key(&cert).map_err(|e| invalid(e.to_string()))?;
    let csr_key = certs::csr_public_key(csr_pem).map_err(|e| invalid(e.to_string()))?;
    if leaf_key != csr_key {
        return Err(invalid(
            "issued certificate does not carry this site's key, possible interception, contact the hub".to_owned(),
        ));
    }
    Ok(Verified { cert, ca })
}

/// The Secrets to write.
struct Prepared {
    /// CA Secret and its PEM, when separate.
    ca: Option<(Secret, String)>,
    /// The site identity Secret.
    site: Secret,
}

impl Drop for Prepared {
    fn drop(&mut self) {
        self.site
            .data
            .iter_mut()
            .flat_map(BTreeMap::values_mut)
            .for_each(|value| value.0.zeroize());
    }
}

/// Build the Secrets for `verified`.
fn prepare(target: &Target, verified: Verified, mut key: Zeroizing<String>) -> Prepared {
    let bytes = |text: String| ByteString(text.into_bytes());
    let mut site = BTreeMap::from([
        ("tls.crt".to_owned(), bytes(verified.cert)),
        ("tls.key".to_owned(), bytes(std::mem::take(&mut *key))),
    ]);
    let ca = if target.site_secret == target.ca_secret {
        site.insert("ca.crt".to_owned(), bytes(verified.ca));
        None
    } else {
        let data = BTreeMap::from([("ca.crt".to_owned(), bytes(verified.ca.clone()))]);
        Some((secret(&target.ca_secret, "Opaque", data), verified.ca))
    };
    Prepared {
        ca,
        site: secret(&target.site_secret, "kubernetes.io/tls", site),
    }
}

/// A Secret labelled as written by enrollment.
fn secret(name: &str, type_: &str, data: BTreeMap<String, ByteString>) -> Secret {
    Secret {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            labels: Some(BTreeMap::from([(
                "app.kubernetes.io/managed-by".to_owned(),
                MANAGED_BY.to_owned(),
            )])),
            ..ObjectMeta::default()
        },
        type_: Some(type_.to_owned()),
        data: Some(data),
        ..Secret::default()
    }
}

/// Whether our identity was the one stored.
enum Written {
    /// Stored.
    Stored,
    /// Another writer got there first.
    Discarded,
}

/// Write the CA Secret if absent, then the identity Secret.
async fn write<S: Store + Sync>(
    store: &S,
    prepared: &Prepared,
    anchors: &str,
) -> Result<Attempt<Written>, EnrollError> {
    if let Some((ca_secret, ca_pem)) = &prepared.ca {
        match store.create(ca_secret).await? {
            Created::Yes => {},
            Created::Retry(reason) => return Ok(Attempt::Retry(reason)),
            Created::AlreadyExists => keep_existing_ca(store, ca_secret, ca_pem, anchors).await?,
        }
    }
    // Written last: it marks the site enrolled.
    match store.create(&prepared.site).await? {
        Created::Yes => Ok(Attempt::Done(Written::Stored)),
        Created::AlreadyExists => {
            tracing::warn!(
                secret = ?prepared.site.metadata.name,
                "site identity Secret appeared concurrently, discarding the issued certificate and its spent token"
            );
            Ok(Attempt::Done(Written::Discarded))
        },
        Created::Retry(reason) => Ok(Attempt::Retry(reason)),
    }
}

/// Accept an existing CA Secret holding our CA and only anchors.
async fn keep_existing_ca<S: Store + Sync>(
    store: &S,
    ca_secret: &Secret,
    ca_pem: &str,
    anchors: &str,
) -> Result<(), EnrollError> {
    let name = ca_secret.metadata.name.as_deref().unwrap_or_default();
    if store
        .get(name)
        .await?
        .and_then(held_ca)
        .is_some_and(|held| within(&held, anchors) && within(ca_pem, &held))
    {
        Ok(())
    } else {
        Err(EnrollError::Config(format!(
            "CA Secret {name} holds another CA, not overwritten"
        )))
    }
}

/// A client that trusts only `roots`.
fn http_client(roots: Vec<reqwest::Certificate>) -> Result<reqwest::Client, EnrollError> {
    reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .tls_certs_only(roots)
        .build()
        .map_err(|e| EnrollError::Config(format!("building the enrollment client: {e}")))
}

/// Resolve the target, then enroll into it.
async fn run(client: &Client, settings: &Settings) -> Result<(), EnrollError> {
    let namespace = client.default_namespace().to_owned();
    let target = Box::pin(resolve_target(client, &namespace, &settings.defaults, settings.backoff)).await?;
    let store = KubeStore(Api::namespaced(client.clone(), &namespace));
    let site = &target.site_secret;
    match Box::pin(enroll(&store, settings, &target)).await? {
        Outcome::AlreadyEnrolled => tracing::info!(secret = %site, "site identity present, enrollment skipped"),
        Outcome::Enrolled(spiffe_id) => tracing::info!(%spiffe_id, secret = %site, "site enrolled"),
        Outcome::Discarded => tracing::info!(secret = %site, "site identity stored by another writer"),
    }
    Ok(())
}

/// Enroll this site unless its identity Secret already exists.
///
/// # Errors
///
/// Returns [`EnrollError`] when enrollment does not complete.
pub async fn ensure_enrolled(client: &Client, config: &Config) -> Result<(), EnrollError> {
    let settings = Settings::from_config(config)?;
    tokio::time::timeout(ENROLL_DEADLINE, Box::pin(run(client, &settings)))
        .await
        .map_err(|_elapsed| EnrollError::TimedOut(ENROLL_DEADLINE.as_secs()))?
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests;
