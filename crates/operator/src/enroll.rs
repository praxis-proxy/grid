//! Site auto-enroll: redeem a one-time token for the grid identity.

use std::{collections::BTreeMap, time::Duration};

use k8s_openapi::{ByteString, api::core::v1::Secret};
use kube::{
    Api, Client,
    api::{ListParams, ObjectMeta, Patch, PatchParams, PostParams},
};
use reqwest::Url;
use zeroize::{Zeroize as _, Zeroizing};

use crate::{
    crd::grid_network::GridNetwork,
    operator_config::{BundleRef, InviteSecretRef},
    resources::tls_backend,
};


/// Response size cap.
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// Retry schedule per enrollment step: about one poll round, so a step that keeps failing
/// returns to the poll loop, where a config edit is seen, instead of holding it.
const STEP_BACKOFF: Backoff = Backoff {
    initial: Duration::from_secs(2),
    max: Duration::from_secs(60),
    budget: Duration::from_secs(15),
    started: None,
};

/// `app.kubernetes.io/managed-by` on the Secrets enrollment writes.
const MANAGED_BY: &str = "grid-operator";

/// Label naming the site an invite Secret was minted for.
const SITE_LABEL: &str = "grid.praxis.fast/site";

/// Recovery hint for a spent token.
const SPENT: &str = "Delete the site's enrollment on the hub with enrollment-admin, then mint a new invite with \
     allowDeletedName as a grid-admin that also holds enrollment-admin.";

/// Secret the site identity is written to when no `GridNetwork` names one.
pub const IDENTITY_SECRET: &str = "grid-site-identity";

/// Secret the grid CA is written to when no `GridNetwork` names one.
pub const CA_SECRET: &str = "grid-ca";

/// Why enrollment did not complete.
#[derive(Debug, thiserror::Error)]
pub enum EnrollError {
    /// The configuration cannot work as given.
    #[error("enrollment misconfigured: {0}")]
    Config(String),

    /// The signer's serving certificate does not chain to the pinned bundle.
    #[error("TLS to the enrollment service failed, check site.signerEndpoint.caBundleRef: {0}")]
    SignerUntrusted(String),

    /// The signer's serving certificate is outside its validity window. Nothing was sent,
    /// and the next poll retries, since the signer recovers by rotating it.
    #[error("the enrollment service's serving certificate is expired or not yet valid; retried on the next poll: {0}")]
    SignerLapsed(String),

    /// An object the local `GridSite` names does not exist.
    #[error("{0}")]
    RefNotFound(String),

    /// The invite was minted for another site.
    #[error("{0}")]
    NameMismatch(String),

    /// The service refused the token.
    #[error("site token rejected ({0}). If it expired or was revoked, mint a new one. {SPENT}")]
    TokenRejected(String),

    /// The site name is already enrolled.
    #[error("site name already enrolled ({0}). {SPENT}")]
    NameTaken(String),

    /// The site name's enrollment was deleted and the token does not re-admit it.
    #[error(
        "site name was deleted ({0}). The token is spent. Mint a new invite with allowDeletedName as a grid-admin \
         that also holds enrollment-admin."
    )]
    NameDeleted(String),

    /// The service rejected the request.
    #[error("enrollment refused: {0}")]
    Refused(String),

    /// The request was sent but no usable answer came back, or the token was refused after an
    /// earlier request got none. `refused` is set for the refusal: it is not sent again with
    /// the same inputs, since the earlier request may have spent the token.
    #[error("enrollment failed after the request was sent ({detail}). The token may be spent. {SPENT}")]
    MaybeSpent {
        /// What happened.
        detail: String,
        /// Whether the signer refused the token after an unanswered request.
        refused: bool,
    },

    /// The service answered with an unusable certificate.
    #[error("enrollment response invalid ({0}). The token is spent. {SPENT}")]
    InvalidResponse(String),

    /// The certificate was issued but could not be stored.
    #[error("enrolled but the identity was not stored ({0}). The token is spent. {SPENT}")]
    NotStored(String),

    /// A Secret by the pending name was not written by enrollment, so it is never adopted.
    #[error("{0}")]
    PendingForeign(String),
    /// The service could not be reached within the step, so nothing was sent. Retried on the
    /// next poll.
    #[error("the enrollment service could not be reached ({0}); retried on the next poll")]
    Unreachable(String),
    /// A Kubernetes call failed. Retried on the next poll.
    #[error("kubernetes: {0}")]
    Kube(String),
}

/// Capped exponential retry schedule, bounded by a time budget.
#[derive(Clone, Copy, Debug)]
struct Backoff {
    /// Delay after the first failure.
    initial: Duration,
    /// Delay cap.
    max: Duration,
    /// How long retrying may go on before the step gives up and returns.
    budget: Duration,
    /// When the budget started, shared by every retry of one step; `None` starts it at the
    /// first retry of each call.
    started: Option<tokio::time::Instant>,
}

impl Backoff {
    /// This schedule with its budget started now, so the calls that share it spend one budget.
    fn started_now(self) -> Self {
        Self {
            started: Some(tokio::time::Instant::now()),
            ..self
        }
    }

    /// Delay after failed attempt `attempt` (1-based), spread by [`jittered`].
    fn delay(self, attempt: u32) -> Duration {
        jittered(
            self.initial
                .saturating_mul(2_u32.saturating_pow(attempt.saturating_sub(1)))
                .min(self.max),
        )
    }
}

/// `wait` plus up to a tenth of itself, so independent sites that failed
/// together do not come back in step and herd on the enrollment service.
pub(crate) fn jittered(wait: Duration) -> Duration {
    let spread = u64::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
    );
    let tenth = wait
        .as_millis()
        .checked_div(10)
        .and_then(|ms| u64::try_from(ms).ok())
        .unwrap_or(0);
    wait.saturating_add(Duration::from_millis(spread.checked_rem(tenth).unwrap_or(0)))
}

/// One attempt's result.
enum Attempt<T> {
    /// Succeeded.
    Done(T),
    /// Failed retryably, with the reason.
    Retry(String),
}

/// Run `attempt` under `backoff` until it is done, or its budget would run out, when the
/// last reason is given up as `give_up`'s error for the next poll to retry.
async fn with_backoff<T, F, Fut>(
    backoff: Backoff,
    what: &'static str,
    give_up: fn(String) -> EnrollError,
    mut attempt: F,
) -> Result<T, EnrollError>
where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<Attempt<T>, EnrollError>> + Send,
    T: Send,
{
    let started = backoff.started.unwrap_or_else(tokio::time::Instant::now);
    let mut failures = 0_u32;
    loop {
        match attempt().await? {
            Attempt::Done(value) => return Ok(value),
            Attempt::Retry(reason) => {
                failures = failures.saturating_add(1);
                let delay = backoff.delay(failures);
                if started.elapsed().saturating_add(delay) > backoff.budget {
                    tracing::warn!(what, attempts = failures, error = %reason, "giving up until the next poll");
                    return Err(give_up(reason));
                }
                tracing::warn!(what, attempt = failures, retry_in_ms = delay.as_millis(), error = %reason, "retrying");
                tokio::time::sleep(delay).await;
            },
        }
    }
}

/// Validated settings.
struct Settings {
    /// Service base URL, https.
    base: signer::SignerUrl,
    /// PEM bundle pinning the service's serving certificate.
    pin_pem: String,
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
    /// The Secrets enrollment reads, by the field naming each, so none is also written.
    read_secrets: Vec<(&'static str, String)>,
}

impl Settings {
    /// Validate the local site's signer and invite, with the bundles they name.
    fn new(
        site_name: &str,
        url: &str,
        invite: &InviteSecretRef,
        pin_pem: &str,
        anchor_pem: Option<&str>,
    ) -> Result<Self, EnrollError> {
        let base = signer::SignerUrl::parse(url).map_err(EnrollError::Config)?;
        certs::validate_site_name(site_name).map_err(|e| EnrollError::Config(format!("site name {site_name}: {e}")))?;
        pem_roots(pin_pem, "site.signerEndpoint.caBundleRef")?;
        let anchor_pem = match anchor_pem {
            Some(anchors) => {
                pem_roots(anchors, "site.signerEndpoint.gridCaBundleRef")?;
                anchors.to_owned()
            },
            None => pin_pem.to_owned(),
        };
        Ok(Self {
            base,
            pin_pem: pin_pem.to_owned(),
            anchor_pem,
            site_name: site_name.to_owned(),
            token_secret: required(Some(&invite.name), "site.inviteSecretRef.name")?,
            token_key: required(Some(&invite.key), "site.inviteSecretRef.key")?,
            // One budget up to the signer's answer; writes that keep the answer start their own.
            backoff: STEP_BACKOFF.started_now(),
            defaults: Target::defaults(),
            read_secrets: vec![("site.inviteSecretRef", invite.name.clone())],
        })
    }

    /// Also reading the bundles `pin` and `anchor` name.
    fn with_bundles(mut self, pin: Option<&BundleRef>, anchor: Option<&BundleRef>) -> Self {
        for (field, reference) in [
            ("site.signerEndpoint.caBundleRef", pin),
            ("site.signerEndpoint.gridCaBundleRef", anchor),
        ] {
            if let Some(reference) = reference {
                self.read_secrets.push((field, reference.name.clone()));
            }
        }
        self
    }

    /// The signer these settings name, reusing `clients`.
    fn signer<'signer>(&'signer self, clients: &'signer signer::Clients) -> signer::HttpsSigner<'signer> {
        signer::HttpsSigner::new(&self.base, Some(&self.pin_pem), clients)
    }
}

/// The signer's base URL: https, and outside the grid domain.
///
/// # Errors
///
/// Returns a message when the URL does not parse, is not https, carries credentials,
/// percent-encodes its authority, or names a host under the grid domain.
pub(crate) fn signer_url(raw: &str) -> Result<Url, String> {
    let url = Url::parse(raw.trim()).map_err(|e| format!("site.signerEndpoint.url: {e}"))?;
    // Plaintext would expose the token.
    if url.scheme() != "https" {
        return Err("site.signerEndpoint.url must be an https URL".to_owned());
    }
    // The operator appends its own paths, so a base URL carries neither.
    if url.query().is_some() || url.fragment().is_some() {
        return Err("site.signerEndpoint.url is a base URL and carries no query or fragment".to_owned());
    }
    // Credentials in the URL would be sent as Basic auth and published in the GridSite status.
    if !url.username().is_empty() || url.password().is_some() {
        return Err("site.signerEndpoint.url must not carry credentials".to_owned());
    }
    raw_authority(raw)?;
    // Any site leaf names a host under the grid domain, so one could stand in for the signer.
    let host = url
        .host_str()
        .map(|host| host.trim_end_matches('.').to_ascii_lowercase())
        .unwrap_or_default();
    let grid_domain = certs::SPIFFE_TRUST_DOMAIN;
    if host == grid_domain || host.ends_with(&format!(".{grid_domain}")) {
        return Err(format!(
            "site.signerEndpoint.url must not name a host under {grid_domain}, where site certificates live"
        ));
    }
    Ok(url)
}

/// Refuse what parsing forgives in the authority of `raw`: an empty host, a
/// percent-encoded one, or an empty or non-numeric port, each of which would slip past
/// the domain rule.
fn raw_authority(raw: &str) -> Result<(), String> {
    let authority = raw
        .trim()
        .split_once("://")
        .map_or("", |(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or_default());
    if authority.is_empty() {
        return Err("site.signerEndpoint.url must name a host".to_owned());
    }
    if authority.contains('%') {
        return Err("site.signerEndpoint.url must not percent-encode the host".to_owned());
    }
    let port = match authority.rsplit_once(']') {
        Some((_host, after)) if authority.starts_with('[') => after.strip_prefix(':'),
        Some(_) | None => authority.rsplit_once(':').map(|(_host, port)| port),
    };
    if port.is_some_and(|port| port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit())) {
        return Err("site.signerEndpoint.url port must be numeric".to_owned());
    }
    Ok(())
}

/// The certificates in `pem`, refusing a bundle with none.
fn pem_roots(pem: &str, name: &'static str) -> Result<Vec<reqwest::Certificate>, EnrollError> {
    grid_client::roots_from_pem(pem.as_bytes(), name).map_err(|e| EnrollError::Config(error_chain(&e)))
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

impl Target {
    /// [`IDENTITY_SECRET`] and [`CA_SECRET`].
    fn defaults() -> Self {
        Self {
            site_secret: IDENTITY_SECRET.to_owned(),
            ca_secret: CA_SECRET.to_owned(),
        }
    }
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
    with_backoff(backoff, "listing GridNetworks", EnrollError::Kube, || async move {
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
    /// Secret `name`'s metadata, if present, without reading its data.
    fn metadata(&self, name: &str) -> impl Future<Output = Result<Option<ObjectMeta>, EnrollError>> + Send;
    /// Secret `name`, if present.
    fn get(&self, name: &str) -> impl Future<Output = Result<Option<Secret>, EnrollError>> + Send;
    /// Create `secret`, reporting a conflict.
    fn create(&self, secret: &Secret) -> impl Future<Output = Result<Created, EnrollError>> + Send;
    /// Dry-run creating `secret`, refusing what the API server would refuse.
    fn check_create(&self, secret: &Secret) -> impl Future<Output = Result<Attempt<()>, EnrollError>> + Send;
    /// Merge `patch` into Secret `name`, refusing it when the resource version it names is stale.
    fn patch(
        &self,
        name: &str,
        patch: &serde_json::Value,
    ) -> impl Future<Output = Result<Attempt<()>, EnrollError>> + Send;
}

/// Kubernetes-backed [`Store`] in the operator namespace.
struct KubeStore(Api<Secret>);

impl Store for KubeStore {
    async fn exists(&self, name: &str) -> Result<bool, EnrollError> {
        self.metadata(name).await.map(|meta| meta.is_some())
    }

    async fn metadata(&self, name: &str) -> Result<Option<ObjectMeta>, EnrollError> {
        self.0
            .get_metadata_opt(name)
            .await
            .map(|meta| meta.map(|meta| meta.metadata))
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

    async fn patch(&self, name: &str, patch: &serde_json::Value) -> Result<Attempt<()>, EnrollError> {
        match self.0.patch(name, &PatchParams::default(), &Patch::Merge(patch)).await {
            Ok(_) => Ok(Attempt::Done(())),
            Err(kube::Error::Api(status)) if status.code == 409 => {
                Err(EnrollError::Kube(format!("Secret {name} changed under the write")))
            },
            Err(e) => transient(name, &e).map(Attempt::Retry),
        }
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

/// Refuse an invite Secret not labelled for `site`. Required, so a `GridSite` cannot
/// send an unrelated Secret as a token.
fn minted_for(secret: &Secret, name: &str, site: &str) -> Result<(), EnrollError> {
    match secret
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(SITE_LABEL))
    {
        Some(minted_for) if minted_for == site => Ok(()),
        Some(minted_for) => Err(EnrollError::NameMismatch(format!(
            "Secret {name} holds the token for site {minted_for}, not {site}"
        ))),
        None => Err(EnrollError::NameMismatch(format!(
            "Secret {name} lacks the label {SITE_LABEL}={site} an invite carries"
        ))),
    }
}

/// The site token, refused if minted for another site.
async fn site_token<S: Store + Sync>(store: &S, settings: &Settings) -> Result<Zeroizing<String>, EnrollError> {
    let name = &settings.token_secret;
    let secret = store
        .get(name)
        .await?
        .ok_or_else(|| EnrollError::RefNotFound(format!("invite Secret {name} not found")))?;
    minted_for(&secret, name, &settings.site_name)?;
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
    let empty = Leaf {
        cert: String::new(),
        ca: String::new(),
    };
    let placeholder = &prepare(target, empty, Zeroizing::default());
    with_backoff(
        backoff,
        "dry-running the identity Secret writes",
        EnrollError::Kube,
        || async move {
            if let Some((ca_secret, _)) = &placeholder.ca
                && !ca_present
                && let Attempt::Retry(reason) = store.check_create(ca_secret).await?
            {
                return Ok(Attempt::Retry(reason));
            }
            store.check_create(&placeholder.site).await
        },
    )
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

/// Refuse a layout where a Secret enrollment writes is also one it reads or clears: the
/// pending Secret named for the grid CA would be erased with the enrollment it kept, and
/// an invite or bundle Secret named for the identity would be written over.
fn check_names(settings: &Settings, target: &Target) -> Result<(), EnrollError> {
    let pending = pending::name(target);
    let written = [
        ("the identity Secret", target.site_secret.as_str()),
        ("the grid CA Secret", target.ca_secret.as_str()),
        ("the enrollment kept beside the identity", pending.as_str()),
    ];
    if pending == target.ca_secret {
        return Err(EnrollError::Config(format!(
            "Secret {pending} is both the grid CA Secret and the enrollment kept beside the identity: clearing \
             the enrollment would erase the grid CA; name the identity and CA Secrets apart"
        )));
    }
    for (writes, name) in written {
        for (reads, read) in &settings.read_secrets {
            if name == read {
                return Err(EnrollError::Config(format!(
                    "Secret {name} is both {writes} and {reads}: enrollment would write over what it reads; name \
                     them apart"
                )));
            }
        }
    }
    Ok(())
}

/// Enroll through `signer` unless already enrolled, then store the identity.
async fn enroll<S: Store + Sync, G: Signer + Sync>(
    store: &S,
    signer: &G,
    settings: &Settings,
    target: &Target,
) -> Result<Outcome, EnrollError> {
    check_names(settings, target)?;
    if store.exists(&target.site_secret).await? {
        drop(pending::clear(store, target, settings.backoff).await?);
        return Ok(Outcome::AlreadyEnrolled);
    }
    check_not_enrolled_elsewhere(store, &settings.defaults, target).await?;
    let ca_present = check_existing_ca(store, target, &settings.anchor_pem).await?;
    check_writable(store, target, ca_present, settings.backoff).await?;
    let token = site_token(store, settings).await?;
    let binding = pending::Binding::of(settings, &token);
    let mut held = Box::pin(in_flight(store, settings, target, &binding)).await?;
    let (issued, keep) = match held.issued.take() {
        Some(issued) => (issued, settings.backoff),
        None => Box::pin(sent(signer, (store, settings), &mut held, &token)).await?,
    };
    let verified = checked_identity(&issued, &held.csr, settings)?;
    let written = Box::pin(stored(
        store,
        (settings, keep),
        target,
        (verified, std::mem::take(&mut held.key)),
    ))
    .await?;
    Ok(match written {
        Written::Stored => Outcome::Enrolled(certs::spiffe_id(&settings.site_name)),
        Written::Discarded => Outcome::Discarded,
    })
}

/// Write `verified` and its key as the identity for `target`, and clear the enrollment kept
/// for it.
async fn stored<S: Store + Sync>(
    store: &S,
    (settings, backoff): (&Settings, Backoff),
    target: &Target,
    (verified, key): (Leaf, Zeroizing<String>),
) -> Result<Written, EnrollError> {
    if !certs::has_svid_profile(&verified.cert).unwrap_or(false) {
        tracing::warn!("issued certificate lacks the X.509-SVID profile, peers in spiffe trust mode will reject it");
    }
    let anchors = &settings.anchor_pem;
    let prepared = &prepare(target, verified, key);
    // A write the budget gave up on is the store's to retry: the issued answer stays kept
    // beside the identity, so the next poll stores it without asking the signer again. Only a
    // refusal makes the token spent for nothing.
    let written = with_backoff(backoff, "writing the identity Secrets", EnrollError::Kube, || {
        write(store, prepared, anchors)
    })
    .await
    .map_err(|e| {
        if matches!(e, EnrollError::Kube(_)) {
            e
        } else {
            EnrollError::NotStored(e.to_string())
        }
    })?;
    drop(pending::clear(store, target, backoff).await?);
    Ok(written)
}

/// The enrollment in flight for `target`, made for `binding`, else a new request kept before
/// it is sent.
async fn in_flight<S: Store + Sync>(
    store: &S,
    settings: &Settings,
    target: &Target,
    binding: &pending::Binding,
) -> Result<pending::Pending, EnrollError> {
    if let Some(held) = pending::read(store, target, binding, settings.backoff).await? {
        return Ok(held);
    }
    let certs::GeneratedCsr { csr_pem, key_pem } =
        certs::generate_csr(&settings.site_name).map_err(|e| EnrollError::Config(format!("generating a CSR: {e}")))?;
    pending::stash(store, target, (csr_pem, key_pem), (binding, settings.backoff)).await
}

/// Send `held`'s request with the invite and keep what comes back, under a budget started at
/// the answer and returned for the writes after it. A token refused after an earlier request
/// went unanswered may have been spent by that request.
async fn sent<S: Store + Sync, G: Signer + Sync>(
    signer: &G,
    (store, settings): (&S, &Settings),
    held: &mut pending::Pending,
    token: &str,
) -> Result<(Issued, Backoff), EnrollError> {
    let resent = held.unanswered;
    held.sending(store, settings.backoff).await?;
    match Box::pin(redeem(signer, settings.backoff, token, &held.csr)).await {
        Ok(issued) => {
            let keep = settings.backoff.started_now();
            held.issued(store, &issued, keep).await?;
            Ok((issued, keep))
        },
        Err(error @ EnrollError::MaybeSpent { .. }) => Err(error),
        Err(EnrollError::TokenRejected(detail)) if resent => Err(EnrollError::MaybeSpent {
            detail: format!(
                "{detail}; an earlier request with this key got no answer and may have spent the invite, so a \
                 new invite may be needed"
            ),
            refused: true,
        }),
        // A request the signer never answered stays unanswered: an answer may still be out
        // there, so a later refusal reads as one after a lost answer.
        Err(error) if resent && !answered_by_signer(&error) => Err(error),
        Err(error) => {
            held.answered(store, settings.backoff.started_now()).await?;
            Err(error)
        },
    }
}

/// Whether `error` is the signer's answer to the request, rather than a failure to reach it,
/// trust it, or build the request.
const fn answered_by_signer(error: &EnrollError) -> bool {
    matches!(
        error,
        EnrollError::TokenRejected(_)
            | EnrollError::NameTaken(_)
            | EnrollError::NameDeleted(_)
            | EnrollError::Refused(_)
            | EnrollError::InvalidResponse(_)
    )
}

/// Present the invite to `signer`, retrying only a request that never left.
async fn redeem<G: Signer + Sync>(
    signer: &G,
    backoff: Backoff,
    token: &str,
    csr_pem: &str,
) -> Result<Issued, EnrollError> {
    with_backoff(
        backoff,
        "reaching the enrollment service",
        EnrollError::Unreachable,
        || async move {
            match signer.enroll(token, csr_pem).await {
                Ok(issued) => Ok(Attempt::Done(issued)),
                Err(SignerError::Transport { sent: false, detail }) => Ok(Attempt::Retry(detail)),
                Err(error) => Err(enroll_error(error)),
            }
        },
    )
    .await
}

/// What a signer's refusal means for enrollment.
fn enroll_error(error: SignerError) -> EnrollError {
    match error {
        SignerError::Unauthenticated(detail) => EnrollError::TokenRejected(detail),
        SignerError::NameTaken(detail) => EnrollError::NameTaken(detail),
        SignerError::NameDeleted(detail) => EnrollError::NameDeleted(detail),
        SignerError::Forbidden(detail) | SignerError::Refused(detail) | SignerError::NoClientCertificate(detail) => {
            EnrollError::Refused(detail)
        },
        SignerError::Disabled(detail) | SignerError::Transport { detail, .. } => {
            EnrollError::MaybeSpent { detail, refused: false }
        },
        SignerError::Untrusted { lapsed: true, detail } => EnrollError::SignerLapsed(detail),
        SignerError::Untrusted { lapsed: false, detail } => EnrollError::SignerUntrusted(detail),
        SignerError::Request(detail) => EnrollError::Config(detail),
        SignerError::Invalid(detail) => EnrollError::InvalidResponse(format!("the enrollment: {detail}")),
    }
}

/// `error` and its sources, through the error an `io::Error` wraps.
fn chain<'error>(
    error: &'error (dyn std::error::Error + 'static),
) -> impl Iterator<Item = &'error (dyn std::error::Error + 'static)> {
    std::iter::successors(Some(error), |err| {
        // io::Error::source skips its wrapped error.
        err.downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            .map(|inner| -> &(dyn std::error::Error + 'static) { inner })
            .or_else(|| err.source())
    })
}

/// Whether `error`'s chain holds a TLS failure.
fn is_tls_failure(error: &(dyn std::error::Error + 'static)) -> bool {
    chain(error).any(tls_backend::is_tls_error)
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

/// Check the identity against the pin, site name, and key.
fn checked_identity(enrollment: &Issued, csr_pem: &str, settings: &Settings) -> Result<Leaf, EnrollError> {
    let site = &settings.site_name;
    signer::issued_leaf(enrollment, &settings.anchor_pem, site, csr_pem).map_err(|refused| {
        EnrollError::InvalidResponse(match refused {
            LeafError::Ca(certs::VerifyError::NotAnchored) => "returned CA is not the pinned grid CA".to_owned(),
            LeafError::Ca(e) => format!("returned CA: {e}"),
            LeafError::Certificate(e) => format!("issued certificate: {e}"),
            LeafError::NotForSite(e) => {
                format!("issued certificate is not valid for {site} ({e}), possible interception, contact the hub")
            },
            LeafError::Key(e) => e,
            LeafError::KeyMismatch => {
                "issued certificate does not carry this site's key, possible interception, contact the hub".to_owned()
            },
        })
    })
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
fn prepare(target: &Target, verified: Leaf, mut key: Zeroizing<String>) -> Prepared {
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

/// How often a running site checks whether it lost its identity.
pub(crate) const LOST_IDENTITY_CHECK: Duration = Duration::from_secs(30);

/// Whether `site_secret` is definitely absent and the invite Secret `invite`, minted for
/// `site`, is present, so a restart would enroll the site again.
///
/// A read that fails, times out or is refused decides nothing: only a definite not-found
/// counts as gone, and only an invite labelled for this site counts.
async fn identity_lost<S: Store + Sync>(store: &S, site_secret: &str, invite: &str, site: &str) -> bool {
    match store.exists(site_secret).await {
        Ok(false) => {},
        Ok(true) => return false,
        Err(error) => {
            tracing::debug!(%error, "site identity not readable; deciding nothing this check");
            return false;
        },
    }
    match store.get(invite).await {
        Ok(Some(invite)) => invited(&invite, site),
        Ok(None) => false,
        Err(error) => {
            tracing::debug!(%error, "invite not readable; deciding nothing this check");
            false
        },
    }
}

/// Whether `invite` was minted for `site`.
fn invited(invite: &Secret, site: &str) -> bool {
    invite
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(SITE_LABEL))
        .is_some_and(|minted_for| minted_for == site)
}

pub mod local;
mod pending;

/// Whether `secrets` holds enrollment's own request in flight for the identity Secret
/// `site_secret`: a site was configured here, whatever the operator config says now.
///
/// # Errors
///
/// [`kube::Error`] when the read fails.
pub(crate) async fn in_flight_for(secrets: &Api<Secret>, site_secret: &str) -> Result<bool, kube::Error> {
    pending::held_for(secrets, site_secret).await
}
pub mod renew;
mod signer;

use signer::{Issued, Leaf, LeafError, Signer, SignerError};

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests;
