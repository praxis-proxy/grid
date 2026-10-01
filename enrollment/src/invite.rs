//! `enrollment invite`: mint site tokens into Secrets.

use std::{collections::BTreeMap, error::Error, path::PathBuf, time::Duration};

use clap::Parser;
use enrollment::generated::{EnrollmentToken, EnrollmentTokenRequest, Error as ErrorBody};
use k8s_openapi::api::core::v1::Secret;
use kube::api::{Api, ObjectMeta, PostParams};
use reqwest::{StatusCode, Url};
use zeroize::{Zeroize, Zeroizing};

/// A `Send + Sync` boxed error.
type BoxError = Box<dyn Error + Send + Sync>;

/// The mint route on the enrollment service.
const TOKENS_PATH: &str = "/v1alpha1/enrollmenttokens";

/// `app.kubernetes.io/managed-by` on invite Secrets.
const MANAGED_BY: &str = "grid-enrollment-invite";

/// Invite Secret annotation holding the token id.
const TOKEN_ID_ANNOTATION: &str = "grid.praxis-proxy.io/token-id";

/// Connect retry schedule for one run.
const BACKOFF: Backoff = Backoff {
    attempts: 12,
    initial: Duration::from_secs(2),
    max: Duration::from_secs(30),
};

/// Response size cap.
const MAX_RESPONSE_BYTES: usize = 16 * 1024;

/// Arguments for `invite`.
#[derive(Parser)]
#[command(name = "invite", about = "Mint site tokens and store them as invite Secrets")]
struct InviteArgs {
    /// Namespace for invite Secrets, else `POD_NAMESPACE` or `default`.
    #[arg(long)]
    namespace: Option<String>,
    /// Enrollment service base URL (https).
    #[arg(long)]
    url: String,
    /// PEM bundle that pins the enrollment server.
    #[arg(long)]
    ca_file: PathBuf,
    /// File holding a grid-admin bearer token.
    #[arg(long)]
    admin_token_file: PathBuf,
    /// JSON list of `{siteName, gridNetworkRef, expiresInSecs}`.
    #[arg(long, env = "ENROLLMENT_INVITES")]
    invites: String,
    /// Invite Secret name prefix.
    #[arg(long, default_value = "grid-invite-")]
    secret_prefix: String,
}

/// Run the `invite` subcommand.
pub(crate) async fn run() -> Result<(), BoxError> {
    let args = InviteArgs::parse_from(std::env::args_os().skip(1));
    let invites = parse_invites(&args.invites).map_err(|err| format!("ENROLLMENT_INVITES: {err}"))?;
    #[cfg(not(feature = "fips"))]
    if rustls::crypto::ring::default_provider().install_default().is_err() {
        tracing::debug!("a rustls crypto provider was already installed");
    }
    let ca_file = &args.ca_file;
    let ca_pem = std::fs::read(ca_file).map_err(|err| format!("reading {}: {err}", ca_file.display()))?;
    let mut minter = Minter {
        http: http_client(&ca_pem).map_err(|err| format!("{}: {err}", ca_file.display()))?,
        base: https_base(&args.url).map_err(|err| format!("--url: {err}"))?,
        admin_token_file: args.admin_token_file,
        retry: Retry::new(BACKOFF),
    };
    let namespace = args
        .namespace
        .or_else(|| std::env::var("POD_NAMESPACE").ok())
        .unwrap_or_else(|| "default".to_owned());
    let secrets = KubeInvites(Api::namespaced(kube::Client::try_default().await?, &namespace));
    invite_all(&secrets, &mut minter, &args.secret_prefix, &invites).await
}

/// Invite every site, then fail naming each site that failed.
async fn invite_all<S: InviteSecrets + Sync>(
    secrets: &S,
    minter: &mut Minter,
    prefix: &str,
    invites: &[EnrollmentTokenRequest],
) -> Result<(), BoxError> {
    let mut failed = Vec::new();
    for invite in invites {
        if let Err(err) = Box::pin(invite_one(secrets, minter, prefix, invite)).await {
            tracing::error!(site = %invite.site_name, error = %err, "invite failed");
            failed.push(format!("site {}: {err}", invite.site_name));
        }
    }
    if failed.is_empty() {
        return Ok(());
    }
    Err(format!(
        "{} of {} invites failed: {}",
        failed.len(),
        invites.len(),
        failed.join("; ")
    )
    .into())
}

/// Parse `raw` as the https base URL, without a trailing slash.
fn https_base(raw: &str) -> Result<String, BoxError> {
    let url = Url::parse(raw)?;
    if url.scheme() != "https" {
        return Err("must be an https URL".into());
    }
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

/// Parse and validate the invite list.
fn parse_invites(json: &str) -> Result<Vec<EnrollmentTokenRequest>, BoxError> {
    let invites: Vec<EnrollmentTokenRequest> = serde_json::from_str(json)?;
    for invite in &invites {
        certs::validate_site_name(&invite.site_name).map_err(|err| format!("invite {:?}: {err}", invite.site_name))?;
    }
    Ok(invites)
}

/// A client that trusts only the pinned bundle.
fn http_client(ca_pem: &[u8]) -> Result<reqwest::Client, BoxError> {
    let roots = reqwest::Certificate::from_pem_bundle(ca_pem)?;
    if roots.is_empty() {
        return Err("enrollment CA bundle holds no certificate".into());
    }
    Ok(reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .tls_certs_only(roots)
        .build()?)
}

/// Capped exponential retry schedule.
#[derive(Clone, Copy, Debug)]
struct Backoff {
    /// Connect failures a run may take, including the last.
    attempts: u32,
    /// Delay after the first failure.
    initial: Duration,
    /// Delay cap.
    max: Duration,
}

/// Connect retry state shared by every call in a run.
#[derive(Debug)]
struct Retry {
    /// Schedule the delays follow.
    schedule: Backoff,
    /// Delay before the next retry.
    delay: Duration,
    /// Connect failures left in the run.
    left: u32,
}

impl Retry {
    /// A full budget at the initial delay.
    const fn new(schedule: Backoff) -> Self {
        Self {
            schedule,
            delay: schedule.initial,
            left: schedule.attempts,
        }
    }

    /// Note a call that reached the service.
    const fn reached(&mut self) {
        self.delay = self.schedule.initial;
    }

    /// Spend a connect failure: wait and return `true`, or `false` once the budget is gone.
    async fn failed(&mut self) -> bool {
        self.left = self.left.saturating_sub(1);
        if self.left == 0 {
            return false;
        }
        tokio::time::sleep(self.delay).await;
        self.delay = self.delay.saturating_mul(2).min(self.schedule.max);
        true
    }
}

/// Invite Secret reads and writes.
trait InviteSecrets {
    /// Token id on Secret `name`, empty if unannotated, `None` if absent.
    fn token_id(&self, name: &str) -> impl Future<Output = Result<Option<String>, BoxError>> + Send;

    /// Create Secret `name` holding `token`.
    fn create(
        &self,
        name: &str,
        invite: &EnrollmentTokenRequest,
        token: &MintedToken,
    ) -> impl Future<Output = Result<(), BoxError>> + Send;
}

/// How one invite ended.
#[derive(Debug, PartialEq, Eq)]
enum Invited {
    /// A token was minted and stored.
    Minted,
    /// The Secret already existed, so nothing was minted.
    Skipped,
    /// Another writer stored the Secret first, so the new token was revoked.
    Raced,
}

/// A minted token.
struct MintedToken {
    /// Identifier, for revoking.
    token_id: uuid::Uuid,
    /// The one-time token.
    token: Zeroizing<String>,
    /// When it stops being usable.
    expires_at: String,
}

/// Mint and store a token for `invite` unless its Secret exists.
async fn invite_one<S: InviteSecrets + Sync>(
    secrets: &S,
    minter: &mut Minter,
    prefix: &str,
    invite: &EnrollmentTokenRequest,
) -> Result<Invited, BoxError> {
    let site = &invite.site_name;
    let name = format!("{prefix}{site}");
    let held = secrets
        .token_id(&name)
        .await
        .map_err(|err| format!("reading Secret {name}: {err}"))?;
    if held.is_some() {
        tracing::info!(secret = %name, %site, "invite Secret exists, skipped");
        return Ok(Invited::Skipped);
    }
    let token = minter.mint(invite).await?;
    let Err(create_err) = secrets.create(&name, invite, &token).await else {
        tracing::info!(secret = %name, %site, expires_at = %token.expires_at, "site invited");
        return Ok(Invited::Minted);
    };
    settle_failed_create(secrets, minter, &name, token.token_id, &*create_err).await
}

/// Keep `id` if Secret `name` holds it after a failed create, else revoke it.
async fn settle_failed_create<S: InviteSecrets + Sync>(
    secrets: &S,
    minter: &mut Minter,
    name: &str,
    id: uuid::Uuid,
    create_err: &(dyn Error + Send + Sync),
) -> Result<Invited, BoxError> {
    match secrets.token_id(name).await {
        Ok(Some(stored)) if stored == id.to_string() => {
            tracing::warn!(secret = %name, error = %create_err, "create failed but the Secret holds the token");
            Ok(Invited::Minted)
        },
        Ok(Some(_)) => {
            minter
                .revoke(id)
                .await
                .map_err(|err| format!("Secret {name} holds another token, {err}"))?;
            tracing::info!(secret = %name, "another writer stored the invite Secret first, revoked the new token");
            Ok(Invited::Raced)
        },
        Ok(None) => {
            let revoked = minter.revoke(id).await.err().map(|why| format!(", {why}"));
            Err(format!("storing Secret {name}: {create_err}{}", revoked.unwrap_or_default()).into())
        },
        Err(read_err) => Err(format!(
            "storing Secret {name}: {create_err}, reading it back: {read_err}, token {id} may be live, revoke it if unheld"
        )
        .into()),
    }
}

/// Calls the enrollment service as a grid-admin.
struct Minter {
    /// Pinned client.
    http: reqwest::Client,
    /// https base URL without a trailing slash.
    base: String,
    /// Grid-admin bearer token file, re-read per request.
    admin_token_file: PathBuf,
    /// Connect retry state for the run.
    retry: Retry,
}

impl Minter {
    /// The current grid-admin bearer token.
    fn admin(&self) -> Result<Zeroizing<String>, BoxError> {
        let path = self.admin_token_file.display();
        let raw =
            Zeroizing::new(std::fs::read(&self.admin_token_file).map_err(|err| format!("reading {path}: {err}"))?);
        let token = std::str::from_utf8(&raw).map_err(|_e| format!("{path}: grid-admin token is not UTF-8"))?;
        Ok(Zeroizing::new(token.trim().to_owned()))
    }

    /// Mint a token for `invite`.
    async fn mint(&mut self, invite: &EnrollmentTokenRequest) -> Result<MintedToken, BoxError> {
        let url = format!("{}{TOKENS_PATH}", self.base);
        let response = self
            .send(|http, admin| http.post(&url).bearer_auth(admin).json(invite))
            .await
            .map_err(|err| format!("minting at {url} failed: {err}"))?;
        decode_minted(response).await
    }

    /// Revoke `token_id`.
    async fn revoke(&mut self, token_id: uuid::Uuid) -> Result<(), BoxError> {
        let url = format!("{}{TOKENS_PATH}/{token_id}", self.base);
        let failed = |why: String| format!("revoking unstored token {token_id} failed: {why}");
        let response = self
            .send(|http, admin| http.delete(&url).bearer_auth(admin))
            .await
            .map_err(|err| failed(err.to_string()))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(failed(response.status().to_string()).into())
        }
    }

    /// Send a request built per attempt, retrying connect failures from the run's budget.
    async fn send(
        &mut self,
        request: impl Fn(&reqwest::Client, &str) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, BoxError> {
        loop {
            if self.retry.left == 0 {
                return Err("not sent, the enrollment service stayed unreachable".into());
            }
            let admin = self.admin()?;
            match request(&self.http, admin.as_str()).send().await {
                Ok(response) => {
                    self.retry.reached();
                    return Ok(response);
                },
                // Only an unsent request is safe to resend.
                Err(error) if error.is_connect() && !is_tls_failure(&error) => {
                    tracing::warn!(left = self.retry.left, error = %error_chain(&error), "enrollment service unreachable");
                    if !self.retry.failed().await {
                        return Err(error_chain(&error).into());
                    }
                },
                Err(error) => return Err(error_chain(&error).into()),
            }
        }
    }
}

/// Decode a mint response.
async fn decode_minted(response: reqwest::Response) -> Result<MintedToken, BoxError> {
    let status = response.status();
    let body = read_capped(response).await?;
    if status.is_success() {
        let mut token: EnrollmentToken = serde_json::from_slice(body.as_slice())?;
        return Ok(MintedToken {
            token_id: token.token_id,
            token: Zeroizing::new(std::mem::take(&mut token.token)),
            expires_at: token.expires_at,
        });
    }
    let detail = serde_json::from_slice::<ErrorBody>(body.as_slice()).map_or_else(
        |_e| status.to_string(),
        |err| format!("{status} {}: {}", err.error, err.message),
    );
    let hint = match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            ": the invite ServiceAccount needs the grid-admin Role and an audience-bound token"
        },
        _ if status.is_server_error() => ": a token may have been minted, revoke unheld ones by id",
        _ => "",
    };
    Err(format!("minting a site token refused: {detail}{hint}").into())
}

/// Read at most [`MAX_RESPONSE_BYTES`] into wiped memory.
async fn read_capped(mut response: reqwest::Response) -> Result<Zeroizing<Vec<u8>>, BoxError> {
    let mut body = Zeroizing::new(Vec::new());
    while let Some(chunk) = response.chunk().await.map_err(|err| error_chain(&err))? {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err("mint response exceeds the size limit".into());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// An error with its source chain.
fn error_chain(error: &dyn Error) -> String {
    let mut out = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

/// Whether `error`'s chain holds a TLS failure.
fn is_tls_failure(error: &(dyn Error + 'static)) -> bool {
    std::iter::successors(Some(error), |&err| {
        // io::Error::source skips its wrapped error.
        err.downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            .map(|inner| -> &(dyn Error + 'static) { inner })
            .or_else(move || err.source())
    })
    .any(is_tls_error)
}

/// Whether `error` is a rustls error.
#[cfg(not(feature = "fips"))]
fn is_tls_error(error: &(dyn Error + 'static)) -> bool {
    error.is::<rustls::Error>()
}

/// Whether `error` is an openssl error.
#[cfg(feature = "fips")]
fn is_tls_error(error: &(dyn Error + 'static)) -> bool {
    error.is::<openssl::ssl::Error>() || error.is::<openssl::error::ErrorStack>()
}

/// Kubernetes-backed [`InviteSecrets`].
struct KubeInvites(Api<Secret>);

impl InviteSecrets for KubeInvites {
    async fn token_id(&self, name: &str) -> Result<Option<String>, BoxError> {
        let meta = self.0.get_metadata_opt(name).await?;
        Ok(meta.map(|meta| {
            meta.metadata
                .annotations
                .and_then(|mut annotations| annotations.remove(TOKEN_ID_ANNOTATION))
                .unwrap_or_default()
        }))
    }

    async fn create(&self, name: &str, invite: &EnrollmentTokenRequest, token: &MintedToken) -> Result<(), BoxError> {
        let secret = Box::new(WipedSecret(invite_secret(name, invite, token)));
        self.0.create(&PostParams::default(), &secret.0).await?;
        Ok(())
    }
}

/// A Secret whose `stringData` values are wiped on drop.
struct WipedSecret(Secret);

impl Drop for WipedSecret {
    fn drop(&mut self) {
        self.0
            .string_data
            .iter_mut()
            .flat_map(BTreeMap::values_mut)
            .for_each(Zeroize::zeroize);
    }
}

/// The invite Secret for a minted token.
fn invite_secret(name: &str, invite: &EnrollmentTokenRequest, minted: &MintedToken) -> Secret {
    let pairs = |items: &[(&str, &str)]| {
        items
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    };
    Secret {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            labels: Some(pairs(&[
                ("app.kubernetes.io/managed-by", MANAGED_BY),
                ("grid.praxis-proxy.io/site", &invite.site_name),
            ])),
            annotations: Some(pairs(&[
                ("grid.praxis-proxy.io/grid-network", &invite.grid_network_ref),
                (TOKEN_ID_ANNOTATION, &minted.token_id.to_string()),
                ("grid.praxis-proxy.io/expires-at", &minted.expires_at),
            ])),
            ..ObjectMeta::default()
        },
        type_: Some("Opaque".to_owned()),
        string_data: Some(pairs(&[("token", &minted.token)])),
        ..Secret::default()
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests;
