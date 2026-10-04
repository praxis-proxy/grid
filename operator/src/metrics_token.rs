//! Short-lived bearer tokens for the grid metrics scraper `ServiceAccount`.
//!
//! An EPP authorizes a metrics scrape with `TokenReview`, so whoever runs the EPP
//! receives the token, valid against the API server. The scraper therefore never
//! sends the operator's own token: it mints one for a `ServiceAccount` allowed only
//! `get` on `/metrics`, through the `TokenRequest` API, and keeps it briefly.

use std::{
    sync::{LazyLock, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use k8s_openapi::api::{
    authentication::v1::{BoundObjectReference, TokenRequest, TokenRequestSpec},
    core::v1::ServiceAccount,
};
use kube::api::{Api, PostParams};
use zeroize::Zeroizing;

use crate::metrics_scraper::MetricsScrapeError;

/// Lifetime requested for each token, the `TokenRequest` minimum.
const TOKEN_LIFETIME: Duration = Duration::from_secs(600);

/// Longest a `TokenRequest` may take before the scrape gives up on it.
const MINT_TIMEOUT: Duration = Duration::from_secs(10);

/// Wait after a failed mint before trying again, so waiting scrapes fail fast meanwhile.
const RETRY_BACKOFF: Duration = Duration::from_secs(30);

/// The scraper `ServiceAccount`, from the chart.
const SERVICE_ACCOUNT_ENV: &str = "GRID_METRICS_SCRAPER_SERVICE_ACCOUNT";
/// The namespace of [`SERVICE_ACCOUNT_ENV`].
const NAMESPACE_ENV: &str = "GRID_METRICS_SCRAPER_NAMESPACE";
/// This pod's name and uid, from the downward API, to bind each token to the pod.
const POD_NAME_ENV: &str = "GRID_POD_NAME";
/// See [`POD_NAME_ENV`].
const POD_UID_ENV: &str = "GRID_POD_UID";

/// A minted token and its timeline.
struct Cached {
    /// The token, wiped on drop.
    token: Zeroizing<String>,
    /// Two thirds into its lifetime, so a scrape never sends one about to expire.
    refresh_at: Instant,
    /// When the API server stops accepting it.
    expires_at: Instant,
    /// After a failed refresh, the earliest moment to try again.
    retry_at: Option<Instant>,
}

/// The held token, and when the last mint with nothing to fall back on failed.
#[derive(Default)]
struct State {
    /// The current token, if any.
    cached: Option<Cached>,
    /// After a failed mint with no usable token, the earliest moment to try again.
    no_mint_before: Option<Instant>,
}

impl State {
    /// Keep a fresh token, or after a failure keep serving a valid one, else back off.
    fn record(
        &mut self,
        minted: Result<(Zeroizing<String>, Duration), MetricsScrapeError>,
        now: Instant,
    ) -> Result<Zeroizing<String>, MetricsScrapeError> {
        let error = match minted {
            Ok((token, lifetime)) => {
                *self = Self {
                    cached: Some(Cached {
                        token: token.clone(),
                        refresh_at: now + lifetime.saturating_mul(2) / 3,
                        expires_at: now + lifetime,
                        retry_at: None,
                    }),
                    no_mint_before: None,
                };
                return Ok(token);
            },
            Err(error) => error,
        };
        if let Some(cached) = self.cached.as_mut().filter(|cached| now < cached.expires_at) {
            tracing::warn!(%error, "metrics scraper token refresh failed; using the held token until it expires");
            cached.retry_at = Some(now + RETRY_BACKOFF);
            return Ok(cached.token.clone());
        }
        self.no_mint_before = Some(now + RETRY_BACKOFF);
        Err(error)
    }
}

/// What a scrape does with the cached token at `now`.
#[derive(Debug, PartialEq, Eq)]
enum Plan {
    /// Send the cached token.
    Use,
    /// Mint a new one.
    Mint,
    /// Fail fast: a mint just failed and no valid token is held.
    Fail,
}

/// Whether `state` still serves at `now`, a new token is due, or a recent failure stands.
fn token_plan(state: &State, now: Instant) -> Plan {
    match state.cached.as_ref() {
        Some(cached) if now < cached.refresh_at => Plan::Use,
        Some(cached) if now < cached.expires_at && cached.retry_at.is_some_and(|at| now < at) => Plan::Use,
        Some(cached) if now < cached.expires_at => Plan::Mint,
        _ if state.no_mint_before.is_some_and(|at| now < at) => Plan::Fail,
        _ => Plan::Mint,
    }
}

/// The current token, shared by every scrape.
static CACHE: LazyLock<tokio::sync::Mutex<State>> = LazyLock::new(|| tokio::sync::Mutex::new(State::default()));

/// This pod, when the downward API names it, as the object each token is bound to.
fn bound_pod() -> Option<BoundObjectReference> {
    let name = std::env::var(POD_NAME_ENV).ok().filter(|v| !v.trim().is_empty())?;
    let uid = std::env::var(POD_UID_ENV).ok().filter(|v| !v.trim().is_empty())?;
    Some(BoundObjectReference {
        api_version: Some("v1".to_owned()),
        kind: Some("Pod".to_owned()),
        name: Some(name),
        uid: Some(uid),
    })
}

/// The configured scraper `ServiceAccount`, read once.
fn scraper_account() -> Option<&'static (String, String)> {
    static ACCOUNT: OnceLock<Option<(String, String)>> = OnceLock::new();
    ACCOUNT
        .get_or_init(|| {
            let name = std::env::var(SERVICE_ACCOUNT_ENV)
                .ok()
                .filter(|v| !v.trim().is_empty())?;
            let namespace = std::env::var(NAMESPACE_ENV).ok().filter(|v| !v.trim().is_empty())?;
            Some((name, namespace))
        })
        .as_ref()
}

/// A fresh token for `namespace/name` and how long it is valid.
async fn mint(
    client: &kube::Client,
    name: &str,
    namespace: &str,
) -> Result<(Zeroizing<String>, Duration), MetricsScrapeError> {
    let request = TokenRequest {
        spec: TokenRequestSpec {
            // The API server's own audience: an EPP's `TokenReview` checks no other.
            audiences: Vec::new(),
            expiration_seconds: i64::try_from(TOKEN_LIFETIME.as_secs()).ok(),
            bound_object_ref: bound_pod(),
        },
        ..TokenRequest::default()
    };
    let api = Api::<ServiceAccount>::namespaced(client.clone(), namespace);
    let issued = tokio::time::timeout(
        MINT_TIMEOUT,
        api.create_token_request(name, &PostParams::default(), &request),
    )
    .await
    .map_err(|_elapsed| MetricsScrapeError::Credential(format!("TokenRequest for {namespace}/{name} timed out")))?
    .map_err(|e| MetricsScrapeError::Credential(format!("TokenRequest for {namespace}/{name}: {e}")))?;
    let status = issued
        .status
        .filter(|status| !status.token.is_empty())
        .ok_or_else(|| MetricsScrapeError::Credential("TokenRequest returned no token".to_owned()))?;
    let lifetime = remaining(status.expiration_timestamp.0.as_second(), SystemTime::now());
    Ok((Zeroizing::new(status.token), lifetime))
}

/// Time left until `expires_unix` seconds, capped at what was requested.
///
/// The API server may grant less than asked, so its stated expiry wins.
fn remaining(expires_unix: i64, now: SystemTime) -> Duration {
    let now_unix = now.duration_since(UNIX_EPOCH).map_or(0, |since| since.as_secs());
    let left = u64::try_from(expires_unix).unwrap_or(0).saturating_sub(now_unix);
    Duration::from_secs(left).min(TOKEN_LIFETIME)
}

/// A valid token for the scraper `ServiceAccount`, minted when the cached one is due.
///
/// A failed refresh keeps serving the held token until it expires, retrying
/// after [`RETRY_BACKOFF`], so an API server blip does not stop every scrape.
///
/// # Errors
///
/// Returns [`MetricsScrapeError::Credential`] when no scraper `ServiceAccount` is
/// configured, or no valid token is held and minting fails. The error never
/// carries a token.
pub(crate) async fn scraper_token(client: &kube::Client) -> Result<Zeroizing<String>, MetricsScrapeError> {
    let (name, namespace) = scraper_account().ok_or_else(|| {
        MetricsScrapeError::Credential(format!("no metrics scraper ServiceAccount: set {SERVICE_ACCOUNT_ENV}"))
    })?;
    // Held while minting, so concurrent scrapes share one `TokenRequest`.
    let mut state = CACHE.lock().await;
    let now = Instant::now();
    match (token_plan(&state, now), state.cached.as_ref()) {
        (Plan::Use, Some(cached)) => return Ok(cached.token.clone()),
        (Plan::Fail, _) => {
            return Err(MetricsScrapeError::Credential(
                "metrics scraper token unavailable; retrying shortly".to_owned(),
            ));
        },
        _ => {},
    }
    let minted = mint(client, name, namespace).await;
    state.record(minted, now)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cached(now: Instant, refresh_in: u64, expires_in: u64, retry_in: Option<u64>) -> State {
        State {
            cached: Some(Cached {
                token: Zeroizing::new("t".to_owned()),
                refresh_at: now + Duration::from_secs(refresh_in),
                expires_at: now + Duration::from_secs(expires_in),
                retry_at: retry_in.map(|secs| now + Duration::from_secs(secs)),
            }),
            no_mint_before: None,
        }
    }

    #[test]
    fn a_held_token_serves_until_refresh_then_mints() {
        let now = Instant::now();
        assert_eq!(token_plan(&State::default(), now), Plan::Mint, "nothing held");
        assert_eq!(token_plan(&cached(now, 10, 30, None), now), Plan::Use, "before refresh");
        let due = now + Duration::from_secs(10);
        assert_eq!(token_plan(&cached(now, 10, 30, None), due), Plan::Mint, "refresh due");
    }

    #[test]
    fn after_a_failed_refresh_the_held_token_serves_until_the_retry() {
        let now = Instant::now();
        let held = cached(now, 0, 600, Some(30));
        assert_eq!(token_plan(&held, now), Plan::Use, "backing off");
        assert_eq!(token_plan(&held, now + Duration::from_secs(30)), Plan::Mint, "retry");
        let expired = cached(now, 0, 5, Some(30));
        assert_eq!(
            token_plan(&expired, now + Duration::from_secs(6)),
            Plan::Mint,
            "an expired token is never sent"
        );
    }

    #[test]
    fn a_failed_mint_with_nothing_held_fails_fast_until_the_backoff_ends() {
        let now = Instant::now();
        let failed = State {
            cached: None,
            no_mint_before: Some(now + Duration::from_secs(30)),
        };
        assert_eq!(token_plan(&failed, now), Plan::Fail, "backing off");
        assert_eq!(
            token_plan(&failed, now + Duration::from_secs(30)),
            Plan::Mint,
            "retry once the backoff ends"
        );
    }

    #[test]
    fn the_api_servers_expiry_wins_over_the_requested_lifetime() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
        assert_eq!(
            remaining(1_000_300, now),
            Duration::from_secs(300),
            "granted less than asked"
        );
        assert_eq!(remaining(1_100_000, now), TOKEN_LIFETIME, "never more than asked");
        assert_eq!(remaining(999_000, now), Duration::ZERO, "already expired");
    }
}
