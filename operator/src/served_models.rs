//! Served models discovered from this site's providers, held in memory.
//!
//! A discovery round runs on its own cadence, like the signals scraper:
//!
//! ```text
//!   every interval
//!     list InferenceProviders with spec.modelDiscovery
//!     poll each source (bounded concurrency)
//!     ServedModelStore::refresh
//!       success               → replace set, renew deadline
//!       failure               → keep set, deadline unchanged
//!       provider not polled   → drop set
//!       deadline passed       → drop set
//! ```
//!
//! Freshness is absence: a set no poll renews expires after `ttl`, so a stale
//! set fails closed and no reader compares one clock against another.

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use futures::{StreamExt as _, stream};
use kube::{
    Client,
    api::{Api, ListParams},
};

use crate::{
    crd::inference_provider::{InferenceProvider, ModelDiscoveryConfig, OpenAiModelsSource},
    error::OperatorError,
    metrics,
    resources::{
        credentials::{self, CredentialPlan, CredentialResolver as _, KubernetesSecretResolver},
        endpoint_tls,
        model_discovery::{DiscoveryError, ModelSource as _, OpenAiModels, ServedModels},
    },
};

// ---------------------------------------------------------------------------
// DiscoveryConfig
// ---------------------------------------------------------------------------

/// Cadence and bounds of discovery rounds.
#[derive(Clone, Copy, Debug)]
pub struct DiscoveryConfig {
    /// Time between rounds.
    pub interval: Duration,

    /// Bound on one provider poll, including reading the response.
    pub timeout: Duration,

    /// How long a discovered set is held without a successful poll.
    pub ttl: Duration,

    /// Maximum providers polled at once.
    pub concurrency: usize,
}

impl Default for DiscoveryConfig {
    /// 60s interval; a set survives two missed rounds (180s).
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            timeout: Duration::from_secs(5),
            ttl: Duration::from_secs(180),
            concurrency: 8,
        }
    }
}

// ---------------------------------------------------------------------------
// ServedModelStore
// ---------------------------------------------------------------------------

/// A provider's served-model set and when it stops being held.
#[derive(Debug)]
struct ServedModel {
    /// Validated model names.
    models: Arc<[String]>,

    /// Deadline after which the set is no longer served.
    expires_at: Instant,
}

/// The result of polling one named provider.
struct ProviderPoll {
    /// `InferenceProvider` name.
    name: String,

    /// Models reported, or a failed poll.
    outcome: PollOutcome,
}

/// What one provider poll reported.
enum PollOutcome {
    /// The validated served-model set, including an empty set.
    Served(ServedModels),

    /// Polling failed; the last good set may remain held.
    Failed,
}

/// Served-model sets keyed by `InferenceProvider` name.
///
/// Cheap to clone; clones share the same sets.
#[derive(Clone, Debug, Default)]
pub struct ServedModelStore {
    /// Provider name to its held set.
    inner: Arc<RwLock<BTreeMap<String, ServedModel>>>,
}

impl ServedModelStore {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Models `provider` serves, or `None` when unknown or expired.
    #[must_use]
    pub fn models(&self, provider: &str) -> Option<Arc<[String]>> {
        let now = Instant::now();
        let guard = self.inner.read().ok()?;
        guard
            .get(provider)
            .filter(|held| held.expires_at > now)
            .map(|held| Arc::clone(&held.models))
    }

    /// Apply one round of poll results.
    ///
    /// `updated` holds every provider polled this round: a served set replaces
    /// the old one, while a failed poll keeps it until expiry. Providers absent
    /// from `updated` are no longer configured and are dropped.
    ///
    /// Refreshes, expirations, and drops are logged at `debug`, so e2e
    /// tests can observe them without a status field.
    fn refresh(&self, updated: Vec<ProviderPoll>, now: Instant, ttl: Duration) {
        let Ok(mut map) = self.inner.write() else {
            return;
        };

        // Remove dropped from map.
        map.retain(|provider: &String, served| {
            let dropped_from_round = updated.iter().all(|u| u.name.as_str() != provider);
            if dropped_from_round {
                tracing::debug!(provider, "served models dropped");
                false
            } else if served.expires_at <= now {
                tracing::debug!(provider, "served models expired");
                false
            } else {
                true
            }
        });

        for provider in updated {
            if let PollOutcome::Served(models) = provider.outcome {
                let name = provider.name;
                let models: Arc<[String]> = models.into_names().into();
                tracing::debug!(name, ?models, "served models refreshed");

                map.insert(
                    name,
                    ServedModel {
                        models,
                        expires_at: now + ttl,
                    },
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Discovery round
// ---------------------------------------------------------------------------

/// Poll every provider that opts into discovery and refresh `store`.
///
/// # Errors
///
/// Returns [`OperatorError`] when providers cannot be listed; `store` is
/// left untouched, so held sets expire on their own.
pub(crate) async fn discover(
    store: &ServedModelStore,
    client: &Client,
    config: &DiscoveryConfig,
) -> Result<(), OperatorError> {
    let api: Api<InferenceProvider> = Api::all(client.clone());
    let providers = api.list(&ListParams::default()).await?.items;

    let discoverable = providers.iter().filter_map(|provider| {
        let source = provider.spec.model_discovery.as_ref()?;
        Some((provider, source))
    });
    let polled = stream::iter(discoverable)
        .map(|(provider, source)| async move { poll(provider, source, client, config.timeout).await })
        .buffer_unordered(config.concurrency.max(1))
        .filter_map(|polled| async move { polled })
        .collect::<Vec<_>>()
        .await;

    store.refresh(polled, Instant::now(), config.ttl);
    Ok(())
}

// ---------------------------------------------------------------------------
// Poll
// ---------------------------------------------------------------------------

/// Why one provider poll failed.
#[derive(Debug, thiserror::Error)]
enum PollError {
    /// The bearer token could not be read.
    #[error("credential unavailable: {0}")]
    Credential(String),

    /// TLS material could not be read or is invalid.
    #[error("TLS unavailable: {0}")]
    Tls(String),

    /// The source failed.
    #[error(transparent)]
    Source(#[from] DiscoveryError),
}

impl PollError {
    /// Bounded reason string for metrics labels.
    fn as_reason(&self) -> &'static str {
        match self {
            Self::Credential(_) => "credential",
            Self::Tls(_) => "tls",
            Self::Source(DiscoveryError::Config(_)) => "config",
            Self::Source(DiscoveryError::Transport(_)) => "unreachable",
            Self::Source(DiscoveryError::Timeout(_)) => "timeout",
            Self::Source(DiscoveryError::Status(_)) => "http_status",
            Self::Source(
                DiscoveryError::BodyTooLarge(_) | DiscoveryError::Malformed(_) | DiscoveryError::InvalidModels(_),
            ) => "invalid_response",
        }
    }
}

/// Query one provider's discovery source and record the outcome.
///
/// `None` when the provider has no name. Poll failures are logged and counted.
async fn poll(
    provider: &InferenceProvider,
    config: &ModelDiscoveryConfig,
    client: &Client,
    timeout: Duration,
) -> Option<ProviderPoll> {
    let name = provider.metadata.name.as_deref()?.to_owned();
    let result = match config {
        ModelDiscoveryConfig::OpenAiModels(openai) => query_openai(provider, openai, client, timeout).await,
    };

    let outcome = match result {
        Ok(models) => {
            metrics::record_model_discovery_success(&name);
            PollOutcome::Served(models)
        },
        Err(error) => {
            metrics::record_model_discovery_failure(&name, error.as_reason());
            tracing::warn!(provider = %name, %error, "model discovery failed");
            PollOutcome::Failed
        },
    };

    Some(ProviderPoll { name, outcome })
}

/// Build an [`OpenAiModels`] source and list its models.
async fn query_openai(
    provider: &InferenceProvider,
    openai: &OpenAiModelsSource,
    client: &Client,
    timeout: Duration,
) -> Result<ServedModels, PollError> {
    let source = openai_source(provider, openai, client, timeout).await?;
    Ok(source.served_models().await?)
}

/// Build an [`OpenAiModels`] source, resolving credentials and TLS.
async fn openai_source(
    provider: &InferenceProvider,
    openai: &OpenAiModelsSource,
    client: &Client,
    timeout: Duration,
) -> Result<OpenAiModels, PollError> {
    let name = provider.metadata.name.as_deref().unwrap_or("?");
    let base = openai.endpoint.as_deref().unwrap_or(&provider.spec.endpoint);
    let url = join_url(base, &openai.path);

    let token = match credentials::credential_plan_from_auth(provider.spec.auth.as_ref()) {
        Ok(CredentialPlan::Bearer(secret_ref)) => Some(
            KubernetesSecretResolver::new(client.clone())
                .resolve(&secret_ref)
                .await
                .map_err(|e| PollError::Credential(e.to_string()))?,
        ),
        Ok(CredentialPlan::Absent | CredentialPlan::Manual) => None,
        Err(e) => return Err(PollError::Credential(e.to_string())),
    };

    let tls = endpoint_tls::resolve_tls_config(openai.tls.as_ref(), Some(client), name)
        .await
        .map_err(|(_, message)| PollError::Tls(message))?;

    Ok(OpenAiModels::new(&url, token.as_ref(), tls, timeout)?)
}

/// Join a base URL and a path with exactly one `/` between them.
///
/// `("http://h/", "/v1/models")` and `("http://h", "v1/models")` both yield
/// `"http://h/v1/models"`.
fn join_url(base: &str, path: &str) -> String {
    format!("{}/{}", base.trim_end_matches('/'), path.trim_start_matches('/'))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::model_discovery::ServedModelsError;

    const TTL: Duration = Duration::from_secs(60);

    #[test]
    fn success_replaces_set() {
        let store = ServedModelStore::new();

        store.refresh(round(&[("p", Some(&["old"]))]), Instant::now(), TTL);
        store.refresh(round(&[("p", Some(&["b", "a"]))]), Instant::now(), TTL);

        assert_eq!(
            names(&store, "p"),
            Some(owned(&["a", "b"])),
            "success should replace set"
        );
    }

    #[test]
    fn empty_success_is_held() {
        let store = ServedModelStore::new();

        store.refresh(round(&[("p", Some(&["a"]))]), Instant::now(), TTL);
        store.refresh(round(&[("p", Some(&[]))]), Instant::now(), TTL);

        assert_eq!(names(&store, "p"), Some(Vec::new()), "empty success should be held");
    }

    #[test]
    fn failure_keeps_last_good_set() {
        let store = ServedModelStore::new();

        store.refresh(round(&[("p", Some(&["a"]))]), Instant::now(), TTL);
        store.refresh(round(&[("p", None)]), Instant::now(), TTL);

        assert_eq!(names(&store, "p"), Some(owned(&["a"])), "failure should keep set");
    }

    #[test]
    fn unrenewed_set_expires() {
        let store = ServedModelStore::new();

        store.refresh(round(&[("p", Some(&["a"]))]), Instant::now(), Duration::ZERO);

        assert_eq!(names(&store, "p"), None, "expired set should not be served");
    }

    #[test]
    fn unpolled_provider_is_dropped() {
        let store = ServedModelStore::new();

        store.refresh(round(&[("p", Some(&["a"])), ("q", Some(&["b"]))]), Instant::now(), TTL);
        store.refresh(round(&[("q", None)]), Instant::now(), TTL);

        assert_eq!(names(&store, "p"), None, "unconfigured provider should be dropped");
        assert_eq!(
            names(&store, "q"),
            Some(owned(&["b"])),
            "failed provider should be kept"
        );
    }

    #[test]
    fn failures_map_to_reasons() {
        assert_eq!(
            PollError::Credential(String::new()).as_reason(),
            "credential",
            "credential"
        );
        assert_eq!(
            PollError::from(DiscoveryError::Status(http::StatusCode::UNAUTHORIZED)).as_reason(),
            "http_status",
            "status"
        );
        assert_eq!(
            PollError::from(DiscoveryError::InvalidModels(ServedModelsError::TooMany)).as_reason(),
            "invalid_response",
            "invalid models"
        );
    }

    #[test]
    fn urls_are_joined_with_one_slash() {
        assert_eq!(
            join_url("http://h/", "/v1/models"),
            "http://h/v1/models",
            "both slashes"
        );
        assert_eq!(join_url("http://h", "v1/models"), "http://h/v1/models", "no slashes");
        assert_eq!(
            join_url("http://h/api", "/v1/models"),
            "http://h/api/v1/models",
            "base path"
        );
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    fn round(entries: &[(&str, Option<&[&str]>)]) -> Vec<ProviderPoll> {
        entries
            .iter()
            .map(|&(provider, models)| {
                let outcome = match models {
                    Some(names) => PollOutcome::Served(
                        ServedModels::try_from_names(names.iter().map(|&name| name.to_owned()))
                            .unwrap_or_else(|_| std::process::abort()),
                    ),
                    None => PollOutcome::Failed,
                };
                ProviderPoll {
                    name: provider.to_owned(),
                    outcome,
                }
            })
            .collect()
    }

    fn owned(names: &[&str]) -> Vec<String> {
        names.iter().map(|&n| n.to_owned()).collect()
    }

    fn names(store: &ServedModelStore, provider: &str) -> Option<Vec<String>> {
        store.models(provider).map(|models| {
            let mut names = models.to_vec();
            names.sort_unstable();
            names
        })
    }
}
