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
    api::{Api, ListParams, Patch, PatchParams},
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
struct ProviderPoll(
    /// Models reported, or a failed poll.
    PollOutcome,
    /// `InferenceProvider` name.
    String,
);

/// The status-relevant outcome of one provider poll.
struct ProviderPollStatus {
    /// `InferenceProvider` name.
    name: String,
    /// Failure reason, or `None` after a successful poll.
    failure_reason: Option<&'static str>,
}

/// What one provider poll reported.
enum PollOutcome {
    /// The validated served-model set, including an empty set.
    Served(ServedModels),

    /// Polling failed; the last good set may remain held.
    Failed(PollError),
}

impl PollOutcome {
    /// Bounded failure reason, absent after a successful poll.
    fn failure_reason(&self) -> Option<&'static str> {
        match self {
            Self::Served(_) => None,
            Self::Failed(error) => Some(error.as_reason()),
        }
    }
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
    /// Changes, expirations, and drops are logged at `debug`.
    fn refresh(&self, updated: Vec<ProviderPoll>, now: Instant, ttl: Duration) {
        let expires_at = now.checked_add(ttl);
        let Ok(mut map) = self.inner.write() else {
            return;
        };

        prune_before_refresh(&mut map, &updated, now);
        for ProviderPoll(outcome, name) in updated {
            match outcome {
                PollOutcome::Served(discovered) => match expires_at {
                    Some(expires_at) => {
                        let models: Arc<[String]> = discovered.into_names().into();
                        tracing::debug!(name, ?models, "served models refreshed");
                        map.insert(name.clone(), ServedModel { models, expires_at });
                    },
                    None => {
                        tracing::warn!(name, ?ttl, "model discovery TTL exceeds the monotonic clock range");
                    },
                },
                PollOutcome::Failed(_) => {},
            }
        }
    }
}

/// Remove unpolled and expired entries before applying successful results.
fn prune_before_refresh(map: &mut BTreeMap<String, ServedModel>, updated: &[ProviderPoll], now: Instant) {
    map.retain(
        |provider: &String, served| match updated.iter().find(|ProviderPoll(_, p)| *p == *provider) {
            None => {
                tracing::debug!(provider, "served models dropped");
                false
            },
            Some(ProviderPoll(PollOutcome::Failed(_), _)) if served.expires_at <= now => {
                tracing::debug!(provider, "served models expired");
                false
            },
            Some(_) => served.expires_at > now,
        },
    );
}

// ---------------------------------------------------------------------------
// Discovery round
// ---------------------------------------------------------------------------

/// Field manager for discovery-owned status fields.
const DISCOVERY_FIELD_MANAGER: &str = "grid-model-discovery";

/// Poll every provider that opts into discovery and refresh `store`.
///
/// # Errors
///
/// Returns [`OperatorError`] when providers cannot be listed. A list failure
/// leaves `store` untouched. Status patch failures are logged per provider.
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

    let status_updates = polled
        .iter()
        .map(|ProviderPoll(outcome, name)| ProviderPollStatus {
            name: name.clone(),
            failure_reason: outcome.failure_reason(),
        })
        .collect::<Vec<_>>();
    store.refresh(polled, Instant::now(), config.ttl);
    publish_discovery_status(&api, &providers, &status_updates).await;
    Ok(())
}

/// Patch changed discovery errors and clear them after success or when disabled.
async fn publish_discovery_status(
    api: &Api<InferenceProvider>,
    providers: &[InferenceProvider],
    polled: &[ProviderPollStatus],
) {
    for provider in providers {
        if let Some((name, incoming_error)) = discovery_error_patch_for_provider(provider, polled)
            && let Err(error) = patch_discovery_status(api, name, incoming_error).await
        {
            tracing::warn!(provider = %name, %error, "failed to patch model discovery status");
        }
    }
}

/// Decide whether one provider's discovery error needs a status patch.
///
/// `None` means no patch; `Some((name, None))` clears the field; and
/// `Some((name, Some(reason)))` sets it.
fn discovery_error_patch_for_provider<'provider>(
    provider: &'provider InferenceProvider,
    polled: &[ProviderPollStatus],
) -> Option<(&'provider str, Option<&'static str>)> {
    let provider_name = provider.metadata.name.as_deref()?;
    let incoming_error = polled
        .iter()
        .find(|status| status.name == provider_name)
        .and_then(|status| status.failure_reason);
    let current_error = provider
        .status
        .as_ref()
        .and_then(|status| status.model_discovery_error.as_deref());
    if current_error == incoming_error {
        None
    } else {
        Some((provider_name, incoming_error))
    }
}

/// Apply only the discovery-owned status field.
async fn patch_discovery_status(
    api: &Api<InferenceProvider>,
    name: &str,
    error: Option<&str>,
) -> Result<(), OperatorError> {
    let discovery_status = match error {
        Some(error) => serde_json::json!({ "modelDiscoveryError": error }),
        None => serde_json::json!({}),
    };
    let patch = serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": name },
        "status": discovery_status
    });
    api.patch_status(
        name,
        &PatchParams::apply(DISCOVERY_FIELD_MANAGER).force(),
        &Patch::Apply(patch),
    )
    .await?;
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
            let reason = error.as_reason();
            metrics::record_model_discovery_failure(&name, reason);
            tracing::warn!(provider = %name, %error, "model discovery failed");
            PollOutcome::Failed(error)
        },
    };

    Some(ProviderPoll(outcome, name))
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
    let url = openai.effective_url(&provider.spec.endpoint);

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

    let ep_tls = openai.tls.as_ref().or(provider.spec.tls.as_ref());
    let client_tls = endpoint_tls::resolve_tls_config(ep_tls, Some(client), name)
        .await
        .map_err(|(_, message)| PollError::Tls(message))?;

    Ok(OpenAiModels::new(&url, token.as_ref(), client_tls.as_ref(), timeout)?)
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
    fn discovery_error_patch_sets_failure_and_skips_unchanged_error() {
        let provider_name = "provider-a";
        let provider_without_status = test_provider(provider_name, None);
        let provider_with_error = test_provider(provider_name, Some("credential"));
        let failed = vec![ProviderPollStatus {
            name: provider_name.to_owned(),
            failure_reason: Some("credential"),
        }];
        assert_eq!(
            discovery_error_patch_for_provider(&provider_without_status, &failed),
            Some((provider_name, Some("credential"))),
            "a failed poll should set its reason"
        );
        assert_eq!(
            discovery_error_patch_for_provider(&provider_with_error, &failed),
            None,
            "an unchanged error should not be patched"
        );
    }

    #[test]
    fn discovery_error_patch_clears_after_success_or_disabled_source() {
        let provider_name = "provider-a";
        let provider_with_error = test_provider(provider_name, Some("credential"));
        assert_eq!(
            discovery_error_patch_for_provider(
                &provider_with_error,
                &[ProviderPollStatus {
                    name: provider_name.to_owned(),
                    failure_reason: None,
                }],
            ),
            Some((provider_name, None)),
            "a successful poll should clear the previous error"
        );
        assert_eq!(
            discovery_error_patch_for_provider(&provider_with_error, &[]),
            Some((provider_name, None)),
            "removing model discovery should clear the previous error"
        );
    }

    #[test]
    fn urls_are_joined_with_one_slash() {
        let url = |base: &str, path: &str| {
            OpenAiModelsSource {
                endpoint: Some(base.to_owned()),
                path: path.to_owned(),
                tls: None,
            }
            .effective_url("unused")
        };
        assert_eq!(url("http://h/", "/v1/models"), "http://h/v1/models", "both slashes");
        assert_eq!(url("http://h", "v1/models"), "http://h/v1/models", "no slashes");
        assert_eq!(url("http://h/api", "/v1/models"), "http://h/api/v1/models", "base path");
    }

    /// Build an OpenAI-compatible model-list response.
    fn model_list_response(models: &[&str]) -> Vec<u8> {
        let body = serde_json::json!({
            "data": models.iter().map(|model| serde_json::json!({"id": model})).collect::<Vec<_>>()
        })
        .to_string();
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    /// Start a TLS model-list endpoint signed by `ca`.
    async fn tls_model_discovery_endpoint(ca: &certs::CaCert, models: &[&str]) -> String {
        let server_cert =
            certs::generate_dns_cert(ca, "model-server", "localhost").unwrap_or_else(|_| std::process::abort());
        crate::resources::test_doubles::start_tls_http_server(
            &server_cert.cert_pem,
            &server_cert.key_pem,
            model_list_response(models),
        )
        .await
    }

    fn openai_source(
        endpoint: Option<&str>,
        tls: Option<crate::crd::inference_provider::EndpointTlsConfig>,
    ) -> OpenAiModelsSource {
        OpenAiModelsSource {
            endpoint: endpoint.map(str::to_owned),
            path: "/v1/models".to_owned(),
            tls,
        }
    }

    #[tokio::test]
    async fn model_discovery_inherits_shared_tls() {
        let ca = certs::generate_ca("shared-discovery-ca").unwrap_or_else(|_| std::process::abort());
        let endpoint = tls_model_discovery_endpoint(&ca, &["model-a", "model-b"]).await;
        let client =
            crate::resources::test_doubles::mock_kube_client_with_secrets(std::collections::HashMap::from([(
                "shared-ca",
                crate::resources::test_doubles::secret_with_key("ca.crt", ca.cert_pem.as_bytes()),
            )]));
        let mut provider = test_provider("provider-a", None);
        provider.spec.endpoint = endpoint;
        provider.spec.tls = Some(crate::resources::test_doubles::endpoint_tls_for_ca("shared-ca"));

        let models = query_openai(&provider, &openai_source(None, None), &client, Duration::from_secs(5))
            .await
            .unwrap_or_else(|_| std::process::abort())
            .into_names();

        assert_eq!(models, vec!["model-a", "model-b"]);
    }

    #[tokio::test]
    async fn model_discovery_override_takes_precedence_over_shared_tls() {
        let ca = certs::generate_ca("override-discovery-ca").unwrap_or_else(|_| std::process::abort());
        let endpoint = tls_model_discovery_endpoint(&ca, &["model-a"]).await;
        let client =
            crate::resources::test_doubles::mock_kube_client_with_secrets(std::collections::HashMap::from([(
                "override-ca",
                crate::resources::test_doubles::secret_with_key("ca.crt", ca.cert_pem.as_bytes()),
            )]));
        let mut provider = test_provider("provider-a", None);
        provider.spec.endpoint = "https://unused.invalid".to_owned();
        provider.spec.tls = Some(crate::resources::test_doubles::endpoint_tls_for_ca("missing-shared-ca"));

        let models = query_openai(
            &provider,
            &openai_source(
                Some(&endpoint),
                Some(crate::resources::test_doubles::endpoint_tls_for_ca("override-ca")),
            ),
            &client,
            Duration::from_secs(5),
        )
        .await
        .unwrap_or_else(|_| std::process::abort())
        .into_names();

        assert_eq!(
            models,
            vec!["model-a"],
            "openai.tls should override shared provider TLS"
        );
    }

    #[tokio::test]
    async fn model_discovery_shared_tls_resolution_failure_is_reported() {
        let client = crate::resources::test_doubles::mock_kube_client_with_secrets(std::collections::HashMap::new());
        let mut provider = test_provider("provider-a", None);
        provider.spec.endpoint = "https://localhost:8443".to_owned();
        provider.spec.tls = Some(crate::resources::test_doubles::endpoint_tls_for_ca("missing-shared-ca"));

        let result = query_openai(&provider, &openai_source(None, None), &client, Duration::from_secs(5)).await;

        assert!(
            matches!(result, Err(PollError::Tls(_))),
            "invalid shared TLS material must fail the poll"
        );
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    fn test_provider(name: &str, discovery_error: Option<&str>) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://provider",
                "models": [{ "name": "model-a" }]
            },
            "status": discovery_error.map(|error| serde_json::json!({ "modelDiscoveryError": error }))
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn round(entries: &[(&str, Option<&[&str]>)]) -> Vec<ProviderPoll> {
        entries
            .iter()
            .map(|&(provider, models)| {
                let outcome = match models {
                    Some(names) => PollOutcome::Served(
                        ServedModels::try_from_names(names.iter().map(|&name| name.to_owned()))
                            .unwrap_or_else(|_| std::process::abort()),
                    ),
                    None => PollOutcome::Failed(PollError::Credential(String::new())),
                };
                ProviderPoll(outcome, provider.to_owned())
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
