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
//!       provider UID/URL changed → drop set
//!       provider not polled   → drop set
//!       deadline passed       → drop set
//!     patch the discovery-owned status fields
//!       successful poll       → status.servedModels and servedModelsExpiresAt
//!       expired set           → both status fields absent
//!       failure reason        → status.modelDiscoveryError
//! ```
//!
//! The in-memory store uses a monotonic TTL. The paired status expiry is a
//! wall-clock boundary so readers can reject a status that remains uncleared
//! between expiry and the next discovery round. `status.servedModels` and its
//! expiry are the durable hand-off to reconcile.

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
    crd::inference_provider::{InferenceProvider, InferenceProviderStatus, ModelDiscoveryConfig, OpenAiModelsSource},
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
    /// Provider incarnation and source that produced this set.
    source: DiscoveryKey,

    /// Validated model names.
    models: Arc<[String]>,

    /// Deadline after which the set is no longer served.
    expires_at: Instant,

    /// RFC 3339 UTC deadline persisted with the model set.
    expires_at_status: String,
}

/// Identity of the provider and concrete endpoint used for discovery.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DiscoveryKey {
    /// Kubernetes UID distinguishes replacements with the same name.
    uid: String,

    /// URL includes the configured endpoint fallback and models path.
    url: String,
}

impl DiscoveryKey {
    /// Resolve the identity for a discoverable provider.
    fn for_provider(provider: &InferenceProvider) -> Option<Self> {
        let uid = provider.metadata.uid.clone()?;
        let url = match provider.spec.model_discovery.as_ref()? {
            ModelDiscoveryConfig::OpenAiModels(source) => source.effective_url(&provider.spec.endpoint),
        };
        Some(Self { uid, url })
    }
}

/// Paired clock readings taken when one provider poll completes successfully.
#[derive(Clone, Copy)]
struct PollCompletedAt {
    /// Monotonic completion time for the in-memory deadline.
    monotonic: Instant,

    /// Wall-clock completion time for the status deadline.
    wall: time::OffsetDateTime,
}

impl PollCompletedAt {
    /// Capture both clocks at poll completion.
    fn now() -> Self {
        Self {
            wall: time::OffsetDateTime::now_utc(),
            monotonic: Instant::now(),
        }
    }

    /// Calculate the paired in-memory and status expiry boundaries for this poll.
    fn status_expiry(self, ttl: Duration) -> Option<(Instant, String)> {
        let monotonic = self.monotonic.checked_add(ttl)?;
        let wall_ttl = time::Duration::try_from(ttl).ok()?;
        let status = self
            .wall
            .checked_add(wall_ttl)?
            .format(&time::format_description::well_known::Rfc3339)
            .ok()?;
        Some((monotonic, status))
    }
}

/// The result of polling one named provider.
struct ProviderPoll(
    /// Models reported, or a failed poll.
    PollOutcome,
    /// `InferenceProvider` name.
    String,
    /// Provider incarnation and source that was polled.
    DiscoveryKey,
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
    /// The validated served-model set and its completion time.
    Served(ServedModels, PollCompletedAt),

    /// Polling failed; the last good set may remain held.
    Failed(PollError),
}

impl PollOutcome {
    /// Bounded failure reason, absent after a successful poll.
    fn failure_reason(&self) -> Option<&'static str> {
        match self {
            Self::Served(..) => None,
            Self::Failed(error) => Some(error.as_reason()),
        }
    }
}

/// Provider names mapped to their currently held served-model sets.
#[derive(Debug, Default)]
struct ServedModelMap(
    /// Provider name to held served-model inventory.
    BTreeMap<String, ServedModel>,
);

impl ServedModelMap {
    /// Look up the held inventory for a provider.
    fn get(&self, name: &str) -> Option<&ServedModel> {
        self.0.get(name)
    }

    /// Drop providers no longer polled and sets whose freshness deadline passed.
    fn retain_for_refresh(&mut self, updated: &[ProviderPoll], now: Instant) {
        self.0.retain(|provider, served| {
            let maybe_provider = updated.iter().find(|ProviderPoll(_, p, _)| *p == *provider);
            match maybe_provider {
                None => {
                    tracing::debug!(provider, "served models dropped");
                    false
                },
                Some(ProviderPoll(_, _, source)) if served.source != *source => {
                    tracing::debug!(provider, "served models provider or source changed");
                    false
                },
                Some(ProviderPoll(PollOutcome::Failed(_), ..)) if served.expires_at <= now => {
                    tracing::debug!(provider, "served models expired");
                    false
                },
                Some(_) => served.expires_at > now,
            }
        });
    }

    /// Replace held sets for providers whose discovery poll succeeded.
    fn insert_updated(&mut self, updated: Vec<ProviderPoll>, ttl: Duration) {
        for ProviderPoll(outcome, name, source) in updated {
            if let PollOutcome::Served(discovered, completed_at) = outcome {
                let Some((expires_at, expires_at_status)) = completed_at.status_expiry(ttl) else {
                    tracing::warn!(
                        name,
                        ?ttl,
                        "model discovery expiry exceeds the supported timestamp range"
                    );
                    continue;
                };
                let models: Arc<[String]> = discovered.into_names().into();
                tracing::debug!(name, ?models, "served models refreshed");
                self.0.insert(
                    name,
                    ServedModel {
                        source,
                        models,
                        expires_at,
                        expires_at_status,
                    },
                );
            }
        }
    }
}

/// Served-model sets keyed by `InferenceProvider` name.
///
/// Cheap to clone; clones share the same sets.
#[derive(Clone, Debug, Default)]
pub struct ServedModelStore {
    /// Provider name to its held set.
    inner: Arc<RwLock<ServedModelMap>>,
}

impl ServedModelStore {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Models `provider` serves, or `None` when unknown, changed, or expired.
    #[must_use]
    pub fn models(&self, provider: &InferenceProvider) -> Option<Arc<[String]>> {
        self.fresh_inventory(provider).map(|(models, _)| models)
    }

    /// Models and their persisted expiry deadline for the same provider source.
    fn fresh_inventory(&self, provider: &InferenceProvider) -> Option<(Arc<[String]>, String)> {
        let name = provider.metadata.name.as_deref()?;
        let source = DiscoveryKey::for_provider(provider)?;
        let now = Instant::now();
        let guard = self.inner.read().ok()?;
        guard
            .get(name)
            .filter(|held| held.source == source && held.expires_at > now)
            .map(|held| (Arc::clone(&held.models), held.expires_at_status.clone()))
    }

    /// Apply one round of poll results.
    ///
    /// `updated` holds every provider polled this round: a served set replaces
    /// the old one, while a failed poll keeps it until expiry. Providers absent
    /// from `updated` are no longer configured and are dropped.
    ///
    /// Changes, expirations, and drops are logged at `debug`.
    fn refresh(&self, updated: Vec<ProviderPoll>, ttl: Duration) {
        let Ok(mut map) = self.inner.write() else {
            return;
        };

        map.retain_for_refresh(&updated, Instant::now());
        map.insert_updated(updated, ttl);
    }
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
        .map(|ProviderPoll(outcome, name, _)| ProviderPollStatus {
            name: name.clone(),
            failure_reason: outcome.failure_reason(),
        })
        .collect::<Vec<_>>();
    store.refresh(polled, config.ttl);
    publish_discovery_status(&api, &providers, store, &status_updates).await;
    Ok(())
}

/// Patch changed discovery-owned status: the failure reason, served set, and expiry.
///
/// `store` is read after this round's refresh, so an expired or dropped set
/// reads as absent and is cleared from the status.
async fn publish_discovery_status(
    api: &Api<InferenceProvider>,
    providers: &[InferenceProvider],
    store: &ServedModelStore,
    polled: &[ProviderPollStatus],
) {
    for provider in providers {
        if let Some(desired) = DiscoveryStatusPatch::for_provider(provider, store, polled) {
            match patch_discovery_status(api, provider, &desired).await {
                Ok(()) => {},
                Err(error) if error.is_conflict() => {
                    tracing::debug!(provider = %desired.name, "model discovery status changed before patch");
                },
                Err(error) => {
                    tracing::warn!(provider = %desired.name, %error, "failed to patch model discovery status");
                },
            }
        }
    }
}

/// The discovery-owned status desired for one provider.
#[derive(Debug, PartialEq)]
struct DiscoveryStatusPatch {
    /// `InferenceProvider` name.
    name: String,

    /// Failure reason, or `None` after a successful poll.
    failure_reason: Option<&'static str>,

    /// Served-model set, or `None` when unknown, expired, or discovery is off.
    models: Option<Vec<String>>,

    /// RFC 3339 expiry of the served-model set, absent with the set.
    expires_at: Option<String>,
}

impl DiscoveryStatusPatch {
    /// Decide whether one provider's discovery status needs a patch.
    ///
    /// `None` when the current status already shows the desired failure reason
    /// and served-model set. The desired set is the store's effective set, which
    /// is absent for a provider whose discovery is disabled, never succeeded, or
    /// expired: absence is the fail-closed state. An empty set is present and
    /// distinct from absence.
    fn for_provider(
        provider: &InferenceProvider,
        store: &ServedModelStore,
        polled: &[ProviderPollStatus],
    ) -> Option<Self> {
        let name = provider.metadata.name.as_deref()?;
        let failure_reason = polled
            .iter()
            .find(|status| status.name == name)
            .and_then(|status| status.failure_reason);
        let inventory = store.fresh_inventory(provider);
        let (models, expires_at) = inventory.map_or((None, None), |(models, expires_at)| {
            (Some(models.to_vec()), Some(expires_at))
        });
        let desired = DiscoveryStatusPatch {
            name: name.to_owned(),
            failure_reason,
            models,
            expires_at,
        };
        let current = provider.status.as_ref();
        if desired.has_same_error(current) && desired.has_same_models(current) && desired.has_same_expiration(current) {
            return None;
        }
        Some(desired)
    }

    /// Whether `current` shows exactly this patch's served-model set.
    ///
    /// Model lists are compared as sets: poll response order is not stable,
    /// so a reordered set is not a change. Both sides are sorted for the
    /// comparison only — the store and the status keep the order they were
    /// given — and sorting also keeps duplicate entries from comparing
    /// equal.
    fn has_same_models(&self, current: Option<&InferenceProviderStatus>) -> bool {
        let current_models = current.and_then(|status| status.served_models.as_deref());
        let sorted = |models: &[String]| {
            let mut names = models.to_vec();
            names.sort_unstable();
            names
        };
        match (current_models, self.models.as_deref()) {
            (None, None) => true,
            (Some(current_names), Some(desired_names)) => sorted(current_names) == sorted(desired_names),
            _ => false,
        }
    }

    /// Whether `current` shows exactly this patch's failure reason.
    fn has_same_error(&self, current: Option<&InferenceProviderStatus>) -> bool {
        let current_error = current.and_then(|status| status.model_discovery_error.as_deref());
        current_error == self.failure_reason
    }

    /// Whether `current` shows exactly this served-model expiry boundary.
    fn has_same_expiration(&self, current: Option<&InferenceProviderStatus>) -> bool {
        current.and_then(|status| status.served_models_expires_at.as_deref()) == self.expires_at.as_deref()
    }

    /// Build the discovery-owned status fields to apply.
    ///
    /// Server-side apply removes a field this manager owns when the applied
    /// config omits it, which clears expired or disabled discovery fields.
    fn status_fields(&self) -> serde_json::Value {
        let mut status = serde_json::Map::new();
        if let Some(reason) = self.failure_reason {
            status.insert("modelDiscoveryError".to_owned(), serde_json::json!(reason));
        }
        if let Some(models) = &self.models {
            status.insert("servedModels".to_owned(), serde_json::json!(models));
        }
        if let Some(expires_at) = &self.expires_at {
            status.insert("servedModelsExpiresAt".to_owned(), serde_json::json!(expires_at));
        }
        status.into()
    }
}

/// Apply only the discovery-owned status fields at the listed resource version.
async fn patch_discovery_status(
    api: &Api<InferenceProvider>,
    provider: &InferenceProvider,
    desired: &DiscoveryStatusPatch,
) -> Result<(), OperatorError> {
    let version = provider.metadata.resource_version.as_deref().ok_or_else(|| {
        OperatorError::InvalidResource(format!("InferenceProvider {} has no resourceVersion", desired.name))
    })?;
    let apply = serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": desired.name, "resourceVersion": version },
        "status": desired.status_fields()
    });
    Box::pin(api.patch_status(
        &desired.name,
        &PatchParams::apply(DISCOVERY_FIELD_MANAGER).force(),
        &Patch::Apply(apply),
    ))
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
    let source = DiscoveryKey::for_provider(provider)?;
    let result = match config {
        ModelDiscoveryConfig::OpenAiModels(openai) => query_openai(provider, openai, client, timeout).await,
    };

    let outcome = match result {
        Ok(models) => {
            let completed_at = PollCompletedAt::now();
            metrics::record_model_discovery_success(&name);
            PollOutcome::Served(models, completed_at)
        },
        Err(error) => {
            let reason = error.as_reason();
            metrics::record_model_discovery_failure(&name, reason);
            tracing::warn!(provider = %name, %error, "model discovery failed");
            PollOutcome::Failed(error)
        },
    };

    Some(ProviderPoll(outcome, name, source))
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

        refresh(&store, round(&[("p", Some(&["old"]))]), TTL);
        refresh(&store, round(&[("p", Some(&["b", "a"]))]), TTL);

        assert_eq!(
            names(&store, "p"),
            Some(owned(&["a", "b"])),
            "success should replace set"
        );
    }

    #[test]
    fn empty_success_is_held() {
        let store = ServedModelStore::new();

        refresh(&store, round(&[("p", Some(&["a"]))]), TTL);
        refresh(&store, round(&[("p", Some(&[]))]), TTL);

        assert_eq!(names(&store, "p"), Some(Vec::new()), "empty success should be held");
    }

    #[test]
    fn failure_keeps_last_good_set() {
        let store = ServedModelStore::new();

        refresh(&store, round(&[("p", Some(&["a"]))]), TTL);
        refresh(&store, round(&[("p", None)]), TTL);

        assert_eq!(names(&store, "p"), Some(owned(&["a"])), "failure should keep set");
    }

    #[test]
    fn unrenewed_set_expires() {
        let store = ServedModelStore::new();

        refresh(&store, round(&[("p", Some(&["a"]))]), Duration::ZERO);

        assert_eq!(names(&store, "p"), None, "expired set should not be served");
    }

    #[test]
    fn unpolled_provider_is_dropped() {
        let store = ServedModelStore::new();

        refresh(&store, round(&[("p", Some(&["a"])), ("q", Some(&["b"]))]), TTL);
        refresh(&store, round(&[("q", None)]), TTL);

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
    fn failure_sets_reason_and_skips_unchanged_status() {
        let store = ServedModelStore::new();
        let provider_name = "provider-a";
        let provider_without_status = test_provider(provider_name, None);
        let provider_with_error = test_provider(provider_name, Some("credential"));
        let failed = vec![ProviderPollStatus {
            name: provider_name.to_owned(),
            failure_reason: Some("credential"),
        }];
        assert_eq!(
            DiscoveryStatusPatch::for_provider(&provider_without_status, &store, &failed),
            Some(DiscoveryStatusPatch {
                name: provider_name.to_owned(),
                failure_reason: Some("credential"),
                models: None,
                expires_at: None,
            }),
            "a failed poll should set its reason"
        );
        assert_eq!(
            DiscoveryStatusPatch::for_provider(&provider_with_error, &store, &failed),
            None,
            "an unchanged error should not be patched"
        );
    }

    #[test]
    fn success_or_disabled_discovery_clears_a_previous_error() {
        let store = ServedModelStore::new();
        let provider_name = "provider-a";
        let provider_with_error = test_provider(provider_name, Some("credential"));
        let succeeded = vec![ProviderPollStatus {
            name: provider_name.to_owned(),
            failure_reason: None,
        }];
        assert_eq!(
            DiscoveryStatusPatch::for_provider(&provider_with_error, &store, &succeeded),
            Some(DiscoveryStatusPatch {
                name: provider_name.to_owned(),
                failure_reason: None,
                models: None,
                expires_at: None,
            }),
            "a successful poll should clear the previous error"
        );
        let polled = &[];
        assert_eq!(
            DiscoveryStatusPatch::for_provider(&provider_with_error, &store, polled),
            Some(DiscoveryStatusPatch {
                name: provider_name.to_owned(),
                failure_reason: None,
                models: None,
                expires_at: None,
            }),
            "removing model discovery should clear the previous error"
        );
    }

    #[test]
    fn success_writes_the_served_set() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["b", "a"]))]), TTL);

        let provider = &discovery_provider("p", &serde_json::json!({}));
        let polled = &[];
        let patch = DiscoveryStatusPatch::for_provider(provider, &store, polled);

        assert_eq!(
            patch,
            Some(DiscoveryStatusPatch {
                name: "p".to_owned(),
                failure_reason: None,
                models: Some(owned(&["b", "a"])),
                expires_at: stored_expiry(&store, "p"),
            }),
            "a changed set should be written as polled"
        );
    }

    #[test]
    fn an_empty_set_is_written_not_cleared() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&[]))]), TTL);

        let provider = &discovery_provider("p", &serde_json::json!({}));
        let polled = &[];
        let patch = DiscoveryStatusPatch::for_provider(provider, &store, polled);

        assert_eq!(
            patch.map(|desired| desired.models),
            Some(Some(Vec::new())),
            "an empty discovered set is present, distinct from absence"
        );
    }

    #[test]
    fn unchanged_status_is_not_patched() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["a"]))]), TTL);

        let provider = discovery_provider("p", &status_with_models(&store, &["a"]));

        let polled = &[];
        assert_eq!(
            DiscoveryStatusPatch::for_provider(&provider, &store, polled),
            None,
            "an unchanged set should not be patched"
        );
    }

    #[test]
    fn a_reordered_set_is_not_a_change() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["a", "b"]))]), TTL);

        let provider = discovery_provider("p", &status_with_models(&store, &["b", "a"]));

        let polled = &[];
        assert_eq!(
            DiscoveryStatusPatch::for_provider(&provider, &store, polled),
            None,
            "response order is not part of the set"
        );
    }

    #[test]
    fn a_substituted_model_is_a_change() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["a", "b"]))]), TTL);

        let provider = discovery_provider("p", &serde_json::json!({ "servedModels": ["a", "c"] }));

        let polled = &[];
        assert_eq!(
            DiscoveryStatusPatch::for_provider(&provider, &store, polled),
            Some(DiscoveryStatusPatch {
                name: "p".to_owned(),
                failure_reason: None,
                models: Some(owned(&["a", "b"])),
                expires_at: stored_expiry(&store, "p"),
            }),
            "a substituted model must be written"
        );
    }

    #[test]
    fn failure_keeps_the_held_set_in_status() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["a"]))]), TTL);
        refresh(&store, round(&[("p", None)]), TTL);

        let provider = discovery_provider("p", &status_with_models(&store, &["a"]));
        let failed = vec![ProviderPollStatus {
            name: "p".to_owned(),
            failure_reason: Some("unreachable"),
        }];

        assert_eq!(
            DiscoveryStatusPatch::for_provider(&provider, &store, &failed),
            Some(DiscoveryStatusPatch {
                name: "p".to_owned(),
                failure_reason: Some("unreachable"),
                models: Some(owned(&["a"])),
                expires_at: stored_expiry(&store, "p"),
            }),
            "a failed poll sets its reason and keeps the held set"
        );
    }

    #[test]
    fn failed_poll_after_provider_replacement_clears_old_inventory() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["a"]))]), TTL);
        let mut provider = discovery_provider("p", &status_with_models(&store, &["a"]));
        provider.metadata.uid = Some("replacement-uid".to_owned());
        assert_failed_poll_clears_old_inventory(&store, &provider);
    }

    #[test]
    fn failed_poll_after_endpoint_change_clears_old_inventory() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["a"]))]), TTL);
        let mut provider = discovery_provider("p", &status_with_models(&store, &["a"]));
        provider.spec.endpoint = "http://replacement".to_owned();
        assert_failed_poll_clears_old_inventory(&store, &provider);
    }

    #[test]
    fn expired_set_is_cleared_from_status() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["a"]))]), Duration::ZERO);

        let provider = discovery_provider("p", &serde_json::json!({ "servedModels": ["a"] }));
        let failed = vec![ProviderPollStatus {
            name: "p".to_owned(),
            failure_reason: Some("unreachable"),
        }];

        assert_eq!(
            DiscoveryStatusPatch::for_provider(&provider, &store, &failed),
            Some(DiscoveryStatusPatch {
                name: "p".to_owned(),
                failure_reason: Some("unreachable"),
                models: None,
                expires_at: None,
            }),
            "an expired set must leave the status: fail closed"
        );
    }

    #[test]
    fn removed_discovery_clears_the_served_set() {
        let store = ServedModelStore::new();
        let provider = test_provider_with_status(
            "p",
            &serde_json::json!({
                "servedModels": ["a"],
                "servedModelsExpiresAt": "2030-01-01T00:00:00Z"
            }),
        );

        let polled = &[];
        assert_eq!(
            DiscoveryStatusPatch::for_provider(&provider, &store, polled),
            Some(DiscoveryStatusPatch {
                name: "p".to_owned(),
                failure_reason: None,
                models: None,
                expires_at: None,
            }),
            "disabling discovery must clear the served set"
        );
    }

    #[test]
    fn status_apply_carries_only_present_fields() {
        let with_both = DiscoveryStatusPatch {
            name: "p".to_owned(),
            failure_reason: Some("timeout"),
            models: Some(owned(&["a"])),
            expires_at: Some("2030-01-01T00:00:00Z".to_owned()),
        };
        assert_eq!(
            with_both.status_fields(),
            serde_json::json!({
                "modelDiscoveryError": "timeout",
                "servedModels": ["a"],
                "servedModelsExpiresAt": "2030-01-01T00:00:00Z"
            })
        );

        let cleared = DiscoveryStatusPatch {
            name: "p".to_owned(),
            failure_reason: None,
            models: None,
            expires_at: None,
        };
        assert_eq!(
            cleared.status_fields(),
            serde_json::json!({}),
            "omitted fields are removed by the apply"
        );
    }

    #[tokio::test]
    async fn stale_discovery_status_patch_is_rejected() {
        let api = conflicting_status_api("7");
        let mut provider = discovery_provider("p", &serde_json::json!({}));
        provider.metadata.resource_version = Some("7".to_owned());
        let desired = DiscoveryStatusPatch {
            name: "p".to_owned(),
            failure_reason: None,
            models: None,
            expires_at: None,
        };

        assert!(
            matches!(patch_discovery_status(&api, &provider, &desired).await, Err(error) if error.is_conflict()),
            "the stale apply must yield a conflict"
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
        let status = discovery_error.map_or_else(
            || serde_json::json!({}),
            |error| serde_json::json!({ "modelDiscoveryError": error }),
        );
        provider_from_json(name, &status, false)
    }

    fn test_provider_with_status(name: &str, status: &serde_json::Value) -> InferenceProvider {
        provider_from_json(name, status, false)
    }

    fn discovery_provider(name: &str, status: &serde_json::Value) -> InferenceProvider {
        provider_from_json(name, status, true)
    }

    fn provider_from_json(name: &str, status: &serde_json::Value, discovery: bool) -> InferenceProvider {
        let mut spec = serde_json::json!({
            "gridNetworkRef": "net",
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://provider",
            "models": [{ "name": "model-a" }]
        });
        if discovery && let Some(spec) = spec.as_object_mut() {
            spec.insert("modelDiscovery".to_owned(), serde_json::json!({ "openAiModels": {} }));
        }
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name, "uid": "test-provider-uid" },
            "spec": spec,
            "status": status.clone()
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
                        PollCompletedAt::now(),
                    ),
                    None => PollOutcome::Failed(PollError::Credential(String::new())),
                };
                ProviderPoll(
                    outcome,
                    provider.to_owned(),
                    DiscoveryKey {
                        uid: "test-provider-uid".to_owned(),
                        url: "http://provider/v1/models".to_owned(),
                    },
                )
            })
            .collect()
    }

    fn refresh(store: &ServedModelStore, updated: Vec<ProviderPoll>, ttl: Duration) {
        store.refresh(updated, ttl);
    }

    /// Mock a newer provider version rejecting the listed version's status apply.
    fn conflicting_status_api(listed_version: &'static str) -> Api<InferenceProvider> {
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| async move {
            use http_body_util::BodyExt as _;
            let body = request
                .into_body()
                .collect()
                .await
                .unwrap_or_else(|_| std::process::abort())
                .to_bytes();
            let apply: serde_json::Value = serde_json::from_slice(&body).unwrap_or_else(|_| std::process::abort());
            assert_eq!(
                apply
                    .pointer("/metadata/resourceVersion")
                    .and_then(serde_json::Value::as_str),
                Some(listed_version),
                "apply must use the listed version"
            );
            let conflict = serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure",
                "reason": "Conflict", "message": "resourceVersion changed", "code": 409
            });
            Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .status(409)
                    .body(kube::client::Body::from(conflict.to_string().into_bytes()))
                    .unwrap_or_else(|_| std::process::abort()),
            )
        });
        Api::all(Client::new(service, "default"))
    }

    /// Verify a failed poll cannot keep or publish another source's inventory.
    fn assert_failed_poll_clears_old_inventory(store: &ServedModelStore, provider: &InferenceProvider) {
        let source = DiscoveryKey::for_provider(provider).unwrap_or_else(|| std::process::abort());
        let failed_poll = ProviderPoll(
            PollOutcome::Failed(PollError::Credential(String::new())),
            "p".to_owned(),
            source,
        );
        refresh(store, vec![failed_poll], TTL);

        assert_eq!(
            store.models(provider),
            None,
            "changed source must not read the old inventory"
        );
        let failed = [ProviderPollStatus {
            name: "p".to_owned(),
            failure_reason: Some("credential"),
        }];
        assert_eq!(
            DiscoveryStatusPatch::for_provider(provider, store, &failed),
            Some(DiscoveryStatusPatch {
                name: "p".to_owned(),
                failure_reason: Some("credential"),
                models: None,
                expires_at: None,
            }),
            "a failed poll after a source change must clear old models and expiry"
        );
    }

    fn owned(names: &[&str]) -> Vec<String> {
        names.iter().map(|&n| n.to_owned()).collect()
    }

    fn stored_expiry(store: &ServedModelStore, provider: &str) -> Option<String> {
        store
            .fresh_inventory(&discovery_provider(provider, &serde_json::json!({})))
            .map(|(_, expires_at)| expires_at)
    }

    fn status_with_models(store: &ServedModelStore, models: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "servedModels": models,
            "servedModelsExpiresAt": stored_expiry(store, "p")
        })
    }

    fn names(store: &ServedModelStore, provider: &str) -> Option<Vec<String>> {
        store
            .models(&discovery_provider(provider, &serde_json::json!({})))
            .map(|models| {
                let mut names = models.to_vec();
                names.sort_unstable();
                names
            })
    }
}
