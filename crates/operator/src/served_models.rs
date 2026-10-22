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
//!       successful poll       → status.servedModels, expiry, and observed generation
//!       expired set           → inventory, expiry, and generation absent
//!       failure reason        → status.modelDiscoveryError
//! ```
//!
//! The in-memory store uses provider UID, generation, source URL, and a
//! monotonic TTL. The paired status expiry is a wall-clock boundary so readers
//! can reject a status that remains uncleared between expiry and the next
//! discovery round. The model set, expiry, and observed generation are the
//! durable hand-off to reconcile.

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
        model_discovery::{self, DiscoveryError, ModelSource as _, OpenAiModels, ServedModels},
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

/// Model names configured directly in a provider spec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StaticModelInventory {
    /// Configured model names.
    models: Arc<[String]>,
}

impl StaticModelInventory {
    /// Build the inventory declared in a provider's spec.
    fn for_provider(provider: &InferenceProvider) -> Self {
        Self {
            models: provider.spec.models.iter().map(|model| model.name.clone()).collect(),
        }
    }
}

/// Fresh discovered model names and the metadata that makes them valid.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DiscoveredModelInventory {
    /// Validated model names.
    models: Arc<[String]>,

    /// RFC 3339 UTC deadline persisted with the model set.
    expires_at: String,

    /// Provider generation that produced this inventory.
    observed_generation: i64,
}

/// Model names selected for an inference provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ModelInventory {
    /// Names configured directly on the provider.
    Static(StaticModelInventory),

    /// Names discovered from the provider backend.
    Discovered(DiscoveredModelInventory),
}

impl ModelInventory {
    /// Model names selected for routing or provider state.
    #[must_use]
    pub(crate) fn models(&self) -> &[String] {
        match self {
            Self::Static(inventory) => inventory.models.as_ref(),
            Self::Discovered(inventory) => inventory.models.as_ref(),
        }
    }

    /// Whether the names came from the provider's static configuration.
    #[must_use]
    pub(crate) fn is_static(&self) -> bool {
        matches!(self, Self::Static(_))
    }
}

/// Identity of the provider and concrete endpoint used for discovery.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DiscoveryKey {
    /// Kubernetes UID distinguishes replacements with the same name.
    uid: String,

    /// Provider spec generation that produced the inventory.
    generation: i64,

    /// URL includes the configured endpoint fallback and models path.
    url: String,
}

impl DiscoveryKey {
    /// Resolve the identity for a discoverable provider.
    fn for_provider(provider: &InferenceProvider) -> Option<Self> {
        let uid = provider.metadata.uid.clone()?;
        let generation = provider.metadata.generation?;
        let url = match provider.spec.model_discovery.as_ref()? {
            ModelDiscoveryConfig::OpenAiModels(source) => source.effective_url(&provider.spec.endpoint),
        };
        Some(Self { uid, generation, url })
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

    /// Select the model inventory configured for `provider`.
    ///
    /// Static providers always have an inventory. Dynamic providers never fall
    /// back to `spec.models` and have no inventory while discovery is unknown or expired.
    #[must_use]
    pub(crate) fn select_for(&self, provider: &InferenceProvider) -> Option<ModelInventory> {
        if provider.spec.model_discovery.is_some() {
            self.fresh_inventory(provider).map(ModelInventory::Discovered)
        } else {
            Some(ModelInventory::Static(StaticModelInventory::for_provider(provider)))
        }
    }

    /// Fresh models, expiry deadline, and producing generation for one provider.
    fn fresh_inventory(&self, provider: &InferenceProvider) -> Option<DiscoveredModelInventory> {
        let name = provider.metadata.name.as_deref()?;
        let source = DiscoveryKey::for_provider(provider)?;
        let now = Instant::now();
        let guard = self.inner.read().ok()?;
        guard
            .get(name)
            .filter(|held| held.source == source && held.expires_at > now)
            .map(|held| DiscoveredModelInventory {
                models: Arc::clone(&held.models),
                expires_at: held.expires_at_status.clone(),
                observed_generation: held.source.generation,
            })
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
    publish_discovery_status(&api, &providers, store, &status_updates, config.interval).await;
    Ok(())
}

/// Patch changed discovery-owned status: the failure reason and served inventory.
///
/// `store` is read after this round's refresh, so an expired or dropped set
/// reads as absent and is cleared from the status.
async fn publish_discovery_status(
    api: &Api<InferenceProvider>,
    providers: &[InferenceProvider],
    store: &ServedModelStore,
    polled: &[ProviderPollStatus],
    interval: Duration,
) {
    for provider in providers {
        let name = provider.metadata.name.as_deref();
        let failure_reason = name.and_then(|name| {
            polled
                .iter()
                .find(|status| status.name == name)
                .and_then(|status| status.failure_reason)
        });
        let inventory = store.fresh_inventory(provider);
        if let Some(desired) = DiscoveryStatusPatch::for_provider(provider, inventory, failure_reason) {
            let current = provider.status.as_ref();
            if desired.should_apply_patch(current, interval) {
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
}

/// The discovery-owned status desired for one provider.
#[derive(Debug, PartialEq)]
struct DiscoveryStatusPatch {
    /// `InferenceProvider` name.
    name: String,

    /// Failure reason, or `None` after a successful poll.
    failure_reason: Option<&'static str>,

    /// Fresh served-model inventory, or `None` when unknown, expired, or disabled.
    inventory: Option<DiscoveredModelInventory>,
}

impl DiscoveryStatusPatch {
    /// Decide whether one provider's discovery status needs a patch.
    ///
    /// `None` when the current status already shows the desired failure reason
    /// and inventory. An absent inventory is the fail-closed state for a
    /// provider whose discovery is disabled, never succeeded, or expired. An
    /// empty model list is present and distinct from absence.
    fn for_provider(
        provider: &InferenceProvider,
        inventory: Option<DiscoveredModelInventory>,
        failure_reason: Option<&'static str>,
    ) -> Option<Self> {
        let name = provider.metadata.name.as_deref()?;
        let desired = DiscoveryStatusPatch {
            name: name.to_owned(),
            failure_reason,
            inventory,
        };
        let current = provider.status.as_ref();
        if desired.has_same_error(current) && desired.has_same_inventory(current) {
            return None;
        }
        Some(desired)
    }

    /// Whether `current` shows exactly this patch's served-model set.
    ///
    /// Model lists are compared as sets: poll response order is not stable,
    /// so a reordered set is not a change. The desired list is validated and
    /// duplicate-free, so equal lengths plus membership checks also detect
    /// extra entries in the current status.
    fn has_same_models(&self, current: Option<&InferenceProviderStatus>) -> bool {
        let current_models = current.and_then(|status| status.served_models.as_deref());
        match (current_models, self.inventory.as_ref()) {
            (None, None) => true,
            (Some(current_names), Some(inventory)) => {
                current_names.len() == inventory.models.len()
                    && inventory.models.iter().all(|name| current_names.contains(name))
            },
            _ => false,
        }
    }

    /// Whether `current` shows the generation associated with this inventory.
    fn has_same_generation(&self, current: Option<&InferenceProviderStatus>) -> bool {
        let current_generation = current.and_then(|status| status.served_models_observed_generation);
        let desired_generation = self.inventory.as_ref().map(|inventory| inventory.observed_generation);
        current_generation == desired_generation
    }

    /// Whether `current` shows exactly this patch's served-model inventory.
    ///
    /// The model set, expiry, and observed generation must all match.
    fn has_same_inventory(&self, current: Option<&InferenceProviderStatus>) -> bool {
        let expiry_matches = current.and_then(|status| status.served_models_expires_at.as_deref())
            == self.inventory.as_ref().map(|inventory| inventory.expires_at.as_str());
        self.has_same_models(current) && expiry_matches && self.has_same_generation(current)
    }

    /// Whether `current` shows exactly this patch's failure reason.
    fn has_same_error(&self, current: Option<&InferenceProviderStatus>) -> bool {
        let current_error = current.and_then(|status| status.model_discovery_error.as_deref());
        current_error == self.failure_reason
    }

    /// Whether to apply this desired status patch now.
    ///
    /// Changed fields and expiry clearing always apply. Expiry-only renewals
    /// are skipped while the persisted boundary remains more than one interval
    /// away. Missing or invalid persisted expiries need a patch.
    fn should_apply_patch(&self, current: Option<&InferenceProviderStatus>, interval: Duration) -> bool {
        if self.inventory.is_none()
            || !self.has_same_error(current)
            || !self.has_same_models(current)
            || !self.has_same_generation(current)
        {
            return true;
        }
        let Some(expiration) = current.and_then(|status| status.served_models_expires_at.as_deref()) else {
            return true;
        };
        let Ok(expiration) = time::OffsetDateTime::parse(expiration, &time::format_description::well_known::Rfc3339)
        else {
            return true;
        };
        let Ok(interval) = time::Duration::try_from(interval) else {
            return true;
        };
        let Some(renew_after) = time::OffsetDateTime::now_utc().checked_add(interval) else {
            return true;
        };
        expiration <= renew_after
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
        if let Some(inventory) = &self.inventory {
            status.insert("servedModels".to_owned(), serde_json::json!(inventory.models.as_ref()));
            status.insert(
                "servedModelsExpiresAt".to_owned(),
                serde_json::json!(inventory.expires_at),
            );
            status.insert(
                "servedModelsObservedGeneration".to_owned(),
                serde_json::json!(inventory.observed_generation),
            );
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
    tokio::time::timeout(timeout, async {
        let source = openai_source(provider, openai, client, timeout).await?;
        Ok(source.served_models().await?)
    })
    .await
    .map_err(|_elapsed| PollError::Source(DiscoveryError::Timeout(timeout)))?
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
            Box::pin(KubernetesSecretResolver::new(client.clone()).resolve(&secret_ref))
                .await
                .map_err(|e| PollError::Credential(e.to_string()))?,
        ),
        Ok(CredentialPlan::Absent | CredentialPlan::Manual) => None,
        Err(e) => return Err(PollError::Credential(e.to_string())),
    };

    let ep_tls = openai.tls.as_ref().or(provider.spec.tls.as_ref());
    let client_tls = Box::pin(endpoint_tls::resolve_tls_config(ep_tls, Some(client), name))
        .await
        .map_err(|(_, message)| PollError::Tls(message))?;

    let uri = model_discovery::validated_uri(&url)?;
    let model_client = model_discovery::build_client(&uri, client_tls.as_ref(), timeout).await?;
    Ok(OpenAiModels::new(
        uri,
        token.as_ref(),
        client_tls.as_ref(),
        model_client,
        timeout,
    )?)
}

/// Build and query a model-discovery source backed by a local test server.
#[cfg(test)]
async fn query_openai_for_test(
    provider: &InferenceProvider,
    openai: &OpenAiModelsSource,
    client: &Client,
    timeout: Duration,
) -> Result<ServedModels, PollError> {
    let source = openai_source_for_test(provider, openai, client, timeout).await?;
    Ok(source.served_models().await?)
}

/// Build a test model-discovery source without shipping a loopback bypass.
#[cfg(test)]
async fn openai_source_for_test(
    provider: &InferenceProvider,
    openai: &OpenAiModelsSource,
    client: &Client,
    timeout: Duration,
) -> Result<OpenAiModels, PollError> {
    let name = provider.metadata.name.as_deref().unwrap_or("?");
    let url = openai.effective_url(&provider.spec.endpoint);

    let token = match credentials::credential_plan_from_auth(provider.spec.auth.as_ref()) {
        Ok(CredentialPlan::Bearer(secret_ref)) => Some(
            Box::pin(KubernetesSecretResolver::new(client.clone()).resolve(&secret_ref))
                .await
                .map_err(|e| PollError::Credential(e.to_string()))?,
        ),
        Ok(CredentialPlan::Absent | CredentialPlan::Manual) => None,
        Err(e) => return Err(PollError::Credential(e.to_string())),
    };

    let ep_tls = openai.tls.as_ref().or(provider.spec.tls.as_ref());
    let client_tls = Box::pin(endpoint_tls::resolve_tls_config(ep_tls, Some(client), name))
        .await
        .map_err(|(_, message)| PollError::Tls(message))?;
    Ok(model_discovery::new_for_test(&url, token.as_ref(), client_tls.as_ref(), timeout).await?)
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
    fn static_provider_selection_uses_configured_models() {
        let provider = test_provider_with_status("p", &serde_json::json!({}));
        let store = ServedModelStore::new();
        let selection = store.select_for(&provider);
        let selected_names = selection.as_ref().map(|inventory| inventory.models().to_vec());

        assert_eq!(
            selected_names,
            Some(owned(&["model-a"])),
            "static providers use configured names"
        );
        assert!(
            selection.as_ref().is_some_and(ModelInventory::is_static),
            "static names use the static inventory variant"
        );
    }

    #[test]
    fn dynamic_provider_selection_uses_only_fresh_inventory() {
        let store = ServedModelStore::new();
        let provider = discovery_provider("p", &serde_json::json!({}));

        let unknown = store.select_for(&provider);
        assert!(
            unknown.is_none(),
            "unknown dynamic inventory must not fall back to spec.models"
        );

        refresh(&store, round(&[("p", Some(&["discovered"]))]), TTL);
        let fresh = store.select_for(&provider);
        assert!(
            matches!(
                &fresh,
                Some(ModelInventory::Discovered(inventory))
                    if inventory.models.as_ref() == [String::from("discovered")]
            ),
            "fresh inventory is selected without using configured names"
        );
        assert!(
            fresh.as_ref().is_some_and(|inventory| !inventory.is_static()),
            "fresh inventory uses the discovered variant"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one fixture verifies discovered inventory is excluded from routing and gossip"
    )]
    fn fresh_discovered_inventory_is_not_routed_or_gossiped() {
        let provider = discovery_provider("p", &serde_json::json!({}));
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["discovered"]))]), TTL);
        let inventory = store.select_for(&provider).unwrap_or_else(|| std::process::abort());
        assert!(!inventory.is_static(), "the fixture must hold discovered inventory");
        assert_eq!(
            inventory.models(),
            &["discovered".to_owned()],
            "the fixture must expose the discovered model"
        );
        let network: crate::crd::grid_network::GridNetwork = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "net" },
            "spec": { "seeds": [], "gridId": "test-id" }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let overlay = crate::resources::routing_overlay::render_routing_overlay_with_admission(
            &network,
            &[],
            std::slice::from_ref(&provider),
            &[],
            &[],
            "site-a",
            None,
            None,
            None,
            &scoring::ScoringWeights::default(),
            None,
            &store,
        )
        .unwrap_or_else(|_| std::process::abort());
        let state = crate::controller::grid_network::provider_state_from_kube(&provider, "net", "site-a", None, &store)
            .unwrap_or_else(|| std::process::abort());

        assert!(
            overlay.candidates.is_empty(),
            "fresh discovered models must not produce routing candidates"
        );
        assert!(state.models.is_empty(), "fresh discovered models must not be gossiped");
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
        assert_eq!(
            patch_for_provider(&provider_without_status, &store, Some("credential")),
            Some(DiscoveryStatusPatch {
                name: provider_name.to_owned(),
                failure_reason: Some("credential"),
                inventory: None,
            }),
            "a failed poll should set its reason"
        );
        assert_eq!(
            patch_for_provider(&provider_with_error, &store, Some("credential")),
            None,
            "an unchanged error should not be patched"
        );
    }

    #[test]
    fn success_or_disabled_discovery_clears_a_previous_error() {
        let store = ServedModelStore::new();
        let provider_name = "provider-a";
        let provider_with_error = test_provider(provider_name, Some("credential"));
        assert_eq!(
            patch_for_provider(&provider_with_error, &store, None),
            Some(DiscoveryStatusPatch {
                name: provider_name.to_owned(),
                failure_reason: None,
                inventory: None,
            }),
            "a successful poll should clear the previous error"
        );
        assert_eq!(
            patch_for_provider(&provider_with_error, &store, None),
            Some(DiscoveryStatusPatch {
                name: provider_name.to_owned(),
                failure_reason: None,
                inventory: None,
            }),
            "removing model discovery should clear the previous error"
        );
    }

    #[test]
    fn success_writes_the_served_set() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["b", "a"]))]), TTL);

        let provider = &discovery_provider("p", &serde_json::json!({}));
        let patch = patch_for_provider(provider, &store, None);

        assert_eq!(
            patch,
            Some(DiscoveryStatusPatch {
                name: "p".to_owned(),
                failure_reason: None,
                inventory: store.fresh_inventory(provider),
            }),
            "a changed set should be written as polled"
        );
    }

    #[test]
    fn an_empty_set_is_written_not_cleared() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&[]))]), TTL);

        let provider = &discovery_provider("p", &serde_json::json!({}));
        let patch = patch_for_provider(provider, &store, None);

        assert_eq!(
            patch
                .and_then(|desired| desired.inventory)
                .map(|inventory| inventory.models.to_vec()),
            Some(Vec::new()),
            "an empty discovered set is present, distinct from absence"
        );
    }

    #[test]
    fn unchanged_status_is_not_patched() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["a"]))]), TTL);

        let provider = discovery_provider("p", &status_with_models(&store, &["a"]));

        assert_eq!(
            patch_for_provider(&provider, &store, None),
            None,
            "an unchanged set should not be patched"
        );
    }

    #[test]
    fn expiry_only_renewal_is_skipped_when_persisted_deadline_is_far() {
        let provider = discovery_provider(
            "p",
            &serde_json::json!({
                "servedModels": ["a"],
                "servedModelsExpiresAt": expiry_after(Duration::from_secs(3600)),
                "servedModelsObservedGeneration": 1
            }),
        );
        let desired = DiscoveryStatusPatch {
            name: "p".to_owned(),
            failure_reason: None,
            inventory: Some(DiscoveredModelInventory {
                models: owned(&["a"]).into(),
                expires_at: expiry_after(Duration::from_secs(7200)),
                observed_generation: 1,
            }),
        };

        assert!(
            !desired.should_apply_patch(provider.status.as_ref(), Duration::from_secs(60)),
            "an expiry-only renewal should be skipped while the current boundary is far away"
        );
    }

    #[test]
    fn expiry_renewal_is_applied_when_persisted_deadline_is_near() {
        let provider = discovery_provider(
            "p",
            &serde_json::json!({
                "servedModels": ["a"],
                "servedModelsExpiresAt": expiry_after(Duration::from_secs(30)),
                "servedModelsObservedGeneration": 1
            }),
        );
        let desired = DiscoveryStatusPatch {
            name: "p".to_owned(),
            failure_reason: None,
            inventory: Some(DiscoveredModelInventory {
                models: owned(&["a"]).into(),
                expires_at: expiry_after(Duration::from_secs(3600)),
                observed_generation: 1,
            }),
        };

        assert!(
            desired.should_apply_patch(provider.status.as_ref(), Duration::from_secs(60)),
            "an expiry-only renewal should be applied within one discovery interval"
        );
    }

    #[test]
    fn changed_models_are_applied_even_when_persisted_deadline_is_far() {
        let provider = discovery_provider(
            "p",
            &serde_json::json!({
                "servedModels": ["a"],
                "servedModelsExpiresAt": expiry_after(Duration::from_secs(3600))
            }),
        );
        let desired = DiscoveryStatusPatch {
            name: "p".to_owned(),
            failure_reason: None,
            inventory: Some(DiscoveredModelInventory {
                models: owned(&["b"]).into(),
                expires_at: expiry_after(Duration::from_secs(7200)),
                observed_generation: 1,
            }),
        };

        assert!(
            desired.should_apply_patch(provider.status.as_ref(), Duration::from_secs(60)),
            "changed model fields should be applied regardless of expiry freshness"
        );
    }

    #[test]
    fn changed_error_is_applied_even_when_persisted_deadline_is_far() {
        let provider = discovery_provider(
            "p",
            &serde_json::json!({
                "modelDiscoveryError": "unreachable",
                "servedModels": ["a"],
                "servedModelsExpiresAt": expiry_after(Duration::from_secs(3600)),
                "servedModelsObservedGeneration": 1
            }),
        );
        let desired = DiscoveryStatusPatch {
            name: "p".to_owned(),
            failure_reason: None,
            inventory: Some(DiscoveredModelInventory {
                models: owned(&["a"]).into(),
                expires_at: expiry_after(Duration::from_secs(7200)),
                observed_generation: 1,
            }),
        };

        assert!(
            desired.should_apply_patch(provider.status.as_ref(), Duration::from_secs(60)),
            "changed failure fields should be applied regardless of expiry freshness"
        );
    }

    #[test]
    fn expiry_clearing_is_applied_when_discovery_has_no_inventory() {
        let provider = discovery_provider(
            "p",
            &serde_json::json!({
                "servedModelsExpiresAt": expiry_after(Duration::from_secs(3600))
            }),
        );
        let desired = DiscoveryStatusPatch {
            name: "p".to_owned(),
            failure_reason: None,
            inventory: None,
        };

        assert!(
            desired.should_apply_patch(provider.status.as_ref(), Duration::from_secs(60)),
            "a stale expiry should be cleared when discovery has no inventory"
        );
    }

    #[test]
    fn a_reordered_set_is_not_a_change() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["a", "b"]))]), TTL);

        let provider = discovery_provider("p", &status_with_models(&store, &["b", "a"]));

        assert_eq!(
            patch_for_provider(&provider, &store, None),
            None,
            "response order is not part of the set"
        );
    }

    #[test]
    fn a_substituted_model_is_a_change() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["a", "b"]))]), TTL);

        let provider = discovery_provider("p", &serde_json::json!({ "servedModels": ["a", "c"] }));

        assert_eq!(
            patch_for_provider(&provider, &store, None),
            Some(DiscoveryStatusPatch {
                name: "p".to_owned(),
                failure_reason: None,
                inventory: store.fresh_inventory(&provider),
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
        assert_eq!(
            patch_for_provider(&provider, &store, Some("unreachable")),
            Some(DiscoveryStatusPatch {
                name: "p".to_owned(),
                failure_reason: Some("unreachable"),
                inventory: store.fresh_inventory(&provider),
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
    fn provider_generation_change_invalidates_held_inventory() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["a"]))]), TTL);
        let mut provider = discovery_provider("p", &status_with_models(&store, &["a"]));
        provider.metadata.generation = Some(2);

        assert!(
            store.select_for(&provider).is_none(),
            "an inventory from an earlier provider generation must not be selected"
        );
        assert_eq!(
            patch_for_provider(&provider, &store, None),
            Some(DiscoveryStatusPatch {
                name: "p".to_owned(),
                failure_reason: None,
                inventory: None,
            }),
            "the old inventory must be cleared from status"
        );
    }

    #[test]
    fn a_generation_mismatch_is_a_status_change() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["a"]))]), TTL);
        let expiry = stored_expiry(&store, "p").unwrap_or_else(|| std::process::abort());
        let provider = discovery_provider(
            "p",
            &serde_json::json!({
                "servedModels": ["a"],
                "servedModelsExpiresAt": expiry,
                "servedModelsObservedGeneration": 0
            }),
        );

        assert_eq!(
            patch_for_provider(&provider, &store, None),
            Some(DiscoveryStatusPatch {
                name: "p".to_owned(),
                failure_reason: None,
                inventory: store.fresh_inventory(&provider),
            }),
            "matching names and expiry still require the current provider generation"
        );
    }

    #[test]
    fn expired_set_is_cleared_from_status() {
        let store = ServedModelStore::new();
        refresh(&store, round(&[("p", Some(&["a"]))]), Duration::ZERO);

        let provider = discovery_provider("p", &serde_json::json!({ "servedModels": ["a"] }));
        assert_eq!(
            patch_for_provider(&provider, &store, Some("unreachable")),
            Some(DiscoveryStatusPatch {
                name: "p".to_owned(),
                failure_reason: Some("unreachable"),
                inventory: None,
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

        assert_eq!(
            patch_for_provider(&provider, &store, None),
            Some(DiscoveryStatusPatch {
                name: "p".to_owned(),
                failure_reason: None,
                inventory: None,
            }),
            "disabling discovery must clear the served set"
        );
    }

    #[test]
    fn status_apply_carries_only_present_fields() {
        let with_both = DiscoveryStatusPatch {
            name: "p".to_owned(),
            failure_reason: Some("timeout"),
            inventory: Some(DiscoveredModelInventory {
                models: owned(&["a"]).into(),
                expires_at: "2030-01-01T00:00:00Z".to_owned(),
                observed_generation: 1,
            }),
        };
        assert_eq!(
            with_both.status_fields(),
            serde_json::json!({
                "modelDiscoveryError": "timeout",
                "servedModels": ["a"],
                "servedModelsExpiresAt": "2030-01-01T00:00:00Z",
                "servedModelsObservedGeneration": 1
            }),
            "present fields are carried by the apply"
        );

        let cleared = DiscoveryStatusPatch {
            name: "p".to_owned(),
            failure_reason: None,
            inventory: None,
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
            inventory: None,
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

        let models = query_openai_for_test(&provider, &openai_source(None, None), &client, Duration::from_secs(5))
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

        let models = query_openai_for_test(
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

        let result =
            query_openai_for_test(&provider, &openai_source(None, None), &client, Duration::from_secs(5)).await;

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
            "metadata": { "name": name, "uid": "test-provider-uid", "generation": 1 },
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
                        generation: 1,
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
            store.select_for(provider).map(|inventory| inventory.models().to_vec()),
            None,
            "changed source must not read the old inventory"
        );
        assert_eq!(
            patch_for_provider(provider, store, Some("credential")),
            Some(DiscoveryStatusPatch {
                name: "p".to_owned(),
                failure_reason: Some("credential"),
                inventory: None,
            }),
            "a failed poll after a source change must clear old models and expiry"
        );
    }

    /// Build a desired status patch from the store's current inventory.
    fn patch_for_provider(
        provider: &InferenceProvider,
        store: &ServedModelStore,
        failure_reason: Option<&'static str>,
    ) -> Option<DiscoveryStatusPatch> {
        DiscoveryStatusPatch::for_provider(provider, store.fresh_inventory(provider), failure_reason)
    }

    fn owned(names: &[&str]) -> Vec<String> {
        names.iter().map(|&n| n.to_owned()).collect()
    }

    fn stored_expiry(store: &ServedModelStore, provider: &str) -> Option<String> {
        store
            .fresh_inventory(&discovery_provider(provider, &serde_json::json!({})))
            .map(|inventory| inventory.expires_at)
    }

    fn expiry_after(duration: Duration) -> String {
        let Ok(duration) = time::Duration::try_from(duration) else {
            std::process::abort();
        };
        let Some(expiration) = time::OffsetDateTime::now_utc().checked_add(duration) else {
            std::process::abort();
        };
        expiration
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| std::process::abort())
    }

    fn status_with_models(store: &ServedModelStore, models: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "servedModels": models,
            "servedModelsExpiresAt": stored_expiry(store, "p"),
            "servedModelsObservedGeneration": 1
        })
    }

    fn names(store: &ServedModelStore, provider: &str) -> Option<Vec<String>> {
        store
            .select_for(&discovery_provider(provider, &serde_json::json!({})))
            .map(|inventory| {
                let mut names = inventory.models().to_vec();
                names.sort_unstable();
                names
            })
    }
}
