//! Pure overlay renderer for Praxis `grid_route` routing candidates.
//!
//! Converts [`GridNetwork`], [`GridSite`], and [`InferenceProvider`]
//! CRDs into a `RoutingOverlay` that is serialised into a Kubernetes
//! `ConfigMap`.  Praxis reads this `ConfigMap` to configure its
//! `grid_route` filter with routing candidates.
//!
//! This renderer is **pure**: it accepts already-fetched CRD data and
//! produces structured output.  No Kubernetes API calls are made inside
//! this module.
//!
//! # Phase 1 / OP-01 semantics
//!
//! - [`GridSite`]s are used to resolve per-provider site membership via `spec.siteSelector.matchLabels`.  An empty
//!   selector matches all sites in the same [`GridNetwork`].
//! - Each `(model, site)` pair becomes one `RoutingCandidate`.
//! - `candidate.site` = the [`GridSite`] name (resolved via selector).
//! - `candidate.cluster` = `spec.routingClusterRef` when set, otherwise the [`InferenceProvider`] metadata name. The
//!   gateway uses this as the upstream cluster reference in its local routing configuration.
//! - When no [`GridSite`]s are provided, the routing identity (`spec.routingClusterRef` or provider name) is used as
//!   both `site` and `cluster` (Phase 1 self-hosted fallback).
//!
//! # Spec-based vs status-based site derivation
//!
//! The renderer derives `candidate.site` from `spec.siteSelector.matchLabels`
//! against live [`GridSite`] data, **not** from
//! `status.matchingSites`.  `status.matchingSites` is set asynchronously by
//! the OP-02 `InferenceProvider` controller and may be stale if sites or
//! labels changed since the last `InferenceProvider` reconcile.  Re-deriving
//! from spec guarantees freshness relative to the current `GridNetwork`
//! reconcile cycle.  `status.matchingSites` exists for human observability
//! and external tools, not as the overlay's authoritative input.
//!
//! # ConfigMap contract
//!
//! - Name: `grid-overlay-{network}-{gateway}` (≤ 63 chars). Long names receive a deterministic FNV-1a hash suffix to
//!   avoid collisions.
//! - Data key: `grid-config.json`
//! - Serialization failures are returned as errors, not silently defaulted.
//!
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork
//! [`GridSite`]: crate::crd::grid_site::GridSite
//! [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider

use std::collections::{BTreeMap, BTreeSet, HashMap};

use k8s_openapi::api::core::v1::ConfigMap;
use serde::{Deserialize, Serialize};

use crate::crd::{
    grid_network::GridNetwork,
    grid_site::GridSite,
    inference_provider::{InferenceProvider, ProviderPhase},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum length for a Kubernetes resource name (DNS label limit).
const MAX_K8S_NAME: usize = 63;

/// Maximum prefix length for each component when a hash suffix is needed.
const MAX_COMPONENT_PREFIX: usize = 20;

/// Candidate kind identifier for inference model entries.
const CANDIDATE_KIND: &str = "inference_model";

/// Fallback locality score when `backend_kind` is absent or unrecognised.
///
/// Matches `scoring::DEFAULT_SIGNAL_SCORE` (0.5) to keep the default
/// consistent with the scoring crate's unknown-metric handling.
const DEFAULT_LOCALITY: f64 = 0.5;

// ---------------------------------------------------------------------------
// Locality scoring
// ---------------------------------------------------------------------------

/// Derive the locality score for an [`InferenceProvider`] from its
/// `spec.backendKind` string.
///
/// Parses the `backend_kind` value as a [`scoring::BackendKind`] and
/// delegates to [`scoring::locality_score`] with no region context
/// (`None, None`).  Unrecognised kinds default to
/// [`DEFAULT_LOCALITY`] (0.5).
///
/// | `backend_kind` | Score |
/// |----------------|-------|
/// | `"local"` | 1.0 |
/// | `"remote"` | 0.5 (no region context) |
/// | `"cloud_managed"` | 0.2 |
/// | `"api_provider"` | 0.1 |
/// | unknown | 0.5 |
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
pub(crate) fn backend_locality_score(backend_kind: &str) -> f64 {
    let kind: Option<scoring::BackendKind> =
        serde_json::from_value(serde_json::Value::String(backend_kind.to_owned())).ok();
    kind.map_or(DEFAULT_LOCALITY, |k| scoring::locality_score(k, None, None))
}

/// Return the routing identity for a provider.
///
/// When `spec.routingClusterRef` is set and non-empty, returns that value;
/// otherwise falls back to `metadata.name`.  This name is used as
/// `candidate.cluster` (and as `candidate.site` in Phase 1 when no
/// [`GridSite`]s are configured), and as the [`scoring::BackendConfig`] name
/// so that score lookups by candidate cluster resolve correctly.
///
/// [`GridSite`]: crate::crd::grid_site::GridSite
pub(crate) fn routing_identity(provider: &InferenceProvider) -> Option<&str> {
    if let Some(r) = &provider.spec.routing_cluster_ref
        && !r.trim().is_empty()
    {
        return Some(r.as_str());
    }
    provider.metadata.name.as_deref()
}

/// Map an [`InferenceProvider`] to a [`scoring::BackendConfig`] for use with
/// [`scoring::score_backends`].
///
/// Returns `None` when:
/// - The provider has no `metadata.name` and no `spec.routingClusterRef`.
/// - `spec.backendKind` does not match any [`scoring::BackendKind`] variant (locality is the primary scoring signal;
///   unknown kinds cannot be ranked).
///
/// The `BackendConfig` name is [`routing_identity`] — `spec.routingClusterRef`
/// if set, otherwise `metadata.name`.  Using the routing identity here ensures
/// that score lookups in `render_routing_overlay` (which key on
/// `candidate.cluster`, not `metadata.name`) resolve correctly.
///
/// `spec.providerKind` is stored as metadata in [`scoring::BackendConfig`] but
/// is not used by the scoring formula.  Unknown values (including `"self_hosted"`
/// for vLLM / llm-d servers, which serve the OpenAI-compatible API) default to
/// [`scoring::ProviderKind::OpenAi`] so that self-hosted providers are not
/// excluded from scoring.
///
/// Cost is converted from per-million-tokens (CRD unit) to per-1k-tokens
/// (scoring-crate unit) by dividing by 1 000.  Missing cost is treated as 0.0
/// (free), which yields the maximum cost score of 1.0.
///
/// Provider region is always `None` in this implementation: the
/// [`InferenceProvider`] CRD carries no region field.  Pass the network's own
/// region as `local_region` to [`scoring::score_backends`] to benefit from
/// same-region preference for remote providers if per-provider regions become
/// available in a future CRD revision.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
pub(crate) fn provider_to_backend_config(provider: &InferenceProvider) -> Option<scoring::BackendConfig> {
    let name = routing_identity(provider)?.to_owned();
    let kind: scoring::BackendKind =
        serde_json::from_value(serde_json::Value::String(provider.spec.backend_kind.clone())).ok()?;
    let provider_kind: scoring::ProviderKind =
        serde_json::from_value(serde_json::Value::String(provider.spec.provider_kind.clone()))
            .unwrap_or(scoring::ProviderKind::OpenAi);
    let cost_per_1k_input = provider
        .spec
        .cost
        .as_ref()
        .map_or(0.0, |c| c.per_million_input_tokens / 1_000.0);
    let cost_per_1k_output = provider
        .spec
        .cost
        .as_ref()
        .map_or(0.0, |c| c.per_million_output_tokens / 1_000.0);
    Some(scoring::BackendConfig::new(
        name,
        cost_per_1k_input,
        cost_per_1k_output,
        provider.spec.endpoint.clone(),
        kind,
        provider_kind,
        None, // provider region: not in CRD; see doc for future work note
    ))
}

/// Neutral signal score when no live metrics are available; matches
/// `scoring::DEFAULT_SIGNAL_SCORE` which is not exported from the scoring crate.
const UNMAPPED_NEUTRAL_SIGNAL: f64 = 0.5;

/// Compute the score equivalent to what [`scoring::score_backends`] would
/// assign to a provider whose `backend_kind` cannot be parsed.
///
/// Applies [`scoring::ScoringWeights::default`] with neutral runtime signals
/// (0.5, the scoring crate's default for missing metrics) and no cost (treated
/// as free → cost signal = 1.0).  This places unmapped providers on the same
/// numeric scale as scored providers so they can be sorted in a single pass.
fn unmapped_provider_score(backend_kind: &str) -> f64 {
    let w = scoring::ScoringWeights::default();
    // 1.0: cost_score(0.0) — providers with no cost data are treated as free.
    w.locality * backend_locality_score(backend_kind)
        + w.cost * 1.0
        + (w.queue_depth + w.kv_cache + w.latency + w.prefix_cache) * UNMAPPED_NEUTRAL_SIGNAL
}

/// Compute per-provider ordering scores for overlay candidate sorting.
///
/// For each provider in `network_name` that can be mapped to a
/// [`scoring::BackendConfig`], the score is produced by
/// [`scoring::score_backends`] using [`scoring::ScoringWeights::default`]
/// and the network's `local_region`.  Providers whose `backend_kind` cannot
/// be parsed fall back to [`unmapped_provider_score`], which is on the same
/// numeric scale.
///
/// `Unavailable` providers are excluded (they are never emitted as candidates).
/// All other phases — `Pending`, `Available`, `Degraded`, and absent status —
/// are scored and included.  The `fresh` flag is set separately per candidate
/// by [`is_candidate_fresh`].
///
/// Returns a map from provider name to score, borrowing names from `providers`.
fn provider_ordering_scores<'a>(
    network_name: &str,
    providers: &'a [InferenceProvider],
    local_region: Option<&str>,
    metrics: Option<&HashMap<&str, scoring::BackendMetrics>>,
) -> HashMap<&'a str, f64> {
    let state = build_grid_state_with_metrics(network_name, providers, metrics);
    let scored = scoring::score_backends(&state, &scoring::ScoringWeights::default(), local_region);
    // `from_engine` is keyed by routing_identity (BackendConfig.name), which matches candidate.cluster.
    let from_engine: HashMap<String, f64> = scored.into_iter().map(|sb| (sb.name, sb.score)).collect();
    providers
        .iter()
        .filter_map(|p| {
            // Use routing_identity so the key matches candidate.cluster in the sort.
            let cluster = routing_identity(p)?;
            let score = from_engine
                .get(cluster)
                .copied()
                .unwrap_or_else(|| unmapped_provider_score(&p.spec.backend_kind));
            Some((cluster, score))
        })
        .collect()
}

/// Build a [`scoring::GridState`] from mappable providers, optionally populated
/// with scraped metrics.
///
/// Providers that are explicitly [`ProviderPhase::Unavailable`] or that belong
/// to a different network are skipped.  Duplicate provider names (a CRD-level
/// invariant violation) are silently ignored.
///
/// When `metrics` is `Some`, each provider whose name appears in the map
/// receives live [`scoring::BackendMetrics`] via
/// [`scoring::GridState::set_metrics`].  This is the integration seam for
/// Prometheus-scraped data — pass `None` for static-only scoring.
pub(crate) fn build_grid_state_with_metrics(
    network_name: &str,
    providers: &[InferenceProvider],
    metrics: Option<&HashMap<&str, scoring::BackendMetrics>>,
) -> scoring::GridState {
    let mut state = scoring::GridState::new();
    for provider in providers {
        if provider.spec.grid_network_ref != network_name || is_explicitly_unavailable(provider) {
            continue;
        }
        if let Some(config) = provider_to_backend_config(provider) {
            let name = config.name.clone();
            drop(state.add_backend(config));
            if let Some(m) = metrics.and_then(|map| map.get(name.as_str())).copied() {
                state.set_metrics(name, m);
            }
        }
    }
    state
}

// ---------------------------------------------------------------------------
// Site resolution
// ---------------------------------------------------------------------------

/// Resolution of matching sites for a single provider.
///
/// This enum distinguishes two semantically different "empty" cases so that
/// the candidate generator cannot accidentally apply the legacy fallback when
/// a real site inventory exists.
///
/// | Variant | Meaning | Candidate action |
/// |---------|---------|-----------------|
/// | `Unavailable` | No [`GridSite`] CRDs in the network | Use provider name as site (Phase 1 fallback) |
/// | `Known(empty)` | CRDs exist but selector matched none | Emit **no** candidates |
/// | `Known(names)` | Selector matched these sites | Emit one candidate per `(model, site)` |
///
/// [`GridSite`]: crate::crd::grid_site::GridSite
enum SiteResolution {
    /// No [`GridSite`] CRDs were supplied to the renderer for this network.
    ///
    /// The provider name is used as the site identity.  This is the Phase 1
    /// self-hosted fallback and should only be used when the cluster has no
    /// site inventory at all.
    ///
    /// [`GridSite`]: crate::crd::grid_site::GridSite
    Unavailable,

    /// Site CRDs are available.  Contains the names matched by the provider's
    /// `siteSelector`.
    ///
    /// An empty `Vec` means the selector matched no sites; the provider
    /// contributes no candidates to the overlay.
    Known(Vec<String>),
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A single routing candidate for the Praxis `grid_route` filter.
///
/// Each candidate represents one (model, site) pair offered by a provider.
/// Praxis uses the candidate list to select a backend cluster for each
/// inference request.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct RoutingCandidate {
    /// Candidate kind.  `"inference_model"` for inference providers; other
    /// variants (e.g. `"mcp_tool"`) are defined by Praxis `grid_route`.
    pub kind: String,

    /// Model name as declared in the [`InferenceProvider`] spec.
    ///
    /// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
    pub name: String,

    /// Site name where this model is hosted.
    ///
    /// Resolved via `spec.siteSelector.matchLabels` against [`GridSite`]
    /// metadata labels.  Falls back to the provider routing identity
    /// (`spec.routingClusterRef`, or provider metadata name when absent)
    /// when no [`GridSite`]s are passed (Phase 1 self-hosted fallback).
    ///
    /// [`GridSite`]: crate::crd::grid_site::GridSite
    pub site: String,

    /// Upstream cluster identifier.
    ///
    /// Uses `spec.routingClusterRef` when set and non-empty, otherwise the
    /// [`InferenceProvider`] metadata name.
    ///
    /// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
    pub cluster: String,

    /// Whether this candidate's data is considered fresh.
    ///
    /// Always `true` for the static overlay.  Freshness updates arrive
    /// in OP-05 (metrics-to-snapshot loop).
    pub fresh: bool,
}

/// The full routing overlay for a single [`GridNetwork`].
///
/// Serialised as JSON under the `grid-config.json` key of the
/// overlay `ConfigMap`.
///
/// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RoutingOverlay {
    /// Name of the [`GridNetwork`] this overlay belongs to.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub network: String,

    /// Local site identifier.
    ///
    /// Supplied per gateway by the controller as
    /// `gw_ref.local_site_name.as_deref().unwrap_or(network_name)`.
    /// Each `GatewayRef` may declare its own `localSiteName`, allowing
    /// multi-gateway networks to produce overlays with distinct
    /// `local_site` values.  Falls back to the network name for
    /// single-site networks.
    ///
    /// Praxis uses `local_site` to score candidates on the same site
    /// higher than remote candidates.
    pub local_site: String,

    /// Routing candidates, sorted by scoring-engine score (descending) then
    /// deterministically by site, name, and cluster.
    pub candidates: Vec<RoutingCandidate>,
}

// ---------------------------------------------------------------------------
// Renderer
// ---------------------------------------------------------------------------

/// Render a [`RoutingOverlay`] from CRD state.
///
/// Only [`InferenceProvider`]s whose `spec.gridNetworkRef` matches
/// `network.metadata.name` are included.  Each provider's
/// `spec.siteSelector.matchLabels` is matched against the supplied
/// `sites`; an empty selector matches all sites in the network.
///
/// The `local_site` parameter identifies this gateway's own site.
/// Praxis uses it to score candidates running on the local site higher
/// than remote candidates.  The caller is responsible for computing
/// `local_site` per gateway:
/// ```text
/// local_site = gw_ref.local_site_name.as_deref().unwrap_or(network_name)
/// ```
///
/// Candidates are sorted by the scoring engine (descending score) then
/// `(site, name, cluster)` for ties.  Scores are computed by
/// [`scoring::score_backends`] using [`scoring::ScoringWeights::default`]
/// and the network's `spec.region` as the locality context.  With no live
/// metrics, the ordering reduces to locality (from `spec.backendKind`) and
/// cost (from `spec.cost`):
/// `local` (1.0) → `remote` (0.5) → `cloud_managed` (0.2) → `api_provider` (0.1).
/// Providers with lower-cost configurations rank ahead of equal-locality
/// peers that have higher cost.  Providers whose `backend_kind` cannot be
/// parsed fall back to an equivalent same-scale locality estimate.
///
/// Exact duplicates — same `(kind, name, site, cluster)` — are removed.
/// Two providers that serve the same model on the same site but with
/// different cluster identifiers are **not** deduplicated.
///
/// # Errors
///
/// Returns a descriptive `String` if:
/// - The network resource has no metadata name.
/// - Any eligible provider has no metadata name.
/// - Any model name in an eligible provider is blank or whitespace-only.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
pub fn render_routing_overlay(
    network: &GridNetwork,
    sites: &[GridSite],
    providers: &[InferenceProvider],
    local_site: &str,
) -> Result<RoutingOverlay, String> {
    let network_name = network
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| "GridNetwork has no name".to_owned())?;

    let ordering = provider_ordering_scores(network_name, providers, network.spec.region.as_deref(), None);

    let mut candidates = collect_candidates(network_name, sites, providers)?;
    candidates.sort_by(|a, b| {
        let score_a = ordering.get(a.cluster.as_str()).copied().unwrap_or(DEFAULT_LOCALITY);
        let score_b = ordering.get(b.cluster.as_str()).copied().unwrap_or(DEFAULT_LOCALITY);
        // Higher score first (descending), then stable alphabetical tiebreak.
        score_b
            .total_cmp(&score_a)
            .then(a.site.cmp(&b.site))
            .then(a.name.cmp(&b.name))
            .then(a.cluster.cmp(&b.cluster))
    });
    candidates.dedup_by(|a, b| a.kind == b.kind && a.name == b.name && a.site == b.site && a.cluster == b.cluster);

    Ok(RoutingOverlay {
        network: network_name.to_owned(),
        local_site: local_site.to_owned(),
        candidates,
    })
}

/// Collect [`RoutingCandidate`]s from providers belonging to `network_name`.
///
/// Providers explicitly marked [`ProviderPhase::Unavailable`] in their status
/// are excluded.  Providers in any other phase (`Pending`, `Available`,
/// `Degraded`, or absent status) are included.  See [`is_explicitly_unavailable`]
/// for the rationale.
fn collect_candidates(
    network_name: &str,
    sites: &[GridSite],
    providers: &[InferenceProvider],
) -> Result<Vec<RoutingCandidate>, String> {
    // Pre-filter sites to those in this network.
    let network_sites: Vec<&GridSite> = sites
        .iter()
        .filter(|s| s.spec.grid_network_ref == network_name)
        .collect();

    let mut all: Vec<RoutingCandidate> = Vec::new();
    for provider in providers {
        if provider.spec.grid_network_ref != network_name {
            continue;
        }
        if is_explicitly_unavailable(provider) {
            continue;
        }
        let resolution = resolve_sites(provider, &network_sites);
        all.extend(candidates_from_provider(provider, &resolution)?);
    }
    Ok(all)
}

/// Resolve matching sites for a provider against the network site inventory.
///
/// Returns [`SiteResolution::Unavailable`] when no site inventory exists,
/// which enables the Phase 1 provider-name fallback.  Returns
/// [`SiteResolution::Known`] otherwise — with an empty `Vec` if the
/// selector matched nothing, which suppresses candidate generation.
fn resolve_sites(provider: &InferenceProvider, network_sites: &[&GridSite]) -> SiteResolution {
    if network_sites.is_empty() {
        return SiteResolution::Unavailable;
    }

    let selector = &provider.spec.site_selector.match_labels;

    let names: Vec<String> = network_sites
        .iter()
        .filter(|site| {
            let site_labels = site.metadata.labels.as_ref();
            selector
                .iter()
                .all(|(k, v)| site_labels.is_some_and(|labels| labels.get(k).is_some_and(|sv| sv == v)))
        })
        .map(|site| site.metadata.name.clone().unwrap_or_else(|| "unknown-site".to_owned()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    SiteResolution::Known(names)
}

/// Build one [`RoutingCandidate`] per `(model, site)` for a single provider.
///
/// The `site_resolution` parameter determines which sites this provider serves:
///
/// - [`SiteResolution::Unavailable`]: no site inventory exists; the provider name is used as the site identity (Phase 1
///   self-hosted fallback).
/// - [`SiteResolution::Known`] with a non-empty list: one candidate per `(model, site)` pair.
/// - [`SiteResolution::Known`] with an empty list: the provider's selector matched no sites; **no candidates are
///   emitted**.  This is distinct from `Unavailable` — it means the inventory exists but excluded this provider.
///
/// # Errors
///
/// Returns an error if the provider has no metadata name or any model
/// name is blank or whitespace-only.
fn candidates_from_provider(
    provider: &InferenceProvider,
    site_resolution: &SiteResolution,
) -> Result<Vec<RoutingCandidate>, String> {
    let provider_name = provider
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| "InferenceProvider has no name".to_owned())?;

    for model in &provider.spec.models {
        if model.name.trim().is_empty() {
            return Err(format!("provider {provider_name} has a blank model name"));
        }
    }

    // Use routing_identity for cluster (and site in Phase 1 fallback).
    // When spec.routingClusterRef is set, it overrides metadata.name so that
    // overlay candidates reference the correct upstream cluster and site.
    let cluster = routing_identity(provider).unwrap_or(provider_name);

    let sites: Vec<&str> = match &site_resolution {
        // No site inventory at all → Phase 1 fallback.
        // Use routing identity so the site field matches the cluster reference.
        SiteResolution::Unavailable => vec![cluster],
        // Inventory exists but selector matched nothing → emit no candidates.
        SiteResolution::Known(names) if names.is_empty() => return Ok(Vec::new()),
        // Inventory exists and selector matched these sites.
        SiteResolution::Known(names) => names.iter().map(String::as_str).collect(),
    };

    let fresh = is_candidate_fresh(provider);
    let mut candidates = Vec::new();
    for model in &provider.spec.models {
        for site in &sites {
            candidates.push(RoutingCandidate {
                kind: CANDIDATE_KIND.to_owned(),
                name: model.name.clone(),
                site: (*site).to_owned(),
                cluster: cluster.to_owned(),
                fresh,
            });
        }
    }
    Ok(candidates)
}

/// Returns `true` only when the provider's status phase is explicitly
/// [`ProviderPhase::Unavailable`].
///
/// Absent status (no [`InferenceProvider`] controller yet), `Pending`,
/// `Available`, and `Degraded` all return `false` — the provider is
/// included in the overlay.  This conservative default ensures that
/// providers are visible before OP-02 populates their status.  The
/// OP-02 `InferenceProvider` controller can tighten this policy once
/// it reliably sets `status.phase`.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
fn is_explicitly_unavailable(provider: &InferenceProvider) -> bool {
    provider
        .status
        .as_ref()
        .is_some_and(|s| s.phase == ProviderPhase::Unavailable)
}

/// Returns `true` when this provider's candidate data is considered fresh.
///
/// Freshness is derived from `status.phase`:
///
/// | Phase | Included | `fresh` |
/// |-------|----------|---------|
/// | `Available` | yes | `true` |
/// | `Pending` | yes | `true` |
/// | absent status | yes | `true` |
/// | `Degraded` | yes | **`false`** |
/// | `Unavailable` | no | — (excluded before this is called) |
///
/// `Degraded` means the provider is reachable but partially unhealthy
/// (e.g. high error rate, endpoint returning errors). Including it with
/// `fresh: false` lets Praxis keep the candidate in its selection pool
/// while signalling that its metrics are stale or unreliable.
///
/// Absent status uses `true` as the conservative default so that
/// providers are visible before OP-02 has populated their status.
///
/// `Unavailable` providers never reach this function — they are excluded
/// by [`is_explicitly_unavailable`] before candidates are generated.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
pub(crate) fn is_candidate_fresh(provider: &InferenceProvider) -> bool {
    provider
        .status
        .as_ref()
        .is_none_or(|s| s.phase != ProviderPhase::Degraded)
}

// ---------------------------------------------------------------------------
// ConfigMap Builder
// ---------------------------------------------------------------------------

/// Build a Kubernetes `ConfigMap` for a [`RoutingOverlay`].
///
/// Returns an error if the overlay cannot be serialised to JSON.
/// This prevents applying an empty or invalid config to the cluster.
///
/// The `ConfigMap` name is computed by `overlay_configmap_name` which
/// ensures names are ≤ 63 characters and collision-safe.
///
/// # Errors
///
/// Returns [`serde_json::Error`] if the [`RoutingOverlay`] cannot be
/// serialised.  In practice this cannot fail for the current type
/// definition, but the caller must handle it to prevent silently
/// applying an empty config.
pub fn build_overlay_configmap(
    overlay: &RoutingOverlay,
    network_name: &str,
    gateway_name: &str,
    namespace: &str,
) -> Result<ConfigMap, serde_json::Error> {
    let json = serde_json::to_string_pretty(overlay)?;
    let name = overlay_configmap_name(network_name, gateway_name);

    let data = BTreeMap::from([("grid-config.json".to_owned(), json)]);

    Ok(ConfigMap {
        metadata: kube::api::ObjectMeta {
            labels: Some(overlay_labels(network_name, gateway_name)),
            name: Some(name),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    })
}

/// Compute the `ConfigMap` name.
///
/// Returns `grid-overlay-{network}-{gateway}` when the full string
/// fits in [`MAX_K8S_NAME`] (63) characters.
///
/// When the raw name would exceed 63 characters, uses a hash-suffixed
/// form: `grid-overlay-{net_prefix}-{gw_prefix}-{hash8}` where
/// `net_prefix` and `gw_prefix` are each at most [`MAX_COMPONENT_PREFIX`]
/// (20) characters and `hash8` is 8 lowercase hex digits derived from a
/// FNV-1a 32-bit hash of `"{network}/{gateway}"`.
///
/// The total of the hash-suffixed form is always ≤ 63 characters:
/// `"grid-overlay-"` (13) + 20 + `"-"` + 20 + `"-"` + 8 = 63.
fn overlay_configmap_name(network_name: &str, gateway_name: &str) -> String {
    let raw = format!("grid-overlay-{network_name}-{gateway_name}");
    if raw.len() <= MAX_K8S_NAME {
        return raw;
    }

    let hash = fnv1a_hex8(&format!("{network_name}/{gateway_name}"));
    let net_prefix: String = network_name.chars().take(MAX_COMPONENT_PREFIX).collect();
    let gw_prefix: String = gateway_name.chars().take(MAX_COMPONENT_PREFIX).collect();
    format!("grid-overlay-{net_prefix}-{gw_prefix}-{hash}")
}

/// FNV-1a 32-bit hash, returned as 8 lowercase hexadecimal digits.
///
/// Deterministic, dependency-free, and sufficient for name disambiguation.
/// Not cryptographically secure; not used for security-critical purposes.
fn fnv1a_hex8(input: &str) -> String {
    const FNV_OFFSET: u32 = 2_166_136_261;
    const FNV_PRIME: u32 = 16_777_619;
    let mut hash = FNV_OFFSET;
    for byte in input.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:08x}")
}

/// Build the standard labels for an overlay `ConfigMap`.
fn overlay_labels(network_name: &str, gateway_name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app.kubernetes.io/managed-by".to_owned(), "grid-operator".to_owned()),
        ("grid.praxis-proxy.io/gateway".to_owned(), gateway_name.to_owned()),
        ("grid.praxis-proxy.io/network".to_owned(), network_name.to_owned()),
    ])
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::too_many_lines,
    reason = "tests"
)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Test utilities
    // -----------------------------------------------------------------------

    fn test_network(name: &str) -> GridNetwork {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": name },
            "spec": { "seeds": [] }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_site(name: &str, network: &str) -> GridSite {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": name },
            "spec": { "gridNetworkRef": network }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_site_with_labels(name: &str, network: &str, labels: &[(&str, &str)]) -> GridSite {
        let labels_map: serde_json::Map<String, serde_json::Value> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridSite",
            "metadata": {
                "name": name,
                "labels": labels_map
            },
            "spec": { "gridNetworkRef": network }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_provider(name: &str, network: &str, models: &[&str]) -> InferenceProvider {
        let models_json: Vec<serde_json::Value> = models.iter().map(|m| serde_json::json!({ "name": m })).collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": models_json
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_provider_with_selector(
        name: &str,
        network: &str,
        models: &[&str],
        selector: &[(&str, &str)],
    ) -> InferenceProvider {
        let models_json: Vec<serde_json::Value> = models.iter().map(|m| serde_json::json!({ "name": m })).collect();
        let match_labels: serde_json::Map<String, serde_json::Value> = selector
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": models_json,
                "siteSelector": { "matchLabels": match_labels }
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_provider_with_phase(name: &str, network: &str, models: &[&str], phase: &str) -> InferenceProvider {
        let models_json: Vec<serde_json::Value> = models.iter().map(|m| serde_json::json!({ "name": m })).collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": models_json
            },
            "status": {
                "phase": phase,
                "matchingSites": [],
                "observedGeneration": 0
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn overlay_json_from_cm(cm: &ConfigMap) -> serde_json::Value {
        let json_str = cm
            .data
            .as_ref()
            .and_then(|d| d.get("grid-config.json"))
            .unwrap_or_else(|| std::process::abort());
        serde_json::from_str(json_str).unwrap_or_else(|_| std::process::abort())
    }

    fn build_cm(overlay: &RoutingOverlay, net: &str, gw: &str) -> ConfigMap {
        build_overlay_configmap(overlay, net, gw, "ns").unwrap_or_else(|_| std::process::abort())
    }

    fn test_provider_with_backend_kind(name: &str, network: &str, backend_kind: &str) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "self_hosted",
                "backendKind": backend_kind,
                "endpoint": "http://localhost:8000",
                "models": [{ "name": "model-a" }]
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_provider_with_cost(name: &str, network: &str, per_million_input: f64) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "open_ai",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": [{ "name": "model-a" }],
                "cost": { "perMillionInputTokens": per_million_input, "perMillionOutputTokens": 0.0 }
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_network_with_region(name: &str, region: &str) -> GridNetwork {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": name },
            "spec": { "seeds": [], "region": region }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    // -----------------------------------------------------------------------
    // provider_to_backend_config — mapping function
    // -----------------------------------------------------------------------

    #[test]
    fn provider_to_backend_config_maps_local_backend_kind() {
        let p = test_provider_with_backend_kind("prov-a", "net", "local");
        let cfg = provider_to_backend_config(&p).unwrap_or_else(|| std::process::abort());
        assert_eq!(cfg.name, "prov-a", "name must match metadata.name");
        assert_eq!(
            cfg.kind,
            scoring::BackendKind::Local,
            "local must map to BackendKind::Local"
        );
    }

    #[test]
    fn provider_to_backend_config_maps_remote_backend_kind() {
        let p = test_provider_with_backend_kind("prov-b", "net", "remote");
        let cfg = provider_to_backend_config(&p).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            cfg.kind,
            scoring::BackendKind::Remote,
            "remote must map to BackendKind::Remote"
        );
    }

    #[test]
    fn provider_to_backend_config_maps_cloud_managed_backend_kind() {
        let p = test_provider_with_backend_kind("prov-c", "net", "cloud_managed");
        let cfg = provider_to_backend_config(&p).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            cfg.kind,
            scoring::BackendKind::CloudManaged,
            "cloud_managed must map correctly"
        );
    }

    #[test]
    fn provider_to_backend_config_maps_api_provider_backend_kind() {
        let p = test_provider_with_backend_kind("prov-d", "net", "api_provider");
        let cfg = provider_to_backend_config(&p).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            cfg.kind,
            scoring::BackendKind::ApiProvider,
            "api_provider must map correctly"
        );
    }

    #[test]
    fn provider_to_backend_config_unknown_backend_kind_returns_none() {
        let p = test_provider_with_backend_kind("prov-x", "net", "nonexistent_kind");
        assert!(
            provider_to_backend_config(&p).is_none(),
            "unknown backend_kind must return None"
        );
    }

    #[test]
    fn provider_to_backend_config_empty_backend_kind_returns_none() {
        let p = test_provider_with_backend_kind("prov-e", "net", "");
        assert!(
            provider_to_backend_config(&p).is_none(),
            "empty backend_kind must return None"
        );
    }

    #[test]
    fn provider_to_backend_config_unknown_provider_kind_defaults_to_open_ai() {
        // "self_hosted" is not a scoring::ProviderKind variant; must default to OpenAi.
        // provider_kind is metadata only and does not affect the scoring formula.
        let p = test_provider_with_backend_kind("prov-f", "net", "local");
        let cfg = provider_to_backend_config(&p).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            cfg.provider,
            scoring::ProviderKind::OpenAi,
            "self_hosted provider_kind must default to OpenAi"
        );
    }

    #[test]
    fn provider_to_backend_config_known_provider_kind_is_preserved() {
        let p: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": "prov-g" },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "anthropic",
                "backendKind": "api_provider",
                "endpoint": "https://api.anthropic.com",
                "models": [{ "name": "claude" }]
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let cfg = provider_to_backend_config(&p).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            cfg.provider,
            scoring::ProviderKind::Anthropic,
            "anthropic must be preserved"
        );
    }

    #[test]
    fn provider_to_backend_config_cost_converted_per_million_to_per_1k() {
        // 1.0 per million input tokens = 0.001 per 1k input tokens
        let p = test_provider_with_cost("prov-h", "net", 1.0);
        let cfg = provider_to_backend_config(&p).unwrap_or_else(|| std::process::abort());
        assert!(
            (cfg.cost_per_1k_input - 0.001_f64).abs() < f64::EPSILON,
            "1.0/million must convert to 0.001/1k, got {}",
            cfg.cost_per_1k_input
        );
    }

    #[test]
    fn provider_to_backend_config_absent_cost_is_zero() {
        let p = test_provider_with_backend_kind("prov-i", "net", "local");
        let cfg = provider_to_backend_config(&p).unwrap_or_else(|| std::process::abort());
        assert_eq!(cfg.cost_per_1k_input, 0.0, "absent cost must be 0.0");
        assert_eq!(cfg.cost_per_1k_output, 0.0, "absent output cost must be 0.0");
    }

    #[test]
    fn provider_to_backend_config_missing_metadata_name_returns_none() {
        let p: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": {},
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": [{ "name": "m" }]
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert!(
            provider_to_backend_config(&p).is_none(),
            "provider with no metadata.name must return None"
        );
    }

    #[test]
    fn provider_to_backend_config_provider_region_is_none() {
        // InferenceProvider carries no region; BackendConfig.region must be None.
        // Region-aware scoring (0.7 same-region) requires per-provider region data
        // which is not yet in the CRD.
        let p = test_provider_with_backend_kind("prov-j", "net", "remote");
        let cfg = provider_to_backend_config(&p).unwrap_or_else(|| std::process::abort());
        assert!(cfg.region.is_none(), "provider region must always be None (not in CRD)");
    }

    // -----------------------------------------------------------------------
    // Scoring-engine-backed ordering (OP-05c-a)
    // -----------------------------------------------------------------------

    #[test]
    fn score_ordered_local_ranks_before_api_provider() {
        // Regression-safe: full scoring engine must preserve local > api ordering.
        let network = test_network("net");
        let local_prov = test_provider_with_backend_kind("local-prov", "net", "local");
        let api_prov = test_provider_with_backend_kind("api-prov", "net", "api_provider");
        let overlay = render_routing_overlay(&network, &[], &[api_prov, local_prov], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("local-prov"),
            "local provider must rank before api_provider regardless of input order"
        );
    }

    #[test]
    fn score_ordered_cost_differentiates_equal_locality_providers() {
        // Two local providers with different costs: lower cost must rank first.
        // Score difference: cost_score(0.0) - cost_score(0.001) = 1.0 - 0.99 = 0.01;
        // multiplied by weight 1.0 → free provider wins.
        let network = test_network("net");
        let free_prov = test_provider_with_cost("free-prov", "net", 0.0);
        let costly_prov = test_provider_with_cost("costly-prov", "net", 50.0); // 50/million = 0.05/1k
        let overlay = render_routing_overlay(&network, &[], &[costly_prov, free_prov], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("free-prov"),
            "lower-cost provider must rank first when locality is equal"
        );
    }

    #[test]
    fn score_ordered_deterministic_for_equal_scores() {
        // Two identical providers (same kind, same cost) → alphabetical tiebreak.
        let network = test_network("net");
        let p_z = test_provider_with_backend_kind("z-local", "net", "local");
        let p_a = test_provider_with_backend_kind("a-local", "net", "local");
        let overlay =
            render_routing_overlay(&network, &[], &[p_z, p_a], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("a-local"),
            "equal scores must fall back to alphabetical cluster ordering"
        );
    }

    #[test]
    fn score_ordered_unknown_backend_kind_uses_same_scale_fallback() {
        // Providers with unknown backend_kind fall back to unmapped_provider_score,
        // which is on the same numeric scale as score_backends output.
        // unknown kind → locality 0.5 → same scale score ≈ 7.0
        // cloud_managed → locality 0.2 → score ≈ 6.1
        // unknown (7.0) must rank before cloud_managed (6.1).
        let network = test_network("net");
        let cloud = test_provider_with_backend_kind("cloud-prov", "net", "cloud_managed");
        let unknown = test_provider_with_backend_kind("unknown-prov", "net", "nonexistent_kind");
        let overlay = render_routing_overlay(&network, &[], &[cloud, unknown], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("unknown-prov"),
            "unmapped-kind provider (same scale as remote, ≈7.0) must rank before cloud_managed (≈6.1)"
        );
    }

    #[test]
    fn score_ordered_self_hosted_provider_kind_is_included() {
        // "self_hosted" provider_kind (vLLM / llm-d) defaults to ProviderKind::OpenAi.
        // The provider must appear in the overlay with correct ordering.
        let network = test_network("net");
        let self_hosted = test_provider_with_backend_kind("vllm-prov", "net", "local");
        let api = test_provider_with_backend_kind("api-prov", "net", "api_provider");
        let overlay = render_routing_overlay(&network, &[], &[api, self_hosted], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 2, "both providers must appear");
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("vllm-prov"),
            "self_hosted local provider must rank before api_provider"
        );
    }

    #[test]
    fn score_ordered_input_order_does_not_affect_output() {
        // Scoring must be deterministic regardless of which slice order providers arrive in.
        let network = test_network("net");
        let local = test_provider_with_backend_kind("local-prov", "net", "local");
        let api = test_provider_with_backend_kind("api-prov", "net", "api_provider");
        let fwd = render_routing_overlay(&network, &[], &[local.clone(), api.clone()], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        let rev =
            render_routing_overlay(&network, &[], &[api, local], "test-site").unwrap_or_else(|_| std::process::abort());
        let fwd_clusters: Vec<&str> = fwd.candidates.iter().map(|c| c.cluster.as_str()).collect();
        let rev_clusters: Vec<&str> = rev.candidates.iter().map(|c| c.cluster.as_str()).collect();
        assert_eq!(
            fwd_clusters, rev_clusters,
            "scoring output must be deterministic regardless of input order"
        );
    }

    #[test]
    fn score_ordered_with_network_region_does_not_break() {
        // Network region is threaded into score_backends. With provider regions always
        // None (not in CRD), remote providers still score 0.5 regardless — but the
        // call must not panic or produce wrong results.
        let network = test_network_with_region("net", "eu-west-1");
        let local = test_provider_with_backend_kind("local-prov", "net", "local");
        let remote = test_provider_with_backend_kind("remote-prov", "net", "remote");
        let overlay = render_routing_overlay(&network, &[], &[remote, local], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("local-prov"),
            "local must still rank first even when network.region is set"
        );
    }

    // -----------------------------------------------------------------------
    // Cross-site routing and API fallback cases
    //
    // These tests validate the static overlay shapes needed for multi-provider
    // configurations: local + remote cross-site routing, unavailable/degraded
    // local with API fallback, and the full four-kind candidate set.
    //
    // Praxis `grid_route` candidate contract (current wire format):
    //   kind     — always "inference_model" for inference providers
    //   name     — model name (used for model-based routing)
    //   site     — site identifier (= provider name in Phase 1 no-site mode)
    //   cluster  — Praxis load_balancer cluster name (= provider name)
    //   fresh    — false when provider is Degraded; Praxis applies staleness penalty
    //
    // Note: `endpoint` is NOT part of the candidate struct.  The cluster name is
    // the reference Praxis uses to look up the backend endpoint in its cluster
    // config.  Adding endpoint to RoutingCandidate would require a coordinated
    // Praxis schema change (CM3 cross-repo mismatch documented in review).
    // -----------------------------------------------------------------------

    /// Build a provider with an explicit backend kind AND a status phase.
    fn test_provider_with_backend_kind_and_phase(
        name: &str,
        network: &str,
        backend_kind: &str,
        phase: &str,
    ) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "self_hosted",
                "backendKind": backend_kind,
                "endpoint": "http://localhost:8000",
                "models": [{ "name": "shared-model" }]
            },
            "status": {
                "phase": phase,
                "matchingSites": [],
                "observedGeneration": 0
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn cross_site_overlay_local_then_remote_candidate_order() {
        // Static shape: two providers, one local and one remote, both offering
        // the same model. Overlay must contain both candidates and order local
        // before remote. Phase-1 no-site mode: site = provider name.
        let network = test_network("mesh-net");
        let local_prov = test_provider_with_backend_kind("provider-self-hosted", "mesh-net", "local");
        let remote_prov = test_provider_with_backend_kind("provider-remote", "mesh-net", "remote");
        let overlay = render_routing_overlay(&network, &[], &[remote_prov, local_prov], "site-a")
            .unwrap_or_else(|_| std::process::abort());

        assert_eq!(overlay.candidates.len(), 2, "both local and remote must appear");
        // Local must rank first (higher locality/score).
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("provider-self-hosted"),
            "local provider must rank before remote"
        );
        // Both fresh (neither is Degraded).
        assert!(
            overlay.candidates.iter().all(|c| c.fresh),
            "all Available/absent-status candidates must be fresh"
        );
        // Candidate fields are correctly populated.
        let c0 = overlay.candidates.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(c0.kind, "inference_model", "kind must be inference_model");
        assert_eq!(
            c0.site, "provider-self-hosted",
            "site equals provider name (Phase 1 fallback)"
        );
        assert_eq!(c0.cluster, "provider-self-hosted", "cluster equals provider name");
    }

    #[test]
    fn unavailable_local_leaves_api_as_only_candidate() {
        // Local/self-hosted is down (Unavailable), API provider remains
        // accessible. Overlay must exclude the unavailable provider and keep
        // only the API candidate.
        let network = test_network("fallback-net");
        let local_down =
            test_provider_with_backend_kind_and_phase("provider-local", "fallback-net", "local", "Unavailable");
        let api_fallback = test_provider_with_backend_kind("provider-api", "fallback-net", "api_provider");
        let overlay = render_routing_overlay(&network, &[], &[local_down, api_fallback], "site-a")
            .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            overlay.candidates.len(),
            1,
            "Unavailable local must be excluded; only API provider remains"
        );
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("provider-api"),
            "API provider must be the sole candidate"
        );
        assert!(
            overlay.candidates.first().is_some_and(|c| c.fresh),
            "API provider with absent status must be fresh"
        );
    }

    #[test]
    fn degraded_local_and_api_both_included_with_correct_freshness() {
        // Local is Degraded (probe returning non-2xx or high error rate). It
        // remains in the overlay so Praxis can still select it if the API
        // provider is also unavailable, but its fresh=false signals that its
        // metrics are stale.
        //
        // Ordering: Degraded local (locality ≈8.5) still outscores API provider
        // (≈5.8) under default weights.  Praxis may deprioritise the stale local
        // candidate via its own freshness penalty, but the overlay carries both.
        let network = test_network("fallback-net");
        let local_degraded =
            test_provider_with_backend_kind_and_phase("provider-local", "fallback-net", "local", "Degraded");
        let api_ok = test_provider_with_backend_kind("provider-api", "fallback-net", "api_provider");
        let overlay = render_routing_overlay(&network, &[], &[api_ok, local_degraded], "site-a")
            .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            overlay.candidates.len(),
            2,
            "Degraded local must remain in overlay alongside API provider"
        );
        let local_c = overlay
            .candidates
            .iter()
            .find(|c| c.cluster == "provider-local")
            .unwrap_or_else(|| std::process::abort());
        let api_c = overlay
            .candidates
            .iter()
            .find(|c| c.cluster == "provider-api")
            .unwrap_or_else(|| std::process::abort());
        assert!(!local_c.fresh, "Degraded local candidate must have fresh=false");
        assert!(api_c.fresh, "API provider with absent status must have fresh=true");
        // Degraded local still outranks API provider by locality score.
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("provider-local"),
            "Degraded local (high locality) must still rank before API provider"
        );
    }

    #[test]
    fn all_four_backend_kinds_in_overlay_with_correct_order() {
        // A network with one provider of each backend kind. No live metrics —
        // ordering is driven entirely by locality score through the scoring
        // engine. Validates the full four-kind candidate set shape.
        let network = test_network("full-net");
        let self_hosted = test_provider_with_backend_kind("prov-local", "full-net", "local");
        let remote = test_provider_with_backend_kind("prov-remote", "full-net", "remote");
        let cloud = test_provider_with_backend_kind("prov-cloud", "full-net", "cloud_managed");
        let api = test_provider_with_backend_kind("prov-api", "full-net", "api_provider");
        let overlay = render_routing_overlay(&network, &[], &[api, cloud, remote, self_hosted], "site-a")
            .unwrap_or_else(|_| std::process::abort());

        assert_eq!(overlay.candidates.len(), 4, "all four backend kinds must be present");
        let clusters: Vec<&str> = overlay.candidates.iter().map(|c| c.cluster.as_str()).collect();
        assert_eq!(
            clusters,
            ["prov-local", "prov-remote", "prov-cloud", "prov-api"],
            "ordering must be local > remote > cloud_managed > api_provider"
        );
        // All candidates are fresh (no Degraded providers in this case).
        assert!(
            overlay.candidates.iter().all(|c| c.fresh),
            "all candidates must be fresh"
        );
    }

    #[test]
    fn local_provider_recovers_from_unavailable_to_available() {
        // Health-check transition: when a local provider's status moves from
        // Unavailable to Available, it reappears in the overlay.  Each render
        // call is pure and stateless; this test simulates two consecutive
        // reconcile cycles.
        let network = test_network("recovery-net");
        let api_always_available = test_provider_with_backend_kind("prov-api", "recovery-net", "api_provider");

        // Cycle 1: local is down — only API candidate.
        let local_down =
            test_provider_with_backend_kind_and_phase("prov-local", "recovery-net", "local", "Unavailable");
        let overlay1 = render_routing_overlay(&network, &[], &[local_down, api_always_available.clone()], "site-a")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay1.candidates.len(),
            1,
            "unavailable local must not appear (cycle 1)"
        );
        assert_eq!(
            overlay1.candidates.first().map(|c| c.cluster.as_str()),
            Some("prov-api"),
            "API must be the only candidate when local is down"
        );

        // Cycle 2: local is back — both candidates, local ranks first.
        let local_up = test_provider_with_backend_kind_and_phase("prov-local", "recovery-net", "local", "Available");
        let overlay2 = render_routing_overlay(&network, &[], &[local_up, api_always_available], "site-a")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay2.candidates.len(), 2, "recovered local must reappear (cycle 2)");
        assert_eq!(
            overlay2.candidates.first().map(|c| c.cluster.as_str()),
            Some("prov-local"),
            "recovered local must rank first"
        );
        assert!(
            overlay2.candidates.first().is_some_and(|c| c.fresh),
            "recovered local must be fresh"
        );
    }

    #[test]
    fn all_providers_unavailable_produces_empty_overlay() {
        // If every provider in the network is Unavailable, the renderer produces
        // an empty candidate list without returning an error.  The reconcile-loop
        // guard (in grid_network controller) skips applying an empty overlay to
        // prevent Praxis hot-reload errors — that guard is covered at the
        // controller integration level.  This test covers the renderer contract.
        let network = test_network("empty-net");
        let p1 = test_provider_with_phase("prov-a", "empty-net", &["model-a"], "Unavailable");
        let p2 = test_provider_with_phase("prov-b", "empty-net", &["model-b"], "Unavailable");
        let overlay =
            render_routing_overlay(&network, &[], &[p1, p2], "site-a").unwrap_or_else(|_| std::process::abort());

        assert!(
            overlay.candidates.is_empty(),
            "all-Unavailable overlay must have zero candidates (no error)"
        );
    }

    #[test]
    fn cross_site_candidate_json_has_required_praxis_fields() {
        // Validate that the ConfigMap JSON payload exposes all fields the Praxis
        // `grid_route` filter reads from each candidate entry.
        //
        // Current candidate wire format: kind, name, site, cluster, fresh.
        // `endpoint` is NOT in the candidate — Praxis looks up the backend
        // endpoint via the `cluster` name in its own load_balancer config.
        let network = test_network("json-net");
        let local_prov = test_provider_with_backend_kind("prov-a", "json-net", "local");
        let api_prov = test_provider_with_backend_kind("prov-b", "json-net", "api_provider");
        let overlay = render_routing_overlay(&network, &[], &[local_prov, api_prov], "site-a")
            .unwrap_or_else(|_| std::process::abort());
        let cm = build_cm(&overlay, "json-net", "gw");
        let json = overlay_json_from_cm(&cm);
        let candidates = json
            .get("candidates")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(candidates.len(), 2, "both providers must appear in JSON");
        for c in candidates {
            assert!(c.get("kind").is_some(), "candidate must have 'kind'");
            assert!(c.get("name").is_some(), "candidate must have 'name'");
            assert!(c.get("site").is_some(), "candidate must have 'site'");
            assert!(c.get("cluster").is_some(), "candidate must have 'cluster'");
            assert!(c.get("fresh").is_some(), "candidate must have 'fresh'");
            assert_eq!(
                c.get("kind").and_then(serde_json::Value::as_str),
                Some("inference_model"),
                "kind must be inference_model"
            );
        }
        // local must appear first in JSON (higher score).
        assert_eq!(
            candidates
                .first()
                .and_then(|c| c.get("cluster"))
                .and_then(serde_json::Value::as_str),
            Some("prov-a"),
            "local provider must appear first in candidate JSON"
        );
    }

    // -----------------------------------------------------------------------
    // backend_locality_score — pure mapping function
    // -----------------------------------------------------------------------

    #[test]
    fn local_backend_kind_scores_highest() {
        let score = backend_locality_score("local");
        assert!((score - 1.0).abs() < f64::EPSILON, "local must score 1.0, got {score}");
    }

    #[test]
    fn remote_backend_kind_scores_half() {
        // No region context → remote falls back to 0.5.
        let score = backend_locality_score("remote");
        assert!(
            (score - 0.5).abs() < f64::EPSILON,
            "remote (no region) must score 0.5, got {score}"
        );
    }

    #[test]
    fn cloud_managed_backend_kind_scores_low() {
        let score = backend_locality_score("cloud_managed");
        assert!(
            (score - 0.2).abs() < f64::EPSILON,
            "cloud_managed must score 0.2, got {score}"
        );
    }

    #[test]
    fn api_provider_backend_kind_scores_lowest() {
        let score = backend_locality_score("api_provider");
        assert!(
            (score - 0.1).abs() < f64::EPSILON,
            "api_provider must score 0.1, got {score}"
        );
    }

    #[test]
    fn unknown_backend_kind_defaults_to_half() {
        let score = backend_locality_score("unknown_kind_xyz");
        assert!(
            (score - DEFAULT_LOCALITY).abs() < f64::EPSILON,
            "unknown kind must default to {DEFAULT_LOCALITY}, got {score}"
        );
    }

    #[test]
    fn empty_backend_kind_defaults_to_half() {
        let score = backend_locality_score("");
        assert!(
            (score - DEFAULT_LOCALITY).abs() < f64::EPSILON,
            "empty kind must default to {DEFAULT_LOCALITY}, got {score}"
        );
    }

    #[test]
    fn locality_scores_are_strictly_ordered() {
        let local = backend_locality_score("local");
        let remote = backend_locality_score("remote");
        let cloud = backend_locality_score("cloud_managed");
        let api = backend_locality_score("api_provider");
        assert!(local > remote, "local ({local}) must outscore remote ({remote})");
        assert!(
            remote > cloud,
            "remote ({remote}) must outscore cloud_managed ({cloud})"
        );
        assert!(
            cloud > api,
            "cloud_managed ({cloud}) must outscore api_provider ({api})"
        );
    }

    // -----------------------------------------------------------------------
    // Locality-ordered candidate sort
    // -----------------------------------------------------------------------

    #[test]
    fn local_provider_ranks_before_api_provider() {
        let network = test_network("net");
        let local_prov = test_provider_with_backend_kind("local-prov", "net", "local");
        let api_prov = test_provider_with_backend_kind("api-prov", "net", "api_provider");
        let overlay = render_routing_overlay(&network, &[], &[api_prov, local_prov], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("local-prov"),
            "local provider must appear before api_provider regardless of input order"
        );
    }

    #[test]
    fn all_four_backend_kinds_order_correctly() {
        let network = test_network("net");
        // Deliberately supply in reverse priority order.
        let api = test_provider_with_backend_kind("z-api", "net", "api_provider");
        let cloud = test_provider_with_backend_kind("z-cloud", "net", "cloud_managed");
        let remote = test_provider_with_backend_kind("z-remote", "net", "remote");
        let local = test_provider_with_backend_kind("z-local", "net", "local");
        let overlay = render_routing_overlay(&network, &[], &[api, cloud, remote, local], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        let clusters: Vec<&str> = overlay.candidates.iter().map(|c| c.cluster.as_str()).collect();
        // local (1.0) → remote (0.5) → cloud_managed (0.2) → api_provider (0.1)
        assert_eq!(
            clusters,
            ["z-local", "z-remote", "z-cloud", "z-api"],
            "candidates must be ordered by locality: local > remote > cloud_managed > api_provider"
        );
    }

    #[test]
    fn same_locality_kind_falls_back_to_alphabetical() {
        let network = test_network("net");
        let p_z = test_provider_with_backend_kind("z-api", "net", "api_provider");
        let p_a = test_provider_with_backend_kind("a-api", "net", "api_provider");
        let overlay =
            render_routing_overlay(&network, &[], &[p_z, p_a], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("a-api"),
            "equal locality must fall back to alphabetical by cluster"
        );
    }

    #[test]
    fn locality_ordering_is_deterministic_regardless_of_input_order() {
        let network = test_network("net");
        let local = test_provider_with_backend_kind("prov-local", "net", "local");
        let api = test_provider_with_backend_kind("prov-api", "net", "api_provider");
        let fwd = render_routing_overlay(&network, &[], &[local.clone(), api.clone()], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        let rev =
            render_routing_overlay(&network, &[], &[api, local], "test-site").unwrap_or_else(|_| std::process::abort());
        let fwd_clusters: Vec<&str> = fwd.candidates.iter().map(|c| c.cluster.as_str()).collect();
        let rev_clusters: Vec<&str> = rev.candidates.iter().map(|c| c.cluster.as_str()).collect();
        assert_eq!(
            fwd_clusters, rev_clusters,
            "locality ordering must be deterministic regardless of input order"
        );
    }

    #[test]
    fn unknown_backend_kind_sorts_with_remote() {
        // Unknown kind defaults to 0.5 (same as remote with no region).
        // Both should sort before cloud_managed (0.2) and api_provider (0.1).
        let network = test_network("net");
        let cloud = test_provider_with_backend_kind("cloud-prov", "net", "cloud_managed");
        let unknown = test_provider_with_backend_kind("unknown-prov", "net", "nonexistent_kind");
        let overlay = render_routing_overlay(&network, &[], &[cloud, unknown], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("unknown-prov"),
            "unknown kind (0.5) must rank before cloud_managed (0.2)"
        );
    }

    // -----------------------------------------------------------------------
    // Basic rendering
    // -----------------------------------------------------------------------

    #[test]
    fn empty_network_renders_empty_candidates() {
        let network = test_network("my-net");
        let overlay = render_routing_overlay(&network, &[], &[], "test-site").unwrap_or_else(|_| std::process::abort());
        assert!(overlay.candidates.is_empty(), "no providers should yield no candidates");
    }

    #[test]
    fn provider_in_different_network_is_excluded() {
        let network = test_network("net-a");
        let provider = test_provider("prov", "net-b", &["model-1"]);
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert!(
            overlay.candidates.is_empty(),
            "provider in net-b must be excluded from net-a overlay"
        );
    }

    #[test]
    fn provider_with_two_models_renders_two_candidates() {
        let network = test_network("net-a");
        let provider = test_provider("prov", "net-a", &["model-1", "model-2"]);
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 2, "two models must produce two candidates");
    }

    #[test]
    fn two_providers_with_same_model_produce_two_candidates() {
        let network = test_network("net");
        let p1 = test_provider("prov-a", "net", &["llama-3"]);
        let p2 = test_provider("prov-b", "net", &["llama-3"]);
        let overlay =
            render_routing_overlay(&network, &[], &[p1, p2], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            2,
            "two providers for the same model must produce two distinct candidates"
        );
        let sites: Vec<&str> = overlay.candidates.iter().map(|c| c.site.as_str()).collect();
        assert!(sites.contains(&"prov-a"), "candidate from prov-a must be present");
        assert!(sites.contains(&"prov-b"), "candidate from prov-b must be present");
    }

    #[test]
    fn candidates_are_sorted_by_site_then_name() {
        let network = test_network("net");
        let p1 = test_provider("site-b", "net", &["z-model", "a-model"]);
        let p2 = test_provider("site-a", "net", &["c-model"]);
        let overlay =
            render_routing_overlay(&network, &[], &[p1, p2], "test-site").unwrap_or_else(|_| std::process::abort());
        let names: Vec<&str> = overlay.candidates.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            ["c-model", "a-model", "z-model"],
            "candidates must be sorted: site-a first, then site-b models alphabetically"
        );
    }

    #[test]
    fn input_order_does_not_affect_output() {
        let network = test_network("net");
        let p1 = test_provider("z-site", "net", &["z-model"]);
        let p2 = test_provider("a-site", "net", &["a-model"]);
        let fwd = render_routing_overlay(&network, &[], &[p1.clone(), p2.clone()], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        let rev =
            render_routing_overlay(&network, &[], &[p2, p1], "test-site").unwrap_or_else(|_| std::process::abort());
        let fwd_names: Vec<&str> = fwd.candidates.iter().map(|c| c.name.as_str()).collect();
        let rev_names: Vec<&str> = rev.candidates.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            fwd_names, rev_names,
            "output must be deterministic regardless of input order"
        );
    }

    #[test]
    fn blank_model_name_returns_error() {
        let network = test_network("net");
        let provider = test_provider("prov", "net", &[""]);
        let result = render_routing_overlay(&network, &[], &[provider], "test-site");
        assert!(result.is_err(), "blank model name must return an error");
    }

    // -----------------------------------------------------------------------
    // Site selector
    // -----------------------------------------------------------------------

    #[test]
    fn empty_selector_matches_all_sites_in_network() {
        let network = test_network("net");
        let site_a = test_site("site-a", "net");
        let site_b = test_site("site-b", "net");
        let provider = test_provider("prov", "net", &["model"]);
        let overlay = render_routing_overlay(&network, &[site_a, site_b], &[provider], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            2,
            "empty selector should produce one candidate per site"
        );
        let sites: Vec<&str> = overlay.candidates.iter().map(|c| c.site.as_str()).collect();
        assert!(sites.contains(&"site-a"), "site-a must be in candidates");
        assert!(sites.contains(&"site-b"), "site-b must be in candidates");
    }

    #[test]
    fn selector_labels_match_only_labeled_sites() {
        let network = test_network("net");
        let site_gpu = test_site_with_labels("gpu-site", "net", &[("hw", "gpu")]);
        let site_cpu = test_site_with_labels("cpu-site", "net", &[("hw", "cpu")]);
        let provider = test_provider_with_selector("prov", "net", &["model"], &[("hw", "gpu")]);
        let overlay = render_routing_overlay(&network, &[site_gpu, site_cpu], &[provider], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "selector hw=gpu should match only gpu-site"
        );
        assert_eq!(overlay.candidates[0].site, "gpu-site", "site must be gpu-site");
    }

    #[test]
    fn sites_in_another_network_are_ignored() {
        // Sites from net-b are pre-filtered before site matching, leaving
        // network_sites = [] for net-a.  An empty network_sites list means
        // SiteResolution::Unavailable, which triggers the provider-name fallback.
        let network = test_network("net-a");
        let site_other = test_site("site-other", "net-b");
        let provider = test_provider("prov", "net-a", &["model"]);
        let overlay = render_routing_overlay(&network, &[site_other], &[provider], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "no sites in this network → Unavailable → provider-name fallback"
        );
        assert_eq!(
            overlay.candidates[0].site, "prov",
            "site must fall back to provider name when network has no site inventory"
        );
    }

    #[test]
    fn known_sites_with_selector_no_match_emits_no_candidates() {
        // Site inventory IS present (one site), but the provider's selector
        // requires hw=gpu and only hw=cpu exists.  Selector matched nothing →
        // SiteResolution::Known([]) → no candidates.  Must NOT fall back to
        // provider name.
        let network = test_network("net");
        let site_cpu = test_site_with_labels("cpu-site", "net", &[("hw", "cpu")]);
        let provider = test_provider_with_selector("prov", "net", &["model"], &[("hw", "gpu")]);
        let overlay = render_routing_overlay(&network, &[site_cpu], &[provider], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert!(
            overlay.candidates.is_empty(),
            "selector matched nothing in a known site inventory — must emit no candidates"
        );
    }

    #[test]
    fn two_providers_same_model_same_site_different_cluster_both_survive() {
        // Two providers on the same site serving the same model.
        // Dedup is on (kind, name, site, cluster); different cluster → both kept.
        let network = test_network("net");
        let site = test_site("site-a", "net");
        let p1 = test_provider("prov-a", "net", &["shared-model"]);
        let p2 = test_provider("prov-b", "net", &["shared-model"]);
        let overlay =
            render_routing_overlay(&network, &[site], &[p1, p2], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            2,
            "two providers with different clusters must produce two candidates even for the same model+site"
        );
        let clusters: Vec<&str> = overlay.candidates.iter().map(|c| c.cluster.as_str()).collect();
        assert!(clusters.contains(&"prov-a"), "prov-a must be in candidates");
        assert!(clusters.contains(&"prov-b"), "prov-b must be in candidates");
    }

    #[test]
    fn provider_with_sites_sets_cluster_to_provider_name() {
        let network = test_network("net");
        let site = test_site("site-a", "net");
        let provider = test_provider("my-provider", "net", &["model"]);
        let overlay = render_routing_overlay(&network, &[site], &[provider], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates[0].cluster, "my-provider",
            "cluster must always equal the provider metadata name"
        );
        assert_eq!(overlay.candidates[0].site, "site-a", "site must be resolved site name");
    }

    // -----------------------------------------------------------------------
    // is_candidate_fresh — pure freshness decision function
    // -----------------------------------------------------------------------

    #[test]
    fn available_phase_is_fresh() {
        let provider = test_provider_with_phase("prov", "net", &["model"], "Available");
        assert!(is_candidate_fresh(&provider), "Available must be fresh");
    }

    #[test]
    fn pending_phase_is_fresh() {
        let provider = test_provider_with_phase("prov", "net", &["model"], "Pending");
        assert!(is_candidate_fresh(&provider), "Pending must be fresh");
    }

    #[test]
    fn absent_status_is_fresh() {
        let provider = test_provider("prov", "net", &["model"]);
        assert!(
            is_candidate_fresh(&provider),
            "absent status must be fresh (conservative default before OP-02 runs)"
        );
    }

    #[test]
    fn degraded_phase_is_not_fresh() {
        let provider = test_provider_with_phase("prov", "net", &["model"], "Degraded");
        assert!(
            !is_candidate_fresh(&provider),
            "Degraded must NOT be fresh (provider included but data is stale)"
        );
    }

    // Unavailable is excluded before is_candidate_fresh is called — no test
    // for Unavailable freshness, as it never reaches this function.

    // -----------------------------------------------------------------------
    // Provider status filtering — inclusion and fresh flag
    // -----------------------------------------------------------------------

    #[test]
    fn unavailable_provider_is_excluded() {
        let network = test_network("net");
        let provider = test_provider_with_phase("prov", "net", &["model-1"], "Unavailable");
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert!(overlay.candidates.is_empty(), "Unavailable provider must be excluded");
    }

    #[test]
    fn available_provider_is_included_with_fresh_true() {
        let network = test_network("net");
        let provider = test_provider_with_phase("prov", "net", &["model-1"], "Available");
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 1, "Available provider must be included");
        assert!(
            overlay.candidates.first().is_some_and(|c| c.fresh),
            "Available provider candidate must have fresh=true"
        );
    }

    #[test]
    fn pending_provider_is_included_with_fresh_true() {
        let network = test_network("net");
        let provider = test_provider_with_phase("prov", "net", &["model-1"], "Pending");
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "Pending provider must be included (default pre-OP-02 state)"
        );
        assert!(
            overlay.candidates.first().is_some_and(|c| c.fresh),
            "Pending provider candidate must have fresh=true"
        );
    }

    #[test]
    fn provider_with_absent_status_is_included_with_fresh_true() {
        let network = test_network("net");
        let provider = test_provider("prov", "net", &["model-1"]);
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "Provider with absent status must be included"
        );
        assert!(
            overlay.candidates.first().is_some_and(|c| c.fresh),
            "Provider with absent status must have fresh=true"
        );
    }

    #[test]
    fn degraded_provider_is_included_with_fresh_false() {
        let network = test_network("net");
        let provider = test_provider_with_phase("prov", "net", &["model-1"], "Degraded");
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "Degraded provider must remain in overlay (kept for selection with staleness hint)"
        );
        assert!(
            overlay.candidates.first().is_some_and(|c| !c.fresh),
            "Degraded provider candidate must have fresh=false"
        );
    }

    #[test]
    fn degraded_provider_all_models_fresh_false() {
        // All models from a Degraded provider inherit fresh=false.
        let network = test_network("net");
        let provider = test_provider_with_phase("prov", "net", &["model-a", "model-b"], "Degraded");
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 2, "both models must be present");
        assert!(
            overlay.candidates.iter().all(|c| !c.fresh),
            "all candidates from a Degraded provider must have fresh=false"
        );
    }

    #[test]
    fn mixed_phases_produce_correct_fresh_values() {
        // Available + Degraded in the same network — each candidate's fresh
        // reflects its provider's phase independently.
        let network = test_network("net");
        let available = test_provider_with_phase("avail-prov", "net", &["model-a"], "Available");
        let degraded = test_provider_with_phase("degr-prov", "net", &["model-a"], "Degraded");
        let overlay = render_routing_overlay(&network, &[], &[available, degraded], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 2, "both providers must contribute candidates");
        let avail_candidate = overlay
            .candidates
            .iter()
            .find(|c| c.cluster == "avail-prov")
            .unwrap_or_else(|| std::process::abort());
        let degr_candidate = overlay
            .candidates
            .iter()
            .find(|c| c.cluster == "degr-prov")
            .unwrap_or_else(|| std::process::abort());
        assert!(avail_candidate.fresh, "Available provider candidate must be fresh");
        assert!(!degr_candidate.fresh, "Degraded provider candidate must not be fresh");
    }

    #[test]
    fn degraded_fresh_false_appears_in_json_output() {
        // End-to-end: Degraded → fresh=false must survive JSON serialisation
        // in the ConfigMap and be readable by the overlay consumer.
        let network = test_network("net");
        let provider = test_provider_with_phase("prov", "net", &["model-1"], "Degraded");
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        let cm = build_cm(&overlay, "net", "gw");
        let json = overlay_json_from_cm(&cm);
        let fresh = json
            .get("candidates")
            .and_then(serde_json::Value::as_array)
            .and_then(|a| a.first())
            .and_then(|c| c.get("fresh"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or_else(|| std::process::abort());
        assert!(!fresh, "Degraded provider must produce fresh=false in ConfigMap JSON");
    }

    #[test]
    fn degraded_with_sites_all_site_candidates_fresh_false() {
        // When a Degraded provider matches multiple sites, all resulting
        // (model, site) candidates must have fresh=false.
        let network = test_network("net");
        let site_a = test_site("site-a", "net");
        let site_b = test_site("site-b", "net");
        let provider = test_provider_with_phase("prov", "net", &["model-a"], "Degraded");
        let overlay = render_routing_overlay(&network, &[site_a, site_b], &[provider], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 2, "one candidate per matched site");
        assert!(
            overlay.candidates.iter().all(|c| !c.fresh),
            "every (model, site) candidate from a Degraded provider must have fresh=false"
        );
    }

    // -----------------------------------------------------------------------
    // ConfigMap builder — fallible serialization
    // -----------------------------------------------------------------------

    #[test]
    fn configmap_name_matches_pattern() {
        let network = test_network("my-net");
        let overlay = render_routing_overlay(&network, &[], &[], "test-site").unwrap_or_else(|_| std::process::abort());
        let cm = build_cm(&overlay, "my-net", "gw");
        assert_eq!(
            cm.metadata.name.as_deref(),
            Some("grid-overlay-my-net-gw"),
            "ConfigMap name must be grid-overlay-{{network}}-{{gateway}}"
        );
    }

    #[test]
    fn configmap_has_correct_labels() {
        let network = test_network("net");
        let overlay = render_routing_overlay(&network, &[], &[], "test-site").unwrap_or_else(|_| std::process::abort());
        let cm = build_cm(&overlay, "net", "gw");
        let labels = cm.metadata.labels.as_ref().unwrap_or_else(|| std::process::abort());
        assert_eq!(
            labels.get("app.kubernetes.io/managed-by").map(String::as_str),
            Some("grid-operator"),
        );
        assert_eq!(
            labels.get("grid.praxis-proxy.io/network").map(String::as_str),
            Some("net")
        );
        assert_eq!(
            labels.get("grid.praxis-proxy.io/gateway").map(String::as_str),
            Some("gw")
        );
    }

    #[test]
    fn configmap_data_key_is_grid_config_json() {
        let network = test_network("net");
        let overlay = render_routing_overlay(&network, &[], &[], "test-site").unwrap_or_else(|_| std::process::abort());
        let cm = build_cm(&overlay, "net", "gw");
        assert!(
            cm.data.as_ref().is_some_and(|d| d.contains_key("grid-config.json")),
            "data key must be grid-config.json"
        );
    }

    #[test]
    fn build_overlay_configmap_is_fallible() {
        // This test verifies the signature is Result-based.
        // Serialization of RoutingOverlay cannot currently fail (all fields
        // are plain Strings / booleans), so we just verify the Ok path.
        let network = test_network("net");
        let overlay = render_routing_overlay(&network, &[], &[], "test-site").unwrap_or_else(|_| std::process::abort());
        let result = build_overlay_configmap(&overlay, "net", "gw", "ns");
        assert!(result.is_ok(), "well-formed overlay must serialize without error");
    }

    // -----------------------------------------------------------------------
    // ConfigMap name — collision safety
    // -----------------------------------------------------------------------

    #[test]
    fn normal_name_is_stable() {
        assert_eq!(
            overlay_configmap_name("net", "gw"),
            "grid-overlay-net-gw",
            "short names must not be hashed"
        );
    }

    #[test]
    fn long_name_is_at_most_63_chars() {
        let net = "a".repeat(50);
        let gw = "b".repeat(50);
        let name = overlay_configmap_name(&net, &gw);
        assert!(name.len() <= MAX_K8S_NAME, "name must be ≤63 chars, got {}", name.len());
    }

    #[test]
    fn two_long_names_with_same_prefix_do_not_collide() {
        let net = "a".repeat(50);
        let gw1 = "b".repeat(50);
        let gw2 = "c".repeat(50);
        let n1 = overlay_configmap_name(&net, &gw1);
        let n2 = overlay_configmap_name(&net, &gw2);
        assert_ne!(n1, n2, "different inputs must produce different names");
    }

    #[test]
    fn fnv1a_hash_is_deterministic() {
        assert_eq!(fnv1a_hex8("net/gw"), fnv1a_hex8("net/gw"), "hash must be deterministic");
        assert_ne!(
            fnv1a_hex8("net-a/gw"),
            fnv1a_hex8("net-b/gw"),
            "different inputs must produce different hashes"
        );
    }

    // -----------------------------------------------------------------------
    // JSON payload
    // -----------------------------------------------------------------------

    #[test]
    fn json_overlay_has_correct_top_level_fields() {
        let network = test_network("my-net");
        let overlay = render_routing_overlay(&network, &[], &[], "site-a").unwrap_or_else(|_| std::process::abort());
        let value = overlay_json_from_cm(&build_cm(&overlay, "my-net", "gw"));
        assert_eq!(value.get("network").and_then(serde_json::Value::as_str), Some("my-net"));
        assert_eq!(
            value.get("local_site").and_then(serde_json::Value::as_str),
            Some("site-a")
        );
        assert!(value.get("candidates").and_then(serde_json::Value::as_array).is_some());
    }

    #[test]
    fn local_site_parameter_flows_to_overlay() {
        let network = test_network("my-net");
        let overlay = render_routing_overlay(&network, &[], &[], "site-a").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.local_site, "site-a",
            "local_site parameter must appear verbatim in the overlay"
        );
        assert_eq!(overlay.network, "my-net", "network field must be the network name");
    }

    #[test]
    fn different_local_site_per_call_produces_different_overlays() {
        // Simulates two gateways in the same network declaring different local sites.
        let network = test_network("my-net");
        let overlay_a = render_routing_overlay(&network, &[], &[], "site-a").unwrap_or_else(|_| std::process::abort());
        let overlay_b = render_routing_overlay(&network, &[], &[], "site-b").unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay_a.local_site, "site-a", "gateway A must identify site-a");
        assert_eq!(overlay_b.local_site, "site-b", "gateway B must identify site-b");
        assert_eq!(
            overlay_a.network, overlay_b.network,
            "both overlays belong to the same network"
        );
    }

    #[test]
    fn json_candidate_has_correct_fields() {
        let network = test_network("my-net");
        let provider = test_provider("prov-a", "my-net", &["granite-3.3-8b"]);
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        let value = overlay_json_from_cm(&build_cm(&overlay, "my-net", "gw"));
        let c = value
            .get("candidates")
            .and_then(serde_json::Value::as_array)
            .and_then(|a| a.first())
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            c.get("kind").and_then(serde_json::Value::as_str),
            Some("inference_model")
        );
        assert_eq!(
            c.get("name").and_then(serde_json::Value::as_str),
            Some("granite-3.3-8b")
        );
        assert_eq!(c.get("site").and_then(serde_json::Value::as_str), Some("prov-a"));
        assert_eq!(c.get("cluster").and_then(serde_json::Value::as_str), Some("prov-a"));
        assert_eq!(c.get("fresh").and_then(serde_json::Value::as_bool), Some(true));
    }

    // -----------------------------------------------------------------------
    // Error paths — missing names (items 14–15 per coverage policy)
    // -----------------------------------------------------------------------

    #[test]
    fn missing_network_name_returns_error() {
        // GridNetwork with no metadata.name must produce an error.
        let network: GridNetwork = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridNetwork",
            "metadata": {},
            "spec": { "seeds": [] }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let result = render_routing_overlay(&network, &[], &[], "local");
        assert!(result.is_err(), "network without metadata.name must return an error");
    }

    #[test]
    fn missing_provider_name_returns_error() {
        // InferenceProvider with no metadata.name must produce an error.
        let network = test_network("net");
        let provider: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": {},
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": [{"name": "model"}]
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let result = render_routing_overlay(&network, &[], &[provider], "local");
        assert!(
            result.is_err(),
            "InferenceProvider without metadata.name must return an error"
        );
    }

    // -----------------------------------------------------------------------
    // build_grid_state_with_metrics — integration seam for live metrics
    // -----------------------------------------------------------------------

    #[test]
    fn live_metrics_queue_depth_affects_ordering() {
        // Two local providers with equal locality and no cost difference.
        // Without metrics they are tied and fall back to alphabetical order.
        // With metrics the high-queue provider scores lower and yields the lead.
        let provider_busy = test_provider_with_backend_kind("provider-busy", "net", "local");
        let provider_idle = test_provider_with_backend_kind("provider-idle", "net", "local");

        let mut metrics: HashMap<&str, scoring::BackendMetrics> = HashMap::new();
        metrics.insert(
            "provider-busy",
            scoring::BackendMetrics::new(0.0, true, 0.0, 0.0, 0.0, 0.9),
        );
        metrics.insert(
            "provider-idle",
            scoring::BackendMetrics::new(0.0, true, 0.0, 0.0, 0.0, 0.1),
        );

        let providers_for_ordering = [provider_busy, provider_idle];
        let ordering = provider_ordering_scores("net", &providers_for_ordering, None, Some(&metrics));

        let busy_score = ordering["provider-busy"];
        let idle_score = ordering["provider-idle"];
        assert!(
            idle_score > busy_score,
            "idle provider (queue 0.1) must score higher than busy provider (queue 0.9), \
             got idle={idle_score}, busy={busy_score}"
        );
    }

    #[test]
    fn no_metrics_map_preserves_static_ordering() {
        // Passing None for metrics must produce the same result as the current
        // static-only path (locality and cost only).
        let local = test_provider_with_backend_kind("prov-local", "net", "local");
        let api = test_provider_with_backend_kind("prov-api", "net", "api_provider");
        let ps_static = [local.clone(), api.clone()];
        let ordering_static = provider_ordering_scores("net", &ps_static, None, None);
        let ps_empty = [local, api];
        let ordering_no_metrics = provider_ordering_scores("net", &ps_empty, None, Some(&HashMap::new()));
        assert_eq!(
            ordering_static["prov-local"], ordering_no_metrics["prov-local"],
            "empty metrics map must yield same score as None"
        );
        assert_eq!(
            ordering_static["prov-api"], ordering_no_metrics["prov-api"],
            "empty metrics map must yield same score as None"
        );
    }

    // -----------------------------------------------------------------------
    // routing_cluster_ref — overlay identity override
    // -----------------------------------------------------------------------

    fn test_provider_with_routing_cluster_ref(
        name: &str,
        network: &str,
        routing_ref: Option<&str>,
    ) -> InferenceProvider {
        let mut spec = serde_json::json!({
            "gridNetworkRef": network,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://localhost:8000",
            "models": [{ "name": "model-x" }]
        });
        if let Some(r) = routing_ref {
            spec["routingClusterRef"] = serde_json::Value::String(r.to_owned());
        }
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": spec
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn absent_routing_cluster_ref_uses_metadata_name() {
        let provider = test_provider_with_routing_cluster_ref("prov-a", "net", None);
        assert_eq!(
            routing_identity(&provider),
            Some("prov-a"),
            "absent ref must fall back to metadata.name"
        );
    }

    #[test]
    fn routing_cluster_ref_overrides_identity() {
        let provider = test_provider_with_routing_cluster_ref("prov-a", "net", Some("gateway-site-x"));
        assert_eq!(
            routing_identity(&provider),
            Some("gateway-site-x"),
            "configured ref must override metadata.name"
        );
    }

    #[test]
    fn empty_routing_cluster_ref_falls_back_to_metadata_name() {
        let provider = test_provider_with_routing_cluster_ref("prov-a", "net", Some(""));
        assert_eq!(
            routing_identity(&provider),
            Some("prov-a"),
            "empty ref must fall back to metadata.name"
        );
    }

    #[test]
    fn routing_cluster_ref_appears_in_candidate_cluster() {
        let network = test_network("net");
        let provider = test_provider_with_routing_cluster_ref("prov-a", "net", Some("gateway-site-x"));
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 1, "one candidate");
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("gateway-site-x"),
            "candidate.cluster must equal routingClusterRef"
        );
    }

    #[test]
    fn routing_cluster_ref_appears_in_candidate_site_phase1() {
        // In Phase 1 (no GridSites), site = routingClusterRef.
        let network = test_network("net");
        let provider = test_provider_with_routing_cluster_ref("prov-a", "net", Some("site-x"));
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.first().map(|c| c.site.as_str()),
            Some("site-x"),
            "candidate.site must equal routingClusterRef in Phase 1 (no sites)"
        );
    }

    #[test]
    fn routing_cluster_ref_applies_to_all_models() {
        let network = test_network("net");
        let provider: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": "prov-a" },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "routingClusterRef": "gateway-site-x",
                "models": [{ "name": "model-a" }, { "name": "model-b" }]
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 2, "two model candidates");
        assert!(
            overlay.candidates.iter().all(|c| c.cluster == "gateway-site-x"),
            "all candidates must use routingClusterRef"
        );
    }

    #[test]
    fn dedup_uses_routing_cluster_ref() {
        // Two identical calls (same kind/name/site/cluster after applying ref) are deduped.
        let network = test_network("net");
        let p1 = test_provider_with_routing_cluster_ref("prov-a", "net", Some("site-x"));
        let p2 = test_provider_with_routing_cluster_ref("prov-b", "net", Some("site-x"));
        // Both produce (kind=inference_model, name=model-x, site=site-x, cluster=site-x)
        // They share the same cluster and site, so after dedup there should be ONE entry.
        let overlay =
            render_routing_overlay(&network, &[], &[p1, p2], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "identical (kind, name, site, cluster) after ref override must be deduped to one"
        );
    }

    #[test]
    fn unavailable_with_routing_cluster_ref_is_excluded() {
        let network = test_network("net");
        let provider: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": "prov-a" },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "routingClusterRef": "site-x",
                "models": [{ "name": "model-x" }]
            },
            "status": { "phase": "Unavailable", "matchingSites": [], "observedGeneration": 0 }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert!(
            overlay.candidates.is_empty(),
            "Unavailable must be excluded even with routingClusterRef"
        );
    }

    #[test]
    fn degraded_with_routing_cluster_ref_has_fresh_false() {
        let network = test_network("net");
        let provider: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": "prov-a" },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "routingClusterRef": "site-x",
                "models": [{ "name": "model-x" }]
            },
            "status": { "phase": "Degraded", "matchingSites": [], "observedGeneration": 0 }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 1, "Degraded must be included");
        assert!(
            overlay.candidates.first().is_some_and(|c| !c.fresh),
            "Degraded candidate must have fresh=false"
        );
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("site-x"),
            "cluster must use routingClusterRef even for Degraded"
        );
    }

    #[test]
    fn scoring_order_works_with_routing_cluster_ref() {
        // local provider with routingClusterRef "site-x" must still outscore api_provider.
        let network = test_network("net");
        let local_with_ref = test_provider_with_routing_cluster_ref("prov-local", "net", Some("site-x"));
        let api_provider: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": "prov-api" },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "anthropic",
                "backendKind": "api_provider",
                "endpoint": "https://api.example.com",
                "models": [{ "name": "model-z" }]
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let overlay = render_routing_overlay(&network, &[], &[api_provider, local_with_ref], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 2, "two candidates");
        // Local (cluster=site-x via ref) must rank before api_provider.
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("site-x"),
            "local provider with routingClusterRef must rank before api_provider"
        );
    }

    #[test]
    fn build_grid_state_with_metrics_attaches_metrics_to_correct_provider() {
        let providers = vec![
            test_provider_with_backend_kind("prov-a", "net", "local"),
            test_provider_with_backend_kind("prov-b", "net", "local"),
        ];
        let mut metrics: HashMap<&str, scoring::BackendMetrics> = HashMap::new();
        metrics.insert("prov-a", scoring::BackendMetrics::new(0.0, true, 0.0, 0.0, 0.0, 0.8));

        let state = build_grid_state_with_metrics("net", &providers, Some(&metrics));
        assert!(state.metrics("prov-a").is_some(), "prov-a must have metrics attached");
        assert!(
            state.metrics("prov-b").is_none(),
            "prov-b must have no metrics (not in map)"
        );
    }
}
