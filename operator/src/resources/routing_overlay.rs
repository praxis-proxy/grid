//! Pure overlay renderer for Praxis `intelligent_route` routing candidates.
//!
//! Converts [`GridNetwork`], [`GridSite`], and [`InferenceProvider`]
//! CRDs into a `RoutingOverlay` that is serialised into a Kubernetes
//! `ConfigMap`.  Praxis reads this `ConfigMap` to configure its
//! `intelligent_route` filter with routing candidates.
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
//! - Data keys: `routing-config.json` (legacy) + `routing-overlay.json` (versioned envelope)
//! - Serialization failures are returned as errors, not silently defaulted.
//!
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork
//! [`GridSite`]: crate::crd::grid_site::GridSite
//! [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crdt;
use k8s_openapi::api::core::v1::ConfigMap;
use serde::{Deserialize, Serialize};

use crate::{
    crd::{
        auth::{AccessPolicy, AuthStrategy},
        grid_network::GridNetwork,
        grid_site::GridSite,
        inference_provider::{InferenceProvider, ProviderPhase},
    },
    resources::geography::{AdmissionState, LocalityTier},
    swim::{MemberStatus, MembershipSnapshot},
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

/// Map a remote [`crdt::ProviderPhase`] to an overlay `fresh` flag.
///
/// Returns `None` when the provider should be excluded from the overlay
/// ([`crdt::ProviderPhase::Unavailable`]); `Some(false)` for
/// [`crdt::ProviderPhase::Degraded`]; `Some(true)` for all other phases
/// ([`crdt::ProviderPhase::Available`], [`crdt::ProviderPhase::Pending`]).
pub(crate) fn crdt_phase_to_fresh(phase: &crdt::ProviderPhase) -> Option<bool> {
    match phase {
        crdt::ProviderPhase::Unavailable => None,
        crdt::ProviderPhase::Degraded => Some(false),
        crdt::ProviderPhase::Available | crdt::ProviderPhase::Pending => Some(true),
    }
}

/// Convert a [`crdt::ProviderMetricsSnapshot`] to [`scoring::BackendMetrics`].
///
/// # Defaults for absent fields
///
/// | Field | Missing default | Rationale |
/// |---|---|---|
/// | `error_rate` | `0.0` | No evidence of errors → do not penalise |
/// | `healthy` | `true` | Assume reachable until evidence of failure |
/// | `kv_cache_utilization` | `0.5` | Neutral (no signal) |
/// | `latency_p99_ms` | `2500.0` | Neutral: `1.0 - 2500/5000 = 0.5` latency score |
/// | `prefix_cache_hit_ratio` | `0.5` | Neutral (no signal) |
/// | `queue_depth` | `0.5` | Neutral (no signal) |
///
/// Using `2500.0` for missing latency (rather than `0.0` or `0.5`) matches the
/// local Prometheus scrape path (`PartialMetrics::into_backend_metrics`) and
/// avoids inflating the latency score of remote providers whose latency has not
/// yet been observed.
///
/// # Sanitize remote signals
///
/// CRDT values are produced by remote operators which may run a different
/// software version.  Non-finite values fall back to the same defaults as
/// absent values, ratio signals are clamped to `[0.0, 1.0]`, and latency is
/// clamped to `≥ 0.0` so that a misbehaving or outdated remote site cannot
/// corrupt overlay scoring through invalid or out-of-range values.
pub(crate) fn crdt_metrics_to_backend(m: &crdt::ProviderMetricsSnapshot) -> scoring::BackendMetrics {
    scoring::BackendMetrics {
        error_rate: finite_or(m.error_rate, 0.0).clamp(0.0, 1.0),
        healthy: m.healthy.unwrap_or(true),
        kv_cache_utilization: finite_or(m.kv_cache_utilization, UNMAPPED_NEUTRAL_SIGNAL).clamp(0.0, 1.0),
        latency_p99_ms: finite_or(m.latency_p99_ms, NEUTRAL_LATENCY_MS).max(0.0),
        prefix_cache_hit_ratio: finite_or(m.prefix_cache_hit_ratio, UNMAPPED_NEUTRAL_SIGNAL).clamp(0.0, 1.0),
        queue_depth: finite_or(m.queue_depth, UNMAPPED_NEUTRAL_SIGNAL).clamp(0.0, 1.0),
    }
}

/// Return `value` if it is `Some` and finite, otherwise return `default`.
///
/// Drops NaN and ±Inf so they cannot corrupt downstream scoring.
fn finite_or(value: Option<f64>, default: f64) -> f64 {
    value.filter(|v| v.is_finite()).unwrap_or(default)
}

/// Convert one remote CRDT provider record to [`RoutingCandidate`]s, one per model.
///
/// Returns an empty `Vec` when the provider phase is
/// [`crdt::ProviderPhase::Unavailable`] (excluded from routing) or when
/// the provider has no models configured.
///
/// The `site` and `cluster` fields are taken directly from the CRDT record:
/// `site_id` → `candidate.site`; `routing_cluster` → `candidate.cluster`.
///
/// Remote CRDT providers do not carry credential references; `credential` is
/// always `None`.  Credentials are a local-operator concern derived from
/// `InferenceProvider.spec.auth`.
pub(crate) fn remote_crdt_provider_to_candidates(provider: &crdt::ProviderState) -> Vec<RoutingCandidate> {
    let Some(fresh) = crdt_phase_to_fresh(&provider.phase) else {
        return Vec::new();
    };
    provider
        .models
        .iter()
        .map(|model| RoutingCandidate {
            kind: CANDIDATE_KIND.to_owned(),
            name: model.clone(),
            site: provider.site_id.clone(),
            cluster: provider.routing_cluster.clone(),
            fresh,
            credential: None,
            stable_id: None,
            admission_state: None,
            selection_tier: None,
            score: None,
            score_breakdown: None,
            rank: None,
        })
        .collect()
}

/// Derive a [`ProjectedCredential`] from a provider's `spec.auth`.
///
/// Returns `Some` only when all of the following hold:
/// - `auth.manual` is `false`
/// - `auth.strategy` is [`AuthStrategy::BearerToken`]
/// - `auth.secretRef` is present with non-blank `name`, `namespace`, and `key`
///
/// Returns `None` for absent auth, `manual = true`, unsupported strategies, or
/// an incomplete `secretRef`.  Incomplete refs are silently ignored here because
/// [`crate::resources::credentials::credential_plan_from_auth`] drives the
/// `Unavailable` phase for validation failures; this function only emits a
/// reference when the ref is fully usable.
pub(crate) fn projected_credential_from_provider(provider: &InferenceProvider) -> Option<ProjectedCredential> {
    let auth = provider.spec.auth.as_ref()?;
    if auth.manual || auth.strategy != AuthStrategy::BearerToken {
        return None;
    }
    let secret_ref = auth.secret_ref.as_ref()?;
    let name = secret_ref.name.trim();
    let namespace = secret_ref.namespace.trim();
    let key = secret_ref.key.as_deref().unwrap_or("").trim();
    if name.is_empty() || namespace.is_empty() || key.is_empty() {
        return None;
    }
    Some(ProjectedCredential {
        strategy: "bearer_token".to_owned(),
        secret_ref: ProjectedCredentialRef {
            name: name.to_owned(),
            namespace: namespace.to_owned(),
            key: key.to_owned(),
        },
    })
}

/// Convert a remote CRDT provider to a [`scoring::BackendConfig`] for overlay scoring.
///
/// Returns `None` for [`crdt::ProviderPhase::Unavailable`] phase.  The
/// `backend_kind` is mapped with locality-aware semantics: `"local"` and
/// `"remote"` both become [`scoring::BackendKind::Remote`] because a
/// locally-deployed provider at a remote site is still remote from this
/// operator's perspective.  `"cloud_managed"` and `"api_provider"` are
/// preserved since their access patterns are provider-type properties,
/// not site-local ones.
///
/// The `endpoint` field is empty for remote CRDT providers — only the
/// scoring locality and metrics matter; the consumer gateway uses
/// `routing_cluster` for connection, not the endpoint in the scoring record.
pub(crate) fn remote_crdt_provider_to_backend_config(provider: &crdt::ProviderState) -> Option<scoring::BackendConfig> {
    if provider.phase == crdt::ProviderPhase::Unavailable {
        return None;
    }
    let kind = match provider.backend_kind.as_str() {
        "cloud_managed" => scoring::BackendKind::CloudManaged,
        "api_provider" => scoring::BackendKind::ApiProvider,
        _ => scoring::BackendKind::Remote,
    };
    Some(scoring::BackendConfig::new(
        provider.routing_cluster.clone(),
        0.0, // cost unknown; treated as free → max cost score
        0.0,
        String::new(), // endpoint not used for scoring
        kind,
        scoring::ProviderKind::OpenAi, // unknown; default matches self-hosted convention
        None,                          // region: not available from CRDT record
    ))
}

/// Neutral signal score when no live metrics are available; mirrors
/// `scoring::DEFAULT_SIGNAL_SCORE` (not exported) so unmapped providers score
/// the same as providers with no observed metrics.
const UNMAPPED_NEUTRAL_SIGNAL: f64 = 0.5;

/// Neutral latency value (ms) applied when no latency observation is available.
///
/// Chosen so that the scoring formula produces a neutral latency score of 0.5:
/// `1.0 - NEUTRAL_LATENCY_MS / MAX_LATENCY_MS = 1.0 - 2500 / 5000 = 0.5`.
///
/// Both the local Prometheus scrape path (`PartialMetrics::into_backend_metrics`)
/// and the CRDT remote path (`crdt_metrics_to_backend`) use this constant so that
/// providers with no latency observation score neutrally on the latency signal in
/// both paths.
const NEUTRAL_LATENCY_MS: f64 = 2500.0;

// ---------------------------------------------------------------------------
// Stale candidate expiry policy
// ---------------------------------------------------------------------------

/// Policy controlling when `fresh=false` routing candidates are garbage-collected
/// from the overlay.
///
/// # Design
///
/// The overlay retains stale (`fresh=false`) candidates for observability and
/// to allow the data plane to prefer a healthy fallback without losing sight of
/// the degraded peer.  However, candidates that have been dead for longer than
/// the TTL should be evicted to prevent unbounded overlay growth.
///
/// # Runtime age source
///
/// `MemberRecord.age_secs` is populated by the SWIM runtime for members that
/// have transitioned to `Dead` or `Suspect`.  `Alive` members report age `0`.
/// The age is derived from an internal `Instant`, not from CRDT provider
/// revisions.
///
/// `age_secs = 0` on a `Dead`/`Suspect` member is treated conservatively as
/// unknown by `dead_or_suspect_age_secs`.  This avoids evicting candidates in
/// the first sub-second after transition and protects callers that construct
/// snapshots without runtime age data.
///
/// # TTL configuration
///
/// `GridNetwork.spec.staleCandidateTtlSeconds` is converted into this policy by
/// the `stale_policy_from_spec` helper.  The field is optional; when absent,
/// `StaleCandidatePolicy::default()` uses `dead_member_ttl_secs = None`, so the
/// filter is wired but retains all stale candidates.
#[derive(Clone, Copy, Debug, Default)]
pub struct StaleCandidatePolicy {
    /// Maximum age (seconds) for a stale (`fresh=false`) candidate.
    ///
    /// `None` means retain indefinitely — the conservative default until a
    /// product-level TTL setting is added.
    pub dead_member_ttl_secs: Option<u64>,
}

/// Build a [`StaleCandidatePolicy`] from `GridNetwork.spec.staleCandidateTtlSeconds`.
///
/// # Mapping
///
/// | `spec.staleCandidateTtlSeconds` | `StaleCandidatePolicy.dead_member_ttl_secs` |
/// |---|---|
/// | `None` (absent) | `None` — retain indefinitely (default no-op) |
/// | `Some(0)` | `None` — defensive guard; the CRD schema rejects `0` |
/// | `Some(n)` where `n >= 1` | `Some(n as u64)` |
///
/// # Pure function
///
/// This function is side-effect-free and depends only on its input.
/// Call it once per reconcile; the result is passed to [`apply_stale_gc_filter`].
#[must_use]
pub(crate) fn stale_policy_from_spec(ttl_seconds: Option<u32>) -> StaleCandidatePolicy {
    // The CRD schema rejects zero, but keep the pure helper defensive so
    // malformed data from tests, downgraded resources, or non-API callers cannot
    // cause immediate eviction of all stale candidates.
    let effective_ttl = ttl_seconds.and_then(|s| (s > 0).then(|| u64::from(s)));
    StaleCandidatePolicy {
        dead_member_ttl_secs: effective_ttl,
    }
}

/// Decide whether a `fresh=false` routing candidate should be retained or evicted.
///
/// This is the single authoritative place where the GC / expiry policy is
/// encoded.  All overlay rendering paths should call this function to decide
/// whether to include a degraded remote candidate.
///
/// # Policy rules
///
/// 1. **Fresh candidates** (`fresh = true`) are **always retained** regardless of age or policy.  Local candidates and
///    healthy remote candidates must never be evicted by a stale-peer GC policy.
///
/// 2. **No TTL configured** (`policy.dead_member_ttl_secs = None`) → retain. The conservative default until a product
///    TTL setting is added.
///
/// 3. **No age data** (`dead_age_secs = None`) → retain. This includes `age_secs = 0` for Dead/Suspect members, which
///    means the transition happened less than one second ago or the caller provided a synthetic snapshot without age.
///
/// 4. **Age below TTL** (`dead_age_secs < ttl`) → retain. The peer has been dead for less than the configured window.
///    The `fresh=false` signal is enough to deprioritise it.
///
/// 5. **Age at or above TTL** (`dead_age_secs >= ttl`) → evict. The peer has been dead long enough that its overlay
///    entries add noise without adding observability value.
///
/// # Honest boundaries
///
/// This function only applies overlay-level filtering.  It does not remove
/// records from CRDT storage.  With the default policy (`ttl = None`) it is a
/// behavioral no-op.
#[must_use]
pub(crate) fn should_retain_candidate(fresh: bool, dead_age_secs: Option<u64>, policy: &StaleCandidatePolicy) -> bool {
    if fresh {
        return true; // Rule 1: fresh candidates are never evicted.
    }
    let Some(ttl) = policy.dead_member_ttl_secs else {
        return true; // Rule 2: no TTL configured → retain.
    };
    let Some(age) = dead_age_secs else {
        return true; // Rule 3: no age data → retain conservatively.
    };
    age < ttl // Rules 4 & 5: retain below TTL, evict at or above.
}

/// Return the age in seconds for which a SWIM member has been `Dead` or `Suspect`.
///
/// Returns `Some(age_secs)` only when the member is found with a
/// `Dead` or `Suspect` status and a non-zero `age_secs` (populated by the
/// SWIM runtime after the age-tracking fix).  Returns `None` when:
/// - The site is not in the snapshot (unknown peer).
/// - The member is `Alive` (no stale age relevant to GC).
/// - `age_secs` is `0` — conservatively treated as "age unknown" so stale candidates are not evicted during the first
///   sub-second after transition or when a synthetic snapshot lacks age data.
///
/// This is the bridge between the SWIM membership snapshot and the
/// [`should_retain_candidate`] GC policy function.
pub(crate) fn dead_or_suspect_age_secs(site_id: &str, membership: Option<&MembershipSnapshot>) -> Option<u64> {
    let snap = membership?;
    let member = snap.members.iter().find(|m| m.site_id == site_id)?;
    match member.status {
        MemberStatus::Dead | MemberStatus::Suspect => {
            // age_secs=0 means the transition is sub-second or the snapshot lacks
            // real runtime age data — retain conservatively.
            (member.age_secs > 0).then_some(member.age_secs)
        },
        MemberStatus::Alive => None,
    }
}

/// Filter remote CRDT providers through the stale-candidate GC policy.
///
/// For each provider:
/// - If `phase != Degraded` (i.e. `fresh=true` candidates), always retained.
/// - If `phase == Degraded` (i.e. `fresh=false` candidates), applies [`should_retain_candidate`] using the member's
///   Dead/Suspect age from the SWIM snapshot.
///
/// This function is a pure filter over the already-staleness-overridden provider
/// list produced by `apply_swim_staleness_override`.  It does **not** modify
/// CRDT storage.
///
/// When `policy.dead_member_ttl_secs` is `None` (the current default), this
/// function is a no-op — all providers are retained regardless of age.
pub(crate) fn apply_stale_gc_filter(
    providers: &[crdt::ProviderState],
    membership: Option<&MembershipSnapshot>,
    policy: &StaleCandidatePolicy,
) -> Vec<crdt::ProviderState> {
    providers
        .iter()
        .filter(|p| {
            let fresh = p.phase != crdt::ProviderPhase::Degraded;
            let age = dead_or_suspect_age_secs(&p.site_id, membership);
            should_retain_candidate(fresh, age, policy)
        })
        .cloned()
        .collect()
}

/// Compute the score breakdown equivalent to what [`scoring::score_backends`]
/// would assign to a provider whose `backend_kind` cannot be parsed.
///
/// Applies the given weights with neutral runtime signals (0.5, the scoring
/// crate's default for missing metrics) and no cost (treated as free → cost
/// signal = 1.0).  This places unmapped providers on the same numeric scale
/// as scored providers so they can be sorted in a single pass.
fn unmapped_provider_breakdown(backend_kind: &str, weights: &scoring::ScoringWeights) -> scoring::ScoreBreakdown {
    let w = weights;
    let locality = w.locality * backend_locality_score(backend_kind);
    let cost = w.cost * 1.0;
    let queue_depth = w.queue_depth * UNMAPPED_NEUTRAL_SIGNAL;
    let kv_cache = w.kv_cache * UNMAPPED_NEUTRAL_SIGNAL;
    let latency = w.latency * UNMAPPED_NEUTRAL_SIGNAL;
    let prefix_cache = w.prefix_cache * UNMAPPED_NEUTRAL_SIGNAL;
    let total = locality + cost + queue_depth + kv_cache + latency + prefix_cache;
    scoring::ScoreBreakdown {
        locality,
        queue_depth,
        kv_cache,
        prefix_cache,
        latency,
        cost,
        total,
    }
}

/// Compute per-provider ordering scores for overlay candidate sorting.
///
/// For each local [`InferenceProvider`] in `network_name` that can be mapped
/// to a [`scoring::BackendConfig`], the score is produced by
/// [`scoring::score_backends`] using the given weights and the network's
/// `local_region`.  Providers whose `backend_kind` cannot be parsed fall back
/// to [`unmapped_provider_breakdown`], which is on the same numeric scale.
///
/// Remote CRDT providers are also scored and included in the result map.
/// Their scores are derived from [`scoring::score_backends`] via
/// [`remote_crdt_provider_to_backend_config`].  Remote providers whose
/// `routing_cluster` already appears in the local map (collision) are
/// skipped.
///
/// `Unavailable` providers are excluded (they are never emitted as candidates).
/// All other phases — `Pending`, `Available`, `Degraded`, and absent status —
/// are scored and included.  The `fresh` flag is set separately per candidate
/// by [`is_candidate_fresh`] (local) or [`crdt_phase_to_fresh`] (remote).
///
/// Returns a map from routing cluster name to score.
#[expect(
    clippy::too_many_arguments,
    reason = "scoring_weights is threaded through overlay rendering without hiding the selected strategy"
)]
fn provider_ordering_scores(
    network_name: &str,
    providers: &[InferenceProvider],
    remote_crdt_providers: &[crdt::ProviderState],
    local_region: Option<&str>,
    metrics: Option<&HashMap<&str, scoring::BackendMetrics>>,
    weights: &scoring::ScoringWeights,
) -> HashMap<String, scoring::ScoreBreakdown> {
    let state = build_grid_state_with_metrics(network_name, providers, remote_crdt_providers, metrics);
    let scored = scoring::score_backends(&state, weights, local_region);
    let from_engine: HashMap<String, scoring::ScoreBreakdown> =
        scored.into_iter().map(|sb| (sb.name, sb.breakdown)).collect();

    let mut result: HashMap<String, scoring::ScoreBreakdown> = providers
        .iter()
        .filter_map(|p| {
            let cluster = routing_identity(p)?.to_owned();
            let breakdown = from_engine
                .get(cluster.as_str())
                .cloned()
                .unwrap_or_else(|| unmapped_provider_breakdown(&p.spec.backend_kind, weights));
            Some((cluster, breakdown))
        })
        .collect();

    for provider in remote_crdt_providers {
        result.entry(provider.routing_cluster.clone()).or_insert_with(|| {
            from_engine
                .get(provider.routing_cluster.as_str())
                .cloned()
                .unwrap_or_else(|| unmapped_provider_breakdown(&provider.backend_kind, weights))
        });
    }

    result
}

/// Build a [`scoring::GridState`] from local and remote CRDT providers,
/// optionally populated with scraped metrics.
///
/// Local providers that are explicitly [`ProviderPhase::Unavailable`] or that
/// belong to a different network are skipped.  Remote CRDT providers with
/// [`crdt::ProviderPhase::Unavailable`] phase are also excluded.  Duplicate
/// provider names (a CRD-level invariant violation) are silently ignored.
///
/// When `metrics` is `Some`, each local provider whose name appears in the map
/// receives live [`scoring::BackendMetrics`] via
/// [`scoring::GridState::set_metrics`].  This is the integration seam for
/// Prometheus-scraped data — pass `None` for static-only scoring.
///
/// Remote CRDT providers receive their metrics from
/// [`crdt_metrics_to_backend`], which applies neutral defaults for `None`
/// fields.
pub(crate) fn build_grid_state_with_metrics(
    network_name: &str,
    providers: &[InferenceProvider],
    remote_crdt_providers: &[crdt::ProviderState],
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
    for provider in remote_crdt_providers {
        if let Some(config) = remote_crdt_provider_to_backend_config(provider) {
            let name = config.name.clone();
            drop(state.add_backend(config));
            state.set_metrics(name, crdt_metrics_to_backend(&provider.metrics));
        }
    }
    state
}

// ---------------------------------------------------------------------------
// Access policy evaluation
// ---------------------------------------------------------------------------

/// Result of evaluating a provider's access policy against consumer site labels.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AccessPolicyResult {
    /// Access allowed: provider has no restrictions OR consumer site matches required labels.
    Allow,

    /// Access denied: provider requires specific labels but consumer site doesn't match.
    Deny,

    /// Access indeterminate: consumer site identity is unknown or ambiguous.
    /// For restricted providers this should be treated as deny (fail closed).
    Unknown,
}

/// Evaluate a provider's access policy against consumer site labels.
///
/// This function implements the provider access-policy enforcement logic:
/// - Empty `accessPolicy.siteSelector.matchLabels` means allow all (preserve existing default)
/// - Non-empty means consumer site must have matching labels
/// - Consumer site identity absence/ambiguity with restricted provider fails closed
///
/// # Arguments
///
/// * `access_policy` - The provider's access policy configuration
/// * `consumer_site_labels` - Labels of the consumer site, if known
///
/// # Returns
///
/// * `AccessPolicyResult::Allow` - Access permitted
/// * `AccessPolicyResult::Deny` - Access explicitly denied due to label mismatch
/// * `AccessPolicyResult::Unknown` - Consumer identity unknown/ambiguous
///
/// # Design Notes
///
/// This function is separate from placement logic (`siteSelector`) and evaluates
/// authorization policy (`accessPolicy`) independently. The provider's `siteSelector`
/// controls where the provider is placed; the `accessPolicy` controls which consumer
/// sites may use it.
pub(crate) fn evaluate_access_policy(
    access_policy: &AccessPolicy,
    consumer_site_labels: Option<&BTreeMap<String, String>>,
) -> AccessPolicyResult {
    let required_labels = &access_policy.site_selector.match_labels;

    // Empty access policy means allow all - preserve existing behavior
    if required_labels.is_empty() {
        return AccessPolicyResult::Allow;
    }

    // Provider has restrictions - consumer site must be known
    let Some(site_labels) = consumer_site_labels else {
        return AccessPolicyResult::Unknown;
    };

    // Check if all required labels are present and match
    let matches = required_labels
        .iter()
        .all(|(required_key, required_value)| site_labels.get(required_key) == Some(required_value));

    if matches {
        AccessPolicyResult::Allow
    } else {
        AccessPolicyResult::Deny
    }
}

// ---------------------------------------------------------------------------
// Site resolution
// ---------------------------------------------------------------------------

/// Resolution of matching sites for a single provider.
///
/// This enum distinguishes two semantically different "empty" cases so that
/// the candidate generator cannot accidentally apply the provider-name fallback when
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

/// A reference to a Kubernetes Secret holding a credential value.
///
/// Contains only locating information — **never** the credential value itself.
/// Safe to persist in a `ConfigMap`.  The xtask harness resolves the token
/// from the referenced Secret; Praxis will eventually do this natively once
/// native Secret-ref support lands in the `intelligent_route` filter.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectedCredentialRef {
    /// Secret name in the cluster.
    pub name: String,

    /// Secret namespace.
    pub namespace: String,

    /// Key within `Secret.data` that holds the credential bytes.
    pub key: String,
}

/// A credential reference projected alongside a routing candidate.
///
/// Carries the authentication strategy and a [`ProjectedCredentialRef`].
/// Never contains the credential value.
///
/// # Security
///
/// The token value is **never** stored here.  The operator writes only the
/// Secret reference into the `ConfigMap`; callers resolve the actual token
/// from the Secret at use time.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectedCredential {
    /// Authentication strategy.  Currently only `"bearer_token"` is emitted.
    pub strategy: String,

    /// Reference to the Secret holding the credential.
    pub secret_ref: ProjectedCredentialRef,
}

/// A single routing candidate for the Praxis `intelligent_route` filter.
///
/// Each candidate represents one (model, site) pair offered by a provider.
/// Praxis uses the candidate list to select a backend cluster for each
/// inference request.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RoutingCandidate {
    /// Candidate kind.  `"inference_model"` for inference providers; other
    /// variants (e.g. `"mcp_tool"`) are defined by Praxis `intelligent_route`.
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

    /// Whether this candidate should be treated as fresh by the data plane.
    ///
    /// Local candidates are `false` only when the provider status is
    /// explicitly `Degraded`. Remote CRDT candidates derive this from their
    /// CRDT provider phase; `Degraded` maps to `false`.
    pub fresh: bool,

    /// Credential reference projected by the operator, when `spec.auth`
    /// declares a bearer-token strategy.
    ///
    /// Contains only the Secret reference — **never** the token value.
    /// The xtask harness resolves the token from the Secret at config-generation
    /// time.  Praxis will eventually consume this reference natively.
    ///
    /// `None` for providers with `manual`, absent, or unsupported-strategy auth.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential: Option<ProjectedCredential>,

    /// Deterministic stable ID for consumer-side session binding.
    ///
    /// Computed as `fnv1a_hex8("{kind}/{name}/{site}/{cluster}")`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_id: Option<String>,

    /// Bounded admission state from provider health and capacity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admission_state: Option<AdmissionState>,

    /// Locality tier between the consumer gateway and this candidate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selection_tier: Option<LocalityTier>,

    /// Weighted score from the production scoring engine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,

    /// Per-signal weighted contributions from the scoring engine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_breakdown: Option<scoring::ScoreBreakdown>,

    /// Zero-based position in the final sorted overlay.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rank: Option<u32>,
}

/// The full routing overlay for a single [`GridNetwork`].
///
/// Serialised as JSON under the `routing-config.json` key of the
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

    /// Routing candidates, ordered by admission state, locality tier, score,
    /// freshness, then alphabetical tiebreak.
    pub candidates: Vec<RoutingCandidate>,

    /// RFC 3339 timestamp of when this overlay was rendered.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generated_at: Option<String>,
}

// ---------------------------------------------------------------------------
// Admission and enrichment
// ---------------------------------------------------------------------------

/// Build admission state for all providers keyed by routing identity.
///
/// Local providers are looked up in `metrics` by [`routing_identity`].
/// Remote CRDT providers use their replicated metrics snapshot.
/// Providers without metrics default to [`AdmissionState::NewAndExisting`].
fn build_admission_map(
    network_name: &str,
    providers: &[InferenceProvider],
    remote_crdt_providers: &[crdt::ProviderState],
    metrics: Option<&HashMap<&str, scoring::BackendMetrics>>,
) -> HashMap<String, AdmissionState> {
    let mut map = HashMap::new();
    for provider in providers {
        if provider.spec.grid_network_ref != network_name || is_explicitly_unavailable(provider) {
            continue;
        }
        if let Some(key) = routing_identity(provider) {
            let m = metrics.and_then(|mmap| mmap.get(key));
            map.insert(key.to_owned(), super::geography::derive_admission_state(m));
        }
    }
    for provider in remote_crdt_providers {
        if provider.phase == crdt::ProviderPhase::Unavailable {
            continue;
        }
        let m = crdt_metrics_to_backend(&provider.metrics);
        map.insert(
            provider.routing_cluster.clone(),
            super::geography::derive_admission_state(Some(&m)),
        );
    }
    map
}

/// Enrich candidates with stable ID, locality tier, and admission state.
///
/// Must be called before sorting so the sort comparator can use the
/// enriched fields.  Rank is **not** assigned here — it depends on the
/// final post-sort position.
fn enrich_candidates(
    candidates: &mut [RoutingCandidate],
    local_site: &str,
    sites: &[GridSite],
    network_name: &str,
    admission_map: &HashMap<String, AdmissionState>,
) {
    for c in candidates.iter_mut() {
        c.stable_id = Some(super::geography::compute_stable_id(
            &c.kind, &c.name, &c.site, &c.cluster,
        ));
        c.selection_tier = Some(super::geography::derive_locality_tier(
            local_site,
            &c.site,
            sites,
            network_name,
        ));
        c.admission_state = Some(
            admission_map
                .get(c.cluster.as_str())
                .copied()
                .unwrap_or(AdmissionState::NewAndExisting),
        );
    }
}

/// Sort key for admission state.
///
/// Lower values rank first. Keep this explicit instead of relying on enum
/// declaration order so future enum reshuffling cannot silently change the
/// routing contract.
fn admission_sort_key(state: Option<AdmissionState>) -> u8 {
    match state.unwrap_or(AdmissionState::NewAndExisting) {
        AdmissionState::NewAndExisting => 0,
        AdmissionState::ExistingOnly => 1,
        AdmissionState::Excluded => 2,
    }
}

/// Sort key for locality tier.
///
/// Lower values rank first. Unknown geography is last, allowing older
/// deployments without `GridSite` geography to fall through to score order
/// because every candidate receives the same key.
fn locality_sort_key(tier: Option<LocalityTier>) -> u8 {
    match tier.unwrap_or(LocalityTier::Unknown) {
        LocalityTier::SameSite => 0,
        LocalityTier::SameZone => 1,
        LocalityTier::SameRegion => 2,
        LocalityTier::CrossRegion => 3,
        LocalityTier::Unknown => 4,
    }
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
/// Provider access policy enforcement: Each provider's `spec.accessPolicy`
/// is evaluated against the consumer site's labels. Providers with non-empty
/// access policies will only generate candidates if the consumer site matches
/// the required labels. Empty access policies allow all consumers (preserving
/// existing behavior).
///
/// Candidates are enriched, filtered, and sorted before being written. The
/// final order depends on the network's
/// [`RoutingPolicy`](crate::crd::grid_network::RoutingPolicy):
///
/// **`GeographyFirst`** (default):
/// 1. admission state: `new_and_existing` before `existing_only`;
/// 2. geography tier: same site, same zone, same region, cross region;
/// 3. scoring engine score, descending;
/// 4. `fresh=true` before `fresh=false`;
/// 5. deterministic `(site, name, cluster)` tiebreak.
///
/// **`ScoreFirst`**:
/// 1. admission state: `new_and_existing` before `existing_only`;
/// 2. `fresh=true` before `fresh=false`;
/// 3. scoring engine score, descending (metrics can outrank locality);
/// 4. geography tier tiebreak: same site, same zone, same region, cross region;
/// 5. deterministic `(site, name, cluster)` tiebreak.
///
/// Scores are computed by [`scoring::score_backends`] using
/// [`scoring::ScoringWeights::default`] and the network's `spec.region` as the
/// locality context.  Providers whose `backend_kind` cannot be parsed fall back
/// to an equivalent same-scale locality estimate.
///
/// The `metrics` parameter accepts a map from provider routing identity
/// (the value of `spec.routingClusterRef`, or `metadata.name` when absent) to
/// [`scoring::BackendMetrics`] produced by scraping and parsing Prometheus
/// `/metrics` endpoints.  When `Some`, providers present in the map receive
/// live signal data (queue depth, KV-cache utilisation, latency P99,
/// prefix-cache hit ratio) that shifts their scores relative to equal-locality
/// peers.  When `None`, scoring uses locality and cost only — identical to the
/// static path.
///
/// # Metric wiring
///
/// The controller builds the `metrics` map by scraping each provider that has
/// `spec.metricsConfig` set via the `provider_metrics` module.  Providers
/// without `metricsConfig` are omitted from the map and score on static
/// signals only.
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
#[expect(
    clippy::too_many_arguments,
    reason = "seven parameters represent distinct overlay inputs; a wrapper struct would obscure the data flow"
)]
#[expect(
    clippy::too_many_lines,
    reason = "sequential render steps: ordering, collect, enrich, filter, sort, dedup, rank; splitting would hide the pipeline"
)]
pub fn render_routing_overlay(
    network: &GridNetwork,
    sites: &[GridSite],
    providers: &[InferenceProvider],
    remote_crdt_providers: &[crdt::ProviderState],
    local_site: &str,
    metrics: Option<&HashMap<&str, scoring::BackendMetrics>>,
    generated_at: Option<&str>,
    weights: &scoring::ScoringWeights,
) -> Result<RoutingOverlay, String> {
    let network_name = network
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| "GridNetwork has no name".to_owned())?;

    let ordering = provider_ordering_scores(
        network_name,
        providers,
        remote_crdt_providers,
        network.spec.region.as_deref(),
        metrics,
        weights,
    );

    let admission_map = build_admission_map(network_name, providers, remote_crdt_providers, metrics);

    // Find the consumer site to get its labels for access policy evaluation
    let consumer_site_labels = sites
        .iter()
        .find(|site| site.metadata.name.as_deref() == Some(local_site) && site.spec.grid_network_ref == network_name)
        .and_then(|site| site.metadata.labels.as_ref());

    let mut candidates = collect_candidates(network_name, sites, providers, consumer_site_labels)?;
    for provider in remote_crdt_providers {
        let access_policy = crdt_access_policy_to_operator(&provider.access_policy);
        let access_result = evaluate_access_policy(&access_policy, consumer_site_labels);
        match access_result {
            AccessPolicyResult::Allow => {
                candidates.extend(remote_crdt_provider_to_candidates(provider));
            },
            AccessPolicyResult::Deny => {
                tracing::debug!(
                    provider_id = %provider.provider_id,
                    site_id = %provider.site_id,
                    network = network_name,
                    "remote access policy denied: consumer site labels do not match provider requirements"
                );
            },
            AccessPolicyResult::Unknown => {
                if provider.access_policy.match_labels.is_empty() {
                    candidates.extend(remote_crdt_provider_to_candidates(provider));
                } else {
                    tracing::debug!(
                        provider_id = %provider.provider_id,
                        site_id = %provider.site_id,
                        network = network_name,
                        "remote access policy failed closed: consumer site identity unknown for restricted provider"
                    );
                }
            },
        }
    }

    enrich_candidates(&mut candidates, local_site, sites, network_name, &admission_map);
    candidates.retain(|c| c.admission_state != Some(AdmissionState::Excluded));

    let policy = network
        .spec
        .routing_policy
        .unwrap_or(crate::crd::grid_network::RoutingPolicy::GeographyFirst);
    let score_of = |cluster: &str| ordering.get(cluster).map_or(DEFAULT_LOCALITY, |bd| bd.total);
    match policy {
        crate::crd::grid_network::RoutingPolicy::GeographyFirst => {
            candidates.sort_by(|a, b| {
                admission_sort_key(a.admission_state)
                    .cmp(&admission_sort_key(b.admission_state))
                    .then(locality_sort_key(a.selection_tier).cmp(&locality_sort_key(b.selection_tier)))
                    .then_with(|| score_of(&b.cluster).total_cmp(&score_of(&a.cluster)))
                    .then(b.fresh.cmp(&a.fresh))
                    .then(a.site.cmp(&b.site))
                    .then(a.name.cmp(&b.name))
                    .then(a.cluster.cmp(&b.cluster))
            });
        },
        crate::crd::grid_network::RoutingPolicy::ScoreFirst => {
            candidates.sort_by(|a, b| {
                admission_sort_key(a.admission_state)
                    .cmp(&admission_sort_key(b.admission_state))
                    .then(b.fresh.cmp(&a.fresh))
                    .then_with(|| score_of(&b.cluster).total_cmp(&score_of(&a.cluster)))
                    .then(locality_sort_key(a.selection_tier).cmp(&locality_sort_key(b.selection_tier)))
                    .then(a.site.cmp(&b.site))
                    .then(a.name.cmp(&b.name))
                    .then(a.cluster.cmp(&b.cluster))
            });
        },
    }
    candidates.dedup_by(|a, b| a.kind == b.kind && a.name == b.name && a.site == b.site && a.cluster == b.cluster);

    for (i, candidate) in candidates.iter_mut().enumerate() {
        #[expect(
            clippy::cast_possible_truncation,
            reason = "candidate count is bounded by provider count; u32 overflow is unreachable"
        )]
        let rank = i as u32;
        candidate.rank = Some(rank);
        if let Some(bd) = ordering.get(candidate.cluster.as_str()) {
            candidate.score = Some(bd.total);
            candidate.score_breakdown = Some(bd.clone());
        }
    }

    Ok(RoutingOverlay {
        network: network_name.to_owned(),
        local_site: local_site.to_owned(),
        candidates,
        generated_at: generated_at.map(str::to_owned),
    })
}

/// Convert a CRDT `ProviderAccessPolicy` to an operator `AccessPolicy` for evaluation.
fn crdt_access_policy_to_operator(crdt_policy: &crdt::ProviderAccessPolicy) -> AccessPolicy {
    use crate::crd::auth::SelectorConfig;
    AccessPolicy {
        site_selector: SelectorConfig {
            match_labels: crdt_policy.match_labels.clone(),
        },
    }
}

/// Check if a provider should be included for a given consumer based on access policy.
///
/// Returns `true` if the provider should generate candidates for the consumer,
/// `false` if it should be excluded due to access policy restrictions.
fn should_include_provider_for_consumer(
    provider: &InferenceProvider,
    consumer_site_labels: Option<&BTreeMap<String, String>>,
    network_name: &str,
) -> bool {
    let access_result = evaluate_access_policy(&provider.spec.access_policy, consumer_site_labels);
    match access_result {
        AccessPolicyResult::Allow => {
            // Provider allows this consumer - proceed with candidate generation
            true
        },
        AccessPolicyResult::Deny => {
            // Provider explicitly denies this consumer - skip candidate generation
            tracing::debug!(
                provider = %provider.metadata.name.as_deref().unwrap_or("unknown"),
                network = network_name,
                "access policy denied: consumer site labels do not match provider requirements"
            );
            false
        },
        AccessPolicyResult::Unknown => {
            // Consumer identity unknown - fail closed for restricted providers
            if provider.spec.access_policy.site_selector.match_labels.is_empty() {
                // Unrestricted provider - allow (preserve existing behavior)
                true
            } else {
                // Restricted provider with unknown consumer - fail closed
                tracing::debug!(
                    provider = %provider.metadata.name.as_deref().unwrap_or("unknown"),
                    network = network_name,
                    "access policy failed closed: consumer site identity unknown for restricted provider"
                );
                false
            }
        },
    }
}

/// Collect [`RoutingCandidate`]s from providers belonging to `network_name`.
///
/// Providers explicitly marked [`ProviderPhase::Unavailable`] in their status
/// are excluded.  Providers in any other phase (`Pending`, `Available`,
/// `Degraded`, or absent status) are included.  See [`is_explicitly_unavailable`]
/// for the rationale.
///
/// Provider access policy enforcement: Each provider's `spec.accessPolicy` is
/// evaluated against the `consumer_site_labels`. Providers with non-empty access
/// policies will only generate candidates if the consumer site matches the required
/// labels. Empty access policies allow all consumers (preserving existing behavior).
fn collect_candidates(
    network_name: &str,
    sites: &[GridSite],
    providers: &[InferenceProvider],
    consumer_site_labels: Option<&BTreeMap<String, String>>,
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

        // Apply access policy enforcement before generating candidates
        if !should_include_provider_for_consumer(provider, consumer_site_labels, network_name) {
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
#[expect(
    clippy::too_many_lines,
    reason = "sequential steps: name extraction, model validation, site resolution, credential projection, and candidate construction"
)]
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
    let credential = projected_credential_from_provider(provider);
    let mut candidates = Vec::new();
    for model in &provider.spec.models {
        for site in &sites {
            candidates.push(RoutingCandidate {
                kind: CANDIDATE_KIND.to_owned(),
                name: model.name.clone(),
                site: (*site).to_owned(),
                cluster: cluster.to_owned(),
                fresh,
                credential: credential.clone(),
                stable_id: None,
                admission_state: None,
                selection_tier: None,
                score: None,
                score_breakdown: None,
                rank: None,
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
#[expect(
    clippy::too_many_lines,
    reason = "dual-key ConfigMap construction with optional envelope and annotations"
)]
pub fn build_overlay_configmap(
    overlay: &RoutingOverlay,
    envelope: Option<&super::overlay_envelope::OverlayEnvelope>,
    network_name: &str,
    gateway_name: &str,
    namespace: &str,
) -> Result<ConfigMap, serde_json::Error> {
    let legacy_json = serde_json::to_string_pretty(overlay)?;
    let name = overlay_configmap_name(network_name, gateway_name);

    let mut data = BTreeMap::from([("routing-config.json".to_owned(), legacy_json)]);

    let annotations = if let Some(env) = envelope {
        let envelope_json = serde_json::to_string_pretty(env)?;
        data.insert(super::overlay_envelope::ENVELOPE_KEY.to_owned(), envelope_json);
        Some(BTreeMap::from([
            (
                super::overlay_envelope::ANNOTATION_SCHEMA_VERSION.to_owned(),
                env.schema_version.clone(),
            ),
            (
                super::overlay_envelope::ANNOTATION_REVISION.to_owned(),
                env.revision.value.clone(),
            ),
            (
                super::overlay_envelope::ANNOTATION_CONTENT_DIGEST.to_owned(),
                env.content_digest.value.clone(),
            ),
        ]))
    } else {
        None
    };

    Ok(ConfigMap {
        metadata: kube::api::ObjectMeta {
            annotations,
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
pub(crate) fn overlay_configmap_name(network_name: &str, gateway_name: &str) -> String {
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
///
/// Also used by [`super::geography::compute_stable_id`] for candidate
/// stable IDs.
pub(crate) fn fnv1a_hex8(input: &str) -> String {
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

    fn test_network_score_first(name: &str) -> GridNetwork {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": name },
            "spec": { "seeds": [], "routingPolicy": "scoreFirst" }
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
            .and_then(|d| d.get("routing-config.json"))
            .unwrap_or_else(|| std::process::abort());
        serde_json::from_str(json_str).unwrap_or_else(|_| std::process::abort())
    }

    fn build_cm(overlay: &RoutingOverlay, net: &str, gw: &str) -> ConfigMap {
        build_overlay_configmap(overlay, None, net, gw, "ns").unwrap_or_else(|_| std::process::abort())
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
    //
    // These tests pass `ScoringWeights::default()` — the scoring crate's
    // legacy combined defaults (locality=3, queue=3, kv=2, …). They verify
    // overlay rendering mechanics (sort stability, admission, locality tiers)
    // against the full six-signal surface. Production deploys use
    // `resolve_scoring_weights(policy)`, which returns strategy-selected
    // one-hot weights; those paths are covered by the dedicated noMetrics,
    // queueDepth, and kvCachePressure sections below and in the CRD tests.
    // -----------------------------------------------------------------------

    #[test]
    fn score_ordered_local_ranks_before_api_provider() {
        // Regression-safe: full scoring engine must preserve local > api ordering.
        let network = test_network("net");
        let local_prov = test_provider_with_backend_kind("local-prov", "net", "local");
        let api_prov = test_provider_with_backend_kind("api-prov", "net", "api_provider");
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[api_prov, local_prov],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[costly_prov, free_prov],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[p_z, p_a],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[cloud, unknown],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[api, self_hosted],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let fwd = render_routing_overlay(
            &network,
            &[],
            &[local.clone(), api.clone()],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        let rev = render_routing_overlay(
            &network,
            &[],
            &[api, local],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[remote, local],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
    // Praxis `intelligent_route` candidate contract (current wire format):
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[remote_prov, local_prov],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[local_down, api_fallback],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        // Default GeographyFirst sort: admission → locality → score → fresh → tiebreak.
        // The degraded local (SameSite, fresh=false) outranks the fresh API
        // (CrossRegion, fresh=true) because geography sorts above freshness.
        let network = test_network("fallback-net");
        let local_degraded =
            test_provider_with_backend_kind_and_phase("provider-local", "fallback-net", "local", "Degraded");
        let api_ok = test_provider_with_backend_kind("provider-api", "fallback-net", "api_provider");
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[api_ok, local_degraded],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("provider-local"),
            "Degraded local must rank before API (GeographyFirst: locality outranks freshness)"
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[api, cloud, remote, self_hosted],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay1 = render_routing_overlay(
            &network,
            &[],
            &[local_down, api_always_available.clone()],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay2 = render_routing_overlay(
            &network,
            &[],
            &[local_up, api_always_available],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[p1, p2],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert!(
            overlay.candidates.is_empty(),
            "all-Unavailable overlay must have zero candidates (no error)"
        );
    }

    #[test]
    fn cross_site_candidate_json_has_required_praxis_fields() {
        // Validate that the ConfigMap JSON payload exposes all fields the Praxis
        // `intelligent_route` filter reads from each candidate entry.
        //
        // Current candidate wire format: kind, name, site, cluster, fresh.
        // `endpoint` is NOT in the candidate — Praxis looks up the backend
        // endpoint via the `cluster` name in its own load_balancer config.
        let network = test_network("json-net");
        let local_prov = test_provider_with_backend_kind("prov-a", "json-net", "local");
        let api_prov = test_provider_with_backend_kind("prov-b", "json-net", "api_provider");
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[local_prov, api_prov],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[api_prov, local_prov],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[api, cloud, remote, local],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[p_z, p_a],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let fwd = render_routing_overlay(
            &network,
            &[],
            &[local.clone(), api.clone()],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        let rev = render_routing_overlay(
            &network,
            &[],
            &[api, local],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[cloud, unknown],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert!(overlay.candidates.is_empty(), "no providers should yield no candidates");
    }

    #[test]
    fn provider_in_different_network_is_excluded() {
        let network = test_network("net-a");
        let provider = test_provider("prov", "net-b", &["model-1"]);
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert!(
            overlay.candidates.is_empty(),
            "provider in net-b must be excluded from net-a overlay"
        );
    }

    #[test]
    fn provider_with_two_models_renders_two_candidates() {
        let network = test_network("net-a");
        let provider = test_provider("prov", "net-a", &["model-1", "model-2"]);
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 2, "two models must produce two candidates");
    }

    #[test]
    fn two_providers_with_same_model_produce_two_candidates() {
        let network = test_network("net");
        let p1 = test_provider("prov-a", "net", &["llama-3"]);
        let p2 = test_provider("prov-b", "net", &["llama-3"]);
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[p1, p2],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[p1, p2],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let fwd = render_routing_overlay(
            &network,
            &[],
            &[p1.clone(), p2.clone()],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        let rev = render_routing_overlay(
            &network,
            &[],
            &[p2, p1],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let result = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        );
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
        let overlay = render_routing_overlay(
            &network,
            &[site_a, site_b],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[site_gpu, site_cpu],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[site_other],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[site_cpu],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[site],
            &[p1, p2],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[site],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert!(overlay.candidates.is_empty(), "Unavailable provider must be excluded");
    }

    #[test]
    fn available_provider_is_included_with_fresh_true() {
        let network = test_network("net");
        let provider = test_provider_with_phase("prov", "net", &["model-1"], "Available");
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[available, degraded],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[site_a, site_b],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 2, "one candidate per matched site");
        assert!(
            overlay.candidates.iter().all(|c| !c.fresh),
            "every (model, site) candidate from a Degraded provider must have fresh=false"
        );
    }

    // -----------------------------------------------------------------------
    // Stale-candidate ordering — fresh=true before fresh=false
    // -----------------------------------------------------------------------

    #[test]
    fn fresh_true_sorts_before_fresh_false_same_score_same_model() {
        // Two providers with the same backend kind (equal locality score, no metrics)
        // serving the same model — one healthy (Available), one stale (Degraded).
        // The fresh=true candidate must appear before the fresh=false one.
        let network = test_network("net");
        // test_provider uses backendKind=local for both → equal locality scores.
        // Stale provider is given Degraded status so is_candidate_fresh returns false.
        let healthy = test_provider("healthy-prov", "net", &["shared-model"]);
        let stale = test_provider_with_phase("stale-prov", "net", &["shared-model"], "Degraded");
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[stale, healthy],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(overlay.candidates.len(), 2, "both candidates must be present");
        let first = overlay.candidates.first().unwrap_or_else(|| std::process::abort());
        let second = overlay.candidates.get(1).unwrap_or_else(|| std::process::abort());
        assert!(
            first.fresh,
            "fresh=true candidate must sort before fresh=false when scores are equal; \
             got first={:?} fresh={}",
            first.cluster, first.fresh
        );
        assert!(
            !second.fresh,
            "fresh=false candidate must be second; got second={:?} fresh={}",
            second.cluster, second.fresh
        );
    }

    #[test]
    fn stale_candidate_retained_not_excluded_by_sort() {
        // Degraded provider (fresh=false) must remain in the overlay — it is kept for
        // observability and as a last-resort fallback when no healthy alternative exists.
        let network = test_network("net");
        let stale = test_provider_with_phase("stale-prov", "net", &["model-x"], "Degraded");
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[stale],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 1, "stale candidate must not be dropped");
        assert!(
            overlay.candidates.first().is_some_and(|c| !c.fresh),
            "retained stale candidate must have fresh=false"
        );
    }

    #[test]
    fn healthy_local_sorts_before_stale_crdt_remote_same_model() {
        // Local provider (fresh=true, backendKind=local, high locality score) vs stale
        // CRDT remote (fresh=false, lower locality score).  Local must appear first
        // due to higher score; freshness tiebreaker reinforces this for equal-score edge cases.
        let network = test_network("net");
        let local_healthy = test_provider("local-prov", "net", &["shared-model"]);
        let remote_stale = make_crdt_provider(
            "remote-site",
            "remote-cluster",
            crdt::ProviderPhase::Degraded,
            &["shared-model"],
        );
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[local_healthy],
            &[remote_stale],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 2, "both local and remote candidates present");
        let first = overlay.candidates.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(first.cluster, "local-prov", "local provider must rank first");
        assert!(first.fresh, "first candidate (local) must be fresh=true");
        let second = overlay.candidates.get(1).unwrap_or_else(|| std::process::abort());
        assert!(!second.fresh, "stale remote must be second with fresh=false");
    }

    #[test]
    fn stale_fresh_sort_order_input_order_independent() {
        // Ordering must be deterministic regardless of input order.
        // Stale candidate submitted first → healthy candidate still wins.
        let network = test_network("net");
        let healthy = test_provider("healthy-prov", "net", &["shared-model"]);
        let stale = test_provider_with_phase("stale-prov", "net", &["shared-model"], "Degraded");
        // Submit stale first, then healthy.
        let overlay_stale_first = render_routing_overlay(
            &network,
            &[],
            &[stale.clone(), healthy.clone()],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        // Submit healthy first, then stale.
        let overlay_healthy_first = render_routing_overlay(
            &network,
            &[],
            &[healthy, stale],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        let first_stale_first = overlay_stale_first
            .candidates
            .first()
            .unwrap_or_else(|| std::process::abort());
        let first_healthy_first = overlay_healthy_first
            .candidates
            .first()
            .unwrap_or_else(|| std::process::abort());
        assert!(
            first_stale_first.fresh,
            "healthy candidate must win regardless of input order (stale submitted first)"
        );
        assert_eq!(
            first_stale_first.cluster, first_healthy_first.cluster,
            "overlay first candidate must be identical regardless of input order"
        );
    }

    // -----------------------------------------------------------------------
    // ConfigMap builder — fallible serialization
    // -----------------------------------------------------------------------

    #[test]
    fn configmap_name_matches_pattern() {
        let network = test_network("my-net");
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        let cm = build_cm(&overlay, "net", "gw");
        assert!(
            cm.data.as_ref().is_some_and(|d| d.contains_key("routing-config.json")),
            "data key must be routing-config.json"
        );
    }

    #[test]
    fn build_overlay_configmap_is_fallible() {
        // This test verifies the signature is Result-based.
        // Serialization of RoutingOverlay cannot currently fail (all fields
        // are plain Strings / booleans), so we just verify the Ok path.
        let network = test_network("net");
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        let result = build_overlay_configmap(&overlay, None, "net", "gw", "ns");
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay_a = render_routing_overlay(
            &network,
            &[],
            &[],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        let overlay_b = render_routing_overlay(
            &network,
            &[],
            &[],
            &[],
            "site-b",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let result = render_routing_overlay(
            &network,
            &[],
            &[],
            &[],
            "local",
            None,
            None,
            &scoring::ScoringWeights::default(),
        );
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
        let result = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "local",
            None,
            None,
            &scoring::ScoringWeights::default(),
        );
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
        let ordering = provider_ordering_scores(
            "net",
            &providers_for_ordering,
            &[],
            None,
            Some(&metrics),
            &scoring::ScoringWeights::default(),
        );

        let busy_score = ordering["provider-busy"].total;
        let idle_score = ordering["provider-idle"].total;
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
        let ordering_static =
            provider_ordering_scores("net", &ps_static, &[], None, None, &scoring::ScoringWeights::default());
        let ps_empty = [local, api];
        let ordering_no_metrics = provider_ordering_scores(
            "net",
            &ps_empty,
            &[],
            None,
            Some(&HashMap::new()),
            &scoring::ScoringWeights::default(),
        );
        assert_eq!(
            ordering_static["prov-local"].total, ordering_no_metrics["prov-local"].total,
            "empty metrics map must yield same score as None"
        );
        assert_eq!(
            ordering_static["prov-api"].total, ordering_no_metrics["prov-api"].total,
            "empty metrics map must yield same score as None"
        );
    }

    // -----------------------------------------------------------------------
    // noMetrics strategy — overlay rendering with production default weights
    //
    // Weight resolution itself is tested in crd::grid_network::tests.
    // These tests verify overlay-level behavior: that zero weights produce
    // zero breakdowns, that admission and geography still apply, and that
    // scoreFirst does not manufacture a preference.
    // -----------------------------------------------------------------------

    fn no_metrics_weights() -> scoring::ScoringWeights {
        crate::crd::grid_network::resolve_scoring_weights(None)
    }

    fn assert_weight(actual: f64, expected: f64, label: &str) {
        assert!(
            (actual - expected).abs() < f64::EPSILON,
            "{label}: expected {expected}, got {actual}"
        );
    }

    #[test]
    fn no_metrics_all_score_contributions_are_zero() {
        let a = test_provider_with_backend_kind("prov-a", "net", "local");
        let b = test_provider_with_backend_kind("prov-b", "net", "local");
        let network = test_network("net");

        let mut metrics = HashMap::new();
        metrics.insert(
            "prov-a",
            scoring::BackendMetrics::new(0.0, true, 0.80, 100.0, 0.0, 0.10),
        );
        metrics.insert(
            "prov-b",
            scoring::BackendMetrics::new(0.0, true, 0.20, 100.0, 0.0, 0.90),
        );

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[a, b],
            &[],
            "gw",
            Some(&metrics),
            None,
            &no_metrics_weights(),
        )
        .unwrap_or_else(|_| std::process::abort());

        for c in &overlay.candidates {
            let bd = c.score_breakdown.as_ref().unwrap_or_else(|| std::process::abort());
            assert_weight(bd.queue_depth, 0.0, &format!("{}: queue_depth", c.cluster));
            assert_weight(bd.kv_cache, 0.0, &format!("{}: kv_cache", c.cluster));
            assert_weight(bd.locality, 0.0, &format!("{}: locality", c.cluster));
            assert_weight(bd.prefix_cache, 0.0, &format!("{}: prefix_cache", c.cluster));
            assert_weight(bd.latency, 0.0, &format!("{}: latency", c.cluster));
            assert_weight(bd.cost, 0.0, &format!("{}: cost", c.cluster));
            assert_weight(bd.total, 0.0, &format!("{}: total", c.cluster));
        }
    }

    #[test]
    fn no_metrics_opposing_metrics_produce_equal_dynamic_scores() {
        let a = test_provider_with_backend_kind("prov-a", "net", "local");
        let b = test_provider_with_backend_kind("prov-b", "net", "local");
        let network = test_network_score_first("net");

        let mut metrics = HashMap::new();
        metrics.insert(
            "prov-a",
            scoring::BackendMetrics::new(0.0, true, 0.80, 100.0, 0.0, 0.10),
        );
        metrics.insert(
            "prov-b",
            scoring::BackendMetrics::new(0.0, true, 0.20, 100.0, 0.0, 0.90),
        );

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[a, b],
            &[],
            "gw",
            Some(&metrics),
            None,
            &no_metrics_weights(),
        )
        .unwrap_or_else(|_| std::process::abort());

        let scores: Vec<f64> = overlay.candidates.iter().map(|c| c.score.unwrap_or(f64::NAN)).collect();
        assert_eq!(scores.len(), 2);
        assert_weight(
            scores[0],
            scores[1],
            "both providers must have equal dynamic scores under noMetrics",
        );
    }

    #[test]
    fn no_metrics_geography_first_still_prefers_local() {
        let local = test_provider_with_backend_kind("prov-local", "net", "local");
        let remote = test_provider_with_backend_kind("prov-remote", "net", "remote");
        let network = test_network("net");

        let mut metrics = HashMap::new();
        metrics.insert(
            "prov-local",
            scoring::BackendMetrics::new(0.0, true, 0.80, 0.0, 0.0, 0.80),
        );
        metrics.insert(
            "prov-remote",
            scoring::BackendMetrics::new(0.0, true, 0.01, 0.0, 0.0, 0.01),
        );

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[remote, local],
            &[],
            "prov-local",
            Some(&metrics),
            None,
            &no_metrics_weights(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("prov-local"),
            "geographyFirst with noMetrics: local must rank first despite worse metrics"
        );
    }

    #[test]
    fn no_metrics_admission_still_restricts_saturated_provider() {
        let healthy = test_provider_with_backend_kind("prov-healthy", "net", "api_provider");
        let saturated = test_provider_with_backend_kind("prov-saturated", "net", "local");
        let network = test_network("net");

        let mut metrics = HashMap::new();
        metrics.insert(
            "prov-saturated",
            scoring::BackendMetrics::new(0.0, true, 0.95, 0.0, 0.0, 0.95),
        );
        metrics.insert(
            "prov-healthy",
            scoring::BackendMetrics::new(0.0, true, 0.1, 0.0, 0.0, 0.1),
        );

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[saturated, healthy],
            &[],
            "gw",
            Some(&metrics),
            None,
            &no_metrics_weights(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("prov-healthy"),
            "noMetrics: admission must still order NewAndExisting above ExistingOnly"
        );
        assert_eq!(
            overlay.candidates[0].admission_state,
            Some(AdmissionState::NewAndExisting),
            "first candidate must be NewAndExisting under noMetrics"
        );
    }

    #[test]
    fn no_metrics_unavailable_provider_still_excluded() {
        let available = test_provider_with_backend_kind("prov-up", "net", "local");
        let unavailable = test_provider_with_backend_kind_and_phase("prov-down", "net", "local", "Unavailable");

        let network = test_network("net");
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[available, unavailable],
            &[],
            "gw",
            None,
            None,
            &no_metrics_weights(),
        )
        .unwrap_or_else(|_| std::process::abort());

        let names: Vec<&str> = overlay.candidates.iter().map(|c| c.cluster.as_str()).collect();
        assert!(names.contains(&"prov-up"), "available provider must appear in overlay");
        assert!(
            !names.contains(&"prov-down"),
            "unavailable provider must be excluded even under noMetrics"
        );
    }

    #[test]
    fn no_metrics_score_first_does_not_manufacture_preference() {
        let a = test_provider_with_backend_kind("prov-a", "net", "local");
        let b = test_provider_with_backend_kind("prov-b", "net", "local");
        let network = test_network_score_first("net");

        let mut metrics = HashMap::new();
        metrics.insert(
            "prov-a",
            scoring::BackendMetrics::new(0.0, true, 0.90, 100.0, 0.0, 0.99),
        );
        metrics.insert(
            "prov-b",
            scoring::BackendMetrics::new(0.0, true, 0.01, 100.0, 0.0, 0.01),
        );

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[a, b],
            &[],
            "gw",
            Some(&metrics),
            None,
            &no_metrics_weights(),
        )
        .unwrap_or_else(|_| std::process::abort());

        let scores: Vec<f64> = overlay.candidates.iter().map(|c| c.score.unwrap_or(f64::NAN)).collect();
        assert_weight(
            scores[0],
            scores[1],
            "scoreFirst with noMetrics: extreme metric differences must not create score preference",
        );
    }

    // -----------------------------------------------------------------------
    // render_routing_overlay metrics path — end-to-end ordering proofs
    // -----------------------------------------------------------------------

    #[test]
    fn render_with_metrics_reorders_equal_locality_providers() {
        // Two local providers with equal locality and no cost difference.
        // Without metrics they are tied and fall back to alphabetical order;
        // provider-busy < provider-idle alphabetically, so busy comes first.
        // With metrics the high-queue provider scores lower and yields the lead.
        let busy = test_provider_with_backend_kind("provider-busy", "net", "local");
        let idle = test_provider_with_backend_kind("provider-idle", "net", "local");
        let network = test_network("net");

        let mut metrics = HashMap::new();
        metrics.insert(
            "provider-busy",
            scoring::BackendMetrics::new(0.0, true, 0.0, 0.0, 0.0, 0.9),
        );
        metrics.insert(
            "provider-idle",
            scoring::BackendMetrics::new(0.0, true, 0.0, 0.0, 0.0, 0.1),
        );

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[busy, idle],
            &[],
            "local-gw",
            Some(&metrics),
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        // idle (queue_depth=0.1) must rank before busy (queue_depth=0.9).
        let first = overlay.candidates.first().map(|c| c.cluster.as_str());
        assert_eq!(
            first,
            Some("provider-idle"),
            "idle provider must rank first when busy has high queue depth; \
             got first={first:?}"
        );
    }

    #[test]
    fn render_without_metrics_preserves_static_locality_ordering() {
        // None metrics and empty-metrics-map must produce the same candidate order.
        // local backend outscores api_provider on locality regardless of input order.
        let local = test_provider_with_backend_kind("prov-local", "net", "local");
        let api = test_provider_with_backend_kind("prov-api", "net", "api_provider");
        let network = test_network("net");

        let overlay_none = render_routing_overlay(
            &network,
            &[],
            &[api.clone(), local.clone()],
            &[],
            "gw",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        let overlay_empty = render_routing_overlay(
            &network,
            &[],
            &[api, local],
            &[],
            "gw",
            Some(&HashMap::new()),
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            overlay_none.candidates[0].cluster, overlay_empty.candidates[0].cluster,
            "None and empty metrics map must produce the same first candidate"
        );
        assert_eq!(
            overlay_none.candidates[0].cluster.as_str(),
            "prov-local",
            "local provider must rank first when no metrics are present"
        );
    }

    #[test]
    fn render_with_provider_absent_from_metrics_map_falls_back_safely() {
        // A provider that is not present in the metrics map must not panic and must
        // still appear in the overlay.  It receives a neutral score from
        // unmapped_provider_score — on the same scale as scored providers.
        let known = test_provider_with_backend_kind("known-prov", "net", "local");
        let unmapped = test_provider_with_backend_kind("unmapped-prov", "net", "local");
        let network = test_network("net");

        let mut metrics = HashMap::new();
        metrics.insert(
            "known-prov",
            scoring::BackendMetrics::new(0.0, true, 0.0, 0.0, 0.0, 0.1),
        );
        // "unmapped-prov" intentionally absent from the metrics map.

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[known, unmapped],
            &[],
            "gw",
            Some(&metrics),
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            overlay.candidates.len(),
            2,
            "both providers must appear in overlay even when one is absent from metrics map"
        );
    }

    #[test]
    fn parse_pipeline_does_not_panic_on_malformed_scrape_data() {
        // Proves the full pipeline: malformed scrape text → parse → BackendMetrics →
        // passed to render_routing_overlay.  None of these must panic or error.
        let malformed_inputs: &[&str] = &[
            "",
            "   ",
            "NaN\n",
            "Inf 1.0\n",
            "metric \t\n",
            "# only comment\n",
            "{bad json}\n",
            "my_queue NaN\n",
            "my_queue Inf\n",
            "my_queue -Inf\n",
        ];

        let names = crate::metrics_parser::MetricNames {
            queue_depth: Some("my_queue".to_owned()),
            ..Default::default()
        };

        for input in malformed_inputs {
            let partial = crate::metrics_parser::parse_prometheus_text(input, &names).unwrap();
            let bm = partial.into_backend_metrics();
            // Non-finite metrics must not reach the scoring engine.
            assert!(
                bm.queue_depth.is_finite(),
                "malformed input {input:?} produced non-finite queue_depth"
            );
            assert!(
                bm.kv_cache_utilization.is_finite(),
                "malformed input {input:?} produced non-finite kv_cache_utilization"
            );
            assert!(
                bm.latency_p99_ms.is_finite(),
                "malformed input {input:?} produced non-finite latency_p99_ms"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Score-driven routing algorithm — sort order proofs
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_pressure_outranks_locality() {
        let local = test_provider_with_backend_kind("prov-local", "net", "local");
        let remote = test_provider_with_backend_kind("prov-remote", "net", "remote");
        let network = test_network_score_first("net");

        let mut metrics = HashMap::new();
        metrics.insert(
            "prov-local",
            scoring::BackendMetrics::new(0.0, true, 0.85, 0.0, 0.0, 0.9),
        );
        metrics.insert(
            "prov-remote",
            scoring::BackendMetrics::new(0.0, true, 0.1, 0.0, 0.0, 0.1),
        );

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[local, remote],
            &[],
            "gw",
            Some(&metrics),
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("prov-remote"),
            "remote provider with low pressure must outrank local provider with high pressure"
        );
    }

    #[test]
    fn equal_metrics_prefers_local() {
        let local = test_provider_with_backend_kind("prov-local", "net", "local");
        let remote = test_provider_with_backend_kind("prov-remote", "net", "remote");
        let network = test_network("net");

        let mut metrics = HashMap::new();
        metrics.insert(
            "prov-local",
            scoring::BackendMetrics::new(0.0, true, 0.3, 100.0, 0.5, 0.3),
        );
        metrics.insert(
            "prov-remote",
            scoring::BackendMetrics::new(0.0, true, 0.3, 100.0, 0.5, 0.3),
        );

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[local, remote],
            &[],
            "gw",
            Some(&metrics),
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("prov-local"),
            "with equal metrics, local provider must rank first via locality score advantage"
        );
    }

    #[test]
    fn fresh_outranks_score() {
        let local = test_provider_with_backend_kind_and_phase("prov-local", "net", "local", "Degraded");
        let api = test_provider_with_backend_kind("prov-api", "net", "api_provider");
        let network = test_network_score_first("net");

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[local, api],
            &[],
            "gw",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        let first = &overlay.candidates[0];
        assert!(first.fresh, "first candidate must be fresh");
        assert_eq!(
            first.cluster.as_str(),
            "prov-api",
            "fresh API provider must outrank degraded local (fresh beats score)"
        );
    }

    #[test]
    fn admission_outranks_everything() {
        let healthy = test_provider_with_backend_kind("prov-healthy", "net", "api_provider");
        let saturated = test_provider_with_backend_kind("prov-saturated", "net", "local");
        let network = test_network("net");

        let mut metrics = HashMap::new();
        metrics.insert(
            "prov-saturated",
            scoring::BackendMetrics::new(0.0, true, 0.95, 0.0, 0.0, 0.95),
        );
        metrics.insert(
            "prov-healthy",
            scoring::BackendMetrics::new(0.0, true, 0.1, 0.0, 0.0, 0.1),
        );

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[saturated, healthy],
            &[],
            "gw",
            Some(&metrics),
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("prov-healthy"),
            "ExistingOnly candidate must rank below NewAndExisting regardless of locality"
        );
        assert_eq!(
            overlay.candidates[0].admission_state,
            Some(AdmissionState::NewAndExisting),
            "first candidate must be NewAndExisting"
        );
        assert_eq!(
            overlay.candidates[1].admission_state,
            Some(AdmissionState::ExistingOnly),
            "second candidate must be ExistingOnly"
        );
    }

    #[test]
    fn score_breakdown_populates_on_candidates() {
        let local = test_provider_with_backend_kind("prov-local", "net", "local");
        let network = test_network("net");

        let mut metrics = HashMap::new();
        metrics.insert(
            "prov-local",
            scoring::BackendMetrics::new(0.0, true, 0.3, 500.0, 0.4, 0.2),
        );

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[local],
            &[],
            "gw",
            Some(&metrics),
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        let c = &overlay.candidates[0];
        assert!(c.score.is_some(), "candidate must have a score");
        let bd = c.score_breakdown.as_ref().unwrap_or_else(|| std::process::abort());
        assert!(bd.locality > 0.0, "locality must be positive for local provider");
        assert!(bd.queue_depth > 0.0, "queue_depth must be positive");
        assert!(bd.kv_cache > 0.0, "kv_cache must be positive");
        assert!(
            (bd.total - c.score.unwrap_or(0.0)).abs() < 1e-10,
            "breakdown.total must equal candidate.score"
        );
    }

    // -----------------------------------------------------------------------
    // Routing policy — backward compatibility and serialization
    // -----------------------------------------------------------------------

    #[test]
    fn geography_first_produces_same_order_as_absent_policy() {
        let local = test_provider_with_backend_kind("prov-local", "net", "local");
        let remote = test_provider_with_backend_kind("prov-remote", "net", "remote");
        let api = test_provider_with_backend_kind("prov-api", "net", "api_provider");

        let no_policy = test_network("net");
        let geo_policy: GridNetwork = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "net" },
            "spec": { "seeds": [], "routingPolicy": "geographyFirst" }
        }))
        .unwrap_or_else(|_| std::process::abort());

        let providers = [local, remote, api];
        let overlay_default = render_routing_overlay(
            &no_policy,
            &[],
            &providers,
            &[],
            "gw",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        let overlay_geo = render_routing_overlay(
            &geo_policy,
            &[],
            &providers,
            &[],
            "gw",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        let order_default: Vec<&str> = overlay_default.candidates.iter().map(|c| c.cluster.as_str()).collect();
        let order_geo: Vec<&str> = overlay_geo.candidates.iter().map(|c| c.cluster.as_str()).collect();
        assert_eq!(
            order_default, order_geo,
            "absent routingPolicy and explicit geographyFirst must produce the same order"
        );
    }

    #[test]
    fn absent_routing_policy_preserves_geography_above_score() {
        let local = test_provider_with_backend_kind("local-site", "net", "local");
        let remote = test_provider_with_backend_kind("prov-remote", "net", "remote");
        let network = test_network("net");

        let mut metrics = HashMap::new();
        metrics.insert(
            "local-site",
            scoring::BackendMetrics::new(0.0, true, 0.70, 0.0, 0.0, 0.80),
        );
        metrics.insert(
            "prov-remote",
            scoring::BackendMetrics::new(0.0, true, 0.1, 0.0, 0.0, 0.1),
        );

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[local, remote],
            &[],
            "local-site",
            Some(&metrics),
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("local-site"),
            "default policy: local must rank first despite worse metrics (geography above score)"
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[p1, p2],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
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
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[api_provider, local_with_ref],
            &[],
            "test-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
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

        let state = build_grid_state_with_metrics("net", &providers, &[], Some(&metrics));
        assert!(state.metrics("prov-a").is_some(), "prov-a must have metrics attached");
        assert!(
            state.metrics("prov-b").is_none(),
            "prov-b must have no metrics (not in map)"
        );
    }

    // -----------------------------------------------------------------------
    // crdt_phase_to_fresh — pure mapping function
    // -----------------------------------------------------------------------

    #[test]
    fn crdt_phase_unavailable_produces_no_candidates() {
        assert!(
            crdt_phase_to_fresh(&crdt::ProviderPhase::Unavailable).is_none(),
            "Unavailable phase must yield None (excluded from overlay)"
        );
    }

    #[test]
    fn crdt_phase_degraded_produces_fresh_false() {
        assert_eq!(
            crdt_phase_to_fresh(&crdt::ProviderPhase::Degraded),
            Some(false),
            "Degraded phase must yield Some(false)"
        );
    }

    #[test]
    fn crdt_phase_available_produces_fresh_true() {
        assert_eq!(
            crdt_phase_to_fresh(&crdt::ProviderPhase::Available),
            Some(true),
            "Available phase must yield Some(true)"
        );
    }

    #[test]
    fn crdt_phase_pending_produces_fresh_true() {
        assert_eq!(
            crdt_phase_to_fresh(&crdt::ProviderPhase::Pending),
            Some(true),
            "Pending phase must yield Some(true)"
        );
    }

    // -----------------------------------------------------------------------
    // remote_crdt_provider_to_candidates — candidate generation
    // -----------------------------------------------------------------------

    fn make_crdt_provider(
        site_id: &str,
        routing_cluster: &str,
        phase: crdt::ProviderPhase,
        models: &[&str],
    ) -> crdt::ProviderState {
        crdt::ProviderState {
            network_id: "net".to_owned(),
            site_id: site_id.to_owned(),
            provider_id: "prov-1".to_owned(),
            routing_cluster: routing_cluster.to_owned(),
            models: models.iter().map(|m| (*m).to_owned()).collect(),
            backend_kind: "local".to_owned(),
            phase,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            access_policy: crdt::ProviderAccessPolicy::default(),
            revision: 1,
            writer_id: site_id.to_owned(),
        }
    }

    #[test]
    fn remote_provider_two_models_produces_two_candidates() {
        let provider = make_crdt_provider(
            "remote-site",
            "cluster-1",
            crdt::ProviderPhase::Available,
            &["model-a", "model-b"],
        );
        let candidates = remote_crdt_provider_to_candidates(&provider);
        assert_eq!(candidates.len(), 2, "two models must produce two candidates");
    }

    #[test]
    fn remote_provider_site_and_cluster_preserved_in_candidates() {
        let provider = make_crdt_provider("remote-site", "cluster-1", crdt::ProviderPhase::Available, &["model-a"]);
        let candidates = remote_crdt_provider_to_candidates(&provider);
        assert_eq!(candidates.len(), 1, "one model produces one candidate");
        let c = candidates.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(c.site, "remote-site", "site must be taken from site_id");
        assert_eq!(c.cluster, "cluster-1", "cluster must be taken from routing_cluster");
        assert!(c.fresh, "Available phase must produce fresh=true");
    }

    #[test]
    fn remote_provider_unavailable_produces_empty_candidates() {
        let provider = make_crdt_provider(
            "remote-site",
            "cluster-1",
            crdt::ProviderPhase::Unavailable,
            &["model-a"],
        );
        let candidates = remote_crdt_provider_to_candidates(&provider);
        assert!(candidates.is_empty(), "Unavailable phase must produce empty candidates");
    }

    // -----------------------------------------------------------------------
    // remote_crdt_provider_to_backend_config — BackendConfig generation
    // -----------------------------------------------------------------------

    #[test]
    fn remote_provider_local_backend_kind_mapped_to_remote() {
        let provider = make_crdt_provider("remote-site", "cluster-1", crdt::ProviderPhase::Available, &["model-a"]);
        let config = remote_crdt_provider_to_backend_config(&provider).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            config.kind,
            scoring::BackendKind::Remote,
            "local backend_kind from remote site must map to BackendKind::Remote"
        );
    }

    #[test]
    fn remote_provider_cloud_managed_preserved() {
        let mut provider = make_crdt_provider("remote-site", "cluster-1", crdt::ProviderPhase::Available, &["model-a"]);
        provider.backend_kind = "cloud_managed".to_owned();
        let config = remote_crdt_provider_to_backend_config(&provider).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            config.kind,
            scoring::BackendKind::CloudManaged,
            "cloud_managed must be preserved for remote CRDT providers"
        );
    }

    #[test]
    fn remote_provider_api_provider_preserved() {
        let mut provider = make_crdt_provider("remote-site", "cluster-1", crdt::ProviderPhase::Available, &["model-a"]);
        provider.backend_kind = "api_provider".to_owned();
        let config = remote_crdt_provider_to_backend_config(&provider).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            config.kind,
            scoring::BackendKind::ApiProvider,
            "api_provider must be preserved for remote CRDT providers"
        );
    }

    #[test]
    fn remote_provider_unavailable_returns_none_from_backend_config() {
        let provider = make_crdt_provider(
            "remote-site",
            "cluster-1",
            crdt::ProviderPhase::Unavailable,
            &["model-a"],
        );
        assert!(
            remote_crdt_provider_to_backend_config(&provider).is_none(),
            "Unavailable phase must return None from backend config"
        );
    }

    // -----------------------------------------------------------------------
    // crdt_metrics_to_backend — metrics mapping
    // -----------------------------------------------------------------------

    #[test]
    fn crdt_metrics_all_some_maps_correctly() {
        let m = crdt::ProviderMetricsSnapshot {
            queue_depth: Some(0.3),
            kv_cache_utilization: Some(0.4),
            latency_p99_ms: Some(120.0),
            prefix_cache_hit_ratio: Some(0.7),
            error_rate: Some(0.1),
            healthy: Some(true),
        };
        let bm = crdt_metrics_to_backend(&m);
        assert!((bm.queue_depth - 0.3).abs() < f64::EPSILON, "queue_depth must map");
        assert!(
            (bm.kv_cache_utilization - 0.4).abs() < f64::EPSILON,
            "kv_cache must map"
        );
        assert!((bm.latency_p99_ms - 120.0).abs() < f64::EPSILON, "latency must map");
        assert!(
            (bm.prefix_cache_hit_ratio - 0.7).abs() < f64::EPSILON,
            "prefix_cache must map"
        );
        assert!((bm.error_rate - 0.1).abs() < f64::EPSILON, "error_rate must map");
        assert!(bm.healthy, "healthy must map");
    }

    #[test]
    fn crdt_metrics_all_none_uses_neutral_defaults() {
        let m = crdt::ProviderMetricsSnapshot::default();
        let bm = crdt_metrics_to_backend(&m);
        assert!(
            (bm.queue_depth - UNMAPPED_NEUTRAL_SIGNAL).abs() < f64::EPSILON,
            "queue_depth must default to neutral 0.5"
        );
        assert!(
            (bm.kv_cache_utilization - UNMAPPED_NEUTRAL_SIGNAL).abs() < f64::EPSILON,
            "kv_cache must default to neutral 0.5"
        );
        assert!(bm.error_rate.abs() < f64::EPSILON, "error_rate must default to 0.0");
        assert!(bm.healthy, "healthy must default to true");
    }

    #[test]
    fn crdt_metrics_absent_latency_defaults_to_neutral_ms() {
        // Missing latency must use NEUTRAL_LATENCY_MS (2500.0) so that a remote
        // provider with no latency observation scores neutrally (0.5) on the latency
        // signal, not optimally (≈1.0 if 0.5ms were used).
        let m = crdt::ProviderMetricsSnapshot::default(); // all None
        let bm = crdt_metrics_to_backend(&m);
        assert!(
            (bm.latency_p99_ms - NEUTRAL_LATENCY_MS).abs() < f64::EPSILON,
            "absent latency_p99_ms must default to {NEUTRAL_LATENCY_MS}ms (neutral), got {}",
            bm.latency_p99_ms
        );
    }

    #[test]
    fn crdt_metrics_ratio_signals_clamped_above_one() {
        // Out-of-range ratio values from a remote site with different schema must not
        // corrupt scoring; they must be clamped to [0.0, 1.0].
        let m = crdt::ProviderMetricsSnapshot {
            queue_depth: Some(1.5),
            kv_cache_utilization: Some(2.0),
            latency_p99_ms: Some(-100.0), // negative latency → clamp to 0.0
            prefix_cache_hit_ratio: Some(1.1),
            error_rate: Some(1.3),
            healthy: Some(false),
        };
        let bm = crdt_metrics_to_backend(&m);
        assert_eq!(bm.queue_depth, 1.0, "queue_depth > 1.0 must be clamped to 1.0");
        assert_eq!(bm.kv_cache_utilization, 1.0, "kv_cache > 1.0 must be clamped to 1.0");
        assert_eq!(bm.latency_p99_ms, 0.0, "negative latency must be clamped to 0.0");
        assert_eq!(
            bm.prefix_cache_hit_ratio, 1.0,
            "prefix_cache_hit_ratio > 1.0 must be clamped to 1.0"
        );
        assert_eq!(bm.error_rate, 1.0, "error_rate > 1.0 must be clamped to 1.0");
        assert!(!bm.healthy, "healthy=false must propagate");
    }

    #[test]
    fn crdt_metrics_ratio_signals_clamped_below_zero() {
        // Negative ratio values must be clamped to 0.0.
        let m = crdt::ProviderMetricsSnapshot {
            queue_depth: Some(-0.1),
            kv_cache_utilization: Some(-0.5),
            latency_p99_ms: None,
            prefix_cache_hit_ratio: Some(-1.0),
            error_rate: Some(-0.2),
            healthy: Some(true),
        };
        let bm = crdt_metrics_to_backend(&m);
        assert_eq!(bm.queue_depth, 0.0, "negative queue_depth must be clamped to 0.0");
        assert_eq!(bm.kv_cache_utilization, 0.0, "negative kv_cache must be clamped to 0.0");
        assert_eq!(
            bm.prefix_cache_hit_ratio, 0.0,
            "negative prefix_cache_hit_ratio must be clamped to 0.0"
        );
        assert_eq!(bm.error_rate, 0.0, "negative error_rate must be clamped to 0.0");
    }

    #[test]
    fn crdt_metrics_non_finite_values_default_before_scoring() {
        // f64::clamp does not sanitize NaN, so CRDT values must be filtered before
        // clamping. Treat non-finite values like absent fields.
        let m = crdt::ProviderMetricsSnapshot {
            queue_depth: Some(f64::NAN),
            kv_cache_utilization: Some(f64::INFINITY),
            latency_p99_ms: Some(f64::NEG_INFINITY),
            prefix_cache_hit_ratio: Some(f64::NAN),
            error_rate: Some(f64::INFINITY),
            healthy: Some(true),
        };
        let bm = crdt_metrics_to_backend(&m);
        assert_eq!(bm.queue_depth, UNMAPPED_NEUTRAL_SIGNAL);
        assert_eq!(bm.kv_cache_utilization, UNMAPPED_NEUTRAL_SIGNAL);
        assert_eq!(bm.latency_p99_ms, NEUTRAL_LATENCY_MS);
        assert_eq!(bm.prefix_cache_hit_ratio, UNMAPPED_NEUTRAL_SIGNAL);
        assert_eq!(bm.error_rate, 0.0);
    }

    // -----------------------------------------------------------------------
    // render_routing_overlay integration — remote CRDT providers
    // -----------------------------------------------------------------------

    #[test]
    fn render_overlay_includes_remote_crdt_candidates() {
        let network = test_network("net");
        let remote = make_crdt_provider(
            "remote-site",
            "remote-cluster",
            crdt::ProviderPhase::Available,
            &["model-remote"],
        );
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[],
            &[remote],
            "local-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "remote CRDT provider must produce a candidate"
        );
        let c = overlay.candidates.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(c.site, "remote-site", "site must come from CRDT site_id");
        assert_eq!(
            c.cluster, "remote-cluster",
            "cluster must come from CRDT routing_cluster"
        );
        assert_eq!(c.name, "model-remote", "name must match CRDT model");
        assert!(c.fresh, "Available CRDT provider must be fresh");
    }

    #[test]
    fn render_overlay_excludes_unavailable_crdt_providers() {
        let network = test_network("net");
        let unavailable = make_crdt_provider(
            "remote-site",
            "remote-cluster",
            crdt::ProviderPhase::Unavailable,
            &["model-remote"],
        );
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[],
            &[unavailable],
            "local-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert!(
            overlay.candidates.is_empty(),
            "Unavailable CRDT provider must be excluded from overlay"
        );
    }

    // -----------------------------------------------------------------------
    // Credential projection — projected_credential_from_provider
    // -----------------------------------------------------------------------

    fn test_provider_with_bearer_auth(name: &str, network: &str) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "open_ai",
                "backendKind": "api_provider",
                "endpoint": "https://api.openai.com",
                "models": [{ "name": "gpt-4" }],
                "auth": {
                    "strategy": "bearer_token",
                    "secretRef": {
                        "name": "my-secret",
                        "namespace": "default",
                        "key": "token"
                    }
                }
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_provider_with_manual_auth(name: &str, network: &str) -> InferenceProvider {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "open_ai",
                "backendKind": "api_provider",
                "endpoint": "https://api.openai.com",
                "models": [{ "name": "gpt-4" }],
                "auth": { "manual": true, "strategy": "bearer_token" }
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn bearer_token_provider_produces_credential_ref() {
        let provider = test_provider_with_bearer_auth("api-prov", "net");
        let cred = projected_credential_from_provider(&provider);
        let cred = cred.expect("bearer_token provider must produce a credential ref");
        assert_eq!(cred.strategy, "bearer_token");
        assert_eq!(cred.secret_ref.name, "my-secret");
        assert_eq!(cred.secret_ref.namespace, "default");
        assert_eq!(cred.secret_ref.key, "token");
    }

    #[test]
    fn manual_auth_produces_no_credential_ref() {
        let provider = test_provider_with_manual_auth("api-prov", "net");
        assert!(
            projected_credential_from_provider(&provider).is_none(),
            "manual auth must produce no credential ref"
        );
    }

    #[test]
    fn absent_auth_produces_no_credential_ref() {
        let provider = test_provider("no-auth-prov", "net", &["model-a"]);
        assert!(
            projected_credential_from_provider(&provider).is_none(),
            "absent auth must produce no credential ref"
        );
    }

    #[test]
    fn unsupported_auth_strategy_produces_no_credential_ref() {
        let provider: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": "sigv4-prov" },
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "bedrock",
                "backendKind": "api_provider",
                "endpoint": "https://bedrock.us-east-1.amazonaws.com",
                "models": [{ "name": "claude" }],
                "auth": { "strategy": "sigv4" }
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert!(
            projected_credential_from_provider(&provider).is_none(),
            "sigv4 strategy must produce no credential ref"
        );
    }

    #[test]
    fn bearer_token_candidate_carries_credential_ref() {
        let network = test_network("net");
        let provider = test_provider_with_bearer_auth("api-prov", "net");
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        let cred = overlay
            .candidates
            .first()
            .and_then(|c| c.credential.as_ref())
            .expect("api_provider with bearer_token must carry credential ref");
        assert_eq!(cred.strategy, "bearer_token", "strategy must be bearer_token");
        assert_eq!(cred.secret_ref.name, "my-secret", "secret name must propagate");
        assert_eq!(cred.secret_ref.namespace, "default", "namespace must propagate");
        assert_eq!(cred.secret_ref.key, "token", "key must propagate");
    }

    #[test]
    fn credential_ref_in_configmap_json_contains_secret_ref_not_token_value() {
        let network = test_network("net");
        let provider = test_provider_with_bearer_auth("api-prov", "net");
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        let cm = build_cm(&overlay, "net", "gw");
        let json = overlay_json_from_cm(&cm);
        let candidate = json
            .get("candidates")
            .and_then(serde_json::Value::as_array)
            .and_then(|a| a.first())
            .expect("must have at least one candidate");
        let cred_json = candidate.get("credential").expect("credential must be in JSON");
        // secretRef must be present with name/namespace/key
        let secret_ref = cred_json.get("secretRef").expect("secretRef must be present");
        assert_eq!(
            secret_ref.get("name").and_then(serde_json::Value::as_str),
            Some("my-secret")
        );
        assert_eq!(
            secret_ref.get("namespace").and_then(serde_json::Value::as_str),
            Some("default")
        );
        assert_eq!(secret_ref.get("key").and_then(serde_json::Value::as_str), Some("token"));
        // The JSON must NOT contain any token-value field
        assert!(cred_json.get("token").is_none(), "token value must not appear in JSON");
        assert!(cred_json.get("value").is_none(), "token value must not appear in JSON");
    }

    #[test]
    fn no_auth_candidate_has_no_credential_field_in_json() {
        let network = test_network("net");
        let provider = test_provider("no-auth", "net", &["model-a"]);
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        let cm = build_cm(&overlay, "net", "gw");
        let json = overlay_json_from_cm(&cm);
        let candidate = json
            .get("candidates")
            .and_then(serde_json::Value::as_array)
            .and_then(|a| a.first())
            .expect("must have candidate");
        assert!(
            candidate.get("credential").is_none(),
            "absent auth must produce no 'credential' field in JSON"
        );
    }

    #[test]
    fn manual_auth_candidate_has_no_credential_field_in_json() {
        let network = test_network("net");
        let provider = test_provider_with_manual_auth("manual-prov", "net");
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[provider],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        let cm = build_cm(&overlay, "net", "gw");
        let json = overlay_json_from_cm(&cm);
        let candidate = json
            .get("candidates")
            .and_then(serde_json::Value::as_array)
            .and_then(|a| a.first())
            .expect("must have candidate");
        assert!(
            candidate.get("credential").is_none(),
            "manual auth must produce no 'credential' field in JSON"
        );
    }

    // -----------------------------------------------------------------------
    // stale_policy_from_spec — spec field to policy conversion
    // -----------------------------------------------------------------------

    #[test]
    fn stale_policy_from_spec_absent_produces_none_ttl() {
        let policy = stale_policy_from_spec(None);
        assert!(
            policy.dead_member_ttl_secs.is_none(),
            "absent TTL spec field must produce no-op policy (None TTL)"
        );
    }

    #[test]
    fn stale_policy_from_spec_some_produces_matching_ttl() {
        let policy = stale_policy_from_spec(Some(3600));
        assert_eq!(
            policy.dead_member_ttl_secs,
            Some(3600),
            "configured TTL must propagate as-is to policy"
        );
    }

    #[test]
    fn stale_policy_from_spec_one_second_minimum() {
        let policy = stale_policy_from_spec(Some(1));
        assert_eq!(
            policy.dead_member_ttl_secs,
            Some(1),
            "minimum TTL of 1 second must be accepted"
        );
    }

    #[test]
    fn stale_policy_from_spec_zero_treated_as_absent() {
        // The CRD schema rejects zero, but the pure helper defensively treats it
        // as absent to avoid accidental immediate eviction.
        let policy = stale_policy_from_spec(Some(0));
        assert!(
            policy.dead_member_ttl_secs.is_none(),
            "zero TTL must be treated as absent internally even though the CRD schema rejects it"
        );
    }

    #[test]
    fn stale_policy_from_spec_large_ttl_preserves_precision() {
        // u32::MAX seconds (~136 years) must round-trip without truncation through u64.
        let policy = stale_policy_from_spec(Some(u32::MAX));
        assert_eq!(
            policy.dead_member_ttl_secs,
            Some(u64::from(u32::MAX)),
            "large TTL must round-trip through u64 without truncation"
        );
    }

    // -----------------------------------------------------------------------
    // dead_or_suspect_age_secs — SWIM age extraction
    // -----------------------------------------------------------------------

    fn make_member(site_id: &str, status: MemberStatus, age_secs: u64) -> crate::swim::MemberRecord {
        crate::swim::MemberRecord {
            site_id: site_id.to_owned(),
            endpoint: "127.0.0.1:7946".to_owned(),
            incarnation: 1,
            status,
            age_secs,
            gateway_address: None,
            site_cert_pem: None,
        }
    }

    fn make_snapshot(members: Vec<crate::swim::MemberRecord>) -> MembershipSnapshot {
        MembershipSnapshot { members }
    }

    #[test]
    fn dead_or_suspect_age_returns_none_for_alive_member() {
        let snap = make_snapshot(vec![make_member("site-a", MemberStatus::Alive, 999)]);
        let age = dead_or_suspect_age_secs("site-a", Some(&snap));
        assert!(age.is_none(), "Alive member must not return an age (not stale)");
    }

    #[test]
    fn dead_or_suspect_age_returns_none_for_unknown_site() {
        let snap = make_snapshot(vec![make_member("site-b", MemberStatus::Dead, 300)]);
        let age = dead_or_suspect_age_secs("site-unknown", Some(&snap));
        assert!(age.is_none(), "unknown site must return None (retain conservatively)");
    }

    #[test]
    fn dead_or_suspect_age_returns_none_when_no_snapshot() {
        let age = dead_or_suspect_age_secs("site-a", None);
        assert!(age.is_none(), "no snapshot must return None (retain conservatively)");
    }

    #[test]
    fn dead_or_suspect_age_returns_none_for_zero_age_dead() {
        // age_secs=0 on a Dead member means sub-second age or synthetic
        // snapshot without age. Conservatively treat as unknown → retain.
        let snap = make_snapshot(vec![make_member("site-a", MemberStatus::Dead, 0)]);
        let age = dead_or_suspect_age_secs("site-a", Some(&snap));
        assert!(
            age.is_none(),
            "Dead with age_secs=0 must return None (sub-second or synthetic age)"
        );
    }

    #[test]
    fn dead_or_suspect_age_returns_age_for_dead_member() {
        let snap = make_snapshot(vec![make_member("site-a", MemberStatus::Dead, 600)]);
        let age = dead_or_suspect_age_secs("site-a", Some(&snap));
        assert_eq!(age, Some(600), "Dead member with real age must return Some(age)");
    }

    #[test]
    fn dead_or_suspect_age_returns_age_for_suspect_member() {
        let snap = make_snapshot(vec![make_member("site-a", MemberStatus::Suspect, 45)]);
        let age = dead_or_suspect_age_secs("site-a", Some(&snap));
        assert_eq!(age, Some(45), "Suspect member with real age must return Some(age)");
    }

    // -----------------------------------------------------------------------
    // apply_stale_gc_filter — overlay-level expiry
    // -----------------------------------------------------------------------

    fn make_crdt_available(site_id: &str) -> crdt::ProviderState {
        crdt::ProviderState {
            network_id: "net".to_owned(),
            site_id: site_id.to_owned(),
            provider_id: "prov".to_owned(),
            routing_cluster: site_id.to_owned(),
            models: vec!["model".to_owned()],
            backend_kind: "remote".to_owned(),
            phase: crdt::ProviderPhase::Available,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            access_policy: crdt::ProviderAccessPolicy::default(),
            revision: 1,
            writer_id: "w".to_owned(),
        }
    }

    fn make_crdt_degraded(site_id: &str) -> crdt::ProviderState {
        crdt::ProviderState {
            phase: crdt::ProviderPhase::Degraded,
            ..make_crdt_available(site_id)
        }
    }

    #[test]
    fn gc_filter_no_ttl_retains_all() {
        let policy = StaleCandidatePolicy {
            dead_member_ttl_secs: None,
        };
        let providers = vec![make_crdt_degraded("site-a")];
        let snap = make_snapshot(vec![make_member("site-a", MemberStatus::Dead, 999_999)]);
        let result = apply_stale_gc_filter(&providers, Some(&snap), &policy);
        assert_eq!(result.len(), 1, "no TTL must retain all providers");
    }

    #[test]
    fn gc_filter_retains_fresh_candidate_even_at_ttl() {
        let policy = StaleCandidatePolicy {
            dead_member_ttl_secs: Some(60),
        };
        let providers = vec![make_crdt_available("site-a")]; // fresh (Available)
        let snap = make_snapshot(vec![make_member("site-a", MemberStatus::Alive, 0)]);
        let result = apply_stale_gc_filter(&providers, Some(&snap), &policy);
        assert_eq!(
            result.len(),
            1,
            "fresh/Available provider must be retained regardless of TTL"
        );
    }

    #[test]
    fn gc_filter_retains_stale_below_ttl() {
        let policy = StaleCandidatePolicy {
            dead_member_ttl_secs: Some(3_600),
        };
        let providers = vec![make_crdt_degraded("site-a")];
        let snap = make_snapshot(vec![make_member("site-a", MemberStatus::Dead, 300)]);
        let result = apply_stale_gc_filter(&providers, Some(&snap), &policy);
        assert_eq!(result.len(), 1, "stale provider with age < TTL must be retained");
    }

    #[test]
    fn gc_filter_evicts_stale_at_or_above_ttl() {
        let policy = StaleCandidatePolicy {
            dead_member_ttl_secs: Some(3_600),
        };
        let providers = vec![make_crdt_degraded("site-a")];
        let snap = make_snapshot(vec![make_member("site-a", MemberStatus::Dead, 3_600)]);
        let result = apply_stale_gc_filter(&providers, Some(&snap), &policy);
        assert!(result.is_empty(), "stale provider with age >= TTL must be evicted");
    }

    #[test]
    fn gc_filter_retains_stale_with_zero_age_conservatively() {
        // age_secs=0 → sub-second age or synthetic snapshot without age.
        // Retain conservatively.
        let policy = StaleCandidatePolicy {
            dead_member_ttl_secs: Some(60),
        };
        let providers = vec![make_crdt_degraded("site-a")];
        let snap = make_snapshot(vec![make_member("site-a", MemberStatus::Dead, 0)]);
        let result = apply_stale_gc_filter(&providers, Some(&snap), &policy);
        assert_eq!(result.len(), 1, "stale with age_secs=0 must be retained (unknown age)");
    }

    #[test]
    fn gc_filter_default_policy_is_noop() {
        // Default policy (None TTL) must not evict anything at any age.
        let policy = StaleCandidatePolicy::default();
        let providers = vec![make_crdt_degraded("site-a"), make_crdt_degraded("site-b")];
        let snap = make_snapshot(vec![
            make_member("site-a", MemberStatus::Dead, 999_999),
            make_member("site-b", MemberStatus::Suspect, 999_999),
        ]);
        let result = apply_stale_gc_filter(&providers, Some(&snap), &policy);
        assert_eq!(result.len(), 2, "default policy must retain all providers (no TTL)");
    }

    // -----------------------------------------------------------------------
    // should_retain_candidate — stale candidate GC policy
    // -----------------------------------------------------------------------

    #[test]
    fn retain_fresh_candidate_regardless_of_age_and_ttl() {
        // Rule 1: fresh=true is never evicted, even with an old age and tight TTL.
        let policy = StaleCandidatePolicy {
            dead_member_ttl_secs: Some(60),
        };
        assert!(
            should_retain_candidate(true, Some(9_999), &policy),
            "fresh=true must never be evicted regardless of age or TTL"
        );
    }

    #[test]
    fn retain_stale_candidate_when_no_ttl_configured() {
        // Rule 2: no TTL → retain indefinitely regardless of age.
        let policy = StaleCandidatePolicy {
            dead_member_ttl_secs: None,
        };
        assert!(
            should_retain_candidate(false, Some(99_999), &policy),
            "fresh=false with no TTL must be retained indefinitely"
        );
    }

    #[test]
    fn retain_stale_candidate_when_age_unknown() {
        // Rule 3: no age data (age_secs always 0 in current runtime) → retain conservatively.
        let policy = StaleCandidatePolicy {
            dead_member_ttl_secs: Some(300),
        };
        assert!(
            should_retain_candidate(false, None, &policy),
            "fresh=false with unknown age must be retained conservatively"
        );
    }

    #[test]
    fn retain_stale_candidate_when_age_below_ttl() {
        // Rule 4: age < TTL → retain (candidate is stale but within the window).
        let policy = StaleCandidatePolicy {
            dead_member_ttl_secs: Some(300),
        };
        assert!(
            should_retain_candidate(false, Some(120), &policy),
            "fresh=false with age < TTL must be retained"
        );
    }

    #[test]
    fn evict_stale_candidate_when_age_meets_ttl() {
        // Rule 5 (boundary): age == TTL → evict.
        let policy = StaleCandidatePolicy {
            dead_member_ttl_secs: Some(300),
        };
        assert!(
            !should_retain_candidate(false, Some(300), &policy),
            "fresh=false with age == TTL must be evicted"
        );
    }

    #[test]
    fn evict_stale_candidate_when_age_exceeds_ttl() {
        // Rule 5: age > TTL → evict.
        let policy = StaleCandidatePolicy {
            dead_member_ttl_secs: Some(300),
        };
        assert!(
            !should_retain_candidate(false, Some(7200), &policy),
            "fresh=false with age > TTL must be evicted"
        );
    }

    #[test]
    fn default_policy_retains_all_stale_candidates() {
        // Current default: None TTL → retain everything.
        // This ensures that until real age data is available, no candidate
        // is silently evicted at runtime.
        let policy = StaleCandidatePolicy::default();
        assert!(
            should_retain_candidate(false, Some(999_999), &policy),
            "default policy must retain all stale candidates (no TTL)"
        );
        assert!(
            should_retain_candidate(false, None, &policy),
            "default policy must retain stale candidates with unknown age"
        );
    }

    #[test]
    fn local_fresh_candidate_never_evicted_by_stale_policy() {
        // Local candidates carry fresh=true by design (they are never Degraded
        // by apply_swim_staleness_override for their own site).  This test
        // documents that fresh=true candidates are not subject to GC.
        let tight_policy = StaleCandidatePolicy {
            dead_member_ttl_secs: Some(1),
        };
        assert!(
            should_retain_candidate(true, Some(9_999), &tight_policy),
            "local fresh candidate must not be evicted even with a tight TTL and large age"
        );
    }

    #[test]
    fn render_overlay_local_providers_unchanged_with_empty_remote() {
        let network = test_network("net");
        let local_prov = test_provider_with_backend_kind("prov-local", "net", "local");
        let overlay = render_routing_overlay(
            &network,
            &[],
            &[local_prov],
            &[],
            "local-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "local provider must appear with no remote providers"
        );
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("prov-local"),
            "local provider must be the sole candidate when remote CRDT providers is empty"
        );
    }

    // -----------------------------------------------------------------------
    // Access policy evaluation tests
    // -----------------------------------------------------------------------

    /// Test utility to create an [`AccessPolicy`] with the given match labels.
    fn test_access_policy(labels: &[(&str, &str)]) -> AccessPolicy {
        use crate::crd::auth::SelectorConfig;
        let match_labels = labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        AccessPolicy {
            site_selector: SelectorConfig { match_labels },
        }
    }

    /// Test utility to create consumer site labels from key-value pairs.
    fn test_site_labels(labels: &[(&str, &str)]) -> BTreeMap<String, String> {
        labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    /// Test utility to create a remote CRDT provider with access policy labels.
    fn test_crdt_provider_with_access_policy(
        site_id: &str,
        provider_id: &str,
        access_policy_labels: &[(&str, &str)],
    ) -> crdt::ProviderState {
        let access_policy = crdt::ProviderAccessPolicy {
            match_labels: access_policy_labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        };
        crdt::ProviderState {
            network_id: "net".to_owned(),
            site_id: site_id.to_owned(),
            provider_id: provider_id.to_owned(),
            routing_cluster: provider_id.to_owned(),
            models: vec!["model-remote".to_owned()],
            backend_kind: "remote".to_owned(),
            phase: crdt::ProviderPhase::Available,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            access_policy,
            revision: 1,
            writer_id: site_id.to_owned(),
        }
    }

    /// Test utility to create an unrestricted remote CRDT provider.
    fn test_crdt_provider_unrestricted(site_id: &str, provider_id: &str) -> crdt::ProviderState {
        test_crdt_provider_with_access_policy(site_id, provider_id, &[])
    }

    #[test]
    fn evaluate_access_policy_empty_policy_allows_all() {
        let policy = test_access_policy(&[]);
        let result = evaluate_access_policy(&policy, None);
        assert_eq!(
            result,
            AccessPolicyResult::Allow,
            "empty access policy must allow all consumers"
        );

        let labels = test_site_labels(&[("env", "prod")]);
        let result = evaluate_access_policy(&policy, Some(&labels));
        assert_eq!(
            result,
            AccessPolicyResult::Allow,
            "empty access policy must allow any consumer"
        );
    }

    #[test]
    fn evaluate_access_policy_matching_labels_allow() {
        let policy = test_access_policy(&[("env", "prod"), ("region", "us-west")]);
        let labels = test_site_labels(&[("env", "prod"), ("region", "us-west"), ("team", "platform")]);
        let result = evaluate_access_policy(&policy, Some(&labels));
        assert_eq!(
            result,
            AccessPolicyResult::Allow,
            "consumer with matching labels must be allowed"
        );
    }

    #[test]
    fn evaluate_access_policy_subset_labels_allow() {
        let policy = test_access_policy(&[("env", "prod")]);
        let labels = test_site_labels(&[("env", "prod"), ("region", "us-west")]);
        let result = evaluate_access_policy(&policy, Some(&labels));
        assert_eq!(
            result,
            AccessPolicyResult::Allow,
            "consumer with superset labels must be allowed"
        );
    }

    #[test]
    fn evaluate_access_policy_missing_required_label_deny() {
        let policy = test_access_policy(&[("env", "prod"), ("region", "us-west")]);
        let labels = test_site_labels(&[("env", "prod")]);
        let result = evaluate_access_policy(&policy, Some(&labels));
        assert_eq!(
            result,
            AccessPolicyResult::Deny,
            "consumer missing required label must be denied"
        );
    }

    #[test]
    fn evaluate_access_policy_wrong_label_value_deny() {
        let policy = test_access_policy(&[("env", "prod")]);
        let labels = test_site_labels(&[("env", "staging")]);
        let result = evaluate_access_policy(&policy, Some(&labels));
        assert_eq!(
            result,
            AccessPolicyResult::Deny,
            "consumer with wrong label value must be denied"
        );
    }

    #[test]
    fn evaluate_access_policy_no_consumer_labels_unknown() {
        let policy = test_access_policy(&[("env", "prod")]);
        let result = evaluate_access_policy(&policy, None);
        assert_eq!(
            result,
            AccessPolicyResult::Unknown,
            "unknown consumer with restricted provider must return unknown"
        );
    }

    #[test]
    fn evaluate_access_policy_empty_consumer_labels_deny() {
        let policy = test_access_policy(&[("env", "prod")]);
        let labels = test_site_labels(&[]);
        let result = evaluate_access_policy(&policy, Some(&labels));
        assert_eq!(
            result,
            AccessPolicyResult::Deny,
            "consumer with no labels but restricted provider must be denied"
        );
    }

    #[test]
    fn evaluate_access_policy_exact_match_required() {
        let policy = test_access_policy(&[("env", "prod")]);
        let labels = test_site_labels(&[("env", "production")]);
        let result = evaluate_access_policy(&policy, Some(&labels));
        assert_eq!(result, AccessPolicyResult::Deny, "label values must match exactly");
    }

    // -----------------------------------------------------------------------
    // Access policy integration tests with CRD objects
    // -----------------------------------------------------------------------

    /// Test utility to create a provider with access policy labels.
    fn test_provider_with_access_policy(
        name: &str,
        network: &str,
        models: &[&str],
        access_policy_labels: &[(&str, &str)],
    ) -> InferenceProvider {
        let models_json: Vec<serde_json::Value> = models.iter().map(|m| serde_json::json!({ "name": m })).collect();
        let match_labels: serde_json::Map<String, serde_json::Value> = access_policy_labels
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
                "accessPolicy": { "siteSelector": { "matchLabels": match_labels } }
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn overlay_unrestricted_provider_allows_all_consumers() {
        // Provider with empty access policy should allow all consumers (existing behavior)
        let network = test_network("net");
        let site_prod = test_site_with_labels("site-prod", "net", &[("env", "prod")]);
        let site_staging = test_site_with_labels("site-staging", "net", &[("env", "staging")]);
        let provider = test_provider("unrestricted-prov", "net", &["model-a"]);

        // The provider has an empty siteSelector, so it should generate candidates
        // for ALL sites in the network when it appears in any overlay.
        // With both sites present and an unrestricted access policy,
        // we get one candidate per site.

        // Consumer from prod site should get candidates for all sites
        let overlay = render_routing_overlay(
            &network,
            &[site_prod.clone(), site_staging.clone()],
            std::slice::from_ref(&provider),
            &[],
            "site-prod",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            2,
            "unrestricted provider with empty selector must serve all sites"
        );
        let site_names: BTreeSet<&str> = overlay.candidates.iter().map(|c| c.site.as_str()).collect();
        assert!(site_names.contains("site-prod"), "must include prod site");
        assert!(site_names.contains("site-staging"), "must include staging site");

        // Consumer from staging site should get the same candidates (all sites)
        let overlay = render_routing_overlay(
            &network,
            &[site_prod, site_staging],
            &[provider],
            &[],
            "site-staging",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            2,
            "unrestricted provider must serve all sites regardless of consumer"
        );
    }

    #[test]
    fn overlay_restricted_provider_allows_matching_consumer() {
        // Provider requiring "env=prod" should allow consumer with matching label
        let network = test_network("net");
        let site_prod = test_site_with_labels("site-prod", "net", &[("env", "prod"), ("region", "us-west")]);
        let site_staging = test_site_with_labels("site-staging", "net", &[("env", "staging")]);
        let provider = test_provider_with_access_policy("prod-only-prov", "net", &["model-a"], &[("env", "prod")]);

        // Consumer from prod site should get candidates
        // Since the provider has an empty siteSelector but restricted access policy,
        // it will generate candidates for all sites if the consumer passes access policy
        let overlay = render_routing_overlay(
            &network,
            &[site_prod.clone(), site_staging.clone()],
            std::slice::from_ref(&provider),
            &[],
            "site-prod",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            2,
            "matching consumer must get candidates for all sites from restricted provider"
        );
        assert!(
            overlay.candidates.iter().all(|c| c.cluster == "prod-only-prov"),
            "all candidates must be from restricted provider"
        );

        // Consumer from staging site should get no candidates due to access policy
        let overlay = render_routing_overlay(
            &network,
            &[site_prod, site_staging],
            &[provider],
            &[],
            "site-staging",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            0,
            "non-matching consumer must get no candidates from restricted provider"
        );
    }

    #[test]
    fn overlay_restricted_provider_denies_non_matching_consumer() {
        // Provider requiring specific labels should deny consumer without those labels
        let network = test_network("net");
        let site_prod = test_site_with_labels("site-prod", "net", &[("env", "prod"), ("team", "platform")]);
        let site_wrong_env = test_site_with_labels("site-staging", "net", &[("env", "staging"), ("team", "platform")]);
        let site_wrong_team = test_site_with_labels("site-other", "net", &[("env", "prod"), ("team", "ml")]);
        let provider = test_provider_with_access_policy(
            "restricted-prov",
            "net",
            &["model-a"],
            &[("env", "prod"), ("team", "platform")],
        );

        // Consumer with exact matching labels should get candidates
        let overlay = render_routing_overlay(
            &network,
            std::slice::from_ref(&site_prod),
            std::slice::from_ref(&provider),
            &[],
            "site-prod",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "exactly matching consumer must get candidates"
        );

        // Consumer with wrong env should get no candidates
        let overlay = render_routing_overlay(
            &network,
            &[site_prod.clone(), site_wrong_env.clone()],
            std::slice::from_ref(&provider),
            &[],
            "site-staging",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            0,
            "consumer with wrong env must get no candidates"
        );

        // Consumer with wrong team should get no candidates
        let overlay = render_routing_overlay(
            &network,
            &[site_prod, site_wrong_env, site_wrong_team],
            &[provider],
            &[],
            "site-other",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            0,
            "consumer with wrong team must get no candidates"
        );
    }

    #[test]
    fn overlay_unknown_consumer_fails_closed_for_restricted_provider() {
        // When consumer site identity is unknown, restricted providers should fail closed
        let network = test_network("net");
        let site = test_site_with_labels("known-site", "net", &[("env", "prod")]);
        let provider_restricted =
            test_provider_with_access_policy("restricted-prov", "net", &["model-a"], &[("env", "prod")]);
        let provider_unrestricted = test_provider("unrestricted-prov", "net", &["model-b"]);

        // Consumer site not in the sites list (unknown identity)
        let overlay = render_routing_overlay(
            &network,
            &[site],
            &[provider_restricted, provider_unrestricted],
            &[],
            "unknown-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        // Should only get candidates from unrestricted provider for the known site
        assert_eq!(
            overlay.candidates.len(),
            1,
            "only unrestricted provider should serve unknown consumer"
        );
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("unrestricted-prov"),
            "candidate must be from unrestricted provider"
        );
    }

    #[test]
    fn overlay_mixed_providers_access_policy_independence() {
        // Multiple providers with different access policies should be evaluated independently
        let network = test_network("net");
        let site_prod = test_site_with_labels("site-prod", "net", &[("env", "prod"), ("team", "platform")]);
        let site_staging = test_site_with_labels("site-staging", "net", &[("env", "staging"), ("team", "platform")]);

        let provider_unrestricted = test_provider("unrestricted-prov", "net", &["model-general"]);
        let provider_prod_only =
            test_provider_with_access_policy("prod-only-prov", "net", &["model-prod"], &[("env", "prod")]);
        let provider_platform_only = test_provider_with_access_policy(
            "platform-only-prov",
            "net",
            &["model-platform"],
            &[("team", "platform")],
        );

        // Prod consumer should get all three providers (each provider generates candidates for both sites)
        let overlay = render_routing_overlay(
            &network,
            &[site_prod, site_staging.clone()],
            &[
                provider_unrestricted.clone(),
                provider_prod_only.clone(),
                provider_platform_only.clone(),
            ],
            &[],
            "site-prod",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        // With 2 sites and 3 providers, we expect 6 candidates (3 providers × 2 sites each)
        assert_eq!(
            overlay.candidates.len(),
            6,
            "prod consumer should get candidates from all providers for all sites"
        );
        let clusters: BTreeSet<&str> = overlay.candidates.iter().map(|c| c.cluster.as_str()).collect();
        assert!(
            clusters.contains("unrestricted-prov"),
            "must include unrestricted provider"
        );
        assert!(clusters.contains("prod-only-prov"), "must include prod-only provider");
        assert!(
            clusters.contains("platform-only-prov"),
            "must include platform-only provider"
        );

        // Staging consumer should get unrestricted + platform-only (but not prod-only)
        let overlay = render_routing_overlay(
            &network,
            &[site_staging],
            &[provider_unrestricted, provider_prod_only, provider_platform_only],
            &[],
            "site-staging",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        // Staging site only, 2 allowed providers (unrestricted + platform-only) = 2 candidates
        assert_eq!(
            overlay.candidates.len(),
            2,
            "staging consumer should get filtered candidates"
        );
        let clusters: BTreeSet<&str> = overlay.candidates.iter().map(|c| c.cluster.as_str()).collect();
        assert!(
            clusters.contains("unrestricted-prov"),
            "must include unrestricted provider"
        );
        assert!(!clusters.contains("prod-only-prov"), "must exclude prod-only provider");
        assert!(
            clusters.contains("platform-only-prov"),
            "must include platform-only provider"
        );
    }

    // -----------------------------------------------------------------------
    // Comprehensive access policy tests (local + remote CRDT)
    // -----------------------------------------------------------------------

    #[test]
    fn local_unrestricted_provider_still_appears() {
        // Local provider with empty access policy should allow all consumers (existing behavior)
        let network = test_network("net");
        let site_prod = test_site_with_labels("site-prod", "net", &[("env", "prod")]);
        let provider = test_provider("local-unrestricted-prov", "net", &["model-a"]);

        let overlay = render_routing_overlay(
            &network,
            &[site_prod],
            std::slice::from_ref(&provider),
            &[],
            "site-prod",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 1, "local unrestricted provider must appear");
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("local-unrestricted-prov"),
            "candidate must be from local unrestricted provider"
        );
    }

    #[test]
    fn local_restricted_provider_appears_only_for_matching_consumer_site() {
        // Local provider requiring specific labels should allow matching consumer, deny others
        let network = test_network("net");
        let site_prod = test_site_with_labels("site-prod", "net", &[("env", "prod")]);
        let site_staging = test_site_with_labels("site-staging", "net", &[("env", "staging")]);
        let provider =
            test_provider_with_access_policy("local-restricted-prov", "net", &["model-a"], &[("env", "prod")]);

        // Matching consumer should get candidates
        let overlay = render_routing_overlay(
            &network,
            std::slice::from_ref(&site_prod),
            std::slice::from_ref(&provider),
            &[],
            "site-prod",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "matching consumer must get candidates from local restricted provider"
        );

        // Non-matching consumer should get no candidates
        let overlay = render_routing_overlay(
            &network,
            &[site_prod, site_staging],
            std::slice::from_ref(&provider),
            &[],
            "site-staging",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            0,
            "non-matching consumer must get no candidates from local restricted provider"
        );
    }

    #[test]
    fn local_restricted_provider_is_denied_for_nonmatching_consumer_site() {
        // Same as above but specifically testing the deny case
        let network = test_network("net");
        let site_wrong = test_site_with_labels("site-wrong", "net", &[("env", "staging"), ("team", "ml")]);
        let provider = test_provider_with_access_policy(
            "local-restricted-prov",
            "net",
            &["model-a"],
            &[("env", "prod"), ("team", "platform")],
        );

        let overlay = render_routing_overlay(
            &network,
            &[site_wrong],
            std::slice::from_ref(&provider),
            &[],
            "site-wrong",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            0,
            "consumer with wrong labels must get no candidates from local restricted provider"
        );
    }

    #[test]
    fn local_restricted_provider_is_denied_when_consumer_site_is_unknown() {
        // When consumer site identity is unknown, local restricted providers should fail closed
        let network = test_network("net");
        let site = test_site_with_labels("known-site", "net", &[("env", "prod")]);
        let provider_restricted =
            test_provider_with_access_policy("local-restricted-prov", "net", &["model-a"], &[("env", "prod")]);
        let provider_unrestricted = test_provider("local-unrestricted-prov", "net", &["model-b"]);

        // Consumer site not in the sites list (unknown identity)
        let overlay = render_routing_overlay(
            &network,
            &[site],
            &[provider_restricted, provider_unrestricted],
            &[],
            "unknown-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        // Should only get candidates from unrestricted provider
        assert_eq!(
            overlay.candidates.len(),
            1,
            "only unrestricted local provider should serve unknown consumer"
        );
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("local-unrestricted-prov"),
            "candidate must be from unrestricted local provider"
        );
    }

    #[test]
    fn remote_unrestricted_crdt_provider_still_appears() {
        // Remote CRDT provider with empty access policy should allow all consumers
        let network = test_network("net");
        let site_prod = test_site_with_labels("site-prod", "net", &[("env", "prod")]);
        let remote_provider = test_crdt_provider_unrestricted("remote-site", "remote-unrestricted-prov");

        let overlay = render_routing_overlay(
            &network,
            &[site_prod],
            &[],
            std::slice::from_ref(&remote_provider),
            "site-prod",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "remote unrestricted CRDT provider must appear"
        );
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("remote-unrestricted-prov"),
            "candidate must be from remote unrestricted provider"
        );
    }

    #[test]
    fn remote_restricted_crdt_provider_appears_only_for_matching_consumer_site() {
        // Remote CRDT provider requiring specific labels should allow matching consumer, deny others
        let network = test_network("net");
        let site_prod = test_site_with_labels("site-prod", "net", &[("env", "prod")]);
        let site_staging = test_site_with_labels("site-staging", "net", &[("env", "staging")]);
        let remote_provider =
            test_crdt_provider_with_access_policy("remote-site", "remote-restricted-prov", &[("env", "prod")]);

        // Matching consumer should get candidates
        let overlay = render_routing_overlay(
            &network,
            std::slice::from_ref(&site_prod),
            &[],
            std::slice::from_ref(&remote_provider),
            "site-prod",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "matching consumer must get candidates from remote restricted provider"
        );

        // Non-matching consumer should get no candidates
        let overlay = render_routing_overlay(
            &network,
            &[site_prod, site_staging],
            &[],
            std::slice::from_ref(&remote_provider),
            "site-staging",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            0,
            "non-matching consumer must get no candidates from remote restricted provider"
        );
    }

    #[test]
    fn remote_restricted_crdt_provider_is_denied_for_nonmatching_consumer_site() {
        // Remote CRDT provider with specific requirements denies consumer with wrong labels
        let network = test_network("net");
        let site_wrong = test_site_with_labels("site-wrong", "net", &[("env", "staging"), ("team", "ml")]);
        let remote_provider = test_crdt_provider_with_access_policy(
            "remote-site",
            "remote-restricted-prov",
            &[("env", "prod"), ("team", "platform")],
        );

        let overlay = render_routing_overlay(
            &network,
            &[site_wrong],
            &[],
            std::slice::from_ref(&remote_provider),
            "site-wrong",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            0,
            "consumer with wrong labels must get no candidates from remote restricted provider"
        );
    }

    #[test]
    fn remote_restricted_crdt_provider_is_denied_when_consumer_site_is_unknown() {
        // When consumer site identity is unknown, remote restricted CRDT providers should fail closed
        let network = test_network("net");
        let site = test_site_with_labels("known-site", "net", &[("env", "prod")]);
        let remote_restricted =
            test_crdt_provider_with_access_policy("remote-site", "remote-restricted-prov", &[("env", "prod")]);
        let remote_unrestricted = test_crdt_provider_unrestricted("remote-site", "remote-unrestricted-prov");

        // Consumer site not in the sites list (unknown identity)
        let overlay = render_routing_overlay(
            &network,
            &[site],
            &[],
            &[remote_restricted, remote_unrestricted],
            "unknown-site",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        // Should only get candidates from unrestricted remote provider
        assert_eq!(
            overlay.candidates.len(),
            1,
            "only unrestricted remote provider should serve unknown consumer"
        );
        assert_eq!(
            overlay.candidates.first().map(|c| c.cluster.as_str()),
            Some("remote-unrestricted-prov"),
            "candidate must be from unrestricted remote provider"
        );
    }

    // -----------------------------------------------------------------------
    // Geography and admission ordering tests
    // -----------------------------------------------------------------------

    fn test_provider_on_site(
        name: &str,
        network: &str,
        site_label: &str,
        backend_kind: &str,
        models: &[&str],
    ) -> InferenceProvider {
        let models_json: Vec<serde_json::Value> = models.iter().map(|m| serde_json::json!({ "name": m })).collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "self_hosted",
                "backendKind": backend_kind,
                "endpoint": "http://localhost:8000",
                "models": models_json,
                "siteSelector": { "matchLabels": { "site": site_label } }
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_site_with_geography_and_label(
        name: &str,
        network: &str,
        region: Option<&str>,
        zone: Option<&str>,
    ) -> GridSite {
        let mut spec = serde_json::json!({ "gridNetworkRef": network });
        if let Some(r) = region {
            spec["region"] = serde_json::json!(r);
        }
        if let Some(z) = zone {
            spec["zone"] = serde_json::json!(z);
        }
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": name, "labels": { "site": name } },
            "spec": spec
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn same_region_existing_only_after_cross_region_new() {
        let network = test_network("net");
        let site_a = test_site_with_geography_and_label("site-a", "net", Some("us-east"), Some("az-1"));
        let site_b = test_site_with_geography_and_label("site-b", "net", Some("us-east"), Some("az-2"));
        let site_c = test_site_with_geography_and_label("site-c", "net", Some("eu-west"), Some("az-1"));

        let prov_local = test_provider_on_site("prov-local", "net", "site-b", "local", &["llm"]);
        let prov_remote = test_provider_on_site("prov-remote", "net", "site-c", "remote", &["llm"]);

        let mut metrics: HashMap<&str, scoring::BackendMetrics> = HashMap::new();
        metrics.insert(
            "prov-local",
            scoring::BackendMetrics::new(0.0, true, 0.5, 100.0, 0.5, 0.90),
        );
        metrics.insert(
            "prov-remote",
            scoring::BackendMetrics::new(0.0, true, 0.3, 100.0, 0.5, 0.2),
        );

        let overlay = render_routing_overlay(
            &network,
            &[site_a, site_b, site_c],
            &[prov_local, prov_remote],
            &[],
            "site-a",
            Some(&metrics),
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(overlay.candidates.len(), 2, "both candidates must be present");
        let first = &overlay.candidates[0];
        let second = &overlay.candidates[1];
        assert_eq!(
            first.admission_state,
            Some(AdmissionState::NewAndExisting),
            "cross-region healthy must be NewAndExisting"
        );
        assert_eq!(first.cluster, "prov-remote", "cross-region healthy must rank first");
        assert_eq!(
            second.admission_state,
            Some(AdmissionState::ExistingOnly),
            "same-region saturated must be ExistingOnly"
        );
        assert_eq!(second.cluster, "prov-local", "same-region saturated must rank second");
    }

    #[test]
    fn all_local_saturated_falls_through_to_remote() {
        let network = test_network("net");
        let site_a = test_site_with_geography_and_label("site-a", "net", Some("us-east"), Some("az-1"));
        let site_b = test_site_with_geography_and_label("site-b", "net", Some("us-east"), Some("az-2"));
        let site_c = test_site_with_geography_and_label("site-c", "net", Some("eu-west"), Some("az-1"));

        let prov_a = test_provider_on_site("prov-a", "net", "site-a", "local", &["llm"]);
        let prov_b = test_provider_on_site("prov-b", "net", "site-b", "local", &["llm"]);
        let prov_c = test_provider_on_site("prov-c", "net", "site-c", "remote", &["llm"]);

        let mut metrics: HashMap<&str, scoring::BackendMetrics> = HashMap::new();
        metrics.insert("prov-a", scoring::BackendMetrics::new(0.0, true, 0.5, 100.0, 0.5, 0.95));
        metrics.insert("prov-b", scoring::BackendMetrics::new(0.0, true, 0.92, 100.0, 0.5, 0.5));
        metrics.insert("prov-c", scoring::BackendMetrics::new(0.0, true, 0.3, 100.0, 0.5, 0.2));

        let overlay = render_routing_overlay(
            &network,
            &[site_a, site_b, site_c],
            &[prov_a, prov_b, prov_c],
            &[],
            "site-a",
            Some(&metrics),
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(overlay.candidates.len(), 3, "all three candidates must be present");
        assert_eq!(
            overlay.candidates[0].admission_state,
            Some(AdmissionState::NewAndExisting),
            "remote healthy must be first (NewAndExisting)"
        );
        assert_eq!(
            overlay.candidates[0].cluster, "prov-c",
            "cross-region healthy must rank first when all local are saturated"
        );
        assert!(
            overlay.candidates[1].admission_state == Some(AdmissionState::ExistingOnly)
                && overlay.candidates[2].admission_state == Some(AdmissionState::ExistingOnly),
            "saturated local providers must both be ExistingOnly"
        );
    }

    #[test]
    fn excluded_candidate_removed_from_output() {
        let network = test_network("net");
        let site_a = test_site_with_geography_and_label("site-a", "net", Some("us-east"), Some("az-1"));
        let site_b = test_site_with_geography_and_label("site-b", "net", Some("us-east"), Some("az-2"));

        let prov_healthy = test_provider_on_site("prov-healthy", "net", "site-a", "local", &["llm"]);
        let prov_dead = test_provider_on_site("prov-dead", "net", "site-b", "local", &["llm"]);

        let mut metrics: HashMap<&str, scoring::BackendMetrics> = HashMap::new();
        metrics.insert(
            "prov-healthy",
            scoring::BackendMetrics::new(0.0, true, 0.3, 100.0, 0.5, 0.2),
        );
        metrics.insert(
            "prov-dead",
            scoring::BackendMetrics::new(0.5, false, 0.3, 100.0, 0.5, 0.2),
        );

        let overlay = render_routing_overlay(
            &network,
            &[site_a, site_b],
            &[prov_healthy, prov_dead],
            &[],
            "site-a",
            Some(&metrics),
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(overlay.candidates.len(), 1, "unhealthy candidate must be excluded");
        assert_eq!(
            overlay.candidates[0].cluster, "prov-healthy",
            "only healthy candidate must remain"
        );
    }

    #[test]
    fn legacy_no_geography_preserves_score_order() {
        let network = test_network("net");
        let prov_local = test_provider_with_backend_kind("prov-local", "net", "local");
        let prov_api = test_provider_with_backend_kind("prov-api", "net", "api_provider");

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[prov_local, prov_api],
            &[],
            "local-gw",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(overlay.candidates.len(), 2, "both candidates must be present");
        assert_eq!(
            overlay.candidates[0].selection_tier,
            Some(LocalityTier::Unknown),
            "no GridSite records must produce Unknown tier"
        );
        assert_eq!(
            overlay.candidates[1].selection_tier,
            Some(LocalityTier::Unknown),
            "no GridSite records must produce Unknown tier"
        );
        assert_eq!(
            overlay.candidates[0].cluster, "prov-local",
            "local backend_kind must score higher than api_provider within Unknown tier"
        );
    }

    #[test]
    fn rank_assigned_after_final_ordering() {
        let network = test_network("net");
        let site_a = test_site_with_geography_and_label("site-a", "net", Some("us-east"), Some("az-1"));
        let site_b = test_site_with_geography_and_label("site-b", "net", Some("us-east"), Some("az-2"));
        let site_c = test_site_with_geography_and_label("site-c", "net", Some("eu-west"), Some("az-1"));

        let prov_a = test_provider_on_site("prov-a", "net", "site-a", "local", &["llm"]);
        let prov_b = test_provider_on_site("prov-b", "net", "site-b", "local", &["llm"]);
        let prov_c = test_provider_on_site("prov-c", "net", "site-c", "remote", &["llm"]);

        let overlay = render_routing_overlay(
            &network,
            &[site_a, site_b, site_c],
            &[prov_a, prov_b, prov_c],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        for (i, c) in overlay.candidates.iter().enumerate() {
            assert_eq!(
                c.rank,
                Some(u32::try_from(i).unwrap_or_else(|_| std::process::abort())),
                "candidate at position {i} must have rank {i}, got {:?}",
                c.rank
            );
        }
    }

    // -----------------------------------------------------------------------
    // Geography metadata tests
    // -----------------------------------------------------------------------

    #[test]
    fn overlay_metadata_in_json() {
        let network = test_network("net");
        let site_a = test_site_with_geography_and_label("site-a", "net", Some("us-east"), Some("az-1"));
        let prov = test_provider_on_site("prov-a", "net", "site-a", "local", &["llm"]);

        let overlay = render_routing_overlay(
            &network,
            &[site_a],
            &[prov],
            &[],
            "site-a",
            None,
            Some("2026-07-24T12:00:00Z"),
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        let json: serde_json::Value = serde_json::to_value(&overlay).unwrap_or_else(|_| std::process::abort());
        let candidate = json["candidates"][0].as_object().unwrap();
        assert!(candidate.contains_key("stable_id"), "stable_id must be present");
        assert!(
            candidate.contains_key("admission_state"),
            "admission_state must be present"
        );
        assert!(
            candidate.contains_key("selection_tier"),
            "selection_tier must be present"
        );
        assert!(candidate.contains_key("rank"), "rank must be present");
    }

    #[test]
    fn backward_compat_ignores_new_fields() {
        let json = r#"{
            "network": "net",
            "local_site": "site-a",
            "candidates": [{
                "kind": "inference_model",
                "name": "llm",
                "site": "site-a",
                "cluster": "prov-a",
                "fresh": true
            }]
        }"#;
        let overlay: RoutingOverlay = serde_json::from_str(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.network, "net", "network must survive round-trip");
        assert_eq!(overlay.local_site, "site-a", "local_site must survive round-trip");
        assert_eq!(overlay.candidates.len(), 1, "one candidate");
        let c = &overlay.candidates[0];
        assert_eq!(c.name, "llm", "name must survive round-trip");
        assert!(c.stable_id.is_none(), "absent stable_id must deserialize as None");
        assert!(
            c.admission_state.is_none(),
            "absent admission_state must deserialize as None"
        );
        assert!(
            c.selection_tier.is_none(),
            "absent selection_tier must deserialize as None"
        );
        assert!(c.rank.is_none(), "absent rank must deserialize as None");
        assert!(
            overlay.generated_at.is_none(),
            "absent generated_at must deserialize as None"
        );
    }

    #[test]
    fn generated_at_in_overlay() {
        let network = test_network("net");
        let prov = test_provider("prov-a", "net", &["llm"]);
        let ts = "2026-07-24T12:00:00Z";

        let overlay = render_routing_overlay(
            &network,
            &[],
            &[prov],
            &[],
            "net",
            None,
            Some(ts),
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            overlay.generated_at.as_deref(),
            Some(ts),
            "generated_at must match the passed timestamp"
        );
        let json: serde_json::Value = serde_json::to_value(&overlay).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            json["generated_at"].as_str(),
            Some(ts),
            "generated_at must appear in serialized JSON"
        );
    }

    #[test]
    fn selection_tier_populated() {
        let network = test_network("net");
        let site_a = test_site_with_geography_and_label("site-a", "net", Some("us-east"), Some("az-1"));
        let site_b = test_site_with_geography_and_label("site-b", "net", Some("us-east"), Some("az-2"));
        let prov = test_provider_on_site("prov-b", "net", "site-b", "local", &["llm"]);

        let overlay = render_routing_overlay(
            &network,
            &[site_a, site_b],
            &[prov],
            &[],
            "site-a",
            None,
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(overlay.candidates.len(), 1, "one candidate");
        assert_eq!(
            overlay.candidates[0].selection_tier,
            Some(LocalityTier::SameRegion),
            "same region different zone must produce SameRegion tier"
        );
        let json: serde_json::Value = serde_json::to_value(&overlay).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            json["candidates"][0]["selection_tier"].as_str(),
            Some("same_region"),
            "SameRegion must serialize as same_region"
        );
    }

    #[test]
    fn admission_state_from_saturated_metrics() {
        let network = test_network("net");
        let site_a = test_site_with_geography_and_label("site-a", "net", Some("us-east"), Some("az-1"));
        let prov = test_provider_on_site("prov-a", "net", "site-a", "local", &["llm"]);

        let mut metrics: HashMap<&str, scoring::BackendMetrics> = HashMap::new();
        metrics.insert("prov-a", scoring::BackendMetrics::new(0.0, true, 0.5, 100.0, 0.5, 0.90));

        let overlay = render_routing_overlay(
            &network,
            &[site_a],
            &[prov],
            &[],
            "site-a",
            Some(&metrics),
            None,
            &scoring::ScoringWeights::default(),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(overlay.candidates.len(), 1, "one candidate");
        assert_eq!(
            overlay.candidates[0].admission_state,
            Some(AdmissionState::ExistingOnly),
            "saturated queue must produce ExistingOnly"
        );
        let json: serde_json::Value = serde_json::to_value(&overlay).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            json["candidates"][0]["admission_state"].as_str(),
            Some("existing_only"),
            "ExistingOnly must serialize as existing_only"
        );
    }
}
