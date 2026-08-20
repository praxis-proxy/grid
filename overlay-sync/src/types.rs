// SPDX-License-Identifier: MIT

//! Overlay envelope wire types for deserialization.
//!
//! These types replicate the operator's canonical envelope structures
//! ([`OverlayEnvelope`], [`RoutingOverlay`], [`RoutingCandidate`]) with
//! identical `serde` attributes so the sidecar's digest computation
//! produces the same SHA-256 value as the operator.
//!
//! A one-shot init fetch publishes an operator-produced envelope before
//! Praxis starts. The long-running sidecar then validates and republishes
//! subsequent operator-produced envelopes.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Envelope
// ---------------------------------------------------------------------------

/// Versioned envelope wrapping a routing overlay.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct OverlayEnvelope {
    /// Envelope schema version (semver).
    pub(crate) schema_version: String,

    /// Content-addressed semantic revision.
    pub(crate) revision: ContentRevision,

    /// Content digest of the canonical semantic payload.
    pub(crate) content_digest: ContentDigest,

    /// Scope identifiers bounding the overlay's applicability.
    pub(crate) scope: OverlayScope,

    /// Provenance metadata for traceability.
    pub(crate) provenance: OverlayProvenance,

    /// The routing overlay payload.
    pub(crate) overlay: RoutingOverlay,
}

/// Content-addressed revision identifier.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ContentRevision {
    /// Revision kind (always `"content_addressed"` in v1).
    pub(crate) kind: String,

    /// Hash algorithm (always `"sha256"` in v1).
    pub(crate) algorithm: String,

    /// Hex-encoded digest value (64 lowercase hexadecimal characters).
    pub(crate) value: String,
}

/// Content digest of the canonical semantic payload.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ContentDigest {
    /// Hash algorithm (always `"sha256"` in v1).
    pub(crate) algorithm: String,

    /// Hex-encoded digest value.
    pub(crate) value: String,
}

/// Scope identifiers bounding the overlay's applicability.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct OverlayScope {
    /// Grid network name.
    pub(crate) network: String,

    /// Gateway name.
    pub(crate) gateway: String,

    /// Gateway namespace.
    pub(crate) namespace: String,

    /// Local site identifier for this gateway.
    pub(crate) local_site: String,
}

/// Provenance metadata for audit and debugging.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct OverlayProvenance {
    /// Producer identifier.
    pub(crate) producer: String,

    /// Producer version.
    pub(crate) producer_version: String,

    /// Producer-defined source name.
    pub(crate) source_name: String,

    /// Producer-defined source UID.
    pub(crate) source_uid: String,

    /// Producer-defined source generation.
    pub(crate) source_generation: i64,

    /// RFC 3339 timestamp when the overlay was rendered.
    pub(crate) rendered_at: String,
}

// ---------------------------------------------------------------------------
// Overlay payload
// ---------------------------------------------------------------------------

/// The full routing overlay for a single grid network.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct RoutingOverlay {
    /// Network name.
    pub(crate) network: String,

    /// Local site identifier.
    pub(crate) local_site: String,

    /// Routing candidates ordered by the operator's sort.
    pub(crate) candidates: Vec<RoutingCandidate>,

    /// Optional local request-selection policy from the Grid operator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) selection_policy: Option<SelectionPolicy>,

    /// RFC 3339 timestamp of when this overlay was rendered.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) generated_at: Option<String>,
}

/// Selection policy copied without interpretation by overlay-sync.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SelectionPolicy {
    /// Selection mode consumed by Praxis.
    pub(crate) mode: SelectionMode,
}

/// Wire-level mode validation. Overlay-sync does not choose or execute it.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum SelectionMode {
    /// Ordered selection.
    Deterministic,
    /// Equal local rotation.
    RoundRobin,
    /// Local random selection.
    Random,
}

/// A single routing candidate.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct RoutingCandidate {
    /// Candidate kind (`"inference_model"`).
    pub(crate) kind: String,

    /// Model name.
    pub(crate) name: String,

    /// Site name where this model is hosted.
    pub(crate) site: String,

    /// Upstream cluster identifier.
    pub(crate) cluster: String,

    /// Whether the candidate is healthy.
    pub(crate) fresh: bool,

    /// Credential reference (Secret locator only, never the value).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) credential: Option<ProjectedCredential>,

    /// Deterministic stable ID for session binding.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stable_id: Option<String>,

    /// Bounded admission state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) admission_state: Option<AdmissionState>,

    /// Locality tier between consumer and candidate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) selection_tier: Option<LocalityTier>,

    /// Weighted score from the scoring engine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) score: Option<f64>,

    /// Per-signal weighted contributions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) score_breakdown: Option<ScoreBreakdown>,

    /// Zero-based position in the final sorted overlay.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rank: Option<u32>,

    /// Producer-assigned active selection group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) selection_group: Option<u32>,
}

/// Credential reference projected alongside a routing candidate.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectedCredential {
    /// Authentication strategy.
    pub(crate) strategy: String,

    /// Reference to the Secret holding the credential.
    pub(crate) secret_ref: ProjectedCredentialRef,
}

/// A reference to a Kubernetes Secret holding a credential value.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectedCredentialRef {
    /// Secret name.
    pub(crate) name: String,

    /// Secret namespace.
    pub(crate) namespace: String,

    /// Key within `Secret.data`.
    pub(crate) key: String,
}

/// Bounded admission state derived from provider health and capacity.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AdmissionState {
    /// Accepts new and existing sessions.
    NewAndExisting,
    /// Preserves established sessions only.
    ExistingOnly,
    /// Not eligible for routing.
    #[serde(rename = "none")]
    Excluded,
}

/// Locality tier between consumer and provider sites.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LocalityTier {
    /// Same named site.
    SameSite,
    /// Same region and zone.
    SameZone,
    /// Same region, different zone.
    SameRegion,
    /// Different regions.
    CrossRegion,
    /// Geography could not be determined.
    Unknown,
}

/// Per-signal weighted score contributions.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct ScoreBreakdown {
    /// Locality contribution.
    pub(crate) locality: f64,
    /// Queue depth contribution.
    pub(crate) queue_depth: f64,
    /// KV cache contribution.
    pub(crate) kv_cache: f64,
    /// Prefix cache contribution.
    pub(crate) prefix_cache: f64,
    /// Latency contribution.
    pub(crate) latency: f64,
    /// Cost contribution.
    pub(crate) cost: f64,
    /// Sum of all weighted contributions.
    pub(crate) total: f64,
}
