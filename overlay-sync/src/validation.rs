// SPDX-License-Identifier: MIT

//! Overlay envelope validation.
//!
//! Validates the content-addressed envelope before the sidecar writes it
//! to the shared volume.  Uses the same RFC 8785 canonicalization and
//! SHA-256 algorithm as the operator so digests match exactly.

use std::{fmt, fmt::Write as _};

#[cfg(test)]
use serde::ser::Error as _;
use sha2::{Digest as _, Sha256};

#[cfg(test)]
use crate::types::RoutingOverlay;
use crate::types::{OverlayEnvelope, OverlayScope};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Supported schema version prefix.
const SUPPORTED_MAJOR: &str = "1.";

/// Maximum hex digest length.
const HEX_DIGEST_LEN: usize = 64;

// ---------------------------------------------------------------------------
// Rejection reasons (bounded label values for metrics)
// ---------------------------------------------------------------------------

/// Bounded reason for rejecting an overlay update.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RejectionReason {
    /// JSON could not be parsed.
    Malformed,
    /// Envelope schema version is not supported.
    UnsupportedSchema,
    /// Scope does not match the expected configuration.
    ScopeMismatch,
    /// Content digest does not match the recomputed value.
    DigestMismatch,
    /// Revision and content digest values disagree.
    RevisionDigestMismatch,
    /// Payload exceeds the configured size limit.
    Oversized,
    /// Incoming semantic revision is identical to the written revision.
    Unchanged,
}

impl fmt::Display for RejectionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => f.write_str("malformed"),
            Self::UnsupportedSchema => f.write_str("unsupported_schema"),
            Self::ScopeMismatch => f.write_str("scope_mismatch"),
            Self::DigestMismatch => f.write_str("digest_mismatch"),
            Self::RevisionDigestMismatch => f.write_str("revision_digest_mismatch"),
            Self::Oversized => f.write_str("oversized"),
            Self::Unchanged => f.write_str("unchanged"),
        }
    }
}

// ---------------------------------------------------------------------------
// Validation result
// ---------------------------------------------------------------------------

/// Result of validating an overlay envelope.
#[derive(Debug)]
pub(crate) struct ValidatedEnvelope {
    /// The verified revision hex string.
    pub(crate) revision: String,
    /// The raw JSON bytes (for atomic write).
    pub(crate) raw_bytes: Vec<u8>,
}

/// Validation error carrying a bounded rejection reason.
#[derive(Debug)]
pub(crate) struct ValidationError {
    /// Bounded reason for metrics.
    pub(crate) reason: RejectionReason,
    /// Human-readable detail (never contains sensitive values).
    pub(crate) detail: String,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.reason, self.detail)
    }
}

// ---------------------------------------------------------------------------
// Expected scope configuration
// ---------------------------------------------------------------------------

/// Expected scope for validation.
#[derive(Clone, Debug)]
pub(crate) struct ExpectedScope {
    /// Expected network name.
    pub(crate) network: String,
    /// Expected gateway name.
    pub(crate) gateway: String,
    /// Expected namespace.
    pub(crate) namespace: String,
    /// Expected local site.
    pub(crate) local_site: String,
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Validate raw JSON bytes as a Grid overlay envelope.
///
/// Checks: size, JSON syntax, schema version, scope, digest, and
/// revision/digest agreement.
pub(crate) fn validate_envelope(
    raw: &[u8],
    expected_scope: &ExpectedScope,
    max_bytes: usize,
    current_revision: Option<&str>,
) -> Result<ValidatedEnvelope, ValidationError> {
    if raw.len() > max_bytes {
        return Err(ValidationError {
            reason: RejectionReason::Oversized,
            detail: format!("payload {} bytes exceeds limit {max_bytes}", raw.len()),
        });
    }

    let (raw_value, envelope) = parse_envelope(raw)?;

    validate_schema_version(&envelope.schema_version)?;
    validate_scope(&envelope.scope, expected_scope)?;
    validate_revision_digest_agreement(&envelope)?;
    validate_content_digest(&raw_value, &envelope)?;

    let revision = envelope.revision.value;

    if let Some(current) = current_revision
        && revision == current
    {
        return Err(ValidationError {
            reason: RejectionReason::Unchanged,
            detail: "identical revision already written".to_owned(),
        });
    }

    Ok(ValidatedEnvelope {
        revision,
        raw_bytes: raw.to_vec(),
    })
}

/// Parse the raw envelope while converting both parser failures into the
/// bounded validation error type.
fn parse_envelope(raw: &[u8]) -> Result<(serde_json::Value, OverlayEnvelope), ValidationError> {
    let raw_value: serde_json::Value = serde_json::from_slice(raw).map_err(|e| ValidationError {
        reason: RejectionReason::Malformed,
        detail: format!("JSON parse failed: {e}"),
    })?;
    let envelope: OverlayEnvelope = serde_json::from_value(raw_value.clone()).map_err(|e| ValidationError {
        reason: RejectionReason::Malformed,
        detail: format!("envelope structure invalid: {e}"),
    })?;
    Ok((raw_value, envelope))
}

/// Check that the schema version is supported.
fn validate_schema_version(version: &str) -> Result<(), ValidationError> {
    if version.starts_with(SUPPORTED_MAJOR) {
        Ok(())
    } else {
        Err(ValidationError {
            reason: RejectionReason::UnsupportedSchema,
            detail: format!("schema_version {version:?} not supported (expected 1.x)"),
        })
    }
}

/// Check that the scope matches expected configuration.
fn validate_scope(scope: &OverlayScope, expected: &ExpectedScope) -> Result<(), ValidationError> {
    if scope.network != expected.network {
        return Err(ValidationError {
            reason: RejectionReason::ScopeMismatch,
            detail: format!("network {:?} != expected {:?}", scope.network, expected.network),
        });
    }
    if scope.gateway != expected.gateway {
        return Err(ValidationError {
            reason: RejectionReason::ScopeMismatch,
            detail: format!("gateway {:?} != expected {:?}", scope.gateway, expected.gateway),
        });
    }
    if scope.namespace != expected.namespace {
        return Err(ValidationError {
            reason: RejectionReason::ScopeMismatch,
            detail: format!("namespace {:?} != expected {:?}", scope.namespace, expected.namespace),
        });
    }
    if scope.local_site != expected.local_site {
        return Err(ValidationError {
            reason: RejectionReason::ScopeMismatch,
            detail: format!(
                "local_site {:?} != expected {:?}",
                scope.local_site, expected.local_site
            ),
        });
    }
    Ok(())
}

/// Check that `revision.value == content_digest.value` (v1 invariant).
fn validate_revision_digest_agreement(envelope: &OverlayEnvelope) -> Result<(), ValidationError> {
    if envelope.revision.value != envelope.content_digest.value {
        return Err(ValidationError {
            reason: RejectionReason::RevisionDigestMismatch,
            detail: "revision.value != content_digest.value".to_owned(),
        });
    }
    if envelope.revision.algorithm != "sha256" || envelope.content_digest.algorithm != "sha256" {
        return Err(ValidationError {
            reason: RejectionReason::UnsupportedSchema,
            detail: "only sha256 algorithm is supported".to_owned(),
        });
    }
    if envelope.revision.value.len() != HEX_DIGEST_LEN {
        return Err(ValidationError {
            reason: RejectionReason::DigestMismatch,
            detail: format!(
                "revision hex length {} != expected {HEX_DIGEST_LEN}",
                envelope.revision.value.len()
            ),
        });
    }
    Ok(())
}

/// Recompute the semantic digest and verify it matches the envelope.
fn validate_content_digest(
    raw_envelope: &serde_json::Value,
    envelope: &OverlayEnvelope,
) -> Result<(), ValidationError> {
    let raw_overlay = raw_envelope.get("overlay").ok_or_else(|| ValidationError {
        reason: RejectionReason::Malformed,
        detail: "overlay payload is missing".to_owned(),
    })?;
    let recomputed = compute_raw_semantic_digest(raw_overlay).map_err(|e| ValidationError {
        reason: RejectionReason::Malformed,
        detail: format!("cannot canonicalize overlay: {e}"),
    })?;

    if recomputed != envelope.content_digest.value {
        return Err(ValidationError {
            reason: RejectionReason::DigestMismatch,
            detail: "recomputed digest does not match envelope".to_owned(),
        });
    }
    Ok(())
}

/// Compute the semantic digest from the raw overlay JSON.
///
/// The operator's semantic payload includes the complete candidate objects.
/// Computing from raw JSON keeps additive routing metadata digest-significant
/// even when this sidecar does not understand the new field yet.
fn compute_raw_semantic_digest(overlay: &serde_json::Value) -> Result<String, String> {
    let object = overlay
        .as_object()
        .ok_or_else(|| "overlay must be a JSON object".to_owned())?;
    let network = object
        .get("network")
        .ok_or_else(|| "overlay.network is missing".to_owned())?;
    let local_site = object
        .get("local_site")
        .ok_or_else(|| "overlay.local_site is missing".to_owned())?;
    let candidates = object
        .get("candidates")
        .ok_or_else(|| "overlay.candidates is missing".to_owned())?;
    let mut semantic_payload = serde_json::json!({
        "candidates": candidates,
        "local_site": local_site,
        "network": network,
    });
    if let Some(selection_policy) = object.get("selection_policy") {
        semantic_payload
            .as_object_mut()
            .ok_or_else(|| "semantic payload is not an object".to_owned())?
            .insert("selection_policy".to_owned(), selection_policy.clone());
    }
    let canonical = serde_json_canonicalizer::to_vec(&semantic_payload)
        .map_err(|e| format!("RFC 8785 canonicalization failed: {e}"))?;
    let digest: [u8; 32] = Sha256::digest(&canonical).into();
    Ok(hex_encode(&digest))
}

// ---------------------------------------------------------------------------
// Digest computation (same algorithm as the operator)
// ---------------------------------------------------------------------------

/// Compute the SHA-256 digest of the RFC 8785 canonical semantic payload.
///
/// The semantic payload includes `network`, `local_site`, and `candidates`,
/// plus `selection_policy` when present — the same fields used by the
/// operator.
#[cfg(test)]
fn compute_semantic_digest(overlay: &RoutingOverlay) -> Result<String, serde_json::Error> {
    let candidates_value = serde_json::to_value(&overlay.candidates)?;
    let mut semantic_payload = serde_json::json!({
        "candidates": candidates_value,
        "local_site": overlay.local_site,
        "network": overlay.network,
    });
    if let Some(policy) = &overlay.selection_policy {
        let Some(object) = semantic_payload.as_object_mut() else {
            return Err(serde_json::Error::custom("semantic payload must be an object"));
        };
        object.insert("selection_policy".to_owned(), serde_json::to_value(policy)?);
    }

    let canonical = serde_json_canonicalizer::to_vec(&semantic_payload).map_err(serde_json::Error::custom)?;

    let digest: [u8; 32] = Sha256::digest(&canonical).into();
    Ok(hex_encode(&digest))
}

/// Encode a byte slice as lowercase hexadecimal.
#[expect(clippy::let_underscore_must_use, reason = "writing to String is infallible")]
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use super::*;
    use crate::types::{ContentDigest, ContentRevision, OverlayProvenance, RoutingCandidate, RoutingOverlay};

    fn test_scope() -> ExpectedScope {
        ExpectedScope {
            network: "test-net".to_owned(),
            gateway: "gw".to_owned(),
            namespace: "ns".to_owned(),
            local_site: "site-a".to_owned(),
        }
    }

    fn minimal_overlay() -> RoutingOverlay {
        RoutingOverlay {
            network: "test-net".to_owned(),
            local_site: "site-a".to_owned(),
            candidates: vec![RoutingCandidate {
                kind: "inference_model".to_owned(),
                name: "model-a".to_owned(),
                site: "site-a".to_owned(),
                cluster: "cluster-a".to_owned(),
                fresh: true,
                credential: None,
                stable_id: Some("abcd1234".to_owned()),
                admission_state: None,
                selection_tier: None,
                score: None,
                score_breakdown: None,
                rank: Some(0),
                selection_group: None,
            }],
            selection_policy: None,
            generated_at: Some("2026-07-29T00:00:00Z".to_owned()),
        }
    }

    fn valid_envelope() -> OverlayEnvelope {
        let overlay = minimal_overlay();
        let digest = compute_semantic_digest(&overlay).unwrap();
        OverlayEnvelope {
            schema_version: "1.0.0".to_owned(),
            revision: ContentRevision {
                kind: "content_addressed".to_owned(),
                algorithm: "sha256".to_owned(),
                value: digest.clone(),
            },
            content_digest: ContentDigest {
                algorithm: "sha256".to_owned(),
                value: digest,
            },
            scope: OverlayScope {
                network: "test-net".to_owned(),
                gateway: "gw".to_owned(),
                namespace: "ns".to_owned(),
                local_site: "site-a".to_owned(),
            },
            provenance: OverlayProvenance {
                producer: "grid-operator".to_owned(),
                producer_version: "0.1.1".to_owned(),
                source_name: "test-net".to_owned(),
                source_uid: "uid-1".to_owned(),
                source_generation: 1,
                rendered_at: "2026-07-29T00:00:00Z".to_owned(),
            },
            overlay,
        }
    }

    #[test]
    fn valid_envelope_passes() {
        let env = valid_envelope();
        let raw = serde_json::to_vec(&env).unwrap();
        let scope = test_scope();
        let result = validate_envelope(&raw, &scope, 1_048_576, None);
        result.unwrap();
    }

    #[test]
    fn additive_candidate_field_is_digest_significant_and_accepted() {
        let env = valid_envelope();
        let mut value = serde_json::to_value(env).unwrap();
        value["overlay"]["candidates"][0]["future_field"] = serde_json::json!({"mode": "x"});
        let digest = compute_raw_semantic_digest(&value["overlay"]).unwrap();
        value["revision"]["value"] = serde_json::Value::String(digest.clone());
        value["content_digest"]["value"] = serde_json::Value::String(digest);

        let raw = serde_json::to_vec(&value).unwrap();
        let result = validate_envelope(&raw, &test_scope(), 1_048_576, None);
        assert!(result.is_ok(), "additive candidate metadata should survive validation");
    }

    #[test]
    fn additive_candidate_field_mutation_without_digest_update_is_rejected() {
        let env = valid_envelope();
        let mut value = serde_json::to_value(env).unwrap();
        value["overlay"]["candidates"][0]["future_field"] = serde_json::json!({"mode": "x"});

        let raw = serde_json::to_vec(&value).unwrap();
        let result = validate_envelope(&raw, &test_scope(), 1_048_576, None);
        assert!(matches!(result.unwrap_err().reason, RejectionReason::DigestMismatch));
    }

    #[test]
    fn credential_value_fields_remain_rejected() {
        let env = valid_envelope();
        let mut value = serde_json::to_value(env).unwrap();
        value["overlay"]["candidates"][0]["credential"] = serde_json::json!({
            "strategy": "bearer_token",
            "secretRef": {
                "name": "credential",
                "namespace": "ns",
                "key": "token"
            },
            "token": "must-not-enter-overlay"
        });

        let raw = serde_json::to_vec(&value).unwrap();
        let result = validate_envelope(&raw, &test_scope(), 1_048_576, None);
        assert!(matches!(result.unwrap_err().reason, RejectionReason::Malformed));
    }

    #[test]
    fn oversized_payload_rejected() {
        let env = valid_envelope();
        let raw = serde_json::to_vec(&env).unwrap();
        let scope = test_scope();
        let result = validate_envelope(&raw, &scope, 10, None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err().reason, RejectionReason::Oversized));
    }

    #[test]
    fn malformed_json_rejected() {
        let raw = b"not valid json";
        let scope = test_scope();
        let result = validate_envelope(raw, &scope, 1_048_576, None);
        assert!(matches!(result.unwrap_err().reason, RejectionReason::Malformed));
    }

    #[test]
    fn unsupported_schema_rejected() {
        let mut env = valid_envelope();
        env.schema_version = "2.0.0".to_owned();
        let raw = serde_json::to_vec(&env).unwrap();
        let scope = test_scope();
        let result = validate_envelope(&raw, &scope, 1_048_576, None);
        assert!(matches!(result.unwrap_err().reason, RejectionReason::UnsupportedSchema));
    }

    #[test]
    fn scope_mismatch_rejected() {
        let env = valid_envelope();
        let raw = serde_json::to_vec(&env).unwrap();
        let mut scope = test_scope();
        scope.network = "wrong-net".to_owned();
        let result = validate_envelope(&raw, &scope, 1_048_576, None);
        assert!(matches!(result.unwrap_err().reason, RejectionReason::ScopeMismatch));
    }

    #[test]
    fn digest_mismatch_rejected() {
        let mut env = valid_envelope();
        env.content_digest.value = "0".repeat(64);
        env.revision.value = "0".repeat(64);
        let raw = serde_json::to_vec(&env).unwrap();
        let scope = test_scope();
        let result = validate_envelope(&raw, &scope, 1_048_576, None);
        assert!(matches!(result.unwrap_err().reason, RejectionReason::DigestMismatch));
    }

    #[test]
    fn revision_digest_disagreement_rejected() {
        let mut env = valid_envelope();
        env.revision.value = "1".repeat(64);
        let raw = serde_json::to_vec(&env).unwrap();
        let scope = test_scope();
        let result = validate_envelope(&raw, &scope, 1_048_576, None);
        assert!(matches!(
            result.unwrap_err().reason,
            RejectionReason::RevisionDigestMismatch
        ));
    }

    #[test]
    fn identical_revision_is_unchanged() {
        let env = valid_envelope();
        let revision = env.revision.value.clone();
        let raw = serde_json::to_vec(&env).unwrap();
        let scope = test_scope();
        let result = validate_envelope(&raw, &scope, 1_048_576, Some(&revision));
        assert!(matches!(result.unwrap_err().reason, RejectionReason::Unchanged));
    }

    #[test]
    fn digest_matches_operator_fixture() {
        let fixture = include_str!("../../tests/fixtures/overlay-contract/v1/valid-minimal.json");
        let envelope: OverlayEnvelope = serde_json::from_str(fixture).unwrap();
        let recomputed = compute_semantic_digest(&envelope.overlay).unwrap();
        assert_eq!(
            recomputed, envelope.revision.value,
            "sidecar digest must match operator fixture"
        );
    }

    #[test]
    fn selection_policy_fixture_digest_matches_operator_contract() {
        let fixture = include_str!("../../tests/fixtures/overlay-contract/v1/valid-selection-policy.json");
        let envelope: OverlayEnvelope = serde_json::from_str(fixture).unwrap();
        let recomputed = compute_semantic_digest(&envelope.overlay).unwrap();
        assert_eq!(
            recomputed, envelope.revision.value,
            "selection policy fixture must match the producer digest; computed={recomputed}"
        );
        let scope = test_scope();
        validate_envelope(fixture.as_bytes(), &scope, 1_048_576, None)
            .expect("selection policy fixture must pass transparent validation");
    }

    #[test]
    fn unknown_selection_policy_field_is_rejected() {
        let env = valid_envelope();
        let mut value = serde_json::to_value(env).unwrap();
        value["overlay"]["selection_policy"] = serde_json::json!({
            "mode": "roundRobin",
            "unexpected": true
        });
        let digest = compute_raw_semantic_digest(&value["overlay"]).unwrap();
        value["revision"]["value"] = serde_json::Value::String(digest.clone());
        value["content_digest"]["value"] = serde_json::Value::String(digest);

        let raw = serde_json::to_vec(&value).unwrap();
        let result = validate_envelope(&raw, &test_scope(), 1_048_576, None);
        assert!(matches!(result.unwrap_err().reason, RejectionReason::Malformed));
    }

    #[test]
    fn hex_encode_lowercase() {
        let bytes = [0xAB, 0xCD, 0x01, 0x23];
        assert_eq!(hex_encode(&bytes), "abcd0123");
    }
}
