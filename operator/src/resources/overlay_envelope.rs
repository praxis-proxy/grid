// SPDX-License-Identifier: MIT

//! Versioned overlay envelope for content-addressed revision tracking.
//!
//! Wraps a routing overlay in a versioned envelope with a SHA-256 digest
//! computed over the [RFC 8785][] canonical form of the semantic payload.
//! The semantic payload includes `network`, `local_site`, and `candidates`,
//! plus `selection_policy` when present, so the revision changes only when
//! routing behavior changes — timestamp and provenance changes are no-ops.
//!
//! The envelope is published as the `routing-overlay.json` key in the overlay
//! `ConfigMap`, alongside the `routing-config.json` legacy key.
//!
//! [RFC 8785]: https://www.rfc-editor.org/rfc/rfc8785

use std::fmt::Write as _;

use serde::{Deserialize, Serialize, ser::Error as _};
use sha2::{Digest as _, Sha256};

use super::routing_overlay::RoutingOverlay;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Schema version for the overlay envelope.
pub const SCHEMA_VERSION: &str = "1.0.0";

/// `ConfigMap` data key for the versioned envelope.
pub const ENVELOPE_KEY: &str = "routing-overlay.json";

/// `ConfigMap` annotation: schema version.
pub const ANNOTATION_SCHEMA_VERSION: &str = "grid.praxis-proxy.io/overlay-schema-version";

/// `ConfigMap` annotation: semantic revision.
pub const ANNOTATION_REVISION: &str = "grid.praxis-proxy.io/overlay-revision";

/// `ConfigMap` annotation: content digest.
pub const ANNOTATION_CONTENT_DIGEST: &str = "grid.praxis-proxy.io/overlay-content-digest";

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// Versioned envelope wrapping a routing overlay.
///
/// Published as JSON under the [`ENVELOPE_KEY`] key of the overlay
/// `ConfigMap`.  The `routing-config.json` key contains the bare
/// [`RoutingOverlay`] for legacy consumers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OverlayEnvelope {
    /// Envelope schema version (semver).
    pub schema_version: String,

    /// Content-addressed semantic revision.
    pub revision: ContentRevision,

    /// Content digest of the canonical semantic payload.
    pub content_digest: ContentDigest,

    /// Scope identifiers bounding the overlay's applicability.
    pub scope: OverlayScope,

    /// Provenance metadata for traceability.
    pub provenance: OverlayProvenance,

    /// The routing overlay payload.
    pub overlay: RoutingOverlay,
}

/// Content-addressed revision identifier.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContentRevision {
    /// Revision kind (always `"content_addressed"` in v1).
    pub kind: String,

    /// Hash algorithm (always `"sha256"` in v1).
    pub algorithm: String,

    /// Hex-encoded digest value (64 lowercase hexadecimal characters).
    pub value: String,
}

/// Content digest of the canonical semantic payload.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContentDigest {
    /// Hash algorithm (always `"sha256"` in v1).
    pub algorithm: String,

    /// Hex-encoded digest value (same as [`ContentRevision::value`] in v1).
    pub value: String,
}

/// Scope identifiers bounding the overlay's applicability.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OverlayScope {
    /// [`GridNetwork`] name.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub network: String,

    /// Gateway name from the `GatewayRef`.
    pub gateway: String,

    /// Gateway namespace.
    pub namespace: String,

    /// Local site identifier for this gateway.
    pub local_site: String,
}

/// Provenance metadata for audit and debugging.
///
/// Evidence only — not included in the semantic digest.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OverlayProvenance {
    /// Producer identifier (`"grid-operator"`).
    pub producer: String,

    /// Producer version from `CARGO_PKG_VERSION`.
    pub producer_version: String,

    /// Producer-defined source name. Grid uses the [`GridNetwork`] name,
    /// matching `scope.network`.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub source_name: String,

    /// Producer-defined source UID. Grid uses the [`GridNetwork`] resource UID.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub source_uid: String,

    /// Producer-defined source generation. Grid uses the [`GridNetwork`]
    /// observed generation.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub source_generation: i64,

    /// RFC 3339 timestamp when the overlay was rendered.
    pub rendered_at: String,
}

// ---------------------------------------------------------------------------
// Result of envelope construction
// ---------------------------------------------------------------------------

/// Result of building an overlay envelope, carrying the computed revision
/// for use in status updates and annotations.
#[derive(Debug)]
pub struct EnvelopeBuildResult {
    /// The constructed envelope.
    pub envelope: OverlayEnvelope,

    /// The semantic revision hex string (64 lowercase hex characters).
    pub revision_hex: String,
}

// ---------------------------------------------------------------------------
// Semantic payload and digest
// ---------------------------------------------------------------------------

/// Compute the SHA-256 digest of the RFC 8785 canonical semantic payload.
///
/// The semantic payload contains only routing-relevant fields: `network`,
/// `local_site`, and the ordered `candidates` array.  Timestamps,
/// provenance, and Kubernetes metadata are excluded so the revision
/// changes only when routing decisions would change.
///
/// [`RoutingOverlay`] fields serialize as default `snake_case`.
/// [`ProjectedCredential`] and [`ProjectedCredentialRef`] serialize as
/// `camelCase` (`rename_all = "camelCase"`).  The canonical payload
/// preserves these existing serialization conventions.
///
/// # Errors
///
/// Returns [`serde_json::Error`] if the overlay cannot be serialized or
/// the canonical form cannot be computed.
///
/// [`ProjectedCredential`]: super::routing_overlay::ProjectedCredential
/// [`ProjectedCredentialRef`]: super::routing_overlay::ProjectedCredentialRef
pub fn compute_semantic_digest(overlay: &RoutingOverlay) -> Result<String, serde_json::Error> {
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

/// Build an [`OverlayEnvelope`] from a rendered overlay and provenance
/// inputs.
///
/// Validates internal invariants before returning:
/// - `scope.network` equals `overlay.network`
/// - `scope.local_site` equals `overlay.local_site`
/// - `revision.value` equals `content_digest.value` (v1 invariant)
///
/// # Errors
///
/// Returns [`serde_json::Error`] if the semantic payload cannot be
/// serialized or canonicalized.
#[expect(
    clippy::too_many_arguments,
    reason = "all arguments are distinct provenance/scope fields"
)]
#[expect(clippy::too_many_lines, reason = "flat struct construction with all fields")]
pub fn build_overlay_envelope(
    overlay: &RoutingOverlay,
    gateway_name: &str,
    namespace: &str,
    network_uid: &str,
    network_generation: i64,
    rendered_at: &str,
) -> Result<EnvelopeBuildResult, serde_json::Error> {
    let revision_hex = compute_semantic_digest(overlay)?;

    let envelope = OverlayEnvelope {
        schema_version: SCHEMA_VERSION.to_owned(),
        revision: ContentRevision {
            kind: "content_addressed".to_owned(),
            algorithm: "sha256".to_owned(),
            value: revision_hex.clone(),
        },
        content_digest: ContentDigest {
            algorithm: "sha256".to_owned(),
            value: revision_hex.clone(),
        },
        scope: OverlayScope {
            network: overlay.network.clone(),
            gateway: gateway_name.to_owned(),
            namespace: namespace.to_owned(),
            local_site: overlay.local_site.clone(),
        },
        provenance: OverlayProvenance {
            producer: "grid-operator".to_owned(),
            producer_version: env!("CARGO_PKG_VERSION").to_owned(),
            source_name: overlay.network.clone(),
            source_uid: network_uid.to_owned(),
            source_generation: network_generation,
            rendered_at: rendered_at.to_owned(),
        },
        overlay: overlay.clone(),
    };

    debug_assert_eq!(
        envelope.scope.network, envelope.overlay.network,
        "scope.network must match overlay.network"
    );
    debug_assert_eq!(
        envelope.scope.local_site, envelope.overlay.local_site,
        "scope.local_site must match overlay.local_site"
    );
    debug_assert_eq!(
        envelope.revision.value, envelope.content_digest.value,
        "v1 revision must equal content_digest"
    );

    Ok(EnvelopeBuildResult { envelope, revision_hex })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
    use crate::resources::routing_overlay::{ProjectedCredential, ProjectedCredentialRef, RoutingCandidate};

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

    #[expect(
        clippy::too_many_lines,
        reason = "two-candidate test fixture with full field coverage"
    )]
    fn multi_candidate_overlay() -> RoutingOverlay {
        RoutingOverlay {
            network: "glb-demo".to_owned(),
            local_site: "east-edge".to_owned(),
            candidates: vec![
                RoutingCandidate {
                    kind: "inference_model".to_owned(),
                    name: "model-east".to_owned(),
                    site: "east-provider".to_owned(),
                    cluster: "sim-east-provider".to_owned(),
                    fresh: true,
                    credential: Some(ProjectedCredential {
                        strategy: "bearer_token".to_owned(),
                        secret_ref: ProjectedCredentialRef {
                            name: "east-cred".to_owned(),
                            namespace: "grid-system".to_owned(),
                            key: "token".to_owned(),
                        },
                    }),
                    stable_id: Some("east1234".to_owned()),
                    admission_state: None,
                    selection_tier: None,
                    score: None,
                    score_breakdown: None,
                    rank: Some(0),
                    selection_group: None,
                },
                RoutingCandidate {
                    kind: "inference_model".to_owned(),
                    name: "model-west".to_owned(),
                    site: "west-provider".to_owned(),
                    cluster: "sim-west-provider".to_owned(),
                    fresh: true,
                    credential: None,
                    stable_id: Some("west5678".to_owned()),
                    admission_state: None,
                    selection_tier: None,
                    score: None,
                    score_breakdown: None,
                    rank: Some(1),
                    selection_group: None,
                },
            ],
            selection_policy: None,
            generated_at: Some("2026-07-29T01:00:00Z".to_owned()),
        }
    }

    #[test]
    fn semantic_digest_is_deterministic() {
        let overlay = minimal_overlay();
        let d1 = compute_semantic_digest(&overlay).unwrap();
        let d2 = compute_semantic_digest(&overlay).unwrap();
        assert_eq!(d1, d2);
        assert_eq!(d1.len(), 64, "SHA-256 hex must be 64 characters");
        assert!(d1.chars().all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()));
    }

    #[test]
    fn semantic_digest_ignores_timestamp() {
        let mut o1 = minimal_overlay();
        let mut o2 = minimal_overlay();
        o1.generated_at = Some("2026-01-01T00:00:00Z".to_owned());
        o2.generated_at = Some("2099-12-31T23:59:59Z".to_owned());
        assert_eq!(
            compute_semantic_digest(&o1).unwrap(),
            compute_semantic_digest(&o2).unwrap(),
            "timestamp must not affect the semantic digest"
        );
    }

    #[test]
    fn semantic_digest_changes_on_candidate_order() {
        let o1 = multi_candidate_overlay();
        let mut o2 = multi_candidate_overlay();
        o2.candidates.swap(0, 1);
        assert_ne!(
            compute_semantic_digest(&o1).unwrap(),
            compute_semantic_digest(&o2).unwrap(),
            "candidate order is semantic"
        );
    }

    #[test]
    fn semantic_digest_changes_on_network() {
        let o1 = minimal_overlay();
        let mut o2 = minimal_overlay();
        o2.network = "other-net".to_owned();
        assert_ne!(
            compute_semantic_digest(&o1).unwrap(),
            compute_semantic_digest(&o2).unwrap(),
        );
    }

    #[test]
    fn semantic_digest_changes_on_local_site() {
        let o1 = minimal_overlay();
        let mut o2 = minimal_overlay();
        o2.local_site = "site-b".to_owned();
        assert_ne!(
            compute_semantic_digest(&o1).unwrap(),
            compute_semantic_digest(&o2).unwrap(),
        );
    }

    #[test]
    fn semantic_digest_changes_on_candidate_field() {
        let o1 = minimal_overlay();
        let mut o2 = minimal_overlay();
        o2.candidates[0].fresh = false;
        assert_ne!(
            compute_semantic_digest(&o1).unwrap(),
            compute_semantic_digest(&o2).unwrap(),
            "every routing-relevant field must affect the digest"
        );
    }

    #[test]
    fn envelope_schema_version() {
        let overlay = minimal_overlay();
        let result = build_overlay_envelope(&overlay, "gw", "ns", "uid-1", 1, "2026-07-29T00:00:00Z").unwrap();
        assert_eq!(result.envelope.schema_version, "1.0.0");
    }

    #[test]
    fn envelope_revision_equals_content_digest() {
        let overlay = minimal_overlay();
        let result = build_overlay_envelope(&overlay, "gw", "ns", "uid-1", 1, "2026-07-29T00:00:00Z").unwrap();
        assert_eq!(
            result.envelope.revision.value, result.envelope.content_digest.value,
            "v1 invariant: revision.value == content_digest.value"
        );
        assert_eq!(result.envelope.revision.kind, "content_addressed");
        assert_eq!(result.envelope.revision.algorithm, "sha256");
        assert_eq!(result.envelope.content_digest.algorithm, "sha256");
    }

    #[test]
    fn envelope_scope_matches_overlay() {
        let overlay = minimal_overlay();
        let result =
            build_overlay_envelope(&overlay, "my-gw", "grid-system", "uid-1", 1, "2026-07-29T00:00:00Z").unwrap();
        assert_eq!(result.envelope.scope.network, overlay.network);
        assert_eq!(result.envelope.scope.local_site, overlay.local_site);
        assert_eq!(result.envelope.scope.gateway, "my-gw");
        assert_eq!(result.envelope.scope.namespace, "grid-system");
    }

    #[test]
    fn envelope_provenance() {
        let overlay = minimal_overlay();
        let result = build_overlay_envelope(&overlay, "gw", "ns", "uid-42", 3, "2026-07-29T00:00:00Z").unwrap();
        assert_eq!(result.envelope.provenance.producer, "grid-operator");
        assert_eq!(result.envelope.provenance.source_name, "test-net");
        assert_eq!(result.envelope.provenance.source_uid, "uid-42");
        assert_eq!(result.envelope.provenance.source_generation, 3);
        assert_eq!(result.envelope.provenance.rendered_at, "2026-07-29T00:00:00Z");
        assert!(!result.envelope.provenance.producer_version.is_empty());
    }

    #[test]
    fn envelope_round_trips_through_json() {
        let overlay = multi_candidate_overlay();
        let result = build_overlay_envelope(&overlay, "gw", "ns", "uid-1", 1, "2026-07-29T00:00:00Z").unwrap();
        let json = serde_json::to_string_pretty(&result.envelope).unwrap();
        let deserialized: OverlayEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.schema_version, result.envelope.schema_version);
        assert_eq!(deserialized.revision.value, result.envelope.revision.value);
        assert_eq!(deserialized.content_digest.value, result.envelope.content_digest.value);
        assert_eq!(deserialized.overlay.candidates.len(), 2);
    }

    #[test]
    fn camel_case_credential_preserved_in_canonical_payload() {
        let overlay = multi_candidate_overlay();
        let candidates_value = serde_json::to_value(&overlay.candidates).unwrap();
        let canonical_json = serde_json::to_string(&candidates_value).unwrap();
        assert!(
            canonical_json.contains("secretRef"),
            "credential secretRef must serialize as camelCase, got: {canonical_json}"
        );
        assert!(
            !canonical_json.contains("secret_ref"),
            "credential must not serialize as snake_case secret_ref"
        );
    }

    #[test]
    fn no_credential_value_in_envelope() {
        let overlay = multi_candidate_overlay();
        let result = build_overlay_envelope(&overlay, "gw", "ns", "uid-1", 1, "2026-07-29T00:00:00Z").unwrap();
        let json = serde_json::to_string(&result.envelope).unwrap();
        let json_lower = json.to_lowercase();
        assert!(!json_lower.contains("bearer "));
        assert!(!json_lower.contains("token_value"));
        assert!(!json_lower.contains("private_key"));
        assert!(!json_lower.contains("-----begin"));
    }

    #[test]
    fn hex_encode_produces_lowercase() {
        let bytes = [0xAB, 0xCD, 0x01, 0x23];
        assert_eq!(hex_encode(&bytes), "abcd0123");
    }

    #[test]
    fn hex_encode_sha256_length() {
        let digest: [u8; 32] = Sha256::digest(b"test").into();
        let hex = hex_encode(&digest);
        assert_eq!(hex.len(), 64);
    }

    // -----------------------------------------------------------------------
    // Dual-key ConfigMap
    // -----------------------------------------------------------------------

    #[test]
    fn dual_key_configmap_has_both_keys() {
        let overlay = multi_candidate_overlay();
        let result = build_overlay_envelope(&overlay, "gw", "ns", "uid-1", 1, "2026-07-29T00:00:00Z").unwrap();
        let cm = crate::resources::routing_overlay::build_overlay_configmap(
            &overlay,
            Some(&result.envelope),
            "glb-demo",
            "gw",
            "ns",
        )
        .unwrap();

        let data = cm.data.as_ref().unwrap();
        assert!(data.contains_key("routing-config.json"), "legacy key must be present");
        assert!(data.contains_key(ENVELOPE_KEY), "envelope key must be present");
    }

    #[test]
    fn configmap_annotations_match_envelope() {
        let overlay = multi_candidate_overlay();
        let result = build_overlay_envelope(&overlay, "gw", "ns", "uid-1", 1, "2026-07-29T00:00:00Z").unwrap();
        let cm = crate::resources::routing_overlay::build_overlay_configmap(
            &overlay,
            Some(&result.envelope),
            "glb-demo",
            "gw",
            "ns",
        )
        .unwrap();

        let annotations = cm.metadata.annotations.as_ref().unwrap();
        assert_eq!(annotations.get(ANNOTATION_SCHEMA_VERSION).unwrap(), "1.0.0");
        assert_eq!(annotations.get(ANNOTATION_REVISION).unwrap(), &result.revision_hex,);
        assert_eq!(
            annotations.get(ANNOTATION_CONTENT_DIGEST).unwrap(),
            &result.revision_hex,
        );
    }

    #[test]
    fn legacy_only_configmap_has_no_envelope_key() {
        let overlay = minimal_overlay();
        let cm = crate::resources::routing_overlay::build_overlay_configmap(&overlay, None, "net", "gw", "ns").unwrap();
        let data = cm.data.as_ref().unwrap();
        assert!(data.contains_key("routing-config.json"));
        assert!(!data.contains_key(ENVELOPE_KEY));
        assert!(cm.metadata.annotations.is_none());
    }

    // -----------------------------------------------------------------------
    // Fixture validation (read-only — never writes to the source tree)
    // -----------------------------------------------------------------------

    #[test]
    fn fixture_directory_and_manifest_exist() {
        let fixture_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("tests/fixtures/overlay-contract/v1");
        assert!(fixture_dir.exists(), "fixture directory must exist");
        assert!(fixture_dir.join("manifest.json").exists(), "manifest.json must exist");
    }

    #[test]
    fn fixture_valid_minimal_matches_computed_digest() {
        let fixture_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("tests/fixtures/overlay-contract/v1");
        let content = std::fs::read_to_string(fixture_dir.join("valid-minimal.json")).unwrap();
        let envelope: OverlayEnvelope = serde_json::from_str(&content).unwrap();
        let recomputed = compute_semantic_digest(&envelope.overlay).unwrap();
        assert_eq!(
            recomputed, envelope.revision.value,
            "valid-minimal revision must match recomputed digest"
        );
        assert_eq!(
            envelope.revision.value, envelope.content_digest.value,
            "v1 invariant: revision == content_digest"
        );
    }

    #[test]
    fn fixture_valid_multi_candidate_matches_computed_digest() {
        let fixture_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("tests/fixtures/overlay-contract/v1");
        let content = std::fs::read_to_string(fixture_dir.join("valid-multi-candidate.json")).unwrap();
        let envelope: OverlayEnvelope = serde_json::from_str(&content).unwrap();
        let recomputed = compute_semantic_digest(&envelope.overlay).unwrap();
        assert_eq!(
            recomputed, envelope.revision.value,
            "valid-multi-candidate revision must match recomputed digest"
        );
    }

    #[test]
    fn fixture_no_secret_values() {
        let fixture_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("tests/fixtures/overlay-contract/v1");
        for entry in std::fs::read_dir(&fixture_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let content = std::fs::read_to_string(&path).unwrap().to_lowercase();
            let name = path.file_name().unwrap().to_str().unwrap();
            assert!(
                !content.contains("bearer "),
                "fixture {name} contains secret-like value"
            );
            assert!(
                !content.contains("token_value"),
                "fixture {name} contains secret-like value"
            );
            assert!(
                !content.contains("private_key"),
                "fixture {name} contains secret-like value"
            );
            assert!(
                !content.contains("-----begin"),
                "fixture {name} contains secret-like value"
            );
        }
    }

    #[test]
    fn fixture_manifest_digests_are_correct() {
        let fixture_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("tests/fixtures/overlay-contract/v1");

        let manifest_path = fixture_dir.join("manifest.json");
        assert!(manifest_path.exists(), "fixture manifest.json must exist");

        let manifest: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();

        let fixtures = manifest["fixtures"].as_object().unwrap();
        for (name, spec) in fixtures {
            let path = fixture_dir.join(name);
            assert!(path.exists(), "fixture {name} must exist");

            if let Some(expected_rev) = spec.get("revision").and_then(|v| v.as_str()) {
                let content = std::fs::read_to_string(&path).unwrap();
                let envelope: OverlayEnvelope = serde_json::from_str(&content).unwrap();
                let recomputed = compute_semantic_digest(&envelope.overlay).unwrap();
                assert_eq!(
                    recomputed, expected_rev,
                    "fixture {name}: recomputed digest must match manifest"
                );
            }
        }
    }
}
