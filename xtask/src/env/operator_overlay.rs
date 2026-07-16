//! Operator overlay wire-format parsing and routing config generation.
//!
//! This module is deliberately **self-contained**. It does not import from
//! the `operator` crate — the `grid-config.json` JSON format is the contract
//! boundary between a routing overlay producer and the xtask test harness.
//!
//! # Wire format
//!
//! A routing overlay producer serialises a `RoutingOverlay` as JSON into the
//! `grid-config.json` key of a Kubernetes `ConfigMap`. This module deserialises
//! that JSON, validates it, and converts it into the Praxis `grid_route`
//! candidates YAML block.
//!
//! # Cluster-naming convention
//!
//! The `candidate.cluster` field is the overlay upstream cluster reference.
//! The xtask `load_balancer` clusters are named `gateway-{site}`. The
//! [`candidates_yaml`] function uses `gateway-{site}` as the cluster reference
//! so that the `grid_route` and `load_balancer` sections stay consistent within
//! the generated Praxis config.
//!
//! This is a local validation bridge for xtask integration testing, not a
//! redefinition of the production cluster identity contract.
//!
//! # Limitations
//!
//! - Only the `"inference_model"` candidate kind is accepted.
//! - All overlay candidate sites must have a corresponding provider entry in the environment config so that the
//!   `load_balancer` section has a matching cluster.

use serde::Deserialize;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// Routing overlay deserialized from a `grid-config.json` file.
///
/// This struct mirrors the operator `RoutingOverlay` type but is defined here
/// independently to avoid a path dependency on the `operator` crate.  JSON
/// field names must match exactly.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct RoutingOverlay {
    /// Name of the `GridNetwork` this overlay belongs to.
    ///
    /// Preserved for wire-format completeness.  The xtask does not use this
    /// field directly; `local_site` identifies the gateway.
    pub(crate) network: String,

    /// Local site identifier for this gateway.
    ///
    /// Praxis uses this value to score candidates on the same site higher than
    /// remote candidates. Supplied per gateway by the overlay producer.
    pub(crate) local_site: String,

    /// Routing candidates, sorted by `(site, name, cluster)`.
    pub(crate) candidates: Vec<RoutingCandidate>,
}

/// A reference to a Kubernetes Secret holding a credential value.
///
/// Mirrors `operator::resources::routing_overlay::ProjectedCredentialRef`.
/// Contains only the Secret locating information — never the token value.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProjectedCredentialRef {
    /// Kubernetes Secret name.
    pub(crate) name: String,
    /// Kubernetes namespace containing the Secret.
    pub(crate) namespace: String,
    /// Key within `Secret.data` that holds the credential bytes.
    pub(crate) key: String,
}

/// A credential reference projected by the operator alongside a routing candidate.
///
/// Mirrors `operator::resources::routing_overlay::ProjectedCredential`.
/// The xtask resolves the actual token from the referenced Secret via
/// [`super::operator::resolve_api_credential`].
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProjectedCredential {
    /// Authentication strategy (e.g. `"bearer_token"`).
    pub(crate) strategy: String,
    /// Reference to the Kubernetes Secret holding the credential.
    pub(crate) secret_ref: ProjectedCredentialRef,
}

/// A single routing candidate from the routing overlay.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct RoutingCandidate {
    /// Candidate kind.  Must be `"inference_model"` for this xtask module.
    pub(crate) kind: String,

    /// Model name as declared in the `InferenceProvider` spec.
    pub(crate) name: String,

    /// Site name where this model is hosted.
    pub(crate) site: String,

    /// Upstream cluster identifier supplied by the overlay.
    ///
    /// Validated to be non-blank by [`validate_overlay`].  Not used directly
    /// in YAML generation — the xtask maps candidates to `gateway-{site}` to
    /// match the generated `load_balancer` cluster names.
    pub(crate) cluster: String,

    /// Whether this candidate's metrics data is considered fresh.
    pub(crate) fresh: bool,

    /// Credential reference projected by the operator, if any.
    ///
    /// Present when the provider's `spec.auth` declares a `bearer_token` strategy.
    /// Absent (`None`) for no-auth, manual, or unsupported-strategy providers.
    /// The xtask uses this to resolve the token from the referenced Secret.
    #[serde(default)]
    pub(crate) credential: Option<ProjectedCredential>,
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors from overlay JSON parsing and validation.
#[derive(Debug, Error)]
pub(crate) enum OverlayError {
    /// Input string is empty or contains only whitespace.
    #[error("overlay input is empty")]
    EmptyInput,

    /// JSON deserialization failed.
    #[error("overlay JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    /// The `local_site` field is blank.
    #[error("overlay local_site is blank")]
    BlankLocalSite,

    /// A required candidate field is blank.
    #[error("candidate has blank {0} field")]
    BlankField(&'static str),

    /// The candidate kind is not `"inference_model"`.
    #[error("unknown candidate kind {0:?} — only \"inference_model\" is supported")]
    UnknownKind(String),
}

// ---------------------------------------------------------------------------
// Parse
// ---------------------------------------------------------------------------

/// Parse the JSON content of a routing overlay `grid-config.json`.
///
/// Validates the overlay before returning it.
///
/// # Errors
///
/// Returns [`OverlayError`] if the input is empty, malformed JSON, or fails
/// semantic validation.
pub(crate) fn parse_grid_config_json(input: &str) -> Result<RoutingOverlay, OverlayError> {
    if input.trim().is_empty() {
        return Err(OverlayError::EmptyInput);
    }
    let overlay: RoutingOverlay = serde_json::from_str(input)?;
    validate_overlay(&overlay)?;
    Ok(overlay)
}

/// Validate a parsed overlay for semantic correctness.
///
/// Checks that `network` and `local_site` are non-blank, all required
/// candidate fields are non-blank, and all candidates use the
/// `"inference_model"` kind.
fn validate_overlay(overlay: &RoutingOverlay) -> Result<(), OverlayError> {
    if overlay.network.trim().is_empty() {
        return Err(OverlayError::BlankField("network"));
    }
    if overlay.local_site.trim().is_empty() {
        return Err(OverlayError::BlankLocalSite);
    }
    for c in &overlay.candidates {
        if c.kind.trim().is_empty() {
            return Err(OverlayError::BlankField("kind"));
        }
        if c.name.trim().is_empty() {
            return Err(OverlayError::BlankField("name"));
        }
        if c.site.trim().is_empty() {
            return Err(OverlayError::BlankField("site"));
        }
        if c.cluster.trim().is_empty() {
            return Err(OverlayError::BlankField("cluster"));
        }
        if c.kind != "inference_model" {
            return Err(OverlayError::UnknownKind(c.kind.clone()));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// YAML generation
// ---------------------------------------------------------------------------

/// Generate the `grid_route` candidates YAML block from a parsed overlay.
///
/// Each candidate produces five YAML lines indented to match the surrounding
/// Praxis config structure.  The cluster reference is `gateway-{site}` rather
/// than `candidate.cluster` — see module-level docs for the naming convention.
///
/// Returns an empty string when the overlay has no candidates.
pub(crate) fn candidates_yaml(overlay: &RoutingOverlay) -> String {
    candidates_yaml_impl(overlay, false)
}

/// Generate the `candidates:` YAML block for a `grid_route` filter config,
/// **including credential secretRef fields** when candidates carry them.
///
/// This is the native-injection-mode variant: the credential reference is
/// emitted so that a downstream `grid_credential_inject` filter can read it
/// from `grid.route.credential.*` filter metadata and inject the bearer token.
///
/// Token values are **never** emitted — only the secretRef `name`,
/// `namespace`, and `key` fields that locate the Kubernetes Secret.
///
/// # Intended config shape
///
/// When `grid_credential_inject` is used, the consumer Praxis config should
/// look like:
///
/// ```yaml
/// - filter: grid_route
///   local_site: site-east
///   candidates:
///     - kind: inference_model
///       name: gpt-4
///       site: site-west
///       cluster: gateway-site-west
///       fresh: true
///       credential:
///         strategy: bearer_token
///         secretRef:
///           name: my-api-token
///           namespace: grid-system
///           key: token
///
/// - filter: grid_credential_inject
///   credentials:
///     - name: my-api-token         # matches credential.secretRef.name
///       namespace: grid-system      # matches credential.secretRef.namespace
///       key: token                  # matches credential.secretRef.key
///       strategy: bearer_token
///       value: "sk-abc123"          # token resolved by xtask from K8s Secret
/// ```
///
/// The `grid_credential_inject` section is generated separately by the xtask
/// credential bridge; it is never part of the overlay JSON.
pub(crate) fn candidates_yaml_with_credentials(overlay: &RoutingOverlay) -> String {
    candidates_yaml_impl(overlay, true)
}

/// Shared implementation for [`candidates_yaml`] and [`candidates_yaml_with_credentials`].
fn candidates_yaml_impl(overlay: &RoutingOverlay, emit_credential: bool) -> String {
    overlay
        .candidates
        .iter()
        .map(|c| {
            let fresh_str = if c.fresh { "true" } else { "false" };
            let mut lines = vec![
                format!("         - kind: {}", c.kind),
                format!("           name: {}", c.name),
                format!("           site: {}", c.site),
                // Use gateway-{site} to match the xtask load_balancer naming convention.
                // The operator's candidate.cluster is the production reference.
                format!("           cluster: gateway-{}", c.site),
                format!("           fresh: {fresh_str}"),
            ];
            if let Some(cred) = c.credential.as_ref().filter(|_| emit_credential) {
                lines.push("           credential:".to_owned());
                lines.push(format!("             strategy: {}", cred.strategy));
                lines.push("             secretRef:".to_owned());
                lines.push(format!("               name: {}", cred.secret_ref.name));
                lines.push(format!("               namespace: {}", cred.secret_ref.namespace));
                lines.push(format!("               key: {}", cred.secret_ref.key));
            }
            lines.join("\n")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Parsing
    // -----------------------------------------------------------------------

    #[test]
    fn empty_input_returns_error() {
        assert!(parse_grid_config_json("").is_err(), "empty input must return an error");
        assert!(
            parse_grid_config_json("   ").is_err(),
            "whitespace-only input must return an error"
        );
    }

    #[test]
    fn invalid_json_returns_error() {
        assert!(
            parse_grid_config_json("{not valid json").is_err(),
            "invalid JSON must return an error"
        );
    }

    #[test]
    fn realistic_overlay_parses_correctly() {
        let json = r#"{
            "network": "test-network",
            "local_site": "consumer-site",
            "candidates": [
                {
                    "kind": "inference_model",
                    "name": "model-a",
                    "site": "site-a",
                    "cluster": "prov-a",
                    "fresh": true
                },
                {
                    "kind": "inference_model",
                    "name": "model-b",
                    "site": "site-b",
                    "cluster": "prov-b",
                    "fresh": true
                }
            ]
        }"#;
        let overlay = parse_grid_config_json(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.network, "test-network");
        assert_eq!(overlay.local_site, "consumer-site");
        assert_eq!(overlay.candidates.len(), 2);
    }

    #[test]
    fn blank_network_returns_error() {
        let json = r#"{"network":"  ","local_site":"consumer-site","candidates":[]}"#;
        assert!(
            parse_grid_config_json(json).is_err(),
            "blank network must return an error"
        );
    }

    #[test]
    fn blank_local_site_returns_error() {
        let json = r#"{"network":"n","local_site":"  ","candidates":[]}"#;
        assert!(
            parse_grid_config_json(json).is_err(),
            "blank local_site must return an error"
        );
    }

    #[test]
    fn blank_candidate_name_returns_error() {
        let json = r#"{
            "network": "net",
            "local_site": "consumer-site",
            "candidates": [{"kind":"inference_model","name":"","site":"s","cluster":"c","fresh":true}]
        }"#;
        assert!(
            parse_grid_config_json(json).is_err(),
            "blank candidate name must return an error"
        );
    }

    #[test]
    fn blank_candidate_site_returns_error() {
        let json = r#"{
            "network": "net",
            "local_site": "consumer-site",
            "candidates": [{"kind":"inference_model","name":"model","site":"","cluster":"c","fresh":true}]
        }"#;
        assert!(
            parse_grid_config_json(json).is_err(),
            "blank candidate site must return an error"
        );
    }

    #[test]
    fn blank_candidate_cluster_returns_error() {
        let json = r#"{
            "network": "net",
            "local_site": "consumer-site",
            "candidates": [{"kind":"inference_model","name":"model","site":"s","cluster":"","fresh":true}]
        }"#;
        assert!(
            parse_grid_config_json(json).is_err(),
            "blank candidate cluster must return an error"
        );
    }

    #[test]
    fn unknown_candidate_kind_returns_error() {
        let json = r#"{
            "network": "net",
            "local_site": "consumer-site",
            "candidates": [{"kind":"mcp_tool","name":"tool","site":"s","cluster":"c","fresh":true}]
        }"#;
        assert!(
            parse_grid_config_json(json).is_err(),
            "unknown candidate kind must return an error"
        );
    }

    // -----------------------------------------------------------------------
    // YAML generation
    // -----------------------------------------------------------------------

    #[test]
    fn empty_candidates_produces_empty_yaml() {
        let json = r#"{"network":"net","local_site":"consumer-site","candidates":[]}"#;
        let overlay = parse_grid_config_json(json).unwrap_or_else(|_| std::process::abort());
        assert!(
            candidates_yaml(&overlay).is_empty(),
            "no candidates must produce empty YAML"
        );
    }

    #[test]
    fn single_candidate_produces_correct_yaml() {
        let json = r#"{
            "network": "net",
            "local_site": "consumer-site",
            "candidates": [{
                "kind": "inference_model",
                "name": "model-a",
                "site": "site-a",
                "cluster": "prov-a",
                "fresh": true
            }]
        }"#;
        let overlay = parse_grid_config_json(json).unwrap_or_else(|_| std::process::abort());
        let yaml = candidates_yaml(&overlay);
        assert!(yaml.contains("kind: inference_model"), "must include kind");
        assert!(yaml.contains("name: model-a"), "must include model name");
        assert!(yaml.contains("site: site-a"), "must include site");
        assert!(
            yaml.contains("cluster: gateway-site-a"),
            "must use gateway-{{site}} convention"
        );
        assert!(yaml.contains("fresh: true"), "must include fresh");
    }

    #[test]
    fn stale_candidate_preserves_fresh_false() {
        let json = r#"{
            "network": "net",
            "local_site": "consumer-site",
            "candidates": [{
                "kind": "inference_model",
                "name": "model",
                "site": "site-a",
                "cluster": "prov",
                "fresh": false
            }]
        }"#;
        let overlay = parse_grid_config_json(json).unwrap_or_else(|_| std::process::abort());
        let yaml = candidates_yaml(&overlay);
        assert!(yaml.contains("fresh: false"), "fresh=false must be preserved");
    }

    #[test]
    fn multiple_candidates_all_appear() {
        let json = r#"{
            "network": "net",
            "local_site": "consumer-site",
            "candidates": [
                {"kind":"inference_model","name":"model-a","site":"site-a","cluster":"prov-a","fresh":true},
                {"kind":"inference_model","name":"model-b","site":"site-b","cluster":"prov-b","fresh":true}
            ]
        }"#;
        let overlay = parse_grid_config_json(json).unwrap_or_else(|_| std::process::abort());
        let yaml = candidates_yaml(&overlay);
        assert!(yaml.contains("model-a"), "first candidate must appear");
        assert!(yaml.contains("model-b"), "second candidate must appear");
        assert!(yaml.contains("gateway-site-a"), "first site cluster must appear");
        assert!(yaml.contains("gateway-site-b"), "second site cluster must appear");
    }

    #[test]
    fn overlay_cluster_field_maps_to_gateway_site_convention() {
        // Proves the xtask naming convention: overlay cluster name is NOT used
        // verbatim.  The xtask bridge maps candidates to gateway-{site} so that
        // grid_route and load_balancer cluster references stay consistent.
        let json = r#"{
            "network": "net",
            "local_site": "consumer-site",
            "candidates": [{
                "kind": "inference_model",
                "name": "model",
                "site": "site-a",
                "cluster": "operator-assigned-name",
                "fresh": true
            }]
        }"#;
        let overlay = parse_grid_config_json(json).unwrap_or_else(|_| std::process::abort());
        let yaml = candidates_yaml(&overlay);
        assert!(
            !yaml.contains("operator-assigned-name"),
            "overlay cluster name must not appear verbatim in YAML"
        );
        assert!(
            yaml.contains("gateway-site-a"),
            "gateway-{{site}} must appear as the cluster reference"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "multi-line JSON fixture with distinct security invariant per assert"
    )]
    fn credential_bearing_candidate_parses_and_yaml_omits_credential() {
        // Regression guard: candidates_yaml() must strip credential data so that
        // grid_route filter YAML never contains secret references or token values.
        // The grid_route filter rejects unknown fields via deny_unknown_fields;
        // writing credential: into YAML would cause parse failure in old images.
        let json = r#"{
            "network": "net",
            "local_site": "consumer-site",
            "candidates": [{
                "kind": "inference_model",
                "name": "api-model",
                "site": "site-b",
                "cluster": "prov-b",
                "fresh": true,
                "credential": {
                    "strategy": "bearer_token",
                    "secretRef": {
                        "name": "api-token-secret",
                        "namespace": "grid-system",
                        "key": "token"
                    }
                }
            }]
        }"#;
        let overlay = parse_grid_config_json(json).unwrap_or_else(|_| std::process::abort());

        // Overlay must parse credential reference correctly.
        let cred = overlay
            .candidates
            .first()
            .and_then(|c| c.credential.as_ref())
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(cred.strategy, "bearer_token", "strategy must parse from JSON");
        assert_eq!(cred.secret_ref.name, "api-token-secret", "secretRef.name must parse");

        // YAML output must not contain credential data.
        let yaml = candidates_yaml(&overlay);
        assert!(yaml.contains("api-model"), "candidate name must appear");
        assert!(yaml.contains("site-b"), "candidate site must appear");
        assert!(
            !yaml.contains("credential"),
            "credential field must not appear in grid_route YAML"
        );
        assert!(
            !yaml.contains("bearer_token"),
            "credential strategy must not appear in grid_route YAML"
        );
        assert!(
            !yaml.contains("secretRef"),
            "secretRef must not appear in grid_route YAML"
        );
        assert!(
            !yaml.contains("api-token-secret"),
            "secret name must not appear in grid_route YAML"
        );
    }

    // -----------------------------------------------------------------------
    // Native injection mode: candidates_yaml_with_credentials
    // -----------------------------------------------------------------------

    #[test]
    fn candidate_without_credential_unchanged_in_native_mode() {
        let json = r#"{
            "network": "net",
            "local_site": "consumer-site",
            "candidates": [{"kind":"inference_model","name":"m","site":"s","cluster":"c","fresh":true}]
        }"#;
        let overlay = parse_grid_config_json(json).unwrap_or_else(|_| std::process::abort());
        let yaml = candidates_yaml_with_credentials(&overlay);
        assert!(yaml.contains("kind: inference_model"), "kind must appear");
        assert!(yaml.contains("name: m"), "name must appear");
        assert!(
            !yaml.contains("credential"),
            "no credential block for no-auth candidate"
        );
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "multi-line JSON fixture with per-field assertions")]
    fn credential_bearing_candidate_emits_secret_ref_in_native_mode() {
        let json = r#"{
            "network": "net",
            "local_site": "consumer-site",
            "candidates": [{
                "kind": "inference_model",
                "name": "api-model",
                "site": "site-b",
                "cluster": "prov-b",
                "fresh": true,
                "credential": {
                    "strategy": "bearer_token",
                    "secretRef": {
                        "name": "api-token-secret",
                        "namespace": "grid-system",
                        "key": "token"
                    }
                }
            }]
        }"#;
        let overlay = parse_grid_config_json(json).unwrap_or_else(|_| std::process::abort());
        let yaml = candidates_yaml_with_credentials(&overlay);
        assert!(yaml.contains("api-model"), "candidate name must appear");
        assert!(
            yaml.contains("credential:"),
            "credential block must appear in native mode"
        );
        assert!(yaml.contains("strategy: bearer_token"), "strategy must appear");
        assert!(yaml.contains("secretRef:"), "secretRef must appear");
        assert!(yaml.contains("name: api-token-secret"), "secretRef.name must appear");
        assert!(
            yaml.contains("namespace: grid-system"),
            "secretRef.namespace must appear"
        );
        assert!(yaml.contains("key: token"), "secretRef.key must appear");
        // Token is never emitted — only the reference that locates the Secret.
        assert!(!yaml.contains("sk-"), "token value must never appear in YAML");
        assert!(!yaml.contains("Bearer"), "token prefix must never appear in YAML");
    }

    #[test]
    fn credential_stripping_mode_still_strips_credential() {
        let json = r#"{
            "network": "net",
            "local_site": "consumer-site",
            "candidates": [{
                "kind": "inference_model",
                "name": "api-model",
                "site": "site-b",
                "cluster": "prov-b",
                "fresh": true,
                "credential": {
                    "strategy": "bearer_token",
                    "secretRef": {"name": "s", "namespace": "ns", "key": "k"}
                }
            }]
        }"#;
        let overlay = parse_grid_config_json(json).unwrap_or_else(|_| std::process::abort());
        let stripped_yaml = candidates_yaml(&overlay);
        assert!(
            !stripped_yaml.contains("credential"),
            "credential-stripping mode must still strip credential"
        );
        assert!(
            !stripped_yaml.contains("secretRef"),
            "credential-stripping mode must still strip secretRef"
        );
        let native_yaml = candidates_yaml_with_credentials(&overlay);
        assert!(
            native_yaml.contains("credential"),
            "native mode must include credential"
        );
    }
}
