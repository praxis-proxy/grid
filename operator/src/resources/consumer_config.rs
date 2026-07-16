//! Operator-owned consumer Praxis config renderer.
//!
//! Generates the `praxis.yaml` content for a consumer gateway `ConfigMap` from
//! a [`RoutingOverlay`].  The generated config includes:
//!
//! - `json_body_field` filter (model field → `X-Model` header)
//! - `grid_route` filter with candidates from the overlay
//! - `grid_credential_inject` filter (only when credential-bearing candidates exist)
//! - `load_balancer` filter with one cluster entry per unique candidate cluster
//!
//! # Security invariants
//!
//! - Token values are **never** emitted.  Credential entries use `file:` sources under
//!   `ConsumerConfig::credential_mount_base`.
//! - The `credential.secretRef` locating information (name, namespace, key) is included in the `grid_route` candidate
//!   block and in the `grid_credential_inject` entry.  This is reference data, not credential bytes.
//!
//! [`RoutingOverlay`]: crate::resources::routing_overlay::RoutingOverlay
//! [`ConsumerConfig`]: crate::crd::grid_network::ConsumerConfig

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::core::v1::ConfigMap;

use crate::resources::routing_overlay::{RoutingCandidate, RoutingOverlay};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors from consumer Praxis config generation.
#[derive(Debug, thiserror::Error)]
pub enum ConsumerConfigError {
    /// The overlay's `local_site` field is blank.
    #[error("overlay local_site must not be blank")]
    BlankLocalSite,

    /// The `credential_mount_base` path is blank.
    #[error("credential_mount_base must not be blank")]
    BlankMountBase,

    /// A candidate has a blank cluster name.
    #[error("candidate {kind:?}/{name:?} has a blank cluster")]
    BlankCluster {
        /// Candidate kind (e.g. `"inference_model"`).
        kind: String,
        /// Candidate name.
        name: String,
    },

    /// JSON serialization failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Generate the YAML content of a consumer Praxis `ConfigMap`.
///
/// The rendered config is compatible with the Praxis `grid_route` and
/// `grid_credential_inject` filters.  It never contains credential token bytes.
///
/// # Parameters
///
/// - `overlay` — the routing overlay produced by the Grid operator for this gateway.
/// - `credential_mount_base` — base directory where credential Secrets are mounted inside the consumer pod (e.g.
///   `/run/secrets/grid-credentials`).
///
/// # Errors
///
/// Returns [`ConsumerConfigError`] when:
/// - `overlay.local_site` is blank.
/// - `credential_mount_base` is blank.
/// - Any candidate has a blank cluster name.
#[expect(
    clippy::too_many_lines,
    reason = "sequential validation + three rendering passes; splitting would obscure the overall config shape"
)]
pub(crate) fn generate_consumer_praxis_config(
    overlay: &RoutingOverlay,
    credential_mount_base: &str,
) -> Result<String, ConsumerConfigError> {
    if overlay.local_site.trim().is_empty() {
        return Err(ConsumerConfigError::BlankLocalSite);
    }
    if credential_mount_base.trim().is_empty() {
        return Err(ConsumerConfigError::BlankMountBase);
    }
    for c in &overlay.candidates {
        if c.cluster.trim().is_empty() {
            return Err(ConsumerConfigError::BlankCluster {
                kind: c.kind.clone(),
                name: c.name.clone(),
            });
        }
    }

    let candidates_yaml = render_candidates(&overlay.candidates);
    let local_site = yaml_scalar(&overlay.local_site)?;

    let credential_inject_section = render_credential_inject(&overlay.candidates, credential_mount_base);
    let load_balancer_section = render_load_balancer(&overlay.candidates);

    let mut config = format!(
        "filter_chains:\n\
         \x20 - name: consumer-chain\n\
         \x20   filters:\n\
         \x20     - filter: json_body_field\n\
         \x20       field: model\n\
         \x20       header: X-Model\n\
         \x20     - filter: grid_route\n\
         \x20       local_site: {local_site}\n\
         \x20       model_header: \"X-Model\"\n\
         \x20       candidates:\n\
         {candidates_yaml}"
    );

    if let Some(inject) = credential_inject_section {
        config.push_str(&inject);
    }

    config.push_str(&load_balancer_section);

    Ok(config)
}

/// Build the Kubernetes `ConfigMap` for the generated consumer Praxis config.
///
/// The `ConfigMap` contains a single `praxis.yaml` key with the rendered YAML.
/// Labels are consistent with routing overlay `ConfigMap`s.
pub(crate) fn build_consumer_config_map(
    config_yaml: &str,
    config_map_name: &str,
    namespace: &str,
    network_name: &str,
    gateway_name: &str,
) -> ConfigMap {
    let mut data = BTreeMap::new();
    data.insert("praxis.yaml".to_owned(), config_yaml.to_owned());

    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/managed-by".to_owned(), "grid-operator".to_owned());
    labels.insert("grid.praxis-proxy.io/gateway".to_owned(), gateway_name.to_owned());
    labels.insert("grid.praxis-proxy.io/network".to_owned(), network_name.to_owned());

    ConfigMap {
        metadata: kube::api::ObjectMeta {
            labels: Some(labels),
            name: Some(config_map_name.to_owned()),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Rendering helpers
// ---------------------------------------------------------------------------

/// Render `grid_route` candidates YAML block.
///
/// Each candidate is indented and includes `credential.secretRef` when present.
/// Token values are never included.
fn render_candidates(candidates: &[RoutingCandidate]) -> String {
    candidates.iter().map(render_candidate).collect::<Vec<_>>().join("\n")
}

/// Render one `grid_route` candidate.
fn render_candidate(c: &RoutingCandidate) -> String {
    let mut lines = vec![
        format!(
            "         - kind: {}",
            yaml_scalar(&c.kind).unwrap_or_else(|_| "\"\"".to_owned())
        ),
        format!(
            "           name: {}",
            yaml_scalar(&c.name).unwrap_or_else(|_| "\"\"".to_owned())
        ),
        format!(
            "           site: {}",
            yaml_scalar(&c.site).unwrap_or_else(|_| "\"\"".to_owned())
        ),
        format!(
            "           cluster: {}",
            yaml_scalar(&c.cluster).unwrap_or_else(|_| "\"\"".to_owned())
        ),
        format!("           fresh: {}", c.fresh),
    ];
    if let Some(cred) = &c.credential {
        lines.extend(render_credential_reference(cred));
    }
    lines.join("\n")
}

/// Render the `credential.secretRef` block for one candidate.
fn render_credential_reference(cred: &crate::resources::routing_overlay::ProjectedCredential) -> Vec<String> {
    vec![
        "           credential:".to_owned(),
        format!(
            "             strategy: {}",
            yaml_scalar(&cred.strategy).unwrap_or_else(|_| "\"\"".to_owned())
        ),
        "             secretRef:".to_owned(),
        format!(
            "               name: {}",
            yaml_scalar(&cred.secret_ref.name).unwrap_or_else(|_| "\"\"".to_owned())
        ),
        format!(
            "               namespace: {}",
            yaml_scalar(&cred.secret_ref.namespace).unwrap_or_else(|_| "\"\"".to_owned())
        ),
        format!(
            "               key: {}",
            yaml_scalar(&cred.secret_ref.key).unwrap_or_else(|_| "\"\"".to_owned())
        ),
    ]
}

/// Render the `grid_credential_inject` filter section.
///
/// Returns `None` when no credential-bearing candidates exist.
/// Each unique `(strategy, name, namespace, key)` tuple produces one entry.
/// Entries use `file:` sources; no `value:` is ever emitted.
#[expect(
    clippy::too_many_lines,
    reason = "BTreeMap collection + format strings for each credential field"
)]
fn render_credential_inject(candidates: &[RoutingCandidate], credential_mount_base: &str) -> Option<String> {
    // Collect unique (strategy, name, namespace, key) → rendered entry.
    // BTreeMap provides deterministic sorted order by key.
    let mut entries: BTreeMap<(String, String, String, String), String> = BTreeMap::new();

    for c in candidates {
        let Some(cred) = &c.credential else {
            continue;
        };
        let map_key = (
            cred.strategy.clone(),
            cred.secret_ref.name.clone(),
            cred.secret_ref.namespace.clone(),
            cred.secret_ref.key.clone(),
        );
        if entries.contains_key(&map_key) {
            continue;
        }

        let file_path = credential_file_path(credential_mount_base, &cred.secret_ref.name, &cred.secret_ref.key);
        let entry = format!(
            "          - name: {}\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20  namespace: {}\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20  key: {}\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20  strategy: {}\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20  file: {}",
            yaml_scalar(&cred.secret_ref.name).unwrap_or_else(|_| "\"\"".to_owned()),
            yaml_scalar(&cred.secret_ref.namespace).unwrap_or_else(|_| "\"\"".to_owned()),
            yaml_scalar(&cred.secret_ref.key).unwrap_or_else(|_| "\"\"".to_owned()),
            yaml_scalar(&cred.strategy).unwrap_or_else(|_| "\"\"".to_owned()),
            yaml_scalar(&file_path).unwrap_or_else(|_| "\"\"".to_owned()),
        );
        entries.insert(map_key, entry);
    }

    if entries.is_empty() {
        return None;
    }

    Some(format!(
        "\n\
         \x20     - filter: grid_credential_inject\n\
         \x20       credentials:\n\
         {}",
        entries.into_values().collect::<Vec<_>>().join("\n")
    ))
}

/// Render the `load_balancer` filter section.
///
/// Produces one cluster entry per unique `candidate.cluster`.  Entries are
/// ordered deterministically.  Endpoint details (URLs, TLS) are not included
/// in the initial implementation — they are provided by the consumer deployment
/// tooling.
fn render_load_balancer(candidates: &[RoutingCandidate]) -> String {
    let clusters: BTreeSet<&str> = candidates.iter().map(|c| c.cluster.as_str()).collect();
    let cluster_lines: Vec<String> = clusters
        .into_iter()
        .map(|name| {
            let quoted = yaml_scalar(name).unwrap_or_else(|_| "\"\"".to_owned());
            format!("          - name: {quoted}")
        })
        .collect();

    format!(
        "\n\
         \x20     - filter: load_balancer\n\
         \x20       clusters:\n\
         {}",
        cluster_lines.join("\n")
    )
}

/// Compute the `file:` path for a credential entry.
///
/// Uses `{credential_mount_base}/{secret-name}/{secret-key}`.
/// The secret name is sanitized to be DNS-label-safe before use.
fn credential_file_path(mount_base: &str, secret_name: &str, secret_key: &str) -> String {
    let safe_name = dns_safe(secret_name);
    format!("{mount_base}/{safe_name}/{secret_key}")
}

/// Render a string as a YAML-safe scalar.
///
/// JSON string syntax is valid YAML, so `serde_json::to_string` gives us a
/// compact quoted scalar without adding a YAML dependency.
fn yaml_scalar(value: &str) -> Result<String, serde_json::Error> {
    serde_json::to_string(value)
}

/// Sanitize a string to be safe as a path component and DNS label.
///
/// Lowercases, replaces characters outside `[a-z0-9-]` with `-`, collapses
/// consecutive `-`, and trims leading/trailing `-`.  Truncates to 63 characters.
///
/// This ensures predictable, collision-resistant path components without
/// requiring a hash for most common Kubernetes Secret names, which are already
/// DNS-safe.
fn dns_safe(s: &str) -> String {
    let lowered = s.to_ascii_lowercase();
    let mut sanitized = String::with_capacity(lowered.len());
    let mut last_was_hyphen = false;
    for ch in lowered.chars() {
        if ch.is_ascii_alphanumeric() {
            sanitized.push(ch);
            last_was_hyphen = false;
        } else if !last_was_hyphen {
            sanitized.push('-');
            last_was_hyphen = true;
        }
    }
    let sanitized = sanitized.trim_matches('-');
    let truncated: String = sanitized.chars().take(63).collect();
    // After truncation, trim a trailing hyphen that may have been introduced.
    truncated.trim_end_matches('-').to_owned()
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
    clippy::too_many_arguments,
    clippy::string_slice,
    reason = "tests"
)]
mod tests {
    use super::*;
    use crate::resources::routing_overlay::{ProjectedCredential, ProjectedCredentialRef, RoutingCandidate};

    // -----------------------------------------------------------------------
    // Test utilities
    // -----------------------------------------------------------------------

    fn plain_candidate(kind: &str, name: &str, site: &str, cluster: &str, fresh: bool) -> RoutingCandidate {
        RoutingCandidate {
            kind: kind.to_owned(),
            name: name.to_owned(),
            site: site.to_owned(),
            cluster: cluster.to_owned(),
            fresh,
            credential: None,
        }
    }

    fn credential_candidate(
        kind: &str,
        name: &str,
        site: &str,
        cluster: &str,
        secret_name: &str,
        secret_ns: &str,
        secret_key: &str,
    ) -> RoutingCandidate {
        RoutingCandidate {
            kind: kind.to_owned(),
            name: name.to_owned(),
            site: site.to_owned(),
            cluster: cluster.to_owned(),
            fresh: true,
            credential: Some(ProjectedCredential {
                strategy: "bearer_token".to_owned(),
                secret_ref: ProjectedCredentialRef {
                    name: secret_name.to_owned(),
                    namespace: secret_ns.to_owned(),
                    key: secret_key.to_owned(),
                },
            }),
        }
    }

    fn simple_overlay(candidates: Vec<RoutingCandidate>) -> RoutingOverlay {
        RoutingOverlay {
            network: "test-net".to_owned(),
            local_site: "site-a".to_owned(),
            candidates,
        }
    }

    const MOUNT_BASE: &str = "/run/secrets/grid-credentials";
    const SENTINEL_TOKEN: &str = "sk-super-secret-bearer-token-do-not-emit";

    // -----------------------------------------------------------------------
    // Renderer: basic structure
    // -----------------------------------------------------------------------

    #[test]
    fn plain_candidates_produce_grid_route_and_load_balancer() {
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "model-a",
            "site-a",
            "gateway-site-a",
            true,
        )]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE).unwrap();
        assert!(yaml.contains("filter: grid_route"), "must include grid_route");
        assert!(yaml.contains("filter: load_balancer"), "must include load_balancer");
        assert!(yaml.contains("filter: json_body_field"), "must include json_body_field");
        assert!(
            yaml.contains("local_site: \"site-a\""),
            "must include YAML-quoted local_site"
        );
        assert!(yaml.contains("model-a"), "candidate name must appear");
        assert!(yaml.contains("gateway-site-a"), "cluster must appear in load_balancer");
    }

    #[test]
    fn no_credential_candidates_produces_no_credential_inject() {
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "model-x",
            "site-a",
            "cluster-a",
            true,
        )]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE).unwrap();
        assert!(
            !yaml.contains("grid_credential_inject"),
            "no credential candidates must produce no grid_credential_inject"
        );
    }

    #[test]
    fn credential_candidate_produces_credential_inject_with_file_source() {
        let overlay = simple_overlay(vec![credential_candidate(
            "inference_model",
            "model-z",
            "api-site",
            "api-cluster",
            "my-secret",
            "default",
            "token",
        )]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE).unwrap();
        assert!(
            yaml.contains("filter: grid_credential_inject"),
            "credential candidate must produce grid_credential_inject"
        );
        assert!(
            yaml.contains("file: \"/run/secrets/grid-credentials/my-secret/token\""),
            "must use file: source with correct path"
        );
        assert!(!yaml.contains("value:"), "must never emit value: in generated config");
    }

    #[test]
    fn multiple_candidates_sharing_same_secret_ref_produce_one_credential_entry() {
        let overlay = simple_overlay(vec![
            credential_candidate(
                "inference_model",
                "model-z1",
                "site-b",
                "cluster-b",
                "shared-creds",
                "ns",
                "token",
            ),
            credential_candidate(
                "inference_model",
                "model-z2",
                "site-b",
                "cluster-b",
                "shared-creds",
                "ns",
                "token",
            ),
        ]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE).unwrap();
        // Count occurrences of the file path — should be exactly 1.
        let count = yaml
            .matches("file: \"/run/secrets/grid-credentials/shared-creds/token\"")
            .count();
        assert_eq!(count, 1, "duplicate secretRef must produce only one credential entry");
    }

    #[test]
    fn multiple_different_credentials_produce_multiple_entries() {
        let overlay = simple_overlay(vec![
            credential_candidate(
                "inference_model",
                "model-a",
                "site-a",
                "cluster-a",
                "creds-a",
                "ns",
                "tok",
            ),
            credential_candidate(
                "inference_model",
                "model-b",
                "site-b",
                "cluster-b",
                "creds-b",
                "ns",
                "tok",
            ),
        ]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE).unwrap();
        assert!(yaml.contains("creds-a"), "first credential name must appear");
        assert!(yaml.contains("creds-b"), "second credential name must appear");
        let count = yaml.matches("file:").count();
        assert_eq!(count, 2, "two distinct credentials must produce two file: entries");
    }

    // -----------------------------------------------------------------------
    // Security invariants
    // -----------------------------------------------------------------------

    #[test]
    fn generated_yaml_does_not_contain_sentinel_token() {
        // The renderer must never emit token bytes even if passed indirectly.
        // This test proves the renderer has no path to emit the sentinel.
        let overlay = simple_overlay(vec![credential_candidate(
            "inference_model",
            "model-z",
            "api",
            "api-cluster",
            "my-creds",
            "default",
            "token",
        )]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE).unwrap();
        assert!(
            !yaml.contains(SENTINEL_TOKEN),
            "generated YAML must not contain token bytes"
        );
    }

    #[test]
    fn generated_yaml_does_not_contain_value_field() {
        let overlay = simple_overlay(vec![credential_candidate(
            "inference_model",
            "model-z",
            "api",
            "api-cluster",
            "creds",
            "ns",
            "key",
        )]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE).unwrap();
        // Ensure 'value:' does not appear — that would indicate static header injection.
        assert!(!yaml.contains("value:"), "must not emit value: in generated config");
    }

    #[test]
    fn generated_yaml_does_not_contain_static_header_injection_filters() {
        let overlay = simple_overlay(vec![credential_candidate(
            "inference_model",
            "model-z",
            "api",
            "cluster",
            "creds",
            "ns",
            "k",
        )]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE).unwrap();
        assert!(
            !yaml.contains("filter: headers"),
            "must not include static header filter"
        );
        assert!(!yaml.contains("request_set"), "must not include request_set");
    }

    #[test]
    fn generated_yaml_contains_secret_ref_locating_info() {
        let overlay = simple_overlay(vec![credential_candidate(
            "inference_model",
            "model-z",
            "api",
            "cluster",
            "my-api-creds",
            "grid-system",
            "token",
        )]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE).unwrap();
        assert!(yaml.contains("my-api-creds"), "secretRef.name must appear");
        assert!(yaml.contains("grid-system"), "secretRef.namespace must appear");
    }

    #[test]
    fn generated_yaml_quotes_dynamic_scalars() {
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "vendor/model:latest",
            "site:a",
            "cluster#a",
            true,
        )]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE).unwrap();
        assert!(
            yaml.contains("name: \"vendor/model:latest\""),
            "model/capability names with YAML-significant characters must be quoted"
        );
        assert!(
            yaml.contains("site: \"site:a\""),
            "site values with YAML-significant characters must be quoted"
        );
        assert!(
            yaml.contains("cluster: \"cluster#a\""),
            "cluster values with YAML-significant characters must be quoted"
        );
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn blank_local_site_returns_error() {
        let overlay = RoutingOverlay {
            network: "n".to_owned(),
            local_site: String::new(),
            candidates: vec![],
        };
        assert!(
            generate_consumer_praxis_config(&overlay, MOUNT_BASE).is_err(),
            "blank local_site must return error"
        );
    }

    #[test]
    fn blank_mount_base_returns_error() {
        let overlay = simple_overlay(vec![]);
        assert!(
            generate_consumer_praxis_config(&overlay, "").is_err(),
            "blank credential_mount_base must return error"
        );
    }

    #[test]
    fn blank_candidate_cluster_returns_error() {
        let overlay = simple_overlay(vec![plain_candidate("inference_model", "m", "s", "", true)]);
        assert!(
            generate_consumer_praxis_config(&overlay, MOUNT_BASE).is_err(),
            "blank candidate cluster must return error"
        );
    }

    // -----------------------------------------------------------------------
    // Determinism and ordering
    // -----------------------------------------------------------------------

    #[test]
    fn output_is_deterministic_for_same_input() {
        let overlay = simple_overlay(vec![
            credential_candidate("inference_model", "m1", "s1", "c1", "creds-b", "ns", "tok"),
            credential_candidate("inference_model", "m2", "s2", "c2", "creds-a", "ns", "tok"),
        ]);
        let yaml1 = generate_consumer_praxis_config(&overlay, MOUNT_BASE).unwrap();
        let yaml2 = generate_consumer_praxis_config(&overlay, MOUNT_BASE).unwrap();
        assert_eq!(yaml1, yaml2, "output must be deterministic");
    }

    #[test]
    fn credential_entries_ordered_deterministically() {
        let overlay = simple_overlay(vec![
            credential_candidate("inference_model", "m1", "s1", "c1", "zzz-creds", "ns", "tok"),
            credential_candidate("inference_model", "m2", "s2", "c2", "aaa-creds", "ns", "tok"),
        ]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE).unwrap();
        // Search within the credential_inject section only (after the section header).
        let inject_start = yaml
            .find("grid_credential_inject")
            .expect("grid_credential_inject section must be present");
        let inject_section = &yaml[inject_start..];
        let pos_aaa = inject_section.find("aaa-creds").unwrap();
        let pos_zzz = inject_section.find("zzz-creds").unwrap();
        assert!(
            pos_aaa < pos_zzz,
            "credential entries must be sorted deterministically (aaa before zzz in inject section)"
        );
    }

    // -----------------------------------------------------------------------
    // Default mount base in file paths
    // -----------------------------------------------------------------------

    #[test]
    fn default_mount_base_appears_in_file_path() {
        let overlay = simple_overlay(vec![credential_candidate(
            "inference_model",
            "model-z",
            "api",
            "api-cluster",
            "my-creds",
            "ns",
            "token",
        )]);
        let yaml = generate_consumer_praxis_config(&overlay, "/run/secrets/grid-credentials").unwrap();
        assert!(
            yaml.contains("file: \"/run/secrets/grid-credentials/my-creds/token\""),
            "default mount base must appear in file path"
        );
    }

    // -----------------------------------------------------------------------
    // dns_safe helper
    // -----------------------------------------------------------------------

    #[test]
    fn dns_safe_passes_through_already_safe_names() {
        assert_eq!(dns_safe("my-secret"), "my-secret");
        assert_eq!(dns_safe("api-creds-v2"), "api-creds-v2");
        assert_eq!(dns_safe("abc123"), "abc123");
    }

    #[test]
    fn dns_safe_lowercases_uppercase() {
        assert_eq!(dns_safe("MySecret"), "mysecret");
    }

    #[test]
    fn dns_safe_replaces_special_chars_with_hyphens() {
        assert_eq!(dns_safe("my.secret/name"), "my-secret-name");
    }

    #[test]
    fn dns_safe_collapses_consecutive_hyphens() {
        assert_eq!(dns_safe("my---secret"), "my-secret");
    }

    #[test]
    fn dns_safe_trims_leading_trailing_hyphens() {
        assert_eq!(dns_safe("---my-secret---"), "my-secret");
    }

    #[test]
    fn dns_safe_truncates_long_names() {
        let long = "a".repeat(100);
        assert!(dns_safe(&long).len() <= 63, "truncated name must be at most 63 chars");
    }

    // -----------------------------------------------------------------------
    // ConfigMap builder
    // -----------------------------------------------------------------------

    #[test]
    fn build_consumer_config_map_uses_praxis_yaml_key() {
        let cm = build_consumer_config_map("yaml-content", "my-cm", "ns", "net", "gw");
        let data = cm.data.unwrap();
        assert!(data.contains_key("praxis.yaml"), "ConfigMap must use praxis.yaml key");
        assert_eq!(data["praxis.yaml"], "yaml-content");
    }

    #[test]
    fn build_consumer_config_map_has_managed_by_label() {
        let cm = build_consumer_config_map("yaml", "cm-name", "ns", "net", "gw");
        let labels = cm.metadata.labels.unwrap();
        assert_eq!(
            labels.get("app.kubernetes.io/managed-by").map(String::as_str),
            Some("grid-operator"),
            "must have managed-by label"
        );
    }

    #[test]
    fn build_consumer_config_map_has_network_and_gateway_labels() {
        let cm = build_consumer_config_map("yaml", "cm-name", "ns", "my-network", "my-gateway");
        let labels = cm.metadata.labels.unwrap();
        assert_eq!(
            labels.get("grid.praxis-proxy.io/network").map(String::as_str),
            Some("my-network")
        );
        assert_eq!(
            labels.get("grid.praxis-proxy.io/gateway").map(String::as_str),
            Some("my-gateway")
        );
    }
}
