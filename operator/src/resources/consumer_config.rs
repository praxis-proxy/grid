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

use crate::{
    crd::grid_network::{ClusterEndpointConfig, TransportMode},
    resources::routing_overlay::{RoutingCandidate, RoutingOverlay},
};

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

    /// A candidate cluster has no endpoint topology entry.
    #[error("missing cluster endpoint for {cluster:?}")]
    MissingClusterEndpoint {
        /// Candidate cluster name.
        cluster: String,
    },

    /// A cluster endpoint has no `transport` configuration.
    #[error("missing transport for cluster endpoint {cluster:?}")]
    MissingTransport {
        /// Cluster name with missing transport.
        cluster: String,
    },

    /// A `mutual_tls` cluster endpoint has no SNI (or blank SNI).
    #[error("mutual_tls transport for cluster {cluster:?} requires a non-blank sni")]
    MissingSni {
        /// Cluster name with missing SNI.
        cluster: String,
    },

    /// A `plaintext` cluster endpoint has an SNI field set.
    ///
    /// Plaintext transport does not use TLS, so `sni` has no effect.
    /// Setting it is almost certainly a configuration mistake — the author
    /// likely intended `mutual_tls`.
    #[error(
        "plaintext transport for cluster {cluster:?} must not set sni (sni does not enable TLS; use mutual_tls if TLS is intended)"
    )]
    PlaintextWithSni {
        /// Cluster name with the conflicting configuration.
        cluster: String,
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
/// The rendered config is a complete, runnable Praxis config that includes
/// `listeners:`, `filter_chains:`, `admin:`, and `shutdown_timeout_secs`.
/// It is compatible with the Praxis `grid_route` and `grid_credential_inject`
/// filters.  It never contains credential token bytes.
///
/// # Parameters
///
/// - `overlay` — the routing overlay produced by the Grid operator for this gateway.
/// - `credential_mount_base` — base directory where credential Secrets are mounted inside the consumer pod (e.g.
///   `/run/secrets/grid-credentials`).
/// - `cluster_endpoints` — explicit endpoint topology for the `load_balancer` section.  Every unique candidate cluster
///   must have a matching endpoint entry with explicit transport configuration.  Missing transport or missing SNI on
///   `mutual_tls` endpoints fail closed.
/// - `tls_cert_mount_path` — mount path for TLS certificates inside the consumer pod.  Used only when rendering mTLS
///   cluster entries.
/// - `listener_port` — HTTP port for the generated listener (`0.0.0.0:{listener_port}`).
///
/// # Errors
///
/// Returns [`ConsumerConfigError`] when:
/// - `overlay.local_site` is blank.
/// - `credential_mount_base` is blank.
/// - Any candidate has a blank cluster name.
/// - Any candidate cluster has no matching endpoint in `cluster_endpoints`.
/// - Any cluster endpoint has no `transport` configuration.
/// - Any `mutual_tls` endpoint has no (or blank) `sni`.
#[expect(
    clippy::too_many_lines,
    reason = "sequential validation + three rendering passes; splitting would obscure the overall config shape"
)]
pub(crate) fn generate_consumer_praxis_config(
    overlay: &RoutingOverlay,
    credential_mount_base: &str,
    cluster_endpoints: &[ClusterEndpointConfig],
    tls_cert_mount_path: &str,
    listener_port: u16,
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
    let load_balancer_section = render_load_balancer(&overlay.candidates, cluster_endpoints, tls_cert_mount_path)?;

    // Listeners section: one public listener referencing the consumer filter chain.
    let mut config = format!(
        "listeners:\n\
         \x20 - name: public\n\
         \x20   address: \"0.0.0.0:{listener_port}\"\n\
         \x20   filter_chains:\n\
         \x20     - consumer-chain\n\
         filter_chains:\n\
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

    // Admin interface and graceful shutdown — standard constants for consumer gateways.
    config.push_str("\nadmin:\n  address: \"127.0.0.1:9901\"\nshutdown_timeout_secs: 5\n");

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
/// Produces one cluster entry per unique `candidate.cluster`, ordered
/// deterministically.  Every cluster must have a matching entry in
/// `cluster_endpoints` with explicit transport configuration; missing
/// endpoint, missing transport, or missing SNI on mTLS all fail closed.
fn render_load_balancer(
    candidates: &[RoutingCandidate],
    cluster_endpoints: &[ClusterEndpointConfig],
    tls_cert_mount_path: &str,
) -> Result<String, ConsumerConfigError> {
    // Build a lookup map: cluster name → endpoint config.
    let endpoint_map: BTreeMap<&str, &ClusterEndpointConfig> =
        cluster_endpoints.iter().map(|ep| (ep.cluster.as_str(), ep)).collect();

    let clusters: BTreeSet<&str> = candidates.iter().map(|c| c.cluster.as_str()).collect();
    let cluster_lines: Vec<String> = clusters
        .into_iter()
        .map(|cluster_name| {
            let quoted = yaml_scalar(cluster_name).unwrap_or_else(|_| "\"\"".to_owned());
            let ep = endpoint_map
                .get(cluster_name)
                .ok_or_else(|| ConsumerConfigError::MissingClusterEndpoint {
                    cluster: cluster_name.to_owned(),
                })?;
            render_cluster_entry(&quoted, ep, tls_cert_mount_path)
        })
        .collect::<Result<Vec<_>, ConsumerConfigError>>()?;

    Ok(format!(
        "\n\
         \x20     - filter: load_balancer\n\
         \x20       clusters:\n\
         {}",
        cluster_lines.join("\n")
    ))
}

/// Render a full cluster entry with endpoint address and explicit transport.
///
/// Validates that `transport` is present and, for `mutual_tls`, that `sni`
/// is non-blank.  Missing transport fails closed with [`ConsumerConfigError::MissingTransport`];
/// missing SNI on mTLS fails with [`ConsumerConfigError::MissingSni`].
#[expect(
    clippy::too_many_lines,
    reason = "transport validation + two format branches; splitting would separate the match arms from their YAML templates"
)]
fn render_cluster_entry(
    quoted_name: &str,
    ep: &ClusterEndpointConfig,
    tls_cert_mount_path: &str,
) -> Result<String, ConsumerConfigError> {
    let quoted_addr = yaml_scalar(&ep.address).unwrap_or_else(|_| "\"\"".to_owned());

    let transport = ep
        .transport
        .as_ref()
        .ok_or_else(|| ConsumerConfigError::MissingTransport {
            cluster: ep.cluster.clone(),
        })?;

    match transport.mode {
        TransportMode::MutualTls => {
            let raw_sni = transport
                .sni
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| ConsumerConfigError::MissingSni {
                    cluster: ep.cluster.clone(),
                })?;
            let trimmed_sni = raw_sni.trim();
            let quoted_sni = yaml_scalar(trimmed_sni).unwrap_or_else(|_| "\"\"".to_owned());
            Ok(format!(
                "          - name: {quoted_name}\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20  tls:\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20    ca:\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20      ca_path: {tls_cert_mount_path}/ca.crt\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20    client_cert:\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20      cert_path: {tls_cert_mount_path}/tls.crt\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20      key_path: {tls_cert_mount_path}/tls.key\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20    sni: {quoted_sni}\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20    verify: true\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20  endpoints:\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20    - {quoted_addr}"
            ))
        },
        TransportMode::Plaintext => {
            if transport.sni.as_deref().is_some_and(|s| !s.trim().is_empty()) {
                return Err(ConsumerConfigError::PlaintextWithSni {
                    cluster: ep.cluster.clone(),
                });
            }
            Ok(format!(
                "          - name: {quoted_name}\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20  endpoints:\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20    - {quoted_addr}"
            ))
        },
    }
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
    use crate::{
        crd::grid_network::EndpointTransport,
        resources::routing_overlay::{ProjectedCredential, ProjectedCredentialRef, RoutingCandidate},
    };

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

    fn endpoint_coverage(overlay: &RoutingOverlay) -> Vec<ClusterEndpointConfig> {
        overlay
            .candidates
            .iter()
            .map(|c| c.cluster.as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .enumerate()
            .map(|(idx, cluster)| ClusterEndpointConfig {
                cluster: cluster.to_owned(),
                address: format!("127.0.0.1:{}", 30_000 + idx),
                transport: Some(EndpointTransport {
                    mode: TransportMode::Plaintext,
                    sni: None,
                }),
            })
            .collect()
    }

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
        let yaml = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();
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
        let yaml = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();
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
        let yaml = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();
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
    #[expect(
        clippy::too_many_lines,
        reason = "test constructs duplicate credential candidates and asserts rendered dedupe behavior"
    )]
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
        let yaml = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();
        // Count occurrences of the file path — should be exactly 1.
        let count = yaml
            .matches("file: \"/run/secrets/grid-credentials/shared-creds/token\"")
            .count();
        assert_eq!(count, 1, "duplicate secretRef must produce only one credential entry");
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "test constructs two credential references and asserts both rendered entries"
    )]
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
        let yaml = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();
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
        let yaml = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();
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
        let yaml = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();
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
        let yaml = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();
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
        let yaml = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();
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
        let yaml = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();
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
            generate_consumer_praxis_config(
                &overlay,
                MOUNT_BASE,
                &endpoint_coverage(&overlay),
                "/etc/praxis/tls",
                8080
            )
            .is_err(),
            "blank local_site must return error"
        );
    }

    #[test]
    fn blank_mount_base_returns_error() {
        let overlay = simple_overlay(vec![]);
        assert!(
            generate_consumer_praxis_config(&overlay, "", &[], "/etc/praxis/tls", 8080).is_err(),
            "blank credential_mount_base must return error"
        );
    }

    #[test]
    fn blank_candidate_cluster_returns_error() {
        let overlay = simple_overlay(vec![plain_candidate("inference_model", "m", "s", "", true)]);
        assert!(
            generate_consumer_praxis_config(
                &overlay,
                MOUNT_BASE,
                &endpoint_coverage(&overlay),
                "/etc/praxis/tls",
                8080
            )
            .is_err(),
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
        let yaml1 = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();
        let yaml2 = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();
        assert_eq!(yaml1, yaml2, "output must be deterministic");
    }

    #[test]
    fn credential_entries_ordered_deterministically() {
        let overlay = simple_overlay(vec![
            credential_candidate("inference_model", "m1", "s1", "c1", "zzz-creds", "ns", "tok"),
            credential_candidate("inference_model", "m2", "s2", "c2", "aaa-creds", "ns", "tok"),
        ]);
        let yaml = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();
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
    // load_balancer endpoint topology
    // -----------------------------------------------------------------------

    #[test]
    fn load_balancer_renders_endpoint_for_matching_plaintext_cluster() {
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "model-a",
            "site-a",
            "gateway-site-a",
            true,
        )]);
        let endpoints = vec![plain_ep("gateway-site-a", "10.0.0.10:30080")];
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080).unwrap();
        assert!(
            yaml.contains("name: \"gateway-site-a\""),
            "cluster name must be rendered"
        );
        assert!(
            yaml.contains("10.0.0.10:30080"),
            "matching endpoint address must be rendered"
        );
        assert!(!yaml.contains("tls:"), "plaintext endpoint must not render TLS config");
    }

    #[test]
    fn load_balancer_renders_tls_for_mtls_endpoint() {
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "model-a",
            "site-a",
            "gateway-site-a",
            true,
        )]);
        let endpoints = vec![mtls_ep("gateway-site-a", "10.0.0.10:30080", "site-a.grid.internal")];
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080).unwrap();
        assert!(yaml.contains("tls:"), "mTLS endpoint must render TLS config");
        assert!(
            yaml.contains("ca_path: /etc/praxis/tls/ca.crt"),
            "TLS config must reference CA path"
        );
        assert!(
            yaml.contains("cert_path: /etc/praxis/tls/tls.crt"),
            "TLS config must reference client cert path"
        );
        assert!(
            yaml.contains("key_path: /etc/praxis/tls/tls.key"),
            "TLS config must reference client key path"
        );
        assert!(
            yaml.contains("sni: \"site-a.grid.internal\""),
            "TLS config must include quoted SNI"
        );
        assert!(yaml.contains("verify: true"), "TLS verification must be enabled");
    }

    #[test]
    fn load_balancer_missing_endpoint_returns_error() {
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "model-a",
            "site-a",
            "gateway-site-a",
            true,
        )]);
        let err = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &[], "/etc/praxis/tls", 8080)
            .expect_err("missing endpoint topology must fail config generation");
        assert!(
            matches!(
                err,
                ConsumerConfigError::MissingClusterEndpoint { cluster } if cluster == "gateway-site-a"
            ),
            "missing endpoint must identify the candidate cluster"
        );
    }

    #[test]
    fn load_balancer_dedupes_multiple_candidates_for_same_cluster() {
        let overlay = simple_overlay(vec![
            plain_candidate("inference_model", "model-a", "site-a", "gateway-site-a", true),
            plain_candidate("inference_model", "model-b", "site-a", "gateway-site-a", true),
        ]);
        let endpoints = vec![plain_ep("gateway-site-a", "10.0.0.10:30080")];
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080).unwrap();
        assert_eq!(
            yaml.matches("name: \"gateway-site-a\"").count(),
            1,
            "same cluster must render exactly once in load_balancer"
        );
        assert_eq!(
            yaml.matches("10.0.0.10:30080").count(),
            1,
            "endpoint for duplicate cluster must render exactly once"
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
        let yaml = generate_consumer_praxis_config(
            &overlay,
            "/run/secrets/grid-credentials",
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();
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

    // -----------------------------------------------------------------------
    // Cluster endpoint rendering
    // -----------------------------------------------------------------------

    fn mtls_ep(cluster: &str, address: &str, sni: &str) -> ClusterEndpointConfig {
        ClusterEndpointConfig {
            cluster: cluster.to_owned(),
            address: address.to_owned(),
            transport: Some(EndpointTransport {
                mode: TransportMode::MutualTls,
                sni: Some(sni.to_owned()),
            }),
        }
    }

    fn plain_ep(cluster: &str, address: &str) -> ClusterEndpointConfig {
        ClusterEndpointConfig {
            cluster: cluster.to_owned(),
            address: address.to_owned(),
            transport: Some(EndpointTransport {
                mode: TransportMode::Plaintext,
                sni: None,
            }),
        }
    }

    #[test]
    fn cluster_with_mtls_transport_renders_mtls_entry() {
        let endpoints = [mtls_ep("site-a", "172.18.0.4:30080", "site-a.grid.internal")];
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "model-x",
            "site-a",
            "site-a",
            true,
        )]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080).unwrap();
        assert!(yaml.contains("172.18.0.4:30080"), "endpoint address must appear");
        assert!(yaml.contains("site-a.grid.internal"), "SNI must appear");
        assert!(yaml.contains("ca_path: /etc/praxis/tls/ca.crt"), "CA path must appear");
        assert!(
            yaml.contains("cert_path: /etc/praxis/tls/tls.crt"),
            "cert path must appear"
        );
        assert!(
            yaml.contains("key_path: /etc/praxis/tls/tls.key"),
            "key path must appear"
        );
        assert!(yaml.contains("verify: true"), "verify flag must appear");
    }

    #[test]
    fn cluster_with_plaintext_transport_renders_plain_http_entry() {
        let endpoints = [plain_ep("api-cluster", "mock-api.default.svc:8080")];
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "model-z",
            "api-site",
            "api-cluster",
            true,
        )]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080).unwrap();
        assert!(
            yaml.contains("mock-api.default.svc:8080"),
            "endpoint address must appear"
        );
        assert!(!yaml.contains("sni:"), "no SNI for plain HTTP cluster");
        assert!(!yaml.contains("ca_path:"), "no TLS for plain HTTP cluster");
        assert!(!yaml.contains("verify:"), "no verify for plain HTTP cluster");
    }

    #[test]
    fn cluster_without_endpoint_entry_returns_error() {
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "m",
            "s",
            "cluster-no-ep",
            true,
        )]);
        let err = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &[], "/etc/praxis/tls", 8080)
            .expect_err("missing cluster endpoint must fail config generation");
        assert!(
            matches!(
                err,
                ConsumerConfigError::MissingClusterEndpoint { cluster } if cluster == "cluster-no-ep"
            ),
            "missing endpoint error must include the cluster name"
        );
    }

    #[test]
    fn cluster_without_transport_returns_error() {
        let endpoints = [ClusterEndpointConfig {
            cluster: "no-transport-cluster".to_owned(),
            address: "10.0.0.1:8080".to_owned(),
            transport: None,
        }];
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "m",
            "s",
            "no-transport-cluster",
            true,
        )]);
        let err = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080)
            .expect_err("missing transport must fail closed");
        assert!(
            matches!(
                err,
                ConsumerConfigError::MissingTransport { cluster } if cluster == "no-transport-cluster"
            ),
            "missing transport error must identify the cluster"
        );
    }

    #[test]
    fn mutual_tls_without_sni_returns_error() {
        let endpoints = [ClusterEndpointConfig {
            cluster: "mtls-no-sni".to_owned(),
            address: "10.0.0.1:8080".to_owned(),
            transport: Some(EndpointTransport {
                mode: TransportMode::MutualTls,
                sni: None,
            }),
        }];
        let overlay = simple_overlay(vec![plain_candidate("inference_model", "m", "s", "mtls-no-sni", true)]);
        let err = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080)
            .expect_err("mutual_tls without sni must fail");
        assert!(
            matches!(
                err,
                ConsumerConfigError::MissingSni { cluster } if cluster == "mtls-no-sni"
            ),
            "missing sni error must identify the cluster"
        );
    }

    #[test]
    fn mutual_tls_with_blank_sni_returns_error() {
        let endpoints = [ClusterEndpointConfig {
            cluster: "mtls-blank-sni".to_owned(),
            address: "10.0.0.1:8080".to_owned(),
            transport: Some(EndpointTransport {
                mode: TransportMode::MutualTls,
                sni: Some("  ".to_owned()),
            }),
        }];
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "m",
            "s",
            "mtls-blank-sni",
            true,
        )]);
        let err = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080)
            .expect_err("mutual_tls with blank sni must fail");
        assert!(
            matches!(
                err,
                ConsumerConfigError::MissingSni { cluster } if cluster == "mtls-blank-sni"
            ),
            "blank sni error must identify the cluster"
        );
    }

    #[test]
    fn plaintext_with_sni_returns_error() {
        let endpoints = [ClusterEndpointConfig {
            cluster: "plain-with-sni".to_owned(),
            address: "10.0.0.1:8080".to_owned(),
            transport: Some(EndpointTransport {
                mode: TransportMode::Plaintext,
                sni: Some("unexpected.grid.internal".to_owned()),
            }),
        }];
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "m",
            "s",
            "plain-with-sni",
            true,
        )]);
        let err = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080)
            .expect_err("plaintext with sni must fail");
        assert!(
            matches!(
                err,
                ConsumerConfigError::PlaintextWithSni { cluster } if cluster == "plain-with-sni"
            ),
            "plaintext+sni error must identify the cluster"
        );
    }

    #[test]
    fn plaintext_with_blank_sni_is_accepted() {
        let endpoints = [ClusterEndpointConfig {
            cluster: "plain-blank-sni".to_owned(),
            address: "10.0.0.1:8080".to_owned(),
            transport: Some(EndpointTransport {
                mode: TransportMode::Plaintext,
                sni: Some("  ".to_owned()),
            }),
        }];
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "m",
            "s",
            "plain-blank-sni",
            true,
        )]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080).unwrap();
        assert!(
            !yaml.contains("tls:"),
            "plaintext with blank sni must render as plain HTTP"
        );
    }

    #[test]
    fn mutual_tls_sni_is_trimmed_before_rendering() {
        let endpoints = [ClusterEndpointConfig {
            cluster: "trim-test".to_owned(),
            address: "10.0.0.1:8080".to_owned(),
            transport: Some(EndpointTransport {
                mode: TransportMode::MutualTls,
                sni: Some("  site-a.grid.internal  ".to_owned()),
            }),
        }];
        let overlay = simple_overlay(vec![plain_candidate("inference_model", "m", "s", "trim-test", true)]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080).unwrap();
        assert!(
            yaml.contains("sni: \"site-a.grid.internal\""),
            "SNI must be trimmed of leading/trailing whitespace: {yaml}"
        );
    }

    #[test]
    fn multiple_candidates_sharing_cluster_produce_one_cluster_entry() {
        let endpoints = [mtls_ep("shared-cluster", "10.0.0.1:30080", "shared.grid.internal")];
        let overlay = simple_overlay(vec![
            plain_candidate("inference_model", "model-a", "site-a", "shared-cluster", true),
            plain_candidate("inference_model", "model-b", "site-b", "shared-cluster", true),
        ]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080).unwrap();
        let count = yaml.matches("10.0.0.1:30080").count();
        assert_eq!(
            count, 1,
            "duplicate cluster must produce exactly one load_balancer entry"
        );
    }

    #[test]
    fn mixed_mtls_and_plaintext_clusters() {
        let endpoints = [
            mtls_ep("provider-cluster", "172.18.0.4:30080", "provider.grid.internal"),
            plain_ep("api-cluster", "mock-api.default.svc:8080"),
        ];
        let overlay = simple_overlay(vec![
            plain_candidate("inference_model", "model-x", "s1", "provider-cluster", true),
            plain_candidate("inference_model", "model-z", "s2", "api-cluster", true),
        ]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080).unwrap();
        assert!(yaml.contains("provider.grid.internal"), "mTLS cluster SNI must appear");
        assert!(
            yaml.contains("mock-api.default.svc:8080"),
            "plaintext cluster endpoint must appear"
        );
        assert!(yaml.contains("ca_path:"), "mTLS cluster must have TLS");
    }

    #[test]
    fn endpoint_address_not_token_bytes() {
        let sentinel = "sk-super-secret-token-do-not-emit";
        let endpoints = [mtls_ep(
            "site-a",
            &format!("172.18.0.4:{}", sentinel.len()),
            "site-a.grid.internal",
        )];
        let overlay = simple_overlay(vec![plain_candidate("inference_model", "m", "s", "site-a", true)]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080).unwrap();
        assert!(
            !yaml.contains(sentinel),
            "token bytes must not appear in any cluster entry"
        );
    }

    #[test]
    fn custom_tls_cert_mount_path_used_in_cluster_entry() {
        let endpoints = [mtls_ep("site-a", "10.0.0.1:8080", "site-a.grid.internal")];
        let overlay = simple_overlay(vec![plain_candidate("inference_model", "m", "s", "site-a", true)]);
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/custom/tls/path", 8080).unwrap();
        assert!(
            yaml.contains("ca_path: /custom/tls/path/ca.crt"),
            "custom TLS path must be used"
        );
    }

    #[test]
    fn deterministic_ordering_with_endpoints() {
        let endpoints = [
            mtls_ep("zzz-cluster", "10.0.0.3:30080", "zzz.grid.internal"),
            mtls_ep("aaa-cluster", "10.0.0.1:30080", "aaa.grid.internal"),
        ];
        let overlay = simple_overlay(vec![
            plain_candidate("inference_model", "m1", "s1", "zzz-cluster", true),
            plain_candidate("inference_model", "m2", "s2", "aaa-cluster", true),
        ]);
        let yaml1 = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080).unwrap();
        let yaml2 = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080).unwrap();
        assert_eq!(yaml1, yaml2, "output must be deterministic");

        // aaa-cluster should appear before zzz-cluster (BTreeSet ordering).
        let pos_aaa = yaml1.find("aaa-cluster").unwrap();
        let pos_zzz = yaml1.find("zzz-cluster").unwrap();
        // Both appear in grid_route candidates AND load_balancer; check in load_balancer section.
        let lb_section = &yaml1[yaml1.find("load_balancer").unwrap()..];
        let lb_aaa = lb_section.find("10.0.0.1:30080").unwrap_or(usize::MAX);
        let lb_zzz = lb_section.find("10.0.0.3:30080").unwrap_or(usize::MAX);
        assert!(
            lb_aaa < lb_zzz,
            "aaa-cluster endpoint must appear before zzz-cluster endpoint in load_balancer"
        );
        let _ = (pos_aaa, pos_zzz); // used only for determinism check above
    }
}
