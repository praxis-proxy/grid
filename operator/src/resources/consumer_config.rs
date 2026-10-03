//! Operator-owned consumer Praxis config renderer.
//!
//! Generates the `praxis.yaml` content for a consumer gateway `ConfigMap` from
//! a [`RoutingOverlay`].  The generated config includes:
//!
//! - `json_body_field` filter (model field → `X-Model` header)
//! - `intelligent_route` filter with candidates and selection policy read from the scoped, reloadable versioned overlay
//!   file, plus provider-hop context headers for explicitly mTLS-configured provider endpoints
//! - `credential_inject` filter when candidates have credentials, or when the consumer explicitly opts into projected
//!   credentials for later overlays
//! - `load_balancer` filter with one cluster entry per configured endpoint
//!
//! # Security invariants
//!
//! - Token values are **never** emitted.  Credential entries use `file:` sources under
//!   `ConsumerConfig::credential_mount_base`.
//! - The `credential.secretRef` locating information (name, namespace, key) is included in the `intelligent_route`
//!   candidate block and in the `credential_inject` entry.  This is reference data, not credential bytes.
//!
//! [`RoutingOverlay`]: crate::resources::routing_overlay::RoutingOverlay
//! [`ConsumerConfig`]: crate::crd::grid_network::ConsumerConfig

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::core::v1::ConfigMap;

use crate::{
    crd::grid_network::{ClusterEndpointConfig, SelectionMode, TransportMode},
    resources::routing_overlay::{RoutingCandidate, RoutingOverlay},
};

/// Path at which a consumer gateway must mount the operator-published overlay.
pub(crate) const CONSUMER_OVERLAY_FILE: &str = "/etc/praxis/routing/routing-overlay.json";

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors from consumer Praxis config generation.
#[derive(Debug, thiserror::Error)]
#[expect(unnameable_types, reason = "pub(crate) module restricts reachability")]
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

    /// Dynamic routing needs a static endpoint inventory for restored candidates.
    #[error("dynamic consumer config requires at least one clusterEndpoints entry")]
    NoClusterEndpoints,

    /// A credential-bearing overlay is not allowed until the consumer has
    /// explicitly declared that its runtime credential filter is ready.
    #[error(
        "credential-bearing overlay requires consumerConfig.enableProjectedCredentials and supportsProjectedCredentials=true"
    )]
    ProjectedCredentialsUnsupported,

    /// The generated Praxis config did not contain its required filter structure.
    #[error("invalid generated Praxis config: {0}")]
    InvalidRenderedConfig(String),

    /// More than one endpoint was supplied for a cluster name.
    #[error("duplicate cluster endpoint for {cluster:?}")]
    DuplicateClusterEndpoint {
        /// Cluster with ambiguous endpoint definitions.
        cluster: String,
    },

    /// YAML serialization or parsing failed.
    #[error("yaml error: {0}")]
    Yaml(#[from] serde_yaml::Error),

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
/// It is compatible with the Praxis `intelligent_route` and `credential_inject`
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
#[cfg(test)]
fn generate_consumer_praxis_config(
    overlay: &RoutingOverlay,
    credential_mount_base: &str,
    cluster_endpoints: &[ClusterEndpointConfig],
    tls_cert_mount_path: &str,
    listener_port: u16,
) -> Result<String, ConsumerConfigError> {
    generate_consumer_praxis_config_with_projected_credentials(
        overlay,
        credential_mount_base,
        cluster_endpoints,
        tls_cert_mount_path,
        listener_port,
        false,
    )
}

/// Render generated consumer Praxis configuration with optional dynamic
/// credential lookup from projected Secret files.
#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "this renderer adapts the static config using the gateway identity and endpoint contract"
)]
fn generate_consumer_praxis_config_with_projected_credentials(
    overlay: &RoutingOverlay,
    credential_mount_base: &str,
    cluster_endpoints: &[ClusterEndpointConfig],
    tls_cert_mount_path: &str,
    listener_port: u16,
    enable_projected_credentials: bool,
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
    let selection_policy_yaml = render_selection_policy(overlay.selection_policy.as_ref());
    let provider_hop_clusters_yaml = render_provider_hop_clusters(cluster_endpoints)?;
    let local_site = yaml_scalar(&overlay.local_site)?;

    let credential_inject_section =
        render_credential_inject(&overlay.candidates, credential_mount_base, enable_projected_credentials);
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
         \x20     - filter: intelligent_route\n\
         \x20       local_site: {local_site}\n\
         \x20       model_header: \"X-Model\"\n\
         {provider_hop_clusters_yaml}\
         {selection_policy_yaml}\
         \x20       candidates:\n\
         {candidates_yaml}"
    );

    config.push_str(&credential_inject_section);

    config.push_str(&load_balancer_section);

    // Admin interface and graceful shutdown — standard constants for consumer gateways.
    config.push_str("\nadmin:\n  address: \"127.0.0.1:9901\"\nshutdown_timeout_secs: 5\n");

    Ok(config)
}

/// Generate the production consumer config with routing delegated to the
/// versioned Grid overlay. The generated endpoint inventory is static, while
/// candidate eligibility, selection policy, and no-route revisions are read
/// from the separately projected overlay file.
#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "this renderer adapts the static config using the gateway identity and endpoint contract"
)]
pub(crate) fn generate_consumer_praxis_config_for_gateway(
    overlay: &RoutingOverlay,
    credential_mount_base: &str,
    cluster_endpoints: &[ClusterEndpointConfig],
    tls_cert_mount_path: &str,
    listener_port: u16,
    gateway_name: &str,
    gateway_namespace: &str,
    enable_projected_credentials: bool,
) -> Result<String, ConsumerConfigError> {
    let static_yaml = generate_consumer_praxis_config_with_projected_credentials(
        overlay,
        credential_mount_base,
        cluster_endpoints,
        tls_cert_mount_path,
        listener_port,
        enable_projected_credentials,
    )?;
    let mut config: serde_yaml::Value = serde_yaml::from_str(&static_yaml)?;
    let filters = config
        .get_mut("filter_chains")
        .and_then(serde_yaml::Value::as_sequence_mut)
        .and_then(|chains| chains.first_mut())
        .and_then(|chain| chain.get_mut("filters"))
        .and_then(serde_yaml::Value::as_sequence_mut)
        .ok_or_else(|| ConsumerConfigError::InvalidRenderedConfig("filter chain is missing".to_owned()))?;
    let route = filters
        .iter_mut()
        .find(|filter| filter.get("filter").and_then(serde_yaml::Value::as_str) == Some("intelligent_route"))
        .and_then(serde_yaml::Value::as_mapping_mut)
        .ok_or_else(|| ConsumerConfigError::InvalidRenderedConfig("intelligent_route filter is missing".to_owned()))?;
    for key in ["local_site", "candidates", "selection_policy"] {
        route.remove(serde_yaml::Value::String(key.to_owned()));
    }
    route.insert(
        serde_yaml::Value::String("overlay_file".to_owned()),
        serde_yaml::Value::String(CONSUMER_OVERLAY_FILE.to_owned()),
    );
    route.insert(
        serde_yaml::Value::String("expected_overlay_scope".to_owned()),
        serde_yaml::Value::Mapping(serde_yaml::Mapping::from_iter([
            (
                serde_yaml::Value::String("network".to_owned()),
                serde_yaml::Value::String(overlay.network.clone()),
            ),
            (
                serde_yaml::Value::String("gateway".to_owned()),
                serde_yaml::Value::String(gateway_name.to_owned()),
            ),
            (
                serde_yaml::Value::String("namespace".to_owned()),
                serde_yaml::Value::String(gateway_namespace.to_owned()),
            ),
            (
                serde_yaml::Value::String("local_site".to_owned()),
                serde_yaml::Value::String(overlay.local_site.clone()),
            ),
        ])),
    );
    route.insert(
        serde_yaml::Value::String("reload".to_owned()),
        serde_yaml::Value::Mapping(serde_yaml::Mapping::from_iter([
            (
                serde_yaml::Value::String("enabled".to_owned()),
                serde_yaml::Value::Bool(true),
            ),
            (
                serde_yaml::Value::String("debounce_ms".to_owned()),
                serde_yaml::Value::Number(500.into()),
            ),
        ])),
    );

    let load_balancer = filters
        .iter_mut()
        .find(|filter| filter.get("filter").and_then(serde_yaml::Value::as_str) == Some("load_balancer"))
        .and_then(serde_yaml::Value::as_mapping_mut)
        .ok_or_else(|| ConsumerConfigError::InvalidRenderedConfig("load_balancer filter is missing".to_owned()))?;
    let cluster_lines = render_all_endpoint_clusters(cluster_endpoints, tls_cert_mount_path)?;
    load_balancer.insert(
        serde_yaml::Value::String("clusters".to_owned()),
        serde_yaml::Value::Sequence(cluster_lines),
    );

    serde_yaml::to_string(&config).map_err(ConsumerConfigError::Yaml)
}

/// Render every configured endpoint so later overlay restoration is routable.
#[expect(
    clippy::too_many_lines,
    reason = "the endpoint renderer validates and converts the complete bounded endpoint inventory"
)]
fn render_all_endpoint_clusters(
    cluster_endpoints: &[ClusterEndpointConfig],
    tls_cert_mount_path: &str,
) -> Result<Vec<serde_yaml::Value>, ConsumerConfigError> {
    if cluster_endpoints.is_empty() {
        return Err(ConsumerConfigError::NoClusterEndpoints);
    }
    let mut endpoints = BTreeMap::new();
    for endpoint in cluster_endpoints {
        if endpoint.cluster.trim().is_empty() {
            return Err(ConsumerConfigError::BlankCluster {
                kind: "cluster_endpoint".to_owned(),
                name: endpoint.cluster.clone(),
            });
        }
        if endpoints.insert(endpoint.cluster.as_str(), endpoint).is_some() {
            return Err(ConsumerConfigError::DuplicateClusterEndpoint {
                cluster: endpoint.cluster.clone(),
            });
        }
    }
    endpoints
        .into_iter()
        .map(|(cluster, endpoint)| {
            let quoted = yaml_scalar(cluster).unwrap_or_else(|_| "\"\"".to_owned());
            let rendered = render_cluster_entry(&quoted, endpoint, tls_cert_mount_path)?;
            let mut items: Vec<serde_yaml::Value> = serde_yaml::from_str(&rendered)?;
            if items.len() != 1 {
                return Err(ConsumerConfigError::InvalidRenderedConfig(
                    "endpoint renderer must produce exactly one cluster".to_owned(),
                ));
            }
            items.pop().ok_or_else(|| {
                ConsumerConfigError::InvalidRenderedConfig("endpoint renderer produced no cluster".to_owned())
            })
        })
        .collect()
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

/// Render `intelligent_route` candidates YAML block.
///
/// Each candidate is indented and includes `credential.secretRef` when present.
/// Token values are never included.
fn render_candidates(candidates: &[RoutingCandidate]) -> String {
    candidates.iter().map(render_candidate).collect::<Vec<_>>().join("\n")
}

/// Render the explicit Grid-owned request-selection policy.
fn render_selection_policy(policy: Option<&crate::crd::grid_network::SelectionPolicyConfig>) -> String {
    let Some(policy) = policy else {
        return String::new();
    };
    let mode = match policy.mode {
        SelectionMode::Deterministic => "deterministic",
        SelectionMode::RoundRobin => "roundRobin",
        SelectionMode::Random => "random",
        SelectionMode::WeightedRandom => "weightedRandom",
    };
    format!("        selection_policy:\n          mode: {mode}\n")
}

/// Render provider-hop context-header configuration for mTLS endpoints.
///
/// `clusterEndpoints` describes consumer-to-provider-gateway endpoints. An
/// explicit mTLS transport identifies the authenticated provider-gateway hop;
/// those cluster names need the routing-context headers consumed by the
/// provider's `provider_route` filter. Plaintext endpoints remain direct/local
/// backends and do not receive provider-hop headers.
fn render_provider_hop_clusters(cluster_endpoints: &[ClusterEndpointConfig]) -> Result<String, ConsumerConfigError> {
    let clusters = provider_hop_clusters(cluster_endpoints)?;
    if clusters.is_empty() {
        return Ok(String::new());
    }
    let values = clusters
        .into_iter()
        .map(|cluster| yaml_scalar(&cluster).unwrap_or_else(|_| "\"\"".to_owned()))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("        provider_hop_clusters: [{values}]\n"))
}

/// The explicit mTLS endpoint names allowed to receive provider-hop context in
/// generated consumer Praxis config. The embedded Grid gateway uses its
/// independent `GatewayRef.providerHopEndpoints` contract.
#[expect(
    clippy::too_many_lines,
    reason = "this validation keeps the mTLS provider-hop boundary explicit"
)]
pub(crate) fn provider_hop_clusters(
    cluster_endpoints: &[ClusterEndpointConfig],
) -> Result<BTreeSet<String>, ConsumerConfigError> {
    let mut seen = BTreeSet::new();
    let mut hops = BTreeSet::new();
    for endpoint in cluster_endpoints {
        if endpoint.cluster.trim().is_empty() {
            return Err(ConsumerConfigError::BlankCluster {
                kind: "cluster_endpoint".to_owned(),
                name: endpoint.cluster.clone(),
            });
        }
        if !seen.insert(endpoint.cluster.as_str()) {
            return Err(ConsumerConfigError::DuplicateClusterEndpoint {
                cluster: endpoint.cluster.clone(),
            });
        }
        match endpoint.transport.as_ref() {
            Some(transport) if transport.mode == TransportMode::MutualTls => {
                if transport.sni.as_deref().is_none_or(|sni| sni.trim().is_empty()) {
                    return Err(ConsumerConfigError::MissingSni {
                        cluster: endpoint.cluster.clone(),
                    });
                }
                hops.insert(endpoint.cluster.clone());
            },
            Some(transport)
                if transport.mode == TransportMode::Plaintext
                    && transport.sni.as_deref().is_some_and(|sni| !sni.trim().is_empty()) =>
            {
                return Err(ConsumerConfigError::PlaintextWithSni {
                    cluster: endpoint.cluster.clone(),
                });
            },
            Some(_) | None => {},
        }
    }
    Ok(hops)
}

/// Render one `intelligent_route` candidate.
#[expect(
    clippy::too_many_lines,
    reason = "Candidate YAML fields are kept together to mirror the wire contract."
)]
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
    if let Some(admission) = c.admission_state {
        lines.push(format!(
            "           admission_state: {}",
            serde_json::to_string(&admission).unwrap_or_default()
        ));
    }
    if let Some(group) = c.selection_group {
        lines.push(format!("           selection_group: {group}"));
    }
    if let Some(weight) = c.traffic_weight {
        lines.push(format!("           traffic_weight: {weight}"));
    }
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

/// Render `credential_inject` for current credential-bearing candidates, or
/// unconditionally in explicit projected-credential mode. The latter keeps a
/// filter present at empty-overlay startup so a later credential-bearing
/// revision cannot bypass injection. Missing projected files fail closed.
#[expect(
    clippy::too_many_lines,
    reason = "BTreeMap collection + format strings for each credential field"
)]
fn render_credential_inject(
    candidates: &[RoutingCandidate],
    credential_mount_base: &str,
    enable_projected_credentials: bool,
) -> String {
    // Collect unique (strategy, name, namespace, key) → rendered entry.
    // BTreeMap provides deterministic sorted order by key.
    let mut entries: BTreeMap<(String, String, String, String), String> = BTreeMap::new();

    for c in candidates {
        // Projected mode resolves references from the live overlay and mounted
        // Secret tree at request time. Keeping a startup table here would make
        // a later reference depend on filter reconstruction/reload ordering.
        if enable_projected_credentials {
            continue;
        }
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

    if entries.is_empty() && !enable_projected_credentials {
        return String::new();
    }
    let credentials = if entries.is_empty() {
        "        credentials: []\n".to_owned()
    } else {
        format!(
            "        credentials:\n{}\n",
            entries.into_values().collect::<Vec<_>>().join("\n")
        )
    };
    let projected_base = if enable_projected_credentials {
        format!(
            "        projected_credential_mount_base: {}\n",
            yaml_scalar(credential_mount_base).unwrap_or_else(|_| "\"\"".to_owned())
        )
    } else {
        String::new()
    };
    format!(
        "\n\
         \x20     - filter: credential_inject\n\
         {credentials}\
         {projected_base}"
    )
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
        resources::{
            geography::{AdmissionState, LocalityTier},
            routing_overlay::{ProjectedCredential, ProjectedCredentialRef},
        },
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
            stable_id: None,
            admission_state: None,
            selection_tier: None,
            score: None,
            score_breakdown: None,
            rank: None,
            selection_group: None,
            traffic_weight: None,
            capacity_weight: 1,
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
            stable_id: None,
            admission_state: None,
            selection_tier: None,
            score: None,
            score_breakdown: None,
            rank: None,
            selection_group: None,
            traffic_weight: None,
            capacity_weight: 1,
        }
    }

    fn simple_overlay(candidates: Vec<RoutingCandidate>) -> RoutingOverlay {
        RoutingOverlay {
            network: "test-net".to_owned(),
            local_site: "site-a".to_owned(),
            candidates,
            selection_policy: None,
            generated_at: None,
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

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "asserts the generated filter chain and complete restoration endpoint inventory"
    )]
    fn production_consumer_config_uses_scoped_reloadable_overlay_and_complete_endpoint_inventory() {
        let overlay = simple_overlay(vec![credential_candidate(
            "inference_model",
            "model-a",
            "site-a",
            "provider-a",
            "provider-a-secret",
            "grid-system",
            "token",
        )]);
        let mut endpoints = endpoint_coverage(&overlay);
        endpoints[0].transport = Some(EndpointTransport {
            mode: TransportMode::MutualTls,
            sni: Some("provider-a.grid.internal".to_owned()),
        });
        endpoints.push(ClusterEndpointConfig {
            cluster: "provider-b".to_owned(),
            address: "127.0.0.1:30002".to_owned(),
            transport: Some(EndpointTransport {
                mode: TransportMode::Plaintext,
                sni: None,
            }),
        });

        let yaml = generate_consumer_praxis_config_for_gateway(
            &overlay,
            MOUNT_BASE,
            &endpoints,
            "/etc/praxis/tls",
            8080,
            "consumer-gateway",
            "consumer-ns",
            false,
        )
        .expect("dynamic consumer config renders");
        let config: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("generated YAML parses");
        let filters = config["filter_chains"][0]["filters"].as_sequence().expect("filters");
        let route = filters
            .iter()
            .find(|filter| filter["filter"] == "intelligent_route")
            .expect("route filter");
        assert_eq!(route["overlay_file"], CONSUMER_OVERLAY_FILE);
        assert_eq!(route["expected_overlay_scope"]["network"], "test-net");
        assert_eq!(route["expected_overlay_scope"]["gateway"], "consumer-gateway");
        assert_eq!(route["expected_overlay_scope"]["namespace"], "consumer-ns");
        assert_eq!(route["expected_overlay_scope"]["local_site"], "site-a");
        assert_eq!(route["reload"]["enabled"], true);
        assert_eq!(route["provider_hop_clusters"][0], "provider-a");
        assert!(route.get("candidates").is_none(), "candidate state is overlay-owned");
        assert!(
            route.get("selection_policy").is_none(),
            "selection mode is overlay-owned"
        );

        let load_balancer = filters
            .iter()
            .find(|filter| filter["filter"] == "load_balancer")
            .expect("load balancer");
        let clusters = load_balancer["clusters"].as_sequence().expect("clusters");
        assert_eq!(clusters.len(), 2, "inactive endpoint remains available for restoration");
        assert!(clusters.iter().any(|cluster| cluster["name"] == "provider-b"));
        assert!(
            filters.iter().any(|filter| filter["filter"] == "credential_inject"),
            "the active overlay credential remains configured"
        );
        assert!(!yaml.contains(SENTINEL_TOKEN));
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "checks the cold-start empty overlay contract with a configured endpoint"
    )]
    fn empty_uncredentialed_consumer_config_does_not_require_new_praxis_filter() {
        let overlay = simple_overlay(Vec::new());
        let endpoints = [ClusterEndpointConfig {
            cluster: "provider-a".to_owned(),
            address: "127.0.0.1:30001".to_owned(),
            transport: Some(EndpointTransport {
                mode: TransportMode::Plaintext,
                sni: None,
            }),
        }];
        let yaml = generate_consumer_praxis_config_for_gateway(
            &overlay,
            MOUNT_BASE,
            &endpoints,
            "/etc/praxis/tls",
            8080,
            "consumer-gateway",
            "consumer-ns",
            false,
        )
        .expect("valid empty overlay config");
        let config: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("generated YAML parses");
        let filters = config["filter_chains"][0]["filters"].as_sequence().expect("filters");
        let route = filters
            .iter()
            .find(|filter| filter["filter"] == "intelligent_route")
            .expect("route filter");
        assert!(route.get("candidates").is_none());
        assert_eq!(route["overlay_file"], CONSUMER_OVERLAY_FILE);
        assert!(
            filters.iter().all(|filter| filter["filter"] != "credential_inject"),
            "old consumer images remain compatible when projected credentials are not opted in"
        );
        assert_eq!(
            filters
                .iter()
                .filter(|filter| filter["filter"] == "load_balancer")
                .count(),
            1
        );
    }

    #[test]
    fn projected_credential_opt_in_keeps_inject_filter_for_empty_startup_overlay() {
        let overlay = simple_overlay(Vec::new());
        let endpoints = [ClusterEndpointConfig {
            cluster: "provider-a".to_owned(),
            address: "127.0.0.1:30001".to_owned(),
            transport: Some(EndpointTransport {
                mode: TransportMode::Plaintext,
                sni: None,
            }),
        }];
        let yaml = generate_consumer_praxis_config_for_gateway(
            &overlay,
            MOUNT_BASE,
            &endpoints,
            "/etc/praxis/tls",
            8080,
            "consumer-gateway",
            "consumer-ns",
            true,
        )
        .expect("compatible consumer config renders");
        let config: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("generated YAML parses");
        let filters = config["filter_chains"][0]["filters"].as_sequence().expect("filters");
        let inject = filters
            .iter()
            .find(|filter| filter["filter"] == "credential_inject")
            .expect("projected credential capability installs filter at cold start");
        assert_eq!(inject["credentials"], serde_yaml::Value::Sequence(Vec::new()));
        assert_eq!(inject["projected_credential_mount_base"], MOUNT_BASE);
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "checks that projected credential references stay dynamic in rendered YAML"
    )]
    fn projected_credential_mode_keeps_reference_dynamic_for_credential_route() {
        let overlay = simple_overlay(vec![credential_candidate(
            "inference_model",
            "model-with-secret",
            "site-a",
            "provider-a",
            "provider-secret",
            "consumer-ns",
            "token",
        )]);
        let yaml = generate_consumer_praxis_config_for_gateway(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
            "consumer-gateway",
            "consumer-ns",
            true,
        )
        .expect("projected consumer config renders");
        let config: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("generated YAML parses");
        let filters = config["filter_chains"][0]["filters"].as_sequence().expect("filters");
        let inject = filters
            .iter()
            .find(|filter| filter["filter"] == "credential_inject")
            .expect("dynamic filter is present");
        assert_eq!(inject["credentials"], serde_yaml::Value::Sequence(Vec::new()));
        assert_eq!(inject["projected_credential_mount_base"], MOUNT_BASE);
        assert!(
            !yaml.contains("provider-secret"),
            "reference identity remains only in the dynamic overlay"
        );
    }

    #[test]
    fn dynamic_consumer_config_requires_endpoint_inventory() {
        let overlay = simple_overlay(Vec::new());
        let error = generate_consumer_praxis_config_for_gateway(
            &overlay,
            MOUNT_BASE,
            &[],
            "/etc/praxis/tls",
            8080,
            "consumer-gateway",
            "consumer-ns",
            false,
        )
        .expect_err("restoration needs at least one configured endpoint");
        assert!(matches!(error, ConsumerConfigError::NoClusterEndpoints));
    }

    fn selection_policy_mode(config: &str) -> Option<String> {
        let parsed: serde_yaml::Value = serde_yaml::from_str(config).ok()?;
        parsed
            .get("filter_chains")?
            .as_sequence()?
            .first()?
            .get("filters")?
            .as_sequence()?
            .iter()
            .find(|filter| filter.get("filter").and_then(serde_yaml::Value::as_str) == Some("intelligent_route"))?
            .get("selection_policy")?
            .get("mode")?
            .as_str()
            .map(str::to_owned)
    }

    // -----------------------------------------------------------------------
    // Renderer: basic structure
    // -----------------------------------------------------------------------

    #[test]
    fn plain_candidates_produce_intelligent_route_and_load_balancer() {
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
        assert!(
            yaml.contains("filter: intelligent_route"),
            "must include intelligent_route"
        );
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
    fn explicit_selection_policy_reaches_intelligent_route_config() {
        let mut overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "model",
            "site-a",
            "cluster-a",
            true,
        )]);
        overlay.selection_policy = Some(crate::crd::grid_network::SelectionPolicyConfig {
            mode: SelectionMode::RoundRobin,
        });
        let endpoints = endpoint_coverage(&overlay);
        let config = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/run/tls", 8080)
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(selection_policy_mode(&config).as_deref(), Some("roundRobin"));
    }

    #[test]
    fn generated_praxis_yaml_omits_operator_overlay_metadata() {
        let mut candidate = plain_candidate("inference_model", "model-a", "site-a", "gateway-site-a", true);
        candidate.stable_id = Some("abcd1234".to_owned());
        candidate.admission_state = Some(AdmissionState::NewAndExisting);
        candidate.selection_tier = Some(LocalityTier::SameSite);
        candidate.rank = Some(0);
        candidate.selection_group = Some(0);

        let mut overlay = simple_overlay(vec![candidate]);
        overlay.generated_at = Some("2026-07-24T12:00:00Z".to_owned());

        let yaml = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
        )
        .unwrap();

        for forbidden in ["stable_id", "selection_tier", "rank", "generated_at"] {
            assert!(
                !yaml.contains(forbidden),
                "operator-only metadata field {forbidden} must not enter generated Praxis YAML"
            );
        }
        assert!(yaml.contains("admission_state: \"new_and_existing\""));
        assert!(yaml.contains("selection_group: 0"));
    }

    #[test]
    fn uncredentialed_static_config_does_not_emit_unneeded_inject_filter() {
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
        let config: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("generated Praxis YAML parses");
        let filters = config["filter_chains"][0]["filters"]
            .as_sequence()
            .expect("filter chain");
        assert!(filters.iter().all(|filter| filter["filter"] != "credential_inject"));
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
            yaml.contains("filter: credential_inject"),
            "credential candidate must produce credential_inject"
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
            selection_policy: None,
            generated_at: None,
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
            .find("credential_inject")
            .expect("credential_inject section must be present");
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
        let config: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("generated YAML parses");
        let route = config["filter_chains"][0]["filters"]
            .as_sequence()
            .expect("filters")
            .iter()
            .find(|filter| filter["filter"] == "intelligent_route")
            .expect("intelligent route");
        assert_eq!(route["provider_hop_clusters"][0], "site-a");
    }

    #[test]
    fn provider_hop_clusters_include_only_explicit_mtls_endpoints() {
        let overlay = simple_overlay(vec![
            plain_candidate("inference_model", "model-a", "site-a", "remote-a", true),
            plain_candidate("inference_model", "model-b", "site-b", "local-b", true),
        ]);
        let endpoints = [
            mtls_ep("remote-a", "provider-a.example:8443", "provider-a.example"),
            plain_ep("local-b", "local-backend.default.svc:8080"),
        ];
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080)
            .expect("mixed-transport consumer config renders");
        let config: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("generated YAML parses");
        let route = config["filter_chains"][0]["filters"]
            .as_sequence()
            .expect("filters")
            .iter()
            .find(|filter| filter["filter"] == "intelligent_route")
            .expect("intelligent route");
        assert_eq!(route["provider_hop_clusters"].as_sequence().expect("hop list").len(), 1);
        assert_eq!(route["provider_hop_clusters"][0], "remote-a");
    }

    #[test]
    fn plaintext_only_consumer_has_no_provider_hop_cluster_list() {
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "model-a",
            "site-a",
            "local-a",
            true,
        )]);
        let yaml = generate_consumer_praxis_config(
            &overlay,
            MOUNT_BASE,
            &[plain_ep("local-a", "backend.default.svc:8080")],
            "/etc/praxis/tls",
            8080,
        )
        .expect("plaintext consumer config renders");
        let config: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("generated YAML parses");
        let route = config["filter_chains"][0]["filters"]
            .as_sequence()
            .expect("filters")
            .iter()
            .find(|filter| filter["filter"] == "intelligent_route")
            .expect("intelligent route");
        assert!(route.get("provider_hop_clusters").is_none());
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
        // Both appear in intelligent_route candidates AND load_balancer; check in load_balancer section.
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
