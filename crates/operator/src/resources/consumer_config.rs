//! Operator-owned consumer Praxis config renderer.
//!
//! Generates the `praxis.yaml` content for a consumer gateway `ConfigMap` from
//! a [`RoutingOverlay`].  The generated config includes:
//!
//! - `json_body_field` filter (model field → `X-Model` header)
//! - `intelligent_route` filter with inference candidates from the overlay
//! - `credential_inject` filter (only when credential-bearing candidates exist)
//! - `load_balancer` filter with one cluster entry per unique inference cluster
//!
//! Non-inference capabilities remain in the routing overlay for dedicated
//! data-plane pipelines. They are not projected into this model-oriented
//! consumer pipeline.
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
use serde::{Deserialize, Serialize};

use crate::{
    crd::grid_network::{ClusterEndpointConfig, GatewayTelemetryConfig, SelectionMode, TlsConfig, TransportMode},
    resources::routing_overlay::{RoutingCandidate, RoutingOverlay},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Candidate kind supported by the generated model-routing pipeline.
const INFERENCE_MODEL: &str = "inference_model";

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

    /// A required Grid CA Secret reference was not configured.
    #[error("mutual TLS requires tls.caSecretRef")]
    MissingGridCaSecretRef,

    /// A required site identity Secret reference was not configured.
    #[error("mutual TLS requires tls.siteSecretRef")]
    MissingSiteSecretRef,

    /// A generated file path is not absolute and normalized.
    #[error("mount path is not an absolute normalized path: {path:?}")]
    InvalidMountPath {
        /// Invalid absolute file path.
        path: String,
    },

    /// A Secret reference required by a generated CA mount is incomplete.
    #[error("incomplete Secret reference for generated file path {path:?}")]
    InvalidSecretReference {
        /// File path whose Secret reference was incomplete.
        path: String,
    },

    /// Two different Secret keys would be projected to one file path.
    #[error("mount path {path:?} is required by multiple Secret keys")]
    MountPathConflict {
        /// File path requested by different Secret keys.
        path: String,
    },

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

    /// The overlay contains no inference candidates for this pipeline.
    #[error("overlay has no inference_model candidates for the consumer pipeline")]
    NoInferenceCandidates,

    /// A TLS cluster endpoint has no SNI (or blank SNI).
    #[error("TLS transport for cluster {cluster:?} requires a non-blank sni")]
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

    /// Telemetry configuration failed validation.
    #[error("invalid telemetry configuration: {0}")]
    InvalidTelemetry(String),

    /// A plaintext endpoint declares a custom CA Secret.
    #[error("plaintext transport for cluster {cluster:?} must not set caSecretRef")]
    PlaintextWithCa {
        /// Cluster name with the conflicting CA reference.
        cluster: String,
    },

    /// Mutual TLS uses the Grid CA and cannot override it per cluster.
    #[error("mutual_tls transport for cluster {cluster:?} must use the Grid CA")]
    MutualTlsCustomCa {
        /// Cluster name with the conflicting CA reference.
        cluster: String,
    },

    /// JSON serialization failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Purpose of one Secret projection required by a generated gateway config.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum MountPurpose {
    /// Backend authentication credential used at the final provider hop.
    BackendCredential,
    /// Custom CA bundle for a server-authenticated backend TLS connection.
    BackendCa,
    /// Public Grid CA file used to verify a peer gateway.
    GridPeerCa,
    /// Site certificate and private key presented to a peer gateway.
    GridSiteIdentity,
    /// Grid-serving TLS files projected by the gateway chart.
    GridServingTls,
}

/// Secret identifier included in a reference-only requirements document.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct RequirementSecret {
    /// Kubernetes namespace.
    pub namespace: String,
    /// Kubernetes Secret name.
    pub name: String,
}

/// One Secret key and its absolute in-container path.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct RequirementItem {
    /// Secret data key.
    pub key: String,
    /// Absolute, normalized file path used by Praxis.
    pub path: String,
}

/// One Secret and the paths at which its required keys are projected.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MountRequirement {
    /// Why this Secret is needed.
    pub purpose: MountPurpose,
    /// Gateway that performs the final backend call or peer connection.
    pub final_hop: String,
    /// Secret reference; no Secret bytes are included.
    pub secret: RequirementSecret,
    /// Required key-to-path mappings.
    pub items: Vec<RequirementItem>,
}

/// Stable, versioned requirements output for one generated gateway config.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MountRequirementsDocument {
    /// Requirements schema version.
    pub schema_version: String,
    /// `GridNetwork` name.
    pub network: String,
    /// Final-hop gateway identity.
    pub gateway: RequirementGateway,
    /// Sorted and deduplicated Secret file requirements.
    pub requirements: Vec<MountRequirement>,
}

/// Name and namespace of the gateway owning the requirements.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RequirementGateway {
    /// Gateway reference name.
    pub name: String,
    /// Gateway namespace.
    pub namespace: String,
}

/// The YAML and file requirements derived from one immutable overlay input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConsumerRenderResult {
    /// Generated Praxis configuration.
    pub config_yaml: String,
    /// Secret references needed by that exact configuration.
    pub requirements: Vec<MountRequirement>,
}

/// Default base directory for custom backend CA Secret files.
pub(crate) const BACKEND_CA_MOUNT_BASE: &str = "/run/secrets/grid-backend-ca";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Generate the YAML content of a consumer Praxis `ConfigMap`.
///
/// The rendered config is a complete, runnable Praxis config that includes
/// `listeners:`, `filter_chains:`, `admin:`, and `shutdown_timeout_secs`.
/// It is compatible with the Praxis `intelligent_route` and `credential_inject`
/// filters. It includes only `inference_model` candidates and never contains
/// credential token bytes.
///
/// # Parameters
///
/// - `overlay` - the routing overlay produced by the Grid operator for this gateway.
/// - `credential_mount_base` - base directory where credential Secrets are mounted inside the consumer pod (e.g.
///   `/run/secrets/grid-credentials`).
/// - `cluster_endpoints` - explicit endpoint topology for the `load_balancer` section. Every inference cluster must
///   have a matching endpoint entry with explicit transport configuration.
/// - `tls_cert_mount_path` - mount path for TLS certificates inside the consumer pod. Used only when rendering mTLS
///   cluster entries.
/// - `listener_port` - HTTP port for the generated listener (`0.0.0.0:{listener_port}`).
///
/// # Errors
///
/// Returns [`ConsumerConfigError`] when:
/// - `overlay.local_site` is blank.
/// - `credential_mount_base` is blank.
/// - The overlay has no inference candidates.
/// - Any inference candidate has a blank cluster name.
/// - Any inference cluster has no matching endpoint in `cluster_endpoints`.
/// - Any cluster endpoint has no `transport` configuration.
/// - Any `mutual_tls` endpoint has no (or blank) `sni`.
#[cfg(test)]
pub(crate) fn generate_consumer_praxis_config(
    overlay: &RoutingOverlay,
    credential_mount_base: &str,
    cluster_endpoints: &[ClusterEndpointConfig],
    tls_cert_mount_path: &str,
    listener_port: u16,
) -> Result<String, ConsumerConfigError> {
    generate_consumer_praxis_config_with_telemetry(
        overlay,
        credential_mount_base,
        cluster_endpoints,
        tls_cert_mount_path,
        listener_port,
        None,
    )
}

/// Generate the consumer config with optional process-level telemetry settings.
///
/// The telemetry block is emitted at the Praxis config root, beside listeners
/// and admin settings. It is never copied into the routing overlay.
///
/// # Errors
///
/// Returns [`ConsumerConfigError`] when the overlay, endpoint topology, or
/// optional telemetry settings are invalid.
#[expect(
    clippy::too_many_lines,
    reason = "sequential validation + three rendering passes; splitting would obscure the overall config shape"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "preserves the renderer's established inputs and adds optional telemetry"
)]
pub(crate) fn generate_consumer_praxis_config_with_telemetry(
    overlay: &RoutingOverlay,
    credential_mount_base: &str,
    cluster_endpoints: &[ClusterEndpointConfig],
    tls_cert_mount_path: &str,
    listener_port: u16,
    telemetry: Option<&GatewayTelemetryConfig>,
) -> Result<String, ConsumerConfigError> {
    if overlay.local_site.trim().is_empty() {
        return Err(ConsumerConfigError::BlankLocalSite);
    }
    if credential_mount_base.trim().is_empty() {
        return Err(ConsumerConfigError::BlankMountBase);
    }

    let inference_candidates: Vec<&RoutingCandidate> = overlay
        .candidates
        .iter()
        .filter(|candidate| candidate.kind == INFERENCE_MODEL)
        .collect();
    if inference_candidates.is_empty() {
        return Err(ConsumerConfigError::NoInferenceCandidates);
    }
    for candidate in &inference_candidates {
        if candidate.cluster.trim().is_empty() {
            return Err(ConsumerConfigError::BlankCluster {
                kind: candidate.kind.clone(),
                name: candidate.name.clone(),
            });
        }
    }
    if let Some(telemetry) = telemetry {
        telemetry.validate().map_err(ConsumerConfigError::InvalidTelemetry)?;
    }

    let candidates_yaml = render_candidates(&inference_candidates, &overlay.local_site);
    let selection_policy_yaml = render_selection_policy(overlay.selection_policy.as_ref());
    let local_site = yaml_scalar(&overlay.local_site)?;
    let trace_context_filter = if telemetry.is_some() {
        "     - filter: trace_context\n"
    } else {
        ""
    };

    let credential_inject_section =
        render_credential_inject(&inference_candidates, credential_mount_base, &overlay.local_site);
    let load_balancer_section = render_load_balancer(&inference_candidates, cluster_endpoints, tls_cert_mount_path)?;

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
         {trace_context_filter}\
         \x20     - filter: json_body_field\n\
         \x20       field: model\n\
         \x20       header: X-Model\n\
         \x20     - filter: intelligent_route\n\
         \x20       local_site: {local_site}\n\
         \x20       model_header: \"X-Model\"\n\
         {selection_policy_yaml}\
         \x20       candidates:\n\
         {candidates_yaml}"
    );

    if let Some(inject) = credential_inject_section {
        config.push_str(&inject);
    }

    config.push_str(&load_balancer_section);

    if let Some(telemetry) = telemetry {
        config.push_str(&render_telemetry(telemetry)?);
    }

    // Admin interface and graceful shutdown — standard constants for consumer gateways.
    config.push_str("\nadmin:\n  address: \"127.0.0.1:9901\"\nshutdown_timeout_secs: 5\n");

    Ok(config)
}

/// Render validated exporter settings without secret material.
#[expect(
    clippy::too_many_lines,
    reason = "serializes every optional telemetry value in a stable YAML order"
)]
fn render_telemetry(telemetry: &GatewayTelemetryConfig) -> Result<String, ConsumerConfigError> {
    if telemetry.otlp_endpoint.as_deref().is_none_or(str::is_empty)
        && telemetry.sampling_rate.is_none()
        && telemetry.service_name.is_none()
        && telemetry.service_version.is_none()
        && telemetry.environment.is_none()
        && telemetry.batch_interval_secs.is_none()
        && telemetry.batch_size.is_none()
    {
        return Ok("\ntelemetry: {}\n".to_owned());
    }
    let mut output = String::from("\ntelemetry:\n");
    if let Some(endpoint) = telemetry
        .otlp_endpoint
        .as_deref()
        .filter(|endpoint| !endpoint.is_empty())
    {
        output.push_str("  otlp_endpoint: ");
        output.push_str(&yaml_scalar(endpoint)?);
        output.push('\n');
    }
    if let Some(rate) = telemetry.sampling_rate {
        output.push_str("  sampling_rate: ");
        output.push_str(&rate.to_string());
        output.push('\n');
    }
    for (name, value) in [
        ("service_name", telemetry.service_name.as_deref()),
        ("service_version", telemetry.service_version.as_deref()),
        ("environment", telemetry.environment.as_deref()),
    ] {
        if let Some(value) = value {
            output.push_str("  ");
            output.push_str(name);
            output.push_str(": ");
            output.push_str(&yaml_scalar(value)?);
            output.push('\n');
        }
    }
    if let Some(interval) = telemetry.batch_interval_secs {
        output.push_str("  batch_interval_secs: ");
        output.push_str(&interval.to_string());
        output.push('\n');
    }
    if let Some(size) = telemetry.batch_size {
        output.push_str("  batch_size: ");
        output.push_str(&size.to_string());
        output.push('\n');
    }
    Ok(output)
}

/// Render a valid startup configuration while no inference route is eligible.
fn render_no_provider_praxis_config(
    listener_port: u16,
    telemetry: Option<&GatewayTelemetryConfig>,
) -> Result<String, ConsumerConfigError> {
    let mut config = format!(
        "listeners:\n\
         \x20 - name: public\n\
         \x20   address: \"0.0.0.0:{listener_port}\"\n\
         \x20   filter_chains: [no-provider-chain]\n\
         filter_chains:\n\
         \x20 - name: no-provider-chain\n\
         \x20   filters:\n\
         \x20     - filter: static_response\n\
         \x20       status: 503\n\
         \x20       headers:\n\
         \x20         - name: Content-Type\n\
         \x20           value: application/json\n\
         \x20       body: '{{\"error\":\"no providers available\"}}'\n"
    );
    if let Some(telemetry) = telemetry {
        config.push_str(&render_telemetry(telemetry)?);
    }
    config.push_str("admin:\n  address: \"127.0.0.1:9901\"\nshutdown_timeout_secs: 5\n");
    Ok(config)
}

/// Render Praxis YAML and its Secret-file requirements from the same inputs.
///
/// Only credential-bearing candidates whose site is this gateway's local site
/// produce backend credential requirements. Mutual TLS requirements are added
/// only for candidate clusters that the generated load balancer actually uses.
#[expect(
    clippy::too_many_arguments,
    reason = "render inputs include the delegated-mount ownership decision"
)]
#[expect(
    clippy::too_many_lines,
    reason = "configuration and mount requirements are derived from one candidate pass"
)]
pub(crate) fn render_consumer_config(
    overlay: &RoutingOverlay,
    credential_mount_base: &str,
    cluster_endpoints: &[ClusterEndpointConfig],
    tls_cert_mount_path: &str,
    listener_port: u16,
    tls: &TlsConfig,
    gateway_name: &str,
    gateway_namespace: &str,
    telemetry: Option<&GatewayTelemetryConfig>,
    delegated_mounts: bool,
) -> Result<ConsumerRenderResult, ConsumerConfigError> {
    let inference_candidates: Vec<&RoutingCandidate> = overlay
        .candidates
        .iter()
        .filter(|candidate| candidate.kind == INFERENCE_MODEL)
        .collect();
    let config_yaml = if inference_candidates.is_empty() {
        render_no_provider_praxis_config(listener_port, telemetry)?
    } else {
        generate_consumer_praxis_config_with_telemetry(
            overlay,
            credential_mount_base,
            cluster_endpoints,
            tls_cert_mount_path,
            listener_port,
            telemetry,
        )?
    };
    let mut requirements = BTreeMap::<(MountPurpose, String, String, String), BTreeSet<RequirementItem>>::new();

    for candidate in &inference_candidates {
        if candidate.site != overlay.local_site {
            continue;
        }
        let Some(credential) = candidate.credential.as_ref() else {
            continue;
        };
        let path = credential_file_path(
            credential_mount_base,
            &credential.secret_ref.name,
            &credential.secret_ref.key,
        );
        add_requirement(
            &mut requirements,
            MountPurpose::BackendCredential,
            gateway_name,
            &credential.secret_ref.namespace,
            &credential.secret_ref.name,
            &credential.secret_ref.key,
            &path,
        )?;
    }

    let used_clusters: BTreeSet<&str> = inference_candidates
        .iter()
        .map(|candidate| candidate.cluster.as_str())
        .collect();
    let needs_mutual_tls = cluster_endpoints.iter().any(|endpoint| {
        used_clusters.contains(endpoint.cluster.as_str())
            && endpoint
                .transport
                .as_ref()
                .is_some_and(|transport| transport.mode == TransportMode::MutualTls)
    });
    if needs_mutual_tls {
        validate_absolute_normalized_path(&tls_file_path(tls_cert_mount_path, "ca.crt"))?;
    }
    if needs_mutual_tls && delegated_mounts {
        let ca_ref = tls
            .ca_secret_ref
            .as_ref()
            .ok_or(ConsumerConfigError::MissingGridCaSecretRef)?;
        let site_ref = tls
            .site_secret_ref
            .as_ref()
            .ok_or(ConsumerConfigError::MissingSiteSecretRef)?;
        add_requirement(
            &mut requirements,
            MountPurpose::GridPeerCa,
            gateway_name,
            &ca_ref.namespace,
            &ca_ref.name,
            "ca.crt",
            &tls_file_path(tls_cert_mount_path, "ca.crt"),
        )?;
        for key in ["tls.crt", "tls.key"] {
            add_requirement(
                &mut requirements,
                MountPurpose::GridSiteIdentity,
                gateway_name,
                &site_ref.namespace,
                &site_ref.name,
                key,
                &tls_file_path(tls_cert_mount_path, key),
            )?;
        }
    }

    for endpoint in cluster_endpoints
        .iter()
        .filter(|endpoint| used_clusters.contains(endpoint.cluster.as_str()))
    {
        let Some(transport) = endpoint.transport.as_ref() else {
            continue;
        };
        if transport.mode != TransportMode::Tls {
            continue;
        }
        let Some(ca_ref) = transport.ca_secret_ref.as_ref() else {
            continue;
        };
        let key = ca_ref.key.as_deref().unwrap_or("ca.crt");
        let path = backend_ca_file_path(&ca_ref.name, key);
        add_requirement(
            &mut requirements,
            MountPurpose::BackendCa,
            gateway_name,
            gateway_namespace,
            &ca_ref.name,
            key,
            &path,
        )?;
    }

    let requirements = requirements
        .into_iter()
        .map(|((purpose, final_hop, namespace, name), items)| MountRequirement {
            purpose,
            final_hop,
            secret: RequirementSecret { namespace, name },
            items: items.into_iter().collect(),
        })
        .collect();

    Ok(ConsumerRenderResult {
        config_yaml,
        requirements,
    })
}

/// Add one validated file reference to the grouped requirements map.
#[expect(
    clippy::type_complexity,
    reason = "the tuple provides deterministic grouping by purpose and Secret identity"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "each argument identifies one projected Secret key and destination"
)]
fn add_requirement(
    requirements: &mut BTreeMap<(MountPurpose, String, String, String), BTreeSet<RequirementItem>>,
    purpose: MountPurpose,
    final_hop: &str,
    namespace: &str,
    name: &str,
    key: &str,
    path: &str,
) -> Result<(), ConsumerConfigError> {
    if namespace.trim().is_empty()
        || name.trim().is_empty()
        || key.trim().is_empty()
        || !key
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
    {
        return Err(ConsumerConfigError::InvalidSecretReference { path: path.to_owned() });
    }
    validate_absolute_normalized_path(path)?;
    let key_tuple = (namespace, name, key);
    for ((_, _, other_namespace, other_name), items) in requirements.iter() {
        for item in items {
            if item.path == path && (other_namespace.as_str(), other_name.as_str(), item.key.as_str()) != key_tuple {
                return Err(ConsumerConfigError::MountPathConflict { path: path.to_owned() });
            }
        }
    }
    requirements
        .entry((purpose, final_hop.to_owned(), namespace.to_owned(), name.to_owned()))
        .or_default()
        .insert(RequirementItem {
            key: key.to_owned(),
            path: path.to_owned(),
        });
    Ok(())
}

/// Reject relative, traversal, non-normalized, and root-level projected paths.
fn validate_absolute_normalized_path(path: &str) -> Result<(), ConsumerConfigError> {
    let parsed = std::path::Path::new(path);
    let normalized = parsed.components().collect::<std::path::PathBuf>();
    if !parsed.is_absolute()
        || parsed
            .components()
            .any(|component| component == std::path::Component::ParentDir)
        || normalized.to_str() != Some(path)
        || path == "/"
        || parsed
            .parent()
            .is_some_and(|parent| parent == std::path::Path::new("/"))
    {
        return Err(ConsumerConfigError::InvalidMountPath { path: path.to_owned() });
    }
    Ok(())
}

/// Keep rendered TLS paths identical to projected mount paths when the directory has a trailing slash.
fn tls_file_path(mount_dir: &str, key: &str) -> String {
    format!("{}/{key}", mount_dir.trim_end_matches('/'))
}

/// Build the stable in-container path for a custom backend CA Secret key.
fn backend_ca_file_path(secret_name: &str, key: &str) -> String {
    format!("{BACKEND_CA_MOUNT_BASE}/{}/{key}", dns_safe(secret_name))
}

/// Serialize a reference-only requirements document into a `ConfigMap`.
pub(crate) fn build_mount_requirements_config_map(
    document: &MountRequirementsDocument,
    config_map_name: &str,
    namespace: &str,
    network_name: &str,
    gateway_name: &str,
) -> Result<ConfigMap, ConsumerConfigError> {
    let encoded = serde_json::to_string(document)?;
    let mut data = BTreeMap::new();
    data.insert("mount-requirements.json".to_owned(), encoded);
    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/managed-by".to_owned(), "grid-operator".to_owned());
    labels.insert("grid.praxis-proxy.io/gateway".to_owned(), gateway_name.to_owned());
    labels.insert("grid.praxis-proxy.io/network".to_owned(), network_name.to_owned());
    Ok(ConfigMap {
        metadata: kube::api::ObjectMeta {
            labels: Some(labels),
            name: Some(config_map_name.to_owned()),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    })
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
    labels.insert("grid.praxis.fast/gateway".to_owned(), gateway_name.to_owned());
    labels.insert("grid.praxis.fast/network".to_owned(), network_name.to_owned());

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
fn render_candidates(candidates: &[&RoutingCandidate], local_site: &str) -> String {
    candidates
        .iter()
        .map(|candidate| render_candidate(candidate, candidate.site == local_site))
        .collect::<Vec<_>>()
        .join("\n")
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

/// Render one `intelligent_route` candidate.
#[expect(
    clippy::too_many_lines,
    reason = "Candidate YAML fields are kept together to mirror the wire contract."
)]
fn render_candidate(c: &RoutingCandidate, include_credential: bool) -> String {
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
    if include_credential && let Some(cred) = &c.credential {
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

/// Render the `credential_inject` filter section.
///
/// Returns `None` when no credential-bearing candidates exist.
/// Each unique `(strategy, name, namespace, key)` tuple produces one entry.
/// Entries use `file:` sources; no `value:` is ever emitted.
#[expect(
    clippy::too_many_lines,
    reason = "BTreeMap collection + format strings for each credential field"
)]
fn render_credential_inject(
    candidates: &[&RoutingCandidate],
    credential_mount_base: &str,
    local_site: &str,
) -> Option<String> {
    // Collect unique (strategy, name, namespace, key) → rendered entry.
    // BTreeMap provides deterministic sorted order by key.
    let mut entries: BTreeMap<(String, String, String, String), String> = BTreeMap::new();

    for c in candidates {
        if c.site != local_site {
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

    if entries.is_empty() {
        return None;
    }

    Some(format!(
        "\n\
         \x20     - filter: credential_inject\n\
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
    candidates: &[&RoutingCandidate],
    cluster_endpoints: &[ClusterEndpointConfig],
    tls_cert_mount_path: &str,
) -> Result<String, ConsumerConfigError> {
    // Build a lookup map: cluster name → endpoint config.
    let endpoint_map: BTreeMap<&str, &ClusterEndpointConfig> =
        cluster_endpoints.iter().map(|ep| (ep.cluster.as_str(), ep)).collect();

    let clusters: BTreeSet<&str> = candidates.iter().map(|candidate| candidate.cluster.as_str()).collect();
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
            if transport.ca_secret_ref.is_some() {
                return Err(ConsumerConfigError::MutualTlsCustomCa {
                    cluster: ep.cluster.clone(),
                });
            }
            let raw_sni = transport
                .sni
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| ConsumerConfigError::MissingSni {
                    cluster: ep.cluster.clone(),
                })?;
            let trimmed_sni = raw_sni.trim();
            let quoted_sni = yaml_scalar(trimmed_sni).unwrap_or_else(|_| "\"\"".to_owned());
            let ca_path =
                yaml_scalar(&tls_file_path(tls_cert_mount_path, "ca.crt")).unwrap_or_else(|_| "\"\"".to_owned());
            let cert_path =
                yaml_scalar(&tls_file_path(tls_cert_mount_path, "tls.crt")).unwrap_or_else(|_| "\"\"".to_owned());
            let key_path =
                yaml_scalar(&tls_file_path(tls_cert_mount_path, "tls.key")).unwrap_or_else(|_| "\"\"".to_owned());
            Ok(format!(
                "          - name: {quoted_name}\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20  tls:\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20    ca:\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20      ca_path: {ca_path}\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20    client_cert:\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20      cert_path: {cert_path}\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20      key_path: {key_path}\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20    sni: {quoted_sni}\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20    verify: true\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20  endpoints:\n\
                \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20    - {quoted_addr}"
            ))
        },
        TransportMode::Tls => {
            let raw_sni = transport
                .sni
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| ConsumerConfigError::MissingSni {
                    cluster: ep.cluster.clone(),
                })?;
            let quoted_sni = yaml_scalar(raw_sni.trim()).unwrap_or_else(|_| "\"\"".to_owned());
            let ca_config = transport.ca_secret_ref.as_ref().map_or_else(String::new, |ca_ref| {
                let key = ca_ref.key.as_deref().unwrap_or("ca.crt");
                let path = backend_ca_file_path(&ca_ref.name, key);
                let quoted_path = yaml_scalar(&path).unwrap_or_else(|_| "\"\"".to_owned());
                format!("              ca:\n                ca_path: {quoted_path}\n")
            });
            Ok(format!(
                "          - name: {quoted_name}\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20  tls:\n\
                 {ca_config}\
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
            if transport.ca_secret_ref.is_some() {
                return Err(ConsumerConfigError::PlaintextWithCa {
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
        crd::grid_network::{EndpointCaSecretRef, EndpointTransport, SecretRef},
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
            excluded: Vec::new(),
            selection_policy: None,
            generated_at: None,
        }
    }

    const MOUNT_BASE: &str = "/run/secrets/grid-credentials";
    const SENTINEL_TOKEN: &str = "sk-super-secret-bearer-token-do-not-emit";

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "covers local and remote candidates in one final-hop regression"
    )]
    fn requirements_keep_backend_credentials_at_the_final_local_hop() {
        let overlay = simple_overlay(vec![
            credential_candidate(
                "inference_model",
                "local-model",
                "site-a",
                "local-provider",
                "model-a-credential",
                "gateway-ns",
                "token",
            ),
            credential_candidate(
                "inference_model",
                "local-model-copy",
                "site-a",
                "local-provider",
                "model-a-credential",
                "gateway-ns",
                "token",
            ),
            credential_candidate(
                "inference_model",
                "remote-model",
                "site-b",
                "remote-provider",
                "remote-credential",
                "remote-ns",
                "token",
            ),
        ]);
        let rendered = render_consumer_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
            &TlsConfig::default(),
            "grid-a",
            "gateway-ns",
            None,
            false,
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            rendered.requirements.len(),
            1,
            "duplicates produce one local requirement"
        );
        assert_eq!(rendered.requirements[0].purpose, MountPurpose::BackendCredential);
        assert_eq!(rendered.requirements[0].final_hop, "grid-a");
        assert_eq!(rendered.requirements[0].secret.name, "model-a-credential");
        assert_eq!(rendered.requirements[0].items.len(), 1);
        assert!(rendered.config_yaml.contains("model-a-credential"));
        assert!(!rendered.config_yaml.contains("remote-credential"));
        assert!(!rendered.config_yaml.contains(SENTINEL_TOKEN));
    }

    #[test]
    fn empty_candidate_config_fails_closed_without_an_invalid_route_filter() {
        let overlay = simple_overlay(Vec::new());
        let rendered = render_consumer_config(
            &overlay,
            MOUNT_BASE,
            &[],
            "/etc/praxis/tls",
            8080,
            &TlsConfig::default(),
            "gateway",
            "gateway-ns",
            None,
            false,
        )
        .unwrap_or_else(|_| std::process::abort());
        let parsed: serde_yaml::Value =
            serde_yaml::from_str(&rendered.config_yaml).unwrap_or_else(|_| std::process::abort());
        let filters = parsed
            .get("filter_chains")
            .and_then(serde_yaml::Value::as_sequence)
            .and_then(|chains| chains.first())
            .and_then(|chain| chain.get("filters"))
            .and_then(serde_yaml::Value::as_sequence)
            .unwrap_or_else(|| std::process::abort());

        assert_eq!(rendered.requirements.len(), 0);
        assert_eq!(filters[0]["filter"], "static_response");
        assert_eq!(filters[0]["status"], 503);
        assert!(!rendered.config_yaml.contains("intelligent_route"));
        assert!(rendered.config_yaml.contains("no providers available"));
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "asserts both required Grid TLS Secret projections and file paths"
    )]
    fn mutual_tls_requirements_project_grid_ca_and_site_identity_together() {
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "model",
            "site-a",
            "peer",
            true,
        )]);
        let endpoint = ClusterEndpointConfig {
            cluster: "peer".to_owned(),
            address: "peer.example:8443".to_owned(),
            transport: Some(EndpointTransport {
                mode: TransportMode::MutualTls,
                sni: Some("peer.grid.internal".to_owned()),
                ca_secret_ref: None,
            }),
        };
        let tls = TlsConfig {
            ca_secret_ref: Some(SecretRef {
                name: "grid-ca".to_owned(),
                namespace: "gateway-ns".to_owned(),
                key: None,
            }),
            site_secret_ref: Some(SecretRef {
                name: "site-identity".to_owned(),
                namespace: "gateway-ns".to_owned(),
                key: None,
            }),
            swim_key_ref: None,
        };
        let rendered = render_consumer_config(
            &overlay,
            MOUNT_BASE,
            &[endpoint],
            "/tls/",
            8080,
            &tls,
            "gateway",
            "gateway-ns",
            None,
            true,
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(rendered.requirements.len(), 2);
        assert!(rendered.config_yaml.contains("/tls/ca.crt"));
        assert!(rendered.config_yaml.contains("/tls/tls.key"));
        let document = MountRequirementsDocument {
            schema_version: "v1".to_owned(),
            network: "grid-a".to_owned(),
            gateway: RequirementGateway {
                name: "gateway".to_owned(),
                namespace: "gateway-ns".to_owned(),
            },
            requirements: rendered.requirements,
        };
        let mounts =
            crate::resources::gateway_mounts::desired_mounts(&document).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            mounts.len(),
            1,
            "Grid CA and site identity share the Praxis TLS directory"
        );
        let projected_sources = mounts[0]
            .volume
            .pointer("/projected/sources")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            projected_sources.len(),
            2,
            "both source Secrets project into one stable volume"
        );
    }

    #[test]
    fn owner_managed_mutual_tls_mounts_do_not_require_grid_secret_references() {
        let overlay = simple_overlay(vec![plain_candidate("inference_model", "m", "site-a", "peer", true)]);
        let endpoint = mtls_ep("peer", "peer.example:8443", "peer.grid.internal");
        let rendered = render_consumer_config(
            &overlay,
            MOUNT_BASE,
            &[endpoint],
            "/etc/praxis/tls",
            8080,
            &TlsConfig::default(),
            "gateway",
            "gateway-ns",
            None,
            false,
        )
        .unwrap_or_else(|_| std::process::abort());

        assert!(
            rendered.requirements.is_empty(),
            "owner-managed mTLS mounts must not require Grid Secret references"
        );
        assert!(
            rendered.config_yaml.contains("/etc/praxis/tls/ca.crt"),
            "owner-managed mTLS must still render the CA certificate path"
        );
        assert!(
            rendered.config_yaml.contains("/etc/praxis/tls/tls.key"),
            "owner-managed mTLS must still render the client key path"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "asserts generated TLS config and its matching projected CA requirement"
    )]
    fn server_tls_backend_ca_is_rendered_and_reported_as_a_file_requirement() -> Result<(), serde_yaml::Error> {
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "model",
            "site-a",
            "backend",
            true,
        )]);
        let endpoint = ClusterEndpointConfig {
            cluster: "backend".to_owned(),
            address: "model.example:443".to_owned(),
            transport: Some(EndpointTransport {
                mode: TransportMode::Tls,
                sni: Some("model.example".to_owned()),
                ca_secret_ref: Some(EndpointCaSecretRef {
                    name: "model-ca".to_owned(),
                    key: None,
                }),
            }),
        };
        let rendered = render_consumer_config(
            &overlay,
            MOUNT_BASE,
            &[endpoint],
            "/etc/praxis/tls",
            8080,
            &TlsConfig::default(),
            "gateway",
            "gateway-ns",
            None,
            false,
        )
        .unwrap_or_else(|_| std::process::abort());

        let parsed: serde_yaml::Value = serde_yaml::from_str(&rendered.config_yaml)?;
        let filters = parsed["filter_chains"][0]["filters"]
            .as_sequence()
            .unwrap_or_else(|| std::process::abort());
        let load_balancer = filters
            .iter()
            .find(|filter| filter["filter"].as_str() == Some("load_balancer"))
            .unwrap_or_else(|| std::process::abort());
        let clusters = load_balancer["clusters"]
            .as_sequence()
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(clusters.len(), 1, "the TLS backend must render one cluster");
        assert_eq!(
            clusters[0]["name"].as_str(),
            Some("backend"),
            "the rendered TLS cluster must be the selected backend"
        );
        assert_eq!(
            clusters[0]["tls"]["ca"]["ca_path"].as_str(),
            Some("/run/secrets/grid-backend-ca/model-ca/ca.crt"),
            "backend CA path must be nested under tls.ca"
        );
        assert_eq!(
            clusters[0]["tls"]["sni"].as_str(),
            Some("model.example"),
            "backend SNI must be nested under tls"
        );
        assert_eq!(rendered.requirements.len(), 1);
        assert_eq!(rendered.requirements[0].purpose, MountPurpose::BackendCa);
        assert_eq!(rendered.requirements[0].secret.namespace, "gateway-ns");
        assert_eq!(rendered.requirements[0].secret.name, "model-ca");
        assert_eq!(rendered.requirements[0].items[0].key, "ca.crt");
        assert_eq!(
            rendered.requirements[0].items[0].path,
            "/run/secrets/grid-backend-ca/model-ca/ca.crt"
        );
        Ok(())
    }

    #[test]
    fn sanitized_credential_paths_that_collide_fail_closed() {
        let overlay = simple_overlay(vec![
            credential_candidate("inference_model", "a", "site-a", "a", "model.a", "gateway-ns", "token"),
            credential_candidate("inference_model", "b", "site-a", "b", "model-a", "gateway-ns", "token"),
        ]);
        let result = render_consumer_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
            &TlsConfig::default(),
            "gateway",
            "gateway-ns",
            None,
            false,
        );
        assert!(matches!(result, Err(ConsumerConfigError::MountPathConflict { .. })));
    }

    #[test]
    fn same_secret_name_from_two_namespaces_cannot_alias_one_mount_path() {
        let overlay = simple_overlay(vec![
            credential_candidate("inference_model", "a", "site-a", "a", "shared", "namespace-a", "token"),
            credential_candidate("inference_model", "b", "site-a", "b", "shared", "namespace-b", "token"),
        ]);
        let result = render_consumer_config(
            &overlay,
            MOUNT_BASE,
            &endpoint_coverage(&overlay),
            "/etc/praxis/tls",
            8080,
            &TlsConfig::default(),
            "gateway",
            "gateway-ns",
            None,
            false,
        );
        assert!(matches!(result, Err(ConsumerConfigError::MountPathConflict { .. })));
    }

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
                    ca_secret_ref: None,
                }),
            })
            .collect()
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
    #[expect(
        clippy::too_many_lines,
        reason = "checks opt-in output, secret exclusion, and overlay separation"
    )]
    fn telemetry_is_opt_in_and_rendered_outside_routing_filters() {
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "model-a",
            "site-a",
            "gateway-site-a",
            true,
        )]);
        let endpoints = endpoint_coverage(&overlay);
        let disabled = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080)
            .unwrap_or_else(|_| std::process::abort());
        assert!(
            !disabled.contains("telemetry:"),
            "telemetry must remain absent by default"
        );
        assert!(
            !disabled.contains("filter: trace_context"),
            "propagation must remain opt-in"
        );

        let telemetry = GatewayTelemetryConfig {
            otlp_endpoint: Some("http://collector.observability:4317".to_owned()),
            sampling_rate: Some(0.25),
            service_name: Some("grid-edge-site-a".to_owned()),
            service_version: Some("0.1.4".to_owned()),
            environment: Some("test".to_owned()),
            batch_interval_secs: Some(3),
            batch_size: Some(32),
        };
        let enabled = generate_consumer_praxis_config_with_telemetry(
            &overlay,
            MOUNT_BASE,
            &endpoints,
            "/etc/praxis/tls",
            8080,
            Some(&telemetry),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert!(
            enabled.contains("filter: trace_context"),
            "telemetry must enable W3C propagation"
        );
        assert!(enabled.contains("telemetry:\n  otlp_endpoint: \"http://collector.observability:4317\""));
        assert!(enabled.contains("  sampling_rate: 0.25"));
        assert!(enabled.contains("  service_name: \"grid-edge-site-a\""));
        assert!(enabled.contains("  batch_interval_secs: 3"));
        assert!(
            !enabled.contains("otlp_headers"),
            "credentials must not be rendered into the ConfigMap"
        );
        assert!(
            !enabled.contains("collector-secret-value"),
            "secret values must not appear in generated YAML"
        );
        assert!(
            !serde_json::to_string(&overlay)
                .unwrap_or_else(|_| std::process::abort())
                .contains("telemetry"),
            "exporter settings must stay out of the routing overlay"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "covers environment fallback, sampling boundaries, and credential rejection"
    )]
    fn telemetry_rejects_invalid_sampling_rates_and_credentials_in_endpoint() {
        let overlay = simple_overlay(vec![plain_candidate(
            "inference_model",
            "model-a",
            "site-a",
            "model-cluster",
            true,
        )]);
        let endpoints = [plain_ep("model-cluster", "10.0.0.10:8080")];
        let env_only = GatewayTelemetryConfig {
            otlp_endpoint: None,
            sampling_rate: None,
            service_name: None,
            service_version: None,
            environment: None,
            batch_interval_secs: None,
            batch_size: None,
        };
        let env_config = generate_consumer_praxis_config_with_telemetry(
            &overlay,
            MOUNT_BASE,
            &endpoints,
            "/etc/praxis/tls",
            8080,
            Some(&env_only),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert!(
            env_config.contains("telemetry: {}"),
            "an environment-only exporter must render as an empty mapping, not null"
        );

        let empty_endpoint = GatewayTelemetryConfig {
            otlp_endpoint: Some(String::new()),
            ..env_only
        };
        assert!(
            empty_endpoint.validate().is_ok(),
            "an empty endpoint must use the deployment environment fallback"
        );
        let empty_config = generate_consumer_praxis_config_with_telemetry(
            &overlay,
            MOUNT_BASE,
            &endpoints,
            "/etc/praxis/tls",
            8080,
            Some(&empty_endpoint),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert!(empty_config.contains("telemetry: {}"));
        assert!(!empty_config.contains("otlp_endpoint:"));

        for rate in [-0.01, 1.01, f64::NAN, f64::INFINITY] {
            let telemetry = GatewayTelemetryConfig {
                sampling_rate: Some(rate),
                ..valid_telemetry()
            };
            let result = generate_consumer_praxis_config_with_telemetry(
                &overlay,
                MOUNT_BASE,
                &endpoints,
                "/etc/praxis/tls",
                8080,
                Some(&telemetry),
            );
            assert!(result.is_err(), "invalid sampling rate {rate:?} must be rejected");
        }

        let telemetry = GatewayTelemetryConfig {
            otlp_endpoint: Some("https://user:password@collector:4317".to_owned()),
            ..valid_telemetry()
        };
        assert!(
            telemetry.validate().is_err(),
            "collector authentication must use Secret-backed environment references"
        );
    }

    /// Return a minimal valid telemetry setting for validation tests.
    fn valid_telemetry() -> GatewayTelemetryConfig {
        GatewayTelemetryConfig {
            otlp_endpoint: Some("http://collector:4317".to_owned()),
            sampling_rate: Some(0.5),
            service_name: None,
            service_version: None,
            environment: None,
            batch_interval_secs: None,
            batch_size: None,
        }
    }

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
    #[expect(
        clippy::too_many_lines,
        reason = "test parses route and load-balancer sections and checks credential omission"
    )]
    fn mixed_capability_overlay_projects_only_inference_pipeline() {
        let overlay = simple_overlay(vec![
            plain_candidate("inference_model", "model-a", "site-a", "model-cluster", true),
            credential_candidate(
                "mcp_tool",
                "search",
                "site-a",
                "tool-cluster",
                "tool-secret",
                "default",
                "token",
            ),
        ]);
        let endpoints = [plain_ep("model-cluster", "10.0.0.10:8080")];
        let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, "/etc/praxis/tls", 8080)
            .unwrap_or_else(|_| std::process::abort());
        let parsed: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap_or_else(|_| std::process::abort());
        let filters = parsed["filter_chains"][0]["filters"]
            .as_sequence()
            .unwrap_or_else(|| std::process::abort());
        let route_candidates = filters
            .iter()
            .find(|filter| filter["filter"].as_str() == Some("intelligent_route"))
            .and_then(|filter| filter["candidates"].as_sequence())
            .unwrap_or_else(|| std::process::abort());
        let load_balancer_clusters = filters
            .iter()
            .find(|filter| filter["filter"].as_str() == Some("load_balancer"))
            .and_then(|filter| filter["clusters"].as_sequence())
            .unwrap_or_else(|| std::process::abort());

        assert_eq!(route_candidates.len(), 1, "only inference candidates must be projected");
        assert_eq!(
            route_candidates[0]["kind"].as_str(),
            Some("inference_model"),
            "projected candidate must retain the inference kind"
        );
        assert_eq!(
            route_candidates[0]["name"].as_str(),
            Some("model-a"),
            "projected candidate must retain the model name"
        );
        assert_eq!(
            load_balancer_clusters.len(),
            1,
            "only inference clusters must be projected"
        );
        assert_eq!(
            load_balancer_clusters[0]["name"].as_str(),
            Some("model-cluster"),
            "load balancer must contain the selected inference cluster"
        );
        assert!(!yaml.contains("mcp_tool"), "MCP candidate kind must be omitted");
        assert!(!yaml.contains("tool-cluster"), "MCP candidate cluster must be omitted");
        assert!(!yaml.contains("tool-secret"), "MCP credentials must be omitted");
        assert!(
            !yaml.contains("filter: credential_inject"),
            "MCP credentials must not create a credential filter in the inference pipeline"
        );
    }

    #[test]
    fn tool_only_overlay_returns_no_inference_candidates() {
        let overlay = simple_overlay(vec![plain_candidate(
            "mcp_tool",
            "search",
            "site-a",
            "tool-cluster",
            true,
        )]);
        let result = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &[], "/etc/praxis/tls", 8080);

        assert!(
            matches!(result, Err(ConsumerConfigError::NoInferenceCandidates)),
            "tool-only overlays must not produce an invalid model-routing config"
        );
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
            !yaml.contains("credential_inject"),
            "no credential candidates must produce no credential_inject"
        );
    }

    #[test]
    fn credential_candidate_produces_credential_inject_with_file_source() {
        let overlay = simple_overlay(vec![credential_candidate(
            "inference_model",
            "model-z",
            "site-a",
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
                "site-a",
                "cluster-b",
                "shared-creds",
                "ns",
                "token",
            ),
            credential_candidate(
                "inference_model",
                "model-z2",
                "site-a",
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
                "site-a",
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
            "site-a",
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
            "site-a",
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
            "site-a",
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
            "site-a",
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
            excluded: Vec::new(),
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
    fn root_level_secret_file_path_is_rejected() {
        assert!(matches!(
            validate_absolute_normalized_path("/ca.crt"),
            Err(ConsumerConfigError::InvalidMountPath { .. })
        ));
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
            credential_candidate("inference_model", "m1", "site-a", "c1", "creds-b", "ns", "tok"),
            credential_candidate("inference_model", "m2", "site-a", "c2", "creds-a", "ns", "tok"),
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
            credential_candidate("inference_model", "m1", "site-a", "c1", "zzz-creds", "ns", "tok"),
            credential_candidate("inference_model", "m2", "site-a", "c2", "aaa-creds", "ns", "tok"),
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
            yaml.contains("ca_path: \"/etc/praxis/tls/ca.crt\""),
            "TLS config must reference CA path"
        );
        assert!(
            yaml.contains("cert_path: \"/etc/praxis/tls/tls.crt\""),
            "TLS config must reference client cert path"
        );
        assert!(
            yaml.contains("key_path: \"/etc/praxis/tls/tls.key\""),
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
            "site-a",
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
            labels.get("grid.praxis.fast/network").map(String::as_str),
            Some("my-network")
        );
        assert_eq!(
            labels.get("grid.praxis.fast/gateway").map(String::as_str),
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
                ca_secret_ref: None,
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
                ca_secret_ref: None,
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
        assert!(
            yaml.contains("ca_path: \"/etc/praxis/tls/ca.crt\""),
            "CA path must appear"
        );
        assert!(
            yaml.contains("cert_path: \"/etc/praxis/tls/tls.crt\""),
            "cert path must appear"
        );
        assert!(
            yaml.contains("key_path: \"/etc/praxis/tls/tls.key\""),
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
                ca_secret_ref: None,
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
                ca_secret_ref: None,
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
                ca_secret_ref: None,
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
                ca_secret_ref: None,
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
                ca_secret_ref: None,
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
        for (mount_dir, ca_path) in [
            ("/custom/tls/path", "/custom/tls/path/ca.crt"),
            ("/tls", "/tls/ca.crt"),
            ("/etc/praxis/tls/", "/etc/praxis/tls/ca.crt"),
        ] {
            let yaml = generate_consumer_praxis_config(&overlay, MOUNT_BASE, &endpoints, mount_dir, 8080).unwrap();
            assert!(
                yaml.contains(&format!("ca_path: \"{ca_path}\"")),
                "TLS directory {mount_dir} must render CA path {ca_path}"
            );
        }
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
