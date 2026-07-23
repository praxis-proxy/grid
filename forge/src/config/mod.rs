//! Configuration model for Forge environments.
//!
//! Defines the YAML configuration shape parsed from `forge.yaml`.  All
//! structs use `#[serde(deny_unknown_fields)]` to reject typos and
//! forward-incompatible additions at parse time.

pub mod schema;
pub mod validate;

use std::{collections::BTreeMap, path::Path};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::error::ForgeError;

/// Expected `apiVersion` value for the current schema generation.
pub const API_VERSION: &str = "forge.praxis.dev/v1alpha1";

/// Expected `kind` value for environment configurations.
pub const KIND: &str = "Environment";

// ---------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------

/// Root configuration object loaded from `forge.yaml`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ForgeConfig {
    /// Schema version identifier.
    pub api_version: String,
    /// Resource kind — must be `"Environment"`.
    pub kind: String,
    /// Resource metadata.
    pub metadata: Metadata,
    /// Environment specification.
    pub spec: EnvironmentSpec,
}

/// Resource metadata.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    /// Unique environment name (DNS-label-like).
    pub name: String,
}

// ---------------------------------------------------------------
// Spec
// ---------------------------------------------------------------

/// Top-level environment specification.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentSpec {
    /// Container runtime settings.
    pub runtime: RuntimeConfig,
    /// Network configuration (reserved for future use).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<NetworkConfig>,
    /// Cluster definitions.
    #[serde(default)]
    pub clusters: Vec<ClusterSpec>,
    /// Host-level service definitions.
    #[serde(default)]
    pub services: Vec<ServiceSpec>,
    /// Certificate authority and site certificate settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificates: Option<CertificateConfig>,
    /// Named deployment stacks.
    #[serde(default)]
    pub stacks: BTreeMap<String, StackSpec>,
}

// ---------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------

/// Container runtime selection.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RuntimeConfig {
    /// Container runtime to use.
    #[serde(default)]
    pub provider: RuntimeProvider,
    /// Prefix for Kind cluster names.
    #[serde(default = "default_cluster_prefix")]
    pub cluster_prefix: String,
}

/// Available container runtime providers.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeProvider {
    /// Auto-detect docker or podman.
    #[default]
    Auto,
    /// Use Docker.
    Docker,
    /// Use Podman.
    Podman,
}

/// Default cluster name prefix.
fn default_cluster_prefix() -> String {
    "forge".to_owned()
}

// ---------------------------------------------------------------
// Networking (placeholder)
// ---------------------------------------------------------------

/// Network configuration (reserved for future phases).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NetworkConfig {
    /// Enable cross-cluster networking.
    #[serde(default)]
    pub cross_cluster: bool,
}

// ---------------------------------------------------------------
// Clusters
// ---------------------------------------------------------------

/// Specification for one Kind cluster.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClusterSpec {
    /// Cluster name (DNS-label-like, unique within the environment).
    pub name: String,
    /// Node layout for this Kind cluster.
    #[serde(default)]
    pub nodes: NodeConfig,
    /// Stacks to apply to this cluster (must exist in `spec.stacks`).
    #[serde(default)]
    pub stacks: Vec<String>,
    /// Arbitrary properties available to stack templates.
    #[serde(default)]
    pub properties: BTreeMap<String, serde_json::Value>,
}

/// Node layout for a Kind cluster.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NodeConfig {
    /// Number of control-plane nodes.
    #[serde(default = "default_control_planes")]
    pub control_planes: u32,
    /// Number of worker nodes.
    #[serde(default = "default_workers")]
    pub workers: u32,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            control_planes: default_control_planes(),
            workers: default_workers(),
        }
    }
}

/// Default control-plane count for a cluster.
fn default_control_planes() -> u32 {
    1
}

/// Default worker count for a cluster.
fn default_workers() -> u32 {
    0
}

// ---------------------------------------------------------------
// Services
// ---------------------------------------------------------------

/// Specification for a host-level container service.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ServiceSpec {
    /// Service name (DNS-label-like, unique within the environment).
    pub name: String,
    /// Container image reference.
    pub image: String,
    /// Host-to-container port mappings.
    #[serde(default)]
    pub ports: Vec<PortMapping>,
    /// Container command arguments.
    #[serde(default)]
    pub args: Vec<String>,
    /// Environment variables for the container.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

/// A host-to-container port mapping.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortMapping {
    /// Host port.
    pub host: u16,
    /// Container port.
    pub container: u16,
    /// Protocol (tcp or udp).
    #[serde(default = "default_protocol")]
    pub protocol: String,
}

/// Default port protocol.
fn default_protocol() -> String {
    "tcp".to_owned()
}

// ---------------------------------------------------------------
// Certificates (placeholder)
// ---------------------------------------------------------------

/// Certificate generation settings (reserved for future phases).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CertificateConfig {
    /// Whether to generate a CA and per-cluster site certificates.
    #[serde(default)]
    pub enabled: bool,
}

// ---------------------------------------------------------------
// Stacks
// ---------------------------------------------------------------

/// A named deployment stack containing an ordered list of steps.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StackSpec {
    /// Human-readable description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Ordered list of deployment steps.
    #[serde(default)]
    pub steps: Vec<StepSpec>,
}

/// A single deployment step within a stack.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum StepSpec {
    /// Apply a remote URL manifest.
    Url {
        /// URL to apply.
        url: String,
        /// Expected SHA-256 digest for the remote content.
        sha256: String,
    },
    /// Apply a local manifest file or directory.
    Manifest {
        /// Path relative to the config root.
        path: String,
    },
    /// Apply a Kustomize overlay.
    Kustomize {
        /// Path to the kustomization directory.
        path: String,
    },
    /// Install or upgrade a Helm release.
    Helm {
        /// Helm release name.
        release: String,
        /// Helm chart reference.
        chart: String,
        /// Helm chart version.
        version: String,
        /// Target namespace.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        namespace: Option<String>,
        /// Helm value overrides.
        #[serde(default)]
        values: BTreeMap<String, serde_json::Value>,
    },
    /// Create a Deployment resource.
    Deployment {
        /// Deployment name.
        name: String,
        /// Container image.
        image: String,
        /// Target namespace.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        namespace: Option<String>,
        /// Container arguments.
        #[serde(default)]
        args: Vec<String>,
    },
    /// Create a Service resource.
    Service {
        /// Service name.
        name: String,
        /// Target port.
        port: u16,
        /// Target namespace.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        namespace: Option<String>,
    },
    /// Wait for a condition.
    Wait {
        /// Resource to wait for (e.g. `deployment/name`).
        resource: String,
        /// Wait condition (e.g. `available`).
        condition: String,
        /// Timeout (e.g. `120s`).  Wait steps must be explicitly bounded.
        timeout: String,
    },
    /// Execute an arbitrary command in the cluster context.
    Exec {
        /// Command to run.
        command: Vec<String>,
    },
    /// Iterate over a cluster property array.
    ForEach {
        /// Cluster property key containing the array.
        property: String,
        /// Steps to apply per element.
        steps: Vec<StepSpec>,
    },
    /// Auto-configure a `MetalLB` IP address pool.
    MetallbAutoPool {
        /// Pool name.
        name: String,
    },
}

// ---------------------------------------------------------------
// Loading
// ---------------------------------------------------------------

/// Load and parse a [`ForgeConfig`] from a YAML file.
///
/// # Errors
///
/// Returns [`ForgeError`] if the file cannot be read or parsed.
pub fn load(path: &Path) -> Result<ForgeConfig, ForgeError> {
    let content = std::fs::read_to_string(path).map_err(|e| ForgeError::Config(format!("{}: {e}", path.display())))?;
    let config: ForgeConfig = serde_yaml::from_str(&content)?;
    Ok(config)
}

/// Generate the minimal YAML content for a new `forge.yaml`.
pub fn minimal_yaml() -> String {
    format!(
        "\
apiVersion: {API_VERSION}
kind: {KIND}

metadata:
  name: minimal

spec:
  runtime:
    provider: auto
    clusterPrefix: forge

  clusters: []

  services: []

  stacks: {{}}
"
    )
}
