//! Environment configuration parsed from TOML.

use std::{collections::BTreeMap, path::Path};

use serde::Deserialize;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors that can occur when loading environment configuration.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ConfigError {
    /// Failed to read the configuration file.
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),

    /// Failed to parse the configuration file.
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),

    /// A cluster referenced in `names` has no corresponding definition.
    #[error("cluster '{0}' listed in names but not defined")]
    MissingCluster(String),
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Top-level environment configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EnvConfig {
    /// Cluster definitions.
    pub clusters: ClusterConfig,

    /// External provider mock definitions.
    pub providers: ProviderConfig,
}

/// Cluster configuration block.
///
/// Note: `deny_unknown_fields` is omitted because `#[serde(flatten)]`
/// treats dynamic map keys as unknown. Cross-field validation is
/// handled by [`EnvConfig::validate`].
#[derive(Debug, Deserialize)]
pub(crate) struct ClusterConfig {
    /// Ordered list of cluster names to create.
    pub names: Vec<String>,

    /// Per-cluster definitions keyed by name.
    #[serde(flatten)]
    pub definitions: BTreeMap<String, ClusterDef>,
}

/// Definition of a single kind cluster.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClusterDef {
    /// Models served by this cluster's provider backend.
    pub models: Vec<String>,

    /// Cluster role in the grid.
    pub role: ClusterRole,

    /// Provider backend deployed inside this cluster.
    ///
    /// Determines which inference backend is deployed when `role = "provider"`.
    /// Defaults to [`ProviderBackend::InferenceSim`] (llm-d-inference-sim).
    #[serde(default)]
    pub backend: ProviderBackend,
}

/// Provider backend deployed inside a provider cluster.
///
/// Controls which inference backend service receives requests routed
/// by the mock EPP.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ProviderBackend {
    /// Deploy `llm-d-inference-sim` per model.  Default.
    ///
    /// One Deployment and Service per model.  Routes via
    /// `inference-sim-{model}.{namespace}.svc:8000`.
    #[default]
    InferenceSim,

    /// Deploy `grid-mock-providers` with the `openai` provider.
    ///
    /// A single Deployment and Service serves all models in the cluster.
    /// Routes all models via `mock-openai-provider.{namespace}.svc:8080`.
    /// Supports Chat Completions and `/v1/responses`.
    MockOpenai,
}

/// Role of a cluster in the test topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ClusterRole {
    /// Provides inference backends.
    Provider,

    /// Consumes inference from other sites.
    Consumer,

    /// Both consumes inference and provides local inference backends.
    Both,
}

impl ClusterRole {
    /// Whether this role can host provider backends.
    pub(crate) fn is_provider(self) -> bool {
        matches!(self, Self::Provider | Self::Both)
    }

    /// Whether this role can host a consumer gateway.
    pub(crate) fn is_consumer(self) -> bool {
        matches!(self, Self::Consumer | Self::Both)
    }
}

/// External mock provider configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProviderConfig {
    /// `OpenAI` mock server settings.
    pub openai: ProviderDef,

    /// `Anthropic` mock server settings.
    pub anthropic: ProviderDef,

    /// AWS `Bedrock` mock server settings.
    pub bedrock: BedrockDef,

    /// Google `Vertex` AI mock server settings.
    pub vertex: VertexDef,
}

/// Basic provider definition with a port.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProviderDef {
    /// Port to listen on.
    pub port: u16,
}

/// Bedrock provider definition.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BedrockDef {
    /// Port to listen on.
    pub port: u16,

    /// AWS region to simulate.
    pub region: String,
}

/// Vertex AI provider definition.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VertexDef {
    /// Port to listen on.
    pub port: u16,

    /// GCP project ID to simulate.
    pub project: String,
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

impl EnvConfig {
    /// Load configuration from a TOML file at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if the file cannot be read, parsed,
    /// or contains a cluster name without a corresponding definition.
    pub(crate) fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        Self::from_str(&content)
    }

    /// Parse configuration from a TOML string.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if the string cannot be parsed or
    /// contains a cluster name without a corresponding definition.
    pub(crate) fn from_str(s: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(s)?;
        config.validate()?;
        Ok(config)
    }

    /// Return the name of the first cluster with [`ClusterRole::Consumer`] role.
    ///
    /// Returns `None` when no consumer cluster is configured.  Call sites
    /// that require a consumer cluster should convert `None` to an error
    /// with their own diagnostic message via `.ok_or(...)`.
    pub(crate) fn consumer_cluster_name(&self) -> Option<&str> {
        self.clusters
            .names
            .iter()
            .find(|name| {
                self.clusters
                    .definitions
                    .get(*name)
                    .is_some_and(|d| d.role.is_consumer())
            })
            .map(String::as_str)
    }

    /// Validate cross-field constraints.
    fn validate(&self) -> Result<(), ConfigError> {
        for name in &self.clusters.names {
            if !self.clusters.definitions.contains_key(name) {
                return Err(ConfigError::MissingCluster(name.clone()));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_CONFIG: &str = r#"
[clusters]
names = ["cluster-a", "cluster-b", "cluster-c"]

[clusters.cluster-a]
models = ["granite-3.3-8b", "mistral-7b"]
role = "provider"

[clusters.cluster-b]
models = ["llama-3.2-8b"]
role = "provider"

[clusters.cluster-c]
models = []
role = "consumer"

[providers]
openai = { port = 10001 }
anthropic = { port = 10002 }
bedrock = { port = 10003, region = "us-east-1" }
vertex = { port = 10004, project = "test-project" }
"#;

    #[test]
    fn parse_valid_config() {
        let config = EnvConfig::from_str(VALID_CONFIG);
        assert!(config.is_ok(), "expected valid config: {config:?}");
        let config = config.ok();
        assert!(config.is_some(), "config should be Some");
        let config = config.as_ref();
        assert!(config.is_some(), "config ref should be Some");

        let cfg = config.unwrap_or_else(|| std::process::abort());
        assert_eq!(cfg.clusters.names.len(), 3, "expected 3 cluster names");
        assert_eq!(cfg.providers.openai.port, 10_001, "openai port mismatch");
        assert_eq!(cfg.providers.bedrock.region, "us-east-1", "bedrock region");
        assert_eq!(cfg.providers.vertex.project, "test-project", "vertex project");
    }

    #[test]
    fn parse_cluster_role() {
        let config = EnvConfig::from_str(VALID_CONFIG);
        assert!(config.is_ok(), "expected valid config");
        let cfg = config.unwrap_or_else(|_| std::process::abort());

        let a = cfg.clusters.definitions.get("cluster-a");
        assert!(a.is_some(), "cluster-a should exist");
        assert_eq!(
            a.unwrap_or_else(|| std::process::abort()).role,
            ClusterRole::Provider,
            "cluster-a should be provider"
        );

        let c = cfg.clusters.definitions.get("cluster-c");
        assert!(c.is_some(), "cluster-c should exist");
        assert_eq!(
            c.unwrap_or_else(|| std::process::abort()).role,
            ClusterRole::Consumer,
            "cluster-c should be consumer"
        );
    }

    #[test]
    fn parse_both_cluster_role() {
        let toml = r#"
[clusters]
names = ["edge"]

[clusters.edge]
models = ["model-local"]
role = "both"

[providers]
openai = { port = 10001 }
anthropic = { port = 10002 }
bedrock = { port = 10003, region = "us-east-1" }
vertex = { port = 10004, project = "test-project" }
"#;
        let cfg = EnvConfig::from_str(toml).unwrap_or_else(|_| std::process::abort());
        let edge = cfg
            .clusters
            .definitions
            .get("edge")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(edge.role, ClusterRole::Both, "role must parse as both");
        assert!(edge.role.is_consumer(), "both must be consumer-capable");
        assert!(edge.role.is_provider(), "both must be provider-capable");
    }

    #[test]
    fn missing_cluster_definition() {
        let bad = r#"
[clusters]
names = ["cluster-a", "cluster-missing"]

[clusters.cluster-a]
models = []
role = "consumer"

[providers]
openai = { port = 10001 }
anthropic = { port = 10002 }
bedrock = { port = 10003, region = "us-east-1" }
vertex = { port = 10004, project = "test-project" }
"#;
        let result = EnvConfig::from_str(bad);
        assert!(result.is_err(), "should reject missing cluster def");
        let msg = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(
            msg.contains("cluster-missing"),
            "error should name the missing cluster: {msg}"
        );
    }

    #[test]
    fn unknown_field_rejected() {
        let bad = r#"
[clusters]
names = ["cluster-a"]
bogus = true

[clusters.cluster-a]
models = []
role = "consumer"

[providers]
openai = { port = 10001 }
anthropic = { port = 10002 }
bedrock = { port = 10003, region = "us-east-1" }
vertex = { port = 10004, project = "test-project" }
"#;
        let result = EnvConfig::from_str(bad);
        assert!(result.is_err(), "should reject unknown fields");
    }

    #[test]
    fn invalid_role_rejected() {
        let bad = r#"
[clusters]
names = ["cluster-a"]

[clusters.cluster-a]
models = []
role = "invalid"

[providers]
openai = { port = 10001 }
anthropic = { port = 10002 }
bedrock = { port = 10003, region = "us-east-1" }
vertex = { port = 10004, project = "test-project" }
"#;
        let result = EnvConfig::from_str(bad);
        assert!(result.is_err(), "should reject invalid role");
    }

    #[test]
    fn missing_providers_rejected() {
        let bad = r#"
[clusters]
names = ["cluster-a"]

[clusters.cluster-a]
models = []
role = "consumer"
"#;
        let result = EnvConfig::from_str(bad);
        assert!(result.is_err(), "should reject missing providers section");
    }

    #[test]
    fn file_not_found() {
        let result = EnvConfig::from_file(Path::new("/nonexistent/config.toml"));
        assert!(result.is_err(), "should fail on missing file");
    }

    #[test]
    fn default_backend_is_inference_sim() {
        let toml = r#"
[clusters]
names = ["provider"]

[clusters.provider]
models = ["model-x"]
role = "provider"

[providers]
openai = { port = 10001 }
anthropic = { port = 10002 }
bedrock = { port = 10003, region = "us-east-1" }
vertex = { port = 10004, project = "test-project" }
"#;
        let cfg = EnvConfig::from_str(toml).unwrap_or_else(|_| std::process::abort());
        let def = cfg
            .clusters
            .definitions
            .get("provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            def.backend,
            ProviderBackend::InferenceSim,
            "default backend must be InferenceSim when not specified"
        );
    }

    #[test]
    fn explicit_inference_sim_backend_parses() {
        let toml = r#"
[clusters]
names = ["provider"]

[clusters.provider]
models = ["model-x"]
role = "provider"
backend = "inference-sim"

[providers]
openai = { port = 10001 }
anthropic = { port = 10002 }
bedrock = { port = 10003, region = "us-east-1" }
vertex = { port = 10004, project = "test-project" }
"#;
        let cfg = EnvConfig::from_str(toml).unwrap_or_else(|_| std::process::abort());
        let def = cfg
            .clusters
            .definitions
            .get("provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            def.backend,
            ProviderBackend::InferenceSim,
            "explicit inference-sim must parse correctly"
        );
    }

    #[test]
    fn mock_openai_backend_parses() {
        let toml = r#"
[clusters]
names = ["provider"]

[clusters.provider]
models = ["model-x"]
role = "provider"
backend = "mock-openai"

[providers]
openai = { port = 10001 }
anthropic = { port = 10002 }
bedrock = { port = 10003, region = "us-east-1" }
vertex = { port = 10004, project = "test-project" }
"#;
        let cfg = EnvConfig::from_str(toml).unwrap_or_else(|_| std::process::abort());
        let def = cfg
            .clusters
            .definitions
            .get("provider")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            def.backend,
            ProviderBackend::MockOpenai,
            "mock-openai backend must parse correctly"
        );
    }

    #[test]
    fn consumer_cluster_name_returns_consumer_cluster() {
        let toml = r#"
[clusters]
names = ["provider-a", "consumer-b"]

[clusters.provider-a]
models = ["model-x"]
role = "provider"

[clusters.consumer-b]
models = []
role = "consumer"

[providers]
openai = { port = 10001 }
anthropic = { port = 10002 }
bedrock = { port = 10003, region = "us-east-1" }
vertex = { port = 10004, project = "test-project" }
"#;
        let cfg = EnvConfig::from_str(toml).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            cfg.consumer_cluster_name(),
            Some("consumer-b"),
            "must return the consumer cluster name"
        );
    }

    #[test]
    fn consumer_cluster_name_accepts_both_cluster() {
        let toml = r#"
[clusters]
names = ["edge"]

[clusters.edge]
models = ["model-local"]
role = "both"

[providers]
openai = { port = 10001 }
anthropic = { port = 10002 }
bedrock = { port = 10003, region = "us-east-1" }
vertex = { port = 10004, project = "test-project" }
"#;
        let cfg = EnvConfig::from_str(toml).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            cfg.consumer_cluster_name(),
            Some("edge"),
            "both-role cluster must be eligible as the consumer cluster"
        );
    }

    #[test]
    fn consumer_cluster_name_returns_none_when_no_consumer_exists() {
        let toml = r#"
[clusters]
names = ["provider-a"]

[clusters.provider-a]
models = ["model-x"]
role = "provider"

[providers]
openai = { port = 10001 }
anthropic = { port = 10002 }
bedrock = { port = 10003, region = "us-east-1" }
vertex = { port = 10004, project = "test-project" }
"#;
        let cfg = EnvConfig::from_str(toml).unwrap_or_else(|_| std::process::abort());
        assert!(
            cfg.consumer_cluster_name().is_none(),
            "must return None when no consumer cluster is configured"
        );
    }

    #[test]
    fn consumer_cluster_name_works_regardless_of_cluster_ordering() {
        let toml = r#"
[clusters]
names = ["provider-b", "provider-c", "consumer-last"]

[clusters.provider-b]
models = ["model-b"]
role = "provider"

[clusters.provider-c]
models = ["model-c"]
role = "provider"

[clusters.consumer-last]
models = []
role = "consumer"

[providers]
openai = { port = 10001 }
anthropic = { port = 10002 }
bedrock = { port = 10003, region = "us-east-1" }
vertex = { port = 10004, project = "test-project" }
"#;
        let cfg = EnvConfig::from_str(toml).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            cfg.consumer_cluster_name(),
            Some("consumer-last"),
            "must find consumer even when it appears last in names list"
        );
    }

    #[test]
    fn invalid_backend_rejected() {
        let toml = r#"
[clusters]
names = ["provider"]

[clusters.provider]
models = ["model-x"]
role = "provider"
backend = "not-a-backend"

[providers]
openai = { port = 10001 }
anthropic = { port = 10002 }
bedrock = { port = 10003, region = "us-east-1" }
vertex = { port = 10004, project = "test-project" }
"#;
        let result = EnvConfig::from_str(toml);
        assert!(result.is_err(), "invalid backend value must be rejected");
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "test exercises multiple cluster definitions")]
    fn mixed_backends_per_cluster() {
        let toml = r#"
[clusters]
names = ["prov-a", "prov-b"]

[clusters.prov-a]
models = ["model-a"]
role = "provider"
backend = "inference-sim"

[clusters.prov-b]
models = ["model-b"]
role = "provider"
backend = "mock-openai"

[providers]
openai = { port = 10001 }
anthropic = { port = 10002 }
bedrock = { port = 10003, region = "us-east-1" }
vertex = { port = 10004, project = "test-project" }
"#;
        let cfg = EnvConfig::from_str(toml).unwrap_or_else(|_| std::process::abort());
        let a = cfg
            .clusters
            .definitions
            .get("prov-a")
            .unwrap_or_else(|| std::process::abort());
        let b = cfg
            .clusters
            .definitions
            .get("prov-b")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            a.backend,
            ProviderBackend::InferenceSim,
            "prov-a must use inference-sim"
        );
        assert_eq!(b.backend, ProviderBackend::MockOpenai, "prov-b must use mock-openai");
    }
}
