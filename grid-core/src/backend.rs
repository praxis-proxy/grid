//! Backend type definitions for the AI Grid.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Provider Kind
// ---------------------------------------------------------------------------

/// Inference provider kind.
///
/// Identifies which API format and authentication model a
/// backend uses.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// Anthropic Messages API.
    Anthropic,

    /// AWS Bedrock Converse API.
    Bedrock,

    /// `OpenAI` chat completions API.
    OpenAi,

    /// Google Vertex AI `generateContent` API.
    Vertex,
}

// ---------------------------------------------------------------------------
// Backend Kind
// ---------------------------------------------------------------------------

/// Backend deployment category.
///
/// Determines locality scoring and metric sources.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    /// Third-party API provider (e.g. `OpenAI`, Anthropic).
    ApiProvider,

    /// Cloud-managed inference service (e.g. Bedrock, Vertex).
    CloudManaged,

    /// Model server on the local cluster.
    Local,

    /// Model server on a remote grid cluster.
    Remote,
}

// ---------------------------------------------------------------------------
// Backend Config
// ---------------------------------------------------------------------------

/// Configuration for a single inference backend.
///
/// Describes the endpoint, provider, deployment kind, and cost
/// parameters used by the scoring engine.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackendConfig {
    /// Unique backend name.
    pub name: String,

    /// Cost per 1k input tokens (USD).
    pub cost_per_1k_input: f64,

    /// Cost per 1k output tokens (USD).
    pub cost_per_1k_output: f64,

    /// HTTP endpoint URL for the backend.
    pub endpoint: String,

    /// Deployment category.
    pub kind: BackendKind,

    /// Inference provider type.
    pub provider: ProviderKind,

    /// Deployment region (optional).
    pub region: Option<String>,
}

impl BackendConfig {
    /// Creates a new backend configuration.
    #[must_use]
    #[expect(clippy::too_many_arguments, reason = "constructor for flat config struct")]
    pub fn new(
        name: String,
        cost_per_1k_input: f64,
        cost_per_1k_output: f64,
        endpoint: String,
        kind: BackendKind,
        provider: ProviderKind,
        region: Option<String>,
    ) -> Self {
        Self {
            name,
            cost_per_1k_input,
            cost_per_1k_output,
            endpoint,
            kind,
            provider,
            region,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_construction() {
        let b = BackendConfig::new(
            "test".to_owned(),
            0.03,
            0.06,
            "http://localhost:8080".to_owned(),
            BackendKind::ApiProvider,
            ProviderKind::OpenAi,
            None,
        );
        assert_eq!(b.name, "test", "name mismatch");
        assert_eq!(b.kind, BackendKind::ApiProvider, "kind mismatch");
        assert_eq!(b.provider, ProviderKind::OpenAi, "provider mismatch");
    }

    #[test]
    fn serde_round_trip() {
        let b = BackendConfig::new(
            "demo".to_owned(),
            0.01,
            0.02,
            "http://example.com".to_owned(),
            BackendKind::Local,
            ProviderKind::Anthropic,
            Some("us-east-1".to_owned()),
        );
        let json = serde_json::to_string(&b).unwrap_or_else(|_| std::process::abort());
        let parsed: BackendConfig = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(parsed.name, "demo", "name mismatch after round-trip");
        assert_eq!(parsed.kind, BackendKind::Local, "kind mismatch after round-trip");
    }
}
