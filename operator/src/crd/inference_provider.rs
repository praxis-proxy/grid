//! [`InferenceProvider`] custom resource definition.
//!
//! Represents an inference backend available over the grid.
//! Three backend categories: self-hosted clusters (llm-d),
//! cloud-managed services (Bedrock, Vertex), and third-party
//! APIs (OpenAI, Anthropic).

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::auth::{AccessPolicy, AuthConfig};

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

/// Specification for an [`InferenceProvider`].
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "grid.praxis-proxy.io",
    version = "v1alpha1",
    kind = "InferenceProvider",
    plural = "inferenceproviders",
    status = "InferenceProviderStatus",
    namespaced = false,
    printcolumn = r#"{"name":"Provider","type":"string","jsonPath":".spec.providerKind"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct InferenceProviderSpec {
    /// Name of the [`GridNetwork`] this provider belongs to.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub grid_network_ref: String,

    /// Which sites can consume this provider.
    #[serde(default)]
    pub access_policy: AccessPolicy,

    /// Authentication configuration.
    pub auth: Option<AuthConfig>,

    /// Backend deployment category.
    pub backend_kind: String,

    /// Cost information.
    pub cost: Option<CostConfig>,

    /// HTTP endpoint URL.
    pub endpoint: String,

    /// Health check configuration.
    pub health_check: Option<HealthCheckConfig>,

    /// Models served by this provider.
    #[serde(default)]
    pub models: Vec<ModelInfo>,

    /// Inference provider type.
    pub provider_kind: String,

    /// Which sites host this provider.
    #[serde(default)]
    pub site_selector: super::auth::SelectorConfig,
}

/// Cost information for an inference provider.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CostConfig {
    /// Cost per million input tokens (USD).
    #[serde(default)]
    pub per_million_input_tokens: f64,

    /// Cost per million output tokens (USD).
    #[serde(default)]
    pub per_million_output_tokens: f64,
}

/// Model metadata for an inference provider.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelInfo {
    /// Model name.
    pub name: String,

    /// Supported capabilities.
    #[serde(default)]
    pub capabilities: Vec<String>,

    /// Maximum context window size.
    pub context_window: Option<u32>,
}

/// Health check configuration.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct HealthCheckConfig {
    /// Check interval (e.g. "30s").
    pub interval: Option<String>,

    /// HTTP path for health probes.
    pub path: Option<String>,

    /// Timeout per check (e.g. "5s").
    pub timeout: Option<String>,
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Observed status of an [`InferenceProvider`].
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InferenceProviderStatus {
    /// Sites matched by the site selector.
    #[serde(default)]
    pub matching_sites: Vec<String>,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,

    /// Current phase.
    #[serde(default)]
    pub phase: ProviderPhase,
}

/// Lifecycle phase of a provider resource.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum ProviderPhase {
    /// Waiting for validation.
    #[default]
    Pending,

    /// Provider is healthy and available for routing.
    Available,

    /// Provider is partially degraded.
    Degraded,

    /// Provider is not reachable.
    Unavailable,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_phase() {
        let phase = ProviderPhase::default();
        assert_eq!(phase, ProviderPhase::Pending, "should default to Pending");
    }

    #[test]
    fn spec_serde() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "providerKind": "anthropic",
            "backendKind": "api_provider",
            "endpoint": "https://api.anthropic.com",
            "models": [{"name": "claude-sonnet-4"}]
        });
        let spec: InferenceProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(spec.provider_kind, "anthropic", "provider kind");
        assert_eq!(spec.models.len(), 1, "model count");
    }
}
