//! [`AgentToAgentProvider`] custom resource definition.
//!
//! Represents A2A agents available over the grid.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    auth::{AccessPolicy, AuthConfig, SelectorConfig},
    inference_provider::ProviderPhase,
};

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

/// Specification for an [`AgentToAgentProvider`].
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "grid.praxis-proxy.io",
    version = "v1alpha1",
    kind = "AgentToAgentProvider",
    plural = "agenttoagentproviders",
    status = "AgentToAgentProviderStatus",
    namespaced = false,
    printcolumn = r#"{"name":"Protocol","type":"string","jsonPath":".spec.protocol"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct AgentToAgentProviderSpec {
    /// Name of the [`GridNetwork`] this provider belongs to.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub grid_network_ref: String,

    /// Which sites can delegate to these agents.
    #[serde(default)]
    pub access_policy: AccessPolicy,

    /// Agent Card metadata from `.well-known/agent.json`.
    pub agent_card: Option<AgentCardInfo>,

    /// Authentication configuration.
    pub auth: Option<AuthConfig>,

    /// HTTP endpoint of the A2A agent.
    pub endpoint: String,

    /// Protocol used (only "a2a" initially).
    #[serde(default = "default_protocol")]
    pub protocol: String,

    /// Which sites host this provider.
    #[serde(default)]
    pub site_selector: SelectorConfig,
}

/// Agent Card metadata from the `.well-known` endpoint.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AgentCardInfo {
    /// Supported input/output modalities.
    #[serde(default)]
    pub modalities: Vec<String>,

    /// Skills the agent can perform.
    #[serde(default)]
    pub skills: Vec<String>,
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Observed status of an [`AgentToAgentProvider`].
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentToAgentProviderStatus {
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

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

/// Default protocol for A2A providers.
fn default_protocol() -> String {
    "a2a".to_owned()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_serde() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "endpoint": "http://agent:8080",
            "agentCard": {
                "skills": ["claims-processing"],
                "modalities": ["text"]
            }
        });
        let spec: AgentToAgentProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(spec.protocol, "a2a", "default protocol");
        let card = spec.agent_card.as_ref();
        assert!(card.is_some(), "should have agent card");
    }
}
