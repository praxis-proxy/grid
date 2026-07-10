//! [`AgentToolProvider`] custom resource definition.
//!
//! Represents MCP tool servers available over the grid.

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

/// Specification for an [`AgentToolProvider`].
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "grid.praxis-proxy.io",
    version = "v1alpha1",
    kind = "AgentToolProvider",
    plural = "agenttoolproviders",
    status = "AgentToolProviderStatus",
    namespaced = false,
    printcolumn = r#"{"name":"Protocol","type":"string","jsonPath":".spec.protocol"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct AgentToolProviderSpec {
    /// Name of the [`GridNetwork`] this provider belongs to.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub grid_network_ref: String,

    /// Which sites can consume these tools.
    #[serde(default)]
    pub access_policy: AccessPolicy,

    /// Authentication configuration.
    pub auth: Option<AuthConfig>,

    /// HTTP endpoint of the MCP server.
    pub endpoint: String,

    /// Protocol used (only "mcp" initially).
    #[serde(default = "default_protocol")]
    pub protocol: String,

    /// Which sites host this provider.
    #[serde(default)]
    pub site_selector: SelectorConfig,

    /// Tool definitions (auto-discovered if omitted).
    #[serde(default)]
    pub tools: Vec<ToolInfo>,
}

/// Metadata for a single MCP tool.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ToolInfo {
    /// Tool name.
    pub name: String,

    /// Human-readable description.
    pub description: Option<String>,
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Observed status of an [`AgentToolProvider`].
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentToolProviderStatus {
    /// Tools discovered via MCP `tools/list`.
    #[serde(default)]
    pub discovered_tools: Vec<String>,

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

/// Default protocol for tool providers.
fn default_protocol() -> String {
    "mcp".to_owned()
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
            "endpoint": "http://tools:8080",
            "tools": [{"name": "db-query", "description": "Query database"}]
        });
        let spec: AgentToolProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(spec.protocol, "mcp", "default protocol");
        assert_eq!(spec.tools.len(), 1, "tool count");
    }
}
