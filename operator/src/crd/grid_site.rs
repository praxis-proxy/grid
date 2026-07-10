//! [`GridSite`] custom resource definition.
//!
//! Represents a remote site in the grid. Created manually for
//! seed peers or automatically by SWIM discovery. The status
//! tracks the site lifecycle from discovery through mTLS
//! establishment to active connectivity.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

/// Specification for a [`GridSite`].
///
/// Describes a remote site's egress endpoint, region, and
/// grid membership.
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "grid.praxis-proxy.io",
    version = "v1alpha1",
    kind = "GridSite",
    plural = "gridsites",
    status = "GridSiteStatus",
    namespaced = false,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Network","type":"string","jsonPath":".spec.gridNetworkRef"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct GridSiteSpec {
    /// Name of the [`GridNetwork`] this site belongs to.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub grid_network_ref: String,

    /// Egress endpoint for data-plane connectivity.
    pub egress: Option<EgressConfig>,

    /// Deployment region.
    pub region: Option<String>,

    /// Sovereignty zone for data residency constraints.
    pub sovereignty_zone: Option<String>,

    /// Availability zone.
    pub zone: Option<String>,
}

/// Egress endpoint configuration for a site.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EgressConfig {
    /// Address of the egress gateway (host:port).
    pub address: String,

    /// TLS mode for the connection.
    #[serde(default)]
    pub tls: EgressTls,
}

/// TLS configuration for site egress.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct EgressTls {
    /// TLS mode: Mutual, Simple, or Passthrough.
    #[serde(default = "default_tls_mode")]
    pub mode: String,
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Observed status of a [`GridSite`].
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GridSiteStatus {
    /// Capabilities offered by this site.
    #[serde(default)]
    pub capabilities: SiteCapabilities,

    /// Timestamp of the last SWIM probe.
    pub last_probe_time: Option<String>,

    /// Timestamp of the last phase transition.
    pub last_transition_time: Option<String>,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,

    /// Current lifecycle phase.
    #[serde(default)]
    pub phase: GridSitePhase,

    /// Remote site's public certificate PEM (received
    /// during mTLS exchange).
    pub public_cert_pem: Option<String>,
}

/// Capabilities a site advertises over the grid.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[expect(clippy::struct_excessive_bools, reason = "capability flags are boolean by nature")]
#[serde(rename_all = "camelCase")]
pub struct SiteCapabilities {
    /// Site offers A2A agent access.
    #[serde(default)]
    pub agent_to_agent: bool,

    /// Site offers MCP tool access.
    #[serde(default)]
    pub agent_tools: bool,

    /// Site offers inference access.
    #[serde(default)]
    pub inference: bool,
}

impl SiteCapabilities {
    /// Returns true if the site offers any capability.
    pub fn has_any(&self) -> bool {
        self.agent_to_agent || self.agent_tools || self.inference
    }
}

/// Lifecycle phase of a [`GridSite`].
///
/// ```text
/// Pending → Discovered → Connecting → Active
///                                       ↓
///                                  Unreachable → Left
/// ```
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum GridSitePhase {
    /// Site record created but not yet seen via SWIM.
    #[default]
    Pending,

    /// SWIM has discovered this site.
    Discovered,

    /// mTLS certificate exchange in progress.
    Connecting,

    /// Fully connected: SWIM + mTLS + capabilities + ping.
    Active,

    /// Previously active but SWIM probes failing.
    Unreachable,

    /// Site has left the grid (graceful or timeout).
    Left,
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

/// Default TLS mode for egress connections.
fn default_tls_mode() -> String {
    "Mutual".to_owned()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_site_phase() {
        let phase = GridSitePhase::default();
        assert_eq!(phase, GridSitePhase::Pending, "should default to Pending");
    }

    #[test]
    fn capabilities_has_any() {
        let empty = SiteCapabilities::default();
        assert!(!empty.has_any(), "empty capabilities");

        let with_inference = SiteCapabilities {
            inference: true,
            ..Default::default()
        };
        assert!(with_inference.has_any(), "inference capability");
    }

    #[test]
    fn spec_serde_round_trip() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "egress": {
                "address": "egress.cluster-b:8443",
                "tls": {"mode": "Mutual"}
            },
            "region": "us-east-1"
        });
        let spec: GridSiteSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(spec.grid_network_ref, "production", "network ref");
        assert_eq!(spec.region.as_deref(), Some("us-east-1"), "region");
    }

    #[test]
    fn status_defaults() {
        let status = GridSiteStatus::default();
        assert_eq!(status.phase, GridSitePhase::Pending, "default phase");
        assert!(!status.capabilities.has_any(), "no default capabilities");
    }
}
