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

    /// Trust policy for this site.
    ///
    /// When configured, the operator verifies the received public certificate against
    /// this policy before promoting the site to `Active`.  If absent, the site remains
    /// `Connecting` with reason `TrustPolicyMissing` regardless of cert material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust: Option<GridSiteTrustPolicy>,
}

/// Trust policy controlling when a [`GridSite`] can advance to `Active`.
///
/// Currently supports SHA-256 fingerprint pinning of the received public certificate.
/// The fingerprint is computed from the raw PEM bytes of `status.publicCertPem`.
///
/// # Security
///
/// Setting `certFingerprint` means you explicitly trust the specific certificate
/// that produces that fingerprint.  Verify the fingerprint out-of-band before
/// configuring it.  The operator does **not** perform X.509 chain verification —
/// the fingerprint is the trust anchor.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GridSiteTrustPolicy {
    /// Expected SHA-256 fingerprint of the remote site's public certificate PEM.
    ///
    /// Format: colon-separated lowercase hex bytes, e.g. `"ab:cd:ef:..."`.
    /// Computed as `sha256(pem_bytes)` where `pem_bytes` are the UTF-8 bytes of
    /// `status.publicCertPem`.
    ///
    /// To compute from the PEM string: `printf '%s' "$cert_pem" | sha256sum`.
    ///
    /// When this field matches the SHA-256 of `status.publicCertPem`, the site is
    /// promoted to `Active` after a successful TCP probe.  When absent, the site
    /// remains `Connecting` with reason `TrustPolicyMissing`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_fingerprint: Option<String>,
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

    /// Human-readable diagnostic for the current phase.
    ///
    /// Never contains credential token bytes.  Populated on every reconcile;
    /// empty when the operator has no additional context.
    #[serde(default)]
    pub message: String,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,

    /// Current lifecycle phase.
    #[serde(default)]
    pub phase: GridSitePhase,

    /// Remote site's public certificate PEM (received
    /// via SWIM state broadcast from the remote operator).
    pub public_cert_pem: Option<String>,

    /// Machine-readable reason for the current phase.
    ///
    /// Empty when `phase` is `Active` without additional context.
    /// Examples: `"AwaitingDiscovery"`, `"EgressKnown"`, `"EgressMissing"`,
    /// `"AwaitingReadiness"`.
    #[serde(default)]
    pub reason: String,
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

    /// Gateway address known; trust and data-plane readiness being established.
    Connecting,

    /// Fully connected according to the deployment workflow.
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
    use kube::CustomResourceExt as _;

    use super::*;

    fn crd_json() -> serde_json::Value {
        serde_json::to_value(GridSite::crd()).unwrap_or_else(|_| std::process::abort())
    }

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

    #[test]
    fn grid_site_crd_has_correct_group_and_plural() {
        let crd = crd_json();
        assert_eq!(
            crd.get("spec")
                .and_then(|spec| spec.get("group"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| std::process::abort()),
            "grid.praxis-proxy.io",
            "wrong CRD group"
        );
        assert_eq!(
            crd.get("spec")
                .and_then(|spec| spec.get("names"))
                .and_then(|names| names.get("plural"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| std::process::abort()),
            "gridsites",
            "wrong plural name"
        );
        assert_eq!(
            crd.get("spec")
                .and_then(|spec| spec.get("names"))
                .and_then(|names| names.get("kind"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| std::process::abort()),
            "GridSite",
            "wrong kind name"
        );
    }

    #[test]
    fn grid_site_crd_has_grid_network_ref() {
        let crd = crd_json();
        let spec_properties = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            spec_properties.contains_key("gridNetworkRef"),
            "CRD schema must include gridNetworkRef field"
        );
    }
}
