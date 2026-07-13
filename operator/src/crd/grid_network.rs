//! [`GridNetwork`] custom resource definition.
//!
//! The top-level tenancy boundary for the AI Grid. A cluster
//! can host multiple `GridNetworks` for multi-tenancy.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

/// Specification for a [`GridNetwork`].
///
/// Defines the grid's seed peers, gateway associations, SWIM
/// tuning, and TLS secret references.
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "grid.praxis-proxy.io",
    version = "v1alpha1",
    kind = "GridNetwork",
    plural = "gridnetworks",
    status = "GridNetworkStatus",
    namespaced = false,
    printcolumn = r#"{"name":"Grid ID","type":"string","jsonPath":".status.gridId"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Sites","type":"integer","jsonPath":".status.connectedSites"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct GridNetworkSpec {
    /// Grid ID for tenancy. Empty on creation; auto-generated
    /// on first join with another site.
    #[serde(default)]
    pub grid_id: String,

    /// Initial SWIM seed peer addresses.
    #[serde(default)]
    pub seeds: Vec<String>,

    /// References to Praxis Gateways that participate in this grid.
    #[serde(default)]
    pub gateway_refs: Vec<GatewayRef>,

    /// Region where this site is deployed.
    pub region: Option<String>,

    /// SWIM protocol configuration.
    #[serde(default)]
    pub swim: SwimConfig,

    /// TLS secret references for grid certificate management.
    #[serde(default)]
    pub tls: TlsConfig,

    /// Availability zone.
    pub zone: Option<String>,
}

/// Reference to a Praxis Gateway that participates in this grid.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayRef {
    /// Gateway name.
    pub name: String,

    /// Gateway namespace.
    pub namespace: String,

    /// Local site name for the `grid_route` overlay generated for this gateway.
    ///
    /// Identifies which [`GridSite`] this gateway's cluster represents.
    /// Praxis uses `local_site` to score candidates running on the same site
    /// higher than remote candidates.
    ///
    /// When absent, the [`GridNetwork`] metadata name is used as a fallback.
    /// This is correct for single-site networks where the network name and
    /// site name are the same.  Multi-site networks should set this to the
    /// [`GridSite`] name for the cluster hosting this gateway.
    ///
    /// [`GridSite`]: crate::crd::grid_site::GridSite
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    #[serde(default)]
    pub local_site_name: Option<String>,
}

/// SWIM protocol tuning parameters.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SwimConfig {
    /// Fanout for indirect probes.
    #[serde(default = "default_gossip_nodes")]
    pub gossip_nodes: u32,

    /// WAN probe interval (e.g. "5s").
    #[serde(default = "default_probe_interval")]
    pub probe_interval: String,

    /// Suspicion timeout before declaring dead (e.g. "10s").
    #[serde(default = "default_suspicion_timeout")]
    pub suspicion_timeout: String,
}

/// TLS configuration for grid certificate management.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    /// Secret storing the grid CA certificate and key.
    pub ca_secret_ref: Option<SecretRef>,

    /// Secret storing this site's certificate and key.
    pub site_secret_ref: Option<SecretRef>,

    /// Secret storing the SWIM encryption key.
    pub swim_key_ref: Option<SecretRef>,
}

/// Reference to a Kubernetes Secret.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct SecretRef {
    /// Secret name.
    pub name: String,

    /// Secret namespace.
    pub namespace: String,
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Observed status of a [`GridNetwork`].
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GridNetworkStatus {
    /// Number of connected (Active) sites.
    #[serde(default)]
    pub connected_sites: u32,

    /// Number of remote provider records received for this network via CRDT state broadcasts.
    ///
    /// Counts remote provider records from the local SWIM runtime's merged
    /// CRDT state.  Local provider records and records for other `GridNetwork`s
    /// are excluded.  Zero when SWIM is disabled or no remote state has been
    /// received yet.
    #[serde(default)]
    pub distributed_provider_count: u32,

    /// The negotiated grid ID.
    #[serde(default)]
    pub grid_id: String,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,

    /// Current lifecycle phase.
    #[serde(default)]
    pub phase: GridNetworkPhase,
}

/// Lifecycle phase of a [`GridNetwork`].
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum GridNetworkPhase {
    /// Waiting for initial configuration.
    #[default]
    Pending,

    /// CA and certs being generated, SWIM starting.
    Initializing,

    /// Grid is operational with connected sites.
    Active,

    /// Grid is degraded (sites unreachable).
    Degraded,
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

/// Default SWIM gossip fanout.
fn default_gossip_nodes() -> u32 {
    3
}

/// Default WAN probe interval.
fn default_probe_interval() -> String {
    "5s".to_owned()
}

/// Default suspicion timeout.
fn default_suspicion_timeout() -> String {
    "10s".to_owned()
}

impl Default for SwimConfig {
    fn default() -> Self {
        Self {
            gossip_nodes: default_gossip_nodes(),
            probe_interval: default_probe_interval(),
            suspicion_timeout: default_suspicion_timeout(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use kube::CustomResourceExt as _;

    use super::*;

    fn crd_json() -> serde_json::Value {
        serde_json::to_value(GridNetwork::crd()).unwrap_or_else(|_| std::process::abort())
    }

    fn crd_spec<'a>(crd: &'a serde_json::Value, field: &str) -> &'a str {
        crd.get("spec")
            .and_then(|spec| spec.get(field))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_else(|| std::process::abort())
    }

    #[test]
    fn default_swim_config() {
        let cfg = SwimConfig::default();
        assert_eq!(cfg.gossip_nodes, 3, "default gossip nodes");
        assert_eq!(cfg.probe_interval, "5s", "default probe interval");
        assert_eq!(cfg.suspicion_timeout, "10s", "default suspicion timeout");
    }

    #[test]
    fn default_network_phase() {
        let phase = GridNetworkPhase::default();
        assert_eq!(phase, GridNetworkPhase::Pending, "should default to Pending");
    }

    #[test]
    fn status_defaults() {
        let status = GridNetworkStatus::default();
        assert_eq!(status.connected_sites, 0, "default sites");
        assert!(status.grid_id.is_empty(), "default grid_id empty");
        assert_eq!(status.phase, GridNetworkPhase::Pending, "default phase");
    }

    #[test]
    fn spec_serde_round_trip() {
        let json = serde_json::json!({
            "gridId": "",
            "seeds": ["grid.cluster-b:7946"],
            "gatewayRefs": [{"name": "gw", "namespace": "ns"}],
            "swim": {"probeInterval": "3s"},
            "tls": {}
        });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(spec.seeds.len(), 1, "should have 1 seed");
        assert_eq!(spec.swim.probe_interval, "3s", "custom probe interval");
    }

    #[test]
    fn gateway_ref_local_site_name_round_trips() {
        let json = serde_json::json!({
            "name": "gw-east",
            "namespace": "grid-system",
            "localSiteName": "cluster-east"
        });
        let gw: GatewayRef = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            gw.local_site_name.as_deref(),
            Some("cluster-east"),
            "localSiteName must round-trip on GatewayRef"
        );
    }

    #[test]
    fn gateway_ref_local_site_name_defaults_to_none() {
        let json = serde_json::json!({"name": "gw", "namespace": "ns"});
        let gw: GatewayRef = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert!(
            gw.local_site_name.is_none(),
            "absent localSiteName must default to None"
        );
    }

    #[test]
    fn grid_network_crd_has_correct_group_and_plural() {
        let crd = crd_json();
        assert_eq!(crd_spec(&crd, "group"), "grid.praxis-proxy.io", "wrong CRD group");
        assert_eq!(
            crd.get("spec")
                .and_then(|spec| spec.get("names"))
                .and_then(|names| names.get("plural"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| std::process::abort()),
            "gridnetworks",
            "wrong plural name"
        );
        assert_eq!(
            crd.get("spec")
                .and_then(|spec| spec.get("names"))
                .and_then(|names| names.get("kind"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| std::process::abort()),
            "GridNetwork",
            "wrong kind name"
        );
    }

    #[test]
    fn grid_network_crd_has_gateway_ref_local_site_name() {
        let crd = crd_json();
        let gateway_ref_properties = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/gatewayRefs/items/properties")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            gateway_ref_properties.contains_key("localSiteName"),
            "CRD schema must include localSiteName field on GatewayRef"
        );
    }
}
