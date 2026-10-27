//! [`GridSite`] custom resource definition.
//!
//! Represents a site in the grid. A peer is created manually for
//! seed peers or automatically by SWIM discovery, and its status
//! tracks the site lifecycle from discovery through mTLS
//! establishment to active connectivity. Status is membership only,
//! each cluster's view of one site, this cluster's own site included:
//! its enrollment and identity conditions live on the `GridNetwork`.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::crd::{grid_network::SignalMode, inference_provider::Condition};

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

/// Add the deprecated `GridSite` endpoint field to the generated schema.
fn add_grid_site_spec_legacy_fields(schema: &mut schemars::Schema) {
    crate::crd::add_legacy_field_aliases(schema, &[("egress", "gatewayEndpoint")]);
}

/// Specification for a [`GridSite`].
///
/// Describes a remote site's egress endpoint, region, and
/// grid membership.
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "grid.praxis.fast",
    version = "v1alpha1",
    kind = "GridSite",
    plural = "gridsites",
    shortname = "gs",
    status = "GridSiteStatus",
    namespaced = false,
    printcolumn = r#"{"name":"Network","type":"string","jsonPath":".spec.gridNetworkRef"}"#,
    printcolumn = r#"{"name":"Available","type":"string","jsonPath":".status.conditions[?(@.type==\"Available\")].status"}"#,
    printcolumn = r#"{"name":"Progressing","type":"string","jsonPath":".status.conditions[?(@.type==\"Progressing\")].status"}"#,
    printcolumn = r#"{"name":"Degraded","type":"string","jsonPath":".status.conditions[?(@.type==\"Degraded\")].status"}"#,
    printcolumn = r#"{"name":"Since","type":"date","jsonPath":".status.conditions[?(@.type==\"Available\")].lastTransitionTime"}"#,
    printcolumn = r#"{"name":"Message","type":"string","jsonPath":".status.conditions[?(@.type==\"Available\")].message","priority":1}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase","priority":1}"#
)]
#[serde(rename_all = "camelCase")]
#[schemars(transform = add_grid_site_spec_legacy_fields)]
pub struct GridSiteSpec {
    /// Name of the [`GridNetwork`] this site belongs to.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub grid_network_ref: String,

    /// Gateway endpoint for data-plane connectivity.
    #[serde(alias = "egress")]
    pub gateway_endpoint: Option<EgressConfig>,

    /// Deployment region.
    pub region: Option<String>,

    /// Sovereignty zone for data residency constraints.
    pub sovereignty_zone: Option<String>,

    /// Availability zone.
    pub zone: Option<String>,

    /// Trust policy for this site.
    ///
    /// When configured, the operator verifies the received public certificate against
    /// this policy before promoting the site to `Active`. If absent, the site remains
    /// `Connecting` with reason `TrustMaterialMissing` regardless of certificate
    /// material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust: Option<GridSiteTrustPolicy>,
}

/// Trust policy controlling when a [`GridSite`] can advance to `Active`.
///
/// Supports both legacy PEM-based fingerprinting (`certFingerprint`) and
/// canonical DER-based fingerprinting (`canonicalFingerprints`).  New
/// deployments should use `canonicalFingerprints`; the legacy field remains
/// readable so the operator can reject it with a clear migration diagnostic.
///
/// # Security
///
/// Fingerprint values must be verified out-of-band before configuration.
/// The operator performs X.509 chain, validity, and identity verification
/// via the TLS handshake; the fingerprint provides additional pin-based
/// binding to a specific leaf certificate.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GridSiteTrustPolicy {
    /// Legacy SHA-256 fingerprint of the remote site's public certificate PEM.
    ///
    /// Format: colon-separated lowercase hex bytes, e.g. `"ab:cd:ef:..."`.
    /// Computed as `sha256(pem_bytes)` where `pem_bytes` are the UTF-8 bytes of
    /// `status.publicCertPem`.
    ///
    /// **Deprecated:** use `canonicalFingerprints` for new deployments.
    /// The two fields must not both be set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_fingerprint: Option<String>,

    /// Canonical DER-certificate SHA-256 fingerprint pins.
    ///
    /// Each entry is a 64-character lowercase hex string computed as
    /// `hex(sha256(der_bytes))` where `der_bytes` are the raw DER encoding
    /// of the leaf certificate.
    ///
    /// At most two entries are allowed: the current pin and an optional
    /// next pin for bounded rotation overlap.  The probe succeeds if the
    /// peer leaf certificate matches **any** entry in this list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 2), inner(regex(pattern = "^[0-9a-f]{64}$")))]
    pub canonical_fingerprints: Option<Vec<String>>,
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
#[serde(rename_all = "camelCase")]
pub struct EgressTls {
    /// TLS transport mode.
    #[serde(default)]
    pub mode: EgressTlsMode,

    /// Expected DNS identity for TLS verification.
    ///
    /// Used as both the TLS SNI value and for certificate SAN
    /// verification.  Required when `mode` is [`EgressTlsMode::Mutual`];
    /// must be absent for [`EgressTlsMode::Plaintext`].
    ///
    /// Must be a valid DNS name (not an IP address), at most 253
    /// characters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 253))]
    pub server_name: Option<String>,
}

/// TLS transport mode for egress connections.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
pub enum EgressTlsMode {
    /// Mutual TLS with certificate-based client authentication.
    #[default]
    Mutual,

    /// Explicit plaintext for reachability diagnostics only.
    ///
    /// A TCP-only endpoint cannot become `Active`; identity-verified TLS is
    /// required for routing eligibility.
    Plaintext,
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Observed status of a [`GridSite`].
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GridSiteStatus {
    /// When gossip stopped vouching for this auto-discovered site; cleared when it returns.
    ///
    /// Auto discovery deletes the site once this is older than 24 hours. Never set on a
    /// declared site.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub absent_since: Option<String>,

    /// Capabilities offered by this site.
    #[serde(default)]
    pub capabilities: SiteCapabilities,

    /// Observed conditions: `Connected`, and the `Available`, `Progressing` and `Degraded`
    /// roll-up, on every site, this cluster's own included. Its `Accepted`, `ResolvedRefs`,
    /// `Enrolled`, `IdentityValid` and `RotationOnSchedule` live on the `GridNetwork`. The
    /// phase stays, kept for v1alpha1 readers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(extend("x-kubernetes-list-type" = "map", "x-kubernetes-list-map-keys" = ["type"]))]
    pub conditions: Vec<Condition>,

    /// Timestamp of the last probe whose resulting status was persisted.
    ///
    /// This is not a liveness heartbeat; it may remain unchanged when a probe
    /// produces no status changes.
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
    /// Examples: `"AwaitingDiscovery"`, `"GatewayAddressKnown"`,
    /// `"TlsVerified"`, and `"IdentityVerificationRequired"`.
    #[serde(default)]
    pub reason: String,
}

/// Capabilities a site advertises over the grid.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
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

impl GridSitePhase {
    /// Whether this site's providers stay candidates. Under poll the gateway drops a silent
    /// site on signal staleness, so a verified site that loses its connection stays.
    #[must_use]
    pub fn routes(&self, signal: SignalMode) -> bool {
        match self {
            Self::Active => true,
            Self::Unreachable => signal == SignalMode::Poll,
            Self::Pending | Self::Discovered | Self::Connecting | Self::Left => false,
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

    #[test]
    fn only_poll_keeps_an_unreachable_site_routable() {
        for (phase, gossip, poll) in [
            (GridSitePhase::Active, true, true),
            (GridSitePhase::Unreachable, false, true),
            (GridSitePhase::Pending, false, false),
            (GridSitePhase::Discovered, false, false),
            (GridSitePhase::Connecting, false, false),
            (GridSitePhase::Left, false, false),
        ] {
            assert_eq!(phase.routes(SignalMode::Gossip), gossip, "{phase:?} under gossip");
            assert_eq!(phase.routes(SignalMode::Poll), poll, "{phase:?} under poll");
        }
    }

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
            "gatewayEndpoint": {
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
            "grid.praxis.fast",
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

    #[test]
    fn egress_tls_mode_defaults_to_mutual() {
        assert_eq!(
            EgressTlsMode::default(),
            EgressTlsMode::Mutual,
            "default TLS mode must be Mutual"
        );
    }

    #[test]
    fn egress_tls_mode_serde_round_trip() {
        let mutual_json = serde_json::json!("Mutual");
        let mutual: EgressTlsMode = serde_json::from_value(mutual_json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(mutual, EgressTlsMode::Mutual, "Mutual must round-trip");

        let plaintext_json = serde_json::json!("Plaintext");
        let pt: EgressTlsMode = serde_json::from_value(plaintext_json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(pt, EgressTlsMode::Plaintext, "Plaintext must round-trip");
    }

    #[test]
    fn unknown_tls_mode_rejected() {
        let unknown = serde_json::json!("Passthrough");
        let result: Result<EgressTlsMode, _> = serde_json::from_value(unknown);
        assert!(result.is_err(), "unknown TLS mode must fail closed");
    }

    #[test]
    fn egress_tls_with_server_name() {
        let json = serde_json::json!({
            "mode": "Mutual",
            "serverName": "east-provider.grid.internal"
        });
        let tls: EgressTls = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(tls.mode, EgressTlsMode::Mutual, "mode");
        assert_eq!(
            tls.server_name.as_deref(),
            Some("east-provider.grid.internal"),
            "serverName"
        );
    }

    #[test]
    fn egress_tls_without_server_name() {
        let json = serde_json::json!({"mode": "Plaintext"});
        let tls: EgressTls = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(tls.mode, EgressTlsMode::Plaintext, "mode");
        assert!(tls.server_name.is_none(), "serverName must be absent for Plaintext");
    }

    #[test]
    fn trust_policy_with_canonical_fingerprints() {
        let json = serde_json::json!({
            "canonicalFingerprints": [
                "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
                "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            ]
        });
        let policy: GridSiteTrustPolicy = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert!(policy.cert_fingerprint.is_none(), "legacy fingerprint must be absent");
        let pins = policy.canonical_fingerprints.unwrap_or_else(|| std::process::abort());
        assert_eq!(pins.len(), 2, "must have 2 canonical pins");
    }

    #[test]
    fn trust_policy_legacy_only() {
        let json = serde_json::json!({
            "certFingerprint": "ab:cd:ef"
        });
        let policy: GridSiteTrustPolicy = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert!(policy.cert_fingerprint.is_some(), "legacy fingerprint must be present");
        assert!(policy.canonical_fingerprints.is_none(), "canonical must be absent");
    }

    #[test]
    fn legacy_egress_deserializes_into_gateway_endpoint() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "egress": {
                "address": "egress.cluster-b:8443",
                "tls": {"mode": "Mutual"}
            }
        });
        let spec: GridSiteSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let endpoint = spec.gateway_endpoint.unwrap_or_else(|| std::process::abort());
        assert_eq!(endpoint.address, "egress.cluster-b:8443");
        assert_eq!(endpoint.tls.mode, EgressTlsMode::Mutual);
    }

    #[test]
    fn legacy_and_current_gateway_endpoint_names_are_rejected_together() {
        let endpoint = serde_json::json!({
            "address": "egress.cluster-b:8443",
            "tls": {"mode": "Mutual"}
        });
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "egress": {
                "address": "egress.cluster-b:8443",
                "tls": {"mode": "Mutual"}
            },
            "gatewayEndpoint": endpoint
        });
        let result: Result<GridSiteSpec, _> = serde_json::from_value(json);
        assert!(result.is_err(), "both endpoint names must be rejected");
    }

    #[test]
    fn backward_compatible_spec_without_new_fields() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "gatewayEndpoint": {
                "address": "egress.cluster-b:8443",
                "tls": {"mode": "Mutual"}
            },
            "trust": {
                "certFingerprint": "ab:cd:ef"
            }
        });
        let spec: GridSiteSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(spec.grid_network_ref, "production", "network ref");
        let egress = spec.gateway_endpoint.unwrap_or_else(|| std::process::abort());
        assert_eq!(egress.tls.mode, EgressTlsMode::Mutual, "mode");
        assert!(egress.tls.server_name.is_none(), "no server_name in legacy spec");
        let trust = spec.trust.unwrap_or_else(|| std::process::abort());
        assert_eq!(
            trust.cert_fingerprint.as_deref(),
            Some("ab:cd:ef"),
            "legacy fingerprint"
        );
        assert!(trust.canonical_fingerprints.is_none(), "no canonical in legacy spec");
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "checks the complete generated GridSite schema")]
    fn grid_site_crd_bounds_identity_fields() {
        let crd = crd_json();
        let pins = crd
            .pointer(
                "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/trust/properties/canonicalFingerprints",
            )
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(pins.get("minItems").and_then(serde_json::Value::as_u64), Some(1));
        assert_eq!(pins.get("maxItems").and_then(serde_json::Value::as_u64), Some(2));
        assert_eq!(
            pins.pointer("/items/pattern").and_then(serde_json::Value::as_str),
            Some("^[0-9a-f]{64}$")
        );

        let server_name = crd
            .pointer(
                "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/gatewayEndpoint/properties/tls/properties/serverName",
            )
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            server_name.get("minLength").and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(
            server_name.get("maxLength").and_then(serde_json::Value::as_u64),
            Some(253)
        );

        let spec = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec")
            .unwrap_or_else(|| std::process::abort());
        assert!(
            spec.pointer("/properties/egress").is_some(),
            "CRD must preserve legacy egress"
        );
        let validations = spec
            .get("x-kubernetes-validations")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            validations.iter().any(|validation| {
                validation.get("rule").and_then(serde_json::Value::as_str)
                    == Some("!(has(self.egress) && has(self.gatewayEndpoint))")
            }),
            "CRD must reject both endpoint names"
        );
    }

    #[test]
    fn the_own_site_carries_no_local_config() {
        let crd = crd_json();
        assert!(
            crd.pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/local")
                .is_none(),
            "this site's own config lives in the operator ConfigMap"
        );
    }
}
