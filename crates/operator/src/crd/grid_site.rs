//! [`GridSite`] custom resource definition.
//!
//! Represents a site in the grid. A peer is created manually for
//! seed peers or automatically by SWIM discovery, and its status
//! tracks the site lifecycle from discovery through mTLS
//! establishment to active connectivity. This cluster's own site
//! carries `spec.local` and reports enrollment in conditions.

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
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Network","type":"string","jsonPath":".spec.gridNetworkRef"}"#
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

    /// This cluster's own site: how it enrolls and renews its identity.
    ///
    /// Set on one `GridSite` only. Before the site has an identity, the `GridSite`
    /// carrying `local` is this cluster's own site. Once it has one, the `GridSite`
    /// named after the identity's site is, and a `local` set elsewhere is reported
    /// on that object and never acted on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local: Option<LocalSite>,
}

/// How this cluster's own site enrolls and renews its identity.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
#[schemars(extend("x-kubernetes-validations" = [{
    "rule": "!has(self.inviteSecretRef) || (has(self.signerEndpoint) && has(self.signerEndpoint.caBundleRef))",
    "message": "inviteSecretRef needs signerEndpoint with a caBundleRef to enroll through"
}]))]
pub struct LocalSite {
    /// The signer this site enrolls and renews its identity through.
    ///
    /// Unset, nothing renews the identity, and `RotationOnSchedule` says so.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer_endpoint: Option<SignerEndpoint>,

    /// Secret holding the one-time invite token this site enrolls with.
    ///
    /// The token is an opaque credential the signer issued: the operator presents it and
    /// never parses it. Read once, while the site has no identity. The Secret must carry
    /// the label `grid.praxis.fast/site: <site name>`, naming this `GridSite`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invite_secret_ref: Option<InviteSecretRef>,
}

/// Whether this operator renews the site identity through the signer.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum Rotation {
    /// Renew through the signer when a third of the identity's lifetime remains. Refused
    /// under pin peer trust, where a renewed leaf would not be trusted.
    #[default]
    Enabled,
    /// Use the signer to enroll only. The identity is kept until it expires unless
    /// another writer renews it.
    Disabled,
}

/// The signer a site enrolls and renews its identity through.
// The host rule spells out certs::SPIFFE_TRUST_DOMAIN, as signer_url in enroll.rs does.
// One case table in enroll/tests.rs holds the two together. Raw CEL strings carry the
// regex escapes, since a CEL string literal has no `\[` escape.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
#[schemars(extend("x-kubernetes-validations" = [{
    "rule": "self.url.startsWith('https://')",
    "message": "url must be an https URL"
}, {
    "rule": "!self.url.matches('[?#]')",
    "message": "url is a base URL and carries no query or fragment"
}, {
    "rule": "self.url.matches(r'^https://(\\[[0-9a-fA-F:.]+\\]|[^/?#%:@\\[][^/?#%:@]*)(:|/|$)')",
    "message": "url must name a host, with no percent-encoding before the path"
}, {
    "rule": "self.url.matches(r'^https://(\\[[^\\]]*\\]|[^/:\\[]*)(:[0-9]{1,5})?(/|$)')",
    "message": "url port must be numeric"
}, {
    "rule": "!self.url.matches('^https://[^/?#]*@')",
    "message": "url must not carry credentials"
}, {
    "rule": "!self.url.lowerAscii().matches('^https://([^/?#:]*[.])?grid[.]internal[.]?(:[0-9]*)?([/?#].*)?$')",
    "message": "url must not name a host under grid.internal, where site certificates live"
}]))]
pub struct SignerEndpoint {
    /// CA bundle pinning the signer's serving certificate.
    ///
    /// Required to enroll. Renewal falls back to the grid CA the site holds. The object
    /// must carry the label `grid.praxis.fast/signer-bundle: "true"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_bundle_ref: Option<BundleRef>,

    /// Grid CA the signer's returned CA must match, when it differs from `caBundleRef`.
    ///
    /// A bootstrap override for a signer that cannot serve its own bundle, such as one
    /// behind a certificate from another CA. The object must carry the label
    /// `grid.praxis.fast/signer-bundle: "true"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grid_ca_bundle_ref: Option<BundleRef>,

    /// Whether this operator renews the identity through the signer.
    ///
    /// `Enabled`, the default, renews when a third of the lifetime remains and is refused
    /// under pin peer trust: the own site reports `Accepted` `False` with
    /// `RotationUnsupportedUnderPinTrust` and nothing renews. `Disabled` uses the signer
    /// to enroll only. Enrollment runs whatever this says.
    #[serde(default)]
    pub rotation: Rotation,

    /// Base URL of the signer: https, naming a host with an optional numeric port, with
    /// no credentials, no query or fragment, no percent-encoding before the path, and
    /// not under `grid.internal`.
    #[schemars(length(min = 1, max = 512))]
    pub url: String,
}

/// A PEM bundle in a `Secret` or `ConfigMap` in the operator namespace.
///
/// The object must carry the label `grid.praxis.fast/signer-bundle: "true"`. The
/// operator refuses one without it, so a `GridSite` cannot pin a bundle that happens
/// to be in the namespace.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BundleRef {
    /// Object name.
    #[schemars(length(min = 1, max = 253))]
    pub name: String,
    /// Key holding the PEM bundle.
    #[serde(default = "default_bundle_key")]
    #[schemars(length(min = 1, max = 253))]
    pub key: String,
    /// Kind of the object holding the bundle: `Secret` or `ConfigMap`.
    pub kind: BundleKind,
}

/// Kind of object a [`BundleRef`] names.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum BundleKind {
    /// A `Secret`.
    Secret,
    /// A `ConfigMap`.
    ConfigMap,
}

/// The invite token's `Secret` in the operator namespace.
///
/// The Secret must carry the label `grid.praxis.fast/site: <site name>`, naming this
/// `GridSite`. The operator refuses one without it, so an unrelated Secret is never
/// sent as a token.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InviteSecretRef {
    /// Secret name.
    #[schemars(length(min = 1, max = 253))]
    pub name: String,
    /// Key holding the token.
    #[serde(default = "default_invite_key")]
    #[schemars(length(min = 1, max = 253))]
    pub key: String,
}

/// Default [`BundleRef::key`].
fn default_bundle_key() -> String {
    "ca.crt".to_owned()
}

/// Default [`InviteSecretRef::key`].
fn default_invite_key() -> String {
    "token".to_owned()
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

    /// On this cluster's own site: `Available`, `Progressing`, `Degraded`, `Accepted`,
    /// `ResolvedRefs`, `Enrolled`, `IdentityValid` and `RotationOnSchedule`. Authoritative
    /// there, where `phase` is informational.
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

    /// The rules of a schema node, as text.
    fn rules(node: &serde_json::Value) -> Vec<&str> {
        node.get("x-kubernetes-validations")
            .and_then(serde_json::Value::as_array)
            .map(|rules| {
                rules
                    .iter()
                    .filter_map(|rule| rule.get("rule").and_then(serde_json::Value::as_str))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The `spec.local` schema node.
    fn local_schema() -> serde_json::Value {
        crd_json()
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/local")
            .cloned()
            .unwrap_or_else(|| std::process::abort())
    }

    #[test]
    fn the_local_site_fields_live_under_spec_local() {
        let crd = crd_json();
        let spec = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec")
            .unwrap_or_else(|| std::process::abort());
        for moved in ["signerEndpoint", "inviteSecretRef", "rotation"] {
            assert!(
                spec.pointer(&format!("/properties/{moved}")).is_none(),
                "{moved} belongs under local"
            );
        }
        assert!(
            local_schema().pointer("/properties/rotation").is_none(),
            "rotation belongs under signerEndpoint"
        );
        assert!(
            !rules(spec).iter().any(|rule| rule.contains("inviteSecretRef")),
            "the invite rule belongs under local"
        );
        assert_eq!(
            rules(&local_schema()),
            ["!has(self.inviteSecretRef) || (has(self.signerEndpoint) && has(self.signerEndpoint.caBundleRef))"]
        );
    }

    #[test]
    fn rotation_defaults_to_enabled_under_the_signer_which_keeps_its_rules() {
        let local = local_schema();
        let signer = local
            .pointer("/properties/signerEndpoint")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            signer.pointer("/properties/rotation/default"),
            Some(&serde_json::json!("Enabled")),
            "rotation defaults to Enabled"
        );
        assert_eq!(
            signer.pointer("/properties/rotation/enum"),
            Some(&serde_json::json!(["Enabled", "Disabled"]))
        );
        assert_eq!(
            signer.pointer("/required"),
            Some(&serde_json::json!(["url"])),
            "rotation is defaulted, never required"
        );
        assert_eq!(
            rules(signer).len(),
            6,
            "https, no query or fragment, a host, a numeric port, no credentials, and the grid domain: {:?}",
            rules(signer)
        );
    }

    #[test]
    fn the_label_requirements_are_in_the_field_descriptions() {
        let local = local_schema();
        let describes = |path: &str, label: &str| {
            local
                .pointer(&format!("{path}/description"))
                .and_then(serde_json::Value::as_str)
                .is_some_and(|text| text.contains(label))
        };
        for bundle in ["caBundleRef", "gridCaBundleRef"] {
            let path = format!("/properties/signerEndpoint/properties/{bundle}");
            assert_eq!(
                local.pointer(&format!("{path}/required")),
                Some(&serde_json::json!(["kind", "name"])),
                "{bundle}: kind is never defaulted"
            );
            assert!(describes(&path, "grid.praxis.fast/signer-bundle"), "{bundle}");
        }
        assert!(describes("/properties/inviteSecretRef", "grid.praxis.fast/site"));
    }

    #[test]
    fn a_bundle_ref_without_a_kind_is_refused() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "local": {"signerEndpoint": {"url": "https://enroll.example.com", "caBundleRef": {"name": "grid-ca-bundle"}}}
        });
        let result: Result<GridSiteSpec, _> = serde_json::from_value(json);
        assert!(result.is_err(), "kind is required");
        let complete = serde_json::json!({
            "gridNetworkRef": "production",
            "local": {"signerEndpoint": {"url": "https://enroll.example.com", "caBundleRef": {"kind": "ConfigMap", "name": "grid-ca"}}}
        });
        let spec: GridSiteSpec = serde_json::from_value(complete).unwrap_or_else(|_| std::process::abort());
        let local = spec.local.unwrap_or_else(|| std::process::abort());
        let signer = local.signer_endpoint.unwrap_or_else(|| std::process::abort());
        assert_eq!(signer.rotation, Rotation::Enabled, "rotation defaults to Enabled");
        let bundle = signer.ca_bundle_ref.unwrap_or_else(|| std::process::abort());
        assert_eq!((bundle.kind, bundle.key.as_str()), (BundleKind::ConfigMap, "ca.crt"));
    }
}
