//! [`AgentToAgentProvider`] custom resource definition.
//!
//! The resource is the control-plane contract for an A2A agent. It describes
//! where the agent is reached and how its Agent Card is discovered; a future
//! controller will populate the bounded card snapshot in status. The Grid
//! operator does not proxy A2A requests.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    auth::{AccessPolicy, AuthConfig, SelectorConfig},
    inference_provider::{EndpointTlsConfig, MetricsConfig, ProviderPhase, TrafficPolicy},
};

// ---------------------------------------------------------------------------
// Bounds and defaults
// ---------------------------------------------------------------------------

/// Default public Agent Card path from the A2A discovery convention.
pub const DEFAULT_AGENT_CARD_PATH: &str = "/.well-known/agent-card.json";

/// Maximum URL length accepted for an endpoint or explicit card URL.
const MAX_CARD_URL_LENGTH: usize = 2048;
/// Maximum relative path length accepted for Agent Card discovery.
const MAX_CARD_PATH_LENGTH: usize = 512;
/// Maximum number of skills retained in configuration or status.
const MAX_SKILLS: usize = 128;
/// Maximum length of an Agent Card skill ID.
const MAX_SKILL_ID_LENGTH: usize = 128;
/// Maximum length of an Agent Card display name.
const MAX_SKILL_NAME_LENGTH: usize = 128;
/// Maximum number of input or output modes retained per skill.
const MAX_MODE_COUNT: usize = 8;
/// Maximum length of an input or output mode.
const MAX_MODE_LENGTH: usize = 128;
/// Maximum number of protocol interfaces retained in status.
const MAX_INTERFACES: usize = 8;
/// Maximum length of an advertised version string.
const MAX_VERSION_LENGTH: usize = 128;
/// Maximum number of site names retained in status.
const MAX_MATCHING_SITES: usize = 256;
/// Maximum length of a `GridSite` name retained in status.
const MAX_SITE_NAME_LENGTH: usize = 253;
/// Maximum serialized size for a fully bounded A2A status.
#[cfg(test)]
const MAX_STATUS_JSON_BYTES: usize = 512 * 1024;

/// Return the initial A2A binding.
fn default_binding() -> AgentToAgentBinding {
    AgentToAgentBinding::JsonRpc
}

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

/// Specification for an [`AgentToAgentProvider`].
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "grid.praxis.fast",
    version = "v1alpha1",
    kind = "AgentToAgentProvider",
    plural = "agenttoagentproviders",
    shortname = "a2ap",
    status = "AgentToAgentProviderStatus",
    namespaced = false,
    printcolumn = r#"{"name":"Binding","type":"string","jsonPath":".spec.binding"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Reason","type":"string","jsonPath":".status.reason","priority":1}"#
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct AgentToAgentProviderSpec {
    /// Name of the [`GridNetwork`] this provider belongs to.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    #[schemars(length(min = 1, max = 253), regex(pattern = r"^.*\S.*$"))]
    pub grid_network_ref: String,

    /// Which sites can consume this agent.
    #[serde(default)]
    pub access_policy: AccessPolicy,

    /// Authentication for the private agent hop and same-origin HTTPS Agent
    /// Card discovery. Credentials are never sent to a different card URL
    /// origin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthConfig>,

    /// Agent Card discovery settings.
    ///
    /// When omitted, discovery requests `/.well-known/agent-card.json` from
    /// the origin of `spec.endpoint`. Card data returned by the endpoint is
    /// never trusted to replace the configured endpoint.
    ///
    /// Discovery follows no redirects and rejects loopback, link-local,
    /// unspecified, unique-local, and cloud-metadata addresses using the
    /// `AgentToolProvider` probe's SSRF policy. Authentication and TLS client
    /// identity are sent only to the HTTPS origin of `spec.endpoint`; an
    /// explicit URL on another origin is fetched without those credentials.
    /// A discovered card never changes the final data-plane authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_card: Option<AgentCardConfig>,

    /// A2A protocol binding accepted by the data plane.
    ///
    /// JSON-RPC over HTTP(S) is the only binding currently supported. The
    /// typed field deliberately rejects future or misspelled bindings at API
    /// admission until their request classifiers exist.
    #[serde(default = "default_binding")]
    pub binding: AgentToAgentBinding,

    /// HTTP(S) authority of the A2A agent.
    ///
    /// This remains the configured routing authority even when a discovered
    /// Agent Card contains another interface URL.
    #[schemars(
        length(min = 1, max = MAX_CARD_URL_LENGTH),
        regex(pattern = r"^https?://[^/?#@\s]+(:[0-9]+)?(?:/[^\s?#]*)?$")
    )]
    pub endpoint: String,

    /// Relative capacity used by an opt-in placement policy.
    #[schemars(range(min = 1, max = 1000))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity_weight: Option<u32>,

    /// Which sites host this provider.
    #[serde(default)]
    pub site_selector: SelectorConfig,

    /// TLS trust and client identity for the private agent hop and same-origin
    /// HTTPS Agent Card discovery. Client identity material is never sent to
    /// a different card URL origin.
    ///
    /// Secret values are resolved by the operator and never enter status,
    /// SWIM state, or routing overlays.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<EndpointTlsConfig>,

    /// Optional Prometheus signals used by later A2A candidate scoring.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_config: Option<MetricsConfig>,

    /// Optional administrative traffic policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traffic_policy: Option<TrafficPolicy>,
}

/// Agent Card discovery settings.
///
/// Set at most one of `url` and `path`. With neither set, discovery requests
/// `/.well-known/agent-card.json` from the origin of `spec.endpoint`.
/// A relative `path` is resolved against that origin, not appended to an
/// endpoint path: for example, `https://agent.example/a2a/v1` with
/// `path: /.well-known/agent-card.json` resolves to
/// `https://agent.example/.well-known/agent-card.json`.
///
/// Discovery follows no redirects and rejects loopback, link-local,
/// unspecified, unique-local, and cloud-metadata addresses using the same
/// SSRF policy as the `AgentToolProvider` probe. Authentication and TLS client
/// identity are sent only to the HTTPS origin of `spec.endpoint`; an explicit
/// URL on another origin is fetched without those credentials. A discovered
/// card never changes the final data-plane authority.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(
    deny_unknown_fields,
    extend("x-kubernetes-validations" = [{
        "rule": "!(has(self.url) && has(self.path))",
        "message": "set at most one of agentCard.url and agentCard.path"
    }])
)]
pub struct AgentCardConfig {
    /// Fully qualified HTTPS or HTTP URL for the Agent Card. This is a
    /// discovery-only URL; it cannot change the final data-plane authority,
    /// and credentials are not sent when it points at another origin.
    #[schemars(
        length(min = 1, max = MAX_CARD_URL_LENGTH),
        regex(pattern = r"^https?://[^/?#@\s]+(:[0-9]+)?(?:/[^\s?#]*)?$")
    )]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    /// Relative Agent Card path on the origin of `spec.endpoint`.
    ///
    /// The path must have one leading slash and is not appended to any path
    /// component already present in `spec.endpoint`.
    #[schemars(
        length(min = 1, max = MAX_CARD_PATH_LENGTH),
        regex(pattern = r"^/(?:[^/?#\s\\][^?#\s\\]*)?$")
    )]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// Optional allowlist of Agent Card skill IDs. Empty accepts every valid
    /// discovered skill; skill IDs, not display names, are authoritative.
    #[schemars(
        length(max = MAX_SKILLS),
        inner(
            length(min = 1, max = MAX_SKILL_ID_LENGTH),
            regex(pattern = r"^.*\S.*$")
        ),
        extend("x-kubernetes-list-type" = "set")
    )]
    #[serde(default)]
    pub skill_allowlist: Vec<String>,
}

/// A2A protocol binding supported by Grid.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum AgentToAgentBinding {
    /// JSON-RPC over HTTP(S), the initial A2A binding.
    #[default]
    #[serde(rename = "JSONRPC")]
    JsonRpc,
}

// ---------------------------------------------------------------------------
// Observed Agent Card
// ---------------------------------------------------------------------------

/// Bounded, routing-relevant subset of a discovered Agent Card.
///
/// The full external card is intentionally not stored in Kubernetes status.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct AgentCardInfo {
    /// Agent Card display name.
    #[schemars(length(min = 1, max = MAX_SKILL_NAME_LENGTH), regex(pattern = r"^.*\S.*$"))]
    pub name: String,

    /// Agent implementation version, when advertised.
    #[schemars(length(min = 1, max = MAX_VERSION_LENGTH), regex(pattern = r"^.*\S.*$"))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

    /// A2A protocol version, when advertised by the card.
    #[schemars(length(min = 1, max = MAX_VERSION_LENGTH), regex(pattern = r"^.*\S.*$"))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<String>,

    /// Card-level default input MIME modes.
    #[schemars(
        length(max = MAX_MODE_COUNT),
        inner(length(min = 1, max = MAX_MODE_LENGTH), regex(pattern = "^[!-~]+$")),
        extend("x-kubernetes-list-type" = "set")
    )]
    #[serde(default)]
    pub default_input_modes: Vec<String>,

    /// Card-level default output MIME modes.
    #[schemars(
        length(max = MAX_MODE_COUNT),
        inner(length(min = 1, max = MAX_MODE_LENGTH), regex(pattern = "^[!-~]+$")),
        extend("x-kubernetes-list-type" = "set")
    )]
    #[serde(default)]
    pub default_output_modes: Vec<String>,

    /// Supported protocol interfaces reduced to bounded routing metadata.
    #[schemars(length(max = MAX_INTERFACES))]
    #[serde(default)]
    pub interfaces: Vec<AgentInterfaceInfo>,

    /// Whether the agent advertises streaming support.
    #[serde(default)]
    pub streaming: bool,

    /// Skills discovered on the card after the configured allowlist was
    /// applied.
    #[schemars(length(max = MAX_SKILLS))]
    #[serde(default)]
    pub skills: Vec<AgentSkillInfo>,
}

/// Bounded protocol-interface metadata observed from an Agent Card.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct AgentInterfaceInfo {
    /// Protocol binding exposed by this interface.
    pub binding: AgentToAgentBinding,

    /// Protocol version exposed by this interface, when present.
    #[schemars(length(min = 1, max = MAX_VERSION_LENGTH), regex(pattern = r"^.*\S.*$"))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<String>,
}

/// Bounded routing metadata for one A2A Agent Card skill.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct AgentSkillInfo {
    /// Stable Agent Card skill identifier. This, rather than `name`, is the
    /// identity used by routing and task affinity.
    #[schemars(length(min = 1, max = MAX_SKILL_ID_LENGTH), regex(pattern = r"^.*\S.*$"))]
    pub id: String,

    /// Human-readable skill name.
    #[schemars(length(min = 1, max = MAX_SKILL_NAME_LENGTH), regex(pattern = r"^.*\S.*$"))]
    pub name: String,

    /// Effective input MIME modes after applying the skill override, or the
    /// card-level `defaultInputModes` when the skill omits an override.
    #[schemars(
        length(max = MAX_MODE_COUNT),
        inner(length(min = 1, max = MAX_MODE_LENGTH), regex(pattern = "^[!-~]+$")),
        extend("x-kubernetes-list-type" = "set")
    )]
    #[serde(default)]
    pub input_modes: Vec<String>,

    /// Effective output MIME modes after applying the skill override, or the
    /// card-level `defaultOutputModes` when the skill omits an override.
    #[schemars(
        length(max = MAX_MODE_COUNT),
        inner(length(min = 1, max = MAX_MODE_LENGTH), regex(pattern = "^[!-~]+$")),
        extend("x-kubernetes-list-type" = "set")
    )]
    #[serde(default)]
    pub output_modes: Vec<String>,
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Observed status of an [`AgentToAgentProvider`].
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct AgentToAgentProviderStatus {
    /// Bounded subset of the most recently discovered Agent Card.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovered_card: Option<AgentCardInfo>,

    /// Sites matched by the site selector.
    #[schemars(
        length(max = MAX_MATCHING_SITES),
        inner(
            length(min = 1, max = MAX_SITE_NAME_LENGTH),
            regex(pattern = "^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$")
        )
    )]
    #[serde(default)]
    pub matching_sites: Vec<String>,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,

    /// Current phase.
    #[serde(default)]
    pub phase: ProviderPhase,

    /// Machine-readable reason for the current phase, `None` when healthy or
    /// when the controller has not yet published a reason.
    ///
    /// Stable values include `ProviderConfigInvalid`, `GridNetworkNotFound`,
    /// `UnsupportedAuthStrategy`, `CredentialSecretRefInvalid`,
    /// `CredentialSecretMissing`, `CredentialSecretKeyMissing`,
    /// `CredentialSecretValueInvalid`, `EndpointTlsSecretMissing`,
    /// `EndpointTlsKeyMissing`, `EndpointTlsMaterialInvalid`,
    /// `AgentCardUnreachable`, `AgentCardInvalid`,
    /// `AgentCardUnsupportedBinding`, and `AgentCardNoRoutableSkills`.
    #[schemars(length(max = 128), regex(pattern = r"^.*\S.*$"))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use kube::CustomResourceExt as _;
    use serde_json::Value;

    use super::*;

    #[test]
    fn spec_defaults_to_json_rpc_and_implicit_card_discovery() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "endpoint": "https://claims-agent.example.test"
        });
        let spec: AgentToAgentProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());

        assert_eq!(spec.binding, AgentToAgentBinding::JsonRpc, "binding default");
        assert!(spec.agent_card.is_none(), "default card config is implicit");
    }

    #[test]
    fn agent_card_config_round_trips_and_serializes_skill_allowlist() {
        let json = serde_json::json!({
            "path": "/well-known/custom-card.json",
            "skillAllowlist": ["claims-processing", "document-review"]
        });
        let config: AgentCardConfig = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            config.path.as_deref(),
            Some("/well-known/custom-card.json"),
            "custom card path round-trips"
        );
        assert_eq!(config.skill_allowlist.len(), 2, "skill allowlist preserves both IDs");

        let value = serde_json::to_value(config).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            pointer(&value, "/skillAllowlist/0"),
            "claims-processing",
            "skill allowlist serializes its first ID"
        );
    }

    #[test]
    fn binding_uses_a2a_jsonrpc_wire_name() {
        let value = serde_json::to_value(AgentToAgentBinding::JsonRpc).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            value,
            serde_json::json!("JSONRPC"),
            "binding uses the A2A JSON-RPC wire name"
        );
    }

    #[test]
    fn status_never_contains_full_card_configuration() {
        let status = AgentToAgentProviderStatus {
            discovered_card: Some(AgentCardInfo {
                name: "Claims Agent".to_owned(),
                version: Some("1.0".to_owned()),
                protocol_version: Some("1.0".to_owned()),
                default_input_modes: vec!["text/plain".to_owned()],
                default_output_modes: vec!["application/json".to_owned()],
                interfaces: vec![AgentInterfaceInfo {
                    binding: AgentToAgentBinding::JsonRpc,
                    protocol_version: Some("1.0".to_owned()),
                }],
                streaming: true,
                skills: vec![AgentSkillInfo {
                    id: "claims-processing".to_owned(),
                    name: "Claims Processing".to_owned(),
                    input_modes: vec!["text/plain".to_owned()],
                    output_modes: vec!["application/json".to_owned()],
                }],
            }),
            ..AgentToAgentProviderStatus::default()
        };
        let value = serde_json::to_value(status).unwrap_or_else(|_| std::process::abort());
        assert!(value.get("auth").is_none(), "status must not contain auth");
        assert!(value.get("endpoint").is_none(), "status must not contain endpoint");
        assert_eq!(
            pointer(&value, "/discoveredCard/skills/0/id"),
            "claims-processing",
            "status retains the routing-relevant skill ID"
        );
    }

    #[test]
    fn maximum_status_stays_below_half_megabyte() {
        let status = maximum_status();

        let bytes = serde_json::to_vec(&status).unwrap_or_else(|_| std::process::abort());
        assert!(
            bytes.len() < MAX_STATUS_JSON_BYTES,
            "maximum bounded status is {} bytes",
            bytes.len()
        );
    }

    fn maximum_status() -> AgentToAgentProviderStatus {
        AgentToAgentProviderStatus {
            discovered_card: Some(maximum_discovered_card()),
            matching_sites: vec!["s".repeat(MAX_SITE_NAME_LENGTH); MAX_MATCHING_SITES],
            observed_generation: i64::MAX,
            phase: ProviderPhase::Unavailable,
            reason: Some(maximum_utf8_string(MAX_VERSION_LENGTH)),
        }
    }

    fn maximum_discovered_card() -> AgentCardInfo {
        let mode = "~".repeat(MAX_MODE_LENGTH);
        AgentCardInfo {
            name: maximum_utf8_string(MAX_SKILL_NAME_LENGTH),
            version: Some(maximum_utf8_string(MAX_VERSION_LENGTH)),
            protocol_version: Some(maximum_utf8_string(MAX_VERSION_LENGTH)),
            default_input_modes: vec![mode.clone(); MAX_MODE_COUNT],
            default_output_modes: vec![mode; MAX_MODE_COUNT],
            interfaces: vec![maximum_interface(); MAX_INTERFACES],
            streaming: true,
            skills: vec![maximum_skill(); MAX_SKILLS],
        }
    }

    fn maximum_interface() -> AgentInterfaceInfo {
        AgentInterfaceInfo {
            binding: AgentToAgentBinding::JsonRpc,
            protocol_version: Some(maximum_utf8_string(MAX_VERSION_LENGTH)),
        }
    }

    fn maximum_skill() -> AgentSkillInfo {
        let mode = "~".repeat(MAX_MODE_LENGTH);
        AgentSkillInfo {
            id: maximum_utf8_string(MAX_SKILL_ID_LENGTH),
            name: maximum_utf8_string(MAX_SKILL_NAME_LENGTH),
            input_modes: vec![mode.clone(); MAX_MODE_COUNT],
            output_modes: vec![mode; MAX_MODE_COUNT],
        }
    }

    fn maximum_utf8_string(length: usize) -> String {
        "\u{10ffff}".repeat(length)
    }

    #[test]
    fn crd_contains_bounded_skill_and_binding_schema() {
        let crd = serde_json::to_value(AgentToAgentProvider::crd()).unwrap_or_else(|_| std::process::abort());
        let binding = pointer(
            &crd,
            "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/binding",
        );
        assert_eq!(
            pointer(binding, "/enum/0"),
            "JSONRPC",
            "schema exposes the JSON-RPC binding"
        );
        let skills = pointer(
            &crd,
            "/spec/versions/0/schema/openAPIV3Schema/properties/status/properties/discoveredCard/properties/skills",
        );
        assert_eq!(
            pointer(skills, "/maxItems"),
            MAX_SKILLS,
            "schema caps discovered skills"
        );
        let skill_id = pointer(skills, "/items/properties/id");
        assert_eq!(pointer(skill_id, "/minLength"), 1, "schema rejects blank skill IDs");
        assert_eq!(
            pointer(skill_id, "/maxLength"),
            MAX_SKILL_ID_LENGTH,
            "schema bounds skill ID length"
        );
    }

    #[test]
    fn status_schema_bounds_modes_and_site_names_for_storage() {
        let crd = serde_json::to_value(AgentToAgentProvider::crd()).unwrap_or_else(|_| std::process::abort());
        let status = pointer(&crd, "/spec/versions/0/schema/openAPIV3Schema/properties/status");
        let modes = pointer(status, "/properties/discoveredCard/properties/defaultInputModes/items");
        assert_eq!(
            pointer(modes, "/pattern"),
            "^[!-~]+$",
            "mode values must be ASCII so character bounds also bound serialized bytes"
        );
        let sites = pointer(status, "/properties/matchingSites/items");
        assert_eq!(
            pointer(sites, "/pattern"),
            "^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$",
            "matching site names must use the bounded GridSite name alphabet"
        );
    }

    #[test]
    fn card_url_and_path_are_mutually_exclusive_in_schema() {
        let crd = serde_json::to_value(AgentToAgentProvider::crd()).unwrap_or_else(|_| std::process::abort());
        let config = pointer(
            &crd,
            "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/agentCard",
        );
        assert_eq!(
            pointer(config, "/x-kubernetes-validations/0/rule"),
            "!(has(self.url) && has(self.path))",
            "agentCard url/path mutual exclusion must be enforced at admission"
        );
    }

    #[test]
    fn card_path_schema_is_origin_relative_and_rejects_authority_forms() {
        let crd = serde_json::to_value(AgentToAgentProvider::crd()).unwrap_or_else(|_| std::process::abort());
        let path = pointer(
            &crd,
            "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/agentCard/properties/path",
        );
        assert_eq!(
            pointer(path, "/pattern"),
            r"^/(?:[^/?#\s\\][^?#\s\\]*)?$",
            "card path schema must remain origin-relative"
        );
    }

    fn pointer<'value>(value: &'value Value, path: &str) -> &'value Value {
        value.pointer(path).unwrap_or_else(|| std::process::abort())
    }
}
