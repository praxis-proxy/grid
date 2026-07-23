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

    /// Maximum age in seconds before a stale (`fresh=false`) remote routing
    /// candidate is removed from the overlay.
    ///
    /// When a remote peer is declared `Dead` or `Suspect` by SWIM, its
    /// routing candidates are marked `fresh=false` and deprioritised.  Without
    /// this field those stale candidates remain in the overlay indefinitely,
    /// which is useful for observability but can accumulate over time if peers
    /// never recover.
    ///
    /// Setting this field activates overlay-level garbage collection: remote
    /// candidates whose SWIM member age is at or above this threshold are
    /// omitted from the rendered overlay.  Fresh (`fresh=true`) candidates and
    /// local candidates are never evicted.  CRDT provider records in storage
    /// are not deleted.
    ///
    /// **Default (absent):** stale candidates are retained indefinitely —
    /// the same behaviour as before this field existed.
    ///
    /// **Minimum value:** `1` second.  The generated CRD schema rejects `0`.
    /// The controller still treats an internally observed `0` as absent
    /// defensively, avoiding accidental immediate eviction if malformed data is
    /// deserialized outside the Kubernetes API path.
    ///
    /// A conservative starting value for production is `3600` (one hour),
    /// which allows short failures to recover without overlay churn while
    /// still bounding accumulation of truly dead peers.
    #[schemars(range(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_candidate_ttl_seconds: Option<u32>,
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

    /// Opt-in configuration for operator-managed consumer Praxis config generation.
    ///
    /// When absent or `enabled: false`, this gateway behaves exactly as before —
    /// only the routing overlay `ConfigMap` is applied.  When `enabled: true`, the
    /// operator additionally renders a consumer Praxis `ConfigMap` containing the
    /// `grid_route` candidates (with credential `secretRef` data), a
    /// `grid_credential_inject` section for credential-bearing candidates, and a
    /// `load_balancer` section with one cluster entry per unique candidate cluster.
    ///
    /// The generated `ConfigMap` contains no token bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_config: Option<ConsumerConfig>,
}

/// Opt-in configuration for operator-generated consumer Praxis config.
///
/// When `enabled` is `true` on a [`GatewayRef`], the `GridNetwork` controller
/// renders a `praxis.yaml`-keyed `ConfigMap` in the gateway namespace in addition
/// to the normal routing overlay `ConfigMap`.  The generated config includes the
/// `grid_route` candidates, `grid_credential_inject` (when credential-bearing
/// candidates are present), and a `load_balancer` section.
///
/// Every cluster referenced by a routing candidate must have a matching
/// `clusterEndpoints` entry.  Missing endpoint topology causes config generation
/// to fail with status reason `MissingClusterEndpoint` instead of rendering an
/// incomplete `load_balancer` cluster.
///
/// # Security
///
/// The generated `ConfigMap` never contains credential token bytes.  Credential
/// entries use a `file:` source under `credentialMountBase`; the mounted
/// Kubernetes Secret provides the token at runtime.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConfig {
    /// Enable operator-managed consumer Praxis config generation for this gateway.
    ///
    /// Default: `false`.  Set to `true` to opt in.
    #[serde(default)]
    pub enabled: bool,

    /// Base directory for mounted credential Secret files inside the consumer pod.
    ///
    /// Each credential Secret is expected to be mounted at
    /// `{credentialMountBase}/{secret-name}/{secret-key}`.
    ///
    /// Default: `/run/secrets/grid-credentials`.
    #[serde(default = "default_credential_mount_base")]
    pub credential_mount_base: String,

    /// Name of the generated consumer Praxis `ConfigMap`.
    ///
    /// Default: `praxis-consumer-config`.
    #[serde(default = "default_consumer_config_map_name")]
    pub config_map_name: String,

    /// Endpoint topology for the generated `load_balancer` section.
    ///
    /// Each entry maps a routing candidate cluster name to a reachable endpoint
    /// address with explicit transport configuration.  Every cluster referenced
    /// by a routing candidate must have a matching entry here with a non-`None`
    /// `transport` field.
    ///
    /// Missing endpoint topology causes config generation to fail with
    /// `MissingClusterEndpoint`.  Missing transport fails with
    /// `MissingTransport`.  Mutual-TLS transport without SNI fails with
    /// `MissingSni`.
    ///
    /// In production, this is populated by whoever manages the consumer gateway
    /// deployment (platform automation, the gateway operator, or a Helm chart).
    /// In local Kind validation, the xtask harness discovers `NodePort` addresses
    /// and populates this field in the test fixture.
    ///
    /// Default: empty — valid only when the rendered overlay has no candidates.
    #[serde(default)]
    pub cluster_endpoints: Vec<ClusterEndpointConfig>,

    /// Mount path for TLS certificates inside the consumer pod.
    ///
    /// Used when rendering mTLS cluster entries from `clusterEndpoints`.
    /// The operator expects the consumer pod to mount a TLS Secret at this path,
    /// containing `ca.crt`, `tls.crt`, and `tls.key`.
    ///
    /// Default: `/etc/praxis/tls`.
    #[serde(default = "default_tls_cert_mount_path")]
    pub tls_cert_mount_path: String,

    /// HTTP port for the generated Praxis listener.
    ///
    /// The rendered `listeners[0].address` is `0.0.0.0:{listenerPort}`.
    ///
    /// Default: `8080`.
    #[serde(default = "default_listener_port")]
    pub listener_port: u16,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            credential_mount_base: default_credential_mount_base(),
            config_map_name: default_consumer_config_map_name(),
            cluster_endpoints: Vec::new(),
            tls_cert_mount_path: default_tls_cert_mount_path(),
            listener_port: default_listener_port(),
        }
    }
}

/// Transport mode for a consumer load-balancer cluster endpoint.
///
/// Determines whether the consumer connects to the provider gateway
/// cluster over mutual TLS or plain HTTP.  This is an explicit security
/// decision — the operator refuses to render a cluster entry without a
/// declared transport mode, preventing accidental plaintext.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportMode {
    /// Mutual TLS with CA verification and client certificate.
    MutualTls,
    /// Plain HTTP — no TLS.  Explicit insecure/dev-only mode.
    Plaintext,
}

/// Transport configuration for a cluster endpoint.
///
/// Bundles the [`TransportMode`] with an optional SNI field.
/// When `mode` is [`MutualTls`](TransportMode::MutualTls), `sni` is
/// required and must match the Subject Alternative Name in the provider
/// gateway's server certificate.  When `mode` is
/// [`Plaintext`](TransportMode::Plaintext), `sni` must not be set —
/// setting it is rejected as a likely misconfiguration.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointTransport {
    /// Transport mode: `mutual_tls` or `plaintext`.
    pub mode: TransportMode,

    /// TLS Server Name Indication (required when mode is `mutual_tls`;
    /// must not be set when mode is `plaintext`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sni: Option<String>,
}

/// Endpoint configuration for one consumer `load_balancer` cluster.
///
/// Maps a routing candidate cluster name to a reachable provider gateway
/// endpoint with explicit transport intent.  Every cluster referenced by
/// a routing candidate must have a matching entry.
///
/// # Transport requirement
///
/// The `transport` field is required.  Missing transport fails closed
/// during config rendering with status reason `MissingTransport`.
/// When `transport.mode` is `mutual_tls`, `transport.sni` must also
/// be present and non-blank; otherwise rendering fails with status
/// reason `MissingSni`.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterEndpointConfig {
    /// Cluster name — must match a `candidate.cluster` value in the routing overlay.
    pub cluster: String,

    /// Reachable endpoint address (`host:port`).
    pub address: String,

    /// Explicit transport configuration.
    ///
    /// Required.  Use `mutual_tls` with `sni` for remote/provider-gateway
    /// traffic.  Use `plaintext` only for local/dev-only endpoints.
    /// Missing transport fails closed during config rendering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<EndpointTransport>,
}

/// Default credential mount base path.
fn default_credential_mount_base() -> String {
    "/run/secrets/grid-credentials".to_owned()
}

/// Default consumer Praxis `ConfigMap` name.
fn default_consumer_config_map_name() -> String {
    "praxis-consumer-config".to_owned()
}

/// Default TLS certificate mount path inside the consumer pod.
fn default_tls_cert_mount_path() -> String {
    "/etc/praxis/tls".to_owned()
}

/// Default HTTP listener port for the generated consumer Praxis config.
fn default_listener_port() -> u16 {
    8080
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

    /// Key within the Secret's `data` map.
    ///
    /// Required when the Secret holds multiple keys (e.g. credential references
    /// in `InferenceProvider.spec.auth.secretRef`).  Omit only when the entire
    /// Secret is consumed (e.g. TLS `ca_secret_ref`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
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

    /// Per-gateway consumer Praxis config render and apply status.
    ///
    /// Populated for every gateway reference that has `consumerConfig.enabled: true`.
    /// Gateways without `consumerConfig` are omitted.  Use this field to
    /// determine whether the operator successfully rendered and applied a
    /// consumer `ConfigMap` for each opted-in gateway.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub consumer_config_status: Vec<ConsumerConfigStatus>,
}

/// Phase of an operator-generated consumer Praxis `ConfigMap` for one gateway.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum ConsumerConfigPhase {
    /// Consumer config was successfully rendered and applied.
    Rendered,
    /// Consumer config render or apply failed.
    Error,
    /// Consumer config generation is disabled for this gateway.
    #[default]
    Disabled,
}

/// Per-gateway status for operator-managed consumer Praxis config generation.
///
/// Reported in [`GridNetworkStatus::consumer_config_status`] for each gateway
/// reference with `consumerConfig.enabled: true`.
///
/// # Security
///
/// `message` must never contain credential token bytes.  Error messages from
/// rendering only describe structural problems (blank fields, unsupported
/// strategies); credential bytes are never included.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConfigStatus {
    /// Name of the `GatewayRef` this status entry corresponds to.
    pub gateway_name: String,

    /// Namespace of the gateway (and the generated `ConfigMap`).
    pub namespace: String,

    /// Name of the generated `ConfigMap`.
    ///
    /// Populated from `consumerConfig.configMapName`; empty for `Disabled` entries.
    #[serde(default)]
    pub config_map_name: String,

    /// Current render/apply phase.
    pub phase: ConsumerConfigPhase,

    /// Machine-readable reason for the current phase.
    ///
    /// `""` when `phase` is `Rendered`.
    /// One of `MissingClusterEndpoint`, `MissingTransport`, `MissingSni`,
    /// `PlaintextWithSni`, `ConsumerConfigRenderFailed`,
    /// `ConsumerConfigApplyFailed`, `ConsumerConfigDisabled` otherwise.
    #[serde(default)]
    pub reason: String,

    /// Human-readable diagnostic message.
    ///
    /// Never contains credential token bytes.
    #[serde(default)]
    pub message: String,

    /// `GridNetwork` generation when this entry was last updated.
    #[serde(default)]
    pub observed_generation: i64,
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
    fn stale_candidate_ttl_defaults_to_none_when_absent() {
        let json = serde_json::json!({
            "gridId": "",
            "seeds": [],
            "swim": {}
        });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert!(
            spec.stale_candidate_ttl_seconds.is_none(),
            "absent staleCandidateTtlSeconds must default to None (no-op GC)"
        );
    }

    #[test]
    fn stale_candidate_ttl_round_trips() {
        let json = serde_json::json!({
            "gridId": "",
            "seeds": [],
            "staleCandidateTtlSeconds": 3600
        });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            spec.stale_candidate_ttl_seconds,
            Some(3600),
            "staleCandidateTtlSeconds must round-trip through serde"
        );
    }

    #[test]
    fn stale_candidate_ttl_serializes_only_when_present() {
        // absent field must not appear in serialized output
        let json = serde_json::json!({ "gridId": "", "seeds": [] });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let serialized = serde_json::to_value(&spec).unwrap_or_else(|_| std::process::abort());
        assert!(
            serialized.get("staleCandidateTtlSeconds").is_none(),
            "absent staleCandidateTtlSeconds must not appear in serialized output"
        );
    }

    #[test]
    fn stale_candidate_ttl_appears_in_crd_schema_with_minimum() {
        let crd = crd_json();
        let ttl_schema = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/staleCandidateTtlSeconds")
            .unwrap_or_else(|| std::process::abort());
        assert!(
            ttl_schema.is_object(),
            "staleCandidateTtlSeconds must appear in the CRD OpenAPI schema"
        );
        assert_eq!(
            ttl_schema.pointer("/minimum").and_then(serde_json::Value::as_f64),
            Some(1.0),
            "staleCandidateTtlSeconds schema must reject zero"
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

    // -----------------------------------------------------------------------
    // ConsumerConfig tests
    // -----------------------------------------------------------------------

    #[test]
    fn consumer_config_absent_deserializes_to_none() {
        let json = serde_json::json!({"name": "gw", "namespace": "ns"});
        let gw: GatewayRef = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert!(
            gw.consumer_config.is_none(),
            "absent consumerConfig must deserialize to None"
        );
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "round-trip test covers all ConsumerConfig fields")]
    fn consumer_config_enabled_round_trips() {
        let json = serde_json::json!({
            "name": "gw",
            "namespace": "ns",
            "consumerConfig": {
                "enabled": true,
                "credentialMountBase": "/run/secrets/grid",
                "configMapName": "my-consumer-config",
                "tlsCertMountPath": "/etc/custom-tls",
                "clusterEndpoints": [{
                    "cluster": "gateway-site-a",
                    "address": "10.0.0.10:30080",
                    "transport": {
                        "mode": "mutual_tls",
                        "sni": "site-a.grid.internal"
                    }
                }]
            }
        });
        let gw: GatewayRef = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let cc = gw.consumer_config.unwrap_or_else(|| std::process::abort());
        assert!(cc.enabled, "enabled must round-trip");
        assert_eq!(
            cc.credential_mount_base, "/run/secrets/grid",
            "credentialMountBase must round-trip"
        );
        assert_eq!(
            cc.config_map_name, "my-consumer-config",
            "configMapName must round-trip"
        );
        assert_eq!(
            cc.tls_cert_mount_path, "/etc/custom-tls",
            "tlsCertMountPath must round-trip"
        );
        let endpoint = cc.cluster_endpoints.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(cc.cluster_endpoints.len(), 1, "clusterEndpoints must round-trip");
        assert_eq!(endpoint.cluster, "gateway-site-a");
        assert_eq!(endpoint.address, "10.0.0.10:30080");
        let transport = endpoint.transport.as_ref().unwrap_or_else(|| std::process::abort());
        assert_eq!(
            transport.mode,
            TransportMode::MutualTls,
            "transport mode must round-trip"
        );
        assert_eq!(
            transport.sni.as_deref(),
            Some("site-a.grid.internal"),
            "transport SNI must round-trip"
        );
    }

    #[test]
    fn transport_mode_plaintext_round_trips() {
        let json = serde_json::json!({
            "cluster": "api-cluster",
            "address": "mock-api.default.svc:8080",
            "transport": { "mode": "plaintext" }
        });
        let ep: ClusterEndpointConfig = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let transport = ep.transport.as_ref().unwrap_or_else(|| std::process::abort());
        assert_eq!(
            transport.mode,
            TransportMode::Plaintext,
            "plaintext mode must round-trip"
        );
        assert!(transport.sni.is_none(), "plaintext must not require SNI");
    }

    #[test]
    fn transport_absent_deserializes_to_none() {
        let json = serde_json::json!({
            "cluster": "legacy-cluster",
            "address": "10.0.0.1:8080"
        });
        let ep: ClusterEndpointConfig = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert!(
            ep.transport.is_none(),
            "absent transport must deserialize to None (fails closed at render time)"
        );
    }

    #[test]
    fn consumer_config_defaults_when_subfields_absent() {
        let json = serde_json::json!({
            "name": "gw",
            "namespace": "ns",
            "consumerConfig": {}
        });
        let gw: GatewayRef = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let cc = gw.consumer_config.unwrap_or_else(|| std::process::abort());
        assert!(!cc.enabled, "enabled must default to false");
        assert_eq!(
            cc.credential_mount_base, "/run/secrets/grid-credentials",
            "credentialMountBase must use default"
        );
        assert_eq!(
            cc.config_map_name, "praxis-consumer-config",
            "configMapName must use default"
        );
        assert!(
            cc.cluster_endpoints.is_empty(),
            "clusterEndpoints must default to empty"
        );
        assert_eq!(
            cc.tls_cert_mount_path, "/etc/praxis/tls",
            "tlsCertMountPath must use default"
        );
    }

    #[test]
    fn consumer_config_absent_not_serialized() {
        let gw = GatewayRef {
            name: "gw".to_owned(),
            namespace: "ns".to_owned(),
            local_site_name: None,
            consumer_config: None,
        };
        let json = serde_json::to_value(&gw).unwrap_or_else(|_| std::process::abort());
        assert!(
            json.get("consumerConfig").is_none(),
            "absent consumerConfig must not appear in serialized output"
        );
    }

    #[test]
    fn grid_network_crd_has_consumer_config_field_on_gateway_ref() {
        let crd = crd_json();
        let gateway_ref_properties = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/gatewayRefs/items/properties")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            gateway_ref_properties.contains_key("consumerConfig"),
            "CRD schema must include consumerConfig field on GatewayRef"
        );
        let consumer_config_properties = gateway_ref_properties
            .get("consumerConfig")
            .and_then(|v| v.pointer("/properties"))
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            consumer_config_properties.contains_key("clusterEndpoints"),
            "CRD schema must include consumerConfig.clusterEndpoints"
        );
        assert!(
            consumer_config_properties.contains_key("tlsCertMountPath"),
            "CRD schema must include consumerConfig.tlsCertMountPath"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "CRD schema test covers transport type, mode enum values, and sni field"
    )]
    fn grid_network_crd_has_transport_schema_on_cluster_endpoints() {
        let crd = crd_json();
        let endpoint_properties = crd
            .pointer(
                "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties\
                 /gatewayRefs/items/properties/consumerConfig/properties\
                 /clusterEndpoints/items/properties",
            )
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());

        assert!(
            endpoint_properties.contains_key("transport"),
            "CRD schema must include transport field on clusterEndpoints items"
        );

        let transport_properties = endpoint_properties
            .get("transport")
            .and_then(|v| v.pointer("/properties"))
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());

        assert!(
            transport_properties.contains_key("mode"),
            "CRD schema must include transport.mode"
        );
        assert!(
            transport_properties.contains_key("sni"),
            "CRD schema must include transport.sni"
        );

        let mode_enum = transport_properties
            .get("mode")
            .and_then(|v| v.get("enum"))
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| std::process::abort());

        let mode_values: Vec<&str> = mode_enum.iter().filter_map(serde_json::Value::as_str).collect();

        assert!(
            mode_values.contains(&"mutual_tls"),
            "transport.mode enum must include mutual_tls: {mode_values:?}"
        );
        assert!(
            mode_values.contains(&"plaintext"),
            "transport.mode enum must include plaintext: {mode_values:?}"
        );
        assert_eq!(
            mode_values.len(),
            2,
            "transport.mode enum must have exactly 2 values: {mode_values:?}"
        );
    }
}
