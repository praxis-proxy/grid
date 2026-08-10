//! [`GridNetwork`] custom resource definition.
//!
//! The top-level tenancy boundary for the AI Grid. A cluster
//! can host multiple `GridNetworks` for multi-tenancy.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Routing policy
// ---------------------------------------------------------------------------

/// Candidate ordering policy for the routing overlay.
///
/// Controls whether geography (locality tier) or the scoring engine's
/// weighted total score is the primary differentiator when ranking
/// candidates after admission state and freshness.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum RoutingPolicy {
    /// Locality tier outranks score.
    ///
    /// Candidates on the same site always rank above remote candidates
    /// regardless of runtime metrics.  This is the production default
    /// and preserves the behaviour of grids created before this field
    /// existed.
    #[default]
    GeographyFirst,

    /// Score outranks locality tier.
    ///
    /// A remote candidate with a higher score can outrank
    /// a local candidate.  Use this when runtime metrics (queue depth,
    /// KV-cache pressure, latency) should drive routing decisions across
    /// sites.
    ScoreFirst,
}

// ---------------------------------------------------------------------------
// Scoring policy
// ---------------------------------------------------------------------------

/// Provider-level strategy used to order inference pools.
///
/// Grid follows llm-d's scorer model: the operator selects one independently
/// meaningful signal instead of blending unrelated objectives into an opaque
/// total. Request-specific decisions, such as prefix-cache affinity, remain in
/// the llm-d EPP after Grid has selected a provider pool.
///
/// When no [`ScoringPolicyConfig`] is set on the [`GridNetworkSpec`], dynamic
/// metric scoring is disabled. This supports external APIs and ordinary
/// providers that do not expose comparable EPP telemetry.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ScoringStrategy {
    /// Do not prefer providers using dynamic metrics.
    ///
    /// All score contributions are zero. Health, admission, freshness,
    /// geography, selection tiers, session affinity, and request-time picker
    /// policy still apply. This is the generic default.
    #[default]
    NoMetrics,

    /// Prefer the provider pool with the shortest normalized queue.
    ///
    /// This load-aware strategy corresponds to llm-d's `queue-scorer`. Lower
    /// queue pressure produces a higher score.
    QueueDepth,

    /// Prefer the provider pool with the most available KV-cache capacity.
    ///
    /// This corresponds to llm-d's `kv-cache-utilization-scorer`: lower
    /// utilization produces a higher score. It is a capacity-pressure signal,
    /// not evidence that the current request's prefix is cached.
    KvCachePressure,
}

impl ScoringStrategy {
    /// Adapts the selected strategy to the existing scoring engine.
    #[must_use]
    pub fn weights(self) -> scoring::ScoringWeights {
        match self {
            Self::NoMetrics => scoring::ScoringWeights {
                locality: 0.0,
                queue_depth: 0.0,
                kv_cache: 0.0,
                prefix_cache: 0.0,
                latency: 0.0,
                cost: 0.0,
            },
            Self::QueueDepth => scoring::ScoringWeights {
                locality: 0.0,
                queue_depth: 1.0,
                kv_cache: 0.0,
                prefix_cache: 0.0,
                latency: 0.0,
                cost: 0.0,
            },
            Self::KvCachePressure => scoring::ScoringWeights {
                locality: 0.0,
                queue_depth: 0.0,
                kv_cache: 1.0,
                prefix_cache: 0.0,
                latency: 0.0,
                cost: 0.0,
            },
        }
    }
}

/// Scoring policy configuration for the routing overlay.
///
/// Selects exactly one provider-level signal. When this field is absent from
/// [`GridNetworkSpec`], `noMetrics` is used.
///
/// # Examples
///
/// ```yaml
/// # Generic default (equivalent to omitting scoringPolicy):
/// scoringPolicy:
///   strategy: noMetrics
///
/// # Opt into llm-d load-aware scoring:
/// scoringPolicy:
///   strategy: queueDepth
///
/// # Or prefer available KV-cache capacity:
/// scoringPolicy:
///   strategy: kvCachePressure
/// ```
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ScoringPolicyConfig {
    /// Provider-level scoring strategy.
    ///
    /// Required when `scoringPolicy` is present. Omit the entire policy to use
    /// the `noMetrics` default.
    pub strategy: ScoringStrategy,
}

/// Resolve the effective [`scoring::ScoringWeights`] from a scoring policy.
///
/// The public API selects one scorer. The weight adapter is internal and
/// keeps the existing scoring engine and score-breakdown contract intact.
pub fn resolve_scoring_weights(policy: Option<&ScoringPolicyConfig>) -> scoring::ScoringWeights {
    policy
        .map_or_else(ScoringStrategy::default, |policy| policy.strategy)
        .weights()
}

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

    /// Candidate ordering policy for the routing overlay.
    ///
    /// **`geographyFirst`** (default): locality tier outranks the scoring
    /// engine's weighted score.  A same-site candidate always ranks above
    /// a remote candidate regardless of runtime metrics.
    ///
    /// **`scoreFirst`**: the scoring engine's weighted total score
    /// outranks locality tier.  A remote candidate with better runtime
    /// metrics (lower queue depth, lower KV-cache pressure) can outrank
    /// a same-site candidate.
    ///
    /// Admission state (`newAndExisting` before `existingOnly`) always
    /// outranks both geography and score in either mode.  In `scoreFirst`
    /// mode, freshness also outranks both; in `geographyFirst` mode,
    /// freshness is a tiebreaker below geography and score.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_policy: Option<RoutingPolicy>,

    /// Scoring policy configuration.
    ///
    /// Selects how the operator scores providers for the routing overlay.
    ///
    /// **Default (absent):** the `noMetrics` strategy is used.
    ///
    /// See [`ScoringStrategy`] for the available provider-level strategies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scoring_policy: Option<ScoringPolicyConfig>,

    /// Maximum time between metric refreshes and score/ranking recalculation.
    ///
    /// This controls the `GridNetwork` reconcile cadence for provider metrics;
    /// it does not change request-path routing or the overlay watch latency.
    /// Use a duration of at least one second, such as `"10s"` or `"1500ms"`.
    /// The default cadence is 300 seconds. TLS-protected provider metrics cap
    /// the cadence at 60 seconds for bounded certificate-rotation detection.
    #[schemars(regex(pattern = "^([1-9][0-9]*s|[1-9][0-9]{3,}ms)$"))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_refresh_interval: Option<String>,

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

    /// Local site name for the `intelligent_route` overlay generated for this gateway.
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
    /// `intelligent_route` candidates (with credential `secretRef` data), a
    /// `credential_inject` section for credential-bearing candidates, and a
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
/// `intelligent_route` candidates, `credential_inject` (when credential-bearing
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
    #[schemars(length(min = 1))]
    pub name: String,

    /// Secret namespace.
    #[schemars(length(min = 1))]
    pub namespace: String,

    /// Key within the Secret's `data` map.
    ///
    /// Required when the Secret holds multiple keys (e.g. credential references
    /// in `InferenceProvider.spec.auth.secretRef`).  Omit only when the entire
    /// Secret is consumed (e.g. TLS `ca_secret_ref`).
    #[schemars(length(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Observed status of a [`GridNetwork`].
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
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

    /// Per-gateway overlay revision status.
    ///
    /// Populated after each overlay reconcile attempt. Captures rendered and
    /// distributed revisions so operators can verify propagation without
    /// inspecting `ConfigMap` contents. A failed update retains evidence for
    /// the last successfully distributed revision.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub overlay_status: Vec<OverlayRevisionStatus>,
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

/// Lifecycle phase of a per-gateway overlay status entry.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum OverlayPhase {
    /// No overlay distribution result has been observed.
    #[default]
    Pending,
    /// Overlay rendered and distributed through the `ConfigMap`.
    Distributed,
    /// Overlay render or apply failed.
    Error,
    /// Previous valid overlay retained (empty candidates or apply failure).
    Retained,
}

/// Per-gateway overlay revision status for observability.
///
/// Reported in [`GridNetworkStatus::overlay_status`] for each gateway
/// after each reconcile attempt.
///
/// # Security
///
/// `rendered_revision`, `distributed_revision`, and `content_digest` are
/// SHA-256 hex digests — they do not contain credential token bytes.
/// `message` must never contain credential bytes.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverlayRevisionStatus {
    /// Name of the `GatewayRef` this status entry corresponds to.
    pub gateway_name: String,

    /// Namespace of the gateway (and the overlay `ConfigMap`).
    pub namespace: String,

    /// Name of the overlay `ConfigMap`.
    pub config_map_name: String,

    /// Envelope schema version.
    pub schema_version: String,

    /// Semantic revision (SHA-256 hex) of the last valid rendered overlay.
    pub rendered_revision: String,

    /// Semantic revision (SHA-256 hex) last distributed through the
    /// `ConfigMap`.
    pub distributed_revision: String,

    /// Content digest (SHA-256 hex) of the rendered overlay.
    pub content_digest: String,

    /// Kubernetes `resourceVersion` of the distributed `ConfigMap`.
    #[serde(default)]
    pub config_map_resource_version: String,

    /// RFC 3339 timestamp when the overlay was rendered.
    #[serde(default)]
    pub rendered_at: String,

    /// Number of candidates in the rendered overlay.
    #[serde(default)]
    pub candidate_count: u32,

    /// Current overlay lifecycle phase.
    #[serde(default)]
    pub phase: OverlayPhase,

    /// Machine-readable reason for the current phase.
    ///
    /// Empty when `phase` is [`OverlayPhase::Distributed`].
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

    // -----------------------------------------------------------------------
    // RoutingPolicy tests
    // -----------------------------------------------------------------------

    #[test]
    fn routing_policy_defaults_to_none_when_absent() {
        let json = serde_json::json!({
            "gridId": "",
            "seeds": [],
            "swim": {}
        });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert!(
            spec.routing_policy.is_none(),
            "absent routingPolicy must default to None"
        );
    }

    #[test]
    fn routing_policy_geography_first_round_trips() {
        let json = serde_json::json!({
            "gridId": "",
            "seeds": [],
            "routingPolicy": "geographyFirst"
        });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            spec.routing_policy,
            Some(RoutingPolicy::GeographyFirst),
            "geographyFirst must round-trip through serde"
        );
    }

    #[test]
    fn routing_policy_score_first_round_trips() {
        let json = serde_json::json!({
            "gridId": "",
            "seeds": [],
            "routingPolicy": "scoreFirst"
        });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            spec.routing_policy,
            Some(RoutingPolicy::ScoreFirst),
            "scoreFirst must round-trip through serde"
        );
    }

    #[test]
    fn routing_policy_absent_not_serialized() {
        let json = serde_json::json!({ "gridId": "", "seeds": [] });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let serialized = serde_json::to_value(&spec).unwrap_or_else(|_| std::process::abort());
        assert!(
            serialized.get("routingPolicy").is_none(),
            "absent routingPolicy must not appear in serialized output"
        );
    }

    #[test]
    fn routing_policy_appears_in_crd_schema() {
        let crd = crd_json();
        let schema = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/routingPolicy")
            .unwrap_or_else(|| std::process::abort());
        assert!(schema.is_object(), "routingPolicy must appear in the CRD schema");
        let enum_values = schema
            .get("enum")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| std::process::abort());
        let values: Vec<&str> = enum_values.iter().filter_map(serde_json::Value::as_str).collect();
        assert!(
            values.contains(&"geographyFirst"),
            "CRD schema must include geographyFirst: {values:?}"
        );
        assert!(
            values.contains(&"scoreFirst"),
            "CRD schema must include scoreFirst: {values:?}"
        );
    }

    #[test]
    fn routing_policy_default_is_geography_first() {
        assert_eq!(
            RoutingPolicy::default(),
            RoutingPolicy::GeographyFirst,
            "default RoutingPolicy must be GeographyFirst"
        );
    }

    #[test]
    fn overlay_phase_default() {
        assert_eq!(OverlayPhase::default(), OverlayPhase::Pending);
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "full-field struct construction and assertion")]
    fn overlay_status_distributed_serialization() {
        let status = OverlayRevisionStatus {
            gateway_name: "gw".to_owned(),
            namespace: "ns".to_owned(),
            config_map_name: "cm".to_owned(),
            schema_version: "1.0.0".to_owned(),
            rendered_revision: "a".repeat(64),
            distributed_revision: "a".repeat(64),
            content_digest: "a".repeat(64),
            config_map_resource_version: "123".to_owned(),
            rendered_at: "2026-07-29T00:00:00Z".to_owned(),
            candidate_count: 2,
            phase: OverlayPhase::Distributed,
            reason: String::new(),
            message: String::new(),
            observed_generation: 1,
        };
        let json = serde_json::to_value(&status).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            json.get("phase").and_then(serde_json::Value::as_str),
            Some("Distributed"),
            "phase must be Distributed"
        );
        assert_eq!(
            json.get("contentDigest").and_then(serde_json::Value::as_str),
            Some("a".repeat(64)).as_deref(),
            "contentDigest must match"
        );
        assert_eq!(
            json.get("renderedAt").and_then(serde_json::Value::as_str),
            Some("2026-07-29T00:00:00Z"),
            "renderedAt must be present"
        );
        let deser: OverlayRevisionStatus = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(deser, status);
    }

    #[test]
    fn overlay_status_error_serialization() {
        let status = OverlayRevisionStatus {
            gateway_name: "gw".to_owned(),
            namespace: "ns".to_owned(),
            config_map_name: "cm".to_owned(),
            schema_version: String::new(),
            rendered_revision: String::new(),
            distributed_revision: String::new(),
            content_digest: String::new(),
            config_map_resource_version: String::new(),
            rendered_at: "2026-07-29T00:00:00Z".to_owned(),
            candidate_count: 0,
            phase: OverlayPhase::Error,
            reason: "ApplyFailed".to_owned(),
            message: "failed to apply ConfigMap".to_owned(),
            observed_generation: 1,
        };
        let json = serde_json::to_value(&status).unwrap_or_else(|_| std::process::abort());
        assert_eq!(json.get("phase").and_then(serde_json::Value::as_str), Some("Error"),);
        assert_eq!(
            json.get("reason").and_then(serde_json::Value::as_str),
            Some("ApplyFailed"),
        );
    }

    #[test]
    fn overlay_status_retained_serialization() {
        let status = OverlayRevisionStatus {
            gateway_name: "gw".to_owned(),
            namespace: "ns".to_owned(),
            config_map_name: "cm".to_owned(),
            schema_version: String::new(),
            rendered_revision: String::new(),
            distributed_revision: String::new(),
            content_digest: String::new(),
            config_map_resource_version: String::new(),
            rendered_at: "2026-07-29T00:00:00Z".to_owned(),
            candidate_count: 0,
            phase: OverlayPhase::Retained,
            reason: "EmptyCandidates".to_owned(),
            message: "no candidates available; previous valid overlay retained".to_owned(),
            observed_generation: 1,
        };
        let json = serde_json::to_value(&status).unwrap_or_else(|_| std::process::abort());
        assert_eq!(json.get("phase").and_then(serde_json::Value::as_str), Some("Retained"),);
        assert_eq!(
            json.get("reason").and_then(serde_json::Value::as_str),
            Some("EmptyCandidates"),
        );
    }

    // -----------------------------------------------------------------------
    // ScoringPolicy tests
    // -----------------------------------------------------------------------

    #[test]
    fn scoring_strategy_default_is_no_metrics() {
        assert_eq!(
            ScoringStrategy::default(),
            ScoringStrategy::NoMetrics,
            "default strategy must be noMetrics"
        );
    }

    #[test]
    fn scoring_policy_absent_defaults_to_none() {
        let json = serde_json::json!({ "gridId": "", "seeds": [] });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert!(
            spec.scoring_policy.is_none(),
            "absent scoringPolicy must default to None"
        );
    }

    #[test]
    fn scoring_policy_queue_depth_round_trips() {
        let json = serde_json::json!({
            "gridId": "",
            "seeds": [],
            "scoringPolicy": { "strategy": "queueDepth" }
        });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let policy = spec.scoring_policy.unwrap_or_else(|| std::process::abort());
        assert_eq!(
            policy.strategy,
            ScoringStrategy::QueueDepth,
            "queueDepth strategy must round-trip"
        );
    }

    #[test]
    fn scoring_policy_no_metrics_round_trips() {
        let json = serde_json::json!({
            "gridId": "",
            "seeds": [],
            "scoringPolicy": { "strategy": "noMetrics" }
        });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let policy = spec.scoring_policy.unwrap_or_else(|| std::process::abort());
        assert_eq!(
            policy.strategy,
            ScoringStrategy::NoMetrics,
            "noMetrics strategy must round-trip"
        );
    }

    #[test]
    fn scoring_policy_kv_cache_pressure_round_trips() {
        let json = serde_json::json!({
            "gridId": "",
            "seeds": [],
            "scoringPolicy": { "strategy": "kvCachePressure" }
        });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let policy = spec.scoring_policy.unwrap_or_else(|| std::process::abort());
        assert_eq!(
            policy.strategy,
            ScoringStrategy::KvCachePressure,
            "kvCachePressure strategy must round-trip"
        );
    }

    #[test]
    fn scoring_policy_rejects_removed_profile_shape() {
        let json = serde_json::json!({
            "gridId": "",
            "seeds": [],
            "scoringPolicy": { "profile": "balanced" }
        });
        let result = serde_json::from_value::<GridNetworkSpec>(json);
        assert!(result.is_err(), "the removed profile/weights API must be rejected");
    }

    #[test]
    fn scoring_policy_absent_not_serialized() {
        let json = serde_json::json!({ "gridId": "", "seeds": [] });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let serialized = serde_json::to_value(&spec).unwrap_or_else(|_| std::process::abort());
        assert!(
            serialized.get("scoringPolicy").is_none(),
            "absent scoringPolicy must not appear in serialized output"
        );
    }

    #[test]
    fn scoring_policy_appears_in_crd_schema() {
        let crd = crd_json();
        let schema = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/scoringPolicy")
            .unwrap_or_else(|| std::process::abort());
        assert!(schema.is_object(), "scoringPolicy must appear in the CRD schema");
    }

    #[test]
    fn scoring_strategy_enum_in_crd_schema() {
        let crd = crd_json();
        let strategy_schema = crd
            .pointer(
                "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/scoringPolicy/properties/strategy",
            )
            .unwrap_or_else(|| std::process::abort());
        let enum_values = strategy_schema
            .get("enum")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| std::process::abort());
        let values: Vec<&str> = enum_values.iter().filter_map(serde_json::Value::as_str).collect();
        assert!(
            values.contains(&"noMetrics"),
            "CRD enum must include noMetrics: {values:?}"
        );
        assert!(
            values.contains(&"queueDepth"),
            "CRD enum must include queueDepth: {values:?}"
        );
        assert!(
            values.contains(&"kvCachePressure"),
            "CRD enum must include kvCachePressure: {values:?}"
        );
        assert_eq!(values.len(), 3, "only the three supported strategies belong in the CRD");
    }

    #[test]
    fn scoring_strategy_is_required_when_policy_is_present() {
        let crd = crd_json();
        let required = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/scoringPolicy/required")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(required, &[serde_json::Value::String("strategy".to_owned())]);
    }

    #[test]
    fn metrics_refresh_interval_schema_requires_positive_duration_of_at_least_one_second() {
        let crd = crd_json();
        let schema = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/metricsRefreshInterval")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(schema.get("type").and_then(serde_json::Value::as_str), Some("string"));
        assert_eq!(
            schema.get("pattern").and_then(serde_json::Value::as_str),
            Some("^([1-9][0-9]*s|[1-9][0-9]{3,}ms)$")
        );
    }

    // -----------------------------------------------------------------------
    // resolve_scoring_weights tests
    // -----------------------------------------------------------------------

    fn assert_weight(actual: f64, expected: f64, label: &str) {
        assert!(
            (actual - expected).abs() < f64::EPSILON,
            "{label}: expected {expected}, got {actual}"
        );
    }

    #[test]
    fn resolve_weights_none_disables_all_signals() {
        let w = resolve_scoring_weights(None);
        assert_weight(w.queue_depth, 0.0, "queue_depth");
        assert_weight(w.locality, 0.0, "locality");
        assert_weight(w.kv_cache, 0.0, "kv_cache");
        assert_weight(w.prefix_cache, 0.0, "prefix_cache");
        assert_weight(w.latency, 0.0, "latency");
        assert_weight(w.cost, 0.0, "cost");
    }

    #[test]
    fn resolve_weights_explicit_no_metrics_disables_all_signals() {
        let policy = ScoringPolicyConfig {
            strategy: ScoringStrategy::NoMetrics,
        };
        let w = resolve_scoring_weights(Some(&policy));
        assert_weight(w.queue_depth, 0.0, "queue_depth");
        assert_weight(w.locality, 0.0, "locality");
        assert_weight(w.kv_cache, 0.0, "kv_cache");
        assert_weight(w.prefix_cache, 0.0, "prefix_cache");
        assert_weight(w.latency, 0.0, "latency");
        assert_weight(w.cost, 0.0, "cost");
    }

    #[test]
    fn resolve_weights_explicit_queue_depth_all_others_zero() {
        let policy = ScoringPolicyConfig {
            strategy: ScoringStrategy::QueueDepth,
        };
        let w = resolve_scoring_weights(Some(&policy));
        assert_weight(w.queue_depth, 1.0, "queue_depth");
        assert_weight(w.locality, 0.0, "locality");
        assert_weight(w.kv_cache, 0.0, "kv_cache");
        assert_weight(w.prefix_cache, 0.0, "prefix_cache");
        assert_weight(w.latency, 0.0, "latency");
        assert_weight(w.cost, 0.0, "cost");
    }

    #[test]
    fn resolve_weights_kv_cache_pressure_all_others_zero() {
        let policy = ScoringPolicyConfig {
            strategy: ScoringStrategy::KvCachePressure,
        };
        let w = resolve_scoring_weights(Some(&policy));
        assert_weight(w.kv_cache, 1.0, "kv_cache");
        assert_weight(w.queue_depth, 0.0, "queue_depth");
        assert_weight(w.locality, 0.0, "locality");
        assert_weight(w.prefix_cache, 0.0, "prefix_cache");
        assert_weight(w.latency, 0.0, "latency");
        assert_weight(w.cost, 0.0, "cost");
    }

    #[test]
    fn scoring_policy_rejects_unknown_strategy() {
        let json = serde_json::json!({
            "gridId": "",
            "seeds": [],
            "scoringPolicy": { "strategy": "prefixAware" }
        });
        let result = serde_json::from_value::<GridNetworkSpec>(json);
        assert!(result.is_err(), "unknown strategy must be rejected");
    }

    #[test]
    fn scoring_policy_rejects_removed_weights_field() {
        let json = serde_json::json!({
            "gridId": "",
            "seeds": [],
            "scoringPolicy": {
                "strategy": "queueDepth",
                "weights": { "locality": 1.0 }
            }
        });
        let result = serde_json::from_value::<GridNetworkSpec>(json);
        assert!(result.is_err(), "weights field must be rejected by deny_unknown_fields");
    }

    // -----------------------------------------------------------------------
    // Hand-calculated scoring assertions
    // -----------------------------------------------------------------------

    fn test_backend(name: &str) -> scoring::BackendConfig {
        scoring::BackendConfig::new(
            name.to_owned(),
            0.0,
            0.0,
            format!("http://{name}:8000"),
            scoring::BackendKind::Local,
            scoring::ProviderKind::OpenAi,
            Some("us-east-1".to_owned()),
        )
    }

    fn assert_all_zero_except(b: &scoring::ScoreBreakdown, active: &str) {
        if active != "queue_depth" {
            assert_weight(b.queue_depth, 0.0, "queue_depth must be zero");
        }
        if active != "kv_cache" {
            assert_weight(b.kv_cache, 0.0, "kv_cache must be zero");
        }
        assert_weight(b.locality, 0.0, "locality must be zero");
        assert_weight(b.prefix_cache, 0.0, "prefix_cache must be zero");
        assert_weight(b.latency, 0.0, "latency must be zero");
        assert_weight(b.cost, 0.0, "cost must be zero");
    }

    #[test]
    fn queue_depth_strategy_hand_calculated_score() {
        let weights = ScoringStrategy::QueueDepth.weights();
        let mut state = scoring::GridState::new();
        state
            .add_backend(test_backend("pool-a"))
            .unwrap_or_else(|_| std::process::abort());
        state.set_metrics(
            "pool-a".to_owned(),
            scoring::BackendMetrics::new(0.0, true, 0.50, 100.0, 0.0, 0.25),
        );

        let scored = scoring::score_backends(&state, &weights, Some("us-east-1"));
        let s = scored.first().unwrap_or_else(|| std::process::abort());

        assert_weight(s.breakdown.queue_depth, 0.75, "queue: 1.0*(1.0-0.25)");
        assert_weight(s.breakdown.total, 0.75, "total score");
        assert_all_zero_except(&s.breakdown, "queue_depth");
    }

    #[test]
    fn kv_cache_pressure_strategy_hand_calculated_score() {
        let weights = ScoringStrategy::KvCachePressure.weights();
        let mut state = scoring::GridState::new();
        state
            .add_backend(test_backend("pool-a"))
            .unwrap_or_else(|_| std::process::abort());
        state.set_metrics(
            "pool-a".to_owned(),
            scoring::BackendMetrics::new(0.0, true, 0.60, 100.0, 0.0, 0.10),
        );

        let scored = scoring::score_backends(&state, &weights, Some("us-east-1"));
        let s = scored.first().unwrap_or_else(|| std::process::abort());

        assert_weight(s.breakdown.kv_cache, 0.40, "kv: 1.0*(1.0-0.60)");
        assert_weight(s.breakdown.total, 0.40, "total score");
        assert_all_zero_except(&s.breakdown, "kv_cache");
    }

    // -----------------------------------------------------------------------
    // Opposing-signal ordering: prove strategies are not combined
    // -----------------------------------------------------------------------

    fn opposing_signal_state() -> scoring::GridState {
        let mut state = scoring::GridState::new();
        state
            .add_backend(test_backend("pool-a"))
            .unwrap_or_else(|_| std::process::abort());
        state
            .add_backend(test_backend("pool-b"))
            .unwrap_or_else(|_| std::process::abort());
        state.set_metrics(
            "pool-a".to_owned(),
            scoring::BackendMetrics::new(0.0, true, 0.80, 100.0, 0.0, 0.10),
        );
        state.set_metrics(
            "pool-b".to_owned(),
            scoring::BackendMetrics::new(0.0, true, 0.20, 100.0, 0.0, 0.90),
        );
        state
    }

    #[test]
    fn no_metrics_strategy_ignores_runtime_metric_differences() {
        let state = opposing_signal_state();
        let weights = ScoringStrategy::NoMetrics.weights();
        let scored = scoring::score_backends(&state, &weights, Some("us-east-1"));

        assert_eq!(scored.len(), 2);
        for backend in scored {
            assert_weight(backend.breakdown.total, 0.0, "total");
            assert_all_zero_except(&backend.breakdown, "none");
        }
    }

    #[test]
    fn queue_depth_strategy_prefers_shorter_queue_despite_worse_kv() {
        let state = opposing_signal_state();
        let weights = ScoringStrategy::QueueDepth.weights();
        let scored = scoring::score_backends(&state, &weights, Some("us-east-1"));

        let first = scored.first().unwrap_or_else(|| std::process::abort());
        let second = scored.get(1).unwrap_or_else(|| std::process::abort());
        assert_eq!(first.name, "pool-a", "pool-a has shorter queue and must rank first");
        assert_eq!(second.name, "pool-b");
        assert_weight(first.breakdown.total, 0.90, "pool-a: 1-0.10");
        assert_weight(second.breakdown.total, 0.10, "pool-b: 1-0.90");
        assert_weight(first.breakdown.kv_cache, 0.0, "kv must not contribute");
        assert_weight(second.breakdown.kv_cache, 0.0, "kv must not contribute");
    }

    #[test]
    fn kv_cache_pressure_strategy_prefers_lower_kv_despite_worse_queue() {
        let state = opposing_signal_state();
        let weights = ScoringStrategy::KvCachePressure.weights();
        let scored = scoring::score_backends(&state, &weights, Some("us-east-1"));

        let first = scored.first().unwrap_or_else(|| std::process::abort());
        let second = scored.get(1).unwrap_or_else(|| std::process::abort());
        assert_eq!(first.name, "pool-b", "pool-b has lower KV and must rank first");
        assert_eq!(second.name, "pool-a");
        assert_weight(first.breakdown.total, 0.80, "pool-b: 1-0.20");
        assert_weight(second.breakdown.total, 0.20, "pool-a: 1-0.80");
        assert_weight(first.breakdown.queue_depth, 0.0, "queue must not contribute");
        assert_weight(second.breakdown.queue_depth, 0.0, "queue must not contribute");
    }
}
