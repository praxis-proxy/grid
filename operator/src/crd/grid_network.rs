//! [`GridNetwork`] custom resource definition.
//!
//! The top-level tenancy boundary for the AI Grid. A cluster
//! can host multiple `GridNetworks` for multi-tenancy.

use std::collections::BTreeMap;

use crdt::GCounter;
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
    /// geography, selection tiers, session affinity, and request-time selection
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

/// Local request-selection mode applied by Praxis inside the active group.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SelectionMode {
    /// Select the first admitted candidate in the active group.
    #[default]
    Deterministic,
    /// Distribute new requests equally in the active group.
    RoundRobin,
    /// Distribute new requests randomly in the active group.
    Random,
    /// Select candidates proportionally to their configured capacity.
    WeightedRandom,
}

/// Request selection policy published in the routing overlay.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct SelectionPolicyConfig {
    /// Local selection mode used by the data-plane gateway.
    pub mode: SelectionMode,
}

/// How signals propagate across the sites of a grid.
///
/// The mode names the dissemination path, not the transport: SWIM membership
/// runs in both modes. Only where the load signal travels changes.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize, clap::ValueEnum)]
#[serde(rename_all = "camelCase")]
pub enum SignalMode {
    /// Propagate signals over the SWIM and CRDT dissemination overlay, with
    /// local scoring on. The established behaviour.
    #[default]
    Gossip,
    /// Propagate signals by direct mTLS pull of each peer's signals endpoint,
    /// with local scoring off.
    Poll,
}

/// Grid-wide signal transport.
///
/// A property of the grid, not of one operator: every site propagates the same
/// way. Absent, the grid gossips, which is non-breaking. A mode change takes
/// effect at operator start, so the operator restarts itself when it flips.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct SignalTransportConfig {
    /// Signal dissemination mode.
    pub mode: SignalMode,
}

/// How a peer site proves its identity beyond chaining to the Grid CA.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize, clap::ValueEnum)]
#[serde(rename_all = "camelCase")]
pub enum PeerTrustMode {
    /// Its leaf digest is declared on its `GridSite`.
    #[default]
    Pin,
    /// Its leaf carries `spiffe://grid.internal/site/<site>`, and no pins are read.
    Spiffe,
}

/// Grid-wide peer trust, the same at every site.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PeerTrustConfig {
    /// Peer trust mode.
    pub mode: PeerTrustMode,
}

/// Explicit static provider-capacity placement strategy.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PlacementStrategy {
    /// Use operator-configured provider capacity weights.
    Static,
}

/// Traffic placement policy. This static stack intentionally has no metric inputs.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PlacementPolicyConfig {
    /// Placement strategy.
    pub strategy: PlacementStrategy,
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
// Admission policy
// ---------------------------------------------------------------------------

/// Controls whether provider admission reacts immediately or uses bounded
/// hysteresis across reconciles.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum AdmissionMode {
    /// Apply the current observation immediately.  This is the compatibility
    /// behaviour used when `admissionPolicy` is omitted.
    #[default]
    Instantaneous,
    /// Require repeated pressure/recovery observations before changing state.
    Stabilized,
}

/// Fail-closed state to use when configured provider metrics are unavailable.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum MissingMetricsPolicy {
    /// Preserve existing sessions but do not admit new ones.
    #[default]
    ExistingOnly,
    /// Remove the provider from the published routing overlay.
    Excluded,
}

/// Pressure and recovery hysteresis configuration.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(
    deny_unknown_fields,
    extend("x-kubernetes-validations" = [{
        "rule": "self.exitThreshold < self.enterThreshold",
        "message": "exitThreshold must be less than enterThreshold"
    }])
)]
pub struct AdmissionPressureConfig {
    /// Pressure level at which a provider is considered saturated.
    #[schemars(range(min = 0.0, max = 1.0))]
    pub enter_threshold: f64,
    /// Lower pressure level required before recovery can begin.
    #[schemars(range(min = 0.0, max = 1.0))]
    pub exit_threshold: f64,
    /// Consecutive pressure observations required to enter `existingOnly`.
    #[schemars(range(min = 1, max = 100))]
    pub failure_threshold: u32,
    /// Consecutive healthy observations required to recover new admission.
    #[schemars(range(min = 1, max = 100))]
    pub success_threshold: u32,
    /// Minimum time a state must remain active before changing.
    #[schemars(regex(pattern = "^[1-9][0-9]*s$"))]
    pub minimum_state_duration: String,
    /// Additional time to hold a recovering provider in `existingOnly`.
    #[schemars(regex(pattern = "^[1-9][0-9]*s$"))]
    pub recovery_hold_down: String,
}

impl Default for AdmissionPressureConfig {
    fn default() -> Self {
        Self {
            enter_threshold: 0.85,
            exit_threshold: 0.70,
            failure_threshold: 2,
            success_threshold: 3,
            minimum_state_duration: "10s".to_owned(),
            recovery_hold_down: "30s".to_owned(),
        }
    }
}

/// Provider admission policy for routing overlays.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct AdmissionPolicyConfig {
    /// Admission evaluation mode.
    #[serde(default)]
    pub mode: AdmissionMode,
    /// Pressure and recovery thresholds.
    #[serde(default)]
    pub pressure: AdmissionPressureConfig,
    /// Fail-closed state for missing or expired metrics.
    #[serde(default = "default_missing_metrics_policy")]
    pub missing_metrics: MissingMetricsPolicy,
}

/// Return the fail-closed default for omitted `missingMetrics`.
fn default_missing_metrics_policy() -> MissingMetricsPolicy {
    MissingMetricsPolicy::ExistingOnly
}

// ---------------------------------------------------------------------------
// Budget policy
// ---------------------------------------------------------------------------

/// Per-tenant budget cap declaration.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct TenantBudgetConfig {
    /// Tenant identifier. Must be non-empty and unique within the policy.
    pub tenant_id: String,

    /// Maximum cumulative spend in USD before this tenant is considered over budget.
    ///
    /// **Minimum value:** `0`. The generated CRD schema rejects negative caps;
    /// [`validate_budget_policy`] additionally rejects `NaN`/infinite values
    /// that the schema's numeric minimum does not catch.
    #[schemars(range(min = 0.0))]
    pub cap_usd: f64,
}

/// Budget policy configuration for per-tenant spend tracking.
///
/// Declares the tenants Grid should track cumulative spend for. Grid tracks
/// and cross-site-converges the spend signal (via G-Counter CRDT, see
/// [`crdt::GridStateSnapshot::tenant_spend`]) and exposes it in
/// [`GridNetworkStatus::budget_status`]; it does **not** enforce budget
/// limits itself — degrade/reject decisions are a gateway-side `praxis-ai`
/// policy-filter concern, not a Grid-side one.
///
/// **Default (absent):** no tenants are tracked; `budgetStatus` is always empty.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct BudgetPolicyConfig {
    /// Per-tenant budget caps.
    #[serde(default)]
    pub tenants: Vec<TenantBudgetConfig>,
}

/// Reason a [`BudgetPolicyConfig`] failed validation.
///
/// The CRD schema's numeric minimum on `capUsd` (see [`TenantBudgetConfig`])
/// already rejects negative values at admission time; [`validate_budget_policy`]
/// is a defensive second layer for callers that construct or deserialize a
/// [`BudgetPolicyConfig`] outside the Kubernetes API path.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BudgetPolicyValidationError {
    /// A tenant's `capUsd` is negative.
    #[error("tenant {tenant_id:?} has a negative capUsd")]
    NegativeCap {
        /// Offending tenant identifier.
        tenant_id: String,
    },
    /// A tenant's `capUsd` is `NaN` or infinite.
    #[error("tenant {tenant_id:?} has a non-finite capUsd")]
    NonFiniteCap {
        /// Offending tenant identifier.
        tenant_id: String,
    },
    /// The same `tenantId` appears more than once.
    #[error("tenant id {tenant_id:?} appears more than once")]
    DuplicateTenant {
        /// The repeated tenant identifier.
        tenant_id: String,
    },
    /// A tenant entry has a blank (empty or whitespace-only) `tenantId`.
    #[error("a tenant entry has a blank tenantId")]
    BlankTenantId,
}

/// Validate a [`BudgetPolicyConfig`].
///
/// # Errors
///
/// Returns [`BudgetPolicyValidationError`] for the first invalid tenant entry
/// found, in declaration order: a blank `tenantId`, a duplicate `tenantId`, a
/// negative `capUsd`, or a non-finite `capUsd`.
pub fn validate_budget_policy(policy: &BudgetPolicyConfig) -> Result<(), BudgetPolicyValidationError> {
    let mut seen_tenant_ids = std::collections::HashSet::new();
    for tenant in &policy.tenants {
        if tenant.tenant_id.trim().is_empty() {
            return Err(BudgetPolicyValidationError::BlankTenantId);
        }
        if !seen_tenant_ids.insert(tenant.tenant_id.as_str()) {
            return Err(BudgetPolicyValidationError::DuplicateTenant {
                tenant_id: tenant.tenant_id.clone(),
            });
        }
        if !tenant.cap_usd.is_finite() {
            return Err(BudgetPolicyValidationError::NonFiniteCap {
                tenant_id: tenant.tenant_id.clone(),
            });
        }
        if tenant.cap_usd < 0.0 {
            return Err(BudgetPolicyValidationError::NegativeCap {
                tenant_id: tenant.tenant_id.clone(),
            });
        }
    }
    Ok(())
}

/// Convert a G-Counter total (cents, `u64`) into USD (`f64`).
#[expect(
    clippy::cast_precision_loss,
    reason = "budget ratio is inherently approximate under partition; see GCounter docs"
)]
pub(crate) fn cents_to_usd(cents: u64) -> f64 {
    cents as f64 / 100.0
}

/// Convert a tenant's cumulative spend counter into a cap-relative ratio.
///
/// `tenant_spend.total()` is denominated in cents (see
/// [`crdt::GridStateSnapshot::tenant_spend`]); `cap_usd` is dollars. The
/// result is always clamped to `0.0..=1.0`:
///
/// - `cap_usd <= 0.0` (including non-finite) is treated defensively as "no budget available" and always returns `1.0`,
///   regardless of spend. The CRD schema and [`validate_budget_policy`] should already prevent this, but a caller
///   bypassing both must not panic or divide by zero.
/// - Spend above the cap clamps to `1.0` rather than exceeding it — a real possibility, not a bug: G-Counter is
///   monotonic and an individual site sees only a lower bound under partition, so local overspend is expected.
#[must_use]
pub fn spend_ratio(tenant_spend: &GCounter, cap_usd: f64) -> f64 {
    if !cap_usd.is_finite() || cap_usd <= 0.0 {
        return 1.0;
    }
    (cents_to_usd(tenant_spend.total()) / cap_usd).clamp(0.0, 1.0)
}

/// Per-tenant budget status derived from policy + merged CRDT spend state.
///
/// Populated in [`GridNetworkStatus::budget_status`] for every tenant
/// declared in `spec.budgetPolicy`, regardless of whether spend has been
/// recorded for that tenant yet. This is a status signal only — Grid does
/// not enforce budget limits (see [`BudgetPolicyConfig`] doc).
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantBudgetStatus {
    /// Tenant identifier, matching `spec.budgetPolicy.tenants[].tenantId`.
    pub tenant_id: String,

    /// Cumulative spend observed for this tenant, in USD.
    ///
    /// Converged across all sites that have merged CRDT state for this
    /// tenant; may lag briefly during a partition (see [`GCounter`]).
    pub spend_usd: f64,

    /// Budget cap for this tenant, in USD, copied from `spec.budgetPolicy`.
    pub cap_usd: f64,

    /// `spend_usd / cap_usd`, clamped to `0.0..=1.0`. See [`spend_ratio`].
    pub spend_ratio: f64,
}

/// Build one tenant's status entry.
///
/// `counter` is `None` when no spend has been recorded for this tenant yet;
/// in that case both `spend_usd` and `spend_ratio` are `0.0` rather than
/// delegating to [`spend_ratio`] (which would read a non-positive cap as
/// maxed — not the right answer for "no traffic yet").
fn tenant_budget_status(tenant: &TenantBudgetConfig, counter: Option<&GCounter>) -> TenantBudgetStatus {
    let (spend_usd, ratio) = counter.map_or((0.0, 0.0), |counter| {
        (cents_to_usd(counter.total()), spend_ratio(counter, tenant.cap_usd))
    });
    TenantBudgetStatus {
        tenant_id: tenant.tenant_id.clone(),
        spend_usd,
        cap_usd: tenant.cap_usd,
        spend_ratio: ratio,
    }
}

/// Assemble per-tenant budget status from policy and merged CRDT spend state.
///
/// Driven by `policy.tenants`, not by `tenant_spend`: a tenant declared in
/// the policy but with no recorded spend yet still gets an entry
/// (`spendUsd: 0.0`); CRDT spend recorded for a tenant no longer declared in
/// the policy is silently excluded — the policy is the source of truth for
/// which tenants are tracked. Output is sorted by `tenantId` for
/// deterministic status ordering.
#[must_use]
pub fn tenant_spend_status(
    policy: &BudgetPolicyConfig,
    tenant_spend: &BTreeMap<String, GCounter>,
) -> Vec<TenantBudgetStatus> {
    let mut statuses: Vec<TenantBudgetStatus> = policy
        .tenants
        .iter()
        .map(|tenant| tenant_budget_status(tenant, tenant_spend.get(&tenant.tenant_id)))
        .collect();
    statuses.sort_by(|a, b| a.tenant_id.cmp(&b.tenant_id));
    statuses
}

/// Resolve tenant budget statuses for [`GridNetworkStatus::budget_status`].
///
/// `policy` is `None` when `spec.budgetPolicy` is absent — no tenants are
/// tracked, so the result is always empty in that case.
#[must_use]
pub fn resolve_budget_statuses(
    policy: Option<&BudgetPolicyConfig>,
    tenant_spend: &BTreeMap<String, GCounter>,
) -> Vec<TenantBudgetStatus> {
    policy.map_or_else(Vec::new, |policy| tenant_spend_status(policy, tenant_spend))
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
    group = "grid.praxis.fast",
    version = "v1alpha1",
    kind = "GridNetwork",
    plural = "gridnetworks",
    shortname = "gnw",
    status = "GridNetworkStatus",
    namespaced = false,
    printcolumn = r#"{"name":"Grid ID","type":"string","jsonPath":".status.gridId"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Sites","type":"integer","jsonPath":".status.connectedSites"}"#
)]
#[schemars(extend("x-kubernetes-validations" = [{
    "rule": "has(self.placementPolicy) == (has(self.selectionPolicy) && self.selectionPolicy.mode == 'weightedRandom')",
    "message": "placementPolicy must be set if and only if selectionPolicy.mode is weightedRandom"
}]))]
#[serde(rename_all = "camelCase")]
pub struct GridNetworkSpec {
    /// Grid ID for tenancy. Empty on creation; auto-generated
    /// on first join with another site.
    #[serde(default)]
    pub grid_id: String,

    /// Initial SWIM seed peer endpoints in host:port form. Literal IPv4, bracketed
    /// IPv6, and DNS hostnames are accepted.
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
    /// freshness is below locality but above score so it cannot interleave
    /// candidates across freshness-based selection groups.
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

    /// Provider admission and pressure-recovery policy.
    ///
    /// When omitted, Grid preserves the historical instantaneous admission
    /// behaviour and providers without metrics remain eligible. Stabilized
    /// pressure transitions require an active metrics scoring strategy and a
    /// matching provider signal name. Without those, the observation is
    /// `NotConfigured` and the provider remains `new_and_existing`; the
    /// `missingMetrics` policy applies only after a configured signal is
    /// missing or stale.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_policy: Option<AdmissionPolicyConfig>,

    /// Local request distribution policy for the active selection group.
    ///
    /// This is independent of scoring. When absent, the overlay carries no
    /// selection override and Praxis uses deterministic selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection_policy: Option<SelectionPolicyConfig>,

    /// Grid-wide signal transport.
    ///
    /// **Default (absent):** `gossip`, the established SWIM and CRDT overlay
    /// path. Set `mode: poll` to propagate signals by direct mTLS pull instead.
    /// The mode is read at operator start, so a change requires an operator
    /// restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal_transport: Option<SignalTransportConfig>,

    /// Grid-wide peer trust on the signals path, `pin` when absent, read at operator start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_trust: Option<PeerTrustConfig>,

    /// Optional static capacity-placement policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement_policy: Option<PlacementPolicyConfig>,

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

    /// Budget policy configuration for per-tenant spend tracking.
    ///
    /// Selects which tenants Grid tracks cumulative spend for. See
    /// [`BudgetPolicyConfig`] for what this does and does not do.
    ///
    /// **Default (absent):** no tenants are tracked; `budgetStatus` is
    /// always empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_policy: Option<BudgetPolicyConfig>,

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
    /// It does not delete `GridSite` objects: auto-discovered stubs that gossip
    /// stops vouching for are collected after a fixed 24 hours, whatever this
    /// field is. Declared `GridSite` objects are never deleted.
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

    /// Explicit mTLS provider-hop endpoints for the embedded `grid-gateway`
    /// serving config. This allowlist is independent of `consumerConfig`, so a
    /// missing or disabled generated Praxis config cannot alter embedded
    /// serving behavior. Every entry must declare `mutual_tls` and a nonblank
    /// SNI. The embedded gateway compares each declaration against the
    /// loaded Praxis load-balancer's verified mTLS backend before it sends
    /// provider-hop context. Changing Praxis backend transport requires a
    /// gateway restart while `gridServing` is enabled.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_hop_endpoints: Vec<ProviderHopEndpointConfig>,

    /// Opt-in configuration for operator-managed consumer Praxis config generation.
    ///
    /// When absent or `enabled: false`, this gateway behaves exactly as before —
    /// only the routing overlay `ConfigMap` is applied.  When `enabled: true`, the
    /// operator additionally renders a consumer Praxis `ConfigMap` containing the
    /// inference-model `intelligent_route` candidates (with credential
    /// `secretRef` data), a `credential_inject` section for credential-bearing
    /// inference candidates, and a `load_balancer` section with one cluster
    /// entry per unique inference cluster. Other capability kinds remain in the
    /// routing overlay for dedicated data-plane pipelines.
    ///
    /// The generated `ConfigMap` contains no token bytes. Credential-bearing
    /// overlays are withheld until the consumer has explicitly declared that its
    /// compatible filter config is already running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_config: Option<ConsumerConfig>,
}

/// Opt-in configuration for operator-generated consumer Praxis config.
///
/// When `enabled` is `true` on a [`GatewayRef`], the `GridNetwork` controller
/// renders a `praxis.yaml`-keyed `ConfigMap` in the gateway namespace in addition
/// to the normal routing overlay `ConfigMap`.  The generated config includes the
/// inference-model `intelligent_route` candidates, `credential_inject` (when
/// credential-bearing inference candidates are present), and a `load_balancer`
/// section.
///
/// An inference candidate whose cluster has no `clusterEndpoints` entry, typed or
/// derived, is withdrawn from this gateway and named in the status message,
/// rather than rendering an incomplete `load_balancer` cluster.
///
/// # Security
///
/// The generated `ConfigMap` never contains credential token bytes.  Credential
/// entries use a `file:` source under `credentialMountBase`; the mounted
/// Kubernetes Secret provides the token at runtime.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "wire-compatible opt-in and attestation fields are separate"
)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConfig {
    /// Enable operator-managed consumer Praxis config generation for this gateway.
    ///
    /// Default: `false`.  Set to `true` to opt in.
    #[serde(default)]
    pub enabled: bool,

    /// Generate the dynamic `credential_inject` filter and projected mount
    /// root, including when the current overlay has no credentials. Enable this
    /// first, then roll out the generated config before asserting readiness
    /// with `supportsProjectedCredentials`.
    ///
    /// Default: `false`.
    #[serde(default)]
    pub enable_projected_credentials: bool,

    /// Declare that the running consumer has the dynamic `credential_inject`
    /// filter configured with the projected Secret mount root. This is a
    /// fail-closed capability gate for credential-bearing overlay revisions.
    /// Set it only after the consumer has loaded a config containing that
    /// filter; the Grid operator does not own or roll out the consumer
    /// Deployment.
    ///
    /// Default: `false`.
    #[serde(default)]
    pub supports_projected_credentials: bool,

    /// Base directory for mounted credential Secret files inside the consumer pod.
    ///
    /// In projected-credential mode, mount each Secret at
    /// `{credentialMountBase}/{secret-namespace}/{secret-name}` with its data
    /// keys as files. Static `file:` entries continue to use their explicit
    /// paths and are not constrained by this directory layout.
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
    /// Each entry maps an inference candidate cluster name to a reachable endpoint
    /// address with explicit transport configuration. Every entry needs a
    /// non-`None` `transport` field.
    ///
    /// A candidate with no entry here, and none derived, is withdrawn from
    /// this gateway.  Missing transport fails with `MissingTransport`.
    /// Mutual-TLS transport without SNI fails with `MissingSni`.
    ///
    /// In production, this is populated by whoever manages the consumer gateway
    /// deployment (platform automation, the gateway operator, or a Helm chart).
    /// In local Kind validation, the xtask harness discovers `NodePort` addresses
    /// and populates this field in the test fixture.
    ///
    /// Default: empty. Supply entries before enabling generated consumer config
    /// for an overlay containing inference candidates.
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

    /// Derive the endpoint topology from the declarations of named providers,
    /// instead of requiring a `clusterEndpoints` entry per candidate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derive_topology: Option<DeriveTopology>,

    /// Opt in to validating Secret references and reconciling required mounts
    /// into a specifically delegated gateway Deployment.
    ///
    /// Disabled by default. When enabled, the named Deployment must carry the
    /// `grid.praxis-proxy.io/mount-reconciliation`, `.../network`, and
    /// `.../gateway` annotations emitted by `charts/praxis-gateway` (or set
    /// them explicitly on an externally managed Deployment).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mount_reconciliation: Option<MountReconciliation>,

    /// HTTP port for the generated Praxis listener.
    ///
    /// The rendered `listeners[0].address` is `0.0.0.0:{listenerPort}`.
    ///
    /// Default: `8080`.
    #[serde(default = "default_listener_port")]
    pub listener_port: u16,

    /// Optional OpenTelemetry exporter and sampling settings for this gateway.
    ///
    /// When present, the generated Praxis configuration opts into telemetry and
    /// adds the `trace_context` filter for outbound W3C header propagation.
    /// Collector authentication must be provided to the gateway Deployment via
    /// `OTEL_EXPORTER_OTLP_HEADERS` from a Secret; credentials are never copied
    /// into this `ConfigMap`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telemetry: Option<GatewayTelemetryConfig>,
}

/// Maximum batch interval accepted by Praxis telemetry configuration.
const MAX_TELEMETRY_BATCH_INTERVAL_SECS: u64 = 300;
/// Maximum batch size accepted by Praxis telemetry configuration.
const MAX_TELEMETRY_BATCH_SIZE: usize = 65_536;

/// Validated OpenTelemetry settings rendered into operator-generated Praxis YAML.
///
/// Secret material is intentionally not part of this type. Supply collector
/// credentials through the gateway Deployment's `OTEL_EXPORTER_OTLP_HEADERS`
/// environment variable using a Secret-backed `valueFrom` reference.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct GatewayTelemetryConfig {
    /// OTLP collector endpoint, for example `http://otel-collector:4317`.
    ///
    /// If omitted, Praxis can read `OTEL_EXPORTER_OTLP_ENDPOINT` from the
    /// gateway Deployment. An empty string has the same meaning as omission.
    #[schemars(regex(pattern = r"^(|https?://[^/?#@\s]+(:[0-9]+)?(/[^\s?#]*)?)$"))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub otlp_endpoint: Option<String>,

    /// Root trace sampling probability from `0.0` through `1.0`.
    #[schemars(range(min = 0.0, max = 1.0))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampling_rate: Option<f64>,

    /// OpenTelemetry `service.name` resource attribute.
    #[schemars(regex(pattern = r"^.*\S.*$"))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,

    /// OpenTelemetry `service.version` resource attribute.
    #[schemars(regex(pattern = r"^.*\S.*$"))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_version: Option<String>,

    /// Deployment environment resource attribute.
    #[schemars(regex(pattern = r"^.*\S.*$"))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,

    /// Batch exporter interval in seconds. Must be from 1 through 300 when set.
    #[schemars(range(min = 1, max = 300))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_interval_secs: Option<u64>,

    /// Maximum spans per export batch. Must be from 1 through 65,536 when set.
    #[schemars(range(min = 1, max = 65_536))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,
}

impl GatewayTelemetryConfig {
    /// Validate values before rendering them into the consumer configuration.
    ///
    /// # Errors
    ///
    /// Returns an explanatory message for invalid strings, sampling rates, or
    /// batch settings outside their supported ranges.
    #[expect(
        clippy::too_many_lines,
        reason = "validates all telemetry settings before CRD rendering"
    )]
    pub fn validate(&self) -> Result<(), String> {
        if self.otlp_endpoint.as_ref().is_some_and(|endpoint| {
            if endpoint.is_empty() {
                return false;
            }
            let endpoint = endpoint.trim();
            let Some((scheme, rest)) = endpoint.split_once("://") else {
                return true;
            };
            let authority = rest.split('/').next().unwrap_or_default();
            !matches!(scheme, "http" | "https")
                || authority.is_empty()
                || authority.contains('@')
                || endpoint.contains('?')
                || endpoint.contains('#')
                || endpoint.chars().any(char::is_whitespace)
        }) {
            return Err("telemetry.otlpEndpoint must be an http(s) URL without credentials".to_owned());
        }
        if let Some(rate) = self.sampling_rate
            && (!rate.is_finite() || !(0.0..=1.0).contains(&rate))
        {
            return Err("telemetry.samplingRate must be between 0.0 and 1.0".to_owned());
        }
        if self
            .batch_interval_secs
            .is_some_and(|seconds| !(1..=MAX_TELEMETRY_BATCH_INTERVAL_SECS).contains(&seconds))
        {
            return Err(format!(
                "telemetry.batchIntervalSecs must be between 1 and {MAX_TELEMETRY_BATCH_INTERVAL_SECS}"
            ));
        }
        if self
            .batch_size
            .is_some_and(|size| !(1..=MAX_TELEMETRY_BATCH_SIZE).contains(&size))
        {
            return Err(format!(
                "telemetry.batchSize must be between 1 and {MAX_TELEMETRY_BATCH_SIZE}"
            ));
        }
        for (field, value) in [
            ("serviceName", self.service_name.as_deref()),
            ("serviceVersion", self.service_version.as_deref()),
            ("environment", self.environment.as_deref()),
        ] {
            if value.is_some_and(|value| value.trim().is_empty()) {
                return Err(format!("telemetry.{field} must not be blank"));
            }
        }
        Ok(())
    }
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            enable_projected_credentials: false,
            supports_projected_credentials: false,
            credential_mount_base: default_credential_mount_base(),
            config_map_name: default_consumer_config_map_name(),
            cluster_endpoints: Vec::new(),
            derive_topology: None,
            tls_cert_mount_path: default_tls_cert_mount_path(),
            mount_reconciliation: None,
            listener_port: default_listener_port(),
            telemetry: None,
        }
    }
}

/// Providers a gateway derives its endpoint topology from. Empty derives nothing.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct DeriveTopology {
    /// Routing identities whose declarations this gateway derives from.
    ///
    /// Each entry is a provider's `routingClusterRef`, or its `metadata.name`
    /// when that is unset. At most 64 entries, each named once.
    #[schemars(
        length(max = 64),
        inner(length(min = 1, max = 253)),
        extend("x-kubernetes-list-type" = "set")
    )]
    #[serde(default)]
    pub from_providers: Vec<String>,
}

/// Explicit delegation of a gateway Deployment's generated Secret mounts.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
#[schemars(extend("x-kubernetes-validations" = [{
    "rule": "!has(self.enabled) || !self.enabled || has(self.deploymentName)",
    "message": "deploymentName is required when mount reconciliation is enabled"
}]))]
pub struct MountReconciliation {
    /// Enable delegated mount reconciliation. Defaults to `false`.
    #[serde(default)]
    pub enabled: bool,

    /// Name of the opted-in Deployment in the gateway namespace.
    #[schemars(length(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment_name: Option<String>,

    /// Name of the Praxis container whose mounts Grid manages.
    #[schemars(length(min = 1))]
    #[serde(default = "default_praxis_container_name")]
    pub container_name: String,
}

/// Default the delegated mount target to the chart's Praxis container name.
fn default_praxis_container_name() -> String {
    "praxis".to_owned()
}

/// Transport mode for a consumer load-balancer cluster endpoint.
///
/// Determines whether the consumer connects to the provider gateway
/// cluster over mutual TLS, server-authenticated TLS, or plain HTTP. This is an explicit security
/// decision — the operator refuses to render a cluster entry without a
/// declared transport mode, preventing accidental plaintext.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportMode {
    /// Mutual TLS with CA verification and client certificate.
    MutualTls,
    /// Server-authenticated TLS without a client certificate.
    Tls,
    /// Plain HTTP — no TLS.  Explicit insecure/dev-only mode.
    Plaintext,
}

/// Transport configuration for a cluster endpoint.
///
/// Bundles transport mode with SNI and an optional custom CA Secret.
/// `sni` is required for both TLS modes and forbidden for plaintext.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
#[schemars(extend("x-kubernetes-validations" = [{
    "rule": "self.mode == 'tls' || !has(self.caSecretRef) || self.caSecretRef == null",
    "message": "caSecretRef is only valid for tls transport"
}, {
    "rule": "self.mode == 'plaintext' || !has(self.sni) || self.sni.size() > 0",
    "message": "an explicitly empty sni is only valid for plaintext transport"
}]))]
pub struct EndpointTransport {
    /// Transport mode: `mutual_tls`, `tls`, or explicit `plaintext`.
    pub mode: TransportMode,

    /// TLS Server Name Indication, required when mode is `mutual_tls` or `tls`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sni: Option<String>,

    /// Optional gateway-namespace Secret supplying a custom CA bundle for `tls` transport.
    /// Specify only its name and optional key; the Secret must be in the namespace
    /// named by the containing `GatewayRef`. The mounted path is
    /// `/run/secrets/grid-backend-ca/{secret-name}/{key}`; the key defaults to
    /// `ca.crt`. Omitted uses the process trust store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_secret_ref: Option<EndpointCaSecretRef>,
}

/// Gateway-namespace Secret reference for a consumer backend CA bundle.
///
/// Unlike [`SecretRef`], this reference has no namespace field: the Secret is
/// resolved in the target gateway's namespace.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointCaSecretRef {
    /// Secret name in the target gateway namespace.
    #[schemars(length(min = 1))]
    pub name: String,

    /// Optional key containing the CA bundle. Defaults to `ca.crt`.
    #[schemars(length(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// Explicit verified-mTLS endpoint identity trusted for embedded provider-hop
/// context. This does not configure the gateway's upstream TLS connection; the
/// embedded gateway must separately use verified TLS with this SNI; the
/// gateway checks the two configurations before serving hop traffic.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderHopEndpointConfig {
    /// Cluster name used by the embedded gateway's `load_balancer` filter.
    pub cluster: String,

    /// TLS mode and SNI configured for that embedded gateway upstream.
    pub transport: EndpointTransport,
}

/// Endpoint configuration for one consumer `load_balancer` cluster.
///
/// Maps an inference candidate cluster name to a reachable provider gateway
/// endpoint with explicit transport intent. Every cluster referenced by a
/// projected inference candidate must have a matching entry.
///
/// # Transport requirement
///
/// The `transport` field is required.  Missing transport fails closed
/// during config rendering with status reason `MissingTransport`.
/// Both TLS modes require nonblank `transport.sni`; otherwise rendering fails
/// with status reason `MissingSni`. `tls` can declare a custom CA Secret.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterEndpointConfig {
    /// Cluster name - must match a projected inference `candidate.cluster` value.
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
pub(crate) const DEFAULT_TLS_CERT_MOUNT_PATH: &str = "/etc/praxis/tls";

/// Default TLS certificate mount path inside the consumer pod.
fn default_tls_cert_mount_path() -> String {
    DEFAULT_TLS_CERT_MOUNT_PATH.to_owned()
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
#[expect(
    clippy::struct_field_names,
    reason = "fields named after Kubernetes Secret references"
)]
pub struct TlsConfig {
    /// Secret storing the grid CA certificate and key.
    pub ca_secret_ref: Option<SecretRef>,

    /// Secret storing this site's certificate and key.
    pub site_secret_ref: Option<SecretRef>,

    /// Secret storing the SWIM encryption key.
    pub swim_key_ref: Option<SecretRef>,
}

/// Reference to a Kubernetes Secret.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
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
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
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

    /// Per-gateway Secret requirements and optional mount reconciliation status.
    ///
    /// Populated for every gateway with `consumerConfig.enabled: true`. Without
    /// explicit mount reconciliation, the phase remains `RequirementsRendered`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mount_reconciliation_status: Vec<MountReconciliationStatus>,

    /// Per-gateway overlay revision status.
    ///
    /// Populated after each overlay reconcile attempt. Captures rendered and
    /// distributed revisions so operators can verify propagation without
    /// inspecting `ConfigMap` contents. A failed update retains evidence for
    /// the last successfully distributed revision.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub overlay_status: Vec<OverlayRevisionStatus>,

    /// Per-tenant budget status, derived from `spec.budgetPolicy` and merged
    /// cross-site CRDT spend state.
    ///
    /// Empty when `budgetPolicy` is absent. This is a status signal only —
    /// Grid does not enforce budget limits itself (see [`BudgetPolicyConfig`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub budget_status: Vec<TenantBudgetStatus>,

    /// This site's identity certificate: when it expires and when rotation is due.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<SiteIdentityStatus>,
}

/// This site's identity certificate.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SiteIdentityStatus {
    /// When the certificate expires, RFC 3339.
    pub not_after: String,
    /// When rotation is due, a third of the lifetime before `notAfter`, RFC 3339. Empty
    /// under pin peer trust, or when this site's operator does not rotate.
    pub rotate_after: String,
    /// SHA-256 of the certificate DER, the digest peers pin.
    pub fingerprint: String,
    /// `IdentityExpired` once the certificate has expired, `IdentityUnreadable` when the
    /// Secret holds no usable certificate.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[schemars(regex(pattern = "^(IdentityExpired|IdentityUnreadable)$"))]
    pub reason: String,
    /// How to recover.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
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

/// Lifecycle phase for delegated gateway mount reconciliation.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum MountReconciliationPhase {
    /// The reference-only requirements document has been generated.
    RequirementsRendered,
    /// A referenced Secret or required key is absent or invalid.
    WaitingForSecret,
    /// Grid is applying the required pod volumes and mounts.
    MountsReconciling,
    /// The Deployment is rolling out a matching configuration revision.
    WaitingForRollout,
    /// Required files are mounted in available pods with matching config.
    Ready,
    /// Delegation, path, ownership, or API reconciliation failed.
    #[default]
    Error,
}

/// Per-gateway progress for Secret mount reconciliation and gateway readiness.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MountReconciliationStatus {
    /// Gateway reference name.
    pub gateway_name: String,
    /// Namespace containing the Deployment and Secret references.
    pub namespace: String,
    /// Delegated Deployment name. Absent when Grid only publishes requirements.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment_name: Option<String>,
    /// Current mount lifecycle phase.
    pub phase: MountReconciliationPhase,
    /// Digest of canonical requirements, containing no Secret bytes.
    #[serde(default)]
    pub requirements_revision: String,
    /// Digest last mounted and rolled out by Grid.
    #[serde(default)]
    pub applied_revision: String,
    /// Stable machine-readable cause; empty on `Ready`.
    #[serde(default)]
    pub reason: String,
    /// Human-readable diagnostic containing identifiers and paths only.
    #[serde(default)]
    pub message: String,
    /// Observed `GridNetwork` generation.
    #[serde(default)]
    pub observed_generation: i64,
    /// Observed Deployment generation, or zero before it is observed.
    #[serde(default)]
    pub deployment_generation: i64,
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
    use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
    use kube::CustomResourceExt as _;

    use super::*;

    fn crd_json() -> serde_json::Value {
        serde_json::to_value(GridNetwork::crd()).unwrap_or_else(|_| std::process::abort())
    }

    fn crd_spec<'val>(crd: &'val serde_json::Value, field: &str) -> &'val str {
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
        assert_eq!(crd_spec(&crd, "group"), "grid.praxis.fast", "wrong CRD group");
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
    fn grid_network_crd_has_short_name() {
        let crd = crd_json();
        assert_eq!(
            crd.pointer("/spec/names/shortNames"),
            Some(&serde_json::json!(["gnw"])),
            "kubectl get gnw needs this short name"
        );
    }

    #[test]
    fn deploy_crd_manifest_has_generated_short_names() {
        let manifest: CustomResourceDefinition =
            serde_yaml::from_str(include_str!("../../../deploy/crds/gridnetwork.yaml"))
                .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            manifest.spec.names.short_names,
            GridNetwork::crd().spec.names.short_names,
            "chart CRD manifest and Rust definition must have the same short names"
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
                "enableProjectedCredentials": true,
                "supportsProjectedCredentials": true,
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
                }],
                "telemetry": {
                    "otlpEndpoint": "http://otel-collector:4317",
                    "samplingRate": 0.125,
                    "serviceName": "grid-edge"
                }
            }
        });
        let gw: GatewayRef = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let cc = gw.consumer_config.unwrap_or_else(|| std::process::abort());
        assert!(cc.enabled, "enabled must round-trip");
        assert!(
            cc.enable_projected_credentials,
            "projected filter opt-in must round-trip"
        );
        assert!(
            cc.supports_projected_credentials,
            "projected credential readiness must round-trip"
        );
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
        let telemetry = cc.telemetry.as_ref().unwrap_or_else(|| std::process::abort());
        assert_eq!(telemetry.otlp_endpoint.as_deref(), Some("http://otel-collector:4317"));
        assert_eq!(telemetry.sampling_rate, Some(0.125));
        assert_eq!(telemetry.service_name.as_deref(), Some("grid-edge"));
        assert!(
            telemetry.validate().is_ok(),
            "valid telemetry settings must pass validation"
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
    #[expect(
        clippy::too_many_lines,
        reason = "asserts every ConsumerConfig default as one contract"
    )]
    fn consumer_config_defaults_when_subfields_absent() {
        let json = serde_json::json!({
            "name": "gw",
            "namespace": "ns",
            "consumerConfig": {}
        });
        let gw: GatewayRef = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let cc = gw.consumer_config.unwrap_or_else(|| std::process::abort());
        assert!(!cc.enabled, "enabled must default to false");
        assert!(
            !cc.enable_projected_credentials,
            "projected filter opt-in must default off"
        );
        assert!(
            !cc.supports_projected_credentials,
            "projected credentials must default closed"
        );
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
        assert!(cc.telemetry.is_none(), "telemetry must remain disabled by default");
        assert_eq!(
            cc.tls_cert_mount_path, "/etc/praxis/tls",
            "tlsCertMountPath must use default"
        );
    }

    #[test]
    fn telemetry_config_rejects_embedded_headers() {
        let with_headers = serde_json::json!({
            "otlpEndpoint": "http://collector:4317",
            "otlpHeaders": {"authorization": "test-value"}
        });
        assert!(
            serde_json::from_value::<GatewayTelemetryConfig>(with_headers).is_err(),
            "collector credentials must not be accepted as config fields"
        );
    }

    #[test]
    fn telemetry_config_accepts_empty_endpoint_fallback() {
        let empty_endpoint = serde_json::from_value::<GatewayTelemetryConfig>(serde_json::json!({
            "otlpEndpoint": ""
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert!(
            empty_endpoint.validate().is_ok(),
            "an empty endpoint must use the deployment environment fallback"
        );
    }

    #[test]
    fn telemetry_batch_bounds_appear_in_crd_schema() {
        let crd = crd_json();
        let properties = crd
            .pointer(
                "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/gatewayRefs/items/properties/consumerConfig/properties/telemetry/properties",
            )
            .unwrap_or_else(|| std::process::abort());

        for (field, maximum) in [("batchIntervalSecs", 300.0), ("batchSize", 65_536.0)] {
            let schema = properties.get(field).unwrap_or_else(|| std::process::abort());
            assert_eq!(
                schema.get("minimum").and_then(serde_json::Value::as_f64),
                Some(1.0),
                "{field} CRD schema must enforce the inclusive lower bound"
            );
            assert_eq!(
                schema.get("maximum").and_then(serde_json::Value::as_f64),
                Some(maximum),
                "{field} CRD schema must enforce the inclusive upper bound"
            );
        }
    }

    #[test]
    fn telemetry_batch_bounds_are_enforced_at_runtime() {
        for (field, maximum) in [("batchIntervalSecs", 300_u64), ("batchSize", 65_536_u64)] {
            for (value, expected_valid) in [(0, false), (1, true), (maximum, true), (maximum + 1, false)] {
                let telemetry = serde_json::from_value::<GatewayTelemetryConfig>(serde_json::json!({(field): value}))
                    .unwrap_or_else(|_| std::process::abort());
                assert_eq!(
                    telemetry.validate().is_ok(),
                    expected_valid,
                    "{field}={value} runtime validation mismatch"
                );
            }
        }
    }

    #[test]
    fn derive_topology_from_providers_is_a_bounded_set() {
        let crd = crd_json();
        let from_providers = crd
            .pointer(
                "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/gatewayRefs/items/properties/consumerConfig/properties/deriveTopology/properties/fromProviders",
            )
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            from_providers.get("maxItems").and_then(serde_json::Value::as_u64),
            Some(64)
        );
        assert_eq!(
            from_providers
                .get("x-kubernetes-list-type")
                .and_then(serde_json::Value::as_str),
            Some("set"),
            "a set refuses a duplicate at admission"
        );
        assert_eq!(
            from_providers
                .pointer("/items/minLength")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(
            from_providers
                .pointer("/items/maxLength")
                .and_then(serde_json::Value::as_u64),
            Some(253)
        );
    }

    #[test]
    fn telemetry_resource_attributes_reject_blank_values_at_admission_and_runtime() {
        let crd = crd_json();
        let telemetry_properties = crd
            .pointer(
                "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/gatewayRefs/items/properties/consumerConfig/properties/telemetry/properties",
            )
            .unwrap_or_else(|| std::process::abort());
        for field in ["serviceName", "serviceVersion", "environment"] {
            let pattern = telemetry_properties
                .get(field)
                .and_then(|property| property.get("pattern"))
                .and_then(serde_json::Value::as_str);
            assert_eq!(
                pattern,
                Some(r"^.*\S.*$"),
                "{field} must reject blank values at admission"
            );
            let telemetry: GatewayTelemetryConfig =
                serde_json::from_value(serde_json::json!({(field): ""})).unwrap_or_else(|_| std::process::abort());
            assert!(
                telemetry.validate().is_err(),
                "{field} must reject blank values at runtime"
            );
        }
    }

    #[test]
    fn telemetry_config_rejects_credentials_in_endpoint() {
        for endpoint in [
            "https://user:password@collector:4317",
            "https://collector:4317?api_key=secret",
            "https://collector:4317/otlp#token=secret",
        ] {
            let telemetry = serde_json::from_value::<GatewayTelemetryConfig>(serde_json::json!({
                "otlpEndpoint": endpoint,
                "samplingRate": 0.5
            }))
            .unwrap_or_else(|_| std::process::abort());
            assert!(
                telemetry.validate().is_err(),
                "credentials in endpoint userinfo, query, or fragment must be rejected: {endpoint}"
            );
        }
    }

    #[test]
    fn consumer_config_absent_not_serialized() {
        let gw = GatewayRef {
            name: "gw".to_owned(),
            namespace: "ns".to_owned(),
            local_site_name: None,
            provider_hop_endpoints: Vec::new(),
            consumer_config: None,
        };
        let json = serde_json::to_value(&gw).unwrap_or_else(|_| std::process::abort());
        assert!(
            json.get("consumerConfig").is_none(),
            "absent consumerConfig must not appear in serialized output"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "asserts generated telemetry fields and all opt-in security boundaries"
    )]
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
        assert!(
            gateway_ref_properties.contains_key("providerHopEndpoints"),
            "CRD schema must include independent providerHopEndpoints on GatewayRef"
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
        let telemetry_properties = consumer_config_properties
            .get("telemetry")
            .and_then(|v| v.pointer("/properties"))
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        let sampling_rate = telemetry_properties
            .get("samplingRate")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            sampling_rate.pointer("/minimum").and_then(serde_json::Value::as_f64),
            Some(0.0),
            "CRD schema must reject sampling rates below zero"
        );
        assert_eq!(
            sampling_rate.pointer("/maximum").and_then(serde_json::Value::as_f64),
            Some(1.0),
            "CRD schema must reject sampling rates above one"
        );
        let endpoint_schema = telemetry_properties
            .get("otlpEndpoint")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            endpoint_schema.get("type").and_then(serde_json::Value::as_str),
            Some("string"),
            "OTLP endpoint remains an optional string"
        );
        assert!(
            !telemetry_properties
                .get("required")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|required| required.iter().any(|field| field.as_str() == Some("otlpEndpoint"))),
            "omitted endpoint must remain valid for environment fallback"
        );
        let endpoint_pattern = endpoint_schema
            .get("pattern")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            endpoint_pattern.starts_with("^(|") && endpoint_pattern.contains("https?://"),
            "CRD schema must accept empty environment fallback and explicit HTTP(S) URLs"
        );
    }

    #[test]
    fn delegated_deployment_name_is_optional_but_nonempty_when_set() {
        let crd = crd_json();
        let mount_properties = crd
            .pointer(
                "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties\
                 /gatewayRefs/items/properties/consumerConfig/properties\
                 /mountReconciliation/properties",
            )
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        let deployment_name = mount_properties
            .get("deploymentName")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            deployment_name.get("minLength").and_then(serde_json::Value::as_u64),
            Some(1),
            "a supplied delegated Deployment name must be nonempty"
        );
        assert!(
            deployment_name.get("default").is_none(),
            "the optional Deployment name must not default to an empty string"
        );
        assert_eq!(
            mount_properties
                .get("enabled")
                .and_then(|enabled| enabled.get("default"))
                .and_then(serde_json::Value::as_bool),
            Some(false),
            "mount reconciliation must remain opt in"
        );
    }

    #[test]
    fn enabled_mount_reconciliation_requires_deployment_name_at_admission() {
        let crd = crd_json();
        let rule = crd.pointer(
            "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties\
             /gatewayRefs/items/properties/consumerConfig/properties/mountReconciliation\
             /x-kubernetes-validations/0/rule",
        );
        assert_eq!(
            rule,
            Some(&serde_json::json!(
                "!has(self.enabled) || !self.enabled || has(self.deploymentName)"
            )),
            "enabled delegation must require a Deployment name at admission"
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
        assert!(
            mode_values.contains(&"tls"),
            "transport.mode enum must include server-authenticated tls: {mode_values:?}"
        );
        assert_eq!(
            mode_values.len(),
            3,
            "transport.mode enum must have exactly 3 values: {mode_values:?}"
        );
        assert!(
            transport_properties.contains_key("caSecretRef"),
            "CRD schema must include optional custom CA Secret reference"
        );
        assert!(
            transport_properties
                .get("sni")
                .and_then(|sni| sni.get("minLength"))
                .is_none(),
            "plaintext transport must admit an explicitly empty SNI"
        );
        assert_eq!(
            endpoint_properties
                .get("transport")
                .and_then(|transport| transport.pointer("/x-kubernetes-validations/0/rule")),
            Some(&serde_json::json!(
                "self.mode == 'tls' || !has(self.caSecretRef) || self.caSecretRef == null"
            )),
            "custom CA references must be admitted only for tls transport"
        );
        assert_eq!(
            endpoint_properties
                .get("transport")
                .and_then(|transport| transport.pointer("/x-kubernetes-validations/1/rule")),
            Some(&serde_json::json!(
                "self.mode == 'plaintext' || !has(self.sni) || self.sni.size() > 0"
            )),
            "explicitly empty SNI must be admitted only for plaintext transport"
        );

        let ca_ref = transport_properties
            .get("caSecretRef")
            .unwrap_or_else(|| std::process::abort());
        let ca_ref_properties = ca_ref
            .pointer("/properties")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            ca_ref_properties.contains_key("name"),
            "transport CA reference must expose name"
        );
        assert!(
            ca_ref_properties.contains_key("key"),
            "transport CA reference must expose optional key"
        );
        assert!(
            !ca_ref_properties.contains_key("namespace"),
            "transport CA reference must resolve in the gateway namespace"
        );
        assert_eq!(
            ca_ref.pointer("/required"),
            Some(&serde_json::json!(["name"])),
            "only the transport CA Secret name is required"
        );

        let grid_ca_ref = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/tls/properties/caSecretRef")
            .unwrap_or_else(|| std::process::abort());
        let grid_ca_properties = grid_ca_ref
            .pointer("/properties")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            grid_ca_properties.contains_key("namespace"),
            "spec.tls.caSecretRef must retain its explicit namespace"
        );
        assert!(
            grid_ca_ref
                .pointer("/required")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|required| required.iter().any(|field| field == "namespace")),
            "spec.tls.caSecretRef must continue requiring namespace"
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
    fn selection_policy_round_trips_and_rejects_unknown_mode() {
        let spec: GridNetworkSpec = serde_json::from_value(serde_json::json!({
            "seeds": [],
            "selectionPolicy": { "mode": "roundRobin" }
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            spec.selection_policy.map(|policy| policy.mode),
            Some(SelectionMode::RoundRobin)
        );

        let result = serde_json::from_value::<GridNetworkSpec>(serde_json::json!({
            "seeds": [],
            "selectionPolicy": { "mode": "not-a-mode" }
        }));
        let Err(error) = result else {
            std::process::abort();
        };
        assert!(error.to_string().contains("unknown variant"));
    }

    #[test]
    fn peer_trust_round_trips_and_rejects_unknown_mode() {
        let spec: GridNetworkSpec =
            serde_json::from_value(serde_json::json!({"seeds": [], "peerTrust": {"mode": "spiffe"}}))
                .unwrap_or_else(|_| std::process::abort());
        assert_eq!(spec.peer_trust.map(|trust| trust.mode), Some(PeerTrustMode::Spiffe));
        let bare: GridNetworkSpec =
            serde_json::from_value(serde_json::json!({"seeds": []})).unwrap_or_else(|_| std::process::abort());
        assert!(bare.peer_trust.is_none(), "absent stays absent");
        assert_eq!(PeerTrustMode::default(), PeerTrustMode::Pin);
        let Err(_unknown) =
            serde_json::from_value::<GridNetworkSpec>(serde_json::json!({"seeds": [], "peerTrust": {"mode": "x"}}))
        else {
            std::process::abort();
        };
    }

    #[test]
    fn signal_transport_round_trips_and_rejects_unknown_mode() {
        let spec: GridNetworkSpec = serde_json::from_value(serde_json::json!({
            "seeds": [],
            "signalTransport": { "mode": "poll" }
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(spec.signal_transport.map(|policy| policy.mode), Some(SignalMode::Poll));

        // Absent policy resolves to the gossip default.
        let bare: GridNetworkSpec =
            serde_json::from_value(serde_json::json!({ "seeds": [] })).unwrap_or_else(|_| std::process::abort());
        assert!(bare.signal_transport.is_none());
        assert_eq!(SignalMode::default(), SignalMode::Gossip);

        let Err(unknown_mode) = serde_json::from_value::<GridNetworkSpec>(serde_json::json!({
            "seeds": [],
            "signalTransport": { "mode": "not-a-mode" }
        })) else {
            std::process::abort();
        };
        assert!(unknown_mode.to_string().contains("unknown variant"));

        // deny_unknown_fields: an extra key is rejected.
        assert!(
            serde_json::from_value::<GridNetworkSpec>(serde_json::json!({
                "seeds": [],
                "signalTransport": { "mode": "poll", "extra": true }
            }))
            .is_err(),
            "unknown field in signalTransport must be rejected"
        );
    }

    #[test]
    fn round_robin_mode_serializes_for_explicit_policy() {
        let serialized = serde_json::to_value(&SelectionPolicyConfig {
            mode: SelectionMode::RoundRobin,
        })
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(serialized, serde_json::json!({"mode": "roundRobin"}));
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

    // -----------------------------------------------------------------------
    // validate_budget_policy tests (A1-A6)
    // -----------------------------------------------------------------------

    fn tenant(id: &str, cap_usd: f64) -> TenantBudgetConfig {
        TenantBudgetConfig {
            tenant_id: id.to_owned(),
            cap_usd,
        }
    }

    #[test]
    fn validate_budget_policy_accepts_valid_config() {
        let policy = BudgetPolicyConfig {
            tenants: vec![tenant("tenant-a", 100.0), tenant("tenant-b", 250.0)],
        };
        assert!(
            validate_budget_policy(&policy).is_ok(),
            "distinct positive caps and non-empty tenant ids must be valid"
        );
    }

    #[test]
    fn validate_budget_policy_rejects_negative_cap() {
        let policy = BudgetPolicyConfig {
            tenants: vec![tenant("tenant-a", -5.0)],
        };
        assert_eq!(
            validate_budget_policy(&policy),
            Err(BudgetPolicyValidationError::NegativeCap {
                tenant_id: "tenant-a".to_owned()
            }),
            "negative capUsd must be rejected"
        );
    }

    #[test]
    fn validate_budget_policy_rejects_non_finite_cap() {
        for bad_cap in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let policy = BudgetPolicyConfig {
                tenants: vec![tenant("tenant-a", bad_cap)],
            };
            assert_eq!(
                validate_budget_policy(&policy),
                Err(BudgetPolicyValidationError::NonFiniteCap {
                    tenant_id: "tenant-a".to_owned()
                }),
                "non-finite capUsd ({bad_cap}) must be rejected"
            );
        }
    }

    #[test]
    fn validate_budget_policy_rejects_duplicate_tenant_id() {
        let policy = BudgetPolicyConfig {
            tenants: vec![tenant("tenant-a", 100.0), tenant("tenant-a", 200.0)],
        };
        assert_eq!(
            validate_budget_policy(&policy),
            Err(BudgetPolicyValidationError::DuplicateTenant {
                tenant_id: "tenant-a".to_owned()
            }),
            "duplicate tenantId must be rejected"
        );
    }

    #[test]
    fn validate_budget_policy_rejects_blank_tenant_id() {
        for blank in ["", "   "] {
            let policy = BudgetPolicyConfig {
                tenants: vec![tenant(blank, 100.0)],
            };
            assert_eq!(
                validate_budget_policy(&policy),
                Err(BudgetPolicyValidationError::BlankTenantId),
                "blank tenantId ({blank:?}) must be rejected"
            );
        }
    }

    #[test]
    fn validate_budget_policy_accepts_empty_tenant_list() {
        let policy = BudgetPolicyConfig { tenants: Vec::new() };
        assert!(
            validate_budget_policy(&policy).is_ok(),
            "an empty tenants list is a valid no-op policy"
        );
    }

    // -----------------------------------------------------------------------
    // spend_ratio tests (B1-B6)
    // -----------------------------------------------------------------------

    fn spend_of(cents: u64) -> GCounter {
        let mut counter = GCounter::new("site-a".to_owned());
        counter.increment(cents);
        counter
    }

    #[test]
    fn spend_ratio_below_cap() {
        // 50.00 spent against a 100.00 cap.
        assert_weight(spend_ratio(&spend_of(5000), 100.0), 0.5, "spend/cap");
    }

    #[test]
    fn spend_ratio_at_cap() {
        assert_weight(spend_ratio(&spend_of(10_000), 100.0), 1.0, "spend == cap");
    }

    #[test]
    fn spend_ratio_overspend_clamps_to_one() {
        // 150.00 spent against a 100.00 cap must not exceed 1.0.
        assert_weight(spend_ratio(&spend_of(15_000), 100.0), 1.0, "overspend must clamp");
    }

    #[test]
    fn spend_ratio_zero_spend_is_zero() {
        assert_weight(
            spend_ratio(&spend_of(0), 100.0),
            0.0,
            "zero spend against a positive cap",
        );
    }

    #[test]
    fn spend_ratio_non_positive_cap_is_always_one() {
        for bad_cap in [0.0, -1.0, f64::NAN, f64::NEG_INFINITY] {
            assert_weight(
                spend_ratio(&spend_of(0), bad_cap),
                1.0,
                &format!("cap_usd={bad_cap} must defensively read as maxed regardless of spend"),
            );
        }
    }

    #[test]
    fn spend_ratio_near_u64_max_does_not_panic_and_clamps() {
        let ratio = spend_ratio(&spend_of(u64::MAX), 100.0);
        assert!(ratio.is_finite(), "must not produce NaN/inf from a huge u64 conversion");
        assert_weight(ratio, 1.0, "huge spend against a small cap must clamp to 1.0");
    }

    // -----------------------------------------------------------------------
    // BudgetPolicy CRD spec wiring tests (D1-D6)
    // -----------------------------------------------------------------------

    #[test]
    fn budget_policy_absent_defaults_to_none() {
        let json = serde_json::json!({ "gridId": "", "seeds": [] });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert!(spec.budget_policy.is_none(), "absent budgetPolicy must default to None");
    }

    #[test]
    fn budget_policy_absent_not_serialized() {
        let json = serde_json::json!({ "gridId": "", "seeds": [] });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let serialized = serde_json::to_value(&spec).unwrap_or_else(|_| std::process::abort());
        assert!(
            serialized.get("budgetPolicy").is_none(),
            "absent budgetPolicy must not appear in serialized output"
        );
    }

    #[test]
    fn budget_policy_with_tenants_round_trips() {
        let json = serde_json::json!({
            "gridId": "",
            "seeds": [],
            "budgetPolicy": {
                "tenants": [
                    { "tenantId": "tenant-a", "capUsd": 100.0 },
                    { "tenantId": "tenant-b", "capUsd": 50.0 }
                ]
            }
        });
        let spec: GridNetworkSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let policy = spec.budget_policy.unwrap_or_else(|| std::process::abort());
        assert_eq!(policy.tenants.len(), 2, "both tenants must round-trip");
        let first = policy.tenants.first().unwrap_or_else(|| std::process::abort());
        let second = policy.tenants.get(1).unwrap_or_else(|| std::process::abort());
        assert_eq!(first.tenant_id, "tenant-a");
        assert_weight(first.cap_usd, 100.0, "tenant-a capUsd");
        assert_eq!(second.tenant_id, "tenant-b");
        assert_weight(second.cap_usd, 50.0, "tenant-b capUsd");
    }

    #[test]
    fn budget_policy_appears_in_crd_schema() {
        let crd = crd_json();
        let schema = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/budgetPolicy")
            .unwrap_or_else(|| std::process::abort());
        assert!(schema.is_object(), "budgetPolicy must appear in the CRD schema");
    }

    #[test]
    fn budget_policy_cap_usd_schema_has_zero_minimum() {
        let crd = crd_json();
        let cap_schema = crd
            .pointer(
                "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties\
                 /budgetPolicy/properties/tenants/items/properties/capUsd",
            )
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            cap_schema.pointer("/minimum").and_then(serde_json::Value::as_f64),
            Some(0.0),
            "capUsd schema must reject negative values"
        );
    }

    #[test]
    fn budget_policy_rejects_unknown_shape() {
        let json = serde_json::json!({
            "gridId": "",
            "seeds": [],
            "budgetPolicy": { "caps": [] }
        });
        let result = serde_json::from_value::<GridNetworkSpec>(json);
        assert!(result.is_err(), "unknown budgetPolicy shape must be rejected");
    }

    // -----------------------------------------------------------------------
    // tenant_spend_status tests (E1-E6)
    // -----------------------------------------------------------------------

    fn spend_map(entries: &[(&str, u64)]) -> BTreeMap<String, GCounter> {
        entries
            .iter()
            .map(|(tenant_id, cents)| ((*tenant_id).to_owned(), spend_of(*cents)))
            .collect()
    }

    #[test]
    fn tenant_spend_status_reports_recorded_spend() {
        let policy = BudgetPolicyConfig {
            tenants: vec![tenant("tenant-a", 100.0)],
        };
        let spend = spend_map(&[("tenant-a", 2500)]);
        let statuses = tenant_spend_status(&policy, &spend);
        assert_eq!(statuses.len(), 1);
        let entry = statuses.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(entry.tenant_id, "tenant-a");
        assert_weight(entry.spend_usd, 25.0, "spend_usd");
        assert_weight(entry.cap_usd, 100.0, "cap_usd");
        assert_weight(entry.spend_ratio, 0.25, "spend_ratio");
    }

    #[test]
    fn tenant_spend_status_includes_tenant_with_no_spend_yet() {
        let policy = BudgetPolicyConfig {
            tenants: vec![tenant("tenant-a", 100.0)],
        };
        let statuses = tenant_spend_status(&policy, &BTreeMap::new());
        assert_eq!(
            statuses.len(),
            1,
            "declared tenant must be present even with zero recorded spend"
        );
        let entry = statuses.first().unwrap_or_else(|| std::process::abort());
        assert_weight(entry.spend_usd, 0.0, "spend_usd with no traffic yet");
        assert_weight(entry.spend_ratio, 0.0, "spend_ratio with no traffic yet");
    }

    #[test]
    fn tenant_spend_status_excludes_spend_for_undeclared_tenant() {
        let policy = BudgetPolicyConfig {
            tenants: vec![tenant("tenant-a", 100.0)],
        };
        let spend = spend_map(&[("tenant-a", 1000), ("tenant-orphan", 9999)]);
        let statuses = tenant_spend_status(&policy, &spend);
        assert_eq!(
            statuses.len(),
            1,
            "CRDT spend for a tenant no longer declared in policy must be excluded"
        );
        assert_eq!(
            statuses.first().unwrap_or_else(|| std::process::abort()).tenant_id,
            "tenant-a"
        );
    }

    #[test]
    fn tenant_spend_status_empty_policy_is_empty() {
        let policy = BudgetPolicyConfig { tenants: Vec::new() };
        let statuses = tenant_spend_status(&policy, &spend_map(&[("tenant-a", 1000)]));
        assert!(statuses.is_empty(), "empty policy must produce empty status");
    }

    #[test]
    fn tenant_spend_status_is_sorted_by_tenant_id() {
        let policy = BudgetPolicyConfig {
            tenants: vec![tenant("tenant-z", 100.0), tenant("tenant-a", 100.0)],
        };
        let statuses = tenant_spend_status(&policy, &BTreeMap::new());
        let ids: Vec<&str> = statuses.iter().map(|s| s.tenant_id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["tenant-a", "tenant-z"],
            "status must be deterministically sorted by tenant_id"
        );
    }

    #[test]
    fn tenant_spend_status_over_cap_clamps_ratio() {
        let policy = BudgetPolicyConfig {
            tenants: vec![tenant("tenant-a", 10.0)],
        };
        // 20.00 spent against a 10.00 cap.
        let spend = spend_map(&[("tenant-a", 2000)]);
        let statuses = tenant_spend_status(&policy, &spend);
        assert_weight(
            statuses.first().unwrap_or_else(|| std::process::abort()).spend_ratio,
            1.0,
            "over-cap spend_ratio must clamp to 1.0",
        );
    }

    // -----------------------------------------------------------------------
    // resolve_budget_statuses tests (G1)
    // -----------------------------------------------------------------------

    #[test]
    fn resolve_budget_statuses_none_policy_is_empty() {
        let statuses = resolve_budget_statuses(None, &spend_map(&[("tenant-a", 1000)]));
        assert!(
            statuses.is_empty(),
            "absent budgetPolicy must produce empty budget_status"
        );
    }

    #[test]
    fn resolve_budget_statuses_delegates_to_tenant_spend_status() {
        let policy = BudgetPolicyConfig {
            tenants: vec![tenant("tenant-a", 100.0)],
        };
        let spend = spend_map(&[("tenant-a", 5000)]);
        let statuses = resolve_budget_statuses(Some(&policy), &spend);
        assert_eq!(statuses.len(), 1);
        assert_weight(
            statuses.first().unwrap_or_else(|| std::process::abort()).spend_ratio,
            0.5,
            "spend_ratio via resolve_budget_statuses",
        );
    }
}
