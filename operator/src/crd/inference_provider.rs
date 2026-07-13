//! [`InferenceProvider`] custom resource definition.
//!
//! Represents an inference backend available over the grid.
//! Three backend categories: self-hosted clusters (llm-d),
//! cloud-managed services (Bedrock, Vertex), and third-party
//! APIs (OpenAI, Anthropic).

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::auth::{AccessPolicy, AuthConfig};

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

/// Specification for an [`InferenceProvider`].
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "grid.praxis-proxy.io",
    version = "v1alpha1",
    kind = "InferenceProvider",
    plural = "inferenceproviders",
    status = "InferenceProviderStatus",
    namespaced = false,
    printcolumn = r#"{"name":"Provider","type":"string","jsonPath":".spec.providerKind"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct InferenceProviderSpec {
    /// Name of the [`GridNetwork`] this provider belongs to.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub grid_network_ref: String,

    /// Which sites can consume this provider.
    #[serde(default)]
    pub access_policy: AccessPolicy,

    /// Authentication configuration.
    pub auth: Option<AuthConfig>,

    /// Backend deployment category.
    pub backend_kind: String,

    /// Cost information.
    pub cost: Option<CostConfig>,

    /// HTTP endpoint URL.
    pub endpoint: String,

    /// Health check configuration.
    pub health_check: Option<HealthCheckConfig>,

    /// Models served by this provider.
    #[serde(default)]
    pub models: Vec<ModelInfo>,

    /// Inference provider type.
    pub provider_kind: String,

    /// Optional routing identity used in overlay candidate `site` and `cluster` fields.
    ///
    /// When set, routing overlay candidates produced for this provider use this value
    /// instead of `metadata.name`:
    ///
    /// - In Phase 1 (no [`GridSite`] inventory), both `candidate.site` and `candidate.cluster` are set to this value.
    /// - When [`GridSite`] resources are present, only `candidate.cluster` is overridden; `candidate.site` is derived
    ///   from the matched `GridSite`.
    ///
    /// Use this to align the provider's routing identity with an upstream cluster
    /// name already configured in the consumer gateway, such as a Praxis
    /// `load_balancer` cluster entry.  When absent, `metadata.name` is used.
    ///
    /// [`GridSite`]: crate::crd::grid_site::GridSite
    pub routing_cluster_ref: Option<String>,

    /// Which sites host this provider.
    #[serde(default)]
    pub site_selector: super::auth::SelectorConfig,

    /// Prometheus metrics scraping configuration.
    ///
    /// When set, the Grid operator scrapes the provider's metrics endpoint during
    /// each [`GridNetwork`] reconcile and incorporates the parsed signals into the
    /// routing overlay scoring pass.  When absent, the provider uses locality and
    /// cost as the only scoring signals (static ordering).
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub metrics_config: Option<MetricsConfig>,
}

/// Prometheus metrics scraping configuration for an `InferenceProvider`.
///
/// The operator scrapes `{spec.endpoint}{path}` and parses the Prometheus text
/// using the `signal_names` mapping.  Signals without a configured name receive
/// the neutral default (`0.5`) in scoring.  Scrape failures are non-fatal:
/// the provider falls back to locality and cost scoring.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricsConfig {
    /// HTTP path for the Prometheus metrics endpoint, relative to `spec.endpoint`.
    ///
    /// Defaults to `"/metrics"` when absent.
    #[serde(default = "default_metrics_path")]
    pub path: String,

    /// Per-request scrape timeout (e.g. `"2s"`, `"500ms"`).
    ///
    /// Defaults to `"2s"`.  Only `s` and `ms` suffixes are recognised; unrecognised
    /// values fall back to the default.
    #[serde(default = "default_metrics_timeout")]
    pub timeout: String,

    /// Mapping from scoring signal names to Prometheus metric names.
    ///
    /// Signals without a configured name are skipped during parsing and receive the
    /// neutral default value (`0.5`) in scoring.  The operator does not normalise raw
    /// queue counts — `queueDepth` must be pre-normalised to 0.0–1.0 by the exporter.
    #[serde(default)]
    pub signal_names: MetricSignalNames,
}

/// Mapping from scoring signal names to Prometheus metric names.
///
/// Every field is optional.  A signal left as `None` is not extracted from the
/// Prometheus text output and receives the neutral default (`0.5`) in scoring.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricSignalNames {
    /// Metric name for normalised queue depth (0.0–1.0).
    pub queue_depth: Option<String>,

    /// Metric name for KV-cache utilisation (0.0–1.0).
    pub kv_cache_utilization: Option<String>,

    /// Metric name for P99 request latency in milliseconds (pre-computed gauge).
    pub latency_p99_ms: Option<String>,

    /// Metric name for prefix-cache hit ratio (0.0–1.0).
    pub prefix_cache_hit_ratio: Option<String>,

    /// Metric name for normalised error rate (0.0–1.0).
    pub error_rate: Option<String>,

    /// Metric name for a health gauge (any positive value = healthy).
    pub healthy: Option<String>,
}

/// Returns the default metrics scrape path.
fn default_metrics_path() -> String {
    "/metrics".to_owned()
}

/// Returns the default metrics scrape timeout.
fn default_metrics_timeout() -> String {
    "2s".to_owned()
}

/// Cost information for an inference provider.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CostConfig {
    /// Cost per million input tokens (USD).
    #[serde(default)]
    pub per_million_input_tokens: f64,

    /// Cost per million output tokens (USD).
    #[serde(default)]
    pub per_million_output_tokens: f64,
}

/// Model metadata for an inference provider.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelInfo {
    /// Model name.
    pub name: String,

    /// Supported capabilities.
    #[serde(default)]
    pub capabilities: Vec<String>,

    /// Maximum context window size.
    pub context_window: Option<u32>,
}

/// Health check configuration.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct HealthCheckConfig {
    /// Check interval (e.g. "30s").
    pub interval: Option<String>,

    /// HTTP path for health probes.
    pub path: Option<String>,

    /// Timeout per check (e.g. "5s").
    pub timeout: Option<String>,
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Observed status of an [`InferenceProvider`].
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InferenceProviderStatus {
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

/// Lifecycle phase of a provider resource.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum ProviderPhase {
    /// Waiting for validation.
    #[default]
    Pending,

    /// Provider is healthy and available for routing.
    Available,

    /// Provider is partially degraded.
    Degraded,

    /// Provider is not reachable.
    Unavailable,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use kube::CustomResourceExt as _;

    use super::*;

    fn crd_json() -> serde_json::Value {
        serde_json::to_value(InferenceProvider::crd()).unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn default_phase() {
        let phase = ProviderPhase::default();
        assert_eq!(phase, ProviderPhase::Pending, "should default to Pending");
    }

    #[test]
    fn spec_serde() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "providerKind": "anthropic",
            "backendKind": "api_provider",
            "endpoint": "https://api.anthropic.com",
            "models": [{"name": "claude-sonnet-4"}]
        });
        let spec: InferenceProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(spec.provider_kind, "anthropic", "provider kind");
        assert_eq!(spec.models.len(), 1, "model count");
    }

    #[test]
    fn inference_provider_crd_has_correct_group_and_plural() {
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
            "inferenceproviders",
            "wrong plural name"
        );
        assert_eq!(
            crd.get("spec")
                .and_then(|spec| spec.get("names"))
                .and_then(|names| names.get("kind"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| std::process::abort()),
            "InferenceProvider",
            "wrong kind name"
        );
    }

    #[test]
    fn inference_provider_crd_has_status_subresource() {
        let crd = crd_json();
        assert!(
            crd.pointer("/spec/versions/0/subresources/status").is_some(),
            "CRD must declare a status subresource"
        );
    }

    #[test]
    fn inference_provider_crd_has_site_selector_field() {
        let crd = crd_json();
        let spec_properties = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            spec_properties.contains_key("siteSelector"),
            "CRD schema must include siteSelector field"
        );
    }

    #[test]
    fn inference_provider_crd_has_metrics_config_field() {
        let crd = crd_json();
        let spec_properties = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            spec_properties.contains_key("metricsConfig"),
            "CRD schema must include metricsConfig field"
        );
    }

    #[test]
    fn metrics_config_absent_deserializes() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://backend:8080",
            "models": [{"name": "model-a"}]
        });
        let spec: InferenceProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert!(
            spec.metrics_config.is_none(),
            "metricsConfig must be absent when not set"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "tests multiple signal_names fields in one assertion block"
    )]
    fn metrics_config_with_path_and_signal_names_deserializes() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://backend:8080",
            "models": [{"name": "model-a"}],
            "metricsConfig": {
                "path": "/custom/metrics",
                "timeout": "500ms",
                "signalNames": {
                    "queueDepth": "provider_queue_depth",
                    "kvCacheUtilization": "provider_kv_cache"
                }
            }
        });
        let spec: InferenceProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let mc = spec.metrics_config.unwrap_or_else(|| std::process::abort());
        assert_eq!(mc.path, "/custom/metrics", "path must round-trip");
        assert_eq!(mc.timeout, "500ms", "timeout must round-trip");
        assert_eq!(
            mc.signal_names.queue_depth.as_deref(),
            Some("provider_queue_depth"),
            "queueDepth must round-trip"
        );
        assert_eq!(
            mc.signal_names.kv_cache_utilization.as_deref(),
            Some("provider_kv_cache"),
            "kvCacheUtilization must round-trip"
        );
        assert!(
            mc.signal_names.latency_p99_ms.is_none(),
            "unconfigured signal must be None"
        );
    }

    #[test]
    fn metrics_config_defaults_apply_when_fields_absent() {
        let json = serde_json::json!({
            "gridNetworkRef": "net",
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://backend:8080",
            "models": [{"name": "model-a"}],
            "metricsConfig": {}
        });
        let spec: InferenceProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let mc = spec.metrics_config.unwrap_or_else(|| std::process::abort());
        assert_eq!(mc.path, "/metrics", "path must default to /metrics");
        assert_eq!(mc.timeout, "2s", "timeout must default to 2s");
        assert!(
            mc.signal_names.queue_depth.is_none(),
            "signal_names must default to all-None"
        );
    }
}
