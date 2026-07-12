//! Grid overlay ConfigMap builder for Praxis integration.
//!
//! Generates a ConfigMap containing grid-specific cluster
//! definitions (with mTLS configuration), scoring filter
//! config, and auth injection config. Praxis merges this
//! overlay with its base configuration.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use serde::Serialize;

// ---------------------------------------------------------------------------
// Overlay Config Types
// ---------------------------------------------------------------------------

/// Grid overlay configuration written to a `ConfigMap`.
///
/// Praxis reads this to configure grid-aware routing,
/// remote site clusters, and credential injection.
#[derive(Clone, Debug, Default, Serialize)]
pub struct GridOverlay {
    /// Cluster definitions for remote grid sites.
    #[serde(default)]
    pub clusters: Vec<OverlayCluster>,

    /// Grid scoring filter configuration.
    #[serde(default)]
    pub scoring: OverlayScoringConfig,
}

/// A cluster definition in the grid overlay.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct OverlayCluster {
    /// Cluster name (e.g. `grid~cluster-b~8443`).
    pub name: String,

    /// Cost per 1k input tokens.
    #[serde(default)]
    pub cost_per_1k_input: f64,

    /// Endpoint addresses.
    pub endpoints: Vec<String>,

    /// Backend kind (`local`, `remote`, `api_provider`, `cloud_managed`).
    pub kind: String,

    /// Provider kind (`open_ai`, `anthropic`, `bedrock`, `vertex`).
    pub provider: String,

    /// TLS configuration for this cluster.
    pub tls: Option<OverlayTls>,
}

/// TLS configuration for a grid overlay cluster.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct OverlayTls {
    /// Path to the CA certificate for verification.
    pub ca: Option<String>,

    /// Path to the client certificate (for mTLS).
    pub client_cert: Option<String>,

    /// Path to the client key (for mTLS).
    pub client_key: Option<String>,

    /// SNI hostname.
    pub sni: Option<String>,

    /// Whether to verify the server certificate.
    pub verify: bool,
}

/// Scoring configuration in the grid overlay.
#[derive(Clone, Debug, Default, Serialize)]
pub struct OverlayScoringConfig {
    /// Backend list for the scoring filter.
    #[serde(default)]
    pub backends: Vec<OverlayScoringBackend>,

    /// Scoring weights.
    #[serde(default)]
    pub weights: OverlayScoringWeights,
}

/// A backend entry in the scoring config.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct OverlayScoringBackend {
    /// Cluster name (must match an overlay cluster).
    pub cluster: String,

    /// Cost per 1k input tokens.
    #[serde(default)]
    pub cost_per_1k_input: f64,

    /// Backend kind.
    pub kind: String,

    /// Provider kind.
    pub provider: String,
}

/// Scoring weights in the overlay config.
#[derive(Clone, Debug, Serialize)]
pub struct OverlayScoringWeights {
    /// Weight for cost signal.
    pub cost: f64,

    /// Weight for KV cache signal.
    pub kv_cache: f64,

    /// Weight for latency signal.
    pub latency: f64,

    /// Weight for locality signal.
    pub locality: f64,

    /// Weight for prefix cache signal.
    pub prefix_cache: f64,

    /// Weight for queue depth signal.
    pub queue_depth: f64,
}

impl Default for OverlayScoringWeights {
    fn default() -> Self {
        Self {
            cost: 1.0,
            kv_cache: 2.0,
            latency: 2.0,
            locality: 3.0,
            prefix_cache: 2.0,
            queue_depth: 3.0,
        }
    }
}

// ---------------------------------------------------------------------------
// ConfigMap Builder
// ---------------------------------------------------------------------------

/// Build a grid overlay `ConfigMap` from the given config.
///
/// The overlay is serialized as JSON under the `grid-config.json` data key.
/// Serialization failure silently falls back to an empty string; use
/// [`crate::resources::routing_overlay::build_overlay_configmap`] for
/// production use where failures must propagate.
pub fn build_overlay_configmap(name: &str, namespace: &str, overlay: &GridOverlay) -> ConfigMap {
    let json = serde_json::to_string_pretty(overlay).unwrap_or_default();

    let mut data = BTreeMap::new();
    data.insert("grid-config.json".to_owned(), json);

    ConfigMap {
        metadata: kube::api::ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_overlay_serializes() {
        let overlay = GridOverlay::default();
        let cm = build_overlay_configmap("grid-overlay-gw", "praxis-system", &overlay);
        assert_eq!(cm.metadata.name.as_deref(), Some("grid-overlay-gw"), "name");
        let data = cm.data.as_ref();
        assert!(data.is_some(), "should have data");
        assert!(
            data.is_some_and(|d| d.contains_key("grid-config.json")),
            "should have config key"
        );
    }

    #[test]
    fn overlay_with_cluster() {
        let overlay = GridOverlay {
            clusters: vec![OverlayCluster {
                name: "grid~cluster-b~8443".to_owned(),
                cost_per_1k_input: 0.01,
                endpoints: vec!["10.0.0.2:8443".to_owned()],
                kind: "remote".to_owned(),
                provider: "open_ai".to_owned(),
                tls: Some(OverlayTls {
                    ca: Some("/grid-tls/ca.pem".to_owned()),
                    client_cert: Some("/grid-tls/tls.crt".to_owned()),
                    client_key: Some("/grid-tls/tls.key".to_owned()),
                    sni: Some("cluster-b.grid.internal".to_owned()),
                    verify: true,
                }),
            }],
            scoring: OverlayScoringConfig::default(),
        };
        let cm = build_overlay_configmap("test", "ns", &overlay);
        let json_str = cm
            .data
            .as_ref()
            .and_then(|d| d.get("grid-config.json"))
            .cloned()
            .unwrap_or_default();
        assert!(json_str.contains("cluster-b"), "should contain cluster name");
    }

    #[test]
    fn default_scoring_weights() {
        let w = OverlayScoringWeights::default();
        assert_eq!(w.locality, 3.0, "locality default");
        assert_eq!(w.queue_depth, 3.0, "queue_depth default");
    }
}
