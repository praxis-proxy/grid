//! The PromQL set evaluated per site and the health thresholds. Both are
//! overridable from the config file so a fleet with different exporters
//! changes values, not code.

use std::collections::BTreeMap;

/// Shipped with the binary; a decode failure is a build defect.
const DEFAULTS_YAML: &str = include_str!("defaults.yaml");

/// One PromQL query, keyed as the collector refers to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    /// Collector key such as `gpuUtil`.
    pub key: String,
    /// The PromQL expression; empty when the key is disabled.
    pub promql: String,
}

/// PromQL per key. An empty expression disables its key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuerySet(BTreeMap<String, String>);

impl QuerySet {
    /// The query set embedded in the binary.
    ///
    /// # Errors
    ///
    /// Returns the YAML error when the embedded defaults do not decode, which
    /// only a defective build can cause.
    pub fn defaults() -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(DEFAULTS_YAML).map(Self)
    }

    /// A copy with `overrides` layered on top; an override may enable or
    /// disable a key as well as change its expression.
    #[must_use]
    pub fn with(&self, overrides: &BTreeMap<String, String>) -> Self {
        let mut merged = self.0.clone();
        merged.extend(overrides.iter().map(|(key, promql)| (key.clone(), promql.clone())));
        Self(merged)
    }

    /// The query for `key`; its expression is empty when the key is unknown
    /// or disabled.
    #[must_use]
    pub fn query(&self, key: &str) -> Query {
        Query {
            key: key.to_owned(),
            promql: self.0.get(key).cloned().unwrap_or_default(),
        }
    }

    /// Whether `key` has a non-empty expression.
    #[must_use]
    pub fn enabled(&self, key: &str) -> bool {
        self.0.get(key).is_some_and(|promql| !promql.is_empty())
    }
}

/// Where a site turns yellow or red.
#[derive(Debug, Clone, PartialEq)]
pub struct Thresholds {
    /// GPU utilization percent at or above which a site is yellow.
    pub gpu_util_warn: f64,
    /// Queue depth at or above which a site is yellow.
    pub queue_warn: f64,
    /// Median latency in milliseconds at or above which a site is yellow.
    pub latency_warn_ms: f64,
    /// Consecutive unreachable polls at or above which a site is red.
    pub red_after_failures: u32,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            gpu_util_warn: 90.0,
            queue_warn: 50.0,
            latency_warn_ms: 5000.0,
            red_after_failures: 2,
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use std::collections::BTreeMap;

    use super::{QuerySet, Thresholds};

    const EVERY_KEY: [&str; 11] = [
        "readyEndpoints",
        "gpuTotal",
        "gpuUtil",
        "gpuUtilFallback",
        "models",
        "rps",
        "p50LatencyMs",
        "tokensPerSec",
        "queueDepth",
        "replicasDown",
        "tenants",
    ];

    #[test]
    fn defaults_define_every_key_and_disable_tenants() {
        let set = QuerySet::defaults().unwrap();
        for key in EVERY_KEY {
            assert!(
                !set.query(key).promql.is_empty() || key == "tenants",
                "default set lacks key {key}"
            );
        }
        assert!(!set.enabled("tenants"), "tenants must be disabled by default");
        assert!(set.enabled("gpuUtil"), "gpuUtil must be enabled by default");
    }

    #[test]
    fn overrides_replace_and_extend_without_mutating_defaults() {
        let overrides = BTreeMap::from([
            ("gpuUtil".to_owned(), "avg(custom_util)".to_owned()),
            ("tenants".to_owned(), "sum by (tenant) (x)".to_owned()),
        ]);
        let set = QuerySet::defaults().unwrap().with(&overrides);
        assert_eq!(set.query("gpuUtil").promql, "avg(custom_util)", "override must win");
        assert_eq!(set.query("gpuUtil").key, "gpuUtil", "key travels with the query");
        assert!(set.enabled("tenants"), "an override can enable a disabled key");
        assert!(set.enabled("rps"), "untouched keys must survive the merge");
        assert_ne!(
            QuerySet::defaults().unwrap().query("gpuUtil").promql,
            "avg(custom_util)",
            "defaults are not mutated"
        );
    }

    #[test]
    fn an_empty_promql_disables_the_key() {
        let overrides = BTreeMap::from([("gpuUtil".to_owned(), String::new())]);
        let set = QuerySet::defaults().unwrap().with(&overrides);
        assert!(!set.enabled("gpuUtil"), "an empty PromQL string disables the key");
    }

    #[test]
    fn default_thresholds_match_the_chart_values() {
        let thresholds = Thresholds::default();
        assert_eq!(
            (
                thresholds.gpu_util_warn,
                thresholds.queue_warn,
                thresholds.latency_warn_ms,
                thresholds.red_after_failures
            ),
            (90.0, 50.0, 5000.0, 2),
            "defaults must match values.yaml"
        );
    }
}
