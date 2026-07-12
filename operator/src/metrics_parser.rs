// Copyright 2026 Praxis Proxy Authors
//! Pure Prometheus text-format parser for inference backend metrics.
//!
//! Parses the Prometheus exposition format (text, version 0.0.4) to extract
//! signal values for the Grid scoring engine.  No network calls are made here;
//! the caller is responsible for fetching the raw text from a backend's
//! `/metrics` endpoint and for scrape scheduling.
//!
//! ## Metric name mapping
//!
//! Because different inference servers expose the same signals under different
//! metric names, callers provide a `MetricNames` mapping.  Unrecognised or
//! unconfigured metric names are silently ignored; the corresponding signal
//! defaults to a neutral value that does not bias scoring.
//!
//! ## v1 limitations
//!
//! - **Gauge values only.**  Prometheus histogram-derived percentiles (e.g. latency P99 from bucket counts) are not
//!   supported.  The server must expose a pre-computed gauge, or a Prometheus recording rule must be configured to
//!   derive one.
//! - **First sample wins.**  Labels are stripped before metric-name matching. If multiple samples share the same metric
//!   name (different label sets), the first one encountered is used.
//! - **`queue_depth` normalisation.**  The `queue_depth` signal is expected to be in the range 0.0–1.0.  If the server
//!   exposes a raw queue length (an integer count), the caller must either normalise it externally (e.g. via a
//!   recording rule) or document the expected maximum.

use scoring::BackendMetrics;

// ---------------------------------------------------------------------------
// Neutral conversion defaults
// ---------------------------------------------------------------------------

/// Neutral score for missing runtime signals; mirrors the scoring crate's
/// private default for absent metrics.
const NEUTRAL_SIGNAL_SCORE: f64 = 0.5;

/// Maximum P99 latency used by the scoring crate for normalization.
///
/// A latency of 2500ms maps to a neutral latency score:
/// `1.0 - 2500 / 5000 = 0.5`.
const SCORING_MAX_LATENCY_MS: f64 = 5000.0;

// ---------------------------------------------------------------------------
// Metric name mapping
// ---------------------------------------------------------------------------

/// Configurable mapping from [`BackendMetrics`] fields to Prometheus metric
/// names.
///
/// Every field is `Option<String>`.  A `None` value means that signal is not
/// scraped; its corresponding field in [`PartialMetrics`] will remain `None`
/// and will be replaced by a neutral default when converting to
/// [`BackendMetrics`].
///
/// # Defaults
///
/// [`MetricNames::default()`] leaves all names as `None`.  Callers must set
/// the names that their inference backend exposes.
#[derive(Clone, Debug, Default)]
pub struct MetricNames {
    /// Metric name for request error rate (normalised 0.0–1.0).
    ///
    /// If absent and no `healthy` gauge is configured, the backend is assumed
    /// to have no errors.
    pub error_rate: Option<String>,

    /// Metric name for a health liveness gauge (any positive value = healthy).
    ///
    /// If absent, liveness is inferred: `error_rate < 1.0` → healthy.
    /// If both are absent, the backend is assumed healthy (conservative default).
    pub healthy: Option<String>,

    /// Metric name for KV cache utilisation (normalised 0.0–1.0).
    pub kv_cache_utilization: Option<String>,

    /// Metric name for observed P99 latency in **milliseconds**.
    ///
    /// Must be a pre-computed gauge.  Histogram-based P99 computation is not
    /// supported in v1; configure a recording rule if needed.
    pub latency_p99_ms: Option<String>,

    /// Metric name for prefix cache hit ratio (normalised 0.0–1.0).
    pub prefix_cache_hit_ratio: Option<String>,

    /// Metric name for normalised queue depth (0.0–1.0).
    ///
    /// The caller is responsible for normalisation.  Raw integer queue lengths
    /// must be normalised before being exposed as this metric, or via a
    /// recording rule.
    pub queue_depth: Option<String>,
}

// ---------------------------------------------------------------------------
// Partial metrics
// ---------------------------------------------------------------------------

/// Raw signal values extracted from a Prometheus scrape, before neutral
/// defaults are applied.
///
/// A `None` field means the corresponding metric name was not configured or
/// the metric was not present in the scraped text.  Convert to
/// [`BackendMetrics`] via [`PartialMetrics::into_backend_metrics`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartialMetrics {
    /// Parsed error rate, or `None` if absent.
    pub error_rate: Option<f64>,

    /// Parsed health gauge value, or `None` if absent.
    pub healthy: Option<f64>,

    /// Parsed KV cache utilisation, or `None` if absent.
    pub kv_cache_utilization: Option<f64>,

    /// Parsed P99 latency in milliseconds, or `None` if absent.
    pub latency_p99_ms: Option<f64>,

    /// Parsed prefix cache hit ratio, or `None` if absent.
    pub prefix_cache_hit_ratio: Option<f64>,

    /// Parsed normalised queue depth, or `None` if absent.
    pub queue_depth: Option<f64>,
}

impl PartialMetrics {
    /// Convert to a [`BackendMetrics`] value, replacing absent signals with
    /// neutral defaults that do not bias scoring.
    ///
    /// | Field | Default when absent |
    /// |-------|---------------------|
    /// | `healthy` | `true` — backend is assumed healthy until proven otherwise |
    /// | `error_rate` | `0.0` — no errors observed |
    /// | `queue_depth` | `0.5` — neutral score for missing queue data |
    /// | `kv_cache_utilization` | `0.5` — neutral score for missing cache data |
    /// | `latency_p99_ms` | `2500.0` — neutral score with current scoring normalization |
    /// | `prefix_cache_hit_ratio` | `0.5` — neutral score for missing prefix-cache data |
    ///
    /// The `healthy` bool is derived as follows:
    /// - If a health gauge was scraped: positive non-zero → `true`, zero → `false`.
    /// - Otherwise, if `error_rate` is present: `error_rate < 1.0` → `true`.
    /// - Otherwise: `true` (conservative default).
    #[must_use]
    pub fn into_backend_metrics(self) -> BackendMetrics {
        let healthy = match self.healthy {
            Some(h) => h > 0.0,
            None => self.error_rate.is_none_or(|e| e < 1.0),
        };
        BackendMetrics::new(
            self.error_rate.unwrap_or(0.0).clamp(0.0, 1.0),
            healthy,
            self.kv_cache_utilization
                .unwrap_or(NEUTRAL_SIGNAL_SCORE)
                .clamp(0.0, 1.0),
            self.latency_p99_ms
                .unwrap_or(NEUTRAL_SIGNAL_SCORE * SCORING_MAX_LATENCY_MS)
                .max(0.0),
            self.prefix_cache_hit_ratio
                .unwrap_or(NEUTRAL_SIGNAL_SCORE)
                .clamp(0.0, 1.0),
            self.queue_depth.unwrap_or(NEUTRAL_SIGNAL_SCORE).clamp(0.0, 1.0),
        )
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parse Prometheus text exposition format and extract configured metric values.
///
/// Reads each non-comment line in `text`, strips label selectors, and matches
/// the metric name against entries in `names`.  The first sample matching a
/// configured name is used; subsequent samples with the same name are ignored
/// (first-wins semantics, independent of label values).
///
/// Malformed lines — those that lack a parseable float value — are silently
/// skipped.  They are not treated as errors.
///
/// # Returns
///
/// A [`PartialMetrics`] where each field is `Some(value)` if the corresponding
/// name was configured and found, or `None` otherwise.
#[expect(
    clippy::too_many_lines,
    reason = "six assign-and-match calls read as a table; extraction would obscure intent"
)]
pub fn parse_prometheus_text(text: &str, names: &MetricNames) -> PartialMetrics {
    let mut result = PartialMetrics::default();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let Some(name_part) = parts.next() else {
            continue;
        };
        let metric_name = strip_labels(name_part);
        let Some(value) = parts
            .next()
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|v| v.is_finite())
        else {
            continue;
        };
        apply_if_match(&mut result.queue_depth, &names.queue_depth, metric_name, value);
        apply_if_match(
            &mut result.kv_cache_utilization,
            &names.kv_cache_utilization,
            metric_name,
            value,
        );
        apply_if_match(
            &mut result.prefix_cache_hit_ratio,
            &names.prefix_cache_hit_ratio,
            metric_name,
            value,
        );
        apply_if_match(&mut result.latency_p99_ms, &names.latency_p99_ms, metric_name, value);
        apply_if_match(&mut result.error_rate, &names.error_rate, metric_name, value);
        apply_if_match(&mut result.healthy, &names.healthy, metric_name, value);
    }
    result
}

/// Strip a Prometheus label selector from a metric name token.
///
/// `"my_metric{foo=\"bar\"}"` → `"my_metric"`.
/// Tokens without labels are returned unchanged.
fn strip_labels(name_with_labels: &str) -> &str {
    name_with_labels
        .split_once('{')
        .map_or(name_with_labels, |(name, _)| name)
}

/// Assign `value` to `target` if `configured` matches `metric_name` and
/// `target` is not already set (first-wins).
fn apply_if_match(target: &mut Option<f64>, configured: &Option<String>, metric_name: &str, value: f64) {
    if target.is_none() && configured.as_deref() == Some(metric_name) {
        *target = Some(value);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    fn names_for_all_signals() -> MetricNames {
        MetricNames {
            error_rate: Some("test_error_rate".to_owned()),
            healthy: Some("test_healthy".to_owned()),
            kv_cache_utilization: Some("test_kv_cache".to_owned()),
            latency_p99_ms: Some("test_latency_p99_ms".to_owned()),
            prefix_cache_hit_ratio: Some("test_prefix_cache".to_owned()),
            queue_depth: Some("test_queue_depth".to_owned()),
        }
    }

    // -----------------------------------------------------------------------
    // parse_prometheus_text — signal extraction
    // -----------------------------------------------------------------------

    #[test]
    fn parses_queue_depth_metric() {
        let text = "# HELP test_queue_depth Queue depth\ntest_queue_depth 0.75\n";
        let names = MetricNames {
            queue_depth: Some("test_queue_depth".to_owned()),
            ..Default::default()
        };
        let result = parse_prometheus_text(text, &names);
        assert_eq!(result.queue_depth, Some(0.75_f64), "queue depth must be parsed");
        assert!(result.kv_cache_utilization.is_none(), "other fields must be absent");
    }

    #[test]
    fn parses_kv_cache_metric() {
        let text = "test_kv_cache 0.42\n";
        let names = MetricNames {
            kv_cache_utilization: Some("test_kv_cache".to_owned()),
            ..Default::default()
        };
        let result = parse_prometheus_text(text, &names);
        assert_eq!(
            result.kv_cache_utilization,
            Some(0.42_f64),
            "KV cache utilisation must be parsed"
        );
    }

    #[test]
    fn parses_prefix_cache_metric() {
        let text = "test_prefix_cache 0.88\n";
        let names = MetricNames {
            prefix_cache_hit_ratio: Some("test_prefix_cache".to_owned()),
            ..Default::default()
        };
        let result = parse_prometheus_text(text, &names);
        assert_eq!(
            result.prefix_cache_hit_ratio,
            Some(0.88_f64),
            "prefix cache ratio must be parsed"
        );
    }

    #[test]
    fn parses_latency_p99_metric() {
        let text = "test_latency_p99_ms 123.4\n";
        let names = MetricNames {
            latency_p99_ms: Some("test_latency_p99_ms".to_owned()),
            ..Default::default()
        };
        let result = parse_prometheus_text(text, &names);
        assert_eq!(result.latency_p99_ms, Some(123.4_f64), "P99 latency must be parsed");
    }

    #[test]
    fn parses_error_rate_and_healthy_metrics() {
        let text = "test_error_rate 0.05\ntest_healthy 1\n";
        let names = names_for_all_signals();
        let result = parse_prometheus_text(text, &names);
        assert_eq!(result.error_rate, Some(0.05_f64));
        assert_eq!(result.healthy, Some(1.0_f64));
    }

    #[test]
    fn missing_metrics_are_absent_in_partial() {
        let text = "test_queue_depth 0.5\n";
        let names = names_for_all_signals();
        let result = parse_prometheus_text(text, &names);
        assert_eq!(result.queue_depth, Some(0.5_f64), "configured and present → Some");
        assert!(result.kv_cache_utilization.is_none(), "not in text → None");
        assert!(result.latency_p99_ms.is_none(), "not in text → None");
        assert!(result.prefix_cache_hit_ratio.is_none(), "not in text → None");
    }

    #[test]
    fn malformed_lines_do_not_panic_and_are_skipped() {
        // Lines without a parseable float value must be skipped silently.
        // "not_a_number" cannot be parsed as f64 → the metric is absent.
        // A bare metric name with no value token is also skipped.
        let text = "not_a_metric_line\n\
                    test_queue_depth   \n\
                    test_queue_depth not_a_number\n\
                    test_kv_cache 0.3\n";
        let names = MetricNames {
            queue_depth: Some("test_queue_depth".to_owned()),
            kv_cache_utilization: Some("test_kv_cache".to_owned()),
            ..Default::default()
        };
        let result = parse_prometheus_text(text, &names);
        assert!(
            result.queue_depth.is_none(),
            "unparseable queue depth value must be skipped"
        );
        assert_eq!(
            result.kv_cache_utilization,
            Some(0.3_f64),
            "valid line after malformed must still be parsed"
        );
    }

    #[test]
    fn duplicate_metric_name_first_sample_wins() {
        // When the same metric name appears twice (e.g. with different labels),
        // the first value is used and subsequent values are ignored.
        let text = "test_queue_depth 0.1\ntest_queue_depth 0.9\n";
        let names = MetricNames {
            queue_depth: Some("test_queue_depth".to_owned()),
            ..Default::default()
        };
        let result = parse_prometheus_text(text, &names);
        assert_eq!(
            result.queue_depth,
            Some(0.1_f64),
            "first sample must win when names collide"
        );
    }

    #[test]
    fn unrelated_metrics_are_ignored() {
        let text = "unrelated_counter 999\nunrelated_gauge 1.0\ntest_kv_cache 0.5\n";
        let names = MetricNames {
            kv_cache_utilization: Some("test_kv_cache".to_owned()),
            ..Default::default()
        };
        let result = parse_prometheus_text(text, &names);
        assert_eq!(
            result.kv_cache_utilization,
            Some(0.5_f64),
            "configured metric must be extracted"
        );
        assert!(
            result.queue_depth.is_none(),
            "unrelated metric must not populate queue_depth"
        );
    }

    #[test]
    fn labels_are_stripped_before_matching() {
        // Labels in braces must be stripped; only the base metric name is matched.
        let text = r#"test_queue_depth{model="llama",pod="0"} 0.6
test_kv_cache{model="llama"} 0.3
"#;
        let names = MetricNames {
            queue_depth: Some("test_queue_depth".to_owned()),
            kv_cache_utilization: Some("test_kv_cache".to_owned()),
            ..Default::default()
        };
        let result = parse_prometheus_text(text, &names);
        assert_eq!(
            result.queue_depth,
            Some(0.6_f64),
            "labels must be stripped for queue depth"
        );
        assert_eq!(
            result.kv_cache_utilization,
            Some(0.3_f64),
            "labels must be stripped for kv cache"
        );
    }

    #[test]
    fn tab_separated_metric_line_is_parsed() {
        let text = "test_queue_depth\t0.44\n";
        let names = MetricNames {
            queue_depth: Some("test_queue_depth".to_owned()),
            ..Default::default()
        };
        let result = parse_prometheus_text(text, &names);
        assert_eq!(
            result.queue_depth,
            Some(0.44_f64),
            "Prometheus whitespace should not be limited to spaces"
        );
    }

    #[test]
    fn non_finite_metric_values_are_skipped() {
        let text = "test_queue_depth NaN\ntest_kv_cache +Inf\n";
        let names = MetricNames {
            queue_depth: Some("test_queue_depth".to_owned()),
            kv_cache_utilization: Some("test_kv_cache".to_owned()),
            ..Default::default()
        };
        let result = parse_prometheus_text(text, &names);
        assert!(result.queue_depth.is_none(), "NaN values must be skipped");
        assert!(result.kv_cache_utilization.is_none(), "infinite values must be skipped");
    }

    #[test]
    fn comment_and_type_lines_are_skipped() {
        let text = "# HELP test_queue_depth Queue depth (normalised)\n\
                    # TYPE test_queue_depth gauge\n\
                    test_queue_depth 0.55\n";
        let names = MetricNames {
            queue_depth: Some("test_queue_depth".to_owned()),
            ..Default::default()
        };
        let result = parse_prometheus_text(text, &names);
        assert_eq!(
            result.queue_depth,
            Some(0.55_f64),
            "metric line after comments must be parsed"
        );
    }

    #[test]
    fn empty_text_returns_all_absent() {
        let result = parse_prometheus_text("", &names_for_all_signals());
        assert_eq!(
            result,
            PartialMetrics::default(),
            "empty text must yield all-None partial"
        );
    }

    #[test]
    fn unconfigured_names_do_not_match_anything() {
        let text = "test_queue_depth 0.5\n";
        let names = MetricNames::default(); // all None
        let result = parse_prometheus_text(text, &names);
        assert_eq!(
            result,
            PartialMetrics::default(),
            "no configured names → all fields remain None"
        );
    }

    // -----------------------------------------------------------------------
    // PartialMetrics::into_backend_metrics — default/healthy derivation
    // -----------------------------------------------------------------------

    #[test]
    fn missing_fields_use_neutral_defaults() {
        let partial = PartialMetrics::default();
        let metrics = partial.into_backend_metrics();
        assert!(metrics.healthy, "absent healthy must default to true");
        assert_eq!(metrics.error_rate, 0.0, "absent error_rate must default to 0.0");
        assert_eq!(
            metrics.queue_depth, 0.5,
            "absent queue_depth must default to neutral 0.5"
        );
        assert_eq!(
            metrics.kv_cache_utilization, 0.5,
            "absent kv_cache must default to neutral 0.5"
        );
        assert_eq!(
            metrics.latency_p99_ms, 2500.0,
            "absent latency must default to neutral 2500ms"
        );
        assert_eq!(
            metrics.prefix_cache_hit_ratio, 0.5,
            "absent prefix cache must default to neutral 0.5"
        );
    }

    #[test]
    fn out_of_range_signals_are_clamped_on_conversion() {
        let partial = PartialMetrics {
            error_rate: Some(1.2),
            kv_cache_utilization: Some(1.5),
            latency_p99_ms: Some(-10.0),
            prefix_cache_hit_ratio: Some(-0.25),
            queue_depth: Some(2.0),
            ..Default::default()
        };
        let metrics = partial.into_backend_metrics();
        assert_eq!(metrics.error_rate, 1.0, "error_rate must be clamped to 1.0");
        assert_eq!(
            metrics.kv_cache_utilization, 1.0,
            "kv cache utilisation must be clamped to 1.0"
        );
        assert_eq!(metrics.latency_p99_ms, 0.0, "latency must not go below 0.0");
        assert_eq!(
            metrics.prefix_cache_hit_ratio, 0.0,
            "prefix cache ratio must be clamped to 0.0"
        );
        assert_eq!(metrics.queue_depth, 1.0, "queue depth must be clamped to 1.0");
    }

    #[test]
    fn health_gauge_present_and_positive_means_healthy() {
        let partial = PartialMetrics {
            healthy: Some(1.0),
            ..Default::default()
        };
        assert!(
            partial.into_backend_metrics().healthy,
            "positive health gauge → healthy"
        );
    }

    #[test]
    fn health_gauge_zero_means_unhealthy() {
        let partial = PartialMetrics {
            healthy: Some(0.0),
            ..Default::default()
        };
        assert!(!partial.into_backend_metrics().healthy, "zero health gauge → unhealthy");
    }

    #[test]
    fn error_rate_1_infers_unhealthy_when_no_health_gauge() {
        let partial = PartialMetrics {
            error_rate: Some(1.0),
            ..Default::default()
        };
        assert!(
            !partial.into_backend_metrics().healthy,
            "error_rate=1.0 must infer unhealthy"
        );
    }

    #[test]
    fn partial_error_rate_infers_healthy() {
        let partial = PartialMetrics {
            error_rate: Some(0.1),
            ..Default::default()
        };
        assert!(
            partial.into_backend_metrics().healthy,
            "error_rate < 1.0 must infer healthy"
        );
    }

    #[test]
    fn health_gauge_takes_precedence_over_error_rate() {
        // healthy=0 (unhealthy gauge) even though error_rate < 1.0 → unhealthy.
        let partial = PartialMetrics {
            healthy: Some(0.0),
            error_rate: Some(0.1),
            ..Default::default()
        };
        assert!(
            !partial.into_backend_metrics().healthy,
            "explicit health gauge must take precedence over error_rate inference"
        );
    }

    #[test]
    fn full_round_trip_all_signals() {
        let text = "test_queue_depth 0.3\ntest_kv_cache 0.6\ntest_prefix_cache 0.9\n\
                    test_latency_p99_ms 250.0\ntest_error_rate 0.02\ntest_healthy 1\n";
        let names = names_for_all_signals();
        let partial = parse_prometheus_text(text, &names);
        let metrics = partial.into_backend_metrics();
        assert_eq!(metrics.queue_depth, 0.3);
        assert_eq!(metrics.kv_cache_utilization, 0.6);
        assert_eq!(metrics.prefix_cache_hit_ratio, 0.9);
        assert_eq!(metrics.latency_p99_ms, 250.0);
        assert_eq!(metrics.error_rate, 0.02);
        assert!(metrics.healthy);
    }
}
