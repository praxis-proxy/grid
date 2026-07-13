//! Provider metrics collection for the [`GridNetwork`] overlay renderer.
//!
//! Scrapes Prometheus `/metrics` endpoints from `InferenceProvider` resources that have
//! `spec.metricsConfig` configured, parses the text with the configured signal
//! names, and returns a map keyed by provider routing identity for use with
//! `render_routing_overlay`.
//!
//! Scrape and parse failures are non-fatal: the provider is omitted from the
//! returned map and falls back to locality and cost scoring.
//!
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork

use std::{collections::HashMap, time::Duration};

use crate::{
    crd::inference_provider::{InferenceProvider, MetricSignalNames},
    metrics_parser::{MetricNames, parse_prometheus_text},
    metrics_scraper::scrape_metrics,
    resources::routing_overlay::routing_identity,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Scrape timeout used when the provider's `metricsConfig.timeout` cannot be parsed.
const DEFAULT_SCRAPE_TIMEOUT: Duration = Duration::from_secs(2);

// ---------------------------------------------------------------------------
// URL construction
// ---------------------------------------------------------------------------

/// Construct the metrics scrape URL from a provider endpoint and configured path.
///
/// Trims a trailing `/` from `endpoint` before appending `path`.  If `path`
/// does not start with `/`, one is prepended.
///
/// ```text
/// metrics_url("http://backend:8080",  "/metrics") → "http://backend:8080/metrics"
/// metrics_url("http://backend:8080/", "/metrics") → "http://backend:8080/metrics"
/// metrics_url("http://backend:8080",  "metrics")  → "http://backend:8080/metrics"
/// ```
pub(crate) fn metrics_url(endpoint: &str, path: &str) -> String {
    let base = endpoint.trim_end_matches('/');
    if path.starts_with('/') {
        format!("{base}{path}")
    } else {
        format!("{base}/{path}")
    }
}

// ---------------------------------------------------------------------------
// Config conversion
// ---------------------------------------------------------------------------

/// Convert a [`MetricSignalNames`] CRD field to a [`MetricNames`] parser config.
///
/// Signal fields that are `None` in the CRD remain `None` in the parser config
/// and are not extracted from the Prometheus text.
pub(crate) fn metric_names_from_config(cfg: &MetricSignalNames) -> MetricNames {
    MetricNames {
        queue_depth: cfg.queue_depth.clone(),
        kv_cache_utilization: cfg.kv_cache_utilization.clone(),
        latency_p99_ms: cfg.latency_p99_ms.clone(),
        prefix_cache_hit_ratio: cfg.prefix_cache_hit_ratio.clone(),
        error_rate: cfg.error_rate.clone(),
        healthy: cfg.healthy.clone(),
    }
}

// ---------------------------------------------------------------------------
// Timeout parsing
// ---------------------------------------------------------------------------

/// Parse a timeout string (`"2s"`, `"500ms"`) to a [`Duration`].
///
/// Supports `s` and `ms` suffixes only; minutes and bare numbers are not
/// recognised.  Returns [`DEFAULT_SCRAPE_TIMEOUT`] for unrecognised formats,
/// empty strings, or zero values.
pub(crate) fn parse_metrics_timeout(s: &str) -> Duration {
    let s = s.trim();
    if let Some(ms_str) = s.strip_suffix("ms")
        && let Ok(n) = ms_str.trim().parse::<u64>()
        && n > 0
    {
        return Duration::from_millis(n);
    }
    if let Some(s_str) = s.strip_suffix('s')
        && let Ok(n) = s_str.trim().parse::<u64>()
        && n > 0
    {
        return Duration::from_secs(n);
    }
    DEFAULT_SCRAPE_TIMEOUT
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

/// Scrape and parse live metrics for providers in `network_name` that have `spec.metricsConfig`.
///
/// Returns a map from provider routing identity (the value of
/// `spec.routingClusterRef`, or `metadata.name` when absent) to
/// [`scoring::BackendMetrics`].
///
/// Providers without `metricsConfig` or with a blank endpoint are skipped and
/// not present in the returned map.  Scrape and parse failures are logged at
/// `warn` level; those providers are also omitted from the map and fall back
/// to neutral scoring in the overlay renderer.
///
/// Returns an empty map when no providers have `metricsConfig` or when all
/// scrapes fail.  The caller should pass `None` to `render_routing_overlay`
/// in that case to preserve static ordering.
#[expect(
    clippy::too_many_lines,
    reason = "sequential per-provider scrape loop with early-continue guards and error logging"
)]
pub(crate) async fn collect_provider_metrics(
    network_name: &str,
    providers: &[InferenceProvider],
) -> HashMap<String, scoring::BackendMetrics> {
    let mut result = HashMap::new();
    for provider in providers {
        if provider.spec.grid_network_ref != network_name {
            continue;
        }
        let Some(mc) = &provider.spec.metrics_config else {
            continue;
        };
        let Some(identity) = routing_identity(provider) else {
            continue;
        };
        let endpoint = provider.spec.endpoint.trim();
        if endpoint.is_empty() {
            continue;
        }
        let url = metrics_url(endpoint, &mc.path);
        let timeout = parse_metrics_timeout(&mc.timeout);
        let names = metric_names_from_config(&mc.signal_names);
        match scrape_metrics(&url, timeout).await {
            Ok(text) => {
                let bm = parse_prometheus_text(&text, &names).into_backend_metrics();
                result.insert(identity.to_owned(), bm);
            },
            Err(e) => {
                tracing::warn!(
                    provider = identity,
                    url = %url,
                    error = %e,
                    "metrics scrape failed; provider will use neutral scoring"
                );
            },
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;
    use crate::crd::inference_provider::MetricsConfig;

    // -----------------------------------------------------------------------
    // Test utilities
    // -----------------------------------------------------------------------

    /// Start a one-shot HTTP server that returns the given raw response bytes.
    ///
    /// Returns the bound `http://127.0.0.1:{port}` base URL.
    async fn start_test_server(response: Vec<u8>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|_| std::process::abort());
        let port = listener.local_addr().unwrap_or_else(|_| std::process::abort()).port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0_u8; 4096];
                drop(stream.read(&mut buf).await);
                drop(stream.write_all(&response).await);
            }
        });
        format!("http://127.0.0.1:{port}")
    }

    /// Build a raw HTTP 200 response with a text/plain body.
    fn ok_response(body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    /// Build a raw HTTP error response with an empty body.
    fn err_response(status: u16) -> Vec<u8> {
        format!("HTTP/1.0 {status} Error\r\nContent-Length: 0\r\n\r\n").into_bytes()
    }

    fn provider_fixture(name: &str, endpoint: &str, mc: Option<MetricsConfig>) -> InferenceProvider {
        let mut spec = serde_json::json!({
            "gridNetworkRef": "net",
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": endpoint,
            "models": [{"name": "model-a"}]
        });
        if let Some(m) = mc
            && let Some(s) = spec.as_object_mut()
        {
            s.insert(
                "metricsConfig".to_owned(),
                serde_json::to_value(m).unwrap_or_else(|_| std::process::abort()),
            );
        }
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": {"name": name},
            "spec": spec
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn mc_with_queue(metric_name: &str) -> MetricsConfig {
        MetricsConfig {
            path: "/metrics".to_owned(),
            timeout: "2s".to_owned(),
            signal_names: MetricSignalNames {
                queue_depth: Some(metric_name.to_owned()),
                ..Default::default()
            },
        }
    }

    // -----------------------------------------------------------------------
    // URL construction
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_url_appends_path_to_endpoint() {
        assert_eq!(
            metrics_url("http://backend:8080", "/metrics"),
            "http://backend:8080/metrics"
        );
    }

    #[test]
    fn metrics_url_trims_trailing_slash_from_endpoint() {
        assert_eq!(
            metrics_url("http://backend:8080/", "/metrics"),
            "http://backend:8080/metrics"
        );
    }

    #[test]
    fn metrics_url_prepends_slash_when_path_lacks_one() {
        assert_eq!(
            metrics_url("http://backend:8080", "metrics"),
            "http://backend:8080/metrics"
        );
    }

    #[test]
    fn metrics_url_with_custom_path() {
        assert_eq!(
            metrics_url("http://backend:8080", "/custom/prometheus"),
            "http://backend:8080/custom/prometheus"
        );
    }

    // -----------------------------------------------------------------------
    // Timeout parsing
    // -----------------------------------------------------------------------

    #[test]
    fn parse_metrics_timeout_seconds() {
        assert_eq!(parse_metrics_timeout("2s"), Duration::from_secs(2));
        assert_eq!(parse_metrics_timeout("10s"), Duration::from_secs(10));
    }

    #[test]
    fn parse_metrics_timeout_milliseconds() {
        assert_eq!(parse_metrics_timeout("500ms"), Duration::from_millis(500));
        assert_eq!(parse_metrics_timeout("100ms"), Duration::from_millis(100));
    }

    #[test]
    fn parse_metrics_timeout_invalid_returns_default() {
        assert_eq!(
            parse_metrics_timeout("5m"),
            DEFAULT_SCRAPE_TIMEOUT,
            "minutes not supported"
        );
        assert_eq!(
            parse_metrics_timeout("5"),
            DEFAULT_SCRAPE_TIMEOUT,
            "bare number not supported"
        );
        assert_eq!(parse_metrics_timeout(""), DEFAULT_SCRAPE_TIMEOUT, "empty string");
        assert_eq!(parse_metrics_timeout("abc"), DEFAULT_SCRAPE_TIMEOUT, "non-numeric");
        assert_eq!(parse_metrics_timeout("0s"), DEFAULT_SCRAPE_TIMEOUT, "zero seconds");
    }

    // -----------------------------------------------------------------------
    // Config conversion
    // -----------------------------------------------------------------------

    #[test]
    fn metric_names_from_config_maps_all_signal_names() {
        let cfg = MetricSignalNames {
            queue_depth: Some("my_queue".to_owned()),
            kv_cache_utilization: Some("my_kv".to_owned()),
            latency_p99_ms: Some("my_latency".to_owned()),
            prefix_cache_hit_ratio: Some("my_prefix".to_owned()),
            error_rate: Some("my_errors".to_owned()),
            healthy: Some("my_health".to_owned()),
        };
        let names = metric_names_from_config(&cfg);
        assert_eq!(names.queue_depth.as_deref(), Some("my_queue"));
        assert_eq!(names.kv_cache_utilization.as_deref(), Some("my_kv"));
        assert_eq!(names.latency_p99_ms.as_deref(), Some("my_latency"));
        assert_eq!(names.prefix_cache_hit_ratio.as_deref(), Some("my_prefix"));
        assert_eq!(names.error_rate.as_deref(), Some("my_errors"));
        assert_eq!(names.healthy.as_deref(), Some("my_health"));
    }

    #[test]
    fn metric_names_from_config_maps_none_for_absent_signals() {
        let names = metric_names_from_config(&MetricSignalNames::default());
        assert!(names.queue_depth.is_none());
        assert!(names.kv_cache_utilization.is_none());
        assert!(names.latency_p99_ms.is_none());
        assert!(names.prefix_cache_hit_ratio.is_none());
        assert!(names.error_rate.is_none());
        assert!(names.healthy.is_none());
    }

    // -----------------------------------------------------------------------
    // collect_provider_metrics
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn collect_metrics_no_config_returns_empty_map() {
        let provider = provider_fixture("prov-a", "http://127.0.0.1:9999", None);
        let result = collect_provider_metrics("net", &[provider]).await;
        assert!(
            result.is_empty(),
            "provider without metricsConfig must not appear in metrics map"
        );
    }

    #[tokio::test]
    async fn collect_metrics_blank_endpoint_is_skipped() {
        let provider = provider_fixture("prov-a", "", Some(mc_with_queue("my_queue")));
        let result = collect_provider_metrics("net", &[provider]).await;
        assert!(result.is_empty(), "provider with blank endpoint must not be scraped");
    }

    #[tokio::test]
    async fn collect_metrics_valid_scrape_inserts_backend_metrics() {
        let body = "my_queue 0.2\n";
        let base_url = start_test_server(ok_response(body)).await;
        let provider = provider_fixture("prov-a", &base_url, Some(mc_with_queue("my_queue")));

        let result = collect_provider_metrics("net", &[provider]).await;
        assert!(
            result.contains_key("prov-a"),
            "provider must appear in metrics map after successful scrape"
        );
        let bm = result.get("prov-a").copied().unwrap_or_else(|| std::process::abort());
        assert!(bm.queue_depth.is_finite(), "queue_depth must be finite");
        assert!(
            bm.queue_depth >= 0.0 && bm.queue_depth <= 1.0,
            "queue_depth must be in [0,1]"
        );
    }

    #[tokio::test]
    async fn collect_metrics_uses_routing_identity_as_key() {
        let body = "my_queue 0.3\n";
        let base_url = start_test_server(ok_response(body)).await;
        let provider: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": {"name": "prov-a"},
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": base_url,
                "models": [{"name": "model-a"}],
                "routingClusterRef": "site-x",
                "metricsConfig": {
                    "path": "/metrics",
                    "timeout": "2s",
                    "signalNames": {"queueDepth": "my_queue"}
                }
            }
        }))
        .unwrap_or_else(|_| std::process::abort());

        let result = collect_provider_metrics("net", &[provider]).await;
        assert!(
            result.contains_key("site-x"),
            "metrics must be keyed by routingClusterRef, not metadata.name"
        );
        assert!(
            !result.contains_key("prov-a"),
            "metadata.name must not be used as key when routingClusterRef is set"
        );
    }

    #[tokio::test]
    async fn collect_metrics_scrape_failure_logs_and_omits_provider() {
        // Port 1 is never open — connection will be refused.
        let provider = provider_fixture("prov-a", "http://127.0.0.1:1", Some(mc_with_queue("my_queue")));
        let result = collect_provider_metrics("net", &[provider]).await;
        assert!(
            result.is_empty(),
            "failed scrape must not panic and must omit provider from map"
        );
    }

    #[tokio::test]
    async fn collect_metrics_non_2xx_omits_provider() {
        let base_url = start_test_server(err_response(503)).await;
        let provider = provider_fixture("prov-a", &base_url, Some(mc_with_queue("my_queue")));
        let result = collect_provider_metrics("net", &[provider]).await;
        assert!(
            result.is_empty(),
            "non-2xx response must omit provider from metrics map"
        );
    }

    #[tokio::test]
    async fn collect_metrics_malformed_body_produces_finite_metrics() {
        // Malformed Prometheus text — metric not found → neutral defaults.
        let body = "not_prometheus_text {invalid} NaN\n";
        let base_url = start_test_server(ok_response(body)).await;
        let provider = provider_fixture("prov-a", &base_url, Some(mc_with_queue("my_queue")));

        let result = collect_provider_metrics("net", &[provider]).await;
        assert!(
            result.contains_key("prov-a"),
            "malformed body must still produce a metrics entry"
        );
        let bm = result.get("prov-a").copied().unwrap_or_else(|| std::process::abort());
        assert!(
            bm.queue_depth.is_finite(),
            "malformed body must produce finite queue_depth"
        );
        assert!(
            bm.kv_cache_utilization.is_finite(),
            "malformed body must produce finite kv_cache_utilization"
        );
        assert!(
            bm.latency_p99_ms.is_finite(),
            "malformed body must produce finite latency_p99_ms"
        );
    }

    #[tokio::test]
    async fn collect_metrics_multiple_providers_all_present() {
        let body_a = "my_queue 0.1\n";
        let body_b = "my_queue 0.9\n";
        let url_a = start_test_server(ok_response(body_a)).await;
        let url_b = start_test_server(ok_response(body_b)).await;

        let prov_a = provider_fixture("prov-a", &url_a, Some(mc_with_queue("my_queue")));
        let prov_b = provider_fixture("prov-b", &url_b, Some(mc_with_queue("my_queue")));

        let result = collect_provider_metrics("net", &[prov_a, prov_b]).await;
        assert!(result.contains_key("prov-a"), "prov-a must be in metrics map");
        assert!(result.contains_key("prov-b"), "prov-b must be in metrics map");
        assert!(
            result
                .get("prov-a")
                .copied()
                .unwrap_or_else(|| std::process::abort())
                .queue_depth
                < result
                    .get("prov-b")
                    .copied()
                    .unwrap_or_else(|| std::process::abort())
                    .queue_depth,
            "prov-a (queue=0.1) must have lower queue_depth than prov-b (queue=0.9)"
        );
    }

    #[tokio::test]
    async fn collect_metrics_skips_providers_from_other_networks() {
        let body = "my_queue 0.2\n";
        let base_url = start_test_server(ok_response(body)).await;
        let provider = provider_fixture("prov-a", &base_url, Some(mc_with_queue("my_queue")));

        let result = collect_provider_metrics("other-net", &[provider]).await;
        assert!(
            result.is_empty(),
            "provider from a different GridNetwork must not be scraped"
        );
    }
}
