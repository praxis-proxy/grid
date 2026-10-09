//! One site's collection: every enabled query concurrently under one
//! deadline, folded into scalars, model counts, and tenant shares.

use std::{collections::BTreeMap, time::Duration};

use futures::future::join_all;
use time::OffsetDateTime;

use super::Metrics;
use crate::{
    metrics::{MetricsError, Sample, Source, Vector},
    model::{ModelCount, TenantShare},
    queries::QuerySet,
    registry::Site,
};

/// Keys whose first sample becomes one scalar field, in report order.
const SCALAR_KEYS: [&str; 8] = [
    "readyEndpoints",
    "gpuTotal",
    "gpuUtil",
    "rps",
    "p50LatencyMs",
    "tokensPerSec",
    "queueDepth",
    "replicasDown",
];

/// Keys whose absence degrades health.
const REQUIRED_KEYS: [&str; 3] = ["readyEndpoints", "gpuTotal", "gpuUtil"];

/// What one poll learned about a site.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SiteMetrics {
    /// Whether at least one query answered.
    pub reachable: bool,
    /// The first error, or a partial-failure summary; empty when all is well.
    pub err: String,
    /// Ready inference endpoints.
    pub ready_endpoints: Option<f64>,
    /// GPUs at the site.
    pub gpu_total: Option<f64>,
    /// GPU utilization in percent.
    pub gpu_util: Option<f64>,
    /// Requests per second.
    pub rps: Option<f64>,
    /// Median request latency in milliseconds.
    pub p50_latency_ms: Option<f64>,
    /// Generated tokens per second.
    pub tokens_per_sec: Option<f64>,
    /// Requests waiting.
    pub queue_depth: Option<f64>,
    /// Serving replicas that are not available.
    pub replicas_down: Option<f64>,
    /// Models running, sorted by name.
    pub models: Vec<ModelCount>,
    /// Tenant shares, sorted descending.
    pub tenants: Vec<TenantShare>,
    /// Required keys that returned no data.
    pub missing: Vec<String>,
}

/// Everything constant across one poll, borrowed by each site collection.
#[derive(Clone, Copy)]
pub struct SitePoller<'poll> {
    /// Where to query.
    pub source: &'poll Source,
    /// What to query.
    pub queries: &'poll QuerySet,
    /// Deadline for each query.
    pub timeout: Duration,
    /// Where to count failures.
    pub metrics: &'poll Metrics,
}

/// One query's outcome.
type Outcome = Result<Vector, MetricsError>;

/// What a failed or absent query contributes.
const NO_SAMPLES: &[Sample] = &[];

impl SitePoller<'_> {
    /// Runs every enabled query for `site` concurrently. Unreachable means
    /// every query failed; a partial failure is reported per key so one
    /// missing exporter does not hide the rest of the site.
    pub async fn collect(&self, site: &Site, at: OffsetDateTime) -> SiteMetrics {
        let keys: Vec<&str> = SCALAR_KEYS
            .iter()
            .chain(&["models", "tenants"])
            .copied()
            .filter(|key| self.queries.enabled(key))
            .collect();
        let mut results = self.run(site, at, &keys).await;
        let failures = self.count_failures(site, &keys, &results);
        let mut metrics = SiteMetrics::default();
        if let Some(first) = failures.first() {
            if failures.len() == keys.len() {
                metrics.err = first.to_string();
                return metrics;
            }
            metrics.err = format!("{} of {} queries failed: {first}", failures.len(), keys.len());
            tracing::debug!(site = %site.name, failures = failures.len(), "partial query failure");
        }
        metrics.reachable = true;
        self.fill_gpu_fallback(site, at, &mut results).await;
        fill_scalars(&mut metrics, &results);
        metrics.models = models_from(vector_of(&results, "models"));
        metrics.tenants = tenants_from(vector_of(&results, "tenants"));
        metrics
    }

    /// Runs `keys` concurrently; results are keyed by query key.
    async fn run(&self, site: &Site, at: OffsetDateTime, keys: &[&str]) -> BTreeMap<String, Outcome> {
        let outcomes = join_all(keys.iter().map(|key| self.one(site, key, at))).await;
        keys.iter().map(|key| (*key).to_owned()).zip(outcomes).collect()
    }

    /// Runs one query under the per-site deadline.
    async fn one(&self, site: &Site, key: &str, at: OffsetDateTime) -> Outcome {
        let query = self.queries.query(key);
        match tokio::time::timeout(self.timeout, self.source.query(site, &query, at)).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(MetricsError::Timeout(self.timeout)),
        }
    }

    /// The errors in `keys` order, each counted in the failure metric.
    fn count_failures<'results>(
        &self,
        site: &Site,
        keys: &[&str],
        results: &'results BTreeMap<String, Outcome>,
    ) -> Vec<&'results MetricsError> {
        let mut failures = Vec::new();
        for key in keys {
            if let Some(Err(err)) = results.get(*key) {
                self.metrics
                    .query_failures
                    .with_label_values(&[site.name.as_str(), key])
                    .inc();
                failures.push(err);
            }
        }
        failures
    }

    /// Replaces an empty `gpuUtil` with the vLLM KV-cache gauge when DCGM is
    /// absent and the fallback query is enabled.
    async fn fill_gpu_fallback(&self, site: &Site, at: OffsetDateTime, results: &mut BTreeMap<String, Outcome>) {
        if !vector_of(results, "gpuUtil").is_empty() || !self.queries.enabled("gpuUtilFallback") {
            return;
        }
        if let Ok(vector) = self.one(site, "gpuUtilFallback", at).await
            && !vector.is_empty()
        {
            results.insert("gpuUtil".to_owned(), Ok(vector));
        }
    }
}

/// The samples for `key`; empty when it failed or was not run.
fn vector_of<'results>(results: &'results BTreeMap<String, Outcome>, key: &str) -> &'results [Sample] {
    results
        .get(key)
        .and_then(|outcome| outcome.as_ref().ok())
        .map_or(NO_SAMPLES, Vec::as_slice)
}

/// Copies each scalar's first sample into its field and lists the required
/// ones that have none.
fn fill_scalars(metrics: &mut SiteMetrics, results: &BTreeMap<String, Outcome>) {
    for key in SCALAR_KEYS {
        let value = vector_of(results, key).first().map(|sample| sample.value);
        if let Some(slot) = scalar_slot(metrics, key) {
            *slot = value;
        }
    }
    metrics.missing = REQUIRED_KEYS
        .into_iter()
        .filter(|key| scalar_slot(metrics, key).is_some_and(|slot| slot.is_none()))
        .map(str::to_owned)
        .collect();
}

/// The field a scalar key fills.
fn scalar_slot<'site>(metrics: &'site mut SiteMetrics, key: &str) -> Option<&'site mut Option<f64>> {
    Some(match key {
        "readyEndpoints" => &mut metrics.ready_endpoints,
        "gpuTotal" => &mut metrics.gpu_total,
        "gpuUtil" => &mut metrics.gpu_util,
        "rps" => &mut metrics.rps,
        "p50LatencyMs" => &mut metrics.p50_latency_ms,
        "tokensPerSec" => &mut metrics.tokens_per_sec,
        "queueDepth" => &mut metrics.queue_depth,
        "replicasDown" => &mut metrics.replicas_down,
        _ => return None,
    })
}

/// Model counts named by `model_name`, sorted by name.
fn models_from(vector: &[Sample]) -> Vec<ModelCount> {
    let mut models: Vec<ModelCount> = vector
        .iter()
        .map(|sample| ModelCount {
            name: name_of(&sample.labels, "model_name"),
            running: sample.value,
        })
        .collect();
    models.sort_by(|left, right| left.name.cmp(&right.name));
    models
}

/// Tenant shares normalized to percentages of their sum, sorted descending.
fn tenants_from(vector: &[Sample]) -> Vec<TenantShare> {
    let total: f64 = vector.iter().map(|sample| sample.value).sum();
    let mut tenants: Vec<TenantShare> = vector
        .iter()
        .map(|sample| TenantShare {
            name: name_of(&sample.labels, "tenant"),
            share_pct: if total > 0.0 {
                (sample.value / total * 10000.0).round() / 100.0
            } else {
                0.0
            },
        })
        .collect();
    tenants.sort_by(|left, right| right.share_pct.total_cmp(&left.share_pct));
    tenants
}

/// The `preferred` label, else the first label that is not the metric name,
/// else `unknown`.
fn name_of(labels: &BTreeMap<String, String>, preferred: &str) -> String {
    labels
        .get(preferred)
        .filter(|name| !name.is_empty())
        .or_else(|| {
            labels
                .iter()
                .find(|(key, _)| *key != "__name__")
                .map(|(_, value)| value)
        })
        .cloned()
        .unwrap_or_else(|| "unknown".to_owned())
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use time::OffsetDateTime;

    use super::{SiteMetrics, SitePoller};
    use crate::{
        collector::Metrics,
        metrics::{
            PerSiteSource, Source,
            testing::{
                EMPTY, FakePrometheus, TOKEN, fake_for, healthy_answers, install_provider, one, refused_base, secrets,
            },
        },
        model::{ModelCount, TenantShare},
        queries::QuerySet,
        registry::Site,
    };

    const TIMEOUT: Duration = Duration::from_secs(5);
    #[tokio::test]
    async fn a_healthy_site_is_reachable_and_reports_every_scalar() {
        let queries = with_tenants();
        let base = fake_for(&queries, &healthy_answers()).serve().await;
        let metrics = collect(&queries, &base).await;
        assert!(
            metrics.reachable && metrics.err.is_empty() && metrics.missing.is_empty(),
            "{metrics:?}"
        );
        let want = [
            Some(2.0),
            Some(2.0),
            Some(41.5),
            Some(11.4),
            Some(842.0),
            Some(1930.0),
            Some(7.0),
            Some(0.0),
        ];
        assert_eq!(
            scalars(&metrics),
            want,
            "readyEndpoints, gpuTotal, gpuUtil, rps, p50, tokens, queue, replicasDown"
        );
    }

    #[tokio::test]
    async fn models_are_sorted_by_name_and_tenants_are_normalized_descending() {
        let queries = with_tenants();
        let base = fake_for(&queries, &healthy_answers()).serve().await;
        let metrics = collect(&queries, &base).await;
        let models = [("llama", 4.0), ("qwen", 9.0)].map(|(name, running)| ModelCount {
            name: name.to_owned(),
            running,
        });
        assert_eq!(metrics.models, models, "models are sorted by name");
        let tenants = [("acme", 55.0), ("globex", 45.0)].map(|(name, share_pct)| TenantShare {
            name: name.to_owned(),
            share_pct,
        });
        assert_eq!(
            metrics.tenants, tenants,
            "tenant shares are percentages of their sum, descending"
        );
    }

    #[tokio::test]
    async fn gpu_util_falls_back_to_the_kv_cache_gauge_when_dcgm_is_absent() {
        let queries = with_tenants();
        let mut answers = healthy_answers();
        answers.insert("gpuUtil", (200, EMPTY.to_owned()));
        answers.insert("gpuUtilFallback", (200, one(63.0)));
        let base = fake_for(&queries, &answers).serve().await;
        let metrics = collect(&queries, &base).await;
        assert_eq!(
            (metrics.gpu_util, metrics.missing.len()),
            (Some(63.0), 0),
            "{metrics:?}"
        );
    }

    #[tokio::test]
    async fn a_site_refusing_connections_is_unreachable() {
        let metrics = collect(&QuerySet::defaults().unwrap(), &refused_base()).await;
        assert!(
            !metrics.reachable && !metrics.err.is_empty() && metrics.gpu_util.is_none(),
            "{metrics:?}"
        );
    }

    #[tokio::test]
    async fn empty_required_results_are_reported_as_missing() {
        let queries = with_tenants();
        let answers = BTreeMap::from([("rps", (200, one(1.0)))]);
        let base = fake_for(&queries, &answers).serve().await;
        let metrics = collect(&queries, &base).await;
        assert!(metrics.reachable && metrics.err.is_empty(), "{metrics:?}");
        assert_eq!(
            metrics.missing,
            ["readyEndpoints", "gpuTotal", "gpuUtil"],
            "only required keys are reported"
        );
    }

    #[tokio::test]
    async fn a_disabled_query_is_never_sent() {
        let queries = QuerySet::defaults().unwrap();
        let base = fake_for(&queries, &healthy_answers()).serve().await;
        let metrics = collect(&queries, &base).await;
        assert!(
            metrics.err.is_empty(),
            "an unexpected tenants query would have failed: {metrics:?}"
        );
        assert!(
            metrics.tenants.is_empty(),
            "tenants are empty when the query is disabled"
        );
    }

    #[tokio::test]
    async fn one_failing_query_leaves_the_site_reachable_and_counts_the_failure() {
        let queries = QuerySet::defaults().unwrap();
        let mut answers = healthy_answers();
        answers.insert("rps", (500, "boom".to_owned()));
        let base = fake_for(&queries, &answers).serve().await;
        let registry = Metrics::new().unwrap();
        install_provider();
        let source = Source::PerSite(PerSiteSource::new(secrets(&[("site-spoke1", TOKEN, &[])]), TIMEOUT));
        let poller = SitePoller {
            source: &source,
            queries: &queries,
            timeout: TIMEOUT,
            metrics: &registry,
        };
        let metrics = poller.collect(&site(&base), now()).await;
        assert!(metrics.reachable, "{metrics:?}");
        assert!(
            metrics.err.contains("1 of"),
            "the partial failure is described: {}",
            metrics.err
        );
        let failures = registry.query_failures.with_label_values(&["spoke1", "rps"]).get();
        assert_eq!(failures, 1, "the failure counter is incremented per site and key");
    }

    #[tokio::test]
    async fn a_slow_site_is_bounded_by_the_timeout() {
        let queries = QuerySet::defaults().unwrap();
        let base = FakePrometheus::new().hang().serve().await;
        let registry = Metrics::new().unwrap();
        let short = Duration::from_millis(100);
        install_provider();
        let source = Source::PerSite(PerSiteSource::new(secrets(&[("site-spoke1", TOKEN, &[])]), short));
        let poller = SitePoller {
            source: &source,
            queries: &queries,
            timeout: short,
            metrics: &registry,
        };
        let metrics = tokio::time::timeout(Duration::from_secs(1), poller.collect(&site(&base), now()))
            .await
            .unwrap();
        assert!(!metrics.reachable, "{metrics:?}");
        assert!(
            metrics.err.contains("timed out"),
            "the error names the timeout: {}",
            metrics.err
        );
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    fn with_tenants() -> QuerySet {
        QuerySet::defaults().unwrap().with(&BTreeMap::from([(
            "tenants".to_owned(),
            "sum by (tenant) (x)".to_owned(),
        )]))
    }

    fn site(base: &str) -> Site {
        Site {
            name: "spoke1".to_owned(),
            metrics_url: base.to_owned(),
            metrics_secret: "site-spoke1".to_owned(),
            ..Site::default()
        }
    }

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_757_160_000).unwrap()
    }

    async fn collect(queries: &QuerySet, base: &str) -> SiteMetrics {
        let registry = Metrics::new().unwrap();
        install_provider();
        let source = Source::PerSite(PerSiteSource::new(secrets(&[("site-spoke1", TOKEN, &[])]), TIMEOUT));
        SitePoller {
            source: &source,
            queries,
            timeout: TIMEOUT,
            metrics: &registry,
        }
        .collect(&site(base), now())
        .await
    }

    fn scalars(metrics: &SiteMetrics) -> [Option<f64>; 8] {
        [
            metrics.ready_endpoints,
            metrics.gpu_total,
            metrics.gpu_util,
            metrics.rps,
            metrics.p50_latency_ms,
            metrics.tokens_per_sec,
            metrics.queue_depth,
            metrics.replicas_down,
        ]
    }
}
