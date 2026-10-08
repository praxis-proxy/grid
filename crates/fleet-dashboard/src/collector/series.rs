//! Fleet-wide time series: `query_range` fanned out to every site and folded
//! into one series per range.

use std::{collections::BTreeMap, sync::PoisonError, time::Duration};

use futures::future::join_all;
use time::OffsetDateTime;

use super::Collector;
use crate::{
    metrics::{Matrix, Window},
    model::{Point, Series},
    registry::Site,
};

/// How long a computed series is served before it is recomputed.
const CACHE_TTL_SECS: i64 = 30;

/// The keys folded into a fleet series.
const SERIES_KEYS: [&str; 3] = ["gpuUtil", "queueDepth", "tokensPerSec"];

/// Why a fleet series could not be produced.
#[derive(Debug, thiserror::Error)]
pub enum SeriesError {
    /// Not one of the supported windows.
    #[error("range must be one of 1h, 6h, 24h: got {0:?}")]
    BadRange(String),
}

/// A supported window and its resolution.
struct RangeSpec {
    /// How far back the window reaches.
    span_secs: i64,
    /// Time between points.
    step: Duration,
}

/// The window for a range name.
fn range_spec(range: &str) -> Option<RangeSpec> {
    match range {
        "1h" => Some(RangeSpec {
            span_secs: 3600,
            step: Duration::from_secs(30),
        }),
        "6h" => Some(RangeSpec {
            span_secs: 21_600,
            step: Duration::from_secs(120),
        }),
        "24h" => Some(RangeSpec {
            span_secs: 86_400,
            step: Duration::from_secs(600),
        }),
        _ => None,
    }
}

/// One site's range result for one key.
type SiteMatrix = (String, &'static str, Matrix);

/// One timestamp's accumulator across sites.
#[derive(Default)]
struct Cell {
    /// Weighted utilization sum and total weight, once any site reported.
    util: Option<(f64, f64)>,
    /// Summed queue depth, once any site reported.
    queue: Option<f64>,
    /// Summed tokens per second, once any site reported.
    tokens: Option<f64>,
}

impl Cell {
    /// Adds one site's sample for `key`.
    fn add(&mut self, key: &str, value: f64, weight: f64) {
        match key {
            "gpuUtil" => {
                let (sum, total) = self.util.unwrap_or_default();
                self.util = Some((sum + value * weight, total + weight));
            },
            "queueDepth" => self.queue = Some(self.queue.unwrap_or_default() + value),
            "tokensPerSec" => self.tokens = Some(self.tokens.unwrap_or_default() + value),
            _ => {},
        }
    }

    /// The folded point at `at`.
    fn point(&self, at: OffsetDateTime) -> Point {
        let gpu_util = self
            .util
            .filter(|(_, total)| *total > 0.0)
            .map(|(sum, total)| sum / total);
        Point {
            at,
            gpu_util,
            queue_depth: self.queue,
            tokens_per_sec: self.tokens,
        }
    }
}

/// Fans `query_range` out to every site and folds the results: GPU
/// utilization weighted by each site's GPU count, queue depth and tokens
/// summed. A series is served from cache for 30 seconds.
pub(super) async fn compute(collector: &Collector, range: &str) -> Result<Series, SeriesError> {
    let spec = range_spec(range).ok_or_else(|| SeriesError::BadRange(range.to_owned()))?;
    let now = (collector.clock)();
    let (cached, gpu_weights) = cached_series_and_weights(collector, range, now);
    if let Some(series) = cached {
        return Ok(series);
    }
    let end = truncate_to(now, spec.step);
    let window = Window {
        start: end.saturating_sub(time::Duration::seconds(spec.span_secs)),
        end,
        step: spec.step,
    };
    let sites = collector.sites.borrow().clone();
    let points = fold(fan_out(collector, &sites, window).await, &gpu_weights);
    let series = Series {
        range: Some(range.to_owned()),
        step: spec.step.as_secs(),
        points,
    };
    let entry = (now, series.clone());
    collector
        .state
        .write()
        .unwrap_or_else(PoisonError::into_inner)
        .series_cache
        .insert(range.to_owned(), entry);
    Ok(series)
}

/// The cached series for `range` while still fresh, and each site's GPU
/// count from the latest snapshot.
fn cached_series_and_weights(
    collector: &Collector,
    range: &str,
    now: OffsetDateTime,
) -> (Option<Series>, BTreeMap<String, f64>) {
    let state = collector.state.read().unwrap_or_else(PoisonError::into_inner);
    let fresh = |at: &OffsetDateTime| now.unix_timestamp().saturating_sub(at.unix_timestamp()) < CACHE_TTL_SECS;
    let cached = state
        .series_cache
        .get(range)
        .filter(|(at, _)| fresh(at))
        .map(|(_, series)| series.clone());
    let gpu_weights = state
        .snapshot
        .iter()
        .flat_map(|snapshot| snapshot.sites.iter())
        .map(|site| (site.name.clone(), site.gpus.total))
        .collect();
    drop(state);
    (cached, gpu_weights)
}

/// Every enabled series key for every site, each under the per-site deadline.
/// A failed or slow query is logged and left out.
async fn fan_out(collector: &Collector, sites: &[Site], window: Window) -> Vec<SiteMatrix> {
    let requests = sites.iter().flat_map(|site| {
        SERIES_KEYS
            .iter()
            .filter(|key| collector.queries.enabled(key))
            .map(move |key| range_query(collector, site, key, window))
    });
    join_all(requests).await.into_iter().flatten().collect()
}

/// One site's range query for `key`.
async fn range_query(collector: &Collector, site: &Site, key: &'static str, window: Window) -> Option<SiteMatrix> {
    let query = collector.queries.query(key);
    match tokio::time::timeout(
        collector.site_timeout,
        collector.source.query_range(site, &query, window),
    )
    .await
    {
        Ok(Ok(matrix)) => Some((site.name.clone(), key, matrix)),
        Ok(Err(err)) => {
            tracing::debug!(site = %site.name, key, error = %err, "query_range failed");
            None
        },
        Err(_elapsed) => {
            tracing::debug!(site = %site.name, key, "query_range timed out");
            None
        },
    }
}

/// Folds per-site matrices into one point per timestamp, in time order. A
/// site without a GPU count weighs 1 in the utilization mean.
fn fold(results: Vec<SiteMatrix>, gpu_weights: &BTreeMap<String, f64>) -> Vec<Point> {
    let mut cells: BTreeMap<i64, Cell> = BTreeMap::new();
    for (site, key, matrix) in results {
        let weight = gpu_weights
            .get(&site)
            .copied()
            .filter(|total| *total > 0.0)
            .unwrap_or(1.0);
        for sample in matrix.into_iter().flat_map(|series| series.points) {
            cells
                .entry(sample.at.unix_timestamp())
                .or_default()
                .add(key, sample.value, weight);
        }
    }
    cells
        .into_iter()
        .filter_map(|(timestamp, cell)| {
            OffsetDateTime::from_unix_timestamp(timestamp)
                .ok()
                .map(|at| cell.point(at))
        })
        .collect()
}

/// `now` rounded down to a multiple of `step`.
fn truncate_to(now: OffsetDateTime, step: Duration) -> OffsetDateTime {
    let step_secs = i64::try_from(step.as_secs()).unwrap_or(1).max(1);
    let timestamp = now.unix_timestamp();
    OffsetDateTime::from_unix_timestamp(timestamp.saturating_sub(timestamp.rem_euclid(step_secs))).unwrap_or(now)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
#[expect(clippy::indexing_slicing, reason = "test assertions on known series shapes")]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use super::SeriesError;
    use crate::{
        collector::{
            Collector,
            testing::{INTERVAL, TIMEOUT, fleet, spoke},
        },
        metrics::testing::{FakePrometheus, fake_for, healthy_answers, matrix, one},
        queries::QuerySet,
    };

    /// 2026-09-06T11:00:00Z.
    const T0: i64 = 1_788_692_400;

    #[tokio::test]
    async fn an_unknown_range_is_rejected() {
        let (collector, _hits) = ranged_fleet(TIMEOUT).await;
        let err = collector.fleet_series("2h").await.unwrap_err();
        assert!(matches!(err, SeriesError::BadRange(_)), "{err}");
    }

    #[tokio::test]
    async fn the_fold_weights_gpu_util_by_gpu_count_and_sums_the_rest() {
        let (collector, _hits) = ranged_fleet(TIMEOUT).await;
        collector.poll().await;
        let series = collector.fleet_series("1h").await.unwrap();
        assert_eq!(
            (series.range.as_deref(), series.step, series.points.len()),
            (Some("1h"), 30, 2),
            "{series:?}"
        );
        let point = &series.points[0];
        let folded = (point.gpu_util, point.queue_depth, point.tokens_per_sec);
        assert_eq!(
            folded,
            (Some(30.0), Some(4.0), Some(400.0)),
            "(10*2 + 40*4)/6 = 30; 1+3; 100+300"
        );
    }

    #[tokio::test]
    async fn results_are_served_from_cache_within_thirty_seconds_then_refreshed() {
        let (collector, hits) = ranged_fleet(TIMEOUT).await;
        collector.fleet_series("1h").await.unwrap();
        let after_first = hits.load(Ordering::Relaxed);
        collector.fleet_series("1h").await.unwrap();
        assert_eq!(
            hits.load(Ordering::Relaxed),
            after_first,
            "the second call 15s later is served from cache"
        );
        collector.fleet_series("1h").await.unwrap();
        assert!(
            hits.load(Ordering::Relaxed) > after_first,
            "the third call 30s later queries again"
        );
    }

    #[tokio::test]
    async fn a_cancelled_caller_leaves_nothing_cached() {
        let (collector, hits) = ranged_fleet(Duration::from_millis(300)).await;
        let aborted = tokio::time::timeout(Duration::from_millis(1), collector.fleet_series("1h")).await;
        assert!(aborted.is_err(), "the caller gave up before the fan-out finished");
        let before = hits.load(Ordering::Relaxed);
        collector.fleet_series("1h").await.unwrap();
        assert!(
            hits.load(Ordering::Relaxed) > before,
            "a live caller must query again, not read a cached partial result"
        );
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// Ohio with 2 GPUs and Oregon with 4, each answering range queries for
    /// gpuUtil, queueDepth, and tokensPerSec with two points; returns Ohio's
    /// request counter.
    async fn ranged_fleet(site_timeout: Duration) -> (Collector, Arc<AtomicUsize>) {
        let queries = QuerySet::defaults().unwrap();
        let ohio = with_ranges(
            fake_for(&queries, &healthy_answers()),
            &queries,
            [&[10.0, 20.0], &[1.0, 2.0], &[100.0, 200.0]],
        );
        let hits = ohio.hit_counter();
        let mut oregon_answers = healthy_answers();
        oregon_answers.insert("gpuTotal", (200, one(4.0)));
        let oregon = with_ranges(
            fake_for(&queries, &oregon_answers),
            &queries,
            [&[40.0, 50.0], &[3.0, 4.0], &[300.0, 400.0]],
        );
        let spoke1 = spoke("spoke1", "Ohio", "us-east-2", &ohio.serve().await);
        let spoke2 = spoke("spoke2", "Oregon", "us-west-2", &oregon.serve().await);
        let (collector, _sites) = fleet(vec![spoke1, spoke2], queries, INTERVAL, site_timeout);
        (collector, hits)
    }

    /// Adds two-point range answers for gpuUtil, queueDepth, and tokensPerSec.
    fn with_ranges(fake: FakePrometheus, queries: &QuerySet, values: [&[f64]; 3]) -> FakePrometheus {
        ["gpuUtil", "queueDepth", "tokensPerSec"]
            .into_iter()
            .zip(values)
            .fold(fake, |fake, (key, points)| {
                fake.answer(
                    "/api/v1/query_range",
                    &queries.query(key).promql,
                    200,
                    &matrix(T0, 30, points),
                )
            })
    }
}
