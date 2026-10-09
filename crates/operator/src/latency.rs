//! A provider's recent request latency, from deltas of its EPP's cumulative histograms.
//!
//! A value with too few completed requests in the window is left unpublished, so it reads as unknown.

use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant},
};

use crate::signals::Observation;

/// How far back the published latency looks.
pub(crate) const WINDOW: Duration = Duration::from_secs(30);

/// Completed requests a value needs in the window before it is published.
pub(crate) const MIN_SAMPLES: f64 = 20.0;

/// Time to first token at the median, streaming requests only, seconds.
pub const TTFT_P50_SIGNAL: &str = "grid_provider_ttft_p50_seconds";
/// Time to first token at the 90th percentile, streaming requests only, seconds.
pub const TTFT_P90_SIGNAL: &str = "grid_provider_ttft_p90_seconds";
/// Mean time per output token, streaming requests only, seconds.
pub const TPOT_SIGNAL: &str = "grid_provider_tpot_seconds";
/// Failed requests over all requests.
pub const ERROR_RATIO_SIGNAL: &str = "grid_provider_error_ratio";

/// EPP time to first token, labeled `streaming`.
const TTFT: &str = "llm_d_epp_request_ttft_seconds";
/// EPP time per output token, streaming requests.
const TPOT: &str = "llm_d_epp_request_streaming_tpot_seconds";
/// EPP input tokens per request.
const INPUT_TOKENS: &str = "llm_d_epp_request_input_tokens";
/// EPP requests processed.
const REQUESTS: &str = "llm_d_epp_request_total";
/// EPP requests that failed.
const ERRORS: &str = "llm_d_epp_request_error_total";

/// A histogram's `_sum` and `_count`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct SumCount {
    /// Summed observations.
    sum: f64,
    /// Observations.
    count: f64,
}

impl SumCount {
    /// `self - earlier`, `None` when a counter went backwards, as after an EPP restart.
    fn since(self, earlier: Self) -> Option<Self> {
        (self.sum >= earlier.sum && self.count >= earlier.count).then_some(Self {
            sum: self.sum - earlier.sum,
            count: self.count - earlier.count,
        })
    }

    /// The mean observation, `None` with none.
    fn mean(self) -> Option<f64> {
        (self.count > 0.0).then(|| self.sum / self.count)
    }
}

/// The EPP's counters summed over models, fairness and priority: cumulative at one scrape,
/// or the difference across a window.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Snapshot {
    /// Streaming TTFT bucket counts by upper bound, in bound order.
    ttft_buckets: Vec<(f64, f64)>,
    /// Streaming TTFT.
    ttft: SumCount,
    /// Streaming time per output token.
    tpot: SumCount,
    /// Input tokens per request.
    input: SumCount,
    /// Requests processed.
    requests: f64,
    /// Requests failed.
    errors: f64,
}

/// The sum of `metric` over the observations `keep` accepts, `None` when none match.
fn summed(observations: &[Observation], metric: &str, keep: impl Fn(&Observation) -> bool) -> Option<f64> {
    observations
        .iter()
        .filter(|o| o.metric == metric && keep(o))
        .map(|o| o.value)
        .reduce(|a, b| a + b)
}

/// The `_sum` and `_count` of histogram `name` over the observations `keep` accepts.
fn sum_count(observations: &[Observation], name: &str, keep: impl Fn(&Observation) -> bool + Copy) -> Option<SumCount> {
    Some(SumCount {
        sum: summed(observations, &format!("{name}_sum"), keep)?,
        count: summed(observations, &format!("{name}_count"), keep)?,
    })
}

/// Whether a TTFT series counts streaming requests.
fn streaming(o: &Observation) -> bool {
    o.labels.get("streaming").is_some_and(|value| value == "true")
}

/// Streaming TTFT bucket counts by upper bound, summed across series.
fn ttft_buckets(observations: &[Observation]) -> Vec<(f64, f64)> {
    let mut buckets: BTreeMap<u64, (f64, f64)> = BTreeMap::new();
    let bucket = format!("{TTFT}_bucket");
    for o in observations.iter().filter(|o| o.metric == bucket && streaming(o)) {
        if let Some(bound) = o.labels.get("le").and_then(|le| le.parse::<f64>().ok()) {
            // Bounds are positive, so their bit patterns sort as the values do.
            buckets.entry(bound.to_bits()).or_insert((bound, 0.0)).1 += o.value;
        }
    }
    buckets.into_values().collect()
}

impl Snapshot {
    /// The cumulative counters in one scrape.
    pub(crate) fn of(observations: &[Observation]) -> Self {
        let any = |_: &Observation| true;
        Self {
            ttft_buckets: ttft_buckets(observations),
            ttft: sum_count(observations, TTFT, streaming).unwrap_or_default(),
            tpot: sum_count(observations, TPOT, any).unwrap_or_default(),
            input: sum_count(observations, INPUT_TOKENS, any).unwrap_or_default(),
            requests: summed(observations, REQUESTS, any).unwrap_or(0.0),
            errors: summed(observations, ERRORS, any).unwrap_or(0.0),
        }
    }

    /// Whether the engine answered since `earlier`: a first token, a completion, or a usage
    /// report.
    ///
    /// Response-derived only. The request counter rises once scheduling succeeds, before any
    /// response, so a stream that ends on EOF or cancellation would count as an answer and
    /// hold a pool that accepts requests and never serves them out of its verdict.
    pub(crate) fn produced_since(&self, earlier: &Self) -> bool {
        self.ttft.count > earlier.ttft.count
            || self.tpot.count > earlier.tpot.count
            || self.input.count > earlier.input.count
    }

    /// `self - earlier`, `None` when any counter went backwards.
    fn since(&self, earlier: &Self) -> Option<Self> {
        (self.requests >= earlier.requests && self.errors >= earlier.errors).then_some(())?;
        Some(Self {
            ttft_buckets: self.bucket_deltas(earlier)?,
            ttft: self.ttft.since(earlier.ttft)?,
            tpot: self.tpot.since(earlier.tpot)?,
            input: self.input.since(earlier.input)?,
            requests: self.requests - earlier.requests,
            errors: self.errors - earlier.errors,
        })
    }

    /// Each TTFT bucket's count less `earlier`'s, `None` when one went backwards. Both bucket
    /// lists are in bound order, so this is one merge pass; a bound `earlier` lacks counts
    /// from zero.
    fn bucket_deltas(&self, earlier: &Self) -> Option<Vec<(f64, f64)>> {
        let mut then = earlier.ttft_buckets.iter().peekable();
        self.ttft_buckets
            .iter()
            .map(|(bound, count)| {
                while then.peek().is_some_and(|(b, _)| b.to_bits() < bound.to_bits()) {
                    then.next();
                }
                let before = then
                    .peek()
                    .filter(|(b, _)| b.to_bits() == bound.to_bits())
                    .map_or(0.0, |(_, earlier_count)| *earlier_count);
                (*count >= before).then_some((*bound, count - before))
            })
            .collect()
    }

    /// The series this window has enough samples to publish.
    fn observations(&self) -> Vec<Observation> {
        let enough = |count: f64| count >= MIN_SAMPLES;
        let mut published = Vec::new();
        if enough(self.ttft.count) {
            published.extend(quantile(&self.ttft_buckets, 0.5).map(|value| sample(TTFT_P50_SIGNAL, value)));
            published.extend(quantile(&self.ttft_buckets, 0.9).map(|value| sample(TTFT_P90_SIGNAL, value)));
        }
        if enough(self.tpot.count) {
            published.extend(self.tpot.mean().map(|value| sample(TPOT_SIGNAL, value)));
        }
        if enough(self.requests) {
            published.push(sample(
                ERROR_RATIO_SIGNAL,
                (self.errors / self.requests).clamp(0.0, 1.0),
            ));
        }
        published
    }
}

/// One provider's recent cumulative snapshots.
#[derive(Clone, Debug, Default)]
pub(crate) struct History {
    /// Snapshots by scrape time, oldest first, one at or before the window's start.
    snapshots: VecDeque<(Instant, Snapshot)>,
}

impl History {
    /// The latest recorded scrape and when it was taken.
    pub(crate) fn latest(&self) -> Option<(Instant, &Snapshot)> {
        self.snapshots.back().map(|(at, snapshot)| (*at, snapshot))
    }

    /// Record this scrape and return the latency series to publish for it.
    ///
    /// A counter that went backwards since the last scrape is a restart: history starts over
    /// from this scrape and it publishes nothing, so no window straddles the reset.
    pub(crate) fn record(&mut self, snapshot: Snapshot, now: Instant) -> Vec<Observation> {
        if self
            .snapshots
            .back()
            .is_some_and(|(_, last)| snapshot.since(last).is_none())
        {
            self.snapshots.clear();
            self.snapshots.push_back((now, snapshot));
            return Vec::new();
        }
        while self
            .snapshots
            .get(1)
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) >= WINDOW)
        {
            self.snapshots.pop_front();
        }
        let published = self
            .snapshots
            .front()
            .and_then(|(_, oldest)| snapshot.since(oldest))
            .map_or_else(Vec::new, |window| window.observations());
        self.snapshots.push_back((now, snapshot));
        published
    }
}

/// The `q` quantile of cumulative bucket counts by upper bound, interpolated within its bucket.
fn quantile(buckets: &[(f64, f64)], q: f64) -> Option<f64> {
    let total = buckets.last()?.1;
    if total <= 0.0 {
        return None;
    }
    let rank = q * total;
    let mut lower = (0.0, 0.0);
    for (bound, count) in buckets {
        if *count >= rank {
            if bound.is_infinite() {
                return Some(lower.0);
            }
            let within = count - lower.1;
            let fraction = if within > 0.0 { (rank - lower.1) / within } else { 1.0 };
            return Some(lower.0 + (bound - lower.0) * fraction);
        }
        lower = (*bound, *count);
    }
    Some(lower.0)
}

/// An unlabeled sample of `metric`.
fn sample(metric: &str, value: f64) -> Observation {
    Observation {
        metric: metric.to_owned(),
        labels: BTreeMap::new(),
        value,
        timestamp_ms: None,
    }
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::float_arithmetic,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::too_many_arguments,
    reason = "tests"
)]
mod tests {
    use super::*;

    impl History {
        /// Record `observations` as one scrape, as the readiness store does.
        fn record_observations(&mut self, observations: &[Observation], now: Instant) -> Vec<Observation> {
            self.record(Snapshot::of(observations), now)
        }
    }


    fn obs(metric: &str, labels: &[(&str, &str)], value: f64) -> Observation {
        Observation {
            metric: metric.to_owned(),
            labels: labels.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect(),
            value,
            timestamp_ms: None,
        }
    }

    /// An EPP exposition after `requests` streaming requests, each with TTFT `ttft`, TPOT
    /// `tpot`, `input` tokens, and `errors` failures.
    fn scrape(requests: f64, ttft: f64, tpot: f64, input: f64, errors: f64) -> Vec<Observation> {
        let streaming = [("streaming", "true"), ("model_name", "m")];
        let mut out = vec![];
        for bound in [0.1, 0.2, 0.4, 0.8, 1.6, f64::INFINITY] {
            let le = if bound.is_infinite() {
                "+Inf".to_owned()
            } else {
                bound.to_string()
            };
            let count = if ttft <= bound { requests } else { 0.0 };
            out.push(obs(
                &format!("{TTFT}_bucket"),
                &[streaming[0], streaming[1], ("le", &le)],
                count,
            ));
        }
        out.push(obs(&format!("{TTFT}_sum"), &streaming, ttft * requests));
        out.push(obs(&format!("{TTFT}_count"), &streaming, requests));
        // A non-streaming series must not count.
        out.push(obs(&format!("{TTFT}_sum"), &[("streaming", "false")], 999.0));
        out.push(obs(&format!("{TTFT}_count"), &[("streaming", "false")], 1.0));
        out.push(obs(&format!("{TPOT}_sum"), &[], tpot * requests));
        out.push(obs(&format!("{TPOT}_count"), &[], requests));
        out.push(obs(&format!("{INPUT_TOKENS}_sum"), &[], input * requests));
        out.push(obs(&format!("{INPUT_TOKENS}_count"), &[], requests));
        out.push(obs(REQUESTS, &[], requests));
        out.push(obs(ERRORS, &[("error_code", "500")], errors));
        out
    }

    fn value(published: &[Observation], metric: &str) -> Option<f64> {
        published.iter().find(|o| o.metric == metric).map(|o| o.value)
    }

    #[test]
    fn a_window_publishes_latency_from_the_requests_inside_it() {
        let mut history = History::default();
        let start = Instant::now();
        let first = history.record_observations(&scrape(100.0, 0.3, 0.02, 1000.0, 0.0), start);
        assert!(first.is_empty(), "a first scrape has no window");
        let later = scrape(200.0, 0.3, 0.02, 1000.0, 5.0);
        let published = history.record_observations(&later, start + Duration::from_secs(10));
        let p50 = value(&published, TTFT_P50_SIGNAL).unwrap();
        assert!(
            (0.2..=0.4).contains(&p50),
            "every request in the 0.2..0.4 bucket: {p50}"
        );
        assert!(value(&published, TTFT_P90_SIGNAL).unwrap() >= p50);
        assert!((value(&published, TPOT_SIGNAL).unwrap() - 0.02).abs() < 1e-9);
        assert!((value(&published, ERROR_RATIO_SIGNAL).unwrap() - 0.05).abs() < 1e-9);
    }

    #[test]
    fn too_few_requests_publish_nothing() {
        let mut history = History::default();
        let start = Instant::now();
        history.record_observations(&scrape(100.0, 0.3, 0.02, 1000.0, 0.0), start);
        let published =
            history.record_observations(&scrape(105.0, 0.3, 0.02, 1000.0, 0.0), start + Duration::from_secs(10));
        assert!(published.is_empty(), "5 requests is too few: {published:?}");
    }

    #[test]
    fn a_counter_reset_publishes_nothing_and_starts_over() {
        let mut history = History::default();
        let start = Instant::now();
        history.record_observations(&scrape(500.0, 0.3, 0.02, 1000.0, 0.0), start);
        let reset = history.record_observations(&scrape(50.0, 0.3, 0.02, 1000.0, 0.0), start + Duration::from_secs(5));
        assert!(reset.is_empty(), "an EPP restart is not negative latency");
        let after =
            history.record_observations(&scrape(100.0, 0.3, 0.02, 1000.0, 0.0), start + Duration::from_secs(10));
        assert!(value(&after, TPOT_SIGNAL).is_some(), "the window restarts at the reset");
    }

    #[test]
    fn a_reset_below_the_oldest_snapshot_still_publishes_nothing() {
        // 10, 100, then 50: the delta against the oldest snapshot is positive, but the counter
        // went backwards since the last one, so the scrape straddles a restart.
        let mut history = History::default();
        let start = Instant::now();
        history.record_observations(&scrape(10.0, 0.3, 0.02, 1000.0, 0.0), start);
        history.record_observations(&scrape(100.0, 0.3, 0.02, 1000.0, 0.0), start + Duration::from_secs(5));
        let reset = history.record_observations(&scrape(50.0, 0.3, 0.02, 1000.0, 0.0), start + Duration::from_secs(10));
        assert!(reset.is_empty(), "nothing crosses the restart: {reset:?}");
        let after =
            history.record_observations(&scrape(100.0, 0.3, 0.02, 1000.0, 0.0), start + Duration::from_secs(15));
        assert!(value(&after, TPOT_SIGNAL).is_some(), "the window restarts at the reset");
    }

    #[test]
    fn bucket_deltas_merge_in_bound_order_and_a_new_bound_counts_from_zero() {
        let earlier = Snapshot {
            ttft_buckets: vec![(0.1, 1.0), (0.4, 3.0), (f64::INFINITY, 3.0)],
            ..Snapshot::default()
        };
        let later = Snapshot {
            ttft_buckets: vec![(0.1, 2.0), (0.2, 2.0), (0.4, 5.0), (f64::INFINITY, 6.0)],
            ..Snapshot::default()
        };
        assert_eq!(
            later.bucket_deltas(&earlier),
            Some(vec![(0.1, 1.0), (0.2, 2.0), (0.4, 2.0), (f64::INFINITY, 3.0)])
        );
        let backwards = Snapshot {
            ttft_buckets: vec![(0.1, 0.0), (0.4, 3.0), (f64::INFINITY, 3.0)],
            ..Snapshot::default()
        };
        assert_eq!(backwards.bucket_deltas(&earlier), None);
    }

    #[test]
    fn the_window_drops_snapshots_older_than_it() {
        let mut history = History::default();
        let start = Instant::now();
        for (seconds, requests) in [(0, 0.0), (20, 100.0), (40, 100.0)] {
            history.record_observations(
                &scrape(requests, 0.3, 0.02, 1000.0, 0.0),
                start + Duration::from_secs(seconds),
            );
        }
        let idle = history.record_observations(&scrape(100.0, 0.3, 0.02, 1000.0, 0.0), start + Duration::from_secs(60));
        assert!(idle.is_empty(), "nothing completed in the last 30s: {idle:?}");
    }


    #[test]
    fn quantiles_interpolate_within_a_bucket() {
        let buckets = [(1.0, 0.0), (2.0, 10.0), (f64::INFINITY, 10.0)];
        assert!((quantile(&buckets, 0.5).unwrap() - 1.5).abs() < 1e-9);
        assert_eq!(quantile(&[(1.0, 0.0), (f64::INFINITY, 0.0)], 0.5), None);
    }
}
