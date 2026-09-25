//! Live load signals kept in a bounded per-series window.
//!
//! Absorbs the operator's exposition and keeps a bounded per-series window, so
//! routing scores candidates on current load. Samples key on the operator's
//! observation time, so a republished cache value never reads as new.
//!
//! The exposition tokenizer is shared with the producer and lives in the
//! `common` crate. This module owns the consumer side: extracting the grid
//! target labels fail-closed and holding the windowed store.

use std::{
    borrow::Cow,
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use common::exposition::{self, PROVIDER_LABEL, SITE_LABEL};
use dashmap::DashMap;

/// Cap on providers retained, so a misconfigured or hostile endpoint cannot grow
/// the store without bound. Keys are retained once seen (not LRU-evicted), so the
/// bound is on distinct site/cluster keys, not on churn; the operator is the trust
/// source that stamps them.
const MAX_PROVIDERS: usize = 4_096;

/// Cap on distinct metric names per provider, bounding a peer that floods unique
/// names past the provider cap.
const MAX_METRICS_PER_PROVIDER: usize = 64;

/// Cap on samples retained per series, bounding both memory and the request-path
/// window scan when one ingest carries far more in-window points than a normal
/// scrape cadence produces. Retention past the cap drops the oldest first.
const MAX_SAMPLES_PER_SERIES: usize = 1_024;

/// Tolerance for a sample stamped ahead of the operator's own clock (its `Date`
/// header). Kept small: a legitimate sample is never meaningfully ahead of the
/// operator's own observation clock, and a large future window only lets a stamp
/// wedge the series head against the later corrected samples `Series::push`
/// drops.
const MAX_CLOCK_SKEW_MS: i64 = 5_000;

/// One observation of a series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sample {
    /// Operator observation time, in milliseconds since the epoch.
    pub at_ms: i64,
    /// Value as the provider reported it.
    pub value: f64,
}

/// A bounded window of one series, oldest first.
#[derive(Debug, Default)]
struct Series {
    /// Samples in timestamp order.
    samples: Vec<Sample>,
}

impl Series {
    /// Append `sample` if it is newer than what is held, then evict past
    /// `window`.
    fn push(&mut self, sample: Sample, window: Duration) {
        if self.samples.last().is_some_and(|last| sample.at_ms <= last.at_ms) {
            return;
        }
        self.samples.push(sample);
        let Ok(window_ms) = i64::try_from(window.as_millis()) else {
            return;
        };
        let cutoff = sample.at_ms.saturating_sub(window_ms);
        let keep_from = self.samples.partition_point(|held| held.at_ms < cutoff);
        // Drop from the front to satisfy both bounds in one pass: everything past
        // the window, and any excess over the count cap when a flood packs more
        // in-window points than a normal scrape cadence produces.
        let over_cap = self.samples.len().saturating_sub(MAX_SAMPLES_PER_SERIES);
        let drop_to = keep_from.max(over_cap);
        if drop_to > 0 {
            self.samples.drain(..drop_to);
        }
    }
}

/// Series held for one provider, keyed by metric name.
#[derive(Debug, Default)]
struct Provider {
    /// Metric name to its window.
    metrics: HashMap<Box<str>, Series>,
}

/// Windowed signals per provider, keyed by `"site/cluster"` so a request-path
/// lookup matches a route candidate. The key is built by concatenation
/// ([`Self::key`]), a small `Box<str>` per lookup.
#[derive(Debug)]
pub struct LoadStore {
    /// Provider key to its series.
    providers: DashMap<Box<str>, Provider>,
    /// Retention per series.
    window: Duration,
}

impl LoadStore {
    /// Create an empty store retaining `window` of history per series.
    #[must_use]
    pub fn new(window: Duration) -> Self {
        Self {
            providers: DashMap::new(),
            window,
        }
    }

    /// The key under which a candidate's series are held.
    #[must_use]
    pub fn key(site: &str, cluster: &str) -> Box<str> {
        format!("{site}/{cluster}").into_boxed_str()
    }

    /// Most recent sample of `metric` for `key`. Test-only since scoring reads
    /// [`Self::window_worst`].
    #[cfg(test)]
    pub fn latest(&self, key: &str, metric: &str) -> Option<Sample> {
        let provider = self.providers.get(key)?;
        provider.metrics.get(metric)?.samples.last().copied()
    }

    /// Most recent sample of `metric` for `key` younger than `max_age_ms`.
    /// Test-only since scoring reads [`Self::window_worst`].
    #[cfg(test)]
    pub fn fresh(&self, key: &str, metric: &str, now_ms: i64, max_age_ms: i64) -> Option<Sample> {
        // Range starts at zero: a future timestamp (publisher clock ahead) yields
        // a negative age that would otherwise read as fresh forever.
        self.latest(key, metric)
            .filter(|sample| (0..=max_age_ms).contains(&now_ms.saturating_sub(sample.at_ms)))
    }

    /// Worst reading of `metric` for `key` within the last `window_ms`, or `None`
    /// when the window holds no sample.
    ///
    /// Worst is the max when lower is better, so a drained burst stays penalised
    /// until it ages out rather than snapping to idle. Future-stamped samples are
    /// skipped.
    #[expect(
        clippy::too_many_arguments,
        reason = "keyed lookup with window bounds and score polarity"
    )]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the shard read guard is held across the bounded window scan by design"
    )]
    #[must_use]
    pub fn window_worst(
        &self,
        key: &str,
        metric: &str,
        now_ms: i64,
        window_ms: i64,
        lower_is_better: bool,
    ) -> Option<f64> {
        let provider = self.providers.get(key)?;
        let cutoff = now_ms.saturating_sub(window_ms);
        let mut worst: Option<f64> = None;
        for sample in &provider.metrics.get(metric)?.samples {
            if sample.at_ms < cutoff || sample.at_ms > now_ms {
                continue;
            }
            worst = Some(match worst {
                None => sample.value,
                Some(held) if lower_is_better => held.max(sample.value),
                Some(held) => held.min(sample.value),
            });
        }
        worst
    }

    /// Number of providers held.
    #[cfg(test)]
    pub fn provider_count(&self) -> usize {
        self.providers.len()
    }

    /// Absorb an exposition response using the local clock as the freshness
    /// reference. Test-only: the poll path uses [`Self::ingest_at`] with the
    /// operator's own clock, so no production path bypasses the `Date` anchor.
    #[cfg(test)]
    pub fn ingest(&self, text: &str) {
        self.ingest_at(text, now_ms());
    }

    /// Absorb an exposition response, skipping lines that do not parse so one bad
    /// line does not cost the rest. `reference_ms` is the operator's own clock
    /// (its `Date` header): a sample stamped implausibly far past it is dropped,
    /// so a future stamp cannot wedge the series head.
    pub fn ingest_at(&self, text: &str, reference_ms: i64) {
        let horizon = reference_ms.saturating_add(MAX_CLOCK_SKEW_MS);
        for line in text.lines() {
            let Some(observation) = parse_sample(line) else {
                continue;
            };
            if observation.sample.at_ms > horizon {
                continue;
            }
            let key = Self::key(observation.site.as_ref(), observation.cluster.as_ref());
            if !self.providers.contains_key(&key) && self.providers.len() >= MAX_PROVIDERS {
                continue;
            }
            let mut provider = self.providers.entry(key).or_default();
            // Probe by borrow so a known metric neither hashes twice nor allocates
            // an owned key; only the first sample of a new metric owns its name,
            // and only if it fits under the per-provider cap that bounds a peer
            // flooding unique names.
            if let Some(series) = provider.metrics.get_mut(observation.metric) {
                series.push(observation.sample, self.window);
            } else if provider.metrics.len() < MAX_METRICS_PER_PROVIDER {
                provider
                    .metrics
                    .entry(observation.metric.into())
                    .or_default()
                    .push(observation.sample, self.window);
            }
        }
    }
}

/// One exposition line resolved to its metric name, owning site and cluster, and
/// a sample.
struct Observation<'text> {
    /// Metric name.
    metric: &'text str,
    /// Owning site, from the `grid_site` label. Borrowed unless the value carried
    /// an escape.
    site: Cow<'text, str>,
    /// Owning provider, from the `grid_provider` label.
    cluster: Cow<'text, str>,
    /// The sample this line reported.
    sample: Sample,
}

/// Parse one exposition line into an [`Observation`], or `None` to skip it.
///
/// A line without a timestamp is skipped: without it a republished sample cannot
/// be told from a new one. A target value carrying a control char or `/` is
/// rejected so it cannot inject the store-key separator, and a duplicated
/// `grid_site` or `grid_provider` is anomalous for a well-formed operator, so
/// the line is rejected (fail closed).
fn parse_sample(line: &str) -> Option<Observation<'_>> {
    let metric = exposition::parse(line)?;
    let at_ms = metric.timestamp_ms()?;
    let mut site: Option<Cow<'_, str>> = None;
    let mut cluster: Option<Cow<'_, str>> = None;
    for (name, value) in metric.labels() {
        let slot = match name {
            SITE_LABEL => &mut site,
            PROVIDER_LABEL => &mut cluster,
            _ => continue,
        };
        // The value keys the store as `site/cluster` and is logged. Reject a
        // control char or `/` so it cannot inject a separator or corrupt a log.
        if value.chars().any(|ch| ch.is_control() || ch == '/') {
            return None;
        }
        // A repeated target label is anomalous for a well-formed operator.
        if slot.replace(value).is_some() {
            return None;
        }
    }
    let sample = Sample {
        at_ms,
        value: metric.value(),
    };
    Some(Observation {
        metric: metric.name(),
        site: site?,
        cluster: cluster?,
        sample,
    })
}

/// Milliseconds since the epoch.
#[must_use]
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| i64::try_from(since.as_millis()).unwrap_or(i64::MAX))
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::arithmetic_side_effects,
    clippy::expect_used,
    clippy::float_cmp,
    clippy::indexing_slicing,
    clippy::significant_drop_tightening,
    reason = "tests"
)]
mod tests {
    use super::*;

    const QUEUE: &str = "inference_pool_average_queue_size";

    fn line(site: &str, cluster: &str, value: f64, at_ms: i64) -> String {
        format!(r#"{QUEUE}{{grid_site="{site}",grid_provider="{cluster}"}} {value} {at_ms}"#)
    }

    fn store() -> LoadStore {
        LoadStore::new(Duration::from_secs(300))
    }

    #[test]
    fn ingests_a_labelled_sample() {
        let store = store();
        store.ingest(&line("east", "pool-a", 3.0, 1_000));
        let sample = store.latest(&LoadStore::key("east", "pool-a"), QUEUE).expect("sample");
        assert_eq!(
            sample,
            Sample {
                at_ms: 1_000,
                value: 3.0
            },
            "value and time as reported"
        );
    }

    #[test]
    fn a_republished_sample_does_not_advance_the_series() {
        let store = store();
        let repeated = line("east", "pool-a", 3.0, 1_000);
        store.ingest(&repeated);
        store.ingest(&repeated);
        store.ingest(&repeated);
        let key = LoadStore::key("east", "pool-a");
        let provider = store.providers.get(&key).expect("provider");
        let held = provider.metrics.get(QUEUE).expect("series").samples.len();
        assert_eq!(held, 1, "the operator's cached republish is not a new observation");
    }

    #[test]
    fn a_newer_sample_advances_the_series() {
        let store = store();
        store.ingest(&line("east", "pool-a", 3.0, 1_000));
        store.ingest(&line("east", "pool-a", 5.0, 2_000));
        let sample = store.latest(&LoadStore::key("east", "pool-a"), QUEUE).expect("sample");
        assert_eq!(sample.value, 5.0, "the newer value wins");
    }

    #[test]
    fn samples_older_than_the_window_are_evicted() {
        let store = LoadStore::new(Duration::from_secs(10));
        for at_ms in [1_000, 5_000, 20_000] {
            store.ingest(&line("east", "pool-a", 1.0, at_ms));
        }
        let key = LoadStore::key("east", "pool-a");
        let provider = store.providers.get(&key).expect("provider");
        let samples = &provider.metrics.get(QUEUE).expect("series").samples;
        assert_eq!(samples.len(), 1, "only what falls inside the window: {samples:?}");
        assert_eq!(
            samples.first().map(|sample| sample.at_ms),
            Some(20_000),
            "the newest survives"
        );
    }

    #[test]
    fn sites_do_not_collide_on_a_shared_cluster_name() {
        let store = store();
        store.ingest(&line("east", "pool-a", 1.0, 1_000));
        store.ingest(&line("west", "pool-a", 9.0, 1_000));
        assert_eq!(store.provider_count(), 2, "the site is part of the key");
        let west = store.latest(&LoadStore::key("west", "pool-a"), QUEUE).expect("west");
        assert_eq!(west.value, 9.0, "each site keeps its own value");
    }

    #[test]
    fn a_line_without_a_timestamp_is_skipped() {
        let store = store();
        store.ingest(&format!(r#"{QUEUE}{{grid_site="east",grid_provider="pool-a"}} 3"#));
        assert_eq!(
            store.provider_count(),
            0,
            "without a timestamp there is no way to order the sample"
        );
    }

    #[test]
    fn unlabelled_and_malformed_lines_are_skipped_without_losing_the_rest() {
        let store = store();
        let text = format!(
            "# HELP something\n{QUEUE} 3 1000\nnot a metric\n{}",
            line("east", "pool-a", 3.0, 1_000)
        );
        store.ingest(&text);
        assert_eq!(store.provider_count(), 1, "the one usable line still lands");
    }

    #[test]
    fn a_provider_cannot_exceed_the_metric_name_cap() {
        let store = store();
        for idx in 0..(MAX_METRICS_PER_PROVIDER + 10) {
            store.ingest(&format!(
                r#"metric_{idx}{{grid_site="east",grid_provider="pool-a"}} 1 1000"#
            ));
        }
        let key = LoadStore::key("east", "pool-a");
        let provider = store.providers.get(&key).expect("provider");
        assert_eq!(
            provider.metrics.len(),
            MAX_METRICS_PER_PROVIDER,
            "a flood of unique metric names is bounded per provider"
        );
    }

    #[test]
    fn a_quoted_comma_does_not_forge_a_target_label() {
        // The injected `,grid_provider=evil` trails the real label: a naive
        // last-write-wins parser would forge pool-a to evil, the quote-aware
        // parser keeps it inside the one value.
        let store = store();
        store.ingest(&format!(
            r#"{QUEUE}{{grid_site="east",grid_provider="pool-a",note="x,grid_provider=evil"}} 3 1000"#
        ));
        assert_eq!(store.provider_count(), 1, "one sample, keyed on the real target labels");
        assert!(
            store.latest(&LoadStore::key("east", "pool-a"), QUEUE).is_some(),
            "the sample keys to the genuine provider"
        );
        assert!(
            store.latest(&LoadStore::key("east", "evil"), QUEUE).is_none(),
            "the forged provider inside a quoted value never keys"
        );
    }

    #[test]
    fn an_escaped_quote_does_not_forge_a_target_label() {
        // An escaped quote must not end the value early and expose a forged label.
        let store = store();
        store.ingest(&format!(
            r#"{QUEUE}{{grid_site="east",grid_provider="pool-a",note="a\",grid_site=evil"}} 3 1000"#
        ));
        assert_eq!(
            store.provider_count(),
            1,
            "the escaped quote stays inside the one value"
        );
        assert!(
            store.latest(&LoadStore::key("east", "pool-a"), QUEUE).is_some(),
            "the genuine site survives the escaped-quote injection"
        );
        assert!(
            store.latest(&LoadStore::key("evil", "pool-a"), QUEUE).is_none(),
            "the forged site never keys"
        );
    }

    #[test]
    fn a_slash_or_control_char_in_a_target_value_is_rejected() {
        // A '/' would collide distinct site/cluster pairs in the store key.
        let with_slash = store();
        with_slash.ingest(&format!(r#"{QUEUE}{{grid_site="a/b",grid_provider="pool-a"}} 3 1000"#));
        assert_eq!(with_slash.provider_count(), 0, "a '/' in a target value is rejected");
        // An unescaped-to-newline control char must not reach a key or a log.
        let with_ctrl = store();
        with_ctrl.ingest(&format!(
            r#"{QUEUE}{{grid_site="ea\nst",grid_provider="pool-a"}} 3 1000"#
        ));
        assert_eq!(
            with_ctrl.provider_count(),
            0,
            "a control char in a target value is rejected"
        );
    }

    #[test]
    fn a_non_finite_value_is_rejected() {
        let store = store();
        store.ingest(&format!(
            r#"{QUEUE}{{grid_site="east",grid_provider="pool-a"}} NaN 1000"#
        ));
        store.ingest(&format!(
            r#"{QUEUE}{{grid_site="east",grid_provider="pool-a"}} +Inf 2000"#
        ));
        assert_eq!(store.provider_count(), 0, "NaN and Inf sample values are rejected");
    }

    #[test]
    fn a_duplicate_target_label_is_rejected() {
        let store = store();
        store.ingest(&format!(
            r#"{QUEUE}{{grid_site="east",grid_site="west",grid_provider="pool-a"}} 3 1000"#
        ));
        assert_eq!(
            store.provider_count(),
            0,
            "a duplicated grid_site is anomalous, so the line is dropped"
        );
    }

    #[test]
    fn an_escaped_label_value_is_parsed() {
        // An escaped non-target value must not break parsing of the line.
        let store = store();
        store.ingest(&format!(
            r#"{QUEUE}{{extra="line\none",grid_site="east",grid_provider="pool-a"}} 3 1000"#
        ));
        assert_eq!(
            store.provider_count(),
            1,
            "the line still lands with an escaped value present"
        );
    }

    #[test]
    fn a_future_sample_is_dropped_and_does_not_wedge_the_series() {
        // Reference is the operator's clock at 1_000. A stamp beyond the skew
        // tolerance is implausible and must not enter the series head, or the
        // later corrected sample would be dropped as older.
        let store = store();
        let future = 1_000 + MAX_CLOCK_SKEW_MS + 10_000;
        store.ingest_at(&line("east", "pool-a", 9.0, future), 1_000);
        store.ingest_at(&line("east", "pool-a", 3.0, 2_000), 1_000);
        let sample = store
            .latest(&LoadStore::key("east", "pool-a"), QUEUE)
            .expect("the corrected sample lands");
        assert_eq!(sample.at_ms, 2_000, "a future stamp cannot wedge the series head");
    }

    #[test]
    fn a_stale_sample_is_withheld_from_routing() {
        let store = store();
        store.ingest(&line("east", "pool-a", 3.0, 1_000));
        let key = LoadStore::key("east", "pool-a");
        assert!(store.fresh(&key, QUEUE, 10_000, 30_000).is_some(), "inside the bound");
        assert!(store.fresh(&key, QUEUE, 60_000, 30_000).is_none(), "past the bound");
    }

    #[test]
    fn windowed_worst_persists_a_drained_burst() {
        let store = store();
        let key = LoadStore::key("east", "pool-a");
        store.ingest(&line("east", "pool-a", 30.0, 1_000)); // burst
        store.ingest(&line("east", "pool-a", 1.0, 5_000)); // drained to idle
        // lower_is_better keeps the worst (max) in the window, so the burst
        // persists rather than snapping to idle.
        assert_eq!(
            store.window_worst(&key, QUEUE, 5_000, 30_000, true),
            Some(30.0),
            "a drained burst must persist as the worst reading in the window"
        );
        // The last-value view (what scoring used before) would have snapped to
        // idle.
        assert_eq!(
            store.latest(&key, QUEUE).map(|sample| sample.value),
            Some(1.0),
            "the last-value view snaps to idle"
        );
    }

    #[test]
    fn a_sample_from_a_clock_ahead_of_ours_is_withheld() {
        let store = store();
        store.ingest(&line("east", "pool-a", 3.0, 60_000));
        let key = LoadStore::key("east", "pool-a");
        assert!(
            store.fresh(&key, QUEUE, 10_000, 30_000).is_none(),
            "a future timestamp must not read as fresh, or a dead site keeps winning"
        );
    }

    #[test]
    fn distinct_providers_are_bounded_by_the_provider_cap() {
        let store = store();
        for idx in 0..(MAX_PROVIDERS + 10) {
            store.ingest(&line("east", &format!("pool-{idx}"), 1.0, 1_000));
        }
        assert_eq!(
            store.provider_count(),
            MAX_PROVIDERS,
            "a flood of distinct site/cluster keys is bounded"
        );
    }

    #[test]
    fn a_series_is_bounded_by_the_sample_cap() {
        // A flood of strictly-increasing in-window stamps would otherwise grow the
        // series unbounded; the count cap drops the oldest and keeps the newest.
        let store = store();
        let cap = i64::try_from(MAX_SAMPLES_PER_SERIES).expect("cap fits i64");
        let mut text = String::new();
        for at_ms in 1..=(cap + 500) {
            text.push_str(&line("east", "pool-a", 1.0, at_ms));
            text.push('\n');
        }
        store.ingest(&text);
        let key = LoadStore::key("east", "pool-a");
        let provider = store.providers.get(&key).expect("provider");
        let samples = &provider.metrics.get(QUEUE).expect("series").samples;
        assert_eq!(
            samples.len(),
            MAX_SAMPLES_PER_SERIES,
            "a flood of in-window samples is bounded per series"
        );
        assert_eq!(
            samples.last().map(|sample| sample.at_ms),
            Some(cap + 500),
            "the newest sample survives the cap"
        );
    }

    #[test]
    fn windowed_worst_keeps_the_min_when_higher_is_better() {
        // For a free-capacity metric higher is better, so the worst reading in the
        // window is the min; a brief recovery must not mask an earlier dip.
        let store = store();
        let key = LoadStore::key("east", "pool-a");
        store.ingest(&line("east", "pool-a", 2.0, 1_000)); // dip
        store.ingest(&line("east", "pool-a", 40.0, 5_000)); // recovered
        assert_eq!(
            store.window_worst(&key, QUEUE, 5_000, 30_000, false),
            Some(2.0),
            "higher-is-better keeps the min as the worst reading in the window"
        );
    }
}
