//! The raw series an llm-d EPP exposes and the grid operator relays as they are. The gateway
//! concludes in-flight from them here; a second provider is another file beside this one.

use grid_signals::{Combine, LoadStore};

use super::{Over, SiteReading, SiteSignals};

/// Requests waiting per endpoint, averaged over the pool, current EPP name first. Orders
/// candidates: lower is a better target.
///
/// The wire carries each backend's raw metric name, so the consumer maps every name it knows.
pub(crate) const QUEUE_METRICS: [&str; 2] = ["llm_d_epp_average_queue_size", "inference_pool_average_queue_size"];

/// The queue metric current llm-d EPPs export.
#[cfg(test)]
pub(crate) const QUEUE_METRIC: &str = QUEUE_METRICS[0];

/// Requests running per endpoint, averaged over the pool, current EPP name first.
const RUNNING_METRICS: [&str; 2] = [
    "llm_d_epp_average_running_requests",
    "inference_pool_average_running_requests",
];

/// Endpoints the EPP counts ready, current EPP name first. In-flight scales by the count taken
/// with each running sample, and zero of them is a site that cannot serve.
const ENDPOINT_METRICS: [&str; 2] = ["llm_d_epp_ready_endpoints", "inference_pool_ready_pods"];

/// Requests the EPP's flow control holds before scheduling: in flight, and a backlog. One
/// series per priority and fairness partition, summed at each instant.
const FLOW_CONTROL_QUEUE_METRIC: &str = "llm_d_epp_flow_control_queue_size";

/// The engine queue per serving unit, one series per unit, the busiest kept at each
/// instant. Its worst unit gates teaching: a scale-out dilutes the average below
/// `queue_full` while the old units still hold the backlog. It also tells a live pool from
/// a drained one: the EPP's pool gauges freeze at their last values once the pool has no
/// units, while this series comes from a collector that simply stops reporting.
const PER_UNIT_QUEUE_METRICS: [&str; 2] = ["inference_pool_per_pod_queue_size", "llm_d_epp_per_endpoint_queue_size"];

/// How same-instant lines of one metric under different labels combine in the store: the
/// flow-control hold is a count split across partitions, everything else is per unit.
pub(crate) fn combine(metric: &str) -> Combine {
    if metric == FLOW_CONTROL_QUEUE_METRIC {
        Combine::Sum
    } else {
        Combine::Max
    }
}

/// Lower is better for every series read here, so the worst sample is the maximum.
const LOWER_IS_BETTER: bool = true;

/// The most of anything a site can plausibly hold. A larger sample is refused as absent, so
/// one enrolled peer cannot teach a ceiling that pulls the grid to itself for days.
const MAX_COUNT: f64 = 1_000_000.0;

/// `value` as a count, or `None` when it is not one a site can hold.
fn plausible(value: f64) -> Option<f64> {
    (value.is_finite() && value <= MAX_COUNT).then(|| value.max(0.0))
}

/// Which raw provider series in the store answer each field of a reading. `LLM_D` is what an
/// llm-d EPP publishes and the operator relays as it is; a provider that means the same things
/// under other names is another value of this type. The gateway concludes in-flight itself
/// from these, so no operator conclusion is read back as an input.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Series {
    /// Requests running per serving unit, averaged, first name found wins.
    pub(crate) running: &'static [&'static str],
    /// Requests waiting per serving unit, averaged, first name found wins.
    pub(crate) queued: &'static [&'static str],
    /// Serving units ready to take requests, first name found wins.
    pub(crate) endpoints: &'static [&'static str],
    /// Work held before scheduling, a count for the whole site.
    pub(crate) held: &'static str,
    /// Work waiting at each serving unit, first name found wins. Present only while the
    /// site has units.
    pub(crate) per_unit_queue: &'static [&'static str],
}

impl Series {
    /// The series an llm-d EPP publishes.
    pub(crate) const LLM_D: Self = Self {
        running: &RUNNING_METRICS,
        queued: &QUEUE_METRICS,
        endpoints: &ENDPOINT_METRICS,
        held: FLOW_CONTROL_QUEUE_METRIC,
        per_unit_queue: &PER_UNIT_QUEUE_METRICS,
    };
}

/// The store read under a series mapping.
///
/// Worst in the window rather than latest. A backend can publish a transient zero while
/// genuinely loaded, and zero is the best value every series here can hold, so the latest
/// sample alone makes the most loaded site the most attractive one for that window.
/// Readiness is the latest at any age: a recovered provider rejoins on its next poll, and a
/// partitioned peer keeps its last 0 rather than reading as ready once it ages out. A 0
/// stands until something newer arrives for the provider.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Mapped<'store> {
    /// Live samples per site.
    pub(crate) store: &'store LoadStore,
    /// Which series answer each field.
    pub(crate) series: &'static Series,
}

impl SiteSignals for Mapped<'_> {
    fn read(&self, site: &str, cluster: &str, over: Over) -> SiteReading {
        let key = LoadStore::key(site, cluster);
        let sampled_at = self.store.newest_at(&key);
        // A pool whose per-unit series stopped while its gauges kept stamping has no units:
        // the gauges are frozen, so the site is unready and nothing it says is measured.
        if self.drained(&key, over.window_ms) {
            return SiteReading {
                unready: true,
                sampled_at,
                ..SiteReading::default()
            };
        }
        self.live(&key, over, sampled_at)
    }
}

impl Mapped<'_> {
    /// The reading of a site with units, `over` its window.
    fn live(&self, key: &str, over: Over, sampled_at: Option<i64>) -> SiteReading {
        let Over {
            now_ms,
            window_ms,
            queue_full,
        } = over;
        let worst = |metric: &str| {
            self.store
                .window_worst(key, metric, now_ms, window_ms, LOWER_IS_BETTER)
                .and_then(plausible)
        };
        let first = |metrics: &[&str]| metrics.iter().find_map(|metric| worst(metric));
        let backlog = |queued: Option<f64>, held: Option<f64>| {
            queued.is_some_and(|queued| queued >= queue_full) || held.is_some_and(|held| held >= 1.0)
        };
        SiteReading {
            in_flight: self.in_flight(key, now_ms, window_ms, |_, _| true),
            congested_in_flight: self.in_flight(key, now_ms, window_ms, backlog),
            queued: first(self.series.queued),
            held: worst(self.series.held),
            deepest_queue: first(self.series.per_unit_queue),
            unready: self.unready(key, sampled_at),
            sampled_at,
        }
    }

    /// What the site holds: per-unit running plus waiting, over its ready units, plus what
    /// flow control holds in front of them. Concluded per instant from readings taken
    /// together, over the instants `at` admits by their waiting and held readings, then
    /// the worst of those in the window, so a unit count from one scrape never multiplies a
    /// running count from another.
    fn in_flight(
        &self,
        key: &str,
        now_ms: i64,
        window_ms: i64,
        at: impl Fn(Option<f64>, Option<f64>) -> bool,
    ) -> Option<f64> {
        self.store.window_worst_of(
            key,
            [
                self.published(key, self.series.running),
                self.published(key, self.series.queued),
                self.published(key, self.series.endpoints),
                self.series.held,
            ],
            now_ms,
            window_ms,
            LOWER_IS_BETTER,
            |[running, queued, endpoints, held]| {
                let queued = queued.and_then(plausible);
                let held = held.and_then(plausible);
                if !at(queued, held) {
                    return None;
                }
                let running = plausible(running?)?;
                let endpoints = plausible(endpoints?)?;
                plausible((running + queued.unwrap_or(0.0)) * endpoints + held.unwrap_or(0.0))
            },
        )
    }

    /// Whether the site's per-unit series last reported more than `lag_ms` before its
    /// ready-unit gauge did: the gauge is frozen at its last value because the pool has no
    /// units left. One exposition stamps every line alike, so the bound only tolerates a
    /// collector that lags by a scrape or two, not one that stopped.
    fn drained(&self, key: &str, lag_ms: i64) -> bool {
        let latest = |metrics: &[&str]| metrics.iter().find_map(|metric| self.store.latest(key, metric));
        match (latest(self.series.per_unit_queue), latest(self.series.endpoints)) {
            (Some(units), Some(ready)) => ready.at_ms.saturating_sub(units.at_ms) > lag_ms,
            _ => false,
        }
    }

    /// The first of `metrics` the site publishes at all, else the first name, so a site that
    /// publishes none joins nothing under it.
    fn published(&self, key: &str, metrics: &'static [&'static str]) -> &'static str {
        metrics
            .iter()
            .copied()
            .find(|metric| self.store.latest(key, metric).is_some())
            .or_else(|| metrics.first().copied())
            .unwrap_or("")
    }

    /// Whether the latest ready-unit count, at any age, is zero: it stands until something
    /// newer arrives for the site.
    fn unready(&self, key: &str, sampled_at: Option<i64>) -> bool {
        self.series
            .endpoints
            .iter()
            .find_map(|metric| self.store.latest(key, metric))
            .is_some_and(|ready| ready.value < 1.0 && sampled_at.is_none_or(|newest| newest <= ready.at_ms))
    }
}

/// The store read under the llm-d mapping.
impl SiteSignals for LoadStore {
    fn read(&self, site: &str, cluster: &str, over: Over) -> SiteReading {
        Mapped {
            store: self,
            series: &Series::LLM_D,
        }
        .read(site, cluster, over)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// A reading over `window_ms` at `now_ms` with `queue_full` as the backlog threshold.
    const fn over(now_ms: i64, window_ms: i64, queue_full: f64) -> Over {
        Over {
            now_ms,
            window_ms,
            queue_full,
        }
    }

    static OTHER: Series = Series {
        running: &["vendor_running"],
        queued: &["vendor_queue"],
        endpoints: &["vendor_units"],
        held: "vendor_held",
        per_unit_queue: &["vendor_unit_queue"],
    };

    /// A store holding one sample of each of `OTHER`'s series for site a.
    fn stocked() -> LoadStore {
        let store = LoadStore::new(Duration::from_secs(60));
        for (metric, value) in [
            ("vendor_running", 10.0),
            ("vendor_queue", 0.5),
            ("vendor_units", 2.0),
            ("vendor_held", 3.0),
            ("vendor_unit_queue", 2.0),
        ] {
            let line = format!(r#"{metric}{{grid_site="a",grid_provider="pool-a"}} {value} 1000"#);
            store.ingest_at(&line, 1_000, 1_000, "a");
        }
        store
    }

    #[test]
    fn a_mapping_reads_the_same_store_under_other_names() {
        let store = stocked();
        let mapped = Mapped {
            store: &store,
            series: &OTHER,
        };
        // (10 running + 0.5 waiting) per unit, 2 units, plus 3 held: 24 in flight, and held
        // work makes that instant a congested one.
        let expected = SiteReading {
            in_flight: Some(24.0),
            congested_in_flight: Some(24.0),
            queued: Some(0.5),
            held: Some(3.0),
            deepest_queue: Some(2.0),
            unready: false,
            sampled_at: Some(1_000),
        };
        assert_eq!(mapped.read("a", "pool-a", over(1_000, 30_000, 1.0)), expected);
        // The llm-d names see the sample stamp and nothing else.
        let provider = store.read("a", "pool-a", over(1_000, 30_000, 1.0));
        assert_eq!(
            provider,
            SiteReading {
                sampled_at: Some(1_000),
                ..SiteReading::default()
            }
        );
    }

    #[test]
    fn zero_ready_units_reads_as_unready_and_unmeasured() {
        let store = LoadStore::new(Duration::from_secs(60));
        for line in [
            r#"llm_d_epp_average_running_requests{grid_site="a",grid_provider="pool-a"} 10 1000"#,
            r#"llm_d_epp_ready_endpoints{grid_site="a",grid_provider="pool-a"} 0 1000"#,
        ] {
            store.ingest_at(line, 1_000, 1_000, "a");
        }
        let reading = store.read("a", "pool-a", over(1_000, 30_000, 1.0));
        assert!(reading.unready);
        assert_eq!(reading.in_flight, Some(0.0), "no unit, nothing in flight");
    }

    #[test]
    fn in_flight_is_concluded_from_readings_taken_together() {
        let store = LoadStore::new(Duration::from_secs(60));
        // Two units running 10 each, then one unit running 20, then a transient zero ready
        // count beside a running 4: 20 in flight, not the 40 of each series' own worst and
        // not the 0 of the window's fewest units.
        for (at, running, ready) in [(1_000, 10.0, 2.0), (2_000, 20.0, 1.0), (3_000, 4.0, 0.0)] {
            for line in [
                format!(r#"llm_d_epp_average_running_requests{{grid_site="a",grid_provider="pool-a"}} {running} {at}"#),
                format!(r#"llm_d_epp_ready_endpoints{{grid_site="a",grid_provider="pool-a"}} {ready} {at}"#),
            ] {
                store.ingest_at(&line, at, at, "a");
            }
        }
        let reading = store.read("a", "pool-a", over(3_000, 30_000, 1.0));
        assert_eq!(reading.in_flight, Some(20.0));
        assert!(reading.unready, "the latest ready count is zero");
    }

    #[test]
    fn congestion_is_read_at_instants_that_had_a_backlog() {
        let store = LoadStore::new(Duration::from_secs(60));
        // At the ceiling with nothing waiting, then well below it with a queue: the site was
        // never at its ceiling while congested, whatever each series' own worst says.
        for (at, running, queued) in [(1_000, 100.0, 0.0), (1_500, 20.0, 5.0)] {
            for line in [
                format!(r#"llm_d_epp_average_running_requests{{grid_site="a",grid_provider="pool-a"}} {running} {at}"#),
                format!(r#"llm_d_epp_average_queue_size{{grid_site="a",grid_provider="pool-a"}} {queued} {at}"#),
                format!(r#"llm_d_epp_ready_endpoints{{grid_site="a",grid_provider="pool-a"}} 1 {at}"#),
            ] {
                store.ingest_at(&line, at, at, "a");
            }
        }
        let reading = store.read("a", "pool-a", over(1_500, 30_000, 1.0));
        assert_eq!(reading.in_flight, Some(100.0));
        assert_eq!(reading.queued, Some(5.0));
        assert_eq!(
            reading.congested_in_flight,
            Some(25.0),
            "20 running plus 5 waiting, at the congested instant"
        );
    }

    #[test]
    fn a_pool_whose_per_unit_series_stopped_reads_as_drained() {
        let store = LoadStore::new(Duration::from_secs(60));
        let labels = r#"grid_site="a",grid_provider="pool-a""#;
        store.ingest_at(
            &format!(
                "llm_d_epp_ready_endpoints{{{labels}}} 2 1000\nllm_d_epp_average_running_requests{{{labels}}} 10 1000\ninference_pool_per_pod_queue_size{{{labels},pod=\"p0\"}} 0 1000"
            ),
            1_000,
            1_000,
            "a",
        );
        assert!(
            !store.read("a", "pool-a", over(1_000, 30_000, 1.0)).unready,
            "units reporting"
        );
        // The pool scales to zero: the gauges freeze at 2 and 10 and keep stamping, the
        // per-unit collector reports nothing more. Within the window's lag it is a collector
        // running late, past it the pool is drained.
        store.ingest_at(
            &format!(
                "llm_d_epp_ready_endpoints{{{labels}}} 2 2000\nllm_d_epp_average_running_requests{{{labels}}} 10 2000"
            ),
            2_000,
            2_000,
            "a",
        );
        assert!(
            !store.read("a", "pool-a", over(2_000, 30_000, 1.0)).unready,
            "one scrape late is not drained"
        );
        let reading = store.read("a", "pool-a", over(2_000, 500, 1.0));
        assert!(reading.unready, "a frozen positive ready count is not a ready pool");
        assert_eq!(reading.in_flight, None, "frozen gauges measure nothing");
    }

    #[test]
    fn held_work_is_the_total_across_partitions() {
        let store = LoadStore::with_combine(Duration::from_secs(60), combine);
        let labels = r#"grid_site="a",grid_provider="pool-a""#;
        store.ingest_at(
            &format!(
                "llm_d_epp_average_running_requests{{{labels}}} 10 1000\nllm_d_epp_ready_endpoints{{{labels}}} 1 1000\nllm_d_epp_flow_control_queue_size{{{labels},priority=\"0\"}} 0 1000\nllm_d_epp_flow_control_queue_size{{{labels},priority=\"1\"}} 4 1000\nllm_d_epp_flow_control_queue_size{{{labels},priority=\"2\"}} 3 1000"
            ),
            1_000,
            1_000,
            "a",
        );
        let reading = store.read("a", "pool-a", over(1_000, 30_000, 1.0));
        assert_eq!(reading.held, Some(7.0), "a zero partition first hides nothing");
        assert_eq!(reading.in_flight, Some(17.0));
    }

    #[test]
    fn an_implausible_count_reads_as_absent() {
        let store = LoadStore::new(Duration::from_secs(60));
        for line in [
            r#"llm_d_epp_average_running_requests{grid_site="a",grid_provider="pool-a"} 1e300 1000"#,
            r#"llm_d_epp_ready_endpoints{grid_site="a",grid_provider="pool-a"} 1 1000"#,
        ] {
            store.ingest_at(line, 1_000, 1_000, "a");
        }
        assert_eq!(store.read("a", "pool-a", over(1_000, 30_000, 1.0)).in_flight, None);
    }
}
