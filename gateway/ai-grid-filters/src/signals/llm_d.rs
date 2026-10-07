//! The raw series an llm-d EPP exposes and the grid operator relays as they are. The gateway
//! concludes in-flight from them here; a second provider is another file beside this one.

use grid_signals::LoadStore;

use super::{SiteReading, SiteSignals};

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

/// Requests the EPP's flow control holds before scheduling: in flight, and a backlog.
const FLOW_CONTROL_QUEUE_METRIC: &str = "llm_d_epp_flow_control_queue_size";

/// The engine queue per pod. Its worst pod gates teaching: a scale-out dilutes the average
/// below `queue_full` while the old pods still hold the backlog.
const PER_POD_QUEUE_METRIC: &str = "inference_pool_per_pod_queue_size";

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
    /// Work waiting at each serving unit.
    pub(crate) deepest_queue: &'static str,
}

impl Series {
    /// The series an llm-d EPP publishes.
    pub(crate) const LLM_D: Self = Self {
        running: &RUNNING_METRICS,
        queued: &QUEUE_METRICS,
        endpoints: &ENDPOINT_METRICS,
        held: FLOW_CONTROL_QUEUE_METRIC,
        deepest_queue: PER_POD_QUEUE_METRIC,
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
    fn read(&self, site: &str, cluster: &str, now_ms: i64, window_ms: i64) -> SiteReading {
        let key = LoadStore::key(site, cluster);
        let worst = |metric: &str| {
            self.store
                .window_worst(&key, metric, now_ms, window_ms, LOWER_IS_BETTER)
                .and_then(plausible)
        };
        let first = |metrics: &[&str]| metrics.iter().find_map(|metric| worst(metric));
        let sampled_at = self.store.newest_at(&key);
        SiteReading {
            in_flight: self.in_flight(&key, now_ms, window_ms),
            queued: first(self.series.queued),
            held: worst(self.series.held),
            deepest_queue: worst(self.series.deepest_queue),
            unready: self.unready(&key, sampled_at),
            sampled_at,
        }
    }
}

impl Mapped<'_> {
    /// What the site holds: per-unit running plus waiting, over its ready units, plus what
    /// flow control holds in front of them. Concluded per instant from readings taken
    /// together, then the worst instant in the window, so a unit count from one scrape never
    /// multiplies a running count from another.
    fn in_flight(&self, key: &str, now_ms: i64, window_ms: i64) -> Option<f64> {
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
                let running = plausible(running?)?;
                let endpoints = plausible(endpoints?)?;
                let queued = queued.and_then(plausible).unwrap_or(0.0);
                let held = held.and_then(plausible).unwrap_or(0.0);
                plausible((running + queued) * endpoints + held)
            },
        )
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
    fn read(&self, site: &str, cluster: &str, now_ms: i64, window_ms: i64) -> SiteReading {
        Mapped {
            store: self,
            series: &Series::LLM_D,
        }
        .read(site, cluster, now_ms, window_ms)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    static OTHER: Series = Series {
        running: &["vendor_running"],
        queued: &["vendor_queue"],
        endpoints: &["vendor_units"],
        held: "vendor_held",
        deepest_queue: "vendor_unit_queue",
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
        // (10 running + 0.5 waiting) per unit, 2 units, plus 3 held: 24 in flight.
        let expected = SiteReading {
            in_flight: Some(24.0),
            queued: Some(0.5),
            held: Some(3.0),
            deepest_queue: Some(2.0),
            unready: false,
            sampled_at: Some(1_000),
        };
        assert_eq!(mapped.read("a", "pool-a", 1_000, 30_000), expected);
        // The llm-d names see the sample stamp and nothing else.
        let provider = store.read("a", "pool-a", 1_000, 30_000);
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
        let reading = store.read("a", "pool-a", 1_000, 30_000);
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
        let reading = store.read("a", "pool-a", 3_000, 30_000);
        assert_eq!(reading.in_flight, Some(20.0));
        assert!(reading.unready, "the latest ready count is zero");
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
        assert_eq!(store.read("a", "pool-a", 1_000, 30_000).in_flight, None);
    }
}
