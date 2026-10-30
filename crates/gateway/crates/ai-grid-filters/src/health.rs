//! Which backend clusters have no healthy endpoint, resolved off the request path.
//!
//! The route filter publishes Praxis's health registry here, and records each dialed
//! request's outcome; the control step reads both on its own tick, so a request never
//! judges raw health.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Instant,
};

use arc_swap::{ArcSwap, ArcSwapOption};
use praxis_core::health::{ClusterHealthState, HealthRegistry};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Consecutive failures that take a dial down.
const DOWN_AFTER: u32 = 2;

/// Consecutive successes that bring a down dial back up.
const UP_AFTER: u32 = 2;

/// How long a down dial is ordered last before a request may try it again, milliseconds.
const COOLDOWN_MS: u64 = 10_000; // 10 s

/// The process start every dial timestamp counts from.
static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Clusters whose every endpoint is down, kept by the control step.
#[derive(Debug, Default)]
pub struct ClusterHealth {
    /// The pipeline's registry, published by the route filter.
    registry: ArcSwapOption<HashMap<Arc<str>, ClusterHealthState>>,
    /// The clusters with no healthy endpoint, or a demoted dial, as of the last tick.
    down: ArcSwap<BTreeSet<Arc<str>>>,

    /// The applied config's dials, by cluster.
    dials: DialHealth,
}

impl ClusterHealth {
    /// Publish `registry` when it is not the one already held: one lock-free load per request.
    pub(crate) fn observe(&self, registry: Option<&HealthRegistry>) {
        let Some(registry) = registry else {
            return;
        };
        let held = self.registry.load();
        if !held.as_ref().is_some_and(|held| Arc::ptr_eq(held, registry)) {
            self.registry.store(Some(Arc::clone(registry)));
        }
    }

    /// Whether praxis has reported any cluster health yet.
    ///
    /// Without a registry the gateway knows nothing about backend health, so demotion
    /// stays off rather than treating every cluster as healthy or as down.
    #[cfg(test)]
    pub(crate) fn observed(&self) -> bool {
        self.registry.load().is_some()
    }

    /// The clusters that were down as of the last [`Self::update`].
    pub(crate) fn down(&self) -> Arc<BTreeSet<Arc<str>>> {
        self.down.load_full()
    }

    /// The dials' health, kept across applies.
    pub(crate) fn dials(&self) -> &DialHealth {
        &self.dials
    }

    /// Recompute the down set from the registry and the dials, returning whether it changed.
    pub(crate) fn update(&self) -> bool {
        self.update_at(monotonic_ms())
    }

    /// [`Self::update`] at `now_ms`.
    pub(crate) fn update_at(&self, now_ms: u64) -> bool {
        let mut now = self.registry.load().as_deref().map(down_clusters).unwrap_or_default();
        // A dial replaces a declared cluster of its name, so only the dial's own health speaks for it.
        let dialed = self.dials.states.load();
        now.retain(|cluster| !dialed.contains_key(cluster));
        now.extend(self.dials.demoted(now_ms));
        // Every tick: a dial that recovers from half open changes no down set.
        self.dials.export();
        if **self.down.load() == now {
            return false;
        }
        tracing::info!(down = ?now, "grid: backend clusters with no healthy endpoint changed");
        self.down.store(Arc::new(now));
        true
    }
}

/// Whether a routed request may take a dial.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Admission {
    /// The dial is up.
    Open,
    /// The dial is down and this request holds its one probe.
    Probe,
    /// The dial is down and another request holds its probe.
    Busy,
}

/// One dial's passive health, recorded on the response path with atomics only.
///
/// Down after [`DOWN_AFTER`] consecutive failures, up after [`UP_AFTER`] consecutive successes.
/// A down dial is ordered last, never excluded, for [`COOLDOWN_MS`]; then it is half open: one
/// request at a time may probe it, and one more failure orders it last again.
#[derive(Debug)]
pub(crate) struct DialState {
    /// The address dialed, so a moved cluster starts healthy.
    address: Arc<str>,

    /// When the dial went down or last failed while down, plus one; 0 while up.
    down_since: AtomicU64,

    /// Consecutive failures.
    failures: AtomicU32,

    /// When the probe of a down dial was taken, plus one; 0 while none is held.
    probe_since: AtomicU64,

    /// The cluster's failure counter, resolved once.
    failures_total: metrics::Counter,

    /// Consecutive successes.
    successes: AtomicU32,
}

impl DialState {
    /// A healthy dial for `cluster` at `address`.
    pub(crate) fn new(cluster: &str, address: Arc<str>) -> Self {
        Self {
            failures_total: metrics::counter!("grid_dial_failures_total", "cluster" => cluster.to_owned()),
            address,
            down_since: AtomicU64::new(0),
            failures: AtomicU32::new(0),
            probe_since: AtomicU64::new(0),
            successes: AtomicU32::new(0),
        }
    }

    /// Whether a request may take the dial now: always while up, else only as its one probe.
    pub(crate) fn admit(&self) -> Admission {
        if !self.down() {
            return Admission::Open;
        }
        self.admit_at(monotonic_ms())
    }

    /// [`Self::admit`] for a down dial at `now_ms`. A probe that never settles lapses after a cooldown.
    pub(crate) fn admit_at(&self, now_ms: u64) -> Admission {
        let held = self.probe_since.load(Ordering::Acquire);
        if held != 0 && now_ms.saturating_add(1).saturating_sub(held) < COOLDOWN_MS {
            return Admission::Busy;
        }
        match self
            .probe_since
            .compare_exchange(held, now_ms.saturating_add(1), Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Admission::Probe,
            Err(_) => Admission::Busy,
        }
    }

    /// Hand back the probe once its request settled.
    pub(crate) fn release_probe(&self) {
        self.probe_since.store(0, Ordering::Release);
    }

    /// Record one request's outcome at `now_ms`.
    pub(crate) fn record(&self, success: bool, now_ms: u64) {
        if success {
            self.failures.store(0, Ordering::Relaxed);
            let successes = self.successes.fetch_add(1, Ordering::Relaxed).saturating_add(1);
            if successes >= UP_AFTER {
                self.down_since.store(0, Ordering::Relaxed);
            }
            return;
        }
        self.failures_total.increment(1);
        self.successes.store(0, Ordering::Relaxed);
        let failures = self.failures.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        if failures >= DOWN_AFTER || self.down() {
            self.down_since.store(now_ms.saturating_add(1), Ordering::Relaxed);
        }
    }

    /// Whether the dial is down, half open included.
    pub(crate) fn down(&self) -> bool {
        self.down_since.load(Ordering::Relaxed) != 0
    }

    /// Whether the dial is ordered last at `now_ms`: down and still cooling down.
    pub(crate) fn demoted(&self, now_ms: u64) -> bool {
        let since = self.down_since.load(Ordering::Relaxed);
        since != 0 && now_ms.saturating_add(1).saturating_sub(since) < COOLDOWN_MS
    }
}

/// The applied config's dial states by cluster, read by the control tick.
#[derive(Debug, Default)]
pub(crate) struct DialHealth {
    /// Dial states by cluster, swapped on apply.
    states: ArcSwap<BTreeMap<Arc<str>, Arc<DialState>>>,
}

impl DialHealth {
    /// `cluster`'s state, kept from the running config so a reload does not forget a down dial,
    /// unless the cluster now dials another `address`.
    pub(crate) fn state(&self, cluster: &str, address: &str) -> Arc<DialState> {
        self.states
            .load()
            .get(cluster)
            .filter(|state| &*state.address == address)
            .map_or_else(|| Arc::new(DialState::new(cluster, Arc::from(address))), Arc::clone)
    }

    /// Track exactly `states`, the applied config's dials.
    pub(crate) fn adopt(&self, states: BTreeMap<Arc<str>, Arc<DialState>>) {
        for (cluster, _) in self
            .states
            .load()
            .iter()
            .filter(|(cluster, _)| !states.contains_key(*cluster))
        {
            metrics::gauge!("grid_dial_down", "cluster" => cluster.to_string()).set(0.0);
        }
        self.states.store(Arc::new(states));
    }

    /// The dials ordered last at `now_ms`.
    fn demoted(&self, now_ms: u64) -> impl Iterator<Item = Arc<str>> {
        let states = self.states.load_full();
        let demoted: Vec<Arc<str>> = states
            .iter()
            .filter(|(_, state)| state.demoted(now_ms))
            .map(|(cluster, _)| Arc::clone(cluster))
            .collect();
        demoted.into_iter()
    }

    /// Export each dial's down state.
    fn export(&self) {
        for (cluster, state) in self.states.load().iter() {
            metrics::gauge!("grid_dial_down", "cluster" => cluster.to_string()).set(if state.down() {
                1.0
            } else {
                0.0
            });
        }
    }
}

/// Milliseconds since the process started, on a clock that never goes back.
pub(crate) fn monotonic_ms() -> u64 {
    u64::try_from(EPOCH.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Clusters in `registry` with endpoints, none of them healthy.
fn down_clusters(registry: &HashMap<Arc<str>, ClusterHealthState>) -> BTreeSet<Arc<str>> {
    registry
        .iter()
        .filter(|(_, state)| {
            let (healthy, total) = state.endpoint_counts();
            total > 0 && healthy == 0
        })
        .map(|(name, _)| Arc::clone(name))
        .collect()
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use std::sync::Mutex;

    use praxis_core::health::{ClusterHealthEntry, EndpointHealth};

    use super::*;

    fn registry(clusters: &[(&str, &[bool])]) -> HealthRegistry {
        Arc::new(
            clusters
                .iter()
                .map(|(name, endpoints)| {
                    let health: Vec<EndpointHealth> = endpoints
                        .iter()
                        .map(|healthy| {
                            let endpoint = EndpointHealth::new();
                            if !healthy {
                                endpoint.mark_unhealthy();
                            }
                            endpoint
                        })
                        .collect();
                    let addresses = (0..endpoints.len())
                        .map(|i| Arc::from(format!("10.0.0.{i}:80")))
                        .collect();
                    let entry = ClusterHealthEntry::new(health, addresses, None, None);
                    (Arc::from(*name), Arc::new(entry))
                })
                .collect(),
        )
    }

    #[test]
    fn a_cluster_is_down_only_when_every_endpoint_is() {
        let health = ClusterHealth::default();
        health.observe(Some(&registry(&[
            ("site-a", &[false]),
            ("site-b", &[false, true]),
            ("site-d", &[true]),
        ])));
        assert!(health.update(), "the first tick finds site-a down");
        assert_eq!(*health.down(), BTreeSet::from([Arc::from("site-a")]));
        assert!(!health.update(), "an unchanged registry is not news");
    }

    #[test]
    fn a_recovered_cluster_leaves_the_down_set() {
        let health = ClusterHealth::default();
        health.observe(Some(&registry(&[("site-a", &[false])])));
        assert!(health.update());
        health.observe(Some(&registry(&[("site-a", &[true])])));
        assert!(health.update(), "the recovered cluster leaves the down set");
        assert!(health.down().is_empty());
    }

    /// Gauges by name and cluster, so a test reads `grid_dial_down` back.
    #[derive(Default)]
    struct Gauges(Mutex<BTreeMap<String, Arc<AtomicU64>>>);

    impl Gauges {
        /// `grid_dial_down` for `cluster`, if ever set.
        fn down(&self, cluster: &str) -> Option<bool> {
            let value = self
                .0
                .lock()
                .unwrap()
                .get(&format!("grid_dial_down:{cluster}"))
                .map(|value| value.load(Ordering::Relaxed))?;
            Some(value == 1.0_f64.to_bits())
        }
    }

    impl metrics::Recorder for Gauges {
        fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}

        fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}

        fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}

        fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
            metrics::Counter::noop()
        }

        fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            let cluster = key
                .labels()
                .find(|label| label.key() == "cluster")
                .map_or("", |label| label.value());
            let name = format!("{}:{cluster}", key.name());
            metrics::Gauge::from_arc(Arc::clone(self.0.lock().unwrap().entry(name).or_default()))
        }

        fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    #[test]
    fn the_down_gauge_clears_when_a_half_open_dial_recovers() {
        let gauges = Gauges::default();
        metrics::with_local_recorder(&gauges, || {
            let health = ClusterHealth::default();
            let dial = health.dials().state("east", "east:8443");
            health
                .dials()
                .adopt(BTreeMap::from([(Arc::from("east"), Arc::clone(&dial))]));
            dial.record(false, 1_000);
            dial.record(false, 2_000);
            assert!(health.update_at(2_000), "two failures demote the dial");
            assert_eq!(gauges.down("east"), Some(true));
            let half_open = 2_000 + COOLDOWN_MS;
            assert!(
                health.update_at(half_open),
                "the cooldown ends and the down set changes"
            );
            assert_eq!(gauges.down("east"), Some(true), "half open is still down");
            dial.record(true, half_open + 100);
            dial.record(true, half_open + 200);
            assert!(!health.update_at(half_open + 200), "recovery changes no down set");
            assert_eq!(gauges.down("east"), Some(false), "but the gauge still clears");
        });
    }

    #[test]
    fn a_dial_goes_down_after_two_failures_and_up_after_two_successes() {
        let dial = DialState::new("east", Arc::from("east:8443"));
        dial.record(false, 1_000);
        assert!(!dial.down(), "one failure is not enough");
        dial.record(true, 1_100);
        dial.record(false, 1_200);
        assert!(!dial.down(), "a success between failures resets the count");
        dial.record(false, 1_300);
        assert!(dial.down(), "two consecutive failures take it down");
        assert!(dial.demoted(1_300), "and order it last");
        dial.record(true, 1_400);
        assert!(dial.down(), "one success is not enough");
        dial.record(false, 1_500);
        dial.record(true, 1_600);
        assert!(dial.down(), "a failure between successes resets the count");
        dial.record(true, 1_700);
        assert!(!dial.down(), "two consecutive successes bring it up");
        assert!(!dial.demoted(1_700), "and it is ordered by load again");
    }

    #[test]
    fn a_down_dial_is_half_open_after_its_cooldown_and_one_failure_rearms_it() {
        let dial = DialState::new("east", Arc::from("east:8443"));
        dial.record(false, 1_000);
        dial.record(false, 2_000);
        assert!(dial.demoted(2_000 + COOLDOWN_MS - 1), "still cooling down");
        assert!(!dial.demoted(2_000 + COOLDOWN_MS), "half open: a request may try it");
        assert!(dial.down(), "half open is still down");
        dial.record(false, 20_000);
        assert!(dial.demoted(20_000), "one failure while half open orders it last again");
        assert!(!dial.demoted(20_000 + COOLDOWN_MS), "for another cooldown");
    }

    #[test]
    fn a_down_dial_admits_one_probe_at_a_time() {
        let dial = Arc::new(DialState::new("east", Arc::from("east:8443")));
        assert_eq!(dial.admit(), Admission::Open, "an up dial admits every request");
        dial.record(false, 1_000);
        dial.record(false, 1_000);
        let now = 1_000 + COOLDOWN_MS;
        let admitted: Vec<Admission> = std::thread::scope(|scope| {
            let racers: Vec<_> = std::iter::repeat_with(|| scope.spawn(|| dial.admit_at(now)))
                .take(16)
                .collect();
            racers.into_iter().map(|racer| racer.join().expect("racer")).collect()
        });
        assert_eq!(
            admitted
                .iter()
                .filter(|admission| **admission == Admission::Probe)
                .count(),
            1,
            "exactly one concurrent request probes: {admitted:?}"
        );
        assert_eq!(dial.admit_at(now + 1), Admission::Busy, "the rest wait for its outcome");
        dial.release_probe();
        assert_eq!(
            dial.admit_at(now + 2),
            Admission::Probe,
            "a settled probe frees the next one"
        );
        assert_eq!(
            dial.admit_at(now + 2 + COOLDOWN_MS),
            Admission::Probe,
            "a probe that never settles lapses"
        );
    }

    #[test]
    fn a_demoted_dial_joins_the_down_set_without_a_registry() {
        let health = ClusterHealth::default();
        let east = health.dials().state("east", "east:8443");
        health
            .dials()
            .adopt(BTreeMap::from([(Arc::from("east"), Arc::clone(&east))]));
        assert!(!health.update_at(1_000), "a healthy dial is not news");
        east.record(false, 1_000);
        east.record(false, 1_000);
        assert!(health.update_at(1_000), "the failing dial is news");
        assert_eq!(*health.down(), BTreeSet::from([Arc::from("east")]));
        assert!(health.update_at(1_000 + COOLDOWN_MS), "half open leaves the down set");
        assert!(health.down().is_empty(), "so the next request may try it");
    }

    #[test]
    fn a_dial_replacing_a_declared_cluster_takes_only_its_own_health() {
        let health = ClusterHealth::default();
        health.observe(Some(&registry(&[("east", &[false]), ("west", &[false])])));
        let east = health.dials().state("east", "east:8443");
        health
            .dials()
            .adopt(BTreeMap::from([(Arc::from("east"), Arc::clone(&east))]));
        assert!(health.update_at(1_000), "west's registry health is news");
        assert_eq!(
            *health.down(),
            BTreeSet::from([Arc::from("west")]),
            "a healthy dial is not down for its declared cluster's endpoints; a non-dialed cluster still is"
        );
        east.record(false, 1_000);
        east.record(false, 1_000);
        assert!(health.update_at(1_000), "the failing dial is news");
        assert_eq!(
            *health.down(),
            BTreeSet::from([Arc::from("east"), Arc::from("west")]),
            "two connect failures take the dial down"
        );
    }

    #[test]
    fn a_dial_keeps_its_health_across_an_apply_and_forgets_a_removed_one() {
        let health = DialHealth::default();
        let east = health.state("east", "east:8443");
        east.record(false, 1_000);
        east.record(false, 1_000);
        health.adopt(BTreeMap::from([(Arc::from("east"), Arc::clone(&east))]));
        assert!(
            Arc::ptr_eq(&health.state("east", "east:8443"), &east),
            "a reload keeps the down dial"
        );
        health.adopt(BTreeMap::new());
        assert!(
            !health.state("east", "east:8443").down(),
            "a dial that left starts healthy if it returns"
        );
    }

    #[test]
    fn a_cluster_that_moves_to_another_address_starts_healthy() {
        let health = DialHealth::default();
        let east = health.state("east", "east:8443");
        east.record(false, 1_000);
        east.record(false, 1_000);
        health.adopt(BTreeMap::from([(Arc::from("east"), Arc::clone(&east))]));
        let moved = health.state("east", "east-2:8443");
        assert!(!Arc::ptr_eq(&moved, &east), "the old address's state is not kept");
        assert!(!moved.down(), "the new address has not failed");
    }

    #[test]
    fn nothing_is_down_and_nothing_is_observed_without_a_registry() {
        let health = ClusterHealth::default();
        health.observe(None);
        assert!(!health.observed(), "no registry, so demotion stays off");
        assert!(!health.update());
        assert!(health.down().is_empty());
        health.observe(Some(&registry(&[("site-a", &[true])])));
        assert!(health.observed(), "praxis reported, so demotion applies");
    }
}
