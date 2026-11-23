//! Closed loop: requests routed by the real selection change the load the next snapshot reads.
//!
//! A pick rule that sends every request to the lightest site of the last snapshot builds a queue
//! here. A test over a fixed load cannot see that, because nothing it routes changes what it reads.

//! Three sites of 40 slots and 2 s requests serve 60 a second, so 3 a step is half of capacity,
//! 4 is two thirds and 5 is five sixths. At count-only sites one waiting request is backlog.
//! Scheduler sites compare physical KV use against the original capacity after a service-slot
//! cut; half of original capacity is 80 percent of service capacity after the cut. The KV-binding
//! five-sixths case gates the old selector's reported ~160-request queue at half that peak.

use std::collections::VecDeque;

use super::{Harness, KeepAll, MODEL, NOW_MS, SiteSpec};
use crate::{descriptor::CapabilityKind, route::select_spread, snapshot::RouteSnapshot};

/// Simulated milliseconds per step.
const STEP_MS: i64 = 100;

/// How often every site publishes its load.
const SCRAPE_MS: i64 = 1_000;

/// How often the gateway takes a new snapshot: the delay that makes a hard pick herd.
const POLL_MS: i64 = 5_000;

/// How far back a snapshot reads load.
const HORIZON_MS: i64 = 3_000;

/// Requests a site runs at once.
const SLOTS: usize = 40;

/// Engines a site behind the scheduler runs, each publishing an average.
const ENGINES: usize = 10;

/// How long one request runs.
const SERVICE_MS: i64 = 2_000;

/// Simulated time a run lasts once its ceilings are taught.
const DURATION_MS: i64 = 150_000;

/// Simulated time every site idles after its ceiling is taught, so load starts on an idle grid.
const IDLE_MS: i64 = 10_000;

/// Simulated time before waiting is counted, allowing the seed to leave the horizon and
/// subsequent snapshots to settle on measured load.
const WARM_UP_MS: i64 = 30_000;

/// How many starting points a run is repeated over. The loop is chaotic enough that one starting
/// point can pass where the next queues a hundred, so a single run proves nothing.
const PHASES: usize = 5;

/// One site's requests: when each running one ends, and how many wait for a slot.
#[derive(Default)]
struct Serving {
    running: VecDeque<i64>,
    waiting: usize,
}

/// One site as it was routed and read at a snapshot boundary.
#[derive(serde::Serialize)]
struct SiteInterval {
    site: &'static str,
    running: usize,
    waiting: usize,
    kv_cache: Option<f64>,
    rho: Option<f64>,
    backlog: Option<bool>,
    full: Option<bool>,
    dispatched: usize,
}

/// Load and dispatches accumulated between two gateway snapshots.
#[derive(serde::Serialize)]
struct Interval {
    since_start_ms: i64,
    offered: usize,
    completed: usize,
    sites: Vec<SiteInterval>,
}

/// Reproducible evidence from one deterministic starting phase.
#[derive(serde::Serialize)]
struct RunEvidence {
    first_turn: usize,
    offered: usize,
    completed: usize,
    final_running: usize,
    final_waiting: usize,
    peak_waiting: usize,
    mean_waiting: f64,
    p95_waiting: usize,
    intervals: Vec<Interval>,
}

/// What a site publishes about its load, which decides how the gateway reads it.
#[derive(Clone, Copy)]
enum Publish {
    /// A request count from one serving unit. The gateway learns a ceiling, and one waiting
    /// request is a backlog.
    Count,
    /// What the llm-d scheduler publishes: averages per engine and physical KV-cache use.
    /// `kv_at_full` is the use at the original 40-slot capacity; reducing concurrent service
    /// slots does not reduce physical KV capacity or the number of engines. Saturation is the
    /// greater of KV use over 0.8 and the queue per engine over 5; backlog starts at one per engine.
    Scheduler { kv_at_full: f64 },
}

/// One closed-loop scenario.
#[derive(Clone, Copy)]
struct Scenario {
    /// Requests arriving per step.
    arriving: usize,
    /// How long every site idles before requests arrive; zero starts on the full-slot seed.
    /// Its initial pressure depends on the selected publication mode.
    idle_ms: i64,
    /// What the sites publish.
    publish: Publish,
    /// The second site's slots from 40 s on, when it loses capacity.
    cut_to: Option<usize>,
}

impl Scenario {
    /// Steady load on an idle grid.
    fn steady(arriving: usize, publish: Publish) -> Self {
        Self {
            arriving,
            idle_ms: IDLE_MS,
            publish,
            cut_to: None,
        }
    }

    /// The slots site `index` has `since_start_ms` in.
    fn slots(&self, index: usize, since_start_ms: i64) -> usize {
        match self.cut_to {
            Some(slots) if index == 1 && since_start_ms >= 40_000 => slots,
            _ => SLOTS,
        }
    }

    /// The most requests waiting at any one site over [`PHASES`] starting points.
    fn peak_waiting(&self, sites: &[SiteSpec]) -> usize {
        self.phases(sites)
            .iter()
            .map(|phase| phase.peak_waiting)
            .max()
            .unwrap_or(0)
    }

    /// One identical-input run for each deterministic turn phase.
    fn phases(&self, sites: &[SiteSpec]) -> Vec<RunEvidence> {
        (0..PHASES).map(|phase| self.run(sites, phase * 7_919)).collect()
    }

    /// The most requests waiting at any one site, after [`WARM_UP_MS`], while requests are routed
    /// between `sites` by the real selection over snapshots [`POLL_MS`] apart, the first request
    /// being turn `first_turn`.
    #[expect(
        clippy::too_many_lines,
        reason = "the simulation keeps ordered scrape, snapshot, arrival, and evidence boundaries together"
    )]
    fn run(&self, sites: &[SiteSpec], first_turn: usize) -> RunEvidence {
        let mut grid = Harness::new(HORIZON_MS);
        let mut snapshot = self.seed(&mut grid, sites);
        let mut serving: Vec<Serving> = sites.iter().map(|_| Serving::default()).collect();
        let (mut turn, mut peak) = (first_turn, 0_usize);
        let mut total_offered = 0_usize;
        let mut total_completed = 0_usize;
        let mut interval_offered = 0_usize;
        let mut interval_completed = 0_usize;
        let mut interval_dispatched = vec![0_usize; sites.len()];
        let mut queue_observations = Vec::new();
        let mut intervals = Vec::new();
        for step in 1..=DURATION_MS / STEP_MS {
            let since_start = step * STEP_MS;
            let at = NOW_MS + since_start;
            for (index, site) in serving.iter_mut().enumerate() {
                while site.running.front().is_some_and(|ends| *ends <= at) {
                    site.running.pop_front();
                    total_completed += 1;
                    interval_completed += 1;
                }
                while site.waiting > 0 && site.running.len() < self.slots(index, since_start) {
                    site.waiting -= 1;
                    site.running.push_back(at + SERVICE_MS);
                }
            }
            if since_start % SCRAPE_MS == 0 {
                for (spec, site) in sites.iter().zip(&serving) {
                    self.publish(&grid, spec, site, at);
                }
            }
            if since_start % POLL_MS == 0 {
                snapshot = grid.order(sites, at);
            }
            let arriving = if since_start < self.idle_ms { 0 } else { self.arriving };
            for _ in 0..arriving {
                total_offered += 1;
                interval_offered += 1;
                turn += 1;
                let chosen = select_spread(
                    &snapshot,
                    CapabilityKind::InferenceModel,
                    MODEL,
                    turn,
                    |_| true,
                    &KeepAll,
                )
                .expect("a site is chosen");
                let index = sites
                    .iter()
                    .position(|spec| *spec.site == *chosen.candidate.site)
                    .expect("the chosen site is one of the sites");
                interval_dispatched[index] += 1;
                let slots = self.slots(index, since_start);
                let site = &mut serving[index];
                if site.running.len() < slots {
                    site.running.push_back(at + SERVICE_MS);
                } else {
                    site.waiting += 1;
                }
            }
            if since_start >= WARM_UP_MS {
                peak = peak.max(serving.iter().map(|site| site.waiting).max().unwrap_or(0));
            }
            if since_start % POLL_MS == 0 {
                let interval_sites = sites
                    .iter()
                    .enumerate()
                    .map(|(index, spec)| {
                        let state = &serving[index];
                        let candidate = snapshot
                            .candidates
                            .iter()
                            .find(|candidate| candidate.site.as_ref() == spec.site);
                        let kv_cache = match self.publish {
                            Publish::Count => None,
                            Publish::Scheduler { kv_at_full } => {
                                Some(kv_at_full * count(state.running.len()) / count(SLOTS))
                            },
                        };
                        SiteInterval {
                            site: spec.site,
                            running: state.running.len(),
                            waiting: state.waiting,
                            kv_cache,
                            rho: candidate.and_then(|candidate| candidate.rho),
                            backlog: candidate.and_then(|candidate| candidate.backlog),
                            full: candidate.and_then(|candidate| candidate.full),
                            dispatched: interval_dispatched[index],
                        }
                    })
                    .collect();
                if since_start >= WARM_UP_MS {
                    queue_observations.extend(serving.iter().map(|site| site.waiting));
                }
                intervals.push(Interval {
                    since_start_ms: since_start,
                    offered: interval_offered,
                    completed: interval_completed,
                    sites: interval_sites,
                });
                interval_offered = 0;
                interval_completed = 0;
                interval_dispatched.fill(0);
            }
        }
        queue_observations.sort_unstable();
        let p95_index = queue_observations
            .len()
            .saturating_mul(95)
            .div_ceil(100)
            .saturating_sub(1);
        let mean_waiting = count(queue_observations.iter().sum()) / count(queue_observations.len());
        RunEvidence {
            first_turn,
            offered: total_offered,
            completed: total_completed,
            final_running: serving.iter().map(|site| site.running.len()).sum(),
            final_waiting: serving.iter().map(|site| site.waiting).sum(),
            peak_waiting: peak,
            mean_waiting,
            p95_waiting: queue_observations.get(p95_index).copied().unwrap_or_default(),
            intervals,
        }
    }

    /// Seed the first snapshot with a full-slot observation in the scenario's publication mode.
    fn seed(&self, grid: &mut Harness, sites: &[SiteSpec]) -> RouteSnapshot {
        match self.publish {
            Publish::Count => grid.refresh(sites, NOW_MS, |site| site.capacity, 0.0),
            Publish::Scheduler { .. } => {
                let full = Serving {
                    running: std::iter::repeat_n(NOW_MS + SERVICE_MS, SLOTS).collect(),
                    waiting: 0,
                };
                for spec in sites {
                    self.publish(grid, spec, &full, NOW_MS);
                }
                grid.order(sites, NOW_MS)
            },
        }
    }

    /// Publish what `site` holds at `at_ms`, as [`Publish`] says.
    fn publish(&self, grid: &Harness, spec: &SiteSpec, site: &Serving, at_ms: i64) {
        let (running, waiting) = (count(site.running.len()), count(site.waiting));
        match self.publish {
            Publish::Count => grid.publish(std::slice::from_ref(spec), at_ms, |_| running + waiting, waiting),
            Publish::Scheduler { kv_at_full } => {
                let labels = format!(r#"grid_site="{}",grid_provider="pool-{}""#, spec.site, spec.site);
                let engines = count(ENGINES);
                // The scheduler publishes averages per engine; retain fractional averages rather than flooring them.
                for (metric, value) in [
                    ("llm_d_epp_average_running_requests", running / engines),
                    ("llm_d_epp_average_queue_size", waiting / engines),
                    ("llm_d_epp_ready_endpoints", engines),
                    ("llm_d_epp_flow_control_queue_size", 0.0),
                    (
                        "llm_d_epp_average_kv_cache_utilization",
                        kv_at_full * running / count(SLOTS),
                    ),
                ] {
                    grid.store.ingest_at(
                        &format!("{metric}{{{labels}}} {value} {at_ms}"),
                        at_ms,
                        at_ms,
                        spec.site,
                    );
                }
            },
        }
    }
}

/// `requests` as the load a site publishes.
fn count(requests: usize) -> f64 {
    f64::from(u32::try_from(requests).unwrap_or(u32::MAX))
}

/// `names` as equal sites of [`SLOTS`] slots.
fn equal(names: &[&'static str]) -> Vec<SiteSpec> {
    names
        .iter()
        .map(|site| SiteSpec {
            site,
            capacity: count(SLOTS),
            enforced: count(SLOTS),
            in_flight: 0.0,
        })
        .collect()
}

/// KV-cache use that reaches saturation 1 at the original 40-slot service capacity.
const KV_BINDS: Publish = Publish::Scheduler { kv_at_full: 0.8 };

/// KV-cache use that leaves the queue as the limit.
const KV_SPARE: Publish = Publish::Scheduler { kv_at_full: 0.5 };

#[test]
fn three_equal_sites_at_half_of_capacity_build_no_queue() {
    let peak = Scenario::steady(3, Publish::Count).peak_waiting(&equal(&["a", "b", "c"]));
    assert!(
        peak <= SLOTS / 10,
        "with half of every site free, a queue of {peak} at one site is the pick herding (lighter-of-two reference: 17)"
    );
}

#[test]
fn sites_behind_the_scheduler_at_two_thirds_of_capacity_build_no_queue() {
    for publish in [KV_BINDS, KV_SPARE] {
        let peak = Scenario::steady(4, publish).peak_waiting(&equal(&["a", "b", "c"]));
        assert!(
            peak <= SLOTS / 4,
            "with a third of every site free, a queue of {peak} at one site is the pick herding (lighter-of-two reference: 51)"
        );
    }
}

#[test]
fn sites_behind_the_scheduler_at_five_sixths_of_capacity_stay_near_empty_when_kv_has_room() {
    let peak = Scenario::steady(5, KV_SPARE).peak_waiting(&equal(&["a", "b", "c"]));
    assert!(
        peak <= SLOTS / 2,
        "with a sixth of every site free, a queue of {peak} at one site is the pick herding (lighter-of-two reference: 154)"
    );
}

#[expect(
    clippy::print_stdout,
    reason = "machine-readable evidence is consumed by the qualification report"
)]
#[test]
fn sites_behind_the_scheduler_at_five_sixths_of_capacity_keep_soft_saturated_kv_sites_in_the_draw() {
    const MAX_PEAK_WAITING: usize = 80;
    let scenario = Scenario::steady(5, KV_BINDS);
    let sites = equal(&["a", "b", "c"]);
    let phases = scenario.phases(&sites);
    let worst_peak = phases.iter().map(|phase| phase.peak_waiting).max().unwrap_or(0);
    let accounted = phases
        .iter()
        .all(|phase| phase.offered == phase.completed + phase.final_running + phase.final_waiting);
    let interval_dispatches_conserved = phases.iter().all(|phase| {
        phase
            .intervals
            .iter()
            .all(|interval| interval.offered == interval.sites.iter().map(|site| site.dispatched).sum::<usize>())
    });
    println!(
        "GRID_SATURATION_EVIDENCE={}",
        serde_json::json!({
            "scenario": "three_equal_sites_five_sixths_kv_binds",
            "selector": "grid_site_route::select_spread",
            "arrivals_per_100ms_step": scenario.arriving,
            "service_time_ms": SERVICE_MS,
            "slots_per_site": SLOTS,
            "kv_at_full": 0.8,
            "sites": ["a", "b", "c"],
            "horizon_ms": DURATION_MS,
            "snapshot_period_ms": POLL_MS,
            "scrape_period_ms": SCRAPE_MS,
            "first_turn_phases": phases.iter().map(|phase| phase.first_turn).collect::<Vec<_>>(),
            "worst_peak_waiting": worst_peak,
            "max_peak_waiting": MAX_PEAK_WAITING,
            "request_accounting_conserved": accounted,
            "interval_dispatch_accounting_conserved": interval_dispatches_conserved,
            "phases": phases,
        })
    );
    assert!(
        accounted,
        "each offered request must be completed or remain running/queued; no traffic is dropped"
    );
    assert!(
        interval_dispatches_conserved,
        "each interval must dispatch every offered request exactly once"
    );
    assert!(
        worst_peak <= MAX_PEAK_WAITING,
        "five-sixths KV-binding load must keep the worst phase at or below half the reproduced ~160-request base queue; observed {worst_peak}"
    );
}

#[test]
fn sites_behind_the_scheduler_settle_after_a_burst() {
    for publish in [KV_BINDS, KV_SPARE] {
        let burst = Scenario {
            idle_ms: 0,
            ..Scenario::steady(4, publish)
        };
        let peak = burst.peak_waiting(&equal(&["a", "b", "c"]));
        assert!(
            peak <= SLOTS / 4,
            "a queue of {peak} at one site long after the burst is the pick herding (lighter-of-two reference: 55)"
        );
    }
}

#[test]
fn a_site_that_loses_three_quarters_of_its_slots_is_shed() {
    let cut = Scenario {
        cut_to: Some(SLOTS / 4),
        ..Scenario::steady(3, KV_SPARE)
    };
    let peak = cut.peak_waiting(&equal(&["a", "b", "c"]));
    assert!(
        peak <= SLOTS * 3 / 2,
        "a queue of {peak} at one site after the cut is the pick failing to redistribute load (lighter-of-two reference: 87)"
    );
}

#[test]
fn scheduler_seed_and_slot_cut_keep_the_declared_metric_units() {
    let sites = equal(&["a"]);
    for (publish, kv) in [(KV_SPARE, 0.5), (KV_BINDS, 0.8)] {
        let scenario = Scenario::steady(4, publish);
        let mut grid = Harness::new(HORIZON_MS);
        let snapshot = scenario.seed(&mut grid, &sites);
        for (metric, expected) in [
            ("llm_d_epp_ready_endpoints", count(ENGINES)),
            ("llm_d_epp_average_running_requests", count(SLOTS) / count(ENGINES)),
            ("llm_d_epp_average_kv_cache_utilization", kv),
        ] {
            let sample = grid.store.latest("a/pool-a", metric).expect("seed metric");
            assert!(
                (sample.value - expected).abs() < 1e-12,
                "{metric} must use scheduler units"
            );
        }
        let candidate = snapshot.candidates.first().expect("seeded site");
        assert!((candidate.rho.expect("scheduler pressure") - kv / 0.8).abs() < 1e-12);
        let reduced = Serving {
            running: std::iter::repeat_n(NOW_MS + SERVICE_MS, SLOTS / 4).collect(),
            waiting: 0,
        };
        scenario.publish(&grid, &sites[0], &reduced, NOW_MS + SCRAPE_MS);
        let sample = grid
            .store
            .latest("a/pool-a", "llm_d_epp_average_kv_cache_utilization")
            .expect("cut metric");
        assert!(
            (sample.value - kv / 4.0).abs() < 1e-12,
            "a service-slot cut retains physical KV capacity"
        );
    }
}
