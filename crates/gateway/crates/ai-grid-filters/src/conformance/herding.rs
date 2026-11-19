//! Closed loop: requests routed by the real selection change the load the next snapshot reads.
//!
//! A pick rule that sends every request to the lightest site of the last snapshot builds a queue
//! here. A test over a fixed load cannot see that, because nothing it routes changes what it reads.

use std::collections::VecDeque;

use super::{Harness, KeepAll, MODEL, NOW_MS, SiteSpec};
use crate::{descriptor::CapabilityKind, route::select_spread};

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

/// Simulated time before waiting is counted: the teaching sample reads every site as full until
/// it leaves the horizon and the first snapshots after it settle.
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

/// What a site publishes about its load, which decides how the gateway reads it.
#[derive(Clone, Copy)]
enum Publish {
    /// A request count from one serving unit. The gateway learns a ceiling, and one waiting
    /// request is a backlog.
    Count,
    /// What the llm-d scheduler publishes: averages per engine and KV-cache use, here `kv_at_full`
    /// when every slot runs. Saturation is the greater of KV use over 0.8 and the queue per engine
    /// over 5, and a backlog is one waiting request per engine.
    Scheduler { kv_at_full: f64 },
}

/// One closed-loop scenario.
#[derive(Clone, Copy)]
struct Scenario {
    /// Requests arriving per step.
    arriving: usize,
    /// How long every site idles before requests arrive; zero starts with a burst onto sites the
    /// teaching sample still reads as full.
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
        (0..PHASES)
            .map(|phase| self.run(sites, phase * 7_919))
            .max()
            .unwrap_or(0)
    }

    /// The most requests waiting at any one site, after [`WARM_UP_MS`], while requests are routed
    /// between `sites` by the real selection over snapshots [`POLL_MS`] apart, the first request
    /// being turn `first_turn`.
    fn run(&self, sites: &[SiteSpec], first_turn: usize) -> usize {
        let mut grid = Harness::new(HORIZON_MS);
        // Teach every ceiling with one sample holding its slots and nothing waiting.
        let mut snapshot = grid.refresh(sites, NOW_MS, |site| site.capacity, 0.0);
        let mut serving: Vec<Serving> = sites.iter().map(|_| Serving::default()).collect();
        let (mut turn, mut peak) = (first_turn, 0_usize);
        for step in 1..=DURATION_MS / STEP_MS {
            let since_start = step * STEP_MS;
            let at = NOW_MS + since_start;
            for (index, site) in serving.iter_mut().enumerate() {
                while site.running.front().is_some_and(|ends| *ends <= at) {
                    site.running.pop_front();
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
        }
        peak
    }

    /// Publish what `site` holds at `at_ms`, as [`Publish`] says.
    fn publish(&self, grid: &Harness, spec: &SiteSpec, site: &Serving, at_ms: i64) {
        let (running, waiting) = (count(site.running.len()), count(site.waiting));
        match self.publish {
            Publish::Count => grid.publish(std::slice::from_ref(spec), at_ms, |_| running + waiting, waiting),
            Publish::Scheduler { kv_at_full } => {
                let labels = format!(r#"grid_site="{}",grid_provider="pool-{}""#, spec.site, spec.site);
                let engines = count(ENGINES);
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

/// KV-cache use that binds before the slots do: saturation reads 1 when every slot runs.
const KV_BINDS: Publish = Publish::Scheduler { kv_at_full: 0.8 };

/// KV-cache use that leaves the queue as the limit.
const KV_SPARE: Publish = Publish::Scheduler { kv_at_full: 0.5 };

// Three sites of 40 slots and 2 s requests serve 60 a second, so 3 a step is half of capacity,
// 4 is two thirds and 5 is five sixths. Each bound is a few times what the pick leaves and well
// under what taking the lighter of two outright leaves, which the comment beside it gives.

#[test]
fn three_equal_sites_at_half_of_capacity_build_no_queue() {
    // Outright: 17. Above about two thirds a count-only site queues under either rule, because one
    // waiting request is a backlog there and drops the site until the next snapshot.
    let peak = Scenario::steady(3, Publish::Count).peak_waiting(&equal(&["a", "b", "c"]));
    assert!(
        peak <= SLOTS / 10,
        "with half of every site free, a queue of {peak} at one site is the pick herding"
    );
}

#[test]
fn sites_behind_the_scheduler_at_two_thirds_of_capacity_build_no_queue() {
    // Outright: 51.
    for publish in [KV_BINDS, KV_SPARE] {
        let peak = Scenario::steady(4, publish).peak_waiting(&equal(&["a", "b", "c"]));
        assert!(
            peak <= SLOTS / 4,
            "with a third of every site free, a queue of {peak} at one site is the pick herding"
        );
    }
}

#[test]
fn sites_behind_the_scheduler_at_five_sixths_of_capacity_stay_near_empty_when_kv_has_room() {
    // Outright: 154. Where KV binds, a site at saturation 1 has no room and is dropped, and about
    // 160 wait under either rule.
    let peak = Scenario::steady(5, KV_SPARE).peak_waiting(&equal(&["a", "b", "c"]));
    assert!(
        peak <= SLOTS / 2,
        "with a sixth of every site free, a queue of {peak} at one site is the pick herding"
    );
}

#[test]
fn sites_behind_the_scheduler_settle_after_a_burst() {
    // Outright: 55. The first requests land on sites the teaching sample still reads as full.
    for publish in [KV_BINDS, KV_SPARE] {
        let burst = Scenario {
            idle_ms: 0,
            ..Scenario::steady(4, publish)
        };
        let peak = burst.peak_waiting(&equal(&["a", "b", "c"]));
        assert!(
            peak <= SLOTS / 4,
            "a queue of {peak} at one site long after the burst is the pick herding"
        );
    }
}

#[test]
fn a_site_that_loses_three_quarters_of_its_slots_is_shed() {
    // Outright: 87 at the cut site. Half of capacity before the cut is 80 percent after it.
    let cut = Scenario {
        cut_to: Some(SLOTS / 4),
        ..Scenario::steady(3, KV_SPARE)
    };
    let peak = cut.peak_waiting(&equal(&["a", "b", "c"]));
    assert!(
        peak <= SLOTS * 3 / 2,
        "a queue of {peak} at the cut site is the pick failing to move away from it"
    );
}
