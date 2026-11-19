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

/// How long one request runs.
const SERVICE_MS: i64 = 2_000;

/// Simulated time a run lasts once its ceilings are taught.
const DURATION_MS: i64 = 150_000;

/// Simulated time every site idles after its ceiling is taught, so load starts on an idle grid.
const IDLE_MS: i64 = 10_000;

/// Simulated time before waiting is counted: the teaching sample reads every site as full until
/// it leaves the horizon and the first snapshots after it settle.
const WARM_UP_MS: i64 = 30_000;

/// One site's requests: when each running one ends, and how many wait for a slot.
#[derive(Default)]
struct Serving {
    running: VecDeque<i64>,
    waiting: usize,
}

/// How many starting points a run is repeated over. The loop is chaotic enough that one starting
/// point can pass where the next queues a hundred, so a single run proves nothing.
const PHASES: usize = 5;

/// The most requests waiting at any one site over [`PHASES`] runs of [`peak_waiting_from`].
fn peak_waiting(sites: &[SiteSpec], arriving: usize) -> usize {
    (0..PHASES)
        .map(|phase| peak_waiting_from(sites, arriving, phase * 7_919))
        .max()
        .unwrap_or(0)
}

/// The most requests waiting at any one site, after [`WARM_UP_MS`], while `arriving` requests per
/// step are routed between `sites` by the real selection over snapshots [`POLL_MS`] apart, the
/// first request being turn `first_turn`.
fn peak_waiting_from(sites: &[SiteSpec], arriving: usize, first_turn: usize) -> usize {
    let mut grid = Harness::new(HORIZON_MS);
    // Teach every ceiling with one sample holding its slots and nothing waiting.
    let mut snapshot = grid.refresh(sites, NOW_MS, |site| site.capacity, 0.0);
    let mut serving: Vec<Serving> = sites.iter().map(|_| Serving::default()).collect();
    let (mut turn, mut peak) = (first_turn, 0_usize);
    for step in 1..=DURATION_MS / STEP_MS {
        let at = NOW_MS + step * STEP_MS;
        let arriving = if at - NOW_MS < IDLE_MS { 0 } else { arriving };
        for site in &mut serving {
            while site.running.front().is_some_and(|ends| *ends <= at) {
                site.running.pop_front();
            }
            while site.waiting > 0 && site.running.len() < SLOTS {
                site.waiting -= 1;
                site.running.push_back(at + SERVICE_MS);
            }
        }
        if (at - NOW_MS) % SCRAPE_MS == 0 {
            for (spec, site) in sites.iter().zip(&serving) {
                let (holding, waiting) = (count(site.running.len() + site.waiting), count(site.waiting));
                grid.publish(std::slice::from_ref(spec), at, |_| holding, waiting);
            }
        }
        if (at - NOW_MS) % POLL_MS == 0 {
            snapshot = grid.order(sites, at);
        }
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
            let site = &mut serving[index];
            if site.running.len() < SLOTS {
                site.running.push_back(at + SERVICE_MS);
            } else {
                site.waiting += 1;
            }
        }
        if at - NOW_MS >= WARM_UP_MS {
            peak = peak.max(serving.iter().map(|site| site.waiting).max().unwrap_or(0));
        }
    }
    peak
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

#[test]
fn three_equal_sites_at_half_of_capacity_build_no_queue() {
    // Three sites of 40 slots and 2 s requests serve 60 a second; 3 a step is 30 a second.
    // Taking the lighter of two outright leaves 17 waiting here. Above about two thirds of
    // capacity a queue forms under either rule, because a site showing any queue is left out
    // until the next snapshot and the rest cannot carry its share.
    let peak = peak_waiting(&equal(&["a", "b", "c"]), 3);
    assert!(
        peak <= SLOTS / 10,
        "with half of every site free, a queue of {peak} at one site is the pick herding"
    );
}
