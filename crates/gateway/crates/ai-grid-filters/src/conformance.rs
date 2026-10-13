//! Routing conformance scenarios: the expected outcome stated before the run.
//!
//! A scenario asserts a bound, an ordering or an absence rather than a measured figure, names
//! the signal it exercises, and ships with a negation that must fail. These target
//! [`select_spread`]; where selection is the older first-match, [`capacity_tracking`] fails by
//! construction.
//!
//! The invariants take a dispatch summary, so the same scenario can also be driven by a cluster
//! run against `llm-d-inference-sim`. The bound is a parameter for that reason: tight here,
//! loose where the noise is real.
//!
//! Two rules hold for every scenario. The negation is mutated too, because a negation that
//! cannot fail proves nothing. And an invariant names its preconditions: a slow site's dispatch
//! share measured 0.176 against a 0.283 slot share and a 0.147 throughput share, all three
//! correct answers to different questions.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::min_ident_chars,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    clippy::as_conversions,
    clippy::cast_precision_loss,
    reason = "a harness, compiled only under cfg(test)"
)]

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use grid_signals::LoadStore;

use crate::{
    descriptor::{AdmissionState, CandidateConfig, CapabilityKind, validate_candidates},
    route::{KeepAll, select_spread},
    serving::AvailabilitySettings,
    snapshot::{Inputs, Learned, RouteSnapshot},
};

/// The model every scenario routes.
const MODEL: &str = "llama";

/// The moment every run reads the store at. Samples are stamped relative to it.
const NOW_MS: i64 = 1_000_000;

/// Milliseconds between the settling refreshes, past `room_after_ms` once a few have passed.
const REFRESH_MS: i64 = 1_000;

/// Measuring refreshes after the teaching one. Saturation moves by `smoothing` per
/// refresh, so a single one leaves every site part-way from the ceiling it just taught.
const SETTLE: i64 = 8;

/// The freshness window the operator sends at the shipped cadence, which it computes
/// as a poll, a scrape, a timeout and a poll of slack. Scenarios assert relative to
/// the window they are handed, so a cadence change moves the boundary without
/// invalidating them.
pub(crate) const WINDOW_MS: i64 = 17_000;

/// One run's parameters. A struct because the suite keeps adding dimensions.
#[derive(Clone, Debug)]
pub(crate) struct Run {
    /// Which requests to dispatch. The draw hashes the turn, so this is the seed.
    pub(crate) turns: std::ops::Range<usize>,
    /// Which decider makes the choice.
    pub(crate) decider: Decider,
    /// What a negation keeps off the wire.
    pub(crate) withheld: Withheld,
    /// The freshness window the gateway reads load over.
    pub(crate) window_ms: i64,
    /// How old every published sample is when the run reads it.
    pub(crate) stale_ms: i64,
    /// Measuring refreshes after the teaching one. A scenario's verdict must not depend
    /// on this: if it does, the scenario is measuring the harness.
    pub(crate) settle: i64,
}

impl Run {
    /// The draw over `turns`, everything published and current.
    pub(crate) fn fresh(turns: std::ops::Range<usize>) -> Self {
        Self {
            turns,
            decider: Decider::Draw,
            withheld: Withheld::Nothing,
            window_ms: WINDOW_MS,
            stale_ms: 0,
            settle: SETTLE,
        }
    }
}

/// One site as a scenario declares it. Service time is not modelled here, so
/// every site is equally fast and the draw's fixed point is its capacity share.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SiteSpec {
    /// Site name, also its cluster's suffix.
    pub(crate) site: &'static str,
    /// Requests it declares it admits at once, the denominator of its published
    /// saturation and the draw's weight. One number in two roles.
    pub(crate) capacity: f64,
    /// Requests it can actually admit. Never published, and equal to `capacity`
    /// unless the scenario overdeclares. The spec asks every scenario to state its
    /// enforcement status, and this is where it says so.
    pub(crate) enforced: f64,
    /// Requests it holds, so `in_flight / capacity` is the published saturation.
    pub(crate) in_flight: f64,
}

/// A signal a negation keeps off the wire. The site still has the property; the
/// decider just cannot see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Withheld {
    /// Every signal published.
    Nothing,
    /// No queue-free sample, so nothing teaches a ceiling and the site falls to the floor.
    Capacity,
    /// No sample at all after the teaching one, so the site reports no room.
    Saturation,
    /// Nothing published at all.
    Everything,
}

impl Withheld {
    /// Whether saturation reaches the wire.
    const fn publishes_saturation(self) -> bool {
        !matches!(self, Self::Saturation | Self::Everything)
    }

    /// Whether capacity reaches the wire.
    const fn publishes_capacity(self) -> bool {
        !matches!(self, Self::Capacity | Self::Everything)
    }
}

/// What one site declared and what the decider sent it.
#[derive(Clone, Debug)]
pub(crate) struct Observed {
    /// Site name.
    pub(crate) site: String,
    /// Capacity as the scenario declared it, published or not.
    pub(crate) capacity: f64,
    /// Requests dispatched to it, the quantity routing decides.
    pub(crate) dispatched: usize,
}

/// Which decider a run uses. The negations exist to be shown failing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Decider {
    /// The capacity-weighted draw under test.
    Draw,
    /// The front admitted candidate every time, the pre-draw behaviour.
    FirstMatch,
    /// Each admitted candidate in turn, blind to capacity and load.
    RoundRobin,
}

/// Which half of a run a sample belongs to.
///
/// A site's ceiling is learned only from a sample holding load with nothing waiting, and
/// its saturation is read against that ceiling. One sample cannot say both, so a run
/// teaches first and measures second, carrying `Learned` between the two.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// The site holds its capacity, nothing waiting: this is what teaches the ceiling.
    Teach,
    /// The site holds what the scenario says, read against the ceiling just taught.
    /// Repeated so smoothed saturation settles on the load rather than on the ceiling.
    Measure(i64),
}

impl Phase {
    /// The clock this phase publishes and is read at, given the run's freshness window.
    ///
    /// Measuring starts past the window, so the teaching sample has aged out of the
    /// worst-in-window reading while the ceiling it taught survives in `Learned`.
    const fn now_ms(self, window_ms: i64) -> i64 {
        match self {
            Self::Teach => NOW_MS,
            Self::Measure(nth) => NOW_MS + window_ms + REFRESH_MS * (nth + 1),
        }
    }
}

/// Publish `spec`'s signals into `store`, less whatever `withheld` keeps off the wire.
fn publish(store: &LoadStore, spec: &SiteSpec, run: &Run, phase: Phase) {
    let labels = format!(r#"grid_site="{}",grid_provider="pool-{}""#, spec.site, spec.site);
    // One ready endpoint per site, so in-flight is the per-endpoint load and a
    // scenario's numbers read as the site holds them.
    let holding = match phase {
        // Teaching: the site holds what it can, with nothing waiting, which is the
        // only shape `ceiling` learns from.
        Phase::Teach => spec.capacity,
        Phase::Measure(_) => spec.in_flight,
    };
    // A site whose capacity is withheld never shows a queue-free sample, so nothing
    // teaches it a ceiling and it falls to the floor. A site whose saturation is
    // withheld publishes only the teaching sample, so it reports no room.
    let teaching_withheld = !run.withheld.publishes_capacity();
    if phase == Phase::Teach && teaching_withheld {
        return;
    }
    if matches!(phase, Phase::Measure(_)) && !run.withheld.publishes_saturation() {
        return;
    }
    // One whole request waiting blocks the ceiling, and in-flight counts waiting as
    // well as running, so the site still holds `holding` either way.
    let waiting = if teaching_withheld { 1.0 } else { 0.0 };
    let series = [
        ("llm_d_epp_average_running_requests", holding - waiting),
        ("llm_d_epp_average_queue_size", waiting),
        ("llm_d_epp_ready_endpoints", 1.0),
        ("llm_d_epp_flow_control_queue_size", 0.0),
    ];
    // Stamped in the past and read at the phase's clock, so `stale_ms` is the sample's age.
    let at_ms = phase.now_ms(run.window_ms);
    let stamped = at_ms.saturating_sub(run.stale_ms);
    for (metric, value) in series {
        let line = format!("{metric}{{{labels}}} {value} {stamped}");
        store.ingest_at(&line, at_ms, at_ms, spec.site);
    }
}

/// One site's candidate entry, as the serving config would declare it.
fn config(spec: &SiteSpec) -> CandidateConfig {
    CandidateConfig {
        admission: AdmissionState::NewAndExisting,
        cluster: format!("pool-{}", spec.site),
        credential: None,
        fresh: true,
        kind: CapabilityKind::InferenceModel,
        name: MODEL.to_owned(),
        site: spec.site.to_owned(),
        stable_id: None,
    }
}

/// A run a scenario drives refresh by refresh, carrying the learned state between them.
///
/// `snapshot` below is the fixed teach-then-measure shape most scenarios want. This is for
/// the ones that need their own sequence over time: an idle spell, a burst, a recovery.
pub(crate) struct Harness {
    store: LoadStore,
    learned: Learned,
    availability: AvailabilitySettings,
    window_ms: i64,
}

impl Harness {
    /// An empty grid reading load over `window_ms`.
    pub(crate) fn new(window_ms: i64) -> Self {
        Self {
            store: LoadStore::new(Duration::from_secs(600)),
            learned: Learned::default(),
            availability: AvailabilitySettings::default(),
            window_ms,
        }
    }

    /// Publish each site holding `holding` with `queued` waiting at `at_ms`, then order.
    ///
    /// One ready endpoint per site, so in-flight is the per-endpoint load. A whole request
    /// waiting is what stops a sample teaching a ceiling.
    pub(crate) fn refresh(
        &mut self,
        sites: &[SiteSpec],
        at_ms: i64,
        holding: impl Fn(&SiteSpec) -> f64,
        queued: f64,
    ) -> RouteSnapshot {
        for spec in sites {
            let labels = format!(r#"grid_site="{}",grid_provider="pool-{}""#, spec.site, spec.site);
            let running = (holding(spec) - queued).max(0.0);
            for (metric, value) in [
                ("llm_d_epp_average_running_requests", running),
                ("llm_d_epp_average_queue_size", queued),
                ("llm_d_epp_ready_endpoints", 1.0),
                ("llm_d_epp_flow_control_queue_size", 0.0),
            ] {
                let line = format!("{metric}{{{labels}}} {value} {at_ms}");
                self.store.ingest_at(&line, at_ms, at_ms, spec.site);
            }
        }
        let validated = validate_candidates(sites.iter().map(config).collect()).expect("candidates");
        RouteSnapshot::from_store(
            validated,
            Arc::from(sites[0].site),
            &mut Inputs {
                signals: &self.store,
                now_ms: at_ms,
                window_ms: self.window_ms,
                horizon_ms: self.window_ms,
                availability: &self.availability,
                learned: &mut self.learned,
            },
        )
    }
}

/// The snapshot for `sites`, built through the real store and the real ordering.
///
/// Two refreshes over one `Learned`: the first teaches each site's ceiling from a sample
/// holding its capacity with nothing waiting, the second reads the load the scenario
/// states against that ceiling. The decider sees only the raw provider series it reads in
/// production; nothing named `grid_*` is published, because nothing reads it.
fn snapshot(sites: &[SiteSpec], run: &Run) -> RouteSnapshot {
    let store = LoadStore::new(Duration::from_secs(60));
    let availability = AvailabilitySettings::default();
    let mut learned = Learned::default();
    let mut build = |phase: Phase| {
        for spec in sites {
            publish(&store, spec, run, phase);
        }
        let validated = validate_candidates(sites.iter().map(config).collect()).expect("candidates");
        // Mirrors control.rs::order: from_store, then shed. `demote` is skipped because
        // the harness registers no cluster health, and production skips it too until
        // health is observed.
        RouteSnapshot::from_store(
            validated,
            Arc::from(sites[0].site),
            &mut Inputs {
                signals: &store,
                now_ms: phase.now_ms(run.window_ms),
                window_ms: run.window_ms,
                horizon_ms: run.window_ms,
                availability: &availability,
                learned: &mut learned,
            },
        )
        .shed(&std::collections::BTreeSet::new(), &availability)
    };
    drop(build(Phase::Teach));
    let mut snapshot = build(Phase::Measure(0));
    for nth in 1..run.settle {
        snapshot = build(Phase::Measure(nth));
    }
    snapshot
}

/// Dispatch the requests `turns` names over `sites` under `decider`, with `withheld`
/// off the wire.
///
/// The draw is a hash of the turn rather than sampled, so a run is reproducible and
/// the range is what a seed would be elsewhere.
pub(crate) fn dispatch(sites: &[SiteSpec], run: &Run) -> Vec<Observed> {
    let snapshot = snapshot(sites, run);
    let order: Vec<&str> = snapshot.candidates.iter().map(|candidate| &*candidate.site).collect();
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for turn in run.turns.clone() {
        let chosen = match run.decider {
            Decider::Draw => select_spread(
                &snapshot,
                CapabilityKind::InferenceModel,
                MODEL,
                turn,
                |_| true,
                &KeepAll,
            )
            .map(|pick| &*pick.candidate.site),
            // The pre-draw behaviour: the front of a pre-ordered list.
            Decider::FirstMatch => order.first().copied(),
            Decider::RoundRobin => order.get(turn % order.len()).copied(),
        };
        *counts.entry(chosen.expect("a site takes the request")).or_insert(0) += 1;
    }
    sites
        .iter()
        .map(|spec| Observed {
            site: spec.site.to_owned(),
            capacity: spec.capacity,
            dispatched: counts.get(spec.site).copied().unwrap_or(0),
        })
        .collect()
}

/// Each site's dispatch share tracks its capacity share.
///
/// Holds only while service times are equal across sites. The draw's fixed point
/// is throughput share, capacity over service time, which coincides with capacity
/// share only at equal speed; a slow site measured 0.176 against a 0.283 slot
/// share. So a heterogeneous-speed run wants a different invariant, not a wider
/// bound on this one.
///
/// Two claims: the ordering survives a change of offered rate, and the band catches a decider
/// that orders correctly but weights wrongly.
///
/// # Errors
///
/// Returns the site and the figures when a share sits outside `tolerance` of its
/// capacity share, or when a larger site did not take strictly more.
pub(crate) fn capacity_tracking(observed: &[Observed], tolerance: f64) -> Result<(), String> {
    let total_capacity: f64 = observed.iter().map(|site| site.capacity).sum();
    let total_dispatched: usize = observed.iter().map(|site| site.dispatched).sum();
    if total_dispatched == 0 || total_capacity <= 0.0 {
        return Err("nothing was dispatched, so the scenario measured nothing".to_owned());
    }
    for site in observed {
        let want = site.capacity / total_capacity;
        let got = site.dispatched as f64 / total_dispatched as f64;
        if (got - want).abs() > tolerance {
            return Err(format!(
                "{} took {got:.3} of dispatch against a capacity share of {want:.3}, outside {tolerance}",
                site.site
            ));
        }
    }
    for bigger in observed {
        for smaller in observed {
            if bigger.capacity > smaller.capacity && bigger.dispatched <= smaller.dispatched {
                return Err(format!(
                    "{} has more capacity than {} but took {} against {}",
                    bigger.site, smaller.site, bigger.dispatched, smaller.dispatched
                ));
            }
        }
    }
    Ok(())
}

/// No site takes more than `ceiling` of dispatch.
///
/// The absence form of the suite's assertions, and the specific thing first-match
/// does: a grid with no signals must still spread, because taking the front of a
/// pre-ordered list is the strategy that pins.
///
/// # Errors
///
/// Returns the site and its share when one is at or above `ceiling`.
pub(crate) fn spreads(observed: &[Observed], ceiling: f64) -> Result<(), String> {
    let total: usize = observed.iter().map(|site| site.dispatched).sum();
    if total == 0 {
        return Err("nothing was dispatched, so the scenario measured nothing".to_owned());
    }
    for site in observed {
        let got = site.dispatched as f64 / total as f64;
        if got >= ceiling {
            return Err(format!(
                "{} took {got:.3} of dispatch, at or above {ceiling}",
                site.site
            ));
        }
    }
    Ok(())
}

/// A site holding less takes strictly more dispatch.
///
/// Precondition: equal capacities, so the saturation ordering is the load ordering.
/// This is the claim that load is read at all, which is what a sample inside the
/// freshness window buys.
///
/// # Errors
///
/// Returns the pair when a site holding less did not take strictly more.
pub(crate) fn load_shapes_dispatch(sites: &[SiteSpec], observed: &[Observed]) -> Result<(), String> {
    for (lighter, lighter_got) in sites.iter().zip(observed) {
        for (heavier, heavier_got) in sites.iter().zip(observed) {
            if lighter.in_flight < heavier.in_flight && lighter_got.dispatched <= heavier_got.dispatched {
                return Err(format!(
                    "{} holds {} against {}'s {} but took {} against {}",
                    lighter.site,
                    lighter.in_flight,
                    heavier.site,
                    heavier.in_flight,
                    lighter_got.dispatched,
                    heavier_got.dispatched
                ));
            }
        }
    }
    Ok(())
}

/// No site takes less dispatch than its share of the capacity it enforces.
///
/// Precondition: equal service times, and every site holding the same share of what
/// it can admit, so the only thing that varies across sites is whether the number a
/// site declares is the number it enforces.
///
/// One-sided: the upper bound is `capacity_tracking`'s job.
///
/// # Errors
///
/// Returns the site and the figures when its dispatch share sits more than
/// `tolerance` below its share of enforced capacity.
pub(crate) fn serving_share_honoured(sites: &[SiteSpec], observed: &[Observed], tolerance: f64) -> Result<(), String> {
    let total_enforced: f64 = sites.iter().map(|spec| spec.enforced).sum();
    let total_dispatched: usize = observed.iter().map(|site| site.dispatched).sum();
    if total_dispatched == 0 || total_enforced <= 0.0 {
        return Err("nothing was dispatched, so the scenario measured nothing".to_owned());
    }
    for (spec, got_site) in sites.iter().zip(observed) {
        let want = spec.enforced / total_enforced;
        let got = got_site.dispatched as f64 / total_dispatched as f64;
        if want - got > tolerance {
            return Err(format!(
                "{} took {got:.3} of dispatch against a serving share of {want:.3}, short by more than {tolerance}",
                spec.site
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Five capacities at one saturation, so capacity is the only thing that varies
    /// and every site is equally fast.
    const HETEROGENEOUS_CAPACITY: [SiteSpec; 5] = [
        SiteSpec {
            site: "hub",
            capacity: 40.0,
            enforced: 40.0,
            in_flight: 10.0,
        },
        SiteSpec {
            site: "factory",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 20.0,
        },
        SiteSpec {
            site: "retail",
            capacity: 120.0,
            enforced: 120.0,
            in_flight: 30.0,
        },
        SiteSpec {
            site: "depot",
            capacity: 160.0,
            enforced: 160.0,
            in_flight: 40.0,
        },
        SiteSpec {
            site: "campus",
            capacity: 200.0,
            enforced: 200.0,
            in_flight: 50.0,
        },
    ];

    /// Requests per run. The draw is a weighted random draw, so deviation falls as
    /// one over the root of this. Below about a thousand the bound would have to
    /// widen faster than it is worth.
    const DRAWS: usize = 3_000;

    /// Five equal sites, for the half of the signals-absent scenario whose invariant
    /// needs them equal.
    const EQUAL_CAPACITY: [SiteSpec; 5] = [
        SiteSpec {
            site: "hub",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 20.0,
        },
        SiteSpec {
            site: "factory",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 20.0,
        },
        SiteSpec {
            site: "retail",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 20.0,
        },
        SiteSpec {
            site: "depot",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 20.0,
        },
        SiteSpec {
            site: "campus",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 20.0,
        },
    ];

    /// How far a share may sit from its capacity share here, absolute.
    ///
    /// Measured: over three capacity mixes and six turn ranges the worst single-site
    /// deviation was 0.0147, so this has most of a factor of three in hand. The
    /// negations sit at 0.157 and beyond, so the gate fails a draw that degrades
    /// without stopping.
    const UNIT_TOLERANCE: f64 = 0.04;

    /// The same bound for a cluster run, where the noise is real: 11 measured runs on
    /// three sites put the worst single-site deviation at 0.066 and the median at
    /// 0.038. Kept here so the two sensitivities sit together rather than drifting.
    #[expect(dead_code, reason = "for the cluster driver, which is not built yet")]
    const CLUSTER_TOLERANCE: f64 = 0.10;

    /// A share at or above this is a pin rather than a spread.
    const CEILING: f64 = 0.9;

    /// Scenario: heterogeneous capacity, equal speed. Exercises the learned ceiling, which is
    /// what weights a site.
    #[test]
    fn heterogeneous_capacity_at_equal_speed_tracks_dispatch_share() {
        let observed = dispatch(&HETEROGENEOUS_CAPACITY, &Run::fresh(0..DRAWS));
        capacity_tracking(&observed, UNIT_TOLERANCE).expect("the draw tracks capacity");
    }

    /// Equal capacity, unequal load, so saturation is the only thing that varies and
    /// crossing the freshness window changes the decision observably.
    const UNEQUAL_LOAD: [SiteSpec; 5] = [
        SiteSpec {
            site: "hub",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 8.0,
        },
        SiteSpec {
            site: "factory",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 24.0,
        },
        SiteSpec {
            site: "retail",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 40.0,
        },
        SiteSpec {
            site: "depot",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 56.0,
        },
        SiteSpec {
            site: "campus",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 72.0,
        },
    ];

    /// Scenario: a site's own published sample is N old. Not relay staleness, which is not in
    /// the routing path.
    ///
    /// Nothing discounts by age inside the window, so a sample seconds old counts in full.
    /// Past it the sample reads absent rather than stale, which is the saturation-withheld case
    /// and not the no-signals one. Asserted against the window the run is handed, never a
    /// literal, since the operator computes it from the poll cadence.
    #[test]
    fn a_stale_published_signal_is_used_inside_the_window() {
        for stale_ms in [0, WINDOW_MS / 2, WINDOW_MS - 1] {
            let observed = dispatch(
                &UNEQUAL_LOAD,
                &Run {
                    stale_ms,
                    settle: SETTLE,
                    ..Run::fresh(0..DRAWS)
                },
            );
            load_shapes_dispatch(&UNEQUAL_LOAD, &observed)
                .unwrap_or_else(|why| panic!("{stale_ms}ms is inside the window: {why}"));
            spreads(&observed, CEILING).unwrap_or_else(|why| panic!("{stale_ms}ms collapsed: {why}"));
        }
    }

    /// The other half: past the window the sample reads absent rather than stale, so
    /// dispatch is the capacity-weighted band and matches withholding saturation.
    #[test]
    fn a_signal_past_the_window_reads_as_absent() {
        let withheld = dispatch(
            &UNEQUAL_LOAD,
            &Run {
                withheld: Withheld::Saturation,
                ..Run::fresh(0..DRAWS)
            },
        );
        let absent: Vec<usize> = withheld.iter().map(|site| site.dispatched).collect();
        for stale_ms in [WINDOW_MS + 1, WINDOW_MS * 4] {
            let observed = dispatch(
                &UNEQUAL_LOAD,
                &Run {
                    stale_ms,
                    settle: SETTLE,
                    ..Run::fresh(0..DRAWS)
                },
            );
            capacity_tracking(&observed, UNIT_TOLERANCE)
                .unwrap_or_else(|why| panic!("{stale_ms}ms is past the window: {why}"));
            let shares: Vec<usize> = observed.iter().map(|site| site.dispatched).collect();
            assert_eq!(
                shares, absent,
                "{stale_ms}ms past the window differs from signals absent"
            );
        }
    }

    /// The negation. With saturation off the wire there is no load to read at any age,
    /// so the inside-the-window half must fail.
    #[test]
    fn the_staleness_scenario_fails_without_the_signal_it_ages() {
        let observed = dispatch(
            &UNEQUAL_LOAD,
            &Run {
                withheld: Withheld::Saturation,
                ..Run::fresh(0..DRAWS)
            },
        );
        assert!(
            load_shapes_dispatch(&UNEQUAL_LOAD, &observed).is_err(),
            "dispatch was load-shaped with no load published, so the scenario does not read saturation"
        );
    }

    /// hub declares twice the requests it admits. The other two declare what they
    /// admit, and all three hold the same share of it, so the declaration is the
    /// only thing that differs.
    const OVERDECLARED: [SiteSpec; 5] = [
        SiteSpec {
            site: "hub",
            capacity: 160.0,
            enforced: 80.0,
            in_flight: 40.0,
        },
        SiteSpec {
            site: "factory",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 40.0,
        },
        SiteSpec {
            site: "retail",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 40.0,
        },
        SiteSpec {
            site: "depot",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 40.0,
        },
        SiteSpec {
            site: "campus",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 40.0,
        },
    ];

    /// The same grid with hub declaring what it enforces. The control: whatever the
    /// overdeclared run shows has to disappear here, or it is a property of the
    /// topology rather than of the overdeclaration.
    const HONEST: [SiteSpec; 5] = [
        SiteSpec {
            site: "hub",
            capacity: 80.0,
            enforced: 80.0,
            in_flight: 40.0,
        },
        OVERDECLARED[1],
        OVERDECLARED[2],
        OVERDECLARED[3],
        OVERDECLARED[4],
    ];

    /// Every site holding exactly what it enforces. hub still reports room, because
    /// its denominator is the number it declared.
    const OVERDECLARED_FULL: [SiteSpec; 5] = [
        SiteSpec {
            in_flight: 80.0,
            ..OVERDECLARED[0]
        },
        SiteSpec {
            in_flight: 80.0,
            ..OVERDECLARED[1]
        },
        SiteSpec {
            in_flight: 80.0,
            ..OVERDECLARED[2]
        },
        SiteSpec {
            in_flight: 80.0,
            ..OVERDECLARED[3]
        },
        SiteSpec {
            in_flight: 80.0,
            ..OVERDECLARED[4]
        },
    ];

    /// The honest grid, full. Its control.
    const HONEST_FULL: [SiteSpec; 5] = [
        SiteSpec {
            in_flight: 80.0,
            ..HONEST[0]
        },
        OVERDECLARED_FULL[1],
        OVERDECLARED_FULL[2],
        OVERDECLARED_FULL[3],
        OVERDECLARED_FULL[4],
    ];

    /// The control for the overdeclaration scenarios. Equal service times, every
    /// site holding the same share of what it admits, every site declaring what it
    /// admits: each takes its serving share, at half load and at full load alike.
    ///
    /// Without this, the two inverted assertions below could be reporting a property
    /// of the topology rather than of the overdeclaration, and an invariant that
    /// never holds anywhere is not an invariant.
    #[test]
    fn an_honest_grid_gives_every_site_its_serving_share() {
        for sites in [&HONEST, &HONEST_FULL] {
            let observed = dispatch(sites, &Run::fresh(0..DRAWS));
            serving_share_honoured(sites, &observed, UNIT_TOLERANCE).expect("no site is starved");
            spreads(&observed, CEILING).expect("no site takes everything");
        }
    }

    /// Scenario: declared capacity above enforced. Asserted inverted rather than absent,
    /// because it is a defect: flipping it to `expect` is what fixing the denominator looks
    /// like, and the suite notices the day that happens.
    ///
    /// Capacity is one number in two roles, the weight and the saturation denominator, so
    /// overdeclaring moves both the same way. Measured, three sites each holding half of what
    /// they admit and hub declaring twice: hub took 0.82 of dispatch against a serving share of
    /// 0.33, and the honest sites 0.09 each.
    ///
    /// What this cannot settle is whether the overdeclared site then queues, drops or slows.
    /// Dispatch is the decided quantity and the harness has no service model.
    #[test]
    fn declared_capacity_above_enforced_starves_the_honest_sites() {
        let observed = dispatch(&OVERDECLARED, &Run::fresh(0..DRAWS));
        assert!(
            serving_share_honoured(&OVERDECLARED, &observed, UNIT_TOLERANCE).is_err(),
            "no site was starved, so either the denominators were corrected, in which \
             case invert this assertion, or the scenario no longer overdeclares"
        );
    }

    /// The sharp end of the same defect: at the moment every site holds exactly what
    /// it admits, the honest sites report rho 1.0 and leave the draw while the
    /// overdeclared one reports 0.5 and keeps all of it.
    ///
    /// The third assertion names the mechanism. With saturation off the wire the same
    /// grid spreads, so what concentrates the traffic is the denominator rho is
    /// computed against, not the draw's weight. That is the number to fix.
    #[test]
    fn an_overdeclared_site_takes_the_whole_grid_when_every_site_is_full() {
        let observed = dispatch(&OVERDECLARED_FULL, &Run::fresh(0..DRAWS));
        assert!(
            spreads(&observed, CEILING).is_err(),
            "the full grid spread, so either rho is no longer computed against the \
             declared capacity, in which case invert this assertion, or the scenario \
             no longer fills the sites"
        );

        let honest = dispatch(&HONEST_FULL, &Run::fresh(0..DRAWS));
        spreads(&honest, CEILING).expect("an honest full grid keeps spreading");

        let blind = dispatch(
            &OVERDECLARED_FULL,
            &Run {
                withheld: Withheld::Saturation,
                ..Run::fresh(0..DRAWS)
            },
        );
        spreads(&blind, CEILING).expect("with no saturation published the overflow band spreads");
    }

    /// Scenario: every signal absent, which splits in two because selection does. With
    /// saturation gone no site reports room and the overflow band still weights by capacity, so
    /// dispatch must keep tracking it. With capacity gone too the only claim left is that it
    /// spreads.
    ///
    /// Precondition for the second half: equal capacities, since an even spread is the capacity
    /// share only when they are equal.
    #[test]
    #[ignore = "reproduces the uniform overflow band: capacity is only assigned alongside a measured \
                 in-flight sample, so under total signal loss the band has no weights and spreads evenly"]
    fn a_grid_with_no_signals_spreads_rather_than_pinning() {
        let unsaturated = dispatch(
            &HETEROGENEOUS_CAPACITY,
            &Run {
                withheld: Withheld::Saturation,
                ..Run::fresh(0..DRAWS)
            },
        );
        capacity_tracking(&unsaturated, UNIT_TOLERANCE).expect("the overflow band still weights by capacity");

        let blind = dispatch(
            &EQUAL_CAPACITY,
            &Run {
                withheld: Withheld::Everything,
                ..Run::fresh(0..DRAWS)
            },
        );
        capacity_tracking(&blind, UNIT_TOLERANCE).expect("equal sites take equal shares");
        spreads(&blind, CEILING).expect("no site takes everything");
    }

    /// The negation for the signals-absent scenario. First-match is what the grid did
    /// before the draw, and it is the behaviour the scenario exists to refuse.
    #[test]
    fn a_signals_blind_grid_pins_under_first_match() {
        let pinned = dispatch(
            &EQUAL_CAPACITY,
            &Run {
                decider: Decider::FirstMatch,
                withheld: Withheld::Everything,
                ..Run::fresh(0..DRAWS)
            },
        );
        assert!(
            spreads(&pinned, CEILING).is_err(),
            "first-match spread, so the scenario does not detect a pin"
        );
        assert!(
            capacity_tracking(&pinned, UNIT_TOLERANCE).is_err(),
            "first-match tracked capacity, which it cannot read"
        );
    }

    /// The negation, in both forms the spec asks for: a decider that cannot read the
    /// signal, and the signal withheld from a decider that would.
    #[test]
    fn the_capacity_scenario_fails_without_capacity_weighting() {
        for blind in [Decider::FirstMatch, Decider::RoundRobin] {
            let observed = dispatch(
                &HETEROGENEOUS_CAPACITY,
                &Run {
                    decider: blind,
                    ..Run::fresh(0..DRAWS)
                },
            );
            assert!(
                capacity_tracking(&observed, UNIT_TOLERANCE).is_err(),
                "{blind:?} passed a scenario it cannot satisfy"
            );
        }
        let unpublished = dispatch(
            &HETEROGENEOUS_CAPACITY,
            &Run {
                withheld: Withheld::Capacity,
                ..Run::fresh(0..DRAWS)
            },
        );
        assert!(
            capacity_tracking(&unpublished, UNIT_TOLERANCE).is_err(),
            "the draw passed with its weight off the wire, so the scenario does not read capacity"
        );
    }

    /// Every site holding exactly the ceiling it has learned, with nothing queued.
    ///
    /// No site has a backlog, so by the decider's own `full` test none is saturated. But a
    /// ceiling is only the most a site has been asked to hold, and `has_room` is
    /// `rho < 1.0`, so every site reads as having no room and selection falls to the
    /// overflow band: the degraded arm, reached by a grid showing no strain anywhere.
    const ALL_AT_CEILING: [SiteSpec; 3] = [
        SiteSpec {
            site: "hub",
            capacity: 100.0,
            enforced: 100.0,
            in_flight: 100.0,
        },
        SiteSpec {
            site: "factory",
            capacity: 100.0,
            enforced: 100.0,
            in_flight: 100.0,
        },
        SiteSpec {
            site: "retail",
            capacity: 100.0,
            enforced: 100.0,
            in_flight: 100.0,
        },
    ];

    /// A grid with no backlog anywhere does not route through the overflow band.
    ///
    /// The band is the no-room arm. Reaching it says every site is out of room, which
    /// the decider elsewhere defines as being at the ceiling *with* a backlog. With no
    /// queue anywhere, nothing has shown a limit, and the ceilings are only what each
    /// site has so far been asked to carry.
    #[test]
    #[ignore = "reproduces the two definitions of saturated: `full` requires a backlog, `has_room` is \
                 rho < 1.0, so a grid with no queue anywhere routes through the overflow band"]
    fn a_grid_with_no_backlog_anywhere_is_not_out_of_room() {
        let run = Run::fresh(0..DRAWS);
        let snap = snapshot(&ALL_AT_CEILING, &run);
        let with_room = snap
            .candidates
            .iter()
            .filter(|candidate| candidate.rho.is_some_and(|rho| rho < 1.0))
            .count();
        let backlogged = snap.candidates.iter().filter(|c| c.full == Some(true)).count();
        assert!(
            with_room > 0,
            "no site carries a backlog ({backlogged} of {} report full), yet every site reads as \
             out of room, so selection falls to the overflow band",
            snap.candidates.len()
        );
    }

    /// The verdict must not depend on how long the harness settles.
    ///
    /// A scenario whose pass or fail turns on `settle` is measuring the harness, not the
    /// decider. This reports the worst deviation from capacity share across a sweep, so a
    /// harness-sensitive scenario is visible rather than silently green at one value.
    #[test]
    fn the_capacity_verdict_does_not_depend_on_settling() {
        let caps: f64 = HETEROGENEOUS_CAPACITY.iter().map(|spec| spec.capacity).sum();
        let mut worst_by_settle = Vec::new();
        for settle in [1_i64, 2, 4, 8, 16, 32] {
            let run = Run {
                settle,
                ..Run::fresh(0..DRAWS)
            };
            let observed = dispatch(&HETEROGENEOUS_CAPACITY, &run);
            let total: usize = observed.iter().map(|site| site.dispatched).sum();
            let worst = observed
                .iter()
                .zip(&HETEROGENEOUS_CAPACITY)
                .map(|(got, spec)| (got.dispatched as f64 / total as f64 - spec.capacity / caps).abs())
                .fold(0.0_f64, f64::max);
            worst_by_settle.push((settle, worst));
        }
        let report: Vec<String> = worst_by_settle
            .iter()
            .map(|(settle, worst)| format!("settle {settle}: {worst:.3}"))
            .collect();
        let spread = worst_by_settle.iter().map(|(_, w)| *w).fold(0.0_f64, f64::max)
            - worst_by_settle.iter().map(|(_, w)| *w).fold(f64::INFINITY, f64::min);
        assert!(
            spread < 0.05,
            "the verdict moves with the harness's settling, by {spread:.3}: {}",
            report.join(", ")
        );
    }

    /// An idle hour, then a burst: a grid quiet overnight and busy in the morning, which the
    /// stepped-load lab run cannot produce. A burst teaches nothing, so the ceiling the idle
    /// hour leaves is the one the burst is read against.
    const IDLE_THEN_BURST: [SiteSpec; 3] = [
        SiteSpec {
            site: "hub",
            capacity: 40.0,
            enforced: 40.0,
            in_flight: 40.0,
        },
        SiteSpec {
            site: "factory",
            capacity: 120.0,
            enforced: 120.0,
            in_flight: 120.0,
        },
        SiteSpec {
            site: "retail",
            capacity: 200.0,
            enforced: 200.0,
            in_flight: 200.0,
        },
    ];

    /// After an idle hour the grid still tells a small site from a large one.
    #[test]
    fn an_idle_hour_leaves_each_site_its_own_ceiling() {
        let mut grid = Harness::new(WINDOW_MS);
        let hour_ms = 3_600_000;
        // Every site carries its capacity with an empty queue, which is what teaches a ceiling.
        grid.refresh(&IDLE_THEN_BURST, NOW_MS, |spec| spec.capacity, 0.0);
        // An idle hour, sampled as the relay would: holding nothing, queueing nothing.
        for minute in (10..=60).step_by(10) {
            grid.refresh(&IDLE_THEN_BURST, NOW_MS + i64::from(minute) * 60_000, |_| 0.0, 0.0);
        }
        let after = grid.refresh(&IDLE_THEN_BURST, NOW_MS + hour_ms, |_| 0.0, 0.0);
        let ceilings: Vec<f64> = after.candidates.iter().filter_map(|site| site.capacity).collect();
        let spread =
            ceilings.iter().fold(0.0_f64, |a, b| a.max(*b)) - ceilings.iter().fold(f64::INFINITY, |a, b| a.min(*b));
        assert!(
            spread > 1.0,
            "a five-to-one capacity spread survived an idle hour as {spread:.1} of ceiling \
             spread: {ceilings:?}"
        );
    }

    /// The burst is measured against the ceiling the load taught, not against a floor.
    #[test]
    fn a_burst_after_an_idle_hour_reads_against_the_ceiling_it_taught() {
        let mut grid = Harness::new(WINDOW_MS);
        let hour_ms = 3_600_000;
        grid.refresh(&IDLE_THEN_BURST, NOW_MS, |spec| spec.capacity, 0.0);
        for minute in (10..=60).step_by(10) {
            grid.refresh(&IDLE_THEN_BURST, NOW_MS + i64::from(minute) * 60_000, |_| 0.0, 0.0);
        }
        // The burst: every site carrying its capacity again, and queueing, as a burst does.
        let mut burst = grid.refresh(&IDLE_THEN_BURST, NOW_MS + hour_ms, |spec| spec.capacity, 2.0);
        for nth in 1..6 {
            burst = grid.refresh(
                &IDLE_THEN_BURST,
                NOW_MS + hour_ms + nth * 1_000,
                |spec| spec.capacity,
                2.0,
            );
        }
        let ceilings: Vec<f64> = burst.candidates.iter().filter_map(|site| site.capacity).collect();
        let carried: Vec<f64> = IDLE_THEN_BURST.iter().map(|spec| spec.capacity).collect();
        let taught_back = ceilings
            .iter()
            .zip(&carried)
            .any(|(ceiling, capacity)| *ceiling > capacity / 2.0);
        assert!(
            taught_back,
            "every site carried its capacity through the burst and none taught a ceiling \
             above half of it: ceilings {ceilings:?} against capacities {carried:?}. \
             Saturation is read against those ceilings"
        );
    }

    /// A quarter load held half an hour reads a quarter saturated, not full: nothing brings the
    /// ceiling down to meet the load.
    #[test]
    fn a_steady_quarter_load_stays_a_quarter_saturated() {
        let sites = EQUAL_CAPACITY;
        let quarter = |spec: &SiteSpec| spec.capacity / 4.0;
        let mut grid = Harness::new(WINDOW_MS);
        // The site has carried its capacity, so that is what it has learned.
        grid.refresh(&sites, NOW_MS, |spec| spec.capacity, 0.0);
        // Then a steady quarter of it, with nothing queued: the site is comfortable.
        let mut snapshot = grid.refresh(&sites, NOW_MS + 5_000, quarter, 0.0);
        for minute in 1..=30 {
            snapshot = grid.refresh(&sites, NOW_MS + i64::from(minute) * 60_000, quarter, 0.0);
        }
        let ceilings: Vec<f64> = snapshot.candidates.iter().filter_map(|site| site.capacity).collect();
        let rhos: Vec<f64> = snapshot.candidates.iter().filter_map(|site| site.rho).collect();
        // A site holding a quarter of what it can hold is a quarter saturated. Anything
        // approaching 1.0 is the ceiling having fallen to meet the load.
        let worst = rhos.iter().fold(0.0_f64, |a, b| a.max(*b));
        assert!(
            worst < 0.5,
            "every site has run a steady quarter of its capacity with an empty queue for half \
             an hour, and reads {worst:.2} saturated rather than 0.25: ceilings {ceilings:?} \
             against a capacity of 80, saturation {rhos:?}"
        );
    }
}
