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

mod revisions;

use crate::{
    descriptor::{AdmissionState, CandidateConfig, CapabilityKind, validate_candidates, validate_serving_candidates},
    route::{KeepAll, select_spread},
    serving::AvailabilitySettings,
    snapshot::{Gauged, Inputs, Learned, MIN_BACKLOG_SPELL_MS, RouteSnapshot},
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
    /// No queue-free sample, so nothing teaches the soft bound. A backlog at the site's limit
    /// still teaches the hard one, which is why this no longer leaves capacity unknown.
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
    gauged: Gauged,
    availability: AvailabilitySettings,
    window_ms: i64,
    /// Serving units each site publishes. The request series are per unit, so a site's total
    /// is the published count times this, which is how the gateway reads them.
    units: f64,
}

impl Harness {
    /// An empty grid with `share` as the explore floor.
    pub(crate) fn with_explore_floor(window_ms: i64, share: f64) -> Self {
        let mut harness = Self::new(window_ms);
        harness.availability.explore_floor = share;
        harness
    }

    /// Clear every site's soft bound, as a periodic reset would.
    pub(crate) fn reset_soft(&mut self) {
        self.gauged.learned.clear_soft();
    }

    /// An empty grid reading load over `window_ms`.
    pub(crate) fn new(window_ms: i64) -> Self {
        Self {
            store: LoadStore::new(Duration::from_secs(600)),
            gauged: Gauged::new(),
            availability: AvailabilitySettings::default(),
            window_ms,
            units: 1.0,
        }
    }

    /// Set how many serving units each site publishes from here on.
    pub(crate) fn set_units(&mut self, units: f64) {
        self.units = units;
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
        self.refresh_with_saturation(sites, at_ms, holding, queued, |_| None)
    }

    /// As `refresh`, with each site's own fullness reading where it publishes one. An llm-d
    /// site publishes it as KV cache utilization.
    #[expect(clippy::too_many_arguments, reason = "a harness publisher, one argument per series")]
    pub(crate) fn refresh_with_saturation(
        &mut self,
        sites: &[SiteSpec],
        at_ms: i64,
        holding: impl Fn(&SiteSpec) -> f64,
        queued: f64,
        reported: impl Fn(&SiteSpec) -> Option<f64>,
    ) -> RouteSnapshot {
        for spec in sites {
            if let Some(share) = reported(spec) {
                let labels = format!(r#"grid_site="{}",grid_provider="pool-{}""#, spec.site, spec.site);
                let line = format!("llm_d_epp_average_kv_cache_utilization{{{labels}}} {share} {at_ms}");
                self.store.ingest_at(&line, at_ms, at_ms, spec.site);
            }
        }
        self.publish(sites, at_ms, holding, queued);
        self.order(sites, at_ms)
    }

    /// Deliver telemetry independently of topology acceptance and snapshot refresh.
    fn publish(&self, sites: &[SiteSpec], at_ms: i64, holding: impl Fn(&SiteSpec) -> f64, queued: f64) {
        for spec in sites {
            let labels = format!(r#"grid_site="{}",grid_provider="pool-{}""#, spec.site, spec.site);
            let running = (holding(spec) - queued).max(0.0);
            for (metric, value) in [
                ("llm_d_epp_average_running_requests", running),
                ("llm_d_epp_average_queue_size", queued),
                ("llm_d_epp_ready_endpoints", self.units),
                ("llm_d_epp_flow_control_queue_size", 0.0),
            ] {
                let line = format!("{metric}{{{labels}}} {value} {at_ms}");
                self.store.ingest_at(&line, at_ms, at_ms, spec.site);
            }
        }
    }

    /// Accept a topology without fabricating a fresh observation for its candidates.
    fn order(&mut self, sites: &[SiteSpec], at_ms: i64) -> RouteSnapshot {
        let validated = validate_serving_candidates(sites.iter().map(config).collect()).expect("serving candidates");
        RouteSnapshot::from_store(
            validated,
            Arc::from(sites.first().map_or("", |site| site.site)),
            &mut Inputs {
                signals: &self.store,
                now_ms: at_ms,
                window_ms: self.window_ms,
                horizon_ms: self.window_ms,
                availability: &self.availability,
                learned: &mut self.gauged.learned,
            },
        )
        .published(&mut self.gauged)
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

    /// Accepting topology before telemetry keeps capacity unknown until a sample arrives.
    #[test]
    fn topology_can_arrive_before_telemetry() {
        let sites = &EQUAL_CAPACITY[..1];
        let mut grid = Harness::new(WINDOW_MS);
        let empty = grid.order(sites, NOW_MS);
        let unmeasured = empty.candidates.first().expect("configured candidate");
        assert_eq!(
            unmeasured.capacity, None,
            "accepting topology cannot manufacture a sample"
        );

        grid.publish(sites, NOW_MS + 1, |site| site.capacity, 0.0);
        let measured = grid.order(sites, NOW_MS + 1);
        let candidate = measured.candidates.first().expect("configured candidate");
        assert_eq!(
            candidate.capacity,
            Some(80.0),
            "later telemetry teaches the configured pool"
        );
    }

    /// Telemetry delivery cannot introduce a candidate without accepted topology.
    #[test]
    fn telemetry_delivery_does_not_publish_topology() {
        let sites = &EQUAL_CAPACITY[..1];
        let mut grid = Harness::new(WINDOW_MS);
        grid.publish(sites, NOW_MS, |site| site.capacity, 0.0);
        assert!(
            grid.gauged.published.is_empty(),
            "delivery alone must not publish a candidate"
        );
        assert!(
            grid.order(&[], NOW_MS).candidates.is_empty(),
            "samples cannot invent topology"
        );

        let accepted = grid.order(sites, NOW_MS + 1);
        let candidate = accepted.candidates.first().expect("configured candidate");
        assert_eq!(
            candidate.capacity,
            Some(80.0),
            "legacy samples are usable after topology acceptance"
        );
    }

    /// Delayed samples cannot undo an accepted withdrawal.
    #[test]
    fn late_telemetry_cannot_restore_withdrawn_topology() {
        let sites = &EQUAL_CAPACITY[..1];
        let mut grid = Harness::new(WINDOW_MS);
        grid.refresh(sites, NOW_MS, |site| site.capacity, 0.0);
        assert!(
            grid.order(&[], NOW_MS + 1).candidates.is_empty(),
            "withdrawal must be accepted"
        );

        grid.publish(sites, NOW_MS + 2, |site| site.capacity, 0.0);
        assert!(
            grid.order(&[], NOW_MS + 2).candidates.is_empty(),
            "late telemetry cannot revive an excluded candidate"
        );
        assert!(
            grid.gauged.published.is_empty(),
            "late telemetry cannot publish a withdrawn pair"
        );
    }

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

    /// A site never seen queue-free still learns what it can carry.
    ///
    /// The soft bound needs a sample with nothing waiting. A site busy from the moment the
    /// gateway starts watching it never gives one, which is what a restart under load leaves
    /// behind: nothing learned, and no quiet moment coming. The hard bound covers it, because a
    /// backlog at the site's own limit says it cannot carry much more than that, and that is a
    /// usable denominator.
    #[test]
    fn a_site_never_seen_queue_free_still_learns_what_it_carries() {
        let observed = dispatch(
            &HETEROGENEOUS_CAPACITY,
            &Run {
                withheld: Withheld::Capacity,
                ..Run::fresh(0..DRAWS)
            },
        );
        capacity_tracking(&observed, UNIT_TOLERANCE).expect("a backlog teaches what a site carries");
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
        // Withholding the queue-free sample alone no longer withholds capacity, because a
        // backlog at the site's limit teaches the hard bound and that bound is capacity. Both
        // have to go.
        let unpublished = dispatch(
            &HETEROGENEOUS_CAPACITY,
            &Run {
                withheld: Withheld::Everything,
                ..Run::fresh(0..DRAWS)
            },
        );
        assert!(
            capacity_tracking(&unpublished, UNIT_TOLERANCE).is_err(),
            "selection passed with nothing on the wire, so the scenario does not read load"
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

    /// A site spent on long contexts, which its request count cannot see.
    ///
    /// Measured on dagobah: one GPU, `max_num_seqs` 128, a 49,424 token KV cache, and vLLM's
    /// own report of a maximum concurrency of 1.51x at a 32,768 token context. At a full
    /// context the engine holds one or two requests and its cache is spent, so a request count
    /// against a ceiling learned from short prompts reads near empty while the site is about
    /// to preempt. Prompt length decides which limit binds, and nothing in the routing path
    /// sees a prompt, so the only reading that is right in both regimes is the larger of the
    /// two.
    #[expect(clippy::too_many_lines, reason = "one sequence: short prompts, then long ones")]
    #[test]
    fn a_site_spent_on_long_contexts_reads_full_at_a_low_request_count() {
        let sites = EQUAL_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        // Short prompts first: the site carries its capacity queue-free and learns the ceiling.
        for nth in 0..=3_i64 {
            grid.refresh_with_saturation(
                &sites,
                NOW_MS + nth * REFRESH_MS,
                |spec| spec.capacity,
                0.0,
                |_| Some(0.3),
            );
        }
        // Then long prompts. Two requests in flight against a ceiling of 80, and a cache that
        // is nearly spent. The queue is still empty: the engine has not started refusing.
        let long = |spec: &SiteSpec| {
            if spec.site == "campus" {
                2.0
            } else {
                spec.capacity / 2.0
            }
        };
        let kv = |spec: &SiteSpec| Some(if spec.site == "campus" { 0.97 } else { 0.4 });
        let mut snapshot = grid.refresh_with_saturation(&sites, NOW_MS + 10 * REFRESH_MS, long, 0.0, kv);
        for nth in 11..=40_i64 {
            snapshot = grid.refresh_with_saturation(&sites, NOW_MS + nth * REFRESH_MS, long, 0.0, kv);
        }
        let of = |name: &str| {
            snapshot
                .candidates
                .iter()
                .find(|candidate| &*candidate.site == name)
                .and_then(|candidate| candidate.rho)
                .unwrap()
        };
        assert!(
            of("campus") > 0.9,
            "two requests in flight against a ceiling of 80 is a request ratio of 0.025, and \
             the site reads {:.3}; its cache is at 0.97",
            of("campus")
        );
        assert!(
            of("hub") < 0.6,
            "a site at the same request count on short prompts is not read as full: {:.3}",
            of("hub")
        );
        // It is ordered behind every peer, which is what selection acts on.
        let worst = snapshot
            .candidates
            .iter()
            .filter(|candidate| &*candidate.site != "campus")
            .filter_map(|candidate| candidate.rho)
            .fold(0.0_f64, f64::max);
        assert!(of("campus") > worst, "the spent site is the most saturated: {worst:.3}");
    }

    /// A spent cache is not a backlog, so nothing sheds for it.
    ///
    /// Fullness is read from the in-flight count against the learned ceiling, so a site holding
    /// two requests on full contexts is never full however little cache it has left. Saturation
    /// now reads the cache, and fullness does not, which is the second place the reading has to
    /// reach. Until it does, the latch that refuses a model when every site is out of room
    /// cannot fire on the one limit that binds at long contexts.
    #[test]
    #[ignore = "reproduces the fullness gap: `full` is in-flight against the ceiling, so a spent \
                 KV cache at a low request count never counts as full"]
    fn a_spent_cache_counts_as_full() {
        let sites = EQUAL_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        for nth in 0..=3_i64 {
            grid.refresh_with_saturation(
                &sites,
                NOW_MS + nth * REFRESH_MS,
                |spec| spec.capacity,
                0.0,
                |_| Some(0.3),
            );
        }
        // Every site spent, every queue shallow, every request count low.
        let mut snapshot = grid.refresh_with_saturation(&sites, NOW_MS + 10 * REFRESH_MS, |_| 2.0, 0.5, |_| Some(0.99));
        for nth in 11..=40_i64 {
            snapshot = grid.refresh_with_saturation(&sites, NOW_MS + nth * REFRESH_MS, |_| 2.0, 0.5, |_| Some(0.99));
        }
        let full = snapshot
            .candidates
            .iter()
            .filter(|candidate| candidate.full == Some(true))
            .count();
        assert_eq!(
            full,
            snapshot.candidates.len(),
            "every site has a spent cache and none counts as full"
        );
    }

    /// One proven site and one that knows nothing, both healthy and both queue-free.
    const PROVEN_AND_FRESH: [SiteSpec; 2] = [
        SiteSpec {
            site: "proven",
            capacity: 200.0,
            enforced: 200.0,
            in_flight: 200.0,
        },
        SiteSpec {
            site: "fresh",
            capacity: 200.0,
            enforced: 200.0,
            in_flight: 0.0,
        },
    ];

    /// What an unproven site converges to when it is only ever offered its weight's share.
    ///
    /// Open loop, the shape the draw produces: each round the fresh site holds its share of a
    /// fixed offered total, queue-free, and that becomes the load it publishes next round.
    fn converged_share(explore_floor: f64, rounds: i64) -> (f64, f64, f64) {
        let sites = PROVEN_AND_FRESH;
        let mut grid = Harness::with_explore_floor(WINDOW_MS, explore_floor);
        let offered = 200.0;
        let mut held = 0.0_f64;
        let mut share = 0.0_f64;
        let mut rho = 0.0_f64;
        for nth in 0..=rounds {
            let snapshot = grid.refresh(
                &sites,
                NOW_MS + nth * REFRESH_MS,
                |spec| if spec.site == "proven" { offered } else { held },
                0.0,
            );
            let of = |name: &str| {
                snapshot
                    .candidates
                    .iter()
                    .find(|candidate| &*candidate.site == name)
                    .map_or((0.0, f64::NAN), |candidate| {
                        (candidate.capacity.unwrap_or(0.0), candidate.rho.unwrap_or(f64::NAN))
                    })
            };
            let (weight_proven, _) = of("proven");
            let (weight_fresh, rho_fresh) = of("fresh");
            share = weight_fresh / (weight_proven + weight_fresh);
            rho = rho_fresh;
            held = offered * share;
        }
        (share, held, rho)
    }

    /// A full cache tells a limit from a burst at once, where a clock has to wait.
    ///
    /// A backlog is either the site's limit showing or a burst of arrivals, and only one of
    /// those should teach a bound. Time can tell them apart, which is why the spell has to
    /// last, but the site's own reading tells them apart immediately: a backlog at a cache
    /// that is nearly spent is a limit on the first sample. This is the whole benefit of
    /// reading the cache into that decision, so it is asserted inside the spell rather than
    /// after it.
    #[test]
    fn a_backlog_at_a_spent_cache_is_a_limit_before_the_spell_elapses() {
        let sites = EQUAL_CAPACITY;
        let spell = AvailabilitySettings::default().full_after_ms.max(MIN_BACKLOG_SPELL_MS);
        let within = spell / 4;
        let learned = |cache: f64| {
            let mut grid = Harness::new(WINDOW_MS);
            // Taught at capacity, queue-free, cache cool.
            grid.refresh_with_saturation(&sites, NOW_MS, |spec| spec.capacity, 0.0, |_| Some(0.2));
            // Then a quarter of the capacity with a queue, well inside the spell.
            let snapshot = grid.refresh_with_saturation(
                &sites,
                NOW_MS + within,
                |spec| spec.capacity / 4.0,
                4.0,
                |_| Some(cache),
            );
            snapshot.candidates[0].capacity.unwrap()
        };
        let taught = sites[0].capacity;
        assert!(
            (learned(0.2) - taught).abs() < f64::EPSILON,
            "a cool cache inside the spell teaches no bound, so the ceiling is what the \
             queue-free sample left: {} against {taught}",
            learned(0.2)
        );
        assert!(
            learned(0.95) < taught,
            "a spent cache inside the spell teaches the bound at once: {} against {taught}",
            learned(0.95)
        );
    }

    /// A site that grew is no longer held by what the smaller one queued at.
    ///
    /// The hard bound is an upper bound on capacity and it was learned about a site of a given
    /// size. A pool that gains units while its queue never empties has no queue-free sample
    /// large enough to retire that bound, and every later queued sample can only tighten it, so
    /// the larger pool keeps being ranked against what the smaller one could carry. The soft
    /// bound survives the same growth, because the most a smaller site carried is still a floor
    /// under what a larger one can.
    #[expect(clippy::too_many_lines, reason = "one sequence: taught, bounded, grown")]
    #[test]
    fn a_site_that_gained_units_is_not_held_by_the_bound_the_smaller_one_taught() {
        let sites = EQUAL_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        grid.set_units(2.0);

        // Two units carrying the site's capacity with nothing waiting teaches the soft bound.
        grid.refresh_with_saturation(&sites, NOW_MS, |spec| spec.capacity / 2.0, 0.0, |_| Some(0.3));

        // Then the cache fills and it queues at half that, which is a hard bound below the soft.
        let mut bounded = grid.refresh_with_saturation(&sites, NOW_MS, |spec| spec.capacity / 4.0, 2.0, |_| Some(0.95));
        for nth in 1..=6 {
            bounded = grid.refresh_with_saturation(
                &sites,
                NOW_MS + nth * REFRESH_MS,
                |spec| spec.capacity / 4.0,
                2.0,
                |_| Some(0.95),
            );
        }
        let small: Vec<f64> = bounded.candidates.iter().filter_map(|site| site.capacity).collect();
        assert!(
            small.iter().all(|against| *against < 60.0),
            "the smaller site queued at half what it carried, so a hard bound holds it: {small:?}"
        );

        // It doubles, stays busy, and its new pods make the cache cool, so no queue-free or
        // newly full sample arrives to retire or replace that bound. The old smaller-pool
        // instants remain in the horizon but must not immediately reteach it.
        grid.set_units(4.0);
        let mut grown = bounded;
        for nth in 7..=(7 + (WINDOW_MS / REFRESH_MS) + 4) {
            grown = grid.refresh_with_saturation(
                &sites,
                NOW_MS + nth * REFRESH_MS,
                |spec| spec.capacity / 4.0,
                2.0,
                |_| Some(0.2),
            );
        }
        let large: Vec<f64> = grown.candidates.iter().filter_map(|site| site.capacity).collect();
        assert!(
            large.iter().zip(&small).all(|(after, before)| after > before),
            "a site with twice the units is read against more than the bound the smaller one \
             taught: {large:?} against {small:?}"
        );
    }

    /// A gateway restart loses both learned bounds. A slot-bound site can remain queued with
    /// a cool cache from the first sample onward, so it must relearn a hard bound from the
    /// sustained backlog without first seeing a queue-free sample.
    #[test]
    fn a_restarted_gateway_relearns_a_slot_bound_site_under_load() {
        let sites = EQUAL_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        grid.availability.smoothing = 1.0;
        let first = grid.refresh_with_saturation(&sites, NOW_MS, |spec| spec.capacity, 2.0, |_| Some(0.2));
        assert!(
            first.candidates.iter().all(|site| site.rho == Some(0.2)),
            "before the backlog settles, the cache is the only admissible reading"
        );

        let spell = AvailabilitySettings::default().full_after_ms.max(MIN_BACKLOG_SPELL_MS);
        let settled = grid.refresh_with_saturation(&sites, NOW_MS + spell, |spec| spec.capacity, 2.0, |_| Some(0.2));
        assert!(
            settled.candidates.iter().all(|site| site.rho == Some(1.0)),
            "the second backlogged sample teaches a hard bound and makes the request count bind"
        );
        assert!(
            settled.candidates.iter().all(|site| site
                .capacity
                .is_some_and(|capacity| capacity > AvailabilitySettings::default().ceiling_floor)),
            "the busy spell relearns above the cold-start floor"
        );
    }

    /// Two congested samples are two observations, not one made of the worst of each.
    ///
    /// Both have a backlog, so filtering on that is not enough: the highest count among them
    /// pairs with the highest fullness among them, and a site that queued at sixty with a cool
    /// cache and at twenty with a spent one has sixty recorded as the limit a spent cache showed.
    /// That is the wrong direction, raising the denominator of a site already at a limit.
    #[test]
    fn the_count_a_limit_is_learned_from_comes_from_the_sample_that_showed_it() {
        let sites = EQUAL_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        let taught = grid.refresh_with_saturation(&sites, NOW_MS, |spec| spec.capacity, 0.0, |_| Some(0.3));
        let soft: Vec<f64> = taught.candidates.iter().filter_map(|site| site.capacity).collect();

        // Queueing at three quarters with a cool cache: a backlog, but nothing says it is a limit.
        grid.refresh_with_saturation(
            &sites,
            NOW_MS + REFRESH_MS,
            |spec| spec.capacity * 0.75,
            2.0,
            |_| Some(0.3),
        );
        // Then queueing at a quarter with the cache spent, which is a limit, at that count.
        let mut seen = taught;
        for nth in 2..=4 {
            seen = grid.refresh_with_saturation(
                &sites,
                NOW_MS + nth * REFRESH_MS,
                |spec| spec.capacity / 4.0,
                2.0,
                |_| Some(0.95),
            );
        }
        let against: Vec<f64> = seen.candidates.iter().filter_map(|site| site.capacity).collect();
        assert!(
            against.iter().all(|held| *held <= 25.0),
            "the spent sample held a quarter, so that is the bound it shows, not the three \
             quarters a cool one held: {against:?} against {soft:?}"
        );
    }

    /// A spent cache in one sample and a backlog in another are not one observation.
    ///
    /// Every series here is the worst in its window, and the two are maximised apart: the cache
    /// reading is the highest in the window, the congested count the highest at an instant that
    /// had a backlog. Nothing makes those the same instant. A site that was full and quiet, then
    /// quiet and queueing at a lower count, would otherwise have the lower count recorded as a
    /// hard bound for a state it was never in, and the cool-cache guard that exists to stop
    /// exactly that is bypassed by the older reading.
    #[test]
    fn a_spent_cache_and_a_later_backlog_are_not_one_congested_sample() {
        let sites = EQUAL_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        // Full cache, nothing waiting, carrying its capacity. This teaches the soft bound.
        let taught = grid.refresh_with_saturation(&sites, NOW_MS, |spec| spec.capacity, 0.0, |_| Some(0.95));
        let soft: Vec<f64> = taught.candidates.iter().filter_map(|site| site.capacity).collect();
        assert!(
            soft.iter().all(|weight| *weight >= 80.0),
            "a queue-free sample at capacity teaches the soft bound: {soft:?}"
        );

        // The cache drains and a burst queues at a much lower count, inside the same window.
        let mut later = taught;
        for nth in 1..=3 {
            later = grid.refresh_with_saturation(
                &sites,
                NOW_MS + nth * REFRESH_MS,
                |spec| spec.capacity / 4.0,
                2.0,
                |_| Some(0.2),
            );
        }
        let against: Vec<f64> = later.candidates.iter().filter_map(|site| site.capacity).collect();
        assert_eq!(
            against, soft,
            "the site was never both spent and queueing at a quarter of its load, so nothing \
             there is a capacity bound: {against:?} against {soft:?}"
        );
    }

    /// A standing queue at low load does not ratchet the ceiling down.
    ///
    /// A queue that never goes away is not a limit. A site taking arrivals in bursts queues at
    /// a quarter of what it has carried, with its cache cool, and a rule that reads any
    /// sustained backlog as a limit latches the bound at whatever the site held when the queue
    /// first appeared. Measured with such a rule: a site carrying eighty read a ceiling of
    /// twenty six, permanently, because clearing the bound needs a queue-free sample and a
    /// persistently busy site never gives one.
    ///
    /// The site's own reading separates the two. Below `REPORTED_FULL` it says it has room, so
    /// the only backlog still worth reading as a limit is one carried at or above the most it
    /// has ever carried queue-free.
    #[test]
    fn a_standing_queue_at_low_load_does_not_lower_the_ceiling() {
        let sites = EQUAL_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        let cool = |_: &SiteSpec| Some(0.3);
        grid.refresh_with_saturation(&sites, NOW_MS, |spec| spec.capacity, 0.0, cool);
        let taught = sites[0].capacity;
        let mut snapshot =
            grid.refresh_with_saturation(&sites, NOW_MS + REFRESH_MS, |spec| spec.capacity / 4.0, 4.0, cool);
        for nth in 2..=40_i64 {
            snapshot =
                grid.refresh_with_saturation(&sites, NOW_MS + nth * REFRESH_MS, |spec| spec.capacity / 4.0, 4.0, cool);
        }
        let ceiling = snapshot.candidates[0].capacity.unwrap();
        assert!(
            (ceiling - taught).abs() < f64::EPSILON,
            "a standing queue at a quarter of the load left the ceiling at {ceiling} against the \
             {taught} the site carried queue-free"
        );
    }

    /// Sustained pressure must affect routing without proving why capacity changed.
    ///
    /// Keep provider identity, ready units, held work and KV use unchanged. Only one
    /// site's queue changes. Its historic peak must not hide that pressure from dispatch.
    #[test]
    fn routing_counterexample_fresh_backlog_reduces_dispatch() {
        let mut observed = Vec::new();
        for count in [2, 3] {
            let sites = &EQUAL_CAPACITY[..count];
            let clean = sustained_pressure(sites, 0.0);
            let queued = sustained_pressure(sites, 2.0);
            assert_configured_sites(&clean, sites);
            assert_configured_sites(&queued, sites);
            let subject = sites.last().expect("a pressure subject").site;
            for candidate in &queued.candidates {
                assert_eq!(candidate.admission_state, AdmissionState::NewAndExisting);
                assert_eq!(candidate.backlog, Some(candidate.site.as_ref() == subject));
            }
            assert!(
                clean
                    .candidates
                    .iter()
                    .all(|candidate| candidate.backlog == Some(false))
            );
            observed.push((count, routed_to(&clean, subject), routed_to(&queued, subject)));
        }
        assert!(
            observed.iter().all(|(_, clean, queued)| queued < clean),
            "fresh sustained backlog must reduce dispatch without a topology change; \
             (sites, queue-free draws, backlogged draws): {observed:?}"
        );
    }

    /// Fresh queue and readiness data keep a usable pool participating when capacity is unknown.
    #[test]
    fn routing_counterexample_unknown_capacity_keeps_participating() {
        let mut observed = Vec::new();
        for count in [2, 3] {
            let sites = &EQUAL_CAPACITY[..count];
            let snapshot = partially_measured(sites);
            assert_configured_sites(&snapshot, sites);
            for candidate in &snapshot.candidates {
                assert_eq!(candidate.admission_state, AdmissionState::NewAndExisting);
                assert_eq!(candidate.backlog, Some(false), "every pool has fresh zero queue");
                if candidate.site.as_ref() == sites[0].site {
                    assert!(candidate.rho.is_some_and(|rho| rho < 1.0), "the measured pool has room");
                } else {
                    assert_eq!(candidate.capacity, None, "no running sample can teach capacity");
                    assert_eq!(candidate.rho, None, "unknown capacity is not measured room");
                    observed.push((count, candidate.site.to_string(), routed_to(&snapshot, &candidate.site)));
                }
            }
        }
        assert!(
            observed.iter().all(|(_, _, draws)| *draws > 0),
            "healthy pools with unknown capacity must receive traffic; (sites, pool, draws): {observed:?}"
        );
    }

    /// Missing concurrency cannot erase direct queue, held-work, or KV pressure.
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one matrix asserts signal state and dispatch together"
    )]
    fn partial_signals_preserve_pressure_without_inventing_capacity() {
        for count in [2, 3] {
            let sites = &EQUAL_CAPACITY[..count];
            for queue in [None, Some(0.0), Some(0.5), Some(1.0)] {
                for held in [None, Some(0.0), Some(1.0)] {
                    for kv in [None, Some(0.3), Some(1.0)] {
                        for ready in [None, Some(1.0)] {
                            let mut grid = Harness::new(WINDOW_MS);
                            publish_cool(&grid, &sites[..1], NOW_MS, 20.0, 0.0);
                            for spec in &sites[1..] {
                                publish_partial(&grid, spec, NOW_MS, queue, held, kv, ready);
                            }
                            let snapshot = grid.order(sites, NOW_MS);
                            let waiting =
                                queue.is_some_and(|value| value >= 1.0) || held.is_some_and(|value| value >= 1.0);
                            let backlog = if waiting { Some(true) } else { queue.map(|_| false) };
                            for candidate in snapshot.candidates.iter().filter(|c| c.site.as_ref() != sites[0].site) {
                                assert_eq!(candidate.capacity, None, "no concurrency sample teaches capacity");
                                assert_eq!(candidate.rho, kv, "direct KV survives missing concurrency");
                                assert_eq!(candidate.backlog, backlog, "queue={queue:?}, held={held:?}");
                                assert_eq!(candidate.full, None, "partial signals cannot authorize shedding");
                                assert_eq!(candidate.relieved, None);
                                let draws = routed_to(&snapshot, &candidate.site);
                                if waiting || kv == Some(1.0) || (queue.is_none() && kv.is_none()) {
                                    assert_eq!(draws, 0, "known pressure is avoided beside a clear pool");
                                } else {
                                    assert!(draws > 0, "unknown capacity must not prevent participation");
                                }
                            }
                            let availability = AvailabilitySettings {
                                shedding: true,
                                ..grid.availability
                            };
                            assert!(
                                snapshot
                                    .shed(&[Arc::from(MODEL)].into(), &availability)
                                    .shedding
                                    .is_empty()
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn fresh_kv_pressure_guides_selection_without_known_capacity() {
        for count in [2, 3] {
            let sites = &EQUAL_CAPACITY[..count];
            let mut grid = Harness::new(WINDOW_MS);
            for (site, kv) in sites.iter().zip([0.95, 0.2, 0.5]) {
                publish_partial(&grid, site, NOW_MS, Some(0.0), None, Some(kv), Some(1.0));
            }
            let mut snapshot = grid.order(sites, NOW_MS);
            let unknown_capacity = snapshot.candidates.iter().all(|candidate| candidate.capacity.is_none());
            assert!(unknown_capacity, "KV-only observations must not invent capacity");
            for _ in 0..2 {
                let low = routed_to(&snapshot, sites[1].site);
                if count == 2 {
                    assert!(
                        (59..=65).contains(&(low * 100 / DRAWS)),
                        "the lower-pressure site should receive about 62 percent, got {low}/{DRAWS}"
                    );
                } else {
                    let middle = routed_to(&snapshot, sites[2].site);
                    assert!(low > middle && middle > 0, "two choices must favor lower pressure");
                    assert_eq!(
                        routed_to(&snapshot, sites[0].site),
                        0,
                        "two choices avoid the highest pressure"
                    );
                }
                snapshot.candidates.reverse();
                snapshot.scores.reverse();
                snapshot.decisions.reverse();
            }
        }
    }

    /// Neither a telemetry gap nor a fresh recovery is interpreted as a learned limit.
    #[test]
    #[expect(clippy::too_many_lines, reason = "one trace covers pressure, expiry, and recovery")]
    fn partial_pressure_expires_and_recovers_without_teaching_a_ceiling() {
        let sites = &EQUAL_CAPACITY[..2];
        for (queue, kv) in [(2.0, 0.3), (0.0, 1.0)] {
            let mut grid = Harness::new(WINDOW_MS);
            publish_cool(&grid, &sites[..1], NOW_MS, 20.0, 0.0);
            publish_partial(&grid, &sites[1], NOW_MS, Some(queue), None, Some(kv), Some(1.0));
            let pressured = grid.order(sites, NOW_MS);
            assert_eq!(routed_to(&pressured, sites[1].site), 0);
            let expired_at = NOW_MS + WINDOW_MS + 1;
            publish_cool(&grid, &sites[..1], expired_at, 20.0, 0.0);
            let expired = grid.order(sites, expired_at);
            let subject = expired
                .candidates
                .iter()
                .find(|c| c.site.as_ref() == sites[1].site)
                .unwrap();
            assert_eq!((subject.capacity, subject.rho, subject.backlog), (None, None, None));
            assert_eq!(
                routed_to(&expired, sites[1].site),
                0,
                "entirely stale pressure remains a fallback"
            );
            publish_partial(&grid, &sites[1], expired_at, Some(0.0), Some(0.0), Some(0.3), Some(1.0));
            let recovered = grid.order(sites, expired_at);
            assert!(
                routed_to(&recovered, sites[1].site) > 0,
                "fresh clear signals restore participation"
            );
            let restored = recovered
                .candidates
                .iter()
                .find(|c| c.site.as_ref() == sites[1].site)
                .unwrap();
            assert_eq!(
                (restored.capacity, restored.rho, restored.backlog),
                (None, Some(0.3), Some(false))
            );
        }
    }

    /// Queue thresholds are configurable. One held request is independently pressure.
    #[test]
    fn partial_pressure_respects_the_configured_queue_threshold() {
        let sites = &EQUAL_CAPACITY[..2];
        for threshold in [0.5, 2.0, 10.0] {
            for (queue, held, blocked) in [(threshold / 2.0, 0.5, false), (threshold, 0.0, true), (0.0, 1.0, true)] {
                let mut grid = Harness::new(WINDOW_MS);
                grid.availability.queue_full = threshold;
                publish_cool(&grid, &sites[..1], NOW_MS, 20.0, 0.0);
                publish_partial(&grid, &sites[1], NOW_MS, Some(queue), Some(held), Some(0.3), Some(1.0));
                let snapshot = grid.order(sites, NOW_MS);
                let subject = snapshot
                    .candidates
                    .iter()
                    .find(|c| c.site.as_ref() == sites[1].site)
                    .unwrap();
                assert_eq!(
                    subject.backlog,
                    Some(blocked),
                    "threshold={threshold}, queue={queue}, held={held}"
                );
                assert_eq!(routed_to(&snapshot, sites[1].site) == 0, blocked);
            }
        }
    }

    /// Clearing recent pressure restores participation before the long queue window expires.
    #[test]
    fn a_partial_pool_rejoins_after_its_recent_queue_clears() {
        let sites = &EQUAL_CAPACITY[..2];
        let mut grid = Harness::new(WINDOW_MS);
        publish_cool(&grid, &sites[..1], NOW_MS, 20.0, 0.0);
        publish_partial(&grid, &sites[1], NOW_MS, Some(2.0), None, Some(0.3), Some(1.0));
        assert_eq!(routed_to(&grid.order(sites, NOW_MS), sites[1].site), 0);
        let settled_at = NOW_MS + grid.availability.room_after_ms + 1;
        publish_cool(&grid, &sites[..1], settled_at, 20.0, 0.0);
        publish_partial(&grid, &sites[1], settled_at, Some(0.0), None, Some(0.3), Some(1.0));
        let settled = grid.order(sites, settled_at);
        let (subject, score) = settled
            .candidates
            .iter()
            .zip(&settled.scores)
            .find(|(c, _)| c.site.as_ref() == sites[1].site)
            .unwrap();
        assert_eq!(subject.backlog, Some(false));
        assert!(
            (*score - 2.0).abs() < f64::EPSILON,
            "the long window still contains the old queue"
        );
        assert!(
            routed_to(&settled, sites[1].site) > 0,
            "recent pressure, not historic queue, gates mixed-capacity participation"
        );
    }

    /// A site out of sequence slots reads full, even with its cache barely touched.
    ///
    /// The slot cap is the one request-denominated limit the engine really has, and short
    /// prompts reach it while the cache stays low: a pool at `max_num_seqs` on 512-token
    /// prompts holds its slots full at a third of its blocks. The site's own reading cannot
    /// see that, because the reading is about blocks. Its count can, which is the whole reason
    /// the count is a limit at all, so the backlog has to be read as evidence of a limit
    /// whether or not the cache agrees.
    #[test]
    fn a_site_out_of_slots_reads_full_with_a_cold_cache() {
        let sites = EQUAL_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        // Taught at capacity, queue-free, cache cool throughout.
        let cool = |_: &SiteSpec| Some(0.3);
        grid.refresh_with_saturation(&sites, NOW_MS, |spec| spec.capacity, 0.0, cool);
        // Then the slots fill and a queue builds, and stays, while the cache stays at 0.3.
        let mut snapshot = grid.refresh_with_saturation(&sites, NOW_MS + REFRESH_MS, |spec| spec.capacity, 4.0, cool);
        for nth in 2..=20_i64 {
            snapshot = grid.refresh_with_saturation(&sites, NOW_MS + nth * REFRESH_MS, |spec| spec.capacity, 4.0, cool);
        }
        let worst = snapshot
            .candidates
            .iter()
            .filter_map(|candidate| candidate.rho)
            .fold(0.0_f64, f64::max);
        assert!(
            worst > 0.9,
            "every slot is full and work is queued, and the site reads {worst:.3} because its \
             cache sits at 0.30"
        );
    }
    /// One row of the timeline bench: its name, how its cache and load move with the tick, and
    /// how much it queues throughout.
    type Regime = (&'static str, fn(i64) -> f64, fn(i64) -> f64, f64);

    /// A timeline bench over the real store, snapshot and draw: which limit binds, and when.
    ///
    /// Ignored because it measures rather than asserts. Run it with
    /// `cargo test -p ai-grid-filters --lib timeline_bench -- --ignored --nocapture`.
    #[test]
    #[ignore = "a bench, not an invariant: prints which limit bound in each regime"]
    #[expect(clippy::too_many_lines, reason = "one table, one row per regime")]
    #[expect(clippy::print_stdout, reason = "a bench reports its table on stdout")]
    fn timeline_bench() {
        let regimes: [Regime; 4] = [
            (
                "cache ramps, load flat",
                |t| 0.2 + 0.78 * (t as f64 / 60.0).min(1.0),
                |_| 0.25,
                0.0,
            ),
            (
                "load ramps, cache flat",
                |_| 0.3,
                |t| 0.25 + 0.75 * (t as f64 / 60.0).min(1.0),
                0.0,
            ),
            (
                "both ramp",
                |t| 0.2 + 0.78 * (t as f64 / 60.0).min(1.0),
                |t| 0.25 + 0.75 * (t as f64 / 60.0).min(1.0),
                0.0,
            ),
            (
                "load ramps with a queue",
                |_| 0.3,
                |t| 0.25 + 0.75 * (t as f64 / 60.0).min(1.0),
                4.0,
            ),
        ];
        println!("\n  regime                      | rho   | reported | counted | ceiling | hard  | binding");
        println!("  ----------------------------|-------|----------|---------|---------|-------|--------");
        for (name, cache, load, queued) in regimes {
            let sites = EQUAL_CAPACITY;
            let mut grid = Harness::new(WINDOW_MS);
            // Teach a ceiling first, queue-free at capacity with a cool cache.
            grid.refresh_with_saturation(&sites, NOW_MS, |spec| spec.capacity, 0.0, |_| Some(0.2));
            let mut snapshot =
                grid.refresh_with_saturation(&sites, NOW_MS + REFRESH_MS, |spec| spec.capacity, 0.0, |_| Some(0.2));
            for tick in 1..=90_i64 {
                let at = NOW_MS + (tick + 1) * REFRESH_MS;
                snapshot = grid.refresh_with_saturation(
                    &sites,
                    at,
                    |spec| spec.capacity * load(tick),
                    queued,
                    |_| Some(cache(tick)),
                );
            }
            let c = &snapshot.candidates[0];
            let rho = c.rho.unwrap_or(f64::NAN);
            let reported = cache(90).min(1.0);
            let ceiling = c.capacity.unwrap_or(f64::NAN);
            let counted = (sites[0].capacity * load(90) / ceiling).min(1.0);
            let binding = if reported >= counted { "reported" } else { "counted" };
            println!(
                "  {name:27} | {rho:<5.3} | {reported:<8.3} | {counted:<7.3} | {ceiling:<7.1} | {:<5} | {binding}",
                c.capacity.map_or_else(|| "-".to_owned(), |_| format!("{ceiling:.0}"))
            );
        }
        println!();
    }

    /// A site that has never been pushed is not read as full.
    ///
    /// Its ceiling is a lower bound on capacity, being the most it has been seen to carry, so
    /// its count over that ceiling is an upper bound on saturation rather than a reading of it.
    /// A site only ever handed twenty requests reads a count of 1.0 whatever it could serve,
    /// and taking the larger of a reading and an upper bound gives the upper bound. The count
    /// is therefore admitted beside the site's own reading only once a backlog has shown where
    /// the limit is.
    ///
    /// Measured before the rule: a site carrying twenty queue-free, reporting its cache at
    /// 0.10, read 1.000 saturated.
    #[test]
    fn a_site_that_has_never_been_pushed_is_not_read_as_full() {
        let sites = EQUAL_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        let reported = 0.1;
        let mut snapshot = grid.refresh_with_saturation(&sites, NOW_MS, |_| 20.0, 0.0, |_| Some(reported));
        for nth in 1..=30_i64 {
            snapshot =
                grid.refresh_with_saturation(&sites, NOW_MS + nth * REFRESH_MS, |_| 20.0, 0.0, |_| Some(reported));
        }
        let worst = snapshot
            .candidates
            .iter()
            .filter_map(|candidate| candidate.rho)
            .fold(0.0_f64, f64::max);
        assert!(
            (worst - reported).abs() < 0.05,
            "every site carries twenty queue-free and reports {reported:.2}, and reads \
             {worst:.3}: its own reading is the only limit anything has shown"
        );
    }

    #[expect(clippy::too_many_lines, reason = "one sequence: taught, shrunk, recovered")]
    #[test]
    fn a_shrunken_site_is_measured_against_what_it_queues_at_now() {
        let sites = EQUAL_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        let taught = grid.refresh(&sites, NOW_MS, |spec| spec.capacity, 0.0);
        let soft: Vec<f64> = taught.candidates.iter().filter_map(|site| site.capacity).collect();
        assert!(
            soft.iter().all(|weight| *weight >= 80.0),
            "taught its capacity: {soft:?}"
        );
        // It shrinks: it queues at a quarter of what it used to carry, and keeps doing so,
        // which is what a quartered pool under the same load looks like.
        let quarter = |spec: &SiteSpec| spec.capacity / 4.0;
        let mut shrunk = taught;
        for nth in 1..=12 {
            shrunk = grid.refresh(&sites, NOW_MS + nth * REFRESH_MS, quarter, 2.0);
        }
        let under_backlog: Vec<f64> = shrunk.candidates.iter().filter_map(|site| site.capacity).collect();
        assert!(
            under_backlog.iter().all(|weight| *weight <= 25.0),
            "a site queueing at a quarter of what it carried is measured against the quarter: \
             {under_backlog:?} against {soft:?}"
        );
        let worst = shrunk
            .candidates
            .iter()
            .filter_map(|site| site.rho)
            .fold(0.0_f64, f64::max);
        assert!(
            worst > 0.5,
            "and reads saturated rather than quarter loaded: {worst:.2}"
        );
        // It grows back. The backlog has to leave the reading window first, because every
        // series here is the worst in its window, so recovery costs one window.
        let mut grown = shrunk;
        for nth in 12..=(12 + (WINDOW_MS / REFRESH_MS) + 2) {
            grown = grid.refresh(&sites, NOW_MS + nth * REFRESH_MS, |spec| spec.capacity, 0.0);
        }
        let recovered: Vec<f64> = grown.candidates.iter().filter_map(|site| site.capacity).collect();
        assert_eq!(
            recovered, soft,
            "one queue-free sample at the old level hands the denominator back: {recovered:?} \
             against {soft:?}"
        );
    }

    /// A site that loses serving units has its learned bounds retired.
    ///
    /// The bounds describe a site of a given size, and a site that lost three quarters of its
    /// units is not that site. Nothing it publishes says how much smaller it got, but the unit
    /// count says that it did, and that is enough: drop what was learned about the old shape
    /// and learn the new one from the next queue-free sample.
    ///
    /// Sustained, because the ready-unit count is a count of fresh scrapes rather than of ready
    /// pods, so it dips whenever a scrape is late. A dip that clears inside the window the load
    /// is read over is a late collector, not a smaller site.
    #[test]
    fn a_site_that_loses_units_retires_what_it_learned_about_the_old_one() {
        let sites = EQUAL_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        grid.set_units(4.0);
        let cool = |_: &SiteSpec| Some(0.3);
        // Four units each carrying a quarter of the site's capacity, with nothing waiting.
        let per_unit = |spec: &SiteSpec| spec.capacity / 4.0;
        let taught = grid.refresh_with_saturation(&sites, NOW_MS, per_unit, 0.0, cool);
        let soft: Vec<f64> = taught.candidates.iter().filter_map(|site| site.capacity).collect();
        assert!(
            soft.iter().all(|weight| *weight >= 80.0),
            "four units taught the site total: {soft:?}"
        );

        // Three units go. The one left carries what one unit carried, and says nothing about
        // being smaller: its cache is as cool as it ever was.
        grid.set_units(1.0);
        let mut shrunk = taught;
        for nth in 1..=((WINDOW_MS / REFRESH_MS) + 6) {
            shrunk = grid.refresh_with_saturation(&sites, NOW_MS + nth * REFRESH_MS, per_unit, 0.0, cool);
        }
        let against: Vec<f64> = shrunk.candidates.iter().filter_map(|site| site.capacity).collect();
        assert!(
            against.iter().all(|weight| *weight <= 30.0),
            "a site down to a quarter of its units is measured against what a quarter carries, \
             not against what four did: {against:?} against {soft:?}"
        );
    }

    /// A unit count that dips and comes back is a late scrape, not a smaller site.
    ///
    /// Measured on a busy site, because that is where retiring early costs something. With a
    /// backlog there is no queue-free sample to relearn from, so bounds dropped on a dip leave
    /// the site read against the floor until its load lets up.
    #[test]
    fn a_unit_count_that_recovers_inside_the_window_keeps_what_was_learned() {
        let sites = EQUAL_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        grid.set_units(4.0);
        let cool = |_: &SiteSpec| Some(0.3);
        let per_unit = |spec: &SiteSpec| spec.capacity / 4.0;
        let taught = grid.refresh_with_saturation(&sites, NOW_MS, per_unit, 0.0, cool);
        let soft: Vec<f64> = taught.candidates.iter().filter_map(|site| site.capacity).collect();

        // The site stays busy throughout, so nothing re-teaches a bound that is dropped.
        grid.set_units(1.0);
        let dip = (WINDOW_MS / REFRESH_MS) - 4;
        let mut snapshot = taught;
        for nth in 1..=dip {
            snapshot = grid.refresh_with_saturation(&sites, NOW_MS + nth * REFRESH_MS, per_unit, 2.0, cool);
        }
        grid.set_units(4.0);
        for nth in dip + 1..=dip + 6 {
            snapshot = grid.refresh_with_saturation(&sites, NOW_MS + nth * REFRESH_MS, per_unit, 2.0, cool);
        }
        let kept: Vec<f64> = snapshot.candidates.iter().filter_map(|site| site.capacity).collect();
        assert_eq!(
            kept, soft,
            "a dip shorter than the window the load is read over is a late collector: \
             {kept:?} against {soft:?}"
        );
    }

    /// The same shrink, reported by the site rather than counted by the gateway.
    ///
    /// Pods go and their blocks go with them, so the work that fit before now fills what is
    /// left. The site says so itself, and the ceiling comes down on that reading alone. The
    /// sibling above reaches the same place from a count against a learned ceiling, with no
    /// reading from the site at all, so the two signals are tested to agree rather than one
    /// being assumed to stand in for the other.
    #[test]
    fn a_shrunken_site_that_reports_a_spent_cache_is_measured_against_what_is_left() {
        let sites = EQUAL_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        let cool = |_: &SiteSpec| Some(0.3);
        let taught = grid.refresh_with_saturation(&sites, NOW_MS, |spec| spec.capacity, 0.0, cool);
        let soft: Vec<f64> = taught.candidates.iter().filter_map(|site| site.capacity).collect();
        assert!(
            soft.iter().all(|weight| *weight >= 80.0),
            "taught its capacity: {soft:?}"
        );

        // A quarter of the pods left, holding a quarter of the work, and the blocks that went
        // with them mean that quarter fills what remains.
        let quarter = |spec: &SiteSpec| spec.capacity / 4.0;
        let spent = |_: &SiteSpec| Some(1.0);
        let mut shrunk = taught;
        for nth in 1..=12 {
            shrunk = grid.refresh_with_saturation(&sites, NOW_MS + nth * REFRESH_MS, quarter, 2.0, spent);
        }
        let against: Vec<f64> = shrunk.candidates.iter().filter_map(|site| site.capacity).collect();
        assert!(
            against.iter().all(|weight| *weight <= 25.0),
            "a site reporting a spent cache at a quarter of what it carried is measured against \
             the quarter: {against:?} against {soft:?}"
        );
        let worst = shrunk
            .candidates
            .iter()
            .filter_map(|site| site.rho)
            .fold(0.0_f64, f64::max);
        assert!(
            worst > 0.5,
            "and reads saturated rather than quarter loaded: {worst:.2}"
        );
    }

    /// A site whose slots shrink without its blocks is read against a stale ceiling.
    ///
    /// Known gap, and not fixable from these signals. Lowering `max_num_seqs` on a rollout
    /// leaves the cache cool, so nothing the site publishes says it got smaller, and a count
    /// against the old ceiling reads it as emptier than it is. Losing pods is not this case:
    /// blocks go with them, the cache fills, and the reported arm catches it at once.
    ///
    /// The backlog cannot carry the evidence either, because
    /// `a_standing_queue_at_low_load_does_not_lower_the_ceiling` is the same sequence of
    /// readings with the opposite answer, and it holds for longer. Duration cannot separate a
    /// bursty site from a throttled one, and of the two mistakes only one is recoverable: a
    /// ceiling left too high is cleared by the next queue-free sample, while one latched too
    /// low needs a queue-free sample a busy site never gives.
    ///
    /// What would close it is the engine publishing its slot capacity. Running requests over
    /// `max_num_seqs` is the same shape as cache utilization, and a site that is out of slots
    /// would report it the way a site out of blocks already does, with nothing learned.
    #[ignore = "needs a published slot capacity; see the doc comment"]
    #[test]
    fn a_shrunken_site_that_reports_a_cool_cache_is_measured_against_what_it_queues_at_now() {
        let sites = EQUAL_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        // A quarter of its slots, against a cache with room to spare: the regime where the
        // cache reading is not the limit and the count is all there is.
        let cool = |_: &SiteSpec| Some(0.3);
        let taught = grid.refresh_with_saturation(&sites, NOW_MS, |spec| spec.capacity, 0.0, cool);
        let soft: Vec<f64> = taught.candidates.iter().filter_map(|site| site.capacity).collect();
        assert!(
            soft.iter().all(|weight| *weight >= 80.0),
            "taught its capacity: {soft:?}"
        );

        let quarter = |spec: &SiteSpec| spec.capacity / 4.0;
        let settles = (WINDOW_MS / REFRESH_MS) + 8;
        let mut shrunk = taught;
        for nth in 1..=settles {
            shrunk = grid.refresh_with_saturation(&sites, NOW_MS + nth * REFRESH_MS, quarter, 2.0, cool);
        }
        let under_backlog: Vec<f64> = shrunk.candidates.iter().filter_map(|site| site.capacity).collect();
        assert!(
            under_backlog.iter().all(|weight| *weight <= 25.0),
            "a site queueing at a quarter of what it carried is measured against the quarter, \
             whether or not it also reports a cool cache: {under_backlog:?} against {soft:?}"
        );
    }

    #[expect(clippy::too_many_lines, reason = "one sequence: taught, reset, recovered")]
    #[test]
    fn a_reset_under_load_recovers_within_the_debounce() {
        let sites = HETEROGENEOUS_CAPACITY;
        let mut grid = Harness::new(WINDOW_MS);
        // Before the reset: each site has carried its capacity queue-free, so each knows it.
        let taught = grid.refresh(&sites, NOW_MS, |spec| spec.capacity, 0.0);
        let before: Vec<f64> = taught.candidates.iter().filter_map(|site| site.capacity).collect();
        let declared: Vec<f64> = sites.iter().map(|spec| spec.capacity).collect();
        assert_eq!(before.len(), declared.len());
        // The reset: a fresh gateway, nothing learned, and a grid that is busy. Every sample
        // from here on carries a queue, so nothing can teach a soft bound.
        let mut reset = Harness::new(WINDOW_MS);
        let busy = |spec: &SiteSpec| spec.capacity;
        let first = reset.refresh(&sites, NOW_MS, busy, 2.0);
        let floor = AvailabilitySettings::default().ceiling_floor;
        for site in &first.candidates {
            assert_eq!(
                site.capacity,
                Some(floor),
                "the first busy refresh after a reset knows nothing: {}",
                site.site
            );
        }
        // The debounce passes, and the backlog is still there.
        let spell = AvailabilitySettings::default().full_after_ms.max(MIN_BACKLOG_SPELL_MS);
        let mut recovered = first;
        for nth in 1..=((spell / REFRESH_MS) + 2) {
            recovered = reset.refresh(&sites, NOW_MS + nth * REFRESH_MS, busy, 2.0);
        }
        let after: Vec<f64> = recovered.candidates.iter().filter_map(|site| site.capacity).collect();
        assert_eq!(
            after, before,
            "a settled backlog recovered what the queue-free samples had taught: {after:?} \
             against {before:?}"
        );
        // And the recovery is not a flat value: the sites are ordered by what they carry
        // again. The spread itself cannot prove much, because `explore_floor` lifts every
        // measured weight to a quarter of the largest and so bounds any spread at four to one
        // whatever the capacities are.
        let ordered = after.windows(2).all(|pair| pair[0] < pair[1]);
        assert!(ordered, "the weights follow the capacities: {after:?} for {declared:?}");
    }

    /// Resetting the soft bound on a clock is the removed descent in another shape.
    ///
    /// A reset under load is cheap, because a settled backlog teaches the hard bound within
    /// the debounce. A reset at steady partial load is not, and that is the case a periodic
    /// reset would mostly land in: the site is queue-free, so there is no backlog to teach a
    /// hard bound, and the soft bound relearns from the only sample available, which is
    /// whatever the site happens to be carrying. The gateway then reads its own offered load
    /// back as the site's capacity.
    ///
    /// Measured at a steady quarter load over two hours: never resetting holds the ceiling at
    /// 80 and saturation at 0.25, while resetting every 5, 15 or 60 minutes collapses it to 20
    /// and saturation to 1.0. The period does not matter, because the relearn is immediate and
    /// wrong rather than slow and wrong.
    ///
    /// The soft bound is taught by offered load, and selection controls offered load. Clearing
    /// it on a clock closes that loop. A published identity change is a different thing: it is
    /// evidence the old number is invalid rather than a guess that it might be stale.
    #[test]
    fn resetting_the_soft_bound_on_a_clock_reads_offered_load_as_capacity() {
        let sites = EQUAL_CAPACITY;
        let quarter = |spec: &SiteSpec| spec.capacity / 4.0;
        let worst_rho = |period_min: i64| {
            let mut grid = Harness::new(WINDOW_MS);
            grid.refresh(&sites, NOW_MS, |spec| spec.capacity, 0.0);
            let mut snap = grid.refresh(&sites, NOW_MS + 1_000, quarter, 0.0);
            for minute in 1..=120_i64 {
                if period_min > 0 && minute % period_min == 0 {
                    grid.reset_soft();
                }
                snap = grid.refresh(&sites, NOW_MS + minute * 60_000, quarter, 0.0);
            }
            snap.candidates
                .iter()
                .filter_map(|candidate| candidate.rho)
                .fold(0.0_f64, f64::max)
        };
        let never = worst_rho(0);
        assert!(
            never < 0.3,
            "a quarter load reads a quarter saturated when nothing resets: {never:.3}"
        );
        for period_min in [5_i64, 15, 60] {
            let reset = worst_rho(period_min);
            assert!(
                reset > 0.9,
                "a reset every {period_min} minutes should read the offered load back as \
                 capacity, and read {reset:.3}"
            );
        }
    }

    #[test]
    #[ignore = "reproduces the unproven-site fixed point: weight is what a site has carried and \
                 load is proportional to weight, so a fresh site converges on explore_floor's share"]
    fn an_unproven_site_discovers_what_it_can_carry() {
        let (share, held, rho) = converged_share(AvailabilitySettings::default().explore_floor, 40);
        assert!(
            held > 150.0,
            "a site that can serve 200 converged holding {held:.1} at a {share:.3} share, \
             reading {rho:.3} saturated with nothing queued"
        );
    }

    /// Raising the explore floor moves the fixed point rather than removing it.
    #[test]
    #[ignore = "reproduces the same fixed point at every explore_floor setting"]
    fn a_higher_explore_floor_lets_an_unproven_site_finish_proving_itself() {
        let (_, low, _) = converged_share(0.25, 40);
        let (_, high, _) = converged_share(0.75, 40);
        assert!(
            high > 150.0 && low > 150.0,
            "tripling the explore floor took the converged load from {low:.1} to {high:.1}, \
             against a true 200, so the floor sets the fixed point rather than escaping it"
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

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// Prevent missing pools or reordered hash slots from satisfying a routing assertion.
    fn assert_configured_sites(snapshot: &RouteSnapshot, sites: &[SiteSpec]) {
        let actual: Vec<&str> = snapshot
            .candidates
            .iter()
            .map(|candidate| candidate.site.as_ref())
            .collect();
        let expected: Vec<&str> = sites.iter().map(|spec| spec.site).collect();
        assert_eq!(
            actual, expected,
            "every configured pool must survive in the same candidate order"
        );
    }

    /// Keep every pool freshly ready and queue-free, but measure capacity only for the first.
    fn partially_measured(sites: &[SiteSpec]) -> RouteSnapshot {
        let mut grid = Harness::new(WINDOW_MS);
        publish_cool(&grid, &sites[..1], NOW_MS, sites[0].capacity, 0.0);
        let mut snapshot = grid.order(sites, NOW_MS);
        for nth in 1..=(WINDOW_MS / REFRESH_MS + SETTLE) {
            let at_ms = NOW_MS + nth * REFRESH_MS;
            publish_cool(&grid, &sites[..1], at_ms, 20.0, 0.0);
            for spec in &sites[1..] {
                let labels = format!(r#"grid_site="{}",grid_provider="pool-{}""#, spec.site, spec.site);
                for (metric, value) in [
                    ("llm_d_epp_average_queue_size", 0.0),
                    ("llm_d_epp_ready_endpoints", 1.0),
                ] {
                    let line = format!("{metric}{{{labels}}} {value} {at_ms}");
                    grid.store.ingest_at(&line, at_ms, at_ms, spec.site);
                }
            }
            snapshot = grid.order(sites, at_ms);
        }
        snapshot
    }

    /// Attribute real selector decisions to a pool rather than inspecting its weight alone.
    fn routed_to(snapshot: &RouteSnapshot, site: &str) -> usize {
        (0..DRAWS)
            .filter(|turn| {
                select_spread(
                    snapshot,
                    CapabilityKind::InferenceModel,
                    MODEL,
                    *turn,
                    |_| true,
                    &KeepAll,
                )
                .expect("an admitted pool takes the request")
                .candidate
                .site
                .as_ref()
                    == site
            })
            .count()
    }

    /// Publish raw observations with low KV use, independently of topology refresh.
    fn publish_cool(grid: &Harness, sites: &[SiteSpec], at_ms: i64, held: f64, queued: f64) {
        grid.publish(sites, at_ms, |_| held, queued);
        for spec in sites {
            let labels = format!(r#"grid_site="{}",grid_provider="pool-{}""#, spec.site, spec.site);
            let line = format!("llm_d_epp_average_kv_cache_utilization{{{labels}}} 0.3 {at_ms}");
            grid.store.ingest_at(&line, at_ms, at_ms, spec.site);
        }
    }

    /// Publish independent pressure signals, deliberately omitting running concurrency.
    #[expect(clippy::too_many_arguments, reason = "each raw signal can be independently absent")]
    fn publish_partial(
        grid: &Harness,
        spec: &SiteSpec,
        at_ms: i64,
        queue: Option<f64>,
        held: Option<f64>,
        kv: Option<f64>,
        ready: Option<f64>,
    ) {
        let labels = format!(r#"grid_site="{}",grid_provider="pool-{}""#, spec.site, spec.site);
        for (metric, value) in [
            ("llm_d_epp_average_queue_size", queue),
            ("llm_d_epp_flow_control_queue_size", held),
            ("llm_d_epp_average_kv_cache_utilization", kv),
            ("llm_d_epp_ready_endpoints", ready),
        ] {
            if let Some(value) = value {
                grid.store.ingest_at(
                    &format!("{metric}{{{labels}}} {value} {at_ms}"),
                    at_ms,
                    at_ms,
                    spec.site,
                );
            }
        }
    }

    /// Vary the last pool's queue without changing topology, held work or candidate order.
    fn sustained_pressure(sites: &[SiteSpec], queued: f64) -> RouteSnapshot {
        let mut grid = Harness::new(WINDOW_MS);
        publish_cool(&grid, sites, NOW_MS, sites[0].capacity, 0.0);
        let mut snapshot = grid.order(sites, NOW_MS);
        assert!(
            snapshot
                .candidates
                .iter()
                .all(|candidate| candidate.capacity == Some(80.0)),
            "the fixture must teach the same historic peak to every pool"
        );
        for nth in 1..=(WINDOW_MS / REFRESH_MS + SETTLE) {
            let at_ms = NOW_MS + nth * REFRESH_MS;
            let last = sites.len() - 1;
            publish_cool(&grid, &sites[..last], at_ms, 20.0, 0.0);
            publish_cool(&grid, &sites[last..], at_ms, 20.0, queued);
            snapshot = grid.order(sites, at_ms);
        }
        snapshot
    }
}
