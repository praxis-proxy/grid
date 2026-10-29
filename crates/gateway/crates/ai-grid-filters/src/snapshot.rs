//! The route snapshot the request path reads, and how live load orders it.
//!
//! A snapshot is a pre-ordered candidate list plus the local site. The request
//! path loads one snapshot and takes the front admitted match, doing no store
//! read or scoring itself. Ordering by live load is a control step
//! ([`RouteSnapshot::from_store`]) run off the request path, so the hot path
//! snapshots resolved order once rather than reading raw signals per request.

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    sync::Arc,
};

use grid_signals::LoadStore;

use crate::{
    decisions::SiteDecisions,
    descriptor::{AdmissionState, CapabilityKind, RouteCandidate},
    serving::AvailabilitySettings,
    signals::{Over, SiteReading, SiteSignals},
};

/// The reading at which a site is treated as having little room left, matching the threshold
/// the EPP's own utilization detector uses over KV.
///
/// A routing threshold, not an engine limit: vLLM runs its cache to about 1.0 and preempts
/// rather than refusing, so this says prefer elsewhere rather than out of room.
const REPORTED_FULL: f64 = 0.8;

/// Minimum duration required before concluding any backlog-based limit, even when the site
/// reports saturation. A sample marked full through `full_in_flight` bypasses this spell.
pub(crate) const MIN_BACKLOG_SPELL_MS: i64 = 5_000;

/// Score of a candidate whose cluster has no healthy endpoint: after every healthy one,
/// unmeasured included, since `total_cmp` orders a positive NaN above infinity.
const UNHEALTHY: f64 = f64::NAN.abs();

/// A resolved, pre-ordered candidate list read atomically per request.
#[derive(Debug)]
pub struct RouteSnapshot {
    /// Candidates in priority order. `select_admitted` takes the front match.
    pub candidates: Vec<RouteCandidate>,

    /// This gateway's own site identifier.
    pub local_site: Arc<str>,

    /// Each candidate's score, parallel to `candidates`; equal scores share traffic.
    pub scores: Vec<f64>,

    /// Each candidate's site decision counters, parallel to `candidates`.
    pub decisions: Vec<SiteDecisions>,

    /// Models answering 503 because every healthy site serving them is past full.
    pub shedding: BTreeSet<Arc<str>>,

    /// Verified provider-hop clusters from the same serving revision as the candidates.
    pub provider_hop_clusters: Arc<BTreeSet<String>>,
}

/// Per model while shedding is decided: whether every site is measured, and whether each
/// site is full and whether it has room.
type Fullness = (bool, Vec<(bool, bool)>);

/// What ordering a snapshot reads: the store, the clock, the window, the availability tuning, and
/// the state learned across refreshes.
pub(crate) struct Inputs<'store, S: SiteSignals = LoadStore> {
    /// What each site publishes, read once per candidate.
    pub(crate) signals: &'store S,
    /// Now, milliseconds.
    pub(crate) now_ms: i64,
    /// Freshness window, milliseconds.
    pub(crate) window_ms: i64,
    /// Worst-of horizon behind a site's newest sample, milliseconds.
    pub(crate) horizon_ms: i64,
    /// Availability tuning, from the filter block.
    pub(crate) availability: &'store AvailabilitySettings,
    /// Learned ceilings and smoothed saturation, kept across refreshes.
    pub(crate) learned: &'store mut Learned,
}

impl RouteSnapshot {
    /// Wrap candidates in their given order, without consulting live load.
    ///
    /// The order is whatever the caller supplies (config order). Used before
    /// any signals exist and as the cold-start fallback.
    pub fn from_static(candidates: Vec<RouteCandidate>, local_site: Arc<str>) -> Self {
        let scores = vec![f64::INFINITY; candidates.len()];
        Self {
            decisions: decisions_for(&candidates),
            candidates,
            local_site,
            scores,
            shedding: BTreeSet::new(),
            provider_hop_clusters: Arc::default(),
        }
    }

    /// Whether `model` is still shed after expiring candidates are removed at request time.
    pub(crate) fn is_shedding(&self, model: &str, now_ms: i64) -> bool {
        self.shedding.contains(model)
            && self.candidates.iter().zip(&self.scores).any(|(candidate, score)| {
                candidate.name.as_ref() == model && !candidate.expired_at(now_ms) && sheds_on(candidate, *score)
            })
    }

    /// Order candidates least-loaded-first from the live store, then wrap them.
    ///
    /// For each candidate the store is read at its `site/cluster` key over the
    /// last `window_ms`. A candidate with no fresh sample sorts after every
    /// candidate that has one, so a measured-healthy site is preferred over an
    /// unmeasured one; among equals the caller's order is preserved (stable
    /// sort), which keeps cold start deterministic. `select_admitted` over the
    /// result then picks the least-loaded admitted site per capability.
    pub(crate) fn from_store<S: SiteSignals>(
        candidates: Vec<RouteCandidate>,
        local_site: Arc<str>,
        inputs: &mut Inputs<'_, S>,
    ) -> Self {
        // Score each candidate once, then sort the pairs: load_of allocates a
        // store key and scans a window, too costly to repeat inside sort_by.
        let mut reset_pairs = BTreeSet::new();
        let ranked = candidates
            .into_iter()
            .map(|candidate| {
                let local = *candidate.site == *local_site;
                Self::resolve(candidate, local, inputs, &mut reset_pairs)
            })
            .collect();
        Self::ranked(
            Self::explore_floor(ranked, inputs.availability.explore_floor),
            local_site,
        )
    }

    /// One candidate with its load score and every signal selection reads, plus whether its
    /// capacity and saturation were measured.
    fn resolve<S: SiteSignals>(
        mut candidate: RouteCandidate,
        local: bool,
        inputs: &mut Inputs<'_, S>,
        reset_pairs: &mut BTreeSet<SitePair>,
    ) -> (f64, RouteCandidate, bool) {
        let readings = observed(&mut candidate, local, inputs);
        let reading = &readings.horizon;
        let recent = &readings.recent;
        if reading.unready {
            candidate.admission_state = AdmissionState::Excluded;
        }
        // The site as a whole had work waiting at some instant of the settling time: its
        // average queue at queue_full, or any work held before scheduling, which is a count
        // and signals pressure at one. This evidence does not require a running count.
        candidate.backlog = if recent
            .queued
            .is_some_and(|queued| queued >= inputs.availability.queue_full)
            || recent.held.is_some_and(|held| held >= 1.0)
        {
            Some(true)
        } else {
            recent.queued.map(|_| false)
        };
        let measured = Self::measured(&candidate, &readings, inputs, reset_pairs);
        if let Some(measured) = &measured {
            candidate.capacity = Some(measured.ceiling);
            candidate.rho = Some(measured.rho);
            candidate.full = measured.full;
            candidate.relieved = measured.relieved;
        } else {
            // Direct pressure remains useful without a count or a learned ceiling.
            candidate.rho = reported_limit(reading);
        }
        // Ordered by queue depth; a site publishing none sorts after every site that does.
        let load = if candidate.admission_state == AdmissionState::NewAndExisting {
            reading.queued.unwrap_or(f64::INFINITY)
        } else {
            UNHEALTHY
        };
        (load, candidate, measured.is_some())
    }

    /// The site over the horizon, and over the settling time alone: `room_after_ms`. Both are
    /// anchored at the site's newest fresh sample, so a site polled less often than either is
    /// still judged on what it last said. Fullness is judged from the second, so one spike a
    /// horizon ago does not hold a site full.
    fn readings<S: SiteSignals>(candidate: &RouteCandidate, inputs: &Inputs<'_, S>) -> Readings {
        let state = || {
            inputs
                .signals
                .learning_state(&candidate.site, &candidate.cluster, inputs.now_ms, inputs.window_ms)
        };
        let before = state();
        let over = |horizon_ms| Over {
            now_ms: inputs.now_ms,
            window_ms: inputs.window_ms,
            horizon_ms,
            queue_full: inputs.availability.queue_full,
            reported_full: REPORTED_FULL,
        };
        let reading = inputs
            .signals
            .read(&candidate.site, &candidate.cluster, over(inputs.horizon_ms));
        let recent = inputs.signals.read(
            &candidate.site,
            &candidate.cluster,
            over(inputs.availability.room_after_ms),
        );
        if before != state() {
            return Readings::default();
        }
        Readings {
            coherent: true,
            horizon: reading,
            recent,
            epoch: before.epoch,
        }
    }

    /// Lift every measured site's weight to at least `share` of the largest ceiling.
    ///
    /// A soft bound only grows when a site carries queue-free load, and a site weighted by a
    /// small ceiling carries little. Without a floor an unproven site would never prove more.
    /// The floor as a device follows Brent Salisbury's pressure-weighted placement, where it keeps
    /// a saturated site eligible; here it keeps an unproven one exploring.
    fn explore_floor(ranked: Vec<(f64, RouteCandidate, bool)>, share: f64) -> Vec<(f64, RouteCandidate)> {
        let largest = ranked
            .iter()
            .filter(|(_, _, measured)| *measured)
            .filter_map(|(_, candidate, _)| candidate.capacity)
            .fold(0.0_f64, f64::max);
        let floor = largest * share;
        ranked
            .into_iter()
            .map(|(load, mut candidate, measured)| {
                if measured {
                    candidate.capacity = candidate.capacity.map(|ceiling| ceiling.max(floor));
                }
                (load, candidate)
            })
            .collect()
    }

    /// The site's learned bounds, smoothed saturation, and fullness, or `None` when it
    /// publishes no in-flight sample in the window.
    ///
    /// The soft bound is the most held queue-free; the hard bound is the least held at a
    /// demonstrated limit. A count is read against the lesser, floored. Saturation is the
    /// nearer of that counted limit and the provider's own fullness, smoothed so one new sample
    /// moves it by `smoothing`; a refresh with no new sample leaves it alone. Full is at the
    /// resulting ceiling with a backlog for `full_after_ms`; room is no such sample in `recent`.
    ///
    /// The smoothing, and reading an absent sample as absent rather than as zero load, follow
    /// Brent Salisbury's pressure-weighted placement. The learned ceiling in place of a
    /// declared capacity is new.
    fn measured<S: SiteSignals>(
        candidate: &RouteCandidate,
        readings: &Readings,
        inputs: &mut Inputs<'_, S>,
        reset_pairs: &mut BTreeSet<SitePair>,
    ) -> Option<Measured> {
        readings.coherent.then_some(())?;
        let key = (Arc::clone(&candidate.site), Arc::clone(&candidate.cluster));
        inputs.learned.retire_changed(&key, readings.epoch, reset_pairs);
        let epoch = readings.epoch?;
        let reading = &readings.horizon;
        let in_flight = reading.in_flight?;
        // The newest sample at or before now is the epoch a smoothing step belongs to.
        let sample_at = reading.sampled_at.map(|at| at.min(inputs.now_ms));
        let previous = inputs.learned.load.get(&key).copied();
        let ceiling = ceiling(previous, reading, inputs.now_ms, inputs.window_ms, inputs.availability);
        let by_count = (in_flight / ceiling.read_against).clamp(0.0, 1.0);
        let raw = nearest_limit(reading, by_count, ceiling.hard.is_some());
        let rho = smoothed(previous, sample_at, raw, inputs.availability.smoothing);
        let (congested, full_since) = congestion(previous, &readings.recent, ceiling.read_against, inputs);
        let reset = reset_pairs.contains(&key);
        let full = (!reset)
            .then(|| full_for(candidate, full_since, inputs.now_ms, inputs.availability.full_after_ms))
            .flatten();
        let age_ms = sample_at.map(|at| inputs.now_ms.saturating_sub(at));
        let limits = Limits::read(in_flight, &ceiling, by_count, full, age_ms);
        publish_limits(&key, &ceiling, reading, &limits);
        inputs.learned.load.insert(
            key,
            SiteLearned::at(&ceiling, rho, inputs.now_ms, sample_at, full_since, epoch),
        );
        Some(Measured {
            ceiling: ceiling.read_against,
            rho,
            full,
            relieved: candidate.backlog.filter(|_| !reset).map(|_| !congested),
        })
    }

    /// This snapshot with the models to shed, given the set `previous` shed.
    ///
    /// A model sheds when every admitted, healthy site serving it has been full for
    /// `full_after_ms`: at its learned ceiling with work waiting. It routes again once any
    /// site has room: its queue emptied, or it has been below its ceiling for `room_after_ms`.
    /// Between the two it keeps its previous state, so a site hovering at the ceiling does not
    /// flap. A model with any healthy site of unknown saturation or unknown queue never sheds:
    /// that site takes the overflow.
    #[must_use]
    pub(crate) fn shed(mut self, previous: &BTreeSet<Arc<str>>, availability: &AvailabilitySettings) -> Self {
        // Per model: whether every healthy site is measured, and whether each is full.
        let mut models: BTreeMap<&Arc<str>, Fullness> = BTreeMap::new();
        // A demoted cluster carries a NaN score and is not one of the model's sites here, nor
        // is a silent one: it takes no overflow.
        let healthy = self
            .candidates
            .iter()
            .zip(&self.scores)
            .filter(|(candidate, score)| sheds_on(candidate, **score));
        for (candidate, _) in healthy {
            let (measured, sites) = models.entry(&candidate.name).or_insert((true, Vec::new()));
            match (candidate.full, candidate.relieved) {
                (Some(full), Some(relieved)) => sites.push((full, relieved)),
                _ => *measured = false,
            }
        }
        self.shedding = models
            .into_iter()
            .filter(|_| availability.shedding)
            .filter(|(model, (measured, sites))| {
                // Full is at the ceiling with work waiting, held long enough: a ceiling alone is a
                // site working flat out, not one that cannot take more. Room is the queue
                // emptying, or below the ceiling long enough; a dip is not room, so no flap.
                let full = sites.iter().all(|(full, _)| *full);
                let room = sites.iter().any(|(_, relieved)| *relieved);
                *measured && !sites.is_empty() && (full || (previous.contains(*model) && !room))
            })
            .map(|(model, _)| Arc::clone(model))
            .collect();
        for model in previous.difference(&self.shedding) {
            metrics::gauge!("grid_route_shedding", "model" => Arc::clone(model)).set(0.0);
        }
        for model in &self.shedding {
            metrics::gauge!("grid_route_shedding", "model" => Arc::clone(model)).set(1.0);
        }
        self
    }

    /// This snapshot with every candidate on a `down` cluster ordered after all the rest.
    ///
    /// Last rather than dropped: while any candidate is healthy it is never chosen, and
    /// when none is, the request still has somewhere to go.
    #[must_use]
    pub(crate) fn demote(self, down: &BTreeSet<Arc<str>>) -> Self {
        self.demote_where(down, |candidate| Some(&candidate.cluster))
    }

    /// This snapshot with every candidate whose `key` is in `down` ordered last.
    fn demote_where(self, down: &BTreeSet<Arc<str>>, key: impl Fn(&RouteCandidate) -> Option<&Arc<str>>) -> Self {
        if down.is_empty() {
            return self;
        }
        let ranked = self
            .scores
            .into_iter()
            .zip(self.candidates)
            .map(|(load, candidate)| {
                let demoted = if key(&candidate).is_some_and(|key| down.contains(key)) {
                    UNHEALTHY
                } else {
                    load
                };
                (demoted, candidate)
            })
            .collect();
        let mut ordered = Self::ranked(ranked, self.local_site);
        ordered.provider_hop_clusters = self.provider_hop_clusters;
        ordered
    }

    /// This snapshot, after setting `grid_route_site_score` for each of its site/cluster pairs.
    ///
    /// The gauge is the score the order used, NaN when the candidate is excluded or
    /// demoted. A pair in `published` but no longer in the topology is set to NaN,
    /// since the exporter keeps a series until restart. `published` becomes this
    /// snapshot's pairs.
    #[must_use]
    pub(crate) fn published(self, gauged: &mut Gauged) -> Self {
        let Gauged { published, learned } = gauged;
        let mut current = BTreeSet::new();
        for (candidate, score) in self.candidates.iter().zip(&self.scores) {
            let key = (Arc::clone(&candidate.site), Arc::clone(&candidate.cluster));
            // The front entry for a pair is its best; later ones are other models.
            if current.insert(key.clone()) {
                site_score(&key, *score);
                site_gauge("grid_route_site_rho", &key, candidate.rho.unwrap_or(f64::NAN));
                site_gauge("grid_route_site_weight", &key, candidate.capacity.unwrap_or(f64::NAN));
                // The limits are set where they are learned; an unmeasured site must not keep
                // showing the last ones it had, or a seed taken during an outage reads them.
                if candidate.capacity.is_none() {
                    clear_measured(&key);
                    site_gauge("grid_route_site_rho_reported", &key, candidate.rho.unwrap_or(f64::NAN));
                }
            }
        }
        for gone in published.difference(&current) {
            site_score(gone, f64::NAN);
            site_gauge("grid_route_site_silent", gone, f64::NAN);
            site_gauge("grid_route_site_last_sample_age_seconds", gone, f64::NAN);
            site_gauge("grid_route_site_round_age_seconds", gone, f64::NAN);
            site_gauge("grid_route_site_rho", gone, f64::NAN);
            site_gauge("grid_route_site_weight", gone, f64::NAN);
            clear_measured(gone);
            // Learned bounds outlive their samples, so a pair that leaves must not keep them:
            // a site rebuilt under the same name would resume them untaught.
            learned.load.remove(gone);
            learned.silence.remove(gone);
            learned.judged_since.remove(gone);
        }
        *published = current;
        self
    }

    /// Sort `ranked` ascending by score, stable among equals, and wrap it.
    fn ranked(mut ranked: Vec<(f64, RouteCandidate)>, local_site: Arc<str>) -> Self {
        ranked.sort_by(|(left, _), (right, _)| left.total_cmp(right));
        let (scores, ordered): (Vec<f64>, Vec<RouteCandidate>) = ranked.into_iter().unzip();
        Self {
            decisions: decisions_for(&ordered),
            candidates: ordered,
            scores,
            local_site,
            shedding: BTreeSet::new(),
            provider_hop_clusters: Arc::default(),
        }
    }
}

/// Decision counters for each of `candidates`, in order.
pub(crate) fn decisions_for(candidates: &[RouteCandidate]) -> Vec<SiteDecisions> {
    candidates
        .iter()
        .map(|candidate| SiteDecisions::new(&candidate.site, &candidate.cluster))
        .collect()
}

/// A candidate's site and cluster, the labels of its score gauge.
type SitePair = (Arc<str>, Arc<str>);

/// What one site's own load has taught the gateway: its running ceiling and smoothed saturation.
#[derive(Clone, Copy, Debug, PartialEq)]
struct SiteLearned {
    /// Qualified source revision that taught this state.
    epoch: u64,
    /// The most it has held with nothing waiting: capacity is at least this. Absent until a
    /// queue-free sample teaches one.
    soft: Option<f64>,
    /// The least it has held while work waited, since the last queue-free sample to reach that
    /// level: capacity is at most this. Absent until a backlog is seen.
    hard: Option<f64>,
    /// Saturation against that ceiling, smoothed by [`AvailabilitySettings::smoothing`].
    rho: f64,
    /// When the site was last measured, so a spell does not resume across a gap.
    at_ms: i64,
    /// The newest sample the saturation was smoothed from, so a refresh with nothing new
    /// does not smooth again.
    sample_at: Option<i64>,
    /// When the site last reached its ceiling with work waiting and has stayed so since, for
    /// `full_after_ms`.
    full_since: Option<i64>,
    /// When the site's current spell of having work waiting began, whatever its ceiling, so one
    /// burst of arrivals does not read as a site that shrank.
    backlog_since: Option<i64>,
    /// Units the site reported when these bounds were learned.
    units: Option<f64>,
    /// When it first reported fewer than that, so a late scrape is not read as a smaller site.
    shrunk_since: Option<i64>,
}

/// A site's two bounds at one refresh, and the denominator they give.
struct Ceiling {
    /// Capacity is at least this: the most held with nothing waiting.
    soft: Option<f64>,
    /// Capacity is at most this: the least held while work waited.
    hard: Option<f64>,
    /// When the current backlog spell began, carried into the next refresh.
    backlog_since: Option<i64>,
    /// What a count is read against, never below `ceiling_floor`.
    read_against: f64,
    /// Units the site reported when these bounds were taught, not this refresh's count. Pinned,
    /// because a count carried forward from the newest reading can never differ from it.
    units: Option<f64>,
    /// When the site first reported fewer units than its bounds were learned at.
    shrunk_since: Option<i64>,
}

/// When the site started reporting fewer serving units than its bounds were taught at, and
/// whether that has held long enough to retire them.
///
/// Bounds describe a site of a given size, and a site reporting fewer units is not that site.
/// Nothing it publishes says how much smaller it got, so what was learned about the old one is
/// dropped rather than scaled, and relearned from the next queue-free sample. Sustained past
/// the window, because the ready-unit count counts fresh scrapes rather than ready pods and
/// dips whenever one is late.
fn shrinking(prior: Option<SiteLearned>, reading: &SiteReading, now_ms: i64, window_ms: i64) -> (Option<i64>, bool) {
    let since = spell_since(
        prior.map(|prior| (prior.at_ms, prior.shrunk_since)),
        lost_units(reading.units, prior.and_then(|prior| prior.units)),
        now_ms,
        window_ms,
    );
    (
        since,
        since.is_some_and(|since| now_ms.saturating_sub(since) >= window_ms),
    )
}

/// Whether anything is waiting at the site, and what it holds if nothing is.
///
/// A sample taken while work waits measures the backlog rather than the capacity, so only a
/// queue-free one can teach what the site carries.
fn queue_state(reading: &SiteReading) -> (bool, Option<f64>) {
    let waiting = waiting(reading);
    (waiting, (!waiting).then(|| reading.in_flight.unwrap_or(0.0)))
}

/// The hard bound still worth keeping.
///
/// A queue-free sample at or above it retires it: the site now carries what it used to queue
/// at, so that bound describes a site that no longer exists.
fn kept_hard(prior: Option<SiteLearned>, queue_free: Option<f64>) -> Option<f64> {
    prior
        .and_then(|prior| prior.hard)
        .filter(|hard| queue_free.is_none_or(|free| free < *hard))
}

/// The unit count the bounds now describe: this refresh's where they were just taught or
/// retired, else the one carried forward. Carrying it is the point, since a count taken from
/// the newest reading every refresh can never differ from it.
fn taught_at(now: Option<f64>, prior: Option<SiteLearned>, soft: Option<f64>, retired: bool) -> Option<f64> {
    if retired || soft != prior.and_then(|prior| prior.soft) {
        now
    } else {
        prior.and_then(|prior| prior.units).or(now)
    }
}

/// What a change in the site's unit count means for the bounds learned about the old one.
struct Resized {
    /// When it first reported fewer units than its bounds were taught at.
    shrunk_since: Option<i64>,
    /// Whether that has held long enough to drop both bounds.
    retired: bool,
    /// Whether it reports more units, which drops the hard bound alone.
    grown: bool,
}

/// Read the unit count against the one the bounds were taught at.
///
/// A site that gained units is no longer held by what the smaller one queued at, and only the
/// hard bound goes: the most a smaller site carried is still a floor under what a larger one
/// can, and dropping that floor would read the offered load as the capacity.
fn resized(prior: Option<SiteLearned>, reading: &SiteReading, now_ms: i64, window_ms: i64) -> Resized {
    let (shrunk_since, retired) = shrinking(prior, reading, now_ms, window_ms);
    Resized {
        shrunk_since,
        retired,
        grown: gained_units(reading.units, prior.and_then(|prior| prior.units)),
    }
}

/// Whether the site reports more serving units than its bounds were learned at.
///
/// Immediate, with no spell behind it, because releasing an upper bound only ever raises the
/// ceiling and the next congested sample teaches a new one. A spell guards the other direction,
/// where a stale count would lower it.
fn gained_units(now: Option<f64>, learned: Option<f64>) -> bool {
    now.zip(learned).is_some_and(|(now, learned)| now > learned)
}

/// Whether the site reports fewer serving units than its bounds were learned at.
///
/// Only a fall counts here. Growth is handled by `gained_units`, which drops the hard bound
/// alone: the soft bound is still a true lower bound on what the larger site can carry.
fn lost_units(now: Option<f64>, learned: Option<f64>) -> bool {
    now.zip(learned).is_some_and(|(now, learned)| now < learned)
}

impl Ceiling {
    /// Set the ceiling gauges, so an operator can tell a bound that moved because the site
    /// carried more from one that moved because it queued.
    fn publish(&self, key: &SitePair) {
        site_gauge("grid_route_site_ceiling", key, self.read_against);
        site_gauge("grid_route_site_ceiling_soft", key, self.soft.unwrap_or(f64::NAN));
        site_gauge("grid_route_site_ceiling_hard", key, self.hard.unwrap_or(f64::NAN));
    }
}

impl SiteLearned {
    /// What this refresh leaves for the next one.
    #[expect(
        clippy::too_many_arguments,
        reason = "retained learning binds its bounds and timing to one revision"
    )]
    fn at(
        ceiling: &Ceiling,
        rho: f64,
        now_ms: i64,
        sample_at: Option<i64>,
        full_since: Option<i64>,
        epoch: u64,
    ) -> Self {
        Self {
            epoch,
            soft: ceiling.soft,
            hard: ceiling.hard,
            rho,
            at_ms: now_ms,
            sample_at,
            full_since,
            backlog_since: ceiling.backlog_since,
            units: ceiling.units,
            shrunk_since: ceiling.shrunk_since,
        }
    }
}

/// Both observation horizons from a consistent publication state.
#[derive(Default)]
struct Readings {
    /// Both horizons were read without an intervening publication.
    coherent: bool,
    /// Qualified learning history shared by both horizons.
    epoch: Option<u64>,
    /// Full measurement horizon.
    horizon: SiteReading,
    /// Recent pressure and congestion horizon.
    recent: SiteReading,
}

/// What one refresh measured about a site.
struct Measured {
    /// The denominator produced by the learned soft and hard bounds.
    ceiling: f64,
    /// Smoothed saturation at the nearer provider-reported or counted limit.
    rho: f64,
    /// At the ceiling with a backlog for `full_after_ms`; `None` without a queue reading.
    full: Option<bool>,
    /// No sample in the settling time at the ceiling with a backlog; `None` without a queue reading.
    relieved: Option<bool>,
}

/// Per-site learned load state, kept across refreshes.
#[derive(Clone, Debug, Default)]
pub(crate) struct Learned {
    /// What each site's own load has taught.
    load: BTreeMap<SitePair, SiteLearned>,
    /// Each silent site on its way back.
    silence: BTreeMap<SitePair, Silence>,
    /// The first poller round each remote pair was judged against, for one never heard from.
    judged_since: BTreeMap<SitePair, i64>,
}

/// A silent site on its way back: the newest sample seen while silent, and how many new
/// samples have arrived since the silence.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Silence {
    /// When the newest sample seen while silent was taken.
    seen: Option<i64>,
    /// New samples since the site went silent.
    returned: u8,
}

/// New samples a silent site sends before it routes again, so one poll through a lossy link
/// does not bring it back.
const RETURN_SAMPLES: u8 = 2;

/// Control state that outlives one snapshot: the pairs with a published score, and what each
/// site's load has taught the gateway.
#[derive(Debug, Default)]
pub(crate) struct Gauged {
    /// `(site, cluster)` pairs with a published score.
    pub(crate) published: BTreeSet<SitePair>,
    /// Learned ceilings and smoothed saturation per site.
    pub(crate) learned: Learned,
}

impl Learned {
    /// Retire changed history once while marking every model sharing the provider.
    fn retire_changed(&mut self, key: &SitePair, epoch: Option<u64>, reset_pairs: &mut BTreeSet<SitePair>) {
        if self.load.get(key).is_some_and(|held| Some(held.epoch) != epoch) {
            self.load.remove(key);
            reset_pairs.insert(key.clone());
        }
    }

    /// Clear every site's soft bound, as a periodic reset would. Test-only, for the scenario
    /// that measures what such a reset would cost.
    #[cfg(test)]
    pub(crate) fn clear_soft(&mut self) {
        for learned in self.load.values_mut() {
            learned.soft = None;
        }
    }
}

impl Gauged {
    /// Empty state: nothing published, nothing learned.
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

/// The saturation estimate after `raw` at `sample_at`: moved by `alpha` once per new sample,
/// left where it was by a refresh that saw nothing new, and `raw` itself the first time.
fn smoothed(prior: Option<SiteLearned>, sample_at: Option<i64>, raw: f64, alpha: f64) -> f64 {
    match prior {
        Some(prior) if prior.sample_at == sample_at => prior.rho,
        Some(prior) => prior.rho.mul_add(1.0 - alpha, raw * alpha),
        None => raw,
    }
}

/// The soft and hard bounds `reading` teaches over what `prior` held, and the floored ceiling
/// they produce. The soft bound rises while the site keeps its shape; a demonstrated limit can
/// tighten the hard bound, and a serving-unit change retires the bounds that no longer apply.
///
/// In-flight counts waiting as well as running, so a sample with one whole request waiting
/// anywhere measures the backlog rather than the capacity and cannot raise the soft bound. It
/// can tighten the hard bound after demonstrating a limit, and reads against the resulting
/// ceiling.
fn ceiling(
    prior: Option<SiteLearned>,
    reading: &SiteReading,
    now_ms: i64,
    window_ms: i64,
    availability: &AvailabilitySettings,
) -> Ceiling {
    let (_, queue_free) = queue_state(reading);
    let sized = resized(prior, reading, now_ms, window_ms);
    let prior = prior.filter(|_| !sized.retired);
    let soft = soft_bound(prior.and_then(|prior| prior.soft), queue_free);
    let kept = kept_hard(prior.filter(|_| !sized.grown), queue_free);
    let backlog_since = sample_spell_since(prior, reading.backlogged_at, window_ms);
    let shrinking = lost_units(reading.units, prior.and_then(|prior| prior.units));
    // A short-lived drop in ready units can happen while a Deployment replaces a pod. Its
    // saturated sample describes that temporary smaller pool, not the size the retained
    // bounds describe. Keep the old hard bound until the smaller size has persisted for the
    // full shrink window; once retired, the new size can teach its own bound.
    let hard = if shrinking && !sized.retired {
        kept
    } else {
        hard_bound(
            kept,
            reading,
            Backlog {
                held: 0.0,
                soft,
                since: backlog_since,
                now_ms: reading.backlogged_at.unwrap_or(now_ms),
            },
            availability,
        )
    };
    Ceiling {
        soft,
        hard,
        backlog_since,
        read_against: read_against(soft, hard).max(availability.ceiling_floor),
        units: taught_at(reading.units, prior, soft, sized.retired || sized.grown),
        // Retiring resets the spell: the bounds now describe the size the site reports.
        shrunk_since: sized.shrunk_since.filter(|_| !sized.retired),
    }
}

/// Whether the site has a whole request waiting anywhere: at the site, at its busiest unit, or
/// held before scheduling. A sample taken then measures the backlog rather than the capacity.
fn waiting(reading: &SiteReading) -> bool {
    [reading.queued, reading.deepest_queue, reading.held]
        .into_iter()
        .any(|queue| queue.is_some_and(|queue| queue >= 1.0))
}

/// The soft bound after this refresh: the most the site has been seen to carry with nothing
/// waiting, which only ever rises.
fn soft_bound(kept: Option<f64>, queue_free: Option<f64>) -> Option<f64> {
    match (kept, queue_free) {
        (Some(held), Some(free)) => Some(held.max(free)),
        (held, free) => held.or(free),
    }
}

/// The hard bound after this refresh: what `kept` held, tightened by a congested observation
/// that counts as the site's limit.
fn hard_bound(
    kept: Option<f64>,
    reading: &SiteReading,
    backlog: Backlog,
    availability: &AvailabilitySettings,
) -> Option<f64> {
    // Two ways a sample shows a limit, each carrying the count it was holding then: one the
    // site reported full, one the gateway concluded from a settled backlog at its ceiling.
    let concluded = reading
        .congested_in_flight
        .filter(|held| at_limit(reading, Backlog { held: *held, ..backlog }, availability));
    let seen = [reading.full_in_flight, concluded]
        .into_iter()
        .flatten()
        .reduce(f64::min);
    match (kept, seen) {
        (Some(held), Some(seen)) => Some(held.min(seen)),
        (held, seen) => held.or(seen),
    }
}

/// What one backlog observation carries: how much the site held at a congested instant, the
/// most it has carried queue-free, when the spell began, and now.
#[derive(Clone, Copy, Debug)]
struct Backlog {
    /// What the site held at a congested instant in the window.
    held: f64,
    /// The most it has carried with nothing waiting.
    soft: Option<f64>,
    /// When the current backlog spell began.
    since: Option<i64>,
    /// Now, milliseconds.
    now_ms: i64,
}

/// Whether a backlog is the site's limit rather than a burst of arrivals.
///
/// Three shapes look alike from a count of in-flight requests, and the site's own reading is
/// what separates them. Queueing below the most it has carried, with the cache nearly spent, is
/// a site that shrank. Queueing at or above that, with a cool cache, is a site out of sequence
/// slots, which is the one request-denominated limit the engine really has. Queueing below that
/// with a cool cache is neither: the site is taking arrivals in bursts and nothing about its
/// capacity has been shown.
///
/// Time alone cannot tell the third from the second, because a standing queue at a quarter of
/// the load lasts as long as anyone cares to wait. A clock on its own reads burstiness as a
/// limit and ratchets the bound down to whatever the site held when the queue first appeared.
///
/// A preempted request returns to the front of the waiting queue rather than leaving the
/// engine, so a cache-limited backlog is visible as a queue at the moment it happens and the
/// in-flight count is unchanged by the preemption. Block pressure shows near a full cache
/// rather than at `REPORTED_FULL`, so a bound is learned where pressure begins rather than
/// where the engine gave up.
///
/// The floor on the spell matters because `full_after_ms` can be set to zero, and at zero even
/// a slot-bound site would latch on its first congested instant.
fn at_limit(reading: &SiteReading, backlog: Backlog, availability: &AvailabilitySettings) -> bool {
    let Backlog {
        held,
        soft,
        since,
        now_ms,
    } = backlog;
    let settled =
        since.is_some_and(|since| now_ms.saturating_sub(since) >= availability.full_after_ms.max(MIN_BACKLOG_SPELL_MS));
    match reading.saturation {
        Some(_) => settled && soft.is_none_or(|soft| held >= soft),
        None => settled,
    }
}

/// The denominator saturation is read against, before the floor.
///
/// The soft bound says capacity is at least `low`; a backlog at `high` says it is at most
/// `high`. With both, the smaller is the honest read: a `high` below `low` means the site
/// contradicted itself, which is what a site whose units got smaller does, and the newer
/// observation wins; a `high` above `low` says nothing `low` did not. With only one, that one,
/// which is what lets a site relearn a usable denominator from a busy spell alone.
fn read_against(low: Option<f64>, high: Option<f64>) -> f64 {
    match (low, high) {
        (Some(low), Some(high)) => low.min(high),
        (bound, None) | (None, bound) => bound.unwrap_or(0.0),
    }
}

/// Whether the site is at `ceiling` with a backlog, and when that spell began.
///
/// Both read from `recent`, the settling time rather than the horizon, and from one instant, so
/// a full moment and a queued moment are not crossed into congestion.
fn congestion<S: SiteSignals>(
    prior: Option<SiteLearned>,
    recent: &SiteReading,
    ceiling: f64,
    inputs: &Inputs<'_, S>,
) -> (bool, Option<i64>) {
    let congested = recent.congested_in_flight.is_some_and(|held| held >= ceiling);
    let since = spell_since(
        prior.map(|prior| (prior.at_ms, prior.full_since)),
        congested,
        inputs.now_ms,
        inputs.window_ms,
    );
    (congested, since)
}

/// Whether the site counts as full: a backlog at its ceiling, held for `full_after_ms`.
///
/// Absent where the site publishes no backlog, which is not the same as having none.
fn full_for(candidate: &RouteCandidate, full_since: Option<i64>, now_ms: i64, full_after_ms: i64) -> Option<bool> {
    candidate
        .backlog
        .map(|_| full_since.is_some_and(|since| now_ms.saturating_sub(since) >= full_after_ms))
}

/// What one measured refresh publishes beside `rho`.
struct Limits {
    /// Requests the site held, as read.
    in_flight: f64,
    /// In-flight over the ceiling, where a hard bound makes that a reading.
    by_count: Option<f64>,
    /// Whether the site is full, absent where it publishes no backlog.
    full: Option<bool>,
    /// Age of the sample the decision read.
    age_ms: Option<i64>,
}

impl Limits {
    /// What one refresh read, as the gauges take it.
    fn read(in_flight: f64, ceiling: &Ceiling, by_count: f64, full: Option<bool>, age_ms: Option<i64>) -> Self {
        Self {
            in_flight,
            // The count is a reading only where a hard bound holds it; elsewhere it is an upper
            // bound on saturation, and must not be published as an input.
            by_count: ceiling.hard.is_some().then_some(by_count),
            full,
            age_ms,
        }
    }
}

/// Set every gauge a measured refresh produces, so an operator reads the inputs the decision
/// used rather than numbers published beside them.
fn publish_limits(key: &SitePair, ceiling: &Ceiling, reading: &SiteReading, limits: &Limits) {
    ceiling.publish(key);
    site_gauge("grid_route_site_in_flight", key, limits.in_flight);
    site_gauge("grid_route_site_rho_counted", key, limits.by_count.unwrap_or(f64::NAN));
    site_gauge(
        "grid_route_site_rho_reported",
        key,
        reported_limit(reading).unwrap_or(f64::NAN),
    );
    site_gauge(
        "grid_route_site_full",
        key,
        limits.full.map_or(f64::NAN, |full| f64::from(u8::from(full))),
    );
    site_gauge(
        "grid_route_site_reading_age_seconds",
        key,
        limits.age_ms.map_or(f64::NAN, seconds),
    );
}

/// How close the site is to the nearest limit it is up against.
///
/// Two limits, grouped by who worked them out. What the provider reports about itself is one:
/// its own fullness, and the backlog behind it, both already relative to its own capacity. What
/// the gateway has to work out is the other: a count of requests over a ceiling learned from
/// the site's own load. The first needs nothing learned and arrives wherever the provider
/// publishes it; the second needs the ceiling and arrives only where a per-request count does.
///
/// The nearest, not a blend. Each limit is a necessary condition for having room, so the larger
/// reading is the binding one. These are limits rather than scores, which is why no weight sits
/// between them: a weighted sum of necessary conditions is not one, and a site at 0.95 of its
/// own reading and 0.05 of its counted ceiling would read half full while it is about to
/// preempt. The EPP weighs its KV and queue scorers against each other because it only ranks
/// endpoints; this reading also answers whether a site has room at all.
///
/// They bind in different regimes and neither knows which regime it is in, because request size
/// decides that and nothing here sees a request: at a full context a site holds one or two
/// requests while its cache is spent, and at small ones the slots fill while the cache is
/// barely touched.
fn nearest_limit(reading: &SiteReading, by_count: f64, at_a_known_limit: bool) -> f64 {
    match reported_limit(reading) {
        // The site says something about itself. Its count is admissible beside that only once
        // a backlog has shown where its limit is, because until then the ceiling is a lower
        // bound on capacity and the count over it is an upper bound on saturation, not a
        // reading. Taking the larger of a reading and an upper bound gives the upper bound,
        // which is how a site that has never been pushed reads as full while idle.
        Some(reported) if at_a_known_limit => reported.max(by_count),
        Some(reported) => reported,
        // The site says nothing, so its count is all there is, upper bound and all.
        None => by_count,
    }
}

/// What the site reports about its own fullness, already joined across its units by the signal
/// mapping.
///
/// The backlog is deliberately not folded in here. A queue is evidence that the site has
/// reached a limit, which is what `at_limit` reads it for, and not a measure of how full the
/// site is: a site at its ceiling with a queue is exactly where its own count should bind, and
/// treating the queue as a reading would suppress that count instead.
///
/// Absent when the site reports nothing, which is when its count is all there is.
fn reported_limit(reading: &SiteReading) -> Option<f64> {
    reading.saturation
}

/// When the site's current spell of `on` began, if it is `on` at `now_ms`: carried from
/// `prior`, that refresh's clock and the spell it held, while the site was measured within the
/// window, else starting now, so a site unmeasured for longer than the window does not resume a
/// spell from before the gap.
fn spell_since(prior: Option<(i64, Option<i64>)>, on: bool, now_ms: i64, window_ms: i64) -> Option<i64> {
    on.then(|| {
        prior
            .filter(|(at_ms, _)| now_ms.saturating_sub(*at_ms) <= window_ms)
            .and_then(|(_, since)| since)
            .unwrap_or(now_ms)
    })
}

/// When a spell of backlogged samples began. Refreshing the same sample does not advance its
/// clock, and a non-backlogged sample clears the spell before a later backlog starts another.
fn sample_spell_since(prior: Option<SiteLearned>, backlogged_at: Option<i64>, window_ms: i64) -> Option<i64> {
    let at = backlogged_at?;
    Some(
        prior
            .filter(|prior| {
                prior.backlog_since.is_some()
                    && prior
                        .sample_at
                        .is_some_and(|previous| at.saturating_sub(previous) <= window_ms)
            })
            .and_then(|prior| prior.backlog_since)
            .unwrap_or(at),
    )
}

/// Set `grid_route_site_score` for one site/cluster pair.
fn site_score(key: &SitePair, score: f64) {
    site_gauge("grid_route_site_score", key, score);
}

/// The gauges only a measured refresh sets. A pair that is still a candidate but published no
/// in-flight sample never reaches that refresh, so each has to be cleared by name: the exporter
/// keeps a series until restart, and a frozen ceiling beside a NaN one reads as a healthy site.
const MEASURED_GAUGES: [&str; 8] = [
    "grid_route_site_ceiling",
    "grid_route_site_ceiling_soft",
    "grid_route_site_ceiling_hard",
    "grid_route_site_rho_reported",
    "grid_route_site_rho_counted",
    "grid_route_site_in_flight",
    "grid_route_site_full",
    "grid_route_site_reading_age_seconds",
];

/// Whether `candidate` is one of the sites its model's shed waits on.
fn sheds_on(candidate: &RouteCandidate, score: f64) -> bool {
    candidate.kind == CapabilityKind::InferenceModel
        && candidate.admission_state == AdmissionState::NewAndExisting
        && !candidate.silent
        && candidate.publishes_load
        && !score.is_nan()
}

/// `candidate`'s readings, with its silence marked.
fn observed<S: SiteSignals>(candidate: &mut RouteCandidate, local: bool, inputs: &mut Inputs<'_, S>) -> Readings {
    let readings = RouteSnapshot::readings(candidate, inputs);
    observe_silence(candidate, local, &readings, inputs);
    readings
}

/// The instant `candidate`'s silence is judged at, `None` while it cannot be judged.
///
/// A peer is judged at the end of the operator's last poll round that tried it, so a stalled
/// poller or relay stops the clock rather than silencing every peer; one whose operator reports no
/// rounds is not judged. This site is judged at the last relay read. A store that has never been
/// relayed to is judged now.
fn judged_at<S: SiteSignals>(candidate: &RouteCandidate, local: bool, inputs: &Inputs<'_, S>) -> Option<i64> {
    let relay = inputs.signals.relay_heard_ms();
    let at = if local {
        Some(relay.unwrap_or(inputs.now_ms))
    } else {
        match (inputs.signals.round_ms(&candidate.site), relay) {
            (Some(round), _) => Some(round),
            (None, Some(_)) => None,
            (None, None) => Some(inputs.now_ms),
        }
    };
    at.map(|at| at.min(inputs.now_ms))
}

/// Mark and publish whether `candidate`'s site is silent.
///
/// A full window without a sample, judged at [`judged_at`], makes it silent, as does a full window
/// of a peer's rounds with no sample at all from a candidate that publishes load. It routes again
/// after [`RETURN_SAMPLES`] new samples with no full window between them. An incoherent read, or
/// one that cannot be judged, keeps the last verdict.
fn observe_silence<S: SiteSignals>(
    candidate: &mut RouteCandidate,
    local: bool,
    readings: &Readings,
    inputs: &mut Inputs<'_, S>,
) {
    let key = (Arc::clone(&candidate.site), Arc::clone(&candidate.cluster));
    let now_ms = inputs.now_ms;
    let window_ms = inputs.window_ms;
    let judged = judged_at(candidate, local, inputs);
    let rounds = (!local).then(|| inputs.signals.round_ms(&candidate.site)).flatten();
    let learned = &mut *inputs.learned;
    let sampled_at = readings.horizon.sampled_at;
    // A stalled poller holds every peer's verdict, so its round's age is published to alert on,
    // NaN for a peer the poller reports no round for.
    let round_age = rounds.map_or(f64::NAN, |round| seconds(now_ms.saturating_sub(round).max(0)));
    site_gauge("grid_route_site_round_age_seconds", &key, round_age);
    let Some(judged) = judged.filter(|_| readings.coherent) else {
        candidate.silent = learned.silence.contains_key(&key);
        publish_silence(&key, candidate.silent, sampled_at, now_ms);
        return;
    };
    let quiet = match (sampled_at, rounds) {
        (Some(at), _) => judged.saturating_sub(at) > window_ms,
        (None, Some(round)) if candidate.publishes_load => {
            let since = *learned.judged_since.entry(key.clone()).or_insert(round);
            round.saturating_sub(since) > window_ms
        },
        (None, _) => false,
    };
    candidate.silent = silence_after(&mut learned.silence, &key, quiet, sampled_at);
    publish_silence(&key, candidate.silent, sampled_at, now_ms);
}

/// Whether the site at `key` is silent after a read that was `quiet` or saw `sampled_at`.
fn silence_after(held: &mut BTreeMap<SitePair, Silence>, key: &SitePair, quiet: bool, sampled_at: Option<i64>) -> bool {
    if quiet {
        // Every full window of silence starts the count again.
        let restart = Silence {
            seen: sampled_at,
            returned: 0,
        };
        if held.insert(key.clone(), restart).is_none() {
            silence_transition(key, "silent");
        }
        true
    } else if let Entry::Occupied(mut silence) = held.entry(key.clone()) {
        let back = silence.get_mut().heard(sampled_at);
        if back {
            silence.remove();
            silence_transition(key, "live");
        }
        !back
    } else {
        false
    }
}

impl Silence {
    /// Count `sampled_at` if it is new; whether the site has now sent enough to route again.
    fn heard(&mut self, sampled_at: Option<i64>) -> bool {
        // Samples aging out of the store is not one arriving.
        if sampled_at.is_some() && self.seen != sampled_at {
            self.seen = sampled_at;
            self.returned = self.returned.saturating_add(1);
        }
        self.returned >= RETURN_SAMPLES
    }
}

/// Count a site entering or leaving silence.
fn silence_transition((site, cluster): &SitePair, to: &'static str) {
    metrics::counter!(
        "grid_route_site_silent_transitions_total",
        "site" => Arc::clone(site),
        "cluster" => Arc::clone(cluster),
        "to" => to
    )
    .increment(1);
}

/// Publish whether the site at `key` is silent and how long since its newest sample.
fn publish_silence(key: &SitePair, silent: bool, sampled_at: Option<i64>, now_ms: i64) {
    site_gauge("grid_route_site_silent", key, f64::from(u8::from(silent)));
    let age = sampled_at.map_or(f64::NAN, |at| seconds(now_ms.saturating_sub(at).max(0)));
    site_gauge("grid_route_site_last_sample_age_seconds", key, age);
}

/// `ms` milliseconds as seconds, for a gauge.
fn seconds(ms: i64) -> f64 {
    f64::from(u32::try_from(ms).unwrap_or(u32::MAX)) / 1000.0
}

/// Set every gauge a measured refresh owns to NaN.
fn clear_measured(key: &SitePair) {
    for name in MEASURED_GAUGES {
        site_gauge(name, key, f64::NAN);
    }
}

/// Set a per-site gauge: a limit, the ceiling it is read against, or the weight.
fn site_gauge(name: &'static str, (site, cluster): &SitePair, value: f64) {
    metrics::gauge!(name, "site" => Arc::clone(site), "cluster" => Arc::clone(cluster)).set(value);
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::min_ident_chars,
    clippy::too_many_lines,
    reason = "tests"
)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{
        descriptor::{CandidateConfig, validate_candidates},
        signals::llm_d::QUEUE_METRIC,
    };

    /// One QUEUE sample for `site`/`cluster` at `at_ms`.
    fn line(site: &str, cluster: &str, value: f64, at_ms: i64) -> String {
        format!(r#"{QUEUE_METRIC}{{grid_site="{site}",grid_provider="{cluster}"}} {value} {at_ms}"#)
    }

    /// A validated candidate for model `name` served by `site`/`cluster`.
    fn cand(name: &str, site: &str, cluster: &str) -> CandidateConfig {
        CandidateConfig {
            expires_at_ms: None,
            admission: AdmissionState::default(),
            cluster: cluster.to_owned(),
            credential: None,
            fresh: true,
            kind: CapabilityKind::InferenceModel,
            name: name.to_owned(),
            publishes_load: true,
            site: site.to_owned(),
            stable_id: None,
        }
    }

    #[test]
    fn a_cluster_with_no_healthy_endpoint_is_never_chosen_while_another_is_healthy() {
        let store = LoadStore::new(Duration::from_secs(60));
        // site-a is the least loaded but down; site-d is unmeasured.
        store.ingest_at(&line("local", "site-a", 0.0, 1_000), 1_000, 1_000, "local");
        store.ingest_at(&line("local", "site-b", 50.0, 1_000), 1_000, 1_000, "local");
        let candidates = validate_candidates(vec![
            cand("m", "local", "site-a"),
            cand("m", "local", "site-b"),
            cand("m", "site-d", "site-d"),
        ])
        .unwrap();
        let down = BTreeSet::from([Arc::from("site-a")]);
        let snapshot = RouteSnapshot::from_store(
            candidates,
            Arc::from("local"),
            &mut Inputs {
                signals: &store,
                now_ms: 1_000,
                window_ms: 60_000,
                horizon_ms: 60_000,
                availability: &AvailabilitySettings::default(),
                learned: &mut Learned::default(),
            },
        )
        .demote(&down);
        let order: Vec<&str> = snapshot.candidates.iter().map(|c| &*c.cluster).collect();
        assert_eq!(order, ["site-b", "site-d", "site-a"], "down sorts after unmeasured");
        for turn in 0..10 {
            let picked = crate::route::select_spread(
                &snapshot,
                CapabilityKind::InferenceModel,
                "m",
                turn,
                |_| true,
                &crate::route::KeepAll,
            )
            .unwrap()
            .candidate;
            assert_ne!(&*picked.cluster, "site-a", "turn {turn}");
        }
    }

    /// One readiness sample for `site`/`cluster` at `at_ms`.
    fn ready(site: &str, cluster: &str, value: f64, at_ms: i64) -> String {
        format!(r#"llm_d_epp_ready_endpoints{{grid_site="{site}",grid_provider="{cluster}"}} {value} {at_ms}"#)
    }

    #[test]
    fn a_provider_reported_unready_is_excluded_until_a_newer_sample_says_ready() {
        let store = LoadStore::new(Duration::from_secs(60));
        // site-b is the least loaded, but its operator says it cannot serve.
        store.ingest_at(&line("site-a", "pool-a", 5.0, 1_000), 1_000, 1_000, "site-a");
        store.ingest_at(&line("site-b", "pool-b", 0.0, 1_000), 1_000, 1_000, "site-b");
        store.ingest_at(&ready("site-b", "pool-b", 0.0, 1_000), 1_000, 1_000, "site-b");
        let candidates = || {
            validate_candidates(vec![
                cand("m", "site-a", "pool-a"),
                cand("m", "site-b", "pool-b"),
                cand("m", "site-d", "pool-d"),
            ])
            .unwrap()
        };
        let snapshot = RouteSnapshot::from_store(
            candidates(),
            Arc::from("hub"),
            &mut Inputs {
                signals: &store,
                now_ms: 1_000,
                window_ms: 30_000,
                horizon_ms: 30_000,
                availability: &AvailabilitySettings::default(),
                learned: &mut Learned::default(),
            },
        );
        let order: Vec<&str> = snapshot.candidates.iter().map(|c| &*c.cluster).collect();
        assert_eq!(order, ["pool-a", "pool-d", "pool-b"], "unready sorts last");
        for turn in 0..10 {
            let picked = crate::route::select_spread(
                &snapshot,
                CapabilityKind::InferenceModel,
                "m",
                turn,
                |_| true,
                &crate::route::KeepAll,
            )
            .unwrap()
            .candidate;
            assert_ne!(&*picked.cluster, "pool-b", "turn {turn}");
        }

        store.ingest_at(&ready("site-b", "pool-b", 1.0, 2_000), 2_000, 2_000, "site-b");
        let recovered = RouteSnapshot::from_store(
            candidates(),
            Arc::from("hub"),
            &mut Inputs {
                signals: &store,
                now_ms: 2_000,
                window_ms: 30_000,
                horizon_ms: 30_000,
                availability: &AvailabilitySettings::default(),
                learned: &mut Learned::default(),
            },
        );
        assert_eq!(
            &*recovered.candidates[0].cluster, "pool-b",
            "back on the next sample, not after the window"
        );
    }

    #[test]
    fn a_not_ready_verdict_holds_through_silence_and_clears_when_the_peer_stops_publishing_it() {
        let store = LoadStore::new(Duration::from_secs(60));
        store.ingest_at(&line("site-b", "pool-b", 0.0, 1_000), 1_000, 1_000, "site-b");
        store.ingest_at(&ready("site-b", "pool-b", 0.0, 1_000), 1_000, 1_000, "site-b");
        let order = |now| {
            let candidates =
                validate_candidates(vec![cand("m", "site-b", "pool-b"), cand("m", "site-a", "pool-a")]).unwrap();
            let snapshot = RouteSnapshot::from_store(
                candidates,
                Arc::from("hub"),
                &mut Inputs {
                    signals: &store,
                    now_ms: now,
                    window_ms: 30_000,
                    horizon_ms: 30_000,
                    availability: &AvailabilitySettings::default(),
                    learned: &mut Learned::default(),
                },
            );
            snapshot.candidates.first().map(|c| c.cluster.to_string())
        };
        // Partitioned: nothing newer arrives, long after the load window.
        assert_eq!(
            order(120_000).as_deref(),
            Some("pool-a"),
            "the last 0 stands through silence"
        );
        // Healed, but the peer no longer publishes readiness.
        store.ingest_at(&line("site-b", "pool-b", 0.0, 121_000), 121_000, 121_000, "site-b");
        assert_eq!(
            order(121_000).as_deref(),
            Some("pool-b"),
            "a newer reading without the series is ready"
        );
    }

    #[test]
    fn an_excluded_candidate_from_the_serving_config_is_never_chosen() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut excluded = cand("m", "site-b", "pool-b");
        excluded.admission = AdmissionState::Excluded;
        let candidates = validate_candidates(vec![excluded, cand("m", "site-a", "pool-a")]).unwrap();
        let snapshot = RouteSnapshot::from_store(
            candidates,
            Arc::from("hub"),
            &mut Inputs {
                signals: &store,
                now_ms: 1_000,
                window_ms: 30_000,
                horizon_ms: 30_000,
                availability: &AvailabilitySettings::default(),
                learned: &mut Learned::default(),
            },
        );
        for turn in 0..4 {
            let picked = crate::route::select_spread(
                &snapshot,
                CapabilityKind::InferenceModel,
                "m",
                turn,
                |_| true,
                &crate::route::KeepAll,
            )
            .unwrap()
            .candidate;
            assert_eq!(&*picked.cluster, "pool-a");
        }
    }

    #[test]
    fn when_every_cluster_is_down_one_is_still_chosen() {
        let store = LoadStore::new(Duration::from_secs(60));
        let candidates = validate_candidates(vec![cand("m", "local", "site-a"), cand("m", "local", "site-b")]).unwrap();
        let down = BTreeSet::from([Arc::from("site-a"), Arc::from("site-b")]);
        let snapshot = RouteSnapshot::from_store(
            candidates,
            Arc::from("local"),
            &mut Inputs {
                signals: &store,
                now_ms: 1_000,
                window_ms: 60_000,
                horizon_ms: 60_000,
                availability: &AvailabilitySettings::default(),
                learned: &mut Learned::default(),
            },
        )
        .demote(&down);
        assert!(
            crate::route::select_spread(
                &snapshot,
                CapabilityKind::InferenceModel,
                "m",
                0,
                |_| true,
                &crate::route::KeepAll
            )
            .is_some()
        );
    }

    #[test]
    fn a_site_reporting_the_legacy_load_name_is_still_ordered() {
        let store = LoadStore::new(Duration::from_secs(60));
        let legacy = |site: &str, cluster: &str, value: f64| {
            format!(r#"inference_pool_average_queue_size{{grid_site="{site}",grid_provider="{cluster}"}} {value} 1000"#)
        };
        store.ingest_at(&legacy("east", "pool-a", 90.0), 1_000, 1_000, "east");
        store.ingest_at(&line("west", "pool-b", 10.0, 1_000), 1_000, 1_000, "west");
        let candidates = validate_candidates(vec![cand("m", "east", "pool-a"), cand("m", "west", "pool-b")]).unwrap();
        let snapshot = RouteSnapshot::from_store(
            candidates,
            Arc::from("local"),
            &mut Inputs {
                signals: &store,
                now_ms: 1_000,
                window_ms: 60_000,
                horizon_ms: 60_000,
                availability: &AvailabilitySettings::default(),
                learned: &mut Learned::default(),
            },
        );
        let order: Vec<&str> = snapshot.candidates.iter().map(|c| &*c.site).collect();
        assert_eq!(
            order,
            ["west", "east"],
            "both names measure load; neither sorts as unmeasured"
        );
    }

    #[test]
    fn the_least_loaded_site_sorts_first() {
        let store = LoadStore::new(Duration::from_secs(60));
        // Same model on two sites: east is busy (90), west is idle (10).
        store.ingest_at(&line("east", "pool-a", 90.0, 1_000), 1_000, 1_000, "east");
        store.ingest_at(&line("west", "pool-b", 10.0, 1_000), 1_000, 1_000, "west");

        let candidates =
            validate_candidates(vec![cand("llama", "east", "pool-a"), cand("llama", "west", "pool-b")]).unwrap();
        let snap = RouteSnapshot::from_store(
            candidates,
            Arc::from("local"),
            &mut Inputs {
                signals: &store,
                now_ms: 1_000,
                window_ms: 30_000,
                horizon_ms: 30_000,
                availability: &AvailabilitySettings::default(),
                learned: &mut Learned::default(),
            },
        );

        assert_eq!(
            &*snap.candidates[0].site, "west",
            "idle site should sort ahead of the busy one"
        );
        assert_eq!(&*snap.candidates[1].site, "east");
    }

    #[test]
    fn an_unmeasured_candidate_sorts_after_a_measured_one() {
        let store = LoadStore::new(Duration::from_secs(60));
        store.ingest_at(&line("east", "pool-a", 50.0, 1_000), 1_000, 1_000, "east");
        // west has no sample at all.

        let candidates =
            validate_candidates(vec![cand("llama", "west", "pool-b"), cand("llama", "east", "pool-a")]).unwrap();
        let snap = RouteSnapshot::from_store(
            candidates,
            Arc::from("local"),
            &mut Inputs {
                signals: &store,
                now_ms: 1_000,
                window_ms: 30_000,
                horizon_ms: 30_000,
                availability: &AvailabilitySettings::default(),
                learned: &mut Learned::default(),
            },
        );

        assert_eq!(
            &*snap.candidates[0].site, "east",
            "a measured site beats an unmeasured one"
        );
        assert_eq!(&*snap.candidates[1].site, "west");
    }

    #[test]
    fn cold_start_preserves_config_order() {
        let store = LoadStore::new(Duration::from_secs(60));
        // No signals at all: every candidate is +inf, stable sort keeps input order.
        let candidates =
            validate_candidates(vec![cand("llama", "east", "pool-a"), cand("llama", "west", "pool-b")]).unwrap();
        let snap = RouteSnapshot::from_store(
            candidates,
            Arc::from("local"),
            &mut Inputs {
                signals: &store,
                now_ms: 1_000,
                window_ms: 30_000,
                horizon_ms: 30_000,
                availability: &AvailabilitySettings::default(),
                learned: &mut Learned::default(),
            },
        );

        assert_eq!(
            &*snap.candidates[0].site, "east",
            "cold start keeps the configured order"
        );
        assert_eq!(&*snap.candidates[1].site, "west");
    }

    /// One snapshot of sites `a` and `b` at the given rho, with capacity 100.
    /// Two sites for `m`, each with a learned ceiling of 100, holding `held` in-flight with
    /// `queued` behind it, then shed against `previous`.
    fn shed_with(sites: [(f64, f64); 2], previous: &BTreeSet<Arc<str>>) -> RouteSnapshot {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        // Exact smoothing: the latch is under test, not the estimator.
        let availability = AvailabilitySettings {
            smoothing: 1.0,
            shedding: true,
            full_after_ms: 0,
            room_after_ms: 0,
            ..AvailabilitySettings::default()
        };
        let mut inputs = Inputs {
            signals: &store,
            now_ms: 1_000,
            window_ms: 30_000,
            horizon_ms: 30_000,
            availability: &availability,
            learned: &mut learned,
        };
        let candidates = || validate_candidates(vec![cand("m", "a", "pool-a"), cand("m", "b", "pool-b")]).unwrap();
        // Teach each site a ceiling of 100 first, then publish what it holds now.
        for site in ["a", "b"] {
            with_in_flight(&store, site, 100.0, 1_000);
        }
        RouteSnapshot::from_store(candidates(), Arc::from("hub"), &mut inputs);
        inputs.now_ms = 2_000;
        inputs.window_ms = 500;
        for (site, (held, queued)) in ["a", "b"].iter().zip(sites) {
            let labels = format!(r#"grid_site="{site}",grid_provider="pool-{site}""#);
            with_in_flight(&store, site, held, 2_000);
            store.ingest_at(&format!("{QUEUE_METRIC}{{{labels}}} {queued} 2000"), 2_000, 2_000, site);
        }
        let snapshot = RouteSnapshot::from_store(candidates(), Arc::from("hub"), &mut inputs);
        snapshot.shed(previous, &availability)
    }

    #[test]
    fn fullness_and_room_are_debounced_in_time() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        let availability = AvailabilitySettings {
            smoothing: 1.0,
            shedding: true,
            full_after_ms: 5_000,
            room_after_ms: 2_000,
            ..AvailabilitySettings::default()
        };
        let none = BTreeSet::new();
        let shed = BTreeSet::from([Arc::from("m")]);
        let mut publish = |in_flight: f64, queued: f64, at_ms: i64| {
            with_in_flight(&store, "a", in_flight, at_ms);
            with_queue(&store, "a", queued, at_ms);
            RouteSnapshot::from_store(
                one("a"),
                Arc::from("hub"),
                &mut at(&store, &availability, &mut learned, at_ms, 30_000),
            )
        };
        publish(100.0, 0.0, 1_000);
        // Saturated with a backlog from 2 s: not full until 5 s of it have passed.
        assert!(
            publish(100.0, 5.0, 2_000)
                .shed(&none, &availability)
                .shedding
                .is_empty(),
            "not yet"
        );
        assert!(
            publish(100.0, 5.0, 6_000)
                .shed(&none, &availability)
                .shedding
                .is_empty(),
            "4 s is not 5"
        );
        assert!(
            publish(100.0, 5.0, 7_500)
                .shed(&none, &availability)
                .shedding
                .contains("m"),
            "full for 5.5 s"
        );
        // Below the ceiling with the queue still backed up: room only once no sample in the
        // last 2 s has shown the site full.
        assert!(
            publish(50.0, 5.0, 8_100)
                .shed(&shed, &availability)
                .shedding
                .contains("m"),
            "a dip is not room"
        );
        assert!(
            publish(50.0, 5.0, 10_500)
                .shed(&shed, &availability)
                .shedding
                .is_empty(),
            "below for 2.4 s is room"
        );
    }

    #[test]
    fn a_full_moment_and_a_queued_moment_are_not_crossed_into_congestion() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        let availability = AvailabilitySettings {
            smoothing: 1.0,
            shedding: true,
            full_after_ms: 0,
            room_after_ms: 2_000,
            ..AvailabilitySettings::default()
        };
        // Teach a ceiling of 100, then within one settling time: at the ceiling with nothing
        // waiting, and well below it with a queue. Neither instant is congested.
        with_in_flight(&store, "a", 100.0, 1_000);
        with_queue(&store, "a", 0.0, 1_000);
        RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut learned, 1_000, 30_000),
        );
        with_in_flight(&store, "a", 100.0, 2_000);
        with_queue(&store, "a", 0.0, 2_000);
        with_in_flight(&store, "a", 20.0, 2_500);
        with_queue(&store, "a", 5.0, 2_500);
        let snapshot = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut learned, 2_500, 30_000),
        );
        let a = &snapshot.candidates[0];
        assert_eq!(a.backlog, Some(true), "the newest sample has a queue");
        assert_eq!(a.full, Some(false), "it was never at the ceiling while queued");
        assert!(snapshot.shed(&BTreeSet::new(), &availability).shedding.is_empty());
    }

    fn with_series(store: &LoadStore, site: &str, metric: &str, value: f64, at_ms: i64) {
        let labels = format!(r#"grid_site="{site}",grid_provider="pool-{site}""#);
        store.ingest_at(&format!("{metric}{{{labels}}} {value} {at_ms}"), at_ms, at_ms, site);
    }

    #[test]
    fn work_held_in_flow_control_counts_as_queued() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        let availability = AvailabilitySettings {
            smoothing: 1.0,
            shedding: true,
            full_after_ms: 0,
            room_after_ms: 0,
            ..AvailabilitySettings::default()
        };
        with_in_flight(&store, "a", 40.0, 1_000);
        with_queue(&store, "a", 0.0, 1_000);
        RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut learned, 1_000, 30_000),
        );
        // The engine queue reads empty while flow control holds 60 of the 100 in flight.
        with_in_flight(&store, "a", 100.0, 2_000);
        with_queue(&store, "a", 0.0, 2_000);
        with_series(&store, "a", "llm_d_epp_flow_control_queue_size", 60.0, 2_000);
        let snapshot = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut learned, 2_000, 500),
        );
        let a = &snapshot.candidates[0];
        assert_eq!(a.backlog, Some(true), "held work is a backlog");
        let ceiling = a.capacity.expect("measured");
        assert!(
            (39.0..=40.0).contains(&ceiling),
            "held work teaches no ceiling, got {ceiling}"
        );
        assert_eq!(a.rho, Some(1.0));
        let shed = snapshot.shed(&BTreeSet::new(), &availability);
        assert!(shed.shedding.contains("m"), "full with work held");
    }

    #[test]
    fn one_queued_pod_stops_teaching_without_counting_as_full() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        let availability = AvailabilitySettings {
            smoothing: 1.0,
            shedding: true,
            full_after_ms: 0,
            room_after_ms: 0,
            ..AvailabilitySettings::default()
        };
        with_in_flight(&store, "a", 40.0, 1_000);
        with_queue(&store, "a", 0.0, 1_000);
        RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut learned, 1_000, 30_000),
        );
        // Three new pods report empty queues: the average is 0.25 while one old pod holds 1.
        with_in_flight(&store, "a", 100.0, 2_000);
        with_queue(&store, "a", 0.25, 2_000);
        with_series(&store, "a", "inference_pool_per_pod_queue_size", 1.0, 2_000);
        let snapshot = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut learned, 2_000, 500),
        );
        let a = &snapshot.candidates[0];
        let ceiling = a.capacity.expect("measured");
        assert!(
            (39.0..=40.0).contains(&ceiling),
            "a queued pod means the count is backlog, got {ceiling}"
        );
        assert_eq!(a.backlog, Some(false), "one busy unit is not a full site");
        let shed = snapshot.shed(&BTreeSet::new(), &availability);
        assert!(
            shed.shedding.is_empty(),
            "an affinity-skewed site still has room: {:?}",
            shed.shedding
        );
    }

    #[test]
    fn a_demoted_cluster_is_not_one_of_the_sites_a_shed_waits_on() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        let availability = AvailabilitySettings {
            smoothing: 1.0,
            shedding: true,
            full_after_ms: 0,
            room_after_ms: 0,
            ..AvailabilitySettings::default()
        };
        let candidates = || validate_candidates(vec![cand("m", "a", "pool-a"), cand("m", "b", "pool-b")]).unwrap();
        // Site a is full with work queued; site b is down, its last samples showing room.
        with_in_flight(&store, "a", 100.0, 1_000);
        with_queue(&store, "a", 5.0, 1_000);
        with_in_flight(&store, "b", 10.0, 1_000);
        with_queue(&store, "b", 0.0, 1_000);
        let down = BTreeSet::from([Arc::from("pool-b")]);
        let snapshot = RouteSnapshot::from_store(
            candidates(),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut learned, 1_000, 30_000),
        )
        .demote(&down);
        let shed = snapshot.shed(&BTreeSet::new(), &availability);
        assert!(
            shed.shedding.contains("m"),
            "the down site's room does not count: {:?}",
            shed.shedding
        );
    }

    /// East is live at `now_ms`; west last spoke at 1s, then went silent.
    fn east_live_west_silent(store: &LoadStore, now_ms: i64) {
        with_in_flight(store, "east", 10.0, now_ms);
        with_queue(store, "east", 0.0, now_ms);
        with_in_flight(store, "west", 0.0, 1_000);
        with_queue(store, "west", 0.0, 1_000);
    }

    fn east_west() -> Vec<RouteCandidate> {
        validate_candidates(vec![cand("m", "west", "pool-west"), cand("m", "east", "pool-east")]).unwrap()
    }

    fn picks(snapshot: &RouteSnapshot) -> Vec<(Arc<str>, bool)> {
        (0..20)
            .map(|turn| {
                let pick = crate::route::select_spread(
                    snapshot,
                    CapabilityKind::InferenceModel,
                    "m",
                    turn,
                    |_| true,
                    &crate::route::KeepAll,
                )
                .unwrap();
                (Arc::clone(&pick.candidate.site), pick.fallback)
            })
            .collect()
    }

    #[test]
    fn a_site_silent_past_the_window_is_never_chosen_while_another_is_live() {
        let store = LoadStore::new(Duration::from_secs(600));
        let availability = AvailabilitySettings::default();
        let now_ms = 1_000 + 30_001;
        east_live_west_silent(&store, now_ms);
        let snapshot = RouteSnapshot::from_store(
            east_west(),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut Learned::default(), now_ms, 30_000),
        );
        assert!(snapshot.candidates.iter().any(|c| &*c.site == "west" && c.silent));
        assert!(
            picks(&snapshot)
                .iter()
                .all(|(site, fallback)| &**site == "east" && !fallback)
        );
    }

    #[test]
    fn a_silent_site_takes_no_overflow_and_does_not_hold_off_shedding() {
        let store = LoadStore::new(Duration::from_secs(600));
        let availability = AvailabilitySettings {
            smoothing: 1.0,
            shedding: true,
            full_after_ms: 0,
            room_after_ms: 0,
            ..AvailabilitySettings::default()
        };
        let now_ms = 1_000 + 30_001;
        // East is at its ceiling with work queued; west said it had room, then went silent.
        with_in_flight(&store, "east", 100.0, now_ms);
        with_queue(&store, "east", 5.0, now_ms);
        with_in_flight(&store, "west", 0.0, 1_000);
        with_queue(&store, "west", 0.0, 1_000);
        let snapshot = RouteSnapshot::from_store(
            east_west(),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut Learned::default(), now_ms, 30_000),
        );
        assert!(
            picks(&snapshot).iter().all(|(site, _)| &**site == "east"),
            "overflow stays live"
        );
        let shed = snapshot.shed(&BTreeSet::new(), &availability);
        assert!(shed.shedding.contains("m"), "{:?}", shed.shedding);
    }

    /// This site's operator reports a poll round of `sites` ending at `at_ms`, read at `at_ms`.
    fn rounds(store: &LoadStore, sites: &[&str], at_ms: i64) {
        let text: String = sites
            .iter()
            .map(|site| {
                format!(
                    "{}{{grid_site=\"{site}\",grid_provider=\"{}\"}} 1 {at_ms}\n",
                    grid_signals::poller::METRIC,
                    grid_signals::poller::PROVIDER
                )
            })
            .collect();
        let _ = store.ingest_at(&text, at_ms, at_ms, "hub");
    }

    /// Whether west is silent at `now_ms`, east live, its candidate publishing load or not.
    fn west_silent_at(store: &LoadStore, learned: &mut Learned, publishes_load: bool, now_ms: i64) -> bool {
        with_in_flight(store, "east", 10.0, now_ms);
        let candidates = validate_candidates(vec![
            CandidateConfig {
                publishes_load,
                ..cand("m", "west", "pool-west")
            },
            cand("m", "east", "pool-east"),
        ])
        .unwrap();
        let availability = AvailabilitySettings::default();
        let snapshot = RouteSnapshot::from_store(
            candidates,
            Arc::from("hub"),
            &mut at(store, &availability, learned, now_ms, 30_000),
        );
        snapshot.candidates.iter().any(|c| &*c.site == "west" && c.silent)
    }

    #[test]
    fn a_peer_never_heard_through_a_window_of_rounds_is_silent() {
        let store = LoadStore::new(Duration::from_secs(600)).with_local_site("hub");
        let mut learned = Learned::default();
        rounds(&store, &["east", "west"], 1_000);
        assert!(!west_silent_at(&store, &mut learned, true, 1_000), "the first round");
        rounds(&store, &["east", "west"], 31_001);
        assert!(
            west_silent_at(&store, &mut learned, true, 31_001),
            "a window of rounds and nothing"
        );
    }

    #[test]
    fn a_peer_that_publishes_no_load_is_never_silent_for_saying_nothing() {
        let store = LoadStore::new(Duration::from_secs(600)).with_local_site("hub");
        let mut learned = Learned::default();
        rounds(&store, &["east", "west"], 1_000);
        assert!(!west_silent_at(&store, &mut learned, false, 1_000));
        rounds(&store, &["east", "west"], 31_001);
        assert!(!west_silent_at(&store, &mut learned, false, 31_001));
    }

    #[test]
    fn a_stalled_poller_stops_the_clock_rather_than_silencing_peers() {
        let store = LoadStore::new(Duration::from_secs(600)).with_local_site("hub");
        let mut learned = Learned::default();
        west_speaks(&store, 1_000);
        rounds(&store, &["east", "west"], 1_000);
        // The relay still answers, but the poller has ended no round since.
        let _ = store.ingest_at("", 40_000, 40_000, "hub");
        assert!(!west_silent_at(&store, &mut learned, true, 40_000));
    }

    #[test]
    fn a_peer_is_not_judged_while_its_operator_reports_no_rounds() {
        let store = LoadStore::new(Duration::from_secs(600)).with_local_site("hub");
        let mut learned = Learned::default();
        west_speaks(&store, 1_000);
        let _ = store.ingest_at("", 40_000, 40_000, "hub");
        assert!(!west_silent_at(&store, &mut learned, true, 40_000));
        rounds(&store, &["east", "west"], 40_000);
        assert!(
            west_silent_at(&store, &mut learned, true, 40_000),
            "judged once rounds are reported"
        );
    }

    #[test]
    fn a_candidate_that_publishes_no_load_takes_no_overflow_and_does_not_hold_off_shedding() {
        let store = LoadStore::new(Duration::from_secs(600));
        let availability = AvailabilitySettings {
            smoothing: 1.0,
            shedding: true,
            full_after_ms: 0,
            room_after_ms: 0,
            ..AvailabilitySettings::default()
        };
        let now_ms = 1_000;
        with_in_flight(&store, "east", 100.0, now_ms);
        with_queue(&store, "east", 5.0, now_ms);
        let candidates = validate_candidates(vec![
            CandidateConfig {
                publishes_load: false,
                ..cand("m", "west", "pool-west")
            },
            cand("m", "east", "pool-east"),
        ])
        .unwrap();
        let snapshot = RouteSnapshot::from_store(
            candidates,
            Arc::from("hub"),
            &mut at(&store, &availability, &mut Learned::default(), now_ms, 30_000),
        );
        assert!(picks(&snapshot).iter().all(|(site, _)| &**site == "east"));
        let shed = snapshot.shed(&BTreeSet::new(), &availability);
        assert!(shed.shedding.contains("m"), "{:?}", shed.shedding);
    }

    #[test]
    fn a_sample_aging_out_of_the_store_does_not_count_toward_a_return() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        west_speaks(&store, 1_000);
        assert!(west_at(&store, &mut learned, 31_001).0, "silent");
        assert!(west_at(&store, &mut learned, 62_000).0, "its sample has left the store");
        west_speaks(&store, 62_500);
        assert!(
            west_at(&store, &mut learned, 62_500).0,
            "one sample is still not a return"
        );
    }

    #[test]
    fn a_peer_that_publishes_no_load_is_a_fallback_while_every_site_that_does_is_silent() {
        let store = LoadStore::new(Duration::from_secs(600));
        with_in_flight(&store, "east", 0.0, 1_000);
        with_queue(&store, "east", 0.0, 1_000);
        let candidates = validate_candidates(vec![
            CandidateConfig {
                publishes_load: false,
                ..cand("m", "west", "pool-west")
            },
            cand("m", "east", "pool-east"),
        ])
        .unwrap();
        let availability = AvailabilitySettings::default();
        let snapshot = RouteSnapshot::from_store(
            candidates,
            Arc::from("hub"),
            &mut at(&store, &availability, &mut Learned::default(), 31_001, 30_000),
        );
        assert!(
            picks(&snapshot)
                .iter()
                .all(|(site, fallback)| &**site == "west" && *fallback),
            "east is silent, so west takes it, counted as a fallback"
        );
    }

    #[test]
    fn this_sites_provider_without_metrics_is_no_fallback() {
        let store = LoadStore::new(Duration::from_secs(600));
        with_in_flight(&store, "east", 0.0, 1_000);
        with_queue(&store, "east", 0.0, 1_000);
        let candidates = validate_candidates(vec![
            CandidateConfig {
                publishes_load: false,
                ..cand("m", "hub", "pool-hub")
            },
            cand("m", "east", "pool-east"),
        ])
        .unwrap();
        let availability = AvailabilitySettings::default();
        let snapshot = RouteSnapshot::from_store(
            candidates,
            Arc::from("hub"),
            &mut at(&store, &availability, &mut Learned::default(), 31_001, 30_000),
        );
        assert!(
            picks(&snapshot)
                .iter()
                .all(|(site, fallback)| &**site == "hub" && !*fallback)
        );
    }

    #[test]
    fn a_site_never_heard_from_is_unmeasured_not_silent() {
        let store = LoadStore::new(Duration::from_secs(600));
        let availability = AvailabilitySettings::default();
        let snapshot = RouteSnapshot::from_store(
            east_west(),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut Learned::default(), 60_000, 30_000),
        );
        assert!(snapshot.candidates.iter().all(|c| !c.silent));
        let sites: BTreeSet<Arc<str>> = picks(&snapshot).into_iter().map(|(site, _)| site).collect();
        assert_eq!(sites.len(), 2, "cold start still spreads over both");
    }

    /// West's silence and whether it is picked at `now_ms`, with `learned` carried across calls.
    fn west_at(store: &LoadStore, learned: &mut Learned, now_ms: i64) -> (bool, bool) {
        let availability = AvailabilitySettings::default();
        // East stays live and busy, so a live west would take some of the draw.
        with_in_flight(store, "east", 10.0, now_ms);
        with_queue(store, "east", 5.0, now_ms);
        let snapshot = RouteSnapshot::from_store(
            east_west(),
            Arc::from("hub"),
            &mut at(store, &availability, learned, now_ms, 30_000),
        );
        let silent = snapshot.candidates.iter().any(|c| &*c.site == "west" && c.silent);
        let picked = picks(&snapshot).iter().any(|(site, _)| &**site == "west");
        (silent, picked)
    }

    /// West publishes an idle sample at `at_ms`.
    fn west_speaks(store: &LoadStore, at_ms: i64) {
        with_in_flight(store, "west", 0.0, at_ms);
        with_queue(store, "west", 0.0, at_ms);
    }

    #[test]
    fn a_silent_site_returns_on_its_second_new_sample() {
        let store = LoadStore::new(Duration::from_secs(600));
        let mut learned = Learned::default();
        west_speaks(&store, 1_000);
        assert_eq!(
            west_at(&store, &mut learned, 31_001),
            (true, false),
            "a full window of silence"
        );
        west_speaks(&store, 32_000);
        assert_eq!(
            west_at(&store, &mut learned, 32_000),
            (true, false),
            "one sample is not a return"
        );
        assert_eq!(
            west_at(&store, &mut learned, 33_000),
            (true, false),
            "a refresh is not a sample"
        );
        west_speaks(&store, 34_000);
        assert_eq!(
            west_at(&store, &mut learned, 34_000),
            (false, true),
            "the second sample returns it"
        );
    }

    #[test]
    fn a_site_silent_again_after_returning_needs_two_samples_again() {
        let recorder = Captured::default();
        metrics::with_local_recorder(&recorder, || {
            let store = LoadStore::new(Duration::from_secs(600));
            let mut learned = Learned::default();
            west_speaks(&store, 1_000);
            assert!(west_at(&store, &mut learned, 31_001).0);
            west_speaks(&store, 32_000);
            assert!(west_at(&store, &mut learned, 32_000).0);
            west_speaks(&store, 33_000);
            assert!(!west_at(&store, &mut learned, 33_000).0, "back after two");
            assert!(west_at(&store, &mut learned, 63_001).0, "silent again");
            west_speaks(&store, 64_000);
            assert!(
                west_at(&store, &mut learned, 64_000).0,
                "the count restarts, so one sample is not a return"
            );
        });
        assert_eq!(recorder.count("grid_route_site_silent_transitions_total:silent"), 2);
        assert_eq!(recorder.count("grid_route_site_silent_transitions_total:live"), 1);
    }

    #[test]
    fn a_site_serving_two_models_counts_each_sample_and_transition_once() {
        let recorder = Captured::default();
        metrics::with_local_recorder(&recorder, || {
            let store = LoadStore::new(Duration::from_secs(600));
            let availability = AvailabilitySettings::default();
            let mut learned = Learned::default();
            let two = || {
                validate_candidates(vec![
                    cand("m", "west", "pool-west"),
                    cand("n", "west", "pool-west"),
                    cand("m", "east", "pool-east"),
                ])
                .unwrap()
            };
            let mut silent_at = |now_ms| {
                with_in_flight(&store, "east", 10.0, now_ms);
                let snapshot = RouteSnapshot::from_store(
                    two(),
                    Arc::from("hub"),
                    &mut at(&store, &availability, &mut learned, now_ms, 30_000),
                );
                let west: Vec<bool> = snapshot
                    .candidates
                    .iter()
                    .filter(|c| &*c.site == "west")
                    .map(|c| c.silent)
                    .collect();
                assert_eq!(west.len(), 2);
                west
            };
            west_speaks(&store, 1_000);
            assert_eq!(silent_at(31_001), [true, true]);
            west_speaks(&store, 32_000);
            assert_eq!(
                silent_at(32_000),
                [true, true],
                "one sample, seen by both models, is one"
            );
            west_speaks(&store, 33_000);
            assert_eq!(silent_at(33_000), [false, false]);
        });
        assert_eq!(recorder.count("grid_route_site_silent_transitions_total:silent"), 1);
        assert_eq!(recorder.count("grid_route_site_silent_transitions_total:live"), 1);
    }

    #[test]
    fn when_every_site_is_silent_the_pick_counts_as_a_fallback() {
        let store = LoadStore::new(Duration::from_secs(600));
        let availability = AvailabilitySettings::default();
        west_speaks(&store, 1_000);
        with_in_flight(&store, "east", 0.0, 1_000);
        let snapshot = RouteSnapshot::from_store(
            east_west(),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut Learned::default(), 31_001, 30_000),
        );
        assert!(picks(&snapshot).iter().all(|(_, fallback)| *fallback));
    }

    #[test]
    fn a_link_that_lets_one_poll_through_per_window_never_routes() {
        let store = LoadStore::new(Duration::from_secs(600));
        let mut learned = Learned::default();
        west_speaks(&store, 1_000);
        let mut sample_at = 1_000;
        for _ in 0..5 {
            let quiet_at = sample_at + 30_001;
            assert_eq!(
                west_at(&store, &mut learned, quiet_at),
                (true, false),
                "silent at {quiet_at}"
            );
            sample_at = quiet_at + 1;
            west_speaks(&store, sample_at);
            assert_eq!(
                west_at(&store, &mut learned, sample_at),
                (true, false),
                "one poll at {sample_at}"
            );
        }
    }

    #[test]
    fn a_site_that_misses_polls_within_the_window_never_goes_silent() {
        let store = LoadStore::new(Duration::from_secs(600));
        let mut learned = Learned::default();
        for at_ms in [1_000, 20_000, 49_000] {
            west_speaks(&store, at_ms);
            assert_eq!(
                west_at(&store, &mut learned, at_ms + 29_000),
                (false, true),
                "at {at_ms}"
            );
        }
    }

    #[test]
    fn when_every_site_is_silent_they_read_as_absent() {
        let store = LoadStore::new(Duration::from_secs(600));
        let availability = AvailabilitySettings::default();
        with_in_flight(&store, "east", 0.0, 1_000);
        with_in_flight(&store, "west", 0.0, 1_000);
        let snapshot = RouteSnapshot::from_store(
            east_west(),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut Learned::default(), 1_000 + 30_001, 30_000),
        );
        let sites: BTreeSet<Arc<str>> = picks(&snapshot).into_iter().map(|(site, _)| site).collect();
        assert_eq!(sites.len(), 2, "both stay in the draw, as when nothing is published");
    }

    #[test]
    fn silence_is_published_per_site_with_a_growing_age() {
        let recorder = Captured::default();
        metrics::with_local_recorder(&recorder, || {
            let store = LoadStore::new(Duration::from_secs(600));
            let availability = AvailabilitySettings::default();
            let mut gauged = Gauged::new();
            with_in_flight(&store, "west", 0.0, 1_000);
            for (now_ms, silent, age) in [
                (2_000, 0.0, 1.0),
                (31_000, 0.0, 30.0),
                (31_001, 1.0, 30.001),
                (91_000, 1.0, 90.0),
            ] {
                drop(
                    RouteSnapshot::from_store(
                        one("west"),
                        Arc::from("hub"),
                        &mut at(&store, &availability, &mut gauged.learned, now_ms, 30_000),
                    )
                    .published(&mut gauged),
                );
                assert_eq!(
                    recorder.get("grid_route_site_silent").to_bits(),
                    f64::to_bits(silent),
                    "silent at {now_ms}"
                );
                assert!(
                    (recorder.get("grid_route_site_last_sample_age_seconds") - age).abs() < 1e-9,
                    "age at {now_ms}"
                );
            }
            assert_eq!(recorder.count("grid_route_site_silent_transitions_total:silent"), 1);
            assert_eq!(recorder.count("grid_route_site_silent_transitions_total:live"), 0);
            drop(RouteSnapshot::from_static(Vec::new(), Arc::from("hub")).published(&mut gauged));
            assert!(
                recorder.get("grid_route_site_silent").is_nan(),
                "a site that leaves is cleared"
            );
        });
    }

    /// Thread-local capture of every gauge by name; one site per test.
    #[derive(Default)]
    struct Captured(std::sync::Mutex<BTreeMap<String, Arc<metrics::atomics::AtomicU64>>>);

    impl Captured {
        fn count(&self, name: &str) -> u64 {
            let counters = self.0.lock().unwrap();
            counters
                .get(name)
                .map_or(0, |value| value.load(std::sync::atomic::Ordering::Relaxed))
        }

        fn get(&self, name: &str) -> f64 {
            let gauges = self.0.lock().unwrap();
            gauges.get(name).map_or(f64::NAN, |value| {
                f64::from_bits(value.load(std::sync::atomic::Ordering::Relaxed))
            })
        }
    }

    impl metrics::Recorder for Captured {
        fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}

        fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}

        fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}

        fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
            let to = key
                .labels()
                .find(|label| label.key() == "to")
                .map_or("", |label| label.value());
            let name = format!("{}:{to}", key.name());
            metrics::Counter::from_arc(Arc::clone(self.0.lock().unwrap().entry(name).or_default()))
        }

        fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            let mut gauges = self.0.lock().unwrap();
            metrics::Gauge::from_arc(Arc::clone(gauges.entry(key.name().to_owned()).or_default()))
        }

        fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    #[test]
    fn a_restarted_gateway_relearns_from_its_first_unqueued_sample() {
        // Learned state lives in the process. After a restart every site starts at the floor,
        // reads as saturated, and must earn its ceiling back from the next sample.
        let store = LoadStore::new(Duration::from_secs(60));
        let availability = AvailabilitySettings {
            smoothing: 1.0,
            shedding: true,
            full_after_ms: 0,
            room_after_ms: 0,
            ..AvailabilitySettings::default()
        };
        with_in_flight(&store, "a", 60.0, 1_000);
        with_queue(&store, "a", 5.0, 1_000);
        // Under overload at restart: nothing teaches, the site is full, and it can shed.
        let mut fresh = Learned::default();
        let full = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut fresh, 1_000, 30_000),
        );
        assert_eq!(
            full.candidates[0].capacity,
            Some(AvailabilitySettings::default().ceiling_floor)
        );
        assert_eq!(full.candidates[0].rho, Some(1.0));
        assert!(full.shed(&BTreeSet::new(), &availability).shedding.contains("m"));
        // The queue drains: the next sample teaches the whole ceiling at once.
        with_in_flight(&store, "a", 60.0, 2_000);
        with_queue(&store, "a", 0.0, 2_000);
        let mut relearned = Learned::default();
        let taught = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut relearned, 2_000, 500),
        );
        assert_eq!(
            taught.candidates[0].capacity,
            Some(60.0),
            "one unqueued sample restores the ceiling"
        );
        assert!(taught.shed(&BTreeSet::new(), &availability).shedding.is_empty());
    }

    #[test]
    fn refreshing_one_backlogged_sample_does_not_settle_its_spell() {
        let store = LoadStore::new(Duration::from_secs(60));
        let availability = AvailabilitySettings {
            smoothing: 1.0,
            full_after_ms: 0,
            ..AvailabilitySettings::default()
        };
        with_in_flight(&store, "a", 60.0, 1_000);
        with_queue(&store, "a", 5.0, 1_000);
        let mut learned = Learned::default();
        let first = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut learned, 1_000, 30_000),
        );
        let reread = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut learned, 10_000, 30_000),
        );
        assert_eq!(
            reread.candidates[0].capacity, first.candidates[0].capacity,
            "the refresh clock cannot turn one queued scrape into a hard bound"
        );
        assert_eq!(reread.candidates[0].rho, first.candidates[0].rho);
    }

    #[test]
    fn reported_fullness_uses_the_joined_cache_reading() {
        let reading = SiteReading {
            saturation: Some(0.85),
            ..SiteReading::default()
        };
        assert_eq!(reported_limit(&reading), Some(0.85));
    }

    #[test]
    fn direct_pressure_is_published_without_capacity_and_cleared_on_expiry_or_withdrawal() {
        use std::sync::atomic::Ordering;

        let recorder = ReportedPressure(Arc::default());
        metrics::with_local_recorder(&recorder, || {
            let store = LoadStore::new(Duration::from_secs(60));
            let mut gauged = Gauged::new();
            let availability = AvailabilitySettings::default();
            with_series(&store, "a", "llm_d_epp_average_kv_cache_utilization", 0.85, 1_000);
            for now_ms in [1_000, 2_000] {
                let snapshot = RouteSnapshot::from_store(
                    one("a"),
                    Arc::from("hub"),
                    &mut at(&store, &availability, &mut gauged.learned, now_ms, 500),
                )
                .published(&mut gauged);
                let candidate = &snapshot.candidates[0];
                let reported = f64::from_bits(recorder.0.load(Ordering::Relaxed));
                assert_eq!(candidate.capacity, None);
                if now_ms == 1_000 {
                    assert_eq!(candidate.rho, Some(0.85));
                    assert_eq!(reported.to_bits(), 0.85_f64.to_bits());
                    drop(RouteSnapshot::from_static(Vec::new(), Arc::from("hub")).published(&mut gauged));
                    assert!(f64::from_bits(recorder.0.load(Ordering::Relaxed)).is_nan());
                    drop(snapshot.published(&mut gauged));
                } else {
                    assert_eq!(candidate.rho, None);
                    assert!(reported.is_nan(), "expired direct pressure must not remain visible");
                }
            }
        });
    }

    /// Thread-local capture of the pressure input used without measured capacity.
    struct ReportedPressure(Arc<metrics::atomics::AtomicU64>);

    impl metrics::Recorder for ReportedPressure {
        fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}

        fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}

        fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}

        fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
            metrics::Counter::noop()
        }

        fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            if key.name() == "grid_route_site_rho_reported" {
                metrics::Gauge::from_arc(Arc::clone(&self.0))
            } else {
                metrics::Gauge::noop()
            }
        }

        fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    #[test]
    fn shedding_switched_off_routes_a_full_model() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        let availability = AvailabilitySettings {
            smoothing: 1.0,
            ..AvailabilitySettings::default()
        };
        with_in_flight(&store, "a", 100.0, 1_000);
        with_queue(&store, "a", 5.0, 1_000);
        let snapshot = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut learned, 1_000, 30_000),
        );
        assert_eq!(snapshot.candidates[0].rho, Some(1.0), "full is still measured");
        let previous = BTreeSet::from([Arc::from("m")]);
        let shed = snapshot.shed(&previous, &availability);
        assert!(shed.shedding.is_empty(), "off releases a latch too");
    }

    #[test]
    fn an_expired_candidate_does_not_keep_a_model_shed() {
        let mut candidates = one("a");
        let Some(candidate) = candidates.first_mut() else {
            std::process::abort();
        };
        candidate.expires_at_ms = Some(100_000);
        let mut snapshot = RouteSnapshot::from_static(candidates, Arc::from("hub"));
        snapshot.shedding.insert(Arc::from("m"));

        assert!(
            snapshot.is_shedding("m", 69_999),
            "the unexpired candidate remains shed"
        );
        assert!(
            !snapshot.is_shedding("m", 70_000),
            "the expiry margin releases stale shedding"
        );
    }

    #[test]
    fn a_site_publishing_no_queue_never_sheds_and_still_teaches() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        let availability = AvailabilitySettings {
            smoothing: 1.0,
            shedding: true,
            full_after_ms: 0,
            room_after_ms: 0,
            ..AvailabilitySettings::default()
        };
        let candidates = || validate_candidates(vec![cand("m", "a", "pool-a"), cand("m", "b", "pool-b")]).unwrap();
        // Site a is full with work queued. Site b holds as much but publishes no queue series.
        with_in_flight(&store, "a", 100.0, 1_000);
        with_queue(&store, "a", 5.0, 1_000);
        with_in_flight(&store, "b", 100.0, 1_000);
        let snapshot = RouteSnapshot::from_store(
            candidates(),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut learned, 1_000, 30_000),
        );
        let b = snapshot.candidates.iter().find(|c| &*c.site == "b").expect("site b");
        assert_eq!(b.capacity, Some(100.0), "without a queue, every sample teaches");
        assert_eq!(b.rho, Some(1.0));
        assert_eq!(b.backlog, None);
        let shed = snapshot.shed(&BTreeSet::new(), &availability);
        assert!(
            shed.shedding.is_empty(),
            "a site of unknown queue takes the overflow: {:?}",
            shed.shedding
        );
    }

    #[test]
    fn a_model_sheds_when_every_site_is_at_its_ceiling_with_work_queued() {
        let none = BTreeSet::new();
        let shed = BTreeSet::from([Arc::from("m")]);
        assert!(
            shed_with([(100.0, 5.0), (100.0, 3.0)], &none).shedding.contains("m"),
            "both full and queued"
        );
        assert!(
            shed_with([(100.0, 5.0), (100.0, 0.0)], &none).shedding.is_empty(),
            "at the ceiling with nothing queued is flat out, not full"
        );
        assert!(
            shed_with([(100.0, 5.0), (60.0, 3.0)], &none).shedding.is_empty(),
            "one site has room"
        );
        assert!(
            shed_with([(100.0, 5.0), (97.0, 3.0)], &shed).shedding.contains("m"),
            "97 running and 3 waiting is 100 in flight: still at the ceiling, still shed"
        );
        assert!(
            shed_with([(100.0, 5.0), (90.0, 3.0)], &shed).shedding.is_empty(),
            "90 ends it"
        );
        assert!(
            shed_with([(100.0, 5.0), (100.0, 0.0)], &shed).shedding.is_empty(),
            "an emptied queue ends it"
        );
    }

    /// Ingest `values` for site `a` as consecutive `metric` samples one second apart.
    fn series(metric: &str, values: &[f64]) -> LoadStore {
        let store = LoadStore::new(Duration::from_secs(60));
        let labels = r#"grid_site="a",grid_provider="pool-a""#;
        store.ingest_at(
            &format!("grid_provider_capacity_requests{{{labels}}} 128 1000"),
            1_000,
            1_000,
            "a",
        );
        let mut at: i64 = 1_000;
        for value in values {
            store.ingest_at(&format!("{metric}{{{labels}}} {value} {at}"), at, at, "a");
            store.ingest_at(&format!("llm_d_epp_ready_endpoints{{{labels}}} 1 {at}"), at, at, "a");
            at = at.saturating_add(1_000);
        }
        store
    }

    /// The single candidate resolved from `store` at `now_ms` over a 30s window.
    fn resolved(store: &LoadStore, now_ms: i64) -> RouteCandidate {
        resolved_over(store, now_ms, 30_000)
    }

    /// Site a resolved at `now_ms`, fresh within 30s, looking `horizon_ms` behind its newest sample.
    fn resolved_over(store: &LoadStore, now_ms: i64, horizon_ms: i64) -> RouteCandidate {
        let candidates = validate_candidates(vec![cand("m", "a", "pool-a")]).unwrap();
        let snapshot = RouteSnapshot::from_store(
            candidates,
            Arc::from("hub"),
            &mut Inputs {
                signals: store,
                now_ms,
                window_ms: 30_000,
                horizon_ms,
                availability: &AvailabilitySettings::default(),
                learned: &mut Learned::default(),
            },
        );
        snapshot.candidates[0].clone()
    }

    /// A store with one site publishing `in_flight` at `at_ms`, and capacity 100 published too.
    fn with_in_flight(store: &LoadStore, site: &str, in_flight: f64, at_ms: i64) {
        let labels = format!(r#"grid_site="{site}",grid_provider="pool-{site}""#);
        // Running per unit on one ready unit: in-flight reads as `in_flight` plus any queue.
        for line in [
            format!("llm_d_epp_average_running_requests{{{labels}}} {in_flight} {at_ms}"),
            format!("llm_d_epp_ready_endpoints{{{labels}}} 1 {at_ms}"),
        ] {
            store.ingest_at(&line, at_ms, at_ms, site);
        }
    }

    fn one(site: &str) -> Vec<RouteCandidate> {
        validate_candidates(vec![cand("m", site, &format!("pool-{site}"))]).unwrap()
    }

    fn with_queue(store: &LoadStore, site: &str, queued: f64, at_ms: i64) {
        let labels = format!(r#"grid_site="{site}",grid_provider="pool-{site}""#);
        store.ingest_at(
            &format!("llm_d_epp_average_queue_size{{{labels}}} {queued} {at_ms}"),
            at_ms,
            at_ms,
            site,
        );
    }

    fn at<'store>(
        store: &'store LoadStore,
        availability: &'store AvailabilitySettings,
        learned: &'store mut Learned,
        now_ms: i64,
        window_ms: i64,
    ) -> Inputs<'store> {
        Inputs {
            signals: store,
            now_ms,
            window_ms,
            horizon_ms: window_ms,
            availability,
            learned,
        }
    }

    #[test]
    fn a_queued_sample_teaches_no_ceiling_and_reads_as_full() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        let availability = AvailabilitySettings::default();
        with_in_flight(&store, "a", 40.0, 1_000);
        with_queue(&store, "a", 0.0, 1_000);
        RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut learned, 1_000, 30_000),
        );
        // The engine now holds 400, most of it waiting: the backlog of an open-loop overload.
        with_in_flight(&store, "a", 400.0, 2_000);
        with_queue(&store, "a", 5.0, 2_000);
        let full = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut learned, 2_000, 30_000),
        );
        let ceiling = full.candidates[0].capacity.expect("measured");
        assert!(
            (39.0..=40.0).contains(&ceiling),
            "a queued sample leaves the ceiling at what ran with nothing waiting, got {ceiling}"
        );
        assert_eq!(
            full.candidates[0].rho,
            Some(1.0),
            "the backlog saturates the ceiling it was taught"
        );
        // The queue drains and 10 run. Both reads are worst-in-window, so the backlog is held
        // for the window; once it has aged out, saturation falls toward 10/40 by one step.
        with_in_flight(&store, "a", 10.0, 3_000);
        with_queue(&store, "a", 0.0, 3_000);
        let room = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut at(&store, &availability, &mut learned, 3_000, 500),
        );
        let rho = room.candidates[0].rho.expect("measured");
        assert!(rho < 0.95, "room returns once the queue empties, got {rho}");
    }

    #[test]
    fn a_transient_zero_does_not_make_a_loaded_site_read_as_idle() {
        // The shape measured on a lab site: 120 in flight, one published zero, 108.
        let store = series("llm_d_epp_average_running_requests", &[120.0, 0.0, 108.0]);
        let c = resolved(&store, 3_000);
        assert_eq!(c.capacity, Some(120.0), "the ceiling is the worst sample in the window");
        assert_eq!(c.rho, Some(1.0), "and the site reads as at it, not as idle");
    }

    #[test]
    fn a_site_that_genuinely_drains_reads_idle_once_the_load_leaves_the_horizon() {
        // Samples land at 1s through 5s. A 2.5s horizon behind the newest starts at 2.5s, so
        // only the trailing zeros remain, and nothing was learned before: the floor is the
        // ceiling. The 30s freshness window still reaches the load; the horizon forgets it.
        let store = series("llm_d_epp_average_running_requests", &[120.0, 108.0, 0.0, 0.0, 0.0]);
        let c = resolved_over(&store, 32_500, 2_500);
        assert_eq!(c.rho, Some(0.0), "a drained site is idle, not pinned to an old peak");
        assert_eq!(c.capacity, Some(AvailabilitySettings::default().ceiling_floor));
    }

    #[test]
    fn in_flight_outside_the_window_reads_as_unmeasured() {
        let store = series("llm_d_epp_average_running_requests", &[120.0]);
        assert_eq!(
            resolved(&store, 1_000_000).rho,
            None,
            "a stale sample is absent, not zero"
        );
    }

    #[test]
    fn measured_in_flight_sets_a_learned_ceiling_and_a_saturation_against_it() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        with_in_flight(&store, "a", 40.0, 1_000);
        let first = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut Inputs {
                signals: &store,
                now_ms: 1_000,
                window_ms: 30_000,
                horizon_ms: 30_000,
                availability: &AvailabilitySettings::default(),
                learned: &mut learned,
            },
        );
        assert_eq!(
            first.candidates[0].capacity,
            Some(40.0),
            "the ceiling is the most seen so far"
        );
        assert_eq!(
            first.candidates[0].rho,
            Some(1.0),
            "at its ceiling a site reads as full"
        );
        // Load falls to 10 against the remembered 40: rho moves by `smoothing`, not all the way.
        with_in_flight(&store, "a", 10.0, 2_000);
        let later = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut Inputs {
                signals: &store,
                now_ms: 2_000,
                window_ms: 500,
                horizon_ms: 500,
                availability: &AvailabilitySettings::default(),
                learned: &mut learned,
            },
        );
        let ceiling = later.candidates[0].capacity.unwrap();
        assert!(
            (ceiling - 40.0).abs() < 0.1,
            "a fallen load does not move the ceiling the site taught: {ceiling}"
        );
        let rho = later.candidates[0].rho.unwrap();
        let expected = 10.0 / ceiling * AvailabilitySettings::default().smoothing
            + 1.0 * (1.0 - AvailabilitySettings::default().smoothing);
        assert!(
            (rho - expected).abs() < 1e-9,
            "smoothed, not snapped: {rho} vs {expected}"
        );
    }

    #[test]
    fn a_transient_endpoint_shrink_cannot_teach_a_smaller_hard_ceiling() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        let availability = AvailabilitySettings::default();
        let refresh = |now_ms: i64, state: &mut Learned| {
            RouteSnapshot::from_store(
                one("a"),
                Arc::from("hub"),
                &mut Inputs {
                    signals: &store,
                    now_ms,
                    window_ms: 500,
                    horizon_ms: 500,
                    availability: &availability,
                    learned: state,
                },
            )
        };
        let sample = |at_ms: i64, running: f64, queue: f64, endpoints: f64, kv: f64| {
            let labels = r#"grid_site="a",grid_provider="pool-a""#;
            for (metric, value) in [
                ("llm_d_epp_average_running_requests", running),
                ("llm_d_epp_average_queue_size", queue),
                ("llm_d_epp_ready_endpoints", endpoints),
                ("llm_d_epp_average_kv_cache_utilization", kv),
                ("llm_d_epp_std_dev_kv_cache_utilization", 0.0),
            ] {
                let line = format!("{metric}{{{labels}}} {value} {at_ms}");
                store.ingest_at(&line, at_ms, at_ms, "a");
            }
        };

        sample(1_000, 20.0, 0.0, 2.0, 0.0);
        let baseline = refresh(1_000, &mut learned);
        assert_eq!(baseline.candidates[0].capacity, Some(40.0));

        // While one simulator pod is being replaced, the remaining endpoint is saturated and
        // queued. That transient nine-request sample must not become the two-endpoint site's
        // hard bound.
        sample(2_000, 8.0, 1.0, 1.0, 1.0);
        let transient = refresh(2_000, &mut learned);
        assert_eq!(
            transient.candidates[0].capacity,
            Some(40.0),
            "the stable two-endpoint ceiling remains while one endpoint is briefly absent"
        );

        sample(3_000, 8.0, 1.0, 2.0, 1.0);
        let restored = refresh(3_000, &mut learned);
        assert_eq!(
            restored.candidates[0].capacity,
            Some(18.0),
            "the restored two-endpoint sample teaches its actual saturated in-flight count"
        );
    }

    #[test]
    fn a_refresh_with_no_new_sample_does_not_smooth_again() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        let availability = AvailabilitySettings::default();
        let mut inputs = Inputs {
            signals: &store,
            now_ms: 1_000,
            window_ms: 30_000,
            horizon_ms: 30_000,
            availability: &availability,
            learned: &mut learned,
        };
        with_in_flight(&store, "a", 40.0, 1_000);
        RouteSnapshot::from_store(one("a"), Arc::from("hub"), &mut inputs);
        with_in_flight(&store, "a", 10.0, 2_000);
        inputs.now_ms = 2_000;
        inputs.window_ms = 500;
        let once = RouteSnapshot::from_store(one("a"), Arc::from("hub"), &mut inputs).candidates[0].rho;
        inputs.now_ms = 2_500;
        let again = RouteSnapshot::from_store(one("a"), Arc::from("hub"), &mut inputs).candidates[0].rho;
        assert_eq!(once, again, "the same sample seen twice moves the estimate once");
    }

    #[test]
    fn a_quiet_site_is_floored_so_it_does_not_read_as_full() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        with_in_flight(&store, "a", 1.0, 1_000);
        let snap = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut Inputs {
                signals: &store,
                now_ms: 1_000,
                window_ms: 30_000,
                horizon_ms: 30_000,
                availability: &AvailabilitySettings::default(),
                learned: &mut learned,
            },
        );
        assert_eq!(
            snap.candidates[0].capacity,
            Some(AvailabilitySettings::default().ceiling_floor)
        );
        assert_eq!(
            snap.candidates[0].rho,
            Some(1.0 / AvailabilitySettings::default().ceiling_floor)
        );
    }

    #[test]
    fn a_ceiling_outlives_the_load_that_taught_it() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        let refresh = |now_ms: i64, state: &mut Learned| {
            RouteSnapshot::from_store(
                one("a"),
                Arc::from("hub"),
                &mut Inputs {
                    signals: &store,
                    now_ms,
                    window_ms: 500,
                    horizon_ms: 500,
                    availability: &AvailabilitySettings::default(),
                    learned: state,
                },
            )
        };
        with_in_flight(&store, "a", 80.0, 1_000);
        let mut snapshot = refresh(1_000, &mut learned);
        assert_eq!(
            snapshot.candidates[0].capacity,
            Some(80.0),
            "the first sample teaches 80"
        );
        for minute in 1..=10 {
            let at_ms = 1_000 + i64::from(minute) * 60_000;
            with_in_flight(&store, "a", 8.0, at_ms);
            snapshot = refresh(at_ms, &mut learned);
        }
        assert_eq!(
            snapshot.candidates[0].capacity,
            Some(80.0),
            "ten minutes at a tenth of the load must not bring the taught ceiling down"
        );
        let rho = snapshot.candidates[0].rho.unwrap();
        assert!(rho < 0.2, "a tenth of the taught ceiling reads light, not full: {rho}");
    }

    #[test]
    fn a_site_that_leaves_the_topology_loses_the_ceiling_it_taught() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut gauged = Gauged::new();
        let refresh = |candidates: Vec<RouteCandidate>, at_ms: i64, state: &mut Gauged| {
            let snapshot = RouteSnapshot::from_store(
                candidates,
                Arc::from("hub"),
                &mut Inputs {
                    signals: &store,
                    now_ms: at_ms,
                    window_ms: 500,
                    horizon_ms: 500,
                    availability: &AvailabilitySettings::default(),
                    learned: &mut state.learned,
                },
            );
            snapshot.published(state)
        };
        let both = validate_candidates(vec![cand("m", "a", "pool-a"), cand("m", "b", "pool-b")]).unwrap();
        with_in_flight(&store, "a", 80.0, 1_000);
        with_in_flight(&store, "b", 80.0, 1_000);
        let taught = refresh(both, 1_000, &mut gauged);
        assert_eq!(taught.candidates.len(), 2, "both sites are taught");
        // b leaves, then rejoins smaller. Keeping its ceiling would resume it untaught.
        drop(refresh(one("a"), 1_500, &mut gauged));
        with_in_flight(&store, "b", 2.0, 2_000);
        let rejoined = refresh(
            validate_candidates(vec![cand("m", "a", "pool-a"), cand("m", "b", "pool-b")]).unwrap(),
            2_000,
            &mut gauged,
        );
        let b = rejoined
            .candidates
            .iter()
            .find(|candidate| &*candidate.site == "b")
            .unwrap();
        assert_eq!(
            b.capacity,
            Some(AvailabilitySettings::default().ceiling_floor),
            "a site that rejoins starts untaught, at the floor"
        );
    }

    #[test]
    fn an_unproven_site_keeps_a_quarter_of_the_largest_ceiling() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        with_in_flight(&store, "a", 120.0, 1_000);
        with_in_flight(&store, "b", 2.0, 1_000);
        let candidates = validate_candidates(vec![cand("m", "a", "pool-a"), cand("m", "b", "pool-b")]).unwrap();
        let snap = RouteSnapshot::from_store(
            candidates,
            Arc::from("hub"),
            &mut Inputs {
                signals: &store,
                now_ms: 1_000,
                window_ms: 30_000,
                horizon_ms: 30_000,
                availability: &AvailabilitySettings::default(),
                learned: &mut learned,
            },
        );
        let by: BTreeMap<&str, f64> = snap
            .candidates
            .iter()
            .map(|c| (&*c.site, c.capacity.unwrap()))
            .collect();
        assert!((by["a"] - 120.0).abs() < 1e-9, "{by:?}");
        assert!(
            (by["b"] - 30.0).abs() < 1e-9,
            "floored to AvailabilitySettings::default().explore_floor of the largest ceiling, above its own 8: {by:?}"
        );
    }
}
