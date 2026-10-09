//! The route snapshot the request path reads, and how live load orders it.
//!
//! A snapshot is a pre-ordered candidate list plus the local site. The request
//! path loads one snapshot and takes the front admitted match, doing no store
//! read or scoring itself. Ordering by live load is a control step
//! ([`RouteSnapshot::from_store`]) run off the request path, so the hot path
//! snapshots resolved order once rather than reading raw signals per request.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use grid_signals::LoadStore;

use crate::{
    decisions::SiteDecisions,
    descriptor::{AdmissionState, CapabilityKind, RouteCandidate},
    serving::AvailabilitySettings,
    signals::{Over, SiteReading, SiteSignals},
};


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
        let ranked = candidates
            .into_iter()
            .map(|candidate| Self::resolve(candidate, inputs))
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
        inputs: &mut Inputs<'_, S>,
    ) -> (f64, RouteCandidate, bool) {
        let (reading, recent) = Self::readings(&candidate, inputs);
        if reading.unready {
            candidate.admission_state = AdmissionState::Excluded;
        }
        // The site as a whole had work waiting at some instant of the settling time: its
        // average queue at queue_full, or any work held before scheduling, which is a count
        // and blocks admission at one. One busy unit is not a full site; it only stops a
        // sample teaching, in measured. Unknown without a queue reading.
        candidate.backlog = recent.queued.map(|_| recent.congested_in_flight.is_some());
        // A site's own in-flight count against the ceiling it has shown. A site that publishes
        // none has no room and is chosen only when no site has any.
        let measured = Self::measured(&candidate, &reading, &recent, inputs);
        if let Some(measured) = &measured {
            candidate.capacity = Some(measured.ceiling);
            candidate.rho = Some(measured.rho);
            candidate.full = measured.full;
            candidate.relieved = measured.relieved;
        }
        // Ordered by queue depth; a site publishing none sorts after every site that does.
        let load = if candidate.admission_state == AdmissionState::NewAndExisting {
            reading.queued.unwrap_or(f64::INFINITY)
        } else {
            UNHEALTHY
        };
        (load, candidate, measured.is_some())
    }

    /// The site over the load window, and over the settling time alone: `room_after_ms`,
    /// stretched to reach the site's newest sample so a site polled less often than that is
    /// still judged on what it last said. Fullness is judged from the second, so one spike a
    /// window ago does not hold a site full.
    fn readings<S: SiteSignals>(candidate: &RouteCandidate, inputs: &Inputs<'_, S>) -> (SiteReading, SiteReading) {
        let over = |window_ms| Over {
            now_ms: inputs.now_ms,
            window_ms,
            queue_full: inputs.availability.queue_full,
        };
        let reading = inputs
            .signals
            .read(&candidate.site, &candidate.cluster, over(inputs.window_ms));
        let newest = reading
            .sampled_at
            .map_or(0, |at| inputs.now_ms.saturating_sub(at).saturating_add(1));
        let settle = inputs.availability.room_after_ms.max(newest);
        let recent = inputs.signals.read(&candidate.site, &candidate.cluster, over(settle));
        (reading, recent)
    }

    /// Lift every measured site's weight to at least `share` of the largest ceiling.
    ///
    /// A ceiling only grows when a site carries load, and a site weighted by a small ceiling
    /// carries little. Without a floor an unproven site would never get the chance to prove more.
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

    /// The site's learned ceiling, smoothed saturation, and fullness from its own in-flight
    /// count, or `None` when it publishes no in-flight sample in the window.
    ///
    /// The ceiling is the most the site has held, decaying with `ceiling_half_life_ms` so a site that
    /// shrank is not judged against a peak it no longer reaches, and floored so a quiet site
    /// does not read as full. Saturation is in-flight over that ceiling, smoothed so one new
    /// sample moves it by `smoothing` rather than all the way; a refresh that brings no
    /// new sample leaves it where it was. Full is at the ceiling with a backlog, by `recent`,
    /// for `full_after_ms`; room is no sample in `recent` showing that. The smoothing, and
    /// reading an absent sample as absent rather than as zero load, follow Brent Salisbury's
    /// pressure-weighted placement; the learned ceiling in place of a declared capacity is new.
    fn measured<S: SiteSignals>(
        candidate: &RouteCandidate,
        reading: &SiteReading,
        recent: &SiteReading,
        inputs: &mut Inputs<'_, S>,
    ) -> Option<Measured> {
        let in_flight = reading.in_flight?;
        let availability = inputs.availability;
        let key = (Arc::clone(&candidate.site), Arc::clone(&candidate.cluster));
        // The newest sample at or before now is the epoch a smoothing step belongs to.
        let sample_at = reading.sampled_at.map(|at| at.min(inputs.now_ms));
        let previous = inputs.learned.0.get(&key).copied();
        let ceiling = ceiling(previous, reading, inputs.now_ms, availability);
        let raw = (in_flight / ceiling).clamp(0.0, 1.0);
        let rho = smoothed(previous, sample_at, raw, availability.smoothing);
        // Congested: at the ceiling at an instant within the settling time that also had a
        // backlog, read together so a full moment and a queued moment are not crossed.
        let congested = recent.congested_in_flight.is_some_and(|held| held >= ceiling);
        let full_since = congested_since(previous, congested, inputs.now_ms, inputs.window_ms);
        let full = candidate
            .backlog
            .map(|_| full_since.is_some_and(|since| inputs.now_ms.saturating_sub(since) >= availability.full_after_ms));
        site_gauge("grid_route_site_ceiling", &key, ceiling);
        inputs.learned.0.insert(
            key,
            SiteLearned {
                ceiling,
                rho,
                at_ms: inputs.now_ms,
                sample_at,
                full_since,
            },
        );
        Some(Measured {
            ceiling,
            rho,
            full,
            relieved: candidate.backlog.map(|_| !congested),
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
        // A demoted cluster carries a NaN score and is not one of the model's sites here.
        let healthy = self.candidates.iter().zip(&self.scores).filter(|(candidate, score)| {
            candidate.kind == CapabilityKind::InferenceModel
                && candidate.admission_state == AdmissionState::NewAndExisting
                && !score.is_nan()
        });
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
    pub(crate) fn published(self, published: &mut BTreeSet<SitePair>) -> Self {
        let mut current = BTreeSet::new();
        for (candidate, score) in self.candidates.iter().zip(&self.scores) {
            let key = (Arc::clone(&candidate.site), Arc::clone(&candidate.cluster));
            // The front entry for a pair is its best; later ones are other models.
            if current.insert(key.clone()) {
                site_score(&key, *score);
                site_gauge("grid_route_site_rho", &key, candidate.rho.unwrap_or(f64::NAN));
                site_gauge("grid_route_site_weight", &key, candidate.capacity.unwrap_or(f64::NAN));
                // The ceiling is set where it is learned; an unmeasured site must not keep
                // showing the last one it had, or a seed taken during an outage reads it.
                if candidate.capacity.is_none() {
                    site_gauge("grid_route_site_ceiling", &key, f64::NAN);
                }
            }
        }
        for gone in published.difference(&current) {
            site_score(gone, f64::NAN);
            site_gauge("grid_route_site_rho", gone, f64::NAN);
            site_gauge("grid_route_site_weight", gone, f64::NAN);
            site_gauge("grid_route_site_ceiling", gone, f64::NAN);
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
    /// The most requests the site has been seen to hold, decayed by
    /// [`AvailabilitySettings::ceiling_half_life_ms`].
    ceiling: f64,
    /// Saturation against that ceiling, smoothed by [`AvailabilitySettings::smoothing`].
    rho: f64,
    /// When `ceiling` was last updated, for the decay.
    at_ms: i64,
    /// The newest sample the saturation was smoothed from, so a refresh with nothing new
    /// does not smooth again.
    sample_at: Option<i64>,
    /// When the site last reached its ceiling with work waiting and has stayed so since, for
    /// `full_after_ms`.
    full_since: Option<i64>,
}

/// What one refresh measured about a site.
struct Measured {
    /// The learned ceiling.
    ceiling: f64,
    /// Smoothed saturation against it.
    rho: f64,
    /// At the ceiling with a backlog for `full_after_ms`; `None` without a queue reading.
    full: Option<bool>,
    /// No sample in the settling time at the ceiling with a backlog; `None` without a queue reading.
    relieved: Option<bool>,
}

/// Per-site learned load state, kept across refreshes.
#[derive(Clone, Debug, Default)]
pub(crate) struct Learned(BTreeMap<SitePair, SiteLearned>);

/// Control state that outlives one snapshot: the pairs with a published score, and what each
/// site's load has taught the gateway.
#[derive(Debug, Default)]
pub(crate) struct Gauged {
    /// `(site, cluster)` pairs with a published score.
    pub(crate) published: BTreeSet<SitePair>,
    /// Learned ceilings and smoothed saturation per site.
    pub(crate) learned: Learned,
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

/// The ceiling `reading` teaches over what `prior` held, decayed, and floored.
///
/// In-flight counts what the engine holds waiting as well as running, so a sample taken with
/// one whole request waiting anywhere, at the site, at its busiest unit, or held before
/// scheduling, measures the backlog, not the capacity, and teaches no ceiling. It still reads
/// against the ceiling learned while nothing waited, where it saturates.
fn ceiling(prior: Option<SiteLearned>, reading: &SiteReading, now_ms: i64, availability: &AvailabilitySettings) -> f64 {
    let decayed = prior.map_or(0.0, |prior| decayed(prior, now_ms, availability.ceiling_half_life_ms));
    let waiting = [reading.queued, reading.deepest_queue, reading.held]
        .into_iter()
        .any(|queue| queue.is_some_and(|queue| queue >= 1.0));
    let taught = if waiting { 0.0 } else { reading.in_flight.unwrap_or(0.0) };
    decayed.max(taught).max(availability.ceiling_floor)
}

/// When the site's current spell of congestion began, if it is congested at `now_ms`: carried
/// from `prior` while the site was measured within the window, else starting now, so a site
/// unmeasured for longer than the window does not resume a spell from before the gap.
fn congested_since(prior: Option<SiteLearned>, congested: bool, now_ms: i64, window_ms: i64) -> Option<i64> {
    congested.then(|| {
        prior
            .filter(|prior| now_ms.saturating_sub(prior.at_ms) <= window_ms)
            .and_then(|prior| prior.full_since)
            .unwrap_or(now_ms)
    })
}

/// `prior`'s ceiling after the time since it was set, halving every `half_life_ms`.
fn decayed(prior: SiteLearned, now_ms: i64, half_life_ms: i64) -> f64 {
    let elapsed = u32::try_from(now_ms.saturating_sub(prior.at_ms).max(0)).unwrap_or(u32::MAX);
    let half_lives = f64::from(elapsed) / f64::from(u32::try_from(half_life_ms).unwrap_or(u32::MAX));
    prior.ceiling * 0.5_f64.powf(half_lives)
}

/// Set `grid_route_site_score` for one site/cluster pair.
fn site_score(key: &SitePair, score: f64) {
    site_gauge("grid_route_site_score", key, score);
}

/// Set a per-site gauge: the learned ceiling, the smoothed saturation, or the weight.
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
            admission: AdmissionState::default(),
            cluster: cluster.to_owned(),
            credential: None,
            fresh: true,
            kind: CapabilityKind::InferenceModel,
            name: name.to_owned(),
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
        let candidates = validate_candidates(vec![cand("m", "a", "pool-a")]).unwrap();
        let snapshot = RouteSnapshot::from_store(
            candidates,
            Arc::from("hub"),
            &mut Inputs {
                signals: store,
                now_ms,
                window_ms: 30_000,
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
    fn a_site_that_genuinely_drains_reads_idle_once_the_load_ages_out() {
        // Samples land at 1s, 2s and 3s. Read at 32.5s the 30s window starts at 2.5s, so only
        // the trailing zero remains, and nothing was learned before: the floor is the ceiling.
        let store = series("llm_d_epp_average_running_requests", &[120.0, 108.0, 0.0]);
        let c = resolved(&store, 32_500);
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
        // Load falls to 10 against the remembered ceiling (40, less one second of decay):
        // rho moves by AvailabilitySettings::default().smoothing toward 10/ceiling rather than all the way.
        with_in_flight(&store, "a", 10.0, 2_000);
        let later = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut Inputs {
                signals: &store,
                now_ms: 2_000,
                window_ms: 500,
                availability: &AvailabilitySettings::default(),
                learned: &mut learned,
            },
        );
        let ceiling = later.candidates[0].capacity.unwrap();
        assert!(
            (ceiling - 40.0).abs() < 0.1,
            "one second barely decays a 10 minute half-life: {ceiling}"
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
    fn a_refresh_with_no_new_sample_does_not_smooth_again() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        let availability = AvailabilitySettings::default();
        let mut inputs = Inputs {
            signals: &store,
            now_ms: 1_000,
            window_ms: 30_000,
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
    fn the_ceiling_decays_by_its_half_life_once_load_falls_away() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut learned = Learned::default();
        with_in_flight(&store, "a", 80.0, 1_000);
        RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut Inputs {
                signals: &store,
                now_ms: 1_000,
                window_ms: 30_000,
                availability: &AvailabilitySettings::default(),
                learned: &mut learned,
            },
        );
        with_in_flight(
            &store,
            "a",
            10.0,
            1_000 + AvailabilitySettings::default().ceiling_half_life_ms,
        );
        let snap = RouteSnapshot::from_store(
            one("a"),
            Arc::from("hub"),
            &mut Inputs {
                signals: &store,
                now_ms: 1_000 + AvailabilitySettings::default().ceiling_half_life_ms,
                window_ms: 500,
                availability: &AvailabilitySettings::default(),
                learned: &mut learned,
            },
        );
        let ceiling = snap.candidates[0].capacity.unwrap();
        assert!(
            (ceiling - 40.0).abs() < 1e-6,
            "80 halves to 40 after one half-life: {ceiling}"
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
