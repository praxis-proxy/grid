//! The route snapshot the request path reads, and how live load orders it.
//!
//! A snapshot is a pre-ordered candidate list plus the local site. The request
//! path loads one snapshot and takes the front admitted match, doing no store
//! read or scoring itself. Ordering by live load is a control step
//! ([`RouteSnapshot::from_store`]) run off the request path, so the hot path
//! snapshots resolved order once rather than reading raw signals per request.

use std::sync::Arc;

use grid_signals::LoadStore;

use crate::descriptor::RouteCandidate;

/// Load metric that orders candidates. Lower queue depth is a better target.
pub(crate) const LOAD_METRIC: &str = "inference_pool_average_queue_size";

/// Lower load is a better routing target.
const LOWER_IS_BETTER: bool = true;

/// A resolved, pre-ordered candidate list read atomically per request.
#[derive(Debug)]
pub struct RouteSnapshot {
    /// Candidates in priority order. `select_admitted` takes the front match.
    pub candidates: Vec<RouteCandidate>,

    /// Each candidate's windowed worst queue depth, `+inf` when unmeasured, parallel to `candidates`.
    pub loads: Vec<f64>,

    /// This gateway's own site identifier.
    pub local_site: Arc<str>,
}

impl RouteSnapshot {
    /// Wrap candidates in their given order, without consulting live load.
    ///
    /// The order is whatever the caller supplies (config order). Used before
    /// any signals exist and as the cold-start fallback.
    pub fn from_static(candidates: Vec<RouteCandidate>, local_site: Arc<str>) -> Self {
        let loads = vec![f64::INFINITY; candidates.len()];
        Self {
            candidates,
            loads,
            local_site,
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
    pub fn from_store(
        candidates: Vec<RouteCandidate>,
        local_site: Arc<str>,
        store: &LoadStore,
        now_ms: i64,
        window_ms: i64,
    ) -> Self {
        // Score each candidate once, then sort the pairs: load_of allocates a
        // store key and scans a window, too costly to repeat inside sort_by.
        let mut scored: Vec<(f64, RouteCandidate)> = candidates
            .into_iter()
            .map(|candidate| (Self::load_of(store, &candidate, now_ms, window_ms), candidate))
            .collect();
        scored.sort_by(|(left, _), (right, _)| left.total_cmp(right));
        let (loads, ordered) = scored.into_iter().unzip();
        Self {
            candidates: ordered,
            loads,
            local_site,
        }
    }

    /// The candidate's worst recent load, or `+inf` when it has no fresh sample.
    ///
    /// `+inf` makes an unmeasured candidate sort last under an ascending order.
    fn load_of(store: &LoadStore, candidate: &RouteCandidate, now_ms: i64, window_ms: i64) -> f64 {
        let key = LoadStore::key(&candidate.site, &candidate.cluster);
        store
            .window_worst(&key, LOAD_METRIC, now_ms, window_ms, LOWER_IS_BETTER)
            .unwrap_or(f64::INFINITY)
    }
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::min_ident_chars,
    reason = "tests"
)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::descriptor::{CandidateConfig, CapabilityKind, validate_candidates};

    /// One QUEUE sample for `site`/`cluster` at `at_ms`.
    fn line(site: &str, cluster: &str, value: f64, at_ms: i64) -> String {
        format!(r#"{LOAD_METRIC}{{grid_site="{site}",grid_provider="{cluster}"}} {value} {at_ms}"#)
    }

    /// A validated candidate for model `name` served by `site`/`cluster`.
    fn cand(name: &str, site: &str, cluster: &str) -> CandidateConfig {
        CandidateConfig {
            cluster: cluster.to_owned(),
            credential: None,
            fresh: true,
            kind: CapabilityKind::InferenceModel,
            name: name.to_owned(),
            site: site.to_owned(),
        }
    }

    #[test]
    fn the_least_loaded_site_sorts_first() {
        let store = LoadStore::new(Duration::from_secs(60));
        // Same model on two sites: east is busy (90), west is idle (10).
        store.ingest_at(&line("east", "pool-a", 90.0, 1_000), 1_000, 1_000, "east");
        store.ingest_at(&line("west", "pool-b", 10.0, 1_000), 1_000, 1_000, "west");

        let candidates =
            validate_candidates(vec![cand("llama", "east", "pool-a"), cand("llama", "west", "pool-b")]).unwrap();
        let snap = RouteSnapshot::from_store(candidates, Arc::from("local"), &store, 1_000, 30_000);

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
        let snap = RouteSnapshot::from_store(candidates, Arc::from("local"), &store, 1_000, 30_000);

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
        let snap = RouteSnapshot::from_store(candidates, Arc::from("local"), &store, 1_000, 30_000);

        assert_eq!(
            &*snap.candidates[0].site, "east",
            "cold start keeps the configured order"
        );
        assert_eq!(&*snap.candidates[1].site, "west");
    }
}
