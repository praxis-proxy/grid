//! Keep a request on the sites that hold its prefix, unless load says otherwise.
//!
//! Follows the prefix-cache-affinity filter in llm-d-router
//! (pkg/epp/framework/plugins/scheduling/filter/prefixcacheaffinity), applied to
//! sites: scores come from this replica's own index, and a [`LoadGate`] weighs
//! the prefill a match saves against how much busier the sticky site is.

use std::sync::LazyLock;

use serde::Deserialize;
use xxhash_rust::xxh64::xxh64;

use super::{BLOCK, PrefixIndex, PrefixKeys};

/// One admitted site selection would consider.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Site<'snap> {
    /// The site's backend cluster, the index's candidate key.
    pub(crate) cluster: &'snap str,
    /// The site's windowed worst queue depth, `+inf` when unmeasured.
    pub(crate) queue: f64,
}

/// What affinity needs to know about a site selection offers: its cluster and its queue depth.
///
/// A trait rather than a conversion, so selection's own view passes through on the request path.
pub(crate) trait Queued {
    /// The site's backend cluster, the index's candidate key.
    fn cluster(&self) -> &str;
    /// The site's windowed worst queue depth, `+inf` when unmeasured.
    fn queue(&self) -> f64;
}

impl Queued for Site<'_> {
    fn cluster(&self) -> &str {
        self.cluster
    }

    fn queue(&self) -> f64 {
        self.queue
    }
}

/// Whether load outweighs a match, in whatever load signal selection orders by.
pub(crate) trait LoadGate {
    /// Whether `stuck`, the best sticky site, is busier than `other`, the best
    /// other site, by more than `saved` seconds of prefill.
    fn outweighs(&self, stuck: &Site<'_>, other: &Site<'_>, saved: f64) -> bool;
}

/// Weighs queue depth, taking each queued request to add a fixed wait.
#[derive(Clone, Copy, Debug)]
pub(crate) struct QueueGate {
    /// Seconds one queued request adds to a new request's wait.
    pub(crate) queued_request_seconds: f64,
}

impl LoadGate for QueueGate {
    fn outweighs(&self, stuck: &Site<'_>, other: &Site<'_>, saved: f64) -> bool {
        // An unmeasured sticky site loses to a measured one; two unmeasured sites compare as NaN and stay.
        stuck.queue - other.queue > saved / self.queued_request_seconds
    }
}

/// Prefix affinity settings from the grid serving config.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct AffinitySettings {
    /// Whether selection prefers the site holding a request's prompt. Off routes on load alone.
    pub enabled: bool,
    /// Share of the request's keys a site must hold to be sticky.
    pub threshold: f64,
    /// Share of requests that skip affinity, so a second site learns a shared prefix.
    pub exploration: f64,
    /// Prompt tokens a site prefills per second, to price the cache a match saves.
    pub prefill_tokens_per_second: f64,
    /// Seconds one queued request adds to a new request's wait, to turn that price into queue depth.
    pub queued_request_seconds: f64,
    /// A file holding the key that authenticates stored-state tags. Without one, tags carry no mac.
    pub tag_key_path: Option<String>,
}

impl Default for AffinitySettings {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold: 0.8,
            exploration: 0.02,
            prefill_tokens_per_second: 10_000.0,
            queued_request_seconds: 2.0,
            tag_key_path: None,
        }
    }
}

impl AffinitySettings {
    /// Refuse settings outside their ranges.
    ///
    /// # Errors
    ///
    /// Returns a message naming the first field out of range.
    pub(crate) fn validate(&self) -> Result<(), String> {
        if !(self.threshold > 0.0 && self.threshold <= 1.0) {
            return Err(format!(
                "prefix_affinity.threshold must be in (0, 1], got {}",
                self.threshold
            ));
        }
        if !(0.0..=1.0).contains(&self.exploration) {
            return Err(format!(
                "prefix_affinity.exploration must be in [0, 1], got {}",
                self.exploration
            ));
        }
        for (name, value) in [
            ("prefill_tokens_per_second", self.prefill_tokens_per_second),
            ("queued_request_seconds", self.queued_request_seconds),
        ] {
            if !(value.is_finite() && value > 0.0) {
                return Err(format!("prefix_affinity.{name} must be above 0, got {value}"));
            }
        }
        Ok(())
    }
}

/// What affinity did for one request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// Narrowed to the sites holding the most of the prefix.
    Sticky,
    /// Keys, but no site held enough of them.
    NoMatch,
    /// Sticky sites existed, but the load gate kept every site.
    LoadOverride,
    /// Skipped for exploration.
    Exploration,
    /// No keys, so nothing to match.
    NotApplicable,
}

impl Outcome {
    /// The metric label.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Sticky => "sticky",
            Self::NoMatch => "no_match",
            Self::LoadOverride => "load_override",
            Self::Exploration => "exploration",
            Self::NotApplicable => "not_applicable",
        }
    }

    /// Count the decision. The label set is fixed, so the series are bounded.
    pub(crate) fn record(self) {
        // Registered once; counting an outcome is one atomic add on the request path.
        static COUNTERS: LazyLock<[metrics::Counter; 5]> = LazyLock::new(|| {
            [
                Outcome::Sticky,
                Outcome::NoMatch,
                Outcome::LoadOverride,
                Outcome::Exploration,
                Outcome::NotApplicable,
            ]
            .map(|outcome| metrics::counter!("grid_route_prefix_affinity_total", "outcome" => outcome.as_str()))
        });
        let index = match self {
            Self::Sticky => 0,
            Self::NoMatch => 1,
            Self::LoadOverride => 2,
            Self::Exploration => 3,
            Self::NotApplicable => 4,
        };
        if let Some(counter) = COUNTERS.get(index) {
            counter.increment(1);
        }
    }
}

/// A match this long is sticky whatever share of the request it is: 8 KiB, about 2k tokens.
const FLOOR_KEYS: usize = 32;

/// A match shorter than this saves too little prefill to keep: 1 KiB, about 256 tokens.
/// Without it, identical short prompts would all follow the first site.
const MIN_KEYS: usize = 4;

/// One request's affinity: its keys, the index to match them in, and the settings.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Affinity<'req> {
    /// This replica's index.
    pub(crate) index: &'req PrefixIndex,
    /// The request's keys, `None` when it has no readable prompt.
    pub(crate) keys: Option<&'req PrefixKeys>,
    /// Threshold, exploration and the prefill price.
    pub(crate) settings: &'req AffinitySettings,
    /// The request's selection turn, for exploration.
    pub(crate) turn: usize,
}

impl Affinity<'_> {
    /// Clear `keep` for every site outside the sticky set, or leave it when `gate`
    /// or exploration reopens the set. `keep` is all true on entry.
    pub(crate) fn narrow(&self, sites: &[impl Queued], keep: &mut [bool], gate: &impl LoadGate) -> Outcome {
        let Some(keys) = self.keys else {
            return Outcome::NotApplicable;
        };
        if explores(self.turn, self.settings.exploration) {
            return Outcome::Exploration;
        }
        let depths = self.index.depths(keys, sites.iter().map(Queued::cluster));
        let deepest = depths.iter().copied().max().unwrap_or(0);
        let total = keys.as_slice().len();
        let qualifies = deepest >= FLOOR_KEYS || share(deepest, total) >= self.settings.threshold;
        if deepest < MIN_KEYS || !qualifies {
            return Outcome::NoMatch;
        }
        // Deepest first, so a site holding only a shared system prompt cannot pull a conversation away.
        if self.overloaded(sites, &depths, deepest, gate) {
            return Outcome::LoadOverride;
        }
        for (slot, depth) in keep.iter_mut().zip(depths) {
            *slot = *slot && depth == deepest;
        }
        Outcome::Sticky
    }

    /// Whether load outweighs the match: the best sticky site is busier than the
    /// best other site by more than the prefill the match saves.
    fn overloaded(&self, sites: &[impl Queued], depths: &[usize], depth: usize, gate: &impl LoadGate) -> bool {
        let best = |pick: bool| {
            sites
                .iter()
                .zip(depths)
                .filter(|(_, held)| (**held == depth) == pick)
                .min_by(|left, right| left.0.queue().total_cmp(&right.0.queue()))
                .map(|(site, _)| site)
        };
        let (Some(stuck), Some(other)) = (best(true), best(false)) else {
            return false;
        };
        // Tokens estimated at 4 bytes each, as the canonical stream is.
        let saved = tokens(depth) / self.settings.prefill_tokens_per_second;
        // Two stack values for the gate; the view type stays selection's own.
        let stuck = Site {
            cluster: stuck.cluster(),
            queue: stuck.queue(),
        };
        let other = Site {
            cluster: other.cluster(),
            queue: other.queue(),
        };
        gate.outweighs(&stuck, &other, saved)
    }
}

/// Prompt tokens in `keys` dense keys.
fn tokens(keys: usize) -> f64 {
    f64::from(u32::try_from(keys.saturating_mul(BLOCK / 4)).unwrap_or(u32::MAX))
}

/// `matched` of `total` keys, as a share.
fn share(matched: usize, total: usize) -> f64 {
    let ratio = u32::try_from(matched).map_or(1.0, f64::from) / u32::try_from(total).map_or(f64::MAX, f64::from);
    ratio.min(1.0)
}

/// Whether request `turn` explores, a stable coin at `probability`.
fn explores(turn: usize, probability: f64) -> bool {
    if probability <= 0.0 {
        return false;
    }
    let coin = xxh64(&turn.to_le_bytes(), EXPLORE) % 1_000_000;
    f64::from(u32::try_from(coin).unwrap_or(u32::MAX)) < probability * 1_000_000.0
}

/// Seeds the exploration coin apart from the two-choice pick.
const EXPLORE: u64 = 0x6578_706C_6F72_6531;

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::min_ident_chars,
    clippy::float_cmp,
    clippy::too_many_lines,
    reason = "tests"
)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn keys(n: u64) -> PrefixKeys {
        PrefixKeys((0..n).collect())
    }

    fn sites(queues: &[(&'static str, f64)]) -> Vec<Site<'static>> {
        queues.iter().map(|&(cluster, queue)| Site { cluster, queue }).collect()
    }

    /// No exploration, so outcomes are deterministic.
    fn settings() -> AffinitySettings {
        AffinitySettings {
            exploration: 0.0,
            ..AffinitySettings::default()
        }
    }

    const GATE: QueueGate = QueueGate {
        queued_request_seconds: 2.0,
    };

    /// Narrow an `n`-key request over `sites` and return the outcome and the kept set.
    fn run_on(index: &PrefixIndex, n: u64, sites: &[impl Queued]) -> (Outcome, Vec<bool>) {
        let mut keep = vec![true; sites.len()];
        let keys = keys(n);
        let settings = settings();
        let affinity = Affinity {
            index,
            keys: Some(&keys),
            settings: &settings,
            turn: 0,
        };
        (affinity.narrow(sites, &mut keep, &GATE), keep)
    }

    fn run(index: &PrefixIndex, n: u64, queues: &[(&'static str, f64)]) -> (Outcome, Vec<bool>) {
        run_on(index, n, &sites(queues))
    }

    fn holding(cluster: &str, n: u64) -> PrefixIndex {
        let index = PrefixIndex::default();
        index.confirm(&keys(n), &Arc::from(cluster));
        index
    }

    #[test]
    fn the_site_holding_the_prefix_is_kept_alone() {
        let index = holding("east", 10);
        let (outcome, keep) = run(&index, 10, &[("east", 0.12), ("west", 0.1)]);
        assert_eq!(outcome, Outcome::Sticky);
        assert_eq!(keep, [true, false]);
    }

    #[test]
    fn identical_short_prompts_do_not_follow_one_site() {
        let index = holding("east", 3);
        assert_eq!(run(&index, 3, &[("east", 0.0), ("west", 0.0)]).0, Outcome::NoMatch);
    }

    #[test]
    fn a_short_share_is_not_sticky_but_a_long_match_is() {
        let index = holding("east", 10);
        assert_eq!(
            run(&index, 20, &[("east", 0.1), ("west", 0.1)]).0,
            Outcome::NoMatch,
            "10 of 20 keys"
        );
        let longer = holding("east", 40);
        assert_eq!(
            run(&longer, 200, &[("east", 0.1), ("west", 0.1)]).0,
            Outcome::Sticky,
            "40 keys pass the floor at any share"
        );
    }

    #[test]
    fn a_longer_match_tolerates_a_deeper_queue() {
        let queues = [("east", 1.0), ("west", 0.0)];
        // 40 keys is 2,560 tokens: 0.256s of prefill, 0.128 of a queued request at 2s each.
        assert_eq!(run(&holding("east", 40), 40, &queues).0, Outcome::LoadOverride);
        // 400 keys is 25,600 tokens: 2.56s of prefill, 1.28 queued requests.
        assert_eq!(run(&holding("east", 400), 400, &queues).0, Outcome::Sticky);
    }

    #[test]
    fn an_unmeasured_sticky_site_gives_way_to_a_measured_one() {
        let index = holding("east", 400);
        assert_eq!(
            run(&index, 400, &[("east", f64::INFINITY), ("west", 3.0)]).0,
            Outcome::LoadOverride
        );
        assert_eq!(
            run(&index, 400, &[("east", f64::INFINITY), ("west", f64::INFINITY)]).0,
            Outcome::Sticky,
            "nothing measured, so the match decides"
        );
        assert_eq!(
            run(&index, 400, &[("east", 3.0), ("west", f64::INFINITY)]).0,
            Outcome::Sticky
        );
    }

    #[test]
    fn the_gate_is_what_selection_says_it_is() {
        struct Never;
        impl LoadGate for Never {
            fn outweighs(&self, _: &Site<'_>, _: &Site<'_>, _: f64) -> bool {
                false
            }
        }
        let index = holding("east", 40);
        let keys = keys(40);
        let settings = settings();
        let affinity = Affinity {
            index: &index,
            keys: Some(&keys),
            settings: &settings,
            turn: 0,
        };
        let mut keep = vec![true; 2];
        let busy = sites(&[("east", 50.0), ("west", 0.0)]);
        assert_eq!(affinity.narrow(&busy, &mut keep, &Never), Outcome::Sticky);
    }

    #[test]
    fn the_deepest_holder_wins_over_a_shared_head() {
        let index = holding("east", 40);
        index.confirm(&keys(36), &Arc::from("west"));
        let (outcome, keep) = run(&index, 40, &[("east", 0.1), ("west", 0.05), ("north", 0.0)]);
        assert_eq!(outcome, Outcome::Sticky);
        assert_eq!(keep, [true, false, false], "west holds only the shared head");
    }

    #[test]
    fn equal_holders_both_stay_for_the_load_rule() {
        let index = holding("east", 40);
        index.confirm(&keys(40), &Arc::from("west"));
        let (outcome, keep) = run(&index, 40, &[("east", 0.1), ("west", 0.12), ("north", 0.0)]);
        assert_eq!(outcome, Outcome::Sticky);
        assert_eq!(keep, [true, true, false]);
    }

    #[test]
    fn each_turn_of_a_conversation_sticks_to_the_site_of_the_last() {
        use super::super::{Api, prefix_keys};
        let index = PrefixIndex::default();
        let mut messages = vec![serde_json::json!({"role": "system", "content": "Be brief. ".repeat(1_000)})];
        let east = Arc::<str>::from("east");
        for turn in 0..6 {
            messages.push(serde_json::json!({"role": "user", "content": format!("q{turn} ").repeat(120)}));
            let body = serde_json::to_vec(&serde_json::json!({"messages": messages})).unwrap();
            let keys = prefix_keys(Api::ChatCompletions, &body).unwrap();
            let sites = sites(&[("west", 0.1), ("east", 0.15)]);
            let mut keep = vec![true; 2];
            let settings = settings();
            let affinity = Affinity {
                index: &index,
                keys: Some(&keys),
                settings: &settings,
                turn,
            };
            let outcome = affinity.narrow(&sites, &mut keep, &GATE);
            if turn == 0 {
                assert_eq!(outcome, Outcome::NoMatch, "the first turn has nothing to match");
            } else {
                assert_eq!((outcome, keep), (Outcome::Sticky, vec![false, true]), "turn {turn}");
            }
            index.record(&keys, &east);
            index.confirm(&keys, &east);
            messages.push(serde_json::json!({"role": "assistant", "content": format!("a{turn} ").repeat(400)}));
        }
    }

    #[test]
    fn no_keys_and_exploration_change_nothing() {
        let index = holding("east", 40);
        let sites = sites(&[("east", 0.1), ("west", 0.1)]);
        let mut keep = vec![true; 2];
        let base = settings();
        let none = Affinity {
            index: &index,
            keys: None,
            settings: &base,
            turn: 0,
        };
        assert_eq!(none.narrow(&sites, &mut keep, &GATE), Outcome::NotApplicable);
        let always = AffinitySettings {
            exploration: 1.0,
            ..base
        };
        let keys = keys(40);
        let exploring = Affinity {
            index: &index,
            keys: Some(&keys),
            settings: &always,
            turn: 7,
        };
        assert_eq!(exploring.narrow(&sites, &mut keep, &GATE), Outcome::Exploration);
        assert_eq!(keep, [true, true]);
    }

    #[test]
    fn exploration_draws_about_its_share() {
        let explored = (0..100_000).filter(|turn| explores(*turn, 0.05)).count();
        assert!((4_000..6_000).contains(&explored), "{explored} of 100000");
        assert!(!(0..1_000).any(|turn| explores(turn, 0.0)));
    }

    #[test]
    fn outcome_labels_are_fixed() {
        let labels = [
            Outcome::Sticky,
            Outcome::NoMatch,
            Outcome::LoadOverride,
            Outcome::Exploration,
            Outcome::NotApplicable,
        ]
        .map(Outcome::as_str);
        assert_eq!(
            labels,
            ["sticky", "no_match", "load_override", "exploration", "not_applicable"]
        );
    }

    #[test]
    fn settings_out_of_range_are_refused() {
        let base = AffinitySettings::default();
        base.validate().expect("the defaults are valid");
        for bad in [
            AffinitySettings {
                threshold: 0.0,
                ..base.clone()
            },
            AffinitySettings {
                threshold: 1.5,
                ..base.clone()
            },
            AffinitySettings {
                exploration: 2.0,
                ..base.clone()
            },
            AffinitySettings {
                prefill_tokens_per_second: 0.0,
                ..base.clone()
            },
            AffinitySettings {
                queued_request_seconds: f64::NAN,
                ..base
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }
}
