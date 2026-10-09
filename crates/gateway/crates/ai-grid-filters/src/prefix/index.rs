//! Which candidate this replica sent each prefix key to.
//!
//! One LRU of keys per candidate. A key recorded at selection counts the
//! requests waiting on it, and a failure withdraws only its own request. Only
//! a success makes a key count toward a match, so a burst behind a new prompt
//! does not follow the first request before any site has answered it.

use std::{
    collections::HashMap,
    fmt,
    num::NonZeroUsize,
    sync::{Arc, PoisonError, RwLock},
    time::{Duration, Instant},
};

use lru::LruCache;

use super::PrefixKeys;

/// Keys one candidate holds: 4 MiB of prompt. Sizing from the site's KV
/// capacity needs a value the operator does not publish yet.
const CANDIDATE_KEYS: NonZeroUsize = NonZeroUsize::new(16_384).expect("nonzero");

/// Keys written under one hold of a candidate's lock.
const WRITE_CHUNK: usize = 64;

/// How long a key recorded for a request with no answer yet stays usable.
const PENDING_TTL: Duration = Duration::from_secs(30);

/// How long a confirmed key stays usable, since the site evicts on its own.
const CONFIRMED_TTL: Duration = Duration::from_secs(600);

/// One recorded key.
#[derive(Clone, Copy)]
struct Entry {
    /// When the key stops counting.
    until: Instant,
    /// Requests that recorded the key and have no answer yet.
    pending: u16,
    /// Whether a success confirmed the key.
    confirmed: bool,
}

/// One candidate's keys. Keys are already uniform hashes, so a fast hasher does,
/// seeded per process so a client cannot aim prompts at one bucket.
type Keys = Arc<RwLock<LruCache<u64, Entry, foldhash::fast::RandomState>>>;

/// The per-replica prefix index. Cheap to clone; clones share state.
#[derive(Clone, Default)]
pub(crate) struct PrefixIndex {
    /// Each candidate's keys, by cluster name.
    inner: Arc<RwLock<HashMap<Arc<str>, Keys>>>,
}

/// Keys are prompt content, so the index never prints them.
impl fmt::Debug for PrefixIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PrefixIndex { .. }")
    }
}

impl PrefixIndex {
    /// How many leading keys of `keys` each of `candidates` holds, in order.
    ///
    /// A candidate's held keys form a prefix of any prompt it holds: keys chain,
    /// and a prompt's keys are inserted tail first, so its head is never older
    /// than its tail. A binary search finds the depth in about nine lookups.
    pub(crate) fn depths(
        &self,
        keys: &PrefixKeys,
        candidates: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Vec<usize> {
        let now = Instant::now();
        let index = self.inner.read().unwrap_or_else(PoisonError::into_inner);
        candidates
            .into_iter()
            .map(|candidate| {
                index.get(candidate.as_ref()).map_or(0, |held| {
                    let held = held.read().unwrap_or_else(PoisonError::into_inner);
                    keys.as_slice()
                        .partition_point(|key| held.peek(key).is_some_and(|entry| entry.confirmed && now < entry.until))
                })
            })
            .collect()
    }

    /// Record `keys` as sent to `candidate`, waiting on [`Self::confirm`] or [`Self::forget`].
    pub(crate) fn record(&self, keys: &PrefixKeys, candidate: &Arc<str>) {
        let now = Instant::now();
        let until = now.checked_add(PENDING_TTL).unwrap_or(now);
        self.update(keys, candidate, |held| match held {
            Some(entry) if now < entry.until => Entry {
                pending: entry.pending.saturating_add(1),
                ..entry
            },
            _ => Entry {
                until,
                pending: 1,
                confirmed: false,
            },
        });
    }

    /// The request sent to `candidate` got a success: its keys hold there.
    pub(crate) fn confirm(&self, keys: &PrefixKeys, candidate: &Arc<str>) {
        let now = Instant::now();
        let until = now.checked_add(CONFIRMED_TTL).unwrap_or(now);
        self.update(keys, candidate, |held| Entry {
            until,
            pending: held.map_or(0, |entry| entry.pending.saturating_sub(1)),
            confirmed: true,
        });
    }

    /// The request sent to `candidate` failed: withdraw it, dropping keys nothing else holds.
    pub(crate) fn forget(&self, keys: &PrefixKeys, candidate: &str) {
        let Some(held) = self.existing(candidate) else {
            return;
        };
        let mut held = held.write().unwrap_or_else(PoisonError::into_inner);
        for key in keys.as_slice() {
            let Some(entry) = held.peek_mut(key) else {
                continue;
            };
            entry.pending = entry.pending.saturating_sub(1);
            if entry.pending == 0 && !entry.confirmed {
                held.pop(key);
            }
        }
    }

    /// `candidate` refused or failed the request: drop its keys there, confirmed or not.
    pub(crate) fn evict(&self, keys: &PrefixKeys, candidate: &str) {
        let Some(held) = self.existing(candidate) else {
            return;
        };
        let mut held = held.write().unwrap_or_else(PoisonError::into_inner);
        for key in keys.as_slice() {
            held.pop(key);
        }
    }

    /// Keep only `candidates`, dropping the keys of any other.
    pub(crate) fn retain(&self, candidates: &[Arc<str>]) {
        let mut index = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        index.retain(|candidate, _| candidates.contains(candidate));
    }

    /// Update each of `keys` for `candidate`, tail first, so the head of a prompt is evicted last.
    fn update(&self, keys: &PrefixKeys, candidate: &Arc<str>, entry: impl Fn(Option<Entry>) -> Entry) {
        let held = self.candidate(candidate);
        // A short lock per chunk lets depth lookups in between; no reader needs the whole prompt at once.
        for chunk in keys.as_slice().rchunks(WRITE_CHUNK) {
            let mut held = held.write().unwrap_or_else(PoisonError::into_inner);
            for key in chunk.iter().rev() {
                match held.get_mut(key) {
                    Some(current) => *current = entry(Some(*current)),
                    None => {
                        held.put(*key, entry(None));
                    },
                }
            }
        }
    }

    /// `candidate`'s keys, when it has any.
    fn existing(&self, candidate: &str) -> Option<Keys> {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(candidate)
            .map(Arc::clone)
    }

    /// `candidate`'s keys, created on first use.
    fn candidate(&self, candidate: &Arc<str>) -> Keys {
        if let Some(held) = self.inner.read().unwrap_or_else(PoisonError::into_inner).get(candidate) {
            return Arc::clone(held);
        }
        let mut index = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(index.entry(Arc::clone(candidate)).or_insert_with(|| {
            Arc::new(RwLock::new(LruCache::with_hasher(
                CANDIDATE_KEYS,
                foldhash::fast::RandomState::default(),
            )))
        }))
    }

    /// Keys held for `candidate`.
    #[cfg(test)]
    fn len(&self, candidate: &str) -> usize {
        let index = self.inner.read().unwrap_or_else(PoisonError::into_inner);
        index
            .get(candidate)
            .map_or(0, |held| held.read().unwrap_or_else(PoisonError::into_inner).len())
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
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    clippy::too_many_lines,
    clippy::disallowed_methods,
    clippy::shadow_unrelated,
    reason = "tests; the load benchmark paces itself with thread::sleep"
)]
mod tests {
    use super::*;

    fn keys(range: std::ops::Range<u64>) -> PrefixKeys {
        PrefixKeys(range.collect())
    }

    fn name(site: &str) -> Arc<str> {
        Arc::from(site)
    }

    #[test]
    fn a_pending_key_is_not_a_match() {
        let index = PrefixIndex::default();
        index.record(&keys(0..5), &name("east"));
        assert_eq!(index.depths(&keys(0..5), ["east"]), [0]);
    }

    #[test]
    fn depth_is_the_matched_head() {
        let index = PrefixIndex::default();
        index.confirm(&keys(0..5), &name("east"));
        assert_eq!(index.depths(&keys(0..8), ["east", "west"]), [5, 0]);
        assert_eq!(
            index.depths(&keys(100..108), ["east"]),
            [0],
            "another prompt matches nothing"
        );
    }

    #[test]
    fn a_failure_withdraws_only_its_own_request() {
        let index = PrefixIndex::default();
        let east = name("east");
        index.record(&keys(0..6), &east);
        index.record(&keys(0..3), &east);
        index.forget(&keys(0..3), &east);
        assert_eq!(index.len("east"), 6, "the other request still waits on the shared head");
        index.forget(&keys(0..6), &east);
        assert_eq!(index.len("east"), 0, "nothing waits and nothing confirmed");
    }

    #[test]
    fn a_confirmed_key_survives_a_later_failure() {
        let index = PrefixIndex::default();
        let east = name("east");
        index.record(&keys(0..3), &east);
        index.confirm(&keys(0..3), &east);
        index.record(&keys(0..6), &east);
        index.forget(&keys(0..6), &east);
        assert_eq!(index.depths(&keys(0..6), ["east"]), [3], "the confirmed head stays");
    }

    #[test]
    fn a_refusal_drops_even_confirmed_keys() {
        let index = PrefixIndex::default();
        let east = name("east");
        index.confirm(&keys(0..4), &east);
        index.evict(&keys(0..4), &east);
        assert_eq!(index.depths(&keys(0..4), ["east"]), [0]);
    }

    #[test]
    fn eviction_takes_the_tail_first() {
        let index = PrefixIndex::default();
        let east = name("east");
        let cap = u64::try_from(CANDIDATE_KEYS.get()).unwrap();
        index.confirm(&keys(0..cap), &east);
        index.confirm(&keys(1_000_000..1_000_010), &east);
        let depth = index.depths(&keys(0..cap), ["east"])[0];
        assert_eq!(
            depth,
            CANDIDATE_KEYS.get() - 10,
            "the old prompt lost its tail and kept its head"
        );
        assert_eq!(index.len("east"), CANDIDATE_KEYS.get());
    }

    #[test]
    fn retain_drops_a_removed_candidate() {
        let index = PrefixIndex::default();
        let (east, west) = (name("east"), name("west"));
        index.confirm(&keys(0..4), &east);
        index.confirm(&keys(0..4), &west);
        index.retain(&[Arc::clone(&east)]);
        assert_eq!(index.depths(&keys(0..4), ["east", "west"]), [4, 0]);
    }

    /// Index cost under load: 512-key prompts (the cap, 128 KiB and more) from 2,000
    /// conversations over 3 sites, open loop at 1k, 2.5k and 5k requests per second
    /// across 16 threads. Each request runs what selection does: depths over every
    /// site, a record at one, then a confirm. Record and confirm hold the site's write
    /// lock for nearly their whole call, so their latency stands in for lock hold.
    /// Run with `--ignored --nocapture` in release.
    #[test]
    #[ignore = "timing, not a check"]
    #[expect(clippy::print_stdout, reason = "reports the timing")]
    fn index_cost_under_load() {
        use std::{
            sync::Mutex,
            time::{Duration, Instant},
        };

        const THREADS: usize = 16;
        const CONVERSATIONS: u64 = 2_000;
        let sites: Vec<Arc<str>> = ["east", "west", "north"].map(Arc::from).to_vec();
        let names: Vec<&str> = sites.iter().map(|site| &**site).collect();
        let prompt = |conversation: u64| {
            PrefixKeys(
                (0..512_u64)
                    .map(|block| xxhash_rust::xxh64::xxh64(&block.to_le_bytes(), conversation))
                    .collect(),
            )
        };
        let quantile = |samples: &mut Vec<Duration>, q: f64| {
            samples.sort_unstable();
            let at = (samples.len() as f64 * q) as usize;
            samples[at.min(samples.len() - 1)]
        };
        for rate in [1_000_u64, 2_500, 5_000] {
            let index = PrefixIndex::default();
            let timings = Mutex::new((Vec::new(), Vec::new(), Vec::new()));
            let gap = Duration::from_secs(1) * u32::try_from(THREADS).unwrap() / u32::try_from(rate).unwrap();
            let started = Instant::now();
            std::thread::scope(|scope| {
                for worker in 0..THREADS {
                    let (index, names, sites, timings) = (&index, &names, &sites, &timings);
                    scope.spawn(move || {
                        let (mut depth, mut record, mut confirm) = (Vec::new(), Vec::new(), Vec::new());
                        // Stagger the threads across one gap, so arrivals spread evenly instead of in bursts.
                        let mut next = started + gap * u32::try_from(worker).unwrap() / u32::try_from(THREADS).unwrap();
                        std::thread::sleep(next.saturating_duration_since(Instant::now()));
                        let mut request = worker as u64;
                        while started.elapsed() < Duration::from_secs(3) {
                            let keys = prompt(request % CONVERSATIONS);
                            let site = &sites[(request % 3) as usize];
                            let at = Instant::now();
                            std::hint::black_box(index.depths(&keys, names));
                            depth.push(at.elapsed());
                            let at = Instant::now();
                            index.record(&keys, site);
                            record.push(at.elapsed());
                            let at = Instant::now();
                            index.confirm(&keys, site);
                            confirm.push(at.elapsed());
                            request += THREADS as u64;
                            next += gap;
                            std::thread::sleep(next.saturating_duration_since(Instant::now()));
                        }
                        let mut all = timings.lock().unwrap();
                        all.0.extend(depth);
                        all.1.extend(record);
                        all.2.extend(confirm);
                    });
                }
            });
            let (mut depth, mut record, mut confirm) = timings.into_inner().unwrap();
            let served = depth.len() as f64 / started.elapsed().as_secs_f64();
            println!(
                "target {rate} rps, ran {served:.0}: depths p50 {:?} p99 {:?} | record p50 {:?} p99 {:?} max {:?} | confirm p50 {:?} p99 {:?} max {:?}",
                quantile(&mut depth, 0.5),
                quantile(&mut depth, 0.99),
                quantile(&mut record, 0.5),
                quantile(&mut record, 0.99),
                quantile(&mut record, 1.0),
                quantile(&mut confirm, 0.5),
                quantile(&mut confirm, 0.99),
                quantile(&mut confirm, 1.0),
            );
        }
    }

    #[test]
    fn debug_prints_no_keys() {
        let index = PrefixIndex::default();
        index.record(&keys(41..42), &name("east"));
        assert_eq!(format!("{index:?}"), "PrefixIndex { .. }");
    }
}
