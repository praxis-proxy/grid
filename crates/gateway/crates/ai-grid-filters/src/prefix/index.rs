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
    sync::{
        Arc, PoisonError, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use lru::LruCache;

use super::{AffinitySettings, PrefixKeys};

/// Keys written under one hold of a candidate's lock.
const WRITE_CHUNK: usize = 64;

/// How long a key recorded for a request with no answer yet stays pending.
const PENDING_TTL: Duration = Duration::from_secs(30);

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

/// One candidate's LRU. Keys are already uniform hashes, so a fast hasher does,
/// seeded per process so a client cannot aim prompts at one bucket.
type Lru = LruCache<u64, Entry, foldhash::fast::RandomState>;

/// One candidate's keys and the series that describe them.
///
/// The handles are registered once, when the candidate is created, so the request
/// path updates them with atomic stores and adds rather than a registry lookup.
struct Held {
    /// The candidate's keys.
    keys: RwLock<Lru>,
    /// Keys held, `grid_route_prefix_index_entries`.
    entries: metrics::Gauge,
    /// Configured capacity, `grid_route_prefix_index_capacity`.
    capacity: metrics::Gauge,
    /// Live keys a full LRU displaced, the undersizing signal.
    evicted_capacity: metrics::Counter,
    /// Expired keys displaced, and confirmed keys sent again after their lifetime.
    evicted_ttl: metrics::Counter,
    /// Keys a failure or refusal removed.
    evicted_forgotten: metrics::Counter,
    /// Set when an apply drops the candidate, so a request still holding it cannot revive its gauge.
    retired: AtomicBool,
}

impl Held {
    /// An empty LRU limited to `capacity`, with its series registered under `cluster`.
    fn new(cluster: &str, capacity: NonZeroUsize) -> Self {
        let mut keys = LruCache::unbounded_with_hasher(foldhash::fast::RandomState::default());
        // Set the entry limit without reserving its full capacity.
        keys.resize(capacity);
        let evicted =
            |reason: &'static str| metrics::counter!(EVICTIONS, "cluster" => cluster.to_owned(), "reason" => reason);
        let held = Self {
            keys: RwLock::new(keys),
            entries: metrics::gauge!(ENTRIES, "cluster" => cluster.to_owned()),
            capacity: metrics::gauge!(CAPACITY, "cluster" => cluster.to_owned()),
            evicted_capacity: evicted("capacity"),
            evicted_ttl: evicted("ttl"),
            evicted_forgotten: evicted("forgotten"),
            retired: AtomicBool::new(false),
        };
        held.capacity.set(usize_gauge(capacity.get()));
        held.entries.set(0.0);
        held
    }

    /// Write access to the keys.
    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Lru> {
        self.keys.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Publish the number of keys held. Called under the write lock, which orders it after [`Self::retire`].
    fn publish(&self, keys: &Lru) {
        if !self.retired.load(Ordering::Relaxed) {
            self.entries.set(usize_gauge(keys.len()));
        }
    }

    /// Clear the series of a candidate an apply dropped.
    fn retire(&self) {
        let lru = self.write();
        self.retired.store(true, Ordering::Relaxed);
        self.entries.set(0.0);
        self.capacity.set(0.0);
        drop(lru);
    }
}

/// A shared candidate.
type Keys = Arc<Held>;

/// Keys held for a backend cluster in this replica.
pub(crate) const ENTRIES: &str = "grid_route_prefix_index_entries";

/// Keys a backend cluster's LRU may hold.
pub(crate) const CAPACITY: &str = "grid_route_prefix_index_capacity";

/// Keys that left a backend cluster's LRU, by reason.
pub(crate) const EVICTIONS: &str = "grid_route_prefix_index_evictions_total";

/// A count as a gauge value. Exact below 2^53, far above any capacity.
#[expect(
    clippy::cast_precision_loss,
    clippy::as_conversions,
    reason = "counts stay below 2^53"
)]
const fn usize_gauge(count: usize) -> f64 {
    count as f64
}

/// Add `count` to `counter`, skipping the atomic add when nothing left.
fn add(counter: &metrics::Counter, count: usize) {
    if count > 0 {
        counter.increment(u64::try_from(count).unwrap_or(u64::MAX));
    }
}

/// The per-replica prefix index. Cheap to clone; clones share state.
#[derive(Clone, Default)]
pub(crate) struct PrefixIndex {
    /// The candidate inventory and policy applied to new records.
    inner: Arc<RwLock<IndexState>>,
}

/// Cache policy and candidate inventory, updated together on configuration apply.
struct IndexState {
    /// Each candidate's keys, by cluster name.
    candidates: HashMap<Arc<str>, Keys>,
    /// Clusters the last apply adopted, absent before the first one.
    adopted: Option<Vec<Arc<str>>>,
    /// Maximum keys retained in each candidate's LRU.
    capacity: NonZeroUsize,
    /// Lifetime of a newly confirmed key.
    confirmed_ttl: Duration,
}

impl Default for IndexState {
    fn default() -> Self {
        let settings = AffinitySettings::default();
        Self {
            candidates: HashMap::new(),
            adopted: None,
            capacity: settings.capacity,
            confirmed_ttl: Duration::from_secs(settings.ttl_secs.get()),
        }
    }
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
    /// Stop at the first missing or expired key: a shorter TTL applied to a later
    /// confirmation can make a prompt's head expire before its older tail.
    pub(crate) fn depths(
        &self,
        keys: &PrefixKeys,
        candidates: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Vec<usize> {
        self.depths_at(keys, candidates, Instant::now())
    }

    /// Match prefixes at one observation time, including the exact expiry boundary.
    fn depths_at(
        &self,
        keys: &PrefixKeys,
        candidates: impl IntoIterator<Item = impl AsRef<str>>,
        now: Instant,
    ) -> Vec<usize> {
        let index = self.inner.read().unwrap_or_else(PoisonError::into_inner);
        candidates
            .into_iter()
            .map(|candidate| {
                index.candidates.get(candidate.as_ref()).map_or(0, |held| {
                    let held = held.keys.read().unwrap_or_else(PoisonError::into_inner);
                    keys.as_slice()
                        .iter()
                        .take_while(|key| {
                            held.peek(*key)
                                .is_some_and(|entry| entry.confirmed && now < entry.until)
                        })
                        .count()
                })
            })
            .collect()
    }

    /// Record `keys` as sent to `candidate`, waiting on [`Self::confirm`] or [`Self::forget`].
    pub(crate) fn record(&self, keys: &PrefixKeys, candidate: &Arc<str>) {
        self.record_at(keys, candidate, Instant::now());
    }

    /// Record at one observation time.
    fn record_at(&self, keys: &PrefixKeys, candidate: &Arc<str>, now: Instant) {
        let Some((held, _)) = self.candidate(candidate) else {
            return;
        };
        let until = now.checked_add(PENDING_TTL).unwrap_or(now);
        Self::update(keys, &held, now, |held| match held {
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
        self.confirm_at(keys, candidate, Instant::now());
    }

    /// Confirm at one observation time.
    fn confirm_at(&self, keys: &PrefixKeys, candidate: &Arc<str>, now: Instant) {
        let Some((held, confirmed_ttl)) = self.candidate(candidate) else {
            return;
        };
        let until = now.checked_add(confirmed_ttl).unwrap_or(now);
        Self::update(keys, &held, now, |held| Entry {
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
        let mut lru = held.write();
        let mut removed = 0_usize;
        for key in keys.as_slice() {
            let Some(entry) = lru.peek_mut(key) else {
                continue;
            };
            entry.pending = entry.pending.saturating_sub(1);
            if entry.pending == 0 && !entry.confirmed {
                lru.pop(key);
                removed = removed.saturating_add(1);
            }
        }
        held.publish(&lru);
        drop(lru);
        add(&held.evicted_forgotten, removed);
    }

    /// `candidate` refused or failed the request: drop its keys there, confirmed or not.
    pub(crate) fn evict(&self, keys: &PrefixKeys, candidate: &str) {
        let Some(held) = self.existing(candidate) else {
            return;
        };
        let mut lru = held.write();
        let removed = keys.as_slice().iter().filter(|key| lru.pop(*key).is_some()).count();
        held.publish(&lru);
        drop(lru);
        add(&held.evicted_forgotten, removed);
    }

    /// Adopt cache limits and retain routed candidates. Existing keys keep their expiry times.
    pub(crate) fn apply(&self, settings: &AffinitySettings, candidates: &[Arc<str>]) {
        let now = Instant::now();
        let mut index = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        index.capacity = settings.capacity;
        index.confirmed_ttl = Duration::from_secs(settings.ttl_secs.get());
        index.candidates.retain(|candidate, held| {
            let kept = candidates.contains(candidate);
            if !kept {
                held.retire();
            }
            kept
        });
        index.adopted = Some(candidates.to_vec());
        for candidate in candidates {
            if let Some(held) = index.candidates.get(candidate) {
                let mut lru = held.write();
                // A smaller capacity drops the least recent keys, sorted as a full LRU's displacements are.
                let (mut capacity, mut ttl) = (0_usize, 0_usize);
                while lru.len() > settings.capacity.get() {
                    match lru.pop_lru() {
                        Some((_, dropped)) if dropped.until <= now => ttl = ttl.saturating_add(1),
                        Some(_) => capacity = capacity.saturating_add(1),
                        None => break,
                    }
                }
                lru.resize(settings.capacity);
                held.publish(&lru);
                drop(lru);
                held.capacity.set(usize_gauge(settings.capacity.get()));
                add(&held.evicted_capacity, capacity);
                add(&held.evicted_ttl, ttl);
            }
        }
    }

    /// Update each of `keys` for `candidate`, tail first, so the head of a prompt is evicted last.
    ///
    /// Counts what leaves at `now`: a key a full LRU displaces is a capacity eviction while it
    /// still counts and a TTL one once it has expired; a confirmed key sent again after its
    /// lifetime is a TTL one too. The counters are added once per call, outside the lock.
    fn update(keys: &PrefixKeys, held: &Keys, now: Instant, entry: impl Fn(Option<Entry>) -> Entry) {
        let (mut capacity, mut ttl) = (0_usize, 0_usize);
        // A short lock per chunk lets depth lookups in between; no reader needs the whole prompt at once.
        for chunk in keys.as_slice().rchunks(WRITE_CHUNK) {
            let mut lru = held.write();
            for key in chunk.iter().rev() {
                if let Some(current) = lru.get_mut(key) {
                    if current.confirmed && current.until <= now {
                        ttl = ttl.saturating_add(1);
                    }
                    *current = entry(Some(*current));
                } else if let Some((_, displaced)) = lru.push(*key, entry(None)) {
                    // The key was absent, so push returned the entry the full LRU displaced.
                    if displaced.until <= now {
                        ttl = ttl.saturating_add(1);
                    } else {
                        capacity = capacity.saturating_add(1);
                    }
                }
            }
            held.publish(&lru);
            drop(lru);
        }
        add(&held.evicted_capacity, capacity);
        add(&held.evicted_ttl, ttl);
    }

    /// `candidate`'s keys, when it has any.
    fn existing(&self, candidate: &str) -> Option<Keys> {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .candidates
            .get(candidate)
            .map(Arc::clone)
    }

    /// The candidate's keys and confirmed lifetime, creating its LRU on first use.
    ///
    /// Absent once an apply has dropped the cluster, so an in-flight request cannot recreate it.
    fn candidate(&self, candidate: &Arc<str>) -> Option<(Keys, Duration)> {
        {
            let index = self.inner.read().unwrap_or_else(PoisonError::into_inner);
            if let Some(held) = index.candidates.get(candidate) {
                return Some((Arc::clone(held), index.confirmed_ttl));
            }
        }
        let mut index = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        if index
            .adopted
            .as_ref()
            .is_some_and(|adopted| !adopted.contains(candidate))
        {
            return None;
        }
        let capacity = index.capacity;
        let held = Arc::clone(
            index
                .candidates
                .entry(Arc::clone(candidate))
                .or_insert_with(|| Arc::new(Held::new(candidate, capacity))),
        );
        Some((held, index.confirmed_ttl))
    }

    /// Keys held for `candidate`.
    #[cfg(test)]
    fn len(&self, candidate: &str) -> usize {
        let index = self.inner.read().unwrap_or_else(PoisonError::into_inner);
        index
            .candidates
            .get(candidate)
            .map_or(0, |held| held.keys.read().unwrap_or_else(PoisonError::into_inner).len())
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
        let capacity = AffinitySettings::default().capacity.get();
        let cap = u64::try_from(capacity).unwrap();
        index.confirm(&keys(0..cap), &east);
        index.confirm(&keys(1_000_000..1_000_010), &east);
        let depth = index.depths(&keys(0..cap), ["east"])[0];
        assert_eq!(depth, capacity - 10, "the old prompt lost its tail and kept its head");
        assert_eq!(index.len("east"), capacity);
    }

    #[test]
    fn retain_drops_a_removed_candidate() {
        let index = PrefixIndex::default();
        let (east, west) = (name("east"), name("west"));
        index.confirm(&keys(0..4), &east);
        index.confirm(&keys(0..4), &west);
        index.apply(&AffinitySettings::default(), &[Arc::clone(&east)]);
        assert_eq!(index.depths(&keys(0..4), ["east", "west"]), [4, 0]);
    }

    /// Per-cluster limits isolate backends and resize existing caches without losing recent prefixes.
    #[test]
    fn configured_capacity_is_independent_for_each_cluster_and_resizes_live_caches() {
        let index = PrefixIndex::default();
        let clusters = [name("east"), name("west")];
        let mut settings: AffinitySettings = serde_yaml::from_str("capacity: 4").expect("settings");
        index.apply(&settings, &clusters);
        for cluster in &clusters {
            index.confirm(&keys(0..8), cluster);
        }
        assert_eq!(
            index.depths(&keys(0..8), ["east", "west"]),
            [4, 4],
            "each cluster gets capacity 4"
        );
        index.confirm(&keys(100..108), &clusters[0]);
        assert_eq!(index.depths(&keys(0..8), ["west"]), [4], "east cannot evict west");

        settings.capacity = NonZeroUsize::new(2).expect("nonzero");
        index.apply(&settings, &clusters);
        assert_eq!(
            index.depths(&keys(100..108), ["east"]),
            [2],
            "shrink retains recent keys"
        );
        assert_eq!(
            index.depths(&keys(0..8), ["west"]),
            [2],
            "west also shrinks to capacity 2"
        );
        let north = name("north");
        index.apply(&settings, &[name("east"), name("west"), Arc::clone(&north)]);
        index.confirm(&keys(0..8), &north);
        assert_eq!(index.len("north"), 2, "new clusters use the current capacity");

        settings.capacity = NonZeroUsize::new(8).expect("nonzero");
        index.apply(&settings, &clusters);
        index.confirm(&keys(0..8), &clusters[1]);
        assert_eq!(index.depths(&keys(0..8), ["west"]), [8], "growth allows more entries");
        assert_eq!(index.len("north"), 0, "removed clusters release their cache");
    }

    /// A reload can drop a cluster while one of its requests is still in flight. The late
    /// outcome must not bring the cache back, or its keys linger until the next apply.
    #[test]
    fn an_in_flight_request_does_not_resurrect_a_dropped_cluster() {
        let index = PrefixIndex::default();
        let settings = AffinitySettings::default();
        let (east, west) = (name("east"), name("west"));
        index.apply(&settings, &[Arc::clone(&east), Arc::clone(&west)]);

        // East is sent a request, then the reload drops it.
        index.record(&keys(0..8), &east);
        index.apply(&settings, &[Arc::clone(&west)]);
        assert_eq!(index.len("east"), 0, "the reload released east's cache");

        // The in-flight request now finishes, either way round.
        index.confirm(&keys(0..8), &east);
        assert_eq!(index.len("east"), 0, "a late success does not recreate the cluster");
        index.record(&keys(0..8), &east);
        assert_eq!(index.len("east"), 0, "nor does a late send");
        assert_eq!(index.depths(&keys(0..8), ["west"]), [0], "west is untouched");
    }

    /// A sparse cache must not reserve storage for every entry its configured limit permits.
    #[test]
    fn a_large_capacity_does_not_reserve_space_for_absent_entries() {
        let index = PrefixIndex::default();
        let east = name("east");
        let settings = AffinitySettings {
            capacity: NonZeroUsize::MAX,
            ..AffinitySettings::default()
        };
        index.apply(&settings, &[Arc::clone(&east)]);
        index.record(&keys(0..1), &east);
        index.confirm(&keys(0..1), &east);
        assert_eq!(
            index.len("east"),
            1,
            "east stores only the single observed prefix entry"
        );
        assert_eq!(
            index.depths(&keys(0..1), ["east"]),
            [1],
            "the sparse entry remains usable"
        );
        let held = index.existing("east").expect("east cache");
        assert_eq!(
            held.keys.read().unwrap().cap(),
            settings.capacity,
            "the configured limit is retained"
        );
    }

    /// Public TTL tuning affects confirmed mappings while pending bookkeeping keeps its fixed lifetime.
    #[test]
    fn pending_lifetime_is_fixed_and_confirmation_lifetime_is_configurable() {
        let index = PrefixIndex::default();
        let east = name("east");
        let settings: AffinitySettings = serde_yaml::from_str("ttl_secs: 11").expect("settings");
        index.apply(&settings, &[Arc::clone(&east)]);
        let before = Instant::now();
        index.record(&keys(0..4), &east);
        let after = Instant::now();
        let pending = deadline(&index, "east", 0);
        assert!(
            pending >= before + Duration::from_secs(30) && pending <= after + Duration::from_secs(30),
            "pending TTL stays at 30 seconds"
        );
        assert_eq!(index.depths(&keys(0..4), ["east"]), [0], "pending entries do not match");

        let before = Instant::now();
        index.confirm(&keys(0..4), &east);
        let after = Instant::now();
        let confirmed = deadline(&index, "east", 0);
        assert!(
            confirmed >= before + Duration::from_secs(11) && confirmed <= after + Duration::from_secs(11),
            "confirmation uses ttl_secs = 11"
        );
        assert_eq!(
            index.depths_at(&keys(0..4), ["east"], before),
            [4],
            "east matches all confirmed keys before expiry"
        );
        assert_eq!(
            index.depths_at(&keys(0..4), ["east"], confirmed),
            [0],
            "expiry is exclusive"
        );
    }

    /// Shorter TTLs can expire a refreshed head first, so matching must stop before its live tail.
    #[test]
    fn ttl_reload_preserves_deadlines_and_an_expired_head_cannot_match_a_live_tail() {
        let index = PrefixIndex::default();
        let east = name("east");
        index.confirm(&keys(0..8), &east);
        let original = deadline(&index, "east", 0);
        let settings: AffinitySettings = serde_yaml::from_str("ttl_secs: 1").expect("settings");
        index.apply(&settings, &[Arc::clone(&east)]);
        assert_eq!(
            deadline(&index, "east", 0),
            original,
            "reload keeps the recorded deadline"
        );
        index.confirm(&keys(0..1), &east);
        let shortened = deadline(&index, "east", 0);
        assert!(shortened < original, "a later confirmation uses the new TTL");
        assert_eq!(
            deadline(&index, "east", 7),
            original,
            "the older tail remains unchanged"
        );
        assert_eq!(
            index.depths_at(&keys(0..8), ["east"], shortened),
            [0],
            "a live tail cannot bridge the expired head"
        );
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

    /// Thread-local capture of the index series, keyed by name and labels.
    #[derive(Default)]
    struct Series(std::sync::Mutex<std::collections::BTreeMap<String, Arc<metrics::atomics::AtomicU64>>>);

    impl Series {
        fn slot(&self, key: &metrics::Key) -> Arc<metrics::atomics::AtomicU64> {
            let labels: Vec<String> = key.labels().map(|label| label.value().to_owned()).collect();
            let name = format!("{}{{{}}}", key.name(), labels.join(","));
            Arc::clone(self.0.lock().unwrap().entry(name).or_default())
        }

        fn count(&self, cluster: &str, reason: &str) -> u64 {
            self.0
                .lock()
                .unwrap()
                .get(&format!("{EVICTIONS}{{{cluster},{reason}}}"))
                .map_or(0, |value| value.load(Ordering::Relaxed))
        }

        /// A gauge holding a count, `None` when never set.
        fn gauge(&self, name: &str, cluster: &str) -> Option<u64> {
            self.0
                .lock()
                .unwrap()
                .get(&format!("{name}{{{cluster}}}"))
                .map(|value| f64::from_bits(value.load(Ordering::Relaxed)) as u64)
        }
    }

    impl metrics::Recorder for Series {
        fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}

        fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}

        fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}

        fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
            metrics::Counter::from_arc(self.slot(key))
        }

        fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::from_arc(self.slot(key))
        }

        fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    /// An index limited to `capacity` keys per cluster, with the default lifetime.
    fn sized(capacity: usize) -> PrefixIndex {
        let index = PrefixIndex::default();
        let settings = AffinitySettings {
            capacity: NonZeroUsize::new(capacity).unwrap(),
            ..AffinitySettings::default()
        };
        index.apply(&settings, &[name("east")]);
        index
    }

    /// One second past the default confirmed lifetime.
    fn past_ttl(from: Instant) -> Instant {
        from + Duration::from_secs(AffinitySettings::default().ttl_secs.get() + 1)
    }

    #[test]
    fn a_full_index_counts_the_live_keys_it_displaces_as_capacity() {
        let series = Series::default();
        metrics::with_local_recorder(&series, || {
            let index = sized(4);
            let east = name("east");
            index.confirm(&keys(0..4), &east);
            index.confirm(&keys(10..12), &east);
            assert_eq!(series.count("east", "capacity"), 2, "two live keys displaced");
            assert_eq!(series.count("east", "ttl"), 0);
            assert_eq!(series.gauge(ENTRIES, "east"), Some(4), "the index stays full");
            assert_eq!(series.gauge(CAPACITY, "east"), Some(4));
        });
    }

    #[test]
    fn an_expired_key_counts_as_ttl_when_displaced_or_sent_again() {
        let series = Series::default();
        metrics::with_local_recorder(&series, || {
            let index = sized(3);
            let east = name("east");
            let start = Instant::now();
            index.confirm_at(&keys(0..2), &east, start);
            index.confirm_at(&keys(0..2), &east, past_ttl(start));
            assert_eq!(series.count("east", "ttl"), 2, "a turn after the lifetime");
            index.confirm_at(&keys(10..13), &east, past_ttl(past_ttl(start)));
            assert_eq!(series.count("east", "ttl"), 4, "two expired keys displaced");
            assert_eq!(series.count("east", "capacity"), 0, "nothing live was displaced");
            assert_eq!(series.gauge(ENTRIES, "east"), Some(3));
        });
    }

    #[test]
    fn an_expired_pending_key_confirmed_late_is_not_a_ttl_eviction() {
        let series = Series::default();
        metrics::with_local_recorder(&series, || {
            let index = sized(8);
            let east = name("east");
            let start = Instant::now();
            index.record_at(&keys(0..3), &east, start);
            index.confirm_at(&keys(0..3), &east, start + PENDING_TTL + Duration::from_secs(1));
            assert_eq!(series.count("east", "ttl"), 0, "a long response is not a long turn gap");
        });
    }

    #[test]
    fn a_failure_or_refusal_counts_its_removed_keys_as_forgotten() {
        let series = Series::default();
        metrics::with_local_recorder(&series, || {
            let index = sized(16);
            let east = name("east");
            index.record(&keys(0..4), &east);
            index.forget(&keys(0..4), &east);
            assert_eq!(series.count("east", "forgotten"), 4);
            assert_eq!(series.gauge(ENTRIES, "east"), Some(0));
            index.confirm(&keys(0..3), &east);
            index.forget(&keys(0..3), &east);
            assert_eq!(series.count("east", "forgotten"), 4, "a failure keeps confirmed keys");
            index.evict(&keys(0..5), &east);
            assert_eq!(
                series.count("east", "forgotten"),
                7,
                "a refusal drops the three it held"
            );
            assert_eq!(series.gauge(ENTRIES, "east"), Some(0));
        });
    }

    #[test]
    fn a_capacity_cut_counts_its_drops_and_a_removed_cluster_reads_zero() {
        let series = Series::default();
        metrics::with_local_recorder(&series, || {
            let index = sized(8);
            let east = name("east");
            index.confirm(&keys(0..8), &east);
            let smaller = AffinitySettings {
                capacity: NonZeroUsize::new(5).unwrap(),
                ..AffinitySettings::default()
            };
            index.apply(&smaller, &[name("east")]);
            assert_eq!(series.count("east", "capacity"), 3);
            assert_eq!(series.gauge(ENTRIES, "east"), Some(5));
            assert_eq!(series.gauge(CAPACITY, "east"), Some(5));
            let in_flight = index.existing("east").expect("east");
            index.apply(&smaller, &[]);
            assert_eq!(
                series.gauge(ENTRIES, "east"),
                Some(0),
                "a dropped cluster holds nothing"
            );
            assert_eq!(series.gauge(CAPACITY, "east"), Some(0));
            PrefixIndex::update(&keys(20..22), &in_flight, Instant::now(), |_| Entry {
                until: Instant::now(),
                pending: 1,
                confirmed: false,
            });
            assert_eq!(
                series.gauge(ENTRIES, "east"),
                Some(0),
                "a request that held the dropped cluster does not revive its gauge"
            );
        });
    }

    /// The recorded deadline of a key, without changing its recency.
    fn deadline(index: &PrefixIndex, candidate: &str, key: u64) -> Instant {
        let held = index.existing(candidate).expect("candidate");
        held.keys.read().unwrap().peek(&key).expect("key").until
    }
}
