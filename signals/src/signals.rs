//! Live load signals kept in a bounded per-series window.
//!
//! Absorbs the operator's exposition and keeps a bounded per-series window, so
//! routing scores candidates on current load. Samples key on the operator's
//! observation time, so a republished cache value never reads as new.
//!
//! The exposition tokenizer is shared with the producer and lives in the sibling
//! exposition module. This module owns the consumer side: extracting the grid
//! target labels fail-closed and holding the windowed store.

use std::{
    borrow::Cow,
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dashmap::{DashMap, mapref::entry::Entry};

use crate::exposition::{self, PROVIDER_LABEL, SITE_LABEL};

/// Cap on providers retained, so a misconfigured or hostile endpoint cannot grow
/// the store without bound. Keys are retained once seen (not LRU-evicted), so the
/// bound is on distinct site/cluster keys, not on churn; the operator is the trust
/// source that stamps them. Size this as roughly max enrolled sites times realistic
/// providers-per-site: owner count is bounded by CA-issued identities, and past
/// [`MAX_PROVIDERS`] / [`MAX_PROVIDERS_PER_OWNER`] owners the global cap
/// reintroduces a milder lockout of later sites.
const MAX_PROVIDERS: usize = 4_096;

/// Cap on providers a single verified owner may hold, so one authenticated owner
/// cannot consume the whole global budget and lock out other sites (R1). Well
/// below [`MAX_PROVIDERS`], so a flood from one owner leaves room for the rest;
/// the global cap still bounds aggregate memory across owners.
const MAX_PROVIDERS_PER_OWNER: usize = 256;

/// Cap on distinct metric names per provider, bounding a peer that floods unique
/// names past the provider cap.
const MAX_METRICS_PER_PROVIDER: usize = 64;

/// Samples retained per series; past the cap the oldest drop. A flood bound, not a
/// working-set size: a normal scrape cadence holds far fewer.
const MAX_SAMPLES_PER_SERIES: usize = 128;

/// Byte cap on a metric name or target label value before it keys the store.
const MAX_KEY_INPUT_BYTES: usize = 256;

/// Tolerance for a sample stamped ahead of the operator's own clock (its `Date`
/// header). Kept small: a legitimate sample is never meaningfully ahead of the
/// operator's own observation clock, and a large future window only lets a stamp
/// wedge the series head against the later corrected samples `Series::push`
/// drops.
const MAX_CLOCK_SKEW_MS: i64 = 5_000;

/// Largest relayed-sample age treated as plausible, one day. A larger apparent
/// age means the peer's clock is skewed or the stamp is garbage, so the sample is
/// restamped fresh rather than trusted to be that old.
const MAX_RELAY_AGE_MS: i64 = 24 * 60 * 60 * 1000;

/// Two published stamps this close name one observation. A relay re-serves a sample
/// for a whole poll cycle and a one-second `Date` restamps it on a moving clock each
/// time, so the published stamp says whether a line is new; a publisher deriving that
/// stamp at render can round it a millisecond either way.
const ORIGIN_TOLERANCE_MS: u64 = 1;

/// A no-skew reference-and-local clock for tests: it sits above the small stamps
/// the tests use and well within [`MAX_RELAY_AGE_MS`] of them, so `rebase_age`
/// restamps each sample onto its own value (an identity).
#[cfg(test)]
const NO_SKEW_NOW_MS: i64 = 1_000_000;

/// How two lines of one metric observed at the same instant under different labels are
/// combined into the one value the series keeps for that instant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Combine {
    /// The larger: right for a per-unit reading whose worst unit matters.
    Max,
    /// The total: right for a count split across partitions.
    Sum,
}

/// Which combination a metric takes, by name.
pub type CombinePolicy = fn(&str) -> Combine;

/// Every metric takes the larger value, which never hides work.
fn combine_max(_metric: &str) -> Combine {
    Combine::Max
}

/// What one ingest kept and dropped, lines counted once each.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Ingested {
    /// Lines stored.
    pub kept: u64,
    /// Lines stamped past the clock-skew bound.
    pub skewed: u64,
    /// Lines whose site label disagreed with the verified owner.
    pub mismatched: u64,
    /// Lines naming a provider the global or per-site cap refused.
    pub capped: u64,
    /// Lines that did not parse as a stamped sample.
    pub unparsed: u64,
}

impl Ingested {
    /// Each drop reason with its count, for a counter.
    #[must_use]
    pub const fn dropped(&self) -> [(&'static str, u64); 4] {
        [
            ("skew", self.skewed),
            ("site_mismatch", self.mismatched),
            ("cap", self.capped),
            ("parse", self.unparsed),
        ]
    }
}

/// One observation of a series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sample {
    /// Operator observation time, in milliseconds since the epoch.
    pub at_ms: i64,
    /// Value as the provider reported it.
    pub value: f64,
}

/// A bounded window of one series, oldest first.
#[derive(Debug, Default)]
struct Series {
    /// Samples in timestamp order.
    samples: Vec<Sample>,
    /// The label sets already folded into the newest sample, so a republished line is
    /// not combined twice while a line under other labels is.
    newest_labels: Vec<u64>,
    /// The newest sample's stamp as the publisher wrote it, before restamping.
    newest_origin_ms: Option<i64>,
}

impl Series {
    /// Append the observation's sample if it is newer than what is held, fold it into the
    /// newest sample if it is another label set at the same instant or the same published
    /// observation restamped, then evict past `window`.
    fn push(&mut self, observation: &Observation<'_>, combine: Combine, window: Duration) {
        let sample = observation.sample;
        if let Some(last) = self.samples.last() {
            let republished = self
                .newest_origin_ms
                .is_some_and(|held| observation.origin_ms.abs_diff(held) <= ORIGIN_TOLERANCE_MS);
            if republished || sample.at_ms == last.at_ms {
                self.fold(observation.labels, sample.value, combine);
                return;
            }
            if sample.at_ms < last.at_ms {
                return;
            }
        }
        self.newest_labels.clear();
        self.newest_labels.push(observation.labels);
        self.newest_origin_ms = Some(observation.origin_ms);
        self.samples.push(sample);
        // Window eviction needs the window in millis. If it does not fit i64 (a
        // caller passing an implausible Duration), skip only the window cutoff; the
        // count cap below still runs, so the series stays bounded regardless.
        let keep_from = match i64::try_from(window.as_millis()) {
            Ok(window_ms) => {
                let cutoff = sample.at_ms.saturating_sub(window_ms);
                self.samples.partition_point(|held| held.at_ms < cutoff)
            },
            Err(_) => 0,
        };
        // Drop from the front to satisfy both bounds in one pass: everything past
        // the window, and any excess over the count cap when a flood packs more
        // in-window points than a normal scrape cadence produces.
        let over_cap = self.samples.len().saturating_sub(MAX_SAMPLES_PER_SERIES);
        let drop_to = keep_from.max(over_cap);
        if drop_to > 0 {
            self.samples.drain(..drop_to);
        }
    }

    /// Combine `value` into the newest sample unless its label set was already folded.
    fn fold(&mut self, labels: u64, value: f64, combine: Combine) {
        if self.newest_labels.contains(&labels) {
            return;
        }
        self.newest_labels.push(labels);
        if let Some(last) = self.samples.last_mut() {
            last.value = match combine {
                Combine::Max => last.value.max(value),
                Combine::Sum => last.value + value,
            };
        }
    }
}

/// Series held for one provider, keyed by metric name.
#[derive(Debug, Default)]
struct Provider {
    /// Metric name to its window.
    metrics: HashMap<Box<str>, Series>,
}

/// Windowed signals per provider, keyed by `"site/cluster"` so a request-path
/// lookup matches a route candidate. The key is built by concatenation
/// ([`Self::key`]), a small `Box<str>` per lookup.
#[derive(Debug)]
pub struct LoadStore {
    /// Provider key to its series.
    providers: DashMap<Box<str>, Provider>,
    /// Attributed site to the count of providers it holds, for the per-site cap.
    /// Keyed on the crypto-verified owner, never a self-reported value, with one
    /// exception: rows from this site's own operator carry the site that operator
    /// attributed from its own verified poll, so for that source the label is the
    /// owner. Keying every relayed site on the one local owner would exhaust the
    /// cap at a few dozen sites.
    owner_providers: DashMap<Box<str>, usize>,
    /// Count of admitted providers, for the global cap. Providers are retained,
    /// never evicted, so this only grows. An atomic check-and-increment bounds it
    /// exactly under concurrent pollers, which a `providers.len()` check followed
    /// by a separate insert cannot.
    admitted: AtomicUsize,
    /// Retention per series.
    window: Duration,
    /// How same-instant lines of one metric under different labels combine.
    combine: CombinePolicy,
    /// This gateway's own site. A source verified as this site is its local
    /// operator, whose rows are attributed by their label.
    local_site: Option<Box<str>>,
}

impl LoadStore {
    /// Create an empty store retaining `window` of history per series, combining
    /// same-instant lines of a metric by their larger value.
    #[must_use]
    pub fn new(window: Duration) -> Self {
        Self::with_combine(window, combine_max)
    }

    /// Create an empty store retaining `window` of history per series, combining
    /// same-instant lines of each metric as `combine` says.
    #[must_use]
    pub fn with_combine(window: Duration, combine: CombinePolicy) -> Self {
        Self {
            providers: DashMap::new(),
            owner_providers: DashMap::new(),
            admitted: AtomicUsize::new(0),
            window,
            combine,
            local_site: None,
        }
    }

    /// Name this gateway's own site, so rows from its local operator are
    /// attributed by the site label that operator stamped.
    #[must_use]
    pub fn with_local_site(mut self, local_site: &str) -> Self {
        self.local_site = Some(local_site.into());
        self
    }

    /// The key under which a candidate's series are held.
    #[must_use]
    pub fn key(site: &str, cluster: &str) -> Box<str> {
        format!("{site}/{cluster}").into_boxed_str()
    }

    /// Most recent sample of `metric` for `key`.
    #[must_use]
    pub fn latest(&self, key: &str, metric: &str) -> Option<Sample> {
        let provider = self.providers.get(key)?;
        provider.metrics.get(metric)?.samples.last().copied()
    }

    /// When anything was last observed for `key`, across all its metrics.
    #[must_use]
    pub fn newest_at(&self, key: &str) -> Option<i64> {
        let provider = self.providers.get(key)?;
        provider
            .metrics
            .values()
            .filter_map(|series| series.samples.last().map(|sample| sample.at_ms))
            .max()
    }

    /// Most recent sample of `metric` for `key` younger than `max_age_ms`.
    #[must_use]
    pub fn fresh(&self, key: &str, metric: &str, now_ms: i64, max_age_ms: i64) -> Option<Sample> {
        // Range starts at zero: a future timestamp (publisher clock ahead) yields
        // a negative age that would otherwise read as fresh forever.
        self.latest(key, metric)
            .filter(|sample| (0..=max_age_ms).contains(&now_ms.saturating_sub(sample.at_ms)))
    }

    /// Worst reading of `metric` for `key` within the last `window_ms`, or `None`
    /// when the window holds no sample.
    ///
    /// Worst is the max when lower is better, so a drained burst stays penalised
    /// until it ages out rather than snapping to idle. Future-stamped samples are
    /// skipped.
    #[expect(
        clippy::too_many_arguments,
        reason = "keyed lookup with window bounds and score polarity"
    )]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the shard read guard is held across the bounded window scan by design"
    )]
    #[must_use]
    pub fn window_worst(
        &self,
        key: &str,
        metric: &str,
        now_ms: i64,
        window_ms: i64,
        lower_is_better: bool,
    ) -> Option<f64> {
        let provider = self.providers.get(key)?;
        let cutoff = now_ms.saturating_sub(window_ms);
        let mut worst: Option<f64> = None;
        for sample in &provider.metrics.get(metric)?.samples {
            if sample.at_ms < cutoff || sample.at_ms > now_ms {
                continue;
            }
            worst = Some(match worst {
                None => sample.value,
                Some(held) if lower_is_better => held.max(sample.value),
                Some(held) => held.min(sample.value),
            });
        }
        worst
    }

    /// Worst of `combine` over the instants in the last `window_ms` at which the first
    /// of `metrics` has a sample, or `None` when no instant yields a value.
    ///
    /// Each instant hands `combine` the sample every metric holds at exactly that
    /// time, `None` where a metric has none, so a value is concluded from readings
    /// taken together rather than from each series' own worst. Worst is the max
    /// when lower is better. Future-stamped samples are skipped.
    #[expect(
        clippy::too_many_arguments,
        reason = "keyed lookup with window bounds, score polarity, and the join"
    )]
    #[must_use]
    pub fn window_worst_of<const N: usize, F>(
        &self,
        key: &str,
        metrics: [&str; N],
        now_ms: i64,
        window_ms: i64,
        lower_is_better: bool,
        combine: F,
    ) -> Option<f64>
    where
        F: Fn([Option<f64>; N]) -> Option<f64>,
    {
        let provider = self.providers.get(key)?;
        let cutoff = now_ms.saturating_sub(window_ms);
        let series = metrics.map(|metric| provider.metrics.get(metric));
        let lead = series.first().copied().flatten()?;
        let mut worst: Option<f64> = None;
        for sample in &lead.samples {
            if sample.at_ms < cutoff || sample.at_ms > now_ms {
                continue;
            }
            // Samples are in timestamp order, so the instant is a binary search.
            let at = |held: &Series| {
                held.samples
                    .binary_search_by_key(&sample.at_ms, |held| held.at_ms)
                    .ok()
                    .and_then(|index| held.samples.get(index))
                    .map(|held| held.value)
            };
            let Some(value) = combine(series.map(|held| held.and_then(at))) else {
                continue;
            };
            worst = Some(match worst {
                None => value,
                Some(held) if lower_is_better => held.max(value),
                Some(held) => held.min(value),
            });
        }
        worst
    }

    /// Number of providers held.
    #[cfg(test)]
    pub fn provider_count(&self) -> usize {
        self.providers.len()
    }

    /// Absorb an exposition response with no clock skew, attributing to the
    /// body's own first site. Test-only: reference and local now coincide above
    /// the small stamps these tests use, so `rebase_age` is an identity and a
    /// sample reads back at the stamp it carried. The poll path uses
    /// [`Self::ingest_at`] with the peer's `Date`, the local clock, and the
    /// crypto-verified owner, so no production path bypasses the anchor or the
    /// owner binding, which the `ingest_at` tests cover.
    #[cfg(test)]
    pub fn ingest(&self, text: &str) {
        self.ingest_at(text, NO_SKEW_NOW_MS, NO_SKEW_NOW_MS, first_grid_site(text));
    }

    /// Absorb an exposition response, skipping lines that do not parse so one bad
    /// line does not cost the rest.
    ///
    /// `reference_ms` is the peer's own clock (its `Date` header) and
    /// `local_now_ms` is this gateway's clock. A sample stamped implausibly far
    /// past the peer's `Date` is dropped, then each surviving sample's age is
    /// re-expressed on the local clock (`rebase_age`) so [`Self::window_worst`]
    /// compares every sample against one clock.
    ///
    /// `owner` is the peer's crypto-verified site (from mTLS): attribution keys on
    /// it, never the self-reported `grid_site` label. A line whose label disagrees
    /// is a cross-site spoof and is dropped, and a line without the label is
    /// attributed to `owner`. The one exception is this gateway's own operator,
    /// named by [`Self::with_local_site`]: it stamped each row from the leaf it
    /// verified, so its label is the key, owner-bound one hop earlier.
    ///
    /// New-provider admission is atomic against both caps, so concurrent pollers
    /// cannot drive the retained-provider count past the global or per-owner
    /// bound.
    ///
    /// Returns what was kept and what was dropped, by reason.
    pub fn ingest_at(&self, text: &str, reference_ms: i64, local_now_ms: i64, owner: &str) -> Ingested {
        let scrape = ScrapeClock {
            reference_ms,
            local_now_ms,
            horizon_ms: reference_ms.saturating_add(MAX_CLOCK_SKEW_MS),
            owner,
            local: self.local_site.as_deref() == Some(owner),
        };
        let mut tally = Ingested::default();
        for line in text.lines() {
            let Some(observation) = parse_sample(line) else {
                if !line.trim().is_empty() && !line.starts_with('#') {
                    tally.unparsed = tally.unparsed.saturating_add(1);
                }
                continue;
            };
            let counted = match self.ingest_line(observation, &scrape) {
                Ok(()) => &mut tally.kept,
                Err(Dropped::Skewed) => &mut tally.skewed,
                Err(Dropped::Mismatched) => &mut tally.mismatched,
                Err(Dropped::Capped) => &mut tally.capped,
            };
            *counted = counted.saturating_add(1);
        }
        tally
    }

    /// Attribute and store one parsed line, or say why it was dropped.
    fn ingest_line(&self, mut observation: Observation<'_>, scrape: &ScrapeClock<'_>) -> Result<(), Dropped> {
        if observation.sample.at_ms > scrape.horizon_ms {
            return Err(Dropped::Skewed);
        }
        // #160, narrowed: from any peer the label must agree with the verified
        // owner. From this site's own operator the label is the owner, since the
        // operator attributed it from its own verified poll.
        let site = match (scrape.local, observation.site.as_deref()) {
            (true, Some(site)) => site,
            (false, Some(site)) if site != scrape.owner => return Err(Dropped::Mismatched),
            _ => scrape.owner,
        };
        observation.sample.at_ms = rebase_age(scrape.reference_ms, observation.sample.at_ms, scrape.local_now_ms);

        let key = Self::key(site, observation.cluster.as_ref());
        let combine = (self.combine)(observation.metric);
        match self.providers.entry(key) {
            Entry::Occupied(mut occupied) => {
                push_observation(occupied.get_mut(), &observation, combine, self.window);
            },
            Entry::Vacant(vacant) => {
                // A new key: admit it against both caps while its shard is
                // locked, so the check and the insert cannot race a
                // concurrent poller into overshooting a cap.
                if !self.admit_new_provider(site) {
                    return Err(Dropped::Capped);
                }
                let mut provider = Provider::default();
                push_observation(&mut provider, &observation, combine, self.window);
                vacant.insert(provider);
            },
        }
        Ok(())
    }

    /// This gateway's own site, if named.
    #[must_use]
    pub fn local_site(&self) -> Option<&str> {
        self.local_site.as_deref()
    }

    /// Reserve a global and a per-owner slot for one new provider, atomically.
    ///
    /// Returns `false`, holding no reservation, when either cap is already met.
    /// The global reservation is an atomic check-and-increment on
    /// [`Self::admitted`]. The per-owner reservation runs under the owner's entry
    /// lock, so two concurrent inserts for one owner cannot both pass the check.
    fn admit_new_provider(&self, owner: &str) -> bool {
        if self
            .admitted
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                (count < MAX_PROVIDERS).then(|| count.saturating_add(1))
            })
            .is_err()
        {
            return false;
        }
        let mut held = self.owner_providers.entry(owner.into()).or_insert(0);
        if *held >= MAX_PROVIDERS_PER_OWNER {
            drop(held);
            self.admitted.fetch_sub(1, Ordering::SeqCst);
            return false;
        }
        *held = held.saturating_add(1);
        true
    }
}

/// Push `observation`'s sample into `provider`, bounding the distinct metric
/// names it holds. A known metric neither re-hashes nor allocates. A new metric
/// owns its name only if it fits under the per-provider cap, which bounds a peer
/// flooding unique names.
fn push_observation(provider: &mut Provider, observation: &Observation<'_>, combine: Combine, window: Duration) {
    if let Some(series) = provider.metrics.get_mut(observation.metric) {
        series.push(observation, combine, window);
    } else if provider.metrics.len() < MAX_METRICS_PER_PROVIDER {
        provider
            .metrics
            .entry(observation.metric.into())
            .or_default()
            .push(observation, combine, window);
    }
}

/// Re-express a peer sample's age on the local clock.
///
/// The peer's `Date` and the sample stamp are both on the peer clock, so their
/// difference is skew-free, and restamping that age onto `local_now_ms` puts the
/// sample on the reader's clock. An implausible age (a garbage stamp, a badly
/// skewed peer, or a stamp ahead of the peer's own `Date`) is treated as fresh
/// rather than trusted, so it never reads as stale or far-future.
fn rebase_age(reference_ms: i64, sample_at_ms: i64, local_now_ms: i64) -> i64 {
    let age = reference_ms.saturating_sub(sample_at_ms);
    if (0..=MAX_RELAY_AGE_MS).contains(&age) {
        local_now_ms.saturating_sub(age)
    } else {
        local_now_ms
    }
}

/// One scrape's clocks and verified owner, shared by every line it carries.
struct ScrapeClock<'scrape> {
    /// The publisher's clock, its `Date`.
    reference_ms: i64,
    /// This gateway's clock.
    local_now_ms: i64,
    /// A stamp past this is skewed beyond belief.
    horizon_ms: i64,
    /// The crypto-verified site on the connection.
    owner: &'scrape str,
    /// Whether the owner is this gateway's own operator.
    local: bool,
}

/// Why a parsed line was not stored.
enum Dropped {
    /// Stamped past the clock-skew bound.
    Skewed,
    /// Its site label disagreed with the verified owner.
    Mismatched,
    /// Its provider was refused by the global or per-site cap.
    Capped,
}

/// One exposition line resolved to its metric name, owning site and cluster, and
/// a sample.
struct Observation<'text> {
    /// Metric name.
    metric: &'text str,
    /// Self-reported owning site from the `grid_site` label, if the line carried
    /// one. Keys the row only from this gateway's own operator, which stamped it
    /// from the leaf it verified; from any other owner a disagreeing label drops
    /// the line.
    site: Option<Cow<'text, str>>,
    /// Owning provider, from the `grid_provider` label.
    cluster: Cow<'text, str>,
    /// A hash of every label on the line, naming the series within the metric.
    labels: u64,
    /// The sample this line reported, its stamp re-expressed on the local clock.
    sample: Sample,
    /// The stamp as published, naming the observation across restamps.
    origin_ms: i64,
}

/// Parse one exposition line into an [`Observation`], or `None` to skip it.
///
/// A line without a timestamp is skipped: without it a republished sample cannot
/// be told from a new one. A target value carrying a control char or `/` is
/// rejected so it cannot inject the store-key separator, and a duplicated
/// `grid_site` or `grid_provider` is anomalous for a well-formed operator, so
/// the line is rejected (fail closed).
fn parse_sample(line: &str) -> Option<Observation<'_>> {
    let metric = exposition::parse(line)?;
    let at_ms = metric.timestamp_ms()?;
    // The metric name keys a provider's series; a relayed over-long name is
    // rejected before it can bloat the store.
    if metric.name().len() > MAX_KEY_INPUT_BYTES {
        return None;
    }
    let (site, cluster) = target_labels(&metric)?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for (name, value) in metric.labels() {
        std::hash::Hash::hash(&(name, value.as_ref()), &mut hasher);
    }
    Some(Observation {
        metric: metric.name(),
        site,
        cluster,
        labels: std::hash::Hasher::finish(&hasher),
        sample: Sample {
            at_ms,
            value: metric.value(),
        },

        origin_ms: at_ms,
    })
}

/// The self-reported site (optional) and the required provider from a line.
type TargetLabels<'text> = (Option<Cow<'text, str>>, Cow<'text, str>);

/// The `grid_provider` value (required) and the self-reported `grid_site` value
/// (optional), or `None` if the provider is missing, either is over-long, carries
/// a control char or `/`, or is repeated.
///
/// `grid_provider` keys the provider within the owner's site. `grid_site` is only
/// a cross-check: ingest keys on the verified owner, never this label, so a
/// missing `grid_site` is not fatal here. A separator or control char is rejected
/// before it can inject the store-key separator or corrupt a log, and a repeated
/// target label is anomalous for a well-formed operator.
fn target_labels<'text>(metric: &exposition::Metric<'text>) -> Option<TargetLabels<'text>> {
    let mut site: Option<Cow<'text, str>> = None;
    let mut cluster: Option<Cow<'text, str>> = None;
    for (name, value) in metric.labels() {
        let slot = match name {
            SITE_LABEL => &mut site,
            PROVIDER_LABEL => &mut cluster,
            _ => continue,
        };
        if value.len() > MAX_KEY_INPUT_BYTES {
            return None;
        }
        if value.chars().any(|ch| ch.is_control() || ch == '/') {
            return None;
        }
        if slot.replace(value).is_some() {
            return None;
        }
    }
    Some((site, cluster?))
}

/// Milliseconds since the epoch.
#[must_use]
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| i64::try_from(since.as_millis()).unwrap_or(i64::MAX))
}

/// The first `grid_site` label value in an exposition, for the test-only
/// [`LoadStore::ingest`] convenience. Not on any production path.
#[cfg(test)]
fn first_grid_site(text: &str) -> &str {
    text.lines()
        .find_map(|line| line.split(r#"grid_site=""#).nth(1))
        .and_then(|rest| rest.split('"').next())
        .unwrap_or("")
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::arithmetic_side_effects,
    clippy::expect_used,
    clippy::float_cmp,
    clippy::indexing_slicing,
    clippy::significant_drop_tightening,
    reason = "tests"
)]
mod tests {
    use super::*;

    const QUEUE: &str = "inference_pool_average_queue_size";

    fn line(site: &str, cluster: &str, value: f64, at_ms: i64) -> String {
        format!(r#"{QUEUE}{{grid_site="{site}",grid_provider="{cluster}"}} {value} {at_ms}"#)
    }

    fn store() -> LoadStore {
        LoadStore::new(Duration::from_secs(300))
    }

    #[test]
    fn ingests_a_labelled_sample() {
        let store = store();
        store.ingest(&line("east", "pool-a", 3.0, 1_000));
        let sample = store.latest(&LoadStore::key("east", "pool-a"), QUEUE).expect("sample");
        assert_eq!(
            sample,
            Sample {
                at_ms: 1_000,
                value: 3.0
            },
            "value and time as reported"
        );
    }

    /// The hold sums across its partitions, everything else keeps the larger value.
    fn total(metric: &str) -> Combine {
        if metric == "held" { Combine::Sum } else { Combine::Max }
    }

    #[test]
    fn same_instant_lines_under_other_labels_combine_and_a_republished_line_does_not() {
        // Two flow-control partitions at one instant: the default keeps the larger, a
        // sum policy keeps the total, and polling the same exposition again changes nothing.
        let lines = concat!(
            "held{grid_site=\"a\",grid_provider=\"p\",priority=\"0\"} 0 1000\n",
            "held{grid_site=\"a\",grid_provider=\"p\",priority=\"1\"} 5 1000\n",
            "held{grid_site=\"a\",grid_provider=\"p\",priority=\"2\"} 2 1000\n"
        );
        let key = LoadStore::key("a", "p");
        let max = LoadStore::new(Duration::from_secs(60));
        max.ingest_at(lines, 1_000, 1_000, "a");
        max.ingest_at(lines, 1_000, 1_000, "a");
        assert_eq!(
            max.latest(&key, "held").map(|sample| sample.value),
            Some(5.0),
            "a zero partition hides nothing"
        );
        let sum = LoadStore::with_combine(Duration::from_secs(60), total);
        sum.ingest_at(lines, 1_000, 1_000, "a");
        sum.ingest_at(lines, 1_000, 1_000, "a");
        assert_eq!(
            sum.latest(&key, "held").map(|sample| sample.value),
            Some(7.0),
            "the total, once"
        );
        // The next instant starts over.
        sum.ingest_at(
            "held{grid_site=\"a\",grid_provider=\"p\",priority=\"1\"} 1 2000",
            2_000,
            2_000,
            "a",
        );
        assert_eq!(sum.latest(&key, "held").map(|sample| sample.value), Some(1.0));
    }

    #[test]
    fn a_republished_sample_does_not_advance_the_series() {
        let store = store();
        let repeated = line("east", "pool-a", 3.0, 1_000);
        store.ingest(&repeated);
        store.ingest(&repeated);
        store.ingest(&repeated);
        let key = LoadStore::key("east", "pool-a");
        let provider = store.providers.get(&key).expect("provider");
        let held = provider.metrics.get(QUEUE).expect("series").samples.len();
        assert_eq!(held, 1, "the operator's cached republish is not a new observation");
    }

    #[test]
    fn a_newer_sample_advances_the_series() {
        let store = store();
        store.ingest(&line("east", "pool-a", 3.0, 1_000));
        store.ingest(&line("east", "pool-a", 5.0, 2_000));
        let sample = store.latest(&LoadStore::key("east", "pool-a"), QUEUE).expect("sample");
        assert_eq!(sample.value, 5.0, "the newer value wins");
    }

    #[test]
    fn samples_older_than_the_window_are_evicted() {
        let store = LoadStore::new(Duration::from_secs(10));
        for at_ms in [1_000, 5_000, 20_000] {
            store.ingest(&line("east", "pool-a", 1.0, at_ms));
        }
        let key = LoadStore::key("east", "pool-a");
        let provider = store.providers.get(&key).expect("provider");
        let samples = &provider.metrics.get(QUEUE).expect("series").samples;
        assert_eq!(samples.len(), 1, "only what falls inside the window: {samples:?}");
        assert_eq!(
            samples.first().map(|sample| sample.at_ms),
            Some(20_000),
            "the newest survives"
        );
    }

    #[test]
    fn sites_do_not_collide_on_a_shared_cluster_name() {
        let store = store();
        store.ingest(&line("east", "pool-a", 1.0, 1_000));
        store.ingest(&line("west", "pool-a", 9.0, 1_000));
        assert_eq!(store.provider_count(), 2, "the site is part of the key");
        let west = store.latest(&LoadStore::key("west", "pool-a"), QUEUE).expect("west");
        assert_eq!(west.value, 9.0, "each site keeps its own value");
    }

    #[test]
    fn a_line_without_a_timestamp_is_skipped() {
        let store = store();
        store.ingest(&format!(r#"{QUEUE}{{grid_site="east",grid_provider="pool-a"}} 3"#));
        assert_eq!(
            store.provider_count(),
            0,
            "without a timestamp there is no way to order the sample"
        );
    }

    #[test]
    fn unlabelled_and_malformed_lines_are_skipped_without_losing_the_rest() {
        let store = store();
        let text = format!(
            "# HELP something\n{QUEUE} 3 1000\nnot a metric\n{}",
            line("east", "pool-a", 3.0, 1_000)
        );
        store.ingest(&text);
        assert_eq!(store.provider_count(), 1, "the one usable line still lands");
    }

    #[test]
    fn a_provider_cannot_exceed_the_metric_name_cap() {
        let store = store();
        for idx in 0..(MAX_METRICS_PER_PROVIDER + 10) {
            store.ingest(&format!(
                r#"metric_{idx}{{grid_site="east",grid_provider="pool-a"}} 1 1000"#
            ));
        }
        let key = LoadStore::key("east", "pool-a");
        let provider = store.providers.get(&key).expect("provider");
        assert_eq!(
            provider.metrics.len(),
            MAX_METRICS_PER_PROVIDER,
            "a flood of unique metric names is bounded per provider"
        );
    }

    #[test]
    fn a_quoted_comma_does_not_forge_a_target_label() {
        // The injected `,grid_provider=evil` trails the real label: a naive
        // last-write-wins parser would forge pool-a to evil, the quote-aware
        // parser keeps it inside the one value.
        let store = store();
        store.ingest(&format!(
            r#"{QUEUE}{{grid_site="east",grid_provider="pool-a",note="x,grid_provider=evil"}} 3 1000"#
        ));
        assert_eq!(store.provider_count(), 1, "one sample, keyed on the real target labels");
        assert!(
            store.latest(&LoadStore::key("east", "pool-a"), QUEUE).is_some(),
            "the sample keys to the genuine provider"
        );
        assert!(
            store.latest(&LoadStore::key("east", "evil"), QUEUE).is_none(),
            "the forged provider inside a quoted value never keys"
        );
    }

    #[test]
    fn an_escaped_quote_does_not_forge_a_target_label() {
        // An escaped quote must not end the value early and expose a forged label.
        let store = store();
        store.ingest(&format!(
            r#"{QUEUE}{{grid_site="east",grid_provider="pool-a",note="a\",grid_site=evil"}} 3 1000"#
        ));
        assert_eq!(
            store.provider_count(),
            1,
            "the escaped quote stays inside the one value"
        );
        assert!(
            store.latest(&LoadStore::key("east", "pool-a"), QUEUE).is_some(),
            "the genuine site survives the escaped-quote injection"
        );
        assert!(
            store.latest(&LoadStore::key("evil", "pool-a"), QUEUE).is_none(),
            "the forged site never keys"
        );
    }

    #[test]
    fn a_slash_or_control_char_in_a_target_value_is_rejected() {
        // A '/' would collide distinct site/cluster pairs in the store key.
        let with_slash = store();
        with_slash.ingest(&format!(r#"{QUEUE}{{grid_site="a/b",grid_provider="pool-a"}} 3 1000"#));
        assert_eq!(with_slash.provider_count(), 0, "a '/' in a target value is rejected");
        // An unescaped-to-newline control char must not reach a key or a log.
        let with_ctrl = store();
        with_ctrl.ingest(&format!(
            r#"{QUEUE}{{grid_site="ea\nst",grid_provider="pool-a"}} 3 1000"#
        ));
        assert_eq!(
            with_ctrl.provider_count(),
            0,
            "a control char in a target value is rejected"
        );
    }

    #[test]
    fn a_non_finite_value_is_rejected() {
        let store = store();
        store.ingest(&format!(
            r#"{QUEUE}{{grid_site="east",grid_provider="pool-a"}} NaN 1000"#
        ));
        store.ingest(&format!(
            r#"{QUEUE}{{grid_site="east",grid_provider="pool-a"}} +Inf 2000"#
        ));
        assert_eq!(store.provider_count(), 0, "NaN and Inf sample values are rejected");
    }

    #[test]
    fn a_duplicate_target_label_is_rejected() {
        let store = store();
        store.ingest(&format!(
            r#"{QUEUE}{{grid_site="east",grid_site="west",grid_provider="pool-a"}} 3 1000"#
        ));
        assert_eq!(
            store.provider_count(),
            0,
            "a duplicated grid_site is anomalous, so the line is dropped"
        );
    }

    #[test]
    fn an_escaped_label_value_is_parsed() {
        // An escaped non-target value must not break parsing of the line.
        let store = store();
        store.ingest(&format!(
            r#"{QUEUE}{{extra="line\none",grid_site="east",grid_provider="pool-a"}} 3 1000"#
        ));
        assert_eq!(
            store.provider_count(),
            1,
            "the line still lands with an escaped value present"
        );
    }

    #[test]
    fn a_future_sample_is_dropped_and_does_not_wedge_the_series() {
        // The peer's Date is 2_000. A stamp beyond the skew tolerance ahead of it
        // is implausible and must not enter the series head, or the later
        // corrected sample would be dropped as older. Reference and local now
        // coincide (no skew), so the corrected sample keeps its own stamp.
        let store = store();
        let date = 2_000;
        let future = date + MAX_CLOCK_SKEW_MS + 10_000;
        store.ingest_at(&line("east", "pool-a", 9.0, future), date, date, "east");
        store.ingest_at(&line("east", "pool-a", 3.0, date), date, date, "east");
        let sample = store
            .latest(&LoadStore::key("east", "pool-a"), QUEUE)
            .expect("the corrected sample lands");
        assert_eq!(sample.at_ms, 2_000, "a future stamp cannot wedge the series head");
    }

    #[test]
    fn a_stale_sample_is_withheld_from_routing() {
        let store = store();
        store.ingest(&line("east", "pool-a", 3.0, 1_000));
        let key = LoadStore::key("east", "pool-a");
        assert!(store.fresh(&key, QUEUE, 10_000, 30_000).is_some(), "inside the bound");
        assert!(store.fresh(&key, QUEUE, 60_000, 30_000).is_none(), "past the bound");
    }

    #[test]
    fn windowed_worst_persists_a_drained_burst() {
        let store = store();
        let key = LoadStore::key("east", "pool-a");
        store.ingest(&line("east", "pool-a", 30.0, 1_000)); // burst
        store.ingest(&line("east", "pool-a", 1.0, 5_000)); // drained to idle
        // lower_is_better keeps the worst (max) in the window, so the burst
        // persists rather than snapping to idle.
        assert_eq!(
            store.window_worst(&key, QUEUE, 5_000, 30_000, true),
            Some(30.0),
            "a drained burst must persist as the worst reading in the window"
        );
        // The last-value view (what scoring used before) would have snapped to
        // idle.
        assert_eq!(
            store.latest(&key, QUEUE).map(|sample| sample.value),
            Some(1.0),
            "the last-value view snaps to idle"
        );
    }

    #[test]
    fn a_sample_from_a_clock_ahead_of_ours_is_withheld() {
        let store = store();
        store.ingest(&line("east", "pool-a", 3.0, 60_000));
        let key = LoadStore::key("east", "pool-a");
        assert!(
            store.fresh(&key, QUEUE, 10_000, 30_000).is_none(),
            "a future timestamp must not read as fresh, or a dead site keeps winning"
        );
    }

    #[test]
    fn a_single_owner_is_bounded_by_its_provider_subcap() {
        let store = store();
        for idx in 0..(MAX_PROVIDERS_PER_OWNER + 10) {
            store.ingest(&line("east", &format!("pool-{idx}"), 1.0, 1_000));
        }
        assert_eq!(
            store.provider_count(),
            MAX_PROVIDERS_PER_OWNER,
            "one owner's flood is bounded by its per-owner sub-cap, not the global cap"
        );
    }

    #[test]
    fn one_owner_cannot_lock_out_another() {
        // R1 (B2): east fills its own per-owner cap, then a different verified owner
        // polls one provider. It must be admitted, not locked out by east's flood.
        // This was rejected before the per-owner partition; it now passes.
        let store = store();
        for idx in 0..(MAX_PROVIDERS_PER_OWNER + 10) {
            store.ingest(&line("east", &format!("pool-{idx}"), 1.0, 1_000));
        }
        store.ingest(&line("west", "pool-a", 9.0, 1_000));
        assert_eq!(
            store
                .latest(&LoadStore::key("west", "pool-a"), QUEUE)
                .map(|sample| sample.value),
            Some(9.0),
            "a valid owner is admitted even after another owner fills its cap"
        );
    }

    #[test]
    fn the_same_provider_on_many_lines_counts_once() {
        let store = store();
        let body = format!(
            "{}\n{}\n{}",
            line("east", "pool-a", 1.0, 1_000),
            line("east", "pool-a", 2.0, 2_000),
            line("east", "pool-a", 3.0, 3_000),
        );
        store.ingest(&body);
        assert_eq!(store.provider_count(), 1, "one provider, not one count per line");
    }

    #[test]
    fn the_global_cap_backstops_across_many_owners() {
        let store = store();
        for idx in 0..MAX_PROVIDERS {
            let owner = format!("site-{idx}");
            store.ingest_at(
                &line(&owner, "pool-a", 1.0, 1_000),
                NO_SKEW_NOW_MS,
                NO_SKEW_NOW_MS,
                &owner,
            );
        }
        assert_eq!(
            store.provider_count(),
            MAX_PROVIDERS,
            "filled to the global cap across owners"
        );
        store.ingest_at(
            &line("late", "pool-a", 9.0, 1_000),
            NO_SKEW_NOW_MS,
            NO_SKEW_NOW_MS,
            "late",
        );
        assert!(
            store.latest(&LoadStore::key("late", "pool-a"), QUEUE).is_none(),
            "the global cap still refuses a new owner once full"
        );
    }

    #[test]
    fn a_series_is_bounded_by_the_sample_cap() {
        // A flood of strictly-increasing in-window stamps would otherwise grow the
        // series unbounded; the count cap drops the oldest and keeps the newest.
        let store = store();
        let cap = i64::try_from(MAX_SAMPLES_PER_SERIES).expect("cap fits i64");
        // Two apart, so no stamp reads as a republication of the one before it.
        let mut text = String::new();
        for at_ms in (2..=2 * (cap + 500)).step_by(2) {
            text.push_str(&line("east", "pool-a", 1.0, at_ms));
            text.push('\n');
        }
        store.ingest(&text);
        let key = LoadStore::key("east", "pool-a");
        let provider = store.providers.get(&key).expect("provider");
        let samples = &provider.metrics.get(QUEUE).expect("series").samples;
        assert_eq!(
            samples.len(),
            MAX_SAMPLES_PER_SERIES,
            "a flood of in-window samples is bounded per series"
        );
        assert_eq!(
            samples.last().map(|sample| sample.at_ms),
            Some(2 * (cap + 500)),
            "the newest sample survives the cap"
        );
    }

    #[test]
    fn a_joined_worst_reads_each_instant_together() {
        let store = LoadStore::new(Duration::from_secs(60));
        // Two units running 10 each, then one unit running 20: 20 in flight both times,
        // never the 40 that each series' own worst would multiply to.
        for (at, running, units) in [(1_000, 10.0, 2.0), (2_000, 20.0, 1.0)] {
            store.ingest_at(
                &format!(
                    "running{{grid_site=\"a\",grid_provider=\"p\"}} {running} {at}\nunits{{grid_site=\"a\",grid_provider=\"p\"}} {units} {at}"
                ),
                at,
                at,
                "a",
            );
        }
        let key = LoadStore::key("a", "p");
        let in_flight = |now: i64| {
            store.window_worst_of(
                &key,
                ["running", "units", "queue"],
                now,
                30_000,
                true,
                |[running, units, queue]| Some((running? + queue.unwrap_or(0.0)) * units?),
            )
        };
        assert_eq!(in_flight(2_000), Some(20.0));
    }

    #[test]
    fn a_joined_worst_has_the_instants_of_its_first_series() {
        let store = LoadStore::new(Duration::from_secs(60));
        store.ingest_at(
            "running{grid_site=\"a\",grid_provider=\"p\"} 10 1000\nunits{grid_site=\"a\",grid_provider=\"p\"} 2 1000\nrunning{grid_site=\"a\",grid_provider=\"p\"} 50 3000",
            3_000,
            3_000,
            "a",
        );
        let key = LoadStore::key("a", "p");
        // An instant missing the second series yields nothing.
        assert_eq!(
            store.window_worst_of(&key, ["running", "units"], 3_000, 30_000, true, |[running, units]| {
                Some(running? * units?)
            }),
            Some(20.0)
        );
        assert_eq!(
            store.window_worst_of(&key, ["units", "running"], 3_000, 30_000, true, |[units, _]| units),
            Some(2.0)
        );
        assert_eq!(
            store.window_worst_of(&key, ["absent", "running"], 3_000, 30_000, true, |[_, running]| running),
            None
        );
    }

    #[test]
    fn windowed_worst_keeps_the_min_when_higher_is_better() {
        // For a free-capacity metric higher is better, so the worst reading in the
        // window is the min; a brief recovery must not mask an earlier dip.
        let store = store();
        let key = LoadStore::key("east", "pool-a");
        store.ingest(&line("east", "pool-a", 2.0, 1_000)); // dip
        store.ingest(&line("east", "pool-a", 40.0, 5_000)); // recovered
        assert_eq!(
            store.window_worst(&key, QUEUE, 5_000, 30_000, false),
            Some(2.0),
            "higher-is-better keeps the min as the worst reading in the window"
        );
    }

    // #160 owner-binding: attribution keys on the verified owner, never the body.

    #[test]
    fn a_matching_body_site_is_attributed_to_the_owner() {
        let store = store();
        store.ingest_at(
            &line("east", "pool-a", 3.0, 1_000),
            NO_SKEW_NOW_MS,
            NO_SKEW_NOW_MS,
            "east",
        );
        assert_eq!(
            store
                .latest(&LoadStore::key("east", "pool-a"), QUEUE)
                .map(|sample| sample.value),
            Some(3.0),
            "an agreeing label lands under the owner"
        );
    }

    #[test]
    fn an_ingest_counts_what_it_kept_and_why_it_dropped_the_rest() {
        let store = LoadStore::new(Duration::from_secs(60)).with_local_site("hub");
        let text = [
            r#"m{grid_site="east",grid_provider="p"} 1 1000"#,
            r#"m{grid_site="west",grid_provider="p"} 1 1000"#,
            r#"m{grid_site="east",grid_provider="q"} 1 9000000"#,
            "not a sample",
            "# HELP m a comment, not a drop",
            "",
        ]
        .join("\n");
        let tally = store.ingest_at(&text, 1_000, 1_000, "east");
        assert_eq!(
            tally,
            Ingested {
                kept: 1,
                skewed: 1,
                mismatched: 1,
                capped: 0,
                unparsed: 1,
            }
        );
        assert_eq!(tally.dropped().iter().map(|(_, count)| count).sum::<u64>(), 3);
    }

    #[test]
    fn a_sample_re_served_across_polls_is_one_observation() {
        let store = LoadStore::new(Duration::from_secs(60));
        let key = LoadStore::key("a", "p");
        let held = || {
            store
                .providers
                .get(&key)
                .expect("provider")
                .metrics
                .get("m")
                .expect("metric")
                .samples
                .len()
        };
        let line = r#"m{grid_site="a",grid_provider="p"} 1 1000"#;
        store.ingest_at(line, 1_000, 1_000, "a");
        // The next poll re-serves the same published stamp: Date a second on, the local
        // clock a second and a half on, so the restamp lands later than what is held.
        store.ingest_at(line, 2_000, 2_500, "a");
        assert_eq!(held(), 1, "a restamped republication is not a new observation");
        store.ingest_at(r#"m{grid_site="a",grid_provider="p"} 2 6000"#, 6_000, 6_500, "a");
        assert_eq!(held(), 2, "a new published stamp is");
    }

    #[test]
    fn a_row_from_the_local_operator_is_attributed_by_its_label() {
        let store = store().with_local_site("east");
        store.ingest_at(
            &line("west", "pool-a", 9.0, 1_000),
            NO_SKEW_NOW_MS,
            NO_SKEW_NOW_MS,
            "east",
        );
        assert_eq!(
            store
                .latest(&LoadStore::key("west", "pool-a"), QUEUE)
                .map(|sample| sample.value),
            Some(9.0),
            "the local operator relays west, so the row lands under west"
        );
        assert!(
            store.latest(&LoadStore::key("east", "pool-a"), QUEUE).is_none(),
            "and is not bound to the operator's own site"
        );
    }

    #[test]
    fn a_peer_other_than_the_local_operator_is_still_owner_bound() {
        let store = store().with_local_site("east");
        store.ingest_at(
            &line("west", "pool-a", 9.0, 1_000),
            NO_SKEW_NOW_MS,
            NO_SKEW_NOW_MS,
            "north",
        );
        assert_eq!(
            store.provider_count(),
            0,
            "a non-local peer claiming another site is dropped as before"
        );
    }

    #[test]
    fn the_per_site_cap_follows_the_attributed_site_for_local_rows() {
        let store = store().with_local_site("east");
        let mut text = String::new();
        for site in 0..3 {
            for pool in 0..MAX_PROVIDERS_PER_OWNER {
                text.push_str(&line(&format!("site-{site}"), &format!("pool-{pool}"), 1.0, 1_000));
                text.push('\n');
            }
        }
        store.ingest_at(&text, NO_SKEW_NOW_MS, NO_SKEW_NOW_MS, "east");
        assert_eq!(
            store.provider_count(),
            3 * MAX_PROVIDERS_PER_OWNER,
            "each relayed site gets its own cap, not one shared cap under the operator"
        );
    }

    #[test]
    fn a_disagreeing_body_site_is_dropped() {
        let store = store();
        // A verified "east" peer claims "west" in the body: the cross-site spoof.
        store.ingest_at(
            &line("west", "pool-a", 9.0, 1_000),
            NO_SKEW_NOW_MS,
            NO_SKEW_NOW_MS,
            "east",
        );
        assert_eq!(store.provider_count(), 0, "a disagreeing label is dropped, not stored");
        assert!(
            store.latest(&LoadStore::key("west", "pool-a"), QUEUE).is_none(),
            "the spoofed west series must not exist"
        );
        assert!(
            store.latest(&LoadStore::key("east", "pool-a"), QUEUE).is_none(),
            "and it is not silently rebound to east either"
        );
    }

    #[test]
    fn an_absent_body_site_is_stamped_with_the_owner() {
        let store = store();
        // A line with grid_provider but no grid_site: attributed to the verified owner.
        store.ingest_at(
            &format!(r#"{QUEUE}{{grid_provider="pool-a"}} 7 1000"#),
            NO_SKEW_NOW_MS,
            NO_SKEW_NOW_MS,
            "east",
        );
        assert_eq!(
            store
                .latest(&LoadStore::key("east", "pool-a"), QUEUE)
                .map(|sample| sample.value),
            Some(7.0),
            "an unlabeled line is attributed to the owner, not dropped"
        );
    }

    #[test]
    fn each_line_is_bound_against_the_owner_independently() {
        let store = store();
        let body = format!(
            "{}\n{}",
            line("east", "pool-a", 3.0, 1_000),
            line("west", "pool-b", 9.0, 1_000),
        );
        store.ingest_at(&body, NO_SKEW_NOW_MS, NO_SKEW_NOW_MS, "east");
        assert_eq!(store.provider_count(), 1, "only the owner's line lands");
        assert_eq!(
            store
                .latest(&LoadStore::key("east", "pool-a"), QUEUE)
                .map(|sample| sample.value),
            Some(3.0),
            "the owner line is kept"
        );
        assert!(
            store.latest(&LoadStore::key("west", "pool-b"), QUEUE).is_none(),
            "the disagreeing line is dropped per line"
        );
    }

    #[test]
    fn rebase_age_restamps_a_sample_onto_the_local_clock() {
        // A sample 3s old on the peer clock lands 3s old on the local clock,
        // whatever the absolute skew between the two clocks.
        assert_eq!(
            rebase_age(100_000, 97_000, 5_000),
            2_000,
            "3s old, restamped onto local now"
        );
        // A stamp ahead of the peer's own Date is implausible, so it reads fresh.
        assert_eq!(
            rebase_age(100_000, 101_000, 5_000),
            5_000,
            "a future-on-peer stamp is fresh"
        );
        // An age beyond a day is implausible, so the sample reads fresh, not old.
        assert_eq!(
            rebase_age(MAX_RELAY_AGE_MS.saturating_add(10), 0, 5_000),
            5_000,
            "an implausible age is stamped fresh"
        );
    }

    #[test]
    fn a_skewed_peer_sample_still_reads_fresh_on_the_local_clock() {
        // The peer's clock runs far ahead of ours: its Date is 9_000_000 and its
        // sample is 1s old on that clock. window_worst uses our clock. Stored raw,
        // the sample would sit ~9_000_000ms ahead of our clock and be skipped.
        // Rebased, it is 1s old locally and inside the window.
        let store = store();
        let local_now = 5_000;
        store.ingest_at(&line("east", "pool-a", 4.0, 8_999_000), 9_000_000, local_now, "east");
        let worst = store.window_worst(&LoadStore::key("east", "pool-a"), QUEUE, local_now, 30_000, true);
        assert_eq!(worst, Some(4.0), "a skewed peer's fresh sample still routes");
    }

    #[test]
    fn concurrent_admission_holds_the_per_owner_cap_exactly() {
        // Many threads race to insert distinct new providers for one owner, more
        // than the per-owner cap. Admission is atomic, so the retained count lands
        // exactly on the cap. A check-then-insert would overshoot under this race.
        let store = std::sync::Arc::new(store());
        let workers: usize = 8;
        let per_worker = MAX_PROVIDERS_PER_OWNER / 4;
        let handles: Vec<_> = (0..workers)
            .map(|worker| {
                let store = std::sync::Arc::clone(&store);
                std::thread::spawn(move || {
                    for slot in 0..per_worker {
                        let cluster = format!("pool-{worker}-{slot}");
                        let text = line("east", &cluster, 1.0, 1_000);
                        store.ingest_at(&text, NO_SKEW_NOW_MS, NO_SKEW_NOW_MS, "east");
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("worker thread");
        }
        assert!(
            workers.saturating_mul(per_worker) > MAX_PROVIDERS_PER_OWNER,
            "the test must attempt more than the cap"
        );
        assert_eq!(
            store.provider_count(),
            MAX_PROVIDERS_PER_OWNER,
            "concurrent admission bounds one owner exactly at its cap"
        );
    }
}
