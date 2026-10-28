//! Provider signals, collected by this operator and served for others to read.
//!
//! The operator already scrapes each provider to score it; this keeps what it
//! parsed and serves it, so a peer or gateway reads a provider's signals without
//! scraping it or holding its credentials. A multi-target exporter, not
//! Prometheus federation: `target` picks one provider, `collect[]` picks signals
//! by name. Held values expire rather than being marked stale, so absence marks
//! a stopped writer and no clock is compared against another's.

use std::{
    collections::{BTreeMap, HashMap},
    net::IpAddr,
    sync::{Arc, Mutex, PoisonError, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures::StreamExt as _;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};

pub use crate::crd::grid_network::PeerTrustMode;
use crate::{
    metrics_scraper::{MetricsScrapeError, PeerTransport, scrape_peer},
    resources::{
        endpoint_resolution,
        tls_backend::{self, ClientTlsConfig},
    },
    swim::MemberRecord,
};

/// The single mTLS path the coarse rollup is served and polled on.
///
/// One path for peers and the local gateway. Scope comes from the caller's
/// certificate, never the path, so no second path can widen a caller's view.
pub const SIGNALS_PATH: &str = "/v1/site/signals";

/// Response header naming the serving site's membership source, `Swim` or `Local`.
pub const MEMBERSHIP_HEADER: &str = "x-grid-membership";

/// Label naming the site a sample was observed at.
pub const SITE_LABEL: &str = "grid_site";

/// Label naming the provider a sample was observed from.
pub const PROVIDER_LABEL: &str = "grid_provider";

/// Whether a metric name is one component of a histogram or summary.
fn is_aggregate_part(metric: &str) -> bool {
    metric.ends_with("_bucket") || metric.ends_with("_sum") || metric.ends_with("_count")
}

/// One parsed sample, owned so labels can be attributed and ordering is stable.
///
/// Labels are a `BTreeMap` so two renders of the same sample are byte-identical.
#[derive(Clone, Debug, PartialEq)]
pub struct Observation {
    /// Metric name.
    pub metric: String,
    /// Labels, including any this site attributed.
    pub labels: BTreeMap<String, String>,
    /// Reported value.
    pub value: f64,
    /// The per-sample exposition timestamp as parsed, epoch milliseconds.
    ///
    /// Read once, to derive a relayed sample's [`age`], and never rendered:
    /// every stamp this site serves is written at render.
    ///
    /// [`age`]: Self::age
    pub timestamp_ms: Option<i64>,
    /// How old the sample already was when this site collected it.
    ///
    /// Zero for a locally scraped sample. On a relayed peer sample, the peer's
    /// `Date` minus the sample's own stamp, both read on the peer's clock. A
    /// render stamps the sample at its own wall reading less this age and the
    /// time held since, so a step of this site's wall clock never lands in a
    /// stored stamp.
    pub age: Duration,
}

/// The label the EPP puts the pool on.
pub(crate) const POOL_LABEL: &str = "inference_pool";

/// Whether `o` belongs to `pool`. An unlabeled series never does.
///
/// Strict on purpose: on an EPP serving more than one pool, a series carrying no
/// `inference_pool` label is the total across pools. Counting it would overstate one
/// pool's readiness and, by inflating the wait subtracted from TTFT, understate its
/// prefill. Both errors flatter the provider, so the unlabeled series is left out and
/// the caller sees the series as absent.
pub(crate) fn in_pool(o: &Observation, pool: Option<&str>) -> bool {
    pool.is_none_or(|pool| o.labels.get(POOL_LABEL).is_some_and(|p| p == pool))
}

/// Parse an exposition response into observations.
///
/// Malformed revision metadata rejects payload while preserving independently
/// valid ordering fences.
#[must_use]
pub fn parse(text: &str) -> Vec<Observation> {
    let now = SystemTime::now();
    if text.lines().any(|line| {
        split_metric_name(line.trim()).is_some_and(|(name, _)| name == grid_signals::revision::METRIC)
            && grid_signals::exposition::parse(line)
                .and_then(|metric| grid_signals::revision::parse(&metric))
                .filter(|revision| grid_signals::revision::valid_epoch(revision.publisher_epoch, now))
                .is_none()
    }) {
        return revision_fences(text, now);
    }
    let observations: Vec<_> = parse_scrape(text)
        .into_iter()
        .filter_map(|(o, republishable)| republishable.then_some(o))
        .collect();
    retain_complete_batches(observations)
}

/// Retain unique, strictly valid metadata as invalidations after a framing failure.
fn revision_fences(text: &str, now: SystemTime) -> Vec<Observation> {
    let mut groups = BTreeMap::new();
    for line in text
        .lines()
        .filter(|line| split_metric_name(line.trim()).is_some_and(|(name, _)| name == grid_signals::revision::METRIC))
    {
        let Some(observation) = parse_sample(line) else {
            continue;
        };
        let key = (
            observation.labels.get(SITE_LABEL).cloned(),
            observation.labels.get(PROVIDER_LABEL).cloned(),
        );
        let fence = grid_signals::exposition::parse(line)
            .and_then(|metric| grid_signals::revision::parse(&metric))
            .filter(|revision| grid_signals::revision::valid_epoch(revision.publisher_epoch, now))
            .map(|_| invalidation(&observation));
        groups.entry(key).and_modify(|entry| *entry = None).or_insert(fence);
    }
    groups.into_values().flatten().collect()
}

/// Refuse incomplete revision groups before timestamp-to-age conversion loses information.
fn retain_complete_batches(observations: Vec<Observation>) -> Vec<Observation> {
    if !observations
        .iter()
        .any(|sample| sample.metric == grid_signals::revision::METRIC)
    {
        return observations;
    }
    let mut groups = BTreeMap::<_, Vec<Observation>>::new();
    for observation in observations {
        let key = (
            observation.labels.get(SITE_LABEL).cloned(),
            observation.labels.get(PROVIDER_LABEL).cloned(),
        );
        groups.entry(key).or_default().push(observation);
    }
    groups
        .into_values()
        .flat_map(|group| {
            let metadata: Vec<_> = group
                .iter()
                .filter(|sample| sample.metric == grid_signals::revision::METRIC)
                .collect();
            match metadata.as_slice() {
                [] => group,
                [metadata] if complete_batch(&group.iter().collect::<Vec<_>>(), metadata) => group,
                [metadata] => vec![invalidation(metadata)],
                _ => Vec::new(),
            }
        })
        .collect()
}

/// Preserve a trustworthy revision fence without qualifying unusable payload.
fn invalidation(metadata: &Observation) -> Observation {
    let mut metadata = metadata.clone();
    metadata
        .labels
        .insert(grid_signals::revision::SAMPLES_LABEL.to_owned(), "0".to_owned());
    metadata
}

/// Parse a scrape this site made itself: every finite sample, each paired with whether
/// [`parse`] would keep it for republishing.
///
/// Counters and histogram parts stay, for this site's own latency and error windows.
#[must_use]
pub(crate) fn parse_scrape(text: &str) -> Vec<(Observation, bool)> {
    // Types first: a declaration may follow its samples, and reading in one
    // pass would admit a counter that had not been typed yet.
    let types: HashMap<&str, &str> = text
        .lines()
        .filter_map(|l| l.strip_prefix('#'))
        .filter_map(|rest| {
            let mut field = rest.split_whitespace();
            (field.next() == Some("TYPE")).then(|| field.next().zip(field.next()))?
        })
        .collect();

    text.lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .filter_map(parse_sample)
        .filter(|o| o.value.is_finite())
        .map(|o| {
            // Gauges and untyped only: a relayed counter reports our restarts,
            // and an aggregate we did not observe cannot be recombined. The name
            // check catches aggregates that arrive untyped.
            let republishable = matches!(types.get(o.metric.as_str()), None | Some(&("gauge" | "untyped")))
                && !is_aggregate_part(&o.metric);
            (o, republishable)
        })
        .collect()
}

/// Parse one sample line: `name{label="v",...} value [timestamp]`.
///
/// Hand-written because common parsers match the name with `\w+` (excluding the
/// colon in every vLLM metric) and delimit labels with `[^}]+` (stopping at a
/// brace inside a quoted value); both drop the line silently.
fn parse_sample(line: &str) -> Option<Observation> {
    let line = line.trim();
    let (metric, rest) = split_metric_name(line)?;
    let (labels, rest) = if rest.starts_with('{') {
        parse_labels(rest)?
    } else {
        (BTreeMap::new(), rest)
    };
    let mut fields = rest.split_whitespace();
    let value = fields.next()?.parse().ok()?;
    // A Prometheus line may carry a trailing millisecond timestamp. Keep it (the
    // peer poller reads it to preserve age), and tolerate a bad one as absent.
    let timestamp_ms = fields.next().and_then(|field| field.parse::<i64>().ok());
    Some(Observation {
        metric: metric.to_owned(),
        labels,
        value,
        timestamp_ms,
        age: Duration::ZERO,
    })
}

/// Split a leading metric name from the rest of the line.
fn split_metric_name(line: &str) -> Option<(&str, &str)> {
    let end = line
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == ':'))
        .unwrap_or(line.len());
    let (name, rest) = line.split_at(end);
    let first = name.chars().next()?;
    (first.is_ascii_alphabetic() || first == '_' || first == ':').then_some((name, rest))
}

/// Parse a `{...}` label set, returning it and what follows the closing brace.
fn parse_labels(rest: &str) -> Option<(BTreeMap<String, String>, &str)> {
    let mut labels = BTreeMap::new();
    let mut s = rest.strip_prefix('{')?;
    loop {
        s = s.trim_start();
        if let Some(tail) = s.strip_prefix('}') {
            return Some((labels, tail));
        }
        let (name, tail) = split_label_name(s)?;
        let tail = tail.trim_start().strip_prefix('=')?.trim_start();
        let (value, tail) = parse_label_value(tail)?;
        labels.insert(name.to_owned(), value);
        s = tail.trim_start();
        s = s.strip_prefix(',').unwrap_or(s);
    }
}

/// Split a leading label name.
fn split_label_name(s: &str) -> Option<(&str, &str)> {
    let end = s
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(s.len());
    (end > 0).then(|| s.split_at(end))
}

/// Parse a quoted label value, honouring the three escapes the format defines.
fn parse_label_value(s: &str) -> Option<(String, &str)> {
    let mut rest = s.strip_prefix('"')?.chars();
    let mut value = String::new();
    loop {
        match rest.next()? {
            '"' => return Some((value, rest.as_str())),
            '\\' => value.push(match rest.next()? {
                'n' => '\n',
                other => other,
            }),
            c => value.push(c),
        }
    }
}

/// Attach the labels this site is authoritative for, preserving a provider's
/// own value under an `exported_` name. Works on parsed labels so a duplicate
/// label, which a scraper would reject, cannot be emitted.
#[must_use]
pub fn attribute(observations: Vec<Observation>, site: &str, provider: &str) -> Vec<Observation> {
    observations
        .into_iter()
        .map(|mut o| {
            for (key, value) in [(SITE_LABEL, site), (PROVIDER_LABEL, provider)] {
                if let Some(theirs) = o.labels.remove(key) {
                    o.labels.insert(format!("exported_{key}"), theirs);
                }
                o.labels.insert(key.to_owned(), value.to_owned());
            }
            o
        })
        .collect()
}

/// One reading of the two clocks a render needs.
///
/// Taken once per response and handed to every render in it, so each stamp and
/// the response's `Date` come from the same wall reading and a reader recovers
/// each age exactly.
#[derive(Clone, Copy, Debug)]
pub struct ClockReading {
    /// The monotonic clock, which ages held values.
    pub instant: Instant,
    /// The wall clock, which stamps them and dates the response.
    pub wall: SystemTime,
}

impl ClockReading {
    /// Read both clocks now.
    #[must_use]
    pub fn now() -> Self {
        Self {
            instant: Instant::now(),
            wall: SystemTime::now(),
        }
    }
}

/// What is held for one target.
#[derive(Clone, Debug)]
struct Cached {
    /// Parsed samples, already attributed.
    samples: Arc<[Observation]>,
    /// When the value was collected, on this process's monotonic clock.
    /// Reported as `Age` (a duration, not a time), so no two clocks are compared.
    collected_at: Instant,
    /// When this stops being served.
    expires_at: Instant,
}

/// The providers each peer has sent load rows for since this process started, its most recently
/// heard [`MAX_PEER_PROVIDERS`], so a candidate that publishes load can be told from one that
/// never will.
#[derive(Clone, Debug, Default)]
pub struct LoadHeard(Arc<RwLock<BTreeMap<String, std::collections::VecDeque<String>>>>);

impl LoadHeard {
    /// Note each provider `peer` sent a load row for in `observations`, keeping each peer's
    /// [`MAX_PEER_PROVIDERS`] most recently heard, so a renamed provider is learned, not refused.
    pub fn record(&self, peer: &str, observations: &[Observation]) {
        let Ok(mut heard) = self.0.write() else {
            return;
        };
        let held = heard.entry(peer.to_owned()).or_default();
        for observation in observations {
            if observation.metric == grid_signals::revision::METRIC {
                continue;
            }
            let Some(provider) = observation.labels.get(PROVIDER_LABEL) else {
                continue;
            };
            if let Some(at) = held.iter().position(|seen| seen == provider) {
                held.remove(at);
            } else if held.len() >= MAX_PEER_PROVIDERS {
                held.pop_front();
            }
            held.push_back(provider.clone());
        }
    }

    /// Forget every site `keep` no longer names.
    pub fn retain_sites<Keep: Fn(&str) -> bool>(&self, keep: Keep) {
        if let Ok(mut heard) = self.0.write() {
            heard.retain(|site, _| keep(site));
        }
    }

    /// Whether `site` has sent load for `provider`.
    #[must_use]
    pub fn heard(&self, site: &str, provider: &str) -> bool {
        self.0.read().is_ok_and(|heard| {
            heard
                .get(site)
                .is_some_and(|held| held.iter().any(|seen| seen == provider))
        })
    }

    /// Whether `site` has sent load for any provider.
    #[must_use]
    pub fn any_from(&self, site: &str) -> bool {
        self.0
            .read()
            .is_ok_and(|heard| heard.get(site).is_some_and(|held| !held.is_empty()))
    }
}

/// When the last poll round that included each peer ended, for this site's own gateway.
#[derive(Clone, Debug, Default)]
pub struct PeerRounds(Arc<RwLock<BTreeMap<String, SystemTime>>>);

/// How far each polled peer is from caught up after a restart: whether its catalog is held,
/// and how many rounds in a row it has gone unanswered. `None` before the first round.
#[derive(Clone, Debug, Default)]
pub struct CatchUp(Arc<RwLock<Option<HashMap<String, PeerCatchUp>>>>);

/// One polled peer's standing in [`CatchUp`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PeerCatchUp {
    /// Whether this site holds the peer's catalog.
    pub held: bool,
    /// Rounds in a row the peer has not answered.
    pub unanswered: u32,
}

impl CatchUp {
    /// Record a round over `polled`; a peer not in it is no longer polled.
    pub fn record<Answered: Fn(&str) -> bool, Held: Fn(&str) -> bool>(
        &self,
        polled: &[String],
        answered: Answered,
        held: Held,
    ) {
        let Ok(mut standing) = self.0.write() else {
            return;
        };
        let previous = standing.take().unwrap_or_default();
        *standing = Some(
            polled
                .iter()
                .map(|peer| {
                    let before = previous.get(peer).copied().unwrap_or_default();
                    let unanswered = if answered(peer) {
                        0
                    } else {
                        before.unanswered.saturating_add(1)
                    };
                    (
                        peer.clone(),
                        PeerCatchUp {
                            held: held(peer),
                            unanswered,
                        },
                    )
                })
                .collect(),
        );
    }

    /// Whether every polled peer's catalog is held, or it has gone `rounds` rounds unanswered.
    /// False before the first round, true for a round that polled no one.
    #[must_use]
    pub fn caught_up(&self, rounds: u32) -> bool {
        self.0.read().is_ok_and(|standing| {
            standing
                .as_ref()
                .is_some_and(|peers| peers.values().all(|peer| peer.held || peer.unanswered >= rounds))
        })
    }
}

impl PeerRounds {
    /// Record that a round polling `peers` ended `at`; a peer not in it is no longer polled.
    pub fn record(&self, peers: &[String], at: SystemTime) {
        if let Ok(mut rounds) = self.0.write() {
            *rounds = peers.iter().map(|peer| (peer.clone(), at)).collect();
        }
    }

    /// One row per peer, stamped at its last round, ahead of anything a byte budget can drop.
    #[must_use]
    pub fn render(&self) -> String {
        let Ok(rounds) = self.0.read() else {
            return String::new();
        };
        let mut out = String::new();
        // Names come from `GridSite` identities, so none needs escaping; one that would is skipped.
        for (peer, at) in rounds
            .iter()
            .filter(|(peer, _)| certs::validate_site_name(peer).is_ok())
        {
            let stamp = at
                .duration_since(UNIX_EPOCH)
                .map_or(0, |since| i64::try_from(since.as_millis()).unwrap_or(i64::MAX));
            for part in [
                grid_signals::poller::METRIC,
                "{",
                SITE_LABEL,
                "=\"",
                peer,
                "\",",
                PROVIDER_LABEL,
                "=\"",
                grid_signals::poller::PROVIDER,
                "\"} 1 ",
                &stamp.to_string(),
                "\n",
            ] {
                out.push_str(part);
            }
        }
        out
    }
}

/// Signals held per target, where a target is a provider routing identity.
#[derive(Clone, Debug, Default)]
pub struct SignalStore {
    /// Target to what is held for it.
    inner: Arc<RwLock<BTreeMap<String, Cached>>>,
    /// Maps each target to the selectors a reader must satisfy to be served it.
    ///
    /// Every selector applies, so routing permission and metric readability
    /// compose without either widening the other. Held apart from the samples
    /// because a policy changes only when its provider is edited, samples every
    /// scrape.
    access: Arc<RwLock<AccessMap>>,
    /// Set once the first refresh lands.
    published: Arc<tokio::sync::watch::Sender<bool>>,
}

/// SHA-256 of a DER certificate, lowercase hex.
///
/// The key [`PeerIdentities`] is built on. Matches the canonical fingerprint the
/// gateway probe pins with, so a site is known by one hash everywhere.
#[must_use]
pub fn leaf_fingerprint(der: &[u8]) -> String {
    tls_backend::sha256(der).iter().map(|b| format!("{b:02x}")).collect()
}

/// Normalise a declared fingerprint to the form [`leaf_fingerprint`] emits.
///
/// Strips colons and lowercases so the raw-compare serve side agrees with the
/// separator-tolerant poll side.
#[must_use]
pub fn canonical_fingerprint(declared: &str) -> String {
    declared
        .chars()
        .filter(|c| *c != ':')
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// What this site holds about one peer.
#[derive(Clone, Debug, Default)]
pub struct PeerRecord {
    /// Labels a policy is matched against.
    ///
    /// The local `GridSite` object's, never the peer's own claim: a peer that
    /// asserted its labels could assert past any policy written about it.
    pub labels: BTreeMap<String, String>,

    /// Leaf fingerprints that name this peer, from
    /// `spec.trust.canonicalFingerprints`.
    ///
    /// A pin is the identity: a caller is whoever presents one of these keys.
    /// Empty means unenrolled, and the serve and poll paths refuse it rather
    /// than falling back to a certificate name.
    pub pins: Vec<String>,

    /// Where this peer serves its signals, derived from the local object's gateway endpoint.
    pub signals: Option<DeclaredSignals>,
}

/// The server name `site`'s gateway is reached and verified under, the DNS name its leaf carries.
#[must_use]
pub fn gateway_server_name(site: &str) -> String {
    format!("{site}.{}", certs::SPIFFE_TRUST_DOMAIN)
}

/// The server name `site`'s signals listener is reached under, beside its gateway's own.
#[must_use]
pub fn signals_server_name(site: &str) -> String {
    format!("{site}-signals.{}", certs::SPIFFE_TRUST_DOMAIN)
}

/// Where a `GridSite` says its site serves signals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeclaredSignals {
    /// Where to connect.
    pub endpoint: SignalsEndpoint,
    /// The TLS server name presented, when a router in front selects on one.
    pub server_name: Option<String>,
}

impl DeclaredSignals {
    /// The poll target for `name`: the endpoint, under the server name when one is declared.
    #[must_use]
    pub fn peer_site(&self, name: String) -> Option<PeerSite> {
        let (url, alias) = match &self.server_name {
            Some(server_name) => (
                SignalsEndpoint::url_at("https", &format!("{server_name}:{}", self.endpoint.port))?,
                Some((server_name.clone(), self.endpoint.host().into_owned())),
            ),
            None => (self.endpoint.url("https")?, None),
        };
        Some(PeerSite {
            name,
            url,
            pins: Vec::new(),
            answering: true,
            alias,
        })
    }
}

/// Who may read, keyed by site name.
///
/// The name is the map key; a record's pins are what authorize a caller. A
/// rotated key changes the pins, not the name, so the record survives renewal.
#[derive(Clone, Debug, Default)]
pub struct PeerIdentities {
    /// Site name to what is held for it.
    inner: Arc<RwLock<BTreeMap<String, PeerRecord>>>,
    /// Whether the peers have been read from the `GridSite` objects at least once.
    loaded: Arc<std::sync::atomic::AtomicBool>,
}

impl PeerIdentities {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace what is known about every peer.
    pub fn set(&self, identities: BTreeMap<String, PeerRecord>) {
        if let Ok(mut guard) = self.inner.write() {
            *guard = identities;
        }
        self.loaded.store(true, std::sync::atomic::Ordering::Release);
    }

    /// Whether the peers have been read at least once, so an empty registry means none.
    #[must_use]
    pub fn loaded(&self) -> bool {
        self.loaded.load(std::sync::atomic::Ordering::Acquire)
    }

    /// How many peers are held.
    #[must_use]
    pub fn count(&self) -> usize {
        self.inner.read().map_or(0, |guard| guard.len())
    }

    /// Whether a record is held for `site`.
    #[must_use]
    pub fn knows(&self, site: &str) -> bool {
        self.inner.read().is_ok_and(|guard| guard.contains_key(site))
    }

    /// Fingerprints declared for `site`, empty when none are.
    #[must_use]
    pub fn pins_for(&self, site: &str) -> Vec<String> {
        let Ok(held) = self.inner.read() else {
            return Vec::new();
        };
        held.get(site).map(|record| record.pins.clone()).unwrap_or_default()
    }

    /// Whether this site refuses `site` outright: no record, or a record with no
    /// pins. Read before polling and before serving, so a refusal stops traffic
    /// both ways.
    #[must_use]
    pub fn refuses(&self, site: &str) -> bool {
        let Ok(held) = self.inner.read() else {
            return true;
        };
        held.get(site).is_none_or(|record| record.pins.is_empty())
    }

    /// Each known peer with no signals endpoint to poll it at, never `local_site`, whose own
    /// record this site is not a peer of.
    #[must_use]
    pub fn without_endpoint(&self, local_site: &str) -> Vec<String> {
        let Ok(held) = self.inner.read() else {
            return Vec::new();
        };
        held.iter()
            .filter(|(site, record)| record.signals.is_none() && *site != local_site)
            .map(|(site, _)| site.clone())
            .collect()
    }

    /// Each peer whose local object declares its signals endpoint.
    #[must_use]
    pub fn declared_endpoints(&self) -> Vec<(String, DeclaredSignals)> {
        let Ok(held) = self.inner.read() else {
            return Vec::new();
        };
        held.iter()
            .filter_map(|(site, record)| Some((site.clone(), record.signals.clone()?)))
            .collect()
    }

    /// Labels held for `site`, whether or not it declared pins.
    #[must_use]
    pub fn labels_for(&self, site: &str) -> Option<BTreeMap<String, String>> {
        self.inner.read().ok()?.get(site).map(|record| record.labels.clone())
    }

    /// Labels for a caller presenting `leaf_sha256`.
    ///
    /// A key nobody pinned names nobody, so an unknown peer is refused, not
    /// merely unprivileged. Preferred over reading a name from the certificate
    /// because a stolen CA can mint any name but not another site's key. Scans
    /// rather than indexes: a grid is sites, not endpoints.
    #[must_use]
    pub fn resolve_by_key(&self, leaf_sha256: &str) -> Option<BTreeMap<String, String>> {
        self.resolve_site_by_key(leaf_sha256).map(|(_, labels)| labels)
    }

    /// The site pinned to `leaf_sha256`, with its labels.
    #[must_use]
    pub fn resolve_site_by_key(&self, leaf_sha256: &str) -> Option<(String, BTreeMap<String, String>)> {
        let Ok(held) = self.inner.read() else {
            return None;
        };
        held.iter()
            .find(|(_, record)| record.pins.iter().any(|pin| pin == leaf_sha256))
            .map(|(site, record)| (site.clone(), record.labels.clone()))
    }
}

/// Selectors a reader must satisfy, all of them, to be served one target.
pub type Selectors = Vec<BTreeMap<String, String>>;

/// Access rules per target.
pub type AccessMap = BTreeMap<String, Selectors>;

/// Whether `have` carries every label `required` demands.
///
/// The one place this rule is written. `evaluate_access_policy` calls it too,
/// so an overlay decision and an endpoint decision cannot disagree.
#[must_use]
pub fn labels_satisfy(required: &BTreeMap<String, String>, have: &BTreeMap<String, String>) -> bool {
    required.iter().all(|(key, value)| have.get(key) == Some(value))
}

/// Whether a reader carrying `have` may be served a target.
///
/// Every selector applies. Fails closed: a restricted target is withheld from a
/// reader whose labels are unknown, which is what `AccessPolicyResult::Unknown`
/// means on the overlay.
#[must_use]
fn permits(required: &Selectors, have: Option<&BTreeMap<String, String>>) -> bool {
    if required.iter().all(BTreeMap::is_empty) {
        return true;
    }
    have.is_some_and(|labels| required.iter().all(|selector| labels_satisfy(selector, labels)))
}

impl SignalStore {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the access rules for every target.
    ///
    /// A target absent from `access` is unrestricted, which is what an empty
    /// `siteSelector` means on the provider.
    pub fn set_access(&self, access: AccessMap) {
        if let Ok(mut guard) = self.access.write() {
            *guard = access;
        }
    }

    /// Refresh the given targets and drop anything past its deadline.
    ///
    /// A target absent from `collected` is left alone rather than removed, so a
    /// single failed scrape does not erase what is known. It expires on its own
    /// once nothing refreshes it.
    pub fn refresh(&self, collected: BTreeMap<String, Vec<Observation>>, ttl: Duration) {
        let now = Instant::now();
        let Ok(mut guard) = self.inner.write() else {
            return;
        };
        for (target, samples) in collected {
            guard.insert(
                target,
                Cached {
                    samples: samples.into(),
                    collected_at: now,
                    expires_at: now + ttl,
                },
            );
        }
        guard.retain(|_, held| held.expires_at > now);
        drop(guard);
        self.published
            .send_if_modified(|published| !std::mem::replace(published, true));
    }

    /// Resolve once the first refresh has landed, at once if one already has.
    pub async fn published(&self) {
        drop(self.published.subscribe().wait_for(|published| *published).await);
    }

    /// Targets this reader may not be served.
    ///
    /// Decided before the samples are locked, so a denial costs no work and the
    /// two locks are never held together.
    fn denied(&self, reader: Option<&BTreeMap<String, String>>) -> std::collections::BTreeSet<String> {
        let Ok(access) = self.access.read() else {
            return std::collections::BTreeSet::new();
        };
        access
            .iter()
            .filter(|(_, required)| !permits(required, reader))
            .map(|(target, _)| target.clone())
            .collect()
    }

    /// Render exposition for `target`, or for every target when it is `None`.
    ///
    /// `reader` is the caller's site labels, from the connection not a parameter.
    /// A denied target is never rendered, so permission withholds data rather
    /// than trimming it after; `collect` narrows only within what is allowed.
    /// Each sample is stamped at `now`'s wall reading less its age, so the
    /// response's `Date` belongs to the same `now`. Returns the body and the age
    /// of its oldest value, so `Age` bounds the whole response.
    #[must_use]
    pub fn render(
        &self,
        target: Option<&str>,
        collect: &[String],
        reader: Option<&BTreeMap<String, String>>,
        now: ClockReading,
    ) -> (String, Duration) {
        self.render_with(target, collect, &self.denied(reader), now)
    }

    /// Render every target, applying no access policy.
    ///
    /// For a `Local` caller only, the site's own data plane, which is entitled
    /// to the whole grid view. Access policy scopes peer reads, not the site.
    #[must_use]
    #[cfg(test)]
    pub fn render_unrestricted(
        &self,
        target: Option<&str>,
        collect: &[String],
        now: ClockReading,
    ) -> (String, Duration) {
        self.render_with(target, collect, &std::collections::BTreeSet::new(), now)
    }

    /// Render for this site's own gateway: contract series only, each target cut
    /// at [`MAX_RENDER_LINES_PER_TARGET`], and whole targets dropped once the body
    /// would pass `budget` bytes.
    ///
    /// The gateway reads every site from one response under its own byte
    /// ceiling, past which the whole poll fails and every site goes unmeasured.
    /// Bounding here turns that cliff into a counted loss of the last targets.
    /// Stamped from `now` as [`Self::render`] is.
    #[must_use]
    pub fn render_bounded(
        &self,
        target: Option<&str>,
        collect: &[String],
        budget: usize,
        now: ClockReading,
    ) -> Rendered {
        let Ok(guard) = self.inner.read() else {
            return Rendered::default();
        };
        let mut rendered = Rendered::default();
        for (name, held) in guard.iter() {
            if held.expires_at <= now.instant || target.is_some_and(|t| t != name) {
                continue;
            }
            let held_for = now.instant.saturating_duration_since(held.collected_at);
            let (block, block_oldest, cut) = render_contract_block(&held.samples, collect, held_for, now.wall);
            rendered.lines_cut = rendered.lines_cut.saturating_add(cut);
            if rendered.body.len().saturating_add(block.len()) > budget {
                rendered.targets_dropped = rendered.targets_dropped.saturating_add(1);
                continue;
            }
            rendered.body.push_str(&block);
            rendered.oldest = rendered.oldest.max(block_oldest);
        }
        rendered
    }

    /// Shared render body over a precomputed set of denied targets.
    fn render_with(
        &self,
        target: Option<&str>,
        collect: &[String],
        denied: &std::collections::BTreeSet<String>,
        now: ClockReading,
    ) -> (String, Duration) {
        let Ok(guard) = self.inner.read() else {
            return (String::new(), Duration::ZERO);
        };
        let mut out = String::new();
        let mut oldest = Duration::ZERO;
        for (name, held) in guard.iter() {
            if held.expires_at <= now.instant || target.is_some_and(|t| t != name) {
                continue;
            }
            if denied.contains(name.as_str()) {
                continue;
            }
            // TTL uses a monotonic instant; exposition wants epoch millis, so
            // derive from the age rather than store a wall clock that can jump.
            let held_for = now.instant.saturating_duration_since(held.collected_at);
            let (block, age, _) = render_batches(&held.samples, collect, held_for, now.wall, false);
            out.push_str(&block);
            oldest = oldest.max(age);
        }
        (out, oldest)
    }

    /// Every sample still served, across targets.
    #[must_use]
    pub fn current(&self) -> Vec<Observation> {
        let Ok(guard) = self.inner.read() else {
            return Vec::new();
        };
        let now = Instant::now();
        guard
            .values()
            .filter(|held| held.expires_at > now)
            .flat_map(|held| held.samples.iter().cloned())
            .collect()
    }

    /// Targets currently held and unexpired.
    #[must_use]
    pub fn targets(&self) -> Vec<String> {
        let Ok(guard) = self.inner.read() else {
            return Vec::new();
        };
        let now = Instant::now();
        guard
            .iter()
            .filter(|(_, held)| held.expires_at > now)
            .map(|(name, _)| name.clone())
            .collect()
    }
}

/// Render one observation as an exposition line, stamped with `stamp_ms`.
///
/// The trailing timestamp is load-bearing: a reader takes the sample's age as the
/// response `Date` less it.
fn render_sample(out: &mut String, o: &Observation, stamp_ms: i64) {
    // Written into the caller's buffer rather than returned. Building a string
    // per label, collecting them, joining them and then formatting the result
    // allocated once per label plus four more per sample, all of it discarded
    // into a buffer that was going to be grown anyway.
    out.push_str(&o.metric);
    if !o.labels.is_empty() {
        out.push('{');
        for (i, (key, value)) in o.labels.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push_str("=\"");
            escape_into(out, value);
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(&o.value.to_string());
    out.push(' ');
    out.push_str(&stamp_ms.to_string());
}

/// Epoch milliseconds for an observation collected `age` ago.
///
/// The consumer reads this against the response `Date`, so both ends of the
/// subtraction come from one reading of this host's clock and no foreign clock
/// is imported.
fn wall_millis(now: SystemTime, age: Duration) -> i64 {
    let since_epoch = now
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .saturating_sub(age);
    i64::try_from(since_epoch.as_millis()).unwrap_or(i64::MAX)
}

/// Escape a label value for exposition.
fn escape_into(out: &mut String, value: &str) {
    // Single pass, no intermediate allocation.
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str(r"\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str(r"\n"),
            other => out.push(other),
        }
    }
}

/// A peer site to collect from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerSite {
    /// Site name, as used in membership.
    pub name: String,
    /// Where to read that site's signals.
    pub url: String,
    /// Fingerprints this site declared for that peer.
    ///
    /// The peer's certificate is verified against these, not the name in the
    /// URL: a pin asserts a known key, not merely that some authority signed a
    /// name (which need not match the advertised address anyway).
    pub pins: Vec<String>,

    /// Whether the site gets the full budget: it answered when last polled, has not been
    /// polled, or is due a full retry. Any other gets [`UNANSWERED_PEER_BUDGET`], so a peer that
    /// cannot answer frees its slot quickly.
    pub answering: bool,

    /// `(server name, host)` when `url` names the TLS server name a router in front of the
    /// peer selects on and the host is what is dialled; `None` dials the host in `url`.
    pub alias: Option<(String, String)>,
}

impl PeerSite {
    /// The alias as borrowed parts.
    #[must_use]
    pub fn alias(&self) -> Option<(&str, &str)> {
        self.alias.as_ref().map(|(name, host)| (name.as_str(), host.as_str()))
    }
}

/// What bounds one peer request's attempts.
#[derive(Clone, Copy, Debug)]
struct Attempt {
    /// When the request began.
    started: Instant,
    /// The most it may take, attempts included.
    budget: Duration,
    /// Whether its retries count in the peer-poll metrics.
    counted: Counted,
    /// The largest body read.
    max_body: usize,
}

impl Attempt {
    /// A signals poll, counted in the peer-poll metrics.
    const fn signals(started: Instant, budget: Duration) -> Self {
        Self {
            started,
            budget,
            counted: Counted::Yes,
            max_body: crate::metrics_scraper::MAX_RESPONSE_BODY_BYTES,
        }
    }

    /// A catalog read, not counted.
    const fn catalog(started: Instant, budget: Duration) -> Self {
        Self {
            started,
            budget,
            counted: Counted::No,
            max_body: crate::site_providers::MAX_BODY_BYTES,
        }
    }
}

/// Whether a request's retries count in the peer-poll metrics, which track signals only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Counted {
    /// A signals poll.
    Yes,
    /// A catalog read, visible in its log line by path.
    No,
}

/// The most one poll of a peer that did not answer last time may take, attempts included.
pub const UNANSWERED_PEER_BUDGET: Duration = Duration::from_secs(2);

/// Members other than this site, addressed at their dialable signals endpoint.
pub fn peer_sites<'member, Members>(
    members: Members,
    local_site: &str,
    scheme: &str,
    fallback_port: u16,
) -> Vec<PeerSite>
where
    Members: Iterator<Item = &'member MemberRecord>,
{
    members
        .filter(|member| member.site_id != local_site)
        .filter_map(|member| {
            Some(PeerSite {
                name: member.site_id.clone(),
                url: dialable_signals_endpoint(member, fallback_port)?.url(scheme)?,
                pins: Vec::new(),
                answering: true,
                alias: None,
            })
        })
        .collect()
}

/// Signals port dialed for a peer that gossips no signals endpoint, unless configured.
pub const DEFAULT_PEER_PORT: u16 = 9091;

/// A peer signals endpoint: an IP literal or a DNS name, and a port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignalsEndpoint {
    /// Where to connect.
    host: EndpointHost,
    /// TCP port, never zero.
    port: u16,
}

/// The host half of a [`SignalsEndpoint`].
#[derive(Clone, Debug, PartialEq, Eq)]
enum EndpointHost {
    /// An IP literal.
    Ip(IpAddr),
    /// A lowercase DNS name.
    Dns(String),
}

impl SignalsEndpoint {
    /// Parse `SocketAddr` text or `dns-name:port`, refusing anything that could carry a URL part.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        if let Ok(addr) = text.parse::<std::net::SocketAddr>() {
            return (addr.port() != 0).then(|| Self {
                host: EndpointHost::Ip(addr.ip()),
                port: addr.port(),
            });
        }
        let (host, port) = text.rsplit_once(':')?;
        if port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()) || !is_dns_name(host) {
            return None;
        }
        let port = port.parse::<u16>().ok().filter(|port| *port != 0)?;
        Some(Self {
            host: EndpointHost::Dns(host.to_ascii_lowercase()),
            port,
        })
    }

    /// The same host on `port`.
    #[must_use]
    pub fn with_port(self, port: u16) -> Self {
        Self { port, ..self }
    }

    /// `host:port`, IPv6 bracketed.
    #[must_use]
    pub fn authority(&self) -> String {
        match &self.host {
            EndpointHost::Ip(ip) => std::net::SocketAddr::new(*ip, self.port).to_string(),
            EndpointHost::Dns(name) => format!("{name}:{}", self.port),
        }
    }

    /// Whether it stays off loopback, link-local, and cloud metadata addresses.
    #[must_use]
    pub fn is_dialable(&self) -> bool {
        match &self.host {
            EndpointHost::Ip(ip) => is_dialable_ip(*ip),
            EndpointHost::Dns(name) => !crate::resources::mcp_probe::is_blocked_hostname(name),
        }
    }

    /// The host half as text.
    #[must_use]
    pub fn host(&self) -> std::borrow::Cow<'_, str> {
        match &self.host {
            EndpointHost::Ip(ip) => std::borrow::Cow::Owned(ip.to_string()),
            EndpointHost::Dns(name) => std::borrow::Cow::Borrowed(name),
        }
    }

    /// The signals URL under `scheme`, built from validated parts.
    #[must_use]
    pub fn url(&self, scheme: &str) -> Option<String> {
        Self::url_at(scheme, &self.authority())
    }

    /// The signals URL under `scheme` at `authority`.
    fn url_at(scheme: &str, authority: &str) -> Option<String> {
        http::Uri::builder()
            .scheme(scheme)
            .authority(authority)
            .path_and_query(SIGNALS_PATH)
            .build()
            .ok()
            .map(|uri| uri.to_string())
    }
}

/// A DNS name of letter, digit, and hyphen labels, not ending in an all-digit label.
pub(crate) fn is_dns_name(host: &str) -> bool {
    let label_ok = |label: &str| {
        (1..=63).contains(&label.len())
            && label.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            && !label.starts_with('-')
            && !label.ends_with('-')
    };
    (1..=253).contains(&host.len())
        && host.split('.').all(label_ok)
        && !host
            .rsplit('.')
            .next()
            .is_some_and(|last| last.bytes().all(|b| b.is_ascii_digit()))
}

/// A member's dialable signals endpoint, gossiped else its SWIM host on `fallback_port`, refusals warned once.
#[must_use]
pub fn dialable_signals_endpoint(member: &MemberRecord, fallback_port: u16) -> Option<SignalsEndpoint> {
    let (text, endpoint) = match &member.signals_address {
        Some(advertised) => (advertised.as_str(), SignalsEndpoint::parse(advertised)),
        None => (
            member.endpoint.as_str(),
            SignalsEndpoint::parse(&member.endpoint).map(|swim| swim.with_port(fallback_port)),
        ),
    };
    let endpoint = endpoint.filter(SignalsEndpoint::is_dialable);
    if endpoint.is_none() {
        warn_refused_once(&member.site_id, text);
    }
    endpoint
}

/// Refused `(site, endpoint)` pairs already warned, bounded.
static REFUSED_WARNED: std::sync::LazyLock<Mutex<std::collections::HashSet<(String, String)>>> =
    std::sync::LazyLock::new(Default::default);

/// Refused pairs remembered before the set starts over.
const REFUSED_WARNED_MAX: usize = 1_024;

/// Warn about a refused endpoint the first time a peer gossips it, `true` when warned.
fn warn_refused_once(site: &str, endpoint: &str) -> bool {
    let endpoint: String = endpoint.chars().take(128).collect();
    let mut warned = REFUSED_WARNED.lock().unwrap_or_else(PoisonError::into_inner);
    if warned.len() >= REFUSED_WARNED_MAX {
        warned.clear();
    }
    let first = warned.insert((site.to_owned(), endpoint.clone()));
    drop(warned);
    if first {
        tracing::warn!(site, endpoint, "refusing a peer's signals endpoint");
    } else {
        tracing::debug!(site, endpoint, "refusing a peer's signals endpoint");
    }
    first
}

/// Whether a peer IP may be dialed: anything but local and metadata addresses.
pub(crate) fn is_dialable_ip(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(v4) => {
            !(v4.is_loopback()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4 == endpoint_resolution::ALIBABA_CLOUD_METADATA_V4)
        },
        IpAddr::V6(v6) => {
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_unicast_link_local()
                || v6 == endpoint_resolution::AWS_METADATA_V6
                || v6 == endpoint_resolution::GCP_METADATA_V6
                || v6 == endpoint_resolution::EKS_POD_IDENTITY_V6
                || v6 == endpoint_resolution::AKAMAI_METADATA_V6)
        },
    }
}

/// Everything outside the RFC 3986 unreserved set is escaped in a query value.
const QUERY_VALUE: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

use grid_client::RequestOutcome;

/// Classify a scrape failure.
fn classify(error: &MetricsScrapeError) -> RequestOutcome {
    match error {
        MetricsScrapeError::Timeout(_) => RequestOutcome::Timeout,
        MetricsScrapeError::NonOkStatus { status, .. } => {
            match http::StatusCode::from_u16(*status).map(grid_client::from_status) {
                // A 2xx carried in an error means the scrape failed while the
                // status said it worked, so nothing was answered.
                Ok(RequestOutcome::Ok) | Err(_) => RequestOutcome::Transport,
                Ok(outcome) => outcome,
            }
        },
        MetricsScrapeError::Encoding(_) => RequestOutcome::Encoding,
        MetricsScrapeError::BodyTooLarge(_) => RequestOutcome::Transport,
        MetricsScrapeError::InvalidUrl(_)
        | MetricsScrapeError::HttpWithTls(_)
        | MetricsScrapeError::TlsMaterial(_)
        | MetricsScrapeError::Credential(_)
        | MetricsScrapeError::PlaintextCredential(_) => RequestOutcome::Config,
        MetricsScrapeError::Transport(inner) => classify_transport(&**inner),
    }
}

/// Separate a trust failure from an unreachable peer.
///
/// Both arrive as transport errors, and they call for opposite responses: one
/// is worth retrying and the other will fail identically until a certificate is
/// replaced. The chain is walked rather than the message matched, because an
/// error string is not an interface.
#[expect(clippy::wildcard_enum_match_arm, reason = "std::io::ErrorKind is non_exhaustive")]
fn classify_transport(error: &(dyn std::error::Error + 'static)) -> RequestOutcome {
    let mut current = Some(error);
    while let Some(err) = current {
        if tls_backend::is_tls_error(err) {
            return RequestOutcome::Tls;
        }
        if let Some(io) = err.downcast_ref::<std::io::Error>() {
            return match io.kind() {
                std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::HostUnreachable
                | std::io::ErrorKind::NetworkUnreachable
                | std::io::ErrorKind::ConnectionReset => RequestOutcome::Refused,
                std::io::ErrorKind::TimedOut => RequestOutcome::Timeout,
                _ => RequestOutcome::Transport,
            };
        }
        current = err.source();
    }
    RequestOutcome::Transport
}

/// Delay before attempt `attempt`, counting the first retry as zero.
///
/// Exponential and capped, spread by a value derived from the peer's name so
/// peers that failed on one partition do not retry in step and herd when it
/// heals. Deriving the spread from the name decorrelates peers repeatably.
fn backoff(base: Duration, attempt: u32, peer: &str) -> Duration {
    let factor = 1_u32 << attempt.min(5);
    let scaled = base.saturating_mul(factor);
    let spread = peer
        .bytes()
        .fold(0_u32, |acc, b| acc.wrapping_mul(31).wrapping_add(u32::from(b)));
    // Up to a quarter of the interval, added rather than subtracted so a delay
    // is never shorter than the backoff asked for.
    let jitter = (scaled / 4).saturating_mul(spread % 100) / 100;
    scaled.saturating_add(jitter)
}

/// One peer's client config under `mode`: its declared pins, or its SPIFFE ID.
fn peer_client_config(
    material: &PeerTlsMaterial,
    mode: PeerTrustMode,
    peer: &str,
    pins: &[String],
) -> Result<ClientTlsConfig, MetricsScrapeError> {
    let cert = material.identity.as_ref().map(|id| id.cert.as_slice());
    let key = material.identity.as_ref().map(|id| id.key.as_slice());
    match mode {
        PeerTrustMode::Pin => crate::metrics_scraper::build_pinned_client_config(&material.ca, cert, key, peer, pins),
        PeerTrustMode::Spiffe => crate::metrics_scraper::build_spiffe_client_config(&material.ca, cert, key, peer),
    }
}

/// PEM material a peer client is built from.
///
/// Kept as bytes rather than a finished config so a config can be made per
/// peer, each verifying against that peer's own declared fingerprints.
#[derive(Clone)]
pub struct PeerTlsMaterial {
    /// Authority the peer's chain is verified against, PEM.
    pub ca: Vec<u8>,
    /// What this site presents to a peer, which is how the peer names us.
    pub identity: Option<PeerClientIdentity>,
}

/// A certificate and the key that goes with it.
///
/// One type because the pair is the only legal shape: a certificate cannot be
/// presented without its key.
#[derive(Clone)]
pub struct PeerClientIdentity {
    /// Certificate chain, PEM.
    pub cert: Vec<u8>,
    /// Private key for `cert`, PEM.
    pub key: zeroize::Zeroizing<Vec<u8>>,
}

/// Prints nothing of the key, which this type exists to hold.
impl std::fmt::Debug for PeerTlsMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerTlsMaterial")
            .field("ca_len", &self.ca.len())
            .field("identity", &self.identity.is_some())
            .finish()
    }
}

/// How long an unused peer connection is kept.
///
/// Above the default poll interval, but below the signals listener's 10s idle
/// timeout and the idle window a stateful firewall is likely to enforce.
const PEER_POOL_IDLE: Duration = Duration::from_secs(8);

/// How long a peer transport may live before it is rebuilt.
///
/// A pooled connection is verified once, when it is made. Rebuilding forces a
/// fresh handshake, and so a fresh leaf check, bounding how long a revoked or
/// expired certificate stays reachable.
const PEER_TRANSPORT_MAX_AGE: Duration = Duration::from_mins(1);

/// Most peers dialled at once, whatever the configured concurrency.
const MAX_PEER_CONCURRENCY: usize = 32;

/// `peer`'s transport age bound: [`PEER_TRANSPORT_MAX_AGE`] spread by up to a fifth either
/// way, fixed per peer, so a fleet's rebuilds and their handshakes do not all land at once.
fn transport_max_age(local: &str, peer: &str) -> Duration {
    // Seeded with both ends, so the sites polling one peer do not rebuild toward it together.
    let spread = local
        .bytes()
        .chain(std::iter::once(0))
        .chain(peer.bytes())
        .fold(0_u32, |acc, b| acc.wrapping_mul(31).wrapping_add(u32::from(b)));
    PEER_TRANSPORT_MAX_AGE.saturating_mul(80 + spread % 41) / 100
}

/// Per-peer transports, with the pins each was built for.
///
/// Held across rounds because building one parses a private key and opens a
/// connection. Keyed by peer with the pins in the value, because keying on the
/// pins would leak an entry per rotation. Each entry owns a connection pool,
/// so an entry is dropped once its peer leaves the grid or its transport
/// reaches its age.
#[derive(Default)]
pub struct PeerTransports(Mutex<HashMap<String, HeldTransport>>);

/// A peer's transport, the pins it was built for, and when it was built.
struct HeldTransport {
    /// The pins the transport verifies against.
    pins: Vec<String>,
    /// The `(server name, host)` it dials under, when not the URL host.
    alias: Option<(String, String)>,
    /// The client, whose pool outlives a round.
    transport: Arc<PeerTransport>,
    /// When it was built, which bounds how long a connection may live.
    built: Instant,
}

impl std::fmt::Debug for PeerTransports {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerTransports").finish_non_exhaustive()
    }
}

/// Reads each peer's signals endpoint over HTTP.
pub struct PollPeers {
    /// Per-attempt request timeout.
    pub timeout: Duration,
    /// Client TLS material, once peers require it.
    ///
    /// The material rather than a built config, because each peer is verified
    /// against its own declared fingerprints and so needs its own verifier.
    pub tls: Option<Arc<PeerTlsMaterial>>,
    /// Signals to ask each peer for; empty asks for all of them.
    pub collect: Vec<String>,
    /// How many peers to poll at once.
    ///
    /// A round fans out to every peer, and at fifty sites that is fifty sockets
    /// opened at once against fifty different networks. Bounding it keeps a
    /// round's cost proportional to the pool rather than to the grid.
    pub concurrency: usize,
    /// Attempts per peer, including the first.
    pub attempts: u32,
    /// Base delay between attempts.
    pub backoff: Duration,
    /// Total time a single peer may take, retries included.
    ///
    /// Without this a peer that fails slowly could hold a round open past the
    /// next one, and rounds would overlap until the pool was full of work
    /// nobody is waiting for any more.
    pub budget: Duration,
    /// The most one round may take. A poll still running at its end times out and is tried again
    /// next round, so peers that cannot answer never delay the rest.
    pub round: Duration,
    /// Duration past which a poll is counted as slow.
    pub slow_after: Duration,
    /// Per-peer transports, held across rounds.
    pub transports: PeerTransports,
    /// Signal that the process is stopping.
    ///
    /// Lets an in-flight round stand down cleanly; dropping the future instead
    /// would stop it anywhere and leave a poll's accounting half done.
    pub shutdown: crate::shutdown::Shutdown,
    /// How each peer's certificate is authorized.
    pub trust: PeerTrustMode,
    /// Why each peer's signals endpoint last failed to answer.
    pub failures: PeerFailures,
    /// This site's name, which spreads its transport rebuilds apart from other sites'.
    pub local_site: String,
}

impl Default for PollPeers {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(2),
            tls: None,
            collect: Vec::new(),
            concurrency: 8,
            attempts: 3,
            backoff: Duration::from_millis(50),
            budget: Duration::from_secs(5),
            round: Duration::from_secs(5),
            slow_after: Duration::from_secs(1),
            shutdown: crate::shutdown::Shutdown::never(),
            trust: PeerTrustMode::Pin,
            transports: PeerTransports::default(),
            failures: PeerFailures::default(),
            local_site: String::new(),
        }
    }
}

/// Why a peer's signals endpoint did not answer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Unreachable {
    /// It answered 404: something listens there, but not this endpoint's signals.
    NotFound,
    /// It did not answer in time.
    Timeout,
    /// The TLS handshake or the peer's identity was refused.
    Tls,
    /// The connection was refused or never established.
    Refused,
    /// This site's round ran out before the peer could be polled in full.
    RoundDeadline,
    /// Any other failure, by its outcome.
    Failed(RequestOutcome),
}

impl Unreachable {
    /// The failure for a poll that ended in `outcome`, with the last HTTP `status` it read.
    #[must_use]
    pub const fn of(outcome: RequestOutcome, status: Option<u16>) -> Self {
        match (outcome, status) {
            (_, Some(404)) => Self::NotFound,
            (RequestOutcome::Deadline, _) => Self::RoundDeadline,
            (RequestOutcome::Timeout, _) => Self::Timeout,
            (RequestOutcome::Tls, _) => Self::Tls,
            (RequestOutcome::Refused, _) => Self::Refused,
            (other, _) => Self::Failed(other),
        }
    }

    /// The condition reason naming it.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::NotFound => "NotFound",
            Self::Timeout => "Timeout",
            Self::Tls => "TLSRefused",
            Self::Refused => "ConnectionRefused",
            Self::RoundDeadline => "RoundDeadline",
            Self::Failed(_) => "RequestFailed",
        }
    }

    /// What an admin is told about it.
    #[must_use]
    pub fn describe(self) -> String {
        match self {
            Self::NotFound => "answered 404: check its signals endpoint and port".to_owned(),
            Self::Timeout => "timed out".to_owned(),
            Self::Tls => "TLS handshake or identity refused".to_owned(),
            Self::Refused => "connection refused".to_owned(),
            Self::RoundDeadline => {
                "not polled in full: this site's poll round ran out first, so lengthen the round or raise \
                 the dial concurrency"
                    .to_owned()
            },
            Self::Failed(outcome) => format!("failed: {}", outcome.as_str()),
        }
    }
}

/// Why each polled peer's signals endpoint last failed, until it answers again.
#[derive(Clone, Debug, Default)]
pub struct PeerFailures(Arc<RwLock<HashMap<String, Unreachable>>>);

impl PeerFailures {
    /// Record a poll of `peer`: forgotten when it `answered`, else failed for `why`.
    fn observe(&self, peer: &str, answered: bool, why: Unreachable) {
        if let Ok(mut failures) = self.0.write() {
            if answered {
                failures.remove(peer);
            } else {
                failures.insert(peer.to_owned(), why);
            }
        }
    }

    /// Why `peer` last failed, `None` once it answered.
    #[must_use]
    pub fn of(&self, peer: &str) -> Option<Unreachable> {
        self.0.read().ok()?.get(peer).copied()
    }
}

impl PollPeers {
    /// Poll one peer, retrying what is worth retrying, and record what happened.
    ///
    /// Returns the body, or nothing if every attempt failed. The outcome is
    /// recorded either way, because a peer that is never reachable has to be
    /// distinguishable from one that has nothing to say.
    #[expect(
        clippy::too_many_lines,
        reason = "one poll: guard, transport, attempts and what it records"
    )]
    async fn poll_one(
        &self,
        target: &Target<'_>,
        (budget, cut): (Duration, bool),
    ) -> Option<(String, crate::metrics_scraper::Stamp)> {
        let Target { peer, url, pins, .. } = target;
        // The guard owns the accounting, including the drop-mid-await path that
        // used to leave the in-flight gauge counting a poll that had ended.
        let mut guard = PollGuard::enter(peer, self.slow_after);
        // A peer the round has no time left for is not dialled at all.
        if budget.is_zero() {
            guard.finish(RequestOutcome::Deadline, 0);
            self.failures.observe(peer, false, Unreachable::RoundDeadline);
            return None;
        }

        let transport = match self.peer_transport(peer, pins, target.alias) {
            Ok(transport) => transport,
            Err(error) => {
                tracing::warn!(peer, %error, "peer client config unusable; not polling");
                guard.finish(RequestOutcome::Config, 0);
                return None;
            },
        };
        let ran_out = if cut {
            RequestOutcome::Deadline
        } else {
            RequestOutcome::Timeout
        };
        let (outcome, result, status) = tokio::time::timeout(
            budget,
            self.attempt_until(peer, url, &transport, Attempt::signals(guard.started, budget)),
        )
        .await
        .unwrap_or((ran_out, None, None));
        guard.finish(outcome, result.as_ref().map_or(0, |(body, _)| body.len()));
        self.failures
            .observe(peer, result.is_some(), Unreachable::of(outcome, status));
        result
    }

    /// Peers dialled at once for `peers` targets: the configured concurrency, at most 32, and
    /// never more than there are peers.
    #[must_use]
    pub fn dial_concurrency(&self, peers: usize) -> usize {
        self.concurrency.min(MAX_PEER_CONCURRENCY).min(peers).max(1)
    }

    /// `peer`'s held transport while it still fits `pins` and `alias` and is under its age.
    fn held_transport(&self, peer: &str, pins: &[String], alias: Option<(&str, &str)>) -> Option<Arc<PeerTransport>> {
        let max_age = transport_max_age(&self.local_site, peer);
        let fresh = |held: &&HeldTransport| {
            held.pins == pins
                && held.alias.as_ref().map(|(name, host)| (name.as_str(), host.as_str())) == alias
                && held.built.elapsed() < max_age
        };
        self.transports
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(peer)
            .filter(fresh)
            .map(|held| Arc::clone(&held.transport))
    }

    /// The transport for `peer`, built on first use and rebuilt when its pins
    /// or alias change or it reaches [`PEER_TRANSPORT_MAX_AGE`].
    ///
    /// The build runs outside the lock; concurrent callers may keep different
    /// usable transports for their rounds, and the last insert stays cached.
    fn peer_transport(
        &self,
        peer: &str,
        pins: &[String],
        alias: Option<(&str, &str)>,
    ) -> Result<Arc<PeerTransport>, MetricsScrapeError> {
        let held = self.held_transport(peer, pins, alias);
        if let Some(transport) = held {
            return Ok(transport);
        }
        let config = match self.tls.as_ref() {
            Some(material) => Some(peer_client_config(material, self.trust, peer, pins)?),
            None => None,
        };
        let transport = Arc::new(PeerTransport::new(config.as_ref(), Some(PEER_POOL_IDLE), alias)?);
        self.transports.0.lock().unwrap_or_else(PoisonError::into_inner).insert(
            peer.to_owned(),
            HeldTransport {
                pins: pins.to_vec(),
                alias: alias.map(|(name, host)| (name.to_owned(), host.to_owned())),
                transport: Arc::clone(&transport),
                built: Instant::now(),
            },
        );
        Ok(transport)
    }

    /// Drop the transports of peers no longer in the grid.
    ///
    /// Without this a departed peer keeps its material for the life of the
    /// process.
    fn retain_transports(&self, current: &[Target<'_>]) {
        self.transports
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|peer, _| current.iter().any(|target| target.peer == peer));
    }

    /// How many peer transports are held.
    #[cfg(all(test, not(feature = "fips")))]
    fn held_transports(&self) -> usize {
        self.transports.0.lock().unwrap_or_else(PoisonError::into_inner).len()
    }

    /// One request, abandoned if the process is stopping.
    ///
    /// The request races the signal. A peer that is slow to answer must not
    /// hold termination open for the length of a timeout it was never going to
    /// beat. `None` means the signal won.
    async fn scrape_or_stand_down(
        &self,
        url: &str,
        transport: &PeerTransport,
        (timeout, max_body): (Duration, usize),
    ) -> Option<Result<(String, crate::metrics_scraper::Stamp), MetricsScrapeError>> {
        tokio::select! {
            biased;
            () = self.shutdown.triggered() => None,
            result = scrape_peer(url, timeout, transport, max_body) => Some(result),
        }
    }

    /// Back off before the next attempt, unless the process is stopping.
    ///
    /// Waiting out a backoff is the easiest place to be stuck during shutdown,
    /// and the least excusable. Returns whether the wait completed.
    async fn wait_before_retry(&self, peer: &str, attempt: u32, started: Instant, budget: Duration) -> bool {
        let wait = backoff(self.backoff, attempt, peer);
        let remaining = budget.saturating_sub(started.elapsed());
        tokio::select! {
            biased;
            () = self.shutdown.triggered() => false,
            () = tokio::time::sleep(wait.min(remaining)) => true,
        }
    }

    /// Attempt until one succeeds, the budget from `started` runs out, or retrying is pointless.
    #[expect(
        clippy::too_many_lines,
        reason = "one retry loop with its budget, backoff and stand-down"
    )]
    async fn attempt_until(
        &self,
        peer: &str,
        url: &str,
        transport: &PeerTransport,
        bounds: Attempt,
    ) -> (
        RequestOutcome,
        Option<(String, crate::metrics_scraper::Stamp)>,
        Option<u16>,
    ) {
        let Attempt {
            started,
            budget,
            counted,
            max_body,
        } = bounds;
        let attempts = self.attempts.max(1);
        let mut outcome = RequestOutcome::Transport;
        let mut status = None;
        for attempt in 0..attempts {
            if self.shutdown.is_triggered() {
                return (RequestOutcome::Cancelled, None, status);
            }
            if started.elapsed() >= budget {
                break;
            }

            // Each attempt may use what the budget has left, up to the configured timeout, so a slow
            // peer on a short budget is not timed out sooner than it would answer.
            let timeout = self.timeout.min(budget.saturating_sub(started.elapsed()));
            let Some(scrape) = self.scrape_or_stand_down(url, transport, (timeout, max_body)).await else {
                return (RequestOutcome::Cancelled, None, status);
            };

            match scrape {
                Ok(pair) => return (RequestOutcome::Ok, Some(pair), None),
                Err(error) => {
                    outcome = classify(&error);
                    status = if let MetricsScrapeError::NonOkStatus { status: code, .. } = &error {
                        Some(*code)
                    } else {
                        None
                    };
                    if attempt + 1 >= attempts || !outcome.is_retryable() {
                        tracing::warn!(site = %peer, %url, outcome = outcome.as_str(), %error, "peer poll failed");
                        break;
                    }
                    if counted == Counted::Yes {
                        crate::metrics::record_peer_retry(peer, outcome.as_str());
                    }
                    if !self.wait_before_retry(peer, attempt, started, budget).await {
                        return (RequestOutcome::Cancelled, None, status);
                    }
                },
            }
        }
        (outcome, None, status)
    }

    /// Observations for each site, keyed by site name.
    ///
    /// A site that did not answer is absent rather than empty, so a caller can
    /// tell silence from a site that genuinely has nothing to report.
    pub async fn collect(&self, sites: &[PeerSite]) -> BTreeMap<String, Vec<Observation>> {
        let mut collected = BTreeMap::new();
        self.collect_each(sites, |peer, observations, _, _| {
            collected.insert(peer, observations);
        })
        .await;
        collected
    }

    /// Hand each site's observations to `publish` as its poll completes, with how
    /// many of its rows were dropped for an implausible age.
    #[expect(
        clippy::too_many_lines,
        reason = "one round: targets, budgets, then each answer as it lands"
    )]
    pub async fn collect_each<Publish>(&self, sites: &[PeerSite], mut publish: Publish)
    where
        Publish: FnMut(String, Vec<Observation>, usize, crate::metrics_scraper::Stamp),
    {
        let collect_query = self
            .collect
            .iter()
            .map(|c| format!("collect[]={}", utf8_percent_encode(c, QUERY_VALUE)))
            .collect::<Vec<_>>()
            .join("&");

        let targets = peer_urls(sites, &collect_query);
        self.retain_transports(&targets);
        let deadline = Instant::now() + self.round;
        let fetches = targets.into_iter().map(|target| async move {
            let budget = if target.answering {
                self.budget
            } else {
                self.budget.min(UNANSWERED_PEER_BUDGET)
            };
            // Too little left for one attempt to finish is not dialled: it waits for the next round.
            let left = deadline.saturating_duration_since(Instant::now());
            let left = if left < self.timeout.min(UNANSWERED_PEER_BUDGET).min(self.round) / 2 {
                Duration::ZERO
            } else {
                left
            };
            let (body, stamp) = self.poll_one(&target, (budget.min(left), left < budget)).await?;
            let peer = target.peer.to_owned();
            // Ages first, so a row dropped for its age never takes a provider slot.
            let (aged, rejected) = reexpress_peer_ages(stamp_origin(parse(&body), &peer), stamp.date, &peer);
            let observations = bound_peer(aged, &peer);
            Some((peer, observations, rejected, stamp))
        });

        // A failed peer is absent, not empty, so silence stays distinct from nothing to report.
        let mut done = futures::stream::iter(fetches).buffer_unordered(self.dial_concurrency(sites.len()));
        while let Some(result) = done.next().await {
            if let Some((peer, observations, rejected, stamp)) = result {
                publish(peer, observations, rejected, stamp);
            }
        }
    }

    /// The providers `site` serves this one, over the transport and trust its signals are polled on.
    ///
    /// `None` for a peer that did not answer or answered with a body a reader refuses, so a
    /// caller keeps what it last held rather than reading silence as no providers.
    pub async fn fetch_providers(
        &self,
        site: &PeerSite,
    ) -> Option<(Vec<crate::site_providers::WireProvider>, Option<String>)> {
        let url = with_path(&site.url, crate::site_providers::PROVIDERS_PATH)?;
        let transport = self
            .peer_transport(&site.name, &site.pins, site.alias())
            .inspect_err(
                |error| tracing::warn!(peer = %site.name, %error, "peer client config unusable; not reading providers"),
            )
            .ok()?;
        let budget = if site.answering {
            self.budget
        } else {
            self.budget.min(UNANSWERED_PEER_BUDGET)
        };
        let started = Instant::now();
        let (_, answer, _) = tokio::time::timeout(
            budget,
            self.attempt_until(&site.name, &url, &transport, Attempt::catalog(started, budget)),
        )
        .await
        .ok()?;
        let (body, stamp) = answer?;
        read_catalog(&site.name, &body, stamp.providers_contract.as_deref())
            .map(|providers| (providers, stamp.providers_version))
    }
}

/// `peer`'s providers from `body`, `None` for an unsupported contract or a body the reader refuses.
fn read_catalog(peer: &str, body: &str, contract: Option<&str>) -> Option<Vec<crate::site_providers::WireProvider>> {
    if !crate::site_providers::reads_contract(contract) {
        tracing::warn!(
            peer,
            ?contract,
            "peer providers contract unsupported; keeping what is held"
        );
        return None;
    }
    let providers = crate::site_providers::parse(body);
    if providers.is_none() {
        tracing::warn!(peer, bytes = body.len(), "peer providers body refused");
    }
    providers
}

/// `url` with its path replaced by `path` and no query, rebuilt through the URI parser.
fn with_path(url: &str, path: &'static str) -> Option<String> {
    let mut parts = url.parse::<http::Uri>().ok()?.into_parts();
    parts.path_and_query = Some(http::uri::PathAndQuery::from_static(path));
    http::Uri::from_parts(parts).ok().map(|uri| uri.to_string())
}

/// Owns the accounting for one poll, however that poll ends.
///
/// A cancelled poll is recorded as cancelled, not dropped: a poll stopped by
/// shutdown is not a failure, and conflating them shows a restart as an outage.
struct PollGuard<'poll> {
    /// Peer being polled.
    peer: &'poll str,
    /// When the poll began.
    started: Instant,
    /// Threshold for counting the poll slow.
    slow_after: Duration,
    /// Set once the outcome has been recorded.
    recorded: bool,
}

impl<'poll> PollGuard<'poll> {
    /// Begin a poll, counting it in flight.
    fn enter(peer: &'poll str, slow_after: Duration) -> Self {
        crate::metrics::peer_polls_in_flight(1);
        Self {
            peer,
            started: Instant::now(),
            slow_after,
            recorded: false,
        }
    }

    /// Record a poll that ran to a conclusion.
    fn finish(&mut self, outcome: RequestOutcome, bytes: usize) {
        self.record(outcome, bytes);
    }

    /// Record an outcome once and only once.
    fn record(&mut self, outcome: RequestOutcome, bytes: usize) {
        if self.recorded {
            return;
        }
        self.recorded = true;
        crate::metrics::record_peer_poll(
            self.peer,
            outcome.as_str(),
            self.started.elapsed(),
            bytes,
            self.slow_after,
        );
        // Reachability is claimed only when the poll concluded: a cancelled
        // poll learned nothing, and marking it down would fake an outage.
        if outcome != RequestOutcome::Cancelled {
            crate::metrics::set_peer_collection_up(self.peer, outcome == RequestOutcome::Ok, SystemTime::now());
        }
    }
}

impl Drop for PollGuard<'_> {
    fn drop(&mut self) {
        crate::metrics::peer_polls_in_flight(-1);
        self.record(RequestOutcome::Cancelled, 0);
    }
}

/// One peer's request, as a round makes it.
struct Target<'sites> {
    /// Site name.
    peer: &'sites str,
    /// What is asked for.
    url: String,
    /// Fingerprints the peer is verified against.
    pins: &'sites [String],
    /// Whether it answered last round.
    answering: bool,
    /// `(server name, host)` when the peer is dialled under another name.
    alias: Option<(&'sites str, &'sites str)>,
}

/// Build the request URL for each peer.
///
/// No `target`: it names a provider, not a site, so passing a site name matches
/// nothing and the peer answers empty. Scoping a peer's answer is the
/// publisher's job; the reader re-checks the site label on receipt.
fn peer_urls<'sites>(sites: &'sites [PeerSite], collect_query: &str) -> Vec<Target<'sites>> {
    sites
        .iter()
        .filter_map(|site| {
            let url = if collect_query.is_empty() {
                site.url.clone()
            } else {
                with_query(&site.url, collect_query)?
            };
            Some(Target {
                peer: &site.name,
                url,
                pins: &site.pins,
                answering: site.answering,
                alias: site.alias(),
            })
        })
        .collect()
}

/// `url` with its query replaced by `query`, rebuilt through the URI parser.
fn with_query(url: &str, query: &str) -> Option<String> {
    let mut parts = url.parse::<http::Uri>().ok()?.into_parts();
    let path = parts.path_and_query.as_ref().map_or("/", http::uri::PathAndQuery::path);
    parts.path_and_query = Some(format!("{path}?{query}").parse().ok()?);
    http::Uri::from_parts(parts).ok().map(|uri| uri.to_string())
}

/// Names a hub keeps from a peer: the cross-site contract, plus the EPP pool averages and
/// ready counts the gateway routes on until it routes on saturation. A peer's custom
/// `signalNames` are dropped here.
const PEER_SIGNAL_NAMES: [&str; 20] = [
    grid_signals::revision::METRIC,
    crate::readiness::READY_SIGNAL,
    crate::readiness::READY_ENDPOINTS_SIGNAL,
    crate::readiness::IN_FLIGHT_SIGNAL,
    crate::latency::TTFT_P50_SIGNAL,
    crate::latency::TTFT_P90_SIGNAL,
    crate::latency::TPOT_SIGNAL,
    crate::latency::ERROR_RATIO_SIGNAL,
    "inference_pool_average_queue_size",
    "llm_d_epp_average_queue_size",
    "inference_pool_average_running_requests",
    "llm_d_epp_average_running_requests",
    "inference_pool_average_kv_cache_utilization",
    "llm_d_epp_average_kv_cache_utilization",
    // The deviation across a pool's units, which a gateway reads beside the mean. Without it
    // a remote site is routed from the mean alone and one hot unit stays hidden behind it.
    "llm_d_epp_std_dev_kv_cache_utilization",
    "llm_d_epp_ready_endpoints",
    "inference_pool_ready_pods",
    // What a gateway reads as a backlog beside the averages, and the per-unit series whose
    // disappearance tells a drained pool from one whose gauges froze.
    "llm_d_epp_flow_control_queue_size",
    "inference_pool_per_pod_queue_size",
    "llm_d_epp_per_endpoint_queue_size",
];

/// Most providers a hub keeps from one peer, so a peer cannot grow the hub's series without
/// bound. The first in name order are kept.
pub const MAX_PEER_PROVIDERS: usize = 64;

/// Signals that are shares, which a peer may not report above one.
const UNIT_SIGNALS: [&str; 5] = [
    crate::readiness::READY_SIGNAL,
    crate::latency::ERROR_RATIO_SIGNAL,
    // KV utilization is a fraction, and a provider publishing a percentage would otherwise
    // read as a hundred times saturated.
    "llm_d_epp_average_kv_cache_utilization",
    "inference_pool_average_kv_cache_utilization",
    "llm_d_epp_std_dev_kv_cache_utilization",
];

/// Whether a hub accepts the value: finite, non-negative, and at most one for a share.
fn in_range(observation: &Observation) -> bool {
    observation.value.is_finite()
        && observation.value >= 0.0
        && (observation.value <= 1.0 || !UNIT_SIGNALS.contains(&observation.metric.as_str()))
}

/// Most bytes in a `grid_provider` label a hub keeps, as a routing cluster name is bounded.
const MAX_PROVIDER_LABEL_LEN: usize = 253;

/// Whether `provider` names a routing cluster a hub can key on: non-blank, bounded, and free
/// of the store's `/` separator and control characters. Cluster names such as `pool.v1` are
/// valid, so this is wider than a site name.
fn valid_provider(provider: &str) -> bool {
    !provider.trim().is_empty()
        && provider != grid_signals::poller::PROVIDER
        && provider.len() <= MAX_PROVIDER_LABEL_LEN
        && !provider.chars().any(|ch| ch.is_control() || ch == '/')
}

/// Why a hub refuses `observation` from a peer, `None` when it accepts it.
fn refusal(observation: &Observation) -> Option<&'static str> {
    if !PEER_SIGNAL_NAMES.contains(&observation.metric.as_str()) {
        Some("name")
    } else if observation
        .labels
        .get(PROVIDER_LABEL)
        .is_none_or(|provider| !valid_provider(provider))
    {
        Some("provider")
    } else if !in_range(observation) {
        Some("value")
    } else {
        None
    }
}

/// Keep what a hub accepts from `peer`: an allowed name, a `grid_provider` that names a
/// routing cluster, a value in range, and at most [`MAX_PEER_PROVIDERS`] providers. Each
/// refusal is counted by reason.
fn bound_peer(observations: Vec<Observation>, peer: &str) -> Vec<Observation> {
    let mut kept = project_peer(observations, peer);
    let payload: std::collections::BTreeSet<&str> = kept
        .iter()
        .filter(|observation| observation.metric != grid_signals::revision::METRIC)
        .filter_map(|observation| observation.labels.get(PROVIDER_LABEL).map(String::as_str))
        .collect();
    let providers: std::collections::BTreeSet<&str> = kept
        .iter()
        .filter_map(|observation| observation.labels.get(PROVIDER_LABEL).map(String::as_str))
        .collect();
    let accepted: std::collections::BTreeSet<_> = payload
        .iter()
        .copied()
        .chain(providers.difference(&payload).copied())
        .take(MAX_PEER_PROVIDERS)
        .map(str::to_owned)
        .collect();
    let before = kept.len();
    kept.retain(|observation| {
        observation
            .labels
            .get(PROVIDER_LABEL)
            .is_some_and(|provider| accepted.contains(provider))
    });
    for _ in kept.len()..before {
        crate::metrics::record_peer_signal_refused(peer, "provider_cap");
    }
    retain_complete_batches(kept)
}

/// Apply the deliberate metric projection without hiding rejected payload rows.
fn project_peer(observations: Vec<Observation>, peer: &str) -> Vec<Observation> {
    let mut kept = Vec::with_capacity(observations.len());
    let mut projected = BTreeMap::<String, usize>::new();
    for observation in observations {
        match refusal(&observation) {
            Some("name") => {
                if let Some(provider) = observation.labels.get(PROVIDER_LABEL) {
                    let removed = projected.entry(provider.clone()).or_default();
                    *removed = removed.saturating_add(1);
                }
                crate::metrics::record_peer_signal_refused(peer, "name");
            },
            Some(reason) => crate::metrics::record_peer_signal_refused(peer, reason),
            None => kept.push(observation),
        }
    }
    recount_projection(&mut kept, &projected);
    kept
}

/// Adjust metadata only for deliberate name filtering, never for corrupt payload.
fn recount_projection(kept: &mut [Observation], projected: &BTreeMap<String, usize>) {
    for observation in kept {
        if observation.metric == grid_signals::revision::METRIC
            && let Some(removed) = observation
                .labels
                .get(PROVIDER_LABEL)
                .and_then(|provider| projected.get(provider))
            && let Some(count) = observation
                .labels
                .get(grid_signals::revision::SAMPLES_LABEL)
                .and_then(|count| count.parse::<usize>().ok())
                .and_then(|count| count.checked_sub(*removed))
        {
            observation
                .labels
                .insert(grid_signals::revision::SAMPLES_LABEL.to_owned(), count.to_string());
        }
    }
}

/// Attribute every observation to the peer this site dialed, refusing one that
/// names a different site.
///
/// Ownership comes from the verified identity on the connection, never from the
/// body, so a peer cannot name another site and a peer need not label its own
/// output at all. A row that does name a different site is a peer serving data
/// on someone else's behalf, which the grid does not allow: it is refused and
/// counted rather than silently re-attributed, so a misconfigured peer shows up
/// as a refusal instead of as inflated numbers of its own.
fn stamp_origin(observations: Vec<Observation>, peer: &str) -> Vec<Observation> {
    let mut kept = Vec::with_capacity(observations.len());
    for mut observation in observations {
        match observation.labels.get(SITE_LABEL) {
            Some(site) if site != peer => {
                crate::metrics::record_peer_signal_refused(peer, "relayed_site");
            },
            _ => {
                observation.labels.insert(SITE_LABEL.to_owned(), peer.to_owned());
                kept.push(observation);
            },
        }
    }
    kept
}

/// The most a bounded render returns, three quarters of the gateway's 1 MiB read ceiling.
pub const MAX_RELAY_BYTES: usize = 768 * 1024;

/// Lines one target may contribute to a bounded render.
pub const MAX_RENDER_LINES_PER_TARGET: usize = 4096;

/// A bounded render and what it left out.
#[derive(Debug, Default)]
pub struct Rendered {
    /// Exposition text, whole lines only.
    pub body: String,
    /// Age of the oldest sample emitted.
    pub oldest: Duration,
    /// Targets skipped because the body would have passed the budget.
    pub targets_dropped: usize,
    /// Lines cut past the per-target cap.
    pub lines_cut: usize,
}

/// Whether `metric` is one of the series a gateway routes on.
#[must_use]
pub fn is_peer_signal(metric: &str) -> bool {
    PEER_SIGNAL_NAMES.contains(&metric)
}

/// One target's contract series, held for `held_for` and stamped from `wall`, as
/// exposition text, its oldest sample age, and the lines cut past
/// [`MAX_RENDER_LINES_PER_TARGET`].
fn render_contract_block(
    samples: &[Observation],
    collect: &[String],
    held_for: Duration,
    wall: SystemTime,
) -> (String, Duration, usize) {
    render_batches(samples, collect, held_for, wall, true)
}

/// Render whole provider batches, retaining metadata through metric selection.
#[expect(
    clippy::too_many_lines,
    reason = "one atomic provider render keeps filtering, row counts, and the line budget together"
)]
fn render_batches(
    samples: &[Observation],
    collect: &[String],
    held_for: Duration,
    wall: SystemTime,
    bounded: bool,
) -> (String, Duration, usize) {
    let mut groups = BTreeMap::<_, Vec<&Observation>>::new();
    for sample in samples {
        let key = (sample.labels.get(SITE_LABEL), sample.labels.get(PROVIDER_LABEL));
        groups.entry(key).or_default().push(sample);
    }
    let mut block = String::new();
    let mut oldest = Duration::ZERO;
    let mut lines = 0_usize;
    let mut cut = 0_usize;
    for group in groups.values() {
        let metadata = group
            .iter()
            .find(|sample| sample.metric == grid_signals::revision::METRIC)
            .copied();
        if let Some(metadata) = metadata
            && !complete_batch(group, metadata)
        {
            cut = cut.saturating_add(group.len());
            continue;
        }
        let selected: Vec<_> = group
            .iter()
            .copied()
            .filter(|sample| {
                sample.metric != grid_signals::revision::METRIC
                    && (!bounded || is_peer_signal(&sample.metric))
                    && (collect.is_empty() || collect.iter().any(|name| name == &sample.metric))
            })
            .collect();
        let count = selected.len().saturating_add(usize::from(metadata.is_some()));
        if bounded && lines.saturating_add(count) > MAX_RENDER_LINES_PER_TARGET {
            cut = cut.saturating_add(count);
            continue;
        }
        if let Some(metadata) = metadata {
            let mut metadata = metadata.clone();
            metadata.labels.insert(
                grid_signals::revision::SAMPLES_LABEL.to_owned(),
                selected.len().to_string(),
            );
            let age = held_for.saturating_add(metadata.age);
            render_sample(&mut block, &metadata, wall_millis(wall, age));
            block.push('\n');
            oldest = oldest.max(age);
        }
        for sample in selected {
            let age = held_for.saturating_add(sample.age);
            render_sample(&mut block, sample, wall_millis(wall, age));
            block.push('\n');
            oldest = oldest.max(age);
        }
        lines = lines.saturating_add(count);
    }
    (block, oldest, cut)
}

/// Whether metadata binds every row in its provider group at the same age.
fn complete_batch(group: &[&Observation], metadata: &Observation) -> bool {
    let mut line = String::new();
    render_sample(&mut line, metadata, metadata.timestamp_ms.unwrap_or(1));
    let Some(revision) =
        grid_signals::exposition::parse(&line).and_then(|metric| grid_signals::revision::parse(&metric))
    else {
        return false;
    };
    group
        .iter()
        .filter(|sample| sample.metric == grid_signals::revision::METRIC)
        .count()
        == 1
        && group.len() == revision.batch_samples.saturating_add(1)
        && group
            .iter()
            .all(|sample| sample.timestamp_ms == metadata.timestamp_ms && sample.age == metadata.age)
}

/// How far a relayed sample may be stamped past its peer's `Date` and still count
/// as now. A peer writes its `Date` from the same clock reading as its stamps, at
/// one-second resolution, so a fresh sample lands up to 999 ms ahead.
const RELAY_AGE_TOLERANCE_MS: i64 = 1_000; // the Date header's resolution

/// The largest relayed-sample age accepted.
///
/// Older, or further ahead of the peer's `Date` than [`RELAY_AGE_TOLERANCE_MS`],
/// means the peer's clock stepped or the stamp is garbage, so the row is dropped
/// rather than refreshed into a value that reads as current.
///
/// The gateway's `grid-signals` holds its own copy of both bounds, and a test
/// fails when the two differ.
const MAX_RELAY_AGE_MS: i64 = 86_400_000; // one day

/// Carry each relayed peer sample's age into [`Observation::age`], dropping and
/// counting as `skew` each row whose age is implausible. Returns the rows kept and
/// how many were dropped.
///
/// Age is the peer's `Date` minus the sample's own timestamp, both on the peer
/// clock so no skew enters. The timestamp is then dropped: a render here stamps
/// the sample from its own clock reading and this age, so no wall time of this
/// site's is stored. A missing `Date` or stamp leaves the row aged from its
/// collection here, never from the peer's absolute clock.
#[expect(
    clippy::too_many_lines,
    reason = "one age conversion keeps invalidations and payload freshness together"
)]
fn reexpress_peer_ages(
    observations: Vec<Observation>,
    date: Option<SystemTime>,
    peer: &str,
) -> (Vec<Observation>, usize) {
    let date_ms = date
        .and_then(|when| when.duration_since(UNIX_EPOCH).ok())
        .and_then(|since| i64::try_from(since.as_millis()).ok());
    let undated: std::collections::BTreeSet<_> = observations
        .iter()
        .filter(|observation| date_ms.is_none() && observation.metric == grid_signals::revision::METRIC)
        .filter_map(|observation| observation.labels.get(PROVIDER_LABEL).cloned())
        .collect();
    let mut kept = Vec::with_capacity(observations.len());
    let mut rejected = 0_usize;
    for mut observation in observations {
        if observation
            .labels
            .get(PROVIDER_LABEL)
            .is_some_and(|provider| undated.contains(provider))
        {
            if observation.metric == grid_signals::revision::METRIC {
                let mut metadata = invalidation(&observation);
                metadata.timestamp_ms = None;
                metadata.age = Duration::from_millis(86_400_001);
                kept.push(metadata);
            }
            rejected = rejected.saturating_add(1);
            crate::metrics::record_peer_signal_refused(peer, "skew");
            continue;
        }
        let age = match (date_ms, observation.timestamp_ms.take()) {
            (Some(date_ms), Some(stamp_ms)) => relay_age(date_ms, stamp_ms),
            _ => Some(Duration::ZERO),
        };
        if let Some(age) = age {
            observation.age = age;
            kept.push(observation);
        } else {
            if observation.metric == grid_signals::revision::METRIC {
                let mut metadata = invalidation(&observation);
                metadata.age = Duration::from_millis(86_400_001);
                kept.push(metadata);
            }
            crate::metrics::record_peer_signal_refused(peer, "skew");
            rejected = rejected.saturating_add(1);
        }
    }
    (retain_complete_batches(kept), rejected)
}

/// The age `stamp_ms` carries against its peer's `date_ms`, `None` when it is
/// implausible. Ahead of the `Date` within [`RELAY_AGE_TOLERANCE_MS`] is now.
fn relay_age(date_ms: i64, stamp_ms: i64) -> Option<Duration> {
    let age = date_ms.saturating_sub(stamp_ms);
    (RELAY_AGE_TOLERANCE_MS.saturating_neg()..=MAX_RELAY_AGE_MS)
        .contains(&age)
        .then(|| Duration::from_millis(u64::try_from(age).unwrap_or(0)))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn a_restarted_site_catches_up_once_every_peer_is_held_or_long_silent() {
        let catch_up = CatchUp::default();
        assert!(!catch_up.caught_up(3), "not before the first round");
        let polled = ["east".to_owned(), "west".to_owned()];
        catch_up.record(&polled, |_| true, |site| site == "east");
        assert!(!catch_up.caught_up(3), "west answered, but its catalog is not held yet");
        catch_up.record(&polled, |site| site == "east", |site| site == "east");
        catch_up.record(&polled, |site| site == "east", |site| site == "east");
        assert!(!catch_up.caught_up(3), "west silent for two rounds");
        catch_up.record(&polled, |site| site == "east", |site| site == "east");
        assert!(catch_up.caught_up(3), "west silent for three rounds is not waited on");
        catch_up.record(&polled, |_| true, |site| site == "east");
        assert!(!catch_up.caught_up(3), "an answer resets the count");
        catch_up.record(&[], |_| true, |_| true);
        assert!(catch_up.caught_up(3), "a site with no peers has nothing to wait for");
    }

    #[test]
    fn an_unlabeled_series_belongs_to_no_named_pool() {
        let labelled = |labels: &[(&str, &str)]| Observation {
            metric: "llm_d_epp_flow_control_queue_size".to_owned(),
            labels: labels.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect(),
            value: 1.0,
            timestamp_ms: None,
            age: Duration::ZERO,
        };
        assert!(
            !in_pool(&labelled(&[]), Some("qwen3")),
            "no pool label on a multi-pool EPP is the total across pools, not this pool's"
        );
        assert!(
            !in_pool(&labelled(&[(POOL_LABEL, "other")]), Some("qwen3")),
            "another pool"
        );
        assert!(in_pool(&labelled(&[(POOL_LABEL, "qwen3")]), Some("qwen3")), "this pool");
        assert!(
            in_pool(&labelled(&[]), None),
            "with no pool named, an unlabeled series is all there is"
        );
    }

    /// A peer observation of `metric` for `provider` with `value`.
    fn peer_sample(metric: &str, provider: &str, value: f64) -> Observation {
        Observation {
            metric: metric.to_owned(),
            labels: BTreeMap::from([
                (SITE_LABEL.to_owned(), "retail".to_owned()),
                (PROVIDER_LABEL.to_owned(), provider.to_owned()),
            ]),
            value,
            timestamp_ms: None,
            age: Duration::ZERO,
        }
    }

    #[test]
    fn a_row_naming_another_site_is_refused_not_re_attributed() {
        // A peer serving data on another site's behalf is the chaining the grid
        // does not allow. Re-attributing it to the dialed peer would turn a
        // misconfigured peer into numbers of its own that are in range, so it is
        // refused and counted instead.
        let mut claims_elsewhere = peer_sample(crate::readiness::READY_SIGNAL, "pool", 1.0);
        claims_elsewhere
            .labels
            .insert(SITE_LABEL.to_owned(), "factory".to_owned());

        let kept = stamp_origin(vec![claims_elsewhere], "retail");

        assert!(
            kept.is_empty(),
            "a row naming another site must not reach this site's view"
        );
    }

    #[test]
    fn a_row_this_peer_labelled_correctly_is_kept() {
        let own = peer_sample(crate::readiness::READY_SIGNAL, "pool", 1.0);

        let kept = stamp_origin(vec![own], "retail");

        assert_eq!(
            kept.first().and_then(|o| o.labels.get(SITE_LABEL)).map(String::as_str),
            Some("retail"),
            "a correctly labelled row survives and keeps its site"
        );
    }

    #[test]
    fn a_sample_with_no_site_is_attributed_to_the_peer() {
        // Previously such a sample was dropped, because attribution read the
        // body. Attribution now comes from the connection, so the peer does not
        // have to label its own output for this site to use it.
        let mut unlabelled = peer_sample(crate::readiness::READY_SIGNAL, "pool", 1.0);
        unlabelled.labels.remove(SITE_LABEL);

        let stamped = stamp_origin(vec![unlabelled], "retail");

        assert_eq!(
            stamped
                .first()
                .and_then(|o| o.labels.get(SITE_LABEL))
                .map(String::as_str),
            Some("retail"),
            "an unlabelled sample should be attributed, not dropped"
        );
    }

    #[test]
    fn stamping_keeps_every_sample_and_leaves_other_labels_alone() {
        let samples = vec![
            peer_sample(crate::readiness::READY_SIGNAL, "pool-a", 1.0),
            peer_sample(crate::readiness::READY_SIGNAL, "pool-b", 2.0),
        ];

        let stamped = stamp_origin(samples, "retail");

        assert_eq!(stamped.len(), 2, "rows the peer owns are all kept");
        assert_eq!(
            stamped
                .iter()
                .filter_map(|o| o.labels.get(PROVIDER_LABEL).map(String::as_str))
                .collect::<Vec<_>>(),
            ["pool-a", "pool-b"],
            "only the site label is rewritten"
        );
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "one table of accepted and refused samples")]
    fn a_hub_refuses_peer_names_providers_and_values_outside_the_contract() {
        let cases = [
            (
                "contract name",
                peer_sample(crate::readiness::IN_FLIGHT_SIGNAL, "pool", 0.5),
                true,
            ),
            (
                "gateway load name",
                peer_sample("inference_pool_average_queue_size", "pool", 3.0),
                true,
            ),
            ("custom name", peer_sample("my_custom_queue", "pool", 3.0), false),
            (
                "provider with a dot, a valid cluster name",
                peer_sample(crate::readiness::IN_FLIGHT_SIGNAL, "pool.v1", 3.0),
                true,
            ),
            (
                "provider with the store separator",
                peer_sample(crate::readiness::IN_FLIGHT_SIGNAL, "pool/v1", 3.0),
                false,
            ),
            (
                "blank provider",
                peer_sample(crate::readiness::IN_FLIGHT_SIGNAL, "  ", 3.0),
                false,
            ),
            (
                "not finite",
                peer_sample(crate::readiness::IN_FLIGHT_SIGNAL, "pool", f64::NAN),
                false,
            ),
            (
                "infinite",
                peer_sample(crate::readiness::IN_FLIGHT_SIGNAL, "pool", f64::INFINITY),
                false,
            ),
            (
                "negative",
                peer_sample(crate::readiness::IN_FLIGHT_SIGNAL, "pool", -1.0),
                false,
            ),
            (
                "share above one",
                peer_sample(crate::latency::ERROR_RATIO_SIGNAL, "pool", 1.5),
                false,
            ),
            (
                "saturation above one",
                peer_sample(crate::readiness::IN_FLIGHT_SIGNAL, "pool", 1.5),
                true,
            ),
        ];
        for (label, observation, kept) in cases {
            assert_eq!(
                bound_peer(vec![observation], "retail").len(),
                usize::from(kept),
                "{label}"
            );
        }
    }

    #[test]
    fn a_hub_keeps_at_most_the_first_providers_of_a_peer_in_name_order() {
        let observations: Vec<Observation> = (0..MAX_PEER_PROVIDERS + 5)
            .map(|i| peer_sample(crate::readiness::READY_SIGNAL, &format!("p{i:03}"), 1.0))
            .collect();
        let kept = bound_peer(observations, "retail");
        assert_eq!(kept.len(), MAX_PEER_PROVIDERS);
        assert!(
            kept.iter()
                .all(|o| o.labels.get(PROVIDER_LABEL).is_some_and(|p| p.as_str() < "p064")),
            "the first {MAX_PEER_PROVIDERS} in name order are kept"
        );
    }

    #[test]
    fn peers_are_dialed_at_a_dialable_signals_endpoint() {
        let member = |site: &str, endpoint: &str, signals: Option<&str>| MemberRecord {
            site_id: site.to_owned(),
            endpoint: endpoint.to_owned(),
            incarnation: 0,
            status: crate::swim::MemberStatus::Alive,
            age_secs: 0,
            gateway_address: None,
            site_cert_pem: None,
            signals_address: signals.map(str::to_owned),
        };
        let members = [
            member("hub", "10.0.0.1:7946", None),
            member("lb", "10.0.0.2:7946", Some("203.0.113.7:9443")),
            member("older", "[fd00::2]:7946", None),
            member("loop", "127.0.0.1:7946", None),
            member("meta", "10.0.0.3:7946", Some("[fd00:ec2::254]:9091")),
            member("named", "10.0.0.4:7946", Some("East.Example:9091")),
        ];
        let urls: Vec<String> = peer_sites(members.iter(), "hub", "https", 9191)
            .into_iter()
            .map(|site| site.url)
            .collect();
        assert_eq!(
            urls,
            [
                "https://203.0.113.7:9443/v1/site/signals",
                "https://[fd00::2]:9191/v1/site/signals",
                "https://east.example:9091/v1/site/signals"
            ]
        );
    }

    /// A plain HTTP peer for `site` that answers once `release` fires.
    async fn held_peer(site: &'static str, release: tokio::sync::oneshot::Receiver<()>) -> PeerSite {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = [0_u8; 1024];
            drop(stream.read(&mut request).await);
            drop(release.await);
            let body = format!("inference_pool_average_queue_size{{grid_site=\"{site}\",grid_provider=\"pool\"}} 1\n");
            let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}", body.len());
            drop(stream.write_all(response.as_bytes()).await);
        });
        PeerSite {
            name: site.to_owned(),
            url: format!("http://{addr}{SIGNALS_PATH}"),
            pins: Vec::new(),
            answering: true,
            alias: None,
        }
    }

    /// A plain HTTP peer for `site` answering `body` once.
    async fn peer_serving(site: &str, body: String) -> PeerSite {
        PeerSite {
            name: site.to_owned(),
            url: serve_once(body).await,
            pins: Vec::new(),
            answering: true,
            alias: None,
        }
    }

    /// Serve `body` to the first request, returning the URL to dial.
    async fn serve_once(body: String) -> String {
        serve_once_with(body, String::new()).await
    }

    /// Serve `body` under the extra header lines `headers` to the first request, returning the URL to dial.
    async fn serve_once_with(body: String, headers: String) -> String {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = [0_u8; 1024];
            drop(stream.read(&mut request).await);
            let response = format!(
                "HTTP/1.1 200 OK\r\n{headers}Content-Length: {}\r\n\r\n{body}",
                body.len()
            );
            drop(stream.write_all(response.as_bytes()).await);
        });
        format!("http://{addr}{SIGNALS_PATH}")
    }

    /// The `Date` a peer answering at `date` sends, and that instant in epoch milliseconds.
    fn dated(date: SystemTime) -> (String, i64) {
        let date_ms =
            i64::try_from(date.duration_since(UNIX_EPOCH).expect("after the epoch").as_millis()).expect("fits i64");
        (format!("Date: {}\r\n", httpdate::fmt_http_date(date)), date_ms)
    }

    /// Through the real poll path: a row naming another site is refused and an unlabeled row is
    /// stamped with the dialed peer, so a relaying peer cannot speak for a third site.
    #[tokio::test]
    async fn a_polled_row_is_bound_to_the_dialed_peer() {
        crate::init_process_crypto();
        let body = "inference_pool_average_queue_size{grid_site=\"west\",grid_provider=\"pool\"} 1\n\
                    inference_pool_average_queue_size{grid_provider=\"own\"} 2\n"
            .to_owned();
        let sites = [peer_serving("east", body).await];
        let poll = PollPeers {
            attempts: 1,
            ..PollPeers::default()
        };
        let round = poll.collect(&sites).await;
        let rows = round.get("east").expect("east polled");
        assert_eq!(rows.len(), 1, "the row naming west is refused: {rows:?}");
        let row = rows.first().expect("one row");
        assert_eq!(row.labels.get(SITE_LABEL).map(String::as_str), Some("east"));
        assert_eq!(row.labels.get("grid_provider").map(String::as_str), Some("own"));
    }

    /// The slow peer answers only after the fast one is published, so a round that waited would hang.
    #[tokio::test]
    async fn a_slow_peer_does_not_hold_back_a_fast_one() {
        crate::init_process_crypto();
        let (release_fast, fast_gate) = tokio::sync::oneshot::channel();
        let (release_slow, slow_gate) = tokio::sync::oneshot::channel();
        let sites = [held_peer("slow", slow_gate).await, held_peer("fast", fast_gate).await];
        release_fast.send(()).unwrap_or(());
        let poll = PollPeers {
            attempts: 1,
            ..PollPeers::default()
        };
        let mut release_slow = Some(release_slow);
        let mut order = Vec::new();
        let round = poll.collect_each(&sites, |peer, observations, _, _| {
            assert_eq!(observations.len(), 1, "{peer}");
            if let Some(release) = release_slow.take() {
                release.send(()).unwrap_or(());
            }
            order.push(peer);
        });
        tokio::time::timeout(Duration::from_secs(10), round)
            .await
            .expect("the fast peer was published before the slow one answered");
        assert_eq!(order, ["fast", "slow"]);
    }

    /// A peer that sends headers promising a body, then stalls.
    async fn stalled_peer(site: &str) -> PeerSite {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = [0_u8; 1024];
            drop(stream.read(&mut request).await);
            drop(
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\nload")
                    .await,
            );
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        PeerSite {
            name: site.to_owned(),
            url: format!("http://{addr}{SIGNALS_PATH}"),
            pins: Vec::new(),
            answering: true,
            alias: None,
        }
    }

    #[tokio::test]
    async fn a_peer_stalled_mid_body_does_not_hold_the_round() {
        crate::init_process_crypto();
        for (label, timeout, budget) in [
            ("request deadline", Duration::from_millis(200), Duration::from_secs(5)),
            ("peer budget", Duration::from_secs(20), Duration::from_millis(300)),
        ] {
            let (release, gate) = tokio::sync::oneshot::channel();
            release.send(()).unwrap_or(());
            let sites = [stalled_peer("stalled").await, held_peer("live", gate).await];
            let poll = PollPeers {
                attempts: 1,
                timeout,
                budget,
                ..PollPeers::default()
            };
            let round = tokio::time::timeout(Duration::from_secs(5), poll.collect(&sites))
                .await
                .expect(label);
            assert_eq!(round.keys().collect::<Vec<_>>(), ["live"], "{label}");
        }
    }

    #[tokio::test]
    #[expect(clippy::too_many_lines, reason = "a peer that answers late, served inline")]
    async fn a_slow_peer_that_missed_a_round_still_answers_inside_its_short_budget() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        crate::init_process_crypto();
        let (date, date_ms) = dated(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        let body = format!("inference_pool_average_queue_size{{grid_provider=\"pool-a\"}} 1 {date_ms}\n");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = [0_u8; 1024];
            drop(stream.read(&mut request).await);
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            let response = format!("HTTP/1.1 200 OK\r\n{date}Content-Length: {}\r\n\r\n{body}", body.len());
            drop(stream.write_all(response.as_bytes()).await);
        });
        let slow = PeerSite {
            name: "east".to_owned(),
            url: format!("http://{addr}{SIGNALS_PATH}"),
            pins: Vec::new(),
            answering: false,
            alias: None,
        };
        let poll = PollPeers {
            timeout: Duration::from_secs(5),
            budget: Duration::from_secs(10),
            round: Duration::from_secs(5),
            ..PollPeers::default()
        };
        let round = poll.collect(&[slow]).await;
        assert!(
            round.contains_key("east"),
            "1.5 s fits the 2 s budget of one that missed a round"
        );
    }

    #[tokio::test]
    async fn a_peer_that_did_not_answer_gets_the_short_budget() {
        crate::init_process_crypto();
        let poll = PollPeers {
            attempts: 1,
            timeout: Duration::from_secs(20),
            budget: Duration::from_secs(20),
            round: Duration::from_secs(20),
            ..PollPeers::default()
        };
        let unanswered = PeerSite {
            answering: false,
            ..stalled_peer("stalled").await
        };
        let started = Instant::now();
        let round = tokio::time::timeout(Duration::from_secs(10), poll.collect(&[unanswered]))
            .await
            .expect("the short budget ends the poll");
        assert!(round.is_empty());
        assert!(started.elapsed() < UNANSWERED_PEER_BUDGET + Duration::from_secs(1));

        let answering = stalled_peer("stalled").await;
        assert!(
            tokio::time::timeout(UNANSWERED_PEER_BUDGET * 2, poll.collect(&[answering]))
                .await
                .is_err(),
            "a peer that answered keeps the full budget"
        );
    }

    #[tokio::test]
    async fn a_round_ends_on_time_however_many_peers_cannot_answer() {
        crate::init_process_crypto();
        let (release, gate) = tokio::sync::oneshot::channel();
        release.send(()).unwrap_or(());
        let mut sites = vec![held_peer("live", gate).await];
        for index in 0..32 {
            sites.push(PeerSite {
                answering: false,
                ..stalled_peer(&format!("dead-{index}")).await
            });
        }
        let poll = PollPeers {
            attempts: 1,
            timeout: Duration::from_secs(20),
            budget: Duration::from_secs(20),
            round: Duration::from_secs(1),
            ..PollPeers::default()
        };
        let started = Instant::now();
        let round = poll.collect(&sites).await;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the round took {:?}",
            started.elapsed()
        );
        assert_eq!(round.keys().collect::<Vec<_>>(), ["live"]);
    }

    #[test]
    fn only_strict_endpoint_text_parses() {
        let cases = [
            ("ipv4", "10.0.0.1:9091", Some("10.0.0.1:9091")),
            ("bracketed ipv6", "[fd00::1]:9091", Some("[fd00::1]:9091")),
            ("dns name", "east.grid.example:9091", Some("east.grid.example:9091")),
            ("bare ipv6", "fd00::1", None),
            ("unbracketed ipv6 with a port", "fd00::1:9091", None),
            ("userinfo", "x@169.254.169.254:443", None),
            ("path and query", "evil.example/x?:9091", None),
            ("fragment", "evil.example#x:9091", None),
            ("percent", "evil%2eexample:9091", None),
            ("no port", "east.example", None),
            ("empty port", "east.example:", None),
            ("signed port", "east.example:+9091", None),
            ("port zero", "10.0.0.1:0", None),
            ("port overflow", "east.example:70000", None),
            ("leading hyphen", "-east.example:9091", None),
            ("ipv4-looking name", "999.1.1.1:9091", None),
            ("empty", "", None),
        ];
        for (label, text, want) in cases {
            let got = SignalsEndpoint::parse(text).map(|endpoint| endpoint.authority());
            assert_eq!(got.as_deref(), want, "{label}");
        }
    }

    #[test]
    fn refused_endpoints_are_warned_once_per_peer() {
        let (site, endpoint) = ("warn-once-site", "x@169.254.169.254:443");
        assert!(warn_refused_once(site, endpoint), "first refusal warns");
        assert!(!warn_refused_once(site, endpoint), "the same refusal is quiet");
        assert!(warn_refused_once(site, "127.0.0.1:9091"), "a new value warns");
    }

    #[test]
    fn a_collect_query_is_rebuilt_through_the_uri_parser() {
        let url = "https://[fd00::1]:9091/v1/site/signals";
        assert_eq!(
            with_query(url, "collect[]=a").as_deref(),
            Some("https://[fd00::1]:9091/v1/site/signals?collect[]=a")
        );
        assert_eq!(with_query(url, "a b"), None, "an invalid query is refused");
    }

    #[test]
    fn a_colon_in_a_metric_name_is_a_name_not_a_delimiter() {
        // Every vLLM metric is named this way. The obvious parser crates match
        // the name with \w+, drop the whole line, and report a short scrape.
        let o = parse("vllm:num_requests_waiting{model=\"a\"} 7");
        let first = o.first().expect("colon-named metric must survive");
        assert_eq!(first.metric, "vllm:num_requests_waiting");
        assert_eq!(first.labels.get("model").map(String::as_str), Some("a"));
        assert!((first.value - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn a_brace_inside_a_label_value_does_not_end_the_label_set() {
        let o = parse(r#"m{note="has } brace",k="v"} 3"#);
        let first = o.first().expect("line must parse");
        assert_eq!(first.labels.get("note").map(String::as_str), Some("has } brace"));
        assert_eq!(first.labels.get("k").map(String::as_str), Some("v"));
    }

    #[test]
    fn an_escaped_quote_stays_inside_the_value() {
        let o = parse(r#"m{a="say \"hi\"",b="c"} 1"#);
        let first = o.first().expect("line must parse");
        assert_eq!(first.labels.get("a").map(String::as_str), Some(r#"say "hi""#));
        assert_eq!(first.labels.get("b").map(String::as_str), Some("c"));
    }

    #[test]
    fn a_type_declared_after_its_samples_still_applies() {
        // Reading in one pass would admit this counter.
        let o = parse("c_total 5\n# TYPE c_total counter\n");
        assert!(o.is_empty(), "late TYPE must still drop the counter: {o:?}");
    }

    #[test]
    fn a_trailing_timestamp_is_kept_and_not_read_as_the_value() {
        let o = parse("m{a=\"b\"} 2 1700000000000");
        let first = o.first().expect("line must parse");
        assert!((first.value - 2.0).abs() < f64::EPSILON, "value, not timestamp");
        assert_eq!(
            first.timestamp_ms,
            Some(1_700_000_000_000),
            "the trailing timestamp is kept"
        );

        let none = parse("m{a=\"b\"} 2");
        assert_eq!(
            none.first().expect("line must parse").timestamp_ms,
            None,
            "no timestamp is None"
        );
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "one table of ages either side of each bound")]
    fn reexpress_peer_ages_keeps_plausible_ages_and_drops_the_rest() {
        let date_ms = 5_000_000_000_000;
        let stamped = |stamp: Option<i64>| Observation {
            timestamp_ms: stamp,
            ..aged(Duration::ZERO)
        };
        let samples = vec![
            stamped(Some(date_ms - 30_000)),
            stamped(Some(date_ms + 1_000)),
            stamped(Some(date_ms + 1_001)),
            stamped(Some(date_ms - MAX_RELAY_AGE_MS)),
            stamped(Some(date_ms - MAX_RELAY_AGE_MS - 1)),
            stamped(None),
        ];
        let date = UNIX_EPOCH + Duration::from_millis(u64::try_from(date_ms).expect("positive"));
        let (kept, rejected) = reexpress_peer_ages(samples, Some(date), "reexpress-bounds");
        assert_eq!(
            kept.iter().map(|o| (o.age, o.timestamp_ms)).collect::<Vec<_>>(),
            [
                (Duration::from_secs(30), None),
                (Duration::ZERO, None),
                (Duration::from_millis(86_400_000), None),
                (Duration::ZERO, None)
            ],
            "30 s and a day keep their age, a second ahead reads as now, no stamp is aged from collection, \
             and no peer stamp is kept to be rendered"
        );
        assert_eq!(rejected, 2, "past a second ahead and past a day old are dropped");
        assert!(
            refused_metric("reexpress-bounds", "skew", 2),
            "each drop is counted as skew"
        );
    }

    #[test]
    fn a_relayed_row_without_a_date_is_aged_from_its_collection() {
        let undated = vec![Observation {
            timestamp_ms: Some(1_000),
            ..aged(Duration::ZERO)
        }];
        let (kept, rejected) = reexpress_peer_ages(undated, None, "undated");
        assert_eq!(
            (kept.first().map(|o| o.age), rejected),
            (Some(Duration::ZERO), 0),
            "without a Date there is no age to carry, and nothing to reject"
        );
    }

    /// The gateway's signals crate, which applies the same relay-age bounds on the next hop.
    const GATEWAY_SIGNALS: &str = include_str!("../../signals/src/signals.rs");

    #[test]
    fn the_relay_age_bounds_match_the_gateways() {
        let declared = |name: &str| -> Option<i64> {
            let declaration = format!("const {name}: i64 = ");
            GATEWAY_SIGNALS
                .lines()
                .find_map(|line| line.split_once(declaration.as_str()))
                .and_then(|(_, value)| value.split(';').next())
                .and_then(|value| value.replace('_', "").parse().ok())
        };
        assert_eq!(
            (declared("RELAY_AGE_TOLERANCE_MS"), declared("MAX_RELAY_AGE_MS")),
            (Some(RELAY_AGE_TOLERANCE_MS), Some(MAX_RELAY_AGE_MS)),
            "both hops keep and drop the same relayed ages"
        );
    }

    #[tokio::test]
    async fn a_polled_row_with_an_implausible_age_is_dropped_and_counted() {
        crate::init_process_crypto();
        let (date, date_ms) = dated(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        let line = |provider: &str, stamp: i64| {
            format!("inference_pool_average_queue_size{{grid_provider=\"{provider}\"}} 1 {stamp}\n")
        };
        let body = [
            line("ahead-by-a-second", date_ms + 1_000),
            line("ahead-past-a-second", date_ms + 1_001),
            line("a-day-old", date_ms - MAX_RELAY_AGE_MS),
            line("past-a-day-old", date_ms - MAX_RELAY_AGE_MS - 1),
        ]
        .concat();
        let round = poll_one_peer("age-bounds", body, date).await;
        let providers: Vec<&str> = round
            .get("age-bounds")
            .expect("the peer answered")
            .iter()
            .filter_map(|row| row.labels.get(PROVIDER_LABEL).map(String::as_str))
            .collect();
        assert_eq!(
            providers,
            ["ahead-by-a-second", "a-day-old"],
            "an age under minus one second or over a day is dropped, not refreshed"
        );
        assert!(
            refused_metric("age-bounds", "skew", 2),
            "both drops are counted as skew"
        );
    }

    #[tokio::test]
    async fn a_peer_serving_implausible_ages_is_named_on_the_condition() {
        crate::init_process_crypto();
        let (date, date_ms) = dated(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        let mut sites = Vec::new();
        for (peer, stamp) in [
            ("ahead", date_ms + 1_001),
            ("ancient", date_ms - MAX_RELAY_AGE_MS - 1),
            ("clean", date_ms - 1_000),
        ] {
            let body = format!("inference_pool_average_queue_size{{grid_provider=\"pool-a\"}} 1 {stamp}\n");
            sites.push(dated_peer(peer, body, date.clone()).await);
        }
        let mut answered = BTreeMap::new();
        one_attempt()
            .collect_each(&sites, |peer, _, rejected, _| {
                answered.insert(peer, rejected);
            })
            .await;
        let mut ages = crate::peer_ages::PeerAges::default();
        ages.observe(
            &sites.iter().map(|site| site.name.clone()).collect::<Vec<_>>(),
            &answered,
        );
        let condition = ages.condition(&[], "t0", Some(1)).expect("raised");
        assert_eq!(condition.status, "False", "{condition:?}");
        assert!(
            condition.message.contains("from ahead, ancient:"),
            "the peers stamping past their Date and a day behind it are named, the clean one is not: {}",
            condition.message
        );
    }

    #[tokio::test]
    async fn a_row_dropped_for_its_age_takes_no_provider_slot() {
        crate::init_process_crypto();
        let (date, date_ms) = dated(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        let line = |provider: &str, stamp: i64| {
            format!("inference_pool_average_queue_size{{grid_provider=\"{provider}\"}} 1 {stamp}\n")
        };
        let body: String = (0..MAX_PEER_PROVIDERS)
            .map(|i| line(&format!("a{i:03}"), date_ms + 60_000))
            .chain(std::iter::once(line("zz", date_ms)))
            .collect();
        let round = poll_one_peer("age-slots", body, date).await;
        let kept: Vec<&str> = round
            .get("age-slots")
            .expect("the peer answered")
            .iter()
            .filter_map(|row| row.labels.get(PROVIDER_LABEL).map(String::as_str))
            .collect();
        assert_eq!(
            kept,
            ["zz"],
            "the rows dropped for their age do not fill the provider cap first"
        );
    }

    /// `n` lines of one contract series for `target`, under distinct labels.
    fn stocked(target: &str, n: usize) -> (String, Vec<Observation>) {
        let samples = (0..n)
            .map(|i| Observation {
                metric: "inference_pool_per_pod_queue_size".to_owned(),
                labels: BTreeMap::from([
                    ("grid_provider".to_owned(), target.to_owned()),
                    ("pod".to_owned(), format!("p{i}")),
                ]),
                value: 1.0,
                timestamp_ms: None,
                age: Duration::ZERO,
            })
            .collect();
        (target.to_owned(), samples)
    }

    /// One contract sample of provider `pool-a`, already `age` old when collected.
    fn aged(age: Duration) -> Observation {
        Observation {
            metric: "inference_pool_average_queue_size".to_owned(),
            labels: BTreeMap::from([(PROVIDER_LABEL.to_owned(), "pool-a".to_owned())]),
            value: 1.0,
            timestamp_ms: None,
            age,
        }
    }

    /// The trailing stamp of the first line of `body`.
    fn stamp_of(body: &str) -> i64 {
        body.lines()
            .next()
            .and_then(|line| line.rsplit_once(' '))
            .and_then(|(_, stamp)| stamp.parse().ok())
            .expect("a stamped line")
    }

    /// A `Date` header value as a reader takes it, epoch milliseconds.
    fn read_date_ms(header: &str) -> i64 {
        let date = httpdate::parse_http_date(header).expect("an HTTP date");
        i64::try_from(date.duration_since(UNIX_EPOCH).expect("after the epoch").as_millis()).expect("fits i64")
    }

    /// A peers store holding one row that `east` served `age_ms` before its `Date`, through the poll path.
    async fn relayed_through_a_poll(age_ms: i64) -> SignalStore {
        crate::init_process_crypto();
        let (date, date_ms) = dated(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        let row = format!(
            "inference_pool_average_queue_size{{grid_provider=\"pool-a\"}} 1 {}\n",
            date_ms - age_ms
        );
        let peers = SignalStore::new();
        peers.refresh(poll_one_peer("east", row, date).await, Duration::from_secs(60));
        peers
    }

    #[tokio::test]
    async fn a_peer_cannot_relay_poller_rounds() {
        crate::init_process_crypto();
        let (date, date_ms) = dated(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        let body = format!(
            "inference_pool_average_queue_size{{grid_provider=\"pool-a\"}} 1 {date_ms}\n\
             inference_pool_average_queue_size{{grid_provider=\"{provider}\"}} 1 {date_ms}\n\
             {}{{grid_site=\"west\",grid_provider=\"{provider}\"}} 1 {date_ms}\n",
            grid_signals::poller::METRIC,
            provider = grid_signals::poller::PROVIDER,
        );
        let round = poll_one_peer("east", body, date).await;
        let metrics: Vec<&str> = round
            .get("east")
            .expect("east answered")
            .iter()
            .map(|row| row.metric.as_str())
            .collect();
        assert_eq!(metrics, ["inference_pool_average_queue_size"]);
    }

    #[test]
    fn heard_load_keeps_each_peers_most_recent_providers() {
        let row = |provider: String| Observation {
            metric: "inference_pool_average_queue_size".to_owned(),
            labels: BTreeMap::from([(PROVIDER_LABEL.to_owned(), provider)]),
            value: 0.0,
            timestamp_ms: None,
            age: Duration::ZERO,
        };
        let heard = LoadHeard::default();
        let first: Vec<_> = (0..MAX_PEER_PROVIDERS).map(|i| row(format!("old-{i}"))).collect();
        heard.record("east", &first);
        heard.record("east", &[row("old-0".to_owned())]);
        heard.record("east", &[row("renamed".to_owned())]);
        assert!(heard.heard("east", "renamed"), "a new name past the cap is learned");
        assert!(heard.heard("east", "old-0"), "one heard again is kept");
        assert!(!heard.heard("east", "old-1"), "the least recently heard is dropped");
        assert!(!heard.heard("west", "renamed"), "kept per peer");
    }

    #[test]
    fn poller_rounds_render_one_stamped_row_per_peer() {
        let rounds = PeerRounds::default();
        let at = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
        rounds.record(&["east".to_owned(), "west".to_owned()], at);
        assert_eq!(
            rounds.render(),
            "grid_peer_poll_round{grid_site=\"east\",grid_provider=\"_poller\"} 1 1700000000123\n\
             grid_peer_poll_round{grid_site=\"west\",grid_provider=\"_poller\"} 1 1700000000123\n"
        );
        rounds.record(&["west".to_owned()], at);
        assert!(!rounds.render().contains("east"), "a peer no longer polled has no row");
    }

    /// One round polling `site`, which answers `body` under the extra header lines `headers`.
    async fn poll_one_peer(site: &str, body: String, headers: String) -> BTreeMap<String, Vec<Observation>> {
        one_attempt().collect(&[dated_peer(site, body, headers).await]).await
    }

    /// A peer named `site` answering `body` once, under the extra header lines `headers`.
    async fn dated_peer(site: &str, body: String, headers: String) -> PeerSite {
        PeerSite {
            name: site.to_owned(),
            url: serve_once_with(body, headers).await,
            pins: Vec::new(),
            answering: true,
            alias: None,
        }
    }

    /// A poller that tries each peer once.
    fn one_attempt() -> PollPeers {
        PollPeers {
            attempts: 1,
            ..PollPeers::default()
        }
    }

    /// Whether `grid_peer_signals_refused_total` reads `count` for `peer` and `reason`.
    fn refused_metric(peer: &str, reason: &str, count: u64) -> bool {
        String::from_utf8(crate::metrics::encode_metrics())
            .expect("utf-8 metrics")
            .contains(&format!(
                r#"grid_peer_signals_refused_total{{peer="{peer}",reason="{reason}"}} {count}"#
            ))
    }

    /// One unlabeled sample of `metric`.
    fn plain(metric: &str) -> Observation {
        Observation {
            metric: metric.to_owned(),
            labels: BTreeMap::new(),
            value: 1.0,
            timestamp_ms: None,
            age: Duration::ZERO,
        }
    }

    #[test]
    fn the_bounded_render_carries_contract_series_only() {
        let store = SignalStore::new();
        let samples = vec![plain("vendor_private_gauge"), plain("inference_pool_ready_pods")];
        store.refresh(
            BTreeMap::from([("pool-a".to_owned(), samples)]),
            Duration::from_secs(60),
        );
        let rendered = store.render_bounded(None, &[], MAX_RELAY_BYTES, ClockReading::now());
        assert!(rendered.body.contains("inference_pool_ready_pods"), "{}", rendered.body);
        assert!(
            !rendered.body.contains("vendor_private_gauge"),
            "a non-contract series is not relayed"
        );
        let (unrestricted, _) = store.render_unrestricted(None, &[], ClockReading::now());
        assert!(
            unrestricted.contains("vendor_private_gauge"),
            "the unbounded render still has it"
        );
    }

    #[test]
    fn revision_metadata_survives_metric_selection_with_exact_count() {
        let store = SignalStore::new();
        store.refresh(
            BTreeMap::from([("model".to_owned(), parse(&revision_text()))]),
            Duration::from_secs(60),
        );
        let selected = vec!["llm_d_epp_average_queue_size".to_owned()];
        for bounded in [false, true] {
            let body = if bounded {
                store
                    .render_bounded(None, &selected, MAX_RELAY_BYTES, ClockReading::now())
                    .body
            } else {
                store.render_unrestricted(None, &selected, ClockReading::now()).0
            };
            let rows = parse(&body);
            assert_eq!(rows.len(), 2, "one selected gauge and its metadata survive: {body}");
            let metadata = rows
                .iter()
                .find(|row| row.metric == grid_signals::revision::METRIC)
                .expect("metadata");
            assert_eq!(
                metadata
                    .labels
                    .get(grid_signals::revision::SAMPLES_LABEL)
                    .map(String::as_str),
                Some("1")
            );
            assert!(rows.iter().all(|row| row.timestamp_ms == metadata.timestamp_ms));
        }
    }

    #[test]
    fn incomplete_conflicting_and_malformed_revision_batches_are_refused() {
        let valid = revision_text();
        assert_eq!(parse(&valid).len(), 3);
        for invalid in [
            valid.replace("provider_uid=\"u1\"", "provider_uid=\"u1\",provider_uid=\"u2\""),
            valid.replace("publisher_epoch=\"1\"", "publisher_epoch=\"0\""),
            format!("{valid}{}\n", valid.lines().next().expect("metadata")),
            format!("{valid}{}", valid.replace(" 7 2000", " 0 2000")),
            format!("{}{valid}", valid.replace(" 7 2000", " 0 2000")),
        ] {
            assert!(
                parse(&invalid).is_empty(),
                "an invalid batch must never be relayed: {invalid}"
            );
        }
    }

    #[test]
    fn future_publisher_epochs_are_never_relayed_as_payload_or_fences() {
        assert_eq!(
            grid_signals::revision::MAX_PUBLISHER_EPOCH_SKEW,
            swim::identity::MAX_LEASE_SKEW,
            "signals and SWIM enforce the same reservation bound"
        );
        let future = revision_text().replace("publisher_epoch=\"1\"", &format!("publisher_epoch=\"{}\"", u64::MAX));
        for invalid in [
            future.clone(),
            future.replace("batch_samples=\"2\"", "batch_samples=\"3\""),
            format!("{future}\nmalformed"),
            future.replace(" 2 2000", " 2 99999999"),
        ] {
            for date in [None, Some(UNIX_EPOCH + Duration::from_secs(20_000_000_000))] {
                let (rows, _) = reexpress_peer_ages(parse(&invalid), date, "east");
                assert!(
                    rows.is_empty(),
                    "future epochs cannot survive as invalidations, even with a missing or forged Date"
                );
            }
        }
        assert_eq!(parse(&revision_text()).len(), 3, "ordinary revisions still relay");
    }

    #[test]
    fn incomplete_payload_retains_only_its_revision_fence() {
        let valid = revision_text();
        for incomplete in [
            valid.replace("batch_samples=\"2\"", "batch_samples=\"3\""),
            valid.replace(" 2 2000", " 2 1999"),
            valid.replace(" 2 2000", " 2"),
        ] {
            let rows = parse(&incomplete);
            assert_eq!(
                rows.len(),
                1,
                "valid metadata survives unusable payload as an invalidation"
            );
            assert_eq!(
                rows.first()
                    .and_then(|row| row.labels.get(grid_signals::revision::SAMPLES_LABEL))
                    .map(String::as_str),
                Some("0")
            );
        }
    }

    #[test]
    fn peer_row_rejection_cannot_leave_a_partially_qualified_batch() {
        let parsed = parse(&revision_text().replace(" 2 2000", " -2 2000"));
        assert_eq!(parsed.len(), 3, "wire completeness precedes value policy");
        let bounded = bound_peer(parsed, "east");
        let (body, ..) = render_contract_block(&bounded, &[], Duration::ZERO, SystemTime::now());
        let rows = parse(&body);
        assert_eq!(rows.len(), 1, "rejected raw rows preserve only an ordered invalidation");
        assert_eq!(
            rows.first()
                .and_then(|row| row.labels.get(grid_signals::revision::SAMPLES_LABEL))
                .map(String::as_str),
            Some("0")
        );
    }

    #[test]
    fn peer_relay_keeps_revision_batch_timestamps_together() {
        let (aged, rejected) = reexpress_peer_ages(
            parse(&revision_text()),
            Some(UNIX_EPOCH + Duration::from_millis(3000)),
            "east",
        );
        assert_eq!(rejected, 0);
        let (body, _, cut) = render_contract_block(
            &bound_peer(aged, "east"),
            &[],
            Duration::from_secs(1),
            UNIX_EPOCH + Duration::from_secs(10),
        );
        let reparsed = parse(&body);
        assert_eq!(reparsed.len(), 3, "all rows survive a relay: {body}");
        assert_eq!(cut, 0);
        assert!(
            reparsed.iter().all(|row| row.timestamp_ms == Some(8000)),
            "the batch retains one relative age"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "exercise HTTP projection, relay and guarded consumption together"
    )]
    async fn a_default_peer_poll_projects_custom_metrics_without_losing_revision() {
        crate::init_process_crypto();
        let body = format!(
            "{}vendor_custom_gauge{{grid_site=\"east\",grid_provider=\"model\"}} 4 2000\n",
            revision_text().replace("batch_samples=\"2\"", "batch_samples=\"3\"")
        );
        let (headers, _) = dated(UNIX_EPOCH + Duration::from_secs(3));
        let peer = PeerSite {
            name: "east".to_owned(),
            url: serve_once_with(body, headers).await,
            pins: Vec::new(),
            answering: true,
            alias: None,
        };
        let collected = one_attempt().collect(&[peer]).await;
        let hub = SignalStore::new();
        hub.refresh(collected, Duration::from_secs(60));
        let clock = ClockReading::now();
        let rendered = hub.render_bounded(None, &[], MAX_RELAY_BYTES, clock);
        let rows = parse(&rendered.body);
        assert_eq!(
            rows.len(),
            3,
            "custom gauges cannot erase legitimate telemetry: {}",
            rendered.body
        );
        assert!(!rendered.body.contains("vendor_custom_gauge"));
        assert!(rendered.body.contains("batch_samples=\"2\""));
        let receiver = grid_signals::LoadStore::new(Duration::from_secs(60)).with_revision_guard(30_000);
        receiver.ingest_at(&rendered.body, wall_millis(clock.wall, Duration::ZERO), 10_000, "east");
        assert!(receiver.learning_state("east/model", 10_000, 30_000).epoch.is_some());
        assert_eq!(
            receiver
                .latest("east/model", "llm_d_epp_average_queue_size")
                .map(|sample| sample.value),
            Some(2.0)
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "invalidation, replay rejection and recovery share one receiver"
    )]
    fn relay_invalidation_fences_old_data_and_allows_later_learning() {
        let old = revision_text();
        let new = old
            .replace("publisher_epoch=\"1\"", "publisher_epoch=\"2\"")
            .replace("provider_uid=\"u1\"", "provider_uid=\"u2\"");
        for invalid in [
            new.replace("batch_samples=\"2\"", "batch_samples=\"3\""),
            new.replace(" 2 2000", " -2 2000"),
            new.replace(" 2 2000", " 2 1999"),
            format!(
                "{new}{}",
                old.replace("grid_provider=\"model\"", "grid_provider=\"unrelated\"")
                    .replace(" 7 2000", " 0 2000")
            ),
        ] {
            let receiver = grid_signals::LoadStore::new(Duration::from_secs(60)).with_revision_guard(30_000);
            ingest_revision_relay(&receiver, &old);
            assert!(receiver.learning_state("east/model", 10_000, 30_000).epoch.is_some());
            ingest_revision_relay(&receiver, &invalid);
            assert_eq!(receiver.learning_state("east/model", 10_000, 30_000).epoch, None);
            assert!(receiver.latest("east/model", "llm_d_epp_average_queue_size").is_none());
            ingest_revision_relay(
                &receiver,
                &old.replace("batch_sequence=\"2\"", "batch_sequence=\"999\""),
            );
            ingest_revision_relay(&receiver, &new);
            assert_eq!(
                receiver.learning_state("east/model", 10_000, 30_000).epoch,
                None,
                "neither old authority nor an equal-order repair can re-enable learning"
            );
            ingest_revision_relay(&receiver, &new.replace("batch_sequence=\"2\"", "batch_sequence=\"3\""));
            assert!(
                receiver.learning_state("east/model", 10_000, 30_000).epoch.is_some(),
                "a later complete publication restores learning"
            );
        }
    }

    /// Exercise peer parsing, age conversion, allowlist projection, relay and guarded ingest.
    fn ingest_revision_relay(receiver: &grid_signals::LoadStore, body: &str) {
        let (aged, _) = reexpress_peer_ages(parse(body), Some(UNIX_EPOCH + Duration::from_secs(3)), "east");
        let (rendered, ..) = render_contract_block(
            &bound_peer(aged, "east"),
            &[],
            Duration::ZERO,
            UNIX_EPOCH + Duration::from_secs(10),
        );
        receiver.ingest_at(&rendered, 10_000, 10_000, "east");
    }

    #[tokio::test]
    async fn a_revision_without_date_relays_only_a_stale_invalidation() {
        crate::init_process_crypto();
        let peer = peer_serving("east", revision_text()).await;
        let collected = one_attempt().collect(&[peer]).await;
        let rows = collected.get("east").expect("peer answered");
        assert_eq!(rows.len(), 1, "undated payload cannot be rejuvenated");
        let metadata = rows.first().expect("ordered invalidation");
        assert_eq!(
            metadata
                .labels
                .get(grid_signals::revision::SAMPLES_LABEL)
                .map(String::as_str),
            Some("0")
        );
        assert!(
            metadata.age > Duration::from_secs(86_400),
            "uncertain freshness never becomes current"
        );
    }

    #[test]
    fn metadata_only_providers_do_not_displace_a_live_pressure_batch() {
        let metadata = parse(&revision_text())
            .into_iter()
            .find(|row| row.metric == grid_signals::revision::METRIC)
            .expect("metadata");
        let mut rows: Vec<_> = (0..MAX_PEER_PROVIDERS)
            .map(|index| {
                let mut row = invalidation(&metadata);
                row.labels.insert(PROVIDER_LABEL.to_owned(), format!("a{index:02}"));
                row
            })
            .collect();
        rows.extend(parse(
            &revision_text().replace("grid_provider=\"model\"", "grid_provider=\"z-live\""),
        ));
        let kept = bound_peer(rows, "east");
        assert_eq!(
            kept.iter()
                .filter(|row| row
                    .labels
                    .get(PROVIDER_LABEL)
                    .is_some_and(|provider| provider == "z-live"))
                .count(),
            3,
            "a pressure-bearing provider survives metadata cardinality"
        );
    }

    /// A complete provider batch before filtering or timestamp rebasing.
    fn revision_text() -> String {
        concat!(
            "grid_provider_config_generation{grid_site=\"east\",grid_provider=\"model\",provider_uid=\"u1\",publisher_epoch=\"1\",batch_sequence=\"2\",batch_samples=\"2\"} 7 2000\n",
            "llm_d_epp_average_queue_size{grid_site=\"east\",grid_provider=\"model\"} 2 2000\n",
            "llm_d_epp_ready_endpoints{grid_site=\"east\",grid_provider=\"model\"} 1 2000\n"
        ).to_owned()
    }

    #[test]
    fn a_target_past_the_line_cap_is_cut_and_the_others_still_render() {
        let store = SignalStore::new();
        let big = stocked("pool-big", MAX_RENDER_LINES_PER_TARGET + 10);
        let small = stocked("pool-small", 1);
        store.refresh(BTreeMap::from([big, small]), Duration::from_secs(60));
        let rendered = store.render_bounded(None, &[], MAX_RELAY_BYTES, ClockReading::now());
        let big_lines = rendered.body.lines().filter(|line| line.contains("pool-big")).count();
        assert_eq!(big_lines, 0, "a provider group is never rendered as a prefix");
        assert_eq!(rendered.lines_cut, MAX_RENDER_LINES_PER_TARGET + 10);
        assert!(rendered.body.contains("pool-small"), "the small target still renders");
        assert_eq!(rendered.targets_dropped, 0);
    }

    #[test]
    fn a_body_past_the_budget_drops_whole_targets_and_never_a_line() {
        let store = SignalStore::new();
        store.refresh(
            BTreeMap::from([stocked("pool-a", 20), stocked("pool-b", 20)]),
            Duration::from_secs(60),
        );
        let one = store
            .render_bounded(Some("pool-a"), &[], MAX_RELAY_BYTES, ClockReading::now())
            .body
            .len();
        // Room for one target and a bit: the second must go whole, not in part.
        let rendered = store.render_bounded(None, &[], one + one / 2, ClockReading::now());
        assert_eq!(rendered.targets_dropped, 1, "{}", rendered.body);
        assert_eq!(rendered.body.len(), one, "the first target is intact");
        assert!(rendered.body.ends_with('\n') && rendered.body.lines().all(|line| line.contains("pool-a")));
        let all = store.render_bounded(None, &[], MAX_RELAY_BYTES, ClockReading::now());
        assert_eq!(all.targets_dropped, 0, "under the budget nothing is dropped");
    }

    #[test]
    fn a_relayed_sample_renders_its_own_age_not_the_relay_time() {
        let store = SignalStore::new();
        store.refresh(
            BTreeMap::from([("pool-a".to_owned(), vec![aged(Duration::from_secs(3600))])]),
            Duration::from_secs(60),
        );
        let now = ClockReading::now();
        let (body, oldest) = store.render_unrestricted(None, &[], now);
        let hour_ago_ms = wall_millis(now.wall, Duration::from_secs(3600));
        assert!(
            (hour_ago_ms - 1_000..=hour_ago_ms).contains(&stamp_of(&body)),
            "a sample an hour old when relayed renders an hour old, not at the relay time: {body}"
        );
        assert!(
            oldest >= Duration::from_secs(3600),
            "Age reflects the sample age, not the relay: {oldest:?}"
        );
    }

    #[tokio::test]
    #[expect(clippy::too_many_lines, reason = "one polled row rendered at three wall readings")]
    async fn a_wall_step_between_poll_and_render_leaves_relayed_ages_alone() {
        let peers = relayed_through_a_poll(30_000).await;
        let later = Instant::now() + Duration::from_secs(5);
        let wall = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let hour = Duration::from_secs(3600);
        let mut seen = Vec::new();
        for (label, stepped) in [("no step", wall), ("forward", wall + hour), ("backward", wall - hour)] {
            let now = ClockReading {
                instant: later,
                wall: stepped,
            };
            let (rendered, oldest) = peers.render_unrestricted(None, &[], now);
            let bounded = peers.render_bounded(None, &[], MAX_RELAY_BYTES, now);
            let read_ms = read_date_ms(&httpdate::fmt_http_date(now.wall));
            let age_ms = read_ms - stamp_of(&rendered);
            assert_eq!(
                read_ms - stamp_of(&bounded.body),
                age_ms,
                "{label}: the bounded render stamps the same"
            );
            assert!(
                (0..=1).contains(&(age_ms - i64::try_from(oldest.as_millis()).expect("fits i64"))),
                "{label}: Date minus the stamp is the age the render reports, {age_ms} ms against {oldest:?}"
            );
            seen.push(age_ms);
        }
        assert!(
            seen.iter().all(|age| (35_000..36_000).contains(age)),
            "Date minus each stamp is the peer's 30 s plus the 5 s held, whatever the wall reads: {seen:?}"
        );
        assert!(
            seen.windows(2).all(|pair| pair.first() == pair.last()),
            "a step of the wall clock moves the stamps and the Date together: {seen:?}"
        );
    }

    #[test]
    fn a_peer_is_not_asked_for_a_target() {
        // target names one provider, and a peer serves from a store keyed by
        // provider. A site name matches nothing, so the peer answers empty and
        // no site relays another's data.
        let sites = vec![PeerSite {
            name: "pool-b".to_owned(),
            url: "http://10.0.0.2:9091/v1/site/signals".to_owned(),
            pins: Vec::new(),
            answering: true,
            alias: None,
        }];
        let urls = peer_urls(&sites, "");
        assert_eq!(
            urls.first().map(|target| target.url.as_str()),
            Some("http://10.0.0.2:9091/v1/site/signals"),
            "the whole site is asked for, with no target"
        );
    }

    #[test]
    fn requested_signal_names_still_reach_the_peer() {
        let sites = vec![PeerSite {
            name: "pool-b".to_owned(),
            url: "http://10.0.0.2:9091/v1/site/signals".to_owned(),
            pins: Vec::new(),
            answering: true,
            alias: None,
        }];
        let urls = peer_urls(&sites, "collect[]=queue");
        assert_eq!(
            urls.first().map(|target| target.url.as_str()),
            Some("http://10.0.0.2:9091/v1/site/signals?collect[]=queue")
        );
    }

    /// `status` as a scrape failure, for a table that reads in numbers.
    fn non_ok(status: u16) -> MetricsScrapeError {
        MetricsScrapeError::NonOkStatus {
            status,
            url: "https://peer/metrics".to_owned(),
        }
    }

    #[test]
    fn a_throttled_peer_is_named_without_being_hit_again_this_round() {
        // Three attempts at a 50ms base is not the schedule a 429 asked for.
        assert_eq!(classify(&non_ok(429)), RequestOutcome::Throttled);
        assert!(!classify(&non_ok(429)).is_retryable());
        assert!(!classify(&non_ok(403)).is_retryable());
    }

    #[test]
    fn a_success_status_carried_in_an_error_is_not_read_as_an_answer() {
        // Marking the peer up here would publish a round that scraped nothing.
        assert_eq!(classify(&non_ok(204)), RequestOutcome::Transport);
    }

    #[test]
    fn a_status_is_split_at_five_hundred() {
        let client = classify(&non_ok(403));
        let server = classify(&non_ok(503));
        assert_eq!(client, RequestOutcome::ClientError, "the peer answered and refused");
        assert_eq!(server, RequestOutcome::ServerError, "the peer is having a bad time");
        assert!(!client.is_retryable() && server.is_retryable());
    }

    #[test]
    fn a_refused_connection_is_told_apart_from_a_trust_failure() {
        // Both arrive as transport errors and call for opposite responses, so
        // the chain is walked rather than the message matched.
        let refused = std::io::Error::from(std::io::ErrorKind::ConnectionRefused);
        assert_eq!(classify_transport(&refused), RequestOutcome::Refused);

        #[cfg(not(feature = "fips"))]
        let tls_err = rustls::Error::DecryptError;
        #[cfg(feature = "fips")]
        let tls_err = openssl::error::ErrorStack::get();
        assert_eq!(classify_transport(&tls_err), RequestOutcome::Tls);
    }

    /// Peer TLS material from a throwaway CA, for the config cache tests.
    #[cfg(not(feature = "fips"))]
    fn test_material() -> Arc<PeerTlsMaterial> {
        let ca = certs::generate_ca("cache-ca").expect("a CA");
        let leaf = certs::generate_site_cert(&ca, "local").expect("a leaf");
        Arc::new(PeerTlsMaterial {
            ca: ca.cert_pem.into_bytes(),
            identity: Some(PeerClientIdentity {
                cert: leaf.cert_pem.into_bytes(),
                key: zeroize::Zeroizing::new(leaf.key_pem.into_bytes()),
            }),
        })
    }

    #[test]
    #[cfg(not(feature = "fips"))]
    fn a_peer_transport_is_held_across_rounds_and_rebuilt_when_its_pins_change() {
        let poll = PollPeers {
            tls: Some(test_material()),
            trust: PeerTrustMode::Pin,
            ..PollPeers::default()
        };
        let pins = vec!["a".repeat(64)];
        let first = poll.peer_transport("east", &pins, None).expect("built");
        let again = poll.peer_transport("east", &pins, None).expect("built");
        assert!(
            Arc::ptr_eq(&first, &again),
            "building twice parses the private key and reopens the connection"
        );
        assert_eq!(poll.held_transports(), 1);

        // A rotation replaces the entry rather than adding one, and takes the
        // old pool with it.
        let rotated = vec!["b".repeat(64)];
        let after = poll.peer_transport("east", &rotated, None).expect("built");
        assert!(!Arc::ptr_eq(&first, &after), "rotated pins must not reuse the socket");
        assert_eq!(poll.held_transports(), 1, "keyed by peer, not by pin set");
    }

    #[test]
    #[cfg(not(feature = "fips"))]
    fn a_transport_past_its_age_is_rebuilt_so_the_leaf_is_checked_again() {
        let poll = PollPeers {
            tls: Some(test_material()),
            trust: PeerTrustMode::Pin,
            ..PollPeers::default()
        };
        let pins = vec!["a".repeat(64)];
        let first = poll.peer_transport("east", &pins, None).expect("built");

        // Age the entry past the bound without waiting for it.
        let aged = Instant::now()
            .checked_sub(transport_max_age("", "east") + Duration::from_secs(1))
            .expect("a time before the bound");
        let mut held = poll.transports.0.lock().unwrap_or_else(PoisonError::into_inner);
        held.get_mut("east").expect("an entry").built = aged;
        drop(held);

        let after = poll.peer_transport("east", &pins, None).expect("built");
        assert!(
            !Arc::ptr_eq(&first, &after),
            "an aged transport must be rebuilt, or a pooled connection is never verified again"
        );
        assert_eq!(
            poll.held_transports(),
            1,
            "the rebuild replaces rather than accumulates"
        );
    }

    #[test]
    #[cfg(not(feature = "fips"))]
    fn a_departed_peer_does_not_keep_its_config() {
        let poll = PollPeers {
            tls: Some(test_material()),
            trust: PeerTrustMode::Pin,
            ..PollPeers::default()
        };
        let pins = vec!["a".repeat(64)];
        drop(poll.peer_transport("east", &pins, None).expect("built"));
        drop(poll.peer_transport("west", &pins, None).expect("built"));
        assert_eq!(poll.held_transports(), 2);

        poll.retain_transports(&[Target {
            peer: "east",
            url: String::new(),
            pins: &pins,
            answering: true,
            alias: None,
        }]);
        assert_eq!(poll.held_transports(), 1, "west left the grid and took its material");
    }

    #[test]
    fn outcome_labels_are_stable() {
        // Dashboards and alert rules are written against these strings, so a
        // rename is a break for anyone already watching.
        assert_eq!(RequestOutcome::Ok.as_str(), "ok");
        assert_eq!(RequestOutcome::Timeout.as_str(), "timeout");
        assert_eq!(RequestOutcome::Refused.as_str(), "refused");
        assert_eq!(RequestOutcome::Tls.as_str(), "tls");
        assert_eq!(RequestOutcome::ClientError.as_str(), "client_error");
        assert_eq!(RequestOutcome::ServerError.as_str(), "server_error");
        assert_eq!(RequestOutcome::Throttled.as_str(), "throttled");
    }

    #[test]
    fn transport_ages_spread_within_a_fifth_and_hold_per_peer() {
        let ages: Vec<Duration> = ["east", "west", "north", "south", "hub"]
            .iter()
            .map(|peer| transport_max_age("hub", peer))
            .collect();
        for age in &ages {
            assert!(
                *age >= Duration::from_secs(48) && *age <= Duration::from_secs(72),
                "{age:?}"
            );
        }
        assert!(
            ages.iter().any(|age| Some(age) != ages.first()),
            "peers spread: {ages:?}"
        );
        assert_eq!(
            transport_max_age("hub", "east"),
            transport_max_age("hub", "east"),
            "fixed per peer"
        );
        let toward_east: Vec<Duration> = ["hub", "west", "north", "south"]
            .iter()
            .map(|local| transport_max_age(local, "east"))
            .collect();
        assert!(
            toward_east.iter().any(|age| Some(age) != toward_east.first()),
            "the sites polling one peer spread too: {toward_east:?}"
        );
    }

    #[test]
    fn dial_concurrency_is_the_peers_up_to_the_cap() {
        let poll = PollPeers {
            concurrency: 100,
            ..PollPeers::default()
        };
        assert_eq!(poll.dial_concurrency(3), 3);
        assert_eq!(poll.dial_concurrency(500), MAX_PEER_CONCURRENCY);
        assert_eq!(poll.dial_concurrency(0), 1);
    }

    #[test]
    fn backoff_grows_and_then_stops_growing() {
        let base = Duration::from_millis(50);
        let delays: Vec<_> = (0..8).map(|a| backoff(base, a, "west")).collect();
        for pair in delays.windows(2).take(5) {
            let [first, second] = pair else { continue };
            assert!(second > first, "each attempt waits longer: {first:?} then {second:?}");
        }
        let last = delays.last().copied().unwrap_or_default();
        assert!(
            last <= base * 64,
            "and the growth is capped so a retry cannot outlive a round: {last:?}"
        );
    }

    #[test]
    fn two_peers_failing_together_do_not_retry_together() {
        // Peers cut off together would otherwise retry in step and herd when
        // the partition heals.
        let base = Duration::from_millis(50);
        assert_ne!(
            backoff(base, 2, "east"),
            backoff(base, 2, "west"),
            "the spread is derived from the peer name"
        );
    }

    #[test]
    fn the_same_peer_backs_off_the_same_way_every_run() {
        // Spread, not randomness: a failing run has to be repeatable.
        let base = Duration::from_millis(50);
        assert_eq!(backoff(base, 3, "north"), backoff(base, 3, "north"));
    }

    const QUEUE: &str = "llm_d_epp_average_queue_size";

    fn scraped() -> Vec<Observation> {
        parse(&format!(
            "# HELP {QUEUE} depth\n# TYPE {QUEUE} gauge\n{QUEUE}{{name=\"pool-a\"}} 3\n"
        ))
    }

    #[test]
    fn comments_and_types_are_not_samples() {
        assert_eq!(scraped().len(), 1, "one sample, no comment lines");
    }

    #[test]
    fn a_peer_need_not_label_its_own_output() {
        // Through the real parse path: exposition text with no grid_site, which
        // today is dropped outright, so this is the contract statement that the
        // connection and not the body decides ownership.
        let observations = parse(&format!(r#"{QUEUE}{{name="pool-a"}} 3"#));

        let out = stamp_origin(observations, "east");

        let o = out.first().expect("the sample should survive without a site label");
        assert_eq!(
            o.labels.get(SITE_LABEL).map(String::as_str),
            Some("east"),
            "the dialed peer owns it"
        );
        assert_eq!(
            o.labels.get("name").map(String::as_str),
            Some("pool-a"),
            "unrelated labels survive"
        );
    }

    #[test]
    fn a_peer_serving_another_sites_row_is_refused() {
        // A peer relaying a third site is importing measurements it did not
        // make. Unlike a provider mislabelling its own output, there is nothing
        // here this site may attribute, so the row is refused rather than
        // re-attributed to the peer that served it.
        let observations = parse(&format!(r#"{QUEUE}{{grid_site="west",name="pool-a"}} 3"#));

        let out = stamp_origin(observations, "east");

        assert!(out.is_empty(), "a third site's row must not enter this site's view");
    }

    #[test]
    fn a_provider_cannot_set_the_labels_this_site_attributes() {
        let observations = parse(&format!(r#"{QUEUE}{{grid_site="evil",name="pool-a"}} 3"#));
        let out = attribute(observations, "east", "pool-a");
        let o = out.first().expect("one observation");
        assert_eq!(
            o.labels.get(SITE_LABEL).map(String::as_str),
            Some("east"),
            "this site decides"
        );
        assert_eq!(
            o.labels.get("exported_grid_site").map(String::as_str),
            Some("evil"),
            "the provider's value is kept under an exported name"
        );
        assert_eq!(
            o.labels.get("name").map(String::as_str),
            Some("pool-a"),
            "unrelated labels survive"
        );
    }

    #[test]
    fn a_label_value_containing_a_space_survives_a_round_trip() {
        let out = attribute(parse(r#"queue{path="/a b"} 3"#), "east", "p");
        let mut line = String::new();
        render_sample(&mut line, out.first().expect("one"), 1_700_000_000_000);
        assert!(line.contains(r#"path="/a b""#), "the space is preserved: {line}");
        // The consumer splits the timestamp off the end, then the value, so a
        // space inside a label must not shift either token.
        let (head, timestamp) = line.rsplit_once(' ').expect("timestamp token");
        let (_, value) = head.rsplit_once(' ').expect("value token");
        assert_eq!(timestamp, "1700000000000", "timestamp is the last token: {line}");
        assert_eq!(value, "3", "value is the token before it: {line}");
    }

    #[test]
    fn aggregates_are_not_republished() {
        // Type declared after the samples, which is how a naive exporter emits
        // it and how the parser is forced to treat them as untyped.
        let text = "h_bucket{le=\"1\"} 1\nh_sum 2\nh_count 1\nq 4\n";
        let names: Vec<String> = parse(text).into_iter().map(|o| o.metric).collect();
        assert_eq!(names, vec!["q".to_owned()], "only the plain gauge survives: {names:?}");
    }

    /// Labels a restricted provider demands.
    fn gold() -> BTreeMap<String, String> {
        BTreeMap::from([("tier".to_owned(), "gold".to_owned())])
    }

    /// A store holding one restricted target and one open one.
    fn restricted_store() -> SignalStore {
        let store = SignalStore::new();
        store.refresh(
            BTreeMap::from([
                ("secret-pool".to_owned(), attribute(scraped(), "east", "secret-pool")),
                ("open-pool".to_owned(), attribute(scraped(), "east", "open-pool")),
            ]),
            Duration::from_secs(60),
        );
        store.set_access(BTreeMap::from([("secret-pool".to_owned(), vec![gold()])]));
        store
    }

    #[test]
    fn removing_the_declared_key_refuses_the_peer() {
        let identities = PeerIdentities::new();
        identities.set(BTreeMap::from([(
            "site-a".to_owned(),
            PeerRecord {
                labels: gold(),
                pins: Vec::new(),
                signals: None,
            },
        )]));
        assert_eq!(
            identities.resolve_by_key(&"ab".repeat(32)),
            None,
            "a record declaring no key names nobody, whatever labels it carries"
        );
        assert!(identities.refuses("site-a"), "and it is not polled either");
    }

    #[test]
    fn a_peer_is_named_by_the_key_it_presents() {
        let identities = PeerIdentities::new();
        identities.set(BTreeMap::from([(
            "site-c".to_owned(),
            PeerRecord {
                labels: gold(),
                pins: vec!["ab".repeat(32)],
                signals: None,
            },
        )]));
        assert_eq!(
            identities.resolve_by_key(&"ab".repeat(32)),
            Some(gold()),
            "a declared key carries the labels held for its site"
        );
        assert_eq!(
            identities.resolve_by_key(&"cd".repeat(32)),
            None,
            "a key nobody declared names nobody"
        );
    }

    #[test]
    fn site_b_is_refused_while_site_c_is_served() {
        let identities = PeerIdentities::new();
        identities.set(BTreeMap::from([
            (
                "site-c".to_owned(),
                PeerRecord {
                    labels: gold(),
                    pins: vec!["cc".repeat(32)],
                    signals: None,
                },
            ),
            (
                "site-b".to_owned(),
                PeerRecord {
                    labels: BTreeMap::from([("tier".to_owned(), "silver".to_owned())]),
                    pins: vec!["bb".repeat(32)],
                    signals: None,
                },
            ),
        ]));
        let store = restricted_store();

        let silver = identities.resolve_by_key(&"bb".repeat(32)).expect("site-b is declared");
        let denied = store.render(None, &[], Some(&silver), ClockReading::now()).0;
        assert!(
            !denied.contains("secret-pool"),
            "site-b is denied the restricted pool: {denied}"
        );

        let matching = identities.resolve_by_key(&"cc".repeat(32)).expect("site-c is declared");
        let served = store.render(None, &[], Some(&matching), ClockReading::now()).0;
        assert!(served.contains("secret-pool"), "site-c is served it: {served}");
    }

    #[test]
    fn rotating_a_key_needs_the_record_updated() {
        // Rotating a key means updating the record. Two pins let an overlap be
        // declared before the switch.
        let identities = PeerIdentities::new();
        identities.set(BTreeMap::from([(
            "site-c".to_owned(),
            PeerRecord {
                labels: gold(),
                pins: vec!["ab".repeat(32), "cd".repeat(32)],
                signals: None,
            },
        )]));
        assert_eq!(identities.resolve_by_key(&"ab".repeat(32)), Some(gold()), "the old key");
        assert_eq!(
            identities.resolve_by_key(&"cd".repeat(32)),
            Some(gold()),
            "and the new one"
        );
        assert_eq!(
            identities.resolve_by_key(&"ef".repeat(32)),
            None,
            "an undeclared third is nobody"
        );
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "one scene, read both ways")]
    fn a_refusal_stops_traffic_in_both_directions() {
        let identities = PeerIdentities::new();
        identities.set(BTreeMap::from([
            (
                "site-b".to_owned(),
                PeerRecord {
                    labels: gold(),
                    pins: Vec::new(),
                    signals: None,
                },
            ),
            (
                "site-c".to_owned(),
                PeerRecord {
                    labels: gold(),
                    pins: vec!["cd".repeat(32)],
                    signals: None,
                },
            ),
        ]));

        assert!(identities.refuses("site-b"), "site-b is not polled");
        assert_eq!(
            identities.resolve_by_key(&"ab".repeat(32)),
            None,
            "and no key reaches it"
        );

        assert!(!identities.refuses("site-c"), "site-c is still polled");
        assert_eq!(
            identities.resolve_by_key(&"cd".repeat(32)),
            Some(gold()),
            "and its declared key is served"
        );
    }

    #[test]
    fn a_fingerprint_is_stable_and_distinguishing() {
        assert_eq!(leaf_fingerprint(b"a").len(), 64, "sha256 as lowercase hex");
        assert_eq!(leaf_fingerprint(b"a"), leaf_fingerprint(b"a"), "stable");
        assert_ne!(leaf_fingerprint(b"a"), leaf_fingerprint(b"b"), "distinguishing");
    }

    #[test]
    fn a_non_canonical_declared_fingerprint_authorizes_and_authenticates() {
        // A pin declared with uppercase hex and colons must both authorize (serve
        // side, raw compare) and authenticate (poll side) the same peer.
        let leaf = leaf_fingerprint(b"peer-leaf-der");
        let declared: String = leaf
            .char_indices()
            .flat_map(|(i, c)| {
                let upper = c.to_ascii_uppercase();
                if i > 0 && i.is_multiple_of(2) {
                    vec![':', upper]
                } else {
                    vec![upper]
                }
            })
            .collect();
        assert_ne!(declared, leaf);
        assert_eq!(canonical_fingerprint(&declared), leaf);

        let record = PeerRecord {
            labels: BTreeMap::from([("tier".to_owned(), "gold".to_owned())]),
            pins: vec![canonical_fingerprint(&declared)],
            signals: None,
        };
        let identities = PeerIdentities::new();
        identities.set(BTreeMap::from([("peer".to_owned(), record)]));
        assert!(identities.resolve_by_key(&leaf).is_some());
        assert_eq!(identities.pins_for("peer"), vec![leaf]);
    }

    #[test]
    fn a_denied_target_is_never_rendered() {
        let silver = BTreeMap::from([("tier".to_owned(), "silver".to_owned())]);
        let body = restricted_store()
            .render(None, &[], Some(&silver), ClockReading::now())
            .0;
        assert!(
            !body.contains("secret-pool"),
            "a reader the policy denies is served nothing for that target: {body}"
        );
        assert!(
            body.contains("open-pool"),
            "and still receives what it may read: {body}"
        );
    }

    #[test]
    fn a_matching_reader_is_served_the_restricted_target() {
        let body = restricted_store()
            .render(None, &[], Some(&gold()), ClockReading::now())
            .0;
        assert!(body.contains("secret-pool"), "labels satisfy the selector: {body}");
    }

    #[test]
    fn an_unnamed_reader_is_denied_a_restricted_target() {
        let body = restricted_store().render(None, &[], None, ClockReading::now()).0;
        assert!(
            !body.contains("secret-pool"),
            "identity unknown fails closed rather than open: {body}"
        );
        assert!(
            body.contains("open-pool"),
            "an unrestricted target still serves: {body}"
        );
    }

    #[test]
    fn naming_a_denied_target_does_not_reach_it() {
        let silver = BTreeMap::from([("tier".to_owned(), "silver".to_owned())]);
        let body = restricted_store()
            .render(Some("secret-pool"), &[], Some(&silver), ClockReading::now())
            .0;
        assert_eq!(body, "", "a parameter cannot widen what the connection decided");
    }

    #[test]
    fn two_callers_see_different_scrapes() {
        // Six pools. site-a may read one, site-b may read all. One endpoint,
        // one store, two responses.
        let store = SignalStore::new();
        let mut held = BTreeMap::new();
        let mut access = BTreeMap::new();
        for n in 1..=6 {
            let pool = format!("pool{n}");
            held.insert(pool.clone(), attribute(scraped(), "east", &pool));
            if n > 1 {
                access.insert(pool, vec![gold()]);
            }
        }
        store.refresh(held, Duration::from_secs(60));
        store.set_access(access);

        let silver = BTreeMap::from([("tier".to_owned(), "silver".to_owned())]);
        let restricted = store.render(None, &[], Some(&silver), ClockReading::now()).0;
        let full = store.render(None, &[], Some(&gold()), ClockReading::now()).0;

        assert!(
            restricted.contains("pool1"),
            "site-a reads the one it may: {restricted}"
        );
        for n in 2..=6 {
            assert!(
                !restricted.contains(&format!("pool{n}")),
                "and none of the other five: {restricted}"
            );
        }
        for n in 1..=6 {
            assert!(full.contains(&format!("pool{n}")), "site-b reads all six: {full}");
        }
        assert_eq!(restricted.lines().filter(|l| !l.is_empty()).count(), 1);
        assert_eq!(full.lines().filter(|l| !l.is_empty()).count(), 6);
    }

    #[test]
    fn collect_cannot_reach_a_denied_target() {
        // `collect[]` is chosen by the caller, so it narrows within what the
        // caller may see and can never name past the policy.
        let silver = BTreeMap::from([("tier".to_owned(), "silver".to_owned())]);
        let body = restricted_store()
            .render(None, &[QUEUE.to_owned()], Some(&silver), ClockReading::now())
            .0;
        assert!(
            !body.contains("secret-pool"),
            "asking for the metric by name does not reach a denied provider: {body}"
        );
        assert!(
            body.contains("open-pool"),
            "and the same request still returns what the caller may read: {body}"
        );
    }

    #[test]
    fn a_denied_provider_leaves_no_trace_in_the_response() {
        // Absent, not zeroed and not redacted. A reader cannot tell a denied
        // provider from one that does not exist or is not reporting.
        let silver = BTreeMap::from([("tier".to_owned(), "silver".to_owned())]);
        let denied = restricted_store()
            .render(None, &[], Some(&silver), ClockReading::now())
            .0;
        let served = restricted_store()
            .render(None, &[], Some(&gold()), ClockReading::now())
            .0;
        let lines = |body: &str| body.lines().filter(|l| !l.is_empty()).count();
        assert_eq!(
            lines(&denied),
            lines(&served) - 1,
            "exactly the denied provider's series are missing, nothing else changes"
        );
        assert!(!denied.contains("secret-pool"), "no label, no name, no line: {denied}");
    }

    #[test]
    fn every_selector_has_to_be_satisfied() {
        let store = SignalStore::new();
        store.refresh(
            BTreeMap::from([("pool-a".to_owned(), attribute(scraped(), "east", "pool-a"))]),
            Duration::from_secs(60),
        );
        // Routing allows the reader, a metrics-specific selector does not.
        store.set_access(BTreeMap::from([(
            "pool-a".to_owned(),
            vec![gold(), BTreeMap::from([("audit".to_owned(), "yes".to_owned())])],
        )]));
        assert_eq!(
            store.render(None, &[], Some(&gold()), ClockReading::now()).0,
            "",
            "satisfying one selector is not satisfying both"
        );
    }

    #[test]
    fn target_selects_one_provider() {
        let store = SignalStore::new();
        store.refresh(
            BTreeMap::from([
                ("pool-a".to_owned(), attribute(scraped(), "east", "pool-a")),
                ("pool-b".to_owned(), attribute(scraped(), "east", "pool-b")),
            ]),
            Duration::from_secs(60),
        );
        let (one, _) = store.render(Some("pool-a"), &[], None, ClockReading::now());
        assert!(one.contains(r#"grid_provider="pool-a""#), "the asked-for target: {one}");
        assert!(!one.contains(r#"grid_provider="pool-b""#), "and only that one: {one}");
        assert_eq!(
            store.render(None, &[], None, ClockReading::now()).0.lines().count(),
            2,
            "no target returns every target"
        );
    }

    #[test]
    fn collect_selects_signals_by_name() {
        let store = SignalStore::new();
        store.refresh(
            BTreeMap::from([("pool-a".to_owned(), attribute(scraped(), "east", "pool-a"))]),
            Duration::from_secs(60),
        );
        assert_eq!(
            store
                .render(None, &[QUEUE.to_owned()], None, ClockReading::now())
                .0
                .lines()
                .count(),
            1,
            "the named signal"
        );
        assert_eq!(
            store
                .render(None, &["absent".to_owned()], None, ClockReading::now())
                .0
                .lines()
                .count(),
            0,
            "and nothing else"
        );
    }

    #[test]
    fn age_reports_the_oldest_value_in_the_response() {
        let store = SignalStore::new();
        store.refresh(
            BTreeMap::from([("pool-a".to_owned(), scraped())]),
            Duration::from_secs(60),
        );
        let (_, age) = store.render(None, &[], None, ClockReading::now());
        assert!(
            age < Duration::from_secs(1),
            "a value just collected is reported as new: {age:?}"
        );
    }

    #[test]
    fn a_target_nothing_refreshes_stops_being_served() {
        let store = SignalStore::new();
        store.refresh(BTreeMap::from([("pool-a".to_owned(), scraped())]), Duration::ZERO);
        assert_eq!(
            store.render(None, &[], None, ClockReading::now()).0,
            "",
            "absence is what says it is stale"
        );
    }

    #[test]
    fn a_target_absent_from_a_refresh_is_kept_until_it_expires() {
        let store = SignalStore::new();
        store.refresh(
            BTreeMap::from([("pool-a".to_owned(), scraped())]),
            Duration::from_secs(60),
        );
        store.refresh(
            BTreeMap::from([("pool-b".to_owned(), scraped())]),
            Duration::from_secs(60),
        );
        assert_eq!(store.targets().len(), 2, "one failed scrape must not erase a target");
    }
    #[test]
    fn non_finite_values_never_reach_the_store() {
        let scraped = "\
q{pool=\"a\"} NaN
q{pool=\"b\"} +Inf
q{pool=\"c\"} -Inf
q{pool=\"d\"} 0.35
";
        let kept = parse(scraped);
        assert_eq!(kept.len(), 1, "only the finite sample survives: {kept:?}");
        let kept_value = kept.first().map(|o| o.value).expect("one sample kept");
        assert!((kept_value - 0.35).abs() < f64::EPSILON);
        assert!(
            kept.iter().all(|o| o.value.is_finite()),
            "a non-finite score outranks every finite one at the consumer"
        );
    }

    /// Every cache reading the gateway can read crosses the relay, and is bounded as a share.
    ///
    /// The two lists are easy to update apart: a name added to one and not the other either
    /// never reaches a peer, or reaches it as a percentage that the gateway then refuses. Both
    /// drop the reading silently, so the gateway routes a remote site from whatever is left.
    #[test]
    fn every_cache_reading_the_gateway_takes_is_relayed_and_bounded() {
        const GATEWAY: &str = include_str!("../../gateway/crates/ai-grid-filters/src/signals/llm_d.rs");

        let names: Vec<&str> = GATEWAY
            .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
            .filter(|part| part.contains("kv_cache_utilization"))
            .collect();
        assert!(
            names.len() >= 3,
            "found only {names:?}, the scan is not reading the gateway's series"
        );
        for name in names {
            assert!(
                PEER_SIGNAL_NAMES.contains(&name),
                "{name} is read by the gateway but never relayed to a peer"
            );
            assert!(
                UNIT_SIGNALS.contains(&name),
                "{name} is a share the gateway refuses above one, but a hub accepts any value"
            );
        }
    }

    #[test]
    fn llm_d_kv_cache_spread_is_relayed_as_a_bounded_share() {
        let spread = peer_sample("llm_d_epp_std_dev_kv_cache_utilization", "pool-b", 0.71);
        assert_eq!(
            refusal(&spread),
            None,
            "the pinned EPP spread series must be accepted and relayed to remote gateways"
        );

        let invalid_spread = peer_sample("llm_d_epp_std_dev_kv_cache_utilization", "pool-b", 1.01);
        assert_eq!(
            refusal(&invalid_spread),
            Some("value"),
            "the KV-cache spread is a normalized share and must stay within one"
        );
    }
}
