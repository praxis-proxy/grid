//! Whether each provider can serve a request now, resolved from its scraped metrics.
//!
//! The signals loop records each scrape here. The verdict feeds three readers: the
//! provider's `Ready` condition, the `grid_provider_ready` series peers poll, and
//! admission in the serving config.

use std::{
    collections::HashMap,
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};

use crate::{crd::inference_provider::Condition, signals::Observation};

/// Metric names that count a pool's ready endpoints, preferred first.
pub(crate) const DEFAULT_READY_ENDPOINTS: [&str; 2] = ["llm_d_epp_ready_endpoints", "inference_pool_ready_pods"];

/// The EPP's per-unit queue series, one per serving unit. They come from a collector that
/// stops reporting when the pool has no units, while the pool gauges freeze at their last
/// values, so their absence tells a drained pool from a live one.
pub(crate) const PER_UNIT_QUEUE: [&str; 2] = ["inference_pool_per_pod_queue_size", "llm_d_epp_per_endpoint_queue_size"];

/// Consecutive scrapes reading zero ready endpoints before a provider is not ready,
/// and reading some before it is ready again.
///
/// One reading can catch a pod between its metrics going stale and a fresh one, and a
/// pool that just drained can read ready once before its first request lands.
pub(crate) const STREAK: u32 = 2;

/// How recently the engine must have answered for a zero count to read as busy, not down.
///
/// The EPP counts endpoints whose metrics are fresh, so a saturated engine whose /metrics
/// answers late reads as zero while it still serves.
pub(crate) const PROGRESS_WINDOW: Duration = Duration::from_secs(30);


/// The series this site publishes per provider: 1 when ready, 0 when not.
pub const READY_SIGNAL: &str = "grid_provider_ready";

/// The resolved signal for requests a provider has running and queued.
pub const IN_FLIGHT_SIGNAL: &str = "grid_provider_in_flight_requests";

/// The endpoints behind the provider that answered with fresh metrics.
///
/// Published so the in-flight count has a denominator at the reader: without it a
/// 16-endpoint site holding 40 requests sorts as more loaded than a 2-endpoint site
/// holding 10.
pub const READY_ENDPOINTS_SIGNAL: &str = "grid_provider_ready_endpoints";

/// The EPP's per-endpoint in-flight count, from its inflight-load-producer plugin. It
/// increments after flow control admits a request, once per scheduling profile's target, so
/// on a P/D pool a request counts on its prefill and its decode endpoint.
const EPP_IN_FLIGHT: &str = "llm_d_epp_inflight_requests";

/// Requests the EPP's flow control holds before scheduling, labeled by `inference_pool`.
const EPP_FLOW_CONTROL_QUEUE: &str = "llm_d_epp_flow_control_queue_size";

/// Pool averages that stand in when the EPP runs no inflight-load-producer.
const EPP_AVERAGES: [[&str; 2]; 2] = [
    [
        "llm_d_epp_average_running_requests",
        "inference_pool_average_running_requests",
    ],
    ["llm_d_epp_average_queue_size", "inference_pool_average_queue_size"],
];

/// The `Ready` condition's status and reason for one provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    /// Scraped recently with at least one ready endpoint.
    Ready,
    /// The latest scrapes counted zero ready endpoints.
    NoEndpointsReady,
    /// The scrape answered, but without the pool's ready-endpoint series, so
    /// readiness is unknown rather than false: a documented vLLM provider
    /// scrapes its own /metrics, which carries no such series.
    NoLivenessCheck,
    /// No successful scrape within the staleness window, and no attempt failed.
    MetricsStale,
    /// No successful scrape within the window, and the latest attempt timed out.
    ScrapeTimedOut,
    /// No successful scrape within the window, and the latest attempt was refused (401 or 403).
    ScrapeUnauthorized,
    /// No successful scrape within the window, and the latest attempt failed TLS.
    TlsHandshakeFailed,
    /// No successful scrape within the window, and the latest attempt failed otherwise.
    ScrapeFailed,
    /// The provider itself is `Unavailable`.
    ProviderUnavailable,
    /// The provider declares no metrics, so readiness is unknown.
    MetricsNotConfigured,
    /// Attempted, with no scrape succeeding yet inside the grace window.
    AwaitingFirstScrape,
}

impl Reason {
    /// The condition reason as written to status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "Ready",
            Self::NoEndpointsReady => "NoEndpointsReady",
            Self::NoLivenessCheck => "NoLivenessCheck",
            Self::MetricsStale => "MetricsStale",
            Self::ScrapeTimedOut => "ScrapeTimedOut",
            Self::ScrapeUnauthorized => "ScrapeUnauthorized",
            Self::TlsHandshakeFailed => "TLSHandshakeFailed",
            Self::ScrapeFailed => "ScrapeFailed",
            Self::ProviderUnavailable => "ProviderUnavailable",
            Self::MetricsNotConfigured => "MetricsNotConfigured",
            Self::AwaitingFirstScrape => "AwaitingFirstScrape",
        }
    }

    /// The condition status: `True`, `False`, or `Unknown`.
    #[must_use]
    pub const fn status(self) -> &'static str {
        match self {
            Self::Ready => "True",
            Self::MetricsNotConfigured | Self::AwaitingFirstScrape | Self::NoLivenessCheck => "Unknown",
            Self::NoEndpointsReady
            | Self::MetricsStale
            | Self::ScrapeTimedOut
            | Self::ScrapeUnauthorized
            | Self::TlsHandshakeFailed
            | Self::ScrapeFailed
            | Self::ProviderUnavailable => "False",
        }
    }

    /// The one-word status shown in the STATUS column: `Ready`, `NotReady`, or `Unknown`.
    #[must_use]
    pub const fn display(self) -> &'static str {
        match self {
            Self::Ready => "Ready",
            Self::MetricsNotConfigured | Self::AwaitingFirstScrape | Self::NoLivenessCheck => "Unknown",
            Self::NoEndpointsReady
            | Self::MetricsStale
            | Self::ScrapeTimedOut
            | Self::ScrapeUnauthorized
            | Self::TlsHandshakeFailed
            | Self::ScrapeFailed
            | Self::ProviderUnavailable => "NotReady",
        }
    }

    /// Whether the provider is known not to serve: unknown counts as ready.
    #[must_use]
    pub const fn excludes(self) -> bool {
        !matches!(
            self,
            Self::Ready | Self::MetricsNotConfigured | Self::AwaitingFirstScrape | Self::NoLivenessCheck
        )
    }
}

/// Why one scrape failed, bounded so it can label a metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScrapeClass {
    /// The request timed out.
    Timeout,
    /// The endpoint answered 401 or 403.
    Unauthorized,
    /// TLS failed: a handshake, a certificate, or the TLS material.
    Tls,
    /// The endpoint's name did not resolve.
    Dns,
    /// The connection failed.
    Connect,
    /// The endpoint answered with another non-2xx status.
    Http,
    /// The body passed the size limit.
    BodyCap,
    /// The body could not be read as text.
    Parse,
    /// The scrape could not be built: the URL, the credential, or plaintext refused.
    Config,
}

impl ScrapeClass {
    /// The class as a metric label and message word.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Unauthorized => "unauthorized",
            Self::Tls => "tls",
            Self::Dns => "dns",
            Self::Connect => "connect",
            Self::Http => "http",
            Self::BodyCap => "body_cap",
            Self::Parse => "parse",
            Self::Config => "config",
        }
    }

    /// The `Ready` reason when this was the latest failure and nothing succeeded since.
    const fn reason(self) -> Reason {
        match self {
            Self::Timeout => Reason::ScrapeTimedOut,
            Self::Unauthorized => Reason::ScrapeUnauthorized,
            Self::Tls => Reason::TlsHandshakeFailed,
            Self::Dns | Self::Connect | Self::Http | Self::BodyCap | Self::Parse | Self::Config => Reason::ScrapeFailed,
        }
    }
}

/// What the signals loop last saw from one provider.
#[derive(Clone, Debug, Default)]
struct Probe {
    /// When the first scrape was attempted, which starts the grace before any success.
    first_attempt: Option<Instant>,
    /// When the last scrape succeeded.
    last_good: Option<Instant>,
    /// Ready endpoints in that scrape, `None` when it exposed no count.
    ready_endpoints: Option<f64>,
    /// Signals from the latest success, until the signals loop publishes them once.
    unpublished: Option<Vec<Observation>>,
    /// The latest attempt's failure, `None` when it succeeded.
    failure: Option<ScrapeClass>,
    /// What the latest success lacked, when it exposed no ready-endpoint count.
    missing: Option<String>,
    /// Consecutive successful scrapes that read zero ready endpoints.
    zero_streak: u32,
    /// Consecutive successful scrapes that read some ready endpoints.
    ready_streak: u32,
    /// Whether the count marks the provider not ready, with [`STREAK`] hysteresis both ways.
    no_endpoints: bool,
    /// When the EPP last recorded an engine answer for this provider.
    last_progress: Option<Instant>,
    /// Whether the latest zero count came while the engine was answering.
    busy: bool,
    /// When a scrape last carried a per-unit series, so their absence means something.
    units_last_seen: Option<Instant>,
    /// Recent EPP latency snapshots.
    latency: crate::latency::History,
}

impl Probe {
    /// Fold one scrape reading zero ready endpoints into this probe.
    fn count_a_zero(&mut self, now: Instant) {
        self.zero_streak = self.zero_streak.saturating_add(1);
        self.ready_streak = 0;
        self.busy = self
            .last_progress
            .is_some_and(|at| now.saturating_duration_since(at) <= PROGRESS_WINDOW);
        // Answering releases the verdict as well as holding it off. Recovery that required
        // the endpoint count to come back could not fire when the count was what broke:
        // exclusion stops the traffic, and a verdict that needs traffic to clear would hold
        // for as long as the metrics stayed down.
        self.no_endpoints = (self.no_endpoints || self.zero_streak >= STREAK) && !self.busy;
        if self.no_endpoints {
            // Down is the end of the drain story, so recovery reasons from the series it
            // sees next rather than from one a previous pool reported.
            self.units_last_seen = None;
        }
    }
}

/// One provider's verdict and the detail behind it.
#[derive(Clone, Debug, PartialEq)]
pub struct Verdict {
    /// Status and reason.
    pub reason: Reason,
    /// Human detail for the condition message.
    pub message: String,
}

/// The latest scrape per provider, keyed by network and provider name.
#[derive(Debug, Default)]
pub struct ReadinessStore(Mutex<HashMap<String, Probe>>);

/// The store key for the provider named `provider` in `network`: names are unique per
/// network only. Keyed by provider, not routing identity, so two providers sharing a
/// cluster keep their own history and one reporting zero endpoints is not cleared by the
/// other.
pub(crate) fn key(network: &str, provider: &str) -> String {
    format!("{network}/{provider}")
}

/// The provider a store key names, for a metric label.
///
/// One derivation, used by both the scrape counters and the cleanup that drops them, so the
/// label a series is recorded under cannot differ from the one it is forgotten under.
pub(crate) fn provider_of(key: &str) -> &str {
    key.split_once('/').map_or(key, |(_, provider)| provider)
}

impl ReadinessStore {
    /// The probes, recovered if a panicking holder poisoned the lock: each write is whole.
    fn probes(&self) -> std::sync::MutexGuard<'_, HashMap<String, Probe>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The ready count a scrape really carries: a positive gauge in a scrape with no
    /// per-unit series, where a recent scrape had them, is frozen at its last value because
    /// the pool drained, and reads as zero.
    ///
    /// Two bounds, because the inference is narrow: a series that was there and is gone means
    /// the pool drained, which only holds while the absence is news and while nothing says
    /// otherwise.
    ///
    /// Recent, not ever: a per-unit series dropped for good, by a renamed label or a plugin no
    /// longer configured, would otherwise make every later positive count read zero.
    ///
    /// And not while the engine is answering: an answer means pods exist, so the count stands
    /// whatever the per-unit collector is doing. Without that, an idle provider whose series
    /// were renamed reads zero, exclusion stops the traffic that would prove otherwise, and
    /// nothing can clear it.
    pub(crate) fn thawed(&self, key: &str, ready: Option<f64>, units_reporting: bool, now: Instant) -> Option<f64> {
        let mut probes = self.probes();
        let probe = probes.entry(key.to_owned()).or_default();
        if units_reporting {
            probe.units_last_seen = Some(now);
        }
        let recently = |at: Instant| now.saturating_duration_since(at) <= PROGRESS_WINDOW;
        let units_seen = probe.units_last_seen.is_some_and(&recently);
        let answering = probe.last_progress.is_some_and(recently);
        drop(probes);
        if !units_reporting && units_seen && !answering && ready.is_some_and(|count| count >= 1.0) {
            return Some(0.0);
        }
        ready
    }

    /// Record a scrape's latency counters and return the latency series it publishes.
    #[expect(clippy::significant_drop_tightening, reason = "the guard covers one whole update")]
    pub(crate) fn record_latency(&self, key: &str, observations: &[Observation], now: Instant) -> Vec<Observation> {
        // Built before locking, so the store is held only to append it.
        let snapshot = crate::latency::Snapshot::of(observations);
        // The counters carry no pool label, so an EPP serving several pools proves nothing.
        let attributable = !serves_several_pools(observations);
        let mut probes = self.probes();
        let probe = probes.entry(key.to_owned()).or_default();
        // Progress is only as recent as the scrape it is measured from: a delta against a
        // baseline older than the window says nothing about the last 30 s.
        if attributable
            && probe.latency.latest().is_some_and(|(at, earlier)| {
                now.saturating_duration_since(at) <= PROGRESS_WINDOW && snapshot.produced_since(earlier)
            })
        {
            probe.last_progress = Some(now);
        }
        let published = probe.latency.record(snapshot, now);
        // History is kept either way, so a pool that stops sharing its EPP has a baseline
        // to resume from. Publication is not: these counters carry no pool label, and a
        // figure summed across every pool an EPP serves, attributed to one provider, would
        // have the gateway route on another pool's latency. Absent reads as unknown, which
        // is the honest answer.
        if attributable { published } else { Vec::new() }
    }

    /// Record a successful scrape at `now`, with `missing` naming the series it lacked.
    #[expect(clippy::significant_drop_tightening, reason = "the guard covers one whole update")]
    #[expect(clippy::too_many_arguments, reason = "one scrape's count, gap, signals, and time")]
    pub(crate) fn record_success(
        &self,
        key: &str,
        ready_endpoints: Option<f64>,
        missing: Option<String>,
        observations: Vec<Observation>,
        now: Instant,
    ) {
        let mut probes = self.probes();
        let probe = probes.entry(key.to_owned()).or_default();
        probe.first_attempt.get_or_insert(now);
        match ready_endpoints {
            Some(count) if count < 1.0 => probe.count_a_zero(now),
            Some(_) => {
                probe.ready_streak = probe.ready_streak.saturating_add(1);
                probe.zero_streak = 0;
                probe.busy = false;
                probe.no_endpoints &= probe.ready_streak < STREAK;
            },
            // No count to read: reachable is all that is known.
            None => {
                probe.zero_streak = 0;
                probe.ready_streak = 0;
                probe.busy = false;
                probe.no_endpoints = false;
            },
        }
        probe.last_good = Some(now);
        probe.ready_endpoints = ready_endpoints;
        probe.unpublished = Some(observations);
        probe.failure = None;
        probe.missing = ready_endpoints.is_none().then_some(missing).flatten();
    }

    /// Record a failed scrape at `now`, keeping what the last good one saw.
    #[expect(clippy::significant_drop_tightening, reason = "the guard covers one whole update")]
    pub(crate) fn record_failure(&self, key: &str, class: ScrapeClass, now: Instant) {
        let mut probes = self.probes();
        let probe = probes.entry(key.to_owned()).or_default();
        probe.first_attempt.get_or_insert(now);
        probe.failure = Some(class);
    }

    /// The verdict for `key` at `now`.
    ///
    /// `AwaitingFirstScrape`, which is `Unknown` and publishes nothing, until the
    /// provider has been attempted and through the first `stale_after` while no
    /// scrape has succeeded yet, so a starting operator states that it is waiting.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the guard covers one small evaluation"
    )]
    pub(crate) fn verdict(&self, key: &str, unavailable: bool, stale_after: Duration, now: Instant) -> Option<Verdict> {
        let awaiting = Verdict {
            reason: Reason::AwaitingFirstScrape,
            message: "awaiting the first metrics scrape".to_owned(),
        };
        let probes = self.probes();
        let Some(probe) = probes.get(key) else {
            return (!unavailable).then_some(awaiting);
        };
        let within = |at: Instant| now.saturating_duration_since(at) <= stale_after;
        if probe.last_good.is_none() && !unavailable && probe.first_attempt.is_some_and(within) {
            return Some(awaiting);
        }
        Some(evaluate(
            probe,
            probe.last_good.is_some_and(within),
            unavailable,
            stale_after,
        ))
    }

    /// The signals of the latest successful scrape, once: stale load is never republished.
    pub(crate) fn take_fresh(&self, key: &str) -> Option<Vec<Observation>> {
        self.probes().get_mut(key)?.unpublished.take()
    }

    /// Ready endpoints in the provider's last successful scrape, `None` when it exposed no count.
    pub(crate) fn ready_endpoints(&self, key: &str) -> Option<f64> {
        self.probes().get(key)?.ready_endpoints
    }

    /// Forget providers not in `keep`.
    pub(crate) fn retain(&self, keep: &std::collections::BTreeSet<String>) {
        self.probes().retain(|key, _| {
            let kept = keep.contains(key);
            if !kept {
                crate::metrics::forget_provider_scrapes(provider_of(key));
            }
            kept
        });
    }
}

/// The verdict for one probe. `fresh` says whether its last success is within `stale_after`.
fn evaluate(probe: &Probe, fresh: bool, unavailable: bool, stale_after: Duration) -> Verdict {
    if unavailable {
        return Verdict {
            reason: Reason::ProviderUnavailable,
            message: "provider is Unavailable".to_owned(),
        };
    }
    if !fresh {
        let window = stale_after.as_secs();
        return match probe.failure {
            Some(class) => Verdict {
                reason: class.reason(),
                message: format!(
                    "no successful metrics scrape in the last {window}s; the latest failed: {}",
                    class.as_str()
                ),
            },
            None => Verdict {
                reason: Reason::MetricsStale,
                message: format!("no successful metrics scrape in the last {window}s"),
            },
        };
    }
    from_ready_endpoints(probe)
}

/// The verdict for a freshly scraped provider, from its ready-endpoint count.
fn from_ready_endpoints(probe: &Probe) -> Verdict {
    let count = probe.ready_endpoints;
    if probe.no_endpoints {
        let message = match count {
            Some(count) if count >= 1.0 => format!("{count} ready endpoints, awaiting a second scrape"),
            _ => format!("0 ready endpoints for {STREAK} or more scrapes"),
        };
        return Verdict {
            reason: Reason::NoEndpointsReady,
            message,
        };
    }
    let Some(count) = count else {
        return Verdict {
            reason: Reason::NoLivenessCheck,
            message: probe
                .missing
                .clone()
                .unwrap_or_else(|| "metrics reachable, but no ready-endpoint series".to_owned()),
        };
    };
    Verdict {
        reason: Reason::Ready,
        message: ready_message(count, probe.busy),
    }
}

/// The detail for a ready provider counting `count` endpoints.
fn ready_message(count: f64, busy: bool) -> String {
    if count >= 1.0 {
        format!("{count} ready endpoints")
    } else if busy {
        format!(
            "0 ready endpoints, but the engine answered in the last {}s",
            PROGRESS_WINDOW.as_secs()
        )
    } else {
        "0 ready endpoints in the latest scrape only".to_owned()
    }
}

/// The condition type this module owns.
pub const READY_CONDITION: &str = "Ready";

/// The `Ready` condition to write, or `None` when `current` already says the same.
///
/// A changed message alone is not written, so a moving endpoint count does not
/// churn status. A new generation is. `lastTransitionTime` moves only when the status does.
pub(crate) fn ready_condition(
    current: &[Condition],
    verdict: &Verdict,
    now_rfc3339: &str,
    generation: Option<i64>,
) -> Option<Condition> {
    let held = current.iter().find(|c| c.type_ == READY_CONDITION);
    let status = verdict.reason.status();
    if held.is_some_and(|c| {
        c.status == status && c.reason == verdict.reason.as_str() && c.observed_generation == generation
    }) {
        return None;
    }
    let last_transition_time = held
        .filter(|c| c.status == status)
        .map_or_else(|| now_rfc3339.to_owned(), |c| c.last_transition_time.clone());
    Some(Condition {
        type_: READY_CONDITION.to_owned(),
        status: status.to_owned(),
        reason: verdict.reason.as_str().to_owned(),
        message: verdict.message.clone(),
        last_transition_time,
        observed_generation: generation,
    })
}

/// Ready endpoints for `pool` in `observations`, from the first of `names` present.
///
/// With a pool, only series whose `name` label is that pool count, so an EPP that
/// serves several pools is read for the configured one.
pub(crate) fn ready_endpoints(observations: &[Observation], names: &[&str], pool: Option<&str>) -> Option<f64> {
    names.iter().find_map(|name| {
        observations
            .iter()
            .filter(|o| o.metric == *name)
            .filter(|o| pool.is_none_or(|pool| o.labels.get("name").is_some_and(|n| n == pool)))
            .map(|o| o.value)
            .reduce(f64::max)
    })
}

/// Where a provider's in-flight count came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InFlightSource {
    /// The EPP's inflight-load-producer, at or above the pool averages.
    Epp,
    /// The pool averages of running and queued times ready endpoints, above the EPP count or
    /// in place of it.
    EngineAverages,
}

/// Requests the provider holds: the larger of the EPP's per-endpoint count summed and its
/// pool averages of running and queued times `ready`, plus requests flow control holds.
///
/// The larger of the two, so an EPP restart that zeroes its count does not read as idle. The
/// per-endpoint count carries no pool label, so it is skipped when the EPP serves several
/// pools. `None` when neither source is present, or the site has no fresh endpoint behind
/// frozen averages.
pub(crate) fn in_flight(
    observations: &[Observation],
    ready: Option<f64>,
    pool: Option<&str>,
) -> Option<(f64, InFlightSource)> {
    let counted = (!serves_several_pools(observations))
        .then(|| epp_count(observations))
        .flatten();
    // No fresh endpoint means the averages are frozen at their last value: unknown, not idle.
    let averaged = ready.filter(|ready| *ready > 0.0).and_then(|ready| {
        EPP_AVERAGES
            .iter()
            .map(|names| ready_endpoints(observations, names, pool))
            .sum::<Option<f64>>()
            .map(|per_endpoint| per_endpoint * ready)
    });
    let (held, source) = match (counted, averaged) {
        (Some(counted), Some(averaged)) if averaged > counted => (averaged, InFlightSource::EngineAverages),
        (Some(counted), _) => (counted, InFlightSource::Epp),
        (None, Some(averaged)) => (averaged, InFlightSource::EngineAverages),
        (None, None) => return None,
    };
    Some((held + flow_control_queued(observations, pool), source))
}

/// The EPP's in-flight count summed over endpoints, each endpoint's largest across producer
/// instances so two producers do not double it. `None` when the EPP exports none.
fn epp_count(observations: &[Observation]) -> Option<f64> {
    let mut producers: std::collections::BTreeMap<&str, std::collections::BTreeMap<(&str, &str), f64>> =
        std::collections::BTreeMap::new();
    for observation in observations.iter().filter(|o| o.metric == EPP_IN_FLIGHT) {
        let label = |name: &str| observation.labels.get(name).map_or("", String::as_str);
        // Series per fairness and priority add up; producer instances repeat the same requests.
        *producers
            .entry(label("producer_name"))
            .or_default()
            .entry((label("namespace"), label("endpoint_name")))
            .or_insert(0.0) += observation.value;
    }
    let mut peak: std::collections::BTreeMap<(&str, &str), f64> = std::collections::BTreeMap::new();
    for counts in producers.values() {
        for (endpoint, count) in counts {
            let held = peak.entry(*endpoint).or_insert(0.0);
            *held = held.max(*count);
        }
    }
    (!peak.is_empty()).then(|| peak.values().sum())
}

/// Whether the EPP reports more than one pool or namespace, so its unlabeled per-endpoint
/// count cannot be attributed to one pool.
fn serves_several_pools(observations: &[Observation]) -> bool {
    let distinct = |metrics: &[&str], label: &str| {
        observations
            .iter()
            .filter(|o| metrics.contains(&o.metric.as_str()))
            .filter_map(|o| o.labels.get(label))
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    };
    distinct(&DEFAULT_READY_ENDPOINTS, "name") > 1 || distinct(&[EPP_IN_FLIGHT], "namespace") > 1
}

/// Requests the EPP's flow control holds for `pool`, 0 when it exports none.
fn flow_control_queued(observations: &[Observation], pool: Option<&str>) -> f64 {
    observations
        .iter()
        .filter(|o| o.metric == EPP_FLOW_CONTROL_QUEUE)
        .filter(|o| crate::signals::in_pool(o, pool))
        .map(|o| o.value)
        .sum()
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::shadow_unrelated,
    reason = "tests"
)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    const STALE: Duration = Duration::from_secs(15);

    fn sample(metric: &str, pool: &str, value: f64) -> Observation {
        Observation {
            metric: metric.to_owned(),
            labels: BTreeMap::from([("name".to_owned(), pool.to_owned())]),
            value,
            timestamp_ms: None,
        }
    }

    #[test]
    fn ready_endpoints_prefers_the_current_name_and_reads_the_configured_pool() {
        let observations = [
            sample("inference_pool_ready_pods", "qwen3", 4.0),
            sample("llm_d_epp_ready_endpoints", "other", 9.0),
            sample("llm_d_epp_ready_endpoints", "qwen3", 2.0),
        ];
        assert_eq!(
            ready_endpoints(&observations, &DEFAULT_READY_ENDPOINTS, Some("qwen3")),
            Some(2.0)
        );
        assert_eq!(
            ready_endpoints(&observations[..1], &DEFAULT_READY_ENDPOINTS, Some("qwen3")),
            Some(4.0),
            "falls back to the deprecated name"
        );
        assert_eq!(
            ready_endpoints(&observations, &DEFAULT_READY_ENDPOINTS, Some("absent")),
            None
        );
    }

    fn reason(store: &ReadinessStore, now: Instant) -> Option<Reason> {
        store.verdict("p", false, STALE, now).map(|verdict| verdict.reason)
    }

    #[test]
    fn verdict_follows_the_latest_scrape_and_staleness() {
        let store = ReadinessStore::default();
        let start = Instant::now();
        assert_eq!(
            reason(&store, start),
            Some(Reason::AwaitingFirstScrape),
            "never attempted"
        );

        store.record_success("p", Some(2.0), None, vec![sample("q", "p", 1.0)], start);
        assert_eq!(reason(&store, start), Some(Reason::Ready));
        assert_eq!(store.take_fresh("p").map(|held| held.len()), Some(1));
        assert_eq!(store.take_fresh("p"), None, "fresh signals are published once");

        let missing = "metrics reachable, but no llm_d_epp_ready_endpoints series for poolName qwen3";
        store.record_success("p", None, Some(missing.to_owned()), Vec::new(), start);
        let verdict = store.verdict("p", false, STALE, start).expect("judged");
        assert_eq!(
            (verdict.reason, verdict.message.as_str()),
            (Reason::NoLivenessCheck, missing),
            "a 200 without the pool's series leaves readiness unknown, and says which series"
        );
        assert!(
            !verdict.reason.excludes(),
            "unknown readiness does not exclude: a vLLM provider carries no such series"
        );
        assert_eq!(store.take_fresh("p").map(|held| held.len()), Some(0));

        store.record_failure("p", ScrapeClass::Connect, start + STALE);
        assert_eq!(
            reason(&store, start + STALE),
            Some(Reason::NoLivenessCheck),
            "a failure within the window keeps the last verdict"
        );
        assert_eq!(store.take_fresh("p"), None, "a failure publishes no held load");
        let stale = store
            .verdict("p", false, STALE, start + STALE + Duration::from_secs(1))
            .expect("judged");
        assert_eq!(stale.reason, Reason::ScrapeFailed);
        assert!(
            stale.message.ends_with("the latest failed: connect"),
            "{}",
            stale.message
        );
    }

    #[test]
    fn zero_endpoints_take_two_scrapes_to_exclude_and_two_to_readmit() {
        let store = ReadinessStore::default();
        let now = Instant::now();
        let scrape = |count| {
            store.record_success("p", Some(count), None, Vec::new(), now);
            reason(&store, now)
        };
        assert_eq!(scrape(0.0), Some(Reason::Ready), "one zero reading is not enough");
        assert_eq!(scrape(0.0), Some(Reason::NoEndpointsReady));
        assert_eq!(
            scrape(1.0),
            Some(Reason::NoEndpointsReady),
            "one ready reading is not enough"
        );
        assert_eq!(
            scrape(0.0),
            Some(Reason::NoEndpointsReady),
            "a relapse restarts the count"
        );
        assert_eq!(scrape(1.0), Some(Reason::NoEndpointsReady));
        assert_eq!(scrape(1.0), Some(Reason::Ready));
    }

    /// One EPP scrape for pool qwen3: its ready count, a running average frozen at 128, and
    /// `answered` usage reports so far.
    fn saturated(ready: f64, answered: f64) -> Vec<Observation> {
        let unlabeled = |metric: &str, value: f64| Observation {
            metric: metric.to_owned(),
            labels: BTreeMap::new(),
            value,
            timestamp_ms: None,
        };
        vec![
            sample("llm_d_epp_ready_endpoints", "qwen3", ready),
            sample("llm_d_epp_average_running_requests", "qwen3", 128.0),
            unlabeled("llm_d_epp_request_input_tokens_count", answered),
            unlabeled("llm_d_epp_request_input_tokens_sum", answered * 100.0),
        ]
    }

    /// Record `observations` as the signals loop does, and return the verdict.
    fn scrape_at(store: &ReadinessStore, observations: &[Observation], now: Instant) -> Option<Reason> {
        let ready = ready_endpoints(observations, &DEFAULT_READY_ENDPOINTS, Some("qwen3"));
        store.record_latency("p", observations, now);
        store.record_success("p", ready, None, observations.to_vec(), now);
        reason(store, now)
    }

    const STEP: Duration = Duration::from_secs(5);

    #[test]
    fn a_saturated_pool_that_still_answers_stays_ready_on_zero_counts() {
        let store = ReadinessStore::default();
        let start = Instant::now();
        assert_eq!(scrape_at(&store, &saturated(2.0, 100.0), start), Some(Reason::Ready));
        for (n, answered) in [(1_u32, 140.0), (2, 180.0), (3, 220.0)] {
            assert_eq!(
                scrape_at(&store, &saturated(0.0, answered), start + STEP * n),
                Some(Reason::Ready),
                "scrape {n}: running 128 and answering is busy, not down"
            );
        }
        let verdict = store.verdict("p", false, STALE, start + STEP * 3).unwrap();
        assert!(verdict.message.contains("answered"), "{}", verdict.message);
    }

    #[test]
    fn a_zero_count_with_no_answers_excludes_even_with_running_requests() {
        let store = ReadinessStore::default();
        let start = Instant::now();
        assert_eq!(scrape_at(&store, &saturated(2.0, 100.0), start), Some(Reason::Ready));
        assert_eq!(
            scrape_at(&store, &saturated(0.0, 100.0), start + STEP),
            Some(Reason::Ready)
        );
        assert_eq!(
            scrape_at(&store, &saturated(0.0, 100.0), start + STEP * 2),
            Some(Reason::NoEndpointsReady),
            "the running average freezes at zero endpoints, so it is no evidence"
        );
    }

    #[test]
    fn answering_again_readmits_a_pool_whose_endpoint_count_never_returns() {
        let store = ReadinessStore::default();
        let start = Instant::now();
        scrape_at(&store, &saturated(2.0, 100.0), start);
        // Eight scrapes of zero with no answers: 35s past the last one, so excluded.
        for n in 1..=8_u32 {
            scrape_at(&store, &saturated(0.0, 100.0), start + STEP * n);
        }
        assert_eq!(
            reason(&store, start + STEP * 8),
            Some(Reason::NoEndpointsReady),
            "zero endpoints and nothing answering is down"
        );
        // The count stays zero, as it does when the metrics path is what broke, and the
        // pool answers again. Recovery must not require the series that caused the verdict.
        assert_eq!(
            scrape_at(&store, &saturated(0.0, 140.0), start + STEP * 9),
            Some(Reason::Ready),
            "answering readmits it even though the endpoint count never came back"
        );
    }

    #[test]
    fn progress_measured_against_a_stale_baseline_does_not_readmit() {
        let store = ReadinessStore::default();
        let start = Instant::now();
        scrape_at(&store, &saturated(2.0, 100.0), start);
        // Eight zero scrapes with no answers: excluded.
        for n in 1..=8_u32 {
            scrape_at(&store, &saturated(0.0, 100.0), start + STEP * n);
        }
        assert_eq!(reason(&store, start + STEP * 8), Some(Reason::NoEndpointsReady));
        // A 70 s gap, then a scrape whose counters rose since the last one. The rise is
        // measured from a baseline older than the window, so it is not recent progress.
        let late = start + STEP * 8 + Duration::from_secs(70);
        assert_eq!(
            scrape_at(&store, &saturated(0.0, 140.0), late),
            Some(Reason::NoEndpointsReady),
            "a delta from before the window is not an answer in the window"
        );
    }

    #[test]
    fn a_pool_that_stops_answering_is_excluded_once_the_window_passes() {
        let store = ReadinessStore::default();
        let start = Instant::now();
        scrape_at(&store, &saturated(2.0, 100.0), start);
        let answered = start + STEP;
        scrape_at(&store, &saturated(0.0, 140.0), answered);
        // Six more scrapes reach 30s after the last answer: still inside the window.
        for n in 1..=6_u32 {
            assert_eq!(
                scrape_at(&store, &saturated(0.0, 140.0), answered + STEP * n),
                Some(Reason::Ready),
                "{}s after the last answer",
                (STEP * n).as_secs()
            );
        }
        assert_eq!(
            scrape_at(&store, &saturated(0.0, 140.0), answered + STEP * 7),
            Some(Reason::NoEndpointsReady)
        );
    }

    #[test]
    fn answers_from_an_epp_serving_several_pools_prove_nothing() {
        let store = ReadinessStore::default();
        let start = Instant::now();
        let with_other_pool = |ready, answered| {
            let mut observations = saturated(ready, answered);
            observations.push(sample("llm_d_epp_ready_endpoints", "llama", 3.0));
            observations
        };
        scrape_at(&store, &with_other_pool(2.0, 100.0), start);
        scrape_at(&store, &with_other_pool(0.0, 140.0), start + STEP);
        assert_eq!(
            scrape_at(&store, &with_other_pool(0.0, 180.0), start + STEP * 2),
            Some(Reason::NoEndpointsReady)
        );
    }

    #[test]
    fn a_first_scrape_that_fails_gets_the_staleness_window_before_a_verdict() {
        let store = ReadinessStore::default();
        let start = Instant::now();
        store.record_failure("p", ScrapeClass::Timeout, start);
        assert_eq!(
            reason(&store, start + STALE),
            Some(Reason::AwaitingFirstScrape),
            "grace, not a guess"
        );
        assert_eq!(
            reason(&store, start + STALE + Duration::from_secs(1)),
            Some(Reason::ScrapeTimedOut)
        );
    }

    #[test]
    fn a_failed_scrape_names_its_class() {
        let cases = [
            (ScrapeClass::Timeout, Reason::ScrapeTimedOut),
            (ScrapeClass::Unauthorized, Reason::ScrapeUnauthorized),
            (ScrapeClass::Tls, Reason::TlsHandshakeFailed),
            (ScrapeClass::Dns, Reason::ScrapeFailed),
            (ScrapeClass::Connect, Reason::ScrapeFailed),
            (ScrapeClass::Http, Reason::ScrapeFailed),
            (ScrapeClass::BodyCap, Reason::ScrapeFailed),
            (ScrapeClass::Parse, Reason::ScrapeFailed),
            (ScrapeClass::Config, Reason::ScrapeFailed),
        ];
        let start = Instant::now();
        for (class, expected) in cases {
            let store = ReadinessStore::default();
            store.record_failure("p", class, start);
            let verdict = store
                .verdict("p", false, STALE, start + STALE + Duration::from_secs(1))
                .expect("judged");
            assert_eq!(verdict.reason, expected, "{class:?}");
            assert!(
                verdict.message.ends_with(class.as_str()),
                "{class:?}: {}",
                verdict.message
            );
            assert!(verdict.reason.excludes(), "{class:?}");
        }
        assert_eq!(
            Reason::TlsHandshakeFailed.as_str(),
            "TLSHandshakeFailed",
            "the contract's spelling"
        );
    }

    #[test]
    fn providers_are_keyed_per_network() {
        let store = ReadinessStore::default();
        let now = Instant::now();
        for _ in 0..STREAK {
            store.record_success(&key("east", "p"), Some(0.0), None, Vec::new(), now);
        }
        store.record_success(&key("west", "p"), Some(3.0), None, Vec::new(), now);
        let reason_in = |network| {
            store
                .verdict(&key(network, "p"), false, STALE, now)
                .map(|verdict| verdict.reason)
        };
        assert_eq!(reason_in("east"), Some(Reason::NoEndpointsReady));
        assert_eq!(reason_in("west"), Some(Reason::Ready));
    }

    #[test]
    fn an_unavailable_provider_is_not_ready_whatever_its_metrics_say() {
        let store = ReadinessStore::default();
        let now = Instant::now();
        store.record_success("p", Some(3.0), None, Vec::new(), now);
        let verdict = store.verdict("p", true, STALE, now).unwrap();
        assert_eq!(verdict.reason, Reason::ProviderUnavailable);
        assert!(verdict.reason.excludes());
    }

    #[test]
    fn the_status_column_reads_like_a_node() {
        assert_eq!(Reason::Ready.display(), "Ready");
        assert_eq!(Reason::MetricsNotConfigured.display(), "Unknown");
        assert_eq!(Reason::AwaitingFirstScrape.display(), "Unknown");
        assert_eq!(Reason::NoLivenessCheck.display(), "Unknown");
        for reason in [
            Reason::NoEndpointsReady,
            Reason::MetricsStale,
            Reason::ScrapeFailed,
            Reason::ProviderUnavailable,
        ] {
            assert_eq!(reason.display(), "NotReady", "{reason:?}");
        }
    }

    #[test]
    fn the_condition_is_written_on_a_status_or_reason_change_only() {
        let ready = Verdict {
            reason: Reason::Ready,
            message: "2 ready endpoints".to_owned(),
        };
        let first = ready_condition(&[], &ready, "t0", Some(3)).expect("absent is written");
        assert_eq!(
            (first.status.as_str(), first.last_transition_time.as_str()),
            ("True", "t0")
        );

        let recount = Verdict {
            message: "3 ready endpoints".to_owned(),
            ..ready
        };
        assert!(
            ready_condition(std::slice::from_ref(&first), &recount, "t1", Some(3)).is_none(),
            "a new count alone is not news"
        );
        let regenerated = ready_condition(std::slice::from_ref(&first), &recount, "t1", Some(4))
            .expect("a new generation is written");
        assert_eq!(
            regenerated.last_transition_time, "t0",
            "the same status keeps its transition time"
        );

        let down = Verdict {
            reason: Reason::NoEndpointsReady,
            message: "0 ready endpoints".to_owned(),
        };
        let second = ready_condition(std::slice::from_ref(&first), &down, "t2", Some(3)).unwrap();
        assert_eq!(
            (second.status.as_str(), second.last_transition_time.as_str()),
            ("False", "t2")
        );

        let unreachable = Verdict {
            reason: Reason::ScrapeFailed,
            message: "no scrape".to_owned(),
        };
        let third = ready_condition(std::slice::from_ref(&second), &unreachable, "t3", Some(3)).unwrap();
        assert_eq!(
            third.last_transition_time, "t2",
            "a new reason under the same status keeps the transition time"
        );
    }

    #[test]
    fn statuses_match_the_condition_contract() {
        assert_eq!(Reason::Ready.status(), "True");
        assert_eq!(Reason::MetricsNotConfigured.status(), "Unknown");
        assert!(!Reason::MetricsNotConfigured.excludes(), "unknown is not down");
        assert_eq!(Reason::AwaitingFirstScrape.status(), "Unknown");
        assert!(!Reason::AwaitingFirstScrape.excludes(), "waiting is not down");
        assert_eq!(Reason::NoLivenessCheck.status(), "Unknown");
        assert!(
            !Reason::NoLivenessCheck.excludes(),
            "a scrape carrying no ready-endpoint series leaves readiness unknown, not false"
        );
        for reason in [
            Reason::NoEndpointsReady,
            Reason::MetricsStale,
            Reason::ScrapeTimedOut,
            Reason::ScrapeUnauthorized,
            Reason::TlsHandshakeFailed,
            Reason::ScrapeFailed,
            Reason::ProviderUnavailable,
        ] {
            assert_eq!(reason.status(), "False");
            assert!(reason.excludes());
        }
    }

    fn counted(endpoint: &str, namespace: &str, producer: &str, value: f64) -> Observation {
        Observation {
            metric: "llm_d_epp_inflight_requests".to_owned(),
            labels: BTreeMap::from([
                ("endpoint_name".to_owned(), endpoint.to_owned()),
                ("namespace".to_owned(), namespace.to_owned()),
                ("producer_name".to_owned(), producer.to_owned()),
            ]),
            value,
            timestamp_ms: None,
        }
    }

    fn queued(pool: &str, value: f64) -> Observation {
        Observation {
            metric: "llm_d_epp_flow_control_queue_size".to_owned(),
            labels: BTreeMap::from([("inference_pool".to_owned(), pool.to_owned())]),
            value,
            timestamp_ms: None,
        }
    }

    #[test]
    fn in_flight_reads_the_larger_of_the_epp_count_and_the_pool_averages() {
        let averages = [
            sample("llm_d_epp_average_running_requests", "qwen3", 6.0),
            sample("llm_d_epp_average_queue_size", "qwen3", 1.5),
        ];
        let mut observations = vec![counted("a", "ns", "p", 7.0), counted("b", "ns", "p", 5.0)];
        assert_eq!(
            in_flight(&observations, Some(2.0), None),
            Some((12.0, InFlightSource::Epp)),
            "the EPP's count alone"
        );
        observations.extend(averages.clone());
        assert_eq!(
            in_flight(&observations, Some(1.0), Some("qwen3")),
            Some((12.0, InFlightSource::Epp)),
            "12 counted is above (6 + 1.5) x 1"
        );
        let restarted = [vec![counted("a", "ns", "p", 0.0)], averages.to_vec()].concat();
        assert_eq!(
            in_flight(&restarted, Some(2.0), Some("qwen3")),
            Some((15.0, InFlightSource::EngineAverages)),
            "an EPP restart reading 0 is floored by (6 + 1.5) x 2"
        );
        assert_eq!(
            in_flight(&averages, Some(2.0), Some("qwen3")),
            Some((15.0, InFlightSource::EngineAverages)),
            "the averages alone"
        );
    }

    #[test]
    fn in_flight_counts_each_endpoint_once_across_producers() {
        let observations = [
            counted("a", "ns", "first", 4.0),
            counted("a", "ns", "second", 3.0),
            counted("b", "ns", "first", 2.0),
        ];
        assert_eq!(
            in_flight(&observations, None, None),
            Some((6.0, InFlightSource::Epp)),
            "4 + 2, not 9"
        );
    }

    #[test]
    fn in_flight_adds_requests_flow_control_holds_for_the_pool() {
        let observations = [counted("a", "ns", "p", 3.0), queued("qwen3", 4.0), queued("other", 9.0)];
        assert_eq!(
            in_flight(&observations, None, Some("qwen3")),
            Some((7.0, InFlightSource::Epp)),
            "3 dispatched plus 4 held for qwen3, not other's 9"
        );
    }

    #[test]
    fn in_flight_uses_pool_averages_when_the_epp_serves_several_pools() {
        let observations = [
            counted("a", "ns", "p", 50.0),
            sample("llm_d_epp_ready_endpoints", "qwen3", 2.0),
            sample("llm_d_epp_ready_endpoints", "llama", 1.0),
            sample("llm_d_epp_average_running_requests", "qwen3", 3.0),
            sample("llm_d_epp_average_queue_size", "qwen3", 1.0),
        ];
        assert_eq!(
            in_flight(&observations, Some(2.0), Some("qwen3")),
            Some((8.0, InFlightSource::EngineAverages)),
            "the unlabeled count covers both pools, so (3 + 1) x 2 for qwen3"
        );
    }

    #[test]
    fn in_flight_is_unknown_rather_than_idle() {
        let averages = [
            sample("llm_d_epp_average_running_requests", "qwen3", 6.0),
            sample("llm_d_epp_average_queue_size", "qwen3", 1.5),
        ];
        assert_eq!(in_flight(&averages, None, None), None, "no endpoint count, no estimate");
        assert_eq!(
            in_flight(&averages, Some(0.0), Some("qwen3")),
            None,
            "no fresh endpoint behind frozen averages"
        );
        assert_eq!(
            in_flight(&averages[..1], Some(2.0), None),
            None,
            "a missing average is not zero"
        );
        assert_eq!(
            in_flight(&[queued("qwen3", 4.0)], None, Some("qwen3")),
            None,
            "a queue alone is no count"
        );
    }

    #[test]
    fn a_multi_pool_epp_publishes_no_latency_but_keeps_its_history() {
        let store = ReadinessStore::default();
        let start = Instant::now();
        // Enough completed requests for the error ratio to publish, so an empty return is
        // the gate rather than an empty window.
        let counters = |requests: f64, pools: usize| {
            let unlabeled = |metric: &str, value: f64| Observation {
                metric: metric.to_owned(),
                labels: BTreeMap::new(),
                value,
                timestamp_ms: None,
            };
            let mut obs = vec![
                sample("llm_d_epp_ready_endpoints", "qwen3", 2.0),
                unlabeled("llm_d_epp_request_total", requests),
                unlabeled("llm_d_epp_request_error_total", 0.0),
            ];
            if pools > 1 {
                obs.push(sample("llm_d_epp_ready_endpoints", "other-pool", 3.0));
            }
            obs
        };
        // One pool: a baseline, then a window with 100 requests in it publishes.
        store.record_latency("one", &counters(10.0, 1), start);
        assert!(
            !store
                .record_latency("one", &counters(110.0, 1), start + STEP)
                .is_empty(),
            "a single-pool EPP publishes its ratio"
        );
        // Two pools, same counters: the request totals carry no pool label, so the figure
        // belongs to no single provider and nothing is published.
        store.record_latency("two", &counters(10.0, 2), start);
        assert!(
            store
                .record_latency("two", &counters(110.0, 2), start + STEP)
                .is_empty(),
            "a figure summed across pools is not this provider's latency"
        );
        // History was kept while unattributable, so one pool again publishes at once.
        assert!(
            !store
                .record_latency("two", &counters(210.0, 1), start + STEP * 2)
                .is_empty(),
            "the baseline survived, so the next single-pool scrape publishes"
        );
    }

    #[test]
    fn a_scrape_label_is_forgotten_under_the_name_it_was_recorded_under() {
        // The defect this pins: scrapes were counted under the routing identity and
        // forgotten under the provider name, so a provider declaring routingClusterRef
        // leaked its series and could collide with another provider's.
        for (network, provider) in [("net", "qwen3-east"), ("net", "qwen3-west")] {
            let key = key(network, provider);
            assert_eq!(
                provider_of(&key),
                provider,
                "the label both sides use comes from the key"
            );
        }
        assert_eq!(
            provider_of("no-separator"),
            "no-separator",
            "a keyless string is itself"
        );
        assert_eq!(provider_of("net/a/b"), "a/b", "only the network is stripped");
    }

    #[test]
    fn the_drain_inference_expires_and_yields_to_an_answering_engine() {
        let store = ReadinessStore::default();
        let start = Instant::now();
        // A series seen long ago is not news: a renamed label must not read as a drain forever.
        assert_eq!(store.thawed("stale", Some(2.0), true, start), Some(2.0));
        assert_eq!(
            store.thawed("stale", Some(2.0), false, start + PROGRESS_WINDOW * 2),
            Some(2.0),
            "past the window the count stands: the series is gone, not the pool"
        );
        // Inside the window, an answering engine proves pods exist whatever the collector does.
        let answering = ReadinessStore::default();
        assert_eq!(answering.thawed("p", Some(2.0), true, start), Some(2.0));
        answering.record_latency("p", &saturated(2.0, 100.0), start);
        answering.record_latency("p", &saturated(2.0, 140.0), start + STEP);
        assert_eq!(
            answering.thawed("p", Some(2.0), false, start + STEP),
            Some(2.0),
            "an engine answering means the count is real, not frozen"
        );
    }

    #[test]
    fn a_positive_count_with_the_per_unit_series_gone_reads_as_zero() {
        let store = ReadinessStore::default();
        let now = Instant::now();
        // Units reporting: the count stands.
        assert_eq!(store.thawed("p", Some(2.0), true, now), Some(2.0));
        // The pool drains: the gauge freezes at 2 while the per-unit collector stops.
        assert_eq!(
            store.thawed("p", Some(2.0), false, now),
            Some(0.0),
            "a frozen gauge is not a ready pool"
        );
        // A provider that never had per-unit series keeps its count.
        assert_eq!(store.thawed("q", Some(3.0), false, now), Some(3.0));
        assert_eq!(store.thawed("p", None, false, now), None);
    }
}
