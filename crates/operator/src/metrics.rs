//! Prometheus metrics for gateway probe observability.
//!
//! All label values are bounded enum variants — no site names,
//! addresses, fingerprints, or PEM content.

use std::{sync::LazyLock, time::Duration};

use prometheus::{
    Encoder as _, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder, proto::MetricFamily,
};

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Global registry for operator metrics.
static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    let r = Registry::new();
    r.register(Box::new(PROBE_TOTAL.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(SITE_IDENTITY_EXPIRY.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(SITE_IDENTITY_RENEWALS.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PROBE_DURATION.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PHASE_TRANSITIONS.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(SITE_PHASE.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(AGENT_TOOL_PROVIDER_PHASE_TRANSITIONS.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(MCP_PROBE_TOTAL.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(MCP_PROBE_DURATION.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PEER_POLL_TOTAL.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PEER_POLL_RETRIES.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PEER_SIGNALS_REFUSED.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PROVIDER_SCRAPES.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PROVIDER_LAST_SCRAPE_SUCCESS.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PEER_POLL_DURATION.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PEER_POLL_SLOW.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PEER_RESPONSE_BYTES.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PEER_COLLECTION_UP.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PEER_LAST_SUCCESS.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PEER_POLLS_IN_FLIGHT.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(SWIM_KEY_PENDING.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(SWIM_PENDING_DROPS.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(SIGNALS_SHED.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(MODEL_DISCOVERY_TOTAL.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r
});

/// Signals connections shed at accept, by the limit that shed them.
static SIGNALS_SHED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "grid_signals_connections_shed_total",
            "Signals connections shed at accept",
        ),
        &["limit"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// The site identity's `notAfter`, Unix seconds; zero when the identity cannot be read.
static SITE_IDENTITY_EXPIRY: LazyLock<IntGauge> = LazyLock::new(|| {
    IntGauge::new(
        "grid_site_identity_expiry_timestamp_seconds",
        "When the site identity certificate expires",
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Site identity rotation attempts, by result.
static SITE_IDENTITY_RENEWALS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("grid_site_identity_rotations_total", "Site identity rotation attempts"),
        &["result"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// 1 while SWIM holds traffic for a key that has not loaded.
static SWIM_KEY_PENDING: LazyLock<IntGauge> = LazyLock::new(|| {
    IntGauge::new("grid_swim_key_pending", "1 while SWIM holds traffic for its key")
        .unwrap_or_else(|_| std::process::abort())
});

/// Inbound SWIM packets dropped while the key is pending.
static SWIM_PENDING_DROPS: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "grid_swim_key_pending_dropped_total",
        "Inbound SWIM packets dropped while the key is pending",
    )
    .unwrap_or_else(|_| std::process::abort())
});

// ---------------------------------------------------------------------------
// Peer polling
//
// Outcome is a label, not a success flag: a refusal means the peer is down, a
// TLS failure means retrying will not help, a 403 means it declined. Collapsing
// them into "error" leaves "why is this peer not scored" unanswerable.
// ---------------------------------------------------------------------------

/// Peer polls by peer and outcome.
static PEER_POLL_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("grid_peer_poll_total", "Peer signal polls by outcome"),
        &["peer", "outcome"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Provider metrics scrapes by provider and result: `success`, `no_series`, or a failure class
/// (`timeout`, `unauthorized`, `tls`, `dns`, `connect`, `http`, `body_cap`, `parse`, `config`).
static PROVIDER_SCRAPES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("grid_provider_scrape_total", "Provider metrics scrapes by result"),
        &["grid_provider", "result"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// When each provider's metrics last scraped with its ready-endpoint series, Unix seconds.
static PROVIDER_LAST_SCRAPE_SUCCESS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "grid_provider_last_scrape_success_timestamp_seconds",
            "Unix time of the provider's last scrape with its ready-endpoint series",
        ),
        &["grid_provider"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Peer observations refused at ingest, by peer and reason.
static PEER_SIGNALS_REFUSED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("grid_peer_signals_refused_total", "Peer observations refused at ingest"),
        &["peer", "reason"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Retried attempts by peer and the outcome that prompted the retry.
///
/// Separate from the poll counter because a poll that succeeded on its third
/// attempt is a success, and counting it as two failures would misreport
/// availability. The retries are the cost of that success, not failures of it.
static PEER_POLL_RETRIES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("grid_peer_poll_retries_total", "Retried peer poll attempts"),
        &["peer", "reason"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Time to complete a poll, including retries.
///
/// Buckets run to ten seconds because a cross-region poll is not a local call
/// and the interesting tail is well past the default buckets.
static PEER_POLL_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "grid_peer_poll_duration_seconds",
            "Peer poll duration including retries",
        )
        .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        &["peer"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Polls that took longer than the configured threshold.
///
/// A histogram already carries this, but a slow poll is worth alerting on and a
/// counter is what an alert can be written against without picking a quantile.
static PEER_POLL_SLOW: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("grid_peer_poll_slow_total", "Peer polls exceeding the slow threshold"),
        &["peer"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Bytes read from peers, which is what the scale argument turns on.
static PEER_RESPONSE_BYTES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "grid_peer_response_bytes_total",
            "Bytes read from peer signal endpoints",
        ),
        &["peer"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Whether this site can currently collect from each peer.
///
/// Without it, a peer with nothing to report and a peer this site cannot reach
/// look identical in the exposition, and a routing decision made in that
/// ambiguity cannot be explained afterward.
static PEER_COLLECTION_UP: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new("grid_collection_up", "Whether the last poll of this peer succeeded"),
        &["peer"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// When each peer was last collected from, as seconds since the epoch.
///
/// A gauge of the moment rather than of the elapsed time, so a reader computes
/// the age against its own clock and the value does not have to be rewritten on
/// every scrape to stay true.
static PEER_LAST_SUCCESS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "grid_peer_last_success_timestamp_seconds",
            "Unix time of the last successful poll of this peer",
        ),
        &["peer"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Polls in flight, which is how close the worker pool is to saturated.
static PEER_POLLS_IN_FLIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    IntGauge::new("grid_peer_polls_in_flight", "Peer polls currently in flight")
        .unwrap_or_else(|_| std::process::abort())
});

/// Total gateway probe attempts by outcome and TLS mode.
static PROBE_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("grid_gateway_probe_total", "Total gateway probe attempts"),
        &["outcome", "tls_mode"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Gateway probe duration in seconds.
static PROBE_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::with_opts(HistogramOpts::new(
        "grid_gateway_probe_duration_seconds",
        "Gateway probe duration",
    ))
    .unwrap_or_else(|_| std::process::abort())
});

/// `GridSite` phase transitions by source phase, target phase, and reason.
static PHASE_TRANSITIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("grid_site_phase_transition_total", "GridSite phase transitions"),
        &["from_phase", "to_phase", "reason"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Each `GridSite`'s phase as a state set, shaped like `kube_pod_status_phase`: 1 for the
/// current phase and 0 for the other five.
static SITE_PHASE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "grid_site_phase",
            "GridSite phase: 1 for the current phase, 0 for the others",
        ),
        &["site", "phase"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Every `GridSite` phase, the `phase` label values of [`SITE_PHASE`].
pub(crate) const SITE_PHASES: [&str; 6] = ["Pending", "Discovered", "Connecting", "Active", "Unreachable", "Left"];

/// Sites with [`SITE_PHASE`] series, so a deleted site's series can be removed.
static SITE_PHASE_SITES: LazyLock<std::sync::Mutex<std::collections::BTreeSet<String>>> =
    LazyLock::new(Default::default);

/// When a reconcile last refreshed [`SITE_PHASE`].
static SITE_PHASE_REFRESHED: std::sync::Mutex<Option<std::time::Instant>> = std::sync::Mutex::new(None);

/// Age past which [`SITE_PHASE`] is cleared: three `GridNetwork` requeues, so the series
/// do not outlive the last `GridNetwork` or a run of failed site lists.
const SITE_PHASE_MAX_AGE: Duration = Duration::from_secs(900);

/// `AgentToolProvider` phase transitions by source phase, target phase, and reason.
///
/// Kept as a distinct metric (rather than reusing [`PHASE_TRANSITIONS`]) so
/// dashboards can alert on each CRD's convergence independently — see grid#9.
static AGENT_TOOL_PROVIDER_PHASE_TRANSITIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "grid_agent_tool_provider_phase_transition_total",
            "AgentToolProvider phase transitions",
        ),
        &["from_phase", "to_phase", "reason"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Total `AgentToolProvider` MCP `tools/list` probe attempts by outcome.
///
/// The `outcome` label comes from
/// [`mcp_probe::mcp_probe_outcome_label`](crate::resources::mcp_probe::mcp_probe_outcome_label),
/// which is deliberately bounded to the fixed `McpProbeOutcome` variant set
/// — never the free-form reason string `TlsConfigInvalid` carries — so this
/// metric's cardinality stays fixed regardless of how many distinct Secret
/// misconfigurations occur in the cluster.
static MCP_PROBE_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "grid_mcp_probe_total",
            "Total AgentToolProvider MCP tools/list probe attempts",
        ),
        &["outcome"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// `AgentToolProvider` MCP `tools/list` probe duration in seconds.
static MCP_PROBE_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::with_opts(HistogramOpts::new(
        "grid_mcp_probe_duration_seconds",
        "AgentToolProvider MCP tools/list probe duration",
    ))
    .unwrap_or_else(|_| std::process::abort())
});

/// Served-model discovery polls by provider and outcome.
static MODEL_DISCOVERY_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("grid_model_discovery_total", "Served-model discovery polls by outcome"),
        &["provider", "outcome"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Record a completed gateway probe.
pub(crate) fn record_probe(outcome: &str, tls_mode: &str, duration: Duration) {
    PROBE_TOTAL.with_label_values(&[outcome, tls_mode]).inc();
    PROBE_DURATION.observe(duration.as_secs_f64());
}

/// Set each site's phase series and remove the series of sites no longer present.
pub(crate) fn set_site_phases<S: AsRef<str>>(phases: impl IntoIterator<Item = (S, &'static str)>) {
    let mut tracked = SITE_PHASE_SITES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut present = std::collections::BTreeSet::new();
    for (site, current) in phases {
        let site = site.as_ref();
        for phase in SITE_PHASES {
            SITE_PHASE
                .with_label_values(&[site, phase])
                .set(i64::from(phase == current));
        }
        present.insert(site.to_owned());
    }
    for gone in tracked.difference(&present) {
        for phase in SITE_PHASES {
            let _absent = SITE_PHASE.remove_label_values(&[gone.as_str(), phase]);
        }
    }
    *tracked = present;
    drop(tracked);
    *SITE_PHASE_REFRESHED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(std::time::Instant::now());
}

/// Remove every site phase series, for when the sites cannot be listed.
pub(crate) fn clear_site_phases() {
    set_site_phases(std::iter::empty::<(&str, &'static str)>());
}

/// Whether series refreshed at `refreshed` are too old to report at `now`.
fn site_phases_expired(refreshed: Option<std::time::Instant>, now: std::time::Instant) -> bool {
    refreshed.is_some_and(|at| now.saturating_duration_since(at) > SITE_PHASE_MAX_AGE)
}

/// Clear site phase series no reconcile has refreshed within [`SITE_PHASE_MAX_AGE`].
fn expire_site_phases() {
    let refreshed = *SITE_PHASE_REFRESHED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if site_phases_expired(refreshed, std::time::Instant::now()) {
        clear_site_phases();
        *SITE_PHASE_REFRESHED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

/// Record a `GridSite` phase transition.
pub(crate) fn record_phase_transition(from: &str, to: &str, reason: &str) {
    PHASE_TRANSITIONS.with_label_values(&[from, to, reason]).inc();
}

/// Record an `AgentToolProvider` phase transition.
pub(crate) fn record_agent_tool_provider_phase_transition(from: &str, to: &str, reason: &str) {
    AGENT_TOOL_PROVIDER_PHASE_TRANSITIONS
        .with_label_values(&[from, to, reason])
        .inc();
}

/// Record a completed `AgentToolProvider` MCP `tools/list` probe attempt.
pub(crate) fn record_mcp_probe(outcome: &str, duration: Duration) {
    MCP_PROBE_TOTAL.with_label_values(&[outcome]).inc();
    MCP_PROBE_DURATION.observe(duration.as_secs_f64());
}

/// Record a successful served-model discovery poll.
pub(crate) fn record_model_discovery_success(provider: &str) {
    MODEL_DISCOVERY_TOTAL.with_label_values(&[provider, "ok"]).inc();
}

/// Record a failed served-model discovery poll; `reason` must be bounded.
pub(crate) fn record_model_discovery_failure(provider: &str, reason: &str) {
    MODEL_DISCOVERY_TOTAL.with_label_values(&[provider, reason]).inc();
}

/// Record a finished peer poll, retries included.
///
/// `outcome` is the outcome of the last attempt, so a poll that succeeded after
/// two retries records one success here and two retries in
/// [`record_peer_retry`]. Availability and cost are separate questions.
pub(crate) fn record_peer_poll(peer: &str, outcome: &str, duration: Duration, bytes: usize, slow_after: Duration) {
    PEER_POLL_TOTAL.with_label_values(&[peer, outcome]).inc();
    PEER_POLL_DURATION
        .with_label_values(&[peer])
        .observe(duration.as_secs_f64());
    if duration >= slow_after {
        PEER_POLL_SLOW.with_label_values(&[peer]).inc();
    }
    if bytes > 0 {
        PEER_RESPONSE_BYTES
            .with_label_values(&[peer])
            .inc_by(bytes.try_into().unwrap_or(u64::MAX));
    }
}

/// Count one scrape of `provider` with `result`.
pub(crate) fn record_provider_scrape(provider: &str, result: &str) {
    PROVIDER_SCRAPES.with_label_values(&[provider, result]).inc();
}

/// Record that `provider` last scraped with its ready-endpoint series at `at`.
pub(crate) fn set_provider_last_scrape_success(provider: &str, at: std::time::SystemTime) {
    let secs = at.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    PROVIDER_LAST_SCRAPE_SUCCESS
        .with_label_values(&[provider])
        .set(secs.try_into().unwrap_or(i64::MAX));
}

/// Drop `provider`'s scrape series once it is gone, so they do not outlive it.
pub(crate) fn forget_provider_scrapes(provider: &str) {
    let _absent = PROVIDER_LAST_SCRAPE_SUCCESS.remove_label_values(&[provider]);
    for result in [
        "success",
        "no_series",
        "timeout",
        "unauthorized",
        "tls",
        "dns",
        "connect",
        "http",
        "body_cap",
        "parse",
        "config",
    ] {
        let _absent_result = PROVIDER_SCRAPES.remove_label_values(&[provider, result]);
    }
}

/// Count a peer observation this hub refused: `name`, `provider`, `value`, or `provider_cap`.
pub(crate) fn record_peer_signal_refused(peer: &str, reason: &str) {
    PEER_SIGNALS_REFUSED.with_label_values(&[peer, reason]).inc();
}

/// Record an attempt that failed and will be tried again.
pub(crate) fn record_peer_retry(peer: &str, reason: &str) {
    PEER_POLL_RETRIES.with_label_values(&[peer, reason]).inc();
}

/// Record whether this site can currently collect from a peer.
///
/// Called on every poll, including the ones that succeed, so the gauge tracks
/// the current state rather than latching on the first failure.
pub(crate) fn set_peer_collection_up(peer: &str, up: bool, at: std::time::SystemTime) {
    PEER_COLLECTION_UP.with_label_values(&[peer]).set(i64::from(up));
    if up {
        let secs = at.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        PEER_LAST_SUCCESS
            .with_label_values(&[peer])
            .set(secs.try_into().unwrap_or(i64::MAX));
    }
}

/// Move the in-flight count, so the worker pool's saturation is visible.
pub(crate) fn peer_polls_in_flight(delta: i64) {
    PEER_POLLS_IN_FLIGHT.add(delta);
}

/// Set whether SWIM is holding traffic for its key.
pub(crate) fn set_swim_key_pending(pending: bool) {
    SWIM_KEY_PENDING.set(i64::from(pending));
}

/// Count an inbound SWIM packet dropped while the key is pending.
pub(crate) fn record_swim_pending_drop() {
    SWIM_PENDING_DROPS.inc();
}

/// Count a signals connection shed by `limit`, `total` or `source`.
pub fn record_signals_shed(limit: &str) {
    SIGNALS_SHED.with_label_values(&[limit]).inc();
}

/// Set when the site identity expires, Unix seconds.
pub fn set_site_identity_expiry(not_after: i64) {
    SITE_IDENTITY_EXPIRY.set(not_after);
}

/// Count a renewal attempt: `renewed`, `refused`, `failed`, or `expired`.
pub fn record_site_identity_renewal(result: &str) {
    SITE_IDENTITY_RENEWALS.with_label_values(&[result]).inc();
}

/// Provider series exported on `/metrics`, as each site's operator resolves them.
const PROVIDER_SIGNALS: [(&str, &str); 7] = [
    ("grid_provider_ready", "1 when the provider can serve, 0 when not."),
    (
        "grid_provider_ready_endpoints",
        "Endpoints behind the provider answering with fresh metrics.",
    ),
    (
        "grid_provider_in_flight_requests",
        "Requests the provider holds, running, engine-queued, and held by flow control.",
    ),
    (
        "grid_provider_ttft_p50_seconds",
        "Median streaming time to first token over the last 30s.",
    ),
    (
        "grid_provider_ttft_p90_seconds",
        "90th percentile streaming time to first token over the last 30s.",
    ),
    (
        "grid_provider_tpot_seconds",
        "Mean streaming time per output token over the last 30s.",
    ),
    (
        "grid_provider_error_ratio",
        "Failed requests over all requests in the last 30s.",
    ),
];

/// Labels on each exported provider series: both bounded by the grid's sites and providers.
const PROVIDER_LABELS: [&str; 2] = ["grid_site", "grid_provider"];

/// Exports the provider series held in `stores` at each scrape, this site's own and those
/// polled from peers. A series this site does not hold is absent, not 0.
struct ProviderSignals {
    /// The local and peer signal stores.
    stores: Vec<crate::signals::SignalStore>,
    /// One gauge per exported series, paired with its metric name so a failed constructor
    /// drops only its own series.
    gauges: Vec<(&'static str, prometheus::GaugeVec)>,
    /// Serializes a scrape's reset and refill of the shared gauges.
    collecting: std::sync::Mutex<()>,
}

impl prometheus::core::Collector for ProviderSignals {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        self.gauges.iter().flat_map(|(_, gauge)| gauge.desc()).collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let _collecting = self
            .collecting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (_, gauge) in &self.gauges {
            gauge.reset();
        }
        let mut seen = std::collections::BTreeSet::new();
        for sample in self.stores.iter().flat_map(crate::signals::SignalStore::current) {
            let (Some(site), Some(provider)) = (sample.labels.get("grid_site"), sample.labels.get("grid_provider"))
            else {
                continue;
            };
            let Some((_, gauge)) = self.gauges.iter().find(|(name, _)| *name == sample.metric) else {
                continue;
            };
            // The first store holding a series wins: this site's own before a peer's.
            if seen.insert((sample.metric.clone(), site.clone(), provider.clone())) {
                gauge
                    .with_label_values(&[site.as_str(), provider.as_str()])
                    .set(sample.value);
            }
        }
        self.gauges
            .iter()
            .flat_map(|(_, gauge)| prometheus::core::Collector::collect(gauge))
            .collect()
    }
}

/// Export the provider series held in `stores` on `/metrics`. Call once, with this site's
/// store first.
pub fn register_provider_signals(stores: Vec<crate::signals::SignalStore>) {
    if let Err(error) = REGISTRY.register(Box::new(provider_signals(stores))) {
        tracing::warn!(%error, "metrics: provider signals already registered");
    }
}

/// The collector over `stores`.
fn provider_signals(stores: Vec<crate::signals::SignalStore>) -> ProviderSignals {
    let gauges = PROVIDER_SIGNALS
        .iter()
        .filter_map(|(name, help)| {
            prometheus::GaugeVec::new(Opts::new(*name, *help), &PROVIDER_LABELS)
                .ok()
                .map(|gauge| (*name, gauge))
        })
        .collect();
    ProviderSignals {
        stores,
        gauges,
        collecting: std::sync::Mutex::new(()),
    }
}

/// Gather all registered metrics for serialization.
pub(crate) fn gather_metrics() -> Vec<MetricFamily> {
    expire_site_phases();
    REGISTRY.gather()
}

/// Encode all metrics as Prometheus text format.
pub fn encode_metrics() -> Vec<u8> {
    let encoder = TextEncoder::new();
    let families = gather_metrics();
    let mut buffer = Vec::new();
    encoder
        .encode(&families, &mut buffer)
        .unwrap_or_else(|_| std::process::abort());
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn site_phases_expire_three_requeues_after_the_last_refresh() {
        let now = std::time::Instant::now();
        assert!(!site_phases_expired(None, now), "never set");
        assert!(
            !site_phases_expired(Some(now), now + SITE_PHASE_MAX_AGE),
            "at the limit"
        );
        assert!(
            site_phases_expired(Some(now), now + SITE_PHASE_MAX_AGE + Duration::from_secs(1)),
            "past it"
        );
    }

    #[test]
    fn record_probe_increments_counter() {
        record_probe("Verified", "mtls", Duration::from_millis(42));
        let val = PROBE_TOTAL.with_label_values(&["Verified", "mtls"]).get();
        assert!(val >= 1, "probe counter should be >= 1, got {val}");
    }

    /// The series `grid_site_phase` holds for `site`, as phase and value.
    fn site_phase_series(site: &str) -> Vec<(String, i64)> {
        use prometheus::core::Collector as _;
        let mut series: Vec<(String, i64)> = SITE_PHASE
            .collect()
            .iter()
            .flat_map(|family| family.get_metric().iter())
            .filter(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|l| l.name() == "site" && l.value() == site)
            })
            .filter_map(|metric| {
                let phase = metric
                    .get_label()
                    .iter()
                    .find(|l| l.name() == "phase")?
                    .value()
                    .to_owned();
                #[expect(clippy::cast_possible_truncation, reason = "the gauge holds 0 or 1")]
                Some((phase, metric.get_gauge().value() as i64))
            })
            .collect();
        series.sort();
        series
    }

    /// The only test that sets `grid_site_phase`, since each call replaces every site's series.
    #[test]
    fn a_site_phase_is_one_state_set_that_follows_transitions_and_goes_with_its_site() {
        let (hub, east) = ("site-phase-test-hub", "site-phase-test-east");
        set_site_phases([(hub, "Discovered"), (east, "Pending")]);
        let ones = |site| {
            site_phase_series(site)
                .into_iter()
                .filter(|(_, value)| *value == 1)
                .map(|(phase, _)| phase)
                .collect::<Vec<_>>()
        };
        assert_eq!(site_phase_series(hub).len(), SITE_PHASES.len(), "one series per phase");
        assert_eq!(ones(hub), ["Discovered"]);

        set_site_phases([(hub, "Active"), (east, "Pending")]);
        assert_eq!(ones(hub), ["Active"], "the transition moves the 1");

        set_site_phases([(hub, "Active")]);
        assert!(site_phase_series(east).is_empty(), "a deleted site's series go with it");
        assert_eq!(ones(hub), ["Active"]);
        set_site_phases(std::iter::empty::<(&str, &'static str)>());
        assert!(site_phase_series(hub).is_empty());
    }

    #[test]
    fn record_phase_transition_increments_counter() {
        record_phase_transition("Connecting", "Active", "TlsVerified");
        let val = PHASE_TRANSITIONS
            .with_label_values(&["Connecting", "Active", "TlsVerified"])
            .get();
        assert!(val >= 1, "transition counter should be >= 1, got {val}");
    }

    #[test]
    fn record_agent_tool_provider_phase_transition_increments_counter() {
        record_agent_tool_provider_phase_transition("Pending", "Available", "SitesMatched");
        let val = AGENT_TOOL_PROVIDER_PHASE_TRANSITIONS
            .with_label_values(&["Pending", "Available", "SitesMatched"])
            .get();
        assert!(
            val >= 1,
            "agent tool provider transition counter should be >= 1, got {val}"
        );
    }

    #[test]
    fn probe_duration_records_observation() {
        record_probe("ConnectTimeout", "mtls", Duration::from_millis(100));
        let count = PROBE_DURATION.get_sample_count();
        assert!(count >= 1, "histogram should have at least 1 observation");
    }

    #[test]
    fn record_mcp_probe_increments_counter_by_outcome() {
        record_mcp_probe("Success", Duration::from_millis(12));
        let val = MCP_PROBE_TOTAL.with_label_values(&["Success"]).get();
        assert!(val >= 1, "mcp probe counter should be >= 1, got {val}");
    }

    #[test]
    fn record_mcp_probe_records_duration_observation() {
        record_mcp_probe("Unreachable", Duration::from_millis(250));
        let count = MCP_PROBE_DURATION.get_sample_count();
        assert!(
            count >= 1,
            "mcp probe duration histogram should have at least 1 observation"
        );
    }

    #[test]
    fn encode_metrics_includes_mcp_probe_metrics() {
        record_mcp_probe("AuthRejected", Duration::from_millis(5));
        let buf = encode_metrics();
        let text = String::from_utf8(buf).unwrap_or_else(|_| std::process::abort());
        assert!(
            text.contains("grid_mcp_probe_total"),
            "output should contain mcp probe counter"
        );
        assert!(
            text.contains("grid_mcp_probe_duration_seconds"),
            "output should contain mcp probe duration histogram"
        );
    }

    #[test]
    fn encode_metrics_produces_prometheus_text() {
        record_probe("ConnectionFailed", "mtls", Duration::from_millis(1));
        let buf = encode_metrics();
        let text = String::from_utf8(buf).unwrap_or_else(|_| std::process::abort());
        assert!(
            text.contains("grid_gateway_probe_total"),
            "output should contain probe counter"
        );
        assert!(
            text.contains("grid_gateway_probe_duration_seconds"),
            "output should contain duration histogram"
        );
    }

    #[test]
    fn provider_scrapes_are_counted_by_result_and_forgotten_with_the_provider() {
        let provider = "scrape-count-test";
        record_provider_scrape(provider, "success");
        record_provider_scrape(provider, "no_series");
        record_provider_scrape(provider, "no_series");
        set_provider_last_scrape_success(provider, std::time::UNIX_EPOCH + Duration::from_secs(42));
        assert_eq!(PROVIDER_SCRAPES.with_label_values(&[provider, "success"]).get(), 1);
        assert_eq!(PROVIDER_SCRAPES.with_label_values(&[provider, "no_series"]).get(), 2);
        assert_eq!(PROVIDER_LAST_SCRAPE_SUCCESS.with_label_values(&[provider]).get(), 42);
        forget_provider_scrapes(provider);
        assert!(
            PROVIDER_SCRAPES.remove_label_values(&[provider, "success"]).is_err(),
            "forgotten with the provider"
        );
        assert!(PROVIDER_LAST_SCRAPE_SUCCESS.remove_label_values(&[provider]).is_err());
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "one store across two scrapes")]
    fn provider_signals_drop_a_series_the_store_no_longer_holds() {
        use prometheus::core::Collector as _;
        let ready = crate::signals::Observation {
            metric: "grid_provider_ready".to_owned(),
            labels: [("grid_site", "hq"), ("grid_provider", "pool")]
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            value: 1.0,
            timestamp_ms: None,
        };
        let store = crate::signals::SignalStore::new();
        let collector = provider_signals(vec![store.clone()]);
        let count = |families: &[MetricFamily]| -> usize {
            families
                .iter()
                .filter(|family| family.name() == "grid_provider_ready")
                .map(|family| family.get_metric().len())
                .sum()
        };
        store.refresh(
            std::collections::BTreeMap::from([("pool".to_owned(), vec![ready])]),
            Duration::from_secs(60),
        );
        assert_eq!(count(&collector.collect()), 1, "a held series is exported");
        store.refresh(
            std::collections::BTreeMap::from([("pool".to_owned(), Vec::new())]),
            Duration::from_secs(60),
        );
        assert_eq!(
            count(&collector.collect()),
            0,
            "a dropped series is not exported from the last scrape"
        );
        assert_eq!(
            collector.desc().len(),
            PROVIDER_SIGNALS.len(),
            "one desc per exported series"
        );
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "one scenario across both stores")]
    fn provider_signals_export_this_site_and_its_peers_and_omit_what_is_not_held() {
        use prometheus::core::Collector as _;
        let held = |site: &str, metric: &str, value: f64| crate::signals::Observation {
            metric: metric.to_owned(),
            labels: [("grid_site", site), ("grid_provider", "pool")]
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            value,
            timestamp_ms: None,
        };
        let local = crate::signals::SignalStore::new();
        local.refresh(
            std::collections::BTreeMap::from([(
                "pool".to_owned(),
                vec![
                    held("hq", "grid_provider_in_flight_requests", 0.5),
                    held("hq", "grid_provider_ttft_p50_seconds", 0.2),
                    held("hq", "llm_d_epp_average_queue_size", 3.0),
                ],
            )]),
            Duration::from_secs(60),
        );
        let peers = crate::signals::SignalStore::new();
        peers.refresh(
            std::collections::BTreeMap::from([(
                "retail".to_owned(),
                vec![
                    held("retail", "grid_provider_in_flight_requests", 0.9),
                    held("hq", "grid_provider_in_flight_requests", 7.0),
                ],
            )]),
            Duration::from_secs(60),
        );
        let families = provider_signals(vec![local, peers]).collect();
        let series = |name: &str| -> Vec<(String, f64)> {
            families
                .iter()
                .filter(|family| family.name() == name)
                .flat_map(|family| family.get_metric().iter())
                .map(|metric| {
                    let site = metric
                        .get_label()
                        .iter()
                        .find(|label| label.name() == "grid_site")
                        .map(|label| label.value().to_owned())
                        .unwrap_or_default();
                    (site, metric.get_gauge().value())
                })
                .collect()
        };
        let mut in_flight = series("grid_provider_in_flight_requests");
        in_flight.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            in_flight,
            [("hq".to_owned(), 0.5), ("retail".to_owned(), 0.9)],
            "this site's value wins"
        );
        assert_eq!(series("grid_provider_ttft_p50_seconds"), [("hq".to_owned(), 0.2)]);
        assert!(series("grid_provider_tpot_seconds").is_empty(), "not held, not 0");
        assert!(
            families
                .iter()
                .all(|family| family.name() != "llm_d_epp_average_queue_size"),
            "only provider series"
        );
    }
}
