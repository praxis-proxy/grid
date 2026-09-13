//! Prometheus metrics for gateway probe observability.
//!
//! All label values are bounded enum variants — no site names,
//! addresses, fingerprints, or PEM content.

use std::{sync::LazyLock, time::Duration};

use prometheus::{
    Encoder as _, Histogram, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder, proto::MetricFamily,
};

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Global registry for operator metrics.
static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    let r = Registry::new();
    r.register(Box::new(PROBE_TOTAL.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PROBE_DURATION.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PHASE_TRANSITIONS.clone()))
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
    r
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

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Record a completed gateway probe.
pub(crate) fn record_probe(outcome: &str, tls_mode: &str, duration: Duration) {
    PROBE_TOTAL.with_label_values(&[outcome, tls_mode]).inc();
    PROBE_DURATION.observe(duration.as_secs_f64());
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

/// Gather all registered metrics for serialization.
pub(crate) fn gather_metrics() -> Vec<MetricFamily> {
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
    fn record_probe_increments_counter() {
        record_probe("Verified", "mtls", Duration::from_millis(42));
        let val = PROBE_TOTAL.with_label_values(&["Verified", "mtls"]).get();
        assert!(val >= 1, "probe counter should be >= 1, got {val}");
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
}
