// SPDX-License-Identifier: Apache-2.0

//! Prometheus metrics for the overlay sync sidecar.
//!
//! All label values are bounded strings — never unbounded revision
//! hashes, resource versions, or user-supplied identifiers.

use prometheus::{Gauge, IntCounterVec, IntGauge, Opts, Registry, TextEncoder};

// ---------------------------------------------------------------------------
// Metrics registry
// ---------------------------------------------------------------------------

/// All sidecar metrics.
pub(crate) struct Metrics {
    /// Overlay events by outcome and reason.
    pub(crate) events_total: IntCounterVec,
    /// Successful file writes by outcome.
    pub(crate) file_writes_total: IntCounterVec,
    /// Validation failures by reason.
    pub(crate) validation_failures_total: IntCounterVec,
    /// Watch reconnections by reason.
    pub(crate) watch_reconnects_total: IntCounterVec,
    /// Whether the sidecar is ready (1) or not (0).
    pub(crate) ready: IntGauge,
    /// Whether the sidecar is degraded (1) or not (0).
    pub(crate) degraded: IntGauge,
    /// Timestamp of the last observed `ConfigMap` event.
    pub(crate) last_observed_timestamp: Gauge,
    /// Timestamp of the last successful file write.
    pub(crate) last_write_timestamp: Gauge,
    /// Size of the last written payload in bytes.
    pub(crate) payload_bytes: Gauge,
    /// Internal Prometheus registry.
    registry: Registry,
}

impl Metrics {
    /// Create and register all metrics.
    ///
    /// # Panics
    ///
    /// Panics if metric registration fails (programming error).
    pub(crate) fn new() -> Self {
        let registry = Registry::new();
        let (events_total, file_writes_total) = register_event_counters(&registry);
        let (validation_failures_total, watch_reconnects_total) = register_diag_counters(&registry);
        let (ready, degraded) = register_state_gauges(&registry);
        let (last_observed_timestamp, last_write_timestamp, payload_bytes) = register_value_gauges(&registry);

        Self {
            events_total,
            file_writes_total,
            validation_failures_total,
            watch_reconnects_total,
            ready,
            degraded,
            last_observed_timestamp,
            last_write_timestamp,
            payload_bytes,
            registry,
        }
    }

    /// Encode all metrics as Prometheus text format.
    pub(crate) fn encode(&self) -> String {
        let encoder = TextEncoder::new();
        let families = self.registry.gather();
        encoder.encode_to_string(&families).unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Registration helpers
// ---------------------------------------------------------------------------

/// Register event and file-write counters.
#[expect(
    clippy::expect_used,
    reason = "metric registration is infallible for fresh registries"
)]
fn register_event_counters(registry: &Registry) -> (IntCounterVec, IntCounterVec) {
    let events_total = IntCounterVec::new(
        Opts::new("grid_overlay_sync_events_total", "Overlay events by outcome and reason"),
        &["outcome", "reason"],
    )
    .expect("metric creation");
    registry
        .register(Box::new(events_total.clone()))
        .expect("metric registration");

    let file_writes_total = IntCounterVec::new(
        Opts::new("grid_overlay_sync_file_writes_total", "File writes by outcome"),
        &["outcome"],
    )
    .expect("metric creation");
    registry
        .register(Box::new(file_writes_total.clone()))
        .expect("metric registration");

    (events_total, file_writes_total)
}

/// Register validation and reconnection counters.
#[expect(
    clippy::expect_used,
    reason = "metric registration is infallible for fresh registries"
)]
fn register_diag_counters(registry: &Registry) -> (IntCounterVec, IntCounterVec) {
    let validation_failures_total = IntCounterVec::new(
        Opts::new(
            "grid_overlay_sync_validation_failures_total",
            "Validation failures by reason",
        ),
        &["reason"],
    )
    .expect("metric creation");
    registry
        .register(Box::new(validation_failures_total.clone()))
        .expect("metric registration");

    let watch_reconnects_total = IntCounterVec::new(
        Opts::new(
            "grid_overlay_sync_watch_reconnects_total",
            "Watch reconnections by reason",
        ),
        &["reason"],
    )
    .expect("metric creation");
    registry
        .register(Box::new(watch_reconnects_total.clone()))
        .expect("metric registration");

    (validation_failures_total, watch_reconnects_total)
}

/// Register state gauge metrics.
#[expect(
    clippy::expect_used,
    reason = "metric registration is infallible for fresh registries"
)]
fn register_state_gauges(registry: &Registry) -> (IntGauge, IntGauge) {
    let ready = IntGauge::new("grid_overlay_sync_ready", "Sidecar readiness").expect("metric creation");
    registry.register(Box::new(ready.clone())).expect("metric registration");

    let degraded = IntGauge::new("grid_overlay_sync_degraded", "Sidecar degraded state").expect("metric creation");
    registry
        .register(Box::new(degraded.clone()))
        .expect("metric registration");

    (ready, degraded)
}

/// Register value gauge metrics.
#[expect(
    clippy::expect_used,
    reason = "metric registration is infallible for fresh registries"
)]
fn register_value_gauges(registry: &Registry) -> (Gauge, Gauge, Gauge) {
    let last_observed_timestamp = Gauge::new(
        "grid_overlay_sync_last_observed_timestamp_seconds",
        "Timestamp of last observed ConfigMap event",
    )
    .expect("metric creation");
    registry
        .register(Box::new(last_observed_timestamp.clone()))
        .expect("metric registration");

    let last_write_timestamp = Gauge::new(
        "grid_overlay_sync_last_write_timestamp_seconds",
        "Timestamp of last successful file write",
    )
    .expect("metric creation");
    registry
        .register(Box::new(last_write_timestamp.clone()))
        .expect("metric registration");

    let payload_bytes = Gauge::new(
        "grid_overlay_sync_payload_bytes",
        "Size of last written payload in bytes",
    )
    .expect("metric creation");
    registry
        .register(Box::new(payload_bytes.clone()))
        .expect("metric registration");

    (last_observed_timestamp, last_write_timestamp, payload_bytes)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn metrics_register_without_panic() {
        let m = Metrics::new();
        m.events_total.with_label_values(&["accepted", "valid"]).inc();
        m.file_writes_total.with_label_values(&["success"]).inc();
        m.ready.set(1);
        let output = m.encode();
        assert!(output.contains("grid_overlay_sync_events_total"));
        assert!(output.contains("grid_overlay_sync_ready"));
    }
}
