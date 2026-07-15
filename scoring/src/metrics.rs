//! Runtime metrics for inference backends.

// ---------------------------------------------------------------------------
// Backend Metrics
// ---------------------------------------------------------------------------

/// Runtime metrics for a single inference backend.
///
/// This struct is the boundary between the metrics ingestion layer (Prometheus
/// scraping, CRDT propagation) and the scoring engine.  The scoring engine
/// trusts callers to supply values that satisfy the contract below; callers are
/// responsible for clamping or defaulting before constructing a `BackendMetrics`.
///
/// # Normalization contract
///
/// | Field | Expected range | Raw or pre-normalized | Who normalizes |
/// |---|---|---|---|
/// | `error_rate` | `[0.0, 1.0]` | Pre-normalized ratio | Prometheus exporter; clamped in `PartialMetrics::into_backend_metrics` and `crdt_metrics_to_backend` |
/// | `healthy` | `bool` | Derived | `PartialMetrics::into_backend_metrics` (from health gauge or `error_rate`); CRDT carries `bool` directly |
/// | `kv_cache_utilization` | `[0.0, 1.0]` | Pre-normalized ratio | Prometheus exporter; clamped in both ingestion paths |
/// | `latency_p99_ms` | `≥ 0.0 ms` | **Raw milliseconds** | Prometheus exporter exports a pre-computed P99 gauge; the scorer normalizes internally via `MAX_LATENCY = 5000 ms` |
/// | `prefix_cache_hit_ratio` | `[0.0, 1.0]` | Pre-normalized ratio | Prometheus exporter; clamped in the scorer |
/// | `queue_depth` | `[0.0, 1.0]` | Pre-normalized ratio | **Must be pre-normalized by the exporter**; raw queue counts are not accepted. Clamped in both ingestion paths. |
///
/// # Missing-value defaults
///
/// Callers that cannot observe a signal supply defaults that avoid penalizing
/// unmeasured providers:
/// - Scored ratio signals (`kv_cache_utilization`, `prefix_cache_hit_ratio`, `queue_depth`): `0.5`, producing a neutral
///   `0.5` signal score.
/// - `latency_p99_ms`: `2500.0 ms`, producing a neutral latency score of `0.5` (`1.0 - 2500/5000`).
/// - `error_rate`: `0.0`, meaning no observed errors.  `error_rate` is used for health derivation by ingestion code; it
///   is not a direct scoring term.
///
/// # NaN / Infinity
///
/// Prometheus scraping drops NaN and ±Inf at parse time (`.filter(|v| v.is_finite())`).
/// CRDT values are filtered and clamped in `crdt_metrics_to_backend`.  The scorer
/// itself does not re-check for NaN/Inf; callers must not propagate them into this
/// struct.
///
/// Populated from Prometheus (local backends), CRDT state (remote backends), or
/// synthetic defaults for unmeasured providers.
#[derive(Clone, Copy, Debug)]
pub struct BackendMetrics {
    /// Request error rate.  Pre-normalized to `[0.0, 1.0]`.
    pub error_rate: f64,

    /// Whether the backend is healthy and accepting requests.
    pub healthy: bool,

    /// KV cache utilization.  Pre-normalized to `[0.0, 1.0]`.
    pub kv_cache_utilization: f64,

    /// Observed P99 request latency in milliseconds.  Raw value; the scorer
    /// normalizes internally using `MAX_LATENCY = 5000 ms`.
    pub latency_p99_ms: f64,

    /// Prefix cache hit ratio.  Pre-normalized to `[0.0, 1.0]`.
    pub prefix_cache_hit_ratio: f64,

    /// Normalized queue depth.  Pre-normalized to `[0.0, 1.0]`; raw queue
    /// counts must be converted to a ratio by the Prometheus exporter before
    /// they reach this field.
    pub queue_depth: f64,
}

impl BackendMetrics {
    /// Creates a new metrics snapshot with all signals.
    #[must_use]
    #[expect(clippy::too_many_arguments, reason = "flat metrics struct")]
    pub fn new(
        error_rate: f64,
        healthy: bool,
        kv_cache_utilization: f64,
        latency_p99_ms: f64,
        prefix_cache_hit_ratio: f64,
        queue_depth: f64,
    ) -> Self {
        Self {
            error_rate,
            healthy,
            kv_cache_utilization,
            latency_p99_ms,
            prefix_cache_hit_ratio,
            queue_depth,
        }
    }

    /// Creates neutral metrics for a healthy backend with no observations.
    #[must_use]
    pub fn healthy_default() -> Self {
        Self {
            error_rate: 0.0,
            healthy: true,
            kv_cache_utilization: 0.5,
            latency_p99_ms: 2500.0,
            prefix_cache_hit_ratio: 0.5,
            queue_depth: 0.5,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn healthy_default_values() {
        let m = BackendMetrics::healthy_default();
        assert!(m.healthy, "default should be healthy");
        assert_eq!(m.error_rate, 0.0, "default error rate");
        assert_eq!(m.latency_p99_ms, 2500.0, "default latency");
        assert_eq!(m.queue_depth, 0.5, "default queue depth");
        assert_eq!(m.kv_cache_utilization, 0.5, "default kv cache");
        assert_eq!(m.prefix_cache_hit_ratio, 0.5, "default prefix cache");
    }

    #[test]
    fn custom_metrics() {
        let m = BackendMetrics::new(0.05, true, 0.6, 150.0, 0.8, 0.3);
        assert!(m.healthy, "should be healthy");
        assert_eq!(m.error_rate, 0.05, "error rate mismatch");
        assert_eq!(m.kv_cache_utilization, 0.6, "kv cache mismatch");
        assert_eq!(m.latency_p99_ms, 150.0, "latency mismatch");
        assert_eq!(m.prefix_cache_hit_ratio, 0.8, "prefix cache mismatch");
        assert_eq!(m.queue_depth, 0.3, "queue depth mismatch");
    }
}
