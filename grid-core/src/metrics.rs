//! Runtime metrics for inference backends.

// ---------------------------------------------------------------------------
// Backend Metrics
// ---------------------------------------------------------------------------

/// Runtime metrics for a single inference backend.
///
/// Populated from Prometheus (local backends), CRDT state
/// (remote backends), or response headers (API providers).
#[derive(Clone, Copy, Debug)]
pub struct BackendMetrics {
    /// Request error rate (0.0 to 1.0).
    pub error_rate: f64,

    /// Whether the backend is healthy and accepting requests.
    pub healthy: bool,

    /// KV cache utilization (0.0 to 1.0).
    pub kv_cache_utilization: f64,

    /// Observed P99 latency in milliseconds.
    pub latency_p99_ms: f64,

    /// Prefix cache hit ratio (0.0 to 1.0).
    pub prefix_cache_hit_ratio: f64,

    /// Current queue depth (normalized 0.0 to 1.0).
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

    /// Creates default metrics for a healthy backend with no
    /// observations.
    #[must_use]
    pub fn healthy_default() -> Self {
        Self {
            error_rate: 0.0,
            healthy: true,
            kv_cache_utilization: 0.0,
            latency_p99_ms: 0.0,
            prefix_cache_hit_ratio: 0.0,
            queue_depth: 0.0,
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
        assert_eq!(m.latency_p99_ms, 0.0, "default latency");
        assert_eq!(m.queue_depth, 0.0, "default queue depth");
        assert_eq!(m.kv_cache_utilization, 0.0, "default kv cache");
        assert_eq!(m.prefix_cache_hit_ratio, 0.0, "default prefix cache");
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
