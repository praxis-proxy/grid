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

    /// Observed P99 latency in milliseconds.
    pub latency_p99_ms: f64,
}

impl BackendMetrics {
    /// Creates a new metrics snapshot.
    #[must_use]
    pub fn new(error_rate: f64, healthy: bool, latency_p99_ms: f64) -> Self {
        Self {
            error_rate,
            healthy,
            latency_p99_ms,
        }
    }

    /// Creates default metrics for a healthy backend with no
    /// observations.
    #[must_use]
    pub fn healthy_default() -> Self {
        Self {
            error_rate: 0.0,
            healthy: true,
            latency_p99_ms: 0.0,
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
    }

    #[test]
    fn custom_metrics() {
        let m = BackendMetrics::new(0.05, true, 150.0);
        assert!(m.healthy, "should be healthy");
        assert_eq!(m.error_rate, 0.05, "error rate mismatch");
        assert_eq!(m.latency_p99_ms, 150.0, "latency mismatch");
    }
}
