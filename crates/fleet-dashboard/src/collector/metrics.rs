//! The dashboard's own Prometheus metrics, in a registry of their own so the
//! binary has no global mutable state and tests do not share one.

use prometheus::{GaugeVec, Histogram, HistogramOpts, IntCounterVec, Opts, Registry};

/// Handles for every metric the dashboard exposes on `/metrics`.
#[derive(Debug, Clone)]
pub struct Metrics {
    /// Gather from here to serve `/metrics`.
    pub registry: Registry,
    /// Time spent collecting one full fleet poll across every site.
    pub poll_duration: Histogram,
    /// 1 if the most recent poll of a site succeeded, 0 otherwise.
    pub site_reachable: GaugeVec,
    /// Queries that failed, by site and query key.
    pub query_failures: IntCounterVec,
}

impl Metrics {
    /// Creates and registers every metric.
    ///
    /// # Errors
    ///
    /// Only if a metric name is invalid or registered twice, which is a defect.
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        let poll_duration = Histogram::with_opts(HistogramOpts::new(
            "fleet_dashboard_poll_duration_seconds",
            "Time spent collecting one full fleet poll across every site.",
        ))?;
        let site_reachable = GaugeVec::new(
            Opts::new(
                "fleet_dashboard_site_reachable",
                "1 if the most recent poll of a site succeeded, 0 otherwise.",
            ),
            &["site"],
        )?;
        let query_failures = IntCounterVec::new(
            Opts::new(
                "fleet_dashboard_query_failures_total",
                "Count of PromQL queries that failed, by site and query key.",
            ),
            &["site", "key"],
        )?;
        registry.register(Box::new(poll_duration.clone()))?;
        registry.register(Box::new(site_reachable.clone()))?;
        registry.register(Box::new(query_failures.clone()))?;
        Ok(Self {
            registry,
            poll_duration,
            site_reachable,
            query_failures,
        })
    }

    /// Drops the reachability gauge of a site that left the registry, so it
    /// does not linger at its last value.
    pub fn forget_site(&self, site: &str) {
        // Err only means no gauge existed for this site, which is the desired state.
        self.site_reachable.remove_label_values(&[site]).unwrap_or_default();
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::Metrics;

    #[test]
    fn registers_three_families_and_forgets_a_pruned_site() {
        let metrics = Metrics::new().unwrap();
        metrics.site_reachable.with_label_values(&["a"]).set(1.0);
        metrics.query_failures.with_label_values(&["a", "rps"]).inc();
        metrics.poll_duration.observe(0.5);
        let names: Vec<String> = metrics
            .registry
            .gather()
            .iter()
            .map(|family| family.name().to_owned())
            .collect();
        assert_eq!(
            names,
            [
                "fleet_dashboard_poll_duration_seconds",
                "fleet_dashboard_query_failures_total",
                "fleet_dashboard_site_reachable"
            ],
            "every family is registered under the Go names"
        );
        metrics.forget_site("a");
        let reachable = metrics
            .registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "fleet_dashboard_site_reachable");
        assert!(
            reachable.as_ref().is_none_or(|family| family.get_metric().is_empty()),
            "no stale gauge: {reachable:?}"
        );
    }
}
