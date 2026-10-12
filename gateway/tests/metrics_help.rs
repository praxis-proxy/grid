//! Every grid metric carries help text in a rendered scrape.
//!
//! The `metrics` crate drops a description sent to the no-op recorder, so this
//! asserts the description survives the recorder the gateway installs.

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    /// Names `describe_metrics` documents, which a scrape must explain.
    const DOCUMENTED: [&str; 13] = [
        "grid_route_decisions_total",
        "grid_route_selections_total",
        "grid_route_prefix_affinity_total",
        "grid_serving_config_reload_total",
        "grid_route_site_rho",
        "grid_route_site_weight",
        "grid_route_site_ceiling",
        "grid_route_site_score",
        "grid_route_shedding",
        "grid_signals_poll_total",
        "grid_signals_ingest_dropped_total",
        "grid_signals_last_success_timestamp_seconds",
        "grid_signals_response_bytes",
    ];

    #[test]
    fn every_grid_metric_renders_help_text() {
        praxis_protocol::http::pingora::metrics::install_prometheus_recorder();
        ai_grid_filters::describe_metrics();

        metrics::counter!("grid_route_decisions_total").increment(1);
        metrics::counter!("grid_route_selections_total", "path" => "cross_site").increment(1);
        metrics::counter!("grid_route_prefix_affinity_total", "outcome" => "hit").increment(1);
        metrics::counter!("grid_serving_config_reload_total", "result" => "ok").increment(1);
        metrics::gauge!("grid_route_site_rho", "site" => "east").set(0.5);
        metrics::gauge!("grid_route_site_weight", "site" => "east").set(1.0);
        metrics::gauge!("grid_route_site_ceiling", "site" => "east").set(8.0);
        metrics::gauge!("grid_route_site_score", "site" => "east").set(0.25);
        metrics::gauge!("grid_route_shedding", "model" => "llama").set(0.0);
        metrics::counter!("grid_signals_poll_total", "result" => "ok").increment(1);
        metrics::counter!("grid_signals_ingest_dropped_total", "reason" => "skew").increment(1);
        metrics::gauge!("grid_signals_last_success_timestamp_seconds").set(1.0);
        metrics::gauge!("grid_signals_response_bytes").set(1.0);

        let text = praxis_protocol::http::pingora::metrics::render_prometheus().expect("recorder installed");
        for name in DOCUMENTED {
            assert!(
                text.contains(&format!("# HELP {name} ")),
                "{name} rendered without help text"
            );
        }
    }
}
