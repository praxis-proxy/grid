//! Every grid metric carries help text in a rendered scrape.
//!
//! The `metrics` crate drops a description sent to the no-op recorder, so this
//! asserts the description survives the recorder the gateway installs.

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    /// Names `describe_metrics` documents, which a scrape must explain.
    const DOCUMENTED: [&str; 21] = [
        "grid_route_decisions_total",
        "grid_route_selections_total",
        "grid_route_prefix_affinity_total",
        "grid_serving_config_reload_total",
        "grid_route_site_rho",
        "grid_route_site_rho_reported",
        "grid_route_site_rho_counted",
        "grid_route_site_weight",
        "grid_route_site_ceiling",
        "grid_route_site_ceiling_soft",
        "grid_route_site_ceiling_hard",
        "grid_route_site_in_flight",
        "grid_route_site_full",
        "grid_route_site_reading_age_seconds",
        "grid_route_site_score",
        "grid_route_shedding",
        "grid_signals_poll_total",
        "grid_signals_ingest_dropped_total",
        "grid_signals_feedback_failures_total",
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
        metrics::gauge!("grid_route_site_rho_reported", "site" => "east").set(0.5);
        metrics::gauge!("grid_route_site_rho_counted", "site" => "east").set(0.5);
        metrics::gauge!("grid_route_site_weight", "site" => "east").set(1.0);
        metrics::gauge!("grid_route_site_ceiling", "site" => "east").set(8.0);
        metrics::gauge!("grid_route_site_ceiling_soft", "site" => "east").set(8.0);
        metrics::gauge!("grid_route_site_ceiling_hard", "site" => "east").set(8.0);
        metrics::gauge!("grid_route_site_in_flight", "site" => "east").set(4.0);
        metrics::gauge!("grid_route_site_full", "site" => "east").set(0.0);
        metrics::gauge!("grid_route_site_reading_age_seconds", "site" => "east").set(1.0);
        metrics::gauge!("grid_route_site_score", "site" => "east").set(0.25);
        metrics::gauge!("grid_route_shedding", "model" => "llama").set(0.0);
        metrics::counter!("grid_signals_poll_total", "result" => "ok").increment(1);
        metrics::counter!("grid_signals_ingest_dropped_total", "reason" => "skew").increment(1);
        metrics::counter!("grid_signals_feedback_failures_total", "reason" => "status").increment(1);
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

    /// Every grid metric the decider emits is named above.
    ///
    /// The test before this one proves only what `DOCUMENTED` lists, so a metric added to the
    /// decider and to neither list renders with no help text and nothing fails. This reads the
    /// emit sites out of the source instead.
    #[test]
    fn every_emitted_grid_metric_is_documented() {
        const SOURCES: [&str; 3] = [
            include_str!("../crates/ai-grid-filters/src/snapshot.rs"),
            include_str!("../crates/ai-grid-filters/src/signals.rs"),
            include_str!("../crates/ai-grid-filters/src/decisions.rs"),
        ];

        let mut emitted: Vec<&str> = SOURCES
            .iter()
            .flat_map(|source| source.split('"').filter(|part| part.starts_with("grid_")))
            .filter(|name| {
                name.bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_' || byte.is_ascii_digit())
            })
            .collect();
        emitted.sort_unstable();
        emitted.dedup();
        assert!(
            emitted.len() >= 10,
            "found only {emitted:?}, the scan is not reading the source"
        );

        for name in emitted {
            assert!(DOCUMENTED.contains(&name), "{name} is emitted but carries no help text");
        }
    }
}
