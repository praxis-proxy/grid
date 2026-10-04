//! The parity gate: the Rust demo fleet must answer the API exactly as the Go
//! implementation did, as captured in `tests/fixtures/`.
//!
//! Numbers are compared as `f64` within a relative tolerance, because Go
//! prints `66` where serde prints `66.0` and `math.Sin` may differ from
//! `f64::sin` in the last bit; neither is visible to the frontend. Everything
//! else, including which keys exist and which values are `null`, is exact.

#![expect(clippy::unwrap_used, reason = "tests")]
#![expect(clippy::indexing_slicing, reason = "test assertions on known JSON structure")]

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use axum::{Router, body::Body};
    use fleet_dashboard::{
        api::{Assets, router},
        collector::{Collector, Metrics, Options},
        demo::sites,
        metrics::Source,
        model::{Config, Thresholds},
        queries::QuerySet,
    };
    use http::Request;
    use http_body_util::BodyExt as _;
    use serde_json::Value;
    use time::macros::datetime;
    use tokio::sync::watch;
    use tower::ServiceExt as _;

    /// The instant the Go fixtures were captured at.
    const CAPTURED_AT: time::OffsetDateTime = datetime!(2026-01-01 00:00:00 UTC);

    #[tokio::test]
    async fn config_matches_the_go_fixture() {
        let app = demo_app().await;
        let mut actual = get(&app, "/api/v1/config").await;
        let mut expected = fixture("config.json");
        actual["version"] = Value::Null;
        expected["version"] = Value::Null;
        assert_same(&actual, &expected, "$");
    }

    #[tokio::test]
    async fn fleet_matches_the_go_fixture() {
        let app = demo_app().await;
        assert_same(&get(&app, "/api/v1/fleet").await, &fixture("fleet.json"), "$");
    }

    #[tokio::test]
    async fn site_details_match_the_go_fixtures() {
        let app = demo_app().await;
        assert_same(
            &get(&app, "/api/v1/sites/frankfurt").await,
            &fixture("site-frankfurt.json"),
            "$",
        );
        assert_same(
            &get(&app, "/api/v1/sites/singapore").await,
            &fixture("site-singapore.json"),
            "$",
        );
    }

    #[tokio::test]
    async fn the_one_hour_series_matches_the_go_fixture() {
        let app = demo_app().await;
        assert_same(
            &get(&app, "/api/v1/series?range=1h").await,
            &fixture("series-1h.json"),
            "$",
        );
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// The demo fleet polled once at the capture instant, behind the router.
    async fn demo_app() -> Router {
        let (_sender, receiver) = watch::channel(Arc::new(sites()));
        let collector = Arc::new(Collector::new(Options {
            sites: receiver,
            source: Source::Demo,
            queries: QuerySet::defaults().unwrap(),
            thresholds: fleet_dashboard::queries::Thresholds::default(),
            hub: None,
            interval: Duration::from_secs(15),
            site_timeout: Duration::from_secs(8),
            metrics: Arc::new(Metrics::new().unwrap()),
            clock: Arc::new(|| CAPTURED_AT),
        }));
        collector.poll().await;
        router(
            collector,
            api_config(),
            Assets::new(&[("index.html", b"<html></html>")]),
            watch::channel(false).1,
        )
    }

    /// The config the Go capture ran with; its version is masked in the test.
    fn api_config() -> Config {
        Config {
            hub: None,
            poll_interval_seconds: 15,
            version: "test".to_owned(),
            thresholds: Thresholds {
                gpu_util_warn: 90.0,
                queue_warn: 50.0,
                latency_warn_ms: 5000.0,
            },
            user: None,
        }
    }

    fn fixture(name: &str) -> Value {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/");
        serde_json::from_str(&std::fs::read_to_string(format!("{path}{name}")).unwrap()).unwrap()
    }

    async fn get(app: &Router, uri: &str) -> Value {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), 200, "{uri}");
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
    }

    /// Structural equality with tolerant numbers; `path` names the mismatch.
    fn assert_same(actual: &Value, expected: &Value, path: &str) {
        match (actual, expected) {
            (Value::Object(left), Value::Object(right)) => {
                let (mut left_keys, mut right_keys): (Vec<_>, Vec<_>) = (left.keys().collect(), right.keys().collect());
                left_keys.sort();
                right_keys.sort();
                assert_eq!(left_keys, right_keys, "{path}: key sets differ");
                for (key, value) in left {
                    assert_same(value, &right[key], &format!("{path}.{key}"));
                }
            },
            (Value::Array(left), Value::Array(right)) => {
                assert_eq!(left.len(), right.len(), "{path}: array lengths differ");
                for (index, (value, want)) in left.iter().zip(right).enumerate() {
                    assert_same(value, want, &format!("{path}[{index}]"));
                }
            },
            (Value::Number(left), Value::Number(right)) => {
                let (left, right) = (left.as_f64().unwrap(), right.as_f64().unwrap());
                let scale = left.abs().max(right.abs()).max(1.0);
                assert!((left - right).abs() <= scale * 1e-9, "{path}: {left} != {right}");
            },
            _ => assert_eq!(actual, expected, "{path}"),
        }
    }
}
