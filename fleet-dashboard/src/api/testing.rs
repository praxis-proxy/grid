#![expect(clippy::expect_used, reason = "test support")]
//! An application wired to one fake site, plus request and body helpers.

use std::sync::Arc;

use axum::{Router, body::Body, response::Response};
use http::Request;
use http_body_util::BodyExt as _;
use tokio::sync::watch;

use super::{Assets, router};
use crate::{
    collector::{
        Collector,
        testing::{INTERVAL, TIMEOUT, fleet, spoke},
    },
    metrics::testing::{fake_for, healthy_answers},
    model::{Config, Thresholds},
    queries::QuerySet,
};

/// A tiny asset table standing in for the Vite build.
pub(super) const ASSETS: &[(&str, &[u8])] = &[
    ("index.html", b"<html>spa</html>"),
    ("assets/app.js", b"console.log(1)"),
];

/// A router over a one-site fleet; `polled` decides whether a snapshot exists.
pub(super) async fn app(polled: bool) -> (Router, Arc<Collector>) {
    let (router, collector, _shutdown) = app_with_shutdown(polled).await;
    (router, collector)
}

/// Like [`app`], also returning the switch that tells the server to shut down.
pub(super) async fn app_with_shutdown(polled: bool) -> (Router, Arc<Collector>, watch::Sender<bool>) {
    let queries = QuerySet::defaults().expect("defaults");
    let spoke1 = spoke(
        "spoke1",
        "Ohio",
        "us-east-2",
        &fake_for(&queries, &healthy_answers()).serve().await,
    );
    let (collector, _sites) = fleet(vec![spoke1], queries, INTERVAL, TIMEOUT);
    let collector = Arc::new(collector);
    if polled {
        collector.poll().await;
    }
    let (shutdown, shutting_down) = watch::channel(false);
    (
        router(
            Arc::clone(&collector),
            test_config(),
            Assets::new(ASSETS),
            shutting_down,
        ),
        collector,
        shutdown,
    )
}

/// The static config every test app serves.
fn test_config() -> Config {
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

/// A GET request.
pub(super) fn get(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).expect("request")
}

/// A GET request carrying `X-Forwarded-User`.
pub(super) fn get_as(uri: &str, user: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header("x-forwarded-user", user)
        .body(Body::empty())
        .expect("request")
}

/// The whole response body.
pub(super) async fn body_bytes(response: Response) -> Vec<u8> {
    response.into_body().collect().await.expect("body").to_bytes().to_vec()
}

/// The response body decoded as JSON.
pub(super) async fn body_json(response: Response) -> serde_json::Value {
    serde_json::from_slice(&body_bytes(response).await).expect("json body")
}
