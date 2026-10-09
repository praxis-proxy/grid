//! The JSON API, health probes, and metrics.

use std::{collections::HashMap, sync::Arc};

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{CACHE_CONTROL, CONTENT_TYPE},
    },
    response::{IntoResponse as _, Response},
    routing::{any, get},
};
use prometheus::{Encoder as _, TextEncoder};
use serde::Serialize;
use tokio::sync::watch;

use super::{Assets, assets, sse};
use crate::{collector::Collector, model::Config};

/// What every handler can reach.
#[derive(Clone)]
pub(super) struct AppState {
    /// The fleet.
    pub(super) collector: Arc<Collector>,
    /// Static configuration the SPA reads once.
    pub(super) config: Config,
    /// The embedded web UI.
    pub(super) assets: Assets,
    /// Turns true when the server is shutting down, so streams end.
    pub(super) shutting_down: watch::Receiver<bool>,
}

/// The whole HTTP surface. Anything under `/api/` that is not an endpoint is
/// a JSON 404; anything else is the SPA. Open streams end when `shutting_down`
/// turns true, so a graceful shutdown can finish.
pub fn router(
    collector: Arc<Collector>,
    config: Config,
    assets: Assets,
    shutting_down: watch::Receiver<bool>,
) -> Router {
    Router::new()
        .route("/api/v1/config", get(config_handler))
        .route("/api/v1/fleet", get(fleet))
        .route("/api/v1/sites/{name}", get(site))
        .route("/api/v1/series", get(series))
        .route("/api/v1/stream", get(sse::stream))
        .route("/api/{*rest}", any(api_not_found))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .fallback(assets::serve)
        .with_state(AppState {
            collector,
            config,
            assets,
            shutting_down,
        })
}

/// The static config plus the caller's identity from oauth-proxy. The state
/// holds a template; each response is its own copy, so one request's header
/// never leaks into another.
async fn config_handler(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let mut config = state.config;
    config.user = headers
        .get("x-forwarded-user")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|user| !user.is_empty())
        .map(str::to_owned);
    json(StatusCode::OK, &config)
}

/// The latest snapshot, or 503 until the first poll completes.
async fn fleet(State(state): State<AppState>) -> Response {
    match state.collector.snapshot() {
        Some(snapshot) => json(StatusCode::OK, &*snapshot),
        None => error(StatusCode::SERVICE_UNAVAILABLE, "first poll not complete"),
    }
}

/// One site with its sparkline.
async fn site(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    match state.collector.site_detail(&name) {
        Some(detail) => json(StatusCode::OK, &detail),
        None => error(StatusCode::NOT_FOUND, "site not found"),
    }
}

/// The fleet series for `?range=`, defaulting to one hour.
async fn series(State(state): State<AppState>, Query(params): Query<HashMap<String, String>>) -> Response {
    let range = params.get("range").map_or("1h", String::as_str);
    match state.collector.fleet_series(range).await {
        Ok(series) => json(StatusCode::OK, &series),
        Err(err) => error(StatusCode::BAD_REQUEST, &err.to_string()),
    }
}

/// Unknown API paths never fall through to the SPA.
async fn api_not_found() -> Response {
    error(StatusCode::NOT_FOUND, "not found")
}

/// Liveness: the process serves requests.
async fn healthz() -> StatusCode {
    StatusCode::OK
}

/// Readiness: the first poll has completed.
async fn readyz(State(state): State<AppState>) -> StatusCode {
    if state.collector.snapshot().is_some() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// The dashboard's own metrics in the Prometheus text format.
async fn metrics(State(state): State<AppState>) -> Response {
    let encoder = TextEncoder::new();
    let mut body = Vec::new();
    match encoder.encode(&state.collector.metrics().registry.gather(), &mut body) {
        Ok(()) => (
            StatusCode::OK,
            [(CONTENT_TYPE, HeaderValue::from_static(prometheus::TEXT_FORMAT))],
            body,
        )
            .into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    }
}

/// A JSON response that is never cached.
fn json<T: Serialize>(status: StatusCode, value: &T) -> Response {
    let mut response = (status, axum::Json(value)).into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// A JSON error of the shape the SPA expects.
fn error(status: StatusCode, message: &str) -> Response {
    json(status, &serde_json::json!({ "error": message }))
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
#[expect(clippy::indexing_slicing, reason = "test assertions on known JSON structure")]
mod tests {
    use http::StatusCode;
    use tower::ServiceExt as _;

    use crate::api::testing::{app, body_bytes, body_json, get, get_as};

    #[tokio::test]
    async fn config_reports_the_interval_version_and_thresholds_with_no_user() {
        let (app, _collector) = app(true).await;
        let response = app.oneshot(get("/api/v1/config")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["cache-control"],
            "no-store",
            "API responses are never cached"
        );
        let config = body_json(response).await;
        assert_eq!(
            (config["pollIntervalSeconds"].as_u64(), config["version"].as_str()),
            (Some(15), Some("test"))
        );
        assert_eq!(config["thresholds"]["gpuUtilWarn"], 90.0, "{config}");
        assert_eq!(
            config["user"],
            serde_json::Value::Null,
            "no proxy header means null, never an empty string"
        );
    }

    #[tokio::test]
    async fn the_forwarded_user_is_reflected_trimmed_and_never_leaks() {
        let (app, _collector) = app(true).await;
        let named = body_json(app.clone().oneshot(get_as("/api/v1/config", " alice ")).await.unwrap()).await;
        assert_eq!(named["user"], "alice", "the proxied identity is reflected, trimmed");
        let anonymous = body_json(app.oneshot(get("/api/v1/config")).await.unwrap()).await;
        assert_eq!(
            anonymous["user"],
            serde_json::Value::Null,
            "a later request without the header sees no user"
        );
    }

    #[tokio::test]
    async fn fleet_returns_the_snapshot_after_a_poll() {
        let (app, _collector) = app(true).await;
        let response = app.oneshot(get("/api/v1/fleet")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let fleet = body_json(response).await;
        assert_eq!(fleet["sites"].as_array().map(Vec::len), Some(1), "{fleet}");
        assert_eq!(fleet["sites"][0]["name"], "spoke1");
    }

    #[tokio::test]
    async fn fleet_and_readyz_are_503_before_the_first_poll_but_healthz_is_200() {
        let (app, _collector) = app(false).await;
        let fleet = app.clone().oneshot(get("/api/v1/fleet")).await.unwrap();
        assert_eq!(
            fleet.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "no snapshot means 503, not an empty fleet"
        );
        let ready = app.clone().oneshot(get("/readyz")).await.unwrap();
        assert_eq!(
            ready.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "not ready until the first poll"
        );
        let live = app.oneshot(get("/healthz")).await.unwrap();
        assert_eq!(live.status(), StatusCode::OK, "liveness never depends on the registry");
    }

    #[tokio::test]
    async fn readyz_is_200_after_the_first_poll() {
        let (app, _collector) = app(true).await;
        assert_eq!(app.oneshot(get("/readyz")).await.unwrap().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn site_detail_is_served_and_unknown_sites_are_404_json() {
        let (app, _collector) = app(true).await;
        let detail = body_json(app.clone().oneshot(get("/api/v1/sites/spoke1")).await.unwrap()).await;
        assert_eq!(
            (detail["name"].as_str(), detail["series"]["step"].as_u64()),
            (Some("spoke1"), Some(15)),
            "{detail}"
        );
        let missing = app.oneshot(get("/api/v1/sites/nope")).await.unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(missing).await["error"], "site not found");
    }

    #[tokio::test]
    async fn series_accepts_known_ranges_and_rejects_others() {
        let (app, _collector) = app(true).await;
        let series = app.clone().oneshot(get("/api/v1/series?range=1h")).await.unwrap();
        assert_eq!(series.status(), StatusCode::OK);
        assert_eq!(body_json(series).await["step"], 30);
        let bad = app.oneshot(get("/api/v1/series?range=2h")).await.unwrap();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_json(bad).await["error"].as_str().unwrap().contains("range"),
            "the error names the parameter"
        );
    }

    #[tokio::test]
    async fn series_defaults_to_one_hour() {
        let (app, _collector) = app(true).await;
        let series = body_json(app.oneshot(get("/api/v1/series")).await.unwrap()).await;
        assert_eq!(series["range"], "1h", "{series}");
    }

    #[tokio::test]
    async fn metrics_exposes_the_registry_in_text_format() {
        let (app, _collector) = app(true).await;
        let response = app.oneshot(get("/metrics")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let text = String::from_utf8(body_bytes(response).await).unwrap();
        assert!(text.contains("fleet_dashboard_poll_duration_seconds"), "{text}");
    }

    #[tokio::test]
    async fn an_unknown_api_path_is_a_json_404_not_the_spa() {
        let (app, _collector) = app(true).await;
        let response = app.oneshot(get("/api/v1/nope")).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "unknown /api/ paths must not fall through to index.html"
        );
        assert_eq!(body_json(response).await["error"], "not found");
    }
}
