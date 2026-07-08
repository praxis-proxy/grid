//! Mock Google `Vertex` AI `generateContent` API server.
//!
//! Implements regional endpoint pattern:
//! `POST /v1/projects/{project}/locations/{location}/publishers/google/models/{model}:generateContent`
//! `POST /v1/projects/{project}/locations/{location}/publishers/google/models/{model}:streamGenerateContent`
//!
//! Validates `OAuth2` bearer token presence.

use axum::{
    Router,
    body::Body,
    http::{Request, Response, StatusCode},
    routing::{get, post},
};
use serde_json::{Value, json};

use crate::common;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the `Vertex` AI mock router.
///
/// Uses a wildcard route because axum does not allow colons in
/// path parameters (`{model}:generateContent` is invalid). The
/// handler inspects the path suffix to dispatch.
pub(crate) fn router() -> Router {
    Router::new()
        .route(
            "/v1/projects/{project}/locations/{location}/publishers/google/models/{*rest}",
            post(dispatch),
        )
        .route("/health", get(common::health_ok))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Dispatch based on the wildcard path suffix.
async fn dispatch(req: Request<Body>) -> Response<Body> {
    if common::extract_bearer(req.headers()).is_none() {
        return oauth2_unauthorized();
    }
    let path = req.uri().path();
    if path.ends_with(":streamGenerateContent") {
        streaming_response()
    } else if path.ends_with(":generateContent") {
        non_streaming_response()
    } else {
        common::json_response(
            StatusCode::NOT_FOUND,
            &json!({"error": {"code": 404, "message": "unknown method"}}),
        )
    }
}

/// Build a 401 response for missing `OAuth2` bearer token.
fn oauth2_unauthorized() -> Response<Body> {
    common::json_response(
        StatusCode::UNAUTHORIZED,
        &json!({
            "error": {
                "code": 401,
                "message": "Request had invalid authentication credentials.",
                "status": "UNAUTHENTICATED",
            }
        }),
    )
}

// ---------------------------------------------------------------------------
// Response builders
// ---------------------------------------------------------------------------

/// Build a non-streaming `generateContent` response.
fn non_streaming_response() -> Response<Body> {
    common::json_response(
        StatusCode::OK,
        &json!({
            "candidates": [{
                "content": {
                    "parts": [{"text": "This is a mock response."}],
                    "role": "model",
                },
                "finishReason": "STOP",
            }],
            "usageMetadata": {
                "promptTokenCount": 10,
                "candidatesTokenCount": 6,
                "totalTokenCount": 16,
            },
        }),
    )
}

/// Build an SSE streaming `streamGenerateContent` response.
fn streaming_response() -> Response<Body> {
    let chunks = streaming_chunks();
    common::sse_response(&chunks, None)
}

/// Build the chunk sequence for a streaming response.
fn streaming_chunks() -> Vec<Value> {
    vec![
        json!({
            "candidates": [{"content": {"parts": [{"text": "This is "}], "role": "model"}}],
        }),
        json!({
            "candidates": [{"content": {"parts": [{"text": "a mock response."}], "role": "model"}, "finishReason": "STOP"}],
            "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 6, "totalTokenCount": 16},
        }),
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use axum::{body::to_bytes, http::header};

    use super::*;

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    /// Send a request to the router and return the response.
    async fn send(req: Request<Body>) -> Response<Body> {
        tower::ServiceExt::oneshot(router(), req).await.unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap_or_default()
        })
    }

    const GENERATE_URL: &str =
        "/v1/projects/test-project/locations/us-central1/publishers/google/models/gemini-pro:generateContent";
    const STREAM_URL: &str =
        "/v1/projects/test-project/locations/us-central1/publishers/google/models/gemini-pro:streamGenerateContent";

    fn bearer_header() -> (http::HeaderName, &'static str) {
        (header::AUTHORIZATION, "Bearer ya29.test-oauth2-token")
    }

    #[tokio::test]
    async fn non_streaming_generate_content() {
        let req = Request::builder()
            .method("POST")
            .uri(GENERATE_URL)
            .header(bearer_header().0.clone(), bearer_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"contents":[{"parts":[{"text":"hi"}]}]}"#))
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::OK, "should return 200");

        let body = to_bytes(resp.into_body(), 65_536).await.unwrap_or_default();
        let json: Value = serde_json::from_slice(&body).unwrap_or_default();
        assert!(json.get("candidates").is_some(), "should have candidates");
        assert!(json.get("usageMetadata").is_some(), "should have usageMetadata");
    }

    #[tokio::test]
    async fn streaming_generate_content() {
        let req = Request::builder()
            .method("POST")
            .uri(STREAM_URL)
            .header(bearer_header().0.clone(), bearer_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"contents":[{"parts":[{"text":"hi"}]}]}"#))
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::OK, "should return 200");

        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert_eq!(ct, "text/event-stream", "should be SSE");

        let body = to_bytes(resp.into_body(), 65_536).await.unwrap_or_default();
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("data: "), "should contain SSE data lines");
        assert!(text.contains("candidates"), "should contain candidates");
    }

    #[tokio::test]
    async fn missing_bearer_returns_401() {
        let req = Request::builder()
            .method("POST")
            .uri(GENERATE_URL)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"contents":[]}"#))
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "should return 401");
    }

    #[tokio::test]
    async fn health_endpoint() {
        let req = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::OK, "health should return 200");
    }
}
