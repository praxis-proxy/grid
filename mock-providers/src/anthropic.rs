//! Mock `Anthropic` Messages API server.
//!
//! Implements `POST /v1/messages` (streaming and non-streaming).
//! Validates `x-api-key` header authentication.

use axum::{
    Router,
    body::Body,
    http::{Request, Response, StatusCode, header},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::common;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the `Anthropic` mock router.
pub(crate) fn router() -> Router {
    Router::new()
        .route("/v1/messages", post(messages))
        .route("/health", get(common::health_ok))
}

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// Minimal messages request body.
#[derive(Debug, Deserialize)]
struct MessagesRequest {
    /// Model name.
    #[serde(default)]
    model: String,

    /// Whether to stream the response.
    #[serde(default)]
    stream: bool,
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/// Extract the `x-api-key` header value.
fn extract_api_key(headers: &http::HeaderMap) -> Option<&str> {
    headers.get("x-api-key").and_then(|v| v.to_str().ok())
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Build a 401 response for missing `x-api-key`.
fn api_key_unauthorized() -> Response<Body> {
    common::json_response(
        StatusCode::UNAUTHORIZED,
        &json!({
            "type": "error",
            "error": {"type": "authentication_error", "message": "missing x-api-key header"}
        }),
    )
}

/// Handle `POST /v1/messages`.
async fn messages(req: Request<Body>) -> Response<Body> {
    if extract_api_key(req.headers()).is_none() {
        return api_key_unauthorized();
    }

    let body_bytes = axum::body::to_bytes(req.into_body(), 1_048_576)
        .await
        .unwrap_or_default();

    let msg_req: MessagesRequest = match serde_json::from_slice(&body_bytes) {
        Ok(r) => r,
        Err(_) => {
            return common::json_response(
                StatusCode::BAD_REQUEST,
                &json!({"type": "error", "error": {"type": "invalid_request_error"}}),
            );
        },
    };

    if msg_req.stream {
        streaming_response(&msg_req.model)
    } else {
        non_streaming_response(&msg_req.model)
    }
}

// ---------------------------------------------------------------------------
// Response builders
// ---------------------------------------------------------------------------

/// Build a non-streaming messages response.
fn non_streaming_response(model: &str) -> Response<Body> {
    common::json_response(
        StatusCode::OK,
        &json!({
            "id": "msg-mock-001",
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": [{
                "type": "text",
                "text": "This is a mock response.",
            }],
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 10,
                "output_tokens": 6,
            },
        }),
    )
}

/// Build an SSE streaming messages response.
///
/// Uses `Anthropic`-specific event types: `message_start`,
/// `content_block_start`, `content_block_delta`,
/// `content_block_stop`, and `message_stop`.
fn streaming_response(model: &str) -> Response<Body> {
    let events = streaming_events(model);
    let mut buf = String::new();
    for (event_type, data) in &events {
        buf.push_str("event: ");
        buf.push_str(event_type);
        buf.push('\n');
        let json = serde_json::to_string(data).unwrap_or_default();
        buf.push_str("data: ");
        buf.push_str(&json);
        buf.push_str("\n\n");
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from(buf))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap_or_default()
        })
}

/// Build the event sequence for a streaming response.
fn streaming_events(model: &str) -> Vec<(&'static str, Value)> {
    let msg_start = json!({
        "type": "message_start",
        "message": {"id": "msg-mock-001", "type": "message", "role": "assistant", "model": model},
    });
    let block_start = json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}});
    let delta = json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "This is a mock response."}});
    let block_stop = json!({"type": "content_block_stop", "index": 0});
    let msg_stop = json!({"type": "message_stop"});

    vec![
        ("message_start", msg_start),
        ("content_block_start", block_start),
        ("content_block_delta", delta),
        ("content_block_stop", block_stop),
        ("message_stop", msg_stop),
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;

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

    fn api_key_header() -> (&'static str, &'static str) {
        ("x-api-key", "sk-ant-test-key")
    }

    #[tokio::test]
    async fn non_streaming_messages() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .header(api_key_header().0, api_key_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"model":"claude-sonnet-4-20250514","max_tokens":1024,"messages":[{"role":"user","content":"hi"}]}"#,
            ))
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::OK, "should return 200");

        let body = to_bytes(resp.into_body(), 65_536).await.unwrap_or_default();
        let json: Value = serde_json::from_slice(&body).unwrap_or_default();
        assert_eq!(json.get("type").and_then(Value::as_str), Some("message"), "wrong type");
        assert_eq!(
            json.get("role").and_then(Value::as_str),
            Some("assistant"),
            "wrong role"
        );
        assert!(json.get("content").is_some(), "should have content");
        assert!(json.get("usage").is_some(), "should have usage");
    }

    #[tokio::test]
    async fn streaming_messages() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .header(api_key_header().0, api_key_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"model":"claude-sonnet-4-20250514","max_tokens":1024,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
            ))
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
        assert!(text.contains("event: message_start"), "should have message_start");
        assert!(
            text.contains("event: content_block_delta"),
            "should have content_block_delta"
        );
        assert!(text.contains("event: message_stop"), "should have message_stop");
    }

    #[tokio::test]
    async fn missing_api_key_returns_401() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"model":"claude-sonnet-4-20250514","max_tokens":1024,"messages":[]}"#,
            ))
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
