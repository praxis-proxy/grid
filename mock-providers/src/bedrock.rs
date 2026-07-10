//! Mock AWS `Bedrock` Converse API server.
//!
//! Implements `POST /model/{model_id}/converse` and
//! `POST /model/{model_id}/converse-stream`. Validates
//! that the `Authorization` header contains an AWS `SigV4`
//! signature prefix (does not verify the actual signature).

use axum::{
    Router,
    body::Body,
    http::{Request, Response, StatusCode, header},
    routing::{get, post},
};
use serde_json::{Value, json};

use crate::common;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the `Bedrock` mock router.
pub fn router() -> Router {
    Router::new()
        .route("/model/{model_id}/converse", post(converse))
        .route("/model/{model_id}/converse-stream", post(converse_stream))
        .route("/health", get(common::health_ok))
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/// Check that the `Authorization` header contains a `SigV4` prefix.
fn has_sigv4_auth(headers: &http::HeaderMap) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("AWS4-HMAC-SHA256"))
}

/// Build a 403 response for missing `SigV4` credentials.
fn sigv4_forbidden() -> Response<Body> {
    common::json_response(
        StatusCode::FORBIDDEN,
        &json!({
            "__type": "AccessDeniedException",
            "message": "missing or invalid AWS SigV4 authorization",
        }),
    )
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Handle `POST /model/{model_id}/converse`.
async fn converse(req: Request<Body>) -> Response<Body> {
    if !has_sigv4_auth(req.headers()) {
        return sigv4_forbidden();
    }
    let model_id = extract_model_id(req.uri().path());
    non_streaming_response(&model_id)
}

/// Handle `POST /model/{model_id}/converse-stream`.
async fn converse_stream(req: Request<Body>) -> Response<Body> {
    if !has_sigv4_auth(req.headers()) {
        return sigv4_forbidden();
    }
    let model_id = extract_model_id(req.uri().path());
    binary_event_stream_response(&model_id)
}

/// Extract model ID from the URL path.
fn extract_model_id(path: &str) -> String {
    path.split('/').nth(2).unwrap_or("unknown-model").to_owned()
}

// ---------------------------------------------------------------------------
// Response builders
// ---------------------------------------------------------------------------

/// Build a non-streaming Converse response.
fn non_streaming_response(model_id: &str) -> Response<Body> {
    common::json_response(
        StatusCode::OK,
        &json!({
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [{"text": "This is a mock response."}],
                }
            },
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 6, "totalTokens": 16},
            "metrics": {"latencyMs": 42},
            "additionalModelResponseFields": {"model_id": model_id},
        }),
    )
}

/// Build a binary event stream response (Bedrock's streaming format).
///
/// Bedrock uses a custom binary event stream protocol, not SSE.
/// Each event is a length-prefixed frame with headers and a JSON
/// payload. For testing purposes, we produce a simplified version
/// that is structurally correct enough to validate parsing.
fn binary_event_stream_response(model_id: &str) -> Response<Body> {
    let events = binary_stream_events(model_id);
    let mut buf = Vec::new();
    for event in &events {
        let payload = serde_json::to_vec(event).unwrap_or_default();
        write_binary_event(&mut buf, &payload);
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/vnd.amazon.eventstream")
        .body(Body::from(buf))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap_or_default()
        })
}

/// Build the event payloads for binary event stream.
fn binary_stream_events(model_id: &str) -> Vec<Value> {
    vec![
        json!({"contentBlockStart": {"contentBlockIndex": 0, "start": {"text": ""}}}),
        json!({"contentBlockDelta": {"contentBlockIndex": 0, "delta": {"text": "This is a mock response."}}}),
        json!({"contentBlockStop": {"contentBlockIndex": 0}}),
        json!({"messageStop": {"stopReason": "end_turn"}}),
        json!({"metadata": {"usage": {"inputTokens": 10, "outputTokens": 6}, "model_id": model_id}}),
    ]
}

/// Write a simplified binary event frame.
///
/// Real Bedrock binary events use a complex header+payload format
/// with CRC checksums. This simplified version uses a 4-byte
/// big-endian length prefix followed by the JSON payload, which
/// is sufficient for testing that the gateway correctly identifies
/// and processes binary event stream content type.
fn write_binary_event(buf: &mut Vec<u8>, payload: &[u8]) {
    let len = u32::try_from(payload.len()).unwrap_or(0);
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(payload);
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

    fn sigv4_header() -> (http::HeaderName, &'static str) {
        (
            header::AUTHORIZATION,
            "AWS4-HMAC-SHA256 Credential=AKID/20260701/us-east-1/bedrock-runtime/aws4_request, Signature=abc123",
        )
    }

    #[tokio::test]
    async fn non_streaming_converse() {
        let req = Request::builder()
            .method("POST")
            .uri("/model/anthropic.claude-3-sonnet/converse")
            .header(sigv4_header().0.clone(), sigv4_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"messages":[{"role":"user","content":[{"text":"hi"}]}]}"#,
            ))
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::OK, "should return 200");

        let body = to_bytes(resp.into_body(), 65_536).await.unwrap_or_default();
        let json: Value = serde_json::from_slice(&body).unwrap_or_default();
        assert!(json.get("output").is_some(), "should have output");
        assert!(json.get("usage").is_some(), "should have usage");
        assert_eq!(
            json.get("stopReason").and_then(Value::as_str),
            Some("end_turn"),
            "wrong stop reason"
        );
    }

    #[tokio::test]
    async fn streaming_converse() {
        let req = Request::builder()
            .method("POST")
            .uri("/model/anthropic.claude-3-sonnet/converse-stream")
            .header(sigv4_header().0.clone(), sigv4_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"messages":[{"role":"user","content":[{"text":"hi"}]}]}"#,
            ))
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::OK, "should return 200");

        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert_eq!(
            ct, "application/vnd.amazon.eventstream",
            "should be binary event stream"
        );

        let body = to_bytes(resp.into_body(), 65_536).await.unwrap_or_default();
        assert!(!body.is_empty(), "body should not be empty");
    }

    #[tokio::test]
    async fn missing_sigv4_returns_403() {
        let req = Request::builder()
            .method("POST")
            .uri("/model/anthropic.claude-3-sonnet/converse")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"messages":[]}"#))
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN, "should return 403");
    }

    #[tokio::test]
    async fn wrong_auth_scheme_returns_403() {
        let req = Request::builder()
            .method("POST")
            .uri("/model/anthropic.claude-3-sonnet/converse")
            .header(header::AUTHORIZATION, "Bearer some-token")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"messages":[]}"#))
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN, "Bearer should be rejected");
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

    #[test]
    fn binary_event_frame_structure() {
        let payload = b"test payload";
        let mut buf = Vec::new();
        write_binary_event(&mut buf, payload);

        assert_eq!(buf.len(), 4 + payload.len(), "frame should be 4-byte prefix + payload");
        let len_bytes: [u8; 4] = buf
            .get(..4)
            .and_then(|s| <[u8; 4]>::try_from(s).ok())
            .unwrap_or_default();
        let len = u32::from_be_bytes(len_bytes);
        assert_eq!(
            len,
            u32::try_from(payload.len()).unwrap_or(0),
            "length prefix should match payload"
        );
    }
}
