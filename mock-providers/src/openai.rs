//! Mock `OpenAI` API server.
//!
//! Implements `POST /v1/chat/completions` (streaming and non-streaming)
//! and `GET /v1/models`. Validates bearer token authentication.

use axum::{
    Router,
    body::Body,
    http::{Request, Response, StatusCode},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::common;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the `OpenAI` mock router.
pub(crate) fn router() -> Router {
    Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/models", get(list_models))
        .route("/health", get(common::health_ok))
}

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// Minimal chat completions request body.
#[derive(Debug, Deserialize)]
struct ChatRequest {
    /// Model name.
    #[serde(default)]
    model: String,

    /// Whether to stream the response.
    #[serde(default)]
    stream: bool,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Handle `POST /v1/chat/completions`.
async fn chat_completions(req: Request<Body>) -> Response<Body> {
    if common::extract_bearer(req.headers()).is_none() {
        return common::unauthorized("missing bearer token");
    }

    let body_bytes = axum::body::to_bytes(req.into_body(), 1_048_576)
        .await
        .unwrap_or_default();

    let chat_req: ChatRequest = match serde_json::from_slice(&body_bytes) {
        Ok(r) => r,
        Err(_) => {
            return common::json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": {"message": "invalid request body"}}),
            );
        },
    };

    if chat_req.stream {
        streaming_response(&chat_req.model)
    } else {
        non_streaming_response(&chat_req.model)
    }
}

/// Handle `GET /v1/models`.
async fn list_models(req: Request<Body>) -> Response<Body> {
    if common::extract_bearer(req.headers()).is_none() {
        return common::unauthorized("missing bearer token");
    }

    common::json_response(
        StatusCode::OK,
        &json!({
            "object": "list",
            "data": [
                {"id": "gpt-4o", "object": "model", "owned_by": "openai"},
                {"id": "o3", "object": "model", "owned_by": "openai"},
            ]
        }),
    )
}

// ---------------------------------------------------------------------------
// Response builders
// ---------------------------------------------------------------------------

/// Build a non-streaming chat completions response.
fn non_streaming_response(model: &str) -> Response<Body> {
    common::json_response(
        StatusCode::OK,
        &json!({
            "id": "chatcmpl-mock-001",
            "object": "chat.completion",
            "model": model,
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "This is a mock response.",
                },
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 6,
                "total_tokens": 16,
            },
        }),
    )
}

/// Build an SSE streaming chat completions response.
fn streaming_response(model: &str) -> Response<Body> {
    common::sse_response(&streaming_chunks(model), Some("[DONE]"))
}

/// Build the SSE chunk sequence for a streaming response.
fn streaming_chunks(model: &str) -> Vec<Value> {
    vec![
        json!({
            "id": "chatcmpl-mock-001",
            "object": "chat.completion.chunk",
            "model": model,
            "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}}],
        }),
        json!({
            "id": "chatcmpl-mock-001",
            "object": "chat.completion.chunk",
            "model": model,
            "choices": [{"index": 0, "delta": {"content": "This is a mock response."}}],
        }),
        json!({
            "id": "chatcmpl-mock-001",
            "object": "chat.completion.chunk",
            "model": model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        }),
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use http::header;

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

    fn auth_header() -> (http::HeaderName, http::HeaderValue) {
        (
            header::AUTHORIZATION,
            "Bearer sk-test-key"
                .parse()
                .unwrap_or_else(|_| http::HeaderValue::from_static("")),
        )
    }

    #[tokio::test]
    async fn non_streaming_chat_completion() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(auth_header().0, auth_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"model":"gpt-4o","stream":false}"#))
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::OK, "should return 200");

        let body = to_bytes(resp.into_body(), 65_536).await.unwrap_or_default();
        let json: Value = serde_json::from_slice(&body).unwrap_or_default();
        assert_eq!(
            json.get("object").and_then(Value::as_str),
            Some("chat.completion"),
            "wrong object type"
        );
        assert!(json.get("choices").is_some(), "should have choices");
        assert!(json.get("usage").is_some(), "should have usage");
    }

    #[tokio::test]
    async fn streaming_chat_completion() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(auth_header().0, auth_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"model":"gpt-4o","stream":true}"#))
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
        assert!(text.contains("[DONE]"), "should end with [DONE]");
    }

    #[tokio::test]
    async fn missing_auth_returns_401() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"model":"gpt-4o"}"#))
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

    #[tokio::test]
    async fn list_models_with_auth() {
        let req = Request::builder()
            .method("GET")
            .uri("/v1/models")
            .header(auth_header().0, auth_header().1)
            .body(Body::empty())
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::OK, "should return 200");

        let body = to_bytes(resp.into_body(), 65_536).await.unwrap_or_default();
        let json: Value = serde_json::from_slice(&body).unwrap_or_default();
        assert_eq!(
            json.get("object").and_then(Value::as_str),
            Some("list"),
            "should be a list"
        );
        let data = json.get("data").and_then(Value::as_array);
        assert!(data.is_some(), "should have data array");
        assert_eq!(data.map(Vec::len).unwrap_or_default(), 2, "should have 2 models");
    }

    #[tokio::test]
    async fn list_models_without_auth() {
        let req = Request::builder()
            .method("GET")
            .uri("/v1/models")
            .body(Body::empty())
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "should return 401");
    }
}
