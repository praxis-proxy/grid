//! Mock `OpenAI` API server.
//!
//! Implements `POST /v1/chat/completions` (streaming and non-streaming),
//! `POST /v1/responses` (streaming and non-streaming), and `GET /v1/models`.
//! Validates bearer token authentication.
//!
//! The `/v1/responses` streaming path uses SSE `data:` events with an embedded
//! `type` field (e.g. `"response.created"`, `"response.output_text.delta"`,
//! `"response.completed"`) rather than named SSE events.

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
pub fn router() -> Router {
    Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/responses", post(responses))
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

/// Minimal Responses API request body.
#[derive(Debug, Deserialize)]
struct ResponsesRequest {
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

/// Handle `POST /v1/responses`.
async fn responses(req: Request<Body>) -> Response<Body> {
    if common::extract_bearer(req.headers()).is_none() {
        return common::unauthorized("missing bearer token");
    }

    let body_bytes = axum::body::to_bytes(req.into_body(), 1_048_576)
        .await
        .unwrap_or_default();

    let resp_req: ResponsesRequest = match serde_json::from_slice(&body_bytes) {
        Ok(r) => r,
        Err(_) => {
            return common::json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": {"message": "invalid request body"}}),
            );
        },
    };

    if resp_req.model.trim().is_empty() {
        return common::json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": {"message": "model is required"}}),
        );
    }

    if resp_req.stream {
        responses_streaming_response(&resp_req.model)
    } else {
        responses_non_streaming_response(&resp_req.model)
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
// Response builders — chat completions
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

/// Build the SSE chunk sequence for a streaming chat completions response.
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
// Response builders — Responses API
// ---------------------------------------------------------------------------

/// Build a non-streaming Responses API response.
fn responses_non_streaming_response(model: &str) -> Response<Body> {
    common::json_response(
        StatusCode::OK,
        &json!({
            "id": "resp-mock-001",
            "object": "response",
            "model": model,
            "status": "completed",
            "output": [{
                "id": "msg-mock-001",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "This is a mock response."}],
            }],
            "usage": {
                "input_tokens": 10,
                "output_tokens": 6,
                "total_tokens": 16,
            },
        }),
    )
}

/// Build an SSE streaming Responses API response.
fn responses_streaming_response(model: &str) -> Response<Body> {
    common::sse_response(&responses_streaming_chunks(model), Some("[DONE]"))
}

/// Build the SSE chunk sequence for a streaming Responses API response.
///
/// Uses `data: {json}` SSE format with an embedded `type` field per event.
/// This is a mock-compatible approximation; the real `OpenAI` Responses
/// streaming protocol uses named SSE events (`event: <name>\ndata: <json>`).
fn responses_streaming_chunks(model: &str) -> Vec<Value> {
    vec![
        json!({
            "id": "resp-mock-001",
            "type": "response.created",
            "object": "response",
            "model": model,
            "status": "in_progress",
        }),
        json!({
            "id": "resp-mock-001",
            "type": "response.output_text.delta",
            "delta": {"type": "output_text", "text": "This is a mock response."},
        }),
        json!({
            "id": "resp-mock-001",
            "type": "response.completed",
            "object": "response",
            "model": model,
            "status": "completed",
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

    #[tokio::test]
    async fn non_streaming_responses_returns_response_object() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/responses")
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
            Some("response"),
            "wrong object type"
        );
        assert!(json.get("output").is_some(), "should have output array");
        assert!(json.get("usage").is_some(), "should have usage");
        assert_eq!(
            json.get("status").and_then(Value::as_str),
            Some("completed"),
            "non-streaming response must carry status=completed"
        );
    }

    #[tokio::test]
    async fn responses_echoes_model() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header(auth_header().0, auth_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"model":"o3","stream":false}"#))
            .unwrap_or_default();

        let resp = send(req).await;
        let body = to_bytes(resp.into_body(), 65_536).await.unwrap_or_default();
        let json: Value = serde_json::from_slice(&body).unwrap_or_default();
        assert_eq!(
            json.get("model").and_then(Value::as_str),
            Some("o3"),
            "should echo requested model"
        );
    }

    #[tokio::test]
    async fn responses_missing_bearer_returns_401() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"model":"gpt-4o"}"#))
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "should return 401");
    }

    #[tokio::test]
    async fn responses_invalid_json_returns_400() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header(auth_header().0, auth_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("not json"))
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "should return 400");
    }

    #[tokio::test]
    async fn responses_missing_model_returns_400() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header(auth_header().0, auth_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"input":"hello"}"#))
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "should return 400");
    }

    #[tokio::test]
    async fn responses_blank_model_returns_400() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header(auth_header().0, auth_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"model":"   ","input":"hello"}"#))
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "should return 400");
    }

    #[tokio::test]
    async fn responses_route_does_not_return_chat_choices() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header(auth_header().0, auth_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"model":"gpt-4o","stream":false}"#))
            .unwrap_or_default();

        let resp = send(req).await;
        let body = to_bytes(resp.into_body(), 65_536).await.unwrap_or_default();
        let json: Value = serde_json::from_slice(&body).unwrap_or_default();
        assert!(json.get("choices").is_none(), "responses must not contain chat choices");
    }

    #[tokio::test]
    async fn streaming_responses_returns_sse() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/responses")
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
    }

    #[tokio::test]
    async fn streaming_responses_contains_response_events() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header(auth_header().0, auth_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"model":"gpt-4o","stream":true}"#))
            .unwrap_or_default();

        let resp = send(req).await;
        let body = to_bytes(resp.into_body(), 65_536).await.unwrap_or_default();
        let text = String::from_utf8_lossy(&body);
        assert!(
            text.contains("response.created"),
            "should contain response.created event"
        );
        assert!(
            text.contains("response.output_text.delta"),
            "should contain delta event",
        );
        assert!(
            text.contains("response.completed"),
            "should contain response.completed event",
        );
    }

    #[tokio::test]
    async fn streaming_responses_ends_with_done_marker() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header(auth_header().0, auth_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"model":"gpt-4o","stream":true}"#))
            .unwrap_or_default();

        let resp = send(req).await;
        let body = to_bytes(resp.into_body(), 65_536).await.unwrap_or_default();
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("[DONE]"), "should end with [DONE]");
    }
}
