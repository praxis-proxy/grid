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
    extract::{Path, State},
    http::{Request, Response, StatusCode},
    middleware::from_fn_with_state,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{AppState, common};

/// Read the optional expected bearer token from `MOCK_EXPECTED_BEARER_TOKEN`.
///
/// When set, handlers return 403 for a mismatched token (vs 401 for missing).
/// When unset, any bearer token is accepted.
fn expected_bearer_token() -> Option<String> {
    std::env::var("MOCK_EXPECTED_BEARER_TOKEN").ok()
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the `OpenAI` mock router with provider attribution.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/responses", post(responses))
        .route("/v1/conversations", post(create_conversation))
        .route("/v1/conversations/{id}", get(get_conversation))
        .route("/v1/models", get(list_models))
        .route("/metrics", get(metrics))
        .route("/__qualification/receipts", get(qualification_receipts))
        .route("/health", get(common::health_ok))
        .layer(from_fn_with_state(state.clone(), common::inject_provider_header))
        .with_state(state)
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

    /// A prior Responses object owned by one provider.
    #[serde(default)]
    previous_response_id: Option<String>,

    /// A Conversation ID or object, when this response uses stored state.
    #[serde(default)]
    conversation: Option<Value>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Handle `POST /v1/chat/completions`.
async fn chat_completions(req: Request<Body>) -> Response<Body> {
    if let Err(resp) = common::validate_bearer(req.headers(), expected_bearer_token().as_deref()) {
        return resp;
    }

    let body_bytes = axum::body::to_bytes(req.into_body(), 1_048_576)
        .await
        .unwrap_or_default();

    let chat_req: ChatRequest = match serde_json::from_slice(&body_bytes) {
        Ok(parsed) => parsed,
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
async fn responses(State(state): State<AppState>, req: Request<Body>) -> Response<Body> {
    if let Err(resp) = common::validate_bearer(req.headers(), expected_bearer_token().as_deref()) {
        return resp;
    }

    let body_bytes = axum::body::to_bytes(req.into_body(), 1_048_576)
        .await
        .unwrap_or_default();

    let resp_req: ResponsesRequest = match serde_json::from_slice(&body_bytes) {
        Ok(parsed) => parsed,
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

    if !state_reference_belongs_to(&state, resp_req.previous_response_id.as_deref(), "resp")
        || !conversation_reference_belongs_to(&state, resp_req.conversation.as_ref())
    {
        return common::json_response(
            StatusCode::NOT_FOUND,
            &json!({"error": {"message": "stored state belongs to another provider"}}),
        );
    }
    let response_id = state.next_state_id("resp");
    let conversation_id = conversation_reference(resp_req.conversation.as_ref());

    if resp_req.stream {
        responses_streaming_response(&resp_req.model, &response_id, conversation_id)
    } else {
        responses_non_streaming_response(&resp_req.model, &response_id, conversation_id)
    }
}

/// Create a provider-owned Conversations resource for stored-state routing tests.
async fn create_conversation(State(state): State<AppState>, req: Request<Body>) -> Response<Body> {
    if let Err(resp) = common::validate_bearer(req.headers(), expected_bearer_token().as_deref()) {
        return resp;
    }
    let body = axum::body::to_bytes(req.into_body(), 1_048_576)
        .await
        .unwrap_or_default();
    if serde_json::from_slice::<Value>(&body).is_err() {
        return common::json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": {"message": "invalid request body"}}),
        );
    }
    common::json_response(
        StatusCode::OK,
        &json!({
            "id": state.next_state_id("conv"),
            "object": "conversation",
            "metadata": {},
        }),
    )
}

/// Read a Conversation only at its owning provider.
async fn get_conversation(State(state): State<AppState>, Path(id): Path<String>, req: Request<Body>) -> Response<Body> {
    if let Err(resp) = common::validate_bearer(req.headers(), expected_bearer_token().as_deref()) {
        return resp;
    }
    if !state_reference_belongs_to(&state, Some(&id), "conv") {
        return common::json_response(
            StatusCode::NOT_FOUND,
            &json!({"error": {"message": "conversation not found at this provider"}}),
        );
    }
    common::json_response(
        StatusCode::OK,
        &json!({"id": id, "object": "conversation", "metadata": {}}),
    )
}

/// Return request receipts for the task-local balancing qualification.
async fn qualification_receipts(State(state): State<AppState>) -> Response<Body> {
    common::qualification_receipts(&state)
}

/// Whether a tagged-state identifier belongs to the provider serving this request.
fn state_reference_belongs_to(state: &AppState, id: Option<&str>, kind: &str) -> bool {
    id.is_none_or(|id| id.starts_with(&format!("{kind}_{}-", state.provider_site)))
}

/// The ID in a Responses API conversation reference, which may be a string or object.
fn conversation_reference(value: Option<&Value>) -> Option<&str> {
    match value? {
        Value::String(id) => Some(id),
        Value::Object(object) => object.get("id").and_then(Value::as_str),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) => None,
    }
}

/// Whether a referenced Conversation belongs to this provider.
fn conversation_reference_belongs_to(state: &AppState, conversation: Option<&Value>) -> bool {
    state_reference_belongs_to(state, conversation_reference(conversation), "conv")
}

/// Handle `GET /v1/models`.
async fn list_models(req: Request<Body>) -> Response<Body> {
    if let Err(resp) = common::validate_bearer(req.headers(), expected_bearer_token().as_deref()) {
        return resp;
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

/// Export the normalized queue-depth signal consumed by the Grid operator.
async fn metrics(State(state): State<AppState>) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "text/plain; version=0.0.4")
        // A ready count, as an EPP exposes, so a grid operator reads the mock as ready.
        .body(Body::from(format!(
            "grid_demo_queue_depth {}\ninference_pool_ready_pods 1\n",
            state.queue_depth
        )))
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Response builders: chat completions
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
// Response builders: Responses API
// ---------------------------------------------------------------------------

/// Build a non-streaming Responses API response.
fn responses_non_streaming_response(model: &str, id: &str, conversation_id: Option<&str>) -> Response<Body> {
    common::json_response(
        StatusCode::OK,
        &json!({
            "id": id,
            "object": "response",
            "model": model,
            "status": "completed",
            "conversation": conversation_id.map(|conversation| json!({"id": conversation, "object": "conversation"})),
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
fn responses_streaming_response(model: &str, id: &str, conversation_id: Option<&str>) -> Response<Body> {
    common::sse_response(&responses_streaming_chunks(model, id, conversation_id), Some("[DONE]"))
}

/// Build the SSE chunk sequence for a streaming Responses API response.
///
/// Uses `data: {json}` SSE format with an embedded `type` field per event.
/// This is a mock-compatible approximation; the real `OpenAI` Responses
/// streaming protocol uses named SSE events (`event: <name>\ndata: <json>`).
fn responses_streaming_chunks(model: &str, id: &str, conversation_id: Option<&str>) -> Vec<Value> {
    vec![
        json!({
            "id": id,
            "type": "response.created",
            "object": "response",
            "model": model,
            "status": "in_progress",
            "conversation": conversation_id.map(|conversation| json!({"id": conversation, "object": "conversation"})),
        }),
        json!({
            "id": id,
            "type": "response.output_text.delta",
            "delta": {"type": "output_text", "text": "This is a mock response."},
        }),
        json!({
            "id": id,
            "type": "response.completed",
            "object": "response",
            "model": model,
            "status": "completed",
            "conversation": conversation_id.map(|conversation| json!({"id": conversation, "object": "conversation"})),
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

    /// Build a test `AppState` with a known provider site.
    fn test_state() -> AppState {
        AppState::new("test-site", 0.25)
    }

    /// Send a request to the router and return the response.
    async fn send(req: Request<Body>) -> Response<Body> {
        send_with(test_state(), req).await
    }

    /// Send a request with shared state so tests can inspect receipts across calls.
    async fn send_with(state: AppState, req: Request<Body>) -> Response<Body> {
        tower::ServiceExt::oneshot(router(state), req)
            .await
            .unwrap_or_else(|_| {
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
    async fn response_captures_only_provider_owned_backend_evidence() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(auth_header().0, auth_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-ai-provider-attribution", "site-us-west")
            .header("x-ai-provider-request-id", "provider-request-1")
            .body(Body::from(r#"{"model":"gpt-4o","stream":false}"#))
            .unwrap_or_default();

        let resp = send(req).await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(common::BACKEND_PROVIDER_CAPTURE_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("site-us-west")
        );
        assert_eq!(
            resp.headers()
                .get(common::BACKEND_REQUEST_ID_CAPTURE_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("provider-request-1")
        );
        assert!(
            resp.headers().get(header::AUTHORIZATION).is_none(),
            "backend evidence must never echo Authorization"
        );
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
            .and_then(|val| val.to_str().ok())
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
    async fn metrics_endpoint_exports_normalized_queue_depth() {
        let req = Request::builder()
            .method("GET")
            .uri("/metrics")
            .body(Body::empty())
            .unwrap_or_default();

        let resp = send(req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), 4096).await.unwrap_or_default();
        assert_eq!(
            body.as_ref(),
            b"grid_demo_queue_depth 0.25\ninference_pool_ready_pods 1\n"
        );
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
    async fn responses_ids_are_provider_specific_and_tag_ready() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header(auth_header().0, auth_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"model":"gpt-4o","input":"hello"}"#))
            .unwrap_or_default();

        let response = send(req).await;
        let body = to_bytes(response.into_body(), 65_536).await.unwrap_or_default();
        let json: Value = serde_json::from_slice(&body).unwrap_or_default();
        assert_eq!(
            json.get("id").and_then(Value::as_str),
            Some("resp_test-site-1"),
            "Responses IDs must be taggable and identify their owning backend"
        );
    }

    #[tokio::test]
    async fn responses_reject_a_previous_id_from_another_provider() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header(auth_header().0, auth_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4o","input":"continue","previous_response_id":"resp_provider-b-9"}"#,
            ))
            .unwrap_or_default();

        let response = send(req).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn conversations_create_and_get_validate_the_owner() {
        let state = test_state();
        let create = Request::builder()
            .method("POST")
            .uri("/v1/conversations")
            .header(auth_header().0, auth_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{}"))
            .unwrap_or_default();
        let response = send_with(state.clone(), create).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 65_536).await.unwrap_or_default();
        let json: Value = serde_json::from_slice(&body).unwrap_or_default();
        let id = json.get("id").and_then(Value::as_str).unwrap_or_default();
        assert_eq!(id, "conv_test-site-1");

        let read = Request::builder()
            .method("GET")
            .uri(format!("/v1/conversations/{id}"))
            .header(auth_header().0, auth_header().1)
            .body(Body::empty())
            .unwrap_or_default();
        assert_eq!(send_with(state.clone(), read).await.status(), StatusCode::OK);

        let wrong_owner = Request::builder()
            .method("GET")
            .uri("/v1/conversations/conv_provider-b-9")
            .header(auth_header().0, auth_header().1)
            .body(Body::empty())
            .unwrap_or_default();
        assert_eq!(
            send_with(state, wrong_owner).await.status(),
            StatusCode::NOT_FOUND,
            "a conversation must not be treated as available on another backend"
        );
    }

    #[tokio::test]
    async fn qualification_receipts_are_opt_in_and_count_unique_request_ids() {
        let state = test_state().with_qualification_receipts(true);
        let inference = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(auth_header().0, auth_header().1)
            .header("x-qualification-request-id", "receipt-1")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"model":"gpt-4o","messages":[]}"#))
            .unwrap_or_default();
        assert_eq!(send_with(state.clone(), inference).await.status(), StatusCode::OK);

        let state_read = Request::builder()
            .method("GET")
            .uri("/v1/conversations/conv_test-site-1")
            .header(auth_header().0, auth_header().1)
            .header("x-qualification-request-id", "receipt-state-read")
            .body(Body::empty())
            .unwrap_or_default();
        assert_eq!(send_with(state.clone(), state_read).await.status(), StatusCode::OK);

        let receipts = Request::builder()
            .method("GET")
            .uri("/__qualification/receipts")
            .body(Body::empty())
            .unwrap_or_default();
        let response = send_with(state, receipts).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 65_536).await.unwrap_or_default();
        let json: Value = serde_json::from_slice(&body).unwrap_or_default();
        assert_eq!(json.get("total").and_then(Value::as_u64), Some(2));
        assert_eq!(
            json.get("request_ids")
                .and_then(Value::as_array)
                .and_then(|ids| ids.first())
                .and_then(Value::as_str),
            Some("receipt-1")
        );
        assert_eq!(
            json.get("request_ids")
                .and_then(Value::as_array)
                .and_then(|ids| ids.get(1))
                .and_then(Value::as_str),
            Some("receipt-state-read")
        );

        let disabled = Request::builder()
            .method("GET")
            .uri("/__qualification/receipts")
            .body(Body::empty())
            .unwrap_or_default();
        assert_eq!(send(disabled).await.status(), StatusCode::NOT_FOUND);
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
            .and_then(|val| val.to_str().ok())
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

    #[tokio::test]
    async fn provider_site_header_present() {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(auth_header().0, auth_header().1)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"model":"gpt-4o","stream":false}"#))
            .unwrap_or_default();

        let resp = send(req).await;
        let hdr = resp
            .headers()
            .get(common::PROVIDER_HEADER_NAME)
            .and_then(|val| val.to_str().ok());
        assert_eq!(hdr, Some("test-site"), "should contain configured provider site");
    }
}
