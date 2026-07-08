//! Shared utilities for mock provider servers.

use axum::{
    body::Body,
    http::{Response, StatusCode, header},
    response::IntoResponse,
};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

/// Build a JSON response with the given status code and body.
pub(crate) fn json_response(status: StatusCode, body: &Value) -> Response<Body> {
    let bytes = serde_json::to_vec(body).unwrap_or_default();
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(bytes))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap_or_default()
        })
}

/// Build an SSE streaming response from a list of events.
///
/// Each event is formatted as `data: <json>\n\n`. The final event
/// is `data: [DONE]\n\n` (`OpenAI` convention) unless `done_marker`
/// is `None`.
pub(crate) fn sse_response(events: &[Value], done_marker: Option<&str>) -> Response<Body> {
    let mut buf = String::new();
    for event in events {
        let json = serde_json::to_string(event).unwrap_or_default();
        buf.push_str("data: ");
        buf.push_str(&json);
        buf.push_str("\n\n");
    }
    if let Some(marker) = done_marker {
        buf.push_str("data: ");
        buf.push_str(marker);
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

/// Standard health check response.
pub(crate) async fn health_ok() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Standard 401 Unauthorized JSON response.
pub(crate) fn unauthorized(message: &str) -> Response<Body> {
    json_response(
        StatusCode::UNAUTHORIZED,
        &serde_json::json!({
            "error": {
                "message": message,
                "type": "authentication_error",
            }
        }),
    )
}

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

/// Extract a bearer token from the Authorization header.
pub(crate) fn extract_bearer(headers: &http::HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use http::HeaderMap;

    use super::*;

    #[test]
    fn json_response_has_correct_content_type() {
        let resp = json_response(StatusCode::OK, &serde_json::json!({"key": "value"}));
        assert_eq!(resp.status(), StatusCode::OK, "status should be 200");
        let ct = resp.headers().get(header::CONTENT_TYPE);
        assert!(ct.is_some(), "content-type should be set");
        assert_eq!(
            ct.and_then(|v| v.to_str().ok()).unwrap_or_default(),
            "application/json",
            "content-type should be application/json"
        );
    }

    #[test]
    fn sse_response_format() {
        let events = vec![serde_json::json!({"chunk": 1}), serde_json::json!({"chunk": 2})];
        let resp = sse_response(&events, Some("[DONE]"));
        assert_eq!(resp.status(), StatusCode::OK, "status should be 200");
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert_eq!(ct, "text/event-stream", "content-type should be text/event-stream");
    }

    #[test]
    fn extract_bearer_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer sk-test-key"
                .parse()
                .unwrap_or_else(|_| http::HeaderValue::from_static("")),
        );
        let token = extract_bearer(&headers);
        assert_eq!(token, Some("sk-test-key"), "should extract bearer token");
    }

    #[test]
    fn extract_bearer_missing() {
        let headers = HeaderMap::new();
        let token = extract_bearer(&headers);
        assert!(token.is_none(), "should return None for missing header");
    }

    #[test]
    fn extract_bearer_wrong_scheme() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Basic dXNlcjpwYXNz"
                .parse()
                .unwrap_or_else(|_| http::HeaderValue::from_static("")),
        );
        let token = extract_bearer(&headers);
        assert!(token.is_none(), "should return None for non-Bearer scheme");
    }

    #[test]
    fn unauthorized_response() {
        let resp = unauthorized("bad key");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "status should be 401");
    }
}
