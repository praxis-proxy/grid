//! OpenAI-compatible `GET /v1/models` source.

use std::time::Duration;

use bytes::Bytes;
use http::{
    HeaderValue, Request, Uri,
    header::{ACCEPT, AUTHORIZATION},
};
use http_body_util::{BodyExt as _, Empty, LengthLimitError, Limited};
use hyper::body::Incoming;
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use serde::Deserialize;

use super::{DiscoveryError, ModelSource, ServedModels};
use crate::resources::{
    credentials::BearerToken,
    tls_backend::{ClientTlsConfig, build_custom_tls_connector, build_native_connector},
};

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

/// Maximum response body size, in bytes.
///
/// vLLM entries carry `permission`, `root`, `parent`, and `max_model_len`,
/// about 0.5–1 KiB each. 1 MiB leaves ~4 KiB per entry at the model cap.
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// `Accept` header value for the model-listing request.
const APPLICATION_JSON: &str = "application/json";

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

/// `GET /v1/models` response; fields other than `data[].id` are ignored.
#[derive(Deserialize)]
struct ModelList {
    /// Listed models.
    data: Vec<ModelEntry>,
}

/// One entry of [`ModelList`].
#[derive(Deserialize)]
struct ModelEntry {
    /// Served model name.
    id: String,
}

// ---------------------------------------------------------------------------
// OpenAiModels
// ---------------------------------------------------------------------------

/// Lists served models via an OpenAI-compatible `GET /v1/models`.
pub(crate) struct OpenAiModels {
    /// Full model-listing URL.
    url: Uri,

    /// Pre-built `Authorization` header, marked sensitive.
    authorization: Option<HeaderValue>,

    /// Custom TLS configuration; system roots when `None`.
    tls: Option<ClientTlsConfig>,

    /// Bound on the whole poll, including reading the body.
    timeout: Duration,
}

impl OpenAiModels {
    /// Build a source for `url`.
    ///
    /// # Errors
    ///
    /// Returns [`DiscoveryError::Config`] when `url` is not `http`/`https`,
    /// when `tls` is set on a plain-HTTP URL, or when `token` is not a valid
    /// header value.
    pub(crate) fn new(
        url: &str,
        token: Option<&BearerToken>,
        tls: Option<ClientTlsConfig>,
        timeout: Duration,
    ) -> Result<Self, DiscoveryError> {
        let url: Uri = url
            .parse()
            .map_err(|e| DiscoveryError::Config(format!("invalid URL: {e}")))?;

        match url.scheme_str() {
            Some("https") => {},
            Some("http") if tls.is_none() => {},
            Some("http") => return Err(DiscoveryError::Config("TLS configured on a plain-HTTP URL".to_owned())),
            _ => return Err(DiscoveryError::Config("URL scheme must be http or https".to_owned())),
        }

        let authorization = token.map(bearer_header).transpose()?;

        Ok(Self {
            url,
            authorization,
            tls,
            timeout,
        })
    }

    /// Send the request and parse the response, without a timeout.
    async fn fetch(&self) -> Result<ServedModels, DiscoveryError> {
        let connector = match &self.tls {
            Some(config) => build_custom_tls_connector(config),
            None => build_native_connector(),
        }
        .map_err(|e| DiscoveryError::Transport(e.into()))?;
        let client = Client::builder(TokioExecutor::new()).build(connector);

        // Redirects are not followed: a 3xx is reported as a status error.
        let response = client
            .request(self.request()?)
            .await
            .map_err(|e| DiscoveryError::Transport(e.into()))?;
        if !response.status().is_success() {
            return Err(DiscoveryError::Status(response.status()));
        }

        let body = read_bounded(response.into_body()).await?;
        parse_model_list(&body)
    }

    /// Build the `GET` request, with the bearer token when configured.
    fn request(&self) -> Result<Request<Empty<Bytes>>, DiscoveryError> {
        let mut request = Request::get(self.url.clone()).header(ACCEPT, APPLICATION_JSON);
        if let Some(authorization) = &self.authorization {
            request = request.header(AUTHORIZATION, authorization);
        }

        request
            .body(Empty::new())
            .map_err(|e| DiscoveryError::Config(e.to_string()))
    }
}

impl ModelSource for OpenAiModels {
    async fn served_models(&self) -> Result<ServedModels, DiscoveryError> {
        tokio::time::timeout(self.timeout, self.fetch())
            .await
            .map_err(|_elapsed| DiscoveryError::Timeout(self.timeout))?
    }
}

/// Read at most [`MAX_BODY_BYTES`] of `body`.
async fn read_bounded(body: Incoming) -> Result<Bytes, DiscoveryError> {
    let collected = Limited::new(body, MAX_BODY_BYTES).collect().await.map_err(|e| {
        if e.is::<LengthLimitError>() {
            DiscoveryError::BodyTooLarge(MAX_BODY_BYTES)
        } else {
            DiscoveryError::Transport(e)
        }
    })?;
    Ok(collected.to_bytes())
}

/// Build a sensitive `Authorization: Bearer <token>` header value.
fn bearer_header(token: &BearerToken) -> Result<HeaderValue, DiscoveryError> {
    let mut value = HeaderValue::try_from(format!("Bearer {}", token.expose_secret()))
        .map_err(|_invalid| DiscoveryError::Config("bearer token is not a valid header value".to_owned()))?;
    value.set_sensitive(true);
    Ok(value)
}

/// Parse an OpenAI-compatible model list into a validated served-model set.
///
/// `{"data":[{"id":"a"},{"id":"b"}]}` yields `["a", "b"]`.
fn parse_model_list(body: &[u8]) -> Result<ServedModels, DiscoveryError> {
    let list: ModelList = serde_json::from_slice(body)?;
    let models = ServedModels::try_from_names(list.data.into_iter().map(|entry| entry.id))?;
    Ok(models)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use axum::{Router, http::StatusCode, response::IntoResponse as _, routing::get};

    use super::*;
    use crate::resources::model_discovery::ServedModelsError;

    #[tokio::test]
    async fn lists_served_models() {
        let url = serve(|| async { r#"{"object":"list","data":[{"id":"b","object":"model"},{"id":"a"}]}"# }).await;

        let models = source(&url, None).served_models().await.map(ServedModels::into_names);

        assert_eq!(
            models.ok(),
            Some(vec!["a".to_owned(), "b".to_owned()]),
            "should list sorted ids"
        );
    }

    #[tokio::test]
    async fn empty_list_is_success() {
        let url = serve(|| async { r#"{"data":[]}"# }).await;

        let models = source(&url, None).served_models().await;

        assert_eq!(models.ok(), Some(ServedModels::default()), "empty list should succeed");
    }

    #[tokio::test]
    async fn sends_bearer_token() {
        let url = serve(|headers: http::HeaderMap| async move {
            if headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()) == Some("Bearer s3cret") {
                (StatusCode::OK, r#"{"data":[{"id":"a"}]}"#)
            } else {
                (StatusCode::UNAUTHORIZED, "")
            }
        })
        .await;
        let token = BearerToken::new("s3cret".to_owned());

        let with_token = source(&url, Some(&token)).served_models().await;
        let without_token = source(&url, None).served_models().await;

        assert!(with_token.is_ok(), "token should be accepted");
        assert!(
            matches!(without_token, Err(DiscoveryError::Status(StatusCode::UNAUTHORIZED))),
            "missing token should be a status error"
        );
    }

    #[tokio::test]
    async fn non_success_status_is_rejected() {
        let url = serve(|| async { StatusCode::SERVICE_UNAVAILABLE.into_response() }).await;

        let models = source(&url, None).served_models().await;

        assert!(
            matches!(models, Err(DiscoveryError::Status(StatusCode::SERVICE_UNAVAILABLE))),
            "503 should be a status error"
        );
    }

    #[tokio::test]
    async fn malformed_json_is_rejected() {
        let url = serve(|| async { r#"{"data":[{"name":"a"}]}"# }).await;

        let models = source(&url, None).served_models().await;

        assert!(
            matches!(models, Err(DiscoveryError::Malformed(_))),
            "missing id should be malformed"
        );
    }

    #[tokio::test]
    async fn duplicate_id_is_rejected() {
        let url = serve(|| async { r#"{"data":[{"id":"a"},{"id":"a"}]}"# }).await;

        let models = source(&url, None).served_models().await;

        assert!(
            matches!(
                models,
                Err(DiscoveryError::InvalidModels(ServedModelsError::Duplicate(_)))
            ),
            "duplicate id should be rejected"
        );
    }

    #[tokio::test]
    async fn oversized_body_is_rejected() {
        let url = serve(|| async { " ".repeat(MAX_BODY_BYTES + 1) }).await;

        let models = source(&url, None).served_models().await;

        assert!(
            matches!(models, Err(DiscoveryError::BodyTooLarge(MAX_BODY_BYTES))),
            "oversized body should be rejected"
        );
    }

    #[tokio::test]
    async fn slow_backend_times_out() {
        let url = serve(|| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            r#"{"data":[]}"#
        })
        .await;
        let timeout = Duration::from_millis(50);

        let models = OpenAiModels::new(&url, None, None, timeout)
            .unwrap_or_else(|_| std::process::abort())
            .served_models()
            .await;

        assert!(
            matches!(models, Err(DiscoveryError::Timeout(t)) if t == timeout),
            "slow backend should time out"
        );
    }

    #[tokio::test]
    async fn unreachable_backend_is_transport_error() {
        let models = source("http://127.0.0.1:1/v1/models", None).served_models().await;

        assert!(
            matches!(models, Err(DiscoveryError::Transport(_))),
            "refused connection should be a transport error"
        );
    }

    #[test]
    fn unsupported_scheme_is_rejected() {
        let result = OpenAiModels::new("ftp://example.com/v1/models", None, None, Duration::from_secs(1));

        assert!(
            matches!(result, Err(DiscoveryError::Config(_))),
            "ftp should be rejected"
        );
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    fn source(url: &str, token: Option<&BearerToken>) -> OpenAiModels {
        OpenAiModels::new(url, token, None, Duration::from_secs(5)).unwrap_or_else(|_| std::process::abort())
    }

    /// Serve `handler` at `/v1/models` on an ephemeral port; returns the URL.
    async fn serve<H, T>(handler: H) -> String
    where
        H: axum::handler::Handler<T, ()>,
        T: 'static,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|_| std::process::abort());
        let addr = listener.local_addr().unwrap_or_else(|_| std::process::abort());
        let app = Router::new().route("/v1/models", get(handler));

        tokio::spawn(async move { axum::serve(listener, app).await });

        format!("http://{addr}/v1/models")
    }
}
