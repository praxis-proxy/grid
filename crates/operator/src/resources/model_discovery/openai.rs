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
    endpoint_resolution::{self, PinnedResolver},
    tls_backend::{
        ClientTlsConfig, HttpsConnector, build_custom_tls_connector_with_resolver, build_native_connector_with_resolver,
    },
};

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

/// Maximum response body size, in bytes.
///
/// vLLM entries carry `permission`, `root`, `parent`, and `max_model_len`,
/// about 0.5 to 1 KiB each. 1 MiB leaves ~4 KiB per entry at the model cap.
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

    /// Client built from the endpoint's validated DNS answers.
    client: ModelClient,

    /// Bound on the whole poll, including reading the body.
    timeout: Duration,
}

impl OpenAiModels {
    /// Build a source for `url`.
    ///
    /// # Errors
    ///
    /// Returns [`DiscoveryError::Config`] when the URI is not `http`/`https`,
    /// when a bearer token or TLS configuration is used with plain HTTP, or
    /// when `token` is not a valid header value.
    pub(crate) fn new(
        uri: Uri,
        token: Option<&BearerToken>,
        tls: Option<&ClientTlsConfig>,
        client: ModelClient,
        timeout: Duration,
    ) -> Result<Self, DiscoveryError> {
        match uri.scheme_str() {
            Some("https") => {},
            Some("http") if token.is_some() => {
                return Err(DiscoveryError::Config("bearer token requires HTTPS".to_owned()));
            },
            Some("http") if tls.is_none() => {},
            Some("http") => return Err(DiscoveryError::Config("TLS configured on a plain-HTTP URL".to_owned())),
            _ => return Err(DiscoveryError::Config("URL scheme must be http or https".to_owned())),
        }

        let authorization = token.map(bearer_header).transpose()?;
        Ok(Self {
            url: uri,
            authorization,
            client,
            timeout,
        })
    }

    /// Send the request and parse the response, without a timeout.
    async fn fetch(&self) -> Result<ServedModels, DiscoveryError> {
        // Redirects are not followed: a 3xx is reported as a status error.
        let response = self
            .client
            .request(self.request()?)
            .await
            .map_err(|e| DiscoveryError::Transport(e.into()))?;
        if !response.status().is_success() {
            return Err(DiscoveryError::Status(response.status()));
        }

        let body = read_bounded(response.into_body()).await?;
        let list: ModelList = serde_json::from_slice(&body)?;
        let models = ServedModels::try_from_names(list.data.into_iter().map(|entry| entry.id))?;
        Ok(models)
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

/// Hyper client pinned to one validated endpoint resolution.
pub(crate) type ModelClient = Client<HttpsConnector<PinnedResolver>, Empty<Bytes>>;

/// Validate and parse a model-discovery endpoint before building its client.
pub(crate) fn validated_uri(url: &str) -> Result<Uri, DiscoveryError> {
    if endpoint_resolution::validate_endpoint_url(url) != endpoint_resolution::EndpointUrlValidation::Ok {
        return Err(DiscoveryError::Config("URL endpoint is blocked or invalid".to_owned()));
    }
    parse_url(url)
}

/// Build a client pinned to the endpoint's validated DNS answers.
pub(crate) async fn build_client(
    uri: &Uri,
    tls: Option<&ClientTlsConfig>,
    timeout: Duration,
) -> Result<ModelClient, DiscoveryError> {
    let endpoint = endpoint_resolution::resolve_endpoint(uri, timeout)
        .await
        .map_err(|error| match error {
            endpoint_resolution::EndpointResolutionError::DnsTimeout => DiscoveryError::Timeout(timeout),
            error @ (endpoint_resolution::EndpointResolutionError::Dns
            | endpoint_resolution::EndpointResolutionError::BlockedAddress) => {
                DiscoveryError::Transport(Box::new(error))
            },
        })?;
    build_client_from_endpoint(endpoint, tls)
}

/// Build a client from an already resolved endpoint.
fn build_client_from_endpoint(
    endpoint: endpoint_resolution::ResolvedEndpoint,
    tls: Option<&ClientTlsConfig>,
) -> Result<ModelClient, DiscoveryError> {
    let resolver = endpoint.into_pinned_resolver();
    let connector = match tls {
        Some(config) => build_custom_tls_connector_with_resolver(config, resolver),
        None => build_native_connector_with_resolver(resolver),
    }
    .map_err(|error| DiscoveryError::Transport(Box::new(error)))?;
    Ok(Client::builder(TokioExecutor::new()).build(connector))
}

/// Build a source for a local fixture server using test-only endpoint setup.
#[cfg(test)]
pub(crate) async fn new_for_test(
    url: &str,
    token: Option<&BearerToken>,
    tls: Option<&ClientTlsConfig>,
    timeout: Duration,
) -> Result<OpenAiModels, DiscoveryError> {
    #[cfg(not(feature = "fips"))]
    drop(rustls::crypto::ring::default_provider().install_default());

    let uri = parse_url(url)?;
    let endpoint = endpoint_resolution::resolve_endpoint_for_test(&uri, timeout)
        .await
        .map_err(|error| DiscoveryError::Transport(Box::new(error)))?;
    let client = build_client_from_endpoint(endpoint, tls)?;
    OpenAiModels::new(uri, token, tls, client, timeout)
}

/// Parse a model-discovery endpoint that already passed validation.
fn parse_url(url: &str) -> Result<Uri, DiscoveryError> {
    url.parse()
        .map_err(|e| DiscoveryError::Config(format!("invalid URL: {e}")))
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

        let models = source(&url, None).await.served_models().await.map(|models| {
            let mut names = models.into_names();
            names.sort_unstable();
            names
        });

        assert_eq!(
            models.ok(),
            Some(vec!["a".to_owned(), "b".to_owned()]),
            "should list all ids"
        );
    }

    #[tokio::test]
    async fn empty_list_is_success() {
        let url = serve(|| async { r#"{"data":[]}"# }).await;

        let models = source(&url, None).await.served_models().await;

        assert_eq!(models.ok(), Some(ServedModels::default()), "empty list should succeed");
    }

    #[tokio::test]
    async fn bearer_token_over_http_is_rejected() {
        let token = BearerToken::new("s3cret".to_owned());
        let client = test_client().await;
        let result = OpenAiModels::new(
            parse_url("http://example.com/v1/models").unwrap_or_else(|_| std::process::abort()),
            Some(&token),
            None,
            client,
            Duration::from_secs(5),
        );

        assert!(
            matches!(result, Err(DiscoveryError::Config(message)) if message == "bearer token requires HTTPS"),
            "HTTP must be rejected when a bearer token is configured"
        );
    }

    #[tokio::test]
    async fn non_success_status_is_rejected() {
        let url = serve(|| async { StatusCode::SERVICE_UNAVAILABLE.into_response() }).await;

        let models = source(&url, None).await.served_models().await;

        assert!(
            matches!(models, Err(DiscoveryError::Status(StatusCode::SERVICE_UNAVAILABLE))),
            "503 should be a status error"
        );
    }

    #[tokio::test]
    async fn malformed_json_is_rejected() {
        let url = serve(|| async { r#"{"data":[{"name":"a"}]}"# }).await;

        let models = source(&url, None).await.served_models().await;

        assert!(
            matches!(models, Err(DiscoveryError::Malformed(_))),
            "missing id should be malformed"
        );
    }

    #[tokio::test]
    async fn duplicate_id_is_rejected() {
        let url = serve(|| async { r#"{"data":[{"id":"a"},{"id":"a"}]}"# }).await;

        let models = source(&url, None).await.served_models().await;

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

        let models = source(&url, None).await.served_models().await;

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

        let models = new_for_test(&url, None, None, timeout)
            .await
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
        let models = source("http://127.0.0.1:1/v1/models", None).await.served_models().await;

        assert!(
            matches!(models, Err(DiscoveryError::Transport(_))),
            "refused connection should be a transport error"
        );
    }

    #[test]
    fn unsupported_scheme_is_rejected() {
        let result = validated_uri("ftp://example.com/v1/models");

        assert!(
            matches!(result, Err(DiscoveryError::Config(_))),
            "ftp should be rejected"
        );
    }

    #[test]
    fn ssrf_sensitive_endpoint_is_rejected_before_model_discovery() {
        let result = validated_uri("http://127.0.0.1:8080/v1/models");

        assert!(
            matches!(result, Err(DiscoveryError::Config(_))),
            "model discovery must apply the shared endpoint SSRF policy"
        );
    }

    #[test]
    fn ordinary_cluster_endpoint_is_accepted() {
        let result = validated_uri("http://10.96.0.42:8080/v1/models");

        assert!(
            result.is_ok(),
            "ordinary cluster addresses must remain eligible for model discovery"
        );
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    async fn source(url: &str, token: Option<&BearerToken>) -> OpenAiModels {
        new_for_test(url, token, None, Duration::from_secs(5))
            .await
            .unwrap_or_else(|_| std::process::abort())
    }

    async fn test_client() -> ModelClient {
        #[cfg(not(feature = "fips"))]
        drop(rustls::crypto::ring::default_provider().install_default());

        let uri = parse_url("http://127.0.0.1:1/v1/models").unwrap_or_else(|_| std::process::abort());
        let endpoint = endpoint_resolution::resolve_endpoint_for_test(&uri, Duration::from_secs(5))
            .await
            .unwrap_or_else(|_| std::process::abort());
        build_client_from_endpoint(endpoint, None).unwrap_or_else(|_| std::process::abort())
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
