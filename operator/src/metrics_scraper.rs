// Copyright 2026 Praxis Proxy Authors
//! Async HTTP scraper for Prometheus `/metrics` endpoints.
//!
//! Fetches the raw Prometheus exposition text from a backend's `/metrics`
//! path.  The returned text is passed directly to
//! [`crate::metrics_parser::parse_prometheus_text`] to extract signal values
//! for the scoring engine.
//!
//! ## Usage
//!
//! ```text
//! let text = scrape_metrics("http://backend:9090/metrics", Duration::from_secs(5)).await?;
//! let signals = parse_prometheus_text(&text, &names);
//! let metrics = signals.into_backend_metrics();
//! state.set_metrics(provider_name.to_owned(), metrics);
//! ```
//!
//! ## v1 scope
//!
//! This module only fetches text via plain `http://` and `https://` requests.
//! mTLS client-certificate probing (required for remote grid peers) is not
//! implemented here; that requires the grid CA and site cert which are managed
//! separately.

use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Empty};
use hyper_util::{client::legacy::Client as HyperClient, rt::TokioExecutor};

/// Error returned by [`scrape_metrics`].
#[derive(Debug, thiserror::Error)]
pub enum MetricsScrapeError {
    /// The URL could not be parsed.
    #[error("invalid metrics URL: {0}")]
    InvalidUrl(String),

    /// The scrape request timed out.
    #[error("metrics scrape timed out after {0:?}")]
    Timeout(Duration),

    /// The server returned a non-2xx status code.
    #[error("metrics endpoint returned HTTP {status}: {url}")]
    NonOkStatus {
        /// HTTP status code.
        status: u16,
        /// URL that was scraped.
        url: String,
    },

    /// A transport or TLS error occurred.
    #[error("metrics scrape transport error: {0}")]
    Transport(Box<dyn std::error::Error + Send + Sync>),

    /// The response body could not be decoded as UTF-8.
    #[error("metrics response body is not valid UTF-8: {0}")]
    Encoding(std::string::FromUtf8Error),
}

/// Scrape the Prometheus text exposition from `url`.
///
/// Makes an HTTP GET request to `url` and returns the response body as a
/// `String` if the status is 2xx.  The caller is responsible for parsing
/// the returned text with [`crate::metrics_parser::parse_prometheus_text`].
///
/// # Errors
///
/// Returns [`MetricsScrapeError::Timeout`] if the request exceeds `timeout`.
/// Returns [`MetricsScrapeError::NonOkStatus`] for non-2xx responses.
/// Returns [`MetricsScrapeError::Transport`] for connection failures.
#[expect(
    clippy::too_many_lines,
    reason = "URL parse + scheme check + client build + request + body read: sequential steps"
)]
pub async fn scrape_metrics(url: &str, timeout: Duration) -> Result<String, MetricsScrapeError> {
    let uri = url
        .parse::<http::Uri>()
        .map_err(|e| MetricsScrapeError::Transport(e.into()))
        .and_then(|u| {
            if u.scheme_str().is_some_and(|s| s == "http" || s == "https") {
                Ok(u)
            } else {
                Err(MetricsScrapeError::InvalidUrl(url.to_owned()))
            }
        })?;

    let connector = build_connector()?;
    let client: HyperClient<_, Empty<Bytes>> = HyperClient::builder(TokioExecutor::new()).build(connector);

    let req = http::Request::builder()
        .method(http::Method::GET)
        .uri(uri.clone())
        .body(Empty::<Bytes>::new())
        .map_err(|e| MetricsScrapeError::Transport(e.into()))?;

    let response = tokio::time::timeout(timeout, client.request(req))
        .await
        .map_err(|_elapsed| MetricsScrapeError::Timeout(timeout))?
        .map_err(|e| MetricsScrapeError::Transport(e.into()))?;

    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(MetricsScrapeError::NonOkStatus {
            status,
            url: url.to_owned(),
        });
    }

    let body_bytes = response
        .collect()
        .await
        .map_err(|e| MetricsScrapeError::Transport(e.into()))?
        .to_bytes();

    String::from_utf8(body_bytes.to_vec()).map_err(MetricsScrapeError::Encoding)
}

/// Build an HTTPS connector using native root certificates.
fn build_connector()
-> Result<hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>, MetricsScrapeError> {
    hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .map(|b| b.https_or_http().enable_http1().build())
        .map_err(|e| MetricsScrapeError::Transport(e.into()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    /// Start a local HTTP server on a random port and return the URL.
    async fn start_test_server(response: &'static [u8]) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|_| std::process::abort());
        let port = listener.local_addr().unwrap_or_else(|_| std::process::abort()).port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0_u8; 4096];
                drop(stream.read(&mut buf).await);
                drop(stream.write_all(response).await);
            }
        });
        format!("http://127.0.0.1:{port}")
    }

    #[tokio::test]
    async fn scrape_returns_body_for_200() {
        let body = b"# HELP test_metric Test\ntest_metric 1.0\n";
        let response = b"HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 39\r\n\r\n# HELP test_metric Test\ntest_metric 1.0\n";
        let url = start_test_server(response).await;
        let result = scrape_metrics(&url, Duration::from_secs(5)).await;
        assert!(result.is_ok(), "HTTP 200 must succeed: {result:?}");
        let text = result.unwrap_or_else(|_| std::process::abort());
        assert!(text.contains("test_metric"), "body must be in scrape result");
        let _ = body; // referenced for documentation
    }

    #[tokio::test]
    async fn scrape_returns_error_for_non_2xx() {
        let response = b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
        let url = start_test_server(response).await;
        let result = scrape_metrics(&url, Duration::from_secs(5)).await;
        assert!(result.is_err(), "HTTP 503 must return an error");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::NonOkStatus { status: 503, .. }),
            "error must be NonOkStatus(503)"
        );
    }

    #[tokio::test]
    async fn scrape_returns_timeout_for_silent_server() {
        // Server accepts but never responds — scrape must time out.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|_| std::process::abort());
        let port = listener.local_addr().unwrap_or_else(|_| std::process::abort()).port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0_u8; 4096];
                drop(stream.read(&mut buf).await);
                // Intentionally never respond — hold open for 60s then drop.
                tokio::time::sleep(Duration::from_secs(60)).await;
                drop(stream);
            }
        });
        let url = format!("http://127.0.0.1:{port}/metrics");
        let result = scrape_metrics(&url, Duration::from_millis(100)).await;
        assert!(result.is_err(), "silent server must time out");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::Timeout(_)),
            "error must be Timeout"
        );
    }

    #[tokio::test]
    async fn scrape_returns_error_for_connection_refused() {
        // Port 1 is never open on any standard OS.
        let result = scrape_metrics("http://127.0.0.1:1/metrics", Duration::from_secs(5)).await;
        assert!(result.is_err(), "connection refused must return an error");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::Transport(_)),
            "error must be Transport"
        );
    }

    #[tokio::test]
    async fn scrape_returns_invalid_url_for_unsupported_scheme() {
        let result = scrape_metrics("ftp://example.com/metrics", Duration::from_secs(5)).await;
        assert!(result.is_err(), "ftp:// must return an error");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::InvalidUrl(_)),
            "error must be InvalidUrl"
        );
    }

    #[test]
    fn error_variants_format_correctly() {
        let timeout_err = MetricsScrapeError::Timeout(Duration::from_secs(5));
        assert!(timeout_err.to_string().contains("timed out"), "timeout format");

        let non_ok_err = MetricsScrapeError::NonOkStatus {
            status: 404,
            url: "http://x".to_owned(),
        };
        assert!(non_ok_err.to_string().contains("404"), "non-ok format");

        let url_err = MetricsScrapeError::InvalidUrl("ftp://bad".to_owned());
        assert!(url_err.to_string().contains("ftp://bad"), "url format");
    }
}
