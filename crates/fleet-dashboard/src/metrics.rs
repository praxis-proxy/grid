//! Queries Prometheus-compatible endpoints: thanos-querier on each spoke, or
//! one central store scoped by a cluster label.

mod client;
mod inject;
mod source;
#[cfg(test)]
pub(crate) mod testing;

pub use client::{Client, Matrix, MatrixSeries, Sample, SeriesPoint, Vector, Window, build_http_client};
pub use inject::inject_matcher;
pub use source::{CentralSource, PerSiteSource, SecretReader, SiteSecret, Source};

/// Why a query could not be answered.
#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    /// The demo fleet simulating an outage.
    #[error("{0}")]
    Simulated(&'static str),
    /// The request did not finish within the per-site timeout.
    #[error("timed out after {0:?}")]
    Timeout(std::time::Duration),
    /// The request never completed.
    #[error("prometheus request: {0}")]
    Http(#[from] reqwest::Error),
    /// A non-success status with a body that is not a Prometheus envelope.
    #[error("prometheus status {status}: {body}")]
    Status {
        /// HTTP status code.
        status: u16,
        /// Start of the response body.
        body: String,
    },
    /// Prometheus answered with its own error envelope.
    #[error("prometheus {error_type}: {message}")]
    Upstream {
        /// Prometheus `errorType`, such as `bad_data`.
        error_type: String,
        /// Prometheus `error` message.
        message: String,
    },
    /// The body was not the expected JSON.
    #[error("decode response: {0}")]
    Decode(#[from] serde_json::Error),
    /// The body exceeded the size cap.
    #[error("response larger than {} bytes", client::MAX_BODY_BYTES)]
    ResponseTooLarge,
    /// A sample timestamp outside the representable range.
    #[error("timestamp {timestamp} is out of range")]
    Timestamp {
        /// The raw Prometheus timestamp.
        timestamp: f64,
        /// Why `time` rejected it.
        source: time::error::ComponentRange,
    },
    /// The store answered with another result type than the endpoint yields.
    #[error("query {promql:?}: result type {got}, want {want}")]
    ResultType {
        /// The query as sent.
        promql: String,
        /// Prometheus `resultType`.
        got: String,
        /// The type this endpoint always returns.
        want: &'static str,
    },
    /// A site CA bundle that is not PEM.
    #[error("ca.crt is not a PEM bundle: {0}")]
    InvalidCa(reqwest::Error),
    /// A site CA bundle with no certificate in it.
    #[error("ca.crt contains no PEM certificates")]
    NoCertificates,
    /// The HTTP client could not be configured.
    #[error("build http client: {0}")]
    BuildClient(reqwest::Error),
    /// A site registered without a `metricsURL` label.
    #[error("site {site}: site has no metricsURL label")]
    NoMetricsUrl {
        /// Site name.
        site: String,
    },
    /// A site whose Secret is missing or has an empty token.
    #[error("site {site}: secret {secret:?}: metrics secret not found")]
    NoSecret {
        /// Site name.
        site: String,
        /// The Secret name the registry points at.
        secret: String,
    },
    /// Central mode could not scope a query.
    #[error("parse {promql:?}: {reason}")]
    InvalidPromql {
        /// The query as configured.
        promql: String,
        /// The parser's message.
        reason: String,
    },
}
