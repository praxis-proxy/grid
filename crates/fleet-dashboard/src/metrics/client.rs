//! The Prometheus HTTP API client: instant and range queries over HTTPS with
//! a bearer token and an optional site CA.

use std::{collections::BTreeMap, time::Duration};

use serde::Deserialize;
use time::OffsetDateTime;

use super::MetricsError;

/// Responses larger than this are rejected rather than buffered.
pub(super) const MAX_BODY_BYTES: usize = 8 << 20;

/// How much of an unexpected body an error message carries.
const ERROR_BODY_CHARS: usize = 200;

/// One instant sample.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    /// Series labels, including `__name__` when present.
    pub labels: BTreeMap<String, String>,
    /// Sample value; always finite.
    pub value: f64,
}

/// The result of an instant query.
pub type Vector = Vec<Sample>;

/// One sample on a range query series.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesPoint {
    /// Sample time at second precision.
    pub at: OffsetDateTime,
    /// Sample value; always finite.
    pub value: f64,
}

/// One series of a range query.
#[derive(Debug, Clone, PartialEq)]
pub struct MatrixSeries {
    /// Series labels.
    pub labels: BTreeMap<String, String>,
    /// Samples in the order the store returned them.
    pub points: Vec<SeriesPoint>,
}

/// The result of a range query.
pub type Matrix = Vec<MatrixSeries>;

/// The span and resolution of a range query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    /// First sample time, inclusive.
    pub start: OffsetDateTime,
    /// Last sample time, inclusive.
    pub end: OffsetDateTime,
    /// Time between samples.
    pub step: Duration,
}

/// A Prometheus-compatible API at one base URL.
#[derive(Debug, Clone)]
pub struct Client {
    /// Base URL without a trailing slash.
    base_url: String,
    /// Bearer token; empty sends no `Authorization` header.
    token: String,
    /// Transport, shared between clients that trust the same CA.
    http: reqwest::Client,
}

/// The envelope every Prometheus API response uses.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Envelope {
    /// `success` or `error`.
    status: String,
    /// Set when `status` is `error`.
    #[serde(rename = "errorType")]
    error_type: String,
    /// Set when `status` is `error`.
    error: String,
    /// Set when `status` is `success`.
    data: Data,
}

/// The payload of a successful response.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Data {
    /// `vector`, `matrix`, `scalar`, or `string`.
    #[serde(rename = "resultType")]
    result_type: String,
    /// Decoded per `result_type` by the caller.
    result: serde_json::Value,
}

/// One element of a vector result.
#[derive(Debug, Deserialize)]
struct RawSample {
    /// Series labels.
    metric: BTreeMap<String, String>,
    /// `[timestamp, "value"]`.
    value: (f64, String),
}

/// One element of a matrix result.
#[derive(Debug, Deserialize)]
struct RawSeries {
    /// Series labels.
    metric: BTreeMap<String, String>,
    /// `[[timestamp, "value"], ...]`.
    values: Vec<(f64, String)>,
}

impl Client {
    /// A client for the API at `base_url`, authenticating with `token` when
    /// it is non-empty.
    #[must_use]
    pub fn new(base_url: &str, token: &str, http: reqwest::Client) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            token: token.to_owned(),
            http,
        }
    }

    /// Evaluates `promql` at `at`. Samples that are not finite are dropped;
    /// they carry no information for the dashboard.
    ///
    /// # Errors
    ///
    /// Any transport, status, or decoding failure, or a non-vector result.
    pub async fn query(&self, promql: &str, at: OffsetDateTime) -> Result<Vector, MetricsError> {
        let params = [("query", promql.to_owned()), ("time", at.unix_timestamp().to_string())];
        let data = self.call("/api/v1/query", &params).await?;
        expect_result_type(&data, "vector", promql)?;
        let raw: Vec<RawSample> = serde_json::from_value(data.result)?;
        Ok(raw
            .into_iter()
            .filter_map(|sample| {
                finite(&sample.value.1).map(|value| Sample {
                    labels: sample.metric,
                    value,
                })
            })
            .collect())
    }

    /// Evaluates `promql` over `window`.
    ///
    /// # Errors
    ///
    /// Any transport, status, or decoding failure, or a non-matrix result.
    pub async fn query_range(&self, promql: &str, window: Window) -> Result<Matrix, MetricsError> {
        let params = [
            ("query", promql.to_owned()),
            ("start", window.start.unix_timestamp().to_string()),
            ("end", window.end.unix_timestamp().to_string()),
            ("step", window.step.as_secs().to_string()),
        ];
        let data = self.call("/api/v1/query_range", &params).await?;
        expect_result_type(&data, "matrix", promql)?;
        let raw: Vec<RawSeries> = serde_json::from_value(data.result)?;
        raw.into_iter()
            .map(|series| {
                Ok(MatrixSeries {
                    labels: series.metric,
                    points: points(series.values)?,
                })
            })
            .collect()
    }

    /// Performs one GET and unwraps the Prometheus envelope.
    async fn call(&self, path: &str, params: &[(&str, String)]) -> Result<Data, MetricsError> {
        let mut request = self.http.get(format!("{}{path}", self.base_url)).query(params);
        if !self.token.is_empty() {
            request = request.bearer_auth(&self.token);
        }
        let response = request.send().await?;
        let status = response.status();
        let body = read_capped(response).await?;
        match serde_json::from_slice::<Envelope>(&body) {
            Ok(envelope) if envelope.status == "success" => Ok(envelope.data),
            Ok(envelope) if !envelope.status.is_empty() => Err(MetricsError::Upstream {
                error_type: envelope.error_type,
                message: envelope.error,
            }),
            Ok(_) | Err(_) if !status.is_success() => Err(MetricsError::Status {
                status: status.as_u16(),
                body: truncate(&body),
            }),
            Ok(_) => Err(MetricsError::Status {
                status: status.as_u16(),
                body: truncate(&body),
            }),
            Err(err) => Err(MetricsError::Decode(err)),
        }
    }
}

/// An HTTPS client trusting the platform roots plus `ca_pem` when given.
/// Cross-cluster scraping keeps certificate verification on, matching the hub
/// EPP.
///
/// # Errors
///
/// [`MetricsError::NoCertificates`] when `ca_pem` holds no certificate, or
/// [`MetricsError::BuildClient`] when the transport cannot be configured.
pub fn build_http_client(ca_pem: Option<&[u8]>, timeout: Duration) -> Result<reqwest::Client, MetricsError> {
    let mut builder = reqwest::Client::builder()
        .timeout(timeout)
        .min_tls_version(reqwest::tls::Version::TLS_1_2);
    if let Some(pem) = ca_pem {
        let roots = reqwest::Certificate::from_pem_bundle(pem).map_err(MetricsError::InvalidCa)?;
        if roots.is_empty() {
            return Err(MetricsError::NoCertificates);
        }
        for root in roots {
            builder = builder.add_root_certificate(root);
        }
    }
    builder.build().map_err(MetricsError::BuildClient)
}

/// Rejects a result of another type than `want`.
fn expect_result_type(data: &Data, want: &'static str, promql: &str) -> Result<(), MetricsError> {
    if data.result_type == want {
        Ok(())
    } else {
        Err(MetricsError::ResultType {
            promql: promql.to_owned(),
            got: data.result_type.clone(),
            want,
        })
    }
}

/// Reads the body, refusing to buffer more than [`MAX_BODY_BYTES`].
async fn read_capped(mut response: reqwest::Response) -> Result<Vec<u8>, MetricsError> {
    if response.content_length().is_some_and(exceeds_cap) {
        return Err(MetricsError::ResponseTooLarge);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len().saturating_add(chunk.len()) > MAX_BODY_BYTES {
            return Err(MetricsError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Whether a declared `Content-Length` is more than the cap.
fn exceeds_cap(length: u64) -> bool {
    usize::try_from(length).is_ok_and(|length| length > MAX_BODY_BYTES)
        || length > u64::try_from(usize::MAX).unwrap_or(u64::MAX)
}

/// Converts raw `[timestamp, value]` pairs, dropping non-finite values.
fn points(values: Vec<(f64, String)>) -> Result<Vec<SeriesPoint>, MetricsError> {
    values
        .into_iter()
        .filter_map(|(timestamp, raw)| finite(&raw).map(|value| (timestamp, value)))
        .map(|(timestamp, value)| {
            Ok(SeriesPoint {
                at: seconds(timestamp)?,
                value,
            })
        })
        .collect()
}

/// A Prometheus timestamp, whole seconds only, as the Go implementation did.
#[expect(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    reason = "truncation toward zero is the intended second-precision behavior; `as` saturates out of range and \
              `from_unix_timestamp` then rejects the result"
)]
fn seconds(timestamp: f64) -> Result<OffsetDateTime, MetricsError> {
    OffsetDateTime::from_unix_timestamp(timestamp as i64)
        .map_err(|source| MetricsError::Timestamp { timestamp, source })
}

/// Parses a Prometheus sample value, rejecting NaN and anything beyond
/// `±1e300`, which the dashboard treats as no data.
fn finite(raw: &str) -> Option<f64> {
    raw.parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && value.abs() <= 1e300)
}

/// The first [`ERROR_BODY_CHARS`] characters of `body`, marked when cut.
fn truncate(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let mut chars = text.chars();
    let head: String = chars.by_ref().take(ERROR_BODY_CHARS).collect();
    if chars.next().is_some() {
        format!("{head}...")
    } else {
        head
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
#[expect(clippy::indexing_slicing, reason = "test assertions on known response shapes")]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use time::OffsetDateTime;

    use super::{Client, MetricsError, Sample, SeriesPoint, Window, build_http_client};
    use crate::metrics::testing::{FakePrometheus, TOKEN, install_provider};

    #[tokio::test]
    async fn a_vector_is_decoded_and_non_finite_samples_are_dropped() {
        let body = r#"{"status":"success","data":{"resultType":"vector","result":[
            {"metric":{"model_name":"qwen"},"value":[1757160000,"9"]},
            {"metric":{},"value":[1757160000,"NaN"]},
            {"metric":{},"value":[1757160000,"1e400"]}]}}"#;
        let base = FakePrometheus::new().vector("sum(up)", body).serve().await;
        let vector = client(&base).query("sum(up)", at(1_757_160_000)).await.unwrap();
        let labels = BTreeMap::from([("model_name".to_owned(), "qwen".to_owned())]);
        assert_eq!(
            vector,
            vec![Sample { labels, value: 9.0 }],
            "NaN and overflow samples carry no information"
        );
    }

    #[tokio::test]
    async fn a_matrix_is_decoded_and_a_trailing_slash_on_the_base_url_is_tolerated() {
        let body = r#"{"status":"success","data":{"resultType":"matrix","result":[
            {"metric":{},"values":[[1757160000,"1.5"],[1757160030,"2.5"]]}]}}"#;
        let base = FakePrometheus::new()
            .answer("/api/v1/query_range", "avg(x)", 200, body)
            .serve()
            .await;
        let matrix = client(&format!("{base}/"))
            .query_range(
                "avg(x)",
                Window {
                    start: at(1_757_160_000),
                    end: at(1_757_160_060),
                    step: Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        assert_eq!(matrix.len(), 1, "one series: {matrix:?}");
        assert_eq!(
            matrix[0].points[1],
            SeriesPoint {
                at: at(1_757_160_030),
                value: 2.5
            },
            "second point"
        );
    }

    #[tokio::test]
    async fn upstream_failures_are_reported_with_their_cause() {
        let cases = [
            ("http error", 502, "nope", "status 502"),
            (
                "prometheus error",
                400,
                r#"{"status":"error","errorType":"bad_data","error":"parse error"}"#,
                "parse error",
            ),
            (
                "wrong result type",
                200,
                r#"{"status":"success","data":{"resultType":"scalar","result":[1,"2"]}}"#,
                "scalar",
            ),
            ("unauthorized", 401, "", "status 401"),
        ];
        for (name, status, body, want) in cases {
            let base = FakePrometheus::new()
                .answer("/api/v1/query", "up", status, body)
                .serve()
                .await;
            let err = client(&base).query("up", at(0)).await.unwrap_err();
            assert!(err.to_string().contains(want), "{name}: {err} should mention {want:?}");
        }
    }

    #[tokio::test]
    async fn a_long_error_body_is_truncated_on_a_character_boundary() {
        let base = FakePrometheus::new()
            .answer("/api/v1/query", "up", 502, &"é".repeat(300))
            .serve()
            .await;
        let message = client(&base).query("up", at(0)).await.unwrap_err().to_string();
        assert!(message.contains("status 502") && message.ends_with("..."), "{message}");
        assert!(
            message.chars().count() < 240,
            "body must be cut to 200 characters: {message}"
        );
    }

    #[test]
    fn a_non_pem_ca_is_rejected_and_no_ca_is_accepted() {
        install_provider();
        let err = build_http_client(Some(b"not a pem"), Duration::from_secs(1)).unwrap_err();
        assert!(
            matches!(err, MetricsError::NoCertificates | MetricsError::InvalidCa(_)),
            "{err}"
        );
        build_http_client(None, Duration::from_secs(3)).unwrap();
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    fn client(base: &str) -> Client {
        install_provider();
        Client::new(base, TOKEN, build_http_client(None, Duration::from_secs(5)).unwrap())
    }


    fn at(unix: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(unix).unwrap()
    }
}
