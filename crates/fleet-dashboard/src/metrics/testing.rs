#![expect(
    clippy::expect_used,
    reason = "test support; a failing fake should abort the test loudly"
)]
//! Test doubles for the metrics path: a fake Prometheus HTTP API with canned
//! answers keyed by path and query, and a static secret store.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, Uri},
};
use axum_server::tls_rustls::RustlsConfig;

use super::{SecretReader, SiteSecret};
use crate::queries::QuerySet;

/// The token every fake expects.
pub(crate) const TOKEN: &str = "tok";

/// How long a hanging fake sleeps before answering; longer than any test timeout.
const HANG: Duration = Duration::from_secs(60);

/// Canned answers for one fake server. Anything not answered, or sent without
/// the expected token, gets a descriptive 400 so a mismatch surfaces in the
/// client's error instead of a hang.
#[derive(Debug, Default)]
pub(crate) struct FakePrometheus {
    /// `(path, query)` → `(status, body)`.
    answers: HashMap<(String, String), (u16, String)>,
    /// Sleep before every answer, to exercise timeouts.
    hang: bool,
    /// Requests received, including rejected ones.
    hits: Arc<AtomicUsize>,
}

impl FakePrometheus {
    /// A fake with no answers yet.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Answers `query` on `path` with `status` and `body`.
    pub(crate) fn answer(mut self, path: &str, query: &str, status: u16, body: &str) -> Self {
        self.answers
            .insert((path.to_owned(), query.to_owned()), (status, body.to_owned()));
        self
    }

    /// A successful instant-query answer.
    pub(crate) fn vector(self, query: &str, body: &str) -> Self {
        self.answer("/api/v1/query", query, 200, body)
    }

    /// A counter of requests received, usable after the fake is serving.
    pub(crate) fn hit_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.hits)
    }

    /// Makes every request stall far longer than any client timeout.
    pub(crate) fn hang(mut self) -> Self {
        self.hang = true;
        self
    }

    /// Starts serving on a free loopback port; returns the base URL.
    pub(crate) async fn serve(self) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let base = format!("http://{}", listener.local_addr().expect("local addr"));
        let app = self.router();
        tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        base
    }

    /// Starts serving over TLS with a fresh self-signed certificate for
    /// `localhost`; returns the base URL and the certificate PEM a client must
    /// trust to reach it.
    pub(crate) async fn serve_tls(self) -> (String, String) {
        install_provider();
        let certified = rcgen::generate_simple_self_signed(["localhost".to_owned()]).expect("self-signed certificate");
        let cert_pem = certified.cert.pem();
        let key_pem = certified.signing_key.serialize_pem();
        let config = RustlsConfig::from_pem(cert_pem.clone().into_bytes(), key_pem.into_bytes())
            .await
            .expect("tls config");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let port = listener.local_addr().expect("local addr").port();
        listener.set_nonblocking(true).expect("non-blocking listener");
        let app = self.router();
        tokio::spawn(async move {
            axum_server::from_tcp_rustls(listener, config)
                .expect("tls listener")
                .serve(app.into_make_service())
                .await
                .expect("serve tls");
        });
        (format!("https://localhost:{port}"), cert_pem)
    }

    /// The router answering from this fake's table.
    fn router(self) -> Router {
        Router::new().fallback(respond).with_state(Arc::new(self))
    }
}

/// Looks up the canned answer, or explains what arrived instead.
async fn respond(
    State(fake): State<Arc<FakePrometheus>>,
    uri: Uri,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> (StatusCode, String) {
    fake.hits.fetch_add(1, Ordering::Relaxed);
    if fake.hang {
        tokio::time::sleep(HANG).await;
    }
    let auth = headers.get("authorization").and_then(|value| value.to_str().ok());
    if auth != Some(&format!("Bearer {TOKEN}")) {
        return (
            StatusCode::BAD_REQUEST,
            format!("fake: unexpected authorization {auth:?}"),
        );
    }
    let query = params.get("query").cloned().unwrap_or_default();
    match fake.answers.get(&(uri.path().to_owned(), query.clone())) {
        Some((status, body)) => (
            StatusCode::from_u16(*status).unwrap_or(StatusCode::IM_A_TEAPOT),
            body.clone(),
        ),
        None => (
            StatusCode::BAD_REQUEST,
            format!("fake: no answer for {} {query:?}", uri.path()),
        ),
    }
}

/// Installs the ring crypto provider, as `main` does at startup; harmless when
/// another test already did.
pub(crate) fn install_provider() {
    drop(rustls::crypto::ring::default_provider().install_default());
}

/// A base URL on a loopback port that nothing listens on.
pub(crate) fn refused_base() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    format!("http://{}", listener.local_addr().expect("local addr"))
}

/// Secrets fixed at construction.
#[derive(Debug, Default)]
pub(crate) struct StaticSecrets(HashMap<String, SiteSecret>);

impl SecretReader for StaticSecrets {
    fn secret(&self, name: &str) -> Option<SiteSecret> {
        self.0.get(name).cloned()
    }
}

/// `(secret name, token, CA PEM)` triples as a secret store.
pub(crate) fn secrets(entries: &[(&str, &str, &[u8])]) -> Arc<StaticSecrets> {
    let map = entries
        .iter()
        .map(|(name, token, ca)| {
            (
                (*name).to_owned(),
                SiteSecret {
                    token: (*token).to_owned(),
                    ca_pem: ca.to_vec(),
                },
            )
        })
        .collect();
    Arc::new(StaticSecrets(map))
}

/// An instant-query answer with no samples.
pub(crate) const EMPTY: &str = r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#;

/// Every key the collector may ask for.
const KEYS: [&str; 11] = [
    "readyEndpoints",
    "gpuTotal",
    "gpuUtil",
    "gpuUtilFallback",
    "models",
    "rps",
    "p50LatencyMs",
    "tokensPerSec",
    "queueDepth",
    "replicasDown",
    "tenants",
];

/// An instant-query answer with one unlabeled sample.
pub(crate) fn one(value: f64) -> String {
    format!(
        r#"{{"status":"success","data":{{"resultType":"vector","result":[{{"metric":{{}},"value":[0,"{value}"]}}]}}}}"#
    )
}

/// An instant-query answer with one sample per `(label value, sample value)`.
pub(crate) fn labeled(label: &str, samples: &[(&str, f64)]) -> String {
    let result: Vec<String> = samples
        .iter()
        .map(|(name, value)| format!(r#"{{"metric":{{"{label}":"{name}"}},"value":[0,"{value}"]}}"#))
        .collect();
    format!(
        r#"{{"status":"success","data":{{"resultType":"vector","result":[{}]}}}}"#,
        result.join(",")
    )
}

/// Answers for a healthy site: 2 GPUs at 41.5%, two models, two tenants.
pub(crate) fn healthy_answers() -> BTreeMap<&'static str, (u16, String)> {
    BTreeMap::from([
        ("readyEndpoints", (200, one(2.0))),
        ("gpuTotal", (200, one(2.0))),
        ("gpuUtil", (200, one(41.5))),
        ("models", (200, labeled("model_name", &[("qwen", 9.0), ("llama", 4.0)]))),
        ("rps", (200, one(11.4))),
        ("p50LatencyMs", (200, one(842.0))),
        ("tokensPerSec", (200, one(1930.0))),
        ("queueDepth", (200, one(7.0))),
        ("replicasDown", (200, one(0.0))),
        ("tenants", (200, labeled("tenant", &[("acme", 55.0), ("globex", 45.0)]))),
    ])
}

/// A fake answering every key `queries` enables: from `answers`, or with an
/// empty vector.
pub(crate) fn fake_for(queries: &QuerySet, answers: &BTreeMap<&str, (u16, String)>) -> FakePrometheus {
    KEYS.iter()
        .filter(|key| queries.enabled(key))
        .fold(FakePrometheus::new(), |fake, key| {
            let (status, body) = answers.get(key).cloned().unwrap_or_else(|| (200, EMPTY.to_owned()));
            fake.answer("/api/v1/query", &queries.query(key).promql, status, &body)
        })
}

/// A range-query answer with one unlabeled series sampled every `step`
/// seconds from `start`.
pub(crate) fn matrix(start: i64, step: i64, values: &[f64]) -> String {
    let samples: Vec<String> = values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let at = start.saturating_add(i64::try_from(index).unwrap_or(i64::MAX).saturating_mul(step));
            format!(r#"[{at},"{value}"]"#)
        })
        .collect();
    format!(
        r#"{{"status":"success","data":{{"resultType":"matrix","result":[{{"metric":{{}},"values":[{}]}}]}}}}"#,
        samples.join(",")
    )
}
