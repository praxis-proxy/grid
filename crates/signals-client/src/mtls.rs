//! mTLS [`SignalSource`]: scrape a peer over verified grid mTLS, authorizing the
//! peer's verified SPIFFE id against the target we dialed.
//!
//! A fresh connection per poll with resumption disabled, so the verifier runs
//! every time and the peer chain is always freshly validated. The verified id is
//! both the authorization check (it must equal the configured target, never a
//! membership-only test, since the verifier ignores SNI) and the `grid_site`
//! attribution the store keys on.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use certs::{DEFAULT_TRUST_DOMAIN, GridSpiffeServerVerifier};
use grid_signals::{
    Ingested,
    feedback::{ACK_EPOCH, ACK_SEQUENCE, INGEST_PATH, IngestReport, REPORT_HEADERS},
};
use http::{Request, header};
use http_body_util::Empty;
use hyper_util::rt::TokioIo;
use rustls::{
    ClientConfig,
    client::{Resumption, danger::ServerCertVerifier},
    crypto::CryptoProvider,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
};
use tokio::{net::TcpStream, time::timeout};
use tokio_rustls::TlsConnector;

use crate::{
    Reporter,
    poller::{FetchError, Scrape, SignalSource},
    scrape::read_scrape_response,
};

/// A [`PeerScraper`] could not be built from its TLS material.
#[derive(Debug, thiserror::Error)]
pub enum MtlsError {
    /// The grid CA bundle was empty or unparseable, or the verifier could not be
    /// constructed.
    #[error("grid verifier: {0}")]
    Verifier(#[from] certs::VerifyError),
    /// The rustls client config (protocol versions or client-auth cert) was invalid.
    #[error("tls client config: {0}")]
    TlsConfig(#[from] rustls::Error),
    /// The process installed no rustls crypto provider to build the client on.
    #[error("no rustls crypto provider installed")]
    NoProvider,
    /// The installed crypto provider could not supply process entropy.
    #[error("gateway reporter entropy unavailable")]
    Entropy,
}

/// Scrapes one peer's signals endpoint over grid mTLS, per poll.
pub struct PeerScraper {
    /// TLS connector carrying the verifier, our client identity, and no resumption.
    connector: TlsConnector,
    /// Verifier handle for the post-handshake id extraction.
    verifier: Arc<GridSpiffeServerVerifier>,
    /// `host:port` to dial.
    addr: String,
    /// SNI for the handshake; the verifier ignores it.
    server_name: ServerName<'static>,
    /// Host header authority.
    authority: String,
    /// Request path, for example `/v1/site/signals`.
    path: String,
    /// SPIFFE id the dialed peer must present; any other id is refused.
    expected_target: String,
    /// Bound on the TCP connect and the TLS handshake.
    connect_timeout: Duration,
    /// Bound on the request/response.
    request_timeout: Duration,
    /// Shared process evidence, absent for ordinary peer readers.
    reporter: Option<Arc<Reporter>>,
}

impl PeerScraper {
    /// Build a source that scrapes `path` at `addr` over mTLS.
    ///
    /// `grid_ca_pem` roots the peer's server-cert verification; the poller
    /// presents `client_cert_chain`/`client_key` as its own grid identity.
    /// `expected_target` is the SPIFFE id the dialed peer must present.
    ///
    /// # Errors
    ///
    /// [`MtlsError`] when the CA bundle or the client-auth material is invalid, or
    /// no process crypto provider is installed.
    #[expect(
        clippy::too_many_arguments,
        reason = "one connection's full identity plus addressing"
    )]
    pub fn new(
        grid_ca_pem: &[u8],
        client_cert_chain: Vec<CertificateDer<'static>>,
        client_key: PrivateKeyDer<'static>,
        addr: &str,
        server_name: ServerName<'static>,
        authority: &str,
        path: &str,
        expected_target: &str,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, MtlsError> {
        // The process provider, so a FIPS gateway polls peers through its validated module.
        let provider = CryptoProvider::get_default().ok_or(MtlsError::NoProvider)?;
        let algorithms = provider.signature_verification_algorithms;
        let verifier = GridSpiffeServerVerifier::new(grid_ca_pem, DEFAULT_TRUST_DOMAIN, algorithms)?;
        // The method form (not Arc::clone) so the unsized coercion to the trait
        // object applies at the annotated binding.
        #[expect(clippy::clone_on_ref_ptr, reason = "unsized coercion to dyn needs the method form")]
        let dyn_verifier: Arc<dyn ServerCertVerifier> = verifier.clone();

        let mut config = ClientConfig::builder_with_provider(Arc::clone(provider))
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(dyn_verifier)
            .with_client_auth_cert(client_cert_chain, client_key)?;
        config.resumption = Resumption::disabled();
        // SP 800-52r2 needs EMS for TLS 1.2, as Praxis requires on its own clients.
        config.require_ems = true;

        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            verifier,
            addr: addr.to_owned(),
            server_name,
            authority: authority.to_owned(),
            path: path.to_owned(),
            expected_target: expected_target.to_owned(),
            connect_timeout,
            request_timeout,
            reporter: None,
        })
    }

    /// Enable process-scoped feedback only for this reporter's verified operator.
    #[must_use]
    pub fn with_reporter(mut self, reporter: Arc<Reporter>) -> Self {
        if self.expected_target == reporter.target {
            self.reporter = Some(reporter);
        }
        self
    }

    /// Open and authorize a fresh connection before sending a bounded request.
    async fn request(
        &self,
        report: Option<&IngestReport>,
    ) -> Result<hyper::Response<hyper::body::Incoming>, FetchError> {
        self.exchange(report).await
    }

    /// Exchange a request while freshly authenticating the dialed operator.
    #[expect(
        clippy::too_many_lines,
        clippy::large_stack_frames,
        reason = "sequential authenticated TLS/HTTP exchange"
    )]
    async fn exchange(
        &self,
        report: Option<&IngestReport>,
    ) -> Result<hyper::Response<hyper::body::Incoming>, FetchError> {
        let tcp = timeout(self.connect_timeout, TcpStream::connect(&self.addr))
            .await
            .map_err(|_elapsed| FetchError::Unreachable(format!("connect timed out to {}", self.addr)))?
            .map_err(|error| FetchError::Unreachable(format!("connect {}: {error}", self.addr)))?;

        let tls = timeout(
            self.connect_timeout,
            self.connector.connect(self.server_name.clone(), tcp),
        )
        .await
        .map_err(|_elapsed| FetchError::Unreachable("tls handshake timed out".to_owned()))?
        .map_err(|error| FetchError::Unreachable(format!("tls handshake: {error}")))?;

        // Authorize on the verified id: fail closed on an absent chain, and reject
        // any peer that is not the target we dialed. Scope the borrow so the
        // stream can move into the HTTP handshake after.
        let verified_id = {
            let (_io, conn) = tls.get_ref();
            self.verifier
                .spiffe_id_from_peer(conn.peer_certificates())
                .map_err(|error| FetchError::Unauthorized(format!("peer identity: {error}")))?
        };
        if verified_id != self.expected_target {
            return Err(FetchError::Unauthorized(format!(
                "peer identity {verified_id} is not the dialed target {}",
                self.expected_target
            )));
        }

        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
            .await
            .map_err(|error| FetchError::Unreachable(format!("http handshake: {error}")))?;
        let driver = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%error, "signals mtls connection ended with error");
            }
        });

        let mut request = Request::builder()
            .method(if report.is_some() {
                http::Method::POST
            } else {
                http::Method::GET
            })
            .uri(report.map_or(self.path.as_str(), |_| INGEST_PATH))
            .header(header::HOST, &self.authority);
        if let Some(report) = report {
            for (name, value) in REPORT_HEADERS.into_iter().zip(report.values()) {
                request = request.header(name, value);
            }
        }
        let request = request
            .body(Empty::<Bytes>::new())
            .map_err(|error| FetchError::Unreachable(error.to_string()))?;

        let response = timeout(self.request_timeout, sender.send_request(request))
            .await
            .map_err(|_elapsed| FetchError::Unreachable("request timed out".to_owned()))?
            .map_err(|error| FetchError::Unreachable(format!("request: {error}")))?;

        drop(driver);
        Ok(response)
    }
}

impl SignalSource for PeerScraper {
    async fn fetch(&self) -> Result<Scrape, FetchError> {
        let response = self.request(None).await?;
        read_scrape_response(response, Arc::from(self.expected_target.as_str()), self.request_timeout).await
    }

    fn report(&self, ingested: Option<Ingested>) -> impl Future<Output = ()> + Send {
        let pending = self.reporter.as_ref().and_then(|reporter| reporter.observe(ingested));
        async move {
            let (Some(reporter), Some(report)) = (&self.reporter, pending) else {
                return;
            };
            let result = timeout(self.request_timeout, self.request(Some(&report)))
                .await
                .map_err(|_elapsed| FeedbackFailure::Timeout)
                .and_then(|response| response.map_err(|_error| FeedbackFailure::Transport));
            complete_report(reporter, &report, result);
        }
    }
}

/// Bounded failure classes; report identities and receiver contents are never labels.
enum FeedbackFailure {
    /// The feedback exchange exceeded its total budget.
    Timeout,
    /// Connection, authentication, or HTTP exchange failed.
    Transport,
    /// The receiver did not return the required status.
    Status,
    /// The receiver omitted, repeated, or mismatched acknowledgment metadata.
    Acknowledgment,
}

impl FeedbackFailure {
    /// Stable Prometheus reason label for one failed delivery attempt.
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Transport => "transport",
            Self::Status => "status",
            Self::Acknowledgment => "acknowledgment",
        }
    }
}

/// Acknowledge only a matching success; count one failure while retaining retry evidence.
fn complete_report<Body>(
    reporter: &Reporter,
    report: &IngestReport,
    result: Result<http::Response<Body>, FeedbackFailure>,
) {
    let failure = match result {
        Ok(response) if response.status() != http::StatusCode::NO_CONTENT => Some(FeedbackFailure::Status),
        Ok(response) if !matches_ack(response.headers(), report) => Some(FeedbackFailure::Acknowledgment),
        Ok(_response) => {
            reporter.acknowledge(report);
            None
        },
        Err(failure) => Some(failure),
    };
    if let Some(failure) = failure {
        metrics::counter!("grid_signals_feedback_failures_total", "reason" => failure.as_str()).increment(1);
    }
}

/// Require one exact value for each acknowledgment scalar.
fn matches_ack(headers: &http::HeaderMap, report: &IngestReport) -> bool {
    [
        (ACK_EPOCH, report.epoch.clone()),
        (ACK_SEQUENCE, report.sequence.to_string()),
    ]
    .into_iter()
    .all(|(name, expected)| {
        let mut values = headers.get_all(name).iter();
        values.next().and_then(|value| value.to_str().ok()) == Some(expected.as_str()) && values.next().is_none()
    })
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::min_ident_chars,
    clippy::too_many_lines,
    reason = "tests"
)]
mod tests {
    use certs::{generate_ca, generate_dns_only_cert, generate_site_cert, spiffe_id};
    use rustls::{ServerConfig, pki_types::pem::PemObject as _};
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpListener,
    };
    use tokio_rustls::TlsAcceptor;

    /// The scraper builds on the process provider; tests install ring, as the gateway installs OpenSSL.
    fn install_ring() {
        drop(rustls::crypto::ring::default_provider().install_default());
    }

    use super::*;

    /// Parse a PEM cert chain and key into DER.
    fn material(cert_pem: &str, key_pem: &str) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
        let chain = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
            .map(|cert| cert.expect("cert").into_owned())
            .collect();
        let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).expect("key");
        (chain, key)
    }

    /// A grid-CA-signed TLS peer that serves one canned response, returns its addr.
    async fn mock_tls_peer(
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        response: Vec<u8>,
    ) -> String {
        let provider = rustls::crypto::ring::default_provider();
        let config = ServerConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .expect("versions")
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .expect("server cert");
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.expect("accept");
            let mut tls = acceptor.accept(tcp).await.expect("tls accept");
            let mut buf = [0_u8; 2048];
            let _read = tls.read(&mut buf).await;
            tls.write_all(&response).await.expect("write");
            tls.flush().await.expect("flush");
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        addr
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_real_mutual_poll_reports_skew_then_clean_in_same_cycle() {
        install_ring();
        let ca = generate_ca("grid-ca").expect("ca");
        let site = generate_site_cert(&ca, "east").expect("site");
        let (server_chain, server_key) = material(&site.cert_pem, &site.key_pem);
        let (client_chain, client_key) = material(&site.cert_pem, &site.key_pem);
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(ca.cert_pem.as_bytes()).expect("root"))
            .expect("root");
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .expect("verifier");
        let config = ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("versions")
            .with_client_cert_verifier(verifier)
            .with_single_cert(server_chain.clone(), server_key)
            .expect("server");
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recording = Arc::clone(&observed);
        let server = tokio::spawn(async move {
            let mut gets = 0;
            for _connection in 0..4 {
                let (tcp, _) = listener.accept().await.expect("accept");
                let mut tls = acceptor.accept(tcp).await.expect("mutual tls");
                assert_eq!(
                    tls.get_ref().1.peer_certificates(),
                    Some(server_chain.as_slice()),
                    "same site certificate proves local gateway"
                );
                let mut head = Vec::new();
                while !head.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    tls.read_exact(&mut byte).await.expect("request");
                    head.extend(byte);
                    assert!(head.len() < 8192);
                }
                let head = String::from_utf8(head).expect("ascii");
                let response = if head.starts_with("GET ") {
                    gets += 1;
                    let stamp = if gets == 1 { 21_000 } else { 1_000 };
                    let body = format!("queue_depth{{grid_site=\"east\",grid_provider=\"pool\"}} 3 {stamp}\n");
                    format!(
                        "HTTP/1.1 200 OK\r\nDate: Thu, 01 Jan 1970 00:00:01 GMT\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                } else {
                    assert!(head.starts_with("POST /v1/site/signals/ingest "));
                    let value = |name: &str| {
                        head.lines()
                            .find_map(|line| {
                                line.split_once(": ")
                                    .filter(|(key, _)| *key == name)
                                    .map(|(_, value)| value.to_owned())
                            })
                            .expect("report header")
                    };
                    let epoch = value(ACK_EPOCH);
                    let sequence = value(ACK_SEQUENCE);
                    let skew = value("x-grid-ingest-skew");
                    let rejected = value("x-grid-ingest-rejected");
                    recording
                        .lock()
                        .expect("recording")
                        .push((sequence.clone(), skew, rejected));
                    format!(
                        "HTTP/1.1 204 No Content\r\n{ACK_EPOCH}: {epoch}\r\n{ACK_SEQUENCE}: {sequence}\r\nConnection: close\r\n\r\n"
                    )
                };
                tls.write_all(response.as_bytes()).await.expect("write");
                tls.flush().await.expect("flush");
            }
        });
        let reporter = Arc::new(Reporter::new(spiffe_id("east")).expect("reporter"));
        let source = PeerScraper::new(
            ca.cert_pem.as_bytes(),
            client_chain,
            client_key,
            &addr,
            sni(),
            "east.grid.internal",
            "/v1/site/signals",
            &spiffe_id("east"),
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .expect("source")
        .with_reporter(reporter);
        let store = grid_signals::LoadStore::new(Duration::from_secs(30));
        for expected in [1, 0] {
            let scrape = source.fetch().await.expect("scrape");
            let ingested = store.ingest_at(&scrape.body, scrape.date_ms, scrape.date_ms, "east");
            assert_eq!(ingested.skewed, expected, "actual ingest is the observation");
            source.report(Some(ingested)).await;
        }
        let recorded = observed.lock().expect("observed").clone();
        server.abort();
        assert_eq!(
            recorded,
            vec![
                ("1".to_owned(), "1".to_owned(), "true".to_owned()),
                ("2".to_owned(), "1".to_owned(), "false".to_owned())
            ],
            "bad and recovery reports complete in their ingest cycles"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unsupported_feedback_retains_pending_bad_without_breaking_source() {
        let recorder = FeedbackRecorder::new();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        install_ring();
        let ca = generate_ca("grid-ca").expect("ca");
        let site = generate_site_cert(&ca, "east").expect("site");
        let (server_chain, server_key) = material(&site.cert_pem, &site.key_pem);
        let (client_chain, client_key) = material(&site.cert_pem, &site.key_pem);
        let addr = mock_tls_peer(
            server_chain,
            server_key,
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
        )
        .await;
        let reporter = Arc::new(Reporter::new(spiffe_id("east")).expect("reporter"));
        let source = PeerScraper::new(
            ca.cert_pem.as_bytes(),
            client_chain,
            client_key,
            &addr,
            sni(),
            "east.grid.internal",
            "/v1/site/signals",
            &spiffe_id("east"),
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .expect("source")
        .with_reporter(Arc::clone(&reporter));
        source
            .report(Some(Ingested {
                skewed: 1,
                ..Ingested::default()
            }))
            .await;
        assert!(
            reporter.observe(None).expect("pending evidence").rejected,
            "unsupported delivery must retain rejection evidence"
        );
        assert_eq!(
            recorder.count("status"),
            1,
            "one unsupported POST counts one delivery failure"
        );
    }

    #[test]
    fn failed_feedback_counts_once_and_matching_success_retains_no_failure() {
        install_ring();
        let recorder = FeedbackRecorder::new();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        let reporter = Reporter::new(spiffe_id("east")).expect("reporter");
        let report = reporter
            .observe(Some(Ingested {
                skewed: 1,
                ..Ingested::default()
            }))
            .expect("pending evidence");
        for (result, reason) in [
            (Err(FeedbackFailure::Timeout), "timeout"),
            (Err(FeedbackFailure::Transport), "transport"),
            (
                Ok(feedback_response(&report, http::StatusCode::NOT_FOUND, Some("1"))),
                "status",
            ),
            (
                Ok(feedback_response(&report, http::StatusCode::NO_CONTENT, None)),
                "acknowledgment",
            ),
            (
                Ok(feedback_response(&report, http::StatusCode::NO_CONTENT, Some("2"))),
                "acknowledgment",
            ),
            (
                Ok(feedback_response(&report, http::StatusCode::NO_CONTENT, Some("01"))),
                "acknowledgment",
            ),
        ] {
            let before = recorder.count(reason);
            complete_report(&reporter, &report, result);
            assert_eq!(
                recorder.count(reason),
                before + 1,
                "each failed attempt counts once: {reason}"
            );
            assert_eq!(
                reporter.observe(None),
                Some(report.clone()),
                "failed delivery preserves exact pending evidence"
            );
        }
        let before = recorder.total();
        complete_report(
            &reporter,
            &report,
            Ok(feedback_response(&report, http::StatusCode::NO_CONTENT, Some("1"))),
        );
        assert!(
            reporter.observe(None).is_none(),
            "a matching success acknowledges pending evidence"
        );
        assert_eq!(recorder.total(), before, "a successful delivery adds no failure");
    }

    /// An empty feedback response with optional exact epoch and supplied sequence.
    fn feedback_response(
        report: &IngestReport,
        status: http::StatusCode,
        sequence: Option<&str>,
    ) -> http::Response<()> {
        let mut response = http::Response::builder().status(status);
        if let Some(sequence) = sequence {
            response = response.header(ACK_EPOCH, &report.epoch).header(ACK_SEQUENCE, sequence);
        }
        response.body(()).expect("response")
    }

    /// Thread-local failure counters that reject unbounded or report-derived labels.
    struct FeedbackRecorder {
        /// Exactly the documented bounded delivery reasons.
        counts: std::collections::BTreeMap<&'static str, Arc<std::sync::atomic::AtomicU64>>,
    }

    impl FeedbackRecorder {
        /// Start an isolated recorder for feedback assertions.
        fn new() -> Self {
            Self {
                counts: ["timeout", "transport", "status", "acknowledgment"]
                    .into_iter()
                    .map(|reason| (reason, Arc::default()))
                    .collect(),
            }
        }

        /// Current count for a documented reason.
        fn count(&self, reason: &str) -> u64 {
            self.counts
                .get(reason)
                .expect("bounded reason")
                .load(std::sync::atomic::Ordering::SeqCst)
        }

        /// Total emitted failure attempts.
        fn total(&self) -> u64 {
            self.counts.keys().map(|reason| self.count(reason)).sum()
        }
    }

    impl metrics::Recorder for FeedbackRecorder {
        fn describe_counter(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn describe_gauge(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn describe_histogram(
            &self,
            _key: metrics::KeyName,
            _unit: Option<metrics::Unit>,
            _description: metrics::SharedString,
        ) {
        }

        fn register_counter(&self, key: &metrics::Key, _metadata: &metrics::Metadata<'_>) -> metrics::Counter {
            assert_eq!(
                key.name(),
                "grid_signals_feedback_failures_total",
                "feedback emits the documented counter"
            );
            let labels: Vec<_> = key.labels().collect();
            assert_eq!(labels.len(), 1, "feedback must have only one bounded label");
            let label = labels.first().expect("reason label");
            assert_eq!(label.key(), "reason", "report contents must never be labels");
            metrics::Counter::from_arc(Arc::clone(self.counts.get(label.value()).expect("bounded reason")))
        }

        fn register_gauge(&self, _key: &metrics::Key, _metadata: &metrics::Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }

        fn register_histogram(&self, _key: &metrics::Key, _metadata: &metrics::Metadata<'_>) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    #[test]
    fn ambiguous_or_mismatched_acknowledgments_are_refused() {
        let report = IngestReport {
            replica: "gateway".to_owned(),
            epoch: "a".repeat(32),
            sequence: 1,
            skew: 1,
            rejected: true,
        };
        let mut headers = http::HeaderMap::new();
        headers.insert(ACK_EPOCH, http::HeaderValue::from_str(&report.epoch).expect("epoch"));
        headers.insert(ACK_SEQUENCE, http::HeaderValue::from_static("1"));
        assert!(matches_ack(&headers, &report));
        headers.append(ACK_SEQUENCE, http::HeaderValue::from_static("2"));
        assert!(!matches_ack(&headers, &report));
        headers.insert(ACK_SEQUENCE, http::HeaderValue::from_static("01"));
        assert!(!matches_ack(&headers, &report));
    }

    fn ok_response() -> Vec<u8> {
        let body = "queue_depth 3 1000\n";
        format!(
            "HTTP/1.1 200 OK\r\nDate: Thu, 01 Jan 1970 00:00:01 GMT\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn sni() -> ServerName<'static> {
        ServerName::try_from("east.grid.internal").expect("sni")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scrapes_and_authorizes_the_dialed_target() {
        let ca = generate_ca("grid-ca").expect("ca");
        let server = generate_site_cert(&ca, "east").expect("server cert");
        let client = generate_site_cert(&ca, "poller").expect("client cert");
        let (server_chain, server_key) = material(&server.cert_pem, &server.key_pem);
        let (client_chain, client_key) = material(&client.cert_pem, &client.key_pem);
        let addr = mock_tls_peer(server_chain, server_key, ok_response()).await;

        install_ring();

        let source = PeerScraper::new(
            ca.cert_pem.as_bytes(),
            client_chain,
            client_key,
            &addr,
            sni(),
            "east.grid.internal",
            "/v1/site/signals",
            &spiffe_id("east"),
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .expect("source");

        let scraped = source.fetch().await.expect("scrape ok");
        assert_eq!(scraped.body, "queue_depth 3 1000\n");
        assert_eq!(scraped.date_ms, 1_000);
        assert_eq!(
            &*scraped.peer_identity,
            &spiffe_id("east"),
            "attribution is the verified id"
        );
    }

    /// A grid-CA-signed TLS peer that sends response headers then stalls the body
    /// forever, for the slowloris body-read-deadline test.
    async fn mock_tls_peer_stalls_body(chain: Vec<CertificateDer<'static>>, key: PrivateKeyDer<'static>) -> String {
        let provider = rustls::crypto::ring::default_provider();
        let config = ServerConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .expect("versions")
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .expect("server cert");
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.expect("accept");
            let mut tls = acceptor.accept(tcp).await.expect("tls accept");
            let mut buf = [0_u8; 2048];
            let _read = tls.read(&mut buf).await;
            // Claim a body far larger than what we send, then never send the rest.
            let headers =
                "HTTP/1.1 200 OK\r\nDate: Thu, 01 Jan 1970 00:00:01 GMT\r\nContent-Length: 1000000\r\n\r\nstart";
            tls.write_all(headers.as_bytes()).await.expect("write");
            tls.flush().await.expect("flush");
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        addr
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejects_a_server_cert_from_another_ca() {
        let ours = generate_ca("grid-ca").expect("ca");
        let theirs = generate_ca("other-ca").expect("other ca");
        // Server cert signed by a CA we do not trust; our client roots on `ours`.
        let server = generate_site_cert(&theirs, "east").expect("server cert");
        let client = generate_site_cert(&ours, "poller").expect("client cert");
        let (server_chain, server_key) = material(&server.cert_pem, &server.key_pem);
        let (client_chain, client_key) = material(&client.cert_pem, &client.key_pem);
        let addr = mock_tls_peer(server_chain, server_key, ok_response()).await;

        install_ring();

        let source = PeerScraper::new(
            ours.cert_pem.as_bytes(),
            client_chain,
            client_key,
            &addr,
            sni(),
            "east.grid.internal",
            "/v1/site/signals",
            &spiffe_id("east"),
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .expect("source");

        let error = source.fetch().await.expect_err("an untrusted CA must not verify");
        assert!(
            matches!(&error, FetchError::Unreachable(message) if message.contains("tls handshake")),
            "{error:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_peer_that_never_completes_the_handshake_times_out() {
        let ca = generate_ca("grid-ca").expect("ca");
        let client = generate_site_cert(&ca, "poller").expect("client cert");
        let (client_chain, client_key) = material(&client.cert_pem, &client.key_pem);
        // A plain TCP listener that accepts the connection but never speaks TLS: a
        // black hole in the handshake phase, before any body.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        tokio::spawn(async move {
            let (_tcp, _) = listener.accept().await.expect("accept");
            tokio::time::sleep(Duration::from_secs(30)).await;
        });

        install_ring();

        let source = PeerScraper::new(
            ca.cert_pem.as_bytes(),
            client_chain,
            client_key,
            &addr,
            sni(),
            "east.grid.internal",
            "/v1/site/signals",
            &spiffe_id("east"),
            Duration::from_millis(200),
            Duration::from_secs(2),
        )
        .expect("source");

        let started = std::time::Instant::now();
        let error = source.fetch().await.expect_err("a stalled handshake must time out");
        assert!(
            matches!(&error, FetchError::Unreachable(message) if message.contains("tls handshake timed out")),
            "{error:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "it bounded the wait, no hang"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejects_a_grid_ca_cert_without_a_spiffe_san() {
        let ca = generate_ca("grid-ca").expect("ca");
        // Grid-CA-signed but carrying NO SPIFFE SAN (a misissued or misconfigured
        // peer): the chain is valid, so this checks the connector enforces the SAN
        // rule, not only the chain.
        let server = generate_dns_only_cert(&ca, "east", &["east.grid.internal".to_owned()]).expect("server cert");
        let client = generate_site_cert(&ca, "poller").expect("client cert");
        let (server_chain, server_key) = material(&server.cert_pem, &server.key_pem);
        let (client_chain, client_key) = material(&client.cert_pem, &client.key_pem);
        let addr = mock_tls_peer(server_chain, server_key, ok_response()).await;

        install_ring();

        let source = PeerScraper::new(
            ca.cert_pem.as_bytes(),
            client_chain,
            client_key,
            &addr,
            sni(),
            "east.grid.internal",
            "/v1/site/signals",
            &spiffe_id("east"),
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .expect("source");

        let error = source
            .fetch()
            .await
            .expect_err("a cert with no SPIFFE SAN must not verify");
        assert!(
            matches!(&error, FetchError::Unreachable(message) if message.contains("tls handshake")),
            "{error:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stalled_body_hits_the_read_deadline() {
        let ca = generate_ca("grid-ca").expect("ca");
        let server = generate_site_cert(&ca, "east").expect("server cert");
        let client = generate_site_cert(&ca, "poller").expect("client cert");
        let (server_chain, server_key) = material(&server.cert_pem, &server.key_pem);
        let (client_chain, client_key) = material(&client.cert_pem, &client.key_pem);
        let addr = mock_tls_peer_stalls_body(server_chain, server_key).await;

        install_ring();

        let source = PeerScraper::new(
            ca.cert_pem.as_bytes(),
            client_chain,
            client_key,
            &addr,
            sni(),
            "east.grid.internal",
            "/v1/site/signals",
            &spiffe_id("east"),
            Duration::from_secs(2),
            Duration::from_millis(200),
        )
        .expect("source");

        let started = std::time::Instant::now();
        let error = source.fetch().await.expect_err("a stalled body must time out");
        assert!(
            matches!(&error, FetchError::Unreachable(message) if message.contains("body read timed out")),
            "{error:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "it bounded the wait, no hang"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejects_a_peer_that_is_not_the_dialed_target() {
        let ca = generate_ca("grid-ca").expect("ca");
        let server = generate_site_cert(&ca, "east").expect("server cert");
        let client = generate_site_cert(&ca, "poller").expect("client cert");
        let (server_chain, server_key) = material(&server.cert_pem, &server.key_pem);
        let (client_chain, client_key) = material(&client.cert_pem, &client.key_pem);
        let addr = mock_tls_peer(server_chain, server_key, ok_response()).await;

        // A grid-CA-valid peer "east" answers, but we dialed "west". A
        // membership-only check would accept it; the ==target check must not.
        install_ring();
        let source = PeerScraper::new(
            ca.cert_pem.as_bytes(),
            client_chain,
            client_key,
            &addr,
            sni(),
            "east.grid.internal",
            "/v1/site/signals",
            &spiffe_id("west"),
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .expect("source");

        let error = source.fetch().await.expect_err("mismatch must be refused");
        assert!(
            matches!(&error, FetchError::Unauthorized(message) if message.contains("not the dialed target")),
            "{error:?}"
        );
    }

    /// Build a source dialing the grid-CA-signed `east` peer at `addr`.
    fn east_source(
        ca_pem: &[u8],
        client_chain: Vec<CertificateDer<'static>>,
        client_key: PrivateKeyDer<'static>,
        addr: &str,
    ) -> PeerScraper {
        install_ring();
        PeerScraper::new(
            ca_pem,
            client_chain,
            client_key,
            addr,
            sni(),
            "east.grid.internal",
            "/v1/site/signals",
            &spiffe_id("east"),
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .expect("source")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_oversize_body_is_refused() {
        let ca = generate_ca("grid-ca").expect("ca");
        let server = generate_site_cert(&ca, "east").expect("server cert");
        let client = generate_site_cert(&ca, "poller").expect("client cert");
        let (server_chain, server_key) = material(&server.cert_pem, &server.key_pem);
        let (client_chain, client_key) = material(&client.cert_pem, &client.key_pem);
        let big = "a".repeat(1024 * 1024 + 1);
        let response = format!(
            "HTTP/1.1 200 OK\r\nDate: Thu, 01 Jan 1970 00:00:01 GMT\r\nContent-Length: {}\r\n\r\n{big}",
            big.len()
        )
        .into_bytes();
        let addr = mock_tls_peer(server_chain, server_key, response).await;

        let error = east_source(ca.cert_pem.as_bytes(), client_chain, client_key, &addr)
            .fetch()
            .await
            .expect_err("an oversize body must be refused");
        assert!(
            matches!(&error, FetchError::TooLarge { limit } if *limit == 1024 * 1024),
            "{error:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_missing_date_is_refused() {
        let ca = generate_ca("grid-ca").expect("ca");
        let server = generate_site_cert(&ca, "east").expect("server cert");
        let client = generate_site_cert(&ca, "poller").expect("client cert");
        let (server_chain, server_key) = material(&server.cert_pem, &server.key_pem);
        let (client_chain, client_key) = material(&client.cert_pem, &client.key_pem);
        let response = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello".as_bytes().to_vec();
        let addr = mock_tls_peer(server_chain, server_key, response).await;

        let error = east_source(ca.cert_pem.as_bytes(), client_chain, client_key, &addr)
            .fetch()
            .await
            .expect_err("a response with no Date must be refused");
        assert!(matches!(&error, FetchError::NoDate), "{error:?}");
    }
}
