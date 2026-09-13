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
//! let text = scrape_metrics("http://backend:9090/metrics", Duration::from_secs(5), None).await?;
//! let signals = parse_prometheus_text(&text, &names);
//! let metrics = signals.into_backend_metrics();
//! state.set_metrics(provider_name.to_owned(), metrics);
//! ```
//!
//! ## TLS and mTLS
//!
//! When a [`rustls::ClientConfig`] is provided via the `tls_config` parameter,
//! the scraper uses it for server verification and (when configured) client
//! certificate presentation.  When `tls_config` is `None`, native root
//! certificates are used (backward-compatible).  There is no
//! `insecureSkipVerify` option.
//!
//! [`rustls::ClientConfig`]: rustls::ClientConfig

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use http_body_util::{BodyExt as _, Empty, Limited};
use hyper_util::{client::legacy::Client as HyperClient, rt::TokioExecutor};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject as _};
use sha2::{Digest as _, Sha256};

// ---------------------------------------------------------------------------
// Bounded input limits
// ---------------------------------------------------------------------------

/// Maximum size for a CA PEM bundle (256 KiB).
const MAX_CA_PEM_BYTES: usize = 256 * 1024;

/// Maximum size for a client certificate PEM (64 KiB).
const MAX_CLIENT_CERT_PEM_BYTES: usize = 64 * 1024;

/// Maximum size for a client private key PEM (64 KiB).
const MAX_CLIENT_KEY_PEM_BYTES: usize = 64 * 1024;

/// Maximum number of certificates in a CA or client chain.
const MAX_CERT_CHAIN_LENGTH: usize = 10;

/// Maximum size for a metrics response body (1 MiB).
const MAX_RESPONSE_BODY_BYTES: usize = 1024 * 1024;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Error returned by [`scrape_metrics`].
#[derive(Debug, thiserror::Error)]
pub enum MetricsScrapeError {
    /// The URL could not be parsed.
    #[error("invalid metrics URL: {0}")]
    InvalidUrl(String),

    /// TLS is configured but the URL does not use HTTPS.
    #[error("metrics TLS configured but URL scheme is not https: {0}")]
    HttpWithTls(String),

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

    /// TLS material could not be parsed or assembled into a valid configuration.
    #[error("metrics TLS material error: {0}")]
    TlsMaterial(String),
}

// ---------------------------------------------------------------------------
// Scrape
// ---------------------------------------------------------------------------

/// Scrape the Prometheus text exposition from `url`.
///
/// Makes an HTTP GET request to `url` and returns the response body as a
/// `String` if the status is 2xx.  The caller is responsible for parsing
/// the returned text with [`crate::metrics_parser::parse_prometheus_text`].
///
/// When `tls_config` is `Some`, the connector uses the provided
/// [`rustls::ClientConfig`] for server verification and optional client
/// certificate presentation.  When `None`, native root certificates are
/// used (backward-compatible).
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
pub async fn scrape_metrics(
    url: &str,
    timeout: Duration,
    tls_config: Option<Arc<rustls::ClientConfig>>,
) -> Result<String, MetricsScrapeError> {
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

    if tls_config.is_some() && uri.scheme_str() != Some("https") {
        return Err(MetricsScrapeError::HttpWithTls(url.to_owned()));
    }

    let connector = if let Some(config) = &tls_config {
        build_custom_tls_connector(config)
    } else {
        build_native_connector()?
    };
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

    let body_bytes = Limited::new(response.into_body(), MAX_RESPONSE_BODY_BYTES)
        .collect()
        .await
        .map_err(|e| {
            if e.downcast_ref::<http_body_util::LengthLimitError>().is_some() {
                MetricsScrapeError::Transport(
                    format!("metrics response body exceeds {MAX_RESPONSE_BODY_BYTES} byte limit").into(),
                )
            } else {
                MetricsScrapeError::Transport(e)
            }
        })?
        .to_bytes();

    // Vec::from reclaims the buffer when this Bytes uniquely owns it, which it
    // does whenever the body arrived in one chunk or was aggregated into one.
    // to_vec copied the whole body unconditionally, up to the megabyte cap,
    // once per peer per round, to hand it straight to a validator that would
    // have been happy to read it where it lay.
    String::from_utf8(Vec::from(body_bytes)).map_err(MetricsScrapeError::Encoding)
}

// ---------------------------------------------------------------------------
// TLS client config builder
// ---------------------------------------------------------------------------

/// Build a [`rustls::ClientConfig`] from raw PEM bytes.
///
/// `ca_pem` is the CA certificate chain (required).  `client_cert_pem` and
/// `client_key_pem` are the client identity for mTLS (both required together,
/// or both absent for one-way TLS).
///
/// # Security invariant
///
/// Private key bytes are consumed by `rustls` and never written to logs,
/// events, status fields, or Prometheus labels.
///
/// # Errors
///
/// Returns [`MetricsScrapeError::TlsMaterial`] when PEM parsing fails or the
/// material is structurally invalid.
#[expect(
    clippy::too_many_lines,
    reason = "sequential PEM parsing for CA, client cert, and client key with validation"
)]
pub fn build_tls_client_config(
    ca_pem: &[u8],
    client_cert_pem: Option<&[u8]>,
    client_key_pem: Option<&[u8]>,
) -> Result<rustls::ClientConfig, MetricsScrapeError> {
    if ca_pem.len() > MAX_CA_PEM_BYTES {
        return Err(MetricsScrapeError::TlsMaterial(format!(
            "CA PEM exceeds maximum size ({} bytes > {MAX_CA_PEM_BYTES})",
            ca_pem.len()
        )));
    }

    let mut root_store = rustls::RootCertStore::empty();
    let ca_certs = CertificateDer::pem_slice_iter(ca_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| MetricsScrapeError::TlsMaterial(format!("CA PEM parse failed: {e}")))?;
    if ca_certs.is_empty() {
        return Err(MetricsScrapeError::TlsMaterial(
            "CA PEM contains no certificates".to_owned(),
        ));
    }
    if ca_certs.len() > MAX_CERT_CHAIN_LENGTH {
        return Err(MetricsScrapeError::TlsMaterial(format!(
            "CA PEM contains too many certificates ({} > {MAX_CERT_CHAIN_LENGTH})",
            ca_certs.len()
        )));
    }
    for cert in &ca_certs {
        root_store
            .add(cert.clone())
            .map_err(|e| MetricsScrapeError::TlsMaterial(format!("CA certificate invalid: {e}")))?;
    }

    let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

    let config = match (client_cert_pem, client_key_pem) {
        (Some(cert_pem), Some(key_pem)) => {
            if cert_pem.len() > MAX_CLIENT_CERT_PEM_BYTES {
                return Err(MetricsScrapeError::TlsMaterial(format!(
                    "client cert PEM exceeds maximum size ({} bytes > {MAX_CLIENT_CERT_PEM_BYTES})",
                    cert_pem.len()
                )));
            }
            if key_pem.len() > MAX_CLIENT_KEY_PEM_BYTES {
                return Err(MetricsScrapeError::TlsMaterial(format!(
                    "client key PEM exceeds maximum size ({} bytes > {MAX_CLIENT_KEY_PEM_BYTES})",
                    key_pem.len()
                )));
            }
            let certs = CertificateDer::pem_slice_iter(cert_pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| MetricsScrapeError::TlsMaterial(format!("client cert PEM parse failed: {e}")))?;
            if certs.is_empty() {
                return Err(MetricsScrapeError::TlsMaterial(
                    "client cert PEM contains no certificates".to_owned(),
                ));
            }
            if certs.len() > MAX_CERT_CHAIN_LENGTH {
                return Err(MetricsScrapeError::TlsMaterial(format!(
                    "client cert PEM contains too many certificates ({} > {MAX_CERT_CHAIN_LENGTH})",
                    certs.len()
                )));
            }
            let key = PrivateKeyDer::from_pem_slice(key_pem)
                .map_err(|e| MetricsScrapeError::TlsMaterial(format!("client key PEM parse failed: {e}")))?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| MetricsScrapeError::TlsMaterial(format!("client identity construction failed: {e}")))?
        },
        (None, None) => builder.with_no_client_auth(),
        _ => {
            return Err(MetricsScrapeError::TlsMaterial(
                "client cert and key must both be present or both absent".to_owned(),
            ));
        },
    };

    Ok(config)
}

// ---------------------------------------------------------------------------
// Connector builders
// ---------------------------------------------------------------------------

/// Build an HTTPS connector using native root certificates.
fn build_native_connector()
-> Result<hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>, MetricsScrapeError> {
    hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .map(|b| b.https_or_http().enable_http1().build())
        .map_err(|e| MetricsScrapeError::Transport(e.into()))
}

/// Build a client config that accepts only the pins declared for one peer.
///
/// Reuses the ordinary config for the CA roots and client identity, then swaps
/// the server verifier for one that also requires the leaf to be a key this
/// site wrote down for that peer.
///
/// # Errors
///
/// Returns [`MetricsScrapeError::TlsMaterial`] when the material cannot be
/// parsed, or when no pins are declared: dialling a peer whose key we have not
/// written down is the case this exists to refuse.
pub fn build_pinned_client_config(
    ca_pem: &[u8],
    client_cert_pem: Option<&[u8]>,
    client_key_pem: Option<&[u8]>,
    pins: &[String],
) -> Result<rustls::ClientConfig, MetricsScrapeError> {
    if pins.is_empty() {
        return Err(MetricsScrapeError::TlsMaterial(
            "no declared fingerprints for this peer".to_owned(),
        ));
    }
    let mut config = build_tls_client_config(ca_pem, client_cert_pem, client_key_pem)?;
    let verifier = pinned_verifier(ca_pem, pins, config.crypto_provider().signature_verification_algorithms)?;
    config.dangerous().set_certificate_verifier(Arc::new(verifier));
    Ok(config)
}

/// The verifier [`build_pinned_client_config`] installs.
///
/// Split out so a test can exercise the same construction the poller uses.
fn pinned_verifier(
    ca_pem: &[u8],
    pins: &[String],
    algorithms: rustls::crypto::WebPkiSupportedAlgorithms,
) -> Result<PinnedPeer, MetricsScrapeError> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(ca_pem) {
        let cert = cert.map_err(|e| MetricsScrapeError::TlsMaterial(format!("CA PEM parse failed: {e}")))?;
        roots
            .add(cert)
            .map_err(|e| MetricsScrapeError::TlsMaterial(format!("CA rejected: {e}")))?;
    }
    let declared = pins.iter().map(|pin| decode_pin(pin)).collect::<Result<_, _>>()?;
    Ok(PinnedPeer {
        roots: Arc::new(roots),
        algorithms,
        pins: declared,
    })
}

/// Accepts a peer whose leaf certificate is one this site declared.
///
/// Hostname verification is deliberately not performed. Membership advertises
/// an IP and a site certificate carries a DNS name, so the two never match, and
/// checking the name would add nothing once the exact key is known. The pin is
/// the stronger statement: not "something the authority signed for this name"
/// but "this key, which we wrote down".
///
/// This is the same rule the listener applies to callers, pointed the other
/// way, so a site deals only with peers it has declared in both directions.
#[derive(Debug)]
pub(crate) struct PinnedPeer {
    /// Authorities the chain is verified against.
    roots: Arc<rustls::RootCertStore>,
    /// Algorithms the chain and its signatures may use.
    algorithms: rustls::crypto::WebPkiSupportedAlgorithms,
    /// Digests this site declared for the peer, decoded once.
    pins: Vec<[u8; 32]>,
}

/// Decodes a declared fingerprint to the digest bytes it names.
///
/// The CRD constrains these to lowercase hex, so the tolerance here is only so
/// a hand-edited pin fails as a bad pin rather than as a rejected peer.
/// Anything that is not 32 bytes of hex is an error: a pin that cannot be
/// decoded can never match, and silently keeping it would refuse the peer for a
/// reason nobody can see.
fn decode_pin(pin: &str) -> Result<[u8; 32], MetricsScrapeError> {
    let hex: String = pin.chars().filter(|c| *c != ':').collect();
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2).unwrap_or(""), 16))
        .collect::<Result<_, _>>()
        .map_err(|source| MetricsScrapeError::TlsMaterial(format!("fingerprint is not hex: {pin}: {source}")))?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        MetricsScrapeError::TlsMaterial(format!("fingerprint is {} bytes, not 32: {pin}", bytes.len()))
    })
}

impl rustls::client::danger::ServerCertVerifier for PinnedPeer {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // Not WebPkiServerVerifier: it ends in verify_server_name, and peers are
        // dialled at an advertised IP, so that check fails every handshake.
        let _ = (server_name, ocsp_response);
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &rustls::server::ParsedCertificate::try_from(end_entity)?,
            &self.roots,
            intermediates,
            now,
            self.algorithms.all,
        )?;
        let presented = Sha256::digest(end_entity);
        if self.pins.iter().any(|pin| pin == presented.as_slice()) {
            return Ok(rustls::client::danger::ServerCertVerified::assertion());
        }
        Err(rustls::Error::General(
            "peer presented a certificate this site has not declared".to_owned(),
        ))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// Builds an HTTPS-only connector from an already-built client config.
pub(crate) fn build_custom_tls_connector(
    config: &rustls::ClientConfig,
) -> hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector> {
    hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(config.clone())
        .https_only()
        .enable_http1()
        .build()
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
        let result = scrape_metrics(&url, Duration::from_secs(5), None).await;
        assert!(result.is_ok(), "HTTP 200 must succeed: {result:?}");
        let text = result.unwrap_or_else(|_| std::process::abort());
        assert!(text.contains("test_metric"), "body must be in scrape result");
        let _ = body; // referenced for documentation
    }

    #[tokio::test]
    async fn scrape_returns_error_for_non_2xx() {
        let response = b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
        let url = start_test_server(response).await;
        let result = scrape_metrics(&url, Duration::from_secs(5), None).await;
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
        let result = scrape_metrics(&url, Duration::from_millis(100), None).await;
        assert!(result.is_err(), "silent server must time out");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::Timeout(_)),
            "error must be Timeout"
        );
    }

    #[tokio::test]
    async fn scrape_returns_error_for_connection_refused() {
        // Port 1 is never open on any standard OS.
        let result = scrape_metrics("http://127.0.0.1:1/metrics", Duration::from_secs(5), None).await;
        assert!(result.is_err(), "connection refused must return an error");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::Transport(_)),
            "error must be Transport"
        );
    }

    #[tokio::test]
    async fn scrape_returns_invalid_url_for_unsupported_scheme() {
        let result = scrape_metrics("ftp://example.com/metrics", Duration::from_secs(5), None).await;
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

    // -----------------------------------------------------------------------
    // build_tls_client_config — PEM material validation
    // -----------------------------------------------------------------------

    #[test]
    fn build_tls_config_valid_ca_only() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let result = build_tls_client_config(ca.cert_pem.as_bytes(), None, None);
        assert!(result.is_ok(), "valid CA PEM must produce a ClientConfig: {result:?}");
    }

    #[test]
    fn build_tls_config_valid_mtls() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let client = certs::generate_dns_cert(&ca, "client", "localhost").unwrap();
        let result = build_tls_client_config(
            ca.cert_pem.as_bytes(),
            Some(client.cert_pem.as_bytes()),
            Some(client.key_pem.as_bytes()),
        );
        assert!(result.is_ok(), "valid CA + client cert + key must succeed: {result:?}");
    }

    #[test]
    fn build_tls_config_empty_ca_pem_fails() {
        let result = build_tls_client_config(b"", None, None);
        assert!(result.is_err(), "empty CA PEM must fail");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::TlsMaterial(_)),
            "error must be TlsMaterial"
        );
    }

    #[test]
    fn build_tls_config_garbage_ca_pem_fails() {
        let result = build_tls_client_config(b"not valid pem data", None, None);
        assert!(result.is_err(), "garbage CA PEM must fail");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::TlsMaterial(_)),
            "error must be TlsMaterial"
        );
    }

    #[test]
    fn build_tls_config_client_cert_without_key_fails() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let client = certs::generate_dns_cert(&ca, "client", "localhost").unwrap();
        let result = build_tls_client_config(ca.cert_pem.as_bytes(), Some(client.cert_pem.as_bytes()), None);
        assert!(result.is_err(), "client cert without key must fail");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::TlsMaterial(_)),
            "error must be TlsMaterial"
        );
    }

    #[test]
    fn build_tls_config_client_key_without_cert_fails() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let client = certs::generate_dns_cert(&ca, "client", "localhost").unwrap();
        let result = build_tls_client_config(ca.cert_pem.as_bytes(), None, Some(client.key_pem.as_bytes()));
        assert!(result.is_err(), "client key without cert must fail");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::TlsMaterial(_)),
            "error must be TlsMaterial"
        );
    }

    #[test]
    fn build_tls_config_mismatched_client_cert_and_key_fails() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let cert_a = certs::generate_dns_cert(&ca, "client-a", "a.localhost").unwrap();
        let cert_b = certs::generate_dns_cert(&ca, "client-b", "b.localhost").unwrap();
        let result = build_tls_client_config(
            ca.cert_pem.as_bytes(),
            Some(cert_a.cert_pem.as_bytes()),
            Some(cert_b.key_pem.as_bytes()),
        );
        assert!(result.is_err(), "mismatched client cert and key must fail");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("identity construction failed"),
            "error must indicate identity mismatch: {err_msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Bounded input limits
    // -----------------------------------------------------------------------

    #[test]
    fn build_tls_config_rejects_oversized_ca_pem() {
        let oversized = vec![b'A'; MAX_CA_PEM_BYTES + 1];
        let result = build_tls_client_config(&oversized, None, None);
        assert!(result.is_err(), "oversized CA PEM must fail");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("exceeds maximum size"),
            "error must mention size limit: {msg}"
        );
    }

    #[test]
    fn build_tls_config_rejects_oversized_client_cert_pem() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let oversized = vec![b'A'; MAX_CLIENT_CERT_PEM_BYTES + 1];
        let client_cert = certs::generate_dns_cert(&ca, "client", "localhost").unwrap();
        let result = build_tls_client_config(
            ca.cert_pem.as_bytes(),
            Some(&oversized),
            Some(client_cert.key_pem.as_bytes()),
        );
        assert!(result.is_err(), "oversized client cert PEM must fail");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("exceeds maximum size"),
            "error must mention size limit: {msg}"
        );
    }

    #[test]
    fn build_tls_config_rejects_oversized_client_key_pem() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let client_cert = certs::generate_dns_cert(&ca, "client", "localhost").unwrap();
        let oversized = vec![b'A'; MAX_CLIENT_KEY_PEM_BYTES + 1];
        let result = build_tls_client_config(
            ca.cert_pem.as_bytes(),
            Some(client_cert.cert_pem.as_bytes()),
            Some(&oversized),
        );
        assert!(result.is_err(), "oversized client key PEM must fail");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("exceeds maximum size"),
            "error must mention size limit: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // TLS server helper
    // -----------------------------------------------------------------------

    /// Start a one-shot TLS server on localhost and return the URL.
    ///
    /// `client_ca_pem`: when `Some`, the server requires client certificate
    /// authentication (mTLS).  When `None`, one-way TLS only.
    #[expect(
        clippy::too_many_lines,
        reason = "TLS server setup: certs + verifier + acceptor + one-shot handler"
    )]
    async fn start_tls_test_server(
        server_cert_pem: &str,
        server_key_pem: &str,
        client_ca_pem: Option<&str>,
        response: Vec<u8>,
    ) -> String {
        let server_certs = CertificateDer::pem_slice_iter(server_cert_pem.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let server_key = PrivateKeyDer::from_pem_slice(server_key_pem.as_bytes()).unwrap();

        let server_config = if let Some(ca) = client_ca_pem {
            let mut root_store = rustls::RootCertStore::empty();
            let ca_certs = CertificateDer::pem_slice_iter(ca.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            for cert in &ca_certs {
                root_store.add(cert.clone()).unwrap();
            }
            let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
                .build()
                .unwrap();
            rustls::ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(server_certs, server_key)
                .unwrap()
        } else {
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(server_certs, server_key)
                .unwrap()
        };

        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await
                && let Ok(tls_stream) = acceptor.accept(stream).await
            {
                let (mut reader, mut writer) = tokio::io::split(tls_stream);
                let mut buf = [0_u8; 4096];
                drop(reader.read(&mut buf).await);
                drop(writer.write_all(&response).await);
            }
        });

        format!("https://localhost:{port}")
    }

    // -----------------------------------------------------------------------
    // End-to-end TLS scrape tests (real certificates, real handshakes)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn scrape_tls_with_matching_ca_succeeds() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let server_cert = certs::generate_dns_cert(&ca, "test-server", "localhost").unwrap();

        let url = start_tls_test_server(
            &server_cert.cert_pem,
            &server_cert.key_pem,
            None,
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 16\r\n\r\ntest_metric 1.0\n".to_vec(),
        )
        .await;

        let tls_config = build_tls_client_config(ca.cert_pem.as_bytes(), None, None).unwrap();
        let result = scrape_metrics(&url, Duration::from_secs(5), Some(Arc::new(tls_config))).await;
        assert!(result.is_ok(), "TLS scrape with matching CA must succeed: {result:?}");
        assert!(
            result.unwrap().contains("test_metric"),
            "response body must contain the scraped metric"
        );
    }

    #[tokio::test]
    async fn scrape_tls_with_wrong_ca_fails() {
        let ca_server = certs::generate_ca("server-ca").unwrap();
        let ca_client = certs::generate_ca("wrong-ca").unwrap();
        let server_cert = certs::generate_dns_cert(&ca_server, "test-server", "localhost").unwrap();

        let url = start_tls_test_server(
            &server_cert.cert_pem,
            &server_cert.key_pem,
            None,
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_vec(),
        )
        .await;

        let tls_config = build_tls_client_config(ca_client.cert_pem.as_bytes(), None, None).unwrap();
        let result = scrape_metrics(&url, Duration::from_secs(5), Some(Arc::new(tls_config))).await;
        assert!(result.is_err(), "TLS scrape with wrong CA must fail");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::Transport(_)),
            "error must be Transport (TLS verification failure)"
        );
    }

    #[tokio::test]
    async fn scrape_mtls_with_valid_client_cert_succeeds() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let server_cert = certs::generate_dns_cert(&ca, "test-server", "localhost").unwrap();
        let client_cert = certs::generate_dns_cert(&ca, "test-client", "client.local").unwrap();

        let url = start_tls_test_server(
            &server_cert.cert_pem,
            &server_cert.key_pem,
            Some(&ca.cert_pem),
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 16\r\n\r\ntest_metric 2.0\n".to_vec(),
        )
        .await;

        let tls_config = build_tls_client_config(
            ca.cert_pem.as_bytes(),
            Some(client_cert.cert_pem.as_bytes()),
            Some(client_cert.key_pem.as_bytes()),
        )
        .unwrap();
        let result = scrape_metrics(&url, Duration::from_secs(5), Some(Arc::new(tls_config))).await;
        assert!(
            result.is_ok(),
            "mTLS scrape with valid client cert must succeed: {result:?}"
        );
        assert!(
            result.unwrap().contains("test_metric"),
            "response body must contain the scraped metric"
        );
    }

    #[tokio::test]
    async fn scrape_mtls_without_client_cert_fails() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let server_cert = certs::generate_dns_cert(&ca, "test-server", "localhost").unwrap();

        let url = start_tls_test_server(
            &server_cert.cert_pem,
            &server_cert.key_pem,
            Some(&ca.cert_pem),
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_vec(),
        )
        .await;

        let tls_config = build_tls_client_config(ca.cert_pem.as_bytes(), None, None).unwrap();
        let result = scrape_metrics(&url, Duration::from_secs(5), Some(Arc::new(tls_config))).await;
        assert!(result.is_err(), "mTLS scrape without client cert must fail");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::Transport(_)),
            "error must be Transport (server rejected missing client cert)"
        );
    }

    // -----------------------------------------------------------------------
    // TLS negative test coverage (Item 6)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn scrape_tls_hostname_mismatch_fails() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let server_cert = certs::generate_dns_cert(&ca, "test-server", "wrong.example.com").unwrap();

        let url = start_tls_test_server(
            &server_cert.cert_pem,
            &server_cert.key_pem,
            None,
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_vec(),
        )
        .await;

        let tls_config = build_tls_client_config(ca.cert_pem.as_bytes(), None, None).unwrap();
        let result = scrape_metrics(&url, Duration::from_secs(5), Some(Arc::new(tls_config))).await;
        assert!(
            result.is_err(),
            "TLS scrape with hostname mismatch must fail (SAN has wrong.example.com, connecting to localhost)"
        );
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::Transport(_)),
            "error must be Transport (hostname verification failure)"
        );
    }

    #[tokio::test]
    async fn scrape_tls_expired_server_cert_fails() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let expired_cert = certs::generate_expired_dns_cert(&ca, "test-server", "localhost").unwrap();

        let url = start_tls_test_server(
            &expired_cert.cert_pem,
            &expired_cert.key_pem,
            None,
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_vec(),
        )
        .await;

        let tls_config = build_tls_client_config(ca.cert_pem.as_bytes(), None, None).unwrap();
        let result = scrape_metrics(&url, Duration::from_secs(5), Some(Arc::new(tls_config))).await;
        assert!(result.is_err(), "TLS scrape with expired server cert must fail");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::Transport(_)),
            "error must be Transport (expired certificate)"
        );
    }

    #[tokio::test]
    async fn scrape_tls_not_yet_valid_server_cert_fails() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let future_cert = certs::generate_not_yet_valid_dns_cert(&ca, "test-server", "localhost").unwrap();

        let url = start_tls_test_server(
            &future_cert.cert_pem,
            &future_cert.key_pem,
            None,
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_vec(),
        )
        .await;

        let tls_config = build_tls_client_config(ca.cert_pem.as_bytes(), None, None).unwrap();
        let result = scrape_metrics(&url, Duration::from_secs(5), Some(Arc::new(tls_config))).await;
        assert!(result.is_err(), "TLS scrape with not-yet-valid server cert must fail");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::Transport(_)),
            "error must be Transport (certificate not yet valid)"
        );
    }

    #[tokio::test]
    async fn scrape_401_returns_non_ok_status() {
        let response = b"HTTP/1.0 401 Unauthorized\r\nContent-Length: 0\r\n\r\n";
        let url = start_test_server(response).await;
        let result = scrape_metrics(&url, Duration::from_secs(5), None).await;
        assert!(result.is_err(), "HTTP 401 must return an error");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::NonOkStatus { status: 401, .. }),
            "error must be NonOkStatus(401)"
        );
    }

    #[tokio::test]
    async fn scrape_403_returns_non_ok_status() {
        let response = b"HTTP/1.0 403 Forbidden\r\nContent-Length: 0\r\n\r\n";
        let url = start_test_server(response).await;
        let result = scrape_metrics(&url, Duration::from_secs(5), None).await;
        assert!(result.is_err(), "HTTP 403 must return an error");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::NonOkStatus { status: 403, .. }),
            "error must be NonOkStatus(403)"
        );
    }

    // -----------------------------------------------------------------------
    // Bounded response body (streaming rejection)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn scrape_rejects_oversized_response_body_during_streaming() {
        let body_size = MAX_RESPONSE_BODY_BYTES + 1;
        let header = format!("HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {body_size}\r\n\r\n");
        let mut response_bytes = header.into_bytes();
        response_bytes.resize(response_bytes.len() + body_size, b'x');

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|_| std::process::abort());
        let port = listener.local_addr().unwrap_or_else(|_| std::process::abort()).port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0_u8; 4096];
                drop(stream.read(&mut buf).await);
                drop(stream.write_all(&response_bytes).await);
            }
        });

        let url = format!("http://127.0.0.1:{port}/metrics");
        let result = scrape_metrics(&url, Duration::from_secs(10), None).await;
        assert!(result.is_err(), "oversized response body must be rejected");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("limit"), "error must mention size limit: {msg}");
    }

    // -----------------------------------------------------------------------
    // Error message safety: no secret bytes in diagnostics
    // -----------------------------------------------------------------------

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "exhaustive PEM-leak check across multiple error paths"
    )]
    fn error_messages_never_contain_pem_material() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let client = certs::generate_dns_cert(&ca, "client", "localhost").unwrap();

        let cases: Vec<(&str, Result<rustls::ClientConfig, MetricsScrapeError>)> = vec![
            ("empty CA", build_tls_client_config(b"", None, None)),
            ("garbage CA", build_tls_client_config(b"not valid", None, None)),
            ("mismatched cert/key", {
                let other = certs::generate_dns_cert(&ca, "other", "other.local").unwrap();
                build_tls_client_config(
                    ca.cert_pem.as_bytes(),
                    Some(client.cert_pem.as_bytes()),
                    Some(other.key_pem.as_bytes()),
                )
            }),
        ];

        for (label, result) in cases {
            if let Err(e) = result {
                let msg = e.to_string();
                assert!(
                    !msg.contains("BEGIN CERTIFICATE"),
                    "{label}: error message must not contain PEM certificate data"
                );
                assert!(
                    !msg.contains("BEGIN PRIVATE KEY"),
                    "{label}: error message must not contain PEM private key data"
                );
                assert!(
                    !msg.contains("BEGIN RSA"),
                    "{label}: error message must not contain RSA key data"
                );
                assert!(
                    !msg.contains("BEGIN EC"),
                    "{label}: error message must not contain EC key data"
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // HTTPS enforcement when TLS is configured
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn scrape_rejects_http_url_with_tls_config() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let config = Arc::new(build_tls_client_config(ca.cert_pem.as_bytes(), None, None).unwrap());
        let result = scrape_metrics("http://127.0.0.1:9090/metrics", Duration::from_secs(5), Some(config)).await;
        assert!(result.is_err(), "HTTP URL with TLS config must fail");
        assert!(
            matches!(result.unwrap_err(), MetricsScrapeError::HttpWithTls(_)),
            "error must be HttpWithTls"
        );
    }

    #[tokio::test]
    async fn scrape_allows_https_url_with_tls_config() {
        let ca = certs::generate_ca("test-ca").unwrap();
        let server = certs::generate_dns_cert(&ca, "server", "localhost").unwrap();
        let response = b"HTTP/1.0 200 OK\r\nContent-Length: 5\r\n\r\nhello";
        let url = start_tls_test_server(&server.cert_pem, &server.key_pem, None, response.to_vec()).await;
        let config = Arc::new(build_tls_client_config(ca.cert_pem.as_bytes(), None, None).unwrap());
        let result = scrape_metrics(&url, Duration::from_secs(5), Some(config)).await;
        assert!(result.is_ok(), "HTTPS URL with TLS config must succeed: {result:?}");
    }

    #[tokio::test]
    async fn scrape_allows_http_url_without_tls_config() {
        let response = b"HTTP/1.0 200 OK\r\nContent-Length: 5\r\n\r\nhello";
        let url = start_test_server(response).await;
        let result = scrape_metrics(&url, Duration::from_secs(5), None).await;
        assert!(result.is_ok(), "HTTP URL without TLS config must succeed: {result:?}");
    }
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod pinned_peer_tests {
    use rustls::{client::danger::ServerCertVerifier as _, pki_types::pem::PemObject as _};

    use super::*;

    /// A CA, and a site cert whose only SAN is a DNS name.
    fn site() -> (certs::CaCert, certs::SiteCertOutput) {
        let ca = certs::generate_ca("test-ca").unwrap();
        let site = certs::generate_site_cert(&ca, "pool-b").unwrap();
        (ca, site)
    }

    fn leaf(site: &certs::SiteCertOutput) -> CertificateDer<'static> {
        CertificateDer::pem_slice_iter(site.cert_pem.as_bytes())
            .next()
            .unwrap()
            .unwrap()
    }

    fn verify(ca: &certs::CaCert, site: &certs::SiteCertOutput, pins: &[String]) -> Result<(), rustls::Error> {
        let algorithms = rustls::crypto::CryptoProvider::get_default().map_or_else(
            || rustls::crypto::ring::default_provider().signature_verification_algorithms,
            |p| p.signature_verification_algorithms,
        );
        let verifier = pinned_verifier(ca.cert_pem.as_bytes(), pins, algorithms)
            .map_err(|e| rustls::Error::General(e.to_string()))?;
        let der = leaf(site);
        // An IP, which is what membership advertises and what the poller dials.
        let dialled = ServerName::try_from("10.89.1.231").unwrap();
        verifier
            .verify_server_cert(&der, &[], &dialled, &[], UnixTime::now())
            .map(|_| ())
    }

    #[test]
    fn a_declared_key_is_accepted_at_an_address_its_name_does_not_cover() {
        // The whole design: a site is named by its key, so the cert's DNS SAN
        // never has to match the address membership advertises. Verifying the
        // chain through WebPkiServerVerifier would fail this on the name alone.
        let (ca, site) = site();
        let pin = crate::signals::leaf_fingerprint(&leaf(&site));
        verify(&ca, &site, &[pin]).expect("a declared key is served at any address");
    }

    #[test]
    fn the_stock_verifier_would_reject_the_address_we_dial() {
        // Guards the choice above. If someone swaps the trust-anchor check back
        // to WebPkiServerVerifier::verify_server_cert, this is what they get.
        let (ca, site) = site();
        let algorithms = rustls::crypto::CryptoProvider::get_default().map_or_else(
            || rustls::crypto::ring::default_provider().signature_verification_algorithms,
            |p| p.signature_verification_algorithms,
        );
        let _ = algorithms;
        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(ca.cert_pem.as_bytes()) {
            roots.add(cert.unwrap()).unwrap();
        }
        let stock = rustls::client::WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .unwrap();
        let dialled = ServerName::try_from("10.89.1.231").unwrap();
        let error = stock
            .verify_server_cert(&leaf(&site), &[], &dialled, &[], UnixTime::now())
            .expect_err("a DNS-SAN cert is not valid for an IP");
        assert!(
            matches!(
                error,
                rustls::Error::InvalidCertificate(
                    rustls::CertificateError::NotValidForName | rustls::CertificateError::NotValidForNameContext { .. }
                )
            ),
            "expected a name failure, got {error:?}"
        );
    }

    #[test]
    fn an_undeclared_key_is_refused() {
        let (ca, site) = site();
        let other = certs::generate_site_cert(&ca, "pool-c").unwrap();
        let pin = crate::signals::leaf_fingerprint(&leaf(&other));
        verify(&ca, &site, &[pin]).expect_err("the CA signing it is not enough");
    }

    #[test]
    fn a_key_from_another_authority_is_refused() {
        let (_, site) = site();
        let stranger = certs::generate_ca("stranger-ca").unwrap();
        let pin = crate::signals::leaf_fingerprint(&leaf(&site));
        assert!(
            verify(&stranger, &site, &[pin]).is_err(),
            "the pin does not replace the chain"
        );
    }

    #[test]
    fn declaring_no_key_refuses_before_a_config_exists() {
        let ca = certs::generate_ca("test-ca").unwrap();
        build_pinned_client_config(ca.cert_pem.as_bytes(), None, None, &[])
            .expect_err("a peer with no declared key is refused");
    }

    #[test]
    fn a_hand_edited_pin_still_matches() {
        let (ca, site) = site();
        let pin = crate::signals::leaf_fingerprint(&leaf(&site));
        let typed = pin
            .to_uppercase()
            .as_bytes()
            .chunks(2)
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .collect::<Vec<_>>()
            .join(":");
        verify(&ca, &site, &[typed]).expect("uppercase and colons name the same key");
    }
}
