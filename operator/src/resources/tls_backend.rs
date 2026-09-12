//! TLS backend abstraction for the operator.
//!
//! Centralizes every TLS primitive the operator uses (client-config
//! assembly, HTTPS connectors, the probe handshake, PEM validation gates,
//! SNI parsing, and the process-wide crypto provider) behind one module so
//! the underlying TLS stack can be swapped without touching call sites.
//!
//! This is the only non-test module that names the TLS stack
//! (`rustls`/`hyper-rustls`/`tokio-rustls`); every other module refers to the
//! aliases and functions exposed here.

use std::sync::Arc;

use hyper_util::client::legacy::connect::HttpConnector;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName as RustlsServerName, pem::PemObject as _};

use crate::{metrics_scraper::MetricsScrapeError, resources::gateway_probe::GatewayProbeOutcome};

/// Maximum number of intermediate certificates accepted in a peer chain.
pub(crate) const MAX_CHAIN_DEPTH: usize = 4;

/// Maximum number of CA certificates accepted from one trust Secret.
pub(crate) const MAX_CA_CERTIFICATES: usize = 16;

/// Maximum size (bytes) of any single certificate in a chain.
pub(crate) const MAX_CERT_BYTES: usize = 16_384;

/// Maximum encoded size of one certificate bundle read from a Secret.
pub(crate) const MAX_CERT_BUNDLE_BYTES: usize = 262_144;

/// Maximum encoded size of one private key read from a Secret.
pub(crate) const MAX_PRIVATE_KEY_BYTES: usize = 65_536;

/// Maximum size for a CA PEM bundle (256 KiB).
pub(crate) const MAX_CA_PEM_BYTES: usize = 256 * 1024;

/// Maximum size for a client certificate PEM (64 KiB).
pub(crate) const MAX_CLIENT_CERT_PEM_BYTES: usize = 64 * 1024;

/// Maximum size for a client private key PEM (64 KiB).
pub(crate) const MAX_CLIENT_KEY_PEM_BYTES: usize = 64 * 1024;

/// Maximum number of certificates in a CA or client chain (scrape path).
const MAX_CERT_CHAIN_LENGTH: usize = 10;

/// Client TLS configuration threaded through probe and scrape call sites.
pub(crate) type ClientTlsConfig = Arc<rustls::ClientConfig>;

/// Validated SNI server name for a gateway probe.
pub(crate) type ServerName = RustlsServerName<'static>;

/// HTTPS connector used by the hyper-based metrics scrape and health probe.
pub(crate) type HttpsConnector = hyper_rustls::HttpsConnector<HttpConnector>;

/// Established client TLS stream produced by [`connect`].
pub(crate) type ClientTlsStream = tokio_rustls::client::TlsStream<tokio::net::TcpStream>;

/// Install the process-wide crypto provider the TLS stack requires.
///
/// Installs `ring` once, up front, so both the `hyper-rustls` client
/// (`with_native_roots`) and the `reqwest`-backed MCP probe (built with
/// `rustls-no-provider`) have a default provider regardless of which
/// reconciler runs first. A second install attempt is ignored.
pub fn init_process_crypto() {
    if rustls::crypto::ring::default_provider().install_default().is_err() {
        tracing::warn!("rustls default CryptoProvider already installed; continuing");
    }
}

/// Parse PEM-encoded CA certificates into a trust root store.
///
/// # Errors
///
/// Returns a description if no valid certificate could be parsed.
pub(crate) fn parse_ca_roots(ca_pem: &[u8]) -> Result<rustls::RootCertStore, &'static str> {
    if ca_pem.len() > MAX_CERT_BUNDLE_BYTES {
        return Err("CA certificate bundle exceeds maximum size");
    }
    let mut roots = rustls::RootCertStore::empty();
    let mut count = 0_usize;
    for cert in CertificateDer::pem_slice_iter(ca_pem) {
        let cert = cert.map_err(|_err| "CA PEM contains invalid certificate data")?;
        if cert.as_ref().len() > MAX_CERT_BYTES {
            return Err("CA certificate exceeds maximum size");
        }
        roots
            .add(cert)
            .map_err(|_err| "CA certificate failed trust store insertion")?;
        count += 1;
        if count > MAX_CA_CERTIFICATES {
            return Err("CA certificate bundle exceeds maximum certificate count");
        }
    }
    if count == 0 {
        return Err("CA PEM contains no certificates");
    }
    Ok(roots)
}

/// Parse PEM-encoded client certificate chain.
///
/// # Errors
///
/// Returns a description if parsing fails or the chain is oversized.
pub(crate) fn parse_client_certs(cert_pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, &'static str> {
    if cert_pem.len() > MAX_CERT_BUNDLE_BYTES {
        return Err("client certificate bundle exceeds maximum size");
    }
    let mut certs = Vec::new();
    for cert in CertificateDer::pem_slice_iter(cert_pem) {
        let cert = cert.map_err(|_err| "client certificate PEM is malformed")?;
        if cert.as_ref().len() > MAX_CERT_BYTES {
            return Err("client certificate exceeds maximum size");
        }
        certs.push(cert);
        if certs.len() > MAX_CHAIN_DEPTH {
            return Err("client certificate chain exceeds maximum depth");
        }
    }
    if certs.is_empty() {
        return Err("client certificate PEM contains no certificates");
    }
    Ok(certs)
}

/// Parse a PEM-encoded private key (PKCS#8 or PKCS#1 or SEC1).
///
/// # Errors
///
/// Returns a description if parsing fails.
pub(crate) fn parse_private_key(key_pem: &[u8]) -> Result<PrivateKeyDer<'static>, &'static str> {
    if key_pem.len() > MAX_PRIVATE_KEY_BYTES {
        return Err("private key PEM exceeds maximum size");
    }
    PrivateKeyDer::from_pem_slice(key_pem).map_err(|_err| "private key PEM is malformed or missing")
}

/// Build a client TLS configuration from parsed trust material.
///
/// Uses only the provided Grid CA roots (no system/native roots). When
/// `client_certs` and `client_key` are provided, configures mTLS client
/// authentication.
///
/// # Errors
///
/// Returns a description if the configuration fails.
pub(crate) fn build_tls_config(
    roots: rustls::RootCertStore,
    client_certs: Option<Vec<CertificateDer<'static>>>,
    client_key: Option<PrivateKeyDer<'static>>,
) -> Result<ClientTlsConfig, &'static str> {
    let provider = rustls::crypto::ring::default_provider();
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|_err| "failed to configure TLS protocol versions")?
        .with_root_certificates(roots);
    let config = match (client_certs, client_key) {
        (Some(certs), Some(key)) => builder
            .with_client_auth_cert(certs, key)
            .map_err(|_err| "client certificate and key are incompatible")?,
        (None, None) => builder.with_no_client_auth(),
        _ => return Err("client certificate and key must both be present or both absent"),
    };
    Ok(Arc::new(config))
}

/// Extract the first DER certificate from a PEM string.
///
/// # Errors
///
/// Returns a bounded description if the PEM is oversized, malformed, or
/// contains no certificate.
pub(crate) fn first_cert_der_from_pem(pem: &str) -> Result<Vec<u8>, &'static str> {
    if pem.len() > MAX_CERT_BUNDLE_BYTES {
        return Err("advertised certificate PEM exceeds maximum size");
    }
    let cert = CertificateDer::pem_slice_iter(pem.as_bytes())
        .next()
        .ok_or("advertised certificate PEM contains no certificate")?
        .map_err(|_err| "advertised certificate PEM is malformed")?;
    if cert.as_ref().len() > MAX_CERT_BYTES {
        return Err("advertised certificate exceeds maximum size");
    }
    Ok(cert.as_ref().to_vec())
}

/// Parse and validate an SNI server name.
///
/// # Errors
///
/// Returns `Err(())` if the name is not a valid DNS name.
pub(crate) fn parse_server_name(server_name: &str) -> Result<ServerName, ()> {
    RustlsServerName::try_from(server_name.to_owned()).map_err(|_err| ())
}

/// Build a client TLS configuration from raw PEM bytes.
///
/// `ca_pem` is the CA certificate chain (required). `client_cert_pem` and
/// `client_key_pem` are the client identity for mTLS (both required together,
/// or both absent for one-way TLS).
///
/// # Security invariant
///
/// Private key bytes are consumed by the TLS stack and never written to logs,
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
pub(crate) fn build_tls_client_config(
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

/// Build an HTTPS connector using native root certificates.
///
/// # Errors
///
/// Returns [`MetricsScrapeError::Transport`] if native roots cannot be loaded.
pub(crate) fn build_native_connector() -> Result<HttpsConnector, MetricsScrapeError> {
    hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .map(|b| b.https_or_http().enable_http1().build())
        .map_err(|e| MetricsScrapeError::Transport(e.into()))
}

/// Build an HTTPS-only connector using a custom client TLS configuration.
pub(crate) fn build_custom_tls_connector(config: &ClientTlsConfig) -> HttpsConnector {
    hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config((**config).clone())
        .https_only()
        .enable_http1()
        .build()
}

/// Perform the client TLS handshake over an established TCP stream.
///
/// # Errors
///
/// Returns the underlying I/O error if the handshake fails.
pub(crate) async fn connect(
    tcp_stream: tokio::net::TcpStream,
    config: &ClientTlsConfig,
    server_name: &ServerName,
) -> Result<ClientTlsStream, std::io::Error> {
    let connector = tokio_rustls::TlsConnector::from(Arc::clone(config));
    connector.connect(server_name.clone(), tcp_stream).await
}

/// Borrow the peer certificate chain (DER) from an established stream.
///
/// Returns `None` if the peer presented no certificates. The slices borrow
/// from `tls_stream`, so no certificate bytes are copied.
pub(crate) fn peer_chain_der(tls_stream: &ClientTlsStream) -> Option<Vec<&[u8]>> {
    let (_, server_conn) = tls_stream.get_ref();
    Some(server_conn.peer_certificates()?.iter().map(AsRef::as_ref).collect())
}

/// Classify a TLS/handshake I/O error into a `GatewayProbeOutcome`.
///
/// Never exposes the raw error string in the outcome.
pub(crate) fn classify_tls_error(err: &std::io::Error) -> GatewayProbeOutcome {
    if let Some(rustls_err) = err.get_ref().and_then(|inner| inner.downcast_ref::<rustls::Error>()) {
        return classify_rustls_error(rustls_err);
    }
    if err.kind() == std::io::ErrorKind::ConnectionRefused {
        return GatewayProbeOutcome::ConnectionFailed;
    }
    if err.kind() == std::io::ErrorKind::TimedOut {
        return GatewayProbeOutcome::ConnectTimeout;
    }
    GatewayProbeOutcome::TlsProtocolError
}

/// Map a rustls error to a `GatewayProbeOutcome`.
#[expect(clippy::wildcard_enum_match_arm, reason = "external type with many variants")]
pub(crate) fn classify_rustls_error(err: &rustls::Error) -> GatewayProbeOutcome {
    use rustls::{CertificateError, Error};

    match err {
        Error::InvalidCertificate(cert_err) => match cert_err {
            CertificateError::UnknownIssuer => GatewayProbeOutcome::UntrustedIssuer,
            CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. } => {
                GatewayProbeOutcome::IdentityMismatch
            },
            CertificateError::Expired | CertificateError::ExpiredContext { .. } => {
                GatewayProbeOutcome::CertificateExpired
            },
            CertificateError::NotValidYet | CertificateError::NotValidYetContext { .. } => {
                GatewayProbeOutcome::CertificateNotYetValid
            },
            _ => GatewayProbeOutcome::TrustMaterialInvalid,
        },
        _ => GatewayProbeOutcome::TlsProtocolError,
    }
}

/// Structurally validate that `pem` decodes to at least one well-formed
/// certificate.
///
/// # Errors
///
/// Returns a description if the PEM is malformed or contains no certificates.
pub(crate) fn validate_pem_certificates(pem: &[u8]) -> Result<(), String> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    if certs.is_empty() {
        return Err("PEM contains no certificates".to_owned());
    }
    Ok(())
}

/// Structurally validate that `pem` decodes to a well-formed private key.
///
/// # Errors
///
/// Returns a description if the key PEM is malformed.
pub(crate) fn validate_pem_private_key(pem: &[u8]) -> Result<(), String> {
    PrivateKeyDer::from_pem_slice(pem)
        .map(|_key| ())
        .map_err(|e| e.to_string())
}
