//! TLS gateway probe — bounded handshake and peer certificate extraction.
//!
//! Connects to a remote gateway, performs a TLS (or mTLS) handshake with
//! locally configured trust roots, extracts the peer leaf certificate, and
//! classifies the result as a `GatewayProbeOutcome`.
//!
//! All timeouts are bounded.  No private key material appears in return
//! values, log fields, or error messages.

use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _};
use tokio::time::Duration;

use crate::resources::gateway_probe::{CanonicalFingerprint, GatewayProbeOutcome, fingerprint_matches_any};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum wall-clock time for TCP connect + TLS handshake combined.
const PROBE_DEADLINE: Duration = Duration::from_secs(10);

/// Maximum TCP connect timeout (subset of [`PROBE_DEADLINE`]).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum number of intermediate certificates accepted in the chain.
const MAX_CHAIN_DEPTH: usize = 4;

/// Maximum number of CA certificates accepted from one trust Secret.
const MAX_CA_CERTIFICATES: usize = 16;

/// Maximum size (bytes) of any single certificate in the chain.
const MAX_CERT_BYTES: usize = 16_384;

/// Maximum encoded size of one certificate bundle read from a Secret.
const MAX_CERT_BUNDLE_BYTES: usize = 262_144;

/// Maximum encoded size of one private key read from a Secret.
const MAX_PRIVATE_KEY_BYTES: usize = 65_536;

// ---------------------------------------------------------------------------
// Probe configuration
// ---------------------------------------------------------------------------

/// Immutable configuration for a single gateway probe.
///
/// Constructed once per reconcile from Kubernetes resources;
/// dropped when the reconcile completes (no caching across
/// reconciles).
pub(crate) struct ProbeConfig {
    /// TCP address to connect to (host:port).
    pub address: String,

    /// rustls [`ClientConfig`] with Grid-only trust roots and optional
    /// client identity.
    ///
    /// [`ClientConfig`]: rustls::ClientConfig
    pub tls_config: Arc<rustls::ClientConfig>,

    /// Expected DNS server name for SNI and SAN verification.
    pub server_name: ServerName<'static>,

    /// Canonical DER fingerprint pins (1–2 entries).
    pub pins: Vec<CanonicalFingerprint>,

    /// Optional SWIM-advertised leaf cert DER, compared with the configured
    /// rotation pins for diagnostics only.
    pub advertised_leaf_der: Option<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// TLS material parsing
// ---------------------------------------------------------------------------

/// Parse PEM-encoded CA certificates into a rustls root store.
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

/// Build a rustls [`ClientConfig`] from parsed trust material.
///
/// Uses only the provided Grid CA roots — no system/native roots.
/// When `client_certs` and `client_key` are provided, configures
/// mTLS client authentication.
///
/// # Errors
///
/// Returns a description if the configuration fails.
///
/// [`ClientConfig`]: rustls::ClientConfig
pub(crate) fn build_tls_config(
    roots: rustls::RootCertStore,
    client_certs: Option<Vec<CertificateDer<'static>>>,
    client_key: Option<PrivateKeyDer<'static>>,
) -> Result<Arc<rustls::ClientConfig>, &'static str> {
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

// ---------------------------------------------------------------------------
// Probe execution
// ---------------------------------------------------------------------------

/// Execute a bounded TLS gateway probe.
///
/// 1. TCP connect with [`CONNECT_TIMEOUT`].
/// 2. TLS handshake under [`PROBE_DEADLINE`] (total, including connect).
/// 3. Extract peer leaf certificate DER.
/// 4. Validate canonical fingerprint pin.
/// 5. If present, compare the SWIM-advertised leaf with the pins and record a mismatch without failing the verified
///    connection.
///
/// Returns a `GatewayProbeOutcome` — never panics, never leaks
/// private material.
pub(crate) async fn probe_gateway(config: &ProbeConfig) -> GatewayProbeOutcome {
    let deadline = tokio::time::Instant::now() + PROBE_DEADLINE;

    let tcp_result = tokio::time::timeout_at(
        deadline.min(tokio::time::Instant::now() + CONNECT_TIMEOUT),
        tokio::net::TcpStream::connect(&config.address),
    )
    .await;

    let tcp_stream = match tcp_result {
        Ok(Ok(stream)) => stream,
        Ok(Err(_err)) => return GatewayProbeOutcome::ConnectionFailed,
        Err(_elapsed) => return GatewayProbeOutcome::ConnectTimeout,
    };

    let connector = tokio_rustls::TlsConnector::from(Arc::clone(&config.tls_config));
    let tls_result = tokio::time::timeout_at(deadline, connector.connect(config.server_name.clone(), tcp_stream)).await;

    let tls_stream = match tls_result {
        Ok(Ok(stream)) => stream,
        Ok(Err(err)) => return classify_tls_error(&err),
        Err(_elapsed) => return GatewayProbeOutcome::HandshakeTimeout,
    };

    verify_peer_certificate(&tls_stream, config)
}

/// Verify the peer certificate after a successful TLS handshake.
fn verify_peer_certificate(
    tls_stream: &tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
    config: &ProbeConfig,
) -> GatewayProbeOutcome {
    let (_, server_conn) = tls_stream.get_ref();
    let Some(peer_certs) = server_conn.peer_certificates() else {
        return GatewayProbeOutcome::TlsProtocolError;
    };

    if peer_certs.len() > MAX_CHAIN_DEPTH {
        return GatewayProbeOutcome::TrustMaterialInvalid;
    }
    for cert in peer_certs {
        if cert.as_ref().len() > MAX_CERT_BYTES {
            return GatewayProbeOutcome::TrustMaterialInvalid;
        }
    }

    let Some(leaf_der) = peer_certs.first() else {
        return GatewayProbeOutcome::TlsProtocolError;
    };

    let leaf_fp = CanonicalFingerprint::from_der(leaf_der.as_ref());
    if !fingerprint_matches_any(&leaf_fp, &config.pins) {
        return GatewayProbeOutcome::PinMismatch;
    }

    if let Some(advertised) = config.advertised_leaf_der.as_ref() {
        let advertised_fp = CanonicalFingerprint::from_der(advertised);
        if !fingerprint_matches_any(&advertised_fp, &config.pins) {
            return GatewayProbeOutcome::AdvertisedCertificateMismatch;
        }
    }

    GatewayProbeOutcome::Verified
}

// ---------------------------------------------------------------------------
// Error classification
// ---------------------------------------------------------------------------

/// Classify a rustls/TLS error into a `GatewayProbeOutcome`.
///
/// Never exposes the raw error string in the outcome — the controller
/// maps the outcome to bounded status reason codes.
fn classify_tls_error(err: &std::io::Error) -> GatewayProbeOutcome {
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
fn classify_rustls_error(err: &rustls::Error) -> GatewayProbeOutcome {
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

// ---------------------------------------------------------------------------
// PEM DER extraction (for advertised cert comparison)
// ---------------------------------------------------------------------------

/// Extract the first DER certificate from a PEM string.
///
/// Used to parse `status.publicCertPem` for comparison against the
/// live peer certificate.
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "tests"
)]
mod tests {
    use super::*;

    fn test_ca() -> certs::CaCert {
        certs::generate_ca("test-grid").unwrap()
    }

    fn test_site(ca: &certs::CaCert, name: &str) -> certs::SiteCertOutput {
        certs::generate_site_cert(ca, name).unwrap()
    }

    // ---- PEM parsing ----

    #[test]
    fn parse_ca_roots_from_valid_pem() {
        let ca = test_ca();
        let roots = parse_ca_roots(ca.cert_pem.as_bytes());
        assert!(roots.is_ok(), "valid CA PEM must parse: {roots:?}");
    }

    #[test]
    fn parse_ca_roots_rejects_empty() {
        let err = parse_ca_roots(b"").unwrap_err();
        assert_eq!(err, "CA PEM contains no certificates");
    }

    #[test]
    fn parse_ca_roots_rejects_garbage() {
        let err = parse_ca_roots(b"not a cert").unwrap_err();
        assert_eq!(err, "CA PEM contains no certificates");
    }

    #[test]
    fn parse_ca_roots_rejects_oversized_bundle() {
        let oversized = vec![b'x'; MAX_CERT_BUNDLE_BYTES + 1];
        assert_eq!(
            parse_ca_roots(&oversized).unwrap_err(),
            "CA certificate bundle exceeds maximum size"
        );
    }

    #[test]
    fn parse_ca_roots_rejects_too_many_certificates() {
        let ca = test_ca();
        let bundle = ca.cert_pem.repeat(MAX_CA_CERTIFICATES + 1);
        assert_eq!(
            parse_ca_roots(bundle.as_bytes()).unwrap_err(),
            "CA certificate bundle exceeds maximum certificate count"
        );
    }

    #[test]
    fn parse_client_certs_from_valid_pem() {
        let ca = test_ca();
        let site = test_site(&ca, "test-site");
        let certs = parse_client_certs(site.cert_pem.as_bytes());
        assert!(certs.is_ok(), "valid site PEM must parse: {certs:?}");
        assert!(!certs.unwrap().is_empty(), "must contain at least one cert");
    }

    #[test]
    fn parse_client_certs_rejects_empty() {
        let err = parse_client_certs(b"").unwrap_err();
        assert_eq!(err, "client certificate PEM contains no certificates");
    }

    #[test]
    fn parse_client_certs_rejects_oversized_bundle() {
        let oversized = vec![b'x'; MAX_CERT_BUNDLE_BYTES + 1];
        assert_eq!(
            parse_client_certs(&oversized).unwrap_err(),
            "client certificate bundle exceeds maximum size"
        );
    }

    #[test]
    fn parse_private_key_from_valid_pem() {
        let ca = test_ca();
        let site = test_site(&ca, "test-site");
        let key = parse_private_key(site.key_pem.as_bytes());
        assert!(key.is_ok(), "valid key PEM must parse: {key:?}");
    }

    #[test]
    fn parse_private_key_rejects_empty() {
        let err = parse_private_key(b"").unwrap_err();
        assert!(err.contains("malformed") || err.contains("missing"), "error: {err}");
    }

    #[test]
    fn parse_private_key_rejects_oversized_input() {
        let oversized = vec![b'x'; MAX_PRIVATE_KEY_BYTES + 1];
        assert_eq!(
            parse_private_key(&oversized).unwrap_err(),
            "private key PEM exceeds maximum size"
        );
    }

    // ---- TLS config builder ----

    #[test]
    fn build_tls_config_with_client_auth() {
        let ca = test_ca();
        let site = test_site(&ca, "test-site");
        let roots = parse_ca_roots(ca.cert_pem.as_bytes()).unwrap();
        let certs = parse_client_certs(site.cert_pem.as_bytes()).unwrap();
        let key = parse_private_key(site.key_pem.as_bytes()).unwrap();
        let config = build_tls_config(roots, Some(certs), Some(key));
        assert!(config.is_ok(), "mTLS config must build: {config:?}");
    }

    #[test]
    fn build_tls_config_without_client_auth() {
        let ca = test_ca();
        let roots = parse_ca_roots(ca.cert_pem.as_bytes()).unwrap();
        let config = build_tls_config(roots, None, None);
        assert!(config.is_ok(), "TLS config without client auth must build: {config:?}");
    }

    #[test]
    fn build_tls_config_rejects_mismatched_args() {
        let ca = test_ca();
        let site = test_site(&ca, "test-site");
        let roots = parse_ca_roots(ca.cert_pem.as_bytes()).unwrap();
        let certs = parse_client_certs(site.cert_pem.as_bytes()).unwrap();
        let err = build_tls_config(roots, Some(certs), None).unwrap_err();
        assert_eq!(err, "client certificate and key must both be present or both absent");
    }

    // ---- PEM DER extraction ----

    #[test]
    fn first_cert_der_from_valid_pem() {
        let ca = test_ca();
        let site = test_site(&ca, "test-site");
        let der = first_cert_der_from_pem(&site.cert_pem);
        assert!(der.is_ok(), "valid PEM must yield DER bytes");
        assert!(!der.unwrap().is_empty(), "DER must not be empty");
    }

    #[test]
    fn first_cert_der_from_invalid_pem() {
        assert!(
            first_cert_der_from_pem("not a cert").is_err(),
            "non-PEM text must be rejected"
        );
        assert!(first_cert_der_from_pem("").is_err(), "empty input must be rejected");
    }

    #[test]
    fn first_cert_der_rejects_oversized_pem() {
        let oversized = "x".repeat(MAX_CERT_BUNDLE_BYTES + 1);
        assert_eq!(
            first_cert_der_from_pem(&oversized).unwrap_err(),
            "advertised certificate PEM exceeds maximum size"
        );
    }

    // ---- Canonical fingerprint from generated cert ----

    #[test]
    fn canonical_fingerprint_from_generated_cert() {
        let ca = test_ca();
        let site = test_site(&ca, "test-site");
        let der = first_cert_der_from_pem(&site.cert_pem).unwrap();
        let fp = CanonicalFingerprint::from_der(&der);
        assert_eq!(fp.as_str().len(), 64, "canonical fingerprint must be 64 hex chars");
    }

    // ---- Error classification ----

    #[test]
    fn classify_connection_refused() {
        let err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        assert_eq!(classify_tls_error(&err), GatewayProbeOutcome::ConnectionFailed);
    }

    #[test]
    fn classify_timed_out() {
        let err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out");
        assert_eq!(classify_tls_error(&err), GatewayProbeOutcome::ConnectTimeout);
    }

    #[test]
    fn classify_unknown_io_error_as_protocol() {
        let err = std::io::Error::other("something else");
        assert_eq!(classify_tls_error(&err), GatewayProbeOutcome::TlsProtocolError);
    }

    #[test]
    fn classify_rustls_unknown_issuer() {
        let rustls_err = rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer);
        assert_eq!(classify_rustls_error(&rustls_err), GatewayProbeOutcome::UntrustedIssuer);
    }

    #[test]
    fn classify_rustls_not_valid_for_name() {
        let rustls_err = rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName);
        assert_eq!(
            classify_rustls_error(&rustls_err),
            GatewayProbeOutcome::IdentityMismatch
        );
    }

    #[test]
    fn classify_rustls_context_certificate_errors() {
        use rustls::{CertificateError, pki_types::UnixTime};

        let wrong_name = rustls::Error::InvalidCertificate(CertificateError::NotValidForNameContext {
            expected: ServerName::try_from("expected.grid.internal".to_owned()).unwrap(),
            presented: vec!["actual.grid.internal".to_owned()],
        });
        assert_eq!(
            classify_rustls_error(&wrong_name),
            GatewayProbeOutcome::IdentityMismatch
        );

        let expired = rustls::Error::InvalidCertificate(CertificateError::ExpiredContext {
            time: UnixTime::since_unix_epoch(Duration::from_secs(2)),
            not_after: UnixTime::since_unix_epoch(Duration::from_secs(1)),
        });
        assert_eq!(classify_rustls_error(&expired), GatewayProbeOutcome::CertificateExpired);

        let not_yet_valid = rustls::Error::InvalidCertificate(CertificateError::NotValidYetContext {
            time: UnixTime::since_unix_epoch(Duration::from_secs(1)),
            not_before: UnixTime::since_unix_epoch(Duration::from_secs(2)),
        });
        assert_eq!(
            classify_rustls_error(&not_yet_valid),
            GatewayProbeOutcome::CertificateNotYetValid
        );
    }

    #[test]
    fn classify_rustls_expired() {
        let rustls_err = rustls::Error::InvalidCertificate(rustls::CertificateError::Expired);
        assert_eq!(
            classify_rustls_error(&rustls_err),
            GatewayProbeOutcome::CertificateExpired
        );
    }

    #[test]
    fn classify_rustls_not_yet_valid() {
        let rustls_err = rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidYet);
        assert_eq!(
            classify_rustls_error(&rustls_err),
            GatewayProbeOutcome::CertificateNotYetValid
        );
    }

    #[test]
    fn no_private_material_in_probe_config_debug() {
        let ca = test_ca();
        let site = test_site(&ca, "test-site");
        let roots = parse_ca_roots(ca.cert_pem.as_bytes()).unwrap();
        let certs = parse_client_certs(site.cert_pem.as_bytes()).unwrap();
        let key = parse_private_key(site.key_pem.as_bytes()).unwrap();
        let tls_config = build_tls_config(roots, Some(certs), Some(key)).unwrap();

        let der = first_cert_der_from_pem(&site.cert_pem).unwrap();
        let fp = CanonicalFingerprint::from_der(&der);

        let config = ProbeConfig {
            address: "10.0.0.1:8443".to_owned(),
            tls_config,
            server_name: ServerName::try_from("test-site.grid.internal").unwrap(),
            pins: vec![fp],
            advertised_leaf_der: None,
        };

        assert!(!config.address.contains("PRIVATE KEY"), "address must not leak keys");
        assert_eq!(config.pins.len(), 1, "pins configured");
    }

    // -----------------------------------------------------------------------
    // Focused TLS handshake tests — real listeners, real certificates
    // -----------------------------------------------------------------------

    fn client_tls_config(ca: &certs::CaCert, client: &certs::SiteCertOutput) -> Arc<rustls::ClientConfig> {
        let roots = parse_ca_roots(ca.cert_pem.as_bytes()).unwrap();
        let client_certs = parse_client_certs(client.cert_pem.as_bytes()).unwrap();
        let client_key = parse_private_key(client.key_pem.as_bytes()).unwrap();
        build_tls_config(roots, Some(client_certs), Some(client_key)).unwrap()
    }

    fn make_probe_config(
        addr: std::net::SocketAddr,
        ca: &certs::CaCert,
        client: &certs::SiteCertOutput,
        server_name: &str,
        pins: Vec<CanonicalFingerprint>,
    ) -> ProbeConfig {
        ProbeConfig {
            address: addr.to_string(),
            tls_config: client_tls_config(ca, client),
            server_name: ServerName::try_from(server_name.to_owned()).unwrap(),
            pins,
            advertised_leaf_der: None,
        }
    }

    fn server_tls_config(server_cert: &certs::SiteCertOutput, ca: &certs::CaCert) -> Arc<rustls::ServerConfig> {
        let certs = parse_client_certs(server_cert.cert_pem.as_bytes()).unwrap();
        let key = parse_private_key(server_cert.key_pem.as_bytes()).unwrap();
        let provider = rustls::crypto::ring::default_provider();
        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(ca.cert_pem.as_bytes()) {
            roots.add(cert.unwrap()).unwrap();
        }
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_client_cert_verifier(
                rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                    .build()
                    .unwrap(),
            )
            .with_single_cert(certs, key)
            .unwrap();
        Arc::new(config)
    }

    fn start_tls_server(server_cert: &certs::SiteCertOutput, ca: &certs::CaCert) -> std::net::SocketAddr {
        let server_config = server_tls_config(server_cert, ca);
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = std_listener.local_addr().unwrap();
        std_listener.set_nonblocking(true).unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let acc = acceptor.clone();
                tokio::spawn(async move {
                    drop(acc.accept(stream).await);
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn verified_handshake_with_matching_pin() {
        let ca = test_ca();
        let server = test_site(&ca, "test-site");
        let client = test_site(&ca, "client-site");
        let addr = start_tls_server(&server, &ca);

        let server_der = first_cert_der_from_pem(&server.cert_pem).unwrap();
        let pin = CanonicalFingerprint::from_der(&server_der);

        let config = make_probe_config(addr, &ca, &client, "test-site.grid.internal", vec![pin]);
        let outcome = probe_gateway(&config).await;
        assert_eq!(outcome, GatewayProbeOutcome::Verified, "valid TLS+pin must verify");
    }

    #[tokio::test]
    async fn connection_refused_yields_connection_failed() {
        let ca = test_ca();
        let client = test_site(&ca, "client-site");
        let server = test_site(&ca, "test-site");
        let server_der = first_cert_der_from_pem(&server.cert_pem).unwrap();
        let pin = CanonicalFingerprint::from_der(&server_der);

        let config = ProbeConfig {
            address: "127.0.0.1:1".to_owned(),
            tls_config: client_tls_config(&ca, &client),
            server_name: ServerName::try_from("test-site.grid.internal").unwrap(),
            pins: vec![pin],
            advertised_leaf_der: None,
        };
        let outcome = probe_gateway(&config).await;
        assert_eq!(outcome, GatewayProbeOutcome::ConnectionFailed, "refused port must fail");
    }

    #[tokio::test]
    async fn wrong_ca_yields_untrusted_issuer() {
        let ca = test_ca();
        let wrong_ca = certs::generate_ca("wrong-ca").unwrap();
        let server = test_site(&ca, "test-site");
        let client = test_site(&wrong_ca, "client-site");
        let addr = start_tls_server(&server, &ca);

        let server_der = first_cert_der_from_pem(&server.cert_pem).unwrap();
        let pin = CanonicalFingerprint::from_der(&server_der);

        let config = make_probe_config(addr, &wrong_ca, &client, "test-site.grid.internal", vec![pin]);
        let outcome = probe_gateway(&config).await;
        assert_eq!(
            outcome,
            GatewayProbeOutcome::UntrustedIssuer,
            "server cert not trusted by wrong CA"
        );
    }

    #[tokio::test]
    async fn wrong_sni_yields_trust_failure() {
        let ca = test_ca();
        let server = test_site(&ca, "test-site");
        let client = test_site(&ca, "client-site");
        let addr = start_tls_server(&server, &ca);

        let server_der = first_cert_der_from_pem(&server.cert_pem).unwrap();
        let pin = CanonicalFingerprint::from_der(&server_der);

        let config = make_probe_config(addr, &ca, &client, "wrong-name.grid.internal", vec![pin]);
        let outcome = probe_gateway(&config).await;
        assert_eq!(
            outcome,
            GatewayProbeOutcome::IdentityMismatch,
            "wrong SNI must produce the distinct identity mismatch outcome"
        );
    }

    #[tokio::test]
    async fn wrong_pin_yields_pin_mismatch() {
        let ca = test_ca();
        let server = test_site(&ca, "test-site");
        let client = test_site(&ca, "client-site");
        let addr = start_tls_server(&server, &ca);

        let wrong_pin = CanonicalFingerprint::parse(&"f".repeat(64)).unwrap();

        let config = make_probe_config(addr, &ca, &client, "test-site.grid.internal", vec![wrong_pin]);
        let outcome = probe_gateway(&config).await;
        assert_eq!(
            outcome,
            GatewayProbeOutcome::PinMismatch,
            "wrong fingerprint pin must be rejected"
        );
    }

    #[tokio::test]
    async fn current_and_next_pins_both_work() {
        let ca = test_ca();
        let server = test_site(&ca, "test-site");
        let client = test_site(&ca, "client-site");
        let addr = start_tls_server(&server, &ca);

        let server_der = first_cert_der_from_pem(&server.cert_pem).unwrap();
        let current_pin = CanonicalFingerprint::from_der(&server_der);
        let next_pin = CanonicalFingerprint::parse(&"e".repeat(64)).unwrap();

        let config = make_probe_config(
            addr,
            &ca,
            &client,
            "test-site.grid.internal",
            vec![current_pin, next_pin],
        );
        let outcome = probe_gateway(&config).await;
        assert_eq!(
            outcome,
            GatewayProbeOutcome::Verified,
            "current pin in two-pin set must match"
        );
    }

    #[tokio::test]
    async fn advertised_cert_mismatch_detected() {
        let ca = test_ca();
        let server = test_site(&ca, "test-site");
        let client = test_site(&ca, "client-site");
        let addr = start_tls_server(&server, &ca);

        let server_der = first_cert_der_from_pem(&server.cert_pem).unwrap();
        let pin = CanonicalFingerprint::from_der(&server_der);

        let different_cert = test_site(&ca, "other-site");
        let different_der = first_cert_der_from_pem(&different_cert.cert_pem).unwrap();

        let config = ProbeConfig {
            advertised_leaf_der: Some(different_der),
            ..make_probe_config(addr, &ca, &client, "test-site.grid.internal", vec![pin])
        };
        let outcome = probe_gateway(&config).await;
        assert_eq!(
            outcome,
            GatewayProbeOutcome::AdvertisedCertificateMismatch,
            "advertised cert outside the configured pin set must be detected"
        );
    }

    #[tokio::test]
    async fn advertised_cert_match_succeeds() {
        let ca = test_ca();
        let server = test_site(&ca, "test-site");
        let client = test_site(&ca, "client-site");
        let addr = start_tls_server(&server, &ca);

        let server_der = first_cert_der_from_pem(&server.cert_pem).unwrap();
        let pin = CanonicalFingerprint::from_der(&server_der);

        let config = ProbeConfig {
            advertised_leaf_der: Some(server_der),
            ..make_probe_config(addr, &ca, &client, "test-site.grid.internal", vec![pin])
        };
        let outcome = probe_gateway(&config).await;
        assert_eq!(
            outcome,
            GatewayProbeOutcome::Verified,
            "matching advertised cert must succeed"
        );
    }

    #[tokio::test]
    async fn advertised_rotation_overlap_succeeds_when_both_pins_are_authorized() {
        let ca = test_ca();
        let live_server = test_site(&ca, "test-site");
        let next_server = test_site(&ca, "test-site");
        let client = test_site(&ca, "client-site");
        let addr = start_tls_server(&live_server, &ca);

        let live_der = first_cert_der_from_pem(&live_server.cert_pem).unwrap();
        let next_der = first_cert_der_from_pem(&next_server.cert_pem).unwrap();
        let live_pin = CanonicalFingerprint::from_der(&live_der);
        let next_pin = CanonicalFingerprint::from_der(&next_der);

        let config = ProbeConfig {
            advertised_leaf_der: Some(next_der),
            ..make_probe_config(addr, &ca, &client, "test-site.grid.internal", vec![live_pin, next_pin])
        };
        let outcome = probe_gateway(&config).await;
        assert_eq!(
            outcome,
            GatewayProbeOutcome::Verified,
            "old and new certificates may overlap when both canonical pins are authorized"
        );
    }

    #[tokio::test]
    async fn empty_pins_yields_pin_mismatch() {
        let ca = test_ca();
        let server = test_site(&ca, "test-site");
        let client = test_site(&ca, "client-site");
        let addr = start_tls_server(&server, &ca);

        let config = make_probe_config(addr, &ca, &client, "test-site.grid.internal", vec![]);
        let outcome = probe_gateway(&config).await;
        assert_eq!(
            outcome,
            GatewayProbeOutcome::PinMismatch,
            "empty pin set must fail closed"
        );
    }

    #[tokio::test]
    async fn probe_output_contains_no_private_material() {
        let ca = test_ca();
        let server = test_site(&ca, "test-site");
        let client = test_site(&ca, "client-site");
        let addr = start_tls_server(&server, &ca);

        let server_der = first_cert_der_from_pem(&server.cert_pem).unwrap();
        let pin = CanonicalFingerprint::from_der(&server_der);

        let config = make_probe_config(addr, &ca, &client, "test-site.grid.internal", vec![pin]);
        let outcome = probe_gateway(&config).await;
        let outcome_str = format!("{outcome:?}");
        assert!(
            !outcome_str.contains("PRIVATE KEY"),
            "outcome debug must not leak keys: {outcome_str}"
        );
        assert!(
            !outcome_str.contains("BEGIN CERTIFICATE"),
            "outcome debug must not leak certs: {outcome_str}"
        );
    }
}
