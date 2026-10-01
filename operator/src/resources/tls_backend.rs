//! TLS backend abstraction for the operator.
//!
//! Centralizes every TLS primitive the operator uses (client-config
//! assembly, HTTPS connectors, the probe handshake, PEM validation gates,
//! SNI parsing, and the process-wide crypto provider) behind one module so
//! the underlying TLS stack can be swapped without touching call sites.
//!
//! The backend is selected by cargo feature: the default `tls-rustls` build
//! uses rustls; the `fips` build routes all TLS through the system OpenSSL.
//! This is the only non-test module that names either TLS stack; every other
//! module refers to the aliases and functions exposed here.

#[cfg(feature = "fips")]
use std::sync::OnceLock;
use std::{borrow::Cow, sync::Arc};

use hyper_util::client::legacy::connect::HttpConnector;
#[cfg(feature = "fips")]
use openssl::{
    pkey::{PKey, Private},
    ssl::{SslAcceptor, SslAcceptorBuilder, SslConnector, SslConnectorBuilder, SslMethod, SslVerifyMode, SslVersion},
    x509::{X509, X509StoreContextRef, X509VerifyResult, store::X509StoreBuilder},
};
#[cfg(not(feature = "fips"))]
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName as RustlsServerName, UnixTime, pem::PemObject as _};

use crate::{metrics_scraper::MetricsScrapeError, resources::gateway_probe::GatewayProbeOutcome};

/// SHA-256 of `data`. The fips build routes this through the OpenSSL EVP digest
/// so identity hashes run in the validated provider, not the pure-Rust sha2 crate.
#[cfg(not(feature = "fips"))]
pub(crate) fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::{Digest as _, Sha256};
    Sha256::digest(data).into()
}

/// EVP one-shot through the system provider. Not `openssl::sha`, a legacy non-EVP
/// path that bypasses the FIPS provider.
#[cfg(feature = "fips")]
#[expect(
    clippy::expect_used,
    reason = "a sha256 failure means the FIPS provider is broken; crashing is the fail-closed response"
)]
pub(crate) fn sha256(data: &[u8]) -> [u8; 32] {
    let digest = openssl::hash::hash(openssl::hash::MessageDigest::sha256(), data).expect("openssl sha256 digest");
    let mut out = [0_u8; 32];
    out.copy_from_slice(&digest);
    out
}

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
#[cfg(not(feature = "fips"))]
pub(crate) type ClientTlsConfig = Arc<rustls::ClientConfig>;
/// Client TLS configuration threaded through probe and scrape call sites.
#[cfg(feature = "fips")]
pub(crate) type ClientTlsConfig = Arc<OpensslClientConfig>;

/// Validated SNI server name for a gateway probe.
#[cfg(not(feature = "fips"))]
pub(crate) type ServerName = RustlsServerName<'static>;
/// Validated SNI server name for a gateway probe.
#[cfg(feature = "fips")]
pub(crate) type ServerName = String;

/// HTTPS connector used by the hyper-based metrics scrape and health probe.
#[cfg(not(feature = "fips"))]
pub(crate) type HttpsConnector = hyper_rustls::HttpsConnector<HttpConnector>;
/// HTTPS connector used by the hyper-based metrics scrape and health probe.
#[cfg(feature = "fips")]
pub(crate) type HttpsConnector = hyper_openssl::client::legacy::HttpsConnector<HttpConnector>;

/// Established client TLS stream produced by [`connect`].
#[cfg(not(feature = "fips"))]
pub(crate) type ClientTlsStream = tokio_rustls::client::TlsStream<tokio::net::TcpStream>;
/// Established client TLS stream produced by [`connect`].
#[cfg(feature = "fips")]
pub(crate) type ClientTlsStream = tokio_openssl::SslStream<tokio::net::TcpStream>;

/// Server TLS configuration threaded through the signals listener.
#[cfg(not(feature = "fips"))]
pub type ServerTlsConfig = Arc<rustls::ServerConfig>;
/// Server TLS configuration threaded through the signals listener.
#[cfg(feature = "fips")]
pub type ServerTlsConfig = Arc<OpensslServerConfig>;

/// Established server TLS stream produced by [`accept`].
#[cfg(not(feature = "fips"))]
pub type ServerTlsStream = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
/// Established server TLS stream produced by [`accept`].
#[cfg(feature = "fips")]
pub type ServerTlsStream = tokio_openssl::SslStream<tokio::net::TcpStream>;

/// Parsed OpenSSL client material: grid trust roots and an optional mTLS identity.
#[cfg(feature = "fips")]
pub(crate) struct OpensslClientConfig {
    /// Grid CA certificates; the only trusted roots (no system roots).
    ca_roots: Vec<X509>,
    /// Optional mTLS client identity: certificate chain and private key.
    identity: Option<(Vec<X509>, PKey<Private>)>,
    /// Peer identity rule beyond the chain, `None` for ordinary name trust.
    rule: Option<LeafRule>,
    /// The connector built from the fields above, shared by every connection.
    /// Built lazily so a config derived with different pins caches its own.
    connector: OnceLock<SslConnector>,
}

#[cfg(feature = "fips")]
impl std::fmt::Debug for OpensslClientConfig {
    /// Redacts all key material: reports only counts and whether a client
    /// identity is present.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpensslClientConfig")
            .field("ca_roots", &self.ca_roots.len())
            .field("has_identity", &self.identity.is_some())
            .field("rule", &self.rule)
            .finish()
    }
}

#[cfg(feature = "fips")]
impl OpensslClientConfig {
    /// Build an SSL connector configured with grid-only trust roots, peer
    /// verification, and the optional client identity. No system roots are
    /// loaded, so only the provided grid CAs are trusted.
    fn connector_builder(&self) -> Result<SslConnectorBuilder, openssl::error::ErrorStack> {
        let mut builder = SslConnector::builder(SslMethod::tls_client())?;
        let mut store = X509StoreBuilder::new()?;
        for ca in &self.ca_roots {
            store.add_cert(ca.clone())?;
        }
        builder.set_cert_store(store.build());
        match self.rule.clone() {
            None => builder.set_verify(SslVerifyMode::PEER),
            Some(rule) => builder.set_verify_callback(SslVerifyMode::PEER, move |ok, ctx| verify_leaf(ok, ctx, &rule)),
        }
        builder.set_min_proto_version(Some(SslVersion::TLS1_2))?;
        if let Some((certs, key)) = &self.identity {
            if let Some((leaf, chain)) = certs.split_first() {
                builder.set_certificate(leaf)?;
                for extra in chain {
                    builder.add_extra_chain_cert(extra.clone())?;
                }
            }
            builder.set_private_key(key)?;
            builder.check_private_key()?;
        }
        Ok(builder)
    }

    /// The shared connector, built once from the parsed material and reused by
    /// every connection. `connector_builder` parses the CA store and installs
    /// the verify callback, so that runs once here rather than per connect.
    ///
    /// # Errors
    ///
    /// Returns the OpenSSL error stack if building the connector fails.
    fn connector(&self) -> Result<&SslConnector, openssl::error::ErrorStack> {
        if let Some(connector) = self.connector.get() {
            return Ok(connector);
        }
        let connector = self.connector_builder()?.build();
        // A lost race drops our connector; both callers get the stored winner.
        Ok(self.connector.get_or_init(|| connector))
    }

    /// Whether a leaf rule, not the name, identifies the peer.
    fn has_leaf_rule(&self) -> bool {
        self.rule.is_some()
    }
}

/// OpenSSL handshake verification failure carrying the `X509_V_ERR_*` code, so
/// the backend-neutral [`classify_tls_error`] can map it after the handshake.
#[cfg(feature = "fips")]
#[derive(Debug)]
struct VerifyFailure(i32);

#[cfg(feature = "fips")]
impl std::fmt::Display for VerifyFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "certificate verification failed (code {})", self.0)
    }
}

#[cfg(feature = "fips")]
impl std::error::Error for VerifyFailure {}

/// Install the process-wide crypto provider the TLS stack requires.
///
/// Installs `ring` once, up front, so both the `hyper-rustls` client
/// (`with_native_roots`) and the `reqwest`-backed MCP probe (built with
/// `rustls-no-provider`) have a default provider regardless of which
/// reconciler runs first. A second install attempt is ignored.
#[cfg(not(feature = "fips"))]
pub fn init_process_crypto() {
    if rustls::crypto::ring::default_provider().install_default().is_err() {
        tracing::warn!("rustls default CryptoProvider already installed; continuing");
    }
}

/// Install the process-wide crypto provider the TLS stack requires.
///
/// OpenSSL needs no process-wide provider, so this is a no-op under `fips`.
#[cfg(feature = "fips")]
pub fn init_process_crypto() {}

/// Parse PEM-encoded CA certificates into a trust root store.
///
/// # Errors
///
/// Returns a description if no valid certificate could be parsed.
#[cfg(not(feature = "fips"))]
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

/// Parse PEM-encoded CA certificates into grid trust roots.
///
/// # Errors
///
/// Returns a description if no valid certificate could be parsed.
#[cfg(feature = "fips")]
pub(crate) fn parse_ca_roots(ca_pem: &[u8]) -> Result<Vec<X509>, &'static str> {
    if ca_pem.len() > MAX_CERT_BUNDLE_BYTES {
        return Err("CA certificate bundle exceeds maximum size");
    }
    let certs = X509::stack_from_pem(ca_pem).map_err(|_err| "CA PEM contains invalid certificate data")?;
    if certs.is_empty() {
        return Err("CA PEM contains no certificates");
    }
    if certs.len() > MAX_CA_CERTIFICATES {
        return Err("CA certificate bundle exceeds maximum certificate count");
    }
    for cert in &certs {
        let der = cert
            .to_der()
            .map_err(|_err| "CA certificate failed trust store insertion")?;
        if der.len() > MAX_CERT_BYTES {
            return Err("CA certificate exceeds maximum size");
        }
    }
    Ok(certs)
}

/// Parse PEM-encoded client certificate chain.
///
/// # Errors
///
/// Returns a description if parsing fails or the chain is oversized.
#[cfg(not(feature = "fips"))]
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

/// Parse PEM-encoded client certificate chain.
///
/// # Errors
///
/// Returns a description if parsing fails or the chain is oversized.
#[cfg(feature = "fips")]
pub(crate) fn parse_client_certs(cert_pem: &[u8]) -> Result<Vec<X509>, &'static str> {
    if cert_pem.len() > MAX_CERT_BUNDLE_BYTES {
        return Err("client certificate bundle exceeds maximum size");
    }
    let certs = X509::stack_from_pem(cert_pem).map_err(|_err| "client certificate PEM is malformed")?;
    if certs.is_empty() {
        return Err("client certificate PEM contains no certificates");
    }
    if certs.len() > MAX_CHAIN_DEPTH {
        return Err("client certificate chain exceeds maximum depth");
    }
    for cert in &certs {
        let der = cert.to_der().map_err(|_err| "client certificate PEM is malformed")?;
        if der.len() > MAX_CERT_BYTES {
            return Err("client certificate exceeds maximum size");
        }
    }
    Ok(certs)
}

/// Parse a PEM-encoded private key (PKCS#8 or PKCS#1 or SEC1).
///
/// # Errors
///
/// Returns a description if parsing fails.
#[cfg(not(feature = "fips"))]
pub(crate) fn parse_private_key(key_pem: &[u8]) -> Result<PrivateKeyDer<'static>, &'static str> {
    if key_pem.len() > MAX_PRIVATE_KEY_BYTES {
        return Err("private key PEM exceeds maximum size");
    }
    PrivateKeyDer::from_pem_slice(key_pem).map_err(|_err| "private key PEM is malformed or missing")
}

/// Parse a PEM-encoded private key (PKCS#8 or PKCS#1 or SEC1).
///
/// # Errors
///
/// Returns a description if parsing fails.
#[cfg(feature = "fips")]
pub(crate) fn parse_private_key(key_pem: &[u8]) -> Result<PKey<Private>, &'static str> {
    if key_pem.len() > MAX_PRIVATE_KEY_BYTES {
        return Err("private key PEM exceeds maximum size");
    }
    PKey::private_key_from_pem(key_pem).map_err(|_err| "private key PEM is malformed or missing")
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
#[cfg(not(feature = "fips"))]
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

/// Build a client TLS configuration from parsed trust material.
///
/// Uses only the provided Grid CA roots (no system/native roots). When
/// `client_certs` and `client_key` are provided, configures mTLS client
/// authentication.
///
/// # Errors
///
/// Returns a description if the client certificate and key are not both
/// present or both absent, or if the identity is structurally invalid.
#[cfg(feature = "fips")]
pub(crate) fn build_tls_config(
    roots: Vec<X509>,
    client_certs: Option<Vec<X509>>,
    client_key: Option<PKey<Private>>,
) -> Result<ClientTlsConfig, &'static str> {
    let identity = match (client_certs, client_key) {
        (Some(certs), Some(key)) => Some((certs, key)),
        (None, None) => None,
        _ => return Err("client certificate and key must both be present or both absent"),
    };
    let config = OpensslClientConfig {
        ca_roots: roots,
        identity,
        rule: None,
        connector: OnceLock::new(),
    };
    config
        .connector_builder()
        .map_err(|_err| "client certificate and key are incompatible")?;
    Ok(Arc::new(config))
}

/// Extract the first DER certificate from a PEM string.
///
/// # Errors
///
/// Returns a bounded description if the PEM is oversized, malformed, or
/// contains no certificate.
#[cfg(not(feature = "fips"))]
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

/// Extract the first DER certificate from a PEM string.
///
/// # Errors
///
/// Returns a bounded description if the PEM is oversized, malformed, or
/// contains no certificate.
#[cfg(feature = "fips")]
pub(crate) fn first_cert_der_from_pem(pem: &str) -> Result<Vec<u8>, &'static str> {
    if pem.len() > MAX_CERT_BUNDLE_BYTES {
        return Err("advertised certificate PEM exceeds maximum size");
    }
    let certs = X509::stack_from_pem(pem.as_bytes()).map_err(|_err| "advertised certificate PEM is malformed")?;
    let cert = certs
        .first()
        .ok_or("advertised certificate PEM contains no certificate")?;
    let der = cert
        .to_der()
        .map_err(|_err| "advertised certificate PEM is malformed")?;
    if der.len() > MAX_CERT_BYTES {
        return Err("advertised certificate exceeds maximum size");
    }
    Ok(der)
}

/// Parse and validate an SNI server name.
///
/// # Errors
///
/// Returns `Err(())` if the name is not a valid DNS name.
#[cfg(not(feature = "fips"))]
pub(crate) fn parse_server_name(server_name: &str) -> Result<ServerName, ()> {
    RustlsServerName::try_from(server_name.to_owned()).map_err(|_err| ())
}

/// Parse and validate an SNI server name.
///
/// The caller validates the name structurally before this point; this rejects
/// empty or control-character input and returns the owned name for SNI.
///
/// # Errors
///
/// Returns `Err(())` if the name is empty or contains control/space bytes.
#[cfg(feature = "fips")]
pub(crate) fn parse_server_name(server_name: &str) -> Result<ServerName, ()> {
    if server_name.is_empty() || server_name.bytes().any(|b| b <= b' ') {
        return Err(());
    }
    Ok(server_name.to_owned())
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
#[cfg(not(feature = "fips"))]
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
#[cfg(feature = "fips")]
#[expect(
    clippy::too_many_lines,
    reason = "sequential PEM parsing for CA, client cert, and client key with validation"
)]
pub(crate) fn build_tls_client_config(
    ca_pem: &[u8],
    client_cert_pem: Option<&[u8]>,
    client_key_pem: Option<&[u8]>,
) -> Result<OpensslClientConfig, MetricsScrapeError> {
    if ca_pem.len() > MAX_CA_PEM_BYTES {
        return Err(MetricsScrapeError::TlsMaterial(format!(
            "CA PEM exceeds maximum size ({} bytes > {MAX_CA_PEM_BYTES})",
            ca_pem.len()
        )));
    }

    let ca_roots = X509::stack_from_pem(ca_pem)
        .map_err(|e| MetricsScrapeError::TlsMaterial(format!("CA PEM parse failed: {e}")))?;
    if ca_roots.is_empty() {
        return Err(MetricsScrapeError::TlsMaterial(
            "CA PEM contains no certificates".to_owned(),
        ));
    }
    if ca_roots.len() > MAX_CERT_CHAIN_LENGTH {
        return Err(MetricsScrapeError::TlsMaterial(format!(
            "CA PEM contains too many certificates ({} > {MAX_CERT_CHAIN_LENGTH})",
            ca_roots.len()
        )));
    }

    let identity = match (client_cert_pem, client_key_pem) {
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
            let certs = X509::stack_from_pem(cert_pem)
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
            let key = PKey::private_key_from_pem(key_pem)
                .map_err(|e| MetricsScrapeError::TlsMaterial(format!("client key PEM parse failed: {e}")))?;
            Some((certs, key))
        },
        (None, None) => None,
        _ => {
            return Err(MetricsScrapeError::TlsMaterial(
                "client cert and key must both be present or both absent".to_owned(),
            ));
        },
    };

    let config = OpensslClientConfig {
        ca_roots,
        identity,
        rule: None,
        connector: OnceLock::new(),
    };
    config
        .connector_builder()
        .map_err(|e| MetricsScrapeError::TlsMaterial(format!("client identity construction failed: {e}")))?;
    Ok(config)
}

/// Build an HTTPS connector using native root certificates.
///
/// # Errors
///
/// Returns [`MetricsScrapeError::Transport`] if native roots cannot be loaded.
#[cfg(not(feature = "fips"))]
pub(crate) fn build_native_connector() -> Result<HttpsConnector, MetricsScrapeError> {
    hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .map(|b| b.https_or_http().enable_http1().build())
        .map_err(|e| MetricsScrapeError::Transport(e.into()))
}

/// Build an HTTPS connector using native root certificates.
///
/// # Errors
///
/// Returns [`MetricsScrapeError::Transport`] if the connector cannot be built.
#[cfg(feature = "fips")]
pub(crate) fn build_native_connector() -> Result<HttpsConnector, MetricsScrapeError> {
    hyper_openssl::client::legacy::HttpsConnector::new().map_err(|e| MetricsScrapeError::Transport(e.into()))
}

/// Build an HTTPS-only connector using a custom client TLS configuration.
///
/// # Errors
///
/// Returns [`MetricsScrapeError::Transport`] if the connector cannot be built.
#[cfg(not(feature = "fips"))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "the fips (openssl) backend's connector build is fallible"
)]
pub(crate) fn build_custom_tls_connector(config: &ClientTlsConfig) -> Result<HttpsConnector, MetricsScrapeError> {
    Ok(hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config((**config).clone())
        .https_only()
        .enable_http1()
        .build())
}

/// Build an HTTPS-only connector using a custom client TLS configuration.
///
/// # Errors
///
/// Returns [`MetricsScrapeError::Transport`] if the connector cannot be built.
#[cfg(feature = "fips")]
pub(crate) fn build_custom_tls_connector(config: &ClientTlsConfig) -> Result<HttpsConnector, MetricsScrapeError> {
    let builder = config
        .connector_builder()
        .map_err(|e| MetricsScrapeError::Transport(e.into()))?;
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    let mut connector = hyper_openssl::client::legacy::HttpsConnector::with_connector(http, builder)
        .map_err(|e| MetricsScrapeError::Transport(e.into()))?;
    if config.has_leaf_rule() {
        // The leaf rule, not the certificate name, identifies the peer.
        connector.set_callback(|cfg, _uri| {
            cfg.set_verify_hostname(false);
            Ok(())
        });
    }
    Ok(connector)
}

/// What a peer's leaf must satisfy once it chains to the grid roots.
#[derive(Clone, Debug)]
pub(crate) enum LeafRule {
    /// Leaf digest is one this site declared.
    Pins(Vec<[u8; 32]>),
    /// Leaf carries exactly this SPIFFE ID.
    Spiffe(String),
}

impl LeafRule {
    /// Whether the DER leaf satisfies the rule.
    fn accepts(&self, leaf_der: &[u8]) -> bool {
        match self {
            Self::Pins(pins) => pins.contains(&sha256(leaf_der)),
            Self::Spiffe(id) => certs::leaf_spiffe_id(leaf_der).as_deref() == Some(id.as_str()),
        }
    }

    /// The pin rule from declared fingerprints, refusing the peer when empty.
    fn from_pins(pins: &[String]) -> Result<Self, MetricsScrapeError> {
        if pins.is_empty() {
            return Err(MetricsScrapeError::TlsMaterial(
                "no declared fingerprints for this peer".to_owned(),
            ));
        }
        Ok(Self::Pins(
            pins.iter().map(|pin| decode_pin(pin)).collect::<Result<_, _>>()?,
        ))
    }
}

/// Client config for a peer that must present `spiffe_id`, chained to the grid roots.
///
/// # Errors
///
/// Returns [`MetricsScrapeError::TlsMaterial`] when the material cannot be parsed.
pub(crate) fn build_spiffe_client_config(
    ca_pem: &[u8],
    client_cert_pem: Option<&[u8]>,
    client_key_pem: Option<&[u8]>,
    spiffe_id: &str,
) -> Result<ClientTlsConfig, MetricsScrapeError> {
    build_rule_client_config(
        ca_pem,
        client_cert_pem,
        client_key_pem,
        LeafRule::Spiffe(spiffe_id.to_owned()),
    )
}

/// Build a client config that accepts only the leaf keys declared for one peer.
///
/// # Errors
///
/// Returns [`MetricsScrapeError::TlsMaterial`] when the material cannot be
/// parsed, or when `pins` is empty (an undeclared peer is refused).
pub(crate) fn build_pinned_client_config(
    ca_pem: &[u8],
    client_cert_pem: Option<&[u8]>,
    client_key_pem: Option<&[u8]>,
    pins: &[String],
) -> Result<ClientTlsConfig, MetricsScrapeError> {
    build_rule_client_config(ca_pem, client_cert_pem, client_key_pem, LeafRule::from_pins(pins)?)
}

/// Decode a declared fingerprint into the 32 digest bytes it names.
///
/// Anything not 32 bytes of hex is an error, so a bad pin fails as a bad pin
/// rather than silently refusing the peer.
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

/// Verify the chain against the grid roots and the leaf against `rule`, not the hostname.
#[cfg(not(feature = "fips"))]
fn build_rule_client_config(
    ca_pem: &[u8],
    client_cert_pem: Option<&[u8]>,
    client_key_pem: Option<&[u8]>,
    rule: LeafRule,
) -> Result<ClientTlsConfig, MetricsScrapeError> {
    let mut config = build_tls_client_config(ca_pem, client_cert_pem, client_key_pem)?;
    let verifier = leaf_rule_verifier(ca_pem, rule, config.crypto_provider().signature_verification_algorithms)?;
    config.dangerous().set_certificate_verifier(Arc::new(verifier));
    Ok(Arc::new(config))
}

/// The verifier [`build_rule_client_config`] installs.
#[cfg(not(feature = "fips"))]
fn leaf_rule_verifier(
    ca_pem: &[u8],
    rule: LeafRule,
    algorithms: rustls::crypto::WebPkiSupportedAlgorithms,
) -> Result<LeafRuleVerifier, MetricsScrapeError> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(ca_pem) {
        let cert = cert.map_err(|e| MetricsScrapeError::TlsMaterial(format!("CA PEM parse failed: {e}")))?;
        roots
            .add(cert)
            .map_err(|e| MetricsScrapeError::TlsMaterial(format!("CA rejected: {e}")))?;
    }
    Ok(LeafRuleVerifier {
        roots: Arc::new(roots),
        algorithms,
        rule,
    })
}

/// Accepts a peer whose chain verifies and whose leaf satisfies its [`LeafRule`], never checking the hostname.
#[cfg(not(feature = "fips"))]
#[derive(Debug)]
pub(crate) struct LeafRuleVerifier {
    /// Authorities the chain is verified against.
    roots: Arc<rustls::RootCertStore>,
    /// Algorithms the chain and its signatures may use.
    algorithms: rustls::crypto::WebPkiSupportedAlgorithms,
    /// What the leaf must satisfy.
    rule: LeafRule,
}

#[cfg(not(feature = "fips"))]
impl rustls::client::danger::ServerCertVerifier for LeafRuleVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &RustlsServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let _ = (server_name, ocsp_response);
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &rustls::server::ParsedCertificate::try_from(end_entity)?,
            &self.roots,
            intermediates,
            now,
            self.algorithms.all,
        )?;
        if self.rule.accepts(end_entity) {
            return Ok(rustls::client::danger::ServerCertVerified::assertion());
        }
        Err(rustls::Error::General(
            "peer presented a certificate this site does not accept for it".to_owned(),
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

/// Verify the chain against the grid roots and the leaf against `rule`, not the hostname.
#[cfg(feature = "fips")]
fn build_rule_client_config(
    ca_pem: &[u8],
    client_cert_pem: Option<&[u8]>,
    client_key_pem: Option<&[u8]>,
    rule: LeafRule,
) -> Result<ClientTlsConfig, MetricsScrapeError> {
    let base = build_tls_client_config(ca_pem, client_cert_pem, client_key_pem)?;
    // A fresh cache: the rule changes the verify callback.
    Ok(Arc::new(OpensslClientConfig {
        rule: Some(rule),
        connector: OnceLock::new(),
        ..base
    }))
}

/// Verify callback for a peer: `preverify_ok` carries the chain result with no hostname set, then the leaf rule.
#[cfg(feature = "fips")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "the &mut is dictated by openssl's set_verify_callback signature"
)]
fn verify_leaf(preverify_ok: bool, ctx: &mut X509StoreContextRef, rule: &LeafRule) -> bool {
    if !preverify_ok {
        return false;
    }
    if ctx.error_depth() != 0 {
        return true;
    }
    let Some(cert) = ctx.current_cert() else {
        return false;
    };
    let Ok(der) = cert.to_der() else {
        return false;
    };
    rule.accepts(&der)
}

/// Perform the client TLS handshake over an established TCP stream.
///
/// # Errors
///
/// Returns the underlying I/O error if the handshake fails.
#[cfg(not(feature = "fips"))]
pub(crate) async fn connect(
    tcp_stream: tokio::net::TcpStream,
    config: &ClientTlsConfig,
    server_name: &ServerName,
) -> Result<ClientTlsStream, std::io::Error> {
    let connector = tokio_rustls::TlsConnector::from(Arc::clone(config));
    connector.connect(server_name.clone(), tcp_stream).await
}

/// Perform the client TLS handshake over an established TCP stream.
///
/// `into_ssl` sets both the SNI and the X509 verification hostname, so a
/// SAN/hostname mismatch surfaces as a verification error. On failure the
/// `X509_V_ERR_*` verification code is carried through the returned I/O error
/// for [`classify_tls_error`].
///
/// # Errors
///
/// Returns an I/O error if the connector, SNI setup, or handshake fails.
#[cfg(feature = "fips")]
pub(crate) async fn connect(
    tcp_stream: tokio::net::TcpStream,
    config: &ClientTlsConfig,
    server_name: &ServerName,
) -> Result<ClientTlsStream, std::io::Error> {
    let to_io = |e: openssl::error::ErrorStack| std::io::Error::other(e);
    let connector = config.connector().map_err(to_io)?;
    let ssl = connector
        .configure()
        .and_then(|mut c| {
            // The leaf rule, not the name, identifies the peer on every call path.
            if config.has_leaf_rule() {
                c.set_verify_hostname(false);
            }
            c.into_ssl(server_name.as_str())
        })
        .map_err(to_io)?;
    let mut stream = tokio_openssl::SslStream::new(ssl, tcp_stream).map_err(to_io)?;
    match std::pin::Pin::new(&mut stream).connect().await {
        Ok(()) => Ok(stream),
        Err(err) => {
            let verify = stream.ssl().verify_result();
            if verify == X509VerifyResult::OK {
                Err(err.into_io_error().unwrap_or_else(std::io::Error::other))
            } else {
                Err(std::io::Error::other(VerifyFailure(verify.as_raw())))
            }
        },
    }
}

/// Borrow the peer certificate chain (DER) from an established stream.
///
/// Returns `None` if the peer presented no certificates. Under rustls the
/// slices borrow from `tls_stream` (no copy); under OpenSSL the DER is owned
/// (`X509::to_der` allocates), hence [`Cow`].
#[cfg(not(feature = "fips"))]
pub(crate) fn peer_chain_der(tls_stream: &ClientTlsStream) -> Option<Vec<Cow<'_, [u8]>>> {
    let (_, server_conn) = tls_stream.get_ref();
    Some(
        server_conn
            .peer_certificates()?
            .iter()
            .map(|cert| Cow::Borrowed(cert.as_ref()))
            .collect(),
    )
}

/// Borrow the peer certificate chain (DER) from an established stream.
///
/// Returns `None` if the peer presented no certificates. Under rustls the
/// slices borrow from `tls_stream` (no copy); under OpenSSL the DER is owned
/// (`X509::to_der` allocates), hence [`Cow`].
#[cfg(feature = "fips")]
pub(crate) fn peer_chain_der(tls_stream: &ClientTlsStream) -> Option<Vec<Cow<'_, [u8]>>> {
    let chain = tls_stream.ssl().peer_cert_chain()?;
    chain.iter().map(|cert| cert.to_der().ok().map(Cow::Owned)).collect()
}

/// Parsed OpenSSL server material: the site identity plus the grid roots that
/// verify a client certificate.
#[cfg(feature = "fips")]
pub struct OpensslServerConfig {
    /// Grid CA certificates a presented client certificate is verified against.
    ca_roots: Vec<X509>,
    /// The server leaf certificate.
    leaf: X509,
    /// Intermediate certificates sent after the leaf.
    chain: Vec<X509>,
    /// Private key for `leaf`.
    key: PKey<Private>,
    /// Whether a client certificate is required.
    require_client: bool,
    /// The acceptor built from the fields above, shared by every connection.
    acceptor: OnceLock<SslAcceptor>,
}

#[cfg(feature = "fips")]
impl std::fmt::Debug for OpensslServerConfig {
    /// Redacts key material: reports only certificate counts.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpensslServerConfig")
            .field("ca_roots", &self.ca_roots.len())
            .field("chain", &(self.chain.len() + 1))
            .finish()
    }
}

#[cfg(feature = "fips")]
impl OpensslServerConfig {
    /// Build an acceptor with the site identity, requiring a client certificate from the grid roots.
    fn acceptor_builder(&self) -> Result<SslAcceptorBuilder, openssl::error::ErrorStack> {
        let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls_server())?;
        builder.set_private_key(&self.key)?;
        builder.set_certificate(&self.leaf)?;
        for extra in &self.chain {
            builder.add_extra_chain_cert(extra.clone())?;
        }
        builder.check_private_key()?;
        let mut store = X509StoreBuilder::new()?;
        for ca in &self.ca_roots {
            store.add_cert(ca.clone())?;
        }
        builder.set_verify_cert_store(store.build())?;
        // Anonymous callers would only hold slots.
        let mode = if self.require_client {
            SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT
        } else {
            SslVerifyMode::PEER
        };
        builder.set_verify(mode);
        Ok(builder)
    }

    /// The shared acceptor, built once from the parsed material and reused by
    /// every connection, so each accept only calls `Ssl::new` on its context.
    ///
    /// # Errors
    ///
    /// Returns the OpenSSL error stack if building the acceptor fails.
    fn acceptor(&self) -> Result<&SslAcceptor, openssl::error::ErrorStack> {
        if let Some(acceptor) = self.acceptor.get() {
            return Ok(acceptor);
        }
        let acceptor = self.acceptor_builder()?.build();
        // A lost race drops our acceptor; both callers get the stored winner.
        Ok(self.acceptor.get_or_init(|| acceptor))
    }
}

/// Build the signals listener TLS config from PEM material.
///
/// A Grid-CA client certificate is required, and the scope rule reads whose it is.
///
/// # Errors
///
/// Returns a description when the CA, certificate, or key cannot be parsed.
pub fn build_server_config(ca_pem: &[u8], cert_pem: &[u8], key_pem: &[u8]) -> Result<ServerTlsConfig, String> {
    server_config(ca_pem, cert_pem, key_pem, true)
}

/// Like [`build_server_config`] with the client certificate optional, for test servers.
#[cfg(test)]
pub(crate) fn build_server_config_optional_client(
    ca_pem: &[u8],
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<ServerTlsConfig, String> {
    server_config(ca_pem, cert_pem, key_pem, false)
}

/// Server TLS from PEM material, a Grid-CA client certificate required when `require_client`.
#[cfg(not(feature = "fips"))]
fn server_config(
    ca_pem: &[u8],
    cert_pem: &[u8],
    key_pem: &[u8],
    require_client: bool,
) -> Result<ServerTlsConfig, String> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(ca_pem) {
        let cert = cert.map_err(|e| format!("signals TLS: CA is not valid PEM: {e}"))?;
        roots.add(cert).map_err(|e| format!("signals TLS: CA rejected: {e}"))?;
    }
    let chain = CertificateDer::pem_slice_iter(cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("signals TLS: certificate is not valid PEM: {e}"))?;
    let key = PrivateKeyDer::from_pem_slice(key_pem).map_err(|e| format!("signals TLS: key is not valid PEM: {e}"))?;
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots));
    let verifier = if require_client {
        verifier
    } else {
        verifier.allow_unauthenticated()
    }
    .build()
    .map_err(|e| format!("signals TLS: client verifier: {e}"))?;
    rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain, key)
        .map(Arc::new)
        .map_err(|e| format!("signals TLS: server config: {e}"))
}

/// Server TLS from PEM material, a Grid-CA client certificate required when `require_client`.
#[cfg(feature = "fips")]
fn server_config(
    ca_pem: &[u8],
    cert_pem: &[u8],
    key_pem: &[u8],
    require_client: bool,
) -> Result<ServerTlsConfig, String> {
    let ca_roots = X509::stack_from_pem(ca_pem).map_err(|e| format!("signals TLS: CA is not valid PEM: {e}"))?;
    if ca_roots.is_empty() {
        return Err("signals TLS: CA PEM contains no certificates".to_owned());
    }
    let mut certs = X509::stack_from_pem(cert_pem)
        .map_err(|e| format!("signals TLS: certificate is not valid PEM: {e}"))?
        .into_iter();
    let leaf = certs
        .next()
        .ok_or_else(|| "signals TLS: certificate PEM contains no certificates".to_owned())?;
    let chain = certs.collect();
    let key = PKey::private_key_from_pem(key_pem).map_err(|e| format!("signals TLS: key is not valid PEM: {e}"))?;
    let config = OpensslServerConfig {
        ca_roots,
        leaf,
        chain,
        key,
        require_client,
        acceptor: OnceLock::new(),
    };
    config
        .acceptor_builder()
        .map_err(|e| format!("signals TLS: server config: {e}"))?;
    Ok(Arc::new(config))
}

/// Accept and handshake one server connection over an established TCP stream.
///
/// # Errors
///
/// Returns an I/O error if the acceptor build or handshake fails.
#[cfg(not(feature = "fips"))]
pub async fn accept(
    tcp_stream: tokio::net::TcpStream,
    config: &ServerTlsConfig,
) -> Result<ServerTlsStream, std::io::Error> {
    tokio_rustls::TlsAcceptor::from(Arc::clone(config))
        .accept(tcp_stream)
        .await
}

/// Accept and handshake one server connection over an established TCP stream.
///
/// # Errors
///
/// Returns an I/O error if the acceptor build or handshake fails.
#[cfg(feature = "fips")]
pub async fn accept(
    tcp_stream: tokio::net::TcpStream,
    config: &ServerTlsConfig,
) -> Result<ServerTlsStream, std::io::Error> {
    let acceptor = config.acceptor().map_err(std::io::Error::other)?;
    let ssl = openssl::ssl::Ssl::new(acceptor.context()).map_err(std::io::Error::other)?;
    let mut stream = tokio_openssl::SslStream::new(ssl, tcp_stream).map_err(std::io::Error::other)?;
    std::pin::Pin::new(&mut stream)
        .accept()
        .await
        .map_err(|e| e.into_io_error().unwrap_or_else(std::io::Error::other))?;
    Ok(stream)
}

/// Borrow the peer's leaf certificate (DER) from an established server stream.
///
/// Returns `None` when the caller presented no certificate.
#[cfg(not(feature = "fips"))]
pub fn server_peer_leaf_der(stream: &ServerTlsStream) -> Option<Vec<u8>> {
    stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(<[_]>::first)
        .map(|cert| cert.as_ref().to_vec())
}

/// Borrow the peer's leaf certificate (DER) from an established server stream.
///
/// Returns `None` when the caller presented no certificate.
#[cfg(feature = "fips")]
pub fn server_peer_leaf_der(stream: &ServerTlsStream) -> Option<Vec<u8>> {
    stream.ssl().peer_certificate().and_then(|cert| cert.to_der().ok())
}

/// Whether an error somewhere in a transport error chain is a TLS failure.
#[cfg(not(feature = "fips"))]
pub(crate) fn is_tls_error(err: &(dyn std::error::Error + 'static)) -> bool {
    err.downcast_ref::<rustls::Error>().is_some()
}

/// Whether an error somewhere in a transport error chain is a TLS failure.
#[cfg(feature = "fips")]
pub(crate) fn is_tls_error(err: &(dyn std::error::Error + 'static)) -> bool {
    err.downcast_ref::<openssl::ssl::Error>().is_some() || err.downcast_ref::<openssl::error::ErrorStack>().is_some()
}

/// Classify a TLS/handshake I/O error into a `GatewayProbeOutcome`.
///
/// Never exposes the raw error string in the outcome.
#[cfg(not(feature = "fips"))]
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

/// Classify a TLS/handshake I/O error into a `GatewayProbeOutcome`.
///
/// Never exposes the raw error string in the outcome.
#[cfg(feature = "fips")]
pub(crate) fn classify_tls_error(err: &std::io::Error) -> GatewayProbeOutcome {
    if let Some(verify) = err.get_ref().and_then(|inner| inner.downcast_ref::<VerifyFailure>()) {
        return classify_openssl_verify(verify.0);
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
#[cfg(not(feature = "fips"))]
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

/// Map an OpenSSL `X509_V_ERR_*` verification code to a `GatewayProbeOutcome`.
#[cfg(feature = "fips")]
fn classify_openssl_verify(code: i32) -> GatewayProbeOutcome {
    match code {
        openssl_sys::X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY
        | openssl_sys::X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN
        | openssl_sys::X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT
        | openssl_sys::X509_V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE => GatewayProbeOutcome::UntrustedIssuer,
        openssl_sys::X509_V_ERR_HOSTNAME_MISMATCH | openssl_sys::X509_V_ERR_IP_ADDRESS_MISMATCH => {
            GatewayProbeOutcome::IdentityMismatch
        },
        openssl_sys::X509_V_ERR_CERT_HAS_EXPIRED => GatewayProbeOutcome::CertificateExpired,
        openssl_sys::X509_V_ERR_CERT_NOT_YET_VALID => GatewayProbeOutcome::CertificateNotYetValid,
        _ => GatewayProbeOutcome::TrustMaterialInvalid,
    }
}

/// Structurally validate that `pem` decodes to at least one well-formed
/// certificate.
///
/// # Errors
///
/// Returns a description if the PEM is malformed or contains no certificates.
#[cfg(not(feature = "fips"))]
pub(crate) fn validate_pem_certificates(pem: &[u8]) -> Result<(), String> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    if certs.is_empty() {
        return Err("PEM contains no certificates".to_owned());
    }
    Ok(())
}

/// Structurally validate that `pem` decodes to at least one well-formed
/// certificate.
///
/// # Errors
///
/// Returns a description if the PEM is malformed or contains no certificates.
#[cfg(feature = "fips")]
pub(crate) fn validate_pem_certificates(pem: &[u8]) -> Result<(), String> {
    let certs = X509::stack_from_pem(pem).map_err(|e| e.to_string())?;
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
#[cfg(not(feature = "fips"))]
pub(crate) fn validate_pem_private_key(pem: &[u8]) -> Result<(), String> {
    PrivateKeyDer::from_pem_slice(pem)
        .map(|_key| ())
        .map_err(|e| e.to_string())
}

/// Structurally validate that `pem` decodes to a well-formed private key.
///
/// # Errors
///
/// Returns a description if the key PEM is malformed.
#[cfg(feature = "fips")]
pub(crate) fn validate_pem_private_key(pem: &[u8]) -> Result<(), String> {
    PKey::private_key_from_pem(pem)
        .map(|_key| ())
        .map_err(|e| e.to_string())
}
