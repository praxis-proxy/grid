//! The serving TLS config, with an optional grid client certificate for renewal.
//!
//! A client may present no certificate, as enroll does. One it presents must chain to
//! the grid CA, and the verified leaf reaches handlers as [`PeerLeaf`].

use std::{io, path::Path, pin::Pin, sync::Arc};

use axum::{Extension, middleware::AddExtension};
use axum_server::accept::Accept;
use tokio::io::{AsyncRead, AsyncWrite};
use tower::Layer as _;

use crate::api::PeerLeaf;

/// Server TLS config: rustls by default, system openssl under `fips`.
#[cfg(not(feature = "fips"))]
pub type TlsConfig = axum_server::tls_rustls::RustlsConfig;
/// Server TLS config: rustls by default, system openssl under `fips`.
#[cfg(feature = "fips")]
pub type TlsConfig = axum_server::tls_openssl::OpenSSLConfig;

/// The backend acceptor [`PeerAcceptor`] wraps.
#[cfg(not(feature = "fips"))]
pub type TlsAcceptor = axum_server::tls_rustls::RustlsAcceptor;
/// The backend acceptor [`PeerAcceptor`] wraps.
#[cfg(feature = "fips")]
pub type TlsAcceptor = axum_server::tls_openssl::OpenSSLAcceptor;

/// Build the serving config from the certificate and key files, trusting `grid_ca_pem`
/// for client certificates.
///
/// # Errors
///
/// Returns an error if the files or the CA do not load.
#[cfg(not(feature = "fips"))]
pub fn server_config(cert: &Path, key: &Path, grid_ca_pem: &str) -> io::Result<TlsConfig> {
    Ok(TlsConfig::from_config(rustls_config(cert, key, grid_ca_pem)?))
}

/// Swap a new certificate, key, and grid CA into a running config.
///
/// # Errors
///
/// Returns an error if the files or the CA do not load. The current config is kept.
#[cfg(not(feature = "fips"))]
pub fn reload(config: &TlsConfig, cert: &Path, key: &Path, grid_ca_pem: &str) -> io::Result<()> {
    config.reload_from_config(rustls_config(cert, key, grid_ca_pem)?);
    Ok(())
}

/// The rustls server config with an optional grid client certificate.
#[cfg(not(feature = "fips"))]
fn rustls_config(cert: &Path, key: &Path, grid_ca_pem: &str) -> io::Result<Arc<rustls::ServerConfig>> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _};

    let invalid = |err: &dyn std::fmt::Display| io::Error::new(io::ErrorKind::InvalidData, err.to_string());
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
    let verifier = certs::GridSpiffeClientVerifier::new(
        grid_ca_pem.as_bytes(),
        certs::DEFAULT_TRUST_DOMAIN,
        provider.signature_verification_algorithms,
    )
    .map_err(|err| invalid(&err))?;
    let chain = CertificateDer::pem_file_iter(cert)
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .map_err(|err| invalid(&err))?;
    let key = PrivateKeyDer::from_pem_file(key).map_err(|err| invalid(&err))?;
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|err| invalid(&err))?
        .with_client_cert_verifier(Arc::new(OptionalClientCert(verifier)))
        .with_single_cert(chain, key)
        .map_err(|err| invalid(&err))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// The grid client verifier, with the certificate optional so enroll needs none.
#[cfg(not(feature = "fips"))]
#[derive(Debug)]
struct OptionalClientCert(Arc<certs::GridSpiffeClientVerifier>);

#[cfg(not(feature = "fips"))]
#[expect(clippy::absolute_paths, reason = "the rustls verifier signatures")]
impl rustls::server::danger::ClientCertVerifier for OptionalClientCert {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        false
    }

    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        self.0.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        self.0.verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

/// Build the serving config from the certificate and key files, trusting `grid_ca_pem`
/// for client certificates.
///
/// # Errors
///
/// Returns an error if the files or the CA do not load.
#[cfg(feature = "fips")]
pub fn server_config(cert: &Path, key: &Path, grid_ca_pem: &str) -> io::Result<TlsConfig> {
    TlsConfig::try_from(openssl_builder(cert, key, grid_ca_pem)?).map_err(io::Error::other)
}

/// Swap a new certificate, key, and grid CA into a running config.
///
/// # Errors
///
/// Returns an error if the files or the CA do not load. The current config is kept.
#[cfg(feature = "fips")]
pub fn reload(config: &TlsConfig, cert: &Path, key: &Path, grid_ca_pem: &str) -> io::Result<()> {
    config.reload_from_acceptor(server_config(cert, key, grid_ca_pem)?.get_inner());
    Ok(())
}

/// The openssl acceptor with an optional grid client certificate.
#[cfg(feature = "fips")]
fn openssl_builder(cert: &Path, key: &Path, grid_ca_pem: &str) -> io::Result<openssl::ssl::SslAcceptorBuilder> {
    use openssl::{
        ssl::{SslAcceptor, SslFiletype, SslMethod, SslVerifyMode},
        x509::{X509, store::X509StoreBuilder},
    };

    let mut builder = SslAcceptor::mozilla_modern_v5(SslMethod::tls()).map_err(io::Error::other)?;
    builder.set_certificate_chain_file(cert).map_err(io::Error::other)?;
    builder
        .set_private_key_file(key, SslFiletype::PEM)
        .map_err(io::Error::other)?;
    let mut store = X509StoreBuilder::new().map_err(io::Error::other)?;
    for ca in X509::stack_from_pem(grid_ca_pem.as_bytes()).map_err(io::Error::other)? {
        store.add_cert(ca).map_err(io::Error::other)?;
    }
    builder.set_verify_cert_store(store.build()).map_err(io::Error::other)?;
    // PEER without FAIL_IF_NO_PEER_CERT: a certificate is optional, but one presented must verify.
    builder.set_verify(SslVerifyMode::PEER);
    // Client verification requires a session id context, or resumption fails.
    builder
        .set_session_id_context(b"grid-enrollment")
        .map_err(io::Error::other)?;
    Ok(builder)
}

/// A TLS stream that can name the client leaf its handshake verified.
pub trait PeerCertificate {
    /// The client leaf, DER, or `None` when the client presented none.
    fn peer_leaf(&self) -> Option<Arc<[u8]>>;
}

#[cfg(not(feature = "fips"))]
impl<I> PeerCertificate for tokio_rustls::server::TlsStream<I> {
    fn peer_leaf(&self) -> Option<Arc<[u8]>> {
        self.get_ref()
            .1
            .peer_certificates()
            .and_then(<[_]>::first)
            .map(|leaf| Arc::from(leaf.as_ref()))
    }
}

#[cfg(feature = "fips")]
impl<I> PeerCertificate for tokio_openssl::SslStream<I> {
    fn peer_leaf(&self) -> Option<Arc<[u8]>> {
        self.ssl()
            .peer_certificate()
            .and_then(|leaf| leaf.to_der().ok())
            .map(Arc::from)
    }
}

/// Wraps the TLS acceptor to hand each connection's client leaf to its handlers.
#[derive(Clone, Debug)]
pub struct PeerAcceptor<A>(pub A);

impl<A, I, S> Accept<I, S> for PeerAcceptor<A>
where
    A: Accept<I, S> + Clone + Send + 'static,
    A::Stream: PeerCertificate + Send,
    A::Service: Send,
    A::Future: Send,
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Send + 'static,
{
    type Future = Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Self::Service)>> + Send>>;
    type Service = AddExtension<A::Service, PeerLeaf>;
    type Stream = A::Stream;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        let accepted = self.0.accept(stream, service);
        Box::pin(async move {
            let (tls, inner) = accepted.await?;
            let leaf = PeerLeaf(tls.peer_leaf());
            Ok((tls, Extension(leaf).layer(inner)))
        })
    }
}
