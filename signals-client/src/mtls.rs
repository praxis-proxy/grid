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
        })
    }
}

impl SignalSource for PeerScraper {
    #[expect(
        clippy::too_many_lines,
        clippy::large_stack_frames,
        reason = "sequential connect, handshake, authorize, request, read over rustls/hyper types"
    )]
    async fn fetch(&self) -> Result<Scrape, FetchError> {
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

        let request = Request::builder()
            .method(http::Method::GET)
            .uri(&self.path)
            .header(header::HOST, &self.authority)
            .body(Empty::<Bytes>::new())
            .map_err(|error| FetchError::Unreachable(error.to_string()))?;

        let response = timeout(self.request_timeout, sender.send_request(request))
            .await
            .map_err(|_elapsed| FetchError::Unreachable("request timed out".to_owned()))?
            .map_err(|error| FetchError::Unreachable(format!("request: {error}")))?;

        let scraped = read_scrape_response(response, Arc::from(verified_id), self.request_timeout).await;
        driver.abort();
        scraped
    }
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
