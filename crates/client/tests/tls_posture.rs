//! What the clients this crate builds will and will not talk to.

#![expect(
    clippy::tests_outside_test_module,
    clippy::expect_used,
    reason = "integration tests live in tests/"
)]

use std::sync::Arc;

use grid_client::{RequestOutcome, Timeouts, classify, from_status, roots_from_pem, trusted_client};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// A CA and a leaf for `localhost`.
struct Material {
    /// The CA that signed the leaf.
    ca_pem: String,
    /// The leaf certificate chain.
    chain: Vec<CertificateDer<'static>>,
    /// The leaf key.
    key: PrivateKeyDer<'static>,
}

/// Initialize test TLS and issue a `localhost` leaf under a fresh CA.
fn material() -> Material {
    let _installed = rustls::crypto::ring::default_provider().install_default();
    let ca = certs::generate_ca("test-ca").expect("a CA");
    let leaf = certs::generate_dns_cert(&ca, "server", "localhost").expect("a leaf");
    Material {
        ca_pem: ca.cert_pem,
        chain: CertificateDer::pem_slice_iter(leaf.cert_pem.as_bytes())
            .collect::<Result<_, _>>()
            .expect("a chain"),
        key: PrivateKeyDer::from_pem_slice(leaf.key_pem.as_bytes()).expect("a key"),
    }
}

/// Serve TLS and answer every request, returning the bound port.
async fn serve(material: Material) -> u16 {
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(material.chain, material.key)
        .expect("a server config");
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("a port");
    let port = listener.local_addr().expect("an address").port();
    drop(tokio::spawn(async move {
        while let Ok((stream, _peer)) = listener.accept().await {
            let acceptor = acceptor.clone();
            drop(tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(stream).await else {
                    return;
                };
                answer(&mut tls).await;
            }));
        }
    }));
    port
}

async fn answer<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(stream: &mut S) {
    let mut buf = [0_u8; 1024];
    let read = stream.read(&mut buf).await.unwrap_or(0);
    let wants_redirect = buf
        .get(..read)
        .and_then(|req| std::str::from_utf8(req).ok())
        .is_some_and(|req| req.contains("/elsewhere"));
    let body: &[u8] = if wants_redirect {
        b"HTTP/1.1 302 Found\r\nlocation: https://example.invalid/\r\ncontent-length: 0\r\n\r\n"
    } else {
        b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok"
    };
    let _written = stream.write_all(body).await;
    let _flushed = stream.shutdown().await;
}

#[tokio::test]
async fn the_right_ca_reaches_the_peer() {
    let material = material();
    let roots = roots_from_pem(material.ca_pem.as_bytes(), "test CA").expect("roots");
    let port = serve(material).await;

    let client = trusted_client(roots, None, Timeouts::DEFAULT).expect("a client");
    let response = client
        .get(format!("https://localhost:{port}/"))
        .send()
        .await
        .expect("the peer answers");
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn an_unrelated_ca_is_refused_and_reads_as_a_trust_failure() {
    let port = serve(material()).await;
    let stranger = certs::generate_ca("stranger-ca").expect("another CA");
    let roots = roots_from_pem(stranger.cert_pem.as_bytes(), "stranger CA").expect("roots");

    let client = trusted_client(roots, None, Timeouts::DEFAULT).expect("a client");
    let error = client
        .get(format!("https://localhost:{port}/"))
        .send()
        .await
        .expect_err("a leaf from an unrelated CA must not verify");
    assert_eq!(
        classify(&error),
        RequestOutcome::Tls,
        "a rejected chain must not read as a refusal, which would retry it: {error}"
    );
    assert!(!classify(&error).is_retryable());
}

#[tokio::test]
async fn plaintext_is_refused() {
    let material = material();
    let roots = roots_from_pem(material.ca_pem.as_bytes(), "test CA").expect("roots");
    let port = serve(material).await;

    let client = trusted_client(roots, None, Timeouts::DEFAULT).expect("a client");
    let error = client
        .get(format!("http://localhost:{port}/"))
        .send()
        .await
        .expect_err("https_only must refuse an http url");
    assert!(!classify(&error).is_retryable(), "{error}");
}

#[tokio::test]
async fn a_connect_timeout_is_safe_to_retry() {
    let material = material();
    let roots = roots_from_pem(material.ca_pem.as_bytes(), "test CA").expect("roots");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("a port");
    let port = listener.local_addr().expect("an address").port();
    let client = trusted_client(
        roots,
        None,
        Timeouts {
            connect: std::time::Duration::from_millis(100),
            request: std::time::Duration::from_secs(5),
        },
    )
    .expect("a client");
    let (response, accepted) = tokio::join!(
        client.get(format!("https://localhost:{port}/")).send(),
        listener.accept()
    );
    let (_stream, _peer) = accepted.expect("accepted TCP");
    let error = response.expect_err("TLS connection must time out");
    assert!(error.is_timeout(), "{error}");
    assert!(error.is_connect(), "{error}");
    assert_eq!(classify(&error), RequestOutcome::Refused);
}

#[tokio::test]
async fn a_timeout_after_sending_is_not_a_connection_refusal() {
    let material = material();
    let roots = roots_from_pem(material.ca_pem.as_bytes(), "test CA").expect("roots");
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(material.chain, material.key)
        .expect("a server config");
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("a port");
    let port = listener.local_addr().expect("an address").port();
    let client = trusted_client(roots, None, Timeouts::DEFAULT).expect("a client");
    let stalled = async {
        let (stream, _peer) = listener.accept().await.expect("accepted TCP");
        let mut tls = acceptor.accept(stream).await.expect("TLS handshake");
        let mut buf = [0_u8; 1024];
        assert!(tls.read(&mut buf).await.expect("a request") > 0);
        tls
    };
    let (response, _tls) = tokio::join!(
        client
            .get(format!("https://localhost:{port}/"))
            .timeout(std::time::Duration::from_secs(1))
            .send(),
        stalled
    );
    let error = response.expect_err("the unanswered request must time out");
    assert!(error.is_timeout(), "{error}");
    assert!(!error.is_connect(), "{error}");
    assert_eq!(classify(&error), RequestOutcome::Timeout);
}

#[tokio::test]
async fn a_redirect_is_handed_back_rather_than_followed() {
    let material = material();
    let roots = roots_from_pem(material.ca_pem.as_bytes(), "test CA").expect("roots");
    let port = serve(material).await;

    let client = trusted_client(roots, None, Timeouts::DEFAULT).expect("a client");
    let response = client
        .get(format!("https://localhost:{port}/elsewhere"))
        .send()
        .await
        .expect("a refused redirect is an answer, not an error");
    assert_eq!(response.status(), 302, "the client must not chase the location");
    assert_eq!(
        from_status(response.status()),
        RequestOutcome::Config,
        "nothing will resolve a 3xx the client will not follow"
    );
    assert!(!from_status(response.status()).is_retryable());
}
