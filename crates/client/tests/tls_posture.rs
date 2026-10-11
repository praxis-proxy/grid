//! What the clients this crate builds will and will not talk to.

#![expect(
    clippy::tests_outside_test_module,
    clippy::expect_used,
    reason = "integration tests live in tests/"
)]

use std::sync::Arc;

use grid_client::{RequestOutcome, Timeouts, classify, roots_from_pem, trusted_client};
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

/// Issue a `localhost` leaf under a fresh CA.
fn material() -> Material {
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
                let mut buf = [0_u8; 1024];
                let _read = tls.read(&mut buf).await;
                let _written = tls.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok").await;
                let _flushed = tls.shutdown().await;
            }));
        }
    }));
    port
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
