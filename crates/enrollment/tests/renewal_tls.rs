//! Renewal over a real mutual-TLS listener: the acceptor hands the handshake's leaf to the handler.

#![cfg(not(feature = "fips"))]
#![expect(
    clippy::tests_outside_test_module,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "integration tests live in tests/, and serde_json::Value indexing yields Null rather than panicking"
)]

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use enrollment::{
    AppState, GridAdmins, SharedCa, Store,
    authz::Authorizer,
    router,
    tls::{PeerAcceptor, TlsAcceptor},
};
use reqwest::StatusCode;
use serde_json::{Value, json};
use time::{Duration, OffsetDateTime};

/// A listener serving the enrollment router over TLS, and the grid CA behind it.
struct Served {
    addr: SocketAddr,
    ca: certs::CaCert,
    state: Arc<AppState>,
}

/// Serve a fresh grid on an ephemeral port, with `hub` reserved.
fn serve() -> Served {
    if rustls::crypto::ring::default_provider().install_default().is_err() {
        // Another test installed it.
    }
    let ca = certs::generate_ca("test-grid-ca").expect("ca");
    let serving = certs::generate_dns_only_cert(&ca, "enroll", &["localhost".to_owned()]).expect("serving");
    let dir = std::env::temp_dir().join(format!("renewal-tls-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).expect("dir");
    let (cert, key): (PathBuf, PathBuf) = (dir.join("tls.crt"), dir.join("tls.key"));
    std::fs::write(&cert, &serving.cert_pem).expect("cert");
    std::fs::write(&key, &serving.key_pem).expect("key");
    let tls = enrollment::tls::server_config(&cert, &key, &ca.cert_pem).expect("tls");

    let copy = certs::load_ca("test-grid-ca", &ca.key_pem, &ca.cert_pem).expect("copy");
    let state = Arc::new(AppState {
        store: Store::memory(),
        ca: SharedCa::new(ca),
        authorizer: Authorizer::Local(GridAdmins::from_table("tester: t0ken\n")),
        cert_lifetime: certs::DEFAULT_SITE_CERT_LIFETIME,
        reserved_sites: vec!["hub".to_owned()],
        renewals_enabled: true,
    });
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let addr = listener.local_addr().expect("addr");
    let server = axum_server::from_tcp(listener)
        .expect("listener")
        .acceptor(PeerAcceptor(TlsAcceptor::new(tls)));
    tokio::spawn(server.serve(router(Arc::clone(&state)).into_make_service()));
    Served { addr, ca: copy, state }
}

/// A client trusting the grid CA, presenting `identity` when given.
fn client(served: &Served, identity: Option<(&str, &str)>) -> reqwest::Client {
    let root = reqwest::Certificate::from_pem(served.ca.cert_pem.as_bytes()).expect("root");
    let mut builder = reqwest::Client::builder().tls_certs_only([root]);
    if let Some((cert, key)) = identity {
        builder = builder.identity(reqwest::Identity::from_pem(format!("{cert}{key}").as_bytes()).expect("identity"));
    }
    builder.build().expect("client")
}

async fn renew(served: &Served, client: &reqwest::Client) -> Result<(StatusCode, Value), reqwest::Error> {
    let csr = certs::generate_csr("hub").expect("csr");
    let response = client
        .post(format!("https://localhost:{}/v1alpha1/rotations", served.addr.port()))
        .json(&json!({ "csr": csr.csr_pem }))
        .send()
        .await?;
    let status = response.status();
    Ok((status, response.json().await.unwrap_or(Value::Null)))
}

/// A hub leaf from the grid CA and its seed, as bootstrap issues them.
async fn hub_leaf(served: &Served, validity: certs::Validity) -> (String, String) {
    let csr = certs::generate_csr("hub").expect("csr");
    let cert = certs::sign_csr(&served.ca, "hub", &csr.csr_pem, validity).expect("sign");
    let seed = enrollment::SeedRecord {
        site_name: "hub".to_owned(),
        key_sha256: cert.public_key_sha256.clone(),
        generation: 1,
        issued_at: certs::cert_validity(&cert.cert_pem).expect("validity").0,
    };
    served.state.store.seed_reserved(&seed).await.expect("seed");
    (cert.cert_pem, csr.key_pem.to_string())
}

#[tokio::test]
async fn the_handshake_leaf_reaches_the_renewal_handler() {
    let served = serve();
    let (cert, key) = hub_leaf(&served, certs::Validity::default()).await;
    let (status, body) = renew(&served, &client(&served, Some((&cert, &key))))
        .await
        .expect("renew");
    assert_eq!(status, StatusCode::OK, "renewed over mutual TLS: {body}");
    assert_eq!(body["spiffeId"], certs::spiffe_id("hub"));
}

#[tokio::test]
async fn a_client_without_a_certificate_still_connects_and_is_refused_renewal() {
    let served = serve();
    let (status, body) = renew(&served, &client(&served, None)).await.expect("connects");
    assert_eq!(status, StatusCode::UNAUTHORIZED, "enroll-style clients still connect");
    assert_eq!(body["error"], "identity_required");
}

#[tokio::test]
async fn an_expired_certificate_fails_the_handshake() {
    let served = serve();
    let now = OffsetDateTime::now_utc();
    let (cert, key) = hub_leaf(
        &served,
        certs::Validity {
            not_before: now.saturating_sub(Duration::days(40)),
            not_after: now.saturating_sub(Duration::days(10)),
        },
    )
    .await;
    let refused = renew(&served, &client(&served, Some((&cert, &key)))).await;
    assert!(refused.is_err(), "an expired leaf never reaches a handler: {refused:?}");
}
