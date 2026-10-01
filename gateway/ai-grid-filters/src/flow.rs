//! Full poll to route flow: scrape a peer over mTLS, store it, order, select.
//!
//! The pieces have unit tests in their own crates. These stitch the real
//! `PeerScraper`, the `LoadStore`, and `RouteSnapshot::from_store` plus
//! `select_admitted` into one path so the composition is exercised, including
//! the failure cases where a peer is unreachable or untrusted and the router
//! must still produce a sound decision.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::min_ident_chars,
    clippy::too_many_lines,
    reason = "tests"
)]

use std::{sync::Arc, time::Duration};

use certs::{DEFAULT_TRUST_DOMAIN, GridSpiffeClientVerifier, generate_ca, generate_site_cert, spiffe_id};
use grid_signals::LoadStore;
use grid_signals_client::{FetchError, PeerScraper, SignalSource as _};
use rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _},
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpListener,
};
use tokio_rustls::TlsAcceptor;

use crate::{
    descriptor::{CandidateConfig, CapabilityKind, RouteCandidate, validate_candidates},
    route::select_admitted,
    snapshot::{LOAD_METRIC, RouteSnapshot},
};

/// PEM chain + key from a generated cert.
fn material(cert_pem: &str, key_pem: &str) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let chain = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .map(|cert| cert.expect("cert").into_owned())
        .collect();
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).expect("key");
    (chain, key)
}

/// The site an owner-verified SPIFFE id names, as the poller binds it.
fn owner_of(peer_identity: &str) -> String {
    certs::site_of_spiffe_id(peer_identity)
        .expect("a grid SPIFFE id")
        .to_owned()
}

/// One exposition line at t=1000ms for `site`/`cluster`.
fn line(site: &str, cluster: &str, value: f64) -> String {
    format!(r#"{LOAD_METRIC}{{grid_site="{site}",grid_provider="{cluster}"}} {value} 1000"#)
}

/// A raw HTTP/1.1 200 carrying `body`, with the Date the poller anchors on
/// (1000ms since the epoch).
fn http_ok(body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 200 OK\r\nDate: Thu, 01 Jan 1970 00:00:01 GMT\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

/// A TLS peer that serves `response` once, verifying only its own side. The
/// client still presents its cert, but this peer does not verify it.
async fn serve_once(chain: Vec<CertificateDer<'static>>, key: PrivateKeyDer<'static>, response: Vec<u8>) -> String {
    let provider = rustls::crypto::ring::default_provider();
    let config = ServerConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .expect("server cert");
    accept_one(config, response).await
}

/// A TLS peer that requires and verifies the client's grid certificate, then
/// serves `response`. Proves the scrape hop is mutually authenticated.
async fn serve_once_mutual(
    ca_pem: &[u8],
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    response: Vec<u8>,
) -> String {
    let provider = rustls::crypto::ring::default_provider();
    let algorithms = provider.signature_verification_algorithms;
    let client_verifier = GridSpiffeClientVerifier::new(ca_pem, DEFAULT_TRUST_DOMAIN, algorithms).expect("verifier");
    let config = ServerConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(chain, key)
        .expect("server cert");
    accept_one(config, response).await
}

/// Bind a TLS listener on a free port, accept one connection, serve `response`.
async fn accept_one(config: ServerConfig, response: Vec<u8>) -> String {
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    tokio::spawn(async move {
        let Ok((tcp, _)) = listener.accept().await else {
            return;
        };
        // A rejected client (the mutual peer refusing a rogue cert) aborts the
        // handshake here, an expected outcome for the negative case, so the
        // server task returns quietly rather than panicking.
        let Ok(mut tls) = acceptor.accept(tcp).await else {
            return;
        };
        let mut buf = [0_u8; 2048];
        let _read = tls.read(&mut buf).await;
        if tls.write_all(&response).await.is_err() {
            return;
        }
        let _flushed = tls.flush().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    });
    addr
}

/// A bare TCP listener that accepts but never completes a TLS handshake, so a
/// scrape against it stalls to the request deadline.
async fn black_hole() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    tokio::spawn(async move {
        let (_tcp, _) = listener.accept().await.expect("accept");
        tokio::time::sleep(Duration::from_secs(30)).await;
    });
    addr
}

/// A `PeerScraper` dialing `addr`, expecting the peer to prove it is `site`.
fn scraper(
    ca_pem: &[u8],
    client: &(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>),
    addr: &str,
    site: &str,
) -> PeerScraper {
    let (chain, key) = (client.0.clone(), client.1.clone_key());
    PeerScraper::new(
        ca_pem,
        chain,
        key,
        addr,
        ServerName::try_from(format!("{site}.grid.internal")).expect("sni"),
        &format!("{site}.grid.internal"),
        "/v1/site/signals",
        &spiffe_id(site),
        Duration::from_millis(500),
        Duration::from_millis(500),
    )
    .expect("scraper")
}

/// Candidates for `model` at each (site, cluster), all admitting new requests.
fn candidates(model: &str, sites: &[(&str, &str)]) -> Vec<RouteCandidate> {
    let raw = sites
        .iter()
        .map(|(site, cluster)| CandidateConfig {
            cluster: (*cluster).to_owned(),
            credential: None,
            fresh: true,
            kind: CapabilityKind::InferenceModel,
            name: model.to_owned(),
            site: (*site).to_owned(),
        })
        .collect();
    validate_candidates(raw).expect("valid candidates")
}

/// Scrape `peer` and record its readings against the verified owner, exactly as
/// the poller would.
async fn scrape_into(store: &LoadStore, peer: &PeerScraper) {
    let scrape = peer.fetch().await.expect("scrape ok");
    store.ingest_at(
        &scrape.body,
        scrape.date_ms,
        scrape.date_ms,
        &owner_of(&scrape.peer_identity),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scrape_store_route_picks_the_least_loaded_site() {
    let ca = generate_ca("grid-ca").expect("ca");
    let poller = material_of(&generate_site_cert(&ca, "poller").expect("poller cert"));
    let east = generate_site_cert(&ca, "east").expect("east cert");
    let west = generate_site_cert(&ca, "west").expect("west cert");
    let (east_chain, east_key) = material(&east.cert_pem, &east.key_pem);
    let (west_chain, west_key) = material(&west.cert_pem, &west.key_pem);

    // East is busy, west is idle, both serving the same model.
    let east_addr = serve_once(east_chain, east_key, http_ok(&line("east", "pool-a", 90.0))).await;
    let west_addr = serve_once(west_chain, west_key, http_ok(&line("west", "pool-b", 10.0))).await;

    let store = LoadStore::new(Duration::from_secs(60));
    scrape_into(&store, &scraper(ca.cert_pem.as_bytes(), &poller, &east_addr, "east")).await;
    scrape_into(&store, &scraper(ca.cert_pem.as_bytes(), &poller, &west_addr, "west")).await;

    let snapshot = RouteSnapshot::from_store(
        candidates("llama", &[("east", "pool-a"), ("west", "pool-b")]),
        Arc::from("local"),
        &store,
        1_000,
        30_000,
    );
    let chosen = select_admitted(&snapshot.candidates, CapabilityKind::InferenceModel, "llama").expect("a route");
    assert_eq!(&*chosen.cluster, "pool-b", "the idle site wins end to end");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_scrape_hop_is_mutually_authenticated_then_routes() {
    let ca = generate_ca("grid-ca").expect("ca");
    let poller = material_of(&generate_site_cert(&ca, "poller").expect("poller cert"));
    let east = generate_site_cert(&ca, "east").expect("east cert");
    let (east_chain, east_key) = material(&east.cert_pem, &east.key_pem);

    // The peer requires and verifies the poller's grid certificate.
    let addr = serve_once_mutual(
        ca.cert_pem.as_bytes(),
        east_chain,
        east_key,
        http_ok(&line("east", "pool-a", 5.0)),
    )
    .await;

    let peer = scraper(ca.cert_pem.as_bytes(), &poller, &addr, "east");
    let scrape = peer.fetch().await.expect("mutual scrape ok");
    assert_eq!(
        &*scrape.peer_identity,
        &spiffe_id("east"),
        "the poller verified the peer's identity, and the peer accepted the poller's",
    );

    let store = LoadStore::new(Duration::from_secs(60));
    store.ingest_at(
        &scrape.body,
        scrape.date_ms,
        scrape.date_ms,
        &owner_of(&scrape.peer_identity),
    );
    let snapshot = RouteSnapshot::from_store(
        candidates("llama", &[("east", "pool-a")]),
        Arc::from("local"),
        &store,
        1_000,
        30_000,
    );
    let chosen = select_admitted(&snapshot.candidates, CapabilityKind::InferenceModel, "llama").expect("a route");
    assert_eq!(&*chosen.cluster, "pool-a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_mutual_peer_refuses_a_client_from_another_ca() {
    let ca = generate_ca("grid-ca").expect("ca");
    let east = generate_site_cert(&ca, "east").expect("east cert");
    let (east_chain, east_key) = material(&east.cert_pem, &east.key_pem);
    let addr = serve_once_mutual(
        ca.cert_pem.as_bytes(),
        east_chain,
        east_key,
        http_ok(&line("east", "pool-a", 5.0)),
    )
    .await;

    // The poller presents a client cert signed by a different CA. The mutual
    // peer verifies clients against the grid CA, so it must abort the handshake.
    // A scrape that succeeded here would mean the peer was not verifying its
    // client at all (the with_no_client_auth trap), so this pins the 1:1 mTLS
    // claim at the composition level, not just in the certs unit tests.
    let rogue_ca = generate_ca("rogue-ca").expect("rogue ca");
    let rogue_client = material_of(&generate_site_cert(&rogue_ca, "poller").expect("rogue poller cert"));
    let err = scraper(ca.cert_pem.as_bytes(), &rogue_client, &addr, "east")
        .fetch()
        .await
        .expect_err("the mutual peer must refuse a client signed by another CA");
    // The peer accepts the TLS handshake far enough to receive the client cert,
    // then aborts on verifying it, so the poller sees the refusal as a dropped
    // connection rather than a handshake error. Either way the scrape fails and
    // no reading is produced.
    assert!(matches!(&err, FetchError::Unreachable(_)), "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loss_of_signal_sorts_the_unmeasured_site_last() {
    let ca = generate_ca("grid-ca").expect("ca");
    let poller = material_of(&generate_site_cert(&ca, "poller").expect("poller cert"));
    let east = generate_site_cert(&ca, "east").expect("east cert");
    let (east_chain, east_key) = material(&east.cert_pem, &east.key_pem);

    // Only east reports. West is never scraped, so it has no reading.
    let east_addr = serve_once(east_chain, east_key, http_ok(&line("east", "pool-a", 90.0))).await;
    let store = LoadStore::new(Duration::from_secs(60));
    scrape_into(&store, &scraper(ca.cert_pem.as_bytes(), &poller, &east_addr, "east")).await;

    let snapshot = RouteSnapshot::from_store(
        candidates("llama", &[("west", "pool-b"), ("east", "pool-a")]),
        Arc::from("local"),
        &store,
        1_000,
        30_000,
    );
    // Even though east is busy (90), west has no signal at all, so it sorts
    // last and the measured site is chosen. Loss of signal is "least preferred".
    let chosen = select_admitted(&snapshot.candidates, CapabilityKind::InferenceModel, "llama").expect("a route");
    assert_eq!(
        &*chosen.cluster, "pool-a",
        "a measured busy site beats an unmeasured one"
    );
    assert_eq!(&*snapshot.candidates.last().expect("candidates").site, "west");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreachable_peer_leaves_no_reading_and_the_reachable_one_routes() {
    let ca = generate_ca("grid-ca").expect("ca");
    let poller = material_of(&generate_site_cert(&ca, "poller").expect("poller cert"));
    let west = generate_site_cert(&ca, "west").expect("west cert");
    let (west_chain, west_key) = material(&west.cert_pem, &west.key_pem);

    let west_addr = serve_once(west_chain, west_key, http_ok(&line("west", "pool-b", 40.0))).await;
    let east_addr = black_hole().await;

    let store = LoadStore::new(Duration::from_secs(60));
    // East stalls the handshake and returns an error, so nothing is stored for it.
    let east_err = scraper(ca.cert_pem.as_bytes(), &poller, &east_addr, "east")
        .fetch()
        .await
        .expect_err("unreachable peer must error");
    assert!(matches!(east_err, FetchError::Unreachable(_)), "{east_err:?}");
    scrape_into(&store, &scraper(ca.cert_pem.as_bytes(), &poller, &west_addr, "west")).await;

    let snapshot = RouteSnapshot::from_store(
        candidates("llama", &[("east", "pool-a"), ("west", "pool-b")]),
        Arc::from("local"),
        &store,
        1_000,
        30_000,
    );
    let chosen = select_admitted(&snapshot.candidates, CapabilityKind::InferenceModel, "llama").expect("a route");
    assert_eq!(&*chosen.cluster, "pool-b", "the reachable site is chosen");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_untrusted_peer_is_refused_and_contributes_no_reading() {
    let ca = generate_ca("grid-ca").expect("ca");
    let poller = material_of(&generate_site_cert(&ca, "poller").expect("poller cert"));

    // The peer's cert is signed by a different CA, so the poller refuses it.
    let rogue_ca = generate_ca("rogue-ca").expect("rogue ca");
    let rogue = generate_site_cert(&rogue_ca, "east").expect("rogue east cert");
    let (rogue_chain, rogue_key) = material(&rogue.cert_pem, &rogue.key_pem);
    let addr = serve_once(rogue_chain, rogue_key, http_ok(&line("east", "pool-a", 1.0))).await;

    let err = scraper(ca.cert_pem.as_bytes(), &poller, &addr, "east")
        .fetch()
        .await
        .expect_err("untrusted peer must be refused");
    // A server cert from another CA fails the TLS handshake itself, so it pins
    // "refused because untrusted," not any connect failure.
    assert!(
        matches!(&err, FetchError::Unreachable(message) if message.contains("tls handshake")),
        "{err:?}"
    );

    // With no reading, the candidate sorts last and cannot be chosen over a
    // measured peer.
    let store = LoadStore::new(Duration::from_secs(60));
    store.ingest_at(&line("west", "pool-b", 50.0), 1_000, 1_000, "west");
    let snapshot = RouteSnapshot::from_store(
        candidates("llama", &[("east", "pool-a"), ("west", "pool-b")]),
        Arc::from("local"),
        &store,
        1_000,
        30_000,
    );
    let chosen = select_admitted(&snapshot.candidates, CapabilityKind::InferenceModel, "llama").expect("a route");
    assert_eq!(&*chosen.cluster, "pool-b", "only the trusted, measured peer routes");
}

/// PEM material from a generated site cert.
fn material_of(cert: &certs::SiteCertOutput) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    material(&cert.cert_pem, &cert.key_pem)
}
