//! The gateway control plane: poll peers, order candidates, swap the snapshot.
//!
//! The operator writes a grid serving config (topology + peers). The gateway
//! reads it, builds the load store and a cold-start snapshot, and spawns one
//! poller per peer. Each poll cycle re-orders the candidate set by live load and
//! swaps the shared snapshot, on one clock, so the data-plane filter only ever
//! reads a resolved order. This is the control side of the control/data split:
//! the poller and the refresh loop live here, not in the filter.

use std::{fs, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use certs::spiffe_id;
use grid_signals::{LoadStore, now_ms};
use grid_signals_client::{PeerScraper, PollHandle, PollerConfig, spawn_on_thread};
use praxis_filter::FilterError;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _};
use serde::Deserialize;

use crate::{
    descriptor::{CandidateConfig, RouteCandidate, validate_candidates, validate_local_site},
    snapshot::RouteSnapshot,
};

/// Default signals endpoint path.
fn default_path() -> String {
    "/v1/site/signals".to_owned()
}

/// Default poll interval, milliseconds.
fn default_interval_ms() -> u64 {
    2_000
}

/// Default connect and request timeout, milliseconds.
fn default_timeout_ms() -> u64 {
    2_000
}

/// The grid serving config the operator writes and the gateway reads directly,
/// distinct from the praxis data-plane config.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GridServingConfig {
    /// This gateway's own site.
    pub local_site: String,

    /// Store retention per series, seconds.
    pub window_secs: u64,

    /// Freshness window the router orders over, milliseconds.
    pub load_window_ms: i64,

    /// The candidate topology: which sites serve which capabilities.
    pub candidates: Vec<CandidateConfig>,

    /// Peers to poll for live load.
    pub peers: Vec<PeerServingConfig>,
}

/// One peer this gateway polls, with the mTLS material to reach it.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerServingConfig {
    /// The peer's mTLS-verified site (its SPIFFE name), the expected scrape target.
    pub site: String,

    /// TCP address of the peer's signals endpoint.
    pub addr: String,

    /// TLS server name (SNI) presented to the peer.
    pub server_name: String,

    /// HTTP authority (Host) for the scrape request.
    pub authority: String,

    /// Signals endpoint path.
    #[serde(default = "default_path")]
    pub path: String,

    /// Poll interval, milliseconds. Rejected when zero, which would hand
    /// `Duration::ZERO` to the poller's interval timer and panic its thread.
    #[serde(
        default = "default_interval_ms",
        deserialize_with = "grid_signals_client::deserialize_interval_ms"
    )]
    pub interval_ms: u64,

    /// Connect timeout, milliseconds.
    #[serde(default = "default_timeout_ms")]
    pub connect_timeout_ms: u64,

    /// Request timeout, milliseconds.
    #[serde(default = "default_timeout_ms")]
    pub request_timeout_ms: u64,

    /// PEM file the grid CA bundle is read from.
    pub grid_ca_path: String,

    /// PEM file this gateway's client certificate chain is read from.
    pub client_cert_path: String,

    /// PEM file the client private key is read from.
    pub client_key_path: String,
}

/// The running control plane: the shared snapshot the filter reads and the
/// pollers that keep it fresh.
pub struct GridRuntime {
    /// The snapshot the refresh loop swaps and the filter reads.
    snapshot: Arc<ArcSwap<RouteSnapshot>>,

    /// The running pollers. Held for their drop: dropping `GridRuntime` stops
    /// every poller and lets the store drain.
    #[expect(dead_code, reason = "held so the pollers keep running; drop stops them")]
    pollers: Vec<PollHandle>,
}

impl GridRuntime {
    /// The shared snapshot to register the filter over.
    #[must_use]
    pub fn snapshot(&self) -> Arc<ArcSwap<RouteSnapshot>> {
        Arc::clone(&self.snapshot)
    }
}

/// Read and parse a grid serving config file.
///
/// # Errors
///
/// Returns [`FilterError`] if the file cannot be read or does not parse.
pub fn load_serving_config(path: &str) -> Result<GridServingConfig, FilterError> {
    let text =
        fs::read_to_string(path).map_err(|error| -> FilterError { format!("grid: reading {path}: {error}").into() })?;
    serde_yaml::from_str(&text).map_err(|error| -> FilterError { format!("grid: parsing {path}: {error}").into() })
}

/// Build the control plane from `config`: the store, the cold-start snapshot, and
/// one poller per peer whose refresh closure re-orders and swaps the snapshot.
///
/// # Errors
///
/// Returns [`FilterError`] if the local site or candidate topology is invalid, a
/// peer's certificate material cannot be read or parsed, or a poller thread
/// cannot be spawned.
pub fn spawn_grid_routing(config: &GridServingConfig) -> Result<GridRuntime, FilterError> {
    validate_local_site(&config.local_site)?;
    let local_site: Arc<str> = Arc::from(config.local_site.as_str());
    let base: Arc<[RouteCandidate]> = Arc::from(validate_candidates(config.candidates.clone())?);
    let store = Arc::new(LoadStore::new(Duration::from_secs(config.window_secs)));
    // Cold start: config order until the first poll re-orders it by live load.
    let cold_start = RouteSnapshot::from_static(base.iter().cloned().collect(), Arc::clone(&local_site));
    let snapshot = Arc::new(ArcSwap::from_pointee(cold_start));

    let mut pollers = Vec::with_capacity(config.peers.len());
    for peer in &config.peers {
        validate_peer(peer)?;
        let scraper = build_scraper(peer)?;
        let poller_config = PollerConfig {
            endpoint: peer.addr.clone(),
            interval_ms: peer.interval_ms,
            window_secs: config.window_secs,
            max_age_ms: config.load_window_ms,
            timeout_ms: peer.request_timeout_ms,
            tls: None,
        };
        let refresh = make_refresh(
            Arc::clone(&base),
            Arc::clone(&local_site),
            Arc::clone(&snapshot),
            config.load_window_ms,
            now_ms,
        );
        let handle = spawn_on_thread(Arc::clone(&store), &poller_config, scraper, refresh)
            .map_err(|error| -> FilterError { format!("grid: spawning poller for {}: {error}", peer.site).into() })?;
        pollers.push(handle);
    }
    Ok(GridRuntime { snapshot, pollers })
}

/// The refresh closure: re-order the base candidate set by live load and swap it
/// into the shared snapshot. Runs on each poll cycle, so the snapshot tracks the
/// store on one clock.
fn make_refresh<N>(
    base: Arc<[RouteCandidate]>,
    local_site: Arc<str>,
    snapshot: Arc<ArcSwap<RouteSnapshot>>,
    window_ms: i64,
    now: N,
) -> impl Fn(&LoadStore) + Send
where
    N: Fn() -> i64 + Send,
{
    move |store: &LoadStore| {
        let ordered = RouteSnapshot::from_store(
            base.iter().cloned().collect(),
            Arc::clone(&local_site),
            store,
            now(),
            window_ms,
        );
        snapshot.store(Arc::new(ordered));
    }
}

/// Reject peer settings that would silently stop a poller.
///
/// A zero interval hands `Duration::ZERO` to the interval timer, which panics the
/// detached poller thread. A zero timeout fires immediately, so the peer never
/// scrapes. Either way that site ages to `+inf` and sorts last, a silent stale
/// misroute. The serde guard on `interval_ms` catches a parsed config, but a
/// struct literal can bypass serde, so admission checks here too.
///
/// # Errors
///
/// Returns [`FilterError`] if the interval or either timeout is zero.
fn validate_peer(peer: &PeerServingConfig) -> Result<(), FilterError> {
    if peer.interval_ms == 0 {
        return Err(format!("grid: peer {} interval_ms must be greater than zero", peer.site).into());
    }
    if peer.connect_timeout_ms == 0 || peer.request_timeout_ms == 0 {
        return Err(format!("grid: peer {} timeouts must be greater than zero", peer.site).into());
    }
    Ok(())
}

/// Build a peer's mTLS scraper from its config, reading and parsing its
/// certificate material.
fn build_scraper(peer: &PeerServingConfig) -> Result<PeerScraper, FilterError> {
    let ca_pem = fs::read(&peer.grid_ca_path)
        .map_err(|error| -> FilterError { format!("grid: reading {}: {error}", peer.grid_ca_path).into() })?;
    let cert_pem = fs::read(&peer.client_cert_path)
        .map_err(|error| -> FilterError { format!("grid: reading {}: {error}", peer.client_cert_path).into() })?;
    let key_pem = fs::read(&peer.client_key_path)
        .map_err(|error| -> FilterError { format!("grid: reading {}: {error}", peer.client_key_path).into() })?;
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&cert_pem)
        .collect::<Result<_, _>>()
        .map_err(|error| -> FilterError { format!("grid: parsing {}: {error}", peer.client_cert_path).into() })?;
    let key = PrivateKeyDer::from_pem_slice(&key_pem)
        .map_err(|error| -> FilterError { format!("grid: parsing {}: {error}", peer.client_key_path).into() })?;
    let server_name = ServerName::try_from(peer.server_name.clone()).map_err(|error| -> FilterError {
        format!("grid: invalid server_name {}: {error}", peer.server_name).into()
    })?;
    let expected_target = spiffe_id(&peer.site);
    PeerScraper::new(
        &ca_pem,
        chain,
        key,
        &peer.addr,
        server_name,
        &peer.authority,
        &peer.path,
        &expected_target,
        Duration::from_millis(peer.connect_timeout_ms),
        Duration::from_millis(peer.request_timeout_ms),
    )
    .map_err(|error| -> FilterError { format!("grid: building scraper for {}: {error}", peer.site).into() })
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::min_ident_chars,
    reason = "tests"
)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};

    use super::*;
    use crate::descriptor::CapabilityKind;

    const LOAD_METRIC: &str = "inference_pool_average_queue_size";

    fn cand(model: &str, site: &str, cluster: &str) -> CandidateConfig {
        CandidateConfig {
            cluster: cluster.to_owned(),
            credential: None,
            fresh: true,
            kind: CapabilityKind::InferenceModel,
            name: model.to_owned(),
            site: site.to_owned(),
        }
    }

    fn line(site: &str, cluster: &str, value: f64, at_ms: i64) -> String {
        format!(r#"{LOAD_METRIC}{{grid_site="{site}",grid_provider="{cluster}"}} {value} {at_ms}"#)
    }

    /// A controllable clock, the shared snapshot, and the refresh closure over
    /// them, wired for the load-change test.
    type Wired = (Arc<AtomicI64>, Arc<ArcSwap<RouteSnapshot>>, Box<dyn Fn(&LoadStore)>);

    /// Build the shared snapshot, a test clock, and the refresh closure that
    /// re-orders the two-site topology over that clock.
    fn wired_refresh() -> Wired {
        let base: Arc<[RouteCandidate]> = Arc::from(
            validate_candidates(vec![cand("llama", "east", "pool-a"), cand("llama", "west", "pool-b")])
                .expect("candidates"),
        );
        let local_site: Arc<str> = Arc::from("local");
        let snapshot = Arc::new(ArcSwap::from_pointee(RouteSnapshot::from_static(
            base.iter().cloned().collect(),
            Arc::clone(&local_site),
        )));
        let clock = Arc::new(AtomicI64::new(1_000));
        let refresh = {
            let clock = Arc::clone(&clock);
            make_refresh(base, local_site, Arc::clone(&snapshot), 30_000, move || {
                clock.load(Ordering::SeqCst)
            })
        };
        (clock, snapshot, Box::new(refresh))
    }

    /// The front site the current snapshot would route to.
    fn front(snapshot: &ArcSwap<RouteSnapshot>) -> Arc<str> {
        Arc::clone(&snapshot.load().candidates[0].site)
    }

    #[test]
    fn a_load_change_reorders_the_snapshot_without_a_config_rebuild() {
        let (clock, snapshot, refresh) = wired_refresh();
        let store = LoadStore::new(Duration::from_secs(600));

        // East busy, west idle. One poll cycle orders west first.
        store.ingest_at(&line("east", "pool-a", 90.0, 1_000), 1_000, 1_000, "east");
        store.ingest_at(&line("west", "pool-b", 10.0, 1_000), 1_000, 1_000, "west");
        refresh(&store);
        assert_eq!(&*front(&snapshot), "west", "the idle site sorts first");

        // The load flips at a later time, past the window of the first samples.
        clock.store(40_000, Ordering::SeqCst);
        store.ingest_at(&line("east", "pool-a", 5.0, 40_000), 40_000, 40_000, "east");
        store.ingest_at(&line("west", "pool-b", 95.0, 40_000), 40_000, 40_000, "west");
        refresh(&store);
        assert_eq!(
            &*front(&snapshot),
            "east",
            "the swap tracks the load change with no config rebuild"
        );
    }

    #[test]
    fn a_serving_config_parses() {
        let yaml = "\
local_site: local
window_secs: 60
load_window_ms: 30000
candidates:
  - kind: inference_model
    name: llama
    site: east
    cluster: pool-a
peers:
  - site: east
    addr: 10.0.0.1:8443
    server_name: east.grid.internal
    authority: east.grid.internal
    grid_ca_path: /etc/grid/ca.pem
    client_cert_path: /etc/grid/tls.crt
    client_key_path: /etc/grid/tls.key
";
        let config: GridServingConfig = serde_yaml::from_str(yaml).expect("serving config parses");
        assert_eq!(config.peers.len(), 1);
        assert_eq!(config.peers[0].path, "/v1/site/signals", "the path default applies");
        assert_eq!(config.candidates.len(), 1);
    }

    fn valid_peer() -> PeerServingConfig {
        PeerServingConfig {
            site: "east".to_owned(),
            addr: "10.0.0.1:8443".to_owned(),
            server_name: "east.grid.internal".to_owned(),
            authority: "east.grid.internal".to_owned(),
            path: default_path(),
            interval_ms: 2_000,
            connect_timeout_ms: 2_000,
            request_timeout_ms: 2_000,
            grid_ca_path: "/etc/grid/ca.pem".to_owned(),
            client_cert_path: "/etc/grid/tls.crt".to_owned(),
            client_key_path: "/etc/grid/tls.key".to_owned(),
        }
    }

    #[test]
    fn a_zero_interval_serving_config_is_rejected_at_load() {
        let yaml = "\
local_site: local
window_secs: 60
load_window_ms: 30000
candidates:
  - kind: inference_model
    name: llama
    site: east
    cluster: pool-a
peers:
  - site: east
    addr: 10.0.0.1:8443
    server_name: east.grid.internal
    authority: east.grid.internal
    interval_ms: 0
    grid_ca_path: /etc/grid/ca.pem
    client_cert_path: /etc/grid/tls.crt
    client_key_path: /etc/grid/tls.key
";
        let err = serde_yaml::from_str::<GridServingConfig>(yaml).expect_err("interval_ms: 0 must be rejected at load");
        assert!(err.to_string().contains("greater than zero"), "{err}");
    }

    #[test]
    fn validate_peer_rejects_a_zero_interval_or_timeout() {
        let mut zero_interval = valid_peer();
        zero_interval.interval_ms = 0;
        validate_peer(&zero_interval).expect_err("a zero interval is refused");

        let mut zero_connect = valid_peer();
        zero_connect.connect_timeout_ms = 0;
        validate_peer(&zero_connect).expect_err("a zero connect timeout is refused");

        let mut zero_request = valid_peer();
        zero_request.request_timeout_ms = 0;
        validate_peer(&zero_request).expect_err("a zero request timeout is refused");

        validate_peer(&valid_peer()).expect("a valid peer is accepted");
    }
}
