//! The gateway control plane: poll peers, order candidates, swap the snapshot.
//!
//! The operator writes a grid serving config (topology + peers). The gateway
//! reads it, builds the load store and a cold-start snapshot, and spawns one
//! poller per peer. Each poll cycle re-orders the candidate set by live load and
//! swaps the shared snapshot, so the data-plane filter only ever reads a
//! resolved order. A watch on the config file applies the operator's rewrites.

use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use arc_swap::ArcSwap;
use certs::spiffe_id;
use grid_signals_client::{PeerScraper, spawn_on_thread_held};
use praxis_filter::FilterError;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _};
use serde::{Deserialize, Serialize};

use crate::{
    control::{Control, ReloadOutcome, Tuning, Watcher, watch},
    descriptor::CandidateConfig,
    health::ClusterHealth,
    prefix::PrefixAffinity,
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
#[derive(Clone, Debug, Deserialize, PartialEq)]
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

/// Site availability tuning: how a site's ceiling is learned, how its saturation is
/// smoothed, how much an unproven site is explored, and when a model sheds.
///
/// Set under `availability` in the `grid_site_route` filter block, not in the serving config: the
/// serving config is routing data the operator writes, and plugin tuning belongs to the
/// plugin's own config. Defaults are what the lab runs used; each is documented in
/// `docs/routing.md`.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AvailabilitySettings {
    /// Weight of the newest in-flight sample against the running saturation estimate, in (0, 1].
    pub smoothing: f64,
    /// How long a learned ceiling takes to halve once load falls away, milliseconds, over 0.
    pub ceiling_half_life_ms: i64,
    /// The least a learned ceiling can be, so a quiet site does not read as full, at least 1.
    pub ceiling_floor: f64,
    /// A measured site's weight is at least this share of the largest ceiling, in [0, 1].
    pub explore_floor: f64,
    /// How long a site must stay at its ceiling with work waiting before it counts as full,
    /// milliseconds, at least 0. A debounce in time, so a site polled every 500 ms and one
    /// polled every 5 s read the same.
    pub full_after_ms: i64,
    /// How long a site must stay below its ceiling before a shedding model routes again,
    /// milliseconds, at least 0. An emptied queue is room at once.
    pub room_after_ms: i64,
    /// Queued work per serving unit at which a site counts as full, at least 0. Fullness only:
    /// a sample stops teaching the ceiling as soon as one whole request waits anywhere.
    pub queue_full: f64,
    /// Whether a model whose every site is full answers 429 rather than queueing deeper. Off
    /// unless every client of the grid retries on 429 with Retry-After, and off where the EPP's
    /// flow control is on, so one overload is not shed twice.
    pub shedding: bool,
}

impl Default for AvailabilitySettings {
    fn default() -> Self {
        Self {
            smoothing: 0.3,
            ceiling_half_life_ms: 600_000,
            ceiling_floor: 8.0,
            explore_floor: 0.25,
            full_after_ms: 5_000,
            room_after_ms: 2_000,
            queue_full: 1.0,
            shedding: false,
        }
    }
}

impl AvailabilitySettings {
    /// Refuse a block whose values selection cannot use.
    ///
    /// # Errors
    ///
    /// Names the first field outside its range.
    pub fn validate(&self) -> Result<(), String> {
        unit("smoothing", self.smoothing, f64::EPSILON)?;
        unit("explore_floor", self.explore_floor, 0.0)?;
        for (name, value) in [
            ("full_after_ms", self.full_after_ms),
            ("room_after_ms", self.room_after_ms),
        ] {
            if value < 0 {
                return Err(format!("availability.{name} must be at least 0, got {value}"));
            }
        }
        if self.ceiling_half_life_ms <= 0 {
            return Err(format!(
                "availability.ceiling_half_life_ms must be over 0, got {}",
                self.ceiling_half_life_ms
            ));
        }
        if !self.ceiling_floor.is_finite() || self.ceiling_floor < 1.0 {
            return Err(format!(
                "availability.ceiling_floor must be at least 1, got {}",
                self.ceiling_floor
            ));
        }
        if !self.queue_full.is_finite() || self.queue_full < 0.0 {
            return Err(format!(
                "availability.queue_full must be at least 0, got {}",
                self.queue_full
            ));
        }
        Ok(())
    }
}

/// An availability setting that must be a finite fraction in `[lo, 1]`.
fn unit(name: &str, value: f64, lo: f64) -> Result<(), String> {
    if !value.is_finite() || value < lo || value > 1.0 {
        return Err(format!("availability.{name} must be in [{lo}, 1], got {value}"));
    }
    Ok(())
}

/// One peer this gateway polls, with the mTLS material to reach it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
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

    /// Leaf SHA-256 digests the peer must also match, rendered under pin trust only.
    #[serde(default)]
    pub pins: Vec<String>,

    /// `host:port` of the peer's gateway, routed to directly; set only while the operator verified it.
    #[serde(default)]
    pub gateway: Option<String>,
}

/// The running control plane: the filter's snapshot, the pollers, and the config watch.
pub struct GridRuntime {
    /// The snapshot the refreshes swap and the filter reads.
    snapshot: Arc<ArcSwap<RouteSnapshot>>,

    /// The topology, store, and pollers. Dropping it stops every poller.
    control: Arc<Mutex<Control>>,

    /// The config file watch, stopped on drop.
    watcher: Option<Watcher>,

    /// The prefix index and affinity settings the route filter reads.
    affinity: Arc<PrefixAffinity>,
    /// Backend cluster health the route filter publishes and the tick reads.
    health: Arc<ClusterHealth>,

    /// The filter's tuning, set when the filter is built and read by the control step.
    tuning: Arc<Tuning>,

    /// Stops the health tick when the runtime drops.
    _health_tick: std::sync::mpsc::Sender<()>,
}

impl GridRuntime {
    /// The prefix index and affinity settings to register the filter over.
    #[must_use]
    pub fn affinity(&self) -> Arc<PrefixAffinity> {
        Arc::clone(&self.affinity)
    }

    /// The filter tuning to register the filter over.
    #[must_use]
    pub fn tuning(&self) -> Arc<Tuning> {
        Arc::clone(&self.tuning)
    }

    /// The shared snapshot to register the filter over.
    #[must_use]
    pub fn snapshot(&self) -> Arc<ArcSwap<RouteSnapshot>> {
        Arc::clone(&self.snapshot)
    }

    /// The cluster health to register the filter over.
    #[must_use]
    pub fn health(&self) -> Arc<ClusterHealth> {
        Arc::clone(&self.health)
    }

    /// Apply a new serving config. `None` when it is already applied.
    ///
    /// # Errors
    ///
    /// Returns [`FilterError`] if the config is invalid or a poller cannot start, keeping the last good one.
    pub fn reload(&self, config: &GridServingConfig) -> Result<Option<ReloadOutcome>, FilterError> {
        self.control
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .apply(config)
    }

    /// Re-read `path` every `every` and apply it when it changes.
    ///
    /// # Errors
    ///
    /// Returns the error if the watch thread cannot be spawned.
    pub fn watch<P: Into<PathBuf>>(&mut self, path: P, every: Duration) -> std::io::Result<()> {
        self.watcher = Some(watch(Arc::clone(&self.control), path.into(), every)?);
        Ok(())
    }

    /// The running watch, for tests that wait on its counts.
    #[cfg(test)]
    pub(crate) fn watcher(&self) -> Option<&Watcher> {
        self.watcher.as_ref()
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
/// one poller per peer whose refresh re-orders and swaps the snapshot.
///
/// # Errors
///
/// Returns [`FilterError`] if the local site or candidate topology is invalid, a
/// peer's certificate material cannot be read or parsed, or a poller thread
/// cannot be spawned.
pub fn spawn_grid_routing(config: &GridServingConfig) -> Result<GridRuntime, FilterError> {
    // The peer scrapers load TLS here, before the server installs the provider.
    praxis_tls::provider::install();
    let start = Box::new(|peer: &PeerServingConfig, poller: &_, store, refresh| {
        let scraper = build_scraper(peer)?;
        spawn_on_thread_held(store, poller, scraper, refresh)
            .map_err(|error| -> FilterError { format!("grid: spawning poller for {}: {error}", peer.site).into() })
    });
    start_runtime(config, start)
}

/// Build the runtime over `start`, the peer poller constructor.
pub(crate) fn start_runtime(
    config: &GridServingConfig,
    start: crate::control::StartPeer,
) -> Result<GridRuntime, FilterError> {
    let mut control = Control::new(config, start)?;
    control.apply(config)?;
    let health_tick = tick_health(control.health(), control.refresh(), control.store())
        .map_err(|error| -> FilterError { format!("grid: spawning the health tick: {error}").into() })?;
    let (snapshot, affinity, health, tuning) = (
        control.snapshot(),
        control.affinity(),
        control.health(),
        control.tuning(),
    );
    let control = Arc::new(Mutex::new(control));
    // A filter built from here on adopts its tuning at once, not on the next re-read.
    tuning.attach(Arc::downgrade(&control));
    Ok(GridRuntime {
        snapshot,
        affinity,
        health,
        tuning,
        control,
        watcher: None,
        _health_tick: health_tick,
    })
}

/// How often the control step re-reads backend cluster health.
const HEALTH_TICK: Duration = Duration::from_millis(100);

/// Re-order the snapshot whenever the set of clusters with no healthy endpoint changes.
///
/// Health moves faster than the poll cycle, so it gets its own tick; the returned
/// sender stops the thread when dropped.
fn tick_health(
    health: Arc<ClusterHealth>,
    refresh: crate::control::Refresh,
    store: Arc<grid_signals::LoadStore>,
) -> std::io::Result<std::sync::mpsc::Sender<()>> {
    let (stop, stopped) = std::sync::mpsc::channel::<()>();
    std::thread::Builder::new()
        .name("grid-health".to_owned())
        .spawn(move || {
            while let Err(std::sync::mpsc::RecvTimeoutError::Timeout) = stopped.recv_timeout(HEALTH_TICK) {
                if health.update() {
                    refresh(&store);
                }
            }
        })?;
    Ok(stop)
}

/// A zero interval hands `Duration::ZERO` to the interval timer, which panics the
/// detached poller thread. A zero timeout fires immediately, so the peer never
/// scrapes. Either way that site ages to `+inf` and sorts last, a silent stale
/// misroute. The serde guard on `interval_ms` catches a parsed config, but a
/// struct literal can bypass serde, so admission checks here too.
///
/// # Errors
///
/// Returns [`FilterError`] if the interval or either timeout is zero.
pub(crate) fn validate_peer(peer: &PeerServingConfig) -> Result<(), FilterError> {
    if peer.interval_ms == 0 {
        return Err(format!("grid: peer {} interval_ms must be greater than zero", peer.site).into());
    }
    if peer.connect_timeout_ms == 0 || peer.request_timeout_ms == 0 {
        return Err(format!("grid: peer {} timeouts must be greater than zero", peer.site).into());
    }
    ServerName::try_from(peer.server_name.as_str()).map_err(|error| -> FilterError {
        format!("grid: peer {} server_name {}: {error}", peer.site, peer.server_name).into()
    })?;
    // A SHA-256 leaf digest, colons allowed.
    let hex_digest = |pin: &String| {
        let digits: Vec<char> = pin.chars().filter(|ch| *ch != ':').collect();
        digits.len() == 64 && digits.iter().all(char::is_ascii_hexdigit)
    };
    if let Some(pin) = peer.pins.iter().find(|pin| !hex_digest(pin)) {
        return Err(format!("grid: peer {} pin {pin} is not a SHA-256 hex digest", peer.site).into());
    }
    Ok(())
}

/// Grid mTLS to `peer`: this site's identity, verified against the grid CA and the peer's server name.
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
    .map(|scraper| scraper.with_pins(&peer.pins))
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
    use super::*;
    use crate::descriptor::{validate_candidates, validate_local_site};

    #[test]
    fn the_documented_example_loads() {
        let json = include_str!("../../../examples/gateway/serving-config.json");
        let config: GridServingConfig = serde_json::from_str(json).expect("the example parses");
        assert_eq!(config.candidates.len(), 3);
        assert_eq!(config.peers[0].interval_ms, 500, "the local operator is polled fast");
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

    #[test]
    fn the_operator_rendered_config_parses_and_validates() {
        // Rendered by the operator's serving_config golden test.
        let json = include_str!("../testdata/serving-config.json");
        let config: GridServingConfig = serde_yaml::from_str(json).expect("operator output parses");
        validate_local_site(&config.local_site).expect("local site");
        validate_candidates(config.candidates).expect("candidates");
        for peer in &config.peers {
            validate_peer(peer).expect("peer");
            ServerName::try_from(peer.server_name.clone()).expect("server name");
        }
    }

    #[test]
    fn declared_pins_parse_and_default_to_none() {
        let peer = |extra: &str| {
            format!(
                "site: east\naddr: 10.0.0.1:9091\nserver_name: east.grid.internal\nauthority: east.grid.internal\n\
                 grid_ca_path: /ca\nclient_cert_path: /crt\nclient_key_path: /key\n{extra}"
            )
        };
        let pinned: PeerServingConfig = serde_yaml::from_str(&peer("pins: [abcd]\n")).expect("pinned parses");
        assert_eq!(pinned.pins, ["abcd"]);
        let bare: PeerServingConfig = serde_yaml::from_str(&peer("")).expect("bare parses");
        assert!(bare.pins.is_empty(), "SPIFFE only without pins");
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
            pins: Vec::new(),
            gateway: None,
        }
    }

    #[test]
    fn a_peer_with_a_bad_server_name_or_pin_is_refused_before_any_poller_starts() {
        let mut bad_name = valid_peer();
        bad_name.server_name = "not a name".to_owned();
        assert!(
            validate_peer(&bad_name).is_err(),
            "server_name is checked from the config alone"
        );

        let mut short_pin = valid_peer();
        short_pin.pins = vec!["ab:cd".to_owned()];
        assert!(validate_peer(&short_pin).is_err(), "a truncated pin");

        let mut pinned = valid_peer();
        pinned.pins = vec!["AB".repeat(32), format!("{}ab", "ab:".repeat(31))];
        assert!(
            validate_peer(&pinned).is_ok(),
            "hex digests with or without colons, any case"
        );
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

    #[test]
    fn a_peer_from_an_operator_without_gateway_support_parses_with_none() {
        let peer: PeerServingConfig = serde_yaml::from_str(
            "{site: east, addr: '10.0.0.1:9091', server_name: east.grid.internal, authority: east.grid.internal, \
             grid_ca_path: /ca, client_cert_path: /crt, client_key_path: /key}",
        )
        .expect("an old operator's peer parses");
        assert!(peer.gateway.is_none(), "no gateway: route through the static cluster");
    }
}
