//! The gateway control plane: poll peers, order candidates, swap the snapshot.
//!
//! The operator writes a grid serving config (topology + peers). The gateway
//! reads it, builds the load store and a cold-start snapshot, and spawns one
//! poller per peer. Each poll cycle re-orders the candidate set by live load and
//! swaps the shared snapshot, so the data-plane filter only ever reads a
//! resolved order. A watch on the config file applies the operator's rewrites.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use arc_swap::ArcSwap;
use certs::spiffe_id;
use grid_client::{PeerScraper, Reporter, spawn_on_thread_held};
use praxis_filter::FilterError;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _};
use serde::{Deserialize, Serialize};

use crate::{
    control::{Binding, Control, ReloadOutcome, Tuning, Watcher, watch},
    descriptor::CandidateConfig,
    health::ClusterHealth,
    prefix::PrefixAffinity,
    snapshot::RouteSnapshot,
};

/// The dial file's name, beside the serving config in the same mount.
pub const DIAL_FILE: &str = "dial.json";

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

    /// Freshness window: a site whose newest sample is older is unmeasured, milliseconds.
    pub load_window_ms: i64,

    /// How far behind a site's newest sample the router reads worst load, milliseconds.
    /// Absent, the freshness window.
    #[serde(default)]
    pub horizon_ms: Option<i64>,

    /// The candidate topology: which sites serve which capabilities.
    pub candidates: Vec<CandidateConfig>,

    /// Clusters allowed to carry authenticated provider-gateway hops.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_hop_clusters: Vec<String>,

    /// TLS SNI declared for each authenticated provider hop.
    #[serde(default)]
    pub provider_hop_sni: BTreeMap<String, String>,

    /// Peers to poll for live load.
    pub peers: Vec<PeerServingConfig>,
}

/// The dial file the operator writes beside the serving config: how the gateway dials each
/// candidate cluster it builds an upstream for. A gateway that predates it never opens it.
///
/// A cluster it names is dialed directly, replacing any praxis `load_balancer` cluster of the
/// same name; any other cluster routes through praxis as before. A missing file names none.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DialConfig {
    /// Dials by candidate cluster.
    #[serde(default)]
    pub dial: BTreeMap<String, DialSpec>,

    /// The schema version, so a later one is refused rather than misread.
    #[serde(default)]
    pub version: DialVersion,
}

/// The dial file schema versions this gateway reads.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum DialVersion {
    /// The first schema.
    #[default]
    #[serde(rename = "v1")]
    V1,
}

/// One cluster's upstream, built by the gateway from paths only; certificate verification is always on.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DialSpec {
    /// `host:port` dialed. A retry reconnects to the same address.
    pub address: String,

    /// The path every request is sent under, from the provider's endpoint URL.
    #[serde(default)]
    pub base_path: Option<String>,

    /// Connect retries on a connect failure, to the same address.
    #[serde(default)]
    pub connect_retries: Option<u32>,

    /// The token a local dial sends in place of the caller's credential.
    #[serde(default)]
    pub credential: Option<DialCredential>,

    /// The connect timeout before TLS, milliseconds.
    #[serde(default)]
    pub connect_timeout_ms: Option<u64>,

    /// The per-read timeout, milliseconds.
    #[serde(default)]
    pub read_timeout_ms: Option<u64>,

    /// The site serving the cluster; every candidate on the cluster names it.
    pub site: String,

    /// The connect timeout including the TLS handshake, milliseconds.
    #[serde(default)]
    pub total_connect_timeout_ms: Option<u64>,

    /// How the connection is secured.
    pub transport: DialTransport,

    /// Whether the address host may resolve to a private range. Only for this site's own hostnames.
    #[serde(default)]
    pub trusted_private: bool,

    /// The per-write timeout, milliseconds.
    #[serde(default)]
    pub write_timeout_ms: Option<u64>,
}

/// The credential a local dial presents, read from a mounted file.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DialCredential {
    /// The file holding the token, under the credential mount.
    pub path: String,

    /// How the token is sent.
    pub strategy: CredentialStrategy,
}

/// How a dial's token is sent.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CredentialStrategy {
    /// `Authorization: Bearer <token>`.
    BearerToken,
}

/// How a dial is secured.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "mode")]
pub enum DialTransport {
    /// Grid mutual TLS with this site's identity, verified against the grid CA, both read from
    /// the identity the gateway fixed at startup.
    MutualTls {
        /// The peer's site name, `{site}.grid.internal`.
        sni: String,
    },
    /// Server-only TLS.
    Tls {
        /// The CA bundle the server is verified with; absent, the system roots.
        #[serde(default)]
        ca_path: Option<String>,
        /// The name the server is verified by.
        sni: String,
    },
    /// No TLS. Only to this site.
    Plaintext,
}

impl DialTransport {
    /// The verified server name, `None` for plaintext.
    #[must_use]
    pub fn sni(&self) -> Option<&str> {
        match self {
            Self::MutualTls { sni, .. } | Self::Tls { sni, .. } => Some(sni),
            Self::Plaintext => None,
        }
    }

    /// The CA file a server-only TLS dial names; the grid identity is the gateway's own.
    pub(crate) fn ca_path(&self) -> Option<&str> {
        match self {
            Self::Tls { ca_path, .. } => ca_path.as_deref(),
            Self::MutualTls { .. } | Self::Plaintext => None,
        }
    }
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
    /// Weight of the newest saturation reading against the running estimate, in (0, 1].
    pub smoothing: f64,
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
    /// Queued work per serving unit that makes a sample backlogged, at least 0. A sustained
    /// backlog can teach the hard capacity bound; any whole request waiting stops that sample
    /// from teaching the soft bound.
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
        deserialize_with = "grid_client::deserialize_interval_ms"
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

    /// Re-read `path`, and the dial file beside it, every `every` and apply them when either changes.
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

/// The dial file beside the serving config at `serving`.
#[must_use]
pub fn dial_config_path(serving: &Path) -> PathBuf {
    serving.with_file_name(DIAL_FILE)
}

/// The revision a kubelet volume at `dir` points `..data` at now, else `dir` itself.
///
/// Reading every file under the one revision keeps a swap from pairing two revisions.
pub(crate) fn mount_revision(dir: &Path) -> PathBuf {
    fs::canonicalize(dir.join("..data")).unwrap_or_else(|_| dir.to_path_buf())
}

/// Read and parse the dial file at `path`; a missing file names no dial.
///
/// # Errors
///
/// Returns [`FilterError`] if the file exists but cannot be read or does not parse.
pub fn load_dial_config(path: &Path) -> Result<DialConfig, FilterError> {
    match fs::read(path) {
        Ok(bytes) => parse_dial_config(path, &bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(DialConfig::default()),
        Err(error) => Err(format!("grid: reading {}: {error}", path.display()).into()),
    }
}

/// Parse the dial file at `path` from `bytes`.
pub(crate) fn parse_dial_config(path: &Path, bytes: &[u8]) -> Result<DialConfig, FilterError> {
    serde_yaml::from_slice(bytes)
        .map_err(|error| -> FilterError { format!("grid: parsing {}: {error}", path.display()).into() })
}

/// Build the control plane from `config`: the store, the cold-start snapshot, and
/// one poller per peer whose refresh re-orders and swaps the snapshot.
///
/// # Errors
///
/// Returns [`FilterError`] if the local site or candidate topology is invalid, a
/// peer's certificate material cannot be read or parsed, or a poller thread
/// cannot be spawned. A dial file that breaks a trust rule is set aside with a warning.
pub fn spawn_grid_routing(
    config: &GridServingConfig,
    dial: &DialConfig,
    binding: Binding,
) -> Result<GridRuntime, FilterError> {
    // The peer scrapers load TLS here, before the server installs the provider.
    praxis_tls::provider::install();
    let reporter = Arc::new(
        Reporter::new(spiffe_id(&config.local_site))
            .map_err(|error| -> FilterError { format!("grid: building ingest reporter: {error}").into() })?,
    );
    let start = Box::new(move |peer: &PeerServingConfig, poller: &_, store, refresh| {
        let scraper = build_scraper(peer)?.with_reporter(Arc::clone(&reporter));
        spawn_on_thread_held(store, poller, scraper, refresh)
            .map_err(|error| -> FilterError { format!("grid: spawning poller for {}: {error}", peer.site).into() })
    });
    start_runtime_with(config, dial, start, binding)
}

/// Build the runtime over `start`, the peer poller constructor.
#[cfg(test)]
pub(crate) fn start_runtime(
    config: &GridServingConfig,
    start: crate::control::StartPeer,
) -> Result<GridRuntime, FilterError> {
    start_runtime_with(config, &DialConfig::default(), start, Binding::default())
}

/// Build the runtime with what the loaded praxis config fixed at startup.
pub(crate) fn start_runtime_with(
    config: &GridServingConfig,
    dial: &DialConfig,
    start: crate::control::StartPeer,
    binding: Binding,
) -> Result<GridRuntime, FilterError> {
    let mut control = Control::new_with(config, start, binding)?;
    if let Err(error) = control.apply_joined(config, dial) {
        if dial.dial.is_empty() {
            return Err(error);
        }
        // A bad dial file never stops the gateway: it serves without dials until a good one lands.
        tracing::warn!(%error, "grid: dial file refused at startup; serving without dials");
        control.apply_joined(config, &DialConfig::default())?;
    }
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
        let json = include_str!("../../../../../examples/gateway/serving-config.json");
        let config: GridServingConfig = serde_json::from_str(json).expect("the example parses");
        assert_eq!(config.candidates.len(), 2, "one model at each of two sites");
        assert_eq!(config.peers.len(), 1, "the gateway polls only its local operator");
        assert_eq!(
            config.peers[0].interval_ms, 5_000,
            "at the interval the operator renders for its gateway"
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
    fn a_rendered_pin_is_refused_rather_than_ignored() {
        let peer = |extra: &str| {
            format!(
                "site: east\naddr: 10.0.0.1:9091\nserver_name: east.grid.internal\nauthority: east.grid.internal\n\
                 grid_ca_path: /ca\nclient_cert_path: /crt\nclient_key_path: /key\n{extra}"
            )
        };
        serde_yaml::from_str::<PeerServingConfig>(&peer("")).expect("bare parses");
        assert!(
            serde_yaml::from_str::<PeerServingConfig>(&peer("pins: [abcd]\n")).is_err(),
            "an operator still rendering pins fails the load, not silently"
        );
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
            gateway: None,
        }
    }

    #[test]
    fn a_peer_with_a_bad_server_name_is_refused_before_any_poller_starts() {
        let mut bad_name = valid_peer();
        bad_name.server_name = "not a name".to_owned();
        assert!(
            validate_peer(&bad_name).is_err(),
            "server_name is checked from the config alone"
        );
        validate_peer(&valid_peer()).expect("a well-formed peer passes");
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
