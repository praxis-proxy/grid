//! The gateway control plane: poll peers, order candidates, swap the snapshot.
//!
//! The operator writes a grid serving config (topology + peers). The gateway
//! reads it, builds the load store and a cold-start snapshot, and spawns one
//! poller per peer. Each poll cycle re-orders the candidate set by live load and
//! swaps the shared snapshot, on one clock, so the data-plane filter only ever
//! reads a resolved order. This is the control side of the control/data split:
//! the poller and the refresh loop live here, not in the filter.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use arc_swap::ArcSwap;
use certs::spiffe_id;
use grid_signals::{LoadStore, now_ms};
use grid_signals_client::{PeerScraper, PollHandle, PollerConfig, spawn_on_thread};
use praxis_filter::FilterError;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

use crate::{
    descriptor::{
        CandidateConfig, RouteCandidate, validate_candidates, validate_local_site, validate_provider_hop_clusters,
        validate_serving_candidates,
    },
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

    /// Explicit mTLS clusters that authenticate provider-hop context.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_hop_clusters: Vec<String>,

    /// TLS SNI that the operator declares for each authenticated provider hop.
    #[serde(default)]
    pub provider_hop_sni: BTreeMap<String, String>,

    /// Peers to poll for live load.
    pub peers: Vec<PeerServingConfig>,
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
}

/// The running control plane: the shared snapshot the filter reads and the
/// pollers that keep it fresh.
pub struct GridRuntime {
    /// Shared routing snapshot, current candidate set, and poller ownership.
    shared: Arc<RuntimeShared>,

    /// Config used to create the initial snapshot; the watcher compares its
    /// first file read against this to avoid baselining unapplied changes.
    startup_config: GridServingConfig,

    /// Stops the projected `ConfigMap` watcher on drop.
    watcher_stop: Arc<AtomicBool>,

    /// Watcher thread. Joining it ensures no config update outlives the runtime.
    watcher: Option<JoinHandle<()>>,
}

/// Request-routing inputs atomically replaced on a valid serving revision.
struct RefreshConfig {
    /// Validated candidates currently published to the route snapshot.
    candidates: Arc<[RouteCandidate]>,
    /// Site used to interpret relative provider locality.
    local_site: Arc<str>,
    /// Freshness window for provider load samples.
    load_window_ms: i64,
    /// Clusters allowed to receive authenticated provider-hop requests.
    provider_hop_clusters: Arc<BTreeSet<String>>,
}

/// State shared by the request filter, metric pollers, and file watcher.
struct RuntimeShared {
    /// Current routing inputs atomically replaced by valid serving revisions.
    base: ArcSwap<RefreshConfig>,
    /// Snapshot observed by request filters.
    snapshot: Arc<ArcSwap<RouteSnapshot>>,
    /// Recent provider load measurements.
    store: Arc<LoadStore>,
    /// Active peer polling threads.
    pollers: Mutex<Vec<PollHandle>>,
    /// Serializes periodic poll refreshes with serving-config replacements.
    /// Without this gate, a refresh that captured the previous candidate set
    /// could publish after a withdrawal and resurrect its routes.
    refresh_gate: Mutex<()>,
    /// TLS identities from the Praxis load balancer loaded at startup.
    backend_tls: BTreeMap<String, String>,
}

impl GridRuntime {
    /// The shared snapshot to register the filter over.
    #[must_use]
    pub fn snapshot(&self) -> Arc<ArcSwap<RouteSnapshot>> {
        Arc::clone(&self.shared.snapshot)
    }

    /// Watch the projected serving `ConfigMap` and apply valid candidate
    /// revisions without restarting the gateway. Malformed replacements retain
    /// the current serving snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`FilterError`] if the initial config cannot be read or parsed,
    /// or if the watcher thread cannot be started.
    #[expect(
        clippy::too_many_lines,
        reason = "initial apply, watcher startup, and last-known-good loop are one lifecycle operation"
    )]
    pub fn watch_config(&mut self, path: &str) -> Result<(), FilterError> {
        let path = path.to_owned();
        let initial = fs::read_to_string(&path)
            .map_err(|error| -> FilterError { format!("grid: reading {path}: {error}").into() })?;
        let next: GridServingConfig = serde_yaml::from_str(&initial)
            .map_err(|error| -> FilterError { format!("grid: parsing {path}: {error}").into() })?;
        let mut current = self.startup_config.clone();
        apply_serving_revision(&self.shared, &mut current, next, &initial)?;
        tracing::info!(
            revision = %format!("{:x}", Sha256::digest(initial.as_bytes())),
            candidate_count = current.candidates.len(),
            "grid_serving_revision_serving"
        );

        let shared = Arc::clone(&self.shared);
        let stop = Arc::clone(&self.watcher_stop);
        let thread_path = path;
        self.watcher = Some(
            thread::Builder::new()
                .name("grid-serving-config-watch".to_owned())
                .spawn(move || {
                    let mut applied = initial;
                    let mut parse_rejected: Option<String> = None;
                    let mut apply_failed: Option<String> = None;
                    let mut retry_delay = Duration::from_millis(CONFIG_POLL_INTERVAL_MS);
                    while !stop.load(Ordering::Acquire) {
                        thread::park_timeout(retry_delay);
                        if stop.load(Ordering::Acquire) {
                            break;
                        }
                        let observed_text = match fs::read_to_string(&thread_path) {
                            Ok(text) => text,
                            Err(error) => {
                                tracing::warn!(path = %thread_path, %error, "grid_serving_config_read_failed: retaining last-known-good");
                                continue;
                            },
                        };
                        if observed_text == applied || parse_rejected.as_deref() == Some(observed_text.as_str()) {
                            retry_delay = Duration::from_millis(CONFIG_POLL_INTERVAL_MS);
                            continue;
                        }
                        let parsed = match serde_yaml::from_str::<GridServingConfig>(&observed_text) {
                            Ok(config) => config,
                            Err(error) => {
                                tracing::warn!(path = %thread_path, %error, "grid_serving_config_rejected: retaining last-known-good");
                                parse_rejected = Some(observed_text);
                                retry_delay = Duration::from_millis(CONFIG_POLL_INTERVAL_MS);
                                continue;
                            },
                        };
                        match apply_serving_revision(&shared, &mut current, parsed, &observed_text) {
                            Ok(()) => {
                                applied = observed_text;
                                parse_rejected = None;
                                apply_failed = None;
                                retry_delay = Duration::from_millis(CONFIG_POLL_INTERVAL_MS);
                            },
                            Err(error) => {
                                if apply_failed.as_deref() != Some(observed_text.as_str()) {
                                    tracing::warn!(
                                        path = %thread_path,
                                        %error,
                                        "grid_serving_config_rejected: retaining last-known-good"
                                    );
                                }
                                apply_failed = Some(observed_text);
                                retry_delay = Duration::from_secs(1);
                            },
                        }
                    }
                })
                .map_err(|error| -> FilterError { format!("grid: starting serving-config watcher: {error}").into() })?,
        );
        Ok(())
    }
}

impl Drop for GridRuntime {
    fn drop(&mut self) {
        self.watcher_stop.store(true, Ordering::Release);
        if let Some(watcher) = self.watcher.take()
            && watcher.join().is_err()
        {
            tracing::warn!("grid_serving_config_watcher_join_failed");
        }
    }
}

/// Poll rate for Kubernetes projected-volume updates.
const CONFIG_POLL_INTERVAL_MS: u64 = 250;

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

/// An operator-declared hop is trusted only when the loaded upstream uses the
/// same verified TLS identity. Empty snapshots have no authorized hops.
fn validate_provider_hop_binding(
    config: &GridServingConfig,
    backend_tls: &BTreeMap<String, String>,
) -> Result<(), FilterError> {
    let declared: BTreeSet<&str> = config.provider_hop_clusters.iter().map(String::as_str).collect();
    let identities: BTreeSet<&str> = config.provider_hop_sni.keys().map(String::as_str).collect();
    if declared != identities {
        return Err("grid: provider-hop clusters and TLS identities differ".into());
    }
    for (cluster, expected_sni) in &config.provider_hop_sni {
        if expected_sni.trim().is_empty() || backend_tls.get(cluster) != Some(expected_sni) {
            return Err(
                format!("grid: provider-hop cluster {cluster:?} does not match a verified mTLS backend").into(),
            );
        }
    }
    Ok(())
}

/// Build the control plane from `config`: the store, the cold-start snapshot, and
/// one poller per peer whose refresh closure re-orders and swaps the snapshot.
///
/// # Errors
///
/// Returns [`FilterError`] if the local site or candidate topology is invalid, a
/// peer's certificate material cannot be read or parsed, or a poller thread
/// cannot be spawned.
#[expect(
    clippy::too_many_lines,
    reason = "validates the initial config and publishes its initial immutable routing snapshot"
)]
pub fn spawn_grid_routing(
    config: &GridServingConfig,
    backend_tls: BTreeMap<String, String>,
) -> Result<GridRuntime, FilterError> {
    validate_local_site(&config.local_site)?;
    let validated = if config.candidates.is_empty() {
        validate_serving_candidates(config.candidates.clone())?
    } else {
        validate_candidates(config.candidates.clone())?
    };
    let provider_hop_clusters = validate_provider_hop_clusters(config.provider_hop_clusters.clone())?;
    validate_provider_hop_binding(config, &backend_tls)?;
    validate_hop_candidate_ids(&config.candidates, &provider_hop_clusters)?;
    let candidates: Arc<[RouteCandidate]> = Arc::from(validated);
    let local_site: Arc<str> = Arc::from(config.local_site.as_str());
    let store = Arc::new(LoadStore::new(Duration::from_secs(config.window_secs)));
    // Cold start: config order until the first poll re-orders it by live load.
    let cold_start = RouteSnapshot::from_static_with_provider_hops(
        candidates.iter().cloned().collect(),
        Arc::clone(&local_site),
        provider_hop_clusters.clone(),
    );
    let snapshot = Arc::new(ArcSwap::from_pointee(cold_start));
    let shared = Arc::new(RuntimeShared {
        base: ArcSwap::from_pointee(RefreshConfig {
            candidates,
            local_site,
            load_window_ms: config.load_window_ms,
            provider_hop_clusters: Arc::new(provider_hop_clusters),
        }),
        snapshot,
        store,
        pollers: Mutex::new(Vec::new()),
        refresh_gate: Mutex::new(()),
        backend_tls,
    });
    let pollers = spawn_pollers(config, &shared)?;
    shared
        .pollers
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .extend(pollers);
    Ok(GridRuntime {
        shared,
        startup_config: config.clone(),
        watcher_stop: Arc::new(AtomicBool::new(false)),
        watcher: None,
    })
}

/// Start pollers for a validated config.
fn spawn_pollers(config: &GridServingConfig, shared: &Arc<RuntimeShared>) -> Result<Vec<PollHandle>, FilterError> {
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
        let refresh = make_refresh(Arc::clone(shared), now_ms);
        let handle = spawn_on_thread(Arc::clone(&shared.store), &poller_config, scraper, refresh)
            .map_err(|error| -> FilterError { format!("grid: spawning poller for {}: {error}", peer.site).into() })?;
        pollers.push(handle);
    }
    Ok(pollers)
}

/// The refresh closure reorders the latest candidate set by live load.
fn make_refresh<N>(shared: Arc<RuntimeShared>, now: N) -> impl Fn(&LoadStore) + Send
where
    N: Fn() -> i64 + Send,
{
    move |store: &LoadStore| {
        let _gate = shared.refresh_gate.lock().unwrap_or_else(PoisonError::into_inner);
        let base = shared.base.load();
        let ordered = RouteSnapshot::from_store_with_provider_hops(
            base.candidates.to_vec(),
            Arc::clone(&base.local_site),
            store,
            now(),
            base.load_window_ms,
            (*base.provider_hop_clusters).clone(),
        );
        shared.snapshot.store(Arc::new(ordered));
    }
}

/// Authenticated provider routes require the exact stable ID emitted by the
/// Grid overlay; a gateway-derived fallback ID would not match provider policy.
fn validate_hop_candidate_ids(
    candidates: &[CandidateConfig],
    provider_hop_clusters: &BTreeSet<String>,
) -> Result<(), FilterError> {
    for candidate in candidates {
        if provider_hop_clusters.contains(&candidate.cluster) && candidate.stable_id.is_none() {
            return Err(format!(
                "grid: candidate '{}' on provider-hop cluster '{}' is missing stable_id",
                candidate.name, candidate.cluster
            )
            .into());
        }
    }
    Ok(())
}

/// Apply a valid candidate revision and replace pollers when peer transport
/// topology changes. The last-known-good snapshot remains intact on any error.
#[expect(
    clippy::too_many_lines,
    reason = "validation and atomic publication must stay in one last-known-good transaction"
)]
fn apply_serving_revision(
    shared: &Arc<RuntimeShared>,
    current: &mut GridServingConfig,
    next: GridServingConfig,
    raw: &str,
) -> Result<(), FilterError> {
    validate_local_site(&next.local_site)?;
    let candidates = validate_serving_candidates(next.candidates.clone())?;
    let provider_hop_clusters = validate_provider_hop_clusters(next.provider_hop_clusters.clone())?;
    validate_provider_hop_binding(&next, &shared.backend_tls)?;
    validate_hop_candidate_ids(&next.candidates, &provider_hop_clusters)?;
    if next.window_secs != current.window_secs {
        return Err("grid: changing window_secs requires a gateway restart".into());
    }
    let replacement_pollers = if next.peers == current.peers {
        None
    } else {
        Some(spawn_pollers(&next, shared)?)
    };
    let base = RefreshConfig {
        candidates: Arc::from(candidates),
        local_site: Arc::from(next.local_site.as_str()),
        load_window_ms: next.load_window_ms,
        provider_hop_clusters: Arc::new(provider_hop_clusters),
    };
    let serving = RouteSnapshot::from_store_with_provider_hops(
        base.candidates.to_vec(),
        Arc::clone(&base.local_site),
        &shared.store,
        now_ms(),
        base.load_window_ms,
        (*base.provider_hop_clusters).clone(),
    );
    {
        let _gate = shared.refresh_gate.lock().unwrap_or_else(PoisonError::into_inner);
        shared.base.store(Arc::new(base));
        shared.snapshot.store(Arc::new(serving));
    }
    if let Some(pollers) = replacement_pollers {
        *shared.pollers.lock().unwrap_or_else(PoisonError::into_inner) = pollers;
    }
    let revision = format!("{:x}", Sha256::digest(raw.as_bytes()));
    tracing::info!(
        revision,
        candidate_count = next.candidates.len(),
        "grid_serving_revision_serving"
    );
    *current = next;
    Ok(())
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
    use std::sync::atomic::AtomicI64;

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
            stable_id: None,
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
        let candidates: Arc<[RouteCandidate]> = Arc::from(
            validate_candidates(vec![cand("llama", "east", "pool-a"), cand("llama", "west", "pool-b")])
                .expect("candidates"),
        );
        let local_site: Arc<str> = Arc::from("local");
        let snapshot = Arc::new(ArcSwap::from_pointee(RouteSnapshot::from_static(
            candidates.iter().cloned().collect(),
            Arc::clone(&local_site),
        )));
        let shared = Arc::new(RuntimeShared {
            base: ArcSwap::from_pointee(RefreshConfig {
                candidates,
                local_site,
                load_window_ms: 30_000,
                provider_hop_clusters: Arc::new(BTreeSet::new()),
            }),
            snapshot: Arc::clone(&snapshot),
            store: Arc::new(LoadStore::new(Duration::from_secs(600))),
            pollers: Mutex::new(Vec::new()),
            refresh_gate: Mutex::new(()),
            backend_tls: BTreeMap::new(),
        });
        let clock = Arc::new(AtomicI64::new(1_000));
        let refresh = {
            let clock = Arc::clone(&clock);
            make_refresh(shared, move || clock.load(Ordering::SeqCst))
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

    #[test]
    fn provider_hop_allowlist_requires_the_exact_grid_stable_id() {
        let hops = validate_provider_hop_clusters(vec!["pool-a".to_owned()]).expect("allowlist");
        let missing_id = vec![cand("llama", "east", "pool-a")];
        assert!(
            validate_hop_candidate_ids(&missing_id, &hops)
                .expect_err("provider route requires overlay ID")
                .to_string()
                .contains("missing stable_id")
        );

        let mut identified = cand("llama", "east", "pool-a");
        identified.stable_id = Some("257a9450".to_owned());
        validate_hop_candidate_ids(&[identified], &hops).expect("matching ID is accepted");
        validate_provider_hop_clusters(vec!["pool-a".to_owned(), "pool-a".to_owned()])
            .expect_err("duplicate provider-hop clusters are rejected");
        validate_provider_hop_clusters(vec![" ".to_owned()]).expect_err("blank provider-hop clusters are rejected");
    }

    fn serving_yaml(candidates: &str) -> String {
        format!("local_site: local\nwindow_secs: 60\nload_window_ms: 30000\ncandidates:{candidates}\npeers: []\n")
    }

    fn wait_for_candidate_count(runtime: &GridRuntime, expected: usize) {
        let deadline = std::time::Instant::now()
            .checked_add(Duration::from_secs(5))
            .expect("test deadline fits in Instant");
        while runtime.snapshot().load().candidates.len() != expected {
            assert!(
                std::time::Instant::now() < deadline,
                "serving snapshot did not converge"
            );
            thread::park_timeout(Duration::from_millis(20));
        }
    }

    fn temporary_config_path() -> (std::path::PathBuf, std::path::PathBuf) {
        let nonce = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        );
        let directory = std::env::temp_dir().join(format!("grid-serving-{nonce}"));
        fs::create_dir(&directory).expect("create temporary directory");
        let path = directory.join("serving-config.json");
        (directory, path)
    }

    #[test]
    fn empty_serving_config_is_a_valid_cold_start() {
        let config: GridServingConfig = serde_yaml::from_str(&serving_yaml(" []")).expect("empty config parses");
        let runtime = spawn_grid_routing(&config, BTreeMap::new()).expect("valid no-route cold start");
        assert!(runtime.snapshot().load().candidates.is_empty());
    }

    #[test]
    fn provider_hop_binding_rejects_plaintext_or_wrong_identity() {
        let mut config: GridServingConfig = serde_yaml::from_str(&serving_yaml(" []")).expect("serving config");
        config.provider_hop_clusters = vec!["provider-a".to_owned()];
        config
            .provider_hop_sni
            .insert("provider-a".to_owned(), "provider-a.grid.internal".to_owned());
        assert!(
            validate_provider_hop_binding(&config, &BTreeMap::new()).is_err(),
            "plaintext backend"
        );
        let wrong = BTreeMap::from([("provider-a".to_owned(), "other.grid.internal".to_owned())]);
        assert!(
            validate_provider_hop_binding(&config, &wrong).is_err(),
            "wrong TLS identity"
        );
        let matching = BTreeMap::from([("provider-a".to_owned(), "provider-a.grid.internal".to_owned())]);
        validate_provider_hop_binding(&config, &matching).expect("matching mTLS identity is accepted");
        config.provider_hop_clusters.clear();
        assert!(
            validate_provider_hop_binding(&config, &matching).is_err(),
            "undeclared hop identity"
        );
    }

    #[test]
    fn serving_config_watcher_withdraws_retains_invalid_and_restores_routes() {
        let (directory, path) = temporary_config_path();
        let active = serving_yaml(
            "\n  - kind: inference_model\n    name: llama\n    site: east\n    cluster: pool-a\n    fresh: true",
        );
        fs::write(&path, &active).expect("write active config");
        let config = load_serving_config(path.to_str().expect("utf-8 path")).expect("active config");
        let mut runtime = spawn_grid_routing(&config, BTreeMap::new()).expect("active runtime");
        runtime
            .watch_config(path.to_str().expect("utf-8 path"))
            .expect("watch config");
        assert_eq!(runtime.snapshot().load().candidates.len(), 1);

        fs::write(&path, serving_yaml(" []")).expect("write empty config");
        wait_for_candidate_count(&runtime, 0);

        fs::write(
            &path,
            serving_yaml("\n  - kind: inference_model\n    name: ''\n    site: east\n    cluster: pool-a"),
        )
        .expect("write invalid config");
        thread::park_timeout(Duration::from_millis(CONFIG_POLL_INTERVAL_MS * 2));
        assert!(
            runtime.snapshot().load().candidates.is_empty(),
            "invalid revision retains empty LKG"
        );

        fs::write(&path, &active).expect("restore active config");
        wait_for_candidate_count(&runtime, 1);
        assert_eq!(&*runtime.snapshot().load().candidates[0].cluster, "pool-a");
        drop(runtime);
        fs::remove_dir_all(directory).expect("remove temporary directory");
    }

    #[test]
    fn serving_watcher_retries_withdrawal_after_peer_tls_recovers() {
        let (directory, path) = temporary_config_path();
        let active = serving_yaml(
            "\n  - kind: inference_model\n    name: llama\n    site: east\n    cluster: pool-a\n    fresh: true",
        );
        fs::write(&path, &active).expect("write active config");
        let config = load_serving_config(path.to_str().expect("utf-8 path")).expect("active config");
        let mut runtime = spawn_grid_routing(&config, BTreeMap::new()).expect("active runtime");
        runtime
            .watch_config(path.to_str().expect("utf-8 path"))
            .expect("watch config");

        let ca = certs::generate_ca("grid-ca").expect("test CA");
        let client = certs::generate_site_cert(&ca, "local").expect("test client certificate");
        let ca_path = directory.join("ca.pem");
        let cert_path = directory.join("client.crt");
        let key_path = directory.join("client.key");
        fs::write(&cert_path, client.cert_pem).expect("write client certificate");
        fs::write(&key_path, client.key_pem).expect("write client key");
        let withdrawn = format!(
            "local_site: local\nwindow_secs: 60\nload_window_ms: 30000\ncandidates: []\npeers:\n  - site: east\n    addr: 127.0.0.1:1\n    server_name: east.grid.internal\n    authority: east.grid.internal\n    grid_ca_path: {}\n    client_cert_path: {}\n    client_key_path: {}\n",
            ca_path.display(),
            cert_path.display(),
            key_path.display()
        );
        fs::write(&path, &withdrawn).expect("publish withdrawal with unavailable peer CA");
        thread::park_timeout(Duration::from_secs(2));
        assert_eq!(
            runtime.snapshot().load().candidates.len(),
            1,
            "failed apply retains the prior route"
        );

        fs::write(&ca_path, ca.cert_pem).expect("repair peer CA without rewriting serving config");
        wait_for_candidate_count(&runtime, 0);
        assert_eq!(fs::read_to_string(&path).expect("read serving config"), withdrawn);
        drop(runtime);
        fs::remove_dir_all(directory).expect("remove temporary directory");
    }

    #[test]
    fn watcher_applies_file_change_between_startup_load_and_watch() {
        let (directory, path) = temporary_config_path();
        let active = serving_yaml(
            "\n  - kind: inference_model\n    name: llama\n    site: east\n    cluster: pool-a\n    fresh: true",
        );
        fs::write(&path, &active).expect("write active config");

        // The gateway binary loads the config and seeds its snapshot before it
        // starts the watcher. A projected-volume update can land in between.
        let startup = load_serving_config(path.to_str().expect("utf-8 path")).expect("startup config");
        let mut runtime = spawn_grid_routing(&startup, BTreeMap::new()).expect("active runtime");
        assert_eq!(runtime.snapshot().load().candidates.len(), 1);

        fs::write(&path, serving_yaml(" []")).expect("withdraw all candidates");
        runtime
            .watch_config(path.to_str().expect("utf-8 path"))
            .expect("watch config applies the current file");

        assert!(
            runtime.snapshot().load().candidates.is_empty(),
            "the first watcher read must apply bytes newer than the startup snapshot"
        );
        drop(runtime);
        fs::remove_dir_all(directory).expect("remove temporary directory");
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
