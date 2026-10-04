//! Applies a grid serving config: the candidate topology and the peer pollers.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use grid_signals::{LoadStore, now_ms};
use grid_signals_client::{PollHandle, PollerConfig};
use praxis_filter::FilterError;

use crate::{
    descriptor::{RouteCandidate, validate_candidates, validate_local_site},
    pin::TagKey,
    prefix::PrefixAffinity,
    serving::{GridServingConfig, PeerServingConfig, validate_peer},
    snapshot::RouteSnapshot,
};

/// The order step a poller runs after each scrape.
pub(crate) type Refresh = Box<dyn Fn(&LoadStore) + Send>;

/// Starts one peer's poller feeding `store`, running `refresh` each cycle.
pub(crate) type StartPeer =
    Box<dyn Fn(&PeerServingConfig, &PollerConfig, Arc<LoadStore>, Refresh) -> Result<PollHandle, FilterError> + Send>;

/// The validated candidate topology the refresh orders.
pub(crate) struct Topology {
    /// Candidates in config order.
    base: Arc<[RouteCandidate]>,

    /// This gateway's own site.
    local_site: Arc<str>,

    /// Freshness window the order reads, milliseconds.
    load_window_ms: i64,
}

impl Topology {
    /// Validate the topology half of `config`.
    fn from_config(config: &GridServingConfig) -> Result<Self, FilterError> {
        validate_local_site(&config.local_site)?;
        Ok(Self {
            base: Arc::from(validate_candidates(config.candidates.clone())?),
            local_site: Arc::from(config.local_site.as_str()),
            load_window_ms: config.load_window_ms,
        })
    }

    /// The snapshot ordered from `store` at `now`.
    fn order(&self, store: &LoadStore, now: i64) -> RouteSnapshot {
        RouteSnapshot::from_store(
            self.base.iter().cloned().collect(),
            Arc::clone(&self.local_site),
            store,
            now,
            self.load_window_ms,
        )
    }
}

/// The next peer set by site, and the pollers started for its new or changed peers.
type Planned<'config> = (HashMap<&'config str, &'config PeerServingConfig>, Vec<RunningPeer>);

/// A running peer poller and the config it was started from.
struct RunningPeer {
    /// The config the poller was built from, compared on reload.
    config: PeerServingConfig,

    /// The freshness bound the poller was built with, compared on reload.
    load_window_ms: i64,

    /// Stops the poller on drop. Started held, committed once its reload is.
    handle: PollHandle,
}

impl RunningPeer {
    /// Whether this poller already runs `peer` under `load_window_ms`.
    fn unchanged(&self, peer: &PeerServingConfig, load_window_ms: i64) -> bool {
        self.config == *peer && self.load_window_ms == load_window_ms
    }
}

/// What a reload changed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReloadOutcome {
    /// Pollers started for new or changed peers.
    pub started: usize,

    /// Pollers stopped for removed or changed peers.
    pub stopped: usize,

    /// Pollers kept, with their gathered load, for unchanged peers.
    pub kept: usize,
}

/// The gateway control plane: the shared store, snapshot, topology, and pollers.
pub(crate) struct Control {
    /// Live load from every peer, shared by all pollers.
    store: Arc<LoadStore>,

    /// The snapshot the filter loads once per request.
    snapshot: Arc<ArcSwap<RouteSnapshot>>,

    /// The topology every refresh orders, swapped on reload.
    topology: Arc<ArcSwap<Topology>>,

    /// Serializes snapshot stores between the refreshes and a reload.
    swap: Arc<Mutex<()>>,

    /// Store retention, fixed for the process.
    window_secs: u64,

    /// Running pollers by peer site.
    peers: HashMap<String, RunningPeer>,

    /// The last applied config, so an unchanged file is a no-op.
    applied: Option<GridServingConfig>,

    /// Digest of the identity files the applied config names, so a renewed Secret restarts the pollers.
    identity: Option<[u8; 32]>,

    /// Builds and spawns a peer's poller.
    start: StartPeer,

    /// The prefix index and affinity settings, applied with each config.
    affinity: Arc<PrefixAffinity>,
}

impl Control {
    /// Build the control plane for `config` without starting any poller.
    pub(crate) fn new(config: &GridServingConfig, start: StartPeer) -> Result<Self, FilterError> {
        let topology = Topology::from_config(config)?;
        // Cold start: config order until the first poll re-orders it by live load.
        let cold_start = RouteSnapshot::from_static(
            topology.base.iter().cloned().collect(),
            Arc::clone(&topology.local_site),
        );
        Ok(Self {
            store: Arc::new(LoadStore::new(Duration::from_secs(config.window_secs))),
            snapshot: Arc::new(ArcSwap::from_pointee(cold_start)),
            topology: Arc::new(ArcSwap::from_pointee(topology)),
            swap: Arc::new(Mutex::new(())),
            window_secs: config.window_secs,
            peers: HashMap::new(),
            applied: None,
            identity: None,
            start,
            affinity: Arc::default(),
        })
    }

    /// The snapshot the filter reads.
    pub(crate) fn snapshot(&self) -> Arc<ArcSwap<RouteSnapshot>> {
        Arc::clone(&self.snapshot)
    }

    /// The prefix index and affinity settings the route filter reads.
    pub(crate) fn affinity(&self) -> Arc<PrefixAffinity> {
        Arc::clone(&self.affinity)
    }

    /// Make `config` the running one: its identity, its affinity settings, and only its clusters' prefixes.
    fn adopt(&mut self, config: &GridServingConfig, identity: [u8; 32], tag_key: Option<TagKey>) {
        let clusters: Vec<Arc<str>> = config
            .candidates
            .iter()
            .map(|candidate| Arc::from(candidate.cluster.as_str()))
            .collect();
        self.affinity.apply(config.prefix_affinity.clone(), tag_key, &clusters);
        self.applied = Some(config.clone());
        self.identity = Some(identity);
    }

    /// Validate `config` fully, then swap it in. `None` when already applied.
    ///
    /// # Errors
    ///
    /// Returns [`FilterError`] for an invalid config or a poller that cannot start, changing nothing.
    pub(crate) fn apply(&mut self, config: &GridServingConfig) -> Result<Option<ReloadOutcome>, FilterError> {
        self.apply_with(config, identity_digest(config))
    }

    /// [`Self::apply`] with the identity digest already computed.
    fn apply_with(
        &mut self,
        config: &GridServingConfig,
        identity: [u8; 32],
    ) -> Result<Option<ReloadOutcome>, FilterError> {
        let renewed = self.identity.is_some_and(|applied| applied != identity);
        if self.applied.as_ref() == Some(config) && !renewed {
            return Ok(None);
        }
        // Validated before any poller starts, so an invalid config starts and drops nothing.
        let topology = Arc::new(validate_config(config)?);
        let tag_key = load_tag_key(config)?;
        if config.window_secs != self.window_secs {
            tracing::warn!(
                current = self.window_secs,
                requested = config.window_secs,
                "grid: window_secs changes take effect on restart"
            );
        }
        let (next, started) = self.start_changed(config, renewed)?;
        let outcome = self.reconcile(&next, started, renewed, config.load_window_ms);
        let ordered = Arc::new(topology.order(&self.store, now_ms()));
        {
            let _swapping = self.swap.lock().unwrap_or_else(PoisonError::into_inner);
            self.topology.store(topology);
            self.snapshot.store(ordered);
        }
        // The reload stands and its topology is published: only now may the new pollers write,
        // so their first refresh orders the new topology. Committing a kept poller is a no-op.
        self.peers.values().for_each(|running| running.handle.commit());
        self.adopt(config, identity, tag_key);
        Ok(Some(outcome))
    }

    /// Start pollers for new or changed peers, or every peer once the identity renewed,
    /// before any swap, so an error keeps the old set.
    fn start_changed<'config>(
        &self,
        config: &'config GridServingConfig,
        renewed: bool,
    ) -> Result<Planned<'config>, FilterError> {
        let mut next = HashMap::with_capacity(config.peers.len());
        let mut started = Vec::new();
        for peer in &config.peers {
            next.insert(peer.site.as_str(), peer);
            let keep = self
                .peers
                .get(&peer.site)
                .is_some_and(|running| running.unchanged(peer, config.load_window_ms));
            if renewed || !keep {
                let poller = self.poller_config(peer, config.load_window_ms);
                let handle = (self.start)(peer, &poller, Arc::clone(&self.store), self.refresh())?;
                started.push(RunningPeer {
                    config: peer.clone(),
                    load_window_ms: config.load_window_ms,
                    handle,
                });
            }
        }
        Ok((next, started))
    }

    /// Stop the pollers of removed or changed peers, or all of them once the identity
    /// renewed, and adopt the started ones.
    fn reconcile(
        &mut self,
        next: &HashMap<&str, &PeerServingConfig>,
        started: Vec<RunningPeer>,
        renewed: bool,
        load_window_ms: i64,
    ) -> ReloadOutcome {
        let before = self.peers.len();
        self.peers.retain(|site, running| {
            !renewed
                && next
                    .get(site.as_str())
                    .is_some_and(|peer| running.unchanged(peer, load_window_ms))
        });
        let kept = self.peers.len();
        let outcome = ReloadOutcome {
            started: started.len(),
            stopped: before.saturating_sub(kept),
            kept,
        };
        self.peers.extend(
            started
                .into_iter()
                .map(|running| (running.config.site.clone(), running)),
        );
        outcome
    }

    /// The poller settings for `peer` under a `load_window_ms` freshness bound.
    fn poller_config(&self, peer: &PeerServingConfig, load_window_ms: i64) -> PollerConfig {
        PollerConfig {
            endpoint: peer.addr.clone(),
            interval_ms: peer.interval_ms,
            window_secs: self.window_secs,
            max_age_ms: load_window_ms,
            timeout_ms: peer.request_timeout_ms,
            tls: None,
        }
    }

    /// The refresh a poller runs each cycle: order the current topology by load.
    fn refresh(&self) -> Refresh {
        make_refresh(
            Arc::clone(&self.topology),
            Arc::clone(&self.snapshot),
            Arc::clone(&self.swap),
            now_ms,
        )
    }
}

/// The stored-state tag key `config` names, or `None` when it names none.
fn load_tag_key(config: &GridServingConfig) -> Result<Option<TagKey>, FilterError> {
    let Some(path) = config.prefix_affinity.tag_key_path.as_deref() else {
        return Ok(None);
    };
    let bytes = std::fs::read(path)
        .map(zeroize::Zeroizing::new)
        .map_err(|error| -> FilterError { format!("grid: reading the tag key {path}: {error}").into() })?;
    TagKey::new(bytes)
        .map(Some)
        .map_err(|error| -> FilterError { format!("grid: {error}").into() })
}

/// The topology of `config`, refusing what no retry can fix: a bad candidate, peer, or duplicate site.
fn validate_config(config: &GridServingConfig) -> Result<Topology, FilterError> {
    let topology = Topology::from_config(config)?;
    config
        .prefix_affinity
        .validate()
        .map_err(|error| -> FilterError { error.into() })?;
    let mut sites = std::collections::HashSet::with_capacity(config.peers.len());
    for peer in &config.peers {
        validate_peer(peer)?;
        if !sites.insert(peer.site.as_str()) {
            return Err(format!("grid: peer site {} appears twice", peer.site).into());
        }
    }
    Ok(topology)
}

/// Order the current topology and swap it in, unless a reload replaced it meanwhile.
pub(crate) fn make_refresh<N>(
    topology: Arc<ArcSwap<Topology>>,
    snapshot: Arc<ArcSwap<RouteSnapshot>>,
    swap: Arc<Mutex<()>>,
    now: N,
) -> Refresh
where
    N: Fn() -> i64 + Send + 'static,
{
    Box::new(move |store: &LoadStore| {
        let current = topology.load_full();
        let ordered = Arc::new(current.order(store, now()));
        let _swapping = swap.lock().unwrap_or_else(PoisonError::into_inner);
        if Arc::ptr_eq(&current, &topology.load()) {
            snapshot.store(ordered);
        }
    })
}

/// A change detector over every identity file `config` names: the grid CA,
/// client certificate, and key. Not a security function.
///
/// A rotation swaps the files at once but they are read one by one, so a pass
/// that straddles the swap mixes versions. Read until two passes agree.
fn identity_digest(config: &GridServingConfig) -> [u8; 32] {
    let mut last = read_identity(config);
    for _ in 0..IDENTITY_READS {
        let next = read_identity(config);
        if next == last {
            break;
        }
        last = next;
    }
    last
}

/// Passes [`identity_digest`] makes after the first; a rotation tears at most one.
const IDENTITY_READS: usize = 3;

/// One pass over the identity files. Each file is hashed on its own, so no
/// buffer holds the concatenated key.
fn read_identity(config: &GridServingConfig) -> [u8; 32] {
    let paths: std::collections::BTreeSet<&str> = config
        .peers
        .iter()
        .flat_map(|peer| [&peer.grid_ca_path, &peer.client_cert_path, &peer.client_key_path])
        .map(String::as_str)
        // A rotated tag key re-applies the config like a renewed certificate.
        .chain(config.prefix_affinity.tag_key_path.as_deref())
        .collect();
    let mut material = Vec::new();
    for path in paths {
        material.extend_from_slice(&path.len().to_be_bytes());
        material.extend_from_slice(path.as_bytes());
        match std::fs::read(path).map(zeroize::Zeroizing::new) {
            Ok(content) => {
                material.push(1);
                material.extend_from_slice(&certs::sha256(&content));
            },
            Err(_) => material.push(0),
        }
    }
    certs::sha256(&material)
}

/// Re-reads the serving config file on an interval and applies changes.
pub(crate) struct Watcher {
    /// Dropping it stops the watch thread.
    _stop: mpsc::Sender<()>,

    /// How many changed files the watch has handled.
    #[cfg_attr(not(test), expect(dead_code, reason = "read by tests"))]
    counts: Arc<WatchCounts>,
}

/// Reloads the watch applied, reused, or rejected.
#[derive(Debug, Default)]
pub(crate) struct WatchCounts {
    /// Reloads that changed the running config or identity.
    applied: AtomicUsize,

    /// Changed files equal to the running config.
    reused: AtomicUsize,

    /// Files that failed to parse or validate.
    rejected: AtomicUsize,
}

impl WatchCounts {
    /// Files applied so far.
    #[cfg(test)]
    pub(crate) fn applied(&self) -> usize {
        self.applied.load(Ordering::SeqCst)
    }

    /// Files equal to the running config so far.
    #[cfg(test)]
    pub(crate) fn reused(&self) -> usize {
        self.reused.load(Ordering::SeqCst)
    }

    /// Files rejected so far.
    #[cfg(test)]
    pub(crate) fn rejected(&self) -> usize {
        self.rejected.load(Ordering::SeqCst)
    }
}

impl Watcher {
    /// The watch's applied and rejected counts.
    #[cfg(test)]
    pub(crate) fn counts(&self) -> &WatchCounts {
        &self.counts
    }
}

/// Apply `path` through `control` when it changes, keeping the last good config on a bad file.
pub(crate) fn watch(control: Arc<Mutex<Control>>, path: PathBuf, every: Duration) -> std::io::Result<Watcher> {
    let (stop, stopped) = mpsc::channel::<()>();
    let counts = Arc::new(WatchCounts::default());
    let tally = Arc::clone(&counts);
    std::thread::Builder::new()
        .name("grid-serving-watch".to_owned())
        .spawn(move || {
            // The last file applied or refused for good. A file that failed for a
            // reason a retry can fix stays pending and is applied again next tick.
            let mut settled: Option<Vec<u8>> = None;
            while let Err(mpsc::RecvTimeoutError::Timeout) = stopped.recv_timeout(every) {
                // The kubelet swaps the mounted ..data symlink, so a plain read sees the new file.
                match std::fs::read(&path) {
                    Ok(bytes) if settled.as_ref() != Some(&bytes) => {
                        if apply_file(&control, &path, &bytes, &tally) {
                            settled = Some(bytes);
                        }
                    },
                    Ok(_) => {},
                    Err(_) => {
                        tracing::warn!(path = %path.display(), "grid: serving config unreadable; keeping the last good config");
                    },
                }
                // The running config renews its identity whatever the pending file does.
                renew_identity(&control, &tally);
            }
        })?;
    Ok(Watcher { _stop: stop, counts })
}

/// Parse and apply one changed file, logging and counting the result.
///
/// Returns whether the file is settled: applied, or refused for good because it
/// does not parse or validate. Any other failure, such as identity files
/// mid-rotation, is retried on the next tick.
fn apply_file(control: &Mutex<Control>, path: &std::path::Path, bytes: &[u8], tally: &WatchCounts) -> bool {
    let parsed = serde_yaml::from_slice::<GridServingConfig>(bytes)
        .map_err(|error| -> FilterError { format!("grid: parsing {}: {error}", path.display()).into() })
        .and_then(|config| validate_config(&config).map(|_| config));
    let (result, settled) = match parsed {
        Ok(config) => {
            let result = control.lock().unwrap_or_else(PoisonError::into_inner).apply(&config);
            let settled = result.is_ok();
            (result, settled)
        },
        Err(error) => (Err(error), true),
    };
    report(&result, tally, "grid: serving config reloaded");
    settled
}

/// Re-apply the last good config, which restarts the pollers once its identity files change.
fn renew_identity(control: &Mutex<Control>, tally: &WatchCounts) {
    let mut control = control.lock().unwrap_or_else(PoisonError::into_inner);
    let Some((config, identity)) = control.applied.as_ref().and_then(|config| {
        let identity = identity_digest(config);
        (control.identity != Some(identity)).then(|| (config.clone(), identity))
    }) else {
        return;
    };
    let result = control.apply_with(&config, identity);
    drop(control);
    if !matches!(result, Ok(None)) {
        report(&result, tally, "grid: identity rotated; peer pollers restarted");
    }
}

/// Count, log, and export one reload attempt.
fn report(result: &Result<Option<ReloadOutcome>, FilterError>, tally: &WatchCounts, applied: &str) {
    let (counter, label) = match result {
        Ok(Some(_)) => (&tally.applied, "applied"),
        Ok(None) => (&tally.reused, "reused"),
        Err(_) => (&tally.rejected, "rejected"),
    };
    counter.fetch_add(1, Ordering::SeqCst);
    metrics::counter!("grid_serving_config_reload_total", "result" => label).increment(1);
    match result {
        Ok(Some(outcome)) => tracing::info!(
            started = outcome.started,
            stopped = outcome.stopped,
            kept = outcome.kept,
            "{applied}"
        ),
        Ok(None) => tracing::info!("grid: serving config unchanged; peer pollers reused"),
        Err(error) => tracing::warn!(%error, "grid: serving config rejected; keeping the last good config"),
    }
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::min_ident_chars,
    clippy::too_many_lines,
    clippy::disallowed_methods,
    reason = "tests; the pollers run on their own threads, so these sync tests wait with thread::sleep"
)]
mod tests {
    use std::{sync::atomic::AtomicI64, time::Instant};

    use grid_signals_client::{FetchError, Scrape, SignalSource, spawn_on_thread_held};

    use super::*;
    use crate::{
        descriptor::{CandidateConfig, CapabilityKind},
        snapshot::LOAD_METRIC,
    };

    /// Queue depth each mock peer reports, and how often each was scraped and started.
    #[derive(Clone, Default)]
    struct Peers {
        load: Arc<Mutex<HashMap<String, f64>>>,
        fetches: Arc<Mutex<HashMap<String, Arc<AtomicUsize>>>>,
        starts: Arc<Mutex<HashMap<String, usize>>>,
    }

    impl Peers {
        fn set_load(&self, site: &str, value: f64) {
            self.load.lock().expect("load").insert(site.to_owned(), value);
        }

        fn fetches(&self, site: &str) -> usize {
            self.fetches
                .lock()
                .expect("fetches")
                .get(site)
                .map_or(0, |count| count.load(Ordering::SeqCst))
        }

        fn starts(&self, site: &str) -> usize {
            self.starts.lock().expect("starts").get(site).copied().unwrap_or(0)
        }

        /// A starter that polls a mock source for each peer every 10ms.
        fn starter(&self) -> StartPeer {
            let peers = self.clone();
            Box::new(move |peer: &PeerServingConfig, poller: &PollerConfig, store, refresh| {
                let mut starts = peers.starts.lock().expect("starts");
                let started = starts.entry(peer.site.clone()).or_default();
                *started = started.saturating_add(1);
                drop(starts);
                let count = Arc::new(AtomicUsize::new(0));
                peers
                    .fetches
                    .lock()
                    .expect("fetches")
                    .insert(peer.site.clone(), Arc::clone(&count));
                let source = MockSource {
                    site: peer.site.clone(),
                    load: Arc::clone(&peers.load),
                    count,
                };
                let fast = PollerConfig {
                    interval_ms: 10,
                    ..poller.clone()
                };
                spawn_on_thread_held(store, &fast, source, refresh)
                    .map_err(|error| -> FilterError { error.to_string().into() })
            })
        }
    }

    /// One peer's signals endpoint, answering with its configured load.
    struct MockSource {
        site: String,
        load: Arc<Mutex<HashMap<String, f64>>>,
        count: Arc<AtomicUsize>,
    }

    impl SignalSource for MockSource {
        async fn fetch(&self) -> Result<Scrape, FetchError> {
            self.count.fetch_add(1, Ordering::SeqCst);
            let value = self.load.lock().expect("load").get(&self.site).copied().unwrap_or(0.0);
            let at = now_ms();
            Ok(Scrape {
                body: format!(
                    r#"{LOAD_METRIC}{{grid_site="{site}",grid_provider="pool-{site}"}} {value} {at}"#,
                    site = self.site
                ),
                date_ms: at,
                peer_identity: Arc::from(certs::spiffe_id(&self.site).as_str()),
            })
        }
    }

    fn candidate(site: &str) -> CandidateConfig {
        CandidateConfig {
            cluster: format!("pool-{site}"),
            credential: None,
            fresh: true,
            kind: CapabilityKind::InferenceModel,
            name: "llama".to_owned(),
            site: site.to_owned(),
        }
    }

    fn peer(site: &str) -> PeerServingConfig {
        PeerServingConfig {
            site: site.to_owned(),
            addr: format!("{site}.grid.internal:9091"),
            server_name: format!("{site}.grid.internal"),
            authority: format!("{site}.grid.internal"),
            path: "/v1/site/signals".to_owned(),
            interval_ms: 2_000,
            connect_timeout_ms: 2_000,
            request_timeout_ms: 2_000,
            grid_ca_path: "/etc/grid/ca.pem".to_owned(),
            client_cert_path: "/etc/grid/tls.crt".to_owned(),
            client_key_path: "/etc/grid/tls.key".to_owned(),
            pins: Vec::new(),
        }
    }

    /// A config serving `llama` from each of `sites` and polling each of them.
    fn config(sites: &[&str]) -> GridServingConfig {
        GridServingConfig {
            local_site: "local".to_owned(),
            window_secs: 60,
            load_window_ms: 30_000,
            candidates: sites.iter().map(|site| candidate(site)).collect(),
            peers: sites.iter().map(|site| peer(site)).collect(),
            prefix_affinity: crate::prefix::AffinitySettings::default(),
        }
    }

    #[test]
    fn a_tag_key_must_exist_and_be_long_enough() {
        let dir = std::env::temp_dir().join(format!("grid-tag-key-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("test io");
        let mut config = config(&["east"]);
        assert!(load_tag_key(&config).expect("loads").is_none(), "no path, no key");
        let at = |name: &str| dir.join(name).display().to_string();
        config.prefix_affinity.tag_key_path = Some(at("missing"));
        assert!(load_tag_key(&config).is_err(), "a missing file is refused");
        std::fs::write(dir.join("short"), [1_u8; 31]).expect("test io");
        config.prefix_affinity.tag_key_path = Some(at("short"));
        assert!(load_tag_key(&config).is_err(), "31 bytes is refused");
        std::fs::write(dir.join("key"), [1_u8; 32]).expect("test io");
        config.prefix_affinity.tag_key_path = Some(at("key"));
        assert!(load_tag_key(&config).expect("loads").is_some());
        let _removed = std::fs::remove_dir_all(&dir);
    }

    fn sites(snapshot: &RouteSnapshot) -> Vec<String> {
        snapshot.candidates.iter().map(|c| c.site.to_string()).collect()
    }

    /// The routable sites, sorted: which sites route, not their load order.
    fn members(snapshot: &RouteSnapshot) -> Vec<String> {
        let mut sites = sites(snapshot);
        sites.sort();
        sites
    }

    /// A `Secret` or `ConfigMap` volume: each file resolves through `..data`, which
    /// `write` swaps in one rename, as the kubelet does.
    struct Mount {
        dir: PathBuf,
        generation: usize,
    }

    impl Mount {
        fn new(name: &str, files: &[(&str, &str)]) -> Self {
            let dir = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
            let _stale = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("dir");
            let mut mount = Self { dir, generation: 0 };
            mount.write(files);
            mount
        }

        fn path(&self, name: &str) -> String {
            self.dir.join(name).to_string_lossy().into_owned()
        }

        /// Publish `files` together; earlier files are dropped, as in a new Secret version.
        fn write(&mut self, files: &[(&str, &str)]) {
            self.generation = self.generation.saturating_add(1);
            let version = format!("..{}", self.generation);
            std::fs::create_dir(self.dir.join(&version)).expect("version dir");
            for (name, content) in files {
                std::fs::write(self.dir.join(&version).join(name), content).expect("write");
                let link = self.dir.join(name);
                if std::fs::symlink_metadata(&link).is_err() {
                    std::os::unix::fs::symlink(format!("..data/{name}"), &link).expect("file link");
                }
            }
            let staged = self.dir.join("..data_tmp");
            std::os::unix::fs::symlink(&version, &staged).expect("data link");
            std::fs::rename(&staged, self.dir.join("..data")).expect("swap");
        }
    }

    impl Drop for Mount {
        fn drop(&mut self) {
            let _removed = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn front(snapshot: &ArcSwap<RouteSnapshot>) -> Option<String> {
        snapshot.load().candidates.first().map(|c| c.site.to_string())
    }

    /// Wait up to 5s for `done`.
    fn eventually(what: &str, done: impl Fn() -> bool) {
        let deadline = Instant::now().checked_add(Duration::from_secs(5)).expect("deadline");
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn runtime(peers: &Peers, initial: &GridServingConfig) -> crate::GridRuntime {
        crate::serving::start_runtime(initial, peers.starter()).expect("runtime starts")
    }

    #[test]
    fn a_joining_peer_is_polled_and_routed() {
        let peers = Peers::default();
        peers.set_load("east", 50.0);
        peers.set_load("west", 5.0);
        let grid = runtime(&peers, &config(&["east"]));
        let snapshot = grid.snapshot();
        eventually("east polled", || peers.fetches("east") > 0);
        assert_eq!(sites(&snapshot.load()), ["east"]);

        let outcome = grid.reload(&config(&["east", "west"])).expect("reload");
        assert_eq!(
            outcome,
            Some(ReloadOutcome {
                started: 1,
                stopped: 0,
                kept: 1
            })
        );
        eventually("west polled", || peers.fetches("west") > 0);
        eventually("idle west routed first", || front(&snapshot).as_deref() == Some("west"));
        assert_eq!(peers.starts("east"), 1, "the unchanged peer kept its poller");
    }

    #[test]
    fn a_leaving_peer_is_dropped_and_stops_polling() {
        let peers = Peers::default();
        let grid = runtime(&peers, &config(&["east", "west"]));
        eventually("west polled", || peers.fetches("west") > 0);

        let outcome = grid.reload(&config(&["east"])).expect("reload");
        assert_eq!(
            outcome,
            Some(ReloadOutcome {
                started: 0,
                stopped: 1,
                kept: 1
            })
        );
        assert_eq!(sites(&grid.snapshot().load()), ["east"], "west is no longer routable");

        // The poller thread winds down on its own after the stop, so wait for the count to hold.
        eventually("west's poller stopped", || {
            let seen = peers.fetches("west");
            std::thread::sleep(Duration::from_millis(100));
            peers.fetches("west") == seen
        });
        let east = peers.fetches("east");
        eventually("east still polled", || peers.fetches("east") > east);
    }

    #[test]
    fn only_a_changed_peer_is_rebuilt() {
        let peers = Peers::default();
        let grid = runtime(&peers, &config(&["east", "west"]));

        let mut moved = config(&["east", "west"]);
        moved.peers[1].addr = "10.0.0.9:9091".to_owned();
        moved.peers[1].pins = vec!["ab".repeat(32)];
        let outcome = grid.reload(&moved).expect("reload");
        assert_eq!(
            outcome,
            Some(ReloadOutcome {
                started: 1,
                stopped: 1,
                kept: 1
            })
        );
        assert_eq!(peers.starts("west"), 2, "the moved peer was rebuilt");
        assert_eq!(peers.starts("east"), 1, "the unchanged peer was kept");
        assert_eq!(
            grid.reload(&moved).expect("reload"),
            None,
            "an unchanged config is a no-op"
        );
    }

    #[test]
    fn a_new_load_window_restarts_the_pollers() {
        let peers = Peers::default();
        let grid = runtime(&peers, &config(&["east", "west"]));
        let mut widened = config(&["east", "west"]);
        widened.load_window_ms = 60_000;
        assert_eq!(
            grid.reload(&widened).expect("reload"),
            Some(ReloadOutcome {
                started: 2,
                stopped: 2,
                kept: 0
            }),
            "every poller restarts under the new freshness bound"
        );
        assert_eq!((peers.starts("east"), peers.starts("west")), (2, 2));
        assert_eq!(
            grid.reload(&widened).expect("reload"),
            None,
            "the same bound restarts nothing"
        );
    }

    #[test]
    fn an_invalid_config_keeps_the_last_good_one() {
        let peers = Peers::default();
        let mut control = Control::new(&config(&["east"]), peers.starter()).expect("control");
        control.apply(&config(&["east"])).expect("first apply");
        // The topology, not the snapshot: a poller's scrape re-publishes the snapshot.
        let before = control.topology.load_full();

        let mut bad_candidate = config(&["east", "west"]);
        bad_candidate.candidates[1].name = String::new();
        let mut twice = config(&["east", "west"]);
        twice.peers[1].site = "east".to_owned();
        let mut zero = config(&["east", "west"]);
        zero.peers[1].interval_ms = 0;
        for bad in [bad_candidate, twice, zero] {
            control.apply(&bad).expect_err("an invalid config is rejected");
        }

        assert!(
            Arc::ptr_eq(&before, &control.topology.load_full()),
            "a rejected config publishes no topology"
        );
        assert_eq!(
            sites(&control.snapshot().load()),
            ["east"],
            "the routable sites are unchanged"
        );
        assert_eq!(peers.starts("west"), 0, "no poller started for a rejected config");
        assert_eq!(peers.starts("east"), 1);
    }

    #[test]
    fn a_failed_poller_start_starts_nothing() {
        let peers = Peers::default();
        let ok = peers.starter();
        let start: StartPeer = Box::new(move |peer, poller, store, refresh| {
            if peer.site == "north" {
                return Err("no route to north".into());
            }
            ok(peer, poller, store, refresh)
        });
        let grid = crate::serving::start_runtime(&config(&["east"]), start).expect("runtime starts");

        grid.reload(&config(&["east", "west", "north"]))
            .expect_err("a peer that cannot start rejects the reload");
        assert_eq!(sites(&grid.snapshot().load()), ["east"], "the old topology stays");
        assert_eq!(
            grid.reload(&config(&["east"])).expect("reload"),
            None,
            "east is still the applied config"
        );
    }

    /// One site reporting a fixed load, counting its fetches.
    struct FixedSource {
        site: String,
        value: f64,
        count: Arc<AtomicUsize>,
    }

    impl SignalSource for FixedSource {
        async fn fetch(&self) -> Result<Scrape, FetchError> {
            self.count.fetch_add(1, Ordering::SeqCst);
            let at = now_ms();
            Ok(Scrape {
                body: format!(
                    r#"{LOAD_METRIC}{{grid_site="{site}",grid_provider="pool-{site}"}} {value} {at}"#,
                    site = self.site,
                    value = self.value
                ),
                date_ms: at,
                peer_identity: Arc::from(certs::spiffe_id(&self.site).as_str()),
            })
        }
    }

    /// The worst load the store holds for `site` over the last minute.
    fn stored(store: &LoadStore, site: &str) -> Option<f64> {
        store.window_worst(
            &LoadStore::key(site, &format!("pool-{site}")),
            LOAD_METRIC,
            now_ms(),
            60_000,
            true,
        )
    }

    #[test]
    fn a_rejected_reload_leaves_the_order_and_the_store_as_they_were() {
        const CHANGED_INTERVAL_MS: u64 = 2_001;
        let peers = Peers::default();
        peers.set_load("east", 0.9);
        peers.set_load("west", 0.1);
        let ok = peers.starter();
        let reported = Arc::new(AtomicUsize::new(0));
        let changed = Arc::clone(&reported);
        let start: StartPeer = Box::new(move |peer, poller, store, refresh| {
            if peer.site == "north" {
                // Fail only once the changed west has reported, as a slow later start would.
                let deadline = Instant::now().checked_add(Duration::from_secs(5)).expect("deadline");
                while changed.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(5));
                }
                return Err("no route to north".into());
            }
            if peer.site == "west" && peer.interval_ms == CHANGED_INTERVAL_MS {
                let source = FixedSource {
                    site: "west".to_owned(),
                    value: 50.0,
                    count: Arc::clone(&changed),
                };
                let fast = PollerConfig {
                    interval_ms: 10,
                    ..poller.clone()
                };
                return spawn_on_thread_held(store, &fast, source, refresh)
                    .map_err(|error| -> FilterError { error.to_string().into() });
            }
            ok(peer, poller, store, refresh)
        });
        let initial = config(&["east", "west"]);
        let mut control = Control::new(&initial, start).expect("control");
        control.apply(&initial).expect("apply");
        let snapshot = control.snapshot();
        eventually("west, the idle site, first", || {
            front(&snapshot).as_deref() == Some("west")
        });
        let before = sites(&snapshot.load());

        let mut next = config(&["east", "west", "north"]);
        for peer in next.peers.iter_mut().filter(|peer| peer.site == "west") {
            peer.interval_ms = CHANGED_INTERVAL_MS;
        }
        control.apply(&next).expect_err("north cannot start");
        assert!(
            reported.load(Ordering::SeqCst) > 0,
            "the changed west reported before the reject"
        );
        std::thread::sleep(Duration::from_millis(100));

        assert_eq!(sites(&snapshot.load()), before, "the active order is unchanged");
        assert_eq!(
            stored(&control.store, "west"),
            Some(0.1),
            "the store holds only the running west's load"
        );
    }

    /// The snapshot a test publishes for a recording refresh to read.
    type Published = Arc<Mutex<Option<Arc<ArcSwap<RouteSnapshot>>>>>;

    #[test]
    fn a_new_pollers_first_scrape_is_ordered_against_the_new_topology() {
        let peers = Peers::default();
        peers.set_load("east", 0.9);
        let ok = peers.starter();
        let published: Published = Arc::default();
        let at_first_cycle: Arc<Mutex<Option<Vec<String>>>> = Arc::default();
        let (cell, seen) = (Arc::clone(&published), Arc::clone(&at_first_cycle));
        let start: StartPeer = Box::new(move |peer, poller, store, refresh| {
            if peer.site != "west" {
                return ok(peer, poller, store, refresh);
            }
            let fetched = Arc::new(AtomicUsize::new(0));
            let source = FixedSource {
                site: "west".to_owned(),
                value: 0.0,
                count: Arc::clone(&fetched),
            };
            let (cell, seen) = (Arc::clone(&cell), Arc::clone(&seen));
            let recording: Refresh = Box::new(move |scraped: &LoadStore| {
                let mut first = seen.lock().expect("seen");
                if first.is_none() {
                    *first = cell
                        .lock()
                        .expect("cell")
                        .as_ref()
                        .map(|snapshot| sites(&snapshot.load()));
                }
                drop(first);
                refresh(scraped);
            });
            // One scrape only, already fetched when the reload commits.
            let once = PollerConfig {
                interval_ms: 60_000,
                ..poller.clone()
            };
            let handle = spawn_on_thread_held(store, &once, source, recording)
                .map_err(|error| -> FilterError { error.to_string().into() })?;
            eventually("west fetched while held", || fetched.load(Ordering::SeqCst) > 0);
            Ok(handle)
        });
        let initial = config(&["east"]);
        let mut control = Control::new(&initial, start).expect("control");
        control.apply(&initial).expect("apply");
        *published.lock().expect("cell") = Some(control.snapshot());

        // Hold the publish lock so a poller committed before the publish would refresh, and record,
        // while the old topology is still the published one.
        let swap = Arc::clone(&control.swap);
        let held = swap.lock().expect("swap");
        std::thread::scope(|scope| {
            let reload = scope.spawn(|| control.apply(&config(&["east", "west"])));
            std::thread::sleep(Duration::from_millis(200));
            drop(held);
            reload.join().expect("reload thread").expect("reload");
        });

        eventually("west's first refresh", || {
            at_first_cycle.lock().expect("seen").is_some()
        });
        let first = at_first_cycle.lock().expect("seen").clone().unwrap_or_default();
        assert!(
            first.contains(&"west".to_owned()),
            "the first refresh saw the new topology: {first:?}"
        );
        eventually("west, idle, first", || {
            front(&control.snapshot()).as_deref() == Some("west")
        });
    }

    #[test]
    fn an_in_flight_request_finishes_on_the_snapshot_it_loaded() {
        let peers = Peers::default();
        let grid = runtime(&peers, &config(&["east", "west"]));
        let snapshot = grid.snapshot();
        let in_flight = snapshot.load_full();

        grid.reload(&config(&["east"])).expect("reload");

        assert_eq!(members(&in_flight), ["east", "west"], "the request keeps its snapshot");
        assert_eq!(sites(&snapshot.load()), ["east"], "the next request sees the new one");
    }

    #[test]
    fn a_refresh_that_raced_a_reload_is_dropped() {
        let first = Arc::new(ArcSwap::from_pointee(
            Topology::from_config(&config(&["east"])).expect("topology"),
        ));
        let snapshot = Arc::new(ArcSwap::from_pointee(RouteSnapshot::from_static(
            Vec::new(),
            Arc::from("local"),
        )));
        let reloaded = Arc::new(Topology::from_config(&config(&["west"])).expect("topology"));
        let refresh = {
            let topology = Arc::clone(&first);
            make_refresh(
                Arc::clone(&first),
                Arc::clone(&snapshot),
                Arc::new(Mutex::new(())),
                move || {
                    // The reload lands while this cycle is ordering.
                    topology.store(Arc::clone(&reloaded));
                    1_000
                },
            )
        };

        refresh(&LoadStore::new(Duration::from_secs(60)));
        assert!(sites(&snapshot.load()).is_empty(), "the stale order is not stored");
    }

    #[test]
    fn a_load_change_reorders_the_snapshot_without_a_config_rebuild() {
        let topology = Arc::new(ArcSwap::from_pointee(
            Topology::from_config(&config(&["east", "west"])).expect("topology"),
        ));
        let snapshot = Arc::new(ArcSwap::from_pointee(RouteSnapshot::from_static(
            Vec::new(),
            Arc::from("local"),
        )));
        let clock = Arc::new(AtomicI64::new(1_000));
        let refresh = {
            let clock = Arc::clone(&clock);
            make_refresh(topology, Arc::clone(&snapshot), Arc::new(Mutex::new(())), move || {
                clock.load(Ordering::SeqCst)
            })
        };
        let line = |site: &str, value: f64, at: i64| {
            format!(r#"{LOAD_METRIC}{{grid_site="{site}",grid_provider="pool-{site}"}} {value} {at}"#)
        };
        let store = LoadStore::new(Duration::from_secs(600));

        store.ingest_at(&line("east", 90.0, 1_000), 1_000, 1_000, "east");
        store.ingest_at(&line("west", 10.0, 1_000), 1_000, 1_000, "west");
        refresh(&store);
        assert_eq!(front(&snapshot).as_deref(), Some("west"), "the idle site sorts first");

        clock.store(40_000, Ordering::SeqCst);
        store.ingest_at(&line("east", 5.0, 40_000), 40_000, 40_000, "east");
        store.ingest_at(&line("west", 95.0, 40_000), 40_000, 40_000, "west");
        refresh(&store);
        assert_eq!(
            front(&snapshot).as_deref(),
            Some("east"),
            "the swap tracks the load change"
        );
    }

    #[test]
    fn the_watch_applies_a_rewrite_and_ignores_a_bad_file() {
        let path = std::env::temp_dir().join(format!("grid-serving-{}.yaml", std::process::id()));
        let write = |config: &str| {
            // Write then rename, as the kubelet's symlink swap replaces the file whole.
            let staged = path.with_extension("tmp");
            std::fs::write(&staged, config).expect("write");
            std::fs::rename(&staged, &path).expect("rename");
        };
        let yaml = |sites: &[&str]| serde_yaml::to_string(&serde_yaml_value(&config(sites))).expect("yaml");
        write(&yaml(&["east"]));

        let peers = Peers::default();
        let mut grid = runtime(&peers, &config(&["east"]));
        grid.watch(&path, Duration::from_millis(20)).expect("watch");
        let snapshot = grid.snapshot();
        let counts = || grid.watcher().expect("watching").counts();
        eventually("the startup file seen", || counts().reused() == 1);

        write("local_site: [not, a, site\n");
        eventually("the bad file handled", || counts().rejected() == 1);
        assert_eq!(counts().applied(), 0, "the bad file was not applied");
        assert_eq!(
            sites(&snapshot.load()),
            ["east"],
            "a bad file keeps the last good config"
        );

        write(&yaml(&["east", "west"]));
        eventually("the rewrite handled", || counts().applied() == 1);
        assert_eq!(members(&snapshot.load()), ["east", "west"], "the rewrite applied");
        eventually("west polled", || peers.fetches("west") > 0);

        drop(grid);
        std::fs::remove_file(&path).expect("cleanup");
    }

    #[test]
    fn a_renewed_identity_restarts_the_pollers_with_no_config_change() {
        let mut identity = Mount::new(
            "grid-identity",
            &[("ca.pem", "old"), ("tls.crt", "old"), ("tls.key", "old")],
        );
        let mut serving = config(&["east"]);
        serving.peers[0].grid_ca_path = identity.path("ca.pem");
        serving.peers[0].client_cert_path = identity.path("tls.crt");
        serving.peers[0].client_key_path = identity.path("tls.key");
        let yaml = serde_yaml::to_string(&serde_yaml_value(&serving)).expect("yaml");
        let file = Mount::new("grid-identity-serving", &[("serving.yaml", &yaml)]);
        let path = file.path("serving.yaml");

        let peers = Peers::default();
        let mut grid = runtime(&peers, &serving);
        assert_eq!(
            grid.reload(&serving).expect("reload"),
            None,
            "the same identity reuses the pollers"
        );
        grid.watch(&path, Duration::from_millis(20)).expect("watch");
        let counts = || grid.watcher().expect("watching").counts();
        eventually("the startup file seen", || counts().reused() == 1);

        identity.write(&[("ca.pem", "old"), ("tls.crt", "renewed"), ("tls.key", "renewed")]);
        eventually("the renewal applied", || counts().applied() == 1);
        assert_eq!(peers.starts("east"), 2, "the poller restarted on the renewed identity");
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(counts().applied(), 1, "a settled identity restarts nothing more");

        drop(grid);
    }

    #[test]
    fn an_identity_read_during_a_rotation_sees_one_version() {
        let old = [("ca.pem", "ca"), ("tls.crt", "old"), ("tls.key", "old")];
        let new = [("ca.pem", "ca"), ("tls.crt", "new"), ("tls.key", "new")];
        let mut identity = Mount::new("grid-torn", &old);
        let mut serving = config(&["east"]);
        serving.peers[0].grid_ca_path = identity.path("ca.pem");
        serving.peers[0].client_cert_path = identity.path("tls.crt");
        serving.peers[0].client_key_path = identity.path("tls.key");
        let old_digest = identity_digest(&serving);
        identity.write(&new);
        let new_digest = identity_digest(&serving);

        // Each swap waits for two reads after the last, so no read spans two swaps,
        // however long the reader stalls. A real rotation is hours apart.
        let reads = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&reads);
        let rotating = std::thread::spawn(move || {
            for round in 0..40 {
                let after = counted.load(Ordering::SeqCst).saturating_add(2);
                let deadline = Instant::now().checked_add(Duration::from_secs(5)).expect("deadline");
                while counted.load(Ordering::SeqCst) < after && Instant::now() < deadline {
                    std::thread::yield_now();
                }
                identity.write(if round % 2 == 0 { &old } else { &new });
            }
            identity
        });
        while !rotating.is_finished() {
            let digest = identity_digest(&serving);
            assert!(
                digest == old_digest || digest == new_digest,
                "read {} mixed two versions of the identity",
                reads.load(Ordering::SeqCst)
            );
            reads.fetch_add(1, Ordering::SeqCst);
        }
        drop(rotating.join().expect("rotation"));
    }

    #[test]
    fn a_change_that_fails_on_unreadable_identity_is_retried_until_it_applies() {
        let mut identity = Mount::new("grid-retry", &[]);
        let cert = PathBuf::from(identity.path("tls.crt"));
        let peers = Peers::default();
        let ok = peers.starter();
        let watched = cert.clone();
        let start: StartPeer = Box::new(move |peer, poller, store, refresh| {
            if peer.client_cert_path == watched.to_string_lossy() && std::fs::metadata(&watched).is_err() {
                return Err(format!("grid: reading {}", peer.client_cert_path).into());
            }
            ok(peer, poller, store, refresh)
        });
        let mut grid = crate::serving::start_runtime(&config(&["west"]), start).expect("runtime starts");
        let mut serving = config(&["west", "east"]);
        serving.peers[1].client_cert_path = cert.to_string_lossy().into_owned();
        let yaml = serde_yaml::to_string(&serde_yaml_value(&serving)).expect("yaml");
        let file = Mount::new("grid-retry-serving", &[("serving.yaml", &yaml)]);
        let path = file.path("serving.yaml");

        grid.watch(&path, Duration::from_millis(20)).expect("watch");
        let counts = || grid.watcher().expect("watching").counts();
        eventually("the change retried", || counts().rejected() >= 2);
        assert_eq!(sites(&grid.snapshot().load()), ["west"], "the old config stays");

        identity.write(&[("tls.crt", "issued")]);
        eventually("the pending change applied", || counts().applied() == 1);
        assert_eq!(
            members(&grid.snapshot().load()),
            ["east", "west"],
            "the pending change applied"
        );

        drop(grid);
    }

    #[test]
    fn a_refused_file_neither_blocks_renewal_nor_repeats_its_rejection() {
        let mut identity = Mount::new(
            "grid-refused",
            &[("ca.pem", "old"), ("tls.crt", "old"), ("tls.key", "old")],
        );
        let mut serving = config(&["east"]);
        serving.peers[0].grid_ca_path = identity.path("ca.pem");
        serving.peers[0].client_cert_path = identity.path("tls.crt");
        serving.peers[0].client_key_path = identity.path("tls.key");
        let yaml = |config: &GridServingConfig| serde_yaml::to_string(&serde_yaml_value(config)).expect("yaml");
        let mut file = Mount::new("grid-refused-serving", &[("serving.yaml", &yaml(&serving))]);
        let path = file.path("serving.yaml");

        let peers = Peers::default();
        let mut grid = runtime(&peers, &serving);
        grid.watch(&path, Duration::from_millis(20)).expect("watch");
        let counts = || grid.watcher().expect("watching").counts();
        eventually("the startup file seen", || counts().reused() == 1);

        let mut twice = config(&["east", "west"]);
        twice.peers[1].site = "east".to_owned();
        file.write(&[("serving.yaml", &yaml(&twice))]);
        eventually("the duplicate site refused", || counts().rejected() == 1);
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            counts().rejected(),
            1,
            "a refused file is rejected once, not every tick"
        );
        assert_eq!(peers.starts("east"), 1, "a refused file starts and drops no poller");

        identity.write(&[("ca.pem", "old"), ("tls.crt", "renewed"), ("tls.key", "renewed")]);
        eventually("the running config renewed", || counts().applied() == 1);
        assert_eq!(
            peers.starts("east"),
            2,
            "the running poller restarted on the renewed identity"
        );
        assert_eq!(
            sites(&grid.snapshot().load()),
            ["east"],
            "the refused file never applied"
        );

        drop(grid);
    }

    /// `config` in the operator's serving-config shape.
    fn serde_yaml_value(config: &GridServingConfig) -> serde_yaml::Value {
        let peers: Vec<serde_yaml::Value> = config
            .peers
            .iter()
            .map(|p| {
                serde_yaml::from_str(&format!(
                    "{{site: {}, addr: '{}', server_name: {}, authority: {}, grid_ca_path: {}, client_cert_path: {}, client_key_path: {}}}",
                    p.site, p.addr, p.server_name, p.authority, p.grid_ca_path, p.client_cert_path, p.client_key_path
                ))
                .expect("peer")
            })
            .collect();
        let candidates: Vec<serde_yaml::Value> = config
            .candidates
            .iter()
            .map(|c| {
                serde_yaml::from_str(&format!(
                    "{{kind: inference_model, name: {}, site: {}, cluster: {}}}",
                    c.name, c.site, c.cluster
                ))
                .expect("candidate")
            })
            .collect();
        let mut root = serde_yaml::Mapping::new();
        root.insert("local_site".into(), config.local_site.clone().into());
        root.insert("window_secs".into(), config.window_secs.into());
        root.insert("load_window_ms".into(), config.load_window_ms.into());
        root.insert("candidates".into(), candidates.into());
        root.insert("peers".into(), peers.into());
        root.into()
    }
}
