//! Applies a grid serving config: the candidate topology and the peer pollers.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock, PoisonError, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use grid_signals::{LoadStore, now_ms};
use grid_signals_client::{PollHandle, PollerConfig};
use praxis_filter::FilterError;

use crate::{
    descriptor::{RouteCandidate, validate_local_site, validate_provider_hop_clusters, validate_serving_candidates},
    health::ClusterHealth,
    pin::TagKey,
    prefix::{AffinitySettings, PrefixAffinity},
    serving::{AvailabilitySettings, GridServingConfig, PeerServingConfig, validate_peer},
    snapshot::{self, Gauged, RouteSnapshot},
};

/// The order step a poller runs after each scrape.
pub(crate) type Refresh = Box<dyn Fn(&LoadStore) + Send>;

/// Starts one peer's poller feeding `store`, running `refresh` each cycle.
pub(crate) type StartPeer =
    Box<dyn Fn(&PeerServingConfig, &PollerConfig, Arc<LoadStore>, Refresh) -> Result<PollHandle, FilterError> + Send>;

/// The `grid_site_route` filter's tuning, set from its praxis config block and read by the
/// control step. The serving config is routing data the operator writes, so plugin tuning
/// lives here, beside the plugin, not there.
#[derive(Default)]
pub struct Tuning {
    /// The availability settings every refresh orders with.
    availability: ArcSwap<AvailabilitySettings>,

    /// The prefix affinity settings each serving apply adopts.
    affinity: ArcSwap<AffinitySettings>,

    /// Bumped on every `set`, so a serving re-read notices a filter configured after it applied.
    generation: AtomicU64,

    /// The control plane to re-apply the running config through on `set`, once it runs.
    control: OnceLock<Weak<Mutex<Control>>>,
}

impl Tuning {
    /// Make `availability` and `affinity` the current tuning, and adopt them into the running config at once.
    pub(crate) fn set(&self, availability: AvailabilitySettings, affinity: AffinitySettings) {
        self.availability.store(Arc::new(availability));
        self.affinity.store(Arc::new(affinity));
        self.generation.fetch_add(1, Ordering::Release);
        if let Some(control) = self.control.get().and_then(Weak::upgrade) {
            control.lock().unwrap_or_else(PoisonError::into_inner).readopt();
        }
    }

    /// Re-apply the running config through `control` whenever the tuning is set. Attached once,
    /// by the runtime that owns the control; a second attach is ignored.
    pub(crate) fn attach(&self, control: Weak<Mutex<Control>>) {
        let _attached = self.control.set(control);
    }

    /// The current availability settings.
    pub(crate) fn availability(&self) -> AvailabilitySettings {
        **self.availability.load()
    }

    /// The current prefix affinity settings.
    pub(crate) fn affinity(&self) -> Arc<AffinitySettings> {
        self.affinity.load_full()
    }

    /// How many times the tuning has been set.
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}

/// The validated candidate topology the refresh orders.
pub(crate) struct Topology {
    /// Candidates in config order.
    base: Arc<[RouteCandidate]>,

    /// This gateway's own site.
    local_site: Arc<str>,

    /// Freshness window the order reads, milliseconds.
    load_window_ms: i64,

    /// Explicitly authenticated provider-gateway hop clusters.
    provider_hop_clusters: Arc<BTreeSet<String>>,
}

impl Topology {
    /// Validate the topology half of `config`.
    fn from_config(config: &GridServingConfig) -> Result<Self, FilterError> {
        validate_local_site(&config.local_site)?;
        let base = validate_serving_candidates(config.candidates.clone())?;
        let provider_hop_clusters = validate_provider_hop_clusters(config.provider_hop_clusters.clone())?;
        for candidate in &config.candidates {
            if provider_hop_clusters.contains(&candidate.cluster) && candidate.stable_id.is_none() {
                return Err(format!(
                    "grid: candidate '{}' on provider-hop cluster '{}' is missing stable_id",
                    candidate.name, candidate.cluster
                )
                .into());
            }
        }
        Ok(Self {
            base: Arc::from(base),
            local_site: Arc::from(config.local_site.as_str()),
            load_window_ms: config.load_window_ms,
            provider_hop_clusters: Arc::new(provider_hop_clusters),
        })
    }

    /// The snapshot ordered from `store` at `now` under `availability`, down clusters and peer
    /// gateways last, with the models to shed given the set `shedding` the previous snapshot shed.
    #[expect(
        clippy::too_many_arguments,
        reason = "each input is a distinct piece of control state"
    )]
    fn order(
        &self,
        store: &LoadStore,
        now: i64,
        health: &ClusterHealth,
        shedding: &BTreeSet<Arc<str>>,
        gauged: &mut Gauged,
        availability: &AvailabilitySettings,
    ) -> RouteSnapshot {
        let mut inputs = snapshot::Inputs {
            signals: store,
            now_ms: now,
            window_ms: self.load_window_ms,
            availability,
            learned: &mut gauged.learned,
        };
        let mut ordered = RouteSnapshot::from_store(
            self.base.iter().cloned().collect(),
            Arc::clone(&self.local_site),
            &mut inputs,
        );
        ordered.provider_hop_clusters = Arc::clone(&self.provider_hop_clusters);
        // Praxis demotes a cluster only once it has reported health. Without a registry the
        // gateway knows nothing about backends, so it demotes nothing rather than guessing.
        let ordered = if health.observed() {
            ordered.demote(&health.down())
        } else {
            ordered
        };
        ordered.shed(shedding, availability).published(&mut gauged.published)
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

    /// Serializes snapshot stores between the refreshes and a reload, and holds the
    /// site/cluster pairs with a published score.
    swap: Arc<Mutex<Gauged>>,

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
    /// Backend clusters with no healthy endpoint, ordered last on every refresh.
    health: Arc<ClusterHealth>,

    /// The filter's tuning, read on every refresh and adopted with each config.
    tuning: Arc<Tuning>,

    /// The tuning generation the applied config adopted.
    tuned: u64,

    /// Verified TLS identities from the loaded Praxis backend configuration.
    backend_tls: BTreeMap<String, String>,
}

impl Control {
    /// Build the control plane for `config` without starting any poller.
    #[cfg(test)]
    pub(crate) fn new(config: &GridServingConfig, start: StartPeer) -> Result<Self, FilterError> {
        Self::new_with_backend_tls(config, start, BTreeMap::new())
    }

    /// Build with the verified backend identities that serving reloads must preserve.
    pub(crate) fn new_with_backend_tls(
        config: &GridServingConfig,
        start: StartPeer,
        backend_tls: BTreeMap<String, String>,
    ) -> Result<Self, FilterError> {
        validate_provider_hop_binding(config, &backend_tls)?;
        let topology = Topology::from_config(config)?;
        // Cold start: config order until the first poll re-orders it by live load.
        let mut gauged = Gauged::new();
        let mut cold_start = RouteSnapshot::from_static(
            topology.base.iter().cloned().collect(),
            Arc::clone(&topology.local_site),
        )
        .published(&mut gauged.published);
        cold_start.provider_hop_clusters = Arc::clone(&topology.provider_hop_clusters);
        Ok(Self {
            store: Arc::new(LoadStore::with_combine(
                Duration::from_secs(config.window_secs),
                crate::signals::llm_d::combine,
            )),
            snapshot: Arc::new(ArcSwap::from_pointee(cold_start)),
            topology: Arc::new(ArcSwap::from_pointee(topology)),
            swap: Arc::new(Mutex::new(gauged)),
            window_secs: config.window_secs,
            peers: HashMap::new(),
            applied: None,
            identity: None,
            start,
            affinity: Arc::default(),
            health: Arc::default(),
            tuning: Arc::default(),
            tuned: 0,
            backend_tls,
        })
    }

    /// The cluster health the route filter publishes into and every refresh reads.
    pub(crate) fn health(&self) -> Arc<ClusterHealth> {
        Arc::clone(&self.health)
    }

    /// The live load store.
    pub(crate) fn store(&self) -> Arc<LoadStore> {
        Arc::clone(&self.store)
    }

    /// The snapshot the filter reads.
    pub(crate) fn snapshot(&self) -> Arc<ArcSwap<RouteSnapshot>> {
        Arc::clone(&self.snapshot)
    }

    /// The prefix index and affinity settings the route filter reads.
    pub(crate) fn affinity(&self) -> Arc<PrefixAffinity> {
        Arc::clone(&self.affinity)
    }

    /// The filter's tuning, set when the filter is built.
    pub(crate) fn tuning(&self) -> Arc<Tuning> {
        Arc::clone(&self.tuning)
    }

    /// Make `config` the running one: its identity, the tuning's affinity settings at
    /// `generation`, and only its clusters' prefixes.
    fn adopt(&mut self, config: &GridServingConfig, identity: [u8; 32], tag_key: Option<TagKey>, generation: u64) {
        let clusters: Vec<Arc<str>> = config
            .candidates
            .iter()
            .map(|candidate| Arc::from(candidate.cluster.as_str()))
            .collect();
        self.affinity
            .apply(self.tuning.affinity().as_ref().clone(), tag_key, &clusters);
        self.applied = Some(config.clone());
        self.identity = Some(identity);
        self.tuned = generation;
    }

    /// Re-apply the running config under the current tuning, keeping it as it was on an error.
    fn readopt(&mut self) {
        let Some(config) = self.applied.clone() else {
            return;
        };
        if let Err(error) = self.apply(&config) {
            tracing::warn!(%error, "grid: filter tuning rejected against the running config; keeping the last adopted");
        }
    }

    /// Validate `config` fully, then swap it in. `None` when already applied.
    ///
    /// # Errors
    ///
    /// Returns [`FilterError`] for an invalid config or a poller that cannot start, changing nothing.
    pub(crate) fn apply(&mut self, config: &GridServingConfig) -> Result<Option<ReloadOutcome>, FilterError> {
        let identity = identity_digest(config, &self.tuning.affinity());
        self.apply_with(config, identity)
    }

    /// [`Self::apply`] with the identity digest already computed.
    fn apply_with(
        &mut self,
        config: &GridServingConfig,
        identity: [u8; 32],
    ) -> Result<Option<ReloadOutcome>, FilterError> {
        let renewed = self.identity.is_some_and(|applied| applied != identity);
        // A filter configured after the last apply changes the tuning, so the same file re-applies.
        let generation = self.tuning.generation();
        if self.applied.as_ref() == Some(config) && !renewed && self.tuned == generation {
            return Ok(None);
        }
        // Validated before any poller starts, so an invalid config starts and drops nothing.
        validate_provider_hop_binding(config, &self.backend_tls)?;
        let topology = Arc::new(validate_config(config)?);
        let tag_key = load_tag_key(&self.tuning.affinity())?;
        if config.window_secs != self.window_secs {
            tracing::warn!(
                current = self.window_secs,
                requested = config.window_secs,
                "grid: window_secs changes take effect on restart"
            );
        }
        let (next, started) = self.start_changed(config, renewed)?;
        let outcome = self.reconcile(&next, started, renewed, config.load_window_ms);
        self.publish(topology);
        // The reload stands and its topology is published: only now may the new pollers write,
        // so their first refresh orders the new topology. Committing a kept poller is a no-op.
        self.peers.values().for_each(|running| running.handle.commit());
        self.adopt(config, identity, tag_key, generation);
        Ok(Some(outcome))
    }

    /// Order `topology` and publish it with its snapshot.
    fn publish(&self, topology: Arc<Topology>) {
        // Order under the lock, so a concurrent refresh never publishes an older down set or load.
        let mut gauged = self.swap.lock().unwrap_or_else(PoisonError::into_inner);
        let current = self.snapshot.load_full();
        let ordered = Arc::new(topology.order(
            &self.store,
            now_ms(),
            &self.health,
            &current.shedding,
            &mut gauged,
            &self.tuning.availability(),
        ));
        self.topology.store(topology);
        self.snapshot.store(ordered);
        drop(gauged);
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
    pub(crate) fn refresh(&self) -> Refresh {
        make_refresh(
            Arc::clone(&self.topology),
            Arc::clone(&self.snapshot),
            Arc::clone(&self.swap),
            Arc::clone(&self.health),
            Arc::clone(&self.tuning),
            now_ms,
        )
    }
}

/// The stored-state tag key `affinity` names, or `None` when it names none.
pub(crate) fn load_tag_key(affinity: &AffinitySettings) -> Result<Option<TagKey>, FilterError> {
    let Some(path) = affinity.tag_key_path.as_deref() else {
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
    let mut sites = std::collections::HashSet::with_capacity(config.peers.len());
    for peer in &config.peers {
        validate_peer(peer)?;
        if !sites.insert(peer.site.as_str()) {
            return Err(format!("grid: peer site {} appears twice", peer.site).into());
        }
    }
    Ok(topology)
}

/// Bind every declared provider hop to one verified backend and its exact TLS SNI.
fn validate_provider_hop_binding(
    config: &GridServingConfig,
    backend_tls: &BTreeMap<String, String>,
) -> Result<(), FilterError> {
    let clusters = validate_provider_hop_clusters(config.provider_hop_clusters.clone())?;
    if config.provider_hop_sni.len() != clusters.len() {
        return Err("grid: provider-hop SNI declarations must match the allowlist".into());
    }
    for cluster in &clusters {
        let declared = config
            .provider_hop_sni
            .get(cluster)
            .filter(|sni| !sni.trim().is_empty())
            .ok_or_else(|| -> FilterError { format!("grid: missing provider-hop SNI for {cluster}").into() })?;
        if backend_tls.get(cluster) != Some(declared) {
            return Err(format!("grid: provider-hop backend {cluster} lacks matching verified TLS identity").into());
        }
    }
    Ok(())
}

/// Order the current topology under the current availability tuning and swap it in.
///
/// Every refresher orders under `swap`, so the last store always reflects the latest
/// topology, down set, and load.
#[expect(
    clippy::too_many_arguments,
    reason = "each input is a distinct piece of control state"
)]
pub(crate) fn make_refresh<N>(
    topology: Arc<ArcSwap<Topology>>,
    snapshot: Arc<ArcSwap<RouteSnapshot>>,
    swap: Arc<Mutex<Gauged>>,
    health: Arc<ClusterHealth>,
    tuning: Arc<Tuning>,
    now: N,
) -> Refresh
where
    N: Fn() -> i64 + Send + 'static,
{
    Box::new(move |store: &LoadStore| {
        let mut gauged = swap.lock().unwrap_or_else(PoisonError::into_inner);
        let current = snapshot.load_full();
        let ordered = topology.load().order(
            store,
            now(),
            &health,
            &current.shedding,
            &mut gauged,
            &tuning.availability(),
        );
        snapshot.store(Arc::new(ordered));
        drop(gauged);
    })
}

/// A change detector over every identity file `config` names, the grid CA, client
/// certificate, and key, and the tag key `affinity` names. Not a security function.
///
/// Each file is hashed on its own, so no buffer holds the concatenated key.
fn identity_digest(config: &GridServingConfig, affinity: &AffinitySettings) -> [u8; 32] {
    let paths: BTreeSet<&str> = config
        .peers
        .iter()
        .flat_map(|peer| [&peer.grid_ca_path, &peer.client_cert_path, &peer.client_key_path])
        .map(String::as_str)
        // A rotated tag key re-applies the config like a renewed certificate.
        .chain(affinity.tag_key_path.as_deref())
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
        let identity = identity_digest(config, &control.tuning.affinity());
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
        // Nothing changed, so nothing to say at info: the counter still records the attempt.
        Ok(None) => tracing::debug!("grid: serving config unchanged; peer pollers reused"),
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
    use std::{
        sync::atomic::{AtomicBool, AtomicI64},
        time::Instant,
    };

    use grid_signals_client::{FetchError, Scrape, SignalSource, spawn_on_thread_held};

    use super::*;
    use crate::{
        descriptor::{CandidateConfig, CapabilityKind},
        signals::llm_d::QUEUE_METRIC,
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
                    r#"{QUEUE_METRIC}{{grid_site="{site}",grid_provider="pool-{site}"}} {value} {at}"#,
                    site = self.site
                ),
                date_ms: at,
                peer_identity: Arc::from(certs::spiffe_id(&self.site).as_str()),
            })
        }
    }

    fn candidate(site: &str) -> CandidateConfig {
        CandidateConfig {
            admission: crate::descriptor::AdmissionState::default(),
            cluster: format!("pool-{site}"),
            credential: None,
            fresh: true,
            kind: CapabilityKind::InferenceModel,
            name: "llama".to_owned(),
            site: site.to_owned(),
            stable_id: None,
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
            gateway: None,
        }
    }

    /// A config serving `llama` from each of `sites` and polling each of them.
    fn config(sites: &[&str]) -> GridServingConfig {
        GridServingConfig {
            local_site: "local".to_owned(),
            window_secs: 60,
            load_window_ms: 30_000,
            candidates: sites.iter().map(|site| candidate(site)).collect(),
            provider_hop_clusters: Vec::new(),
            provider_hop_sni: BTreeMap::new(),
            peers: sites.iter().map(|site| peer(site)).collect(),
        }
    }

    #[test]
    fn a_tag_key_must_exist_and_be_long_enough() {
        let dir = std::env::temp_dir().join(format!("grid-tag-key-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("test io");
        let tuning = Tuning::default();
        assert!(
            load_tag_key(&tuning.affinity()).expect("loads").is_none(),
            "no path, no key"
        );
        let at = |name: &str| {
            let affinity = AffinitySettings {
                tag_key_path: Some(dir.join(name).display().to_string()),
                ..AffinitySettings::default()
            };
            tuning.set(AvailabilitySettings::default(), affinity);
            tuning.affinity()
        };
        assert!(load_tag_key(&at("missing")).is_err(), "a missing file is refused");
        std::fs::write(dir.join("short"), [1_u8; 31]).expect("test io");
        assert!(load_tag_key(&at("short")).is_err(), "31 bytes is refused");
        std::fs::write(dir.join("key"), [1_u8; 32]).expect("test io");
        assert!(load_tag_key(&at("key")).expect("loads").is_some());
        let _removed = std::fs::remove_dir_all(&dir);
    }

    fn sites(snapshot: &RouteSnapshot) -> Vec<String> {
        snapshot.candidates.iter().map(|c| c.site.to_string()).collect()
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
    fn a_filter_built_after_the_apply_is_adopted_at_once() {
        let peers = Peers::default();
        let grid = runtime(&peers, &config(&["east"]));
        assert_eq!(grid.reload(&config(&["east"])).expect("reload"), None);

        let affinity = AffinitySettings {
            enabled: false,
            ..AffinitySettings::default()
        };
        grid.tuning().set(AvailabilitySettings::default(), affinity);
        assert!(
            !grid.affinity().settings.load().enabled,
            "the filter's affinity settings were adopted on set"
        );
        assert_eq!(
            grid.reload(&config(&["east"])).expect("reload"),
            None,
            "the adopted tuning is a no-op again"
        );
    }

    #[test]
    fn a_tuning_set_before_the_control_runs_is_adopted_on_the_next_apply() {
        let peers = Peers::default();
        let initial = config(&["east"]);
        let mut control = Control::new(&initial, peers.starter()).expect("control");
        control.apply(&initial).expect("apply");

        let affinity = AffinitySettings {
            enabled: false,
            ..AffinitySettings::default()
        };
        control.tuning().set(AvailabilitySettings::default(), affinity);
        assert!(
            control.affinity().settings.load().enabled,
            "nothing to re-apply through yet"
        );
        assert!(
            control.apply(&initial).expect("apply").is_some(),
            "the new tuning re-applies the same config"
        );
        assert!(!control.affinity().settings.load().enabled);
        assert_eq!(control.apply(&initial).expect("apply"), None);
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
        let grid = runtime(&peers, &config(&["east"]));
        let before = grid.snapshot().load_full();

        let mut bad_candidate = config(&["east", "west"]);
        bad_candidate.candidates[1].name = String::new();
        let mut twice = config(&["east", "west"]);
        twice.peers[1].site = "east".to_owned();
        let mut zero = config(&["east", "west"]);
        zero.peers[1].interval_ms = 0;
        for bad in [bad_candidate, twice, zero] {
            grid.reload(&bad).expect_err("an invalid config is rejected");
        }

        // Content, not identity: a poll may republish the same order at any time.
        assert_eq!(
            sites(&before),
            sites(&grid.snapshot().load_full()),
            "the snapshot keeps the last good topology"
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
                    r#"{QUEUE_METRIC}{{grid_site="{site}",grid_provider="pool-{site}"}} {value} {at}"#,
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
            QUEUE_METRIC,
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

        // A poller refresh may reorder the snapshot, so compare the set.
        let mut kept = sites(&in_flight);
        kept.sort();
        assert_eq!(kept, ["east", "west"], "the request keeps its snapshot");
        assert_eq!(sites(&snapshot.load()), ["east"], "the next request sees the new one");
    }

    #[test]
    fn a_refresh_orders_while_holding_the_swap_lock() {
        // Ordering under the lock means a reload or health tick cannot publish between
        // this cycle reading its inputs and storing its order.
        let topology = Arc::new(ArcSwap::from_pointee(
            Topology::from_config(&config(&["east"])).expect("topology"),
        ));
        let snapshot = Arc::new(ArcSwap::from_pointee(RouteSnapshot::from_static(
            Vec::new(),
            Arc::from("local"),
        )));
        let swap = Arc::new(Mutex::new(Gauged::new()));
        let held = Arc::new(AtomicBool::new(false));
        let refresh = {
            let (swap, held) = (Arc::clone(&swap), Arc::clone(&held));
            make_refresh(
                topology,
                Arc::clone(&snapshot),
                Arc::clone(&swap),
                Arc::default(),
                Arc::default(),
                move || {
                    held.store(swap.try_lock().is_err(), Ordering::SeqCst);
                    1_000
                },
            )
        };

        refresh(&LoadStore::new(Duration::from_secs(60)));
        assert!(held.load(Ordering::SeqCst), "ordered outside the swap lock");
        assert_eq!(sites(&snapshot.load()), ["east"]);
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
            make_refresh(
                topology,
                Arc::clone(&snapshot),
                Arc::new(Mutex::new(Gauged::new())),
                Arc::default(),
                Arc::default(),
                move || clock.load(Ordering::SeqCst),
            )
        };
        let line = |site: &str, value: f64, at: i64| {
            format!(r#"{QUEUE_METRIC}{{grid_site="{site}",grid_provider="pool-{site}"}} {value} {at}"#)
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
        assert_eq!(sites(&snapshot.load()), ["east", "west"], "the rewrite applied");
        eventually("west polled", || peers.fetches("west") > 0);

        write(&yaml(&[]));
        eventually("the no-route revision applied", || counts().applied() == 2);
        assert!(snapshot.load().candidates.is_empty(), "the final withdrawal is serving");

        write("local_site: [not, a, site\n");
        eventually("the malformed revision rejected", || counts().rejected() == 2);
        assert!(
            snapshot.load().candidates.is_empty(),
            "malformed updates retain the no-route revision"
        );

        write(&yaml(&["east"]));
        eventually("the restored route applied", || counts().applied() == 3);
        assert_eq!(sites(&snapshot.load()), ["east"], "restoration resumes routing");

        drop(grid);
        std::fs::remove_file(&path).expect("cleanup");
    }

    #[test]
    fn a_renewed_identity_restarts_the_pollers_with_no_config_change() {
        let dir = std::env::temp_dir().join(format!("grid-identity-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let file = |name: &str| dir.join(name).to_string_lossy().into_owned();
        for name in ["ca.pem", "tls.crt", "tls.key"] {
            std::fs::write(file(name), "old").expect("write");
        }
        let mut serving = config(&["east"]);
        serving.peers[0].grid_ca_path = file("ca.pem");
        serving.peers[0].client_cert_path = file("tls.crt");
        serving.peers[0].client_key_path = file("tls.key");
        let path = dir.join("serving.yaml");
        std::fs::write(&path, serde_yaml::to_string(&serde_yaml_value(&serving)).expect("yaml")).expect("write");

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

        // Two plain writes, where a mounted Secret swaps atomically: the watcher may apply
        // once per file, so the test waits for at least one renewal and then for quiet.
        for name in ["tls.crt", "tls.key"] {
            std::fs::write(file(name), "renewed").expect("renew");
        }
        eventually("the renewal applied", || counts().applied() >= 1);
        eventually("the poller restarted on the renewed identity", || {
            peers.starts("east") >= 2
        });
        std::thread::sleep(Duration::from_millis(300));
        let applied = counts().applied();
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(counts().applied(), applied, "a settled identity restarts nothing more");

        drop(grid);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn a_change_that_fails_on_unreadable_identity_is_retried_until_it_applies() {
        let dir = std::env::temp_dir().join(format!("grid-retry-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let cert = dir.join("tls.crt");
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
        let path = dir.join("serving.yaml");
        std::fs::write(&path, serde_yaml::to_string(&serde_yaml_value(&serving)).expect("yaml")).expect("write");

        grid.watch(&path, Duration::from_millis(20)).expect("watch");
        let counts = || grid.watcher().expect("watching").counts();
        eventually("the change retried", || counts().rejected() >= 2);
        assert_eq!(sites(&grid.snapshot().load()), ["west"], "the old config stays");

        std::fs::write(&cert, "issued").expect("issue");
        eventually("the pending change applied", || counts().applied() == 1);
        assert_eq!(sites(&grid.snapshot().load()), ["west", "east"]);

        drop(grid);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn a_refused_file_neither_blocks_renewal_nor_repeats_its_rejection() {
        let dir = std::env::temp_dir().join(format!("grid-refused-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let file = |name: &str| dir.join(name).to_string_lossy().into_owned();
        for name in ["ca.pem", "tls.crt", "tls.key"] {
            std::fs::write(file(name), "old").expect("write");
        }
        let mut serving = config(&["east"]);
        serving.peers[0].grid_ca_path = file("ca.pem");
        serving.peers[0].client_cert_path = file("tls.crt");
        serving.peers[0].client_key_path = file("tls.key");
        let path = dir.join("serving.yaml");
        let write = |config: &GridServingConfig| {
            std::fs::write(&path, serde_yaml::to_string(&serde_yaml_value(config)).expect("yaml")).expect("write");
        };
        write(&serving);

        let peers = Peers::default();
        let mut grid = runtime(&peers, &serving);
        grid.watch(&path, Duration::from_millis(20)).expect("watch");
        let counts = || grid.watcher().expect("watching").counts();
        eventually("the startup file seen", || counts().reused() == 1);

        let mut twice = config(&["east", "west"]);
        twice.peers[1].site = "east".to_owned();
        write(&twice);
        eventually("the duplicate site refused", || counts().rejected() == 1);
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            counts().rejected(),
            1,
            "a refused file is rejected once, not every tick"
        );
        assert_eq!(peers.starts("east"), 1, "a refused file starts and drops no poller");

        for name in ["tls.crt", "tls.key"] {
            std::fs::write(file(name), "renewed").expect("renew");
        }
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
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// `config` in the operator's serving-config shape.
    fn serde_yaml_value(config: &GridServingConfig) -> serde_yaml::Value {
        let peers: Vec<serde_yaml::Value> = config
            .peers
            .iter()
            .map(|p| {
                let mut peer: serde_yaml::Value = serde_yaml::from_str(&format!(
                    "{{site: {}, addr: '{}', server_name: {}, authority: {}, grid_ca_path: {}, client_cert_path: {}, client_key_path: {}}}",
                    p.site, p.addr, p.server_name, p.authority, p.grid_ca_path, p.client_cert_path, p.client_key_path
                ))
                .expect("peer");
                if let (Some(gateway), Some(fields)) = (&p.gateway, peer.as_mapping_mut()) {
                    fields.insert("gateway".into(), gateway.clone().into());
                }
                peer
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

    /// A writer the test subscriber logs into.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("log buffer").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for Captured {
        type Writer = Self;

        fn make_writer(&'writer self) -> Self::Writer {
            self.clone()
        }
    }

    /// What `run` logs at info and above.
    fn info_log(run: impl FnOnce()) -> String {
        let out = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .with_writer(out.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, run);
        String::from_utf8(out.0.lock().expect("log buffer").clone()).expect("utf-8 log")
    }

    #[test]
    fn unchanged_reload_logs_nothing_at_info() {
        let tally = WatchCounts::default();
        let quiet = info_log(|| report(&Ok(None), &tally, "grid: serving config reloaded"));
        assert!(quiet.is_empty(), "a no-op reload logged at info: {quiet}");
        assert_eq!(
            tally.reused.load(Ordering::SeqCst),
            1,
            "the no-op reload is still counted"
        );

        let outcome = ReloadOutcome {
            started: 1,
            ..ReloadOutcome::default()
        };
        let loud = info_log(|| report(&Ok(Some(outcome)), &tally, "grid: serving config reloaded"));
        assert!(
            loud.contains("grid: serving config reloaded"),
            "a real reload logs at info: {loud}"
        );
    }
}
