//! AI Grid operator binary.
//!
//! Runs Kubernetes controllers for [`GridNetwork`], [`GridSite`], and
//! [`InferenceProvider`] resources, and optionally starts a live SWIM
//! membership runtime for peer-to-peer mesh formation.
//!
//! # SWIM configuration
//!
//! Set `GRID_SWIM_BIND_ADDR` (e.g. `"0.0.0.0:7946"`) to enable the SWIM
//! runtime. Set `GRID_SWIM_ADVERTISE_ADDR` when the bind address is not
//! directly reachable by peers, and set `GRID_SWIM_SEEDS` to a comma-separated
//! list of `host:port` seed endpoints. Both literal IP addresses and DNS
//! hostnames are accepted; DNS is resolved once, with a bounded timeout, before
//! the runtime starts. When `GRID_SWIM_BIND_ADDR` is absent
//! the operator runs in static mode (`membership = None`);
//! `GridNetwork.status.connectedSites` and `distributedProviderCount` remain
//! 0, and the phase stays `Pending`/`Initializing` based on TLS configuration
//! only.
//! `GRID_SWIM_SERVICE_NAME` advertises that Service's `LoadBalancer` address instead.
//!
//! # SWIM encryption (environment variable)
//!
//! Set `GRID_SWIM_ENCRYPT_KEY` to a 64-character hex string (32 bytes)
//! to enable AES-256-GCM encryption for all SWIM gossip packets.  When set,
//! packets from peers without the same key are dropped.
//! A malformed value stops the operator.
//!
//! This is the environment-variable path, intended for local development and
//! Kind-based testing.  Environment variables are visible to same-host process
//! inspectors, so the production configuration path uses
//! `GridNetwork.spec.tls.swimKeyRef` to source the key from a Kubernetes
//! Secret; the `GridNetwork` controller loads it and calls
//! `SwimHandle::set_swim_key` at reconcile time.
//! SWIM holds all traffic until a key loads or no `GridNetwork` declares one.
//!
//! The key value is **never** written to logs or tracing spans.
//!
//! [`GridNetwork`]: operator::crd::grid_network::GridNetwork
//! [`GridSite`]: operator::crd::grid_site::GridSite
//! [`InferenceProvider`]: operator::crd::inference_provider::InferenceProvider

#![deny(unsafe_code)]
#![expect(
    clippy::arithmetic_side_effects,
    clippy::min_ident_chars,
    reason = "operator uses short closure params and index arithmetic pervasively"
)]

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::response::IntoResponse as _;
use clap::Parser as _;
use futures::StreamExt as _;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use kube::{
    Api, Client,
    api::{ObjectMeta, PostParams},
    runtime::{controller::Controller, watcher},
};
use operator::{
    cli::Cli,
    controller::{
        agent_tool_provider,
        grid_network::{self, OperatorCtx},
        grid_site, inference_provider,
    },
    crd::{
        agent_tool_provider::AgentToolProvider,
        grid_network::{GridNetwork, SignalMode},
        grid_site::GridSite,
        inference_provider::InferenceProvider,
    },
    gateway,
    resources::tls_backend::{self, ServerTlsConfig},
    served_models, swim_advertise,
    swim_endpoint::{SwimEndpoint, resolve_endpoint, resolve_endpoint_list_partial},
    swim_runtime::{self, RevisionLease, SwimConfig},
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
#[expect(
    clippy::large_stack_frames,
    clippy::too_many_lines,
    reason = "top-level binary with tokio runtime; startup sequence (crypto provider, CLI parsing, \
              SWIM bootstrap, controller fan-out) reads clearer sequential than split further"
)]
async fn main() {
    tracing_subscriber::fmt::init();
    tracing::info!("starting grid-operator");

    // Install the process-wide crypto provider the TLS stack requires, once,
    // up front, before any reconciler builds a client.
    operator::init_process_crypto();

    let config = Cli::parse();

    let client = match Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to create kube client");
            return;
        },
    };

    // Probes answer during enrollment and the LoadBalancer wait.
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let metrics_server = tokio::spawn(run_metrics_server(Arc::clone(&ready)));

    if config.enrollment.enabled
        && let Err(error) = Box::pin(operator::enroll::ensure_enrolled(&client, &config.enrollment)).await
    {
        tracing::error!(%error, "site enrollment failed");
        std::process::exit(1);
    }

    let GridModes {
        signal: signal_mode,
        trust,
    } = match resolve_grid_modes(&client).await {
        Ok(modes) => modes,
        Err(error) => {
            tracing::error!(%error, "failed to resolve grid modes");
            std::process::exit(1);
        },
    };
    let signals_enabled = matches!(signal_mode, SignalMode::Poll);
    let peer_settings = grid_network::PeerSettings {
        local_signals_addr: config.signals.local_addr(),
        trust,
        peer_port: config.signals.peer_port,
    };
    let ctx = Arc::new(
        OperatorCtx::new(client.clone(), None, signal_mode)
            .with_peer_settings(peer_settings)
            .hold_membership(),
    );

    // Controllers run now. Only SWIM and what dials peers wait on the advertise address.
    let (swim_tx, swim_rx) = tokio::sync::watch::channel(SwimStage::Starting);

    // A round of peer polls can be in flight when the pod is told to terminate;
    // the trigger lets it stand down cleanly rather than being dropped mid-await.
    // Only installed with the feature, so the default path keeps today's signal
    // disposition unchanged.
    let (trigger, shutdown) = operator::shutdown::Trigger::new();
    if signals_enabled {
        tokio::spawn(watch_for_termination(trigger));
    } else {
        drop(trigger);
    }

    let result = tokio::try_join!(
        start_swim(
            client.clone(),
            config.clone(),
            Arc::clone(&ctx),
            Arc::clone(&ready),
            swim_tx,
        ),
        run_network_controller(client.clone(), Arc::clone(&ctx), swim_rx.clone()),
        run_site_controller(client.clone()),
        run_provider_controller(client.clone()),
        run_agent_tool_provider_controller(client.clone()),
        async { metrics_server.await? },
        run_signals_server(
            signals_enabled.then(|| SignalsListener {
                bind: config.signals.addr,
                client: client.clone(),
                published: Published {
                    site: ctx.signals(),
                    peers: ctx.peers(),
                },
                peer_identities: ctx.peer_identities(),
                trust,
                max_per_peer: config.signals.max_per_peer,
            }),
            swim_rx.clone(),
        ),
        run_peer_poller(
            signals_enabled.then(|| config.signals.clone()),
            Arc::clone(&ctx),
            swim_rx,
            client.clone(),
            shutdown.clone(),
        ),
        run_local_scraper(
            signals_enabled.then(|| config.signals.scrape_interval()),
            Arc::clone(&ctx),
            client.clone(),
        ),
        run_model_discovery(Arc::clone(&ctx), client.clone()),
    );

    if let Err(e) = result {
        tracing::error!(error = %e, "controller error");
        std::process::exit(1);
    }
}

/// Where SWIM startup stands.
#[derive(Clone)]
enum SwimStage {
    /// Waiting for the advertise address or the runtime.
    Starting,
    /// Done: the runtime, or `None` when SWIM is not configured.
    Settled(Option<Arc<swim_runtime::SwimHandle>>),
}

/// SWIM startup progress, watched by what waits on the advertise address.
type SwimStartup = tokio::sync::watch::Receiver<SwimStage>;

/// Start SWIM off the controllers' path, marking ready once it runs or is not configured.
///
/// # Errors
///
/// Returns the reason SWIM cannot start, which stops the operator.
async fn start_swim(
    client: Client,
    cli: Cli,
    ctx: Arc<OperatorCtx>,
    ready: Arc<std::sync::atomic::AtomicBool>,
    started: tokio::sync::watch::Sender<SwimStage>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let swim = Box::pin(maybe_start_swim(&client, &cli))
        .await
        .map_err(|error| format!("SWIM startup failed: {error}"))?;
    if let Some(handle) = &swim {
        ctx.set_swim(Arc::clone(handle));
        tokio::spawn(gateway::run_discovery_poller(
            client.clone(),
            Arc::clone(handle),
            cli.gateway.clone(),
        ));
        let (handle, ctx) = (Arc::clone(handle), Arc::clone(&ctx));
        tokio::spawn(async move {
            let members = || !handle.snapshot().members.is_empty();
            converged(members, handle.reconciliation_events(), MEMBERSHIP_GRACE).await;
            ctx.release_membership();
        });
    } else {
        ctx.release_membership();
    }
    started.send_replace(SwimStage::Settled(swim));
    ready.store(true, std::sync::atomic::Ordering::Release);
    Ok(())
}

/// Longest wait for a first peer before membership-derived writes run without one.
const MEMBERSHIP_GRACE: std::time::Duration = std::time::Duration::from_secs(15);

/// Wait until `has_members` holds, rechecked on each `changes` item, or `grace` passes.
async fn converged(
    has_members: impl Fn() -> bool,
    changes: impl futures::Stream<Item = ()>,
    grace: std::time::Duration,
) {
    let mut changes = std::pin::pin!(changes);
    let deadline = tokio::time::sleep(grace);
    let mut deadline = std::pin::pin!(deadline);
    while !has_members() {
        tokio::select! {
            () = &mut deadline => return,
            next = changes.next() => if next.is_none() { return },
        }
    }
}

/// The SWIM runtime once startup settles, `None` when none runs.
async fn swim_settled(mut startup: SwimStartup) -> Option<Arc<swim_runtime::SwimHandle>> {
    let stage = startup
        .wait_for(|stage| matches!(stage, SwimStage::Settled(_)))
        .await
        .ok()?
        .clone();
    match stage {
        SwimStage::Settled(swim) => swim,
        SwimStage::Starting => None,
    }
}

/// Grid-wide modes read once from the `GridNetwork` at startup.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct GridModes {
    /// Signal propagation.
    signal: SignalMode,
    /// Peer authorization on the signals path.
    trust: operator::signals::PeerTrustMode,
}

impl GridModes {
    /// The modes `network` declares, defaults for absent fields.
    fn of(network: &GridNetwork) -> Self {
        Self {
            signal: network
                .spec
                .signal_transport
                .as_ref()
                .map(|t| t.mode)
                .unwrap_or_default(),
            trust: network.spec.peer_trust.as_ref().map(|t| t.mode).unwrap_or_default(),
        }
    }
}

/// Resolve the grid-wide modes from the sole `GridNetwork`, read once at startup.
async fn resolve_grid_modes(client: &Client) -> Result<GridModes, String> {
    let networks: Api<GridNetwork> = Api::all(client.clone());
    let items = networks
        .list(&kube::api::ListParams::default())
        .await
        .map_err(|e| format!("listing GridNetworks: {e}"))?
        .items;
    match items.as_slice() {
        [] => {
            tracing::info!("no GridNetwork at startup; gossip signals, pin peer trust");
            Ok(GridModes::default())
        },
        [network] => {
            let modes = GridModes::of(network);
            tracing::info!(?modes, "resolved grid-wide modes");
            Ok(modes)
        },
        many => Err(format!(
            "multiple GridNetworks unsupported: found {}; a process-global signals serve/poll path \
             cannot serve more than one grid mode",
            many.len()
        )),
    }
}

// ---------------------------------------------------------------------------
// Hostname helper
// ---------------------------------------------------------------------------

/// Optionally start the SWIM runtime from environment variables.
///
/// Returns `Some(handle)` if `GRID_SWIM_BIND_ADDR` is set and the runtime
/// starts successfully, `None` when SWIM is not configured or a non-contract
/// startup dependency is unavailable, and an error when an explicitly
/// configured bind or advertise endpoint is invalid or cannot be resolved.
///
/// Gateway address resolution uses [`operator::gateway::resolve`]:
/// `GRID_GATEWAY_ADDRESS` env var wins; otherwise the operator discovers
/// its own provider gateway Service `LoadBalancer` IP from Kubernetes.
#[expect(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::large_stack_frames,
    reason = "sequential env-var parsing + runtime startup; splitting would obscure the startup sequence"
)]
async fn maybe_start_swim(client: &Client, cli: &Cli) -> Result<Option<Arc<swim_runtime::SwimHandle>>, String> {
    let Some(addr_str) = std::env::var("GRID_SWIM_BIND_ADDR").ok() else {
        return Ok(None);
    };
    let bind_addr: SocketAddr = match addr_str.parse() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(addr = %addr_str, error = %e, "GRID_SWIM_BIND_ADDR not a valid socket address");
            return Err(format!("GRID_SWIM_BIND_ADDR is not a valid socket address: {e}"));
        },
    };
    let (advertise_addr, lb_watch) = swim_advertise_addr(client, bind_addr).await?;
    if advertise_addr.unwrap_or(bind_addr).ip().is_unspecified() {
        return Err("refusing to advertise an unspecified SWIM address; set GRID_SWIM_ADVERTISE_ADDR".to_owned());
    }
    let seed_values = std::env::var("GRID_SWIM_SEEDS").unwrap_or_default();
    let seed_values: Vec<String> = seed_values.split(',').map(str::to_owned).collect();
    let seed_resolution = resolve_endpoint_list_partial(&seed_values, "GRID_SWIM_SEEDS").await;
    for failure in &seed_resolution.failures {
        tracing::warn!(
            source = %failure.source,
            endpoint = %failure.endpoint,
            reason = %failure.reason,
            "ignoring unusable SWIM seed"
        );
    }
    if seed_resolution.configured && seed_resolution.addresses.is_empty() {
        tracing::warn!(
            source = "GRID_SWIM_SEEDS",
            failures = seed_resolution.failures.len(),
            "no configured SWIM seeds resolved; keeping SWIM active with a seedless bootstrap"
        );
    }
    let seeds = seed_resolution.addresses;
    let site_name = std::env::var("GRID_SWIM_SITE_NAME").unwrap_or_else(|_| hostname_or_default());
    let gateway_address = match gateway::resolve(client, &cli.gateway).await {
        Ok(addr) => addr,
        Err(e) => {
            tracing::error!(error = %e, "gateway address discovery failed; continuing without");
            None
        },
    };
    let (key, retry_key) = match parse_swim_key_env("GRID_SWIM_ENCRYPT_KEY")? {
        Some(key) => (swim_runtime::KeyState::Key(Arc::new(key)), false),
        None => startup_key_state(client, cli.swim.require_key).await,
    };
    let revision_lease = match reserve_revision_lease(client, &site_name).await {
        Ok(lease) => lease,
        Err(error) => {
            tracing::error!(%error, "failed to reserve SWIM revisions; running in static mode");
            return Ok(None);
        },
    };
    let signals_address = match (&cli.signals.advertise_addr, &lb_watch) {
        (Some(explicit), _) => Some(explicit.clone()),
        (None, Some(watch)) => watch.signals.clone(),
        (None, None) => None,
    };
    tracing::info!(signals = ?signals_address, "signals endpoint gossiped to peers");
    let cfg = SwimConfig {
        bind_addr,
        advertise_addr,
        signals_address,
        site_name: site_name.clone(),
        seeds,
        gateway_address,
        key,
        revision_lease,
        revision_renewer: Some(revision_renewer(client.clone(), site_name.clone())),
    };
    match swim_runtime::start(cfg).await {
        Ok(handle) => {
            tracing::info!(addr = %addr_str, "SWIM runtime started");
            if let Some(watch) = lb_watch {
                let track_signals = cli.signals.advertise_addr.is_none();
                tokio::spawn(LbWatch { track_signals, ..watch }.restart_on_change(Arc::clone(&handle)));
            }
            if retry_key {
                tokio::spawn(retry_startup_key(
                    client.clone(),
                    Arc::clone(&handle),
                    cli.swim.require_key,
                ));
            }
            Ok(Some(handle))
        },
        Err(e) => {
            tracing::error!(error = %e, "SWIM runtime failed to start; running in static mode");
            Ok(None)
        },
    }
}

/// What the `GridNetwork`s declare about the SWIM key.
#[derive(Clone, Debug)]
enum DeclaredKey {
    /// No `GridNetwork` exists yet.
    NoNetwork,
    /// A `GridNetwork` exists and names no key.
    None,
    /// The first `swimKeyRef` found.
    Ref(operator::crd::grid_network::SecretRef),
}

/// SWIM key state before the first send, and whether listing failed and must be retried.
async fn startup_key_state(client: &Client, require_key: bool) -> (swim_runtime::KeyState, bool) {
    match declared_swim_key(client).await {
        Ok(declared) => (key_state_for(client, &declared, require_key).await, false),
        Err(error) => {
            tracing::warn!(%error, "cannot list GridNetworks at SWIM startup; holding and retrying");
            (swim_runtime::KeyState::Pending, true)
        },
    }
}

/// The state `declared` calls for: held for an unloaded key, or for no network when one is required.
async fn key_state_for(client: &Client, declared: &DeclaredKey, require_key: bool) -> swim_runtime::KeyState {
    use swim_runtime::KeyState;
    let key_ref = match unkeyed_state(declared, require_key) {
        Ok(state) => return state,
        Err(key_ref) => key_ref,
    };
    match operator::resources::secret::read_swim_key(client, key_ref).await {
        Ok(Some(key)) => return KeyState::Key(Arc::new(key)),
        Ok(None) => tracing::warn!(secret = %key_ref.name, "swimKeyRef holds no valid key yet; holding SWIM"),
        Err(error) => tracing::warn!(secret = %key_ref.name, %error, "swimKeyRef not readable yet; holding SWIM"),
    }
    KeyState::Pending
}

/// The state when no key is to be read, else the key reference to read.
fn unkeyed_state(
    declared: &DeclaredKey,
    require_key: bool,
) -> Result<swim_runtime::KeyState, &operator::crd::grid_network::SecretRef> {
    match declared {
        DeclaredKey::NoNetwork if require_key => Ok(swim_runtime::KeyState::Pending),
        DeclaredKey::NoNetwork | DeclaredKey::None => Ok(swim_runtime::KeyState::Plain),
        DeclaredKey::Ref(key_ref) => Err(key_ref),
    }
}

/// Retry the startup key decision until `GridNetwork`s list, then apply it.
async fn retry_startup_key(client: Client, handle: Arc<swim_runtime::SwimHandle>, require_key: bool) {
    for delay in startup_retry_delays() {
        tokio::time::sleep(delay).await;
        match declared_swim_key(&client).await {
            Ok(declared) => {
                match key_state_for(&client, &declared, require_key).await {
                    swim_runtime::KeyState::Key(key) => drop(handle.set_swim_key(*key)),
                    swim_runtime::KeyState::Plain => drop(handle.release_plain()),
                    swim_runtime::KeyState::Pending => {},
                }
                tracing::info!(?declared, "SWIM startup key decision applied");
                return;
            },
            Err(error) => tracing::warn!(%error, ?delay, "still cannot list GridNetworks for the SWIM key"),
        }
    }
}

/// Backoff between startup key retries: doubling from one second, capped at a minute.
fn startup_retry_delays() -> impl Iterator<Item = std::time::Duration> {
    std::iter::successors(Some(std::time::Duration::from_secs(1)), |delay| {
        Some((*delay * 2).min(STARTUP_RETRY_MAX))
    })
}

/// Longest wait between startup key retries.
const STARTUP_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(60);

/// What the `GridNetwork`s declare about the SWIM key, listed under [`STARTUP_LIST_TIMEOUT`].
async fn declared_swim_key(client: &Client) -> Result<DeclaredKey, String> {
    let networks: Api<GridNetwork> = Api::all(client.clone());
    let list = tokio::time::timeout(STARTUP_LIST_TIMEOUT, networks.list(&kube::api::ListParams::default()))
        .await
        .map_err(|_elapsed| "timed out".to_owned())?
        .map_err(|error| error.to_string())?;
    Ok(declared_of(&list.items))
}

/// The key declaration across `networks`.
fn declared_of(networks: &[GridNetwork]) -> DeclaredKey {
    if networks.is_empty() {
        return DeclaredKey::NoNetwork;
    }
    networks
        .iter()
        .find_map(|n| n.spec.tls.swim_key_ref.clone())
        .map_or(DeclaredKey::None, DeclaredKey::Ref)
}

/// Bound on each startup API call.
const STARTUP_LIST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The SWIM advertise address, and the `LoadBalancer` to watch when it was discovered.
async fn swim_advertise_addr(
    client: &Client,
    bind_addr: SocketAddr,
) -> Result<(Option<SocketAddr>, Option<LbWatch>), String> {
    let text = match swim_advertise::plan(
        std::env::var("GRID_SWIM_ADVERTISE_ADDR").ok(),
        std::env::var("GRID_SWIM_ADVERTISE_FALLBACK").ok(),
        std::env::var("GRID_SWIM_SERVICE_NAME").ok(),
    ) {
        swim_advertise::Plan::Explicit(value) => value,
        swim_advertise::Plan::Pod(chart) => std::env::var("POD_IP")
            .ok()
            .and_then(|ip| swim_advertise::pod_endpoint(&ip, bind_addr.port()))
            .unwrap_or(chart),
        swim_advertise::Plan::Local => return Ok((None, None)),
        swim_advertise::Plan::LoadBalancer(service) => {
            let watch = LbWatch::discover(client, service, bind_addr.port()).await?;
            return Ok((Some(watch.addr), Some(watch)));
        },
    };
    let endpoint = text
        .parse::<SwimEndpoint>()
        .map_err(|error| format!("GRID_SWIM_ADVERTISE_ADDR is invalid: {error}"))?;
    let addresses = resolve_endpoint(&endpoint, "GRID_SWIM_ADVERTISE_ADDR")
        .await
        .map_err(|error| format!("cannot resolve GRID_SWIM_ADVERTISE_ADDR: {error}"))?;
    let resolved = addresses.first().copied();
    if let Some(address) = resolved {
        tracing::info!(configured = %endpoint.as_text(), %address, "resolved SWIM advertise endpoint");
    }
    Ok((resolved, None))
}

/// The SWIM Service whose `LoadBalancer` address this site advertises.
struct LbWatch {
    /// Services in the operator namespace.
    services: Api<Service>,
    /// SWIM Service name.
    service: String,
    /// SWIM bind port, to pick the Service port.
    port: u16,
    /// Address advertised at startup.
    addr: SocketAddr,
    /// Ingress text `addr` was resolved from, watched for change.
    text: String,
    /// Signals endpoint on the Service's `LoadBalancer` address.
    signals: Option<String>,
    /// Whether `signals` is gossiped, so a change to it restarts.
    track_signals: bool,
}

/// Wait for a leave to reach peers before exiting.
const LEAVE_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

impl LbWatch {
    /// Wait for the Service's `LoadBalancer` address, however long it takes.
    async fn discover(client: &Client, service: String, port: u16) -> Result<Self, String> {
        let watch = Self {
            services: Api::default_namespaced(client.clone()),
            service,
            port,
            addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            text: String::new(),
            signals: None,
            track_signals: true,
        };
        tracing::info!(service = %watch.service, "waiting for SWIM Service LoadBalancer address");
        let (advertised, addr, signals) = swim_advertise::wait_for_lb(
            || watch.resolved(),
            swim_advertise::LB_POLL_INTERVAL,
            swim_advertise::LB_PATIENCE,
        )
        .await?;
        tracing::info!(lb = %advertised, %addr, ?signals, "discovered SWIM advertise address");
        Ok(Self {
            addr,
            text: advertised,
            signals,
            ..watch
        })
    }

    /// The Service, refused when it is not a `LoadBalancer`.
    async fn load_balancer(&self) -> Result<Option<Service>, swim_advertise::LookupError> {
        let svc = self
            .services
            .get_opt(&self.service)
            .await
            .map_err(|error| swim_advertise::LookupError::Retry(error.to_string()))?;
        svc.as_ref().map(swim_advertise::require_load_balancer).transpose()?;
        Ok(svc)
    }

    /// The address text, its first resolution, and the signals endpoint, retried while a hostname has no DNS.
    async fn resolved(&self) -> Result<Option<(String, SocketAddr, Option<String>)>, swim_advertise::LookupError> {
        let Some((text, signals)) = self
            .load_balancer()
            .await?
            .and_then(|svc| swim_advertise::lb_advertised(&svc, self.port))
        else {
            return Ok(None);
        };
        let addresses = resolve_text(&text).await.map_err(swim_advertise::LookupError::Retry)?;
        Ok(addresses.first().map(|addr| (text, *addr, signals)))
    }

    /// Every ingress endpoint the Service lists now, unresolved, or none once a tracked signals endpoint moved.
    async fn ingress(&self) -> Result<Option<Vec<String>>, swim_advertise::LookupError> {
        Ok(self.load_balancer().await?.map(|svc| {
            let signals = swim_advertise::lb_signals_endpoint(&svc);
            if self.track_signals && signals != self.signals {
                tracing::warn!(gossiped = ?self.signals, current = ?signals, "SWIM Service signals endpoint changed");
                return Vec::new();
            }
            swim_advertise::lb_endpoints(&svc, self.port)
        }))
    }

    /// Leave the cluster and exit once the advertised address changes.
    async fn restart_on_change(self, swim: Arc<swim_runtime::SwimHandle>) {
        let current =
            swim_advertise::watch_lb(|| self.ingress(), &self.text, swim_advertise::LB_SLOW_POLL_INTERVAL).await;
        tracing::warn!(
            advertised = %self.text,
            ?current,
            "SWIM Service LoadBalancer address or signals endpoint changed; leaving to advertise the new one"
        );
        swim.leave(LEAVE_WAIT).await;
        std::process::exit(0);
    }
}

/// Resolve one `host:port` endpoint text.
async fn resolve_text(text: &str) -> Result<Vec<SocketAddr>, String> {
    let endpoint = text
        .parse::<SwimEndpoint>()
        .map_err(|error| format!("{text}: {error}"))?;
    resolve_endpoint(&endpoint, "GRID_SWIM_SERVICE_NAME").await
}

/// Revisions reserved durably per lease, renewed as a process uses them.
const REVISION_LEASE_SIZE: u64 = swim::state_broadcast::REVISION_LEASE_SPAN;
/// Maximum in-process foca identity renewals reserved for one operator.
const NODE_GENERATION_LEASE_SIZE: u64 = 1_u64 << 20;
/// Maximum resource-version conflicts retried during one reservation.
const REVISION_RESERVATION_ATTEMPTS: usize = 8;
/// `ConfigMap` data key containing the last reserved transport revision.
const REVISION_HIGH_KEY: &str = "revisionHighWatermark";
/// `ConfigMap` data key containing the last reserved identity generation.
const NODE_GENERATION_HIGH_KEY: &str = "nodeGenerationHighWatermark";

/// Reserve a disjoint transport-revision range and node generation.
async fn reserve_revision_lease(client: &Client, site_name: &str) -> Result<RevisionLease, String> {
    let lease = cas_revision_lease(client, site_name, |held| match held {
        Some((revision, generation)) => next_revision_lease(revision, generation),
        None => initial_revision_lease(),
    })
    .await?;
    tracing::info!(
        first_revision = lease.first_revision,
        last_revision = lease.last_revision,
        first_node_generation = lease.first_node_generation,
        last_node_generation = lease.last_node_generation,
        "reserved SWIM revision range"
    );
    Ok(lease)
}

/// Renewer that persists the next revision range past a process's own `last` before it is used.
fn revision_renewer(client: Client, site_name: String) -> swim_runtime::RevisionRenewer {
    Arc::new(move |last| {
        let (client, site_name) = (client.clone(), site_name.clone());
        Box::pin(async move {
            let lease = cas_revision_lease(&client, &site_name, |held| {
                let (revision, generation) = held.unwrap_or((last, 0));
                let first = next_revision_start(revision.max(last), unix_millis()?);
                Ok(RevisionLease {
                    first_node_generation: generation,
                    last_node_generation: generation,
                    ..revision_range(first)?
                })
            })
            .await?;
            tracing::debug!(
                first = lease.first_revision,
                last = lease.last_revision,
                "renewed SWIM revision range"
            );
            Ok((lease.first_revision, lease.last_revision))
        })
    })
}

/// Persist the lease `plan` makes from the held revision and generation marks, before any of it is used.
#[expect(
    clippy::too_many_lines,
    clippy::large_stack_frames,
    reason = "the Kubernetes read/create/replace CAS loop keeps each conflict and fail-closed path explicit"
)]
async fn cas_revision_lease(
    client: &Client,
    site_name: &str,
    plan: impl Fn(Option<(u64, u64)>) -> Result<RevisionLease, String>,
) -> Result<RevisionLease, String> {
    let api: Api<ConfigMap> = Api::default_namespaced(client.clone());
    let cm_name = format!("grid-swim-revision-hwm-{site_name}");
    for _attempt in 0..REVISION_RESERVATION_ATTEMPTS {
        let written = match api.get(&cm_name).await {
            Ok(mut cm) => {
                let data = cm.data.as_ref().ok_or_else(|| format!("{cm_name} has no data"))?;
                let revision = parse_revision_value(data, REVISION_HIGH_KEY)
                    .or_else(|| parse_revision_value(data, "revision"))
                    .ok_or_else(|| format!("{cm_name} has no valid revision high-water mark"))?;
                let generation = parse_revision_value(data, NODE_GENERATION_HIGH_KEY)
                    .or_else(|| parse_revision_value(data, "nodeGeneration"))
                    .unwrap_or(0);
                let lease = plan(Some((revision, generation)))?;
                cm.data = Some(revision_lease_data(&lease));
                api.replace(&cm_name, &PostParams::default(), &cm).await.map(|_| lease)
            },
            Err(kube::Error::Api(not_found)) if not_found.code == 404 => {
                let lease = plan(None)?;
                let cm = ConfigMap {
                    metadata: ObjectMeta {
                        name: Some(cm_name.clone()),
                        ..ObjectMeta::default()
                    },
                    data: Some(revision_lease_data(&lease)),
                    ..ConfigMap::default()
                };
                api.create(&PostParams::default(), &cm).await.map(|_| lease)
            },
            Err(read_err) => return Err(format!("read {cm_name}: {read_err}")),
        };
        match written {
            Ok(lease) => return Ok(lease),
            Err(kube::Error::Api(conflict)) if conflict.code == 409 => {},
            Err(write_err) => return Err(format!("write {cm_name}: {write_err}")),
        }
    }
    Err(format!(
        "could not reserve SWIM revisions in {cm_name} after {REVISION_RESERVATION_ATTEMPTS} conflicts"
    ))
}

/// Parse an unsigned value from `ConfigMap` data.
fn parse_revision_value(data: &BTreeMap<String, String>, key: &str) -> Option<u64> {
    data.get(key).and_then(|value| value.parse().ok())
}

/// Render the durable high-water marks for one reservation.
fn revision_lease_data(lease: &RevisionLease) -> BTreeMap<String, String> {
    BTreeMap::from([
        (REVISION_HIGH_KEY.to_owned(), lease.last_revision.to_string()),
        (
            NODE_GENERATION_HIGH_KEY.to_owned(),
            lease.last_node_generation.to_string(),
        ),
    ])
}

/// Build the first durable lease from wall-clock seeds.
fn initial_revision_lease() -> Result<RevisionLease, String> {
    lease_from_seeds(unix_millis()?, unix_nanos()?)
}

/// Build the next lease after persisted high-water marks.
fn next_revision_lease(current_high: u64, current_generation_high: u64) -> Result<RevisionLease, String> {
    plan_next_lease(current_high, current_generation_high, unix_millis()?, unix_nanos()?)
}

/// The next lease at `now_ms` and `now_nanos`, past the persisted marks.
fn plan_next_lease(
    current_high: u64,
    current_generation_high: u64,
    now_ms: u64,
    now_nanos: u64,
) -> Result<RevisionLease, String> {
    let node_generation = current_generation_high
        .checked_add(1)
        .ok_or_else(|| "SWIM node generation exhausted".to_owned())?
        .max(now_nanos);
    lease_from_seeds(next_revision_start(current_high, now_ms), node_generation)
}

/// First revision past `current_high` and the clock, reseeded at the clock when its lease would pass the peer cap.
fn next_revision_start(current_high: u64, now_ms: u64) -> u64 {
    let next = current_high.saturating_add(1).max(now_ms);
    let last = next.saturating_add(REVISION_LEASE_SIZE - 1);
    if last > swim::state_broadcast::max_leased_revision(now_ms) {
        tracing::warn!(
            current_high,
            now_ms,
            "SWIM revision mark is past every peer's cap; reseeding at the clock"
        );
        return now_ms;
    }
    next
}

/// Build bounded revision and generation ranges from inclusive first values.
fn lease_from_seeds(first_revision: u64, node_generation: u64) -> Result<RevisionLease, String> {
    let last_node_generation = node_generation
        .checked_add(NODE_GENERATION_LEASE_SIZE - 1)
        .ok_or_else(|| "SWIM node generation range exhausted".to_owned())?;
    Ok(RevisionLease {
        first_node_generation: node_generation,
        last_node_generation,
        ..revision_range(first_revision)?
    })
}

/// A lease of [`REVISION_LEASE_SIZE`] revisions from `first`, generations left zero.
fn revision_range(first: u64) -> Result<RevisionLease, String> {
    let last_revision = first
        .checked_add(REVISION_LEASE_SIZE - 1)
        .ok_or_else(|| "SWIM revision range exhausted".to_owned())?;
    Ok(RevisionLease {
        first_revision: first,
        last_revision,
        first_node_generation: 0,
        last_node_generation: 0,
    })
}

/// Return milliseconds since the Unix epoch as `u64`.
fn unix_millis() -> Result<u64, String> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock before Unix epoch: {error}"))?
        .as_millis();
    u64::try_from(millis).map_err(|_error| "Unix millisecond value exceeds u64".to_owned())
}

/// Return nanoseconds since the Unix epoch as `u64`.
fn unix_nanos() -> Result<u64, String> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock before Unix epoch: {error}"))?
        .as_nanos();
    u64::try_from(nanos).map_err(|_error| "Unix nanosecond value exceeds u64".to_owned())
}

/// Parse `GRID_SWIM_ENCRYPT_KEY` as a 32-byte AES-256-GCM key from 64 hex characters.
///
/// Absent is `Ok(None)`. Present but malformed is an error, never a silent fallback to plaintext.
///
/// # Security invariant
///
/// The decoded key bytes are never written to logs or tracing spans.
fn parse_swim_key_env(name: &str) -> Result<Option<swim::crypto::SwimKey>, String> {
    let Ok(hex) = std::env::var(name) else {
        return Ok(None);
    };
    let key = parse_swim_key(&hex).map_err(|reason| format!("{name} {reason}"))?;
    tracing::info!(env = name, "SWIM encryption key loaded from environment");
    Ok(Some(key))
}

/// Decode 64 hex characters into a 32-byte key.
fn parse_swim_key(hex: &str) -> Result<swim::crypto::SwimKey, &'static str> {
    let hex = hex.trim();
    if hex.len() != 64 {
        return Err("must be 64 hex characters (32 bytes)");
    }
    // to_digit(16) yields 0..=15, so the cast to u8 cannot truncate.
    let nibbles: Vec<u8> = hex
        .chars()
        .filter_map(|c| c.to_digit(16).and_then(|n| u8::try_from(n).ok()))
        .collect();
    if nibbles.len() != 64 {
        return Err("contains a character that is not hex");
    }
    let mut key = [0_u8; 32];
    for (byte, pair) in key.iter_mut().zip(nibbles.chunks_exact(2)) {
        if let [hi, lo] = pair {
            *byte = (hi << 4) | lo;
        }
    }
    Ok(key)
}

/// Return the machine hostname or a safe fallback.
fn hostname_or_default() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "grid-operator".to_owned())
}

// ---------------------------------------------------------------------------
// Controller Setup
// ---------------------------------------------------------------------------

/// Run the [`GridNetwork`] controller.
///
/// In addition to watching `GridNetwork` resources, this controller watches
/// `InferenceProvider`, `GridSite`, and `Secret` resources.  Secret changes
/// trigger reconciliation of affected `GridNetwork`s when providers change.
///
/// Metrics TLS rotation is detected by bounded requeue rather than a
/// cluster-wide Secret watch — the operator only reads referenced
/// Secrets by explicit namespace/name during reconciliation.
async fn run_network_controller(
    client: Client,
    ctx: Arc<OperatorCtx>,
    swim: SwimStartup,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<GridNetwork>::all(client.clone());
    let provider_api = Api::<InferenceProvider>::all(client.clone());
    let site_api = Api::<GridSite>::all(client.clone());

    let controller = Controller::new(api, watcher::Config::default())
        .watches(
            provider_api,
            watcher::Config::default(),
            grid_network::network_refs_from_inference_provider,
        )
        .watches(
            site_api,
            watcher::Config::default(),
            grid_network::network_refs_from_grid_site,
        );
    // Reconcile everything once SWIM starts, then on every membership change.
    let swim_events = futures::stream::once(swim_settled(swim))
        .filter_map(std::future::ready)
        .flat_map(|handle| futures::stream::once(std::future::ready(())).chain(handle.reconciliation_events()));
    controller
        .reconcile_all_on(swim_events)
        .run(grid_network::reconcile, grid_network::error_policy, ctx)
        .for_each(|result| async {
            match result {
                Ok((obj, _action)) => tracing::info!(%obj, "reconciled GridNetwork"),
                Err(e) => log_controller_error("GridNetwork", &e),
            }
        })
        .await;

    Ok(())
}

/// Log a controller error, an expired watch at debug since kube-runtime relists.
fn log_controller_error<R: std::fmt::Debug>(kind: &str, error: &kube::runtime::controller::Error<R, watcher::Error>) {
    if watch_expired(error) {
        tracing::debug!(kind, ?error, "watch expired; relisting");
    } else {
        tracing::error!(error = ?error, "{kind} watch error");
    }
}

/// Whether a controller error is the apiserver expiring a watch's resource version.
fn watch_expired<R>(error: &kube::runtime::controller::Error<R, watcher::Error>) -> bool {
    let kube::runtime::controller::Error::QueueError(queue) = error else {
        return false;
    };
    let code = match queue {
        watcher::Error::WatchError(status)
        | watcher::Error::WatchFailed(kube::Error::Api(status))
        | watcher::Error::WatchStartFailed(kube::Error::Api(status))
        | watcher::Error::InitialListFailed(kube::Error::Api(status)) => status.code,
        watcher::Error::WatchFailed(_)
        | watcher::Error::WatchStartFailed(_)
        | watcher::Error::InitialListFailed(_)
        | watcher::Error::NoResourceVersion => return false,
    };
    code == http::StatusCode::GONE.as_u16()
}

/// Run the [`GridSite`] controller.
async fn run_site_controller(client: Client) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<GridSite>::all(client.clone());

    Controller::new(api, watcher::Config::default())
        .with_config(kube::runtime::controller::Config::default().concurrency(16))
        .run(grid_site::reconcile, grid_site::error_policy, Arc::new(client))
        .for_each(|result| async {
            match result {
                Ok((obj, _action)) => tracing::info!(%obj, "reconciled GridSite"),
                Err(e) => log_controller_error("GridSite", &e),
            }
        })
        .await;

    Ok(())
}

/// Run the [`InferenceProvider`] controller (OP-02).
///
/// Watches `InferenceProvider` resources.  Metrics TLS rotation is detected
/// by bounded requeue rather than a cluster-wide Secret watch — the operator
/// only reads referenced Secrets by explicit namespace/name during
/// reconciliation.
///
/// [`InferenceProvider`]: operator::crd::inference_provider::InferenceProvider
async fn run_provider_controller(client: Client) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<InferenceProvider>::all(client.clone());

    Controller::new(api, watcher::Config::default())
        .run(
            inference_provider::reconcile,
            inference_provider::error_policy,
            Arc::new(client),
        )
        .for_each(|result| async {
            match result {
                Ok((obj, _action)) => tracing::info!(%obj, "reconciled InferenceProvider"),
                Err(e) => log_controller_error("InferenceProvider", &e),
            }
        })
        .await;

    Ok(())
}

/// Run the [`AgentToolProvider`] controller (grid#41).
///
/// Watches `AgentToolProvider` resources. Mirrors
/// [`run_provider_controller`]'s structure; cross-resource watches for
/// `GridNetwork`/`GridSite` changes are a follow-up, matching
/// [`InferenceProvider`]'s own documented watch limitation.
async fn run_agent_tool_provider_controller(client: Client) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<AgentToolProvider>::all(client.clone());

    Controller::new(api, watcher::Config::default())
        .run(
            agent_tool_provider::reconcile,
            agent_tool_provider::error_policy,
            Arc::new(client),
        )
        .for_each(|result| async {
            match result {
                Ok((obj, _action)) => tracing::info!(%obj, "reconciled AgentToolProvider"),
                Err(e) => log_controller_error("AgentToolProvider", &e),
            }
        })
        .await;

    Ok(())
}

/// Serve Prometheus metrics and health endpoints.
async fn run_metrics_server(
    ready: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = std::env::var("GRID_METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:9090".to_owned());
    let app = axum::Router::new()
        .route("/metrics", axum::routing::get(metrics_handler))
        .route("/healthz", axum::routing::get(health_handler))
        .route(
            "/readyz",
            axum::routing::get(move || std::future::ready(readiness(&ready))),
        );
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let bound_addr = listener.local_addr().map_or_else(|_| addr.clone(), |a| a.to_string());
    tracing::info!(addr = %bound_addr, "metrics server started");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Prometheus text-format metrics handler.
async fn metrics_handler() -> impl axum::response::IntoResponse {
    let body = operator::metrics::encode_metrics();
    (
        [(http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

/// Liveness handler.
async fn health_handler() -> &'static str {
    "ok"
}

/// Not ready until enrollment and SWIM startup, including any `LoadBalancer` wait, have finished.
fn readiness(ready: &std::sync::atomic::AtomicBool) -> (http::StatusCode, &'static str) {
    if ready.load(std::sync::atomic::Ordering::Acquire) {
        (http::StatusCode::OK, "ok")
    } else {
        (http::StatusCode::SERVICE_UNAVAILABLE, "starting")
    }
}

// ---------------------------------------------------------------------------
// Signals serving and peer polling (poll mode)
// ---------------------------------------------------------------------------
//
// Under poll mode this site serves the coarse rollup over mTLS and polls its
// peers; the gossip-carried signals and local scoring paths are untouched here.
// Poll mode also stops carrying metrics through SWIM gossip and turns off local
// scoring, but that gating lives in the GridNetwork reconcile path, not here.

/// How often the listener and poller re-read their own certificate.
///
/// Material rarely arrives with the process: cert-manager writes the Secret
/// after the operator rolls and rewrites it on renewal. Re-reading keeps a
/// listener that came up early from being stuck without TLS, and one that came
/// up before a renewal from serving the old key.
const SIGNALS_TLS_POLL: std::time::Duration = std::time::Duration::from_secs(30);

/// Trigger cooperative shutdown on the first termination signal.
async fn watch_for_termination(trigger: operator::shutdown::Trigger) {
    let signal = first_termination_signal().await;
    tracing::info!(signal, "standing down");
    trigger.trigger();
}

/// Resolve on SIGTERM, or SIGINT, whichever arrives first.
async fn first_termination_signal() -> &'static str {
    let Ok(mut term) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) else {
        tracing::warn!("cannot watch for SIGTERM; only an interrupt will stand down cleanly");
        drop(tokio::signal::ctrl_c().await);
        return "SIGINT";
    };
    tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = tokio::signal::ctrl_c() => "SIGINT",
    }
}

/// Who is asking, which decides what they are served.
///
/// Decided from the certificate a connection presented, before any request
/// parameter is read, so a caller cannot widen its scope by asking differently.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Caller {
    /// Another site, named by the certificate it presented.
    ///
    /// Carries the labels this site holds for that peer, matched against the
    /// local `GridSite` record rather than the peer's own claim. `None` when
    /// this site holds no record for the name, holds one that refuses reads, or
    /// holds pins the presented key does not match, or when no certificate was
    /// presented at all. A `None` peer is served nothing.
    Peer(Option<BTreeMap<String, String>>),
    /// The co-located gateway, proven by this site's own certificate.
    Local,
}

/// How a caller is named on the signals listener.
#[derive(Clone)]
struct SignalsIdentity {
    /// Keys peers have declared, and the labels held for each.
    peers: operator::signals::PeerIdentities,
    /// This site's own leaf, when it has one.
    own: Option<grid_network::OwnLeaf>,
    /// Whether callers are named by pinned key or by SPIFFE ID.
    trust: operator::signals::PeerTrustMode,
}

/// Everything this operator publishes on the signals path.
#[derive(Clone)]
struct Published {
    /// This site's own signals.
    site: operator::signals::SignalStore,
    /// What peers reported about themselves.
    peers: operator::signals::SignalStore,
}

/// What the signals listener needs from startup.
struct SignalsListener {
    /// Explicit bind address, else [`SIGNALS_DEFAULT_V6`] with an IPv4 fallback.
    bind: Option<SocketAddr>,
    /// Kube client, for this site's TLS material.
    client: Client,
    /// What is served.
    published: Published,
    /// Keys peers have declared, and the labels held for each.
    peer_identities: operator::signals::PeerIdentities,
    /// Whether callers are named by pinned key or by SPIFFE ID.
    trust: operator::signals::PeerTrustMode,
    /// Authenticated connections one identity may hold.
    max_per_peer: usize,
}

/// Serve the coarse signal rollup on the single mTLS wire path, fail closed.
///
/// There is no plaintext branch: without verified TLS material the rollup is
/// not exposed at all, rather than served to every caller as `Local`. The
/// listener rebinds whenever this site's certificate changes, because rustls
/// fixes the verifier at build time.
async fn run_signals_server(
    listener: Option<SignalsListener>,
    swim: SwimStartup,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(listener) = listener else {
        return Ok(());
    };
    // Peers learn this listener from the advertised address, so it waits for SWIM to settle.
    drop(swim_settled(swim).await);
    let app = axum::Router::new()
        .route(operator::signals::SIGNALS_PATH, axum::routing::get(signals_handler))
        .with_state(listener.published.clone());

    // The opt-in listener rebinds on every material change, so a transient
    // bind blip (a socket in TIME_WAIT across a rebind) must not be fatal to
    // reconcile and the metrics server through `try_join!`: a serve error logs
    // and retries rather than propagating.
    #[expect(
        clippy::infinite_loop,
        reason = "serves for the process lifetime alongside the controllers"
    )]
    loop {
        if let Err(error) = listener.serve_once(&app).await {
            tracing::error!(%error, bind = ?listener.bind, "signals listener error; retrying");
            tokio::time::sleep(SIGNALS_TLS_POLL).await;
        }
    }
}

impl SignalsListener {
    /// Serve until this site's TLS material changes, or wait while it is unavailable.
    async fn serve_once(&self, app: &axum::Router) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (tls, own) = Box::pin(signals_identity(&self.client)).await;
        let Some(tls) = tls else {
            tracing::warn!("signals: TLS material unavailable; not serving the rollup (fail closed)");
            tokio::time::sleep(SIGNALS_TLS_POLL).await;
            return Ok(());
        };
        let identity = SignalsIdentity {
            peers: self.peer_identities.clone(),
            own: own.clone(),
            trust: self.trust,
        };
        let listener = bind_signals(self.bind).await?;
        let bound = listener
            .local_addr()
            .map_or_else(|_| "unknown".to_owned(), |a| a.to_string());
        tracing::info!(addr = %bound, tls = true, "signals server started");
        let admission = Admission::new(self.max_per_peer);
        let changed = material_changed(self.client.clone(), own);
        let serving = Serving {
            tls,
            app: app.clone(),
            identity,
        };
        serve_signals_tls(listener, serving, admission, changed).await?;
        tracing::info!("signals TLS material changed; serving again");
        Ok(())
    }
}

/// Dual-stack signals listener address when `GRID_SIGNALS_ADDR` is unset.
const SIGNALS_DEFAULT_V6: SocketAddr = SocketAddr::new(std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), 9091);

/// IPv4 signals listener address when the host has no IPv6.
const SIGNALS_DEFAULT_V4: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 9091);

/// `EAFNOSUPPORT`, which std maps to no stable error kind.
const EAFNOSUPPORT: i32 = if cfg!(target_os = "linux") { 97 } else { 47 };

/// Bind `explicit`, else [`SIGNALS_DEFAULT_V6`], falling back to [`SIGNALS_DEFAULT_V4`] without IPv6.
async fn bind_signals(explicit: Option<SocketAddr>) -> std::io::Result<tokio::net::TcpListener> {
    bind_with_fallback(explicit, |addr| std::future::ready(bind_dual_stack(addr))).await
}

/// Bind `addr`, an IPv6 wildcard accepting IPv4 too even where `bindv6only` is set.
fn bind_dual_stack(addr: SocketAddr) -> std::io::Result<tokio::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    if addr.is_ipv6() && addr.ip().is_unspecified() {
        socket.set_only_v6(false)?;
    }
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    tokio::net::TcpListener::from_std(socket.into())
}

/// Bind with `bind`, falling back to IPv4 only for the default address on a host without IPv6.
async fn bind_with_fallback<Listener, Bind, Bound>(
    explicit: Option<SocketAddr>,
    bind: Bind,
) -> std::io::Result<Listener>
where
    Bind: Fn(SocketAddr) -> Bound,
    Bound: Future<Output = std::io::Result<Listener>>,
{
    if let Some(addr) = explicit {
        return bind(addr).await;
    }
    match bind(SIGNALS_DEFAULT_V6).await {
        Err(error) if lacks_ipv6(&error) => {
            tracing::info!(%error, fallback = %SIGNALS_DEFAULT_V4, "no IPv6 for the signals listener; binding IPv4");
            bind(SIGNALS_DEFAULT_V4).await
        },
        bound => bound,
    }
}

/// Whether a bind failed because the host has no IPv6.
fn lacks_ipv6(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::AddrNotAvailable || error.raw_os_error() == Some(EAFNOSUPPORT)
}

/// Resolves when this site's certificate definitely stops matching `serving`.
///
/// A transient read failure is not a change: a momentary API-server hiccup must
/// not tear down a healthy mTLS listener (or the poller) for a poll interval
/// when the serving certificate never actually moved. Only a successfully
/// read leaf that differs, including a genuine removal, resolves.
async fn material_changed(client: Client, serving: Option<grid_network::OwnLeaf>) {
    loop {
        tokio::time::sleep(SIGNALS_TLS_POLL).await;
        if let Some(observed) = Box::pin(observe_own_leaf(&client)).await
            && observed != serving
        {
            return;
        }
    }
}

/// This site's own leaf, `None` when unreadable.
async fn observe_own_leaf(client: &Client) -> Option<Option<grid_network::OwnLeaf>> {
    let networks: Api<GridNetwork> = Api::all(client.clone());
    let list = networks.list(&kube::api::ListParams::default()).await.ok()?;
    let Some(network) = list.items.into_iter().next() else {
        return Some(None);
    };
    grid_network::own_leaf_identity(&network, client).await.ok()
}

/// What every connection on one listener is served with.
#[derive(Clone)]
struct Serving {
    /// TLS for the handshake.
    tls: ServerTlsConfig,
    /// The signals routes.
    app: axum::Router,
    /// How callers are named.
    identity: SignalsIdentity,
}

/// Accept signals connections, deciding scope from the certificate presented.
async fn serve_signals_tls(
    listener: tokio::net::TcpListener,
    serving: Serving,
    admission: Admission,
    changed: impl Future<Output = ()> + Send,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut changed = std::pin::pin!(changed);
    let mut shed_warned: Option<std::time::Instant> = None;
    loop {
        let accepted = tokio::select! {
            () = &mut changed => return Ok(()),
            accepted = listener.accept() => accepted,
        };
        let Some((stream, remote)) = accepted_or_backoff(accepted).await else {
            continue;
        };
        let Some((slot, source)) = admission.admit(remote, &mut shed_warned) else {
            continue;
        };
        let handshaking = Handshaking {
            source,
            peers: admission.peers.clone(),
        };
        let serve = Box::pin(serve_signals_connection(serving.clone(), stream, remote, handshaking));
        tokio::spawn(async move {
            serve.await;
            drop(slot);
        });
    }
}

/// Concurrent signals connections, where a poll round is one per peer.
const SIGNALS_MAX_CONNECTIONS: usize = 256;

/// Connections peers can never hold, kept for the co-located gateway.
const SIGNALS_LOCAL_RESERVE: usize = 8;

/// Concurrent handshakes from one source.
const SIGNALS_MAX_PER_SOURCE: usize = 8;

/// Concurrent handshakes from one global unicast IPv6 /48, across its /64s.
const SIGNALS_MAX_PER_SOURCE_48: usize = 32;

/// Pause after a failed accept.
const SIGNALS_ACCEPT_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

/// Least time between warnings about shed connections.
const SIGNALS_SHED_WARN_EVERY: std::time::Duration = std::time::Duration::from_secs(60);

/// Bound on the TLS handshake.
const SIGNALS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// Bound on reading request headers, including between keep-alive requests.
const SIGNALS_HEADER_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Bound on a response write that makes no progress.
const SIGNALS_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Age at which an authenticated connection is closed after its current request.
const SIGNALS_MAX_CONNECTION_AGE: std::time::Duration = std::time::Duration::from_secs(300);

/// The accepted connection, or `None` after pausing on a failed accept.
async fn accepted_or_backoff(
    accepted: std::io::Result<(tokio::net::TcpStream, SocketAddr)>,
) -> Option<(tokio::net::TcpStream, SocketAddr)> {
    match accepted {
        Ok(accepted) => Some(accepted),
        Err(error) => {
            // EMFILE fails every accept until a descriptor frees.
            tracing::warn!(%error, "signals accept failed");
            tokio::time::sleep(SIGNALS_ACCEPT_BACKOFF).await;
            None
        },
    }
}

/// The connection caps one listener enforces.
struct Admission {
    /// Every open connection.
    slots: Arc<tokio::sync::Semaphore>,
    /// Handshakes in progress, per source.
    sources: SourceSlots,
    /// Authenticated peer connections, per site.
    peers: PeerSlots,
}

impl Admission {
    /// Caps for one listener, `max_per_peer` connections per site.
    fn new(max_per_peer: usize) -> Self {
        Self {
            slots: Arc::new(tokio::sync::Semaphore::new(SIGNALS_MAX_CONNECTIONS)),
            sources: SourceSlots::new(SIGNALS_MAX_PER_SOURCE, SIGNALS_MAX_PER_SOURCE_48),
            peers: PeerSlots::new(
                max_per_peer,
                SIGNALS_MAX_CONNECTIONS.saturating_sub(SIGNALS_LOCAL_RESERVE),
            ),
        }
    }

    /// A connection slot and a handshake slot for `remote`, or `None` to shed it.
    fn admit(
        &self,
        remote: SocketAddr,
        warned: &mut Option<std::time::Instant>,
    ) -> Option<(tokio::sync::OwnedSemaphorePermit, SourceSlot)> {
        let held = match Arc::clone(&self.slots).try_acquire_owned() {
            Ok(slot) => self
                .sources
                .acquire(remote.ip())
                .map(|source| (slot, source))
                .ok_or("source"),
            Err(_) => Err("total"),
        };
        held.inspect_err(|limit| shed(remote, limit, warned)).ok()
    }
}

/// Record a shed connection, warning at most once per [`SIGNALS_SHED_WARN_EVERY`].
fn shed(remote: SocketAddr, limit: &str, warned: &mut Option<std::time::Instant>) {
    operator::metrics::record_signals_shed(limit);
    let quiet = warned.is_some_and(|at| at.elapsed() < SIGNALS_SHED_WARN_EVERY);
    if quiet {
        tracing::debug!(%remote, limit, "signals connection shed");
    } else {
        *warned = Some(std::time::Instant::now());
        tracing::warn!(%remote, limit, "signals connection limit reached; shedding");
    }
}

/// Open handshakes per source, each capped, with a second cap per global IPv6 /48.
#[derive(Clone)]
struct SourceSlots {
    /// Open handshakes by source key.
    open: Arc<std::sync::Mutex<std::collections::HashMap<std::net::IpAddr, usize>>>,
    /// Cap per source key.
    limit: usize,
    /// Cap per global unicast IPv6 /48.
    limit_48: usize,
}

impl SourceSlots {
    /// A tracker allowing `limit` handshakes per source and `limit_48` per IPv6 /48.
    fn new(limit: usize, limit_48: usize) -> Self {
        Self {
            open: Arc::default(),
            limit,
            limit_48,
        }
    }

    /// A slot for `ip`, released on drop, or `None` at either cap.
    fn acquire(&self, ip: std::net::IpAddr) -> Option<SourceSlot> {
        let (key, wider) = source_keys(ip);
        let mut open = self.open.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let held = |counted: &std::net::IpAddr| open.get(counted).copied().unwrap_or_default();
        if held(&key) >= self.limit || wider.is_some_and(|wider| held(&wider) >= self.limit_48) {
            return None;
        }
        for counted in std::iter::once(key).chain(wider) {
            let count = open.entry(counted).or_default();
            *count = count.saturating_add(1);
        }
        drop(open);
        Some(SourceSlot {
            slots: self.clone(),
            keys: (key, wider),
        })
    }
}

/// The keys a source counts against: a global unicast IPv6 /64 and its /48, else its own address.
fn source_keys(ip: std::net::IpAddr) -> (std::net::IpAddr, Option<std::net::IpAddr>) {
    let prefix = |v6: std::net::Ipv6Addr, bits: u32| -> std::net::IpAddr {
        std::net::Ipv6Addr::from_bits(v6.to_bits() & (u128::MAX << (128 - bits))).into()
    };
    match ip.to_canonical() {
        // 2000::/3 is the global unicast range.
        std::net::IpAddr::V6(v6) if v6.to_bits() >> 125 == 0b001 => (prefix(v6, 64), Some(prefix(v6, 48))),
        scoped @ (std::net::IpAddr::V4(_) | std::net::IpAddr::V6(_)) => (scoped, None),
    }
}

/// One held handshake slot.
struct SourceSlot {
    /// The tracker to release into.
    slots: SourceSlots,
    /// The keys counted.
    keys: (std::net::IpAddr, Option<std::net::IpAddr>),
}

impl Drop for SourceSlot {
    fn drop(&mut self) {
        let mut open = self
            .slots
            .open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for key in std::iter::once(self.keys.0).chain(self.keys.1) {
            if let Some(count) = open.get_mut(&key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    open.remove(&key);
                }
            }
        }
    }
}

/// Open authenticated peer connections per site, under a cap across all peers.
#[derive(Clone)]
struct PeerSlots {
    /// Open connections by site.
    open: Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>>,
    /// Cap per site.
    limit: usize,
    /// Cap across every peer.
    total: Arc<tokio::sync::Semaphore>,
}

impl PeerSlots {
    /// A tracker allowing `limit` connections per site and `total` across them.
    fn new(limit: usize, total: usize) -> Self {
        Self {
            open: Arc::default(),
            limit: limit.max(1),
            total: Arc::new(tokio::sync::Semaphore::new(total.max(1))),
        }
    }

    /// A slot for `site`, released on drop, or `None` at either cap.
    fn acquire(&self, site: &str) -> Option<PeerSlot> {
        let permit = Arc::clone(&self.total).try_acquire_owned().ok()?;
        let mut open = self.open.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = open.entry(site.to_owned()).or_default();
        if *count >= self.limit {
            return None;
        }
        *count = count.saturating_add(1);
        drop(open);
        Some(PeerSlot {
            slots: self.clone(),
            site: site.to_owned(),
            _permit: permit,
        })
    }
}

/// One held authenticated slot.
struct PeerSlot {
    /// The tracker to release into.
    slots: PeerSlots,
    /// The site counted.
    site: String,
    /// The share of the cap across peers.
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for PeerSlot {
    fn drop(&mut self) {
        let mut open = self
            .slots
            .open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = open.get_mut(&self.site) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                open.remove(&self.site);
            }
        }
    }
}

/// A connection still in its handshake: the source slot it holds, and the per-peer caps that follow.
struct Handshaking {
    /// Released once the handshake ends.
    source: SourceSlot,
    /// Where the authenticated connection is counted.
    peers: PeerSlots,
}

/// Who a leaf speaks for and the site it counts against, `Local` only for this site's current leaf.
fn caller_for(leaf: Option<&[u8]>, identity: &SignalsIdentity, remote: SocketAddr) -> (Caller, Option<String>) {
    let Some(leaf) = leaf else {
        tracing::debug!(%remote, "signals caller presented no certificate");
        return (Caller::Peer(None), None);
    };
    let fingerprint = operator::signals::leaf_fingerprint(leaf);
    if identity.own.as_ref().is_some_and(|own| own.fingerprint == fingerprint) {
        return (Caller::Local, None);
    }
    let named = match identity.trust {
        operator::signals::PeerTrustMode::Pin => identity.peers.resolve_site_by_key(&fingerprint),
        operator::signals::PeerTrustMode::Spiffe => site_by_spiffe(leaf, identity),
    };
    let Some((site, labels)) = named else {
        tracing::debug!(%remote, "signals caller is not a site this one names");
        return (Caller::Peer(None), None);
    };
    (Caller::Peer(Some(labels)), Some(site))
}

/// Spiffe mode: the site the leaf's SPIFFE ID names, with its labels, never this site's own.
fn site_by_spiffe(leaf: &[u8], identity: &SignalsIdentity) -> Option<(String, BTreeMap<String, String>)> {
    let spiffe = certs::leaf_spiffe_id(leaf)?;
    let own_id = identity.own.as_ref().and_then(|own| own.spiffe.as_ref());
    if own_id == Some(&spiffe) {
        return None;
    }
    let site = certs::site_of_spiffe_id(&spiffe)?;
    identity.peers.labels_for(site).map(|labels| (site.to_owned(), labels))
}

/// Handshake one connection, then serve it with the scope its certificate earns.
#[cfg_attr(
    not(feature = "fips"),
    expect(clippy::large_stack_frames, reason = "async future over a rustls handshake")
)]
async fn serve_signals_connection(
    serving: Serving,
    tcp: tokio::net::TcpStream,
    remote: SocketAddr,
    handshaking: Handshaking,
) {
    let accepted = tokio::time::timeout(SIGNALS_HANDSHAKE_TIMEOUT, tls_backend::accept(tcp, &serving.tls)).await;
    // The source cap covers only the unauthenticated phase.
    drop(handshaking.source);
    let Ok(Ok(stream)) = accepted else {
        tracing::debug!(%remote, "signals handshake failed or timed out");
        return;
    };
    let Some(leaf) = tls_backend::server_peer_leaf_der(&stream) else {
        return;
    };
    let (caller, site) = caller_for(Some(&leaf), &serving.identity, remote);
    let slot = match (&caller, site) {
        (Caller::Local, _) => None,
        (_, Some(site)) => {
            let Some(slot) = handshaking.peers.acquire(&site) else {
                operator::metrics::record_signals_shed("peer");
                tracing::debug!(%remote, site, "signals site at its connection cap; answering 503");
                Box::pin(serve_http(stream, unavailable(), Caller::Peer(None), remote)).await;
                return;
            };
            Some(slot)
        },
        (_, None) => return,
    };
    Box::pin(serve_http(stream, serving.app, caller, remote)).await;
    drop(slot);
}

/// Answers every request 503, for a caller shed at its cap.
fn unavailable() -> axum::Router {
    axum::Router::new().fallback(|| std::future::ready(http::StatusCode::SERVICE_UNAVAILABLE))
}

/// Serve HTTP on an authenticated stream with the scope `caller` earned, bounded by every signals timeout.
#[cfg_attr(
    not(feature = "fips"),
    expect(
        clippy::large_stack_frames,
        reason = "async future over a rustls stream and a hyper connection"
    )
)]
async fn serve_http(stream: tls_backend::ServerTlsStream, app: axum::Router, caller: Caller, remote: SocketAddr) {
    // A shed caller gets one answer, then the connection closes.
    let keep_alive = caller != Caller::Peer(None);
    let service = hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
        use tower::Service as _;
        let mut request = request;
        request.extensions_mut().insert(caller.clone());
        app.clone().call(request)
    });
    let io = hyper_util::rt::TokioIo::new(WriteDeadline::new(stream, SIGNALS_WRITE_TIMEOUT));
    let connection = hyper::server::conn::http1::Builder::new()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(SIGNALS_HEADER_READ_TIMEOUT)
        .keep_alive(keep_alive)
        .serve_connection(io, service);
    if let Err(error) = serve_until_aged(connection, SIGNALS_MAX_CONNECTION_AGE).await {
        tracing::debug!(%remote, %error, "signals connection ended");
    }
}

/// Serve `connection`, then let it finish its current request and close once it reaches `max_age`.
async fn serve_until_aged<Io, Service>(
    connection: hyper::server::conn::http1::Connection<Io, Service>,
    max_age: std::time::Duration,
) -> hyper::Result<()>
where
    Io: hyper::rt::Read + hyper::rt::Write + Unpin,
    Service: hyper::service::HttpService<hyper::body::Incoming>,
    Service::ResBody: 'static,
    <Service::ResBody as hyper::body::Body>::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let mut connection = std::pin::pin!(connection);
    tokio::select! {
        result = connection.as_mut() => return result,
        () = tokio::time::sleep(max_age) => {},
    }
    connection.as_mut().graceful_shutdown();
    tokio::time::timeout(SIGNALS_WRITE_TIMEOUT, connection)
        .await
        .unwrap_or(Ok(()))
}

/// A stream whose writes fail once they make no progress for `limit`.
struct WriteDeadline<Stream> {
    /// The wrapped stream.
    inner: Stream,
    /// Longest a write may stay pending.
    limit: std::time::Duration,
    /// Armed while a write is pending.
    stalled: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

impl<Stream> WriteDeadline<Stream> {
    /// Wrap `inner`, failing a write pending longer than `limit`.
    fn new(inner: Stream, limit: std::time::Duration) -> Self {
        Self {
            inner,
            limit,
            stalled: None,
        }
    }

    /// Track one write attempt, turning a stall past the limit into an error.
    fn guard<Output>(
        &mut self,
        cx: &mut std::task::Context<'_>,
        attempt: std::task::Poll<std::io::Result<Output>>,
    ) -> std::task::Poll<std::io::Result<Output>> {
        if attempt.is_ready() {
            self.stalled = None;
            return attempt;
        }
        let limit = self.limit;
        let stalled = self.stalled.get_or_insert_with(|| Box::pin(tokio::time::sleep(limit)));
        match stalled.as_mut().poll(cx) {
            std::task::Poll::Ready(()) => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "signals response write stalled",
            ))),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl<Stream: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for WriteDeadline<Stream> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<Stream: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for WriteDeadline<Stream> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let attempt = std::pin::Pin::new(&mut this.inner).poll_write(cx, buf);
        this.guard(cx, attempt)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let attempt = std::pin::Pin::new(&mut this.inner).poll_flush(cx);
        this.guard(cx, attempt)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let attempt = std::pin::Pin::new(&mut this.inner).poll_shutdown(cx);
        this.guard(cx, attempt)
    }
}

/// Serve the coarse rollup, following the multi-target exporter pattern.
///
/// `target` names one provider, `collect[]` names the signals wanted. Scope
/// comes from the connection, so no parameter can widen it.
async fn signals_handler(
    axum::extract::State(published): axum::extract::State<Published>,
    axum::Extension(caller): axum::Extension<Caller>,
    axum::extract::Query(params): axum::extract::Query<Vec<(String, String)>>,
) -> axum::response::Response {
    let target = params.iter().find(|(k, _)| k == "target").map(|(_, v)| v.as_str());
    let collect: Vec<String> = params
        .iter()
        .filter(|(k, _)| k == "collect[]" || k == "collect")
        .map(|(_, v)| v.clone())
        .collect();

    // Local, the site's own data plane, gets the whole grid view unscoped.
    // Access policy bounds peer reads, not the site reading itself.
    let (mut body, mut oldest) = match &caller {
        Caller::Local => published.site.render_unrestricted(target, &collect),
        Caller::Peer(Some(labels)) => published.site.render(target, &collect, Some(labels)),
        Caller::Peer(None) => return refused(),
    };
    if caller == Caller::Local {
        // Peers relay only to Local, and the peers store carries no access map.
        // A Peer(Some) relay would need per-target scoping added here.
        let (relayed, relayed_age) = published.peers.render_unrestricted(target, &collect);
        body.push_str(&relayed);
        oldest = oldest.max(relayed_age);
    }
    served(body, oldest)
}

/// Refuse a caller this site will not serve.
///
/// A status rather than an empty body: an empty exposition says nothing is held
/// right now, which a peer would chase as a fault; a refusal says the answer
/// will not change until an administrator changes it. The reason is the same
/// for every cause, so a caller learns only that it is refused.
fn refused() -> axum::response::Response {
    (
        http::StatusCode::FORBIDDEN,
        "signals: caller is not permitted to read this site\n",
    )
        .into_response()
}

/// One exposition response, with `Age` bounding the whole body.
fn served(body: String, oldest: std::time::Duration) -> axum::response::Response {
    (
        [
            (http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8"),
            (http::header::AGE, &*oldest.as_secs().to_string()),
        ],
        body,
    )
        .into_response()
}

/// TLS for the signals listener, `None` when absent or unreadable, and this site's own leaf.
async fn signals_identity(client: &Client) -> (Option<ServerTlsConfig>, Option<grid_network::OwnLeaf>) {
    let Some(network) = sole_network(client).await else {
        return (None, None);
    };
    (
        Box::pin(signals_listener_tls(&network, client)).await,
        Box::pin(own_leaf(&network, client)).await,
    )
}

/// The `GridNetwork` this operator serves, `None` when absent or unlistable.
async fn sole_network(client: &Client) -> Option<GridNetwork> {
    let networks: Api<GridNetwork> = Api::all(client.clone());
    networks
        .list(&kube::api::ListParams::default())
        .await
        .ok()
        .and_then(|list| list.items.into_iter().next())
}

/// TLS for the listener, or `None` when unconfigured or unreadable.
async fn signals_listener_tls(network: &GridNetwork, client: &Client) -> Option<ServerTlsConfig> {
    match grid_network::signals_server_config(network, client).await {
        Ok(config) => {
            if config.is_none() {
                tracing::info!("signals TLS not configured; rollup not exposed");
            }
            config
        },
        Err(error) => {
            // Configured but unreadable: fail closed rather than serve plaintext.
            tracing::error!(%error, "signals TLS configured but unavailable; not serving (fail closed)");
            None
        },
    }
}

/// This site's own leaf from the network this operator serves.
async fn sole_own_leaf(client: &Client) -> Option<grid_network::OwnLeaf> {
    let network = Box::pin(sole_network(client)).await?;
    Box::pin(own_leaf(&network, client)).await
}

/// This site's own leaf, logging a read error.
async fn own_leaf(network: &GridNetwork, client: &Client) -> Option<grid_network::OwnLeaf> {
    match grid_network::own_leaf_identity(network, client).await {
        Ok(own) => own,
        Err(error) => {
            tracing::warn!(%error, "this site's own certificate is unreadable; its workloads cannot be recognised");
            None
        },
    }
}

/// Scrape this site's providers on their own interval and publish the result.
///
/// Separate from reconcile, which runs on an interval sized for declarations
/// and is two orders of magnitude slower than these values move.
async fn run_local_scraper(
    interval: Option<std::time::Duration>,
    ctx: Arc<OperatorCtx>,
    client: Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(interval) = interval else {
        return Ok(());
    };
    tracing::info!(interval_ms = interval.as_millis(), "local signals scraper started");
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    #[expect(
        clippy::infinite_loop,
        reason = "runs for the process lifetime alongside the controllers"
    )]
    loop {
        ticker.tick().await;
        let networks: Api<GridNetwork> = Api::all(client.clone());
        let Ok(list) = networks.list(&kube::api::ListParams::default()).await else {
            continue;
        };
        for network in list.items.iter().filter_map(|n| n.metadata.name.as_deref()) {
            if let Err(error) = grid_network::refresh_signals(&ctx, &client, network).await {
                tracing::warn!(network, %error, "local signals refresh failed");
            }
        }
    }
}

/// Poll providers that opt into model discovery and hold what they serve.
///
/// Separate from reconcile: a served set must expire on its own cadence.
async fn run_model_discovery(
    ctx: Arc<OperatorCtx>,
    client: Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = model_discovery_config();
    tracing::info!(
        interval_secs = config.interval.as_secs(),
        ttl_secs = config.ttl.as_secs(),
        "model discovery started"
    );

    let mut ticker = tokio::time::interval(config.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    #[expect(
        clippy::infinite_loop,
        reason = "runs for the process lifetime alongside the controllers"
    )]
    loop {
        ticker.tick().await;
        if let Err(error) = grid_network::refresh_served_models(&ctx, &client, &config).await {
            tracing::warn!(%error, "model discovery round failed");
        }
    }
}

/// Maximum amount of time to retain a discovered model set.
const MAX_MODEL_DISCOVERY_TTL: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 60 * 60);

/// Maximum delay between model-discovery rounds.
const MAX_MODEL_DISCOVERY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30 * 24 * 60 * 60);

/// Maximum time allowed for one model-discovery request.
const MAX_MODEL_DISCOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Discovery cadence from `GRID_MODEL_DISCOVERY_*`.
///
/// Interval and timeout are bounded; the TTL covers both and is capped at one
/// year.
fn model_discovery_config() -> served_models::DiscoveryConfig {
    let defaults = served_models::DiscoveryConfig::default();
    let interval = bounded_discovery_duration(
        "GRID_MODEL_DISCOVERY_INTERVAL_SECS",
        defaults.interval,
        MAX_MODEL_DISCOVERY_INTERVAL,
    );
    let timeout = bounded_discovery_duration(
        "GRID_MODEL_DISCOVERY_TIMEOUT_SECS",
        defaults.timeout,
        MAX_MODEL_DISCOVERY_TIMEOUT,
    );
    let ttl = bounded_model_discovery_ttl(interval, timeout, defaults.ttl);

    served_models::DiscoveryConfig {
        interval,
        timeout,
        ttl,
        concurrency: parse_env_or("GRID_MODEL_DISCOVERY_CONCURRENCY", defaults.concurrency),
    }
}

/// Derive a TTL that covers one poll round and remains within its supported cap.
fn bounded_model_discovery_ttl(
    interval: std::time::Duration,
    timeout: std::time::Duration,
    default: std::time::Duration,
) -> std::time::Duration {
    let requested_ttl =
        std::time::Duration::from_secs(parse_env_or("GRID_MODEL_DISCOVERY_TTL_SECS", default.as_secs()).max(1));
    let minimum_ttl = interval.saturating_add(timeout);
    let unclamped_ttl = requested_ttl.max(minimum_ttl);
    let ttl = unclamped_ttl.min(MAX_MODEL_DISCOVERY_TTL);
    if unclamped_ttl > MAX_MODEL_DISCOVERY_TTL {
        tracing::warn!(
            requested_ttl_secs = requested_ttl.as_secs(),
            minimum_ttl_secs = minimum_ttl.as_secs(),
            applied_ttl_secs = ttl.as_secs(),
            max_ttl_secs = MAX_MODEL_DISCOVERY_TTL.as_secs(),
            "model discovery TTL exceeds the supported maximum; clamping"
        );
    }
    ttl
}

/// Parse one discovery duration, enforcing a positive value and maximum.
fn bounded_discovery_duration(
    name: &str,
    default: std::time::Duration,
    maximum: std::time::Duration,
) -> std::time::Duration {
    let requested = std::time::Duration::from_secs(parse_env_or(name, default.as_secs()).max(1));
    if requested > maximum {
        tracing::warn!(
            setting = name,
            requested_secs = requested.as_secs(),
            applied_secs = maximum.as_secs(),
            "model discovery duration exceeds the supported maximum; clamping"
        );
    }
    requested.min(maximum)
}

/// Parse an environment variable, using `default` when it is absent or invalid.
fn parse_env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Poll every alive peer's signals endpoint on a coarse interval, fail closed.
///
/// The wire path is mTLS only, so a round requires verified client material.
/// Without it the poller idles rather than polling peers in plaintext, and it
/// rebuilds when this site's certificate changes.
async fn run_peer_poller(
    settings: Option<operator::cli::SignalsArgs>,
    ctx: Arc<OperatorCtx>,
    swim: SwimStartup,
    client: Client,
    shutdown: operator::shutdown::Shutdown,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(settings) = settings else {
        return Ok(());
    };
    let Some(swim) = swim_settled(swim).await else {
        tracing::info!("peer signals poller disabled: SWIM is not running");
        return Ok(());
    };
    PeerPoller {
        ctx,
        swim,
        client,
        settings,
        shutdown,
    }
    .run()
    .await;
    Ok(())
}

/// The invariant state one peer-polling session threads through every round.
struct PeerPoller {
    /// Where polled peer signals are published and peer identities are read.
    ctx: Arc<OperatorCtx>,
    /// Membership, for the alive peers and their advertised addresses.
    swim: Arc<swim_runtime::SwimHandle>,
    /// Kube client, for resolving this site's TLS material.
    client: Client,
    /// Poll cadence, timeouts, and what to collect.
    settings: operator::cli::SignalsArgs,
    /// Lets an in-flight round stand down when the process is stopping.
    shutdown: operator::shutdown::Shutdown,
}

impl PeerPoller {
    /// Rebuild and poll until shutdown.
    async fn run(&self) {
        while !self.shutdown.is_triggered() && self.cycle().await {}
        tracing::info!("peer signals poller stopped");
    }

    /// One build-and-poll cycle; `false` means shutdown arrived while idle.
    async fn cycle(&self) -> bool {
        let own = Box::pin(sole_own_leaf(&self.client)).await;
        let Some(source) = Box::pin(peer_source(
            &self.client,
            self.shutdown.clone(),
            self.ctx.peer_settings().trust,
            &self.settings,
        ))
        .await
        else {
            tracing::warn!("peer signals poller idle: TLS material unavailable (fail closed)");
            return self.idle_until_retry().await;
        };
        tracing::info!(
            interval_secs = self.settings.peer_interval_secs,
            source = "poll",
            "peer signals poller started"
        );
        self.poll_until_changed(&source, material_changed(self.client.clone(), own))
            .await;
        true
    }

    /// Wait out the TLS poll interval; `false` if shutdown arrives first.
    async fn idle_until_retry(&self) -> bool {
        tokio::select! {
            biased;
            () = self.shutdown.triggered() => false,
            () = tokio::time::sleep(SIGNALS_TLS_POLL) => true,
        }
    }

    /// Poll every interval until this site's material changes, or shutdown.
    async fn poll_until_changed(
        &self,
        source: &operator::signals::PollPeers,
        changed: impl Future<Output = ()> + Send,
    ) {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(self.settings.peer_interval_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut changed = std::pin::pin!(changed);
        loop {
            tokio::select! {
                biased;
                () = self.shutdown.triggered() => return,
                () = &mut changed => {
                    tracing::info!("peer signals TLS material changed; rebuilding the client");
                    return;
                },
                _ = ticker.tick() => {},
            }
            self.poll_once(source).await;
        }
    }

    /// Collect from every alive peer and replace what is published for them.
    async fn poll_once(&self, source: &operator::signals::PollPeers) {
        let targets = poll_targets(
            &self.swim.snapshot(),
            &self.ctx.peer_identities(),
            source.trust,
            self.swim.is_encrypted(),
            self.swim.site_name(),
            self.settings.peer_port,
        );
        let Some(sites) = targets.filter(|sites| !sites.is_empty()) else {
            return;
        };
        // Each peer lands as it answers, so one slow peer cannot age out the rest.
        let peers = self.ctx.peers();
        let ttl = self.settings.peer_ttl();
        source
            .collect_each(&sites, |peer, observations| {
                peers.refresh(BTreeMap::from([(peer, observations)]), ttl);
            })
            .await;
    }
}

/// Alive peers this site would name under `trust`, `None` in SPIFFE mode over plaintext gossip.
#[expect(
    clippy::too_many_arguments,
    reason = "membership, trust, and addressing inputs of one round"
)]
fn poll_targets(
    snapshot: &operator::swim::MembershipSnapshot,
    identities: &operator::signals::PeerIdentities,
    trust: operator::signals::PeerTrustMode,
    encrypted: bool,
    local_site: &str,
    fallback_port: u16,
) -> Option<Vec<operator::signals::PeerSite>> {
    let pinned = trust == operator::signals::PeerTrustMode::Pin;
    if !pinned && !encrypted {
        tracing::debug!("SPIFFE peer polling waits for encrypted gossip");
        return None;
    }
    let alive = snapshot
        .members
        .iter()
        .filter(|m| m.status == operator::swim::MemberStatus::Alive)
        .filter(|m| {
            if pinned {
                !identities.refuses(&m.site_id)
            } else {
                identities.labels_for(&m.site_id).is_some()
            }
        });
    let mut sites = operator::signals::peer_sites(alive, local_site, "https", fallback_port);
    if pinned {
        for site in &mut sites {
            site.pins = identities.pins_for(&site.name);
        }
    }
    Some(sites)
}

/// Build the peer poller source from verified TLS material, or `None` when absent.
async fn peer_source(
    client: &Client,
    shutdown: operator::shutdown::Shutdown,
    trust: operator::signals::PeerTrustMode,
    settings: &operator::cli::SignalsArgs,
) -> Option<operator::signals::PollPeers> {
    let tls = peer_tls(client).await?;
    Some(operator::signals::PollPeers {
        timeout: std::time::Duration::from_secs(settings.peer_timeout_secs),
        tls: Some(tls),
        collect: settings.peer_collect(),
        concurrency: settings.peer_concurrency,
        attempts: settings.peer_attempts,
        backoff: std::time::Duration::from_millis(settings.peer_backoff_ms),
        budget: std::time::Duration::from_secs(settings.peer_budget_secs),
        slow_after: std::time::Duration::from_millis(settings.peer_slow_ms),
        shutdown,
        trust,
    })
}

/// Client TLS for peer polling, or `None` when unconfigured or unreadable.
///
/// Fails closed: configured-but-unreadable material returns `None` so the
/// poller idles rather than dialing peers in plaintext.
async fn peer_tls(client: &Client) -> Option<Arc<operator::signals::PeerTlsMaterial>> {
    let networks: Api<GridNetwork> = Api::all(client.clone());
    let list = networks.list(&kube::api::ListParams::default()).await.ok()?;
    let network = list.items.into_iter().next()?;
    match grid_network::peer_tls_config(&network, client).await {
        Ok(Some(material)) => Some(material),
        Ok(None) => {
            tracing::info!("peer signals: no TLS configured; peers not polled");
            None
        },
        Err(error) => {
            tracing::warn!(%error, "peer signals TLS configured but unavailable; not polling (fail closed)");
            None
        },
    }
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    /// A caller is scoped from its certificate, and nothing is trusted for
    /// presenting nothing: only this site's own certificate earns `Local`, and
    /// a missing or undeclared certificate is served nothing.
    #[test]
    fn signals_caller_scope_requires_a_positive_credential() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let leaf = [1_u8, 2, 3, 4];
        let own = |fingerprint: String| {
            Some(grid_network::OwnLeaf {
                fingerprint,
                spiffe: None,
            })
        };

        let owner = SignalsIdentity {
            peers: operator::signals::PeerIdentities::new(),
            own: own(operator::signals::leaf_fingerprint(&leaf)),
            trust: operator::signals::PeerTrustMode::Pin,
        };
        // A caller that presents no certificate is served nothing, never Local.
        assert_eq!(caller_for(None, &owner, addr), (Caller::Peer(None), None));
        // This site's own certificate is the only key that earns Local.
        assert_eq!(caller_for(Some(&leaf), &owner, addr), (Caller::Local, None));

        // A certificate this site has not declared is served nothing, not Local.
        let stranger = SignalsIdentity {
            peers: operator::signals::PeerIdentities::new(),
            own: own("0".repeat(64)),
            trust: operator::signals::PeerTrustMode::Pin,
        };
        assert_eq!(caller_for(Some(&leaf), &stranger, addr), (Caller::Peer(None), None));
    }

    /// DER of a Grid-CA site certificate for `site`.
    fn site_der(site: &str) -> Vec<u8> {
        let ca = certs::generate_ca("grid-ca").expect("fixture");
        let cert = certs::generate_site_cert(&ca, site).expect("fixture");
        pem_der(&cert.cert_pem)
    }

    /// DER of the first PEM block.
    fn pem_der(pem: &str) -> Vec<u8> {
        pem::parse(pem).expect("fixture").into_contents()
    }

    /// Labels held for every known site in these tests.
    fn gpu() -> BTreeMap<String, String> {
        BTreeMap::from([("tier".to_owned(), "gpu".to_owned())])
    }

    /// A listener for site `hub` under `trust`, knowing `east` (pinned to `east_der`) and `west` (no pins).
    fn hub_identity(trust: operator::signals::PeerTrustMode, own: &[u8], east_der: &[u8]) -> SignalsIdentity {
        let record = |pins: Vec<String>| operator::signals::PeerRecord { labels: gpu(), pins };
        let peers = operator::signals::PeerIdentities::new();
        peers.set(BTreeMap::from([
            (
                "east".to_owned(),
                record(vec![operator::signals::leaf_fingerprint(east_der)]),
            ),
            ("west".to_owned(), record(Vec::new())),
        ]));
        SignalsIdentity {
            peers,
            own: Some(grid_network::OwnLeaf {
                fingerprint: operator::signals::leaf_fingerprint(own),
                spiffe: Some(certs::spiffe_id("hub")),
            }),
            trust,
        }
    }

    /// The same callers under both trust modes.
    #[test]
    #[expect(clippy::too_many_lines, reason = "case table")]
    fn signals_caller_by_trust_mode() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let (own, east, west, stranger) = (site_der("hub"), site_der("east"), site_der("west"), site_der("nowhere"));
        let reissued = site_der("hub");
        let pin = hub_identity(operator::signals::PeerTrustMode::Pin, &own, &east);
        let spiffe = hub_identity(operator::signals::PeerTrustMode::Spiffe, &own, &east);
        let named = || Caller::Peer(Some(gpu()));
        let cases = [
            ("own cert", Some(own.as_slice()), &pin, Caller::Local),
            ("pinned peer", Some(east.as_slice()), &pin, named()),
            ("unpinned peer", Some(west.as_slice()), &pin, Caller::Peer(None)),
            (
                "own site, another leaf",
                Some(reissued.as_slice()),
                &pin,
                Caller::Peer(None),
            ),
            ("own spiffe id", Some(own.as_slice()), &spiffe, Caller::Local),
            (
                "own spiffe id, another leaf",
                Some(reissued.as_slice()),
                &spiffe,
                Caller::Peer(None),
            ),
            ("known site", Some(east.as_slice()), &spiffe, named()),
            ("known site without pins", Some(west.as_slice()), &spiffe, named()),
            ("unknown site", Some(stranger.as_slice()), &spiffe, Caller::Peer(None)),
            ("junk leaf", Some(b"junk".as_slice()), &spiffe, Caller::Peer(None)),
            ("no certificate", None, &spiffe, Caller::Peer(None)),
        ];
        for (label, leaf, listener, want) in cases {
            let (caller, site) = caller_for(leaf, listener, addr);
            assert_eq!(caller, want, "{label}");
            let counted = matches!(want, Caller::Peer(Some(_)))
                .then(|| if leaf == Some(west.as_slice()) { "west" } else { "east" });
            assert_eq!(site.as_deref(), counted, "{label}: counted against the resolved site");
        }
    }

    /// Fresh caps, and a handshake slot for loopback against them.
    fn handshaking(sources: &SourceSlots, peers: &PeerSlots) -> Handshaking {
        Handshaking {
            source: sources
                .acquire(std::net::IpAddr::from([127, 0, 0, 1]))
                .expect("a free slot"),
            peers: peers.clone(),
        }
    }

    #[tokio::test(start_paused = true)]
    #[expect(clippy::too_many_lines, reason = "server TLS setup and a silent client")]
    async fn a_silent_client_is_dropped_at_the_handshake_timeout() {
        let ca = certs::generate_ca("grid-ca").expect("fixture");
        let site = certs::generate_site_cert(&ca, "east").expect("fixture");
        let tls = tls_backend::build_server_config(
            ca.cert_pem.as_bytes(),
            site.cert_pem.as_bytes(),
            site.key_pem.as_bytes(),
        )
        .expect("fixture");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("fixture");
        let addr = listener.local_addr().expect("fixture");
        // Connects and never speaks.
        let _silent = tokio::net::TcpStream::connect(addr).await.expect("fixture");
        let (stream, remote) = listener.accept().await.expect("fixture");
        let identity = SignalsIdentity {
            peers: operator::signals::PeerIdentities::new(),
            own: None,
            trust: operator::signals::PeerTrustMode::Pin,
        };
        let sources = SourceSlots::new(1, 1);
        let started = tokio::time::Instant::now();
        let serving = Serving {
            tls,
            app: axum::Router::new(),
            identity,
        };
        let serve = Box::pin(serve_signals_connection(
            serving,
            stream,
            remote,
            handshaking(&sources, &PeerSlots::new(1, 8)),
        ));
        tokio::time::timeout(SIGNALS_HANDSHAKE_TIMEOUT * 2, serve)
            .await
            .expect("released by the handshake bound");
        assert_eq!(
            started.elapsed(),
            SIGNALS_HANDSHAKE_TIMEOUT,
            "released exactly at the bound"
        );
        assert!(
            sources.acquire(remote.ip()).is_some(),
            "a failed handshake frees its source slot"
        );
    }

    /// A rustls client presenting `client` (a Grid-CA site cert) to a server named `east`.
    #[cfg(not(feature = "fips"))]
    fn site_client(ca: &certs::CaCert, client: &certs::SiteCertOutput) -> tokio_rustls::TlsConnector {
        use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _};
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(ca.cert_pem.as_bytes()).expect("fixture"))
            .expect("fixture");
        let chain = vec![CertificateDer::from_pem_slice(client.cert_pem.as_bytes()).expect("fixture")];
        let key = PrivateKeyDer::from_pem_slice(client.key_pem.as_bytes()).expect("fixture");
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("fixture")
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .expect("fixture");
        tokio_rustls::TlsConnector::from(Arc::new(config))
    }

    /// A Grid CA, a server `east`, and a client `west`.
    #[cfg(not(feature = "fips"))]
    struct Mesh {
        /// The Grid CA.
        ca: certs::CaCert,
        /// The listening site.
        east: certs::SiteCertOutput,
        /// The calling site.
        west: certs::SiteCertOutput,
    }

    #[cfg(not(feature = "fips"))]
    impl Mesh {
        fn new() -> Self {
            let ca = certs::generate_ca("grid-ca").expect("fixture");
            let east = certs::generate_site_cert(&ca, "east").expect("fixture");
            let west = certs::generate_site_cert(&ca, "west").expect("fixture");
            Self { ca, east, west }
        }

        /// A listener on which `west` is this site's own, Local, caller.
        fn west_is_local(&self) -> SignalsIdentity {
            SignalsIdentity {
                peers: operator::signals::PeerIdentities::new(),
                own: Some(grid_network::OwnLeaf {
                    fingerprint: operator::signals::leaf_fingerprint(&pem_der(&self.west.cert_pem)),
                    spiffe: None,
                }),
                trust: operator::signals::PeerTrustMode::Pin,
            }
        }

        /// A listener that pins `west` as a peer.
        fn west_is_pinned(&self) -> SignalsIdentity {
            let peers = operator::signals::PeerIdentities::new();
            peers.set(BTreeMap::from([(
                "west".to_owned(),
                operator::signals::PeerRecord {
                    labels: gpu(),
                    pins: vec![operator::signals::leaf_fingerprint(&pem_der(&self.west.cert_pem))],
                },
            )]));
            SignalsIdentity {
                peers,
                own: None,
                trust: operator::signals::PeerTrustMode::Pin,
            }
        }

        /// Serve `connections` connections as `east`, each with a fresh handshake slot.
        async fn serve(
            &self,
            identity: SignalsIdentity,
            app: axum::Router,
            (sources, peers): (&SourceSlots, &PeerSlots),
            connections: usize,
        ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
            let tls = tls_backend::build_server_config(
                self.ca.cert_pem.as_bytes(),
                self.east.cert_pem.as_bytes(),
                self.east.key_pem.as_bytes(),
            )
            .expect("fixture");
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("fixture");
            let addr = listener.local_addr().expect("fixture");
            let (sources, peers) = (sources.clone(), peers.clone());
            let server = tokio::spawn(async move {
                let mut served = tokio::task::JoinSet::new();
                for _ in 0..connections {
                    let (stream, remote) = listener.accept().await.expect("fixture");
                    let handshaking = handshaking(&sources, &peers);
                    let serving = Serving {
                        tls: Arc::clone(&tls),
                        app: app.clone(),
                        identity: identity.clone(),
                    };
                    served.spawn(serve_signals_connection(serving, stream, remote, handshaking));
                }
                served.join_all().await;
            });
            (addr, server)
        }

        /// Connect to `addr` as `west`.
        async fn connect(&self, addr: SocketAddr) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
            let tcp = tokio::net::TcpStream::connect(addr).await.expect("fixture");
            let name = rustls::pki_types::ServerName::try_from("east.grid.internal").expect("fixture");
            site_client(&self.ca, &self.west)
                .connect(name, tcp)
                .await
                .expect("handshake")
        }
    }

    /// One request on `conn`, returning the start of the response, empty once the server hung up.
    #[cfg(not(feature = "fips"))]
    async fn get(conn: &mut tokio_rustls::client::TlsStream<tokio::net::TcpStream>) -> Vec<u8> {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        if conn
            .write_all(b"GET /v1/site/signals HTTP/1.1\r\nHost: east\r\n\r\n")
            .await
            .is_err()
        {
            return Vec::new();
        }
        let mut answer = vec![0_u8; 512];
        let read = conn.read(&mut answer).await.unwrap_or(0);
        answer.truncate(read);
        answer
    }

    /// An app answering `ok` on the signals path.
    #[cfg(not(feature = "fips"))]
    fn ok_app() -> axum::Router {
        axum::Router::new().route(operator::signals::SIGNALS_PATH, axum::routing::get(|| async { "ok" }))
    }

    /// An unnamed caller is closed after the handshake without an HTTP answer.
    #[cfg(not(feature = "fips"))]
    #[tokio::test]
    async fn an_unnamed_caller_is_closed_unanswered() {
        use tokio::io::AsyncReadExt as _;
        let mesh = Mesh::new();
        let identity = SignalsIdentity {
            peers: operator::signals::PeerIdentities::new(),
            own: None,
            trust: operator::signals::PeerTrustMode::Pin,
        };
        let (sources, peers) = (SourceSlots::new(1, 1), PeerSlots::new(1, 8));
        let (addr, server) = mesh.serve(identity, ok_app(), (&sources, &peers), 1).await;
        let mut conn = mesh.connect(addr).await;
        let mut rest = Vec::new();
        let read = tokio::time::timeout(std::time::Duration::from_secs(5), conn.read_to_end(&mut rest)).await;
        assert!(read.is_ok() && rest.is_empty(), "closed with no answer");
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("server finished")
            .expect("server task");
    }

    /// An authenticated caller's idle keep-alive connection closes at the header read bound.
    #[cfg(not(feature = "fips"))]
    #[tokio::test]
    async fn an_idle_peer_connection_closes_at_the_header_read_bound() {
        use tokio::io::AsyncReadExt as _;
        let mesh = Mesh::new();
        let (sources, peers) = (SourceSlots::new(1, 1), PeerSlots::new(1, 8));
        let (addr, server) = mesh.serve(mesh.west_is_local(), ok_app(), (&sources, &peers), 1).await;
        let mut conn = mesh.connect(addr).await;
        let head = get(&mut conn).await;
        assert!(head.starts_with(b"HTTP/1.1 200"), "{}", String::from_utf8_lossy(&head));
        assert!(
            sources.acquire(std::net::IpAddr::from([127, 0, 0, 1])).is_some(),
            "an authenticated connection holds no source slot"
        );
        // Real time for the handshake, virtual time for the idle wait.
        tokio::time::pause();
        let idle = tokio::time::Instant::now();
        let mut rest = Vec::new();
        let closed = tokio::time::timeout(SIGNALS_HEADER_READ_TIMEOUT * 2, conn.read_to_end(&mut rest)).await;
        assert!(closed.is_ok(), "the server kept an idle connection open");
        assert!(idle.elapsed() >= SIGNALS_HEADER_READ_TIMEOUT, "held until the bound");
        server.await.expect("server task");
    }

    /// A site past its cap is answered 503 and closed, while its first connection serves.
    #[cfg(not(feature = "fips"))]
    #[tokio::test]
    async fn a_peer_site_is_capped_after_the_handshake() {
        use tokio::io::AsyncReadExt as _;
        let mesh = Mesh::new();
        let (sources, peers) = (SourceSlots::new(8, 8), PeerSlots::new(1, 8));
        let (addr, _server) = mesh.serve(mesh.west_is_pinned(), ok_app(), (&sources, &peers), 2).await;
        let mut first = mesh.connect(addr).await;
        assert!(
            get(&mut first).await.starts_with(b"HTTP/1.1 200"),
            "the first connection serves"
        );
        let mut second = mesh.connect(addr).await;
        let head = get(&mut second).await;
        assert!(head.starts_with(b"HTTP/1.1 503"), "{}", String::from_utf8_lossy(&head));
        let mut rest = Vec::new();
        let closed = tokio::time::timeout(std::time::Duration::from_secs(5), second.read_to_end(&mut rest)).await;
        assert!(closed.is_ok(), "then closed");
        assert!(
            get(&mut first).await.starts_with(b"HTTP/1.1 200"),
            "the first still serves"
        );
    }

    #[test]
    #[expect(clippy::significant_drop_tightening, reason = "slots are held to reach the cap")]
    fn peers_never_take_the_local_reserve() {
        let peers = PeerSlots::new(2, 3);
        let held: Vec<PeerSlot> = ["a", "b", "c"].iter().filter_map(|site| peers.acquire(site)).collect();
        assert_eq!(held.len(), 3, "one per site up to the peer total");
        assert!(peers.acquire("d").is_none(), "the peer total holds");
        drop(held);
        let same: Vec<PeerSlot> = (0..3).filter_map(|_| peers.acquire("a")).collect();
        assert_eq!(same.len(), 2, "per site");
        let admission = Admission::new(4);
        assert_eq!(
            admission.peers.total.available_permits(),
            SIGNALS_MAX_CONNECTIONS - SIGNALS_LOCAL_RESERVE,
            "the gateway keeps its reserve"
        );
    }

    /// A caller that keeps asking is closed once the connection reaches its maximum age.
    #[tokio::test(start_paused = true)]
    async fn a_busy_connection_closes_at_its_maximum_age() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let (mut client, server_io) = tokio::io::duplex(4096);
        let service = hyper::service::service_fn(|_request: http::Request<hyper::body::Incoming>| {
            std::future::ready(Ok::<_, std::convert::Infallible>(http::Response::new(
                http_body_util::Full::new(bytes::Bytes::from_static(b"ok")),
            )))
        });
        let connection = hyper::server::conn::http1::Builder::new()
            .timer(hyper_util::rt::TokioTimer::new())
            .header_read_timeout(SIGNALS_HEADER_READ_TIMEOUT)
            .serve_connection(hyper_util::rt::TokioIo::new(server_io), service);
        let server = tokio::spawn(serve_until_aged(connection, SIGNALS_MAX_CONNECTION_AGE));
        let opened = tokio::time::Instant::now();
        let mut answer = [0_u8; 256];
        loop {
            let asked = client.write_all(b"GET / HTTP/1.1\r\nHost: east\r\n\r\n").await;
            let read = client.read(&mut answer).await.unwrap_or(0);
            if asked.is_err() || read == 0 {
                break;
            }
            assert!(opened.elapsed() <= SIGNALS_MAX_CONNECTION_AGE, "outlived its age");
            tokio::time::sleep(SIGNALS_HEADER_READ_TIMEOUT / 2).await;
        }
        assert!(
            opened.elapsed() >= SIGNALS_MAX_CONNECTION_AGE,
            "closed at its age, not before"
        );
        assert!(server.await.expect("server task").is_ok(), "a graceful close");
    }

    /// A caller that stops reading a large response is dropped at the write bound.
    #[cfg(not(feature = "fips"))]
    #[tokio::test]
    async fn a_stalled_reader_is_dropped_at_the_write_timeout() {
        use tokio::io::AsyncWriteExt as _;
        let mesh = Mesh::new();
        let body = "x".repeat(64 << 20);
        let app = axum::Router::new().route(
            operator::signals::SIGNALS_PATH,
            axum::routing::get(move || std::future::ready(body.clone())),
        );
        let (sources, peers) = (SourceSlots::new(1, 1), PeerSlots::new(1, 8));
        let (addr, server) = mesh.serve(mesh.west_is_local(), app, (&sources, &peers), 1).await;
        let mut conn = mesh.connect(addr).await;
        conn.write_all(b"GET /v1/site/signals HTTP/1.1\r\nHost: east\r\n\r\n")
            .await
            .expect("request");
        tokio::time::pause();
        let started = tokio::time::Instant::now();
        tokio::time::timeout(SIGNALS_WRITE_TIMEOUT * 3, server)
            .await
            .expect("the stalled connection was dropped")
            .expect("server task");
        assert!(started.elapsed() >= SIGNALS_WRITE_TIMEOUT, "held until the write bound");
        drop(conn);
    }

    /// A queue error carrying an apiserver status with `code`.
    fn queue_status(code: u16, as_event: bool) -> kube::runtime::controller::Error<std::io::Error, watcher::Error> {
        let mut status = kube::core::Status::failure("gone", "Expired");
        status.code = code;
        let status = Box::new(status);
        kube::runtime::controller::Error::QueueError(if as_event {
            watcher::Error::WatchError(status)
        } else {
            watcher::Error::WatchFailed(kube::Error::Api(status))
        })
    }

    #[test]
    fn only_an_expired_watch_is_routine() {
        let cases = [
            ("watch event 410", queue_status(410, true), true),
            ("watch stream 410", queue_status(410, false), true),
            ("watch event 500", queue_status(500, true), false),
            (
                "no resource version",
                kube::runtime::controller::Error::QueueError(watcher::Error::NoResourceVersion),
                false,
            ),
        ];
        for (label, error, want) in cases {
            assert_eq!(watch_expired(&error), want, "{label}");
        }
    }

    #[test]
    fn the_startup_key_holds_until_a_network_declares_none() {
        let network = |key: Option<&str>| {
            let tls = key.map_or_else(
                || serde_json::json!({}),
                |name| serde_json::json!({"swimKeyRef": {"name": name, "namespace": "grid"}}),
            );
            let spec = serde_json::from_value(serde_json::json!({"seeds": [], "tls": tls}));
            GridNetwork::new("net", spec.unwrap_or_else(|_| std::process::abort()))
        };
        let state = |networks: &[GridNetwork], require_key: bool| {
            unkeyed_state(&declared_of(networks), require_key)
                .map(|state| format!("{state:?}"))
                .ok()
        };
        assert_eq!(
            state(&[], true),
            Some("Pending".to_owned()),
            "no network yet holds by default"
        );
        assert_eq!(state(&[], false), Some("Plain".to_owned()), "opted out");
        assert_eq!(
            state(&[network(None)], true),
            Some("Plain".to_owned()),
            "declares no key"
        );
        let declared = declared_of(&[network(None), network(Some("swim-key"))]);
        assert!(
            matches!(unkeyed_state(&declared, true), Err(key_ref) if key_ref.name == "swim-key"),
            "reads the key"
        );
    }

    #[tokio::test]
    async fn gated_tasks_wait_for_swim_to_settle() {
        let (started, startup) = tokio::sync::watch::channel(SwimStage::Starting);
        let mut settled = Box::pin(swim_settled(startup));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut settled)
                .await
                .is_err(),
            "still starting"
        );
        started.send_replace(SwimStage::Settled(None));
        assert!(settled.await.is_none(), "settled without SWIM");
        assert!(readiness(&std::sync::atomic::AtomicBool::new(false)).0 == http::StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test(start_paused = true)]
    async fn membership_writes_wait_for_a_peer_or_the_grace() {
        let seen = std::sync::atomic::AtomicBool::new(false);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let changes = futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|()| ((), rx)) });
        let wait = converged(
            || seen.load(std::sync::atomic::Ordering::Acquire),
            changes,
            MEMBERSHIP_GRACE,
        );
        let mut wait = std::pin::pin!(wait);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), &mut wait)
                .await
                .is_err(),
            "no peer yet"
        );
        seen.store(true, std::sync::atomic::Ordering::Release);
        tx.send(()).unwrap_or_else(|_| std::process::abort());
        let started = tokio::time::Instant::now();
        wait.await;
        assert!(started.elapsed() < MEMBERSHIP_GRACE, "a peer releases before the grace");

        let alone = tokio::time::Instant::now();
        converged(|| false, futures::stream::pending(), MEMBERSHIP_GRACE).await;
        assert_eq!(alone.elapsed(), MEMBERSHIP_GRACE, "a lone site writes after the grace");
    }

    #[test]
    fn startup_key_retries_back_off_to_a_bound() {
        let delays: Vec<u64> = startup_retry_delays().take(9).map(|d| d.as_secs()).collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 32, 60, 60, 60]);
    }

    #[test]
    fn a_malformed_env_key_is_an_error_not_plaintext() {
        let good = "ab".repeat(32);
        assert_eq!(parse_swim_key(&good), Ok([0xAB; 32]));
        assert_eq!(parse_swim_key(&format!(" {good}\n")), Ok([0xAB; 32]), "trimmed");
        for bad in ["", "ab", &"zz".repeat(32), &"ab".repeat(33)] {
            assert!(parse_swim_key(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn handshake_sources_are_keyed_by_address_scope() {
        let ip = |text: &str| text.parse::<std::net::IpAddr>().expect("ip");
        let cases = [
            ("ipv4 per address", "10.0.0.1", "10.0.0.1", None),
            ("mapped ipv4 is ipv4", "::ffff:10.0.0.1", "10.0.0.1", None),
            (
                "global ipv6 per /64 and /48",
                "2001:db8:1:2::1",
                "2001:db8:1:2::",
                Some("2001:db8:1::"),
            ),
            ("unique-local per address", "fd00::1", "fd00::1", None),
            ("link-local per address", "fe80::1", "fe80::1", None),
        ];
        for (label, source, key, wider) in cases {
            assert_eq!(source_keys(ip(source)), (ip(key), wider.map(ip)), "{label}");
        }
    }

    #[test]
    fn handshake_slots_cap_a_source_and_a_global_48() {
        let slots = SourceSlots::new(1, 2);
        let ip = |text: &str| text.parse::<std::net::IpAddr>().expect("ip");
        let first = slots.acquire(ip("2001:db8:1:1::1"));
        assert!(first.is_some());
        assert!(slots.acquire(ip("2001:db8:1:1::ffff")).is_none(), "same /64");
        let second = slots.acquire(ip("2001:db8:1:2::1"));
        assert!(second.is_some(), "another /64");
        assert!(slots.acquire(ip("2001:db8:1:3::1")).is_none(), "the /48 is full");
        assert!(slots.acquire(ip("2001:db8:2:1::1")).is_some(), "another /48");
        assert!(
            slots.acquire(ip("fd00::1")).is_some() && slots.acquire(ip("fd00::2")).is_some(),
            "ULA per address"
        );
        drop(first);
        assert!(
            slots.acquire(ip("2001:db8:1:3::1")).is_some(),
            "a released slot frees the /48"
        );
        drop(second);
    }

    #[test]
    #[expect(clippy::significant_drop_tightening, reason = "slots are held to reach the cap")]
    fn peer_slots_cap_one_site_and_release_on_drop() {
        let slots = PeerSlots::new(2, 8);
        let first = slots.acquire("aa");
        let second = slots.acquire("aa");
        assert!(first.is_some() && second.is_some(), "up to the cap");
        assert!(slots.acquire("aa").is_none(), "past the cap");
        assert!(slots.acquire("bb").is_some(), "another leaf is unaffected");
        drop(first);
        assert!(slots.acquire("aa").is_some(), "a closed connection frees its slot");
    }

    #[tokio::test]
    async fn the_ipv6_wildcard_accepts_ipv4() {
        let bound = bind_dual_stack(SocketAddr::from((std::net::Ipv6Addr::UNSPECIFIED, 0)));
        if bound.as_ref().is_err_and(lacks_ipv6) {
            return;
        }
        let listener = bound.expect("bind");
        assert_eq!(socket2::SockRef::from(&listener).only_v6().ok(), Some(false));
        let port = listener.local_addr().expect("addr").port();
        let connect = tokio::net::TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], port)));
        let (connected, accepted) = tokio::join!(connect, listener.accept());
        assert!(
            connected.is_ok() && accepted.is_ok(),
            "an IPv4 client reaches the wildcard"
        );
    }

    #[tokio::test]
    #[expect(clippy::too_many_lines, reason = "every bind outcome against one fake binder")]
    async fn the_default_bind_falls_back_to_ipv4_without_ipv6() {
        let refuse_v6 = |kind: Option<std::io::ErrorKind>, raw: Option<i32>| {
            move |addr: SocketAddr| {
                std::future::ready(match (addr.is_ipv6(), kind, raw) {
                    (true, Some(kind), _) => Err(std::io::Error::from(kind)),
                    (true, None, Some(raw)) => Err(std::io::Error::from_raw_os_error(raw)),
                    _ => Ok(addr),
                })
            }
        };
        let none = refuse_v6(None, None);
        assert_eq!(
            bind_with_fallback(None, none).await.ok(),
            Some(SIGNALS_DEFAULT_V6),
            "dual stack"
        );
        let no_family = refuse_v6(None, Some(EAFNOSUPPORT));
        assert_eq!(bind_with_fallback(None, no_family).await.ok(), Some(SIGNALS_DEFAULT_V4));
        let no_addr = refuse_v6(Some(std::io::ErrorKind::AddrNotAvailable), None);
        assert_eq!(bind_with_fallback(None, no_addr).await.ok(), Some(SIGNALS_DEFAULT_V4));
        let in_use = refuse_v6(Some(std::io::ErrorKind::AddrInUse), None);
        assert!(
            bind_with_fallback(None, in_use).await.is_err(),
            "only a missing family falls back"
        );
        let explicit = SocketAddr::from(([10, 0, 0, 1], 9443));
        let still_no_addr = refuse_v6(Some(std::io::ErrorKind::AddrNotAvailable), None);
        assert_eq!(
            bind_with_fallback(Some(explicit), still_no_addr).await.ok(),
            Some(explicit),
            "explicit as given"
        );
        let listener = bind_signals(Some(SocketAddr::from(([127, 0, 0, 1], 0))))
            .await
            .expect("bind");
        assert!(listener.local_addr().expect("addr").is_ipv4());
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "membership and identities for both trust modes")]
    fn peers_are_polled_only_when_this_site_would_name_them() {
        let member = |site: &str, endpoint: &str| operator::swim::MemberRecord {
            site_id: site.to_owned(),
            endpoint: endpoint.to_owned(),
            incarnation: 0,
            status: operator::swim::MemberStatus::Alive,
            age_secs: 0,
            gateway_address: None,
            site_cert_pem: None,
            signals_address: None,
        };
        let snapshot = operator::swim::MembershipSnapshot {
            members: vec![
                member("hub", "10.0.0.1:7946"),
                member("east", "10.0.0.2:7946"),
                member("west", "10.0.0.3:7946"),
                member("stranger", "10.0.0.4:7946"),
            ],
        };
        let identities = operator::signals::PeerIdentities::new();
        identities.set(BTreeMap::from([
            (
                "east".to_owned(),
                operator::signals::PeerRecord {
                    labels: gpu(),
                    pins: vec!["aa".to_owned()],
                },
            ),
            (
                "west".to_owned(),
                operator::signals::PeerRecord {
                    labels: gpu(),
                    pins: Vec::new(),
                },
            ),
        ]));
        let names = |trust, encrypted| {
            poll_targets(&snapshot, &identities, trust, encrypted, "hub", 9091)
                .map(|sites| sites.into_iter().map(|site| site.name).collect::<Vec<_>>())
        };
        let (pin, spiffe) = (
            operator::signals::PeerTrustMode::Pin,
            operator::signals::PeerTrustMode::Spiffe,
        );
        assert_eq!(
            names(pin, false),
            Some(vec!["east".to_owned()]),
            "pins guard plaintext gossip"
        );
        assert_eq!(names(spiffe, false), None, "SPIFFE waits for encrypted gossip");
        assert_eq!(
            names(spiffe, true),
            Some(vec!["east".to_owned(), "west".to_owned()]),
            "named sites only"
        );
    }

    #[test]
    fn lease_from_seeds_reserves_full_disjoint_block() {
        let first = lease_from_seeds(100, 7).unwrap_or_else(|_| std::process::abort());
        let second = lease_from_seeds(
            first
                .last_revision
                .checked_add(1)
                .unwrap_or_else(|| std::process::abort()),
            first
                .last_node_generation
                .checked_add(1)
                .unwrap_or_else(|| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(first.first_revision, 100);
        assert_eq!(first.last_revision - first.first_revision + 1, REVISION_LEASE_SIZE);
        assert!(second.first_revision > first.last_revision);
        assert!(second.first_node_generation > first.last_node_generation);
    }

    #[test]
    fn persisted_values_win_when_clock_is_behind() {
        let now_ms = 1_800_000_000_000_u64;
        let now_nanos = now_ms * 1_000_000;
        let ahead = now_ms + 60_000;
        let lease = plan_next_lease(ahead, now_nanos + 5, now_ms, now_nanos).unwrap_or_else(|_| std::process::abort());
        assert_eq!(lease.first_revision, ahead + 1, "a mark within the cap is respected");
        assert_eq!(lease.first_node_generation, now_nanos + 6);
    }

    #[test]
    fn back_to_back_restarts_stay_under_every_peer_cap() {
        let now_ms = 1_800_000_000_000_u64;
        let now_nanos = now_ms * 1_000_000;
        let mut lease = lease_from_seeds(now_ms, now_nanos).unwrap_or_else(|_| std::process::abort());
        for restart in 0..2_000 {
            let next = plan_next_lease(lease.last_revision, lease.last_node_generation, now_ms, now_nanos)
                .unwrap_or_else(|_| std::process::abort());
            assert!(
                next.first_revision > lease.last_revision,
                "restart {restart} reuses no revision"
            );
            assert!(
                next.last_revision <= swim::state_broadcast::max_leased_revision(now_ms),
                "restart {restart} stays under the cap"
            );
            assert!(
                !swim::identity::is_future_generation(next.last_node_generation, now_nanos),
                "restart {restart} keeps its generation in the skew"
            );
            lease = next;
        }
    }

    #[test]
    fn a_mark_from_unbounded_leases_reseeds_at_the_clock() {
        let now_ms = 1_800_000_000_000_u64;
        for far in [now_ms + (1 << 32) * 5, u64::MAX] {
            let lease = plan_next_lease(far, 1, now_ms, 7).unwrap_or_else(|_| std::process::abort());
            assert_eq!(lease.first_revision, now_ms, "{far} reseeds");
        }
        assert_eq!(
            next_revision_start(now_ms - 5, now_ms),
            now_ms,
            "never behind the clock"
        );
    }

    #[test]
    fn exhausted_generation_fails_closed() {
        assert!(
            next_revision_lease(1, u64::MAX).is_err(),
            "u64::MAX generation must overflow"
        );
        assert!(lease_from_seeds(u64::MAX, 1).is_err(), "u64::MAX seed must overflow");
    }

    #[test]
    fn reservation_data_round_trips() {
        let lease = RevisionLease {
            first_revision: 10,
            last_revision: 20,
            first_node_generation: 30,
            last_node_generation: 40,
        };
        let data = revision_lease_data(&lease);
        assert_eq!(parse_revision_value(&data, REVISION_HIGH_KEY), Some(20));
        assert_eq!(parse_revision_value(&data, NODE_GENERATION_HIGH_KEY), Some(40));
    }
}
