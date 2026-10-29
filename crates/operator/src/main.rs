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
//! `GridNetwork.status.connectedSites` and `remoteProviderCount` remain
//! 0, and the phase stays `Pending`/`Initializing` based on TLS configuration
//! only.
//! `GRID_SWIM_SERVICE_NAME` advertises that Service's `LoadBalancer` address instead, and
//! `GRID_SIGNALS_SERVICE_NAME` names the Service carrying the signals port.
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
//! `GridNetwork.spec.tls.swimKeySecretRef` to source the key from a Kubernetes
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
    collections::{BTreeMap, BTreeSet},
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
    api::{ObjectMeta, Patch, PatchParams, PostParams},
    runtime::{controller::Controller, watcher},
};
use operator::{
    cli::{Cli, Membership},
    controller::{
        agent_tool_provider,
        grid_network::{self, GridModes, OperatorCtx},
        grid_site, inference_provider,
    },
    crd::{
        agent_tool_provider::AgentToolProvider,
        grid_network::{GridNetwork, SignalMode},
        grid_site::GridSite,
        inference_provider::InferenceProvider,
    },
    error::OperatorError,
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

    // Install the process-wide crypto provider the TLS stack requires, once,
    // up front, before any reconciler builds a client.
    operator::init_process_crypto();

    let mut config = Cli::parse();
    version::get().log_startup("grid-operator");
    // A Local install names its site without the SWIM prefix; either name serves.
    if let Some(site_name) = config.grid.site_name.take() {
        config.swim.site_name.get_or_insert(site_name);
    }
    config.grid.signal_transport = config.grid.startup_signal_transport();

    let client = match Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to create kube client");
            return;
        },
    };

    // Probes answer during enrollment and the LoadBalancer wait, except under site identity TLS, which waits for
    // enrollment.
    let ready = Arc::new(Readiness::default());
    let metrics_tls = match operator::metrics_tls::MetricsTls::from_args(
        config.metrics.cert.clone(),
        config.metrics.key.clone(),
        config.metrics.site_identity_secret.clone(),
        &client,
    ) {
        Ok(tls) => tls,
        Err(error) => {
            tracing::error!(%error, "invalid metrics TLS settings");
            std::process::exit(1);
        },
    };
    let metrics_server = tokio::spawn(run_metrics_server(Arc::clone(&ready), metrics_tls));

    // Watched before enrolling, because enrolling retries a retryable failure
    // for as long as it keeps failing and must still answer a SIGTERM.
    let (trigger, shutdown) = operator::shutdown::Trigger::new();
    let trigger = Arc::new(trigger);
    let routes = match holder_routes(&client, &config) {
        Ok(routes) => routes,
        Err(error) => {
            tracing::error!(%error, "invalid leader election settings");
            std::process::exit(1);
        },
    };
    tokio::spawn(watch_for_termination(StandDown {
        trigger: Arc::clone(&trigger),
        ready: Arc::clone(&ready),
        routes: routes.clone(),
    }));

    // Everything below writes, so only the lease holder runs it.
    let stand_down_on_loss = StandDown {
        trigger: Arc::clone(&trigger),
        ready: Arc::clone(&ready),
        routes: routes.clone(),
    };
    let leadership = match elect(&client, &config.leader, &shutdown, stand_down_on_loss).await {
        Ok(Some(leadership)) => leadership,
        Ok(None) => {
            tracing::info!("stood down before taking the operator lease");
            return;
        },
        Err(error) => {
            tracing::error!(%error, "invalid leader election settings");
            std::process::exit(1);
        },
    };

    // The identity names the site, and without one the site idles.
    let invalid_identity = |error: &str| -> SiteIdentity {
        tracing::error!(error, "invalid site identity or name");
        std::process::exit(1)
    };
    let mut found = site_from_identity(&client, &config)
        .await
        .unwrap_or_else(|error| invalid_identity(&error));
    // The operator config names the site and its signer; an edit applies without a restart.
    let signer_hosts = operator::operator_config::SignerHosts::from_list(&config.signer_hosts);
    if signer_hosts.is_any() {
        tracing::warn!(
            "GRID_SIGNER_HOSTS is unset: the operator config may name any signer host; set it to the signers \
             this install trusts"
        );
    }
    let mut operator_config = operator::operator_config::ConfigWatch::spawn(
        client.clone(),
        client.default_namespace(),
        &config.operator_config,
        signer_hosts,
    );
    // The site enrolls now, else while it waits for an identity.
    let mut enrollment = operator::enroll::local::Enrollment::default();
    let identified_as = found.name.as_deref().filter(|_name| found.present);
    let stepped = tokio::select! {
        biased;
        () = shutdown.triggered() => {
            tracing::info!("stood down before enrolling");
            return;
        },
        stepped = async {
            let read = operator_config.read().await;
            Box::pin(enrollment.step(&client, identified_as, &read)).await
        } => stepped,
    };
    match stepped {
        // The identity just written names the site, so this process serves under it.
        Ok(operator::enroll::local::Step::Enrolled) => {
            found = site_from_identity(&client, &config)
                .await
                .unwrap_or_else(|error| invalid_identity(&error));
        },
        Ok(_) => {},
        Err(error) => {
            tracing::warn!(%error, "listing GridSites to enroll failed, retrying while waiting for an identity");
        },
    }
    let SiteIdentity {
        at: identity,
        present: has_identity,
        name: site_name,
    } = found;
    config.swim.site_name = site_name;
    let startup = SiteStartup::resolve(config.swim.site_name.is_some(), has_identity);
    match startup {
        SiteStartup::Idle => {
            tracing::info!("no site identity or site name: idle until an admin creates or joins a grid");
        },
        SiteStartup::AwaitingIdentity => {
            tracing::info!("site named but not yet identified: waiting for its identity");
        },
        SiteStartup::Identified => {},
    }
    if startup.creates_records() && config.grid.create_local_records {
        let result = Box::pin(create_local_records(&client, &config, identity)).await;
        if let Err(error) = result {
            tracing::error!(%error, "local grid records not created");
            std::process::exit(1);
        }
    }
    let install_modes = GridModes::without_network(config.grid.signal_transport, config.grid.peer_trust)
        .with_membership(config.grid.membership);
    let (modes, membership_declared) = match resolve_grid_modes(&client, has_identity, install_modes).await {
        Ok(modes) => modes,
        Err(error) => {
            tracing::error!(%error, "failed to resolve grid modes");
            std::process::exit(1);
        },
    };
    config.grid.membership = modes.membership;
    let signal_mode = modes.signal;
    let trust = modes.trust;
    // The loop follows the declared peer trust, which may change after startup.
    let (declared_trust, trust_changes) = tokio::sync::watch::channel(trust);
    // Renews through the signer the operator config names; an unidentified site restarts once enrolled.
    // Released once the renewal loop has returned, after any write it had under way.
    let (renewal_done_tx, renewal_done) = operator::shutdown::Trigger::new();
    let mut renewal_done_tx = Some(renewal_done_tx);
    if has_identity {
        let settings = operator::enroll::renew::Settings::new()
            .with_config(operator_config.clone())
            .with_site(config.swim.site_name.clone())
            .with_gateway(&config.gateway.namespace, &config.gateway.service_name)
            .with_declared_trust(install_modes, trust_changes);
        let (renewing, stop, done) = (client.clone(), shutdown.clone(), renewal_done_tx.take());
        tokio::spawn(async move {
            operator::enroll::renew::run(renewing, settings, stop).await;
            drop(done);
        });
        // Enrolling runs only while unidentified: a site that loses its identity, with an
        // invite to enroll again, stands down so its restart enrolls, rather than run on unidentified.
        if let Some(site) = config.swim.site_name.clone() {
            let stand_down = StandDown {
                trigger: Arc::clone(&trigger),
                ready: Arc::clone(&ready),
                routes: routes.clone(),
            };
            let (watching, invites) = (client.clone(), operator_config.clone());
            tokio::spawn(async move {
                operator::enroll::local::lost_identity_with_invite(&watching, &site, invites).await;
                stand_down.begin().await;
            });
        }
    }
    drop(renewal_done_tx);
    let signals_enabled = matches!(signal_mode, SignalMode::Poll);
    let peer_settings = grid_network::PeerSettings {
        local_signals_addr: config.signals.local_addr(),
        trust,
        peer_port: config.signals.peer_port,
        signals_port: config.signals.peer_endpoint_port,
        peer_interval: std::time::Duration::from_secs(config.signals.peer_interval_secs),
        peer_budget: std::time::Duration::from_secs(config.signals.peer_budget_secs),
    };
    let (mode_restart, mode_change) = tokio::sync::watch::channel(None);
    tokio::spawn(restart_for_modes(
        mode_change,
        StandDown {
            trigger: Arc::clone(&trigger),
            ready: Arc::clone(&ready),
            routes: routes.clone(),
        },
    ));
    let ctx = Arc::new(
        OperatorCtx::new(client.clone(), None, signal_mode)
            .with_peer_settings(peer_settings)
            .with_declared_trust(declared_trust)
            .with_operator_config(operator_config.clone())
            .with_mode_restart(mode_restart)
            .with_site_name(config.swim.site_name.clone())
            .with_membership(config.grid.membership)
            .with_membership_fallback(install_modes.membership)
            .with_scrape_interval(config.signals.scrape_interval())
            .hold_membership(),
    );
    // This site's providers first, then what its peers publish, on /metrics.
    operator::metrics::register_provider_signals(vec![ctx.signals(), ctx.peers()]);

    // Controllers run now. Only SWIM and what dials peers wait on the advertise address.
    let (swim_tx, swim_rx) = tokio::sync::watch::channel(SwimStage::Starting);
    route_to_holder(&routes, signals_enabled.then(|| ctx.signals()), &leadership.holding());

    // Stop every state writer before leaving SWIM. Otherwise the leave event can
    // drive one last reconcile that publishes every peer as dead or tries to
    // reconfigure a runtime that has already stopped.
    let (writers_done_tx, writers_done) = operator::shutdown::Trigger::new();
    let writers_shutdown = shutdown.clone();
    let writer_client = client.clone();
    let writer_ctx = Arc::clone(&ctx);
    let writer_swim = swim_rx.clone();
    let writer_site_name = ctx.site_name().map(str::to_owned);
    // Every mode scrapes for readiness; only Poll mode publishes what it scrapes.
    let writer_scrape_interval = config.signals.scrape_interval();
    let writer_group = async move {
        tokio::try_join!(
            run_network_controller(writer_client.clone(), Arc::clone(&writer_ctx), writer_swim),
            run_site_controller(writer_client.clone(), writer_ctx.membership()),
            run_provider_controller(writer_client.clone(), writer_site_name),
            run_agent_tool_provider_controller(writer_client.clone()),
            run_local_scraper(writer_scrape_interval, Arc::clone(&writer_ctx), writer_client.clone()),
            run_model_discovery(writer_ctx, writer_client),
        )
        .map(|_joined| ())
    };
    let writers = stop_writers_on_shutdown(writers_shutdown, writers_done_tx, writer_group);

    // The lease goes once no writer runs and the site has left, before the drain grace.
    let (left_tx, left) = operator::shutdown::Trigger::new();
    let releasing = tokio::spawn(async move {
        left.triggered().await;
        leadership.finish().await;
    });
    let standing_down = stand_down(
        shutdown.clone(),
        (writers_done, renewal_done.clone()),
        swim_rx.clone(),
        left_tx,
    );
    let serving = async {
        tokio::try_join!(
            start_swim(
                config.clone(),
                Arc::clone(&ctx),
                StandDown {
                    trigger: Arc::clone(&trigger),
                    ready: Arc::clone(&ready),
                    routes: routes.clone(),
                },
                swim_tx,
                membership_declared,
            ),
            writers,
            async { metrics_server.await? },
            run_signals_server(
                signals_enabled.then(|| SignalsListener {
                    bind: config.signals.addr,
                    client: client.clone(),
                    published: Published {
                        site: ctx.signals(),
                        peers: ctx.peers(),
                        rounds: ctx.peer_rounds(),
                        providers: ctx.local_providers(),
                        membership: ctx.membership().as_str(),
                        feedback: operator::gateway_ages::GatewayFeedback::new(client.clone(), shutdown.clone()),
                    },
                    peer_identities: ctx.peer_identities(),
                    trust,
                    max_per_peer: config.signals.max_per_peer,
                    max_per_source: config.signals.max_per_source,
                }),
                swim_rx.clone(),
                shutdown.clone(),
            ),
            run_peer_poller(
                signals_enabled.then(|| config.signals.clone()),
                Arc::clone(&ctx),
                swim_rx,
                client.clone(),
                shutdown.clone(),
            ),
        )
        .map(|_joined| ())
    };

    // Waiting exits once the identity appears, so the restart names the site from
    // it and creates its records.
    let identified = async {
        if startup.waits_for_identity() {
            identity_appears(&client, &config, &mut enrollment, &mut operator_config).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    // Standing down wins, and dropping the work closes every connection pool
    // it owns. The listener drains while this waits, because both sides stay
    // polled until one of them finishes.
    let result = tokio::select! {
        biased;
        () = standing_down => Ok(()),
        () = identified => {
            // Exit 0 so the kubelet restarts the pod; its restart backoff still applies.
            tracing::info!("restarting under the site identity's name");
            trigger.trigger();
            wait_for_renewal(&renewal_done).await;
            for routed in [&routes.signals, &routes.swim].into_iter().flatten() {
                routed.withdraw().await;
            }
            drop(releasing.await);
            return;
        },
        result = serving => result,
    };

    drop(releasing.await);
    if let Err(e) = result {
        tracing::error!(error = %e, "controller error");
        std::process::exit(1);
    }
    // The one line that distinguishes a clean exit from a SIGKILL in pod logs.
    tracing::info!("stood down");
}

/// Take the operator lease named in `args`, standing down if it is later lost.
///
/// `Ok(None)` when shutdown comes first. With no lease configured, this replica writes unelected.
async fn elect(
    client: &Client,
    args: &operator::cli::LeaderArgs,
    shutdown: &operator::shutdown::Shutdown,
    on_loss: StandDown,
) -> Result<Option<Leadership>, String> {
    let Some(lease) = args.lease.as_deref() else {
        tracing::info!("no operator lease configured; writing unelected");
        return Ok(Some(Leadership(None)));
    };
    let identity = args
        .identity
        .clone()
        .filter(|identity| !identity.is_empty())
        .ok_or("GRID_LEADER_LEASE is set but GRID_LEADER_IDENTITY is not")?;
    let store = operator::leader::KubeLease::new(client.clone(), client.default_namespace(), lease);
    let elector = operator::leader::Elector::new(store, identity);
    // A candidate is ready: peers reach only the holder, through the slices it writes.
    on_loss.ready.mark_started();
    let lost = move || {
        // Restarting is the only way back to candidacy: SWIM starts once per process.
        tracing::warn!("lost the operator lease; standing down to restart as a candidate");
        tokio::spawn(on_loss.begin_lost());
    };
    let replicas = operator::unelected::Replicas::new(client, args.replica_selector.clone());
    let held = Box::pin(operator::leader::lead(elector, &replicas, shutdown, lost)).await;
    Ok(held.map(|held| Leadership(Some(held))))
}

/// The operator lease while held, or nothing when this replica writes unelected.
struct Leadership(Option<operator::leader::Leadership<operator::leader::KubeLease>>);

impl Leadership {
    /// Whether this replica may still act as the holder; always, unelected.
    fn holding(&self) -> operator::leader::Holding {
        self.0.as_ref().map_or_else(
            operator::leader::Holding::unelected,
            operator::leader::Leadership::holding,
        )
    }

    /// Stop renewing and release the lease, so a successor takes over without waiting it out.
    async fn finish(self) {
        if let Some(held) = self.0 {
            held.finish().await;
        }
    }
}

/// Cancel every state writer on shutdown, then release the leave barrier.
async fn stop_writers_on_shutdown<E>(
    shutdown: operator::shutdown::Shutdown,
    done: operator::shutdown::Trigger,
    writers: impl Future<Output = Result<(), E>>,
) -> Result<(), E> {
    // This drops after the unselected writer future, so observing `done` means
    // no controller or background writer can make another state change.
    let _done_on_drop = done;
    tokio::select! {
        biased;
        () = shutdown.triggered() => Ok(()),
        result = writers => result,
    }
}

/// Resolve once the process should stop, after writers stop and the site leaves.
///
/// Writers stop before leave so the membership change cannot drive a final
/// degraded overlay or touch the stopped SWIM runtime. The grace that follows
/// is what the inbound listener drains in.
async fn stand_down(
    shutdown: operator::shutdown::Shutdown,
    (writers_done, renewal_done): (operator::shutdown::Shutdown, operator::shutdown::Shutdown),
    swim: SwimStartup,
    left_site: operator::shutdown::Trigger,
) {
    shutdown.triggered().await;
    writers_done.triggered().await;
    wait_for_renewal(&renewal_done).await;
    let left = match settled(&swim) {
        Some(handle) => {
            handle.leave(LEAVE_WAIT).await;
            true
        },
        None => false,
    };
    left_site.trigger();
    tokio::time::sleep(SHUTDOWN_GRACE).await;
    // The work is still polled through the grace, so SWIM can finish starting
    // and advertise this site after the read above. Leaving a runtime that
    // appeared in that window beats making peers wait for suspicion.
    if let Some(handle) = settled(&swim).filter(|_| !left) {
        handle.leave(LEAVE_WAIT).await;
    }
}

/// Wait, up to [`RENEWAL_DRAIN`], for the identity renewal loop to stand down. It gives up a
/// request to the signer at once and finishes only an identity write or gateway roll under way,
/// so neither starts once the lease is released.
async fn wait_for_renewal(renewal_done: &operator::shutdown::Shutdown) {
    if tokio::time::timeout(RENEWAL_DRAIN, renewal_done.triggered())
        .await
        .is_err()
    {
        tracing::warn!("identity renewal still under way at the stand-down bound; releasing anyway");
    }
}

/// How long a stand-down waits for an identity write or gateway roll under way.
const RENEWAL_DRAIN: std::time::Duration = std::time::Duration::from_secs(5);

/// The SWIM handle if one exists, read rather than awaited.
///
/// Awaiting would hold a stand-down for as long as a `LoadBalancer` address
/// takes, which is unbounded.
fn settled(swim: &SwimStartup) -> Option<Arc<swim_runtime::SwimHandle>> {
    match &*swim.borrow() {
        SwimStage::Settled(handle) => handle.clone(),
        SwimStage::Starting => None,
    }
}

/// How long in-flight work has after the cluster has been left.
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

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
    cli: Cli,
    ctx: Arc<OperatorCtx>,
    restart: StandDown,
    started: tokio::sync::watch::Sender<SwimStage>,
    membership_declared: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = ctx.client.clone();
    let retry = restart.clone();
    let ready = Arc::clone(&restart.ready);
    let started_swim = Box::pin(maybe_start_swim(
        &client,
        &cli,
        ctx.signal_mode(),
        restart,
        membership_declared,
    ))
    .await;
    let swim = report_swim(&client, cli.grid.membership, started_swim, retry)?;
    if let Some(handle) = &swim {
        ctx.set_swim(Arc::clone(handle));
        tokio::spawn(gateway::run_discovery_poller(
            client.clone(),
            Arc::clone(handle),
            cli.gateway.clone(),
        ));
        tokio::spawn(release_membership_once_settled(Arc::clone(handle), Arc::clone(&ctx)));
    } else {
        settle_without_swim(&client, cli.grid.membership, &ctx).await?;
    }
    started.send_replace(SwimStage::Settled(swim));
    ready.mark_started();
    Ok(())
}

/// Release membership-derived writes once every live named peer has delivered, or after the grace.
async fn release_membership_once_settled(handle: Arc<swim_runtime::SwimHandle>, ctx: Arc<OperatorCtx>) {
    // Only the poll path hears load and gates on peers' state; push releases on a first member.
    let poll = ctx.signal_mode() == SignalMode::Poll;
    let settled = || {
        let members = handle.snapshot();
        if poll {
            peers_delivered(
                &members,
                &handle.state_snapshot(),
                &ctx.peer_identities(),
                &ctx.load_heard(),
                handle.site_name(),
            )
        } else {
            !members.members.is_empty()
        }
    };
    // Load arrives on poll rounds, which raise no SWIM event, so recheck each second too.
    let ticks = futures::stream::unfold((), |()| async {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Some(((), ()))
    });
    let changes = futures::stream::select(handle.reconciliation_events(), ticks);
    converged(settled, changes, MEMBERSHIP_GRACE).await;
    ctx.release_membership();
}

/// With no SWIM, reserve the signal revisions a Local site publishes under, and release
/// membership-derived writes: under Local once peers catch up, otherwise at once.
async fn settle_without_swim(client: &Client, membership: Membership, ctx: &Arc<OperatorCtx>) -> Result<(), String> {
    if membership == Membership::Local
        && let Some(site) = ctx.site_name()
    {
        let lease = reserve_revision_lease(client, site)
            .await
            .map_err(|error| format!("reserving signal revisions: {error}"))?;
        ctx.set_publisher(grid_network::Publisher {
            site: site.to_owned(),
            reservation: (lease.first_node_generation, lease.last_node_generation),
        });
    }
    if membership == Membership::Local {
        tokio::spawn(release_membership_once_caught_up(Arc::clone(ctx)));
        // A site that ran Swim before keeps a Swim condition that no longer says anything.
        tokio::spawn(clear_swim_condition(client.clone()));
    } else {
        ctx.release_membership();
    }
    Ok(())
}

/// Release membership-derived writes once every polled peer's catalog is held, or it has gone
/// [`UNANSWERED_ROUNDS`] rounds unanswered, or after the grace. Without it a restarted Local
/// site's first serving write would carry no remote candidate.
async fn release_membership_once_caught_up(ctx: Arc<OperatorCtx>) {
    let catch_up = ctx.peer_catch_up();
    let ticks = futures::stream::unfold((), |()| async {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Some(((), ()))
    });
    converged(|| catch_up.caught_up(UNANSWERED_ROUNDS), ticks, MEMBERSHIP_GRACE).await;
    ctx.release_membership();
}

/// Rounds in a row a peer may go unanswered before the restart hold stops waiting on it.
const UNANSWERED_ROUNDS: u32 = 3;

/// Longest wait for peers' state before membership-derived writes run without it: one state
/// republish, every 30 s, and the membership convergence before it.
const MEMBERSHIP_GRACE: std::time::Duration = std::time::Duration::from_secs(45);

/// Whether a peer is a member and every live member a `GridSite` names has delivered providers
/// and load, so the first serving write after a restart is not missing peers or their load. A
/// live peer that publishes no load holds the write for the whole grace.
fn peers_delivered(
    members: &operator::swim::MembershipSnapshot,
    state: &crdt::GridStateSnapshot,
    identities: &operator::signals::PeerIdentities,
    heard: &operator::signals::LoadHeard,
    local_site: &str,
) -> bool {
    let mut peers = members.members.iter().filter(|m| m.site_id != local_site).peekable();
    identities.loaded()
        && peers.peek().is_some()
        && peers
            .filter(|m| m.status == operator::swim::MemberStatus::Alive && identities.knows(&m.site_id))
            .all(|m| state.providers.values().any(|p| p.site_id == m.site_id) && heard.any_from(&m.site_id))
}

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

/// Resolve the grid-wide modes from the sole `GridNetwork`, read once at startup, or
/// `without_network` when none exists yet.
async fn resolve_grid_modes(
    client: &Client,
    enrolled: bool,
    without_network: GridModes,
) -> Result<(GridModes, bool), String> {
    let networks: Api<GridNetwork> = Api::all(client.clone());
    let items = networks
        .list(&kube::api::ListParams::default())
        .await
        .map_err(|e| format!("listing GridNetworks: {e}"))?
        .items;
    let (modes, declared) = match items.as_slice() {
        [] => {
            let identity = if enrolled { " (enrolled identity)" } else { "" };
            tracing::info!(modes = ?without_network, "no GridNetwork yet; starting in the install's modes{identity}");
            (without_network, false)
        },
        [network] => {
            let modes = GridModes::of(network, without_network.membership);
            tracing::info!(?modes, "resolved grid-wide modes");
            (modes, network.spec.membership.is_some())
        },
        many => {
            return Err(format!(
                "multiple GridNetworks unsupported: found {}; a process-global signals serve/poll path \
             cannot serve more than one grid mode",
                many.len()
            ));
        },
    };
    if modes.membership == Membership::Local && modes.signal != SignalMode::Poll {
        return Err("Local membership requires signalTransport.mode poll".to_owned());
    }
    Ok((modes, declared))
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
/// `GRID_GATEWAY_ADDRESS` env var wins; otherwise enabled discovery reads the
/// site's gateway Service `LoadBalancer` IP from Kubernetes.
#[expect(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::large_stack_frames,
    reason = "sequential env-var parsing + runtime startup; splitting would obscure the startup sequence"
)]
async fn maybe_start_swim(
    client: &Client,
    cli: &Cli,
    signal_mode: SignalMode,
    restart: StandDown,
    membership_declared: bool,
) -> Result<Option<Arc<swim_runtime::SwimHandle>>, SwimFailure> {
    let bind = std::env::var("GRID_SWIM_BIND_ADDR").ok();
    if !swim_requested(cli.grid.membership, membership_declared, bind.is_some())? {
        return Ok(None);
    }
    let Some(addr_str) = bind else {
        return Ok(None);
    };
    let bind_addr: SocketAddr = match addr_str.parse() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(addr = %addr_str, error = %e, "GRID_SWIM_BIND_ADDR not a valid socket address");
            return Err(SwimFailure::new(
                "BindAddressInvalid",
                format!("GRID_SWIM_BIND_ADDR is not a valid socket address: {e}"),
            ));
        },
    };
    let Some(site_name) = cli.swim.site_name.clone() else {
        return Ok(None);
    };
    let Ok(advertised) =
        tokio::time::timeout(swim_advertise::LB_PATIENCE, swim_advertise_addr(client, bind_addr)).await
    else {
        let client = client.clone();
        tokio::spawn(async move {
            let appeared = async { swim_advertise_addr(&client, bind_addr).await.is_ok() };
            Box::pin(restart_when(appeared, restart)).await;
        });
        return Err(SwimFailure::new(
            "AdvertiseAddressPending",
            format!(
                "no SWIM advertise address within {}s; waiting for one and restarting when it appears",
                swim_advertise::LB_PATIENCE.as_secs()
            ),
        ));
    };
    let (advertise_addr, lb_watch) =
        advertised.map_err(|cause| SwimFailure::new("AdvertiseAddressUnresolved", cause))?;
    if advertise_addr.unwrap_or(bind_addr).ip().is_unspecified() {
        return Err(SwimFailure::new(
            "AdvertiseAddressUnspecified",
            "refusing to advertise an unspecified SWIM address; set GRID_SWIM_ADVERTISE_ADDR",
        ));
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
    let gateway_address = match gateway::resolve(client, &cli.gateway).await {
        Ok(addr) => addr,
        Err(e) => {
            tracing::error!(error = %e, "gateway address discovery failed; continuing without");
            None
        },
    };
    let (key, retry_key) =
        match parse_swim_key_env("GRID_SWIM_ENCRYPT_KEY").map_err(|cause| SwimFailure::new("KeyInvalid", cause))? {
            Some(key) => (swim_runtime::KeyState::Key(Arc::new(key)), false),
            None => startup_key_state(client, cli.swim.require_key).await,
        };
    let revision_lease = reserve_revision_lease(client, &site_name)
        .await
        .map_err(|error| SwimFailure::new("RevisionLeaseFailed", format!("reserving SWIM revisions: {error}")))?;
    let signals_address = match (&cli.signals.advertise_addr, &lb_watch) {
        (Some(explicit), _) => Some(explicit.clone()),
        (None, Some(watch)) => watch.signals.clone(),
        (None, None) => None,
    };
    tracing::debug!(signals = ?signals_address, "signals endpoint to advertise");
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
        departed: if signal_mode == SignalMode::Poll {
            swim_runtime::Departed::RetainProviders
        } else {
            swim_runtime::Departed::Evict
        },
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
        Err(e) => Err(SwimFailure::new(
            "StartFailed",
            format!("SWIM runtime failed to start: {e}"),
        )),
    }
}

/// Whether startup may open SWIM, preserving install validation while honoring a network override.
fn swim_requested(membership: Membership, declared: bool, has_bind: bool) -> Result<bool, SwimFailure> {
    match membership {
        Membership::Local if has_bind && !declared => Err(SwimFailure::new(
            "MembershipConflict",
            "GRID_MEMBERSHIP_SOURCE Local runs no SWIM, but GRID_SWIM_BIND_ADDR is set",
        )),
        Membership::Local => Ok(false),
        Membership::Swim => Ok(has_bind),
    }
}

/// Why SWIM did not start: a reason naming the cause, for the `Swim` condition, and its detail.
#[derive(Clone, Debug)]
struct SwimFailure {
    /// The `Swim` condition's `False` reason.
    reason: &'static str,
    /// What happened, in words.
    cause: String,
}

impl SwimFailure {
    /// Whether another start could succeed with nothing changed, so the site retries it. A
    /// pending advertise address restarts on its own once it appears, and a bad setting waits
    /// for a rollout that fixes it.
    fn retried(&self) -> bool {
        matches!(
            self.reason,
            "AdvertiseAddressUnresolved" | "RevisionLeaseFailed" | "StartFailed"
        )
    }

    /// A failure for `reason`, described by `cause`.
    fn new<Cause: Into<String>>(reason: &'static str, cause: Cause) -> Self {
        Self {
            reason,
            cause: cause.into(),
        }
    }
}

impl std::fmt::Display for SwimFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.cause)
    }
}

/// SWIM's startup outcome, reported: under Swim a failure is logged, counted and written
/// as the `Swim` condition and the site runs on without it; under Local it stops startup.
fn report_swim(
    client: &Client,
    membership: Membership,
    started: Result<Option<Arc<swim_runtime::SwimHandle>>, SwimFailure>,
    retry: StandDown,
) -> Result<Option<Arc<swim_runtime::SwimHandle>>, String> {
    match started {
        // A Local install that sets SWIM contradicts itself, which only a fix to it resolves.
        Err(error) if membership == Membership::Local => Err(format!("SWIM startup failed: {error}")),
        Err(error) => {
            tracing::error!(%error, "SWIM is unavailable; running without membership gossip");
            operator::metrics::set_swim_unavailable(true);
            if error.retried() {
                tokio::spawn(retry_swim_later(retry));
            }
            tokio::spawn(write_swim_condition(client.clone(), Some(error)));
            Ok(None)
        },
        Ok(swim) => {
            if swim.is_some() {
                operator::metrics::set_swim_unavailable(false);
                tokio::spawn(write_swim_condition(client.clone(), None));
            }
            Ok(swim)
        },
    }
}

/// Once `appeared` says the advertise address the startup deadline gave up on exists, stand
/// down so the kubelet restarts the pod and SWIM starts.
async fn restart_when(appeared: impl Future<Output = bool>, stand_down: StandDown) {
    if appeared.await {
        tracing::info!("SWIM advertise address appeared; standing down to restart and start SWIM");
        stand_down.begin().await;
    }
}

/// Once a `GridNetwork` declares other modes, stand down so the restart starts in them.
async fn restart_for_modes(mut owed: tokio::sync::watch::Receiver<Option<GridModes>>, stand_down: StandDown) {
    let owed = owed.wait_for(Option::is_some).await.map(|next| *next);
    if let Ok(next) = owed {
        tracing::info!(?next, "standing down to restart in the declared modes");
        stand_down.begin().await;
    }
}

/// How long a site runs without SWIM after a start that may succeed on another try.
const SWIM_RETRY: std::time::Duration = std::time::Duration::from_secs(300);

/// After [`SWIM_RETRY`], stand down so the restart tries SWIM again.
async fn retry_swim_later(stand_down: StandDown) {
    tokio::time::sleep(SWIM_RETRY).await;
    tracing::info!("standing down to restart and try SWIM again");
    stand_down.begin().await;
}

/// Condition type naming whether SWIM runs on this site.
const SWIM_CONDITION: &str = "Swim";

/// Field manager for the [`SWIM_CONDITION`] condition, apart from every other `GridNetwork` writer.
const SWIM_FIELD_MANAGER: &str = "grid-operator-swim";

/// Write [`SWIM_CONDITION`] on the sole `GridNetwork`, `False` with `unavailable`'s cause,
/// retrying until a network exists and the write lands.
async fn write_swim_condition(client: Client, unavailable: Option<SwimFailure>) {
    loop {
        if let Some(network) = sole_network(&client).await
            && let Some(name) = network.metadata.name.as_deref()
            && let Ok(now) = time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339)
        {
            let condition = swim_condition(unavailable.as_ref(), &now, network.metadata.generation);
            let patch = serde_json::json!({
                "apiVersion": "grid.praxis.fast/v1alpha1",
                "kind": "GridNetwork",
                "metadata": { "name": name },
                "status": { "conditions": [condition] }
            });
            let api: Api<GridNetwork> = Api::all(client.clone());
            let params = PatchParams::apply(SWIM_FIELD_MANAGER).force();
            match Box::pin(api.patch_status(name, &params, &Patch::Apply(patch))).await {
                Ok(_) => return,
                Err(error) => tracing::warn!(%error, "Swim condition not written; retrying"),
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }
}

/// Remove [`SWIM_CONDITION`] from the sole `GridNetwork`, retrying until a network exists and
/// the write lands. Conditions merge by type, so applying none under its field manager removes
/// only the one it wrote.
async fn clear_swim_condition(client: Client) {
    loop {
        if let Some(network) = sole_network(&client).await
            && let Some(name) = network.metadata.name.as_deref()
        {
            let api: Api<GridNetwork> = Api::all(client.clone());
            let params = PatchParams::apply(SWIM_FIELD_MANAGER).force();
            match Box::pin(api.patch_status(name, &params, &Patch::Apply(swim_cleared(name)))).await {
                Ok(_) => return,
                Err(error) => tracing::warn!(%error, "Swim condition not cleared; retrying"),
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }
}

/// The status patch that removes what [`SWIM_FIELD_MANAGER`] wrote on `name`.
fn swim_cleared(name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": name },
        "status": { "conditions": [] }
    })
}

/// The [`SWIM_CONDITION`] condition: `True` while SWIM runs, `False` with the cause when it cannot.
fn swim_condition(
    unavailable: Option<&SwimFailure>,
    now: &str,
    generation: Option<i64>,
) -> operator::crd::inference_provider::Condition {
    let (status, reason, message) = match unavailable {
        Some(failure) => (
            "False",
            failure.reason,
            format!("SWIM is not running: {}", failure.cause),
        ),
        None => ("True", SWIM_CONDITION, "SWIM membership gossip is running".to_owned()),
    };
    operator::crd::inference_provider::Condition {
        type_: SWIM_CONDITION.to_owned(),
        status: status.to_owned(),
        reason: reason.to_owned(),
        message,
        last_transition_time: now.to_owned(),
        observed_generation: generation,
    }
}

/// What the `GridNetwork`s declare about the SWIM key.
#[derive(Clone, Debug)]
enum DeclaredKey {
    /// No `GridNetwork` exists yet.
    NoNetwork,
    /// A `GridNetwork` exists and names no key.
    None,
    /// The first `swimKeySecretRef` found.
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
        Ok(None) => tracing::warn!(secret = %key_ref.name, "swimKeySecretRef holds no valid key yet; holding SWIM"),
        Err(error) => tracing::warn!(secret = %key_ref.name, %error, "swimKeySecretRef not readable yet; holding SWIM"),
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
        .find_map(|n| n.spec.tls.swim_key_secret_ref.clone())
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
            let signals_service = std::env::var("GRID_SIGNALS_SERVICE_NAME").ok();
            let watch = LbWatch::discover(client, service, signals_service, bind_addr.port()).await?;
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
    /// Signals Service name. `None` reads the signals port off the SWIM Service.
    signals_service: Option<String>,
    /// Signals endpoint on that Service's `LoadBalancer` address.
    signals: Option<String>,
    /// Whether `signals` is gossiped, so a change to it restarts.
    track_signals: bool,
}

/// Wait for a leave to reach peers before exiting.
const LEAVE_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

impl LbWatch {
    /// Wait for the Service's `LoadBalancer` address, however long it takes.
    async fn discover(
        client: &Client,
        service: String,
        signals_service: Option<String>,
        port: u16,
    ) -> Result<Self, String> {
        let watch = Self {
            services: Api::default_namespaced(client.clone()),
            service,
            signals_service,
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
        let Some(text) = self
            .load_balancer()
            .await?
            .and_then(|svc| swim_advertise::lb_advertised(&svc, self.port))
        else {
            return Ok(None);
        };
        if Box::pin(self.signals_pending()).await? {
            return Ok(None);
        }
        let signals = self.signals_endpoint().await?;
        let addresses = resolve_text(&text).await.map_err(swim_advertise::LookupError::Retry)?;
        Ok(addresses.first().map(|addr| (text, *addr, signals)))
    }

    /// Whether a signals `LoadBalancer` of its own is still waiting for ingress.
    ///
    /// The two Services are provisioned independently, so SWIM can have an address while
    /// signals does not. Settling then gossips a site with no signals endpoint, and the
    /// watch takes `LB_CHANGE_CONFIRMATIONS` slow polls to notice, during which peers
    /// cannot poll this site. Only a `LoadBalancer` is awaited: nothing else ever reports
    /// ingress, so waiting on one would never return.
    async fn signals_pending(&self) -> Result<bool, swim_advertise::LookupError> {
        let Some(name) = self.signals_service.as_deref() else {
            return Ok(false);
        };
        let Some(svc) = self
            .services
            .get_opt(name)
            .await
            .map_err(|error| swim_advertise::LookupError::Retry(error.to_string()))?
        else {
            return Ok(false);
        };
        Ok(swim_advertise::signals_awaiting_ingress(&svc))
    }

    /// Every ingress endpoint the Service lists now, unresolved, or none once a tracked signals endpoint moved.
    async fn ingress(&self) -> Result<Option<Vec<String>>, swim_advertise::LookupError> {
        let signals = self.signals_endpoint().await?;
        Ok(self.load_balancer().await?.map(|svc| {
            if self.track_signals && signals != self.signals {
                tracing::warn!(gossiped = ?self.signals, current = ?signals, "signals endpoint changed");
                return Vec::new();
            }
            swim_advertise::lb_endpoints(&svc, self.port)
        }))
    }

    /// The signals endpoint, from its own Service or the SWIM one. No ingress yet gives
    /// `None`, not an error, so discovery keeps waiting.
    async fn signals_endpoint(&self) -> Result<Option<String>, swim_advertise::LookupError> {
        let name = self.signals_service.as_deref().unwrap_or(&self.service);
        let svc = self
            .services
            .get_opt(name)
            .await
            .map_err(|error| swim_advertise::LookupError::Retry(error.to_string()))?;
        Ok(svc.as_ref().and_then(swim_advertise::lb_signals_endpoint))
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

/// What startup does about the site's name and its records.
///
/// The identity names the site and the records need it, so a configured name
/// alone is not enough to proceed: a named site still waits for its identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SiteStartup {
    /// Neither a name nor an identity: nothing to do until an admin acts.
    Idle,
    /// Named, but the identity has not appeared yet.
    AwaitingIdentity,
    /// Identified, so the records this release owns can be created.
    Identified,
}

impl SiteStartup {
    /// Decide from whether the site has a name and whether its identity is present.
    const fn resolve(named: bool, has_identity: bool) -> Self {
        match (named, has_identity) {
            // The identity names the site, so being identified settles it either way.
            (_, true) => Self::Identified,
            (true, false) => Self::AwaitingIdentity,
            (false, false) => Self::Idle,
        }
    }

    /// Whether to poll for an identity and exit once one appears.
    const fn waits_for_identity(self) -> bool {
        !matches!(self, Self::Identified)
    }

    /// Whether the local records can be created now.
    const fn creates_records(self) -> bool {
        matches!(self, Self::Identified)
    }
}

/// How often a site without an identity checks for one.
const IDENTITY_POLL: std::time::Duration = std::time::Duration::from_secs(15);

/// Resolve once the site identity appears, enrolling the site meanwhile, at once on an edit to
/// the operator config, and warning once per distinct error.
async fn identity_appears(
    client: &Client,
    config: &Cli,
    enrollment: &mut operator::enroll::local::Enrollment,
    operator_config: &mut operator::operator_config::ConfigWatch,
) {
    let mut last_error = None;
    loop {
        tokio::select! {
            () = tokio::time::sleep(IDENTITY_POLL) => {},
            () = operator_config.changed() => {},
        }
        let read = operator_config.current();
        if let Err(error) = Box::pin(enrollment.step(client, None, &read)).await {
            tracing::debug!(%error, "listing GridSites to enroll failed");
        }
        match site_from_identity(client, config).await {
            Ok(SiteIdentity { present: true, .. }) => return,
            Ok(_) => last_error = None,
            Err(error) if last_error.as_ref() != Some(&error) => {
                tracing::warn!(%error, "site identity present but unusable");
                last_error = Some(error);
            },
            Err(_) => {},
        }
    }
}

/// Where the identity is, whether it exists, and the site name it gives.
struct SiteIdentity {
    /// The identity Secret.
    at: operator::site_name::Identity,
    /// Whether the Secret holds a certificate.
    present: bool,
    /// The site name, from the identity, else configured.
    name: Option<String>,
}

/// Locate the identity and resolve the site name from it, else from configuration.
async fn site_from_identity(client: &Client, config: &Cli) -> Result<SiteIdentity, String> {
    let namespace = config
        .grid
        .identity_namespace
        .clone()
        .unwrap_or_else(|| client.default_namespace().to_owned());
    let at = operator::site_name::locate(client, &namespace, operator::enroll::IDENTITY_SECRET)
        .await
        .map_err(|e| e.to_string())?;
    let pem = operator::site_name::identity_pem(client, &at)
        .await
        .map_err(|e| e.to_string())?;
    let name = operator::site_name::resolve(pem.as_deref(), config.swim.site_name.as_deref())?;
    Ok(SiteIdentity {
        at,
        present: pem.is_some(),
        name,
    })
}

/// Create this site's missing `GridNetwork` and `GridSite`, propagating lookup and creation errors.
async fn create_local_records(
    client: &Client,
    config: &Cli,
    identity: operator::site_name::Identity,
) -> Result<(), OperatorError> {
    let Some(site) = config.swim.site_name.as_deref() else {
        return Ok(());
    };
    let plan = operator::local_records::Plan {
        network: config.grid.network.clone(),
        site: site.to_owned(),
        identity,
        ca_secret: operator::enroll::CA_SECRET.to_owned(),
        swim_key_secret: None,
        modes: GridModes::without_network(config.grid.signal_transport, config.grid.peer_trust)
            .with_membership(config.grid.membership),
    };
    // Without SWIM the network names no SWIM key.
    let swim_key = match config.grid.membership {
        Membership::Swim => config.grid.swim_key_secret.as_str(),
        Membership::Local => "",
    };
    let outcome = operator::local_records::create(client, client.default_namespace(), plan, swim_key).await?;
    tracing::info!(?outcome, "local grid records checked");
    Ok(())
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
/// cluster-wide Secret watch; the operator only reads referenced
/// Secrets by explicit namespace/name during reconciliation.
async fn run_network_controller(
    client: Client,
    ctx: Arc<OperatorCtx>,
    swim: SwimStartup,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<GridNetwork>::all(client.clone());
    let provider_api = Api::<InferenceProvider>::all(client.clone());
    let tool_provider_api = Api::<AgentToolProvider>::all(client.clone());
    let site_api = Api::<GridSite>::all(client.clone());

    let controller = Controller::new(api, watcher::Config::default())
        .watches(
            provider_api,
            watcher::Config::default(),
            grid_network::network_refs_from_inference_provider,
        )
        .watches(
            tool_provider_api,
            watcher::Config::default(),
            grid_network::network_refs_from_agent_tool_provider,
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
    // And whenever a polled peer's catalog changes, which no watched object reflects.
    let catalog_events = ctx.peer_providers().changes();
    controller
        .reconcile_all_on(futures::stream::select(swim_events, catalog_events))
        .run(grid_network::reconcile, grid_network::error_policy, ctx)
        .for_each(|result| async {
            match result {
                Ok((obj, _action)) => tracing::debug!(%obj, "reconciled GridNetwork"),
                Err(e) => log_controller_error("GridNetwork", &e),
            }
        })
        .await;

    Ok(())
}

/// Log a controller error, an expired watch at debug since kube-runtime relists.
fn log_controller_error(kind: &str, error: &kube::runtime::controller::Error<OperatorError, watcher::Error>) {
    if watch_expired(error) {
        tracing::debug!(kind, ?error, "watch expired; relisting");
    } else if reconcile_conflicted(error) {
        tracing::debug!(kind, ?error, "object moved underneath the write; reapplying");
    } else {
        tracing::error!(error = ?error, "{kind} watch error");
    }
}

/// Whether a controller error is the reconciler losing a write race.
///
/// The error policy already requeues these. Both sites have to agree, or a conflict the
/// reconciler treats as routine still reaches the log as a failure.
fn reconcile_conflicted(error: &kube::runtime::controller::Error<OperatorError, watcher::Error>) -> bool {
    matches!(error, kube::runtime::controller::Error::ReconcilerFailed(e, _) if e.is_conflict())
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
async fn run_site_controller(
    client: Client,
    membership: Membership,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<GridSite>::all(client.clone());

    // Unfiltered: a status write from the peer poller reconciles, so Degraded follows it.
    Controller::new(api, watcher::Config::default())
        .with_config(kube::runtime::controller::Config::default().concurrency(16))
        .run(
            grid_site::reconcile,
            grid_site::error_policy,
            Arc::new(grid_site::SiteContext {
                client: Arc::new(client),
                membership,
            }),
        )
        .for_each(|result| async {
            match result {
                Ok((obj, _action)) => tracing::debug!(%obj, "reconciled GridSite"),
                Err(e) => log_controller_error("GridSite", &e),
            }
        })
        .await;

    Ok(())
}

/// Run the [`InferenceProvider`] controller (OP-02).
///
/// Watches `InferenceProvider` resources, and every `GridSite` because providers resolve
/// `status.matchingSites` from them. Metrics TLS rotation is detected by bounded requeue
/// rather than a cluster-wide Secret watch; the operator only reads referenced Secrets by
/// explicit namespace/name during reconciliation.
///
/// [`InferenceProvider`]: operator::crd::inference_provider::InferenceProvider
async fn run_provider_controller(
    client: Client,
    local_site: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<InferenceProvider>::all(client.clone());
    let site_api = Api::<GridSite>::all(client.clone());

    // A site is not addressable as one provider, so a change re-resolves them all.
    let mut inputs = inference_provider::SiteMatchInputs::default();
    let site_events = watcher(site_api, watcher::Config::default()).filter_map(move |event| {
        std::future::ready(event.ok().filter(|event| inputs.affects_providers(event)).map(|_| ()))
    });

    Controller::new(api, watcher::Config::default())
        .reconcile_all_on(site_events)
        .run(
            inference_provider::reconcile,
            inference_provider::error_policy,
            Arc::new(inference_provider::ProviderCtx { client, local_site }),
        )
        .for_each(|result| async {
            match result {
                Ok((obj, _action)) => tracing::debug!(%obj, "reconciled InferenceProvider"),
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
                Ok((obj, _action)) => tracing::debug!(%obj, "reconciled AgentToolProvider"),
                Err(e) => log_controller_error("AgentToolProvider", &e),
            }
        })
        .await;

    Ok(())
}

/// Serve Prometheus metrics and health endpoints.
async fn run_metrics_server(
    ready: Arc<Readiness>,
    tls: Option<operator::metrics_tls::MetricsTls>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = std::env::var("GRID_METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:9090".to_owned());
    let app = axum::Router::new()
        .route("/metrics", axum::routing::get(metrics_handler))
        .route("/healthz", axum::routing::get(health_handler))
        .route(
            "/readyz",
            axum::routing::get(move || std::future::ready(ready.status())),
        );
    if let Some(tls) = tls {
        return operator::metrics_tls::serve_metrics_tls(&addr, app, tls, METRICS_TLS_RELOAD).await;
    }
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let bound_addr = listener.local_addr().map_or_else(|_| addr.clone(), |a| a.to_string());
    tracing::info!(addr = %bound_addr, tls = false, "metrics server started");
    axum::serve(listener, app).await?;
    Ok(())
}

/// How often the metrics listener checks its certificate for rotation.
const METRICS_TLS_RELOAD: std::time::Duration = std::time::Duration::from_secs(10);

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

/// Whether `/readyz` answers ready.
#[derive(Debug, Default)]
struct Readiness {
    /// Enrollment and SWIM startup, including any `LoadBalancer` wait, have finished.
    started: std::sync::atomic::AtomicBool,
    /// A termination signal arrived; never cleared, so a late start cannot undo it.
    stopping: std::sync::atomic::AtomicBool,
}

impl Readiness {
    /// Startup finished.
    fn mark_started(&self) {
        self.started.store(true, std::sync::atomic::Ordering::Release);
    }

    /// Standing down: not ready from now on, whatever startup does next.
    fn mark_stopping(&self) {
        self.stopping.store(true, std::sync::atomic::Ordering::Release);
    }

    /// The `/readyz` answer.
    fn status(&self) -> (http::StatusCode, &'static str) {
        if self.stopping.load(std::sync::atomic::Ordering::Acquire) {
            (http::StatusCode::SERVICE_UNAVAILABLE, "stopping")
        } else if self.started.load(std::sync::atomic::Ordering::Acquire) {
            (http::StatusCode::OK, "ok")
        } else {
            (http::StatusCode::SERVICE_UNAVAILABLE, "starting")
        }
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
async fn watch_for_termination(stand_down: StandDown) {
    let signal = first_termination_signal().await;
    tracing::info!(signal, "standing down");
    stand_down.begin().await;
}

/// What standing down touches, in the order it touches them.
#[derive(Clone)]
struct StandDown {
    /// Asks every worker to stop.
    trigger: Arc<operator::shutdown::Trigger>,
    /// Answers not ready.
    ready: Arc<Readiness>,
    /// Routes peers to this replica while it holds the lease.
    routes: HolderRouting,
}

impl StandDown {
    /// Route peers away first, then stop as [`begin_stand_down`] does.
    async fn begin(self) {
        for routes in [&self.routes.signals, &self.routes.swim].into_iter().flatten() {
            routes.withdraw().await;
        }
        begin_stand_down(&self.trigger, &self.ready);
    }

    /// Stop every writer at once, then route peers away: having lost the lease, a successor
    /// may take over before slow slice writes finish.
    async fn begin_lost(self) {
        begin_stand_down(&self.trigger, &self.ready);
        for routes in [&self.routes.signals, &self.routes.swim].into_iter().flatten() {
            routes.withdraw().await;
        }
    }
}

/// The Services peers reach the holder on, each routed to it alone.
#[derive(Clone, Default)]
struct HolderRouting {
    /// Routed once the holder has signals to answer with.
    signals: Option<Arc<operator::holder_route::HolderRoutes>>,
    /// Routed at once: SWIM settles only once a `LoadBalancer` has an address, which some
    /// publish only for a Service with an endpoint.
    swim: Option<Arc<operator::holder_route::HolderRoutes>>,
}

/// The routes for the Services peers reach the holder on, none when unelected or none are named.
fn holder_routes(client: &Client, config: &Cli) -> Result<HolderRouting, String> {
    let args = &config.leader;
    if args.lease.is_none() || (args.signals_services.is_empty() && args.swim_services.is_empty()) {
        return Ok(HolderRouting::default());
    }
    let address = args.pod_ip.ok_or("holder Services are named but POD_IP is not set")?;
    let pod = args
        .identity
        .clone()
        .ok_or("holder Services are named but GRID_LEADER_IDENTITY is not set")?;
    let me = operator::holder_route::Me {
        address,
        pod,
        node: args.node_name.clone(),
    };
    let ports = holder_ports(config);
    let routes = |names: &[String]| {
        (!names.is_empty()).then(|| {
            Arc::new(operator::holder_route::HolderRoutes::new(
                client,
                names.to_vec(),
                me.clone(),
                ports.clone(),
            ))
        })
    };
    Ok(HolderRouting {
        signals: routes(&args.signals_services),
        swim: routes(&args.swim_services),
    })
}

/// Named target ports, resolved to the listeners this process binds.
fn holder_ports(config: &Cli) -> BTreeMap<String, i32> {
    let swim = std::env::var("GRID_SWIM_BIND_ADDR")
        .ok()
        .and_then(|bind| bind.parse::<SocketAddr>().ok())
        .map(|bind| ("swim-udp".to_owned(), i32::from(bind.port())));
    let signals = config.signals.addr.map_or(9091, |addr| addr.port());
    std::iter::once(("signals".to_owned(), i32::from(signals)))
        .chain(swim)
        .collect()
}

/// Route the holder's Services to it: SWIM's at once, signals' once it has published or
/// [`ROUTE_FALLBACK`] has passed, so a site with nothing to publish is still reachable. With
/// no local signals collected, as under gossip, there is no publish to wait for.
fn route_to_holder(
    routes: &HolderRouting,
    signals: Option<operator::signals::SignalStore>,
    holding: &operator::leader::Holding,
) {
    if let Some(swim) = routes.swim.clone() {
        let holding = holding.clone();
        tokio::spawn(async move { swim.route_when(std::future::ready("holding the lease"), holding).await });
    }
    if let Some(routed) = routes.signals.clone() {
        let holding = holding.clone();
        tokio::spawn(async move {
            let published = async {
                let Some(signals) = signals else {
                    return "no local signals to wait for";
                };
                tokio::select! {
                    () = signals.published() => "signals published",
                    () = tokio::time::sleep(ROUTE_FALLBACK) => {
                        tracing::warn!("no signals published in time; routing peers to this holder anyway");
                        "no signals published in time"
                    },
                }
            };
            routed.route_when(published, holding).await;
        });
    }
}

/// How long a holder waits for its first signals before peers are routed to it anyway.
const ROUTE_FALLBACK: std::time::Duration = std::time::Duration::from_secs(60);

/// Answer not ready first, so nothing new is routed here, then ask every worker to stop.
fn begin_stand_down(trigger: &operator::shutdown::Trigger, ready: &Readiness) {
    ready.mark_stopping();
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
    /// Independently persisted gateway diagnostic evidence.
    feedback: operator::gateway_ages::GatewayFeedback,
    /// This site's own signals.
    site: operator::signals::SignalStore,
    /// What peers reported about themselves.
    peers: operator::signals::SignalStore,
    /// When the poller last tried each peer, for this site's own gateway only.
    rounds: operator::signals::PeerRounds,
    /// This site's providers, for peers building candidates.
    providers: operator::site_providers::LocalProviders,
    /// This site's membership mode, named on every answer.
    membership: &'static str,
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
    /// Concurrent handshakes one source address may hold.
    max_per_source: usize,
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
    shutdown: operator::shutdown::Shutdown,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(listener) = listener else {
        return Ok(());
    };
    // Peers learn this listener from the advertised address, so it waits for SWIM to settle.
    let advertised = swim_settled(swim)
        .await
        .and_then(|handle| handle.signals_address().map(str::to_owned));
    let app = signals_app(listener.published.clone());

    // The opt-in listener rebinds on every material change, so a transient
    // bind blip (a socket in TIME_WAIT across a rebind) must not be fatal to
    // reconcile and the metrics server through `try_join!`: a serve error logs
    // and retries rather than propagating.
    while !shutdown.is_triggered() {
        match listener.serve_once(&app, advertised.as_deref(), shutdown.clone()).await {
            Ok(()) if !shutdown.is_triggered() => {
                tracing::info!("signals TLS material changed; serving again");
            },
            Ok(()) => {},
            Err(error) => {
                tracing::error!(%error, bind = ?listener.bind, "signals listener error; retrying");
                tokio::time::sleep(SIGNALS_TLS_POLL).await;
            },
        }
    }
    Ok(())
}

impl SignalsListener {
    /// Serve until this site's TLS material changes, or wait while it is unavailable.
    async fn serve_once(
        &self,
        app: &axum::Router,
        advertised: Option<&str>,
        shutdown: operator::shutdown::Shutdown,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
        tracing::info!(addr = %bound, advertised = ?advertised, tls = true, "signals server serving; peers poll the advertised endpoint");
        let admission = Admission::new(self.max_per_peer, self.max_per_source, self.peer_identities.clone());
        let changed = material_changed(self.client.clone(), own);
        let serving = Serving {
            tls,
            app: app.clone(),
            identity,
        };
        serve_signals_tls(listener, serving, admission, changed, shutdown).await
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
    mut admission: Admission,
    changed: impl Future<Output = ()> + Send,
    shutdown: operator::shutdown::Shutdown,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut changed = std::pin::pin!(changed);
    loop {
        let accepted = tokio::select! {
            biased;
            () = shutdown.triggered() => {
                // Close the socket, so a new connection is refused at once rather than left in
                // the backlog until its client times out, then let open connections finish.
                drop(listener);
                drain_connections(&admission.slots, admission.capacity).await;
                return Ok(());
            },
            () = &mut changed => return Ok(()),
            accepted = listener.accept() => accepted,
        };
        let Some((stream, remote)) = accepted_or_backoff(accepted).await else {
            continue;
        };
        let Some((slot, turn, source, silent)) = admission.admit(remote) else {
            continue;
        };
        let serve = Box::pin(serve_signals_connection(
            serving.clone(),
            stream,
            remote,
            Handshaking {
                turn,
                source,
                silent,
                peers: admission.peers.clone(),
                warned: Arc::clone(&admission.warned),
            },
            shutdown.clone(),
        ));
        tokio::spawn(async move {
            serve.await;
            drop(slot);
        });
    }
}

/// Wait for every open signals connection to finish, up to [`SHUTDOWN_GRACE`].
///
/// Holding every permit means nothing is being served, which is the only way
/// to know no peer is mid-answer. A peer still answering at the end of the
/// grace loses its connection when the process goes.
async fn drain_connections(slots: &Arc<tokio::sync::Semaphore>, capacity: usize) {
    let all = u32::try_from(capacity).unwrap_or(u32::MAX);
    match tokio::time::timeout(SHUTDOWN_GRACE, slots.acquire_many(all)).await {
        Ok(Ok(permits)) => {
            drop(permits);
            tracing::debug!("signals: every connection closed");
        },
        Ok(Err(_closed)) => {},
        Err(_elapsed) => tracing::warn!("signals: connections still open at the end of the grace"),
    }
}

/// Concurrent signals connections at least, however few members the grid declares.
const SIGNALS_MAX_CONNECTIONS: usize = 256;

/// Concurrent signals connections at most, however many members the grid declares.
const SIGNALS_CONNECTIONS_CEILING: usize = 4096;

/// Handshakes in progress across every source, so handshakes cannot hold the slots that
/// authenticated connections need.
const SIGNALS_MAX_HANDSHAKES: usize = 64;

/// Connections peers can never hold, kept for the co-located gateway.
const SIGNALS_LOCAL_RESERVE: usize = 8;

/// Concurrent handshakes from one global unicast IPv6 /48, across its /64s, at least.
const SIGNALS_MAX_PER_SOURCE_48: usize = 32;

/// Pause after a failed accept.
const SIGNALS_ACCEPT_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

/// Least time between warnings about shed connections.
const SIGNALS_SHED_WARN_EVERY: std::time::Duration = std::time::Duration::from_secs(60);

/// Bound on the TLS handshake.
const SIGNALS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// How long an accepted connection may send nothing before it is dropped. A connection that
/// has not begun its handshake costs a slot and proves nothing, and a source that opens many
/// of them is the cheapest way to hold the share.
const SIGNALS_SILENT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

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
    /// The permits [`Self::slots`] was given.
    capacity: usize,
    /// Handshakes in progress, across every source.
    handshakes: Arc<tokio::sync::Semaphore>,
    /// Handshakes in progress, per source.
    sources: SourceSlots,
    /// Accepted connections that have sent nothing yet, per source. Separate from `sources`
    /// and much tighter: behind a router every peer is one source, so the handshake cap has
    /// to cover the whole peer set, while a source that sends nothing earns a quarter of it.
    silent: SourceSlots,
    /// Authenticated peer connections, per site.
    peers: PeerSlots,
    /// Connections one peer may hold.
    per_peer: usize,
    /// The declared members the caps are sized for.
    members: operator::signals::PeerIdentities,
    /// When a shed connection was last warned about, shared with every connection task.
    warned: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}

impl Admission {
    /// Caps for one listener: `max_per_peer` connections per site, `max_per_source`
    /// handshakes per source address, and room for every one of `members`.
    fn new(max_per_peer: usize, max_per_source: usize, members: operator::signals::PeerIdentities) -> Self {
        let share = SIGNALS_MAX_HANDSHAKES / 2;
        let mut admission = Self {
            slots: Arc::new(tokio::sync::Semaphore::new(SIGNALS_MAX_CONNECTIONS)),
            capacity: SIGNALS_MAX_CONNECTIONS,
            handshakes: Arc::new(tokio::sync::Semaphore::new(SIGNALS_MAX_HANDSHAKES)),
            sources: SourceSlots::new(
                max_per_source.min(share),
                max_per_source.max(SIGNALS_MAX_PER_SOURCE_48).min(share),
            ),
            silent: SourceSlots::new((share / 4).max(1), (share / 4).max(1)),
            peers: PeerSlots::new(
                max_per_peer,
                SIGNALS_MAX_CONNECTIONS.saturating_sub(SIGNALS_LOCAL_RESERVE + SIGNALS_MAX_HANDSHAKES),
            ),
            per_peer: max_per_peer.max(1),
            members,
            warned: Arc::default(),
        };
        admission.size_for_members();
        operator::metrics::set_signals_connection_cap(admission.capacity);
        operator::metrics::set_signals_admission(operator::metrics::AdmissionHandles {
            slots: Arc::clone(&admission.slots),
            handshakes: Arc::clone(&admission.handshakes),
            max_handshakes: SIGNALS_MAX_HANDSHAKES,
            sources: Arc::clone(&admission.sources.pending),
            source_cap: admission.sources.limit,
        });
        admission
    }

    /// Grow the caps to fit every declared member, never shrinking them under a held slot.
    fn size_for_members(&mut self) {
        let wanted = self
            .members
            .count()
            .saturating_mul(self.per_peer)
            .saturating_add(SIGNALS_LOCAL_RESERVE + SIGNALS_MAX_HANDSHAKES)
            .clamp(SIGNALS_MAX_CONNECTIONS, SIGNALS_CONNECTIONS_CEILING);
        let grow = wanted.saturating_sub(self.capacity);
        if grow > 0 {
            self.slots.add_permits(grow);
            self.peers.total.add_permits(grow);
            self.capacity = wanted;
            operator::metrics::set_signals_connection_cap(wanted);
            tracing::info!(cap = wanted, "signals connection cap grown for the declared members");
        }
    }

    /// A connection slot, a handshake turn and a source slot for `remote`, or `None` to shed it.
    fn admit(
        &mut self,
        remote: SocketAddr,
    ) -> Option<(
        tokio::sync::OwnedSemaphorePermit,
        tokio::sync::OwnedSemaphorePermit,
        SourceSlot,
        SourceSlot,
    )> {
        self.size_for_members();
        let held = Arc::clone(&self.slots)
            .try_acquire_owned()
            .map_err(|_full| "total")
            .and_then(|slot| {
                let turn = Arc::clone(&self.handshakes)
                    .try_acquire_owned()
                    .map_err(|_full| "handshake")?;
                let source = self.sources.acquire(remote.ip()).ok_or("source")?;
                let silent = self.silent.acquire(remote.ip()).ok_or("silent")?;
                Ok((slot, turn, source, silent))
            });
        held.inspect_err(|limit| shed(remote, limit, &self.warned)).ok()
    }
}

/// Record a shed connection, warning at most once per [`SIGNALS_SHED_WARN_EVERY`].
fn shed(remote: SocketAddr, limit: &str, warned: &std::sync::Mutex<Option<std::time::Instant>>) {
    operator::metrics::record_signals_shed(limit);
    let quiet = {
        let mut warned = warned.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let quiet = warned.is_some_and(|at| at.elapsed() < SIGNALS_SHED_WARN_EVERY);
        if !quiet {
            *warned = Some(std::time::Instant::now());
        }
        quiet
    };
    if quiet {
        tracing::debug!(%remote, limit, "signals connection shed");
    } else {
        tracing::warn!(%remote, limit, "signals connection limit reached; shedding");
    }
}

/// Open handshakes per source, each capped, with a second cap per global IPv6 /48. At a cap the
/// newest handshake is refused, so a flood from one source cannot cut its own handshakes that are
/// already in progress; [`SIGNALS_HANDSHAKE_TIMEOUT`] bounds how long a slow one holds a slot.
#[derive(Clone)]
struct SourceSlots {
    /// Pending handshakes by source key.
    pending: Arc<std::sync::Mutex<std::collections::HashMap<std::net::IpAddr, usize>>>,
    /// Cap per source key.
    limit: usize,
    /// Cap per global unicast IPv6 /48.
    limit_48: usize,
}

impl SourceSlots {
    /// A tracker allowing `limit` handshakes per source and `limit_48` per IPv6 /48.
    fn new(limit: usize, limit_48: usize) -> Self {
        Self {
            pending: Arc::default(),
            limit,
            limit_48,
        }
    }

    /// A slot for `ip`, released on drop, or `None` at either cap.
    fn acquire(&self, ip: std::net::IpAddr) -> Option<SourceSlot> {
        let (key, wider) = source_keys(ip);
        let mut pending = self.pending.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let full = std::iter::once((key, self.limit))
            .chain(wider.map(|wider| (wider, self.limit_48)))
            .any(|(counted, limit)| pending.get(&counted).copied().unwrap_or(0) >= limit);
        if full {
            return None;
        }
        for counted in std::iter::once(key).chain(wider) {
            *pending.entry(counted).or_default() += 1;
        }
        drop(pending);
        Some(SourceSlot {
            slots: self.clone(),
            keys: (key, wider),
        })
    }

    /// Handshakes pending for `ip`'s source key.
    #[cfg(test)]
    fn held(&self, ip: std::net::IpAddr) -> usize {
        let (key, _) = source_keys(ip);
        let pending = self.pending.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        pending.get(&key).copied().unwrap_or(0)
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
        let mut pending = self
            .slots
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for key in std::iter::once(self.keys.0).chain(self.keys.1) {
            if let std::collections::hash_map::Entry::Occupied(mut held) = pending.entry(key) {
                *held.get_mut() = held.get().saturating_sub(1);
                if *held.get() == 0 {
                    held.remove();
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

/// A connection still in its handshake: the slots it holds, and the per-peer caps that follow.
struct Handshaking {
    /// Released once the handshake ends.
    turn: tokio::sync::OwnedSemaphorePermit,
    /// Released once the handshake ends.
    source: SourceSlot,
    /// Released as soon as the connection sends anything, or when it is dropped for sending
    /// nothing. It covers only the window before the first byte.
    silent: SourceSlot,
    /// Where the authenticated connection is counted.
    peers: PeerSlots,
    /// When a shed connection was last warned about, shared across the listener.
    warned: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}

/// Who a leaf speaks for and the site it counts against, `Local` only for this site's current leaf.
fn caller_for(leaf: Option<&[u8]>, identity: &SignalsIdentity, remote: SocketAddr) -> (Caller, Option<String>) {
    let Some(leaf) = leaf else {
        tracing::debug!(%remote, "signals caller presented no certificate");
        return (Caller::Peer(None), None);
    };
    let fingerprint = operator::signals::leaf_fingerprint(leaf);
    if identity.own.as_ref().is_some_and(|own| own.is_own(&fingerprint)) {
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
    // A pin or SPIFFE ID picks the site; the leaf must also name only that site, as the poller
    // requires of the peers it reads.
    if !certs::leaf_names_site(leaf, &site) {
        tracing::debug!(%remote, %site, "signals caller's leaf does not name the site it matched");
        return (Caller::Peer(None), None);
    }
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

/// Whether `tcp` sent anything within [`SIGNALS_SILENT_TIMEOUT`].
///
/// A connection that never speaks costs a slot and proves nothing, and opening many of them
/// is the cheapest way to hold the handshake share. `readable()` does not consume, so the
/// handshake that follows still sees the `ClientHello`. False also covers shutdown, where the
/// caller stops for its own reasons.
async fn spoke_in_time(
    tcp: &tokio::net::TcpStream,
    remote: SocketAddr,
    shutdown: &operator::shutdown::Shutdown,
) -> bool {
    let ready = tokio::select! {
        biased;
        () = shutdown.triggered() => return false,
        ready = tokio::time::timeout(SIGNALS_SILENT_TIMEOUT, tcp.readable()) => ready,
    };
    if matches!(ready, Ok(Ok(()))) {
        return true;
    }
    operator::metrics::record_signals_shed("silent");
    tracing::debug!(%remote, "signals connection sent nothing; dropping");
    false
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
    shutdown: operator::shutdown::Shutdown,
) {
    let handshake = (handshaking.turn, handshaking.source);
    if !spoke_in_time(&tcp, remote, &shutdown).await {
        return;
    }
    // It has spoken, so it no longer counts against the silent cap; the handshake caps take over.
    drop(handshaking.silent);
    let accepted = tokio::select! {
        biased;
        () = shutdown.triggered() => return,
        accepted = tokio::time::timeout(SIGNALS_HANDSHAKE_TIMEOUT, tls_backend::accept(tcp, &serving.tls)) => accepted,
    };
    // The handshake caps cover only the unauthenticated phase.
    drop(handshake);
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
                shed(remote, "peer", &handshaking.warned);
                tracing::debug!(%remote, site, "signals site at its connection cap; answering 503");
                Box::pin(serve_http(
                    stream,
                    unavailable(),
                    (Caller::Peer(None), None),
                    remote,
                    shutdown,
                ))
                .await;
                return;
            };
            Some(slot)
        },
        (_, None) => return,
    };
    let rescope = Some((Arc::<[u8]>::from(leaf), serving.identity));
    Box::pin(serve_http(stream, serving.app, (caller, rescope), remote, shutdown)).await;
    drop(slot);
}

/// Answers every request 503, for a caller shed at its cap.
fn unavailable() -> axum::Router {
    axum::Router::new().fallback(|| std::future::ready(http::StatusCode::SERVICE_UNAVAILABLE))
}

/// A connection's caller, and the leaf and identity to name it again by on each request.
type Scope = (Caller, Option<(Arc<[u8]>, SignalsIdentity)>);

/// Serve HTTP on an authenticated stream with the scope `caller` earned, bounded by every signals timeout.
///
/// With a leaf and identity to rescope by, each request is named again against the current
/// peer records, so a peer removed or re-keyed is refused on its next request, not at reconnect.
async fn serve_http(
    stream: tls_backend::ServerTlsStream,
    app: axum::Router,
    (caller, rescope): Scope,
    remote: SocketAddr,
    shutdown: operator::shutdown::Shutdown,
) {
    // A shed caller gets one answer, then the connection closes.
    let keep_alive = caller != Caller::Peer(None);
    let service = hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
        use tower::Service as _;
        let mut request = request;
        let scoped = rescope.as_ref().map_or_else(
            || caller.clone(),
            |(leaf, identity)| caller_for(Some(leaf), identity, remote).0,
        );
        request.extensions_mut().insert(scoped);
        app.clone().call(request)
    });
    let io = hyper_util::rt::TokioIo::new(WriteDeadline::new(stream, SIGNALS_WRITE_TIMEOUT));
    let connection = hyper::server::conn::http1::Builder::new()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(SIGNALS_HEADER_READ_TIMEOUT)
        .keep_alive(keep_alive)
        .serve_connection(io, service);
    if let Err(error) = Box::pin(serve_until_aged(connection, SIGNALS_MAX_CONNECTION_AGE, shutdown)).await {
        tracing::debug!(%remote, %error, "signals connection ended");
    }
}

/// Serve `connection`, then let it finish its current request and close once it reaches `max_age`.
async fn serve_until_aged<Io, Service>(
    connection: hyper::server::conn::http1::Connection<Io, Service>,
    max_age: std::time::Duration,
    shutdown: operator::shutdown::Shutdown,
) -> hyper::Result<()>
where
    Io: hyper::rt::Read + hyper::rt::Write + Unpin,
    Service: hyper::service::HttpService<hyper::body::Incoming>,
    Service::ResBody: 'static,
    <Service::ResBody as hyper::body::Body>::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let mut connection = std::pin::pin!(connection);
    tokio::select! {
        biased;
        result = connection.as_mut() => return result,
        () = shutdown.triggered() => {},
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

/// Header naming the producer's release on every cross-site answer.
const GRID_VERSION_HEADER: &str = "x-grid-version";

/// Signals and feedback routes sharing the certificate-derived caller scope.
fn signals_app(published: Published) -> axum::Router {
    let membership = http::HeaderValue::from_static(published.membership);
    axum::Router::new()
        .route(operator::signals::SIGNALS_PATH, axum::routing::get(signals_handler))
        .route(
            operator::site_providers::PROVIDERS_PATH,
            axum::routing::get(providers_handler),
        )
        .route(grid_signals::feedback::INGEST_PATH, axum::routing::post(ingest_handler))
        .with_state(published)
        .layer(axum::middleware::map_response(
            move |mut response: axum::response::Response| {
                let headers = response.headers_mut();
                headers.insert(operator::signals::MEMBERSHIP_HEADER, membership.clone());
                headers.insert(GRID_VERSION_HEADER, http::HeaderValue::from_static(version::VERSION));
                std::future::ready(response)
            },
        ))
}

/// Accept completed ingests only from the certificate-proven local gateway.
async fn ingest_handler(
    axum::extract::State(published): axum::extract::State<Published>,
    axum::Extension(caller): axum::Extension<Caller>,
    request: axum::extract::Request,
) -> axum::response::Response {
    published.feedback.respond(caller == Caller::Local, request).await
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
    let now = operator::signals::ClockReading::now();
    let (body, oldest, reader) = match &caller {
        Caller::Local => {
            let (body, oldest) = local_relay(&published, target, &collect, now);
            (body, oldest, operator::site_providers::Reader::Local)
        },
        Caller::Peer(Some(labels)) => {
            let (body, oldest) = published.site.render(target, &collect, Some(labels), now);
            (body, oldest, operator::site_providers::Reader::Peer(labels))
        },
        Caller::Peer(None) => return refused(),
    };
    with_catalog_version(served(body, oldest, now.wall), &published, reader)
}

/// `response` naming the version of the catalog `reader` would read, so it re-reads only when
/// that changes.
fn with_catalog_version(
    mut response: axum::response::Response,
    published: &Published,
    reader: operator::site_providers::Reader<'_>,
) -> axum::response::Response {
    if let Some(version) = published
        .providers
        .version_for(reader)
        .and_then(|version| http::HeaderValue::from_str(&version).ok())
    {
        response
            .headers_mut()
            .insert(operator::site_providers::VERSION_HEADER, version);
    }
    response
}

/// Serve this site's providers, scoped by the caller's certificate like its signals.
async fn providers_handler(
    axum::extract::State(published): axum::extract::State<Published>,
    axum::Extension(caller): axum::Extension<Caller>,
) -> axum::response::Response {
    let reader = match &caller {
        Caller::Local => operator::site_providers::Reader::Local,
        Caller::Peer(Some(labels)) => operator::site_providers::Reader::Peer(labels),
        Caller::Peer(None) => return refused(),
    };
    // Before the first publish an answer would read as no providers, so a peer keeps what it holds.
    let Some(rendered) = published.providers.rendered(reader) else {
        return http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let contract = operator::site_providers::CONTRACT_VERSION.to_string();
    // The version and the body come from one render, so the reader records what it read.
    (
        [
            (http::header::CONTENT_TYPE.as_str(), "application/json"),
            (operator::site_providers::CONTRACT_HEADER, contract.as_str()),
            (operator::site_providers::VERSION_HEADER, rendered.version.as_str()),
        ],
        rendered.body,
    )
        .into_response()
}

/// This site's own gateway's read: the poller's rounds, then the site, then its peers, in one
/// body under the gateway's read ceiling, so a flood of relayed rows can only cost the last peers,
/// never this site.
fn local_relay(
    published: &Published,
    target: Option<&str>,
    collect: &[String],
    now: operator::signals::ClockReading,
) -> (String, std::time::Duration) {
    let rounds = published.rounds.render();
    let site = published.site.render_bounded(
        target,
        collect,
        operator::signals::MAX_RELAY_BYTES.saturating_sub(rounds.len()),
        now,
    );
    // Peers relay only to Local, and the peers store carries no access map.
    // A Peer(Some) relay would need per-target scoping added here.
    let peers = published.peers.render_bounded(
        target,
        collect,
        operator::signals::MAX_RELAY_BYTES.saturating_sub(rounds.len().saturating_add(site.body.len())),
        now,
    );
    operator::metrics::record_relay_truncated("site", &site);
    operator::metrics::record_relay_truncated("peers", &peers);
    let mut body = rounds;
    body.push_str(&site.body);
    body.push_str(&peers.body);
    (body, site.oldest.max(peers.oldest))
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

/// One exposition response, with `Age` bounding the whole body and `Date` the
/// wall reading its stamps were written from.
///
/// A reader takes each row's age as `Date` minus its stamp. hyper's own `Date` is
/// cached and, after a backward clock step, stays frozen for about the size of
/// the step, which would age every row by that much; an explicit `Date` wins over
/// it.
fn served(body: String, oldest: std::time::Duration, wall: SystemTime) -> axum::response::Response {
    (
        [
            (http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8"),
            (http::header::AGE, &*oldest.as_secs().to_string()),
            (http::header::DATE, &*httpdate::fmt_http_date(wall)),
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
    Box::pin(operator::site_name::sole_network(client)).await.ok().flatten()
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

/// Scrape this site's providers on their own interval, write their readiness, and in Poll
/// mode publish the result.
///
/// Separate from reconcile, which runs on an interval sized for declarations
/// and is two orders of magnitude slower than these values move.
async fn run_local_scraper(
    interval: std::time::Duration,
    ctx: Arc<OperatorCtx>,
    client: Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
        let live: Vec<&str> = list.items.iter().filter_map(|n| n.metadata.name.as_deref()).collect();
        // A network deleted with no finalizer is never reconciled again, so forget it here.
        ctx.local_providers().retain_networks(|held| live.contains(&held));
        for network in live {
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
    let swim = swim_settled(swim).await;
    if swim.is_none() {
        tracing::info!("peer signals poller polls declared endpoints only: SWIM is not running");
    }
    let poller = PeerPoller {
        ctx,
        swim,
        client,
        settings,
        shutdown,
        ages: operator::peer_ages::PeerAges::default(),
        memberships: operator::peer_membership::PeerMemberships::default(),
        reach: operator::peer_reach::PeerReach::default(),
        misses: operator::peer_reach::Misses::default(),
        written: NetworkWritten::default(),
        catalogs: operator::peer_reach::PeerReach::none(&operator::peer_reach::CATALOGS),
        sites_written: false,
        catalog_versions: std::collections::HashMap::new(),
        remembered: BTreeMap::new(),
        unanswered: BTreeSet::new(),
        turn: 0,
    };
    Box::pin(poller.run()).await;
    Ok(())
}

/// Whether a peer's catalog is due a read: on the [`CATALOG_ROUNDS`] schedule, the floor under a
/// version that misses a change, and between, when the version it names differs from the one
/// last read.
fn catalog_stale(named: Option<&str>, last_read: Option<&str>, scheduled: bool) -> bool {
    scheduled || named.is_some_and(|version| last_read != Some(version))
}

/// Rounds between reads of a peer's catalog, which changes with its providers, not its load.
const CATALOG_ROUNDS: usize = 6;

/// The invariant state one peer-polling session threads through every round.
struct PeerPoller {
    /// Where polled peer signals are published and peer identities are read.
    ctx: Arc<OperatorCtx>,
    /// Membership, for the peers and their advertised addresses, `None` without SWIM.
    swim: Option<Arc<swim_runtime::SwimHandle>>,
    /// Kube client, for resolving this site's TLS material.
    client: Client,
    /// Poll cadence, timeouts, and what to collect.
    settings: operator::cli::SignalsArgs,
    /// Lets an in-flight round stand down when the process is stopping.
    shutdown: operator::shutdown::Shutdown,
    /// Peers whose relayed ages the last rounds rejected, for the `GridNetwork` condition.
    ages: operator::peer_ages::PeerAges,
    /// Peers naming a different membership mode, for the `GridNetwork` condition.
    memberships: operator::peer_membership::PeerMemberships,
    /// Under Local membership, which peers' signals endpoints did not answer, as last written.
    reach: operator::peer_reach::PeerReach,
    /// Rounds in a row each peer went unanswered, which a reachable peer outlasts.
    misses: operator::peer_reach::Misses,
    /// What the `GridNetwork`'s peer conditions were last written from.
    written: NetworkWritten,
    /// Under Local membership, which peers' catalogs were refused, as last written.
    catalogs: operator::peer_reach::PeerReach,
    /// Whether the peer `GridSite` conditions were last written in full.
    sites_written: bool,
    /// Each peer's catalog version as last read, so a changed one is read in the next round.
    catalog_versions: std::collections::HashMap<String, String>,
    /// Each peer where it was last polled, so one SWIM no longer lists is still polled there.
    remembered: BTreeMap<String, operator::signals::PeerSite>,
    /// Peers polled last round that did not answer, polled last and briefly.
    unanswered: BTreeSet<String>,
    /// Rounds run, which rotates the order the unanswered peers are polled in.
    turn: usize,
}

impl PeerPoller {
    /// Rebuild and poll until shutdown.
    async fn run(mut self) {
        while !self.shutdown.is_triggered() && self.cycle().await {}
        tracing::info!("peer signals poller stopped");
    }

    /// One build-and-poll cycle; `false` means shutdown arrived while idle.
    async fn cycle(&mut self) -> bool {
        let own = Box::pin(sole_own_leaf(&self.client)).await;
        let Some(source) = Box::pin(peer_source(
            &self.client,
            self.shutdown.clone(),
            (
                self.ctx.peer_settings().trust,
                self.ctx.site_name().unwrap_or_default().to_owned(),
            ),
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
        &mut self,
        source: &operator::signals::PollPeers,
        changed: impl Future<Output = ()> + Send,
    ) {
        let interval = std::time::Duration::from_secs(self.settings.peer_interval_secs);
        let mut changed = std::pin::pin!(changed);
        let mut next = tokio::time::Instant::now();
        loop {
            tokio::select! {
                biased;
                () = self.shutdown.triggered() => return,
                () = &mut changed => {
                    tracing::info!("peer signals TLS material changed; rebuilding the client");
                    return;
                },
                () = tokio::time::sleep_until(next) => {},
            }
            self.poll_once(source).await;
            // Each round is spread apart, so sites that started together do not poll in step.
            next = round_after(next, interval);
        }
    }

    /// Under Local membership, write on each peer's `GridSite` whether its signals endpoint
    /// answered and its catalog was accepted, and the counts on the `GridNetwork`, when that
    /// changed. Under Swim, remove what a Local run left on the `GridSite` objects, once.
    async fn report_reach(
        &mut self,
        (source, local_site): (&operator::signals::PollPeers, &str),
        polled: &[String],
        answered: &BTreeMap<String, usize>,
        urls: &std::collections::HashMap<String, String>,
    ) {
        use operator::peer_reach::{CATALOGS, PeerReach, REACH};
        if self.ctx.membership() != Membership::Local {
            if !self.sites_written {
                let none = [PeerReach::none(&REACH), PeerReach::none(&CATALOGS)];
                let sites = Box::pin(write_site_conditions(&self.client, &none, &self.shutdown)).await;
                let network = Box::pin(withdraw_network_conditions(&self.client, &self.shutdown)).await;
                self.sites_written = sites && network;
            }
            return;
        }
        let (reach, catalogs, skipped) = self.judge((source, local_site), polled, answered, urls);
        Box::pin(self.write_network((&reach, &catalogs), (skipped, polled.len()))).await;
        for (peer, judged) in reach.judged() {
            operator::metrics::set_peer_signals_reachable(peer, judged.is_clear());
        }
        for (peer, judged) in catalogs.judged() {
            operator::metrics::set_peer_catalog_refused(peer, !judged.is_clear());
        }
        let changed = reach != self.reach || catalogs != self.catalogs;
        // Rechecked against the live objects on the catalog schedule, so a lost write heals.
        if changed || !self.sites_written || self.turn.is_multiple_of(CATALOG_ROUNDS) {
            let judged = [reach.clone(), catalogs.clone()];
            self.sites_written = Box::pin(write_site_conditions(&self.client, &judged, &self.shutdown)).await;
        }
        self.reach = reach;
        self.catalogs = catalogs;
    }

    /// Write each `GridNetwork` peer condition this round changed, or whose last write failed.
    async fn write_network(
        &mut self,
        (reach, catalogs): (&operator::peer_reach::PeerReach, &operator::peer_reach::PeerReach),
        polls: (usize, usize),
    ) {
        if write_due(self.written.polls.as_ref(), &polls) {
            let landed = Box::pin(write_peer_polls(&self.client, polls, &self.shutdown)).await;
            self.written.polls = landed.then_some(polls);
        }
        if write_due(self.written.reach.as_ref(), reach) {
            let landed = Box::pin(write_peer_reach(&self.client, reach, &self.shutdown)).await;
            self.written.reach = landed.then(|| reach.clone());
        }
        if write_due(self.written.catalogs.as_ref(), catalogs) {
            let landed = Box::pin(write_peer_reach(&self.client, catalogs, &self.shutdown)).await;
            self.written.catalogs = landed.then(|| catalogs.clone());
        }
    }

    /// This round's signals and catalog verdicts on each peer, and how many peers it had no
    /// time to poll.
    fn judge(
        &self,
        (source, local_site): (&operator::signals::PollPeers, &str),
        polled: &[String],
        answered: &BTreeMap<String, usize>,
        urls: &std::collections::HashMap<String, String>,
    ) -> (operator::peer_reach::PeerReach, operator::peer_reach::PeerReach, usize) {
        use operator::peer_reach::PeerReach;
        let without_endpoint = self.ctx.peer_identities().without_endpoint(local_site);
        let reach = PeerReach::of(
            answered.keys().map(String::as_str),
            self.unanswered.iter().map(|peer| {
                let failure = source
                    .failures
                    .of(peer)
                    .map(|why| (why, urls.get(peer).map_or("", String::as_str)));
                (peer.as_str(), failure)
            }),
            without_endpoint.iter().map(String::as_str),
        );
        let skipped = reach.skipped();
        let reach = reach.held_over(&self.reach, &self.misses);
        let held = self.ctx.peer_providers();
        let catalogs = PeerReach::catalogs(
            polled.iter().map(String::as_str).filter(|site| held.holds(site)),
            &held.verdicts(),
        );
        (reach, catalogs, skipped)
    }

    /// Collect from every known peer, replace what is published for them, and say
    /// on the `GridNetwork` whose relayed ages are being rejected.
    #[expect(
        clippy::too_many_lines,
        reason = "one round: targets, order, collection, then what it taught"
    )]
    async fn poll_once(&mut self, source: &operator::signals::PollPeers) {
        let membership_ready = self.ctx.membership_hold().is_none();
        let snapshot = self.swim.as_ref().map(|swim| swim.snapshot());
        let Some(local_site) = self
            .swim
            .as_ref()
            .map(|swim| swim.site_name())
            .or_else(|| self.ctx.site_name())
            .map(str::to_owned)
        else {
            tracing::debug!("peer polling waits for this site's name");
            return;
        };
        let identities = self.ctx.peer_identities();
        let targets = Targeting {
            identities: &identities,
            trust: source.trust,
            encrypted: self.swim.as_ref().is_some_and(|swim| swim.is_encrypted()),
            local_site: &local_site,
        }
        .targets(snapshot.as_ref(), &mut self.remembered);
        // A Local site whose peers are all endpoint-less still reports on them, and clears what
        // the last pollable one left.
        let idle = idle_round(self.ctx.membership(), identities.loaded());
        let Some(mut sites) = targets.or_else(|| idle.then(Vec::new)) else {
            return;
        };
        self.turn = self.turn.wrapping_add(1);
        order_targets(&mut sites, &self.unanswered, self.turn);
        for site in sites.iter_mut().filter(|site| !site.answering) {
            site.answering = self.misses.full_budget_due(&site.name);
        }
        // Each peer lands as it answers, so one slow peer cannot age out the rest.
        let peers = self.ctx.peers();
        let heard = self.ctx.load_heard();
        heard.retain_sites(|site| identities.knows(site));
        let ttl = self.settings.peer_ttl();
        let mut answered = BTreeMap::new();
        let mut modes = BTreeMap::new();
        let mut versions = std::collections::HashMap::<String, Option<String>>::new();
        source
            .collect_each(&sites, |peer, observations, rejected, stamp| {
                answered.insert(peer.clone(), rejected);
                modes.insert(peer.clone(), stamp.membership);
                versions.insert(peer.clone(), stamp.providers_version);
                heard.record(&peer, &observations);
                peers.refresh(BTreeMap::from([(peer, observations)]), ttl);
            })
            .await;
        // A round stood down by shutdown learned nothing, and writing would hold termination open.
        if self.shutdown.is_triggered() {
            return;
        }
        // Catalogs change rarely: a peer's is read when none is held, then every
        // CATALOG_ROUNDS rounds, within one round. A silent peer keeps what it last served.
        let catalogs = self.ctx.peer_providers();
        let due = self.turn.is_multiple_of(CATALOG_ROUNDS);
        let read_at = &self.catalog_versions;
        let stale = |site: &str| {
            catalog_stale(
                versions.get(site).and_then(Option::as_deref),
                read_at.get(site).map(String::as_str),
                due,
            )
        };
        let wanted = sites
            .iter()
            .filter(|site| answered.contains_key(&site.name) && (stale(&site.name) || !catalogs.holds(&site.name)));
        let concurrency = source.dial_concurrency(sites.len());
        let Some(read) = read_catalogs(source, wanted, concurrency, &catalogs, &self.shutdown).await else {
            return;
        };
        // The version the providers answer itself named, not the one the signals answer did.
        for (site, version) in read {
            match version {
                Some(version) => self.catalog_versions.insert(site, version),
                None => self.catalog_versions.remove(&site),
            };
        }
        let urls: std::collections::HashMap<String, String> =
            sites.iter().map(|site| (site.name.clone(), site.url.clone())).collect();
        let polled: Vec<String> = sites.into_iter().map(|site| site.name).collect();
        self.catalog_versions.retain(|site, _| polled.contains(site));
        self.ctx
            .peer_providers()
            .retain_sites(|site| polled.iter().any(|polled| polled == site));
        operator::metrics::retain_peers(|site| polled.iter().any(|polled| polled == site));
        self.ctx
            .peer_catch_up()
            .record(&polled, |site| answered.contains_key(site), |site| catalogs.holds(site));
        self.ctx.peer_rounds().record(&polled, SystemTime::now());
        self.misses.record(
            &polled,
            |site| answered.contains_key(site),
            |site| source.failures.of(site) == Some(operator::signals::Unreachable::RoundDeadline),
        );
        self.unanswered = polled
            .iter()
            .filter(|site| !answered.contains_key(*site))
            .cloned()
            .collect();
        Box::pin(self.report_reach((source, &local_site), &polled, &answered, &urls)).await;
        if observe_peer_ages(membership_ready, &mut self.ages, &polled, &answered) {
            Box::pin(write_peer_ages(&self.client, &self.ages, &self.shutdown)).await;
        }
        if membership_ready {
            let ours = self.ctx.membership().as_str();
            self.memberships.observe(&polled, &modes, ours);
            Box::pin(write_peer_memberships(
                &self.client,
                &self.memberships,
                ours,
                &self.shutdown,
            ))
            .await;
        }
    }
}

/// Read the catalogs of `wanted` into `catalogs` within one round, with the version each
/// answer named, or `None` when shutdown stood the reads down.
async fn read_catalogs(
    source: &operator::signals::PollPeers,
    wanted: impl Iterator<Item = &operator::signals::PeerSite>,
    concurrency: usize,
    catalogs: &operator::site_providers::PeerProviders,
    shutdown: &operator::shutdown::Shutdown,
) -> Option<Vec<(String, Option<String>)>> {
    let read = std::sync::Mutex::new(Vec::new());
    let reads = futures::stream::iter(wanted).for_each_concurrent(concurrency, |site| async {
        if let Some((providers, version)) = source.fetch_providers(site).await {
            operator::metrics::record_peer_catalog(&site.name, "replaced");
            catalogs.replace(&site.name, providers);
            read.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((site.name.clone(), version));
        } else {
            operator::metrics::record_peer_catalog(&site.name, "failed");
        }
    });
    let finished = tokio::select! {
        biased;
        () = shutdown.triggered() => return None,
        finished = tokio::time::timeout(source.round, reads) => finished,
    };
    if finished.is_err() {
        tracing::debug!("peer catalog reads cut at the round deadline");
    }
    Some(read.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner))
}

/// When the round after one started at `started` begins: `interval` later, jittered, and never
/// in the past, so a round that overran is followed at once and not by a burst.
fn round_after(started: tokio::time::Instant, interval: std::time::Duration) -> tokio::time::Instant {
    (started + operator::enroll::jittered(interval)).max(tokio::time::Instant::now())
}

/// The order a round polls `sites` in.
///
/// One peer that did not answer last round goes first, so a peer cut from a saturated round
/// always has a way back; then the peers that answered; then the other unanswered peers. Both
/// groups rotate by `turn`, so a round that runs out cuts different peers each time.
fn order_targets(sites: &mut [operator::signals::PeerSite], unanswered: &BTreeSet<String>, turn: usize) {
    for site in sites.iter_mut() {
        site.answering = !unanswered.contains(&site.name);
    }
    sites.sort_by_key(|site| !site.answering);
    let split = sites.partition_point(|site| site.answering);
    let (answering, rest) = sites.split_at_mut(split);
    for group in [&mut *answering, &mut *rest] {
        if !group.is_empty() {
            let len = group.len();
            group.rotate_left(turn % len);
        }
    }
    // The reserved slot: the head of the unanswered group moves ahead of every answering peer.
    if !rest.is_empty() {
        sites
            .get_mut(..=split)
            .into_iter()
            .for_each(|front| front.rotate_right(1));
    }
}

/// Observe a round only if membership converged before its target snapshot, so
/// convergence during collection cannot make an old startup snapshot erase evidence.
fn observe_peer_ages(
    membership_ready: bool,
    ages: &mut operator::peer_ages::PeerAges,
    polled: &[String],
    answered: &BTreeMap<String, usize>,
) -> bool {
    if !membership_ready {
        return false;
    }
    ages.observe(polled, answered);
    true
}

/// Write the `PeerTimeValid` condition on the sole `GridNetwork` when it changed.
///
/// Compared against the live status every round, so a restart, a recreated
/// `GridNetwork` or an edited status is set right on the next round, and nothing
/// is written while the condition already says this.
async fn write_peer_ages(
    client: &Client,
    ages: &operator::peer_ages::PeerAges,
    shutdown: &operator::shutdown::Shutdown,
) {
    tokio::select! {
        biased;
        () = shutdown.triggered() => {},
        result = tokio::time::timeout(PEER_AGES_WRITE_TIMEOUT, Box::pin(apply_peer_ages(client, ages))) => {
            if result.is_err() {
                tracing::warn!("peer ages condition write timed out; retrying next round");
            }
        },
    }
}

/// Total lookup-and-patch budget, so diagnostic publication cannot hold peer
/// freshness behind a slow Kubernetes API.
const PEER_AGES_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Look up the live network and apply its peer-age condition when changed.
async fn apply_peer_ages(client: &Client, ages: &operator::peer_ages::PeerAges) {
    let Some(network) = Box::pin(sole_network(client)).await else {
        return;
    };
    let Ok(now) = time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339) else {
        return;
    };
    let Some((name, patch)) = operator::peer_ages::patch(&network, ages, &now) else {
        return;
    };
    let api: Api<GridNetwork> = Api::all(client.clone());
    let params = PatchParams::apply(operator::peer_ages::FIELD_MANAGER).force();
    match Box::pin(api.patch_status(name, &params, &Patch::Apply(patch))).await {
        Ok(_) if ages.rejecting().is_empty() => {
            tracing::info!("relayed signal ages accepted from every polled peer");
        },
        Ok(_) => tracing::warn!(
            peers = ?ages.rejecting(),
            "relayed signal ages rejected from peers; check their clocks"
        ),
        Err(error) => tracing::warn!(%error, "peer ages condition not written; retrying next round"),
    }
}

/// Write the `PeerSignalsReachable` or `PeerCatalogsAccepted` count on the sole `GridNetwork`,
/// bounded and cut short by shutdown.
async fn write_peer_reach(
    client: &Client,
    reach: &operator::peer_reach::PeerReach,
    shutdown: &operator::shutdown::Shutdown,
) -> bool {
    Box::pin(write_network_condition(
        client,
        reach.kind().field_manager,
        |network, now| operator::peer_reach::patch(network, reach, now),
        shutdown,
    ))
    .await
}

/// Whether a `GridNetwork` condition judged as `next` is to be written: it differs from what
/// was `written` last, `None` when that write did not land.
fn write_due<T: PartialEq>(written: Option<&T>, next: &T) -> bool {
    written != Some(next)
}

/// What the `GridNetwork`'s peer conditions were last written from, each `None` until a write
/// lands, so one that failed is written again however the next round judges.
#[derive(Default)]
struct NetworkWritten {
    /// Peers a round had no time for, of how many it polled.
    polls: Option<(usize, usize)>,
    /// `PeerSignalsReachable`'s verdicts.
    reach: Option<operator::peer_reach::PeerReach>,
    /// `PeerCatalogsAccepted`'s verdicts.
    catalogs: Option<operator::peer_reach::PeerReach>,
}

/// Whether a round with nothing to poll still runs: under Local once the peers are read, so
/// endpoint-less peers are reported and a last endpoint's removal is seen. Without that input
/// the round waits.
fn idle_round(membership: Membership, peers_read: bool) -> bool {
    membership == Membership::Local && peers_read
}

/// Withdraw what a Local run left on the `GridNetwork`, so a site now under Swim is not
/// degraded by it. `true` once every one is gone.
async fn withdraw_network_conditions(client: &Client, shutdown: &operator::shutdown::Shutdown) -> bool {
    use operator::peer_reach::{CATALOGS, PEER_POLLS_COMPLETE, POLLS_FIELD_MANAGER, REACH};
    let mut withdrawn = true;
    for (manager, type_) in [
        (REACH.field_manager, REACH.type_),
        (CATALOGS.field_manager, CATALOGS.type_),
        (POLLS_FIELD_MANAGER, PEER_POLLS_COMPLETE),
    ] {
        withdrawn &= Box::pin(write_network_condition(
            client,
            manager,
            |network, _| operator::peer_reach::withdrawal(network, type_),
            shutdown,
        ))
        .await;
    }
    withdrawn
}

/// Write `PeerPollsComplete` after a round that had no time for `skipped` of its `judged` peers,
/// `true` once written.
async fn write_peer_polls(
    client: &Client,
    (skipped, judged): (usize, usize),
    shutdown: &operator::shutdown::Shutdown,
) -> bool {
    Box::pin(write_network_condition(
        client,
        operator::peer_reach::POLLS_FIELD_MANAGER,
        |network, now| {
            operator::peer_reach::network_patch(network, |current| {
                operator::peer_reach::polls_condition(current, (skipped, judged), now)
            })
        },
        shutdown,
    ))
    .await
}

/// Apply the condition `build` makes for the sole `GridNetwork` under `manager`, bounded and
/// stood down by shutdown. `true` when it landed or the live status already says it.
async fn write_network_condition<Build>(
    client: &Client,
    manager: &str,
    build: Build,
    shutdown: &operator::shutdown::Shutdown,
) -> bool
where
    Build: for<'network> FnOnce(&'network GridNetwork, &str) -> Option<(&'network str, serde_json::Value)> + Send,
{
    let apply = async {
        let Some(network) = Box::pin(sole_network(client)).await else {
            return false;
        };
        let Ok(now) = time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339) else {
            return false;
        };
        let Some((name, patch)) = build(&network, &now) else {
            return true;
        };
        let api: Api<GridNetwork> = Api::all(client.clone());
        let params = PatchParams::apply(manager).force();
        Box::pin(api.patch_status(name, &params, &Patch::Apply(patch)))
            .await
            .inspect_err(|error| tracing::warn!(%error, manager, "peer condition not written; retrying next round"))
            .is_ok()
    };
    tokio::select! {
        biased;
        () = shutdown.triggered() => false,
        result = tokio::time::timeout(PEER_AGES_WRITE_TIMEOUT, apply) => result.unwrap_or_else(|_elapsed| {
            tracing::warn!(manager, "peer condition write timed out; retrying next round");
            false
        }),
    }
}

/// Write each `GridSite`'s condition for every one of `judged`, against the live objects,
/// bounded and cut short by shutdown. `true` when every write landed.
async fn write_site_conditions(
    client: &Client,
    judged: &[operator::peer_reach::PeerReach],
    shutdown: &operator::shutdown::Shutdown,
) -> bool {
    tokio::select! {
        biased;
        () = shutdown.triggered() => false,
        result = tokio::time::timeout(PEER_AGES_WRITE_TIMEOUT, Box::pin(apply_site_conditions(client, judged))) => {
            result.unwrap_or_else(|_| {
                tracing::warn!("peer GridSite condition writes timed out; retrying next round");
                false
            })
        },
    }
}

/// List the `GridSite` objects and apply each condition of `judged` that differs; `true` when
/// every write landed.
async fn apply_site_conditions(client: &Client, judged: &[operator::peer_reach::PeerReach]) -> bool {
    let api: Api<GridSite> = Api::all(client.clone());
    let sites = match Box::pin(api.list(&kube::api::ListParams::default())).await {
        Ok(sites) => sites,
        Err(error) => {
            tracing::warn!(%error, "peer GridSites not listed; their conditions retry next round");
            return false;
        },
    };
    let Ok(now) = time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339) else {
        return false;
    };
    let mut landed = true;
    for site in &sites.items {
        for reach in judged {
            let Some((name, patch)) = reach.site_patch(site, &now) else {
                continue;
            };
            let params = PatchParams::apply(reach.kind().field_manager).force();
            if let Err(error) = Box::pin(api.patch_status(name, &params, &Patch::Apply(patch))).await {
                tracing::warn!(%error, site = name, "peer GridSite condition not written; retrying next round");
                landed = false;
            }
        }
    }
    landed
}

/// Write the `PeerMembership` condition on the sole `GridNetwork` when it changed.
async fn write_peer_memberships(
    client: &Client,
    memberships: &operator::peer_membership::PeerMemberships,
    ours: &str,
    shutdown: &operator::shutdown::Shutdown,
) {
    let apply = async {
        let Some(network) = Box::pin(sole_network(client)).await else {
            return;
        };
        let Ok(now) = time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339) else {
            return;
        };
        let Some((name, patch)) = operator::peer_membership::patch(&network, memberships, ours, &now) else {
            return;
        };
        let api: Api<GridNetwork> = Api::all(client.clone());
        let params = PatchParams::apply(operator::peer_membership::FIELD_MANAGER).force();
        match Box::pin(api.patch_status(name, &params, &Patch::Apply(patch))).await {
            Ok(_) if memberships.mismatched().is_empty() => {},
            Ok(_) => tracing::warn!(peers = ?memberships.mismatched(), ours, "peers run a different membership mode"),
            Err(error) => tracing::warn!(%error, "peer membership condition not written; retrying next round"),
        }
    };
    tokio::select! {
        biased;
        () = shutdown.triggered() => {},
        result = tokio::time::timeout(PEER_AGES_WRITE_TIMEOUT, apply) => {
            if result.is_err() {
                tracing::warn!("peer membership condition write timed out; retrying next round");
            }
        },
    }
}

/// What one round polls, from the local `GridSite` objects and, when readable, gossip.
struct Targeting<'round> {
    /// Who may be polled, and where each is declared.
    identities: &'round operator::signals::PeerIdentities,
    /// How peers are authorized.
    trust: operator::signals::PeerTrustMode,
    /// Whether gossip is encrypted, which SPIFFE trust needs before reading it.
    encrypted: bool,
    /// This site, never polled.
    local_site: &'round str,
}

impl Targeting<'_> {
    /// Whether `site` is one this site would name under its trust.
    fn named(&self, site: &str) -> bool {
        if self.trust == operator::signals::PeerTrustMode::Pin {
            !self.identities.refuses(site)
        } else {
            self.identities.labels_for(site).is_some()
        }
    }

    /// Peers this site would name, `None` when there is none to poll.
    ///
    /// Gossiped peers first: every member whatever its SWIM state at the signals address it
    /// gossiped, then each peer SWIM no longer lists at the address it was last polled at while
    /// its `GridSite` still names it, so polling never waits on gossip. Gossip is not read in
    /// SPIFFE mode while plaintext. A `GridSite` that declares a signals endpoint is then polled
    /// there, without SWIM and over any gossiped address.
    fn targets(
        &self,
        snapshot: Option<&operator::swim::MembershipSnapshot>,
        remembered: &mut BTreeMap<String, operator::signals::PeerSite>,
    ) -> Option<Vec<operator::signals::PeerSite>> {
        let pinned = self.trust == operator::signals::PeerTrustMode::Pin;
        if !pinned && !self.encrypted {
            tracing::debug!("SPIFFE peer polling ignores plaintext gossip");
        }
        let gossiped = snapshot.filter(|_| pinned || self.encrypted);
        let mut targets = BTreeMap::new();
        if let Some(members) = gossiped {
            self.gossiped(members, remembered, &mut targets);
        }
        for (name, declared) in self.identities.declared_endpoints() {
            if name == self.local_site || !self.named(&name) {
                continue;
            }
            if let Some(site) = declared.peer_site(name) {
                targets.insert(site.name.clone(), site);
            }
        }
        if gossiped.is_none() && targets.is_empty() {
            return None;
        }
        let mut sites: Vec<_> = targets.into_values().collect();
        if pinned {
            for site in &mut sites {
                site.pins = self.identities.pins_for(&site.name);
            }
        }
        Some(sites)
    }

    /// The gossiped peers into `targets`, each remembered where it was last polled.
    fn gossiped(
        &self,
        members: &operator::swim::MembershipSnapshot,
        remembered: &mut BTreeMap<String, operator::signals::PeerSite>,
        targets: &mut BTreeMap<String, operator::signals::PeerSite>,
    ) {
        let listed = members
            .members
            .iter()
            .filter(|m| m.signals_address.is_some() && self.named(&m.site_id));
        let sites = operator::signals::peer_sites(listed, self.local_site, "https", 0);
        remembered.retain(|site, _| self.named(site));
        for site in sites {
            remembered.insert(site.name.clone(), site);
        }
        targets.extend(remembered.iter().map(|(name, site)| (name.clone(), site.clone())));
    }
}

/// Build the peer poller source from verified TLS material, or `None` when absent.
async fn peer_source(
    client: &Client,
    shutdown: operator::shutdown::Shutdown,
    (trust, local_site): (operator::signals::PeerTrustMode, String),
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
        round: std::time::Duration::from_secs(settings.peer_interval_secs),
        slow_after: std::time::Duration::from_millis(settings.peer_slow_ms),
        shutdown,
        trust,
        transports: operator::signals::PeerTransports::default(),
        failures: operator::signals::PeerFailures::default(),
        local_site,
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

    #[tokio::test]
    async fn startup_reads_network_membership_before_selecting_the_swim_listener() -> Result<(), String> {
        let install = GridModes::without_network(Some(SignalMode::Poll), Some(operator::signals::PeerTrustMode::Pin));
        for (spec, expected, declared) in [
            (None, Membership::Swim, false),
            (Some(serde_json::json!({})), Membership::Swim, false),
            (
                Some(serde_json::json!({"membership": {"mode": "Local"}, "signalTransport": {"mode": "poll"}})),
                Membership::Local,
                true,
            ),
            (
                Some(serde_json::json!({"membership": {"mode": "Swim"}})),
                Membership::Swim,
                true,
            ),
        ] {
            let client = network_modes_client(spec.clone());
            let (modes, membership_declared) = resolve_grid_modes(&client, true, install).await?;
            assert_eq!(modes.membership, expected, "spec={spec:?}, modes={modes:?}");
            assert_eq!(membership_declared, declared, "spec={spec:?}, modes={modes:?}");
            let swim = swim_requested(modes.membership, membership_declared, true);
            assert!(
                swim.as_ref()
                    .is_ok_and(|enabled| *enabled == (expected == Membership::Swim)),
                "spec={spec:?}, modes={modes:?}, membership_declared={membership_declared}, swim={swim:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn startup_validates_transport_after_network_overrides() -> Result<(), String> {
        let cli = Cli::try_parse_from(["operator", "--membership-source", "Local", "--grid-signals", "gossip"])
            .map_err(|error| error.to_string())?;
        let install = GridModes::without_network(cli.grid.startup_signal_transport(), cli.grid.peer_trust)
            .with_membership(cli.grid.membership);
        for (spec, expected) in [
            (
                Some(serde_json::json!({"membership": {"mode": "Swim"}, "signalTransport": {"mode": "gossip"}})),
                Some((Membership::Swim, SignalMode::Gossip)),
            ),
            (
                Some(serde_json::json!({"membership": {"mode": "Local"}, "signalTransport": {"mode": "poll"}})),
                Some((Membership::Local, SignalMode::Poll)),
            ),
            (
                Some(serde_json::json!({"membership": {"mode": "Local"}, "signalTransport": {"mode": "gossip"}})),
                None,
            ),
            (None, None),
        ] {
            let resolved = resolve_grid_modes(&network_modes_client(spec.clone()), true, install).await;
            if let Some((membership, signal)) = expected {
                let (modes, declared) = resolved?;
                assert_eq!(
                    (modes.membership, modes.signal),
                    (membership, signal),
                    "spec={spec:?}, modes={modes:?}"
                );
                assert!(
                    declared,
                    "spec={spec:?} must override the install's contradictory modes"
                );
            } else {
                assert!(
                    resolved
                        .as_ref()
                        .is_err_and(|error| error.contains("Local membership requires")),
                    "spec={spec:?} leaves Local/gossip invalid: {resolved:?}"
                );
            }
        }
        Ok(())
    }

    /// An API serving the optional network used to resolve startup modes.
    fn network_modes_client(spec: Option<serde_json::Value>) -> Client {
        let items: Vec<_> = spec
            .into_iter()
            .map(|spec| {
                serde_json::json!({
                    "apiVersion": "grid.praxis.fast/v1alpha1", "kind": "GridNetwork",
                    "metadata": {"name": "grid"}, "spec": spec
                })
            })
            .collect();
        let body = serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1", "kind": "GridNetworkList",
            "metadata": {}, "items": items
        })
        .to_string();
        Client::new(
            tower::service_fn(move |_request: http::Request<kube::client::Body>| {
                let body = body.clone();
                async move { Ok::<_, std::convert::Infallible>(http::Response::new(axum::body::Body::from(body))) }
            }),
            "default",
        )
    }

    #[test]
    fn declared_local_membership_disables_an_installed_swim_listener() {
        for has_bind in [false, true] {
            assert!(swim_requested(Membership::Local, true, has_bind).is_ok_and(|enabled| !enabled));
            assert!(swim_requested(Membership::Swim, true, has_bind).is_ok_and(|enabled| enabled == has_bind));
        }
        assert!(
            swim_requested(Membership::Local, false, true).is_err(),
            "without a network override, contradictory install settings remain an error"
        );
        assert!(swim_requested(Membership::Local, false, false).is_ok_and(|enabled| !enabled));
    }

    #[tokio::test]
    async fn feedback_handler_refuses_every_peer_scope() {
        let client = Client::new(
            tower::service_fn(|_request: http::Request<kube::client::Body>| async {
                std::future::pending::<Result<http::Response<axum::body::Body>, std::convert::Infallible>>().await
            }),
            "default",
        );
        let published = Published {
            site: operator::signals::SignalStore::new(),
            peers: operator::signals::SignalStore::new(),
            rounds: operator::signals::PeerRounds::default(),
            providers: operator::site_providers::LocalProviders::default(),
            membership: "Swim",
            feedback: operator::gateway_ages::GatewayFeedback::new(client, operator::shutdown::Shutdown::never()),
        };
        for caller in [Caller::Peer(None), Caller::Peer(Some(BTreeMap::new()))] {
            let response = ingest_handler(
                axum::extract::State(published.clone()),
                axum::Extension(caller),
                http::Request::new(axum::body::Body::empty()),
            )
            .await;
            assert_eq!(
                response.status(),
                http::StatusCode::FORBIDDEN,
                "neither authorized peers nor absent identities can publish gateway status"
            );
        }
    }

    #[tokio::test]
    async fn peer_age_observation_waits_for_membership_release() {
        let client = stalled_peer_ages_client(false, Arc::new(std::sync::atomic::AtomicUsize::new(0)));
        let ctx = OperatorCtx::new(client, None, SignalMode::Poll).hold_membership();
        let mut ages = operator::peer_ages::PeerAges::default();
        ages.observe(&["east".to_owned()], &BTreeMap::from([("east".to_owned(), 1)]));
        let previous = ages.clone();
        let live = ages
            .condition(&[], "t0", Some(1))
            .expect("rejection recorded before convergence");
        let held_round = ctx.membership_hold().is_none();
        ctx.release_membership();
        assert!(
            !observe_peer_ages(held_round, &mut ages, &[], &BTreeMap::new()),
            "release during collection must not make a held startup snapshot publishable"
        );
        assert_eq!(ages, previous, "the membership hold must prevent evidence pruning");
        let ready_round = ctx.membership_hold().is_none();
        assert!(
            observe_peer_ages(ready_round, &mut ages, &[], &BTreeMap::new()),
            "the following converged round enables observation and publication"
        );
        assert_eq!(
            ages.condition(std::slice::from_ref(&live), "t1", Some(1))
                .map(|condition| condition.status),
            Some("True".to_owned()),
            "a genuinely empty snapshot after convergence clears rejection"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn peer_age_publication_bounds_stalled_lookups_and_patches() {
        for stall_patch in [false, true] {
            let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let client = stalled_peer_ages_client(stall_patch, Arc::clone(&requests));
            let ages = operator::peer_ages::PeerAges::default();
            let shutdown = operator::shutdown::Shutdown::never();
            let start = tokio::time::Instant::now();
            let completed = tokio::time::timeout(
                PEER_AGES_WRITE_TIMEOUT.saturating_add(std::time::Duration::from_secs(1)),
                write_peer_ages(&client, &ages, &shutdown),
            )
            .await;
            assert!(completed.is_ok(), "a stalled API must release the peer polling round");
            assert_eq!(
                start.elapsed(),
                PEER_AGES_WRITE_TIMEOUT,
                "one budget covers lookup and patch"
            );
            assert_eq!(
                requests.load(std::sync::atomic::Ordering::SeqCst),
                if stall_patch { 2 } else { 1 },
                "the test must reach the intended stalled request"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cancels_peer_age_publication_during_lookup_or_patch() {
        for stall_patch in [false, true] {
            let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let client = stalled_peer_ages_client(stall_patch, Arc::clone(&requests));
            let ages = operator::peer_ages::PeerAges::default();
            let (trigger, shutdown) = operator::shutdown::Trigger::new();
            let writing = tokio::spawn(async move { write_peer_ages(&client, &ages, &shutdown).await });
            let expected_requests = if stall_patch { 2 } else { 1 };
            wait_for_peer_age_requests(&requests, expected_requests).await;
            let start = tokio::time::Instant::now();
            trigger.trigger();
            assert!(
                writing.await.is_ok(),
                "a stalled diagnostic must not hold termination open"
            );
            assert_eq!(
                start.elapsed(),
                std::time::Duration::ZERO,
                "shutdown must beat the deadline"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_already_triggered_shutdown_skips_peer_age_publication() {
        let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let client = stalled_peer_ages_client(false, Arc::clone(&requests));
        let ages = operator::peer_ages::PeerAges::default();
        let (trigger, shutdown) = operator::shutdown::Trigger::new();
        trigger.trigger();
        write_peer_ages(&client, &ages, &shutdown).await;
        assert_eq!(
            requests.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "shutdown must not start a diagnostic request"
        );
    }

    /// A caller is scoped from its certificate, and nothing is trusted for
    /// presenting nothing: only this site's own certificate earns `Local`, and
    /// a missing or undeclared certificate is served nothing.
    /// After a renewal the co-located gateway stays Local on either leaf until it reloads.
    #[test]
    fn the_replaced_leaf_stays_local_until_the_gateway_reloads() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let (renewed, replaced, stranger) = ([1_u8, 1], [2_u8, 2], [3_u8, 3]);
        let identity = SignalsIdentity {
            peers: operator::signals::PeerIdentities::new(),
            own: Some(grid_network::OwnLeaf {
                fingerprint: operator::signals::leaf_fingerprint(&renewed),
                spiffe: None,
                previous: Some(operator::signals::leaf_fingerprint(&replaced)),
                anchors: None,
            }),
            trust: operator::signals::PeerTrustMode::Pin,
        };
        assert_eq!(
            caller_for(Some(&renewed), &identity, addr).0,
            Caller::Local,
            "the renewed leaf"
        );
        assert_eq!(
            caller_for(Some(&replaced), &identity, addr).0,
            Caller::Local,
            "the replaced leaf"
        );
        assert_eq!(
            caller_for(Some(&stranger), &identity, addr),
            (Caller::Peer(None), None),
            "anything else is not this site"
        );
    }

    #[test]
    fn signals_caller_scope_requires_a_positive_credential() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let leaf = [1_u8, 2, 3, 4];
        let own = |fingerprint: String| {
            Some(grid_network::OwnLeaf {
                fingerprint,
                spiffe: None,
                previous: None,
                anchors: None,
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
        let record = |pins: Vec<String>| operator::signals::PeerRecord {
            labels: gpu(),
            pins,
            signals: None,
        };
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
                previous: None,
                anchors: None,
            }),
            trust,
        }
    }

    /// A key pinned for `east` whose leaf names another site is refused, as the poller refuses it.
    #[test]
    fn a_pinned_leaf_must_name_the_site_its_pin_picks() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let (own, west) = (site_der("hub"), site_der("west"));
        // west's leaf, pinned as east's.
        let pin = hub_identity(operator::signals::PeerTrustMode::Pin, &own, &west);
        assert_eq!(caller_for(Some(&west), &pin, addr), (Caller::Peer(None), None));
    }

    /// The same callers under both trust modes.
    #[test]
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

    /// Fresh caps, and a handshake slot for loopback against them. `silent` is its own
    /// tracker, as in the listener, so a test can watch either cap on its own.
    fn handshaking(sources: &SourceSlots, peers: &PeerSlots) -> Handshaking {
        handshaking_with(sources, &SourceSlots::new(1, 1), peers)
    }

    /// As [`handshaking`], with the quiet tracker named.
    fn handshaking_with(sources: &SourceSlots, silent: &SourceSlots, peers: &PeerSlots) -> Handshaking {
        let loopback = std::net::IpAddr::from([127, 0, 0, 1]);
        Handshaking {
            warned: Arc::default(),
            turn: Arc::new(tokio::sync::Semaphore::new(1))
                .try_acquire_owned()
                .expect("a free turn"),
            source: sources.acquire(loopback).expect("a free slot"),
            silent: silent.acquire(loopback).expect("a free quiet slot"),
            peers: peers.clone(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_client_is_dropped_before_its_handshake_is_waited_out() {
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
            operator::shutdown::Shutdown::never(),
        ));
        let silent = SourceSlots::new(1, 1);
        tokio::time::timeout(SIGNALS_HANDSHAKE_TIMEOUT * 2, serve)
            .await
            .expect("released by the silence bound");
        assert_eq!(
            started.elapsed(),
            SIGNALS_SILENT_TIMEOUT,
            "a connection that sends nothing goes at the silence bound, not the handshake one"
        );
        assert!(
            SIGNALS_SILENT_TIMEOUT < SIGNALS_HANDSHAKE_TIMEOUT,
            "silence is the tighter of the two, or this proves nothing"
        );
        assert_eq!(
            sources.held(remote.ip()),
            0,
            "a dropped connection frees its source slot"
        );
        assert_eq!(silent.held(remote.ip()), 0, "and holds no quiet slot once it is gone");
    }

    #[test]
    fn the_quiet_cap_is_a_quarter_of_the_handshake_share() {
        let share = SIGNALS_MAX_HANDSHAKES / 2;
        let admission = Admission::new(8, share, operator::signals::PeerIdentities::new());
        let loopback = std::net::IpAddr::from([127, 0, 0, 1]);
        // The handshake cap is the whole share, which is what a peer set behind one router
        // needs; the quiet cap is a quarter of it, which is what one slow source gets.
        let mut held = Vec::new();
        for _ in 0..share {
            held.push(admission.sources.acquire(loopback).expect("the share is available"));
        }
        assert!(
            admission.sources.acquire(loopback).is_none(),
            "the handshake cap stops at the share"
        );
        let mut quiet = Vec::new();
        for _ in 0..(share / 4) {
            quiet.push(admission.silent.acquire(loopback).expect("a quarter is available"));
        }
        assert!(
            admission.silent.acquire(loopback).is_none(),
            "the quiet cap stops at a quarter of the share"
        );
        assert_eq!(quiet.len() * 4, held.len(), "a quarter, whatever the share is");
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

    /// Serve one fixture connection without lifecycle cancellation.
    #[cfg(not(feature = "fips"))]
    async fn serve_fixture(
        serving: Serving,
        stream: tokio::net::TcpStream,
        remote: SocketAddr,
        handshaking: Handshaking,
    ) {
        Box::pin(serve_signals_connection(
            serving,
            stream,
            remote,
            handshaking,
            operator::shutdown::Shutdown::never(),
        ))
        .await;
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
                    previous: None,
                    anchors: None,
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
                    signals: None,
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
                    served.spawn(serve_fixture(serving, stream, remote, handshaking));
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

    /// A provider served to every site, and one only `tier=gold` sites may read.
    #[cfg(not(feature = "fips"))]
    fn open_and_gold() -> operator::site_providers::LocalProviders {
        let provider = |id: &str, labels: BTreeMap<String, String>| operator::site_providers::WireProvider {
            network_id: "net".to_owned(),
            provider_id: id.to_owned(),
            routing_cluster: id.to_owned(),
            models: vec!["m".to_owned()],
            tools: Vec::new(),
            backend_kind: "local".to_owned(),
            capacity_weight: 2,
            phase: crdt::ProviderPhase::Available,
            match_labels: labels,
            revision: 1,
        };
        let local = operator::site_providers::LocalProviders::default();
        local.replace(
            "net",
            vec![
                provider("open", BTreeMap::new()),
                provider("gold", BTreeMap::from([("tier".to_owned(), "gold".to_owned())])),
            ],
        );
        local
    }

    /// What a site serving [`open_and_gold`] publishes.
    #[cfg(not(feature = "fips"))]
    fn serving_providers() -> Published {
        Published {
            site: operator::signals::SignalStore::new(),
            peers: operator::signals::SignalStore::new(),
            rounds: operator::signals::PeerRounds::default(),
            providers: open_and_gold(),
            membership: "Swim",
            feedback: operator::gateway_ages::GatewayFeedback::new(
                stalled_peer_ages_client(false, Arc::new(std::sync::atomic::AtomicUsize::new(0))),
                operator::shutdown::Shutdown::never(),
            ),
        }
    }

    /// The real peer client, presenting `west` and trusting the mesh CA.
    #[cfg(not(feature = "fips"))]
    fn west_poller(mesh: &Mesh) -> operator::signals::PollPeers {
        operator::signals::PollPeers {
            tls: Some(Arc::new(operator::signals::PeerTlsMaterial {
                ca: mesh.ca.cert_pem.clone().into_bytes(),
                identity: Some(operator::signals::PeerClientIdentity {
                    cert: mesh.west.cert_pem.clone().into_bytes(),
                    key: zeroize::Zeroizing::new(mesh.west.key_pem.clone().into_bytes()),
                }),
            })),
            attempts: 1,
            ..operator::signals::PollPeers::default()
        }
    }

    /// A pinned peer reads over mTLS only the providers its labels admit, through the real client.
    #[cfg(not(feature = "fips"))]
    #[tokio::test]
    async fn a_peer_reads_only_the_providers_its_labels_admit() {
        let mesh = Mesh::new();
        let app = signals_app(serving_providers());
        let (sources, peers) = (SourceSlots::new(1, 1), PeerSlots::new(1, 8));
        let (addr, server) = mesh.serve(mesh.west_is_pinned(), app, (&sources, &peers), 1).await;
        let poll = west_poller(&mesh);
        let east = operator::signals::PeerSite {
            name: "east".to_owned(),
            url: format!("https://{addr}{}", operator::signals::SIGNALS_PATH),
            pins: vec![operator::signals::leaf_fingerprint(&pem_der(&mesh.east.cert_pem))],
            answering: true,
            alias: None,
        };
        let (read, _) = poll.fetch_providers(&east).await.expect("east answered");
        let ids: Vec<&str> = read.iter().map(|p| p.provider_id.as_str()).collect();
        assert_eq!(
            ids,
            ["open"],
            "a tier=gpu peer is served the open provider and not the gold one"
        );
        drop(poll);
        server.await.expect("server task");
    }

    /// A peer removed while its connection is open is refused on that connection's next
    /// request, not only when it reconnects.
    #[cfg(not(feature = "fips"))]
    #[tokio::test]
    async fn a_removed_peer_is_refused_on_its_open_connection() {
        let mesh = Mesh::new();
        let identity = mesh.west_is_pinned();
        let records = identity.peers.clone();
        let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Arc::clone(&requests);
        let app = axum::Router::new().route(
            operator::site_providers::PROVIDERS_PATH,
            axum::routing::get(move |axum::Extension(caller): axum::Extension<Caller>| {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::future::ready(match caller {
                    Caller::Peer(Some(_)) => (http::StatusCode::OK, r#"{"providers":[]}"#),
                    Caller::Peer(None) | Caller::Local => (http::StatusCode::FORBIDDEN, ""),
                })
            }),
        );
        let (sources, peers) = (SourceSlots::new(1, 1), PeerSlots::new(1, 8));
        let (addr, server) = mesh.serve(identity, app, (&sources, &peers), 1).await;
        let poll = west_poller(&mesh);
        let east = operator::signals::PeerSite {
            name: "east".to_owned(),
            url: format!("https://{addr}{}", operator::signals::SIGNALS_PATH),
            pins: vec![operator::signals::leaf_fingerprint(&pem_der(&mesh.east.cert_pem))],
            answering: true,
            alias: None,
        };
        assert!(poll.fetch_providers(&east).await.is_some(), "admitted while named");
        records.set(BTreeMap::new());
        assert!(poll.fetch_providers(&east).await.is_none(), "refused once removed");
        assert!(
            requests.load(std::sync::atomic::Ordering::SeqCst) >= 2,
            "the refusal came on the one served connection, not a failed reconnect"
        );
        drop(poll);
        server.await.expect("server task");
    }

    /// A catalog is read when its named version changes, else only on the schedule.
    #[test]
    fn a_catalog_is_read_when_its_version_changes_and_on_schedule_regardless() {
        assert!(
            catalog_stale(Some("b"), Some("a"), false),
            "a new version is read at once"
        );
        assert!(catalog_stale(Some("a"), None, false), "a version never read is read");
        assert!(
            !catalog_stale(Some("a"), Some("a"), false),
            "an unchanged version is not re-read between scheduled reads"
        );
        assert!(
            catalog_stale(Some("a"), Some("a"), true),
            "the schedule re-reads even an unchanged version, the floor under a missed change"
        );
        assert!(catalog_stale(None, Some("a"), true), "no version: the schedule decides");
        assert!(!catalog_stale(None, Some("a"), false), "no version, not due");
    }

    /// Every signals answer names the catalog version and the producer release.
    #[cfg(not(feature = "fips"))]
    #[tokio::test]
    async fn a_signals_answer_names_the_catalog_version_and_the_release() {
        let mesh = Mesh::new();
        let published = serving_providers();
        let expected = published
            .providers
            .version_for(operator::site_providers::Reader::Peer(&BTreeMap::new()));
        assert!(expected.is_some(), "a published catalog has a version");
        let app = signals_app(published);
        let (sources, peers) = (SourceSlots::new(1, 1), PeerSlots::new(1, 8));
        let (addr, server) = mesh.serve(mesh.west_is_pinned(), app, (&sources, &peers), 1).await;
        let poll = west_poller(&mesh);
        let east = operator::signals::PeerSite {
            name: "east".to_owned(),
            url: format!("https://{addr}{}", operator::signals::SIGNALS_PATH),
            pins: vec![operator::signals::leaf_fingerprint(&pem_der(&mesh.east.cert_pem))],
            answering: true,
            alias: None,
        };
        let mut named = None;
        poll.collect_each(std::slice::from_ref(&east), |_, _, _, stamp| {
            named = stamp.providers_version;
        })
        .await;
        assert_eq!(named, expected);
        drop(poll);
        server.await.expect("server task");
    }

    /// A site that has not published its providers answers 503, which a reader takes as
    /// nothing new rather than no providers.
    #[cfg(not(feature = "fips"))]
    #[tokio::test]
    async fn an_unpublished_catalog_is_not_read_as_empty() {
        let mesh = Mesh::new();
        let mut published = serving_providers();
        published.providers = operator::site_providers::LocalProviders::default();
        let app = signals_app(published);
        let (sources, peers) = (SourceSlots::new(1, 1), PeerSlots::new(1, 8));
        let (addr, server) = mesh.serve(mesh.west_is_pinned(), app, (&sources, &peers), 1).await;
        let poll = west_poller(&mesh);
        let east = operator::signals::PeerSite {
            name: "east".to_owned(),
            url: format!("https://{addr}{}", operator::signals::SIGNALS_PATH),
            pins: vec![operator::signals::leaf_fingerprint(&pem_der(&mesh.east.cert_pem))],
            answering: true,
            alias: None,
        };
        assert!(
            poll.fetch_providers(&east).await.is_none(),
            "a 503 keeps the reader's copy"
        );
        drop(poll);
        server.await.expect("server task");
    }

    /// Every answer names the serving site's membership mode, and the poller reads it back.
    #[cfg(not(feature = "fips"))]
    #[tokio::test]
    async fn a_peer_answer_names_its_membership_mode() {
        let mesh = Mesh::new();
        let mut published = serving_providers();
        published.membership = "Local";
        let app = signals_app(published);
        let (sources, peers) = (SourceSlots::new(1, 1), PeerSlots::new(1, 8));
        let (addr, server) = mesh.serve(mesh.west_is_pinned(), app, (&sources, &peers), 1).await;
        let poll = west_poller(&mesh);
        let east = operator::signals::PeerSite {
            name: "east".to_owned(),
            url: format!("https://{addr}{}", operator::signals::SIGNALS_PATH),
            pins: vec![operator::signals::leaf_fingerprint(&pem_der(&mesh.east.cert_pem))],
            answering: true,
            alias: None,
        };
        let mut named = None;
        poll.collect_each(std::slice::from_ref(&east), |_, _, _, stamp| named = stamp.membership)
            .await;
        assert_eq!(named.as_deref(), Some("Local"));
        drop(poll);
        server.await.expect("server task");
    }

    /// A peer dialled under another name presents that name and is verified by its pins all the same.
    #[cfg(not(feature = "fips"))]
    #[tokio::test]
    async fn a_peer_behind_a_name_router_is_read_under_its_server_name() {
        let mesh = Mesh::new();
        let app = signals_app(serving_providers());
        let (sources, peers) = (SourceSlots::new(1, 1), PeerSlots::new(1, 8));
        let (addr, server) = mesh.serve(mesh.west_is_pinned(), app, (&sources, &peers), 1).await;
        let poll = west_poller(&mesh);
        let east = operator::signals::PeerSite {
            name: "east".to_owned(),
            url: format!(
                "https://east-signals.grid.internal:{}{}",
                addr.port(),
                operator::signals::SIGNALS_PATH
            ),
            pins: vec![operator::signals::leaf_fingerprint(&pem_der(&mesh.east.cert_pem))],
            answering: true,
            alias: Some(("east-signals.grid.internal".to_owned(), addr.ip().to_string())),
        };
        let (read, _) = poll
            .fetch_providers(&east)
            .await
            .expect("east answered under its server name");
        assert_eq!(
            read.iter().map(|p| p.provider_id.as_str()).collect::<Vec<_>>(),
            ["open"],
            "the same scoped read, dialled at the address rather than the name"
        );
        drop(poll);
        server.await.expect("server task");
    }

    /// An unnamed peer is refused the providers, and this site's own gateway is served them all.
    #[cfg(not(feature = "fips"))]
    #[tokio::test]
    async fn an_unnamed_peer_is_refused_the_providers_and_local_reads_them_all() {
        let refused = providers_handler(
            axum::extract::State(serving_providers()),
            axum::Extension(Caller::Peer(None)),
        )
        .await;
        assert_eq!(
            refused.status(),
            http::StatusCode::FORBIDDEN,
            "a peer this site holds no record for"
        );
        let local = providers_handler(
            axum::extract::State(serving_providers()),
            axum::Extension(Caller::Local),
        )
        .await;
        let body = axum::body::to_bytes(local.into_body(), operator::site_providers::MAX_BODY_BYTES)
            .await
            .expect("body");
        let read = operator::site_providers::parse(std::str::from_utf8(&body).expect("utf-8")).expect("parses");
        assert_eq!(read.len(), 2, "the local gateway is not scoped by access policy");
    }

    /// A renewal rebinds the listener on the new leaf: a peer's open connection keeps
    /// polling across it, and new connections see the renewed leaf.
    #[cfg(not(feature = "fips"))]
    #[tokio::test]
    async fn a_renewal_rebinds_the_listener_without_dropping_a_peer_poll() {
        let mesh = Mesh::new();
        let renewed = certs::generate_site_cert(&mesh.ca, "east").expect("fixture");
        let identity = || {
            let peers = operator::signals::PeerIdentities::new();
            peers.set(BTreeMap::from([(
                "west".to_owned(),
                operator::signals::PeerRecord {
                    labels: gpu(),
                    pins: Vec::new(),
                    signals: None,
                },
            )]));
            SignalsIdentity {
                peers,
                own: None,
                trust: operator::signals::PeerTrustMode::Spiffe,
            }
        };
        let serve = |leaf: &certs::SiteCertOutput| {
            let tls = tls_backend::build_server_config(
                mesh.ca.cert_pem.as_bytes(),
                leaf.cert_pem.as_bytes(),
                leaf.key_pem.as_bytes(),
            )
            .expect("fixture");
            let serving = Serving {
                tls,
                app: ok_app(),
                identity: identity(),
            };
            async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("fixture");
                let addr = listener.local_addr().expect("fixture");
                let (rotate, rotated) = tokio::sync::oneshot::channel::<()>();
                let changed = async move { drop(rotated.await) };
                let server = tokio::spawn(serve_signals_tls(
                    listener,
                    serving,
                    Admission::new(8, 8, operator::signals::PeerIdentities::new()),
                    changed,
                    operator::shutdown::Shutdown::never(),
                ));
                (addr, rotate, server)
            }
        };
        let presented = |conn: &tokio_rustls::client::TlsStream<tokio::net::TcpStream>| {
            conn.get_ref()
                .1
                .peer_certificates()
                .and_then(<[_]>::first)
                .map(|leaf| leaf.as_ref().to_vec())
        };

        let (before, rotate, server) = serve(&mesh.east).await;
        let mut polling = mesh.connect(before).await;
        assert!(get(&mut polling).await.starts_with(b"HTTP/1.1 200"), "polls before");
        rotate.send(()).expect("rotate");
        server
            .await
            .expect("server task")
            .expect("the listener stops for the new leaf");
        assert!(
            get(&mut polling).await.starts_with(b"HTTP/1.1 200"),
            "the open connection still polls across the rotation"
        );

        let (after, _rotate, _server) = serve(&renewed).await;
        let mut fresh = mesh.connect(after).await;
        assert_eq!(
            presented(&fresh),
            Some(pem_der(&renewed.cert_pem)),
            "the rebound listener presents the renewed leaf"
        );
        assert!(get(&mut fresh).await.starts_with(b"HTTP/1.1 200"), "and serves");
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
        assert_eq!(
            sources.held(std::net::IpAddr::from([127, 0, 0, 1])),
            0,
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
        let admission = Admission::new(4, 8, operator::signals::PeerIdentities::new());
        assert_eq!(
            admission.peers.total.available_permits(),
            SIGNALS_MAX_CONNECTIONS - SIGNALS_LOCAL_RESERVE - SIGNALS_MAX_HANDSHAKES,
            "the gateway keeps its reserve, and handshakes their share"
        );
    }

    #[test]
    fn the_connection_cap_grows_with_the_declared_members_up_to_its_ceiling() {
        let members = operator::signals::PeerIdentities::new();
        let mut admission = Admission::new(4, 8, members.clone());
        assert_eq!(admission.capacity, SIGNALS_MAX_CONNECTIONS, "the floor with no members");
        let declare = |count: usize| {
            members.set(
                (0..count)
                    .map(|n| (format!("site-{n}"), operator::signals::PeerRecord::default()))
                    .collect(),
            );
        };
        declare(100);
        let held = admission.admit(([127, 0, 0, 1], 1).into()).expect("admitted");
        let wanted = 100 * 4 + SIGNALS_LOCAL_RESERVE + SIGNALS_MAX_HANDSHAKES;
        assert_eq!(admission.capacity, wanted, "every member's share fits");
        assert_eq!(
            admission.peers.total.available_permits(),
            wanted - SIGNALS_LOCAL_RESERVE - SIGNALS_MAX_HANDSHAKES,
            "peers grow with the cap and keep off the reserve"
        );
        drop(held);
        declare(10);
        admission.size_for_members();
        assert_eq!(admission.capacity, wanted, "never shrunk under a held slot");
        declare(10_000);
        admission.size_for_members();
        assert_eq!(
            admission.capacity, SIGNALS_CONNECTIONS_CEILING,
            "bounded by the ceiling"
        );
    }

    #[test]
    fn handshakes_never_take_the_slots_authenticated_connections_need() {
        let mut admission = Admission::new(4, 64, operator::signals::PeerIdentities::new());
        let held: Vec<_> = (0..=u8::MAX)
            .filter_map(|n| admission.admit(([10, 0, 1, n], 1).into()))
            .collect();
        assert_eq!(held.len(), SIGNALS_MAX_HANDSHAKES, "handshakes stop at their share");
        assert_eq!(
            admission.slots.available_permits(),
            SIGNALS_MAX_CONNECTIONS - SIGNALS_MAX_HANDSHAKES,
            "the rest stay free for authenticated connections"
        );
        drop(held);
        // Through admit the quiet cap binds first, because a connection has not spoken yet
        // when it is admitted: one source gets a quarter of the share while it stays quiet.
        let one_source: Vec<_> = (0..SIGNALS_MAX_HANDSHAKES)
            .filter_map(|_| admission.admit(([10, 0, 2, 1], 1).into()))
            .collect();
        assert_eq!(
            one_source.len(),
            SIGNALS_MAX_HANDSHAKES / 8,
            "a source that has not spoken holds a quarter of the share, whatever its configured cap"
        );
        drop(one_source);
        // The handshake cap itself is still the whole share, which is what a peer set behind
        // one router needs once its connections speak and give their quiet slots back.
        let speaking: Vec<_> = (0..SIGNALS_MAX_HANDSHAKES)
            .filter_map(|_| admission.sources.acquire(std::net::IpAddr::from([10, 0, 2, 1])))
            .collect();
        assert_eq!(
            speaking.len(),
            SIGNALS_MAX_HANDSHAKES / 2,
            "one source holds at most half the handshakes, whatever its configured cap"
        );
    }

    /// Serve a small successful response on an in-memory HTTP/1 connection.
    async fn serve_ok_connection(
        server_io: tokio::io::DuplexStream,
        shutdown: operator::shutdown::Shutdown,
    ) -> hyper::Result<()> {
        let service = hyper::service::service_fn(|_request: http::Request<hyper::body::Incoming>| {
            std::future::ready(Ok::<_, std::convert::Infallible>(http::Response::new(
                http_body_util::Full::new(bytes::Bytes::from_static(b"ok")),
            )))
        });
        let connection = hyper::server::conn::http1::Builder::new()
            .timer(hyper_util::rt::TokioTimer::new())
            .header_read_timeout(SIGNALS_HEADER_READ_TIMEOUT)
            .serve_connection(hyper_util::rt::TokioIo::new(server_io), service);
        Box::pin(serve_until_aged(connection, SIGNALS_MAX_CONNECTION_AGE, shutdown)).await
    }

    /// Serve one response whose completion the test controls.
    async fn serve_gated_connection(
        server_io: tokio::io::DuplexStream,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        shutdown: operator::shutdown::Shutdown,
    ) -> hyper::Result<()> {
        let service = hyper::service::service_fn(move |_request: http::Request<hyper::body::Incoming>| {
            let (entered, release) = (Arc::clone(&entered), Arc::clone(&release));
            async move {
                entered.notify_one();
                release.notified().await;
                Ok::<_, std::convert::Infallible>(http::Response::new(http_body_util::Full::new(
                    bytes::Bytes::from_static(b"finished"),
                )))
            }
        });
        let connection = hyper::server::conn::http1::Builder::new()
            .serve_connection(hyper_util::rt::TokioIo::new(server_io), service);
        Box::pin(serve_until_aged(connection, SIGNALS_MAX_CONNECTION_AGE, shutdown)).await
    }

    /// Assert an async reader reaches EOF cleanly.
    async fn expect_eof(stream: &mut (impl tokio::io::AsyncRead + Unpin)) {
        use tokio::io::AsyncReadExt as _;
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await.expect("closed connection");
    }

    /// A caller that keeps asking is closed once the connection reaches its maximum age.
    #[tokio::test(start_paused = true)]
    async fn a_busy_connection_closes_at_its_maximum_age() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let (mut client, server_io) = tokio::io::duplex(4096);
        let server = tokio::spawn(serve_ok_connection(server_io, operator::shutdown::Shutdown::never()));
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

    /// Shutdown closes an idle keep-alive without waiting for its header timer or maximum age.
    #[tokio::test(start_paused = true)]
    async fn an_idle_connection_closes_promptly_on_shutdown() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let (mut client, server_io) = tokio::io::duplex(4096);
        let (trigger, shutdown) = operator::shutdown::Trigger::new();
        let server = tokio::spawn(serve_ok_connection(server_io, shutdown));

        client
            .write_all(b"GET / HTTP/1.1\r\nHost: east\r\n\r\n")
            .await
            .expect("request");
        let mut answer = vec![0_u8; 256];
        let read = client.read(&mut answer).await.expect("response");
        answer.truncate(read);
        assert!(answer.starts_with(b"HTTP/1.1 200"), "request completed");

        let stopped = tokio::time::Instant::now();
        trigger.trigger();
        assert!(server.await.expect("server task").is_ok(), "a graceful close");
        assert!(
            stopped.elapsed() < SIGNALS_HEADER_READ_TIMEOUT,
            "shutdown must beat the idle header timer"
        );
        expect_eof(&mut client).await;
    }

    /// Graceful shutdown lets the request already being answered finish before closing.
    #[tokio::test(start_paused = true)]
    async fn shutdown_finishes_the_in_flight_response() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (mut client, server_io) = tokio::io::duplex(4096);
        let (trigger, shutdown) = operator::shutdown::Trigger::new();
        let server = tokio::spawn(serve_gated_connection(
            server_io,
            Arc::clone(&entered),
            Arc::clone(&release),
            shutdown,
        ));

        client
            .write_all(b"GET / HTTP/1.1\r\nHost: east\r\n\r\n")
            .await
            .expect("request");
        entered.notified().await;
        trigger.trigger();
        assert!(
            !server.is_finished(),
            "the in-flight response holds graceful shutdown open"
        );

        release.notify_one();
        let mut answer = Vec::new();
        tokio::time::timeout(SIGNALS_WRITE_TIMEOUT, client.read_to_end(&mut answer))
            .await
            .expect("connection close bound")
            .expect("response and EOF");
        assert!(answer.starts_with(b"HTTP/1.1 200"), "the response completed");
        assert!(answer.ends_with(b"finished"), "the complete body was written");
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
                |name| serde_json::json!({"swimKeySecretRef": {"name": name, "namespace": "grid"}}),
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
        assert!(Readiness::default().status().0 == http::StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn the_first_write_waits_for_every_live_named_peer_to_deliver() {
        let member = |site: &str, status| operator::swim::MemberRecord {
            site_id: site.to_owned(),
            endpoint: "10.0.0.2:7946".to_owned(),
            incarnation: 0,
            status,
            age_secs: 0,
            gateway_address: None,
            site_cert_pem: None,
            signals_address: None,
        };
        let (alive, dead) = (operator::swim::MemberStatus::Alive, operator::swim::MemberStatus::Dead);
        let identities = operator::signals::PeerIdentities::new();
        let record = operator::signals::PeerRecord {
            labels: BTreeMap::new(),
            pins: vec!["aa".to_owned()],
            signals: None,
        };
        let heard = operator::signals::LoadHeard::default();
        let nothing = crdt::GridStateSnapshot::new("hub".to_owned());
        let east_alone = operator::swim::MembershipSnapshot {
            members: vec![member("east", alive.clone())],
        };
        assert!(
            !peers_delivered(&east_alone, &nothing, &identities, &heard, "hub"),
            "no GridSite read yet, so no peer is known not to need waiting on"
        );
        identities.set(BTreeMap::from([
            ("east".to_owned(), record.clone()),
            ("west".to_owned(), record),
        ]));
        let mut state = crdt::GridStateSnapshot::new("hub".to_owned());
        let members = |list| operator::swim::MembershipSnapshot { members: list };
        let delivered = |snapshot: &operator::swim::MembershipSnapshot, held: &crdt::GridStateSnapshot| {
            peers_delivered(snapshot, held, &identities, &heard, "hub")
        };

        assert!(
            !delivered(&members(vec![member("hub", alive.clone())]), &state),
            "no peer yet"
        );
        let east_and_west = members(vec![member("east", alive.clone()), member("west", dead)]);
        assert!(!delivered(&east_and_west, &state), "east is live and has said nothing");
        state.upsert_provider(crdt::ProviderState {
            network_id: "net".to_owned(),
            site_id: "east".to_owned(),
            provider_id: "p".to_owned(),
            routing_cluster: "east".to_owned(),
            models: Vec::new(),
            tools: Vec::new(),
            backend_kind: "local".to_owned(),
            capacity_weight: 1,
            phase: crdt::ProviderPhase::Available,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            access_policy: crdt::ProviderAccessPolicy::default(),
            revision: 1,
            writer_id: "east".to_owned(),
        });
        assert!(!delivered(&east_and_west, &state), "east has sent no load yet");
        let row = operator::signals::Observation {
            metric: "inference_pool_average_queue_size".to_owned(),
            labels: BTreeMap::from([("grid_provider".to_owned(), "p".to_owned())]),
            value: 0.0,
            timestamp_ms: None,
            age: std::time::Duration::ZERO,
        };
        heard.record("east", &[row]);
        assert!(delivered(&east_and_west, &state), "a dead peer is not waited on");
        let stranger = members(vec![member("east", alive.clone()), member("stranger", alive)]);
        assert!(delivered(&stranger, &state), "nor one no GridSite names");
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

    #[tokio::test(start_paused = true)]
    async fn rounds_are_spread_apart_and_an_overrun_round_is_followed_at_once() {
        let interval = std::time::Duration::from_secs(10);
        let started = tokio::time::Instant::now();
        let next = round_after(started, interval);
        assert!(
            (interval..=interval + interval / 10).contains(&(next - started)),
            "an interval later, spread by up to a tenth: {:?}",
            next - started
        );
        tokio::time::advance(interval * 3).await;
        assert_eq!(
            round_after(started, interval),
            tokio::time::Instant::now(),
            "a round that overran is followed now, not by the rounds it missed"
        );
    }

    #[test]
    fn handshake_slots_cap_a_source_and_a_global_48_by_refusing_the_newest() {
        let slots = SourceSlots::new(1, 2);
        let ip = |text: &str| text.parse::<std::net::IpAddr>().expect("ip");
        let first = slots.acquire(ip("2001:db8:1:1::1")).expect("first");
        assert!(
            slots.acquire(ip("2001:db8:1:1::ffff")).is_none(),
            "same /64: the newest is refused"
        );
        assert_eq!(
            slots.held(ip("2001:db8:1:1::1")),
            1,
            "the handshake in progress is kept"
        );
        let second = slots.acquire(ip("2001:db8:1:2::1")).expect("another /64");
        assert!(
            slots.acquire(ip("2001:db8:1:3::1")).is_none(),
            "the /48 is full: the newest is refused"
        );
        assert!(slots.acquire(ip("2001:db8:2:1::1")).is_some(), "another /48");
        drop(first);
        assert_eq!(
            slots.held(ip("2001:db8:1:1::1")),
            0,
            "a finished handshake frees its slot"
        );
        assert!(slots.acquire(ip("2001:db8:1:3::1")).is_some(), "the /48 has room again");
        drop(second);
        assert!(
            SourceSlots::new(0, 0).acquire(ip("fd00::1")).is_none(),
            "a cap of zero admits nothing"
        );
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
    fn peers_are_polled_only_when_this_site_would_name_them() {
        let member = |site: &str, endpoint: &str| operator::swim::MemberRecord {
            site_id: site.to_owned(),
            endpoint: endpoint.to_owned(),
            incarnation: 0,
            status: operator::swim::MemberStatus::Alive,
            age_secs: 0,
            gateway_address: None,
            site_cert_pem: None,
            signals_address: Some(endpoint.replace(":7946", ":9443")),
        };
        let snapshot = operator::swim::MembershipSnapshot {
            members: vec![
                member("hub", "10.0.0.1:7946"),
                member("east", "10.0.0.2:7946"),
                member("west", "10.0.0.3:7946"),
                member("stranger", "10.0.0.4:7946"),
                operator::swim::MemberRecord {
                    signals_address: None,
                    ..member("mute", "10.0.0.5:7946")
                },
            ],
        };
        let identities = operator::signals::PeerIdentities::new();
        identities.set(BTreeMap::from([
            (
                "mute".to_owned(),
                operator::signals::PeerRecord {
                    labels: gpu(),
                    pins: vec!["bb".to_owned()],
                    signals: None,
                },
            ),
            (
                "east".to_owned(),
                operator::signals::PeerRecord {
                    labels: gpu(),
                    pins: vec!["aa".to_owned()],
                    signals: None,
                },
            ),
            (
                "west".to_owned(),
                operator::signals::PeerRecord {
                    labels: gpu(),
                    pins: Vec::new(),
                    signals: None,
                },
            ),
        ]));
        let names = |trust, encrypted| {
            Targeting {
                identities: &identities,
                trust,
                encrypted,
                local_site: "hub",
            }
            .targets(Some(&snapshot), &mut BTreeMap::new())
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
    fn a_declared_endpoint_is_polled_without_gossip_and_over_a_gossiped_address() {
        let declared = |address: &str, server_name: Option<&str>, pins: Vec<String>| operator::signals::PeerRecord {
            labels: BTreeMap::new(),
            pins,
            signals: Some(operator::signals::DeclaredSignals {
                endpoint: operator::signals::SignalsEndpoint::parse(address).expect("endpoint"),
                server_name: server_name.map(str::to_owned),
            }),
        };
        let pinned = || vec!["aa".to_owned()];
        let identities = operator::signals::PeerIdentities::new();
        identities.set(BTreeMap::from([
            (
                "east".to_owned(),
                declared("router.example.com:443", Some("east-signals.grid.internal"), pinned()),
            ),
            ("west".to_owned(), declared("10.0.0.3:9443", None, pinned())),
            ("hub".to_owned(), declared("10.0.0.1:9443", None, pinned())),
            ("unpinned".to_owned(), declared("10.0.0.4:9443", None, Vec::new())),
        ]));
        let mut remembered = BTreeMap::new();
        let round = |snapshot: Option<&operator::swim::MembershipSnapshot>, memory: &mut BTreeMap<_, _>| {
            Targeting {
                identities: &identities,
                trust: operator::signals::PeerTrustMode::Pin,
                encrypted: false,
                local_site: "hub",
            }
            .targets(snapshot, memory)
            .map(|sites| {
                sites
                    .into_iter()
                    .map(|site| (site.name.clone(), site))
                    .collect::<BTreeMap<_, _>>()
            })
        };
        let sites = round(None, &mut remembered).expect("declared peers are polled without SWIM");
        assert_eq!(
            sites.keys().collect::<Vec<_>>(),
            ["east", "west"],
            "not this site, not an unpinned one"
        );
        let east = sites.get("east").expect("east");
        assert_eq!(east.url, "https://east-signals.grid.internal:443/v1/site/signals");
        assert_eq!(east.alias(), Some(("east-signals.grid.internal", "router.example.com")));
        assert_eq!(east.pins, pinned());
        let west = sites.get("west").expect("west");
        assert_eq!(west.url, "https://10.0.0.3:9443/v1/site/signals");
        assert_eq!(west.alias(), None);

        let snapshot = operator::swim::MembershipSnapshot {
            members: vec![operator::swim::MemberRecord {
                site_id: "west".to_owned(),
                endpoint: "10.0.0.9:7946".to_owned(),
                incarnation: 0,
                status: operator::swim::MemberStatus::Alive,
                age_secs: 0,
                gateway_address: None,
                site_cert_pem: None,
                signals_address: Some("10.0.0.9:9443".to_owned()),
            }],
        };
        let with_gossip = round(Some(&snapshot), &mut remembered).expect("peers");
        assert_eq!(
            with_gossip.get("west").map(|site| site.url.as_str()),
            Some("https://10.0.0.3:9443/v1/site/signals"),
            "a declared address wins over a gossiped one"
        );

        // SPIFFE trust waits for encrypted gossip before reading gossiped addresses; a declared
        // address is admitted by the local object, so it is polled over plaintext gossip or none.
        let spiffe = Targeting {
            identities: &identities,
            trust: operator::signals::PeerTrustMode::Spiffe,
            encrypted: false,
            local_site: "hub",
        };
        let declared_only = spiffe
            .targets(Some(&snapshot), &mut BTreeMap::new())
            .expect("declared peers are polled under SPIFFE without encrypted gossip");
        assert_eq!(
            declared_only.iter().map(|site| site.name.as_str()).collect::<Vec<_>>(),
            ["east", "unpinned", "west"],
            "every declared peer with a record, pins or not, and nothing from gossip"
        );

        identities.set(BTreeMap::new());
        assert!(
            round(None, &mut remembered).is_none(),
            "nothing declared and no gossip is nothing to poll"
        );
    }

    #[test]
    fn a_failed_network_write_is_retried_though_the_next_round_judges_the_same() {
        assert!(write_due(None, &1), "the last write did not land");
        assert!(write_due(Some(&1), &2), "the verdict changed");
        assert!(!write_due(Some(&1), &1), "unchanged and written");
    }

    #[test]
    fn a_local_round_with_only_endpoint_less_peers_still_reports_them() {
        let identities = operator::signals::PeerIdentities::new();
        identities.set(BTreeMap::from([(
            "east".to_owned(),
            operator::signals::PeerRecord::default(),
        )]));
        let targets = Targeting {
            identities: &identities,
            trust: operator::signals::PeerTrustMode::Spiffe,
            encrypted: true,
            local_site: "hub",
        }
        .targets(None, &mut BTreeMap::new());
        assert!(targets.is_none(), "nothing to poll");
        assert!(
            idle_round(Membership::Local, identities.loaded()),
            "yet the round runs, so east reads EgressOnly and a removed endpoint is cleared"
        );
        assert!(
            !idle_round(Membership::Local, operator::signals::PeerIdentities::new().loaded()),
            "before the peers are read, the round waits for them"
        );
        assert!(
            !idle_round(Membership::Swim, true),
            "under Swim gossip drives the round"
        );
    }

    #[test]
    fn this_site_never_polls_itself_though_its_own_grid_site_declares_an_endpoint() {
        let declared = |host: &str| operator::signals::PeerRecord {
            labels: BTreeMap::new(),
            pins: vec!["aa".to_owned()],
            signals: operator::signals::SignalsEndpoint::parse(&format!("{host}:9443")).map(|endpoint| {
                operator::signals::DeclaredSignals {
                    endpoint,
                    server_name: None,
                }
            }),
        };
        let identities = operator::signals::PeerIdentities::new();
        identities.set(BTreeMap::from([
            ("hub".to_owned(), declared("hub.example.com")),
            ("east".to_owned(), declared("east.example.com")),
        ]));
        let gossip = |site: &str| operator::swim::MemberRecord {
            site_id: site.to_owned(),
            endpoint: format!("{site}.example.com:7946"),
            incarnation: 0,
            status: operator::swim::MemberStatus::Alive,
            age_secs: 0,
            gateway_address: None,
            site_cert_pem: None,
            signals_address: Some(format!("{site}.example.com:9443")),
        };
        let snapshot = operator::swim::MembershipSnapshot {
            members: vec![gossip("hub"), gossip("east")],
        };
        let polled: Vec<String> = Targeting {
            identities: &identities,
            trust: operator::signals::PeerTrustMode::Pin,
            encrypted: false,
            local_site: "hub",
        }
        .targets(Some(&snapshot), &mut BTreeMap::new())
        .unwrap_or_default()
        .into_iter()
        .map(|site| site.name)
        .collect();
        assert_eq!(polled, ["east"], "declared and gossiped alike, the own site is skipped");
    }

    #[test]
    fn a_peer_swim_lost_is_still_polled_while_its_grid_site_names_it() {
        let member = |site: &str, endpoint: &str, status| operator::swim::MemberRecord {
            site_id: site.to_owned(),
            endpoint: endpoint.to_owned(),
            incarnation: 0,
            status,
            age_secs: 0,
            gateway_address: None,
            site_cert_pem: None,
            signals_address: Some(endpoint.replace(":7946", ":9443")),
        };
        let alive = || member("east", "10.0.0.2:7946", operator::swim::MemberStatus::Alive);
        let dead = || member("west", "10.0.0.3:7946", operator::swim::MemberStatus::Dead);
        let record = || operator::signals::PeerRecord {
            labels: BTreeMap::new(),
            pins: vec!["aa".to_owned()],
            signals: None,
        };
        let identities = operator::signals::PeerIdentities::new();
        identities.set(BTreeMap::from([
            ("east".to_owned(), record()),
            ("west".to_owned(), record()),
        ]));
        let mut remembered = BTreeMap::new();
        let mut round = |members| {
            let snapshot = operator::swim::MembershipSnapshot { members };
            Targeting {
                identities: &identities,
                trust: operator::signals::PeerTrustMode::Pin,
                encrypted: false,
                local_site: "hub",
            }
            .targets(Some(&snapshot), &mut remembered)
            .unwrap_or_default()
            .into_iter()
            .map(|site| (site.name, site.url))
            .collect::<Vec<_>>()
        };
        let east = ("east".to_owned(), "https://10.0.0.2:9443/v1/site/signals".to_owned());
        let west = ("west".to_owned(), "https://10.0.0.3:9443/v1/site/signals".to_owned());

        assert_eq!(
            round(vec![dead(), alive()]),
            [east.clone(), west.clone()],
            "a dead member is polled"
        );
        assert_eq!(
            round(vec![alive()]),
            [east.clone(), west],
            "a member SWIM dropped is polled where it last was"
        );
        identities.set(BTreeMap::from([("east".to_owned(), record())]));
        assert_eq!(
            round(vec![alive()]),
            [east],
            "a site whose GridSite is gone is forgotten"
        );
    }

    #[test]
    fn one_peer_that_did_not_answer_leads_each_round_in_turn() {
        let site = |name: &str| operator::signals::PeerSite {
            name: name.to_owned(),
            url: format!("https://{name}:9443/v1/site/signals"),
            pins: Vec::new(),
            answering: true,
            alias: None,
        };
        let unanswered: BTreeSet<String> = ["a", "b", "c"].iter().map(|name| (*name).to_owned()).collect();
        let order = |turn| {
            let mut sites: Vec<_> = ["a", "b", "c", "x", "y"].iter().map(|name| site(name)).collect();
            order_targets(&mut sites, &unanswered, turn);
            sites.into_iter().map(|target| target.name).collect::<Vec<_>>()
        };
        assert_eq!(order(0), ["a", "x", "y", "b", "c"]);
        assert_eq!(order(1), ["b", "y", "x", "c", "a"]);
        assert_eq!(order(2), ["c", "x", "y", "a", "b"]);
    }

    #[test]
    fn a_round_too_small_for_every_peer_still_reaches_each_in_turn() {
        // Four peers that each fill a slot, a round with room for two: whoever is dialled answers.
        let names = ["a", "b", "c", "d"];
        let mut unanswered = BTreeSet::new();
        let mut reached = BTreeSet::new();
        for turn in 1..=6 {
            let mut sites: Vec<_> = names
                .iter()
                .map(|name| operator::signals::PeerSite {
                    name: (*name).to_owned(),
                    url: format!("https://{name}:9443/v1/site/signals"),
                    pins: Vec::new(),
                    answering: true,
                    alias: None,
                })
                .collect();
            order_targets(&mut sites, &unanswered, turn);
            let (dialled, cut) = sites.split_at(2);
            reached.extend(dialled.iter().map(|target| target.name.clone()));
            unanswered = cut.iter().map(|target| target.name.clone()).collect();
        }
        assert_eq!(reached.len(), names.len(), "every peer was dialled: {reached:?}");
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

    /// A listener fixture serving `mesh`'s CA with an east leaf, admitting west.
    #[cfg(not(feature = "fips"))]
    fn serving_for(mesh: &Mesh) -> Serving {
        let leaf = certs::generate_site_cert(&mesh.ca, "east").expect("fixture");
        let peers = operator::signals::PeerIdentities::new();
        peers.set(BTreeMap::from([(
            "west".to_owned(),
            operator::signals::PeerRecord {
                labels: gpu(),
                pins: Vec::new(),
                signals: None,
            },
        )]));
        Serving {
            tls: tls_backend::build_server_config(
                mesh.ca.cert_pem.as_bytes(),
                leaf.cert_pem.as_bytes(),
                leaf.key_pem.as_bytes(),
            )
            .expect("fixture"),
            app: ok_app(),
            identity: SignalsIdentity {
                peers,
                own: None,
                trust: operator::signals::PeerTrustMode::Spiffe,
            },
        }
    }

    /// The drain holds for a peer mid-answer, and gives up at the grace.
    #[tokio::test(start_paused = true)]
    async fn the_drain_waits_for_an_open_connection_and_then_gives_up() {
        let slots = Arc::new(tokio::sync::Semaphore::new(SIGNALS_MAX_CONNECTIONS));
        let held = Arc::clone(&slots).try_acquire_owned().expect("a slot");

        let waiting = tokio::time::Instant::now();
        drain_connections(&slots, SIGNALS_MAX_CONNECTIONS).await;
        assert!(
            waiting.elapsed() >= SHUTDOWN_GRACE,
            "a peer mid-answer must hold the drain open"
        );

        drop(held);
        let idle = tokio::time::Instant::now();
        drain_connections(&slots, SIGNALS_MAX_CONNECTIONS).await;
        assert!(
            idle.elapsed() < SHUTDOWN_GRACE,
            "with nothing open the drain must not wait out the grace"
        );
    }

    /// Stand-down answers not ready before any worker sees the signal, and a startup that
    /// finishes later cannot make it ready again.
    #[tokio::test]
    async fn stand_down_answers_not_ready_before_workers_stop() {
        let ready = Arc::new(Readiness::default());
        ready.mark_started();
        assert_eq!(ready.status().0, http::StatusCode::OK);
        let (trigger, shutdown) = operator::shutdown::Trigger::new();
        let observed = Arc::clone(&ready);
        let worker = tokio::spawn(async move {
            shutdown.triggered().await;
            observed.status()
        });
        begin_stand_down(&trigger, &ready);
        let seen = tokio::time::timeout(std::time::Duration::from_secs(5), worker)
            .await
            .expect("the worker saw the signal")
            .expect("worker task");
        assert_eq!(seen, (http::StatusCode::SERVICE_UNAVAILABLE, "stopping"));
        ready.mark_started();
        assert_eq!(
            ready.status().0,
            http::StatusCode::SERVICE_UNAVAILABLE,
            "stays stopping"
        );
    }

    /// The SWIM restart stands down as a SIGTERM does, and only once the address appears.
    #[tokio::test]
    async fn the_swim_restart_stands_down_in_order() {
        let ready = Arc::new(Readiness::default());
        ready.mark_started();
        let (trigger, shutdown) = operator::shutdown::Trigger::new();
        let stand_down = StandDown {
            trigger: Arc::new(trigger),
            ready: Arc::clone(&ready),
            routes: HolderRouting::default(),
        };
        restart_when(std::future::ready(false), stand_down.clone()).await;
        assert_eq!(ready.status().0, http::StatusCode::OK, "no address, no restart");
        assert!(!shutdown.is_triggered(), "no address, no restart");
        restart_when(std::future::ready(true), stand_down).await;
        assert_eq!(ready.status(), (http::StatusCode::SERVICE_UNAVAILABLE, "stopping"));
        assert!(shutdown.is_triggered(), "the workers are asked to stop");
    }

    /// While an answer still holds the drain open, a new connection is refused at once rather
    /// than left in the backlog.
    #[cfg(not(feature = "fips"))]
    #[tokio::test]
    async fn the_drain_refuses_new_connections_at_once() {
        let mesh = Mesh::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("fixture");
        let addr = listener.local_addr().expect("fixture");
        let (trigger, shutdown) = operator::shutdown::Trigger::new();
        let admission = Admission::new(8, 8, operator::signals::PeerIdentities::new());
        // Stands in for a connection mid-answer, which holds the drain.
        let answering = Arc::clone(&admission.slots).acquire_owned().await.expect("slot");
        let server = tokio::spawn(serve_signals_tls(
            listener,
            serving_for(&mesh),
            admission,
            std::future::pending(),
            shutdown,
        ));
        let mut polling = mesh.connect(addr).await;
        assert!(get(&mut polling).await.starts_with(b"HTTP/1.1 200"), "serves before");

        trigger.trigger();
        let refused = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while tokio::net::TcpStream::connect(addr).await.is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(refused.is_ok(), "a new connection is refused within 2s of the signal");
        assert!(!server.is_finished(), "refused while an answer still holds the drain");

        drop(answering);
        tokio::time::timeout(SHUTDOWN_GRACE, server)
            .await
            .expect("the drain ends once the answer does")
            .expect("server task")
            .expect("the listener stands down cleanly");
    }

    /// The signal closes the listener, where before only SIGKILL did.
    #[cfg(not(feature = "fips"))]
    #[tokio::test]
    async fn the_listener_stops_accepting_once_the_signal_is_given() {
        let mesh = Mesh::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("fixture");
        let addr = listener.local_addr().expect("fixture");
        let (trigger, shutdown) = operator::shutdown::Trigger::new();
        let server = tokio::spawn(serve_signals_tls(
            listener,
            serving_for(&mesh),
            Admission::new(8, 8, operator::signals::PeerIdentities::new()),
            std::future::pending(),
            shutdown,
        ));

        let mut polling = mesh.connect(addr).await;
        assert!(get(&mut polling).await.starts_with(b"HTTP/1.1 200"), "serves before");

        let started = tokio::time::Instant::now();
        trigger.trigger();
        server
            .await
            .expect("server task")
            .expect("the listener stands down cleanly");
        assert!(
            started.elapsed() < SHUTDOWN_GRACE,
            "an idle connection releases its drain slot"
        );

        expect_eof(&mut polling).await;

        assert!(
            tokio::net::TcpStream::connect(addr).await.is_err(),
            "the listener socket must be closed once the signal is given"
        );
    }

    /// A stage with no handle has nothing to leave, at either read.
    #[test]
    fn a_site_with_no_swim_runtime_has_no_handle_to_leave() {
        let (tx, rx) = tokio::sync::watch::channel(SwimStage::Starting);
        assert!(settled(&rx).is_none(), "starting carries no handle");
        tx.send_replace(SwimStage::Settled(None));
        assert!(settled(&rx).is_none(), "settled without a runtime carries no handle");
    }

    /// An apiserver that routes one holder slice, then never answers again.
    fn routes_then_hangs() -> Arc<operator::holder_route::HolderRoutes> {
        let answered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let seen = answered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                let respond = |code: u16, body: serde_json::Value| {
                    http::Response::builder()
                        .status(code)
                        .body(kube::client::Body::from(body.to_string().into_bytes()))
                        .expect("response")
                };
                match seen {
                    0 => Ok::<_, std::convert::Infallible>(respond(
                        404,
                        serde_json::json!({"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}),
                    )),
                    1 => Ok(respond(
                        200,
                        serde_json::json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"signals","namespace":"grid","uid":"u"},"spec":{"ports":[{"name":"signals","port":9091}]}}),
                    )),
                    2 => {
                        drop(request);
                        Ok(respond(
                            200,
                            serde_json::json!({"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","metadata":{"name":"signals-holder","resourceVersion":"1"},"addressType":"IPv4","endpoints":[]}),
                        ))
                    },
                    _ => std::future::pending().await,
                }
            }
        });
        let me = operator::holder_route::Me {
            address: "10.0.0.1".parse().expect("address"),
            pod: "a".to_owned(),
            node: None,
        };
        Arc::new(operator::holder_route::HolderRoutes::new(
            &Client::new(service, "grid"),
            vec!["signals".to_owned()],
            me,
            BTreeMap::new(),
        ))
    }

    /// Having lost the lease, every writer is told to stop before any slice write, so one that
    /// hangs cannot keep a writer running past the moment a successor may take over.
    #[tokio::test(start_paused = true)]
    async fn a_lost_lease_stops_the_writers_before_routing_away() {
        let routes = routes_then_hangs();
        let routing = routes.route_when(std::future::ready("test"), operator::leader::Holding::unelected());
        drop(tokio::time::timeout(std::time::Duration::from_millis(50), routing).await);
        let (trigger, shutdown) = operator::shutdown::Trigger::new();
        let ready = Arc::new(Readiness::default());
        ready.mark_started();
        let stand_down = StandDown {
            trigger: Arc::new(trigger),
            ready: Arc::clone(&ready),
            routes: HolderRouting {
                signals: Some(routes),
                swim: None,
            },
        };
        let standing_down = tokio::spawn(stand_down.begin_lost());
        let stopped = tokio::time::timeout(std::time::Duration::from_millis(10), shutdown.triggered()).await;
        assert!(stopped.is_ok(), "the writers are told to stop at once");
        assert_eq!(ready.status().0, http::StatusCode::SERVICE_UNAVAILABLE);
        assert!(!standing_down.is_finished(), "while the slice write still hangs");
        assert!(
            operator::leader::RENEW_DEADLINE < operator::leader::LEASE_DURATION,
            "a lost holder stops before a candidate could take the lease"
        );
        standing_down.abort();
    }

    /// An identity renewal under way holds the lease until it finishes, whether the stand-down
    /// began on a signal or on a lost lease.
    #[tokio::test(start_paused = true)]
    async fn a_renewal_under_way_holds_the_lease_release_on_either_path() {
        for lost in [false, true] {
            let (trigger, shutdown) = operator::shutdown::Trigger::new();
            let (writers_done_tx, writers_done) = operator::shutdown::Trigger::new();
            let (renewal_done_tx, renewal_done) = operator::shutdown::Trigger::new();
            let (_swim_tx, swim) = tokio::sync::watch::channel(SwimStage::Settled(None));
            let (left_tx, left) = operator::shutdown::Trigger::new();
            // The renewal loop with a Secret write in flight: it returns only once the write lands.
            let (write_lands, landed) = tokio::sync::oneshot::channel::<()>();
            let renewal_stop = shutdown.clone();
            let renewal = tokio::spawn(async move {
                renewal_stop.triggered().await;
                drop(landed.await);
                drop(renewal_done_tx);
            });
            let standing_down = tokio::spawn(stand_down(shutdown, (writers_done, renewal_done), swim, left_tx));
            let begin = StandDown {
                trigger: Arc::new(trigger),
                ready: Arc::new(Readiness::default()),
                routes: HolderRouting::default(),
            };
            if lost {
                begin.begin_lost().await;
            } else {
                begin.begin().await;
            }
            drop(writers_done_tx);
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            assert!(
                !left.is_triggered(),
                "lost={lost}: the lease is held while the renewal writes"
            );
            let _sent = write_lands.send(());
            renewal.await.expect("renewal task");
            tokio::task::yield_now().await;
            assert!(left.is_triggered(), "lost={lost}: released once the write has landed");
            tokio::time::advance(SHUTDOWN_GRACE).await;
            standing_down.await.expect("stand-down task");
        }
    }

    /// A start that may succeed on another try is retried; a pending address or a bad setting is not.
    #[test]
    fn only_a_swim_start_that_may_succeed_again_is_retried() {
        for reason in ["AdvertiseAddressUnresolved", "RevisionLeaseFailed", "StartFailed"] {
            assert!(SwimFailure::new(reason, "x").retried(), "{reason}");
        }
        for reason in [
            "AdvertiseAddressPending",
            "AdvertiseAddressUnspecified",
            "BindAddressInvalid",
            "KeyInvalid",
            "MembershipConflict",
        ] {
            assert!(!SwimFailure::new(reason, "x").retried(), "{reason}");
        }
        assert_eq!(
            swim_cleared("grid").pointer("/status/conditions"),
            Some(&serde_json::json!([])),
            "clearing applies no conditions under the Swim manager"
        );
    }

    /// SWIM that cannot start reads False with its cause, and running SWIM reads True.
    #[test]
    fn the_swim_condition_names_its_cause() {
        let pending = SwimFailure::new("AdvertiseAddressPending", "no SWIM advertise address within 180s");
        let down = swim_condition(Some(&pending), "t", Some(2));
        assert_eq!(
            (down.type_.as_str(), down.status.as_str(), down.reason.as_str()),
            ("Swim", "False", "AdvertiseAddressPending")
        );
        assert!(down.message.contains("no SWIM advertise address"), "{}", down.message);
        assert_eq!(down.observed_generation, Some(2));
        let up = swim_condition(None, "t", None);
        assert_eq!(
            (up.status.as_str(), up.reason.as_str()),
            ("True", "Swim"),
            "True names the type"
        );
    }

    /// Stand-down cannot start its leave grace until every state writer has stopped.
    #[tokio::test(start_paused = true)]
    async fn stand_down_waits_for_writers_before_the_grace() {
        let (shutdown_tx, shutdown) = operator::shutdown::Trigger::new();
        let (writers_done_tx, writers_done) = operator::shutdown::Trigger::new();
        let (_swim_tx, swim) = tokio::sync::watch::channel(SwimStage::Settled(None));
        let (left_tx, left) = operator::shutdown::Trigger::new();
        let (_, no_renewal) = operator::shutdown::Trigger::new();
        let standing_down = tokio::spawn(stand_down(shutdown, (writers_done, no_renewal), swim, left_tx));

        shutdown_tx.trigger();
        tokio::time::advance(SHUTDOWN_GRACE * 2).await;
        tokio::task::yield_now().await;
        assert!(!standing_down.is_finished(), "writers hold the leave barrier");
        assert!(!left.is_triggered(), "the lease stays while a writer may run");

        writers_done_tx.trigger();
        tokio::task::yield_now().await;
        assert!(!standing_down.is_finished(), "the grace begins only after writers stop");
        assert!(left.is_triggered(), "the lease goes before the grace");
        tokio::time::advance(SHUTDOWN_GRACE).await;
        standing_down.await.expect("stand-down task");
    }

    /// The completion latch fires only after cancellation drops the writer future.
    #[tokio::test]
    async fn writer_completion_means_the_writers_are_dropped() {
        struct MarkDropped(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for MarkDropped {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::Release);
            }
        }

        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let marker = MarkDropped(Arc::clone(&dropped));
        let writers = async move {
            let _marker = marker;
            std::future::pending::<Result<(), std::convert::Infallible>>().await
        };
        let (shutdown_tx, shutdown) = operator::shutdown::Trigger::new();
        let (done_tx, done) = operator::shutdown::Trigger::new();
        let stopping = tokio::spawn(stop_writers_on_shutdown(shutdown, done_tx, writers));

        shutdown_tx.trigger();
        done.triggered().await;
        assert!(
            dropped.load(std::sync::atomic::Ordering::Acquire),
            "the writer future drops before the completion latch fires"
        );
        stopping.await.expect("writer group").expect("stopped");
    }

    /// `Date` uses the render's wall reading to approximate message origination
    /// time, as specified by [RFC 9110 Section 6.6.1].
    ///
    /// [RFC 9110 Section 6.6.1]: https://datatracker.ietf.org/doc/html/rfc9110#section-6.6.1
    #[test]
    fn a_signals_response_is_dated_from_the_reading_it_was_rendered_at() {
        let stepped_back = UNIX_EPOCH + std::time::Duration::from_secs(1_600_000_000);
        let response = served(String::new(), std::time::Duration::from_secs(7), stepped_back);
        let header = |name: http::header::HeaderName| {
            response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        assert_eq!(
            header(http::header::DATE),
            Some(httpdate::fmt_http_date(stepped_back)),
            "the Date is the render's wall reading, not the server's cached clock"
        );
        assert_eq!(header(http::header::AGE).as_deref(), Some("7"));
    }

    /// hyper writes its cached `Date` only when a response carries none, so the
    /// render's own `Date` is the only one a reader gets.
    /// `Date` carries the origination time per [RFC 9110 Section 6.6.1]; duplicate
    /// field lines are forbidden for this non-list field per [RFC 9110 Section 5.2].
    ///
    /// [RFC 9110 Section 6.6.1]: https://datatracker.ietf.org/doc/html/rfc9110#section-6.6.1
    /// [RFC 9110 Section 5.2]: https://datatracker.ietf.org/doc/html/rfc9110#section-5.2
    #[tokio::test]
    async fn the_explicit_date_is_the_only_date_on_the_wire() {
        let stepped_back = UNIX_EPOCH + std::time::Duration::from_secs(1_600_000_000);
        let answer = served_on_the_wire(stepped_back).await;
        let dates: Vec<&str> = answer
            .lines()
            .take_while(|line| !line.is_empty())
            .filter_map(|line| line.split_once(':'))
            .filter(|(name, _)| name.eq_ignore_ascii_case("date"))
            .map(|(_, value)| value.trim())
            .collect();
        assert_eq!(
            dates,
            [httpdate::fmt_http_date(stepped_back)],
            "the render's Date replaces hyper's cached one rather than joining it"
        );
    }

    /// The raw HTTP/1.1 answer hyper writes for an empty exposition rendered at `wall`.
    async fn served_on_the_wire(wall: SystemTime) -> String {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let (mut client, server_io) = tokio::io::duplex(4096);
        let service = hyper::service::service_fn(move |_request: http::Request<hyper::body::Incoming>| {
            std::future::ready(Ok::<_, std::convert::Infallible>(served(
                String::new(),
                std::time::Duration::ZERO,
                wall,
            )))
        });
        let connection = hyper::server::conn::http1::Builder::new()
            .timer(hyper_util::rt::TokioTimer::new())
            .keep_alive(false)
            .serve_connection(hyper_util::rt::TokioIo::new(server_io), service);
        let server = tokio::spawn(connection);
        client
            .write_all(b"GET /v1/site/signals HTTP/1.1\r\nHost: east\r\n\r\n")
            .await
            .expect("request");
        let mut answer = Vec::new();
        client.read_to_end(&mut answer).await.expect("response");
        assert!(
            server.await.expect("server task").is_ok(),
            "one answer, then a clean close"
        );
        String::from_utf8_lossy(&answer).into_owned()
    }

    #[tokio::test]
    async fn a_relayed_row_reads_its_own_age_against_the_response_date() {
        let client = stalled_peer_ages_client(false, Arc::new(std::sync::atomic::AtomicUsize::new(0)));
        let published = Published {
            site: operator::signals::SignalStore::new(),
            peers: operator::signals::SignalStore::new(),
            rounds: operator::signals::PeerRounds::default(),
            providers: operator::site_providers::LocalProviders::default(),
            membership: "Swim",
            feedback: operator::gateway_ages::GatewayFeedback::new(client, operator::shutdown::Shutdown::never()),
        };
        published.peers.refresh(
            BTreeMap::from([("east".to_owned(), vec![row_aged(std::time::Duration::from_secs(30))])]),
            std::time::Duration::from_secs(60),
        );
        let response = signals_handler(
            axum::extract::State(published),
            axum::Extension(Caller::Local),
            axum::extract::Query(Vec::new()),
        )
        .await;
        let date = date_ms_of(&response);
        let stamp = last_stamp_of(response).await;
        assert!(
            (29_000..=31_000).contains(&(date - stamp)),
            "Date minus the stamp is the row's 30 s age: {}",
            date - stamp
        );
    }

    /// One relayed contract row, already `age` old when collected.
    fn row_aged(age: std::time::Duration) -> operator::signals::Observation {
        operator::signals::Observation {
            metric: "inference_pool_average_queue_size".to_owned(),
            labels: BTreeMap::from([("grid_provider".to_owned(), "pool-a".to_owned())]),
            value: 1.0,
            timestamp_ms: None,
            age,
        }
    }

    /// The `Date` a response carries, epoch milliseconds.
    fn date_ms_of(response: &axum::response::Response) -> i64 {
        response
            .headers()
            .get(http::header::DATE)
            .and_then(|value| value.to_str().ok())
            .and_then(|text| httpdate::parse_http_date(text).ok())
            .and_then(|when| when.duration_since(UNIX_EPOCH).ok())
            .map(|since| i64::try_from(since.as_millis()).expect("fits i64"))
            .expect("the response carries its own Date")
    }

    /// The trailing stamp of the last row a response carries.
    async fn last_stamp_of(response: axum::response::Response) -> i64 {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        String::from_utf8_lossy(&body)
            .trim_end()
            .rsplit_once(' ')
            .and_then(|(_, stamp)| stamp.parse().ok())
            .expect("a stamped row")
    }

    /// Wait until the mock receives a request, failing instead of spinning if
    /// the expected lookup or patch was never made.
    async fn wait_for_peer_age_requests(requests: &std::sync::atomic::AtomicUsize, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while requests.load(std::sync::atomic::Ordering::SeqCst) < expected {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the intended diagnostic request is reached");
    }

    /// A Kubernetes client whose list or status patch never produces a response.
    fn stalled_peer_ages_client(stall_patch: bool, requests: Arc<std::sync::atomic::AtomicUsize>) -> Client {
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if !stall_patch || request.method() == http::Method::PATCH {
                    std::future::pending::<()>().await;
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let body = serde_json::json!({
                    "apiVersion": "grid.praxis.fast/v1alpha1",
                    "kind": "GridNetworkList",
                    "items": [{
                        "apiVersion": "grid.praxis.fast/v1alpha1",
                        "kind": "GridNetwork",
                        "metadata": { "name": "grid", "generation": 1 },
                        "spec": { "seeds": [], "gridId": "test-id" }
                    }]
                });
                Ok::<_, std::convert::Infallible>(http::Response::new(axum::body::Body::from(body.to_string())))
            }
        });
        Client::new(service, "default")
    }

    /// Startup's name and identity decision, over every combination.
    mod site_startup {
        use super::super::SiteStartup;

        /// No name and no identity: idle, and still watching for an identity.
        #[test]
        fn unnamed_and_unidentified_idles_and_waits() {
            let startup = SiteStartup::resolve(false, false);
            assert_eq!(startup, SiteStartup::Idle);
            assert!(
                startup.waits_for_identity(),
                "an idle site must notice an identity arriving"
            );
            assert!(!startup.creates_records(), "records need the identity");
        }

        /// Named but not yet identified: the gap that skipped both paths. The
        /// records need the identity, so this site waits for it rather than
        /// proceeding on the name alone.
        #[test]
        fn named_but_unidentified_waits_without_creating_records() {
            let startup = SiteStartup::resolve(true, false);
            assert_eq!(startup, SiteStartup::AwaitingIdentity);
            assert!(
                startup.waits_for_identity(),
                "a named site must still wait for its identity"
            );
            assert!(!startup.creates_records(), "records need the identity");
        }

        /// Named and identified: create the records and stop watching.
        #[test]
        fn named_and_identified_creates_records_and_stops_waiting() {
            let startup = SiteStartup::resolve(true, true);
            assert_eq!(startup, SiteStartup::Identified);
            assert!(
                !startup.waits_for_identity(),
                "an identified site has nothing to wait for"
            );
            assert!(startup.creates_records());
        }

        /// The identity names the site, so an identity without a configured name
        /// is identified too.
        #[test]
        fn unnamed_but_identified_is_identified() {
            let startup = SiteStartup::resolve(false, true);
            assert_eq!(startup, SiteStartup::Identified);
            assert!(!startup.waits_for_identity());
            assert!(startup.creates_records());
        }

        /// Exactly one disposition creates records, and it is the only one that
        /// stops waiting.
        #[test]
        fn creating_records_and_waiting_are_exclusive() {
            for (named, has_identity) in [(false, false), (true, false), (true, true), (false, true)] {
                let startup = SiteStartup::resolve(named, has_identity);
                assert_ne!(
                    startup.creates_records(),
                    startup.waits_for_identity(),
                    "named={named} has_identity={has_identity} must either create records or wait"
                );
            }
        }
    }

    /// Real Kubernetes and mutual-TLS feedback proof for the isolated CI cluster.
    #[cfg(not(feature = "fips"))]
    mod feedback_contract {
        use super::*;

        /// Object owned only by this isolated contract test.
        const NETWORK: &str = "gateway-feedback-contract";

        #[tokio::test]
        #[ignore = "requires the dedicated pre-install hub Kind cluster and checked-in CRD"]
        async fn tls_feedback_persists_against_real_kubernetes() {
            let client = real_client().await;
            let api: Api<GridNetwork> = Api::all(client.clone());
            initialize(&api).await;
            reject_invalid_schema(&api).await;
            let writes = Arc::new(Writes::default());
            let feedback_client = racing_client(client, Arc::clone(&writes));
            let feedback =
                operator::gateway_ages::GatewayFeedback::new(feedback_client, operator::shutdown::Shutdown::never());
            let mesh = Mesh::new();
            peer_is_forbidden(&mesh, feedback.clone(), &writes).await;
            local_is_acknowledged(&mesh, feedback).await;
            verify_writes(&writes);
            verify_evidence(&api).await;
            api.delete(NETWORK, &kube::api::DeleteParams::default())
                .await
                .expect("remove the test network");
        }

        // ---------------------------------------------------------------------------
        // Test Utilities
        // ---------------------------------------------------------------------------

        /// Observed requests and actual API-server conflicts.
        #[derive(Default)]
        struct Writes {
            /// Real API-server responses carrying HTTP 409.
            conflicts: std::sync::atomic::AtomicUsize,
            /// Forwarded status patches, including the retry.
            patches: std::sync::atomic::AtomicUsize,
        }

        /// A real peer certificate must never cause a gateway status write.
        async fn peer_is_forbidden(mesh: &Mesh, feedback: operator::gateway_ages::GatewayFeedback, writes: &Writes) {
            let peer = post(mesh, mesh.west_is_pinned(), feedback).await;
            assert!(
                peer.starts_with("HTTP/1.1 403"),
                "a real peer certificate cannot ingest: {peer}"
            );
            assert_eq!(
                writes.patches.load(std::sync::atomic::Ordering::SeqCst),
                0,
                "peer makes no status write"
            );
        }

        /// Only the local certificate earns an exact durable receipt.
        async fn local_is_acknowledged(mesh: &Mesh, feedback: operator::gateway_ages::GatewayFeedback) {
            let local = post(mesh, mesh.west_is_local(), feedback).await;
            assert!(
                local.starts_with("HTTP/1.1 204"),
                "the real local certificate is acknowledged: {local}"
            );
            let headers = local.to_ascii_lowercase();
            assert!(
                headers.contains("x-grid-ingest-epoch: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n"),
                "durable acknowledgment identifies the exact epoch"
            );
            assert!(
                headers.contains("x-grid-ingest-sequence: 1\r\n"),
                "durable acknowledgment identifies the exact sequence"
            );
        }

        /// One actual stale-version rejection is followed by one fresh retry.
        fn verify_writes(writes: &Writes) {
            assert_eq!(
                writes.conflicts.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "the API server really rejected a stale version"
            );
            assert_eq!(
                writes.patches.load(std::sync::atomic::Ordering::SeqCst),
                2,
                "one stale patch then one fresh retry"
            );
        }

        /// Use only the explicit isolated cluster supplied by the CI harness.
        async fn real_client() -> Client {
            let context = std::env::var("GRID_FEEDBACK_TEST_CONTEXT").expect("explicit isolated Kind context");
            let options = kube::config::KubeConfigOptions {
                context: Some(context),
                ..Default::default()
            };
            let config = kube::Config::from_kubeconfig(&options)
                .await
                .expect("isolated cluster credentials");
            Client::try_from(config).expect("real Kubernetes client")
        }

        /// Create one network and independently publish the peer's condition.
        async fn initialize(api: &Api<GridNetwork>) {
            let before = api
                .list(&kube::api::ListParams::default())
                .await
                .expect("list isolated cluster");
            assert!(
                before.items.is_empty(),
                "the contract test must not touch existing networks"
            );
            let network = serde_json::from_value(serde_json::json!({"apiVersion":"grid.praxis.fast/v1alpha1","kind":"GridNetwork","metadata":{"name":NETWORK},"spec":{"seeds":[],"gridId":"feedback-test"}})).expect("network fixture");
            api.create(&PostParams::default(), &network)
                .await
                .expect("create network through real CRD");
            let peer = serde_json::json!({
                "apiVersion":"grid.praxis.fast/v1alpha1", "kind":"GridNetwork", "metadata":{"name":NETWORK},
                "status":{"conditions":[{"type":"PeerTimeValid","status":"False","reason":"TimestampOutOfRange","message":"peer rejection must survive gateway evidence","lastTransitionTime":"2026-10-14T00:00:00Z"}]}
            });
            api.patch_status(
                NETWORK,
                &PatchParams::apply(operator::peer_ages::FIELD_MANAGER),
                &Patch::Apply(&peer),
            )
            .await
            .expect("peer field manager");
        }

        /// An integer bound in the checked-in CRD must reject invalid status.
        async fn reject_invalid_schema(api: &Api<GridNetwork>) {
            let invalid = serde_json::json!({"status":{"gatewayAgeReports":[{"epoch":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","replica":"gateway","sequence":0,"skew":0,"rejected":false,"reportedRejected":false,"receivedAt":"2026-10-14T00:00:00Z"}]}});
            let result = api
                .patch_status(NETWORK, &PatchParams::default(), &Patch::Merge(&invalid))
                .await;
            assert!(
                matches!(&result, Err(kube::Error::Api(error)) if error.code == 422),
                "the API server enforces sequence minimum one: {result:?}"
            );
        }

        /// Forward every request to the real API after one competing metadata write.
        fn racing_client(client: Client, writes: Arc<Writes>) -> Client {
            let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
                let (client, writes) = (client.clone(), Arc::clone(&writes));
                async move {
                    if request.method() == http::Method::PATCH
                        && request.uri().path().ends_with("/status")
                        && writes.patches.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0
                    {
                        let api: Api<GridNetwork> = Api::all(client.clone());
                        let competing = serde_json::json!({"metadata":{"annotations":{"feedback-contract-race":"1"}}});
                        api.patch_metadata(NETWORK, &PatchParams::default(), &Patch::Merge(&competing))
                            .await?;
                    }
                    let response = client.send(request).await?;
                    if response.status() == http::StatusCode::CONFLICT {
                        writes.conflicts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    Ok::<_, kube::Error>(response)
                }
            });
            Client::new(service, "default")
        }

        /// Post the wire contract through the same router and TLS classification as production.
        async fn post(
            mesh: &Mesh,
            identity: SignalsIdentity,
            feedback: operator::gateway_ages::GatewayFeedback,
        ) -> String {
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
            let (sources, peers) = (SourceSlots::new(1, 1), PeerSlots::new(1, 8));
            let (addr, server) = mesh
                .serve(identity, feedback_app(feedback), (&sources, &peers), 1)
                .await;
            let mut stream = mesh.connect(addr).await;
            stream.write_all(wire_report().as_bytes()).await.expect("real TLS POST");
            let mut answer = Vec::new();
            tokio::time::timeout(std::time::Duration::from_secs(10), stream.read_to_end(&mut answer))
                .await
                .expect("bounded response")
                .expect("read response");
            server.await.expect("TLS server finished");
            String::from_utf8(answer).expect("HTTP response")
        }

        /// The exact production router with the real status client under test.
        fn feedback_app(feedback: operator::gateway_ages::GatewayFeedback) -> axum::Router {
            signals_app(Published {
                site: operator::signals::SignalStore::new(),
                peers: operator::signals::SignalStore::new(),
                rounds: operator::signals::PeerRounds::default(),
                providers: operator::site_providers::LocalProviders::default(),
                membership: "Swim",
                feedback,
            })
        }

        /// One empty-body version-one observation on the production POST path.
        fn wire_report() -> String {
            format!(
                "POST {} HTTP/1.1\r\nHost: east\r\nConnection: close\r\nContent-Length: 0\r\nx-grid-ingest-version: 1\r\nx-grid-ingest-replica: gateway\r\nx-grid-ingest-epoch: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\nx-grid-ingest-sequence: 1\r\nx-grid-ingest-skew: 1\r\nx-grid-ingest-rejected: true\r\n\r\n",
                grid_signals::feedback::INGEST_PATH
            )
        }

        /// Both durable evidence and each writer's real SSA ownership survive the retry.
        async fn verify_evidence(api: &Api<GridNetwork>) {
            let network = api.get(NETWORK).await.expect("durable evidence");
            verify_conditions(network.status.as_ref().expect("status persisted"));
            let fields = serde_json::to_value(&network.metadata.managed_fields).expect("managed fields");
            verify_owner(
                &fields,
                operator::peer_ages::FIELD_MANAGER,
                "PeerTimeValid",
                "GatewayAgesRejected",
            );
            verify_owner(
                &fields,
                operator::gateway_ages::FIELD_MANAGER,
                "GatewayAgesRejected",
                "PeerTimeValid",
            );
        }

        /// A gateway's fresh rejection does not recover the independently rejected peer.
        fn verify_conditions(status: &operator::crd::grid_network::GridNetworkStatus) {
            let peer = status
                .conditions
                .iter()
                .find(|held| held.type_ == "PeerTimeValid")
                .expect("peer condition preserved");
            assert_eq!(peer.status, "False", "gateway publication cannot recover the peer");
            assert_eq!(peer.reason, "TimestampOutOfRange", "peer reason preserved");
            let gateway = status
                .conditions
                .iter()
                .find(|held| held.type_ == "GatewayAgesRejected")
                .expect("gateway condition persisted");
            assert_eq!(gateway.status, "True", "fresh gateway rejection persisted");
            assert_eq!(status.gateway_age_reports.len(), 1, "one durable reporter");
            assert!(
                status
                    .gateway_age_reports
                    .first()
                    .is_some_and(|report| report.sequence == 1 && report.rejected),
                "exact accepted gateway evidence"
            );
        }

        /// A real field manager owns its keyed condition without claiming the other's key.
        fn verify_owner(fields: &serde_json::Value, manager: &str, own: &str, other: &str) {
            let owner = fields
                .as_array()
                .expect("field managers")
                .iter()
                .find(|entry| entry.get("manager").and_then(serde_json::Value::as_str) == Some(manager))
                .expect("independent manager");
            let conditions = owner
                .pointer("/fieldsV1/f:status/f:conditions")
                .expect("owned condition fields");
            let own_key = format!("k:{{\"type\":\"{own}\"}}");
            let other_key = format!("k:{{\"type\":\"{other}\"}}");
            assert!(conditions.get(&own_key).is_some(), "{manager} owns its keyed condition");
            assert!(
                conditions.get(&other_key).is_none(),
                "{manager} cannot own the other condition"
            );
        }
    }
}
