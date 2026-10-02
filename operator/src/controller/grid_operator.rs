//! Publish this cluster's [`GridOperator`] status from what the operator already observes.
//!
//! The inputs are computed once, where `GridNetwork` status and the metrics already compute them.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        LazyLock, Mutex, OnceLock, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use k8s_openapi::api::apps::v1::Deployment;
use kube::{
    Api, Client,
    api::{Patch, PatchParams, PostParams},
};

use crate::{
    crd::{
        condition::{self, ConditionStatus, Observed},
        grid_network::{
            ConsumerConfigPhase, ConsumerConfigStatus, GridNetworkPhase, OverlayPhase, OverlayRevisionStatus,
        },
        grid_operator::{
            GRID_OPERATOR_NAME, GridOperator, GridOperatorSpec, GridOperatorStatus, ManagementState, OperatorLogLevel,
        },
        grid_site::GridSitePhase,
        inference_provider::ProviderPhase,
    },
    error::OperatorError,
};

/// Every `*Available` condition is true.
pub const AVAILABLE: &str = "Available";

/// Some `*Progressing` condition is true.
pub const PROGRESSING: &str = "Progressing";

/// Some `*Degraded` condition is true.
pub const DEGRADED: &str = "Degraded";

/// This site joined the grid and sees a peer, or the grid has no other site.
pub const GRID_SITES_AVAILABLE: &str = "GridSitesAvailable";

/// This site is joining over SWIM.
pub const GRID_SITES_PROGRESSING: &str = "GridSitesProgressing";

/// This site's SWIM runtime stopped.
pub const GRID_SITES_DEGRADED: &str = "GridSitesDegraded";

/// Local providers exist and at least one is ready, or none exist.
pub const PROVIDERS_AVAILABLE: &str = "ProvidersAvailable";

/// A local provider fails its probe or metrics scrape.
pub const PROVIDERS_DEGRADED: &str = "ProvidersDegraded";

/// A gateway's Deployment is rolling out.
pub const GATEWAY_PROGRESSING: &str = "GatewayProgressing";

/// A gateway overlay or consumer config fails to render.
pub const GATEWAY_CONFIG_DEGRADED: &str = "GatewayConfigDegraded";

/// This site's certificate is within [`CERT_EXPIRY_WARNING`] of expiry.
pub const SITE_CERTIFICATE_DEGRADED: &str = "SiteCertificateDegraded";

/// How long a failure must persist before it degrades the operator.
pub const DEGRADED_GRACE: Duration = Duration::from_secs(300);

/// How close to expiry this site's certificate may get before it degrades the operator.
pub const CERT_EXPIRY_WARNING: time::Duration = time::Duration::days(7);

/// How often the status is republished without a reconcile.
pub const REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// A `*Degraded` message with no open failure.
const NO_FAILURES: &str = "no failures";

/// Field manager for `GridOperator` status writes.
const FIELD_MANAGER: &str = "grid-operator";

/// What one `GridNetwork` reconcile saw, shared with that network's own status.
#[derive(Clone, Debug, Default)]
pub(crate) struct NetworkObservation {
    /// Network phase.
    pub phase: GridNetworkPhase,
    /// Overlay distribution per gateway.
    pub overlays: Vec<OverlayRevisionStatus>,
    /// Consumer config per gateway.
    pub consumers: Vec<ConsumerConfigStatus>,
    /// Each gateway ref as namespace and name.
    pub gateways: Vec<(String, String)>,
    /// Each remote site and its phase, reported but never degrading.
    pub sites: Vec<(String, GridSitePhase)>,
    /// Each local provider and its phase.
    pub providers: Vec<(String, ProviderPhase)>,
    /// The SWIM runtime stopped.
    pub gossip_lost: bool,
    /// This site's certificate expires within [`CERT_EXPIRY_WARNING`].
    pub cert_expiring: bool,
}

/// Every input to the status: per-network observations and the clocks of open failures.
#[derive(Debug, Default)]
pub(crate) struct HealthBoard {
    /// The latest observation of each `GridNetwork`.
    networks: BTreeMap<String, NetworkObservation>,
    /// When each open failure was first seen, keyed by what failed.
    failing_since: BTreeMap<String, Instant>,
    /// Where pre-start enrollment is, while it runs.
    enrollment: Option<Enrollment>,
    /// The SPIFFE ID this site enrolled as.
    enrolled: Option<String>,
    /// Why the process is about to restart into new grid modes.
    restarting: Option<String>,
}

/// Where pre-start enrollment is.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Enrollment {
    /// Waiting for a `GridNetwork` to name the grid.
    WaitingForGridNetwork {
        /// The last retry reason.
        last: Option<String>,
    },
    /// Redeeming the site token.
    Enrolling {
        /// Site name the token pins.
        site: String,
        /// Enrollment service URL.
        url: String,
        /// The last retry reason.
        last: Option<String>,
    },
}

impl HealthBoard {
    /// Open or clear the failure `key` at `now`, keeping the first time it failed.
    fn mark(&mut self, key: String, failing: bool, now: Instant) {
        if failing {
            self.failing_since.entry(key).or_insert(now);
        } else {
            self.failing_since.remove(&key);
        }
    }

    /// Replace `network`'s observation and its failure clocks.
    pub(crate) fn observe(&mut self, network: &str, observation: NetworkObservation, now: Instant) {
        let prefix = format!("network {network} ");
        let open = open_failures(&prefix, &observation);
        self.failing_since
            .retain(|key, _| !key.starts_with(&prefix) || open.contains(key));
        for key in open {
            self.mark(key, true, now);
        }
        self.networks.insert(network.to_owned(), observation);
    }

    /// Drop networks that no longer exist, with their failure clocks.
    pub(crate) fn retain_networks(&mut self, live: &[String]) {
        self.networks.retain(|name, _| live.contains(name));
        self.failing_since.retain(|key, _| {
            key.strip_prefix("network ")
                .and_then(|rest| rest.split_once(' '))
                .is_none_or(|(name, _)| live.iter().any(|live| live == name))
        });
    }
}

/// The failure keys `observation` holds open, each under `prefix`.
fn open_failures(prefix: &str, observation: &NetworkObservation) -> Vec<String> {
    let mut open: Vec<String> = Vec::new();
    for overlay in &observation.overlays {
        if overlay.phase == OverlayPhase::Error {
            open.push(format!(
                "{prefix}overlay {}/{}",
                overlay.namespace, overlay.gateway_name
            ));
        }
    }
    for consumer in &observation.consumers {
        if consumer.phase == ConsumerConfigPhase::Error {
            open.push(format!(
                "{prefix}consumer config {}/{}",
                consumer.namespace, consumer.gateway_name
            ));
        }
    }
    for (provider, phase) in &observation.providers {
        if matches!(phase, ProviderPhase::Degraded | ProviderPhase::Unavailable) {
            open.push(format!("{prefix}provider {provider} probe"));
        }
    }
    if observation.gossip_lost {
        open.push(format!("{prefix}gossip join"));
    }
    if observation.cert_expiring {
        open.push(format!("{prefix}site certificate"));
    }
    open
}

/// What a gateway's Deployment reports about its rollout.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Rollout {
    /// `metadata.generation`.
    pub generation: i64,
    /// `status.observedGeneration`.
    pub observed_generation: i64,
    /// Desired pods.
    pub replicas: i32,
    /// Pods on the current spec.
    pub updated: i32,
    /// Available pods.
    pub available: i32,
    /// The first container image when it is pinned by digest.
    pub digest: Option<String>,
}

impl Rollout {
    /// Read the rollout fields of `deployment`.
    fn of(deployment: &Deployment) -> Self {
        let status = deployment.status.clone().unwrap_or_default();
        let spec = deployment.spec.as_ref();
        Self {
            generation: deployment.metadata.generation.unwrap_or(0),
            observed_generation: status.observed_generation.unwrap_or(0),
            replicas: spec.and_then(|s| s.replicas).unwrap_or(1),
            updated: status.updated_replicas.unwrap_or(0),
            available: status.available_replicas.unwrap_or(0),
            digest: spec
                .and_then(|s| s.template.spec.as_ref())
                .and_then(|pod| pod.containers.first())
                .and_then(|c| c.image.clone())
                .filter(|image| image.contains('@')),
        }
    }

    /// The controller has not converged on the current spec.
    const fn in_progress(&self) -> bool {
        self.observed_generation < self.generation || self.updated < self.replicas || self.available < self.replicas
    }
}

/// A gateway as `namespace/name` and its Deployment rollout, `None` when it has no Deployment.
pub(crate) type GatewayRollout = (String, Option<Rollout>);

/// The process-wide board, fed by reconciles and provider scrapes.
static BOARD: LazyLock<Mutex<HealthBoard>> = LazyLock::new(|| Mutex::new(HealthBoard::default()));

/// Run `update` on the shared board.
fn with_board<R>(update: impl FnOnce(&mut HealthBoard) -> R) -> R {
    update(&mut BOARD.lock().unwrap_or_else(PoisonError::into_inner))
}

/// Record what a `GridNetwork` reconcile observed.
pub(crate) fn observe(network: &str, observation: NetworkObservation) {
    with_board(|board| board.observe(network, observation, Instant::now()));
}

/// Wakes the publisher early.
static REPUBLISH: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// Record where enrollment is, and republish.
pub fn set_enrollment(stage: Option<Enrollment>) {
    with_board(|board| board.enrollment = stage);
    REPUBLISH.notify_one();
}

/// Record that this site enrolled as `spiffe_id`, and republish.
pub fn set_enrolled(spiffe_id: String) {
    with_board(|board| {
        board.enrollment = None;
        board.enrolled = Some(spiffe_id);
    });
    REPUBLISH.notify_one();
}

/// Record why the current enrollment step is retrying, and republish.
pub fn enrollment_retry(reason: String) {
    with_board(|board| {
        if let Some(Enrollment::WaitingForGridNetwork { last } | Enrollment::Enrolling { last, .. }) =
            &mut board.enrollment
        {
            *last = Some(reason);
        }
    });
    REPUBLISH.notify_one();
}

/// Report that the process restarts into new grid modes, publishing before it exits.
pub(crate) async fn restarting_for_modes(client: &Client, message: String) {
    with_board(|board| board.restarting = Some(message));
    if let Err(error) = publish(client).await {
        tracing::warn!(%error, "GridOperator status publish failed");
    }
}

/// Record the outcome of scraping a provider's metrics.
pub(crate) fn record_scrape(provider: &str, ok: bool) {
    with_board(|board| board.mark(format!("provider {provider} scrape"), !ok, Instant::now()));
}

/// Every condition for `board` and `gateways` at `now`, the derived three first.
pub(crate) fn derive(board: &HealthBoard, gateways: &[GatewayRollout], now: Instant) -> Vec<Observed> {
    let networks = || board.networks.values();
    let sites: Vec<&(String, GridSitePhase)> = networks().flat_map(|n| &n.sites).collect();
    let providers: Vec<&(String, ProviderPhase)> = networks().flat_map(|n| &n.providers).collect();
    let mut conditions = vec![
        grid_sites_available(board, &sites),
        grid_sites_progressing(board),
        degraded_by(board, now, GRID_SITES_DEGRADED, "GossipJoinLost", &[" gossip join"]),
        providers_available(&providers),
        degraded_by(board, now, PROVIDERS_DEGRADED, "ProbeOrScrapeFailing", &[" provider "]),
        gateway_progressing(gateways),
        gateway_config_degraded(board, now),
        degraded_by(
            board,
            now,
            SITE_CERTIFICATE_DEGRADED,
            "ExpiresSoon",
            &[" site certificate"],
        ),
    ];
    conditions.splice(0..0, union(&conditions));
    conditions
}

/// `Available`, `Progressing`, and `Degraded` as the union of the specific conditions, by type suffix.
pub(crate) fn union(specific: &[Observed]) -> [Observed; 3] {
    let first = |suffix: &str, contributes: &dyn Fn(&Observed) -> bool| {
        specific.iter().find(|c| c.type_.ends_with(suffix) && contributes(c))
    };
    let from = |type_: &'static str, status: ConditionStatus, source: &Observed| {
        Observed::new(type_, status, &source.reason).with_message(format!("{}: {}", source.type_, source.message))
    };
    let settled = |type_: &'static str, status: ConditionStatus, message: &str| {
        Observed::new(type_, status, "AsExpected").with_message(message.to_owned())
    };
    [
        first("Available", &|c| c.status != ConditionStatus::True).map_or_else(
            || settled(AVAILABLE, ConditionStatus::True, "every component is available"),
            |c| from(AVAILABLE, ConditionStatus::False, c),
        ),
        first("Progressing", &|c| c.status == ConditionStatus::True).map_or_else(
            || settled(PROGRESSING, ConditionStatus::False, "nothing is converging"),
            |c| from(PROGRESSING, ConditionStatus::True, c),
        ),
        first("Degraded", &|c| c.status == ConditionStatus::True).map_or_else(
            || {
                settled(
                    DEGRADED,
                    ConditionStatus::False,
                    "no failure outlasted the grace window",
                )
            },
            |c| from(DEGRADED, ConditionStatus::True, c),
        ),
    ]
}

/// Phases in which SWIM sees a peer.
const SEEN: [GridSitePhase; 3] = [
    GridSitePhase::Discovered,
    GridSitePhase::Connecting,
    GridSitePhase::Active,
];

/// `GridSitesAvailable`: joined and seeing a peer over SWIM, `Isolated` when no known peer is seen.
fn grid_sites_available(board: &HealthBoard, sites: &[&(String, GridSitePhase)]) -> Observed {
    let at = |status, reason: &str, message: String| {
        Observed::new(GRID_SITES_AVAILABLE, status, reason).with_message(message)
    };
    if board.networks.is_empty() {
        let message = board.enrolled.as_ref().map_or_else(
            || "no GridNetwork exists on this cluster yet".to_owned(),
            |id| format!("enrolled as {id}; no GridNetwork yet"),
        );
        return at(ConditionStatus::Unknown, "NoGridNetwork", message);
    }
    if let Some((name, network)) = board.networks.iter().find(|(_, n)| n.phase != GridNetworkPhase::Active) {
        return at(
            ConditionStatus::Unknown,
            "Joining",
            format!("GridNetwork {name} is {:?}; waiting to join", network.phase),
        );
    }
    let message = sites_message(sites);
    if !sites.is_empty() && !sites.iter().any(|(_, phase)| SEEN.contains(phase)) {
        return at(ConditionStatus::False, "Isolated", message);
    }
    at(ConditionStatus::True, "Connected", message)
}

/// "N of M sites are available, K degraded (first)".
fn sites_message(sites: &[&(String, GridSitePhase)]) -> String {
    let down: Vec<&str> = sites
        .iter()
        .filter(|(_, phase)| *phase != GridSitePhase::Active)
        .map(|(name, _)| name.as_str())
        .collect();
    let first = down.first().map(|first| format!(" ({first})")).unwrap_or_default();
    format!(
        "{} of {} sites are available, {} degraded{first}",
        sites.len() - down.len(),
        sites.len(),
        down.len()
    )
}

/// `GridSitesProgressing`: enrolling, or joining over SWIM.
fn grid_sites_progressing(board: &HealthBoard) -> Observed {
    let yes = |reason: &str, message: String| {
        Observed::new(GRID_SITES_PROGRESSING, ConditionStatus::True, reason).with_message(message)
    };
    if let Some(message) = &board.restarting {
        return yes("RestartingForModes", message.clone());
    }
    if let Some(stage) = &board.enrollment {
        return enrollment_progressing(stage);
    }
    if board.networks.is_empty() {
        return Observed::new(GRID_SITES_PROGRESSING, ConditionStatus::Unknown, "NoGridNetwork")
            .with_message("no GridNetwork exists on this cluster yet".to_owned());
    }
    for (name, network) in &board.networks {
        if matches!(
            network.phase,
            GridNetworkPhase::Pending | GridNetworkPhase::Initializing
        ) {
            return yes(
                "JoiningGrid",
                format!("GridNetwork {name} is {:?}; joining over SWIM", network.phase),
            );
        }
    }
    Observed::new(GRID_SITES_PROGRESSING, ConditionStatus::False, "AsExpected").with_message("joined".to_owned())
}

/// `GridSitesProgressing` while pre-start enrollment runs.
fn enrollment_progressing(stage: &Enrollment) -> Observed {
    let last = |last: &Option<String>| last.as_ref().map(|l| format!(" (last: {l})")).unwrap_or_default();
    let (reason, message) = match stage {
        Enrollment::WaitingForGridNetwork { last: reason } => (
            "WaitingForGridNetwork",
            format!(
                "enrollment waits for a GridNetwork to name the grid; install grid-site{}",
                last(reason)
            ),
        ),
        Enrollment::Enrolling {
            site,
            url,
            last: reason,
        } => ("Enrolling", format!("enrolling as {site} at {url}{}", last(reason))),
    };
    Observed::new(GRID_SITES_PROGRESSING, ConditionStatus::True, reason).with_message(message)
}

/// `ProvidersAvailable`: false only when local providers exist and none is ready.
fn providers_available(providers: &[&(String, ProviderPhase)]) -> Observed {
    let ready = providers
        .iter()
        .filter(|(_, phase)| *phase == ProviderPhase::Available)
        .count();
    let message = format!("{ready} of {} local providers are ready", providers.len());
    let (status, reason) = if !providers.is_empty() && ready == 0 {
        (ConditionStatus::False, "NoneReady")
    } else {
        (ConditionStatus::True, "Ready")
    };
    Observed::new(PROVIDERS_AVAILABLE, status, reason).with_message(message)
}

/// `GatewayProgressing`: a gateway's Deployment has not converged on its current spec.
fn gateway_progressing(gateways: &[GatewayRollout]) -> Observed {
    let rolling: Vec<String> = gateways
        .iter()
        .filter_map(|(gateway, rollout)| rollout.as_ref().filter(|r| r.in_progress()).map(|r| (gateway, r)))
        .map(|(gateway, r)| {
            let digest = r.digest.as_deref().map(|d| format!(" ({d})")).unwrap_or_default();
            format!(
                "{gateway} rolling out: {} of {} pods updated{digest}",
                r.updated, r.replicas
            )
        })
        .collect();
    if !rolling.is_empty() {
        return Observed::new(GATEWAY_PROGRESSING, ConditionStatus::True, "RollingOut")
            .with_message(rolling.join("; "));
    }
    let settled: Vec<String> = gateways
        .iter()
        .map(|(gateway, rollout)| match rollout {
            Some(r) => format!("{gateway}: {0} of {0} pods on the current spec", r.replicas),
            None => format!("no Deployment found for {gateway}"),
        })
        .collect();
    let message = if settled.is_empty() {
        "no gateways".to_owned()
    } else {
        settled.join("; ")
    };
    Observed::new(GATEWAY_PROGRESSING, ConditionStatus::False, "AsExpected").with_message(message)
}

/// `GatewayConfigDegraded`, listing each rendered revision when nothing fails.
fn gateway_config_degraded(board: &HealthBoard, now: Instant) -> Observed {
    let mut degraded = degraded_by(
        board,
        now,
        GATEWAY_CONFIG_DEGRADED,
        "RenderFailing",
        &[" overlay ", " consumer config "],
    );
    if degraded.message == NO_FAILURES {
        let rendered: Vec<String> = board
            .networks
            .values()
            .flat_map(|n| &n.overlays)
            .map(|o| {
                format!(
                    "{}/{} revision {} rendered with {} candidates",
                    o.namespace, o.gateway_name, o.distributed_revision, o.candidate_count
                )
            })
            .collect();
        degraded.message = if rendered.is_empty() {
            "no gateway configs".to_owned()
        } else {
            rendered.join("; ")
        };
    }
    degraded
}

/// `type_` from the failure clocks whose key contains a marker, true only past [`DEGRADED_GRACE`].
fn degraded_by(board: &HealthBoard, now: Instant, type_: &'static str, reason: &str, markers: &[&str]) -> Observed {
    let open: Vec<(&str, Duration)> = board
        .failing_since
        .iter()
        .filter(|(key, _)| markers.iter().any(|marker| format!(" {key}").contains(marker)))
        .map(|(key, since)| (key.as_str(), now.saturating_duration_since(*since)))
        .collect();
    let persistent: Vec<&(&str, Duration)> = open.iter().filter(|(_, age)| *age >= DEGRADED_GRACE).collect();
    let Some((first, age)) = persistent.first() else {
        let message = if open.is_empty() {
            NO_FAILURES.to_owned()
        } else {
            format!(
                "{} failure(s) within the {}s grace window",
                open.len(),
                DEGRADED_GRACE.as_secs()
            )
        };
        return Observed::new(type_, ConditionStatus::False, "AsExpected").with_message(message);
    };
    Observed::new(type_, ConditionStatus::True, reason).with_message(format!(
        "{first} failing for {}s ({} in all)",
        age.as_secs(),
        persistent.len()
    ))
}

/// Whether `spec.managementState` is `Unmanaged`.
static UNMANAGED: AtomicBool = AtomicBool::new(false);

/// Swaps the log filter, set once at startup.
pub type LogReload = tracing_subscriber::reload::Handle<tracing_subscriber::EnvFilter, tracing_subscriber::Registry>;

/// The process log filter handle.
static LOG_RELOAD: OnceLock<LogReload> = OnceLock::new();

/// The level last applied through [`LOG_RELOAD`].
static LOG_LEVEL: Mutex<OperatorLogLevel> = Mutex::new(OperatorLogLevel::Normal);

/// Register the handle `spec.operatorLogLevel` is applied through.
pub fn set_log_reload(handle: LogReload) {
    if LOG_RELOAD.set(handle).is_err() {
        tracing::warn!("operator log reload handle already set");
    }
}

/// Whether controllers must skip reconciling.
#[must_use]
pub fn unmanaged() -> bool {
    UNMANAGED.load(Ordering::Relaxed)
}

/// Apply `spec` to this process: the management gate and, when it changed, the log level.
fn apply_spec(spec: &GridOperatorSpec) {
    UNMANAGED.store(spec.management_state == ManagementState::Unmanaged, Ordering::Relaxed);
    let mut applied = LOG_LEVEL.lock().unwrap_or_else(PoisonError::into_inner);
    if *applied == spec.operator_log_level {
        return;
    }
    let Some(handle) = LOG_RELOAD.get() else { return };
    let filter = spec.operator_log_level.directive().map_or_else(
        tracing_subscriber::EnvFilter::from_default_env,
        tracing_subscriber::EnvFilter::new,
    );
    let reloaded = handle.reload(filter);
    if reloaded.is_ok() {
        *applied = spec.operator_log_level;
    }
    drop(applied);
    match reloaded {
        Ok(()) => tracing::info!(level = ?spec.operator_log_level, "operator log level applied"),
        Err(error) => tracing::warn!(%error, "operator log level reload failed"),
    }
}

/// The `cluster` singleton, created with the default spec if missing.
#[expect(clippy::large_stack_frames, reason = "async future with kube API types")]
async fn get_or_create(api: &Api<GridOperator>) -> Result<GridOperator, OperatorError> {
    if let Some(current) = api.get_opt(GRID_OPERATOR_NAME).await? {
        return Ok(current);
    }
    let created = GridOperator::new(GRID_OPERATOR_NAME, GridOperatorSpec::default());
    match api.create(&PostParams::default(), &created).await {
        Ok(object) => Ok(object),
        // Another writer, or the chart, created it first.
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(api.get(GRID_OPERATOR_NAME).await?),
        Err(e) => Err(e.into()),
    }
}

/// The Deployment rollout of every gateway the board knows, by its gateway ref name.
async fn gateway_rollouts(client: &Client) -> Vec<GatewayRollout> {
    let refs: BTreeSet<(String, String)> = with_board(|board| {
        board
            .networks
            .values()
            .flat_map(|n| n.gateways.iter().cloned())
            .collect()
    });
    let mut rollouts = Vec::with_capacity(refs.len());
    for (namespace, name) in refs {
        let deployments: Api<Deployment> = Api::namespaced(client.clone(), &namespace);
        // A gateway may run outside this cluster, so an unreadable Deployment is not a failure.
        let rollout = match deployments.get_opt(&name).await {
            Ok(deployment) => deployment.as_ref().map(Rollout::of),
            Err(error) => {
                tracing::debug!(%error, namespace, name, "gateway Deployment unreadable");
                None
            },
        };
        rollouts.push((format!("{namespace}/{name}"), rollout));
    }
    rollouts
}

/// Create the `cluster` singleton if missing, then write its status when it changed.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures.
#[expect(clippy::large_stack_frames, reason = "async future with kube API types")]
pub async fn publish(client: &Client) -> Result<(), OperatorError> {
    let api: Api<GridOperator> = Api::all(client.clone());
    let current = get_or_create(&api).await?;
    apply_spec(&current.spec);
    let previous = current.status.unwrap_or_default();
    let gateways = gateway_rollouts(client).await;
    let observed = with_board(|board| derive(board, &gateways, Instant::now()));
    let status = GridOperatorStatus {
        conditions: condition::refresh(
            Some(&previous.conditions),
            observed,
            current.metadata.generation.unwrap_or(0),
        ),
    };
    if status == previous {
        return Ok(());
    }
    let patch = serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridOperator",
        "status": status,
    });
    api.patch_status(
        GRID_OPERATOR_NAME,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(patch),
    )
    .await?;
    Ok(())
}

/// Republish on a timer, pruning networks that were deleted, so the status moves without a reconcile.
pub async fn run_publisher(client: Client) {
    let networks: Api<crate::crd::grid_network::GridNetwork> = Api::all(client.clone());
    loop {
        match networks.list(&kube::api::ListParams::default()).await {
            Ok(list) => {
                let live: Vec<String> = list.items.iter().filter_map(|n| n.metadata.name.clone()).collect();
                with_board(|board| board.retain_networks(&live));
            },
            Err(error) => tracing::warn!(%error, "GridOperator: listing GridNetworks failed"),
        }
        if let Err(error) = publish(&client).await {
            tracing::warn!(%error, "GridOperator status publish failed");
        }
        tokio::select! {
            () = tokio::time::sleep(REFRESH_INTERVAL) => {},
            () = REPUBLISH.notified() => {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn overlay(gateway: &str, phase: OverlayPhase, revision: &str) -> OverlayRevisionStatus {
        OverlayRevisionStatus {
            gateway_name: gateway.to_owned(),
            namespace: "grid".to_owned(),
            distributed_revision: revision.to_owned(),
            candidate_count: 2,
            phase,
            message: "boom".to_owned(),
            ..OverlayRevisionStatus::default()
        }
    }

    fn healthy(revision: &str) -> NetworkObservation {
        NetworkObservation {
            phase: GridNetworkPhase::Active,
            overlays: vec![overlay("gw", OverlayPhase::Distributed, revision)],
            ..NetworkObservation::default()
        }
    }

    /// The condition of `type_` in `observed`.
    fn get<'list>(observed: &'list [Observed], type_: &str) -> &'list Observed {
        observed
            .iter()
            .find(|o| o.type_ == type_)
            .unwrap_or_else(|| std::process::abort())
    }

    /// The rules every grid kind's conditions follow.
    pub(crate) fn assert_conforms(observed: &[Observed]) {
        for condition in observed {
            assert_ne!(
                condition.reason, condition.type_,
                "{} reason repeats the type",
                condition.type_
            );
            assert!(
                !condition.message.is_empty(),
                "{} has an empty message",
                condition.type_
            );
        }
        let (top, specific) = observed.split_at(3);
        assert_eq!(
            top,
            union(specific),
            "top-level conditions are the union of the specific ones"
        );
    }

    #[test]
    fn every_reading_conforms() {
        let now = Instant::now();
        let mut boards = vec![HealthBoard::default()];
        let mut board = HealthBoard::default();
        board.observe("grid", healthy("abc"), now);
        boards.push(board);
        let mut failing = HealthBoard::default();
        failing.observe(
            "grid",
            NetworkObservation {
                phase: GridNetworkPhase::Initializing,
                cert_expiring: true,
                gossip_lost: true,
                overlays: vec![overlay("gw", OverlayPhase::Error, "")],
                sites: vec![("b".to_owned(), GridSitePhase::Unreachable)],
                providers: vec![("p".to_owned(), ProviderPhase::Unavailable)],
                ..healthy("")
            },
            now,
        );
        boards.push(failing);
        for each in &boards {
            for at in [now, now + DEGRADED_GRACE] {
                assert_conforms(&derive(each, &[], at));
            }
        }
    }

    #[test]
    fn a_healthy_cluster_is_available_and_settles() {
        let now = Instant::now();
        let mut board = HealthBoard::default();
        board.observe("grid", healthy("abc"), now);
        let first = derive(&board, &[], now);
        assert_eq!(get(&first, AVAILABLE).status, ConditionStatus::True);
        assert_eq!(
            get(&first, GATEWAY_CONFIG_DEGRADED).message,
            "grid/gw revision abc rendered with 2 candidates"
        );
        board.observe("grid", healthy("abc"), now);
        let second = derive(&board, &[], now);
        assert_eq!(get(&second, PROGRESSING).status, ConditionStatus::False);
        assert_eq!(get(&second, DEGRADED).reason, "AsExpected");
        assert_eq!(
            get(&second, GRID_SITES_AVAILABLE).reason,
            "Connected",
            "a grid with no other site"
        );
    }

    #[test]
    fn joining_is_progress_not_failure() {
        let now = Instant::now();
        let mut board = HealthBoard::default();
        board.observe(
            "grid",
            NetworkObservation {
                phase: GridNetworkPhase::Pending,
                ..healthy("")
            },
            now,
        );
        let observed = derive(&board, &[], now);
        assert_eq!(get(&observed, GRID_SITES_AVAILABLE).status, ConditionStatus::Unknown);
        assert_eq!(get(&observed, AVAILABLE).reason, "Joining");
        assert_eq!(get(&observed, PROGRESSING).reason, "JoiningGrid");
        assert_eq!(get(&observed, DEGRADED).status, ConditionStatus::False);
    }

    #[test]
    fn a_failure_degrades_only_after_the_grace_window() {
        let now = Instant::now();
        let mut board = HealthBoard::default();
        board.observe(
            "grid",
            NetworkObservation {
                overlays: vec![overlay("gw", OverlayPhase::Error, "")],
                ..healthy("")
            },
            now,
        );
        let fresh = derive(&board, &[], now);
        assert_eq!(
            get(&fresh, DEGRADED).status,
            ConditionStatus::False,
            "never on a first failure"
        );
        let late = derive(&board, &[], now + DEGRADED_GRACE);
        assert_eq!(get(&late, GATEWAY_CONFIG_DEGRADED).status, ConditionStatus::True);
        assert_eq!(get(&late, DEGRADED).reason, "RenderFailing");

        board.observe("grid", healthy("abc"), now + DEGRADED_GRACE);
        let recovered = derive(&board, &[], now + DEGRADED_GRACE);
        assert_eq!(
            get(&recovered, DEGRADED).status,
            ConditionStatus::False,
            "a fix clears the clock"
        );
    }

    #[test]
    fn each_owned_failure_has_its_own_condition() {
        let now = Instant::now();
        let mut board = HealthBoard::default();
        board.observe(
            "grid",
            NetworkObservation {
                cert_expiring: true,
                gossip_lost: true,
                ..healthy("abc")
            },
            now,
        );
        board.mark("provider p scrape".to_owned(), true, now);
        let observed = derive(&board, &[], now + DEGRADED_GRACE);
        for type_ in [SITE_CERTIFICATE_DEGRADED, GRID_SITES_DEGRADED, PROVIDERS_DEGRADED] {
            assert_eq!(get(&observed, type_).status, ConditionStatus::True, "{type_}");
        }
        assert_eq!(get(&observed, GATEWAY_CONFIG_DEGRADED).status, ConditionStatus::False);
        assert!(
            get(&observed, PROVIDERS_DEGRADED)
                .message
                .contains("provider p scrape failing for 300s (1 in all)")
        );
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "case sequence")]
    fn peer_health_never_degrades_and_only_isolation_is_unavailable() {
        let now = Instant::now();
        let mut board = HealthBoard::default();
        board.observe(
            "grid",
            NetworkObservation {
                sites: vec![
                    ("b".to_owned(), GridSitePhase::Unreachable),
                    ("c".to_owned(), GridSitePhase::Active),
                ],
                providers: vec![
                    ("p1".to_owned(), ProviderPhase::Unavailable),
                    ("p2".to_owned(), ProviderPhase::Available),
                ],
                ..healthy("abc")
            },
            now,
        );
        let observed = derive(&board, &[], now + DEGRADED_GRACE);
        let sites = get(&observed, GRID_SITES_AVAILABLE);
        assert_eq!(sites.reason, "Connected");
        assert_eq!(sites.message, "1 of 2 sites are available, 1 degraded (b)");
        assert_eq!(
            get(&observed, PROVIDERS_AVAILABLE).message,
            "1 of 2 local providers are ready"
        );
        assert_eq!(get(&observed, PROVIDERS_DEGRADED).reason, "ProbeOrScrapeFailing");

        board.observe(
            "grid",
            NetworkObservation {
                sites: vec![("b".to_owned(), GridSitePhase::Discovered)],
                ..healthy("abc")
            },
            now,
        );
        let discovered = derive(&board, &[], now + DEGRADED_GRACE);
        assert_eq!(
            get(&discovered, GRID_SITES_AVAILABLE).reason,
            "Connected",
            "a peer seen over SWIM without a gateway address"
        );

        board.observe(
            "grid",
            NetworkObservation {
                sites: vec![("b".to_owned(), GridSitePhase::Unreachable)],
                ..healthy("abc")
            },
            now,
        );
        let unreachable = derive(&board, &[], now + DEGRADED_GRACE);
        assert_eq!(get(&unreachable, GRID_SITES_AVAILABLE).reason, "Isolated");
        assert_eq!(get(&unreachable, AVAILABLE).reason, "Isolated");
        assert_eq!(
            get(&unreachable, DEGRADED).status,
            ConditionStatus::False,
            "peers never degrade this cluster"
        );
    }

    #[test]
    fn a_deleted_network_takes_its_failures_with_it() {
        let now = Instant::now();
        let mut board = HealthBoard::default();
        board.observe(
            "gone",
            NetworkObservation {
                overlays: vec![overlay("gw", OverlayPhase::Error, "")],
                ..healthy("")
            },
            now,
        );
        board.mark("provider p scrape".to_owned(), true, now);
        board.retain_networks(&[]);
        assert!(board.networks.is_empty());
        assert_eq!(board.failing_since.len(), 1, "scrape clocks are not network-scoped");
    }

    fn rollout(replicas: i32, updated: i32) -> Rollout {
        Rollout {
            generation: 2,
            observed_generation: 2,
            replicas,
            updated,
            available: updated,
            digest: Some("gw@sha256:ab".to_owned()),
        }
    }

    #[test]
    fn a_gateway_deployment_mid_rollout_is_progressing() {
        let gateways = [("grid/gw".to_owned(), Some(rollout(3, 1)))];
        let observed = derive(&HealthBoard::default(), &gateways, Instant::now());
        let gateway = get(&observed, GATEWAY_PROGRESSING);
        assert_eq!(gateway.status, ConditionStatus::True);
        assert_eq!(
            gateway.message,
            "grid/gw rolling out: 1 of 3 pods updated (gw@sha256:ab)"
        );
        assert_eq!(get(&observed, PROGRESSING).reason, "RollingOut");
        let stale = [(
            "grid/gw".to_owned(),
            Some(Rollout {
                observed_generation: 1,
                ..rollout(3, 3)
            }),
        )];
        assert_eq!(
            get(
                &derive(&HealthBoard::default(), &stale, Instant::now()),
                GATEWAY_PROGRESSING
            )
            .status,
            ConditionStatus::True,
            "the controller has not seen the new spec"
        );
    }

    #[test]
    fn a_converged_gateway_deployment_is_not_progressing() {
        let gateways = [("grid/gw".to_owned(), Some(rollout(3, 3)))];
        let observed = derive(&HealthBoard::default(), &gateways, Instant::now());
        let gateway = get(&observed, GATEWAY_PROGRESSING);
        assert_eq!(gateway.status, ConditionStatus::False);
        assert_eq!(gateway.reason, "AsExpected");
        assert_eq!(gateway.message, "grid/gw: 3 of 3 pods on the current spec");
        assert_conforms(&observed);
    }

    #[test]
    fn a_gateway_without_a_deployment_is_not_an_error() {
        let gateways = [("grid/gw".to_owned(), None)];
        let observed = derive(&HealthBoard::default(), &gateways, Instant::now());
        let gateway = get(&observed, GATEWAY_PROGRESSING);
        assert_eq!(gateway.status, ConditionStatus::False);
        assert_eq!(gateway.message, "no Deployment found for grid/gw");
        assert_eq!(get(&observed, DEGRADED).status, ConditionStatus::False);
    }

    #[test]
    fn enrollment_and_a_missing_network_are_reported() {
        let now = Instant::now();
        let mut board = HealthBoard::default();
        let sites = get(&derive(&board, &[], now), GRID_SITES_PROGRESSING).clone();
        assert_eq!(
            sites.status,
            ConditionStatus::Unknown,
            "not joined without a GridNetwork"
        );
        assert_eq!(sites.reason, "NoGridNetwork");

        board.enrollment = Some(Enrollment::WaitingForGridNetwork {
            last: Some("finding the GridNetwork: no GridNetwork yet".to_owned()),
        });
        assert_eq!(
            get(&derive(&board, &[], now), GRID_SITES_PROGRESSING).message,
            "enrollment waits for a GridNetwork to name the grid; install grid-site \
             (last: finding the GridNetwork: no GridNetwork yet)"
        );

        board.enrollment = Some(Enrollment::Enrolling {
            site: "site-d".to_owned(),
            url: "https://hub/".to_owned(),
            last: Some("connection refused".to_owned()),
        });
        let enrolling = derive(&board, &[], now);
        assert_eq!(get(&enrolling, PROGRESSING).reason, "Enrolling");
        assert_eq!(
            get(&enrolling, GRID_SITES_PROGRESSING).message,
            "enrolling as site-d at https://hub/ (last: connection refused)"
        );
        assert_conforms(&enrolling);
    }

    #[test]
    fn an_enrolled_site_without_a_network_is_unavailable_not_degraded() {
        let board = HealthBoard {
            enrolled: Some("spiffe://grid/site/site-d".to_owned()),
            ..HealthBoard::default()
        };
        let enrolled = derive(&board, &[], Instant::now() + DEGRADED_GRACE);
        assert_eq!(
            get(&enrolled, GRID_SITES_AVAILABLE).message,
            "enrolled as spiffe://grid/site/site-d; no GridNetwork yet"
        );
        assert_eq!(get(&enrolled, AVAILABLE).status, ConditionStatus::False);
        assert_eq!(get(&enrolled, DEGRADED).status, ConditionStatus::False);
        assert_conforms(&enrolled);
    }

    #[test]
    fn a_mode_restart_is_progress() {
        let board = HealthBoard {
            restarting: Some("GridNetwork grid sets signalTransport=Poll peerTrust=Spiffe".to_owned()),
            ..HealthBoard::default()
        };
        let observed = derive(&board, &[], Instant::now());
        assert_eq!(get(&observed, GRID_SITES_PROGRESSING).reason, "RestartingForModes");
        assert_eq!(get(&observed, PROGRESSING).status, ConditionStatus::True);
        assert_conforms(&observed);
    }
}
