//! The `grid_site_route` filter: pick a cross-site cluster for a request.
//!
//! Reads the model header, chooses a site for that model from the current snapshot
//! by what each provider publishes, and sets `ctx.cluster` for the downstream load
//! balancer. Each site's load, requests held over capacity (rho), is resolved when
//! the snapshot is built, so a request only filters and picks. A request for an unknown
//! model gets 404; one naming no model, or with no healthy site, gets 503 with Retry-After;
//! one for a shed model gets 429 with Retry-After. Each carries an OpenAI-style error,
//! logged at debug. Every outcome counts in
//! `grid_route_decisions_total` by site and reason. A routed response names the
//! chosen site and cluster in `x-grid-site` and `x-grid-backend`.

//! Among two or more sites with room, prefix affinity first narrows to the sites holding the
//! request's prompt, unless their queue outweighs the match; selection runs over what it keeps.
//! A request naming a stored response or conversation goes to the site that stored it.

use std::{
    collections::HashSet,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use praxis_filter::{
    BodyAccess, BodyMode, FilterAction, FilterError, HttpFilter, HttpFilterContext, Rejection, TerminalResponse,
    parse_filter_config,
};
use serde::Deserialize;

use crate::{
    control::Tuning,
    decisions::{Refused, SiteDecisions},
    descriptor::{AdmissionState, CapabilityKind, RouteCandidate, validate_model_header},
    health::ClusterHealth,
    pin::{self, Collection, Pushed, StatePath, TagKey, Tagger},
    prefix::{self, Affinity, AffinitySettings, Api, PrefixAffinity, PrefixKeys, QueueGate, Queued},
    serving::AvailabilitySettings,
    snapshot::RouteSnapshot,
};

/// Default request header carrying the model name.
fn default_model_header() -> String {
    "X-Model".to_owned()
}

/// `grid_site_route` configuration as written in the praxis filter section.
///
/// One block per gateway: every block writes the same process-wide tuning, so a second
/// block with different settings silently replaces the first.
///
/// The model header, the declared clusters, and the plugin's tuning live here. The
/// candidate topology and the poller settings live in the grid serving config the
/// operator writes and the gateway reads, and the snapshot is injected, so the data
/// plane parses nothing about the control plane.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GridSiteRouteConfig {
    /// Request header naming the model (default `X-Model`).
    #[serde(default = "default_model_header")]
    model_header: String,

    /// The `load_balancer` clusters this gateway declares. When set, a candidate
    /// with no peer gateway and a cluster not listed has no route and is skipped.
    #[serde(default)]
    clusters: Option<Vec<DeclaredCluster>>,

    /// Site availability tuning. Every field has a default.
    #[serde(default)]
    availability: AvailabilitySettings,

    /// How strongly a conversation keeps to the site holding its prompt.
    #[serde(default)]
    prefix_affinity: AffinitySettings,
}

/// How a cluster's endpoints are dialed, as its `load_balancer` cluster declares.
///
/// Parsed and validated but unused: nothing probes endpoints in this filter yet, and a
/// config carrying a transport must still load rather than fail the gateway's start.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TransportKind {
    /// Grid mutual TLS with the site identity.
    MutualTls,
    /// Server-only TLS, verified with the cluster's CA.
    Tls,
    /// No TLS.
    Plaintext,
}

/// One declared cluster: a bare name, or a name with the transport its endpoints are dialed over.
#[derive(Debug)]
enum DeclaredCluster {
    /// Routable, not probed.
    Name(String),
    /// Routable, and carrying the transport its endpoints are dialed over.
    Probed(ProbedCluster),
}

// By hand, not untagged: untagged hides which field or transport was wrong.
impl<'de> Deserialize<'de> for DeclaredCluster {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = DeclaredCluster;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a cluster name or {name, transport}")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(DeclaredCluster::Name(v.to_owned()))
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                ProbedCluster::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                    .map(DeclaredCluster::Probed)
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

/// A declared cluster that names how its remote endpoints are dialed.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(
    dead_code,
    reason = "parsed so a config carrying a transport loads; nothing probes yet"
)]
struct ProbedCluster {
    /// The `load_balancer` cluster name.
    name: String,
    /// How the cluster dials its endpoints.
    transport: TransportKind,
    /// The CA a `tls` cluster verifies with.
    #[serde(default)]
    ca_path: Option<String>,
    /// The server name a `tls` cluster expects.
    #[serde(default)]
    sni: Option<String>,
}

impl DeclaredCluster {
    /// The cluster name.
    fn name(&self) -> &str {
        match self {
            Self::Name(name) | Self::Probed(ProbedCluster { name, .. }) => name,
        }
    }
}

/// Routes a request to a cross-site cluster by model, honouring live-load order.
#[derive(Debug)]
pub(crate) struct GridSiteRouteFilter {
    /// The resolved, pre-ordered candidate snapshot the control step swaps. Shared
    /// with the refresh loop, so every request reads the latest ordering.
    snapshot: Arc<ArcSwap<RouteSnapshot>>,

    /// Header the request carries the model name in.
    model_header: http::header::HeaderName,

    /// The replica's prefix index and the live affinity settings.
    affinity: Arc<PrefixAffinity>,

    /// Counts requests from a random start, so gateway replicas pick independently.
    turn: AtomicUsize,

    /// Where the filter publishes Praxis's health registry for the control step.
    health: Arc<ClusterHealth>,

    /// Declared clusters, `None` when the config does not list them.
    clusters: Option<HashSet<Arc<str>>>,
}

impl GridSiteRouteFilter {
    /// Build the filter from its config section over an injected snapshot, setting
    /// the block's `availability` and `prefix_affinity` into `tuning` for the control step.
    ///
    /// The gateway owns `snapshot` and its refresh loop, and the filter only reads it.
    ///
    /// # Errors
    ///
    /// Returns [`FilterError`] if the config fails to parse, the model header is
    /// invalid, or a tuning value is out of range.
    pub(crate) fn from_config(
        config: &serde_yaml::Value,
        snapshot: Arc<ArcSwap<RouteSnapshot>>,
        affinity: Arc<PrefixAffinity>,
        health: Arc<ClusterHealth>,
        tuning: &Tuning,
    ) -> Result<Box<dyn HttpFilter>, FilterError> {
        let cfg: GridSiteRouteConfig = parse_filter_config("grid_site_route", config)?;
        let model_header = validate_model_header(&cfg.model_header)?;
        let tuned = |error: String| -> FilterError { format!("grid_site_route: {error}").into() };
        cfg.availability.validate().map_err(tuned)?;
        cfg.prefix_affinity.validate().map_err(tuned)?;
        // A tag key the block names must load here, where praxis refuses a bad config, or every
        // serving reload after it would fail on the same path.
        drop(crate::control::load_tag_key(&cfg.prefix_affinity)?);
        tuning.set(cfg.availability, cfg.prefix_affinity);
        let declared = cfg.clusters;
        tracing::info!(
            "grid_site_route: choosing sites by measured in-flight against a learned ceiling: two choices among 3 or \
             more sites with room, a weighted pick between 2, and a capacity-weighted pick when none has room"
        );
        Ok(Box::new(Self {
            snapshot,
            model_header,
            affinity,
            turn: AtomicUsize::new(random_seed()),
            health,
            clusters: declared.map(|clusters| clusters.iter().map(|cluster| Arc::from(cluster.name())).collect()),
        }))
    }

    /// The site for a request for `model`, or the answer when no site can take it.
    ///
    /// Prefix affinity narrows selection to the sites holding the prompt, unless their queue
    /// outweighs the match; a new conversation keeps every site and gets the full spread.
    fn choose<'snap>(
        &self,
        snapshot: &'snap RouteSnapshot,
        model: &str,
        keys: Option<&PrefixKeys>,
    ) -> Result<Pick<'snap>, FilterAction> {
        let turn = self.turn.fetch_add(1, Ordering::Relaxed);
        if snapshot.shedding.contains(model) {
            return Err(refuse(Refused::Shed, model, turn));
        }
        let routable = |candidate: &RouteCandidate| has_route(candidate, self.clusters.as_ref());
        let settings = self.affinity.settings.load();
        let pick = if settings.enabled {
            let sticky = Sticky {
                affinity: Affinity {
                    index: &self.affinity.index,
                    keys,
                    settings: &settings,
                    turn,
                },
                gate: QueueGate {
                    queued_request_seconds: settings.queued_request_seconds,
                },
            };
            select_spread(snapshot, CapabilityKind::InferenceModel, model, turn, routable, &sticky)
        } else {
            select_spread(
                snapshot,
                CapabilityKind::InferenceModel,
                model,
                turn,
                routable,
                &KeepAll,
            )
        };
        pick.ok_or_else(|| refuse(unserved(snapshot, model, routable), model, turn))
    }

    /// Send the request to `pick`: count the decision, select its cluster, and record its prefix
    /// there for the response to confirm or withdraw.
    fn route(&self, ctx: &mut HttpFilterContext<'_>, pick: &Pick<'_>, keys: Option<PrefixKeys>) {
        pick.decisions.record(pick.fallback);
        let candidate = pick.candidate;
        let mut responses = false;
        with_state(ctx, |state| {
            responses = state.responses;
            state.served = Some((Arc::clone(&candidate.site), Arc::clone(&candidate.cluster)));
            if let Some(keys) = keys {
                self.affinity.index.record(&keys, &candidate.cluster);
                state.recorded = Some((keys, Arc::clone(&candidate.cluster)));
            }
        });
        if responses {
            // Ids are tagged in the body, so it must come back uncompressed.
            ctx.request_headers_to_remove.push(http::header::ACCEPT_ENCODING);
        }
        ctx.cluster = Some(Arc::clone(&candidate.cluster));
    }

    /// Route a request that names stored state to the site that stored it.
    ///
    /// `None` for a request that names none. A site no longer in the grid gets
    /// 404, since the state lives only there. One that admits no new request now
    /// gets 503.
    fn pin(&self, ctx: &mut HttpFilterContext<'_>, model: Option<&str>) -> Option<FilterAction> {
        let snapshot = self.snapshot.load();
        let key = self.affinity.tag_key.load();
        let (holder, rewrite) = match pinned_site(ctx, &snapshot, key.as_deref())? {
            Ok(pinned) => pinned,
            Err(answer) => return Some(answer),
        };
        let site = holder.as_ref().map(|holder| &holder.site);
        let candidate = match pinned_target(&snapshot, holder.as_ref(), model) {
            Target::Found(candidate) => candidate,
            Target::Gone => {
                tracing::debug!(site = ?site, "grid_site_route: the cluster holding the state left the grid");
                return Some(FilterAction::Reject(Rejection::status(404)));
            },
            Target::Unavailable => {
                tracing::debug!(site = ?site, "grid_site_route: the cluster holding the state admits no new request");
                return Some(FilterAction::Reject(
                    Rejection::status(503).with_header("retry-after", RETRY_AFTER_SECS),
                ));
            },
        };
        let cluster = Arc::clone(&candidate.cluster);
        with_state(ctx, |state| {
            state.responses = true;
            state.served = Some((Arc::clone(&candidate.site), Arc::clone(&candidate.cluster)));
        });
        ctx.cluster = Some(cluster);
        ctx.rewritten_path = rewrite.or_else(|| ctx.rewritten_path.take());
        // Ids are tagged in the body, so it must come back uncompressed.
        ctx.request_headers_to_remove.push(http::header::ACCEPT_ENCODING);
        Some(FilterAction::Continue)
    }

    /// A tagger for the ids of the answer `state` was served by.
    fn tagger(&self, state: &RouteState) -> Option<Tagger> {
        let (site, cluster) = state.served.as_ref()?;
        Some(Tagger::new(pin::tag(
            site,
            cluster,
            self.affinity.tag_key.load().as_deref(),
        )))
    }

    /// Confirm or withdraw the recorded prefix by the response status, and start
    /// tagging a Responses API answer's ids with the site that served it.
    fn settle(&self, ctx: &mut HttpFilterContext<'_>) {
        let head = ctx.response_header.as_deref();
        let success = head.is_some_and(|response| response.status.is_success());
        // A 429 can be one tenant's quota, so only a server error speaks for the site.
        let failed = head.is_some_and(|response| response.status.is_server_error());
        // A compressed body cannot be scanned, so only an identity body is tagged.
        let identity = head.is_some_and(|response| {
            response
                .headers
                .get(http::header::CONTENT_ENCODING)
                .is_none_or(|encoding| encoding.as_bytes().eq_ignore_ascii_case(b"identity"))
        });
        let Some(state) = ctx.get_filter_state_mut::<RouteState>() else {
            return;
        };
        // A success means the site took the request, not that it cached the prompt yet:
        // headers can precede scheduling. A site that failed loses the prefix.
        if let Some((keys, cluster)) = state.recorded.take() {
            if success {
                self.affinity.index.confirm(&keys, &cluster);
            } else if failed {
                self.affinity.index.evict(&keys, &cluster);
            } else {
                self.affinity.index.forget(&keys, &cluster);
            }
        }
        let tag = (state.responses && success && identity)
            .then(|| self.tagger(state))
            .flatten();
        let tagging = tag.is_some();
        state.tagger = tag;
        if let (true, Some(response)) = (tagging, ctx.response_header.as_mut()) {
            // Tagging lengthens the body, so the length is no longer known up front.
            response.headers.remove(http::header::CONTENT_LENGTH);
            ctx.response_headers_modified = true;
        }
    }
}

#[async_trait]
impl HttpFilter for GridSiteRouteFilter {
    fn name(&self) -> &'static str {
        "grid_site_route"
    }

    fn selects_cluster(&self) -> bool {
        true
    }

    fn request_body_access(&self) -> BodyAccess {
        // Write, to strip the site tag from a stored-state id.
        BodyAccess::ReadWrite
    }

    fn request_body_mode(&self) -> BodyMode {
        BodyMode::StreamBuffer {
            max_bytes: Some(BODY_LIMIT),
        }
    }

    fn needs_request_context(&self) -> bool {
        true
    }

    fn response_body_access(&self) -> BodyAccess {
        BodyAccess::ReadWrite
    }

    async fn on_request_body(
        &self,
        ctx: &mut HttpFilterContext<'_>,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
    ) -> Result<FilterAction, FilterError> {
        if end_of_stream {
            let enabled = self.affinity.settings.load().enabled;
            let key = self.affinity.tag_key.load_full();
            let path = ctx.request.uri.path();
            let state = if body.as_ref().is_some_and(|bytes| bytes.len() > INLINE_BODY) {
                // A large body can take tens of milliseconds to key, too long for a request worker.
                let (path, mut taken) = (path.to_owned(), body.take());
                let read = tokio::task::spawn_blocking(move || {
                    let state = read_body(&path, &mut taken, enabled, key.as_deref());
                    (state, taken)
                });
                let (state, read_back) = read
                    .await
                    .map_err(|error| -> FilterError { error.to_string().into() })?;
                *body = read_back;
                state
            } else {
                read_body(path, body, enabled, key.as_deref())
            };
            ctx.insert_filter_state(state);
        }
        Ok(FilterAction::Continue)
    }

    async fn on_request(&self, ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        self.health.observe(ctx.health_registry);
        // An earlier selecting filter wins; never override its choice.
        if ctx.cluster.is_some() || ctx.upstream.is_some() {
            return Ok(FilterAction::Continue);
        }
        // A copy of the request reference, so the model outlives writes to ctx.
        let request = ctx.request;
        let model = request
            .headers
            .get(&self.model_header)
            .and_then(|value| value.to_str().ok());
        if let Some(action) = self.pin(ctx, model) {
            return Ok(action);
        }
        let Some(model) = model else {
            // Nothing after this filter selects a cluster, so answer here rather than fail in the load balancer.
            tracing::debug!(path = %ctx.request.uri.path(), "grid_site_route: no model in the request");
            Refused::NoModel.record();
            return Ok(unrouted(400));
        };

        let snapshot = self.snapshot.load();
        let keys = ctx
            .get_filter_state_mut::<RouteState>()
            .and_then(|state| state.keys.take())
            .map(|keys| keys.for_model(model));
        match self.choose(&snapshot, model, keys.as_ref()) {
            Ok(pick) => {
                self.route(ctx, &pick, keys);
                Ok(FilterAction::Continue)
            },
            Err(answer) => Ok(answer),
        }
    }

    async fn on_response(&self, ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        self.settle(ctx);
        Ok(FilterAction::Continue)
    }

    fn on_response_body(
        &self,
        ctx: &mut HttpFilterContext<'_>,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
    ) -> Result<FilterAction, FilterError> {
        if let Some(tagger) = ctx
            .get_filter_state_mut::<RouteState>()
            .and_then(|state| state.tagger.as_mut())
            && let Pushed::Replaced(tagged) = tagger.push(body.as_deref().unwrap_or_default(), end_of_stream)
        {
            *body = (!tagged.is_empty()).then_some(tagged);
        }
        Ok(FilterAction::Continue)
    }
}

/// Most of a request body the filter buffers to read its prompt, as `model_to_header` does.
const BODY_LIMIT: usize = 10 << 20;

/// Largest body keyed on the request worker. Keying one this size takes under a
/// millisecond; 10 MiB of tiny messages takes about 30.
const INLINE_BODY: usize = 256 << 10;

/// Seconds a client should wait before retrying a request pinned to a site that admits none now.
const RETRY_AFTER_SECS: &str = "5";

/// What the request body says: its prefix keys when `affinity` is on, or the
/// site a stored response pins it to.
fn read_body(path: &str, body: &mut Option<Bytes>, affinity: bool, key: Option<&TagKey>) -> RouteState {
    let mut state = RouteState::default();
    let Some(bytes) = body.as_ref() else {
        return state;
    };
    if pin::state_path(path) == Some(StatePath::Create(Collection::Responses)) {
        state.responses = true;
        // One parse serves both pinning and the prompt's keys.
        let Some(request) = prefix::parse_responses(bytes) else {
            return state;
        };
        let pinned = pin::pinned_fields(bytes, request.previous_response_id, request.conversation, key);
        if pinned.is_none() && affinity {
            state.keys = prefix::responses_keys(&request);
        }
        if let Some(((site, mark), stripped)) = pinned {
            *body = Some(Bytes::from(stripped));
            state.pinned = Some(Holder { site, mark: Some(mark) });
        }
        return state;
    }
    state.keys = Api::from_path(path)
        .filter(|_| affinity)
        .and_then(|api| prefix::prefix_keys(api, bytes));
    state
}

/// This filter's per-request state. Praxis keeps one state per filter, so
/// everything the response needs lives here.
#[derive(Debug, Default)]
struct RouteState {
    /// The site and cluster the request went to, for tagging stored-state ids.
    served: Option<(Arc<str>, Arc<str>)>,
    /// The prompt's keys, before the model is folded in.
    keys: Option<PrefixKeys>,
    /// The keys recorded at the chosen cluster, for the response to confirm or withdraw.
    recorded: Option<(PrefixKeys, Arc<str>)>,
    /// Where a stored response pins the request.
    pinned: Option<Holder>,
    /// Whether the answer carries Responses API ids to tag.
    responses: bool,
    /// Tags the answer's ids with the site that served it.
    tagger: Option<Tagger>,
}

/// Run `change` on this request's state, created on first use.
fn with_state(ctx: &mut HttpFilterContext<'_>, change: impl FnOnce(&mut RouteState)) {
    if ctx.get_filter_state::<RouteState>().is_none() {
        ctx.insert_filter_state(RouteState::default());
    }
    if let Some(state) = ctx.get_filter_state_mut::<RouteState>() {
        change(state);
    }
}

/// Where stored state lives: its site, and its cluster's mark when an id names one.
#[derive(Clone, Debug)]
struct Holder {
    /// The site.
    site: Arc<str>,
    /// The [`pin::cluster_mark`] of the cluster, `None` for any at the site.
    mark: Option<String>,
}

/// Where a request's stored state pins it, and the path to send, or the answer
/// when the path names an id without a valid tag. A new conversation pins
/// nowhere, `None` inside, when the local site cannot take it, so it goes to the
/// front site that can. `None` outside for a request naming no stored state.
#[expect(clippy::type_complexity, reason = "one private call site")]
fn pinned_site(
    ctx: &HttpFilterContext<'_>,
    snapshot: &RouteSnapshot,
    key: Option<&TagKey>,
) -> Option<Result<(Option<Holder>, Option<String>), FilterAction>> {
    match pin::state_path(ctx.request.uri.path()) {
        Some(StatePath::Stored { collection, id, rest }) => Some(pin::untag(id, key).map_or_else(
            || {
                tracing::debug!("grid_site_route: a stored id without a valid tag");
                Err(FilterAction::Reject(Rejection::status(404)))
            },
            |pinned| {
                let rewrite = pin::upstream_path(collection, &pinned.upstream, rest);
                let holder = Holder {
                    site: Arc::from(pinned.site),
                    mark: Some(pinned.mark.to_owned()),
                };
                Ok((Some(holder), Some(rewrite)))
            },
        )),
        // A new conversation has no model to route by: keep it local when the local site can take it.
        Some(StatePath::Create(Collection::Conversations)) => {
            let local = Holder {
                site: Arc::clone(&snapshot.local_site),
                mark: None,
            };
            let admits = pinned_target(snapshot, Some(&local), None).found().is_some();
            Some(Ok((admits.then_some(local), None)))
        },
        _ => Some(Ok((Some(ctx.get_filter_state::<RouteState>()?.pinned.clone()?), None))),
    }
}

/// Where a pinned request can go.
enum Target<'snap> {
    /// A candidate at the site that admits it.
    Found(&'snap RouteCandidate),
    /// The site serves nothing that matches: it left the grid.
    Gone,
    /// The site is in the grid but admits no new request now.
    Unavailable,
}

impl<'snap> Target<'snap> {
    /// The candidate, when found.
    fn found(self) -> Option<&'snap RouteCandidate> {
        match self {
            Self::Found(candidate) => Some(candidate),
            Self::Gone | Self::Unavailable => None,
        }
    }
}

/// The front candidate at `holder`, anywhere when `None`, that admits a pinned
/// request, for `model` when named.
fn pinned_target<'snap>(snapshot: &'snap RouteSnapshot, holder: Option<&Holder>, model: Option<&str>) -> Target<'snap> {
    let mut at_site = snapshot
        .candidates
        .iter()
        .filter(|candidate| {
            holder.is_none_or(|holder| {
                *candidate.site == *holder.site
                    && holder
                        .mark
                        .as_deref()
                        .is_none_or(|mark| pin::cluster_mark(&candidate.cluster) == mark)
            }) && candidate.kind == CapabilityKind::InferenceModel
                && model.is_none_or(|model| &*candidate.name == model)
        })
        .peekable();
    if at_site.peek().is_none() {
        return Target::Gone;
    }
    at_site
        .find(|candidate| is_admitted_for_new_request(candidate.admission_state))
        .map_or(Target::Unavailable, Target::Found)
}

/// Count and answer a request no candidate took.
fn refuse(reason: Refused, model: &str, turn: usize) -> FilterAction {
    tracing::debug!(model = %model, reason = ?reason, "grid_site_route: no admitted candidate");
    reason.record();
    // Known but excluded everywhere is a temporary outage, not an unknown model.
    match reason {
        Refused::UnknownModel => error_response(
            404,
            "invalid_request_error",
            "model_not_found",
            "no site serves this model",
            None,
        ),
        // Load, not an outage: 429 is what an OpenAI client backs off and retries on.
        Refused::Shed => retry_later(
            turn,
            429,
            "rate_limit_exceeded",
            "capacity_exhausted",
            "every site serving this model is at capacity",
        ),
        Refused::NotReady | Refused::NoRoute | Refused::NoModel => retry_later(
            turn,
            503,
            "server_error",
            "no_healthy_site",
            "no healthy site serves this model now",
        ),
    }
}

/// Why no candidate took a request for `model`: unknown, unroutable, or excluded.
fn unserved(snapshot: &RouteSnapshot, model: &str, routable: impl Fn(&RouteCandidate) -> bool) -> Refused {
    let mut matches = snapshot
        .candidates
        .iter()
        .filter(|candidate| candidate.kind == CapabilityKind::InferenceModel && &*candidate.name == model)
        .peekable();
    if matches.peek().is_none() {
        return Refused::UnknownModel;
    }
    if matches.any(|candidate| is_admitted_for_new_request(candidate.admission_state) && !routable(candidate)) {
        Refused::NoRoute
    } else {
        Refused::NotReady
    }
}

/// The candidate `select_spread` chose and why.
#[derive(Debug)]
pub(crate) struct Pick<'snap> {
    /// The chosen candidate.
    pub(crate) candidate: &'snap RouteCandidate,
    /// Whether it was demoted, chosen only because no healthy site was left.
    pub(crate) fallback: bool,
    /// The chosen site's decision counters.
    pub(crate) decisions: &'snap SiteDecisions,
}

impl<'snap> Pick<'snap> {
    /// `site` as chosen, a `fallback` when no healthy site was left.
    const fn new((candidate, decisions, _): Site<'snap>, fallback: bool) -> Self {
        Self {
            candidate,
            fallback,
            decisions,
        }
    }
}

/// The router's own answer to a request it cannot route: a response, not a rejection,
/// since praxis logs every rejection at WARN.
fn unrouted(status: u16) -> FilterAction {
    FilterAction::TerminalResponse(Box::new(TerminalResponse::new(status)))
}

/// `status` with a Retry-After picked by `turn` and an OpenAI-style error naming `code`.
fn retry_later(turn: usize, status: u16, kind: &str, code: &str, message: &str) -> FilterAction {
    let retry_after = turn
        .checked_rem(RETRY_AFTER_BACKOFF.len())
        .and_then(|index| RETRY_AFTER_BACKOFF.get(index))
        .copied()
        .unwrap_or("5");
    error_response(status, kind, code, message, Some(retry_after))
}

/// A gateway-originated error: `status`, an OpenAI-style error object, and an optional Retry-After.
fn error_response(
    status: u16,
    kind: &str,
    code: &str,
    message: &str,
    retry_after: Option<&'static str>,
) -> FilterAction {
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    if let Some(retry_after) = retry_after {
        headers.insert(http::header::RETRY_AFTER, http::HeaderValue::from_static(retry_after));
    }
    // Every code and message is a constant here, so none needs JSON escaping.
    let body = format!(r#"{{"error":{{"message":"{message}","type":"{kind}","code":"{code}"}}}}"#);
    FilterAction::TerminalResponse(Box::new(
        TerminalResponse::new(status).with_headers(headers).with_body(body),
    ))
}

/// Whether a request routed to `candidate` has somewhere to go: a declared cluster.
/// With no declared list, every cluster is assumed to exist.
fn has_route(candidate: &RouteCandidate, clusters: Option<&HashSet<Arc<str>>>) -> bool {
    clusters.is_none_or(|clusters| clusters.contains(&candidate.cluster))
}

/// One site selection may choose, as a [`Narrow`] stage sees it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SiteView<'snap> {
    /// The candidate, with its saturation and whatever else a narrowing stage reads.
    pub(crate) candidate: &'snap RouteCandidate,
    /// The site's load score, queue depth, that the affinity gate weighs a match against.
    pub(crate) queue: f64,
}

/// A per-request stage that narrows the sites with room before selection.
///
/// It may only clear entries of `keep`, which start all true, so it never adds a site
/// that health, readiness, or shedding removed. Clearing every entry keeps them all.
pub(crate) trait Narrow {
    /// Clear `keep[i]` for each of `sites` selection should not consider.
    fn keep(&self, sites: &[SiteView<'_>], keep: &mut [bool]);
}

impl Queued for SiteView<'_> {
    fn cluster(&self) -> &str {
        &self.candidate.cluster
    }

    fn queue(&self) -> f64 {
        self.queue
    }
}

/// Keeps every site: selection by load alone.
pub(crate) struct KeepAll;

impl Narrow for KeepAll {
    fn keep(&self, _sites: &[SiteView<'_>], _keep: &mut [bool]) {}
}

/// Prefix affinity as a narrowing of selection: keeps the sites holding the request's prompt
/// unless their queue outweighs the match, and records the outcome.
struct Sticky<'req> {
    /// The request's keys against the replica's prefix index.
    affinity: Affinity<'req>,
    /// Weighs a match against the queue it would join.
    gate: QueueGate,
}

impl Narrow for Sticky<'_> {
    fn keep(&self, sites: &[SiteView<'_>], keep: &mut [bool]) {
        self.affinity.narrow(sites, keep, &self.gate).record();
    }
}

/// Most sites with room one request weighs. Past it, the rest wait for a later snapshot order.
const MAX_SITES: usize = 64;

/// Bounds of either site's share when two sites with room are picked between.
const TWO_SITE_SHARE: (f64, f64) = (0.1, 0.9);

/// A candidate with its decision counters.
type Site<'snap> = (&'snap RouteCandidate, &'snap SiteDecisions, f64);

/// Up to [`MAX_SITES`] sites, the first `len` filled.
struct Sites<'snap> {
    /// The sites, filled from the front.
    slots: [Option<Site<'snap>>; MAX_SITES],
    /// How many slots are filled.
    len: usize,
}

impl<'snap> Sites<'snap> {
    /// The first [`MAX_SITES`] of `sites`, warning once when more are offered.
    fn gather(sites: impl Iterator<Item = Site<'snap>>) -> Self {
        let mut gathered = Self {
            slots: [None; MAX_SITES],
            len: 0,
        };
        for site in sites {
            let Some(slot) = gathered.slots.get_mut(gathered.len) else {
                warn_past_max_sites();
                break;
            };
            *slot = Some(site);
            gathered.len = gathered.len.saturating_add(1);
        }
        gathered
    }

    /// The filled sites in order.
    fn iter(&self) -> impl Iterator<Item = Site<'snap>> + '_ {
        self.slots.iter().take(self.len).flatten().copied()
    }

    /// The site at `index`, if filled.
    fn get(&self, index: usize) -> Option<Site<'snap>> {
        self.slots.get(..self.len)?.get(index).copied().flatten()
    }
}

/// Choose a site for `name` among the admitted, `routable` matches.
///
/// Among healthy matches with room (rho below 1) that `narrow` keeps: two choices at
/// three or more, picked by capacity, taking the lower rho; a weighted pick between two,
/// by capacity over 1 + rho with each share clamped to 0.1..0.9; or the only one. With
/// none, a capacity-weighted pick over the healthy matches not full, or all of them when
/// every one is, tied on the best polled queue score (a prefix, since scores ascend), which
/// covers sites that publish no rho without herding. With no healthy match, the front
/// demoted one, as a fallback.
#[expect(
    clippy::too_many_arguments,
    reason = "the request path passes its inputs directly rather than build a struct per request"
)]
pub(crate) fn select_spread<'snap>(
    snapshot: &'snap RouteSnapshot,
    kind: CapabilityKind,
    name: &str,
    turn: usize,
    routable: impl Fn(&RouteCandidate) -> bool,
    narrow: &impl Narrow,
) -> Option<Pick<'snap>> {
    let matches = matching(snapshot, kind, name, &routable);
    let (front, front_score, front_decisions) = matches.clone().next()?;
    let site = |(candidate, score, decisions): Scored<'snap>| (candidate, decisions, score);
    let healthy = Sites::gather(matches.filter(|(_, score, _)| !score.is_nan()).map(site));
    if healthy.len == 0 {
        // Demotion scores a candidate NaN: every healthy site is gone, so try the front one.
        return Some(Pick::new((front, front_decisions, front_score), true));
    }
    // Affinity narrows the eligible set first; its gate weighs a match against load. The
    // pick then spreads over what it kept: the sites with room, or failing that the lightest.
    let kept = narrowed(&healthy, narrow);
    let room = Sites::gather(kept.iter().filter(|(candidate, ..)| has_room(candidate)));
    let band = || {
        // A site full for `full_after_ms` is the last resort: a site that has just drained
        // still carries its backlog in the window's worst queue reading, and would tie.
        let open = Sites::gather(kept.iter().filter(|(candidate, ..)| candidate.full != Some(true)));
        let pool = if open.len > 0 { open } else { kept };
        let best = pool.iter().map(|(.., score)| score).fold(f64::INFINITY, f64::min);
        Sites::gather(pool.iter().filter(|(.., score)| score.total_cmp(&best).is_eq()))
    };
    select(&room, band, turn).map(|chosen| Pick::new(chosen, false))
}

/// Seconds a client should wait before retrying a model with no admitted candidate:
/// about one operator scrape plus one peer poll, spread so retries do not arrive together.
const RETRY_AFTER_BACKOFF: [&str; 5] = ["3", "4", "5", "6", "7"];

/// A candidate with its score and decision counters.
type Scored<'snap> = (&'snap RouteCandidate, f64, &'snap SiteDecisions);

/// The admitted, `routable` candidates for `kind` and `name`, in snapshot order.
fn matching<'snap, 'req>(
    snapshot: &'snap RouteSnapshot,
    kind: CapabilityKind,
    name: &'req str,
    routable: &'req impl Fn(&RouteCandidate) -> bool,
) -> impl Iterator<Item = Scored<'snap>> + Clone + 'req
where
    'snap: 'req,
{
    snapshot
        .candidates
        .iter()
        .zip(&snapshot.scores)
        .zip(&snapshot.decisions)
        .map(|((candidate, score), decisions)| (candidate, *score, decisions))
        .filter(move |(candidate, ..)| {
            candidate.kind == kind
                && &*candidate.name == name
                && is_admitted_for_new_request(candidate.admission_state)
                && routable(candidate)
        })
}

/// The tier for `room` sites: two choices at three or more, a weighted pick at two, the
/// only one at one, and with none a capacity-weighted pick over `band`.
fn select<'snap>(room: &Sites<'snap>, band: impl FnOnce() -> Sites<'snap>, turn: usize) -> Option<Site<'snap>> {
    // Registered once; counting an arm is one atomic add on the request path.
    static SELECTIONS: LazyLock<[metrics::Counter; 4]> = LazyLock::new(|| {
        ["by_capacity", "lone", "between_two", "two_choices"]
            .map(|path| metrics::counter!("grid_route_selections_total", "path" => path))
    });
    let arm = room.len.min(3);
    if let Some(counter) = SELECTIONS.get(arm) {
        counter.increment(1);
    }
    match arm {
        0 => by_capacity(&band(), turn),
        1 => room.get(0),
        2 => Some(between_two(room.get(0)?, room.get(1)?, turn)),
        _ => two_choices(room, turn),
    }
}

/// Whether `candidate` publishes load and has room for one more request.
fn has_room(candidate: &RouteCandidate) -> bool {
    candidate.rho.is_some_and(|rho| rho < 1.0)
}

/// The sites `narrow` keeps of `room`, or all of them when it keeps none.
fn narrowed<'snap>(room: &Sites<'snap>, narrow: &impl Narrow) -> Sites<'snap> {
    let view = |(candidate, _, queue): Site<'snap>| SiteView { candidate, queue };
    // A stack array seeded from the first site: the room is never narrowed when empty.
    let Some(first) = room.get(0) else {
        return Sites::gather(room.iter());
    };
    let mut views = [view(first); MAX_SITES];
    for (slot, site) in views.iter_mut().zip(room.iter()) {
        *slot = view(site);
    }
    let mut keep = [true; MAX_SITES];
    if let (Some(views), Some(kept)) = (views.get(..room.len), keep.get_mut(..room.len)) {
        narrow.keep(views, kept);
    }
    if !keep.iter().take(room.len).any(|kept| *kept) {
        return Sites::gather(room.iter());
    }
    Sites::gather(room.iter().zip(keep).filter(|(_, kept)| *kept).map(|(site, _)| site))
}

/// A capacity-weighted pick over `sites`. Unpublished capacity weighs the mean of the published, or 1.
fn by_capacity<'snap>(sites: &Sites<'snap>, turn: usize) -> Option<Site<'snap>> {
    // The mean of what is published stands in for a site that publishes nothing, without a Vec.
    let (sum, count) = sites
        .iter()
        .filter_map(|(candidate, ..)| candidate.capacity)
        .fold((0.0_f64, 0.0_f64), |(sum, count), capacity| {
            (sum + capacity, count + 1.0)
        });
    let unpublished = if count > 0.0 { sum / count } else { 1.0 };
    weighted(sites, unit(turn, SALTS.0), None, |candidate| {
        candidate.capacity.unwrap_or(unpublished)
    })
}

/// A pick between two sites by capacity over 1 + rho, each share clamped to 0.1..0.9.
fn between_two<'snap>(first: Site<'snap>, second: Site<'snap>, turn: usize) -> Site<'snap> {
    let rate = |candidate: &RouteCandidate| candidate.capacity.unwrap_or(1.0) / (1.0 + candidate.rho.unwrap_or(0.0));
    let share = (rate(first.0) / (rate(first.0) + rate(second.0))).clamp(TWO_SITE_SHARE.0, TWO_SITE_SHARE.1);
    if unit(turn, SALTS.0) < share { first } else { second }
}

/// Two distinct picks from `sites` by capacity, taking the lower rho; exact ties go to the first.
fn two_choices<'snap>(sites: &Sites<'snap>, turn: usize) -> Option<Site<'snap>> {
    let capacity = |candidate: &RouteCandidate| candidate.capacity.unwrap_or(1.0);
    let first = weighted(sites, unit(turn, SALTS.0), None, capacity)?;
    let second = weighted(sites, unit(turn, SALTS.1), Some(first.0), capacity).unwrap_or(first);
    Some(if second.0.rho < first.0.rho { second } else { first })
}

/// The site `point` (in 0..1) lands on when `sites` are weighted by `weight`, skipping `skip`.
fn weighted<'snap>(
    sites: &Sites<'snap>,
    point: f64,
    skip: Option<&RouteCandidate>,
    weight: impl Fn(&RouteCandidate) -> f64,
) -> Option<Site<'snap>> {
    let eligible = || {
        sites
            .iter()
            .filter(|(candidate, ..)| skip.is_none_or(|skip| !std::ptr::eq(*candidate, skip)))
    };
    let mut target = point * eligible().map(|(candidate, ..)| weight(candidate)).sum::<f64>();
    let mut last = None;
    for site in eligible() {
        last = Some(site);
        target -= weight(site.0);
        if target < 0.0 {
            return Some(site);
        }
    }
    last
}

/// Salts that make a request's two picks independent.
const SALTS: (u64, u64) = (0x9E37_79B9_7F4A_7C15, 0xC2B2_AE3D_27D4_EB4F);

/// A uniform value in [0, 1) from `turn` and `salt`, mixed so consecutive turns do not correlate.
fn unit(turn: usize, salt: u64) -> f64 {
    let mut mixed = u64::try_from(turn).unwrap_or(u64::MAX) ^ salt;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^= mixed >> 31;
    // The top 53 bits as a fraction, built from two exact halves.
    let bits = mixed >> 11;
    let high = u32::try_from(bits >> 21).unwrap_or(0);
    let low = u32::try_from(bits & 0x1F_FFFF).unwrap_or(0);
    (f64::from(high) * 2_f64.powi(21) + f64::from(low)) / 2_f64.powi(53)
}

/// A random starting turn, so replicas started together do not pick in step.
fn random_seed() -> usize {
    use std::hash::BuildHasher as _;
    let hashed = std::collections::hash_map::RandomState::new().hash_one(std::time::SystemTime::now());
    usize::try_from(hashed).unwrap_or_else(|_| usize::try_from(hashed >> 32).unwrap_or(0))
}

/// Log once that a model has more sites than one request weighs.
fn warn_past_max_sites() {
    static WARNED: std::sync::Once = std::sync::Once::new();
    WARNED.call_once(|| {
        tracing::warn!(
            max = MAX_SITES,
            "grid_site_route: more sites than one request weighs; the rest wait for a later order"
        );
    });
}

/// Whether a candidate in this admission state accepts a new request.
fn is_admitted_for_new_request(state: AdmissionState) -> bool {
    matches!(state, AdmissionState::NewAndExisting)
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::min_ident_chars,
    clippy::type_complexity,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    reason = "tests"
)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::{
        descriptor::{CandidateConfig, validate_candidates},
        snapshot::{Inputs, Learned, decisions_for},
    };

    /// The pick for `kind` and `name` over `candidates` in their given order.
    fn front(candidates: Vec<RouteCandidate>, kind: CapabilityKind, name: &str) -> Option<String> {
        let snapshot = RouteSnapshot::from_static(candidates, Arc::from("east"));
        select_spread(&snapshot, kind, name, 0, |_| true, &KeepAll).map(|pick| pick.candidate.cluster.to_string())
    }

    /// A validated one-candidate list for `model` at `site`/`cluster`.
    fn one(model: &str, site: &str, cluster: &str, admission: AdmissionState) -> Vec<RouteCandidate> {
        let mut candidates = validate_candidates(vec![CandidateConfig {
            admission: AdmissionState::default(),
            cluster: cluster.to_owned(),
            credential: None,
            fresh: true,
            kind: CapabilityKind::InferenceModel,
            name: model.to_owned(),
            site: site.to_owned(),
        }])
        .unwrap();
        candidates[0].admission_state = admission;
        candidates
    }

    /// A filter over `snapshot` with affinity at `settings`.
    fn filter(snapshot: RouteSnapshot, settings: AffinitySettings) -> GridSiteRouteFilter {
        let affinity = PrefixAffinity::default();
        affinity.settings.store(Arc::new(settings));
        GridSiteRouteFilter {
            snapshot: Arc::new(ArcSwap::from_pointee(snapshot)),
            health: Arc::default(),
            clusters: None,
            model_header: http::header::HeaderName::from_static("x-model"),
            affinity: Arc::new(affinity),
            turn: AtomicUsize::new(0),
        }
    }

    /// `llama` at each `(site, queue, admission)`, ordered least-loaded first as the snapshot is.
    fn queued(sites: &[(&str, f64, AdmissionState)]) -> RouteSnapshot {
        let mut sorted = sites.to_vec();
        sorted.sort_by(|left, right| left.1.total_cmp(&right.1));
        let mut candidates = validate_candidates(
            sorted
                .iter()
                .map(|(site, ..)| CandidateConfig {
                    cluster: format!("pool-{site}"),
                    credential: None,
                    admission: AdmissionState::default(),
                    fresh: true,
                    kind: CapabilityKind::InferenceModel,
                    name: "llama".to_owned(),
                    site: (*site).to_owned(),
                })
                .collect(),
        )
        .unwrap();
        for (candidate, (_, _, admission)) in candidates.iter_mut().zip(&sorted) {
            candidate.admission_state = *admission;
        }
        RouteSnapshot {
            decisions: decisions_for(&candidates),
            scores: sorted.iter().map(|(_, queue, _)| *queue).collect(),
            candidates,
            local_site: Arc::from("a"),
            shedding: BTreeSet::new(),
        }
    }

    /// Affinity on, with no exploration so outcomes are deterministic.
    fn steady() -> AffinitySettings {
        AffinitySettings {
            exploration: 0.0,
            ..AffinitySettings::default()
        }
    }

    /// The site `filter` picks for a request whose prompt site `a` holds, as 40 keys.
    fn pick_for_a_prompt(filter: &GridSiteRouteFilter) -> Option<String> {
        let keys = PrefixKeys::from_keys((0..40).collect());
        filter.affinity.index.confirm(&keys, &Arc::from("pool-a"));
        let snapshot = filter.snapshot.load();
        filter
            .choose(&snapshot, "llama", Some(&keys))
            .ok()
            .map(|pick| pick.candidate.site.to_string())
    }

    const OPEN: AdmissionState = AdmissionState::NewAndExisting;

    #[test]
    fn a_conversation_stays_on_the_site_holding_its_prompt() {
        let filter = filter(queued(&[("a", 0.1, OPEN), ("b", 0.0, OPEN)]), steady());
        assert_eq!(
            pick_for_a_prompt(&filter).as_deref(),
            Some("a"),
            "b is lighter but holds nothing"
        );
    }

    #[test]
    fn a_much_deeper_queue_gives_way_to_the_lighter_site() {
        // 40 keys save 0.256s of prefill, 0.128 of a queued request at 2s each.
        let filter = filter(queued(&[("a", 1.0, OPEN), ("b", 0.0, OPEN)]), steady());
        assert_eq!(pick_for_a_prompt(&filter).as_deref(), Some("b"));
    }

    #[test]
    fn a_site_that_admits_nothing_never_takes_its_conversations() {
        let filter = filter(
            queued(&[("a", 0.0, AdmissionState::Excluded), ("b", 0.5, OPEN), ("c", 0.6, OPEN)]),
            steady(),
        );
        assert_eq!(pick_for_a_prompt(&filter).as_deref(), Some("b"));
    }

    #[test]
    fn affinity_off_routes_on_load_alone() {
        let off = AffinitySettings {
            enabled: false,
            ..steady()
        };
        let filter = filter(queued(&[("a", 0.1, OPEN), ("b", 0.0, OPEN)]), off);
        assert_eq!(pick_for_a_prompt(&filter).as_deref(), Some("b"));
        let mut body = Some(Bytes::from_static(br#"{"messages":[{"role":"user","content":"hi"}]}"#));
        assert!(
            read_body("/v1/chat/completions", &mut body, false, None).keys.is_none(),
            "no prompt is read"
        );
    }

    /// `site`, at the cluster named `cluster` when given.
    fn holder(site: &str, cluster: Option<&str>) -> Holder {
        Holder {
            site: Arc::from(site),
            mark: cluster.map(pin::cluster_mark),
        }
    }

    #[test]
    fn a_pinned_site_that_left_is_gone_and_one_that_admits_nothing_is_unavailable() {
        let snapshot = queued(&[("a", 0.0, OPEN), ("b", 0.0, AdmissionState::Excluded)]);
        assert!(matches!(
            pinned_target(&snapshot, Some(&holder("a", Some("pool-a"))), Some("llama")),
            Target::Found(candidate) if &*candidate.site == "a"
        ));
        assert!(matches!(
            pinned_target(&snapshot, Some(&holder("b", Some("pool-b"))), Some("llama")),
            Target::Unavailable
        ));
        assert!(matches!(
            pinned_target(&snapshot, Some(&holder("gone", None)), None),
            Target::Gone
        ));
        assert!(matches!(
            pinned_target(&snapshot, Some(&holder("a", Some("pool-a"))), Some("granite")),
            Target::Gone
        ));
        assert!(
            matches!(
                pinned_target(&snapshot, Some(&holder("a", Some("pool-left"))), None),
                Target::Gone
            ),
            "the cluster that stored it left, though the site did not"
        );
    }

    #[test]
    fn a_pin_without_a_model_reaches_the_cluster_that_stored_it() {
        let mut candidates = validate_candidates(
            [("llama", "pool-llama"), ("granite", "pool-granite")]
                .into_iter()
                .map(|(name, cluster)| CandidateConfig {
                    cluster: cluster.to_owned(),
                    credential: None,
                    admission: AdmissionState::default(),
                    fresh: true,
                    kind: CapabilityKind::InferenceModel,
                    name: name.to_owned(),
                    site: "a".to_owned(),
                })
                .collect(),
        )
        .unwrap();
        for candidate in &mut candidates {
            candidate.admission_state = OPEN;
        }
        let snapshot = RouteSnapshot::from_static(candidates, Arc::from("a"));
        let stored = holder("a", Some("pool-granite"));
        assert!(matches!(
            pinned_target(&snapshot, Some(&stored), None),
            Target::Found(candidate) if &*candidate.cluster == "pool-granite"
        ));
    }
    /// Three tied candidates for `m`, at sites a, b and d.
    fn three() -> Vec<RouteCandidate> {
        ["a", "b", "d"]
            .iter()
            .flat_map(|site| one("m", site, &format!("pool-{site}"), AdmissionState::NewAndExisting))
            .collect()
    }

    fn pick(snapshot: &RouteSnapshot, turn: usize) -> String {
        select_spread(snapshot, CapabilityKind::InferenceModel, "m", turn, |_| true, &KeepAll)
            .map(|chosen| chosen.candidate.cluster.to_string())
            .unwrap()
    }

    #[test]
    fn tied_candidates_split_evenly() {
        let snapshot = RouteSnapshot::from_static(three(), Arc::from("hub"));
        let picks: Vec<String> = (0..3_000).map(|turn| pick(&snapshot, turn)).collect();
        for cluster in ["pool-a", "pool-b", "pool-d"] {
            let taken = picks.iter().filter(|p| *p == cluster).count();
            assert!((850..=1_150).contains(&taken), "{cluster} took {taken} of 3000");
        }
    }

    /// The documented filter block, as praxis hands it to the factory.
    fn documented_example() -> serde_yaml::Value {
        let yaml = include_str!("../../../examples/gateway/grid-site-route.yaml");
        let filters: Vec<serde_yaml::Value> = serde_yaml::from_str(yaml).expect("the example is a filter list");
        // The example also carries the load_balancer block that follows selection.
        filters
            .into_iter()
            .find(|filter| filter.get("filter").and_then(serde_yaml::Value::as_str) == Some("grid_site_route"))
            .expect("the example has a grid_site_route block")
    }

    #[test]
    fn the_documented_example_loads_with_shedding_on() {
        let cfg: GridSiteRouteConfig =
            parse_filter_config("grid_site_route", &documented_example()).expect("the example parses");
        cfg.availability
            .validate()
            .expect("the example's availability block validates");
        cfg.prefix_affinity
            .validate()
            .expect("the example's affinity block validates");
        assert!(cfg.availability.shedding, "the example shows shedding switched on");
        assert_eq!(
            cfg.availability,
            AvailabilitySettings {
                shedding: true,
                ..AvailabilitySettings::default()
            },
            "the rest is the defaults"
        );
        assert_eq!(cfg.prefix_affinity, AffinitySettings::default());
        assert_eq!(cfg.model_header, "X-Gateway-Model-Name");
    }

    #[test]
    fn building_the_filter_sets_its_tuning_and_refuses_a_bad_value() {
        let tuning = Arc::new(Tuning::default());
        let build = |config: &serde_yaml::Value| {
            GridSiteRouteFilter::from_config(
                config,
                Arc::new(ArcSwap::from_pointee(RouteSnapshot::from_static(
                    Vec::new(),
                    Arc::from("hub"),
                ))),
                Arc::default(),
                Arc::default(),
                &tuning,
            )
        };
        assert!(build(&documented_example()).is_ok(), "the example builds");
        assert_eq!(tuning.generation(), 1);
        assert!(tuning.availability().shedding, "the block's availability is the tuning");

        let bad: serde_yaml::Value = serde_yaml::from_str("availability: {full_after_ms: -1}").expect("yaml");
        let error = build(&bad).err().expect("a value out of range is refused");
        assert!(error.to_string().contains("full_after_ms"), "{error}");
        assert_eq!(tuning.generation(), 1, "a refused block sets nothing");

        let missing: serde_yaml::Value =
            serde_yaml::from_str("prefix_affinity: {tag_key_path: /nonexistent/grid-tag-key}").expect("yaml");
        let refused = build(&missing)
            .err()
            .expect("a tag key that does not load is refused here, not at every reload");
        assert!(refused.to_string().contains("tag key"), "{refused}");
        assert_eq!(tuning.generation(), 1, "a refused block sets nothing");
    }

    #[test]
    fn a_declared_cluster_is_a_name_or_a_name_with_its_transport() {
        let cfg: GridSiteRouteConfig = serde_yaml::from_str(
            "clusters:\n  - site-a\n  - name: east\n    transport: tls\n    ca_path: /etc/ca.crt\n    sni: east.svc\n  - name: west\n    transport: plaintext\n",
        )
        .expect("a config carrying transports still loads");
        let declared = cfg.clusters.expect("declared");
        let names: Vec<&str> = declared.iter().map(DeclaredCluster::name).collect();
        assert_eq!(names, ["site-a", "east", "west"], "every declared cluster is routable");
        let unknown = serde_yaml::from_str::<GridSiteRouteConfig>("clusters:\n  - name: east\n    transport: quic\n")
            .expect_err("an unknown transport is rejected");
        assert!(unknown.to_string().contains("quic"), "{unknown}");
        assert!(
            serde_yaml::from_str::<GridSiteRouteConfig>("clusters:\n  - name: east\n    transport: tls\n    ca: /x\n")
                .is_err(),
            "an unknown key is rejected"
        );
    }

    #[test]
    fn a_candidate_with_no_peer_gateway_and_no_declared_cluster_has_no_route() {
        let mut candidates = one("llama", "site-d", "site-d", AdmissionState::NewAndExisting);
        candidates.extend(one("llama", "dagobah", "site-a", AdmissionState::NewAndExisting));
        let snapshot = RouteSnapshot::from_static(candidates, Arc::from("dagobah"));
        let declared: HashSet<Arc<str>> = HashSet::from([Arc::from("site-a")]);
        for turn in 0..4 {
            let picked = select_spread(
                &snapshot,
                CapabilityKind::InferenceModel,
                "llama",
                turn,
                |c| has_route(c, Some(&declared)),
                &KeepAll,
            )
            .expect("site-a is routable")
            .candidate;
            assert_eq!(
                &*picked.cluster, "site-a",
                "site-d is not a declared cluster, turn {turn}"
            );
        }
        assert!(
            snapshot.candidates.iter().all(|c| has_route(c, None)),
            "with no declared list, every cluster is assumed to exist"
        );
        let undeclared = one("llama", "site-d", "site-d", AdmissionState::NewAndExisting).remove(0);
        assert!(
            !has_route(&undeclared, Some(&declared)),
            "a cluster the config never declared is no route"
        );
    }

    /// Whether the pick at `turn` for `llama` over `snapshot` was a fallback.
    fn fallback(snapshot: &RouteSnapshot, turn: usize) -> Option<bool> {
        select_spread(
            snapshot,
            CapabilityKind::InferenceModel,
            "llama",
            turn,
            |_| true,
            &KeepAll,
        )
        .map(|pick| pick.fallback)
    }

    #[test]
    fn a_pick_is_a_fallback_only_when_no_healthy_site_is_left() {
        assert_eq!(fallback(&two_sites(Some(1.0), Some(5.0)), 0), Some(false));
        assert_eq!(fallback(&two_sites(Some(2.0), Some(2.0)), 1), Some(false));
        let alone = RouteSnapshot::from_static(
            one("llama", "east", "pool-a", AdmissionState::NewAndExisting),
            Arc::from("east"),
        );
        assert_eq!(fallback(&alone, 0), Some(false));
        let down = BTreeSet::from([Arc::from("pool-a"), Arc::from("pool-b")]);
        assert_eq!(fallback(&two_sites(Some(1.0), Some(5.0)).demote(&down), 0), Some(true));
        // One cluster left healthy is a routed pick, not a fallback.
        let one = BTreeSet::from([Arc::from("pool-a")]);
        assert_eq!(fallback(&two_sites(Some(1.0), Some(5.0)).demote(&one), 0), Some(false));
    }

    #[test]
    fn an_unserved_request_names_why() {
        let mut candidates = one("llama", "east", "pool-a", AdmissionState::Excluded);
        candidates.extend(one("llama", "west", "pool-b", AdmissionState::NewAndExisting));
        let snapshot = RouteSnapshot::from_static(candidates, Arc::from("east"));
        assert_eq!(unserved(&snapshot, "granite", |_| true), Refused::UnknownModel);
        assert_eq!(unserved(&snapshot, "llama", |_| false), Refused::NoRoute);
        let excluded = RouteSnapshot::from_static(
            one("llama", "east", "pool-a", AdmissionState::Excluded),
            Arc::from("east"),
        );
        assert_eq!(unserved(&excluded, "llama", |_| false), Refused::NotReady);
    }

    #[test]
    fn front_match_is_selected() {
        let candidates = one("llama", "east", "pool-a", AdmissionState::NewAndExisting);
        assert_eq!(
            front(candidates, CapabilityKind::InferenceModel, "llama").as_deref(),
            Some("pool-a")
        );
    }

    #[test]
    fn a_different_model_does_not_match() {
        let candidates = one("llama", "east", "pool-a", AdmissionState::NewAndExisting);
        assert!(front(candidates, CapabilityKind::InferenceModel, "granite").is_none());
    }

    #[test]
    fn an_excluded_candidate_is_skipped() {
        let candidates = one("llama", "east", "pool-a", AdmissionState::Excluded);
        assert!(front(candidates, CapabilityKind::InferenceModel, "llama").is_none());
    }

    #[test]
    fn an_mcp_kind_does_not_match_an_inference_query() {
        let candidates = one("llama", "east", "pool-a", AdmissionState::NewAndExisting);
        assert!(front(candidates, CapabilityKind::McpTool, "llama").is_none());
    }

    /// A snapshot of `llama` on east and west, with the given queue depths.
    fn two_sites(east: Option<f64>, west: Option<f64>) -> RouteSnapshot {
        let store = grid_signals::LoadStore::new(std::time::Duration::from_secs(60));
        for (site, cluster, load) in [("east", "pool-a", east), ("west", "pool-b", west)] {
            if let Some(value) = load {
                let line = format!(
                    r#"{}{{grid_site="{site}",grid_provider="{cluster}"}} {value} 1000"#,
                    crate::signals::llm_d::QUEUE_METRIC
                );
                store.ingest_at(&line, 1_000, 1_000, site);
            }
        }
        let mut candidates = one("llama", "east", "pool-a", AdmissionState::NewAndExisting);
        candidates.extend(one("llama", "west", "pool-b", AdmissionState::NewAndExisting));
        RouteSnapshot::from_store(
            candidates,
            Arc::from("east"),
            &mut Inputs {
                signals: &store,
                now_ms: 1_000,
                window_ms: 30_000,
                availability: &AvailabilitySettings::default(),
                learned: &mut Learned::default(),
            },
        )
    }

    fn picks(snapshot: &RouteSnapshot) -> Vec<String> {
        (0..2_000)
            .map(|turn| {
                let chosen = select_spread(
                    snapshot,
                    CapabilityKind::InferenceModel,
                    "llama",
                    turn,
                    |_| true,
                    &KeepAll,
                )
                .unwrap()
                .candidate;
                chosen.site.to_string()
            })
            .collect()
    }

    #[test]
    fn equal_scores_spread_across_the_tied_candidates() {
        for (snapshot, case) in [
            (two_sites(None, None), "unmeasured"),
            (two_sites(Some(5.0), Some(5.0)), "measured"),
        ] {
            let east = picks(&snapshot).iter().filter(|site| *site == "east").count();
            assert!((850..=1_150).contains(&east), "{case}: east took {east} of 2000");
        }
    }

    #[test]
    fn a_better_score_always_wins() {
        assert_eq!(picks(&two_sites(Some(9.0), Some(1.0))), ["west"; 2_000]);
        assert_eq!(
            picks(&two_sites(Some(1.0), None)),
            ["east"; 2_000],
            "measured beats unmeasured"
        );
    }

    #[test]
    fn an_excluded_tie_is_never_picked() {
        let mut snapshot = two_sites(None, None);
        snapshot.candidates[1].admission_state = AdmissionState::Excluded;
        assert_eq!(picks(&snapshot), ["east"; 2_000]);
    }

    /// `value` running per endpoint on one ready endpoint at `at`, as the EPP publishes it.
    fn publish_running(store: &grid_signals::LoadStore, site: &str, cluster: &str, value: f64, at: i64) {
        let labels = format!(r#"grid_site="{site}",grid_provider="{cluster}""#);
        let line = format!(
            "llm_d_epp_average_running_requests{{{labels}}} {value} {at}\nllm_d_epp_ready_endpoints{{{labels}}} 1 {at}"
        );
        store.ingest_at(&line, at, at, site);
    }

    /// Sites publishing (in flight, capacity), each `None` to publish nothing, as one snapshot.
    fn loaded(sites: &[(&str, Option<f64>, Option<f64>)]) -> RouteSnapshot {
        let store = grid_signals::LoadStore::new(std::time::Duration::from_secs(60));
        let mut candidates = Vec::new();
        for (site, in_flight, capacity) in sites {
            let cluster = format!("pool-{site}");
            // Teach the ceiling at 1s, then publish what the site holds now at 2s.
            for (value, at) in [(capacity, 1_000), (in_flight, 2_000)] {
                if let Some(value) = value {
                    publish_running(&store, site, &cluster, *value, at);
                }
            }
            candidates.extend(one("llama", site, &cluster, AdmissionState::NewAndExisting));
        }
        // Exact: a sample moves saturation all the way, and the floor is one request.
        let availability = AvailabilitySettings {
            smoothing: 1.0,
            ceiling_floor: 1.0,
            ..AvailabilitySettings::default()
        };
        let mut learned = Learned::default();
        let mut inputs = Inputs {
            signals: &store,
            now_ms: 1_000,
            window_ms: 500,
            availability: &availability,
            learned: &mut learned,
        };
        RouteSnapshot::from_store(candidates.clone(), Arc::from("hub"), &mut inputs);
        inputs.now_ms = 2_000;
        RouteSnapshot::from_store(candidates, Arc::from("hub"), &mut inputs)
    }

    /// How many of `requests` each site took.
    fn shares(
        snapshot: &RouteSnapshot,
        requests: usize,
        narrow: &impl Narrow,
    ) -> std::collections::BTreeMap<String, usize> {
        let mut taken = std::collections::BTreeMap::new();
        for turn in 0..requests {
            let chosen = select_spread(
                snapshot,
                CapabilityKind::InferenceModel,
                "llama",
                turn,
                |_| true,
                narrow,
            )
            .unwrap();
            *taken.entry(chosen.candidate.site.to_string()).or_insert(0) += 1;
        }
        taken
    }

    #[test]
    fn two_choices_prefer_the_lighter_site_and_never_a_full_one() {
        let snapshot = loaded(&[
            ("a", Some(2.0), Some(10.0)),
            ("b", Some(8.0), Some(10.0)),
            ("c", Some(5.0), Some(10.0)),
            ("d", Some(10.0), Some(10.0)),
        ]);
        let taken = shares(&snapshot, 3_000, &KeepAll);
        assert_eq!(taken.get("d"), None, "rho 1 has no room");
        let (a, c) = (taken["a"], taken["c"]);
        assert!(a > c && c > 0, "the lighter site takes more: a {a}, c {c}");
        assert_eq!(
            taken.get("b"),
            None,
            "two distinct picks never choose the heaviest of three"
        );
    }

    #[test]
    fn two_sites_split_by_capacity_over_load_within_a_tenth_and_nine_tenths() {
        let even = shares(
            &loaded(&[("a", Some(1.0), Some(10.0)), ("b", Some(1.0), Some(10.0))]),
            4_000,
            &KeepAll,
        );
        assert!(
            (1_800..=2_200).contains(&even["a"]),
            "equal sites split evenly: {even:?}"
        );
        let lopsided = shares(
            &loaded(&[("a", Some(0.0), Some(100.0)), ("b", Some(9.0), Some(10.0))]),
            4_000,
            &KeepAll,
        );
        assert!(
            (280..=520).contains(&lopsided["b"]),
            "the clamp keeps a tenth for the slow site: {lopsided:?}"
        );
    }

    #[test]
    fn a_lone_site_with_room_takes_every_request() {
        let snapshot = loaded(&[("a", Some(12.0), Some(10.0)), ("b", Some(3.0), Some(10.0))]);
        assert_eq!(shares(&snapshot, 200, &KeepAll).get("b"), Some(&200));
    }

    #[test]
    fn with_no_room_anywhere_the_overflow_draws_by_capacity() {
        let snapshot = loaded(&[("a", Some(30.0), Some(30.0)), ("b", Some(10.0), Some(10.0))]);
        let taken = shares(&snapshot, 4_000, &KeepAll);
        assert!(
            (2_700..=3_300).contains(&taken["a"]),
            "a has three quarters of the capacity: {taken:?}"
        );
    }

    #[test]
    fn sites_with_equal_ceilings_and_equal_load_are_drawn_evenly() {
        let snapshot = loaded(&[
            ("a", Some(50.0), Some(100.0)),
            ("b", Some(50.0), Some(100.0)),
            ("c", Some(50.0), Some(100.0)),
        ]);
        let taken = shares(&snapshot, 3_000, &KeepAll);
        for site in ["a", "b", "c"] {
            assert!(
                (850..=1_150).contains(&taken[site]),
                "equal ceilings, equal load: {taken:?}"
            );
        }
    }

    #[test]
    fn a_demoted_site_is_never_drawn_while_another_is_healthy() {
        let down = BTreeSet::from([Arc::from("pool-a")]);
        let snapshot = loaded(&[
            ("a", Some(0.0), Some(100.0)),
            ("b", Some(5.0), Some(10.0)),
            ("c", Some(5.0), Some(10.0)),
        ])
        .demote(&down);
        assert_eq!(shares(&snapshot, 500, &KeepAll).get("a"), None);
    }

    /// Keeps only site `b`, or clears everything when `all` is set.
    struct Only {
        all: bool,
    }

    impl Narrow for Only {
        fn keep(&self, sites: &[SiteView<'_>], keep: &mut [bool]) {
            for (site, kept) in sites.iter().zip(keep) {
                *kept = !self.all && &*site.candidate.site == "b" && site.candidate.rho.unwrap_or(0.0) < 1.0;
            }
        }
    }

    #[test]
    fn a_narrowing_stage_can_only_remove_sites_and_clearing_all_keeps_all() {
        let snapshot = loaded(&[
            ("a", Some(1.0), Some(10.0)),
            ("b", Some(5.0), Some(10.0)),
            ("c", Some(12.0), Some(10.0)),
        ]);
        assert_eq!(shares(&snapshot, 300, &Only { all: false }).get("b"), Some(&300));
        let taken = shares(&snapshot, 300, &Only { all: true });
        assert_eq!(taken.get("c"), None, "a full site is never added back");
        assert!(taken.contains_key("a") && taken.contains_key("b"), "{taken:?}");
    }

    #[test]
    fn a_shed_answers_429_and_an_outage_503_each_with_an_openai_error() {
        // Load: an OpenAI client backs off on 429 and retries.
        let shed = answered(Refused::Shed);
        assert_eq!(shed.0, 429);
        assert!(shed.1, "a shed says when to come back");
        assert!(
            shed.2.contains(r#""code":"capacity_exhausted""#) && shed.2.contains(r#""type":"rate_limit_exceeded""#),
            "{}",
            shed.2
        );
        // An outage is not load, so it stays 503.
        let outage = answered(Refused::NotReady);
        assert_eq!(outage.0, 503);
        assert!(outage.1);
        assert!(
            outage.2.contains(r#""code":"no_healthy_site""#) && outage.2.contains(r#""type":"server_error""#),
            "{}",
            outage.2
        );
        // An unknown model is the caller's error, with nothing to retry.
        let unknown = answered(Refused::UnknownModel);
        assert_eq!(unknown.0, 404);
        assert!(!unknown.1, "a model that does not exist will not appear");
        assert!(unknown.2.contains(r#""code":"model_not_found""#), "{}", unknown.2);
    }

    #[test]
    fn retry_after_spreads_across_turns_so_retries_do_not_arrive_together() {
        let values: BTreeSet<String> = (0..RETRY_AFTER_BACKOFF.len())
            .map(|turn| match refuse(Refused::Shed, "llama", turn) {
                FilterAction::TerminalResponse(response) => response
                    .headers
                    .get(http::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned)
                    .expect("a shed says when to come back"),
                FilterAction::Continue
                | FilterAction::Reject(_)
                | FilterAction::StreamingTerminalResponse(_)
                | FilterAction::Release
                | FilterAction::BodyDone => panic!("a refusal is answered here"),
            })
            .collect();
        assert_eq!(
            values.len(),
            RETRY_AFTER_BACKOFF.len(),
            "one value per turn, all different: {values:?}"
        );
        assert!(values.iter().all(|value| RETRY_AFTER_BACKOFF.contains(&value.as_str())));
    }

    /// How `reason` is answered: status, whether it says when to retry, and its body.
    fn answered(reason: Refused) -> (u16, bool, String) {
        match refuse(reason, "llama", 0) {
            FilterAction::TerminalResponse(response) => {
                let body = String::from_utf8(response.body.clone().unwrap_or_default().to_vec()).unwrap();
                (
                    response.status,
                    response.headers.contains_key(http::header::RETRY_AFTER),
                    body,
                )
            },
            FilterAction::Continue
            | FilterAction::Reject(_)
            | FilterAction::StreamingTerminalResponse(_)
            | FilterAction::Release
            | FilterAction::BodyDone => panic!("a refusal is answered here"),
        }
    }

    #[test]
    fn draws_are_uniform_and_seeds_differ() {
        let mean = (0..10_000).map(|turn| unit(turn, SALTS.0)).sum::<f64>() / 10_000.0;
        assert!((0.48..=0.52).contains(&mean), "{mean}");
        assert!((0..10_000).all(|turn| (0.0..1.0).contains(&unit(turn, SALTS.1))));
        assert_ne!(random_seed(), random_seed());
    }
}
