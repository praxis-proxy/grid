//! The `grid_site_route` filter: pick a cross-site cluster for a request.
//!
//! Reads the model header, finds the front admitted candidate for that model in
//! the current snapshot, and sets `ctx.cluster` for the downstream load
//! balancer. Selection is `select_admitted` over a pre-ordered list. The
//! ordering by live load happens off the request path in `snapshot`.
//!
//! Among two or more admitted candidates, prefix affinity first narrows to the
//! sites holding the request's prompt, unless their queue outweighs the match.
//! A request naming a stored response or conversation goes to the site that
//! stored it.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use praxis_filter::{
    BodyAccess, BodyMode, FilterAction, FilterError, HttpFilter, HttpFilterContext, Rejection, parse_filter_config,
};
use serde::Deserialize;

use crate::{
    descriptor::{AdmissionState, CapabilityKind, RouteCandidate, validate_model_header},
    pin::{self, Collection, Pushed, StatePath, TagKey, Tagger},
    prefix::{self, Affinity, AffinitySettings, Api, PrefixAffinity, PrefixKeys, QueueGate, Site},
    snapshot::RouteSnapshot,
};

/// Default request header carrying the model name.
fn default_model_header() -> String {
    "X-Model".to_owned()
}

/// `grid_site_route` configuration as written in the praxis filter section.
///
/// Only the model header lives here. The candidate topology and the poller
/// settings live in the grid serving config the gateway reads, and the snapshot
/// is injected, so the data plane parses nothing about the control plane.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GridSiteRouteConfig {
    /// Request header naming the model (default `X-Model`).
    #[serde(default = "default_model_header")]
    model_header: String,
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

    /// Counts requests, so each draws its own exploration turn.
    turn: AtomicUsize,
}

impl GridSiteRouteFilter {
    /// Build the filter from its config section over an injected snapshot.
    ///
    /// The gateway owns `snapshot` and its refresh loop, and the filter only reads it.
    ///
    /// # Errors
    ///
    /// Returns [`FilterError`] if the config fails to parse or the model header is
    /// invalid.
    pub(crate) fn from_config(
        config: &serde_yaml::Value,
        snapshot: Arc<ArcSwap<RouteSnapshot>>,
        affinity: Arc<PrefixAffinity>,
    ) -> Result<Box<dyn HttpFilter>, FilterError> {
        let cfg: GridSiteRouteConfig = parse_filter_config("grid_site_route", config)?;
        let model_header = validate_model_header(&cfg.model_header)?;
        Ok(Box::new(Self {
            snapshot,
            model_header,
            affinity,
            turn: AtomicUsize::new(0),
        }))
    }

    /// The candidate for `model`: the front admitted one, or among two or more,
    /// the front one affinity keeps.
    fn choose<'snap>(
        &self,
        snapshot: &'snap RouteSnapshot,
        model: &str,
        keys: Option<&PrefixKeys>,
    ) -> Option<&'snap RouteCandidate> {
        let settings = self.affinity.settings.load();
        let front = select_admitted(&snapshot.candidates, CapabilityKind::InferenceModel, model);
        if !settings.enabled {
            return front;
        }
        let admitted: Vec<(&RouteCandidate, f64)> = snapshot
            .candidates
            .iter()
            .zip(&snapshot.loads)
            .filter(|(candidate, _)| admits(candidate, CapabilityKind::InferenceModel, model))
            .map(|(candidate, load)| (candidate, *load))
            .collect();
        // With one site there is nothing to prefer, and no outcome to count.
        if admitted.len() < 2 {
            return front;
        }
        self.prefer(&admitted, keys, &settings)
    }

    /// The front of `admitted`, least loaded first, that affinity keeps for `keys`.
    fn prefer<'snap>(
        &self,
        admitted: &[(&'snap RouteCandidate, f64)],
        keys: Option<&PrefixKeys>,
        settings: &AffinitySettings,
    ) -> Option<&'snap RouteCandidate> {
        let affinity = Affinity {
            index: &self.affinity.index,
            keys,
            settings,
            turn: self.turn.fetch_add(1, Ordering::Relaxed),
        };
        let sites: Vec<Site<'_>> = admitted
            .iter()
            .map(|(candidate, queue)| Site {
                cluster: &candidate.cluster,
                queue: *queue,
            })
            .collect();
        let mut keep = vec![true; sites.len()];
        let gate = QueueGate {
            queued_request_seconds: settings.queued_request_seconds,
        };
        affinity.narrow(&sites, &mut keep, &gate).record();
        admitted
            .iter()
            .zip(keep)
            .find_map(|((candidate, _), kept)| kept.then_some(*candidate))
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

    /// Send the request to `candidate`, recording its prefix there for the response to confirm or withdraw.
    fn route(&self, ctx: &mut HttpFilterContext<'_>, candidate: &RouteCandidate, keys: Option<PrefixKeys>) {
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
        // An earlier cluster-selecting filter wins; never override its choice.
        if ctx.cluster.is_some() {
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
            // No model header: not ours to route, leave it for the next filter.
            return Ok(FilterAction::Continue);
        };

        let snapshot = self.snapshot.load();
        let keys = ctx
            .get_filter_state_mut::<RouteState>()
            .and_then(|state| state.keys.take())
            .map(|keys| keys.for_model(model));
        let Some(candidate) = self.choose(&snapshot, model, keys.as_ref()) else {
            tracing::debug!(model = %model, "grid_site_route: no admitted candidate");
            return Ok(FilterAction::Reject(Rejection::status(404)));
        };
        self.route(ctx, candidate, keys);
        Ok(FilterAction::Continue)
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

/// The front candidate matching `kind` and `name` that admits new requests.
///
/// The list is pre-ordered (least-loaded first), so the front match is the
/// chosen target. This is the reused selection: a linear first-match, never a
/// score computed here.
pub(crate) fn select_admitted<'list>(
    candidates: &'list [RouteCandidate],
    kind: CapabilityKind,
    name: &str,
) -> Option<&'list RouteCandidate> {
    candidates.iter().find(|candidate| admits(candidate, kind, name))
}

/// Whether `candidate` serves `kind` `name` and admits a new request.
fn admits(candidate: &RouteCandidate, kind: CapabilityKind, name: &str) -> bool {
    candidate.kind == kind && &*candidate.name == name && is_admitted_for_new_request(candidate.admission_state)
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
    reason = "tests"
)]
mod tests {
    use super::*;
    use crate::descriptor::{CandidateConfig, validate_candidates};

    /// A validated one-candidate list for `model` at `site`/`cluster`.
    fn one(model: &str, site: &str, cluster: &str, admission: AdmissionState) -> Vec<RouteCandidate> {
        let mut candidates = validate_candidates(vec![CandidateConfig {
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
            candidates,
            loads: sorted.iter().map(|(_, queue, _)| *queue).collect(),
            local_site: Arc::from("a"),
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
            .map(|candidate| candidate.site.to_string())
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

    #[test]
    fn front_match_is_selected() {
        let candidates = one("llama", "east", "pool-a", AdmissionState::NewAndExisting);
        let chosen = select_admitted(&candidates, CapabilityKind::InferenceModel, "llama").expect("a match");
        assert_eq!(&*chosen.cluster, "pool-a");
    }

    #[test]
    fn a_different_model_does_not_match() {
        let candidates = one("llama", "east", "pool-a", AdmissionState::NewAndExisting);
        assert!(select_admitted(&candidates, CapabilityKind::InferenceModel, "granite").is_none());
    }

    #[test]
    fn an_excluded_candidate_is_skipped() {
        let candidates = one("llama", "east", "pool-a", AdmissionState::Excluded);
        assert!(select_admitted(&candidates, CapabilityKind::InferenceModel, "llama").is_none());
    }

    #[test]
    fn an_mcp_kind_does_not_match_an_inference_query() {
        let candidates = one("llama", "east", "pool-a", AdmissionState::NewAndExisting);
        assert!(select_admitted(&candidates, CapabilityKind::McpTool, "llama").is_none());
    }
}
