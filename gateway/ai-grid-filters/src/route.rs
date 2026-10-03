//! The `grid_site_route` filter: pick a cross-site cluster for a request.
//!
//! Reads the model header, finds the front admitted candidate for that model in
//! the current snapshot, and sets `ctx.cluster` for the downstream load
//! balancer. Selection is `select_admitted` over a pre-ordered list. The
//! ordering by live load happens off the request path in `snapshot`.

use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use http::header::{HeaderName, HeaderValue};
use praxis_filter::{FilterAction, FilterError, HttpFilter, HttpFilterContext, Rejection, parse_filter_config};
use serde::Deserialize;

use crate::{
    descriptor::{AdmissionState, CapabilityKind, RouteCandidate, validate_model_header},
    snapshot::RouteSnapshot,
};

/// Internal request header carrying the selected candidate's stable ID.
const SELECTED_CANDIDATE_HEADER: &str = "x-ai-routing-candidate";
/// Internal request header carrying the provider-hop request correlation ID.
const PROVIDER_HOP_REQUEST_ID_HEADER: &str = "x-ai-routing-request-id";
/// Internal request header carrying the serving overlay revision.
const OVERLAY_REVISION_HEADER: &str = "x-ai-routing-revision";

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
    model_header: HeaderName,
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
    ) -> Result<Box<dyn HttpFilter>, FilterError> {
        let cfg: GridSiteRouteConfig = parse_filter_config("grid_site_route", config)?;
        let model_header = validate_model_header(&cfg.model_header)?;
        Ok(Box::new(Self { snapshot, model_header }))
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

    #[expect(
        clippy::too_many_lines,
        reason = "header scrubbing, candidate selection, and attribution form one request-context operation"
    )]
    async fn on_request(&self, ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        // Never accept client-controlled provider-hop context. When this route
        // selects an explicitly configured mTLS provider gateway, it writes a
        // fresh candidate identity and request ID after the removal is queued.
        ctx.request_headers_to_remove
            .push(HeaderName::from_static(SELECTED_CANDIDATE_HEADER));
        ctx.request_headers_to_remove
            .push(HeaderName::from_static(PROVIDER_HOP_REQUEST_ID_HEADER));
        ctx.request_headers_to_remove
            .push(HeaderName::from_static(OVERLAY_REVISION_HEADER));

        // Borrow the model header out of *ctx.request; that field is disjoint
        // from the ctx.cluster write below, so no owned copy is needed.
        let Some(model) = ctx
            .request
            .headers
            .get(&self.model_header)
            .and_then(|value| value.to_str().ok())
        else {
            // No model header: not ours to route, leave it for the next filter.
            return Ok(FilterAction::Continue);
        };

        let snapshot = self.snapshot.load();
        let candidate = match route_decision(&snapshot, model, ctx.cluster.is_some()) {
            RouteDecision::KeepEarlier => return Ok(FilterAction::Continue),
            RouteDecision::NoRoute => {
                tracing::debug!(model = %model, "grid_site_route: no admitted candidate");
                return Ok(FilterAction::Reject(Rejection::status(404)));
            },
            RouteDecision::Select(candidate) => candidate,
        };
        ctx.cluster = Some(Arc::clone(&candidate.cluster));
        if snapshot.provider_hop_clusters.contains(candidate.cluster.as_ref()) {
            if candidate.stable_id.trim().is_empty() {
                return Err("grid_site_route: provider-hop candidate has no stable ID".into());
            }
            let candidate_id = HeaderValue::from_str(candidate.stable_id.as_ref()).map_err(|error| {
                FilterError::from(format!("grid_site_route: invalid provider-hop candidate ID: {error}"))
            })?;
            let request_id = ctx.id_generator.generate(ctx.time_source);
            let request_id = HeaderValue::from_str(&request_id).map_err(|error| {
                FilterError::from(format!("grid_site_route: invalid provider-hop request ID: {error}"))
            })?;
            ctx.request_headers_to_set
                .push((HeaderName::from_static(SELECTED_CANDIDATE_HEADER), candidate_id));
            ctx.request_headers_to_set
                .push((HeaderName::from_static(PROVIDER_HOP_REQUEST_ID_HEADER), request_id));
        }
        Ok(FilterAction::Continue)
    }
}

/// A prior selector cannot revive a model route after an authoritative empty snapshot.
enum RouteDecision<'candidate> {
    /// A previous filter selected a cluster while Grid still has candidates.
    KeepEarlier,
    /// Select this candidate from the current snapshot.
    Select(&'candidate RouteCandidate),
    /// No candidate may serve this model.
    NoRoute,
}

/// Resolve one model request without letting a preselected cluster bypass no-route.
fn route_decision<'snapshot>(
    snapshot: &'snapshot RouteSnapshot,
    model: &str,
    preselected: bool,
) -> RouteDecision<'snapshot> {
    if snapshot.candidates.is_empty() {
        return RouteDecision::NoRoute;
    }
    if preselected {
        return RouteDecision::KeepEarlier;
    }
    select_admitted(&snapshot.candidates, CapabilityKind::InferenceModel, model)
        .map_or(RouteDecision::NoRoute, RouteDecision::Select)
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
    candidates.iter().find(|candidate| {
        candidate.kind == kind && &*candidate.name == name && is_admitted_for_new_request(candidate.admission_state)
    })
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
            stable_id: None,
        }])
        .unwrap();
        candidates[0].admission_state = admission;
        candidates
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

    #[test]
    fn empty_snapshot_rejects_even_with_a_preselected_cluster() {
        let empty = RouteSnapshot::from_static(Vec::new(), Arc::from("local"));
        assert!(matches!(route_decision(&empty, "llama", true), RouteDecision::NoRoute));

        let active = RouteSnapshot::from_static(
            one("llama", "east", "pool-a", AdmissionState::NewAndExisting),
            Arc::from("local"),
        );
        assert!(matches!(
            route_decision(&active, "llama", true),
            RouteDecision::KeepEarlier
        ));
    }
}
