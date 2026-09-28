//! The `grid_site_route` filter: pick a cross-site cluster for a request.
//!
//! Reads the model header, finds the front admitted candidate for that model in
//! the current snapshot, and sets `ctx.cluster` for the downstream load
//! balancer. Selection is `select_admitted` over a pre-ordered list. The
//! ordering by live load happens off the request path in `snapshot`.

use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use praxis_filter::{FilterAction, FilterError, HttpFilter, HttpFilterContext, Rejection, parse_filter_config};
use serde::Deserialize;

use crate::{
    descriptor::{
        AdmissionState, CandidateConfig, CapabilityKind, RouteCandidate, validate_candidates, validate_local_site,
        validate_model_header,
    },
    snapshot::RouteSnapshot,
};

/// Default request header carrying the model name.
fn default_model_header() -> String {
    "X-Model".to_owned()
}

/// `grid_site_route` configuration as written in the gateway config.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GridSiteRouteConfig {
    /// Request header naming the model (default `X-Model`).
    #[serde(default = "default_model_header")]
    model_header: String,

    /// This gateway's own site identifier.
    local_site: String,

    /// The candidate topology: which sites serve which capabilities.
    candidates: Vec<CandidateConfig>,
}

/// Routes a request to a cross-site cluster by model, honouring live-load order.
#[derive(Debug)]
pub(crate) struct GridSiteRouteFilter {
    /// The resolved, pre-ordered candidate snapshot, swapped by the control step.
    snapshot: Arc<ArcSwap<RouteSnapshot>>,

    /// Header the request carries the model name in.
    model_header: http::header::HeaderName,
}

impl GridSiteRouteFilter {
    /// Build the filter from its config section.
    ///
    /// # Errors
    ///
    /// Returns [`FilterError`] if the config fails to parse or any identifier,
    /// model header, or candidate entry is invalid.
    pub(crate) fn from_config(config: &serde_yaml::Value) -> Result<Box<dyn HttpFilter>, FilterError> {
        let cfg: GridSiteRouteConfig = parse_filter_config("grid_site_route", config)?;
        validate_local_site(&cfg.local_site)?;
        let model_header = validate_model_header(&cfg.model_header)?;
        let candidates = validate_candidates(cfg.candidates)?;
        let local_site: Arc<str> = Arc::from(cfg.local_site.as_str());
        // Config-order snapshot to start. The gateway's refresh step swaps in a
        // live-load-ordered one once signals arrive.
        let snapshot = Arc::new(ArcSwap::from_pointee(RouteSnapshot::from_static(
            candidates, local_site,
        )));
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

    async fn on_request(&self, ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        // An earlier cluster-selecting filter wins; never override its choice.
        if ctx.cluster.is_some() {
            return Ok(FilterAction::Continue);
        }
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
        let Some(candidate) = select_admitted(&snapshot.candidates, CapabilityKind::InferenceModel, model) else {
            tracing::debug!(model = %model, "grid_site_route: no admitted candidate");
            return Ok(FilterAction::Reject(Rejection::status(404)));
        };
        ctx.cluster = Some(Arc::clone(&candidate.cluster));
        Ok(FilterAction::Continue)
    }
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
