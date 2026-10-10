//! Grid data-plane routing filters for the Praxis gateway.
//!
//! Registers `grid_site_route`, which routes a request to a cross-site cluster
//! by model, preferring the least-loaded site from live signals. The filter
//! reuses the descriptor data model and a first-admitted selection. The grid
//! contribution is ordering the candidates by live load off the request path.

mod control;
mod decisions;
mod descriptor;
#[cfg(test)]
mod flow;
mod health;
mod metadata;
mod pin;
mod prefix;
mod route;
mod serving;
mod signals;
mod snapshot;

use std::sync::Arc;

use arc_swap::ArcSwap;
pub use control::{ReloadOutcome, Tuning};
pub use decisions::SiteDecisions;
// The routing model and the snapshot builder are the crate's control-plane API:
// the gateway's refresh step orders candidates by live load and swaps the
// snapshot. The request path only reads a snapshot.
pub use descriptor::{AdmissionState, CandidateConfig, CapabilityKind, RouteCandidate};
pub use health::ClusterHealth;
pub use metadata::{CandidateCredential, CredentialRef};
use praxis_filter::{FilterError, FilterFactory, FilterRegistry, HttpFilter};
pub use prefix::{AffinitySettings, PrefixAffinity};
pub use serving::{
    AvailabilitySettings, GridRuntime, GridServingConfig, PeerServingConfig, load_serving_config, spawn_grid_routing,
};
pub use signals::{Over, SiteReading, SiteSignals};
pub use snapshot::RouteSnapshot;

/// Grid counters the gateway emits, with the help text a scrape shows.
const COUNTER_HELP: [(&str, &str); 6] = [
    (
        "grid_route_decisions_total",
        "Requests the gateway decided, by outcome. A refusal records an empty site and cluster.",
    ),
    (
        "grid_route_selections_total",
        "Decisions by the path that produced them.",
    ),
    (
        "grid_route_prefix_affinity_total",
        "Prefix-affinity decisions by outcome.",
    ),
    ("grid_serving_config_reload_total", "Serving-config reloads by result."),
    ("grid_signals_poll_total", "Polls of the local operator by result."),
    (
        "grid_signals_ingest_dropped_total",
        "Rows the operator served that the gateway refused, by reason.",
    ),
];

/// Grid gauges the gateway emits.
const GAUGE_HELP: [(&str, &str); 7] = [
    (
        "grid_route_site_rho",
        "Saturation the gateway last read for the site, in-flight over its ceiling. NaN when unmeasured.",
    ),
    (
        "grid_route_site_weight",
        "Capacity the draw weights the site by. NaN when unmeasured.",
    ),
    (
        "grid_route_site_ceiling",
        "Ceiling the gateway has learned for the site. NaN when unmeasured.",
    ),
    (
        "grid_route_site_score",
        "Score the site ranks by in the current snapshot. NaN when excluded or demoted.",
    ),
    (
        "grid_route_shedding",
        "1 while the gateway sheds this model, 0 otherwise.",
    ),
    (
        "grid_signals_last_success_timestamp_seconds",
        "Unix time of the last successful poll of the local operator.",
    ),
    (
        "grid_signals_response_bytes",
        "Size of the last successful poll's response, in bytes.",
    ),
];

/// Describe every grid metric the gateway emits, so a scrape carries help text.
///
/// Call once after the Prometheus recorder is installed: the `metrics` crate
/// drops a description sent to the no-op recorder, and the emit sites run too
/// late and too often to carry one.
pub fn describe_metrics() {
    for (name, help) in COUNTER_HELP {
        metrics::describe_counter!(name, metrics::Unit::Count, help);
    }
    for (name, help) in GAUGE_HELP {
        metrics::describe_gauge!(name, help);
    }
}

/// The number of prefix keys `body` yields for a request to `path`, for the
/// peak-memory test; not an API.
#[doc(hidden)]
#[must_use]
pub fn prefix_key_count(path: &str, body: &[u8]) -> usize {
    prefix::Api::from_path(path)
        .and_then(|api| prefix::prefix_keys(api, body))
        .map_or(0, |keys| keys.as_slice().len())
}

/// Register `grid_site_route` into `registry` over a shared snapshot the gateway
/// owns and its refresh loop swaps.
///
/// Call this from the gateway after `FilterRegistry::with_builtins()`, passing
/// the snapshot, cluster health, and tuning from [`spawn_grid_routing`]. The factory captures
/// the snapshot, so every filter praxis rebuilds on a config reload clones the same `Arc` and
/// sees the live swaps. Each build sets the filter block's `availability` and `prefix_affinity` into
/// `tuning`, which the control step reads.
///
/// # Errors
///
/// Returns [`FilterError`] if the filter name is already registered.
pub fn register_grid_filters(
    registry: &mut FilterRegistry,
    snapshot: Arc<ArcSwap<RouteSnapshot>>,
    affinity: Arc<PrefixAffinity>,
    health: Arc<ClusterHealth>,
    tuning: Arc<Tuning>,
) -> Result<(), FilterError> {
    let factory = move |config: &serde_yaml::Value| -> Result<Box<dyn HttpFilter>, FilterError> {
        route::GridSiteRouteFilter::from_config(
            config,
            Arc::clone(&snapshot),
            Arc::clone(&affinity),
            Arc::clone(&health),
            &tuning,
        )
    };
    registry.register("grid_site_route", FilterFactory::Http(Arc::new(factory)))
}
