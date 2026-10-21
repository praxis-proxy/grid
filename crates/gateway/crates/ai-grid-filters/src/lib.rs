//! Grid data-plane routing filters for the Praxis gateway.
//!
//! Registers `grid_site_route`, which routes a request to a cross-site cluster
//! by model, preferring the least-loaded site from live signals. The filter
//! reuses the descriptor data model and a first-admitted selection. The grid
//! contribution is ordering the candidates by live load off the request path.

#[cfg(test)]
mod conformance;
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
const COUNTER_HELP: [(&str, &str); 8] = [
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
    (
        "grid_route_site_silent_transitions_total",
        "Sites entering or leaving silence, by the state entered. A steady rate is a flapping link.",
    ),
    ("grid_signals_poll_total", "Polls of the local operator by result."),
    (
        "grid_signals_ingest_dropped_total",
        "Rows the operator served that the gateway refused, by reason.",
    ),
    (
        "grid_signals_feedback_failures_total",
        "Failed gateway feedback delivery attempts, by bounded reason.",
    ),
];

/// Grid gauges the gateway emits.
const GAUGE_HELP: [(&str, &str); 17] = [
    (
        "grid_route_site_rho",
        "Saturation the draw weights the site by: the nearest limit it is up against, smoothed across samples. NaN when unmeasured.",
    ),
    (
        "grid_route_site_rho_reported",
        "Saturation the site reports about itself, as its KV cache utilization plus the spread across its pods. NaN where it reports none.",
    ),
    (
        "grid_route_site_rho_counted",
        "Requests in flight over the ceiling they are read against. NaN until a hard bound makes the count a reading rather than an upper bound.",
    ),
    (
        "grid_route_site_weight",
        "Capacity the draw weights the site by, after the explore floor. NaN when unmeasured.",
    ),
    (
        "grid_route_site_ceiling",
        "Requests a count is read against: the lesser of the two bounds, never below the floor. NaN when unmeasured.",
    ),
    (
        "grid_route_site_ceiling_soft",
        "Most requests the current site size has carried with nothing waiting. NaN until it is seen queue-free; retired after a sustained loss of serving units.",
    ),
    (
        "grid_route_site_ceiling_hard",
        "Fewest requests the site held while at a limit. NaN until it has been pushed to one.",
    ),
    (
        "grid_route_site_in_flight",
        "Requests the site held, as read. The numerator of the counted saturation. NaN when unmeasured.",
    ),
    (
        "grid_route_site_full",
        "1 while the site has had a backlog at its ceiling for long enough to shed, 0 otherwise. NaN where it publishes no backlog.",
    ),
    (
        "grid_route_site_reading_age_seconds",
        "Age of the sample the decision read. A frozen value and a steady one are the same picture without it.",
    ),
    (
        "grid_route_site_silent",
        "1 while a site that has published has sent nothing within the freshness window, so it takes no request while a live site can.",
    ),
    (
        "grid_route_site_last_sample_age_seconds",
        "Seconds since the site's newest sample, still counting past the freshness window.",
    ),
    (
        "grid_route_site_round_age_seconds",
        "Seconds since the operator's last poll round that tried this peer. NaN for this site or with no rounds reported.",
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
