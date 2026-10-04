//! Grid data-plane routing filters for the Praxis gateway.
//!
//! Registers `grid_site_route`, which routes a request to a cross-site cluster
//! by model, preferring the least-loaded site from live signals. The filter
//! reuses the descriptor data model and a first-admitted selection. The grid
//! contribution is ordering the candidates by live load off the request path.

mod control;
mod descriptor;
#[cfg(test)]
mod flow;
mod metadata;
mod pin;
mod prefix;
mod route;
mod serving;
mod snapshot;

use std::sync::Arc;

use arc_swap::ArcSwap;
pub use control::ReloadOutcome;
// The routing model and the snapshot builder are the crate's control-plane API:
// the gateway's refresh step orders candidates by live load and swaps the
// snapshot. The request path only reads a snapshot.
pub use descriptor::{AdmissionState, CandidateConfig, CapabilityKind, RouteCandidate};
pub use metadata::{CandidateCredential, CredentialRef};
use praxis_filter::{FilterError, FilterFactory, FilterRegistry, HttpFilter};
pub use prefix::{AffinitySettings, PrefixAffinity};
pub use serving::{GridRuntime, GridServingConfig, PeerServingConfig, load_serving_config, spawn_grid_routing};
pub use snapshot::RouteSnapshot;

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
/// the snapshot from [`spawn_grid_routing`]. The factory captures the snapshot,
/// so every filter praxis rebuilds on a config reload clones the same `Arc` and
/// sees the live swaps.
///
/// # Errors
///
/// Returns [`FilterError`] if the filter name is already registered.
pub fn register_grid_filters(
    registry: &mut FilterRegistry,
    snapshot: Arc<ArcSwap<RouteSnapshot>>,
    affinity: Arc<PrefixAffinity>,
) -> Result<(), FilterError> {
    let factory = move |config: &serde_yaml::Value| -> Result<Box<dyn HttpFilter>, FilterError> {
        route::GridSiteRouteFilter::from_config(config, Arc::clone(&snapshot), Arc::clone(&affinity))
    };
    registry.register("grid_site_route", FilterFactory::Http(Arc::new(factory)))
}
