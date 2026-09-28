//! Grid data-plane routing filters for the Praxis gateway.
//!
//! Registers `grid_site_route`, which routes a request to a cross-site cluster
//! by model, preferring the least-loaded site from live signals. The filter
//! reuses the descriptor data model and a first-admitted selection. The grid
//! contribution is ordering the candidates by live load off the request path.

mod descriptor;
mod metadata;
mod route;
mod snapshot;

// The routing model and the snapshot builder are the crate's control-plane API:
// the gateway's refresh step orders candidates by live load and swaps the
// snapshot. The request path only reads a snapshot.
pub use descriptor::{AdmissionState, CapabilityKind, RouteCandidate};
pub use metadata::{CandidateCredential, CredentialRef};
use praxis_filter::{FilterRegistry, register_filters};
pub use snapshot::RouteSnapshot;

/// Register the grid routing filters into `registry`.
///
/// Call this from the gateway after `FilterRegistry::with_builtins()`, the same
/// way `register_ai_filters` is called.
pub fn register_grid_filters(registry: &mut FilterRegistry) {
    register_filters!(
        @register registry,
        http "grid_site_route" => route::GridSiteRouteFilter::from_config
    );
}
