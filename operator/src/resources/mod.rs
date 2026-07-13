//! Kubernetes resource builders for the Grid Operator.

/// Bridge from operator routing overlays to Praxis `grid_route` filter config.
pub mod overlay_bridge;
/// Provider metrics collection for the [`GridNetwork`] overlay renderer.
///
/// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
pub(crate) mod provider_metrics;
/// Pure overlay renderer for Praxis `grid_route` routing candidates.
pub mod routing_overlay;
/// Secret builders for grid TLS certificates.
pub mod secret;
/// Trust bundle management for grid mTLS.
pub mod trust_bundle;
