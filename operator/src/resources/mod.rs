//! Kubernetes resource builders for the Grid Operator.

/// Grid overlay ConfigMap builder for Praxis integration.
pub mod overlay;
/// Bridge from operator routing overlays to Praxis `grid_route` filter config.
pub mod overlay_bridge;
/// Pure overlay renderer for Praxis `grid_route` routing candidates.
pub mod routing_overlay;
/// Secret builders for grid TLS certificates.
pub mod secret;
/// Trust bundle management for grid mTLS.
pub mod trust_bundle;
