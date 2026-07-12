//! Kubernetes resource builders for the Grid Operator.

/// Grid overlay ConfigMap builder for Praxis integration.
pub mod overlay;
/// Pure overlay renderer for Praxis `grid_route` routing candidates.
pub mod routing_overlay;
/// Secret builders for grid TLS certificates.
pub mod secret;
/// Trust bundle management for grid mTLS.
pub mod trust_bundle;
