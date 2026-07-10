//! Kubernetes controllers for the Grid Operator.

/// [`GridNetwork`] controller.
///
/// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
pub mod grid_network;

/// [`GridSite`] controller.
///
/// [`GridSite`]: crate::crd::grid_site::GridSite
pub mod grid_site;
