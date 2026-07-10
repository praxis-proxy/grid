//! Custom resource definitions for the AI Grid.

/// [`GridNetwork`] — the grid itself, top-level tenancy boundary.
///
/// [`GridNetwork`]: grid_network::GridNetwork
pub mod grid_network;

/// [`GridSite`] — a remote site in the grid.
///
/// [`GridSite`]: grid_site::GridSite
pub mod grid_site;
