//! AI Grid Kubernetes operator library.
//!
//! Provides CRD definitions, controllers, and resource builders
//! for the Grid Operator. The operator orchestrates a peer-to-peer
//! mesh of Praxis AI gateways across clusters.

#![deny(unsafe_code)]
#![expect(
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    clippy::min_ident_chars,
    reason = "operator uses short closure params, index arithmetic, and casts pervasively"
)]

/// Command-line interface.
pub mod cli;

/// Kubernetes controllers.
pub mod controller;
/// Custom resource definitions.
pub mod crd;
/// Operator error types.
pub mod error;
/// Prometheus metrics for gateway probe and phase-transition observability.
pub mod metrics;
/// Pure Prometheus text-format parser for inference backend metrics.
pub mod metrics_parser;
/// Async HTTP scraper for Prometheus `/metrics` endpoints.
pub mod metrics_scraper;
/// Kubernetes resource builders.
pub mod resources;

pub use resources::trust_bundle::sha256_fingerprint;
/// Provider gateway address self-discovery.
pub mod gateway;
/// SWIM membership data model and status summarization.
///
/// Pure data layer for peer discovery; the live UDP runtime is implemented in
/// [`swim_runtime`].
pub mod swim;
/// Live SWIM membership runtime (foca-backed UDP event loop).
///
/// Produces [`swim::MembershipSnapshot`]s consumed by the [`GridNetwork`]
/// controller via [`controller::grid_network::OperatorCtx`].
///
/// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
pub mod swim_runtime;
