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

#[cfg(all(feature = "tls-rustls", feature = "fips"))]
compile_error!(
    "features `tls-rustls` and `fips` are mutually exclusive; build a FIPS binary with --no-default-features --features fips"
);

#[cfg(not(any(feature = "tls-rustls", feature = "fips")))]
compile_error!("one of `tls-rustls` or `fips` must be enabled");

/// Command-line interface.
pub mod cli;

/// Kubernetes controllers.
pub mod controller;
/// Custom resource definitions.
pub mod crd;
/// Site auto-enroll on startup.
pub mod enroll;
/// Operator error types.
pub mod error;
/// Routes the Services peers reach the operator on to the lease holder alone.
pub mod holder_route;
/// A provider's recent request latency and error ratio, from deltas of its EPP's histograms.
pub mod latency;
/// Leader election on a lease: the holder runs everything that writes.
pub mod leader;
/// Creates this site's `GridNetwork` and `GridSite` once its identity exists.
pub mod local_records;
/// The label providers select their site by.
pub mod local_site;
/// Prometheus metrics for gateway probe and phase-transition observability.
pub mod metrics;
/// Pure Prometheus text-format parser for inference backend metrics.
pub mod metrics_parser;
/// Async HTTP scraper for Prometheus `/metrics` endpoints.
pub mod metrics_scraper;
/// TLS for the metrics and health listener.
pub mod metrics_tls;
/// Short-lived tokens for the metrics scraper ServiceAccount.
pub(crate) mod metrics_token;
/// This operator's own settings, from its `ConfigMap`.
pub mod operator_config;
/// The `GridNetwork` condition naming peers whose relayed signal ages are rejected.
pub mod peer_ages;
pub mod peer_membership;
/// The `GridNetwork` condition naming peers whose signals endpoints did not answer.
pub mod peer_reach;
mod provider_revision;
pub mod readiness;
/// Kubernetes resource builders.
pub mod resources;
/// The `Available`, `Progressing` and `Degraded` roll-up every kind writes.
pub mod rollup;
pub mod served_models;
/// The site name, from the site identity.
pub mod site_name;

pub use resources::{tls_backend::init_process_crypto, trust_bundle::sha256_fingerprint};
/// Provider gateway address self-discovery.
pub mod gateway;
/// Durable gateway ingest diagnostic feedback.
pub mod gateway_ages;
/// Shutdown signal for unwinding in-flight work cleanly.
pub mod shutdown;
/// Provider signals, served as a multi-target exporter.
pub mod signals;
/// The providers a site serves to peers, and those polled from them.
pub mod site_providers;
/// SWIM membership data model and status summarization.
///
/// Pure data layer for peer discovery; the live UDP runtime is implemented in
/// [`swim_runtime`].
pub mod swim;
/// SWIM advertise endpoint selection and Service `LoadBalancer` discovery.
pub mod swim_advertise;
/// SWIM endpoint parsing and bounded DNS resolution.
pub mod swim_endpoint;
/// Live SWIM membership runtime (foca-backed UDP event loop).
///
/// Produces [`swim::MembershipSnapshot`]s consumed by the [`GridNetwork`]
/// controller via [`controller::grid_network::OperatorCtx`].
///
/// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
pub mod swim_runtime;
/// Holds a candidate back from the lease while an unelected replica still writes.
pub mod unelected;
