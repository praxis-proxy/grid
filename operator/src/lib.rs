//! AI Grid Kubernetes operator library.
//!
//! Provides CRD definitions, controllers, and resource builders
//! for the Grid Operator. The operator orchestrates a peer-to-peer
//! mesh of Praxis AI gateways across clusters.

#![deny(unsafe_code)]

/// Kubernetes controllers.
pub mod controller;
/// Custom resource definitions.
pub mod crd;
/// Operator error types.
pub mod error;
/// Pure Prometheus text-format parser for inference backend metrics.
pub mod metrics_parser;
/// Kubernetes resource builders.
pub mod resources;
