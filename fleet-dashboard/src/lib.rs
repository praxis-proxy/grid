//! Fleet dashboard for an AI Grid hub: draws every registered site on a map,
//! colors it from that site's own Prometheus, and serves the detail the SPA
//! renders.
//!
//! The binary in `main.rs` only wires these modules together; everything with
//! behavior lives here where it is tested.

pub mod api;
pub mod collector;
pub mod config;
pub mod demo;
pub mod geocode;
pub mod health;
pub mod metrics;
pub mod model;
pub mod options;
pub mod queries;
pub mod registry;
