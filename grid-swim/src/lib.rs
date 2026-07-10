//! SWIM membership protocol wrapper for the AI Grid.
//!
//! Wraps the [`foca`] crate to provide peer-to-peer membership
//! discovery with a Tokio-native async interface. The SWIM
//! runtime sends [`MemberEvent`]s to the Grid Operator's
//! controllers via channels.
//!
//! ```
//! use grid_swim::NodeId;
//!
//! let id = NodeId::new("cluster-a".to_owned(), "10.0.0.1:7946".parse().unwrap());
//! assert_eq!(id.site_name(), "cluster-a");
//! ```
//!
//! [`foca`]: https://crates.io/crates/foca

#![deny(unsafe_code)]

/// Membership events emitted by the SWIM runtime.
pub mod event;
/// SWIM node identity for the AI Grid.
pub mod identity;
/// Foca runtime adapter with accumulated output.
pub mod runtime;

pub use event::MemberEvent;
pub use identity::NodeId;
pub use runtime::{AccumulatedOutput, GridRuntime};
