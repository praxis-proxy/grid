//! SWIM membership protocol wrapper for the AI Grid.
//!
//! Wraps the [`foca`] crate to provide peer-to-peer membership
//! discovery with a Tokio-native async interface. The SWIM
//! runtime sends [`MemberEvent`]s to the Grid Operator's
//! controllers via channels.
//!
//! ```
//! use swim::NodeId;
//!
//! let id = NodeId::new("cluster-a".to_owned(), "10.0.0.1:7946".parse().unwrap());
//! assert_eq!(id.site_name(), "cluster-a");
//! ```
//!
//! [`foca`]: https://crates.io/crates/foca

#![deny(unsafe_code)]

/// AES-256-GCM encryption/authentication for SWIM UDP packets.
pub mod crypto;
/// Membership events emitted by the SWIM runtime.
pub mod event;
/// SWIM node identity for the AI Grid.
pub mod identity;
/// High-level SWIM node wrapping a foca Foca instance.
pub mod node;
/// Foca runtime adapter with accumulated output.
pub mod runtime;
/// State snapshot payloads for SWIM custom broadcasts.
pub mod state_broadcast;

pub use event::MemberEvent;
pub use identity::NodeId;
pub use node::SwimNode;
pub use runtime::{AccumulatedOutput, GridRuntime};
pub use state_broadcast::{
    STATE_BROADCAST_VERSION, STATE_BROADCAST_VERSION_V1, StateBroadcast, StateBroadcastError, StateBroadcastHandler,
    StateBroadcastKey,
};
