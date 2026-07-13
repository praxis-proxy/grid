//! Delta CRDT types for AI Grid state propagation.
//!
//! Three CRDT types piggybacked on SWIM probe messages
//! for partition-tolerant state propagation across grid sites:
//!
//! - [`LwwRegister`]: Last-Writer-Wins for metrics (queue depth, latency, KV cache utilization, health state)
//! - [`OrSet`]: Observed-Remove Set for capabilities (models, tools, agents)
//! - [`GCounter`]: Grow-only counter for budget tracking (per-tenant spend)
//!
//! All types are commutative, associative, and idempotent
//! (semilattice merge). Messages can arrive out of order, be
//! duplicated, or be lost without causing inconsistency.

#![deny(unsafe_code)]

/// Grow-only counter for budget tracking.
pub mod gcounter;
/// Mergeable grid state snapshots.
pub mod grid_state;
/// Last-Writer-Wins register for metrics.
pub mod lww;
/// Observed-Remove Set for capabilities.
pub mod orset;

pub use gcounter::GCounter;
pub use grid_state::{Capability, GridStateSnapshot, ProviderMetricsSnapshot, ProviderPhase, ProviderState};
pub use lww::LwwRegister;
pub use orset::OrSet;
