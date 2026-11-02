//! Shared grid signal primitives.
//!
//! The grid signals vocabulary. The Prometheus exposition tokenizer and the grid
//! label names are plane-neutral: the control-plane producer and the data-plane
//! consumer agree on them. The load-signal store is the key-value store a source
//! fills and the routing filters read. The store splits into its own crate once a
//! second source needs it.

pub mod exposition;

/// Gateway ingest feedback wire vocabulary.
pub mod feedback;

/// The operator's peer poller rounds, relayed to its own gateway.
pub mod poller;

pub mod revision;

/// Clock-skew margin the gateway applies before a discovered-model expiry deadline.
pub const CANDIDATE_EXPIRY_SKEW_MARGIN_MS: u64 = 30_000;

mod signals;

pub use signals::{Combine, CombinePolicy, Ingested, LearningState, LoadStore, Sample, Timeline, now_ms};
