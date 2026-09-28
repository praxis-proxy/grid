//! Shared grid signal primitives.
//!
//! The grid signals vocabulary. The Prometheus exposition tokenizer and the grid
//! label names are plane-neutral: the control-plane producer and the data-plane
//! consumer agree on them. The load-signal store is the key-value store a source
//! fills and the routing filters read. The store splits into its own crate once a
//! second source needs it.

pub mod exposition;

mod signals;

pub use signals::{LoadStore, Sample, now_ms};
