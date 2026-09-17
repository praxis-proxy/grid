//! Shared grid primitives.
//!
//! A plane-neutral leaf crate: both the control-plane operator and the
//! data-plane gateway may depend on it. It holds no plane-specific logic, only
//! primitives the producer and consumer must agree on. Today that is the
//! Prometheus exposition tokenizer and the grid label names.

pub mod exposition;
