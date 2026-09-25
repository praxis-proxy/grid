//! Grid data-plane filter primitives.
//!
//! The consumer side of the grid signals contract: a bounded store that absorbs
//! the operator's exposition and answers per-candidate load queries on the
//! request path. The shared exposition tokenizer lives in the `common` crate.

mod signals;

pub use signals::{LoadStore, Sample, now_ms};
