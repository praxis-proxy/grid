//! Core types and scoring engine for the AI Grid.
//!
//! Provides backend type definitions, runtime metrics, grid state
//! management, and a weighted scoring engine that ranks backends
//! by locality, cost, and latency.
//!
//! ```
//! # fn main() -> Result<(), grid_core::CoreError> {
//! use grid_core::{
//!     BackendConfig, BackendKind, GridState, ProviderKind, ScoringWeights, score_backends,
//! };
//!
//! let mut state = GridState::new();
//! state.add_backend(BackendConfig::new(
//!     "local-vllm".to_owned(),
//!     0.001,
//!     0.002,
//!     "http://localhost:8080".to_owned(),
//!     BackendKind::Local,
//!     ProviderKind::OpenAi,
//!     None,
//! ))?;
//! let ranked = score_backends(&state, &ScoringWeights::default(), None);
//! assert_eq!(ranked.len(), 1);
//! # Ok(())
//! # }
//! ```

#![deny(unsafe_code)]

mod backend;
mod error;
mod metrics;
mod scoring;
mod state;

pub use backend::{BackendConfig, BackendKind, ProviderKind};
pub use error::CoreError;
pub use metrics::BackendMetrics;
pub use scoring::{ScoredBackend, ScoringWeights, locality_score, score_backends};
pub use state::GridState;
