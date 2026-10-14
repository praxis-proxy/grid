//! Served-model discovery sources.
//!
//! A [`ModelSource`] exposes the models that a given backend
//! exposes at any time.
//!
//! [`ModelSource`]: crate::resources::model_discovery::ModelSource

mod models;
mod openai;

use std::time::Duration;

pub(crate) use models::{ServedModels, ServedModelsError};
pub(crate) use openai::OpenAiModels;

// ---------------------------------------------------------------------------
// ModelSource
// ---------------------------------------------------------------------------

/// A backend that can report the models it serves.
///
/// Implementations must be bounded in time and size, and must reject a
/// malformed answer instead of returning a partial set.
pub(crate) trait ModelSource {
    /// Fetch the current served-model set.
    ///
    /// # Errors
    ///
    /// Returns [`DiscoveryError`] when the backend cannot be queried or its
    /// answer is not a valid model list.
    async fn served_models(&self) -> Result<ServedModels, DiscoveryError>;
}

// ---------------------------------------------------------------------------
// DiscoveryError
// ---------------------------------------------------------------------------

/// Why a discovery poll failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DiscoveryError {
    /// The source is misconfigured (bad URL, TLS on plain HTTP).
    #[error("invalid configuration: {0}")]
    Config(String),

    /// The request could not be sent or the response could not be read.
    #[error("transport error: {0}")]
    Transport(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The poll exceeded its timeout.
    #[error("timed out after {0:?}")]
    Timeout(Duration),

    /// The backend answered with a non-success status.
    #[error("unexpected HTTP status {0}")]
    Status(http::StatusCode),

    /// The response body exceeds the size cap.
    #[error("response exceeds {0} bytes")]
    BodyTooLarge(usize),

    /// The response is not a model list.
    #[error("malformed response: {0}")]
    Malformed(#[from] serde_json::Error),

    /// The model list is well-formed but not a valid served-model set.
    #[error(transparent)]
    InvalidModels(#[from] ServedModelsError),
}
