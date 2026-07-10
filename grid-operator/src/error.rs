//! Operator error types.

// ---------------------------------------------------------------------------
// Operator Error
// ---------------------------------------------------------------------------

/// Errors produced by the Grid Operator.
#[derive(Debug, thiserror::Error)]
pub enum OperatorError {
    /// Certificate generation failed.
    #[error("certificate error: {0}")]
    Certificate(#[from] grid_certs::GenerateError),

    /// Kubernetes API error.
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),

    /// JSON serialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// A required resource was not found.
    #[error("not found: {0}")]
    NotFound(String),
}
