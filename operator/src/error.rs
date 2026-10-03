//! Operator error types.

// ---------------------------------------------------------------------------
// Operator Error
// ---------------------------------------------------------------------------

/// Errors produced by the Grid Operator.
#[derive(Debug, thiserror::Error)]
pub enum OperatorError {
    /// Certificate generation failed.
    #[error("certificate error: {0}")]
    Certificate(#[from] certs::GenerateError),

    /// Kubernetes API error.
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),

    /// JSON serialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// A required resource was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Routing overlay rendering failed.
    #[error("overlay render: {0}")]
    OverlayRender(String),

    /// Consumer Praxis config rendering failed.
    #[error("consumer config render: {0}")]
    ConsumerConfigRender(#[from] crate::resources::consumer_config::ConsumerConfigError),

    /// Delegated gateway mount validation or reconciliation failed.
    #[error("gateway mount reconciliation: {0}")]
    MountReconciliation(#[from] GatewayMountFailure),

    /// SWIM encryption key configuration failed.
    #[error("swim key configuration: {0}")]
    SwimKeyConfig(String),

    /// A watched resource is missing required metadata.
    #[error("invalid resource: {0}")]
    InvalidResource(String),
}

/// A sanitized gateway mount failure suitable for status reporting.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{reason}: {message}")]
pub struct GatewayMountFailure {
    /// Stable status reason.
    pub reason: &'static str,
    /// Diagnostic with Secret identifiers and paths only.
    pub message: String,
}

impl GatewayMountFailure {
    /// Construct a sanitized mount reconciliation failure.
    #[must_use]
    pub fn new<M: Into<String>>(reason: &'static str, message: M) -> Self {
        Self {
            reason,
            message: message.into(),
        }
    }
}
