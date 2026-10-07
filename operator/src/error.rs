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

    /// SWIM encryption key configuration failed.
    #[error("swim key configuration: {0}")]
    SwimKeyConfig(String),

    /// A watched resource is missing required metadata.
    #[error("invalid resource: {0}")]
    InvalidResource(String),
}

impl OperatorError {
    /// Whether this is the apiserver refusing a write because the object moved underneath it.
    ///
    /// Two writers on one object is the normal case, not a fault: the next reconcile reads the
    /// new version and reapplies. Logging it at error made a steady state look like a failure.
    #[must_use]
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Kube(kube::Error::Api(status)) if status.code == 409)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An apiserver status with `code`, as `enroll::tests` scripts one.
    fn api(code: u16) -> OperatorError {
        OperatorError::Kube(kube::Error::Api(Box::new(
            kube::core::Status::failure("scripted", "r").with_code(code),
        )))
    }

    #[test]
    fn only_a_409_is_a_lost_write_race() {
        assert!(api(409).is_conflict(), "409 is the apiserver refusing a stale write");
        assert!(!api(404).is_conflict(), "a missing object is not a race");
        assert!(!api(500).is_conflict(), "a server error is not a race");
        assert!(
            !OperatorError::NotFound("grid".to_owned()).is_conflict(),
            "a non-kube error is not a race"
        );
    }
}
