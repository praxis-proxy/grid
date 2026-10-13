//! Error types for scoring operations.

/// Errors produced by scoring operations.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    /// A backend with this name already exists in the [`GridState`].
    ///
    /// [`GridState`]: crate::GridState
    #[error("duplicate backend name: {name}")]
    DuplicateBackend {
        /// The conflicting backend name.
        name: String,
    },
}
