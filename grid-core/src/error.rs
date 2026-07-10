//! Error types for grid-core operations.

/// Errors produced by grid-core operations.
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
