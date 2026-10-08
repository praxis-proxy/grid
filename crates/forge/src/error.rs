//! Error types for the Forge CLI.

/// Errors produced by Forge operations.
#[derive(Debug, thiserror::Error)]
pub enum ForgeError {
    /// Configuration file could not be read or parsed.
    #[error("config error: {0}")]
    Config(String),

    /// Semantic validation of a parsed configuration failed.
    #[error("validation error: {0}")]
    Validation(String),

    /// Filesystem I/O failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// YAML deserialization or serialization failed.
    #[error("yaml error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    /// An external command failed or could not be found.
    #[error("command failed: {program}: {message}")]
    Command {
        /// The program that was invoked.
        program: String,
        /// Human-readable failure description.
        message: String,
    },

    /// State file could not be read, written, or is corrupt.
    #[error("state error: {0}")]
    State(String),

    /// Container runtime detection or probing failed.
    #[error("runtime error: {0}")]
    Runtime(String),

    /// Could not acquire or release a file lock.
    #[error("lock error: {0}")]
    Lock(String),
}
