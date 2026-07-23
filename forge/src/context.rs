//! Shared execution context for Forge commands.
//!
//! [`ForgeContext`] bundles the command runner, parsed configuration,
//! and global options into a single value so command handlers stay
//! under the five-argument limit.

use std::path::PathBuf;

use crate::{command::runner::CommandRunner, config::ForgeConfig, output::OutputFormat};

/// Shared execution context for a single command invocation.
///
/// Constructed in `main.rs` after parsing CLI arguments and loading
/// the configuration.  Command handlers receive `&ForgeContext`
/// instead of five separate parameters.
pub struct ForgeContext<'a> {
    /// Command runner (real or mock).
    pub runner: &'a dyn CommandRunner,
    /// Parsed and validated Forge configuration.
    pub config: &'a ForgeConfig,
    /// Directory for state files and locks.
    pub state_dir: PathBuf,
    /// Output format (text or JSON).
    pub format: OutputFormat,
    /// If true, skip all mutating operations.
    pub dry_run: bool,
}
