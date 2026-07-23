//! Advisory file locking for exclusive state access.
//!
//! Uses [`fs2::FileExt`] for cross-platform advisory locks.
//! The lock is released automatically when the [`StateLock`] guard
//! is dropped.

use std::{fs::File, path::Path};

use fs2::FileExt as _;

use crate::error::ForgeError;

/// Lock file name within the state directory.
const LOCK_FILE: &str = "lock";

/// RAII guard that holds an exclusive advisory lock on a file.
///
/// The lock is released when this guard is dropped (the underlying
/// file handle closes, releasing the advisory lock).
pub struct StateLock {
    /// Held open for the lock lifetime.
    _file: File,
}

/// Acquire an exclusive lock on `<state_dir>/lock`.
///
/// Creates the lock file and state directory if they do not exist.
/// Blocks until the lock is acquired.
///
/// # Errors
///
/// Returns [`ForgeError::Lock`] if the lock file cannot be created
/// or the lock cannot be acquired.
pub fn acquire(state_dir: &Path) -> Result<StateLock, ForgeError> {
    crate::state::ensure_dir(state_dir)?;
    let file = open_lock_file(&lock_path(state_dir))?;
    lock_exclusive(&file)?;
    Ok(StateLock { _file: file })
}

/// Build the lock file path.
fn lock_path(state_dir: &Path) -> std::path::PathBuf {
    state_dir.join(LOCK_FILE)
}

/// Open or create the lock file.
fn open_lock_file(path: &Path) -> Result<File, ForgeError> {
    std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .map_err(|e| ForgeError::Lock(format!("cannot open lock file {}: {e}", path.display())))
}

/// Lock the file exclusively, blocking until acquired.
fn lock_exclusive(file: &File) -> Result<(), ForgeError> {
    file.lock_exclusive()
        .map_err(|e| ForgeError::Lock(format!("cannot acquire lock: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_creates_lock_file() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        let state_dir = dir.path().join("state");
        let _lock = acquire(&state_dir).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(state_dir.join(LOCK_FILE).exists(), "lock file should exist");
    }

    #[test]
    fn acquire_creates_state_dir() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        let state_dir = dir.path().join("nested").join("state");
        let _lock = acquire(&state_dir).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(state_dir.exists(), "state directory should be created");
    }
}
