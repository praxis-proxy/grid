// SPDX-License-Identifier: Apache-2.0

//! Atomic file write with `fsync` and rename.
//!
//! Writes a temporary file in the target directory, flushes and fsyncs
//! it, then atomically renames it over the destination.  A failed write
//! leaves the current file unchanged.

use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
};

use uuid::Uuid;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Atomic write error.
#[derive(Debug)]
pub(crate) struct AtomicWriteError {
    /// Human-readable detail.
    pub(crate) detail: String,
}

impl std::fmt::Display for AtomicWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.detail)
    }
}

impl From<std::io::Error> for AtomicWriteError {
    fn from(e: std::io::Error) -> Self {
        Self {
            detail: format!("I/O error: {e}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Atomic write
// ---------------------------------------------------------------------------

/// Atomically write `data` to `target_path`.
///
/// 1. Writes a temporary file in the same directory as `target_path`.
/// 2. Flushes and fsyncs it.
/// 3. Renames it over `target_path`.
/// 4. Fsyncs the parent directory.
///
/// Returns the number of bytes written on success.
pub(crate) fn atomic_write(target_path: &Path, data: &[u8]) -> Result<usize, AtomicWriteError> {
    let parent = target_path.parent().ok_or_else(|| AtomicWriteError {
        detail: "target path has no parent directory".to_owned(),
    })?;

    let tmp_name = format!(".overlay-sync-tmp-{}", Uuid::new_v4());
    let tmp_path: PathBuf = parent.join(&tmp_name);

    let write_result = write_and_sync(&tmp_path, data);
    if let Err(e) = &write_result {
        tracing::warn!(
            tmp = %tmp_path.display(),
            error = %e,
            "removing failed temporary file"
        );
        drop(fs::remove_file(&tmp_path));
        return Err(AtomicWriteError {
            detail: format!("temporary write failed: {e}"),
        });
    }

    fs::rename(&tmp_path, target_path).map_err(|e| {
        drop(fs::remove_file(&tmp_path));
        AtomicWriteError {
            detail: format!("atomic rename failed: {e}"),
        }
    })?;

    sync_parent(parent);

    Ok(data.len())
}

/// Write data to a file, flush, and fsync.
fn write_and_sync(path: &Path, data: &[u8]) -> Result<(), std::io::Error> {
    let mut file = fs::File::create(path)?;
    file.write_all(data)?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

/// Fsync the parent directory for rename durability.
fn sync_parent(parent: &Path) {
    if let Ok(dir) = fs::File::open(parent) {
        drop(dir.sync_all());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("overlay.json");
        let data = b"test payload";
        let n = atomic_write(&target, data).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(fs::read_to_string(&target).unwrap(), "test payload");
    }

    #[test]
    fn atomic_write_replaces_existing() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("overlay.json");
        fs::write(&target, b"old data").unwrap();
        atomic_write(&target, b"new data").unwrap();
        assert_eq!(fs::read_to_string(&target).unwrap(), "new data");
    }

    #[test]
    fn no_temp_files_left_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("overlay.json");
        atomic_write(&target, b"data").unwrap();
        let entries: Vec<_> = fs::read_dir(dir.path()).unwrap().filter_map(Result::ok).collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file_name().to_str().unwrap(), "overlay.json");
    }
}
