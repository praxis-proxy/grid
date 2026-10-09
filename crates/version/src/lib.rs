//! Build and version information for AI Grid binaries.
//!
//! Every fact is resolved by `build.rs` at compile time, so reading it costs nothing and
//! a binary can always say what it is. A fact the build could not resolve reads
//! `unknown`, never an empty string, so a binary built without provenance is visible
//! rather than silently blank.

/// The build's version with its tree state, for a CLI `--version` flag that needs a
/// `&'static str`. `build.rs` concatenates it; `Display` on [`Info`] prints the same text.
pub const VERSION: &str = env!("GRID_VERSION");

/// Build and version facts for the running binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Info {
    /// `git describe` of the build, or the crate version when git was unavailable.
    pub git_version: &'static str,
    /// Full commit the build came from, or `unknown`.
    pub git_commit: &'static str,
    /// `clean`, `dirty`, or `unknown`.
    pub git_tree_state: &'static str,
    /// Build date as `YYYYMMDD`, or `unknown`.
    pub build_date: &'static str,
    /// Compiler that built this binary.
    pub rustc_version: &'static str,
    /// Target triple this binary was built for.
    pub platform: &'static str,
}

/// Facts for the running binary.
#[must_use]
pub const fn get() -> Info {
    Info {
        git_version: env!("GRID_GIT_VERSION"),
        git_commit: env!("GRID_GIT_COMMIT"),
        git_tree_state: env!("GRID_GIT_TREE_STATE"),
        build_date: env!("GRID_BUILD_DATE"),
        rustc_version: env!("GRID_RUSTC_VERSION"),
        platform: env!("GRID_TARGET"),
    }
}

impl Info {
    /// Whether the build came from an unmodified work tree.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        matches!(self.git_tree_state.as_bytes(), b"clean")
    }

    /// Whether the build carries no commit, so it cannot be traced to a source revision.
    #[must_use]
    pub const fn is_untraceable(&self) -> bool {
        matches!(self.git_commit.as_bytes(), b"unknown")
    }

    /// Log these facts at startup.
    ///
    /// `name` is the binary's own name, since one crate can build several. Process start
    /// is a state change, which is why this is `INFO`. A build with no commit warns
    /// instead: it cannot be traced back to source, and that is worth noticing in a lab.
    pub fn log_startup(&self, name: &str) {
        if self.is_untraceable() {
            tracing::warn!(
                binary = name,
                version = self.git_version,
                tree = self.git_tree_state,
                built = self.build_date,
                rustc = self.rustc_version,
                platform = self.platform,
                "starting a build with no source commit; it cannot be traced to a revision"
            );
            return;
        }
        tracing::info!(
            binary = name,
            version = %self,
            commit = self.git_commit,
            tree = self.git_tree_state,
            built = self.build_date,
            rustc = self.rustc_version,
            platform = self.platform,
            "starting"
        );
    }
}

/// The version, suffixed with the tree state when the build was not from a clean tree.
impl std::fmt::Display for Info {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_clean() {
            return f.write_str(self.git_version);
        }
        write!(f, "{}-{}", self.git_version, self.git_tree_state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_fact_is_populated() {
        let info = get();
        for (field, value) in [
            ("git_version", info.git_version),
            ("git_commit", info.git_commit),
            ("git_tree_state", info.git_tree_state),
            ("build_date", info.build_date),
            ("rustc_version", info.rustc_version),
            ("platform", info.platform),
        ] {
            assert!(!value.is_empty(), "{field} is empty; build.rs must fall back to a word");
        }
    }

    #[test]
    fn display_suffixes_the_tree_state_only_when_it_is_not_clean() {
        let clean = Info {
            git_version: "v1.2.3",
            git_tree_state: "clean",
            ..get()
        };
        assert_eq!(clean.to_string(), "v1.2.3");
        let dirty = Info {
            git_tree_state: "dirty",
            ..clean
        };
        assert_eq!(dirty.to_string(), "v1.2.3-dirty");
        let unknown = Info {
            git_tree_state: "unknown",
            ..clean
        };
        assert_eq!(
            unknown.to_string(),
            "v1.2.3-unknown",
            "an unknown tree is not a clean one"
        );
    }

    #[test]
    fn an_absent_commit_is_reported_as_untraceable() {
        assert!(
            Info {
                git_commit: "unknown",
                ..get()
            }
            .is_untraceable()
        );
        assert!(
            !Info {
                git_commit: "abc123",
                ..get()
            }
            .is_untraceable()
        );
    }

    #[test]
    fn info_serializes_with_the_field_names_the_flightctl_shape_uses() -> Result<(), serde_json::Error> {
        let json = serde_json::to_string(&Info {
            git_version: "v1.2.3",
            ..get()
        })?;
        for key in ["\"gitVersion\"", "\"gitCommit\"", "\"gitTreeState\"", "\"buildDate\""] {
            assert!(json.contains(key), "{key} missing from {json}");
        }
        Ok(())
    }
}
