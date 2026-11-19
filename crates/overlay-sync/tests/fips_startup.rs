//! Overlay sync verifies FIPS before creating a client or touching its output directory.

#![cfg(feature = "fips")]

#[path = "../../fips/test-support/startup.rs"]
mod startup;

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::startup;

    #[test]
    fn overlay_sync_fips_startup_contract() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let output_dir = dir.path().join("untouched");
        let output = output_dir.join("serving.json");
        check_output(&output)?;
        assert!(
            !output_dir.exists(),
            "FIPS rejection must not create the output directory"
        );
        Ok(())
    }

    #[test]
    fn fips_rejection_preserves_existing_output() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let output = dir.path().join("serving.json");
        let sentinel = b"previous serving config";
        fs::write(&output, sentinel)?;
        let modified = output.metadata()?.modified()?;
        check_output(&output)?;
        assert_eq!(
            fs::read(&output)?,
            sentinel,
            "FIPS rejection must preserve existing config bytes"
        );
        assert_eq!(
            output.metadata()?.modified()?,
            modified,
            "FIPS rejection must not rewrite existing config"
        );
        assert_eq!(
            fs::read_dir(dir.path())?.count(),
            1,
            "FIPS rejection must not leave additional output files"
        );
        Ok(())
    }

    // Test Utilities

    /// Run both refusal cases against the caller's controlled output file.
    fn check_output(output: &Path) -> Result<(), Box<dyn std::error::Error>> {
        startup::assert_startup_contract(
            env!("CARGO_BIN_EXE_grid-overlay-sync"),
            &[
                "--config-map",
                "unused",
                "--expected-network",
                "grid",
                "--expected-gateway",
                "gateway",
                "--expected-local-site",
                "site-a",
                "--output",
                output.to_str().ok_or("temporary output path must be UTF-8")?,
            ],
            &[&["--help"], &["--version"]],
        )
    }
}
