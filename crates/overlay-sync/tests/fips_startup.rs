//! Overlay sync verifies FIPS before creating a client or touching its output directory.

#![cfg(feature = "fips")]

#[path = "../../fips/test-support/startup.rs"]
mod startup;

#[cfg(test)]
mod tests {
    use super::startup;

    #[test]
    fn overlay_sync_fips_startup_contract() -> Result<(), Box<dyn std::error::Error>> {
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
            ],
            &[&["--help"], &["--version"]],
        )
    }
}
