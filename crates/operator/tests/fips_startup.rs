//! The deployed operator must reject a non-FIPS runtime before creating clients.

#![cfg(feature = "fips")]

#[path = "../../fips/test-support/startup.rs"]
mod startup;

#[cfg(test)]
mod tests {
    use super::startup;

    #[test]
    fn operator_fips_startup_contract() -> Result<(), Box<dyn std::error::Error>> {
        startup::assert_startup_contract(env!("CARGO_BIN_EXE_operator"), &[], &[&["--help"], &["--version"]])
    }
}
