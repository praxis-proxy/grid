//! Enrollment's service and signer commands must verify FIPS before touching state.

#![cfg(feature = "fips")]

#[path = "../../fips/test-support/startup.rs"]
mod startup;

#[cfg(test)]
mod tests {
    use super::startup;

    #[test]
    fn enrollment_fips_startup_contract() -> Result<(), Box<dyn std::error::Error>> {
        startup::assert_startup_contract(env!("CARGO_BIN_EXE_enrollment"), &[], &[&["--version"]])
    }

    #[test]
    #[cfg(feature = "bootstrap")]
    fn enrollment_bootstrap_fips_startup_contract() -> Result<(), Box<dyn std::error::Error>> {
        startup::assert_startup_contract(
            env!("CARGO_BIN_EXE_enrollment"),
            &["bootstrap"],
            &[&["bootstrap", "--help"]],
        )
    }

    #[test]
    #[cfg(feature = "bootstrap")]
    fn enrollment_invite_fips_startup_contract() -> Result<(), Box<dyn std::error::Error>> {
        startup::assert_startup_contract(
            env!("CARGO_BIN_EXE_enrollment"),
            &[
                "invite",
                "--url",
                "https://unused.invalid",
                "--ca-file",
                "unused-ca",
                "--admin-token-file",
                "unused-token",
                "--invites",
                "[]",
            ],
            &[&["invite", "--help"]],
        )
    }
}
