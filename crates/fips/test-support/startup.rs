//! Subprocess assertions shared by the control-plane FIPS startup tests.

use std::{error::Error, io::Write as _, process::Command};

/// Check refusal for disabled properties and selected-but-unavailable FIPS,
/// retaining informational CLI access in both cases.
pub(super) fn assert_startup_contract(
    binary: &str,
    service_args: &[&str],
    information_args: &[&[&str]],
) -> Result<(), Box<dyn Error>> {
    for (properties, reason) in [
        ("fips=no", "OpenSSL default properties do not enable FIPS"),
        ("fips=yes", "OpenSSL approved digest unavailable"),
    ] {
        let modules = tempfile::tempdir()?;
        let mut config = tempfile::NamedTempFile::new()?;
        write!(
            config,
            "openssl_conf = grid_init\n[grid_init]\nalg_section = grid_algorithms\n[grid_algorithms]\ndefault_properties = {properties}\n"
        )?;
        let output = Command::new(binary)
            .args(service_args)
            .env("OPENSSL_CONF", config.path())
            .env("OPENSSL_MODULES", modules.path())
            .output()?;
        let logs = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !output.status.success(),
            "an unusable FIPS runtime must refuse service startup: {logs}"
        );
        assert!(
            logs.contains("FIPS startup verification failed") && logs.contains(reason),
            "{properties}: refusal must come from the FIPS guard, before Kubernetes or configuration access: {logs}"
        );
        for args in information_args {
            let info_output = Command::new(binary)
                .args(*args)
                .env("OPENSSL_CONF", config.path())
                .env("OPENSSL_MODULES", modules.path())
                .output()?;
            assert!(
                info_output.status.success(),
                "informational arguments {args:?} remain available: {}",
                String::from_utf8_lossy(&info_output.stderr)
            );
        }
    }
    Ok(())
}
