//! Verify the host configuration required by Grid's FIPS runtime binaries.
//!
//! Verification does not enable FIPS mode or establish deployment certification.

#![forbid(unsafe_code)]

#[cfg(feature = "runtime")]
use openssl::{
    hash::{MessageDigest, hash},
    md::Md,
};

/// A missing prerequisite for the supported FIPS deployment configuration.
#[derive(Debug, thiserror::Error)]
pub enum FipsError {
    /// A required host configuration file could not be read.
    #[error("FIPS startup verification failed: cannot read {path}: {source}")]
    HostFile {
        /// Host file needed to establish the runtime configuration.
        path: &'static str,
        /// Operating system read failure.
        source: std::io::Error,
    },
    /// The kernel did not explicitly report FIPS mode.
    #[error("FIPS startup verification failed: /proc/sys/crypto/fips_enabled must be 1")]
    Kernel,
    /// The process's OpenSSL default properties do not require approved algorithms.
    #[error("FIPS startup verification failed: OpenSSL default properties do not enable FIPS")]
    OpenSslDisabled,
    /// The selected provider could not fetch or execute an approved digest.
    #[cfg(feature = "runtime")]
    #[error("FIPS startup verification failed: OpenSSL approved digest unavailable: {0}")]
    Digest(#[from] openssl::error::ErrorStack),
    /// The digest operation did not produce its known answer.
    #[error("FIPS startup verification failed: OpenSSL SHA-256 known-answer check failed")]
    SelfTest,
}

/// Verify the current host and OpenSSL configuration before starting services.
///
/// Requires the default OpenSSL context to select FIPS-approved algorithms and
/// the FIPS provider to supply SHA-256. A known-answer digest exercises the
/// default context actually used by the control-plane TLS and identity code.
/// The kernel must report FIPS mode. The container-visible crypto policy is
/// logged for diagnostics only: it may differ from the effective host policy.
/// OpenSSL initializes its configured providers; this function does not
/// explicitly load providers or enable FIPS properties.
///
/// # Errors
///
/// Refuses a missing or non-FIPS kernel flag, disabled FIPS properties, or
/// failure of the approved digest check. This is configuration verification,
/// not a claim that all cryptography in the binary is inside a validated module.
#[cfg(feature = "runtime")]
pub fn verify() -> Result<(), FipsError> {
    if !grid_openssl_fips::enabled() {
        return Err(FipsError::OpenSslDisabled);
    }
    let _approved = Md::fetch(None, "SHA256", Some("provider=fips,fips=yes"))?;
    let digest = hash(MessageDigest::sha256(), b"abc")?;
    if digest.as_ref()
        != [
            0xBA, 0x78, 0x16, 0xBF, 0x8F, 0x01, 0xCF, 0xEA, 0x41, 0x41, 0x40, 0xDE, 0x5D, 0xAE, 0x22, 0x23, 0xB0, 0x03,
            0x61, 0xA3, 0x96, 0x17, 0x7A, 0x9C, 0xB4, 0x10, 0xFF, 0x61, 0xF2, 0x00, 0x15, 0xAD,
        ]
    {
        return Err(FipsError::SelfTest);
    }
    let read = |path| std::fs::read_to_string(path).map_err(|source| FipsError::HostFile { path, source });
    check_host(
        &read("/proc/sys/crypto/fips_enabled")?,
        std::fs::read_to_string("/etc/crypto-policies/config"),
    )?;
    Ok(())
}

/// Require the kernel FIPS flag and report the container policy for diagnostics.
#[cfg(any(feature = "runtime", test))]
fn check_host(kernel: &str, policy: std::io::Result<String>) -> Result<(), FipsError> {
    if kernel.trim() != "1" {
        return Err(FipsError::Kernel);
    }
    match policy {
        Ok(policy) => tracing::info!(
            policy = policy.trim(),
            "container crypto policy is diagnostic only; effective OpenSSL FIPS checks passed"
        ),
        Err(error) => tracing::warn!(
            %error,
            "cannot read container crypto policy; diagnostic only, effective OpenSSL FIPS checks passed"
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{FipsError, check_host};

    #[test]
    fn only_an_explicit_kernel_fips_flag_is_accepted() {
        for kernel in ["", "0", "true", "2", "1 extra"] {
            assert!(
                matches!(check_host(kernel, Ok("FIPS".to_owned())), Err(FipsError::Kernel)),
                "kernel value {kernel:?} must be refused"
            );
        }
        assert!(
            matches!(check_host("1\n", Ok("FIPS".to_owned())), Ok(())),
            "kernel flag 1 with a trailing newline must be accepted"
        );
    }

    #[test]
    fn container_policy_content_is_diagnostic_only() {
        for policy in ["FIPS", "FIPS:OSPP", "DEFAULT", "", "UNKNOWN", "FIPS:SHA1"] {
            assert!(
                check_host("1", Ok(policy.to_owned())).is_ok(),
                "container policy {policy:?} must not reject a FIPS kernel"
            );
            assert!(
                matches!(check_host("0", Ok(policy.to_owned())), Err(FipsError::Kernel)),
                "container policy {policy:?} must not bypass the kernel check"
            );
        }
    }

    #[test]
    fn missing_or_unreadable_container_policy_is_diagnostic_only() {
        for kind in [std::io::ErrorKind::NotFound, std::io::ErrorKind::PermissionDenied] {
            assert!(
                check_host("1", Err(std::io::Error::from(kind))).is_ok(),
                "policy read error {kind:?} must not reject a FIPS kernel"
            );
            assert!(
                matches!(check_host("0", Err(std::io::Error::from(kind))), Err(FipsError::Kernel)),
                "policy read error {kind:?} must not bypass the kernel check"
            );
        }
    }
}
