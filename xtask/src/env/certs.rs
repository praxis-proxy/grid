//! Certificate generation and distribution for the test environment.

use std::path::{Path, PathBuf};

use certs::{DEFAULT_ORGANIZATION, generate_ca, generate_cert_with_org, generate_site_cert, load_ca};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default output directory for generated certificates.
const CERTS_DIR: &str = "tests/env/certs";

/// CA common name.
const CA_CN: &str = "AI Grid Test CA";

/// Organization used in the wrong-org negative trust test.
///
/// A cert signed by the generated test CA with this org is used to prove
/// that `peer_identity_trust` enforces organization matching at the filter
/// layer (TLS handshake succeeds; filter rejects with HTTP 403).
pub(crate) const WRONG_ORG: &str = "not-ai-grid";

/// File name stem for the wrong-org client cert (cert + key).
const WRONG_ORG_CERT_NAME: &str = "wrong-org-client";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Generate a CA and per-cluster certificates.
///
/// Writes PEM files to `{certs_dir}/ca.pem`, `{certs_dir}/ca-key.pem`,
/// and per-cluster `{certs_dir}/{name}-cert.pem`, `{certs_dir}/{name}-key.pem`.
///
/// If `ca.pem` and `ca-key.pem` already exist they are reused, so that
/// running `env up` for a second topology (e.g., adding clusters) does not
/// invalidate certificates already distributed to existing clusters.  The CA
/// is only regenerated when neither file is present (fresh environment) or
/// when `env down` has cleaned the directory.
///
/// # Errors
///
/// Returns an error if certificate generation or file writes fail.
pub(crate) fn generate_all(cluster_names: &[String]) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let dir = PathBuf::from(CERTS_DIR);
    std::fs::create_dir_all(&dir)?;

    let ca_was_complete = dir.join("ca.pem").exists() && dir.join("ca-key.pem").exists();
    let ca = load_or_generate_ca(&dir)?;

    for name in cluster_names {
        if ca_was_complete && identity_exists(&dir, name) {
            restrict_private_key(&dir.join(format!("{name}-key.pem")))?;
            eprintln!("  reusing cert for {name}");
            continue;
        }
        let site = generate_site_cert(&ca, name)?;
        write_pem(&dir.join(format!("{name}-cert.pem")), &site.cert_pem)?;
        write_pem(&dir.join(format!("{name}-key.pem")), &site.key_pem)?;
        eprintln!("  generated cert for {name} (SAN: {})", site.sans.join(", "));
    }

    ensure_wrong_org_identity(&dir, &ca, cluster_names, ca_was_complete)?;

    Ok(dir)
}

/// Remove the generated certificates directory.
///
/// # Errors
///
/// Returns an error if the directory cannot be removed.
pub(crate) fn cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let dir = PathBuf::from(CERTS_DIR);
    if dir.exists() {
        std::fs::remove_dir_all(&dir)?;
        eprintln!("  removed certificate directory");
    }
    Ok(())
}

/// Check whether the certificates directory exists and has a CA cert.
pub(crate) fn certs_exist() -> bool {
    Path::new(CERTS_DIR).join("ca.pem").exists()
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Return whether both files for one generated identity are present.
fn identity_exists(dir: &Path, name: &str) -> bool {
    dir.join(format!("{name}-cert.pem")).is_file() && dir.join(format!("{name}-key.pem")).is_file()
}

/// Create or reuse the same-CA, wrong-organization negative-test identity.
fn ensure_wrong_org_identity(
    dir: &Path,
    ca: &certs::CaCert,
    cluster_names: &[String],
    ca_was_complete: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if ca_was_complete && identity_exists(dir, WRONG_ORG_CERT_NAME) {
        restrict_private_key(&dir.join(format!("{WRONG_ORG_CERT_NAME}-key.pem")))?;
        eprintln!("  reusing wrong-org cert");
        return Ok(());
    }

    let first_cluster = cluster_names.first().map_or("consumer", String::as_str);
    let wrong_org_cert = generate_cert_with_org(ca, first_cluster, WRONG_ORG)?;
    write_pem(
        &dir.join(format!("{WRONG_ORG_CERT_NAME}-cert.pem")),
        &wrong_org_cert.cert_pem,
    )?;
    write_pem(
        &dir.join(format!("{WRONG_ORG_CERT_NAME}-key.pem")),
        &wrong_org_cert.key_pem,
    )?;
    eprintln!("  generated wrong-org cert (org={WRONG_ORG}, expected={DEFAULT_ORGANIZATION})");
    Ok(())
}

/// Write PEM content to a file.
fn write_pem(path: &Path, content: &str) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::write(path, content)?;
    restrict_private_key(path)?;
    Ok(())
}

/// Restrict generated private keys while leaving certificate files readable.
fn restrict_private_key(path: &Path) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        let is_private_key = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .is_some_and(|name| name.ends_with("-key.pem"));
        let mode = if is_private_key { 0o600 } else { 0o644 };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// Load the existing CA from disk, or generate and write a new one.
pub(crate) fn load_or_generate_ca(dir: &Path) -> Result<certs::CaCert, Box<dyn std::error::Error>> {
    let ca_cert_path = dir.join("ca.pem");
    let ca_key_path = dir.join("ca-key.pem");

    if ca_cert_path.exists() && ca_key_path.exists() {
        let cert_pem = std::fs::read_to_string(&ca_cert_path)?;
        let key_pem = std::fs::read_to_string(&ca_key_path)?;
        eprintln!("  reusing existing CA certificate");
        Ok(load_ca(CA_CN, &key_pem, &cert_pem)?)
    } else {
        let ca = generate_ca(CA_CN)?;
        write_pem(&ca_cert_path, &ca.cert_pem)?;
        write_pem(&ca_key_path, &ca.key_pem)?;
        eprintln!("  generated CA certificate");
        Ok(ca)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_all_creates_files() {
        let clusters = vec!["test-a".to_owned(), "test-b".to_owned()];

        let test_dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());

        let ca = generate_ca(CA_CN).unwrap_or_else(|_| std::process::abort());
        let ca_path = test_dir.path().join("ca.pem");
        write_pem(&ca_path, &ca.cert_pem).unwrap_or_default();
        assert!(ca_path.exists(), "CA cert should be written");

        for name in &clusters {
            let site = generate_site_cert(&ca, name).unwrap_or_else(|_| std::process::abort());
            let cert_path = test_dir.path().join(format!("{name}-cert.pem"));
            write_pem(&cert_path, &site.cert_pem).unwrap_or_default();
            assert!(cert_path.exists(), "site cert for {name} should exist");
        }
    }

    #[test]
    fn write_pem_creates_file() {
        let test_dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let path = test_dir.path().join("grid-certs-write-test.pem");
        let result = write_pem(&path, "TEST-PEM-DATA");
        assert!(result.is_ok(), "write should succeed");
        let content = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(content, "TEST-PEM-DATA", "content should match");
    }

    #[test]
    fn identity_exists_requires_cert_and_key() {
        let test_dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        write_pem(&test_dir.path().join("edge-cert.pem"), "CERT").unwrap_or_else(|_| std::process::abort());
        assert!(!identity_exists(test_dir.path(), "edge"));
        write_pem(&test_dir.path().join("edge-key.pem"), "KEY").unwrap_or_else(|_| std::process::abort());
        assert!(identity_exists(test_dir.path(), "edge"));
    }

    #[cfg(unix)]
    #[test]
    fn write_pem_restricts_private_key_permissions() {
        use std::os::unix::fs::PermissionsExt as _;

        let test_dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let path = test_dir.path().join("grid-certs-permissions-key.pem");
        write_pem(&path, "TEST-PRIVATE-KEY").unwrap_or_else(|_| std::process::abort());

        let mode = std::fs::metadata(&path)
            .unwrap_or_else(|_| std::process::abort())
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "private key should be owner-readable only");
    }
}
