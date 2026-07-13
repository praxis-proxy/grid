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
/// that `grid_ingress_trust` enforces organization matching at the filter
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

    let ca = load_or_generate_ca(&dir)?;

    for name in cluster_names {
        let site = generate_site_cert(&ca, name)?;
        write_pem(&dir.join(format!("{name}-cert.pem")), &site.cert_pem)?;
        write_pem(&dir.join(format!("{name}-key.pem")), &site.key_pem)?;
        eprintln!("  generated cert for {name} (SAN: {})", site.sans.join(", "));
    }

    let first_cluster = cluster_names.first().map_or("consumer", String::as_str);
    let wrong_org_cert = generate_cert_with_org(&ca, first_cluster, WRONG_ORG)?;
    write_pem(
        &dir.join(format!("{WRONG_ORG_CERT_NAME}-cert.pem")),
        &wrong_org_cert.cert_pem,
    )?;
    write_pem(
        &dir.join(format!("{WRONG_ORG_CERT_NAME}-key.pem")),
        &wrong_org_cert.key_pem,
    )?;
    eprintln!("  generated wrong-org cert (org={WRONG_ORG}, expected={DEFAULT_ORGANIZATION})");

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

/// Write PEM content to a file.
fn write_pem(path: &Path, content: &str) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::write(path, content)?;
    Ok(())
}

/// Load the existing CA from disk, or generate and write a new one.
fn load_or_generate_ca(dir: &Path) -> Result<certs::CaCert, Box<dyn std::error::Error>> {
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

        let test_dir = std::env::temp_dir().join("grid-certs-xtask-test");
        std::fs::create_dir_all(&test_dir).unwrap_or_default();

        let ca = generate_ca(CA_CN).unwrap_or_else(|_| std::process::abort());
        let ca_path = test_dir.join("ca.pem");
        write_pem(&ca_path, &ca.cert_pem).unwrap_or_default();
        assert!(ca_path.exists(), "CA cert should be written");

        for name in &clusters {
            let site = generate_site_cert(&ca, name).unwrap_or_else(|_| std::process::abort());
            let cert_path = test_dir.join(format!("{name}-cert.pem"));
            write_pem(&cert_path, &site.cert_pem).unwrap_or_default();
            assert!(cert_path.exists(), "site cert for {name} should exist");
        }

        let _cleanup = std::fs::remove_dir_all(&test_dir);
    }

    #[test]
    fn write_pem_creates_file() {
        let path = std::env::temp_dir().join("grid-certs-write-test.pem");
        let result = write_pem(&path, "TEST-PEM-DATA");
        assert!(result.is_ok(), "write should succeed");
        let content = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(content, "TEST-PEM-DATA", "content should match");
        let _cleanup = std::fs::remove_file(&path);
    }
}
