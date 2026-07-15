//! Certificate generation using [`rcgen`].
//!
//! Produces a self-signed CA and per-site certificates for
//! POC/testing. Production deployments use SPIFFE/SPIRE instead.

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose,
};

/// X.509 organization set on all generated site certificates.
///
/// Used by the Praxis `peer_identity_trust` filter to match
/// verified peer identity via the `organization` field.
/// Production deployments should use cert digest pinning or
/// SAN/SPIFFE identity instead.
pub const DEFAULT_ORGANIZATION: &str = "ai-grid";

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors from certificate generation.
#[derive(Debug, thiserror::Error)]
pub enum GenerateError {
    /// `rcgen` certificate generation failed.
    #[error("certificate generation failed: {0}")]
    Rcgen(#[from] rcgen::Error),

    /// CA certificate PEM could not be decoded.
    ///
    /// Returned by [`load_ca`] when `ca_cert_pem` is not valid PEM.
    #[error("CA certificate PEM could not be parsed")]
    InvalidCaCert,

    /// CA certificate and private key do not correspond to the same key pair.
    ///
    /// Detected by [`load_ca`] by checking that the key's public bytes appear
    /// in the certificate DER body.  Pass matching `ca_cert_pem` and
    /// `ca_key_pem` (both written by [`generate_ca`] in the same call), or
    /// run `cargo xtask env down && cargo xtask env up` to regenerate all
    /// certificates from a fresh CA.
    #[error(
        "CA certificate and private key do not match: regenerate with `cargo xtask env down && cargo xtask env up`"
    )]
    CaCertKeyMismatch,
}

// ---------------------------------------------------------------------------
// CA generation
// ---------------------------------------------------------------------------

/// A generated CA certificate and key pair.
#[derive(Debug)]
pub struct CaCert {
    /// PEM-encoded CA certificate.
    pub cert_pem: String,

    /// PEM-encoded CA private key.
    pub key_pem: String,

    /// The certificate parameters (for signing site certs).
    pub(crate) params: CertificateParams,

    /// The CA key pair (for signing site certs).
    pub(crate) key_pair: KeyPair,
}

/// Generate a self-signed CA certificate.
///
/// # Errors
///
/// Returns [`GenerateError`] if key generation or signing fails.
pub fn generate_ca(common_name: &str) -> Result<CaCert, GenerateError> {
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, common_name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    params.key_usages.push(KeyUsagePurpose::CrlSign);

    let key_pair = KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;

    Ok(CaCert {
        cert_pem: cert.pem(),
        key_pem: key_pair.serialize_pem(),
        params,
        key_pair,
    })
}

// ---------------------------------------------------------------------------
// Site certificate generation
// ---------------------------------------------------------------------------

/// A generated site certificate and key pair.
#[derive(Debug)]
pub struct SiteCertOutput {
    /// PEM-encoded site certificate.
    pub cert_pem: String,

    /// PEM-encoded site private key.
    pub key_pem: String,

    /// X.509 subject organization (`O=` field).
    pub organization: String,

    /// Subject Alternative Names on this certificate.
    pub sans: Vec<String>,
}

/// Generate a site certificate signed by the given CA.
///
/// The certificate includes DNS SANs for the site name
/// (e.g., `cluster-a.grid.internal`) and sets X.509
/// `OrganizationName` to [`DEFAULT_ORGANIZATION`] so the
/// Praxis `peer_identity_trust` filter can match on it.
///
/// # Errors
///
/// Returns [`GenerateError`] if key generation or signing fails.
pub fn generate_site_cert(ca: &CaCert, site_name: &str) -> Result<SiteCertOutput, GenerateError> {
    let dns_san = format!("{site_name}.grid.internal");
    let params = build_site_params(site_name, &dns_san)?;

    let site_key = KeyPair::generate()?;
    let issuer = Issuer::new(ca.params.clone(), &ca.key_pair);
    let cert = params.signed_by(&site_key, &issuer)?;

    Ok(SiteCertOutput {
        cert_pem: cert.pem(),
        key_pem: site_key.serialize_pem(),
        organization: DEFAULT_ORGANIZATION.to_owned(),
        sans: vec![dns_san],
    })
}

/// Generate a certificate signed by the given CA with a specific organization.
///
/// Identical to [`generate_site_cert`] except `OrganizationName` is set to
/// `org` rather than [`DEFAULT_ORGANIZATION`]. Use this to create test certs
/// that will fail `peer_identity_trust` org matching despite being signed by
/// the same trusted CA (TLS handshake succeeds; filter rejects).
///
/// # Errors
///
/// Returns [`GenerateError`] if key generation or signing fails.
pub fn generate_cert_with_org(ca: &CaCert, site_name: &str, org: &str) -> Result<SiteCertOutput, GenerateError> {
    let dns_san = format!("{site_name}.grid.internal");
    let params = build_site_params_with_org(site_name, &dns_san, org)?;

    let site_key = KeyPair::generate()?;
    let issuer = Issuer::new(ca.params.clone(), &ca.key_pair);
    let cert = params.signed_by(&site_key, &issuer)?;

    Ok(SiteCertOutput {
        cert_pem: cert.pem(),
        key_pem: site_key.serialize_pem(),
        organization: org.to_owned(),
        sans: vec![dns_san],
    })
}

/// Load an existing CA from PEM files and reconstruct a [`CaCert`] for signing.
///
/// Use this to reuse a CA that was previously generated and written to disk
/// rather than calling [`generate_ca`] again.  The `common_name` must match
/// what was used in the original [`generate_ca`] call so that the issuer `DN`
/// in newly-signed site certificates is correct.
///
/// The cert and key are validated to confirm they correspond to the same key
/// pair: the key's public bytes must appear in the certificate DER body.  This
/// catches the most common failure mode (mixed-up files after a partial
/// regeneration) and fails with a clear error before any signing attempt.
///
/// # Errors
///
/// Returns [`GenerateError::Rcgen`] if the key PEM is malformed.
/// Returns [`GenerateError::InvalidCaCert`] if the cert PEM cannot be decoded.
/// Returns [`GenerateError::CaCertKeyMismatch`] if cert and key do not match.
pub fn load_ca(common_name: &str, ca_key_pem: &str, ca_cert_pem: &str) -> Result<CaCert, GenerateError> {
    let key_pair = KeyPair::from_pem(ca_key_pem)?;

    // Validate that the cert corresponds to this key by checking that the
    // key's raw public bytes (uncompressed SEC1 point for ECDSA P-256:
    // `04 || X || Y`) appear as a subsequence in the certificate DER.  This
    // uses only the `pem` crate — already a transitive dep of `rcgen` — to
    // parse the PEM to DER without requiring the `x509-parser` feature.
    let cert_der = pem::parse(ca_cert_pem).map_err(|_pem_err| GenerateError::InvalidCaCert)?;
    let key_bytes = key_pair.public_key_raw();
    if !cert_der.contents().windows(key_bytes.len()).any(|w| w == key_bytes) {
        return Err(GenerateError::CaCertKeyMismatch);
    }

    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, common_name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    params.key_usages.push(KeyUsagePurpose::CrlSign);

    Ok(CaCert {
        cert_pem: ca_cert_pem.to_owned(),
        key_pem: ca_key_pem.to_owned(),
        params,
        key_pair,
    })
}

/// Build the certificate parameters for a site certificate.
///
/// Separated from [`generate_site_cert`] so tests can verify
/// the distinguished name entries without generating a full
/// signed certificate.
fn build_site_params(site_name: &str, dns_san: &str) -> Result<CertificateParams, GenerateError> {
    build_site_params_with_org(site_name, dns_san, DEFAULT_ORGANIZATION)
}

/// Build certificate parameters for a site certificate with a specific org.
fn build_site_params_with_org(
    site_name: &str,
    dns_san: &str,
    organization: &str,
) -> Result<CertificateParams, GenerateError> {
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, site_name);
    params.distinguished_name.push(DnType::OrganizationName, organization);
    params
        .subject_alt_names
        .push(rcgen::SanType::DnsName(dns_san.to_owned().try_into()?));
    params.extended_key_usages.push(ExtendedKeyUsagePurpose::ServerAuth);
    params.extended_key_usages.push(ExtendedKeyUsagePurpose::ClientAuth);
    Ok(params)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_ca_produces_pem() {
        let ca = generate_ca("AI Grid Test CA");
        assert!(ca.is_ok(), "CA generation should succeed");
        let ca = ca.unwrap_or_else(|_| std::process::abort());
        assert!(ca.cert_pem.contains("BEGIN CERTIFICATE"), "should be PEM cert");
        assert!(ca.key_pem.contains("BEGIN PRIVATE KEY"), "should be PEM key");
    }

    #[test]
    fn generate_site_cert_has_san() {
        let ca = generate_ca("Test CA").unwrap_or_else(|_| std::process::abort());
        let site = generate_site_cert(&ca, "cluster-a");
        assert!(site.is_ok(), "site cert generation should succeed");
        let site = site.unwrap_or_else(|_| std::process::abort());
        assert!(site.cert_pem.contains("BEGIN CERTIFICATE"), "should be PEM cert");
        assert!(site.key_pem.contains("BEGIN PRIVATE KEY"), "should be PEM key");
        assert_eq!(site.sans.len(), 1, "should have 1 SAN");
        assert_eq!(
            site.sans.first().map(String::as_str),
            Some("cluster-a.grid.internal"),
            "SAN should match site name"
        );
    }

    #[test]
    fn generate_site_cert_has_organization() {
        let ca = generate_ca("Test CA").unwrap_or_else(|_| std::process::abort());
        let site = generate_site_cert(&ca, "cluster-a").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            site.organization, DEFAULT_ORGANIZATION,
            "site cert output should carry the default organization"
        );
    }

    #[test]
    fn site_params_contain_correct_distinguished_name() {
        let params =
            build_site_params("cluster-a", "cluster-a.grid.internal").unwrap_or_else(|_| std::process::abort());
        let dn = &params.distinguished_name;

        let cn = dn.get(&DnType::CommonName);
        assert_eq!(
            cn,
            Some(&rcgen::DnValue::Utf8String("cluster-a".to_owned())),
            "CommonName should be the site name"
        );

        let org = dn.get(&DnType::OrganizationName);
        assert_eq!(
            org,
            Some(&rcgen::DnValue::Utf8String(DEFAULT_ORGANIZATION.to_owned())),
            "OrganizationName should be DEFAULT_ORGANIZATION"
        );
    }

    #[test]
    fn generate_cert_with_org_uses_requested_organization() {
        let ca = generate_ca("Test CA").unwrap_or_else(|_| std::process::abort());
        let site = generate_cert_with_org(&ca, "cluster-a", "not-ai-grid").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            site.organization, "not-ai-grid",
            "site cert output should carry the requested organization"
        );
    }

    #[test]
    fn custom_site_params_contain_requested_organization() {
        let params = build_site_params_with_org("cluster-a", "cluster-a.grid.internal", "not-ai-grid")
            .unwrap_or_else(|_| std::process::abort());
        let dn = &params.distinguished_name;

        let org = dn.get(&DnType::OrganizationName);
        assert_eq!(
            org,
            Some(&rcgen::DnValue::Utf8String("not-ai-grid".to_owned())),
            "OrganizationName should match the requested organization"
        );
    }

    #[test]
    fn different_sites_get_different_keys() {
        let ca = generate_ca("Test CA").unwrap_or_else(|_| std::process::abort());
        let a = generate_site_cert(&ca, "cluster-a").unwrap_or_else(|_| std::process::abort());
        let b = generate_site_cert(&ca, "cluster-b").unwrap_or_else(|_| std::process::abort());
        assert_ne!(a.key_pem, b.key_pem, "sites should have different keys");
        assert_ne!(a.cert_pem, b.cert_pem, "sites should have different certs");
    }

    #[test]
    fn ca_cert_differs_from_site_cert() {
        let ca = generate_ca("Test CA").unwrap_or_else(|_| std::process::abort());
        let site = generate_site_cert(&ca, "cluster-a").unwrap_or_else(|_| std::process::abort());
        assert_ne!(ca.cert_pem, site.cert_pem, "CA and site certs should differ");
        assert_ne!(ca.key_pem, site.key_pem, "CA and site keys should differ");
    }

    #[test]
    fn load_ca_with_matching_pair_succeeds() {
        let ca = generate_ca("Test CA").unwrap_or_else(|_| std::process::abort());
        let loaded = load_ca("Test CA", &ca.key_pem, &ca.cert_pem);
        assert!(loaded.is_ok(), "load_ca must succeed when cert and key match");
    }

    #[test]
    fn load_ca_with_malformed_key_pem_fails() {
        let ca = generate_ca("Test CA").unwrap_or_else(|_| std::process::abort());
        let result = load_ca("Test CA", "not a valid pem key", &ca.cert_pem);
        assert!(result.is_err(), "load_ca must fail when key PEM is malformed");
    }

    #[test]
    fn load_ca_with_mismatched_cert_and_key_fails() {
        let ca_a = generate_ca("CA-A").unwrap_or_else(|_| std::process::abort());
        let ca_b = generate_ca("CA-B").unwrap_or_else(|_| std::process::abort());
        let result = load_ca("CA-A", &ca_b.key_pem, &ca_a.cert_pem);
        assert!(
            matches!(result, Err(GenerateError::CaCertKeyMismatch)),
            "load_ca must return CaCertKeyMismatch when cert and key are from different CA pairs"
        );
    }
}
