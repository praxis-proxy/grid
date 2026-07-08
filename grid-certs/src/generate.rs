//! Certificate generation using [`rcgen`].
//!
//! Produces a self-signed CA and per-site certificates for
//! POC/testing. Production deployments use SPIFFE/SPIRE instead.

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose,
};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors from certificate generation.
#[derive(Debug, thiserror::Error)]
pub enum GenerateError {
    /// `rcgen` certificate generation failed.
    #[error("certificate generation failed: {0}")]
    Rcgen(#[from] rcgen::Error),
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

    /// Subject Alternative Names on this certificate.
    pub sans: Vec<String>,
}

/// Generate a site certificate signed by the given CA.
///
/// The certificate includes DNS SANs for the site name
/// (e.g., `cluster-a.grid.internal`).
///
/// # Errors
///
/// Returns [`GenerateError`] if key generation or signing fails.
pub fn generate_site_cert(ca: &CaCert, site_name: &str) -> Result<SiteCertOutput, GenerateError> {
    let dns_san = format!("{site_name}.grid.internal");

    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, site_name);
    params
        .subject_alt_names
        .push(rcgen::SanType::DnsName(dns_san.clone().try_into()?));
    params.extended_key_usages.push(ExtendedKeyUsagePurpose::ServerAuth);
    params.extended_key_usages.push(ExtendedKeyUsagePurpose::ClientAuth);

    let site_key = KeyPair::generate()?;
    let issuer = Issuer::new(ca.params.clone(), &ca.key_pair);
    let cert = params.signed_by(&site_key, &issuer)?;

    Ok(SiteCertOutput {
        cert_pem: cert.pem(),
        key_pem: site_key.serialize_pem(),
        sans: vec![dns_san],
    })
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
}
