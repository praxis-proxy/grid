//! Certificate generation.
//!
//! Produces a self-signed CA and per-site certificates for POC/testing.
//! Production deployments use SPIFFE/SPIRE instead. The crypto primitive is
//! selected at compile time by the [`backend`] seam.

use time::{Duration, OffsetDateTime};

use crate::backend::{self, BackendError, CertSpec};

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
    /// Key generation or signing failed in the crypto backend.
    #[error("certificate generation failed: {0}")]
    Backend(String),

    /// CA certificate PEM could not be decoded.
    ///
    /// Returned by [`load_ca`] when `ca_cert_pem` is not valid PEM.
    #[error("CA certificate PEM could not be parsed")]
    InvalidCaCert,

    /// CA certificate and private key do not correspond to the same key pair.
    ///
    /// Detected by [`load_ca`]. Pass matching `ca_cert_pem` and `ca_key_pem`
    /// (both written by [`generate_ca`] in the same call), or run
    /// `cargo xtask env down && cargo xtask env up` to regenerate all
    /// certificates from a fresh CA.
    #[error(
        "CA certificate and private key do not match: regenerate with `cargo xtask env down && cargo xtask env up`"
    )]
    CaCertKeyMismatch,
}

/// Map a backend failure onto the generation error it stands for.
fn map_backend(err: BackendError) -> GenerateError {
    match err {
        BackendError::CaCertKeyMismatch => GenerateError::CaCertKeyMismatch,
        BackendError::InvalidCaCert => GenerateError::InvalidCaCert,
        BackendError::KeyGen(msg) | BackendError::Sign(msg) | BackendError::InvalidCaKey(msg) => {
            GenerateError::Backend(msg)
        },
        // No generate or load path produces a request-side failure.
        BackendError::ParseCsr
        | BackendError::CsrBadSignature
        | BackendError::CsrUnsupportedExtension
        | BackendError::BadSignature => GenerateError::Backend("unexpected backend error".to_owned()),
    }
}

// ---------------------------------------------------------------------------
// CA generation
// ---------------------------------------------------------------------------

/// A generated CA certificate and key pair.
#[derive(Debug)]
#[expect(
    clippy::partial_pub_fields,
    reason = "cert_pem/key_pem are public API; material is internal signing state"
)]
pub struct CaCert {
    /// PEM-encoded CA certificate.
    pub cert_pem: String,

    /// PEM-encoded CA private key.
    pub key_pem: String,

    /// Backend material for signing site certificates.
    pub(crate) material: backend::CaMaterial,
}

/// Generate a self-signed CA certificate.
///
/// # Errors
///
/// Returns [`GenerateError`] if key generation or signing fails.
pub fn generate_ca(common_name: &str) -> Result<CaCert, GenerateError> {
    let (not_before, not_after) = default_ca_validity();
    let spec = CertSpec {
        common_name,
        organization: None,
        dns_sans: &[],
        uri_sans: &[],
        is_ca: true,
        not_before,
        not_after,
    };
    let ca = backend::generate_ca(&spec).map_err(map_backend)?;
    Ok(CaCert {
        cert_pem: ca.cert_pem,
        key_pem: ca.key_pem,
        material: ca.material,
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
    generate_dns_cert(ca, site_name, &dns_san)
}

/// Trust domain every site in this grid is named under.
pub const SPIFFE_TRUST_DOMAIN: &str = "grid.internal";

/// Generate a site certificate carrying extra DNS names.
///
/// A site is reached by more than one name. Peers dial it by its grid identity,
/// and workloads inside its own cluster reach the same listener through a
/// Service. One certificate has to answer to both, or the in-cluster caller
/// fails hostname verification against a certificate that is otherwise correct.
///
/// # Errors
///
/// Returns [`GenerateError`] if any name is invalid or signing fails.
pub fn generate_site_cert_with_names(
    ca: &CaCert,
    site_name: &str,
    extra: &[String],
) -> Result<SiteCertOutput, GenerateError> {
    let primary = format!("{site_name}.grid.internal");
    let mut id = site_identity(site_name, &primary);
    id.dns_sans.extend_from_slice(extra);

    let (not_before, not_after) = default_leaf_validity();
    let out = backend::issue_leaf(&ca.material, &id.spec(not_before, not_after)).map_err(map_backend)?;
    Ok(SiteCertOutput {
        cert_pem: out.cert_pem,
        key_pem: out.key_pem,
        organization: DEFAULT_ORGANIZATION.to_owned(),
        sans: id.dns_sans,
    })
}

/// Generate a certificate for an exact DNS name, signed by the given CA.
///
/// Unlike [`generate_site_cert`], this function does not append the Grid
/// internal DNS suffix. Callers use it when the complete service DNS name is
/// already known.
///
/// # Errors
///
/// Returns [`GenerateError`] if the DNS name is invalid or certificate
/// generation fails.
pub fn generate_dns_cert(ca: &CaCert, common_name: &str, dns_name: &str) -> Result<SiteCertOutput, GenerateError> {
    let id = site_identity(common_name, dns_name);
    let (not_before, not_after) = default_leaf_validity();
    let out = backend::issue_leaf(&ca.material, &id.spec(not_before, not_after)).map_err(map_backend)?;
    Ok(SiteCertOutput {
        cert_pem: out.cert_pem,
        key_pem: out.key_pem,
        organization: DEFAULT_ORGANIZATION.to_owned(),
        sans: vec![dns_name.to_owned()],
    })
}

/// Generate a certificate that has already expired, signed by the given CA.
///
/// Sets `notBefore` to 2 days before now and `notAfter` to 1 day before now,
/// producing a certificate that will always fail validity checks.
///
/// # Errors
///
/// Returns [`GenerateError`] if certificate generation fails.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "now +/- 1-2 days cannot overflow OffsetDateTime"
)]
pub fn generate_expired_dns_cert(
    ca: &CaCert,
    common_name: &str,
    dns_name: &str,
) -> Result<SiteCertOutput, GenerateError> {
    let now = OffsetDateTime::now_utc();
    generate_validity_bounded_dns_cert(
        ca,
        common_name,
        dns_name,
        now - Duration::days(2),
        now - Duration::days(1),
    )
}

/// Generate a certificate that is not yet valid, signed by the given CA.
///
/// Sets `notBefore` to 1 day from now and `notAfter` to 2 days from now,
/// producing a certificate that will always fail validity checks.
///
/// # Errors
///
/// Returns [`GenerateError`] if certificate generation fails.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "now +/- 1-2 days cannot overflow OffsetDateTime"
)]
pub fn generate_not_yet_valid_dns_cert(
    ca: &CaCert,
    common_name: &str,
    dns_name: &str,
) -> Result<SiteCertOutput, GenerateError> {
    let now = OffsetDateTime::now_utc();
    generate_validity_bounded_dns_cert(
        ca,
        common_name,
        dns_name,
        now + Duration::days(1),
        now + Duration::days(2),
    )
}

/// Generate a certificate with explicit validity bounds, signed by the given CA.
fn generate_validity_bounded_dns_cert(
    ca: &CaCert,
    common_name: &str,
    dns_name: &str,
    not_before: OffsetDateTime,
    not_after: OffsetDateTime,
) -> Result<SiteCertOutput, GenerateError> {
    let id = site_identity(common_name, dns_name);
    let out = backend::issue_leaf(&ca.material, &id.spec(not_before, not_after)).map_err(map_backend)?;
    Ok(SiteCertOutput {
        cert_pem: out.cert_pem,
        key_pem: out.key_pem,
        organization: DEFAULT_ORGANIZATION.to_owned(),
        sans: vec![dns_name.to_owned()],
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
    let id = site_identity_with_org(site_name, &dns_san, org);
    let (not_before, not_after) = default_leaf_validity();
    let out = backend::issue_leaf(&ca.material, &id.spec(not_before, not_after)).map_err(map_backend)?;
    Ok(SiteCertOutput {
        cert_pem: out.cert_pem,
        key_pem: out.key_pem,
        organization: org.to_owned(),
        sans: vec![dns_san],
    })
}

/// Load an existing CA from PEM files and reconstruct a [`CaCert`] for signing.
///
/// Use this to reuse a CA that was previously generated and written to disk
/// rather than calling [`generate_ca`] again. The `common_name` must match
/// what was used in the original [`generate_ca`] call so that the issuer `DN`
/// in newly-signed site certificates is correct.
///
/// The cert and key are validated to confirm they correspond to the same key
/// pair. This catches the most common failure mode (mixed-up files after a
/// partial regeneration) and fails with a clear error before any signing.
///
/// # Errors
///
/// Returns [`GenerateError::Backend`] if the key PEM is malformed.
/// Returns [`GenerateError::InvalidCaCert`] if the cert PEM cannot be decoded.
/// Returns [`GenerateError::CaCertKeyMismatch`] if cert and key do not match.
pub fn load_ca(common_name: &str, ca_key_pem: &str, ca_cert_pem: &str) -> Result<CaCert, GenerateError> {
    let (not_before, not_after) = default_ca_validity();
    let spec = CertSpec {
        common_name,
        organization: None,
        dns_sans: &[],
        uri_sans: &[],
        is_ca: true,
        not_before,
        not_after,
    };
    let material = backend::load_ca(&spec, ca_key_pem, ca_cert_pem).map_err(map_backend)?;
    Ok(CaCert {
        cert_pem: ca_cert_pem.to_owned(),
        key_pem: ca_key_pem.to_owned(),
        material,
    })
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

/// The SPIFFE ID a site is known by inside the grid.
///
/// One trust domain per grid. The path names the site, so a verifier reads
/// which site is calling from material the issuer signed.
#[must_use]
pub fn spiffe_id(site_name: &str) -> String {
    format!("spiffe://{SPIFFE_TRUST_DOMAIN}/site/{site_name}")
}

/// The identity a site certificate binds: subject name, organization, and SANs.
///
/// An organization proves membership and a DNS name proves where to dial.
/// Neither answers which site is calling, which is why the SPIFFE URI SAN names
/// the site, bound by the signature rather than asserted beside it.
pub(crate) struct SiteIdentity {
    /// Subject common name (the site name).
    pub(crate) common_name: String,
    /// Subject organization, proving membership.
    pub(crate) organization: String,
    /// DNS SANs, naming where to dial the site.
    pub(crate) dns_sans: Vec<String>,
    /// SPIFFE URI SANs, naming which site is calling.
    pub(crate) uri_sans: Vec<String>,
}

impl SiteIdentity {
    /// The certificate spec for this identity over the given validity window.
    pub(crate) fn spec(&self, not_before: OffsetDateTime, not_after: OffsetDateTime) -> CertSpec<'_> {
        CertSpec {
            common_name: &self.common_name,
            organization: Some(&self.organization),
            dns_sans: &self.dns_sans,
            uri_sans: &self.uri_sans,
            is_ca: false,
            not_before,
            not_after,
        }
    }
}

/// Build the identity for a site certificate under [`DEFAULT_ORGANIZATION`].
pub(crate) fn site_identity(site_name: &str, dns_san: &str) -> SiteIdentity {
    site_identity_with_org(site_name, dns_san, DEFAULT_ORGANIZATION)
}

/// Build the identity for a site certificate with a specific organization.
pub(crate) fn site_identity_with_org(site_name: &str, dns_san: &str, organization: &str) -> SiteIdentity {
    SiteIdentity {
        common_name: site_name.to_owned(),
        organization: organization.to_owned(),
        dns_sans: vec![dns_san.to_owned()],
        uri_sans: vec![spiffe_id(site_name)],
    }
}

/// Validity window for a generated leaf certificate.
///
/// Backdated slightly so a verifier whose clock runs behind does not refuse a
/// certificate issued moments ago.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "now +/- a fixed span cannot overflow OffsetDateTime"
)]
fn default_leaf_validity() -> (OffsetDateTime, OffsetDateTime) {
    let now = OffsetDateTime::now_utc();
    (now - Duration::minutes(5), now + Duration::days(365))
}

/// Validity window for a generated CA certificate.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "now +/- a fixed span cannot overflow OffsetDateTime"
)]
fn default_ca_validity() -> (OffsetDateTime, OffsetDateTime) {
    let now = OffsetDateTime::now_utc();
    (now - Duration::minutes(5), now + Duration::days(3650))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {

    #[test]
    fn a_site_certificate_can_answer_to_more_than_one_name() {
        // Peers dial the grid identity; a workload in the site's own cluster
        // reaches the same listener through a Service. One certificate, both
        // names, or the in-cluster caller fails hostname verification against a
        // certificate that is otherwise correct.
        let ca = generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let svc = "grid-operator-signals.grid-system.svc.cluster.local".to_owned();
        let site = generate_site_cert_with_names(&ca, "pool-a", std::slice::from_ref(&svc))
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            site.sans,
            vec!["pool-a.grid.internal".to_owned(), svc],
            "the grid identity stays first and the Service name is added"
        );
    }

    #[test]
    fn no_extra_names_matches_the_plain_site_certificate() {
        let ca = generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let with = generate_site_cert_with_names(&ca, "pool-a", &[]).unwrap_or_else(|_| std::process::abort());
        let plain = generate_site_cert(&ca, "pool-a").unwrap_or_else(|_| std::process::abort());
        assert_eq!(with.sans, plain.sans, "adding nothing changes nothing");
    }
    use super::*;

    /// Site the interop fixtures are minted for.
    #[cfg(test)]
    const INTEROP_FIXTURE_SITE: &str = "alpha";

    /// Fixture directory for the backend this binary was built with.
    #[cfg(feature = "fips")]
    const INTEROP_FIXTURE_DIR: &str = "tests/fixtures/openssl";
    /// Fixture directory for the backend this binary was built with.
    #[cfg(not(feature = "fips"))]
    const INTEROP_FIXTURE_DIR: &str = "tests/fixtures/rcgen";

    /// Regenerate this backend's interop fixtures under `tests/fixtures/<backend>/`.
    ///
    /// Ignored in normal runs. Run once per backend to refresh the committed PEM.
    ///
    /// ```text
    /// cargo test -p certs -- --ignored regenerate_interop_fixtures
    /// cargo test -p certs --no-default-features --features fips -- --ignored regenerate_interop_fixtures
    /// ```
    ///
    /// The leaf is dated far ahead so the committed fixture does not expire.
    #[test]
    #[ignore = "writes fixtures to disk, run explicitly to regenerate"]
    fn regenerate_interop_fixtures() -> Result<(), Box<dyn std::error::Error>> {
        let ca = generate_ca("grid-ca")?;
        let now = OffsetDateTime::now_utc();
        let dns = format!("{INTEROP_FIXTURE_SITE}.grid.internal");
        let leaf = generate_validity_bounded_dns_cert(
            &ca,
            INTEROP_FIXTURE_SITE,
            &dns,
            now - Duration::days(1),
            now + Duration::days(36_500),
        )?;
        std::fs::create_dir_all(INTEROP_FIXTURE_DIR)?;
        std::fs::write(format!("{INTEROP_FIXTURE_DIR}/ca.pem"), &ca.cert_pem)?;
        std::fs::write(format!("{INTEROP_FIXTURE_DIR}/leaf.pem"), &leaf.cert_pem)?;
        Ok(())
    }

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
    fn generate_dns_cert_preserves_exact_san() {
        let ca = generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let cert =
            generate_dns_cert(&ca, "Grid demo ingress", "api.grid-glb.test").unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            cert.sans,
            ["api.grid-glb.test"],
            "DNS SAN must not gain the Grid internal suffix"
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
    fn site_identity_contains_correct_common_name_and_org() {
        let id = site_identity("cluster-a", "cluster-a.grid.internal");
        assert_eq!(id.common_name, "cluster-a", "CommonName should be the site name");
        assert_eq!(
            id.organization, DEFAULT_ORGANIZATION,
            "OrganizationName should be DEFAULT_ORGANIZATION"
        );
        assert_eq!(
            id.uri_sans,
            vec![spiffe_id("cluster-a")],
            "the SPIFFE URI SAN names the site"
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
    fn custom_site_identity_contains_requested_organization() {
        let id = site_identity_with_org("cluster-a", "cluster-a.grid.internal", "not-ai-grid");
        assert_eq!(
            id.organization, "not-ai-grid",
            "OrganizationName should match the requested organization"
        );
    }

    #[test]
    fn different_sites_get_different_keys() {
        let ca = generate_ca("Test CA").unwrap_or_else(|_| std::process::abort());
        let site_a = generate_site_cert(&ca, "cluster-a").unwrap_or_else(|_| std::process::abort());
        let site_b = generate_site_cert(&ca, "cluster-b").unwrap_or_else(|_| std::process::abort());
        assert_ne!(site_a.key_pem, site_b.key_pem, "sites should have different keys");
        assert_ne!(site_a.cert_pem, site_b.cert_pem, "sites should have different certs");
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
    fn generate_expired_dns_cert_produces_valid_pem() {
        let ca = generate_ca("Test CA").unwrap_or_else(|_| std::process::abort());
        let cert = generate_expired_dns_cert(&ca, "expired-site", "expired.grid.internal")
            .unwrap_or_else(|_| std::process::abort());
        assert!(cert.cert_pem.contains("BEGIN CERTIFICATE"), "should be PEM cert");
        assert!(cert.key_pem.contains("BEGIN PRIVATE KEY"), "should be PEM key");
        assert_eq!(
            cert.sans,
            ["expired.grid.internal"],
            "DNS SAN must match the requested dns_name"
        );
    }

    #[test]
    fn generate_not_yet_valid_dns_cert_produces_valid_pem() {
        let ca = generate_ca("Test CA").unwrap_or_else(|_| std::process::abort());
        let cert = generate_not_yet_valid_dns_cert(&ca, "future-site", "future.grid.internal")
            .unwrap_or_else(|_| std::process::abort());
        assert!(cert.cert_pem.contains("BEGIN CERTIFICATE"), "should be PEM cert");
        assert!(cert.key_pem.contains("BEGIN PRIVATE KEY"), "should be PEM key");
        assert_eq!(
            cert.sans,
            ["future.grid.internal"],
            "DNS SAN must match the requested dns_name"
        );
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

#[cfg(test)]
mod spiffe_id_tests {
    use super::{generate_ca, generate_site_cert_with_names, spiffe_id};

    /// A URI SAN is an `IA5String`, so the name appears verbatim in the DER the
    /// CA signed. Checking the bytes avoids a parser dependency and still
    /// asserts the thing that matters: the name is inside the signature.
    #[test]
    fn a_site_certificate_carries_the_site_name_in_its_signed_bytes() {
        let ca = generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let site = generate_site_cert_with_names(&ca, "pool-a", &["pool-a.grid-system".to_owned()])
            .unwrap_or_else(|_| std::process::abort());
        let der = pem::parse(&site.cert_pem).unwrap_or_else(|_| std::process::abort());
        let bytes = der.contents();

        let id = spiffe_id("pool-a");
        assert!(
            bytes.windows(id.len()).any(|w| w == id.as_bytes()),
            "the name a peer routes on has to be signed, not asserted alongside"
        );
        let other = spiffe_id("pool-b");
        assert!(
            !bytes.windows(other.len()).any(|w| w == other.as_bytes()),
            "and it must name one site"
        );
    }

    #[test]
    fn two_sites_are_told_apart_by_it() {
        assert_ne!(spiffe_id("pool-a"), spiffe_id("pool-b"));
        assert!(spiffe_id("pool-a").starts_with("spiffe://"));
    }
}
