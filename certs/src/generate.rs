//! Certificate generation.
//!
//! Produces a self-signed CA and per-site certificates for POC/testing.
//! Production deployments use SPIFFE/SPIRE instead. The crypto primitive is
//! selected at compile time by the [`backend`] seam.

use time::{Duration, OffsetDateTime};

use crate::backend::{self, BackendError, CertSpec};

/// Longest site name a certificate subject will carry, the DNS label limit.
pub const MAX_SITE_NAME_LEN: usize = 63;

/// Whether `site_name` is a lowercase DNS label short enough for a subject.
///
/// The site name becomes both the SPIFFE path segment and the subject
/// organization, and the organization is what a receiving gateway authorizes
/// on. Restricting the space keeps the two in step and keeps the organization
/// to one unambiguous RDN value.
#[must_use]
pub fn is_valid_site_name(site_name: &str) -> bool {
    !site_name.is_empty()
        && site_name.len() <= MAX_SITE_NAME_LEN
        && site_name.starts_with(|ch: char| ch.is_ascii_lowercase() || ch.is_ascii_digit())
        && site_name.ends_with(|ch: char| ch.is_ascii_lowercase() || ch.is_ascii_digit())
        && site_name
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors from certificate generation.
#[derive(Debug, thiserror::Error)]
pub enum GenerateError {
    /// Key generation or signing failed in the crypto backend.
    #[error("certificate generation failed: {0}")]
    Backend(String),

    /// Site name is not a lowercase DNS label short enough for a subject.
    ///
    /// The name becomes the SPIFFE path segment and the subject organization, and
    /// a receiving gateway authorizes on the organization, so an unconstrained name
    /// would widen what an authorized value can look like.
    #[error("site name is not a lowercase DNS label of at most {MAX_SITE_NAME_LEN} characters")]
    InvalidSiteName,

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
        BackendError::ParseCsr | BackendError::CsrBadSignature | BackendError::BadSignature => {
            GenerateError::Backend("unexpected backend error".to_owned())
        },
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

/// A CSR and its private key.
pub struct GeneratedCsr {
    /// PEM-encoded PKCS#10 request.
    pub csr_pem: String,
    /// PEM-encoded private key.
    pub key_pem: zeroize::Zeroizing<String>,
}

/// Generate a key and a CSR naming only `common_name`.
///
/// # Errors
///
/// Returns [`GenerateError`] if key generation or signing fails.
pub fn generate_csr(common_name: &str) -> Result<GeneratedCsr, GenerateError> {
    let out = backend::generate_csr(common_name).map_err(map_backend)?;
    Ok(GeneratedCsr {
        csr_pem: out.csr_pem,
        key_pem: zeroize::Zeroizing::new(out.key_pem),
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
/// (e.g., `cluster-a.grid.internal`) and sets X.509
/// `OrganizationName` to the site name, which is what a receiving
/// Praxis `peer_identity_trust` filter authorizes on.
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
    issue_leaf_cert(ca, site_name, extra, true)
}

/// Generate a leaf carrying only DNS SANs and no SPIFFE identity.
///
/// For infrastructure endpoints (a database, a serving listener) that are not
/// peer-mesh members. Omitting the SPIFFE SAN keeps them out of the grid site
/// identity space, so the peer verifier never accepts an infra cert as a site.
///
/// # Errors
///
/// Returns [`GenerateError`] if any name is invalid or signing fails.
pub fn generate_dns_only_cert(
    ca: &CaCert,
    common_name: &str,
    dns_names: &[String],
) -> Result<SiteCertOutput, GenerateError> {
    issue_leaf_cert(ca, common_name, dns_names, false)
}

/// Issue a leaf for `common_name` with `extra` DNS SANs, carrying its SPIFFE site
/// identity only when `spiffe` is set. The primary `<name>.grid.internal` DNS SAN
/// is always present.
fn issue_leaf_cert(
    ca: &CaCert,
    common_name: &str,
    extra: &[String],
    spiffe: bool,
) -> Result<SiteCertOutput, GenerateError> {
    let primary = format!("{common_name}.grid.internal");
    let mut id = if spiffe {
        site_identity(common_name, &primary)?
    } else {
        infra_identity(common_name, &primary)
    };
    id.dns_sans.extend_from_slice(extra);

    let (not_before, not_after) = default_leaf_validity();
    let out = backend::issue_leaf(&ca.material, &id.spec(not_before, not_after)).map_err(map_backend)?;
    Ok(SiteCertOutput {
        cert_pem: out.cert_pem,
        key_pem: out.key_pem,
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
    let id = site_identity(common_name, dns_name)?;
    let (not_before, not_after) = default_leaf_validity();
    let out = backend::issue_leaf(&ca.material, &id.spec(not_before, not_after)).map_err(map_backend)?;
    Ok(SiteCertOutput {
        cert_pem: out.cert_pem,
        key_pem: out.key_pem,
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
pub(crate) fn generate_validity_bounded_dns_cert(
    ca: &CaCert,
    common_name: &str,
    dns_name: &str,
    not_before: OffsetDateTime,
    not_after: OffsetDateTime,
) -> Result<SiteCertOutput, GenerateError> {
    let id = site_identity(common_name, dns_name)?;
    let out = backend::issue_leaf(&ca.material, &id.spec(not_before, not_after)).map_err(map_backend)?;
    Ok(SiteCertOutput {
        cert_pem: out.cert_pem,
        key_pem: out.key_pem,
        sans: vec![dns_name.to_owned()],
    })
}

/// Generate a certificate signed by the given CA with a specific organization.
///
/// Identical to [`generate_site_cert`] except `OrganizationName` is set to
/// `org` rather than the site name. Use this to create test certs
/// that will fail `peer_identity_trust` org matching despite being signed by
/// the same trusted CA (TLS handshake succeeds; filter rejects).
///
/// # Errors
///
/// Returns [`GenerateError`] if key generation or signing fails.
pub fn generate_cert_with_org(ca: &CaCert, site_name: &str, org: &str) -> Result<SiteCertOutput, GenerateError> {
    // The site name still becomes the SPIFFE path segment, so it is validated here even
    // though the organization is deliberately left free for a negative fixture.
    if !is_valid_site_name(site_name) {
        return Err(GenerateError::InvalidSiteName);
    }
    let dns_san = format!("{site_name}.grid.internal");
    let id = site_identity_with_org(site_name, &dns_san, org);
    let (not_before, not_after) = default_leaf_validity();
    let out = backend::issue_leaf(&ca.material, &id.spec(not_before, not_after)).map_err(map_backend)?;
    Ok(SiteCertOutput {
        cert_pem: out.cert_pem,
        key_pem: out.key_pem,
        sans: vec![dns_san],
    })
}

/// Sign `message` with the CA key, for a record only the CA's holder may vouch for.
///
/// # Errors
///
/// Returns [`GenerateError`] if signing fails.
pub fn sign_with_ca(ca: &CaCert, message: &[u8]) -> Result<Vec<u8>, GenerateError> {
    backend::sign_message(&ca.material, message).map_err(map_backend)
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
    /// Subject organization, naming the site a peer authorizes on. `None` for an
    /// infrastructure leaf, which names no site.
    pub(crate) organization: Option<String>,
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
            organization: self.organization.as_deref(),
            dns_sans: &self.dns_sans,
            uri_sans: &self.uri_sans,
            is_ca: false,
            not_before,
            not_after,
        }
    }
}

/// Build the identity for a site certificate, naming the site in its subject.
pub(crate) fn site_identity(site_name: &str, dns_san: &str) -> Result<SiteIdentity, GenerateError> {
    if !is_valid_site_name(site_name) {
        return Err(GenerateError::InvalidSiteName);
    }
    Ok(site_identity_with_org(site_name, dns_san, site_name))
}

/// Build the identity for an infrastructure certificate: a name and where to dial it.
///
/// No SPIFFE SAN and no organization, so a peer verifier never accepts it as a site and
/// a receiving gateway never authorizes it as one. The name is therefore free text, a
/// database or a CA subject rather than a site, and is not validated as a site name.
pub(crate) fn infra_identity(common_name: &str, dns_san: &str) -> SiteIdentity {
    SiteIdentity {
        common_name: common_name.to_owned(),
        organization: None,
        dns_sans: vec![dns_san.to_owned()],
        uri_sans: Vec::new(),
    }
}

/// Build the identity for a site certificate with a specific organization.
pub(crate) fn site_identity_with_org(site_name: &str, dns_san: &str, organization: &str) -> SiteIdentity {
    SiteIdentity {
        common_name: site_name.to_owned(),
        organization: Some(organization.to_owned()),
        dns_sans: vec![dns_san.to_owned()],
        uri_sans: vec![spiffe_id(site_name)],
    }
}

/// Validity window for a generated leaf certificate.
///
/// Backdated an hour so a node whose clock runs behind does not refuse, or
/// bootstrap re-issue, a certificate issued moments ago.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "now +/- a fixed span cannot overflow OffsetDateTime"
)]
fn default_leaf_validity() -> (OffsetDateTime, OffsetDateTime) {
    let now = OffsetDateTime::now_utc();
    (now - Duration::hours(1), now + Duration::days(365))
}

/// Validity window for a generated CA certificate.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "now +/- a fixed span cannot overflow OffsetDateTime"
)]
fn default_ca_validity() -> (OffsetDateTime, OffsetDateTime) {
    let now = OffsetDateTime::now_utc();
    (now - Duration::hours(1), now + Duration::days(3650))
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
    use x509_parser::prelude::{FromDer as _, X509Certificate};

    use super::*;

    /// Full subject of an issued certificate, as a comparable string.
    fn subject_of(cert_pem: &str) -> String {
        let der = pem::parse(cert_pem).unwrap_or_else(|_| std::process::abort());
        let (_rest, parsed) = X509Certificate::from_der(der.contents()).unwrap_or_else(|_| std::process::abort());
        parsed.subject.to_string()
    }

    /// Subject organization values on an issued certificate, in order.
    fn subject_organizations(cert_pem: &str) -> Vec<String> {
        let der = pem::parse(cert_pem).unwrap_or_else(|_| std::process::abort());
        let (_rest, parsed) = X509Certificate::from_der(der.contents()).unwrap_or_else(|_| std::process::abort());
        parsed
            .subject
            .iter_organization()
            .filter_map(|org| org.as_str().ok())
            .map(str::to_owned)
            .collect()
    }


    /// Site the interop fixtures are minted for.
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
            generate_dns_cert(&ca, "grid-demo-ingress", "api.grid-glb.test").unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            cert.sans,
            ["api.grid-glb.test"],
            "DNS SAN must not gain the Grid internal suffix"
        );
    }

    #[test]
    fn the_organization_is_the_spiffe_path_segment() {
        // A receiving gateway authorizes on the organization while the handshake proves the
        // SPIFFE ID. They have to name the same site, or policy and identity disagree.
        for name in ["cluster-a", "s", "a-b-c", &"x".repeat(MAX_SITE_NAME_LEN)] {
            let id = site_identity(name, "ignored.example").unwrap_or_else(|_| std::process::abort());
            let segment = id
                .uri_sans
                .first()
                .and_then(|uri| crate::verify::site_of_spiffe_id(uri))
                .unwrap_or_else(|| std::process::abort());
            assert_eq!(
                id.organization.as_deref(),
                Some(segment),
                "{name}: organization must be the SPIFFE path segment"
            );
        }
    }

    /// The issued subject carries exactly one organization, and it names the site.
    ///
    /// Pingora concatenates several organization RDNs with no separator, so two of them
    /// read as one value. One RDN per leaf is what keeps that unreachable. Read from the
    /// parsed certificate rather than from the output struct, which would only echo the
    /// argument this test passed in.
    #[test]
    fn an_issued_site_leaf_names_the_site_in_exactly_one_organization() {
        let ca = generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let site = generate_site_cert(&ca, "cluster-a").unwrap_or_else(|_| std::process::abort());
        let orgs = subject_organizations(&site.cert_pem);
        assert_eq!(orgs.len(), 1, "a site leaf must carry exactly one organization RDN");
        assert_eq!(
            orgs.first().map(String::as_str),
            Some("cluster-a"),
            "the one organization RDN must name the site"
        );
    }

    /// An infrastructure leaf named for its service does not share the CA's subject.
    ///
    /// It carries no organization, so a leaf sharing the CA's common name has the same
    /// subject as the certificate that signed it. The signature still verifies, which is
    /// why a signature check does not catch this, but a verifier building a path reads an
    /// identical subject and issuer as self-signed and refuses the chain.
    #[test]
    fn an_infra_leaf_named_for_its_service_does_not_share_the_ca_subject() {
        let ca = generate_ca("grid-ca").unwrap_or_else(|_| std::process::abort());
        let sans = ["enroll.grid.svc".to_owned()];

        let collides = generate_dns_only_cert(&ca, "grid-ca", &sans).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            subject_of(&collides.cert_pem),
            subject_of(&ca.cert_pem),
            "a leaf named for its own CA shares its subject, which is the hazard"
        );

        let named = generate_dns_only_cert(&ca, "grid-enrollment", &sans).unwrap_or_else(|_| std::process::abort());
        assert_ne!(
            subject_of(&named.cert_pem),
            subject_of(&ca.cert_pem),
            "naming infrastructure for its service keeps the subject distinct"
        );
    }

    /// An infrastructure leaf names no site, on either axis.
    #[test]
    fn an_infra_leaf_carries_no_organization_and_no_spiffe_id() {
        // Its name is a database or a CA subject, so it is not a site name and is not
        // validated as one. Carrying a site-shaped organization would let it be authorized.
        let ca = generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let infra = generate_dns_only_cert(&ca, "Acme Grid Root CA", &["db.ns.svc".to_owned()])
            .unwrap_or_else(|_| std::process::abort());
        assert!(
            subject_organizations(&infra.cert_pem).is_empty(),
            "an infra leaf must carry no organization"
        );
        assert!(
            !infra.cert_pem.is_empty(),
            "a name that is not a DNS label must still issue"
        );
    }

    #[test]
    fn a_name_that_is_not_a_dns_label_mints_no_identity() {
        // The organization is an authorization value, so the space it can take is bounded.
        for bad in [
            "",
            "Cluster-A",
            "has space",
            "-leading",
            "trailing-",
            "under_score",
            "site-d/admin",
            "site-d/../site-a",
            &"x".repeat(MAX_SITE_NAME_LEN + 1),
        ] {
            assert!(
                site_identity(bad, "ignored.example").is_err(),
                "{bad:?} must not mint a site identity"
            );
        }
    }

    #[test]
    fn site_identity_contains_correct_common_name_and_org() {
        let id = site_identity("cluster-a", "cluster-a.grid.internal").unwrap_or_else(|_| std::process::abort());
        assert_eq!(id.common_name, "cluster-a", "CommonName should be the site name");
        assert_eq!(
            id.organization.as_deref(),
            Some("cluster-a"),
            "OrganizationName should be the site name, which is what a peer authorizes on"
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
            subject_organizations(&site.cert_pem).first().map(String::as_str),
            Some("not-ai-grid"),
            "the issued subject should carry the requested organization"
        );
    }

    #[test]
    fn custom_site_identity_contains_requested_organization() {
        let id = site_identity_with_org("cluster-a", "cluster-a.grid.internal", "not-ai-grid");
        assert_eq!(
            id.organization.as_deref(),
            Some("not-ai-grid"),
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
    use super::{generate_ca, generate_dns_only_cert, generate_site_cert_with_names, spiffe_id};

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

    /// An infra cert keeps its DNS SAN for hostname verification but carries no
    /// SPIFFE name, so the peer verifier never accepts it as a grid site.
    #[test]
    fn a_dns_only_certificate_carries_no_spiffe_name() {
        let ca = generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let cert = generate_dns_only_cert(&ca, "grid-enrollment-db", &["grid-enrollment-db.ns.svc".to_owned()])
            .unwrap_or_else(|_| std::process::abort());
        let der = pem::parse(&cert.cert_pem).unwrap_or_else(|_| std::process::abort());
        let bytes = der.contents();

        let id = spiffe_id("grid-enrollment-db");
        assert!(
            !bytes.windows(id.len()).any(|window| window == id.as_bytes()),
            "an infra cert must not carry a grid-site SPIFFE identity"
        );
        let dns = b"grid-enrollment-db.ns.svc";
        assert!(
            bytes.windows(dns.len()).any(|window| window == dns),
            "the DNS SAN the caller asked for has to be present for hostname verification"
        );
    }
}
