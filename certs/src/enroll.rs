//! Signing certificate requests from enrollees.
//!
//! [`generate_site_cert`](crate::generate_site_cert) mints a key and a
//! certificate together, which suits a site the grid operates itself. An
//! enrollee is different: it holds a private key the grid must never see, so it
//! sends a certificate signing request and the grid signs the public half.
//!
//! The request contributes exactly one thing, the public key. Every name on the
//! issued certificate is rebuilt from the site name the grid assigned, so a
//! request cannot influence the identity it is granted.

use rcgen::{CertificateSigningRequestParams, Issuer};
use sha2::{Digest as _, Sha256};
use time::{Duration, OffsetDateTime};

use crate::generate::{CaCert, GenerateError, build_site_params, spiffe_id};

/// Longest accepted site name, matching the DNS label limit.
const MAX_SITE_NAME_LEN: usize = 63;

/// Backdating applied to `not_before`, so a peer whose clock runs slightly slow
/// does not reject a certificate issued moments ago.
const CLOCK_SKEW_ALLOWANCE: Duration = Duration::minutes(5);

/// How long an issued certificate lasts when a caller does not say.
///
/// Finite, because expiry is the only thing that removes a member: a grid has no
/// revocation list, so a certificate is trusted until it lapses.
///
/// Deliberately not the hours-long lifetime this should eventually have. Nothing
/// renews yet, so a short lifetime would strand every site the first time one
/// lapsed. Shorten this once renewal exists.
pub const DEFAULT_SITE_CERT_LIFETIME: Duration = Duration::days(30);

/// When a certificate is valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Validity {
    /// Not valid before this instant.
    pub not_before: OffsetDateTime,

    /// Not valid after this instant.
    pub not_after: OffsetDateTime,
}

impl Validity {
    /// Valid from a little before now, for `lifetime`.
    ///
    /// The start is backdated by `CLOCK_SKEW_ALLOWANCE`, since a verifier whose
    /// clock is a minute behind would otherwise refuse a certificate that was
    /// just issued to it.
    #[must_use]
    pub fn starting_now(lifetime: Duration) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            not_before: now.saturating_sub(CLOCK_SKEW_ALLOWANCE),
            not_after: now.saturating_add(lifetime),
        }
    }
}

impl Default for Validity {
    fn default() -> Self {
        Self::starting_now(DEFAULT_SITE_CERT_LIFETIME)
    }
}

/// Largest accepted request, before parsing.
///
/// A request holds a public key and a signature. Anything approaching this is
/// not a request this grid issued a name for.
pub const MAX_CSR_PEM_BYTES: usize = 16 * 1024;

/// A certificate issued to an enrollee.
#[derive(Debug, Clone)]
pub struct EnrolledCert {
    /// PEM-encoded certificate. The enrollee already holds the private key.
    pub cert_pem: String,

    /// The identity bound into the certificate, for the record.
    pub spiffe_id: String,

    /// Subject Alternative Names on the certificate.
    pub sans: Vec<String>,

    /// Lowercase hex SHA-256 over the request's `SubjectPublicKeyInfo`.
    ///
    /// Names the key rather than the certificate, so it stays the same across
    /// reissue and changes when an enrollee presents a new key.
    pub public_key_sha256: String,
}

/// Reasons a request is refused.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EnrollError {
    /// The request is larger than [`MAX_CSR_PEM_BYTES`].
    #[error("certificate request exceeds {MAX_CSR_PEM_BYTES} bytes")]
    TooLarge,

    /// The bytes are not a certificate request.
    #[error("certificate request could not be parsed")]
    Malformed,

    /// The request is not signed by the key it carries.
    ///
    /// The signature is what proves the requester holds the private half. A
    /// request failing here asks for a certificate over someone else's key.
    #[error("certificate request signature is invalid")]
    BadSignature,

    /// The request asks for an X.509 extension this grid does not issue.
    #[error("certificate request carries an unsupported extension")]
    UnsupportedExtension,

    /// The assigned site name is not a valid name.
    ///
    /// The name is interpolated into a SPIFFE URI and a DNS name, so anything
    /// outside a DNS label could reshape the identity path.
    #[error("site name is not a lowercase DNS label of at most {MAX_SITE_NAME_LEN} characters")]
    InvalidSiteName,

    /// Signing failed.
    #[error("signing failed: {0}")]
    Signing(String),
}

/// Check that a site name is a lowercase DNS label.
///
/// The SPIFFE ID is built by interpolation, so a name carrying `/` would name a
/// different path than the one the grid approved. Rejecting anything outside a
/// DNS label keeps the assigned name and the issued name the same string.
///
/// # Errors
///
/// Returns [`EnrollError::InvalidSiteName`] when the name is not a lowercase DNS
/// label of at most the allowed length.
pub fn validate_site_name(site_name: &str) -> Result<(), EnrollError> {
    let valid = !site_name.is_empty()
        && site_name.len() <= MAX_SITE_NAME_LEN
        && site_name.starts_with(|ch: char| ch.is_ascii_lowercase() || ch.is_ascii_digit())
        && site_name.ends_with(|ch: char| ch.is_ascii_lowercase() || ch.is_ascii_digit())
        && site_name
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-');

    if valid {
        Ok(())
    } else {
        Err(EnrollError::InvalidSiteName)
    }
}

/// Sign an enrollee's certificate request under a grid-assigned name.
///
/// `site_name` is the name the grid decided on. It is not read from the request,
/// so an enrollee asking to be `site-a` receives whatever the approver assigned
/// instead.
///
/// `validity` is explicit rather than defaulted, because the alternative is
/// rcgen's own default, which runs to the year 4096. A certificate that never
/// expires cannot be taken away from a member.
///
/// # Errors
///
/// Returns [`EnrollError`] if the request is oversized, unparseable, signed by a
/// key it does not carry, or if `site_name` is not a DNS label.
pub fn sign_csr(ca: &CaCert, site_name: &str, csr_pem: &str, validity: Validity) -> Result<EnrolledCert, EnrollError> {
    validate_site_name(site_name)?;

    if csr_pem.len() > MAX_CSR_PEM_BYTES {
        return Err(EnrollError::TooLarge);
    }

    // Parsing also verifies the request's self-signature, which is what proves
    // the requester holds the private half of the key it presents.
    let mut csr = CertificateSigningRequestParams::from_pem(csr_pem).map_err(|err| map_parse_error(&err))?;

    let public_key_sha256 = key_fingerprint(&csr.public_key);

    // Everything the request asked for is dropped here. The public key is the
    // only field carried forward, and the names below are the grid's own.
    let primary = format!("{site_name}.{}", crate::SPIFFE_TRUST_DOMAIN);
    let mut params = build_site_params(site_name, &primary).map_err(|err| signing_failed(&err))?;
    params.not_before = validity.not_before;
    params.not_after = validity.not_after;
    csr.params = params;

    let issuer = Issuer::new(ca.params.clone(), &ca.key_pair);
    let cert = csr
        .signed_by(&issuer)
        .map_err(|err| signing_failed(&GenerateError::Rcgen(err)))?;

    Ok(EnrolledCert {
        cert_pem: cert.pem(),
        spiffe_id: spiffe_id(site_name),
        sans: vec![primary],
        public_key_sha256,
    })
}

/// Verify a certificate signing request and return its key fingerprint.
///
/// Parsing verifies the request self-signature, so a request whose signature
/// does not match its key is refused. No name is involved and no certificate is
/// issued. This is the possession check run at submit, before an operator has
/// pinned a name. Issuance happens later in [`sign_csr`] under the assigned name.
///
/// # Errors
///
/// [`EnrollError::TooLarge`] past [`MAX_CSR_PEM_BYTES`], and
/// [`EnrollError::BadSignature`], [`EnrollError::UnsupportedExtension`], or
/// [`EnrollError::Malformed`] when the request does not parse.
pub fn verify_csr(csr_pem: &str) -> Result<String, EnrollError> {
    if csr_pem.len() > MAX_CSR_PEM_BYTES {
        return Err(EnrollError::TooLarge);
    }
    let csr = CertificateSigningRequestParams::from_pem(csr_pem).map_err(|err| map_parse_error(&err))?;
    Ok(key_fingerprint(&csr.public_key))
}

/// Map an rcgen request-parse failure onto the request-side error it stands for.
///
/// An invalid self-signature is the one that matters: it means the caller does
/// not hold the private half, so the request is refused rather than issued.
fn map_parse_error(err: &rcgen::Error) -> EnrollError {
    if *err == rcgen::Error::InvalidCertificationRequestSignature {
        EnrollError::BadSignature
    } else if *err == rcgen::Error::UnsupportedExtension {
        EnrollError::UnsupportedExtension
    } else {
        EnrollError::Malformed
    }
}

/// Lowercase hex SHA-256 over a request's `SubjectPublicKeyInfo`.
fn key_fingerprint(public_key: &rcgen::PublicKey) -> String {
    use rcgen::PublicKeyData as _;
    hex(&Sha256::digest(public_key.der_bytes()))
}

/// Wrap a generation failure, which is a grid-side fault rather than a bad request.
fn signing_failed(err: &GenerateError) -> EnrollError {
    EnrollError::Signing(err.to_string())
}

/// Lowercase hex encoding.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use rcgen::{CertificateParams, KeyPair, SanType};

    use super::*;
    use crate::generate::generate_ca;

    /// Build a request the way an enrollee would, asking for `requested_names`.
    fn csr_asking_for(requested_names: &[SanType]) -> (String, KeyPair) {
        let key = KeyPair::generate().expect("key");
        let mut params = CertificateParams::default();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "whatever-i-want");
        params.subject_alt_names = requested_names.to_vec();
        let csr = params.serialize_request(&key).expect("csr");
        (csr.pem().expect("pem"), key)
    }

    fn plain_csr() -> (String, KeyPair) {
        csr_asking_for(&[])
    }

    /// Read the URI SANs off an issued certificate.
    fn uri_sans_of(cert_pem: &str) -> Vec<String> {
        use x509_parser::prelude::{FromDer as _, GeneralName, X509Certificate};

        let der = pem::parse(cert_pem).expect("cert pem");
        let (_rest, cert) = X509Certificate::from_der(der.contents()).expect("parse cert");
        cert.subject_alternative_name()
            .ok()
            .flatten()
            .map(|san| {
                san.value
                    .general_names
                    .iter()
                    .filter_map(|name| {
                        if let GeneralName::URI(uri) = name {
                            Some((*uri).to_owned())
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn dns_sans_of(cert_pem: &str) -> Vec<String> {
        use x509_parser::prelude::{FromDer as _, GeneralName, X509Certificate};

        let der = pem::parse(cert_pem).expect("cert pem");
        let (_rest, cert) = X509Certificate::from_der(der.contents()).expect("parse cert");
        cert.subject_alternative_name()
            .ok()
            .flatten()
            .map(|san| {
                san.value
                    .general_names
                    .iter()
                    .filter_map(|name| {
                        if let GeneralName::DNSName(dns) = name {
                            Some((*dns).to_owned())
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn an_issued_certificate_carries_the_assigned_identity() {
        let ca = generate_ca("test-ca").expect("ca");
        let (csr, _key) = plain_csr();

        let issued = sign_csr(&ca, "site-d", &csr, Validity::default()).expect("sign");

        assert_eq!(issued.spiffe_id, "spiffe://grid.internal/site/site-d");
        assert_eq!(
            uri_sans_of(&issued.cert_pem),
            vec!["spiffe://grid.internal/site/site-d"]
        );
    }

    /// The property the whole enrollment flow rests on.
    #[test]
    fn a_request_cannot_choose_its_own_identity() {
        let ca = generate_ca("test-ca").expect("ca");
        let (csr, _key) = csr_asking_for(&[SanType::URI(
            "spiffe://grid.internal/site/site-a".to_owned().try_into().expect("ia5"),
        )]);

        let issued = sign_csr(&ca, "site-d", &csr, Validity::default()).expect("sign");

        assert_eq!(
            uri_sans_of(&issued.cert_pem),
            vec!["spiffe://grid.internal/site/site-d"],
            "the requested identity must not survive signing"
        );
    }

    #[test]
    fn a_request_cannot_add_names_of_its_own() {
        let ca = generate_ca("test-ca").expect("ca");
        let (csr, _key) = csr_asking_for(&[SanType::DnsName(
            "site-a.grid.internal".to_owned().try_into().expect("ia5"),
        )]);

        let issued = sign_csr(&ca, "site-d", &csr, Validity::default()).expect("sign");

        assert_eq!(dns_sans_of(&issued.cert_pem), vec!["site-d.grid.internal"]);
    }

    #[test]
    fn a_request_signed_by_another_key_is_refused() {
        let ca = generate_ca("test-ca").expect("ca");
        let (csr, _key) = plain_csr();

        // Corrupt the signature while leaving the structure intact.
        let der = pem::parse(&csr).expect("pem");
        let mut bytes = der.contents().to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        let tampered = pem::encode(&pem::Pem::new("CERTIFICATE REQUEST", bytes));

        assert!(
            matches!(
                sign_csr(&ca, "site-d", &tampered, Validity::default()),
                Err(EnrollError::BadSignature)
            ),
            "a request whose signature does not match its key must be refused"
        );
    }

    #[test]
    fn bytes_that_are_not_a_request_are_refused() {
        let ca = generate_ca("test-ca").expect("ca");
        assert!(
            matches!(
                sign_csr(&ca, "site-d", "not a pem file", Validity::default()),
                Err(EnrollError::Malformed)
            ),
            "bytes that are not a request must be refused"
        );
    }

    #[test]
    fn an_oversized_request_is_refused_before_parsing() {
        let ca = generate_ca("test-ca").expect("ca");
        let padded = "-".repeat(MAX_CSR_PEM_BYTES + 1);
        assert!(
            matches!(
                sign_csr(&ca, "site-d", &padded, Validity::default()),
                Err(EnrollError::TooLarge)
            ),
            "an oversized request must be refused before parsing"
        );
    }

    /// A name carrying a separator would name a different path than the approver saw.
    #[test]
    fn a_site_name_cannot_reshape_the_identity_path() {
        let ca = generate_ca("test-ca").expect("ca");
        let (csr, _key) = plain_csr();

        for name in ["site-d/../site-a", "site-d/admin", "Site-D", "", "site_d", "-site-d"] {
            assert!(
                matches!(
                    sign_csr(&ca, name, &csr, Validity::default()),
                    Err(EnrollError::InvalidSiteName)
                ),
                "{name} should be refused"
            );
        }
    }

    #[test]
    fn the_key_fingerprint_names_the_key_not_the_certificate() {
        let ca = generate_ca("test-ca").expect("ca");
        let (csr, _key) = plain_csr();

        let first = sign_csr(&ca, "site-d", &csr, Validity::default()).expect("sign");
        let second = sign_csr(&ca, "site-e", &csr, Validity::default()).expect("sign again");

        assert_eq!(
            first.public_key_sha256, second.public_key_sha256,
            "the same key reissued under a new name keeps its fingerprint"
        );
        assert_ne!(first.cert_pem, second.cert_pem);

        let (other_csr, _other) = plain_csr();
        let other = sign_csr(&ca, "site-d", &other_csr, Validity::default()).expect("sign other");
        assert_ne!(first.public_key_sha256, other.public_key_sha256);
    }

    /// An enrolled site and a grid-operated one have to look the same to a verifier.
    #[test]
    fn an_enrolled_certificate_matches_a_locally_minted_one() {
        let ca = generate_ca("test-ca").expect("ca");
        let (csr, _key) = plain_csr();

        let enrolled = sign_csr(&ca, "site-d", &csr, Validity::default()).expect("sign");
        let local = crate::generate_site_cert(&ca, "site-d").expect("local");

        assert_eq!(uri_sans_of(&enrolled.cert_pem), uri_sans_of(&local.cert_pem));
        assert_eq!(dns_sans_of(&enrolled.cert_pem), dns_sans_of(&local.cert_pem));
        assert_eq!(enrolled.sans, local.sans);
    }

    /// A declared lifetime has to reach the certificate, or nothing bounds it.
    #[test]
    fn the_declared_lifetime_is_what_the_certificate_carries() {
        use x509_parser::prelude::{FromDer as _, X509Certificate};

        let ca = generate_ca("test-ca").expect("ca");
        let (csr, _key) = plain_csr();

        let lifetime = Duration::days(7);
        let validity = Validity::starting_now(lifetime);
        let issued = sign_csr(&ca, "site-d", &csr, validity).expect("sign");

        let der = pem::parse(&issued.cert_pem).expect("pem");
        let (_rest, cert) = X509Certificate::from_der(der.contents()).expect("parse");

        let not_before = cert.validity().not_before.timestamp();
        let not_after = cert.validity().not_after.timestamp();
        assert_eq!(
            not_before,
            validity.not_before.unix_timestamp(),
            "the declared start must reach the certificate"
        );
        assert_eq!(
            not_after,
            validity.not_after.unix_timestamp(),
            "the declared end must reach the certificate"
        );

        let carried = not_after.saturating_sub(not_before);
        let expected = (lifetime + CLOCK_SKEW_ALLOWANCE).whole_seconds();
        assert_eq!(carried, expected, "the span is the lifetime plus the skew allowance");
    }

    /// Backdating exists so a verifier a minute behind does not refuse a fresh
    /// certificate.
    #[test]
    fn a_fresh_certificate_is_already_valid() {
        let ca = generate_ca("test-ca").expect("ca");
        let (csr, _key) = plain_csr();
        let issued = sign_csr(&ca, "site-d", &csr, Validity::default()).expect("sign");

        crate::verify_site_cert(&ca.cert_pem, &issued.cert_pem, "site-d")
            .expect("a certificate issued moments ago must verify now");
    }

    /// The point of a finite lifetime: an expired certificate stops working.
    #[test]
    fn an_expired_lifetime_is_refused_by_verification() {
        let ca = generate_ca("test-ca").expect("ca");
        let (csr, _key) = plain_csr();

        let past = OffsetDateTime::now_utc() - Duration::days(2);
        let expired = Validity {
            not_before: past,
            not_after: past + Duration::days(1),
        };
        let issued = sign_csr(&ca, "site-d", &csr, expired).expect("sign");

        assert_eq!(
            crate::verify_site_cert(&ca.cert_pem, &issued.cert_pem, "site-d"),
            Err(crate::VerifyError::NotCurrentlyValid),
            "an expired certificate must stop establishing membership"
        );
    }

    /// The default has to be finite, or expiry can never remove anyone.
    #[test]
    fn the_default_lifetime_is_finite() {
        let validity = Validity::default();
        let span = validity.not_after - validity.not_before;
        assert!(
            span < Duration::days(366),
            "the default lifetime must be finite and short enough to matter, got {span}"
        );
    }

    #[test]
    fn the_issued_certificate_is_signed_by_the_grid_ca() {
        use x509_parser::prelude::{FromDer as _, X509Certificate};

        let ca = generate_ca("test-ca").expect("ca");
        let (csr, _key) = plain_csr();
        let issued = sign_csr(&ca, "site-d", &csr, Validity::default()).expect("sign");

        let ca_der = pem::parse(&ca.cert_pem).expect("ca pem");
        let (_r, ca_cert) = X509Certificate::from_der(ca_der.contents()).expect("ca parse");
        let issued_der = pem::parse(&issued.cert_pem).expect("issued pem");
        let (_r2, issued_cert) = X509Certificate::from_der(issued_der.contents()).expect("issued parse");

        assert_eq!(issued_cert.issuer(), ca_cert.subject());
        issued_cert
            .verify_signature(Some(ca_cert.public_key()))
            .expect("issued cert must verify against the CA");
    }
}
