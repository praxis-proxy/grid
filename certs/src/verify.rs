//! Verifying that a certificate belongs to the site claiming it.
//!
//! A peer's certificate arrives over gossip, where the only thing the transport
//! proves is that the sender holds the grid's shared key. That is group
//! membership, not identity. This turns the certificate into an identity claim
//! the receiver can check for itself: it chains to the grid CA, and the name
//! bound into it is the name the sender says it has.
//!
//! The grid CA issues site certificates directly, so a chain is one link long.
//! An intermediate would need a path-building verifier instead.

use x509_parser::prelude::{FromDer as _, GeneralName, X509Certificate};

use crate::generate::spiffe_id;

/// Largest certificate this will look at, before parsing.
pub const MAX_CERT_PEM_BYTES: usize = 16 * 1024;

/// Reasons a certificate does not establish the identity claimed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VerifyError {
    /// The certificate is larger than [`MAX_CERT_PEM_BYTES`].
    #[error("certificate exceeds {MAX_CERT_PEM_BYTES} bytes")]
    TooLarge,

    /// The certificate could not be parsed.
    #[error("certificate could not be parsed")]
    Malformed,

    /// The grid CA certificate could not be parsed.
    #[error("grid CA certificate could not be parsed")]
    MalformedCa,

    /// The certificate was issued by something other than this grid's CA.
    #[error("certificate was not issued by this grid's CA")]
    WrongIssuer,

    /// The signature does not verify against the grid CA.
    #[error("certificate signature does not verify against this grid's CA")]
    BadSignature,

    /// The certificate is expired or not yet valid.
    #[error("certificate is outside its validity period")]
    NotCurrentlyValid,

    /// The certificate does not carry exactly one SPIFFE URI SAN.
    ///
    /// One name, or the identity is ambiguous and a verifier would have to
    /// choose which to believe.
    #[error("certificate does not carry exactly one SPIFFE URI name")]
    NotOneSpiffeName,

    /// The certificate names a different site than the one claiming it.
    #[error("certificate names {found}, but {claimed} is claiming it")]
    NameMismatch {
        /// The name bound into the certificate.
        found: String,
        /// The name the sender claimed.
        claimed: String,
    },
}

/// Check that `leaf_pem` was issued by this grid to `claimed_site`.
///
/// On success, returns the leaf's public key point, which is what a caller needs
/// to check a signature the site made.
///
/// # Errors
///
/// Returns [`VerifyError`] if the certificate is unparseable, was not issued by
/// this grid's CA, is outside its validity period, or names a different site.
pub fn verify_site_cert(ca_cert_pem: &str, leaf_pem: &str, claimed_site: &str) -> Result<Vec<u8>, VerifyError> {
    if leaf_pem.len() > MAX_CERT_PEM_BYTES {
        return Err(VerifyError::TooLarge);
    }

    let ca_der = pem::parse(ca_cert_pem).map_err(|_bad| VerifyError::MalformedCa)?;
    let (_after_ca, ca) = X509Certificate::from_der(ca_der.contents()).map_err(|_bad| VerifyError::MalformedCa)?;

    let leaf_der = pem::parse(leaf_pem).map_err(|_bad| VerifyError::Malformed)?;
    let (_after_leaf, leaf) = X509Certificate::from_der(leaf_der.contents()).map_err(|_bad| VerifyError::Malformed)?;

    if leaf.issuer() != ca.subject() {
        return Err(VerifyError::WrongIssuer);
    }
    leaf.verify_signature(Some(ca.public_key()))
        .map_err(|_bad| VerifyError::BadSignature)?;
    if !leaf.validity().is_valid() {
        return Err(VerifyError::NotCurrentlyValid);
    }

    // The name has to be bound by the signature, not asserted next to it.
    let expected = spiffe_id(claimed_site);
    let found = single_spiffe_name(&leaf).ok_or(VerifyError::NotOneSpiffeName)?;
    if found != expected {
        return Err(VerifyError::NameMismatch {
            found,
            claimed: expected,
        });
    }

    Ok(leaf.public_key().subject_public_key.data.to_vec())
}

/// The canonical fingerprint of a certificate: lowercase hex SHA-256 over its
/// DER form.
///
/// Taken over DER rather than PEM, so line wrapping and trailing newlines cannot
/// change it. This is the form a peer pins.
///
/// # Errors
///
/// Returns [`VerifyError`] if the certificate is oversized or unparseable.
pub fn canonical_fingerprint(cert_pem: &str) -> Result<String, VerifyError> {
    use sha2::{Digest as _, Sha256};

    if cert_pem.len() > MAX_CERT_PEM_BYTES {
        return Err(VerifyError::TooLarge);
    }
    let der = pem::parse(cert_pem).map_err(|_bad| VerifyError::Malformed)?;

    Ok(Sha256::digest(der.contents())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// The public key a certificate request carries, as a key point.
///
/// An enrollee proves it is the one that made a request by signing with the key
/// half it kept. This returns the half the request published, so a caller can
/// check that signature.
///
/// # Errors
///
/// Returns [`VerifyError`] if the request is oversized or unparseable.
pub fn csr_public_key(csr_pem: &str) -> Result<Vec<u8>, VerifyError> {
    use x509_parser::certification_request::X509CertificationRequest;

    if csr_pem.len() > MAX_CERT_PEM_BYTES {
        return Err(VerifyError::TooLarge);
    }

    let der = pem::parse(csr_pem).map_err(|_bad| VerifyError::Malformed)?;
    let (_rest, csr) = X509CertificationRequest::from_der(der.contents()).map_err(|_bad| VerifyError::Malformed)?;

    Ok(csr
        .certification_request_info
        .subject_pki
        .subject_public_key
        .data
        .to_vec())
}

/// The one SPIFFE URI name on a certificate, when there is exactly one.
fn single_spiffe_name(leaf: &X509Certificate<'_>) -> Option<String> {
    let san = leaf.subject_alternative_name().ok().flatten()?;
    let mut uris = san.value.general_names.iter().filter_map(|name| {
        if let GeneralName::URI(uri) = name {
            uri.starts_with("spiffe://").then(|| (*uri).to_owned())
        } else {
            None
        }
    });

    match (uris.next(), uris.next()) {
        (Some(only), None) => Some(only),
        _ambiguous_or_absent => None,
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;
    use crate::{
        enroll::sign_csr,
        generate::{generate_ca, generate_expired_dns_cert, generate_site_cert},
    };

    fn csr_for(_site: &str) -> String {
        let key = rcgen::KeyPair::generate().expect("key");
        let params = rcgen::CertificateParams::default();
        params.serialize_request(&key).expect("csr").pem().expect("pem")
    }

    #[test]
    fn a_certificate_this_grid_issued_verifies() {
        let ca = generate_ca("grid-ca").expect("ca");
        let issued = sign_csr(&ca, "site-d", &csr_for("site-d"), crate::Validity::default()).expect("sign");

        let spki = verify_site_cert(&ca.cert_pem, &issued.cert_pem, "site-d").expect("should verify");
        assert!(!spki.is_empty(), "the public key should come back for signature checks");
    }

    /// The claim being checked: a certificate cannot vouch for a name it does not carry.
    #[test]
    fn a_certificate_cannot_vouch_for_another_site() {
        let ca = generate_ca("grid-ca").expect("ca");
        let issued = sign_csr(&ca, "site-d", &csr_for("site-d"), crate::Validity::default()).expect("sign");

        let result = verify_site_cert(&ca.cert_pem, &issued.cert_pem, "site-a");
        assert_eq!(
            result,
            Err(VerifyError::NameMismatch {
                found: "spiffe://grid.internal/site/site-d".to_owned(),
                claimed: "spiffe://grid.internal/site/site-a".to_owned(),
            }),
            "site-d's certificate must not establish site-a"
        );
    }

    /// A certificate from a CA this grid does not know establishes nothing.
    #[test]
    fn a_certificate_from_another_ca_is_refused() {
        let ours = generate_ca("grid-ca").expect("ca");
        let theirs = generate_ca("someone-elses-ca").expect("other ca");
        let issued = sign_csr(&theirs, "site-d", &csr_for("site-d"), crate::Validity::default()).expect("sign");

        assert_eq!(
            verify_site_cert(&ours.cert_pem, &issued.cert_pem, "site-d"),
            Err(VerifyError::WrongIssuer),
            "another CA's certificate must not establish membership"
        );
    }

    /// Same subject name, different key: the signature is what decides.
    #[test]
    fn a_forged_issuer_name_does_not_pass() {
        let ours = generate_ca("grid-ca").expect("ca");
        let impostor = generate_ca("grid-ca").expect("impostor with the same name");
        let issued = sign_csr(&impostor, "site-d", &csr_for("site-d"), crate::Validity::default()).expect("sign");

        assert_eq!(
            verify_site_cert(&ours.cert_pem, &issued.cert_pem, "site-d"),
            Err(VerifyError::BadSignature),
            "matching the CA's name must not be enough"
        );
    }

    #[test]
    fn an_expired_certificate_is_refused() {
        let ca = generate_ca("grid-ca").expect("ca");
        let expired = generate_expired_dns_cert(&ca, "site-d", "site-d.grid.internal").expect("expired");

        let result = verify_site_cert(&ca.cert_pem, &expired.cert_pem, "site-d");
        assert!(
            matches!(
                result,
                Err(VerifyError::NotCurrentlyValid | VerifyError::NotOneSpiffeName)
            ),
            "an expired certificate must not verify, got {result:?}"
        );
    }

    #[test]
    fn a_grid_minted_certificate_verifies_the_same_way() {
        let ca = generate_ca("grid-ca").expect("ca");
        let local = generate_site_cert(&ca, "site-a").expect("local");

        verify_site_cert(&ca.cert_pem, &local.cert_pem, "site-a").expect("a grid-minted cert should verify");
    }

    /// The pin a peer configures has to be the one this computes.
    #[test]
    fn a_fingerprint_is_sixty_four_lowercase_hex_characters() {
        let ca = generate_ca("grid-ca").expect("ca");
        let issued = sign_csr(&ca, "site-d", &csr_for("site-d"), crate::Validity::default()).expect("sign");

        let fingerprint = canonical_fingerprint(&issued.cert_pem).expect("fingerprint");
        assert_eq!(fingerprint.len(), 64, "a SHA-256 in hex is 64 characters");
        assert!(
            fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
            "the operator accepts only lowercase hex, got {fingerprint}"
        );
    }

    /// Whitespace around the PEM must not change the pin.
    #[test]
    fn a_fingerprint_is_taken_over_der_not_the_pem_text() {
        let ca = generate_ca("grid-ca").expect("ca");
        let issued = sign_csr(&ca, "site-d", &csr_for("site-d"), crate::Validity::default()).expect("sign");

        let plain = canonical_fingerprint(&issued.cert_pem).expect("fingerprint");
        let padded = canonical_fingerprint(&format!("\n{}\n\n", issued.cert_pem.trim())).expect("fingerprint");
        assert_eq!(plain, padded, "trailing newlines must not change the pin");
    }

    #[test]
    fn two_certificates_have_different_fingerprints() {
        let ca = generate_ca("grid-ca").expect("ca");
        let first = sign_csr(&ca, "site-d", &csr_for("site-d"), crate::Validity::default()).expect("sign");
        let second = sign_csr(&ca, "site-e", &csr_for("site-e"), crate::Validity::default()).expect("sign");

        assert_ne!(
            canonical_fingerprint(&first.cert_pem).expect("first"),
            canonical_fingerprint(&second.cert_pem).expect("second"),
            "different certificates must pin differently"
        );
    }

    #[test]
    fn rubbish_is_refused() {
        let ca = generate_ca("grid-ca").expect("ca");
        assert_eq!(
            verify_site_cert(&ca.cert_pem, "not a certificate", "site-d"),
            Err(VerifyError::Malformed),
            "bytes that are not a certificate must be refused"
        );
        assert_eq!(
            verify_site_cert(&ca.cert_pem, &"x".repeat(MAX_CERT_PEM_BYTES + 1), "site-d"),
            Err(VerifyError::TooLarge),
            "an oversized certificate must be refused before parsing"
        );
    }
}
