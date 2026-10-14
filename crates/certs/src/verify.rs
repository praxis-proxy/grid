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

use crate::generate::{SPIFFE_TRUST_DOMAIN, spiffe_id};

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

    /// The CA is not a pinned anchor.
    #[error("CA certificate is not one of the pinned grid CAs")]
    NotAnchored,

    /// The certificate is not a self-signed CA.
    #[error("certificate is not a self-signed CA")]
    NotSelfSignedCa,

    /// The certificate names a different site than the one claiming it.
    #[error("certificate names {found}, but {claimed} is claiming it")]
    NameMismatch {
        /// The name bound into the certificate.
        found: String,
        /// The name the sender claimed.
        claimed: String,
    },

    /// The SPIFFE name is not in this grid's trust domain.
    #[error("certificate names {found}, not the {expected} trust domain")]
    WrongTrustDomain {
        /// The SPIFFE name bound into the certificate.
        found: String,
        /// The trust domain this grid expects.
        expected: String,
    },

    /// The peer presented no certificate to extract an identity from.
    #[cfg(feature = "verifier")]
    #[error("peer presented no certificate")]
    NoPeerCertificate,
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
    with_issued_leaf(ca_cert_pem, leaf_pem, |leaf| {
        // The name has to be bound by the signature, not asserted next to it.
        let expected = spiffe_id(claimed_site);
        let found = single_spiffe_name(leaf).ok_or(VerifyError::NotOneSpiffeName)?;
        if found != expected {
            return Err(VerifyError::NameMismatch {
                found,
                claimed: expected,
            });
        }
        Ok(leaf.public_key().subject_public_key.data.to_vec())
    })
}

/// Check that `leaf_pem` was issued by the CA in `ca_cert_pem` and is currently
/// valid, without any identity check.
///
/// # Errors
///
/// Returns [`VerifyError`] if either certificate is unparseable, the leaf was
/// not signed by this CA, or it is outside its validity period.
pub fn verify_issued_by(ca_cert_pem: &str, leaf_pem: &str) -> Result<(), VerifyError> {
    with_issued_leaf(ca_cert_pem, leaf_pem, |_leaf| Ok(()))
}

/// Run `then` on the leaf once it is proven issued by the CA and currently valid.
fn with_issued_leaf<T>(
    ca_cert_pem: &str,
    leaf_pem: &str,
    then: impl FnOnce(&X509Certificate<'_>) -> Result<T, VerifyError>,
) -> Result<T, VerifyError> {
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
    crate::backend::verify_leaf_signature(ca_cert_pem, leaf_pem).map_err(|_bad| VerifyError::BadSignature)?;
    if !leaf.validity().is_valid() {
        return Err(VerifyError::NotCurrentlyValid);
    }
    then(&leaf)
}

/// A certificate's issuer name and `notAfter`, for logging. Carries no key
/// material.
///
/// # Errors
///
/// Returns [`VerifyError`] if the certificate is oversized or unparseable.
pub fn cert_issuer_and_expiry(cert_pem: &str) -> Result<(String, String), VerifyError> {
    with_cert(cert_pem, |cert| {
        (cert.issuer().to_string(), cert.validity().not_after.to_string())
    })
}

/// Whether a certificate has expired or expires within `window`.
///
/// # Errors
///
/// Returns [`VerifyError`] if the certificate is oversized or unparseable.
pub fn cert_expires_within(cert_pem: &str, window: time::Duration) -> Result<bool, VerifyError> {
    with_cert(cert_pem, |cert| {
        cert.validity().time_to_expiration().is_none_or(|left| left < window)
    })
}

/// Check a [`crate::sign_with_ca`] signature over `message` against the CA certificate.
///
/// # Errors
///
/// Returns [`VerifyError`] if the CA is unparseable or the signature does not verify.
pub fn verify_ca_signature(ca_cert_pem: &str, message: &[u8], signature: &[u8]) -> Result<(), VerifyError> {
    if ca_cert_pem.len() > MAX_CERT_PEM_BYTES {
        return Err(VerifyError::TooLarge);
    }
    crate::backend::verify_message(ca_cert_pem, message, signature).map_err(|err| {
        if err == crate::backend::BackendError::InvalidCaCert {
            VerifyError::MalformedCa
        } else {
            VerifyError::BadSignature
        }
    })
}

/// A certificate's `notBefore` and `notAfter`.
///
/// # Errors
///
/// Returns [`VerifyError`] if the certificate is oversized or unparseable.
pub fn cert_validity(cert_pem: &str) -> Result<(time::OffsetDateTime, time::OffsetDateTime), VerifyError> {
    with_cert(cert_pem, |cert| {
        (
            cert.validity().not_before.to_datetime(),
            cert.validity().not_after.to_datetime(),
        )
    })
}

/// Lowercase hex SHA-256 over a certificate's `SubjectPublicKeyInfo`, the key digest
/// enrollment records and [`crate::verify_csr`] returns.
///
/// # Errors
///
/// Returns [`VerifyError`] if the certificate is oversized or unparseable.
pub fn cert_public_key_sha256(cert_pem: &str) -> Result<String, VerifyError> {
    Ok(crate::backend::sha256(&cert_public_key(cert_pem)?)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// One DER certificate as PEM.
#[must_use]
pub fn cert_pem_from_der(der: &[u8]) -> String {
    encode_cert(der.to_vec())
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
    if cert_pem.len() > MAX_CERT_PEM_BYTES {
        return Err(VerifyError::TooLarge);
    }
    let der = pem::parse(cert_pem).map_err(|_bad| VerifyError::Malformed)?;

    Ok(crate::backend::sha256(der.contents())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// The canonical fingerprint of every certificate in a bundle, which may hold the
/// current CA and the one it replaced.
///
/// # Errors
///
/// Returns [`VerifyError::TooLarge`] past [`MAX_CERT_PEM_BYTES`], and
/// [`VerifyError::Malformed`] if the bundle does not parse or holds no certificate.
pub fn bundle_fingerprints(bundle_pem: &str) -> Result<std::collections::BTreeSet<String>, VerifyError> {
    // The same bound as bundle_within, so a bundle is never planned on and then refused.
    if bundle_pem.len() > MAX_CERT_PEM_BYTES {
        return Err(VerifyError::TooLarge);
    }
    let fingerprints: std::collections::BTreeSet<String> = cert_ders(bundle_pem)?
        .iter()
        .map(|der| {
            crate::backend::sha256(der)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect()
        })
        .collect();
    if fingerprints.is_empty() {
        return Err(VerifyError::Malformed);
    }
    Ok(fingerprints)
}

/// The DNS SANs on a certificate, as encoded. Case is not normalized, so the
/// caller owns any case-insensitive comparison.
///
/// # Errors
///
/// Returns [`VerifyError`] if the certificate is oversized or unparseable.
pub fn cert_dns_sans(cert_pem: &str) -> Result<Vec<String>, VerifyError> {
    with_cert(cert_pem, |cert| {
        let Some(san) = cert.subject_alternative_name().ok().flatten() else {
            return Vec::new();
        };
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
}

/// The public key a certificate request carries, as `SubjectPublicKeyInfo` DER.
///
/// An enrollee proves it is the one that made a request by signing with the key
/// half it kept. This returns the half the request published, in the same
/// `SubjectPublicKeyInfo` form the backends fingerprint, so a caller can check
/// that signature or match the key against an issued certificate.
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

    Ok(csr.certification_request_info.subject_pki.raw.to_vec())
}

/// The certificate's `SubjectPublicKeyInfo` DER.
///
/// # Errors
///
/// Returns [`VerifyError`] if the certificate is oversized or unparseable.
pub fn cert_public_key(cert_pem: &str) -> Result<Vec<u8>, VerifyError> {
    with_cert(cert_pem, |cert| cert.public_key().raw.to_vec())
}

/// Whether a leaf has the X.509-SVID constraints and key usage.
///
/// # Errors
///
/// Returns [`VerifyError`] if the certificate is oversized or unparseable.
pub fn has_svid_profile(cert_pem: &str) -> Result<bool, VerifyError> {
    with_cert(cert_pem, |cert| {
        let not_ca = cert
            .basic_constraints()
            .ok()
            .flatten()
            .is_some_and(|bc| bc.critical && !bc.value.ca);
        let signs_only = cert.key_usage().ok().flatten().is_some_and(|ku| {
            ku.critical && ku.value.digital_signature() && !ku.value.key_cert_sign() && !ku.value.crl_sign()
        });
        not_ca && signs_only
    })
}

/// Apply `read` to the size-checked, parsed `cert_pem`.
fn with_cert<T>(cert_pem: &str, read: impl FnOnce(&X509Certificate<'_>) -> T) -> Result<T, VerifyError> {
    if cert_pem.len() > MAX_CERT_PEM_BYTES {
        return Err(VerifyError::TooLarge);
    }
    let der = pem::parse(cert_pem).map_err(|_bad| VerifyError::Malformed)?;
    let (_rest, cert) = X509Certificate::from_der(der.contents()).map_err(|_bad| VerifyError::Malformed)?;
    Ok(read(&cert))
}

/// The single CA in `ca_pem` if it is a pinned, self-signed CA.
///
/// # Errors
///
/// Returns [`VerifyError`] if it is unpinned, invalid, or unparseable.
pub fn anchored_ca(ca_pem: &str, anchors_pem: &str) -> Result<String, VerifyError> {
    if ca_pem.len() > MAX_CERT_PEM_BYTES {
        return Err(VerifyError::TooLarge);
    }
    let [ca_der] = cert_ders(ca_pem)
        .map_err(|_bad| VerifyError::MalformedCa)?
        .try_into()
        .map_err(|_many: Vec<_>| VerifyError::MalformedCa)?;
    if !cert_ders(anchors_pem)?.contains(&ca_der) {
        return Err(VerifyError::NotAnchored);
    }
    let (_rest, ca) = X509Certificate::from_der(&ca_der).map_err(|_bad| VerifyError::MalformedCa)?;
    if !ca.is_ca() || ca.issuer() != ca.subject() {
        return Err(VerifyError::NotSelfSignedCa);
    }
    if !ca.validity().is_valid() {
        return Err(VerifyError::NotCurrentlyValid);
    }
    let pem = encode_cert(ca_der);
    crate::backend::verify_leaf_signature(&pem, &pem).map_err(|_bad| VerifyError::NotSelfSignedCa)?;
    Ok(pem)
}

/// The certificate in the first PEM block of `cert_pem`, re-encoded alone.
///
/// # Errors
///
/// Returns [`VerifyError`] if the certificate is oversized or unparseable.
pub fn leaf_only(cert_pem: &str) -> Result<String, VerifyError> {
    if cert_pem.len() > MAX_CERT_PEM_BYTES {
        return Err(VerifyError::TooLarge);
    }
    let block = pem::parse(cert_pem).map_err(|_bad| VerifyError::Malformed)?;
    let der = block.contents();
    let (rest, _cert) = X509Certificate::from_der(der).map_err(|_bad| VerifyError::Malformed)?;
    let used = der.len().saturating_sub(rest.len());
    Ok(encode_cert(der.get(..used).unwrap_or_default().to_vec()))
}

/// `der` as one LF-ended `CERTIFICATE` block.
fn encode_cert(der: Vec<u8>) -> String {
    pem::encode_config(
        &pem::Pem::new("CERTIFICATE", der),
        pem::EncodeConfig::new().set_line_ending(pem::LineEnding::LF),
    )
}

/// Whether `bundle_pem` is non-empty and all in `anchors_pem`.
///
/// # Errors
///
/// Errors if either is oversized or holds a malformed PEM block.
pub fn bundle_within(bundle_pem: &str, anchors_pem: &str) -> Result<bool, VerifyError> {
    if bundle_pem.len() > MAX_CERT_PEM_BYTES || anchors_pem.len() > MAX_CERT_PEM_BYTES {
        return Err(VerifyError::TooLarge);
    }
    let anchors = cert_ders(anchors_pem)?;
    let bundle = cert_ders(bundle_pem)?;
    Ok(!bundle.is_empty() && bundle.iter().all(|der| anchors.contains(der)))
}

/// The DER of each certificate in `bundle_pem`.
fn cert_ders(bundle_pem: &str) -> Result<Vec<Vec<u8>>, VerifyError> {
    Ok(pem::parse_many(bundle_pem)
        .map_err(|_bad| VerifyError::Malformed)?
        .into_iter()
        .filter(|block| block.tag() == "CERTIFICATE")
        .map(pem::Pem::into_contents)
        .collect())
}

/// The trust domain part of a SPIFFE id: `spiffe://<domain>/...`.
fn trust_domain_of(spiffe: &str) -> Option<&str> {
    spiffe
        .strip_prefix("spiffe://")
        .and_then(|rest| rest.split('/').next())
        .filter(|domain| !domain.is_empty())
}

/// The one in-domain SPIFFE id on a leaf, the rule the handshake and later extraction share.
pub(crate) fn grid_spiffe_id(leaf_der: &[u8], expected_domain: &str) -> Result<String, VerifyError> {
    let (_rest, leaf) = X509Certificate::from_der(leaf_der).map_err(|_bad| VerifyError::Malformed)?;
    let name = single_spiffe_name(&leaf).ok_or(VerifyError::NotOneSpiffeName)?;
    match trust_domain_of(&name) {
        Some(domain) if domain == expected_domain => Ok(name),
        _wrong_or_absent => Err(VerifyError::WrongTrustDomain {
            found: name,
            expected: expected_domain.to_owned(),
        }),
    }
}

/// The one grid-domain SPIFFE ID on a DER leaf whose chain the caller verified.
#[must_use]
pub fn leaf_spiffe_id(leaf_der: &[u8]) -> Option<String> {
    grid_spiffe_id(leaf_der, SPIFFE_TRUST_DOMAIN).ok()
}

/// The site a grid SPIFFE ID names, `None` for any other shape.
#[must_use]
pub fn site_of_spiffe_id(id: &str) -> Option<&str> {
    id.strip_prefix("spiffe://")
        .and_then(|rest| rest.strip_prefix(SPIFFE_TRUST_DOMAIN))
        .and_then(|rest| rest.strip_prefix("/site/"))
        .filter(|site| is_spiffe_segment(site))
}

/// A SPIFFE path segment: `[A-Za-z0-9._-]+`, neither `.` nor `..`.
fn is_spiffe_segment(segment: &str) -> bool {
    !matches!(segment, "" | "." | "..")
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// The one SPIFFE URI name on a certificate, when there is exactly one.
pub(crate) fn single_spiffe_name(leaf: &X509Certificate<'_>) -> Option<String> {
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
    fn a_returned_ca_is_trusted_only_when_pinned() {
        let ca = generate_ca("grid-ca").expect("ca");
        let other = generate_ca("grid-ca").expect("other ca");
        let bundle = format!("{}{}", other.cert_pem, ca.cert_pem);
        let pinned = anchored_ca(&ca.cert_pem, &bundle).expect("a pinned CA is accepted");
        assert!(
            bundle_within(&pinned, &ca.cert_pem).expect("pem"),
            "the same cert comes back"
        );
        assert_eq!(
            anchored_ca(&ca.cert_pem, &other.cert_pem),
            Err(VerifyError::NotAnchored),
            "a CA outside the anchors is refused"
        );
        assert_eq!(
            anchored_ca(&bundle, &bundle),
            Err(VerifyError::MalformedCa),
            "exactly one CA certificate is accepted"
        );
        let leaf = generate_site_cert(&ca, "site-d").expect("leaf");
        assert_eq!(
            anchored_ca(&leaf.cert_pem, &leaf.cert_pem),
            Err(VerifyError::NotSelfSignedCa),
            "a pinned leaf is not a CA"
        );
    }

    #[test]
    fn leaf_only_keeps_the_first_certificate_alone() {
        let ca = generate_ca("grid-ca").expect("ca");
        let leaf = generate_site_cert(&ca, "site-d").expect("leaf").cert_pem;
        let padded = format!("{leaf}{}trailing junk\n", ca.cert_pem);
        let only = leaf_only(&padded).expect("first block parses");
        assert_eq!(pem::parse_many(&only).expect("pem").len(), 1, "one block is kept");
        assert_eq!(bundle_within(&only, &leaf), Ok(true), "the first certificate is kept");
        assert_eq!(leaf_only("not pem"), Err(VerifyError::Malformed), "non-PEM is refused");
    }

    #[test]
    fn a_bundle_is_within_the_anchors_only_when_every_cert_is() {
        let ca = generate_ca("grid-ca").expect("ca").cert_pem;
        let other = generate_ca("other").expect("other ca").cert_pem;
        let both = format!("{ca}{other}");
        for (bundle, anchors, within) in [
            (ca.as_str(), both.as_str(), true),
            (both.as_str(), both.as_str(), true),
            (both.as_str(), ca.as_str(), false),
            (other.as_str(), ca.as_str(), false),
            ("", ca.as_str(), false),
        ] {
            assert_eq!(bundle_within(bundle, anchors), Ok(within), "{bundle} in {anchors}");
        }
        let oversized = "x".repeat(MAX_CERT_PEM_BYTES + 1);
        assert_eq!(
            bundle_within(&oversized, &ca),
            Err(VerifyError::TooLarge),
            "an oversized bundle"
        );
        assert_eq!(
            bundle_within(&ca, &oversized),
            Err(VerifyError::TooLarge),
            "oversized anchors"
        );
    }

    #[test]
    fn the_svid_profile_needs_critical_ca_false_and_signing_only() {
        let leaf = |explicit: bool| {
            let mut params = rcgen::CertificateParams::default();
            if explicit {
                params.is_ca = rcgen::IsCa::ExplicitNoCa;
                params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
            }
            let key = rcgen::KeyPair::generate().expect("key");
            params.self_signed(&key).expect("cert").pem()
        };
        assert_eq!(has_svid_profile(&leaf(true)), Ok(true), "SVID profile");
        assert_eq!(
            has_svid_profile(&leaf(false)),
            Ok(false),
            "no basic constraints or key usage"
        );
        let ca = generate_ca("grid-ca").expect("ca").cert_pem;
        assert_eq!(has_svid_profile(&ca), Ok(false), "a CA is not a leaf");
    }

    #[test]
    fn cert_and_csr_keys_compare_as_spki() {
        let ca = generate_ca("grid-ca").expect("ca");
        let csr = csr_for("site-d");
        let issued = sign_csr(&ca, "site-d", &csr, crate::Validity::default()).expect("sign");
        assert_eq!(
            cert_public_key(&issued.cert_pem).expect("cert key"),
            csr_public_key(&csr).expect("csr key"),
            "an issued leaf carries the request's key"
        );
        assert_ne!(
            cert_public_key(&issued.cert_pem).expect("cert key"),
            csr_public_key(&csr_for("site-d")).expect("csr key"),
            "another request's key does not match"
        );
    }

    #[test]
    fn a_certificate_this_grid_issued_verifies() {
        let ca = generate_ca("grid-ca").expect("ca");
        let issued = sign_csr(&ca, "site-d", &csr_for("site-d"), crate::Validity::default()).expect("sign");

        let spki = verify_site_cert(&ca.cert_pem, &issued.cert_pem, "site-d").expect("should verify");
        assert!(!spki.is_empty(), "the public key should come back for signature checks");
    }

    #[test]
    fn leaf_spiffe_id_reads_the_site_name_or_none() {
        let ca = generate_ca("grid-ca").expect("ca");
        let site = generate_site_cert(&ca, "east").expect("site");
        let der = pem::parse(&site.cert_pem).expect("pem");
        assert_eq!(
            leaf_spiffe_id(der.contents()).as_deref(),
            Some("spiffe://grid.internal/site/east")
        );
        let infra = crate::generate::generate_dns_only_cert(&ca, "grid-ca", &["a.svc".to_owned()]).expect("leaf");
        let infra_der = pem::parse(&infra.cert_pem).expect("pem");
        assert_eq!(leaf_spiffe_id(infra_der.contents()), None, "no SPIFFE name");
        assert_eq!(leaf_spiffe_id(b"junk"), None, "unparseable");
    }

    #[test]
    fn site_of_spiffe_id_reads_only_grid_site_ids() {
        let cases = [
            ("spiffe://grid.internal/site/east", Some("east")),
            ("spiffe://other.domain/site/east", None),
            ("spiffe://grid.internal/site/", None),
            ("spiffe://grid.internal/site/east/extra", None),
            ("spiffe://grid.internal/site/east?query", None),
            ("spiffe://grid.internal/site/east#fragment", None),
            ("spiffe://grid.internal/site/..", None),
            ("spiffe://grid.internal/workload/east", None),
            ("https://grid.internal/site/east", None),
        ];
        for (id, want) in cases {
            assert_eq!(site_of_spiffe_id(id), want, "{id}");
        }
    }

    #[test]
    fn cert_dns_sans_lists_the_dns_names_as_encoded() {
        let ca = generate_ca("grid-ca").expect("ca");
        let names = vec!["a.grid.svc".to_owned(), "B.Apps.Example.com".to_owned()];
        let leaf = crate::generate::generate_dns_only_cert(&ca, "grid-ca", &names).expect("leaf");
        let sans = cert_dns_sans(&leaf.cert_pem).expect("sans");
        assert!(
            names.iter().all(|name| sans.contains(name)),
            "every requested name, case kept: {sans:?}"
        );
        assert!(
            cert_dns_sans("not a certificate").is_err(),
            "garbage is an error, not empty"
        );
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
    fn verify_issued_by_checks_issuer_signature_and_validity() {
        let ca = generate_ca("grid-ca").expect("ca");
        let other = generate_ca("grid-ca").expect("other ca");
        let leaf = crate::generate_dns_only_cert(&ca, "grid-ca", &["enroll.grid.svc".to_owned()]).expect("leaf");
        let expired = generate_expired_dns_cert(&ca, "grid-ca", "enroll.grid.svc").expect("expired");

        assert_eq!(verify_issued_by(&ca.cert_pem, &leaf.cert_pem), Ok(()), "own leaf");
        assert!(
            matches!(
                verify_issued_by(&other.cert_pem, &leaf.cert_pem),
                Err(VerifyError::BadSignature | VerifyError::WrongIssuer)
            ),
            "a same-named CA with another key must not verify"
        );
        assert_eq!(
            verify_issued_by(&ca.cert_pem, &expired.cert_pem),
            Err(VerifyError::NotCurrentlyValid),
            "expired leaf"
        );
        assert_eq!(
            verify_issued_by(&ca.cert_pem, "not a cert"),
            Err(VerifyError::Malformed),
            "garbage leaf"
        );
        let (issuer, not_after) = cert_issuer_and_expiry(&leaf.cert_pem).expect("summary");
        assert!(issuer.contains("grid-ca"), "issuer names the CA: {issuer}");
        assert!(!not_after.is_empty(), "expiry is rendered");
    }

    #[test]
    fn a_ca_loaded_under_another_common_name_issues_leaves_that_chain() {
        let stored = generate_ca("grid-ca").expect("ca");
        let loaded = crate::load_ca("renamed-ca", &stored.key_pem, &stored.cert_pem).expect("load");
        let leaf = crate::generate_dns_only_cert(&loaded, "renamed-ca", &["enroll.grid.svc".to_owned()]).expect("leaf");
        assert_eq!(
            verify_issued_by(&stored.cert_pem, &leaf.cert_pem),
            Ok(()),
            "the issuer name follows the stored CA, not the configured common name"
        );
    }

    #[test]
    fn leaves_are_backdated_to_tolerate_clock_skew() {
        let ca = generate_ca("grid-ca").expect("ca");
        let leaf = crate::generate_dns_only_cert(&ca, "grid-ca", &["enroll.grid.svc".to_owned()]).expect("leaf");
        let der = pem::parse(&leaf.cert_pem).expect("pem");
        let (_rest, cert) = X509Certificate::from_der(der.contents()).expect("der");
        let skew = time::OffsetDateTime::now_utc().unix_timestamp() - cert.validity().not_before.timestamp();
        assert!(skew >= 55 * 60, "notBefore is backdated about an hour, got {skew}s");
    }

    #[test]
    fn cert_expires_within_reports_the_renewal_window() {
        let ca = generate_ca("grid-ca").expect("ca");
        let now = time::OffsetDateTime::now_utc();
        let short = crate::generate::generate_validity_bounded_dns_cert(
            &ca,
            "grid-ca",
            "enroll.grid.svc",
            now - time::Duration::minutes(5),
            now + time::Duration::days(10),
        )
        .expect("short-lived");
        let long = crate::generate_dns_only_cert(&ca, "grid-ca", &["enroll.grid.svc".to_owned()]).expect("leaf");
        let expired = generate_expired_dns_cert(&ca, "grid-ca", "enroll.grid.svc").expect("expired");
        let window = time::Duration::days(30);

        assert_eq!(cert_expires_within(&short.cert_pem, window), Ok(true), "10 days left");
        assert_eq!(cert_expires_within(&long.cert_pem, window), Ok(false), "a year left");
        assert_eq!(cert_expires_within(&expired.cert_pem, window), Ok(true), "expired");
        assert_eq!(
            cert_expires_within("not a cert", window),
            Err(VerifyError::Malformed),
            "garbage"
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

    #[test]
    fn a_leaf_reports_its_key_digest_and_validity() {
        let ca = generate_ca("grid-ca").expect("ca");
        let csr = csr_for("site-a");
        let validity = crate::Validity::starting_now(time::Duration::days(30));
        let issued = sign_csr(&ca, "site-a", &csr, validity).expect("sign");
        assert_eq!(
            cert_public_key_sha256(&issued.cert_pem),
            Ok(issued.public_key_sha256.clone()),
            "the digest enrollment records"
        );
        assert_eq!(
            crate::verify_csr(&csr).ok(),
            Some(issued.public_key_sha256),
            "the CSR names the same key"
        );
        let (not_before, not_after) = cert_validity(&issued.cert_pem).expect("validity");
        assert_eq!(not_before.unix_timestamp(), validity.not_before.unix_timestamp());
        assert_eq!(not_after.unix_timestamp(), validity.not_after.unix_timestamp());
        let der = pem::parse(&issued.cert_pem).expect("pem").into_contents();
        assert_eq!(
            canonical_fingerprint(&cert_pem_from_der(&der)),
            canonical_fingerprint(&issued.cert_pem),
            "DER round-trips to the same certificate"
        );
    }

    #[test]
    fn a_ca_signature_verifies_only_for_its_message_and_ca() {
        let ca = generate_ca("grid-ca").expect("ca");
        let other = generate_ca("grid-ca").expect("other");
        let signature = crate::sign_with_ca(&ca, b"seed").expect("sign");
        assert_eq!(verify_ca_signature(&ca.cert_pem, b"seed", &signature), Ok(()));
        assert_eq!(
            verify_ca_signature(&ca.cert_pem, b"seeds", &signature),
            Err(VerifyError::BadSignature),
            "another message"
        );
        assert_eq!(
            verify_ca_signature(&other.cert_pem, b"seed", &signature),
            Err(VerifyError::BadSignature),
            "another CA"
        );
        assert_eq!(
            verify_ca_signature(&ca.cert_pem, b"seed", b"junk"),
            Err(VerifyError::BadSignature),
            "a malformed signature"
        );
    }

    #[test]
    fn a_bundle_fingerprints_each_certificate() {
        let old = generate_ca("grid-ca").expect("old");
        let current = generate_ca("grid-ca").expect("current");
        let both = format!("{}{}", old.cert_pem, current.cert_pem);
        let fingerprints = bundle_fingerprints(&both).expect("bundle");
        assert_eq!(fingerprints.len(), 2);
        assert!(fingerprints.contains(&canonical_fingerprint(&current.cert_pem).expect("fp")));
        assert_eq!(bundle_fingerprints("not pem"), Err(VerifyError::Malformed));
        assert_eq!(bundle_fingerprints(""), Err(VerifyError::Malformed));
    }
}
