//! rcgen backend: key generation, signing, and verification for non-FIPS builds.
//!
//! A request contributes only its public key. Requested extensions are ignored,
//! never rejected: the issued certificate carries only the controller-owned
//! `CertSpec`, so a requester extension never reaches the output either way.
//! Ignoring keeps acceptance identical to the openssl backend, so the same
//! request is accepted whichever backend a build compiles.

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose,
    SanType, SubjectPublicKeyInfo,
};
use x509_parser::{
    certification_request::X509CertificationRequest,
    prelude::{FromDer as _, X509Certificate},
};

use super::{BackendError, CertSpec, GeneratedCa, GeneratedCert, SignedCsr};

/// Parse a request, verify its self-signature, and return its
/// `SubjectPublicKeyInfo` DER.
///
/// The requested extensions are not read: parsing goes through x509-parser
/// rather than rcgen's request parser, which rejects extensions it does not
/// recognize. This is what makes acceptance identical to the openssl backend.
fn request_spki_der(csr_pem: &str) -> Result<Vec<u8>, BackendError> {
    let der = pem::parse(csr_pem).map_err(|_bad| BackendError::ParseCsr)?;
    let (_rest, csr) = X509CertificationRequest::from_der(der.contents()).map_err(|_bad| BackendError::ParseCsr)?;
    // The self-signature proves the requester holds the private half.
    csr.verify_signature().map_err(|_bad| BackendError::CsrBadSignature)?;
    Ok(csr.certification_request_info.subject_pki.raw.to_vec())
}

/// Material for signing site certificates under a CA.
pub(crate) struct CaMaterial {
    /// The CA as issuer: its subject name, key usages, and signing key.
    issuer: Issuer<'static, KeyPair>,
}

impl std::fmt::Debug for CaMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaMaterial").finish_non_exhaustive()
    }
}

/// Build certificate parameters from a backend-neutral spec.
fn params_from_spec(spec: &CertSpec<'_>) -> Result<CertificateParams, BackendError> {
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, spec.common_name);
    if let Some(org) = spec.organization {
        params.distinguished_name.push(DnType::OrganizationName, org);
    }
    if spec.is_ca {
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages.push(KeyUsagePurpose::KeyCertSign);
        params.key_usages.push(KeyUsagePurpose::CrlSign);
    } else {
        for dns in spec.dns_sans {
            let name = dns
                .clone()
                .try_into()
                .map_err(|err: rcgen::Error| BackendError::Sign(err.to_string()))?;
            params.subject_alt_names.push(SanType::DnsName(name));
        }
        for uri in spec.uri_sans {
            let name = uri
                .clone()
                .try_into()
                .map_err(|err: rcgen::Error| BackendError::Sign(err.to_string()))?;
            params.subject_alt_names.push(SanType::URI(name));
        }
        params.extended_key_usages.push(ExtendedKeyUsagePurpose::ServerAuth);
        params.extended_key_usages.push(ExtendedKeyUsagePurpose::ClientAuth);
    }
    params.not_before = spec.not_before;
    params.not_after = spec.not_after;
    Ok(params)
}

/// Generate a self-signed CA from the spec.
pub(crate) fn generate_ca(spec: &CertSpec<'_>) -> Result<GeneratedCa, BackendError> {
    let params = params_from_spec(spec)?;
    let key_pair = KeyPair::generate().map_err(|err| BackendError::KeyGen(err.to_string()))?;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|err| BackendError::Sign(err.to_string()))?;
    Ok(GeneratedCa {
        cert_pem: cert.pem(),
        key_pem: key_pair.serialize_pem(),
        material: CaMaterial {
            issuer: Issuer::new(params, key_pair),
        },
    })
}

/// Mint a leaf key and certificate signed by the CA.
pub(crate) fn issue_leaf(ca: &CaMaterial, spec: &CertSpec<'_>) -> Result<GeneratedCert, BackendError> {
    let params = params_from_spec(spec)?;
    let key = KeyPair::generate().map_err(|err| BackendError::KeyGen(err.to_string()))?;
    let cert = params
        .signed_by(&key, &ca.issuer)
        .map_err(|err| BackendError::Sign(err.to_string()))?;
    Ok(GeneratedCert {
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
    })
}

/// Sign a request's public key under the spec, returning the cert and key DER.
///
/// Only the request's public key is carried forward; the identity comes from
/// `spec`. Requested extensions are ignored (see the module comment).
pub(crate) fn sign_csr(ca: &CaMaterial, spec: &CertSpec<'_>, csr_pem: &str) -> Result<SignedCsr, BackendError> {
    let spki_der = request_spki_der(csr_pem)?;
    let public_key = SubjectPublicKeyInfo::from_der(&spki_der).map_err(|err| BackendError::Sign(err.to_string()))?;
    let params = params_from_spec(spec)?;
    let cert = params
        .signed_by(&public_key, &ca.issuer)
        .map_err(|err| BackendError::Sign(err.to_string()))?;
    Ok(SignedCsr {
        cert_pem: cert.pem(),
        public_key_der: spki_der,
    })
}

/// Verify a request's self-signature and return its `SubjectPublicKeyInfo` DER.
pub(crate) fn csr_spki_der(csr_pem: &str) -> Result<Vec<u8>, BackendError> {
    request_spki_der(csr_pem)
}

/// Load CA material from a PEM key and cert, checking they correspond.
///
/// The issuer name comes from the stored cert, not `_spec`, so leaves keep
/// chaining to it when the configured common name changes.
pub(crate) fn load_ca(_spec: &CertSpec<'_>, key_pem: &str, cert_pem: &str) -> Result<CaMaterial, BackendError> {
    let key_pair = KeyPair::from_pem(key_pem).map_err(|err| BackendError::InvalidCaKey(err.to_string()))?;

    // Cert must certify this key: the point in its SPKI is the key pair's point.
    let cert_der = pem::parse(cert_pem).map_err(|_bad| BackendError::InvalidCaCert)?;
    let (_rest, cert) = X509Certificate::from_der(cert_der.contents()).map_err(|_bad| BackendError::InvalidCaCert)?;
    if cert.public_key().subject_public_key.data.as_ref() != key_pair.public_key_raw() {
        return Err(BackendError::CaCertKeyMismatch);
    }

    let issuer = Issuer::from_ca_cert_der(&cert_der.contents().to_vec().into(), key_pair)
        .map_err(|_bad| BackendError::InvalidCaCert)?;
    Ok(CaMaterial { issuer })
}

/// Verify a leaf's signature against the CA public key.
pub(crate) fn verify_leaf_signature(ca_cert_pem: &str, leaf_pem: &str) -> Result<(), BackendError> {
    let ca_der = pem::parse(ca_cert_pem).map_err(|_bad| BackendError::InvalidCaCert)?;
    let (_after_ca, ca) = X509Certificate::from_der(ca_der.contents()).map_err(|_bad| BackendError::InvalidCaCert)?;
    let leaf_der = pem::parse(leaf_pem).map_err(|_bad| BackendError::BadSignature)?;
    let (_after_leaf, leaf) =
        X509Certificate::from_der(leaf_der.contents()).map_err(|_bad| BackendError::BadSignature)?;
    leaf.verify_signature(Some(ca.public_key()))
        .map_err(|_bad| BackendError::BadSignature)
}

/// SHA-256 digest (pure-Rust; the non-FIPS default path).
pub(crate) fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::{Digest as _, Sha256};
    Sha256::digest(data).into()
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::unwrap_used, reason = "tests")]
mod tests {
    use rcgen::CustomExtension;
    use time::{Duration, OffsetDateTime};

    use super::*;

    fn window() -> (OffsetDateTime, OffsetDateTime) {
        let now = OffsetDateTime::now_utc();
        (
            now.saturating_sub(Duration::minutes(5)),
            now.saturating_add(Duration::days(30)),
        )
    }

    fn ca_spec(common_name: &str) -> CertSpec<'_> {
        let (not_before, not_after) = window();
        CertSpec {
            common_name,
            organization: None,
            dns_sans: &[],
            uri_sans: &[],
            is_ca: true,
            not_before,
            not_after,
        }
    }

    fn leaf_spec<'spec>(
        common_name: &'spec str,
        dns_sans: &'spec [String],
        uri_sans: &'spec [String],
    ) -> CertSpec<'spec> {
        let (not_before, not_after) = window();
        CertSpec {
            common_name,
            organization: Some("ai-grid"),
            dns_sans,
            uri_sans,
            is_ca: false,
            not_before,
            not_after,
        }
    }

    fn plain_csr() -> String {
        let key = KeyPair::generate().expect("key");
        CertificateParams::default()
            .serialize_request(&key)
            .expect("csr")
            .pem()
            .expect("pem")
    }

    /// A request carrying an extension outside the recognized set.
    fn csr_with_unsupported_extension() -> String {
        let key = KeyPair::generate().expect("key");
        let mut params = CertificateParams::default();
        params.custom_extensions.push(CustomExtension::from_oid_content(
            &[1, 3, 6, 1, 4, 1, 57264, 1],
            vec![0x05, 0x00],
        ));
        params.serialize_request(&key).expect("csr").pem().expect("pem")
    }

    #[test]
    fn sha256_matches_a_known_vector() {
        let digest = sha256(b"hello world");
        let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(
            hex, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
            "the sha2 path must match the published vector"
        );
    }

    #[test]
    fn params_from_spec_marks_a_ca_and_gives_it_signing_usage() {
        let params = params_from_spec(&ca_spec("grid-ca")).expect("params");
        assert!(matches!(params.is_ca, IsCa::Ca(_)), "a CA spec must produce CA params");
        assert!(
            params.key_usages.contains(&KeyUsagePurpose::KeyCertSign),
            "a CA must be allowed to sign certificates"
        );
        assert!(params.extended_key_usages.is_empty(), "a CA carries no EKU");
    }

    #[test]
    fn params_from_spec_gives_a_leaf_server_and_client_eku_and_no_ca_flag() {
        let params = params_from_spec(&leaf_spec("site-d", &[], &[])).expect("params");
        assert!(matches!(params.is_ca, IsCa::NoCa), "a leaf spec must not be a CA");
        assert!(
            params
                .extended_key_usages
                .contains(&ExtendedKeyUsagePurpose::ServerAuth)
                && params
                    .extended_key_usages
                    .contains(&ExtendedKeyUsagePurpose::ClientAuth),
            "a leaf must carry server and client auth EKU"
        );
    }

    #[test]
    fn generate_ca_marks_the_certificate_as_a_ca() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        let der = pem::parse(&ca.cert_pem).expect("pem");
        let (_rest, cert) = X509Certificate::from_der(der.contents()).expect("parse");
        let bc = cert.basic_constraints().expect("bc").expect("present");
        assert!(bc.value.ca, "a CA certificate must assert the CA basic constraint");
    }

    #[test]
    fn ca_material_debug_does_not_print_key_material() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        let material = load_ca(&ca_spec("grid-ca"), &ca.key_pem, &ca.cert_pem).expect("load");
        assert_eq!(
            format!("{material:?}"),
            "CaMaterial { .. }",
            "the signing key must not appear in a debug rendering"
        );
    }

    #[test]
    fn issue_leaf_carries_the_requested_names() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        let material = load_ca(&ca_spec("grid-ca"), &ca.key_pem, &ca.cert_pem).expect("load");
        let dns = vec!["site-d.grid.internal".to_owned()];
        let uris = vec!["spiffe://grid.internal/site/site-d".to_owned()];
        let leaf = issue_leaf(&material, &leaf_spec("site-d", &dns, &uris)).expect("leaf");
        let der = pem::parse(&leaf.cert_pem).expect("pem");
        let (_rest, cert) = X509Certificate::from_der(der.contents()).expect("parse");
        let san = cert.subject_alternative_name().expect("san").expect("present");
        assert_eq!(
            san.value.general_names.len(),
            2,
            "both the DNS and URI name must reach the leaf"
        );
    }

    #[test]
    fn sign_csr_ignores_an_unsupported_extension() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        let material = load_ca(&ca_spec("grid-ca"), &ca.key_pem, &ca.cert_pem).expect("load");
        let uris = vec!["spiffe://grid.internal/site/site-d".to_owned()];
        let signed = sign_csr(
            &material,
            &leaf_spec("site-d", &[], &uris),
            &csr_with_unsupported_extension(),
        );
        assert!(
            signed.is_ok(),
            "the rcgen backend must ignore an unsupported requested extension, not reject it"
        );
    }

    #[test]
    fn sign_csr_rejects_a_tampered_request() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        let material = load_ca(&ca_spec("grid-ca"), &ca.key_pem, &ca.cert_pem).expect("load");
        let csr = plain_csr();
        let der = pem::parse(&csr).expect("pem");
        let mut bytes = der.contents().to_vec();
        if let Some(last) = bytes.last_mut() {
            *last ^= 0xFF;
        }
        let tampered = pem::encode(&pem::Pem::new("CERTIFICATE REQUEST", bytes));
        assert_eq!(
            sign_csr(&material, &leaf_spec("site-d", &[], &[]), &tampered).unwrap_err(),
            BackendError::CsrBadSignature,
            "a request not signed by its own key must be refused"
        );
    }

    #[test]
    fn csr_spki_der_rejects_malformed_input_and_returns_a_key_for_a_valid_request() {
        assert_eq!(
            csr_spki_der("not a request").unwrap_err(),
            BackendError::ParseCsr,
            "bytes that are not a request must be refused"
        );
        assert!(
            !csr_spki_der(&plain_csr()).expect("spki").is_empty(),
            "a valid request yields its key"
        );
    }

    #[test]
    fn load_ca_accepts_a_matching_pair() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        assert!(
            load_ca(&ca_spec("grid-ca"), &ca.key_pem, &ca.cert_pem).is_ok(),
            "a cert and key from the same pair must load"
        );
    }

    #[test]
    fn load_ca_rejects_a_cert_and_key_from_different_pairs() {
        let ca_a = generate_ca(&ca_spec("grid-ca")).expect("ca a");
        let ca_b = generate_ca(&ca_spec("grid-ca")).expect("ca b");
        assert_eq!(
            load_ca(&ca_spec("grid-ca"), &ca_b.key_pem, &ca_a.cert_pem).unwrap_err(),
            BackendError::CaCertKeyMismatch,
            "a cert and key from different pairs must not load"
        );
    }

    #[test]
    fn load_ca_rejects_malformed_key_and_cert_pem() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        assert!(
            matches!(
                load_ca(&ca_spec("grid-ca"), "not a key", &ca.cert_pem),
                Err(BackendError::InvalidCaKey(_))
            ),
            "a malformed key PEM must be an invalid key"
        );
        assert_eq!(
            load_ca(&ca_spec("grid-ca"), &ca.key_pem, "not a cert").unwrap_err(),
            BackendError::InvalidCaCert,
            "a malformed cert PEM must be an invalid cert"
        );
    }

    #[test]
    fn verify_leaf_signature_accepts_a_leaf_and_rejects_another_ca_and_rubbish() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        let material = load_ca(&ca_spec("grid-ca"), &ca.key_pem, &ca.cert_pem).expect("load");
        let leaf = issue_leaf(&material, &leaf_spec("site-d", &[], &[])).expect("leaf");
        verify_leaf_signature(&ca.cert_pem, &leaf.cert_pem).expect("a leaf signed by the CA must verify");

        let other = generate_ca(&ca_spec("grid-ca")).expect("other ca");
        assert_eq!(
            verify_leaf_signature(&other.cert_pem, &leaf.cert_pem).unwrap_err(),
            BackendError::BadSignature,
            "a leaf must not verify against another CA"
        );
        assert_eq!(
            verify_leaf_signature(&ca.cert_pem, "not a cert").unwrap_err(),
            BackendError::BadSignature,
            "a malformed leaf must not verify"
        );
    }
}
