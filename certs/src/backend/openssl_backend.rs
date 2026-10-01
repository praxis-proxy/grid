//! OpenSSL EVP backend for FIPS builds.
//!
//! Keys come from `PkeyCtx` (EVP), not `EcKey::generate`: on a FIPS host EVP is
//! the path the validated module enforces, so a non-approved curve is refused.

use openssl::{
    asn1::Asn1Time,
    bn::{BigNum, MsbOption},
    error::ErrorStack,
    hash::MessageDigest,
    nid::Nid,
    pkey::{HasPublic, Id, PKey, PKeyRef, Private},
    pkey_ctx::PkeyCtx,
    x509::{
        X509, X509Builder, X509NameBuilder, X509Ref, X509Req,
        extension::{BasicConstraints, ExtendedKeyUsage, KeyUsage, SubjectAlternativeName},
    },
};

use super::{BackendError, CertSpec, GeneratedCa, GeneratedCert, GeneratedCsr, SignedCsr};

/// Material for signing site certificates under a CA.
pub(crate) struct CaMaterial {
    /// CA certificate, used as the issuer for leaves.
    cert: X509,
    /// CA signing key.
    key: PKey<Private>,
}

impl std::fmt::Debug for CaMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaMaterial").finish_non_exhaustive()
    }
}

/// Map an OpenSSL error onto a keygen failure.
fn keygen_err(err: &ErrorStack) -> BackendError {
    BackendError::KeyGen(err.to_string())
}

/// Map an OpenSSL error onto a signing failure.
fn sign_err(err: &ErrorStack) -> BackendError {
    BackendError::Sign(err.to_string())
}

/// SHA-256 through the OpenSSL EVP interface, so a fips host dispatches it to the
/// validated provider. The one-shot `openssl::sha::sha256` binds the legacy
/// `SHA256()` symbol, which runs libcrypto built-in code outside the module.
#[expect(
    clippy::expect_used,
    reason = "SHA-256 is FIPS-approved, so an EVP digest failure means the crypto module is unusable and the process must fail closed"
)]
pub(crate) fn sha256(data: &[u8]) -> [u8; 32] {
    let digest = openssl::hash::hash(MessageDigest::sha256(), data)
        .expect("SHA-256 EVP digest failed, so the crypto module is unusable");
    let mut out = [0_u8; 32];
    out.copy_from_slice(&digest);
    out
}

/// Generate a P-256 key pair through EVP (the FIPS-enforced path).
fn generate_p256() -> Result<PKey<Private>, BackendError> {
    let mut ctx = PkeyCtx::new_id(Id::EC).map_err(|err| keygen_err(&err))?;
    ctx.keygen_init().map_err(|err| keygen_err(&err))?;
    ctx.set_ec_paramgen_curve_nid(Nid::X9_62_PRIME256V1)
        .map_err(|err| keygen_err(&err))?;
    ctx.keygen().map_err(|err| keygen_err(&err))
}

/// Build the subject name for a spec: common name, and organization for a leaf.
fn subject_name(spec: &CertSpec<'_>) -> Result<openssl::x509::X509Name, BackendError> {
    let mut builder = X509NameBuilder::new().map_err(|err| sign_err(&err))?;
    builder
        .append_entry_by_text("CN", spec.common_name)
        .map_err(|err| sign_err(&err))?;
    if let Some(org) = spec.organization {
        builder.append_entry_by_text("O", org).map_err(|err| sign_err(&err))?;
    }
    Ok(builder.build())
}

/// Append the CA extensions: critical basic-constraints CA and cert/CRL signing.
fn append_ca_extensions(builder: &mut X509Builder) -> Result<(), BackendError> {
    builder
        .append_extension(
            BasicConstraints::new()
                .critical()
                .ca()
                .build()
                .map_err(|err| sign_err(&err))?,
        )
        .map_err(|err| sign_err(&err))?;
    let usage = KeyUsage::new()
        .critical()
        .key_cert_sign()
        .crl_sign()
        .build()
        .map_err(|err| sign_err(&err))?;
    builder.append_extension(usage).map_err(|err| sign_err(&err))
}

/// Append the X.509-SVID leaf constraints: critical CA:FALSE and digitalSignature only.
fn append_svid_leaf_extensions(builder: &mut X509Builder) -> Result<(), BackendError> {
    let constraints = BasicConstraints::new()
        .critical()
        .build()
        .map_err(|err| sign_err(&err))?;
    builder.append_extension(constraints).map_err(|err| sign_err(&err))?;
    let usage = KeyUsage::new()
        .critical()
        .digital_signature()
        .build()
        .map_err(|err| sign_err(&err))?;
    builder.append_extension(usage).map_err(|err| sign_err(&err))
}

/// Append the leaf extensions: the X.509-SVID constraints, server/client EKU, and SANs.
fn append_leaf_extensions(
    builder: &mut X509Builder,
    spec: &CertSpec<'_>,
    issuer_cert: Option<&X509Ref>,
) -> Result<(), BackendError> {
    append_svid_leaf_extensions(builder)?;
    let eku = ExtendedKeyUsage::new()
        .server_auth()
        .client_auth()
        .build()
        .map_err(|err| sign_err(&err))?;
    builder.append_extension(eku).map_err(|err| sign_err(&err))?;
    if spec.dns_sans.is_empty() && spec.uri_sans.is_empty() {
        return Ok(());
    }
    let mut san = SubjectAlternativeName::new();
    for dns in spec.dns_sans {
        san.dns(dns);
    }
    for uri in spec.uri_sans {
        san.uri(uri);
    }
    let extension = {
        let ctx = builder.x509v3_context(issuer_cert, None);
        san.build(&ctx).map_err(|err| sign_err(&err))?
    };
    builder.append_extension(extension).map_err(|err| sign_err(&err))
}

/// Set version, serial, subject, issuer, key, and validity on a fresh builder.
fn init_builder<T: HasPublic>(
    builder: &mut X509Builder,
    spec: &CertSpec<'_>,
    subject_pubkey: &PKeyRef<T>,
    issuer_cert: Option<&X509Ref>,
) -> Result<(), BackendError> {
    builder.set_version(2).map_err(|err| sign_err(&err))?;

    let mut serial = BigNum::new().map_err(|err| sign_err(&err))?;
    serial
        .rand(159, MsbOption::MAYBE_ZERO, false)
        .map_err(|err| sign_err(&err))?;
    let serial = serial.to_asn1_integer().map_err(|err| sign_err(&err))?;
    builder.set_serial_number(&serial).map_err(|err| sign_err(&err))?;

    let subject = subject_name(spec)?;
    builder.set_subject_name(&subject).map_err(|err| sign_err(&err))?;
    match issuer_cert {
        Some(ca) => builder
            .set_issuer_name(ca.subject_name())
            .map_err(|err| sign_err(&err))?,
        None => builder.set_issuer_name(&subject).map_err(|err| sign_err(&err))?,
    }
    builder.set_pubkey(subject_pubkey).map_err(|err| sign_err(&err))?;

    let not_before = Asn1Time::from_unix(spec.not_before.unix_timestamp()).map_err(|err| sign_err(&err))?;
    let not_after = Asn1Time::from_unix(spec.not_after.unix_timestamp()).map_err(|err| sign_err(&err))?;
    builder.set_not_before(&not_before).map_err(|err| sign_err(&err))?;
    builder.set_not_after(&not_after).map_err(|err| sign_err(&err))
}

/// Build and sign a certificate. `issuer_cert`/`signing_key` are `None`/self for
/// a self-signed CA, or the CA cert and key for a leaf.
fn build_signed_cert<T: HasPublic>(
    spec: &CertSpec<'_>,
    subject_pubkey: &PKeyRef<T>,
    issuer_cert: Option<&X509Ref>,
    signing_key: &PKeyRef<Private>,
) -> Result<X509, BackendError> {
    let mut builder = X509Builder::new().map_err(|err| sign_err(&err))?;
    init_builder(&mut builder, spec, subject_pubkey, issuer_cert)?;
    if spec.is_ca {
        append_ca_extensions(&mut builder)?;
    } else {
        append_leaf_extensions(&mut builder, spec, issuer_cert)?;
    }
    builder
        .sign(signing_key, MessageDigest::sha256())
        .map_err(|err| sign_err(&err))?;
    Ok(builder.build())
}

/// Encode a certificate as PEM.
fn to_pem(cert: &X509) -> Result<String, BackendError> {
    let pem = cert.to_pem().map_err(|err| sign_err(&err))?;
    String::from_utf8(pem).map_err(|err| BackendError::Sign(err.to_string()))
}

/// Encode a private key as PKCS#8 PEM.
fn key_to_pem(key: &PKeyRef<Private>) -> Result<String, BackendError> {
    let pem = key.private_key_to_pem_pkcs8().map_err(|err| sign_err(&err))?;
    String::from_utf8(pem).map_err(|err| BackendError::Sign(err.to_string()))
}

/// Generate a self-signed CA from the spec.
pub(crate) fn generate_ca(spec: &CertSpec<'_>) -> Result<GeneratedCa, BackendError> {
    let key = generate_p256()?;
    let cert = build_signed_cert(spec, &key, None, &key)?;
    Ok(GeneratedCa {
        cert_pem: to_pem(&cert)?,
        key_pem: key_to_pem(&key)?,
        material: CaMaterial { cert, key },
    })
}

/// Generate a key and a CSR naming only `common_name`.
pub(crate) fn generate_csr(common_name: &str) -> Result<GeneratedCsr, BackendError> {
    let key = generate_p256()?;
    let mut subject = X509NameBuilder::new().map_err(|err| sign_err(&err))?;
    subject
        .append_entry_by_text("CN", common_name)
        .map_err(|err| sign_err(&err))?;
    let subject = subject.build();
    let mut builder = X509Req::builder().map_err(|err| sign_err(&err))?;
    builder.set_subject_name(&subject).map_err(|err| sign_err(&err))?;
    builder.set_pubkey(&key).map_err(|err| sign_err(&err))?;
    builder
        .sign(&key, MessageDigest::sha256())
        .map_err(|err| sign_err(&err))?;
    let req = builder.build();
    let csr_pem = String::from_utf8(req.to_pem().map_err(|err| sign_err(&err))?)
        .map_err(|err| BackendError::Sign(err.to_string()))?;
    Ok(GeneratedCsr {
        csr_pem,
        key_pem: key_to_pem(&key)?,
    })
}

/// Mint a leaf key and certificate signed by the CA.
pub(crate) fn issue_leaf(ca: &CaMaterial, spec: &CertSpec<'_>) -> Result<GeneratedCert, BackendError> {
    let key = generate_p256()?;
    let cert = build_signed_cert(spec, &key, Some(&ca.cert), &ca.key)?;
    Ok(GeneratedCert {
        cert_pem: to_pem(&cert)?,
        key_pem: key_to_pem(&key)?,
    })
}

/// Sign a request's public key under the spec, returning the cert and the key DER.
pub(crate) fn sign_csr(ca: &CaMaterial, spec: &CertSpec<'_>, csr_pem: &str) -> Result<SignedCsr, BackendError> {
    let req = X509Req::from_pem(csr_pem.as_bytes()).map_err(|_bad| BackendError::ParseCsr)?;
    let request_key = req.public_key().map_err(|_bad| BackendError::ParseCsr)?;
    // A verify error means an unusable key, so treat it as a bad request.
    if !req.verify(&request_key).map_err(|_bad| BackendError::CsrBadSignature)? {
        return Err(BackendError::CsrBadSignature);
    }
    // Only the request's public key is carried forward. Names come from `spec`.
    let cert = build_signed_cert(spec, &request_key, Some(&ca.cert), &ca.key)?;
    Ok(SignedCsr {
        cert_pem: to_pem(&cert)?,
        public_key_der: request_key.public_key_to_der().map_err(|err| sign_err(&err))?,
    })
}

/// Verify a request's self-signature and return its `SubjectPublicKeyInfo` DER.
pub(crate) fn csr_spki_der(csr_pem: &str) -> Result<Vec<u8>, BackendError> {
    let req = X509Req::from_pem(csr_pem.as_bytes()).map_err(|_bad| BackendError::ParseCsr)?;
    let request_key = req.public_key().map_err(|_bad| BackendError::ParseCsr)?;
    // A verify error means an unusable key, so treat it as a bad request.
    if !req.verify(&request_key).map_err(|_bad| BackendError::CsrBadSignature)? {
        return Err(BackendError::CsrBadSignature);
    }
    request_key.public_key_to_der().map_err(|err| sign_err(&err))
}

/// Load CA material from a PEM key and cert, checking they correspond.
pub(crate) fn load_ca(_spec: &CertSpec<'_>, key_pem: &str, cert_pem: &str) -> Result<CaMaterial, BackendError> {
    let key =
        PKey::private_key_from_pem(key_pem.as_bytes()).map_err(|err| BackendError::InvalidCaKey(err.to_string()))?;
    let cert = X509::from_pem(cert_pem.as_bytes()).map_err(|_bad| BackendError::InvalidCaCert)?;
    let cert_key = cert.public_key().map_err(|_bad| BackendError::InvalidCaCert)?;
    if !cert_key.public_eq(&key) {
        return Err(BackendError::CaCertKeyMismatch);
    }
    Ok(CaMaterial { cert, key })
}

/// Verify a leaf's signature against the CA public key.
pub(crate) fn verify_leaf_signature(ca_cert_pem: &str, leaf_pem: &str) -> Result<(), BackendError> {
    let ca = X509::from_pem(ca_cert_pem.as_bytes()).map_err(|_bad| BackendError::InvalidCaCert)?;
    let ca_key = ca.public_key().map_err(|_bad| BackendError::InvalidCaCert)?;
    let leaf = X509::from_pem(leaf_pem.as_bytes()).map_err(|_bad| BackendError::BadSignature)?;
    if leaf.verify(&ca_key).map_err(|err| sign_err(&err))? {
        Ok(())
    } else {
        Err(BackendError::BadSignature)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::unwrap_used, reason = "tests")]
mod tests {
    use time::{Duration, OffsetDateTime};
    use x509_parser::prelude::{FromDer as _, X509Certificate};

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

    /// Build a request with rcgen so its self-signature is well-formed.
    fn valid_csr() -> String {
        let key = rcgen::KeyPair::generate().expect("key");
        let params = rcgen::CertificateParams::default();
        params.serialize_request(&key).expect("csr").pem().expect("pem")
    }

    #[test]
    fn generate_p256_produces_an_ec_key() {
        let key = generate_p256().expect("keygen");
        assert_eq!(key.id(), Id::EC, "the FIPS path must generate an EC key");
        assert_eq!(key.bits(), 256, "the curve must be P-256");
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
    fn sha256_matches_a_known_vector() {
        // SHA-256("hello world"), the canonical test vector.
        let digest = sha256(b"hello world");
        let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(
            hex, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
            "EVP SHA-256 must match the published vector"
        );
    }

    #[test]
    fn generate_ca_marks_the_certificate_as_a_ca() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        assert!(ca.cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(ca.key_pem.contains("BEGIN PRIVATE KEY"));
        let der = pem::parse(&ca.cert_pem).expect("pem");
        let (_rest, cert) = X509Certificate::from_der(der.contents()).expect("parse");
        let bc = cert.basic_constraints().expect("bc").expect("present");
        assert!(bc.value.ca, "a CA certificate must assert the CA basic constraint");
    }

    #[test]
    fn issue_leaf_with_no_sans_still_signs() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        let material = load_ca(&ca_spec("grid-ca"), &ca.key_pem, &ca.cert_pem).expect("load");
        let leaf = issue_leaf(&material, &leaf_spec("site-d", &[], &[])).expect("leaf");
        let der = pem::parse(&leaf.cert_pem).expect("pem");
        let (_rest, cert) = X509Certificate::from_der(der.contents()).expect("parse");
        assert!(
            cert.subject_alternative_name().expect("san call").is_none(),
            "a leaf with no requested names carries no SAN extension"
        );
    }

    #[test]
    fn append_leaf_extensions_carries_many_sans() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        let material = load_ca(&ca_spec("grid-ca"), &ca.key_pem, &ca.cert_pem).expect("load");
        let dns: Vec<String> = (0..8).map(|index| format!("node-{index}.grid.internal")).collect();
        let uris = vec!["spiffe://grid.internal/site/site-d".to_owned()];
        let leaf = issue_leaf(&material, &leaf_spec("site-d", &dns, &uris)).expect("leaf");
        let der = pem::parse(&leaf.cert_pem).expect("pem");
        let (_rest, cert) = X509Certificate::from_der(der.contents()).expect("parse");
        let san = cert.subject_alternative_name().expect("san").expect("present");
        assert_eq!(
            san.value.general_names.len(),
            dns.len() + uris.len(),
            "every requested DNS and URI name must reach the certificate"
        );
    }

    #[test]
    fn subject_name_rejects_a_common_name_past_the_x509_limit() {
        let long = "a".repeat(65);
        assert!(
            subject_name(&leaf_spec(&long, &[], &[])).is_err(),
            "a common name longer than 64 characters is not a valid X.509 CN"
        );
    }

    #[test]
    fn sign_csr_carries_only_the_controller_identity() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        let material = load_ca(&ca_spec("grid-ca"), &ca.key_pem, &ca.cert_pem).expect("load");
        let uris = vec!["spiffe://grid.internal/site/site-d".to_owned()];
        let signed = sign_csr(&material, &leaf_spec("site-d", &[], &uris), &valid_csr()).expect("sign");
        assert!(signed.cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(
            !signed.public_key_der.is_empty(),
            "the request's key comes back for fingerprinting"
        );
    }

    #[test]
    fn sign_csr_rejects_a_tampered_request() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        let material = load_ca(&ca_spec("grid-ca"), &ca.key_pem, &ca.cert_pem).expect("load");
        let csr = valid_csr();
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
    fn csr_spki_der_rejects_malformed_input() {
        assert_eq!(
            csr_spki_der("not a request").unwrap_err(),
            BackendError::ParseCsr,
            "bytes that are not a request must be refused"
        );
    }

    #[test]
    fn csr_spki_der_returns_the_key_for_a_valid_request() {
        let spki = csr_spki_der(&valid_csr()).expect("spki");
        assert!(!spki.is_empty(), "a valid request yields its public key");
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
    fn load_ca_rejects_malformed_key_pem() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        assert!(
            matches!(
                load_ca(&ca_spec("grid-ca"), "not a key", &ca.cert_pem),
                Err(BackendError::InvalidCaKey(_))
            ),
            "a malformed key PEM must be reported as an invalid key"
        );
    }

    #[test]
    fn load_ca_rejects_malformed_cert_pem() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        assert_eq!(
            load_ca(&ca_spec("grid-ca"), &ca.key_pem, "not a cert").unwrap_err(),
            BackendError::InvalidCaCert,
            "a malformed cert PEM must be reported as an invalid cert"
        );
    }

    #[test]
    fn verify_leaf_signature_accepts_a_leaf_signed_by_the_ca() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        let material = load_ca(&ca_spec("grid-ca"), &ca.key_pem, &ca.cert_pem).expect("load");
        let leaf = issue_leaf(&material, &leaf_spec("site-d", &[], &[])).expect("leaf");
        verify_leaf_signature(&ca.cert_pem, &leaf.cert_pem).expect("a leaf signed by the CA must verify");
    }

    #[test]
    fn verify_leaf_signature_rejects_a_leaf_from_another_ca() {
        let ours = generate_ca(&ca_spec("grid-ca")).expect("ca");
        let theirs = generate_ca(&ca_spec("grid-ca")).expect("other ca");
        let theirs_material = load_ca(&ca_spec("grid-ca"), &theirs.key_pem, &theirs.cert_pem).expect("load");
        let leaf = issue_leaf(&theirs_material, &leaf_spec("site-d", &[], &[])).expect("leaf");
        assert_eq!(
            verify_leaf_signature(&ours.cert_pem, &leaf.cert_pem).unwrap_err(),
            BackendError::BadSignature,
            "a leaf signed by another CA must not verify against ours"
        );
    }

    #[test]
    fn verify_leaf_signature_rejects_a_self_signed_ca_as_a_leaf() {
        let ours = generate_ca(&ca_spec("grid-ca")).expect("ca");
        let other = generate_ca(&ca_spec("grid-ca")).expect("other self-signed ca");
        assert_eq!(
            verify_leaf_signature(&ours.cert_pem, &other.cert_pem).unwrap_err(),
            BackendError::BadSignature,
            "a self-signed certificate from another key must not verify under ours"
        );
    }

    #[test]
    fn verify_leaf_signature_rejects_malformed_input() {
        let ca = generate_ca(&ca_spec("grid-ca")).expect("ca");
        assert_eq!(
            verify_leaf_signature(&ca.cert_pem, "not a cert").unwrap_err(),
            BackendError::BadSignature,
            "a malformed leaf must not verify"
        );
    }
}
