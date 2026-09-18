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

use super::{BackendError, CertSpec, GeneratedCa, GeneratedCert, SignedCsr};

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

/// Append the leaf extensions: basic-constraints, server/client EKU, and SANs.
fn append_leaf_extensions(
    builder: &mut X509Builder,
    spec: &CertSpec<'_>,
    issuer_cert: Option<&X509Ref>,
) -> Result<(), BackendError> {
    builder
        .append_extension(BasicConstraints::new().build().map_err(|err| sign_err(&err))?)
        .map_err(|err| sign_err(&err))?;
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
