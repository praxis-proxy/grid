//! rcgen backend: key generation, signing, and verification for non-FIPS builds.

use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, PublicKeyData as _, SanType,
};
use x509_parser::prelude::{FromDer as _, X509Certificate};

use super::{BackendError, CertSpec, GeneratedCa, GeneratedCert, SignedCsr};

/// Material for signing site certificates under a CA.
pub(crate) struct CaMaterial {
    /// CA subject parameters, reused as the issuer for leaves.
    params: CertificateParams,
    /// CA signing key.
    key_pair: KeyPair,
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
        material: CaMaterial { params, key_pair },
    })
}

/// Mint a leaf key and certificate signed by the CA.
pub(crate) fn issue_leaf(ca: &CaMaterial, spec: &CertSpec<'_>) -> Result<GeneratedCert, BackendError> {
    let params = params_from_spec(spec)?;
    let key = KeyPair::generate().map_err(|err| BackendError::KeyGen(err.to_string()))?;
    let issuer = Issuer::new(ca.params.clone(), &ca.key_pair);
    let cert = params
        .signed_by(&key, &issuer)
        .map_err(|err| BackendError::Sign(err.to_string()))?;
    Ok(GeneratedCert {
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
    })
}

/// Sign a request's public key under the spec, returning the cert and the key DER.
pub(crate) fn sign_csr(ca: &CaMaterial, spec: &CertSpec<'_>, csr_pem: &str) -> Result<SignedCsr, BackendError> {
    // from_pem also verifies the request's self-signature.
    let mut csr = CertificateSigningRequestParams::from_pem(csr_pem).map_err(|err| map_csr_error(&err))?;
    let public_key_der = csr.public_key.der_bytes().to_vec();
    csr.params = params_from_spec(spec)?;
    let issuer = Issuer::new(ca.params.clone(), &ca.key_pair);
    let cert = csr
        .signed_by(&issuer)
        .map_err(|err| BackendError::Sign(err.to_string()))?;
    Ok(SignedCsr {
        cert_pem: cert.pem(),
        public_key_der,
    })
}

/// Verify a request's self-signature and return its `SubjectPublicKeyInfo` DER.
pub(crate) fn csr_spki_der(csr_pem: &str) -> Result<Vec<u8>, BackendError> {
    let csr = CertificateSigningRequestParams::from_pem(csr_pem).map_err(|err| map_csr_error(&err))?;
    Ok(csr.public_key.der_bytes().to_vec())
}

/// Load CA material from a PEM key and cert, checking they correspond.
pub(crate) fn load_ca(spec: &CertSpec<'_>, key_pem: &str, cert_pem: &str) -> Result<CaMaterial, BackendError> {
    let key_pair = KeyPair::from_pem(key_pem).map_err(|err| BackendError::InvalidCaKey(err.to_string()))?;

    // Cert must belong to this key: its raw public point appears in the cert DER.
    // Uses only `pem`, so no x509 parser feature is required here.
    let cert_der = pem::parse(cert_pem).map_err(|_bad| BackendError::InvalidCaCert)?;
    let key_bytes = key_pair.public_key_raw();
    if !cert_der
        .contents()
        .windows(key_bytes.len())
        .any(|window| window == key_bytes)
    {
        return Err(BackendError::CaCertKeyMismatch);
    }

    let params = params_from_spec(spec)?;
    Ok(CaMaterial { params, key_pair })
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

/// Map an rcgen request-parse failure onto the backend error it stands for.
fn map_csr_error(err: &rcgen::Error) -> BackendError {
    if *err == rcgen::Error::InvalidCertificationRequestSignature {
        BackendError::CsrBadSignature
    } else if *err == rcgen::Error::UnsupportedExtension {
        BackendError::CsrUnsupportedExtension
    } else {
        BackendError::ParseCsr
    }
}
