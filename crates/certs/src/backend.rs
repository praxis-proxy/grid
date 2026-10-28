//! Crypto backend seam: rcgen (default) or OpenSSL EVP (`fips`).
//!
//! The keygen/sign/verify primitive is chosen at compile time. `fips` routes
//! through OpenSSL EVP so a FIPS host enforces the validated module.
//!
//! The two backends are equivalent under this crate's verifier (signature,
//! issuer match, SPIFFE SAN, validity), not byte-for-byte: the extension sets
//! differ, so a persisted CA should be loaded by the backend that minted it.

use time::OffsetDateTime;

#[cfg(feature = "rcgen")]
mod rcgen_backend;
#[cfg(feature = "rcgen")]
pub(crate) use rcgen_backend::{
    CaMaterial, csr_spki_der, generate_ca, generate_csr, hmac_sha256, issue_leaf, key_spki_der, load_ca, sha256,
    sign_csr, sign_message, verify_leaf_signature, verify_message,
};

#[cfg(feature = "fips")]
mod openssl_backend;
#[cfg(feature = "fips")]
pub(crate) use openssl_backend::{
    CaMaterial, csr_spki_der, generate_ca, generate_csr, hmac_sha256, issue_leaf, key_spki_der, load_ca, sha256,
    sign_csr, sign_message, verify_leaf_signature, verify_message,
};

/// The PEM tag of an RFC 5915 `ECPrivateKey`, the form cert-manager writes by default.
#[cfg(any(feature = "rcgen", test))]
pub(crate) const SEC1_TAG: &str = "EC PRIVATE KEY";

/// An RFC 5915 `ECPrivateKey` DER wrapped as the RFC 5958 `PrivateKeyInfo` a PKCS#8 reader
/// takes: version 0, the EC algorithm naming the curve the key carries in its parameters,
/// and the SEC1 bytes as the key. `None` when the bytes are not such a key or name no
/// curve. The openssl backend reads both forms itself; this lets the rcgen backend, which
/// reads PKCS#8 only, accept the same keys.
#[cfg(any(feature = "rcgen", test))]
pub(crate) fn sec1_to_pkcs8(sec1: &[u8]) -> Option<Vec<u8>> {
    use x509_parser::asn1_rs::{
        Any, Class, Error, FromDer as _, Integer, OctetString, Oid, Sequence, Tag, ToDer as _, oid,
    };

    let (_rest, key) = Sequence::from_der(sec1).ok()?;
    // parameters [0] EXPLICIT ECParameters: the named curve, the only form RFC 5915 allows.
    let curve = key
        .der_iter::<Any<'_>, Error>()
        .flatten()
        .find(|field| field.class() == Class::ContextSpecific && field.tag() == Tag(0))
        .and_then(|parameters| Oid::from_der(parameters.data).ok())
        .map(|(_after_oid, curve)| curve)?;
    let mut algorithm = oid!(1.2.840.10045.2.1).to_der_vec().ok()?;
    algorithm.extend(curve.to_der_vec().ok()?);
    let mut info = Integer::from_const_array([0]).to_der_vec().ok()?;
    info.extend(Sequence::new(algorithm.into()).to_der_vec().ok()?);
    info.extend(OctetString::new(sec1).to_der_vec().ok()?);
    Sequence::new(info.into()).to_der_vec().ok()
}

/// Refuse anything but the canonical DER encoding of an uncompressed P-256 or
/// P-384 key, so one key cannot acquire another fingerprint through its encoding.
pub(crate) fn check_request_key(spki_der: &[u8]) -> Result<(), BackendError> {
    /// P-256 `AlgorithmIdentifier` and BIT STRING header, including the uncompressed marker.
    const P256: &[u8] =
        b"\x30\x59\x30\x13\x06\x07\x2a\x86\x48\xce\x3d\x02\x01\x06\x08\x2a\x86\x48\xce\x3d\x03\x01\x07\x03\x42\x00\x04";
    /// P-384 `AlgorithmIdentifier` and BIT STRING header, including the uncompressed marker.
    const P384: &[u8] =
        b"\x30\x76\x30\x10\x06\x07\x2a\x86\x48\xce\x3d\x02\x01\x06\x05\x2b\x81\x04\x00\x22\x03\x62\x00\x04";
    for (prefix, coordinates) in [(P256, 64), (P384, 96)] {
        if spki_der
            .strip_prefix(prefix)
            .is_some_and(|point| point.len() == coordinates)
        {
            return Ok(());
        }
    }
    Err(BackendError::UnsupportedKey)
}

/// What a certificate should say, independent of the backend that mints it.
///
/// `is_ca` selects the extension set: a CA gets basic-constraints and cert-sign
/// usage, a leaf gets the server/client EKU, its DNS/SPIFFE SANs, and an org.
pub(crate) struct CertSpec<'spec> {
    /// Subject common name.
    pub common_name: &'spec str,
    /// Subject organization: `None` for a CA, `Some` for a leaf.
    pub organization: Option<&'spec str>,
    /// DNS SANs (leaf only).
    pub dns_sans: &'spec [String],
    /// SPIFFE URI SANs (leaf only).
    pub uri_sans: &'spec [String],
    /// Whether to mint a CA certificate.
    pub is_ca: bool,
    /// Not valid before.
    pub not_before: OffsetDateTime,
    /// Not valid after.
    pub not_after: OffsetDateTime,
}

/// A generated CA: its PEM pair plus the material needed to sign leaves.
pub(crate) struct GeneratedCa {
    /// PEM-encoded CA certificate.
    pub cert_pem: String,
    /// PEM-encoded CA private key.
    pub key_pem: String,
    /// Backend material for signing site certificates.
    pub material: CaMaterial,
}

/// A freshly minted certificate and the key generated with it.
pub(crate) struct GeneratedCert {
    /// PEM-encoded certificate.
    pub cert_pem: String,
    /// PEM-encoded private key.
    pub key_pem: String,
}

/// A CSR and its private key.
pub(crate) struct GeneratedCsr {
    /// PEM-encoded PKCS#10 request.
    pub csr_pem: String,
    /// PEM-encoded private key.
    pub key_pem: String,
}

/// A certificate signed from a request, plus the request's public key.
#[derive(Debug)]
pub(crate) struct SignedCsr {
    /// PEM-encoded certificate.
    pub cert_pem: String,
    /// The request's `SubjectPublicKeyInfo` DER, for fingerprinting the key.
    pub public_key_der: Vec<u8>,
}

/// Reasons a backend operation fails, mapped by callers onto their own errors.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BackendError {
    /// Key generation failed.
    KeyGen(String),
    /// Certificate signing failed.
    Sign(String),
    /// A certificate request could not be parsed.
    ParseCsr,
    /// A certificate request's self-signature does not verify.
    CsrBadSignature,
    /// A certificate request's key is not an EC key on P-256 or P-384.
    UnsupportedKey,
    /// A CA certificate could not be parsed.
    InvalidCaCert,
    /// A CA private key could not be parsed.
    InvalidCaKey(String),
    /// A CA certificate and private key do not correspond.
    CaCertKeyMismatch,
    /// A leaf signature does not verify against the CA.
    BadSignature,
}
