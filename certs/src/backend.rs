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
    CaMaterial, csr_spki_der, generate_ca, issue_leaf, load_ca, sha256, sign_csr, verify_leaf_signature,
};

#[cfg(feature = "fips")]
mod openssl_backend;
#[cfg(feature = "fips")]
pub(crate) use openssl_backend::{
    CaMaterial, csr_spki_der, generate_ca, issue_leaf, load_ca, sha256, sign_csr, verify_leaf_signature,
};

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

/// A certificate signed from a request, plus the request's public key.
pub(crate) struct SignedCsr {
    /// PEM-encoded certificate.
    pub cert_pem: String,
    /// The request's `SubjectPublicKeyInfo` DER, for fingerprinting the key.
    pub public_key_der: Vec<u8>,
}

/// Reasons a backend operation fails, mapped by callers onto their own errors.
#[derive(Debug)]
pub(crate) enum BackendError {
    /// Key generation failed.
    KeyGen(String),
    /// Certificate signing failed.
    Sign(String),
    /// A certificate request could not be parsed.
    ParseCsr,
    /// A certificate request's self-signature does not verify.
    CsrBadSignature,
    /// Request carries an extension this grid does not issue, rejected only by
    /// the rcgen backend. Both stay safe because `sign_csr` rebuilds the
    /// identity from the assigned site and keeps only the request's public key.
    #[cfg_attr(
        not(feature = "rcgen"),
        expect(dead_code, reason = "constructed only by the rcgen backend")
    )]
    CsrUnsupportedExtension,
    /// A CA certificate could not be parsed.
    InvalidCaCert,
    /// A CA private key could not be parsed.
    InvalidCaKey(String),
    /// A CA certificate and private key do not correspond.
    CaCertKeyMismatch,
    /// A leaf signature does not verify against the CA.
    BadSignature,
}
