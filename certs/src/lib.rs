//! Certificate management for AI Grid site-to-site mTLS.
//!
//! Provides a [`CertificateProvider`] trait that abstracts
//! certificate sourcing. The POC uses [`StaticFileProvider`]
//! (certs from disk); production will use a `SpiffeProvider`
//! (SPIRE workload API) without changing the mTLS plumbing.

#[cfg(all(feature = "rcgen", feature = "fips"))]
compile_error!("features `rcgen` and `fips` are mutually exclusive");
#[cfg(not(any(feature = "rcgen", feature = "fips")))]
compile_error!("one of `rcgen` or `fips` must be enabled");

mod backend;
mod enroll;
mod generate;
#[cfg(feature = "verifier")]
mod grid_verifier;
mod provider;
mod verify;

pub use enroll::{
    DEFAULT_SITE_CERT_LIFETIME, EnrollError, EnrolledCert, MAX_CSR_PEM_BYTES, Validity, sign_csr, validate_site_name,
    verify_csr,
};
pub use generate::{
    CaCert, DEFAULT_ORGANIZATION, GenerateError, GeneratedCsr, SPIFFE_TRUST_DOMAIN, SiteCertOutput, generate_ca,
    generate_cert_with_org, generate_csr, generate_dns_cert, generate_dns_only_cert, generate_expired_dns_cert,
    generate_not_yet_valid_dns_cert, generate_site_cert, generate_site_cert_with_names, load_ca, spiffe_id,
};
#[cfg(feature = "verifier")]
pub use grid_verifier::{DEFAULT_TRUST_DOMAIN, GridSpiffeClientVerifier, GridSpiffeServerVerifier};
pub use provider::{CertificateProvider, ProviderError, SiteCertificate, StaticFileProvider, TrustBundle};
pub use verify::{
    MAX_CERT_PEM_BYTES, VerifyError, anchored_ca, bundle_within, canonical_fingerprint, cert_dns_sans,
    cert_expires_within, cert_issuer_and_expiry, cert_public_key, csr_public_key, has_svid_profile, leaf_only,
    leaf_spiffe_id, site_of_spiffe_id, verify_issued_by, verify_site_cert,
};

/// SHA-256 through the active backend: the sha2 crate by default, system openssl
/// under `fips`. A caller hashing an identity value (a token, a public key) reuses
/// this so the digest follows the crate's FIPS backend instead of picking its own.
///
/// # Panics
///
/// Under `fips`, panics if the OpenSSL EVP SHA-256 digest fails, since a
/// FIPS-approved digest failing means the crypto module is unusable.
#[must_use]
pub fn sha256(data: &[u8]) -> [u8; 32] {
    backend::sha256(data)
}
