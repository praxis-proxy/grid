//! Certificate management for AI Grid site-to-site mTLS.
//!
//! Provides a [`CertificateProvider`] trait that abstracts
//! certificate sourcing. The POC uses [`StaticFileProvider`]
//! (certs from disk); production will use a `SpiffeProvider`
//! (SPIRE workload API) without changing the mTLS plumbing.

mod enroll;
mod generate;
mod provider;
mod verify;

pub use enroll::{
    DEFAULT_SITE_CERT_LIFETIME, EnrollError, EnrolledCert, MAX_CSR_PEM_BYTES, Validity, sign_csr, validate_site_name,
    verify_csr,
};
pub use generate::{
    CaCert, DEFAULT_ORGANIZATION, GenerateError, SPIFFE_TRUST_DOMAIN, SiteCertOutput, generate_ca,
    generate_cert_with_org, generate_dns_cert, generate_expired_dns_cert, generate_not_yet_valid_dns_cert,
    generate_site_cert, generate_site_cert_with_names, load_ca, spiffe_id,
};
pub use provider::{CertificateProvider, ProviderError, SiteCertificate, StaticFileProvider, TrustBundle};
pub use verify::{MAX_CERT_PEM_BYTES, VerifyError, canonical_fingerprint, csr_public_key, verify_site_cert};
