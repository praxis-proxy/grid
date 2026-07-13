//! Certificate management for AI Grid site-to-site mTLS.
//!
//! Provides a [`CertificateProvider`] trait that abstracts
//! certificate sourcing. The POC uses [`StaticFileProvider`]
//! (certs from disk); production will use a `SpiffeProvider`
//! (SPIRE workload API) without changing the mTLS plumbing.

mod generate;
mod provider;

pub use generate::{
    CaCert, DEFAULT_ORGANIZATION, GenerateError, SiteCertOutput, generate_ca, generate_cert_with_org,
    generate_site_cert, load_ca,
};
pub use provider::{CertificateProvider, ProviderError, SiteCertificate, StaticFileProvider, TrustBundle};
