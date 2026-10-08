//! Credential locator types carried on a route candidate.
//!
//! Only locator fields travel in Grid config, never secret bytes. This
//! increment carries the type. It does not resolve or inject the credential.

use serde::Deserialize;

/// Bearer token injection strategy identifier.
pub(crate) const STRATEGY_BEARER_TOKEN: &str = "bearer_token";

/// Kubernetes Secret reference. Only locator fields are carried in Grid
/// configuration and request metadata.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CredentialRef {
    /// Secret data key.
    pub key: String,
    /// Secret name.
    pub name: String,
    /// Secret namespace.
    pub namespace: String,
}

/// Provider-local reference to credential bytes resolved downstream.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CandidateCredential {
    /// Injection strategy.
    pub strategy: String,

    /// Secret locator, never secret bytes.
    pub secret_ref: CredentialRef,
}
