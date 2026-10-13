//! Authentication strategy types shared across provider CRDs.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::grid_network::SecretRef;

// ---------------------------------------------------------------------------
// Auth Config
// ---------------------------------------------------------------------------

/// Authentication configuration for consuming a provider.
///
/// Declares how consumers authenticate to this provider.
/// The Grid Operator manages credential lifecycle and
/// configures Praxis to inject them transparently.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthConfig {
    /// Whether the user manages credentials manually.
    ///
    /// When true, the operator does not inject credentials
    /// and the user is responsible for configuring auth.
    #[serde(default)]
    pub manual: bool,

    /// Reference to a Secret containing the credential.
    pub secret_ref: Option<SecretRef>,

    /// How credentials are presented to the provider.
    pub strategy: AuthStrategy,
}

/// How credentials are presented to a provider.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthStrategy {
    /// API key in a custom header (e.g. `x-api-key`).
    ApiKey,

    /// Bearer token in the Authorization header.
    BearerToken,

    /// User-configured header and value.
    Custom,

    /// Grid mTLS certificate identity (no extra header).
    MtlsOnly,

    /// `OAuth2` token with automatic refresh.
    Oauth2,

    /// Kubernetes `ServiceAccount` token.
    ServiceAccount,

    /// AWS `SigV4` per-request signing.
    Sigv4,
}

// ---------------------------------------------------------------------------
// Access Policy
// ---------------------------------------------------------------------------

/// Access policy for a provider.
///
/// Controls which sites (and optionally workloads) can
/// consume this provider. Empty selectors mean all allowed.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessPolicy {
    /// Which sites can route to this provider.
    #[serde(default)]
    pub site_selector: SelectorConfig,
}

/// Label selector for access policies.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectorConfig {
    /// Label key-value pairs that must match.
    #[serde(default)]
    pub match_labels: std::collections::BTreeMap<String, String>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_strategy_serde() {
        let json = serde_json::to_string(&AuthStrategy::BearerToken).unwrap_or_else(|_| std::process::abort());
        assert_eq!(json, "\"bearer_token\"", "snake_case serialization");
    }

    #[test]
    fn access_policy_default_allows_all() {
        let policy = AccessPolicy::default();
        assert!(policy.site_selector.match_labels.is_empty(), "default should allow all");
    }

    #[test]
    fn auth_config_manual_default_false() {
        let json = serde_json::json!({
            "strategy": "bearer_token"
        });
        let cfg: AuthConfig = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert!(!cfg.manual, "manual should default false");
    }
}
