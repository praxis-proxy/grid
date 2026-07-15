// SPDX-License-Identifier: Apache-2.0

//! Controller-owned credential resolution for API-provider authentication.
//!
//! # Architecture
//!
//! The credential path is:
//! 1. `InferenceProvider.spec.auth` declares the strategy and a [`SecretRef`].
//! 2. [`credential_plan_from_auth`] parses the spec into a [`CredentialPlan`] — pure, no I/O.
//! 3. [`CredentialResolver`] implementations fetch the actual secret value.
//! 4. The controller calls [`verify_credential_accessible`] during reconcile to surface missing or misconfigured
//!    Secrets as `Unavailable` phase before routing begins.
//!
//! # v1 backend
//!
//! [`KubernetesSecretResolver`] is the v1 implementation. It reads from Kubernetes
//! `Secret.data` using the kube API client. Future backends — Vault, External Secrets
//! Operator, `OAuth2` token refresh, `SigV4` signing, workload identity — implement
//! the same [`CredentialResolver`] trait without changing callers.
//!
//! # Security
//!
//! [`BearerToken`](crate::resources::credentials::BearerToken) never appears in `Debug`
//! output or `tracing` spans.
//! Token values must not be written to Kubernetes resources (status, annotations,
//! labels, ConfigMaps).  Pass
//! [`BearerToken`](crate::resources::credentials::BearerToken) only to the
//! data-plane config generator that injects it into Praxis filter configuration.
//!
//! [`SecretRef`]: crate::crd::grid_network::SecretRef

use std::{collections::BTreeMap, fmt};

use k8s_openapi::{ByteString, api::core::v1::Secret};
use kube::{Client, api::Api};

use crate::{
    crd::{
        auth::{AuthConfig, AuthStrategy},
        grid_network::SecretRef,
    },
    error::OperatorError,
};

// ---------------------------------------------------------------------------
// CredentialFailureReason
// ---------------------------------------------------------------------------

/// Machine-readable reason for a credential validation failure.
///
/// Surfaced in [`InferenceProvider`] `status.reason` so administrators can
/// diagnose configuration errors without inspecting operator logs.
/// Values are stable across releases and safe for automation to parse.
///
/// Never includes token values or raw Secret data.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialFailureReason {
    /// The declared `auth.strategy` is not yet supported by the controller.
    UnsupportedAuthStrategy,
    /// `auth.strategy = bearer_token` but `secretRef` is absent or has a blank field.
    CredentialSecretRefInvalid,
    /// The referenced Secret does not exist or has no `data` section.
    CredentialSecretMissing,
    /// The expected key is absent from `Secret.data`.
    CredentialSecretKeyMissing,
    /// The value in `Secret.data` for the expected key is not valid UTF-8.
    CredentialSecretValueInvalid,
}

impl CredentialFailureReason {
    /// Machine-readable string used in `InferenceProvider.status.reason`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedAuthStrategy => "UnsupportedAuthStrategy",
            Self::CredentialSecretRefInvalid => "CredentialSecretRefInvalid",
            Self::CredentialSecretMissing => "CredentialSecretMissing",
            Self::CredentialSecretKeyMissing => "CredentialSecretKeyMissing",
            Self::CredentialSecretValueInvalid => "CredentialSecretValueInvalid",
        }
    }
}

// ---------------------------------------------------------------------------
// CredentialPlan
// ---------------------------------------------------------------------------

/// The credential action derived from `InferenceProvider.spec.auth`.
///
/// This is a **pure data type** — no I/O.  Build it with
/// [`credential_plan_from_auth`], then use a [`CredentialResolver`] to
/// fetch the actual value.
#[derive(Debug, PartialEq)]
pub enum CredentialPlan {
    /// `spec.auth` is absent — no credential injection.
    Absent,

    /// `auth.manual = true` — the user manages credentials; the operator does not inject.
    Manual,

    /// `auth.strategy = bearer_token` — resolve a bearer token from the referenced Secret.
    Bearer(BearerTokenRef),
}

/// Reference to a Kubernetes Secret holding a bearer token.
///
/// All fields must be non-empty; validation is enforced by
/// [`credential_plan_from_auth`].
#[derive(Clone, Debug, PartialEq)]
pub struct BearerTokenRef {
    /// Secret name in the cluster.
    pub secret_name: String,
    /// Secret namespace.
    pub namespace: String,
    /// Key within `Secret.data` that holds the base64-encoded token.
    pub key: String,
}

// ---------------------------------------------------------------------------
// BearerToken — opaque, non-logging value type
// ---------------------------------------------------------------------------

/// A resolved bearer token ready for data-plane injection.
///
/// The token value is intentionally hidden from [`fmt::Debug`] to prevent
/// accidental logging.  Pass this value only to the Praxis config generator
/// that writes it into a Praxis filter pipeline at request time.
///
/// **Do not write the token value to Kubernetes resources.**
pub struct BearerToken(String);

impl BearerToken {
    /// Create a new `BearerToken` from a raw string.
    #[must_use]
    pub fn new(raw: String) -> Self {
        Self(raw)
    }

    /// Return the token value.
    ///
    /// Use only when the token is needed for config injection.
    /// Never log, store in status, or serialize to a Kubernetes resource.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BearerToken(<redacted>)")
    }
}

// ---------------------------------------------------------------------------
// CredentialResolver trait
// ---------------------------------------------------------------------------

/// Resolves a [`CredentialPlan`] to a concrete [`BearerToken`].
///
/// # Extension point
///
/// v1 is [`KubernetesSecretResolver`].  Implement this trait to add backends:
/// - Vault / External Secrets Operator
/// - `OAuth2` token refresh
/// - `SigV4` per-request signing (produces a signing context, not a static token)
/// - Kubernetes workload identity (`ServiceAccount` tokens)
///
/// The trait is not object-safe (uses `impl Trait` return) because concrete
/// resolver types are substituted at compile time.  Use generics or boxing at
/// the call site if dynamic dispatch is needed in the future.
pub trait CredentialResolver {
    /// Resolve a bearer token from the referenced backend.
    ///
    /// The token value is returned inside [`BearerToken`], which suppresses
    /// debug output.  Callers must not log the value.
    ///
    /// # Errors
    ///
    /// Returns [`OperatorError`] when the Secret is inaccessible, the key is
    /// absent, or the stored value is not valid UTF-8.
    #[expect(
        async_fn_in_trait,
        reason = "concrete resolver types are substituted at compile time; object safety is not required"
    )]
    async fn resolve(&self, plan: &BearerTokenRef) -> Result<BearerToken, OperatorError>;
}

// ---------------------------------------------------------------------------
// KubernetesSecretResolver — v1 backend
// ---------------------------------------------------------------------------

/// v1 credential resolver: reads bearer tokens from Kubernetes Secrets.
///
/// The resolver fetches `Secret.data[key]`, decodes the base64 value, and
/// returns it as a [`BearerToken`].  The decoded value is never logged.
pub struct KubernetesSecretResolver {
    /// Kube API client used to fetch Secrets.
    client: Client,
}

impl KubernetesSecretResolver {
    /// Create a resolver backed by the given kube client.
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl CredentialResolver for KubernetesSecretResolver {
    async fn resolve(&self, plan: &BearerTokenRef) -> Result<BearerToken, OperatorError> {
        let api: Api<Secret> = Api::namespaced(self.client.clone(), &plan.namespace);
        let Some(secret) = api.get_opt(&plan.secret_name).await? else {
            return Err(OperatorError::NotFound(format!(
                "credential Secret {}/{} not found",
                plan.namespace, plan.secret_name
            )));
        };

        let data = secret.data.as_ref().ok_or_else(|| {
            OperatorError::NotFound(format!(
                "credential Secret {}/{} has no data",
                plan.namespace, plan.secret_name
            ))
        })?;

        let bytes = data.get(&plan.key).ok_or_else(|| {
            OperatorError::NotFound(format!(
                "credential Secret {}/{} missing key {:?}",
                plan.namespace, plan.secret_name, plan.key
            ))
        })?;

        let token = String::from_utf8(bytes.0.clone()).map_err(|e| {
            OperatorError::NotFound(format!(
                "credential Secret {}/{} key {:?} is not valid UTF-8: {e}",
                plan.namespace, plan.secret_name, plan.key
            ))
        })?;

        // Token is in `BearerToken` which suppresses debug output.
        Ok(BearerToken::new(token))
    }
}

// ---------------------------------------------------------------------------
// credential_plan_from_auth — pure function
// ---------------------------------------------------------------------------

/// Build a [`CredentialPlan`] from `InferenceProvider.spec.auth`.
///
/// This function is **pure** — no I/O.  All validation of the parsed plan
/// (e.g. Secret existence) is a controller responsibility; call
/// [`verify_credential_accessible`] in the reconcile loop.
///
/// # Errors
///
/// Returns [`OperatorError::NotFound`] for:
/// - unsupported strategy (anything other than `bearer_token`)
/// - `bearer_token` without a `secretRef`
/// - `secretRef` missing `name`, `namespace`, or `key` fields
pub fn credential_plan_from_auth(auth: Option<&AuthConfig>) -> Result<CredentialPlan, OperatorError> {
    let Some(auth) = auth else {
        return Ok(CredentialPlan::Absent);
    };

    if auth.manual {
        return Ok(CredentialPlan::Manual);
    }

    match auth.strategy {
        AuthStrategy::BearerToken => {
            let secret_ref = auth.secret_ref.as_ref().ok_or_else(|| {
                OperatorError::NotFound("auth.strategy is bearer_token but spec.auth.secretRef is missing".to_owned())
            })?;
            let bearer_ref = bearer_token_ref_from_secret_ref(secret_ref)?;
            Ok(CredentialPlan::Bearer(bearer_ref))
        },

        // Strategies that require per-request computation or token refresh —
        // not yet implemented in the controller.
        AuthStrategy::Sigv4 | AuthStrategy::Oauth2 | AuthStrategy::ServiceAccount => {
            Err(OperatorError::NotFound(format!(
                "auth strategy {:?} is not yet supported by the controller; \
                 implement a CredentialResolver for this backend",
                auth.strategy
            )))
        },

        // Strategies that the data plane handles without a Secret projection.
        AuthStrategy::MtlsOnly => Ok(CredentialPlan::Absent),

        // Strategies the controller does not project (user or API-key injection
        // patterns that are not yet wired).
        AuthStrategy::ApiKey | AuthStrategy::Custom => Err(OperatorError::NotFound(format!(
            "auth strategy {:?} is not yet supported for controller-driven \
                 credential projection; use auth.manual = true to manage manually",
            auth.strategy
        ))),
    }
}

/// Derive the [`CredentialFailureReason`] for a failed [`credential_plan_from_auth`] call.
///
/// Call this only after [`credential_plan_from_auth`] has returned `Err`.
/// Returns the machine-readable reason for surfacing in `status.reason`.
///
/// Pure function — no I/O.
pub fn credential_failure_reason_for_auth(auth: Option<&AuthConfig>) -> CredentialFailureReason {
    let Some(auth) = auth else {
        // credential_plan_from_auth(None) always returns Ok(Absent); reaching
        // here is a caller logic error.  Return a safe default.
        return CredentialFailureReason::CredentialSecretRefInvalid;
    };
    if auth.manual {
        // credential_plan_from_auth(manual=true) always returns Ok(Manual).
        return CredentialFailureReason::CredentialSecretRefInvalid;
    }
    match auth.strategy {
        AuthStrategy::BearerToken => {
            // BearerToken fails only when secretRef is absent or has blank fields.
            CredentialFailureReason::CredentialSecretRefInvalid
        },
        _ => CredentialFailureReason::UnsupportedAuthStrategy,
    }
}

/// Build a [`BearerTokenRef`] from a raw [`SecretRef`].
///
/// Returns an error if any required field is missing or blank.
fn bearer_token_ref_from_secret_ref(secret_ref: &SecretRef) -> Result<BearerTokenRef, OperatorError> {
    if secret_ref.name.trim().is_empty() {
        return Err(OperatorError::NotFound(
            "spec.auth.secretRef.name must not be blank".to_owned(),
        ));
    }
    if secret_ref.namespace.trim().is_empty() {
        return Err(OperatorError::NotFound(
            "spec.auth.secretRef.namespace must not be blank".to_owned(),
        ));
    }
    let key = secret_ref.key.as_deref().unwrap_or("").trim();
    if key.is_empty() {
        return Err(OperatorError::NotFound(
            "spec.auth.secretRef.key must not be blank for bearer_token strategy".to_owned(),
        ));
    }
    Ok(BearerTokenRef {
        secret_name: secret_ref.name.clone(),
        namespace: secret_ref.namespace.clone(),
        key: key.to_owned(),
    })
}

// ---------------------------------------------------------------------------
// verify_credential_accessible
// ---------------------------------------------------------------------------

/// Verify that the credential Secret exists, contains the expected key, and the
/// value is valid UTF-8.
///
/// This function does **not** return the token value.  It is intended for
/// use in the reconcile loop to surface missing or misconfigured Secrets before
/// routing begins.
///
/// # Returns
///
/// - `Ok(None)` — credential is accessible and valid.
/// - `Ok(Some(reason))` — credential-specific failure; the provider should be marked [`Unavailable`] with the returned
///   reason in `status.reason`.
///
/// [`Unavailable`]: crate::crd::inference_provider::ProviderPhase::Unavailable
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures (network, server error,
/// authorization denied).  These are transient; the controller should requeue
/// rather than marking the provider [`Unavailable`].
///
/// `Absent` and `Manual` plans always return `Ok(None)`.
pub async fn verify_credential_accessible(
    client: &Client,
    plan: &CredentialPlan,
) -> Result<Option<CredentialFailureReason>, OperatorError> {
    let CredentialPlan::Bearer(bearer_ref) = plan else {
        // Absent and Manual require no Secret.
        return Ok(None);
    };

    let api: Api<Secret> = Api::namespaced(client.clone(), &bearer_ref.namespace);

    let Some(secret) = api.get_opt(&bearer_ref.secret_name).await? else {
        return Ok(Some(CredentialFailureReason::CredentialSecretMissing));
    };

    Ok(validate_bearer_secret_data(&bearer_ref.key, secret.data.as_ref()).err())
}

/// Validate that a bearer credential Secret's data map is well-formed.
///
/// Pure function — no I/O.  Checks:
/// 1. `data` is `Some` (the Secret has a `data` section).
/// 2. `key` is present in `data`.
/// 3. The value for `key` is valid UTF-8.
///
/// Does not return or log the value.
///
/// # Errors
///
/// Returns:
/// - [`CredentialFailureReason::CredentialSecretMissing`] when `data` is `None`.
/// - [`CredentialFailureReason::CredentialSecretKeyMissing`] when `key` is absent.
/// - [`CredentialFailureReason::CredentialSecretValueInvalid`] when value is not valid UTF-8.
pub(crate) fn validate_bearer_secret_data(
    key: &str,
    data: Option<&BTreeMap<String, ByteString>>,
) -> Result<(), CredentialFailureReason> {
    let data = data.ok_or(CredentialFailureReason::CredentialSecretMissing)?;
    let bytes = data
        .get(key)
        .ok_or(CredentialFailureReason::CredentialSecretKeyMissing)?;
    std::str::from_utf8(&bytes.0).map_err(|_e| CredentialFailureReason::CredentialSecretValueInvalid)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "test module suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use std::collections::BTreeMap;

    use k8s_openapi::ByteString;

    use super::*;
    use crate::crd::{auth::AuthConfig, grid_network::SecretRef};

    fn bearer_auth(name: &str, ns: &str, key: &str) -> AuthConfig {
        AuthConfig {
            manual: false,
            strategy: AuthStrategy::BearerToken,
            secret_ref: Some(SecretRef {
                name: name.to_owned(),
                namespace: ns.to_owned(),
                key: Some(key.to_owned()),
            }),
        }
    }

    // -----------------------------------------------------------------------
    // Absent
    // -----------------------------------------------------------------------

    #[test]
    fn absent_when_auth_is_none() {
        let plan = credential_plan_from_auth(None).unwrap();
        assert_eq!(plan, CredentialPlan::Absent);
    }

    // -----------------------------------------------------------------------
    // Manual
    // -----------------------------------------------------------------------

    #[test]
    fn manual_when_manual_flag_is_true() {
        let auth = AuthConfig {
            manual: true,
            strategy: AuthStrategy::BearerToken,
            secret_ref: None,
        };
        let plan = credential_plan_from_auth(Some(&auth)).unwrap();
        assert_eq!(
            plan,
            CredentialPlan::Manual,
            "manual=true must suppress injection regardless of strategy"
        );
    }

    #[test]
    fn manual_beats_strategy_even_with_secret_ref() {
        let auth = AuthConfig {
            manual: true,
            strategy: AuthStrategy::BearerToken,
            secret_ref: Some(SecretRef {
                name: "s".to_owned(),
                namespace: "default".to_owned(),
                key: Some("token".to_owned()),
            }),
        };
        let plan = credential_plan_from_auth(Some(&auth)).unwrap();
        assert_eq!(plan, CredentialPlan::Manual);
    }

    // -----------------------------------------------------------------------
    // Bearer — valid
    // -----------------------------------------------------------------------

    #[test]
    fn valid_bearer_token_plan() {
        let auth = bearer_auth("api-creds", "default", "token");
        let plan = credential_plan_from_auth(Some(&auth)).unwrap();
        assert_eq!(
            plan,
            CredentialPlan::Bearer(BearerTokenRef {
                secret_name: "api-creds".to_owned(),
                namespace: "default".to_owned(),
                key: "token".to_owned(),
            })
        );
    }

    // -----------------------------------------------------------------------
    // Bearer — missing/invalid SecretRef
    // -----------------------------------------------------------------------

    #[test]
    fn bearer_without_secret_ref_is_error() {
        let auth = AuthConfig {
            manual: false,
            strategy: AuthStrategy::BearerToken,
            secret_ref: None,
        };
        let err = credential_plan_from_auth(Some(&auth)).unwrap_err();
        assert!(
            err.to_string().contains("secretRef is missing"),
            "expected secretRef missing error, got: {err}"
        );
    }

    #[test]
    fn bearer_with_blank_secret_name_is_error() {
        let auth = AuthConfig {
            manual: false,
            strategy: AuthStrategy::BearerToken,
            secret_ref: Some(SecretRef {
                name: "  ".to_owned(),
                namespace: "default".to_owned(),
                key: Some("token".to_owned()),
            }),
        };
        let err = credential_plan_from_auth(Some(&auth)).unwrap_err();
        assert!(err.to_string().contains("name must not be blank"), "{err}");
    }

    #[test]
    fn bearer_with_blank_namespace_is_error() {
        let auth = AuthConfig {
            manual: false,
            strategy: AuthStrategy::BearerToken,
            secret_ref: Some(SecretRef {
                name: "my-secret".to_owned(),
                namespace: String::new(),
                key: Some("token".to_owned()),
            }),
        };
        let err = credential_plan_from_auth(Some(&auth)).unwrap_err();
        assert!(err.to_string().contains("namespace must not be blank"), "{err}");
    }

    #[test]
    fn bearer_with_missing_key_is_error() {
        let auth = AuthConfig {
            manual: false,
            strategy: AuthStrategy::BearerToken,
            secret_ref: Some(SecretRef {
                name: "my-secret".to_owned(),
                namespace: "default".to_owned(),
                key: None,
            }),
        };
        let err = credential_plan_from_auth(Some(&auth)).unwrap_err();
        assert!(err.to_string().contains("key must not be blank"), "{err}");
    }

    #[test]
    fn bearer_with_blank_key_is_error() {
        let auth = AuthConfig {
            manual: false,
            strategy: AuthStrategy::BearerToken,
            secret_ref: Some(SecretRef {
                name: "my-secret".to_owned(),
                namespace: "default".to_owned(),
                key: Some("  ".to_owned()),
            }),
        };
        let err = credential_plan_from_auth(Some(&auth)).unwrap_err();
        assert!(err.to_string().contains("key must not be blank"), "{err}");
    }

    // -----------------------------------------------------------------------
    // Unsupported strategies
    // -----------------------------------------------------------------------

    #[test]
    fn sigv4_strategy_is_unsupported() {
        let auth = AuthConfig {
            manual: false,
            strategy: AuthStrategy::Sigv4,
            secret_ref: None,
        };
        let err = credential_plan_from_auth(Some(&auth)).unwrap_err();
        assert!(err.to_string().contains("not yet supported"), "{err}");
    }

    #[test]
    fn oauth2_strategy_is_unsupported() {
        let auth = AuthConfig {
            manual: false,
            strategy: AuthStrategy::Oauth2,
            secret_ref: None,
        };
        let err = credential_plan_from_auth(Some(&auth)).unwrap_err();
        assert!(err.to_string().contains("not yet supported"), "{err}");
    }

    #[test]
    fn api_key_strategy_is_unsupported_for_controller_projection() {
        let auth = AuthConfig {
            manual: false,
            strategy: AuthStrategy::ApiKey,
            secret_ref: None,
        };
        let err = credential_plan_from_auth(Some(&auth)).unwrap_err();
        assert!(err.to_string().contains("not yet supported"), "{err}");
    }

    // -----------------------------------------------------------------------
    // MtlsOnly — special: no Secret needed
    // -----------------------------------------------------------------------

    #[test]
    fn mtls_only_strategy_returns_absent() {
        let auth = AuthConfig {
            manual: false,
            strategy: AuthStrategy::MtlsOnly,
            secret_ref: None,
        };
        let plan = credential_plan_from_auth(Some(&auth)).unwrap();
        assert_eq!(plan, CredentialPlan::Absent, "mtls_only requires no Secret projection");
    }

    // -----------------------------------------------------------------------
    // BearerToken Debug redaction
    // -----------------------------------------------------------------------

    #[test]
    fn bearer_token_debug_does_not_expose_value() {
        let token = BearerToken::new("super-secret-value".to_owned());
        let debug = format!("{token:?}");
        assert!(
            !debug.contains("super-secret"),
            "token value must not appear in Debug: {debug}"
        );
        assert!(debug.contains("redacted"), "debug output should say redacted: {debug}");
    }

    #[test]
    fn bearer_token_expose_secret_returns_value() {
        let token = BearerToken::new("my-token".to_owned());
        assert_eq!(token.expose_secret(), "my-token");
    }

    // -----------------------------------------------------------------------
    // validate_bearer_secret_data — pure UTF-8 + structure validation
    // -----------------------------------------------------------------------

    fn make_data(key: &str, value: &[u8]) -> BTreeMap<String, ByteString> {
        let mut m = BTreeMap::new();
        m.insert(key.to_owned(), ByteString(value.to_vec()));
        m
    }

    #[test]
    fn secret_has_no_data_returns_missing() {
        let err = validate_bearer_secret_data("token", None).unwrap_err();
        assert_eq!(err, CredentialFailureReason::CredentialSecretMissing, "{err:?}");
    }

    #[test]
    fn secret_key_absent_returns_key_missing() {
        let data = make_data("other-key", b"value");
        let err = validate_bearer_secret_data("token", Some(&data)).unwrap_err();
        assert_eq!(err, CredentialFailureReason::CredentialSecretKeyMissing, "{err:?}");
    }

    #[test]
    fn secret_key_invalid_utf8_returns_value_invalid() {
        let data = make_data("token", &[0xFF, 0xFE]); // invalid UTF-8
        let err = validate_bearer_secret_data("token", Some(&data)).unwrap_err();
        assert_eq!(
            err,
            CredentialFailureReason::CredentialSecretValueInvalid,
            "invalid UTF-8 bytes must return CredentialSecretValueInvalid"
        );
    }

    #[test]
    fn valid_secret_key_passes() {
        let data = make_data("token", b"sk-live-abc123");
        validate_bearer_secret_data("token", Some(&data)).expect("valid UTF-8 bearer token must pass validation");
    }

    // -----------------------------------------------------------------------
    // CredentialFailureReason — reason codes
    // -----------------------------------------------------------------------

    #[test]
    fn credential_failure_reason_as_str_stable_values() {
        assert_eq!(
            CredentialFailureReason::UnsupportedAuthStrategy.as_str(),
            "UnsupportedAuthStrategy"
        );
        assert_eq!(
            CredentialFailureReason::CredentialSecretRefInvalid.as_str(),
            "CredentialSecretRefInvalid"
        );
        assert_eq!(
            CredentialFailureReason::CredentialSecretMissing.as_str(),
            "CredentialSecretMissing"
        );
        assert_eq!(
            CredentialFailureReason::CredentialSecretKeyMissing.as_str(),
            "CredentialSecretKeyMissing"
        );
        assert_eq!(
            CredentialFailureReason::CredentialSecretValueInvalid.as_str(),
            "CredentialSecretValueInvalid"
        );
    }

    #[test]
    fn credential_failure_reason_for_auth_unsupported_strategy() {
        let auth = AuthConfig {
            manual: false,
            strategy: AuthStrategy::Sigv4,
            secret_ref: None,
        };
        let r = credential_failure_reason_for_auth(Some(&auth));
        assert_eq!(r, CredentialFailureReason::UnsupportedAuthStrategy);
    }

    #[test]
    fn credential_failure_reason_for_auth_bearer_with_bad_ref() {
        let auth = AuthConfig {
            manual: false,
            strategy: AuthStrategy::BearerToken,
            secret_ref: None,
        };
        let r = credential_failure_reason_for_auth(Some(&auth));
        assert_eq!(r, CredentialFailureReason::CredentialSecretRefInvalid);
    }
}
