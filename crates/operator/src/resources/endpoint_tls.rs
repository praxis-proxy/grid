//! Shared TLS material resolution and validation for endpoint probes.
//!
//! Provides reusable functions for reading Kubernetes Secrets containing
//! CA certificates and client identity material, building a
//! [`rustls::ClientConfig`], and validating that referenced Secrets are
//! accessible before a live probe runs.
//!
//! Used by metrics scraping (`provider_metrics`), health check probing
//! (`controller::inference_provider`), and model discovery (`served_models`).

use std::sync::Arc;

use crate::{
    crd::inference_provider::{CaSource, ClientCertificateSecretRef, EndpointTlsConfig},
    metrics_scraper,
    resources::{secret::SecretKeyLookup, tls_backend::ClientTlsConfig},
};

// ---------------------------------------------------------------------------
// TLS failure reason
// ---------------------------------------------------------------------------

/// Machine-readable reason for a TLS configuration failure.
///
/// Surfaced in [`InferenceProvider`] `status.reason` so administrators can
/// diagnose material and configuration errors without inspecting operator
/// logs.  Values are stable across releases and safe for automation to parse.
///
/// Only material/configuration failures that the controller can observe
/// during reconciliation appear here.  Runtime failures (TLS handshake,
/// HTTP 401/403, timeout) are surfaced as structured log fields only —
/// they cannot be reproduced deterministically and should not appear in
/// status.
///
/// Never includes raw certificate or key content.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TlsFailureReason {
    /// A referenced Secret does not exist or has no `data` section.
    SecretMissing,
    /// The expected key is absent from `Secret.data` or its value is empty.
    KeyMissing,
    /// PEM material could not be parsed or contains no certificates/keys.
    MaterialInvalid,
    /// The client certificate and private key do not form a valid identity.
    IdentityMismatch,
}

impl TlsFailureReason {
    /// Build a prefixed machine-readable status reason string.
    ///
    /// `prefix` is typically `"Metrics"` or `"HealthCheck"`, producing
    /// values such as `"MetricsTlsSecretMissing"` or
    /// `"HealthCheckTlsSecretMissing"`.
    #[must_use]
    pub(crate) fn as_status_reason(self, prefix: &str) -> String {
        match self {
            Self::SecretMissing => format!("{prefix}TlsSecretMissing"),
            Self::KeyMissing => format!("{prefix}TlsKeyMissing"),
            Self::MaterialInvalid => format!("{prefix}TlsMaterialInvalid"),
            Self::IdentityMismatch => format!("{prefix}TlsIdentityMismatch"),
        }
    }

    /// Classify a [`MetricsScrapeError`](metrics_scraper::MetricsScrapeError)
    /// from [`build_tls_client_config`](metrics_scraper::build_tls_client_config)
    /// into the appropriate failure reason.
    ///
    /// The `"identity construction failed"` message maps to
    /// [`IdentityMismatch`](Self::IdentityMismatch); all other errors map to
    /// [`MaterialInvalid`](Self::MaterialInvalid).
    #[must_use]
    pub(crate) fn from_build_tls_error(e: &metrics_scraper::MetricsScrapeError) -> Self {
        let msg = e.to_string();
        if msg.contains("identity construction failed") {
            Self::IdentityMismatch
        } else {
            Self::MaterialInvalid
        }
    }
}

// ---------------------------------------------------------------------------
// Secret read helpers
// ---------------------------------------------------------------------------

/// Build a [`SecretRef`](crate::crd::grid_network::SecretRef) from a
/// [`ClientCertificateSecretRef`] for Secret reads.
pub(crate) fn secret_ref_from_client_cert(
    client_ref: &ClientCertificateSecretRef,
) -> crate::crd::grid_network::SecretRef {
    crate::crd::grid_network::SecretRef {
        name: client_ref.name.clone(),
        namespace: client_ref.namespace.clone(),
        key: None,
    }
}

/// Read raw bytes from a Kubernetes Secret for TLS material resolution.
///
/// Returns a structured `(TlsFailureReason, String)` error so callers can
/// derive a machine-readable `status.reason` without parsing error text.
pub(crate) async fn read_secret_bytes_for_tls(
    client: &kube::Client,
    secret_ref: &crate::crd::grid_network::SecretRef,
    key_name: &str,
    provider_identity: &str,
    material_desc: &str,
) -> Result<Vec<u8>, (TlsFailureReason, String)> {
    use crate::resources::secret::read_secret_bytes;

    match read_secret_bytes(client, secret_ref, key_name).await {
        Ok(SecretKeyLookup::Found(bytes)) => Ok(bytes),
        Ok(SecretKeyLookup::KeyMissing) => Err((
            TlsFailureReason::KeyMissing,
            format!(
                "{material_desc} key {key_name:?} in Secret {}/{} is absent or empty for provider {provider_identity}",
                secret_ref.namespace, secret_ref.name
            ),
        )),
        Ok(SecretKeyLookup::SecretMissing) => Err((
            TlsFailureReason::SecretMissing,
            format!(
                "{material_desc} Secret {}/{} not found for provider {provider_identity}",
                secret_ref.namespace, secret_ref.name
            ),
        )),
        Err(e) => Err((
            TlsFailureReason::MaterialInvalid,
            format!(
                "{material_desc} Secret {}/{} read failed for provider {provider_identity}: {e}",
                secret_ref.namespace, secret_ref.name
            ),
        )),
    }
}

/// Read `tls`'s CA PEM from its one configured source, Secret or `ConfigMap`.
///
/// # Errors
///
/// Returns the failure reason and a message when neither or both sources are
/// set, or the source cannot be read.
pub(crate) async fn read_ca_for_tls(
    client: &kube::Client,
    tls: &EndpointTlsConfig,
    provider_identity: &str,
) -> Result<Vec<u8>, (TlsFailureReason, String)> {
    match tls.ca_source() {
        Some(CaSource::Secret(secret)) => {
            let key = secret.key.as_deref().unwrap_or("ca.crt");
            read_secret_bytes_for_tls(client, secret, key, provider_identity, "CA").await
        },
        Some(CaSource::ConfigMap(config_map)) => read_config_map_ca(client, config_map, provider_identity).await,
        None => Err((
            TlsFailureReason::MaterialInvalid,
            format!("set exactly one of caSecretRef and caConfigMapRef for provider {provider_identity}"),
        )),
    }
}

/// Read a CA PEM from `config_map`, mapping a failure to its TLS reason.
async fn read_config_map_ca(
    client: &kube::Client,
    config_map: &crate::crd::inference_provider::ConfigMapKeyRef,
    provider_identity: &str,
) -> Result<Vec<u8>, (TlsFailureReason, String)> {
    let key = config_map.key.as_deref().unwrap_or("ca.crt");
    let where_ = format!("ConfigMap {}/{}", config_map.namespace, config_map.name);
    match crate::resources::secret::read_config_map_bytes(client, config_map, key).await {
        Ok(SecretKeyLookup::Found(bytes)) => Ok(bytes),
        Ok(SecretKeyLookup::KeyMissing) => Err((
            TlsFailureReason::KeyMissing,
            format!("CA key {key:?} in {where_} is absent or empty for provider {provider_identity}"),
        )),
        Ok(SecretKeyLookup::SecretMissing) => Err((
            TlsFailureReason::SecretMissing,
            format!("CA {where_} not found for provider {provider_identity}"),
        )),
        Err(e) => Err((
            TlsFailureReason::MaterialInvalid,
            format!("CA {where_} read failed for provider {provider_identity}: {e}"),
        )),
    }
}

/// [`read_ca_for_tls`] for validation: an API error is returned, a missing CA is a reason.
async fn read_ca_for_verify(
    client: &kube::Client,
    tls: &EndpointTlsConfig,
) -> Result<Result<Vec<u8>, TlsFailureReason>, crate::error::OperatorError> {
    let lookup = match tls.ca_source() {
        Some(CaSource::Secret(secret)) => {
            return read_tls_secret_for_verify(client, secret, secret.key.as_deref().unwrap_or("ca.crt")).await;
        },
        Some(CaSource::ConfigMap(config_map)) => {
            let key = config_map.key.as_deref().unwrap_or("ca.crt");
            crate::resources::secret::read_config_map_bytes(client, config_map, key).await?
        },
        None => return Ok(Err(TlsFailureReason::MaterialInvalid)),
    };
    Ok(match lookup {
        SecretKeyLookup::Found(bytes) => Ok(bytes),
        SecretKeyLookup::SecretMissing => Err(TlsFailureReason::SecretMissing),
        SecretKeyLookup::KeyMissing => Err(TlsFailureReason::KeyMissing),
    })
}

// ---------------------------------------------------------------------------
// TLS config resolution
// ---------------------------------------------------------------------------

/// Resolve a [`rustls::ClientConfig`] from an endpoint TLS configuration.
///
/// When `tls_config` is `None`, returns `Ok(None)` (use native roots).
/// When `tls_config` is `Some`, reads the referenced Kubernetes Secrets,
/// parses the PEM material, and builds a `ClientConfig`.
///
/// # Fail-closed
///
/// Any resolution failure returns `Err` — the caller must NOT fall back to
/// native root certificates.  The probe/scrape is skipped entirely.
///
/// # Security invariant
///
/// Private key bytes are passed to [`metrics_scraper::build_tls_client_config`]
/// and are never written to logs, events, status fields, or Prometheus labels.
#[expect(
    clippy::large_stack_frames,
    clippy::too_many_lines,
    reason = "async future with kube API types and PEM buffers; sequential Secret reads for CA, client cert, and client key"
)]
pub(crate) async fn resolve_tls_config(
    tls_config: Option<&EndpointTlsConfig>,
    client: Option<&kube::Client>,
    provider_identity: &str,
) -> Result<Option<ClientTlsConfig>, (TlsFailureReason, String)> {
    let Some(tls) = tls_config else {
        return Ok(None);
    };
    let Some(kube_client) = client else {
        return Err((
            TlsFailureReason::MaterialInvalid,
            "TLS configured but no Kubernetes client available".to_owned(),
        ));
    };

    let ca_pem = read_ca_for_tls(kube_client, tls, provider_identity).await?;

    let (client_cert_pem, client_key_pem) = if let Some(client_ref) = &tls.client_certificate_secret_ref {
        let cert_ref = secret_ref_from_client_cert(client_ref);
        let cert = read_secret_bytes_for_tls(
            kube_client,
            &cert_ref,
            &client_ref.certificate_key,
            provider_identity,
            "client cert",
        )
        .await?;
        let key = read_secret_bytes_for_tls(
            kube_client,
            &cert_ref,
            &client_ref.private_key_key,
            provider_identity,
            "client key",
        )
        .await?;
        (Some(cert), Some(key))
    } else {
        (None, None)
    };

    let config =
        metrics_scraper::build_tls_client_config(&ca_pem, client_cert_pem.as_deref(), client_key_pem.as_deref())
            .map_err(|e| {
                let reason = TlsFailureReason::from_build_tls_error(&e);
                (reason, e.to_string())
            })?;

    Ok(Some(Arc::new(config)))
}

// ---------------------------------------------------------------------------
// TLS validation
// ---------------------------------------------------------------------------

/// Verify that TLS Secrets exist, contain the expected keys, and the PEM
/// material can be assembled into a valid [`rustls::ClientConfig`].
///
/// This runs during reconcile to surface configuration errors early.
/// The same material is resolved again at probe/scrape time; this check
/// catches misconfigurations before the first attempt.
///
/// # Returns
///
/// - `Ok(None)` — TLS material is accessible and valid (or no TLS configured).
/// - `Ok(Some(reason))` — failure; the provider should be marked [`Degraded`] with the returned reason in
///   `status.reason`.
///
/// [`Degraded`]: crate::crd::inference_provider::ProviderPhase::Degraded
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures (network, server error,
/// authorization denied).  These are transient; the controller should requeue.
///
/// [`OperatorError`]: crate::error::OperatorError
#[expect(
    clippy::large_stack_frames,
    reason = "sequential Secret reads for CA, client cert, and client key with match arms"
)]
pub(crate) async fn verify_tls_accessible(
    client: &kube::Client,
    tls_config: Option<&EndpointTlsConfig>,
) -> Result<Option<TlsFailureReason>, crate::error::OperatorError> {
    let Some(tls) = tls_config else {
        return Ok(None);
    };

    let ca_pem = match read_ca_for_verify(client, tls).await? {
        Ok(bytes) => bytes,
        Err(reason) => return Ok(Some(reason)),
    };

    let (client_cert_pem, client_key_pem) = if let Some(client_ref) = &tls.client_certificate_secret_ref {
        let sref = secret_ref_from_client_cert(client_ref);
        let cert = match read_tls_secret_for_verify(client, &sref, &client_ref.certificate_key).await? {
            Ok(bytes) => bytes,
            Err(reason) => return Ok(Some(reason)),
        };
        let key = match read_tls_secret_for_verify(client, &sref, &client_ref.private_key_key).await? {
            Ok(bytes) => bytes,
            Err(reason) => return Ok(Some(reason)),
        };
        (Some(cert), Some(key))
    } else {
        (None, None)
    };

    match metrics_scraper::build_tls_client_config(&ca_pem, client_cert_pem.as_deref(), client_key_pem.as_deref()) {
        Ok(_) => Ok(None),
        Err(e) => Ok(Some(TlsFailureReason::from_build_tls_error(&e))),
    }
}

// ---------------------------------------------------------------------------
// Secret helpers (private)
// ---------------------------------------------------------------------------

/// Read raw bytes from a Kubernetes Secret for TLS validation.
///
/// Thin wrapper over [`read_secret_bytes`](crate::resources::secret::read_secret_bytes)
/// that maps its
/// [`SecretKeyLookup`] result
/// onto the [`TlsFailureReason`] this module's callers expect, so "Secret
/// not found" and "key not found" map to the correct variant.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures.
///
/// [`OperatorError`]: crate::error::OperatorError
async fn read_tls_secret_for_verify(
    client: &kube::Client,
    secret_ref: &crate::crd::grid_network::SecretRef,
    key_name: &str,
) -> Result<Result<Vec<u8>, TlsFailureReason>, crate::error::OperatorError> {
    use crate::resources::secret::read_secret_bytes;

    Ok(match read_secret_bytes(client, secret_ref, key_name).await? {
        SecretKeyLookup::Found(bytes) => Ok(bytes),
        SecretKeyLookup::SecretMissing => Err(TlsFailureReason::SecretMissing),
        SecretKeyLookup::KeyMissing => Err(TlsFailureReason::KeyMissing),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::resources::test_doubles::{
        config_map_with_key, mock_kube_client_with_config_maps, mock_kube_client_with_secrets, secret_with_key,
    };

    fn test_tls_config(ca_secret_name: &str) -> EndpointTlsConfig {
        EndpointTlsConfig {
            ca_secret_ref: Some(crate::crd::grid_network::SecretRef {
                name: ca_secret_name.to_owned(),
                namespace: "default".to_owned(),
                key: None,
            }),
            ca_config_map_ref: None,
            client_certificate_secret_ref: None,
        }
    }

    // -----------------------------------------------------------------------
    // secret_ref_from_client_cert — field mapping
    // -----------------------------------------------------------------------

    #[test]
    fn secret_ref_from_client_cert_maps_fields() {
        let client_ref = ClientCertificateSecretRef {
            name: "my-cert".to_owned(),
            namespace: "my-ns".to_owned(),
            certificate_key: "tls.crt".to_owned(),
            private_key_key: "tls.key".to_owned(),
        };
        let sref = secret_ref_from_client_cert(&client_ref);
        assert_eq!(sref.name, "my-cert", "name must be copied");
        assert_eq!(sref.namespace, "my-ns", "namespace must be copied");
        assert!(sref.key.is_none(), "key must be None (not used for Secret lookup)");
    }

    // -----------------------------------------------------------------------
    // TlsFailureReason::as_status_reason — stable prefixed strings
    // -----------------------------------------------------------------------

    #[test]
    fn as_status_reason_all_variants_metrics_prefix() {
        assert_eq!(
            TlsFailureReason::SecretMissing.as_status_reason("Metrics"),
            "MetricsTlsSecretMissing"
        );
        assert_eq!(
            TlsFailureReason::KeyMissing.as_status_reason("Metrics"),
            "MetricsTlsKeyMissing"
        );
        assert_eq!(
            TlsFailureReason::MaterialInvalid.as_status_reason("Metrics"),
            "MetricsTlsMaterialInvalid"
        );
        assert_eq!(
            TlsFailureReason::IdentityMismatch.as_status_reason("Metrics"),
            "MetricsTlsIdentityMismatch"
        );
    }

    #[test]
    fn as_status_reason_all_variants_health_check_prefix() {
        assert_eq!(
            TlsFailureReason::SecretMissing.as_status_reason("HealthCheck"),
            "HealthCheckTlsSecretMissing"
        );
        assert_eq!(
            TlsFailureReason::KeyMissing.as_status_reason("HealthCheck"),
            "HealthCheckTlsKeyMissing"
        );
        assert_eq!(
            TlsFailureReason::MaterialInvalid.as_status_reason("HealthCheck"),
            "HealthCheckTlsMaterialInvalid"
        );
        assert_eq!(
            TlsFailureReason::IdentityMismatch.as_status_reason("HealthCheck"),
            "HealthCheckTlsIdentityMismatch"
        );
    }

    // -----------------------------------------------------------------------
    // TlsFailureReason::from_build_tls_error — error classification
    // -----------------------------------------------------------------------

    #[test]
    fn from_build_tls_error_identity_mismatch() {
        let err = metrics_scraper::MetricsScrapeError::TlsMaterial(
            "client identity construction failed: key mismatch".to_owned(),
        );
        assert_eq!(
            TlsFailureReason::from_build_tls_error(&err),
            TlsFailureReason::IdentityMismatch,
            "\"identity construction failed\" must map to IdentityMismatch"
        );
    }

    #[test]
    fn from_build_tls_error_material_invalid_ca_parse() {
        let err = metrics_scraper::MetricsScrapeError::TlsMaterial("CA PEM parse failed: invalid base64".to_owned());
        assert_eq!(
            TlsFailureReason::from_build_tls_error(&err),
            TlsFailureReason::MaterialInvalid,
            "CA parse errors must map to MaterialInvalid"
        );
    }

    #[test]
    fn from_build_tls_error_material_invalid_generic() {
        let err = metrics_scraper::MetricsScrapeError::TlsMaterial(
            "client cert and key must both be present or both absent".to_owned(),
        );
        assert_eq!(
            TlsFailureReason::from_build_tls_error(&err),
            TlsFailureReason::MaterialInvalid,
            "unrecognised TLS build errors must fall back to MaterialInvalid"
        );
    }

    // -----------------------------------------------------------------------
    // resolve_tls_config — None input
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn resolve_tls_config_none_returns_ok_none() {
        let result = resolve_tls_config(None, None, "test-provider").await;
        assert!(matches!(result, Ok(None)), "None tls_config must return Ok(None)");
    }

    #[tokio::test]
    async fn resolve_tls_config_some_without_client_returns_err() {
        let tls = EndpointTlsConfig {
            ca_secret_ref: Some(crate::crd::grid_network::SecretRef {
                name: "ca".to_owned(),
                namespace: "ns".to_owned(),
                key: None,
            }),
            ca_config_map_ref: None,
            client_certificate_secret_ref: None,
        };
        let result = resolve_tls_config(Some(&tls), None, "test-provider").await;
        assert!(result.is_err(), "TLS configured without a kube client must return Err");
        let (reason, _msg) = result.unwrap_err();
        assert_eq!(
            reason,
            TlsFailureReason::MaterialInvalid,
            "TLS configured without a kube client must return MaterialInvalid"
        );
    }

    // -----------------------------------------------------------------------
    // resolve_tls_config / verify_tls_accessible — SecretMissing vs
    // KeyMissing (grid#58), against a mocked Kubernetes API
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn resolve_tls_config_ca_secret_absent_yields_secret_missing() {
        let client = mock_kube_client_with_secrets(HashMap::new());
        let tls = test_tls_config("absent");
        let (reason, _msg) = resolve_tls_config(Some(&tls), Some(&client), "test-provider")
            .await
            .unwrap_err();
        assert_eq!(reason, TlsFailureReason::SecretMissing);
    }

    #[tokio::test]
    async fn resolve_tls_config_ca_key_absent_from_existing_secret_yields_key_missing() {
        let client =
            mock_kube_client_with_secrets(HashMap::from([("ca-secret", secret_with_key("wrong-key", b"bytes"))]));
        let tls = test_tls_config("ca-secret");
        let (reason, _msg) = resolve_tls_config(Some(&tls), Some(&client), "test-provider")
            .await
            .unwrap_err();
        assert_eq!(
            reason,
            TlsFailureReason::KeyMissing,
            "grid#58: a key absent from an existing Secret's data must be KeyMissing, not SecretMissing"
        );
    }

    #[tokio::test]
    async fn verify_tls_accessible_ca_secret_absent_returns_secret_missing() {
        let client = mock_kube_client_with_secrets(HashMap::new());
        let tls = test_tls_config("absent");
        let result = verify_tls_accessible(&client, Some(&tls)).await.expect("no API error");
        assert_eq!(result, Some(TlsFailureReason::SecretMissing));
    }

    #[tokio::test]
    async fn verify_tls_accessible_ca_key_absent_from_existing_secret_returns_key_missing() {
        let client =
            mock_kube_client_with_secrets(HashMap::from([("ca-secret", secret_with_key("wrong-key", b"bytes"))]));
        let tls = test_tls_config("ca-secret");
        let result = verify_tls_accessible(&client, Some(&tls)).await.expect("no API error");
        assert_eq!(
            result,
            Some(TlsFailureReason::KeyMissing),
            "grid#58: a key absent from an existing Secret's data must be KeyMissing, not SecretMissing"
        );
    }

    #[tokio::test]
    async fn verify_tls_accessible_no_tls_config_returns_none() {
        let client = mock_kube_client_with_secrets(HashMap::new());
        let result = verify_tls_accessible(&client, None).await.expect("no API error");
        assert!(result.is_none(), "no TLS configured must skip validation");
    }

    fn service_ca_config(name: &str) -> EndpointTlsConfig {
        EndpointTlsConfig {
            ca_secret_ref: None,
            ca_config_map_ref: Some(crate::crd::inference_provider::ConfigMapKeyRef {
                name: name.to_owned(),
                namespace: "grid".to_owned(),
                key: Some("service-ca.crt".to_owned()),
            }),
            client_certificate_secret_ref: None,
        }
    }

    #[tokio::test]
    async fn a_service_ca_config_map_is_the_trusted_ca() {
        let ca = certs::generate_ca("service-serving-signer").unwrap();
        let client = mock_kube_client_with_config_maps(HashMap::from([(
            "openshift-service-ca.crt",
            config_map_with_key("service-ca.crt", &ca.cert_pem),
        )]));
        let resolved =
            resolve_tls_config(Some(&service_ca_config("openshift-service-ca.crt")), Some(&client), "p").await;
        assert!(
            matches!(resolved, Ok(Some(_))),
            "the ConfigMap CA builds a client config"
        );
    }

    #[tokio::test]
    async fn a_missing_service_ca_config_map_or_key_fails_closed() {
        let client =
            mock_kube_client_with_config_maps(HashMap::from([("wrong-key", config_map_with_key("ca.crt", "x"))]));
        let absent = resolve_tls_config(Some(&service_ca_config("absent")), Some(&client), "p").await;
        assert!(matches!(absent, Err((TlsFailureReason::SecretMissing, _))));
        let keyless = resolve_tls_config(Some(&service_ca_config("wrong-key")), Some(&client), "p").await;
        assert!(matches!(keyless, Err((TlsFailureReason::KeyMissing, _))));
        assert_eq!(
            verify_tls_accessible(&client, Some(&service_ca_config("absent")))
                .await
                .unwrap(),
            Some(TlsFailureReason::SecretMissing)
        );
    }

    #[tokio::test]
    async fn exactly_one_ca_source_is_required() {
        let client = mock_kube_client_with_config_maps(HashMap::new());
        let mut both = service_ca_config("x");
        both.ca_secret_ref = test_tls_config("ca").ca_secret_ref;
        let mut neither = service_ca_config("x");
        neither.ca_config_map_ref = None;
        for tls in [both, neither] {
            let resolved = resolve_tls_config(Some(&tls), Some(&client), "p").await;
            assert!(matches!(resolved, Err((TlsFailureReason::MaterialInvalid, _))));
        }
    }
}
