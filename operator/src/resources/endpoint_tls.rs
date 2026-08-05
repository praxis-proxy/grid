//! Shared TLS material resolution and validation for endpoint probes.
//!
//! Provides reusable functions for reading Kubernetes Secrets containing
//! CA certificates and client identity material, building a
//! [`rustls::ClientConfig`], and validating that referenced Secrets are
//! accessible before a live probe runs.
//!
//! Used by both metrics scraping ([`super::provider_metrics`]) and
//! health check probing ([`crate::controller::inference_provider`]).

use std::sync::Arc;

use crate::{
    crd::inference_provider::{ClientCertificateSecretRef, EndpointTlsConfig},
    metrics_scraper,
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
/// Returns an error string suitable for logging (never includes the byte
/// content itself).
pub(crate) async fn read_secret_bytes_for_tls(
    client: &kube::Client,
    secret_ref: &crate::crd::grid_network::SecretRef,
    key_name: &str,
    provider_identity: &str,
    material_desc: &str,
) -> Result<Vec<u8>, String> {
    use crate::resources::secret::read_secret_bytes;

    match read_secret_bytes(client, secret_ref, key_name).await {
        Ok(Some(bytes)) if !bytes.is_empty() => Ok(bytes),
        Ok(Some(_)) => Err(format!(
            "{material_desc} key {key_name:?} in Secret {}/{} is empty for provider {provider_identity}",
            secret_ref.namespace, secret_ref.name
        )),
        Ok(None) => Err(format!(
            "{material_desc} Secret {}/{} or key {key_name:?} not found for provider {provider_identity}",
            secret_ref.namespace, secret_ref.name
        )),
        Err(e) => Err(format!(
            "{material_desc} Secret {}/{} read failed for provider {provider_identity}: {e}",
            secret_ref.namespace, secret_ref.name
        )),
    }
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
) -> Result<Option<Arc<rustls::ClientConfig>>, String> {
    let Some(tls) = tls_config else {
        return Ok(None);
    };
    let Some(kube_client) = client else {
        return Err("TLS configured but no Kubernetes client available".to_owned());
    };

    let ca_key = tls.ca_secret_ref.key.as_deref().unwrap_or("ca.crt");
    let ca_pem = read_secret_bytes_for_tls(kube_client, &tls.ca_secret_ref, ca_key, provider_identity, "CA")
        .await
        .map_err(|e| format!("CA secret resolution failed: {e}"))?;

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
            .map_err(|e| format!("{e}"))?;

    Ok(Some(Arc::new(config)))
}

// ---------------------------------------------------------------------------
// TLS validation
// ---------------------------------------------------------------------------

/// Result of reading a TLS Secret key for validation.
enum TlsSecretCheck {
    /// The key was found and contains non-empty bytes.
    Ok(Vec<u8>),
    /// The Secret does not exist or has no `data` section.
    SecretMissing,
    /// The expected key is absent or its value is empty.
    KeyMissing,
}

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
/// - `Ok(Some(reason))` — failure; the provider should be marked [`Degraded`]
///   with the returned reason in `status.reason`.
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
    clippy::too_many_lines,
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

    let ca_key = tls.ca_secret_ref.key.as_deref().unwrap_or("ca.crt");
    let ca_pem = match read_tls_secret_for_verify(client, &tls.ca_secret_ref, ca_key).await? {
        TlsSecretCheck::Ok(bytes) => bytes,
        TlsSecretCheck::SecretMissing => return Ok(Some(TlsFailureReason::SecretMissing)),
        TlsSecretCheck::KeyMissing => return Ok(Some(TlsFailureReason::KeyMissing)),
    };

    let (client_cert_pem, client_key_pem) = if let Some(client_ref) = &tls.client_certificate_secret_ref {
        let sref = secret_ref_from_client_cert(client_ref);
        let cert = match read_tls_secret_for_verify(client, &sref, &client_ref.certificate_key).await? {
            TlsSecretCheck::Ok(bytes) => bytes,
            TlsSecretCheck::SecretMissing => return Ok(Some(TlsFailureReason::SecretMissing)),
            TlsSecretCheck::KeyMissing => return Ok(Some(TlsFailureReason::KeyMissing)),
        };
        let key = match read_tls_secret_for_verify(client, &sref, &client_ref.private_key_key).await? {
            TlsSecretCheck::Ok(bytes) => bytes,
            TlsSecretCheck::SecretMissing => return Ok(Some(TlsFailureReason::SecretMissing)),
            TlsSecretCheck::KeyMissing => return Ok(Some(TlsFailureReason::KeyMissing)),
        };
        (Some(cert), Some(key))
    } else {
        (None, None)
    };

    match metrics_scraper::build_tls_client_config(&ca_pem, client_cert_pem.as_deref(), client_key_pem.as_deref()) {
        Ok(_) => Ok(None),
        Err(e) => {
            let msg = e.to_string();
            let reason = if msg.contains("identity construction failed") {
                TlsFailureReason::IdentityMismatch
            } else {
                TlsFailureReason::MaterialInvalid
            };
            Ok(Some(reason))
        },
    }
}

/// Read raw bytes from a Kubernetes Secret for TLS validation.
///
/// Distinguishes between "Secret not found" and "key not found" to map to
/// the correct [`TlsFailureReason`] variant.
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
) -> Result<TlsSecretCheck, crate::error::OperatorError> {
    let api: kube::Api<k8s_openapi::api::core::v1::Secret> =
        kube::Api::namespaced(client.clone(), &secret_ref.namespace);
    let Some(secret) = api.get_opt(&secret_ref.name).await? else {
        return Ok(TlsSecretCheck::SecretMissing);
    };
    let Some(data) = &secret.data else {
        return Ok(TlsSecretCheck::KeyMissing);
    };
    match data.get(key_name) {
        Some(bytes) if !bytes.0.is_empty() => Ok(TlsSecretCheck::Ok(bytes.0.clone())),
        _ => Ok(TlsSecretCheck::KeyMissing),
    }
}
