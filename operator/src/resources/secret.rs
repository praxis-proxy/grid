//! Kubernetes Secret builders for grid TLS certificates and SWIM encryption keys.

use std::collections::BTreeMap;

use k8s_openapi::{ByteString, api::core::v1::Secret};

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

/// Build a Kubernetes Secret with the given data.
pub fn build(name: &str, namespace: &str, data: BTreeMap<String, ByteString>) -> Secret {
    Secret {
        metadata: kube::api::ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        data: Some(data),
        type_: Some("Opaque".to_owned()),
        ..Default::default()
    }
}

/// Build Secret data for a grid CA certificate.
pub fn ca_secret_data(ca: &certs::CaCert) -> BTreeMap<String, ByteString> {
    let mut data = BTreeMap::new();
    data.insert("ca.crt".to_owned(), ByteString(ca.cert_pem.as_bytes().to_vec()));
    data.insert("ca.key".to_owned(), ByteString(ca.key_pem.as_bytes().to_vec()));
    data
}

/// Read only the public certificate PEM from a site certificate Secret.
///
/// Reads the `tls.crt` key from the named Secret.  The private key (`tls.key`)
/// is deliberately not read — this function must never return private key material.
///
/// Returns `None` when the Secret does not exist or does not contain the
/// `tls.crt` key.
///
/// # Errors
///
/// Returns [`kube::Error`] on Kubernetes API failures.
pub async fn read_site_cert_pem(
    client: &kube::Client,
    secret_ref: &Option<crate::crd::grid_network::SecretRef>,
) -> Result<Option<String>, kube::Error> {
    let Some(r) = secret_ref else {
        return Ok(None);
    };
    let api: kube::Api<Secret> = kube::Api::namespaced(client.clone(), &r.namespace);
    let Some(secret) = api.get_opt(&r.name).await? else {
        return Ok(None);
    };
    Ok(public_cert_pem_from_secret(&secret))
}

/// Extract public certificate PEM from `secret.data["tls.crt"]`.
///
/// Returns `None` for missing, empty, invalid UTF-8, or private-key-looking
/// content.  This is deliberately conservative because the returned value may
/// be broadcast to peers and written to `GridSite.status.publicCertPem`.
fn public_cert_pem_from_secret(secret: &Secret) -> Option<String> {
    secret
        .data
        .as_ref()
        .and_then(|d| d.get("tls.crt"))
        .and_then(|b| String::from_utf8(b.0.clone()).ok())
        .filter(|s| !s.trim().is_empty())
        .filter(|s| !contains_private_key_marker(s))
}

/// Return true if PEM text appears to contain private key material.
fn contains_private_key_marker(pem: &str) -> bool {
    pem.contains("PRIVATE KEY")
}

/// Read a 32-byte AES-256-GCM encryption key from a Kubernetes Secret.
///
/// The Secret is identified by the provided `SecretRef`.  The key name within
/// the Secret's `data` map defaults to `"key"` when `SecretRef::key` is
/// `None`.
///
/// Returns `Ok(Some(key))` when the Secret exists and contains a valid 32-byte
/// value, `Ok(None)` when the Secret does not exist or the key field is absent
/// or has the wrong length, and `Err` on Kubernetes API failures.
///
/// # Errors
///
/// Returns [`kube::Error`] on Kubernetes API failures (e.g. network errors,
/// RBAC denial).  Key-length and missing-field issues return `Ok(None)`, not
/// `Err`.
///
/// # Security invariant
///
/// The decoded key bytes are **never** written to logs, tracing spans, error
/// messages, or Kubernetes resources.  This function does not expose key
/// contents in any return path — callers receive either the raw bytes or
/// `None`/`Err`.
pub async fn read_swim_key(
    client: &kube::Client,
    secret_ref: &crate::crd::grid_network::SecretRef,
) -> Result<Option<swim::crypto::SwimKey>, kube::Error> {
    let api: kube::Api<Secret> = kube::Api::namespaced(client.clone(), &secret_ref.namespace);
    let Some(secret) = api.get_opt(&secret_ref.name).await? else {
        return Ok(None);
    };
    let key_field = secret_ref.key.as_deref().unwrap_or("key");
    let Some(bytes) = secret.data.as_ref().and_then(|d| d.get(key_field)) else {
        tracing::warn!(
            secret = %secret_ref.name,
            namespace = %secret_ref.namespace,
            key_field = %key_field,
            "swimKeyRef Secret missing key field; SWIM key not applied"
        );
        return Ok(None);
    };
    if bytes.0.len() != 32 {
        tracing::warn!(
            secret = %secret_ref.name,
            namespace = %secret_ref.namespace,
            key_field = %key_field,
            len = bytes.0.len(),
            "swimKeyRef key must be exactly 32 bytes; SWIM key not applied"
        );
        return Ok(None);
    }
    let mut key = [0_u8; 32];
    key.copy_from_slice(&bytes.0);
    Ok(Some(key))
}

/// Build Secret data for a site certificate.
pub fn site_cert_secret_data(site: &certs::SiteCertOutput) -> BTreeMap<String, ByteString> {
    let mut data = BTreeMap::new();
    data.insert("tls.crt".to_owned(), ByteString(site.cert_pem.as_bytes().to_vec()));
    data.insert("tls.key".to_owned(), ByteString(site.key_pem.as_bytes().to_vec()));
    data
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_creates_secret_with_metadata() {
        let mut data = BTreeMap::new();
        data.insert("key".to_owned(), ByteString(b"value".to_vec()));
        let secret = build("test-secret", "test-ns", data);

        assert_eq!(secret.metadata.name.as_deref(), Some("test-secret"), "name mismatch");
        assert_eq!(
            secret.metadata.namespace.as_deref(),
            Some("test-ns"),
            "namespace mismatch"
        );
        assert_eq!(secret.type_.as_deref(), Some("Opaque"), "type mismatch");
    }

    #[test]
    fn ca_secret_data_has_expected_keys() {
        let ca = certs::generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let data = ca_secret_data(&ca);
        assert!(data.contains_key("ca.crt"), "should have ca.crt");
        assert!(data.contains_key("ca.key"), "should have ca.key");
    }

    #[test]
    fn site_cert_data_has_expected_keys() {
        let ca = certs::generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let site = certs::generate_site_cert(&ca, "test-site").unwrap_or_else(|_| std::process::abort());
        let data = site_cert_secret_data(&site);
        assert!(data.contains_key("tls.crt"), "should have tls.crt");
        assert!(data.contains_key("tls.key"), "should have tls.key");
    }

    #[test]
    fn public_cert_pem_from_secret_reads_only_tls_crt() {
        let secret = build(
            "site-cert",
            "default",
            BTreeMap::from([
                (
                    "tls.crt".to_owned(),
                    ByteString(b"-----BEGIN CERTIFICATE-----\npublic\n-----END CERTIFICATE-----\n".to_vec()),
                ),
                (
                    "tls.key".to_owned(),
                    ByteString(b"-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----\n".to_vec()),
                ),
            ]),
        );

        let pem = public_cert_pem_from_secret(&secret).unwrap_or_else(|| std::process::abort());
        assert!(pem.contains("BEGIN CERTIFICATE"));
        assert!(!pem.contains("PRIVATE KEY"));
    }

    #[test]
    fn public_cert_pem_from_secret_rejects_private_key_marker_in_tls_crt() {
        let secret = build(
            "site-cert",
            "default",
            BTreeMap::from([(
                "tls.crt".to_owned(),
                ByteString(b"-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----\n".to_vec()),
            )]),
        );

        assert!(
            public_cert_pem_from_secret(&secret).is_none(),
            "tls.crt content with private-key marker must not be propagated"
        );
    }
}
