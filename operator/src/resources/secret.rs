//! Kubernetes Secret builders for grid TLS certificates.

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
}
