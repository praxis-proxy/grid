//! Trust bundle management for grid mTLS.
//!
//! The trust bundle is a Kubernetes Secret containing the
//! concatenated CA certificates of all sites in the grid.
//! The PraxisConfig controller references this bundle to
//! configure upstream mTLS verification.

use std::collections::BTreeMap;

use k8s_openapi::ByteString;

// ---------------------------------------------------------------------------
// Trust Bundle Operations
// ---------------------------------------------------------------------------

/// Append a site's public certificate to the trust bundle.
///
/// The bundle is stored as a single `bundle.pem` key containing
/// all site certificates concatenated. This function adds the
/// new certificate if it is not already present.
pub fn append_cert(data: &mut BTreeMap<String, ByteString>, site_name: &str, cert_pem: &str) {
    let key = format!("{site_name}.pem");
    data.entry(key)
        .or_insert_with(|| ByteString(cert_pem.as_bytes().to_vec()));
}

/// Remove a site's certificate from the trust bundle.
pub fn remove_cert(data: &mut BTreeMap<String, ByteString>, site_name: &str) {
    let key = format!("{site_name}.pem");
    data.remove(&key);
}

/// Concatenate all certificates in the bundle into a single PEM.
pub fn concatenated_pem(data: &BTreeMap<String, ByteString>) -> String {
    let mut pem = String::new();
    for value in data.values() {
        let cert = String::from_utf8_lossy(&value.0);
        pem.push_str(&cert);
        if !pem.ends_with('\n') {
            pem.push('\n');
        }
    }
    pem
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_adds_cert() {
        let mut data = BTreeMap::new();
        append_cert(&mut data, "cluster-a", "CERT-A");
        assert!(data.contains_key("cluster-a.pem"), "should add cert");
    }

    #[test]
    fn append_is_idempotent() {
        let mut data = BTreeMap::new();
        append_cert(&mut data, "cluster-a", "CERT-A");
        append_cert(&mut data, "cluster-a", "CERT-A-NEW");
        assert_eq!(data.len(), 1, "should not duplicate");
        let val = String::from_utf8_lossy(&data.get("cluster-a.pem").unwrap_or_else(|| std::process::abort()).0);
        assert_eq!(val, "CERT-A", "first write wins");
    }

    #[test]
    fn remove_deletes_cert() {
        let mut data = BTreeMap::new();
        append_cert(&mut data, "cluster-a", "CERT-A");
        remove_cert(&mut data, "cluster-a");
        assert!(!data.contains_key("cluster-a.pem"), "should remove cert");
    }

    #[test]
    fn concatenated_pem_joins_all() {
        let mut data = BTreeMap::new();
        append_cert(&mut data, "a", "CERT-A\n");
        append_cert(&mut data, "b", "CERT-B\n");
        let pem = concatenated_pem(&data);
        assert!(pem.contains("CERT-A"), "should contain A");
        assert!(pem.contains("CERT-B"), "should contain B");
    }
}
