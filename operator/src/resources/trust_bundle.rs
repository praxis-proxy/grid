//! Trust bundle management for grid mTLS.
//!
//! The trust bundle is a Kubernetes Secret containing the
//! concatenated CA certificates of all sites in the grid.
//! The PraxisConfig controller references this bundle to
//! configure upstream mTLS verification.

use std::collections::BTreeMap;

use k8s_openapi::ByteString;

// ---------------------------------------------------------------------------
// PEM structure validation
// ---------------------------------------------------------------------------

/// Outcome of a public certificate PEM structure check.
///
/// This is a **structural** check, not cryptographic verification.
/// [`ValidStructure`] means the input looks like a public certificate PEM;
/// it does not mean the certificate is trusted or chain-verified against any CA.
///
/// [`ValidStructure`]: CertPemStatus::ValidStructure
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CertPemStatus {
    /// Input has a `-----BEGIN CERTIFICATE-----` header and no private-key markers.
    ///
    /// The PEM structure is consistent with a public certificate.  This is a
    /// header/marker check only — the certificate has not been parsed as X.509,
    /// chain-verified, or checked against any CA.
    ValidStructure,

    /// Input contains a `PRIVATE KEY` marker.
    ///
    /// Private key material must never appear in `GridSite.status.publicCertPem`,
    /// trust bundles, SWIM broadcasts, or status fields.  This outcome is a
    /// security violation indicator: the caller must discard the input and log at
    /// error level.
    ContainsPrivateKey,

    /// Input does not contain a `-----BEGIN CERTIFICATE-----` header.
    ///
    /// This covers garbage input, empty strings, and valid PEM of a different
    /// type (e.g. a CSR or public key).
    NotACertificate,
}

/// Check whether `pem_str` is structurally consistent with a public certificate.
///
/// This is a **marker-based structural check**, not cryptographic verification.
/// The function:
/// 1. Rejects any input that contains `"PRIVATE KEY"` — private key material must never appear in public-facing cert
///    fields.
/// 2. Accepts input that contains `"-----BEGIN CERTIFICATE-----"` as structurally valid.
/// 3. Rejects everything else as not a certificate.
///
/// A [`CertPemStatus::ValidStructure`] result does **not** mean:
/// - The certificate was parsed as X.509.
/// - The issuer, validity period, or SANs were checked.
/// - The certificate is signed by a trusted CA.
/// - The peer holding this cert is authorized for routing.
///
/// Use the result to gate storage in `publicCertPem` and trust bundles, but
/// treat verified-status as at most `TrustMaterialPresent` — never `Trusted`.
#[must_use]
pub fn check_cert_pem(pem_str: &str) -> CertPemStatus {
    // Security invariant: private key markers must be rejected immediately.
    // A misconfigured or malicious peer might send private key PEM; do not store it.
    if pem_str.contains("PRIVATE KEY") {
        return CertPemStatus::ContainsPrivateKey;
    }
    if pem_str.contains("-----BEGIN CERTIFICATE-----") {
        return CertPemStatus::ValidStructure;
    }
    CertPemStatus::NotACertificate
}

// ---------------------------------------------------------------------------
// Trust Bundle Operations
// ---------------------------------------------------------------------------

/// Append a site's public certificate to the trust bundle.
///
/// The bundle is stored as a single `bundle.pem` key containing
/// all site certificates concatenated. This function adds the
/// new certificate if it is structurally valid and not already present.
///
/// Returns the structural validation status.  Only [`CertPemStatus::ValidStructure`]
/// input is appended.
pub fn append_cert(data: &mut BTreeMap<String, ByteString>, site_name: &str, cert_pem: &str) -> CertPemStatus {
    let status = check_cert_pem(cert_pem);
    if status != CertPemStatus::ValidStructure {
        return status;
    }
    let key = format!("{site_name}.pem");
    data.entry(key)
        .or_insert_with(|| ByteString(cert_pem.as_bytes().to_vec()));
    status
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

    // -----------------------------------------------------------------------
    // check_cert_pem
    // -----------------------------------------------------------------------

    const SAMPLE_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\n\
                                    MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA\n\
                                    -----END CERTIFICATE-----\n";

    #[test]
    fn check_cert_pem_valid_structure_accepted() {
        assert_eq!(
            check_cert_pem(SAMPLE_CERT_PEM),
            CertPemStatus::ValidStructure,
            "a PEM with CERTIFICATE header must be accepted as ValidStructure"
        );
    }

    #[test]
    fn check_cert_pem_rejects_rsa_private_key() {
        let rsa_key = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA\n-----END RSA PRIVATE KEY-----\n";
        assert_eq!(
            check_cert_pem(rsa_key),
            CertPemStatus::ContainsPrivateKey,
            "RSA private key must be rejected"
        );
    }

    #[test]
    fn check_cert_pem_rejects_pkcs8_private_key() {
        let pkcs8_key = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkq\n-----END PRIVATE KEY-----\n";
        assert_eq!(
            check_cert_pem(pkcs8_key),
            CertPemStatus::ContainsPrivateKey,
            "PKCS#8 private key must be rejected"
        );
    }

    #[test]
    fn check_cert_pem_rejects_ec_private_key() {
        let ec_key = "-----BEGIN EC PRIVATE KEY-----\nMHQCAQEEIABCDE\n-----END EC PRIVATE KEY-----\n";
        assert_eq!(
            check_cert_pem(ec_key),
            CertPemStatus::ContainsPrivateKey,
            "EC private key must be rejected"
        );
    }

    #[test]
    fn check_cert_pem_rejects_encrypted_private_key() {
        let enc_key = "-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIFHDBOBgkq\n-----END ENCRYPTED PRIVATE KEY-----\n";
        assert_eq!(
            check_cert_pem(enc_key),
            CertPemStatus::ContainsPrivateKey,
            "encrypted private key must be rejected"
        );
    }

    #[test]
    fn check_cert_pem_rejects_empty_string() {
        assert_eq!(
            check_cert_pem(""),
            CertPemStatus::NotACertificate,
            "empty string is not a certificate"
        );
    }

    #[test]
    fn check_cert_pem_rejects_garbage() {
        assert_eq!(
            check_cert_pem("not a cert at all"),
            CertPemStatus::NotACertificate,
            "garbage input is not a certificate"
        );
    }

    #[test]
    fn check_cert_pem_rejects_public_key_pem() {
        let pub_key = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkq\n-----END PUBLIC KEY-----\n";
        assert_eq!(
            check_cert_pem(pub_key),
            CertPemStatus::NotACertificate,
            "public key PEM (not a cert) must not be accepted as ValidStructure"
        );
    }

    #[test]
    fn check_cert_pem_valid_structure_does_not_mean_trusted() {
        // This test documents the invariant: ValidStructure is a PEM marker check,
        // not chain verification. The result must never be treated as "trusted".
        let result = check_cert_pem(SAMPLE_CERT_PEM);
        assert_eq!(result, CertPemStatus::ValidStructure);
        // Callers must use ValidStructure only to gate storage, not to authorize peers.
        assert_ne!(
            format!("{result:?}"),
            "Trusted",
            "ValidStructure must not be aliased as Trusted"
        );
    }

    #[test]
    fn check_cert_pem_private_key_wins_even_with_cert_header() {
        // If a payload has both headers, private key detection wins.
        let mixed = "-----BEGIN CERTIFICATE-----\nABC\n-----END CERTIFICATE-----\n\
                     -----BEGIN PRIVATE KEY-----\nDEF\n-----END PRIVATE KEY-----\n";
        assert_eq!(
            check_cert_pem(mixed),
            CertPemStatus::ContainsPrivateKey,
            "private key detection must win over cert header"
        );
    }

    #[test]
    fn append_adds_cert() {
        let mut data = BTreeMap::new();
        assert_eq!(
            append_cert(&mut data, "cluster-a", SAMPLE_CERT_PEM),
            CertPemStatus::ValidStructure
        );
        assert!(data.contains_key("cluster-a.pem"), "should add cert");
    }

    #[test]
    fn append_is_idempotent() {
        let mut data = BTreeMap::new();
        append_cert(&mut data, "cluster-a", SAMPLE_CERT_PEM);
        append_cert(&mut data, "cluster-a", SAMPLE_CERT_PEM);
        assert_eq!(data.len(), 1, "should not duplicate");
        let val = String::from_utf8_lossy(&data.get("cluster-a.pem").unwrap_or_else(|| std::process::abort()).0);
        assert_eq!(val, SAMPLE_CERT_PEM, "first write wins");
    }

    #[test]
    fn append_rejects_private_key_material() {
        let mut data = BTreeMap::new();
        let status = append_cert(
            &mut data,
            "cluster-a",
            "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----\n",
        );
        assert_eq!(status, CertPemStatus::ContainsPrivateKey);
        assert!(data.is_empty(), "private key material must not be appended");
    }

    #[test]
    fn remove_deletes_cert() {
        let mut data = BTreeMap::new();
        append_cert(&mut data, "cluster-a", SAMPLE_CERT_PEM);
        remove_cert(&mut data, "cluster-a");
        assert!(!data.contains_key("cluster-a.pem"), "should remove cert");
    }

    #[test]
    fn concatenated_pem_joins_all() {
        let mut data = BTreeMap::new();
        append_cert(&mut data, "a", SAMPLE_CERT_PEM);
        append_cert(
            &mut data,
            "b",
            "-----BEGIN CERTIFICATE-----\nMIIB-second\n-----END CERTIFICATE-----\n",
        );
        let pem = concatenated_pem(&data);
        assert!(pem.contains("MIIB"), "should contain first cert");
        assert!(pem.contains("MIIB-second"), "should contain second cert");
    }
}
