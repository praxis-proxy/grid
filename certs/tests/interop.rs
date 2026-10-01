//! Cross-backend certificate interop.
//!
//! A certificate minted by the rcgen backend must verify under the openssl
//! backend and the reverse, so a node built either way accepts the other's
//! identity through this crate's `verify_site_cert`. Scope is that verifier. The
//! live TLS handshake (rustls or openssl mTLS) is exercised elsewhere. The
//! fixtures are committed PEM, one CA and one leaf per backend, minted by the
//! `regenerate_interop_fixtures` test in `generate.rs`. Whichever backend this
//! build compiles verifies both sets.
//!
//! This file also carries the shared backend contract: the request-acceptance
//! tests below build one request and submit it through the public API, so CI
//! runs them once under the default backend and once under `--no-default-features
//! --features fips` and both builds must accept it the same way.
//!
//! Regenerating the committed fixtures is a two-step, once-per-backend workflow:
//!
//! ```text
//! cargo test -p certs -- --ignored regenerate_interop_fixtures
//! cargo test -p certs --no-default-features --features fips -- --ignored regenerate_interop_fixtures
//! ```

#![allow(clippy::tests_outside_test_module, reason = "integration tests live in tests/")]
#![expect(clippy::expect_used, reason = "tests")]

use certs::{VerifyError, verify_site_cert};

const RCGEN_CA: &str = include_str!("fixtures/rcgen/ca.pem");
const RCGEN_LEAF: &str = include_str!("fixtures/rcgen/leaf.pem");
const OPENSSL_CA: &str = include_str!("fixtures/openssl/ca.pem");
const OPENSSL_LEAF: &str = include_str!("fixtures/openssl/leaf.pem");

/// Site both fixture leaves are bound to.
const SITE: &str = "alpha";

#[test]
fn rcgen_leaf_verifies_under_this_backend() {
    assert!(
        verify_site_cert(RCGEN_CA, RCGEN_LEAF, SITE).is_ok(),
        "an rcgen-minted leaf must verify against its rcgen CA under this build's verifier"
    );
}

#[test]
fn openssl_leaf_verifies_under_this_backend() {
    assert!(
        verify_site_cert(OPENSSL_CA, OPENSSL_LEAF, SITE).is_ok(),
        "an openssl-minted leaf must verify against its openssl CA under this build's verifier"
    );
}

#[test]
fn a_leaf_does_not_verify_against_the_other_backend_ca() {
    // Both CAs share the CN=grid-ca subject, so the issuer name matches and the
    // signature is what rejects the pairing: each leaf is signed by its own CA's
    // key, not the other's.
    assert_eq!(
        verify_site_cert(OPENSSL_CA, RCGEN_LEAF, SITE),
        Err(VerifyError::BadSignature),
        "the rcgen leaf must not verify against the openssl CA"
    );
    assert_eq!(
        verify_site_cert(RCGEN_CA, OPENSSL_LEAF, SITE),
        Err(VerifyError::BadSignature),
        "the openssl leaf must not verify against the rcgen CA"
    );
}

// ---------------------------------------------------------------------------
// Shared backend contract: request acceptance must not depend on the build.
// ---------------------------------------------------------------------------

use certs::{EnrollError, Validity, generate_ca, sign_csr, verify_csr};
use rcgen::{CertificateParams, CustomExtension, KeyPair, SanType};
use x509_parser::prelude::{FromDer as _, X509Certificate};

/// An OID under a private arc that no backend issues, used to prove a request
/// carrying an extension outside the recognized set is accepted, not refused.
const UNSUPPORTED_EXT_OID: &[u64] = &[1, 3, 6, 1, 4, 1, 57264, 1];

/// The same OID in dotted form, for matching against issued-certificate extensions.
const UNSUPPORTED_EXT_OID_STR: &str = "1.3.6.1.4.1.57264.1";

/// Build a request the way an enrollee would, optionally asking for a custom
/// extension the grid does not issue.
fn request_with_custom_extension() -> String {
    let key = KeyPair::generate().expect("test fixture");
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "whatever-i-want");
    params.subject_alt_names.push(SanType::DnsName(
        "i-picked-this.example".to_owned().try_into().expect("test fixture"),
    ));
    params
        .custom_extensions
        .push(CustomExtension::from_oid_content(UNSUPPORTED_EXT_OID, vec![0x05, 0x00]));
    params
        .serialize_request(&key)
        .expect("test fixture")
        .pem()
        .expect("test fixture")
}

/// The policy the two backends must share: an unsupported requested extension is
/// ignored, not rejected. Whichever backend this build compiles must accept the
/// request, so the enrollment API answers the same request the same way.
#[test]
fn a_request_with_an_unsupported_extension_is_accepted() {
    let ca = generate_ca("grid-ca").expect("test fixture");
    let csr = request_with_custom_extension();

    // Submit-time possession check accepts it.
    assert!(
        verify_csr(&csr).is_ok(),
        "an unsupported requested extension must not make a request unverifiable"
    );

    // Issuance accepts it, whichever backend this build compiles.
    let issued = sign_csr(&ca, "site-d", &csr, Validity::default());
    assert!(
        issued.is_ok(),
        "an unsupported requested extension must be ignored, not rejected: {issued:?}"
    );
}

/// The reason ignoring is safe: the requested extension never reaches the issued
/// certificate, so acceptance carries nothing the requester chose.
#[test]
fn an_unsupported_requested_extension_never_reaches_the_certificate() {
    let ca = generate_ca("grid-ca").expect("test fixture");
    let csr = request_with_custom_extension();
    let issued = sign_csr(&ca, "site-d", &csr, Validity::default()).expect("test fixture");

    let der = pem::parse(&issued.cert_pem).expect("test fixture");
    let (_rest, cert) = X509Certificate::from_der(der.contents()).expect("test fixture");
    assert!(
        cert.extensions()
            .iter()
            .all(|ext| ext.oid.to_id_string() != UNSUPPORTED_EXT_OID_STR),
        "the requester's extension must not appear on the issued certificate"
    );
    // The identity is the assigned one, not the one the request asked for.
    assert_eq!(
        verify_site_cert(&ca.cert_pem, &issued.cert_pem, "site-d"),
        Ok(cert.public_key().subject_public_key.data.to_vec()),
        "the issued certificate must carry the assigned identity"
    );
}

/// A request whose self-signature does not match its key is refused by both
/// backends, so ignoring extensions does not weaken possession.
#[test]
fn a_request_signed_by_another_key_is_refused_by_both_backends() {
    let ca = generate_ca("grid-ca").expect("test fixture");
    let csr = request_with_custom_extension();

    let der = pem::parse(&csr).expect("test fixture");
    let mut bytes = der.contents().to_vec();
    if let Some(last) = bytes.last_mut() {
        *last ^= 0xFF;
    }
    let tampered = pem::encode(&pem::Pem::new("CERTIFICATE REQUEST", bytes));

    assert_eq!(
        sign_csr(&ca, "site-d", &tampered, Validity::default()).err(),
        Some(EnrollError::BadSignature),
        "a request not signed by its own key must be refused by either backend"
    );
}

/// The fingerprint the crate computes for a request must be the SHA-256 of the
/// request's `SubjectPublicKeyInfo` DER, whichever backend this build compiles.
///
/// The two backends are compile-time mutually exclusive, so no single process
/// runs both. Pinning the fingerprint to that one canonical representation is
/// what closes the cross-backend gap: CI runs this test under the default and
/// the fips build, and both must equal the same independently-computed digest,
/// so the same key fingerprints identically across backends.
#[test]
fn the_key_fingerprint_is_the_canonical_spki_digest_under_either_backend() {
    use x509_parser::certification_request::X509CertificationRequest;

    let csr = request_with_custom_extension();
    let fingerprint = verify_csr(&csr).expect("a well-formed request must verify");

    let der = pem::parse(&csr).expect("csr pem");
    let (_rest, parsed) = X509CertificationRequest::from_der(der.contents()).expect("parse csr");
    let spki_der = parsed.certification_request_info.subject_pki.raw;
    let expected: String = certs::sha256(spki_der)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    assert_eq!(
        fingerprint, expected,
        "the fingerprint must be SHA-256 over the SubjectPublicKeyInfo DER, identical across backends"
    );
}

#[test]
fn generate_csr_carries_only_a_name_and_a_key_the_server_signs() {
    use x509_parser::certification_request::X509CertificationRequest;

    let ca = generate_ca("grid-ca").expect("test fixture");
    let request = certs::generate_csr("site-d").expect("a well-formed request must generate");
    assert!(
        request.key_pem.contains("PRIVATE KEY"),
        "the requester keeps a usable key"
    );
    let der = pem::parse(&request.csr_pem).expect("pem");
    let (_rest, csr) = X509CertificationRequest::from_der(der.contents()).expect("parse");
    assert!(
        csr.requested_extensions().is_none_or(|mut ext| ext.next().is_none()),
        "the request asks for no extensions"
    );
    let issued = sign_csr(&ca, "site-d", &request.csr_pem, Validity::default()).expect("test fixture");
    assert!(
        verify_site_cert(&ca.cert_pem, &issued.cert_pem, "site-d").is_ok(),
        "the issued leaf verifies as the assigned site"
    );
    assert_eq!(
        certs::cert_public_key(&issued.cert_pem).expect("leaf key"),
        certs::csr_public_key(&request.csr_pem).expect("request key"),
        "the leaf carries the requester's key"
    );
}
