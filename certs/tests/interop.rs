//! Cross-backend certificate interop.
//!
//! A certificate minted by the rcgen backend must verify under the openssl
//! backend and the reverse, so a node built either way accepts the other's
//! identity through this crate's `verify_site_cert`. Scope is that verifier. The
//! live TLS handshake (rustls or openssl mTLS) is exercised elsewhere. The
//! fixtures are committed PEM, one CA and one leaf per backend, minted by the
//! `regenerate_interop_fixtures` test in `generate.rs`. Whichever backend this
//! build compiles verifies both sets.

#![allow(clippy::tests_outside_test_module, reason = "integration tests live in tests/")]

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
