//! The request key policy, run under whichever backend this build compiles.
//!
//! The committed requests come from the openssl CLI, as with
//! `openssl req -new -newkey rsa:2048 -nodes -subj /CN=site-d`, keys discarded.

#![expect(clippy::tests_outside_test_module, reason = "integration tests live in tests/")]
#![expect(clippy::expect_used, clippy::indexing_slicing, reason = "tests")]

use certs::{EnrollError, Validity, generate_ca, sign_csr, verify_csr};
use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256, PKCS_ECDSA_P384_SHA384, SigningKey as _};

/// Requests for keys outside the policy, by what they carry.
const REFUSED: [(&str, &str); 7] = [
    ("RSA-1024", include_str!("fixtures/requests/rsa-1024.csr")),
    ("RSA-2048", include_str!("fixtures/requests/rsa-2048.csr")),
    ("RSA-4096", include_str!("fixtures/requests/rsa-4096.csr")),
    ("P-521", include_str!("fixtures/requests/p-521.csr")),
    ("secp256k1", include_str!("fixtures/requests/secp256k1.csr")),
    ("Ed25519", include_str!("fixtures/requests/ed25519.csr")),
    (
        "P-256 with explicit parameters",
        include_str!("fixtures/requests/p-256-explicit.csr"),
    ),
];

const ID_EC_PUBLIC_KEY: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x02, 0x01];
const PRIME256V1: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x03, 0x01, 0x07];
const SECP384R1: &[u8] = &[0x2B, 0x81, 0x04, 0x00, 0x22];
const ECDSA_WITH_SHA256: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x04, 0x03, 0x02];
const SHA256_WITH_RSA: &[u8] = &[0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0B];

/// One DER element.
fn der(tag: u8, content: &[u8]) -> Vec<u8> {
    let len = content.len();
    let mut out = vec![tag];
    match u8::try_from(len) {
        Ok(short) if short < 0x80 => out.push(short),
        _ => {
            let bytes: Vec<u8> = len.to_be_bytes().into_iter().skip_while(|byte| *byte == 0).collect();
            out.push(0x80 | u8::try_from(bytes.len()).expect("length"));
            out.extend(bytes);
        },
    }
    out.extend_from_slice(content);
    out
}

fn seq(parts: &[Vec<u8>]) -> Vec<u8> {
    der(0x30, &parts.concat())
}

/// A request whose key is `point` labelled as `curve`, with `algorithm` named as its
/// signature algorithm, signed by `key`.
fn request(key: &KeyPair, curve: &[u8], point: &[u8], algorithm: &[u8]) -> String {
    let spki = seq(&[
        seq(&[der(0x06, ID_EC_PUBLIC_KEY), der(0x06, curve)]),
        der(0x03, &[&[0x00], point].concat()),
    ]);
    request_with_spki(key, spki, algorithm)
}

/// Sign the exact supplied SPKI encoding, including any deliberately malformed DER.
fn request_with_spki(key: &KeyPair, spki: Vec<u8>, algorithm: &[u8]) -> String {
    let name = seq(&[der(0x31, &seq(&[der(0x06, &[0x55, 0x04, 0x03]), der(0x0C, b"site-d")]))]);
    let info = seq(&[der(0x02, &[0x00]), name, spki, der(0xA0, &[])]);
    let signature = key.sign(&info).expect("sign");
    let csr = seq(&[
        info,
        seq(&[der(0x06, algorithm)]),
        der(0x03, &[&[0x00], signature.as_slice()].concat()),
    ]);
    pem::encode(&pem::Pem::new("CERTIFICATE REQUEST", csr))
}

/// Whether a request is refused both where it is submitted and where it is signed.
fn refused(csr: &str) -> (Result<String, EnrollError>, Option<EnrollError>) {
    let ca = generate_ca("grid-ca").expect("ca");
    (verify_csr(csr), sign_csr(&ca, "site-d", csr, Validity::default()).err())
}

#[test]
fn a_key_outside_the_policy_is_refused_at_submit_and_at_signing() {
    for (what, csr) in REFUSED {
        assert_eq!(
            refused(csr),
            (Err(EnrollError::UnsupportedKey), Some(EnrollError::UnsupportedKey)),
            "{what}"
        );
    }
}

#[test]
fn p256_and_p384_keys_are_accepted() {
    let ca = generate_ca("grid-ca").expect("ca");
    for algorithm in [&PKCS_ECDSA_P256_SHA256, &PKCS_ECDSA_P384_SHA384] {
        let key = KeyPair::generate_for(algorithm).expect("key");
        let csr = rcgen::CertificateParams::default()
            .serialize_request(&key)
            .expect("csr")
            .pem()
            .expect("pem");
        assert!(verify_csr(&csr).is_ok(), "{algorithm:?}");
        assert!(
            sign_csr(&ca, "site-d", &csr, Validity::default()).is_ok(),
            "{algorithm:?}"
        );
    }
    let operator = certs::generate_csr("site-d").expect("operator csr");
    assert!(verify_csr(&operator.csr_pem).is_ok(), "the key the operator generates");
}

#[test]
fn a_hand_built_request_passes_only_when_its_key_matches_its_curve() {
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("key");
    let point = key.public_key_raw();
    let control = request(&key, PRIME256V1, point, ECDSA_WITH_SHA256);
    assert!(verify_csr(&control).is_ok(), "the builder makes a valid request");

    let mislabelled = request(&key, SECP384R1, point, ECDSA_WITH_SHA256);
    let (submitted, signed) = refused(&mislabelled);
    assert!(submitted.is_err() && signed.is_some(), "a P-256 point named as P-384");

    let mut compressed = vec![0x02 | (point[64] & 1)];
    compressed.extend_from_slice(&point[1..33]);
    assert_eq!(
        refused(&request(&key, PRIME256V1, &compressed, ECDSA_WITH_SHA256)),
        (Err(EnrollError::UnsupportedKey), Some(EnrollError::UnsupportedKey)),
        "a compressed point"
    );
}

#[test]
fn alternate_encodings_cannot_give_one_key_another_fingerprint() {
    const ECDSA_WITH_SHA384: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x04, 0x03, 0x03];
    for (algorithm, curve, signature) in [
        (&PKCS_ECDSA_P256_SHA256, PRIME256V1, ECDSA_WITH_SHA256),
        (&PKCS_ECDSA_P384_SHA384, SECP384R1, ECDSA_WITH_SHA384),
    ] {
        let key = KeyPair::generate_for(algorithm).expect("key");
        let spki = seq(&[
            seq(&[der(0x06, ID_EC_PUBLIC_KEY), der(0x06, curve)]),
            der(0x03, &[&[0x00], key.public_key_raw()].concat()),
        ]);
        verify_csr(&request_with_spki(&key, spki.clone(), signature)).expect("canonical encoding");
        let nonminimal = [&[0x30, 0x81, spki[1]], &spki[2..]].concat();
        let extra_parameter = seq(&[
            seq(&[der(0x06, ID_EC_PUBLIC_KEY), der(0x06, curve), der(0x05, &[])]),
            der(0x03, &[&[0x00], key.public_key_raw()].concat()),
        ]);
        let unused_bits = seq(&[
            seq(&[der(0x06, ID_EC_PUBLIC_KEY), der(0x06, curve)]),
            der(0x03, &[&[0x01], key.public_key_raw()].concat()),
        ]);
        for (name, encoded) in [
            ("non-minimal sequence length", nonminimal),
            ("extra algorithm parameter", extra_parameter),
            ("nonzero unused bits", unused_bits),
        ] {
            let csr = request_with_spki(&key, encoded, signature);
            let (submitted, signed) = refused(&csr);
            assert!(
                submitted.is_err() && signed.is_some(),
                "{algorithm:?}: {name}: {submitted:?} {signed:?}"
            );
        }
    }
}

#[test]
fn a_signature_algorithm_that_disagrees_with_the_key_is_refused() {
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("key");
    let (submitted, signed) = refused(&request(&key, PRIME256V1, key.public_key_raw(), SHA256_WITH_RSA));
    assert!(submitted.is_err() && signed.is_some(), "{submitted:?} {signed:?}");
}
