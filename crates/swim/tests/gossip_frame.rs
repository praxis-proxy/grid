//! Integration tests for encrypted SWIM gossip frames.
//!
//! These tests drive the public transport-crypto API with real gossip
//! payloads: a [`StateBroadcast`] is encoded on the wire format, sealed into
//! an encrypted frame, and recovered by a receiving peer.  The AEAD backend
//! (`RustCrypto` `aes-gcm`, or system OpenSSL under the `fips` feature) is
//! selected at compile time, so CI runs this file once per backend
//! configuration and each run proves the backend that binary actually links.

#![expect(clippy::tests_outside_test_module, reason = "integration tests live in tests/")]

use swim::{
    crypto::{self, CryptoError, OVERHEAD},
    node::MAX_SWIM_PACKET_BYTES,
    state_broadcast::{STATE_BROADCAST_VERSION, StateBroadcast},
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn an_encoded_state_broadcast_round_trips_through_the_encrypted_transport() {
    let key = [0x5A_u8; 32];
    let broadcast = sample_broadcast();

    let plaintext = broadcast.encode().unwrap_or_else(|_| std::process::abort());
    let frame = crypto::encrypt(&key, &plaintext).unwrap_or_else(|_| std::process::abort());
    assert_eq!(
        frame.len(),
        OVERHEAD + plaintext.len(),
        "the encrypted frame must add exactly the constant transport overhead"
    );

    let recovered = crypto::decrypt(&key, &frame).unwrap_or_else(|_| std::process::abort());
    assert_eq!(
        recovered, plaintext,
        "decryption must return the encoded broadcast bytes"
    );

    let decoded = StateBroadcast::decode(&recovered).unwrap_or_else(|_| std::process::abort());
    assert_eq!(
        decoded.version, STATE_BROADCAST_VERSION,
        "the decoded broadcast keeps its wire version"
    );
    assert_eq!(
        decoded.origin_site, "site-west",
        "the decoded broadcast keeps its origin site"
    );
    assert_eq!(decoded.revision, 7, "the decoded broadcast keeps its revision");
    assert_eq!(
        decoded.gateway_address.as_deref(),
        Some("grid-gateway.grid-system.svc.cluster.local:9090"),
        "the decoded broadcast keeps its gateway address extension"
    );
    assert_eq!(
        decoded.signals_address.as_deref(),
        Some("grid-gateway.grid-system.svc.cluster.local:9101"),
        "the decoded broadcast keeps its signals address extension"
    );
}

#[test]
fn an_encrypted_broadcast_under_a_wrong_key_is_dropped() {
    let sender_key = [0x01_u8; 32];
    let receiver_key = [0x02_u8; 32];
    let plaintext = sample_broadcast().encode().unwrap_or_else(|_| std::process::abort());

    let frame = crypto::encrypt(&sender_key, &plaintext).unwrap_or_else(|_| std::process::abort());

    assert!(
        matches!(crypto::decrypt(&receiver_key, &frame), Err(CryptoError::AuthFailed)),
        "a peer holding a different key must drop the frame as unauthenticated"
    );
}

#[test]
fn a_tampered_encrypted_broadcast_is_dropped() {
    let key = [0x07_u8; 32];
    let plaintext = sample_broadcast().encode().unwrap_or_else(|_| std::process::abort());

    let mut frame = crypto::encrypt(&key, &plaintext).unwrap_or_else(|_| std::process::abort());
    let tag_start = frame.len().saturating_sub(1);
    if let Some(byte) = frame.get_mut(tag_start) {
        *byte ^= 0xFF;
    }

    assert!(
        matches!(crypto::decrypt(&key, &frame), Err(CryptoError::AuthFailed)),
        "a flipped GCM tag byte must drop the frame before Foca decodes it"
    );
}

#[test]
fn an_encrypted_broadcast_fits_the_swim_datagram_budget() {
    let key = [0x33_u8; 32];
    let plaintext = sample_broadcast().encode().unwrap_or_else(|_| std::process::abort());

    let frame = crypto::encrypt(&key, &plaintext).unwrap_or_else(|_| std::process::abort());

    assert!(
        frame.len() <= MAX_SWIM_PACKET_BYTES,
        "an encrypted single-site broadcast must fit one SWIM UDP datagram"
    );
}

// ---------------------------------------------------------------------------
// Test Utilities
// ---------------------------------------------------------------------------

/// A realistic single-site broadcast: a CRDT snapshot plus the gateway and
/// signals address extensions the runtime gossips for every site.
fn sample_broadcast() -> StateBroadcast {
    StateBroadcast::new(
        "site-west".to_owned(),
        7,
        crdt::GridStateSnapshot::new("site-west".to_owned()),
        Some("grid-gateway.grid-system.svc.cluster.local:9090".to_owned()),
    )
    .with_signals_address(Some("grid-gateway.grid-system.svc.cluster.local:9101".to_owned()))
}
