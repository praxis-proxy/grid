//! ECDSA P-256 sign/verify primitives for SWIM state broadcasts.
//!
//! Deliberately decoupled from *where* key material comes from: callers
//! supply raw PKCS8 DER (for signing) or a raw uncompressed EC point (for
//! verification) and this module only performs the cryptographic operation.
//! Sourcing and pinning key material against a [`GridSite`] identity is an
//! operator-level concern tracked separately.
//! FIPS builds use OpenSSL; other builds use `ring`.
//!
//! [`GridSite`]: https://github.com/praxis-proxy/grid/issues/75

#[cfg(feature = "fips")]
use openssl::{
    bn::BigNumContext,
    ec::{EcGroup, EcKey, EcPoint, PointConversionForm},
    hash::MessageDigest,
    nid::Nid,
    pkey::PKey,
    sign::{Signer, Verifier},
};
#[cfg(feature = "ring-signing")]
use ring::{
    rand::SystemRandom,
    signature::{
        ECDSA_P256_SHA256_ASN1, ECDSA_P256_SHA256_ASN1_SIGNING, EcdsaKeyPair, KeyPair as _, UnparsedPublicKey,
    },
};

#[cfg(all(feature = "fips", feature = "ring-signing"))]
compile_error!("features `fips` and `ring-signing` are mutually exclusive");
#[cfg(not(any(feature = "fips", feature = "ring-signing")))]
compile_error!("one of `fips` or `ring-signing` must be enabled");

/// Errors produced while signing a payload.
#[derive(Debug, thiserror::Error)]
pub enum SigningError {
    /// The supplied PKCS8 DER key material was rejected by the signing backend.
    #[error("invalid PKCS8 signing key")]
    InvalidKey,

    /// Signing failed for a reason the backend does not disclose in detail.
    #[error("signing operation failed")]
    SigningFailed,
}

/// Errors produced while verifying a payload's signature.
#[derive(Debug, thiserror::Error)]
pub enum VerificationError {
    /// The signature does not match the payload under the supplied public key.
    #[error("signature verification failed")]
    Invalid,
}

/// Derive the raw uncompressed P-256 public key from PKCS8 DER.
///
/// # Errors
///
/// Returns [`SigningError::InvalidKey`] when `pkcs8_der` is not a valid
/// ECDSA P-256 key.
pub fn public_key_ecdsa_p256(pkcs8_der: &[u8]) -> Result<Vec<u8>, SigningError> {
    #[cfg(feature = "fips")]
    {
        let key = PKey::private_key_from_pkcs8(pkcs8_der).map_err(|_error| SigningError::InvalidKey)?;
        let ec_key = key.ec_key().map_err(|_error| SigningError::InvalidKey)?;
        if ec_key.group().curve_name() != Some(Nid::X9_62_PRIME256V1) {
            return Err(SigningError::InvalidKey);
        }
        let mut context = BigNumContext::new().map_err(|_error| SigningError::InvalidKey)?;
        ec_key
            .public_key()
            .to_bytes(ec_key.group(), PointConversionForm::UNCOMPRESSED, &mut context)
            .map_err(|_error| SigningError::InvalidKey)
    }
    #[cfg(not(feature = "fips"))]
    {
        let rng = SystemRandom::new();
        let key_pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, pkcs8_der, &rng)
            .map_err(|_key_rejected| SigningError::InvalidKey)?;
        Ok(key_pair.public_key().as_ref().to_vec())
    }
}

/// Sign `payload` with an ECDSA P-256 key supplied as PKCS8 DER.
///
/// Returns the signature in ASN.1 DER form (~70-72 bytes for P-256).
///
/// # Errors
///
/// Returns [`SigningError::InvalidKey`] if `pkcs8_der` is not a valid ECDSA
/// P-256 PKCS8 key, or [`SigningError::SigningFailed`] if the underlying RNG
/// or signing operation fails.
pub fn sign_ecdsa_p256(pkcs8_der: &[u8], payload: &[u8]) -> Result<Vec<u8>, SigningError> {
    #[cfg(feature = "fips")]
    {
        let key = PKey::private_key_from_pkcs8(pkcs8_der).map_err(|_error| SigningError::InvalidKey)?;
        let ec_key = key.ec_key().map_err(|_error| SigningError::InvalidKey)?;
        if ec_key.group().curve_name() != Some(Nid::X9_62_PRIME256V1) {
            return Err(SigningError::InvalidKey);
        }
        let mut signer = Signer::new(MessageDigest::sha256(), &key).map_err(|_error| SigningError::SigningFailed)?;
        signer.update(payload).map_err(|_error| SigningError::SigningFailed)?;
        signer.sign_to_vec().map_err(|_error| SigningError::SigningFailed)
    }
    #[cfg(not(feature = "fips"))]
    {
        let rng = SystemRandom::new();
        let key_pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, pkcs8_der, &rng)
            .map_err(|_key_rejected| SigningError::InvalidKey)?;
        let signature = key_pair
            .sign(&rng, payload)
            .map_err(|_signing_failed| SigningError::SigningFailed)?;
        Ok(signature.as_ref().to_vec())
    }
}

/// Verify `signature` over `payload` under an ECDSA P-256 public key
/// supplied as a raw uncompressed EC point (the `subjectPublicKey` bit
/// string contents of an X.509 SPKI structure, sec1 uncompressed form).
///
/// # Errors
///
/// Returns [`VerificationError::Invalid`] when the signature does not
/// verify, including when `raw_pubkey` is malformed.
pub fn verify_ecdsa_p256(raw_pubkey: &[u8], payload: &[u8], signature: &[u8]) -> Result<(), VerificationError> {
    #[cfg(feature = "fips")]
    {
        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).map_err(|_error| VerificationError::Invalid)?;
        let mut context = BigNumContext::new().map_err(|_error| VerificationError::Invalid)?;
        let point =
            EcPoint::from_bytes(&group, raw_pubkey, &mut context).map_err(|_error| VerificationError::Invalid)?;
        let ec_key = EcKey::from_public_key(&group, &point).map_err(|_error| VerificationError::Invalid)?;
        let key = PKey::from_ec_key(ec_key).map_err(|_error| VerificationError::Invalid)?;
        let mut verifier = Verifier::new(MessageDigest::sha256(), &key).map_err(|_error| VerificationError::Invalid)?;
        verifier.update(payload).map_err(|_error| VerificationError::Invalid)?;
        match verifier.verify(signature) {
            Ok(true) => Ok(()),
            Ok(false) | Err(_) => Err(VerificationError::Invalid),
        }
    }
    #[cfg(not(feature = "fips"))]
    {
        let verifier = UnparsedPublicKey::new(&ECDSA_P256_SHA256_ASN1, raw_pubkey);
        verifier
            .verify(payload, signature)
            .map_err(|_unspecified| VerificationError::Invalid)
    }
}

#[cfg(test)]
mod tests {
    use rcgen::KeyPair;

    use super::*;

    // Test Utilities

    /// Generate an ECDSA P-256 keypair plus the raw SPKI EC point ring
    /// expects for verification, mirroring how a `GridSite`'s certificate
    /// key material is produced by `certs::generate`.
    fn generate_key_and_raw_pubkey() -> (Vec<u8>, Vec<u8>) {
        let key_pair = KeyPair::generate().unwrap_or_else(|_| std::process::abort());
        let pkcs8_der = key_pair.serialize_der();
        let params = rcgen::CertificateParams::new(vec!["spike.grid.internal".to_owned()])
            .unwrap_or_else(|_| std::process::abort());
        let cert = params.self_signed(&key_pair).unwrap_or_else(|_| std::process::abort());
        let (_, parsed) = x509_parser::parse_x509_certificate(cert.der()).unwrap_or_else(|_| std::process::abort());
        let raw_pubkey = parsed.public_key().subject_public_key.as_ref().to_vec();
        (pkcs8_der, raw_pubkey)
    }

    #[test]
    fn sign_then_verify_round_trips_for_a_matching_key_and_payload() {
        let (pkcs8_der, raw_pubkey) = generate_key_and_raw_pubkey();
        let payload = b"origin-site-a|revision=1";

        let signature = sign_ecdsa_p256(&pkcs8_der, payload).unwrap_or_else(|_| std::process::abort());

        verify_ecdsa_p256(&raw_pubkey, payload, &signature).unwrap_or_else(|_| std::process::abort());
    }

    #[test]
    fn public_key_derivation_matches_the_certificate_key() {
        let (pkcs8_der, raw_pubkey) = generate_key_and_raw_pubkey();

        let derived = public_key_ecdsa_p256(&pkcs8_der).unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            derived, raw_pubkey,
            "the signing backend derives the matching P-256 public point"
        );
    }

    #[test]
    fn verify_rejects_a_signature_over_a_different_payload() {
        let (pkcs8_der, raw_pubkey) = generate_key_and_raw_pubkey();
        let signature = sign_ecdsa_p256(&pkcs8_der, b"revision=1").unwrap_or_else(|_| std::process::abort());

        let result = verify_ecdsa_p256(&raw_pubkey, b"revision=2", &signature);

        assert!(matches!(result, Err(VerificationError::Invalid)));
    }

    #[test]
    fn verify_rejects_a_signature_from_an_unrelated_key() {
        let (pkcs8_der, _) = generate_key_and_raw_pubkey();
        let (_, other_raw_pubkey) = generate_key_and_raw_pubkey();
        let payload = b"origin-site-a|revision=1";
        let signature = sign_ecdsa_p256(&pkcs8_der, payload).unwrap_or_else(|_| std::process::abort());

        let result = verify_ecdsa_p256(&other_raw_pubkey, payload, &signature);

        assert!(matches!(result, Err(VerificationError::Invalid)));
    }

    #[test]
    fn sign_rejects_malformed_pkcs8_key_material() {
        let result = sign_ecdsa_p256(b"not-a-real-key", b"payload");

        assert!(matches!(result, Err(SigningError::InvalidKey)));
    }

    #[test]
    fn invalid_key_error_message_is_descriptive() {
        assert!(SigningError::InvalidKey.to_string().contains("PKCS8"));
    }

    #[test]
    fn signing_failed_error_message_is_descriptive() {
        // SigningFailed only arises from an RNG failure inside ring's sign
        // call, which cannot be triggered deterministically through this
        // module's public API. Constructed directly to verify the message
        // wording, matching this codebase's `..._error_formats_correctly`
        // convention for defensive error variants (see e.g.
        // `node::tests::state_broadcast_error_formats_correctly`).
        assert!(SigningError::SigningFailed.to_string().contains("signing"));
    }

    #[test]
    fn verification_invalid_error_message_is_descriptive() {
        assert!(VerificationError::Invalid.to_string().contains("verification"));
    }
}
