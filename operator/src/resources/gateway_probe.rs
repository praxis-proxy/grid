//! Typed gateway probe outcome and phase-transition contracts.
//!
//! All functions in this module are pure — no network I/O, no Kubernetes
//! access.  This separation keeps security-critical transition logic
//! directly testable without mocks.

use crate::crd::grid_site::GridSitePhase;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of canonical fingerprint pins (current + next).
pub(crate) const MAX_CANONICAL_PINS: usize = 2;

/// Expected length of a SHA-256 hex digest (lowercase, no separators).
const SHA256_HEX_LEN: usize = 64;

/// Maximum length of a DNS server name for egress TLS.
pub(crate) const MAX_SERVER_NAME_LEN: usize = 253;

/// Maximum status message length written to the CRD.
pub(crate) const MAX_STATUS_MESSAGE_LEN: usize = 256;

// ---------------------------------------------------------------------------
// GatewayProbeOutcome
// ---------------------------------------------------------------------------

/// Result of probing a remote gateway endpoint.
///
/// Each variant represents a distinct, machine-readable failure mode.
/// The controller maps these to phase transitions and stable status
/// reasons without exposing raw library error strings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GatewayProbeOutcome {
    /// TLS handshake succeeded, certificate chain valid, identity
    /// matched, and pin verified.
    Verified,

    /// No egress address configured on the [`GridSite`].
    ///
    /// [`GridSite`]: crate::crd::grid_site::GridSite
    AddressMissing,

    /// TCP connect timed out before a connection was established.
    ConnectTimeout,

    /// TCP connected, but the TLS handshake did not complete before the
    /// total probe deadline.
    HandshakeTimeout,

    /// TCP connect failed (connection refused, unreachable, DNS error).
    ConnectionFailed,

    /// Required trust material (CA cert, client cert, or client key)
    /// could not be loaded from the referenced Kubernetes Secret.
    TrustMaterialMissing,

    /// Trust material was present but structurally invalid (corrupt
    /// PEM, undecodable DER, private-key marker in a public cert).
    TrustMaterialInvalid,

    /// The TLS handshake failed at the protocol level (version
    /// mismatch, cipher negotiation failure, alert received).
    TlsProtocolError,

    /// The server certificate does not chain to the configured
    /// Grid trust root (CA).
    UntrustedIssuer,

    /// The server certificate's DNS SAN does not match the
    /// expected `serverName`.
    IdentityMismatch,

    /// The server certificate's `notAfter` is in the past.
    CertificateExpired,

    /// The server certificate's `notBefore` is in the future.
    CertificateNotYetValid,

    /// The server certificate's canonical fingerprint does not
    /// match any configured pin.
    PinMismatch,

    /// The certificate advertised via SWIM does not match any configured
    /// canonical pin.
    AdvertisedCertificateMismatch,

    /// Plaintext probe: TCP connect succeeded.
    PlaintextReachable,

    /// Plaintext probe: TCP connect failed or timed out.
    PlaintextUnreachable,
}

impl GatewayProbeOutcome {
    /// Bounded reason string for metrics labels.
    pub(crate) fn as_reason(&self) -> &'static str {
        match self {
            Self::Verified => "Verified",
            Self::AddressMissing => "AddressMissing",
            Self::ConnectTimeout => "ConnectTimeout",
            Self::HandshakeTimeout => "HandshakeTimeout",
            Self::ConnectionFailed => "ConnectionFailed",
            Self::TrustMaterialMissing => "TrustMaterialMissing",
            Self::TrustMaterialInvalid => "TrustMaterialInvalid",
            Self::TlsProtocolError => "TlsProtocolError",
            Self::UntrustedIssuer => "UntrustedIssuer",
            Self::IdentityMismatch => "IdentityMismatch",
            Self::CertificateExpired => "CertificateExpired",
            Self::CertificateNotYetValid => "CertificateNotYetValid",
            Self::PinMismatch => "PinMismatch",
            Self::AdvertisedCertificateMismatch => "AdvertisedCertMismatch",
            Self::PlaintextReachable => "PlaintextReachable",
            Self::PlaintextUnreachable => "PlaintextUnreachable",
        }
    }
}

// ---------------------------------------------------------------------------
// Phase transition
// ---------------------------------------------------------------------------

/// Deterministic phase transition produced by a probe outcome.
pub(crate) struct ProbeTransition {
    /// Target lifecycle phase.
    pub phase: GridSitePhase,
    /// Machine-readable reason code (bounded, stable).
    pub reason: &'static str,
    /// Human-readable message (bounded to [`MAX_STATUS_MESSAGE_LEN`]).
    pub message: String,
}

/// Map a probe outcome to a deterministic phase transition.
///
/// `current_phase` distinguishes demotion (Active → Connecting) from
/// stall (Connecting → Connecting) and recovery (Unreachable → Active).
pub(crate) fn probe_transition(current_phase: &GridSitePhase, outcome: &GatewayProbeOutcome) -> ProbeTransition {
    use GatewayProbeOutcome as O;

    match outcome {
        O::Verified => success("TlsVerified", "TLS handshake verified: chain, identity, and pin valid"),
        O::PlaintextReachable => trust_failure(
            "IdentityVerificationRequired",
            "TCP endpoint is reachable but did not provide identity-verified TLS",
        ),
        O::AddressMissing => connectivity_failure(current_phase, "EgressMissing", "no egress address configured"),
        O::ConnectTimeout => connectivity_failure(current_phase, "ConnectTimeout", "TCP connect timed out"),
        O::HandshakeTimeout => trust_failure("HandshakeTimeout", "TLS handshake timed out"),
        O::ConnectionFailed => connectivity_failure(current_phase, "ConnectionFailed", "TCP connection failed"),
        O::PlaintextUnreachable => connectivity_failure(current_phase, "PlaintextUnreachable", "TCP probe failed"),
        O::TrustMaterialMissing => trust_failure("TrustMaterialMissing", "required trust material not available"),
        O::TrustMaterialInvalid => trust_failure("TrustMaterialInvalid", "trust material is structurally invalid"),
        O::TlsProtocolError => trust_failure("TlsProtocolError", "TLS handshake failed at protocol level"),
        O::UntrustedIssuer => trust_failure("UntrustedIssuer", "server cert does not chain to Grid CA"),
        O::IdentityMismatch => trust_failure("IdentityMismatch", "server cert SAN does not match serverName"),
        O::CertificateExpired => trust_failure("CertificateExpired", "server certificate has expired"),
        O::CertificateNotYetValid => trust_failure("CertificateNotYetValid", "server certificate is not yet valid"),
        O::PinMismatch => trust_failure(
            "PinMismatch",
            "server cert fingerprint does not match any configured pin",
        ),
        // Recorded, not acted on: the live leaf already matched a pin above.
        O::AdvertisedCertificateMismatch => success(
            "AdvertisedCertMismatch",
            "SWIM-advertised certificate does not match any configured pin; live leaf verified",
        ),
    }
}

/// Success: always Active.
fn success(reason: &'static str, msg: &str) -> ProbeTransition {
    ProbeTransition {
        phase: GridSitePhase::Active,
        reason,
        message: truncate_message(msg),
    }
}

/// Connectivity failure: Active → Unreachable, others stay.
fn connectivity_failure(current: &GridSitePhase, reason: &'static str, msg: &str) -> ProbeTransition {
    ProbeTransition {
        phase: match current {
            GridSitePhase::Active => GridSitePhase::Unreachable,
            _ => current.clone(),
        },
        reason,
        message: truncate_message(msg),
    }
}

/// Trust/verification failure: always Connecting (demotion or stall).
fn trust_failure(reason: &'static str, msg: &str) -> ProbeTransition {
    ProbeTransition {
        phase: GridSitePhase::Connecting,
        reason,
        message: truncate_message(msg),
    }
}

/// Truncate a message to at most [`MAX_STATUS_MESSAGE_LEN`] characters.
fn truncate_message(msg: &str) -> String {
    if msg.chars().count() <= MAX_STATUS_MESSAGE_LEN {
        return msg.to_owned();
    }
    let mut truncated: String = msg.chars().take(MAX_STATUS_MESSAGE_LEN - 3).collect();
    truncated.push_str("...");
    truncated
}

// ---------------------------------------------------------------------------
// Canonical fingerprint
// ---------------------------------------------------------------------------

/// A validated canonical DER-certificate SHA-256 fingerprint.
///
/// Format: 64 lowercase hexadecimal characters (no colons, no prefix).
/// Computed as `hex(sha256(der_bytes))` where `der_bytes` are the raw
/// DER encoding of the leaf certificate.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CanonicalFingerprint(String);

impl CanonicalFingerprint {
    /// Parse and validate a canonical fingerprint string.
    ///
    /// Accepts exactly 64 lowercase hex characters.
    pub(crate) fn parse(s: &str) -> Result<Self, FingerprintError> {
        if s.len() != SHA256_HEX_LEN {
            return Err(FingerprintError::WrongLength(s.len()));
        }
        if !s.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()) {
            return Err(FingerprintError::InvalidCharacters);
        }
        Ok(Self(s.to_owned()))
    }

    /// Compute a canonical fingerprint from DER-encoded certificate bytes.
    pub(crate) fn from_der(der: &[u8]) -> Self {
        use sha2::{Digest as _, Sha256};
        let digest = Sha256::digest(der);
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        Self(hex)
    }

    /// The fingerprint as a hex string.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "used in test assertions; production callers use PartialEq")
    )]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// Errors from fingerprint parsing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FingerprintError {
    /// Fingerprint string is not exactly 64 characters.
    WrongLength(usize),
    /// Fingerprint contains non-lowercase-hex characters.
    InvalidCharacters,
}

impl std::fmt::Display for FingerprintError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongLength(len) => {
                write!(f, "canonical fingerprint must be 64 hex characters, got {len}")
            },
            Self::InvalidCharacters => {
                write!(f, "canonical fingerprint must contain only lowercase hex characters")
            },
        }
    }
}

/// Validate a bounded set of canonical fingerprint pins.
///
/// Returns parsed fingerprints or an error.  The set is bounded to
/// [`MAX_CANONICAL_PINS`] (current + next).
pub(crate) fn validate_canonical_pins(pins: &[String]) -> Result<Vec<CanonicalFingerprint>, CanonicalPinError> {
    if pins.is_empty() {
        return Err(CanonicalPinError::Empty);
    }
    if pins.len() > MAX_CANONICAL_PINS {
        return Err(CanonicalPinError::TooMany(pins.len()));
    }
    let mut parsed = Vec::with_capacity(pins.len());
    for (i, pin) in pins.iter().enumerate() {
        let fp = CanonicalFingerprint::parse(pin).map_err(|e| CanonicalPinError::Invalid { index: i, error: e })?;
        parsed.push(fp);
    }
    Ok(parsed)
}

/// Errors from canonical pin set validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CanonicalPinError {
    /// Pin list is empty.
    Empty,
    /// Pin list exceeds [`MAX_CANONICAL_PINS`].
    TooMany(usize),
    /// A specific pin failed validation.
    Invalid {
        /// Zero-based index in the list.
        index: usize,
        /// Parse error.
        error: FingerprintError,
    },
}

impl std::fmt::Display for CanonicalPinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "canonicalFingerprints must not be empty"),
            Self::TooMany(n) => write!(
                f,
                "canonicalFingerprints has {n} entries, maximum is {MAX_CANONICAL_PINS}"
            ),
            Self::Invalid { index, error } => {
                write!(f, "canonicalFingerprints[{index}]: {error}")
            },
        }
    }
}

/// Check whether a computed fingerprint matches any pin in the set.
pub(crate) fn fingerprint_matches_any(fp: &CanonicalFingerprint, pins: &[CanonicalFingerprint]) -> bool {
    pins.iter().any(|pin| pin == fp)
}

// ---------------------------------------------------------------------------
// Server name validation
// ---------------------------------------------------------------------------

/// Validate a DNS server name for egress TLS.
///
/// Rejects blank, oversized, IP-literal, and malformed values.
/// The name is used both as TLS SNI and for certificate SAN verification.
pub(crate) fn validate_server_name(name: &str) -> Result<(), ServerNameError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(ServerNameError::Blank);
    }
    if trimmed.len() > MAX_SERVER_NAME_LEN {
        return Err(ServerNameError::TooLong(trimmed.len()));
    }
    if trimmed.parse::<std::net::IpAddr>().is_ok() {
        return Err(ServerNameError::IpAddress);
    }
    if trimmed.starts_with('.') || trimmed.ends_with('.') {
        return Err(ServerNameError::Malformed);
    }
    if trimmed.contains("..") {
        return Err(ServerNameError::Malformed);
    }
    if !trimmed
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
    {
        return Err(ServerNameError::Malformed);
    }
    Ok(())
}

/// Errors from server name validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ServerNameError {
    /// Server name is blank or whitespace-only.
    Blank,
    /// Server name exceeds [`MAX_SERVER_NAME_LEN`].
    TooLong(usize),
    /// Server name is an IP address (use DNS names for SNI).
    IpAddress,
    /// Server name has invalid structure.
    Malformed,
}

impl std::fmt::Display for ServerNameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Blank => write!(f, "serverName must not be blank"),
            Self::TooLong(len) => write!(f, "serverName length {len} exceeds maximum {MAX_SERVER_NAME_LEN}"),
            Self::IpAddress => write!(f, "serverName must be a DNS name, not an IP address"),
            Self::Malformed => write!(f, "serverName contains invalid characters or structure"),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "tests"
)]
mod tests {
    use super::*;

    // ---- Phase transitions ----

    #[test]
    fn verified_promotes_to_active() {
        let t = probe_transition(&GridSitePhase::Connecting, &GatewayProbeOutcome::Verified);
        assert_eq!(t.phase, GridSitePhase::Active);
        assert_eq!(t.reason, "TlsVerified");
    }

    #[test]
    fn verified_keeps_active() {
        let t = probe_transition(&GridSitePhase::Active, &GatewayProbeOutcome::Verified);
        assert_eq!(t.phase, GridSitePhase::Active);
    }

    #[test]
    fn verified_recovers_from_unreachable() {
        let t = probe_transition(&GridSitePhase::Unreachable, &GatewayProbeOutcome::Verified);
        assert_eq!(t.phase, GridSitePhase::Active);
    }

    #[test]
    fn connect_timeout_demotes_active_to_unreachable() {
        let t = probe_transition(&GridSitePhase::Active, &GatewayProbeOutcome::ConnectTimeout);
        assert_eq!(t.phase, GridSitePhase::Unreachable);
        assert_eq!(t.reason, "ConnectTimeout");
    }

    #[test]
    fn connect_timeout_stalls_connecting() {
        let t = probe_transition(&GridSitePhase::Connecting, &GatewayProbeOutcome::ConnectTimeout);
        assert_eq!(t.phase, GridSitePhase::Connecting);
    }

    #[test]
    fn connection_failed_stalls_unreachable() {
        let t = probe_transition(&GridSitePhase::Unreachable, &GatewayProbeOutcome::ConnectionFailed);
        assert_eq!(t.phase, GridSitePhase::Unreachable);
    }

    #[test]
    fn untrusted_issuer_demotes_active_to_connecting() {
        let t = probe_transition(&GridSitePhase::Active, &GatewayProbeOutcome::UntrustedIssuer);
        assert_eq!(t.phase, GridSitePhase::Connecting);
        assert_eq!(t.reason, "UntrustedIssuer");
    }

    #[test]
    fn identity_mismatch_demotes_active_to_connecting() {
        let t = probe_transition(&GridSitePhase::Active, &GatewayProbeOutcome::IdentityMismatch);
        assert_eq!(t.phase, GridSitePhase::Connecting);
    }

    #[test]
    fn certificate_expired_demotes_active_to_connecting() {
        let t = probe_transition(&GridSitePhase::Active, &GatewayProbeOutcome::CertificateExpired);
        assert_eq!(t.phase, GridSitePhase::Connecting);
        assert_eq!(t.reason, "CertificateExpired");
    }

    #[test]
    fn pin_mismatch_demotes_active_to_connecting() {
        let t = probe_transition(&GridSitePhase::Active, &GatewayProbeOutcome::PinMismatch);
        assert_eq!(t.phase, GridSitePhase::Connecting);
        assert_eq!(t.reason, "PinMismatch");
    }

    #[test]
    fn advertised_cert_mismatch_keeps_active() {
        let t = probe_transition(
            &GridSitePhase::Active,
            &GatewayProbeOutcome::AdvertisedCertificateMismatch,
        );
        assert_eq!(
            t.phase,
            GridSitePhase::Active,
            "the live leaf already matched a pin; the advertised copy must not demote"
        );
        assert_eq!(
            t.reason, "AdvertisedCertMismatch",
            "the mismatch is still recorded in the reason"
        );
    }

    #[test]
    fn advertised_cert_mismatch_recovers_from_unreachable() {
        let t = probe_transition(
            &GridSitePhase::Unreachable,
            &GatewayProbeOutcome::AdvertisedCertificateMismatch,
        );
        assert_eq!(
            t.phase,
            GridSitePhase::Active,
            "the peer answered and verified, so it is no longer unreachable"
        );
    }

    #[test]
    fn advertised_cert_mismatch_does_not_block_active() {
        let t = probe_transition(
            &GridSitePhase::Connecting,
            &GatewayProbeOutcome::AdvertisedCertificateMismatch,
        );
        assert_eq!(
            t.phase,
            GridSitePhase::Active,
            "reached only after chain, SAN, and live-leaf pin verified"
        );
        assert_eq!(
            t.reason, "AdvertisedCertMismatch",
            "reason must be AdvertisedCertMismatch"
        );
    }

    #[test]
    fn trust_material_missing_stays_connecting() {
        let t = probe_transition(&GridSitePhase::Connecting, &GatewayProbeOutcome::TrustMaterialMissing);
        assert_eq!(t.phase, GridSitePhase::Connecting);
        assert_eq!(t.reason, "TrustMaterialMissing");
    }

    #[test]
    fn trust_material_invalid_stays_connecting() {
        let t = probe_transition(&GridSitePhase::Connecting, &GatewayProbeOutcome::TrustMaterialInvalid);
        assert_eq!(t.phase, GridSitePhase::Connecting);
    }

    #[test]
    fn tls_protocol_error_demotes_active_to_connecting() {
        let t = probe_transition(&GridSitePhase::Active, &GatewayProbeOutcome::TlsProtocolError);
        assert_eq!(t.phase, GridSitePhase::Connecting);
        assert_eq!(t.reason, "TlsProtocolError");
    }

    #[test]
    fn handshake_timeout_demotes_active_to_connecting() {
        let t = probe_transition(&GridSitePhase::Active, &GatewayProbeOutcome::HandshakeTimeout);
        assert_eq!(t.phase, GridSitePhase::Connecting);
        assert_eq!(t.reason, "HandshakeTimeout");
    }

    #[test]
    fn address_missing_demotes_active_to_unreachable() {
        let t = probe_transition(&GridSitePhase::Active, &GatewayProbeOutcome::AddressMissing);
        assert_eq!(t.phase, GridSitePhase::Unreachable);
    }

    #[test]
    fn address_missing_stalls_connecting() {
        let t = probe_transition(&GridSitePhase::Connecting, &GatewayProbeOutcome::AddressMissing);
        assert_eq!(t.phase, GridSitePhase::Connecting);
    }

    #[test]
    fn plaintext_reachable_cannot_promote_to_active() {
        let t = probe_transition(&GridSitePhase::Connecting, &GatewayProbeOutcome::PlaintextReachable);
        assert_eq!(t.phase, GridSitePhase::Connecting);
        assert_eq!(t.reason, "IdentityVerificationRequired");
    }

    #[test]
    fn plaintext_reachable_demotes_active_to_connecting() {
        let t = probe_transition(&GridSitePhase::Active, &GatewayProbeOutcome::PlaintextReachable);
        assert_eq!(t.phase, GridSitePhase::Connecting);
        assert_eq!(t.reason, "IdentityVerificationRequired");
    }

    #[test]
    fn plaintext_unreachable_demotes_active() {
        let t = probe_transition(&GridSitePhase::Active, &GatewayProbeOutcome::PlaintextUnreachable);
        assert_eq!(t.phase, GridSitePhase::Unreachable);
    }

    #[test]
    fn all_trust_failures_demote_active_to_connecting() {
        let trust_failures = [
            GatewayProbeOutcome::TrustMaterialMissing,
            GatewayProbeOutcome::TrustMaterialInvalid,
            GatewayProbeOutcome::HandshakeTimeout,
            GatewayProbeOutcome::TlsProtocolError,
            GatewayProbeOutcome::UntrustedIssuer,
            GatewayProbeOutcome::IdentityMismatch,
            GatewayProbeOutcome::CertificateExpired,
            GatewayProbeOutcome::CertificateNotYetValid,
            GatewayProbeOutcome::PinMismatch,
        ];
        for outcome in &trust_failures {
            let t = probe_transition(&GridSitePhase::Active, outcome);
            assert_eq!(
                t.phase,
                GridSitePhase::Connecting,
                "trust failure {outcome:?} from Active should demote to Connecting"
            );
        }
    }

    #[test]
    fn all_trust_failures_stall_connecting() {
        let trust_failures = [
            GatewayProbeOutcome::TrustMaterialMissing,
            GatewayProbeOutcome::TrustMaterialInvalid,
            GatewayProbeOutcome::HandshakeTimeout,
            GatewayProbeOutcome::TlsProtocolError,
            GatewayProbeOutcome::UntrustedIssuer,
            GatewayProbeOutcome::IdentityMismatch,
            GatewayProbeOutcome::CertificateExpired,
            GatewayProbeOutcome::CertificateNotYetValid,
            GatewayProbeOutcome::PinMismatch,
        ];
        for outcome in &trust_failures {
            let t = probe_transition(&GridSitePhase::Connecting, outcome);
            assert_eq!(
                t.phase,
                GridSitePhase::Connecting,
                "trust failure {outcome:?} from Connecting should stay Connecting"
            );
        }
    }

    // ---- Message truncation ----

    #[test]
    fn truncate_short_message() {
        let msg = truncate_message("short");
        assert_eq!(msg, "short");
    }

    #[test]
    fn truncate_long_message() {
        let long = "a".repeat(300);
        let msg = truncate_message(&long);
        assert_eq!(msg.chars().count(), MAX_STATUS_MESSAGE_LEN);
        assert!(msg.ends_with("..."));
    }

    // ---- Canonical fingerprint ----

    #[test]
    fn valid_fingerprint_accepted() {
        let hex = "a".repeat(64);
        let fp = CanonicalFingerprint::parse(&hex);
        assert!(fp.is_ok());
        assert_eq!(fp.unwrap().as_str(), hex);
    }

    #[test]
    fn fingerprint_wrong_length_rejected() {
        let short = "ab".repeat(16);
        assert_eq!(short.len(), 32);
        let err = CanonicalFingerprint::parse(&short).unwrap_err();
        assert_eq!(err, FingerprintError::WrongLength(32));
    }

    #[test]
    fn fingerprint_uppercase_rejected() {
        let hex = format!("{}A{}", "a".repeat(32), "b".repeat(31));
        assert_eq!(hex.len(), 64);
        let err = CanonicalFingerprint::parse(&hex).unwrap_err();
        assert_eq!(err, FingerprintError::InvalidCharacters);
    }

    #[test]
    fn fingerprint_non_hex_rejected() {
        let bad = format!("{}g{}", "a".repeat(32), "b".repeat(31));
        assert_eq!(bad.len(), 64);
        let err = CanonicalFingerprint::parse(&bad).unwrap_err();
        assert_eq!(err, FingerprintError::InvalidCharacters);
    }

    #[test]
    fn fingerprint_from_der_roundtrip() {
        let der = b"test certificate der bytes";
        let fp = CanonicalFingerprint::from_der(der);
        assert_eq!(fp.as_str().len(), 64);
        let parsed = CanonicalFingerprint::parse(fp.as_str()).unwrap();
        assert_eq!(fp, parsed);
    }

    #[test]
    fn fingerprint_matches_any_positive() {
        let fp = CanonicalFingerprint::from_der(b"cert-a");
        let pins = vec![CanonicalFingerprint::from_der(b"cert-b"), fp.clone()];
        assert!(fingerprint_matches_any(&fp, &pins));
    }

    #[test]
    fn fingerprint_matches_any_negative() {
        let fp = CanonicalFingerprint::from_der(b"cert-a");
        let pins = vec![
            CanonicalFingerprint::from_der(b"cert-b"),
            CanonicalFingerprint::from_der(b"cert-c"),
        ];
        assert!(!fingerprint_matches_any(&fp, &pins));
    }

    // ---- Canonical pin set validation ----

    #[test]
    fn valid_single_pin() {
        let pins = vec!["a".repeat(64)];
        assert!(validate_canonical_pins(&pins).is_ok());
    }

    #[test]
    fn valid_two_pins() {
        let pins = vec!["a".repeat(64), "b".repeat(64)];
        let result = validate_canonical_pins(&pins).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn empty_pins_rejected() {
        let err = validate_canonical_pins(&[]).unwrap_err();
        assert_eq!(err, CanonicalPinError::Empty);
    }

    #[test]
    fn three_pins_rejected() {
        let pins = vec!["a".repeat(64), "b".repeat(64), "c".repeat(64)];
        let err = validate_canonical_pins(&pins).unwrap_err();
        assert_eq!(err, CanonicalPinError::TooMany(3));
    }

    #[test]
    fn invalid_pin_in_set_rejected() {
        let pins = vec!["a".repeat(64), "short".to_owned()];
        let err = validate_canonical_pins(&pins).unwrap_err();
        match err {
            CanonicalPinError::Invalid { index: 1, .. } => {},
            other => panic!("expected Invalid at index 1, got {other:?}"),
        }
    }

    // ---- Server name validation ----

    #[test]
    fn valid_server_name() {
        assert!(validate_server_name("east-provider.grid.internal").is_ok());
    }

    #[test]
    fn valid_simple_name() {
        assert!(validate_server_name("provider").is_ok());
    }

    #[test]
    fn blank_server_name_rejected() {
        assert_eq!(validate_server_name("").unwrap_err(), ServerNameError::Blank);
        assert_eq!(validate_server_name("   ").unwrap_err(), ServerNameError::Blank);
    }

    #[test]
    fn oversized_server_name_rejected() {
        let long = "a".repeat(254);
        assert_eq!(validate_server_name(&long).unwrap_err(), ServerNameError::TooLong(254));
    }

    #[test]
    fn ip_address_rejected() {
        assert_eq!(
            validate_server_name("192.168.1.1").unwrap_err(),
            ServerNameError::IpAddress
        );
        assert_eq!(validate_server_name("::1").unwrap_err(), ServerNameError::IpAddress);
    }

    #[test]
    fn leading_dot_rejected() {
        assert_eq!(
            validate_server_name(".example.com").unwrap_err(),
            ServerNameError::Malformed
        );
    }

    #[test]
    fn trailing_dot_rejected() {
        assert_eq!(
            validate_server_name("example.com.").unwrap_err(),
            ServerNameError::Malformed
        );
    }

    #[test]
    fn double_dot_rejected() {
        assert_eq!(
            validate_server_name("example..com").unwrap_err(),
            ServerNameError::Malformed
        );
    }

    #[test]
    fn underscore_rejected() {
        assert_eq!(
            validate_server_name("bad_name.com").unwrap_err(),
            ServerNameError::Malformed
        );
    }

    // ---- Safety: no secrets in reason strings or messages ----

    #[test]
    #[expect(clippy::too_many_lines, reason = "exhaustive check of all 16 variants × 3 phases")]
    fn all_reasons_are_bounded_static_strings() {
        let all = [
            GatewayProbeOutcome::Verified,
            GatewayProbeOutcome::AddressMissing,
            GatewayProbeOutcome::ConnectTimeout,
            GatewayProbeOutcome::HandshakeTimeout,
            GatewayProbeOutcome::ConnectionFailed,
            GatewayProbeOutcome::TrustMaterialMissing,
            GatewayProbeOutcome::TrustMaterialInvalid,
            GatewayProbeOutcome::TlsProtocolError,
            GatewayProbeOutcome::UntrustedIssuer,
            GatewayProbeOutcome::IdentityMismatch,
            GatewayProbeOutcome::CertificateExpired,
            GatewayProbeOutcome::CertificateNotYetValid,
            GatewayProbeOutcome::PinMismatch,
            GatewayProbeOutcome::AdvertisedCertificateMismatch,
            GatewayProbeOutcome::PlaintextReachable,
            GatewayProbeOutcome::PlaintextUnreachable,
        ];
        assert_eq!(all.len(), 16, "must cover all 16 variants");

        let forbidden = [
            "BEGIN CERTIFICATE",
            "END CERTIFICATE",
            "PRIVATE KEY",
            "Bearer ",
            "-----",
        ];

        for outcome in &all {
            let reason = outcome.as_reason();
            assert!(!reason.is_empty(), "{outcome:?} has empty reason");
            assert!(reason.len() <= 40, "{outcome:?} reason {reason:?} exceeds 40 chars");
            for pat in &forbidden {
                assert!(
                    !reason.contains(pat),
                    "{outcome:?} reason contains forbidden pattern {pat:?}"
                );
            }

            for phase in &[GridSitePhase::Pending, GridSitePhase::Connecting, GridSitePhase::Active] {
                let t = probe_transition(phase, outcome);
                for pat in &forbidden {
                    assert!(
                        !t.message.contains(pat),
                        "{outcome:?} from {phase:?}: message contains {pat:?}: {:?}",
                        t.message
                    );
                }
                assert!(
                    t.message.len() <= MAX_STATUS_MESSAGE_LEN,
                    "{outcome:?} from {phase:?}: message exceeds {MAX_STATUS_MESSAGE_LEN} chars"
                );
            }
        }
    }
}
