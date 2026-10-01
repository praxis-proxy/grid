//! Enforced SPIFFE peer-mTLS verification for grid sites.
//!
//! A peer is trusted when its leaf chains to the grid CA and carries exactly one
//! SPIFFE name in this grid's trust domain. That verified name is the peer's
//! grid site identity, which the caller authorizes and attributes on.
//!
//! One trust decision ([`GridTrust`]) sits behind two rustls faces:
//! [`GridSpiffeClientVerifier`] (a server checking a client) and
//! [`GridSpiffeServerVerifier`] (a client checking the server it dials). SPIFFE
//! and pin are distinct verifier types and never cross-authenticate.
//!
//! FIPS follows the injected crypto provider: the verifier takes its algorithms
//! from the provider the consumer installs, so there is no ring path to leak
//! under an OpenSSL provider.

use std::sync::Arc;

use rustls::{
    DigitallySignedStruct, DistinguishedName, Error, RootCertStore, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::WebPkiSupportedAlgorithms,
    pki_types::{CertificateDer, ServerName, UnixTime, pem::PemObject as _},
    server::danger::{ClientCertVerified, ClientCertVerifier},
};

use crate::{VerifyError, generate::SPIFFE_TRUST_DOMAIN, verify::grid_spiffe_id};

/// The trust decision shared by both verifier faces.
#[derive(Debug)]
struct GridTrust {
    /// The grid CA(s) a peer chain must anchor to.
    roots: Arc<RootCertStore>,
    /// Signature-verification algorithms from the consumer's crypto provider.
    algorithms: WebPkiSupportedAlgorithms,
    /// The trust domain a peer's SPIFFE name must be in.
    expected_domain: String,
    /// CA subjects to hint to a connecting client, computed once.
    hint_subjects: Vec<DistinguishedName>,
}

impl GridTrust {
    /// Parse the grid CA bundle and hold the provider's algorithms.
    fn new(
        grid_ca_pem: &[u8],
        expected_domain: String,
        algorithms: WebPkiSupportedAlgorithms,
    ) -> Result<Self, VerifyError> {
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(grid_ca_pem) {
            let cert = cert.map_err(|_bad| VerifyError::MalformedCa)?;
            roots.add(cert).map_err(|_bad| VerifyError::MalformedCa)?;
        }
        if roots.is_empty() {
            return Err(VerifyError::MalformedCa);
        }
        let hint_subjects = roots.subjects();
        Ok(Self {
            roots: Arc::new(roots),
            algorithms,
            expected_domain,
            hint_subjects,
        })
    }

    /// The full trust decision, run entirely in the handshake: the leaf chains to
    /// the grid CA (provider algorithms, handshake clock), then carries exactly
    /// one SPIFFE name in this grid's domain. A SAN-rule failure fails the
    /// handshake, so a chain-valid but wrong-name cert never completes it.
    ///
    /// clientAuth/serverAuth key-usage is deliberately not split: a grid site has
    /// one peer identity used both ways, and the SPIFFE name is the authorization.
    fn verify_and_identify(
        &self,
        leaf: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<(), Error> {
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &rustls::server::ParsedCertificate::try_from(leaf)?,
            &self.roots,
            intermediates,
            now,
            self.algorithms.all,
        )?;
        grid_spiffe_id(leaf, &self.expected_domain).map_err(|reason| Error::General(reason.to_string()))?;
        Ok(())
    }

    /// The verified SPIFFE id of a leaf, under this verifier's trust domain.
    fn spiffe_id_from_cert(&self, leaf: &CertificateDer<'_>) -> Result<String, VerifyError> {
        grid_spiffe_id(leaf, &self.expected_domain)
    }

    /// Verify a TLS 1.2 handshake signature with the provider's algorithms.
    fn verify_tls12(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    /// Verify a TLS 1.3 handshake signature with the provider's algorithms.
    fn verify_tls13(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }
}

/// The verified SPIFFE id of the peer's leaf, or [`VerifyError::NoPeerCertificate`].
///
/// Fails closed on an absent or empty chain rather than indexing it, so a
/// resumed session that presents no certificate yields no identity.
fn identity_of(trust: &GridTrust, peer: Option<&[CertificateDer<'_>]>) -> Result<String, VerifyError> {
    let leaf = peer.and_then(<[_]>::first).ok_or(VerifyError::NoPeerCertificate)?;
    trust.spiffe_id_from_cert(leaf)
}

/// Verifies the server a grid client dials: an outbound peer-mTLS verifier.
///
/// Install on a rustls `ClientConfig` via `dangerous().set_certificate_verifier`.
/// The verifier authenticates that the server is a valid in-domain grid peer. The
/// caller authorizes which peer by comparing [`Self::spiffe_id_from_peer`] against
/// the site it meant to reach. Server name (SNI) is not checked, because identity
/// is the SPIFFE name and membership advertises an address no DNS name matches.
#[derive(Debug)]
pub struct GridSpiffeServerVerifier(GridTrust);

impl GridSpiffeServerVerifier {
    /// Build a verifier trusting `grid_ca_pem`, requiring names in `expected_domain`
    /// ([`SPIFFE_TRUST_DOMAIN`] for this grid), using `algorithms` from the
    /// consumer's crypto provider (`config.crypto_provider().signature_verification_algorithms`).
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError`] if the CA bundle is empty or unparseable.
    pub fn new(
        grid_ca_pem: &[u8],
        expected_domain: &str,
        algorithms: WebPkiSupportedAlgorithms,
    ) -> Result<Arc<Self>, VerifyError> {
        Ok(Arc::new(Self(GridTrust::new(
            grid_ca_pem,
            expected_domain.to_owned(),
            algorithms,
        )?)))
    }

    /// The verified SPIFFE id of the peer chain after a handshake, fail-closed on
    /// an absent chain. Same rule the handshake enforced, so it cannot diverge.
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError`] if the peer presented no certificate or its name is
    /// not one in-domain SPIFFE id.
    pub fn spiffe_id_from_peer(&self, peer: Option<&[CertificateDer<'_>]>) -> Result<String, VerifyError> {
        identity_of(&self.0, peer)
    }
}

impl ServerCertVerifier for GridSpiffeServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        self.0.verify_and_identify(end_entity, intermediates, now)?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.0.verify_tls12(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.0.verify_tls13(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.algorithms.supported_schemes()
    }
}

/// Verifies a client connecting to a grid server: an inbound peer-mTLS verifier.
///
/// Install on a rustls `ServerConfig` via `with_client_cert_verifier`. Client
/// authentication is mandatory: an anonymous client is refused. The verified
/// SPIFFE id is the connecting peer's grid site identity, read with
/// [`Self::spiffe_id_from_peer`].
#[derive(Debug)]
pub struct GridSpiffeClientVerifier(GridTrust);

impl GridSpiffeClientVerifier {
    /// Build a verifier trusting `grid_ca_pem`, requiring names in `expected_domain`,
    /// using `algorithms` from the consumer's crypto provider.
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError`] if the CA bundle is empty or unparseable.
    pub fn new(
        grid_ca_pem: &[u8],
        expected_domain: &str,
        algorithms: WebPkiSupportedAlgorithms,
    ) -> Result<Arc<Self>, VerifyError> {
        Ok(Arc::new(Self(GridTrust::new(
            grid_ca_pem,
            expected_domain.to_owned(),
            algorithms,
        )?)))
    }

    /// The verified SPIFFE id of the peer chain, fail-closed on an absent chain.
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError`] if the peer presented no certificate or its name is
    /// not one in-domain SPIFFE id.
    pub fn spiffe_id_from_peer(&self, peer: Option<&[CertificateDer<'_>]>) -> Result<String, VerifyError> {
        identity_of(&self.0, peer)
    }
}

impl ClientCertVerifier for GridSpiffeClientVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &self.0.hint_subjects
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        self.0.verify_and_identify(end_entity, intermediates, now)?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.0.verify_tls12(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.0.verify_tls13(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.algorithms.supported_schemes()
    }
}

/// The grid trust domain names default to [`SPIFFE_TRUST_DOMAIN`]. A caller that
/// runs more than one grid passes its own.
pub const DEFAULT_TRUST_DOMAIN: &str = SPIFFE_TRUST_DOMAIN;

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use rcgen::{CertificateParams, KeyPair, SanType, string::Ia5String};

    use super::*;
    use crate::{
        generate::{generate_ca, generate_expired_dns_cert, generate_site_cert},
        spiffe_id,
    };

    /// The provider's algorithms, from ring in tests.
    fn algs() -> WebPkiSupportedAlgorithms {
        rustls::crypto::ring::default_provider().signature_verification_algorithms
    }

    /// The first certificate in a PEM bundle, owned.
    fn der_of(pem: &str) -> CertificateDer<'static> {
        CertificateDer::pem_slice_iter(pem.as_bytes())
            .next()
            .expect("one certificate")
            .expect("valid certificate")
            .into_owned()
    }

    /// A self-signed leaf carrying exactly the given URI SANs, for the SAN rule.
    fn cert_with_uris(uris: &[&str]) -> CertificateDer<'static> {
        let key = KeyPair::generate().expect("key");
        let mut params = CertificateParams::default();
        for uri in uris {
            params
                .subject_alt_names
                .push(SanType::URI(Ia5String::try_from(*uri).expect("ia5")));
        }
        params.self_signed(&key).expect("self signed").der().clone()
    }

    fn a_name() -> ServerName<'static> {
        ServerName::try_from("site-a.grid.internal").expect("name")
    }

    #[test]
    fn a_grid_peer_verifies_and_its_verified_name_is_the_site_identity() {
        let ca = generate_ca("grid-ca").expect("ca");
        let leaf = der_of(&generate_site_cert(&ca, "site-a").expect("leaf").cert_pem);
        let verifier =
            GridSpiffeServerVerifier::new(ca.cert_pem.as_bytes(), DEFAULT_TRUST_DOMAIN, algs()).expect("verifier");

        verifier
            .verify_server_cert(&leaf, &[], &a_name(), &[], UnixTime::now())
            .expect("a grid peer should verify");
        let peer = [leaf];
        assert_eq!(
            verifier.spiffe_id_from_peer(Some(&peer)).expect("id"),
            spiffe_id("site-a"),
            "the verified name must be the site identity the caller authorizes and attributes on"
        );
    }

    #[test]
    fn the_inbound_face_verifies_the_same_grid_peer() {
        let ca = generate_ca("grid-ca").expect("ca");
        let leaf = der_of(&generate_site_cert(&ca, "site-b").expect("leaf").cert_pem);
        let verifier =
            GridSpiffeClientVerifier::new(ca.cert_pem.as_bytes(), DEFAULT_TRUST_DOMAIN, algs()).expect("verifier");

        verifier
            .verify_client_cert(&leaf, &[], UnixTime::now())
            .expect("a grid client should verify");
        assert!(verifier.client_auth_mandatory(), "client auth must be mandatory");
    }

    #[test]
    fn a_peer_from_another_ca_is_refused() {
        let ours = generate_ca("grid-ca").expect("ca");
        let theirs = generate_ca("other-ca").expect("other ca");
        let leaf = der_of(&generate_site_cert(&theirs, "site-a").expect("leaf").cert_pem);
        let verifier =
            GridSpiffeServerVerifier::new(ours.cert_pem.as_bytes(), DEFAULT_TRUST_DOMAIN, algs()).expect("verifier");

        verifier
            .verify_server_cert(&leaf, &[], &a_name(), &[], UnixTime::now())
            .expect_err("a cert from another CA must not verify");
    }

    #[test]
    fn a_name_outside_the_expected_domain_fails_in_the_handshake() {
        let ca = generate_ca("grid-ca").expect("ca");
        let leaf = der_of(&generate_site_cert(&ca, "site-a").expect("leaf").cert_pem);
        let verifier = GridSpiffeServerVerifier::new(ca.cert_pem.as_bytes(), "other.grid", algs()).expect("verifier");

        verifier
            .verify_server_cert(&leaf, &[], &a_name(), &[], UnixTime::now())
            .expect_err("a name in the wrong trust domain must fail the handshake, not just later");
    }

    #[test]
    fn an_expired_peer_is_refused() {
        let ca = generate_ca("grid-ca").expect("ca");
        let expired = der_of(
            &generate_expired_dns_cert(&ca, "site-a", "site-a.grid.internal")
                .expect("expired")
                .cert_pem,
        );
        let verifier =
            GridSpiffeServerVerifier::new(ca.cert_pem.as_bytes(), DEFAULT_TRUST_DOMAIN, algs()).expect("verifier");

        verifier
            .verify_server_cert(&expired, &[], &a_name(), &[], UnixTime::now())
            .expect_err("an expired cert must not verify");
    }

    #[test]
    fn an_absent_peer_chain_yields_no_identity() {
        let ca = generate_ca("grid-ca").expect("ca");
        let verifier =
            GridSpiffeServerVerifier::new(ca.cert_pem.as_bytes(), DEFAULT_TRUST_DOMAIN, algs()).expect("verifier");

        assert_eq!(verifier.spiffe_id_from_peer(None), Err(VerifyError::NoPeerCertificate));
        assert_eq!(
            verifier.spiffe_id_from_peer(Some(&[])),
            Err(VerifyError::NoPeerCertificate)
        );
    }

    #[test]
    fn the_san_rule_requires_exactly_one_spiffe_name() {
        let none = cert_with_uris(&[]);
        assert_eq!(
            grid_spiffe_id(&none, SPIFFE_TRUST_DOMAIN),
            Err(VerifyError::NotOneSpiffeName)
        );

        let two = cert_with_uris(&["spiffe://grid.internal/site/a", "spiffe://grid.internal/site/b"]);
        assert_eq!(
            grid_spiffe_id(&two, SPIFFE_TRUST_DOMAIN),
            Err(VerifyError::NotOneSpiffeName)
        );
    }

    #[test]
    fn the_san_rule_asserts_the_trust_domain() {
        let foreign = cert_with_uris(&["spiffe://other.grid/site/a"]);
        assert_eq!(
            grid_spiffe_id(&foreign, SPIFFE_TRUST_DOMAIN),
            Err(VerifyError::WrongTrustDomain {
                found: "spiffe://other.grid/site/a".to_owned(),
                expected: SPIFFE_TRUST_DOMAIN.to_owned(),
            })
        );

        let ours = cert_with_uris(&["spiffe://grid.internal/site/a"]);
        assert_eq!(
            grid_spiffe_id(&ours, SPIFFE_TRUST_DOMAIN).expect("in domain"),
            "spiffe://grid.internal/site/a"
        );
    }
}
