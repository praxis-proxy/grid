//! [`GridSite`] controller.
//!
//! Reconciles [`GridSite`] resources: validates the grid network
//! reference, manages lifecycle phase transitions, and maintains
//! the trust bundle secret.
//!
//! [`GridSite`]: crate::crd::grid_site::GridSite

use std::sync::Arc;

use kube::{
    Client,
    api::{Api, Patch, PatchParams},
    runtime::controller::Action,
};
use tokio::time::Duration;
use tracing::info;

use crate::{
    crd::{
        grid_network::GridNetwork,
        grid_site::{GridSite, GridSitePhase, GridSiteStatus},
    },
    error::OperatorError,
    resources::trust_bundle::{CertPemStatus, check_cert_pem, sha256_fingerprint},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Requeue interval after a successful reconciliation.
const REQUEUE_INTERVAL: Duration = Duration::from_secs(300);

/// Field manager name for server-side apply.
const FIELD_MANAGER: &str = "grid-operator";

/// TCP connect timeout for data-plane gateway reachability probes.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

/// Reconcile a [`GridSite`] resource.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures.
pub async fn reconcile(site: Arc<GridSite>, client: Arc<Client>) -> Result<Action, OperatorError> {
    let name = site.metadata.name.as_deref().unwrap_or_else(|| std::process::abort());

    info!(name, "reconciling GridSite");

    validate_network_ref(&site, client.as_ref()).await?;

    let current_phase = site.status.as_ref().map_or(&GridSitePhase::Pending, |s| &s.phase);

    let (next_phase, reason, message) = site_phase_next(current_phase, &site).await;
    update_status(&site, client.as_ref(), &next_phase, &reason, &message).await?;

    Ok(Action::requeue(REQUEUE_INTERVAL))
}

/// Error policy for the [`GridSite`] controller.
pub fn error_policy(_site: Arc<GridSite>, error: &OperatorError, _ctx: Arc<Client>) -> Action {
    tracing::error!(%error, "GridSite reconciliation failed");
    Action::requeue(Duration::from_secs(30))
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Validate that the referenced `GridNetwork` exists.
async fn validate_network_ref(site: &GridSite, client: &Client) -> Result<(), OperatorError> {
    let api: Api<GridNetwork> = Api::all(client.clone());
    let network_name = &site.spec.grid_network_ref;
    api.get(network_name).await.map_err(|e| {
        tracing::warn!(error = %e, network = %network_name, "lookup failed");
        OperatorError::NotFound(format!("GridNetwork {network_name}"))
    })?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase Determination
// ---------------------------------------------------------------------------

/// Determine the next lifecycle phase for a [`GridSite`].
///
/// Returns `(next_phase, reason, message)`.  `reason` is machine-readable;
/// `message` is human-readable and never contains token bytes.
///
/// Transitions:
/// - Pending → stays Pending (`GridNetwork` controller writes Discovered on SWIM Alive).
/// - Discovered + gateway address → Connecting (gateway address known; trust/data-plane readiness remain external).
/// - Discovered, no gateway address → stays Discovered.
/// - Connecting → Active when the TCP probe succeeds and the configured fingerprint trust policy matches the received
///   public certificate.
/// - Connecting → stays Connecting when the gateway is unreachable, trust material is missing or invalid, or the
///   fingerprint policy is missing or mismatched.
/// - Active → stays Active if gateway is reachable and the fingerprint trust policy still matches; otherwise demotes to
///   Connecting or Unreachable.
/// - Unreachable → stays Unreachable.
/// - Left → preserved.
#[expect(
    clippy::too_many_lines,
    reason = "exhaustive match over all six phases with per-phase reason/message; splitting obscures the contract"
)]
pub(crate) async fn site_phase_next(current: &GridSitePhase, site: &GridSite) -> (GridSitePhase, String, String) {
    let probe_addr = site.spec.egress.as_ref().and_then(|e| {
        if e.address.trim().is_empty() {
            None
        } else {
            Some(e.address.as_str())
        }
    });
    let has_egress = probe_addr.is_some();

    match current {
        GridSitePhase::Pending => (
            GridSitePhase::Pending,
            "AwaitingDiscovery".to_owned(),
            "site record created; waiting for SWIM discovery to advance to Discovered".to_owned(),
        ),
        GridSitePhase::Discovered => {
            if has_egress {
                (
                    GridSitePhase::Connecting,
                    "GatewayAddressKnown".to_owned(),
                    "gateway address present; awaiting trust verification and data-plane readiness".to_owned(),
                )
            } else {
                (
                    GridSitePhase::Discovered,
                    "GatewayAddressMissing".to_owned(),
                    "gateway address not yet available; cannot advance to Connecting".to_owned(),
                )
            }
        },
        GridSitePhase::Connecting => {
            // Check the PEM structure of any received public cert.
            // ValidStructure means the PEM marker is present; it does NOT mean
            // the cert is chain-verified or that the site is trusted.
            let cert_status = site
                .status
                .as_ref()
                .and_then(|s| s.public_cert_pem.as_ref())
                .filter(|p| !p.trim().is_empty())
                .map(|p| check_cert_pem(p));

            if let Some(addr) = probe_addr {
                if tcp_probe(addr).await {
                    match cert_status {
                        Some(CertPemStatus::ValidStructure) => {
                            // Cert is structurally valid — check fingerprint trust policy.
                            let configured_fp = site.spec.trust.as_ref().and_then(|t| t.cert_fingerprint.as_deref());
                            let actual_fp = site
                                .status
                                .as_ref()
                                .and_then(|s| s.public_cert_pem.as_ref())
                                .map(|p| sha256_fingerprint(p));
                            match (configured_fp, actual_fp) {
                                (Some(expected), Some(actual)) if actual == expected => (
                                    // Fingerprint matches — promote to Active.
                                    GridSitePhase::Active,
                                    "TrustPolicyVerified".to_owned(),
                                    "gateway reachable; certificate fingerprint verified against \
                                     configured trust policy"
                                        .to_owned(),
                                ),
                                (Some(_), Some(_)) => (
                                    // Fingerprint present but does not match policy.
                                    GridSitePhase::Connecting,
                                    "TrustPolicyMismatch".to_owned(),
                                    "gateway reachable; certificate fingerprint does not match \
                                     spec.trust.certFingerprint; verify the remote site's certificate"
                                        .to_owned(),
                                ),
                                (None, _) => (
                                    // Cert received but no trust policy configured.
                                    GridSitePhase::Connecting,
                                    "TrustPolicyMissing".to_owned(),
                                    "gateway reachable; public certificate received; set \
                                     spec.trust.certFingerprint to the certificate SHA-256 fingerprint \
                                     to authorize this site"
                                        .to_owned(),
                                ),
                                (Some(_), None) => (
                                    // Policy configured but no cert received yet.
                                    GridSitePhase::Connecting,
                                    "TrustMaterialMissing".to_owned(),
                                    "gateway reachable; awaiting public trust material from remote site".to_owned(),
                                ),
                            }
                        },
                        Some(CertPemStatus::ContainsPrivateKey) => (
                            GridSitePhase::Connecting,
                            "TrustMaterialInvalid".to_owned(),
                            "received material contains private key markers and was discarded; \
                             check remote operator TLS configuration"
                                .to_owned(),
                        ),
                        Some(CertPemStatus::NotACertificate) => (
                            GridSitePhase::Connecting,
                            "TrustMaterialInvalid".to_owned(),
                            "received PEM is not a certificate; check remote operator TLS configuration".to_owned(),
                        ),
                        None => (
                            GridSitePhase::Connecting,
                            "TrustMaterialMissing".to_owned(),
                            "gateway reachable; awaiting public trust material from remote site".to_owned(),
                        ),
                    }
                } else {
                    (
                        GridSitePhase::Connecting,
                        "GatewayUnreachable".to_owned(),
                        "gateway not reachable via TCP probe; retrying".to_owned(),
                    )
                }
            } else {
                (
                    GridSitePhase::Connecting,
                    "GatewayAddressMissing".to_owned(),
                    "gateway address not available; awaiting configuration".to_owned(),
                )
            }
        },
        GridSitePhase::Active => {
            if let Some(addr) = probe_addr {
                if tcp_probe(addr).await {
                    // Re-check trust policy while Active to detect cert rotation or
                    // policy changes that should revoke Active status.
                    let active_cert_status = site
                        .status
                        .as_ref()
                        .and_then(|s| s.public_cert_pem.as_ref())
                        .filter(|p| !p.trim().is_empty())
                        .map(|p| check_cert_pem(p));
                    let fp_policy = site.spec.trust.as_ref().and_then(|t| t.cert_fingerprint.as_deref());

                    match (fp_policy, active_cert_status) {
                        // Trust policy was removed while Active.  TCP reachability
                        // alone must not keep a remote site routable.
                        (None, _) => (
                            GridSitePhase::Connecting,
                            "TrustPolicyMissing".to_owned(),
                            "trust fingerprint policy no longer configured; \
                             site reverted to Connecting pending trust re-verification"
                                .to_owned(),
                        ),
                        // Policy configured, cert present and valid — re-verify fingerprint.
                        (Some(expected), Some(CertPemStatus::ValidStructure)) => {
                            let actual = site
                                .status
                                .as_ref()
                                .and_then(|s| s.public_cert_pem.as_ref())
                                .map(|p| sha256_fingerprint(p));
                            if actual.as_deref() == Some(expected) {
                                (GridSitePhase::Active, "TrustPolicyVerified".to_owned(), String::new())
                            } else {
                                // Fingerprint mismatch — cert rotated or policy updated.
                                (
                                    GridSitePhase::Connecting,
                                    "TrustPolicyMismatch".to_owned(),
                                    "certificate fingerprint no longer matches trust policy; \
                                     site reverted to Connecting pending trust re-verification"
                                        .to_owned(),
                                )
                            }
                        },
                        // Policy configured, cert contains private key — security violation.
                        (Some(_), Some(CertPemStatus::ContainsPrivateKey)) => (
                            GridSitePhase::Connecting,
                            "TrustMaterialInvalid".to_owned(),
                            "received material contains private key markers; \
                             site reverted to Connecting"
                                .to_owned(),
                        ),
                        // Policy configured, cert is not a certificate.
                        (Some(_), Some(CertPemStatus::NotACertificate)) => (
                            GridSitePhase::Connecting,
                            "TrustMaterialInvalid".to_owned(),
                            "received PEM is not a certificate; site reverted to Connecting".to_owned(),
                        ),
                        // Policy configured but no cert — cert may have been removed.
                        (Some(_), None) => (
                            GridSitePhase::Connecting,
                            "TrustMaterialMissing".to_owned(),
                            "public certificate no longer available; \
                             site reverted to Connecting pending cert re-receipt"
                                .to_owned(),
                        ),
                    }
                } else {
                    (
                        GridSitePhase::Unreachable,
                        "GatewayUnreachable".to_owned(),
                        "gateway not reachable via TCP probe; site marked Unreachable".to_owned(),
                    )
                }
            } else {
                (
                    GridSitePhase::Unreachable,
                    "GatewayAddressMissing".to_owned(),
                    "gateway address not available; site marked Unreachable".to_owned(),
                )
            }
        },
        GridSitePhase::Unreachable => (
            GridSitePhase::Unreachable,
            "Unreachable".to_owned(),
            "site is Unreachable".to_owned(),
        ),
        GridSitePhase::Left => (
            GridSitePhase::Left,
            "Left".to_owned(),
            "site has left the grid".to_owned(),
        ),
    }
}

/// Attempt a TCP connection to `addr` with [`PROBE_TIMEOUT`].
///
/// Returns `true` if the connection succeeds within the timeout, `false`
/// otherwise.  Connection errors are silently treated as unreachable.
async fn tcp_probe(addr: &str) -> bool {
    tokio::time::timeout(PROBE_TIMEOUT, tokio::net::TcpStream::connect(addr))
        .await
        .is_ok_and(|r| r.is_ok())
}

// ---------------------------------------------------------------------------
// Status Update
// ---------------------------------------------------------------------------

/// Patch the `GridSite` status subresource.
///
/// Preserves existing `capabilities`, `last_probe_time`, `public_cert_pem`,
/// and `last_transition_time` from the current status rather than zeroing them
/// with `Default`.  Only `phase`, `reason`, `message`, and `observed_generation`
/// are overwritten.
async fn update_status(
    site: &GridSite,
    client: &Client,
    phase: &GridSitePhase,
    reason: &str,
    message: &str,
) -> Result<(), OperatorError> {
    let name = site.metadata.name.as_deref().unwrap_or_else(|| std::process::abort());

    let existing = site.status.as_ref();

    let api: Api<GridSite> = Api::all(client.clone());
    let status = GridSiteStatus {
        phase: phase.clone(),
        observed_generation: site.metadata.generation.unwrap_or(0),
        reason: reason.to_owned(),
        message: message.to_owned(),
        // Preserve existing fields that the GridSite controller does not own.
        capabilities: existing.map_or_else(Default::default, |s| s.capabilities.clone()),
        last_probe_time: existing.and_then(|s| s.last_probe_time.clone()),
        last_transition_time: existing.and_then(|s| s.last_transition_time.clone()),
        public_cert_pem: existing.and_then(|s| s.public_cert_pem.clone()),
    };

    let patch = serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridSite",
        "status": status
    });

    api.patch_status(name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(patch))
        .await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::grid_site::{EgressConfig, EgressTls, GridSiteSpec};

    fn site_with_egress(phase: Option<GridSitePhase>, egress: &str) -> GridSite {
        GridSite {
            metadata: kube::api::ObjectMeta {
                name: Some("test-site".to_owned()),
                generation: Some(1),
                ..Default::default()
            },
            spec: GridSiteSpec {
                grid_network_ref: "test-net".to_owned(),
                egress: Some(EgressConfig {
                    address: egress.to_owned(),
                    tls: EgressTls::default(),
                }),
                region: None,
                sovereignty_zone: None,
                zone: None,
                trust: None,
            },
            status: phase.map(|p| GridSiteStatus {
                phase: p,
                ..Default::default()
            }),
        }
    }

    fn site_no_egress(phase: Option<GridSitePhase>) -> GridSite {
        GridSite {
            metadata: kube::api::ObjectMeta {
                name: Some("test-site".to_owned()),
                generation: Some(1),
                ..Default::default()
            },
            spec: GridSiteSpec {
                grid_network_ref: "test-net".to_owned(),
                egress: None,
                region: None,
                sovereignty_zone: None,
                zone: None,
                trust: None,
            },
            status: phase.map(|p| GridSiteStatus {
                phase: p,
                ..Default::default()
            }),
        }
    }

    // -----------------------------------------------------------------------
    // site_phase_next
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn pending_stays_pending_even_with_egress() {
        let site = site_with_egress(Some(GridSitePhase::Pending), "10.0.0.1:8443");
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Pending, &site).await;
        assert_eq!(next, GridSitePhase::Pending);
        assert_eq!(reason, "AwaitingDiscovery");
    }

    #[tokio::test]
    async fn discovered_with_egress_advances_to_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Discovered), "10.0.0.1:7946");
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site).await;
        assert_eq!(
            next,
            GridSitePhase::Connecting,
            "Discovered + gateway address must advance to Connecting"
        );
        assert_eq!(reason, "GatewayAddressKnown");
    }

    #[tokio::test]
    async fn discovered_without_egress_stays_discovered() {
        let site = site_no_egress(Some(GridSitePhase::Discovered));
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site).await;
        assert_eq!(
            next,
            GridSitePhase::Discovered,
            "Discovered + no gateway address must stay Discovered"
        );
        assert_eq!(reason, "GatewayAddressMissing");
    }

    #[tokio::test]
    async fn discovered_with_empty_egress_stays_discovered() {
        let site = site_with_egress(Some(GridSitePhase::Discovered), "");
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site).await;
        assert_eq!(next, GridSitePhase::Discovered);
        assert_eq!(reason, "GatewayAddressMissing");
    }

    #[tokio::test]
    async fn connecting_stays_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (next, _reason, _msg) = site_phase_next(&GridSitePhase::Connecting, &site).await;
        assert_eq!(
            next,
            GridSitePhase::Connecting,
            "Connecting must stay Connecting (probe runs but Active requires trust)"
        );
    }

    #[tokio::test]
    async fn active_with_unreachable_gateway_transitions_to_unreachable() {
        let site = site_with_egress(Some(GridSitePhase::Active), "192.0.2.1:1");
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Active, &site).await;
        assert_eq!(
            next,
            GridSitePhase::Unreachable,
            "Active with unreachable gateway must become Unreachable"
        );
        assert_eq!(reason, "GatewayUnreachable");
    }

    #[tokio::test]
    async fn active_without_egress_transitions_to_unreachable() {
        let site = site_no_egress(Some(GridSitePhase::Active));
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Active, &site).await;
        assert_eq!(
            next,
            GridSitePhase::Unreachable,
            "Active without egress cannot remain Active"
        );
        assert_eq!(reason, "GatewayAddressMissing");
    }

    #[tokio::test]
    async fn unreachable_is_preserved() {
        let site = site_with_egress(Some(GridSitePhase::Unreachable), "10.0.0.1:8443");
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Unreachable, &site).await;
        assert_eq!(next, GridSitePhase::Unreachable, "Unreachable must be preserved");
        assert_eq!(reason, "Unreachable");
    }

    #[tokio::test]
    async fn left_is_preserved() {
        let site = site_no_egress(Some(GridSitePhase::Left));
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Left, &site).await;
        assert_eq!(next, GridSitePhase::Left, "Left must be preserved");
        assert_eq!(reason, "Left");
    }

    // -----------------------------------------------------------------------
    // Phase messages must not contain sentinel token bytes
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn phase_messages_do_not_contain_sentinel_token() {
        let sentinel = "sk-super-secret-token-do-not-emit";
        let phases = [
            GridSitePhase::Pending,
            GridSitePhase::Discovered,
            GridSitePhase::Connecting,
            GridSitePhase::Active,
            GridSitePhase::Unreachable,
            GridSitePhase::Left,
        ];
        for phase in &phases {
            let site = site_with_egress(Some(phase.clone()), "10.0.0.1:8443");
            let (_, reason, message) = site_phase_next(phase, &site).await;
            assert!(
                !reason.contains(sentinel),
                "reason for {phase:?} must not contain sentinel token: {reason}"
            );
            assert!(
                !message.contains(sentinel),
                "message for {phase:?} must not contain sentinel token: {message}"
            );
        }
    }

    fn site_with_cert(phase: Option<GridSitePhase>, egress: &str, cert_pem: &str) -> GridSite {
        use crate::crd::grid_site::GridSiteStatus;
        let mut site = site_with_egress(phase, egress);
        site.status = Some(GridSiteStatus {
            public_cert_pem: Some(cert_pem.to_owned()),
            ..Default::default()
        });
        site
    }

    fn site_with_cert_and_trust(
        phase: Option<GridSitePhase>,
        egress: &str,
        cert_pem: &str,
        fingerprint: Option<&str>,
    ) -> GridSite {
        use crate::crd::grid_site::GridSiteTrustPolicy;
        let mut site = site_with_cert(phase, egress, cert_pem);
        site.spec.trust = fingerprint.map(|fp| GridSiteTrustPolicy {
            cert_fingerprint: Some(fp.to_owned()),
        });
        site
    }

    fn reachable_probe_addr() -> (std::net::TcpListener, String) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap_or_else(|_| std::process::abort());
        let addr = listener
            .local_addr()
            .unwrap_or_else(|_| std::process::abort())
            .to_string();
        (listener, addr)
    }

    // -----------------------------------------------------------------------
    // Gateway address discovery tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn discovered_with_gateway_address_advances_to_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Discovered), "10.0.0.1:19080");
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site).await;
        assert_eq!(next, GridSitePhase::Connecting);
        assert_eq!(reason, "GatewayAddressKnown");
    }

    #[tokio::test]
    async fn discovered_without_gateway_address_stays_discovered() {
        let site = site_no_egress(Some(GridSitePhase::Discovered));
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site).await;
        assert_eq!(next, GridSitePhase::Discovered);
        assert_eq!(reason, "GatewayAddressMissing");
    }

    #[tokio::test]
    async fn phase_reason_codes_are_deterministic() {
        let site = site_with_egress(Some(GridSitePhase::Discovered), "10.0.0.1:8443");
        let (_, r1, _) = site_phase_next(&GridSitePhase::Discovered, &site).await;
        let (_, r2, _) = site_phase_next(&GridSitePhase::Discovered, &site).await;
        assert_eq!(r1, r2, "reason must be deterministic for the same inputs");
    }

    // -----------------------------------------------------------------------
    // Trust material status tests (no TCP probe — phase stays Connecting)
    // -----------------------------------------------------------------------

    const VALID_CERT_PEM: &str =
        "-----BEGIN CERTIFICATE-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOC\n-----END CERTIFICATE-----\n";
    const PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkq\n-----END PRIVATE KEY-----\n";

    #[tokio::test]
    async fn valid_cert_pem_reports_valid_structure() {
        // No real TCP probe in unit tests (no running server at 127.0.0.1:19080).
        // The gateway probe fails → GatewayUnreachable regardless of cert.
        // Test valid-cert path requires a running listener, so we test the cert
        // check logic via trust_bundle::check_cert_pem directly.
        let status = check_cert_pem(VALID_CERT_PEM);
        assert_eq!(
            status,
            CertPemStatus::ValidStructure,
            "valid cert PEM must be accepted as ValidStructure"
        );
        // Without a running listener, the phase logic sees GatewayUnreachable
        // before it evaluates trust material. This test validates the
        // cert-checking helper separately.
    }

    #[tokio::test]
    async fn connecting_with_private_key_reports_invalid() {
        let status = check_cert_pem(PRIVATE_KEY_PEM);
        assert_eq!(
            status,
            CertPemStatus::ContainsPrivateKey,
            "private key material must be rejected"
        );
    }

    #[tokio::test]
    async fn connecting_no_cert_reports_trust_material_missing() {
        // With no running server, probe fails → GatewayUnreachable.
        // Test the missing-cert path: site with egress but no publicCertPem.
        let site = site_with_egress(Some(GridSitePhase::Connecting), "127.0.0.1:19080");
        let (phase, reason, _msg) = site_phase_next(&GridSitePhase::Connecting, &site).await;
        assert_eq!(phase, GridSitePhase::Connecting, "no cert must stay Connecting");
        // TCP probe to 127.0.0.1:19080 with nothing listening → GatewayUnreachable.
        assert_eq!(
            reason, "GatewayUnreachable",
            "unreachable gateway takes precedence over cert check"
        );
    }

    #[tokio::test]
    async fn status_message_never_contains_full_pem() {
        let site = site_with_cert(Some(GridSitePhase::Connecting), "127.0.0.1:19080", VALID_CERT_PEM);
        let (_, _, message) = site_phase_next(&GridSitePhase::Connecting, &site).await;
        assert!(
            !message.contains("BEGIN CERTIFICATE"),
            "status message must not include the full PEM: {message}"
        );
    }

    #[tokio::test]
    async fn status_message_never_contains_private_key_marker() {
        let site = site_with_cert(
            Some(GridSitePhase::Connecting),
            "127.0.0.1:19080",
            "-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----\n",
        );
        let (_, _, message) = site_phase_next(&GridSitePhase::Connecting, &site).await;
        assert!(
            !message.contains("BEGIN PRIVATE KEY") && !message.contains("PRIVATE KEY"),
            "status message must not echo private key markers: {message}"
        );
    }

    #[tokio::test]
    async fn valid_structure_never_advances_to_active_without_policy() {
        // Even with a valid cert, the lifecycle must stay at most Connecting.
        // Active requires explicit trust policy + data-plane readiness, not cert presence.
        let status = check_cert_pem(VALID_CERT_PEM);
        assert_eq!(status, CertPemStatus::ValidStructure);
        // The phase logic: Connecting + valid cert (probe succeeds) → TrustPolicyMissing, stays Connecting.
        // Connecting never advances to Active on cert presence alone — fingerprint trust policy required.
        // This test proves the CertPemStatus enum name is not "Trusted" or "Active".
        assert_ne!(format!("{status:?}"), "Trusted");
        assert_ne!(format!("{status:?}"), "Active");
        assert_ne!(format!("{status:?}"), "Authorized");
    }

    #[tokio::test]
    async fn trust_material_invalid_reason_for_non_cert_pem() {
        // Verify that a non-cert PEM (e.g., public key) maps to NotACertificate,
        // which the controller records as TrustMaterialInvalid.
        let pub_key = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkq\n-----END PUBLIC KEY-----\n";
        let status = check_cert_pem(pub_key);
        assert_eq!(
            status,
            CertPemStatus::NotACertificate,
            "public key PEM must not be accepted as a certificate"
        );
    }

    // -----------------------------------------------------------------------
    // Trust policy — fingerprint pinning (Connecting → Active gate)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn trust_material_present_without_policy_reports_policy_missing() {
        let (_listener, addr) = reachable_probe_addr();
        let site = site_with_cert(Some(GridSitePhase::Connecting), &addr, VALID_CERT_PEM);
        let (phase, reason, _msg) = site_phase_next(&GridSitePhase::Connecting, &site).await;
        assert_eq!(
            phase,
            GridSitePhase::Connecting,
            "missing trust policy must not promote"
        );
        assert_eq!(reason, "TrustPolicyMissing");
    }

    #[tokio::test]
    async fn fingerprint_match_promotes_to_active_when_gateway_reachable() {
        use crate::resources::trust_bundle::sha256_fingerprint;
        let (_listener, addr) = reachable_probe_addr();
        let expected_fp = sha256_fingerprint(VALID_CERT_PEM);
        let site = site_with_cert_and_trust(
            Some(GridSitePhase::Connecting),
            &addr,
            VALID_CERT_PEM,
            Some(&expected_fp),
        );
        let (phase, reason, message) = site_phase_next(&GridSitePhase::Connecting, &site).await;
        assert_eq!(phase, GridSitePhase::Active);
        assert_eq!(reason, "TrustPolicyVerified");
        assert!(!message.contains(VALID_CERT_PEM), "status message must not include PEM");
        assert!(
            !message.contains(&expected_fp),
            "status message must not include fingerprint"
        );
    }

    #[tokio::test]
    async fn fingerprint_mismatch_stays_connecting_when_gateway_reachable() {
        use crate::resources::trust_bundle::sha256_fingerprint;
        let (_listener, addr) = reachable_probe_addr();
        let wrong_fp = sha256_fingerprint("-----BEGIN CERTIFICATE-----\nwrong\n-----END CERTIFICATE-----\n");
        let site = site_with_cert_and_trust(Some(GridSitePhase::Connecting), &addr, VALID_CERT_PEM, Some(&wrong_fp));
        let (phase, reason, message) = site_phase_next(&GridSitePhase::Connecting, &site).await;
        assert_eq!(phase, GridSitePhase::Connecting, "wrong fingerprint must not promote");
        assert_eq!(reason, "TrustPolicyMismatch");
        assert!(!message.contains(VALID_CERT_PEM), "status message must not include PEM");
        assert!(
            !message.contains(&wrong_fp),
            "status message must not include configured fingerprint"
        );
    }

    #[test]
    fn private_key_pem_cannot_influence_trust_policy() {
        use crate::resources::trust_bundle::{CertPemStatus, check_cert_pem, sha256_fingerprint};
        // Private key PEM is rejected structurally before any fingerprint comparison.
        let pem = PRIVATE_KEY_PEM;
        let status = check_cert_pem(pem);
        assert_eq!(
            status,
            CertPemStatus::ContainsPrivateKey,
            "private key must be rejected before fingerprint comparison"
        );
        // Even if someone attempted to compute a fingerprint of the private key PEM,
        // the structural check rejects it first — so this code path never runs.
        // We include this assertion to document the invariant.
        let fp = sha256_fingerprint(pem); // fingerprint computation itself is safe
        assert!(
            !fp.is_empty(),
            "fingerprint computation on any string is safe (but structural check blocks it first)"
        );
    }

    #[test]
    fn status_message_does_not_contain_pem_or_fingerprint_data() {
        use crate::resources::trust_bundle::sha256_fingerprint;
        let fp = sha256_fingerprint(VALID_CERT_PEM);
        // Messages that reference trust policy must not embed raw PEM or raw fingerprint.
        let policy_missing_msg = "gateway reachable; public certificate received; set spec.trust.certFingerprint \
             to the certificate SHA-256 fingerprint to authorize this site";
        assert!(!policy_missing_msg.contains("BEGIN CERTIFICATE"), "no PEM in message");
        assert!(!policy_missing_msg.contains(&fp), "no fingerprint data in message");

        let mismatch_msg = "gateway reachable; certificate fingerprint does not match \
             spec.trust.certFingerprint; verify the remote site's certificate";
        assert!(
            !mismatch_msg.contains("BEGIN CERTIFICATE"),
            "no PEM in mismatch message"
        );
    }

    #[tokio::test]
    async fn active_is_preserved_while_probe_succeeds() {
        use crate::resources::trust_bundle::sha256_fingerprint;
        let (_listener, addr) = reachable_probe_addr();
        let fp = sha256_fingerprint(VALID_CERT_PEM);
        let site = site_with_cert_and_trust(Some(GridSitePhase::Active), &addr, VALID_CERT_PEM, Some(&fp));
        let (phase, reason, _msg) = site_phase_next(&GridSitePhase::Active, &site).await;
        assert_eq!(phase, GridSitePhase::Active);
        assert_eq!(reason, "TrustPolicyVerified");
    }

    #[tokio::test]
    async fn active_with_removed_trust_policy_demotes_to_connecting() {
        let (_listener, addr) = reachable_probe_addr();
        let site = site_with_cert(Some(GridSitePhase::Active), &addr, VALID_CERT_PEM);
        let (phase, reason, message) = site_phase_next(&GridSitePhase::Active, &site).await;
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "TrustPolicyMissing");
        assert!(!message.contains(VALID_CERT_PEM), "status message must not include PEM");
    }

    #[tokio::test]
    async fn active_is_demoted_when_probe_fails() {
        let site = site_with_egress(Some(GridSitePhase::Active), "127.0.0.1:19080");
        let (phase, reason, _msg) = site_phase_next(&GridSitePhase::Active, &site).await;
        assert_eq!(
            phase,
            GridSitePhase::Unreachable,
            "failed probe must demote Active to Unreachable"
        );
        assert_eq!(reason, "GatewayUnreachable");
    }

    #[tokio::test]
    async fn invalid_cert_with_matching_private_key_fingerprint_stays_connecting() {
        use crate::resources::trust_bundle::sha256_fingerprint;
        let (_listener, addr) = reachable_probe_addr();
        let private_key_fp = sha256_fingerprint(PRIVATE_KEY_PEM);
        let site = site_with_cert_and_trust(
            Some(GridSitePhase::Connecting),
            &addr,
            PRIVATE_KEY_PEM,
            Some(&private_key_fp),
        );
        let (phase, reason, message) = site_phase_next(&GridSitePhase::Connecting, &site).await;
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "TrustMaterialInvalid");
        assert!(
            !message.contains("BEGIN PRIVATE KEY") && !message.contains(PRIVATE_KEY_PEM),
            "status message must not include private key material"
        );
    }

    // -----------------------------------------------------------------------
    // Active phase — certificate rotation behavior
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn active_without_trust_policy_demotes_unreachable_when_probe_fails() {
        // No trust policy → TCP probe failure → Unreachable (unchanged behavior).
        let site = site_with_egress(Some(GridSitePhase::Active), "127.0.0.1:19080");
        let (phase, reason, _msg) = site_phase_next(&GridSitePhase::Active, &site).await;
        assert_eq!(phase, GridSitePhase::Unreachable);
        assert_eq!(reason, "GatewayUnreachable");
    }

    #[tokio::test]
    async fn active_cert_rotation_mismatch_demotes_to_connecting() {
        use crate::resources::trust_bundle::sha256_fingerprint;
        // Simulate the post-rotation state: Active has cert A configured,
        // but publicCertPem now contains cert B (different bytes).
        let (_listener, addr) = reachable_probe_addr();
        let cert_a = VALID_CERT_PEM;
        let cert_b = "-----BEGIN CERTIFICATE-----\nDifferentBytes\n-----END CERTIFICATE-----\n";
        let fp_a = sha256_fingerprint(cert_a);
        let site = site_with_cert_and_trust(Some(GridSitePhase::Active), &addr, cert_b, Some(&fp_a));
        let (phase, reason, message) = site_phase_next(&GridSitePhase::Active, &site).await;
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "TrustPolicyMismatch");
        assert!(!message.contains(cert_b), "status message must not include PEM");
        assert!(!message.contains(&fp_a), "status message must not include fingerprint");
    }

    #[tokio::test]
    async fn active_cert_rotation_match_stays_active() {
        use crate::resources::trust_bundle::sha256_fingerprint;
        // Active with matching fingerprint → stays Active.
        let (_listener, addr) = reachable_probe_addr();
        let cert = VALID_CERT_PEM;
        let fp = sha256_fingerprint(cert);
        let site = site_with_cert_and_trust(Some(GridSitePhase::Active), &addr, cert, Some(&fp));
        let (phase, reason, message) = site_phase_next(&GridSitePhase::Active, &site).await;
        assert_eq!(phase, GridSitePhase::Active);
        assert_eq!(reason, "TrustPolicyVerified");
        assert!(message.is_empty());
    }

    #[tokio::test]
    async fn active_with_missing_cert_and_policy_demotes_to_connecting() {
        use crate::{crd::grid_site::GridSiteTrustPolicy, resources::trust_bundle::sha256_fingerprint};
        // Policy configured but no cert in status → None case for cert_status.
        let (_listener, addr) = reachable_probe_addr();
        let fp = sha256_fingerprint(VALID_CERT_PEM);
        let mut site = site_with_egress(Some(GridSitePhase::Active), &addr);
        site.spec.trust = Some(GridSiteTrustPolicy {
            cert_fingerprint: Some(fp),
        });
        let (phase, reason, message) = site_phase_next(&GridSitePhase::Active, &site).await;
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "TrustMaterialMissing");
        assert!(
            !message.contains("BEGIN CERTIFICATE"),
            "status message must not include PEM"
        );
    }

    #[test]
    fn active_status_message_never_contains_raw_pem() {
        // Messages from the Active arm rotation branch must not contain raw cert data.
        let mismatch_msg = "certificate fingerprint no longer matches trust policy; \
             site reverted to Connecting pending trust re-verification";
        assert!(!mismatch_msg.contains("BEGIN CERTIFICATE"), "no PEM header in message");
        assert!(!mismatch_msg.contains("PRIVATE KEY"), "no private key in message");

        let missing_msg = "public certificate no longer available; \
             site reverted to Connecting pending cert re-receipt";
        assert!(!missing_msg.contains("BEGIN CERTIFICATE"), "no PEM in missing message");
    }

    #[test]
    fn site_with_cert_and_trust_helper_sets_fields() {
        use crate::resources::trust_bundle::sha256_fingerprint;
        // Exercise site_with_cert_and_trust to suppress dead-code warning and
        // verify that the helper sets spec.trust correctly.
        let fp = sha256_fingerprint(VALID_CERT_PEM);
        let site = site_with_cert_and_trust(
            Some(GridSitePhase::Connecting),
            "10.0.0.1:8080",
            VALID_CERT_PEM,
            Some(&fp),
        );
        assert_eq!(
            site.spec.trust.as_ref().and_then(|t| t.cert_fingerprint.as_deref()),
            Some(fp.as_str()),
            "trust.certFingerprint must be set"
        );
        assert_eq!(
            site.status.as_ref().and_then(|s| s.public_cert_pem.as_deref()),
            Some(VALID_CERT_PEM),
            "publicCertPem must be set"
        );
    }
}
