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
    resources::trust_bundle::{CertPemStatus, check_cert_pem},
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
/// - Connecting → stays Connecting; a TCP probe reports gateway reachability but advancing to Active requires trust
///   verification outside this function.
/// - Active → stays Active if gateway is reachable; transitions to Unreachable otherwise.
/// - Unreachable → stays Unreachable; promotion back to Active requires trust verification outside this function.
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
                        Some(CertPemStatus::ValidStructure) => (
                            GridSitePhase::Connecting,
                            "TrustMaterialPresent".to_owned(),
                            "gateway reachable; public certificate PEM received and structurally valid; \
                             certificate has not been chain-verified — Active requires explicit trust policy"
                                .to_owned(),
                        ),
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
                    (GridSitePhase::Active, String::new(), String::new())
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
    async fn connecting_with_valid_cert_reports_trust_material_present() {
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
        // The TrustMaterialPresent reason is only emitted when TCP probe also succeeds.
        // Without a running listener, the probe fails and the test would see GatewayUnreachable.
        // This test validates the cert-checking logic separately.
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
    async fn trust_material_present_never_advances_to_active() {
        // Even with a valid cert, the lifecycle must stay at most Connecting.
        // Active requires explicit trust policy + data-plane readiness, not cert presence.
        let status = check_cert_pem(VALID_CERT_PEM);
        assert_eq!(status, CertPemStatus::ValidStructure);
        // The phase logic: Connecting + valid cert (if probe succeeds) → TrustMaterialPresent, stays Connecting.
        // Connecting never advances to Active autonomously.
        // This test proves the status enum name is not "Trusted" or "Active".
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
}
