//! [`GridSite`] controller.
//!
//! Reconciles [`GridSite`] resources: validates the grid network
//! reference, manages lifecycle phase transitions, and maintains
//! the trust bundle secret.
//!
//! [`GridSite`]: crate::crd::grid_site::GridSite

use std::sync::Arc;

use k8s_openapi::api::core::v1::ObjectReference;
use kube::{
    Client, Resource as _,
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::Action,
        events::{Event, EventType, Recorder, Reporter},
    },
};
use rustls::pki_types::ServerName;
use tokio::time::Duration;
use tracing::info;
use zeroize::Zeroizing;

use crate::{
    crd::{
        grid_network::GridNetwork,
        grid_site::{EgressTlsMode, GridSite, GridSitePhase, GridSiteStatus},
    },
    error::OperatorError,
    resources::{
        gateway_probe::{
            CanonicalFingerprint, GatewayProbeOutcome, probe_transition, validate_canonical_pins, validate_server_name,
        },
        secret::read_secret_bytes,
        tls_probe::{
            build_tls_config, first_cert_der_from_pem, parse_ca_roots, parse_client_certs, parse_private_key,
            probe_gateway,
        },
    },
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Requeue interval after a successful reconciliation.
///
/// Kept at 60 s so that Secret or trust-policy rotation is observed
/// within one minute without requiring a dedicated Secret watch.
const REQUEUE_INTERVAL: Duration = Duration::from_secs(60);

/// TCP connect timeout for plaintext gateway reachability probes.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

/// Reconcile a [`GridSite`] resource.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures.
#[expect(
    clippy::large_stack_frames,
    clippy::too_many_lines,
    reason = "TLS material loading + event recording requires intermediaries; \
              splitting hides the reconciliation flow"
)]
pub async fn reconcile(site: Arc<GridSite>, client: Arc<Client>) -> Result<Action, OperatorError> {
    let name = site.metadata.name.as_deref().unwrap_or_else(|| std::process::abort());

    let reporter = Reporter {
        controller: "grid-site-controller".into(),
        instance: None,
    };
    let object_ref = site.object_ref(&());
    let recorder = Recorder::new(client.as_ref().clone(), reporter);

    info!(name, "reconciling GridSite");

    let network = fetch_network(&site, client.as_ref()).await?;
    let current_phase = site.status.as_ref().map_or(&GridSitePhase::Pending, |s| &s.phase);

    let outcome = if needs_probe(current_phase) {
        let start = std::time::Instant::now();
        let result = evaluate_gateway(&site, client.as_ref(), &network).await;
        let tls_mode = if is_plaintext_transport(&site) {
            "Plaintext"
        } else {
            "Mutual"
        };
        crate::metrics::record_probe(result.as_reason(), tls_mode, start.elapsed());
        Some(result)
    } else {
        None
    };

    let probed = outcome.is_some();
    let (next_phase, reason, message) = site_phase_next(current_phase, &site, outcome.as_ref());
    Box::pin(update_status(
        &site,
        client.as_ref(),
        &next_phase,
        &reason,
        &message,
        probed,
        &recorder,
        &object_ref,
    ))
    .await?;

    Ok(Action::requeue(REQUEUE_INTERVAL))
}

/// Error policy for the [`GridSite`] controller.
pub fn error_policy(_site: Arc<GridSite>, error: &OperatorError, _ctx: Arc<Client>) -> Action {
    tracing::error!(%error, "GridSite reconciliation failed");
    Action::requeue(Duration::from_secs(30))
}

// ---------------------------------------------------------------------------
// Network lookup
// ---------------------------------------------------------------------------

/// Fetch the referenced [`GridNetwork`].
async fn fetch_network(site: &GridSite, client: &Client) -> Result<GridNetwork, OperatorError> {
    let api: Api<GridNetwork> = Api::all(client.clone());
    let network_name = &site.spec.grid_network_ref;
    api.get(network_name).await.map_err(|e| {
        tracing::warn!(error = %e, network = %network_name, "lookup failed");
        OperatorError::NotFound(format!("GridNetwork {network_name}"))
    })
}

/// Whether the current phase requires a gateway probe.
fn needs_probe(phase: &GridSitePhase) -> bool {
    matches!(
        phase,
        GridSitePhase::Connecting | GridSitePhase::Active | GridSitePhase::Unreachable
    )
}

// ---------------------------------------------------------------------------
// Phase Determination
// ---------------------------------------------------------------------------

/// Determine the next lifecycle phase for a [`GridSite`].
///
/// Pure function: the caller supplies the probe `outcome` (from
/// [`evaluate_gateway`]) for phases that require it.  Phases that do
/// not need a probe (`Pending`, `Discovered`, `Left`) ignore `outcome`.
///
/// Returns `(next_phase, reason, message)`.  `reason` is machine-readable;
/// `message` is human-readable and never contains private material.
#[expect(
    clippy::too_many_lines,
    reason = "match arms are individually trivial; splitting would fragment the state machine"
)]
pub(crate) fn site_phase_next(
    current: &GridSitePhase,
    site: &GridSite,
    outcome: Option<&GatewayProbeOutcome>,
) -> (GridSitePhase, String, String) {
    let has_egress_address = site.spec.egress.as_ref().is_some_and(|e| !e.address.trim().is_empty());

    match current {
        GridSitePhase::Pending => (
            GridSitePhase::Pending,
            "AwaitingDiscovery".to_owned(),
            "site record created; waiting for SWIM discovery to advance to Discovered".to_owned(),
        ),
        GridSitePhase::Discovered => {
            if has_egress_address {
                (
                    GridSitePhase::Connecting,
                    "GatewayAddressKnown".to_owned(),
                    "gateway address present; awaiting control-plane trust verification".to_owned(),
                )
            } else {
                (
                    GridSitePhase::Discovered,
                    "GatewayAddressMissing".to_owned(),
                    "gateway address not yet available; cannot advance to Connecting".to_owned(),
                )
            }
        },
        GridSitePhase::Left => (
            GridSitePhase::Left,
            "Left".to_owned(),
            "site has left the grid".to_owned(),
        ),
        _ => {
            let outcome = outcome.unwrap_or(&GatewayProbeOutcome::AddressMissing);
            let t = probe_transition(current, outcome);
            (t.phase, t.reason.to_owned(), t.message)
        },
    }
}

// ---------------------------------------------------------------------------
// Gateway evaluation
// ---------------------------------------------------------------------------

/// Evaluate the gateway health of a [`GridSite`].
///
/// For plaintext transport, performs a bounded TCP probe for diagnostics.
/// Plaintext reachability never makes a site routing-eligible.
/// For TLS transport, loads trust material from Kubernetes Secrets
/// and performs a bounded TLS handshake with certificate verification.
///
/// Never leaks private key material in the returned outcome.
async fn evaluate_gateway(site: &GridSite, client: &Client, network: &GridNetwork) -> GatewayProbeOutcome {
    let probe_addr = site.spec.egress.as_ref().and_then(|e| {
        if e.address.trim().is_empty() {
            None
        } else {
            Some(e.address.as_str())
        }
    });

    let Some(addr) = probe_addr else {
        return GatewayProbeOutcome::AddressMissing;
    };

    if is_plaintext_transport(site) {
        return if tcp_probe(addr).await {
            GatewayProbeOutcome::PlaintextReachable
        } else {
            GatewayProbeOutcome::PlaintextUnreachable
        };
    }

    match build_probe_config_from_secrets(site, addr, client, network).await {
        Ok(config) => probe_gateway(&config).await,
        Err(outcome) => outcome,
    }
}

/// SWIM-advertised leaf DER, or `None` when absent or unparseable.
///
/// Gossiped, so unparseable is ignored: an `Err` would skip the authenticating handshake.
fn advertised_leaf_der(site: &GridSite) -> Option<Vec<u8>> {
    site.status
        .as_ref()
        .and_then(|s| s.public_cert_pem.as_ref())
        .and_then(|pem| match first_cert_der_from_pem(pem) {
            Ok(der) => Some(der),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    site = site.metadata.name.as_deref().unwrap_or_default(),
                    "ignoring unparseable advertised certificate"
                );
                None
            },
        })
}

/// Build a `ProbeConfig` by loading trust material from Kubernetes
/// Secrets referenced by the `GridNetwork`.
///
/// Returns a `GatewayProbeOutcome` on failure so the caller can
/// report the precise failure mode.
#[expect(
    clippy::too_many_lines,
    reason = "linear secret-loading sequence; splitting would fragment error provenance"
)]
#[expect(
    clippy::large_stack_frames,
    reason = "TLS material parsing requires several Vec<u8> intermediaries"
)]
async fn build_probe_config_from_secrets(
    site: &GridSite,
    addr: &str,
    client: &Client,
    network: &GridNetwork,
) -> Result<crate::resources::tls_probe::ProbeConfig, GatewayProbeOutcome> {
    use GatewayProbeOutcome as O;

    let ca_ref = network.spec.tls.ca_secret_ref.as_ref().ok_or(O::TrustMaterialMissing)?;
    let ca_bytes = read_secret_bytes(client, ca_ref, "ca.crt")
        .await
        .map_err(|_err| O::TrustMaterialMissing)?
        .into_bytes()
        .ok_or(O::TrustMaterialMissing)?;
    let roots = parse_ca_roots(&ca_bytes).map_err(|_err| O::TrustMaterialInvalid)?;

    let secret_ref = network
        .spec
        .tls
        .site_secret_ref
        .as_ref()
        .ok_or(O::TrustMaterialMissing)?;
    let cert_bytes = read_secret_bytes(client, secret_ref, "tls.crt")
        .await
        .map_err(|_err| O::TrustMaterialMissing)?
        .into_bytes()
        .ok_or(O::TrustMaterialMissing)?;
    let key_bytes = Zeroizing::new(
        read_secret_bytes(client, secret_ref, "tls.key")
            .await
            .map_err(|_err| O::TrustMaterialMissing)?
            .into_bytes()
            .ok_or(O::TrustMaterialMissing)?,
    );
    let client_certs = parse_client_certs(&cert_bytes).map_err(|_err| O::TrustMaterialInvalid)?;
    let client_key = parse_private_key(&key_bytes).map_err(|_err| O::TrustMaterialInvalid)?;

    let tls_config =
        build_tls_config(roots, Some(client_certs), Some(client_key)).map_err(|_err| O::TrustMaterialInvalid)?;

    let server_name_str = site
        .spec
        .egress
        .as_ref()
        .and_then(|e| e.tls.server_name.as_deref())
        .ok_or(O::TrustMaterialMissing)?;
    validate_server_name(server_name_str).map_err(|_err| O::TrustMaterialInvalid)?;
    let server_name = ServerName::try_from(server_name_str.to_owned()).map_err(|_err| O::TrustMaterialInvalid)?;

    let pins = resolve_pins(site)?;

    let advertised = advertised_leaf_der(site);

    Ok(crate::resources::tls_probe::ProbeConfig {
        address: addr.to_owned(),
        tls_config,
        server_name,
        pins,
        advertised_leaf_der: advertised,
    })
}

/// Resolve the canonical fingerprint pins from the [`GridSite`] trust policy.
///
/// Enforces mutual exclusion between `certFingerprint` (legacy) and
/// `canonicalFingerprints` (canonical).  When only the legacy field is set,
/// the probe fails closed — migration to canonical format is required.
///
/// Missing pin policy is reported separately from malformed pin policy so
/// operators can distinguish incomplete bootstrap from invalid configuration.
fn resolve_pins(site: &GridSite) -> Result<Vec<CanonicalFingerprint>, GatewayProbeOutcome> {
    use GatewayProbeOutcome as O;

    let Some(trust) = site.spec.trust.as_ref() else {
        return Err(O::TrustMaterialMissing);
    };

    let has_legacy = trust.cert_fingerprint.is_some();
    let has_canonical = trust.canonical_fingerprints.as_ref().is_some_and(|v| !v.is_empty());

    if has_legacy && has_canonical {
        tracing::warn!("certFingerprint and canonicalFingerprints are mutually exclusive");
        return Err(O::TrustMaterialInvalid);
    }

    if has_legacy {
        tracing::warn!("certFingerprint is deprecated; migrate to canonicalFingerprints");
        return Err(O::TrustMaterialInvalid);
    }

    match trust.canonical_fingerprints.as_ref() {
        Some(fps) => validate_canonical_pins(fps).map_err(|e| {
            tracing::warn!(error = %e, "canonical pin validation failed");
            O::TrustMaterialInvalid
        }),
        None => Err(O::TrustMaterialMissing),
    }
}

/// Bounded label for a [`GridSitePhase`] value in metrics.
fn phase_label(phase: &GridSitePhase) -> &'static str {
    match phase {
        GridSitePhase::Pending => "Pending",
        GridSitePhase::Discovered => "Discovered",
        GridSitePhase::Connecting => "Connecting",
        GridSitePhase::Active => "Active",
        GridSitePhase::Unreachable => "Unreachable",
        GridSitePhase::Left => "Left",
    }
}

/// Whether the site's egress transport is plaintext (no TLS).
fn is_plaintext_transport(site: &GridSite) -> bool {
    site.spec
        .egress
        .as_ref()
        .is_some_and(|e| e.tls.mode == EgressTlsMode::Plaintext)
}

/// Attempt a TCP connection to `addr` with [`PROBE_TIMEOUT`].
///
/// Returns `true` if the connection succeeds within the timeout, `false`
/// otherwise.  Used only for plaintext transport probes.
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
/// Patches only fields owned by this controller. `capabilities` and
/// `public_cert_pem` are owned by SWIM reconciliation and are deliberately
/// omitted. Updates `last_probe_time` when a probe was executed and
/// `last_transition_time` when the phase changes.
#[expect(
    clippy::too_many_lines,
    reason = "linear status-patching sequence; splitting would fragment field ownership"
)]
#[expect(
    clippy::too_many_arguments,
    clippy::large_stack_frames,
    clippy::cognitive_complexity,
    reason = "status patch requires phase, reason, message, and probe flag from the reconcile caller; \
              CAS conflict handling + mutation detection adds branches"
)]
async fn update_status(
    site: &GridSite,
    client: &Client,
    phase: &GridSitePhase,
    reason: &str,
    message: &str,
    probed: bool,
    recorder: &Recorder,
    object_ref: &ObjectReference,
) -> Result<(), OperatorError> {
    let name = site.metadata.name.as_deref().unwrap_or_else(|| std::process::abort());

    let existing = site.status.as_ref();
    let current_phase = existing.map(|s| &s.phase);
    let phase_changed = current_phase != Some(phase);

    let now = rfc3339_now();
    let probe_time = if probed {
        now.clone()
    } else {
        existing.and_then(|s| s.last_probe_time.clone())
    };
    let transition_time = if phase_changed {
        now
    } else {
        existing.and_then(|s| s.last_transition_time.clone())
    };

    let api: Api<GridSite> = Api::all(client.clone());
    let status = GridSiteStatus {
        phase: phase.clone(),
        observed_generation: site.metadata.generation.unwrap_or(0),
        reason: reason.to_owned(),
        message: message.to_owned(),
        capabilities: existing.map_or_else(Default::default, |s| s.capabilities.clone()),
        last_probe_time: probe_time,
        last_transition_time: transition_time,
        public_cert_pem: existing.and_then(|s| s.public_cert_pem.clone()),
    };

    if !grid_site_status_needs_update(existing, &status) {
        return Ok(());
    }

    // Patch only fields owned by this controller. The GridNetwork controller
    // updates capabilities and publicCertPem independently; replacing the
    // complete status object here could overwrite a newer SWIM observation.
    //
    // Include metadata.resourceVersion as a CAS precondition so the API
    // server returns 409 Conflict if another replica already wrote a newer
    // version. On conflict we yield silently — the informer will deliver
    // the updated object on the next reconcile.
    let rv = site.metadata.resource_version.as_deref();
    let patch = grid_site_owned_status_patch(&status, rv);

    let patched = match api
        .patch_status(name, &PatchParams::default(), &Patch::Merge(patch))
        .await
    {
        Ok(p) => p,
        Err(kube::Error::Api(e)) if e.code == 409 => {
            tracing::debug!(
                grid_site = name,
                "status patch conflict — another replica won the CAS race"
            );
            return Ok(());
        },
        Err(e) => return Err(e.into()),
    };

    // Mutation detection: compare resourceVersion before/after to gate
    // Event emission and metric recording.
    let patched_rv = patched.metadata.resource_version.as_deref();
    let patch_caused_mutation = rv != patched_rv;

    let reason_changed = existing.is_none_or(|current| current.reason != reason);
    if patch_caused_mutation && (phase_changed || reason_changed) {
        tracing::info!(
            grid_site = name,
            previous_phase = ?current_phase,
            phase = ?phase,
            reason,
            "GridSite gateway health state changed"
        );

        let event_type = event_type_for_reason(reason);
        let event_note = truncate_event_note(message);
        if let Err(e) = recorder
            .publish(
                &Event {
                    type_: event_type,
                    reason: reason.to_owned(),
                    note: Some(event_note),
                    action: "GatewayProbe".to_owned(),
                    secondary: None,
                },
                object_ref,
            )
            .await
        {
            tracing::warn!(error = %e, "failed to publish GridSite event");
        }

        let from_label = current_phase.map_or("None", phase_label);
        crate::metrics::record_phase_transition(from_label, phase_label(phase), reason);
    }

    Ok(())
}

/// Map a status reason to a Kubernetes [`EventType`].
///
/// [`Normal`] for successful lifecycle progressions and expected states;
/// [`Warning`] for trust, identity, and connectivity failures.
///
/// [`Normal`]: EventType::Normal
/// [`Warning`]: EventType::Warning
fn event_type_for_reason(reason: &str) -> EventType {
    match reason {
        // AdvertisedCertMismatch is a success path, so Warning would be false alarm.
        "TlsVerified" | "AwaitingDiscovery" | "GatewayAddressKnown" | "Left" | "AdvertisedCertMismatch" => {
            EventType::Normal
        },
        _ => EventType::Warning,
    }
}

/// Truncate an event note to a bounded length.
///
/// Reuses [`MAX_STATUS_MESSAGE_LEN`](crate::resources::gateway_probe::MAX_STATUS_MESSAGE_LEN)
/// from [`gateway_probe`](crate::resources::gateway_probe) to keep
/// Event notes well within the Kubernetes soft 1 KB limit and prevent
/// accidental PEM or key leakage.
fn truncate_event_note(message: &str) -> String {
    use crate::resources::gateway_probe::MAX_STATUS_MESSAGE_LEN;
    if message.chars().count() <= MAX_STATUS_MESSAGE_LEN {
        message.to_owned()
    } else {
        let mut s: String = message.chars().take(MAX_STATUS_MESSAGE_LEN - 3).collect();
        s.push_str("...");
        s
    }
}

/// Return whether the status subresource differs from the desired status.
fn grid_site_status_needs_update(current: Option<&GridSiteStatus>, desired: &GridSiteStatus) -> bool {
    current != Some(desired)
}

/// Build a merge patch containing only fields owned by the `GridSite`
/// controller, with `metadata.resourceVersion` as a CAS precondition.
fn grid_site_owned_status_patch(status: &GridSiteStatus, resource_version: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "resourceVersion": resource_version
        },
        "status": {
            "phase": status.phase,
            "observedGeneration": status.observed_generation,
            "reason": status.reason,
            "message": status.message,
            "lastProbeTime": status.last_probe_time,
            "lastTransitionTime": status.last_transition_time
        }
    })
}

/// Current UTC time as an RFC 3339 string.
///
/// Returns `None` on format failure rather than panicking.
fn rfc3339_now() -> Option<String> {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .ok()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grid_site_status_update_is_skipped_when_semantically_unchanged() {
        let baseline = GridSiteStatus {
            phase: GridSitePhase::Active,
            observed_generation: 2,
            reason: "Ready".to_owned(),
            message: "gateway reachable".to_owned(),
            ..GridSiteStatus::default()
        };
        assert!(!grid_site_status_needs_update(Some(&baseline), &baseline));

        let changed = GridSiteStatus {
            phase: GridSitePhase::Unreachable,
            ..baseline.clone()
        };
        assert!(grid_site_status_needs_update(Some(&baseline), &changed));
        assert!(grid_site_status_needs_update(None, &baseline));
    }

    #[test]
    fn status_patch_does_not_claim_swim_owned_fields() {
        let status = GridSiteStatus {
            capabilities: crate::crd::grid_site::SiteCapabilities {
                inference: true,
                ..Default::default()
            },
            public_cert_pem: Some("sentinel-public-cert".to_owned()),
            phase: GridSitePhase::Active,
            reason: "TlsVerified".to_owned(),
            ..Default::default()
        };
        let patch = grid_site_owned_status_patch(&status, Some("12345"));
        let owned = patch
            .get("status")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(!owned.contains_key("capabilities"));
        assert!(!owned.contains_key("publicCertPem"));
    }

    #[test]
    fn status_patch_includes_resource_version_as_cas_precondition() {
        let status = GridSiteStatus {
            phase: GridSitePhase::Connecting,
            reason: "PinMismatch".to_owned(),
            ..Default::default()
        };
        let patch = grid_site_owned_status_patch(&status, Some("99887"));
        let rv = patch
            .get("metadata")
            .and_then(|m| m.get("resourceVersion"))
            .and_then(serde_json::Value::as_str);
        assert_eq!(rv, Some("99887"), "patch must carry resourceVersion for CAS");
    }

    #[test]
    fn status_patch_carries_null_resource_version_when_absent() {
        let status = GridSiteStatus::default();
        let patch = grid_site_owned_status_patch(&status, None);
        let rv = patch.get("metadata").and_then(|m| m.get("resourceVersion"));
        assert!(
            rv.is_some_and(serde_json::Value::is_null),
            "patch should carry null resourceVersion when not set"
        );
    }
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
    // site_phase_next — non-probe phases (outcome = None)
    // -----------------------------------------------------------------------

    #[test]
    fn pending_stays_pending_even_with_egress() {
        let site = site_with_egress(Some(GridSitePhase::Pending), "10.0.0.1:8443");
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Pending, &site, None);
        assert_eq!(next, GridSitePhase::Pending);
        assert_eq!(reason, "AwaitingDiscovery");
    }

    #[test]
    fn discovered_with_egress_advances_to_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Discovered), "10.0.0.1:7946");
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        assert_eq!(
            next,
            GridSitePhase::Connecting,
            "Discovered + gateway address must advance to Connecting"
        );
        assert_eq!(reason, "GatewayAddressKnown");
    }

    #[test]
    fn discovered_without_egress_stays_discovered() {
        let site = site_no_egress(Some(GridSitePhase::Discovered));
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        assert_eq!(
            next,
            GridSitePhase::Discovered,
            "Discovered + no gateway address must stay Discovered"
        );
        assert_eq!(reason, "GatewayAddressMissing");
    }

    #[test]
    fn discovered_with_empty_egress_stays_discovered() {
        let site = site_with_egress(Some(GridSitePhase::Discovered), "");
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        assert_eq!(next, GridSitePhase::Discovered);
        assert_eq!(reason, "GatewayAddressMissing");
    }

    #[test]
    fn left_is_preserved() {
        let site = site_no_egress(Some(GridSitePhase::Left));
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Left, &site, None);
        assert_eq!(next, GridSitePhase::Left, "Left must be preserved");
        assert_eq!(reason, "Left");
    }

    #[test]
    fn left_remains_terminal() {
        let site = site_with_plaintext_egress(Some(GridSitePhase::Left), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(&GridSitePhase::Left, &site, None);
        assert_eq!(phase, GridSitePhase::Left, "Left must remain terminal");
        assert_eq!(reason, "Left");
    }

    #[test]
    fn discovered_with_gateway_address_advances_to_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Discovered), "10.0.0.1:19080");
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        assert_eq!(next, GridSitePhase::Connecting);
        assert_eq!(reason, "GatewayAddressKnown");
    }

    #[test]
    fn discovered_without_gateway_address_stays_discovered() {
        let site = site_no_egress(Some(GridSitePhase::Discovered));
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        assert_eq!(next, GridSitePhase::Discovered);
        assert_eq!(reason, "GatewayAddressMissing");
    }

    #[test]
    fn phase_reason_codes_are_deterministic() {
        let site = site_with_egress(Some(GridSitePhase::Discovered), "10.0.0.1:8443");
        let (_, r1, _) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        let (_, r2, _) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        assert_eq!(r1, r2, "reason must be deterministic for the same inputs");
    }

    // -----------------------------------------------------------------------
    // site_phase_next — probe outcome transitions
    // -----------------------------------------------------------------------

    #[test]
    fn connecting_with_connection_failure_stays_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (next, reason, _msg) = site_phase_next(
            &GridSitePhase::Connecting,
            &site,
            Some(&GatewayProbeOutcome::ConnectionFailed),
        );
        assert_eq!(
            next,
            GridSitePhase::Connecting,
            "Connecting must stay on connection failure"
        );
        assert_eq!(reason, "ConnectionFailed");
    }

    #[test]
    fn connecting_with_verified_outcome_promotes_to_active() {
        let site = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (next, reason, _msg) =
            site_phase_next(&GridSitePhase::Connecting, &site, Some(&GatewayProbeOutcome::Verified));
        assert_eq!(next, GridSitePhase::Active, "Verified must promote to Active");
        assert_eq!(reason, "TlsVerified");
    }

    #[test]
    fn active_with_connection_failure_demotes_to_unreachable() {
        let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (next, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::ConnectionFailed),
        );
        assert_eq!(
            next,
            GridSitePhase::Unreachable,
            "Active with connection failure must become Unreachable"
        );
        assert_eq!(reason, "ConnectionFailed");
    }

    #[test]
    fn active_with_address_missing_demotes_to_unreachable() {
        let site = site_no_egress(Some(GridSitePhase::Active));
        let (next, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::AddressMissing),
        );
        assert_eq!(
            next,
            GridSitePhase::Unreachable,
            "Active without egress cannot remain Active"
        );
        assert_eq!(reason, "EgressMissing");
    }

    #[test]
    fn active_with_verified_stays_active() {
        let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) =
            site_phase_next(&GridSitePhase::Active, &site, Some(&GatewayProbeOutcome::Verified));
        assert_eq!(phase, GridSitePhase::Active);
        assert_eq!(reason, "TlsVerified");
    }

    #[test]
    fn unreachable_with_connection_failure_stays_unreachable() {
        let site = site_with_egress(Some(GridSitePhase::Unreachable), "10.0.0.1:8443");
        let (next, reason, _msg) = site_phase_next(
            &GridSitePhase::Unreachable,
            &site,
            Some(&GatewayProbeOutcome::ConnectionFailed),
        );
        assert_eq!(
            next,
            GridSitePhase::Unreachable,
            "Unreachable with failed probe must stay Unreachable"
        );
        assert_eq!(reason, "ConnectionFailed");
    }

    #[test]
    fn unreachable_with_address_missing_stays_unreachable() {
        let site = site_no_egress(Some(GridSitePhase::Unreachable));
        let (next, reason, _msg) = site_phase_next(
            &GridSitePhase::Unreachable,
            &site,
            Some(&GatewayProbeOutcome::AddressMissing),
        );
        assert_eq!(
            next,
            GridSitePhase::Unreachable,
            "Unreachable without gateway must stay Unreachable"
        );
        assert_eq!(reason, "EgressMissing");
    }

    #[test]
    fn unreachable_with_verified_recovers_to_active() {
        let site = site_with_egress(Some(GridSitePhase::Unreachable), "10.0.0.1:8443");
        let (phase, reason, _msg) =
            site_phase_next(&GridSitePhase::Unreachable, &site, Some(&GatewayProbeOutcome::Verified));
        assert_eq!(
            phase,
            GridSitePhase::Active,
            "Unreachable + verified must recover to Active"
        );
        assert_eq!(reason, "TlsVerified");
    }

    // -----------------------------------------------------------------------
    // Trust failure outcomes — always demote to Connecting
    // -----------------------------------------------------------------------

    #[test]
    fn trust_material_missing_stays_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Connecting,
            &site,
            Some(&GatewayProbeOutcome::TrustMaterialMissing),
        );
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "TrustMaterialMissing");
    }

    #[test]
    fn trust_material_invalid_stays_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Connecting,
            &site,
            Some(&GatewayProbeOutcome::TrustMaterialInvalid),
        );
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "TrustMaterialInvalid");
    }

    #[test]
    fn untrusted_issuer_demotes_active_to_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::UntrustedIssuer),
        );
        assert_eq!(
            phase,
            GridSitePhase::Connecting,
            "trust failure must demote to Connecting"
        );
        assert_eq!(reason, "UntrustedIssuer");
    }

    #[test]
    fn identity_mismatch_demotes_active_to_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::IdentityMismatch),
        );
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "IdentityMismatch");
    }

    #[test]
    fn certificate_expired_demotes_active_to_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::CertificateExpired),
        );
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "CertificateExpired");
    }

    #[test]
    fn pin_mismatch_demotes_active_to_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) =
            site_phase_next(&GridSitePhase::Active, &site, Some(&GatewayProbeOutcome::PinMismatch));
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "PinMismatch");
    }

    /// A site whose only relevant property is its advertised certificate.
    fn site_with_advertised(pem: Option<&str>) -> GridSite {
        GridSite {
            status: Some(GridSiteStatus {
                public_cert_pem: pem.map(ToOwned::to_owned),
                ..Default::default()
            }),
            ..site_no_egress(None)
        }
    }

    /// Absent advertised material yields no DER, and no error.
    #[test]
    fn advertised_leaf_absent_is_none() {
        assert!(advertised_leaf_der(&site_no_egress(None)).is_none(), "no status");
        assert!(
            advertised_leaf_der(&site_with_advertised(None)).is_none(),
            "status, no PEM"
        );
    }

    /// A real advertised certificate parses to the expected DER.
    #[test]
    fn advertised_leaf_valid_is_parsed() {
        let ca = certs::generate_ca("t").unwrap_or_else(|_| std::process::abort());
        let leaf = certs::generate_site_cert(&ca, "peer").unwrap_or_else(|_| std::process::abort());
        let want = first_cert_der_from_pem(&leaf.cert_pem).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            advertised_leaf_der(&site_with_advertised(Some(&leaf.cert_pem))),
            Some(want)
        );
    }

    /// A chain PEM yields the leaf, not an intermediate.
    #[test]
    fn advertised_leaf_of_chain_is_the_leaf() {
        let ca = certs::generate_ca("t").unwrap_or_else(|_| std::process::abort());
        let leaf = certs::generate_site_cert(&ca, "peer").unwrap_or_else(|_| std::process::abort());
        let chain = format!("{}{}", leaf.cert_pem, ca.cert_pem);
        let want = first_cert_der_from_pem(&leaf.cert_pem).unwrap_or_else(|_| std::process::abort());
        assert_eq!(advertised_leaf_der(&site_with_advertised(Some(&chain))), Some(want));
    }

    /// An unparseable advertised PEM is ignored, not surfaced as an error.
    #[test]
    fn unparseable_advertised_leaf_is_ignored() {
        let bad = "-----BEGIN CERTIFICATE-----\nMIIBIjANBgkqhkiG9\n-----END CERTIFICATE-----";
        assert!(
            first_cert_der_from_pem(bad).is_err(),
            "fixture must be the unparseable case this guards"
        );
        assert!(
            advertised_leaf_der(&site_with_advertised(Some(bad))).is_none(),
            "unparseable advertised material must be ignored, never surfaced as an error"
        );
    }

    #[test]
    fn advertised_cert_mismatch_promotes_connecting_to_active() {
        let site = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Connecting,
            &site,
            Some(&GatewayProbeOutcome::AdvertisedCertificateMismatch),
        );
        assert_eq!(phase, GridSitePhase::Active);
        assert_eq!(reason, "AdvertisedCertMismatch");
    }

    #[test]
    fn advertised_cert_mismatch_recovers_unreachable_to_active() {
        let site = site_with_egress(Some(GridSitePhase::Unreachable), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Unreachable,
            &site,
            Some(&GatewayProbeOutcome::AdvertisedCertificateMismatch),
        );
        assert_eq!(phase, GridSitePhase::Active);
        assert_eq!(reason, "AdvertisedCertMismatch");
    }

    #[test]
    fn advertised_cert_mismatch_keeps_active() {
        let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::AdvertisedCertificateMismatch),
        );
        assert_eq!(phase, GridSitePhase::Active);
        assert_eq!(reason, "AdvertisedCertMismatch");
    }

    // -----------------------------------------------------------------------
    // Plaintext probe outcomes
    // -----------------------------------------------------------------------

    fn site_with_plaintext_egress(phase: Option<GridSitePhase>, egress: &str) -> GridSite {
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
                    tls: EgressTls {
                        mode: EgressTlsMode::Plaintext,
                        server_name: None,
                    },
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

    #[test]
    fn plaintext_connecting_remains_ineligible_when_reachable() {
        let site = site_with_plaintext_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Connecting,
            &site,
            Some(&GatewayProbeOutcome::PlaintextReachable),
        );
        assert_eq!(
            phase,
            GridSitePhase::Connecting,
            "TCP reachability without verified identity must not promote to Active"
        );
        assert_eq!(reason, "IdentityVerificationRequired");
    }

    #[test]
    fn plaintext_connecting_stays_connecting_when_unreachable() {
        let site = site_with_plaintext_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Connecting,
            &site,
            Some(&GatewayProbeOutcome::PlaintextUnreachable),
        );
        assert_eq!(
            phase,
            GridSitePhase::Connecting,
            "plaintext + unreachable must stay Connecting"
        );
        assert_eq!(reason, "PlaintextUnreachable");
    }

    #[test]
    fn plaintext_active_demotes_when_reachable_without_identity() {
        let site = site_with_plaintext_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::PlaintextReachable),
        );
        assert_eq!(
            phase,
            GridSitePhase::Connecting,
            "changing an Active site to plaintext must revoke routing eligibility"
        );
        assert_eq!(reason, "IdentityVerificationRequired");
    }

    #[test]
    fn plaintext_active_demotes_when_unreachable() {
        let site = site_with_plaintext_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::PlaintextUnreachable),
        );
        assert_eq!(
            phase,
            GridSitePhase::Unreachable,
            "plaintext Active + unreachable must demote"
        );
        assert_eq!(reason, "PlaintextUnreachable");
    }

    #[test]
    fn plaintext_unreachable_moves_to_connecting_when_reachable() {
        let site = site_with_plaintext_egress(Some(GridSitePhase::Unreachable), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Unreachable,
            &site,
            Some(&GatewayProbeOutcome::PlaintextReachable),
        );
        assert_eq!(
            phase,
            GridSitePhase::Connecting,
            "reachable plaintext cannot recover directly to Active"
        );
        assert_eq!(reason, "IdentityVerificationRequired");
    }

    // -----------------------------------------------------------------------
    // Message safety — no private material in probe transition messages
    // -----------------------------------------------------------------------

    #[test]
    fn phase_messages_do_not_contain_sentinel_token() {
        let sentinel = "sk-super-secret-token-do-not-emit";
        let non_probe = [GridSitePhase::Pending, GridSitePhase::Discovered, GridSitePhase::Left];
        for phase in &non_probe {
            let site = site_with_egress(Some(phase.clone()), "10.0.0.1:8443");
            let (_, reason, message) = site_phase_next(phase, &site, None);
            assert!(
                !reason.contains(sentinel),
                "reason for {phase:?} must not contain sentinel: {reason}"
            );
            assert!(
                !message.contains(sentinel),
                "message for {phase:?} must not contain sentinel: {message}"
            );
        }
        let outcomes = [
            GatewayProbeOutcome::Verified,
            GatewayProbeOutcome::ConnectionFailed,
            GatewayProbeOutcome::TrustMaterialMissing,
            GatewayProbeOutcome::PinMismatch,
            GatewayProbeOutcome::PlaintextReachable,
        ];
        for outcome in &outcomes {
            let site = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
            let (_, reason, message) = site_phase_next(&GridSitePhase::Connecting, &site, Some(outcome));
            assert!(!reason.contains(sentinel), "reason must not contain sentinel: {reason}");
            assert!(
                !message.contains(sentinel),
                "message must not contain sentinel: {message}"
            );
        }
    }

    #[test]
    fn probe_outcome_messages_never_contain_pem_or_key_markers() {
        let outcomes = [
            GatewayProbeOutcome::Verified,
            GatewayProbeOutcome::ConnectionFailed,
            GatewayProbeOutcome::ConnectTimeout,
            GatewayProbeOutcome::HandshakeTimeout,
            GatewayProbeOutcome::TrustMaterialMissing,
            GatewayProbeOutcome::TrustMaterialInvalid,
            GatewayProbeOutcome::UntrustedIssuer,
            GatewayProbeOutcome::IdentityMismatch,
            GatewayProbeOutcome::CertificateExpired,
            GatewayProbeOutcome::CertificateNotYetValid,
            GatewayProbeOutcome::PinMismatch,
            GatewayProbeOutcome::AdvertisedCertificateMismatch,
            GatewayProbeOutcome::PlaintextReachable,
            GatewayProbeOutcome::PlaintextUnreachable,
            GatewayProbeOutcome::AddressMissing,
            GatewayProbeOutcome::TlsProtocolError,
        ];
        for outcome in &outcomes {
            let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
            let (_, _, message) = site_phase_next(&GridSitePhase::Active, &site, Some(outcome));
            assert!(
                !message.contains("BEGIN CERTIFICATE"),
                "message must not include PEM: {message}"
            );
            assert!(
                !message.contains("PRIVATE KEY"),
                "message must not include key marker: {message}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Helper tests
    // -----------------------------------------------------------------------

    #[test]
    fn is_plaintext_transport_detects_mode() {
        let plaintext = site_with_plaintext_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8080");
        assert!(is_plaintext_transport(&plaintext), "Plaintext mode must be detected");

        let mutual = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8080");
        assert!(!is_plaintext_transport(&mutual), "Mutual mode must not be plaintext");

        let no_egress = site_no_egress(Some(GridSitePhase::Connecting));
        assert!(!is_plaintext_transport(&no_egress), "no egress must not be plaintext");
    }

    #[test]
    fn needs_probe_for_active_phases() {
        assert!(needs_probe(&GridSitePhase::Connecting), "Connecting needs probe");
        assert!(needs_probe(&GridSitePhase::Active), "Active needs probe");
        assert!(needs_probe(&GridSitePhase::Unreachable), "Unreachable needs probe");
        assert!(!needs_probe(&GridSitePhase::Pending), "Pending does not need probe");
        assert!(
            !needs_probe(&GridSitePhase::Discovered),
            "Discovered does not need probe"
        );
        assert!(!needs_probe(&GridSitePhase::Left), "Left does not need probe");
    }

    // -----------------------------------------------------------------------
    // Pin resolution — rotation and legacy compatibility
    // -----------------------------------------------------------------------

    use crate::crd::grid_site::GridSiteTrustPolicy;

    fn site_with_trust(phase: Option<GridSitePhase>, egress: &str, trust: Option<GridSiteTrustPolicy>) -> GridSite {
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
                trust,
            },
            status: phase.map(|p| GridSiteStatus {
                phase: p,
                ..Default::default()
            }),
        }
    }

    fn valid_pin() -> String {
        "a".repeat(64)
    }

    fn valid_pin_2() -> String {
        "b".repeat(64)
    }

    #[test]
    fn resolve_pins_no_trust_policy_fails_closed() {
        let site = site_with_trust(Some(GridSitePhase::Connecting), "10.0.0.1:8443", None);
        assert_eq!(
            resolve_pins(&site),
            Err(GatewayProbeOutcome::TrustMaterialMissing),
            "no trust policy must remain in bootstrap"
        );
    }

    #[test]
    fn resolve_pins_single_canonical_pin() {
        let trust = GridSiteTrustPolicy {
            cert_fingerprint: None,
            canonical_fingerprints: Some(vec![valid_pin()]),
        };
        let site = site_with_trust(Some(GridSitePhase::Connecting), "10.0.0.1:8443", Some(trust));
        let pins = resolve_pins(&site).unwrap_or_else(|_| std::process::abort());
        assert_eq!(pins.len(), 1, "single canonical pin");
    }

    #[test]
    fn resolve_pins_two_canonical_pins_for_rotation() {
        let trust = GridSiteTrustPolicy {
            cert_fingerprint: None,
            canonical_fingerprints: Some(vec![valid_pin(), valid_pin_2()]),
        };
        let site = site_with_trust(Some(GridSitePhase::Connecting), "10.0.0.1:8443", Some(trust));
        let pins = resolve_pins(&site).unwrap_or_else(|_| std::process::abort());
        assert_eq!(pins.len(), 2, "two canonical pins for rotation overlap");
    }

    #[test]
    fn resolve_pins_three_pins_rejected() {
        let trust = GridSiteTrustPolicy {
            cert_fingerprint: None,
            canonical_fingerprints: Some(vec![valid_pin(), valid_pin_2(), "c".repeat(64)]),
        };
        let site = site_with_trust(Some(GridSitePhase::Connecting), "10.0.0.1:8443", Some(trust));
        let result = resolve_pins(&site);
        assert_eq!(
            result,
            Err(GatewayProbeOutcome::TrustMaterialInvalid),
            "three pins must be rejected"
        );
    }

    #[test]
    fn resolve_pins_empty_pin_list_rejected() {
        let trust = GridSiteTrustPolicy {
            cert_fingerprint: None,
            canonical_fingerprints: Some(Vec::new()),
        };
        let site = site_with_trust(Some(GridSitePhase::Connecting), "10.0.0.1:8443", Some(trust));
        assert_eq!(
            resolve_pins(&site),
            Err(GatewayProbeOutcome::TrustMaterialInvalid),
            "present but empty pin policy is invalid"
        );
    }

    #[test]
    fn resolve_pins_invalid_pin_format_rejected() {
        let trust = GridSiteTrustPolicy {
            cert_fingerprint: None,
            canonical_fingerprints: Some(vec!["not-a-valid-hex-fingerprint".to_owned()]),
        };
        let site = site_with_trust(Some(GridSitePhase::Connecting), "10.0.0.1:8443", Some(trust));
        let result = resolve_pins(&site);
        assert_eq!(
            result,
            Err(GatewayProbeOutcome::TrustMaterialInvalid),
            "invalid pin format must be rejected"
        );
    }

    #[test]
    fn resolve_pins_legacy_fingerprint_only_rejected() {
        let trust = GridSiteTrustPolicy {
            cert_fingerprint: Some("ab:cd:ef:01:23".to_owned()),
            canonical_fingerprints: None,
        };
        let site = site_with_trust(Some(GridSitePhase::Connecting), "10.0.0.1:8443", Some(trust));
        let result = resolve_pins(&site);
        assert_eq!(
            result,
            Err(GatewayProbeOutcome::TrustMaterialInvalid),
            "legacy-only fingerprint must be rejected (migration required)"
        );
    }

    #[test]
    fn resolve_pins_both_legacy_and_canonical_rejected() {
        let trust = GridSiteTrustPolicy {
            cert_fingerprint: Some("ab:cd:ef:01:23".to_owned()),
            canonical_fingerprints: Some(vec![valid_pin()]),
        };
        let site = site_with_trust(Some(GridSitePhase::Connecting), "10.0.0.1:8443", Some(trust));
        let result = resolve_pins(&site);
        assert_eq!(
            result,
            Err(GatewayProbeOutcome::TrustMaterialInvalid),
            "both legacy and canonical must be rejected (mutually exclusive)"
        );
    }

    // -----------------------------------------------------------------------
    // Event helpers
    // -----------------------------------------------------------------------

    #[test]
    fn event_type_tls_verified_is_normal() {
        assert!(matches!(event_type_for_reason("TlsVerified"), EventType::Normal));
    }

    #[test]
    fn event_type_awaiting_discovery_is_normal() {
        assert!(matches!(event_type_for_reason("AwaitingDiscovery"), EventType::Normal));
    }

    #[test]
    fn event_type_gateway_address_known_is_normal() {
        assert!(matches!(
            event_type_for_reason("GatewayAddressKnown"),
            EventType::Normal
        ));
    }

    #[test]
    fn event_type_left_is_normal() {
        assert!(matches!(event_type_for_reason("Left"), EventType::Normal));
    }

    #[test]
    fn event_type_advertised_cert_mismatch_is_normal() {
        assert!(matches!(
            event_type_for_reason("AdvertisedCertMismatch"),
            EventType::Normal
        ));
    }

    #[test]
    fn event_type_pin_mismatch_is_warning() {
        assert!(matches!(event_type_for_reason("PinMismatch"), EventType::Warning));
    }

    #[test]
    fn event_type_connection_failed_is_warning() {
        assert!(matches!(event_type_for_reason("ConnectionFailed"), EventType::Warning));
    }

    #[test]
    fn event_type_certificate_expired_is_warning() {
        assert!(matches!(
            event_type_for_reason("CertificateExpired"),
            EventType::Warning
        ));
    }

    #[test]
    fn event_type_trust_material_missing_is_warning() {
        assert!(matches!(
            event_type_for_reason("TrustMaterialMissing"),
            EventType::Warning
        ));
    }

    #[test]
    fn event_type_identity_mismatch_is_warning() {
        assert!(matches!(event_type_for_reason("IdentityMismatch"), EventType::Warning));
    }

    #[test]
    fn truncate_event_note_short_message_unchanged() {
        let msg = "TLS handshake verified";
        assert_eq!(truncate_event_note(msg), msg);
    }

    #[test]
    fn truncate_event_note_long_message_truncated() {
        let msg = "a".repeat(300);
        let result = truncate_event_note(&msg);
        assert!(result.ends_with("..."), "long message should end with ...");
        assert!(
            result.chars().count() <= crate::resources::gateway_probe::MAX_STATUS_MESSAGE_LEN,
            "truncated message must not exceed MAX_STATUS_MESSAGE_LEN"
        );
    }
}
