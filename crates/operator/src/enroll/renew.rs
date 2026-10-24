//! Renew the site identity before it expires, in place, with the current certificate.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use k8s_openapi::{
    ByteString,
    api::{apps::v1::Deployment, core::v1::Secret},
};
use kube::{
    Api, Client,
    api::{ListParams, Patch, PatchParams},
};
use reqwest::StatusCode;
use time::OffsetDateTime;
use zeroize::Zeroizing;

use super::{
    Issued, Signer, SignerError, Target, jittered,
    local::{self, BundleError, Bundles, Local, Verdict},
    signer::{Clients, HttpsSigner, RENEW_PATH},
};
use crate::{
    controller::grid_network::GridModes,
    crd::{
        grid_network::{GridNetwork, PeerTrustMode},
        grid_site::{BundleRef, GridSite, LocalSite, Rotation, SignerEndpoint},
    },
    metrics,
};


/// Longest wait between checks, so an identity replaced out of band is still watched.
const CHECK_EVERY: Duration = Duration::from_secs(60 * 60);

/// First retry after a failed renewal, doubling to [`RETRY_MAX`].
const RETRY_INITIAL: Duration = Duration::from_secs(60);

/// Longest wait between retries.
const RETRY_MAX: Duration = Duration::from_secs(30 * 60);

/// Secret keys holding a renewal in flight, so a restart retries with the same key.
const PENDING_KEY: &str = "rotate.key";
/// The CSR for [`PENDING_KEY`].
const PENDING_CSR: &str = "rotate.csr";

/// Pod template annotation naming the identity the gateway pods loaded.
pub const GATEWAY_IDENTITY_ANNOTATION: &str = "grid.praxis.fast/site-identity-fingerprint";

/// The leaf the current one replaced, kept so the co-located gateway stays recognised until it reloads.
pub const PREVIOUS_CERT: &str = "previous.crt";

/// When the identity needs renewing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Due {
    /// Not before this instant.
    At(OffsetDateTime),
    /// Now: less than a third of its lifetime remains.
    Now,
    /// It has expired and can no longer authenticate a renewal.
    Expired,
}

/// When an identity valid from `not_before` to `not_after` is due at `now`.
#[must_use]
pub fn due(not_before: OffsetDateTime, not_after: OffsetDateTime, now: OffsetDateTime) -> Due {
    if now >= not_after {
        return Due::Expired;
    }
    let renew_at = renew_after(not_before, not_after);
    if now >= renew_at { Due::Now } else { Due::At(renew_at) }
}

/// The instant a third of the lifetime remains.
#[must_use]
pub fn renew_after(not_before: OffsetDateTime, not_after: OffsetDateTime) -> OffsetDateTime {
    not_after.saturating_sub((not_after - not_before) / 3)
}

/// Why a renewal did not complete.
#[derive(Debug, thiserror::Error)]
pub(super) enum RenewError {
    /// The identity Secret or a key it needs is absent.
    #[error("site identity absent: {0}")]
    Missing(String),
    /// The identity Secret holds material that cannot be used.
    #[error("site identity unreadable: {0}")]
    Material(String),
    /// A Kubernetes call failed.
    #[error("kubernetes: {0}")]
    Kube(String),
    /// The service could not be reached or did not answer usably. Retried.
    #[error("enrollment service unavailable: {0}")]
    Transport(String),
    /// The service did not accept the presented certificate.
    #[error("rotation unauthenticated ({0}); re-enroll this site with a new site token")]
    Unauthenticated(String),
    /// The service refused this identity.
    #[error("rotation refused ({0}); re-enroll this site with a new site token")]
    Refused(String),
    /// The service answered with a certificate this site cannot use.
    #[error("rotation response invalid: {0}")]
    Invalid(String),
    /// The service refuses every rotation: rotation is turned off on the hub.
    #[error("rotation disabled at the signer ({0}); turn it on before the identity expires")]
    SignerDisabled(String),
}

impl RenewError {
    /// Metric label.
    const fn result(&self) -> &'static str {
        match self {
            Self::Unauthenticated(_) | Self::Refused(_) => "refused",
            Self::Missing(_)
            | Self::Material(_)
            | Self::Kube(_)
            | Self::Transport(_)
            | Self::Invalid(_)
            | Self::SignerDisabled(_) => "failed",
        }
    }
}

/// What a check found or did.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Checked {
    /// Not due until this instant.
    Waiting(OffsetDateTime),
    /// Renewed, valid until this instant.
    Renewed(OffsetDateTime),
    /// Expired at this instant.
    Expired(OffsetDateTime),
    /// Nothing renews this identity: why, and whether a renewal is already due.
    Off {
        /// Why.
        why: Off,
        /// Whether the renewal this would have made is due.
        due: Due,
    },
}

/// Why nothing renews the identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Off {
    /// The signer's rotation is Enabled under pin peer trust, where a pinned peer refuses
    /// a renewed leaf: the conflict is reported on `Accepted` and renewal is held back.
    UnsupportedUnderPin,
    /// This site's `GridSite` names no signer.
    NoSigner,
    /// The signer's rotation is Disabled.
    Disabled,
}

/// The informational `RotationOnSchedule` reason while nothing renews and renewal is not due.
const ROTATION_DISABLED: &str = "RotationDisabled";

impl Off {
    /// What the admin reads.
    const fn message(self) -> &'static str {
        match self {
            Self::UnsupportedUnderPin => {
                "rotation is held back: spec.local.signerEndpoint.rotation is Enabled under pin peer trust, see Accepted"
            },
            Self::NoSigner => {
                "rotation disabled: spec.local names no signer, so the identity is kept until it expires unless \
                 another writer renews it"
            },
            Self::Disabled => {
                "rotation disabled: spec.local.signerEndpoint.rotation is Disabled, so the identity is kept until it \
                 expires unless another writer renews it"
            },
        }
    }

    /// A stable word for logs.
    const fn label(self) -> &'static str {
        match self {
            Self::UnsupportedUnderPin => "unsupported-under-pin",
            Self::NoSigner => "no-signer",
            Self::Disabled => "disabled",
        }
    }
}

/// The identity as stored.
pub(super) struct Held {
    /// Current leaf, PEM.
    pub(super) cert: String,
    /// Its key, PEM.
    pub(super) key: Zeroizing<String>,
    /// The grid CA, PEM.
    pub(super) ca: String,
    /// A renewal in flight: its CSR and key.
    pub(super) pending: Option<(String, Zeroizing<String>)>,
    /// The site Secret's resource version, a write precondition.
    pub(super) version: String,
    /// The identity has rotated at least once: the Secret holds the leaf it replaced.
    pub(super) rotated: bool,
}

/// Where the identity is read and written.
pub(super) trait IdentityStore {
    /// Read the identity in `target`.
    fn read(&self, target: &Target) -> impl Future<Output = Result<Held, RenewError>> + Send;
    /// Set or remove (`None`) keys of the site Secret at `version`, returning the new version.
    fn write(
        &self,
        target: &Target,
        version: &str,
        data: BTreeMap<&'static str, Option<Zeroizing<String>>>,
    ) -> impl Future<Output = Result<String, RenewError>> + Send;
}

/// What rolling the gateway onto a renewed identity did.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum GatewayRoll {
    /// New pods are starting on the identity.
    Rolled,
    /// The pods already carry it.
    Unchanged,
    /// This site has no gateway Deployment.
    NoGateway,
}

/// The site's gateway, which loads its upstream client certificate only at start.
pub(super) trait GatewayRoller {
    /// Roll the gateway onto the identity with `fingerprint`; `owed` when a gateway
    /// never rolled must roll too.
    fn roll(&self, fingerprint: &str, owed: bool) -> impl Future<Output = Result<GatewayRoll, RenewError>> + Send;
}

/// The patch that rolls a gateway whose pods carry `current` onto `fingerprint`, if any.
///
/// A gateway on an older leaf always rolls. One never rolled rolls only when `owed`,
/// so a fresh install is left running.
fn roll_patch(current: Option<&str>, fingerprint: &str, owed: bool) -> Option<serde_json::Value> {
    let behind = current.is_some_and(|current| current != fingerprint);
    (behind || (owed && current.is_none())).then(|| {
        serde_json::json!({ "spec": { "template": { "metadata": { "annotations": {
            GATEWAY_IDENTITY_ANNOTATION: fingerprint
        } } } } })
    })
}

/// Roll the gateway onto the identity now in `target`.
///
/// # Errors
///
/// Returns [`RenewError`] when the identity or the gateway cannot be read or patched.
pub(super) async fn roll_gateway<S: IdentityStore + Sync, G: GatewayRoller + Sync>(
    store: &S,
    gateway: &G,
    target: &Target,
    renewed: bool,
) -> Result<GatewayRoll, RenewError> {
    let held = store.read(target).await?;
    let fingerprint =
        certs::canonical_fingerprint(&held.cert).map_err(|e| RenewError::Material(format!("certificate: {e}")))?;
    // A rotated identity is owed a roll even on a recheck, so a first roll that failed
    // is retried; a fresh install, never rotated, is left running.
    gateway.roll(&fingerprint, renewed || held.rotated).await
}

/// Renew the identity in `target` if it is due at `now`.
///
/// # Errors
///
/// Returns [`RenewError`] when a due renewal does not complete.
#[cfg(test)]
pub(super) async fn check<S: IdentityStore + Sync, G: Signer + Sync>(
    store: &S,
    signer: &G,
    target: &Target,
    now: OffsetDateTime,
) -> Result<Checked, RenewError> {
    let held = store.read(target).await?;
    check_held(store, signer, target, held, now).await
}

/// The validity of the certificate `held`.
fn validity(held: &Held) -> Result<(OffsetDateTime, OffsetDateTime), RenewError> {
    certs::cert_validity(&held.cert).map_err(|e| RenewError::Material(format!("certificate: {e}")))
}

/// Renew `held`, the identity in `target`, if it is due at `now`.
async fn check_held<S: IdentityStore + Sync, G: Signer + Sync>(
    store: &S,
    signer: &G,
    target: &Target,
    held: Held,
    now: OffsetDateTime,
) -> Result<Checked, RenewError> {
    let (not_before, not_after) = validity(&held)?;
    metrics::set_site_identity_expiry(not_after.unix_timestamp());
    match due(not_before, not_after, now) {
        Due::At(at) => Ok(Checked::Waiting(at)),
        Due::Expired => Ok(Checked::Expired(not_after)),
        Due::Now => renew(store, signer, target, held).await,
    }
}

/// Renew `held` now and store the result in place.
async fn renew<S: IdentityStore + Sync, G: Signer + Sync>(
    store: &S,
    signer: &G,
    target: &Target,
    mut held: Held,
) -> Result<Checked, RenewError> {
    let site = site_of(&held.cert)?;
    let (csr, key) = pending(store, target, &mut held, &site).await?;
    let issued = signer.renew(&held, &csr).await.map_err(renew_error)?;
    let cert = verified(&issued, &held.ca, &site, &csr)?;
    let (_issued, valid_until) = certs::cert_validity(&cert).map_err(|e| RenewError::Invalid(e.to_string()))?;
    let replaced = BTreeMap::from([
        ("tls.crt", Some(Zeroizing::new(cert))),
        ("tls.key", Some(key)),
        (PREVIOUS_CERT, Some(Zeroizing::new(held.cert.clone()))),
        (PENDING_CSR, None),
        (PENDING_KEY, None),
    ]);
    store.write(target, &held.version, replaced).await?;
    metrics::set_site_identity_expiry(valid_until.unix_timestamp());
    Ok(Checked::Renewed(valid_until))
}

/// The renewal in flight, else a new key stored before it is sent, so a lost
/// response is retried with the key the service recorded.
async fn pending<S: IdentityStore + Sync>(
    store: &S,
    target: &Target,
    held: &mut Held,
    site: &str,
) -> Result<(String, Zeroizing<String>), RenewError> {
    if let Some(pending) = held.pending.take() {
        return Ok(pending);
    }
    let certs::GeneratedCsr { csr_pem, key_pem } =
        certs::generate_csr(site).map_err(|e| RenewError::Material(format!("generating a CSR: {e}")))?;
    let stash = BTreeMap::from([
        (PENDING_CSR, Some(Zeroizing::new(csr_pem.clone()))),
        (PENDING_KEY, Some(key_pem.clone())),
    ]);
    held.version = store.write(target, &held.version, stash).await?;
    Ok((csr_pem, key_pem))
}

/// The site a leaf's SPIFFE ID names.
fn site_of(cert_pem: &str) -> Result<String, RenewError> {
    crate::site_name::of_cert(cert_pem).ok_or_else(|| RenewError::Material("certificate names no grid site".to_owned()))
}

/// What a signer's refusal means for renewal.
fn renew_error(error: SignerError) -> RenewError {
    match error {
        SignerError::Unauthenticated(detail) => RenewError::Unauthenticated(detail),
        SignerError::Refused {
            status: StatusCode::FORBIDDEN,
            detail,
            ..
        } => RenewError::Refused(detail),
        SignerError::Disabled(detail) => RenewError::SignerDisabled(detail),
        SignerError::Refused { detail, .. }
        | SignerError::Transport { detail, .. }
        | SignerError::Untrusted { detail, .. } => RenewError::Transport(detail),
        SignerError::Request(detail) => RenewError::Material(detail),
        SignerError::Invalid(detail) => RenewError::Invalid(detail),
    }
}

/// The renewed leaf, if it is this site's, from the CA it already holds, for the CSR's key.
fn verified(enrollment: &Issued, held_ca: &str, site: &str, csr_pem: &str) -> Result<String, RenewError> {
    let invalid = RenewError::Invalid;
    let ca = certs::anchored_ca(&enrollment.ca_certificate, held_ca)
        .map_err(|e| invalid(format!("returned CA is not this site's grid CA ({e})")))?;
    let cert = certs::leaf_only(&enrollment.certificate).map_err(|e| invalid(format!("certificate: {e}")))?;
    certs::verify_site_cert(&ca, &cert, site).map_err(|e| invalid(format!("certificate is not {site}'s ({e})")))?;
    let leaf_key = certs::cert_public_key(&cert).map_err(|e| invalid(e.to_string()))?;
    let csr_key = certs::csr_public_key(csr_pem).map_err(|e| invalid(e.to_string()))?;
    if leaf_key != csr_key {
        return Err(invalid("certificate does not carry the requested key".to_owned()));
    }
    Ok(cert)
}

/// The site identity Secret, through the Kubernetes API.
pub(super) struct KubeIdentity(pub(super) Api<Secret>);

/// One Secret value as text.
fn text(data: &BTreeMap<String, ByteString>, key: &str) -> Option<Zeroizing<String>> {
    data.get(key)
        .and_then(|value| String::from_utf8(value.0.clone()).ok())
        .map(Zeroizing::new)
}

impl IdentityStore for KubeIdentity {
    async fn read(&self, target: &Target) -> Result<Held, RenewError> {
        let missing = |what: &str| RenewError::Missing(what.to_owned());
        let site = Box::pin(self.0.get_opt(&target.site_secret))
            .await
            .map_err(|e| RenewError::Kube(e.to_string()))?
            .ok_or_else(|| missing(&format!("Secret {}", target.site_secret)))?;
        let version = site.metadata.resource_version.clone().unwrap_or_default();
        let data = site.data.unwrap_or_default();
        let ca = if target.ca_secret == target.site_secret {
            text(&data, "ca.crt")
        } else {
            Box::pin(self.0.get_opt(&target.ca_secret))
                .await
                .map_err(|e| RenewError::Kube(e.to_string()))?
                .and_then(|secret| secret.data)
                .and_then(|ca_data| text(&ca_data, "ca.crt"))
        };
        let pending = text(&data, PENDING_CSR).zip(text(&data, PENDING_KEY));
        Ok(Held {
            cert: text(&data, "tls.crt").ok_or_else(|| missing("tls.crt"))?.to_string(),
            key: text(&data, "tls.key").ok_or_else(|| missing("tls.key"))?,
            ca: ca.ok_or_else(|| missing("ca.crt"))?.to_string(),
            pending: pending.map(|(csr, key)| (csr.to_string(), key)),
            version,
            rotated: data.contains_key(PREVIOUS_CERT),
        })
    }

    async fn write(
        &self,
        target: &Target,
        version: &str,
        data: BTreeMap<&'static str, Option<Zeroizing<String>>>,
    ) -> Result<String, RenewError> {
        let encoded: BTreeMap<&str, Option<ByteString>> = data
            .into_iter()
            .map(|(key, value)| (key, value.map(|text| ByteString(text.as_bytes().to_vec()))))
            .collect();
        // The resource version makes the patch fail if another writer changed the Secret.
        let patch = serde_json::json!({ "metadata": { "resourceVersion": version }, "data": encoded });
        let written = Box::pin(
            self.0
                .patch(&target.site_secret, &PatchParams::default(), &Patch::Merge(&patch)),
        )
        .await
        .map_err(|e| RenewError::Kube(e.to_string()))?;
        Ok(written.metadata.resource_version.unwrap_or_default())
    }
}

/// The signer a renewal goes to, read from `spec.local.signerEndpoint` on this site's `GridSite`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Endpoint {
    /// The signer's base URL.
    pub(super) base: reqwest::Url,
    /// The PEM pinning the signer, else the site's own grid CA.
    pub(super) pin: Option<String>,
}

impl Endpoint {
    /// The renewal URL, for messages.
    fn renewal_url(&self) -> String {
        HttpsSigner::endpoint(&self.base, RENEW_PATH)
    }
}

/// The gateway Deployment, named like its Service.
pub struct KubeGateway {
    /// Deployments in the gateway's namespace.
    api: Api<Deployment>,
    /// The Deployment.
    name: String,
    /// Whether a missing Deployment was already logged.
    missing_logged: AtomicBool,
}

impl KubeGateway {
    /// The gateway `name` in `namespace`.
    #[must_use]
    pub fn new(client: Client, namespace: &str, name: &str) -> Self {
        Self {
            api: Api::namespaced(client, namespace),
            name: name.to_owned(),
            missing_logged: AtomicBool::new(false),
        }
    }
}

impl GatewayRoller for KubeGateway {
    #[expect(clippy::large_stack_frames, reason = "a Deployment read, once per renewal")]
    async fn roll(&self, fingerprint: &str, owed: bool) -> Result<GatewayRoll, RenewError> {
        let kube = |e: kube::Error| RenewError::Kube(e.to_string());
        let Some(deployment) = Box::pin(self.api.get_opt(&self.name))
            .await
            .map_err(kube)?
            .map(Box::new)
        else {
            return Ok(GatewayRoll::NoGateway);
        };
        let current = deployment
            .spec
            .and_then(|spec| spec.template.metadata)
            .and_then(|meta| meta.annotations)
            .and_then(|mut annotations| annotations.remove(GATEWAY_IDENTITY_ANNOTATION));
        let Some(patch) = roll_patch(current.as_deref(), fingerprint, owed) else {
            return Ok(GatewayRoll::Unchanged);
        };
        Box::pin(
            self.api
                .patch(&self.name, &PatchParams::default(), &Patch::Merge(&patch)),
        )
        .await
        .map_err(kube)?;
        Ok(GatewayRoll::Rolled)
    }
}

/// Settings for the renewal loop.
pub struct Settings {
    /// Where the identity lives when no `GridNetwork` names it.
    defaults: Target,
    /// The gateway Deployment to roll after a renewal: namespace and name.
    gateway: Option<(String, String)>,
    /// The modes the install declares, which apply until a `GridNetwork` exists.
    install: GridModes,
    /// The declared peer trust, sent by the `GridNetwork` reconcile when it changes.
    trust_changes: Option<tokio::sync::watch::Receiver<PeerTrustMode>>,
    /// Set while a signer to renew through is named.
    configured: Arc<AtomicBool>,
    /// The site the identity names, whose `GridSite` is this cluster's own.
    site: Option<String>,
}

impl Default for Settings {
    fn default() -> Self {
        Self::new()
    }
}

impl Settings {
    /// Renew the identity in the default Secrets through the signer this site's `GridSite` names.
    #[must_use]
    pub fn new() -> Self {
        Self {
            defaults: Target::defaults(),
            gateway: None,
            install: GridModes::WITHOUT_NETWORK,
            trust_changes: None,
            configured: Arc::new(AtomicBool::new(false)),
            site: None,
        }
    }

    /// Renew through the `GridSite` named `site`, the name the identity carries.
    #[must_use]
    pub fn with_site(mut self, site: Option<String>) -> Self {
        self.site = site;
        self
    }

    /// Publish into `configured` whether a signer to renew through is named.
    #[must_use]
    pub fn with_configured(mut self, configured: Arc<AtomicBool>) -> Self {
        self.configured = configured;
        self
    }

    /// Roll the gateway Deployment `name` in `namespace` after each renewal.
    #[must_use]
    pub fn with_gateway(mut self, namespace: &str, name: &str) -> Self {
        self.gateway = Some((namespace.to_owned(), name.to_owned()));
        self
    }

    /// Follow the install's modes until a `GridNetwork` exists, and check again as soon
    /// as the declared peer trust changes.
    #[must_use]
    pub fn with_declared_trust(
        mut self,
        install: GridModes,
        trust_changes: tokio::sync::watch::Receiver<PeerTrustMode>,
    ) -> Self {
        self.install = install;
        self.trust_changes = Some(trust_changes);
        self
    }

    /// The gateway to roll, if one is configured.
    fn gateway_in(&self, client: &Client) -> Option<KubeGateway> {
        self.gateway
            .as_ref()
            .map(|(namespace, name)| KubeGateway::new(client.clone(), namespace, name))
    }
}

/// This site's `GridSite` and the others wrongly carrying `spec.local`, as listed this round.
struct Own {
    /// The `GridSite` the identity names, when declared.
    site: Option<GridSite>,
    /// `GridSite`s carrying `spec.local` under another name.
    strays: Vec<GridSite>,
}

/// List the `GridSite`s and pick this site's, the one `name` names, clearing what a
/// stray left behind.
async fn own_site(client: &Client, name: Option<&str>) -> Result<Own, RenewError> {
    let sites: Api<GridSite> = Api::all(client.clone());
    let listed = Box::pin(sites.list(&ListParams::default()))
        .await
        .map_err(|e| RenewError::Kube(e.to_string()))?
        .items;
    Box::pin(local::clear(client, &local::stale(&listed, name))).await;
    Ok(match local::local(&listed, name) {
        Local::Named { site, strays } => Own {
            site: site.cloned(),
            strays: strays.into_iter().cloned().collect(),
        },
        // Without the identity's name, no GridSite is this site's.
        Local::Absent | Local::One(_) | Local::Several(_) => Own {
            site: None,
            strays: Vec::new(),
        },
    })
}

/// What this site's `GridSite` declares about renewal.
#[derive(Debug, PartialEq, Eq)]
enum Declared<'site> {
    /// A signer whose rotation is Enabled.
    Renews(&'site SignerEndpoint),
    /// A signer whose rotation is Disabled: it enrolls only.
    Disabled,
    /// No `spec.local`, or none naming a signer.
    NoSigner,
}

/// Read `spec.local` of `site`, this site's `GridSite` if declared.
fn declared(site: Option<&GridSite>) -> Declared<'_> {
    match site.and_then(|site| site.spec.local.as_ref()) {
        Some(LocalSite {
            signer_endpoint: Some(endpoint),
            ..
        }) => match endpoint.rotation {
            Rotation::Enabled => Declared::Renews(endpoint),
            Rotation::Disabled => Declared::Disabled,
        },
        Some(LocalSite {
            signer_endpoint: None, ..
        })
        | None => Declared::NoSigner,
    }
}

/// The signer this round renews through, else why nothing renews.
#[derive(Debug, PartialEq, Eq)]
enum Resolved {
    /// Renew through this signer.
    Signer(Endpoint),
    /// Nothing renews.
    Off(Off),
}

/// Resolve the signer `spec.local.signerEndpoint` on this site's `GridSite` names, with its pin.
async fn resolve_signer(client: &Client, own: &Own) -> Result<Resolved, RenewError> {
    let endpoint = match declared(own.site.as_ref()) {
        Declared::Renews(endpoint) => endpoint,
        Declared::Disabled => return Ok(Resolved::Off(Off::Disabled)),
        Declared::NoSigner => return Ok(Resolved::Off(Off::NoSigner)),
    };
    let base = super::signer_url(&endpoint.url).map_err(RenewError::Material)?;
    let pin = match &endpoint.ca_bundle_ref {
        Some(reference) => Some(declared_pin(client, reference).await?),
        None => None,
    };
    Ok(Resolved::Signer(Endpoint { base, pin }))
}

/// The PEM `local.signerEndpoint.caBundleRef` names; a bundle that does not opt in is material, not the API.
async fn declared_pin(client: &Client, reference: &BundleRef) -> Result<String, RenewError> {
    Bundles::new(client, client.default_namespace())
        .pem(reference)
        .await
        .map_err(|error| match error {
            BundleError::Api(why) => RenewError::Kube(why),
            BundleError::Refused(why) => RenewError::Material(format!("local.signerEndpoint.caBundleRef: {why}")),
        })?
        .map(|(pem, _version)| pem)
        .ok_or_else(|| RenewError::Material(format!("local.signerEndpoint.caBundleRef {} absent", reference.name)))
}

/// What renewal keeps across rounds: the identity store, the renewal clients and the gateway.
pub struct Rounds {
    /// The API.
    client: Client,
    /// The operator namespace, where the identity lives.
    namespace: String,
    /// The identity Secret.
    store: KubeIdentity,
    /// The renewal clients kept across rounds.
    clients: Clients,
    /// The gateway to roll after a renewal.
    gateway: Option<KubeGateway>,
    /// The modes the install declares, which apply until a `GridNetwork` exists.
    install: GridModes,
}

impl Rounds {
    /// Renewal rounds for `settings` against `client`.
    #[must_use]
    pub fn new(client: Client, settings: &Settings) -> Self {
        let namespace = client.default_namespace().to_owned();
        Self {
            store: KubeIdentity(Api::namespaced(client.clone(), &namespace)),
            clients: Clients::default(),
            gateway: settings.gateway_in(&client),
            install: settings.install,
            client,
            namespace,
        }
    }

    /// One round: resolve the identity Secrets, check the identity through its signer,
    /// report on this site's `GridSite`, and roll the gateway after a renewal. Returns
    /// whether a renewal replaced the identity.
    pub async fn round(&self, settings: &Settings) -> bool {
        matches!(self.checked(settings).await, Ok(Checked::Renewed(_)))
    }

    /// One round, with what the check found.
    async fn checked(&self, settings: &Settings) -> Result<Checked, RenewError> {
        let loop_ = Loop {
            client: &self.client,
            store: &self.store,
            clients: &self.clients,
            install: self.install,
        };
        let target = Box::pin(super::resolve_target(
            &self.client,
            &self.namespace,
            &settings.defaults,
            super::STEP_BACKOFF,
        ))
        .await;
        let round = Box::pin(tick(&loop_, settings, &target)).await;
        roll_after_renewal(&self.store, self.gateway.as_ref(), &target, &round.checked).await;
        round.checked
    }
}

/// Keep the site identity renewed for the life of the process.
#[expect(
    clippy::infinite_loop,
    reason = "a background renewer runs for the life of the process"
)]
pub async fn run(client: Client, mut settings: Settings) {
    let rounds = Rounds::new(client, &settings);
    let mut last: Option<String> = None;
    let mut failures = 0_u32;
    loop {
        let checked = Box::pin(rounds.checked(&settings)).await;
        let wait = settle(&checked, &mut last, &mut failures);
        pause(jittered(wait), &mut settings.trust_changes).await;
    }
}

/// One round: resolve this site's `GridSite`, check `target` through its signer, and
/// report on it.
async fn tick(loop_: &Loop<'_>, settings: &Settings, target: &Result<Target, super::EnrollError>) -> Round {
    let own = match Box::pin(own_site(loop_.client, settings.site.as_deref())).await {
        Ok(own) => own,
        Err(error) => return Round::failed(error),
    };
    let round = match target {
        Err(error) => Round::failed(RenewError::Kube(error.to_string())),
        Ok(target) => Box::pin(check_round(loop_, target, &own)).await,
    };
    settings.configured.store(round.signer.is_some(), Ordering::Relaxed);
    if let Ok(target) = target {
        Box::pin(report_round(
            loop_.client,
            &own,
            target,
            &round,
            settings.site.as_deref(),
        ))
        .await;
    }
    round
}

/// What every round of the loop reaches.
struct Loop<'run> {
    /// The API.
    client: &'run Client,
    /// The identity Secret.
    store: &'run KubeIdentity,
    /// The renewal clients kept across rounds.
    clients: &'run Clients,
    /// The modes the install declares, which apply until a `GridNetwork` exists.
    install: GridModes,
}

/// What the identity Secret held this round.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Identity {
    /// The Secret or a key is absent.
    Missing(String),
    /// The certificate cannot be parsed.
    Unreadable(String),
    /// Expired at this instant.
    Expired(OffsetDateTime),
    /// Peers under spiffe trust refuse it: no X.509-SVID profile.
    Unusable,
    /// Usable until this instant.
    Valid(OffsetDateTime),
}

/// One round's check, the signer it went through, and the identity it read.
struct Round {
    /// What the check found or did.
    checked: Result<Checked, RenewError>,
    /// The signer's renewal URL, when one was named and renews.
    signer: Option<String>,
    /// The identity as read, `None` when the round failed before reading it.
    identity: Option<Identity>,
    /// Why `Accepted` is `False`: the signer's rotation is Enabled under pin peer trust.
    refused: Option<String>,
}

impl Round {
    /// A round that did not reach a check.
    const fn failed(error: RenewError) -> Self {
        Self {
            checked: Err(error),
            signer: None,
            identity: None,
            refused: None,
        }
    }
}

/// The peer trust the grid declares, and whether it lets this site renew.
#[derive(Debug, PartialEq, Eq)]
struct Trust {
    /// Whether a renewed leaf would be trusted.
    renews: bool,
    /// Who declares the mode, for the `Accepted` message when it refuses renewal.
    declared: String,
}

impl Trust {
    /// The `Accepted` message when the signer's rotation is Enabled under this trust.
    fn refusal(&self) -> String {
        format!(
            "spec.local.signerEndpoint.rotation is Enabled but {}, which does not renew; set rotation Disabled or \
             peerTrust.mode spiffe",
            self.declared
        )
    }
}

/// The trust the grid declares: the sole `GridNetwork`'s peer trust, else the install's.
async fn grid_trust(client: &Client, install: GridModes) -> Result<Trust, RenewError> {
    let networks: Api<GridNetwork> = Api::all(client.clone());
    let declared = Box::pin(networks.list(&ListParams::default()))
        .await
        .map_err(|e| RenewError::Kube(e.to_string()))?;
    Ok(declared_trust(&declared.items, install))
}

/// The identity as read this round, with when its renewal is due.
struct Read {
    /// The identity.
    held: Held,
    /// When its renewal is due.
    due: Due,
    /// Its expiry.
    not_after: OffsetDateTime,
}

/// Read the identity in `target`, or the round that reports why it could not be.
async fn read_identity(loop_: &Loop<'_>, target: &Target) -> Result<Read, Round> {
    let held = match loop_.store.read(target).await {
        Ok(held) => held,
        Err(RenewError::Missing(what)) => {
            return Err(Round {
                checked: Err(RenewError::Missing(what.clone())),
                identity: Some(Identity::Missing(what)),
                ..Round::failed(RenewError::Missing(String::new()))
            });
        },
        Err(error) => return Err(Round::failed(error)),
    };
    let (not_before, not_after) = validity(&held).map_err(|error| Round {
        identity: Some(Identity::Unreadable(error.to_string())),
        ..Round::failed(error)
    })?;
    metrics::set_site_identity_expiry(not_after.unix_timestamp());
    Ok(Read {
        due: due(not_before, not_after, OffsetDateTime::now_utc()),
        held,
        not_after,
    })
}

/// What `IdentityValid` reads off `read` in a grid that `renews` or not.
fn identity_of(read: &Read, renews: bool) -> Identity {
    match read.due {
        Due::Expired => Identity::Expired(read.not_after),
        Due::At(_) | Due::Now if renews && !certs::has_svid_profile(&read.held.cert).unwrap_or(false) => {
            Identity::Unusable
        },
        Due::At(_) | Due::Now => Identity::Valid(read.not_after),
    }
}

/// The signer this round renews through, else why nothing renews, else why it could not
/// be resolved. A signer whose rotation is Enabled under a trust that does not renew is
/// held back, since a pinned peer would refuse the renewed leaf.
async fn through(client: &Client, own: &Own, trust: &Trust) -> Result<Endpoint, Result<Off, RenewError>> {
    match Box::pin(resolve_signer(client, own)).await {
        Ok(Resolved::Signer(signer)) if trust.renews => Ok(signer),
        Ok(Resolved::Signer(_)) => Err(Ok(Off::UnsupportedUnderPin)),
        Ok(Resolved::Off(why)) => Err(Ok(why)),
        Err(error) => Err(Err(error)),
    }
}

/// Read the identity in `target` and check it through the signer resolved for `own`.
async fn check_round(loop_: &Loop<'_>, target: &Target, own: &Own) -> Round {
    let read = match Box::pin(read_identity(loop_, target)).await {
        Ok(read) => read,
        Err(round) => return round,
    };
    let trust = match Box::pin(grid_trust(loop_.client, loop_.install)).await {
        Ok(trust) => trust,
        Err(error) => return Round::failed(error),
    };
    let identity = Some(identity_of(&read, trust.renews));
    match Box::pin(through(loop_.client, own, &trust)).await {
        Ok(signer) => Box::pin(renew_through(loop_, target, read, signer, identity)).await,
        Err(Ok(why)) => Round {
            checked: Ok(Checked::Off { why, due: read.due }),
            signer: None,
            identity,
            refused: (why == Off::UnsupportedUnderPin).then(|| trust.refusal()),
        },
        Err(Err(error)) => Round {
            identity,
            ..Round::failed(error)
        },
    }
}

/// Renew the identity `read` from `target` through `signer` if it is due.
async fn renew_through(
    loop_: &Loop<'_>,
    target: &Target,
    read: Read,
    endpoint: Endpoint,
    identity: Option<Identity>,
) -> Round {
    let url = endpoint.renewal_url();
    let signer = HttpsSigner::new(endpoint.base, endpoint.pin, loop_.clients);
    Round {
        checked: Box::pin(check_held(
            loop_.store,
            &signer,
            target,
            read.held,
            OffsetDateTime::now_utc(),
        ))
        .await,
        signer: Some(url),
        identity,
        refused: None,
    }
}

/// Report the round on this site's `GridSite`: `Enrolled` by the identity in `target`,
/// `IdentityValid` by what was read, `RotationOnSchedule` by the check, `Available` from
/// those, and `spec.local` as ignored on every stray.
async fn report_round(client: &Client, own: &Own, target: &Target, round: &Round, name: Option<&str>) {
    if let Some(name) = name {
        let strays: Vec<&GridSite> = own.strays.iter().collect();
        Box::pin(local::report_strays(client, &strays, name)).await;
    }
    let Some(site) = &own.site else {
        return;
    };
    let verdicts = round_verdicts(round, target, local::enrolled(site));
    let written = Box::pin(local::report_many(client, site, &verdicts)).await;
    Box::pin(local::report_available(client, &written)).await;
}

/// The conditions `round` writes on the own site, as (type, manager, verdict). An identity
/// present reports `Enrolled` only while the site does not read enrolled, so `Issued` stays.
/// `Accepted` is `False` while the signer's rotation conflicts with the grid's peer trust,
/// a conflict CEL cannot see, and `True` again once either side changes.
fn round_verdicts(round: &Round, target: &Target, enrolled: bool) -> Vec<(&'static str, &'static str, Verdict)> {
    let mut verdicts = Vec::with_capacity(5);
    let accepted = round.refused.as_ref().map_or_else(Verdict::own_site, |why| {
        Verdict::new("False", "RotationUnsupportedUnderPinTrust", why.clone())
    });
    verdicts.push((local::ACCEPTED, local::ACCEPTED_FIELD_MANAGER, accepted));
    if !enrolled && !matches!(round.identity, Some(Identity::Missing(_))) {
        verdicts.push((
            local::ENROLLED,
            local::FIELD_MANAGER,
            Verdict::present(&target.site_secret),
        ));
    }
    if let Some(identity) = &round.identity {
        verdicts.push((
            local::IDENTITY_VALID,
            local::IDENTITY_FIELD_MANAGER,
            identity_verdict(identity),
        ));
    }
    let rotation = rotation_verdict(&round.checked, round.signer.as_ref());
    verdicts.push((
        local::DEGRADED,
        local::DEGRADED_FIELD_MANAGER,
        local::degraded(&rotation),
    ));
    verdicts.push((local::ROTATION_ON_SCHEDULE, local::RENEWAL_FIELD_MANAGER, rotation));
    verdicts
}

/// What `IdentityValid` says about `identity`.
fn identity_verdict(identity: &Identity) -> Verdict {
    match identity {
        Identity::Missing(what) => Verdict::new("False", "IdentityMissing", format!("site identity absent: {what}")),
        Identity::Unreadable(why) => Verdict::new("False", "CertificateUnreadable", why.clone()),
        Identity::Expired(at) => Verdict::new(
            "False",
            "CertificateExpired",
            format!("the site identity expired at {at}; re-enroll this site with a new site token"),
        ),
        Identity::Unusable => Verdict::new(
            "False",
            "CertificateUnusable",
            "the certificate lacks the X.509-SVID profile peers under spiffe trust require; re-enroll through a \
             current signer"
                .to_owned(),
        ),
        Identity::Valid(at) => Verdict::new("True", "AsExpected", format!("valid until {at}")),
    }
}

/// What `RotationOnSchedule` says after a check through the signer at `url`: whether this
/// identity renews before it expires.
fn rotation_verdict(checked: &Result<Checked, RenewError>, url: Option<&String>) -> Verdict {
    match checked {
        Err(error @ (RenewError::Unauthenticated(_) | RenewError::Refused(_))) => {
            Verdict::new("False", "RenewalRefused", error.to_string())
        },
        Err(error @ RenewError::SignerDisabled(_)) => Verdict::new("False", "RotationOverdue", error.to_string()),
        Err(error) => Verdict::new("False", "RenewalFailing", error.to_string()),
        Ok(Checked::Waiting(_) | Checked::Renewed(_)) => match url {
            Some(url) => Verdict::new(
                "True",
                "AsExpected",
                format!("renews through {url}, from spec.local.signerEndpoint"),
            ),
            None => Verdict::new(
                "False",
                "RenewalFailing",
                "renewal went through no named signer".to_owned(),
            ),
        },
        Ok(Checked::Expired(at)) => Verdict::new(
            "False",
            "RotationOverdue",
            format!("the site identity expired at {at} and cannot renew; re-enroll this site with a new site token"),
        ),
        Ok(Checked::Off { why, due: Due::At(_) }) => Verdict::new("True", ROTATION_DISABLED, why.message().to_owned()),
        Ok(Checked::Off { why, .. }) => Verdict::new(
            "False",
            "RotationOverdue",
            format!("renewal is due and nothing renews: {}", why.message()),
        ),
    }
}

/// Roll the gateway when a renewal replaced the identity it mounts.
async fn roll_after_renewal(
    store: &KubeIdentity,
    gateway: Option<&KubeGateway>,
    target: &Result<Target, super::EnrollError>,
    checked: &Result<Checked, RenewError>,
) {
    if let (Some(renewed), Ok(target), Some(gateway)) = (rolls_after(checked), target, gateway) {
        log_roll(
            Box::pin(roll_gateway(store, gateway, target, renewed)).await,
            gateway,
            renewed,
        );
    }
}

/// Sleep for `wait`, or until the declared peer trust changes.
async fn pause(wait: Duration, trust_changes: &mut Option<tokio::sync::watch::Receiver<PeerTrustMode>>) {
    let Some(changes) = trust_changes.as_mut() else {
        tokio::time::sleep(wait).await;
        return;
    };
    tokio::select! {
        () = tokio::time::sleep(wait) => {},
        result = changes.changed() => {
            // A closed channel never changes again, so only the timer is left.
            if result.is_err() {
                *trust_changes = None;
            }
        },
    }
}

/// The trust the declared grid puts this site under. Without a `GridNetwork`, the
/// install's modes apply.
fn declared_trust(networks: &[GridNetwork], install: GridModes) -> Trust {
    // The mode as the admin writes it, lowercase.
    let mode = |modes: GridModes| {
        serde_json::to_value(modes.trust)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_default()
    };
    match networks {
        [] => Trust {
            renews: install.renews(),
            declared: format!(
                "the install declares peerTrust.mode {} and no GridNetwork exists yet",
                mode(install)
            ),
        },
        [network] => Trust {
            renews: GridModes::of(network).renews(),
            declared: format!(
                "GridNetwork {} declares peerTrust.mode {}",
                network.metadata.name.as_deref().unwrap_or_default(),
                mode(GridModes::of(network))
            ),
        },
        // One operator serves one grid: with several declared, renew under none of them.
        _ => Trust {
            renews: false,
            declared: "several GridNetworks are declared, so no peer trust applies".to_owned(),
        },
    }
}

/// Whether the declared grid lets this site renew.
#[cfg(test)]
fn declared_renews(networks: &[GridNetwork], install: GridModes) -> bool {
    declared_trust(networks, install).renews
}

/// Whether `checked` rolls the gateway: after a renewal, or on a later check that
/// catches up a missed roll. `Some(true)` after a renewal.
const fn rolls_after(checked: &Result<Checked, RenewError>) -> Option<bool> {
    match checked {
        Ok(Checked::Renewed(_)) => Some(true),
        Ok(Checked::Waiting(_)) => Some(false),
        Ok(Checked::Expired(_) | Checked::Off { .. }) | Err(_) => None,
    }
}

/// Log a gateway roll; a missing gateway once.
#[expect(clippy::cognitive_complexity, reason = "one tracing call per outcome")]
fn log_roll(rolled: Result<GatewayRoll, RenewError>, gateway: &KubeGateway, renewed: bool) {
    let deployment = gateway.name.as_str();
    match rolled {
        Ok(GatewayRoll::Rolled) => tracing::info!(deployment, "rolled the gateway onto the rotated site identity"),
        Ok(GatewayRoll::Unchanged) => tracing::debug!(deployment, "gateway already runs the rotated site identity"),
        // Only a renewal's roll reports a missing gateway, once; a recheck has nothing to report.
        Ok(GatewayRoll::NoGateway) if renewed && !gateway.missing_logged.swap(true, Ordering::Relaxed) => {
            tracing::info!(
                deployment,
                "no gateway Deployment to roll onto the rotated site identity"
            );
        },
        Ok(GatewayRoll::NoGateway) => tracing::debug!(deployment, "no gateway Deployment to roll"),
        Err(error) if renewed => {
            tracing::warn!(deployment, %error, "rolling the gateway onto the rotated site identity failed; retrying");
        },
        Err(error) => {
            tracing::debug!(deployment, %error, "rolling the gateway onto the rotated site identity failed again");
        },
    }
}

/// Log a check once per change of state, count it, and return the wait before the next.
fn settle(checked: &Result<Checked, RenewError>, last: &mut Option<String>, failures: &mut u32) -> Duration {
    let (state, wait) = match checked {
        Ok(Checked::Waiting(at)) => (
            "waiting".to_owned(),
            until(*at, OffsetDateTime::now_utc()).min(CHECK_EVERY),
        ),
        // Re-read soon, so the next renewal is scheduled from the new leaf.
        Ok(Checked::Renewed(_)) => ("rotated".to_owned(), RETRY_INITIAL),
        Ok(Checked::Expired(_)) => ("expired".to_owned(), CHECK_EVERY),
        Ok(Checked::Off { why, due }) => (format!("off {} {due:?}", why.label()), CHECK_EVERY),
        Err(error) => (error.to_string(), retry_delay(failures.saturating_add(1))),
    };
    report(checked, last.as_deref() != Some(state.as_str()), wait);
    *failures = if checked.is_err() {
        failures.saturating_add(1)
    } else {
        0
    };
    *last = Some(state);
    wait
}

/// Log and count a check: a renewal always, anything else only when it changed.
#[expect(clippy::cognitive_complexity, reason = "one tracing call per outcome")]
fn report(checked: &Result<Checked, RenewError>, changed: bool, wait: Duration) {
    match checked {
        Ok(Checked::Renewed(until)) => {
            metrics::record_site_identity_renewal("rotated");
            tracing::info!(not_after = %until, "site identity rotated");
        },
        Ok(Checked::Waiting(at)) if changed => tracing::info!(renew_after = %at, "site identity rotation scheduled"),
        Ok(Checked::Expired(at)) if changed => {
            metrics::record_site_identity_renewal("expired");
            tracing::error!(
                not_after = %at,
                "site identity expired and cannot rotate; re-enroll this site with a new site token"
            );
        },
        Ok(Checked::Off { why, due }) if changed => {
            tracing::info!(why = why.label(), due = ?due, "{}", why.message());
        },
        Err(error) if changed => {
            metrics::record_site_identity_renewal(error.result());
            tracing::warn!(%error, retry_in_s = wait.as_secs(), "site identity rotation failed");
        },
        Err(error) => {
            metrics::record_site_identity_renewal(error.result());
            tracing::debug!(%error, retry_in_s = wait.as_secs(), "site identity rotation failed again");
        },
        Ok(_) => {},
    }
}

/// Time from `now` to `at`, zero once passed.
fn until(at: OffsetDateTime, now: OffsetDateTime) -> Duration {
    (at - now).try_into().unwrap_or(Duration::ZERO)
}

/// The wait after `failures` consecutive failures.
fn retry_delay(failures: u32) -> Duration {
    RETRY_INITIAL
        .saturating_mul(2_u32.saturating_pow(failures.saturating_sub(1)))
        .min(RETRY_MAX)
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests;
