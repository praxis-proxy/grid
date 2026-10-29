//! Renew the site identity before it expires, in place, with the current certificate.

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, Ordering},
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
use time::OffsetDateTime;
use zeroize::Zeroizing;

use super::{
    Issued, Signer, SignerError, Target, jittered,
    local::{self, Verdict},
    signer::{Clients, HttpsSigner, LeafError, SignerUrl, issued_leaf},
};
use crate::{
    controller::grid_network::GridModes,
    crd::grid_network::{GridNetwork, PeerTrustMode},
    metrics,
    operator_config::{BundleRef, ConfigWatch, Loaded, Rotation, Section, SignerEndpoint, Snapshot},
    shutdown::Shutdown,
};


/// Longest wait between checks, so an identity replaced out of band is still watched.
const CHECK_EVERY: Duration = Duration::from_secs(60 * 60);

/// How often a waiting loop reads the identity Secret's version, so a write by any manager,
/// cert-manager among them, is checked within this rather than at the next due time.
pub(crate) const IDENTITY_POLL: Duration = Duration::from_secs(30);

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
    /// The service received no client certificate: TLS ended before it.
    #[error(
        "the signer did not receive this site's certificate ({0}); renewal needs TLS end to end to the \
         signer, through a passthrough Route, a load balancer or the in-cluster Service, not a terminating \
         Route or an intercepting proxy"
    )]
    NeedsPassthrough(String),
    /// The service answered with a certificate this site cannot use.
    #[error("rotation response invalid: {0}")]
    Invalid(String),
    /// The service refuses every rotation: rotation is turned off on the hub.
    #[error("rotation disabled at the signer ({0}); turn it on before the identity expires")]
    SignerDisabled(String),
    /// The process stood down before the signer answered. The kept request goes out again.
    #[error("renewal stood down before the signer answered; the kept request is sent again on the next start")]
    StoodDown,
    /// The signer's serving certificate is outside its validity window: the signer must rotate it.
    #[error(
        "the signer's serving certificate is expired or not yet valid ({0}); the signer must rotate its serving \
         certificate, and renewal retries"
    )]
    SignerLapsed(String),
    /// The signer's serving certificate is trusted by neither the grid CA held nor the pin.
    #[error(
        "the signer's serving certificate is not trusted ({0}); pin the CA that issued it in \
         site.signerEndpoint.caBundleRef, or serve one the grid CA issued"
    )]
    Untrusted(String),
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
            | Self::SignerDisabled(_)
            | Self::Untrusted(_)
            | Self::NeedsPassthrough(_)
            | Self::SignerLapsed(_)
            | Self::StoodDown => "failed",
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
    /// The operator does not renew this identity: why, whether a renewal is already due, and
    /// the cert-manager Certificate the Secret names as its renewer, if any.
    Off {
        /// Why.
        why: Off,
        /// Whether the renewal this would have made is due.
        due: Due,
        /// The Certificate in the Secret's [`CERT_MANAGER_CERTIFICATE`] annotation.
        renewer: Option<String>,
    },
}

/// Why nothing renews the identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Off {
    /// The signer's rotation is Enabled under pin peer trust, where a pinned peer refuses
    /// a renewed leaf: the conflict is reported on `Accepted` and renewal is held back.
    UnsupportedUnderPin,
    /// The operator config is refused for this site, as `Accepted` reports, so no section of
    /// it is acted on.
    Refused,
    /// The operator config names no signer.
    NoSigner,
    /// The signer's rotation is Disabled.
    Disabled,
}

/// The annotation cert-manager puts on a Secret it issues, naming its Certificate.
pub(super) const CERT_MANAGER_CERTIFICATE: &str = "cert-manager.io/certificate-name";

/// The annotation cert-manager puts on a Secret it issues, counting its issuances from 1.
const CERT_MANAGER_REVISION: &str = "cert-manager.io/certificate-revision";

/// Whether cert-manager has issued the identity in a Secret with `metadata` more than once.
fn reissued(metadata: &kube::api::ObjectMeta) -> bool {
    metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(CERT_MANAGER_REVISION))
        .and_then(|revision| revision.parse::<u64>().ok())
        .is_some_and(|revision| revision > 1)
}

impl Off {
    /// What the admin reads.
    const fn message(self) -> &'static str {
        match self {
            Self::UnsupportedUnderPin => {
                "rotation is held back: site.signerEndpoint.rotation is Enabled under pin peer trust, see Accepted"
            },
            Self::Refused => "renewal is held back: the operator config is refused for this site, see Accepted",
            Self::NoSigner => "the operator config names no site.signerEndpoint",
            Self::Disabled => "site.signerEndpoint.rotation is Disabled",
        }
    }

    /// A stable word for logs.
    const fn label(self) -> &'static str {
        match self {
            Self::UnsupportedUnderPin => "unsupported-under-pin",
            Self::Refused => "config-refused",
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
    /// The cert-manager Certificate the Secret names as its renewer.
    pub(super) renewer: Option<String>,
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
    check_held(store, signer, target, held, (now, None)).await
}

/// The validity of the certificate `held`.
fn validity(held: &Held) -> Result<(OffsetDateTime, OffsetDateTime), RenewError> {
    certs::cert_validity(&held.cert).map_err(|e| RenewError::Material(format!("certificate: {e}")))
}

/// Renew `held`, the identity in `target`, if it is due at `now`, giving up the request to the
/// signer once `stop` triggers.
async fn check_held<S: IdentityStore + Sync, G: Signer + Sync>(
    store: &S,
    signer: &G,
    target: &Target,
    held: Held,
    (now, stop): (OffsetDateTime, Option<&Shutdown>),
) -> Result<Checked, RenewError> {
    let (not_before, not_after) = validity(&held)?;
    metrics::set_site_identity_expiry(not_after.unix_timestamp());
    match due(not_before, not_after, now) {
        Due::At(at) => Ok(Checked::Waiting(at)),
        Due::Expired => Ok(Checked::Expired(not_after)),
        Due::Now => renew(store, signer, target, (held, stop)).await,
    }
}

/// Renew `held` now and store the result in place. The request is given up once `stop`
/// triggers, since the kept key makes a resend safe; the write is not.
async fn renew<S: IdentityStore + Sync, G: Signer + Sync>(
    store: &S,
    signer: &G,
    target: &Target,
    (mut held, stop): (Held, Option<&Shutdown>),
) -> Result<Checked, RenewError> {
    let site = site_of(&held.cert)?;
    let (csr, key) = pending(store, target, &mut held, &site).await?;
    let request = signer.renew(&held, &csr);
    let answer = match stop {
        Some(stop) => tokio::select! {
            biased;
            () = stop.triggered() => return Err(RenewError::StoodDown),
            answer = request => answer,
        },
        None => request.await,
    };
    let issued = answer.map_err(renew_error)?;
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
        SignerError::NoClientCertificate(detail) => RenewError::NeedsPassthrough(detail),
        SignerError::Forbidden(detail) => RenewError::Refused(detail),
        SignerError::Disabled(detail) => RenewError::SignerDisabled(detail),
        SignerError::NameTaken(detail)
        | SignerError::NameDeleted(detail)
        | SignerError::Refused(detail)
        | SignerError::Transport { detail, .. } => RenewError::Transport(detail),
        SignerError::Untrusted { lapsed: true, detail } => RenewError::SignerLapsed(detail),
        SignerError::Untrusted { lapsed: false, detail } => RenewError::Untrusted(detail),
        SignerError::Request(detail) => RenewError::Material(detail),
        SignerError::Invalid(detail) => RenewError::Invalid(detail),
    }
}

/// The renewed leaf, if it is this site's, from the CA it already holds, for the CSR's key.
fn verified(enrollment: &Issued, held_ca: &str, site: &str, csr_pem: &str) -> Result<String, RenewError> {
    issued_leaf(enrollment, held_ca, site, csr_pem)
        .map(|leaf| leaf.cert)
        .map_err(|refused| {
            RenewError::Invalid(match refused {
                LeafError::Ca(e) => format!("returned CA is not this site's grid CA ({e})"),
                LeafError::Certificate(e) => format!("certificate: {e}"),
                LeafError::NotForSite(e) => format!("certificate is not {site}'s ({e})"),
                LeafError::Key(e) => e,
                LeafError::KeyMismatch => "certificate does not carry the requested key".to_owned(),
            })
        })
}

/// The cert-manager Certificate a Secret's annotation names as its renewer.
pub(super) fn renewer_of(metadata: &kube::api::ObjectMeta) -> Option<String> {
    metadata
        .annotations
        .as_ref()?
        .get(CERT_MANAGER_CERTIFICATE)
        .filter(|name| !name.is_empty())
        .cloned()
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
        let renewer = renewer_of(&site.metadata);
        let reissued = reissued(&site.metadata);
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
            rotated: reissued || data.contains_key(PREVIOUS_CERT),
            renewer,
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

/// The signer a renewal goes to, read from `site.signerEndpoint` in the operator config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Endpoint<'config> {
    /// The signer's base URL. The signer is trusted through the grid CA the site holds, or
    /// the bundle `pin` names.
    pub(super) base: SignerUrl,
    /// `site.signerEndpoint.caBundleRef`, the bundle pinning the signer's serving certificate.
    pub(super) pin: Option<&'config BundleRef>,
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
    /// The site the identity names, whose `GridSite` is this cluster's own.
    site: Option<String>,
    /// The operator config naming the signer.
    config: ConfigWatch,
}

impl Default for Settings {
    fn default() -> Self {
        Self::new()
    }
}

impl Settings {
    /// Renew the identity in the default Secrets through the signer the operator config names.
    #[must_use]
    pub fn new() -> Self {
        Self {
            defaults: Target::defaults(),
            gateway: None,
            install: GridModes::WITHOUT_NETWORK,
            trust_changes: None,
            site: None,
            config: ConfigWatch::fixed(Snapshot::absent()),
        }
    }

    /// Renew through the signer `config` names, checking again as soon as it changes.
    #[must_use]
    pub fn with_config(mut self, config: ConfigWatch) -> Self {
        self.config = config;
        self
    }

    /// Renew through the `GridSite` named `site`, the name the identity carries.
    #[must_use]
    pub fn with_site(mut self, site: Option<String>) -> Self {
        self.site = site;
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

/// What the operator config declares about renewal for the site `identity` names.
#[derive(Debug, PartialEq, Eq)]
enum Declared<'config> {
    /// A signer whose rotation is Enabled.
    Renews(&'config SignerEndpoint),
    /// A signer whose rotation is Disabled: it enrolls only.
    Disabled,
    /// No site section, or none naming a signer.
    NoSigner,
    /// Refused for this site, with the `Accepted` verdict saying why.
    Refused(Verdict),
}

/// Read the site section of `loaded` for the site `identity` names.
fn declared<'config>(loaded: &'config Loaded, identity: Option<&str>) -> Declared<'config> {
    match loaded.site() {
        Section::Unset => Declared::NoSigner,
        Section::Invalid(why) => Declared::Refused(Verdict::config_invalid(why)),
        Section::Site(site) => match identity {
            Some(identity) if identity != site.name => Declared::Refused(Verdict::name_mismatch(&site.name, identity)),
            _ => match &site.signer_endpoint {
                Some(endpoint) => match endpoint.rotation {
                    Rotation::Enabled => Declared::Renews(endpoint),
                    Rotation::Disabled => Declared::Disabled,
                },
                None => Declared::NoSigner,
            },
        },
    }
}

/// What `Accepted` says of `loaded` for the site `identity` names, under `trust`: the one
/// verdict the enrollment step and the renewal loop both write.
fn accepted(loaded: &Loaded, identity: Option<&str>, (networks, trust): (&[GridNetwork], &Trust)) -> Verdict {
    if let [network] = networks
        && network.spec.grid_id.is_empty()
    {
        return Verdict::grid_id_unset(network.metadata.name.as_deref().unwrap_or_default());
    }
    match declared(loaded, identity) {
        Declared::Refused(refusal) => refusal,
        Declared::Renews(_) if !trust.renews => trust.refusal(),
        Declared::Renews(_) | Declared::Disabled => Verdict::own_site(),
        Declared::NoSigner => match loaded.site() {
            Section::Site(_) => Verdict::own_site(),
            Section::Unset | Section::Invalid(_) => Verdict::no_site(matches!(loaded, Loaded::Absent | Loaded::Unread)),
        },
    }
}

/// [`accepted`] under the trust `networks` declare, for the enrollment step, which writes only
/// while one exists.
pub(super) fn accepted_over(loaded: &Loaded, identity: Option<&str>, networks: &[GridNetwork]) -> Verdict {
    accepted(
        loaded,
        identity,
        (networks, &declared_trust(networks, GridModes::WITHOUT_NETWORK)),
    )
}

/// The signer this round renews through, else why nothing renews.
#[derive(Debug, PartialEq, Eq)]
enum Resolved<'config> {
    /// Renew through this signer.
    Signer(Endpoint<'config>),
    /// Nothing renews.
    Off(Off),
}

/// Resolve the signer `site.signerEndpoint` names.
fn resolve_signer<'config>(declared: &Declared<'config>) -> Result<Resolved<'config>, RenewError> {
    let endpoint = match declared {
        Declared::Renews(endpoint) => endpoint,
        Declared::Disabled => return Ok(Resolved::Off(Off::Disabled)),
        Declared::NoSigner => return Ok(Resolved::Off(Off::NoSigner)),
        Declared::Refused(_) => return Ok(Resolved::Off(Off::Refused)),
    };
    let base = SignerUrl::parse(&endpoint.url).map_err(RenewError::Material)?;
    Ok(Resolved::Signer(Endpoint {
        base,
        pin: endpoint.ca_bundle_ref.as_ref(),
    }))
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
    /// Gives up a request to the signer, once the loop stands down.
    stop: Option<Shutdown>,
    /// The identity Secret the last round checked.
    checked_secret: std::sync::Mutex<Option<String>>,
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
            stop: None,
            checked_secret: std::sync::Mutex::default(),
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
            stop: self.stop.as_ref(),
        };
        let (round, target) = Box::pin(tick(&loop_, settings, &self.namespace)).await;
        roll_after_renewal(&self.store, self.gateway.as_ref(), &target, &round.checked).await;
        *self
            .checked_secret
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = target.ok().map(|target| target.site_secret);
        round.checked
    }

    /// The resource version of the identity Secret the last round checked, `None` when it is
    /// absent or no round named one.
    ///
    /// # Errors
    ///
    /// [`kube::Error`] when the read fails.
    pub(crate) async fn identity_version(&self) -> Result<Option<String>, kube::Error> {
        let name = self
            .checked_secret
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(name) = name else {
            return Ok(None);
        };
        let held = self.store.0.get_metadata_opt(&name).await?;
        Ok(held.and_then(|secret| secret.metadata.resource_version))
    }

    /// Resolve once the identity Secret is written, created or deleted after `seen`, read every
    /// [`IDENTITY_POLL`]. A failed read is not a change.
    pub(crate) async fn identity_moved(&self, seen: Option<String>) {
        loop {
            tokio::time::sleep(IDENTITY_POLL).await;
            match self.identity_version().await {
                Ok(now) if now != seen => return,
                Ok(_) => {},
                Err(error) => tracing::debug!(%error, "identity Secret version unread; not a change"),
            }
        }
    }
}

/// Keep the site identity renewed until `shutdown`.
///
/// A request to the signer is given up at once. An identity write or gateway roll already under
/// way finishes before this returns, so neither starts once the caller has seen it return.
pub async fn run(client: Client, mut settings: Settings, shutdown: Shutdown) {
    let rounds = Rounds {
        stop: Some(shutdown.clone()),
        ..Rounds::new(client, &settings)
    };
    let mut last: Option<String> = None;
    let mut failures = 0_u32;
    while !shutdown.is_triggered() {
        let checked = Box::pin(rounds.checked(&settings)).await;
        if matches!(checked, Err(RenewError::StoodDown)) {
            return;
        }
        let wait = settle(&checked, &mut last, &mut failures);
        let seen = Box::pin(rounds.identity_version()).await.ok().flatten();
        tokio::select! {
            biased;
            () = shutdown.triggered() => return,
            () = pause(jittered(wait), &mut settings.trust_changes, &mut settings.config) => {},
            () = Box::pin(rounds.identity_moved(seen)) => tracing::debug!("identity Secret written; checking it now"),
        }
    }
}

/// One round: list the `GridNetwork`s once for the identity Secrets they name, the trust
/// they declare and where to report, check the identity through its signer, and report.
async fn tick(loop_: &Loop<'_>, settings: &Settings, namespace: &str) -> (Round, Result<Target, RenewError>) {
    let listed = match Box::pin(list_networks(loop_.client)).await {
        Ok(listed) => listed,
        Err(error) => {
            return (
                Round::failed(error),
                Err(RenewError::Kube("GridNetworks not listed".to_owned())),
            );
        },
    };
    Box::pin(round_over(loop_, settings, &listed, namespace)).await
}

/// One round over `listed`, the `GridNetwork`s as this round sees them.
async fn round_over(
    loop_: &Loop<'_>,
    settings: &Settings,
    listed: &[GridNetwork],
    namespace: &str,
) -> (Round, Result<Target, RenewError>) {
    let target = super::target_for(listed, namespace, &settings.defaults).map_err(|e| RenewError::Kube(e.to_string()));
    let trust = declared_trust(listed, loop_.install);
    let snapshot = settings.config.current();
    let site = settings.site.as_deref();
    let declared = declared(&snapshot.loaded, site);
    let round = match &target {
        Err(error) => Round::failed(RenewError::Kube(error.to_string())),
        Ok(target) => Box::pin(check_round(loop_, target, site, (&declared, &trust))).await,
    };
    // A round given up for a stand-down says nothing new, and writing would hold the stand-down.
    if let (Ok(target), [own], false) = (&target, listed, matches!(round.checked, Err(RenewError::StoodDown))) {
        let accepted = accepted(&snapshot.loaded, site, (listed, &trust));
        Box::pin(report_round(
            loop_.client,
            (own, target),
            &round,
            (accepted, &snapshot.loaded),
        ))
        .await;
    }
    (round, target)
}

/// The `GridNetwork`s, listed.
async fn list_networks(client: &Client) -> Result<Vec<GridNetwork>, RenewError> {
    let networks: Api<GridNetwork> = Api::all(client.clone());
    Box::pin(networks.list(&ListParams::default()))
        .await
        .map(|list| list.items)
        .map_err(|e| RenewError::Kube(e.to_string()))
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
    /// Gives up a request to the signer.
    stop: Option<&'run Shutdown>,
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
    /// Not issued by the grid CA held, or outside its validity: what to fix.
    Untrusted(String),
    /// Issued for another site, or for no single one: what to fix.
    NameMismatch(String),
    /// The key is not the certificate's: what to fix.
    KeyMismatch(String),
    /// Missing what peers check, with the reason naming the field and what to fix.
    Unservable(&'static str, String),
    /// Usable until this instant, with the fingerprint peers pin.
    Valid(OffsetDateTime, String),
}

/// One round's check, the signer it went through, and the identity it read.
struct Round {
    /// What the check found or did.
    checked: Result<Checked, RenewError>,
    /// The signer's renewal URL, when one was named and renews.
    signer: Option<String>,
    /// The identity as read, `None` when the round failed before reading it.
    identity: Option<Identity>,
}

impl Round {
    /// A round that did not reach a check.
    const fn failed(error: RenewError) -> Self {
        Self {
            checked: Err(error),
            signer: None,
            identity: None,
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
    /// What `Accepted` says when the signer's rotation is Enabled under this trust.
    fn refusal(&self) -> Verdict {
        Verdict::new(
            "False",
            "RotationUnsupportedUnderPinTrust",
            format!(
                "site.signerEndpoint.rotation is Enabled but {}, which does not renew; set rotation Disabled or \
                 peerTrust.mode spiffe",
                self.declared
            ),
        )
    }
}

/// The identity as read this round, with when its renewal is due.
struct Read {
    /// The identity.
    held: Held,
    /// When its renewal is due.
    due: Due,
    /// Its expiry.
    not_after: OffsetDateTime,
    /// When its renewal falls due.
    renew_at: OffsetDateTime,
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
    let (not_before, not_after) = validity(&held).map_err(|error| {
        // Zero, not the last good notAfter, so an expiry alert fires on broken material.
        metrics::set_site_identity_expiry(0);
        Round {
            identity: Some(Identity::Unreadable(error.to_string())),
            ..Round::failed(error)
        }
    })?;
    metrics::set_site_identity_expiry(not_after.unix_timestamp());
    Ok(Read {
        due: due(not_before, not_after, OffsetDateTime::now_utc()),
        held,
        not_after,
        renew_at: renew_after(not_before, not_after),
    })
}

/// What `IdentityValid` reads off `read` from `target`, as `site`'s, in a grid that `renews`
/// or not.
fn identity_of(read: &Read, target: &Target, site: Option<&str>, renews: bool) -> Identity {
    if read.due == Due::Expired {
        return Identity::Expired(read.not_after);
    }
    if let Err(why) = held_by(&read.held, target, site) {
        return why;
    }
    if renews && !certs::has_svid_profile(&read.held.cert).unwrap_or(false) {
        return Identity::Unusable;
    }
    let fingerprint = certs::canonical_fingerprint(&read.held.cert).unwrap_or_default();
    Identity::Valid(read.not_after, fingerprint)
}

/// Whether `held` is `site`'s identity from the grid CA it holds, with its own key.
fn held_by(held: &Held, target: &Target, site: Option<&str>) -> Result<(), Identity> {
    let checked = match site {
        Some(site) => certs::verify_site_cert(&held.ca, &held.cert, site).map(drop),
        None => certs::verify_issued_by(&held.ca, &held.cert),
    };
    checked.map_err(|error| untrusted(&error, target, site))?;
    if let Some(site) = site {
        served_as(&held.cert, target, site)?;
    }
    if !certs::key_matches_cert(&held.key, &held.cert) {
        return Err(Identity::KeyMismatch(format!(
            "tls.key in Secret {} is not the key of its tls.crt; write the certificate and its key together",
            target.site_secret
        )));
    }
    Ok(())
}

/// Whether `cert`, in `target`, carries what peers check for `site`: the DNS name they dial
/// and the organization a gateway authorizes on.
fn served_as(cert: &str, target: &Target, site: &str) -> Result<(), Identity> {
    let secret = &target.site_secret;
    let dns = format!("{site}.{}", certs::SPIFFE_TRUST_DOMAIN);
    let named = certs::cert_dns_sans(cert)
        .unwrap_or_default()
        .iter()
        .any(|san| san.eq_ignore_ascii_case(&dns));
    if !named {
        return Err(Identity::Unservable(
            "CertificateDnsNameMissing",
            format!("tls.crt in Secret {secret} lacks the DNS name {dns} peers dial; issue it with that name"),
        ));
    }
    if certs::cert_organizations(cert).unwrap_or_default() != [site] {
        return Err(Identity::Unservable(
            "CertificateOrganizationMismatch",
            format!(
                "tls.crt in Secret {secret} must name organization {site} alone, which a gateway authorizes \
                 peers on; issue it with subject O={site}"
            ),
        ));
    }
    Ok(())
}

/// What `IdentityValid` says about a leaf in `target` that `error` refused as `site`'s.
fn untrusted(error: &certs::VerifyError, target: &Target, site: Option<&str>) -> Identity {
    let (secret, ca) = (&target.site_secret, &target.ca_secret);
    if matches!(
        error,
        certs::VerifyError::NameMismatch { .. } | certs::VerifyError::NotOneSpiffeName
    ) {
        let site = site.unwrap_or_default();
        Identity::NameMismatch(format!(
            "tls.crt in Secret {secret} is not site {site}'s ({error}); issue it for {}, or set site.name to the \
             site it names",
            certs::spiffe_id(site)
        ))
    } else {
        Identity::Untrusted(format!(
            "tls.crt in Secret {secret} is not trusted by the grid CA in Secret {ca} ({error}); replace it with a \
             certificate that CA issued, or re-enroll this site"
        ))
    }
}

/// The signer this round renews through, else why nothing renews, else why it could not
/// be resolved. A signer whose rotation is Enabled under a trust that does not renew is
/// held back, since a pinned peer would refuse the renewed leaf.
fn through<'config>(declared: &Declared<'config>, trust: &Trust) -> Result<Endpoint<'config>, Result<Off, RenewError>> {
    match resolve_signer(declared) {
        Ok(Resolved::Signer(signer)) if trust.renews => Ok(signer),
        Ok(Resolved::Signer(_)) => Err(Ok(Off::UnsupportedUnderPin)),
        Ok(Resolved::Off(why)) => Err(Ok(why)),
        Err(error) => Err(Err(error)),
    }
}

/// Read the identity in `target` and check it through the signer `declared` names.
async fn check_round(
    loop_: &Loop<'_>,
    target: &Target,
    site: Option<&str>,
    (declared, trust): (&Declared<'_>, &Trust),
) -> Round {
    let read = match Box::pin(read_identity(loop_, target)).await {
        Ok(read) => read,
        Err(round) => return round,
    };
    let identity = Some(identity_of(&read, target, site, trust.renews));
    match through(declared, trust) {
        Ok(signer) => Box::pin(renew_through(loop_, target, read, signer, identity)).await,
        Err(Ok(why)) => off_round(why, read, identity),
        Err(Err(error)) => Round {
            identity,
            ..Round::failed(error)
        },
    }
}

/// The round in which the operator does not renew `read`, for `why`.
fn off_round(why: Off, read: Read, identity: Option<Identity>) -> Round {
    let due = external_due(
        read.due,
        read.renew_at,
        read.held.renewer.is_some(),
        OffsetDateTime::now_utc(),
    );
    Round {
        checked: Ok(Checked::Off {
            why,
            due,
            renewer: read.held.renewer,
        }),
        signer: None,
        identity,
    }
}

/// How long past due an external renewer, a cert-manager Certificate the Secret names, has to
/// replace the identity before it reads overdue: cert-manager renews at the same point.
const EXTERNAL_GRACE: time::Duration = time::Duration::minutes(15);

/// When renewal is due for an identity renewed `externally` or not, at `now`: within
/// [`EXTERNAL_GRACE`] of `renew_at`, an external renewer is not yet overdue.
fn external_due(due: Due, renew_at: OffsetDateTime, externally: bool, now: OffsetDateTime) -> Due {
    let grace_ends = renew_at.saturating_add(EXTERNAL_GRACE);
    match due {
        Due::Now if externally && now < grace_ends => Due::At(grace_ends),
        held @ (Due::At(_) | Due::Now | Due::Expired) => held,
    }
}

/// The PEM `reference` pins the signer with. A bundle refused for its label is left out,
/// as `ResolvedRefs` reports, and the held grid CA alone still trusts the signer.
async fn signer_pin(client: &Client, reference: Option<&BundleRef>) -> Result<Option<String>, RenewError> {
    let Some(reference) = reference else {
        return Ok(None);
    };
    match Box::pin(local::Bundles::new(client, client.default_namespace()).pem(reference)).await {
        Ok(found) => Ok(found.map(|(pem, _version)| pem)),
        Err(local::BundleError::Api(why)) => Err(RenewError::Kube(why)),
        Err(local::BundleError::Refused(_)) => Ok(None),
    }
}

/// Renew the identity `read` from `target` through `signer` if it is due.
async fn renew_through(
    loop_: &Loop<'_>,
    target: &Target,
    read: Read,
    endpoint: Endpoint<'_>,
    identity: Option<Identity>,
) -> Round {
    let url = endpoint.base.renewal();
    let pin = match Box::pin(signer_pin(loop_.client, endpoint.pin)).await {
        Ok(pin) => pin,
        Err(error) => {
            return Round {
                checked: Err(error),
                signer: Some(url),
                identity,
            };
        },
    };
    let signer = HttpsSigner::new(&endpoint.base, pin.as_deref(), loop_.clients);
    Round {
        checked: Box::pin(check_held(
            loop_.store,
            &signer,
            target,
            read.held,
            (OffsetDateTime::now_utc(), loop_.stop),
        ))
        .await,
        signer: Some(url),
        identity,
    }
}

/// Report the round on `own`, this cluster's `GridNetwork`: `Accepted` as given, `ResolvedRefs`
/// by what `loaded` names, `Enrolled` by the identity in `target`, `IdentityValid` by what was
/// read and `RotationOnSchedule` by the check.
async fn report_round(
    client: &Client,
    (own, target): (&GridNetwork, &Target),
    round: &Round,
    (accepted, loaded): (Verdict, &Loaded),
) {
    let refs = match loaded.site() {
        Section::Site(section) => Box::pin(local::named_refs(client, Some(section))).await,
        Section::Unset => Some(Verdict::resolved()),
        Section::Invalid(_) => None,
    };
    let verdicts = round_verdicts(round, target, local::enrolled(own), (accepted, refs));
    drop(Box::pin(local::report_many(client, own, &verdicts)).await);
}

/// The conditions `round` writes, as (type, manager, verdict), beside the `accepted` and
/// `refs` verdicts given. A round that failed before reading the identity says nothing about
/// it. An identity present through a signer reports `Enrolled` only while the network does
/// not read enrolled, so the issuance message stays. One written outside a signer always
/// says what renews it.
fn round_verdicts(
    round: &Round,
    target: &Target,
    enrolled: bool,
    (accepted, refs): (Verdict, Option<Verdict>),
) -> Vec<(&'static str, &'static str, Verdict)> {
    let mut verdicts = Vec::with_capacity(5);
    verdicts.push((local::ACCEPTED, local::ACCEPTED_FIELD_MANAGER, accepted));
    if let Some(refs) = refs {
        verdicts.push((local::RESOLVED_REFS, local::REFS_FIELD_MANAGER, refs));
    }
    let Some(identity) = &round.identity else {
        return verdicts;
    };
    if let Some(held) = enrolled_verdict(round, identity, target, enrolled) {
        verdicts.push((local::ENROLLED, local::FIELD_MANAGER, held));
    }
    verdicts.push((
        local::IDENTITY_VALID,
        local::IDENTITY_FIELD_MANAGER,
        identity_verdict(identity),
    ));
    let rotation = rotation_verdict(&round.checked, round.signer.as_ref());
    verdicts.push((local::ROTATION_ON_SCHEDULE, local::RENEWAL_FIELD_MANAGER, rotation));
    verdicts
}

/// What `Enrolled` says of `identity` this round, `None` to leave an issuance message standing.
fn enrolled_verdict(round: &Round, identity: &Identity, target: &Target, enrolled: bool) -> Option<Verdict> {
    let outside = match &round.checked {
        Ok(Checked::Off {
            why: Off::NoSigner,
            renewer,
            ..
        }) => Some(renewer.as_deref()),
        _ => None,
    };
    match (identity, outside) {
        (Identity::Missing(_), _) => Some(Verdict::identity_lost(&target.site_secret)),
        (_, Some(renewer)) => Some(Verdict::provisioned(renewer)),
        (_, None) => (!enrolled).then(|| Verdict::present(&target.site_secret)),
    }
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
        Identity::Untrusted(why) => Verdict::new("False", "CertificateUntrusted", why.clone()),
        Identity::NameMismatch(why) => Verdict::new("False", "CertificateNameMismatch", why.clone()),
        Identity::KeyMismatch(why) => Verdict::new("False", "CertificateKeyMismatch", why.clone()),
        Identity::Unservable(reason, why) => Verdict::new("False", *reason, why.clone()),
        Identity::Valid(at, fingerprint) => Verdict::new(
            "True",
            local::IDENTITY_VALID,
            format!("{} is the expiry; fingerprint sha256:{fingerprint}", rfc3339(*at)),
        ),
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
        Err(error @ RenewError::Untrusted(_)) => Verdict::new("False", "SignerUntrusted", error.to_string()),
        Err(error @ RenewError::SignerLapsed(_)) => Verdict::new("False", "SignerLapsed", error.to_string()),
        Err(error @ RenewError::NeedsPassthrough(_)) => {
            Verdict::new("False", "RenewalNeedsPassthrough", error.to_string())
        },
        Err(error) => Verdict::new("False", "RenewalFailing", error.to_string()),
        Ok(Checked::Waiting(at)) => on_schedule(url, Some(*at)),
        Ok(Checked::Renewed(_)) => on_schedule(url, None),
        Ok(Checked::Expired(at)) => Verdict::new(
            "False",
            "RotationOverdue",
            format!("the site identity expired at {at} and cannot renew; re-enroll this site with a new site token"),
        ),
        Ok(Checked::Off { why, due, renewer }) => off_verdict(*why, *due, renewer.as_deref()),
    }
}

/// What `RotationOnSchedule` says of a check through the signer at `url` that is renewing
/// at `due`, or just renewed.
fn on_schedule(url: Option<&String>, due: Option<OffsetDateTime>) -> Verdict {
    let Some(url) = url else {
        return Verdict::new(
            "False",
            "RenewalFailing",
            "renewal went through no named signer".to_owned(),
        );
    };
    let message = match due {
        Some(at) => format!("renews at {} through {url}, from site.signerEndpoint", rfc3339(at)),
        None => format!("renewed through {url}, from site.signerEndpoint"),
    };
    Verdict::new("True", local::ROTATION_ON_SCHEDULE, message)
}

/// What `RotationOnSchedule` says while the operator does not renew for `why`, with renewal
/// `due` and `renewer` the cert-manager Certificate the Secret names, if any. A refused config
/// is not evaluated until renewal falls due, so it never reads as a choice.
fn off_verdict(why: Off, due: Due, renewer: Option<&str>) -> Verdict {
    let overdue = |message: String| Verdict::new("False", "RotationOverdue", message);
    match (why, due, renewer) {
        (Off::Refused, Due::At(_), _) => Verdict::new("Unknown", "NotEvaluated", why.message().to_owned()),
        (Off::Refused, Due::Now | Due::Expired, _) => overdue(
            "renewal is overdue and held back: the operator config is refused for this site, see Accepted".to_owned(),
        ),
        (Off::UnsupportedUnderPin, Due::At(_), _) => {
            Verdict::new("True", local::ROTATION_ON_SCHEDULE, why.message().to_owned())
        },
        (Off::NoSigner | Off::Disabled, Due::At(_), Some(certificate)) => Verdict::new(
            "True",
            local::ROTATION_ON_SCHEDULE,
            format!("renewed by cert-manager Certificate {certificate}"),
        ),
        (Off::NoSigner | Off::Disabled, Due::At(_), None) => Verdict::new(
            "True",
            local::ROTATION_ON_SCHEDULE,
            format!(
                "{}: nothing is known to renew this identity; it is kept until it expires",
                why.message()
            ),
        ),
        (Off::NoSigner | Off::Disabled, Due::Now | Due::Expired, Some(certificate)) => overdue(format!(
            "renewal is due and cert-manager Certificate {certificate} has not replaced the identity"
        )),
        (_, Due::Now | Due::Expired, _) => overdue(format!("renewal is due and nothing renews: {}", why.message())),
    }
}

/// Roll the gateway when a renewal replaced the identity it mounts.
async fn roll_after_renewal(
    store: &KubeIdentity,
    gateway: Option<&KubeGateway>,
    target: &Result<Target, RenewError>,
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

/// Sleep for `wait`, or until the declared peer trust or the operator config changes.
async fn pause(
    wait: Duration,
    trust_changes: &mut Option<tokio::sync::watch::Receiver<PeerTrustMode>>,
    config: &mut ConfigWatch,
) {
    let peer_trust = async {
        match trust_changes.as_mut() {
            Some(changes) => {
                // A closed channel never changes again, so only the timer is left.
                if changes.changed().await.is_err() {
                    *trust_changes = None;
                    std::future::pending::<()>().await;
                }
            },
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        () = tokio::time::sleep(wait) => {},
        () = peer_trust => {},
        () = config.changed() => {},
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
            renews: GridModes::of(network, install.membership).renews(),
            declared: format!(
                "GridNetwork {} declares peerTrust.mode {}",
                network.metadata.name.as_deref().unwrap_or_default(),
                mode(GridModes::of(network, install.membership))
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
/// catches up a missed roll, this loop's or the cert-manager Certificate's that renews the
/// identity instead. `Some(true)` after a renewal.
const fn rolls_after(checked: &Result<Checked, RenewError>) -> Option<bool> {
    match checked {
        Ok(Checked::Renewed(_)) => Some(true),
        Ok(
            Checked::Waiting(_)
            | Checked::Off {
                why: Off::NoSigner | Off::Disabled,
                renewer: Some(_),
                ..
            },
        ) => Some(false),
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
        // Checked again when renewal falls due, so an overdue identity is reported then.
        Ok(Checked::Off { why, due, .. }) => (
            format!("off {} {due:?}", why.label()),
            match due {
                Due::At(at) => until(*at, OffsetDateTime::now_utc()).min(CHECK_EVERY),
                Due::Now | Due::Expired => CHECK_EVERY,
            },
        ),
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
        Ok(Checked::Off { why, due, .. }) if changed => {
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

/// `at` in RFC 3339, as conditions say it.
fn rfc3339(at: OffsetDateTime) -> String {
    at.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
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
