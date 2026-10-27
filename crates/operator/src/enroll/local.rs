//! Enroll this cluster's own site, as the operator config names it, and report it on its `GridSite`.

use std::borrow::Cow;

use k8s_openapi::api::core::v1::{ObjectReference, Secret};
use kube::{
    Api, Client,
    api::{ListParams, Patch, PatchParams},
    runtime::events::{Event, EventType, Recorder, Reporter},
};
use serde_json::Value;

use super::{EnrollError, KubeStore, Outcome, Settings, Store as _, Target};
use crate::{
    crd::{grid_site::GridSite, inference_provider::Condition},
    operator_config::{
        BundleKind, BundleRef, ConfigWatch, InviteSecretRef, Section, SignerEndpoint, SiteConfig, Snapshot,
    },
    rollup,
};

/// The enrollment condition's type: whether this site holds an identity it enrolled for.
pub const ENROLLED: &str = "Enrolled";

/// Whether the operator config's site section is accepted for this cluster's own site.
pub const ACCEPTED: &str = "Accepted";

/// Whether every object the site section names resolves.
pub const RESOLVED_REFS: &str = "ResolvedRefs";

/// Whether the identity held is usable.
pub const IDENTITY_VALID: &str = "IdentityValid";

/// Whether the identity renews before it expires.
pub const ROTATION_ON_SCHEDULE: &str = "RotationOnSchedule";

/// The gates the own site's `Available` rolls up, in the order the first failing one is
/// reported. `RotationOnSchedule` does not gate: it raises `Degraded` instead.
const GATES: [&str; 4] = [ACCEPTED, RESOLVED_REFS, ENROLLED, IDENTITY_VALID];

/// Every own-site condition type written here, each by its own manager, beside the roll-up
/// under [`OWN_ROLLUP_FIELD_MANAGER`].
const TYPES: [(&str, &str); 5] = [
    (ENROLLED, FIELD_MANAGER),
    (ACCEPTED, ACCEPTED_FIELD_MANAGER),
    (RESOLVED_REFS, REFS_FIELD_MANAGER),
    (IDENTITY_VALID, IDENTITY_FIELD_MANAGER),
    (ROTATION_ON_SCHEDULE, RENEWAL_FIELD_MANAGER),
];

/// Label a `Secret` carries, set to `true`, to serve as a signer bundle.
///
/// An opt-in, so the config cannot pin a bundle that happens to be in the namespace.
pub const SIGNER_BUNDLE_LABEL: &str = "grid.praxis.fast/signer-bundle";

/// Server-side apply manager for `Enrolled`, apart from the `GridSite` reconcile.
pub const FIELD_MANAGER: &str = "grid-operator-enrollment";

/// Server-side apply manager for `RotationOnSchedule`: an apply replaces what its
/// manager holds, so each condition type has its own.
pub const RENEWAL_FIELD_MANAGER: &str = "grid-operator-renewal";

/// Manager for `Accepted`.
pub const ACCEPTED_FIELD_MANAGER: &str = "grid-operator-accepted";

/// Manager for `ResolvedRefs`.
pub const REFS_FIELD_MANAGER: &str = "grid-operator-refs";

/// Manager for the own site's `Available`, `Progressing` and `Degraded`. Apart from the peer
/// roll-up's, so a second writer of the own site's roll-up shows as a manager conflict.
pub const OWN_ROLLUP_FIELD_MANAGER: &str = "grid-operator-own-site";

/// Manager for `IdentityValid`.
pub const IDENTITY_FIELD_MANAGER: &str = "grid-operator-identity";


/// This cluster's own `GridSite` among `sites`: the one named `name`.
#[must_use]
pub fn own<'sites>(sites: &'sites [GridSite], name: Option<&str>) -> Option<&'sites GridSite> {
    let name = name?;
    sites.iter().find(|site| site.metadata.name.as_deref() == Some(name))
}

/// Whether `site` holds a condition written here, as only this cluster's own site does.
#[must_use]
pub fn is_own(site: &GridSite) -> bool {
    held(site, ACCEPTED).is_some()
}

/// What a condition says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Verdict {
    /// `True`, `False`, or `Unknown`.
    pub(crate) status: &'static str,
    /// CamelCase reason.
    pub(crate) reason: Cow<'static, str>,
    /// Detail, never holding token bytes.
    pub(crate) message: String,
}

impl Verdict {
    /// A verdict.
    pub(crate) fn new(status: &'static str, reason: impl Into<Cow<'static, str>>, message: String) -> Self {
        Self {
            status,
            reason: reason.into(),
            message,
        }
    }

    /// `Accepted`: this `GridSite` is this cluster's own site, with nothing conflicting.
    pub(crate) fn own_site() -> Self {
        Self::new("True", ACCEPTED, "this cluster's own site".to_owned())
    }

    /// `Accepted`: the operator config was refused as a whole, so no section of it is acted on.
    pub(crate) fn config_invalid(why: &str) -> Self {
        Self::new("False", "OperatorConfigInvalid", why.to_owned())
    }

    /// `Accepted`: the config names another site than the identity this cluster holds.
    pub(crate) fn name_mismatch(configured: &str, identity: &str) -> Self {
        Self::new(
            "False",
            "SiteNameMismatch",
            format!("site.name is {configured} but this cluster's identity names site {identity}"),
        )
    }

    /// No `GridSite` of the site's name yet, so nothing is enrolled or reported there.
    fn awaiting_grid_site(name: &str) -> Self {
        Self::new(
            "False",
            "AwaitingGridSite",
            format!("no GridSite named {name}: declare this site's GridSite, and it enrolls"),
        )
    }

    /// No invite to enroll with: an admin must act.
    fn awaiting_invite() -> Self {
        Self::new(
            "False",
            "AwaitingInvite",
            "set site.inviteSecretRef in the operator config to the Secret holding this site's invite, beside \
             site.signerEndpoint"
                .to_owned(),
        )
    }

    /// Not evaluated yet: the first attempt has not returned.
    fn awaiting_first_attempt() -> Self {
        Self::new(
            "Unknown",
            "AwaitingFirstAttempt",
            "the first enrollment attempt has not returned".to_owned(),
        )
    }

    /// `ResolvedRefs`: every object the site section names resolves.
    pub(crate) fn resolved() -> Self {
        Self::new("True", RESOLVED_REFS, String::new())
    }

    /// The signer issued `spiffe_id`.
    fn issued(spiffe_id: &str) -> Self {
        Self::new("True", "Issued", format!("enrolled as {spiffe_id}"))
    }

    /// The identity Secret already exists.
    pub(crate) fn present(secret: &str) -> Self {
        Self::new(
            "True",
            "IdentityPresent",
            format!("site identity Secret {secret} exists"),
        )
    }

    /// Enrollment ended without an identity.
    fn failed(error: &EnrollError) -> Self {
        Self::new("False", reason(error), error.to_string())
    }
}

/// The condition reason for `error`.
const fn reason(error: &EnrollError) -> &'static str {
    match error {
        EnrollError::Config(_) => "Misconfigured",
        EnrollError::SignerUntrusted(_) | EnrollError::SignerLapsed(_) => "SignerUntrusted",
        EnrollError::RefNotFound(_) => "RefNotFound",
        EnrollError::NameMismatch(_) => "NameMismatch",
        EnrollError::TokenRejected(_) => "InviteInvalid",
        EnrollError::NameTaken(_) => "NameTaken",
        EnrollError::NameDeleted(_) => "NameDeleted",
        EnrollError::Refused(_) => "Refused",
        EnrollError::MaybeSpent(_) => "InviteMaybeSpent",
        EnrollError::InvalidResponse(_) => "InvalidResponse",
        EnrollError::NotStored(_) => "IdentityNotStored",
        EnrollError::Kube(_) => "StoreUnavailable",
    }
}

/// Whether `error` is about an object the site section names rather than the enrollment.
const fn about_refs(error: &EnrollError) -> bool {
    matches!(error, EnrollError::RefNotFound(_))
}

/// Whether retrying `error` with the same inputs cannot succeed. A request that may
/// have spent the token is retried: a spent token then answers as rejected.
const fn definitive(error: &EnrollError) -> bool {
    match error {
        EnrollError::Config(_)
        | EnrollError::SignerUntrusted(_)
        | EnrollError::RefNotFound(_)
        | EnrollError::NameMismatch(_)
        | EnrollError::TokenRejected(_)
        | EnrollError::NameTaken(_)
        | EnrollError::NameDeleted(_)
        | EnrollError::Refused(_)
        | EnrollError::InvalidResponse(_) => true,
        EnrollError::SignerLapsed(_)
        | EnrollError::MaybeSpent(_)
        | EnrollError::NotStored(_)
        | EnrollError::Kube(_) => false,
    }
}

/// The live condition of `type_` on `site`.
fn held<'site>(site: &'site GridSite, type_: &str) -> Option<&'site Condition> {
    site.status
        .as_ref()
        .and_then(|live| live.conditions.iter().find(|held| held.type_ == type_))
}

/// The condition of `type_` for `verdict`, or `None` when the live status already says it.
pub(crate) fn condition(site: &GridSite, type_: &str, verdict: &Verdict, now_rfc3339: &str) -> Option<Condition> {
    let generation = site.metadata.generation;
    let held = held(site, type_);
    if held.is_some_and(|held| {
        held.status == verdict.status
            && held.reason == verdict.reason.as_ref()
            && held.message == verdict.message
            && held.observed_generation == generation
    }) {
        return None;
    }
    let last_transition_time = held
        .filter(|held| held.status == verdict.status)
        .map_or_else(|| now_rfc3339.to_owned(), |held| held.last_transition_time.clone());
    Some(Condition {
        type_: type_.to_owned(),
        status: verdict.status.to_owned(),
        reason: verdict.reason.clone().into_owned(),
        message: verdict.message.clone(),
        last_transition_time,
        observed_generation: generation,
    })
}

/// The apply patch carrying `condition` on the `GridSite` `name`.
fn patch(name: &str, condition: &Condition) -> Value {
    serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridSite",
        "metadata": { "name": name },
        "status": { "conditions": [condition] }
    })
}

/// Whether the live `Enrolled` condition is already `True`.
pub(crate) fn enrolled(site: &GridSite) -> bool {
    held(site, ENROLLED).is_some_and(|held| held.status == "True")
}

/// Write `verdict` as `Enrolled` on `site`.
async fn report_enrolled(client: &Client, site: &GridSite, verdict: &Verdict) -> Option<GridSite> {
    report(client, site, ENROLLED, FIELD_MANAGER, verdict).await
}

/// Write `verdict` as the condition `type_` on `site` under `manager`, returning the
/// written object so the next write compares against it. A failure is logged: the
/// condition reports, it never gates.
pub(crate) async fn report(
    client: &Client,
    site: &GridSite,
    type_: &str,
    manager: &str,
    verdict: &Verdict,
) -> Option<GridSite> {
    let name = site.metadata.name.as_deref()?;
    let now = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .ok()?;
    let condition = condition(site, type_, verdict, &now)?;
    let api: Api<GridSite> = Api::all(client.clone());
    let params = PatchParams::apply(manager).force();
    match Box::pin(api.patch_status(name, &params, &Patch::Apply(patch(name, &condition)))).await {
        Ok(written) => Some(written),
        Err(error) => {
            tracing::warn!(grid_site = name, condition = type_, %error, "writing the condition failed");
            None
        },
    }
}

/// Write each of `verdicts`, as (type, manager, verdict), on `site` in turn, returning
/// the object as last written so a later write compares against it.
pub(crate) async fn report_many(client: &Client, site: &GridSite, verdicts: &[(&str, &str, Verdict)]) -> GridSite {
    let mut latest = site.clone();
    for (type_, manager, verdict) in verdicts {
        if let Some(written) = Box::pin(report(client, &latest, type_, manager, verdict)).await {
            latest = written;
        }
    }
    latest
}

/// `Progressing` while an enrollment request is sent.
const ENROLLING: rollup::Cause<'static> = ("Enrolling", "sending the enrollment request to the signer");

/// The own site's roll-up over the conditions `site` holds: `Available` from the gates, a
/// gate not written yet reading `Unknown`, `Degraded` from a `RotationOnSchedule` that is
/// `False`, and `Progressing` from `in_flight`.
pub(crate) fn own_rollup(site: &GridSite, in_flight: Option<rollup::Cause<'_>>, now: &str) -> [Condition; 3] {
    let current = site
        .status
        .as_ref()
        .map_or(&[][..], |status| status.conditions.as_slice());
    let stamp = rollup::Stamp {
        generation: site.metadata.generation,
        now,
    };
    let gates: Vec<Condition> = GATES
        .iter()
        .map(|type_| {
            held(site, type_).cloned().unwrap_or_else(|| {
                let unwritten = ("Unknown", "AwaitingFirstCheck", "not evaluated yet".to_owned());
                rollup::condition(&[], type_, unwritten, stamp)
            })
        })
        .collect();
    let gates: Vec<&Condition> = gates.iter().collect();
    let degraded = held(site, ROTATION_ON_SCHEDULE)
        .filter(|rotation| rotation.status == "False")
        .map(|rotation| (rotation.reason.as_str(), rotation.message.as_str()));
    rollup::trio(current, &gates, (degraded, in_flight), stamp)
}

/// Write the own site's roll-up on `site` under [`OWN_ROLLUP_FIELD_MANAGER`], when it
/// changed. A failure is logged: the roll-up reports, it never gates.
pub(crate) async fn report_rollup(client: &Client, site: &GridSite, in_flight: Option<rollup::Cause<'_>>) {
    let Some(name) = site.metadata.name.as_deref() else {
        return;
    };
    let Ok(now) = time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339) else {
        return;
    };
    let trio = own_rollup(site, in_flight, &now);
    let current = site
        .status
        .as_ref()
        .map_or(&[][..], |status| status.conditions.as_slice());
    if !rollup::differs(current, &trio) {
        return;
    }
    let patch = serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridSite",
        "metadata": { "name": name },
        "status": { "conditions": trio }
    });
    let api: Api<GridSite> = Api::all(client.clone());
    let params = PatchParams::apply(OWN_ROLLUP_FIELD_MANAGER).force();
    if let Err(error) = Box::pin(api.patch_status(name, &params, &Patch::Apply(patch))).await {
        tracing::warn!(grid_site = name, %error, "writing the roll-up failed");
    }
}

/// Report `IdentityValid` as not evaluated yet on `site`, which has no identity: the
/// renewal loop reads it once there is one.
async fn identity_not_evaluated(client: &Client, site: &GridSite, secret: &str) {
    if held(site, IDENTITY_VALID).is_none() {
        let waiting = Verdict::new(
            "Unknown",
            "AwaitingFirstAttempt",
            format!("waiting for a site identity: Secret {secret} does not exist yet, see Enrolled"),
        );
        drop(Box::pin(report(client, site, IDENTITY_VALID, IDENTITY_FIELD_MANAGER, &waiting)).await);
    }
}

/// The `GridSites` among `sites` that hold a condition of ours without being this cluster's
/// own site, `own`: what a renamed site leaves behind.
pub(crate) fn stale<'sites>(sites: &'sites [GridSite], own: Option<&str>) -> Vec<&'sites GridSite> {
    sites
        .iter()
        .filter(|site| site.metadata.name.as_deref() != own)
        .filter(|site| TYPES.iter().any(|(type_, _)| held(site, type_).is_some()))
        .collect()
}

/// Remove the conditions our managers hold on every `GridSite` in `stale`. An apply
/// carrying no conditions gives up what its manager owned and nothing else.
pub(crate) async fn clear(client: &Client, stale: &[&GridSite]) {
    let api: Api<GridSite> = Api::all(client.clone());
    for site in stale {
        let Some(name) = site.metadata.name.as_deref() else {
            continue;
        };
        let managers = TYPES
            .iter()
            .map(|(_, manager)| *manager)
            .chain([OWN_ROLLUP_FIELD_MANAGER]);
        for manager in managers {
            let empty = serde_json::json!({
                "apiVersion": "grid.praxis.fast/v1alpha1",
                "kind": "GridSite",
                "metadata": { "name": name },
                "status": { "conditions": [] }
            });
            let params = PatchParams::apply(manager).force();
            if let Err(error) = Box::pin(api.patch_status(name, &params, &Patch::Apply(empty))).await {
                tracing::warn!(grid_site = name, manager, %error, "clearing a stale condition failed");
            }
        }
    }
}

/// Clear what a renamed site left behind among `listed`, keeping `own`'s.
async fn clear_stale(client: &Client, listed: &[GridSite], own: Option<&str>) {
    Box::pin(clear(client, &stale(listed, own))).await;
}

/// What one [`Enrollment::step`] found or did.
#[derive(Debug, PartialEq, Eq)]
pub enum Step {
    /// The operator config names no site, and no identity names one.
    NoLocalSite,
    /// The operator config is refused for this site, so nothing was sent.
    Refused,
    /// The identity exists.
    Present,
    /// No invite yet.
    AwaitingInvite,
    /// Enrolled now.
    Enrolled,
    /// The inputs that last failed are unchanged, so nothing was sent.
    Unchanged,
    /// Ended without an identity, as reported on the `GridSite`.
    Failed,
}

/// One enrollment request, and the inputs it spends.
struct Attempt {
    /// The validated request settings.
    settings: Settings,
    /// What the request was built from.
    inputs: Inputs,
}

/// Where an identity is stored.
struct Destination {
    /// Secrets in the operator namespace.
    store: KubeStore,
    /// The identity and CA Secrets.
    target: Target,
}

impl Destination {
    /// The Secrets in the operator namespace the `GridNetwork` names, else the defaults.
    async fn resolve(client: &Client) -> Result<Self, EnrollError> {
        let namespace = client.default_namespace();
        let defaults = Target::defaults();
        let target = Box::pin(super::resolve_target(client, namespace, &defaults, super::STEP_BACKOFF)).await?;
        Ok(Self {
            store: KubeStore(Api::namespaced(client.clone(), namespace)),
            target,
        })
    }
}

/// The inputs an attempt spent: a failed attempt is retried only once one changes.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Inputs {
    /// The site section the request was built from, so an unrelated edit does not resend.
    site: String,
    /// Resource versions of the invite, pin, and anchor objects, `None` when absent.
    versions: [Option<String>; 3],
}

/// Enrollment of the local site, across polls.
#[derive(Debug, Default)]
pub struct Enrollment {
    /// The inputs of the last attempt that ended without an identity.
    failed: Option<Inputs>,
    /// Why the last attempt failed in a way the next poll retries, so `Progressing` holds
    /// `Retrying`.
    retrying: Option<String>,
    /// The last Event recorded on the operator config, so a repeat is not recorded again.
    noted: Option<Verdict>,
}

impl Enrollment {
    /// Enroll the local site if it has an invite and no identity, reporting the result on its
    /// `GridSite`. Until that `GridSite` is declared, nothing is sent, and an Event on the
    /// operator config says why. `identity` is the site the identity names, once there is one.
    ///
    /// # Errors
    ///
    /// Returns the error when `GridSite`s cannot be listed.
    pub async fn step(
        &mut self,
        client: &Client,
        identity: Option<&str>,
        config: &Snapshot,
    ) -> Result<Step, kube::Error> {
        let sites: Api<GridSite> = Api::all(client.clone());
        let listed = Box::pin(sites.list(&ListParams::default())).await?.items;
        let section = config.loaded.site();
        let named = identity.or_else(|| section.name());
        Box::pin(clear_stale(client, &listed, named)).await;
        let own = own(&listed, named);
        let site = match section {
            Section::Unset => {
                return Ok(match (identity, own) {
                    (Some(_), Some(own)) => Box::pin(self.settle(client, own, None)).await,
                    _ => Step::NoLocalSite,
                });
            },
            Section::Invalid(why) => {
                return Ok(Box::pin(self.refuse(client, own, config, Verdict::config_invalid(why))).await);
            },
            Section::Site(site) => site,
        };
        if let Some(identity) = identity.filter(|identity| *identity != site.name) {
            let mismatch = Verdict::name_mismatch(&site.name, identity);
            return Ok(Box::pin(self.refuse(client, own, config, mismatch)).await);
        }
        Ok(match own {
            Some(own) => Box::pin(self.settle(client, own, Some(site))).await,
            None if identity.is_none() => Box::pin(self.await_grid_site(client, &site.name, config)).await,
            None => Step::NoLocalSite,
        })
    }

    /// Wait for the `GridSite` `name`, saying so as an Event on the operator config.
    async fn await_grid_site(&mut self, client: &Client, name: &str, config: &Snapshot) -> Step {
        let waiting = Verdict::awaiting_grid_site(name);
        tracing::info!(reason = %waiting.reason, "{}", waiting.message);
        Box::pin(self.note(client, config, waiting)).await;
        Step::NoLocalSite
    }

    /// Report `refusal` as `Accepted` on `own`, else as an Event on the operator config.
    async fn refuse(&mut self, client: &Client, own: Option<&GridSite>, config: &Snapshot, refusal: Verdict) -> Step {
        tracing::error!(reason = %refusal.reason, error = %refusal.message, "operator config refused for this site");
        match own {
            Some(own) => {
                let written = Box::pin(report(client, own, ACCEPTED, ACCEPTED_FIELD_MANAGER, &refusal)).await;
                Box::pin(report_rollup(client, written.as_ref().unwrap_or(own), None)).await;
            },
            None => Box::pin(self.note(client, config, refusal)).await,
        }
        Step::Refused
    }

    /// Record `verdict` as an Event on the operator config, unless it was the last one.
    async fn note(&mut self, client: &Client, config: &Snapshot, verdict: Verdict) {
        if self.noted.as_ref() == Some(&verdict) {
            return;
        }
        if let Some(object) = &config.object {
            Box::pin(record(client, object, &verdict)).await;
        }
        self.noted = Some(verdict);
    }

    /// Accept `site` as this cluster's own, enroll it through `config`, and roll its conditions up.
    async fn settle(&mut self, client: &Client, site: &GridSite, config: Option<&SiteConfig>) -> Step {
        let accepted = Box::pin(report(
            client,
            site,
            ACCEPTED,
            ACCEPTED_FIELD_MANAGER,
            &Verdict::own_site(),
        ))
        .await;
        let site = accepted.as_ref().unwrap_or(site);
        let step = Box::pin(self.enroll(client, site, config)).await;
        // Settle against the object as enrollment left it, not as it stood before.
        let site = Box::pin(latest(client, site)).await;
        let retrying = self.retrying.take();
        let in_flight = retrying.as_deref().map(|message| ("Retrying", message));
        Box::pin(report_rollup(client, &site, in_flight)).await;
        step
    }

    /// Enroll `site`, the local one, through the signer and invite `config` names.
    async fn enroll(&mut self, client: &Client, site: &GridSite, config: Option<&SiteConfig>) -> Step {
        let into = match Box::pin(Destination::resolve(client)).await {
            Ok(into) => into,
            Err(error) => return Box::pin(self.fail(client, site, None, &error)).await,
        };
        match Box::pin(into.store.exists(&into.target.site_secret)).await {
            Ok(true) => return Box::pin(self.present(client, site, &into.target)).await,
            Ok(false) => Box::pin(identity_not_evaluated(client, site, &into.target.site_secret)).await,
            Err(error) => return Box::pin(self.fail(client, site, None, &error)).await,
        }
        let (Some(config), Some(signer), Some(invite)) = (
            config,
            config.and_then(|config| config.signer_endpoint.as_ref()),
            config.and_then(|config| config.invite_secret_ref.as_ref()),
        ) else {
            return Box::pin(await_invite(client, site, config)).await;
        };
        let (inputs, settings) = match Box::pin(prepare(client, config, signer, invite)).await {
            Ok(prepared) => prepared,
            Err(error) => return Box::pin(self.fail(client, site, None, &error)).await,
        };
        if self.failed.as_ref() == Some(&inputs) {
            return Step::Unchanged;
        }
        match settings {
            Ok(settings) => Box::pin(self.redeem(client, site, &into, Attempt { settings, inputs })).await,
            Err(error) => Box::pin(self.fail(client, site, Some(inputs), &error)).await,
        }
    }

    /// Send the `attempt` and store the identity `into` its Secrets.
    async fn redeem(&mut self, client: &Client, own: &GridSite, into: &Destination, attempt: Attempt) -> Step {
        let Attempt { settings, inputs } = attempt;
        let target = &into.target;
        let mut verdicts = vec![(RESOLVED_REFS, REFS_FIELD_MANAGER, Verdict::resolved())];
        if held(own, ENROLLED).is_none() {
            verdicts.push((ENROLLED, FIELD_MANAGER, Verdict::awaiting_first_attempt()));
        }
        let latest = Box::pin(report_many(client, own, &verdicts)).await;
        let site = &latest;
        // A resend after a retried failure is still the same progress, so Retrying holds.
        if held(site, rollup::PROGRESSING).is_none_or(|progressing| progressing.status != "True") {
            Box::pin(report_rollup(client, site, Some(ENROLLING))).await;
        }
        let clients = super::signer::Clients::default();
        let signer = settings.signer(&clients);
        match Box::pin(super::enroll(&into.store, &signer, &settings, target)).await {
            Ok(Outcome::Enrolled(spiffe_id)) => {
                tracing::info!(%spiffe_id, secret = %target.site_secret, "site enrolled");
                drop(Box::pin(report_enrolled(client, site, &Verdict::issued(&spiffe_id))).await);
                self.failed = None;
                Step::Enrolled
            },
            Ok(Outcome::AlreadyEnrolled | Outcome::Discarded) => Box::pin(self.present(client, site, target)).await,
            // A Config from the attempt itself reads state the inputs do not track, such as
            // the CA Secret or the GridNetwork, so a fix there must resend.
            Err(error @ EnrollError::Config(_)) => Box::pin(self.fail(client, site, None, &error)).await,
            Err(error) => Box::pin(self.fail(client, site, Some(inputs), &error)).await,
        }
    }

    /// Report the identity in `target`, unless `site` already reads enrolled.
    async fn present(&mut self, client: &Client, site: &GridSite, target: &Target) -> Step {
        if !enrolled(site) {
            drop(Box::pin(report_enrolled(client, site, &Verdict::present(&target.site_secret))).await);
        }
        self.failed = None;
        Step::Present
    }

    /// Report `error` on `site`. A definitive one holds `inputs` until one changes; a
    /// transient one leaves the held inputs alone, so the next poll retries.
    async fn fail(&mut self, client: &Client, site: &GridSite, inputs: Option<Inputs>, error: &EnrollError) -> Step {
        tracing::error!(%error, "site enrollment failed");
        let verdict = Verdict::failed(error);
        // The next poll retries with the same inputs, so this is progress that holds across
        // polls, not a settled verdict; Enrolled still records what the attempt returned.
        if !definitive(error) {
            self.retrying = Some(error.to_string());
        }
        if matches!(error, EnrollError::Kube(_)) {
            // A store failure says nothing about the enrollment, so Enrolled keeps its last value.
        } else if about_refs(error) {
            drop(Box::pin(report(client, site, RESOLVED_REFS, REFS_FIELD_MANAGER, &verdict)).await);
        } else {
            let resolved = Verdict::resolved();
            let written = Box::pin(report(client, site, RESOLVED_REFS, REFS_FIELD_MANAGER, &resolved)).await;
            drop(Box::pin(report_enrolled(client, written.as_ref().unwrap_or(site), &verdict)).await);
        }
        if let Some(inputs) = inputs.filter(|_inputs| definitive(error)) {
            self.failed = Some(inputs);
        }
        Step::Failed
    }
}

/// Record `verdict` as an Event on the operator config `object`. A failure is logged.
async fn record(client: &Client, object: &ObjectReference, verdict: &Verdict) {
    let reporter = Reporter {
        controller: FIELD_MANAGER.into(),
        instance: None,
    };
    let event = Event {
        type_: if verdict.status == "False" {
            EventType::Warning
        } else {
            EventType::Normal
        },
        reason: verdict.reason.clone().into_owned(),
        note: Some(verdict.message.clone()),
        action: "Enroll".into(),
        secondary: None,
    };
    if let Err(error) = Box::pin(Recorder::new(client.clone(), reporter).publish(&event, object)).await {
        tracing::debug!(%error, "recording an enrollment Event failed");
    }
}

/// Resolve once this site's identity Secret is definitely gone and the invite the operator
/// config names for `site` is present and minted for it, so a restart would enroll it again.
pub async fn lost_identity_with_invite(client: &Client, site: &str, config: ConfigWatch) {
    loop {
        tokio::time::sleep(super::jittered(super::LOST_IDENTITY_CHECK)).await;
        if Box::pin(identity_lost(client, site, &config)).await {
            return;
        }
    }
}

/// One check for [`lost_identity_with_invite`]. A read that fails decides nothing.
async fn identity_lost(client: &Client, site: &str, config: &ConfigWatch) -> bool {
    let snapshot = config.current();
    let invite = match snapshot.loaded.site() {
        Section::Site(section) if section.name == site => section.invite_secret_ref.clone(),
        Section::Site(_) | Section::Unset | Section::Invalid(_) => None,
    };
    let Some(invite) = invite else { return false };
    let Ok(into) = Box::pin(Destination::resolve(client)).await else {
        return false;
    };
    let lost = Box::pin(super::identity_lost(
        &into.store,
        &into.target.site_secret,
        &invite.name,
        site,
    ))
    .await;
    if lost {
        tracing::warn!(
            secret = %into.target.site_secret,
            invite = %invite.name,
            "site identity is gone and an invite is present; standing down to enroll again"
        );
    }
    lost
}

/// `site` as the API holds it now, or `site` itself when the read fails.
async fn latest(client: &Client, site: &GridSite) -> GridSite {
    let Some(name) = site.metadata.name.as_deref() else {
        return site.clone();
    };
    match Box::pin(Api::<GridSite>::all(client.clone()).get(name)).await {
        Ok(current) => current,
        Err(error) => {
            tracing::debug!(grid_site = name, %error, "re-reading the site failed; settling on the last write");
            site.clone()
        },
    }
}

/// The inputs an attempt for the site `config` names spends, and the settings they make.
async fn prepare(
    client: &Client,
    config: &SiteConfig,
    signer: &SignerEndpoint,
    invite: &InviteSecretRef,
) -> Result<(Inputs, Result<Settings, EnrollError>), EnrollError> {
    let bundles = Bundles::new(client, client.default_namespace());
    let invite_version = Box::pin(bundles.secret_version(&invite.name))
        .await
        .map_err(|e| EnrollError::Kube(format!("reading Secret {}: {e}", invite.name)))?;
    let pin = Box::pin(bundles.read(signer.ca_bundle_ref.as_ref())).await;
    let anchors = Box::pin(bundles.read(signer.grid_ca_bundle_ref.as_ref())).await;
    let inputs = Inputs {
        site: serde_json::to_string(config).unwrap_or_default(),
        versions: [invite_version, pin.version(), anchors.version()],
    };
    let settings = pin.pem("site.signerEndpoint.caBundleRef").and_then(|pin_pem| {
        let anchor_pem = anchors.optional_pem("site.signerEndpoint.gridCaBundleRef")?;
        Settings::new(&config.name, &signer.url, invite, &pin_pem, anchor_pem.as_deref())
    });
    Ok((inputs, settings))
}

/// Report on `site` that `config` names no invite to send yet, with what the bundles it
/// does name resolve to.
async fn await_invite(client: &Client, site: &GridSite, config: Option<&SiteConfig>) -> Step {
    let refs = Box::pin(named_refs(client, config)).await;
    let written = Box::pin(report(client, site, RESOLVED_REFS, REFS_FIELD_MANAGER, &refs)).await;
    let site = written.as_ref().unwrap_or(site);
    drop(Box::pin(report_enrolled(client, site, &Verdict::awaiting_invite())).await);
    Step::AwaitingInvite
}

/// What `ResolvedRefs` says of the bundles `config` names, read without trusting them.
pub(crate) async fn named_refs(client: &Client, config: Option<&SiteConfig>) -> Verdict {
    let bundles = Bundles::new(client, client.default_namespace());
    let signer = config.and_then(|config| config.signer_endpoint.as_ref());
    for (field, reference) in [
        (
            "site.signerEndpoint.caBundleRef",
            signer.and_then(|signer| signer.ca_bundle_ref.as_ref()),
        ),
        (
            "site.signerEndpoint.gridCaBundleRef",
            signer.and_then(|signer| signer.grid_ca_bundle_ref.as_ref()),
        ),
    ] {
        if let Read::Missing(why) = Box::pin(bundles.read(reference)).await {
            return Verdict::new("False", "RefNotFound", format!("{field}: {why}"));
        }
    }
    Verdict::resolved()
}

/// A bundle as read: absent, unreadable, or its PEM and resource version.
#[derive(Debug)]
enum Read {
    /// No reference was set.
    Unset,
    /// The reference does not resolve, with why.
    Missing(String),
    /// The PEM, and the object's resource version.
    Found(String, Option<String>),
}

impl Read {
    /// The object's resource version, `None` unless found.
    fn version(&self) -> Option<String> {
        match self {
            Self::Found(_, version) => version.clone(),
            Self::Unset | Self::Missing(_) => None,
        }
    }

    /// The PEM a required reference named `field` holds.
    fn pem(&self, field: &str) -> Result<String, EnrollError> {
        match self {
            Self::Found(pem, _) => Ok(pem.clone()),
            Self::Missing(why) => Err(EnrollError::RefNotFound(format!("{field}: {why}"))),
            Self::Unset => Err(EnrollError::Config(format!("{field} is required to enroll"))),
        }
    }

    /// The PEM an optional reference named `field` holds, if set.
    fn optional_pem(&self, field: &str) -> Result<Option<String>, EnrollError> {
        match self {
            Self::Unset => Ok(None),
            Self::Found(..) | Self::Missing(_) => self.pem(field).map(Some),
        }
    }
}

/// Bundle Secrets in the operator namespace.
pub(crate) struct Bundles {
    /// Secrets.
    secrets: Api<Secret>,
}

impl Bundles {
    /// Bundles in `namespace`.
    pub(crate) fn new(client: &Client, namespace: &str) -> Self {
        Self {
            secrets: Api::namespaced(client.clone(), namespace),
        }
    }

    /// The resource version of Secret `name`, `None` when absent.
    async fn secret_version(&self, name: &str) -> Result<Option<String>, kube::Error> {
        Ok(Box::pin(self.secrets.get_metadata_opt(name))
            .await?
            .and_then(|meta| meta.metadata.resource_version))
    }

    /// The bundle `reference` names, if set.
    async fn read(&self, reference: Option<&BundleRef>) -> Read {
        let Some(reference) = reference else {
            return Read::Unset;
        };
        match self.pem(reference).await {
            Ok(Some((pem, version))) => Read::Found(pem, version),
            Ok(None) => Read::Missing(format!(
                "{:?} {} has no key {}",
                reference.kind, reference.name, reference.key
            )),
            Err(why) => Read::Missing(why.to_string()),
        }
    }

    /// The PEM and resource version `reference` names, `None` when the object or key is absent.
    ///
    /// # Errors
    ///
    /// [`BundleError::Api`] when the object cannot be read, [`BundleError::Refused`] when it
    /// does not opt in or the value is not UTF-8.
    pub(crate) async fn pem(&self, reference: &BundleRef) -> Result<Option<(String, Option<String>)>, BundleError> {
        let (name, key) = (&reference.name, &reference.key);
        let Some((bytes, version)) = Box::pin(self.raw(reference)).await? else {
            return Ok(None);
        };
        bytes
            .map(|bytes| {
                String::from_utf8(bytes)
                    .map_err(|_e| BundleError::Refused(format!("{:?} {name} key {key} is not UTF-8", reference.kind)))
            })
            .transpose()
            .map(|pem| pem.map(|pem| (pem, version)))
    }

    /// The bytes under the key `reference` names, and the object's resource version,
    /// `None` when the object is absent, refusing one that does not opt in.
    async fn raw(&self, reference: &BundleRef) -> Result<Option<(Option<Vec<u8>>, Option<String>)>, BundleError> {
        let (name, key) = (&reference.name, &reference.key);
        let read = |e: kube::Error| BundleError::Api(format!("reading {:?} {name}: {e}", reference.kind));
        let BundleKind::Secret = reference.kind;
        Box::pin(self.secrets.get_opt(name))
            .await
            .map_err(read)?
            .map(|secret| {
                labelled(&secret.metadata, reference)?;
                let bytes = secret.data.and_then(|mut data| data.remove(key)).map(|bytes| bytes.0);
                Ok::<_, BundleError>((bytes, secret.metadata.resource_version))
            })
            .transpose()
    }
}

/// Why a bundle object yielded no PEM: the API failed, or the object is not a signer bundle.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BundleError {
    /// The API call failed.
    #[error("{0}")]
    Api(String),
    /// The object exists but does not opt in, or its value is not UTF-8.
    #[error("{0}")]
    Refused(String),
}

/// Refuse a bundle object that does not opt in with [`SIGNER_BUNDLE_LABEL`].
fn labelled(meta: &kube::api::ObjectMeta, reference: &BundleRef) -> Result<(), BundleError> {
    let opted_in = meta
        .labels
        .as_ref()
        .and_then(|labels| labels.get(SIGNER_BUNDLE_LABEL))
        .is_some_and(|value| value == "true");
    if opted_in {
        Ok(())
    } else {
        Err(BundleError::Refused(format!(
            "{:?} {} lacks the label {SIGNER_BUNDLE_LABEL}=true a signer bundle carries",
            reference.kind, reference.name
        )))
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests;

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod conformance;
