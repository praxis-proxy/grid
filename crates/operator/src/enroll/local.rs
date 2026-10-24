//! Enroll this cluster's own `GridSite`, the one carrying `spec.local`, and report it as `Enrolled`.

use std::borrow::Cow;

use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::{
    Api, Client,
    api::{ListParams, Patch, PatchParams},
};
use serde_json::Value;

use super::{EnrollError, KubeStore, Outcome, Settings, Store as _, Target};
use crate::crd::{
    grid_site::{BundleKind, BundleRef, GridSite, InviteSecretRef, SignerEndpoint},
    inference_provider::Condition,
};

/// The enrollment condition's type: whether this site holds an identity it enrolled for.
pub const ENROLLED: &str = "Enrolled";

/// Whether `spec.local` on this `GridSite` is this cluster's own site.
pub const ACCEPTED: &str = "Accepted";

/// Whether every object `spec.local` names resolves.
pub const RESOLVED_REFS: &str = "ResolvedRefs";

/// Whether the identity held is usable.
pub const IDENTITY_VALID: &str = "IdentityValid";

/// Whether the identity renews before it expires.
pub const ROTATION_ON_SCHEDULE: &str = "RotationOnSchedule";

/// Whether an enrollment is in flight.
pub const PROGRESSING: &str = "Progressing";

/// The roll-up: `True` when every gate is, else the first failing gate's reason.
pub const AVAILABLE: &str = "Available";

/// `True` while the identity will not renew before it expires: the early warning.
pub const DEGRADED: &str = "Degraded";

/// The gates `Available` rolls up, in the order the first failing one is reported.
/// `RotationOnSchedule` does not gate: it raises `Degraded` instead.
const GATES: [&str; 4] = [ACCEPTED, RESOLVED_REFS, ENROLLED, IDENTITY_VALID];

/// Every condition type written here, each by its own manager.
const TYPES: [(&str, &str); 8] = [
    (DEGRADED, DEGRADED_FIELD_MANAGER),
    (ENROLLED, FIELD_MANAGER),
    (ACCEPTED, ACCEPTED_FIELD_MANAGER),
    (RESOLVED_REFS, REFS_FIELD_MANAGER),
    (IDENTITY_VALID, IDENTITY_FIELD_MANAGER),
    (ROTATION_ON_SCHEDULE, RENEWAL_FIELD_MANAGER),
    (PROGRESSING, PROGRESSING_FIELD_MANAGER),
    (AVAILABLE, AVAILABLE_FIELD_MANAGER),
];

/// Label a `Secret` or `ConfigMap` carries, set to `true`, to serve as a signer bundle.
///
/// An opt-in, so a `GridSite` cannot pin a bundle that happens to be in the namespace.
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

/// Manager for `IdentityValid`.
pub const IDENTITY_FIELD_MANAGER: &str = "grid-operator-identity";

/// Manager for `Progressing`.
pub const PROGRESSING_FIELD_MANAGER: &str = "grid-operator-progressing";

/// Manager for `Available`.
pub const AVAILABLE_FIELD_MANAGER: &str = "grid-operator-own-site";

/// Manager for `Degraded`.
pub const DEGRADED_FIELD_MANAGER: &str = "grid-operator-degraded";

/// Which `GridSite` is this cluster's own.
#[derive(Debug)]
pub enum Local<'sites> {
    /// No identity, and no `GridSite` carries `spec.local`.
    Absent,
    /// No identity, and exactly one does.
    One(&'sites GridSite),
    /// No identity, and several do, so none is trusted to name this site.
    Several(Vec<&'sites GridSite>),
    /// The identity names the site.
    Named {
        /// Its `GridSite`, when declared.
        site: Option<&'sites GridSite>,
        /// Other `GridSite`s carrying `spec.local`, which is ignored on them.
        strays: Vec<&'sites GridSite>,
    },
}

/// This cluster's own site among `sites`: the one `identity` names, else the one
/// carrying `spec.local`.
#[must_use]
pub fn local<'sites>(sites: &'sites [GridSite], identity: Option<&str>) -> Local<'sites> {
    let carrying = sites.iter().filter(|site| site.spec.local.is_some());
    if let Some(name) = identity {
        let named = |site: &&GridSite| site.metadata.name.as_deref() == Some(name);
        return Local::Named {
            site: sites.iter().find(named),
            strays: carrying.filter(|site| !named(site)).collect(),
        };
    }
    let mut declared: Vec<&GridSite> = carrying.collect();
    match declared.len() {
        0 => Local::Absent,
        1 => declared.pop().map_or(Local::Absent, Local::One),
        _ => Local::Several(declared),
    }
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

    /// Nothing is wrong.
    fn as_expected(message: &str) -> Self {
        Self::new("True", "AsExpected", message.to_owned())
    }

    /// `Accepted`: this `GridSite` is this cluster's own site, with nothing conflicting.
    pub(crate) fn own_site() -> Self {
        Self::as_expected("this cluster's own site")
    }

    /// No invite to enroll with: an admin must act.
    fn awaiting_invite() -> Self {
        Self::new(
            "False",
            "AwaitingInvite",
            "set local.inviteSecretRef to the Secret holding this site's invite, beside local.signerEndpoint"
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

    /// A store call failed and the next poll retries with the same inputs.
    fn retrying(error: &EnrollError) -> Self {
        Self::new("True", "Retrying", error.to_string())
    }

    /// The request is being sent.
    fn enrolling() -> Self {
        Self::new(
            "True",
            "Enrolling",
            "sending the enrollment request to the signer".to_owned(),
        )
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

    /// Several `GridSite`s carry `spec.local` before an identity.
    fn duplicate(count: usize) -> Self {
        Self::new(
            "False",
            "DuplicateLocalSite",
            format!("{count} GridSites set spec.local; keep it on this cluster's own site only"),
        )
    }

    /// `spec.local` on a `GridSite` the identity does not name.
    pub(crate) fn not_local(identity: &str) -> Self {
        Self::new(
            "False",
            "NotLocalSite",
            format!("this cluster's identity names site {identity}; spec.local here is ignored"),
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

/// Whether `error` is about an object `spec.local` names rather than the enrollment.
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

/// Write `verdict` as `Enrolled` on `site`.
async fn report_enrolled(client: &Client, site: &GridSite, verdict: &Verdict) -> Option<GridSite> {
    report(client, site, ENROLLED, FIELD_MANAGER, verdict).await
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

/// The roll-up `Available` says about the conditions `site` holds: `True` when every gate
/// present is, else the first failing gate's status, reason and message; `None` with no
/// gate to read.
pub(crate) fn available(site: &GridSite) -> Option<Verdict> {
    let gates: Vec<&Condition> = GATES.iter().filter_map(|type_| held(site, type_)).collect();
    if gates.is_empty() {
        return None;
    }
    Some(gates.iter().find(|gate| gate.status != "True").map_or_else(
        || Verdict::as_expected(""),
        |gate| {
            Verdict::new(
                if gate.status == "Unknown" { "Unknown" } else { "False" },
                gate.reason.clone(),
                gate.message.clone(),
            )
        },
    ))
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

/// What `Degraded` says given `rotation`, the `RotationOnSchedule` verdict: `True` with its
/// reason while the identity will not renew before it expires.
pub(crate) fn degraded(rotation: &Verdict) -> Verdict {
    if rotation.status == "False" {
        Verdict::new("True", rotation.reason.clone(), rotation.message.clone())
    } else {
        Verdict::new("False", "AsExpected", String::new())
    }
}

/// Write the `Available` roll-up for the conditions `site` holds.
pub(crate) async fn report_available(client: &Client, site: &GridSite) {
    if let Some(verdict) = available(site) {
        drop(Box::pin(report(client, site, AVAILABLE, AVAILABLE_FIELD_MANAGER, &verdict)).await);
    }
}

/// Report `spec.local` as ignored on every `GridSite` in `strays`, since `identity` names the site.
pub(crate) async fn report_strays(client: &Client, strays: &[&GridSite], identity: &str) {
    let verdict = Verdict::not_local(identity);
    for stray in strays {
        let written = Box::pin(report(client, stray, ACCEPTED, ACCEPTED_FIELD_MANAGER, &verdict)).await;
        Box::pin(report_available(client, written.as_ref().unwrap_or(stray))).await;
    }
}

/// The `GridSite`s among `sites` that hold a condition of ours without being this
/// cluster's own site, `own`, or carrying `spec.local`: what a stray or duplicate leaves
/// behind once its `spec.local` goes.
pub(crate) fn stale<'sites>(sites: &'sites [GridSite], own: Option<&str>) -> Vec<&'sites GridSite> {
    sites
        .iter()
        .filter(|site| site.spec.local.is_none() && site.metadata.name.as_deref() != own)
        .filter(|site| TYPES.iter().any(|(type_, _)| held(site, type_).is_some()))
        .collect()
}

/// Clear what a stray or duplicate left behind among `listed`, keeping the own site's.
async fn clear_stale(client: &Client, listed: &[GridSite], identity: Option<&str>) {
    let own = match local(listed, identity) {
        Local::One(site) | Local::Named { site: Some(site), .. } => site.metadata.name.as_deref(),
        Local::Absent | Local::Several(_) | Local::Named { site: None, .. } => None,
    };
    Box::pin(clear(client, &stale(listed, own))).await;
}

/// Remove the conditions our managers hold on every `GridSite` in `stale`. An apply
/// carrying no conditions gives up what its manager owned and nothing else.
pub(crate) async fn clear(client: &Client, stale: &[&GridSite]) {
    let api: Api<GridSite> = Api::all(client.clone());
    for site in stale {
        let Some(name) = site.metadata.name.as_deref() else {
            continue;
        };
        for (_, manager) in TYPES {
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

/// What one [`Enrollment::step`] found or did.
#[derive(Debug, PartialEq, Eq)]
pub enum Step {
    /// No `GridSite` is this cluster's own.
    NoLocalSite,
    /// Several carry `spec.local`, so nothing was sent.
    Duplicate,
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
    /// The `GridSite`'s UID.
    uid: Option<String>,
    /// The signer and invite the spec names, so an unrelated spec edit does not resend.
    spec: String,
    /// Resource versions of the invite, pin, and anchor objects, `None` when absent.
    versions: [Option<String>; 3],
}

/// Enrollment of the local site, across polls.
#[derive(Debug, Default)]
pub struct Enrollment {
    /// The inputs of the last attempt that ended without an identity.
    failed: Option<Inputs>,
    /// The last attempt hit a store failure the next poll retries, so `Progressing` stays.
    retrying: bool,
}

impl Enrollment {
    /// Enroll the local site if it has an invite and no identity, reporting the result on
    /// it. `identity` is the site the identity names, once there is one.
    ///
    /// # Errors
    ///
    /// Returns the error when `GridSite`s cannot be listed.
    pub async fn step(&mut self, client: &Client, identity: Option<&str>) -> Result<Step, kube::Error> {
        let sites: Api<GridSite> = Api::all(client.clone());
        let listed = Box::pin(sites.list(&ListParams::default())).await?.items;
        Box::pin(clear_stale(client, &listed, identity)).await;
        let site = match local(&listed, identity) {
            Local::Absent => return Ok(Step::NoLocalSite),
            Local::Several(several) => {
                let verdict = Verdict::duplicate(several.len());
                for site in several {
                    let written = Box::pin(report(client, site, ACCEPTED, ACCEPTED_FIELD_MANAGER, &verdict)).await;
                    Box::pin(report_available(client, written.as_ref().unwrap_or(site))).await;
                }
                return Ok(Step::Duplicate);
            },
            Local::One(site) => site,
            Local::Named { site, strays } => {
                // Named is built from an identity, so this is never taken.
                let Some(name) = identity else {
                    return Ok(Step::NoLocalSite);
                };
                Box::pin(report_strays(client, &strays, name)).await;
                match site {
                    Some(site) => site,
                    None => return Ok(Step::NoLocalSite),
                }
            },
        };
        Ok(Box::pin(self.settle(client, site)).await)
    }

    /// Accept `site` as this cluster's own, enroll it, and roll its conditions up.
    async fn settle(&mut self, client: &Client, site: &GridSite) -> Step {
        let accepted = Box::pin(report(
            client,
            site,
            ACCEPTED,
            ACCEPTED_FIELD_MANAGER,
            &Verdict::own_site(),
        ))
        .await;
        let site = accepted.as_ref().unwrap_or(site);
        let step = Box::pin(self.enroll(client, site)).await;
        // Settle against the object as enrollment left it, not as it stood before.
        let site = Box::pin(latest(client, site)).await;
        let settled = if std::mem::take(&mut self.retrying) {
            None
        } else {
            let settled = Verdict::new("False", "AsExpected", String::new());
            Box::pin(report(client, &site, PROGRESSING, PROGRESSING_FIELD_MANAGER, &settled)).await
        };
        Box::pin(report_available(client, settled.as_ref().unwrap_or(&site))).await;
        step
    }

    /// Enroll `site`, the local one.
    #[expect(clippy::large_stack_frames, reason = "one enrollment attempt, once per poll")]
    async fn enroll(&mut self, client: &Client, site: &GridSite) -> Step {
        let into = match Box::pin(Destination::resolve(client)).await {
            Ok(into) => into,
            Err(error) => return Box::pin(self.fail(client, site, None, &error)).await,
        };
        match Box::pin(into.store.exists(&into.target.site_secret)).await {
            Ok(true) => return Box::pin(self.present(client, site, &into.target)).await,
            Ok(false) => Box::pin(identity_not_evaluated(client, site, &into.target.site_secret)).await,
            Err(error) => return Box::pin(self.fail(client, site, None, &error)).await,
        }
        let local = site.spec.local.as_ref();
        let (Some(signer), Some(invite)) = (
            local.and_then(|local| local.signer_endpoint.as_ref()),
            local.and_then(|local| local.invite_secret_ref.as_ref()),
        ) else {
            drop(Box::pin(report_enrolled(client, site, &Verdict::awaiting_invite())).await);
            return Step::AwaitingInvite;
        };
        let (inputs, settings) = match Box::pin(prepare(client, site, signer, invite)).await {
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
        let mut verdicts = Vec::with_capacity(2);
        if held(own, ENROLLED).is_none() {
            verdicts.push((ENROLLED, FIELD_MANAGER, Verdict::awaiting_first_attempt()));
        }
        // A resend after a retried failure is still the same progress, so Retrying holds.
        if held(own, PROGRESSING).is_none_or(|progressing| progressing.status != "True") {
            verdicts.push((PROGRESSING, PROGRESSING_FIELD_MANAGER, Verdict::enrolling()));
        }
        let latest = Box::pin(report_many(client, own, &verdicts)).await;
        let site = &latest;
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
            self.retrying = true;
            let retrying = Verdict::retrying(error);
            drop(Box::pin(report(client, site, PROGRESSING, PROGRESSING_FIELD_MANAGER, &retrying)).await);
        }
        if matches!(error, EnrollError::Kube(_)) {
            // A store failure says nothing about the enrollment, so Enrolled keeps its last value.
        } else if about_refs(error) {
            drop(Box::pin(report(client, site, RESOLVED_REFS, REFS_FIELD_MANAGER, &verdict)).await);
        } else {
            let resolved = Verdict::as_expected("");
            let written = Box::pin(report(client, site, RESOLVED_REFS, REFS_FIELD_MANAGER, &resolved)).await;
            drop(Box::pin(report_enrolled(client, written.as_ref().unwrap_or(site), &verdict)).await);
        }
        if let Some(inputs) = inputs.filter(|_inputs| definitive(error)) {
            self.failed = Some(inputs);
        }
        Step::Failed
    }
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

/// The inputs an attempt for `site` spends, and the settings they make.
async fn prepare(
    client: &Client,
    site: &GridSite,
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
        uid: site.metadata.uid.clone(),
        spec: serde_json::to_string(&(signer, invite)).unwrap_or_default(),
        versions: [invite_version, pin.version(), anchors.version()],
    };
    let name = site.metadata.name.as_deref().unwrap_or_default();
    let settings = pin.pem("local.signerEndpoint.caBundleRef").and_then(|pin_pem| {
        let anchor_pem = anchors.optional_pem("local.signerEndpoint.gridCaBundleRef")?;
        Settings::new(name, &signer.url, invite, &pin_pem, anchor_pem.as_deref())
    });
    Ok((inputs, settings))
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

/// Bundle objects in the operator namespace.
pub(crate) struct Bundles {
    /// Secrets.
    secrets: Api<Secret>,
    /// `ConfigMap`s.
    config_maps: Api<ConfigMap>,
}

impl Bundles {
    /// Bundles in `namespace`.
    pub(crate) fn new(client: &Client, namespace: &str) -> Self {
        Self {
            secrets: Api::namespaced(client.clone(), namespace),
            config_maps: Api::namespaced(client.clone(), namespace),
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
        Ok(match reference.kind {
            BundleKind::Secret => Box::pin(self.secrets.get_opt(name))
                .await
                .map_err(read)?
                .map(|secret| {
                    labelled(&secret.metadata, reference)?;
                    let bytes = secret.data.and_then(|mut data| data.remove(key)).map(|bytes| bytes.0);
                    Ok::<_, BundleError>((bytes, secret.metadata.resource_version))
                })
                .transpose()?,
            BundleKind::ConfigMap => Box::pin(self.config_maps.get_opt(name))
                .await
                .map_err(read)?
                .map(|map| {
                    labelled(&map.metadata, reference)?;
                    let text = map.data.and_then(|mut data| data.remove(key)).map(String::into_bytes);
                    Ok::<_, BundleError>((text, map.metadata.resource_version))
                })
                .transpose()?,
        })
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
