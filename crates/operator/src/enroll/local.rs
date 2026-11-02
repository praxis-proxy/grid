//! Enroll this cluster's own site, as the operator config names it, and report it on its `GridNetwork`.

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
    crd::{
        grid_network::GridNetwork,
        inference_provider::{Condition, ConditionStatus},
    },
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

/// The gates the `GridNetwork`'s `Available` rolls up beside the controller's, in the order
/// the first failing one is reported. `RotationOnSchedule` does not gate: it raises
/// `Degraded` instead.
pub(crate) const GATES: [&str; 4] = [ACCEPTED, RESOLVED_REFS, ENROLLED, IDENTITY_VALID];

/// Every condition type written here, each by its own manager.
#[cfg(test)]
pub(crate) const TYPES: [(&str, &str); 5] = [
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

/// Server-side apply manager for `Enrolled`, apart from the `GridNetwork` reconcile.
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

/// `Enrolled`'s reason while a first request is out, which `Progressing` reads as `Enrolling`.
pub const ENROLLING: &str = "Enrolling";

/// `Progressing`'s reason while a failed enrollment is retried.
pub const RETRYING: &str = "Retrying";

/// The `Enrolled` reasons of a failure the next poll retries with the same inputs, which
/// `Progressing` reads as [`RETRYING`].
pub(crate) const RETRIED: [&str; 5] = [
    "SignerLapsed",
    "SignerUnreachable",
    "InviteMaybeSpent",
    "IdentityNotStored",
    "StoreUnavailable",
];

/// What `Progressing` reads off `held`'s `Enrolled`: `Enrolling` while a first request is
/// out, `Retrying` while a failure is retried, else nothing in flight.
#[must_use]
pub(crate) fn in_flight(held: &[Condition]) -> Option<rollup::Cause<'_>> {
    let enrolled = held.iter().find(|held| held.type_ == ENROLLED)?;
    let reason = enrolled.reason.as_str();
    if reason == ENROLLING {
        Some((ENROLLING, enrolled.message.as_str()))
    } else if RETRIED.contains(&reason) {
        Some((RETRYING, enrolled.message.as_str()))
    } else {
        None
    }
}

/// What a condition says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Verdict {
    /// The condition status.
    pub(crate) status: ConditionStatus,
    /// CamelCase reason.
    pub(crate) reason: Cow<'static, str>,
    /// Detail, never holding token bytes.
    pub(crate) message: String,
}

impl Verdict {
    /// A verdict.
    pub(crate) fn new(status: ConditionStatus, reason: impl Into<Cow<'static, str>>, message: String) -> Self {
        Self {
            status,
            reason: reason.into(),
            message,
        }
    }

    /// `Accepted`: the site section applies to this cluster's own site, with nothing conflicting.
    pub(crate) fn own_site() -> Self {
        Self::new(ConditionStatus::True, ACCEPTED, "this cluster's own site".to_owned())
    }

    /// `Accepted`: a `GridNetwork` naming no grid to join.
    pub(crate) fn grid_id_unset(network: &str) -> Self {
        Self::new(
            ConditionStatus::False,
            "GridIdUnset",
            format!(
                "GridNetwork {network} sets no spec.gridId: bootstrap must supply this grid's persisted ID, \
                 directly or through grid.id. The grid runtime waits until it is set"
            ),
        )
    }

    /// `Accepted`: no site section to act on, so enrollment is off.
    pub(crate) fn no_site(absent: bool) -> Self {
        let message = if absent {
            "no operator config, enrollment off"
        } else {
            "the operator config names no site, enrollment off"
        };
        Self::new(ConditionStatus::True, ACCEPTED, message.to_owned())
    }

    /// `Enrolled`: an identity written outside a signer, with what renews it.
    pub(crate) fn provisioned(renewer: Option<&str>) -> Self {
        let message = match renewer {
            Some(certificate) => {
                format!("identity provisioned outside the signer, renewed by cert-manager Certificate {certificate}")
            },
            None => "identity provisioned outside the signer; nothing is known to renew it".to_owned(),
        };
        Self::new(ConditionStatus::True, ENROLLED, message)
    }

    /// `Enrolled`: the identity this site held is gone.
    pub(crate) fn identity_lost(secret: &str) -> Self {
        Self::new(
            ConditionStatus::False,
            "IdentityMissing",
            format!("site identity Secret {secret} is gone: restore it, or name site.inviteSecretRef to enroll again"),
        )
    }

    /// `Enrolled`: no identity, and nothing to enroll with.
    fn enrollment_off(secret: &str) -> Self {
        Self::new(
            ConditionStatus::False,
            "EnrollmentOff",
            format!(
                "no site identity: Secret {secret} does not exist, and the operator config names no site to enroll"
            ),
        )
    }

    /// `Accepted`: the operator config was refused as a whole, so no section of it is acted on.
    pub(crate) fn config_invalid(why: &str) -> Self {
        Self::new(ConditionStatus::False, "OperatorConfigInvalid", why.to_owned())
    }

    /// `Accepted`: the config names another site than the identity this cluster holds.
    pub(crate) fn name_mismatch(configured: &str, identity: &str) -> Self {
        Self::new(
            ConditionStatus::False,
            "SiteNameMismatch",
            format!("site.name is {configured} but this cluster's identity names site {identity}"),
        )
    }

    /// No `GridNetwork` yet, so nothing is enrolled or reported there.
    fn awaiting_grid_network(name: &str) -> Self {
        Self::new(
            ConditionStatus::False,
            "AwaitingGridNetwork",
            format!("no GridNetwork yet: declare this cluster's GridNetwork, and site {name} enrolls"),
        )
    }

    /// More than one `GridNetwork`, so none is this cluster's.
    fn many_networks(count: usize) -> Self {
        Self::new(
            ConditionStatus::False,
            "MoreThanOneGridNetwork",
            format!("{count} GridNetworks found, but one operator serves one grid: delete all but one"),
        )
    }

    /// The first request is out.
    fn enrolling() -> Self {
        Self::new(
            ConditionStatus::Unknown,
            ENROLLING,
            "sending the enrollment request to the signer".to_owned(),
        )
    }

    /// No invite to enroll with: an admin must act.
    fn awaiting_invite() -> Self {
        Self::new(
            ConditionStatus::False,
            "AwaitingInvite",
            "set site.inviteSecretRef in the operator config to the Secret holding this site's invite, beside \
             site.signerEndpoint"
                .to_owned(),
        )
    }

    /// `ResolvedRefs`: every object the site section names resolves.
    pub(crate) fn resolved() -> Self {
        Self::new(ConditionStatus::True, RESOLVED_REFS, String::new())
    }

    /// The signer issued `spiffe_id`.
    fn issued(spiffe_id: &str) -> Self {
        Self::new(
            ConditionStatus::True,
            ENROLLED,
            format!("enrolled as {spiffe_id}, issued by the signer"),
        )
    }

    /// The identity Secret already exists.
    pub(crate) fn present(secret: &str) -> Self {
        Self::new(
            ConditionStatus::True,
            ENROLLED,
            format!("site identity Secret {secret} exists"),
        )
    }

    /// Enrollment ended without an identity.
    fn failed(error: &EnrollError) -> Self {
        Self::new(ConditionStatus::False, reason(error), error.to_string())
    }
}

/// The condition reason for `error`.
const fn reason(error: &EnrollError) -> &'static str {
    match error {
        EnrollError::Config(_) => "Misconfigured",
        EnrollError::SignerUntrusted(_) => "SignerUntrusted",
        EnrollError::SignerLapsed(_) => "SignerLapsed",
        EnrollError::RefNotFound(_) => "RefNotFound",
        EnrollError::NameMismatch(_) => "NameMismatch",
        EnrollError::TokenRejected(_) => "InviteInvalid",
        EnrollError::NameTaken(_) => "NameTaken",
        EnrollError::NameDeleted(_) => "NameDeleted",
        EnrollError::Refused(_) => "Refused",
        EnrollError::MaybeSpent { refused: false, .. } => "InviteMaybeSpent",
        // Never resent, so Progressing does not read it as retried.
        EnrollError::MaybeSpent { refused: true, .. } => "InviteRefusedAfterLostAnswer",
        EnrollError::InvalidResponse(_) => "InvalidResponse",
        EnrollError::NotStored(_) => "IdentityNotStored",
        EnrollError::PendingForeign(_) => "PendingSecretForeign",
        EnrollError::Unreachable(_) => "SignerUnreachable",
        EnrollError::Kube(_) => "StoreUnavailable",
    }
}

/// Whether `error` is about an object the site section names rather than the enrollment.
const fn about_refs(error: &EnrollError) -> bool {
    matches!(error, EnrollError::RefNotFound(_))
}

/// Whether retrying `error` with the same inputs cannot succeed. A request that may
/// have spent the token is retried, and a spent token then answers as rejected; that
/// refusal is definitive, so the token is not presented again until an input changes.
const fn definitive(error: &EnrollError) -> bool {
    match error {
        EnrollError::MaybeSpent { refused, .. } => *refused,
        EnrollError::Config(_)
        | EnrollError::SignerUntrusted(_)
        | EnrollError::RefNotFound(_)
        | EnrollError::NameMismatch(_)
        | EnrollError::TokenRejected(_)
        | EnrollError::NameTaken(_)
        | EnrollError::NameDeleted(_)
        | EnrollError::Refused(_)
        | EnrollError::InvalidResponse(_)
        | EnrollError::PendingForeign(_) => true,
        EnrollError::SignerLapsed(_)
        | EnrollError::NotStored(_)
        | EnrollError::Unreachable(_)
        | EnrollError::Kube(_) => false,
    }
}

/// The live condition of `type_` on `site`.
fn held<'site>(site: &'site GridNetwork, type_: &str) -> Option<&'site Condition> {
    site.status
        .as_ref()
        .and_then(|live| live.conditions.iter().find(|held| held.type_ == type_))
}

/// The condition of `type_` for `verdict`, or `None` when the live status already says it.
pub(crate) fn condition(site: &GridNetwork, type_: &str, verdict: &Verdict, now_rfc3339: &str) -> Option<Condition> {
    // Only a verdict that read the spec says which generation it saw: Enrolled and
    // IdentityValid read spec.tls.
    let generation = matches!(type_, ENROLLED | IDENTITY_VALID)
        .then_some(site.metadata.generation)
        .flatten();
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
        status: verdict.status,
        reason: verdict.reason.clone().into_owned(),
        message: verdict.message.clone(),
        last_transition_time,
        observed_generation: generation,
    })
}

/// The apply patch carrying `condition` on the `GridNetwork` `name`.
fn patch(name: &str, condition: &Condition) -> Value {
    serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": name },
        "status": { "conditions": [condition] }
    })
}

/// Whether the live `Enrolled` condition is already `True`.
pub(crate) fn enrolled(site: &GridNetwork) -> bool {
    held(site, ENROLLED).is_some_and(|held| held.status == ConditionStatus::True)
}

/// Write `verdict` as `Enrolled` on `site`.
async fn report_enrolled(client: &Client, site: &GridNetwork, verdict: &Verdict) -> Option<Box<GridNetwork>> {
    report(client, site, ENROLLED, FIELD_MANAGER, verdict).await
}

/// Write `verdict` as the condition `type_` on `site` under `manager`, returning the
/// written object so the next write compares against it. A failure is logged: the
/// condition reports, it never gates.
pub(crate) async fn report(
    client: &Client,
    site: &GridNetwork,
    type_: &str,
    manager: &str,
    verdict: &Verdict,
) -> Option<Box<GridNetwork>> {
    let name = site.metadata.name.as_deref()?;
    let now = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .ok()?;
    condition(site, type_, verdict, &now)?;
    let api: Api<GridNetwork> = Api::all(client.clone());
    // Judged again against the object as it stands, for the generation it carries now.
    let fresh = match Box::pin(api.get(name)).await {
        Ok(fresh) => fresh,
        Err(error) => {
            tracing::warn!(grid_network = name, condition = type_, %error, "reading the GridNetwork failed");
            return None;
        },
    };
    let condition = condition(&fresh, type_, verdict, &now)?;
    let params = PatchParams::apply(manager).force();
    match Box::pin(api.patch_status(name, &params, &Patch::Apply(patch(name, &condition)))).await {
        Ok(written) => Some(Box::new(written)),
        Err(error) => {
            tracing::warn!(grid_network = name, condition = type_, %error, "writing the condition failed");
            None
        },
    }
}

/// Write each of `verdicts`, as (type, manager, verdict), on `site` in turn, returning
/// the object as last written so a later write compares against it.
pub(crate) async fn report_many(
    client: &Client,
    site: &GridNetwork,
    verdicts: &[(&str, &str, Verdict)],
) -> Box<GridNetwork> {
    let mut latest = Box::new(site.clone());
    for (type_, manager, verdict) in verdicts {
        if let Some(written) = Box::pin(report(client, &latest, type_, manager, verdict)).await {
            latest = written;
        }
    }
    latest
}

/// Report `IdentityValid` as not evaluated yet on `site`, which has no identity: the
/// renewal loop reads it once there is one.
async fn identity_not_evaluated(client: &Client, site: &GridNetwork, secret: &str) {
    // Written whenever it differs, so a verdict on an identity since deleted never stands.
    let waiting = Verdict::new(
        ConditionStatus::Unknown,
        "AwaitingFirstAttempt",
        format!("waiting for a site identity: Secret {secret} does not exist yet, see Enrolled"),
    );
    drop(Box::pin(report(client, site, IDENTITY_VALID, IDENTITY_FIELD_MANAGER, &waiting)).await);
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
    /// Ended without an identity, as reported on the `GridNetwork`.
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
    /// Boxed: it rides in the step's frame.
    site: Box<SiteConfig>,
    /// Resource versions of the invite, pin, and anchor objects, `None` when absent.
    versions: [Option<String>; 3],
}

/// Enrollment of the local site, across polls.
#[derive(Debug, Default)]
pub struct Enrollment {
    /// The inputs of the last attempt that ended without an identity.
    failed: Option<Inputs>,
    /// The last Event recorded on the operator config, so a repeat is not recorded again.
    noted: Option<Verdict>,
    /// The process holds an identity, so a lost one is the renewal loop's to report.
    identified: bool,
}

impl Enrollment {
    /// Enroll the local site if it has an invite and no identity, reporting the result on the
    /// `GridNetwork`. Until one exists, nothing is sent, and an Event on the operator config
    /// says why. `identity` is the site the identity names, once there is one.
    ///
    /// # Errors
    ///
    /// Returns the error when `GridNetwork`s cannot be listed.
    pub async fn step(
        &mut self,
        client: &Client,
        identity: Option<&str>,
        config: &Snapshot,
    ) -> Result<Step, kube::Error> {
        let networks: Api<GridNetwork> = Api::all(client.clone());
        let listed = Box::pin(networks.list(&ListParams::default())).await?.items;
        let own = match listed.as_slice() {
            [] => None,
            [own] => Some(own),
            many => return Ok(Box::pin(self.many(client, config, many.len())).await),
        };
        let Some(own) = own else {
            return Ok(Box::pin(self.without_network(client, identity, config)).await);
        };
        self.identified = identity.is_some();
        // The one verdict the renewal loop writes too, so the two never disagree.
        let accepted = super::renew::accepted_over(&config.loaded, identity, &listed);
        Ok(match config.loaded.site() {
            Section::Invalid(_) => Box::pin(Self::refuse(client, own, &accepted)).await,
            Section::Site(site) if identity.is_some_and(|identity| identity != site.name) => {
                Box::pin(Self::refuse(client, own, &accepted)).await
            },
            Section::Site(site) => Box::pin(self.settle(client, own, Some(site), &accepted)).await,
            Section::Unset => Box::pin(self.settle(client, own, None, &accepted)).await,
        })
    }

    /// With no `GridNetwork` to report on, say on the operator config why nothing is sent.
    async fn without_network(&mut self, client: &Client, identity: Option<&str>, config: &Snapshot) -> Step {
        let refusal = match config.loaded.site() {
            Section::Invalid(why) => Verdict::config_invalid(why),
            Section::Site(site) => match identity {
                Some(identity) if identity != site.name => Verdict::name_mismatch(&site.name, identity),
                Some(_) => return Step::NoLocalSite,
                None => return Box::pin(self.await_grid_network(client, &site.name, config)).await,
            },
            Section::Unset => return Step::NoLocalSite,
        };
        tracing::error!(reason = %refusal.reason, error = %refusal.message, "operator config refused for this site");
        Box::pin(self.note(client, config, refusal)).await;
        Step::Refused
    }

    /// Enroll nowhere among `count` `GridNetwork`s, saying so as an Event on the operator config.
    async fn many(&mut self, client: &Client, config: &Snapshot, count: usize) -> Step {
        Box::pin(self.note(client, config, Verdict::many_networks(count))).await;
        Step::NoLocalSite
    }

    /// Wait for a `GridNetwork`, saying so as an Event on the operator config.
    async fn await_grid_network(&mut self, client: &Client, name: &str, config: &Snapshot) -> Step {
        let waiting = Verdict::awaiting_grid_network(name);
        tracing::info!(reason = %waiting.reason, "{}", waiting.message);
        Box::pin(self.note(client, config, waiting)).await;
        Step::NoLocalSite
    }

    /// Report `refusal` as `Accepted` on `own`; nothing is sent.
    async fn refuse(client: &Client, own: &GridNetwork, refusal: &Verdict) -> Step {
        tracing::error!(reason = %refusal.reason, error = %refusal.message, "operator config refused for this site");
        drop(Box::pin(report(client, own, ACCEPTED, ACCEPTED_FIELD_MANAGER, refusal)).await);
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

    /// Report `verdict` as `Accepted` on `site`, this cluster's `GridNetwork`, and enroll through
    /// `config`.
    async fn settle(
        &mut self,
        client: &Client,
        site: &GridNetwork,
        config: Option<&SiteConfig>,
        verdict: &Verdict,
    ) -> Step {
        let accepted = Box::pin(report(client, site, ACCEPTED, ACCEPTED_FIELD_MANAGER, verdict)).await;
        let site = accepted.as_deref().unwrap_or(site);
        Box::pin(self.enroll(client, site, config)).await
    }

    /// Enroll `site`, the local one, through the signer and invite `config` names.
    async fn enroll(&mut self, client: &Client, site: &GridNetwork, config: Option<&SiteConfig>) -> Step {
        let into = match Box::pin(Destination::resolve(client)).await {
            Ok(into) => into,
            Err(error) => return Box::pin(self.fail(client, site, None, &error)).await,
        };
        let outside = config.and_then(|config| config.signer_endpoint.as_ref()).is_none();
        match Box::pin(into.store.exists(&into.target.site_secret)).await {
            Ok(true) => return Box::pin(self.present(client, site, &into, outside)).await,
            // The renewal loop reports an identity this process held and lost.
            Ok(false) if self.identified => return Step::NoLocalSite,
            Ok(false) => Box::pin(identity_not_evaluated(client, site, &into.target.site_secret)).await,
            Err(error) => return Box::pin(self.fail(client, site, None, &error)).await,
        }
        let Some((config, signer, invite)) = config.and_then(|config| {
            Some((
                config,
                config.signer_endpoint.as_ref()?,
                config.invite_secret_ref.as_ref()?,
            ))
        }) else {
            return Box::pin(await_invite(client, site, config, &into.target.site_secret)).await;
        };
        Box::pin(self.attempt(client, site, &into, (config, signer, invite))).await
    }

    /// Prepare an attempt through `signer` with `invite`, as `config` names them, and send it
    /// unless the same inputs were refused already.
    async fn attempt(
        &mut self,
        client: &Client,
        site: &GridNetwork,
        into: &Destination,
        (config, signer, invite): (&SiteConfig, &SignerEndpoint, &InviteSecretRef),
    ) -> Step {
        let (inputs, settings) = match Box::pin(prepare(client, config, signer, invite)).await {
            Ok(prepared) => prepared,
            Err(error) => return Box::pin(self.fail(client, site, None, &error)).await,
        };
        if self.failed.as_ref() == Some(&inputs) {
            return Step::Unchanged;
        }
        match settings {
            Ok(settings) => Box::pin(self.redeem(client, site, into, Attempt { settings, inputs })).await,
            Err(error) => Box::pin(self.fail(client, site, Some(inputs), &error)).await,
        }
    }

    /// Send the `attempt` and store the identity `into` its Secrets.
    async fn redeem(&mut self, client: &Client, own: &GridNetwork, into: &Destination, attempt: Attempt) -> Step {
        let Attempt { settings, inputs } = attempt;
        let target = &into.target;
        let mut verdicts = vec![(RESOLVED_REFS, REFS_FIELD_MANAGER, Verdict::resolved())];
        // A resend after a retried failure is still the same progress, so Retrying holds.
        // A refusal after a lost answer holds too: a restart resends once, and says nothing new.
        let retried = held(own, ENROLLED).is_some_and(|held| {
            RETRIED.contains(&held.reason.as_str()) || held.reason == "InviteRefusedAfterLostAnswer"
        });
        if !retried {
            verdicts.push((ENROLLED, FIELD_MANAGER, Verdict::enrolling()));
        }
        let latest = Box::pin(report_many(client, own, &verdicts)).await;
        let site = &*latest;
        let clients = super::signer::Clients::default();
        let signer = settings.signer(&clients);
        match Box::pin(super::enroll(&into.store, &signer, &settings, target)).await {
            Ok(Outcome::Enrolled(spiffe_id)) => {
                tracing::info!(%spiffe_id, secret = %target.site_secret, "site enrolled");
                drop(Box::pin(report_enrolled(client, site, &Verdict::issued(&spiffe_id))).await);
                self.failed = None;
                Step::Enrolled
            },
            Ok(Outcome::AlreadyEnrolled | Outcome::Discarded) => {
                Box::pin(self.present(client, site, into, false)).await
            },
            // A Config from the attempt itself reads state the inputs do not track, such as
            // the CA Secret or the GridNetwork, so a fix there must resend.
            Err(error @ (EnrollError::Config(_) | EnrollError::PendingForeign(_))) => {
                Box::pin(self.fail(client, site, None, &error)).await
            },
            Err(error) => Box::pin(self.fail(client, site, Some(inputs), &error)).await,
        }
    }

    /// Report the identity in `target`, unless `site` already reads enrolled.
    async fn present(&mut self, client: &Client, site: &GridNetwork, into: &Destination, outside: bool) -> Step {
        let secret = &into.target.site_secret;
        // Whatever stopped an earlier process before it cleared the request, its key goes now.
        Box::pin(clear_kept(into)).await;
        // Written outside a signer, the identity says what renews it, as the renewal loop does.
        let verdict = if outside {
            let meta = Box::pin(into.store.metadata(secret)).await.ok().flatten();
            Some(Verdict::provisioned(
                meta.as_ref().and_then(super::renew::renewer_of).as_deref(),
            ))
        } else {
            (!enrolled(site)).then(|| Verdict::present(secret))
        };
        if let Some(verdict) = verdict {
            drop(Box::pin(report_enrolled(client, site, &verdict)).await);
        }
        self.failed = None;
        Step::Present
    }

    /// Report `error` on `site`. A definitive one holds `inputs` until one changes; a
    /// transient one leaves the held inputs alone, so the next poll retries.
    async fn fail(&mut self, client: &Client, site: &GridNetwork, inputs: Option<Inputs>, error: &EnrollError) -> Step {
        tracing::error!(%error, "site enrollment failed");
        let verdict = Verdict::failed(error);
        if matches!(error, EnrollError::Kube(_)) {
            // A store failure says nothing about the enrollment, so Enrolled keeps its last value.
        } else if about_refs(error) {
            drop(Box::pin(report(client, site, RESOLVED_REFS, REFS_FIELD_MANAGER, &verdict)).await);
        } else {
            let resolved = Verdict::resolved();
            let written = Box::pin(report(client, site, RESOLVED_REFS, REFS_FIELD_MANAGER, &resolved)).await;
            drop(Box::pin(report_enrolled(client, written.as_deref().unwrap_or(site), &verdict)).await);
        }
        if let Some(inputs) = inputs.filter(|_inputs| definitive(error)) {
            self.failed = Some(inputs);
        }
        Step::Failed
    }
}

/// Clear the enrollment kept beside the identity `into` holds, saying what was found.
async fn clear_kept(into: &Destination) {
    match Box::pin(super::pending::clear(&into.store, &into.target, super::STEP_BACKOFF)).await {
        Ok(super::pending::Swept::Foreign(why)) => tracing::warn!(%why, "pending enrollment Secret"),
        Ok(super::pending::Swept::Cleared) => {
            tracing::info!(secret = %into.target.site_secret, "cleared the enrollment kept beside the identity");
        },
        Ok(super::pending::Swept::Nothing) => {},
        Err(error) => {
            tracing::warn!(%error, "clearing the enrollment kept beside the identity failed: retried next poll");
        },
    }
}

/// Record `verdict` as an Event on the operator config `object`. A failure is logged.
async fn record(client: &Client, object: &ObjectReference, verdict: &Verdict) {
    let reporter = Reporter {
        controller: FIELD_MANAGER.into(),
        instance: None,
    };
    let event = Event {
        type_: if verdict.status == ConditionStatus::False {
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
    // An API failure is retried, and says nothing about whether the objects exist.
    for (field, read) in [
        ("site.signerEndpoint.caBundleRef", &pin),
        ("site.signerEndpoint.gridCaBundleRef", &anchors),
    ] {
        if let Read::Unavailable(why) = read {
            return Err(EnrollError::Kube(format!("{field}: {why}")));
        }
    }
    let inputs = Inputs {
        site: Box::new(config.clone()),
        versions: [invite_version, pin.version(), anchors.version()],
    };
    let settings = pin.pem("site.signerEndpoint.caBundleRef").and_then(|pin_pem| {
        let anchor_pem = anchors.optional_pem("site.signerEndpoint.gridCaBundleRef")?;
        Settings::new(&config.name, &signer.url, invite, &pin_pem, anchor_pem.as_deref())
            .map(|settings| settings.with_bundles(signer.ca_bundle_ref.as_ref(), signer.grid_ca_bundle_ref.as_ref()))
    });
    Ok((inputs, settings))
}

/// Report on `site` that `config` names no invite to send yet, with what the bundles it
/// does name resolve to.
async fn await_invite(client: &Client, site: &GridNetwork, config: Option<&SiteConfig>, secret: &str) -> Step {
    let written = match Box::pin(named_refs(client, config)).await {
        Some(refs) => Box::pin(report(client, site, RESOLVED_REFS, REFS_FIELD_MANAGER, &refs)).await,
        None => None,
    };
    let site = written.as_deref().unwrap_or(site);
    let waiting = match config {
        Some(_) => Verdict::awaiting_invite(),
        None => Verdict::enrollment_off(secret),
    };
    drop(Box::pin(report_enrolled(client, site, &waiting)).await);
    match config {
        Some(_) => Step::AwaitingInvite,
        None => Step::NoLocalSite,
    }
}

/// What `ResolvedRefs` says of the bundles `config` names, read without trusting them.
/// `None` when the API did not answer a read: that says nothing new, so the last verdict
/// stands and the next poll reads again.
pub(crate) async fn named_refs(client: &Client, config: Option<&SiteConfig>) -> Option<Verdict> {
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
        match Box::pin(bundles.read(reference)).await {
            Read::Missing(why) => {
                return Some(Verdict::new(
                    ConditionStatus::False,
                    "RefNotFound",
                    format!("{field}: {why}"),
                ));
            },
            Read::Unavailable(why) => {
                tracing::warn!(field, error = %why, "reading a bundle failed; ResolvedRefs keeps its last verdict");
                return None;
            },
            Read::Unset | Read::Found(..) => {},
        }
    }
    Some(Verdict::resolved())
}

/// A bundle as read: absent, unreadable, or its PEM and resource version.
#[derive(Debug)]
enum Read {
    /// No reference was set.
    Unset,
    /// The reference does not resolve, with why.
    Missing(String),
    /// The API did not answer the read, with why. Retried; the object may well exist.
    Unavailable(String),
    /// The PEM, and the object's resource version.
    Found(String, Option<String>),
}

impl Read {
    /// The object's resource version, `None` unless found.
    fn version(&self) -> Option<String> {
        match self {
            Self::Found(_, version) => version.clone(),
            Self::Unset | Self::Missing(_) | Self::Unavailable(_) => None,
        }
    }

    /// The PEM a required reference named `field` holds.
    fn pem(&self, field: &str) -> Result<String, EnrollError> {
        match self {
            Self::Found(pem, _) => Ok(pem.clone()),
            Self::Missing(why) => Err(EnrollError::RefNotFound(format!("{field}: {why}"))),
            Self::Unavailable(why) => Err(EnrollError::Kube(format!("{field}: {why}"))),
            Self::Unset => Err(EnrollError::Config(format!("{field} is required to enroll"))),
        }
    }

    /// The PEM an optional reference named `field` holds, if set.
    fn optional_pem(&self, field: &str) -> Result<Option<String>, EnrollError> {
        match self {
            Self::Unset => Ok(None),
            Self::Found(..) | Self::Missing(_) | Self::Unavailable(_) => self.pem(field).map(Some),
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
            Err(BundleError::Refused(why)) => Read::Missing(why),
            Err(BundleError::Api(why)) => Read::Unavailable(why),
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
