//! Kubernetes reconciliation for signer-side `SiteInvite` resources.

use std::{sync::Arc, time::Duration};

use futures::StreamExt as _;
use k8s_openapi::api::core::v1::Secret;
use kube::{
    Api, Client, Resource as _, ResourceExt as _,
    api::{DeleteParams, ObjectMeta, Patch, PatchParams, PostParams},
    runtime::{Controller, controller::Action, finalizer, watcher},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{AppState, NewSiteToken, auth::digest};

/// Finalizer that keeps revocation ahead of deletion.
const FINALIZER: &str = "grid.praxis.fast/siteinvite";
/// Secret key containing the one-time token.
const SECRET_KEY: &str = "token";
/// Secret annotation containing the public token ID.
const TOKEN_ID: &str = "grid.praxis.fast/token-id";
/// Secret annotation containing the token expiry.
const EXPIRES_AT: &str = "grid.praxis.fast/expires-at";
/// Retry interval for transient reconciliation errors and expiry checks.
const REQUEUE: Duration = Duration::from_secs(60);

/// Cluster-scoped invitation managed by the enrollment service.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, kube::CustomResource)]
#[kube(
    group = "grid.praxis.fast",
    version = "v1alpha1",
    kind = "SiteInvite",
    plural = "siteinvites",
    status = "SiteInviteStatus",
    namespaced = false
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SiteInviteSpec {
    /// Site name pinned by this invite.
    pub site_name: String,
    /// Jurisdictions assigned to the site.
    pub jurisdictions: Vec<String>,
    /// Optional SHA-256 digest of the site's public key.
    #[serde(rename = "publicKeySHA256", default, skip_serializing_if = "Option::is_none")]
    pub public_key_sha256: Option<String>,
    /// Token lifetime, such as `24h`.
    pub lifetime: String,
    /// Whether a previously deleted site name may enroll again.
    #[serde(default)]
    pub allow_deleted_name: bool,
}

/// Public lifecycle information for an invite.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SiteInviteStatus {
    /// Last generation reconciled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Public token identifier; never the token secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_id: Option<String>,
    /// Secret holding the one-time token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<InviteSecretReference>,
    /// Expiry of the token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    /// CSR name that redeemed this invite.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redeemed_by: Option<String>,
    /// Current readiness condition.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<SiteInviteCondition>,
}

/// A `SiteInvite` lifecycle condition.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SiteInviteCondition {
    /// Condition type.
    #[serde(rename = "type")]
    pub type_: String,
    /// Whether the invite is ready.
    pub status: String,
    /// Stable reason code.
    pub reason: String,
    /// Short human-readable state.
    pub message: String,
    /// Observed generation.
    pub observed_generation: i64,
    /// Last transition time.
    pub last_transition_time: String,
}

/// Namespace and name of the invite Secret.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InviteSecretReference {
    /// Secret namespace.
    pub namespace: String,
    /// Secret name.
    pub name: String,
    /// Key containing the token.
    pub key: String,
}

/// State shared by invite reconciliations.
#[derive(Clone)]
struct Context {
    /// API server client.
    client: Client,
    /// Enrollment store and CA state.
    state: Arc<AppState>,
    /// Namespace holding invitation Secrets.
    namespace: String,
}

/// Restart checkpoint recovered from the owned Secret.
struct TokenCheckpoint {
    /// Public token ID.
    token_id: Uuid,
    /// Token digest written to the enrollment store.
    token_sha256: String,
    /// Expiry parsed for the store and status.
    expires_at: OffsetDateTime,
    /// Original expiry text from the Secret annotation.
    expires_at_raw: String,
}

/// Invite lifecycle condition state.
#[derive(Clone, Copy)]
enum InviteState {
    /// Token remains usable.
    Ready,
    /// Token lifetime ended.
    Expired,
    /// Token has been redeemed.
    Redeemed,
    /// Token was removed before redemption.
    Revoked,
}

/// State of the token row relative to the invite.
enum TokenPhase {
    /// Token may be redeemed.
    Outstanding,
    /// Token was spent by this enrollment.
    Redeemed(Uuid),
    /// A previously recorded token row no longer exists.
    Revoked,
}

/// A reconciliation failure with its source.
#[derive(Debug, thiserror::Error)]
enum ReconcileError {
    /// Kubernetes API request failed.
    #[error("Kubernetes API: {0}")]
    Kubernetes(#[from] kube::Error),
    /// Enrollment store request failed.
    #[error("enrollment store: {0}")]
    Store(#[from] crate::StoreError),
    /// Invite data cannot be reconciled safely.
    #[error("invalid invite: {0}")]
    Invalid(String),
}

/// Run the `SiteInvite` controller until the process shuts down.
pub async fn run(client: Client, state: Arc<AppState>, namespace: String) {
    let invites = Api::<SiteInvite>::all(client.clone());
    Controller::new(invites, watcher::Config::default())
        .run(
            reconcile,
            error_policy,
            Arc::new(Context {
                client,
                state,
                namespace,
            }),
        )
        .for_each(|result| async move {
            if let Err(error) = result {
                tracing::error!(?error, "SiteInvite reconciliation failed");
            }
        })
        .await;
}

/// Reconcile one object through its finalizer.
async fn reconcile(invite: Arc<SiteInvite>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let api = Api::<SiteInvite>::all(ctx.client.clone());
    Box::pin(finalizer(&api, FINALIZER, invite, |event| async {
        match event {
            finalizer::Event::Apply(applied) => Box::pin(apply_invite(&applied, &ctx)).await,
            finalizer::Event::Cleanup(cleaning) => Box::pin(cleanup_invite(&cleaning, &ctx)).await,
        }
    }))
    .await
    .map_err(|error| ReconcileError::Invalid(error.to_string()))?;
    Ok(Action::requeue(REQUEUE))
}

/// Create or recover an invite token and publish its public status.
async fn apply_invite(invite: &SiteInvite, ctx: &Context) -> Result<Action, ReconcileError> {
    let namespace = &ctx.namespace;
    let secret_name = format!("grid-invite-{}", invite.spec.site_name);
    let secrets = Api::<Secret>::namespaced(ctx.client.clone(), namespace);
    if invite.status.as_ref().is_some_and(|status| {
        status.conditions.iter().any(|condition| {
            condition.type_ == "Ready"
                && ((condition.status == "False" && condition.reason == "Expired")
                    || (condition.status == "True" && condition.reason == "Redeemed")
                    || (condition.status == "False" && condition.reason == "Revoked"))
        })
    }) {
        return Ok(Action::await_change());
    }
    let checkpoint = Box::pin(read_invite_checkpoint(invite, &secrets, namespace, &secret_name)).await?;
    match Box::pin(token_phase(invite, ctx, checkpoint.token_id)).await? {
        TokenPhase::Outstanding => {},
        TokenPhase::Redeemed(enrollment_id) => {
            return Box::pin(redeem_invite(invite, ctx, &secret_name, checkpoint, enrollment_id)).await;
        },
        TokenPhase::Revoked => {
            return Box::pin(revoke_invite(invite, ctx, &secret_name, checkpoint)).await;
        },
    }
    if checkpoint.expires_at <= OffsetDateTime::now_utc() {
        return Box::pin(expire_invite(invite, ctx, &secrets, &secret_name, checkpoint)).await;
    }
    Box::pin(activate_invite(invite, ctx, namespace, &secret_name, checkpoint)).await
}

/// Recover the token checkpoint without holding a Kubernetes object across awaits.
async fn read_invite_checkpoint(
    invite: &SiteInvite,
    secrets: &Api<Secret>,
    namespace: &str,
    secret_name: &str,
) -> Result<TokenCheckpoint, ReconcileError> {
    let secret = Box::pin(get_or_create_secret(invite, secrets, namespace, secret_name)).await?;
    read_checkpoint(invite, &secret, namespace, secret_name)
}

/// Distinguish usable, spent, and externally revoked tokens.
async fn token_phase(invite: &SiteInvite, ctx: &Context, token_id: Uuid) -> Result<TokenPhase, ReconcileError> {
    match Box::pin(ctx.state.store.redeemed_enrollment(token_id)).await {
        Ok(Some(enrollment_id)) => Ok(TokenPhase::Redeemed(enrollment_id)),
        Ok(None) => Ok(TokenPhase::Outstanding),
        Err(crate::StoreError::NotFound)
            if invite
                .status
                .as_ref()
                .and_then(|status| status.token_id.as_ref())
                .is_none() =>
        {
            Ok(TokenPhase::Outstanding)
        },
        Err(crate::StoreError::NotFound) => Ok(TokenPhase::Revoked),
        Err(error) => Err(error.into()),
    }
}

/// Record an unexpired token and expose only its Secret reference.
async fn activate_invite(
    invite: &SiteInvite,
    ctx: &Context,
    namespace: &str,
    secret_name: &str,
    checkpoint: TokenCheckpoint,
) -> Result<Action, ReconcileError> {
    let uid = invite
        .uid()
        .ok_or_else(|| ReconcileError::Invalid("SiteInvite UID is missing".to_owned()))?;
    let status = invite_status(
        invite,
        &checkpoint,
        Some((namespace, secret_name)),
        InviteState::Ready,
        None,
    )?;
    Box::pin(ctx.state.store.mint_site_token(NewSiteToken {
        token_id: Some(checkpoint.token_id),
        token_sha256: checkpoint.token_sha256,
        public_key_sha256: invite.spec.public_key_sha256.clone(),
        site_name: invite.spec.site_name.clone(),
        grid_network_ref: "site-invite".to_owned(),
        issued_by: format!("siteinvite/{uid}"),
        expires_at: checkpoint.expires_at,
        allow_deleted_name: invite.spec.allow_deleted_name,
    }))
    .await?;
    Box::pin(api_patch_status(&ctx.client, invite, status)).await?;
    Ok(Action::requeue(REQUEUE))
}

/// Remove an expired token Secret and update its condition.
async fn expire_invite(
    invite: &SiteInvite,
    ctx: &Context,
    secrets: &Api<Secret>,
    secret_name: &str,
    checkpoint: TokenCheckpoint,
) -> Result<Action, ReconcileError> {
    match Box::pin(secrets.delete(secret_name, &DeleteParams::default())).await {
        Ok(_) => {},
        Err(kube::Error::Api(response)) if response.code == 404 => {},
        Err(error) => return Err(error.into()),
    }
    match Box::pin(ctx.state.store.revoke_site_token(checkpoint.token_id)).await {
        Ok(()) | Err(crate::StoreError::NotFound) => {},
        Err(error) => return Err(error.into()),
    }
    let status = invite_status(invite, &checkpoint, None, InviteState::Expired, None)?;
    Box::pin(api_patch_status(&ctx.client, invite, status)).await?;
    Ok(Action::await_change())
}

/// Remove a spent token Secret and record the CSR name.
async fn redeem_invite(
    invite: &SiteInvite,
    ctx: &Context,
    secret_name: &str,
    checkpoint: TokenCheckpoint,
    enrollment_id: Uuid,
) -> Result<Action, ReconcileError> {
    let secrets = Api::<Secret>::namespaced(ctx.client.clone(), &ctx.namespace);
    Box::pin(delete_secret(&secrets, secret_name)).await?;
    let request_name = crate::record::record_name(&invite.spec.site_name, enrollment_id);
    let status = invite_status(invite, &checkpoint, None, InviteState::Redeemed, Some(&request_name))?;
    Box::pin(api_patch_status(&ctx.client, invite, status)).await?;
    Ok(Action::await_change())
}

/// Remove a revoked token Secret and expose the revoked condition.
async fn revoke_invite(
    invite: &SiteInvite,
    ctx: &Context,
    secret_name: &str,
    checkpoint: TokenCheckpoint,
) -> Result<Action, ReconcileError> {
    let secrets = Api::<Secret>::namespaced(ctx.client.clone(), &ctx.namespace);
    Box::pin(delete_secret(&secrets, secret_name)).await?;
    let status = invite_status(invite, &checkpoint, None, InviteState::Revoked, None)?;
    Box::pin(api_patch_status(&ctx.client, invite, status)).await?;
    Ok(Action::await_change())
}

/// Delete a token Secret, treating an already absent object as success.
async fn delete_secret(secrets: &Api<Secret>, secret_name: &str) -> Result<(), ReconcileError> {
    match Box::pin(secrets.delete(secret_name, &DeleteParams::default())).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(response)) if response.code == 404 => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Build status with a Ready condition and optional Secret reference.
#[expect(
    clippy::too_many_lines,
    reason = "status fields and the matching condition must be patched together"
)]
fn invite_status(
    invite: &SiteInvite,
    checkpoint: &TokenCheckpoint,
    secret: Option<(&str, &str)>,
    state: InviteState,
    redeemed_by: Option<&str>,
) -> Result<serde_json::Value, ReconcileError> {
    let (condition_status, reason, message) = match state {
        InviteState::Ready => ("True", "Ready", "Invite token is ready"),
        InviteState::Expired => ("False", "Expired", "Invite token expired"),
        InviteState::Redeemed => ("True", "Redeemed", "Invite token was redeemed"),
        InviteState::Revoked => ("False", "Revoked", "Invite token was revoked"),
    };
    let prior_transition = invite.status.as_ref().and_then(|status| {
        status.conditions.iter().find(|condition| {
            condition.type_ == "Ready" && condition.status == condition_status && condition.reason == reason
        })
    });
    let last_transition_time = if let Some(condition) = prior_transition {
        condition.last_transition_time.clone()
    } else {
        OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|error| ReconcileError::Invalid(format!("formatting condition time: {error}")))?
    };
    let condition = serde_json::json!({
        "type": "Ready",
        "status": condition_status,
        "reason": reason,
        "message": message,
        "observedGeneration": invite.metadata.generation.unwrap_or_default(),
        "lastTransitionTime": last_transition_time
    });
    let mut status = serde_json::Map::from_iter([
        (
            "observedGeneration".to_owned(),
            serde_json::json!(invite.metadata.generation),
        ),
        ("tokenId".to_owned(), serde_json::json!(checkpoint.token_id)),
        ("expiresAt".to_owned(), serde_json::json!(checkpoint.expires_at_raw)),
        ("conditions".to_owned(), serde_json::json!([condition])),
    ]);
    if let Some((namespace, name)) = secret {
        status.insert(
            "secretRef".to_owned(),
            serde_json::json!({"namespace": namespace, "name": name, "key": SECRET_KEY}),
        );
    }
    if let Some(request_name) = redeemed_by {
        status.insert("redeemedBy".to_owned(), serde_json::json!(request_name));
    }
    Ok(serde_json::json!({"status": status}))
}

/// Validate Secret ownership and extract only the values reconciliation needs.
#[expect(
    clippy::too_many_lines,
    reason = "validate the owner and extract the restart checkpoint together"
)]
fn read_checkpoint(
    invite: &SiteInvite,
    secret: &Secret,
    namespace: &str,
    secret_name: &str,
) -> Result<TokenCheckpoint, ReconcileError> {
    let uid = invite
        .uid()
        .ok_or_else(|| ReconcileError::Invalid("SiteInvite UID is missing".to_owned()))?;
    if !secret.metadata.owner_references.as_ref().is_some_and(|owners| {
        owners
            .iter()
            .any(|owner| owner.kind == "SiteInvite" && owner.uid == uid)
    }) {
        return Err(ReconcileError::Invalid(format!(
            "Secret {namespace}/{secret_name} is not owned by this SiteInvite"
        )));
    }
    let token_sha256 = secret
        .data
        .as_ref()
        .and_then(|data| data.get(SECRET_KEY))
        .and_then(|bytes| std::str::from_utf8(&bytes.0).ok())
        .map(digest)
        .ok_or_else(|| ReconcileError::Invalid(format!("Secret {namespace}/{secret_name} has no UTF-8 token")))?;
    let annotations = secret.metadata.annotations.as_ref();
    let token_id = annotations
        .and_then(|values| values.get(TOKEN_ID))
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or_else(|| ReconcileError::Invalid(format!("Secret {namespace}/{secret_name} has no token id")))?;
    let expires_at_raw = annotations
        .and_then(|values| values.get(EXPIRES_AT))
        .cloned()
        .ok_or_else(|| ReconcileError::Invalid(format!("Secret {namespace}/{secret_name} has no expiry")))?;
    let expires_at = OffsetDateTime::parse(&expires_at_raw, &Rfc3339)
        .map_err(|error| ReconcileError::Invalid(format!("Secret expiry is invalid: {error}")))?;
    Ok(TokenCheckpoint {
        token_id,
        token_sha256,
        expires_at,
        expires_at_raw,
    })
}

/// Read the checkpoint Secret or create it before storing the token digest.
async fn get_or_create_secret(
    invite: &SiteInvite,
    secrets: &Api<Secret>,
    namespace: &str,
    secret_name: &str,
) -> Result<Secret, ReconcileError> {
    match Box::pin(secrets.get(secret_name)).await {
        Ok(secret) => Ok(secret),
        Err(kube::Error::Api(not_found)) if not_found.code == 404 => {
            if invite
                .status
                .as_ref()
                .and_then(|status| status.token_id.as_ref())
                .is_some()
            {
                return Err(ReconcileError::Invalid(format!(
                    "Secret {namespace}/{secret_name} is missing; delete and recreate this invite"
                )));
            }
            let secret = new_secret(invite, namespace, secret_name)?;
            match Box::pin(secrets.create(&PostParams::default(), &secret)).await {
                Ok(created) => Ok(created),
                Err(kube::Error::Api(already_exists)) if already_exists.code == 409 => {
                    Box::pin(secrets.get(secret_name)).await.map_err(Into::into)
                },
                Err(error) => Err(error.into()),
            }
        },
        Err(error) => Err(error.into()),
    }
}

/// Build the token Secret that checkpoints a mint across controller restarts.
fn new_secret(invite: &SiteInvite, namespace: &str, secret_name: &str) -> Result<Secret, ReconcileError> {
    let id = Uuid::new_v4();
    let token = crate::api::random_hex(32).map_err(|error| ReconcileError::Invalid(error.to_string()))?;
    let expires_at = expiry(&invite.spec.lifetime)?;
    let mut annotations = std::collections::BTreeMap::new();
    annotations.insert(TOKEN_ID.to_owned(), id.to_string());
    annotations.insert(
        EXPIRES_AT.to_owned(),
        expires_at
            .format(&Rfc3339)
            .map_err(|error| ReconcileError::Invalid(format!("formatting invite expiry: {error}")))?,
    );
    Ok(Secret {
        metadata: ObjectMeta {
            name: Some(secret_name.to_owned()),
            namespace: Some(namespace.to_owned()),
            annotations: Some(annotations),
            owner_references: Some(vec![
                invite
                    .controller_owner_ref(&())
                    .ok_or_else(|| ReconcileError::Invalid("SiteInvite UID is missing".to_owned()))?,
            ]),
            ..ObjectMeta::default()
        },
        type_: Some("Opaque".to_owned()),
        data: Some([(SECRET_KEY.to_owned(), k8s_openapi::ByteString(token.into_bytes()))].into()),
        ..Secret::default()
    })
}

/// Revoke an unused token and remove its Secret before deleting the invite.
async fn cleanup_invite(invite: &SiteInvite, ctx: &Context) -> Result<Action, ReconcileError> {
    let secrets = Api::<Secret>::namespaced(ctx.client.clone(), &ctx.namespace);
    if let Some(id) = invite
        .status
        .as_ref()
        .and_then(|status| status.token_id.as_deref())
        .and_then(|id| Uuid::parse_str(id).ok())
    {
        match ctx.state.store.revoke_site_token(id).await {
            Ok(()) | Err(crate::StoreError::NotFound) => {},
            Err(error) => return Err(error.into()),
        }
    }
    match secrets
        .delete(
            &format!("grid-invite-{}", invite.spec.site_name),
            &DeleteParams::default(),
        )
        .await
    {
        Ok(_) => {},
        Err(kube::Error::Api(response)) if response.code == 404 => {},
        Err(error) => return Err(error.into()),
    }
    Ok(Action::await_change())
}

/// Apply the controller-owned status fields.
async fn api_patch_status(client: &Client, invite: &SiteInvite, value: serde_json::Value) -> Result<(), kube::Error> {
    Api::<SiteInvite>::all(client.clone())
        .patch_status(
            &invite.name_any(),
            &PatchParams::apply("grid-enrollment"),
            &Patch::Apply(value),
        )
        .await
        .map(|_updated| ())
}

/// Retry failed reconciliations and log their cause.
fn error_policy(_invite: Arc<SiteInvite>, error: &ReconcileError, _ctx: Arc<Context>) -> Action {
    tracing::error!(%error, "SiteInvite reconciliation failed");
    Action::requeue(REQUEUE)
}

/// Add a Kubernetes duration to the current UTC time.
fn expiry(raw: &str) -> Result<OffsetDateTime, ReconcileError> {
    Ok(OffsetDateTime::now_utc().saturating_add(duration(raw)?))
}

/// Parse and bound a Kubernetes duration used for an invite.
#[expect(
    clippy::too_many_lines,
    reason = "all supported duration units share the same bounds check"
)]
fn duration(raw: &str) -> Result<time::Duration, ReconcileError> {
    let mut rest = raw;
    let mut total_nanos = 0_i64;
    while !rest.is_empty() {
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return Err(ReconcileError::Invalid(format!("invalid lifetime {raw:?}")));
        }
        let number = rest
            .get(..digits)
            .ok_or_else(|| ReconcileError::Invalid(format!("invalid lifetime {raw:?}")))?;
        let value = number
            .parse::<i64>()
            .map_err(|error| ReconcileError::Invalid(format!("invalid lifetime {raw:?}: {error}")))?;
        rest = rest
            .get(digits..)
            .ok_or_else(|| ReconcileError::Invalid(format!("invalid lifetime {raw:?}")))?;
        let (unit, scale) = [
            ("µs", 1_000),
            ("ms", 1_000_000),
            ("us", 1_000),
            ("ns", 1),
            ("h", 3_600_000_000_000),
            ("m", 60_000_000_000),
            ("s", 1_000_000_000),
        ]
        .into_iter()
        .find(|(unit, _scale)| rest.starts_with(unit))
        .ok_or_else(|| ReconcileError::Invalid(format!("invalid lifetime unit in {raw:?}")))?;
        total_nanos = total_nanos.saturating_add(value.saturating_mul(scale));
        rest = rest
            .get(unit.len()..)
            .ok_or_else(|| ReconcileError::Invalid(format!("invalid lifetime {raw:?}")))?;
    }
    if !(300_000_000_000..=604_800_000_000_000).contains(&total_nanos) {
        return Err(ReconcileError::Invalid(format!("lifetime {raw:?} must be 5m to 168h")));
    }
    Ok(time::Duration::nanoseconds(total_nanos))
}

#[cfg(test)]
mod tests {
    use super::{ReconcileError, duration};

    #[test]
    fn parses_kubernetes_durations_within_the_invite_bounds() {
        assert!(matches!(duration("5m"), Ok(value) if value.whole_seconds() == 300));
        assert!(matches!(duration("1h30m"), Ok(value) if value.whole_seconds() == 5_400));
        assert!(matches!(duration("5m500ms"), Ok(value) if value.whole_nanoseconds() == 300_500_000_000));
        assert!(matches!(duration("4m59s"), Err(ReconcileError::Invalid(_))));
        assert!(matches!(duration("169h"), Err(ReconcileError::Invalid(_))));
        assert!(matches!(duration("24hours"), Err(ReconcileError::Invalid(_))));
    }
}
