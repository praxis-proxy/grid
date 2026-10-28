//! Kubernetes reconciliation for signer-side `SiteInvite` resources.

use std::{sync::Arc, time::Duration};

use futures::StreamExt as _;
use k8s_openapi::api::core::v1::Secret;
use kube::{
    Api, Client, Resource as _, ResourceExt as _,
    api::{DeleteParams, ObjectMeta, Patch, PatchParams, PostParams, Preconditions},
    runtime::{Controller, controller::Action, finalizer, watcher},
};
use site_invite_api::{SiteInvite, SiteInviteStatus};
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
    token_sha256: Option<String>,
    /// Expiry parsed for the store and status.
    expires_at: OffsetDateTime,
    /// Original expiry text from the Secret annotation.
    expires_at_raw: String,
}

/// Invite lifecycle condition state.
#[derive(Clone, Copy)]
enum InviteState {
    /// Token checkpoint is durable before the token is minted.
    Pending,
    /// Token remains usable.
    Ready,
    /// Token lifetime ended.
    Expired,
    /// Token has been redeemed.
    Redeemed,
    /// Token was removed before redemption.
    Revoked,
    /// The signer refused to mint: `NameTaken`, `TokenOutstanding` or `NameDeleted`.
    Refused(&'static str, &'static str),
}

/// One condition as `(type, status, reason, message)`.
type Row = (&'static str, &'static str, &'static str, &'static str);

impl InviteState {
    /// The minted, redeemed and issued facts, with `Available` copying whether the token can
    /// still be presented.
    const fn facts(self) -> [Row; 4] {
        const SPENT: &str = "Invite token was redeemed";
        match self {
            Self::Pending => [
                ("Minted", "False", "Minting", "Invite token mint is pending"),
                ("Redeemed", "False", "AwaitingRequest", "Invite token mint is pending"),
                ("Issued", "False", "AwaitingRequest", "No enrollment request yet"),
                ("Available", "False", "Minting", "Invite token mint is pending"),
            ],
            Self::Refused(reason, message) => [
                ("Minted", "False", reason, message),
                ("Redeemed", "False", "AwaitingRequest", "Invite token was not minted"),
                ("Issued", "False", "AwaitingRequest", "No enrollment request yet"),
                ("Available", "False", reason, message),
            ],
            Self::Ready => [
                ("Minted", "True", "Minted", ""),
                ("Redeemed", "False", "AwaitingRequest", "Invite token is ready"),
                ("Issued", "False", "AwaitingRequest", "No enrollment request yet"),
                ("Available", "True", "Available", ""),
            ],
            Self::Expired => [
                ("Minted", "True", "Minted", ""),
                ("Redeemed", "False", "Expired", "Invite token expired"),
                ("Issued", "False", "Expired", "Invite token expired"),
                ("Available", "False", "Expired", "Invite token expired"),
            ],
            // Redemption issues the certificate in the same store transaction.
            Self::Redeemed => [
                ("Minted", "True", "Minted", ""),
                ("Redeemed", "True", "Redeemed", ""),
                ("Issued", "True", "Issued", ""),
                ("Available", "False", "Redeemed", SPENT),
            ],
            Self::Revoked => [
                ("Minted", "True", "Minted", ""),
                ("Redeemed", "False", "Revoked", "Invite token was revoked"),
                ("Issued", "False", "InviteRevoked", "Invite token was revoked"),
                ("Available", "False", "Revoked", "Invite token was revoked"),
            ],
        }
    }

    /// `Progressing`, `Degraded` and the `Ready` condition v1alpha1 readers hold, mirroring
    /// `Available` under its shipped reasons.
    const fn rest(self) -> [Row; 3] {
        let progressing = match self {
            Self::Pending => ("Progressing", "True", "Minting", "Invite token mint is pending"),
            Self::Refused(..) | Self::Ready | Self::Expired | Self::Redeemed | Self::Revoked => {
                ("Progressing", "False", "AsExpected", "")
            },
        };
        let ready = match self {
            Self::Pending => ("Ready", "False", "Pending", "Invite token mint is pending"),
            Self::Refused(_, message) => ("Ready", "False", "Pending", message),
            Self::Ready => ("Ready", "True", "Ready", "Invite token is ready"),
            Self::Expired => ("Ready", "False", "Expired", "Invite token expired"),
            Self::Redeemed => ("Ready", "False", "Redeemed", "Invite token was redeemed"),
            Self::Revoked => ("Ready", "False", "Revoked", "Invite token was revoked"),
        };
        [progressing, ("Degraded", "False", "AsExpected", ""), ready]
    }

    /// The state for a signer refusal at mint, `None` for any other failure.
    const fn refused(error: &crate::StoreError) -> Option<Self> {
        match error {
            crate::StoreError::NameTaken => Some(Self::Refused("NameTaken", "Another member already holds this name")),
            crate::StoreError::TokenOutstanding => Some(Self::Refused(
                "TokenOutstanding",
                "A live invite token already pins this site name",
            )),
            crate::StoreError::NameDeleted => Some(Self::Refused(
                "NameDeleted",
                "This name's enrollment was deleted; set allowDeletedName to reuse it",
            )),
            crate::StoreError::NotFound
            | crate::StoreError::KeyReused
            | crate::StoreError::KeyMismatch
            | crate::StoreError::TokenInvalid
            | crate::StoreError::Refused(_)
            | crate::StoreError::Backend(_) => None,
        }
    }
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

/// Wait for the operator-installed CRD before starting its watcher.
pub async fn run_when_available(client: Client, state: Arc<AppState>, namespace: String) {
    let gvk = kube::api::GroupVersionKind::gvk("grid.praxis.fast", "v1alpha1", "SiteInvite");
    loop {
        match kube::discovery::pinned_kind(&client, &gvk).await {
            Ok(_) => {
                tracing::info!("SiteInvite API is available; starting controller");
                run(client, state, namespace).await;
                return;
            },
            Err(error) => tracing::info!(%error, "SiteInvite API is not available; controller is waiting"),
        }
        tokio::time::sleep(REQUEUE).await;
    }
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
    let secret_name = format!("grid-invite-{}", invite.name_any());
    let secrets = Api::<Secret>::namespaced(ctx.client.clone(), namespace);
    if invite.status.as_ref().is_some_and(|status| {
        status.conditions.iter().any(|condition| {
            // Ready read True Redeemed before it mirrored Available, so any status counts.
            condition.type_ == "Ready" && ["Expired", "Redeemed", "Revoked"].contains(&condition.reason.as_str())
        })
    }) {
        return Ok(Action::await_change());
    }
    let checkpoint = Box::pin(read_invite_checkpoint(invite, &secrets, namespace, &secret_name)).await?;
    match Box::pin(token_phase(invite, ctx, checkpoint.token_id)).await? {
        TokenPhase::Outstanding => {
            if checkpoint.expires_at <= OffsetDateTime::now_utc() {
                return Box::pin(expire_invite(invite, ctx, &secrets, &secret_name, checkpoint)).await;
            }
            if checkpoint.token_sha256.is_none() {
                match Box::pin(ctx.state.store.revoke_site_token(checkpoint.token_id)).await {
                    Ok(()) | Err(crate::StoreError::NotFound) => {},
                    Err(error) => return Err(error.into()),
                }
                return Box::pin(revoke_invite(invite, ctx, &secret_name, checkpoint)).await;
            }
        },
        TokenPhase::Redeemed(enrollment_id) => {
            return Box::pin(redeem_invite(invite, ctx, &secret_name, checkpoint, enrollment_id)).await;
        },
        TokenPhase::Revoked => {
            if checkpoint.expires_at <= OffsetDateTime::now_utc() {
                return Box::pin(expire_invite(invite, ctx, &secrets, &secret_name, checkpoint)).await;
            }
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
    match Box::pin(secrets.get(secret_name)).await {
        Ok(secret) => read_checkpoint(invite, &secret, namespace, secret_name),
        Err(kube::Error::Api(response)) if response.code == 404 => {
            let Some(status) = invite.status.as_ref() else {
                let secret = Box::pin(get_or_create_secret(invite, secrets, namespace, secret_name)).await?;
                return read_checkpoint(invite, &secret, namespace, secret_name);
            };
            if status.token_id.is_none() || status.expires_at.is_none() {
                let secret = Box::pin(get_or_create_secret(invite, secrets, namespace, secret_name)).await?;
                return read_checkpoint(invite, &secret, namespace, secret_name);
            }
            checkpoint_from_status(status)
        },
        Err(error) => Err(error.into()),
    }
}

/// Rebuild terminal-transition data from status after the checkpoint Secret was deleted.
fn checkpoint_from_status(status: &SiteInviteStatus) -> Result<TokenCheckpoint, ReconcileError> {
    let token_id = status
        .token_id
        .as_deref()
        .ok_or_else(|| ReconcileError::Invalid("status token id is missing".to_owned()))?;
    let expires_at_raw = status
        .expires_at
        .as_deref()
        .ok_or_else(|| ReconcileError::Invalid("status expiry is missing".to_owned()))?;
    let token_id = Uuid::parse_str(token_id)
        .map_err(|error| ReconcileError::Invalid(format!("status token id is invalid: {error}")))?;
    let expires_at = OffsetDateTime::parse(expires_at_raw, &Rfc3339)
        .map_err(|error| ReconcileError::Invalid(format!("status expiry is invalid: {error}")))?;
    Ok(TokenCheckpoint {
        token_id,
        token_sha256: None,
        expires_at,
        expires_at_raw: expires_at_raw.to_owned(),
    })
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
                .is_none()
                || invite.status.as_ref().is_some_and(|status| {
                    status.conditions.iter().any(|condition| {
                        condition.type_ == "Ready" && condition.status == "False" && condition.reason == "Pending"
                    })
                }) =>
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
    let pending = invite_status(
        invite,
        &checkpoint,
        Some((namespace, secret_name)),
        InviteState::Pending,
        None,
    )?;
    // A refused mint holds Pending and a live token holds Ready, so neither rewrites Minting.
    if !holds_pending_or_ready(invite) {
        Box::pin(api_patch_status(&ctx.client, invite, pending)).await?;
    }
    let minted = Box::pin(ctx.state.store.mint_site_token(NewSiteToken {
        token_id: Some(checkpoint.token_id),
        token_sha256: checkpoint.token_sha256.clone().ok_or_else(|| {
            ReconcileError::Invalid(
                "invite Secret is missing for an outstanding token; delete and recreate this invite".to_owned(),
            )
        })?,
        public_key_sha256: invite.spec.public_key_sha256.clone(),
        site_name: invite.spec.site_name.clone(),
        grid_network_ref: "site-invite".to_owned(),
        issued_by: format!("siteinvite/{uid}"),
        expires_at: checkpoint.expires_at,
        allow_deleted_name: invite.spec.allow_deleted_name,
    }))
    .await;
    if let Err(error) = minted {
        if let Some(refused) = InviteState::refused(&error) {
            let refusal = invite_status(invite, &checkpoint, Some((namespace, secret_name)), refused, None)?;
            Box::pin(api_patch_status(&ctx.client, invite, refusal)).await?;
        }
        return Err(error.into());
    }
    Box::pin(api_patch_status(&ctx.client, invite, status)).await?;
    Ok(Action::requeue(REQUEUE))
}

/// Whether the invite already reads `Ready` `False` `Pending`, checkpointed and not yet
/// minted, or `Ready` `True`, minted.
fn holds_pending_or_ready(invite: &SiteInvite) -> bool {
    invite.status.as_ref().is_some_and(|status| {
        status.conditions.iter().any(|condition| {
            condition.type_ == "Ready"
                && ((condition.status == "False" && condition.reason == "Pending") || condition.status == "True")
        })
    })
}

/// Remove an expired token Secret and update its condition.
async fn expire_invite(
    invite: &SiteInvite,
    ctx: &Context,
    secrets: &Api<Secret>,
    secret_name: &str,
    checkpoint: TokenCheckpoint,
) -> Result<Action, ReconcileError> {
    Box::pin(delete_secret(secrets, secret_name)).await?;
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

/// Build status with its conditions and optional Secret reference.
fn invite_status(
    invite: &SiteInvite,
    checkpoint: &TokenCheckpoint,
    secret: Option<(&str, &str)>,
    state: InviteState,
    redeemed_by: Option<&str>,
) -> Result<serde_json::Value, ReconcileError> {
    let conditions = invite_conditions(invite, state)?;
    let mut status = serde_json::Map::from_iter([
        (
            "observedGeneration".to_owned(),
            serde_json::json!(invite.metadata.generation.unwrap_or_default()),
        ),
        ("tokenId".to_owned(), serde_json::json!(checkpoint.token_id)),
        ("expiresAt".to_owned(), serde_json::json!(checkpoint.expires_at_raw)),
        (
            "publicKeySHA256".to_owned(),
            serde_json::json!(invite.spec.public_key_sha256),
        ),
        ("conditions".to_owned(), serde_json::json!(conditions)),
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

/// Every condition `state` calls for, each keeping its `lastTransitionTime` while its status
/// is unchanged.
fn invite_conditions(invite: &SiteInvite, state: InviteState) -> Result<Vec<serde_json::Value>, ReconcileError> {
    let now = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|error| ReconcileError::Invalid(format!("formatting condition time: {error}")))?;
    let held = invite
        .status
        .as_ref()
        .map_or(&[][..], |status| status.conditions.as_slice());
    let generation = invite.metadata.generation.unwrap_or_default();
    let rows = state.facts().into_iter().chain(state.rest());
    Ok(rows
        .map(|(type_, status, reason, message)| {
            let last_transition_time = held
                .iter()
                .find(|condition| condition.type_ == type_ && condition.status == status)
                .map_or_else(|| now.clone(), |condition| condition.last_transition_time.clone());
            serde_json::json!({
                "type": type_,
                "status": status,
                "reason": reason,
                "message": message,
                "observedGeneration": generation,
                "lastTransitionTime": last_transition_time
            })
        })
        .collect())
}

/// Validate Secret ownership and extract only the values reconciliation needs.
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
        token_sha256: Some(token_sha256),
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
    let secret_name = format!("grid-invite-{}", invite.name_any());
    let secret = Box::pin(owned_secret(invite, &secrets, &ctx.namespace, &secret_name)).await?;
    let id = secret
        .as_ref()
        .and_then(|secret| secret.metadata.annotations.as_ref())
        .and_then(|annotations| annotations.get(TOKEN_ID))
        .and_then(|value| Uuid::parse_str(value).ok())
        .or_else(|| {
            invite
                .status
                .as_ref()
                .and_then(|status| status.token_id.as_deref())
                .and_then(|value| Uuid::parse_str(value).ok())
        });
    if let Some(id) = id {
        match ctx.state.store.revoke_site_token(id).await {
            Ok(()) | Err(crate::StoreError::NotFound) => {},
            Err(error) => return Err(error.into()),
        }
    }
    Box::pin(delete_owned_secret(secret, &secrets, &ctx.namespace, &secret_name)).await?;
    Ok(Action::await_change())
}

/// Delete only the Secret previously verified as owned by this invite.
async fn delete_owned_secret(
    secret: Option<Secret>,
    secrets: &Api<Secret>,
    namespace: &str,
    secret_name: &str,
) -> Result<(), ReconcileError> {
    let Some(secret) = secret else { return Ok(()) };
    let uid = secret
        .metadata
        .uid
        .ok_or_else(|| ReconcileError::Invalid(format!("Secret {namespace}/{secret_name} has no UID")))?;
    let params = DeleteParams {
        preconditions: Some(Preconditions {
            uid: Some(uid),
            ..Preconditions::default()
        }),
        ..DeleteParams::default()
    };
    match secrets.delete(secret_name, &params).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(response)) if response.code == 404 => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Return the invite's Secret only after checking its owner reference.
async fn owned_secret(
    invite: &SiteInvite,
    secrets: &Api<Secret>,
    namespace: &str,
    secret_name: &str,
) -> Result<Option<Secret>, ReconcileError> {
    let secret = match secrets.get(secret_name).await {
        Ok(secret) => secret,
        Err(kube::Error::Api(response)) if response.code == 404 => return Ok(None),
        Err(error) => return Err(error.into()),
    };
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
    Ok(Some(secret))
}

/// Apply the controller-owned status fields.
async fn api_patch_status(client: &Client, invite: &SiteInvite, value: serde_json::Value) -> Result<(), kube::Error> {
    Api::<SiteInvite>::all(client.clone())
        .patch_status(
            &invite.name_any(),
            &PatchParams::apply("grid-enrollment"),
            &Patch::Apply(status_apply_patch(&value)),
        )
        .await
        .map(|_updated| ())
}

/// Add the resource metadata required by Kubernetes server-side apply.
fn status_apply_patch(value: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "SiteInvite",
        "status": value.get("status").cloned().unwrap_or_default(),
    })
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
mod conformance;

#[cfg(test)]
mod tests {
    use super::{InviteState, ReconcileError, checkpoint_from_status, duration, invite_conditions};

    #[test]
    fn a_reason_change_alone_keeps_the_transition_time() {
        let mut invite: site_invite_api::SiteInvite = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "SiteInvite",
            "metadata": { "name": "site-d", "generation": 1 },
            "spec": { "siteName": "site-d", "jurisdictions": [], "lifetime": "1h" }
        }))
        .unwrap_or_else(|_| std::process::abort());
        invite.status = Some(site_invite_api::SiteInviteStatus {
            conditions: vec![site_invite_api::SiteInviteCondition {
                type_: "Redeemed".to_owned(),
                status: "False".to_owned(),
                reason: "AwaitingRequest".to_owned(),
                message: String::new(),
                observed_generation: 1,
                last_transition_time: "2026-10-01T00:00:00Z".to_owned(),
            }],
            ..site_invite_api::SiteInviteStatus::default()
        });
        let conditions = invite_conditions(&invite, InviteState::Expired).unwrap_or_default();
        let redeemed = conditions.iter().find(|condition| condition["type"] == "Redeemed");
        assert!(redeemed.is_some_and(|condition| {
            condition["reason"] == "Expired" && condition["lastTransitionTime"] == "2026-10-01T00:00:00Z"
        }));
    }

    #[test]
    fn parses_kubernetes_durations_within_the_invite_bounds() {
        assert!(matches!(duration("5m"), Ok(value) if value.whole_seconds() == 300));
        assert!(matches!(duration("1h30m"), Ok(value) if value.whole_seconds() == 5_400));
        assert!(matches!(duration("5m500ms"), Ok(value) if value.whole_nanoseconds() == 300_500_000_000));
        assert!(matches!(duration("4m59s"), Err(ReconcileError::Invalid(_))));
        assert!(matches!(duration("169h"), Err(ReconcileError::Invalid(_))));
        assert!(matches!(duration("24hours"), Err(ReconcileError::Invalid(_))));
    }

    #[test]
    fn status_apply_patch_includes_the_resource_type() {
        let status_value = serde_json::json!({"status": {"tokenId": "id"}});
        let patch = super::status_apply_patch(&status_value);

        assert!(
            patch
                .get("apiVersion")
                .is_some_and(|field| field == "grid.praxis.fast/v1alpha1")
        );
        assert!(patch.get("kind").is_some_and(|field| field == "SiteInvite"));
        assert!(
            patch
                .get("status")
                .and_then(|status| status.get("tokenId"))
                .is_some_and(|field| field == "id")
        );
    }

    #[test]
    fn rebuilds_terminal_checkpoint_from_status_without_secret_digest() {
        let status = site_invite_api::SiteInviteStatus {
            token_id: Some("e550fae6-e6f8-4e3a-9d13-1b42f2cce67d".to_owned()),
            expires_at: Some("2026-10-14T12:00:00Z".to_owned()),
            ..site_invite_api::SiteInviteStatus::default()
        };

        let checkpoint = checkpoint_from_status(&status);

        assert!(matches!(checkpoint, Ok(value)
            if value.token_id.to_string() == "e550fae6-e6f8-4e3a-9d13-1b42f2cce67d"
                && value.expires_at_raw == "2026-10-14T12:00:00Z"
                && value.token_sha256.is_none()));
    }
}
