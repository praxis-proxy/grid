//! Grid-admin authorization, with a pluggable backend.
//!
//! A grid-admin is the party allowed to mint and revoke site tokens, named so it
//! is not confused with the grid-operator controller. The caller presents
//! `Authorization: Bearer <token>`. What differs between backends is the token's
//! origin and who decides.
//!
//!   - [`Authorizer::Local`] is the grid-admin token table, the standalone, cluster-free default. A valid token
//!     authorizes the action.
//!   - `Authorizer::Kube` (feature `sar`) reuses Kubernetes RBAC. The bearer is authenticated with a `TokenReview`, and
//!     the action authorized with a `SubjectAccessReview` against the virtual `grid.praxis-proxy.io/enrollmenttokens`
//!     resource. Permissions are ordinary `Roles` or `ClusterRoles`, no CRD required.
//!
//! Reaching the review APIs needs only a `ServiceAccount` bound to
//! `system:auth-delegator`, not a kubeconfig for managing resources.

use crate::auth::GridAdmins;

/// The virtual apiGroup grid-admin permissions are written against.
#[cfg(feature = "sar")]
const ENROLLMENTS_GROUP: &str = "grid.praxis-proxy.io";

/// Why a grid-admin request was refused.
#[derive(Debug, thiserror::Error)]
pub enum AuthzError {
    /// No credential, or one that does not authenticate.
    #[error("a grid-admin credential is required")]
    Unauthenticated,
    /// Authenticated, but not permitted the action.
    #[error("not permitted to {0} site tokens")]
    Forbidden(String),
    /// The authorization backend itself failed (e.g. the API server is unreachable).
    #[error("authorization backend error: {0}")]
    Backend(String),
}

/// The action a route authorizes: a verb on a resource, optionally on a
/// subresource.
///
/// Minting a token is `create` and revoking is `delete` on the `enrollmenttokens`
/// resource, so Role authors use the standard verb set and can grant minting
/// apart from any other permission.
#[derive(Debug, Clone, Copy)]
pub struct Operation {
    /// The RBAC resource acted on (e.g. `enrollmenttokens`).
    pub resource: &'static str,
    /// The RBAC verb (e.g. `create`, `delete`).
    pub verb: &'static str,
    /// The subresource the verb acts on, if any.
    pub subresource: Option<&'static str>,
}

/// Grid-admin authorization backend, chosen at startup.
pub enum Authorizer {
    /// Grid-admin token table: a valid token resolves to the grid-admin's name.
    Local(GridAdmins),
    /// Kubernetes RBAC via `TokenReview` + `SubjectAccessReview`.
    #[cfg(feature = "sar")]
    Kube(KubeAuthorizer),
}

impl std::fmt::Debug for Authorizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(_) => f.write_str("Authorizer::Local"),
            #[cfg(feature = "sar")]
            Self::Kube(_) => f.write_str("Authorizer::Kube"),
        }
    }
}

impl Authorizer {
    /// Authorize the operation for the caller, returning the grid-admin name to
    /// record on success.
    ///
    /// # Errors
    ///
    /// [`AuthzError::Unauthenticated`] for a missing or unknown credential,
    /// [`AuthzError::Forbidden`] when authenticated but not permitted, and
    /// [`AuthzError::Backend`] when the backend cannot render a decision.
    #[cfg_attr(
        not(feature = "sar"),
        expect(
            unused_variables,
            clippy::unused_async,
            reason = "operation and async are consulted only by the Kubernetes-RBAC backend"
        )
    )]
    pub async fn decide(&self, bearer: &str, operation: Operation) -> Result<String, AuthzError> {
        match self {
            Self::Local(admins) => admins
                .resolve(bearer)
                .map(str::to_owned)
                .ok_or(AuthzError::Unauthenticated),
            #[cfg(feature = "sar")]
            Self::Kube(kube) => kube.decide(bearer, operation).await,
        }
    }
}

/// Kubernetes-RBAC authorizer: `TokenReview` to authenticate, `SubjectAccessReview`
/// to authorize.
#[cfg(feature = "sar")]
pub struct KubeAuthorizer {
    /// Client for the review APIs, using the auth-delegator `ServiceAccount`.
    client: kube::Client,
}

#[cfg(feature = "sar")]
impl KubeAuthorizer {
    /// Connect using in-cluster config (the auth-delegator `ServiceAccount`) or,
    /// out of cluster, the ambient kubeconfig.
    ///
    /// # Errors
    ///
    /// Returns the client error if no usable configuration is found.
    pub async fn connect() -> Result<Self, String> {
        let client = kube::Client::try_default().await.map_err(|error| error.to_string())?;
        Ok(Self { client })
    }

    /// Authenticate the bearer, then authorize the operation. The recorded
    /// grid-admin name is the authenticated username.
    ///
    /// Two API-server round trips per request (`TokenReview`, then
    /// `SubjectAccessReview`) with no caching. Deliberately uncached: at
    /// enrollment volumes, where grid-admins mint tokens, the cost is negligible,
    /// and caching an authorization decision is its own hazard.
    async fn decide(&self, bearer: &str, operation: Operation) -> Result<String, AuthzError> {
        let (user, groups) = self.authenticate(bearer).await?;
        if self.authorize(&user, &groups, operation).await? {
            Ok(user)
        } else {
            Err(AuthzError::Forbidden(operation.verb.to_owned()))
        }
    }

    /// Resolve the bearer to a Kubernetes identity via `TokenReview`.
    async fn authenticate(&self, bearer: &str) -> Result<(String, Vec<String>), AuthzError> {
        use k8s_openapi::api::authentication::v1::{TokenReview, TokenReviewSpec};
        use kube::api::{Api, PostParams};

        let review = TokenReview {
            spec: TokenReviewSpec {
                token: Some(bearer.to_owned()),
                audiences: None,
            },
            ..Default::default()
        };
        let api: Api<TokenReview> = Api::all(self.client.clone());
        let reviewed = api
            .create(&PostParams::default(), &review)
            .await
            .map_err(|error| AuthzError::Backend(error.to_string()))?;

        let status = reviewed
            .status
            .ok_or_else(|| AuthzError::Backend("TokenReview returned no status".to_owned()))?;
        if !status.authenticated.unwrap_or(false) {
            return Err(AuthzError::Unauthenticated);
        }
        let userinfo = status.user.ok_or(AuthzError::Unauthenticated)?;
        Ok((
            userinfo.username.unwrap_or_default(),
            userinfo.groups.unwrap_or_default(),
        ))
    }

    /// Ask Kubernetes RBAC whether `user`/`groups` may perform `operation` on its
    /// resource.
    async fn authorize(&self, user: &str, groups: &[String], operation: Operation) -> Result<bool, AuthzError> {
        use k8s_openapi::api::authorization::v1::{ResourceAttributes, SubjectAccessReview, SubjectAccessReviewSpec};
        use kube::api::{Api, PostParams};

        let review = SubjectAccessReview {
            spec: SubjectAccessReviewSpec {
                user: Some(user.to_owned()),
                groups: Some(groups.to_vec()),
                resource_attributes: Some(ResourceAttributes {
                    group: Some(ENROLLMENTS_GROUP.to_owned()),
                    resource: Some(operation.resource.to_owned()),
                    subresource: operation.subresource.map(str::to_owned),
                    verb: Some(operation.verb.to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let api: Api<SubjectAccessReview> = Api::all(self.client.clone());
        let reviewed = api
            .create(&PostParams::default(), &review)
            .await
            .map_err(|error| AuthzError::Backend(error.to_string()))?;

        Ok(reviewed.status.is_some_and(|status| status.allowed))
    }
}
