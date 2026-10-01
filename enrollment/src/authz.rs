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

/// The pod's namespace, as the kubelet mounts it with the service account token.
#[cfg(feature = "sar")]
const SERVICE_ACCOUNT_NAMESPACE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

/// Audience a grid-admin token must be bound to unless configured otherwise.
#[cfg(feature = "sar")]
pub const DEFAULT_TOKEN_AUDIENCE: &str = "grid-enrollment";

/// API-server audiences every general token carries; binding to one would let
/// any API token authenticate.
#[cfg(feature = "sar")]
const API_SERVER_AUDIENCES: [&str; 3] = [
    "https://kubernetes.default.svc",
    "https://kubernetes.default.svc.cluster.local",
    "kubernetes",
];

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
    /// Audience the bearer must be bound to, so a token minted for another
    /// service cannot be replayed here.
    audience: String,
    /// Namespace the review is scoped to, so a `Role` there can grant access.
    namespace: String,
}

#[cfg(feature = "sar")]
impl KubeAuthorizer {
    /// Connect using in-cluster config (the auth-delegator `ServiceAccount`) or,
    /// out of cluster, the ambient kubeconfig with `POD_NAMESPACE` set. Bearers
    /// must be bound to `audience`. Reviews are scoped to `POD_NAMESPACE`, else
    /// the pod's service account namespace, never a kubeconfig context namespace.
    ///
    /// # Errors
    ///
    /// Returns an error for a rejected audience, no resolvable namespace, or the
    /// client error if no usable configuration is found.
    pub async fn connect(audience: String) -> Result<Self, String> {
        check_audience(&audience)?;
        let client = kube::Client::try_default().await.map_err(|error| error.to_string())?;
        let namespace = review_namespace(std::env::var("POD_NAMESPACE").ok(), || {
            std::fs::read_to_string(SERVICE_ACCOUNT_NAMESPACE).ok()
        })
        .ok_or("set POD_NAMESPACE to the enrollment namespace the grid-admin review is scoped to")?;
        Ok(Self {
            client,
            audience,
            namespace,
        })
    }

    /// The namespace grid-admin reviews are scoped to.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Authenticate the bearer, then authorize the operation. The recorded
    /// grid-admin name is the authenticated username.
    ///
    /// Two API-server round trips per request (`TokenReview`, then
    /// `SubjectAccessReview`) with no caching. Deliberately uncached: at
    /// enrollment volumes, where grid-admins mint tokens, the cost is negligible,
    /// and caching an authorization decision is its own hazard.
    async fn decide(&self, bearer: &str, operation: Operation) -> Result<String, AuthzError> {
        // Refuse junk before it costs two uncached API-server round trips.
        if !is_jwt_shaped(bearer) {
            return Err(AuthzError::Unauthenticated);
        }
        let user = self.authenticate(bearer).await?;
        if self.authorize(&user, operation).await? {
            Ok(user.username.unwrap_or_default())
        } else {
            Err(AuthzError::Forbidden(operation.verb.to_owned()))
        }
    }

    /// Resolve the bearer to a Kubernetes identity via `TokenReview`.
    async fn authenticate(&self, bearer: &str) -> Result<k8s_openapi::api::authentication::v1::UserInfo, AuthzError> {
        use k8s_openapi::api::authentication::v1::{TokenReview, TokenReviewSpec};
        use kube::api::{Api, PostParams};

        let review = TokenReview {
            spec: TokenReviewSpec {
                token: Some(bearer.to_owned()),
                audiences: Some(vec![self.audience.clone()]),
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
        reviewed_identity(status, &self.audience)
    }

    /// Ask Kubernetes RBAC whether the reviewed `user` may perform `operation`
    /// on its resource.
    async fn authorize(
        &self,
        user: &k8s_openapi::api::authentication::v1::UserInfo,
        operation: Operation,
    ) -> Result<bool, AuthzError> {
        use k8s_openapi::api::authorization::v1::SubjectAccessReview;
        use kube::api::{Api, PostParams};

        let review = access_review(&self.namespace, user, operation);
        let api: Api<SubjectAccessReview> = Api::all(self.client.clone());
        let reviewed = api
            .create(&PostParams::default(), &review)
            .await
            .map_err(|error| AuthzError::Backend(error.to_string()))?;

        Ok(reviewed.status.is_some_and(|status| status.allowed))
    }
}

/// The resource a grid-admin acts on, in the enrollment service's namespace, so
/// a `Role` bound there grants it and a `ClusterRole` still does cluster-wide.
#[cfg(feature = "sar")]
fn resource_attributes(
    namespace: &str,
    operation: Operation,
) -> k8s_openapi::api::authorization::v1::ResourceAttributes {
    k8s_openapi::api::authorization::v1::ResourceAttributes {
        namespace: Some(namespace.to_owned()),
        group: Some(ENROLLMENTS_GROUP.to_owned()),
        resource: Some(operation.resource.to_owned()),
        subresource: operation.subresource.map(str::to_owned),
        verb: Some(operation.verb.to_owned()),
        ..Default::default()
    }
}

/// `pod_namespace` if non-blank, else the service account namespace.
#[cfg(feature = "sar")]
fn review_namespace(pod_namespace: Option<String>, service_account: impl FnOnce() -> Option<String>) -> Option<String> {
    let non_blank = |namespace: String| Some(namespace.trim().to_owned()).filter(|trimmed| !trimmed.is_empty());
    pod_namespace
        .and_then(non_blank)
        .or_else(|| service_account().and_then(non_blank))
}

/// Refuse a blank audience or one every API token already carries.
#[cfg(feature = "sar")]
fn check_audience(audience: &str) -> Result<(), String> {
    if audience.trim().is_empty() {
        return Err("the grid-admin token audience must not be blank".to_owned());
    }
    if audience.trim() != audience {
        return Err(format!(
            "the grid-admin token audience {audience:?} has surrounding whitespace"
        ));
    }
    if API_SERVER_AUDIENCES.contains(&audience.trim_end_matches('/')) {
        return Err(format!(
            "the grid-admin token audience {audience:?} is an API-server audience; use a dedicated one"
        ));
    }
    Ok(())
}

/// Upper bound on a bearer worth reviewing; bound service account tokens are
/// well under 2 KiB.
#[cfg(feature = "sar")]
const MAX_BEARER_LEN: usize = 8192;

/// Whether `bearer` has the shape of a service account token: a bounded JWT of
/// three non-empty base64url segments.
#[cfg(feature = "sar")]
fn is_jwt_shaped(bearer: &str) -> bool {
    bearer.len() <= MAX_BEARER_LEN
        && bearer.split('.').count() == 3
        && bearer.split('.').all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        })
}

/// The `SubjectAccessReview` for `operation` by the reviewed `user`. It carries
/// the token's uid and extra (for a bound service account token, the
/// credential id and pod binding), so an authorizer or webhook keyed on them
/// judges the same identity the `TokenReview` authenticated.
#[cfg(feature = "sar")]
fn access_review(
    namespace: &str,
    user: &k8s_openapi::api::authentication::v1::UserInfo,
    operation: Operation,
) -> k8s_openapi::api::authorization::v1::SubjectAccessReview {
    use k8s_openapi::api::authorization::v1::{SubjectAccessReview, SubjectAccessReviewSpec};

    SubjectAccessReview {
        spec: SubjectAccessReviewSpec {
            user: user.username.clone(),
            groups: user.groups.clone(),
            uid: user.uid.clone(),
            extra: user.extra.clone(),
            resource_attributes: Some(resource_attributes(namespace, operation)),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// The identity a `TokenReview` authenticated, provided it is bound to
/// `audience`. The API server returns the audiences the token is valid for,
/// and an authenticator that ignores audiences returns none, so both an empty
/// list and a list without `audience` are refused.
#[cfg(feature = "sar")]
fn reviewed_identity(
    status: k8s_openapi::api::authentication::v1::TokenReviewStatus,
    audience: &str,
) -> Result<k8s_openapi::api::authentication::v1::UserInfo, AuthzError> {
    if !status.authenticated.unwrap_or(false) {
        return Err(AuthzError::Unauthenticated);
    }
    if !status
        .audiences
        .unwrap_or_default()
        .iter()
        .any(|bound| bound == audience)
    {
        return Err(AuthzError::Unauthenticated);
    }
    status
        .user
        .filter(|user| user.username.as_deref().is_some_and(|name| !name.is_empty()))
        .ok_or(AuthzError::Unauthenticated)
}

#[cfg(test)]
#[cfg(feature = "sar")]
mod tests {
    use std::collections::BTreeMap;

    use k8s_openapi::api::authentication::v1::{TokenReviewStatus, UserInfo};

    use super::{
        AuthzError, MAX_BEARER_LEN, Operation, access_review, check_audience, is_jwt_shaped, resource_attributes,
        review_namespace, reviewed_identity,
    };

    fn status(authenticated: bool, audiences: Option<&[&str]>) -> TokenReviewStatus {
        TokenReviewStatus {
            authenticated: Some(authenticated),
            audiences: audiences.map(|list| list.iter().map(|aud| (*aud).to_owned()).collect()),
            user: Some(UserInfo {
                username: Some("alice".to_owned()),
                groups: Some(vec!["grid-admins".to_owned()]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn blank_pod_namespace_falls_back_to_the_service_account() {
        let sa = || Some("grid-system\n".to_owned());
        assert_eq!(
            review_namespace(Some("  ".to_owned()), sa).as_deref(),
            Some("grid-system")
        );
        assert_eq!(review_namespace(None, sa).as_deref(), Some("grid-system"));
        assert_eq!(review_namespace(Some(" edge ".to_owned()), sa).as_deref(), Some("edge"));
        assert_eq!(review_namespace(Some(String::new()), || Some(" ".to_owned())), None);
    }

    #[test]
    fn accepts_a_token_bound_to_the_audience() {
        let identity = reviewed_identity(status(true, Some(&["grid-enrollment"])), "grid-enrollment");
        assert!(
            matches!(&identity, Ok(user) if user.username.as_deref() == Some("alice")),
            "got {identity:?}"
        );
    }

    #[test]
    fn refuses_a_token_without_the_audience() {
        for audiences in [None, Some(&[][..]), Some(&["https://kubernetes.default.svc"][..])] {
            let identity = reviewed_identity(status(true, audiences), "grid-enrollment");
            assert!(
                matches!(identity, Err(AuthzError::Unauthenticated)),
                "{audiences:?}: got {identity:?}"
            );
        }
    }

    #[test]
    fn refuses_an_unauthenticated_token() {
        let identity = reviewed_identity(status(false, Some(&["grid-enrollment"])), "grid-enrollment");
        assert!(matches!(identity, Err(AuthzError::Unauthenticated)), "got {identity:?}");
    }

    #[test]
    fn jwt_shaped_bearers_pass_the_precheck() {
        assert!(
            is_jwt_shaped("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhIn0.c2ln-_x"),
            "a JWT passes"
        );
    }

    #[test]
    fn junk_bearers_fail_the_precheck() {
        let oversized = format!("a.b.{}", "c".repeat(MAX_BEARER_LEN));
        for bearer in [
            "",
            "opaque",
            "a.b",
            "a.b.c.d",
            "a..c",
            "a.b.c=",
            "a.b.c d",
            oversized.as_str(),
        ] {
            assert!(!is_jwt_shaped(bearer), "{} must fail", bearer.len());
        }
    }

    #[test]
    fn refuses_api_server_and_blank_audiences() {
        for audience in [
            "",
            " ",
            " grid-enrollment",
            "grid-enrollment\n",
            "kubernetes",
            "https://kubernetes.default.svc",
            "https://kubernetes.default.svc/",
        ] {
            assert!(check_audience(audience).is_err(), "{audience:?} must be refused");
        }
        assert!(
            check_audience("grid-enrollment").is_ok(),
            "a dedicated audience is accepted"
        );
    }

    #[test]
    fn refuses_a_token_with_no_username() {
        let mut reviewed = status(true, Some(&["grid-enrollment"]));
        if let Some(user) = reviewed.user.as_mut() {
            user.username = Some(String::new());
        }
        let identity = reviewed_identity(reviewed, "grid-enrollment");
        assert!(matches!(identity, Err(AuthzError::Unauthenticated)), "got {identity:?}");
    }

    #[test]
    fn access_review_carries_the_token_uid_and_extra() {
        let scopes = BTreeMap::from([(
            "authentication.kubernetes.io/credential-id".to_owned(),
            vec!["JTI=token-1".to_owned()],
        )]);
        let user = UserInfo {
            username: Some("alice".to_owned()),
            groups: Some(vec!["grid-admins".to_owned()]),
            uid: Some("uid-1".to_owned()),
            extra: Some(scopes.clone()),
        };
        let operation = Operation {
            resource: "enrollmenttokens",
            verb: "create",
            subresource: None,
        };
        let spec = access_review("grid", &user, operation).spec;
        assert_eq!(spec.user.as_deref(), Some("alice"), "user");
        assert_eq!(spec.groups, Some(vec!["grid-admins".to_owned()]), "groups");
        assert_eq!(spec.uid.as_deref(), Some("uid-1"), "uid");
        assert_eq!(spec.extra, Some(scopes), "extra");
        assert_eq!(
            spec.resource_attributes
                .and_then(|attributes| attributes.namespace)
                .as_deref(),
            Some("grid"),
            "namespace"
        );
    }

    #[test]
    fn the_review_is_scoped_to_the_enrollment_namespace() {
        let operation = Operation {
            resource: "enrollmenttokens",
            verb: "create",
            subresource: None,
        };
        let attributes = resource_attributes("grid", operation);
        assert_eq!(attributes.namespace.as_deref(), Some("grid"), "namespace");
        assert_eq!(attributes.group.as_deref(), Some("grid.praxis-proxy.io"), "group");
        assert_eq!(attributes.resource.as_deref(), Some("enrollmenttokens"), "resource");
        assert_eq!(attributes.verb.as_deref(), Some("create"), "verb");
    }
}
