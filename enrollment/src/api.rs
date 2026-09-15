//! The HTTP interface for site-token enrollment.
//!
//! A grid-admin mints a single-use site token that pins a name. A site presents
//! the token and a certificate signing request and receives a signed certificate
//! in the same response. Minting is gated by a grid-admin credential. Enrolling
//! is gated by the token.

use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, FromRequestParts, MatchedPath, Path, Request, State},
    http::{HeaderMap, Method, StatusCode, request::Parts},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete as delete_route, post},
};
use certs::{CaCert, EnrollError, MAX_CSR_PEM_BYTES, Validity, sign_csr, validate_site_name, verify_csr};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    auth::digest,
    authz::{Authorizer, AuthzError, Operation},
    generated::{Enrollment, EnrollmentRequest, EnrollmentToken, EnrollmentTokenRequest, Error as ErrorBody},
    store::{Issued, NewSiteToken, Pin, Store, StoreError},
};

/// How long a token stays usable when the grid-admin names no expiry.
const DEFAULT_TOKEN_TTL_SECS: i64 = 24 * 60 * 60;

/// How long a single request may run before it is cut off. Bounds the time a slow
/// caller holds a task, the way the body limit bounds the bytes.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// What the handlers need.
#[derive(Debug)]
pub struct AppState {
    /// Where tokens and enrollments are kept.
    pub store: Store,

    /// The CA that signs enrolled certificates.
    pub ca: CaCert,

    /// How minting and revoking are authorized (grid-admin token table, or RBAC).
    pub authorizer: Authorizer,

    /// How long an issued certificate lasts.
    ///
    /// Held here rather than taken per call, so every certificate this grid
    /// issues has the same bound and no route can quietly issue a longer one.
    pub cert_lifetime: time::Duration,
}

/// Failures the interface can report.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// The submission was not usable.
    #[error("{message}")]
    BadRequest {
        /// Machine-readable code.
        code: &'static str,
        /// What went wrong.
        message: String,
    },

    /// No token with that identifier.
    #[error("no such site token")]
    NotFound,

    /// Another member already holds the pinned name.
    #[error("site name is already taken")]
    NameTaken,

    /// The caller presented no grid-admin credential, or one that is not known.
    #[error("a grid-admin credential is required")]
    Unauthorized,

    /// The enrollment presented no usable site token.
    ///
    /// Missing, expired, and already redeemed are one error, so a caller cannot
    /// learn which tokens exist or have been used.
    #[error("a valid site token is required")]
    InvalidToken,

    /// The caller authenticated but is not permitted the action.
    #[error("not permitted")]
    Forbidden,

    /// The service itself failed.
    #[error("{0}")]
    Internal(String),
}

impl From<AuthzError> for ApiError {
    fn from(error: AuthzError) -> Self {
        match error {
            AuthzError::Unauthenticated => Self::Unauthorized,
            AuthzError::Forbidden(_) => Self::Forbidden,
            AuthzError::Backend(message) => Self::Internal(message),
        }
    }
}

impl From<StoreError> for ApiError {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::NotFound => Self::NotFound,
            StoreError::NameTaken => Self::NameTaken,
            StoreError::TokenInvalid => Self::InvalidToken,
            StoreError::Backend(detail) => Self::Internal(detail),
        }
    }
}

/// An authenticated grid-admin, the party allowed to mint and revoke site tokens.
///
/// Extracting this is what gates minting, so a handler that takes it cannot be
/// reached without a credential. Named grid-admin rather than operator, so it is
/// not confused with the grid-operator controller.
#[derive(Debug, Clone)]
pub struct GridAdmin(pub String);

impl FromRequestParts<Arc<AppState>> for GridAdmin {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &Arc<AppState>) -> Result<Self, Self::Rejection> {
        let presented = bearer(&parts.headers).ok_or(ApiError::Unauthorized)?;

        // The action being authorized, derived from the matched route. The local
        // token backend ignores it; the Kubernetes-RBAC backend maps it to a
        // SubjectAccessReview.
        let operation = route_operation(parts);

        // An unknown token and a missing one are reported the same way, so the
        // interface cannot be used to test whether a credential exists.
        state
            .authorizer
            .decide(presented, operation)
            .await
            .map(Self)
            .map_err(ApiError::from)
    }
}

/// The authorization operation for the matched route.
///
/// Only the enrollment-token routes extract a grid-admin, so the resource is
/// always enrollmenttokens. Minting is a create, revoking is a delete, and RBAC
/// can grant them apart from any other permission.
fn route_operation(parts: &Parts) -> Operation {
    let verb = if parts.method == Method::DELETE {
        "delete"
    } else {
        "create"
    };
    // Keyed off the matched route so a route added later fails closed (an
    // unmapped path resolves to a resource no Role grants) instead of inheriting.
    let resource = if matches!(
        parts.extensions.get::<MatchedPath>().map(MatchedPath::as_str),
        Some("/v1alpha1/enrollmenttokens" | "/v1alpha1/enrollmenttokens/{token_id}")
    ) {
        "enrollmenttokens"
    } else {
        "unknown"
    };
    Operation {
        resource,
        verb,
        subresource: None,
    }
}

impl ApiError {
    /// The HTTP status, machine-readable code, and human message for the wire.
    #[expect(
        clippy::too_many_lines,
        reason = "one arm per error variant, each with its wire message"
    )]
    fn rendered(self) -> (StatusCode, &'static str, String) {
        match self {
            Self::BadRequest { code, message } => (StatusCode::BAD_REQUEST, code, message),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "no site token has that identifier".to_owned(),
            ),
            Self::NameTaken => (
                StatusCode::CONFLICT,
                "name_taken",
                "another member already holds this site name".to_owned(),
            ),
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "minting or revoking a token requires a grid-admin credential".to_owned(),
            ),
            Self::InvalidToken => (
                StatusCode::UNAUTHORIZED,
                "invalid_token",
                "a valid site token is required, ask a grid-admin for one".to_owned(),
            ),
            Self::Forbidden => (
                StatusCode::FORBIDDEN,
                "forbidden",
                "not permitted to mint or revoke site tokens".to_owned(),
            ),
            Self::Internal(message) => {
                tracing::error!(error = %message, "enrollment request could not be served");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    "the enrollment service could not complete the request".to_owned(),
                )
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = self.rendered();
        (
            status,
            Json(ErrorBody {
                error: code.to_owned(),
                message,
            }),
        )
            .into_response()
    }
}

/// Turn a signing refusal into something the caller can act on.
///
/// A refused request is the caller's to fix, except for a signing fault, which
/// is the grid's.
fn signing_error(err: EnrollError) -> ApiError {
    match err {
        EnrollError::Signing(detail) => ApiError::Internal(detail),
        EnrollError::TooLarge
        | EnrollError::Malformed
        | EnrollError::BadSignature
        | EnrollError::UnsupportedExtension
        | EnrollError::InvalidSiteName => ApiError::BadRequest {
            code: "invalid_csr",
            message: err.to_string(),
        },
    }
}

/// The routes, with a body limit sized for a certificate request.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/v1alpha1/enrollmenttokens", post(mint_site_token))
        .route("/v1alpha1/enrollmenttokens/{token_id}", delete_route(revoke_site_token))
        .route("/v1alpha1/enrollments", post(enroll))
        .layer(DefaultBodyLimit::max(MAX_CSR_PEM_BYTES.saturating_mul(2)))
        .layer(middleware::from_fn(enforce_timeout))
        .with_state(state)
}

/// Cut a request off if it runs past [`REQUEST_TIMEOUT`].
///
/// Body extraction runs inside the handler, so this also bounds a slow caller that
/// dribbles a request body under the size limit. It does not bound the TLS
/// handshake or the header read, which happen before this layer runs.
async fn enforce_timeout(request: Request, next: Next) -> Response {
    match tokio::time::timeout(REQUEST_TIMEOUT, next.run(request)).await {
        Ok(response) => response,
        Err(_elapsed) => (
            StatusCode::REQUEST_TIMEOUT,
            Json(ErrorBody {
                error: "timeout".to_owned(),
                message: "the request exceeded the time limit".to_owned(),
            }),
        )
            .into_response(),
    }
}

/// Mint a site token so a site can enroll under a name the grid-admin pins.
///
/// The pin is validated here, so a bad name fails at mint rather than at the
/// site's enroll. The token is returned once, and only its digest is stored.
#[expect(
    clippy::too_many_lines,
    reason = "validate, mint, store, and build the response read as one flow"
)]
async fn mint_site_token(
    State(state): State<Arc<AppState>>,
    GridAdmin(admin): GridAdmin,
    Json(input): Json<EnrollmentTokenRequest>,
) -> Result<(StatusCode, Json<EnrollmentToken>), ApiError> {
    validate_site_name(&input.site_name).map_err(|err| ApiError::BadRequest {
        code: "invalid_site_name",
        message: err.to_string(),
    })?;
    if input.grid_network_ref.trim().is_empty() {
        return Err(ApiError::BadRequest {
            code: "missing_grid_network",
            message: "gridNetworkRef must name the grid the site joins".to_owned(),
        });
    }

    // An explicit invalid expiry is refused rather than quietly defaulting, so a
    // caller cannot be granted a longer-lived token than it asked for. The default
    // is reserved for an unset expiry.
    let ttl_secs = match input.expires_in_secs {
        None => DEFAULT_TOKEN_TTL_SECS,
        Some(secs) if secs <= 0 => {
            return Err(ApiError::BadRequest {
                code: "invalid_token_ttl",
                message: "expiresInSecs must be greater than zero".to_owned(),
            });
        },
        Some(secs) => secs,
    };
    let expires_at = OffsetDateTime::now_utc().saturating_add(time::Duration::seconds(ttl_secs));
    let expires_at_wire = expires_at
        .format(&Rfc3339)
        .map_err(|err| ApiError::Internal(format!("formatting expiry: {err}")))?;

    let token = generate_token()?;
    let token_id = state
        .store
        .mint_site_token(NewSiteToken {
            token_sha256: digest(&token),
            site_name: input.site_name.clone(),
            grid_network_ref: input.grid_network_ref,
            issued_by: admin.clone(),
            expires_at,
        })
        .await?;

    tracing::info!(%token_id, site = %input.site_name, %admin, "site token minted");
    Ok((
        StatusCode::CREATED,
        Json(EnrollmentToken {
            token_id,
            token,
            site_name: input.site_name,
            expires_at: expires_at_wire,
        }),
    ))
}

/// Revoke a site token before it is redeemed, a kill switch for one that leaked.
async fn revoke_site_token(
    State(state): State<Arc<AppState>>,
    GridAdmin(admin): GridAdmin,
    Path(token_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state.store.revoke_site_token(token_id).await?;
    tracing::info!(%token_id, %admin, "site token revoked");
    Ok(StatusCode::NO_CONTENT)
}

/// Enroll under a site token: prove possession, redeem the token, get a cert.
///
/// The token is checked before the CSR is parsed, so an unusable token is refused
/// before any signature work. The certificate is signed against the token's
/// pinned name, and the token is spent only when the certificate is issued.
#[expect(
    clippy::too_many_lines,
    reason = "the enroll flow, token to signed certificate, reads as one narrative"
)]
async fn enroll(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<EnrollmentRequest>,
) -> Result<(StatusCode, Json<Enrollment>), ApiError> {
    let token = site_token(&headers)?;
    let token_sha256 = digest(&token);

    // Refuse an unusable token before spending CSR crypto on it.
    if !state.store.token_valid(&token_sha256).await? {
        return Err(ApiError::InvalidToken);
    }

    // Prove possession of the key before consuming the token. verify_csr confirms
    // the CSR self-signature and fails closed. No name is involved here.
    verify_csr(&input.csr).map_err(signing_error)?;

    let csr = input.csr;
    let validity = Validity::starting_now(state.cert_lifetime);
    let (enrollment_id, issued) = state
        .store
        .redeem_and_issue(&token_sha256, |pin: &Pin| {
            // Signed under the pinned name, with every SAN rebuilt from it.
            sign_csr(&state.ca, &pin.site_name, &csr, validity)
                .map(|cert| Issued {
                    certificate: cert.cert_pem,
                    spiffe_id: cert.spiffe_id,
                    public_key_sha256: cert.public_key_sha256,
                })
                .map_err(|err| StoreError::Backend(format!("signing failed: {err}")))
        })
        .await?;

    tracing::info!(%enrollment_id, spiffe_id = %issued.spiffe_id, "site enrolled");
    Ok((
        StatusCode::CREATED,
        Json(Enrollment {
            id: enrollment_id,
            certificate: issued.certificate,
            // The grid CA rides back with the certificate, so a site holds the
            // trust anchor without a separate fetch it could not yet verify.
            ca_certificate: state.ca.cert_pem.clone(),
            spiffe_id: issued.spiffe_id,
            public_key_sha256: issued.public_key_sha256,
        }),
    ))
}

/// The one-time site token from `Authorization: Bearer`, or a refusal.
///
/// The site presents its token in the same header shape the grid-admin routes
/// use. A missing or malformed header is the same [`ApiError::InvalidToken`] as
/// an unusable token, so the endpoint reveals nothing about which tokens exist.
fn site_token(headers: &HeaderMap) -> Result<String, ApiError> {
    bearer(headers).map(str::to_owned).ok_or(ApiError::InvalidToken)
}

/// The bearer credential from `Authorization: Bearer <token>`, trimmed and
/// non-empty, or `None`.
///
/// Both credentials use it: the grid-admin's on the token routes and the site's
/// on enroll, so the two cannot drift in how they read the header. The scheme is
/// matched case-insensitively, as RFC 7235 requires. The token itself is taken
/// verbatim.
fn bearer(headers: &HeaderMap) -> Option<&str> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())?;
    let (scheme, token) = value.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim())
        .filter(|token| !token.is_empty())
}

/// A fresh one-time site token: 256 bits from the system CSPRNG, hex encoded.
///
/// ring's `SystemRandom`, the same source the gossip signing path uses, and never
/// `SmallRng`. Hex keeps it header-safe with no padding.
fn generate_token() -> Result<String, ApiError> {
    use ring::rand::SecureRandom as _;
    let mut bytes = [0_u8; 32];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_unspecified| ApiError::Internal("system random source unavailable".to_owned()))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}
