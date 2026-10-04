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
    http::{HeaderMap, HeaderValue, Method, StatusCode, header, request::Parts},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete as delete_route, get, post},
};
use certs::{EnrollError, MAX_CSR_PEM_BYTES, Validity, sign_csr, validate_site_name, verify_csr};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    SharedCa,
    auth::digest,
    authz::{Authorizer, AuthzError, Operation},
    generated::{
        Enrollment, EnrollmentRequest, EnrollmentStatus, EnrollmentStatusState, EnrollmentToken,
        EnrollmentTokenRequest, Error as ErrorBody, ErrorError as ErrorCode,
    },
    store::{Issued, NewSiteToken, Pin, Refusal, RenewAction, Renewal, Renewed, Store, StoreError},
};

/// How long a token stays usable when the grid-admin names no expiry.
const DEFAULT_TOKEN_TTL_SECS: i64 = 24 * 60 * 60;

/// Longest token lifetime a grid-admin may ask for.
const MAX_TOKEN_TTL_SECS: i64 = 7 * 24 * 60 * 60;

/// How long a single request may run before it is cut off. Bounds the time a slow
/// caller holds a task, the way the body limit bounds the bytes.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Seconds a client waits before retrying a 503 the API answered.
const RETRY_AFTER_SECS: u64 = 300;

/// Seconds a prober waits before asking again while the store is down.
const READY_RETRY_AFTER_SECS: u64 = 5;

/// What the handlers need.
#[derive(Debug)]
pub struct AppState {
    /// Where tokens and enrollments are kept.
    pub store: Store,

    /// The CA that signs enrolled certificates, reloaded when its Secret changes.
    pub ca: SharedCa,

    /// How grid-admin requests are authorized (grid-admin token table, or RBAC).
    pub authorizer: Authorizer,

    /// How long an issued certificate lasts.
    ///
    /// Held here rather than taken per call, so every certificate this grid
    /// issues has the same bound and no route can quietly issue a longer one.
    pub cert_lifetime: time::Duration,

    /// Site names issued outside enrollment, such as the hub's, that no token may claim.
    pub reserved_sites: Vec<String>,

    /// Whether renewals are signed. Off stops all rotation; issued identities stay valid.
    pub renewals_enabled: bool,
}

/// Failures the interface can report.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// The submission was not usable.
    #[error("{message}")]
    BadRequest {
        /// Machine-readable code.
        code: ErrorCode,
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

    /// A renewal presented no usable grid site certificate.
    #[error("a current grid site certificate is required")]
    IdentityRequired,

    /// The enrollment record does not admit the presented identity.
    ///
    /// One error for every reason, so a caller cannot learn which names are held.
    #[error("this identity may not rotate")]
    IdentityRefused,

    /// No enrollment holds the site name.
    #[error("no enrollment holds that site name")]
    NoEnrollment,

    /// The name is reserved for bootstrap.
    #[error("the site name is reserved")]
    ReservedSite,

    /// Renewal is turned off for the grid.
    #[error("rotation is disabled")]
    RotationDisabled,

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
            StoreError::Refused(_) => Self::IdentityRefused,
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
/// Minting and revoking a token act on enrollmenttokens, deleting a site's record
/// on enrollments, so RBAC can grant each apart from any other permission.
fn route_operation(parts: &Parts) -> Operation {
    let verb = match parts.method {
        Method::DELETE => "delete",
        Method::GET => "get",
        _ => "create",
    };
    // Keyed off the matched route so a route added later fails closed (an
    // unmapped path resolves to a resource no Role grants) instead of inheriting.
    let resource = match parts.extensions.get::<MatchedPath>().map(MatchedPath::as_str) {
        Some("/v1alpha1/enrollmenttokens" | "/v1alpha1/enrollmenttokens/{token_id}") => "enrollmenttokens",
        Some("/v1alpha1/enrollments/{site_name}") => "enrollments",
        _ => "unknown",
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
    fn rendered(self) -> (StatusCode, ErrorCode, String) {
        match self {
            Self::BadRequest { code, message } => (StatusCode::BAD_REQUEST, code, message),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                ErrorCode::NotFound,
                "no site token has that identifier".to_owned(),
            ),
            Self::NameTaken => (
                StatusCode::CONFLICT,
                ErrorCode::NameTaken,
                "another member already holds this site name".to_owned(),
            ),
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                ErrorCode::Unauthorized,
                "this action requires a grid-admin credential".to_owned(),
            ),
            Self::InvalidToken => (
                StatusCode::UNAUTHORIZED,
                ErrorCode::InvalidToken,
                "a valid site token is required, ask a grid-admin for one".to_owned(),
            ),
            Self::Forbidden => (
                StatusCode::FORBIDDEN,
                ErrorCode::Forbidden,
                "this grid-admin credential is not permitted this action".to_owned(),
            ),
            Self::IdentityRequired => (
                StatusCode::UNAUTHORIZED,
                ErrorCode::IdentityRequired,
                "rotation requires the site's current grid certificate over mutual TLS".to_owned(),
            ),
            Self::IdentityRefused => (
                StatusCode::FORBIDDEN,
                ErrorCode::IdentityRefused,
                "this identity may not rotate; re-enroll with a new site token".to_owned(),
            ),
            Self::NoEnrollment => (
                StatusCode::NOT_FOUND,
                ErrorCode::NotFound,
                "no enrollment holds that site name".to_owned(),
            ),
            Self::RotationDisabled => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::RotationDisabled,
                "rotation is turned off for this grid; current identities stay valid until they expire".to_owned(),
            ),
            Self::ReservedSite => (
                StatusCode::CONFLICT,
                ErrorCode::ReservedSite,
                "a reserved site's identity is re-issued by the enrollment bootstrap, not deleted here".to_owned(),
            ),
            Self::Internal(message) => {
                tracing::error!(error = %message, "enrollment request could not be served");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorCode::Internal,
                    "the enrollment service could not complete the request".to_owned(),
                )
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = self.rendered();
        let mut response = (status, Json(ErrorBody { error: code, message })).into_response();
        if status == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(RETRY_AFTER_SECS));
        }
        response
    }
}

/// Turn a signing refusal into something the caller can act on.
///
/// A refused request is the caller's to fix, except for a signing fault, which
/// is the grid's.
fn signing_error(err: EnrollError) -> ApiError {
    match err {
        EnrollError::Signing(detail) => ApiError::Internal(detail),
        EnrollError::TooLarge | EnrollError::Malformed | EnrollError::BadSignature | EnrollError::InvalidSiteName => {
            ApiError::BadRequest {
                code: ErrorCode::InvalidCsr,
                message: err.to_string(),
            }
        },
    }
}

/// Liveness: the process is up and serving. Always 200.
async fn healthz() -> StatusCode {
    StatusCode::OK
}

/// Readiness: the store backend is reachable, else 503 so the pod is pulled from
/// endpoints until the database is up.
async fn readyz(State(state): State<Arc<AppState>>) -> Response {
    if state.store.ready().await {
        StatusCode::OK.into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, HeaderValue::from(READY_RETRY_AFTER_SECS))],
        )
            .into_response()
    }
}

/// The routes, with a body limit sized for a certificate request.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1alpha1/enrollmenttokens", post(mint_site_token))
        .route("/v1alpha1/enrollmenttokens/{token_id}", delete_route(revoke_site_token))
        .route("/v1alpha1/enrollments", post(enroll))
        .route(
            "/v1alpha1/enrollments/{site_name}",
            get(get_enrollment).delete(delete_enrollment),
        )
        .route("/v1alpha1/rotations", post(renew))
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
                error: ErrorCode::Timeout,
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
        code: ErrorCode::InvalidSiteName,
        message: err.to_string(),
    })?;
    if state.reserved_sites.contains(&input.site_name) {
        return Err(ApiError::NameTaken);
    }
    if input.grid_network_ref.trim().is_empty() {
        return Err(ApiError::BadRequest {
            code: ErrorCode::MissingGridNetwork,
            message: "gridNetworkRef must name the grid the site joins".to_owned(),
        });
    }

    // An explicit invalid expiry is refused rather than quietly defaulting, so a
    // caller cannot be granted a longer-lived token than it asked for. The default
    // is reserved for an unset expiry.
    let ttl_secs = match input.expires_in_secs {
        None => DEFAULT_TOKEN_TTL_SECS,
        Some(secs) if !(1..=MAX_TOKEN_TTL_SECS).contains(&secs) => {
            return Err(ApiError::BadRequest {
                code: ErrorCode::InvalidTokenTtl,
                message: format!("expiresInSecs must be between 1 and {MAX_TOKEN_TTL_SECS}"),
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
    // One snapshot, so the certificate and the CA returned with it always match.
    let ca = state.ca.current();
    let (enrollment_id, issued) = Box::pin(state.store.redeem_and_issue(&token_sha256, |pin: &Pin| {
        // A token minted before the name was reserved still cannot claim it.
        if state.reserved_sites.contains(&pin.site_name) {
            return Err(StoreError::NameTaken);
        }
        // Signed under the pinned name, with every SAN rebuilt from it.
        sign_csr(&ca, &pin.site_name, &csr, validity)
            .map(|cert| Issued {
                certificate: cert.cert_pem,
                spiffe_id: cert.spiffe_id,
                public_key_sha256: cert.public_key_sha256,
            })
            .map_err(|err| StoreError::Backend(format!("signing failed: {err}")))
    }))
    .await?;

    tracing::info!(%enrollment_id, spiffe_id = %issued.spiffe_id, "site enrolled");
    Ok((
        StatusCode::CREATED,
        Json(Enrollment {
            id: enrollment_id,
            certificate: issued.certificate,
            // The grid CA rides back with the certificate, so a site holds the
            // trust anchor without a separate fetch it could not yet verify.
            ca_certificate: ca.cert_pem.clone(),
            spiffe_id: issued.spiffe_id,
            public_key_sha256: issued.public_key_sha256,
        }),
    ))
}

/// The client certificate the TLS handshake proved, DER, set per connection.
///
/// `None` when the client presented none. The acceptor verified the handshake
/// signature, so the caller holds the leaf's key.
#[derive(Clone, Debug, Default)]
pub struct PeerLeaf(pub Option<Arc<[u8]>>);

/// A caller authenticated by the grid site certificate its TLS handshake proved.
///
/// Checked again here against the current CA, so a TLS layer that lags a CA
/// rotation cannot admit a leaf from the old one. Extracted before the body, so an
/// unauthenticated request is refused before it is parsed.
#[derive(Debug, Clone)]
pub struct SiteLeaf {
    /// The site its SPIFFE ID names.
    pub site_name: String,
    /// Its key digest.
    pub key_sha256: String,
    /// Its `notBefore`.
    pub not_before: OffsetDateTime,
}

impl FromRequestParts<Arc<AppState>> for SiteLeaf {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &Arc<AppState>) -> Result<Self, Self::Rejection> {
        let leaf_der = parts
            .extensions
            .get::<PeerLeaf>()
            .and_then(|PeerLeaf(der)| der.clone())
            .ok_or(ApiError::IdentityRequired)?;
        let leaf_pem = certs::cert_pem_from_der(&leaf_der);
        let site_name = certs::leaf_spiffe_id(&leaf_der)
            .as_deref()
            .and_then(certs::site_of_spiffe_id)
            .map(str::to_owned)
            .ok_or(ApiError::IdentityRequired)?;
        if let Err(reason) = certs::verify_site_cert(&state.ca.current().cert_pem, &leaf_pem, &site_name) {
            tracing::warn!(site = %site_name, %reason, "rotation refused: the presented certificate does not verify");
            return Err(ApiError::IdentityRequired);
        }
        let key_sha256 = certs::cert_public_key_sha256(&leaf_pem).map_err(|_bad| ApiError::IdentityRequired)?;
        let (not_before, _not_after) = certs::cert_validity(&leaf_pem).map_err(|_bad| ApiError::IdentityRequired)?;
        Ok(Self {
            site_name,
            key_sha256,
            not_before,
        })
    }
}

/// Renew a site identity with its current certificate: no token, the same name.
#[expect(clippy::too_many_lines, reason = "decide, sign, and audit read as one flow")]
async fn renew(
    State(state): State<Arc<AppState>>,
    leaf: SiteLeaf,
    Json(input): Json<EnrollmentRequest>,
) -> Result<(StatusCode, Json<Enrollment>), ApiError> {
    if !state.renewals_enabled {
        return Err(ApiError::RotationDisabled);
    }
    let renewal = Renewal {
        site_name: leaf.site_name,
        presented_key: leaf.key_sha256,
        requested_key: verify_csr(&input.csr).map_err(signing_error)?,
        presented_not_before: leaf.not_before,
    };
    let validity = Validity::starting_now(state.cert_lifetime);
    let ca = state.ca.current();
    let csr = input.csr;
    let signed = Box::pin(state.store.renew_and_issue(&renewal, || {
        sign_csr(&ca, &renewal.site_name, &csr, validity)
            .map(|cert| Issued {
                certificate: cert.cert_pem,
                spiffe_id: cert.spiffe_id,
                public_key_sha256: cert.public_key_sha256,
            })
            .map_err(|err| StoreError::Backend(format!("signing failed: {err}")))
    }))
    .await;
    let renewed = match signed {
        Ok(signed) => signed,
        Err(StoreError::Refused(reason)) => {
            refused(&renewal, reason);
            return Err(ApiError::IdentityRefused);
        },
        Err(other) => return Err(other.into()),
    };
    let message = match renewed.action {
        RenewAction::Rotate => "site identity rotated",
        RenewAction::Resign => "site identity re-signed for a retried rotation",
    };
    tracing::info!(
        site = %renewal.site_name,
        old_key = %renewed.replaced_key,
        new_key = %renewal.requested_key,
        "{message}"
    );
    let Renewed { id, issued, .. } = renewed;
    Ok((
        StatusCode::OK,
        Json(Enrollment {
            id,
            certificate: issued.certificate,
            ca_certificate: ca.cert_pem.clone(),
            spiffe_id: issued.spiffe_id,
            public_key_sha256: issued.public_key_sha256,
        }),
    ))
}

/// Log a refused renewal; a fork loudly, with its recovery.
#[expect(clippy::cognitive_complexity, reason = "one tracing call per refusal")]
fn refused(renewal: &Renewal, reason: Refusal) {
    let (site, presented_key, requested_key) = (&renewal.site_name, &renewal.presented_key, &renewal.requested_key);
    match reason {
        Refusal::Forked => tracing::warn!(
            site,
            presented_key,
            requested_key,
            "rotation fork: a valid certificate this site's record no longer holds asked for a new key, so two \
             parties hold this identity. Rotation for the site is frozen until a grid-admin deletes its enrollment \
             and it re-enrolls. A holder of a stolen older key can cause this; it fails closed."
        ),
        Refusal::RecordBehind => tracing::warn!(
            site,
            presented_key,
            "rotation refused: the certificate is newer than the site's record, as after the enrollment database \
             was restored. The site re-enrolls."
        ),
        Refusal::Superseded => tracing::warn!(
            site,
            presented_key,
            "rotation refused: the certificate predates the site's current enrollment. If the site was not \
             recovered or re-issued, another party holds an older leaf for it; investigate."
        ),
        Refusal::UnknownSite | Refusal::KeyReused | Refusal::Frozen => {
            tracing::warn!(site, presented_key, reason = reason.as_str(), "rotation refused");
        },
    }
}

/// Read a site's enrollment record: digests and state, never key material.
async fn get_enrollment(
    State(state): State<Arc<AppState>>,
    GridAdmin(_admin): GridAdmin,
    Path(site_name): Path<String>,
) -> Result<Json<EnrollmentStatus>, ApiError> {
    let record = state
        .store
        .enrollment(&site_name)
        .await?
        .ok_or(ApiError::NoEnrollment)?;
    let time = |at: OffsetDateTime| at.format(&Rfc3339).map_err(|err| ApiError::Internal(err.to_string()));
    Ok(Json(EnrollmentStatus {
        id: record.held.id,
        site_name,
        state: if record.held.frozen {
            EnrollmentStatusState::Frozen
        } else {
            EnrollmentStatusState::Active
        },
        public_key_sha256: record.held.current_key,
        previous_public_key_sha256: record.held.previous_key,
        incarnation_started_at: time(record.held.epoch_at)?,
        rotated_at: record.renewed_at.map(time).transpose()?,
        not_after: record.not_after.map(time).transpose()?,
        reserved: record.reserved,
    }))
}

/// Delete a site's enrollment: its renewals end and the name is released to re-enroll.
async fn delete_enrollment(
    State(state): State<Arc<AppState>>,
    GridAdmin(admin): GridAdmin,
    Path(site_name): Path<String>,
) -> Result<StatusCode, ApiError> {
    // A reserved name's record comes from bootstrap: re-issue its identity there instead.
    if state.reserved_sites.contains(&site_name) {
        return Err(ApiError::ReservedSite);
    }
    state.store.delete_enrollment(&site_name).await.map_err(|err| {
        if matches!(err, StoreError::NotFound) {
            ApiError::NoEnrollment
        } else {
            err.into()
        }
    })?;
    tracing::info!(site = %site_name, %admin, "site enrollment deleted; the name may re-enroll");
    Ok(StatusCode::NO_CONTENT)
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
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())?;
    let (scheme, token) = value.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim())
        .filter(|token| !token.is_empty())
}

/// A fresh one-time site token: 256 bits from the system CSPRNG, hex encoded.
fn generate_token() -> Result<String, ApiError> {
    random_hex(32)
}

/// `len` bytes from the system CSPRNG, hex encoded.
///
/// The default build draws from ring's `SystemRandom`. A fips build draws from
/// system openssl so the entropy source stays in the validated module. Hex keeps
/// it header-safe with no padding.
///
/// # Errors
///
/// Returns [`ApiError::Internal`] when the system random source fails.
pub fn random_hex(len: usize) -> Result<String, ApiError> {
    Ok(random_bytes(len)?.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// `len` bytes from the same CSPRNG as [`random_hex`].
///
/// # Errors
///
/// Returns [`ApiError::Internal`] when the system random source fails.
pub fn random_bytes(len: usize) -> Result<Vec<u8>, ApiError> {
    let mut bytes = vec![0_u8; len];
    fill_random(&mut bytes)?;
    Ok(bytes)
}

/// Fill a buffer from ring's system CSPRNG.
#[cfg(not(feature = "fips"))]
fn fill_random(bytes: &mut [u8]) -> Result<(), ApiError> {
    use ring::rand::SecureRandom as _;
    ring::rand::SystemRandom::new()
        .fill(bytes)
        .map_err(|_unspecified| ApiError::Internal("system random source unavailable".to_owned()))
}

/// Fill a buffer from system openssl, keeping the entropy source in the module.
#[cfg(feature = "fips")]
fn fill_random(bytes: &mut [u8]) -> Result<(), ApiError> {
    openssl::rand::rand_bytes(bytes).map_err(|_err| ApiError::Internal("system random source unavailable".to_owned()))
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod error_codes {
    use axum::{http::StatusCode, response::IntoResponse as _};

    use super::{ApiError, ErrorCode};

    /// Every code the API answers with, and its status: the set clients rely on.
    #[test]
    #[expect(clippy::too_many_lines, reason = "one row per wire code")]
    fn the_wire_codes_are_the_documented_set() {
        let bad = |code| ApiError::BadRequest {
            code,
            message: String::new(),
        };
        let answered = [
            bad(ErrorCode::InvalidCsr),
            bad(ErrorCode::InvalidSiteName),
            bad(ErrorCode::InvalidTokenTtl),
            bad(ErrorCode::MissingGridNetwork),
            ApiError::Unauthorized,
            ApiError::InvalidToken,
            ApiError::IdentityRequired,
            ApiError::Forbidden,
            ApiError::IdentityRefused,
            ApiError::NotFound,
            ApiError::NoEnrollment,
            ApiError::NameTaken,
            ApiError::ReservedSite,
            ApiError::Internal(String::new()),
            ApiError::RotationDisabled,
        ]
        .map(|error| {
            let (status, code, _message) = error.rendered();
            (status.as_u16(), serde_json::to_value(code).expect("code"))
        });
        let documented: Vec<(u16, serde_json::Value)> = [
            (400, "invalid_csr"),
            (400, "invalid_site_name"),
            (400, "invalid_token_ttl"),
            (400, "missing_grid_network"),
            (401, "unauthorized"),
            (401, "invalid_token"),
            (401, "identity_required"),
            (403, "forbidden"),
            (403, "identity_refused"),
            (404, "not_found"),
            (404, "not_found"),
            (409, "name_taken"),
            (409, "reserved_site"),
            (500, "internal"),
            (503, "rotation_disabled"),
        ]
        .into_iter()
        .map(|(status, code)| (status, serde_json::json!(code)))
        .collect();
        assert_eq!(answered.to_vec(), documented);
        assert_eq!(serde_json::to_value(ErrorCode::Timeout).expect("code"), "timeout");
    }

    #[test]
    fn a_503_says_when_to_retry() {
        let response = ApiError::RotationDisabled.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().contains_key("retry-after"));
        assert!(
            !ApiError::IdentityRefused
                .into_response()
                .headers()
                .contains_key("retry-after")
        );
    }
}
