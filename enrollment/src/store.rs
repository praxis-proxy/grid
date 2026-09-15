//! Where site tokens and issued enrollments are kept.
//!
//! A backend enum rather than a trait object, so the Postgres backend can be
//! added without every caller becoming generic. A MaaS deployment points this at
//! the Postgres it already runs. A standalone grid brings its own.

use std::{collections::HashMap, sync::Mutex};

use time::OffsetDateTime;
use uuid::Uuid;

pub mod postgres;

pub use postgres::PgStore;

/// Reasons a store operation could not be carried out.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// No token with that identifier.
    #[error("no such site token")]
    NotFound,

    /// Another member already holds this name.
    #[error("site name is already taken")]
    NameTaken,

    /// The presented token is not usable.
    ///
    /// One error for missing, already redeemed, and expired, so a caller cannot
    /// tell which tokens exist or have been used.
    #[error("site token is not valid")]
    TokenInvalid,

    /// The backend itself failed, or issuing the certificate did.
    #[error("store backend failed: {0}")]
    Backend(String),
}

/// A token an operator mints so a site can enroll under a pinned name.
///
/// Only the token digest is held. The caller generates the token, keeps it to
/// hand to the site, and passes its digest here.
#[derive(Debug, Clone)]
pub struct NewSiteToken {
    /// Lowercase hex SHA-256 of the one-time token.
    pub token_sha256: String,

    /// The name the site will enroll under. The operator chooses it here, so the
    /// site never names itself.
    pub site_name: String,

    /// The grid the site will join.
    pub grid_network_ref: String,

    /// The operator who minted the token.
    pub issued_by: String,

    /// When the token stops being usable.
    pub expires_at: OffsetDateTime,
}

/// The pinned name a redeemed token yields. The only source of a member's name.
#[derive(Debug, Clone)]
pub struct Pin {
    /// The name the certificate is signed under.
    pub site_name: String,
}

/// A signed identity, returned inline from enroll and recorded as an audit row.
#[derive(Debug, Clone)]
pub struct Issued {
    /// The issued certificate, PEM encoded.
    pub certificate: String,

    /// The name bound into the certificate, as a SPIFFE URI.
    pub spiffe_id: String,

    /// Lowercase hex SHA-256 over the request's public key.
    pub public_key_sha256: String,
}

/// Where tokens and enrollments are kept.
#[derive(Debug)]
pub enum Store {
    /// Held in this process. Suits a standalone grid and the tests.
    ///
    /// Everything is lost on restart, and two replicas share nothing, so this is
    /// not a deployment a grid should depend on. Boxed so its several maps do not
    /// make this the large variant next to the pooled Postgres handle.
    Memory(Box<MemoryStore>),

    /// Held in Postgres. A MaaS deployment already runs one.
    Postgres(PgStore),
}

impl Store {
    /// A store that keeps records in this process.
    #[must_use]
    pub fn memory() -> Self {
        Self::Memory(Box::default())
    }

    /// A store backed by Postgres, with the schema applied.
    ///
    /// This connects with whatever the URL specifies. The TLS posture of a
    /// production URL is enforced by the caller before this point (the binary
    /// refuses a plaintext-capable sslmode at startup), so tests can still point
    /// this at a local plaintext database.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] if the database is unreachable or the
    /// schema cannot be applied.
    pub async fn postgres(url: &str) -> Result<Self, StoreError> {
        Ok(Self::Postgres(PgStore::connect(url).await?))
    }

    /// Mint a site token, returning its identifier.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] if the backend failed.
    pub async fn mint_site_token(&self, token: NewSiteToken) -> Result<Uuid, StoreError> {
        match self {
            Self::Memory(store) => store.mint_site_token(token),
            Self::Postgres(store) => store.mint_site_token(token).await,
        }
    }

    /// Revoke a site token before it is redeemed.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] if no token has that identifier.
    pub async fn revoke_site_token(&self, token_id: Uuid) -> Result<(), StoreError> {
        match self {
            Self::Memory(store) => store.revoke_site_token(token_id),
            Self::Postgres(store) => store.revoke_site_token(token_id).await,
        }
    }

    /// Whether a token with this digest is present, unredeemed, and unexpired,
    /// without consuming it.
    ///
    /// A cheap precheck so an invalid token is refused before any CSR crypto
    /// runs. The authoritative one-shot consume is [`Self::redeem_and_issue`],
    /// whose guard still catches a redemption that raced this check.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] if the backend failed.
    pub async fn token_valid(&self, token_sha256: &str) -> Result<bool, StoreError> {
        match self {
            Self::Memory(store) => store.token_valid(token_sha256),
            Self::Postgres(store) => store.token_valid(token_sha256).await,
        }
    }

    /// Redeem a token by digest and issue the certificate, as one step.
    ///
    /// The token is consumed and the certificate signed and recorded in a single
    /// transaction, so the token is spent if and only if the certificate is
    /// issued. `sign` receives the pinned name and returns the signed identity.
    /// If it fails, the transaction rolls back and the token stays unspent. The
    /// guarded consume is the authoritative gate against a double redemption. The
    /// returned identifier is the issued-enrollment row's.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::TokenInvalid`] if the token is missing, already
    /// redeemed, or expired, [`StoreError::NameTaken`] if an issued member
    /// already holds the pinned name, and [`StoreError::Backend`] if signing or
    /// the backend failed.
    pub async fn redeem_and_issue<F>(&self, token_sha256: &str, sign: F) -> Result<(Uuid, Issued), StoreError>
    where
        F: FnOnce(&Pin) -> Result<Issued, StoreError> + Send,
    {
        match self {
            Self::Memory(store) => store.redeem_and_issue(token_sha256, sign),
            Self::Postgres(store) => store.redeem_and_issue(token_sha256, sign).await,
        }
    }
}

/// Records held in this process.
#[derive(Debug, Default)]
pub struct MemoryStore {
    /// One lock over all state, so there is no lock order to get wrong.
    inner: Mutex<Inner>,
}

/// The tokens and the names already issued.
#[derive(Debug, Default)]
struct Inner {
    /// Outstanding and spent tokens, by identifier.
    tokens: HashMap<Uuid, TokenRow>,

    /// Token digest to identifier, so a lookup by digest is O(1) rather than a
    /// scan, matching the shape of the Postgres backend's indexed lookup (not its
    /// unique constraint). Kept in step with `tokens` on mint and revoke.
    by_digest: HashMap<String, Uuid>,

    /// Names already issued, to the enrollment that holds each.
    issued_names: HashMap<String, Uuid>,
}

/// One site token held in memory.
#[derive(Debug, Clone)]
struct TokenRow {
    /// Lowercase hex SHA-256 of the token.
    token_sha256: String,
    /// The pinned name.
    site_name: String,
    /// When the token stops being usable.
    expires_at: OffsetDateTime,
    /// The enrollment that spent it, once redeemed.
    redeemed_by: Option<Uuid>,
}

impl MemoryStore {
    /// Record a token.
    fn mint_site_token(&self, token: NewSiteToken) -> Result<Uuid, StoreError> {
        let mut inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        let token_id = Uuid::new_v4();
        inner.by_digest.insert(token.token_sha256.clone(), token_id);
        inner.tokens.insert(
            token_id,
            TokenRow {
                token_sha256: token.token_sha256,
                site_name: token.site_name,
                expires_at: token.expires_at,
                redeemed_by: None,
            },
        );
        drop(inner);
        Ok(token_id)
    }

    /// Revoke a token that has not been redeemed.
    ///
    /// A redeemed token's row stays as issuance provenance, matching the Postgres
    /// backend, so an already-redeemed token is reported as not found.
    fn revoke_site_token(&self, token_id: Uuid) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        let outstanding = inner.tokens.get(&token_id).is_some_and(|row| row.redeemed_by.is_none());
        if outstanding && let Some(row) = inner.tokens.remove(&token_id) {
            inner.by_digest.remove(&row.token_sha256);
        }
        drop(inner);
        outstanding.then_some(()).ok_or(StoreError::NotFound)
    }

    /// Whether a usable token has this digest, without consuming it.
    fn token_valid(&self, token_sha256: &str) -> Result<bool, StoreError> {
        let inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        let now = OffsetDateTime::now_utc();
        Ok(inner
            .by_digest
            .get(token_sha256)
            .and_then(|id| inner.tokens.get(id))
            .is_some_and(|row| row.redeemed_by.is_none() && row.expires_at > now))
    }

    /// Redeem a token by digest, sign under its pin, and record the enrollment.
    ///
    /// One lock covers the guard, the sign, and the record, so the token is spent
    /// only when the certificate is issued and a name check cannot race a record.
    fn redeem_and_issue<F>(&self, token_sha256: &str, sign: F) -> Result<(Uuid, Issued), StoreError>
    where
        F: FnOnce(&Pin) -> Result<Issued, StoreError>,
    {
        let mut inner = self.inner.lock().map_err(|_poisoned| poisoned())?;

        let now = OffsetDateTime::now_utc();
        let token_id = inner
            .by_digest
            .get(token_sha256)
            .copied()
            .ok_or(StoreError::TokenInvalid)?;
        let pin = inner
            .tokens
            .get(&token_id)
            .filter(|row| row.redeemed_by.is_none() && row.expires_at > now)
            .map(|row| Pin {
                site_name: row.site_name.clone(),
            })
            .ok_or(StoreError::TokenInvalid)?;

        if inner.issued_names.contains_key(&pin.site_name) {
            return Err(StoreError::NameTaken);
        }

        // Sign before consuming, so a signing failure leaves the token unspent.
        let issued = sign(&pin)?;

        let enrollment_id = Uuid::new_v4();
        if let Some(row) = inner.tokens.get_mut(&token_id) {
            row.redeemed_by = Some(enrollment_id);
        }
        inner.issued_names.insert(pin.site_name, enrollment_id);
        drop(inner);

        Ok((enrollment_id, issued))
    }
}

/// A poisoned lock means another thread panicked holding it.
fn poisoned() -> StoreError {
    StoreError::Backend("in-memory store lock was poisoned".to_owned())
}
