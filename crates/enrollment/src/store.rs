//! Where site tokens and issued enrollments are kept.
//!
//! A backend enum rather than a trait object, so the Postgres backend can be
//! added without every caller becoming generic. A MaaS deployment points this at
//! the Postgres it already runs. A standalone grid brings its own.

use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use time::OffsetDateTime;
use uuid::Uuid;

#[cfg(test)]
mod lifecycle_model;
pub mod postgres;
pub mod renewal;

pub use postgres::PgStore;
pub use renewal::{Held, Refusal, RenewAction, Renewal, Renewed, SeedRecord, Seeded};

/// Reasons a store operation could not be carried out.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// No token with that identifier.
    #[error("no such site token")]
    NotFound,

    /// Another member already holds this name.
    #[error("site name is already taken")]
    NameTaken,

    /// A live token already pins this name.
    #[error("a live site token already pins this site name")]
    TokenOutstanding,

    /// The name's enrollment was deleted, and the token does not opt in to it.
    #[error("site name was deleted")]
    NameDeleted,

    /// The key was enrolled before, under this or another name.
    #[error("key was enrolled before")]
    KeyReused,

    /// The invite is bound to a different public key.
    #[error("the public key does not match the invite")]
    KeyMismatch,

    /// The presented token is not usable.
    ///
    /// One error for missing, already redeemed, and expired, so a caller cannot
    /// tell which tokens exist or have been used.
    #[error("site token is not valid")]
    TokenInvalid,

    /// The enrollment record does not admit this renewal.
    #[error("rotation refused: {0}")]
    Refused(#[from] Refusal),

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
    /// Identifier selected by a Kubernetes reconciler so it can persist the
    /// token Secret before recording the digest. Ordinary API mints leave this
    /// unset and receive a fresh identifier.
    pub token_id: Option<Uuid>,

    /// Lowercase hex SHA-256 of the one-time token.
    pub token_sha256: String,

    /// Optional lowercase SHA-256 of the public key allowed to redeem this token.
    pub public_key_sha256: Option<String>,

    /// The name the site will enroll under. The operator chooses it here, so the
    /// site never names itself.
    pub site_name: String,

    /// The grid the site will join.
    pub grid_network_ref: String,

    /// The operator who minted the token.
    pub issued_by: String,

    /// When the token stops being usable.
    pub expires_at: OffsetDateTime,

    /// Whether the token may enroll a name whose enrollment was deleted.
    pub allow_deleted_name: bool,
}

/// The pinned name a redeemed token yields. The only source of a member's name.
#[derive(Debug, Clone)]
pub struct Pin {
    /// The name the certificate is signed under.
    pub site_name: String,
    /// Optional public key constraint carried by the token.
    pub public_key_sha256: Option<String>,
}

/// A signed identity, returned inline from enroll and recorded as an audit row.
#[derive(Debug, Clone)]
pub struct Issued {
    /// The site name bound into the certificate.
    pub site_name: String,

    /// The issued certificate, PEM encoded.
    pub certificate: String,

    /// The name bound into the certificate, as a SPIFFE URI.
    pub spiffe_id: String,

    /// Lowercase hex SHA-256 over the request's public key.
    pub public_key_sha256: String,
}

impl Issued {
    /// When the issued certificate expires.
    fn not_after(&self) -> Option<OffsetDateTime> {
        certs::cert_validity(&self.certificate).ok().map(|(_from, until)| until)
    }
}

/// A name's enrollment record, as a grid-admin reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrollmentRecord {
    /// The record renewal decides on.
    pub held: Held,
    /// A name bootstrap issues.
    pub reserved: bool,
    /// When the record last took a new key, by renewal or a bootstrap seed.
    pub renewed_at: Option<OffsetDateTime>,
    /// When the latest certificate the service issued for it expires.
    pub not_after: Option<OffsetDateTime>,
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
    /// This connects with whatever the URL specifies. The binary refuses any
    /// sslmode short of verify-full before this point, so tests can still point
    /// this at a local plaintext database.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] if the database is unreachable or the
    /// schema cannot be applied.
    pub async fn postgres(url: &str) -> Result<Self, StoreError> {
        Ok(Self::Postgres(Box::pin(PgStore::connect(url)).await?))
    }

    /// Mint a site token, returning its identifier.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NameTaken`] if an enrollment holds the name,
    /// [`StoreError::NameDeleted`] if the name's enrollment was deleted and the
    /// token does not opt in, [`StoreError::TokenOutstanding`] if an unredeemed,
    /// unexpired token already pins it, and [`StoreError::Backend`] if the backend
    /// failed.
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

    /// Read the enrollment ID that redeemed `token_id`, if it is still outstanding.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] when the token is absent.
    pub async fn redeemed_enrollment(&self, token_id: Uuid) -> Result<Option<Uuid>, StoreError> {
        match self {
            Self::Memory(store) => store.redeemed_enrollment(token_id),
            Self::Postgres(store) => store.redeemed_enrollment(token_id).await,
        }
    }

    /// Whether the backend is reachable, for the readiness probe. The in-memory
    /// store is always ready, and the Postgres store pings its pool.
    pub async fn ready(&self) -> bool {
        match self {
            Self::Memory(_) => true,
            Self::Postgres(store) => store.ready().await,
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
    /// A token refused for a deleted name is revoked. A taken name or a reused key
    /// leaves it unspent.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::TokenInvalid`] if the token is missing, already
    /// redeemed, or expired, [`StoreError::NameTaken`] if an issued member
    /// already holds the pinned name, [`StoreError::NameDeleted`] if the name was
    /// deleted and the token does not opt in, [`StoreError::KeyReused`] if the key
    /// was ever enrolled, and [`StoreError::Backend`] if signing or the backend failed.
    pub async fn redeem_and_issue<F>(&self, token_sha256: &str, sign: F) -> Result<(Uuid, Issued), StoreError>
    where
        F: FnOnce(&Pin) -> Result<Issued, StoreError> + Send,
    {
        match self {
            Self::Memory(store) => store.redeem_and_issue(token_sha256, sign),
            Self::Postgres(store) => store.redeem_and_issue(token_sha256, sign).await,
        }
    }

    /// Admit a renewal against the name's record, sign, and record the new key, as one step.
    ///
    /// A fork freezes the record before it is refused, and a new key ever enrolled is refused.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Refused`] when the record does not admit it, and
    /// [`StoreError::Backend`] if signing or the backend failed.
    pub async fn renew_and_issue<F>(&self, renewal: &Renewal, sign: F) -> Result<Renewed, StoreError>
    where
        F: FnOnce() -> Result<Issued, StoreError> + Send,
    {
        match self {
            Self::Memory(store) => store.renew_and_issue(renewal, sign),
            Self::Postgres(store) => store.renew_and_issue(renewal, sign).await,
        }
    }

    /// Apply a verified bootstrap seed to a reserved name's record.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] if the backend failed.
    pub async fn seed_reserved(&self, seed: &SeedRecord) -> Result<Seeded, StoreError> {
        match self {
            Self::Memory(store) => store.seed_reserved(seed),
            Self::Postgres(store) => store.seed_reserved(seed).await,
        }
    }

    /// Read a name's enrollment record, `None` when no record holds it.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] if the backend failed.
    pub async fn enrollment(&self, site_name: &str) -> Result<Option<EnrollmentRecord>, StoreError> {
        match self {
            Self::Memory(store) => store.enrollment(site_name),
            Self::Postgres(store) => store.enrollment(site_name).await,
        }
    }

    /// The fingerprint of the grid CA these records belong to, `None` before one is recorded.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] if the backend failed.
    pub async fn recorded_ca(&self) -> Result<Option<String>, StoreError> {
        match self {
            Self::Memory(store) => store.recorded_ca(),
            Self::Postgres(store) => store.recorded_ca().await,
        }
    }

    /// Record `fingerprint` as the grid CA when `expected` is the one recorded now,
    /// `None` meaning none is. Returns whether it was recorded.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] if the backend failed.
    pub async fn swap_recorded_ca(&self, expected: Option<&str>, fingerprint: &str) -> Result<bool, StoreError> {
        match self {
            Self::Memory(store) => store.swap_recorded_ca(expected, fingerprint),
            Self::Postgres(store) => store.swap_recorded_ca(expected, fingerprint).await,
        }
    }

    /// Delete a name's enrollment record, ending its renewals.
    ///
    /// The name is recorded as deleted and its keys stay enrolled, so neither is reused by default.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] if no record holds the name.
    pub async fn delete_enrollment(&self, site_name: &str) -> Result<(), StoreError> {
        match self {
            Self::Memory(store) => store.delete_enrollment(site_name),
            Self::Postgres(store) => store.delete_enrollment(site_name).await,
        }
    }
}

/// Records held in this process.
#[derive(Debug, Default)]
pub struct MemoryStore {
    /// One lock over all state, so there is no lock order to get wrong.
    inner: Mutex<Inner>,
    /// Seconds a test moves the clock forward.
    #[cfg(test)]
    offset: std::sync::atomic::AtomicI64,
}

/// The tokens and the names already issued.
#[derive(Debug, Default, Clone)]
struct Inner {
    /// Outstanding and spent tokens, by identifier.
    tokens: HashMap<Uuid, TokenRow>,

    /// Token digest to identifier, so a lookup by digest is O(1) rather than a
    /// scan, matching the shape of the Postgres backend's indexed lookup (not its
    /// unique constraint). Kept in step with `tokens` on mint and revoke.
    by_digest: HashMap<String, Uuid>,

    /// Names already issued, to the record renewal reads and whether the name is reserved.
    issued_names: HashMap<String, (Held, bool)>,

    /// When the latest certificate issued for a name expires.
    not_after: HashMap<String, OffsetDateTime>,

    /// Every key ever enrolled, never removed.
    enrolled_keys: HashSet<String>,

    /// Names whose enrollment was deleted.
    deleted_names: HashSet<String>,

    /// The fingerprint of the grid CA these records belong to.
    ca_fingerprint: Option<String>,
}

impl Inner {
    /// Drop a token, as revoke does.
    fn forget(&mut self, token_id: Uuid) {
        if let Some(row) = self.tokens.remove(&token_id) {
            self.by_digest.remove(&row.token_sha256);
        }
    }
}

/// One site token held in memory.
#[derive(Debug, Clone)]
struct TokenRow {
    /// Lowercase hex SHA-256 of the token.
    token_sha256: String,
    /// Optional public key constraint carried by the token.
    public_key_sha256: Option<String>,
    /// The pinned name.
    site_name: String,
    /// The grid the token admits the site to.
    grid_network_ref: String,
    /// The operator that issued the token.
    issued_by: String,
    /// When the token stops being usable.
    expires_at: OffsetDateTime,
    /// Whether it may enroll a deleted name.
    allow_deleted_name: bool,
    /// The enrollment that spent it, once redeemed.
    redeemed_by: Option<Uuid>,
}

impl MemoryStore {
    /// The store's clock.
    #[cfg_attr(not(test), expect(clippy::unused_self, reason = "a test moves this clock"))]
    fn now(&self) -> OffsetDateTime {
        let now = OffsetDateTime::now_utc();
        #[cfg(test)]
        let now = now.saturating_add(time::Duration::seconds(
            self.offset.load(std::sync::atomic::Ordering::Relaxed),
        ));
        now
    }

    /// Move the clock forward.
    #[cfg(test)]
    fn advance(&self, by: time::Duration) {
        self.offset
            .fetch_add(by.whole_seconds(), std::sync::atomic::Ordering::Relaxed);
    }

    /// The whole state, for a test that restores it.
    #[cfg(test)]
    fn snapshot(&self) -> Inner {
        self.inner.lock().map(|inner| inner.clone()).unwrap_or_default()
    }

    /// Put back a state [`Self::snapshot`] took.
    #[cfg(test)]
    fn restore(&self, state: Inner) {
        if let Ok(mut inner) = self.inner.lock() {
            *inner = state;
        }
    }

    /// Record a token, unless the name is held, deleted without opt-in, or pinned by a live token.
    #[expect(
        clippy::too_many_lines,
        reason = "idempotent retries and name guards must stay under one lock"
    )]
    fn mint_site_token(&self, token: NewSiteToken) -> Result<Uuid, StoreError> {
        let mut inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        let token_id = token.token_id.unwrap_or_else(Uuid::new_v4);
        if let Some(existing) = inner.tokens.get(&token_id) {
            return (existing.token_sha256 == token.token_sha256
                && existing.site_name == token.site_name
                && existing.grid_network_ref == token.grid_network_ref
                && existing.issued_by == token.issued_by
                && existing.public_key_sha256 == token.public_key_sha256
                && existing.expires_at == token.expires_at
                && existing.allow_deleted_name == token.allow_deleted_name)
                .then_some(token_id)
                .ok_or_else(|| StoreError::Backend("site token id is already in use".to_owned()));
        }
        if inner.issued_names.contains_key(&token.site_name) {
            return Err(StoreError::NameTaken);
        }
        if !token.allow_deleted_name && inner.deleted_names.contains(&token.site_name) {
            return Err(StoreError::NameDeleted);
        }
        let now = self.now();
        if inner
            .tokens
            .values()
            .any(|row| row.site_name == token.site_name && row.redeemed_by.is_none() && row.expires_at > now)
        {
            return Err(StoreError::TokenOutstanding);
        }
        if inner.by_digest.contains_key(&token.token_sha256) {
            return Err(StoreError::Backend("site token digest is already in use".to_owned()));
        }
        inner.by_digest.insert(token.token_sha256.clone(), token_id);
        inner.tokens.insert(
            token_id,
            TokenRow {
                token_sha256: token.token_sha256,
                site_name: token.site_name,
                grid_network_ref: token.grid_network_ref,
                issued_by: token.issued_by,
                public_key_sha256: token.public_key_sha256,
                expires_at: token.expires_at,
                allow_deleted_name: token.allow_deleted_name,
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
        if outstanding {
            inner.forget(token_id);
        }
        drop(inner);
        outstanding.then_some(()).ok_or(StoreError::NotFound)
    }

    /// Whether a usable token has this digest, without consuming it.
    fn token_valid(&self, token_sha256: &str) -> Result<bool, StoreError> {
        let inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        let now = self.now();
        Ok(inner
            .by_digest
            .get(token_sha256)
            .and_then(|id| inner.tokens.get(id))
            .is_some_and(|row| row.redeemed_by.is_none() && row.expires_at > now))
    }

    /// Return the enrollment that spent this token, if any.
    fn redeemed_enrollment(&self, token_id: Uuid) -> Result<Option<Uuid>, StoreError> {
        let inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        inner
            .tokens
            .get(&token_id)
            .map(|row| row.redeemed_by)
            .ok_or(StoreError::NotFound)
    }

    /// Redeem a token by digest, sign under its pin, and record the enrollment.
    ///
    /// One lock covers the guard, the sign, and the record, so the token is spent
    /// only when the certificate is issued and a name check cannot race a record.
    #[expect(
        clippy::too_many_lines,
        reason = "the guard, sign, and record read as one step under the lock"
    )]
    fn redeem_and_issue<F>(&self, token_sha256: &str, sign: F) -> Result<(Uuid, Issued), StoreError>
    where
        F: FnOnce(&Pin) -> Result<Issued, StoreError>,
    {
        let mut inner = self.inner.lock().map_err(|_poisoned| poisoned())?;

        let now = self.now();
        let token_id = inner
            .by_digest
            .get(token_sha256)
            .copied()
            .ok_or(StoreError::TokenInvalid)?;
        let (pin, allow_deleted_name) = inner
            .tokens
            .get(&token_id)
            .filter(|row| row.redeemed_by.is_none() && row.expires_at > now)
            .map(|row| {
                let pin = Pin {
                    site_name: row.site_name.clone(),
                    public_key_sha256: row.public_key_sha256.clone(),
                };
                (pin, row.allow_deleted_name)
            })
            .ok_or(StoreError::TokenInvalid)?;

        // Sign before consuming, so a signing failure leaves the token unspent.
        let signed = if inner.issued_names.contains_key(&pin.site_name) {
            Err(StoreError::NameTaken)
        } else if !allow_deleted_name && inner.deleted_names.contains(&pin.site_name) {
            Err(StoreError::NameDeleted)
        } else {
            sign(&pin)
        };
        let issued = match signed {
            Err(lost @ StoreError::NameDeleted) => {
                inner.forget(token_id);
                return Err(lost);
            },
            signed => signed?,
        };
        if !inner.enrolled_keys.insert(issued.public_key_sha256.clone()) {
            return Err(StoreError::KeyReused);
        }

        let enrollment_id = Uuid::new_v4();
        if let Some(row) = inner.tokens.get_mut(&token_id) {
            row.redeemed_by = Some(enrollment_id);
        }
        let held = Held::new(enrollment_id, issued.public_key_sha256.clone(), None, now);
        if let Some(until) = issued.not_after() {
            inner.not_after.insert(pin.site_name.clone(), until);
        }
        inner.issued_names.insert(pin.site_name, (held, false));
        drop(inner);

        Ok((enrollment_id, issued))
    }

    /// Admit a renewal, sign, and record the new key under one lock.
    #[expect(
        clippy::too_many_lines,
        reason = "the decide, sign, and record read as one step under the lock"
    )]
    fn renew_and_issue<F>(&self, renewal: &Renewal, sign: F) -> Result<Renewed, StoreError>
    where
        F: FnOnce() -> Result<Issued, StoreError>,
    {
        let mut guard = self.inner.lock().map_err(|_poisoned| poisoned())?;
        let inner = &mut *guard;
        let Some((held, _reserved)) = inner.issued_names.get_mut(&renewal.site_name) else {
            return Err(Refusal::UnknownSite.into());
        };
        let action = match renewal::decide(Some(held), renewal) {
            Err(Refusal::Forked) => {
                held.frozen = true;
                return Err(Refusal::Forked.into());
            },
            decided => decided?,
        };
        if action == RenewAction::Rotate && inner.enrolled_keys.contains(&renewal.requested_key) {
            return Err(Refusal::KeyReused.into());
        }
        let replaced_key = held.current_key.clone();
        let issued = sign()?;
        if action == RenewAction::Rotate {
            inner.enrolled_keys.insert(renewal.requested_key.clone());
            held.previous_key = Some(std::mem::replace(&mut held.current_key, renewal.requested_key.clone()));
            held.recorded_at = self.now();
        }
        let id = held.id;
        if let Some(until) = issued.not_after() {
            inner.not_after.insert(renewal.site_name.clone(), until);
        }
        drop(guard);
        Ok(Renewed {
            id,
            action,
            replaced_key,
            issued,
        })
    }

    /// Apply a seed under the lock.
    fn seed_reserved(&self, seed: &SeedRecord) -> Result<Seeded, StoreError> {
        let mut inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        let entry = inner.issued_names.get(&seed.site_name);
        let seeded = renewal::seed_decision(entry.map(|(held, reserved)| (held, *reserved)), seed);
        let id = entry.map_or_else(Uuid::new_v4, |(held, _)| held.id);
        if seeded == Seeded::Acknowledged
            && let Some((held, _reserved)) = inner.issued_names.get_mut(&seed.site_name)
        {
            held.seed_generation = Some(seed.generation);
        }
        if matches!(seeded, Seeded::Registered | Seeded::Reset { .. }) {
            let held = Held {
                seed_generation: Some(seed.generation),
                epoch_at: seed.issued_at,
                ..Held::new(id, seed.key_sha256.clone(), None, self.now())
            };
            inner.issued_names.insert(seed.site_name.clone(), (held, true));
            inner.not_after.remove(&seed.site_name);
            inner.enrolled_keys.insert(seed.key_sha256.clone());
        }
        drop(inner);
        Ok(seeded)
    }

    /// Read a record under the lock.
    fn enrollment(&self, site_name: &str) -> Result<Option<EnrollmentRecord>, StoreError> {
        let inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        Ok(inner
            .issued_names
            .get(site_name)
            .map(|(held, reserved)| EnrollmentRecord {
                renewed_at: (held.recorded_at > held.epoch_at).then_some(held.recorded_at),
                not_after: inner.not_after.get(site_name).copied(),
                held: held.clone(),
                reserved: *reserved,
            }))
    }

    /// Read the recorded CA under the lock.
    fn recorded_ca(&self) -> Result<Option<String>, StoreError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_poisoned| poisoned())?
            .ca_fingerprint
            .clone())
    }

    /// Swap the recorded CA under the lock when `expected` is current.
    fn swap_recorded_ca(&self, expected: Option<&str>, fingerprint: &str) -> Result<bool, StoreError> {
        let mut inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        let swapped = inner.ca_fingerprint.as_deref() == expected;
        if swapped {
            inner.ca_fingerprint = Some(fingerprint.to_owned());
        }
        drop(inner);
        Ok(swapped)
    }

    /// Delete a record under the lock.
    fn delete_enrollment(&self, site_name: &str) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        let removed = inner.issued_names.remove(site_name);
        inner.not_after.remove(site_name);
        if removed.is_some() {
            inner.deleted_names.insert(site_name.to_owned());
        }
        drop(inner);
        removed.map(drop).ok_or(StoreError::NotFound)
    }
}

/// A poisoned lock means another thread panicked while holding it.
fn poisoned() -> StoreError {
    StoreError::Backend("in-memory store lock was poisoned".to_owned())
}

#[cfg(test)]
mod token_id_tests {
    use time::OffsetDateTime;
    use uuid::Uuid;

    use super::{MemoryStore, NewSiteToken, StoreError};

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "the retry test checks all fields that define token identity"
    )]
    fn token_id_mint_is_retryable_only_for_the_same_token() -> Result<(), StoreError> {
        let store = MemoryStore::default();
        let token_id = Uuid::new_v4();
        let token = NewSiteToken {
            token_id: Some(token_id),
            token_sha256: "digest".to_owned(),
            public_key_sha256: None,
            site_name: "west".to_owned(),
            grid_network_ref: "site-invite".to_owned(),
            issued_by: "siteinvite/test".to_owned(),
            expires_at: OffsetDateTime::now_utc().saturating_add(time::Duration::hours(1)),
            allow_deleted_name: false,
        };
        assert_eq!(store.mint_site_token(token.clone())?, token_id);
        assert_eq!(store.mint_site_token(token.clone())?, token_id);
        for changed in [
            NewSiteToken {
                site_name: "east".to_owned(),
                ..token.clone()
            },
            NewSiteToken {
                grid_network_ref: "other-grid".to_owned(),
                ..token.clone()
            },
            NewSiteToken {
                issued_by: "other-operator".to_owned(),
                ..token
            },
        ] {
            assert!(matches!(store.mint_site_token(changed), Err(StoreError::Backend(_))));
        }
        Ok(())
    }

    #[test]
    fn a_bound_key_mismatch_keeps_the_token_available() -> Result<(), StoreError> {
        let store = MemoryStore::default();
        store.mint_site_token(NewSiteToken {
            token_id: Some(Uuid::new_v4()),
            token_sha256: "bound-token".to_owned(),
            public_key_sha256: Some("expected-key".to_owned()),
            site_name: "west".to_owned(),
            grid_network_ref: "site-invite".to_owned(),
            issued_by: "siteinvite/test".to_owned(),
            expires_at: OffsetDateTime::now_utc().saturating_add(time::Duration::hours(1)),
            allow_deleted_name: false,
        })?;
        let result = store.redeem_and_issue("bound-token", |pin| {
            if pin.public_key_sha256.as_deref() != Some("presented-key") {
                return Err(StoreError::KeyMismatch);
            }
            Err(StoreError::Backend("unexpected signing path".to_owned()))
        });
        assert!(matches!(result, Err(StoreError::KeyMismatch)));
        assert!(store.token_valid("bound-token")?);
        Ok(())
    }
}
