//! Records kept in Postgres.
//!
//! A MaaS deployment already runs one, so this points at that. The one-shot
//! guarantee the in-process backend enforces by holding a lock is enforced here
//! by the schema and a guarded update, because two services sharing a database
//! cannot hold each other's locks.

use sqlx::{PgPool, Row as _};
use uuid::Uuid;

use super::{
    Held, Issued, NewSiteToken, Pin, Refusal, RenewAction, Renewal, Renewed, SeedRecord, Seeded, StoreError, renewal,
};

/// The site-token table.
static SCHEMA_TOKENS: &str = include_str!("../../db/schema/0001_create_site_tokens.up.sql");
/// The issued-enrollment audit table.
static SCHEMA_ENROLLMENTS: &str = include_str!("../../db/schema/0002_create_site_enrollments.up.sql");
/// Renewal columns on the enrollment table.
static SCHEMA_RENEWAL: &str = include_str!("../../db/schema/0003_site_enrollment_renewal.up.sql");
/// Advisory-lock key that serializes schema application across instances.
const SCHEMA_LOCK_KEY: i64 = 0x671D_E401;

/// Records held in Postgres.
#[derive(Debug, Clone)]
pub struct PgStore {
    /// Connection pool.
    pool: PgPool,
}

impl PgStore {
    /// Connect and make sure the schema is present.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] if the database is unreachable or the
    /// schema cannot be applied.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = Box::pin(PgPool::connect(url)).await.map_err(backend)?;
        // Serialize schema application across instances that start at once. Two
        // connections running the DDL concurrently race on a table's implicit
        // type, which no IF NOT EXISTS prevents. An advisory lock, released when
        // the transaction commits, makes the guarded creates effective.
        let mut tx = Box::pin(pool.begin()).await.map_err(backend)?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(SCHEMA_LOCK_KEY)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        sqlx::raw_sql(SCHEMA_TOKENS).execute(&mut *tx).await.map_err(backend)?;
        sqlx::raw_sql(SCHEMA_ENROLLMENTS)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        sqlx::raw_sql(SCHEMA_RENEWAL).execute(&mut *tx).await.map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(Self { pool })
    }

    /// Record a token.
    pub(super) async fn mint_site_token(&self, token: NewSiteToken) -> Result<Uuid, StoreError> {
        let token_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO site_tokens
                 (id, token_sha256, site_name, grid_network_ref, issued_by, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(token_id)
        .bind(&token.token_sha256)
        .bind(&token.site_name)
        .bind(&token.grid_network_ref)
        .bind(&token.issued_by)
        .bind(token.expires_at)
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(token_id)
    }

    /// Revoke a token that has not been redeemed.
    ///
    /// A redeemed token's row is left in place: it is issuance provenance, and
    /// deleting it would neither revoke the certificate nor be recoverable. A
    /// zero-row result means no such outstanding token, reported as not found.
    pub(super) async fn revoke_site_token(&self, token_id: Uuid) -> Result<(), StoreError> {
        let result = sqlx::query("DELETE FROM site_tokens WHERE id = $1 AND redeemed_at IS NULL")
            .bind(token_id)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        if result.rows_affected() == 0 {
            return Err(StoreError::NotFound);
        }
        Ok(())
    }

    /// Whether a usable token has this digest, without consuming it.
    pub(super) async fn token_valid(&self, token_sha256: &str) -> Result<bool, StoreError> {
        let row = sqlx::query(
            "SELECT 1 AS ok FROM site_tokens
              WHERE token_sha256 = $1 AND redeemed_at IS NULL AND expires_at > NOW()",
        )
        .bind(token_sha256)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        Ok(row.is_some())
    }

    /// Redeem a token by digest, sign under its pin, and record the enrollment.
    ///
    /// One transaction covers the guarded consume, the sign, and the audit
    /// insert. A failing sign or a name conflict rolls it back, so the token is
    /// spent only when the certificate is issued. The guarded update is the row
    /// lock that serializes concurrent redemptions of one token.
    #[expect(
        clippy::too_many_lines,
        reason = "the consume, sign, and audit insert read as one transaction"
    )]
    pub(super) async fn redeem_and_issue<F>(&self, token_sha256: &str, sign: F) -> Result<(Uuid, Issued), StoreError>
    where
        F: FnOnce(&Pin) -> Result<Issued, StoreError> + Send,
    {
        let enrollment_id = Uuid::new_v4();
        let mut tx = Box::pin(self.pool.begin()).await.map_err(backend)?;

        let consumed = sqlx::query(
            "UPDATE site_tokens
                SET redeemed_at = NOW(), redeemed_by = $2
              WHERE token_sha256 = $1 AND redeemed_at IS NULL AND expires_at > NOW()
              RETURNING id, site_name",
        )
        .bind(token_sha256)
        .bind(enrollment_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?
        .ok_or(StoreError::TokenInvalid)?;
        let token_id: Uuid = consumed.try_get("id").map_err(backend)?;
        let pin = Pin {
            site_name: consumed.try_get("site_name").map_err(backend)?,
        };

        // Sign inside the transaction. A failure returns here, and the dropped
        // transaction rolls the consume back, so the token is not spent.
        let issued = sign(&pin)?;

        let inserted = sqlx::query(
            "INSERT INTO site_enrollments (id, site_token_id, site_name, public_key_sha256, spiffe_id, not_after)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(enrollment_id)
        .bind(token_id)
        .bind(&pin.site_name)
        .bind(&issued.public_key_sha256)
        .bind(&issued.spiffe_id)
        .bind(issued.not_after())
        .execute(&mut *tx)
        .await;
        match inserted {
            Ok(_done) => {},
            Err(err) if is_unique_violation(&err) => return Err(StoreError::NameTaken),
            Err(err) => return Err(backend(err)),
        }

        tx.commit().await.map_err(backend)?;
        Ok((enrollment_id, issued))
    }

    /// The name's record, locked for this transaction, with whether it is reserved.
    async fn locked(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        site_name: &str,
    ) -> Result<Option<(Held, bool)>, StoreError> {
        let row = sqlx::query(
            "SELECT id, public_key_sha256, previous_public_key_sha256, frozen_at IS NOT NULL AS frozen,
                    reserved, seed_generation, epoch_at, COALESCE(renewed_at, issued_at) AS recorded_at
               FROM site_enrollments
              WHERE site_name = $1
                FOR UPDATE",
        )
        .bind(site_name)
        .fetch_optional(&mut **tx)
        .await
        .map_err(backend)?;
        row.map(|row| Ok::<_, sqlx::Error>((held_from(&row)?, row.try_get("reserved")?)))
            .transpose()
            .map_err(backend)
    }

    /// Admit a renewal against the name's locked record, sign, and record the new key.
    ///
    /// The row lock serializes renewals of one name, so two retries cannot both rotate.
    /// A fork commits the freeze before it is refused.
    #[expect(
        clippy::too_many_lines,
        reason = "the lock, decide, sign, and write read as one transaction"
    )]
    pub(super) async fn renew_and_issue<F>(&self, renewal: &Renewal, sign: F) -> Result<Renewed, StoreError>
    where
        F: FnOnce() -> Result<Issued, StoreError> + Send,
    {
        let mut tx = Box::pin(self.pool.begin()).await.map_err(backend)?;
        let held = Box::pin(Self::locked(&mut tx, &renewal.site_name))
            .await?
            .map(|(held, _)| held);
        let action = match renewal::decide(held.as_ref(), renewal) {
            Err(Refusal::Forked) => {
                sqlx::query("UPDATE site_enrollments SET frozen_at = NOW() WHERE site_name = $1")
                    .bind(&renewal.site_name)
                    .execute(&mut *tx)
                    .await
                    .map_err(backend)?;
                tx.commit().await.map_err(backend)?;
                return Err(Refusal::Forked.into());
            },
            decided => decided?,
        };
        let Some(held) = held else {
            return Err(Refusal::UnknownSite.into());
        };
        let issued = sign()?;
        if action == RenewAction::Rotate {
            sqlx::query(
                "UPDATE site_enrollments
                    SET public_key_sha256 = $2, previous_public_key_sha256 = $3, renewed_at = NOW()
                  WHERE site_name = $1",
            )
            .bind(&renewal.site_name)
            .bind(&renewal.requested_key)
            .bind(&renewal.presented_key)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        sqlx::query("UPDATE site_enrollments SET not_after = $2 WHERE site_name = $1")
            .bind(&renewal.site_name)
            .bind(issued.not_after())
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(Renewed {
            id: held.id,
            action,
            replaced_key: held.current_key,
            issued,
        })
    }

    /// Apply a verified seed to a reserved name's locked record.
    #[expect(
        clippy::too_many_lines,
        reason = "the lock, decide, and write read as one transaction"
    )]
    pub(super) async fn seed_reserved(&self, seed: &SeedRecord) -> Result<Seeded, StoreError> {
        let mut tx = Box::pin(self.pool.begin()).await.map_err(backend)?;
        let locked = Box::pin(Self::locked(&mut tx, &seed.site_name)).await?;
        let seeded = renewal::seed_decision(locked.as_ref().map(|(held, reserved)| (held, *reserved)), seed);
        let generation = i64::try_from(seed.generation).map_err(backend)?;
        let written = match &seeded {
            Seeded::Registered => sqlx::query(
                "INSERT INTO site_enrollments
                     (id, site_token_id, site_name, public_key_sha256, spiffe_id, renewed_at, reserved, seed_generation,
                      epoch_at)
                 VALUES ($1, NULL, $2, $3, $4, NOW(), TRUE, $5, $6)",
            )
            .bind(Uuid::new_v4())
            .bind(&seed.site_name)
            .bind(&seed.key_sha256)
            .bind(certs::spiffe_id(&seed.site_name))
            .bind(generation)
            .bind(seed.issued_at)
            .execute(&mut *tx)
            .await
            .map(drop),
            Seeded::Reset { .. } => sqlx::query(
                "UPDATE site_enrollments
                    SET public_key_sha256 = $2, previous_public_key_sha256 = NULL, frozen_at = NULL,
                        seed_generation = $3, renewed_at = NOW(), epoch_at = $4, not_after = NULL
                  WHERE site_name = $1",
            )
            .bind(&seed.site_name)
            .bind(&seed.key_sha256)
            .bind(generation)
            .bind(seed.issued_at)
            .execute(&mut *tx)
            .await
            .map(drop),
            Seeded::Acknowledged => {
                sqlx::query("UPDATE site_enrollments SET seed_generation = $2 WHERE site_name = $1")
                    .bind(&seed.site_name)
                    .bind(generation)
                    .execute(&mut *tx)
                    .await
                    .map(drop)
            },
            Seeded::Unchanged | Seeded::Older | Seeded::NotReserved => Ok(()),
        };
        written.map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(seeded)
    }

    /// Read a name's record without locking it.
    pub(super) async fn enrollment(&self, site_name: &str) -> Result<Option<super::EnrollmentRecord>, StoreError> {
        let row = sqlx::query(
            "SELECT id, public_key_sha256, previous_public_key_sha256, frozen_at IS NOT NULL AS frozen, reserved,
                    seed_generation, epoch_at, COALESCE(renewed_at, issued_at) AS recorded_at, renewed_at, not_after
               FROM site_enrollments
              WHERE site_name = $1",
        )
        .bind(site_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        row.map(|row| {
            Ok::<_, sqlx::Error>(super::EnrollmentRecord {
                held: held_from(&row)?,
                reserved: row.try_get("reserved")?,
                renewed_at: row.try_get("renewed_at")?,
                not_after: row.try_get("not_after")?,
            })
        })
        .transpose()
        .map_err(backend)
    }

    /// Delete a name's record.
    pub(super) async fn delete_enrollment(&self, site_name: &str) -> Result<(), StoreError> {
        let deleted = sqlx::query("DELETE FROM site_enrollments WHERE site_name = $1")
            .bind(site_name)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        if deleted.rows_affected() == 0 {
            return Err(StoreError::NotFound);
        }
        Ok(())
    }

    /// Ping the pool, for the readiness probe.
    pub(super) async fn ready(&self) -> bool {
        sqlx::query("SELECT 1").execute(&self.pool).await.is_ok()
    }
}

/// The renewal record in a `site_enrollments` row.
fn held_from(row: &sqlx::postgres::PgRow) -> Result<Held, sqlx::Error> {
    let generation: Option<i64> = row.try_get("seed_generation")?;
    Ok(Held {
        id: row.try_get("id")?,
        current_key: row.try_get("public_key_sha256")?,
        previous_key: row.try_get("previous_public_key_sha256")?,
        recorded_at: row.try_get("recorded_at")?,
        epoch_at: row.try_get("epoch_at")?,
        frozen: row.try_get("frozen")?,
        seed_generation: generation.and_then(|value| u64::try_from(value).ok()),
    })
}

/// Whether an error is the unique index refusing a duplicate.
fn is_unique_violation(err: &sqlx::Error) -> bool {
    matches!(err.as_database_error().and_then(sqlx::error::DatabaseError::code), Some(code) if code == "23505")
}

/// Wrap any backend failure.
fn backend<E: std::fmt::Display>(err: E) -> StoreError {
    StoreError::Backend(err.to_string())
}
