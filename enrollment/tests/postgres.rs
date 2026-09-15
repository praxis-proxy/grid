//! The Postgres backend and the full mint-to-issue lifecycle, against a real
//! database.
//!
//! Skipped unless `ENROLLMENT_TEST_DATABASE_URL` points at one, so the suite
//! stays runnable without a database. Start one with:
//!
//! ```text
//! podman run -d --name grid-enroll-pg -e POSTGRES_PASSWORD=test \
//!   -e POSTGRES_DB=enrollment -p 55432:5432 docker.io/library/postgres:16-alpine
//! export ENROLLMENT_TEST_DATABASE_URL=postgres://postgres:test@127.0.0.1:55432/enrollment
//! ```

#![allow(clippy::tests_outside_test_module, reason = "integration tests live in tests/")]
#![expect(clippy::expect_used, reason = "tests")]

use std::sync::{Arc, LazyLock};

use enrollment::{Issued, NewSiteToken, Pin, Store, StoreError};
use time::{Duration, OffsetDateTime};
use tokio::sync::{Mutex, MutexGuard};

/// Serializes these tests against the one shared database. They exercise a single
/// service's lifecycle, not many services racing one database, so running them
/// concurrently only produces cross-test lock contention.
static DB_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// A store against the test database and the exclusive lock over it, or `None`
/// when no database is configured.
async fn store() -> Option<(Store, MutexGuard<'static, ()>)> {
    let url = std::env::var("ENROLLMENT_TEST_DATABASE_URL").ok()?;
    let guard = DB_LOCK.lock().await;
    let store = Store::postgres(&url).await.expect("connect to the test database");
    Some((store, guard))
}

/// Give each test its own names and tokens, so they can share one database.
fn unique(prefix: &str) -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", id.get(..8).unwrap_or("x"))
}

/// A token pinning `site`, keyed by a digest unique to this test. The store
/// compares digests as opaque strings, so the API-layer hashing is not needed
/// here.
fn new_token(site: &str, digest: &str) -> NewSiteToken {
    NewSiteToken {
        token_sha256: digest.to_owned(),
        site_name: site.to_owned(),
        grid_network_ref: "demo-grid".to_owned(),
        issued_by: "sam".to_owned(),
        expires_at: OffsetDateTime::now_utc().saturating_add(Duration::hours(1)),
    }
}

/// A signer that issues a stub certificate for the pinned name, standing in for
/// the certs primitive the handler uses.
#[expect(clippy::unnecessary_wraps, reason = "matches the redeem_and_issue signer signature")]
fn sign_for(pin: &Pin) -> Result<Issued, StoreError> {
    Ok(Issued {
        certificate: format!(
            "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----",
            pin.site_name
        ),
        spiffe_id: format!("spiffe://grid.internal/site/{}", pin.site_name),
        public_key_sha256: "a".repeat(64),
    })
}

/// The full lifecycle: a grid-admin mints a token, a site redeems it under the
/// pinned name, and the certificate is signed and returned.
#[tokio::test]
async fn the_full_lifecycle_runs_mint_to_issued() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let site = unique("site");
    let digest = unique("digest");

    store.mint_site_token(new_token(&site, &digest)).await.expect("mint");
    assert!(
        store.token_valid(&digest).await.expect("peek"),
        "a fresh token is usable"
    );

    let (enrollment_id, issued) = store
        .redeem_and_issue(&digest, sign_for)
        .await
        .expect("redeem and issue");
    assert!(!enrollment_id.is_nil(), "redeem returns the issued-enrollment row id");
    assert_eq!(
        issued.spiffe_id,
        format!("spiffe://grid.internal/site/{site}"),
        "the certificate is signed under the pinned name"
    );

    assert!(
        !store.token_valid(&digest).await.expect("peek"),
        "the token is spent once redeemed"
    );
}

#[tokio::test]
async fn a_token_redeems_at_most_once() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let digest = unique("digest");
    store
        .mint_site_token(new_token(&unique("once"), &digest))
        .await
        .expect("mint");

    store.redeem_and_issue(&digest, sign_for).await.expect("first redeem");
    let second = store.redeem_and_issue(&digest, sign_for).await;
    assert!(
        matches!(second, Err(StoreError::TokenInvalid)),
        "a token cannot enroll a second site, got {second:?}"
    );
}

#[tokio::test]
async fn an_unknown_or_expired_token_is_invalid() {
    let Some((store, _guard)) = store().await else {
        return;
    };

    let unknown = store.redeem_and_issue(&unique("nope"), sign_for).await;
    assert!(
        matches!(unknown, Err(StoreError::TokenInvalid)),
        "a token nobody minted is invalid, got {unknown:?}"
    );

    let digest = unique("digest");
    let mut expired = new_token(&unique("stale"), &digest);
    expired.expires_at = OffsetDateTime::now_utc().saturating_sub(Duration::hours(1));
    store.mint_site_token(expired).await.expect("mint expired token");
    assert!(
        !store.token_valid(&digest).await.expect("peek"),
        "an expired token is unusable"
    );
    let redeemed = store.redeem_and_issue(&digest, sign_for).await;
    assert!(
        matches!(redeemed, Err(StoreError::TokenInvalid)),
        "an expired token cannot be redeemed, got {redeemed:?}"
    );
}

#[tokio::test]
async fn a_revoked_token_cannot_be_redeemed() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let digest = unique("digest");
    let token_id = store
        .mint_site_token(new_token(&unique("revoked"), &digest))
        .await
        .expect("mint");

    store.revoke_site_token(token_id).await.expect("revoke");
    let redeemed = store.redeem_and_issue(&digest, sign_for).await;
    assert!(
        matches!(redeemed, Err(StoreError::TokenInvalid)),
        "a revoked token is unusable, got {redeemed:?}"
    );
}

/// Revoke is a pre-redemption kill switch: a redeemed token's row stays as
/// issuance provenance, so revoking it reports not found rather than deleting.
#[tokio::test]
async fn a_redeemed_token_cannot_be_revoked() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let digest = unique("digest");
    let token_id = store
        .mint_site_token(new_token(&unique("redeemed"), &digest))
        .await
        .expect("mint");
    store.redeem_and_issue(&digest, sign_for).await.expect("redeem");

    let revoked = store.revoke_site_token(token_id).await;
    assert!(
        matches!(revoked, Err(StoreError::NotFound)),
        "a redeemed token is no longer revocable, got {revoked:?}"
    );
}

/// The partial unique index, not the process, is what guarantees this.
#[tokio::test]
async fn two_sites_cannot_hold_one_name() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let site = unique("dup");

    let first = unique("digest");
    store
        .mint_site_token(new_token(&site, &first))
        .await
        .expect("mint first");
    store.redeem_and_issue(&first, sign_for).await.expect("first enroll");

    let second = unique("digest");
    store
        .mint_site_token(new_token(&site, &second))
        .await
        .expect("mint second");
    let again = store.redeem_and_issue(&second, sign_for).await;
    assert!(
        matches!(again, Err(StoreError::NameTaken)),
        "a name an issued member holds must be refused, got {again:?}"
    );
    assert!(
        store.token_valid(&second).await.expect("peek"),
        "the refused redeem rolls back, so the second token is not spent"
    );
}

/// Redeem `digest` on its own task, so two can race the same store.
fn spawn_redeem(
    store: &Arc<Store>,
    digest: String,
) -> tokio::task::JoinHandle<Result<(uuid::Uuid, Issued), StoreError>> {
    let store = Arc::clone(store);
    tokio::spawn(async move { store.redeem_and_issue(&digest, sign_for).await })
}

/// Two racers redeeming one token: the guarded UPDATE, not the process, makes it
/// single-use, so exactly one wins and the other is refused as invalid.
#[tokio::test]
async fn concurrent_redeems_spend_a_token_once() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let digest = unique("digest");
    store
        .mint_site_token(new_token(&unique("race"), &digest))
        .await
        .expect("mint");

    let store = Arc::new(store);
    let first = spawn_redeem(&store, digest.clone());
    let second = spawn_redeem(&store, digest);
    let first = first.await.expect("join first");
    let second = second.await.expect("join second");

    let succeeded = usize::from(first.is_ok()) + usize::from(second.is_ok());
    let invalid = usize::from(matches!(first, Err(StoreError::TokenInvalid)))
        + usize::from(matches!(second, Err(StoreError::TokenInvalid)));
    assert_eq!(succeeded, 1, "exactly one redeem wins, got {first:?} and {second:?}");
    assert_eq!(
        invalid, 1,
        "the loser is refused as invalid, got {first:?} and {second:?}"
    );
}

/// Two racers redeeming different tokens that pin the same name: the unique index,
/// not the process, holds the name, so exactly one wins and the other is refused.
#[tokio::test]
async fn concurrent_redeems_of_one_name_leave_one_holder() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let site = unique("dupe");
    let first_digest = unique("digest");
    let second_digest = unique("digest");
    store
        .mint_site_token(new_token(&site, &first_digest))
        .await
        .expect("mint first");
    store
        .mint_site_token(new_token(&site, &second_digest))
        .await
        .expect("mint second");

    let store = Arc::new(store);
    let first = spawn_redeem(&store, first_digest);
    let second = spawn_redeem(&store, second_digest);
    let first = first.await.expect("join first");
    let second = second.await.expect("join second");

    let succeeded = usize::from(first.is_ok()) + usize::from(second.is_ok());
    let name_taken = usize::from(matches!(first, Err(StoreError::NameTaken)))
        + usize::from(matches!(second, Err(StoreError::NameTaken)));
    assert_eq!(succeeded, 1, "exactly one holder wins, got {first:?} and {second:?}");
    assert_eq!(
        name_taken, 1,
        "the loser is refused as name taken, got {first:?} and {second:?}"
    );
}

/// The invariant: the token is spent only when the certificate is issued.
#[tokio::test]
async fn a_signing_failure_leaves_the_token_unspent() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let digest = unique("digest");
    store
        .mint_site_token(new_token(&unique("unsigned"), &digest))
        .await
        .expect("mint");

    let failed = store
        .redeem_and_issue(&digest, |_pin| Err(StoreError::Backend("signing fault".to_owned())))
        .await;
    assert!(
        matches!(failed, Err(StoreError::Backend(_))),
        "a signing failure surfaces, got {failed:?}"
    );
    assert!(
        store.token_valid(&digest).await.expect("peek"),
        "a signing failure rolls back, so the token stays unspent"
    );
}
