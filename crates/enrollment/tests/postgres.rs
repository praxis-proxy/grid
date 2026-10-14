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

use enrollment::{
    CaAction, Issued, NewSiteToken, Pin, Store, StoreError,
    ca::{AnchorError, Anchored, anchor},
    ca_action,
};
use time::{Duration, OffsetDateTime};
use tokio::sync::{Mutex, MutexGuard};

/// Serializes these tests against the one shared database. They exercise a single
/// service's lifecycle, not many services racing one database, so running them
/// concurrently only produces cross-test lock contention.
static DB_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// A store against the test database and the exclusive lock over it, or `None`
/// when no database is configured.
async fn store() -> Option<(Store, MutexGuard<'static, ()>)> {
    let Ok(url) = std::env::var("ENROLLMENT_TEST_DATABASE_URL") else {
        // CI sets this, so a dropped database fails the run instead of skipping every test.
        assert!(
            std::env::var_os("ENROLLMENT_REQUIRE_TEST_DATABASE").is_none(),
            "ENROLLMENT_REQUIRE_TEST_DATABASE is set but ENROLLMENT_TEST_DATABASE_URL is not"
        );
        return None;
    };
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
        allow_deleted_name: false,
    }
}

/// A signer that issues a stub certificate for the pinned name under a fresh
/// key, standing in for the certs primitive the handler uses.
#[expect(clippy::unnecessary_wraps, reason = "matches the redeem_and_issue signer signature")]
fn sign_for(pin: &Pin) -> Result<Issued, StoreError> {
    Ok(issued(pin, &keys()('0')))
}

/// A stub certificate for the pinned name carrying `key`.
fn issued(pin: &Pin, key: &str) -> Issued {
    Issued {
        site_name: String::new(),
        certificate: format!(
            "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----",
            pin.site_name
        ),
        spiffe_id: format!("spiffe://grid.internal/site/{}", pin.site_name),
        public_key_sha256: key.to_owned(),
    }
}

/// Key digests unique to one test, by character, since the database keeps every key it enrolled.
fn keys() -> impl Fn(char) -> String {
    let prefix = format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
    move |letter| {
        format!(
            "{}{}",
            prefix.get(..56).unwrap_or_default(),
            letter.to_string().repeat(8)
        )
    }
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

/// A name an enrollment holds, or a live token pins, is refused at mint.
#[tokio::test]
async fn a_name_mints_one_live_token_and_none_once_held() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let site = unique("dup");

    let first = unique("digest");
    store
        .mint_site_token(new_token(&site, &first))
        .await
        .expect("mint first");
    let second = store.mint_site_token(new_token(&site, &unique("digest"))).await;
    assert!(
        matches!(second, Err(StoreError::TokenOutstanding)),
        "a live token already pins the name, got {second:?}"
    );

    store.redeem_and_issue(&first, sign_for).await.expect("first enroll");
    let held = store.mint_site_token(new_token(&site, &unique("digest"))).await;
    assert!(
        matches!(held, Err(StoreError::NameTaken)),
        "an enrolled name is refused, got {held:?}"
    );
}

/// Concurrent mints of one name: the per-name lock, not the process, lets one through.
#[tokio::test]
async fn concurrent_mints_of_one_name_leave_one_live_token() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let site = unique("mint-race");
    let store = Arc::new(store);
    let racers: Vec<_> = std::iter::repeat_with(|| {
        let (store, token) = (Arc::clone(&store), new_token(&site, &unique("digest")));
        tokio::spawn(async move { store.mint_site_token(token).await })
    })
    .take(8)
    .collect();
    let mut minted = 0_usize;
    for racer in racers {
        let result = racer.await.expect("join");
        assert!(
            matches!(result, Ok(_) | Err(StoreError::TokenOutstanding)),
            "a racing mint either wins or finds the live token, got {result:?}"
        );
        minted = minted.saturating_add(usize::from(result.is_ok()));
    }
    assert_eq!(minted, 1, "exactly one mint wins the name");
}

/// An expired, unredeemed token holds nothing; a live one holds the name.
#[tokio::test]
async fn an_expired_token_frees_the_name_but_a_live_one_holds_it() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let site = unique("expired");
    let mut expired = new_token(&site, &unique("digest"));
    expired.expires_at = OffsetDateTime::now_utc().saturating_sub(Duration::seconds(1));
    store.mint_site_token(expired).await.expect("mint expired");
    store
        .mint_site_token(new_token(&site, &unique("digest")))
        .await
        .expect("an expired token does not hold the name");
    let again = store.mint_site_token(new_token(&site, &unique("digest"))).await;
    assert!(matches!(again, Err(StoreError::TokenOutstanding)), "got {again:?}");
}

/// Mints racing a redeem never leave a live token beside the enrollment.
#[tokio::test]
async fn a_mint_racing_a_redeem_leaves_no_live_token_beside_the_enrollment() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let site = unique("mint-redeem");
    let digest = unique("digest");
    store.mint_site_token(new_token(&site, &digest)).await.expect("mint");
    let store = Arc::new(store);
    let redeem = spawn_redeem(&store, digest);
    let minters: Vec<_> = std::iter::repeat_with(|| {
        let (store, token) = (Arc::clone(&store), new_token(&site, &unique("digest")));
        tokio::spawn(async move { store.mint_site_token(token).await })
    })
    .take(8)
    .collect();
    redeem.await.expect("join").expect("the live token redeems");
    for minter in minters {
        let result = minter.await.expect("join");
        assert!(
            matches!(result, Err(StoreError::TokenOutstanding | StoreError::NameTaken)),
            "no mint lands while the name is pinned or held, got {result:?}"
        );
    }
}

/// A row written before mint refused a second live token for a name.
async fn insert_legacy_token(site: &str, digest: &str) {
    let url = std::env::var("ENROLLMENT_TEST_DATABASE_URL").expect("url");
    let pool = Box::pin(sqlx::PgPool::connect(&url)).await.expect("pool");
    sqlx::query(
        "INSERT INTO site_tokens (id, token_sha256, site_name, grid_network_ref, issued_by, expires_at)
         VALUES ($1, $2, $3, 'demo-grid', 'sam', NOW() + INTERVAL '1 hour')",
    )
    .bind(uuid::Uuid::new_v4())
    .bind(digest)
    .bind(site)
    .execute(&pool)
    .await
    .expect("insert legacy token");
}

/// A token that loses its name at redeem is spent, even after the holder is deleted.
#[tokio::test]
async fn a_token_that_loses_its_name_cannot_claim_it_after_a_delete() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let site = unique("loser");
    let (winner, loser) = (unique("digest"), unique("digest"));
    store.mint_site_token(new_token(&site, &winner)).await.expect("mint");
    Box::pin(insert_legacy_token(&site, &loser)).await;
    store.redeem_and_issue(&winner, sign_for).await.expect("first enroll");

    let lost = store.redeem_and_issue(&loser, sign_for).await;
    assert!(matches!(lost, Err(StoreError::NameTaken)), "got {lost:?}");
    assert!(
        store.token_valid(&loser).await.expect("peek"),
        "a taken name leaves the losing token unspent"
    );

    store.delete_enrollment(&site).await.expect("delete");
    let later = store.redeem_and_issue(&loser, sign_for).await;
    assert!(
        matches!(later, Err(StoreError::NameDeleted)),
        "the losing token cannot claim the deleted name, got {later:?}"
    );
    assert!(
        !store.token_valid(&loser).await.expect("peek"),
        "the deleted-name refusal spends it"
    );
}

/// A name deleted before the history existed is still a deleted name after the upgrade.
#[tokio::test]
async fn a_name_deleted_before_the_upgrade_is_backfilled_as_deleted() {
    let Some((store, guard)) = store().await else {
        return;
    };
    let site = unique("predeleted");
    let digest = unique("digest");
    store.mint_site_token(new_token(&site, &digest)).await.expect("mint");
    store.redeem_and_issue(&digest, sign_for).await.expect("enroll");
    // The pre-upgrade delete: the enrollment goes, the redeemed token and no tombstone stay.
    let url = std::env::var("ENROLLMENT_TEST_DATABASE_URL").expect("url");
    let pool = Box::pin(sqlx::PgPool::connect(&url)).await.expect("pool");
    sqlx::query("DELETE FROM site_enrollments WHERE site_name = $1")
        .bind(&site)
        .execute(&pool)
        .await
        .expect("legacy delete");
    sqlx::query("DELETE FROM deleted_site_names WHERE site_name = $1")
        .bind(&site)
        .execute(&pool)
        .await
        .expect("no tombstone");
    drop(store);

    // Starting again applies the schema, as an upgrade does.
    let upgraded = Store::postgres(&url).await.expect("reconnect");
    let refused = upgraded.mint_site_token(new_token(&site, &unique("digest"))).await;
    assert!(matches!(refused, Err(StoreError::NameDeleted)), "got {refused:?}");
    drop(guard);
}

/// A token the signer refuses for a taken name stays unspent.
#[tokio::test]
async fn a_token_refused_by_the_signer_is_not_spent() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let digest = unique("digest");
    store
        .mint_site_token(new_token(&unique("reserved"), &digest))
        .await
        .expect("mint");
    let refused = store.redeem_and_issue(&digest, |_pin| Err(StoreError::NameTaken)).await;
    assert!(matches!(refused, Err(StoreError::NameTaken)), "got {refused:?}");
    assert!(store.token_valid(&digest).await.expect("peek"), "the token is unspent");
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
/// not the process, holds the name, so exactly one wins and the loser stays unspent.
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
    Box::pin(insert_legacy_token(&site, &second_digest)).await;

    let store = Arc::new(store);
    let first = spawn_redeem(&store, first_digest.clone());
    let second = spawn_redeem(&store, second_digest.clone());
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
    for (digest, won) in [(first_digest, first.is_ok()), (second_digest, second.is_ok())] {
        let live = store.token_valid(&digest).await.expect("peek");
        assert_eq!(live, !won, "only the winner's token is spent");
    }
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

/// A renewal of `site` presenting `presented` and asking for `requested`.
fn renewal(site: &str, presented: &str, requested: &str) -> enrollment::Renewal {
    enrollment::Renewal {
        site_name: site.to_owned(),
        presented_key: presented.to_owned(),
        requested_key: requested.to_owned(),
        presented_not_before: OffsetDateTime::now_utc().saturating_sub(Duration::minutes(5)),
    }
}

/// A signer standing in for the CA on renewal.
fn resign(site: &str) -> Result<Issued, StoreError> {
    sign_for(&Pin {
        site_name: site.to_owned(),
    })
}

/// Rotate, re-sign a lost response, fork, stay frozen, and release on delete.
#[tokio::test]
#[expect(clippy::too_many_lines, reason = "one record's whole lifecycle")]
async fn a_renewal_rotates_the_recorded_key_and_a_fork_freezes_it() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let key = keys();
    let (site, digest) = (unique("site"), unique("digest"));
    store.mint_site_token(new_token(&site, &digest)).await.expect("mint");
    store
        .redeem_and_issue(&digest, |pin| Ok(issued(pin, &key('a'))))
        .await
        .expect("redeem");

    let renewed = store
        .renew_and_issue(&renewal(&site, &key('a'), &key('b')), || resign(&site))
        .await
        .expect("the enrolled key renews");
    assert_eq!(renewed.action, enrollment::RenewAction::Rotate);
    assert_eq!(renewed.replaced_key, key('a'));
    let retried = store
        .renew_and_issue(&renewal(&site, &key('a'), &key('b')), || resign(&site))
        .await
        .expect("a lost response retries");
    assert_eq!(retried.action, enrollment::RenewAction::Resign);
    assert_eq!(retried.id, renewed.id, "the same record");
    let record = store.enrollment(&site).await.expect("read").expect("held");
    assert_eq!(
        (record.held.current_key, record.held.previous_key),
        (key('b'), Some(key('a')))
    );
    assert!(
        record.renewed_at.is_some() && !record.held.frozen,
        "renewed, not frozen"
    );

    let forked = store
        .renew_and_issue(&renewal(&site, &key('a'), &key('c')), || resign(&site))
        .await;
    assert!(
        matches!(forked, Err(StoreError::Refused(enrollment::Refusal::Forked))),
        "{forked:?}"
    );
    let frozen = store
        .renew_and_issue(&renewal(&site, &key('b'), &key('c')), || resign(&site))
        .await;
    assert!(
        matches!(frozen, Err(StoreError::Refused(enrollment::Refusal::Frozen))),
        "the freeze committed: {frozen:?}"
    );
    let frozen_record = store.enrollment(&site).await.expect("read").expect("held");
    assert!(frozen_record.held.frozen, "a grid-admin reads the freeze");

    store.delete_enrollment(&site).await.expect("delete");
    assert!(store.enrollment(&site).await.expect("read").is_none());
    assert!(matches!(
        store.delete_enrollment(&site).await,
        Err(StoreError::NotFound)
    ));
    let gone = store
        .renew_and_issue(&renewal(&site, &key('b'), &key('c')), || resign(&site))
        .await;
    assert!(
        matches!(gone, Err(StoreError::Refused(enrollment::Refusal::UnknownSite))),
        "{gone:?}"
    );
}

/// A seed registers and resets a reserved record; nothing else records without a token.
#[tokio::test]
#[expect(clippy::too_many_lines, reason = "the seed outcomes, then the schema check")]
async fn a_seed_registers_a_reserved_name_and_only_it_records_without_a_token() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let hub = unique("hub");
    let key = keys();
    let seed = |letter, generation| enrollment::SeedRecord {
        site_name: hub.clone(),
        key_sha256: key(letter),
        generation,
        issued_at: OffsetDateTime::now_utc().saturating_sub(Duration::days(1)),
    };
    assert_eq!(
        store.seed_reserved(&seed('d', 10)).await.expect("seed"),
        enrollment::Seeded::Registered
    );
    assert_eq!(
        store.seed_reserved(&seed('d', 10)).await.expect("seed"),
        enrollment::Seeded::Unchanged
    );
    assert_eq!(
        store.seed_reserved(&seed('e', 9)).await.expect("seed"),
        enrollment::Seeded::Older,
        "an older generation"
    );
    assert_eq!(
        store.seed_reserved(&seed('e', 11)).await.expect("seed"),
        enrollment::Seeded::Reset { replaced_key: key('d') }
    );

    let url = std::env::var("ENROLLMENT_TEST_DATABASE_URL").expect("url");
    let pool = Box::pin(sqlx::PgPool::connect(&url)).await.expect("pool");
    let row: (Option<uuid::Uuid>, bool, String, Option<i64>) = sqlx::query_as(
        "SELECT site_token_id, reserved, public_key_sha256, seed_generation
           FROM site_enrollments WHERE site_name = $1",
    )
    .bind(&hub)
    .fetch_one(&pool)
    .await
    .expect("row");
    assert_eq!(row, (None, true, key('e'), Some(11)), "token-less, reserved, seeded");

    let (spoke, digest) = (unique("spoke"), unique("digest"));
    store.mint_site_token(new_token(&spoke, &digest)).await.expect("mint");
    store.redeem_and_issue(&digest, sign_for).await.expect("redeem");
    let over_spoke = enrollment::SeedRecord {
        site_name: spoke.clone(),
        key_sha256: key('f'),
        generation: 1,
        issued_at: OffsetDateTime::now_utc(),
    };
    assert_eq!(
        store.seed_reserved(&over_spoke).await.expect("seed"),
        enrollment::Seeded::NotReserved,
        "a seed never takes over a spoke's record"
    );

    let forged = sqlx::query(
        "INSERT INTO site_enrollments (id, site_token_id, site_name, public_key_sha256, spiffe_id)
         VALUES ($1, NULL, $2, $3, $4)",
    )
    .bind(uuid::Uuid::new_v4())
    .bind(unique("forged"))
    .bind(key('f'))
    .bind("spiffe://grid.internal/site/x")
    .execute(&pool)
    .await;
    assert!(forged.is_err(), "the schema refuses a token-less spoke record");
}

/// Mint a token for `site` and redeem it under `key`.
async fn enroll_with(store: &Store, site: &str, key: &str, allow_deleted_name: bool) -> Result<(), StoreError> {
    let digest = unique("digest");
    store
        .mint_site_token(NewSiteToken {
            allow_deleted_name,
            ..new_token(site, &digest)
        })
        .await?;
    store
        .redeem_and_issue(&digest, |pin| Ok(issued(pin, key)))
        .await
        .map(drop)
}

/// A key enrolled once is refused under any name, before and after its enrollment is deleted.
#[tokio::test]
async fn a_key_enrolled_once_is_refused_under_any_name() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let key = keys();
    let (east, west) = (unique("east"), unique("west"));
    enroll_with(&store, &east, &key('a'), false).await.expect("enroll");

    let digest = unique("digest");
    store
        .mint_site_token(new_token(&west, &digest))
        .await
        .expect("mint west");
    let live = store.redeem_and_issue(&digest, |pin| Ok(issued(pin, &key('a')))).await;
    assert!(matches!(live, Err(StoreError::KeyReused)), "a live key, got {live:?}");
    store.delete_enrollment(&east).await.expect("delete");
    let other = store.redeem_and_issue(&digest, |pin| Ok(issued(pin, &key('a')))).await;
    assert!(
        matches!(other, Err(StoreError::KeyReused)),
        "another name, got {other:?}"
    );
    let same = enroll_with(&store, &east, &key('a'), true).await;
    assert!(matches!(same, Err(StoreError::KeyReused)), "its old name, got {same:?}");
    assert!(
        store.enrollment(&west).await.expect("read").is_none(),
        "a refused key records nothing"
    );
}

/// Racing enrollments of one key under many names: the primary key lets one through.
#[tokio::test]
async fn concurrent_enrollments_of_one_key_leave_one_holder() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let shared = keys()('a');
    let store = Arc::new(store);
    let racers: Vec<_> = std::iter::repeat_with(|| {
        let (store, shared) = (Arc::clone(&store), shared.clone());
        tokio::spawn(async move { Box::pin(enroll_with(&store, &unique("racer"), &shared, false)).await })
    })
    .take(8)
    .collect();
    let mut enrolled = 0_usize;
    for racer in racers {
        let result = racer.await.expect("join");
        assert!(
            matches!(result, Ok(()) | Err(StoreError::KeyReused)),
            "a racer wins or finds the key enrolled, got {result:?}"
        );
        enrolled = enrolled.saturating_add(usize::from(result.is_ok()));
    }
    assert_eq!(enrolled, 1, "exactly one name holds the key");
}

/// A deleted name is refused without the opt-in, at mint and at redeem, and enrolls with it.
#[tokio::test]
async fn a_deleted_name_enrolls_again_only_with_the_opt_in() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let key = keys();
    let site = unique("site");
    enroll_with(&store, &site, &key('a'), false).await.expect("enroll");
    let waiting = unique("digest");
    Box::pin(insert_legacy_token(&site, &waiting)).await;
    store.delete_enrollment(&site).await.expect("delete");

    let minted = store.mint_site_token(new_token(&site, &unique("digest"))).await;
    assert!(matches!(minted, Err(StoreError::NameDeleted)), "got {minted:?}");
    let claimed = store.redeem_and_issue(&waiting, |pin| Ok(issued(pin, &key('b')))).await;
    assert!(
        matches!(claimed, Err(StoreError::NameDeleted)),
        "a token minted before the delete, got {claimed:?}"
    );
    assert!(
        !store.token_valid(&waiting).await.expect("peek"),
        "the refusal spends the token"
    );
    enroll_with(&store, &site, &key('c'), true)
        .await
        .expect("the opt-in enrolls the deleted name");
    let record = store.enrollment(&site).await.expect("read").expect("held");
    assert_eq!(record.held.current_key, key('c'));
}

/// A redeem racing the delete of its name never claims the name without the opt-in.
#[tokio::test]
async fn a_redeem_racing_a_delete_never_claims_the_name() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let key = keys();
    let site = unique("site");
    enroll_with(&store, &site, &key('a'), false).await.expect("enroll");
    let waiting = unique("digest");
    Box::pin(insert_legacy_token(&site, &waiting)).await;

    let store = Arc::new(store);
    let deleter = {
        let (store, site) = (Arc::clone(&store), site.clone());
        tokio::spawn(async move { store.delete_enrollment(&site).await })
    };
    let redeemer = {
        let (store, fresh) = (Arc::clone(&store), key('b'));
        tokio::spawn(async move { store.redeem_and_issue(&waiting, |pin| Ok(issued(pin, &fresh))).await })
    };
    deleter.await.expect("join").expect("delete");
    let claimed = redeemer.await.expect("join");
    assert!(
        matches!(claimed, Err(StoreError::NameTaken | StoreError::NameDeleted)),
        "got {claimed:?}"
    );
    assert!(
        store.enrollment(&site).await.expect("read").is_none(),
        "nothing holds the name"
    );
}

/// Rotation to any key ever enrolled is refused without freezing the record.
#[tokio::test]
#[expect(clippy::too_many_lines, reason = "the reused keys, then the record")]
async fn rotation_to_a_key_ever_enrolled_is_refused() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let key = keys();
    let (east, west) = (unique("east"), unique("west"));
    enroll_with(&store, &east, &key('e'), false).await.expect("enroll east");
    enroll_with(&store, &west, &key('a'), false).await.expect("enroll west");
    store.delete_enrollment(&east).await.expect("delete east");

    let stolen = store
        .renew_and_issue(&renewal(&west, &key('a'), &key('e')), || resign(&west))
        .await;
    assert!(
        matches!(stolen, Err(StoreError::Refused(enrollment::Refusal::KeyReused))),
        "a deleted site's key, got {stolen:?}"
    );
    for (presented, requested) in [('a', 'b'), ('b', 'c')] {
        store
            .renew_and_issue(&renewal(&west, &key(presented), &key(requested)), || resign(&west))
            .await
            .expect("rotate");
    }
    let back = store
        .renew_and_issue(&renewal(&west, &key('c'), &key('a')), || resign(&west))
        .await;
    assert!(
        matches!(back, Err(StoreError::Refused(enrollment::Refusal::KeyReused))),
        "its own key from two rotations back, got {back:?}"
    );
    let record = store.enrollment(&west).await.expect("read").expect("held");
    assert!(!record.held.frozen, "a refused reuse does not freeze");
    assert_eq!(record.held.current_key, key('c'));
    let reused = enroll_with(&store, &unique("fresh"), &key('b'), false).await;
    assert!(
        matches!(reused, Err(StoreError::KeyReused)),
        "a key recorded only during rotation cannot enroll a fresh name: {reused:?}"
    );
}
/// Clear the recorded grid CA, standing in for a new database.
async fn forget_ca() {
    let url = std::env::var("ENROLLMENT_TEST_DATABASE_URL").expect("database url");
    let pool = Box::pin(sqlx::PgPool::connect(&url)).await.expect("pool");
    sqlx::query("DELETE FROM grid_ca_anchor")
        .execute(&pool)
        .await
        .expect("clear the anchor");
}

/// A freshly minted CA's fingerprint.
fn minted_ca() -> String {
    let ca = certs::generate_ca("grid-ca").expect("ca");
    certs::canonical_fingerprint(&ca.cert_pem).expect("fingerprint")
}

/// A first start records a CA, then an attacker deletes the CA key Secret and every
/// distributed copy, so bootstrap mints a new one and the service restarts.
/// Returns the restarted store, the recorded fingerprint, and the minted one.
async fn restarted_with_a_minted_ca() -> Option<(Store, MutexGuard<'static, ()>, String, String)> {
    let (store, guard) = store().await?;
    forget_ca().await;
    let original = minted_ca();
    assert_eq!(
        anchor(&store, &original, None).await.expect("first start"),
        Anchored::Recorded,
        "the first start records the CA"
    );
    drop(store);
    assert_eq!(
        ca_action(None, false, &[]),
        CaAction::Mint,
        "with every Secret gone bootstrap mints"
    );
    let url = std::env::var("ENROLLMENT_TEST_DATABASE_URL").expect("database url");
    let restarted = Store::postgres(&url).await.expect("restart");
    Some((restarted, guard, original, minted_ca()))
}

#[tokio::test]
async fn a_ca_minted_after_its_secrets_were_deleted_is_refused() {
    let Some((store, _guard, original, minted)) = restarted_with_a_minted_ca().await else {
        return;
    };
    let refused = anchor(&store, &minted, None).await;
    assert!(
        matches!(&refused, Err(AnchorError::Mismatch { recorded, loaded }) if *recorded == original && *loaded == minted),
        "{refused:?}"
    );
    let truncated = original.get(..63).expect("fingerprint");
    let upper = original.to_ascii_uppercase();
    let padded = format!(" {original}");
    for wrong in ["", truncated, &upper, &padded, &minted, "not-a-fingerprint"] {
        assert!(
            anchor(&store, &minted, Some(wrong)).await.is_err(),
            "the override {wrong:?} does not name the recorded CA"
        );
    }
    assert_eq!(store.recorded_ca().await.expect("read"), Some(original));
}

#[tokio::test]
async fn starting_over_needs_the_old_fingerprint_confirmed() {
    let Some((store, _guard, original, minted)) = restarted_with_a_minted_ca().await else {
        return;
    };
    assert_eq!(
        anchor(&store, &minted, Some(&original)).await.expect("start over"),
        Anchored::Replaced { previous: original },
    );
    assert_eq!(store.recorded_ca().await.expect("read"), Some(minted));
}

/// Two replicas racing a first start with different CAs: only one is recorded.
#[tokio::test]
async fn racing_first_starts_record_one_ca() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    forget_ca().await;
    let (left, right) = (minted_ca(), minted_ca());
    let (first, second) = tokio::join!(anchor(&store, &left, None), anchor(&store, &right, None));
    assert!(
        first.is_ok() != second.is_ok(),
        "exactly one CA wins: {first:?} {second:?}"
    );
}
