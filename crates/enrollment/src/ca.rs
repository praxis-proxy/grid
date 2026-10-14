//! The signing CA, swappable while the service runs.
//!
//! Bootstrap can regenerate or restore the CA Secret under a running pod. The
//! kubelet updates the mounted files, and [`SharedCa::reload`] swaps the new CA
//! in, so new site certificates are never signed by a CA the grid has replaced.

use std::{
    collections::BTreeSet,
    path::Path,
    sync::{
        Arc, PoisonError, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use certs::CaCert;

use crate::{Store, StoreError};

/// Confirms replacing the recorded grid CA; its value is the replaced CA's fingerprint.
pub const CONFIRM_ROTATION_VAR: &str = "ENROLLMENT_CONFIRM_CA_ROTATION";

/// The current signing CA. A handler takes one [`SharedCa::current`] snapshot per
/// request, so the certificate it signs and the CA it returns always match.
#[derive(Debug)]
pub struct SharedCa {
    /// The CA in use.
    ca: RwLock<Arc<CaCert>>,
    /// Whether the database last recorded this CA's key; signing stops when it does not.
    anchored: AtomicBool,
}

/// Why a CA reload was refused. The current CA stays in use.
#[derive(Debug, thiserror::Error)]
pub enum ReloadError {
    /// The mounted certificate or key could not be read.
    #[error("reading CA material: {0}")]
    Read(#[from] std::io::Error),
    /// The new certificate could not be fingerprinted.
    #[error("CA certificate: {0}")]
    Certificate(#[from] certs::VerifyError),
    /// The new certificate and key do not load as a CA.
    #[error("CA material: {0}")]
    Load(#[from] certs::GenerateError),
    /// The new CA is not the one the database records.
    #[error(transparent)]
    Anchor(#[from] AnchorError),
}

impl SharedCa {
    /// Hold `ca` as the current signing CA.
    #[must_use]
    pub fn new(ca: CaCert) -> Self {
        Self {
            ca: RwLock::new(Arc::new(ca)),
            anchored: AtomicBool::new(true),
        }
    }

    /// The current signing CA.
    #[must_use]
    pub fn current(&self) -> Arc<CaCert> {
        Arc::clone(&self.ca.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// The CA to sign with, or `None` while the database records another CA.
    #[must_use]
    pub fn signer(&self) -> Option<Arc<CaCert>> {
        self.anchored.load(Ordering::Acquire).then(|| self.current())
    }

    /// Reload mounted CA material, checking the held CA's record even if a file cannot be read.
    ///
    /// # Errors
    ///
    /// Returns [`ReloadError`] if reading, loading, or anchoring the mounted CA fails.
    pub async fn reload_from_files(
        &self,
        record: RecordedCa<'_>,
        common_name: &str,
        cert_path: &Path,
        key_path: &Path,
    ) -> Result<Option<(String, String)>, ReloadError> {
        let material = async {
            let cert = tokio::fs::read_to_string(cert_path).await?;
            let key = tokio::fs::read_to_string(key_path).await?;
            Ok::<_, std::io::Error>((cert, key))
        }
        .await;
        match material {
            Ok((cert, key)) => self.reload(record, common_name, &cert, &key).await,
            Err(err) => {
                drop(self.check_record(record.store, &self.current().cert_pem).await);
                Err(err.into())
            },
        }
    }

    /// Swap in the CA from `cert_pem` and `key_pem` when its certificate differs
    /// from the current one and [`anchor`] admits it against `record`. Returns the
    /// old and new fingerprints on a swap, or `None` when the CA is unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`ReloadError`] when the new material does not load or the
    /// database records another CA. The current CA is kept.
    #[expect(
        clippy::too_many_lines,
        reason = "keep the CA validation and held-signer fallback together"
    )]
    pub async fn reload(
        &self,
        record: RecordedCa<'_>,
        common_name: &str,
        cert_pem: &str,
        key_pem: &str,
    ) -> Result<Option<(String, String)>, ReloadError> {
        let held = self.current();
        let incoming = match certs::canonical_fingerprint(cert_pem) {
            Ok(fingerprint) => fingerprint,
            Err(err) => {
                drop(self.check_record(record.store, &held.cert_pem).await);
                return Err(err.into());
            },
        };
        let current = certs::canonical_fingerprint(&held.cert_pem)?;
        if incoming == current {
            // Another replica may have confirmed a rotation this mount has not seen yet.
            self.check_record(record.store, &held.cert_pem).await?;
            return Ok(None);
        }
        let admitted = async {
            // Load first so the record never moves to a CA that cannot sign.
            let ca = certs::load_ca(common_name, key_pem, cert_pem)?;
            // The anchor is the key, so a certificate renewed on the same key passes.
            anchor(record.store, &certs::cert_public_key_sha256(cert_pem)?, record.confirm).await?;
            Ok::<_, ReloadError>(ca)
        }
        .await;
        match admitted {
            Ok(ca) => {
                *self.ca.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(ca);
                self.anchored.store(true, Ordering::Release);
                Ok(Some((current, incoming)))
            },
            Err(refused) => {
                // The held CA is kept, so it must still be the recorded one to sign.
                drop(self.check_record(record.store, &held.cert_pem).await);
                Err(refused)
            },
        }
    }

    /// Sign only while the database records the held CA's key, recording it if the record is gone.
    async fn check_record(&self, store: &Store, cert_pem: &str) -> Result<(), ReloadError> {
        let checked = async {
            let held = certs::cert_public_key_sha256(cert_pem)?;
            Ok::<_, ReloadError>(anchor(store, &held, None).await?)
        }
        .await;
        match checked {
            Ok(_) => {
                self.anchored.store(true, Ordering::Release);
                Ok(())
            },
            // Any refusal, a mismatch or a store that cannot be read, withholds the
            // signer: an unverifiable record is not a verified one.
            Err(refused) => {
                self.anchored.store(false, Ordering::Release);
                Err(refused)
            },
        }
    }
}

/// The CA record a reload is checked against.
#[derive(Clone, Copy, Debug)]
pub struct RecordedCa<'store> {
    /// The store holding the record.
    pub store: &'store Store,
    /// The recorded fingerprint an operator confirmed replacing, if any.
    pub confirm: Option<&'store str>,
}

/// How a CA stands against the one the database records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Anchored {
    /// None was recorded, so this one now is.
    Recorded,
    /// It is the recorded CA.
    Matched,
    /// It replaced the recorded CA, as an operator confirmed.
    Replaced {
        /// The fingerprint of the CA it replaced.
        previous: String,
    },
}

/// Why a CA was refused against the one the database records.
#[derive(Debug, thiserror::Error)]
pub enum AnchorError {
    /// The database records a different CA and no confirmation names it.
    #[error(
        "the database records grid CA {recorded}, but the loaded CA is {loaded}: a different CA was minted or \
         restored. Restore the CA with fingerprint {recorded}, or to start a new grid on purpose set \
         {CONFIRM_ROTATION_VAR}={recorded}"
    )]
    Mismatch {
        /// The fingerprint the database records.
        recorded: String,
        /// The fingerprint of the CA offered.
        loaded: String,
    },
    /// The record could not be read or written.
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Square the CA with fingerprint `loaded` with the one `store` records.
///
/// The first CA is recorded. A different one is refused unless `confirm` is
/// exactly the recorded fingerprint, in which case it replaces it.
///
/// # Errors
///
/// Returns [`AnchorError::Mismatch`] for an unconfirmed different CA, and
/// [`AnchorError::Store`] if the backend failed.
pub async fn anchor(store: &Store, loaded: &str, confirm: Option<&str>) -> Result<Anchored, AnchorError> {
    let recorded = store.recorded_ca().await?;
    let decided = match recorded.as_deref() {
        Some(recorded) if recorded == loaded => return Ok(Anchored::Matched),
        None => Anchored::Recorded,
        Some(recorded) if confirm == Some(recorded) => Anchored::Replaced {
            previous: recorded.to_owned(),
        },
        Some(recorded) => return Err(mismatch(recorded, loaded)),
    };
    if store.swap_recorded_ca(recorded.as_deref(), loaded).await? {
        return Ok(decided);
    }
    // Another service moved the record first: accept only what it recorded.
    match store.recorded_ca().await? {
        Some(now) if now == loaded => Ok(Anchored::Matched),
        Some(now) => Err(mismatch(&now, loaded)),
        None => Err(StoreError::Backend("the recorded grid CA was removed while being recorded".to_owned()).into()),
    }
}

/// A refusal naming both fingerprints.
fn mismatch(recorded: &str, loaded: &str) -> AnchorError {
    AnchorError::Mismatch {
        recorded: recorded.to_owned(),
        loaded: loaded.to_owned(),
    }
}

/// One copy of the grid CA already distributed to sites.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CaCopy {
    /// The fingerprints of the CAs it holds: the current one, and any it replaced.
    Holds(BTreeSet<String>),
    /// It exists but does not parse.
    Unreadable(String),
}

/// What bootstrap does about the grid CA.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CaAction {
    /// Load the CA its key Secret holds.
    Load,
    /// Mint a new CA: none is distributed, or a grid-admin asked for one.
    Mint,
    /// Refuse, for this reason: a new or different CA would split the grid.
    Refuse(String),
}

/// Decide from the fingerprint of the CA the key Secret holds, if any, whether a
/// regeneration was asked for, and every copy of the CA already distributed.
#[must_use]
pub fn ca_action(key: Option<&str>, force_regenerate: bool, copies: &[CaCopy]) -> CaAction {
    if force_regenerate {
        return CaAction::Mint;
    }
    let mut held = Vec::new();
    for copy in copies {
        match copy {
            CaCopy::Unreadable(why) => {
                return CaAction::Refuse(format!("a distributed copy of the grid CA cannot be read ({why})"));
            },
            CaCopy::Holds(fingerprints) => held.push(fingerprints),
        }
    }
    match (key, held.first()) {
        (None, None) => CaAction::Mint,
        (None, Some(out)) => CaAction::Refuse(format!(
            "the CA key Secret is missing, but the grid already uses CA {}",
            out.iter().next().map_or("", String::as_str)
        )),
        (Some(key), _) if held.iter().all(|out| out.contains(key)) => CaAction::Load,
        (Some(key), _) => CaAction::Refuse(format!(
            "the CA key Secret holds CA {key}, which a distributed copy does not: a different CA was restored"
        )),
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use super::{
        AnchorError, Anchored, CONFIRM_ROTATION_VAR, CaAction, CaCopy, RecordedCa, ReloadError, SharedCa, anchor,
        ca_action,
    };
    use crate::Store;

    #[test]
    fn the_ca_changes_only_when_asked() {
        let holds = |fps: &[&str]| CaCopy::Holds(fps.iter().map(|fp| (*fp).to_owned()).collect());
        let refused = |action: CaAction| matches!(action, CaAction::Refuse(_));
        let out = [holds(&["ab"])];
        assert_eq!(ca_action(Some("ab"), false, &out), CaAction::Load, "the key matches");
        assert_eq!(ca_action(Some("ab"), false, &[]), CaAction::Load, "an upgrade");
        assert_eq!(ca_action(None, false, &[]), CaAction::Mint, "a first install");
        assert!(refused(ca_action(None, false, &out)), "the key is gone, the CA is out");
        assert!(refused(ca_action(Some("cd"), false, &out)), "another CA was restored");
        let rotating = [holds(&["ab"]), holds(&["old", "ab"])];
        assert_eq!(
            ca_action(Some("ab"), false, &rotating),
            CaAction::Load,
            "a rotation bundle holds it"
        );
        let split = [holds(&["ab"]), holds(&["cd"])];
        assert!(refused(ca_action(Some("ab"), false, &split)), "copies that disagree");
        let unreadable = [CaCopy::Unreadable("not PEM".to_owned())];
        assert!(
            refused(ca_action(None, false, &unreadable)),
            "an unreadable copy is not none"
        );
        assert!(refused(ca_action(Some("ab"), false, &unreadable)), "nor a match");
        assert_eq!(ca_action(None, true, &out), CaAction::Mint, "a grid-admin starts over");
    }

    /// A reload record with no confirmation.
    fn unconfirmed(store: &Store) -> RecordedCa<'_> {
        RecordedCa { store, confirm: None }
    }

    /// The fingerprint of a fresh CA, and the CA.
    fn fresh() -> (String, certs::CaCert) {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        (certs::cert_public_key_sha256(&ca.cert_pem).expect("fingerprint"), ca)
    }

    /// A new self-signed certificate for `ca`'s key, as a renewal re-issues one.
    fn reissued(ca: &certs::CaCert) -> String {
        let key = rcgen::KeyPair::from_pem(&ca.key_pem).expect("key");
        let mut params = rcgen::CertificateParams::default();
        params.distinguished_name.push(rcgen::DnType::CommonName, "grid-ca");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign, rcgen::KeyUsagePurpose::CrlSign];
        params.self_signed(&key).expect("reissue").pem()
    }

    #[tokio::test]
    async fn a_ca_reissued_on_the_same_key_reloads_without_confirmation() {
        let (fingerprint, ca) = fresh();
        let renewed = reissued(&ca);
        assert_ne!(
            certs::canonical_fingerprint(&renewed).expect("cert"),
            certs::canonical_fingerprint(&ca.cert_pem).expect("cert"),
            "the renewal is a different certificate"
        );
        let key_pem = ca.key_pem.clone();
        let shared = SharedCa::new(ca);
        let store = Store::memory();
        anchor(&store, &fingerprint, None).await.expect("first start");

        let swapped = shared
            .reload(unconfirmed(&store), "grid-ca", &renewed, &key_pem)
            .await
            .expect("same key reloads");
        assert!(swapped.is_some(), "the renewed certificate is swapped in");
        assert_eq!(shared.current().cert_pem, renewed);
    }

    #[tokio::test]
    async fn the_first_ca_is_recorded_and_a_different_one_refused() {
        let store = Store::memory();
        let (old, _) = fresh();
        let (new, _) = fresh();
        assert_eq!(anchor(&store, &old, None).await.expect("first"), Anchored::Recorded);
        assert_eq!(store.recorded_ca().await.expect("read"), Some(old.clone()));
        assert_eq!(anchor(&store, &old, None).await.expect("again"), Anchored::Matched);

        let refused = anchor(&store, &new, None).await.expect_err("a different CA");
        let message = refused.to_string();
        assert!(
            message.contains(&old) && message.contains(&new) && message.contains(CONFIRM_ROTATION_VAR),
            "the refusal names both fingerprints and the override: {message}"
        );
        assert!(
            anchor(&store, &new, Some(&new)).await.is_err(),
            "confirming the new CA is not confirming the old one"
        );
        assert_eq!(
            store.recorded_ca().await.expect("read"),
            Some(old),
            "a refusal records nothing"
        );
    }

    #[tokio::test]
    async fn confirming_the_old_fingerprint_replaces_it_once() {
        let store = Store::memory();
        let (old, _) = fresh();
        let (new, _) = fresh();
        let (third, _) = fresh();
        anchor(&store, &old, None).await.expect("first");
        assert_eq!(
            anchor(&store, &new, Some(&old)).await.expect("confirmed"),
            Anchored::Replaced { previous: old.clone() },
        );
        assert_eq!(store.recorded_ca().await.expect("read"), Some(new.clone()));
        assert!(
            anchor(&store, &third, Some(&old)).await.is_err(),
            "a stale confirmation does not admit another CA"
        );
    }

    #[tokio::test]
    async fn an_override_that_is_not_exactly_the_recorded_fingerprint_is_refused() {
        let store = Store::memory();
        let (old, _) = fresh();
        let (new, _) = fresh();
        anchor(&store, &old, None).await.expect("first");
        let truncated = old.get(..63).expect("fingerprint");
        let upper = old.to_ascii_uppercase();
        let padded = format!("{old}\n");
        for wrong in ["", truncated, &upper, &padded, &new] {
            assert!(
                anchor(&store, &new, Some(wrong)).await.is_err(),
                "override {wrong:?} must not admit a new CA"
            );
        }
        assert_eq!(store.recorded_ca().await.expect("read"), Some(old));
    }

    #[tokio::test]
    async fn an_unchanged_ca_is_not_swapped() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let (cert, key) = (ca.cert_pem.clone(), ca.key_pem.clone());
        let shared = SharedCa::new(ca);
        let store = Store::memory();
        assert!(
            shared
                .reload(unconfirmed(&store), "grid-ca", &cert, &key)
                .await
                .expect("reload")
                .is_none(),
            "same cert, no swap"
        );
    }

    #[tokio::test]
    async fn a_new_ca_is_swapped_in_and_signs() {
        let shared = SharedCa::new(certs::generate_ca("grid-ca").expect("old ca"));
        let next = certs::generate_ca("grid-ca").expect("new ca");
        let store = Store::memory();
        let swapped = shared
            .reload(unconfirmed(&store), "grid-ca", &next.cert_pem, &next.key_pem)
            .await
            .expect("reload with nothing recorded");
        assert!(swapped.is_some(), "a different cert swaps");
        assert_eq!(shared.current().cert_pem, next.cert_pem, "the new CA is current");

        let leaf =
            certs::generate_dns_only_cert(&shared.current(), "grid-ca", &["enroll.grid.svc".to_owned()]).expect("leaf");
        assert_eq!(
            certs::verify_issued_by(&next.cert_pem, &leaf.cert_pem),
            Ok(()),
            "new leaves chain to the new CA"
        );
    }

    #[tokio::test]
    async fn a_reload_the_database_does_not_record_is_refused() {
        let (old_fp, old) = fresh();
        let (_, next) = fresh();
        let original = old.cert_pem.clone();
        let shared = SharedCa::new(old);
        let store = Store::memory();
        anchor(&store, &old_fp, None).await.expect("first start");

        let refused = shared
            .reload(unconfirmed(&store), "grid-ca", &next.cert_pem, &next.key_pem)
            .await;
        assert!(
            matches!(refused, Err(ReloadError::Anchor(AnchorError::Mismatch { .. }))),
            "{refused:?}"
        );
        assert_eq!(shared.current().cert_pem, original, "the current CA is kept");

        let confirmed = shared
            .reload(
                RecordedCa {
                    confirm: Some(&old_fp),
                    ..unconfirmed(&store)
                },
                "grid-ca",
                &next.cert_pem,
                &next.key_pem,
            )
            .await
            .expect("confirmed reload");
        assert!(confirmed.is_some(), "a confirmed CA swaps");
        assert_eq!(shared.current().cert_pem, next.cert_pem);
    }

    #[tokio::test]
    async fn a_bad_reload_keeps_the_current_ca() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let original = ca.cert_pem.clone();
        let other = certs::generate_ca("grid-ca").expect("other");
        let shared = SharedCa::new(ca);
        let store = Store::memory();

        assert!(
            shared
                .reload(unconfirmed(&store), "grid-ca", "not a cert", "not a key")
                .await
                .is_err(),
            "garbage"
        );
        let mismatched = shared
            .reload(unconfirmed(&store), "grid-ca", &other.cert_pem, "not a key")
            .await;
        assert!(mismatched.is_err(), "a cert without its key");
        assert_eq!(shared.current().cert_pem, original, "the current CA is kept");
        assert_eq!(
            store.recorded_ca().await.expect("read"),
            Some(certs::cert_public_key_sha256(&original).expect("fingerprint")),
            "material that does not load is never recorded, only the CA still held"
        );
    }

    #[tokio::test]
    async fn an_unchanged_ca_stops_signing_once_another_replica_confirms_a_rotation() {
        let (old_fp, ca) = fresh();
        let (new_fp, _) = fresh();
        let (cert, key) = (ca.cert_pem.clone(), ca.key_pem.clone());
        let shared = SharedCa::new(ca);
        let store = Store::memory();
        anchor(&store, &old_fp, None).await.expect("first start");
        assert!(
            shared
                .reload(unconfirmed(&store), "grid-ca", &cert, &key)
                .await
                .expect("unchanged")
                .is_none(),
            "an unchanged mount swaps nothing"
        );
        assert!(shared.signer().is_some(), "the recorded CA signs");

        anchor(&store, &new_fp, Some(&old_fp))
            .await
            .expect("another replica confirms");
        let refused = shared.reload(unconfirmed(&store), "grid-ca", &cert, &key).await;
        assert!(
            matches!(refused, Err(ReloadError::Anchor(AnchorError::Mismatch { .. }))),
            "{refused:?}"
        );
        assert!(
            shared.signer().is_none(),
            "a CA the database no longer records must not sign"
        );
    }

    #[tokio::test]
    async fn a_refused_new_ca_withholds_a_held_ca_the_record_has_moved_past() {
        let (old_fp, ca) = fresh();
        let (new_fp, _) = fresh();
        let (_, third) = fresh();
        let shared = SharedCa::new(ca);
        let store = Store::memory();
        anchor(&store, &old_fp, None).await.expect("first start");
        anchor(&store, &new_fp, Some(&old_fp))
            .await
            .expect("another replica confirms");

        let refused = shared
            .reload(unconfirmed(&store), "grid-ca", &third.cert_pem, &third.key_pem)
            .await;
        assert!(
            matches!(refused, Err(ReloadError::Anchor(AnchorError::Mismatch { .. }))),
            "{refused:?}"
        );
        assert!(shared.signer().is_none(), "the kept CA is no longer the recorded one");
    }

    #[tokio::test]
    async fn a_refused_new_ca_keeps_signing_with_the_recorded_one() {
        let (old_fp, ca) = fresh();
        let (_, other) = fresh();
        let shared = SharedCa::new(ca);
        let store = Store::memory();
        anchor(&store, &old_fp, None).await.expect("first start");

        let refused = shared
            .reload(unconfirmed(&store), "grid-ca", &other.cert_pem, &other.key_pem)
            .await;
        assert!(refused.is_err(), "an unconfirmed CA is refused");
        assert!(shared.signer().is_some(), "the held CA is still the recorded one");
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "follow the held signer through record mismatch and recovery"
    )]
    async fn malformed_material_still_checks_the_held_signer_against_the_record() {
        let (old_fp, ca) = fresh();
        let (new_fp, _) = fresh();
        let shared = SharedCa::new(ca);
        let store = Store::memory();
        anchor(&store, &old_fp, None).await.expect("first start");
        shared
            .reload(unconfirmed(&store), "grid-ca", "not a cert", "not a key")
            .await
            .expect_err("malformed certificate");
        assert!(shared.signer().is_some(), "matching record keeps the held CA usable");
        anchor(&store, &new_fp, Some(&old_fp))
            .await
            .expect("another replica rotates");
        shared
            .reload(unconfirmed(&store), "grid-ca", "not a cert", "not a key")
            .await
            .expect_err("malformed certificate after record change");
        assert!(
            shared.signer().is_none(),
            "bad material cannot bypass the changed record"
        );
        anchor(&store, &old_fp, Some(&new_fp))
            .await
            .expect("restore the record");
        shared
            .reload(unconfirmed(&store), "grid-ca", "not a cert", "not a key")
            .await
            .expect_err("malformed certificate after record recovery");
        assert!(
            shared.signer().is_some(),
            "the held CA can sign again when its record matches"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "exercise missing certificate and key through mismatch and recovery"
    )]
    async fn unreadable_ca_files_still_check_the_held_signer_against_the_record() {
        let (old_fp, ca) = fresh();
        let (new_fp, _) = fresh();
        let dir = std::env::temp_dir().join(format!("ca-reload-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).expect("test directory");
        let cert_path = dir.join("ca.crt");
        let key_path = dir.join("ca.key");
        let missing = dir.join("missing");
        std::fs::write(&cert_path, &ca.cert_pem).expect("certificate");
        std::fs::write(&key_path, &ca.key_pem).expect("key");
        let shared = SharedCa::new(ca);
        let store = Store::memory();
        anchor(&store, &old_fp, None).await.expect("first start");

        for (cert, key) in [(&missing, &key_path), (&cert_path, &missing)] {
            let initial = shared
                .reload_from_files(unconfirmed(&store), "grid-ca", cert, key)
                .await;
            assert!(matches!(initial, Err(ReloadError::Read(_))), "{initial:?}");
            assert!(shared.signer().is_some(), "matching record keeps the held CA usable");
            anchor(&store, &new_fp, Some(&old_fp))
                .await
                .expect("another replica rotates");
            let reload = shared
                .reload_from_files(unconfirmed(&store), "grid-ca", cert, key)
                .await;
            assert!(matches!(reload, Err(ReloadError::Read(_))), "{reload:?}");
            assert!(
                shared.signer().is_none(),
                "an unreadable mount cannot bypass the changed record"
            );
            anchor(&store, &old_fp, Some(&new_fp))
                .await
                .expect("restore the record");
            shared
                .reload_from_files(unconfirmed(&store), "grid-ca", &cert_path, &key_path)
                .await
                .expect("recover");
            assert!(shared.signer().is_some(), "a matching record restores signing");
        }
        std::fs::remove_dir_all(&dir).expect("remove test files");
    }

    #[tokio::test]
    async fn a_missing_record_is_restored_to_the_held_ca() {
        let (fingerprint, ca) = fresh();
        let (cert, key) = (ca.cert_pem.clone(), ca.key_pem.clone());
        let shared = SharedCa::new(ca);
        let store = Store::memory();

        shared
            .reload(unconfirmed(&store), "grid-ca", &cert, &key)
            .await
            .expect("an empty record takes the held CA");
        assert!(shared.signer().is_some(), "the held CA is now the recorded one");
        assert_eq!(store.recorded_ca().await.expect("read"), Some(fingerprint));
    }
}
