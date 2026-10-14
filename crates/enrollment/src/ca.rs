//! The signing CA, swappable while the service runs.
//!
//! Bootstrap can regenerate or restore the CA Secret under a running pod. The
//! kubelet updates the mounted files, and [`SharedCa::reload`] swaps the new CA
//! in, so new site certificates are never signed by a CA the grid has replaced.

use std::{
    collections::BTreeSet,
    sync::{Arc, PoisonError, RwLock},
};

use certs::CaCert;

/// The current signing CA. A handler takes one [`SharedCa::current`] snapshot per
/// request, so the certificate it signs and the CA it returns always match.
#[derive(Debug)]
pub struct SharedCa(RwLock<Arc<CaCert>>);

/// Why a CA reload was refused. The current CA stays in use.
#[derive(Debug, thiserror::Error)]
pub enum ReloadError {
    /// The new certificate could not be fingerprinted.
    #[error("CA certificate: {0}")]
    Certificate(#[from] certs::VerifyError),
    /// The new certificate and key do not load as a CA.
    #[error("CA material: {0}")]
    Load(#[from] certs::GenerateError),
}

impl SharedCa {
    /// Hold `ca` as the current signing CA.
    #[must_use]
    pub fn new(ca: CaCert) -> Self {
        Self(RwLock::new(Arc::new(ca)))
    }

    /// The current signing CA.
    #[must_use]
    pub fn current(&self) -> Arc<CaCert> {
        Arc::clone(&self.0.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// Swap in the CA from `cert_pem` and `key_pem` when its certificate differs
    /// from the current one. Returns the old and new fingerprints on a swap, or
    /// `None` when the CA is unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`ReloadError`] when the new material does not load. The current
    /// CA is kept.
    pub fn reload(
        &self,
        common_name: &str,
        cert_pem: &str,
        key_pem: &str,
    ) -> Result<Option<(String, String)>, ReloadError> {
        let incoming = certs::canonical_fingerprint(cert_pem)?;
        let current = certs::canonical_fingerprint(&self.current().cert_pem)?;
        if incoming == current {
            return Ok(None);
        }
        let ca = certs::load_ca(common_name, key_pem, cert_pem)?;
        *self.0.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(ca);
        Ok(Some((current, incoming)))
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
    use super::{CaAction, CaCopy, SharedCa, ca_action};

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

    #[test]
    fn an_unchanged_ca_is_not_swapped() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let (cert, key) = (ca.cert_pem.clone(), ca.key_pem.clone());
        let shared = SharedCa::new(ca);
        assert!(
            shared.reload("grid-ca", &cert, &key).expect("reload").is_none(),
            "same cert, no swap"
        );
    }

    #[test]
    fn a_new_ca_is_swapped_in_and_signs() {
        let shared = SharedCa::new(certs::generate_ca("grid-ca").expect("old ca"));
        let next = certs::generate_ca("grid-ca").expect("new ca");
        let swapped = shared.reload("grid-ca", &next.cert_pem, &next.key_pem).expect("reload");
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

    #[test]
    fn a_bad_reload_keeps_the_current_ca() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let original = ca.cert_pem.clone();
        let other = certs::generate_ca("grid-ca").expect("other");
        let shared = SharedCa::new(ca);

        assert!(shared.reload("grid-ca", "not a cert", "not a key").is_err(), "garbage");
        let mismatched = shared.reload("grid-ca", &other.cert_pem, "not a key");
        assert!(mismatched.is_err(), "a cert without its key");
        assert_eq!(shared.current().cert_pem, original, "the current CA is kept");
    }
}
