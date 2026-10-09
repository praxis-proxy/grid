//! Which renewals and seeds a site's enrollment record admits, the same for every backend.
//! The caller has already proven possession of an unexpired leaf for the name.

use time::{Duration, OffsetDateTime};
use uuid::Uuid;

/// Slack when comparing a leaf's `notBefore` with record times: the leaf's own backdate
/// plus the same again for clock skew between the signer and the database. Minutes, so
/// a stolen leaf cannot pass for one issued in another incarnation of the record.
pub const RECORD_SKEW: Duration = Duration::minutes(10);

/// An authenticated renewal request.
#[derive(Debug, Clone)]
pub struct Renewal {
    /// The site the presented leaf names.
    pub site_name: String,
    /// Key digest of the leaf presented over mTLS.
    pub presented_key: String,
    /// Key digest of the CSR, the key the new leaf certifies.
    pub requested_key: String,
    /// The presented leaf's `notBefore`.
    pub presented_not_before: OffsetDateTime,
}

/// A name's enrollment record, as renewal reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Held {
    /// The enrollment record's identifier.
    pub id: Uuid,
    /// The key the latest issued leaf certifies.
    pub current_key: String,
    /// The key it replaced, kept so a renewal whose response was lost can retry.
    pub previous_key: Option<String>,
    /// When the record last changed.
    pub recorded_at: OffsetDateTime,
    /// When this incarnation of the record began: its insert, or a seed reset.
    pub epoch_at: OffsetDateTime,
    /// Renewal refused until a grid-admin deletes the record, after a fork.
    pub frozen: bool,
    /// The last bootstrap seed applied, for a reserved name.
    pub seed_generation: Option<u64>,
}

impl Held {
    /// A record written at `now`.
    #[must_use]
    pub fn new(id: Uuid, current_key: String, previous_key: Option<String>, now: OffsetDateTime) -> Self {
        Self {
            id,
            current_key,
            previous_key,
            recorded_at: now,
            epoch_at: now,
            frozen: false,
            seed_generation: None,
        }
    }
}

/// An admitted, signed renewal.
#[derive(Debug, Clone)]
pub struct Renewed {
    /// The enrollment record's identifier.
    pub id: Uuid,
    /// What the renewal did to the record.
    pub action: RenewAction,
    /// The key the record held as current before.
    pub replaced_key: String,
    /// The signed certificate.
    pub issued: super::Issued,
}

/// What an admitted renewal does to the record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RenewAction {
    /// The current key renews: the requested key becomes current.
    Rotate,
    /// A retry after a lost response: re-sign the current key, record unchanged.
    Resign,
}

/// Why a renewal was refused. Logged, never told to the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// No record holds the name.
    #[error("no enrollment holds this site name")]
    UnknownSite,
    /// The CSR reuses a key the record already holds.
    #[error("the certificate request reuses a held key")]
    KeyReused,
    /// A valid leaf the record no longer holds asked for a key it does not hold: two
    /// holders of one identity. The record is frozen.
    #[error("a replaced key asked for a new key; the record is now frozen")]
    Forked,
    /// The record was frozen by an earlier fork.
    #[error("the record is frozen until a grid-admin deletes it")]
    Frozen,
    /// The leaf predates this incarnation of the record, which a recovery began.
    #[error("the presented certificate predates the site's current enrollment")]
    Superseded,
    /// The leaf is newer than the record, as after the database was restored.
    #[error("the presented certificate is newer than the site's record")]
    RecordBehind,
}

impl Refusal {
    /// Metric and log label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownSite => "unknown_site",
            Self::KeyReused => "key_reused",
            Self::Forked => "forked",
            Self::Frozen => "frozen",
            Self::Superseded => "superseded",
            Self::RecordBehind => "record_behind",
        }
    }
}

/// Decide a renewal against the name's record.
///
/// # Errors
///
/// Returns the [`Refusal`] when the record does not admit the presented key.
/// [`Refusal::Forked`] obliges the caller to freeze the record.
pub fn decide(held: Option<&Held>, renewal: &Renewal) -> Result<RenewAction, Refusal> {
    let held = held.ok_or(Refusal::UnknownSite)?;
    if held.frozen {
        return Err(Refusal::Frozen);
    }
    let rolls_back =
        renewal.presented_key == held.current_key && held.previous_key.as_ref() == Some(&renewal.requested_key);
    if renewal.requested_key == renewal.presented_key || rolls_back {
        return Err(Refusal::KeyReused);
    }
    if renewal.presented_key == held.current_key {
        return Ok(RenewAction::Rotate);
    }
    if held.previous_key.as_ref() == Some(&renewal.presented_key) && renewal.requested_key == held.current_key {
        return Ok(RenewAction::Resign);
    }
    // A leaf from before a recovery, or from after a restored record, is not a fork.
    if renewal.presented_not_before.saturating_add(RECORD_SKEW) < held.epoch_at {
        return Err(Refusal::Superseded);
    }
    if renewal.presented_not_before > held.recorded_at.saturating_add(RECORD_SKEW) {
        return Err(Refusal::RecordBehind);
    }
    // A site renews once per two thirds of a leaf's lifetime, so it never presents a
    // still-valid leaf older than the one it replaced: any other key is a second holder.
    Err(Refusal::Forked)
}

/// A reserved name's identity as bootstrap last vouched for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedRecord {
    /// The site.
    pub site_name: String,
    /// Key digest of the identity bootstrap issued.
    pub key_sha256: String,
    /// Increases with every identity bootstrap issues.
    pub generation: u64,
    /// The seeded leaf's `notBefore`, where the record's incarnation begins.
    pub issued_at: OffsetDateTime,
}

/// What applying a seed did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Seeded {
    /// The reserved name had no record and now has one.
    Registered,
    /// A newer seed replaced the record's key, clearing any freeze.
    Reset {
        /// The key the record held.
        replaced_key: String,
    },
    /// A newer seed for a key the record already holds, current or previous: only its
    /// generation is recorded, so it never clears a freeze or rolls a renewal back.
    Acknowledged,
    /// The record already reflects this seed.
    Unchanged,
    /// The seed is older than the one the record applied: a rollback or a replay.
    Older,
    /// A record that is not reserved holds the name. Left alone.
    NotReserved,
}

/// What a seed does to a record, `held` with whether it is reserved.
#[must_use]
pub fn seed_decision(held: Option<(&Held, bool)>, seed: &SeedRecord) -> Seeded {
    match held {
        None => Seeded::Registered,
        Some((_, false)) => Seeded::NotReserved,
        Some((held, true)) if held.seed_generation.is_some_and(|applied| applied > seed.generation) => Seeded::Older,
        Some((held, true)) if held.seed_generation == Some(seed.generation) => Seeded::Unchanged,
        // A seed resets only to a key the record never held. Re-seeding the current key
        // must not clear a freeze, and one read before the site renewed must not roll it back.
        Some((held, true))
            if held.current_key == seed.key_sha256 || held.previous_key.as_ref() == Some(&seed.key_sha256) =>
        {
            Seeded::Acknowledged
        },
        Some((held, true)) => Seeded::Reset {
            replaced_key: held.current_key.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn renewal(presented: &str, requested: &str) -> Renewal {
        issued_at(
            presented,
            requested,
            OffsetDateTime::now_utc().saturating_sub(Duration::minutes(5)),
        )
    }

    fn issued_at(presented: &str, requested: &str, not_before: OffsetDateTime) -> Renewal {
        Renewal {
            site_name: "site-a".to_owned(),
            presented_key: presented.to_owned(),
            requested_key: requested.to_owned(),
            presented_not_before: not_before,
        }
    }

    /// A stray valid leaf is a fork only inside this incarnation, within the skew, at both edges.
    #[test]
    #[expect(clippy::too_many_lines, reason = "one row per edge")]
    fn a_stray_leaf_forks_only_within_the_record_incarnation() {
        let epoch = OffsetDateTime::now_utc().saturating_sub(Duration::days(10));
        let record = Held {
            epoch_at: epoch,
            recorded_at: epoch.saturating_add(Duration::days(1)),
            ..held("k1", Some("k0"))
        };
        let second = Duration::seconds(1);
        let floor = epoch.saturating_sub(RECORD_SKEW);
        let ceiling = record.recorded_at.saturating_add(RECORD_SKEW);
        let cases = [
            (
                "before the incarnation",
                floor.saturating_sub(second),
                Err(Refusal::Superseded),
            ),
            (
                "just inside the floor",
                floor.saturating_add(second),
                Err(Refusal::Forked),
            ),
            (
                "just inside the ceiling",
                ceiling.saturating_sub(second),
                Err(Refusal::Forked),
            ),
            (
                "after the record",
                ceiling.saturating_add(second),
                Err(Refusal::RecordBehind),
            ),
        ];
        for (name, not_before, expected) in cases {
            assert_eq!(
                decide(Some(&record), &issued_at("kx", "k9", not_before)),
                expected,
                "{name}"
            );
        }
        assert!(RECORD_SKEW <= Duration::minutes(10), "the window stays minutes wide");
    }

    fn held(current: &str, previous: Option<&str>) -> Held {
        Held::new(
            Uuid::nil(),
            current.to_owned(),
            previous.map(str::to_owned),
            OffsetDateTime::now_utc(),
        )
    }

    /// A decision's name, record, renewal, and outcome.
    type Case<'held> = (&'static str, Option<&'held Held>, Renewal, Result<RenewAction, Refusal>);

    #[test]
    #[expect(clippy::too_many_lines, reason = "one row per decision")]
    fn decisions() {
        let record = held("k1", Some("k0"));
        let frozen = Held {
            frozen: true,
            ..record.clone()
        };
        let cases: [Case<'_>; 9] = [
            (
                "the current key renews",
                Some(&record),
                renewal("k1", "k2"),
                Ok(RenewAction::Rotate),
            ),
            (
                "a lost response retries",
                Some(&record),
                renewal("k0", "k1"),
                Ok(RenewAction::Resign),
            ),
            (
                "the replaced key forks",
                Some(&record),
                renewal("k0", "k9"),
                Err(Refusal::Forked),
            ),
            (
                "an older valid key forks",
                Some(&record),
                renewal("kx", "k9"),
                Err(Refusal::Forked),
            ),
            (
                "the presented key again",
                Some(&record),
                renewal("k1", "k1"),
                Err(Refusal::KeyReused),
            ),
            (
                "the replaced key again",
                Some(&record),
                renewal("k1", "k0"),
                Err(Refusal::KeyReused),
            ),
            ("no record", None, renewal("k1", "k2"), Err(Refusal::UnknownSite)),
            (
                "a frozen record",
                Some(&frozen),
                renewal("k1", "k2"),
                Err(Refusal::Frozen),
            ),
            (
                "a frozen record retried",
                Some(&frozen),
                renewal("k0", "k1"),
                Err(Refusal::Frozen),
            ),
        ];
        for (name, held, renewal, expected) in cases {
            assert_eq!(decide(held, &renewal), expected, "{name}");
        }
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "one assertion per seed outcome")]
    fn seeds() {
        let seed = |generation| SeedRecord {
            site_name: "hub".to_owned(),
            key_sha256: "kb".to_owned(),
            generation,
            issued_at: OffsetDateTime::now_utc(),
        };
        let applied = Held {
            seed_generation: Some(5),
            ..held("k1", None)
        };
        assert_eq!(seed_decision(None, &seed(1)), Seeded::Registered, "no record");
        assert_eq!(
            seed_decision(Some((&applied, true)), &seed(5)),
            Seeded::Unchanged,
            "same seed"
        );
        assert_eq!(
            seed_decision(Some((&applied, true)), &seed(4)),
            Seeded::Older,
            "older seed"
        );
        assert_eq!(
            seed_decision(Some((&applied, true)), &seed(6)),
            Seeded::Reset {
                replaced_key: "k1".to_owned()
            },
            "newer seed"
        );
        let holding = Held {
            seed_generation: Some(5),
            frozen: true,
            ..held("kb", None)
        };
        assert_eq!(
            seed_decision(Some((&holding, true)), &seed(9)),
            Seeded::Acknowledged,
            "re-seeding the held key leaves a freeze in place"
        );
        let renewed_away = Held {
            seed_generation: Some(5),
            ..held("k2", Some("kb"))
        };
        assert_eq!(
            seed_decision(Some((&renewed_away, true)), &seed(9)),
            Seeded::Acknowledged,
            "a seed read before the hub renewed does not roll it back"
        );
        assert_eq!(
            seed_decision(Some((&applied, false)), &seed(9)),
            Seeded::NotReserved,
            "a spoke's record"
        );
    }
}
