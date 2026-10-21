//! Provider revision and fenced publication wire contract.

use std::{
    borrow::Cow,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::exposition::{Metric, PROVIDER_LABEL, SITE_LABEL};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum future publisher reservation, matching SWIM's node-generation lease skew.
pub const MAX_PUBLISHER_EPOCH_SKEW: Duration = Duration::from_mins(10);

/// Provider spec generation, bound to one complete observation batch.
pub const METRIC: &str = "grid_provider_config_generation";
/// Provider object lifetime.
pub const UID_LABEL: &str = "provider_uid";
/// Durable publisher fencing token.
pub const EPOCH_LABEL: &str = "publisher_epoch";
/// Strictly increasing publication number within the publisher epoch.
pub const SEQUENCE_LABEL: &str = "batch_sequence";
/// Number of nonmetadata rows in this provider batch.
pub const SAMPLES_LABEL: &str = "batch_samples";
/// Largest supported generation, below the binary64 exact-integer boundary.
pub const MAX_GENERATION: u64 = 9_007_199_254_740_991;
/// Maximum payload rows in one provider publication.
pub const MAX_BATCH_SAMPLES: usize = 4_095;
/// Maximum bytes in an identity label.
const MAX_ID_BYTES: usize = 256;

/// A complete provider publication's identity and ordering metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Revision {
    /// Number of nonmetadata rows expected in the publication.
    pub batch_samples: usize,
    /// Publication sequence within the publisher epoch.
    pub batch_sequence: u64,
    /// Provider spec generation.
    pub generation: u64,
    /// Durable publisher fencing token.
    pub publisher_epoch: u64,
    /// Common observation timestamp for every row in the publication.
    pub timestamp_ms: i64,
    /// Provider object UID.
    pub uid: String,
}

impl Revision {
    /// Publication order, independent of provider lifetime and spec generation.
    #[must_use]
    pub const fn order(&self) -> (u64, u64) {
        (self.publisher_epoch, self.batch_sequence)
    }
}

/// Whether a nanosecond publisher reservation is within the receiver's wall-clock bound.
#[must_use]
pub fn valid_epoch(epoch: u64, now: SystemTime) -> bool {
    now.duration_since(UNIX_EPOCH).is_ok_and(|since| {
        let limit = u64::try_from(since.saturating_add(MAX_PUBLISHER_EPOCH_SKEW).as_nanos()).unwrap_or(u64::MAX);
        epoch <= limit
    })
}

/// Validate exact integer values and the complete metadata label vocabulary.
///
/// Unknown or duplicate labels are rejected rather than interpreted as another
/// source of authority. Generation must use canonical unsigned decimal text.
#[must_use]
pub fn parse(metric: &Metric<'_>) -> Option<Revision> {
    if metric.name() != METRIC {
        return None;
    }
    let [site, provider, uid, epoch, sequence, samples] = metadata_labels(metric)?;
    if !valid_target(&site) || !valid_target(&provider) || !valid_id(&uid) {
        return None;
    }
    let generation = positive_integer(metric.value_text())?;
    let batch_samples = usize::try_from(decimal_integer(&samples)?).ok()?;
    if generation > MAX_GENERATION || batch_samples > MAX_BATCH_SAMPLES {
        return None;
    }
    Some(Revision {
        batch_samples,
        batch_sequence: positive_integer(&sequence)?,
        generation,
        publisher_epoch: positive_integer(&epoch)?,
        timestamp_ms: metric.timestamp_ms()?,
        uid: uid.into_owned(),
    })
}

/// Extract exactly the six metadata labels without losing duplicates.
fn metadata_labels<'text>(metric: &Metric<'text>) -> Option<[Cow<'text, str>; 6]> {
    let mut labels = [None, None, None, None, None, None];
    for (name, value) in metric.labels() {
        let slot = match name {
            SITE_LABEL => labels.get_mut(0)?,
            PROVIDER_LABEL => labels.get_mut(1)?,
            UID_LABEL => labels.get_mut(2)?,
            EPOCH_LABEL => labels.get_mut(3)?,
            SEQUENCE_LABEL => labels.get_mut(4)?,
            SAMPLES_LABEL => labels.get_mut(5)?,
            _ => return None,
        };
        if slot.replace(value).is_some() {
            return None;
        }
    }
    let [
        Some(site),
        Some(provider),
        Some(uid),
        Some(epoch),
        Some(sequence),
        Some(samples),
    ] = labels
    else {
        return None;
    };
    Some([site, provider, uid, epoch, sequence, samples])
}

/// Whether an identity is bounded and safe in a store key or exposition label.
#[must_use]
pub fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

/// Existing routing-key grammar, without narrowing provider aliases to UIDs.
#[must_use]
pub fn valid_target(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_ID_BYTES && !value.chars().any(|ch| ch.is_control() || ch == '/')
}

/// Parse a positive canonical decimal integer without rounding through a float.
#[must_use]
pub fn positive_integer(value: &str) -> Option<u64> {
    decimal_integer(value).filter(|number| *number != 0)
}

/// Parse an unsigned canonical decimal integer, including zero.
fn decimal_integer(value: &str) -> Option<u64> {
    if value.is_empty()
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return None;
    }
    value.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exposition;

    #[test]
    fn publisher_epoch_uses_the_inclusive_local_wall_clock_bound() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        let limit = 1_600_000_000_000;
        assert!(
            valid_epoch(1, now),
            "old reservations remain valid for long-lived publishers"
        );
        assert!(valid_epoch(limit, now), "the SWIM skew boundary is inclusive");
        assert!(
            !valid_epoch(limit + 1, now),
            "future reservations cannot poison the fence"
        );
        assert!(
            !valid_epoch(u64::MAX, now),
            "a maximal epoch is not a valid current reservation"
        );
    }

    #[test]
    fn parses_exact_revision_and_order() {
        let Some(revision) = read("7", "") else {
            std::process::abort()
        };
        assert_eq!(revision.generation, 7);
        assert_eq!(revision.order(), (1, 2));
        assert_eq!(revision.batch_samples, 4);
        assert_eq!(revision.timestamp_ms, 2_000);
        assert_eq!(revision.uid, "u1");
    }

    #[test]
    fn generation_requires_canonical_exact_positive_decimal() {
        for value in [
            "0",
            "-1",
            "+1",
            "01",
            "1.0",
            "1e0",
            "9007199254740992",
            "9007199254740990.1",
            "NaN",
            "inf",
        ] {
            assert!(read(value, "").is_none(), "must reject {value}");
        }
        assert_eq!(
            read("9007199254740991", "").map(|revision| revision.generation),
            Some(MAX_GENERATION)
        );
    }

    #[test]
    fn unknown_and_repeated_metadata_labels_are_rejected() {
        for label in [
            ",unknown=\"x\"",
            ",provider_uid=\"u1\"",
            ",grid_site=\"b\"",
            ",batch_samples=\"4\"",
        ] {
            assert!(read("7", label).is_none(), "must reject {label}");
        }
    }

    #[test]
    fn metadata_requires_every_label_and_timestamp() {
        let valid = line("7", "");
        for removed in [
            "grid_site=\"b\",",
            "grid_provider=\"m\",",
            "provider_uid=\"u1\",",
            "publisher_epoch=\"1\",",
            "batch_sequence=\"2\",",
            ",batch_samples=\"4\"",
            " 2000",
        ] {
            let changed = valid.replace(removed, "");
            assert!(
                exposition::parse(&changed).and_then(|metric| parse(&metric)).is_none(),
                "requires {removed}"
            );
        }
    }

    #[test]
    fn ordering_tokens_and_sample_counts_are_bounded() {
        let valid = line("7", "");
        for (from, to) in [
            ("publisher_epoch=\"1\"", "publisher_epoch=\"0\""),
            ("batch_sequence=\"2\"", "batch_sequence=\"18446744073709551616\""),
            ("batch_samples=\"4\"", "batch_samples=\"4096\""),
            ("batch_samples=\"4\"", "batch_samples=\"-1\""),
        ] {
            let changed = valid.replace(from, to);
            assert!(
                exposition::parse(&changed).and_then(|metric| parse(&metric)).is_none(),
                "rejects {to}"
            );
        }
        assert_eq!(positive_integer("18446744073709551615"), Some(u64::MAX));
        let empty = valid.replace("batch_samples=\"4\"", "batch_samples=\"0\"");
        assert_eq!(
            exposition::parse(&empty)
                .and_then(|metric| parse(&metric))
                .map(|revision| revision.batch_samples),
            Some(0)
        );
    }

    #[test]
    fn identities_reject_unsafe_or_unbounded_text() {
        for value in ["", " ", "with space", "a/b", "x\ny", "u\"1", "é"] {
            assert!(!valid_id(value), "rejects {value:?}");
        }
        assert!(valid_id("kubernetes-uid_1.foo:bar"));
        assert!(valid_id(&"a".repeat(256)));
        assert!(!valid_id(&"a".repeat(257)));
    }

    #[test]
    fn decimal_round_trips_cover_safe_integer_boundaries() {
        for number in (1..10_000).chain([MAX_GENERATION.saturating_sub(1), MAX_GENERATION, u64::MAX]) {
            assert_eq!(
                positive_integer(&number.to_string()),
                Some(number),
                "exact integer round trip"
            );
        }
    }

    #[test]
    fn provider_alias_grammar_is_not_narrowed_to_uid_tokens() {
        let text = line("7", "").replace("grid_provider=\"m\"", "grid_provider=\"pool+gpu\"");
        assert!(exposition::parse(&text).and_then(|metric| parse(&metric)).is_some());
        assert!(valid_target("pool+gpu"));
        assert!(!valid_target("pool/gpu"));
        assert!(!valid_target("  "));
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    fn read(value: &str, extra: &str) -> Option<Revision> {
        exposition::parse(&line(value, extra)).and_then(|metric| parse(&metric))
    }

    fn line(value: &str, extra: &str) -> String {
        format!(
            "{METRIC}{{grid_site=\"b\",grid_provider=\"m\",provider_uid=\"u1\",publisher_epoch=\"1\",batch_sequence=\"2\",batch_samples=\"4\"{extra}}} {value} 2000"
        )
    }
}
