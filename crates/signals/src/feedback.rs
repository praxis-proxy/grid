//! Bounded gateway ingest evidence carried over the local signals channel.

/// Authenticated POST endpoint for completed gateway ingests.
pub const INGEST_PATH: &str = "/v1/site/signals/ingest";
/// Versioned scalar request headers, in their wire order.
pub const REPORT_HEADERS: [&str; 6] = [
    "x-grid-ingest-version",
    "x-grid-ingest-replica",
    "x-grid-ingest-epoch",
    "x-grid-ingest-sequence",
    "x-grid-ingest-skew",
    "x-grid-ingest-rejected",
];
/// Process epoch acknowledged after durable storage.
pub const ACK_EPOCH: &str = "x-grid-ingest-epoch";
/// Sequence acknowledged after durable storage.
pub const ACK_SEQUENCE: &str = "x-grid-ingest-sequence";

/// One completed observation, with cumulative evidence retained until acknowledged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngestReport {
    /// Diagnostic replica label, never an authorization claim.
    pub replica: String,
    /// Random process identity, shared across poller reloads.
    pub epoch: String,
    /// Monotonically increasing report sequence.
    pub sequence: i64,
    /// Cumulative age rejections during this process.
    pub skew: i64,
    /// Whether unacknowledged rejection or arithmetic exhaustion remains.
    pub rejected: bool,
}

impl IngestReport {
    /// Whether every scalar fits the fixed wire bounds.
    #[must_use]
    pub fn valid(&self) -> bool {
        !self.replica.is_empty()
            && self.replica.len() <= 128
            && self
                .replica
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
            && self.epoch.len() == 32
            && self
                .epoch
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            && self.sequence > 0
            && self.skew >= 0
            && (self.rejected || (self.sequence < i64::MAX && self.skew < i64::MAX))
    }

    /// Values corresponding to [`REPORT_HEADERS`].
    #[must_use]
    pub fn values(&self) -> [String; 6] {
        [
            "1".to_owned(),
            self.replica.clone(),
            self.epoch.clone(),
            self.sequence.to_string(),
            self.skew.to_string(),
            self.rejected.to_string(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_bounds_and_terminal_values_are_enforced() {
        let valid = IngestReport {
            replica: "gateway-1".to_owned(),
            epoch: "a".repeat(32),
            sequence: 1,
            skew: 0,
            rejected: false,
        };
        assert!(valid.valid(), "bounded report must be accepted: {valid:?}");
        for invalid in [
            IngestReport {
                replica: "a".repeat(129),
                ..valid.clone()
            },
            IngestReport {
                epoch: "A".repeat(32),
                ..valid.clone()
            },
            IngestReport {
                sequence: 0,
                ..valid.clone()
            },
            IngestReport {
                skew: -1,
                ..valid.clone()
            },
            IngestReport {
                sequence: i64::MAX,
                ..valid.clone()
            },
            IngestReport {
                skew: i64::MAX,
                ..valid.clone()
            },
        ] {
            assert!(!invalid.valid(), "out-of-bounds report must be rejected: {invalid:?}");
        }
        assert!(
            IngestReport {
                sequence: i64::MAX,
                skew: i64::MAX,
                rejected: true,
                ..valid
            }
            .valid(),
            "terminal rejection must remain valid at the numeric bounds"
        );
    }
}
