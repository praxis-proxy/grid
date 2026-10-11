//! Process-scoped evidence retained across transport and poller replacement.

use std::sync::{Mutex, PoisonError};

use grid_signals::{Ingested, feedback::IngestReport};

/// Gateway reporter shared by all pollers targeting its local operator.
pub struct Reporter {
    /// Verified operator identity; acknowledgments never cross this boundary.
    pub(crate) target: String,
    /// Synchronously folded observations, released before network operations.
    state: Mutex<State>,
}

/// Bounded observation state for a single gateway process.
struct State {
    /// Identity and latest allocated sequence.
    head: IngestReport,
    /// Cumulative skew durably acknowledged.
    acknowledged: i64,
    /// Immutable pending rejection, or supersedable pending clean report.
    pending: Option<IngestReport>,
    /// Arithmetic cannot represent further observations.
    exhausted: bool,
}

impl Reporter {
    /// Create one process identity using the installed TLS crypto provider.
    ///
    /// # Errors
    /// Returns an error when process entropy is unavailable.
    pub fn new(target: String) -> Result<Self, crate::MtlsError> {
        let (replica, epoch) = identity()?;
        Ok(Self {
            target,
            state: Mutex::new(State {
                head: IngestReport {
                    replica,
                    epoch,
                    sequence: 0,
                    skew: 0,
                    rejected: false,
                },
                acknowledged: 0,
                pending: None,
                exhausted: false,
            }),
        })
    }

    /// Fold an actual ingest, or retry only pending bad evidence after a failed fetch.
    pub(crate) fn observe(&self, ingested: Option<Ingested>) -> Option<IngestReport> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(ingested) = ingested else {
            return state.pending.clone().filter(|report| report.rejected);
        };
        let total = i64::try_from(ingested.skewed)
            .ok()
            .and_then(|skew| state.head.skew.checked_add(skew));
        state.exhausted |= total.is_none() || total == Some(i64::MAX);
        state.head.skew = total.unwrap_or(i64::MAX);
        let rejected = state.exhausted || state.head.skew > state.acknowledged;
        if state
            .pending
            .as_ref()
            .is_some_and(|pending| pending.rejected || !rejected)
        {
            return state.pending.clone();
        }
        state.allocate(rejected)
    }

    /// Accept only an acknowledgment of the currently pending immutable report.
    pub(crate) fn acknowledge(&self, report: &IngestReport) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.pending.as_ref() == Some(report) {
            state.acknowledged = report.skew;
            state.pending = None;
            if state.head.skew > report.skew || state.exhausted {
                state.head.sequence = state.head.sequence.saturating_add(1);
                state.head.rejected = true;
                state.pending = Some(state.head.clone());
            }
        }
    }
}

impl State {
    /// Reserve the highest sequence for a terminal rejected report.
    fn allocate(&mut self, rejected: bool) -> Option<IngestReport> {
        let sequence = self.head.sequence.checked_add(1);
        self.exhausted |= sequence.is_none() || sequence == Some(i64::MAX);
        self.head.sequence = sequence.unwrap_or(i64::MAX);
        self.head.rejected = rejected || self.exhausted;
        self.pending = Some(self.head.clone());
        self.pending.clone()
    }
}

/// Cryptographic process identity with a bounded diagnostic replica label.
fn identity() -> Result<(String, String), crate::MtlsError> {
    let provider = rustls::crypto::CryptoProvider::get_default().ok_or(crate::MtlsError::NoProvider)?;
    let mut bytes = [0_u8; 16];
    provider
        .secure_random
        .fill(&mut bytes)
        .map_err(|_entropy| crate::MtlsError::Entropy)?;
    let epoch: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    let replica = std::env::var("HOSTNAME")
        .ok()
        .filter(|name| {
            !name.is_empty()
                && name.len() <= 128
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        })
        .unwrap_or_else(|| epoch.clone());
    Ok((replica, epoch))
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn lost_bad_ack_survives_clean_and_none_then_recovers() {
        let reporter = reporter();
        let bad = reporter.observe(Some(ingest(1))).expect("bad");
        assert_eq!(reporter.observe(Some(ingest(0))), Some(bad.clone()));
        assert_eq!(reporter.observe(None), Some(bad.clone()));
        reporter.acknowledge(&bad);
        assert!(reporter.observe(None).is_none(), "failure invents no clean");
        let clean = reporter.observe(Some(ingest(0))).expect("clean");
        assert!(!clean.rejected && clean.sequence > bad.sequence);
    }

    #[test]
    fn pending_clean_is_superseded_and_old_ack_cannot_clear_bad() {
        let reporter = reporter();
        let clean = reporter.observe(Some(ingest(0))).expect("clean");
        let bad = reporter.observe(Some(ingest(2))).expect("bad");
        assert!(bad.rejected && bad.sequence > clean.sequence);
        reporter.acknowledge(&clean);
        assert_eq!(reporter.observe(None), Some(bad));
    }

    #[test]
    fn ack_uses_snapshot_and_failed_fetch_drains_later_skew() {
        let reporter = reporter();
        let first = reporter.observe(Some(ingest(1))).expect("first");
        assert_eq!(reporter.observe(Some(ingest(2))), Some(first.clone()));
        reporter.acknowledge(&first);
        let second = reporter.observe(None).expect("accumulated bad");
        assert_eq!(second.skew, 3, "acknowledging one never acknowledges three");
        assert!(second.rejected && second.sequence > first.sequence);
    }

    #[test]
    fn top_sequence_is_reserved_for_terminal_rejection() {
        let reporter = reporter();
        reporter.state.lock().expect("state").head.sequence = i64::MAX.saturating_sub(2);
        let clean = reporter.observe(Some(ingest(0))).expect("penultimate clean");
        let terminal = reporter.observe(Some(ingest(1))).expect("terminal bad");
        assert_eq!(terminal.sequence, i64::MAX);
        assert!(terminal.rejected);
        reporter.acknowledge(&clean);
        assert_eq!(reporter.observe(None), Some(terminal.clone()));
        reporter.acknowledge(&terminal);
        assert!(reporter.observe(Some(ingest(0))).expect("latched").rejected);
    }

    #[test]
    fn cumulative_overflow_latches_without_wrapping() {
        let reporter = reporter();
        let report = reporter.observe(Some(ingest(u64::MAX))).expect("overflow");
        assert_eq!(report.skew, i64::MAX);
        assert!(report.rejected);
        reporter.acknowledge(&report);
        assert!(reporter.observe(Some(ingest(0))).expect("latched").rejected);
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// Deterministic reporter without environment or provider coupling.
    fn reporter() -> Reporter {
        Reporter {
            target: "operator".to_owned(),
            state: Mutex::new(State {
                head: IngestReport {
                    replica: "gateway".to_owned(),
                    epoch: "a".repeat(32),
                    sequence: 0,
                    skew: 0,
                    rejected: false,
                },
                acknowledged: 0,
                pending: None,
                exhausted: false,
            }),
        }
    }

    /// One completed ingest's skew count.
    fn ingest(skewed: u64) -> Ingested {
        Ingested {
            skewed,
            ..Ingested::default()
        }
    }
}
