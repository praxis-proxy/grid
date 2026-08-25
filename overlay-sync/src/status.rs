// SPDX-License-Identifier: MIT

//! Sidecar status tracking.
//!
//! Thread-safe status that tracks the current overlay revision, write
//! timestamps, and degraded state.

use std::{
    sync::{Arc, Mutex},
    time::SystemTime,
};

use serde::Serialize;

// ---------------------------------------------------------------------------
// Sidecar state
// ---------------------------------------------------------------------------

/// Observable sidecar state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SidecarState {
    /// No valid overlay has been written yet.
    Cold,
    /// A valid overlay is being served.
    Ready,
    /// A valid overlay was served, but the source is degraded.
    ReadyDegraded,
}

impl std::fmt::Display for SidecarState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cold => f.write_str("cold"),
            Self::Ready => f.write_str("ready"),
            Self::ReadyDegraded => f.write_str("ready_degraded"),
        }
    }
}

// ---------------------------------------------------------------------------
// Status response
// ---------------------------------------------------------------------------

/// JSON response for the `/status` endpoint.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct StatusResponse {
    /// Watched namespace.
    namespace: String,
    /// Watched `ConfigMap` name.
    config_map: String,
    /// `ConfigMap` data key.
    data_key: String,
    /// Last observed overlay revision (hex), if any.
    observed_overlay_revision: Option<String>,
    /// Last successfully written overlay revision (hex), if any.
    written_overlay_revision: Option<String>,
    /// Timestamp of last observation (RFC 3339), if any.
    last_observed_at: Option<String>,
    /// Timestamp of last write (RFC 3339), if any.
    last_written_at: Option<String>,
    /// Current sidecar state.
    state: String,
    /// Reason for degraded state, if any.
    degraded_reason: Option<String>,
}

// ---------------------------------------------------------------------------
// Shared status
// ---------------------------------------------------------------------------

/// Mutable inner state behind the lock.
#[derive(Debug)]
struct StatusInner {
    /// Current sidecar state.
    state: SidecarState,
    /// Last observed overlay revision.
    observed_revision: Option<String>,
    /// Last written overlay revision.
    written_revision: Option<String>,
    /// Last observation time.
    last_observed_at: Option<SystemTime>,
    /// Last write time.
    last_written_at: Option<SystemTime>,
    /// Degraded reason.
    degraded_reason: Option<String>,
}

/// Thread-safe sidecar status.
#[derive(Clone, Debug)]
pub(crate) struct SharedStatus {
    /// Configuration values for the status response.
    namespace: String,
    /// Watched `ConfigMap` name.
    config_map: String,
    /// `ConfigMap` data key.
    data_key: String,
    /// Mutable state.
    inner: Arc<Mutex<StatusInner>>,
}

impl SharedStatus {
    /// Create a new status tracker in the `Cold` state.
    pub(crate) fn new(namespace: &str, config_map: &str, data_key: &str) -> Self {
        Self {
            namespace: namespace.to_owned(),
            config_map: config_map.to_owned(),
            data_key: data_key.to_owned(),
            inner: Arc::new(Mutex::new(StatusInner {
                state: SidecarState::Cold,
                observed_revision: None,
                written_revision: None,
                last_observed_at: None,
                last_written_at: None,
                degraded_reason: None,
            })),
        }
    }

    /// Record an observed revision.
    pub(crate) fn record_observed(&self, revision: &str) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.observed_revision = Some(revision.to_owned());
            inner.last_observed_at = Some(SystemTime::now());
        }
    }

    /// Record a successful write and transition to Ready.
    pub(crate) fn record_written(&self, revision: &str) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.written_revision = Some(revision.to_owned());
            inner.last_written_at = Some(SystemTime::now());
            inner.state = SidecarState::Ready;
            inner.degraded_reason = None;
        }
    }

    /// Mark the sidecar as degraded with a reason.
    pub(crate) fn mark_degraded(&self, reason: &str) {
        if let Ok(mut inner) = self.inner.lock() {
            if inner.state != SidecarState::Cold {
                inner.state = SidecarState::ReadyDegraded;
            }
            inner.degraded_reason = Some(reason.to_owned());
        }
    }

    /// Clear degraded state (back to Ready if not Cold).
    pub(crate) fn clear_degraded(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            if inner.state == SidecarState::ReadyDegraded {
                inner.state = SidecarState::Ready;
            }
            inner.degraded_reason = None;
        }
    }

    /// Whether the sidecar is ready (has written at least one valid overlay).
    pub(crate) fn is_ready(&self) -> bool {
        self.inner.lock().is_ok_and(|inner| inner.state != SidecarState::Cold)
    }

    /// Get the current written revision, if any.
    pub(crate) fn written_revision(&self) -> Option<String> {
        self.inner.lock().ok().and_then(|inner| inner.written_revision.clone())
    }

    /// Build the JSON status response.
    pub(crate) fn to_response(&self) -> StatusResponse {
        let (state, observed_rev, written_rev, obs_at, write_at, degraded) = match self.inner.lock() {
            Ok(inner) => (
                inner.state.to_string(),
                inner.observed_revision.clone(),
                inner.written_revision.clone(),
                inner.last_observed_at.map(format_rfc3339),
                inner.last_written_at.map(format_rfc3339),
                inner.degraded_reason.clone(),
            ),
            Err(_) => ("unknown".to_owned(), None, None, None, None, None),
        };
        StatusResponse {
            namespace: self.namespace.clone(),
            config_map: self.config_map.clone(),
            data_key: self.data_key.clone(),
            observed_overlay_revision: observed_rev,
            written_overlay_revision: written_rev,
            last_observed_at: obs_at,
            last_written_at: write_at,
            state,
            degraded_reason: degraded,
        }
    }
}

/// Format a `SystemTime` as a simplified RFC 3339 string.
fn format_rfc3339(t: SystemTime) -> String {
    let dur = t.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    let total_secs = dur.as_secs();
    let (days, rem) = (total_secs / 86400, total_secs % 86400);
    let (hours, rem) = (rem / 3600, rem % 3600);
    let (mins, secs) = (rem / 60, rem % 60);

    let (y, m, d) = days_to_date(days);
    format!("{y:04}-{m:02}-{d:02}T{hours:02}:{mins:02}:{secs:02}Z")
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_date(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn initial_state_is_cold() {
        let s = SharedStatus::new("ns", "cm", "key");
        assert!(!s.is_ready());
        let r = s.to_response();
        assert_eq!(r.state, "cold");
    }

    #[test]
    fn becomes_ready_after_write() {
        let s = SharedStatus::new("ns", "cm", "key");
        s.record_observed("abc123");
        s.record_written("abc123");
        assert!(s.is_ready());
        let r = s.to_response();
        assert_eq!(r.state, "ready");
        assert_eq!(r.written_overlay_revision.as_deref(), Some("abc123"));
    }

    #[test]
    fn degraded_after_source_issue() {
        let s = SharedStatus::new("ns", "cm", "key");
        s.record_written("abc123");
        s.mark_degraded("configmap_deleted");
        let r = s.to_response();
        assert_eq!(r.state, "ready_degraded");
        assert_eq!(r.degraded_reason.as_deref(), Some("configmap_deleted"));
    }

    #[test]
    fn clear_degraded_returns_to_ready() {
        let s = SharedStatus::new("ns", "cm", "key");
        s.record_written("abc123");
        s.mark_degraded("api_unavailable");
        s.clear_degraded();
        let r = s.to_response();
        assert_eq!(r.state, "ready");
        assert!(r.degraded_reason.is_none());
    }

    #[test]
    fn cold_stays_cold_when_degraded() {
        let s = SharedStatus::new("ns", "cm", "key");
        s.mark_degraded("api_unavailable");
        assert!(!s.is_ready());
        let r = s.to_response();
        assert_eq!(r.state, "cold");
    }
}
