// SPDX-License-Identifier: MIT

//! `ConfigMap` watch loop.
//!
//! Watches a single named `ConfigMap` using `kube::runtime::watcher`,
//! validates the overlay envelope, and atomically writes it to the
//! shared volume.  Handles reconnection, relisting, deletion, and
//! last-known-good retention.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use futures::TryStreamExt as _;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
    Client,
    api::Api,
    runtime::watcher::{self, Event},
};

use crate::{
    atomic_file,
    metrics::Metrics,
    status::SharedStatus,
    validation::{self, ExpectedScope, RejectionReason},
};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Watcher configuration.
#[derive(Clone, Debug)]
pub(crate) struct WatcherConfig {
    /// Kubernetes namespace.
    pub(crate) namespace: String,
    /// `ConfigMap` name.
    pub(crate) config_map_name: String,
    /// Data key within the `ConfigMap`.
    pub(crate) data_key: String,
    /// Output file path.
    pub(crate) output_path: PathBuf,
    /// Expected scope for validation.
    pub(crate) expected_scope: ExpectedScope,
    /// Maximum payload size in bytes.
    pub(crate) max_payload_bytes: usize,
}

/// Result of processing one `ConfigMap` event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProcessOutcome {
    /// A new valid revision was written.
    Written,
    /// The source is valid and already installed.
    Unchanged,
    /// The source or local write was rejected.
    Rejected,
}

/// Restore readiness from a valid last-known-good file already present in the
/// shared volume. This is used when the long-running sidecar restarts while
/// Praxis continues serving from the same `emptyDir`.
pub(crate) fn restore_last_known_good(config: &WatcherConfig, status: &SharedStatus, metrics: &Arc<Metrics>) -> bool {
    let Ok(raw) = std::fs::read(&config.output_path) else {
        return false;
    };
    match validation::validate_envelope(&raw, &config.expected_scope, config.max_payload_bytes, None) {
        Ok(validated) => {
            status.record_observed(&validated.revision);
            status.record_written(&validated.revision);
            metrics.ready.set(1);
            metrics.degraded.set(0);
            #[expect(
                clippy::cast_precision_loss,
                reason = "overlay payloads are well under f64 mantissa range"
            )]
            metrics.payload_bytes.set(raw.len() as f64);
            tracing::info!(revision = %validated.revision, "last-known-good overlay restored");
            true
        },
        Err(e) => {
            tracing::warn!(reason = %e.reason, "existing overlay rejected during startup");
            false
        },
    }
}

// ---------------------------------------------------------------------------
// Initial fetch
// ---------------------------------------------------------------------------

/// Perform the initial GET before the watch loop starts.
///
/// Fetches the current `ConfigMap`, validates the overlay, and writes
/// it to the output file.  Returns `true` if a valid overlay was
/// written.
pub(crate) async fn initial_fetch(
    client: &Client,
    config: &WatcherConfig,
    status: &SharedStatus,
    metrics: &Arc<Metrics>,
) -> bool {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), &config.namespace);

    match api.get(&config.config_map_name).await {
        Ok(cm) => matches!(
            process_configmap(&cm, config, status, metrics),
            ProcessOutcome::Written | ProcessOutcome::Unchanged
        ),
        Err(e) => {
            tracing::warn!(
                config_map = %config.config_map_name,
                error = %e,
                "initial GET failed"
            );
            status.mark_degraded("api_unavailable");
            metrics.degraded.set(1);
            metrics
                .events_total
                .with_label_values(&["error", "api_unavailable"])
                .inc();
            false
        },
    }
}

/// Wait until an operator-produced overlay has been fetched and published.
/// Used by the one-shot init container to gate Praxis startup.
pub(crate) async fn initial_fetch_until_ready(
    client: &Client,
    config: &WatcherConfig,
    status: &SharedStatus,
    metrics: &Arc<Metrics>,
) {
    let mut delay = Duration::from_secs(1);
    while !initial_fetch(client, config, status, metrics).await {
        let (jittered, next) = next_backoff(delay);
        tokio::time::sleep(jittered).await;
        delay = next;
    }
}

// ---------------------------------------------------------------------------
// Watch loop
// ---------------------------------------------------------------------------

/// Run the `ConfigMap` watch loop until cancelled.
///
/// Uses `kube::runtime::watcher` which handles resource-version
/// tracking, reconnection after watch closure, and relisting after
/// expired resource versions.  The sidecar never exits solely because
/// the API is temporarily unavailable.
#[expect(clippy::infinite_loop, reason = "sidecar runs until externally cancelled")]
pub(crate) async fn run_watch_loop(
    client: &Client,
    config: &WatcherConfig,
    status: &SharedStatus,
    metrics: &Arc<Metrics>,
) {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), &config.namespace);
    let mut delay = Duration::from_secs(1);

    loop {
        tracing::info!(
            config_map = %config.config_map_name,
            namespace = %config.namespace,
            "overlay_watch_started"
        );

        let started = Instant::now();
        run_single_watch(&api, config, status, metrics).await;
        if started.elapsed() >= Duration::from_secs(60) {
            delay = Duration::from_secs(1);
        }
        let (jittered, next) = next_backoff(delay);
        tokio::time::sleep(jittered).await;
        delay = next;
    }
}

/// Execute one watch stream to completion, then return.
async fn run_single_watch(api: &Api<ConfigMap>, config: &WatcherConfig, status: &SharedStatus, metrics: &Arc<Metrics>) {
    let field_selector = format!("metadata.name={}", config.config_map_name);
    let w = watcher::watcher(api.clone(), watcher::Config::default().fields(&field_selector));

    let result = w
        .try_for_each(|event| {
            let config = config.clone();
            let status = status.clone();
            let metrics = Arc::clone(metrics);
            async move {
                handle_event(event, &config, &status, &metrics);
                Ok(())
            }
        })
        .await;

    record_stream_outcome(&result, status, metrics);
}

/// Record the outcome of a watch stream.
fn record_stream_outcome(result: &Result<(), watcher::Error>, status: &SharedStatus, metrics: &Arc<Metrics>) {
    match result {
        Ok(()) => {
            tracing::info!("overlay_watch_reconnected: stream ended normally");
            metrics
                .watch_reconnects_total
                .with_label_values(&["stream_ended"])
                .inc();
        },
        Err(e) => {
            tracing::warn!(error = %e, "overlay_watch_reconnected: stream error");
            metrics
                .watch_reconnects_total
                .with_label_values(&["stream_error"])
                .inc();
            status.mark_degraded("api_unavailable");
            metrics.degraded.set(1);
        },
    }
}

// ---------------------------------------------------------------------------
// Event handling
// ---------------------------------------------------------------------------

/// Handle a single watcher event.
fn handle_event(event: Event<ConfigMap>, config: &WatcherConfig, status: &SharedStatus, metrics: &Arc<Metrics>) {
    match event {
        Event::Apply(cm) | Event::InitApply(cm) => {
            if !matches!(
                process_configmap(&cm, config, status, metrics),
                ProcessOutcome::Rejected
            ) {
                status.clear_degraded();
                metrics.degraded.set(0);
            }
        },
        Event::Delete(cm) => {
            let name = cm.metadata.name.as_deref().unwrap_or("<unknown>");
            tracing::warn!(config_map = %name, "overlay_source_deleted: retaining last-known-good");
            status.mark_degraded("deleted");
            metrics.degraded.set(1);
            metrics.events_total.with_label_values(&["deleted", "deleted"]).inc();
        },
        Event::Init | Event::InitDone => {
            tracing::debug!("watch init event");
        },
    }
}

/// Process a `ConfigMap`: extract, validate, and write the overlay.
#[expect(
    clippy::too_many_lines,
    reason = "the extraction, validation, and publication outcomes are clearer in one bounded pipeline"
)]
fn process_configmap(
    cm: &ConfigMap,
    config: &WatcherConfig,
    status: &SharedStatus,
    metrics: &Arc<Metrics>,
) -> ProcessOutcome {
    let Some(raw) = extract_data(cm, &config.data_key) else {
        tracing::warn!(key = %config.data_key, "overlay_rejected: missing data key");
        metrics
            .validation_failures_total
            .with_label_values(&["missing_key"])
            .inc();
        metrics
            .events_total
            .with_label_values(&["rejected", "missing_key"])
            .inc();
        status.mark_degraded("missing_key");
        metrics.degraded.set(1);
        return ProcessOutcome::Rejected;
    };

    let current_revision = status.written_revision();
    let result = validation::validate_envelope(
        raw.as_bytes(),
        &config.expected_scope,
        config.max_payload_bytes,
        current_revision.as_deref(),
    );

    match &result {
        Ok(validated) => {
            if handle_validated(validated, config, status, metrics) {
                ProcessOutcome::Written
            } else {
                ProcessOutcome::Rejected
            }
        },
        Err(e) => {
            if handle_validation_error(e, status, metrics) {
                ProcessOutcome::Unchanged
            } else {
                ProcessOutcome::Rejected
            }
        },
    }
}

/// Write a validated envelope to disk and update metrics.
fn handle_validated(
    validated: &validation::ValidatedEnvelope,
    config: &WatcherConfig,
    status: &SharedStatus,
    metrics: &Arc<Metrics>,
) -> bool {
    status.record_observed(&validated.revision);
    record_timestamp(&metrics.last_observed_timestamp);

    tracing::info!(
        revision = %validated.revision,
        bytes = validated.raw_bytes.len(),
        "overlay_validated"
    );

    match &atomic_file::atomic_write(&config.output_path, &validated.raw_bytes) {
        Ok(n) => {
            record_write_success(validated, config, status, metrics, *n);
            true
        },
        Err(e) => {
            record_write_failure(e, status, metrics);
            false
        },
    }
}

/// Update status and metrics after a successful write.
fn record_write_success(
    validated: &validation::ValidatedEnvelope,
    config: &WatcherConfig,
    status: &SharedStatus,
    metrics: &Arc<Metrics>,
    bytes_written: usize,
) {
    tracing::info!(
        revision = %validated.revision,
        path = %config.output_path.display(),
        "overlay_written"
    );
    status.record_written(&validated.revision);
    record_timestamp(&metrics.last_write_timestamp);
    #[expect(
        clippy::cast_precision_loss,
        reason = "overlay payloads are well under f64 mantissa range"
    )]
    let bytes_f64 = bytes_written as f64;
    metrics.payload_bytes.set(bytes_f64);
    metrics.ready.set(1);
    metrics.degraded.set(0);
    metrics.file_writes_total.with_label_values(&["success"]).inc();
    metrics.events_total.with_label_values(&["accepted", "valid"]).inc();
}

/// Update status and metrics after a write failure.
fn record_write_failure(e: &atomic_file::AtomicWriteError, status: &SharedStatus, metrics: &Arc<Metrics>) {
    tracing::error!(error = %e, "overlay write failed: retaining last-known-good");
    metrics.file_writes_total.with_label_values(&["failure"]).inc();
    metrics.events_total.with_label_values(&["error", "write_failed"]).inc();
    status.mark_degraded("write_failed");
    metrics.degraded.set(1);
}

/// Log and record a validation rejection.
fn handle_validation_error(e: &validation::ValidationError, status: &SharedStatus, metrics: &Arc<Metrics>) -> bool {
    let reason_str = e.reason.to_string();

    if matches!(e.reason, RejectionReason::Unchanged) {
        tracing::debug!(reason = %reason_str, "overlay_unchanged");
        metrics
            .events_total
            .with_label_values(&["unchanged", &reason_str])
            .inc();
        true
    } else {
        tracing::warn!(reason = %reason_str, detail = %e.detail, "overlay_rejected");
        metrics
            .validation_failures_total
            .with_label_values(&[&reason_str])
            .inc();
        metrics.events_total.with_label_values(&["rejected", &reason_str]).inc();
        status.mark_degraded(&reason_str);
        metrics.degraded.set(1);
        false
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract a data key from a `ConfigMap`.
fn extract_data<'a>(cm: &'a ConfigMap, key: &str) -> Option<&'a String> {
    cm.data.as_ref().and_then(|data| data.get(key))
}

/// Record the current wall-clock time in a Prometheus gauge.
fn record_timestamp(gauge: &prometheus::Gauge) {
    if let Ok(d) = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        gauge.set(d.as_secs_f64());
    }
}

/// Maximum jitter ratio applied to backoff delays (25%).
const JITTER_FRACTION_PERCENT: u32 = 25;

/// Apply up to 25% jitter to a delay using sub-microsecond clock variation.
///
/// Returns a duration between `base * 0.75` and `base * 1.0`.  The
/// jitter source is the sub-microsecond portion of the system clock,
/// which provides adequate decorrelation for distributed retry
/// without requiring a CSPRNG dependency.
fn apply_jitter(base: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let jitter_range = base / (100 / JITTER_FRACTION_PERCENT);
    let offset = jitter_range.mul_f64(f64::from(nanos % 1000) / 1000.0);
    base.saturating_sub(offset)
}

/// Advance an exponential backoff delay with a 30-second cap and 25% jitter.
fn next_backoff(base: Duration) -> (Duration, Duration) {
    let jittered = apply_jitter(base);
    let next = (base * 2).min(Duration::from_secs(30));
    (jittered, next)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use std::collections::BTreeMap;

    use sha2::Digest as _;

    use super::*;
    use crate::{metrics::Metrics, status::SharedStatus};

    fn test_config(output_path: PathBuf) -> WatcherConfig {
        WatcherConfig {
            namespace: "ns".to_owned(),
            config_map_name: "cm".to_owned(),
            data_key: "routing-overlay.json".to_owned(),
            output_path,
            expected_scope: ExpectedScope {
                network: "test-net".to_owned(),
                gateway: "gw".to_owned(),
                namespace: "ns".to_owned(),
                local_site: "site-a".to_owned(),
            },
            max_payload_bytes: 1_048_576,
        }
    }

    fn test_overlay(scope: &ExpectedScope) -> crate::types::RoutingOverlay {
        use crate::types::*;
        RoutingOverlay {
            network: scope.network.clone(),
            local_site: scope.local_site.clone(),
            candidates: vec![RoutingCandidate {
                kind: "inference_model".to_owned(),
                name: "model-a".to_owned(),
                site: scope.local_site.clone(),
                cluster: "cluster-a".to_owned(),
                fresh: true,
                credential: None,
                stable_id: Some("abcd1234".to_owned()),
                admission_state: None,
                selection_tier: None,
                score: None,
                score_breakdown: None,
                rank: Some(0),
            }],
            generated_at: Some("2026-07-29T00:00:00Z".to_owned()),
        }
    }

    fn overlay_digest(overlay: &crate::types::RoutingOverlay) -> String {
        let canonical = serde_json::json!({
            "candidates": serde_json::to_value(&overlay.candidates).unwrap(),
            "local_site": overlay.local_site,
            "network": overlay.network,
        });
        let canonical_bytes = serde_json_canonicalizer::to_vec(&canonical).unwrap();
        let digest: [u8; 32] = sha2::Sha256::new().chain_update(&canonical_bytes).finalize().into();
        digest.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn test_provenance() -> crate::types::OverlayProvenance {
        crate::types::OverlayProvenance {
            producer: "grid-operator".to_owned(),
            producer_version: "0.1.1".to_owned(),
            source_name: "test-net".to_owned(),
            source_uid: "uid-1".to_owned(),
            source_generation: 1,
            rendered_at: "2026-07-29T00:00:00Z".to_owned(),
        }
    }

    fn valid_envelope_json(scope: &ExpectedScope) -> String {
        use crate::types::*;
        let overlay = test_overlay(scope);
        let hex = overlay_digest(&overlay);
        let envelope = OverlayEnvelope {
            schema_version: "1.0.0".to_owned(),
            revision: ContentRevision {
                kind: "content_addressed".to_owned(),
                algorithm: "sha256".to_owned(),
                value: hex.clone(),
            },
            content_digest: ContentDigest {
                algorithm: "sha256".to_owned(),
                value: hex,
            },
            scope: OverlayScope {
                network: scope.network.clone(),
                gateway: scope.gateway.clone(),
                namespace: scope.namespace.clone(),
                local_site: scope.local_site.clone(),
            },
            provenance: test_provenance(),
            overlay,
        };
        serde_json::to_string_pretty(&envelope).unwrap()
    }

    // -- restore_last_known_good -----------------------------------------------

    #[test]
    fn restore_lkg_from_valid_file() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("routing-overlay.json");
        let config = test_config(out.clone());
        let json = valid_envelope_json(&config.expected_scope);
        std::fs::write(&out, &json).unwrap();

        let status = SharedStatus::new("ns", "cm", "key");
        let metrics = Arc::new(Metrics::new());

        assert!(restore_last_known_good(&config, &status, &metrics));
        assert!(status.is_ready());
        assert_eq!(metrics.ready.get(), 1);
        assert_eq!(metrics.degraded.get(), 0);
    }

    #[test]
    fn restore_lkg_rejects_malformed_file() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("routing-overlay.json");
        let config = test_config(out.clone());
        std::fs::write(&out, "not valid json").unwrap();

        let status = SharedStatus::new("ns", "cm", "key");
        let metrics = Arc::new(Metrics::new());

        assert!(!restore_last_known_good(&config, &status, &metrics));
        assert!(!status.is_ready());
    }

    #[test]
    fn restore_lkg_returns_false_for_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("does-not-exist.json");
        let config = test_config(out);
        let status = SharedStatus::new("ns", "cm", "key");
        let metrics = Arc::new(Metrics::new());

        assert!(!restore_last_known_good(&config, &status, &metrics));
        assert!(!status.is_ready());
    }

    // -- process_configmap -----------------------------------------------------

    #[test]
    fn process_configmap_writes_valid_overlay() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("routing-overlay.json");
        let config = test_config(out.clone());
        let json = valid_envelope_json(&config.expected_scope);

        let mut data = BTreeMap::new();
        data.insert("routing-overlay.json".to_owned(), json);
        let cm = ConfigMap {
            data: Some(data),
            ..Default::default()
        };

        let status = SharedStatus::new("ns", "cm", "key");
        let metrics = Arc::new(Metrics::new());

        let outcome = process_configmap(&cm, &config, &status, &metrics);
        assert_eq!(outcome, ProcessOutcome::Written);
        assert!(status.is_ready());
        assert!(out.exists());
    }

    #[test]
    fn process_configmap_rejects_missing_key() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("overlay.json"));
        let cm = ConfigMap {
            data: Some(BTreeMap::new()),
            ..Default::default()
        };
        let status = SharedStatus::new("ns", "cm", "key");
        let metrics = Arc::new(Metrics::new());

        let outcome = process_configmap(&cm, &config, &status, &metrics);
        assert_eq!(outcome, ProcessOutcome::Rejected);
        assert!(!status.is_ready());
    }

    #[test]
    fn process_configmap_rejects_malformed_json() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("overlay.json"));
        let mut data = BTreeMap::new();
        data.insert("routing-overlay.json".to_owned(), "{bad".to_owned());
        let cm = ConfigMap {
            data: Some(data),
            ..Default::default()
        };
        let status = SharedStatus::new("ns", "cm", "key");
        let metrics = Arc::new(Metrics::new());

        let outcome = process_configmap(&cm, &config, &status, &metrics);
        assert_eq!(outcome, ProcessOutcome::Rejected);
    }

    #[test]
    fn process_configmap_unchanged_for_same_revision() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("overlay.json");
        let config = test_config(out);
        let json = valid_envelope_json(&config.expected_scope);
        let mut data = BTreeMap::new();
        data.insert("routing-overlay.json".to_owned(), json);
        let cm = ConfigMap {
            data: Some(data),
            ..Default::default()
        };
        let status = SharedStatus::new("ns", "cm", "key");
        let metrics = Arc::new(Metrics::new());

        assert_eq!(
            process_configmap(&cm, &config, &status, &metrics),
            ProcessOutcome::Written
        );
        assert_eq!(
            process_configmap(&cm, &config, &status, &metrics),
            ProcessOutcome::Unchanged
        );
    }

    #[test]
    fn unchanged_event_clears_api_degradation() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("overlay.json");
        let config = test_config(out);
        let json = valid_envelope_json(&config.expected_scope);
        let mut data = BTreeMap::new();
        data.insert("routing-overlay.json".to_owned(), json);
        let cm = ConfigMap {
            data: Some(data),
            ..Default::default()
        };
        let status = SharedStatus::new("ns", "cm", "key");
        let metrics = Arc::new(Metrics::new());

        process_configmap(&cm, &config, &status, &metrics);
        status.mark_degraded("api_unavailable");
        metrics.degraded.set(1);

        let event = Event::Apply(cm);
        handle_event(event, &config, &status, &metrics);

        assert!(status.is_ready(), "status must be ready after unchanged apply");
        assert_eq!(metrics.degraded.get(), 0, "degraded must clear on valid event");
    }

    #[test]
    fn malformed_event_stays_degraded() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("overlay.json");
        let config = test_config(out);
        let json = valid_envelope_json(&config.expected_scope);

        let status = SharedStatus::new("ns", "cm", "key");
        let metrics = Arc::new(Metrics::new());

        let mut valid_data = BTreeMap::new();
        valid_data.insert("routing-overlay.json".to_owned(), json);
        let valid_cm = ConfigMap {
            data: Some(valid_data),
            ..Default::default()
        };
        process_configmap(&valid_cm, &config, &status, &metrics);

        let mut bad_data = BTreeMap::new();
        bad_data.insert("routing-overlay.json".to_owned(), "{bad".to_owned());
        let bad_cm = ConfigMap {
            data: Some(bad_data),
            ..Default::default()
        };
        handle_event(Event::Apply(bad_cm), &config, &status, &metrics);

        assert!(status.is_ready(), "still ready from earlier valid write");
        assert_eq!(metrics.degraded.get(), 1, "degraded must be set for malformed event");
    }

    #[test]
    fn delete_event_retains_written_revision() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("overlay.json");
        let config = test_config(out.clone());
        let json = valid_envelope_json(&config.expected_scope);
        let mut data = BTreeMap::new();
        data.insert("routing-overlay.json".to_owned(), json);
        let cm = ConfigMap {
            data: Some(data),
            ..Default::default()
        };
        let status = SharedStatus::new("ns", "cm", "key");
        let metrics = Arc::new(Metrics::new());

        process_configmap(&cm, &config, &status, &metrics);
        let written_rev = status.written_revision();
        assert!(written_rev.is_some());

        handle_event(Event::Delete(cm), &config, &status, &metrics);

        assert_eq!(status.written_revision(), written_rev);
        assert!(out.exists(), "file must be retained after delete");
        assert_eq!(metrics.degraded.get(), 1);
    }

    // -- backoff jitter --------------------------------------------------------

    #[test]
    fn jitter_stays_within_bounds() {
        let base = Duration::from_secs(10);
        let min_expected = base.mul_f64(0.74);
        let max_expected = base;
        for _ in 0..100 {
            let jittered = apply_jitter(base);
            assert!(jittered >= min_expected, "jittered {jittered:?} < min {min_expected:?}");
            assert!(jittered <= max_expected, "jittered {jittered:?} > max {max_expected:?}");
        }
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let (_, next) = next_backoff(Duration::from_secs(1));
        assert_eq!(next, Duration::from_secs(2));
        let (_, next) = next_backoff(Duration::from_secs(16));
        assert_eq!(next, Duration::from_secs(30));
        let (_, next) = next_backoff(Duration::from_secs(30));
        assert_eq!(next, Duration::from_secs(30));
    }
}
