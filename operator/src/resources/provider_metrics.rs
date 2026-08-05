//! Provider metrics collection for the [`GridNetwork`] overlay renderer.
//!
//! Scrapes Prometheus `/metrics` endpoints from `InferenceProvider` resources that have
//! `spec.metricsConfig` configured, parses the text with the configured signal
//! names, and returns a map keyed by provider routing identity for use with
//! `render_routing_overlay`.
//!
//! Scrape failures are non-fatal: when a valid cached sample exists within the
//! `staleMetricsSeconds` grace period, it is reused.  When no cache entry is
//! available (or the grace period has expired), the provider is inserted with
//! `UNOBSERVABLE_METRICS` (`healthy: false`), which causes the scoring engine
//! to exclude it from active routing.  Providers without `metricsConfig` are
//! unaffected — they receive neutral default scoring as before.
//!
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::Mutex;

use crate::{
    crd::inference_provider::{ClientCertificateSecretRef, InferenceProvider, MetricSignalNames, MetricsTlsConfig},
    metrics_parser::{MetricNames, parse_prometheus_text},
    metrics_scraper::{self, scrape_metrics},
    resources::routing_overlay::routing_identity,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Scrape timeout used when the provider's `metricsConfig.timeout` cannot be parsed.
const DEFAULT_SCRAPE_TIMEOUT: Duration = Duration::from_secs(2);

/// Metrics inserted for a provider whose configured metrics endpoint cannot be
/// observed (scrape failed and no valid cache entry).
///
/// `healthy: false` causes the scoring engine's `is_healthy` filter to exclude
/// the provider from active routing.  This prevents an unobservable secure
/// metrics endpoint from receiving favorable neutral default scores.
///
/// Providers without `metricsConfig` are unaffected — they have no metrics
/// entry in the map and receive neutral scoring as before.
const UNOBSERVABLE_METRICS: scoring::BackendMetrics = scoring::BackendMetrics {
    error_rate: 1.0,
    healthy: false,
    kv_cache_utilization: 1.0,
    latency_p99_ms: 5000.0,
    prefix_cache_hit_ratio: 0.0,
    queue_depth: 1.0,
};

// ---------------------------------------------------------------------------
// Timestamped metrics and cache
// ---------------------------------------------------------------------------

/// A [`scoring::BackendMetrics`] value paired with the [`Instant`] it was scraped.
///
/// Stored in [`MetricsCache`] so that a recent successful scrape result can be
/// reused during a brief endpoint outage if `metricsConfig.stale_metrics_seconds`
/// is configured.
#[derive(Clone, Debug)]
pub(crate) struct TimestampedMetrics {
    /// The scraped and parsed metrics.
    pub(crate) metrics: scoring::BackendMetrics,
    /// When the scrape completed successfully.
    pub(crate) scraped_at: Instant,
}

/// Cross-reconcile cache for recently-scraped provider metrics.
///
/// Keyed by `(network_name, provider_routing_identity)`.  Entries are updated on
/// every successful scrape and consulted when a subsequent scrape fails and the
/// provider has `metricsConfig.stale_metrics_seconds` set.
pub(crate) type MetricsCache = HashMap<(String, String), TimestampedMetrics>;

// ---------------------------------------------------------------------------
// URL construction
// ---------------------------------------------------------------------------

/// Construct the metrics scrape URL from a provider endpoint and configured path.
///
/// Trims a trailing `/` from `endpoint` before appending `path`.  If `path`
/// does not start with `/`, one is prepended.
///
/// ```text
/// metrics_url("http://backend:8080",  "/metrics") → "http://backend:8080/metrics"
/// metrics_url("http://backend:8080/", "/metrics") → "http://backend:8080/metrics"
/// metrics_url("http://backend:8080",  "metrics")  → "http://backend:8080/metrics"
/// ```
pub(crate) fn metrics_url(endpoint: &str, path: &str) -> String {
    let base = endpoint.trim_end_matches('/');
    if path.starts_with('/') {
        format!("{base}{path}")
    } else {
        format!("{base}/{path}")
    }
}

// ---------------------------------------------------------------------------
// Config conversion
// ---------------------------------------------------------------------------

/// Convert CRD metrics configuration to a [`MetricNames`] parser config.
///
/// Signal fields that are `None` in the CRD remain `None` in the parser config
/// and are not extracted from the Prometheus text.  Pool-name selection and
/// queue-capacity normalisation pass through when configured.
pub(crate) fn metric_names_from_config(
    cfg: &MetricSignalNames,
    pool_name: Option<&str>,
    queue_capacity: Option<u32>,
) -> MetricNames {
    MetricNames {
        queue_depth: cfg.queue_depth.clone(),
        kv_cache_utilization: cfg.kv_cache_utilization.clone(),
        latency_p99_ms: cfg.latency_p99_ms.clone(),
        prefix_cache_hit_ratio: cfg.prefix_cache_hit_ratio.clone(),
        error_rate: cfg.error_rate.clone(),
        healthy: cfg.healthy.clone(),
        pool_name: pool_name.map(str::to_owned),
        queue_capacity: queue_capacity.map(f64::from),
    }
}

// ---------------------------------------------------------------------------
// Timeout parsing
// ---------------------------------------------------------------------------

/// Parse a timeout string (`"2s"`, `"500ms"`) to a [`Duration`].
///
/// Supports `s` and `ms` suffixes only; minutes and bare numbers are not
/// recognised.  Returns [`DEFAULT_SCRAPE_TIMEOUT`] for unrecognised formats,
/// empty strings, or zero values.
pub(crate) fn parse_metrics_timeout(s: &str) -> Duration {
    let s = s.trim();
    if let Some(ms_str) = s.strip_suffix("ms")
        && let Ok(n) = ms_str.trim().parse::<u64>()
        && n > 0
    {
        return Duration::from_millis(n);
    }
    if let Some(s_str) = s.strip_suffix('s')
        && let Ok(n) = s_str.trim().parse::<u64>()
        && n > 0
    {
        return Duration::from_secs(n);
    }
    DEFAULT_SCRAPE_TIMEOUT
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

/// Scrape and parse live metrics for providers in `network_name` that have `spec.metricsConfig`.
///
/// Returns a map from provider routing identity (the value of
/// `spec.routingClusterRef`, or `metadata.name` when absent) to
/// [`scoring::BackendMetrics`].
///
/// Providers without `metricsConfig` or with a blank endpoint are skipped and
/// not present in the returned map.  Scrape failures are logged at `warn`
/// level unless a cached sample is used.
///
/// When `metricsConfig.tls` is configured, the TLS material is resolved from
/// Kubernetes Secrets via `client` and used for server verification (and
/// optional mTLS client authentication).  TLS resolution failures are
/// fail-closed: the scrape is skipped and the provider falls back to stale
/// cache or neutral scoring.
///
/// # Stale metrics grace period
///
/// When `metricsConfig.stale_metrics_seconds` is set and a scrape fails, the
/// function consults `cache` for a previously-scraped value.  If the cached
/// entry is no older than `stale_metrics_seconds`, the cached
/// [`scoring::BackendMetrics`] is used instead of neutral scoring.  After the
/// grace period the provider falls back to absent metrics (neutral scoring).
///
/// When `stale_metrics_seconds` is absent (default), scrape failures always
/// produce neutral scoring — the same backward-compatible behaviour as before
/// this field was added.
///
/// `now` is passed in (rather than read from `Instant::now()`) so tests can
/// control the clock without sleeping.
#[expect(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    clippy::large_stack_frames,
    reason = "sequential per-provider scrape loop with early-continue guards, cache read/write, TLS resolution, and error logging"
)]
pub(crate) async fn collect_provider_metrics(
    network_name: &str,
    providers: &[InferenceProvider],
    cache: &Mutex<MetricsCache>,
    now: Instant,
    client: Option<&kube::Client>,
) -> HashMap<String, scoring::BackendMetrics> {
    // Snapshot only the relevant cache entries (for this network) so the lock
    // is held for microseconds rather than across network I/O.
    let cache_snapshot: HashMap<String, TimestampedMetrics> = {
        let guard = cache.lock().await;
        guard
            .iter()
            .filter(|((net, _), _)| net == network_name)
            .map(|((_, id), tm)| (id.clone(), tm.clone()))
            .collect()
    };

    let mut result = HashMap::new();
    let mut cache_updates: Vec<((String, String), TimestampedMetrics)> = Vec::new();

    for provider in providers {
        if provider.spec.grid_network_ref != network_name {
            continue;
        }
        let Some(mc) = &provider.spec.metrics_config else {
            continue;
        };
        let Some(identity) = routing_identity(provider) else {
            continue;
        };
        let endpoint = provider.spec.endpoint.trim();
        if endpoint.is_empty() {
            continue;
        }
        if let Some(ep) = mc.metrics_endpoint.as_deref()
            && ep.trim().is_empty()
        {
            tracing::warn!(
                provider = identity,
                "metricsEndpoint is present but blank; skipping metrics collection"
            );
            continue;
        }
        if let Some(pn) = mc.pool_name.as_deref()
            && pn.trim().is_empty()
        {
            tracing::warn!(
                provider = identity,
                "poolName is present but blank; skipping metrics collection"
            );
            continue;
        }
        let base = mc.metrics_endpoint.as_deref().unwrap_or(endpoint);
        let url = metrics_url(base, &mc.path);
        let timeout = parse_metrics_timeout(&mc.timeout);
        let names = metric_names_from_config(&mc.signal_names, mc.pool_name.as_deref(), mc.queue_capacity);

        let tls_config = match resolve_tls_config(mc.tls.as_ref(), client, identity).await {
            Ok(cfg) => cfg,
            Err(e) => {
                let used_cache =
                    try_cached_metrics(identity, mc.stale_metrics_seconds, &cache_snapshot, now, &mut result);
                if used_cache {
                    tracing::debug!(
                        provider = identity,
                        error = %e,
                        "metrics TLS resolution failed; using cached sample within stale_metrics_seconds grace period"
                    );
                } else {
                    if mc.tls.is_some() {
                        tracing::warn!(
                            provider = identity,
                            error = %e,
                            "metrics TLS resolution failed; provider excluded from routing"
                        );
                        result.insert(identity.to_owned(), UNOBSERVABLE_METRICS);
                    } else {
                        tracing::warn!(
                            provider = identity,
                            error = %e,
                            "metrics scrape setup failed; provider metrics absent (neutral scoring)"
                        );
                    }
                }
                continue;
            },
        };

        let scrape_result = scrape_metrics(&url, timeout, tls_config).await;
        let parse_result = match &scrape_result {
            Ok(text) => parse_prometheus_text(text, &names),
            Err(e) => Err(e.to_string()),
        };
        match parse_result {
            Ok(parsed) => {
                let bm = parsed.into_backend_metrics();
                cache_updates.push((
                    (network_name.to_owned(), identity.to_owned()),
                    TimestampedMetrics {
                        metrics: bm,
                        scraped_at: now,
                    },
                ));
                result.insert(identity.to_owned(), bm);
            },
            Err(e) => {
                let reason_str = scrape_result
                    .as_ref()
                    .err()
                    .map_or("MetricsScrapeError", |e| classify_scrape_error(e));
                let used_cache =
                    try_cached_metrics(identity, mc.stale_metrics_seconds, &cache_snapshot, now, &mut result);
                if used_cache {
                    tracing::debug!(
                        provider = identity,
                        url = %url,
                        reason = reason_str,
                        error = %e,
                        "metrics scrape failed; using cached sample within stale_metrics_seconds grace period"
                    );
                } else {
                    if mc.tls.is_some() {
                        tracing::warn!(
                            provider = identity,
                            url = %url,
                            reason = reason_str,
                            error = %e,
                            "metrics scrape failed; provider excluded from routing"
                        );
                        result.insert(identity.to_owned(), UNOBSERVABLE_METRICS);
                    } else {
                        tracing::warn!(
                            provider = identity,
                            url = %url,
                            reason = reason_str,
                            error = %e,
                            "metrics scrape failed; provider metrics absent (neutral scoring)"
                        );
                    }
                }
            },
        }
    }

    // Write back successful scrapes to the shared cache.
    if !cache_updates.is_empty() {
        let mut guard = cache.lock().await;
        for (key, val) in cache_updates {
            guard.insert(key, val);
        }
    }

    result
}

/// Attempt to populate `result` with a cached metric sample for `identity`.
///
/// Returns `true` if a valid cached sample was found and inserted; `false` if
/// no grace period is configured, the cache has no entry, or the entry is
/// too old.
fn try_cached_metrics(
    identity: &str,
    stale_metrics_seconds: Option<u32>,
    cache_snapshot: &HashMap<String, TimestampedMetrics>,
    now: Instant,
    result: &mut HashMap<String, scoring::BackendMetrics>,
) -> bool {
    // Zero is treated as absent defensively (schema already rejects 0).
    let Some(ttl_secs) = stale_metrics_seconds.filter(|&s| s > 0) else {
        return false;
    };
    let ttl = Duration::from_secs(u64::from(ttl_secs));
    let Some(cached) = cache_snapshot.get(identity) else {
        return false;
    };
    let age = now.saturating_duration_since(cached.scraped_at);
    if age > ttl {
        return false;
    }
    result.insert(identity.to_owned(), cached.metrics);
    true
}

// ---------------------------------------------------------------------------
// TLS material resolution
// ---------------------------------------------------------------------------

/// Build a [`SecretRef`](crate::crd::grid_network::SecretRef) from a [`ClientCertificateSecretRef`] for Secret reads.
fn secret_ref_from_client_cert(client_ref: &ClientCertificateSecretRef) -> crate::crd::grid_network::SecretRef {
    crate::crd::grid_network::SecretRef {
        name: client_ref.name.clone(),
        namespace: client_ref.namespace.clone(),
        key: None,
    }
}

/// Resolve a [`rustls::ClientConfig`] from a provider's metrics TLS configuration.
///
/// When `tls_config` is `None`, returns `Ok(None)` (use native roots).
/// When `tls_config` is `Some`, reads the referenced Kubernetes Secrets,
/// parses the PEM material, and builds a `ClientConfig`.
///
/// # Fail-closed
///
/// Any resolution failure returns `Err` — the caller must NOT fall back to
/// native root certificates.  The scrape is skipped entirely.
///
/// # Security invariant
///
/// Private key bytes are passed to [`metrics_scraper::build_tls_client_config`]
/// and are never written to logs, events, status fields, or Prometheus labels.
#[expect(
    clippy::large_stack_frames,
    clippy::too_many_lines,
    reason = "async future with kube API types and PEM buffers; sequential Secret reads for CA, client cert, and client key"
)]
async fn resolve_tls_config(
    tls_config: Option<&MetricsTlsConfig>,
    client: Option<&kube::Client>,
    provider_identity: &str,
) -> Result<Option<Arc<rustls::ClientConfig>>, String> {
    let Some(tls) = tls_config else {
        return Ok(None);
    };
    let Some(kube_client) = client else {
        return Err("metrics TLS configured but no Kubernetes client available".to_owned());
    };

    let ca_key = tls.ca_secret_ref.key.as_deref().unwrap_or("ca.crt");
    let ca_pem = read_secret_bytes_for_tls(kube_client, &tls.ca_secret_ref, ca_key, provider_identity, "CA")
        .await
        .map_err(|e| format!("CA secret resolution failed: {e}"))?;

    let (client_cert_pem, client_key_pem) = if let Some(client_ref) = &tls.client_certificate_secret_ref {
        let cert_ref = secret_ref_from_client_cert(client_ref);
        let cert = read_secret_bytes_for_tls(
            kube_client,
            &cert_ref,
            &client_ref.certificate_key,
            provider_identity,
            "client cert",
        )
        .await?;
        let key = read_secret_bytes_for_tls(
            kube_client,
            &cert_ref,
            &client_ref.private_key_key,
            provider_identity,
            "client key",
        )
        .await?;
        (Some(cert), Some(key))
    } else {
        (None, None)
    };

    let config =
        metrics_scraper::build_tls_client_config(&ca_pem, client_cert_pem.as_deref(), client_key_pem.as_deref())
            .map_err(|e| format!("{e}"))?;

    Ok(Some(Arc::new(config)))
}

/// Read raw bytes from a Kubernetes Secret for TLS material resolution.
///
/// Returns an error string suitable for logging (never includes the byte
/// content itself).
async fn read_secret_bytes_for_tls(
    client: &kube::Client,
    secret_ref: &crate::crd::grid_network::SecretRef,
    key_name: &str,
    provider_identity: &str,
    material_desc: &str,
) -> Result<Vec<u8>, String> {
    use crate::resources::secret::read_secret_bytes;

    match read_secret_bytes(client, secret_ref, key_name).await {
        Ok(Some(bytes)) if !bytes.is_empty() => Ok(bytes),
        Ok(Some(_)) => Err(format!(
            "{material_desc} key {key_name:?} in Secret {}/{} is empty for provider {provider_identity}",
            secret_ref.namespace, secret_ref.name
        )),
        Ok(None) => Err(format!(
            "{material_desc} Secret {}/{} or key {key_name:?} not found for provider {provider_identity}",
            secret_ref.namespace, secret_ref.name
        )),
        Err(e) => Err(format!(
            "{material_desc} Secret {}/{} read failed for provider {provider_identity}: {e}",
            secret_ref.namespace, secret_ref.name
        )),
    }
}

// ---------------------------------------------------------------------------
// Metrics TLS validation
// ---------------------------------------------------------------------------

/// Machine-readable reason for a metrics TLS configuration failure.
///
/// Surfaced in [`InferenceProvider`] `status.reason` so administrators can
/// diagnose material and configuration errors without inspecting operator
/// logs.  Values are stable across releases and safe for automation to parse.
///
/// Only material/configuration failures that the controller can observe
/// during reconciliation appear here.  Scrape-time failures (TLS handshake,
/// HTTP 401/403, timeout) are classified by [`classify_scrape_error`] and
/// surfaced as structured log fields only — they cannot be reproduced
/// deterministically by the controller and should not appear in status.
///
/// Never includes raw certificate or key content.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(
    clippy::enum_variant_names,
    reason = "variants are stable status reason strings matching the bounded failure categories"
)]
pub(crate) enum MetricsFailureReason {
    /// A referenced Secret does not exist or has no `data` section.
    MetricsTlsSecretMissing,
    /// The expected key is absent from `Secret.data` or its value is empty.
    MetricsTlsKeyMissing,
    /// PEM material could not be parsed or contains no certificates/keys.
    MetricsTlsMaterialInvalid,
    /// The client certificate and private key do not form a valid identity.
    MetricsTlsIdentityMismatch,
}

impl MetricsFailureReason {
    /// Machine-readable string for `InferenceProvider.status.reason`.
    ///
    /// Only material and configuration failures appear in status — the
    /// controller can observe these during reconciliation without
    /// performing a live scrape.  Scrape-time failures (handshake,
    /// auth, timeout) are surfaced as structured logs only.
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::MetricsTlsSecretMissing => "MetricsTlsSecretMissing",
            Self::MetricsTlsKeyMissing => "MetricsTlsKeyMissing",
            Self::MetricsTlsMaterialInvalid => "MetricsTlsMaterialInvalid",
            Self::MetricsTlsIdentityMismatch => "MetricsTlsIdentityMismatch",
        }
    }
}

/// Classify a [`MetricsScrapeError`](crate::metrics_scraper::MetricsScrapeError) into a
/// bounded log-level reason string.
///
/// These categories are used for structured logging only — they do not
/// appear in `InferenceProvider.status.reason`.  Status reasons are
/// reserved for material/configuration failures that the controller
/// can observe during reconciliation (see [`MetricsFailureReason`]).
pub(crate) fn classify_scrape_error(err: &metrics_scraper::MetricsScrapeError) -> &'static str {
    match err {
        metrics_scraper::MetricsScrapeError::Timeout(_) => "MetricsScrapeTimeout",
        metrics_scraper::MetricsScrapeError::NonOkStatus { status, .. } if *status == 401 || *status == 403 => {
            "MetricsUnauthorized"
        },
        metrics_scraper::MetricsScrapeError::Transport(e) => {
            let msg = e.to_string().to_lowercase();
            if msg.contains("tls") || msg.contains("certificate") || msg.contains("handshake") || msg.contains("ssl") {
                "MetricsTlsHandshakeFailed"
            } else {
                "MetricsScrapeError"
            }
        },
        metrics_scraper::MetricsScrapeError::TlsMaterial(_) | metrics_scraper::MetricsScrapeError::HttpWithTls(_) => {
            "MetricsTlsMaterialInvalid"
        },
        _ => "MetricsScrapeError",
    }
}

/// Result of reading a TLS Secret key for validation.
enum TlsSecretCheck {
    /// The key was found and contains non-empty bytes.
    Ok(Vec<u8>),
    /// The Secret does not exist or has no `data` section.
    SecretMissing,
    /// The expected key is absent or its value is empty.
    KeyMissing,
}

/// Verify that the metrics TLS Secrets exist, contain the expected keys, and
/// the PEM material can be assembled into a valid [`rustls::ClientConfig`].
///
/// This runs during reconcile to surface configuration errors early.
/// The same material is resolved again at scrape time; this check catches
/// misconfigurations before the first scrape attempt.
///
/// # Returns
///
/// - `Ok(None)` — TLS material is accessible and valid (or no TLS configured).
/// - `Ok(Some(reason))` — failure; the provider should be marked [`Degraded`] with the returned reason in
///   `status.reason`.
///
/// [`Degraded`]: crate::crd::inference_provider::ProviderPhase::Degraded
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures (network, server error,
/// authorization denied).  These are transient; the controller should requeue.
///
/// [`OperatorError`]: crate::error::OperatorError
#[expect(
    clippy::too_many_lines,
    clippy::large_stack_frames,
    reason = "sequential Secret reads for CA, client cert, and client key with match arms"
)]
pub(crate) async fn verify_metrics_tls_accessible(
    client: &kube::Client,
    tls_config: Option<&MetricsTlsConfig>,
) -> Result<Option<MetricsFailureReason>, crate::error::OperatorError> {
    let Some(tls) = tls_config else {
        return Ok(None);
    };

    let ca_key = tls.ca_secret_ref.key.as_deref().unwrap_or("ca.crt");
    let ca_pem = match read_tls_secret_for_verify(client, &tls.ca_secret_ref, ca_key).await? {
        TlsSecretCheck::Ok(bytes) => bytes,
        TlsSecretCheck::SecretMissing => return Ok(Some(MetricsFailureReason::MetricsTlsSecretMissing)),
        TlsSecretCheck::KeyMissing => return Ok(Some(MetricsFailureReason::MetricsTlsKeyMissing)),
    };

    let (client_cert_pem, client_key_pem) = if let Some(client_ref) = &tls.client_certificate_secret_ref {
        let sref = secret_ref_from_client_cert(client_ref);
        let cert = match read_tls_secret_for_verify(client, &sref, &client_ref.certificate_key).await? {
            TlsSecretCheck::Ok(bytes) => bytes,
            TlsSecretCheck::SecretMissing => return Ok(Some(MetricsFailureReason::MetricsTlsSecretMissing)),
            TlsSecretCheck::KeyMissing => return Ok(Some(MetricsFailureReason::MetricsTlsKeyMissing)),
        };
        let key = match read_tls_secret_for_verify(client, &sref, &client_ref.private_key_key).await? {
            TlsSecretCheck::Ok(bytes) => bytes,
            TlsSecretCheck::SecretMissing => return Ok(Some(MetricsFailureReason::MetricsTlsSecretMissing)),
            TlsSecretCheck::KeyMissing => return Ok(Some(MetricsFailureReason::MetricsTlsKeyMissing)),
        };
        (Some(cert), Some(key))
    } else {
        (None, None)
    };

    match metrics_scraper::build_tls_client_config(&ca_pem, client_cert_pem.as_deref(), client_key_pem.as_deref()) {
        Ok(_) => Ok(None),
        Err(e) => {
            let msg = e.to_string();
            let reason = if msg.contains("identity construction failed") {
                MetricsFailureReason::MetricsTlsIdentityMismatch
            } else {
                MetricsFailureReason::MetricsTlsMaterialInvalid
            };
            Ok(Some(reason))
        },
    }
}

/// Read raw bytes from a Kubernetes Secret for TLS validation.
///
/// Distinguishes between "Secret not found" and "key not found" to map to
/// the correct [`MetricsFailureReason`] variant.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures.
///
/// [`OperatorError`]: crate::error::OperatorError
async fn read_tls_secret_for_verify(
    client: &kube::Client,
    secret_ref: &crate::crd::grid_network::SecretRef,
    key_name: &str,
) -> Result<TlsSecretCheck, crate::error::OperatorError> {
    let api: kube::Api<k8s_openapi::api::core::v1::Secret> =
        kube::Api::namespaced(client.clone(), &secret_ref.namespace);
    let Some(secret) = api.get_opt(&secret_ref.name).await? else {
        return Ok(TlsSecretCheck::SecretMissing);
    };
    let Some(data) = &secret.data else {
        return Ok(TlsSecretCheck::KeyMissing);
    };
    match data.get(key_name) {
        Some(bytes) if !bytes.0.is_empty() => Ok(TlsSecretCheck::Ok(bytes.0.clone())),
        _ => Ok(TlsSecretCheck::KeyMissing),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;
    use crate::crd::inference_provider::MetricsConfig;

    // -----------------------------------------------------------------------
    // Test utilities
    // -----------------------------------------------------------------------

    /// Start a one-shot HTTP server that returns the given raw response bytes.
    ///
    /// Returns the bound `http://127.0.0.1:{port}` base URL.
    async fn start_test_server(response: Vec<u8>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|_| std::process::abort());
        let port = listener.local_addr().unwrap_or_else(|_| std::process::abort()).port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0_u8; 4096];
                drop(stream.read(&mut buf).await);
                drop(stream.write_all(&response).await);
            }
        });
        format!("http://127.0.0.1:{port}")
    }

    /// Build a raw HTTP 200 response with a text/plain body.
    fn ok_response(body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    /// Build a raw HTTP error response with an empty body.
    fn err_response(status: u16) -> Vec<u8> {
        format!("HTTP/1.0 {status} Error\r\nContent-Length: 0\r\n\r\n").into_bytes()
    }

    fn provider_fixture(name: &str, endpoint: &str, mc: Option<MetricsConfig>) -> InferenceProvider {
        let mut spec = serde_json::json!({
            "gridNetworkRef": "net",
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": endpoint,
            "models": [{"name": "model-a"}]
        });
        if let Some(m) = mc
            && let Some(s) = spec.as_object_mut()
        {
            s.insert(
                "metricsConfig".to_owned(),
                serde_json::to_value(m).unwrap_or_else(|_| std::process::abort()),
            );
        }
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": {"name": name},
            "spec": spec
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn mc_with_queue(metric_name: &str) -> MetricsConfig {
        MetricsConfig {
            path: "/metrics".to_owned(),
            timeout: "2s".to_owned(),
            signal_names: MetricSignalNames {
                queue_depth: Some(metric_name.to_owned()),
                ..Default::default()
            },
            stale_metrics_seconds: None,
            metrics_endpoint: None,
            pool_name: None,
            queue_capacity: None,
            tls: None,
        }
    }

    fn mc_with_queue_and_ttl(metric_name: &str, ttl: u32) -> MetricsConfig {
        MetricsConfig {
            path: "/metrics".to_owned(),
            timeout: "2s".to_owned(),
            signal_names: MetricSignalNames {
                queue_depth: Some(metric_name.to_owned()),
                ..Default::default()
            },
            stale_metrics_seconds: Some(ttl),
            metrics_endpoint: None,
            pool_name: None,
            queue_capacity: None,
            tls: None,
        }
    }

    /// Return a fresh empty metrics cache wrapped in a Mutex.
    fn empty_cache() -> Mutex<MetricsCache> {
        Mutex::new(MetricsCache::new())
    }

    // -----------------------------------------------------------------------
    // URL construction
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_url_appends_path_to_endpoint() {
        assert_eq!(
            metrics_url("http://backend:8080", "/metrics"),
            "http://backend:8080/metrics"
        );
    }

    #[test]
    fn metrics_url_trims_trailing_slash_from_endpoint() {
        assert_eq!(
            metrics_url("http://backend:8080/", "/metrics"),
            "http://backend:8080/metrics"
        );
    }

    #[test]
    fn metrics_url_prepends_slash_when_path_lacks_one() {
        assert_eq!(
            metrics_url("http://backend:8080", "metrics"),
            "http://backend:8080/metrics"
        );
    }

    #[test]
    fn metrics_url_with_custom_path() {
        assert_eq!(
            metrics_url("http://backend:8080", "/custom/prometheus"),
            "http://backend:8080/custom/prometheus"
        );
    }

    // -----------------------------------------------------------------------
    // Timeout parsing
    // -----------------------------------------------------------------------

    #[test]
    fn parse_metrics_timeout_seconds() {
        assert_eq!(parse_metrics_timeout("2s"), Duration::from_secs(2));
        assert_eq!(parse_metrics_timeout("10s"), Duration::from_secs(10));
    }

    #[test]
    fn parse_metrics_timeout_milliseconds() {
        assert_eq!(parse_metrics_timeout("500ms"), Duration::from_millis(500));
        assert_eq!(parse_metrics_timeout("100ms"), Duration::from_millis(100));
    }

    #[test]
    fn parse_metrics_timeout_invalid_returns_default() {
        assert_eq!(
            parse_metrics_timeout("5m"),
            DEFAULT_SCRAPE_TIMEOUT,
            "minutes not supported"
        );
        assert_eq!(
            parse_metrics_timeout("5"),
            DEFAULT_SCRAPE_TIMEOUT,
            "bare number not supported"
        );
        assert_eq!(parse_metrics_timeout(""), DEFAULT_SCRAPE_TIMEOUT, "empty string");
        assert_eq!(parse_metrics_timeout("abc"), DEFAULT_SCRAPE_TIMEOUT, "non-numeric");
        assert_eq!(parse_metrics_timeout("0s"), DEFAULT_SCRAPE_TIMEOUT, "zero seconds");
    }

    // -----------------------------------------------------------------------
    // Config conversion
    // -----------------------------------------------------------------------

    #[test]
    fn metric_names_from_config_maps_all_signal_names() {
        let cfg = MetricSignalNames {
            queue_depth: Some("my_queue".to_owned()),
            kv_cache_utilization: Some("my_kv".to_owned()),
            latency_p99_ms: Some("my_latency".to_owned()),
            prefix_cache_hit_ratio: Some("my_prefix".to_owned()),
            error_rate: Some("my_errors".to_owned()),
            healthy: Some("my_health".to_owned()),
        };
        let names = metric_names_from_config(&cfg, None, None);
        assert_eq!(names.queue_depth.as_deref(), Some("my_queue"));
        assert_eq!(names.kv_cache_utilization.as_deref(), Some("my_kv"));
        assert_eq!(names.latency_p99_ms.as_deref(), Some("my_latency"));
        assert_eq!(names.prefix_cache_hit_ratio.as_deref(), Some("my_prefix"));
        assert_eq!(names.error_rate.as_deref(), Some("my_errors"));
        assert_eq!(names.healthy.as_deref(), Some("my_health"));
    }

    #[test]
    fn metric_names_from_config_maps_none_for_absent_signals() {
        let names = metric_names_from_config(&MetricSignalNames::default(), None, None);
        assert!(names.queue_depth.is_none());
        assert!(names.kv_cache_utilization.is_none());
        assert!(names.latency_p99_ms.is_none());
        assert!(names.prefix_cache_hit_ratio.is_none());
        assert!(names.error_rate.is_none());
        assert!(names.healthy.is_none());
    }

    #[test]
    fn metric_names_from_config_passes_pool_name_and_queue_capacity() {
        let cfg = MetricSignalNames {
            queue_depth: Some("q".to_owned()),
            ..Default::default()
        };
        let names = metric_names_from_config(&cfg, Some("my-pool"), Some(100));
        assert_eq!(names.pool_name.as_deref(), Some("my-pool"));
        assert_eq!(names.queue_capacity, Some(100.0));
    }

    // -----------------------------------------------------------------------
    // collect_provider_metrics
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn collect_metrics_no_config_returns_empty_map() {
        let provider = provider_fixture("prov-a", "http://127.0.0.1:9999", None);
        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            result.is_empty(),
            "provider without metricsConfig must not appear in metrics map"
        );
    }

    #[tokio::test]
    async fn collect_metrics_blank_endpoint_is_skipped() {
        let provider = provider_fixture("prov-a", "", Some(mc_with_queue("my_queue")));
        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(result.is_empty(), "provider with blank endpoint must not be scraped");
    }

    #[tokio::test]
    async fn collect_metrics_valid_scrape_inserts_backend_metrics() {
        let body = "my_queue 0.2\n";
        let base_url = start_test_server(ok_response(body)).await;
        let provider = provider_fixture("prov-a", &base_url, Some(mc_with_queue("my_queue")));

        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            result.contains_key("prov-a"),
            "provider must appear in metrics map after successful scrape"
        );
        let bm = result.get("prov-a").copied().unwrap_or_else(|| std::process::abort());
        assert!(bm.queue_depth.is_finite(), "queue_depth must be finite");
        assert!(
            bm.queue_depth >= 0.0 && bm.queue_depth <= 1.0,
            "queue_depth must be in [0,1]"
        );
    }

    #[tokio::test]
    async fn collect_metrics_uses_routing_identity_as_key() {
        let body = "my_queue 0.3\n";
        let base_url = start_test_server(ok_response(body)).await;
        let provider: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": {"name": "prov-a"},
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": base_url,
                "models": [{"name": "model-a"}],
                "routingClusterRef": "site-x",
                "metricsConfig": {
                    "path": "/metrics",
                    "timeout": "2s",
                    "signalNames": {"queueDepth": "my_queue"}
                }
            }
        }))
        .unwrap_or_else(|_| std::process::abort());

        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            result.contains_key("site-x"),
            "metrics must be keyed by routingClusterRef, not metadata.name"
        );
        assert!(
            !result.contains_key("prov-a"),
            "metadata.name must not be used as key when routingClusterRef is set"
        );
    }

    #[tokio::test]
    async fn collect_metrics_scrape_failure_plaintext_omits_entry() {
        let provider = provider_fixture("prov-a", "http://127.0.0.1:1", Some(mc_with_queue("my_queue")));
        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            !result.contains_key("prov-a"),
            "plaintext provider scrape failure must not insert unobservable metrics"
        );
    }

    #[tokio::test]
    async fn collect_metrics_non_2xx_plaintext_omits_entry() {
        let base_url = start_test_server(err_response(503)).await;
        let provider = provider_fixture("prov-a", &base_url, Some(mc_with_queue("my_queue")));
        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            !result.contains_key("prov-a"),
            "plaintext provider non-2xx response must not insert unobservable metrics"
        );
    }

    #[tokio::test]
    async fn collect_metrics_malformed_body_produces_finite_metrics() {
        // Malformed Prometheus text — metric not found → neutral defaults.
        let body = "not_prometheus_text {invalid} NaN\n";
        let base_url = start_test_server(ok_response(body)).await;
        let provider = provider_fixture("prov-a", &base_url, Some(mc_with_queue("my_queue")));

        let result = collect_provider_metrics("net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            result.contains_key("prov-a"),
            "malformed body must still produce a metrics entry"
        );
        let bm = result.get("prov-a").copied().unwrap_or_else(|| std::process::abort());
        assert!(
            bm.queue_depth.is_finite(),
            "malformed body must produce finite queue_depth"
        );
        assert!(
            bm.kv_cache_utilization.is_finite(),
            "malformed body must produce finite kv_cache_utilization"
        );
        assert!(
            bm.latency_p99_ms.is_finite(),
            "malformed body must produce finite latency_p99_ms"
        );
    }

    #[tokio::test]
    async fn collect_metrics_multiple_providers_all_present() {
        let body_a = "my_queue 0.1\n";
        let body_b = "my_queue 0.9\n";
        let url_a = start_test_server(ok_response(body_a)).await;
        let url_b = start_test_server(ok_response(body_b)).await;

        let prov_a = provider_fixture("prov-a", &url_a, Some(mc_with_queue("my_queue")));
        let prov_b = provider_fixture("prov-b", &url_b, Some(mc_with_queue("my_queue")));

        let result = collect_provider_metrics("net", &[prov_a, prov_b], &empty_cache(), Instant::now(), None).await;
        assert!(result.contains_key("prov-a"), "prov-a must be in metrics map");
        assert!(result.contains_key("prov-b"), "prov-b must be in metrics map");
        assert!(
            result
                .get("prov-a")
                .copied()
                .unwrap_or_else(|| std::process::abort())
                .queue_depth
                < result
                    .get("prov-b")
                    .copied()
                    .unwrap_or_else(|| std::process::abort())
                    .queue_depth,
            "prov-a (queue=0.1) must have lower queue_depth than prov-b (queue=0.9)"
        );
    }

    #[tokio::test]
    async fn collect_metrics_skips_providers_from_other_networks() {
        let body = "my_queue 0.2\n";
        let base_url = start_test_server(ok_response(body)).await;
        let provider = provider_fixture("prov-a", &base_url, Some(mc_with_queue("my_queue")));

        let result = collect_provider_metrics("other-net", &[provider], &empty_cache(), Instant::now(), None).await;
        assert!(
            result.is_empty(),
            "provider from a different GridNetwork must not be scraped"
        );
    }

    // -----------------------------------------------------------------------
    // Stale metrics cache
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn stale_cache_used_within_ttl_on_scrape_failure() {
        // Seed the cache with a known good sample from T=0.
        let t0 = Instant::now();
        let provider = provider_fixture("prov-a", "http://127.0.0.1:1", Some(mc_with_queue_and_ttl("q", 60)));
        let cache = empty_cache();
        {
            let mut guard = cache.lock().await;
            guard.insert(
                ("net".to_owned(), "prov-a".to_owned()),
                TimestampedMetrics {
                    metrics: scoring::BackendMetrics {
                        queue_depth: 0.42,
                        healthy: true,
                        kv_cache_utilization: 0.5,
                        latency_p99_ms: 2500.0,
                        prefix_cache_hit_ratio: 0.5,
                        error_rate: 0.0,
                    },
                    scraped_at: t0,
                },
            );
        }
        // Scrape fails (port 1 is always refused); cache is within 60 s TTL.
        let result = collect_provider_metrics("net", &[provider], &cache, t0, None).await;
        assert!(
            result.contains_key("prov-a"),
            "failed scrape within TTL must use cached metrics"
        );
        let cached_bm = result.get("prov-a").copied().unwrap_or_else(|| std::process::abort());
        assert!(
            (cached_bm.queue_depth - 0.42).abs() < f64::EPSILON,
            "cached queue_depth must be returned"
        );
    }

    #[tokio::test]
    async fn stale_cache_not_used_after_ttl_expires() {
        let t0 = Instant::now();
        // The TTL is 1 s; advance the clock by 2 s to make the cache stale.
        let t_after_ttl = t0.checked_add(Duration::from_secs(2)).unwrap_or(t0);
        let provider = provider_fixture("prov-a", "http://127.0.0.1:1", Some(mc_with_queue_and_ttl("q", 1)));
        let cache = empty_cache();
        {
            let mut guard = cache.lock().await;
            guard.insert(
                ("net".to_owned(), "prov-a".to_owned()),
                TimestampedMetrics {
                    metrics: scoring::BackendMetrics {
                        queue_depth: 0.42,
                        healthy: true,
                        kv_cache_utilization: 0.5,
                        latency_p99_ms: 2500.0,
                        prefix_cache_hit_ratio: 0.5,
                        error_rate: 0.0,
                    },
                    scraped_at: t0,
                },
            );
        }
        // Clock advanced past TTL → cache entry is expired → plaintext provider omitted.
        let result = collect_provider_metrics("net", &[provider], &cache, t_after_ttl, None).await;
        assert!(
            !result.contains_key("prov-a"),
            "expired cache on plaintext provider must not insert unobservable metrics"
        );
    }

    #[tokio::test]
    async fn no_ttl_configured_scrape_failure_plaintext_omits_entry() {
        let provider = provider_fixture("prov-a", "http://127.0.0.1:1", Some(mc_with_queue("q")));
        let cache = empty_cache();
        {
            let mut guard = cache.lock().await;
            guard.insert(
                ("net".to_owned(), "prov-a".to_owned()),
                TimestampedMetrics {
                    metrics: scoring::BackendMetrics {
                        queue_depth: 0.42,
                        healthy: true,
                        kv_cache_utilization: 0.5,
                        latency_p99_ms: 2500.0,
                        prefix_cache_hit_ratio: 0.5,
                        error_rate: 0.0,
                    },
                    scraped_at: Instant::now(),
                },
            );
        }
        let result = collect_provider_metrics("net", &[provider], &cache, Instant::now(), None).await;
        assert!(
            !result.contains_key("prov-a"),
            "plaintext provider without stale_metrics_seconds must not insert unobservable metrics"
        );
    }

    #[tokio::test]
    async fn successful_scrape_updates_cache() {
        let body = "my_queue 0.33\n";
        let base_url = start_test_server(ok_response(body)).await;
        let provider = provider_fixture("prov-a", &base_url, Some(mc_with_queue_and_ttl("my_queue", 30)));
        let cache = empty_cache();
        let t0 = Instant::now();

        let _unused = collect_provider_metrics("net", &[provider], &cache, t0, None).await;

        // Read from the cache: extract the queue_depth while holding the lock,
        // then release the guard so the MutexGuard does not live across the assert.
        let cached_queue_depth = cache
            .lock()
            .await
            .get(&("net".to_owned(), "prov-a".to_owned()))
            .map(|tm| tm.metrics.queue_depth);
        assert!(cached_queue_depth.is_some(), "successful scrape must write to cache");
        assert!(
            (cached_queue_depth.unwrap_or_else(|| std::process::abort()) - 0.33).abs() < f64::EPSILON,
            "cache must hold the scraped queue_depth value"
        );
    }

    #[tokio::test]
    async fn zero_ttl_treated_as_absent_plaintext_omits_entry() {
        let provider = provider_fixture("prov-a", "http://127.0.0.1:1", Some(mc_with_queue_and_ttl("q", 0)));
        let cache = empty_cache();
        {
            let mut guard = cache.lock().await;
            guard.insert(
                ("net".to_owned(), "prov-a".to_owned()),
                TimestampedMetrics {
                    metrics: scoring::BackendMetrics {
                        queue_depth: 0.99,
                        healthy: true,
                        kv_cache_utilization: 0.5,
                        latency_p99_ms: 2500.0,
                        prefix_cache_hit_ratio: 0.5,
                        error_rate: 0.0,
                    },
                    scraped_at: Instant::now(),
                },
            );
        }
        let result = collect_provider_metrics("net", &[provider], &cache, Instant::now(), None).await;
        assert!(
            !result.contains_key("prov-a"),
            "plaintext provider with zero TTL must not insert unobservable metrics"
        );
    }

    // -----------------------------------------------------------------------
    // MetricsFailureReason
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_failure_reason_as_str_stable_values() {
        assert_eq!(
            MetricsFailureReason::MetricsTlsSecretMissing.as_str(),
            "MetricsTlsSecretMissing",
            "stable status reason code"
        );
        assert_eq!(
            MetricsFailureReason::MetricsTlsKeyMissing.as_str(),
            "MetricsTlsKeyMissing",
            "stable status reason code"
        );
        assert_eq!(
            MetricsFailureReason::MetricsTlsMaterialInvalid.as_str(),
            "MetricsTlsMaterialInvalid",
            "stable status reason code"
        );
        assert_eq!(
            MetricsFailureReason::MetricsTlsIdentityMismatch.as_str(),
            "MetricsTlsIdentityMismatch",
            "stable status reason code"
        );
    }

    // -----------------------------------------------------------------------
    // classify_scrape_error — log-level classification
    // -----------------------------------------------------------------------

    #[test]
    fn classify_timeout_returns_scrape_timeout() {
        let err = metrics_scraper::MetricsScrapeError::Timeout(Duration::from_secs(2));
        assert_eq!(
            classify_scrape_error(&err),
            "MetricsScrapeTimeout",
            "timeout must classify as MetricsScrapeTimeout"
        );
    }

    #[test]
    fn classify_401_returns_unauthorized() {
        let err = metrics_scraper::MetricsScrapeError::NonOkStatus {
            status: 401,
            url: "http://x".to_owned(),
        };
        assert_eq!(
            classify_scrape_error(&err),
            "MetricsUnauthorized",
            "HTTP 401 must classify as MetricsUnauthorized"
        );
    }

    #[test]
    fn classify_403_returns_unauthorized() {
        let err = metrics_scraper::MetricsScrapeError::NonOkStatus {
            status: 403,
            url: "http://x".to_owned(),
        };
        assert_eq!(
            classify_scrape_error(&err),
            "MetricsUnauthorized",
            "HTTP 403 must classify as MetricsUnauthorized"
        );
    }

    #[test]
    fn classify_500_returns_generic() {
        let err = metrics_scraper::MetricsScrapeError::NonOkStatus {
            status: 500,
            url: "http://x".to_owned(),
        };
        assert_eq!(
            classify_scrape_error(&err),
            "MetricsScrapeError",
            "HTTP 500 has no specific failure category"
        );
    }

    #[test]
    fn classify_tls_material_error_returns_material_invalid() {
        let err = metrics_scraper::MetricsScrapeError::TlsMaterial("bad PEM".to_owned());
        assert_eq!(
            classify_scrape_error(&err),
            "MetricsTlsMaterialInvalid",
            "TLS material error must classify as MetricsTlsMaterialInvalid"
        );
    }

    #[test]
    fn classify_invalid_url_returns_generic() {
        let err = metrics_scraper::MetricsScrapeError::InvalidUrl("ftp://bad".to_owned());
        assert_eq!(
            classify_scrape_error(&err),
            "MetricsScrapeError",
            "invalid URL has no specific failure category"
        );
    }
}
