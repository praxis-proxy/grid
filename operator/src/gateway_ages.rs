//! Durable gateway age evidence, independent of peer polling and reconciliation.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::{Body, to_bytes},
    response::{IntoResponse as _, Response},
};
use grid_signals::feedback::{ACK_EPOCH, ACK_SEQUENCE, IngestReport, REPORT_HEADERS};
use http::{HeaderMap, StatusCode};
use kube::{
    Api, Client,
    api::{ListParams, Patch, PatchParams},
};

use crate::{
    crd::{
        grid_network::{GatewayAgeReport, GridNetwork},
        inference_provider::Condition,
    },
    shutdown::Shutdown,
};

/// Gateway condition type; True means actual fresh rejection evidence.
pub const GATEWAY_AGES_REJECTED: &str = "GatewayAgesRejected";
/// Status owner, separate from reconciliation and peer diagnostics.
pub const FIELD_MANAGER: &str = "grid-operator-gateway-signals";
/// Monotonic evidence freshness; expiration never proves recovery.
const FRESHNESS: Duration = Duration::from_secs(300);
/// Total request-body, admission, lookup, and publication budget.
const WRITE_BUDGET: Duration = Duration::from_secs(5);
/// Maximum durable reporter identities, including unresolved retired processes.
const MAX_REPORTERS: usize = 128;

/// Serialized publication with bounded process-local monotonic receipt tracking.
#[derive(Clone)]
pub struct GatewayFeedback {
    /// API client for the local network status.
    client: Client,
    /// Updated only after durable acceptance; empty after operator restart.
    receipts: Arc<tokio::sync::Mutex<BTreeMap<String, Instant>>>,
    /// Cancels publication when the operator terminates.
    shutdown: Shutdown,
}

impl GatewayFeedback {
    /// Start with conservative freshness for all previously persisted evidence.
    #[must_use]
    pub fn new(client: Client, shutdown: Shutdown) -> Self {
        Self {
            client,
            receipts: Arc::default(),
            shutdown,
        }
    }

    /// Accept only the connection's cryptographically proven local scope.
    pub async fn respond(&self, local: bool, request: http::Request<Body>) -> Response {
        if !local {
            return StatusCode::FORBIDDEN.into_response();
        }
        tokio::select! {
            biased;
            () = self.shutdown.triggered() => StatusCode::SERVICE_UNAVAILABLE.into_response(),
            result = tokio::time::timeout(WRITE_BUDGET, self.publish(request)) => {
                result.unwrap_or_else(|_| StatusCode::SERVICE_UNAVAILABLE.into_response())
            },
        }
    }

    /// Parse and publish one report before acknowledging its epoch and sequence.
    async fn publish(&self, request: http::Request<Body>) -> Response {
        let report = match read_report(request).await {
            Ok(report) => report,
            Err(status) => return status.into_response(),
        };
        let mut receipts = self.receipts.lock().await;
        let api: Api<GridNetwork> = Api::all(self.client.clone());
        for _attempt in 0..3 {
            match attempt(&api, &report, &receipts).await {
                Ok((update, instant)) => {
                    if update.fresh {
                        receipts.insert(report.epoch.clone(), instant);
                    }
                    receipts.retain(|epoch, _| update.evidence.reports.iter().any(|row| &row.epoch == epoch));
                    drop(receipts);
                    return acknowledgment(&report, update.overflow);
                },
                Err(StatusCode::CONFLICT) => {},
                Err(status) => return status.into_response(),
            }
        }
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}

/// One CAS attempt; concurrent status writes are retried from a new live lookup.
async fn attempt(
    api: &Api<GridNetwork>,
    report: &IngestReport,
    receipts: &BTreeMap<String, Instant>,
) -> Result<(Update, Instant), StatusCode> {
    let network = Box::pin(read_network(api)).await?;
    let now = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|_format| StatusCode::SERVICE_UNAVAILABLE)?;
    let instant = Instant::now();
    let update = fold(&network, report, &now, receipts, instant).map_err(|()| StatusCode::BAD_REQUEST)?;
    let name = network
        .metadata
        .name
        .as_deref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let applied = Box::pin(api.patch_status(
        name,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(&update.patch),
    ))
    .await
    .map_err(|error| write_error(&error))?;
    if !durable(&applied, &update) {
        tracing::warn!("gateway feedback response did not contain the intended evidence");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok((update, instant))
}

/// Echo exact report identity only after storage and capacity acceptance.
fn acknowledgment(report: &IngestReport, overflow: bool) -> Response {
    if overflow {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    (
        StatusCode::NO_CONTENT,
        [
            (ACK_EPOCH, report.epoch.clone()),
            (ACK_SEQUENCE, report.sequence.to_string()),
        ],
    )
        .into_response()
}

/// Bound the body before admitting the request to the serialized status writer.
async fn read_report(request: http::Request<Body>) -> Result<IngestReport, StatusCode> {
    let (parts, body) = request.into_parts();
    let report = parse(&parts.headers).ok_or(StatusCode::BAD_REQUEST)?;
    to_bytes(body, 0).await.map_err(|_body| StatusCode::PAYLOAD_TOO_LARGE)?;
    Ok(report)
}

/// Resolve exactly one live network, keeping the status API future on the heap.
async fn read_network(api: &Api<GridNetwork>) -> Result<Box<GridNetwork>, StatusCode> {
    let mut items = Box::pin(api.list(&ListParams::default().limit(2)))
        .await
        .map_err(|_read| StatusCode::SERVICE_UNAVAILABLE)?
        .items
        .into_iter();
    let network = items.next().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    if items.next().is_some() {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(Box::new(network))
}

/// Retry only resourceVersion races; other API failures retain client evidence.
fn write_error(error: &kube::Error) -> StatusCode {
    if matches!(error, kube::Error::Api(error) if error.code == 409) {
        StatusCode::CONFLICT
    } else {
        tracing::warn!(%error, "gateway age feedback was not persisted");
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Strict single-value parsing; no field is trusted before its scalar bound.
fn parse(headers: &HeaderMap) -> Option<IngestReport> {
    let values: Option<Vec<String>> = REPORT_HEADERS
        .iter()
        .map(|name| {
            let mut all = headers.get_all(*name).iter();
            let value = all.next()?.to_str().ok()?;
            (all.next().is_none() && value.len() <= 128).then(|| value.to_owned())
        })
        .collect();
    let [version, replica, epoch, sequence, skew, rejected] = <[String; 6]>::try_from(values?).ok()?;
    if version != "1" {
        return None;
    }
    let report = IngestReport {
        replica,
        epoch,
        sequence: sequence.parse().ok()?,
        skew: skew.parse().ok()?,
        rejected: rejected.parse().ok()?,
    };
    (report.valid() && report.sequence.to_string() == sequence && report.skew.to_string() == skew).then_some(report)
}

/// Atomic desired status, with freshness committed only after publication.
struct Update {
    /// SSA document including the read resourceVersion.
    patch: serde_json::Value,
    /// Structured desired evidence for verifying the storage response.
    evidence: Evidence,
    /// Whether this is a strictly newer accepted observation.
    fresh: bool,
    /// Capacity refusal, whose uncertainty is still published.
    overflow: bool,
}

/// Separately owned durable evidence and its diagnostic condition.
struct Evidence {
    /// Retained process records.
    reports: Vec<GatewayAgeReport>,
    /// Capacity uncertainty requiring explicit verification.
    uncertain: bool,
    /// Aggregate condition from these records.
    condition: Condition,
}

/// Monotonic receipt context; missing receipt knowledge remains stale after restart.
struct Freshness<'receipt> {
    /// Live accepted receipts.
    receipts: &'receipt BTreeMap<String, Instant>,
    /// Shared reading for this operation.
    instant: Instant,
    /// Only a strictly newer observation renews its epoch.
    epoch: Option<&'receipt str>,
}

impl Freshness<'_> {
    /// Whether this rejected row has a current monotonic receipt.
    fn contains(&self, row: &GatewayAgeReport) -> bool {
        self.epoch == Some(row.epoch.as_str())
            || self
                .receipts
                .get(&row.epoch)
                .is_some_and(|received| self.instant.saturating_duration_since(*received) < FRESHNESS)
    }
}

/// Fold one observation using durable sequence state and monotonic live freshness.
fn fold(
    network: &GridNetwork,
    report: &IngestReport,
    now: &str,
    receipts: &BTreeMap<String, Instant>,
    instant: Instant,
) -> Result<Update, ()> {
    let status = network.status.as_ref();
    let mut reports = status.map_or_else(Vec::new, |status| status.gateway_age_reports.clone());
    let (fresh, overflow) = observe(&mut reports, report, now)?;
    let uncertain = overflow || status.is_some_and(|status| status.gateway_age_report_overflow);
    reports.sort_by(|left, right| left.epoch.cmp(&right.epoch));
    let freshness = Freshness {
        receipts,
        instant,
        epoch: fresh.then_some(report.epoch.as_str()),
    };
    let condition = condition(network, &reports, uncertain, now, &freshness);
    let patch = serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1", "kind": "GridNetwork",
        "metadata": { "name": network.metadata.name, "resourceVersion": network.metadata.resource_version },
        "status": { "gatewayAgeReports": reports, "gatewayAgeReportOverflow": uncertain, "conditions": [condition] }
    });
    Ok(Update {
        patch,
        evidence: Evidence {
            reports,
            uncertain,
            condition,
        },
        fresh,
        overflow,
    })
}

/// Admit a new identity only by reclaiming clean bookkeeping.
fn observe(reports: &mut Vec<GatewayAgeReport>, report: &IngestReport, now: &str) -> Result<(bool, bool), ()> {
    if let Some(held) = reports.iter_mut().find(|held| held.epoch == report.epoch) {
        return replace(held, report, now).map(|fresh| (fresh, false));
    }
    if reports.len() >= MAX_REPORTERS {
        reports.sort_by(|left, right| (&left.received_at, &left.epoch).cmp(&(&right.received_at, &right.epoch)));
        let Some(position) = reports.iter().position(|held| !held.rejected) else {
            return Ok((false, true));
        };
        reports.remove(position);
    }
    reports.push(row(report, report.rejected || report.skew > 0, now));
    Ok((true, false))
}

/// Reject replay or changed duplicates; counter rises override a clean claim.
fn replace(held: &mut GatewayAgeReport, report: &IngestReport, now: &str) -> Result<bool, ()> {
    if held.replica != report.replica || report.sequence < held.sequence || report.skew < held.skew {
        return Err(());
    }
    if report.sequence == held.sequence {
        return (report.skew == held.skew && report.rejected == held.reported_rejected)
            .then_some(false)
            .ok_or(());
    }
    *held = row(report, report.rejected || report.skew > held.skew, now);
    Ok(true)
}

/// Preserve transition time while updating diagnostic detail and observed generation.
#[expect(
    clippy::too_many_lines,
    reason = "one condition derives bounded detail and preserves transition metadata"
)]
fn condition(
    network: &GridNetwork,
    reports: &[GatewayAgeReport],
    uncertain: bool,
    now: &str,
    freshness: &Freshness<'_>,
) -> Condition {
    let rejecting = reports.iter().filter(|held| held.rejected).collect::<Vec<_>>();
    let (standing, reason) = if rejecting.iter().any(|held| freshness.contains(held)) {
        ("True", "GatewayAgesRejected")
    } else if uncertain || !rejecting.is_empty() {
        ("Unknown", "GatewayEvidenceStale")
    } else {
        ("False", "GatewayAgesAccepted")
    };
    let labels = rejecting
        .iter()
        .take(10)
        .map(|held| held.replica.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let message = format!(
        "{} unresolved gateway processes ({labels}); reporter capacity uncertainty: {uncertain}",
        rejecting.len()
    );
    let held = network.status.as_ref().and_then(|status| {
        status
            .conditions
            .iter()
            .find(|held| held.type_ == GATEWAY_AGES_REJECTED)
    });
    Condition {
        type_: GATEWAY_AGES_REJECTED.to_owned(),
        status: standing.to_owned(),
        reason: reason.to_owned(),
        message,
        last_transition_time: held
            .filter(|held| held.status == standing)
            .map_or_else(|| now.to_owned(), |held| held.last_transition_time.clone()),
        observed_generation: network.metadata.generation,
    }
}

/// Acknowledgment requires the response to contain the intended durable evidence.
fn durable(network: &GridNetwork, update: &Update) -> bool {
    network.status.as_ref().is_some_and(|status| {
        status.gateway_age_reports == update.evidence.reports
            && status.gateway_age_report_overflow == update.evidence.uncertain
            && status
                .conditions
                .iter()
                .find(|condition| condition.type_ == GATEWAY_AGES_REJECTED)
                == Some(&update.evidence.condition)
    })
}

/// Preserve the original flag separately from counter-derived rejection.
fn row(report: &IngestReport, rejected: bool, now: &str) -> GatewayAgeReport {
    GatewayAgeReport {
        replica: report.replica.clone(),
        epoch: report.epoch.clone(),
        sequence: report.sequence,
        skew: report.skew,
        rejected,
        reported_rejected: report.rejected,
        received_at: now.to_owned(),
    }
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::too_many_lines,
    reason = "tests and integration fixtures"
)]
mod tests {
    use super::*;

    #[test]
    fn clean_other_replica_and_replacement_epoch_cannot_clear_bad() {
        let mut network = network();
        let instant = Instant::now();
        let first = report("a", 1, 1, true);
        let update = fold(&network, &first, "t0", &BTreeMap::new(), instant).expect("bad");
        apply(&mut network, &update);
        let receipts = BTreeMap::from([(first.epoch, instant)]);
        let other = fold(&network, &report("b", 1, 0, false), "t1", &receipts, instant).expect("other");
        assert_eq!(standing(&other), "True");
        apply(&mut network, &other);
        let restart = fold(&network, &report("c", 1, 0, false), "t2", &BTreeMap::new(), instant).expect("restart");
        assert_eq!(standing(&restart), "Unknown", "new epoch cannot refute old epoch");
        apply(&mut network, &restart);
        let recovered = fold(&network, &report("a", 2, 1, false), "t3", &receipts, instant).expect("recovery");
        assert_eq!(standing(&recovered), "False");
    }

    #[test]
    fn counter_increase_wins_and_exact_duplicates_never_refresh() {
        let mut network = network();
        let report = report("a", 1, 3, false);
        let instant = Instant::now();
        let update = fold(&network, &report, "t0", &BTreeMap::new(), instant).expect("counter delta");
        assert_eq!(standing(&update), "True");
        apply(&mut network, &update);
        let duplicate = fold(&network, &report, "t1", &BTreeMap::new(), instant).expect("exact duplicate");
        assert!(!duplicate.fresh);
        assert_eq!(
            standing(&duplicate),
            "Unknown",
            "restart cannot renew receipt from a duplicate"
        );
        let conflicting = IngestReport {
            rejected: true,
            ..report.clone()
        };
        assert!(fold(&network, &conflicting, "t2", &BTreeMap::new(), instant).is_err());
        assert!(
            fold(
                &network,
                &IngestReport {
                    sequence: 2,
                    skew: 2,
                    ..report
                },
                "t2",
                &BTreeMap::new(),
                instant
            )
            .is_err()
        );
    }

    #[test]
    fn expiry_retains_uncertainty_and_overflow_cannot_be_cleared_by_client() {
        let mut network = network();
        let instant = Instant::now();
        for number in 0..MAX_REPORTERS {
            let report = IngestReport {
                epoch: format!("{number:032x}"),
                ..report("a", 1, 1, true)
            };
            let update = fold(&network, &report, "t0", &BTreeMap::new(), instant).expect("bounded bad");
            apply(&mut network, &update);
        }
        let overflow =
            fold(&network, &report("f", 1, 1, true), "t1", &BTreeMap::new(), instant).expect("overflow flag");
        assert!(overflow.overflow && overflow.evidence.reports.len() == MAX_REPORTERS);
        assert_eq!(standing(&overflow), "Unknown");
        apply(&mut network, &overflow);
        for number in 0..MAX_REPORTERS {
            let report = IngestReport {
                epoch: format!("{number:032x}"),
                ..report("a", 2, 1, false)
            };
            let update = fold(&network, &report, "t2", &BTreeMap::new(), instant).expect("existing recovery");
            apply(&mut network, &update);
        }
        let held = fold(&network, &report("f", 1, 0, false), "t3", &BTreeMap::new(), instant).expect("new admitted");
        assert_eq!(
            standing(&held),
            "Unknown",
            "coverage overflow requires explicit verification"
        );
    }

    #[test]
    fn monotonic_expiry_and_message_updates_never_claim_recovery() {
        let mut network = network();
        let report = report("a", 2, 1, true);
        let instant = Instant::now();
        let first = fold(&network, &report, "t0", &BTreeMap::new(), instant).expect("bad");
        apply(&mut network, &first);
        let receipts = BTreeMap::from([(report.epoch.clone(), instant)]);
        let expired = instant.checked_add(FRESHNESS).expect("bounded instant");
        let stale = fold(&network, &report, "t1", &receipts, expired).expect("duplicate");
        assert_eq!(standing(&stale), "Unknown");
        assert!(stale.evidence.reports[0].rejected);
        assert!(
            fold(
                &network,
                &IngestReport { sequence: 1, ..report },
                "t2",
                &receipts,
                expired
            )
            .is_err()
        );
    }

    #[test]
    fn parsing_refuses_duplicates_versions_bounds_and_noncanonical_numbers() {
        let report = report("a", 1, 0, false);
        let mut headers = headers(&report);
        assert_eq!(parse(&headers), Some(report));
        headers.append(REPORT_HEADERS[0], http::HeaderValue::from_static("1"));
        assert!(parse(&headers).is_none());
        for (name, bad) in [
            (REPORT_HEADERS[0], "2"),
            (REPORT_HEADERS[3], "01"),
            (REPORT_HEADERS[4], "-1"),
            (REPORT_HEADERS[2], "bad"),
        ] {
            let mut invalid_headers = self::headers(&self::report("a", 1, 0, false));
            invalid_headers.insert(name, http::HeaderValue::from_str(bad).expect("header"));
            assert!(parse(&invalid_headers).is_none());
        }
    }

    #[tokio::test]
    async fn durable_publication_sets_then_clears_and_preserves_peer_condition() {
        let (client, stored, patches) = mock_api(StorageMode::Normal);
        let (_trigger, shutdown) = crate::shutdown::Trigger::new();
        let feedback = GatewayFeedback::new(client, shutdown);
        let bad = report("a", 1, 1, true);
        let response = feedback.respond(true, request(&bad, "")).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(response.headers().get(ACK_EPOCH).expect("epoch"), bad.epoch.as_str());
        assert_eq!(
            stored.lock().expect("stored")["status"]["conditions"][1]["status"],
            "True"
        );
        let clean = report("a", 2, 1, false);
        assert_eq!(
            feedback.respond(true, request(&clean, "")).await.status(),
            StatusCode::NO_CONTENT
        );
        let network = stored.lock().expect("stored");
        assert_eq!(network["status"]["conditions"][1]["status"], "False");
        assert_eq!(network["status"]["conditions"][0]["type"], "PeerAgesRejected");
        assert_eq!(network["status"]["phase"], "Active");
        drop(network);
        assert_eq!(patches.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn conflict_retries_against_live_version_and_exact_duplicate_keeps_receipt() {
        let (client, stored, patches) = mock_api(StorageMode::Conflict);
        let (_trigger, shutdown) = crate::shutdown::Trigger::new();
        let feedback = GatewayFeedback::new(client, shutdown);
        let clean = report("a", 1, 0, false);
        assert_eq!(
            feedback.respond(true, request(&clean, "")).await.status(),
            StatusCode::NO_CONTENT
        );
        let persisted = stored.lock().expect("stored").clone();
        assert_eq!(
            persisted["status"]["gatewayAgeReports"]
                .as_array()
                .expect("reports")
                .len(),
            2,
            "retry preserves another writer's rejection evidence"
        );
        assert_eq!(
            persisted["status"]["conditions"][1]["status"], "Unknown",
            "a clean incoming epoch cannot erase injected unresolved evidence"
        );
        let held = feedback.receipts.lock().await.clone();
        assert_eq!(patches.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(
            feedback.respond(true, request(&clean, "")).await.status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            *feedback.receipts.lock().await,
            held,
            "duplicate cannot renew freshness"
        );
    }

    #[tokio::test]
    async fn failed_or_changed_storage_never_acknowledges_or_commits_receipts() {
        for mode in [StorageMode::Changed, StorageMode::Failure] {
            let (client, _stored, _patches) = mock_api(mode);
            let (_trigger, shutdown) = crate::shutdown::Trigger::new();
            let feedback = GatewayFeedback::new(client, shutdown);
            assert_eq!(
                feedback
                    .respond(true, request(&report("a", 1, 1, true), ""))
                    .await
                    .status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
            assert!(feedback.receipts.lock().await.is_empty());
        }
    }

    #[tokio::test]
    async fn peer_scope_malformed_and_nonempty_reports_never_reach_storage() {
        let (client, _stored, patches) = mock_api(StorageMode::Normal);
        let (_trigger, shutdown) = crate::shutdown::Trigger::new();
        let feedback = GatewayFeedback::new(client, shutdown);
        let bad = report("a", 1, 1, true);
        assert_eq!(
            feedback.respond(false, request(&bad, "")).await.status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            feedback.respond(true, request(&bad, "body")).await.status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        let mut malformed = request(&bad, "");
        malformed
            .headers_mut()
            .append(REPORT_HEADERS[0], http::HeaderValue::from_static("1"));
        assert_eq!(
            feedback.respond(true, malformed).await.status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(patches.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_lookup_and_shutdown_are_bounded() {
        let client = Client::new(
            tower::service_fn(|_request: http::Request<kube::client::Body>| async {
                std::future::pending::<Result<http::Response<Body>, std::convert::Infallible>>().await
            }),
            "default",
        );
        let (trigger, shutdown) = crate::shutdown::Trigger::new();
        let feedback = GatewayFeedback::new(client, shutdown);
        let started = tokio::time::Instant::now();
        assert_eq!(
            feedback
                .respond(true, request(&report("a", 1, 1, true), ""))
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(started.elapsed(), WRITE_BUDGET);
        trigger.trigger();
        assert_eq!(
            feedback
                .respond(true, request(&report("a", 1, 1, true), ""))
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    /// Report request with an explicitly bounded body.
    fn request(report: &IngestReport, body: &str) -> http::Request<Body> {
        let mut request = http::Request::new(Body::from(body.to_owned()));
        *request.headers_mut() = headers(report);
        request
    }

    /// Independent storage failure modes for publication proofs.
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum StorageMode {
        /// Successfully store and return the patch.
        Normal,
        /// First write races another manager.
        Conflict,
        /// Returned evidence differs from the intended write.
        Changed,
        /// Every write fails.
        Failure,
    }

    /// In-memory Kubernetes endpoint with CAS, failure, and response mutation fixtures.
    fn mock_api(
        mode: StorageMode,
    ) -> (
        Client,
        Arc<std::sync::Mutex<serde_json::Value>>,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let mut fixture = serde_json::to_value(network()).expect("network");
        fixture["status"] = serde_json::json!({"phase":"Active","conditions":[{"type":"PeerAgesRejected","status":"True","reason":"Peer","message":"peer","lastTransitionTime":"2026-10-12T00:00:00Z"}]});
        let stored = Arc::new(std::sync::Mutex::new(fixture));
        let patches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let service = tower::service_fn({
            let (stored, patches) = (Arc::clone(&stored), Arc::clone(&patches));
            move |request: http::Request<kube::client::Body>| {
                let (stored, patches) = (Arc::clone(&stored), Arc::clone(&patches));
                async move {
                    use http_body_util::BodyExt as _;
                    if request.method() == http::Method::GET {
                        let network = stored.lock().expect("stored").clone();
                        return Ok::<_, std::convert::Infallible>(http::Response::new(Body::from(serde_json::json!({"apiVersion":"grid.praxis.fast/v1alpha1","kind":"GridNetworkList","items":[network]}).to_string())));
                    }
                    let count = patches.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if mode == StorageMode::Failure || (mode == StorageMode::Conflict && count == 0) {
                        if mode == StorageMode::Conflict {
                            let mut concurrent = stored.lock().expect("stored");
                            concurrent["metadata"]["resourceVersion"] = serde_json::json!("2");
                            concurrent["status"]["gatewayAgeReports"] =
                                serde_json::json!([row(&report("b", 7, 4, true), true, "2026-10-12T00:00:00Z")]);
                        }
                        let code = if mode == StorageMode::Failure { 500 } else { 409 };
                        let error = serde_json::json!({"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Conflict","message":"fixture","code":code});
                        return Ok(http::Response::builder()
                            .status(code)
                            .body(Body::from(error.to_string()))
                            .expect("response"));
                    }
                    let bytes = request.into_body().collect().await.expect("body").to_bytes();
                    let patch: serde_json::Value = serde_json::from_slice(&bytes).expect("patch");
                    let mut network = stored.lock().expect("stored");
                    assert_eq!(
                        patch["metadata"]["resourceVersion"], network["metadata"]["resourceVersion"],
                        "CAS uses live version"
                    );
                    let peer = network["status"]["conditions"][0].clone();
                    network["status"] = patch["status"].clone();
                    network["status"]["phase"] = serde_json::json!("Active");
                    network["status"]["conditions"]
                        .as_array_mut()
                        .expect("conditions")
                        .insert(0, peer);
                    network["metadata"]["resourceVersion"] = serde_json::json!(count.saturating_add(2).to_string());
                    let mut response = network.clone();
                    drop(network);
                    if mode == StorageMode::Changed {
                        response["status"] = serde_json::json!({"phase":"Active"});
                    }
                    Ok(http::Response::new(Body::from(response.to_string())))
                }
            }
        });
        (Client::new(service, "default"), stored, patches)
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// Named network with a CAS version.
    fn network() -> GridNetwork {
        serde_json::from_value(serde_json::json!({"apiVersion":"grid.praxis.fast/v1alpha1","kind":"GridNetwork","metadata":{"name":"grid","resourceVersion":"1"},"spec":{"seeds":[],"gridId":"grid"}})).expect("network")
    }

    /// A bounded process observation.
    fn report(epoch: &str, sequence: i64, skew: i64, rejected: bool) -> IngestReport {
        IngestReport {
            replica: "gateway".to_owned(),
            epoch: epoch.repeat(32),
            sequence,
            skew,
            rejected,
        }
    }

    /// Durable application from the atomic patch.
    fn apply(network: &mut GridNetwork, update: &Update) {
        network.status = Some(serde_json::from_value(update.patch["status"].clone()).expect("status"));
    }

    /// Aggregate condition standing.
    fn standing(update: &Update) -> &str {
        update.patch["status"]["conditions"][0]["status"]
            .as_str()
            .expect("standing")
    }

    /// Scalar report request headers.
    fn headers(report: &IngestReport) -> HeaderMap {
        REPORT_HEADERS
            .into_iter()
            .zip(report.values())
            .map(|(name, value)| {
                (
                    http::HeaderName::from_static(name),
                    http::HeaderValue::from_str(&value).expect("header"),
                )
            })
            .collect()
    }
}
