//! Runtime controls for the task-owned deterministic llm-d-sim endpoints.

use std::{
    collections::BTreeMap,
    process::Command,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

use super::{
    DATA_PLANE_INTERVAL, DATA_PLANE_WAIT, EPP_READY_METRICS, GRID_SYSTEM_NS, KV_CACHE_METRIC_TOLERANCE,
    first_prom_value, kind_context, kubectl_exec_epp_metrics, parse_required_epp_metrics, simulator_kv_cache_mean,
    simulator_ready_replicas, wait_for_simulator_service_endpoints,
};
use crate::env::verify::{PortForwardGuard, find_free_port};

/// HTTP port exposed by the simulator's administrative API.
const SIMULATOR_HTTP_PORT: u16 = 8000;
/// Fake-metric fields whose values are changed or restored by this harness.
const FAKE_METRIC_FIELDS: [&str; 3] = ["waiting-requests", "running-requests", "kv-cache-usage"];
/// Pod label prefix used to find the single simulator replica for a deployment.
const SIMULATOR_POD_SELECTOR: &str = "app=vllm-vcr,instance=";
/// Maximum time allowed for one simulator administrative API request.
const API_TIMEOUT_SECONDS: &str = "15";

/// Original simulator metric objects keyed by cluster and deployment.
type OriginalMetrics = BTreeMap<(String, String), serde_json::Value>;

/// Process-local restoration state for the active qualification run.
static ORIGINAL_METRICS: OnceLock<Mutex<OriginalMetrics>> = OnceLock::new();

/// Runtime connection and saved state for one simulator deployment.
struct SimulatorRuntimeTarget {
    /// Kubernetes Deployment name for the simulator.
    deployment: String,
    /// KV-cache metric requested for this deployment.
    kv_cache_usage: f64,
    /// Local port forwarded to the simulator administrative API.
    port: u16,
    /// Pod identity and restart counts captured before the update.
    before: Vec<SimulatorPodIdentity>,
    /// Original fake-metric values used during cleanup.
    original_metrics: serde_json::Value,
    /// Port-forward process kept alive while the target is in use.
    _forward: PortForwardGuard,
}

/// Stable identity and restart counters for a ready simulator Pod.
#[derive(Clone, Debug, Eq, PartialEq)]
struct SimulatorPodIdentity {
    /// Kubernetes Pod name.
    name: String,
    /// Kubernetes-assigned immutable Pod UID.
    uid: String,
    /// Restart count for each named container in the Pod.
    restart_counts: Vec<(String, u64)>,
}

/// Simulator metric values as exported by EPP.
#[derive(Clone, Copy, Debug, PartialEq)]
struct SimulatorMetricValues {
    /// Mean waiting requests across ready simulator endpoints.
    waiting: f64,
    /// Mean running requests across ready simulator endpoints.
    running: f64,
    /// Mean KV-cache usage across ready simulator endpoints.
    kv_cache: f64,
}

/// Start a fresh run-scoped record of pre-stimulus metrics.
pub(super) fn begin_metric_session() -> Result<(), Box<dyn std::error::Error>> {
    original_metrics_store()
        .lock()
        .map_err(|error| std::io::Error::other(format!("initial simulator metric lock failed: {error}")))?
        .clear();
    Ok(())
}

/// Update one run-owned pool using the simulator's atomic runtime configuration API.
pub(crate) fn set_simulator_fake_metrics(
    cluster: &str,
    waiting_requests: u32,
    running_requests: u32,
    kv_cache_usage: f64,
    mtls: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    set_simulator_fake_metrics_with_endpoint_kv(
        cluster,
        waiting_requests,
        running_requests,
        kv_cache_usage,
        kv_cache_usage,
        mtls,
    )
}

/// Update fake metrics for both endpoint Deployments without restarting their Pods.
#[expect(
    clippy::too_many_arguments,
    reason = "the queue, running load, and each endpoint's cache value describe the stimulus directly"
)]
pub(crate) fn set_simulator_fake_metrics_with_endpoint_kv(
    cluster: &str,
    waiting_requests: u32,
    running_requests: u32,
    vcr1_kv_cache_usage: f64,
    vcr2_kv_cache_usage: f64,
    mtls: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !vcr1_kv_cache_usage.is_finite()
        || !(0.0..=1.0).contains(&vcr1_kv_cache_usage)
        || !vcr2_kv_cache_usage.is_finite()
        || !(0.0..=1.0).contains(&vcr2_kv_cache_usage)
    {
        return Err("simulator KV-cache values must be finite and in [0, 1]".into());
    }

    let context = kind_context(cluster);
    let desired = [("vcr-1", vcr1_kv_cache_usage), ("vcr-2", vcr2_kv_cache_usage)];
    let mut targets = Vec::new();

    for (deployment, kv_cache_usage) in desired {
        let replicas = deployment_replicas(&context, deployment)?;
        if replicas == 0 {
            eprintln!("  [SIMULATOR] {cluster}/{deployment} is intentionally scaled to zero; no runtime update sent");
            continue;
        }
        if replicas != 1 {
            return Err(format!(
                "runtime simulator controls require exactly one {deployment} replica, found {replicas}"
            )
            .into());
        }

        let before = wait_for_pod_identity(&context, deployment)?;
        let pod = before
            .first()
            .ok_or_else(|| format!("{cluster}/{deployment} has no Ready Pod"))?;
        let port = find_free_port()?;
        let resource = format!("pod/{}", pod.name);
        let mut forward =
            PortForwardGuard::start_resource(&context, GRID_SYSTEM_NS, &resource, port, SIMULATOR_HTTP_PORT)?;
        forward.wait_until_ready(port)?;
        let config = request_admin_config(port, None)?;
        let original_metrics = fake_metric_subset(&config)?;
        targets.push(SimulatorRuntimeTarget {
            deployment: deployment.to_owned(),
            kv_cache_usage,
            port,
            before,
            original_metrics,
            _forward: forward,
        });
    }

    if targets.is_empty() {
        return Err(format!("no ready simulator endpoints are available in {cluster}").into());
    }

    remember_original_metrics(cluster, &targets)?;

    for target in &targets {
        let update = simulator_metric_payload(waiting_requests, running_requests, target.kv_cache_usage);
        request_admin_config(target.port, Some(&update))?;
        verify_runtime_update(&context, cluster, target, &update)?;
    }

    let ready_replicas = wait_for_simulator_service_endpoints(&context)?;
    let expected_endpoints = ready_replicas.total().map_err(std::io::Error::other)?;
    let expected_kv_cache = simulator_kv_cache_mean(vcr1_kv_cache_usage, vcr2_kv_cache_usage, ready_replicas)
        .map_err(std::io::Error::other)?;
    wait_for_epp_metrics(
        cluster,
        mtls,
        SimulatorMetricValues {
            waiting: f64::from(waiting_requests),
            running: f64::from(running_requests),
            kv_cache: expected_kv_cache,
        },
        expected_endpoints,
        "runtime simulator update",
    )?;

    eprintln!(
        "  [OK] {cluster}: runtime /admin/config updated active simulators; EPP reports waiting={waiting_requests}, running={running_requests}, endpoint-weighted KV={expected_kv_cache:.3}, ready endpoints={expected_endpoints}; Pod UIDs and restart counts stayed unchanged"
    );
    Ok(())
}

/// Restore each simulator's initial runtime metric values and verify the EPP has recovered.
pub(crate) fn restore_initial_metrics(mtls: bool) -> Result<Vec<String>, Vec<String>> {
    let originals = match original_metrics_store().lock() {
        Ok(mut saved) => std::mem::take(&mut *saved),
        Err(error) => {
            return Err(vec![format!("initial metric state lock failed: {error}")]);
        },
    };
    if originals.is_empty() {
        return Ok(vec!["no simulator metrics were changed by this run".to_owned()]);
    }

    let mut observations = Vec::new();
    let mut failures = Vec::new();
    for ((cluster, deployment), prior) in &originals {
        match restore_one_deployment(cluster, deployment, prior) {
            Ok(()) => observations.push(format!(
                "{cluster}/{deployment}: original fake metrics restored through POST /admin/config without Pod restart"
            )),
            Err(error) => failures.push(format!("{cluster}/{deployment}: {error}")),
        }
    }

    let clusters: std::collections::BTreeSet<_> = originals.keys().map(|(cluster, _)| cluster.clone()).collect();
    for cluster in clusters {
        match verify_restored_epp_metrics(&cluster, mtls, &originals) {
            Ok(description) => observations.push(description),
            Err(error) => failures.push(format!("{cluster} EPP recovery: {error}")),
        }
    }

    if failures.is_empty() {
        Ok(observations)
    } else {
        observations.extend(failures);
        Err(observations)
    }
}

/// Return the process-local metric restoration record.
fn original_metrics_store() -> &'static Mutex<OriginalMetrics> {
    ORIGINAL_METRICS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Save each deployment's original fake metrics once for later restoration.
fn remember_original_metrics(
    cluster: &str,
    targets: &[SimulatorRuntimeTarget],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut saved = original_metrics_store()
        .lock()
        .map_err(|error| std::io::Error::other(format!("initial simulator metric lock failed: {error}")))?;
    for target in targets {
        saved
            .entry((cluster.to_owned(), target.deployment.clone()))
            .or_insert_with(|| target.original_metrics.clone());
    }
    drop(saved);
    Ok(())
}

/// Read the configured replica count for one simulator Deployment.
fn deployment_replicas(context: &str, deployment: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            &format!("deployment/{deployment}"),
            "-o",
            "jsonpath={.spec.replicas}",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "read deployment/{deployment} replicas: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let output = String::from_utf8_lossy(&output.stdout);
    if output.trim().is_empty() {
        return Ok(1);
    }
    output
        .trim()
        .parse::<usize>()
        .map_err(|error| format!("deployment/{deployment} has invalid spec.replicas: {error}").into())
}

/// Wait until one simulator Deployment has ready Pods and return their identities.
fn wait_for_pod_identity(
    context: &str,
    deployment: &str,
) -> Result<Vec<SimulatorPodIdentity>, Box<dyn std::error::Error>> {
    let selector = format!("{SIMULATOR_POD_SELECTOR}{deployment}");
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last = String::from("no simulator Pod was observed");
    while Instant::now() < deadline {
        let output = Command::new("kubectl")
            .args([
                "--context",
                context,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                "pods",
                "-l",
                &selector,
                "-o",
                "json",
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "read simulator Pod identity for {deployment}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )
            .into());
        }
        let parsed = match serde_json::from_slice::<serde_json::Value>(&output.stdout) {
            Ok(pods) => parse_pod_identities(&pods),
            Err(error) => Err(error.into()),
        };
        match parsed {
            Ok(pods) if pods.len() == 1 => return Ok(pods),
            Ok(pods) => last = format!("found {} Ready simulator Pods for {deployment}", pods.len()),
            Err(error) => last = error.to_string(),
        }
        std::thread::sleep(DATA_PLANE_INTERVAL);
    }
    Err(format!("{context}: {deployment} Pod did not become a single Ready endpoint: {last}").into())
}

/// Parse ready Pod identities and their per-container restart counters.
fn parse_pod_identities(pods: &serde_json::Value) -> Result<Vec<SimulatorPodIdentity>, Box<dyn std::error::Error>> {
    let items = pods
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or("Kubernetes Pod list omitted items")?;
    let mut identities = Vec::new();
    for pod in items {
        if pod.pointer("/metadata/deletionTimestamp").is_some() {
            continue;
        }
        let name = pod
            .pointer("/metadata/name")
            .and_then(serde_json::Value::as_str)
            .ok_or("simulator Pod omitted metadata.name")?;
        let uid = pod
            .pointer("/metadata/uid")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("simulator Pod {name} omitted metadata.uid"))?;
        let ready = pod
            .pointer("/status/conditions")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|conditions| {
                conditions.iter().any(|condition| {
                    condition.get("type").and_then(serde_json::Value::as_str) == Some("Ready")
                        && condition.get("status").and_then(serde_json::Value::as_str) == Some("True")
                })
            });
        if !ready {
            continue;
        }
        let containers = pod
            .pointer("/status/containerStatuses")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| format!("Ready simulator Pod {name} omitted containerStatuses"))?;
        let mut restart_counts = containers
            .iter()
            .map(|container| {
                let container_name = container
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| format!("Pod {name} has a container without a name"))?;
                let restart_count = container
                    .get("restartCount")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| format!("Pod {name}/{container_name} has an invalid restartCount"))?;
                Ok((container_name.to_owned(), restart_count))
            })
            .collect::<Result<Vec<_>, String>>()?;
        restart_counts.sort();
        identities.push(SimulatorPodIdentity {
            name: name.to_owned(),
            uid: uid.to_owned(),
            restart_counts,
        });
    }
    identities.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(identities)
}

/// Read or atomically update a simulator's runtime configuration.
fn request_admin_config(
    local_port: u16,
    body: Option<&serde_json::Value>,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let url = format!("http://127.0.0.1:{local_port}/admin/config");
    let mut command = Command::new("curl");
    command.args([
        "--silent",
        "--show-error",
        "--fail-with-body",
        "--noproxy",
        "*",
        "--connect-timeout",
        "3",
        "--max-time",
        API_TIMEOUT_SECONDS,
    ]);
    if let Some(body) = body {
        command.args(["--request", "POST", "--header", "Content-Type: application/json"]);
        command.arg("--data-binary").arg(serde_json::to_string(body)?);
    }
    let output = command.arg(&url).output()?;
    if !output.status.success() {
        return Err(format!(
            "simulator admin API request failed ({}): {}{}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim(),
            String::from_utf8_lossy(&output.stdout).trim()
        )
        .into());
    }
    serde_json::from_slice(&output.stdout).map_err(|error| {
        format!(
            "simulator admin API returned invalid JSON: {error}; body={}",
            String::from_utf8_lossy(&output.stdout).trim()
        )
        .into()
    })
}

/// Extract the fake-metric fields managed by this qualification.
fn fake_metric_subset(config: &serde_json::Value) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let values = config
        .get("fake-metrics")
        .and_then(serde_json::Value::as_object)
        .ok_or("simulator /admin/config omitted the fake-metrics object")?;
    let mut subset = serde_json::Map::new();
    for field in FAKE_METRIC_FIELDS {
        let value = values
            .get(field)
            .ok_or_else(|| format!("simulator /admin/config omitted fake-metrics.{field}"))?;
        subset.insert(field.to_owned(), value.clone());
    }
    Ok(serde_json::Value::Object(subset))
}

/// Build a partial runtime update for the simulator's fake metrics.
fn simulator_metric_payload(waiting: u32, running: u32, kv_cache: f64) -> serde_json::Value {
    serde_json::json!({
        "fake-metrics": {
            "waiting-requests": waiting,
            "running-requests": running,
            "kv-cache-usage": kv_cache
        }
    })
}

/// Verify the requested fake metrics and unchanged Pod identity after an update.
fn verify_runtime_update(
    context: &str,
    cluster: &str,
    target: &SimulatorRuntimeTarget,
    requested: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let actual_config = request_admin_config(target.port, None)?;
    let actual_metrics = fake_metric_subset(&actual_config)?;
    let expected_metrics = fake_metric_subset(requested)?;
    if !fake_metric_values_match(&actual_metrics, &expected_metrics)? {
        return Err(format!(
            "{cluster}/{} returned fake-metrics {actual_metrics}, expected {expected_metrics}",
            target.deployment
        )
        .into());
    }
    let after = wait_for_pod_identity(context, &target.deployment)?;
    if target.before != after {
        return Err(format!(
            "{cluster}/{} Pod identity or restart count changed across POST /admin/config: before={:?}, after={after:?}",
            target.deployment, target.before
        )
        .into());
    }
    Ok(())
}

/// Compare managed fake-metric values, accepting numeric JSON representations.
fn fake_metric_values_match(
    actual: &serde_json::Value,
    expected: &serde_json::Value,
) -> Result<bool, Box<dyn std::error::Error>> {
    for field in FAKE_METRIC_FIELDS {
        let actual_value = actual
            .get(field)
            .ok_or_else(|| format!("fake-metrics omitted {field}"))?;
        let expected_value = expected
            .get(field)
            .ok_or_else(|| format!("requested fake-metrics omitted {field}"))?;
        let actual_number = numeric_metric(actual_value);
        let expected_number = numeric_metric(expected_value);
        let values_match = match (actual_number, expected_number) {
            (Some(actual_metric), Some(expected_metric)) => (actual_metric - expected_metric).abs() <= f64::EPSILON,
            (None, None) => actual_value == expected_value,
            _ => false,
        };
        if !values_match {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Parse a metric represented as a JSON number or numeric string.
fn numeric_metric(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()))
}

/// Restore one deployment's saved fake metrics and verify no restart occurred.
fn restore_one_deployment(
    cluster: &str,
    deployment: &str,
    original_metrics: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let context = kind_context(cluster);
    let before = wait_for_pod_identity(&context, deployment)?;
    let pod = before
        .first()
        .ok_or_else(|| format!("{cluster}/{deployment} has no Ready Pod"))?;
    let port = find_free_port()?;
    let resource = format!("pod/{}", pod.name);
    let mut forward = PortForwardGuard::start_resource(&context, GRID_SYSTEM_NS, &resource, port, SIMULATOR_HTTP_PORT)?;
    forward.wait_until_ready(port)?;
    request_admin_config(port, Some(&serde_json::json!({"fake-metrics": original_metrics})))?;
    let actual = fake_metric_subset(&request_admin_config(port, None)?)?;
    if !fake_metric_values_match(&actual, original_metrics)? {
        return Err(format!("restored fake-metrics {actual}, expected {original_metrics}").into());
    }
    let after = wait_for_pod_identity(&context, deployment)?;
    if before != after {
        return Err(format!(
            "Pod identity or restart count changed during metric restoration: before={before:?}, after={after:?}"
        )
        .into());
    }
    Ok(())
}

/// Confirm EPP has returned to the saved aggregate simulator metric values.
fn verify_restored_epp_metrics(
    cluster: &str,
    mtls: bool,
    originals: &OriginalMetrics,
) -> Result<String, Box<dyn std::error::Error>> {
    let context = kind_context(cluster);
    let expected = expected_values_from_originals(cluster, originals)?;
    let ready = wait_for_simulator_service_endpoints(&context)?;
    let endpoint_count = ready.total().map_err(std::io::Error::other)?;
    wait_for_epp_metrics(cluster, mtls, expected, endpoint_count, "saved-metric restoration")?;
    Ok(format!(
        "{cluster}: EPP recovered saved waiting={}, running={}, KV={:.3} across {endpoint_count} ready endpoints",
        expected.waiting, expected.running, expected.kv_cache
    ))
}

/// Compute expected endpoint-weighted EPP values from the saved simulator state.
fn expected_values_from_originals(
    cluster: &str,
    originals: &OriginalMetrics,
) -> Result<SimulatorMetricValues, Box<dyn std::error::Error>> {
    let mut weighted = [0.0_f64; 3];
    let mut count = 0_usize;
    let ready = simulator_ready_replicas(&kind_context(cluster))?;
    for (deployment, replicas) in [("vcr-1", ready.vcr_1), ("vcr-2", ready.vcr_2)] {
        if replicas == 0 {
            continue;
        }
        let values = originals
            .get(&(cluster.to_owned(), deployment.to_owned()))
            .ok_or_else(|| format!("no saved fake-metric state for active {cluster}/{deployment}"))?;
        let values = fake_metric_subset(&serde_json::json!({"fake-metrics": values}))?;
        let replica_weight = f64::from(
            u32::try_from(replicas).map_err(|error| format!("{cluster}/{deployment} replica count: {error}"))?,
        );
        for (weighted_value, field) in weighted.iter_mut().zip(FAKE_METRIC_FIELDS) {
            let value = values.get(field).and_then(numeric_metric).ok_or_else(|| {
                format!("saved {cluster}/{deployment} fake-metrics.{field} is not a fixed numeric value")
            })?;
            *weighted_value += value * replica_weight;
        }
        count += replicas;
    }
    if count == 0 {
        return Err(format!("{cluster} has no ready simulator endpoints while restoring metrics").into());
    }
    let divisor = f64::from(u32::try_from(count).map_err(|error| format!("ready endpoint count: {error}"))?);
    Ok(SimulatorMetricValues {
        waiting: weighted[0] / divisor,
        running: weighted[1] / divisor,
        kv_cache: weighted[2] / divisor,
    })
}

/// Wait for EPP to publish the expected aggregate metrics and endpoint count.
fn wait_for_epp_metrics(
    cluster: &str,
    mtls: bool,
    expected: SimulatorMetricValues,
    expected_endpoints: usize,
    phase: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected_endpoint_metric = u32::try_from(expected_endpoints)
        .map(f64::from)
        .map_err(|error| std::io::Error::other(format!("EPP endpoint count is too large: {error}")))?;
    let deadline = Instant::now() + DATA_PLANE_WAIT;
    let mut last = String::from("no valid EPP metric sample");
    while Instant::now() < deadline {
        match kubectl_exec_epp_metrics(cluster, mtls) {
            Ok(text) => match parse_required_epp_metrics(&text) {
                Ok(metrics) => {
                    let running = first_prom_value(&text, &["llm_d_epp_average_running_requests"])
                        .ok_or("EPP publishes no llm_d_epp_average_running_requests")?;
                    let ready = first_prom_value(&text, EPP_READY_METRICS);
                    last = format!(
                        "queue={}, running={running}, KV={}, ready={ready:?}; expected={expected:?}/{expected_endpoints}",
                        metrics.queue_size, metrics.kv_cache
                    );
                    if (metrics.queue_size - expected.waiting).abs() <= KV_CACHE_METRIC_TOLERANCE
                        && (running - expected.running).abs() <= KV_CACHE_METRIC_TOLERANCE
                        && (metrics.kv_cache - expected.kv_cache).abs() <= KV_CACHE_METRIC_TOLERANCE
                        && ready.is_some_and(|value| (value - expected_endpoint_metric).abs() < f64::EPSILON)
                    {
                        return Ok(());
                    }
                },
                Err(error) => last = format!("EPP metrics parse failed: {error}"),
            },
            Err(error) => last = format!("EPP scrape failed: {error}"),
        }
        std::thread::sleep(DATA_PLANE_INTERVAL);
    }
    Err(format!("{cluster}: EPP did not reflect {phase} within timeout; last observation: {last}").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_metric_subset_preserves_the_original_fixed_and_generated_values() {
        let config = serde_json::json!({
            "fake-metrics": {
                "waiting-requests": 0,
                "running-requests": "oscillate:1:3:1s",
                "kv-cache-usage": 0.45,
                "total-prompt-tokens": 19
            }
        });

        let subset = fake_metric_subset(&config);

        assert_eq!(
            subset.ok(),
            Some(serde_json::json!({
                "waiting-requests": 0,
                "running-requests": "oscillate:1:3:1s",
                "kv-cache-usage": 0.45
            })),
            "capture only the three mutable scalar fields while preserving their wire representations"
        );
    }

    #[test]
    fn fake_metric_subset_rejects_missing_original_scalar_values() {
        let config = serde_json::json!({
            "fake-metrics": {
                "waiting-requests": 0,
                "running-requests": 0
            }
        });

        assert!(
            fake_metric_subset(&config).is_err(),
            "a scalar that cannot be restored must stop before mutation"
        );
    }

    #[test]
    fn runtime_update_payload_changes_only_the_requested_metrics() {
        let payload = simulator_metric_payload(9, 2, 0.95);

        assert_eq!(
            payload,
            serde_json::json!({
                "fake-metrics": {
                    "waiting-requests": 9,
                    "running-requests": 2,
                    "kv-cache-usage": 0.95
                }
            })
        );
        assert_eq!(
            fake_metric_subset(&payload).ok(),
            Some(serde_json::json!({
                "waiting-requests": 9,
                "running-requests": 2,
                "kv-cache-usage": 0.95
            }))
        );
    }

    #[test]
    fn fake_metric_value_comparison_handles_integer_and_float_json_numbers() {
        assert!(
            fake_metric_values_match(
                &serde_json::json!({"waiting-requests": 0, "running-requests": 2, "kv-cache-usage": 0.5}),
                &serde_json::json!({"waiting-requests": 0.0, "running-requests": 2.0, "kv-cache-usage": 0.5})
            )
            .is_ok_and(|matches| matches)
        );
    }

    #[test]
    fn pod_identity_parser_records_uid_and_restart_count_only_for_ready_pods() {
        let pods = serde_json::json!({
            "items": [
                {
                    "metadata": {"name": "vcr-1-old", "uid": "old", "deletionTimestamp": "now"},
                    "status": {"conditions": [{"type": "Ready", "status": "True"}], "containerStatuses": [{"name": "vcr", "restartCount": 9}]}
                },
                {
                    "metadata": {"name": "vcr-1-live", "uid": "live"},
                    "status": {
                        "conditions": [{"type": "Ready", "status": "True"}],
                        "containerStatuses": [{"name": "simulator", "restartCount": 1}]
                    }
                },
                {
                    "metadata": {"name": "vcr-1-pending", "uid": "pending"},
                    "status": {"conditions": [{"type": "Ready", "status": "False"}], "containerStatuses": [{"name": "vcr", "restartCount": 0}]}
                }
            ]
        });

        let identities = parse_pod_identities(&pods);

        assert_eq!(
            identities.ok(),
            Some(vec![SimulatorPodIdentity {
                name: "vcr-1-live".to_owned(),
                uid: "live".to_owned(),
                restart_counts: vec![("simulator".to_owned(), 1)],
            }])
        );
    }
}
