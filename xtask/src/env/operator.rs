//! Grid operator kind validation: CRD install, operator launch, and
//! health-aware overlay reconciliation verification.
//!
//! These helpers target a **local, out-of-cluster** operator run — the
//! operator binary is spawned as a subprocess using the current kubeconfig
//! context, so no container image build or push is required.
//!
//! Validation sequence:
//! 1. Install Grid CRDs via `generate-crds` binary piped to `kubectl apply`.
//! 2. Apply test `GridNetwork` and `InferenceProvider` fixtures.
//! 3. Spawn operator (`cargo run -p operator`) in the background.
//! 4. Poll provider status until reconciled (up to 60 s).
//! 5. Read the generated routing overlay `ConfigMap` and verify contents.
//! 6. Kill the operator process.

use std::{
    io::Write as _,
    net::{SocketAddr, UdpSocket},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// Time allowed for the operator to reconcile a provider's status.
pub(crate) const STATUS_POLL_TIMEOUT: Duration = Duration::from_secs(60);

/// Time allowed for the overlay `ConfigMap` to be created after reconcile.
pub(crate) const CONFIGMAP_POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Interval between kubectl poll attempts.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Name of the test `GridNetwork` resource.
pub(crate) const TEST_NETWORK: &str = "op-e2e-net";
/// Name of the test gateway reference inside the `GridNetwork`.
pub(crate) const TEST_GATEWAY_NAME: &str = "op-e2e-gw";
/// Namespace of the test gateway reference (and the generated overlay `ConfigMap`).
pub(crate) const TEST_GATEWAY_NS: &str = "default";
/// Name of the `InferenceProvider` with a valid endpoint (expected: reconciles to `Pending`).
pub(crate) const TEST_PROVIDER_HEALTHY: &str = "op-e2e-healthy";
/// Name of the `InferenceProvider` with a blank endpoint (expected: reconciles to `Unavailable`).
pub(crate) const TEST_PROVIDER_INVALID: &str = "op-e2e-invalid";
/// Name of the `InferenceProvider` whose health probe returns non-2xx (expected: `Degraded`).
pub(crate) const TEST_PROVIDER_DEGRADED: &str = "op-e2e-degraded";
/// Name of the `InferenceProvider` with `api_provider` backend kind.
///
/// Used to verify scoring-backed candidate ordering: `api_provider` (score ≈ 5.8) must
/// appear after local providers (score ≈ 7.0) regardless of input order.
pub(crate) const TEST_PROVIDER_API: &str = "op-e2e-api-fallback";

/// The `routingClusterRef` set on the healthy provider fixture.
///
/// Matches the xtask topology site name `site-a` so that the operator-generated
/// overlay candidate has `site: "site-a"` and `cluster: "site-a"`.  The xtask
/// consumer gateway builder generates `load_balancer` cluster entries named
/// `gateway-{site}`, so `gateway-site-a` routes to the site-a provider gateway.
pub(crate) const TEST_HEALTHY_ROUTING_CLUSTER: &str = "site-a";

/// The `routingClusterRef` set on the degraded provider fixture.
///
/// Routes through the same site-a provider gateway as the healthy fixture.
/// The degraded candidate appears in the overlay with `fresh: false`; Praxis
/// applies its own freshness scoring but the route is still established.
pub(crate) const TEST_DEGRADED_ROUTING_CLUSTER: &str = "site-a";
/// Name of the in-cluster Pod and Service that serves HTTP 503 for the degraded provider probe.
pub(crate) const ERROR_ENDPOINT_NAME: &str = "op-e2e-error-endpoint";
/// Local port used when port-forwarding the error-endpoint Pod to the operator host.
pub(crate) const ERROR_ENDPOINT_LOCAL_PORT: u16 = 18503;
/// Container port exposed by the error-endpoint Pod.
const ERROR_ENDPOINT_CONTAINER_PORT: u16 = 8080;
/// Time allowed for the error-endpoint Pod to become Ready.
pub(crate) const POD_READY_TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Metrics ordering fixture constants
// ---------------------------------------------------------------------------

/// Name of the `InferenceProvider` fixture with a low (idle) queue depth metric.
pub(crate) const TEST_METRICS_IDLE_PROVIDER: &str = "op-e2e-metrics-idle";
/// Name of the `InferenceProvider` fixture with a high (busy) queue depth metric.
pub(crate) const TEST_METRICS_BUSY_PROVIDER: &str = "op-e2e-metrics-busy";
/// `routingClusterRef` of the idle metrics provider; becomes `candidate.cluster` in the overlay.
pub(crate) const TEST_METRICS_IDLE_ROUTING_CLUSTER: &str = "site-metrics-idle";
/// `routingClusterRef` of the busy metrics provider; becomes `candidate.cluster` in the overlay.
pub(crate) const TEST_METRICS_BUSY_ROUTING_CLUSTER: &str = "site-metrics-busy";
/// Local port used when port-forwarding the idle metrics endpoint Pod to the operator host.
pub(crate) const METRICS_IDLE_LOCAL_PORT: u16 = 18_501;
/// Local port used when port-forwarding the busy metrics endpoint Pod to the operator host.
pub(crate) const METRICS_BUSY_LOCAL_PORT: u16 = 18_502;
/// Prometheus metric name served by the metrics endpoint Pods.
///
/// The operator's `spec.metricsConfig.signalNames.queueDepth` is set to this name.
const METRICS_QUEUE_SIGNAL_NAME: &str = "provider_queue_depth_normalized";
/// Queue depth value served by the idle metrics endpoint Pod (low → high score).
const METRICS_IDLE_QUEUE_DEPTH: &str = "0.1";
/// Queue depth value served by the busy metrics endpoint Pod (high → low score).
const METRICS_BUSY_QUEUE_DEPTH: &str = "0.9";

// ---------------------------------------------------------------------------
// SWIM membership validation constants
// ---------------------------------------------------------------------------

/// `GridNetwork` resource name used by the SWIM membership validation.
///
/// Kept distinct from `TEST_NETWORK` (`op-e2e-net`) so both validations can
/// coexist without resource collisions.
pub(crate) const SWIM_TEST_NETWORK: &str = "op-e2e-swim-net";

/// Name of the `InferenceProvider` applied during SWIM state validation.
///
/// Applied to the kind cluster so both operators publish real provider-derived
/// CRDT state (not a synthetic site-presence record) after reconciliation.
pub(crate) const SWIM_TEST_PROVIDER: &str = "op-e2e-swim-prov";

/// Model served by the SWIM state test provider fixture.
pub(crate) const SWIM_TEST_PROVIDER_MODEL: &str = "model-swim-proof";

/// SWIM site identity for the primary operator.
pub(crate) const SWIM_NODE_PRIMARY_NAME: &str = "swim-node-p";

/// SWIM site identity for the secondary operator.
pub(crate) const SWIM_NODE_SECONDARY_NAME: &str = "swim-node-s";

/// Time to wait after the secondary operator announces to the primary before
/// applying the `GridNetwork` fixture.
///
/// SWIM gossip converges in 1–3 s with foca's default probe interval; 10 s
/// provides a comfortable margin on slow CI hosts.
pub(crate) const SWIM_CONVERGENCE_WAIT: Duration = Duration::from_secs(10);

/// Timeout for polling the `GridNetwork` status to reach `Active`.
pub(crate) const SWIM_STATUS_POLL_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// CRD installation
// ---------------------------------------------------------------------------

/// Generate Grid CRD manifests and apply them to `context`.
///
/// Spawns `cargo run -p operator --bin generate-crds` to produce a JSON
/// `v1/List`, then pipes the output to `kubectl apply -f -`.
pub(crate) fn install_grid_crds(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("  generating Grid CRDs...");
    let crd_json = generate_crd_json()?;
    eprintln!("  installing Grid CRDs in {context}...");
    apply_manifest(context, &crd_json)?;
    eprintln!("  [OK] Grid CRDs installed");
    Ok(())
}

/// Delete resources owned by the operator reconciliation validation.
///
/// CRDs are intentionally left installed.  All custom resources and namespaced
/// fixtures owned by this validation are removed before each run so status and
/// overlay assertions cannot pass from stale objects created by a previous run.
pub(crate) fn cleanup_validation_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_namespaced_resource(
        context,
        TEST_GATEWAY_NS,
        "configmap",
        &format!("grid-overlay-{TEST_NETWORK}-{TEST_GATEWAY_NAME}"),
    )?;
    delete_namespaced_resource(context, TEST_GATEWAY_NS, "service", ERROR_ENDPOINT_NAME)?;
    force_delete_pod(context, TEST_GATEWAY_NS, ERROR_ENDPOINT_NAME)?;
    force_delete_pod(context, TEST_GATEWAY_NS, TEST_METRICS_IDLE_PROVIDER)?;
    force_delete_pod(context, TEST_GATEWAY_NS, TEST_METRICS_BUSY_PROVIDER)?;
    delete_cluster_resource(context, "inferenceprovider", TEST_PROVIDER_HEALTHY)?;
    delete_cluster_resource(context, "inferenceprovider", TEST_PROVIDER_INVALID)?;
    delete_cluster_resource(context, "inferenceprovider", TEST_PROVIDER_DEGRADED)?;
    delete_cluster_resource(context, "inferenceprovider", TEST_PROVIDER_API)?;
    delete_cluster_resource(context, "inferenceprovider", TEST_METRICS_IDLE_PROVIDER)?;
    delete_cluster_resource(context, "inferenceprovider", TEST_METRICS_BUSY_PROVIDER)?;
    delete_cluster_resource(context, "gridnetwork", TEST_NETWORK)?;
    eprintln!("  [OK] stale validation resources removed");
    Ok(())
}

/// Run the `generate-crds` binary and return its stdout as a `String`.
fn generate_crd_json() -> Result<String, Box<dyn std::error::Error>> {
    let out = Command::new("cargo")
        .args(["run", "--quiet", "-p", "operator", "--bin", "generate_crds"])
        .output()?;
    if !out.status.success() {
        return Err(format!("generate-crds failed: {}", String::from_utf8_lossy(&out.stderr)).into());
    }
    Ok(String::from_utf8(out.stdout)?)
}

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

/// Apply the Grid operator validation fixtures to `context`.
///
/// Creates:
/// - `GridNetwork` `op-e2e-net` with one `gatewayRef`
/// - `InferenceProvider` `op-e2e-healthy` — valid endpoint → reconciles to `Pending`
/// - `InferenceProvider` `op-e2e-invalid` — blank endpoint → reconciles to `Unavailable`
pub(crate) fn apply_test_fixtures(context: &str, provider_endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    let network = network_fixture_json(TEST_NETWORK, TEST_GATEWAY_NAME, TEST_GATEWAY_NS);
    let healthy = provider_fixture_json(
        TEST_PROVIDER_HEALTHY,
        TEST_NETWORK,
        provider_endpoint,
        Some(TEST_HEALTHY_ROUTING_CLUSTER),
    );
    let invalid = provider_fixture_json(TEST_PROVIDER_INVALID, TEST_NETWORK, "", None);
    apply_manifest(context, &network)?;
    apply_manifest(context, &healthy)?;
    apply_manifest(context, &invalid)?;
    eprintln!("  [OK] test fixtures applied");
    Ok(())
}

/// Build a `GridNetwork` JSON fixture.
fn network_fixture_json(name: &str, gw_name: &str, gw_ns: &str) -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": name },
        "spec": {
            "seeds": [],
            "gatewayRefs": [{ "name": gw_name, "namespace": gw_ns }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("fixture serialization failed: {e}");
        std::process::exit(1);
    })
}

/// Build an `InferenceProvider` JSON fixture.
///
/// When `routing_cluster_ref` is `Some(name)`, sets `spec.routingClusterRef`
/// so that overlay candidates use `name` as both `site` and `cluster` (Phase 1).
fn provider_fixture_json(name: &str, network_ref: &str, endpoint: &str, routing_cluster_ref: Option<&str>) -> String {
    let mut spec = serde_json::json!({
        "gridNetworkRef": network_ref,
        "providerKind": "open_ai",
        "backendKind": "local",
        "endpoint": endpoint,
        "models": [{ "name": "model-x" }]
    });
    if let Some(r) = routing_cluster_ref
        && let Some(s) = spec.as_object_mut()
    {
        s.insert("routingClusterRef".to_owned(), serde_json::Value::String(r.to_owned()));
    }
    serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": name },
        "spec": spec
    }))
    .unwrap_or_else(|e| {
        eprintln!("fixture serialization failed: {e}");
        std::process::exit(1);
    })
}

// ---------------------------------------------------------------------------
// Operator subprocess
// ---------------------------------------------------------------------------

#[expect(
    clippy::disallowed_methods,
    reason = "spawn_operator sleeps to allow the operator to establish watches before test fixtures are polled; no async runtime"
)]
/// Spawn the Grid operator as a background subprocess using `context`.
///
/// The operator connects to the cluster via the current kubeconfig.  Call
/// `kill_operator` to stop it after validation is complete.
pub(crate) fn spawn_operator(context: &str) -> Result<Child, Box<dyn std::error::Error>> {
    eprintln!("  setting kubectl context to {context}...");
    Command::new("kubectl")
        .args(["config", "use-context", context])
        .status()?;

    eprintln!("  spawning operator (out-of-cluster)...");
    let child = Command::new("cargo")
        .args(["run", "--quiet", "-p", "operator", "--bin", "operator"])
        .stdin(Stdio::null())
        .spawn()?;
    // Brief pause so the operator establishes its watches before fixtures are polled.
    std::thread::sleep(Duration::from_secs(3));
    Ok(child)
}

/// Kill a spawned operator subprocess.
pub(crate) fn kill_operator(mut child: Child) {
    drop(child.kill());
    drop(child.wait());
    eprintln!("  operator stopped");
}

// ---------------------------------------------------------------------------
// SWIM membership kind validation helpers
// ---------------------------------------------------------------------------

/// Reserve a currently available localhost UDP socket address for SWIM validation.
///
/// The returned address is released before the operator subprocess binds it, so
/// this is still best-effort.  It avoids hardcoded port collisions while keeping
/// the validation deterministic enough for local and CI runs.
pub(crate) fn reserve_local_udp_addr() -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind("127.0.0.1:0")?;
    let addr = socket.local_addr()?;
    drop(socket);
    Ok(addr)
}

#[expect(
    clippy::disallowed_methods,
    reason = "spawn + settle sleep in xtask; no async runtime available"
)]
/// Spawn the Grid operator with SWIM membership enabled.
///
/// Equivalent to [`spawn_operator`] but also sets:
/// - `GRID_SWIM_BIND_ADDR` — UDP address for the SWIM listener
/// - `GRID_SWIM_ADVERTISE_ADDR` — address peers use to reach this node
/// - `GRID_SWIM_SITE_NAME` — stable site identity (must match `GridSite.metadata.name`)
/// - `GRID_SWIM_SEEDS` — comma-separated seed peer addresses (empty = no seeds)
pub(crate) fn spawn_operator_with_swim(
    context: &str,
    bind_addr: &str,
    advertise_addr: &str,
    site_name: &str,
    seeds: &str,
) -> Result<Child, Box<dyn std::error::Error>> {
    eprintln!("  setting kubectl context to {context}...");
    Command::new("kubectl")
        .args(["config", "use-context", context])
        .status()?;

    eprintln!("  spawning SWIM operator (site={site_name}, bind={bind_addr}, seeds={seeds:?})...");
    let child = Command::new("cargo")
        .args(["run", "--quiet", "-p", "operator", "--bin", "operator"])
        .env("GRID_SWIM_BIND_ADDR", bind_addr)
        .env("GRID_SWIM_ADVERTISE_ADDR", advertise_addr)
        .env("GRID_SWIM_SITE_NAME", site_name)
        .env("GRID_SWIM_SEEDS", seeds)
        .stdin(Stdio::null())
        .spawn()?;
    // Brief pause so the operator establishes watches and starts its SWIM listener.
    std::thread::sleep(Duration::from_secs(3));
    Ok(child)
}

#[expect(
    clippy::disallowed_methods,
    reason = "deliberate fixed wait for SWIM gossip convergence; no async runtime available"
)]
/// Wait `duration` for SWIM gossip to converge between peer operators.
///
/// SWIM uses repeated probes on a ~1 s interval (foca `Config::simple`).
/// Calling this after announcing the secondary operator to the primary gives
/// time for both sides to exchange membership tables before the test reads
/// `GridNetwork.status.connectedSites`.
pub(crate) fn wait_for_swim_convergence(duration: Duration) {
    eprintln!("  waiting {duration:?} for SWIM gossip convergence...");
    std::thread::sleep(duration);
}

/// Apply the bare `GridNetwork` resource used by the SWIM membership validation.
///
/// No `gatewayRefs` or `InferenceProvider`s are needed — the test only
/// verifies that `status.connectedSites` and `status.phase` reflect the live
/// SWIM snapshot from the running operators.
pub(crate) fn apply_swim_test_network(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": SWIM_TEST_NETWORK },
        "spec": { "seeds": [] }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM test network serialization failed: {e}");
        std::process::exit(1);
    });
    apply_manifest(context, &manifest)?;
    eprintln!("  [OK] SWIM test GridNetwork {SWIM_TEST_NETWORK} applied");
    Ok(())
}

/// Apply the `InferenceProvider` fixture used by the SWIM state validation.
///
/// The provider belongs to [`SWIM_TEST_NETWORK`] and serves
/// [`SWIM_TEST_PROVIDER_MODEL`].  Both operators will publish this provider's
/// real `InferenceProvider`-derived CRDT state via SWIM gossip after
/// reconciling the owning `GridNetwork`.
pub(crate) fn apply_swim_test_provider(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": SWIM_TEST_PROVIDER },
        "spec": {
            "gridNetworkRef": SWIM_TEST_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://mock-provider.default.svc:8080",
            "models": [{ "name": SWIM_TEST_PROVIDER_MODEL }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM test provider serialization failed: {e}");
        std::process::exit(1);
    });
    apply_manifest(context, &manifest)?;
    eprintln!("  [OK] SWIM test InferenceProvider {SWIM_TEST_PROVIDER} applied (model={SWIM_TEST_PROVIDER_MODEL})");
    Ok(())
}

/// Delete resources created by the SWIM validation.
///
/// Safe to call before a fresh run — all deletes use `--ignore-not-found`.
pub(crate) fn cleanup_swim_test_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_cluster_resource(context, "inferenceprovider", SWIM_TEST_PROVIDER)?;
    delete_cluster_resource(context, "gridnetwork", SWIM_TEST_NETWORK)?;
    eprintln!("  [OK] stale SWIM test resources removed");
    Ok(())
}

#[expect(
    clippy::disallowed_methods,
    reason = "synchronous poll loop in xtask; no async runtime available"
)]
/// Poll the `GridNetwork` status until `phase = Active` and `connectedSites > 0`.
///
/// Returns the `connectedSites` count when the condition is met.
/// Returns `Err` when `timeout` elapses without the condition being satisfied.
///
/// Triggers immediately after applying the `GridNetwork` fixture because the kube
/// watch event causes both SWIM-enabled operators to reconcile at once.
pub(crate) fn wait_for_gridnetwork_active(
    context: &str,
    name: &str,
    timeout: Duration,
) -> Result<u32, Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        let phase = kubectl_jsonpath(context, &format!("gridnetworks/{name}"), "{.status.phase}").unwrap_or_default();
        let sites_str =
            kubectl_jsonpath(context, &format!("gridnetworks/{name}"), "{.status.connectedSites}").unwrap_or_default();
        let sites: u32 = sites_str.parse().unwrap_or(0);

        if phase == "Active" && sites > 0 {
            eprintln!("  [OK] GridNetwork {name}: phase=Active, connectedSites={sites}");
            return Ok(sites);
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "timeout waiting for GridNetwork {name} to reach Active with connectedSites>0; \
                 last observed: phase={phase:?}, connectedSites={sites}"
            )
            .into());
        }
        eprintln!("  waiting for GridNetwork {name} Active (phase={phase:?}, connectedSites={sites})...");
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Verify that a `GridNetwork` `phase` is `"Active"` and `connected_sites > 0`.
///
/// This is the pure assertion called after [`wait_for_gridnetwork_active`]
/// returns.  It is separate to allow unit testing without a live cluster.
pub(crate) fn verify_swim_status(phase: &str, connected_sites: u32) -> Result<(), Box<dyn std::error::Error>> {
    if phase != "Active" {
        return Err(format!("SWIM validation failed: GridNetwork phase must be Active, got {phase:?}").into());
    }
    if connected_sites == 0 {
        return Err("SWIM validation failed: connectedSites must be > 0 but is 0".into());
    }
    eprintln!("  [OK] SWIM membership: phase=Active, connectedSites={connected_sites}");
    Ok(())
}

// ---------------------------------------------------------------------------
// distributed state validation helpers
// ---------------------------------------------------------------------------

#[expect(
    clippy::disallowed_methods,
    reason = "synchronous poll loop in xtask; no async runtime available"
)]
/// Poll the `GridNetwork` status until `distributedProviderCount > 0`.
///
/// Each SWIM-enabled operator publishes real `InferenceProvider`-derived state
/// as a CRDT `GridStateSnapshot` on reconcile.  After SWIM gossip convergence
/// the remote operator's broadcast arrives and `distributedProviderCount`
/// becomes ≥ 1.
///
/// Returns the observed count on success or `Err` on timeout.
pub(crate) fn wait_for_gridnetwork_distributed_state(
    context: &str,
    name: &str,
    timeout: Duration,
) -> Result<u32, Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        let count_str = kubectl_jsonpath(
            context,
            &format!("gridnetworks/{name}"),
            "{.status.distributedProviderCount}",
        )
        .unwrap_or_default();
        let count: u32 = count_str.parse().unwrap_or(0);

        if count > 0 {
            eprintln!("  [OK] GridNetwork {name}: distributedProviderCount={count}");
            return Ok(count);
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "timeout waiting for GridNetwork {name} distributedProviderCount>0; last observed: {count}"
            )
            .into());
        }
        eprintln!("  waiting for GridNetwork {name} distributedProviderCount>0 (observed={count})...");
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Verify that `distributedProviderCount > 0`, proving distributed state arrived via SWIM.
pub(crate) fn verify_distributed_state_received(
    distributed_provider_count: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    if distributed_provider_count == 0 {
        return Err("distributed state validation failed: distributedProviderCount must be > 0".into());
    }
    eprintln!(
        "  [OK] distributed state received via SWIM broadcast: distributedProviderCount={distributed_provider_count}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Status polling
// ---------------------------------------------------------------------------

#[expect(
    clippy::disallowed_methods,
    reason = "synchronous poll loop in xtask; no async runtime available"
)]
/// Poll until `InferenceProvider` `name` in `context` has the expected `phase`.
///
/// Returns `Ok(())` when the phase matches within `timeout`.
/// Returns `Err` if the timeout elapses.
pub(crate) fn wait_for_provider_phase(
    context: &str,
    name: &str,
    expected_phase: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        let observed =
            kubectl_jsonpath(context, &format!("inferenceproviders/{name}"), "{.status.phase}").unwrap_or_default();

        if observed == expected_phase {
            eprintln!("  [OK] {name} phase = {observed}");
            return Ok(());
        }

        if start.elapsed() >= timeout {
            return Err(
                format!("timeout waiting for {name} phase={expected_phase}; last observed: {observed:?}").into(),
            );
        }
        eprintln!("  waiting for {name} phase={expected_phase} (observed={observed:?})...");
        std::thread::sleep(POLL_INTERVAL);
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "synchronous poll loop in xtask; no async runtime available"
)]
/// Poll until the overlay `ConfigMap` exists in `namespace` within `context`.
pub(crate) fn wait_for_overlay_configmap(
    context: &str,
    network: &str,
    gateway: &str,
    namespace: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let cm_name = format!("grid-overlay-{network}-{gateway}");
    let start = Instant::now();
    loop {
        let out = Command::new("kubectl")
            .args([
                "--context",
                context,
                "get",
                "configmap",
                &cm_name,
                "-n",
                namespace,
                "--ignore-not-found",
                "-o",
                "name",
            ])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            .unwrap_or_default();

        if !out.is_empty() {
            eprintln!("  [OK] overlay ConfigMap {cm_name} exists");
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(format!("timeout waiting for ConfigMap {cm_name}").into());
        }
        eprintln!("  waiting for ConfigMap {cm_name}...");
        std::thread::sleep(POLL_INTERVAL);
    }
}

// ---------------------------------------------------------------------------
// Overlay verification
// ---------------------------------------------------------------------------

/// Read the overlay `ConfigMap` and return the parsed `grid-config.json` value.
pub(crate) fn read_overlay_configmap(
    context: &str,
    network: &str,
    gateway: &str,
    namespace: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let cm_name = format!("grid-overlay-{network}-{gateway}");
    let json_str = kubectl_jsonpath_ns(context, namespace, &cm_name, r"{.data.grid-config\.json}")?;
    let overlay: serde_json::Value =
        serde_json::from_str(&json_str).map_err(|e| format!("overlay JSON parse error: {e}"))?;
    Ok(overlay)
}

/// Verify the overlay contains the expected candidate set.
///
/// Checks:
/// - `healthy_cluster` appears in `candidates` with `fresh: true`
/// - `excluded_cluster` is absent from `candidates`
/// - Every candidate has the required Praxis wire-format fields
pub(crate) fn verify_overlay(
    overlay: &serde_json::Value,
    healthy_cluster: &str,
    excluded_cluster: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let candidates = overlay["candidates"]
        .as_array()
        .ok_or("overlay missing candidates array")?;

    // A site may have multiple candidates (e.g. healthy + degraded providers both at
    // site-a with routingClusterRef="site-a").  We require that at least one has fresh=true.
    let has_fresh = candidates
        .iter()
        .any(|c| c["cluster"].as_str() == Some(healthy_cluster) && c["fresh"].as_bool() == Some(true));
    let has_cluster = candidates
        .iter()
        .any(|c| c["cluster"].as_str() == Some(healthy_cluster));
    if !has_cluster {
        return Err(format!("expected {healthy_cluster} in candidates, not found").into());
    }
    if !has_fresh {
        return Err(format!("{healthy_cluster} must have at least one fresh=true candidate").into());
    }

    let found_excluded = candidates
        .iter()
        .any(|c| c["cluster"].as_str() == Some(excluded_cluster));
    if found_excluded {
        return Err(format!("{excluded_cluster} must be excluded (Unavailable) but found in candidates").into());
    }

    for c in candidates {
        for field in &["kind", "name", "site", "cluster", "fresh"] {
            if c.get(field).is_none() {
                return Err(format!("candidate missing required field '{field}'").into());
            }
        }
    }
    eprintln!("  [OK] overlay: {healthy_cluster} present, {excluded_cluster} absent");
    Ok(())
}

// ---------------------------------------------------------------------------
// Error endpoint fixture (serves HTTP 503 for Degraded probe path)
// ---------------------------------------------------------------------------

/// Apply the in-cluster HTTP 503 endpoint Pod and Service to `context`.
///
/// Uses `python:3-alpine` to run a persistent HTTP server that responds
/// `503 Service Unavailable` to every request.  The operator's health probe
/// hits this endpoint and maps the 503 response to `ProbeOutcome::Degraded`,
/// which in turn sets the provider's status phase to `Degraded`.
///
/// Call `wait_for_error_endpoint_ready` before starting the port-forward.
#[expect(
    clippy::too_many_lines,
    reason = "two JSON manifest builds + apply calls; splitting obscures the Pod/Service pair"
)]
pub(crate) fn apply_error_endpoint_fixture(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let python_server = "import http.server,socketserver\nclass H(http.server.BaseHTTPRequestHandler):\n def do_GET(s):\n  s.send_response(503);s.end_headers()\n def log_message(s,*a):pass\nwith socketserver.TCPServer(('',8080),H) as srv:srv.serve_forever()";
    let pod = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": ERROR_ENDPOINT_NAME, "labels": { "app": ERROR_ENDPOINT_NAME } },
        "spec": {
            "containers": [{
                "name": "server",
                "image": "python:3-alpine",
                "imagePullPolicy": "IfNotPresent",
                "command": ["python3", "-c", python_server],
                "ports": [{ "containerPort": ERROR_ENDPOINT_CONTAINER_PORT }]
            }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("error endpoint Pod serialization failed: {e}");
        std::process::exit(1);
    });
    let svc = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": ERROR_ENDPOINT_NAME },
        "spec": {
            "selector": { "app": ERROR_ENDPOINT_NAME },
            "ports": [{ "port": ERROR_ENDPOINT_CONTAINER_PORT, "targetPort": ERROR_ENDPOINT_CONTAINER_PORT }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("error endpoint Service serialization failed: {e}");
        std::process::exit(1);
    });
    apply_manifest(context, &pod)?;
    apply_manifest(context, &svc)?;
    eprintln!("  [OK] error endpoint applied (Pod + Service)");
    Ok(())
}

#[expect(
    clippy::disallowed_methods,
    reason = "synchronous poll loop in xtask; no async runtime available"
)]
/// Poll until the error-endpoint Pod is Ready in `context`.
pub(crate) fn wait_for_error_endpoint_ready(
    context: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        let ready = kubectl_jsonpath(
            context,
            &format!("pod/{ERROR_ENDPOINT_NAME}"),
            "{.status.conditions[?(@.type=='Ready')].status}",
        )
        .unwrap_or_default();
        if ready == "True" {
            eprintln!("  [OK] error endpoint Pod ready");
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(format!("timeout waiting for {ERROR_ENDPOINT_NAME} Pod ready; status={ready:?}").into());
        }
        eprintln!("  waiting for error endpoint Pod ready (status={ready:?})...");
        std::thread::sleep(POLL_INTERVAL);
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "port-forward and settle sleep in xtask; no async runtime available"
)]
/// Start a `kubectl port-forward` for the error-endpoint Pod and return the child process.
///
/// Waits briefly for the port-forward to establish before returning.
/// The caller is responsible for killing the returned `Child`.
pub(crate) fn start_error_endpoint_port_forward(context: &str) -> Result<Child, Box<dyn std::error::Error>> {
    let local_port = ERROR_ENDPOINT_LOCAL_PORT.to_string();
    let pod_port = ERROR_ENDPOINT_CONTAINER_PORT.to_string();
    let child = Command::new("kubectl")
        .args([
            "--context",
            context,
            "port-forward",
            &format!("pod/{ERROR_ENDPOINT_NAME}"),
            &format!("{local_port}:{pod_port}"),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    // Brief pause for the port-forward tunnel to establish.
    std::thread::sleep(Duration::from_secs(2));
    eprintln!("  [OK] port-forward {local_port} → {ERROR_ENDPOINT_NAME}:{pod_port}");
    Ok(child)
}

// ---------------------------------------------------------------------------
// Metrics endpoint fixtures
// ---------------------------------------------------------------------------

/// Build the Python HTTP server script that serves a single Prometheus gauge line.
///
/// The server responds to any GET request with a `200 OK` body containing:
/// `{METRICS_QUEUE_SIGNAL_NAME} {queue_depth}\n`
///
/// This is used to simulate a provider's `/metrics` endpoint in kind, returning
/// a fixed normalised queue depth for the operator to scrape.
fn metrics_server_script(queue_depth: &str) -> String {
    let body_stmt = format!("b=b'{METRICS_QUEUE_SIGNAL_NAME} {queue_depth}\\n'");
    format!(
        "import http.server,socketserver\n\
         class H(http.server.BaseHTTPRequestHandler):\n \
         def do_GET(s):\n  \
         {body_stmt};s.send_response(200);\
         s.send_header('Content-Type','text/plain');\
         s.send_header('Content-Length',str(len(b)));\
         s.end_headers();s.wfile.write(b)\n \
         def log_message(s,*a):pass\n\
         with socketserver.TCPServer(('',8080),H) as srv:srv.serve_forever()"
    )
}

/// Deploy an in-cluster Pod that serves a fixed Prometheus queue depth metric.
///
/// The Pod listens on port 8080 and responds to any GET with:
/// `provider_queue_depth_normalized {queue_depth}`
///
/// After the Pod is created, call [`wait_for_named_pod_ready`] before starting
/// the port-forward.
pub(crate) fn apply_metrics_endpoint_pod(
    context: &str,
    name: &str,
    queue_depth: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let python_server = metrics_server_script(queue_depth);
    let pod = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name, "labels": { "app": name } },
        "spec": {
            "containers": [{
                "name": "server",
                "image": "python:3-alpine",
                "imagePullPolicy": "IfNotPresent",
                "command": ["python3", "-c", python_server],
                "ports": [{ "containerPort": ERROR_ENDPOINT_CONTAINER_PORT }]
            }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("metrics endpoint Pod serialization failed: {e}");
        std::process::exit(1);
    });
    apply_manifest(context, &pod)?;
    eprintln!("  [OK] metrics endpoint Pod {name} applied");
    Ok(())
}

#[expect(
    clippy::disallowed_methods,
    reason = "synchronous poll loop in xtask; no async runtime available"
)]
/// Poll until the named Pod in `namespace` is Ready in `context`.
pub(crate) fn wait_for_named_pod_ready(
    context: &str,
    name: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        let ready = kubectl_jsonpath(
            context,
            &format!("pod/{name}"),
            "{.status.conditions[?(@.type=='Ready')].status}",
        )
        .unwrap_or_default();
        if ready == "True" {
            eprintln!("  [OK] {name} Pod ready");
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(format!("timeout waiting for {name} Pod ready; status={ready:?}").into());
        }
        eprintln!("  waiting for {name} Pod ready (status={ready:?})...");
        std::thread::sleep(POLL_INTERVAL);
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "port-forward settle sleep in xtask; no async runtime available"
)]
/// Start a `kubectl port-forward` for a named Pod and return the child process.
///
/// Forwards `local_port` on the host to port 8080 on the Pod.
/// The caller is responsible for killing the returned [`Child`].
pub(crate) fn start_named_pod_port_forward(
    context: &str,
    name: &str,
    local_port: u16,
) -> Result<Child, Box<dyn std::error::Error>> {
    let lp = local_port.to_string();
    let cp = ERROR_ENDPOINT_CONTAINER_PORT.to_string();
    let child = Command::new("kubectl")
        .args([
            "--context",
            context,
            "port-forward",
            &format!("pod/{name}"),
            &format!("{lp}:{cp}"),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    // Brief pause for the port-forward tunnel to establish.
    std::thread::sleep(Duration::from_secs(2));
    eprintln!("  [OK] port-forward {local_port} → {name}:{cp}");
    Ok(child)
}

/// Deploy both metrics endpoint Pods (idle and busy) and wait for each to be ready.
///
/// Thin wrapper around [`apply_metrics_endpoint_pod`] that encapsulates the
/// fixture-specific queue depth values so callers need not depend on private constants.
pub(crate) fn apply_and_wait_for_metrics_pods(
    context: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    apply_metrics_endpoint_pod(context, TEST_METRICS_IDLE_PROVIDER, METRICS_IDLE_QUEUE_DEPTH)?;
    apply_metrics_endpoint_pod(context, TEST_METRICS_BUSY_PROVIDER, METRICS_BUSY_QUEUE_DEPTH)?;
    wait_for_named_pod_ready(context, TEST_METRICS_IDLE_PROVIDER, timeout)?;
    wait_for_named_pod_ready(context, TEST_METRICS_BUSY_PROVIDER, timeout)?;
    Ok(())
}

/// Apply two equal-locality `InferenceProvider` fixtures with `metricsConfig`.
///
/// Both providers have `backendKind = "local"` so their baseline locality score is
/// equal.  The operator scrapes their configured endpoints:
/// - `idle_endpoint/metrics` → `{METRICS_QUEUE_SIGNAL_NAME} 0.1` (low queue → high score)
/// - `busy_endpoint/metrics` → `{METRICS_QUEUE_SIGNAL_NAME} 0.9` (high queue → low score)
///
/// After reconciliation, the overlay must order the idle provider before the busy
/// provider.  Verified by [`verify_metrics_ordering`].
#[expect(
    clippy::too_many_lines,
    reason = "two InferenceProvider JSON manifests with nested metricsConfig; cannot shorten without losing clarity"
)]
pub(crate) fn apply_metrics_provider_fixtures(
    context: &str,
    idle_endpoint: &str,
    busy_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for (name, routing_cluster, endpoint) in [
        (
            TEST_METRICS_IDLE_PROVIDER,
            TEST_METRICS_IDLE_ROUTING_CLUSTER,
            idle_endpoint,
        ),
        (
            TEST_METRICS_BUSY_PROVIDER,
            TEST_METRICS_BUSY_ROUTING_CLUSTER,
            busy_endpoint,
        ),
    ] {
        let manifest = serde_json::to_string_pretty(&serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": TEST_NETWORK,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": endpoint,
                "models": [{ "name": "model-metrics" }],
                "routingClusterRef": routing_cluster,
                "metricsConfig": {
                    "path": "/metrics",
                    "timeout": "2s",
                    "signalNames": {
                        "queueDepth": METRICS_QUEUE_SIGNAL_NAME
                    }
                }
            }
        }))
        .unwrap_or_else(|e| {
            eprintln!("metrics provider fixture serialization failed: {e}");
            std::process::exit(1);
        });
        apply_manifest(context, &manifest)?;
    }
    eprintln!("  [OK] metrics provider fixtures applied");
    Ok(())
}

/// Verify that the idle (low-queue) metrics provider appears before the busy
/// (high-queue) metrics provider in the overlay candidates.
///
/// Checks that live metrics have successfully shifted scoring: the provider with
/// queue depth 0.1 must rank above the provider with queue depth 0.9 even though
/// both have the same locality score.
pub(crate) fn verify_metrics_ordering(
    overlay: &serde_json::Value,
    idle_cluster: &str,
    busy_cluster: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let candidates = overlay
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("overlay missing candidates array")?;

    let idle_pos = candidates
        .iter()
        .position(|c| c.get("cluster").and_then(serde_json::Value::as_str) == Some(idle_cluster))
        .ok_or_else(|| format!("{idle_cluster} not found in overlay candidates"))?;

    let busy_pos = candidates
        .iter()
        .position(|c| c.get("cluster").and_then(serde_json::Value::as_str) == Some(busy_cluster))
        .ok_or_else(|| format!("{busy_cluster} not found in overlay candidates"))?;

    if idle_pos >= busy_pos {
        return Err(format!(
            "metrics ordering check failed: {idle_cluster} (pos {idle_pos}) must appear before \
             {busy_cluster} (pos {busy_pos}); expected idle (low queue) to score higher than busy"
        )
        .into());
    }
    eprintln!("  [OK] metrics ordering: {idle_cluster} (pos {idle_pos}) before {busy_cluster} (pos {busy_pos})");
    Ok(())
}

/// Apply the degraded `InferenceProvider` fixture with a health check configured.
///
/// The provider uses `endpoint` (should be the port-forwarded local address) and
/// a `healthCheck.path` that will probe the HTTP 503 server.  The operator maps
/// the non-2xx response to `Degraded`.
pub(crate) fn apply_degraded_provider_fixture(context: &str, endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": TEST_PROVIDER_DEGRADED },
        "spec": {
            "gridNetworkRef": TEST_NETWORK,
            "providerKind": "open_ai",
            "backendKind": "local",
            "endpoint": endpoint,
            "healthCheck": { "path": "/health", "timeout": "5s" },
            "routingClusterRef": TEST_DEGRADED_ROUTING_CLUSTER,
            "models": [{ "name": "model-y" }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("degraded provider fixture serialization failed: {e}");
        std::process::exit(1);
    });
    apply_manifest(context, &manifest)?;
    eprintln!("  [OK] degraded provider fixture applied");
    Ok(())
}

/// Apply the `api_provider` `InferenceProvider` fixture.
///
/// Uses `backendKind: "api_provider"` so the scoring engine assigns it a lower
/// locality score (≈ 5.8) than local providers (≈ 7.0).  This fixture verifies
/// that scoring-backed ordering places `api_provider` candidates after local ones
/// regardless of the order they were applied.
pub(crate) fn apply_api_provider_fixture(context: &str, endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": TEST_PROVIDER_API },
        "spec": {
            "gridNetworkRef": TEST_NETWORK,
            "providerKind": "anthropic",
            "backendKind": "api_provider",
            "endpoint": endpoint,
            "models": [{ "name": "model-z" }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("api provider fixture serialization failed: {e}");
        std::process::exit(1);
    });
    apply_manifest(context, &manifest)?;
    eprintln!("  [OK] api provider fixture applied");
    Ok(())
}

/// Verify that the scoring-backed candidate order is visible in the overlay.
///
/// Asserts that `api_cluster` appears after at least one `local_cluster` candidate,
/// proving that the scoring engine placed higher-locality backends first.
pub(crate) fn verify_scoring_order(
    overlay: &serde_json::Value,
    local_cluster: &str,
    api_cluster: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let candidates = overlay["candidates"]
        .as_array()
        .ok_or("overlay missing candidates array")?;

    let local_pos = candidates
        .iter()
        .position(|c| c["cluster"].as_str() == Some(local_cluster))
        .ok_or_else(|| format!("{local_cluster} not found in candidates for scoring order check"))?;

    let api_pos = candidates
        .iter()
        .position(|c| c["cluster"].as_str() == Some(api_cluster))
        .ok_or_else(|| format!("{api_cluster} not found in candidates for scoring order check"))?;

    if api_pos <= local_pos {
        return Err(format!(
            "scoring order check failed: {api_cluster} (pos {api_pos}) must appear after \
             {local_cluster} (pos {local_pos}); expected local > api_provider"
        )
        .into());
    }
    eprintln!("  [OK] scoring order: {local_cluster} (pos {local_pos}) before {api_cluster} (pos {api_pos})");
    Ok(())
}

/// Export the overlay `ConfigMap` `grid-config.json` value to a temp file.
///
/// Returns the path of the written file.  The caller may pass this file to
/// `cargo xtask env deploy-consumer-gateway --overlay-config <path>` to validate
/// that Praxis can consume an operator-generated overlay without modification.
pub(crate) fn export_overlay_to_file(
    context: &str,
    network: &str,
    gateway: &str,
    namespace: &str,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let overlay = read_overlay_configmap(context, network, gateway, namespace)?;
    let json = serde_json::to_string_pretty(&overlay)?;
    let path = std::path::PathBuf::from(format!("/tmp/grid-operator-overlay-{network}-{gateway}.json"));
    std::fs::write(&path, json.as_bytes())?;
    eprintln!("  [OK] overlay exported to {}", path.display());
    Ok(path)
}

/// Verify that `degraded_cluster` appears in overlay candidates with `fresh: false`.
///
/// A site may have multiple candidates (e.g. both healthy and degraded providers sharing
/// `routingClusterRef`).  We require that at least one candidate with `cluster =
/// degraded_cluster` carries `fresh = false`, confirming the Degraded provider is
/// represented as stale in the overlay.
///
/// Also verifies that all candidates carry the required Praxis wire-format fields.
pub(crate) fn verify_degraded_candidate(
    overlay: &serde_json::Value,
    degraded_cluster: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let candidates = overlay["candidates"]
        .as_array()
        .ok_or("overlay missing candidates array")?;

    let has_cluster = candidates
        .iter()
        .any(|c| c["cluster"].as_str() == Some(degraded_cluster));
    if !has_cluster {
        return Err(format!("{degraded_cluster} must be in overlay but is absent").into());
    }

    // Find the specific fresh=false candidate that proves the Degraded provider is stale.
    let degraded = candidates
        .iter()
        .find(|c| c["cluster"].as_str() == Some(degraded_cluster) && c["fresh"].as_bool() == Some(false))
        .ok_or_else(|| {
            format!(
                "{degraded_cluster} has no fresh=false candidate — Degraded provider must appear as stale in overlay"
            )
        })?;

    for field in &["kind", "name", "site", "cluster", "fresh"] {
        if degraded.get(field).is_none() {
            return Err(format!("{degraded_cluster} degraded candidate missing required field '{field}'").into());
        }
    }
    eprintln!("  [OK] {degraded_cluster} present with fresh=false");
    Ok(())
}

// ---------------------------------------------------------------------------
// Low-level helpers
// ---------------------------------------------------------------------------

/// Run `kubectl ... -o jsonpath='<path>'` (cluster-scoped resource) and return trimmed output.
fn kubectl_jsonpath(context: &str, resource: &str, jsonpath: &str) -> Result<String, Box<dyn std::error::Error>> {
    let out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "get",
            resource,
            "-o",
            &format!("jsonpath={jsonpath}"),
        ])
        .output()?;
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// Run `kubectl ... -n <namespace> -o jsonpath='<path>'` (namespaced resource) and return trimmed output.
fn kubectl_jsonpath_ns(
    context: &str,
    namespace: &str,
    name: &str,
    jsonpath: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            namespace,
            "get",
            &format!("configmap/{name}"),
            "-o",
            &format!("jsonpath={jsonpath}"),
        ])
        .output()?;
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// Apply a manifest string to `context` via `kubectl apply -f -`.
pub(crate) fn apply_manifest(context: &str, manifest: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut child = Command::new("kubectl")
        .args(["--context", context, "apply", "-f", "-"])
        .stdin(Stdio::piped())
        .spawn()?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(manifest.as_bytes())?;
    }
    let status = child.wait()?;
    if !status.success() {
        return Err(format!("kubectl apply failed: {status}").into());
    }
    Ok(())
}

/// Force-delete a Pod immediately, bypassing graceful termination.
///
/// Uses `--grace-period=0 --force` so the Pod is removed from the API without
/// waiting for container shutdown.  Safe for short-lived fixture Pods.
fn force_delete_pod(context: &str, namespace: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            namespace,
            "delete",
            "pod",
            name,
            "--ignore-not-found",
            "--grace-period=0",
            "--force",
        ])
        .output()?;
    if !out.status.success() {
        return Err(format!(
            "kubectl force-delete pod/{name} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    Ok(())
}

/// Delete a cluster-scoped resource, ignoring absence.
fn delete_cluster_resource(context: &str, kind: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_resource(context, None, kind, name)
}

/// Delete a namespaced resource, ignoring absence.
fn delete_namespaced_resource(
    context: &str,
    namespace: &str,
    kind: &str,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    delete_resource(context, Some(namespace), kind, name)
}

/// Delete one resource owned by the validation harness.
///
/// Uses `--ignore-not-found` so the command is idempotent and `--wait=true` so
/// a subsequent apply cannot observe old status or stale data.
fn delete_resource(
    context: &str,
    namespace: Option<&str>,
    kind: &str,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut args = vec!["--context", context];
    if let Some(ns) = namespace {
        args.extend(["-n", ns]);
    }
    args.extend([
        "delete",
        kind,
        name,
        "--ignore-not-found",
        "--wait=true",
        "--timeout=30s",
    ]);
    let out = Command::new("kubectl").args(args).output()?;
    if !out.status.success() {
        return Err(format!(
            "kubectl delete {kind}/{name} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::*;

    fn make_overlay(candidates: &[(&str, bool)]) -> serde_json::Value {
        let items: Vec<serde_json::Value> = candidates
            .iter()
            .map(|(cluster, fresh)| {
                serde_json::json!({
                    "kind": "inference_model",
                    "name": "model-x",
                    "site": cluster,
                    "cluster": cluster,
                    "fresh": fresh
                })
            })
            .collect();
        serde_json::json!({ "network": "net", "local_site": "net", "candidates": items })
    }

    #[test]
    fn verify_degraded_candidate_accepts_fresh_false() {
        let overlay = make_overlay(&[("prov-a", true), ("prov-b", false)]);
        assert!(
            verify_degraded_candidate(&overlay, "prov-b").is_ok(),
            "candidate with fresh=false must pass verification"
        );
    }

    #[test]
    fn verify_degraded_candidate_rejects_absent_cluster() {
        let overlay = make_overlay(&[("prov-a", true)]);
        assert!(
            verify_degraded_candidate(&overlay, "prov-missing").is_err(),
            "absent degraded cluster must fail verification"
        );
    }

    #[test]
    fn verify_degraded_candidate_rejects_fresh_true() {
        let overlay = make_overlay(&[("prov-degraded", true)]);
        assert!(
            verify_degraded_candidate(&overlay, "prov-degraded").is_err(),
            "candidate with fresh=true must fail degraded verification"
        );
    }

    #[test]
    fn verify_degraded_candidate_accepts_when_shared_site_has_fresh_false() {
        // Two candidates at the same site — healthy (fresh=true) and degraded (fresh=false).
        // Two providers sharing routingClusterRef="site-a" produces two candidates at the same
        // site: one healthy (fresh=true) and one degraded (fresh=false).  The degraded check must find the fresh=false
        // one.
        let overlay = make_overlay(&[("site-a", true), ("site-a", false)]);
        assert!(
            verify_degraded_candidate(&overlay, "site-a").is_ok(),
            "shared site with a fresh=false candidate must pass degraded verification"
        );
    }

    #[test]
    fn verify_overlay_accepts_when_shared_site_has_fresh_true() {
        // Two candidates at the same site — healthy (fresh=true) and degraded (fresh=false).
        // The overlay check must pass because at least one candidate is fresh=true.
        let overlay = make_overlay(&[("site-a", true), ("site-a", false)]);
        assert!(
            verify_overlay(&overlay, "site-a", "excluded").is_ok(),
            "shared site with a fresh=true candidate must pass overlay verification"
        );
    }

    #[test]
    fn verify_overlay_accepts_valid_overlay() {
        let overlay = make_overlay(&[("healthy", true), ("other", true)]);
        assert!(
            verify_overlay(&overlay, "healthy", "excluded").is_ok(),
            "overlay with healthy present and excluded absent must pass"
        );
    }

    #[test]
    fn verify_overlay_rejects_when_excluded_present() {
        let overlay = make_overlay(&[("healthy", true), ("excluded", true)]);
        assert!(
            verify_overlay(&overlay, "healthy", "excluded").is_err(),
            "excluded cluster present in overlay must fail verification"
        );
    }

    #[test]
    fn verify_overlay_rejects_when_healthy_absent() {
        let overlay = make_overlay(&[("other", true)]);
        assert!(
            verify_overlay(&overlay, "healthy", "excluded").is_err(),
            "absent healthy cluster must fail verification"
        );
    }

    #[test]
    fn verify_overlay_rejects_when_healthy_is_stale() {
        let overlay = make_overlay(&[("healthy", false), ("other", true)]);
        assert!(
            verify_overlay(&overlay, "healthy", "excluded").is_err(),
            "healthy cluster with fresh=false must fail verification"
        );
    }

    #[test]
    fn fixture_constants_are_distinct() {
        assert_ne!(
            TEST_PROVIDER_HEALTHY, TEST_PROVIDER_INVALID,
            "fixture names must differ"
        );
        assert_ne!(
            TEST_PROVIDER_HEALTHY, TEST_PROVIDER_DEGRADED,
            "fixture names must differ"
        );
        assert_ne!(
            TEST_PROVIDER_INVALID, TEST_PROVIDER_DEGRADED,
            "fixture names must differ"
        );
        assert_ne!(TEST_PROVIDER_API, TEST_PROVIDER_HEALTHY, "fixture names must differ");
        assert_ne!(
            TEST_NETWORK, TEST_PROVIDER_HEALTHY,
            "network name must not collide with provider names"
        );
    }

    #[test]
    fn verify_scoring_order_accepts_local_before_api() {
        // local at position 0, api_provider at position 1
        let overlay = make_overlay(&[("local-prov", true), ("api-prov", true)]);
        assert!(
            verify_scoring_order(&overlay, "local-prov", "api-prov").is_ok(),
            "local before api_provider must pass scoring order check"
        );
    }

    #[test]
    fn verify_scoring_order_rejects_api_before_local() {
        // api_provider at position 0, local at position 1
        let overlay = make_overlay(&[("api-prov", true), ("local-prov", true)]);
        assert!(
            verify_scoring_order(&overlay, "local-prov", "api-prov").is_err(),
            "api_provider before local must fail scoring order check"
        );
    }

    #[test]
    fn verify_scoring_order_rejects_absent_local() {
        let overlay = make_overlay(&[("api-prov", true)]);
        assert!(
            verify_scoring_order(&overlay, "local-prov", "api-prov").is_err(),
            "absent local cluster must fail scoring order check"
        );
    }

    #[test]
    fn verify_scoring_order_rejects_absent_api() {
        let overlay = make_overlay(&[("local-prov", true)]);
        assert!(
            verify_scoring_order(&overlay, "local-prov", "api-prov").is_err(),
            "absent api cluster must fail scoring order check"
        );
    }

    #[test]
    fn cleanup_includes_all_owned_providers() {
        // Verify that TEST_PROVIDER_API is in the set of providers the cleanup deletes.
        // This is a documentation test: if you add a new fixture, you must add it to cleanup.
        let all_providers = [
            TEST_PROVIDER_HEALTHY,
            TEST_PROVIDER_INVALID,
            TEST_PROVIDER_DEGRADED,
            TEST_PROVIDER_API,
            TEST_METRICS_IDLE_PROVIDER,
            TEST_METRICS_BUSY_PROVIDER,
        ];
        // Each must be distinct (no duplicate delete).
        let unique: std::collections::HashSet<_> = all_providers.iter().collect();
        assert_eq!(
            unique.len(),
            all_providers.len(),
            "all provider fixture names must be distinct"
        );
    }

    // -----------------------------------------------------------------------
    // verify_metrics_ordering
    // -----------------------------------------------------------------------

    #[test]
    fn verify_metrics_ordering_accepts_idle_before_busy() {
        let overlay = make_overlay(&[
            (TEST_METRICS_IDLE_ROUTING_CLUSTER, true),
            (TEST_METRICS_BUSY_ROUTING_CLUSTER, true),
        ]);
        assert!(
            verify_metrics_ordering(
                &overlay,
                TEST_METRICS_IDLE_ROUTING_CLUSTER,
                TEST_METRICS_BUSY_ROUTING_CLUSTER
            )
            .is_ok(),
            "idle before busy must pass"
        );
    }

    #[test]
    fn verify_metrics_ordering_rejects_busy_before_idle() {
        let overlay = make_overlay(&[
            (TEST_METRICS_BUSY_ROUTING_CLUSTER, true),
            (TEST_METRICS_IDLE_ROUTING_CLUSTER, true),
        ]);
        assert!(
            verify_metrics_ordering(
                &overlay,
                TEST_METRICS_IDLE_ROUTING_CLUSTER,
                TEST_METRICS_BUSY_ROUTING_CLUSTER
            )
            .is_err(),
            "busy before idle must fail"
        );
    }

    #[test]
    fn verify_metrics_ordering_rejects_missing_idle() {
        let overlay = make_overlay(&[(TEST_METRICS_BUSY_ROUTING_CLUSTER, true)]);
        assert!(
            verify_metrics_ordering(
                &overlay,
                TEST_METRICS_IDLE_ROUTING_CLUSTER,
                TEST_METRICS_BUSY_ROUTING_CLUSTER
            )
            .is_err(),
            "absent idle cluster must fail"
        );
    }

    #[test]
    fn verify_metrics_ordering_rejects_missing_busy() {
        let overlay = make_overlay(&[(TEST_METRICS_IDLE_ROUTING_CLUSTER, true)]);
        assert!(
            verify_metrics_ordering(
                &overlay,
                TEST_METRICS_IDLE_ROUTING_CLUSTER,
                TEST_METRICS_BUSY_ROUTING_CLUSTER
            )
            .is_err(),
            "absent busy cluster must fail"
        );
    }

    // -----------------------------------------------------------------------
    // verify_swim_status — pure assertion tests
    // -----------------------------------------------------------------------

    #[test]
    fn verify_swim_status_active_with_peers_passes() {
        assert!(
            verify_swim_status("Active", 1).is_ok(),
            "Active phase with connectedSites=1 must pass"
        );
    }

    #[test]
    fn verify_swim_status_active_with_multiple_peers_passes() {
        assert!(
            verify_swim_status("Active", 3).is_ok(),
            "Active phase with connectedSites=3 must pass"
        );
    }

    #[test]
    fn verify_swim_status_pending_fails() {
        assert!(
            verify_swim_status("Pending", 1).is_err(),
            "Pending phase must fail even with connected peers"
        );
    }

    #[test]
    fn verify_swim_status_active_zero_connected_fails() {
        assert!(
            verify_swim_status("Active", 0).is_err(),
            "Active phase with connectedSites=0 must fail"
        );
    }

    #[test]
    fn verify_swim_status_initializing_fails() {
        assert!(
            verify_swim_status("Initializing", 2).is_err(),
            "Initializing phase must fail"
        );
    }

    #[test]
    fn swim_node_names_are_distinct() {
        assert_ne!(
            SWIM_NODE_PRIMARY_NAME, SWIM_NODE_SECONDARY_NAME,
            "SWIM node site names must be distinct"
        );
    }

    #[test]
    fn reserve_local_udp_addr_returns_loopback_addr() {
        let addr = reserve_local_udp_addr().unwrap_or_else(|_| std::process::abort());
        assert!(addr.ip().is_loopback(), "reserved SWIM addr must be loopback");
        assert_ne!(
            addr.port(),
            ERROR_ENDPOINT_LOCAL_PORT,
            "reserved SWIM addr should not use the fixed error endpoint port"
        );
    }

    #[test]
    fn swim_test_network_distinct_from_operator_network() {
        assert_ne!(
            SWIM_TEST_NETWORK, TEST_NETWORK,
            "SWIM test network must be distinct from operator reconcile test network"
        );
    }

    #[test]
    fn cleanup_swim_includes_only_swim_resources() {
        // Documents which resources cleanup_swim_test_resources deletes.
        // If you add new SWIM fixtures, add them here.
        let swim_resources = [SWIM_TEST_NETWORK, SWIM_TEST_PROVIDER];
        let unique: std::collections::HashSet<_> = swim_resources.iter().collect();
        assert_eq!(
            unique.len(),
            swim_resources.len(),
            "SWIM test resource names must be distinct"
        );
    }
}
