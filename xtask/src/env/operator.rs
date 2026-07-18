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
    net::{SocketAddr, UdpSocket},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use sha2::{Digest as _, Sha256};

use super::{kind, kubectl};

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

/// The model name served by the API-provider fallback fixture.
///
/// Distinct from the self-hosted models so the consumer `grid_route` can route
/// it to the API-provider cluster without ambiguity.
pub(crate) const API_FALLBACK_MODEL: &str = "model-z";

// ---------------------------------------------------------------------------
// API-provider credential constants
// ---------------------------------------------------------------------------

/// Name of the Kubernetes Secret that holds the API-provider bearer token.
///
/// Created by the xtask validation harness before the operator reconcile so
/// `InferenceProvider.spec.auth.secretRef` can reference it immediately.
/// The validation reads the Secret back to prove the Secret-backed
/// CRD-to-Praxis-config flow, not a hardcoded generated config value.
pub(crate) const API_PROVIDER_SECRET_NAME: &str = "op-e2e-api-provider-creds";

/// Namespace of the API-provider credential Secret.
pub(crate) const API_PROVIDER_SECRET_NS: &str = "default";

/// Key within the API-provider credential Secret that holds the bearer token.
pub(crate) const API_PROVIDER_SECRET_KEY: &str = "token";

/// Name of the operator-generated consumer Praxis `ConfigMap` in the consumer-config validation.
///
/// Distinct from `praxis-consumer-config` (the xtask-generated name) so the two can coexist
/// during the transition while the operator-generated config is shape-validated without
/// replacing the xtask-generated config used by the live consumer pod.
pub(crate) const TEST_CONSUMER_CONFIGMAP_NAME: &str = "op-e2e-consumer-config";

// ---------------------------------------------------------------------------
// Full-grid routing validation constants
// ---------------------------------------------------------------------------

/// `GridNetwork` name used by the full-grid routing validation.
pub(crate) const FULL_GRID_NETWORK: &str = "op-e2e-full-grid-net";

/// Gateway reference name inside the full-grid `GridNetwork`.
pub(crate) const FULL_GRID_GW: &str = "op-e2e-gw";

/// `InferenceProvider` name for the local/self-hosted east provider in full-grid.
pub(crate) const FULL_GRID_PROVIDER_EAST: &str = "op-e2e-fg-east";

/// `InferenceProvider` name for the remote/self-hosted west provider in full-grid.
pub(crate) const FULL_GRID_PROVIDER_WEST: &str = "op-e2e-fg-west";

/// `InferenceProvider` name for the cloud-managed provider in full-grid.
pub(crate) const FULL_GRID_PROVIDER_CLOUD: &str = "op-e2e-fg-cloud";

/// `InferenceProvider` name for the API-provider in full-grid.
pub(crate) const FULL_GRID_PROVIDER_API: &str = "op-e2e-fg-api";

/// Model served by the local/east provider in full-grid.  Distinct per backend kind.
pub(crate) const FULL_GRID_MODEL_EAST: &str = "model-east";

/// Model served by the remote/west provider in full-grid.
pub(crate) const FULL_GRID_MODEL_WEST: &str = "model-west";

/// Model served by the cloud-managed provider in full-grid.
pub(crate) const FULL_GRID_MODEL_CLOUD: &str = "model-cloud";

/// Model served by the API-provider in full-grid.
pub(crate) const FULL_GRID_MODEL_API: &str = "model-api";

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
// Metrics-driven routing validation constants
// ---------------------------------------------------------------------------

/// `GridNetwork` name for the metrics-driven routing validation.
///
/// Separate from [`TEST_NETWORK`] and [`SWIM_TEST_NETWORK`] to avoid resource
/// collisions when the single-provider validation and metrics-routing validation
/// run against the same cluster.
pub(crate) const METRICS_ROUTING_NETWORK: &str = "op-e2e-metrics-routing-net";

/// Gateway reference name inside the metrics-routing `GridNetwork`.
pub(crate) const METRICS_ROUTING_GW: &str = "op-e2e-gw";

/// `InferenceProvider` for the east/low-queue site in metrics-routing.
pub(crate) const METRICS_ROUTING_EAST_PROVIDER: &str = "op-e2e-mr-east";

/// `InferenceProvider` for the west/high-queue site in metrics-routing.
pub(crate) const METRICS_ROUTING_WEST_PROVIDER: &str = "op-e2e-mr-west";

/// Model name shared by both east and west providers in metrics-routing.
///
/// Both sites serve this model so only the scoring signal (queue depth) determines
/// which overlay candidate appears first and which provider handles the request.
pub(crate) const METRICS_ROUTING_MODEL: &str = "model-metrics-shared";

/// Pod name for the east-site metrics HTTP server.
pub(crate) const METRICS_ROUTING_EAST_POD: &str = "op-e2e-mr-metrics-east";

/// Pod name for the west-site metrics HTTP server.
pub(crate) const METRICS_ROUTING_WEST_POD: &str = "op-e2e-mr-metrics-west";

/// Host-side port for the east-site metrics pod port-forward.
///
/// Chosen to avoid conflicts with the single-provider metrics ports
/// (`18501`, `18502`) used by `run_operator_reconcile`.
pub(crate) const METRICS_ROUTING_EAST_PORT: u16 = 18_611;

/// Host-side port for the west-site metrics pod port-forward.
pub(crate) const METRICS_ROUTING_WEST_PORT: u16 = 18_612;

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

/// `GridNetwork` resource name used by the CRD-seed SWIM validation.
///
/// Kept distinct from [`SWIM_TEST_NETWORK`] so both validations can coexist.
/// The CRD-seed test uses `spec.seeds` instead of `GRID_SWIM_SEEDS` to drive
/// peer discovery, proving the CRD path rather than the env-var path.
pub(crate) const CRD_SEEDS_TEST_NETWORK: &str = "op-e2e-swim-crd-seeds-net";

// ---------------------------------------------------------------------------
// SWIM overlay validation constants
// ---------------------------------------------------------------------------

/// `GridNetwork` resource name used by the SWIM overlay validation.
///
/// Kept distinct from `SWIM_TEST_NETWORK` and `TEST_NETWORK` to avoid resource
/// collisions when multiple validations run in the same cluster.
pub(crate) const SWIM_OVERLAY_NETWORK: &str = "op-e2e-swim-overlay";

// ---------------------------------------------------------------------------
// Three-node SWIM mesh validation constants
// ---------------------------------------------------------------------------

/// `GridNetwork` resource name used by the three-node SWIM mesh validation.
pub(crate) const SWIM_MESH_NETWORK: &str = "op-e2e-swim-mesh-net";

/// Gateway reference name in the three-node mesh `GridNetwork` fixture.
pub(crate) const SWIM_MESH_GW: &str = "op-e2e-swim-mesh-gw";

/// SWIM site identity for node A (origin; seeds nobody).
pub(crate) const SWIM_MESH_SITE_A: &str = "swim-mesh-a";

/// SWIM site identity for node B (bridge; seeds A).
pub(crate) const SWIM_MESH_SITE_B: &str = "swim-mesh-b";

/// SWIM site identity for node C (leaf; seeds B only, not A).
pub(crate) const SWIM_MESH_SITE_C: &str = "swim-mesh-c";

/// `InferenceProvider` published by node C in the three-node mesh fixture.
pub(crate) const SWIM_MESH_PROVIDER_C: &str = "op-e2e-swim-mesh-prov-c";

/// Model served by the node-C provider fixture.
pub(crate) const SWIM_MESH_MODEL_C: &str = "model-swim-mesh-c";

/// `GridNetwork` name used for cross-network isolation in the three-node mesh proof.
pub(crate) const SWIM_MESH_WRONG_NETWORK: &str = "op-e2e-swim-mesh-wrong-net";

/// `InferenceProvider` applied to the wrong network; must not appear in the main overlay.
pub(crate) const SWIM_MESH_WRONG_PROVIDER: &str = "op-e2e-swim-mesh-prov-wrong";

/// Model served by the wrong-network provider; must not appear in the main overlay.
pub(crate) const SWIM_MESH_WRONG_MODEL: &str = "model-swim-mesh-wrong";

// ---------------------------------------------------------------------------
// Fingerprint trust promotion validation constants
// ---------------------------------------------------------------------------

/// `GridNetwork` resource name for the trust fingerprint promotion validation.
pub(crate) const SWIM_TRUST_NETWORK: &str = "op-e2e-swim-trust-net";

/// Gateway reference name in the trust fingerprint test `GridNetwork`.
pub(crate) const SWIM_TRUST_GW: &str = "op-e2e-swim-trust-gw";

/// SWIM site identity for node A (primary — renders overlay, no TLS cert).
pub(crate) const SWIM_TRUST_SITE_A: &str = "swim-trust-a";

/// SWIM site identity for node B (secondary — has TLS cert, whose promotion is tested).
pub(crate) const SWIM_TRUST_SITE_B: &str = "swim-trust-b";

/// `InferenceProvider` published by site B in the trust fingerprint test.
pub(crate) const SWIM_TRUST_PROVIDER_B: &str = "op-e2e-swim-trust-prov";

/// Model served by the trust test provider fixture.
pub(crate) const SWIM_TRUST_MODEL_B: &str = "model-swim-trust";

/// Kubernetes Secret name for site B's grid CA certificate.
pub(crate) const SWIM_TRUST_CA_SECRET: &str = "swim-trust-ca";

/// Kubernetes Secret name for site B's site certificate.
pub(crate) const SWIM_TRUST_SITE_SECRET: &str = "swim-trust-b-cert";

/// `GridNetwork` name used by the SWIM transport encryption validation.
pub(crate) const SWIM_ENCRYPT_NETWORK: &str = "op-e2e-swim-encrypt-net";

/// Gateway reference name for the SWIM encryption validation overlay.
pub(crate) const SWIM_ENCRYPT_GW: &str = "op-e2e-swim-encrypt-gw";

/// Primary keyed node name (site A).
pub(crate) const SWIM_ENCRYPT_NODE_A: &str = "swim-encrypt-a";
/// Secondary keyed node name (site B — same key as A).
pub(crate) const SWIM_ENCRYPT_NODE_B: &str = "swim-encrypt-b";
/// Wrong-key node name (site C — different key, must be rejected).
pub(crate) const SWIM_ENCRYPT_NODE_WRONG: &str = "swim-encrypt-wrong";
/// Plaintext node name (site D — no key, must be rejected by keyed cluster).
pub(crate) const SWIM_ENCRYPT_NODE_PLAIN: &str = "swim-encrypt-plain";

/// `InferenceProvider` name for the primary keyed node (A).
pub(crate) const SWIM_ENCRYPT_PROVIDER_A: &str = "op-e2e-swim-encrypt-prov-a";
/// Model name served by the primary keyed provider.
pub(crate) const SWIM_ENCRYPT_MODEL_A: &str = "model-swim-encrypt-a";

/// Secret name used by the SWIM encryption `SecretRef` validation.
pub(crate) const SWIM_ENCRYPT_KEY_SECRET: &str = "op-e2e-swim-encrypt-key";

/// Gateway reference name used in the SWIM overlay `GridNetwork` fixture.
pub(crate) const SWIM_OVERLAY_GW: &str = "op-e2e-gw";

/// `InferenceProvider` name for the SWIM overlay validation fixture.
pub(crate) const SWIM_OVERLAY_PROVIDER: &str = "op-e2e-swim-ov-prov";

/// Model name served by the SWIM overlay provider fixture.
pub(crate) const SWIM_OVERLAY_MODEL: &str = "model-swim-overlay";

// ---------------------------------------------------------------------------
// SWIM routing validation constants
// ---------------------------------------------------------------------------

/// `GridNetwork` name used by the cross-cluster SWIM routing validation.
///
/// Both the east and west operators reconcile a `GridNetwork` with this name
/// (in their respective clusters).  The east operator generates the overlay
/// `ConfigMap`; the west operator publishes CRDT state that populates the
/// remote candidates in that overlay.
pub(crate) const SWIM_ROUTING_NETWORK: &str = "op-e2e-swim-routing";

/// Gateway reference name used in the east `GridNetwork` fixture.
pub(crate) const SWIM_ROUTING_GW: &str = "op-e2e-gw";

/// `InferenceProvider` applied on the east (primary) cluster.
pub(crate) const SWIM_ROUTING_EAST_PROVIDER: &str = "op-e2e-swim-rt-east";

/// `InferenceProvider` applied on the west (peer) cluster.
pub(crate) const SWIM_ROUTING_WEST_PROVIDER: &str = "op-e2e-swim-rt-west";

/// Time to wait after the secondary operator announces to the primary before
/// applying the `GridNetwork` fixture.
///
/// SWIM gossip converges in 1–3 s with foca's default probe interval; 10 s
/// provides a comfortable margin on slow CI hosts.
pub(crate) const SWIM_CONVERGENCE_WAIT: Duration = Duration::from_secs(10);

/// Timeout for polling the `GridNetwork` status to reach `Active`.
pub(crate) const SWIM_STATUS_POLL_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Site join / discovery validation constants
// ---------------------------------------------------------------------------

/// `GridNetwork` name used by the site-join-discovery validation.
pub(crate) const SITE_JOIN_NETWORK: &str = "op-e2e-sjd-net";

/// `GridNetwork` name used for the cross-network isolation check.
pub(crate) const SITE_JOIN_WRONG_NETWORK: &str = "op-e2e-sjd-wrong-net";

/// Gateway reference name used in the site-join-discovery `GridNetwork`.
pub(crate) const SITE_JOIN_GW: &str = "op-e2e-sjd-gw";

/// `GridSite` name for the primary (already-established) site.
pub(crate) const SITE_JOIN_PRIMARY_SITE: &str = "op-e2e-sjd-primary";

/// `GridSite` name for the joining (new) site.
pub(crate) const SITE_JOIN_JOINING_SITE: &str = "op-e2e-sjd-joining";

/// `GridSite` name used for the cross-network isolation check.
pub(crate) const SITE_JOIN_WRONG_SITE: &str = "op-e2e-sjd-wrong";

/// `InferenceProvider` name for the primary site's local provider.
pub(crate) const SITE_JOIN_PRIMARY_PROVIDER: &str = "op-e2e-sjd-prov";

/// `InferenceProvider` name for the joining site's local provider.
pub(crate) const SITE_JOIN_JOINING_PROVIDER: &str = "op-e2e-sjd-prov-joining";

/// `InferenceProvider` name for the wrong-network isolation test.
pub(crate) const SITE_JOIN_WRONG_PROVIDER: &str = "op-e2e-sjd-prov-wrong";

/// Model name served by the primary site's provider.
pub(crate) const SITE_JOIN_PRIMARY_MODEL: &str = "model-sjd-primary";

/// Model name served by the joining site's provider.
pub(crate) const SITE_JOIN_JOINING_MODEL: &str = "model-sjd-joining";

/// Metadata label key used in `GridSite` objects to enable per-site `siteSelector` matching.
///
/// The value is the site role string (e.g. `"primary"`, `"joining"`, `"wrong"`).
/// Harness-only: production site labels are not required to follow this pattern.
pub(crate) const SITE_JOIN_LABEL_KEY: &str = "grid.praxis-proxy.io/sjd-site";

/// Egress address for the primary site (Kind east-cluster node IP + TLS port).
///
/// This is metadata used to populate `GridSite.spec.egress.address` in the
/// validation harness.  It is not connected to during the test.
pub(crate) const SITE_JOIN_PRIMARY_EGRESS: &str = "172.18.0.4:8443";

/// Timeout for polling `GridSite` phase transitions.
///
/// 60 s covers two back-to-back reconcile windows plus the 5 s TCP probe timeout
/// in the `GridSite` controller.  The assertion itself (e.g. `Connecting`) is not
/// weakened — a longer window is required because the `Discovered → Connecting`
/// transition depends on the `GridNetwork` controller applying the egress spec,
/// which the `GridSite` controller then reconciles.  The primary anti-flakiness
/// measure is the cleanup in [`cleanup_site_join_resources`], which removes stale
/// auto-discovered `GridSite` resources that would otherwise start the controller
/// in an error-retry loop and consume most of the budget before the new site appears.
pub(crate) const SITE_JOIN_PHASE_POLL_TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Failover / lost-peer validation constants
// ---------------------------------------------------------------------------

/// `GridNetwork` name used by the failover-under-lost-peer validation.
pub(crate) const FAILOVER_NETWORK: &str = "op-e2e-failover-net";

/// Gateway reference name used in the failover `GridNetwork`.
pub(crate) const FAILOVER_GW: &str = "op-e2e-failover-gw";

/// `InferenceProvider` name for the east (local/primary) site.
pub(crate) const FAILOVER_EAST_PROVIDER: &str = "op-e2e-failover-east";

/// `InferenceProvider` name for the west (remote/joining) site.
pub(crate) const FAILOVER_WEST_PROVIDER: &str = "op-e2e-failover-west";

/// Model name served by the local east provider.
pub(crate) const FAILOVER_LOCAL_MODEL: &str = "model-failover-local";

/// Model name served by the remote west provider.
pub(crate) const FAILOVER_REMOTE_MODEL: &str = "model-failover-remote";

/// Shared model served by BOTH east (local, healthy fallback) and west (remote, stale after loss).
///
/// Used in the Packet 2 routing-away proof: before west dies both candidates are fresh=true;
/// after west dies east (fresh=true, higher score) sorts before west (fresh=false) for this model,
/// and a consumer request routes to the healthy east candidate.
pub(crate) const FAILOVER_SHARED_MODEL: &str = "model-failover-shared";

/// `InferenceProvider` name for the east healthy-fallback provider that serves `FAILOVER_SHARED_MODEL`.
pub(crate) const FAILOVER_SHARED_EAST_PROVIDER: &str = "op-e2e-failover-shared-east";

/// Time to wait after killing the west operator before triggering a reconcile.
///
/// With `foca::Config::simple()` the probe period is 1.5 s and
/// `suspect_to_down_after` is 3 s, so a killed process is declared `Dead`
/// within ~6 s.  20 s provides a safe margin on loaded CI hosts.
pub(crate) const SWIM_DEAD_MEMBER_WAIT: Duration = Duration::from_secs(20);

/// Timeout for polling the overlay until a remote candidate becomes `fresh=false`.
pub(crate) const FAILOVER_STALE_POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Wait after restarting a killed SWIM peer before checking recovery.
///
/// After restart the rejoining operator announces to seeds, SWIM detects it
/// `Alive`, the east operator reconciles (triggered by `GridNetwork` bump), and the
/// overlay is regenerated.  20 s matches [`SWIM_DEAD_MEMBER_WAIT`] and provides
/// enough headroom on loaded CI hosts.
pub(crate) const FAILOVER_REJOIN_WAIT: Duration = Duration::from_secs(20);

/// Timeout for polling the overlay until a remote candidate returns to `fresh=true`.
///
/// Recovery requires SWIM Alive detection + east reconcile + overlay render.
/// The reconcile is triggered by a `bump_gridnetwork` call immediately after
/// restart, so the principal variable is SWIM convergence time (~6 s max).
pub(crate) const FAILOVER_RECOVERY_POLL_TIMEOUT: Duration = Duration::from_secs(45);

// ---------------------------------------------------------------------------
// Stale-candidate TTL GC validation constants
// ---------------------------------------------------------------------------

/// `GridNetwork` name used by the stale-candidate TTL GC validation.
///
/// Kept distinct from all other test networks to avoid resource collisions.
pub(crate) const STALE_GC_NETWORK: &str = "op-e2e-stale-gc-net";

/// Gateway reference name used in the stale-GC `GridNetwork`.
pub(crate) const STALE_GC_GW: &str = "op-e2e-stale-gc-gw";

/// `InferenceProvider` name for the east (local/primary) site.
pub(crate) const STALE_GC_EAST_PROVIDER: &str = "op-e2e-stale-gc-east";

/// `InferenceProvider` name for the west (remote/joining) site.
pub(crate) const STALE_GC_WEST_PROVIDER: &str = "op-e2e-stale-gc-west";

/// Model served by the local east provider.
pub(crate) const STALE_GC_LOCAL_MODEL: &str = "model-stale-gc-local";

/// Model served by the remote west provider.
pub(crate) const STALE_GC_REMOTE_MODEL: &str = "model-stale-gc-remote";

/// TTL (seconds) configured in the test `GridNetwork`.
///
/// 5 seconds is short enough that after `SWIM_DEAD_MEMBER_WAIT` (20 s) the
/// SWIM Dead age (~14 s) reliably exceeds the TTL, triggering GC.  It is long
/// enough to avoid false eviction during the normal setup window when no peer
/// is Dead.
pub(crate) const STALE_GC_TTL_SECS: u32 = 5;

/// Timeout for polling until the stale remote candidate is absent from the overlay.
///
/// After killing west, the absence of the stale candidate requires:
/// - SWIM dead detection (≤ 6 s with `foca::Config::simple()`)
/// - Age accumulation past `STALE_GC_TTL_SECS` (5 s)
/// - At least one `GridNetwork` reconcile (triggered by periodic bump inside the helper)
///
/// 45 s provides a safe margin on loaded CI hosts.
pub(crate) const STALE_GC_ABSENT_POLL_TIMEOUT: Duration = Duration::from_secs(45);

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
    kubectl::apply_manifest(context, &crd_json)?;
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
    // Best-effort: remove operator-generated consumer ConfigMap from previous runs.
    let _unused = delete_namespaced_resource(context, TEST_GATEWAY_NS, "configmap", TEST_CONSUMER_CONFIGMAP_NAME);
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
    // Remove auto-discovered GridSites created during previous validation runs.
    cleanup_auto_discovered_gridsites_for_network(context, TEST_NETWORK);
    eprintln!("  [OK] stale validation resources removed");
    Ok(())
}

// ---------------------------------------------------------------------------
// Install/RBAC validation helpers
// ---------------------------------------------------------------------------

/// Apply the operator install manifests from `deploy/operator/`.
///
/// The namespace must exist before resources that target it, so the
/// namespace manifest is applied first as a separate step.
pub(crate) fn apply_install_manifests(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ns = Command::new("kubectl")
        .args(["apply", "--context", context, "-f", "deploy/operator/namespace.yaml"])
        .status()?;
    if !ns.success() {
        return Err("failed to apply deploy/operator/namespace.yaml".into());
    }
    let rest = Command::new("kubectl")
        .args(["apply", "--context", context, "-f", "deploy/operator/"])
        .status()?;
    if !rest.success() {
        return Err("failed to apply install manifests from deploy/operator/".into());
    }
    Ok(())
}

/// Remove the operator install manifests from the cluster.
pub(crate) fn cleanup_install_manifests(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let _status = Command::new("kubectl")
        .args([
            "delete",
            "--context",
            context,
            "-f",
            "deploy/operator/",
            "--ignore-not-found",
        ])
        .status()?;
    eprintln!("  [OK] install manifests removed");
    Ok(())
}

/// Clean up test resources created by `verify-operator-install-rbac`.
pub(crate) fn cleanup_install_rbac_test_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_namespaced_resource(
        context,
        "default",
        "configmap",
        "grid-overlay-op-e2e-rbac-net-op-e2e-rbac-gw",
    )?;
    delete_cluster_resource(context, "inferenceprovider", "op-e2e-rbac-prov")?;
    delete_cluster_resource(context, "gridnetwork", "op-e2e-rbac-net")?;
    cleanup_auto_discovered_gridsites_for_network(context, "op-e2e-rbac-net");
    eprintln!("  [OK] stale install/RBAC test resources removed");
    Ok(())
}

/// Container image name for the Grid operator.
pub(crate) const OPERATOR_IMAGE: &str = "grid-operator:latest";

/// Build the Grid operator container image.
///
/// Compiles the operator binary, copies it into a temporary build context,
/// and runs `docker build` (or `podman build`) to produce the image.
///
/// # Errors
///
/// Returns an error if the binary build, context setup, or image build fails.
pub(crate) fn build_operator_image() -> Result<(), Box<dyn std::error::Error>> {
    ensure_operator_binary_built()?;
    let ctx_dir = tempfile::tempdir()?;
    std::fs::copy(operator_binary_path(), ctx_dir.path().join("operator"))?;
    let engine = super::images::docker_engine();
    let containerfile = "deploy/operator/Containerfile";
    let ctx = ctx_dir.path().to_str().ok_or("temp dir path not UTF-8")?;
    eprintln!("  building {OPERATOR_IMAGE} with {engine}...");
    let status = Command::new(&engine)
        .args(["build", "-f", containerfile, "-t", OPERATOR_IMAGE, ctx])
        .status()?;
    if !status.success() {
        return Err(format!("{engine} build failed for {OPERATOR_IMAGE}").into());
    }
    Ok(())
}

/// Load the operator image into a Kind cluster.
///
/// When Podman is the container engine, `kind load docker-image` cannot
/// access the Podman image store directly.  This function detects Podman
/// and falls back to saving the image as a tar archive, then loading it
/// with `kind load image-archive`.
///
/// # Errors
///
/// Returns an error if the image save or `kind load` fails.
pub(crate) fn load_operator_image(cluster_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let engine = super::images::docker_engine();
    if engine == "podman" {
        load_operator_image_podman(cluster_name)
    } else {
        let status = Command::new("kind")
            .args(["load", "docker-image", OPERATOR_IMAGE, "--name", cluster_name])
            .status()?;
        if !status.success() {
            return Err(format!("kind load docker-image {OPERATOR_IMAGE} failed for {cluster_name}").into());
        }
        Ok(())
    }
}

/// Podman-specific image load: re-tag, save as archive, `kind load`.
fn load_operator_image_podman(cluster_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let qualified = format!("docker.io/library/{OPERATOR_IMAGE}");
    let tag_status = Command::new("podman")
        .args(["tag", OPERATOR_IMAGE, &qualified])
        .status()?;
    if !tag_status.success() {
        return Err(format!("podman tag {OPERATOR_IMAGE} → {qualified} failed").into());
    }
    let archive = tempfile::NamedTempFile::new()?;
    let archive_path = archive.path().to_str().ok_or("temp path not UTF-8")?;
    let save_status = Command::new("podman")
        .args(["save", "-o", archive_path, &qualified])
        .status()?;
    if !save_status.success() {
        return Err(format!("podman save {qualified} failed").into());
    }
    let load_status = Command::new("kind")
        .args(["load", "image-archive", archive_path, "--name", cluster_name])
        .status()?;
    if !load_status.success() {
        return Err(format!("kind load image-archive failed for {cluster_name}").into());
    }
    Ok(())
}

/// Patch the `grid-operator` Deployment env vars for in-cluster testing.
///
/// # Errors
///
/// Returns an error if `kubectl set env` fails.
pub(crate) fn patch_operator_deployment_env(context: &str, site_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("kubectl")
        .args([
            "--context",
            context,
            "set",
            "env",
            "deployment/grid-operator",
            "-n",
            "grid-system",
            &format!("GRID_SWIM_SITE_NAME={site_name}"),
            "GRID_SWIM_BIND_ADDR=0.0.0.0:7946",
            "GRID_SWIM_SEEDS=",
            "GRID_GATEWAY_ADDRESS=",
            "RUST_LOG=info",
        ])
        .status()?;
    if !status.success() {
        return Err("kubectl set env for grid-operator deployment failed".into());
    }
    Ok(())
}

/// Run `kubectl auth can-i` as the `grid-operator` `ServiceAccount`.
///
/// Returns `true` if the action is allowed, `false` if denied.
pub(crate) fn kubectl_auth_can_i(
    context: &str,
    verb: &str,
    resource: &str,
    namespace: Option<&str>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let mut args = vec![
        "auth",
        "can-i",
        verb,
        resource,
        "--context",
        context,
        "--as=system:serviceaccount:grid-system:grid-operator",
    ];
    if let Some(ns) = namespace {
        args.push("-n");
        args.push(ns);
    }
    let output = Command::new("kubectl").args(&args).output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.trim() == "yes")
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
    kubectl::apply_manifest(context, &network)?;
    kubectl::apply_manifest(context, &healthy)?;
    kubectl::apply_manifest(context, &invalid)?;
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
// Operator binary freshness
// ---------------------------------------------------------------------------

/// Path to the pre-compiled operator binary used by direct-spawn helpers.
///
/// This is the path that `spawn_operator_with_swim_for_context` executes.
/// It differs from the `cargo run` path used by `spawn_operator` and
/// `spawn_operator_with_swim`: those paths let cargo decide whether to
/// recompile, while this path bypasses cargo entirely.
#[must_use]
pub(crate) fn operator_binary_path() -> std::path::PathBuf {
    std::path::PathBuf::from("target/debug/operator")
}

/// Cargo arguments to build the operator binary in debug mode.
#[must_use]
pub(crate) fn operator_build_args() -> &'static [&'static str] {
    &["build", "-p", "operator", "--bin", "operator"]
}

/// Ensure the operator binary is built and up-to-date before spawning it directly.
///
/// Runs `cargo build -p operator --bin operator` synchronously.  The build is
/// incremental so this is fast when sources are already current; it rebuilds
/// only when source or dependency changes are detected.
///
/// Call this before any code path that executes [`operator_binary_path()`]
/// directly rather than going through `cargo run`.  This prevents false
/// validation failures caused by a stale binary that reflects an earlier
/// commit or in-progress working-tree state.
///
/// # Errors
///
/// Returns an error if `cargo build` fails (compilation error or process
/// spawn failure).
pub(crate) fn ensure_operator_binary_built() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("  building operator binary for spawned SWIM validation...");
    let status = Command::new("cargo").args(operator_build_args()).status()?;
    if !status.success() {
        return Err("cargo build -p operator --bin operator failed".into());
    }
    Ok(())
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

/// Spawn a SWIM-enabled operator against a specific kind cluster without relying on
/// the global kubeconfig current-context.
///
/// Exports a minimal kubeconfig for `context` to a temp file and passes it via
/// `KUBECONFIG` so the operator binary uses the correct cluster regardless of
/// what `kubectl config use-context` has set globally.  This avoids the race
/// between consecutive [`spawn_operator_with_swim`] calls where the second
/// `use-context` fires before the first operator binary has read its config.
///
/// `bind_addr` and `advertise_addr` are the SWIM UDP addresses.
/// `site_name` is the stable SWIM site identity.
/// `seeds` is a comma-separated list of seed addresses (empty = no seeds).
#[expect(
    clippy::too_many_lines,
    reason = "kubeconfig export + process spawn + sleep: splitting obscures the startup contract"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "each argument maps to a distinct operator env var; a wrapper struct would obscure the address model"
)]
pub(crate) fn spawn_operator_with_swim_for_context(
    context: &str,
    bind_addr: &str,
    advertise_addr: &str,
    site_name: &str,
    seeds: &str,
    gateway_address: Option<&str>,
) -> Result<Child, Box<dyn std::error::Error>> {
    // Ensure the operator binary is current before executing it directly.
    // This call is incremental (cargo skips recompilation if sources are
    // unchanged) so it is fast in the common case and prevents false failures
    // from a stale binary that reflects an earlier commit.
    ensure_operator_binary_built()?;

    // Export a minimal kubeconfig for this specific context to a temp file.
    // `kubectl config view --minify --flatten --context {context}` exports
    // only the cluster, user, and context for `context`, with that context
    // set as current-context.
    let kubeconfig_path = std::path::PathBuf::from(format!("/tmp/grid-kubeconfig-{context}.yaml"));
    let output = Command::new("kubectl")
        .args(["config", "view", "--minify", "--flatten", "--context", context])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl config view --minify failed for context {context}: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    std::fs::write(&kubeconfig_path, &output.stdout)?;

    eprintln!(
        "  spawning SWIM operator (site={site_name}, bind={bind_addr}, seeds={seeds:?}, \
         context={context}, kubeconfig={})",
        kubeconfig_path.display()
    );
    // Redirect stdout and stderr to per-site log files rather than inheriting the
    // parent's stdio.  When the parent's output is piped (e.g. `cargo xtask ... | tee`),
    // shared stdout/stderr can back-pressure the child if the pipe buffer fills while
    // the xtask process is also writing.  Redirecting avoids that and gives a clear
    // per-site log for post-mortem if convergence fails.
    let log_path = format!("/tmp/grid-operator-{site_name}.log");
    let log_file =
        std::fs::File::create(&log_path).map_err(|e| format!("could not create operator log {log_path}: {e}"))?;
    let log_file2 = log_file.try_clone()?;
    // Run the pre-compiled binary directly to avoid cargo's startup overhead and
    // environment forwarding behaviour under `cargo xtask`.
    // ensure_operator_binary_built() above guarantees the binary exists and is current.
    let operator_bin = operator_binary_path();
    let mut cmd = Command::new(&operator_bin);
    cmd.env("KUBECONFIG", &kubeconfig_path)
        .env("GRID_SWIM_BIND_ADDR", bind_addr)
        .env("GRID_SWIM_ADVERTISE_ADDR", advertise_addr)
        .env("GRID_SWIM_SITE_NAME", site_name)
        .env("GRID_SWIM_SEEDS", seeds)
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file2));
    if let Some(gw) = gateway_address.filter(|s| !s.is_empty()) {
        cmd.env("GRID_GATEWAY_ADDRESS", gw);
    }
    let child = cmd.spawn()?;
    #[expect(
        clippy::disallowed_methods,
        reason = "deliberate fixed wait for operator startup before the caller continues"
    )]
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

/// Default data-plane gateway port used in site-join SWIM tests.
pub(crate) const SITE_JOIN_GATEWAY_PORT: u16 = 19080;

/// Spawn the Grid operator with SWIM membership enabled.
///
/// Equivalent to [`spawn_operator`] but also sets:
/// - `GRID_SWIM_BIND_ADDR` — UDP address for the SWIM listener
/// - `GRID_SWIM_ADVERTISE_ADDR` — address peers use to reach this node
/// - `GRID_SWIM_SITE_NAME` — stable site identity (must match `GridSite.metadata.name`)
/// - `GRID_SWIM_SEEDS` — comma-separated seed peer addresses (empty = no seeds)
/// - `GRID_GATEWAY_ADDRESS` — optional data-plane gateway address (omitted if `None` or empty)
#[expect(
    clippy::too_many_arguments,
    reason = "each argument maps to a distinct operator env var; a wrapper struct would obscure the address model"
)]
#[expect(
    clippy::disallowed_methods,
    reason = "spawn + settle sleep in xtask; no async runtime available"
)]
pub(crate) fn spawn_operator_with_swim(
    context: &str,
    bind_addr: &str,
    advertise_addr: &str,
    site_name: &str,
    seeds: &str,
    gateway_address: Option<&str>,
) -> Result<Child, Box<dyn std::error::Error>> {
    eprintln!("  setting kubectl context to {context}...");
    Command::new("kubectl")
        .args(["config", "use-context", context])
        .status()?;

    eprintln!("  spawning SWIM operator (site={site_name}, bind={bind_addr}, seeds={seeds:?})...");
    let mut cmd = Command::new("cargo");
    cmd.args(["run", "--quiet", "-p", "operator", "--bin", "operator"])
        .env("GRID_SWIM_BIND_ADDR", bind_addr)
        .env("GRID_SWIM_ADVERTISE_ADDR", advertise_addr)
        .env("GRID_SWIM_SITE_NAME", site_name)
        .env("GRID_SWIM_SEEDS", seeds)
        .stdin(Stdio::null());
    if let Some(gw) = gateway_address.filter(|s| !s.is_empty()) {
        cmd.env("GRID_GATEWAY_ADDRESS", gw);
    }
    let child = cmd.spawn()?;
    // Brief pause so the operator establishes watches and starts its SWIM listener.
    std::thread::sleep(Duration::from_secs(3));
    Ok(child)
}

/// Generate a random 64-character hex string suitable as a `GRID_SWIM_ENCRYPT_KEY`.
///
/// Each call returns a unique 32-byte AES-256-GCM key encoded as lowercase hex.
/// The returned string is safe to log (it is a key IDENTIFIER for test output,
/// but the caller must not log the value in production code).
///
/// # Panics
///
/// Panics if the OS random source is unavailable (catastrophic environment failure).
#[must_use]
pub(crate) fn generate_swim_key_hex() -> String {
    use rand::RngCore as _;
    let mut key = [0_u8; 32];
    rand::rng().fill_bytes(&mut key);
    key.iter().map(|b| format!("{b:02x}")).collect()
}

/// Create paired stdout/stderr log files for an out-of-cluster operator process.
fn operator_log_files(site_name: &str) -> Result<(std::fs::File, std::fs::File), Box<dyn std::error::Error>> {
    let log_path = format!("/tmp/grid-operator-{site_name}.log");
    let log_file =
        std::fs::File::create(&log_path).map_err(|e| format!("could not create operator log {log_path}: {e}"))?;
    let log_file2 = log_file.try_clone()?;
    Ok((log_file, log_file2))
}

/// Spawn the Grid operator with SWIM membership and optional AES-256-GCM encryption.
///
/// Like [`spawn_operator_with_swim`] but also sets `GRID_SWIM_ENCRYPT_KEY` when
/// `swim_key_hex` is `Some`.  The key must be a 64-character lowercase hex string
/// (32 bytes).  Peers must use the same key; mismatched-key peers are silently
/// dropped.
///
/// # Panics
///
/// Does not panic; propagates all errors via `Result`.
///
/// # Security invariant
///
/// The key value is passed to the child process via the environment — it is
/// visible to other processes on the same host (standard Unix env var rules).
/// In Kind-based tests this is acceptable; in production use
/// `GridNetwork.spec.tls.swimKeyRef` instead.
#[expect(
    clippy::disallowed_methods,
    reason = "spawns a child operator process; sleep is deliberate startup pause"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "each argument maps to one operator env var; grouping would obscure the contract"
)]
pub(crate) fn spawn_operator_with_swim_keyed(
    context: &str,
    bind_addr: &str,
    advertise_addr: &str,
    site_name: &str,
    seeds: &str,
    gateway_address: Option<&str>,
    swim_key_hex: Option<&str>,
) -> Result<Child, Box<dyn std::error::Error>> {
    eprintln!("  setting kubectl context to {context}...");
    Command::new("kubectl")
        .args(["config", "use-context", context])
        .status()?;

    let encrypted = swim_key_hex.is_some();
    eprintln!(
        "  spawning SWIM operator (site={site_name}, bind={bind_addr}, seeds={seeds:?}, encrypted={encrypted})..."
    );
    let (log_file, log_file2) = operator_log_files(site_name)?;
    let operator_bin = operator_binary_path();
    let mut cmd = Command::new(&operator_bin);
    cmd.env("GRID_SWIM_BIND_ADDR", bind_addr)
        .env("GRID_SWIM_ADVERTISE_ADDR", advertise_addr)
        .env("GRID_SWIM_SITE_NAME", site_name)
        .env("GRID_SWIM_SEEDS", seeds)
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file2));
    if let Some(gw) = gateway_address.filter(|s| !s.is_empty()) {
        cmd.env("GRID_GATEWAY_ADDRESS", gw);
    }
    if let Some(key) = swim_key_hex {
        cmd.env("GRID_SWIM_ENCRYPT_KEY", key);
    }
    let child = cmd.spawn()?;
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
    kubectl::apply_manifest(context, &manifest)?;
    eprintln!("  [OK] SWIM test GridNetwork {SWIM_TEST_NETWORK} applied");
    Ok(())
}

/// Apply the `GridNetwork` fixture for the CRD-seed SWIM validation.
///
/// Unlike [`apply_swim_test_network`], this fixture sets `spec.seeds` to the
/// supplied list of socket addresses so the operator reconcile loop can announce
/// those peers via `SwimHandle::announce_seeds` without any `GRID_SWIM_SEEDS`
/// env var.
///
/// # Why this proves CRD-driven seeds
///
/// Both operators are started with `GRID_SWIM_SEEDS=""`.  The secondary operator
/// has no peer at startup.  After this fixture is applied, the secondary's
/// `GridNetwork` reconcile calls `parse_crd_seeds(spec.seeds, local_addr)`
/// which filters out the primary address only if it matches the secondary's
/// own `local_addr`; since it does not, the secondary announces to the primary
/// via the SWIM runtime channel.  SWIM gossip then converges and the `GridNetwork`
/// reaches `Active`.
///
/// # Errors
///
/// Returns an error if the manifest cannot be serialised or `kubectl apply` fails.
pub(crate) fn apply_swim_test_network_with_seeds(
    context: &str,
    seeds: &[SocketAddr],
) -> Result<(), Box<dyn std::error::Error>> {
    let seeds_json: Vec<String> = seeds.iter().map(SocketAddr::to_string).collect();
    let manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": CRD_SEEDS_TEST_NETWORK },
        "spec": { "seeds": seeds_json }
    }))
    .unwrap_or_else(|e| {
        eprintln!("CRD-seeds test network serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &manifest)?;
    eprintln!("  [OK] CRD-seed GridNetwork {CRD_SEEDS_TEST_NETWORK} applied (seeds={seeds:?})");
    Ok(())
}

/// Delete resources created by the CRD-seed SWIM validation.
///
/// Safe to call before a fresh run — all deletes use `--ignore-not-found`.
pub(crate) fn cleanup_swim_crd_seeds_test_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_cluster_resource(context, "gridnetwork", CRD_SEEDS_TEST_NETWORK)?;
    cleanup_auto_discovered_gridsites_for_network(context, CRD_SEEDS_TEST_NETWORK);
    eprintln!("  [OK] stale CRD-seed SWIM test resources removed");
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
    kubectl::apply_manifest(context, &manifest)?;
    eprintln!("  [OK] SWIM test InferenceProvider {SWIM_TEST_PROVIDER} applied (model={SWIM_TEST_PROVIDER_MODEL})");
    Ok(())
}

/// Delete resources created by the SWIM validation.
///
/// Safe to call before a fresh run — all deletes use `--ignore-not-found`.
pub(crate) fn cleanup_swim_test_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_cluster_resource(context, "inferenceprovider", SWIM_TEST_PROVIDER)?;
    delete_cluster_resource(context, "gridnetwork", SWIM_TEST_NETWORK)?;
    // Remove auto-discovered GridSites created by reconcile_discovered_sites during SWIM tests.
    cleanup_auto_discovered_gridsites_for_network(context, SWIM_TEST_NETWORK);
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

/// Assert that a rejected SWIM peer stays isolated for the whole observation window.
///
/// This is intentionally stronger than reading `connectedSites` once.  The helper:
///
/// - bumps the `GridNetwork` on every loop so the controller keeps reconciling;
/// - waits until `status.observedGeneration` exists at least once;
/// - fails immediately if `connectedSites` or `distributedProviderCount` becomes non-zero;
/// - fails if the overlay ever contains a candidate from `rejected_site`.
///
/// # Errors
///
/// Returns an error if status never appears before `window` elapses, or if the
/// rejected peer is observed through membership, CRDT state, or overlay output.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous poll loop in xtask harness; no async runtime available"
)]
#[expect(
    clippy::too_many_lines,
    reason = "single observation loop keeps the negative proof easy to audit"
)]
pub(crate) fn assert_swim_peer_stays_isolated(
    context: &str,
    network: &str,
    gw: &str,
    rejected_site: &str,
    window: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let resource = format!("gridnetworks/{network}");
    let mut observed_status = false;

    loop {
        drop(bump_gridnetwork(context, network));

        let observed_gen_str = kubectl_jsonpath(context, &resource, "{.status.observedGeneration}").unwrap_or_default();
        let observed_gen: i64 = observed_gen_str.parse().unwrap_or(0);
        if observed_gen > 0 {
            observed_status = true;
        }

        let connected_str = kubectl_jsonpath(context, &resource, "{.status.connectedSites}").unwrap_or_default();
        let connected_sites: u32 = connected_str.parse().unwrap_or(0);
        if connected_sites != 0 {
            return Err(format!(
                "SWIM encryption negative proof failed: rejected site {rejected_site:?} changed \
                 connectedSites to {connected_sites}"
            )
            .into());
        }

        let distributed_str =
            kubectl_jsonpath(context, &resource, "{.status.distributedProviderCount}").unwrap_or_default();
        let distributed_provider_count: u32 = distributed_str.parse().unwrap_or(0);
        if distributed_provider_count != 0 {
            return Err(format!(
                "SWIM encryption negative proof failed: rejected site {rejected_site:?} changed \
                 distributedProviderCount to {distributed_provider_count}"
            )
            .into());
        }

        if read_overlay_configmap(context, network, gw, "default").is_ok() {
            assert_no_crdt_candidates_for_site(context, network, gw, rejected_site)?;
        }

        if start.elapsed() >= window {
            break;
        }
        std::thread::sleep(POLL_INTERVAL);
    }

    if !observed_status {
        return Err(format!(
            "SWIM encryption negative proof failed: GridNetwork {network} never wrote status during isolation window"
        )
        .into());
    }

    eprintln!(
        "  [PASS] rejected peer {rejected_site:?} stayed isolated for {window:?}: \
         connectedSites=0, distributedProviderCount=0, no overlay candidate"
    );
    Ok(())
}

/// Assert that a `GridNetwork` reconcile is blocked: no status written, no
/// SWIM side effects, no overlay generated.
///
/// Unlike [`assert_swim_peer_stays_isolated`], this helper does NOT require
/// `status.observedGeneration` to appear.  It is designed for scenarios where
/// the reconcile itself fails before reaching `update_status()` — e.g., when
/// `swimKeyRef` is configured but the referenced Secret does not exist.
///
/// The proof is: for the entire `window`, `connectedSites` and
/// `distributedProviderCount` stay at zero (or absent), and no overlay
/// `ConfigMap` is generated.  The absence of `observedGeneration` is expected
/// and is not treated as an error.
///
/// # Errors
///
/// Returns an error if any SWIM side effect is observed during the window.
#[expect(
    clippy::too_many_lines,
    reason = "single observation loop with three invariant checks; splitting would obscure the proof"
)]
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous poll loop in xtask harness; no async runtime available"
)]
pub(crate) fn assert_reconcile_blocked_no_side_effects(
    context: &str,
    network: &str,
    gw: &str,
    window: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let resource = format!("gridnetworks/{network}");

    loop {
        let connected_str = kubectl_jsonpath(context, &resource, "{.status.connectedSites}").unwrap_or_default();
        let connected_sites: u32 = connected_str.parse().unwrap_or(0);
        if connected_sites != 0 {
            return Err(format!(
                "reconcile-blocked proof failed: connectedSites unexpectedly became {connected_sites}"
            )
            .into());
        }

        let distributed_str =
            kubectl_jsonpath(context, &resource, "{.status.distributedProviderCount}").unwrap_or_default();
        let distributed_count: u32 = distributed_str.parse().unwrap_or(0);
        if distributed_count != 0 {
            return Err(format!(
                "reconcile-blocked proof failed: distributedProviderCount unexpectedly became {distributed_count}"
            )
            .into());
        }

        if read_overlay_configmap(context, network, gw, "default").is_ok() {
            return Err(format!(
                "reconcile-blocked proof failed: overlay ConfigMap for {network}/{gw} was unexpectedly generated"
            )
            .into());
        }

        if start.elapsed() >= window {
            break;
        }
        std::thread::sleep(POLL_INTERVAL);
    }

    eprintln!(
        "  [PASS] reconcile blocked for {window:?}: no status written, no overlay generated, \
         connectedSites=0, distributedProviderCount=0"
    );
    Ok(())
}

/// Wait for an out-of-cluster operator log to contain all expected strings.
///
/// `spawn_operator_with_swim_for_context` redirects stdout/stderr to
/// `/tmp/grid-operator-<site_name>.log`. This helper gives E2E tests a
/// non-Kubernetes signal that a reconcile reached a specific fail-closed path
/// without printing the log body back to the terminal.
///
/// # Errors
///
/// Returns an error if the log file does not contain every expected string
/// before the timeout expires.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous poll loop in xtask harness; no async runtime available"
)]
pub(crate) fn wait_for_operator_log_contains(
    site_name: &str,
    expected: &[&str],
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let log_path = format!("/tmp/grid-operator-{site_name}.log");
    let start = Instant::now();

    loop {
        if let Ok(contents) = std::fs::read_to_string(&log_path)
            && expected.iter().all(|needle| contents.contains(needle))
        {
            eprintln!("  [PASS] operator log for {site_name:?} contains expected reconcile failure signal");
            return Ok(());
        }

        if start.elapsed() >= timeout {
            return Err(format!("operator log {log_path} did not contain all expected strings before timeout").into());
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Snapshot of a `GridNetwork` status read from the cluster.
///
/// Returned by [`wait_for_gridnetwork_status`] for inspecting status before
/// SWIM convergence — e.g. to verify isolation when no seeds are configured.
pub(crate) struct GridNetworkStatusSnapshot {
    /// Current lifecycle phase (e.g. `"Pending"`, `"Active"`).
    pub phase: String,
    /// Number of connected SWIM sites reported by the operator.
    pub connected_sites: u32,
}

/// Poll until the `GridNetwork` has been reconciled at least once
/// (`status.observedGeneration > 0`).
///
/// Unlike [`wait_for_gridnetwork_active`], this function does not require the network
/// to reach `Active`.  It returns as soon as the operator has written a status.
/// Use it to inspect isolation state when seeds are intentionally empty.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous poll loop in xtask harness; not in async context"
)]
pub(crate) fn wait_for_gridnetwork_status(
    context: &str,
    name: &str,
    timeout: Duration,
) -> Result<GridNetworkStatusSnapshot, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let resource = format!("gridnetworks/{name}");
    loop {
        let gen_str = kubectl_jsonpath(context, &resource, "{.status.observedGeneration}").unwrap_or_default();
        let observed_gen: i64 = gen_str.parse().unwrap_or(0);

        if observed_gen > 0 {
            let phase = kubectl_jsonpath(context, &resource, "{.status.phase}").unwrap_or_default();
            let sites_str = kubectl_jsonpath(context, &resource, "{.status.connectedSites}").unwrap_or_default();
            let connected_sites: u32 = sites_str.parse().unwrap_or(0);
            return Ok(GridNetworkStatusSnapshot { phase, connected_sites });
        }

        if start.elapsed() >= timeout {
            return Err(
                format!("timeout waiting for GridNetwork {name} first reconcile (observedGeneration>0)").into(),
            );
        }
        eprintln!("  waiting for GridNetwork {name} first reconcile (observedGeneration={observed_gen})...");
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

/// Poll the `GridNetwork` status until `distributedProviderCount > 0`.
/// Force an immediate `GridNetwork` reconcile by patching a timestamp annotation.
///
/// The operator's `GridNetwork` controller has a long requeue interval (300 s)
/// and a cross-watch that fires only when related objects change.  When the
/// first reconcile wave races the peer's CRDT broadcast by milliseconds, the
/// `distributedProviderCount` is recorded as 0.  Bumping an annotation creates
/// a watch event that triggers a fresh reconcile — by which point the CRDT
/// broadcast has already been received and merged into `state_snapshot()`.
///
/// This is an xtask validation helper; it does not affect production behavior.
pub(crate) fn bump_gridnetwork(context: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let annotation = format!("grid.praxis-proxy.io/reconcile-at={ts}");
    let status = Command::new("kubectl")
        .args([
            "--context",
            context,
            "annotate",
            "gridnetwork",
            name,
            &annotation,
            "--overwrite",
        ])
        .status()?;
    if !status.success() {
        return Err(format!("kubectl annotate gridnetwork {name} failed").into());
    }
    eprintln!("  [OK] bumped {name} annotation to force reconcile");
    Ok(())
}

/// Force an immediate `GridSite` reconcile by patching a timestamp annotation.
///
/// This is used by the site-join-discovery validation after each harness
/// `status.phase` patch.  The status patch proves Kubernetes accepted the
/// lifecycle phase; the annotation bump creates a separate watch event so the
/// controller reconciles after that patch, and the subsequent phase poll proves
/// the controller preserved the phase.
pub(crate) fn bump_gridsite(context: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let annotation = format!("grid.praxis-proxy.io/reconcile-at={ts}");
    let status = Command::new("kubectl")
        .args([
            "--context",
            context,
            "annotate",
            "gridsite",
            name,
            &annotation,
            "--overwrite",
        ])
        .status()?;
    if !status.success() {
        return Err(format!("kubectl annotate gridsite {name} failed").into());
    }
    eprintln!("  [OK] bumped GridSite {name:?} annotation to force reconcile");
    Ok(())
}

/// Each SWIM-enabled operator publishes real `InferenceProvider`-derived state
/// as a CRDT `GridStateSnapshot` on reconcile.  After SWIM gossip convergence
/// the remote operator's broadcast arrives and `distributedProviderCount`
/// becomes ≥ 1.
///
/// Returns the observed count on success or `Err` on timeout.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous poll loop in xtask; no async runtime available"
)]
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
        bump_gridnetwork(context, name)?;
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Verify that `distributedProviderCount` is exactly 1 for the SWIM state validation.
///
/// The SWIM state test applies exactly one `InferenceProvider` to exactly one
/// `GridNetwork`.  A count of 1 proves a single remote provider record arrived
/// via SWIM custom broadcast, scoped to the test network.
///
/// A count > 1 indicates cross-network state leakage: provider records from an
/// unrelated network (e.g. leftover `op-e2e-net` resources) are bleeding into
/// the SWIM test network's count.  This is a harness isolation failure and must
/// be fixed before results are trusted.
pub(crate) fn verify_distributed_state_received(
    distributed_provider_count: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    match distributed_provider_count {
        0 => Err("distributed state validation failed: distributedProviderCount must be 1 (received 0 - state not propagated)".into()),
        1 => {
            eprintln!("  [OK] distributed state received via SWIM broadcast: distributedProviderCount=1");
            Ok(())
        },
        n => Err(format!(
            "distributed state validation failed: distributedProviderCount={n} but expected exactly 1; \
             cross-network state leakage suspected - ensure op-e2e-net resources are cleaned up before \
             running verify-swim-state"
        ).into()),
    }
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
    kubectl::apply_manifest(context, &pod)?;
    kubectl::apply_manifest(context, &svc)?;
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
    kubectl::apply_manifest(context, &pod)?;
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
        kubectl::apply_manifest(context, &manifest)?;
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

// ---------------------------------------------------------------------------
// Metrics-driven routing fixture helpers
// ---------------------------------------------------------------------------

/// Apply the `GridNetwork` and two `InferenceProvider` fixtures for metrics-routing.
///
/// Both providers serve [`METRICS_ROUTING_MODEL`] so only the scraped queue-depth
/// metric distinguishes them in overlay scoring.  `routingClusterRef` is set to the
/// real provider site so the overlay candidate routes to the actual provider gateway.
///
/// `spec.endpoint` for each provider is `http://127.0.0.1:{east_port}` /
/// `http://127.0.0.1:{west_port}` — the host-side port-forwards to the Python
/// metrics pods.  The operator scrapes `{endpoint}/metrics` and uses the result in
/// scoring.  This follows the same pattern as the single-provider metrics test.
#[expect(clippy::too_many_lines, reason = "GridNetwork + 2 InferenceProvider JSON manifests")]
pub(crate) fn apply_metrics_routing_fixtures(
    context: &str,
    east_site: &str,
    west_site: &str,
    east_metrics_port: u16,
    west_metrics_port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let network = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": METRICS_ROUTING_NETWORK },
        "spec": {
            "seeds": [],
            "gatewayRefs": [{ "name": METRICS_ROUTING_GW, "namespace": "default" }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("metrics-routing network serialization failed: {e}");
        std::process::exit(1)
    });

    for (name, site, port) in [
        (METRICS_ROUTING_EAST_PROVIDER, east_site, east_metrics_port),
        (METRICS_ROUTING_WEST_PROVIDER, west_site, west_metrics_port),
    ] {
        let endpoint = format!("http://127.0.0.1:{port}");
        let manifest = serde_json::to_string_pretty(&serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": METRICS_ROUTING_NETWORK,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": endpoint,
                "models": [{ "name": METRICS_ROUTING_MODEL }],
                "routingClusterRef": site,
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
            eprintln!("metrics-routing provider serialization failed: {e}");
            std::process::exit(1)
        });
        kubectl::apply_manifest(context, &manifest)?;
    }

    kubectl::apply_manifest(context, &network)?;
    eprintln!(
        "  [OK] metrics-routing fixtures applied \
         ({METRICS_ROUTING_EAST_PROVIDER}@{east_site}/{east_metrics_port}, \
          {METRICS_ROUTING_WEST_PROVIDER}@{west_site}/{west_metrics_port})"
    );
    Ok(())
}

/// Deploy two Python metrics server Pods for the metrics-routing validation.
///
/// Each pod serves a single Prometheus gauge line at `GET /metrics`.  The pods
/// are named [`METRICS_ROUTING_EAST_POD`] and [`METRICS_ROUTING_WEST_POD`] and
/// are deployed in the given context cluster (the primary provider cluster).
///
/// The caller is responsible for port-forwarding to the host before the operator
/// scrapes them, and for deleting the pods after the validation phase completes.
pub(crate) fn apply_metrics_routing_pods(
    context: &str,
    east_queue: &str,
    west_queue: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    apply_metrics_endpoint_pod(context, METRICS_ROUTING_EAST_POD, east_queue)?;
    apply_metrics_endpoint_pod(context, METRICS_ROUTING_WEST_POD, west_queue)?;
    eprintln!(
        "  [OK] metrics-routing pods applied \
         ({METRICS_ROUTING_EAST_POD}: queue={east_queue}, \
          {METRICS_ROUTING_WEST_POD}: queue={west_queue})"
    );
    Ok(())
}

/// Delete the metrics-routing Python pods.
///
/// Best-effort: uses `--force --grace-period=0` so the next phase can redeploy
/// immediately.  The caller must restart any active port-forwards after deletion.
pub(crate) fn delete_metrics_routing_pods(context: &str) {
    for pod in [METRICS_ROUTING_EAST_POD, METRICS_ROUTING_WEST_POD] {
        let _s = Command::new("kubectl")
            .args([
                "--context",
                context,
                "-n",
                "default",
                "delete",
                "pod",
                pod,
                "--ignore-not-found",
                "--force",
                "--grace-period=0",
            ])
            .status();
    }
    eprintln!("  [OK] metrics-routing pods deleted");
}

/// Delete all resources created by the metrics-routing validation.
///
/// Idempotent: all deletes use `--ignore-not-found`.
pub(crate) fn cleanup_metrics_routing_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_namespaced_resource(
        context,
        "default",
        "configmap",
        &format!("grid-overlay-{METRICS_ROUTING_NETWORK}-{METRICS_ROUTING_GW}"),
    )?;
    delete_cluster_resource(context, "inferenceprovider", METRICS_ROUTING_EAST_PROVIDER)?;
    delete_cluster_resource(context, "inferenceprovider", METRICS_ROUTING_WEST_PROVIDER)?;
    delete_cluster_resource(context, "gridnetwork", METRICS_ROUTING_NETWORK)?;
    delete_metrics_routing_pods(context);
    eprintln!("  [OK] stale metrics-routing resources removed");
    Ok(())
}

/// Verify the metrics-routing overlay positions the expected low-queue site first.
///
/// Both east and west providers serve [`METRICS_ROUTING_MODEL`] with `backendKind =
/// "local"` (equal locality).  After metrics scraping, the provider with a lower
/// `queueDepth` signal receives a higher score and appears at a smaller index in the
/// `candidates` array.
///
/// Returns `Ok(())` when ordering is correct; returns `Err` with a diagnostic
/// message if any expected candidate is missing or the ordering is wrong.
#[expect(
    clippy::too_many_lines,
    reason = "two candidate position lookups with diagnostic messages"
)]
pub(crate) fn verify_metrics_routing_overlay(
    overlay: &serde_json::Value,
    expected_first_site: &str,
    expected_second_site: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let candidates = overlay["candidates"]
        .as_array()
        .ok_or("overlay missing candidates array")?;

    // The raw overlay JSON uses the `routingClusterRef` value as the cluster field
    // (e.g. "site-east"), not the consumer-config cluster name ("gateway-site-east").
    // The gateway-{site} prefix is added later by `candidates_yaml` when the xtask
    // generates the Praxis consumer config.
    let first_pos = candidates
        .iter()
        .position(|c| {
            c.get("site").and_then(serde_json::Value::as_str) == Some(expected_first_site)
                && c.get("name").and_then(serde_json::Value::as_str) == Some(METRICS_ROUTING_MODEL)
        })
        .ok_or_else(|| {
            format!("overlay candidate for model={METRICS_ROUTING_MODEL:?} at site={expected_first_site:?} not found")
        })?;

    let second_pos = candidates
        .iter()
        .position(|c| {
            c.get("site").and_then(serde_json::Value::as_str) == Some(expected_second_site)
                && c.get("name").and_then(serde_json::Value::as_str) == Some(METRICS_ROUTING_MODEL)
        })
        .ok_or_else(|| {
            format!("overlay candidate for model={METRICS_ROUTING_MODEL:?} at site={expected_second_site:?} not found")
        })?;

    if first_pos >= second_pos {
        return Err(format!(
            "metrics-routing overlay order unexpected: \
             {expected_first_site} (pos {first_pos}) must appear before \
             {expected_second_site} (pos {second_pos}); \
             check that the low-queue provider is scraping correctly"
        )
        .into());
    }

    eprintln!(
        "  [OK] metrics-routing overlay order: {expected_first_site} (pos {first_pos}, lower queue) \
         before {expected_second_site} (pos {second_pos}, higher queue)"
    );
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
    kubectl::apply_manifest(context, &manifest)?;
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
            "models": [{ "name": "model-z" }],
            "auth": {
                "strategy": "bearer_token",
                "secretRef": {
                    "name": API_PROVIDER_SECRET_NAME,
                    "namespace": API_PROVIDER_SECRET_NS,
                    "key": API_PROVIDER_SECRET_KEY
                }
            }
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("api provider fixture serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &manifest)?;
    eprintln!(
        "  [OK] api provider fixture applied \
         (auth.strategy=bearer_token, secretRef={API_PROVIDER_SECRET_NAME:?}/{API_PROVIDER_SECRET_KEY:?})"
    );
    Ok(())
}

/// Name of the Kubernetes `Service` that exposes the provider Praxis gateway.
///
/// This is the service probed for `NodePort` discovery when building consumer
/// endpoint topology for the operator-generated consumer `ConfigMap`.
const PROVIDER_GATEWAY_SVC: &str = "praxis-provider";

/// Apply the Grid operator validation fixtures with `consumerConfig` enabled.
///
/// Identical to [`apply_test_fixtures`] but includes
/// `GatewayRef.consumerConfig.enabled: true` with `clusterEndpoints` populated
/// from the provider cluster's `NodePort` address.  This allows the operator-
/// generated consumer `ConfigMap` to include full `load_balancer` cluster entries
/// (endpoint URL + mTLS config) instead of name-only stubs.
///
/// Two cluster entries are populated:
/// - `TEST_HEALTHY_ROUTING_CLUSTER` — the self-hosted provider gateway, reached via `NodePort` with mTLS (SNI:
///   `{cluster}.grid.internal`).
/// - `TEST_PROVIDER_API` — the API-provider mock, reached via the in-cluster service DNS name (plain HTTP; no TLS).
///
/// When `NodePort` discovery fails for the provider gateway, the function falls
/// back to a name-only stub for that cluster and logs a warning.
pub(crate) fn apply_test_fixtures_with_consumer_config(
    context: &str,
    provider_endpoint: &str,
    api_provider_svc_address: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Discover the provider gateway NodePort address for the self-hosted cluster.
    let provider_cluster_endpoint = discover_provider_cluster_endpoint(context, TEST_HEALTHY_ROUTING_CLUSTER);

    let cluster_endpoints = build_consumer_cluster_endpoints(
        provider_cluster_endpoint.as_deref(),
        TEST_HEALTHY_ROUTING_CLUSTER,
        api_provider_svc_address,
        TEST_PROVIDER_API,
    );

    let network = network_fixture_with_consumer_config_json(
        TEST_NETWORK,
        TEST_GATEWAY_NAME,
        TEST_GATEWAY_NS,
        TEST_CONSUMER_CONFIGMAP_NAME,
        &cluster_endpoints,
    );
    let healthy = provider_fixture_json(
        TEST_PROVIDER_HEALTHY,
        TEST_NETWORK,
        provider_endpoint,
        Some(TEST_HEALTHY_ROUTING_CLUSTER),
    );
    let invalid = provider_fixture_json(TEST_PROVIDER_INVALID, TEST_NETWORK, "", None);
    kubectl::apply_manifest(context, &network)?;
    kubectl::apply_manifest(context, &healthy)?;
    kubectl::apply_manifest(context, &invalid)?;
    eprintln!("  [OK] test fixtures applied (consumerConfig.enabled: true, cluster_endpoints populated)");
    Ok(())
}

/// Attempt to discover the `NodePort` endpoint for the provider gateway.
///
/// Returns `Some("ip:port")` when discovery succeeds, or `None` when the
/// service has no `NodePort` or the cluster node IP cannot be determined.
/// Logs a diagnostic on failure.
fn discover_provider_cluster_endpoint(context: &str, cluster_name: &str) -> Option<String> {
    let port = kind::service_node_port(context, PROVIDER_GATEWAY_SVC, "default")?;
    match kind::kind_node_ip(context) {
        Ok(ip) => {
            let addr = format!("{ip}:{port}");
            eprintln!("  [OK] {cluster_name} provider endpoint: {addr}");
            Some(addr)
        },
        Err(e) => {
            eprintln!("  [WARN] could not get node IP for {cluster_name}: {e}; cluster will use stub");
            None
        },
    }
}

/// Build the JSON value for `consumerConfig.clusterEndpoints`.
///
/// - For the self-hosted provider (`provider_cluster`): uses mTLS when an address is available, stub otherwise.
/// - For the API provider (`api_cluster`): always uses plain HTTP.
fn build_consumer_cluster_endpoints(
    provider_address: Option<&str>,
    provider_cluster: &str,
    api_provider_address: &str,
    api_cluster: &str,
) -> serde_json::Value {
    let mut endpoints: Vec<serde_json::Value> = Vec::new();

    if let Some(addr) = provider_address {
        endpoints.push(serde_json::json!({
            "cluster": provider_cluster,
            "address": addr,
            "sni": format!("{provider_cluster}.grid.internal")
        }));
    }

    if !api_provider_address.is_empty() {
        endpoints.push(serde_json::json!({
            "cluster": api_cluster,
            "address": api_provider_address
            // sni absent → plain HTTP
        }));
    }

    serde_json::Value::Array(endpoints)
}

/// Build a `GridNetwork` JSON fixture with `consumerConfig` enabled.
///
/// The gateway reference includes `consumerConfig.enabled: true`, the given
/// `config_map_name`, and the provided `cluster_endpoints` so the operator
/// generates a consumer Praxis `ConfigMap` with full load-balancer entries where
/// endpoint information is available.
fn network_fixture_with_consumer_config_json(
    name: &str,
    gw_name: &str,
    gw_ns: &str,
    config_map_name: &str,
    cluster_endpoints: &serde_json::Value,
) -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": name },
        "spec": {
            "seeds": [],
            "gatewayRefs": [{
                "name": gw_name,
                "namespace": gw_ns,
                "consumerConfig": {
                    "enabled": true,
                    "configMapName": config_map_name,
                    "clusterEndpoints": cluster_endpoints
                }
            }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("fixture serialization failed: {e}");
        std::process::exit(1);
    })
}

/// Wait for the operator-generated consumer Praxis `ConfigMap` to appear.
///
/// Polls until the `ConfigMap` named `config_map_name` exists in `namespace`.
/// Returns an error if the timeout expires before the `ConfigMap` appears.
#[expect(
    clippy::too_many_lines,
    reason = "poll loop with kubectl args; mirrors wait_for_overlay_configmap structure"
)]
pub(crate) fn wait_for_consumer_configmap(
    context: &str,
    config_map_name: &str,
    namespace: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        let out = Command::new("kubectl")
            .args([
                "--context",
                context,
                "get",
                "configmap",
                config_map_name,
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
            eprintln!("  [OK] operator-generated consumer ConfigMap {config_map_name} exists");
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(format!("timeout waiting for consumer ConfigMap {config_map_name}").into());
        }
        eprintln!("  waiting for consumer ConfigMap {config_map_name}...");
        #[expect(
            clippy::disallowed_methods,
            reason = "synchronous poll loop; no async runtime available at call site"
        )]
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Read the `praxis.yaml` value from the operator-generated consumer `ConfigMap`.
///
/// Returns the raw YAML string under the `praxis.yaml` key.
pub(crate) fn read_consumer_configmap_praxis_yaml(
    context: &str,
    config_map_name: &str,
    namespace: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    kubectl_jsonpath_ns(context, namespace, config_map_name, r"{.data.praxis\.yaml}")
}

/// Assert the operator-generated consumer `ConfigMap` has the expected shape.
///
/// Reads the `praxis.yaml` data from the `ConfigMap` and calls
/// [`verify_consumer_praxis_yaml`] to check structural invariants.
#[expect(
    clippy::too_many_arguments,
    reason = "distinct arg per secretRef field + context + namespace; collapsing would obscure the security boundary"
)]
pub(crate) fn verify_operator_consumer_configmap(
    context: &str,
    config_map_name: &str,
    namespace: &str,
    api_token: &str,
    secret_name: &str,
    secret_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = read_consumer_configmap_praxis_yaml(context, config_map_name, namespace)?;
    if yaml.is_empty() {
        return Err(format!("consumer ConfigMap {config_map_name} has empty praxis.yaml").into());
    }
    verify_consumer_praxis_yaml(&yaml, api_token, secret_name, secret_key)
}

/// Verify structural invariants of an operator-generated consumer Praxis YAML.
///
/// This is a pure function that inspects the YAML string directly; it does not
/// interact with Kubernetes.  Use it in unit tests or after reading the
/// `ConfigMap` with [`verify_operator_consumer_configmap`].
///
/// # Invariants checked
///
/// **Must be present:** `listeners:`, `admin:`, `filter: grid_route`,
/// `filter: grid_credential_inject`, `filter: load_balancer`, `credential:`,
/// `secretRef:`, `file: …/{secret_name}/{secret_key}`, `endpoints:`.
///
/// **Must be absent:** token value, `value:`, `filter: headers`, `request_set:`.
#[expect(
    clippy::too_many_lines,
    reason = "required-element loop + forbidden-element loop with an eprintln; hard to split without obscuring the invariants"
)]
pub(crate) fn verify_consumer_praxis_yaml(
    yaml: &str,
    api_token: &str,
    secret_name: &str,
    secret_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected_file_path = format!("/run/secrets/grid-credentials/{secret_name}/{secret_key}");
    // Must be present.
    for required in &[
        "listeners:",
        "admin:",
        "filter: grid_route",
        "filter: grid_credential_inject",
        "filter: load_balancer",
        "credential:",
        "secretRef:",
        expected_file_path.as_str(),
        "endpoints:",
    ] {
        if !yaml.contains(required) {
            return Err(format!("operator-generated consumer config missing required element: {required:?}").into());
        }
    }
    // Must be absent.
    if yaml.contains(api_token) {
        return Err(
            "operator-generated consumer config contains token bytes — token must not appear in ConfigMap".into(),
        );
    }
    for forbidden in &["value:", "filter: headers", "request_set:"] {
        if yaml.contains(forbidden) {
            return Err(format!("operator-generated consumer config contains forbidden element: {forbidden:?}").into());
        }
    }
    eprintln!(
        "  [PASS] operator-generated consumer ConfigMap shape: listeners + filter_chains \
         (grid_route + grid_credential_inject (file:{expected_file_path}) + load_balancer \
         endpoints) + admin; token absent; no static header injection"
    );
    Ok(())
}

/// Read the `status.consumerConfigStatus` JSON array from a `GridNetwork`.
///
/// Returns the raw JSON string (may be empty if the field is not yet populated).
pub(crate) fn read_gridnetwork_consumer_config_status(
    context: &str,
    network_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "get",
            &format!("gridnetwork/{network_name}"),
            "-o",
            "jsonpath={.status.consumerConfigStatus}",
        ])
        .output()?;
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// Assert that the `GridNetwork` status reports `Rendered` for the expected consumer config.
///
/// Reads `status.consumerConfigStatus[]` and asserts:
/// - An entry with `gatewayName == expected_gateway` exists.
/// - Its `phase` is `"Rendered"`.
/// - Its `configMapName` matches `expected_config_map`.
/// - The status JSON does not contain the API token value (token-safety invariant).
#[expect(
    clippy::too_many_lines,
    reason = "sequential validation steps: read → token-absent check → JSON parse → entry lookup → phase check → configmap check"
)]
pub(crate) fn verify_consumer_config_status_rendered(
    context: &str,
    network_name: &str,
    expected_gateway: &str,
    expected_config_map: &str,
    api_token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let raw = read_gridnetwork_consumer_config_status(context, network_name)?;

    if raw.is_empty() {
        return Err(format!(
            "GridNetwork/{network_name} status.consumerConfigStatus is empty; \
             operator may not have reconciled yet or the field is not populated"
        )
        .into());
    }

    // Security invariant: token bytes must never appear in status.
    if raw.contains(api_token) {
        return Err(format!(
            "SECURITY VIOLATION: token bytes found in GridNetwork/{network_name} \
             status.consumerConfigStatus — status must never contain credential token bytes"
        )
        .into());
    }

    let statuses: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| format!("failed to parse consumerConfigStatus JSON from {network_name}: {e}"))?;

    let entries = statuses
        .as_array()
        .ok_or_else(|| format!("GridNetwork/{network_name} status.consumerConfigStatus is not a JSON array"))?;

    let entry = entries
        .iter()
        .find(|e| e["gatewayName"].as_str() == Some(expected_gateway))
        .ok_or_else(|| {
            format!(
                "no consumerConfigStatus entry for gateway {expected_gateway:?} in \
                 GridNetwork/{network_name}; entries: {raw}"
            )
        })?;

    let phase = entry["phase"].as_str().unwrap_or("(missing)");
    if phase != "Rendered" {
        let reason = entry["reason"].as_str().unwrap_or("");
        let message = entry["message"].as_str().unwrap_or("");
        return Err(format!(
            "consumer config status for gateway {expected_gateway:?} is {phase:?} \
             (reason={reason:?}, message={message:?}); expected Rendered"
        )
        .into());
    }

    let actual_cm = entry["configMapName"].as_str().unwrap_or("(missing)");
    if actual_cm != expected_config_map {
        return Err(
            format!("consumer config status configMapName is {actual_cm:?}; expected {expected_config_map:?}").into(),
        );
    }

    eprintln!(
        "  [PASS] GridNetwork/{network_name} status.consumerConfigStatus: \
         gateway={expected_gateway:?} phase=Rendered configMapName={expected_config_map:?}; \
         token absent from status JSON"
    );
    Ok(())
}

/// Poll until `GridNetwork.status.consumerConfigStatus[]` reports `Rendered`
/// for the expected consumer config.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous xtask poll loop; no async runtime available"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "synchronous xtask poll over one explicit status assertion; grouping args would obscure the call site"
)]
pub(crate) fn wait_for_consumer_config_status_rendered(
    context: &str,
    network_name: &str,
    expected_gateway: &str,
    expected_config_map: &str,
    api_token: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();

    loop {
        let last_err = match verify_consumer_config_status_rendered(
            context,
            network_name,
            expected_gateway,
            expected_config_map,
            api_token,
        ) {
            Ok(()) => return Ok(()),
            Err(e) => e.to_string(),
        };

        if start.elapsed() >= timeout {
            return Err(format!(
                "timeout waiting for GridNetwork/{network_name} consumerConfigStatus \
                 gateway={expected_gateway:?} phase=Rendered; last error: {last_err}"
            )
            .into());
        }

        eprintln!(
            "  waiting for GridNetwork/{network_name} consumerConfigStatus \
             gateway={expected_gateway:?} phase=Rendered ({last_err})..."
        );
        std::thread::sleep(POLL_INTERVAL);
    }
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

#[expect(
    clippy::too_many_lines,
    reason = "multi-assertion overlay check: presence, freshness, and scoring order"
)]
/// Verify the API-provider fallback candidate is present in the overlay.
///
/// Asserts that:
/// 1. A candidate for `api_model` at `api_cluster` exists.
/// 2. The candidate has `fresh = true` (the API provider is not marked Degraded).
/// 3. A candidate for the local model at `local_cluster` also exists and sorts before the API one.
///
/// This proves the overlay contains both routing paths and that the scoring
/// engine placed the API-provider candidate at lower priority than the local one.
pub(crate) fn verify_api_fallback_overlay(
    overlay: &serde_json::Value,
    local_cluster: &str,
    api_cluster: &str,
    api_model: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let candidates = overlay["candidates"]
        .as_array()
        .ok_or("overlay missing candidates array")?;

    // API-provider candidate must be present and fresh.
    let api_candidate = candidates
        .iter()
        .find(|c| c["cluster"].as_str() == Some(api_cluster) && c["name"].as_str() == Some(api_model))
        .ok_or_else(|| {
            format!("api_provider candidate for model={api_model:?} at cluster={api_cluster:?} not found in overlay")
        })?;

    if api_candidate["fresh"].as_bool() != Some(true) {
        return Err(format!("api_provider candidate for {api_model:?} has fresh=false; expected fresh=true (api_provider is not degraded)").into());
    }
    eprintln!("  [OK] api_provider candidate: model={api_model:?} cluster={api_cluster:?} fresh=true");

    // Local candidate must sort before API-provider.
    let local_pos = candidates
        .iter()
        .position(|c| c["cluster"].as_str() == Some(local_cluster))
        .ok_or_else(|| format!("{local_cluster} not found in overlay candidates"))?;
    let api_pos = candidates
        .iter()
        .position(|c| c["cluster"].as_str() == Some(api_cluster))
        .ok_or_else(|| format!("{api_cluster} not found in overlay candidates"))?;

    if api_pos <= local_pos {
        return Err(format!(
            "api fallback scoring: {api_cluster} (pos {api_pos}) must appear after \
             {local_cluster} (pos {local_pos}); local provider should have higher priority"
        )
        .into());
    }
    eprintln!(
        "  [OK] overlay scoring order: local={local_cluster} (pos {local_pos}) before api={api_cluster} (pos {api_pos})"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Full-grid routing fixture helpers
// ---------------------------------------------------------------------------

/// Apply the full-grid `GridNetwork` and all four `InferenceProvider` fixtures.
///
/// Creates four providers in one `GridNetwork` [`FULL_GRID_NETWORK`]:
/// - Local/self-hosted east: `backendKind = "local"`, `routingClusterRef = east_site`
/// - Remote/self-hosted west: `backendKind = "remote"`, `routingClusterRef = west_site`
/// - Cloud-managed: `backendKind = "cloud_managed"`, no `routingClusterRef`
/// - API-provider: `backendKind = "api_provider"`, no `routingClusterRef`
///
/// Without a `routingClusterRef`, the cloud and api candidates use the provider
/// name (`op-e2e-fg-cloud`, `op-e2e-fg-api`) as both `site` and `cluster` in
/// the Phase 1 fallback.
///
/// The cloud/API fixtures use local OpenAI-compatible mocks. They prove that
/// Grid can model, score, and route the `cloud_managed` and `api_provider`
/// backend categories in one overlay; they do not prove real Bedrock, Vertex,
/// `OpenAI`, or Anthropic provider authentication.
#[expect(clippy::too_many_arguments, reason = "one endpoint argument per backend kind")]
#[expect(
    clippy::too_many_lines,
    reason = "four JSON manifest builds (GridNetwork + 4 InferenceProviders) plus apply calls"
)]
pub(crate) fn apply_full_grid_fixtures(
    context: &str,
    east_site: &str,
    west_site: &str,
    east_endpoint: &str,
    west_endpoint: &str,
    cloud_endpoint: &str,
    api_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let network = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": FULL_GRID_NETWORK },
        "spec": {
            "seeds": [],
            "gatewayRefs": [{ "name": FULL_GRID_GW, "namespace": "default" }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("full-grid network fixture serialization failed: {e}");
        std::process::exit(1)
    });

    let east = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": FULL_GRID_PROVIDER_EAST },
        "spec": {
            "gridNetworkRef": FULL_GRID_NETWORK,
            "providerKind": "open_ai",
            "backendKind": "local",
            "endpoint": east_endpoint,
            "models": [{ "name": FULL_GRID_MODEL_EAST }],
            "routingClusterRef": east_site
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("full-grid east fixture serialization failed: {e}");
        std::process::exit(1)
    });

    let west = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": FULL_GRID_PROVIDER_WEST },
        "spec": {
            "gridNetworkRef": FULL_GRID_NETWORK,
            "providerKind": "open_ai",
            "backendKind": "remote",
            "endpoint": west_endpoint,
            "models": [{ "name": FULL_GRID_MODEL_WEST }],
            "routingClusterRef": west_site
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("full-grid west fixture serialization failed: {e}");
        std::process::exit(1)
    });

    let cloud = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": FULL_GRID_PROVIDER_CLOUD },
        "spec": {
            "gridNetworkRef": FULL_GRID_NETWORK,
            "providerKind": "open_ai",
            "backendKind": "cloud_managed",
            "endpoint": cloud_endpoint,
            "models": [{ "name": FULL_GRID_MODEL_CLOUD }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("full-grid cloud fixture serialization failed: {e}");
        std::process::exit(1)
    });

    let api = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": FULL_GRID_PROVIDER_API },
        "spec": {
            "gridNetworkRef": FULL_GRID_NETWORK,
            "providerKind": "anthropic",
            "backendKind": "api_provider",
            "endpoint": api_endpoint,
            "models": [{ "name": FULL_GRID_MODEL_API }],
            "auth": {
                "strategy": "bearer_token",
                "secretRef": {
                    "name": API_PROVIDER_SECRET_NAME,
                    "namespace": API_PROVIDER_SECRET_NS,
                    "key": API_PROVIDER_SECRET_KEY
                }
            }
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("full-grid api fixture serialization failed: {e}");
        std::process::exit(1)
    });

    kubectl::apply_manifest(context, &network)?;
    kubectl::apply_manifest(context, &east)?;
    kubectl::apply_manifest(context, &west)?;
    kubectl::apply_manifest(context, &cloud)?;
    kubectl::apply_manifest(context, &api)?;
    eprintln!(
        "  [OK] full-grid fixtures applied (east={east_site}/{FULL_GRID_MODEL_EAST}, \
         west={west_site}/{FULL_GRID_MODEL_WEST}, cloud/{FULL_GRID_MODEL_CLOUD}, \
         api/{FULL_GRID_MODEL_API})"
    );
    Ok(())
}

/// Delete all resources created by the full-grid routing validation.
///
/// Safe to call before a fresh run — all deletes use `--ignore-not-found`.
pub(crate) fn cleanup_full_grid_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_namespaced_resource(
        context,
        "default",
        "configmap",
        &format!("grid-overlay-{FULL_GRID_NETWORK}-{FULL_GRID_GW}"),
    )?;
    delete_cluster_resource(context, "inferenceprovider", FULL_GRID_PROVIDER_EAST)?;
    delete_cluster_resource(context, "inferenceprovider", FULL_GRID_PROVIDER_WEST)?;
    delete_cluster_resource(context, "inferenceprovider", FULL_GRID_PROVIDER_CLOUD)?;
    delete_cluster_resource(context, "inferenceprovider", FULL_GRID_PROVIDER_API)?;
    delete_cluster_resource(context, "gridnetwork", FULL_GRID_NETWORK)?;
    eprintln!("  [OK] stale full-grid resources removed");
    Ok(())
}

/// Verify the full-grid overlay contains all four backend-kind candidates.
///
/// Asserts:
/// - `model-east` candidate at `east_site` (local backend, `fresh=true`)
/// - `model-west` candidate at `west_site` (remote backend, `fresh=true`)
/// - `model-cloud` candidate at cloud cluster identity (`fresh=true`)
/// - `model-api` candidate at api cluster identity (`fresh=true`)
/// - Scoring order respects locality: `local` > `remote` > `cloud_managed` > `api_provider`
#[expect(
    clippy::too_many_lines,
    reason = "four candidate assertions plus scoring order check"
)]
pub(crate) fn verify_full_grid_overlay(
    overlay: &serde_json::Value,
    east_site: &str,
    west_site: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let candidates = overlay["candidates"]
        .as_array()
        .ok_or("overlay missing candidates array")?;

    for (model, site_or_cluster, label) in [
        (FULL_GRID_MODEL_EAST, east_site, "local east"),
        (FULL_GRID_MODEL_WEST, west_site, "remote west"),
        (FULL_GRID_MODEL_CLOUD, FULL_GRID_PROVIDER_CLOUD, "cloud_managed"),
        (FULL_GRID_MODEL_API, FULL_GRID_PROVIDER_API, "api_provider"),
    ] {
        let found = candidates
            .iter()
            .find(|c| c["name"].as_str() == Some(model) && c["site"].as_str() == Some(site_or_cluster));
        match found {
            Some(c) if c["fresh"].as_bool() == Some(true) => {
                eprintln!(
                    "  [OK] full-grid overlay: {label} candidate model={model:?} \
                     site={site_or_cluster:?} fresh=true"
                );
            },
            Some(_) => {
                return Err(format!(
                    "full-grid overlay: {label} candidate model={model:?} at site={site_or_cluster:?} \
                     has fresh=false (expected fresh=true)"
                )
                .into());
            },
            None => {
                return Err(format!(
                    "full-grid overlay: {label} candidate model={model:?} at site={site_or_cluster:?} \
                     not found in overlay"
                )
                .into());
            },
        }
    }

    // Verify scoring order: local (east) ≻ remote (west) ≻ cloud ≻ api_provider.
    // Each model is unique, so positions are determined by scoring, not routing ties.
    let pos = |model: &str, site: &str| -> Option<usize> {
        candidates
            .iter()
            .position(|c| c["name"].as_str() == Some(model) && c["site"].as_str() == Some(site))
    };

    let east_pos = pos(FULL_GRID_MODEL_EAST, east_site).unwrap_or(usize::MAX);
    let west_pos = pos(FULL_GRID_MODEL_WEST, west_site).unwrap_or(usize::MAX);
    let cloud_pos = pos(FULL_GRID_MODEL_CLOUD, FULL_GRID_PROVIDER_CLOUD).unwrap_or(usize::MAX);
    let api_pos = pos(FULL_GRID_MODEL_API, FULL_GRID_PROVIDER_API).unwrap_or(usize::MAX);

    if east_pos < west_pos && west_pos < cloud_pos && cloud_pos < api_pos {
        eprintln!(
            "  [OK] full-grid overlay scoring order: local(pos {east_pos}) < \
             remote(pos {west_pos}) < cloud(pos {cloud_pos}) < api(pos {api_pos})"
        );
    } else {
        return Err(format!(
            "full-grid overlay scoring order unexpected: \
             east={east_pos} west={west_pos} cloud={cloud_pos} api={api_pos}; \
             expected local < remote < cloud_managed < api_provider"
        )
        .into());
    }

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

// ---------------------------------------------------------------------------
// SWIM overlay fixture helpers
// ---------------------------------------------------------------------------

/// Apply the `GridNetwork` with a gateway ref and `InferenceProvider` fixtures
/// for the SWIM overlay validation.
///
/// Creates:
/// - `GridNetwork` [`SWIM_OVERLAY_NETWORK`] with one `gatewayRef` named [`SWIM_OVERLAY_GW`] in the `default` namespace,
///   with `localSiteName` set to `primary_site_name`.
/// - `InferenceProvider` [`SWIM_OVERLAY_PROVIDER`] belonging to [`SWIM_OVERLAY_NETWORK`] serving
///   [`SWIM_OVERLAY_MODEL`].
#[expect(
    clippy::too_many_lines,
    reason = "two JSON manifest builds (GridNetwork + InferenceProvider) plus apply calls; splitting would obscure the fixture pair"
)]
pub(crate) fn apply_swim_overlay_test_fixtures(
    context: &str,
    primary_site_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let network = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": SWIM_OVERLAY_NETWORK },
        "spec": {
            "seeds": [],
            "gatewayRefs": [{
                "name": SWIM_OVERLAY_GW,
                "namespace": "default",
                "localSiteName": primary_site_name
            }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM overlay network fixture serialization failed: {e}");
        std::process::exit(1);
    });
    let provider = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": SWIM_OVERLAY_PROVIDER },
        "spec": {
            "gridNetworkRef": SWIM_OVERLAY_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [{ "name": SWIM_OVERLAY_MODEL }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM overlay provider fixture serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &network)?;
    kubectl::apply_manifest(context, &provider)?;
    eprintln!(
        "  [OK] SWIM overlay fixtures applied (network={SWIM_OVERLAY_NETWORK}, \
         provider={SWIM_OVERLAY_PROVIDER}, model={SWIM_OVERLAY_MODEL})"
    );
    Ok(())
}

/// Minimal structurally-valid certificate PEM used in eligibility E2E tests.
///
/// This is not a real certificate — it exists only to satisfy the `GridSite`
/// controller's fingerprint trust policy check so test sites can reach `Active`
/// without full TLS infrastructure.  The matching fingerprint is computed by
/// [`sha256_fingerprint`] and configured in `spec.trust.certFingerprint`.
pub(crate) const TEST_TRUST_CERT_PEM: &str =
    "-----BEGIN CERTIFICATE-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA\n-----END CERTIFICATE-----\n";

/// Apply a `GridSite` configured for routing eligibility testing.
///
/// Configures `spec.trust.certFingerprint` (fingerprint of [`TEST_TRUST_CERT_PEM`])
/// and injects `TEST_TRUST_CERT_PEM` into `status.publicCertPem` so the `GridSite`
/// controller can verify the fingerprint and promote the site to `Active`
/// automatically when the TCP probe at `egress_addr` succeeds.
///
/// The caller must ensure a TCP listener is bound at `egress_addr` so the probe
/// passes.  After calling this function, wait for `Active` (or `TrustPolicyVerified`)
/// rather than asserting immediately.
#[expect(
    clippy::too_many_lines,
    reason = "two sequential kubectl calls (spec apply + status merge patch) with distinct error paths; splitting adds indirection without clarity"
)]
pub(crate) fn apply_active_gridsite_for_eligibility(
    context: &str,
    site_k8s_name: &str,
    network_ref: &str,
    egress_addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let test_fp = sha256_fingerprint(TEST_TRUST_CERT_PEM);
    // 1. Configure spec: egress + trust policy. Strategy: try a merge patch first (non-destructive — never removes
    //    existing labels or other spec fields).  If the resource does not yet exist, fall back to a plain kubectl apply
    //    to create it.  A plain apply on a NEW resource is safe because there are no prior labels to lose; on an
    //    EXISTING resource it would do a three-way merge that strips labels from the previous
    //    last-applied-configuration, so we always prefer the patch path when the resource already exists. Server-side
    //    apply without --force-conflicts would be another option, but SSA also removes labels that were set by an
    //    earlier client-side apply because those labels end up with no managedFields owner.
    let spec_patch = serde_json::json!({
        "spec": {
            "gridNetworkRef": network_ref,
            "egress": { "address": egress_addr, "tls": { "mode": "Mutual" } },
            "trust": { "certFingerprint": test_fp }
        }
    });
    let patch_out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "patch",
            "gridsites",
            site_k8s_name,
            "--type=merge",
            "-p",
            &spec_patch.to_string(),
        ])
        .output()?;
    if !patch_out.status.success() {
        // Resource doesn't exist yet — create it with a minimal apply.
        let create_spec = serde_json::to_string_pretty(&serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": site_k8s_name },
            "spec": {
                "gridNetworkRef": network_ref,
                "egress": { "address": egress_addr, "tls": { "mode": "Mutual" } },
                "trust": { "certFingerprint": test_fp }
            }
        }))
        .unwrap_or_else(|e| {
            eprintln!("GridSite {site_k8s_name} spec serialization failed: {e}");
            std::process::exit(1);
        });
        kubectl::apply_manifest(context, &create_spec)?;
    }
    // 2. Seed status with phase=Connecting and inject the matching cert.  The GridSite controller Connecting arm
    //    promotes to Active when the TCP probe passes and the fingerprint matches. Seeding Connecting is necessary for
    //    newly-created GridSites that would otherwise remain in Pending (which the GridSite controller never
    //    auto-advances; only the GridNetwork controller does that on SWIM Alive events).  For pre-existing Connecting
    //    sites this is a no-op on phase, only adding the cert.
    let cert_merge = serde_json::json!({
        "status": {
            "phase": "Connecting",
            "reason": "HarnessPreparation",
            "message": "harness-seeded Connecting; awaiting trust verification",
            "publicCertPem": TEST_TRUST_CERT_PEM,
        }
    });
    let out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "patch",
            "gridsites",
            site_k8s_name,
            "--subresource=status",
            "--type=merge",
            "-p",
            &cert_merge.to_string(),
        ])
        .output()?;
    if !out.status.success() {
        return Err(format!(
            "status merge patch for {site_k8s_name} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    eprintln!(
        "  [OK] GridSite {site_k8s_name:?} spec+trust configured; \
         publicCertPem injected — controller will promote to Active when TCP probe passes \
         (network={network_ref:?}, egress={egress_addr:?})"
    );
    Ok(())
}

/// Assert that the overlay for `network/gw` has NO candidate with `site == excluded_site`.
///
/// Used to prove that CRDT provider records from a specific site with no Active `GridSite`
/// are excluded from the routing overlay (routing eligibility gate).
///
/// `excluded_site` is the SWIM site identity of the peer whose CRDT providers should be absent.
/// This is more precise than checking `site != primary` because local providers may use
/// fallback site names (e.g., the provider name) when no `GridSite` matches.
pub(crate) fn assert_no_crdt_candidates_for_site(
    context: &str,
    network: &str,
    gw: &str,
    excluded_site: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let overlay = read_overlay_configmap(context, network, gw, "default")?;
    let candidates = overlay
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("overlay missing candidates array")?;

    let has_excluded = candidates
        .iter()
        .any(|c| c.get("site").and_then(serde_json::Value::as_str) == Some(excluded_site));

    if has_excluded {
        return Err(format!(
            "routing eligibility violation: overlay contains a candidate with site={excluded_site:?} \
             from a site without an Active GridSite; SWIM gossip must not be sufficient for routing"
        )
        .into());
    }

    eprintln!(
        "  [PASS] routing eligibility: no overlay candidate with site={excluded_site:?} \
         (CRDT providers from non-Active GridSite excluded)"
    );
    Ok(())
}

/// Delete resources created by the SWIM overlay validation.
///
/// Safe to call before a fresh run — all deletes use `--ignore-not-found`.
pub(crate) fn cleanup_swim_overlay_test_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_namespaced_resource(
        context,
        "default",
        "configmap",
        &format!("grid-overlay-{SWIM_OVERLAY_NETWORK}-{SWIM_OVERLAY_GW}"),
    )?;
    delete_cluster_resource(context, "inferenceprovider", SWIM_OVERLAY_PROVIDER)?;
    delete_cluster_resource(context, "gridnetwork", SWIM_OVERLAY_NETWORK)?;
    cleanup_auto_discovered_gridsites_for_network(context, SWIM_OVERLAY_NETWORK);
    eprintln!("  [OK] stale SWIM overlay test resources removed");
    Ok(())
}

/// Apply test fixtures for the SWIM transport encryption E2E.
///
/// Creates a `GridNetwork` with a single `gatewayRef` and an `InferenceProvider`
/// for the keyed node.  The wrong-key and plaintext nodes should NOT have their
/// providers appear in the overlay.
pub(crate) fn apply_swim_encrypt_test_fixtures(
    context: &str,
    site_a_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    apply_swim_encrypt_test_fixtures_with_options(context, site_a_name, &[], false)
}

/// Apply SWIM encryption fixtures with optional CRD seed and `SecretRef` config.
#[expect(
    clippy::too_many_lines,
    reason = "sequential GridNetwork + InferenceProvider JSON construction and apply"
)]
pub(crate) fn apply_swim_encrypt_test_fixtures_with_options(
    context: &str,
    site_a_name: &str,
    seeds: &[String],
    use_swim_key_ref: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let tls = if use_swim_key_ref {
        serde_json::json!({
            "swimKeyRef": {
                "name": SWIM_ENCRYPT_KEY_SECRET,
                "namespace": "default",
                "key": "key"
            }
        })
    } else {
        serde_json::json!({})
    };
    let network = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": SWIM_ENCRYPT_NETWORK },
        "spec": {
            "seeds": seeds,
            "tls": tls,
            "gatewayRefs": [{
                "name": SWIM_ENCRYPT_GW,
                "namespace": "default",
                "localSiteName": site_a_name
            }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM encrypt network fixture serialization failed: {e}");
        std::process::exit(1);
    });
    let provider = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": SWIM_ENCRYPT_PROVIDER_A },
        "spec": {
            "gridNetworkRef": SWIM_ENCRYPT_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [{ "name": SWIM_ENCRYPT_MODEL_A }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM encrypt provider fixture serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &network)?;
    kubectl::apply_manifest(context, &provider)?;
    eprintln!(
        "  [OK] SWIM encrypt fixtures applied (network={SWIM_ENCRYPT_NETWORK}, provider={SWIM_ENCRYPT_PROVIDER_A})"
    );
    Ok(())
}

/// Apply a Secret suitable for `GridNetwork.spec.tls.swimKeyRef`.
///
/// `key_material` must be exactly 32 UTF-8 bytes so Kubernetes stores a valid
/// AES-256-GCM key in the Secret's `data.key` field.
pub(crate) fn apply_swim_encrypt_key_secret(
    context: &str,
    key_material: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if key_material.len() != 32 {
        return Err("SWIM encryption test Secret key material must be exactly 32 bytes".into());
    }
    let manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": SWIM_ENCRYPT_KEY_SECRET,
            "namespace": "default"
        },
        "type": "Opaque",
        "stringData": {
            "key": key_material
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM encrypt key Secret serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &manifest)?;
    eprintln!("  [OK] SWIM encrypt key Secret applied (name={SWIM_ENCRYPT_KEY_SECRET})");
    Ok(())
}

/// Remove resources created by the SWIM encryption validation.
pub(crate) fn cleanup_swim_encrypt_test_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_namespaced_resource(
        context,
        "default",
        "configmap",
        &format!("grid-overlay-{SWIM_ENCRYPT_NETWORK}-{SWIM_ENCRYPT_GW}"),
    )?;
    delete_namespaced_resource(context, "default", "secret", SWIM_ENCRYPT_KEY_SECRET)?;
    delete_cluster_resource(context, "inferenceprovider", SWIM_ENCRYPT_PROVIDER_A)?;
    delete_cluster_resource(context, "gridnetwork", SWIM_ENCRYPT_NETWORK)?;
    cleanup_auto_discovered_gridsites_for_network(context, SWIM_ENCRYPT_NETWORK);
    eprintln!("  [OK] stale SWIM encryption test resources removed");
    Ok(())
}

/// Verify that the overlay `ConfigMap` contains at least one remote CRDT candidate.
///
/// The overlay is read from `grid-overlay-{network}-{gw}` in the `default`
/// namespace.  At least one candidate must have `site` different from
/// `primary_site_name`, proving that a CRDT-sourced record entered the overlay.
///
/// # Errors
///
/// Returns an error when:
/// - The `ConfigMap` cannot be read or parsed as JSON.
/// - The `candidates` array is empty.
/// - No candidate has a `site` field different from `primary_site_name`.
pub(crate) fn verify_swim_overlay_candidates(
    context: &str,
    network: &str,
    gw: &str,
    primary_site_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let overlay = read_overlay_configmap(context, network, gw, "default")?;
    let candidates = overlay
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("overlay missing candidates array")?;

    if candidates.is_empty() {
        return Err("SWIM overlay validation failed: candidates array is empty".into());
    }

    let has_remote = candidates
        .iter()
        .any(|c| c.get("site").and_then(serde_json::Value::as_str) != Some(primary_site_name));

    if !has_remote {
        return Err(format!(
            "SWIM overlay validation failed: no candidate has a site != {primary_site_name:?}; \
             remote CRDT provider records have not entered the overlay"
        )
        .into());
    }

    let remote_count = candidates
        .iter()
        .filter(|c| c.get("site").and_then(serde_json::Value::as_str) != Some(primary_site_name))
        .count();
    eprintln!(
        "  [OK] SWIM overlay: {remote_count} remote candidate(s) present \
         (site != {primary_site_name:?})"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Three-node SWIM mesh helpers
// ---------------------------------------------------------------------------

/// Poll until `GridNetwork.status.distributedProviderCount >= min_count`.
///
/// Uses the same bump-per-poll pattern as [`wait_for_gridnetwork_distributed_state`]
/// so the operator reconciles during each wait cycle.
///
/// Returns the actual count when the threshold is reached.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous poll loop in xtask; no async runtime available"
)]
pub(crate) fn wait_for_distributed_state_count(
    context: &str,
    name: &str,
    min_count: u32,
    timeout: Duration,
) -> Result<u32, Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        drop(bump_gridnetwork(context, name));
        let count_str = kubectl_jsonpath(
            context,
            &format!("gridnetworks/{name}"),
            "{.status.distributedProviderCount}",
        )
        .unwrap_or_default();
        let count: u32 = count_str.parse().unwrap_or(0);
        if count >= min_count {
            eprintln!("  [OK] GridNetwork {name}: distributedProviderCount={count} (>= {min_count})");
            return Ok(count);
        }
        if start.elapsed() >= timeout {
            return Err(format!(
                "timeout waiting for GridNetwork {name} distributedProviderCount >= {min_count}; \
                 last observed: {count}"
            )
            .into());
        }
        eprintln!(
            "  waiting for GridNetwork {name} distributedProviderCount >= {min_count} \
             (current={count})..."
        );
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Assert that the overlay contains a candidate with `site == expected_site`.
///
/// Used to prove that a CRDT provider from a specific site reached the overlay after
/// its `GridSite` was set to `Active`.
pub(crate) fn verify_overlay_has_site_candidate(
    context: &str,
    network: &str,
    gw: &str,
    expected_site: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let overlay = read_overlay_configmap(context, network, gw, "default")?;
    let candidates = overlay
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("overlay missing candidates array")?;

    let found = candidates
        .iter()
        .any(|c| c.get("site").and_then(serde_json::Value::as_str) == Some(expected_site));

    if !found {
        return Err(format!(
            "routing eligibility: overlay has no candidate with site={expected_site:?}; \
             expected the site's CRDT provider to appear after GridSite became Active"
        )
        .into());
    }
    eprintln!(
        "  [PASS] routing eligibility: overlay contains candidate with site={expected_site:?} \
         (GridSite Active → eligible)"
    );
    Ok(())
}

/// Poll until the overlay for `network/gw` contains a candidate with `site == expected_site`.
///
/// Bumps the `GridNetwork` on each poll cycle to force a reconcile.  Used after setting
/// a `GridSite` to `Active` to wait for the overlay to be re-rendered with the new candidate.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous poll loop in xtask; no async runtime available"
)]
pub(crate) fn wait_for_site_candidate_in_overlay(
    context: &str,
    network: &str,
    gw: &str,
    expected_site: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        drop(bump_gridnetwork(context, network));
        if let Ok(()) = verify_overlay_has_site_candidate(context, network, gw, expected_site) {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(format!(
                "timeout waiting for overlay candidate with site={expected_site:?} \
                 in {network}/{gw}"
            )
            .into());
        }
        eprintln!("  waiting for overlay candidate with site={expected_site:?} in {network}/{gw}...");
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Apply the three-node SWIM mesh `GridNetwork` and leaf-node `InferenceProvider` fixtures.
///
/// The `GridNetwork` uses `localSiteName = site_a_name` so node A's operator renders the
/// overlay `ConfigMap`.  The `InferenceProvider` has no `siteSelector` or `routingClusterRef`,
/// so each operator publishes it as CRDT with its own `site_id`.  The leaf node C's CRDT
/// contribution (`site_id = site_c_name`) is the primary proof target.
#[expect(
    clippy::too_many_lines,
    reason = "sequential GridNetwork + InferenceProvider JSON construction and apply"
)]
pub(crate) fn apply_swim_mesh_test_fixtures(
    context: &str,
    site_a_name: &str,
    provider_name: &str,
    model_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let network = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": SWIM_MESH_NETWORK },
        "spec": {
            "seeds": [],
            "gatewayRefs": [{
                "name": SWIM_MESH_GW,
                "namespace": "default",
                "localSiteName": site_a_name
            }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM mesh network fixture serialization failed: {e}");
        std::process::exit(1);
    });
    let provider = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": provider_name },
        "spec": {
            "gridNetworkRef": SWIM_MESH_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [{ "name": model_name }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM mesh provider fixture serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &network)?;
    kubectl::apply_manifest(context, &provider)?;
    eprintln!(
        "  [OK] SWIM mesh fixtures applied (network={SWIM_MESH_NETWORK}, \
         provider={provider_name}, model={model_name})"
    );
    Ok(())
}

/// Apply a wrong-network `GridNetwork` and `InferenceProvider` for cross-network
/// isolation testing.
///
/// The fixtures use [`SWIM_MESH_WRONG_NETWORK`] as the network name.  Resources in
/// this network must never appear in the overlay rendered for [`SWIM_MESH_NETWORK`].
#[expect(
    clippy::too_many_lines,
    reason = "sequential wrong-network GridNetwork + InferenceProvider JSON construction and apply"
)]
pub(crate) fn apply_swim_mesh_wrong_network_fixtures(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let network = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": SWIM_MESH_WRONG_NETWORK },
        "spec": {
            "seeds": [],
            "gatewayRefs": [{
                "name": SWIM_MESH_GW,
                "namespace": "default",
                "localSiteName": "swim-mesh-wrong"
            }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM mesh wrong-network fixture serialization failed: {e}");
        std::process::exit(1);
    });
    let provider = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": SWIM_MESH_WRONG_PROVIDER },
        "spec": {
            "gridNetworkRef": SWIM_MESH_WRONG_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [{ "name": SWIM_MESH_WRONG_MODEL }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM mesh wrong-network provider serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &network)?;
    kubectl::apply_manifest(context, &provider)?;
    eprintln!(
        "  [OK] SWIM mesh wrong-network fixtures applied \
         (network={SWIM_MESH_WRONG_NETWORK}, provider={SWIM_MESH_WRONG_PROVIDER})"
    );
    Ok(())
}

/// Assert that the overlay contains no candidate with `model == excluded_model`.
///
/// Used to prove that providers from a wrong-network `GridNetwork` do not leak into
/// the correct-network overlay.
pub(crate) fn assert_no_overlay_candidate_for_model(
    context: &str,
    network: &str,
    gw: &str,
    excluded_model: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let overlay = read_overlay_configmap(context, network, gw, "default")?;
    let candidates = overlay
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("overlay missing candidates array")?;

    let found = candidates
        .iter()
        .any(|c| c.get("name").and_then(serde_json::Value::as_str) == Some(excluded_model));

    if found {
        return Err(format!(
            "cross-network isolation violation: overlay contains a candidate with \
             model={excluded_model:?} from wrong-network resources"
        )
        .into());
    }
    eprintln!("  [PASS] cross-network isolation: overlay has no candidate with model={excluded_model:?}");
    Ok(())
}

/// Delete all resources created by the three-node SWIM mesh validation.
///
/// Safe to call before a fresh run — all deletes use `--ignore-not-found`.
pub(crate) fn cleanup_swim_mesh_test_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_namespaced_resource(
        context,
        "default",
        "configmap",
        &format!("grid-overlay-{SWIM_MESH_NETWORK}-{SWIM_MESH_GW}"),
    )?;
    delete_cluster_resource(context, "inferenceprovider", SWIM_MESH_PROVIDER_C)?;
    delete_cluster_resource(context, "inferenceprovider", SWIM_MESH_WRONG_PROVIDER)?;
    delete_cluster_resource(context, "gridnetwork", SWIM_MESH_NETWORK)?;
    delete_cluster_resource(context, "gridnetwork", SWIM_MESH_WRONG_NETWORK)?;
    cleanup_auto_discovered_gridsites_for_network(context, SWIM_MESH_NETWORK);
    cleanup_auto_discovered_gridsites_for_network(context, SWIM_MESH_WRONG_NETWORK);
    eprintln!("  [OK] stale SWIM mesh test resources removed");
    Ok(())
}

// ---------------------------------------------------------------------------
// Fingerprint trust promotion helpers
// ---------------------------------------------------------------------------

/// Compute the SHA-256 fingerprint of a PEM string.
///
/// Returns a colon-separated lowercase hex string (95 chars: 32 pairs + 31 colons).
/// This matches the algorithm used by `operator::resources::trust_bundle::sha256_fingerprint`
/// and is the correct value for `GridSite.spec.trust.certFingerprint`.
#[must_use]
pub(crate) fn sha256_fingerprint(pem_str: &str) -> String {
    // Trim surrounding whitespace so fingerprints match regardless of trailing
    // newlines from kubectl jsonpath vs Kubernetes API deserialization.
    let digest = Sha256::digest(pem_str.trim().as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(":")
}

/// Apply the trust fingerprint test `GridNetwork` and `InferenceProvider` fixtures.
///
/// The `GridNetwork` is configured with `localSiteName = site_a_name` so that
/// node A's operator renders the overlay.  `spec.tls` references are set so that
/// node B's operator generates a site certificate and broadcasts it via SWIM.
#[expect(clippy::too_many_lines, reason = "sequential JSON fixture construction")]
pub(crate) fn apply_swim_trust_test_fixtures(
    context: &str,
    site_a_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let network = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": {
            "name": SWIM_TRUST_NETWORK,
            "labels": {
                // Enable auto-discovery so the operator creates auto-discovered GridSites
                // and populates publicCertPem when cert broadcasts are received.
                "grid.praxis-proxy.io/auto-discover-sites": "true"
            }
        },
        "spec": {
            "seeds": [],
            "gatewayRefs": [{
                "name": SWIM_TRUST_GW,
                "namespace": "default",
                "localSiteName": site_a_name
            }],
            "tls": {
                "caSecretRef": {
                    "name": SWIM_TRUST_CA_SECRET,
                    "namespace": "default"
                },
                "siteSecretRef": {
                    "name": SWIM_TRUST_SITE_SECRET,
                    "namespace": "default"
                }
            }
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM trust network fixture serialization failed: {e}");
        std::process::exit(1);
    });
    let provider = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": SWIM_TRUST_PROVIDER_B },
        "spec": {
            "gridNetworkRef": SWIM_TRUST_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [{ "name": SWIM_TRUST_MODEL_B }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM trust provider fixture serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &network)?;
    kubectl::apply_manifest(context, &provider)?;
    eprintln!(
        "  [OK] SWIM trust fixtures applied (network={SWIM_TRUST_NETWORK}, \
         provider={SWIM_TRUST_PROVIDER_B}, model={SWIM_TRUST_MODEL_B})"
    );
    Ok(())
}

/// Delete all resources created by the fingerprint trust promotion validation.
pub(crate) fn cleanup_swim_trust_test_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_namespaced_resource(
        context,
        "default",
        "configmap",
        &format!("grid-overlay-{SWIM_TRUST_NETWORK}-{SWIM_TRUST_GW}"),
    )?;
    delete_cluster_resource(context, "inferenceprovider", SWIM_TRUST_PROVIDER_B)?;
    delete_cluster_resource(context, "gridnetwork", SWIM_TRUST_NETWORK)?;
    // Clean up TLS Secrets created by ensure_tls_secrets.
    drop(
        Command::new("kubectl")
            .args([
                "--context",
                context,
                "delete",
                "secret",
                SWIM_TRUST_CA_SECRET,
                SWIM_TRUST_SITE_SECRET,
                "-n",
                "default",
                "--ignore-not-found",
            ])
            .output(),
    );
    cleanup_auto_discovered_gridsites_for_network(context, SWIM_TRUST_NETWORK);
    eprintln!("  [OK] stale SWIM trust test resources removed");
    Ok(())
}

/// Read `status.publicCertPem` from a `GridSite` resource.
///
/// Returns `None` when the field is absent or empty, `Some(pem)` otherwise.
pub(crate) fn read_gridsite_public_cert_pem(context: &str, site_name: &str) -> Option<String> {
    let out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "get",
            "gridsites",
            site_name,
            "-o",
            "jsonpath={.status.publicCertPem}",
            "--ignore-not-found",
        ])
        .output()
        .ok()?;
    let pem = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    if pem.is_empty() { None } else { Some(pem) }
}

/// Apply egress address to a `GridSite` spec without patching the phase.
///
/// This allows the `GridSite` controller to advance phases naturally —
/// use this to exercise the trust-policy-gated promotion path from the
/// auto-discovered lifecycle state.
pub(crate) fn apply_gridsite_egress(
    context: &str,
    site_k8s_name: &str,
    network_ref: &str,
    egress_addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let spec = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridSite",
        "metadata": { "name": site_k8s_name },
        "spec": {
            "gridNetworkRef": network_ref,
            "egress": { "address": egress_addr, "tls": { "mode": "Mutual" } }
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("GridSite egress spec serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &spec)?;
    eprintln!("  [OK] GridSite {site_k8s_name:?}: egress={egress_addr:?} applied (phase NOT patched)");
    Ok(())
}

/// Poll until `GridSite.status.publicCertPem` is non-empty.
///
/// Returns the PEM string when available, or an error on timeout.
#[expect(clippy::disallowed_methods, reason = "synchronous poll loop in xtask")]
pub(crate) fn wait_for_gridsite_public_cert_pem(
    context: &str,
    site_name: &str,
    timeout: Duration,
) -> Result<String, Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        if let Some(pem) = read_gridsite_public_cert_pem(context, site_name) {
            eprintln!("  [OK] GridSite {site_name:?}: publicCertPem received");
            return Ok(pem);
        }
        if start.elapsed() >= timeout {
            return Err(format!("timeout waiting for GridSite {site_name:?} publicCertPem").into());
        }
        eprintln!("  waiting for GridSite {site_name:?} publicCertPem...");
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Read the `status.reason` field from a `GridSite` resource.
pub(crate) fn read_gridsite_reason(context: &str, site_name: &str) -> String {
    Command::new("kubectl")
        .args([
            "--context",
            context,
            "get",
            "gridsites",
            site_name,
            "-o",
            "jsonpath={.status.reason}",
            "--ignore-not-found",
        ])
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default()
}

/// Patch `GridSite.spec.trust.certFingerprint`.
pub(crate) fn patch_gridsite_cert_fingerprint(
    context: &str,
    site_name: &str,
    fingerprint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let patch = serde_json::to_string(&serde_json::json!({
        "spec": { "trust": { "certFingerprint": fingerprint } }
    }))
    .unwrap_or_else(|_| "{}".to_owned());
    let out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "patch",
            "gridsites",
            site_name,
            "--type=merge",
            "-p",
            &patch,
        ])
        .output()?;
    if out.status.success() {
        eprintln!("  [OK] GridSite {site_name:?}: spec.trust.certFingerprint patched");
        Ok(())
    } else {
        Err(format!(
            "kubectl patch gridsite {site_name} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into())
    }
}

/// Poll until `GridSite.status.reason == expected_reason`, bumping `network` each cycle.
#[expect(clippy::disallowed_methods, reason = "synchronous poll loop in xtask")]
pub(crate) fn wait_for_gridsite_reason_in_network(
    context: &str,
    site_name: &str,
    network: &str,
    expected_reason: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        drop(bump_gridnetwork(context, network));
        let reason = read_gridsite_reason(context, site_name);
        if reason == expected_reason {
            eprintln!("  [OK] GridSite {site_name:?}: status.reason={reason:?}");
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(format!(
                "timeout waiting for GridSite {site_name:?} reason={expected_reason:?}; \
                 last observed: {reason:?}"
            )
            .into());
        }
        eprintln!(
            "  waiting for GridSite {site_name:?} reason={expected_reason:?} \
             (current={reason:?})..."
        );
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Poll until `GridSite.status.reason == expected_reason` (bumps [`SWIM_TRUST_NETWORK`]).
#[expect(clippy::disallowed_methods, reason = "synchronous poll loop in xtask")]
pub(crate) fn wait_for_gridsite_reason(
    context: &str,
    site_name: &str,
    expected_reason: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        drop(bump_gridnetwork(context, SWIM_TRUST_NETWORK));
        let reason = read_gridsite_reason(context, site_name);
        if reason == expected_reason {
            eprintln!("  [OK] GridSite {site_name:?}: status.reason={reason:?}");
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(format!(
                "timeout waiting for GridSite {site_name:?} reason={expected_reason:?}; \
                 last observed: {reason:?}"
            )
            .into());
        }
        eprintln!(
            "  waiting for GridSite {site_name:?} reason={expected_reason:?} \
             (current={reason:?})..."
        );
        std::thread::sleep(POLL_INTERVAL);
    }
}

// ---------------------------------------------------------------------------
// SWIM routing fixture helpers
// ---------------------------------------------------------------------------

/// Apply the east-side fixtures for the cross-cluster SWIM routing validation.
///
/// Creates on the east cluster:
/// - [`SWIM_ROUTING_NETWORK`] `GridNetwork` with a `gatewayRef` pointing to [`SWIM_ROUTING_GW`] and `localSiteName` set
///   to `east_site_name`.  The primary operator writes the overlay `ConfigMap` to this cluster.
/// - [`SWIM_ROUTING_EAST_PROVIDER`] `InferenceProvider` serving `east_model` with `routingClusterRef = east_site_name`.
///
/// The `routingClusterRef` must match the east provider gateway's site name so
/// that `candidates_yaml` maps the candidate to `gateway-{east_site_name}` in
/// the consumer `load_balancer`.
#[expect(
    clippy::too_many_lines,
    reason = "two JSON manifest builds (GridNetwork + InferenceProvider) plus apply calls"
)]
pub(crate) fn apply_swim_routing_east_fixtures(
    context: &str,
    east_site_name: &str,
    east_model: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let network = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": SWIM_ROUTING_NETWORK },
        "spec": {
            "seeds": [],
            "gatewayRefs": [{
                "name": SWIM_ROUTING_GW,
                "namespace": "default",
                "localSiteName": east_site_name
            }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM routing east network fixture serialization failed: {e}");
        std::process::exit(1);
    });
    let provider = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": SWIM_ROUTING_EAST_PROVIDER },
        "spec": {
            "gridNetworkRef": SWIM_ROUTING_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [{ "name": east_model }],
            "routingClusterRef": east_site_name
            // No healthCheck: omitting health checks leaves the phase at
            // Pending, which is included in overlay candidates.  A configured
            // health check that fails from outside the cluster sets the phase
            // to Unavailable and the provider would be excluded from the overlay.
            // The annotation-bump approach forces the post-gossip reconcile
            // deterministically without relying on health-check-driven cascades.
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM routing east provider fixture serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &network)?;
    kubectl::apply_manifest(context, &provider)?;
    eprintln!(
        "  [OK] SWIM routing east fixtures applied \
         (network={SWIM_ROUTING_NETWORK}, provider={SWIM_ROUTING_EAST_PROVIDER}, \
         model={east_model}, routingClusterRef={east_site_name})"
    );
    Ok(())
}

/// Apply the west-side fixtures for the cross-cluster SWIM routing validation.
///
/// Creates on the west cluster:
/// - [`SWIM_ROUTING_NETWORK`] `GridNetwork` without a `gatewayRef` — the peer operator reconciles this to publish its
///   CRDT state but does not write an overlay `ConfigMap` (only the east primary generates the overlay).
/// - [`SWIM_ROUTING_WEST_PROVIDER`] `InferenceProvider` serving `west_model` with `routingClusterRef = west_site_name`.
///
/// After SWIM gossip, the primary (east) operator reads the peer's CRDT state
/// and adds a remote candidate for `west_model` to the east overlay.
#[expect(
    clippy::too_many_lines,
    reason = "two JSON manifest builds (GridNetwork + InferenceProvider) plus apply calls"
)]
pub(crate) fn apply_swim_routing_west_fixtures(
    context: &str,
    west_site_name: &str,
    west_model: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let network = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": SWIM_ROUTING_NETWORK },
        "spec": { "seeds": [] }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM routing west network fixture serialization failed: {e}");
        std::process::exit(1);
    });
    let provider = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": SWIM_ROUTING_WEST_PROVIDER },
        "spec": {
            "gridNetworkRef": SWIM_ROUTING_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [{ "name": west_model }],
            "routingClusterRef": west_site_name
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("SWIM routing west provider fixture serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &network)?;
    kubectl::apply_manifest(context, &provider)?;
    eprintln!(
        "  [OK] SWIM routing west fixtures applied \
         (network={SWIM_ROUTING_NETWORK}, provider={SWIM_ROUTING_WEST_PROVIDER}, \
         model={west_model}, routingClusterRef={west_site_name})"
    );
    Ok(())
}

/// Delete resources created by the SWIM routing validation on a given cluster.
///
/// Safe to call before a fresh run — all deletes use `--ignore-not-found`.
/// Call once per cluster (east and west).
pub(crate) fn cleanup_swim_routing_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_namespaced_resource(
        context,
        "default",
        "configmap",
        &format!("grid-overlay-{SWIM_ROUTING_NETWORK}-{SWIM_ROUTING_GW}"),
    )?;
    delete_cluster_resource(context, "inferenceprovider", SWIM_ROUTING_EAST_PROVIDER)?;
    delete_cluster_resource(context, "inferenceprovider", SWIM_ROUTING_WEST_PROVIDER)?;
    delete_cluster_resource(context, "gridnetwork", SWIM_ROUTING_NETWORK)?;
    // Also delete any auto-discovered GridSites for this network.
    // These are created by reconcile_discovered_sites when the network has the opt-in
    // label; cleanup must remove them even if the label was later removed or the test
    // runs mixed between opt-in and non-opt-in states.
    cleanup_auto_discovered_gridsites_for_network(context, SWIM_ROUTING_NETWORK);
    eprintln!("  [OK] stale SWIM routing test resources removed on {context}");
    Ok(())
}

/// Delete all `GridSite` resources whose `spec.gridNetworkRef` matches `network_name`.
///
/// Used during cleanup to remove auto-discovered `GridSite` records that were created
/// by `reconcile_discovered_sites` when the network had the opt-in label.  Deletion is
/// best-effort; individual failures are logged but do not abort the cleanup.
pub(crate) fn cleanup_auto_discovered_gridsites_for_network(context: &str, network_name: &str) {
    let names = list_gridsites_for_network(context, network_name).unwrap_or_default();
    for name in &names {
        delete_cluster_resource(context, "gridsite", name)
            .unwrap_or_else(|e| eprintln!("  note: failed to delete GridSite {name:?}: {e}"));
    }
    if !names.is_empty() {
        eprintln!(
            "  [OK] deleted {} auto-discovered GridSite(s) for network {network_name:?}",
            names.len()
        );
    }
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
// Multi-provider fixture helpers
// ---------------------------------------------------------------------------

/// Derive the `InferenceProvider` fixture name for a provider site in
/// multi-provider mode.
///
/// Used by `run_multi_provider_reconcile` to create distinct fixture names
/// without colliding with the single-provider fixtures (`op-e2e-healthy`, etc.).
pub(crate) fn multi_provider_fixture_name(site: &str) -> String {
    format!("op-e2e-{site}")
}

/// Build an `InferenceProvider` JSON fixture for one provider site.
///
/// `models` is taken from the site's config entry so the overlay candidates
/// carry the correct model names for consumer-gateway routing.
/// `routing_cluster` is set as `spec.routingClusterRef` so the operator
/// generates overlay candidates with `site = cluster = routing_cluster`.
fn multi_provider_fixture_json(
    name: &str,
    network_ref: &str,
    endpoint: &str,
    routing_cluster: &str,
    models: &[String],
) -> String {
    let models_json: Vec<serde_json::Value> = models.iter().map(|m| serde_json::json!({ "name": m })).collect();
    serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": name },
        "spec": {
            "gridNetworkRef": network_ref,
            "providerKind": "open_ai",
            "backendKind": "local",
            "endpoint": endpoint,
            "models": models_json,
            "routingClusterRef": routing_cluster
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("multi-provider fixture serialization failed: {e}");
        std::process::exit(1);
    })
}

/// Apply a `GridNetwork` + one `InferenceProvider` per provider site.
///
/// Used in multi-provider mode instead of `apply_test_fixtures`.  Each
/// provider gets a distinct fixture name (`op-e2e-{site}`) and
/// `routingClusterRef = site` so the operator-generated overlay candidates
/// carry the correct site identity for consumer-gateway routing.
///
/// The shared in-cluster `provider_endpoint` must be non-blank so providers
/// reconcile to `Pending` rather than `Unavailable`.
pub(crate) fn apply_multi_provider_fixtures(
    context: &str,
    providers: &[(&str, &[String])],
    provider_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let network = network_fixture_json(TEST_NETWORK, TEST_GATEWAY_NAME, TEST_GATEWAY_NS);
    kubectl::apply_manifest(context, &network)?;
    for &(site_name, models) in providers {
        let fixture_name = multi_provider_fixture_name(site_name);
        let json = multi_provider_fixture_json(&fixture_name, TEST_NETWORK, provider_endpoint, site_name, models);
        kubectl::apply_manifest(context, &json)?;
    }
    eprintln!("  [OK] multi-provider fixtures applied ({} sites)", providers.len());
    Ok(())
}

/// Delete the overlay `ConfigMap`, `GridNetwork`, and all multi-provider
/// `InferenceProvider` fixtures created by `apply_multi_provider_fixtures`.
pub(crate) fn cleanup_multi_provider_resources(
    context: &str,
    site_names: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    delete_namespaced_resource(
        context,
        TEST_GATEWAY_NS,
        "configmap",
        &format!("grid-overlay-{TEST_NETWORK}-{TEST_GATEWAY_NAME}"),
    )?;
    for &site_name in site_names {
        delete_cluster_resource(context, "inferenceprovider", &multi_provider_fixture_name(site_name))?;
    }
    delete_cluster_resource(context, "gridnetwork", TEST_NETWORK)?;
    eprintln!("  [OK] stale multi-provider validation resources removed");
    Ok(())
}

/// Verify that the operator-generated overlay contains at least one
/// `fresh=true` candidate for each expected provider site, and that all
/// candidates have the required Praxis wire-format fields.
///
/// This is the multi-provider equivalent of `verify_overlay`: it checks
/// coverage across all provider sites rather than verifying a single healthy
/// cluster and an excluded unavailable cluster.
pub(crate) fn verify_multi_provider_overlay(
    overlay: &serde_json::Value,
    site_names: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let candidates = overlay["candidates"]
        .as_array()
        .ok_or("overlay missing candidates array")?;

    // Every candidate must carry the required Praxis wire-format fields.
    for c in candidates {
        for field in &["kind", "name", "site", "cluster", "fresh"] {
            if c.get(*field).is_none() {
                return Err(format!("candidate missing required field '{field}'").into());
            }
        }
    }

    // Every expected site must have at least one fresh=true candidate.
    for &site_name in site_names {
        let has_fresh = candidates
            .iter()
            .any(|c| c["site"].as_str() == Some(site_name) && c["fresh"].as_bool() == Some(true));
        if !has_fresh {
            return Err(format!(
                "provider site '{site_name}' must have at least one fresh=true candidate in overlay; \
                 check that the operator reconciled the InferenceProvider and the site is reachable"
            )
            .into());
        }
        eprintln!("  [OK] site '{site_name}' present with fresh=true candidate");
    }
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
// Site join / discovery helpers
// ---------------------------------------------------------------------------

/// Apply the `GridNetwork` for the site-join-discovery validation.
///
/// Creates `GridNetwork` [`SITE_JOIN_NETWORK`] on `context` with a single
/// `gatewayRef` pointing at [`SITE_JOIN_GW`].  `local_site_name` is the
/// `localSiteName` entry used by the operator to locate its own overlay slot.
pub(crate) fn apply_site_join_network(context: &str, local_site_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": {
            "name": SITE_JOIN_NETWORK,
            // Opt-in label: enables automatic GridSite discovery for Alive SWIM members.
            // Only networks with this label run reconcile_discovered_sites in the controller.
            "labels": { "grid.praxis-proxy.io/auto-discover-sites": "true" }
        },
        "spec": {
            "seeds": [],
            "gatewayRefs": [{
                "name": SITE_JOIN_GW,
                "namespace": "default",
                "localSiteName": local_site_name
            }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("site-join GridNetwork serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &manifest)?;
    eprintln!("  [OK] GridNetwork {SITE_JOIN_NETWORK:?} applied (localSiteName={local_site_name:?})");
    Ok(())
}

/// Apply the wrong-network `GridNetwork` used for cross-network isolation testing.
///
/// Creates `GridNetwork` [`SITE_JOIN_WRONG_NETWORK`] on `context` with no
/// gateway refs.  The operator reconciles this as a valid network; resources
/// referencing it stay isolated from [`SITE_JOIN_NETWORK`].
pub(crate) fn apply_site_join_wrong_network(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": SITE_JOIN_WRONG_NETWORK },
        "spec": { "seeds": [] }
    }))
    .unwrap_or_else(|e| {
        eprintln!("site-join wrong GridNetwork serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &manifest)?;
    eprintln!("  [OK] GridNetwork {SITE_JOIN_WRONG_NETWORK:?} applied (isolation target)");
    Ok(())
}

/// Apply a `GridSite` resource with an egress address and a harness label.
///
/// The `label_value` is set on [`SITE_JOIN_LABEL_KEY`] so the site can be
/// selected by `InferenceProvider.spec.siteSelector.matchLabels` in the
/// overlay rendering step.
///
/// The site controller validates that `network_ref` exists as a `GridNetwork`
/// on the same cluster.  Apply the `GridNetwork` before this call.
pub(crate) fn apply_gridsite(
    context: &str,
    site_name: &str,
    network_ref: &str,
    egress_addr: &str,
    label_value: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridSite",
        "metadata": {
            "name": site_name,
            "labels": { SITE_JOIN_LABEL_KEY: label_value }
        },
        "spec": {
            "gridNetworkRef": network_ref,
            "egress": {
                "address": egress_addr,
                "tls": { "mode": "Mutual" }
            }
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("GridSite {site_name} serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &manifest)?;
    eprintln!(
        "  [OK] GridSite {site_name:?} applied \
         (network={network_ref:?}, egress={egress_addr:?}, label={label_value:?})"
    );
    Ok(())
}

/// Patch the `GridSite` status subresource to set `phase`.
///
/// The site controller reconciles after this patch.  Polling with
/// [`wait_for_gridsite_phase`] after this call confirms the expected phase is
/// observed after reconciliation.
///
/// This is xtask validation infrastructure — it simulates lifecycle states
/// that are otherwise reached through SWIM discovery, gateway reachability, and
/// trust policy configuration.
pub(crate) fn patch_gridsite_phase(
    context: &str,
    site_name: &str,
    phase: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let patch = serde_json::to_string(&serde_json::json!({"status": {"phase": phase}})).unwrap_or_else(|e| {
        eprintln!("gridsite phase patch serialization failed: {e}");
        std::process::exit(1);
    });
    let out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "patch",
            "gridsites",
            site_name,
            "--subresource=status",
            "--type=merge",
            "-p",
            &patch,
        ])
        .output()?;
    if !out.status.success() {
        return Err(format!(
            "kubectl patch gridsites/{site_name} status failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    eprintln!("  [OK] GridSite {site_name:?}: status.phase patched to {phase:?}");
    Ok(())
}

/// Read the current `status.phase` of a `GridSite`.
fn read_gridsite_phase(context: &str, site_name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "get",
            "gridsites",
            site_name,
            "-o",
            "jsonpath={.status.phase}",
        ])
        .output()?;
    if !out.status.success() {
        return Err(format!(
            "kubectl get gridsites/{site_name} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// Poll until `GridSite` `site_name` reaches `expected_phase`, or return `Err` on timeout.
///
/// The polling interval is 2 seconds, matching the pattern used by
/// [`wait_for_gridnetwork_active`] and other xtask polling helpers.
///
/// A successful poll confirms that the site controller preserved the phase
/// through at least one reconcile cycle after a [`patch_gridsite_phase`] call.
///
/// Returns `Err` immediately if `expected_phase` is not a recognised lifecycle
/// phase (as defined by [`gridsite_phase_index`]).
pub(crate) fn wait_for_gridsite_phase(
    context: &str,
    site_name: &str,
    expected_phase: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    // Guard: reject unknown phase names before starting the poll loop.
    if gridsite_phase_index(expected_phase).is_none() {
        return Err(format!("wait_for_gridsite_phase: {expected_phase:?} is not a recognised GridSite phase").into());
    }
    let deadline = Instant::now() + timeout;
    let mut last_phase = String::new();
    loop {
        let phase = read_gridsite_phase(context, site_name).unwrap_or_default();
        if phase == expected_phase {
            eprintln!("  [OK] GridSite {site_name:?}: phase={expected_phase:?} (confirmed)");
            return Ok(());
        }
        last_phase.clone_from(&phase);
        if Instant::now() >= deadline {
            return Err(format!(
                "timeout waiting for GridSite {site_name} phase={expected_phase:?}; \
                 last observed: {last_phase:?}"
            )
            .into());
        }
        #[expect(clippy::disallowed_methods, reason = "polling wait between GridSite status reads")]
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// Verify that a `GridSite` has the expected routing-relevant spec fields set.
///
/// Checks `spec.gridNetworkRef` and `spec.egress.address`, which together
/// provide the network identity and data-plane endpoint needed for routing.
/// The `status.phase` value is reported but not asserted here — call
/// [`wait_for_gridsite_phase`] separately to assert the lifecycle state.
#[expect(
    clippy::too_many_lines,
    reason = "jsonpath field split + two field assertions + diagnostic messages"
)]
pub(crate) fn verify_gridsite_routing_data(
    context: &str,
    site_name: &str,
    expected_network: &str,
    expected_egress: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "get",
            "gridsites",
            site_name,
            "-o",
            "jsonpath={.spec.gridNetworkRef}/{.spec.egress.address}/{.status.phase}",
        ])
        .output()?;
    if !out.status.success() {
        return Err(format!(
            "kubectl get gridsites/{site_name} for routing data failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    let fields: Vec<&str> = std::str::from_utf8(&out.stdout)
        .unwrap_or("")
        .trim()
        .splitn(3, '/')
        .collect();
    let network = fields.first().copied().unwrap_or("");
    let egress = fields.get(1).copied().unwrap_or("");
    let phase = fields.get(2).copied().unwrap_or("");

    if network != expected_network {
        return Err(format!("GridSite {site_name}: gridNetworkRef={network:?} (expected {expected_network:?})").into());
    }
    if egress != expected_egress {
        return Err(format!("GridSite {site_name}: egress.address={egress:?} (expected {expected_egress:?})").into());
    }
    eprintln!(
        "  [PASS] GridSite {site_name:?}: routing data complete \
         (network={expected_network:?}, egress={expected_egress:?}, phase={phase:?})"
    );
    Ok(())
}

/// Apply the primary site's `InferenceProvider` for the overlay generation step.
///
/// Uses `siteSelector.matchLabels` to restrict candidates to the primary
/// `GridSite` only, so the overlay contains exactly one candidate per model.
pub(crate) fn apply_site_join_primary_provider(
    context: &str,
    local_site_name: &str,
    model: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": SITE_JOIN_PRIMARY_PROVIDER },
        "spec": {
            "gridNetworkRef": SITE_JOIN_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [{ "name": model }],
            "routingClusterRef": local_site_name,
            "siteSelector": {
                "matchLabels": { SITE_JOIN_LABEL_KEY: "primary" }
            }
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("site-join primary provider serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &manifest)?;
    eprintln!(
        "  [OK] InferenceProvider {SITE_JOIN_PRIMARY_PROVIDER:?} applied \
         (model={model:?}, siteSelector=primary)"
    );
    Ok(())
}

/// Apply the joining site's `InferenceProvider` for the overlay generation step.
///
/// Uses `siteSelector.matchLabels` to restrict candidates to the joining
/// `GridSite` only.  After the joining site reaches `Active` phase, this
/// provider's model appears in the overlay under the joining site's identity.
pub(crate) fn apply_site_join_joining_provider(
    context: &str,
    joining_site_name: &str,
    model: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": SITE_JOIN_JOINING_PROVIDER },
        "spec": {
            "gridNetworkRef": SITE_JOIN_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "remote",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [{ "name": model }],
            "routingClusterRef": joining_site_name,
            "siteSelector": {
                "matchLabels": { SITE_JOIN_LABEL_KEY: "joining" }
            }
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("site-join joining provider serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &manifest)?;
    eprintln!(
        "  [OK] InferenceProvider {SITE_JOIN_JOINING_PROVIDER:?} applied \
         (model={model:?}, siteSelector=joining)"
    );
    Ok(())
}

/// Apply a wrong-network `InferenceProvider` for the cross-network isolation step.
///
/// This provider references [`SITE_JOIN_WRONG_NETWORK`] and must NOT appear
/// as a candidate in the overlay for [`SITE_JOIN_NETWORK`].
pub(crate) fn apply_site_join_wrong_provider(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": SITE_JOIN_WRONG_PROVIDER },
        "spec": {
            "gridNetworkRef": SITE_JOIN_WRONG_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [{ "name": "model-sjd-wrong" }],
            "routingClusterRef": SITE_JOIN_WRONG_SITE
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("site-join wrong provider serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &manifest)?;
    eprintln!(
        "  [OK] InferenceProvider {SITE_JOIN_WRONG_PROVIDER:?} applied \
         (wrong network, must be absent from {SITE_JOIN_NETWORK:?} overlay)"
    );
    Ok(())
}

/// Verify the site-join overlay contains the expected candidates and excludes wrong-network sites.
///
/// Reads the overlay `ConfigMap` generated for [`SITE_JOIN_NETWORK`] and
/// asserts:
/// - `primary_model` appears as a candidate attributed to `primary_site`
/// - `joining_model` appears as a candidate attributed to `joining_site`
/// - No candidate has `site == wrong_site` (cross-network isolation)
///
/// Returns `Ok(())` when all three assertions pass.
#[expect(
    clippy::too_many_lines,
    reason = "three candidate assertions with diagnostic messages"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "six distinct check parameters: primary/joining/wrong each need site and model"
)]
pub(crate) fn verify_site_join_overlay(
    context: &str,
    primary_site: &str,
    primary_model: &str,
    joining_site: &str,
    joining_model: &str,
    wrong_site: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let overlay = read_overlay_configmap(context, SITE_JOIN_NETWORK, SITE_JOIN_GW, "default")?;
    let candidates = overlay
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("overlay missing candidates array")?;

    let has_primary = candidates.iter().any(|c| {
        c.get("site").and_then(serde_json::Value::as_str) == Some(primary_site)
            && c.get("name").and_then(serde_json::Value::as_str) == Some(primary_model)
    });
    if !has_primary {
        return Err(
            format!("overlay missing primary candidate: model={primary_model:?} at site={primary_site:?}").into(),
        );
    }
    eprintln!("  [PASS] overlay: primary model {primary_model:?} -> site {primary_site:?}");

    let has_joining = candidates.iter().any(|c| {
        c.get("site").and_then(serde_json::Value::as_str) == Some(joining_site)
            && c.get("name").and_then(serde_json::Value::as_str) == Some(joining_model)
    });
    if !has_joining {
        return Err(
            format!("overlay missing joining candidate: model={joining_model:?} at site={joining_site:?}").into(),
        );
    }
    eprintln!("  [PASS] overlay: joining model {joining_model:?} -> site {joining_site:?}");

    let has_wrong = candidates
        .iter()
        .any(|c| c.get("site").and_then(serde_json::Value::as_str) == Some(wrong_site));
    if has_wrong {
        return Err(format!(
            "cross-network leakage: wrong-site {wrong_site:?} appears in \
             {SITE_JOIN_NETWORK:?} overlay"
        )
        .into());
    }
    eprintln!("  [PASS] overlay: wrong-network site {wrong_site:?} absent from {SITE_JOIN_NETWORK:?}");

    Ok(())
}

/// Poll until the site-join overlay contains both the primary and joining candidates.
///
/// Uses [`verify_site_join_overlay`] for each check.  Bumps the [`SITE_JOIN_NETWORK`]
/// `GridNetwork` each cycle so the controller re-renders the overlay with the most
/// recent provider list.  This is necessary because the `InferenceProvider` informer
/// cache may lag behind provider creation — a one-shot check right after bumping may
/// read a stale overlay that is missing newly-applied providers.
#[expect(clippy::disallowed_methods, reason = "synchronous poll loop in xtask")]
#[expect(clippy::too_many_arguments, reason = "delegates to verify_site_join_overlay")]
pub(crate) fn wait_for_site_join_overlay(
    context: &str,
    primary_site: &str,
    primary_model: &str,
    joining_site: &str,
    joining_model: &str,
    wrong_site: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        drop(bump_gridnetwork(context, SITE_JOIN_NETWORK));
        match verify_site_join_overlay(
            context,
            primary_site,
            primary_model,
            joining_site,
            joining_model,
            wrong_site,
        ) {
            Ok(()) => return Ok(()),
            Err(e) => {
                if start.elapsed() >= timeout {
                    return Err(format!("timeout: {e}").into());
                }
                eprintln!("  waiting for site-join overlay ({e})...");
                std::thread::sleep(POLL_INTERVAL);
            },
        }
    }
}

/// Derive the Kubernetes resource name for an auto-discovered `GridSite`.
///
/// Mirrors the logic in `operator::controller::grid_network::discovered_site_k8s_name`.
/// Both must stay in sync: the operator uses this to name the resource; the xtask uses
/// it to look up and verify the created resource.
pub(crate) fn auto_discovered_gridsite_name(network_name: &str, site_id: &str) -> String {
    let sanitise = |s: &str| -> String {
        let raw: String = s
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect();
        raw.trim_matches('-').to_owned()
    };
    let net = sanitise(network_name);
    let site = sanitise(site_id);
    let candidate = match (net.is_empty(), site.is_empty()) {
        (false, false) => format!("{net}-{site}"),
        (false, true) => net,
        (true, false) => site,
        (true, true) => "discovered-site".to_owned(),
    };
    candidate.chars().take(253).collect()
}

/// Poll until a `GridSite` named `site_name` exists and has `spec.gridNetworkRef = expected_network`.
///
/// Used by the auto-discovery validation to confirm that the primary operator created a
/// `GridSite` for a remote SWIM Alive member without any harness-assisted `kubectl apply`.
#[expect(
    clippy::too_many_lines,
    reason = "polling loop with per-iteration kubectl + deadline + sleep"
)]
pub(crate) fn wait_for_auto_gridsite(
    context: &str,
    site_name: &str,
    expected_network: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        let out = Command::new("kubectl")
            .args([
                "--context",
                context,
                "get",
                "gridsites",
                site_name,
                "-o",
                "jsonpath={.spec.gridNetworkRef}",
                "--ignore-not-found",
            ])
            .output()
            .unwrap_or_else(|_| std::process::abort());
        let network = String::from_utf8_lossy(&out.stdout).trim().to_owned();
        if network == expected_network {
            eprintln!(
                "  [OK] GridSite {site_name:?} auto-created by operator \
                 (gridNetworkRef={expected_network:?})"
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timeout waiting for auto-discovered GridSite {site_name:?} \
                 in network {expected_network:?}; last observed network: {network:?}"
            )
            .into());
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "polling wait for operator-auto-created GridSite"
        )]
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// Verify spec and status fields of an auto-discovered `GridSite`.
///
/// Checks `spec.gridNetworkRef`, `spec.egress.address`, and `status.phase`.
/// The expected phase is supplied by the caller; use [`wait_for_gridsite_phase`]
/// before calling this to ensure the operator has had time to advance the phase.
///
/// For auto-discovered sites with a non-empty advertised gateway address, the `GridSite`
/// controller advances the phase from `Discovered` → `Connecting` automatically,
/// so `expected_phase` should be `"Connecting"` in E2E tests that wait long enough
/// for the second reconcile.
#[expect(
    clippy::too_many_lines,
    reason = "kubectl fetch + three field validations + diagnostic messages"
)]
pub(crate) fn verify_auto_gridsite_fields(
    context: &str,
    site_name: &str,
    expected_network: &str,
    expected_phase: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "get",
            "gridsites",
            site_name,
            "-o",
            "jsonpath={.spec.gridNetworkRef}/{.spec.egress.address}/{.status.phase}",
        ])
        .output()?;
    if !out.status.success() {
        return Err(format!(
            "kubectl get gridsites/{site_name} for auto-discovery fields failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    let raw = String::from_utf8_lossy(&out.stdout);
    let fields: Vec<&str> = raw.trim().splitn(3, '/').collect();
    let network = fields.first().copied().unwrap_or("");
    let egress = fields.get(1).copied().unwrap_or("");
    let phase = fields.get(2).copied().unwrap_or("");

    if network != expected_network {
        return Err(format!(
            "auto-discovered GridSite {site_name:?}: gridNetworkRef={network:?} \
             (expected {expected_network:?})"
        )
        .into());
    }
    if egress.is_empty() {
        return Err(format!(
            "auto-discovered GridSite {site_name:?}: spec.egress.address is empty; \
             expected the advertised gateway address"
        )
        .into());
    }
    if phase != expected_phase {
        return Err(format!(
            "auto-discovered GridSite {site_name:?}: status.phase={phase:?} \
             (expected {expected_phase:?})"
        )
        .into());
    }
    eprintln!(
        "  [PASS] auto-discovered GridSite {site_name:?}: \
         gridNetworkRef={network:?}, egress={egress:?}, phase={phase:?}"
    );
    Ok(())
}

/// Assert that a `GridSite`'s `spec.egress.address` equals the expected gateway address
/// and is distinct from the SWIM UDP bind address.
///
/// Hard-fails if the egress address equals the SWIM UDP address — that would indicate
/// the gateway address was not propagated through the SWIM state broadcast and the site
/// received the SWIM endpoint instead of the data-plane gateway address.
#[expect(
    clippy::too_many_lines,
    reason = "sequential kubectl fetch + three defensive checks + diagnostic eprintln; splitting would obscure the invariant"
)]
pub(crate) fn verify_auto_gridsite_egress(
    context: &str,
    site_name: &str,
    expected_gateway_addr: &str,
    swim_udp_addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "get",
            "gridsites",
            site_name,
            "-o",
            "jsonpath={.spec.egress.address}",
        ])
        .output()?;
    if !out.status.success() {
        return Err(format!(
            "kubectl get gridsites/{site_name} egress address failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    let actual = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    if actual.is_empty() {
        return Err(format!(
            "GridSite {site_name:?}: spec.egress.address is empty; \
             expected gateway address {expected_gateway_addr:?}"
        )
        .into());
    }
    if actual == swim_udp_addr {
        return Err(format!(
            "GridSite {site_name:?}: spec.egress.address={actual:?} equals the SWIM UDP address; \
             expected gateway address {expected_gateway_addr:?} — \
             the data-plane gateway address was not propagated through SWIM"
        )
        .into());
    }
    if actual != expected_gateway_addr {
        return Err(format!(
            "GridSite {site_name:?}: spec.egress.address={actual:?}; \
             expected {expected_gateway_addr:?}"
        )
        .into());
    }
    eprintln!(
        "  [PASS] GridSite {site_name:?}: egress.address={actual:?} \
         matches gateway address (distinct from SWIM UDP {swim_udp_addr:?})"
    );
    Ok(())
}

/// Delete all resources created by the site-join-discovery validation on `context`.
///
/// Safe to call before a fresh run — all deletes use `--ignore-not-found`.
pub(crate) fn cleanup_site_join_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_namespaced_resource(
        context,
        "default",
        "configmap",
        &format!("grid-overlay-{SITE_JOIN_NETWORK}-{SITE_JOIN_GW}"),
    )?;
    delete_cluster_resource(context, "inferenceprovider", SITE_JOIN_PRIMARY_PROVIDER)?;
    delete_cluster_resource(context, "inferenceprovider", SITE_JOIN_JOINING_PROVIDER)?;
    delete_cluster_resource(context, "inferenceprovider", SITE_JOIN_WRONG_PROVIDER)?;
    delete_cluster_resource(context, "gridsite", SITE_JOIN_PRIMARY_SITE)?;
    delete_cluster_resource(context, "gridsite", SITE_JOIN_JOINING_SITE)?;
    delete_cluster_resource(context, "gridsite", SITE_JOIN_WRONG_SITE)?;
    // Delete any auto-discovered GridSites for this network so stale resources
    // from a previous run do not interfere with phase-transition timing.
    cleanup_auto_discovered_gridsites_for_network(context, SITE_JOIN_NETWORK);
    delete_cluster_resource(context, "gridnetwork", SITE_JOIN_NETWORK)?;
    delete_cluster_resource(context, "gridnetwork", SITE_JOIN_WRONG_NETWORK)?;
    eprintln!("  [OK] stale site-join-discovery resources removed from {context}");
    Ok(())
}

/// Delete a `GridSite` that was auto-discovered and created by the operator.
///
/// Wraps `delete_cluster_resource` with `--ignore-not-found` so cleanup is
/// idempotent even when the site was never created (e.g. SWIM did not converge).
pub(crate) fn cleanup_auto_discovered_gridsite(
    context: &str,
    site_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    delete_cluster_resource(context, "gridsite", site_name)?;
    eprintln!("  [OK] auto-discovered GridSite {site_name:?} removed from {context}");
    Ok(())
}

/// List `GridSite` names on `context` whose `spec.gridNetworkRef` matches `network`.
///
/// Runs `kubectl get gridsites -o json` and delegates filtering to the pure
/// [`gridsites_in_network`] helper.  Returns an empty `Vec` if no sites exist
/// for the network, which the caller can use to assert isolation.
pub(crate) fn list_gridsites_for_network(
    context: &str,
    network: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let out = Command::new("kubectl")
        .args(["--context", context, "get", "gridsites", "-o", "json"])
        .output()?;
    if !out.status.success() {
        return Err(format!("kubectl get gridsites failed: {}", String::from_utf8_lossy(&out.stderr)).into());
    }
    let all: serde_json::Value = serde_json::from_slice(&out.stdout)?;
    let items = all
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or("gridsites list missing items array")?;
    let names = gridsites_in_network(items, network)
        .into_iter()
        .map(String::from)
        .collect();
    Ok(names)
}

/// Filter site names from a JSON list of `GridSite` objects by `network`.
///
/// Used in the cross-network isolation check: given all `GridSite` objects
/// on a cluster, returns only the names whose `spec.gridNetworkRef` matches
/// `network`.
///
/// This is a pure function, suitable for unit testing without kubectl.
pub(crate) fn gridsites_in_network<'a>(sites: &'a [serde_json::Value], network: &str) -> Vec<&'a str> {
    sites
        .iter()
        .filter_map(|s| {
            let site_network = s.pointer("/spec/gridNetworkRef").and_then(serde_json::Value::as_str)?;
            if site_network == network {
                s.pointer("/metadata/name").and_then(serde_json::Value::as_str)
            } else {
                None
            }
        })
        .collect()
}

/// Return the ordinal position of a `GridSite` lifecycle phase string.
///
/// Lower values are earlier in the join lifecycle:
/// `Pending(0) -> Discovered(1) -> Connecting(2) -> Active(3) -> Unreachable(4) -> Left(5)`.
///
/// Returns `None` for unrecognised phase strings, enabling assertion helpers
/// to reject unknown values rather than accepting them silently.
///
/// This is a pure function, suitable for unit testing without kubectl.
pub(crate) fn gridsite_phase_index(phase: &str) -> Option<usize> {
    const PHASES: &[&str] = &["Pending", "Discovered", "Connecting", "Active", "Unreachable", "Left"];
    PHASES.iter().position(|p| *p == phase)
}

// ---------------------------------------------------------------------------
// Failover / lost-peer helpers
// ---------------------------------------------------------------------------

/// Apply east-cluster fixtures for the failover validation.
///
/// Creates `GridNetwork` [`FAILOVER_NETWORK`] with a `gatewayRef` pointing at
/// [`FAILOVER_GW`] (the overlay is generated on the east/primary cluster) and
/// `InferenceProvider` [`FAILOVER_EAST_PROVIDER`] with `backendKind: "local"`.
#[expect(clippy::too_many_lines, reason = "two JSON manifests with full K8s structure")]
pub(crate) fn apply_failover_east_fixtures(
    context: &str,
    east_site: &str,
    model: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let network = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": FAILOVER_NETWORK },
        "spec": {
            "seeds": [],
            "gatewayRefs": [{
                "name": FAILOVER_GW,
                "namespace": "default",
                "localSiteName": east_site
            }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("failover east GridNetwork serialization failed: {e}");
        std::process::exit(1);
    });
    let provider = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": FAILOVER_EAST_PROVIDER },
        "spec": {
            "gridNetworkRef": FAILOVER_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [{ "name": model }],
            "routingClusterRef": east_site
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("failover east provider serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &network)?;
    kubectl::apply_manifest(context, &provider)?;
    eprintln!(
        "  [OK] failover east fixtures applied \
         ({FAILOVER_EAST_PROVIDER}, model={model:?}, routingClusterRef={east_site:?})"
    );
    Ok(())
}

/// Poll the overlay until the candidate for `remote_cluster` has `fresh=false`.
///
/// This proves that the grid-network controller's `apply_swim_staleness_override` fired
/// during the operator reconcile triggered after the remote SWIM member was declared Dead.
pub(crate) fn wait_for_remote_candidate_stale(
    context: &str,
    network: &str,
    gw: &str,
    remote_cluster: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(overlay) = read_overlay_configmap(context, network, gw, "default") {
            let stale = overlay
                .get("candidates")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|cands| {
                    cands.iter().any(|c| {
                        c.get("cluster").and_then(serde_json::Value::as_str) == Some(remote_cluster)
                            && c.get("fresh").and_then(serde_json::Value::as_bool) == Some(false)
                    })
                });
            if stale {
                eprintln!("  [OK] overlay: remote candidate {remote_cluster:?} is now fresh=false");
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(
                format!("timeout waiting for remote candidate {remote_cluster:?} to become fresh=false").into(),
            );
        }
        #[expect(clippy::disallowed_methods, reason = "polling wait between overlay reads")]
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// Poll the overlay `ConfigMap` until a remote candidate has `fresh=true`.
///
/// Called during rejoin recovery to confirm that the east overlay reflects the
/// recovered west SWIM peer.  The condition is met once SWIM marks the rejoined
/// peer `Alive` and the east operator re-renders the overlay.
///
/// Bumps the `GridNetwork` every `RECOVERY_BUMP_INTERVAL` seconds so that as
/// soon as SWIM fires a `Joined` event and updates the membership snapshot, the
/// operator performs a fresh reconcile and the overlay reflects `fresh=true`.
/// A single pre-poll bump is insufficient because the `Joined` event may arrive
/// after the first reconcile completes (the bump and the SWIM Alive detection are
/// asynchronous with respect to each other).
///
/// # Errors
///
/// Returns an error when `timeout` elapses without `fresh=true` for the candidate.
#[expect(
    clippy::too_many_lines,
    reason = "polling loop with periodic bump + overlay read + deadline check; splitting would hide the recovery protocol"
)]
pub(crate) fn wait_for_remote_candidate_fresh(
    context: &str,
    network: &str,
    gw: &str,
    remote_cluster: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    const RECOVERY_BUMP_INTERVAL: Duration = Duration::from_secs(5);
    let deadline = Instant::now() + timeout;
    let mut last_bump = Instant::now() - RECOVERY_BUMP_INTERVAL; // trigger first bump immediately
    loop {
        // Periodic bump: ensure the operator reconciles after each SWIM membership
        // update so the overlay reflects the latest fresh=true state.
        if last_bump.elapsed() >= RECOVERY_BUMP_INTERVAL {
            if let Err(e) = bump_gridnetwork(context, network) {
                eprintln!("  [warn] recovery bump failed (will retry): {e}");
            }
            last_bump = Instant::now();
        }

        if let Ok(overlay) = read_overlay_configmap(context, network, gw, "default") {
            let fresh = overlay
                .get("candidates")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|cands| {
                    cands.iter().any(|c| {
                        c.get("cluster").and_then(serde_json::Value::as_str) == Some(remote_cluster)
                            && c.get("fresh").and_then(serde_json::Value::as_bool) == Some(true)
                    })
                });
            if fresh {
                eprintln!("  [OK] overlay: remote candidate {remote_cluster:?} recovered to fresh=true");
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(
                format!("timeout waiting for remote candidate {remote_cluster:?} to recover to fresh=true").into(),
            );
        }
        #[expect(clippy::disallowed_methods, reason = "polling wait between overlay reads")]
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// Verify the failover overlay state: local candidate fresh, remote candidate reflects expected freshness.
///
/// When `expect_remote_stale = true`, uses [`verify_degraded_candidate`] to assert `fresh=false`.
/// When `expect_remote_stale = false`, asserts the remote candidate is present with `fresh=true`.
#[expect(clippy::too_many_lines, reason = "two candidate assertions with diagnostic messages")]
#[expect(
    clippy::too_many_arguments,
    reason = "local and remote each need cluster + model + stale flag: 8 params total"
)]
pub(crate) fn verify_failover_overlay(
    context: &str,
    network: &str,
    gw: &str,
    local_cluster: &str,
    local_model: &str,
    remote_cluster: &str,
    remote_model: &str,
    expect_remote_stale: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let overlay = read_overlay_configmap(context, network, gw, "default")?;
    let candidates = overlay
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("overlay missing candidates array")?;

    // Local candidate must be present and fresh.
    let local_ok = candidates.iter().any(|c| {
        c.get("cluster").and_then(serde_json::Value::as_str) == Some(local_cluster)
            && c.get("name").and_then(serde_json::Value::as_str) == Some(local_model)
            && c.get("fresh").and_then(serde_json::Value::as_bool) == Some(true)
    });
    if !local_ok {
        return Err(
            format!("local candidate {local_cluster:?}/{local_model:?} not found or not fresh in overlay").into(),
        );
    }
    eprintln!("  [PASS] overlay: local candidate {local_cluster:?} fresh=true");

    if expect_remote_stale {
        verify_degraded_candidate(&overlay, remote_cluster)?;
        eprintln!("  [PASS] overlay: remote candidate {remote_cluster:?} fresh=false (stale after partition)");
    } else {
        let remote_ok = candidates.iter().any(|c| {
            c.get("cluster").and_then(serde_json::Value::as_str) == Some(remote_cluster)
                && c.get("name").and_then(serde_json::Value::as_str) == Some(remote_model)
                && c.get("fresh").and_then(serde_json::Value::as_bool) == Some(true)
        });
        if !remote_ok {
            return Err(format!(
                "remote candidate {remote_cluster:?}/{remote_model:?} not found or not fresh in overlay"
            )
            .into());
        }
        eprintln!("  [PASS] overlay: remote candidate {remote_cluster:?} fresh=true (before partition)");
    }
    Ok(())
}

/// Apply the east healthy-fallback `InferenceProvider` for the shared model.
///
/// This provider serves [`FAILOVER_SHARED_MODEL`] from the east cluster as a
/// `backendKind: "local"` provider.  It is the healthy alternative that should
/// rank first among shared-model candidates after west is lost, proving request-level route-away.
pub(crate) fn apply_failover_shared_east_provider(
    context: &str,
    east_site: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": FAILOVER_SHARED_EAST_PROVIDER },
        "spec": {
            "gridNetworkRef": FAILOVER_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [{ "name": FAILOVER_SHARED_MODEL }],
            "routingClusterRef": east_site
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("failover shared-east provider serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &manifest)?;
    eprintln!(
        "  [OK] {FAILOVER_SHARED_EAST_PROVIDER:?} applied \
         (backendKind=local, model={FAILOVER_SHARED_MODEL:?}, routingClusterRef={east_site:?})"
    );
    Ok(())
}

/// Apply west fixtures that include both the dedicated remote model and the shared model.
///
/// The west CRDT provider publishes both [`FAILOVER_REMOTE_MODEL`] and
/// [`FAILOVER_SHARED_MODEL`], allowing the east overlay to contain a remote stale
/// candidate for the shared model after west is lost.
#[expect(clippy::too_many_lines, reason = "two JSON manifests with full K8s structure")]
pub(crate) fn apply_failover_west_fixtures_with_shared(
    context: &str,
    west_site: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let network = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": FAILOVER_NETWORK },
        "spec": { "seeds": [] }
    }))
    .unwrap_or_else(|e| {
        eprintln!("failover west GridNetwork (with-shared) serialization failed: {e}");
        std::process::exit(1);
    });
    let provider = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": FAILOVER_WEST_PROVIDER },
        "spec": {
            "gridNetworkRef": FAILOVER_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "remote",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [
                { "name": FAILOVER_REMOTE_MODEL },
                { "name": FAILOVER_SHARED_MODEL }
            ],
            "routingClusterRef": west_site
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("failover west provider (with-shared) serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &network)?;
    kubectl::apply_manifest(context, &provider)?;
    eprintln!(
        "  [OK] failover west fixtures (with-shared) applied \
         ({FAILOVER_WEST_PROVIDER}, models=[{FAILOVER_REMOTE_MODEL:?}, {FAILOVER_SHARED_MODEL:?}], \
         routingClusterRef={west_site:?})"
    );
    Ok(())
}

/// Assert the shared-model overlay ordering: local (east, fresh=true) before remote (west, stale).
///
/// When `expect_west_stale = false` (before partition): both candidates fresh=true, east first.
/// When `expect_west_stale = true` (after partition): west is fresh=false, east still first.
///
/// Attribution is overlay-based: the same mock backend echoes the model name in both cases,
/// so response body cannot distinguish east from west.  The shared-model ordering proof
/// (east first among candidates for that model) is the stated evidence for routing preference.
#[expect(
    clippy::too_many_lines,
    reason = "two candidate position lookups with diagnostic messages"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "six distinct check parameters: context, network, gw, two clusters, stale flag"
)]
pub(crate) fn verify_shared_model_overlay_ordering(
    context: &str,
    network: &str,
    gw: &str,
    east_cluster: &str,
    west_cluster: &str,
    expect_west_stale: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let overlay = read_overlay_configmap(context, network, gw, "default")?;
    let candidates = overlay
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("overlay missing candidates array")?;

    // Find east and west positions for the shared model.
    let east_pos = candidates
        .iter()
        .position(|c| {
            c.get("cluster").and_then(serde_json::Value::as_str) == Some(east_cluster)
                && c.get("name").and_then(serde_json::Value::as_str) == Some(FAILOVER_SHARED_MODEL)
        })
        .ok_or_else(|| format!("east shared-model candidate ({east_cluster:?}) not found in overlay"))?;

    let west_pos = candidates
        .iter()
        .position(|c| {
            c.get("cluster").and_then(serde_json::Value::as_str) == Some(west_cluster)
                && c.get("name").and_then(serde_json::Value::as_str) == Some(FAILOVER_SHARED_MODEL)
        })
        .ok_or_else(|| format!("west shared-model candidate ({west_cluster:?}) not found in overlay"))?;

    if east_pos >= west_pos {
        return Err(format!(
            "shared model overlay ordering wrong: east ({east_cluster:?}, pos {east_pos}) \
             must appear before west ({west_cluster:?}, pos {west_pos})"
        )
        .into());
    }

    let east_fresh = candidates
        .get(east_pos)
        .and_then(|c| c.get("fresh"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let west_fresh = candidates
        .get(west_pos)
        .and_then(|c| c.get("fresh"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);

    if !east_fresh {
        return Err(
            format!("east candidate ({east_cluster:?}) for {FAILOVER_SHARED_MODEL:?} must be fresh=true").into(),
        );
    }
    if expect_west_stale && west_fresh {
        return Err(format!(
            "west candidate ({west_cluster:?}) for {FAILOVER_SHARED_MODEL:?} must be fresh=false \
             after partition"
        )
        .into());
    }

    let stale_tag = if expect_west_stale {
        "fresh=false (stale)"
    } else {
        "fresh=true"
    };
    eprintln!(
        "  [PASS] shared model {FAILOVER_SHARED_MODEL:?}: east ({east_cluster:?}) \
         pos={east_pos} fresh=true → west ({west_cluster:?}) pos={west_pos} {stale_tag}"
    );
    Ok(())
}

/// Delete all east-cluster resources created by the failover validation.
pub(crate) fn cleanup_failover_east_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_namespaced_resource(
        context,
        "default",
        "configmap",
        &format!("grid-overlay-{FAILOVER_NETWORK}-{FAILOVER_GW}"),
    )?;
    delete_cluster_resource(context, "inferenceprovider", FAILOVER_EAST_PROVIDER)?;
    delete_cluster_resource(context, "inferenceprovider", FAILOVER_SHARED_EAST_PROVIDER)?;
    delete_cluster_resource(context, "gridnetwork", FAILOVER_NETWORK)?;
    eprintln!("  [OK] stale failover east resources removed from {context}");
    Ok(())
}

/// Delete all west-cluster resources created by the failover validation.
pub(crate) fn cleanup_failover_west_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_cluster_resource(context, "inferenceprovider", FAILOVER_WEST_PROVIDER)?;
    delete_cluster_resource(context, "gridnetwork", FAILOVER_NETWORK)?;
    eprintln!("  [OK] stale failover west resources removed from {context}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Stale-candidate TTL GC validation helpers
// ---------------------------------------------------------------------------

/// Apply east fixtures for the stale-GC validation.
///
/// Creates [`STALE_GC_NETWORK`] with `spec.staleCandidateTtlSeconds = ttl_secs`
/// and [`STALE_GC_EAST_PROVIDER`] (local, `backendKind=local`).
#[expect(clippy::too_many_lines, reason = "two JSON manifests with full K8s structure")]
pub(crate) fn apply_stale_gc_east_fixtures(
    context: &str,
    east_site: &str,
    ttl_secs: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let network = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": STALE_GC_NETWORK },
        "spec": {
            "seeds": [],
            "staleCandidateTtlSeconds": ttl_secs,
            "gatewayRefs": [{
                "name": STALE_GC_GW,
                "namespace": "default",
                "localSiteName": east_site
            }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("stale-GC east GridNetwork serialization failed: {e}");
        std::process::exit(1);
    });
    let provider = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": STALE_GC_EAST_PROVIDER },
        "spec": {
            "gridNetworkRef": STALE_GC_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [{ "name": STALE_GC_LOCAL_MODEL }],
            "routingClusterRef": east_site
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("stale-GC east provider serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &network)?;
    kubectl::apply_manifest(context, &provider)?;
    eprintln!(
        "  [OK] stale-GC east fixtures applied \
         ({STALE_GC_EAST_PROVIDER}, staleCandidateTtlSeconds={ttl_secs})"
    );
    Ok(())
}

/// Apply west fixtures for the stale-GC validation.
///
/// Creates [`STALE_GC_NETWORK`] on the west cluster and
/// [`STALE_GC_WEST_PROVIDER`] (remote, `backendKind=remote`).
#[expect(clippy::too_many_lines, reason = "two JSON manifests with full K8s structure")]
pub(crate) fn apply_stale_gc_west_fixtures(context: &str, west_site: &str) -> Result<(), Box<dyn std::error::Error>> {
    let network = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": STALE_GC_NETWORK },
        "spec": { "seeds": [] }
    }))
    .unwrap_or_else(|e| {
        eprintln!("stale-GC west GridNetwork serialization failed: {e}");
        std::process::exit(1);
    });
    let provider = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": STALE_GC_WEST_PROVIDER },
        "spec": {
            "gridNetworkRef": STALE_GC_NETWORK,
            "providerKind": "self_hosted",
            "backendKind": "remote",
            "endpoint": "http://mock-openai-provider.default.svc:8080",
            "models": [{ "name": STALE_GC_REMOTE_MODEL }],
            "routingClusterRef": west_site
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("stale-GC west provider serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(context, &network)?;
    kubectl::apply_manifest(context, &provider)?;
    eprintln!(
        "  [OK] stale-GC west fixtures applied \
         ({STALE_GC_WEST_PROVIDER}, routingClusterRef={west_site:?})"
    );
    Ok(())
}

/// Poll the overlay until the candidate for `remote_cluster` is **absent**.
///
/// Called after the west operator is killed and the TTL-based stale-GC policy
/// is expected to evict the stale remote candidate.  Unlike
/// [`wait_for_remote_candidate_stale`] (which asserts `fresh=false`), this
/// asserts the candidate is completely gone from the overlay, proving that
/// `apply_stale_gc_filter` removed it.
///
/// Bumps the `GridNetwork` every 5 s so a fresh reconcile runs after the member
/// age crosses the TTL.
///
/// # Determinism
///
/// Age advances via the 1-second runtime tick independently of SWIM events.
/// After `SWIM_DEAD_MEMBER_WAIT` (20 s) the west member's `age_secs` is
/// typically ≥ 14 s (SWIM declares Dead within ~6 s of the kill).  With
/// `STALE_GC_TTL_SECS = 5`, this comfortably exceeds the TTL.  The bump loop
/// ensures a reconcile fires after each age-tick window.
///
/// `GridNetwork`: the `operator` crate's `GridNetwork` CRD type
#[expect(
    clippy::too_many_lines,
    reason = "periodic bump + overlay read + deadline check; splitting would obscure the GC polling protocol"
)]
pub(crate) fn wait_for_remote_candidate_absent(
    context: &str,
    network: &str,
    gw: &str,
    remote_cluster: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    const BUMP_INTERVAL: Duration = Duration::from_secs(5);
    let deadline = Instant::now() + timeout;
    let mut last_bump = Instant::now() - BUMP_INTERVAL; // trigger first bump immediately
    loop {
        // Bump periodically so a reconcile fires after each age-tick window.
        if last_bump.elapsed() >= BUMP_INTERVAL {
            if let Err(e) = bump_gridnetwork(context, network) {
                eprintln!("  [warn] GC absence bump failed (will retry): {e}");
            }
            last_bump = Instant::now();
        }
        if let Ok(overlay) = read_overlay_configmap(context, network, gw, "default") {
            let still_present = overlay
                .get("candidates")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|cands| {
                    cands
                        .iter()
                        .any(|c| c.get("cluster").and_then(serde_json::Value::as_str) == Some(remote_cluster))
                });
            if !still_present {
                eprintln!(
                    "  [OK] overlay: remote candidate {remote_cluster:?} is absent \
                     (evicted by TTL-based stale GC)"
                );
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timeout waiting for remote candidate {remote_cluster:?} to be absent \
                 (stale-GC TTL should have evicted it)"
            )
            .into());
        }
        #[expect(clippy::disallowed_methods, reason = "polling wait between overlay reads")]
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// Delete all resources created by the stale-GC validation on east cluster.
pub(crate) fn cleanup_stale_gc_east_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_namespaced_resource(
        context,
        "default",
        "configmap",
        &format!("grid-overlay-{STALE_GC_NETWORK}-{STALE_GC_GW}"),
    )?;
    delete_cluster_resource(context, "inferenceprovider", STALE_GC_EAST_PROVIDER)?;
    delete_cluster_resource(context, "gridnetwork", STALE_GC_NETWORK)?;
    cleanup_auto_discovered_gridsites_for_network(context, STALE_GC_NETWORK);
    eprintln!("  [OK] stale-GC east resources removed");
    Ok(())
}

/// Delete all resources created by the stale-GC validation on west cluster.
pub(crate) fn cleanup_stale_gc_west_resources(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_cluster_resource(context, "inferenceprovider", STALE_GC_WEST_PROVIDER)?;
    delete_cluster_resource(context, "gridnetwork", STALE_GC_NETWORK)?;
    cleanup_auto_discovered_gridsites_for_network(context, STALE_GC_NETWORK);
    eprintln!("  [OK] stale-GC west resources removed");
    Ok(())
}

// ---------------------------------------------------------------------------
// API-provider credential helpers
// ---------------------------------------------------------------------------

/// Parsed credential plan derived from `InferenceProvider.spec.auth` JSON.
///
/// A pure data type used by the xtask credential path; no kubectl calls.
/// Produced by `parse_api_credential_plan` (test-only) or by
/// [`api_credential_plan_from_overlay`] (production path).
#[derive(Debug, PartialEq)]
pub(crate) enum ApiCredentialPlan {
    /// `auth.manual = true` — the user manages credentials; the harness does not inject.
    ///
    /// Constructed by `parse_api_credential_plan` (test-only); kept as a
    /// valid arm in [`resolve_api_credential`] for defensive completeness.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "constructed only by test-only parse_api_credential_plan")
    )]
    Manual,
    /// `spec.auth` is absent — no credential injection.
    ///
    /// Constructed by `parse_api_credential_plan` (test-only); kept as a
    /// valid arm in [`resolve_api_credential`] for defensive completeness.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "constructed only by test-only parse_api_credential_plan")
    )]
    Absent,
    /// `auth.strategy = bearer_token` with a resolved `SecretRef`.
    BearerToken {
        /// Secret name in the cluster.
        secret_name: String,
        /// Secret namespace.
        namespace: String,
        /// Key within `Secret.data` that holds the token.
        key: String,
    },
}

/// Parse `InferenceProvider.spec.auth` from its raw JSON representation.
///
/// This is a **pure function**: it does not call kubectl or read any Kubernetes
/// resources.  Use it in tests and as the first step of the credential-projection
/// path before calling [`read_api_credential`].
///
/// # Rules
///
/// | `manual` | `strategy` | `secret_ref` | Result |
/// |---|---|---|---|
/// | `true` | any | any | `Ok(Manual)` |
/// | absent/null | — | — | `Ok(Absent)` |
/// | `false` | `bearer_token` | present | `Ok(BearerToken { … })` |
/// | `false` | `bearer_token` | absent | `Err("missing secretRef")` |
/// | `false` | other | any | `Err("unsupported strategy …")` |
#[cfg(test)]
#[expect(
    clippy::too_many_lines,
    reason = "match table with diagnostic messages for each auth variant"
)]
pub(crate) fn parse_api_credential_plan(
    auth_json: &serde_json::Value,
) -> Result<ApiCredentialPlan, Box<dyn std::error::Error>> {
    if auth_json.is_null() {
        return Ok(ApiCredentialPlan::Absent);
    }

    if auth_json.get("manual").and_then(serde_json::Value::as_bool) == Some(true) {
        return Ok(ApiCredentialPlan::Manual);
    }

    let strategy = auth_json
        .get("strategy")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");

    match strategy {
        "bearer_token" => {
            let secret_ref = auth_json
                .get("secretRef")
                .ok_or("auth.strategy is bearer_token but spec.auth.secretRef is missing")?;
            let name = secret_ref
                .get("name")
                .and_then(serde_json::Value::as_str)
                .ok_or("spec.auth.secretRef.name is missing or not a string")?;
            let namespace = secret_ref
                .get("namespace")
                .and_then(serde_json::Value::as_str)
                .ok_or("spec.auth.secretRef.namespace is missing or not a string")?;
            let key = secret_ref
                .get("key")
                .and_then(serde_json::Value::as_str)
                .ok_or("spec.auth.secretRef.key is missing or not a string")?;
            Ok(ApiCredentialPlan::BearerToken {
                secret_name: name.to_owned(),
                namespace: namespace.to_owned(),
                key: key.to_owned(),
            })
        },
        other => Err(format!(
            "unsupported auth strategy {other:?}: only bearer_token is supported \
             for harness-driven credential projection"
        )
        .into()),
    }
}

/// Create a Kubernetes Secret containing an API-provider bearer token.
///
/// Uses `stringData` so the Kubernetes API server handles base64 encoding,
/// avoiding both the `base64` subprocess dependency and GNU base64's default
/// 76-char line-wrapping (which would corrupt tokens longer than 57 bytes if
/// the `data` field were used with an unwrapped subprocess).
///
/// The manifest is piped via stdin to `kubectl apply` and is **not logged** —
/// only the Secret name and key appear in xtask output.
pub(crate) fn create_api_credential_secret(
    context: &str,
    name: &str,
    namespace: &str,
    key: &str,
    token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // stringData: Kubernetes stores the plain-text value as base64 in .data.
    // The manifest string is kept in-process and piped via stdin; it is never
    // written to disk or logged.
    let manifest = [
        "apiVersion: v1",
        "kind: Secret",
        "metadata:",
        &format!("  name: {name}"),
        &format!("  namespace: {namespace}"),
        "type: Opaque",
        "stringData:",
        &format!("  {key}: {token}"),
        "",
    ]
    .join("\n");
    kubectl::apply_manifest(context, &manifest)?;
    eprintln!(
        "  [OK] credential Secret {name:?} created in {namespace:?} \
         (key={key:?}, token not logged)"
    );
    Ok(())
}

/// Read a bearer token from a Kubernetes Secret.
///
/// Fetches the Secret as JSON and base64-decodes `Secret.data[key]`.
/// Kubernetes stores `Secret.data` values as standard base64; they are
/// decoded here using the system `base64 -d` command so no extra crate is needed.
///
/// The decoded token value is returned but **never logged** — callers
/// must not print the return value.
#[expect(clippy::too_many_lines, reason = "kubectl fetch + JSON parse + base64 decode chain")]
pub(crate) fn read_api_credential(
    context: &str,
    secret_name: &str,
    namespace: &str,
    key: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    use std::io::Write as _;
    // Fetch the whole Secret as JSON so the key lookup is not subject to
    // jsonpath escaping rules for keys that contain dots or slashes.
    let out = Command::new("kubectl")
        .args([
            "--context",
            context,
            "get",
            "secret",
            secret_name,
            "-n",
            namespace,
            "-o",
            "json",
        ])
        .output()?;
    if !out.status.success() {
        return Err(format!(
            "kubectl get secret {secret_name:?} in {namespace:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    let json: serde_json::Value = serde_json::from_slice(&out.stdout)?;
    let b64 = json
        .get("data")
        .and_then(|d| d.get(key))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("Secret {secret_name:?} in {namespace:?} has no key {key:?}"))?;

    // Kubernetes encodes Secret.data values as standard (non-URL-safe) base64.
    let mut child = Command::new("base64")
        .arg("-d")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(b64.as_bytes())?;
    }
    let decoded = child.wait_with_output()?;
    if !decoded.status.success() {
        return Err(format!("base64 decode of Secret {secret_name:?}/{key:?} failed").into());
    }
    let token = String::from_utf8(decoded.stdout)?;

    eprintln!("  [OK] credential read from Secret {secret_name:?} key={key:?} (token not logged)");
    Ok(token)
}

/// Resolve a parsed credential plan to a bearer token string.
///
/// This is the **v1 credential backend boundary**.  New credential sources
/// (External Secrets, Vault, workload identity, `OAuth2` refresh, `SigV4` signing)
/// add a new [`ApiCredentialPlan`] variant and a new match arm here; callers
/// and the rest of the harness do not need to change.
///
/// | Plan | Return |
/// |---|---|
/// | `Absent` | `Ok(None)` — no injection |
/// | `Manual` | `Ok(None)` — caller manages credentials |
/// | `BearerToken { … }` | `Ok(Some(token))` — read from k8s Secret |
///
/// The returned token is never logged; only the Secret name and key are printed.
pub(crate) fn resolve_api_credential(
    context: &str,
    plan: &ApiCredentialPlan,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    match plan {
        ApiCredentialPlan::Absent | ApiCredentialPlan::Manual => Ok(None),
        ApiCredentialPlan::BearerToken {
            secret_name,
            namespace,
            key,
        } => {
            let token = read_api_credential(context, secret_name, namespace, key)?;
            Ok(Some(token))
        },
    }
}

/// Extract a bearer-token [`ApiCredentialPlan`] for a cluster from an operator-produced overlay.
///
/// Scans the overlay candidates for a credential reference with
/// `strategy = "bearer_token"` and matching `cluster`, then converts it
/// to an [`ApiCredentialPlan`].
///
/// This is the **xtask harness bridge**: instead of reading `spec.auth` directly
/// from the `InferenceProvider` resource, the harness reads what the operator
/// already projected into the overlay `ConfigMap`.  When Praxis supports native
/// Secret-ref consumption, the data-plane will read the same `credential` field.
///
/// Returns `None` if no candidate for `cluster` carries a bearer-token credential.
pub(crate) fn api_credential_plan_from_overlay(
    overlay: &crate::env::operator_overlay::RoutingOverlay,
    cluster: &str,
) -> Option<ApiCredentialPlan> {
    overlay.candidates.iter().find_map(|c| {
        if c.cluster != cluster {
            return None;
        }
        let cred = c.credential.as_ref()?;
        if cred.strategy != "bearer_token" {
            return None;
        }
        Some(ApiCredentialPlan::BearerToken {
            secret_name: cred.secret_ref.name.clone(),
            namespace: cred.secret_ref.namespace.clone(),
            key: cred.secret_ref.key.clone(),
        })
    })
}

/// Delete the API-provider credential Secret (best-effort, used during cleanup).
pub(crate) fn delete_api_credential_secret(context: &str, namespace: &str) -> Result<(), Box<dyn std::error::Error>> {
    delete_namespaced_resource(context, namespace, "secret", API_PROVIDER_SECRET_NAME)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "tests"
)]
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

    // -----------------------------------------------------------------------
    // verify_distributed_state_received — exact count assertions
    // -----------------------------------------------------------------------

    #[test]
    fn verify_distributed_count_exactly_one_passes() {
        assert!(
            verify_distributed_state_received(1).is_ok(),
            "distributedProviderCount=1 must pass"
        );
    }

    #[test]
    fn verify_distributed_count_zero_fails() {
        let err = verify_distributed_state_received(0).unwrap_err();
        assert!(
            err.to_string().contains("received 0"),
            "error for count=0 must explain that no state was received; got: {err}"
        );
    }

    #[test]
    fn verify_distributed_count_greater_than_one_fails_with_leakage_message() {
        let err = verify_distributed_state_received(6).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("distributedProviderCount=6"),
            "error must include the observed count; got: {msg}"
        );
        assert!(
            msg.contains("cross-network state leakage"),
            "error must explain leakage risk; got: {msg}"
        );
    }

    #[test]
    fn verify_distributed_count_two_also_fails() {
        // Exactly 1 is the only correct count; 2 means unexpected extra records.
        assert!(
            verify_distributed_state_received(2).is_err(),
            "distributedProviderCount=2 must fail"
        );
    }

    // -----------------------------------------------------------------------
    // multi_provider_fixture_name
    // -----------------------------------------------------------------------

    #[test]
    fn multi_provider_fixture_name_prefixes_site() {
        assert_eq!(
            multi_provider_fixture_name("site-east"),
            "op-e2e-site-east",
            "fixture name must be op-e2e-{{site}}"
        );
        assert_eq!(
            multi_provider_fixture_name("site-west"),
            "op-e2e-site-west",
            "fixture name must be op-e2e-{{site}}"
        );
    }

    #[test]
    fn multi_provider_fixture_name_does_not_collide_with_single_provider_constants() {
        // The multi-provider name must not clash with any of the fixed single-provider fixture names.
        let single_provider_names = [
            TEST_PROVIDER_HEALTHY,
            TEST_PROVIDER_INVALID,
            TEST_PROVIDER_DEGRADED,
            TEST_PROVIDER_API,
        ];
        for &name in &single_provider_names {
            assert_ne!(
                multi_provider_fixture_name("site-a"),
                name,
                "op-e2e-site-a must not equal single-provider constant '{name}'"
            );
        }
    }

    // -----------------------------------------------------------------------
    // multi_provider_fixture_json
    // -----------------------------------------------------------------------

    #[test]
    fn multi_provider_fixture_json_includes_routing_cluster_ref() {
        let models = vec!["model-east".to_owned()];
        let json = multi_provider_fixture_json(
            "op-e2e-site-east",
            "op-e2e-net",
            "http://backend:8080",
            "site-east",
            &models,
        );
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            parsed
                .get("spec")
                .and_then(|s| s.get("routingClusterRef"))
                .and_then(serde_json::Value::as_str),
            Some("site-east"),
            "routingClusterRef must match site name"
        );
    }

    #[test]
    fn multi_provider_fixture_json_includes_all_models() {
        let models = vec!["model-east".to_owned(), "model-east-v2".to_owned()];
        let json = multi_provider_fixture_json(
            "op-e2e-site-east",
            "op-e2e-net",
            "http://backend:8080",
            "site-east",
            &models,
        );
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        let spec_models = parsed
            .get("spec")
            .and_then(|s| s.get("models"))
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(spec_models.len(), 2, "fixture must include all declared models");
        assert_eq!(
            spec_models
                .first()
                .and_then(|m| m.get("name"))
                .and_then(serde_json::Value::as_str),
            Some("model-east"),
            "first model must match"
        );
    }

    #[test]
    fn multi_provider_fixture_json_two_sites_produce_distinct_routing_clusters() {
        let models_east = vec!["model-east".to_owned()];
        let models_west = vec!["model-west".to_owned()];
        let json_east = multi_provider_fixture_json("op-e2e-site-east", "net", "http://ep", "site-east", &models_east);
        let json_west = multi_provider_fixture_json("op-e2e-site-west", "net", "http://ep", "site-west", &models_west);
        let east: serde_json::Value = serde_json::from_str(&json_east).unwrap_or_else(|_| std::process::abort());
        let west: serde_json::Value = serde_json::from_str(&json_west).unwrap_or_else(|_| std::process::abort());
        let east_ref = east
            .get("spec")
            .and_then(|s| s.get("routingClusterRef"))
            .and_then(serde_json::Value::as_str);
        let west_ref = west
            .get("spec")
            .and_then(|s| s.get("routingClusterRef"))
            .and_then(serde_json::Value::as_str);
        assert_ne!(
            east_ref, west_ref,
            "two provider sites must produce distinct routingClusterRef values"
        );
    }

    // -----------------------------------------------------------------------
    // verify_multi_provider_overlay
    // -----------------------------------------------------------------------

    fn make_overlay_with_sites(sites: &[(&str, bool)]) -> serde_json::Value {
        let candidates: Vec<serde_json::Value> = sites
            .iter()
            .map(|&(site, fresh)| {
                serde_json::json!({
                    "kind": "inference_model",
                    "name": "some-model",
                    "site": site,
                    "cluster": site,
                    "fresh": fresh
                })
            })
            .collect();
        serde_json::json!({ "network": "net", "local_site": "net", "candidates": candidates })
    }

    #[test]
    fn verify_multi_provider_overlay_accepts_all_sites_fresh() {
        let overlay = make_overlay_with_sites(&[("site-east", true), ("site-west", true)]);
        assert!(
            verify_multi_provider_overlay(&overlay, &["site-east", "site-west"]).is_ok(),
            "overlay with all sites fresh must pass"
        );
    }

    #[test]
    fn verify_multi_provider_overlay_rejects_missing_site() {
        let overlay = make_overlay_with_sites(&[("site-east", true)]);
        let err = verify_multi_provider_overlay(&overlay, &["site-east", "site-west"]).unwrap_err();
        assert!(
            err.to_string().contains("site-west"),
            "error must name the missing site; got: {err}"
        );
    }

    #[test]
    fn verify_multi_provider_overlay_rejects_site_with_only_stale_candidates() {
        let overlay = make_overlay_with_sites(&[("site-east", true), ("site-west", false)]);
        let err = verify_multi_provider_overlay(&overlay, &["site-east", "site-west"]).unwrap_err();
        assert!(
            err.to_string().contains("site-west"),
            "error must name the stale site; got: {err}"
        );
    }

    #[test]
    fn verify_multi_provider_overlay_rejects_missing_candidates_field() {
        let overlay = serde_json::json!({ "network": "net" });
        assert!(
            verify_multi_provider_overlay(&overlay, &["site-east"]).is_err(),
            "overlay without candidates array must fail"
        );
    }

    #[test]
    fn verify_multi_provider_overlay_rejects_candidate_missing_required_field() {
        let overlay = serde_json::json!({
            "network": "net",
            "candidates": [{ "site": "site-east", "cluster": "site-east", "fresh": true }]
        });
        // missing "kind" and "name" fields
        assert!(
            verify_multi_provider_overlay(&overlay, &["site-east"]).is_err(),
            "candidate missing required fields must fail"
        );
    }

    #[test]
    fn verify_multi_provider_overlay_empty_site_list_always_passes() {
        // Degenerate case: no sites expected → validation trivially passes.
        let overlay = make_overlay_with_sites(&[("site-east", true)]);
        assert!(
            verify_multi_provider_overlay(&overlay, &[]).is_ok(),
            "empty site list must pass (no constraints to check)"
        );
    }

    // -----------------------------------------------------------------------
    // verify_metrics_routing_overlay — ordering assertions
    // -----------------------------------------------------------------------

    fn make_metrics_overlay(site_order: &[&str]) -> serde_json::Value {
        let candidates: Vec<serde_json::Value> = site_order
            .iter()
            .map(|site| {
                serde_json::json!({
                    "kind": "inference_model",
                    "name": METRICS_ROUTING_MODEL,
                    "site": site,
                    "cluster": site,
                    "fresh": true
                })
            })
            .collect();
        serde_json::json!({ "network": "net", "local_site": "net", "candidates": candidates })
    }

    #[test]
    fn verify_metrics_routing_overlay_correct_order_passes() {
        let overlay = make_metrics_overlay(&["site-east", "site-west"]);
        assert!(
            verify_metrics_routing_overlay(&overlay, "site-east", "site-west").is_ok(),
            "east before west with lower queue depth must pass"
        );
    }

    #[test]
    fn verify_metrics_routing_overlay_reversed_order_fails() {
        let overlay = make_metrics_overlay(&["site-west", "site-east"]);
        assert!(
            verify_metrics_routing_overlay(&overlay, "site-east", "site-west").is_err(),
            "east after west must fail when east is expected first"
        );
    }

    #[test]
    fn verify_metrics_routing_overlay_missing_first_candidate_fails() {
        let overlay = make_metrics_overlay(&["site-west"]);
        assert!(
            verify_metrics_routing_overlay(&overlay, "site-east", "site-west").is_err(),
            "absent expected-first candidate must fail"
        );
    }

    #[test]
    fn verify_metrics_routing_overlay_missing_second_candidate_fails() {
        let overlay = make_metrics_overlay(&["site-east"]);
        assert!(
            verify_metrics_routing_overlay(&overlay, "site-east", "site-west").is_err(),
            "absent expected-second candidate must fail"
        );
    }

    // -----------------------------------------------------------------------
    // bump_gridnetwork — annotation structure
    // -----------------------------------------------------------------------

    #[test]
    fn bump_gridnetwork_builds_non_empty_annotation() {
        // The bump annotation must include a non-empty value (a Unix timestamp).
        // This is a structural test that does not run kubectl — it verifies the
        // annotation string is well-formed so the actual bump call is predictable.
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let annotation = format!("grid.praxis-proxy.io/reconcile-at={ts}");
        assert!(
            annotation.starts_with("grid.praxis-proxy.io/reconcile-at="),
            "annotation must use the reconcile-at key"
        );
        assert!(ts > 0, "timestamp must be non-zero on a real system");
    }

    // -----------------------------------------------------------------------
    // site join / discovery — pure helper tests
    // -----------------------------------------------------------------------

    #[test]
    fn gridsites_in_network_returns_matching_sites() {
        let sites = vec![
            serde_json::json!({"metadata": {"name": "sjd-primary"}, "spec": {"gridNetworkRef": "sjd-net"}}),
            serde_json::json!({"metadata": {"name": "sjd-joining"}, "spec": {"gridNetworkRef": "sjd-net"}}),
            serde_json::json!({"metadata": {"name": "sjd-wrong"}, "spec": {"gridNetworkRef": "wrong-net"}}),
        ];
        let in_net = gridsites_in_network(&sites, "sjd-net");
        assert_eq!(in_net.len(), 2, "two sites belong to sjd-net");
        assert!(in_net.contains(&"sjd-primary"), "primary must be present");
        assert!(in_net.contains(&"sjd-joining"), "joining must be present");
        assert!(!in_net.contains(&"sjd-wrong"), "wrong-network site must be absent");
    }

    #[test]
    fn gridsites_in_network_excludes_wrong_network_site() {
        let sites =
            vec![serde_json::json!({"metadata": {"name": "sjd-wrong"}, "spec": {"gridNetworkRef": "wrong-net"}})];
        let in_net = gridsites_in_network(&sites, "sjd-net");
        assert!(
            in_net.is_empty(),
            "wrong-network-only list must return empty for the correct network"
        );
    }

    #[test]
    fn gridsites_in_network_empty_list_returns_empty() {
        let in_net = gridsites_in_network(&[], "sjd-net");
        assert!(in_net.is_empty(), "empty input must return empty");
    }

    #[test]
    fn gridsite_phase_index_lifecycle_order() {
        let pending = gridsite_phase_index("Pending").unwrap();
        let discovered = gridsite_phase_index("Discovered").unwrap();
        let connecting = gridsite_phase_index("Connecting").unwrap();
        let active = gridsite_phase_index("Active").unwrap();
        assert!(pending < discovered, "Pending must precede Discovered");
        assert!(discovered < connecting, "Discovered must precede Connecting");
        assert!(connecting < active, "Connecting must precede Active");
    }

    #[test]
    fn gridsite_phase_index_join_sequence_is_monotone() {
        let sequence = ["Pending", "Discovered", "Connecting", "Active"];
        let indices: Vec<usize> = sequence.iter().map(|p| gridsite_phase_index(p).unwrap()).collect();
        let is_strictly_increasing = indices.windows(2).all(|w| matches!(w, [a, b] if a < b));
        assert!(
            is_strictly_increasing,
            "join lifecycle must be a strictly increasing sequence"
        );
    }

    #[test]
    fn gridsite_phase_index_unknown_is_none() {
        assert!(
            gridsite_phase_index("Unknown").is_none(),
            "unknown phase must return None"
        );
        assert!(gridsite_phase_index("").is_none(), "empty string must return None");
        assert!(gridsite_phase_index("pending").is_none(), "lowercase must not match");
    }

    #[test]
    fn gridsite_phase_index_covers_all_defined_phases() {
        let defined = ["Pending", "Discovered", "Connecting", "Active", "Unreachable", "Left"];
        for phase in defined {
            assert!(
                gridsite_phase_index(phase).is_some(),
                "defined phase {phase:?} must have an index"
            );
        }
    }

    // -----------------------------------------------------------------------
    // parse_api_credential_plan — pure function tests
    // -----------------------------------------------------------------------

    #[test]
    fn credential_plan_absent_when_auth_is_null() {
        let plan = parse_api_credential_plan(&serde_json::Value::Null).unwrap();
        assert_eq!(plan, ApiCredentialPlan::Absent, "null auth must produce Absent");
    }

    #[test]
    fn credential_plan_manual_when_manual_is_true() {
        let auth = serde_json::json!({ "manual": true, "strategy": "bearer_token" });
        let plan = parse_api_credential_plan(&auth).unwrap();
        assert_eq!(
            plan,
            ApiCredentialPlan::Manual,
            "manual=true must suppress injection regardless of strategy"
        );
    }

    #[test]
    fn credential_plan_bearer_token_extracts_secret_ref() {
        let auth = serde_json::json!({
            "strategy": "bearer_token",
            "secretRef": { "name": "my-secret", "namespace": "default", "key": "token" }
        });
        let plan = parse_api_credential_plan(&auth).unwrap();
        assert!(
            matches!(
                &plan,
                ApiCredentialPlan::BearerToken { secret_name, namespace, key }
                    if secret_name == "my-secret" && namespace == "default" && key == "token"
            ),
            "bearer_token must extract the secretRef fields; got {plan:?}"
        );
    }

    #[test]
    fn credential_plan_bearer_token_missing_secret_ref_is_error() {
        let auth = serde_json::json!({ "strategy": "bearer_token" });
        assert!(
            parse_api_credential_plan(&auth).is_err(),
            "bearer_token without secretRef must fail"
        );
    }

    #[test]
    fn credential_plan_bearer_token_missing_key_is_error() {
        let auth = serde_json::json!({
            "strategy": "bearer_token",
            "secretRef": { "name": "s", "namespace": "default" }
        });
        assert!(
            parse_api_credential_plan(&auth).is_err(),
            "secretRef without key must fail"
        );
    }

    #[test]
    fn credential_plan_unsupported_strategy_is_error() {
        let auth = serde_json::json!({ "strategy": "api_key" });
        let err = parse_api_credential_plan(&auth).unwrap_err();
        assert!(
            err.to_string().contains("unsupported auth strategy"),
            "unsupported strategy must produce a clear error; got {err}"
        );
    }

    #[test]
    fn credential_plan_token_not_in_overlay_candidate() {
        // Prove RoutingCandidate JSON has no credential-related fields.
        let candidate = serde_json::json!({
            "kind": "inference_model",
            "name": "model-z",
            "site": "op-e2e-api-fallback",
            "cluster": "op-e2e-api-fallback",
            "fresh": true
        });
        assert!(
            candidate.get("token").is_none(),
            "overlay candidate must not carry a token field"
        );
        assert!(
            candidate.get("credential").is_none(),
            "overlay candidate must not carry a credential field"
        );
        assert!(
            candidate.get("auth").is_none(),
            "overlay candidate must not carry an auth field"
        );
    }

    // -----------------------------------------------------------------------
    // api_credential_plan_from_overlay — overlay credential extraction
    // -----------------------------------------------------------------------

    fn parse_overlay(json: &serde_json::Value) -> crate::env::operator_overlay::RoutingOverlay {
        let s = serde_json::to_string(json).unwrap_or_else(|_| std::process::abort());
        crate::env::operator_overlay::parse_grid_config_json(&s).unwrap_or_else(|_| std::process::abort())
    }

    fn make_overlay_with_credential(cluster: &str) -> serde_json::Value {
        serde_json::json!({
            "network": "net",
            "local_site": "site-a",
            "candidates": [{
                "kind": "inference_model",
                "name": "model-z",
                "site": cluster,
                "cluster": cluster,
                "fresh": true,
                "credential": {
                    "strategy": "bearer_token",
                    "secretRef": {
                        "name": "my-secret",
                        "namespace": "default",
                        "key": "token"
                    }
                }
            }]
        })
    }

    #[test]
    fn api_credential_plan_from_overlay_finds_bearer_ref_for_cluster() {
        let json = make_overlay_with_credential("api-cluster");
        let overlay = parse_overlay(&json);
        let plan = api_credential_plan_from_overlay(&overlay, "api-cluster").expect("must find bearer plan");
        match plan {
            ApiCredentialPlan::BearerToken {
                secret_name,
                namespace,
                key,
            } => {
                assert_eq!(secret_name, "my-secret");
                assert_eq!(namespace, "default");
                assert_eq!(key, "token");
            },
            _ => panic!("expected BearerToken plan"),
        }
    }

    #[test]
    fn api_credential_plan_from_overlay_absent_for_no_credential() {
        let json = make_overlay(&[("api-cluster", true)]);
        let overlay = parse_overlay(&json);
        assert!(
            api_credential_plan_from_overlay(&overlay, "api-cluster").is_none(),
            "overlay with no credential field must return None"
        );
    }

    #[test]
    fn api_credential_plan_from_overlay_absent_for_different_cluster() {
        let json = make_overlay_with_credential("api-cluster");
        let overlay = parse_overlay(&json);
        assert!(
            api_credential_plan_from_overlay(&overlay, "other-cluster").is_none(),
            "credential extraction must be scoped to the requested cluster"
        );
    }

    #[test]
    fn api_credential_plan_from_overlay_skips_non_bearer_strategy() {
        let json = serde_json::json!({
            "network": "net",
            "local_site": "site-a",
            "candidates": [{
                "kind": "inference_model",
                "name": "model-z",
                "site": "api-cluster",
                "cluster": "api-cluster",
                "fresh": true,
                "credential": {
                    "strategy": "sigv4",
                    "secretRef": {
                        "name": "aws-secret",
                        "namespace": "default",
                        "key": "key"
                    }
                }
            }]
        });
        let overlay = parse_overlay(&json);
        assert!(
            api_credential_plan_from_overlay(&overlay, "api-cluster").is_none(),
            "non-bearer strategy must not produce a plan"
        );
    }

    // -----------------------------------------------------------------------
    // operator_binary_path / operator_build_args
    // -----------------------------------------------------------------------

    #[test]
    fn operator_binary_path_ends_with_debug_operator() {
        let path = operator_binary_path();
        assert!(
            path.ends_with("target/debug/operator"),
            "binary path must point to target/debug/operator, got {path:?}"
        );
    }

    #[test]
    fn operator_binary_path_is_relative() {
        let path = operator_binary_path();
        assert!(
            path.is_relative(),
            "binary path must be relative so it resolves from the workspace root"
        );
    }

    #[test]
    fn operator_build_args_contains_required_flags() {
        let args = operator_build_args();
        assert!(args.contains(&"build"), "args must include 'build'");
        assert!(args.contains(&"-p"), "args must include '-p'");
        assert!(args.contains(&"operator"), "args must include 'operator'");
        assert!(args.contains(&"--bin"), "args must include '--bin'");
    }

    #[test]
    fn operator_build_args_targets_correct_binary() {
        let args = operator_build_args();
        // "--bin" must be immediately followed by "operator"
        let bin_pos = args.iter().position(|&a| a == "--bin").expect("--bin must be present");
        assert_eq!(
            args.get(bin_pos + 1).copied(),
            Some("operator"),
            "--bin must be followed by 'operator'"
        );
    }

    // -----------------------------------------------------------------------
    // verify_consumer_praxis_yaml
    // -----------------------------------------------------------------------

    /// Build a minimal but structurally valid operator-generated consumer Praxis YAML.
    ///
    /// Matches the complete config shape produced by `generate_consumer_praxis_config`:
    /// `listeners:` → `filter_chains:` → `admin:` → `shutdown_timeout_secs`.
    #[expect(
        clippy::too_many_lines,
        reason = "multi-line YAML template; each line is a required field"
    )]
    fn sample_consumer_yaml(secret_name: &str, secret_key: &str) -> String {
        format!(
            "listeners:\n\
             \x20 - name: public\n\
             \x20   address: \"0.0.0.0:8080\"\n\
             \x20   filter_chains:\n\
             \x20     - consumer-chain\n\
             filter_chains:\n\
             \x20 - name: consumer-chain\n\
             \x20   filters:\n\
             \x20     - filter: json_body_field\n\
             \x20       field: model\n\
             \x20       header: X-Model\n\
             \x20     - filter: grid_route\n\
             \x20       local_site: site-a\n\
             \x20       model_header: X-Model\n\
             \x20       candidates:\n\
             \x20         - kind: inference_model\n\
             \x20           name: model-z\n\
             \x20           site: op-e2e-api-fallback\n\
             \x20           cluster: op-e2e-api-fallback\n\
             \x20           fresh: true\n\
             \x20           credential:\n\
             \x20             strategy: bearer_token\n\
             \x20             secretRef:\n\
             \x20               name: {secret_name}\n\
             \x20               namespace: default\n\
             \x20               key: {secret_key}\n\
             \x20     - filter: grid_credential_inject\n\
             \x20       credentials:\n\
             \x20         - name: {secret_name}\n\
             \x20           namespace: default\n\
             \x20           key: {secret_key}\n\
             \x20           strategy: bearer_token\n\
             \x20           file: /run/secrets/grid-credentials/{secret_name}/{secret_key}\n\
             \x20     - filter: load_balancer\n\
             \x20       clusters:\n\
             \x20         - name: op-e2e-api-fallback\n\
             \x20           endpoints:\n\
             \x20             - \"mock-api-provider.default.svc:8080\"\n\
             admin:\n\
             \x20 address: \"127.0.0.1:9901\"\n\
             shutdown_timeout_secs: 5\n"
        )
    }

    #[test]
    fn verify_consumer_praxis_yaml_passes_for_valid_config() {
        let yaml = sample_consumer_yaml("my-creds", "token");
        assert!(
            verify_consumer_praxis_yaml(&yaml, "sentinel-token-must-not-appear", "my-creds", "token").is_ok(),
            "valid consumer YAML must pass verification"
        );
    }

    #[test]
    fn verify_consumer_praxis_yaml_rejects_missing_grid_route() {
        // Include listeners: and admin: so the checker reaches the grid_route check.
        let yaml = "listeners:\nadmin:\nfilter: load_balancer\nfilter: grid_credential_inject\ncredential:\nsecretRef:\nfile: /run/secrets/grid-credentials/creds/token\nendpoints:\n";
        let err = verify_consumer_praxis_yaml(yaml, "tok", "creds", "token").unwrap_err();
        assert!(
            err.to_string().contains("grid_route"),
            "must report missing grid_route: {err}"
        );
    }

    #[test]
    fn verify_consumer_praxis_yaml_rejects_token_bytes_in_yaml() {
        let secret_token = "sk-real-api-token-must-not-appear";
        let yaml = format!(
            "{}\ntoken-leaked: {secret_token}\n",
            sample_consumer_yaml("creds", "token")
        );
        let err = verify_consumer_praxis_yaml(&yaml, secret_token, "creds", "token").unwrap_err();
        assert!(
            err.to_string().contains("token bytes"),
            "must report token bytes present: {err}"
        );
    }

    #[test]
    fn verify_consumer_praxis_yaml_rejects_static_header_injection() {
        let yaml = format!(
            "{}\n- filter: headers\n  request_set:\n    - name: Authorization\n",
            sample_consumer_yaml("creds", "token")
        );
        let result = verify_consumer_praxis_yaml(&yaml, "sentinel-token", "creds", "token");
        assert!(result.is_err(), "static header injection must be rejected");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("headers") || err.contains("request_set"),
            "error must mention forbidden element: {err}"
        );
    }

    #[test]
    fn verify_consumer_praxis_yaml_rejects_inline_value_source() {
        let yaml = format!(
            "{}\n          value: sk-some-token\n",
            sample_consumer_yaml("creds", "token")
        );
        let err = verify_consumer_praxis_yaml(&yaml, "sentinel-token", "creds", "token").unwrap_err();
        assert!(
            err.to_string().contains("value:"),
            "inline value source must be rejected: {err}"
        );
    }

    #[test]
    fn verify_consumer_praxis_yaml_rejects_missing_file_path() {
        // YAML with grid_credential_inject but no file: path for the secret.
        // Includes listeners: and admin: so the checker reaches the file-path check.
        let yaml = "listeners:\nadmin:\nfilter: grid_route\nfilter: grid_credential_inject\nfilter: load_balancer\ncredential:\nsecretRef:\nendpoints:\n";
        let err = verify_consumer_praxis_yaml(yaml, "tok", "my-creds", "token").unwrap_err();
        assert!(
            err.to_string().contains("/run/secrets/grid-credentials/my-creds/token"),
            "missing file path must be reported: {err}"
        );
    }

    #[test]
    fn network_fixture_with_consumer_config_includes_consumer_config() {
        let endpoints = serde_json::json!([{
            "cluster": "gateway-site-a",
            "address": "10.0.0.10:30080",
            "sni": "site-a.grid.internal"
        }]);
        let json_str = network_fixture_with_consumer_config_json("net", "gw", "ns", "my-cm", &endpoints);
        let value: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        let refs = &value["spec"]["gatewayRefs"];
        let first = &refs[0];
        assert_eq!(
            first["consumerConfig"]["enabled"].as_bool(),
            Some(true),
            "consumerConfig.enabled must be true"
        );
        assert_eq!(
            first["consumerConfig"]["configMapName"].as_str(),
            Some("my-cm"),
            "configMapName must be the provided name"
        );
        assert_eq!(
            first["consumerConfig"]["clusterEndpoints"][0]["cluster"].as_str(),
            Some("gateway-site-a"),
            "clusterEndpoints must be included"
        );
    }
}
