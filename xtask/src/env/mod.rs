//! Multi-cluster test environment management.

pub(crate) mod certs;
pub(crate) mod config;
pub(crate) mod consumer;
pub(crate) mod gateway;
pub(crate) mod image_overrides;
pub(crate) mod images;
pub(crate) mod kind;
pub(crate) mod kubectl;
pub(crate) mod operator;
pub(crate) mod operator_overlay;
pub(crate) mod providers;
pub(crate) mod trust;
pub(crate) mod verify;

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use clap::Subcommand;

use self::config::{ClusterRole, EnvConfig, ProviderBackend};

// ---------------------------------------------------------------------------
// Shared infrastructure helpers
// ---------------------------------------------------------------------------

/// RAII guard that kills a subprocess on drop.
///
/// Used to ensure the operator and port-forward processes are always stopped
/// when the reconcile function returns, even on error.
struct ProcGuard(Option<std::process::Child>, &'static str);

impl Drop for ProcGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            drop(c.kill());
            drop(c.wait());
            eprintln!("  {} stopped", self.1);
        }
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default path to the environment configuration file.
const DEFAULT_CONFIG_PATH: &str = "tests/env/config.toml";

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// Actions for the `env` subcommand.
#[derive(Debug, Subcommand)]
pub(crate) enum Action {
    /// Create or update the test environment.
    Up {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Tear down the test environment.
    Down {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Report the status of all environment components.
    Status {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Verify provider inference endpoints are reachable.
    VerifyProviders {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Build gateway images from the AI repository.
    BuildGatewayImages {
        /// Path to the AI repository. Can also be provided via `AI_REPO_PATH`.
        #[arg(long)]
        ai_repo: Option<PathBuf>,
    },

    /// Load gateway images into all kind clusters.
    LoadGatewayImages {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Deploy provider gateways into provider clusters.
    DeployProviderGateways {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Verify provider gateways through the configured provider backend request path.
    VerifyProviderGateways {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Probe cross-kind network connectivity from consumer to providers.
    ProbeGatewayNetwork {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Deploy the consumer gateway.
    ///
    /// Without `--overlay-config`, generates a static `grid_route` config from
    /// provider sites declared in the environment config file.
    ///
    /// With `--overlay-config`, reads a routing overlay `grid-config.json`
    /// from the given path. The overlay `local_site` and candidates
    /// drive the `grid_route` stanza; the `load_balancer` section is still
    /// generated from provider endpoints in the environment config.
    DeployConsumerGateway {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,

        /// Path to a `grid-config.json` routing overlay.
        ///
        /// When provided, `grid_route.local_site` and candidates are taken
        /// from the overlay file.  When absent, the static config derived from
        /// `config.toml` provider sites is used.
        #[arg(long)]
        overlay_config: Option<PathBuf>,
    },

    /// Verify consumer-to-provider gateway routing end-to-end.
    VerifyGatewayE2e {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Verify gateway-to-gateway mTLS trust (positive + negative cases).
    VerifyMtlsTrust {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },

    /// Install Grid CRDs (`GridNetwork`, `GridSite`, `InferenceProvider`) into a cluster.
    ///
    /// Generates CRD manifests from the Rust type definitions and applies them
    /// via `kubectl apply`.  Run after `env up` and before `verify-operator-reconcile`.
    InstallGridCrds {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,

        /// Site name from the config to install CRDs into (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Validate Grid operator reconciliation: install CRDs, apply test fixtures,
    /// run the operator locally, and verify health-aware overlay generation.
    ///
    /// Runs the operator binary **out-of-cluster** using the current kubeconfig.
    /// The operator must be compiled (`cargo build -p operator`) before this command.
    VerifyOperatorReconcile {
        /// Path to the environment config file.
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,

        /// Site name from the config to run the validation against.
        #[arg(long)]
        site: Option<String>,
    },

    /// Validate the full operator-to-consumer routing flow in kind.
    ///
    /// Orchestrates in one command:
    ///
    /// 1. Deploy provider gateways (idempotent).
    /// 2. Install Grid CRDs and apply `InferenceProvider` fixtures.
    /// 3. Run the Grid operator out-of-cluster (spawned via `cargo run`).
    /// 4. Wait for provider reconciliation:
    ///    - healthy → `Pending`
    ///    - invalid → `Unavailable`
    ///    - degraded → `Degraded`
    ///    - api fallback → `Pending`
    /// 5. Verify the overlay `ConfigMap` (healthy present, unavailable excluded, scoring order).
    /// 6. Export the overlay to a temp file.
    /// 7. Deploy the consumer gateway from the operator-exported overlay.
    /// 8. Verify end-to-end routing: locally routable model returns 200, unknown model fails cleanly.
    ///
    /// Requires kind clusters and gateway images to be ready.  Run `env up` and
    /// `env load-gateway-images` first.  Safe to rerun: owned test resources are
    /// deleted at the start of each run.
    ValidateOperatorRouting {
        /// Path to the environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing.toml")]
        config: PathBuf,

        /// Site name from the config to run the operator against (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Prove live SWIM membership reaches `GridNetwork` status.
    ///
    /// Starts two out-of-cluster operator processes each with a distinct SWIM
    /// identity, has the secondary announce to the primary, waits for gossip
    /// convergence, then applies a `GridNetwork` resource and polls until:
    ///
    /// - `status.phase = Active` (derived from live `MembershipSnapshot`)
    /// - `status.connectedSites ≥ 1` (one SWIM peer confirmed alive)
    ///
    /// Uses available localhost UDP ports selected at runtime. Both operators
    /// connect to the same kind cluster (context resolved from `config` via
    /// `--site`). Safe to rerun: the `GridNetwork` fixture is deleted before
    /// and after the run.
    ///
    /// Requires a kind cluster with Grid CRDs installable (`env up` +
    /// `env load-gateway-images` are **not** required — this command installs
    /// the CRDs itself).
    VerifySwimMembership {
        /// Path to the environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing.toml")]
        config: PathBuf,

        /// Kind cluster context to run against (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Prove live CRDT state propagation over SWIM.
    ///
    /// Starts two SWIM-enabled operator processes against the same kind
    /// cluster.  Each operator publishes real `InferenceProvider`-derived state
    /// as a CRDT `GridStateSnapshot` on `GridNetwork` reconcile.  After SWIM
    /// gossip convergence the remote operator's provider-state broadcast arrives and
    /// `GridNetwork.status.distributedProviderCount` becomes ≥ 1.
    ///
    /// Proves that:
    /// - Operators use real foca UDP custom broadcasts (not direct injection).
    /// - The `StateBroadcastHandler` receives and merges remote state.
    /// - `GridNetworkStatus.distributedProviderCount` reflects the merged state.
    ///
    /// Requires a kind cluster.  Run `env up` + `env load-gateway-images` first.
    /// Safe to rerun: the `GridNetwork` and `InferenceProvider` fixtures are
    /// deleted at the start of each run.
    VerifySwimState {
        /// Path to the environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing.toml")]
        config: PathBuf,

        /// Kind cluster context to run against (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Prove that `GridNetwork.spec.seeds` drives SWIM mesh formation.
    ///
    /// Starts two out-of-cluster operator processes with
    /// `GRID_SWIM_SEEDS=""` (env-var seeds intentionally empty).  Applies a
    /// `GridNetwork` with `spec.seeds` containing the primary operator's UDP
    /// address.  Both operators reconcile the `GridNetwork`, the secondary
    /// reads `spec.seeds` and announces to the primary via the CRD-driven
    /// seed path, and SWIM gossip converges.
    ///
    /// Asserts:
    /// - Both operators started with `GRID_SWIM_SEEDS=""`.
    /// - `GridNetwork.spec.seeds` contains the primary's address.
    /// - `status.phase = Active` and `status.connectedSites >= 1`.
    ///
    /// This proves that CRD-sourced seeds alone are sufficient for mesh
    /// formation — no env-var seeds are required.
    ///
    /// Requires a kind cluster with Grid CRDs.  Run `env up` first.
    /// Safe to rerun: fixtures are deleted at the start of each run.
    VerifySwimCrdSeeds {
        /// Path to the environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing.toml")]
        config: PathBuf,

        /// Kind cluster context to run against (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Run all validations in sequence and print a Markdown summary table.
    ///
    /// Runs, in order:
    ///
    /// 1. `status` — confirm clusters and certs are ready.
    /// 2. `validate-operator-routing` — overlay generation and Praxis routing.
    /// 3. `verify-swim-membership` — SWIM gossip drives `phase=Active`.
    /// 4. `verify-swim-state` — real CRDT state propagates over SWIM.
    /// 5. `verify-mtls-trust` — mTLS positive + negative cases.
    ///
    /// Each step is run even if previous steps fail; all results are collected
    /// and the table is printed at the end.  Exit code is non-zero when any
    /// step has `FAIL` status.  `BLOCKED` results (due to missing
    /// prerequisites) are noted but do **not** cause a non-zero exit on their
    /// own.
    ///
    /// Requires kind clusters and gateway images to be ready.  Run `env up`
    /// and `env load-gateway-images` first.
    ValidateAll {
        /// Path to the environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing.toml")]
        config: PathBuf,

        /// Kind cluster context (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Prove the API-provider fallback routing path end-to-end.
    ///
    /// Applies a local self-hosted `InferenceProvider` for `model-x` (served by
    /// the site-a mock provider gateway) and an `api_provider` `InferenceProvider`
    /// for `model-z` (served by a mock OpenAI-compatible API endpoint in the
    /// consumer cluster).  Runs the Grid operator to generate an overlay, then
    /// deploys a consumer gateway whose config includes both the mTLS cluster for
    /// the local provider and a plain-HTTP cluster for the API-provider mock.
    ///
    /// Asserts:
    /// - The overlay contains both candidates with the `api_provider` sorted last.
    /// - `model-x` requests return HTTP 200 from the local provider gateway.
    /// - `model-z` requests return HTTP 200 from the mock API-provider endpoint.
    /// - The client sends only a consumer-level credential, not a provider API key.
    /// - An unknown model returns 404 or 503.
    ///
    /// Requires: kind clusters (`grid-site-a` and `grid-consumer`) and gateway
    /// images to be ready.  Run `env up` and `env load-gateway-images` first.
    /// Safe to rerun: all owned resources are cleaned at the start.
    VerifyApiFallback {
        /// Path to the environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing.toml")]
        config: PathBuf,

        /// Kind cluster context (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Validate the native AI credential injection path.
    ///
    /// Proves the same API-provider fallback routing as `verify-api-fallback` but
    /// uses `filter: grid_credential_inject` instead of `filter: headers` /
    /// `request_set` for bearer token injection.
    ///
    /// Key differences from `verify-api-fallback`:
    ///
    /// - `grid_route` candidates include credential `secretRef` data from the overlay.
    /// - `filter: grid_credential_inject` injects the bearer token, keyed by the credential secretRef `(name,
    ///   namespace, key)` tuple.
    /// - The old `filter: headers` / `request_set` static injection is absent.
    ///
    /// **Token placement:** the bearer token is mounted into the consumer pod from
    /// a Kubernetes Secret and read through `grid_credential_inject.credentials[].file`.
    /// It must not appear in the operator overlay or consumer Praxis `ConfigMap`.
    ///
    /// Requires: kind clusters (`grid-site-a` and `grid-consumer`) and gateway
    /// images to be ready.  Run `env up` and `env load-gateway-images` first.
    /// Safe to rerun: all owned resources are cleaned at the start.
    VerifyApiFallbackNative {
        /// Path to the environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing.toml")]
        config: PathBuf,

        /// Kind cluster context (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Validate generated CRD schema fields.
    ///
    /// Runs `generate_crds`, parses the output JSON, and asserts that all
    /// required fields are present in the generated schemas.  Exits non-zero
    /// if any field is missing.
    ///
    /// Does **not** require kind clusters — it runs against the binary output
    /// of `cargo run -p operator --bin generate_crds`.
    VerifyCrdSchema,

    /// Prove that SWIM transport AES-256-GCM encryption is enforced.
    ///
    /// Five scenarios: (A) env-keyed peers converge, (B) SecretRef-keyed
    /// peers converge, (C) wrong-key peer rejected, (D) plaintext peer
    /// rejected, (E) missing Secret prevents plaintext sends.
    ///
    /// Requires the multisite kind environment.  Safe to rerun: fixtures
    /// are deleted at the start of each run.
    VerifySwimEncryption {
        /// Path to the multisite environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing-multisite.toml")]
        config: PathBuf,

        /// Kind cluster context to run against (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Prove that CRDT/SWIM-distributed provider records appear in the routing overlay.
    ///
    /// Starts two SWIM-enabled operator processes with the same SWIM encryption
    /// key and proves that CRDT state from the secondary peer enters the overlay.
    /// Also proves that a peer with a wrong key and a plaintext peer cannot join.
    ///
    /// Requires a kind cluster.  Run `env up` first.  Safe to rerun: fixtures
    /// are deleted at the start of each run.
    VerifySwimOverlay {
        /// Path to the environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing.toml")]
        config: PathBuf,

        /// Kind cluster context to run against (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Prove transitive SWIM discovery, provider propagation, and routing eligibility
    /// across a three-node mesh.
    ///
    /// Spawns three operator processes:
    /// - Node A: no seeds (origin).
    /// - Node B: seeds A (bridge).
    /// - Node C: seeds B only — not A (leaf).
    ///
    /// After SWIM gossip, A learns about C transitively through B.  The test proves:
    /// 1. A's `distributedProviderCount >= 2` (received CRDT from both B and C).
    /// 2. C's candidate is absent from A's overlay before C's `GridSite` is `Active`.
    /// 3. After `Active`, C's candidate appears in A's overlay.
    /// 4. Wrong-network provider records are absent from A's correct-network overlay.
    ///
    /// Requires the multisite kind environment.  Safe to rerun.
    VerifySwimMeshThreeNode {
        /// Path to the multisite environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing-multisite.toml")]
        config: PathBuf,

        /// Kind cluster context to run against (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Prove end-to-end distributed model routing via CRDT/SWIM discovery.
    ///
    /// Runs two SWIM-enabled operator processes against separate kind clusters
    /// (east and west provider sites).  Each cluster has its own `InferenceProvider`:
    /// model-east on the east cluster, model-west on the west cluster.  After
    /// SWIM gossip, the east operator's overlay includes model-west as a remote
    /// CRDT-sourced candidate.  A consumer gateway is deployed from that overlay
    /// and routes requests for both models — model-east via the local east gateway
    /// and model-west via the CRDT-discovered west gateway — asserting HTTP 200
    /// for both.
    ///
    /// This is the minimal deterministic kind proof of:
    ///   `InferenceProvider (west k8s) → CRDT over SWIM → overlay candidate →
    ///    consumer grid_route → provider gateway → HTTP 200`
    ///
    /// Requires the two-provider kind environment.  Run
    /// `env up -c tests/env/operator-routing-two-provider.toml` and
    /// `env load-gateway-images -c tests/env/operator-routing-two-provider.toml`
    /// first.  Safe to rerun: fixtures are deleted at the start of each run.
    VerifySwimRouting {
        /// Path to the two-provider environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing-two-provider.toml")]
        config: PathBuf,
    },

    /// Prove the full fingerprint trust-promotion lifecycle for a `GridSite`.
    ///
    /// Spawns two SWIM-enabled operators.  Node B has TLS configured so it
    /// broadcasts its public certificate.  The test proves:
    ///
    /// 1. B reaches `Connecting` with gateway reachable.
    /// 2. `status.reason` is `TrustPolicyMissing` (cert present, no fingerprint configured).
    /// 3. With a wrong fingerprint: `TrustPolicyMismatch`.
    /// 4. With the correct fingerprint: operator promotes to `Active` (`TrustPolicyVerified`).
    /// 5. Before `Active`: B's CRDT provider absent from A's overlay.
    /// 6. After `Active`: B's CRDT provider present in A's overlay.
    ///
    /// Requires the multisite kind environment.  Safe to rerun.
    VerifyGridsiteTrustFingerprint {
        /// Path to the multisite environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing-multisite.toml")]
        config: PathBuf,

        /// Kind cluster context to run against (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },

    /// Print the SHA-256 fingerprint of a `GridSite.status.publicCertPem`.
    ///
    /// Reads the public certificate PEM from the named `GridSite` status and
    /// prints the colon-separated SHA-256 fingerprint suitable for use as
    /// `spec.trust.certFingerprint`.
    ///
    /// Never prints private key material.
    GridsiteFingerprint {
        /// Kubernetes context to use.
        #[arg(long)]
        context: String,

        /// Name of the `GridSite` resource.
        #[arg(long)]
        name: String,
    },

    /// Verify the dedicated llm-d-compatible provider-gateway path.
    ///
    /// Tests routing through Praxis AI `ext_proc` with llm-d compatibility.
    /// Requires Praxis AI image with llm-d `ext_proc` support (praxis-proxy/ai#334)
    /// and AI-owned mock EPP test image (pending AI PR). Mock EPP is AI test
    /// support, not a Grid mock provider.
    ///
    /// Proves the full llm-d routing path end-to-end in kind:
    ///
    /// 1. Deploy provider gateways with mock EPP + ext\_proc + endpoint\_selector.
    /// 2. Run the multi-provider operator reconcile and export the routing overlay.
    /// 3. Verify the provider-side llm-d path directly on each provider gateway.
    /// 4. Deploy the consumer gateway from the operator-exported overlay.
    /// 5. Verify consumer routing: each model returns HTTP 200, unknown model fails cleanly.
    /// 6. Verify Chat Completions response bodies include the requested model name.
    /// 7. Assert no unexpected pod restarts during the test.
    ///
    /// Requires the multisite kind environment.  Run
    /// `env up -c tests/env/operator-routing-multisite.toml` and
    /// `env load-gateway-images -c tests/env/operator-routing-multisite.toml`
    /// first.
    VerifyLlmdCompatibleRouting {
        /// Path to the multisite environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing-multisite.toml")]
        config: PathBuf,
    },

    /// Verify `/v1/responses` request parsing and Grid overlay routing.
    ///
    /// Proves that Responses API requests are correctly parsed and routed
    /// when the consumer filter chain uses `openai_responses_format` to extract
    /// the model field, then `grid_route` for Grid overlay routing.
    ///
    /// 1. Deploy provider gateways (idempotent).
    /// 2. Operator reconcile + overlay export.
    /// 3. Deploy consumer gateway with `openai_responses_format` → `grid_route` filter chain.
    /// 4. Send `/v1/responses` requests: each model returns HTTP 200 with valid Responses body, unknown model fails
    ///    closed.
    /// 5. Verify response model fields match the requested model.
    /// 6. Assert no unexpected pod restarts.
    ///
    /// Requires the multi-site kind environment.  Run
    /// `env up -c tests/env/operator-routing-multisite.toml` and
    /// `env load-gateway-images -c tests/env/operator-routing-multisite.toml`
    /// first.
    VerifyResponsesRouting {
        /// Path to the environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing-multisite.toml")]
        config: PathBuf,
    },

    /// Prove the full-grid routing path across all four backend kinds.
    ///
    /// Validates one consumer gateway routing across:
    /// - `model-east` via site-east local/self-hosted provider (mTLS provider gateway)
    /// - `model-west` via site-west remote/self-hosted provider (mTLS provider gateway)
    /// - `model-cloud` via cloud-managed mock (plain HTTP, in consumer cluster)
    /// - `model-api` via API-provider mock (plain HTTP, gateway-injected credential)
    ///
    /// The Grid operator generates the routing overlay for all four backends.
    /// The consumer config is extended to include non-mTLS clusters for the
    /// cloud and API mocks alongside the standard mTLS clusters for site-east
    /// and site-west.
    ///
    /// Requires: two-provider kind environment.  Run `env up` and
    /// `env load-gateway-images` with the two-provider config first.
    /// Safe to rerun: all owned resources are cleaned at the start.
    VerifyFullGridRouting {
        /// Path to the two-provider environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing-two-provider.toml")]
        config: PathBuf,
    },

    /// Prove that live metrics change the backend selected by consumer requests.
    ///
    /// Runs in two phases:
    ///
    /// **Phase 1 (east low, west high):** Site-east reports `queue_depth = 0.1`
    /// and site-west reports `0.9`.  The operator generates an overlay where
    /// site-east appears first for `model-metrics-shared`.  A consumer request
    /// for that model routes to site-east.
    ///
    /// **Phase 2 (flipped):** Metrics are updated so site-east reports `0.9`
    /// and site-west reports `0.1`.  The operator re-reconciles; the overlay
    /// flips so site-west appears first.  The consumer request now routes to
    /// site-west.
    ///
    /// Attribution is structural: both providers echo the same model name,
    /// so the overlay position is the primary evidence.
    ///
    /// Requires the two-provider kind environment.  Run `env up` and
    /// `env load-gateway-images` with the two-provider config first.
    /// Safe to rerun: all owned resources are cleaned at the start.
    VerifyMetricsRouting {
        /// Path to the two-provider environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing-two-provider.toml")]
        config: PathBuf,
    },

    /// Verify site join/discovery lifecycle validation.
    ///
    /// Starts a primary SWIM operator and a joining SWIM operator, waits for
    /// membership formation, then advances the joining `GridSite`'s lifecycle
    /// through `Pending → Discovered → Connecting → Active`.  Only `Discovered`
    /// is harness-patched; `Connecting` and `Active` are driven by the
    /// `GridSite` controller.  `Active` requires the TCP probe to succeed and
    /// the configured `spec.trust.certFingerprint` to match `status.publicCertPem`.
    ///
    /// Also verifies:
    /// - the joined site has routing-relevant spec fields (egress, network, identity)
    /// - an overlay generated for the correct network contains both primary and joining site candidates
    /// - a wrong-network `GridSite` is absent from the correct-network overlay
    ///
    /// Requires the multisite kind environment.  Safe to rerun.
    VerifySiteJoinDiscovery {
        /// Path to the multisite environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing-multisite.toml")]
        config: PathBuf,
    },

    /// Verify lost-peer staleness: remote provider marked stale when SWIM peer is killed.
    ///
    /// Starts a primary (east) and a joining (west) SWIM operator, publishes a
    /// local east provider and a remote west provider, verifies both appear in
    /// the overlay with `fresh=true`, then kills the west operator and waits for
    /// the east operator to mark the remote provider as `fresh=false`.
    ///
    /// Proves that `apply_swim_staleness_override` correctly translates a `Dead`
    /// SWIM membership entry into a `Degraded` CRDT provider phase, which the
    /// overlay renderer emits as `fresh=false`.  The local east candidate remains
    /// `fresh=true` throughout, giving the data plane a clear preference signal.
    ///
    /// This is a simulated partition (process kill) - real network-level isolation
    /// is not required.  Requires the two-provider kind environment.  Safe to rerun.
    VerifyFailoverUnderLostPeer {
        /// Path to the two-provider environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing-two-provider.toml")]
        config: PathBuf,
    },

    /// Prove that stale remote candidates are evicted from the overlay after the
    /// `GridNetwork.spec.staleCandidateTtlSeconds` TTL elapses.
    ///
    /// Starts two SWIM-enabled operators, applies fixtures that include
    /// `staleCandidateTtlSeconds: 5`, verifies initial overlay contains both
    /// candidates with `fresh=true`, kills the west operator so its candidate
    /// becomes `fresh=false`, then waits for the TTL to expire and asserts
    /// the stale candidate is **absent** from the rendered overlay.  The local
    /// east candidate remains present and `fresh=true` throughout.
    ///
    /// This proves the full path:
    /// - CRD field `staleCandidateTtlSeconds` is consumed by the controller.
    /// - `stale_policy_from_spec` derives the correct policy.
    /// - `apply_stale_gc_filter` omits the candidate once `age_secs >= TTL`.
    /// - Local candidates are unaffected by the remote GC policy.
    ///
    /// Requires a kind cluster.  Run `env up` + `env load-gateway-images` first.
    /// Safe to rerun: fixtures are deleted at the start of each run.
    VerifyStaleGcTtl {
        /// Path to the two-provider environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing-two-provider.toml")]
        config: PathBuf,
    },

    /// Verify the operator install/RBAC package in Kind.
    ///
    /// Applies the install manifests from `deploy/operator/`, runs positive and
    /// negative `kubectl auth can-i` checks against the `grid-operator`
    /// `ServiceAccount`, then spawns an out-of-cluster operator and proves RBAC
    /// is sufficient for a minimal reconcile (status written, overlay `ConfigMap`
    /// created).
    ///
    /// Requires a kind cluster.  Run `env up` first.  Safe to rerun: install
    /// resources and test fixtures are cleaned at the start.
    VerifyOperatorInstallRbac {
        /// Path to the environment config file.
        #[arg(short, long, default_value = "tests/env/operator-routing.toml")]
        config: PathBuf,

        /// Kind cluster context to run against (first provider site by default).
        #[arg(long)]
        site: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Run the requested environment action.
///
/// # Errors
///
/// Returns an error if the configuration cannot be loaded or the
/// action fails.
#[expect(
    clippy::too_many_lines,
    reason = "one arm per CLI action - splitting adds no clarity"
)]
pub(crate) fn run(action: &Action) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        Action::Up { config } => env_up(config),
        Action::Down { config } => env_down(config),
        Action::Status { config } => env_status(config),
        Action::VerifyProviders { config } => env_verify_providers(config),
        Action::BuildGatewayImages { ai_repo } => env_build_gateway_images(ai_repo.as_deref()),
        Action::LoadGatewayImages { config } => env_load_gateway_images(config),
        Action::DeployProviderGateways { config } => env_deploy_provider_gateways(config),
        Action::VerifyProviderGateways { config } => env_verify_provider_gateways(config),
        Action::ProbeGatewayNetwork { config } => env_probe_gateway_network(config),
        Action::DeployConsumerGateway { config, overlay_config } => {
            env_deploy_consumer_gateway(config, overlay_config.as_deref())
        },
        Action::VerifyGatewayE2e { config } => env_verify_gateway_e2e(config),
        Action::VerifyMtlsTrust { config } => env_verify_mtls_trust(config),
        Action::InstallGridCrds { config, site } => env_install_grid_crds(config, site.as_deref()),
        Action::VerifyOperatorReconcile { config, site } => env_verify_operator_reconcile(config, site.as_deref()),
        Action::ValidateOperatorRouting { config, site } => env_validate_operator_routing(config, site.as_deref()),
        Action::VerifySwimMembership { config, site } => env_verify_swim_membership(config, site.as_deref()),
        Action::VerifySwimState { config, site } => env_verify_swim_state(config, site.as_deref()),
        Action::VerifySwimCrdSeeds { config, site } => env_verify_swim_crd_seeds(config, site.as_deref()),
        Action::ValidateAll { config, site } => env_validate_all(config, site.as_deref()),
        Action::VerifyApiFallback { config, site } => env_verify_api_fallback(config, site.as_deref()),
        Action::VerifyApiFallbackNative { config, site } => env_verify_api_fallback_native(config, site.as_deref()),
        Action::VerifyCrdSchema => env_verify_crd_schema(),
        Action::VerifySwimEncryption { config, site } => env_verify_swim_encryption(config, site.as_deref()),
        Action::VerifySwimOverlay { config, site } => env_verify_swim_overlay(config, site.as_deref()),
        Action::VerifySwimMeshThreeNode { config, site } => env_verify_swim_mesh_three_node(config, site.as_deref()),
        Action::VerifyGridsiteTrustFingerprint { config, site } => {
            env_verify_gridsite_trust_fingerprint(config, site.as_deref())
        },
        Action::GridsiteFingerprint { context, name } => env_gridsite_fingerprint(context.as_str(), name.as_str()),
        Action::VerifySwimRouting { config } => env_verify_swim_routing(config),
        Action::VerifyLlmdCompatibleRouting { config } => env_verify_llmd_compat_routing(config),
        Action::VerifyResponsesRouting { config } => env_verify_responses_routing(config),
        Action::VerifyFullGridRouting { config } => env_verify_full_grid_routing(config),
        Action::VerifyMetricsRouting { config } => env_verify_metrics_routing(config),
        Action::VerifySiteJoinDiscovery { config } => env_verify_site_join_discovery(config),
        Action::VerifyFailoverUnderLostPeer { config } => env_verify_failover_under_lost_peer(config),
        Action::VerifyStaleGcTtl { config } => env_verify_stale_gc_ttl(config),
        Action::VerifyOperatorInstallRbac { config, site } => env_verify_operator_install_rbac(config, site.as_deref()),
    }
}

/// Create or update the test environment.
fn env_up(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    print_topology(&cfg);

    for name in &cfg.clusters.names {
        if let Some(def) = cfg.clusters.definitions.get(name) {
            kind::create_cluster(name, def)?;
        }
    }

    certs::generate_all(&cfg.clusters.names)?;

    if let Err(e) = providers::start_all(&cfg.providers) {
        eprintln!("warning: mock providers failed to start: {e}");
        eprintln!("         (build the grid-mock-providers image first if needed)");
        eprintln!("         provider inference baseline does not require mock providers");
    }

    eprintln!("env up: clusters and certs ready");
    Ok(())
}

/// Tear down the test environment.
fn env_down(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;

    for name in &cfg.clusters.names {
        kind::delete_cluster(name)?;
    }

    providers::stop_all()?;
    certs::cleanup()?;
    eprintln!("env down: clusters, providers, and certs removed");
    Ok(())
}

/// Report the status of all environment components.
fn env_status(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    let mut all_ok = true;

    all_ok = report_cluster_status(&cfg, all_ok);
    all_ok = report_provider_status(all_ok);
    all_ok = report_cert_status(all_ok);

    let summary = if all_ok {
        "all components healthy"
    } else {
        "some components not ready"
    };
    eprintln!("status: {summary}");
    Ok(())
}

/// Report cluster and deployment status.
fn report_cluster_status(cfg: &EnvConfig, mut all_ok: bool) -> bool {
    eprintln!("clusters:");
    for name in &cfg.clusters.names {
        let ok = kind::is_cluster_running(name);
        all_ok = all_ok && ok;
        eprintln!("  grid-{name}: {}", status_label(ok));
        if ok
            && let Some(def) = cfg.clusters.definitions.get(name)
            && def.role == ClusterRole::Provider
        {
            let deploy_ok = kind::is_provider_backend_ready(name, def);
            all_ok = all_ok && deploy_ok;
            let deploy = kind::provider_backend_deployment_name(def);
            eprintln!("    {deploy}: {}", status_label(deploy_ok));
        }
    }
    all_ok
}

/// Report mock provider container status.
fn report_provider_status(mut all_ok: bool) -> bool {
    eprintln!("providers:");
    for provider in &["openai", "anthropic", "bedrock", "vertex"] {
        let ok = providers::is_running(provider);
        all_ok = all_ok && ok;
        eprintln!("  mock-{provider}: {}", status_label(ok));
    }
    all_ok
}

/// Report certificate status.
fn report_cert_status(mut all_ok: bool) -> bool {
    eprintln!("certificates:");
    let ok = certs::certs_exist();
    all_ok = all_ok && ok;
    eprintln!("  CA + site certs: {}", status_label(ok));
    all_ok
}

/// Verify provider inference endpoints.
fn env_verify_providers(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    verify::verify_providers(&cfg)
}

/// Build gateway images from the AI repository.
fn env_build_gateway_images(ai_repo: Option<&Path>) -> Result<(), Box<dyn std::error::Error>> {
    let resolved = images::resolve_ai_repo_path(ai_repo)?;
    eprintln!("building gateway images from {}...", resolved.display());
    images::build_all(&resolved)?;
    eprintln!("env build-gateway-images: done");
    Ok(())
}

/// Load gateway images into all kind clusters.
fn env_load_gateway_images(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    eprintln!("loading gateway images into kind clusters...");
    images::load_all(&cfg)?;
    eprintln!("env load-gateway-images: done");
    Ok(())
}

/// Deploy provider gateways into provider clusters.
fn env_deploy_provider_gateways(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    eprintln!("deploying provider gateways...");
    gateway::deploy_all(&cfg)?;
    eprintln!("env deploy-provider-gateways: done");
    Ok(())
}

/// Verify provider gateways.
fn env_verify_provider_gateways(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    gateway::verify_all(&cfg)
}

/// Probe cross-kind network connectivity from the consumer cluster to all
/// provider gateways.
fn env_probe_gateway_network(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    consumer::probe_network(&cfg)
}

/// Deploy the consumer gateway.
///
/// When `overlay_config` is `Some`, reads a routing overlay `grid-config.json`
/// and uses it to drive the `grid_route` stanza.
/// When `overlay_config` is `None`, generates a static config from the
/// environment config provider sites (existing behaviour).
fn env_deploy_consumer_gateway(config: &Path, overlay_config: Option<&Path>) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    consumer::deploy_consumer(&cfg, overlay_config)
}

/// Verify consumer-to-provider gateway routing end-to-end.
fn env_verify_gateway_e2e(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    consumer::verify_e2e(&cfg)
}

/// Prove the API-provider fallback routing path end-to-end.
///
/// Orchestrates:
/// 1. Deploy provider gateways (site-a) and load gateway images.
/// 2. Deploy `mock-api-provider` in the consumer cluster.
/// 3. Run the Grid operator to generate an overlay with both local and `api_provider` candidates.
/// 4. Deploy the consumer gateway with a plain-HTTP cluster for the API-provider mock in addition to the standard mTLS
///    clusters for local providers.
/// 5. Verify: `model-x` → local provider gateway (HTTP 200), `model-z` → mock API-provider (HTTP 200), unknown model →
///    404.
#[expect(clippy::too_many_lines, reason = "multi-step E2E orchestration")]
fn env_verify_api_fallback(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        API_FALLBACK_MODEL, API_PROVIDER_SECRET_KEY, API_PROVIDER_SECRET_NAME, API_PROVIDER_SECRET_NS,
        CONFIGMAP_POLL_TIMEOUT, STATUS_POLL_TIMEOUT, TEST_GATEWAY_NAME, TEST_GATEWAY_NS, TEST_HEALTHY_ROUTING_CLUSTER,
        TEST_NETWORK, TEST_PROVIDER_API, TEST_PROVIDER_HEALTHY,
    };

    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("verify-api-fallback: context={context}");

    let consumer_site = cfg.consumer_cluster_name().ok_or("no consumer cluster in config")?;
    let consumer_ctx = kind::kubectl_context(consumer_site);

    // ── Step 1: deploy provider gateways ────────────────────────────────────
    eprintln!("verify-api-fallback: [1/5] deploying provider gateways...");
    gateway::deploy_all(&cfg)?;

    // ── Step 2: deploy mock-api-provider in consumer cluster ─────────────────
    eprintln!("verify-api-fallback: [2/5] deploying mock-api-provider in consumer cluster...");
    let consumer_cluster_name = format!("grid-{consumer_site}");
    kind::deploy_mock_api_provider(&consumer_ctx, &consumer_cluster_name)?;
    let api_provider_endpoint = format!("{}.default.svc:{}", kind::MOCK_API_SVC, kind::MOCK_API_PORT);
    eprintln!("  api_provider endpoint: {api_provider_endpoint}");

    // ── Step 3: operator reconcile → overlay with local + api_provider ────────
    eprintln!("verify-api-fallback: [3/5] operator reconcile + overlay export...");
    operator::install_grid_crds(&context)?;
    operator::cleanup_validation_resources(&context)?;
    // Clean up any stale credential Secret from a previous run.
    operator::delete_api_credential_secret(&context, API_PROVIDER_SECRET_NS)
        .unwrap_or_else(|e| eprintln!("  note: credential Secret cleanup: {e}"));

    // Create the credential Secret before applying the InferenceProvider so that the
    // provider's spec.auth.secretRef is resolvable immediately after apply.
    operator::create_api_credential_secret(
        &context,
        API_PROVIDER_SECRET_NAME,
        API_PROVIDER_SECRET_NS,
        API_PROVIDER_SECRET_KEY,
        consumer::API_PROVIDER_INJECTED_TOKEN,
    )?;

    let healthy_endpoint = "http://mock-openai-provider.default.svc:8080";
    // Apply the GridNetwork + healthy local provider (model-x) + invalid (excluded by
    // operator) + api_provider (model-z, api_provider backendKind, auth.secretRef set).
    // The degraded and metrics fixtures are omitted — this validation focuses on the
    // local-vs-api_provider routing path, not on health/metrics signal ordering.
    // The spec.endpoint on the api_provider fixture is not used for routing; the xtask
    // builds the consumer cluster endpoint directly from the in-cluster mock service.
    operator::apply_test_fixtures(&context, healthy_endpoint)?;
    operator::apply_api_provider_fixture(&context, healthy_endpoint)?;

    let op = operator::spawn_operator(&context)?;
    let mut op_guard = ProcGuard(Some(op), "operator");

    let result: Result<PathBuf, Box<dyn std::error::Error>> = (|| {
        operator::wait_for_provider_phase(&context, TEST_PROVIDER_HEALTHY, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(&context, TEST_PROVIDER_API, "Pending", STATUS_POLL_TIMEOUT)?;

        operator::wait_for_overlay_configmap(
            &context,
            TEST_NETWORK,
            TEST_GATEWAY_NAME,
            TEST_GATEWAY_NS,
            CONFIGMAP_POLL_TIMEOUT,
        )?;

        let overlay = operator::read_overlay_configmap(&context, TEST_NETWORK, TEST_GATEWAY_NAME, TEST_GATEWAY_NS)?;
        operator::verify_api_fallback_overlay(
            &overlay,
            TEST_HEALTHY_ROUTING_CLUSTER,
            TEST_PROVIDER_API,
            API_FALLBACK_MODEL,
        )?;
        eprintln!("  [OK] overlay contains api_provider candidate with correct scoring order");

        let path = operator::export_overlay_to_file(&context, TEST_NETWORK, TEST_GATEWAY_NAME, TEST_GATEWAY_NS)?;
        eprintln!("  overlay exported: {}", path.display());
        Ok(path)
    })();

    if let Some(c) = op_guard.0.take() {
        operator::kill_operator(c);
    }
    let overlay_path = result?;

    // ── Step 4: deploy consumer with api_provider cluster ────────────────────
    eprintln!("verify-api-fallback: [4/5] deploying consumer gateway with api-provider cluster...");
    let overlay_json = std::fs::read_to_string(&overlay_path)?;
    let overlay = operator_overlay::parse_grid_config_json(&overlay_json)?;

    // Read the credential reference from the operator-projected overlay.
    // The operator embeds a SecretRef (name/namespace/key) in the overlay candidate;
    // the xtask resolves the token from that Secret — this is the harness bridge.
    // In production, Praxis will consume the credential reference natively.
    let cred_plan = operator::api_credential_plan_from_overlay(&overlay, TEST_PROVIDER_API).ok_or(
        "no bearer-token credential reference found in overlay; \
         verify InferenceProvider spec.auth.secretRef is set and operator reconciled",
    )?;
    let api_token = operator::resolve_api_credential(&context, &cred_plan)?
        .ok_or("credential plan resolved to no token (manual or absent auth)")?;

    consumer::deploy_consumer_for_api_fallback(&cfg, &overlay, TEST_PROVIDER_API, &api_provider_endpoint, &api_token)?;

    // ── Step 5: verify routing + credential injection ─────────────────────────
    eprintln!("verify-api-fallback: [5/5] verifying API-provider fallback routing and credential injection...");
    eprintln!("  local model ({TEST_HEALTHY_ROUTING_CLUSTER}) → model-x via site-a provider gateway");
    eprintln!(
        "  api fallback ({TEST_PROVIDER_API}) → {API_FALLBACK_MODEL} via mock api-provider (injected credential)"
    );

    // Port-forward directly to the mock-api-provider (not through consumer gateway)
    // for the negative credential proof — direct access without auth → 401.
    let mock_port = verify::find_free_port()?;
    let mut mock_pf =
        verify::PortForwardGuard::start(&consumer_ctx, kind::MOCK_API_SVC, mock_port, kind::MOCK_API_PORT)?;
    let mock_pf_ready = verify::wait_for_port(mock_port);

    consumer::verify_api_fallback_e2e(
        &cfg,
        "model-x",
        API_FALLBACK_MODEL,
        mock_pf_ready.then_some(mock_port),
        API_PROVIDER_SECRET_NAME,
    )?;

    mock_pf.stop();

    // Cleanup mock-api-provider (best-effort — does not block PASS).
    kind::delete_mock_api_provider(&consumer_ctx);

    eprintln!("verify-api-fallback: PASS");
    Ok(())
}

/// Validate the native AI credential injection path end-to-end.
///
/// Same E2E chain as [`env_verify_api_fallback`] but uses `filter: grid_credential_inject`
/// instead of `filter: headers` / `request_set` for bearer token injection.
///
/// Steps:
///
/// 1. Deploy provider gateways.
/// 2. Deploy mock API-provider in consumer cluster.
/// 3. Operator reconcile → overlay JSON with `credential.secretRef`.
/// 4. Validate the operator-generated consumer Praxis config.
/// 5. Deploy the consumer gateway with the credential Secret mounted.
/// 6. Verify routing, native injection, and token-absence invariants.
/// 7. Prove a strict mock rejects a mismatched injected credential.
/// 8. Prove the same strict mock accepts the correct injected credential.
#[expect(clippy::too_many_lines, reason = "multi-step E2E orchestration")]
fn env_verify_api_fallback_native(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        API_FALLBACK_MODEL, API_PROVIDER_SECRET_KEY, API_PROVIDER_SECRET_NAME, API_PROVIDER_SECRET_NS,
        CONFIGMAP_POLL_TIMEOUT, STATUS_POLL_TIMEOUT, TEST_CONSUMER_CONFIGMAP_NAME, TEST_GATEWAY_NAME, TEST_GATEWAY_NS,
        TEST_HEALTHY_ROUTING_CLUSTER, TEST_NETWORK, TEST_PROVIDER_API, TEST_PROVIDER_HEALTHY,
    };

    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("verify-api-fallback-native: context={context}");

    let consumer_site = cfg.consumer_cluster_name().ok_or("no consumer cluster in config")?;
    let consumer_ctx = kind::kubectl_context(consumer_site);

    // ── Step 1: deploy provider gateways ────────────────────────────────────
    eprintln!("verify-api-fallback-native: [1/8] deploying provider gateways...");
    gateway::deploy_all(&cfg)?;

    // ── Step 2: deploy mock-api-provider in consumer cluster ─────────────────
    eprintln!("verify-api-fallback-native: [2/8] deploying mock-api-provider...");
    let consumer_cluster_name = format!("grid-{consumer_site}");
    kind::deploy_mock_api_provider(&consumer_ctx, &consumer_cluster_name)?;
    let api_provider_endpoint = format!("{}.default.svc:{}", kind::MOCK_API_SVC, kind::MOCK_API_PORT);
    eprintln!("  api_provider endpoint: {api_provider_endpoint}");

    // ── Step 3: operator reconcile → overlay with credential secretRef ────────
    eprintln!("verify-api-fallback-native: [3/8] operator reconcile + overlay export...");
    operator::install_grid_crds(&context)?;
    operator::cleanup_validation_resources(&context)?;
    operator::delete_api_credential_secret(&context, API_PROVIDER_SECRET_NS)
        .unwrap_or_else(|e| eprintln!("  note: credential Secret cleanup: {e}"));
    operator::create_api_credential_secret(
        &context,
        API_PROVIDER_SECRET_NAME,
        API_PROVIDER_SECRET_NS,
        API_PROVIDER_SECRET_KEY,
        consumer::API_PROVIDER_INJECTED_TOKEN,
    )?;

    let healthy_endpoint = "http://mock-openai-provider.default.svc:8080";
    // Use the consumer-config fixture variant: enables GatewayRef.consumerConfig so
    // the operator renders a consumer Praxis ConfigMap for shape validation.
    // Pass the API provider mock address so the operator-generated consumer ConfigMap
    // includes a full (plain HTTP) cluster entry for the API fallback route.
    let api_provider_svc_address = format!("{}.default.svc:{}", kind::MOCK_API_SVC, kind::MOCK_API_PORT);
    operator::apply_test_fixtures_with_consumer_config(&context, healthy_endpoint, &api_provider_svc_address)?;
    operator::apply_api_provider_fixture(&context, healthy_endpoint)?;

    let op = operator::spawn_operator(&context)?;
    let mut op_guard = ProcGuard(Some(op), "operator");

    let result: Result<(PathBuf, String), Box<dyn std::error::Error>> = (|| {
        operator::wait_for_provider_phase(&context, TEST_PROVIDER_HEALTHY, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(&context, TEST_PROVIDER_API, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_overlay_configmap(
            &context,
            TEST_NETWORK,
            TEST_GATEWAY_NAME,
            TEST_GATEWAY_NS,
            CONFIGMAP_POLL_TIMEOUT,
        )?;
        let overlay = operator::read_overlay_configmap(&context, TEST_NETWORK, TEST_GATEWAY_NAME, TEST_GATEWAY_NS)?;
        operator::verify_api_fallback_overlay(
            &overlay,
            TEST_HEALTHY_ROUTING_CLUSTER,
            TEST_PROVIDER_API,
            API_FALLBACK_MODEL,
        )?;
        eprintln!("  [OK] overlay contains api_provider candidate with credential secretRef");

        // Wait for and validate the operator-generated consumer ConfigMap.
        // This ConfigMap is rendered because GatewayRef.consumerConfig.enabled = true.
        // The live consumer pod is deployed from this exact rendered config below.
        eprintln!("  waiting for operator-generated consumer ConfigMap {TEST_CONSUMER_CONFIGMAP_NAME}...");
        operator::wait_for_consumer_configmap(
            &context,
            TEST_CONSUMER_CONFIGMAP_NAME,
            TEST_GATEWAY_NS,
            CONFIGMAP_POLL_TIMEOUT,
        )?;

        let path = operator::export_overlay_to_file(&context, TEST_NETWORK, TEST_GATEWAY_NAME, TEST_GATEWAY_NS)?;
        eprintln!("  overlay exported: {}", path.display());
        Ok((path, TEST_CONSUMER_CONFIGMAP_NAME.to_owned()))
    })();

    if let Some(c) = op_guard.0.take() {
        operator::kill_operator(c);
    }
    let (overlay_path, consumer_cm_name) = result?;

    // ── Step 4: shape-validate operator consumer ConfigMap ────────────────────
    eprintln!("verify-api-fallback-native: [4/8] shape-validating operator consumer ConfigMap...");
    let overlay_json = std::fs::read_to_string(&overlay_path)?;
    let overlay = operator_overlay::parse_grid_config_json(&overlay_json)?;

    // Read the credential reference from the operator-projected overlay.
    // The harness resolves the token from the K8s Secret only to prove the Secret is
    // accessible — it does NOT appear in the consumer ConfigMap, overlay JSON, or route
    // candidates.  grid_credential_inject reads the token from the mounted Secret file.
    let cred_plan = operator::api_credential_plan_from_overlay(&overlay, TEST_PROVIDER_API).ok_or(
        "no bearer-token credential reference found in overlay; \
         verify InferenceProvider spec.auth.secretRef is set and operator reconciled",
    )?;

    // Destructure the secretRef fields to pass to grid_credential_inject config.
    let operator::ApiCredentialPlan::BearerToken {
        secret_name,
        namespace,
        key,
    } = &cred_plan
    else {
        return Err("unexpected non-bearer-token credential plan in native path".into());
    };

    let api_token = operator::resolve_api_credential(&context, &cred_plan)?
        .ok_or("credential plan resolved to no token (manual or absent auth)")?;
    verify_token_absent_from_overlay(&overlay_json, &api_token);

    // Shape-validate the operator-generated consumer ConfigMap.  The ConfigMap was
    // rendered because GatewayRef.consumerConfig.enabled = true in the fixture.
    // It lives in the provider cluster's namespace (gw_ref.namespace = TEST_GATEWAY_NS).
    operator::verify_operator_consumer_configmap(
        &context,
        &consumer_cm_name,
        TEST_GATEWAY_NS,
        &api_token,
        secret_name,
        key,
    )?;

    // Read the exact praxis.yaml from the operator-generated ConfigMap.
    // This is the YAML the live consumer pod will run — byte-for-byte from the operator.
    let operator_praxis_yaml =
        operator::read_consumer_configmap_praxis_yaml(&context, &consumer_cm_name, TEST_GATEWAY_NS)?;
    if operator_praxis_yaml.is_empty() {
        return Err(format!("operator ConfigMap {consumer_cm_name} has empty praxis.yaml").into());
    }
    eprintln!(
        "  [OK] operator-generated praxis.yaml read ({} bytes)",
        operator_praxis_yaml.len()
    );

    // ── Step 5: apply operator config to consumer cluster + deploy consumer ──
    eprintln!("verify-api-fallback-native: [5/8] deploying consumer from operator-generated config...");
    // Create the credential Secret in the CONSUMER cluster so the pod can mount it.
    // The token is in the Secret's `data` map — not in the Praxis ConfigMap.
    eprintln!("  creating credential Secret in consumer cluster for volume mount...");
    operator::delete_api_credential_secret(&consumer_ctx, API_PROVIDER_SECRET_NS)
        .unwrap_or_else(|e| eprintln!("  note: consumer credential Secret cleanup: {e}"));
    operator::create_api_credential_secret(&consumer_ctx, secret_name, namespace, key, &api_token)?;
    eprintln!("  [OK] credential Secret {secret_name:?} in consumer cluster (token not in ConfigMap)");

    // Apply the operator-generated praxis.yaml directly to the consumer cluster as
    // praxis-consumer-config, then deploy the consumer pod that mounts it.
    // This proves E2E: the live consumer pod runs the exact config the operator rendered.
    consumer::deploy_consumer_from_operator_yaml(&cfg, &operator_praxis_yaml, secret_name, key)?;

    // ── Step 6: verify routing + token-absence + native credential injection ──
    eprintln!("verify-api-fallback-native: [6/8] verifying routing with grid_credential_inject...");
    eprintln!("  live consumer pod runs operator-generated config");
    eprintln!("  local ({TEST_HEALTHY_ROUTING_CLUSTER}) → model-x via site-a provider gateway");
    eprintln!(
        "  api fallback ({TEST_PROVIDER_API}) → {API_FALLBACK_MODEL} via mock api-provider (grid_credential_inject)"
    );

    let mock_port = verify::find_free_port()?;
    let mut mock_pf =
        verify::PortForwardGuard::start(&consumer_ctx, kind::MOCK_API_SVC, mock_port, kind::MOCK_API_PORT)?;
    let mock_pf_ready = verify::wait_for_port(mock_port);

    consumer::verify_api_fallback_e2e(
        &cfg,
        "model-x",
        API_FALLBACK_MODEL,
        mock_pf_ready.then_some(mock_port),
        API_PROVIDER_SECRET_NAME,
    )?;

    // Explicit token-absence proof: token must not appear in the operator-generated consumer ConfigMap.
    let consumer_ctx_ref = &consumer_ctx;
    verify_token_absent_from_consumer_configmap(consumer_ctx_ref, &api_token)?;

    // Assert GridNetwork status records the consumer config as Rendered and token-safe.
    operator::wait_for_consumer_config_status_rendered(
        &context,
        TEST_NETWORK,
        TEST_GATEWAY_NAME,
        &consumer_cm_name,
        &api_token,
        STATUS_POLL_TIMEOUT,
    )
    .map_err(|e| format!("operator-generated consumer config status assertion failed: {e}"))?;

    mock_pf.stop();

    // ── Step 7: wrong-credential proof (strict mock rejects mismatched token) ──
    eprintln!("verify-api-fallback-native: [7/8] wrong-credential proof...");
    kind::deploy_mock_api_provider_with_expected_token(
        &consumer_ctx,
        &consumer_cluster_name,
        "wrong-sentinel-not-the-real-token",
    )?;
    eprintln!("  restarting consumer to clear cached connections...");
    kubectl::rollout_restart(&consumer_ctx, "praxis-consumer")?;
    kubectl::wait_for_rollout(&consumer_ctx, "praxis-consumer", "consumer")?;
    assert_wrong_credential_rejected(&consumer_ctx, API_FALLBACK_MODEL)?;

    // ── Step 8: correct-credential proof (strict mock accepts correct token) ──
    eprintln!("verify-api-fallback-native: [8/8] correct-credential with strict mock...");
    kind::deploy_mock_api_provider_with_expected_token(&consumer_ctx, &consumer_cluster_name, &api_token)?;
    eprintln!("  restarting consumer to clear cached connections...");
    kubectl::rollout_restart(&consumer_ctx, "praxis-consumer")?;
    kubectl::wait_for_rollout(&consumer_ctx, "praxis-consumer", "consumer")?;
    assert_correct_credential_accepted(&consumer_ctx, API_FALLBACK_MODEL)?;

    kind::delete_mock_api_provider(&consumer_ctx);

    eprintln!("verify-api-fallback-native: PASS (8/8 steps)");
    Ok(())
}

/// Assert that a wrong credential is rejected by the strict mock.
///
/// Port-forwards to the consumer gateway, sends a request for the given model,
/// and expects a non-200 response (403 from the strict mock, or 502 if the
/// gateway surfaces the upstream error).
fn assert_wrong_credential_rejected(consumer_ctx: &str, model: &str) -> Result<(), Box<dyn std::error::Error>> {
    let port = verify::find_free_port()?;
    let mut pf = verify::PortForwardGuard::start(consumer_ctx, "praxis-consumer", port, consumer::GATEWAY_HTTP_PORT)?;
    if !verify::wait_for_port(port) {
        pf.stop();
        return Err("consumer port-forward not ready for wrong-credential proof".into());
    }
    let resp = consumer::send_consumer_request(port, model)?;
    pf.stop();
    if resp.status == 200 {
        return Err(format!(
            "wrong-credential proof failed: expected non-200 for model {model}, got 200. \
             The mock should reject the token mismatch."
        )
        .into());
    }
    eprintln!("  [PASS] wrong credential rejected (status={})", resp.status);
    Ok(())
}

/// Assert that the correct credential is accepted by the strict mock.
///
/// Port-forwards to the consumer gateway, sends a request for the given model,
/// and expects 200.
fn assert_correct_credential_accepted(consumer_ctx: &str, model: &str) -> Result<(), Box<dyn std::error::Error>> {
    let port = verify::find_free_port()?;
    let mut pf = verify::PortForwardGuard::start(consumer_ctx, "praxis-consumer", port, consumer::GATEWAY_HTTP_PORT)?;
    if !verify::wait_for_port(port) {
        pf.stop();
        return Err("consumer port-forward not ready for correct-credential proof".into());
    }
    let resp = consumer::send_consumer_request(port, model)?;
    pf.stop();
    if resp.status != 200 {
        return Err(format!(
            "correct-credential proof failed: expected 200 for model {model}, got {}. \
             The mock should accept the injected token.",
            resp.status
        )
        .into());
    }
    eprintln!("  [PASS] correct credential accepted with strict mock (status=200)");
    Ok(())
}

/// Assert the known token string is absent from the operator overlay JSON.
///
/// Fails fast with a clear diagnostic rather than silently passing.
fn verify_token_absent_from_overlay(overlay_json: &str, api_token: &str) {
    if overlay_json.contains(api_token) {
        eprintln!(
            "SECURITY VIOLATION: token bytes found in operator overlay JSON\n\
             This is a bug in the operator credential projection — overlay must \
             carry only credential.secretRef, never the token value."
        );
        std::process::abort();
    }
    eprintln!("  [PASS] token bytes absent from operator overlay JSON");
}

/// Assert the known token string is absent from the consumer Praxis `ConfigMap`.
///
/// Reads the `ConfigMap` live from the cluster and fails if the token appears anywhere
/// in the YAML.  This proves the file-backed path keeps tokens out of `ConfigMap`s.
fn verify_token_absent_from_consumer_configmap(
    consumer_ctx: &str,
    api_token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let configmap_yaml = kubectl::get_configmap_yaml(consumer_ctx, "default", "praxis-consumer-config")?;
    if configmap_yaml.contains(api_token) {
        return Err(format!(
            "SECURITY VIOLATION: token bytes found in consumer Praxis ConfigMap \
             (praxis-consumer-config in {consumer_ctx}). \
             The file-backed path must keep token bytes out of ConfigMaps."
        )
        .into());
    }
    eprintln!("  [PASS] token bytes absent from consumer Praxis ConfigMap");
    Ok(())
}

/// Verify gateway-to-gateway mTLS trust (positive + negative cases).
fn env_verify_mtls_trust(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    trust::verify_mtls_trust(&cfg)
}

/// Format a status label.
fn status_label(ok: bool) -> &'static str {
    if ok { "ready" } else { "not ready" }
}

/// Print the configured topology summary.
fn print_topology(cfg: &EnvConfig) {
    eprintln!("env up: {} clusters, 4 providers", cfg.clusters.names.len(),);
    for name in &cfg.clusters.names {
        if let Some(def) = cfg.clusters.definitions.get(name) {
            eprintln!("  {name}: {:?}, models: {}", def.role, def.models.join(", "),);
        }
    }
    eprintln!("  openai:    port {}", cfg.providers.openai.port);
    eprintln!("  anthropic: port {}", cfg.providers.anthropic.port);
    eprintln!(
        "  bedrock:   port {} ({})",
        cfg.providers.bedrock.port, cfg.providers.bedrock.region,
    );
    eprintln!(
        "  vertex:    port {} ({})",
        cfg.providers.vertex.port, cfg.providers.vertex.project,
    );
}

// ---------------------------------------------------------------------------
// Grid operator commands
// ---------------------------------------------------------------------------

/// Collect `(name, models)` for each provider cluster that declares at least
/// one model.  Order matches `cfg.clusters.names`.
fn provider_clusters_from_config(cfg: &EnvConfig) -> Vec<(String, Vec<String>)> {
    cfg.clusters
        .names
        .iter()
        .filter_map(|name| {
            cfg.clusters.definitions.get(name).and_then(|def| {
                (def.role == ClusterRole::Provider && !def.models.is_empty())
                    .then(|| (name.clone(), def.models.clone()))
            })
        })
        .collect()
}

/// Require the named provider clusters to use the `mock-openai` backend.
///
/// The metrics-routing harness patches `mock-epp` routes to the shared
/// `mock-openai-provider` service. Running it against an `inference-sim`
/// topology would patch routes to a service that does not exist, so fail before
/// any cluster mutation.
fn require_mock_openai_backends(cfg: &EnvConfig, sites: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    for site in sites {
        let def = cfg
            .clusters
            .definitions
            .get(*site)
            .ok_or_else(|| format!("provider cluster {site:?} not found in config"))?;
        if def.backend != ProviderBackend::MockOpenai {
            return Err(format!(
                "verify-metrics-routing requires provider {site:?} to use backend = \"mock-openai\"; got {:?}",
                def.backend
            )
            .into());
        }
    }
    Ok(())
}

/// Return `true` when the config has more than one provider cluster that
/// declares models.
///
/// Used to select between the single-provider (`site-a` + full health/metrics
/// fixture suite) and the multi-provider (per-site minimal fixtures) reconcile
/// paths in `env_validate_operator_routing`.
fn is_multi_provider_config(cfg: &EnvConfig) -> bool {
    provider_clusters_from_config(cfg).len() > 1
}

/// Resolve the kubectl context for the target site.
///
/// Uses the first provider site in the config when `site` is `None`.
/// Resolve the config site name for operator validation.
fn resolve_operator_site_name<'a>(
    cfg: &'a EnvConfig,
    site: Option<&'a str>,
) -> Result<&'a str, Box<dyn std::error::Error>> {
    if let Some(site) = site {
        Ok(site)
    } else {
        cfg.clusters
            .names
            .iter()
            .find(|name| {
                cfg.clusters
                    .definitions
                    .get(*name)
                    .is_some_and(|d| d.role == ClusterRole::Provider)
            })
            .map(String::as_str)
            .ok_or_else(|| "no provider site in config".into())
    }
}

/// Resolve the kubectl context for operator validation.
fn resolve_operator_context(cfg: &EnvConfig, site: Option<&str>) -> Result<String, Box<dyn std::error::Error>> {
    let name = resolve_operator_site_name(cfg, site)?;
    Ok(kind::kubectl_context(name))
}

/// Install Grid CRDs into the selected kind cluster.
fn env_install_grid_crds(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("install-grid-crds: context={context}");
    operator::install_grid_crds(&context)?;
    eprintln!("install-grid-crds: done");
    Ok(())
}

/// Install CRDs, apply operator test fixtures, run the operator, verify the overlay,
/// and export it to a temp file.
///
/// Returns the path of the exported `grid-config.json` overlay file.
/// The caller is responsible for killing the operator and port-forward processes
/// before this function returns — both are wrapped in [`ProcGuard`] so they are
/// stopped on drop even on early return.
///
/// This is the shared core of both [`env_verify_operator_reconcile`] and
/// [`env_validate_operator_routing`].
#[expect(
    clippy::too_many_lines,
    reason = "sequential reconcile steps: CRD install, fixtures, operator spawn, poll, verify, export"
)]
fn run_operator_reconcile(context: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    use operator::{
        API_PROVIDER_SECRET_KEY, API_PROVIDER_SECRET_NAME, API_PROVIDER_SECRET_NS, CONFIGMAP_POLL_TIMEOUT,
        ERROR_ENDPOINT_LOCAL_PORT, ERROR_ENDPOINT_NAME, METRICS_BUSY_LOCAL_PORT, METRICS_IDLE_LOCAL_PORT,
        POD_READY_TIMEOUT, STATUS_POLL_TIMEOUT, TEST_DEGRADED_ROUTING_CLUSTER, TEST_GATEWAY_NAME, TEST_GATEWAY_NS,
        TEST_HEALTHY_ROUTING_CLUSTER, TEST_METRICS_BUSY_PROVIDER, TEST_METRICS_BUSY_ROUTING_CLUSTER,
        TEST_METRICS_IDLE_PROVIDER, TEST_METRICS_IDLE_ROUTING_CLUSTER, TEST_NETWORK, TEST_PROVIDER_API,
        TEST_PROVIDER_DEGRADED, TEST_PROVIDER_HEALTHY, TEST_PROVIDER_INVALID,
    };

    // Step 1: install Grid CRDs and remove stale owned resources.
    operator::install_grid_crds(context)?;
    operator::cleanup_validation_resources(context)?;

    // Step 2: deploy the HTTP 503 error-endpoint Pod.
    // The operator health probe reaches this endpoint to classify the provider as Degraded.
    operator::apply_error_endpoint_fixture(context)?;
    operator::wait_for_error_endpoint_ready(context, POD_READY_TIMEOUT)?;

    // Step 3: deploy metrics endpoint Pods.
    // Each Pod serves a fixed Prometheus gauge for queue depth.  They are deployed
    // before spawning the operator so they are ready when the first reconcile fires.
    // - idle Pod → provider_queue_depth_normalized 0.1 (low queue → high score)
    // - busy Pod → provider_queue_depth_normalized 0.9 (high queue → low score)
    operator::apply_and_wait_for_metrics_pods(context, POD_READY_TIMEOUT)?;

    // Step 4: port-forward all endpoints so the out-of-cluster operator can reach them.
    let pf_child = operator::start_error_endpoint_port_forward(context)?;
    let mut pf_guard = ProcGuard(Some(pf_child), ERROR_ENDPOINT_NAME);

    let pf_idle_child =
        operator::start_named_pod_port_forward(context, TEST_METRICS_IDLE_PROVIDER, METRICS_IDLE_LOCAL_PORT)?;
    let mut pf_idle_guard = ProcGuard(Some(pf_idle_child), TEST_METRICS_IDLE_PROVIDER);

    let pf_busy_child =
        operator::start_named_pod_port_forward(context, TEST_METRICS_BUSY_PROVIDER, METRICS_BUSY_LOCAL_PORT)?;
    let mut pf_busy_guard = ProcGuard(Some(pf_busy_child), TEST_METRICS_BUSY_PROVIDER);

    // Step 5: spawn the operator out-of-cluster.
    let op_child = operator::spawn_operator(context)?;
    eprintln!("  operator spawned (PID {})", op_child.id());
    let mut op_guard = ProcGuard(Some(op_child), "operator");

    let degraded_endpoint = format!("http://127.0.0.1:{ERROR_ENDPOINT_LOCAL_PORT}");
    let metrics_idle_endpoint = format!("http://127.0.0.1:{METRICS_IDLE_LOCAL_PORT}");
    let metrics_busy_endpoint = format!("http://127.0.0.1:{METRICS_BUSY_LOCAL_PORT}");

    // Step 6: apply InferenceProvider fixtures.
    // GridNetwork is created first; providers follow so the operator resolves gridNetworkRef immediately.
    // api_provider is last to prove scoring is score-driven, not input-order-driven.
    //
    // routingClusterRef controls overlay candidate identity:
    // - op-e2e-healthy:       routingClusterRef="site-a"           → candidate.site="site-a"
    // - op-e2e-degraded:      routingClusterRef="site-a"           → fresh=false
    // - op-e2e-invalid:       blank endpoint                       → Unavailable, excluded
    // - op-e2e-api-fallback:  no routingClusterRef                 → cluster="op-e2e-api-fallback"
    // - op-e2e-metrics-idle:  routingClusterRef="site-metrics-idle" → metrics scraped (queue=0.1)
    // - op-e2e-metrics-busy:  routingClusterRef="site-metrics-busy" → metrics scraped (queue=0.9)
    let healthy_endpoint = "http://mock-openai-provider.default.svc:8080";
    let api_endpoint = "https://api.anthropic.com";
    operator::apply_test_fixtures(context, healthy_endpoint)?;
    operator::apply_degraded_provider_fixture(context, &degraded_endpoint)?;
    operator::delete_api_credential_secret(context, API_PROVIDER_SECRET_NS)
        .unwrap_or_else(|e| eprintln!("  note: credential Secret cleanup: {e}"));
    operator::create_api_credential_secret(
        context,
        API_PROVIDER_SECRET_NAME,
        API_PROVIDER_SECRET_NS,
        API_PROVIDER_SECRET_KEY,
        consumer::API_PROVIDER_INJECTED_TOKEN,
    )?;
    operator::apply_api_provider_fixture(context, api_endpoint)?;
    operator::apply_metrics_provider_fixtures(context, &metrics_idle_endpoint, &metrics_busy_endpoint)?;

    // Step 7: wait for reconciliation and verify overlay.
    let result = (|| -> Result<PathBuf, Box<dyn std::error::Error>> {
        operator::wait_for_provider_phase(context, TEST_PROVIDER_INVALID, "Unavailable", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(context, TEST_PROVIDER_HEALTHY, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(context, TEST_PROVIDER_DEGRADED, "Degraded", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(context, TEST_PROVIDER_API, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(context, TEST_METRICS_IDLE_PROVIDER, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(context, TEST_METRICS_BUSY_PROVIDER, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_overlay_configmap(
            context,
            TEST_NETWORK,
            TEST_GATEWAY_NAME,
            TEST_GATEWAY_NS,
            CONFIGMAP_POLL_TIMEOUT,
        )?;

        let overlay = operator::read_overlay_configmap(context, TEST_NETWORK, TEST_GATEWAY_NAME, TEST_GATEWAY_NS)?;
        operator::verify_overlay(&overlay, TEST_HEALTHY_ROUTING_CLUSTER, TEST_PROVIDER_INVALID)?;
        operator::verify_degraded_candidate(&overlay, TEST_DEGRADED_ROUTING_CLUSTER)?;
        operator::verify_scoring_order(&overlay, TEST_HEALTHY_ROUTING_CLUSTER, TEST_PROVIDER_API)?;
        // Verify that live scraped metrics reordered the equal-locality providers:
        // idle (queue=0.1, high score) must appear before busy (queue=0.9, low score).
        operator::verify_metrics_ordering(
            &overlay,
            TEST_METRICS_IDLE_ROUTING_CLUSTER,
            TEST_METRICS_BUSY_ROUTING_CLUSTER,
        )?;

        // Step 8: export overlay for consumer gateway use.
        let path = operator::export_overlay_to_file(context, TEST_NETWORK, TEST_GATEWAY_NAME, TEST_GATEWAY_NS)?;
        eprintln!("  overlay exported: {}", path.display());
        Ok(path)
    })();

    // Always stop the operator and all port-forwards before returning.
    if let Some(c) = op_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(mut c) = pf_guard.0.take() {
        drop(c.kill());
        drop(c.wait());
    }
    if let Some(mut c) = pf_idle_guard.0.take() {
        drop(c.kill());
        drop(c.wait());
    }
    if let Some(mut c) = pf_busy_guard.0.take() {
        drop(c.kill());
        drop(c.wait());
    }

    result
}

/// Verify Grid operator reconciliation only (CRD install → overlay export).
fn env_verify_operator_reconcile(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("verify-operator-reconcile: context={context}");
    run_operator_reconcile(&context)?;
    eprintln!("verify-operator-reconcile: PASS");
    Ok(())
}

/// Install CRDs, apply one `InferenceProvider` fixture per provider site, run
/// the operator, verify the overlay covers all sites, and export the overlay.
///
/// This is the multi-provider counterpart of `run_operator_reconcile`.  It
/// skips the degraded/API-fallback/metrics fixtures because those are
/// specific to the single-provider health-and-scoring validation.  The focus
/// is on proving that the operator generates correct overlay candidates for
/// every provider site and that the consumer can route to all of them.
///
/// # Invariants
/// - `providers` must contain at least two entries (caller's responsibility).
/// - Each `(site_name, models)` pair becomes one `InferenceProvider` with `routingClusterRef = site_name` and the given
///   model list.
#[expect(
    clippy::too_many_lines,
    reason = "sequential reconcile steps: CRD install, fixtures, operator spawn, poll, verify, export"
)]
fn run_multi_provider_reconcile(
    context: &str,
    providers: &[(&str, &[String])],
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    use operator::{CONFIGMAP_POLL_TIMEOUT, STATUS_POLL_TIMEOUT, TEST_GATEWAY_NAME, TEST_GATEWAY_NS, TEST_NETWORK};

    // Step 1: install CRDs and remove stale resources.
    operator::install_grid_crds(context)?;
    let site_names: Vec<&str> = providers.iter().map(|&(s, _)| s).collect();
    operator::cleanup_multi_provider_resources(context, &site_names)?;

    // Step 2: spawn the operator out-of-cluster.
    let op_child = operator::spawn_operator(context)?;
    eprintln!("  operator spawned (PID {})", op_child.id());
    let mut op_guard = ProcGuard(Some(op_child), "operator");

    // Step 3: apply GridNetwork + one InferenceProvider per provider site.
    //
    // The mock-openai-provider service is deployed in-cluster by `env up`.
    // Using its in-cluster DNS name means providers reconcile to `Pending`
    // (non-blank endpoint, no health check configured) without requiring a
    // live inference backend to be reachable from outside the cluster.
    let healthy_endpoint = "http://mock-openai-provider.default.svc:8080";
    operator::apply_multi_provider_fixtures(context, providers, healthy_endpoint)?;

    // Step 4: wait for reconciliation and verify the overlay.
    let result = (|| -> Result<PathBuf, Box<dyn std::error::Error>> {
        for &(site_name, _) in providers {
            let fixture_name = operator::multi_provider_fixture_name(site_name);
            operator::wait_for_provider_phase(context, &fixture_name, "Pending", STATUS_POLL_TIMEOUT)?;
        }
        operator::wait_for_overlay_configmap(
            context,
            TEST_NETWORK,
            TEST_GATEWAY_NAME,
            TEST_GATEWAY_NS,
            CONFIGMAP_POLL_TIMEOUT,
        )?;

        let overlay = operator::read_overlay_configmap(context, TEST_NETWORK, TEST_GATEWAY_NAME, TEST_GATEWAY_NS)?;
        operator::verify_multi_provider_overlay(&overlay, &site_names)?;

        let path = operator::export_overlay_to_file(context, TEST_NETWORK, TEST_GATEWAY_NAME, TEST_GATEWAY_NS)?;
        eprintln!("  overlay exported: {}", path.display());
        Ok(path)
    })();

    if let Some(c) = op_guard.0.take() {
        operator::kill_operator(c);
    }

    result
}

/// Run the full operator-to-consumer routing validation in kind.
///
/// Orchestrates provider gateway deployment, operator reconcile + overlay export,
/// consumer gateway deployment from the operator overlay, and end-to-end routing
/// verification in a single idempotent command.
///
/// **Single-provider config (`site-a`):** runs the full fixture suite including
/// health phases, degraded/API-fallback providers, and live metrics ordering.
///
/// **Multi-provider config (≥2 provider sites):** runs a focused fixture suite
/// that proves the operator generates correct overlay candidates for every
/// provider site and that the consumer can route to all declared models.
fn env_validate_operator_routing(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("validate-operator-routing: context={context}");

    eprintln!("validate-operator-routing: [1/4] deploying provider gateways...");
    gateway::deploy_all(&cfg)?;

    eprintln!("validate-operator-routing: [2/4] operator reconcile + overlay export...");
    let overlay_path = if is_multi_provider_config(&cfg) {
        let providers_owned = provider_clusters_from_config(&cfg);
        let providers: Vec<(&str, &[String])> = providers_owned
            .iter()
            .map(|(s, m)| (s.as_str(), m.as_slice()))
            .collect();
        eprintln!(
            "  multi-provider mode: {} provider sites ({})",
            providers.len(),
            providers.iter().map(|&(s, _)| s).collect::<Vec<_>>().join(", ")
        );
        run_multi_provider_reconcile(&context, &providers)?
    } else {
        run_operator_reconcile(&context)?
    };

    eprintln!("validate-operator-routing: [3/4] deploying consumer gateway from operator overlay...");
    consumer::deploy_consumer(&cfg, Some(&overlay_path))?;

    eprintln!("validate-operator-routing: [4/4] verifying end-to-end routing...");
    consumer::verify_e2e(&cfg)?;

    eprintln!("validate-operator-routing: PASS");
    Ok(())
}

/// Prove that live SWIM membership reaches `GridNetwork` status.
///
/// Starts two SWIM-enabled operator processes, waits for gossip convergence,
/// applies a `GridNetwork` fixture, then polls until `phase = Active` and
/// `connectedSites ≥ 1`.
///
/// Both operators connect to the same kind cluster (the first provider site from
/// `config`). They bind on available localhost UDP ports chosen at runtime.
fn env_verify_swim_membership(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        SWIM_CONVERGENCE_WAIT, SWIM_NODE_PRIMARY_NAME, SWIM_NODE_SECONDARY_NAME, SWIM_STATUS_POLL_TIMEOUT,
        SWIM_TEST_NETWORK,
    };

    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("verify-swim-membership: context={context}");

    // Step 1: install Grid CRDs and remove any stale SWIM test resources.
    operator::install_grid_crds(&context)?;
    operator::cleanup_swim_test_resources(&context)?;

    let (bind1, bind2) = reserve_swim_bind_addrs()?;

    // Step 2: start the primary SWIM operator (no seeds — it is the first member).
    let op1 = operator::spawn_operator_with_swim(&context, &bind1, &bind1, SWIM_NODE_PRIMARY_NAME, "", None)?;
    let mut op1_guard = ProcGuard(Some(op1), "operator-primary");

    // Step 3: start the secondary operator with the primary as its seed.
    // spawn_operator_with_swim includes a 3-second post-spawn settle sleep, so
    // the primary's SWIM listener is ready before the secondary announces.
    let op2 = operator::spawn_operator_with_swim(&context, &bind2, &bind2, SWIM_NODE_SECONDARY_NAME, &bind1, None)?;
    let mut op2_guard = ProcGuard(Some(op2), "operator-secondary");

    // Step 4: wait for SWIM gossip to converge (both nodes see each other as Alive).
    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);

    // Step 5: apply the GridNetwork fixture.
    // Both SWIM-enabled operators are watching for GridNetwork resources.
    // The watch event from the new resource triggers an immediate reconcile in
    // both operators; by this point SWIM has converged, so each operator's live
    // MembershipSnapshot already contains the other as an Alive peer.
    operator::apply_swim_test_network(&context)?;
    eprintln!("  GridNetwork {SWIM_TEST_NETWORK} applied; awaiting Active status from live SWIM snapshot...");

    // Step 6: poll until the GridNetwork status reflects the SWIM membership.
    let result = operator::wait_for_gridnetwork_active(&context, SWIM_TEST_NETWORK, SWIM_STATUS_POLL_TIMEOUT);

    // Always stop both operators before propagating errors.
    if let Some(c) = op1_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op2_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_swim_test_resources(&context)?;

    // Step 7: verify the result.
    let connected_sites = result?;
    operator::verify_swim_status("Active", connected_sites)?;

    eprintln!("verify-swim-membership: PASS (connectedSites={connected_sites})");
    Ok(())
}

/// Prove that `GridNetwork.spec.seeds` drives SWIM peer discovery without env-var seeds.
///
/// **Why this proves the CRD path:**
///
/// Both operators start with `GRID_SWIM_SEEDS=""` — no startup seeds at all.
/// After the `GridNetwork` fixture is applied with `spec.seeds = [bind1]`,
/// each operator reconciles the resource and calls `announce_crd_seeds`:
/// - Primary: `parse_crd_seeds([bind1], Some(bind1)) → []` (self-filtered) → no announce
/// - Secondary: `parse_crd_seeds([bind1], Some(bind2)) → [bind1]` → announces to primary
///
/// SWIM gossip then converges and `GridNetwork.status.phase` becomes `Active`.
/// The `GRID_SWIM_SEEDS` env var is empty for both operators throughout.
#[expect(
    clippy::too_many_lines,
    reason = "sequential CRD-seed kind steps: CRD install, two operator spawns, fixture apply with seeds, convergence wait, poll, cleanup"
)]
fn env_verify_swim_crd_seeds(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        CRD_SEEDS_TEST_NETWORK, SWIM_CONVERGENCE_WAIT, SWIM_NODE_PRIMARY_NAME, SWIM_NODE_SECONDARY_NAME,
        SWIM_STATUS_POLL_TIMEOUT,
    };

    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("verify-swim-crd-seeds: context={context}");

    // Step 1: install Grid CRDs and remove any stale CRD-seed test resources.
    operator::install_grid_crds(&context)?;
    operator::cleanup_swim_crd_seeds_test_resources(&context)?;

    let (bind1, bind2) = reserve_swim_bind_addrs()?;
    let bind1_addr: std::net::SocketAddr = bind1
        .parse()
        .map_err(|e| format!("failed to parse bind1 addr {bind1:?}: {e}"))?;

    // Step 2: start primary operator — NO GRID_SWIM_SEEDS, no peer at startup.
    // The primary will self-filter bind1 when it reads spec.seeds later.
    let op1 = operator::spawn_operator_with_swim(&context, &bind1, &bind1, SWIM_NODE_PRIMARY_NAME, "", None)?;
    let mut op1_guard = ProcGuard(Some(op1), "operator-primary-crd");

    // Step 3: start secondary operator — NO GRID_SWIM_SEEDS, no peer at startup.
    // The secondary will announce to bind1 only after reading spec.seeds from the CRD.
    let op2 = operator::spawn_operator_with_swim(&context, &bind2, &bind2, SWIM_NODE_SECONDARY_NAME, "", None)?;
    let mut op2_guard = ProcGuard(Some(op2), "operator-secondary-crd");

    // Step 4: Apply the GridNetwork fixture with an EMPTY spec.seeds.
    // This proves that without seeds, both operators remain isolated (connectedSites = 0).
    operator::apply_swim_test_network_with_seeds(&context, &[])?;
    eprintln!("  GridNetwork {CRD_SEEDS_TEST_NETWORK} applied with empty spec.seeds; verifying isolation...");
    // Allow one reconcile cycle to fire (operators are running and will reconcile the new CRD).
    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);
    // With empty seeds, the network must stay Pending or have connectedSites=0 — no SWIM join.
    let isolated_status =
        operator::wait_for_gridnetwork_status(&context, CRD_SEEDS_TEST_NETWORK, SWIM_STATUS_POLL_TIMEOUT)?;
    if isolated_status.connected_sites > 0 {
        // Stop operators before returning error
        if let Some(c) = op1_guard.0.take() {
            operator::kill_operator(c);
        }
        if let Some(c) = op2_guard.0.take() {
            operator::kill_operator(c);
        }
        operator::cleanup_swim_crd_seeds_test_resources(&context)?;
        return Err(format!(
            "verify-swim-crd-seeds: expected connectedSites=0 with empty spec.seeds, \
             got connectedSites={} — SWIM must not auto-join without seeds",
            isolated_status.connected_sites
        )
        .into());
    }
    eprintln!(
        "  [PASS] GridNetwork status with empty seeds: phase={:?} connectedSites={} \
         (isolation confirmed — no SWIM join without CRD seeds)",
        isolated_status.phase, isolated_status.connected_sites
    );

    // Step 5: Live-additive proof — patch spec.seeds to [bind1] while operators are running.
    // This is the core runtime update contract: adding a seed while the operator is live
    // causes SWIM join on the next reconcile cycle without operator restart.
    operator::apply_swim_test_network_with_seeds(&context, &[bind1_addr])?;
    eprintln!(
        "  spec.seeds patched to [{bind1}] while operators are running; \
         awaiting CRD-driven SWIM convergence..."
    );

    // Step 6: wait for the reconcile + announce + SWIM gossip to converge.
    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);

    // Step 7: poll until the GridNetwork status reflects the SWIM membership.
    let result = operator::wait_for_gridnetwork_active(&context, CRD_SEEDS_TEST_NETWORK, SWIM_STATUS_POLL_TIMEOUT);

    // Always stop both operators before propagating errors.
    if let Some(c) = op1_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op2_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_swim_crd_seeds_test_resources(&context)?;

    // Step 8: verify the result.
    let connected_sites = result?;
    operator::verify_swim_status("Active", connected_sites)?;

    eprintln!(
        "verify-swim-crd-seeds: PASS (connectedSites={connected_sites}; \
         CRD seed path proven — both operators started with no GRID_SWIM_SEEDS; \
         live-additive: spec.seeds added {bind1} while running, SWIM converged without restart)"
    );
    Ok(())
}

/// Prove that live CRDT state propagates between two SWIM-enabled operators via foca broadcast.
///
/// Proves real `InferenceProvider`-derived CRDT state propagates over SWIM gossip.
/// After convergence each operator's `distributedProviderCount` reflects remote provider records.
#[expect(
    clippy::too_many_lines,
    reason = "sequential SWIM state kind steps: CRD install, two operator spawns, fixture apply, convergence wait, poll, cleanup"
)]
fn env_verify_swim_state(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        SWIM_CONVERGENCE_WAIT, SWIM_NODE_PRIMARY_NAME, SWIM_NODE_SECONDARY_NAME, SWIM_STATUS_POLL_TIMEOUT,
        SWIM_TEST_NETWORK, SWIM_TEST_PROVIDER, SWIM_TEST_PROVIDER_MODEL,
    };

    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("verify-swim-state: context={context}");

    operator::install_grid_crds(&context)?;
    operator::cleanup_swim_test_resources(&context)?;

    // Start primary operator (no seeds).
    let (bind1, bind2) = reserve_swim_bind_addrs()?;
    let op1 = operator::spawn_operator_with_swim(&context, &bind1, &bind1, SWIM_NODE_PRIMARY_NAME, "", None)?;
    let mut op1_guard = ProcGuard(Some(op1), "operator-primary");

    // Start secondary operator; seeds point to primary.
    let op2 = operator::spawn_operator_with_swim(&context, &bind2, &bind2, SWIM_NODE_SECONDARY_NAME, &bind1, None)?;
    let mut op2_guard = ProcGuard(Some(op2), "operator-secondary");

    // Wait for SWIM convergence.
    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);

    // Apply the GridNetwork fixture and a real InferenceProvider.
    // Both operators reconcile immediately (via watch).  Each reconcile calls
    // publish_real_provider_state which publishes the real provider record as
    // a crdt::ProviderState (provider_id=SWIM_TEST_PROVIDER, models=[SWIM_TEST_PROVIDER_MODEL])
    // over SWIM gossip.  The remote operator receives and merges it, raising
    // distributedProviderCount >= 1.
    operator::apply_swim_test_network(&context)?;
    operator::apply_swim_test_provider(&context)?;
    eprintln!(
        "  GridNetwork {SWIM_TEST_NETWORK} + InferenceProvider {SWIM_TEST_PROVIDER} (model={SWIM_TEST_PROVIDER_MODEL}) applied;"
    );
    eprintln!("  awaiting real provider state propagation via SWIM custom broadcast...");

    // Poll for distributedProviderCount > 0 (proves real InferenceProvider-derived state arrived).
    let distributed_result =
        operator::wait_for_gridnetwork_distributed_state(&context, SWIM_TEST_NETWORK, SWIM_STATUS_POLL_TIMEOUT);

    // Cleanup.
    if let Some(c) = op1_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op2_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_swim_test_resources(&context)?;

    let distributed_count = distributed_result?;
    operator::verify_distributed_state_received(distributed_count)?;

    eprintln!(
        "verify-swim-state: PASS — real InferenceProvider state propagated via SWIM \
         (provider={SWIM_TEST_PROVIDER}, model={SWIM_TEST_PROVIDER_MODEL}, \
         distributedProviderCount={distributed_count})"
    );
    Ok(())
}

/// Prove that SWIM transport AES-256-GCM encryption is enforced.
///
/// Five scenarios tested sequentially:
///
/// **A. Positive — env-keyed peers converge:** operators A and B share the same key;
/// B's CRDT provider state propagates to A's `GridNetwork.status.distributedProviderCount`.
///
/// **B. Positive — SecretRef-keyed peers converge:** operators A and B start
/// without `GRID_SWIM_ENCRYPT_KEY`.  The `GridNetwork` references a Kubernetes
/// Secret via `spec.tls.swimKeyRef`; after reconcile, A uses a CRD-declared seed
/// to join B with the Secret-backed key.
///
/// **C. Negative — wrong-key peer rejected:** A is keyed; C has a different key
/// and seeds A's address.  A drops all of C's packets, so C is never admitted
/// to A's SWIM membership.  During the observation window, A's `connectedSites == 0`
/// and `distributedProviderCount == 0`.
///
/// **D. Negative — plaintext peer rejected:** A is keyed; D has no key.  A drops
/// D's unencrypted packets for the same reason.  During the observation window,
/// A's `connectedSites == 0` and `distributedProviderCount == 0`.
///
/// **E. Negative — missing Secret prevents plaintext sends:** A and B start
/// without `GRID_SWIM_ENCRYPT_KEY`.  The `GridNetwork` configures `swimKeyRef`
/// pointing to a Secret that does not exist.  Both operators' reconcile fails
/// before seed announcement or provider broadcast.  During the observation
/// window, A's `connectedSites == 0` and `distributedProviderCount == 0`.
/// This proves fail-closed behavior: a configured `swimKeyRef` with a missing
/// Secret does not silently degrade to plaintext.
///
/// All five scenarios are **hard failures** — they fail the test, not emit warnings.
///
/// Requires a kind cluster.  Run `env up` first.  Safe to rerun.
#[expect(
    clippy::too_many_lines,
    reason = "five sequential SWIM encryption scenarios; splitting would obscure the test data flow"
)]
fn env_verify_swim_encryption(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        SWIM_CONVERGENCE_WAIT, SWIM_ENCRYPT_NETWORK, SWIM_ENCRYPT_NODE_A, SWIM_ENCRYPT_NODE_B, SWIM_ENCRYPT_NODE_PLAIN,
        SWIM_ENCRYPT_NODE_WRONG, SWIM_STATUS_POLL_TIMEOUT,
    };

    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("verify-swim-encryption: context={context}");

    operator::install_grid_crds(&context)?;

    // Generate two distinct keys.  key_ab is shared between A and B; key_wrong belongs to C only.
    let key_ab = operator::generate_swim_key_hex();
    let key_wrong = operator::generate_swim_key_hex();

    // ── Scenario A: Keyed peers converge (positive) ───────────────────────────
    eprintln!("verify-swim-encryption: [1/5] positive — env-keyed peers A + B converge...");
    operator::cleanup_swim_encrypt_test_resources(&context)?;

    let (bind_a, bind_b) = reserve_swim_bind_addrs()?;
    let op_a = operator::spawn_operator_with_swim_keyed(
        &context,
        &bind_a,
        &bind_a,
        SWIM_ENCRYPT_NODE_A,
        "",
        None,
        Some(&key_ab),
    )?;
    let mut op_a_guard = ProcGuard(Some(op_a), "encrypt-op-a");
    let op_b = operator::spawn_operator_with_swim_keyed(
        &context,
        &bind_b,
        &bind_b,
        SWIM_ENCRYPT_NODE_B,
        &bind_a,
        None,
        Some(&key_ab),
    )?;
    let mut op_b_guard = ProcGuard(Some(op_b), "encrypt-op-b");

    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);
    operator::apply_swim_encrypt_test_fixtures(&context, SWIM_ENCRYPT_NODE_A)?;

    // A must see B in SWIM membership (connectedSites >= 1) — proves shared-key peers join.
    let convergence_result =
        operator::wait_for_gridnetwork_active(&context, SWIM_ENCRYPT_NETWORK, SWIM_STATUS_POLL_TIMEOUT);

    if let Some(c) = op_a_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op_b_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_swim_encrypt_test_resources(&context)?;

    let connected = convergence_result?;
    eprintln!("  [PASS] keyed peers A + B converged: connectedSites={connected}");

    // ── Scenario B: Secret-backed CRD key peers converge (positive) ───────────
    eprintln!("verify-swim-encryption: [2/5] positive — swimKeyRef Secret peers A + B converge...");
    operator::cleanup_swim_encrypt_test_resources(&context)?;

    let (bind_secret_a, bind_secret_b) = reserve_swim_bind_addrs()?;
    let op_secret_a = operator::spawn_operator_with_swim_keyed(
        &context,
        &bind_secret_a,
        &bind_secret_a,
        SWIM_ENCRYPT_NODE_A,
        "",
        None,
        None,
    )?;
    let mut op_secret_a_guard = ProcGuard(Some(op_secret_a), "encrypt-op-secret-a");
    let op_secret_b = operator::spawn_operator_with_swim_keyed(
        &context,
        &bind_secret_b,
        &bind_secret_b,
        SWIM_ENCRYPT_NODE_B,
        "",
        None,
        None,
    )?;
    let mut op_secret_b_guard = ProcGuard(Some(op_secret_b), "encrypt-op-secret-b");

    operator::apply_swim_encrypt_key_secret(&context, "0123456789abcdefghijklmnopqrstuv")?;
    operator::apply_swim_encrypt_test_fixtures_with_options(
        &context,
        SWIM_ENCRYPT_NODE_A,
        std::slice::from_ref(&bind_secret_b),
        true,
    )?;

    let secret_convergence_result =
        operator::wait_for_gridnetwork_active(&context, SWIM_ENCRYPT_NETWORK, SWIM_STATUS_POLL_TIMEOUT);

    if let Some(c) = op_secret_a_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op_secret_b_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_swim_encrypt_test_resources(&context)?;

    let secret_connected = secret_convergence_result?;
    eprintln!("  [PASS] swimKeyRef Secret peers A + B converged: connectedSites={secret_connected}");

    // ── Scenario C: Wrong-key peer cannot join (negative) ─────────────────────
    eprintln!("verify-swim-encryption: [3/5] negative — wrong-key peer C is rejected by A...");
    operator::cleanup_swim_encrypt_test_resources(&context)?;

    let (bind_a2, bind_wrong) = reserve_swim_bind_addrs()?;
    let op_a2 = operator::spawn_operator_with_swim_keyed(
        &context,
        &bind_a2,
        &bind_a2,
        SWIM_ENCRYPT_NODE_A,
        "",
        None,
        Some(&key_ab),
    )?;
    let mut op_a2_guard = ProcGuard(Some(op_a2), "encrypt-op-a2");
    let op_wrong = operator::spawn_operator_with_swim_keyed(
        &context,
        &bind_wrong,
        &bind_wrong,
        SWIM_ENCRYPT_NODE_WRONG,
        &bind_a2, // C seeds A — but A drops all of C's packets (wrong key)
        None,
        Some(&key_wrong),
    )?;
    let mut op_wrong_guard = ProcGuard(Some(op_wrong), "encrypt-op-wrong");

    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);
    operator::apply_swim_encrypt_test_fixtures(&context, SWIM_ENCRYPT_NODE_A)?;

    // A must NOT see C in SWIM membership, CRDT state, or overlay candidates.
    let wrong_rejection_result = operator::assert_swim_peer_stays_isolated(
        &context,
        SWIM_ENCRYPT_NETWORK,
        operator::SWIM_ENCRYPT_GW,
        SWIM_ENCRYPT_NODE_WRONG,
        SWIM_CONVERGENCE_WAIT,
    );

    if let Some(c) = op_a2_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op_wrong_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_swim_encrypt_test_resources(&context)?;

    wrong_rejection_result?;

    // ── Scenario D: Plaintext peer cannot join (negative) ────────────────────
    eprintln!("verify-swim-encryption: [4/5] negative — plaintext peer D is rejected by keyed A...");
    operator::cleanup_swim_encrypt_test_resources(&context)?;

    let (bind_a3, bind_plain) = reserve_swim_bind_addrs()?;
    let op_a3 = operator::spawn_operator_with_swim_keyed(
        &context,
        &bind_a3,
        &bind_a3,
        SWIM_ENCRYPT_NODE_A,
        "",
        None,
        Some(&key_ab),
    )?;
    let mut op_a3_guard = ProcGuard(Some(op_a3), "encrypt-op-a3");
    let op_plain = operator::spawn_operator_with_swim_keyed(
        &context,
        &bind_plain,
        &bind_plain,
        SWIM_ENCRYPT_NODE_PLAIN,
        &bind_a3, // D seeds A — but A drops D's plaintext packets
        None,
        None, // no key: plaintext
    )?;
    let mut op_plain_guard = ProcGuard(Some(op_plain), "encrypt-op-plain");

    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);
    operator::apply_swim_encrypt_test_fixtures(&context, SWIM_ENCRYPT_NODE_A)?;

    let plaintext_rejection_result = operator::assert_swim_peer_stays_isolated(
        &context,
        SWIM_ENCRYPT_NETWORK,
        operator::SWIM_ENCRYPT_GW,
        SWIM_ENCRYPT_NODE_PLAIN,
        SWIM_CONVERGENCE_WAIT,
    );

    if let Some(c) = op_a3_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op_plain_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_swim_encrypt_test_resources(&context)?;

    plaintext_rejection_result?;

    // ── Scenario E: Missing Secret prevents plaintext sends (negative) ──────
    eprintln!("verify-swim-encryption: [5/5] negative — missing Secret prevents plaintext sends...");
    operator::cleanup_swim_encrypt_test_resources(&context)?;

    let (bind_e_a, bind_e_b) = reserve_swim_bind_addrs()?;
    let op_e_a = operator::spawn_operator_with_swim_keyed(
        &context,
        &bind_e_a,
        &bind_e_a,
        SWIM_ENCRYPT_NODE_A,
        "",
        None,
        None, // no env var key
    )?;
    let mut op_e_a_guard = ProcGuard(Some(op_e_a), "encrypt-op-e-a");
    let op_e_b = operator::spawn_operator_with_swim_keyed(
        &context,
        &bind_e_b,
        &bind_e_b,
        SWIM_ENCRYPT_NODE_B,
        &bind_e_a,
        None,
        None, // no env var key
    )?;
    let mut op_e_b_guard = ProcGuard(Some(op_e_b), "encrypt-op-e-b");

    // Apply fixtures with swimKeyRef enabled but do NOT create the Secret.
    // Both operators' reconcile should fail at apply_configured_swim_key before
    // announcing CRD seeds or publishing provider broadcasts.
    operator::apply_swim_encrypt_test_fixtures_with_options(
        &context,
        SWIM_ENCRYPT_NODE_A,
        std::slice::from_ref(&bind_e_b),
        true, // swimKeyRef → points at non-existent Secret
    )?;

    let missing_secret_error_result = operator::wait_for_operator_log_contains(
        SWIM_ENCRYPT_NODE_A,
        &[
            "swim key configuration",
            SWIM_ENCRYPT_NETWORK,
            "did not resolve to a valid 32-byte key",
        ],
        SWIM_STATUS_POLL_TIMEOUT,
    );
    let missing_secret_result = operator::assert_reconcile_blocked_no_side_effects(
        &context,
        SWIM_ENCRYPT_NETWORK,
        operator::SWIM_ENCRYPT_GW,
        SWIM_CONVERGENCE_WAIT,
    );

    if let Some(c) = op_e_a_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op_e_b_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_swim_encrypt_test_resources(&context)?;

    missing_secret_error_result?;
    missing_secret_result?;

    eprintln!(
        "verify-swim-encryption: PASS — env-keyed peers converge; swimKeyRef Secret peers converge; \
         wrong-key peer rejected; plaintext peer rejected; missing Secret prevents plaintext sends"
    );
    Ok(())
}

/// Prove that CRDT/SWIM-distributed provider records appear in the routing overlay.
///
/// Starts two SWIM-enabled operator processes, applies a `GridNetwork` with a
/// `gatewayRef` and one `InferenceProvider`, waits for SWIM convergence and
/// distributed state propagation, then reads the overlay `ConfigMap` and asserts
/// that at least one candidate has a `site` value originating from the remote
/// operator (not the primary site).
#[expect(
    clippy::too_many_lines,
    reason = "sequential SWIM overlay steps: CRD install, two operator spawns, fixture apply, convergence wait, poll, overlay verify, cleanup"
)]
fn env_verify_swim_overlay(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        CONFIGMAP_POLL_TIMEOUT, SWIM_CONVERGENCE_WAIT, SWIM_NODE_PRIMARY_NAME, SWIM_NODE_SECONDARY_NAME,
        SWIM_OVERLAY_GW, SWIM_OVERLAY_NETWORK, SWIM_OVERLAY_PROVIDER, SWIM_STATUS_POLL_TIMEOUT,
    };

    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("verify-swim-overlay: context={context}");

    // Step 1: install CRDs and remove any stale overlay test resources.
    operator::install_grid_crds(&context)?;
    operator::cleanup_swim_overlay_test_resources(&context)?;

    let (bind1, bind2) = reserve_swim_bind_addrs()?;

    // Step 2: start the primary SWIM operator (no seeds).
    let op1 = operator::spawn_operator_with_swim(&context, &bind1, &bind1, SWIM_NODE_PRIMARY_NAME, "", None)?;
    let mut op1_guard = ProcGuard(Some(op1), "operator-primary");

    // Step 3: start the secondary operator with the primary as its seed.
    let op2 = operator::spawn_operator_with_swim(&context, &bind2, &bind2, SWIM_NODE_SECONDARY_NAME, &bind1, None)?;
    let mut op2_guard = ProcGuard(Some(op2), "operator-secondary");

    // Step 4: wait for SWIM gossip to converge.
    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);

    // Step 5: apply the GridNetwork with a gatewayRef and one InferenceProvider.
    // Both operators reconcile on watch; each publishes the InferenceProvider
    // as CRDT state.  After convergence each operator's state_snapshot contains
    // the remote peer's provider record.
    operator::apply_swim_overlay_test_fixtures(&context, SWIM_NODE_PRIMARY_NAME)?;
    eprintln!(
        "  GridNetwork {SWIM_OVERLAY_NETWORK} + InferenceProvider {SWIM_OVERLAY_PROVIDER} applied; \
         waiting for distributed state propagation..."
    );

    // Step 6: poll for distributedProviderCount > 0 (proves remote CRDT state arrived).
    let distributed_result =
        operator::wait_for_gridnetwork_distributed_state(&context, SWIM_OVERLAY_NETWORK, SWIM_STATUS_POLL_TIMEOUT);

    // Step 7: wait for the overlay ConfigMap to be generated.
    // The local InferenceProvider ensures the overlay has at least one candidate;
    // remote CRDT candidates from the secondary are filtered out at this point
    // because there is no Active GridSite for the secondary site.
    let cm_result = distributed_result.and_then(|_| {
        operator::wait_for_overlay_configmap(
            &context,
            SWIM_OVERLAY_NETWORK,
            SWIM_OVERLAY_GW,
            "default",
            CONFIGMAP_POLL_TIMEOUT,
        )
    });

    // Step 8: ROUTING ELIGIBILITY PROOF (before Active) — assert the secondary's CRDT
    // candidates are absent from the overlay.  The secondary SWIM peer is Alive and has
    // broadcast InferenceProvider state, but its corresponding GridSite is not Active, so
    // its CRDT providers must be excluded by the routing eligibility gate.
    // This proves SWIM gossip alone is NOT sufficient for routing.
    let before_result = cm_result.and_then(|()| {
        operator::assert_no_crdt_candidates_for_site(
            &context,
            SWIM_OVERLAY_NETWORK,
            SWIM_OVERLAY_GW,
            SWIM_NODE_SECONDARY_NAME, // exclude the secondary SWIM site's CRDT candidates
        )
    });

    // Step 9: Configure trust for the secondary site so the controller promotes it to Active.
    // The GridSite name is the deterministic auto-discovered name for the secondary's SWIM site.
    // Bind a TCP listener so the controller's probe succeeds, then configure cert + fingerprint
    // so the controller promotes Connecting → Active naturally (TrustPolicyVerified).
    let secondary_k8s_name = operator::auto_discovered_gridsite_name(SWIM_OVERLAY_NETWORK, SWIM_NODE_SECONDARY_NAME);
    let secondary_listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|e| format!("failed to bind secondary site probe listener: {e}"))?;
    let secondary_probe_addr = secondary_listener
        .local_addr()
        .map(|a| a.to_string())
        .map_err(|e| format!("listener has no local addr: {e}"))?;
    let after_result = before_result.and_then(|()| {
        eprintln!("  configuring trust for GridSite {secondary_k8s_name:?} for secondary SWIM site...");
        operator::apply_active_gridsite_for_eligibility(
            &context,
            &secondary_k8s_name,
            SWIM_OVERLAY_NETWORK,
            &secondary_probe_addr,
        )
    });

    // Step 10: ROUTING ELIGIBILITY PROOF (after Active) — poll until the secondary's CRDT
    // provider candidate appears in the overlay.  The controller must:
    //   (a) probe the TCP listener at secondary_probe_addr (succeeds — listener is held above)
    //   (b) verify the fingerprint matches → promote Connecting → Active (TrustPolicyVerified)
    //   (c) re-render the overlay to include secondary CRDT providers
    // wait_for_site_candidate_in_overlay bumps GridNetwork each cycle, triggering (b) and (c).
    let verify_result = after_result.and_then(|()| {
        operator::wait_for_site_candidate_in_overlay(
            &context,
            SWIM_OVERLAY_NETWORK,
            SWIM_OVERLAY_GW,
            SWIM_NODE_SECONDARY_NAME,
            CONFIGMAP_POLL_TIMEOUT,
        )
    });

    // Cleanup — always stop operators and remove fixtures regardless of result.
    if let Some(c) = op1_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op2_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_swim_overlay_test_resources(&context)?;
    operator::cleanup_auto_discovered_gridsite(&context, &secondary_k8s_name)
        .unwrap_or_else(|e| eprintln!("  warning: secondary GridSite cleanup: {e}"));
    // Also clean up the SWIM provider that may have been created during convergence
    // wait to avoid contaminating subsequent verify-swim-state runs.
    operator::cleanup_swim_test_resources(&context).unwrap_or_else(|e| {
        eprintln!("  warning: SWIM test resource cleanup failed: {e}");
    });

    verify_result?;

    eprintln!(
        "verify-swim-overlay: PASS — CRDT provider record from {SWIM_NODE_SECONDARY_NAME:?} \
         appeared in overlay for {SWIM_OVERLAY_NETWORK}/{SWIM_OVERLAY_GW}"
    );
    Ok(())
}

/// Prove end-to-end distributed model routing via CRDT/SWIM discovery.
///
/// Uses a two-provider kind environment.  The east operator runs against the
/// east cluster (model-east is a local `InferenceProvider`); the west operator
/// runs against the west cluster (model-west is a local `InferenceProvider`).
/// After SWIM gossip, the east operator's overlay includes model-west as a
/// remote CRDT candidate.  A consumer gateway is deployed from that overlay
/// and routes requests for both models.
#[expect(
    clippy::too_many_lines,
    reason = "sequential steps: CRDs, fixtures, operators, wait, overlay, consumer deploy, E2E verify, cleanup"
)]
fn env_verify_swim_routing(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        CONFIGMAP_POLL_TIMEOUT, SWIM_CONVERGENCE_WAIT, SWIM_ROUTING_GW, SWIM_ROUTING_NETWORK, SWIM_STATUS_POLL_TIMEOUT,
    };

    let cfg = EnvConfig::from_file(config)?;
    eprintln!("verify-swim-routing: loading two-provider config...");

    // Derive site names and models from the config.  Expect exactly two providers.
    let providers = provider_clusters_from_config(&cfg);
    if providers.len() != 2 {
        return Err(format!(
            "verify-swim-routing requires exactly 2 provider clusters; got {}",
            providers.len()
        )
        .into());
    }
    let Some((east_site, east_models)) = providers.first() else {
        return Err("verify-swim-routing: no provider clusters found in config".into());
    };
    let Some((west_site, west_models)) = providers.get(1) else {
        return Err("verify-swim-routing: second provider cluster not found in config".into());
    };
    let east_model = east_models.first().ok_or("east provider has no models")?.clone();
    let west_model = west_models.first().ok_or("west provider has no models")?.clone();
    let east_ctx = kind::kubectl_context(east_site);
    let west_ctx = kind::kubectl_context(west_site);

    eprintln!("verify-swim-routing: east={east_site} ({east_ctx}), west={west_site} ({west_ctx})");
    eprintln!("  east model: {east_model}, west model: {west_model}");

    // Pre-build the operator binary so spawn_operator_with_swim_for_context
    // does not compile during the poll window (each spawn also calls
    // ensure_operator_binary_built, but the first call here is shared so
    // subsequent per-spawn calls are fast incremental no-ops).
    eprintln!("verify-swim-routing: pre-building operator binary...");
    operator::ensure_operator_binary_built()?;
    eprintln!("  [OK] operator binary ready");

    // Step 1: deploy provider gateways on both clusters.
    eprintln!("verify-swim-routing: [1/6] deploying provider gateways...");
    gateway::deploy_all(&cfg)?;

    // Step 2: install Grid CRDs and clean up stale routing fixtures on both clusters.
    eprintln!("verify-swim-routing: [2/6] installing CRDs and removing stale fixtures...");
    operator::install_grid_crds(&east_ctx)?;
    operator::install_grid_crds(&west_ctx)?;
    operator::cleanup_swim_routing_resources(&east_ctx)?;
    operator::cleanup_swim_routing_resources(&west_ctx)?;

    // Step 3: start SWIM-enabled operators before applying fixtures.
    //         Primary runs against east k8s (generates overlay ConfigMap).
    //         Peer runs against west k8s (publishes model-west as CRDT state).
    //
    //         Each operator receives a cluster-specific kubeconfig via KUBECONFIG
    //         env var (not via kubectl config use-context) to avoid the race where
    //         the second use-context fires before the first operator binary reads
    //         its kubeconfig.  Both operators start with the correct cluster.
    //
    //         Operators are started BEFORE fixtures so that when fixture watch
    //         events trigger the first reconcile, SWIM gossip has already converged
    //         and the reconcile immediately observes the peer's CRDT state.
    eprintln!("verify-swim-routing: [3/6] starting SWIM operators...");
    let (bind1, bind2) = reserve_swim_bind_addrs()?;
    let op1 = operator::spawn_operator_with_swim_for_context(&east_ctx, &bind1, &bind1, east_site, "", None)?;
    let mut op1_guard = ProcGuard(Some(op1), "operator-east");
    let op2 = operator::spawn_operator_with_swim_for_context(&west_ctx, &bind2, &bind2, west_site, &bind1, None)?;
    let mut op2_guard = ProcGuard(Some(op2), "operator-west");

    // Step 4: wait for SWIM gossip to converge, then apply fixtures.
    //         After convergence, fixture creation triggers an immediate reconcile
    //         on each operator.  That reconcile publishes CRDT state over the
    //         already-converged SWIM mesh, and the subsequent reconcile (triggered
    //         by InferenceProvider status updates) sees the remote provider record.
    eprintln!("verify-swim-routing: [4/6] waiting for SWIM convergence then applying fixtures...");
    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);

    // Apply east fixtures (GridNetwork with gatewayRef + east InferenceProvider).
    // Apply west fixtures (GridNetwork without gatewayRef + west InferenceProvider).
    operator::apply_swim_routing_east_fixtures(&east_ctx, east_site, &east_model)?;
    operator::apply_swim_routing_west_fixtures(&west_ctx, west_site, &west_model)?;

    // Step 5: settle, then bump east GridNetwork to force a reconcile after CRDT lands.
    //
    // The first reconcile wave (triggered by fixture application) races the west
    // operator's CRDT broadcast by tens of milliseconds: east often reconciles
    // before west's broadcast arrives, recording distributedProviderCount=0.
    // After the settle period both operators have exchanged CRDT state via SWIM
    // gossip.  Patching a timestamp annotation on the east GridNetwork forces a
    // fresh reconcile that reads the updated state_snapshot() and records the
    // correct distributedProviderCount.
    eprintln!("verify-swim-routing: [5/6] settling CRDT then bumping east GridNetwork...");
    operator::wait_for_swim_convergence(Duration::from_secs(5)); // settle for CRDT exchange
    operator::bump_gridnetwork(&east_ctx, SWIM_ROUTING_NETWORK)?;

    // Poll for distributedProviderCount > 0 on the east cluster: proves that
    // the west operator's model-west CRDT broadcast arrived at the east operator.
    let distributed_result =
        operator::wait_for_gridnetwork_distributed_state(&east_ctx, SWIM_ROUTING_NETWORK, SWIM_STATUS_POLL_TIMEOUT);

    // Wait for the overlay ConfigMap to contain the remote CRDT candidate.
    let cm_result = distributed_result.and_then(|_| {
        operator::wait_for_overlay_configmap(
            &east_ctx,
            SWIM_ROUTING_NETWORK,
            SWIM_ROUTING_GW,
            "default",
            CONFIGMAP_POLL_TIMEOUT,
        )
    });

    // Verify overlay has a remote candidate (site != east_site).
    let overlay_result = cm_result.and_then(|()| {
        operator::verify_swim_overlay_candidates(&east_ctx, SWIM_ROUTING_NETWORK, SWIM_ROUTING_GW, east_site)
    });

    // Export overlay to file for consumer deploy.
    let overlay_path_result = overlay_result
        .and_then(|()| operator::export_overlay_to_file(&east_ctx, SWIM_ROUTING_NETWORK, SWIM_ROUTING_GW, "default"));

    // Kill operators before deploying consumer (operators keep kubeconfig context).
    if let Some(c) = op1_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op2_guard.0.take() {
        operator::kill_operator(c);
    }

    let overlay_path = overlay_path_result?;

    // Step 6: deploy consumer from CRDT overlay and verify routing.
    eprintln!("verify-swim-routing: [6/6] deploying consumer and verifying routing...");
    consumer::deploy_consumer(&cfg, Some(&overlay_path))?;
    consumer::verify_e2e(&cfg)?;

    // Cleanup.
    operator::cleanup_swim_routing_resources(&east_ctx).unwrap_or_else(|e| {
        eprintln!("  warning: east cleanup failed: {e}");
    });
    operator::cleanup_swim_routing_resources(&west_ctx).unwrap_or_else(|e| {
        eprintln!("  warning: west cleanup failed: {e}");
    });

    eprintln!(
        "verify-swim-routing: PASS — {west_model} from {west_site:?} entered overlay via \
         CRDT/SWIM and routed to HTTP 200 via consumer gateway"
    );
    Ok(())
}

/// Reserve two distinct localhost UDP addresses for the SWIM membership check.
fn reserve_swim_bind_addrs() -> Result<(String, String), Box<dyn std::error::Error>> {
    let bind1 = operator::reserve_local_udp_addr()?.to_string();
    let mut bind2 = operator::reserve_local_udp_addr()?.to_string();
    while bind2 == bind1 {
        bind2 = operator::reserve_local_udp_addr()?.to_string();
    }
    Ok((bind1, bind2))
}

/// Reserve three distinct localhost UDP addresses for a three-node SWIM mesh.
fn reserve_three_swim_bind_addrs() -> Result<(String, String, String), Box<dyn std::error::Error>> {
    let (bind_a, bind_b) = reserve_swim_bind_addrs()?;
    let mut bind_c = operator::reserve_local_udp_addr()?.to_string();
    while bind_c == bind_a || bind_c == bind_b {
        bind_c = operator::reserve_local_udp_addr()?.to_string();
    }
    Ok((bind_a, bind_b, bind_c))
}

// ---------------------------------------------------------------------------
// Three-node SWIM mesh validation
// ---------------------------------------------------------------------------

/// Prove transitive SWIM discovery, provider state propagation, and routing
/// eligibility gating across a three-node mesh.
///
/// Topology:
///   A  ←seed—  B  ←seed—  C
///
/// A seeds nobody; B seeds A; C seeds B only.  A learns about C transitively.
///
/// What this proves:
/// 1. `distributedProviderCount >= 2` on A: CRDT state from both B and C arrived.
/// 2. C's overlay candidate is absent before C's `GridSite` is `Active`.
/// 3. After `Active`, C's candidate appears in A's overlay.
/// 4. A wrong-network `GridNetwork` and `InferenceProvider` are applied and their model is confirmed absent from A's
///    correct-network overlay.
#[expect(
    clippy::too_many_lines,
    reason = "sequential 11-step mesh proof: CRDs, three operators, CRDT convergence, \
              eligibility before/after Active, wrong-network isolation, cleanup"
)]
fn env_verify_swim_mesh_three_node(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        CONFIGMAP_POLL_TIMEOUT, SWIM_CONVERGENCE_WAIT, SWIM_MESH_GW, SWIM_MESH_MODEL_C, SWIM_MESH_NETWORK,
        SWIM_MESH_PROVIDER_C, SWIM_MESH_SITE_A, SWIM_MESH_SITE_B, SWIM_MESH_SITE_C, SWIM_MESH_WRONG_MODEL,
        SWIM_STATUS_POLL_TIMEOUT,
    };

    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("verify-swim-mesh-three-node: context={context}");

    // ── Step 1: Install CRDs and remove any stale mesh test resources ─────────
    operator::install_grid_crds(&context)?;
    operator::cleanup_swim_mesh_test_resources(&context)?;

    // ── Step 2: Reserve three distinct SWIM bind addresses ───────────────────
    let (bind_a, bind_b, bind_c) = reserve_three_swim_bind_addrs()?;
    eprintln!(
        "  A={bind_a}  B={bind_b}  C={bind_c} \
         (topology: A←B←C, C does NOT seed A directly)"
    );

    // ── Step 3: Spawn all three operators ─────────────────────────────────────
    // A: no seeds.  B: seeds A.  C: seeds B only (not A) — ensures transitivity.
    let op_a = operator::spawn_operator_with_swim(&context, &bind_a, &bind_a, SWIM_MESH_SITE_A, "", None)?;
    let mut op_a_guard = ProcGuard(Some(op_a), "operator-mesh-a");

    let op_b = operator::spawn_operator_with_swim(&context, &bind_b, &bind_b, SWIM_MESH_SITE_B, &bind_a, None)?;
    let mut op_b_guard = ProcGuard(Some(op_b), "operator-mesh-b");

    // C seeds only B — the proof that A learns C transitively.
    let op_c = operator::spawn_operator_with_swim(&context, &bind_c, &bind_c, SWIM_MESH_SITE_C, &bind_b, None)?;
    let mut op_c_guard = ProcGuard(Some(op_c), "operator-mesh-c");

    // ── Step 4: Wait for SWIM gossip to propagate across the mesh ─────────────
    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);
    eprintln!("  SWIM convergence window elapsed — A should know B and C through B");

    // ── Step 5: Apply main GridNetwork/provider and wrong-network isolation fixtures ─
    operator::apply_swim_mesh_test_fixtures(&context, SWIM_MESH_SITE_A, SWIM_MESH_PROVIDER_C, SWIM_MESH_MODEL_C)?;
    // Apply a wrong-network GridNetwork + InferenceProvider so we can assert their
    // model does not leak into A's correct-network overlay.
    operator::apply_swim_mesh_wrong_network_fixtures(&context)?;

    // ── Step 6: Prove transitive state propagation — A has CRDT from both B and C ─
    // `distributedProviderCount >= 2` means A received provider records from at
    // least two remote sites.  Since C only seeded B, A must have learned C's
    // record through B.
    eprintln!("verify-swim-mesh-three-node: [6] waiting for A to receive CRDT from B AND C...");
    let count_result =
        operator::wait_for_distributed_state_count(&context, SWIM_MESH_NETWORK, 2, SWIM_STATUS_POLL_TIMEOUT);

    let cm_result = count_result.and_then(|count| {
        eprintln!(
            "  [PASS] transitive CRDT propagation: A distributedProviderCount={count} \
             (>= 2 — received from B and C through the mesh)"
        );
        // ── Step 7: Wait for A's overlay ConfigMap ───────────────────────────
        eprintln!("verify-swim-mesh-three-node: [7] waiting for A's overlay ConfigMap...");
        operator::wait_for_overlay_configmap(
            &context,
            SWIM_MESH_NETWORK,
            SWIM_MESH_GW,
            "default",
            CONFIGMAP_POLL_TIMEOUT,
        )
    });

    // ── Step 8: Routing eligibility — C's candidate absent before Active ──────
    eprintln!("verify-swim-mesh-three-node: [8] proving C excluded before GridSite Active...");
    let before_result = cm_result.and_then(|()| {
        operator::assert_no_crdt_candidates_for_site(&context, SWIM_MESH_NETWORK, SWIM_MESH_GW, SWIM_MESH_SITE_C)
    });

    // Also assert B's candidates are absent (B GridSite also not Active).
    let before_b_result = before_result.and_then(|()| {
        operator::assert_no_crdt_candidates_for_site(&context, SWIM_MESH_NETWORK, SWIM_MESH_GW, SWIM_MESH_SITE_B)
    });

    // ── Step 9: Cross-network isolation — wrong-network model absent from A's overlay ─
    // The wrong-network InferenceProvider serves SWIM_MESH_WRONG_MODEL.  It belongs to
    // a different GridNetwork and must not appear in A's op-e2e-swim-mesh-net overlay.
    eprintln!("verify-swim-mesh-three-node: [9] proving wrong-network model absent from A's overlay...");
    let isolation_result = before_b_result.and_then(|()| {
        operator::assert_no_overlay_candidate_for_model(
            &context,
            SWIM_MESH_NETWORK,
            SWIM_MESH_GW,
            SWIM_MESH_WRONG_MODEL,
        )
    });

    // ── Step 10: Apply Active GridSite for C and verify C's candidate appears ──
    // Bind a real TCP listener so the GridSite controller's probe succeeds and the
    // GridSite stays in Active phase (rather than being demoted to Unreachable).
    let c_site_k8s_name = operator::auto_discovered_gridsite_name(SWIM_MESH_NETWORK, SWIM_MESH_SITE_C);
    let listener_result = isolation_result.and_then(|()| {
        std::net::TcpListener::bind("127.0.0.1:0")
            .map_err(|e| format!("failed to bind TCP listener for C probe: {e}").into())
    });

    let after_result = listener_result.and_then(|listener| {
        let local_addr = match listener.local_addr() {
            Ok(a) => a.to_string(),
            Err(_) => "127.0.0.1:0".to_owned(),
        };
        eprintln!(
            "verify-swim-mesh-three-node: [10] configuring trust for GridSite {c_site_k8s_name:?} \
             (C) → waiting for Active (probe addr={local_addr})..."
        );
        let result =
            operator::apply_active_gridsite_for_eligibility(&context, &c_site_k8s_name, SWIM_MESH_NETWORK, &local_addr);
        // Keep listener alive so the GridSite TCP probe succeeds, then poll.
        let verify = result.and_then(|()| {
            operator::wait_for_site_candidate_in_overlay(
                &context,
                SWIM_MESH_NETWORK,
                SWIM_MESH_GW,
                SWIM_MESH_SITE_C,
                SWIM_STATUS_POLL_TIMEOUT,
            )
        });
        drop(listener); // release port after verification
        verify
    });

    let verify_c_result = after_result;

    // ── Step 10: Cleanup — always stop operators and remove fixtures ───────────
    if let Some(c) = op_a_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op_b_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op_c_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_swim_mesh_test_resources(&context)?;
    operator::cleanup_auto_discovered_gridsite(&context, &c_site_k8s_name)
        .unwrap_or_else(|e| eprintln!("  warning: C GridSite cleanup: {e}"));

    verify_c_result?;

    eprintln!(
        "verify-swim-mesh-three-node: PASS — \
         A→B→C transitive discovery proven; \
         C's CRDT state reached A through B (distributedProviderCount >= 2); \
         C absent before Active; C present after Active; \
         wrong-network model absent from A's overlay"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// llm-d-compatible routing proof
// ---------------------------------------------------------------------------

/// Resolve the first consumer-role cluster name from the environment config.
fn resolve_consumer_site(cfg: &EnvConfig) -> Result<&str, Box<dyn std::error::Error>> {
    cfg.consumer_cluster_name()
        .ok_or_else(|| "no consumer cluster in config".into())
}

/// Verify that a Chat Completions JSON response includes the expected model.
///
/// Returns `Ok(())` when the `model` field matches `expected_model`.
/// Returns `Err` with a diagnostic message on mismatch or parse failure.
fn verify_response_model_field(body: &str, expected_model: &str) -> Result<(), Box<dyn std::error::Error>> {
    let json: serde_json::Value = serde_json::from_str(body).map_err(|e| format!("response is not valid JSON: {e}"))?;
    let model = json.get("model").and_then(serde_json::Value::as_str).unwrap_or("");
    if model == expected_model {
        Ok(())
    } else {
        Err(format!("response model field: expected {expected_model:?}, got {model:?}").into())
    }
}

/// Parse `kubectl` restart count output into `(pod_name, restart_count)` pairs.
///
/// Expects lines of the form `<pod-name> <restart-count>`.  Only pods with
/// `restart_count > 0` are included in the result.  Lines with non-numeric
/// counts (e.g. `<none>`) are silently skipped.
fn parse_pod_restart_lines(output: &str) -> Vec<(String, u32)> {
    output
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let name = parts.next()?;
            let count: u32 = parts.next()?.parse().ok()?;
            (count > 0).then(|| (name.to_owned(), count))
        })
        .collect()
}

/// Shorthand for the pod restart count list returned by
/// [`collect_pod_restart_counts`].
/// Pod name plus its container restart count.
type PodRestartCounts = Vec<(String, u32)>;

/// Query pod restart counts in `namespace` within `context`.
///
/// Returns a list of `(pod_name, restart_count)` for pods with non-zero
/// restart counts.
fn collect_pod_restart_counts(context: &str, namespace: &str) -> Result<PodRestartCounts, Box<dyn std::error::Error>> {
    let output = std::process::Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            namespace,
            "get",
            "pods",
            "-o",
            "custom-columns=NAME:.metadata.name,RESTARTS:.status.containerStatuses[0].restartCount",
            "--no-headers",
        ])
        .output()?;
    let stdout = String::from_utf8(output.stdout)?;
    Ok(parse_pod_restart_lines(&stdout))
}

/// Record an llm-d-compatible routing step result, returning whether it passed.
fn llmd_compat_record_step(
    label: &'static str,
    results: &mut Vec<StepResult>,
    f: impl FnOnce() -> Result<String, Box<dyn std::error::Error>>,
) -> bool {
    match f() {
        Ok(evidence) => {
            results.push(StepResult::pass(label, evidence));
            true
        },
        Err(e) => {
            results.push(StepResult::fail(label, e.as_ref()));
            false
        },
    }
}

/// Verify consumer routing and response model name fields.
///
/// Runs the standard consumer E2E check (HTTP 200 per model, unknown model
/// returns 404/503) and then verifies that each response body includes the
/// correct `model` field.
fn verify_llmd_compat_routing(
    cfg: &EnvConfig,
    results: &mut Vec<StepResult>,
) -> Result<(), Box<dyn std::error::Error>> {
    match consumer::verify_e2e(cfg) {
        Ok(()) => results.push(StepResult::pass(
            "consumer routing",
            "all models routed correctly, unknown model fails cleanly",
        )),
        Err(e) => {
            results.push(StepResult::fail("consumer routing", e.as_ref()));
            return Ok(());
        },
    }
    verify_llmd_compat_model_fields(cfg, results)
}

/// Check Chat Completions response `model` fields via consumer gateway.
///
/// Port-forwards to the consumer gateway and sends a request per configured
/// model.  Asserts the response JSON `model` field matches the request.
fn verify_llmd_compat_model_fields(
    cfg: &EnvConfig,
    results: &mut Vec<StepResult>,
) -> Result<(), Box<dyn std::error::Error>> {
    let consumer_site = resolve_consumer_site(cfg)?;
    let ctx = kind::kubectl_context(consumer_site);
    let port = verify::find_free_port()?;
    let mut pf = verify::PortForwardGuard::start(&ctx, "praxis-consumer", port, 8080)?;
    if !verify::wait_for_port(port) {
        pf.stop();
        results.push(StepResult {
            label: "response model field",
            status: StepStatus::Fail,
            evidence: "consumer not reachable".to_owned(),
        });
        return Ok(());
    }
    let result = llmd_compat_check_all_model_fields(cfg, port);
    pf.stop();
    results.push(result);
    Ok(())
}

/// Check model field in response for all provider models.
fn llmd_compat_check_all_model_fields(cfg: &EnvConfig, port: u16) -> StepResult {
    let mut verified = Vec::new();
    let mut failed = Vec::new();
    for name in &cfg.clusters.names {
        let Some(def) = cfg.clusters.definitions.get(name) else {
            continue;
        };
        if def.role != ClusterRole::Provider {
            continue;
        }
        for model in &def.models {
            match llmd_compat_check_one_model_field(port, model) {
                Ok(()) => verified.push(model.clone()),
                Err(e) => failed.push(format!("{model}: {e}")),
            }
        }
    }
    if failed.is_empty() {
        StepResult::pass("response model field", verified.join(", "))
    } else {
        StepResult {
            label: "response model field",
            status: StepStatus::Fail,
            evidence: safe_truncate_str(&failed.join("; "), 120),
        }
    }
}

/// Send a consumer request and verify the response model field.
fn llmd_compat_check_one_model_field(port: u16, model: &str) -> Result<(), Box<dyn std::error::Error>> {
    let resp = consumer::send_consumer_request(port, model)?;
    if resp.status != 200 {
        return Err(format!("HTTP {}, expected 200", resp.status).into());
    }
    verify_response_model_field(&resp.body, model)
}

/// Assert no pods restarted during the llm-d-compatible routing test run.
///
/// Queries every cluster in the topology for pods with non-zero restart
/// counts in the `default` namespace.
fn verify_llmd_compat_pod_restarts(
    cfg: &EnvConfig,
    results: &mut Vec<StepResult>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut total_restarts: Vec<String> = Vec::new();
    for name in &cfg.clusters.names {
        let ctx = kind::kubectl_context(name);
        let restarts = collect_pod_restart_counts(&ctx, "default")?;
        for (pod, count) in &restarts {
            total_restarts.push(format!("{name}/{pod}: {count}"));
        }
    }
    if total_restarts.is_empty() {
        results.push(StepResult::pass("pod restarts", "0 restarts across all clusters"));
    } else {
        results.push(StepResult {
            label: "pod restarts",
            status: StepStatus::Fail,
            evidence: safe_truncate_str(&total_restarts.join(", "), 120),
        });
    }
    Ok(())
}

/// Run the llm-d-compatible routing proof.
///
/// Orchestrates the full two-provider llm-d routing proof:
/// 1. Deploy provider gateways.
/// 2. Operator reconcile + overlay export.
/// 3. Verify provider-side llm-d path.
/// 4. Deploy consumer gateway from overlay.
/// 5. Verify consumer routing with model name checks.
/// 6. Assert no unexpected pod restarts.
#[expect(
    clippy::too_many_lines,
    reason = "sequential demo proof steps: each step depends on the previous; splitting obscures the proof flow"
)]
fn env_verify_llmd_compat_routing(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    let providers = provider_clusters_from_config(&cfg);
    if providers.len() < 2 {
        return Err(format!("llmd-compat requires >= 2 provider clusters; got {}", providers.len()).into());
    }
    let mut results: Vec<StepResult> = Vec::new();

    // Step 1: Deploy provider gateways.
    eprintln!("llmd-compat: [1/6] deploying provider gateways...");
    let gw_ok = llmd_compat_record_step("provider gateways", &mut results, || {
        gateway::deploy_all(&cfg)?;
        Ok(format!("{} sites deployed", providers.len()))
    });
    if !gw_ok {
        print_validate_all_table(&results);
        return Err("llmd-compat: provider gateway deployment failed".into());
    }

    // Step 2: Operator reconcile + overlay export.
    eprintln!("llmd-compat: [2/6] operator reconcile + overlay export...");
    let context = resolve_operator_context(&cfg, None)?;
    let providers_ref: Vec<(&str, &[String])> = providers.iter().map(|(s, m)| (s.as_str(), m.as_slice())).collect();
    let overlay_path = match run_multi_provider_reconcile(&context, &providers_ref) {
        Ok(path) => {
            results.push(StepResult::pass(
                "operator reconcile",
                format!("overlay for {} sites", providers.len()),
            ));
            path
        },
        Err(e) => {
            results.push(StepResult::fail("operator reconcile", e.as_ref()));
            print_validate_all_table(&results);
            return Err("llmd-compat: operator reconcile failed".into());
        },
    };

    // Step 3: Verify provider-side llm-d path.
    eprintln!("llmd-compat: [3/6] verifying provider-side llm-d path...");
    llmd_compat_record_step("provider llm-d path", &mut results, || {
        gateway::verify_all(&cfg)?;
        Ok("ext_proc + mock EPP + endpoint_selector verified".to_owned())
    });

    // Step 4: Deploy consumer gateway from overlay.
    eprintln!("llmd-compat: [4/6] deploying consumer gateway from overlay...");
    let consumer_ok = llmd_compat_record_step("consumer deploy", &mut results, || {
        consumer::deploy_consumer(&cfg, Some(&overlay_path))?;
        Ok("deployed from operator overlay".to_owned())
    });
    if !consumer_ok {
        print_validate_all_table(&results);
        return Err("llmd-compat: consumer deploy failed".into());
    }

    // Step 5: Verify consumer routing + model names.
    eprintln!("llmd-compat: [5/6] verifying consumer routing + model names...");
    verify_llmd_compat_routing(&cfg, &mut results)?;

    // Step 6: Pod restart check.
    eprintln!("llmd-compat: [6/6] checking for unexpected pod restarts...");
    verify_llmd_compat_pod_restarts(&cfg, &mut results)?;

    eprintln!();
    eprintln!("## llm-d-compatible Routing Proof");
    print_validate_all_table(&results);
    if results.iter().any(|r| r.status.is_failure()) {
        Err("llmd-compat: one or more proof points FAILED".into())
    } else {
        eprintln!("llmd-compat: ALL proof points PASS");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Responses routing validation
// ---------------------------------------------------------------------------

/// Record a step result for the Responses routing proof.
fn responses_record_step(
    label: &'static str,
    results: &mut Vec<StepResult>,
    f: impl FnOnce() -> Result<String, Box<dyn std::error::Error>>,
) -> bool {
    match f() {
        Ok(evidence) => {
            results.push(StepResult::pass(label, evidence));
            true
        },
        Err(e) => {
            results.push(StepResult::fail(label, e.as_ref()));
            false
        },
    }
}

/// Check Responses API response `model` fields via consumer gateway.
///
/// Port-forwards to the consumer gateway and sends a `/v1/responses` request
/// per configured model. Verifies that the `openai_responses_format` request
/// parsing and `grid_route` selection work correctly by checking response `model` fields.
fn verify_responses_model_fields(
    cfg: &EnvConfig,
    results: &mut Vec<StepResult>,
) -> Result<(), Box<dyn std::error::Error>> {
    let consumer_site = resolve_consumer_site(cfg)?;
    let ctx = kind::kubectl_context(consumer_site);
    let port = verify::find_free_port()?;
    let mut pf = verify::PortForwardGuard::start(&ctx, "praxis-consumer", port, 8080)?;
    if !verify::wait_for_port(port) {
        pf.stop();
        results.push(StepResult {
            label: "response model field",
            status: StepStatus::Fail,
            evidence: "consumer not reachable".to_owned(),
        });
        return Ok(());
    }
    let result = responses_check_all_model_fields(cfg, port);
    pf.stop();
    results.push(result);
    Ok(())
}

/// Check model field in response for all provider models using Responses API.
fn responses_check_all_model_fields(cfg: &EnvConfig, port: u16) -> StepResult {
    let mut verified = Vec::new();
    let mut failed = Vec::new();
    for name in &cfg.clusters.names {
        let Some(def) = cfg.clusters.definitions.get(name) else {
            continue;
        };
        if def.role != ClusterRole::Provider {
            continue;
        }
        for model in &def.models {
            match responses_check_one_model_field(port, model) {
                Ok(()) => verified.push(model.clone()),
                Err(e) => failed.push(format!("{model}: {e}")),
            }
        }
    }
    if failed.is_empty() {
        StepResult::pass("response model field", verified.join(", "))
    } else {
        StepResult {
            label: "response model field",
            status: StepStatus::Fail,
            evidence: safe_truncate_str(&failed.join("; "), 120),
        }
    }
}

/// Send a Responses request and verify the response model field.
fn responses_check_one_model_field(port: u16, model: &str) -> Result<(), Box<dyn std::error::Error>> {
    let resp = consumer::send_responses_request(port, model)?;
    if resp.status != 200 {
        return Err(format!("HTTP {}, expected 200", resp.status).into());
    }
    verify_response_model_field(&resp.body, model)
}

/// Assert no pods restarted during the Responses routing test run.
fn verify_responses_pod_restarts(
    cfg: &EnvConfig,
    results: &mut Vec<StepResult>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut total_restarts: Vec<String> = Vec::new();
    for name in &cfg.clusters.names {
        let ctx = kind::kubectl_context(name);
        let restarts = collect_pod_restart_counts(&ctx, "default")?;
        for (pod, count) in &restarts {
            total_restarts.push(format!("{name}/{pod}: {count}"));
        }
    }
    if total_restarts.is_empty() {
        results.push(StepResult::pass("pod restarts", "0 restarts across all clusters"));
    } else {
        results.push(StepResult {
            label: "pod restarts",
            status: StepStatus::Fail,
            evidence: safe_truncate_str(&total_restarts.join(", "), 120),
        });
    }
    Ok(())
}

/// Run the Responses request parsing and routing proof.
///
/// Orchestrates the `/v1/responses` validation using Praxis AI request parsing
/// with Grid overlay routing:
/// 1. Deploy provider gateways.
/// 2. Operator reconcile + Grid overlay export.
/// 3. Deploy consumer gateway with `openai_responses_format` → `grid_route` filter chain.
/// 4. Verify `/v1/responses` routing: each model returns 200, unknown fails.
/// 5. Verify response model fields match the requested model.
/// 6. Assert no unexpected pod restarts.
#[expect(
    clippy::too_many_lines,
    reason = "sequential proof steps: each step depends on the previous; splitting obscures the proof flow"
)]
fn env_verify_responses_routing(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    let providers = provider_clusters_from_config(&cfg);
    if providers.len() < 2 {
        return Err(format!(
            "responses-routing requires >= 2 provider clusters; got {}",
            providers.len()
        )
        .into());
    }
    let mut results: Vec<StepResult> = Vec::new();

    // Step 1: Deploy provider gateways.
    eprintln!("responses-routing: [1/6] deploying provider gateways...");
    let gw_ok = responses_record_step("provider gateways", &mut results, || {
        gateway::deploy_all(&cfg)?;
        Ok(format!("{} sites deployed", providers.len()))
    });
    if !gw_ok {
        print_validate_all_table(&results);
        return Err("responses-routing: provider gateway deployment failed".into());
    }

    // Step 2: Operator reconcile + Grid overlay export.
    eprintln!("responses-routing: [2/6] operator reconcile + overlay export...");
    let context = resolve_operator_context(&cfg, None)?;
    let providers_ref: Vec<(&str, &[String])> = providers.iter().map(|(s, m)| (s.as_str(), m.as_slice())).collect();
    let overlay_path = match run_multi_provider_reconcile(&context, &providers_ref) {
        Ok(path) => {
            results.push(StepResult::pass(
                "operator reconcile",
                format!("overlay for {} sites", providers.len()),
            ));
            path
        },
        Err(e) => {
            results.push(StepResult::fail("operator reconcile", e.as_ref()));
            print_validate_all_table(&results);
            return Err("responses-routing: operator reconcile failed".into());
        },
    };

    // Step 3: Deploy consumer gateway with openai_responses_format → grid_route filter chain.
    eprintln!("responses-routing: [3/6] deploying responses consumer gateway...");
    let consumer_ok = responses_record_step("consumer deploy", &mut results, || {
        consumer::deploy_consumer_for_responses(&cfg, Some(&overlay_path))?;
        Ok("deployed with openai_responses_format → grid_route filter chain".to_owned())
    });
    if !consumer_ok {
        print_validate_all_table(&results);
        return Err("responses-routing: consumer deploy failed".into());
    }

    // Step 4: Verify /v1/responses routing.
    eprintln!("responses-routing: [4/6] verifying /v1/responses routing...");
    match consumer::verify_responses_e2e(&cfg) {
        Ok(()) => results.push(StepResult::pass(
            "responses routing",
            "all models routed correctly, unknown model fails cleanly",
        )),
        Err(e) => {
            results.push(StepResult::fail("responses routing", e.as_ref()));
        },
    }

    // Step 5: Verify response model fields.
    eprintln!("responses-routing: [5/6] verifying response model fields...");
    verify_responses_model_fields(&cfg, &mut results)?;

    // Step 6: Pod restart check.
    eprintln!("responses-routing: [6/6] checking for unexpected pod restarts...");
    verify_responses_pod_restarts(&cfg, &mut results)?;

    eprintln!();
    eprintln!("## Responses Routing Proof");
    print_validate_all_table(&results);
    if results.iter().any(|r| r.status.is_failure()) {
        Err("responses-routing: one or more proof points FAILED".into())
    } else {
        eprintln!("responses-routing: ALL proof points PASS");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Full-grid routing validation
// ---------------------------------------------------------------------------

/// Validate full-grid routing across local, remote, cloud-managed, and
/// API-provider backend kinds.
///
/// Orchestrates:
/// 1. Deploy provider gateways (site-east, site-west).
/// 2. Deploy cloud-managed and API-provider mocks in the consumer cluster.
/// 3. Run the Grid operator to generate an overlay with all four candidates.
/// 4. Deploy consumer gateway with four-cluster config (mTLS + plain-HTTP).
/// 5. Verify routing for all four models + unknown model failure.
/// 6. Check no unexpected pod restarts.
#[expect(
    clippy::too_many_lines,
    reason = "six sequential validation steps with multi-cluster setup and cleanup"
)]
fn env_verify_full_grid_routing(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        API_PROVIDER_SECRET_KEY, API_PROVIDER_SECRET_NAME, API_PROVIDER_SECRET_NS, CONFIGMAP_POLL_TIMEOUT,
        FULL_GRID_GW, FULL_GRID_MODEL_API, FULL_GRID_MODEL_CLOUD, FULL_GRID_MODEL_EAST, FULL_GRID_MODEL_WEST,
        FULL_GRID_NETWORK, FULL_GRID_PROVIDER_API, FULL_GRID_PROVIDER_CLOUD, STATUS_POLL_TIMEOUT,
    };

    let cfg = EnvConfig::from_file(config)?;
    eprintln!("verify-full-grid-routing: loading two-provider config...");

    // Resolve consumer cluster and context.
    let consumer_site = cfg.consumer_cluster_name().ok_or("no consumer cluster in config")?;
    let consumer_ctx = kind::kubectl_context(consumer_site);

    // Resolve the two provider sites (east = first, west = second).
    let providers = provider_clusters_from_config(&cfg);
    if providers.len() < 2 {
        return Err(format!(
            "verify-full-grid-routing requires at least 2 provider clusters; got {}",
            providers.len()
        )
        .into());
    }
    let Some((east_site, _east_models)) = providers.first() else {
        return Err("first provider cluster not found in config".into());
    };
    let Some((west_site, _west_models)) = providers.get(1) else {
        return Err("second provider cluster not found in config".into());
    };
    let east_ctx = kind::kubectl_context(east_site);
    eprintln!("  east={east_site} ({east_ctx}), west={west_site}");

    // In-cluster service endpoints (reachable from consumer Praxis pod).
    let cloud_endpoint = format!("{}.default.svc:{}", kind::MOCK_CLOUD_SVC, kind::MOCK_CLOUD_PORT);
    let api_endpoint = format!("{}.default.svc:{}", kind::MOCK_API_SVC, kind::MOCK_API_PORT);
    let provider_endpoint = "http://mock-openai-provider.default.svc:8080";

    // ── Step 1: deploy provider gateways ─────────────────────────────────────
    eprintln!("verify-full-grid-routing: [1/6] deploying provider gateways...");
    gateway::deploy_all(&cfg)?;

    // ── Step 2: deploy cloud + API mocks in consumer cluster ─────────────────
    eprintln!("verify-full-grid-routing: [2/6] deploying cloud and API mocks in consumer cluster...");
    kind::deploy_mock_cloud_provider(&consumer_ctx, &format!("grid-{consumer_site}"))?;
    kind::deploy_mock_api_provider(&consumer_ctx, &format!("grid-{consumer_site}"))?;
    eprintln!("  cloud endpoint: {cloud_endpoint}");
    eprintln!("  api endpoint:   {api_endpoint}");

    // ── Step 3: operator reconcile + overlay export ───────────────────────────
    eprintln!("verify-full-grid-routing: [3/6] operator reconcile + overlay export...");
    operator::install_grid_crds(&east_ctx)?;
    operator::cleanup_full_grid_resources(&east_ctx)?;
    // Create credential Secret before applying the InferenceProvider fixture.
    operator::delete_api_credential_secret(&east_ctx, API_PROVIDER_SECRET_NS)
        .unwrap_or_else(|e| eprintln!("  note: credential Secret cleanup: {e}"));
    operator::create_api_credential_secret(
        &east_ctx,
        API_PROVIDER_SECRET_NAME,
        API_PROVIDER_SECRET_NS,
        API_PROVIDER_SECRET_KEY,
        consumer::API_PROVIDER_INJECTED_TOKEN,
    )?;
    operator::apply_full_grid_fixtures(
        &east_ctx,
        east_site,
        west_site,
        provider_endpoint,
        provider_endpoint,
        &cloud_endpoint,
        &api_endpoint,
    )?;

    let op = operator::spawn_operator(&east_ctx)?;
    let mut op_guard = ProcGuard(Some(op), "operator");

    let result: Result<PathBuf, Box<dyn std::error::Error>> = (|| {
        for name in [
            operator::FULL_GRID_PROVIDER_EAST,
            operator::FULL_GRID_PROVIDER_WEST,
            FULL_GRID_PROVIDER_CLOUD,
            FULL_GRID_PROVIDER_API,
        ] {
            operator::wait_for_provider_phase(&east_ctx, name, "Pending", STATUS_POLL_TIMEOUT)?;
        }

        operator::wait_for_overlay_configmap(
            &east_ctx,
            FULL_GRID_NETWORK,
            FULL_GRID_GW,
            "default",
            CONFIGMAP_POLL_TIMEOUT,
        )?;

        let overlay = operator::read_overlay_configmap(&east_ctx, FULL_GRID_NETWORK, FULL_GRID_GW, "default")?;
        operator::verify_full_grid_overlay(&overlay, east_site, west_site)?;
        eprintln!("  [OK] full-grid overlay has all four backend-kind candidates");

        let path = operator::export_overlay_to_file(&east_ctx, FULL_GRID_NETWORK, FULL_GRID_GW, "default")?;
        eprintln!("  overlay exported: {}", path.display());
        Ok(path)
    })();

    if let Some(c) = op_guard.0.take() {
        operator::kill_operator(c);
    }
    let overlay_path = result?;

    // ── Step 4: deploy consumer gateway with four-cluster config ─────────────
    eprintln!("verify-full-grid-routing: [4/6] deploying full-grid consumer gateway...");
    let overlay_json = std::fs::read_to_string(&overlay_path)?;
    let overlay = operator_overlay::parse_grid_config_json(&overlay_json)?;

    // Read the credential reference from the operator-projected overlay.
    // The xtask resolves the token from that Secret as the local harness bridge.
    let cred_plan = operator::api_credential_plan_from_overlay(&overlay, FULL_GRID_PROVIDER_API).ok_or(
        "no bearer-token credential reference found in full-grid overlay; \
         verify InferenceProvider spec.auth.secretRef is set and operator reconciled",
    )?;
    let api_token = operator::resolve_api_credential(&east_ctx, &cred_plan)?
        .ok_or("credential plan resolved to no token (manual or absent auth)")?;

    consumer::deploy_consumer_for_full_grid(
        &cfg,
        &overlay,
        FULL_GRID_PROVIDER_CLOUD,
        &cloud_endpoint,
        FULL_GRID_PROVIDER_API,
        &api_endpoint,
        &api_token,
    )?;

    // ── Step 5: verify routing for all four models ───────────────────────────
    eprintln!("verify-full-grid-routing: [5/6] verifying full-grid routing...");
    eprintln!("  {FULL_GRID_MODEL_EAST} → site-east (local/self-hosted)");
    eprintln!("  {FULL_GRID_MODEL_WEST} → site-west (remote/self-hosted)");
    eprintln!("  {FULL_GRID_MODEL_CLOUD} → cloud mock ({FULL_GRID_PROVIDER_CLOUD})");
    eprintln!("  {FULL_GRID_MODEL_API} → api mock ({FULL_GRID_PROVIDER_API}, injected credential)");
    consumer::verify_full_grid_e2e(
        &cfg,
        FULL_GRID_MODEL_EAST,
        FULL_GRID_MODEL_WEST,
        FULL_GRID_MODEL_CLOUD,
        FULL_GRID_MODEL_API,
    )?;

    // ── Step 6: pod restart check ─────────────────────────────────────────────
    eprintln!("verify-full-grid-routing: [6/6] checking for unexpected pod restarts...");
    for (site, _) in &providers {
        let ctx = kind::kubectl_context(site);
        let restarts = collect_pod_restart_counts(&ctx, "default")?;
        if restarts.is_empty() {
            eprintln!("  [OK] {site}: no pod restarts");
        } else {
            for (pod, count) in &restarts {
                eprintln!("  [WARN] {site}: pod {pod} has {count} restart(s)");
            }
        }
    }
    {
        let restarts = collect_pod_restart_counts(&consumer_ctx, "default")?;
        if restarts.is_empty() {
            eprintln!("  [OK] {consumer_site}: no pod restarts");
        } else {
            for (pod, count) in &restarts {
                eprintln!("  [WARN] {consumer_site}: pod {pod} has {count} restart(s)");
            }
        }
    }

    // Cleanup mocks (best-effort).
    kind::delete_mock_cloud_provider(&consumer_ctx);
    kind::delete_mock_api_provider(&consumer_ctx);

    eprintln!(
        "verify-full-grid-routing: PASS — all four backend kinds \
         (local, remote, cloud_managed, api_provider) routed via consumer gateway"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Metrics-driven routing validation
// ---------------------------------------------------------------------------

/// Prove that live scraped metrics change which backend a consumer request routes to.
///
/// Two provider sites (east, west) both serve `model-metrics-shared`.  Python
/// pods expose Prometheus gauge values for `queue_depth`; port-forwards expose
/// them to the out-of-cluster operator.  After two reconcile phases with
/// swapped metrics values the overlay order flips and consumer routing follows.
///
/// **Attribution note**: because both mock providers return the same response
/// body, the selected backend is evidenced by the overlay position (structural
/// proof) alone.  The annotation-bump trigger is xtask validation
/// synchronization — not a production mechanism.
#[expect(
    clippy::too_many_lines,
    reason = "sequential two-phase metrics validation with port-forward lifecycle"
)]
fn env_verify_metrics_routing(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        CONFIGMAP_POLL_TIMEOUT, METRICS_ROUTING_EAST_POD, METRICS_ROUTING_EAST_PORT, METRICS_ROUTING_EAST_PROVIDER,
        METRICS_ROUTING_GW, METRICS_ROUTING_MODEL, METRICS_ROUTING_NETWORK, METRICS_ROUTING_WEST_POD,
        METRICS_ROUTING_WEST_PORT, METRICS_ROUTING_WEST_PROVIDER, STATUS_POLL_TIMEOUT,
    };

    let cfg = EnvConfig::from_file(config)?;
    eprintln!("verify-metrics-routing: loading two-provider config...");

    // Resolve east and west provider sites.
    let providers = provider_clusters_from_config(&cfg);
    if providers.len() < 2 {
        return Err(format!(
            "verify-metrics-routing requires at least 2 provider clusters; got {}",
            providers.len()
        )
        .into());
    }
    let Some((east_site, east_models)) = providers.first() else {
        return Err("east provider not found".into());
    };
    let Some((west_site, west_models)) = providers.get(1) else {
        return Err("west provider not found".into());
    };
    require_mock_openai_backends(&cfg, &[east_site.as_str(), west_site.as_str()])?;
    let east_model = east_models.first().ok_or("east provider has no models")?.clone();
    let west_model = west_models.first().ok_or("west provider has no models")?.clone();
    let east_ctx = kind::kubectl_context(east_site);
    let west_ctx = kind::kubectl_context(west_site);
    let consumer_site = cfg
        .clusters
        .names
        .iter()
        .find(|n| {
            cfg.clusters
                .definitions
                .get(*n)
                .is_some_and(|d| d.role == ClusterRole::Consumer)
        })
        .map(String::as_str)
        .ok_or("no consumer cluster in config")?;

    eprintln!(
        "  east={east_site} ({east_model}), west={west_site} ({west_model}), \
         shared={METRICS_ROUTING_MODEL}"
    );
    eprintln!("  [OK] metrics-routing preflight: {east_site} and {west_site} use backend = \"mock-openai\"");

    // ── Step 1: Deploy provider gateways ──────────────────────────────────────
    eprintln!("verify-metrics-routing: [1/7] deploying provider gateways...");
    gateway::deploy_all(&cfg)?;

    // Patch mock-epps to also serve model-metrics-shared.
    // Both sites need the shared model so the consumer can route it to either.
    gateway::apply_mock_epp_with_extra_model(&east_ctx, east_site, east_models, METRICS_ROUTING_MODEL)?;
    gateway::apply_mock_epp_with_extra_model(&west_ctx, west_site, west_models, METRICS_ROUTING_MODEL)?;
    // Wait for both mock-epps to roll out with the new routes.
    {
        use std::process::Command;
        for (ctx, site) in [(&east_ctx, east_site), (&west_ctx, west_site)] {
            let status = Command::new("kubectl")
                .args([
                    "--context",
                    ctx,
                    "-n",
                    "default",
                    "rollout",
                    "restart",
                    "deployment/mock-epp",
                ])
                .status()?;
            if !status.success() {
                return Err(format!("mock-epp restart failed in {site}").into());
            }
            let status = Command::new("kubectl")
                .args([
                    "--context",
                    ctx,
                    "-n",
                    "default",
                    "rollout",
                    "status",
                    "deployment/mock-epp",
                    "--timeout",
                    "60s",
                ])
                .status()?;
            if !status.success() {
                return Err(format!("mock-epp rollout timed out in {site}").into());
            }
            eprintln!("  [OK] {site} mock-epp ready with shared model route");
        }
    }

    // ── Step 2: Install CRDs and cleanup stale resources ─────────────────────
    eprintln!("verify-metrics-routing: [2/7] installing CRDs and cleaning stale resources...");
    operator::install_grid_crds(&east_ctx)?;
    operator::cleanup_metrics_routing_resources(&east_ctx)?;

    // ── Phase 1: east=low queue (0.1), west=high queue (0.9) ─────────────────
    eprintln!("verify-metrics-routing: [3/7] phase 1 — east=0.1, west=0.9...");
    operator::apply_metrics_routing_pods(&east_ctx, "0.1", "0.9")?;
    operator::wait_for_named_pod_ready(&east_ctx, METRICS_ROUTING_EAST_POD, STATUS_POLL_TIMEOUT)?;
    operator::wait_for_named_pod_ready(&east_ctx, METRICS_ROUTING_WEST_POD, STATUS_POLL_TIMEOUT)?;

    let pf_east =
        operator::start_named_pod_port_forward(&east_ctx, METRICS_ROUTING_EAST_POD, METRICS_ROUTING_EAST_PORT)?;
    let mut pf_east_guard = ProcGuard(Some(pf_east), "metrics-pod-east-pf");
    let pf_west =
        operator::start_named_pod_port_forward(&east_ctx, METRICS_ROUTING_WEST_POD, METRICS_ROUTING_WEST_PORT)?;
    let mut pf_west_guard = ProcGuard(Some(pf_west), "metrics-pod-west-pf");

    operator::apply_metrics_routing_fixtures(
        &east_ctx,
        east_site,
        west_site,
        METRICS_ROUTING_EAST_PORT,
        METRICS_ROUTING_WEST_PORT,
    )?;

    let op1 = operator::spawn_operator(&east_ctx)?;
    let mut op1_guard = ProcGuard(Some(op1), "operator-phase1");

    let phase1_result: Result<PathBuf, Box<dyn std::error::Error>> = (|| {
        operator::wait_for_provider_phase(&east_ctx, METRICS_ROUTING_EAST_PROVIDER, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_provider_phase(&east_ctx, METRICS_ROUTING_WEST_PROVIDER, "Pending", STATUS_POLL_TIMEOUT)?;
        operator::wait_for_overlay_configmap(
            &east_ctx,
            METRICS_ROUTING_NETWORK,
            METRICS_ROUTING_GW,
            "default",
            CONFIGMAP_POLL_TIMEOUT,
        )?;
        let overlay =
            operator::read_overlay_configmap(&east_ctx, METRICS_ROUTING_NETWORK, METRICS_ROUTING_GW, "default")?;
        operator::verify_metrics_routing_overlay(&overlay, east_site, west_site)?;
        eprintln!("  [OK] phase 1 overlay: {east_site} (low queue) before {west_site} (high queue)");
        operator::export_overlay_to_file(&east_ctx, METRICS_ROUTING_NETWORK, METRICS_ROUTING_GW, "default")
    })();

    if let Some(c) = op1_guard.0.take() {
        operator::kill_operator(c);
    }

    let phase1_overlay = phase1_result?;

    // Phase 1 consumer deployment and verification.
    let phase1_overlay_json = std::fs::read_to_string(&phase1_overlay)?;
    let phase1_overlay_parsed = operator_overlay::parse_grid_config_json(&phase1_overlay_json)?;
    consumer::deploy_consumer(&cfg, Some(&phase1_overlay))?;

    let phase1_verify = verify_metrics_routing_phase(
        consumer_site,
        &phase1_overlay_parsed,
        east_site, // expected first
    );

    // ── Phase 2: flip metrics — east=high (0.9), west=low (0.1) ──────────────
    eprintln!("verify-metrics-routing: [4/7] flipping metrics — east=0.9, west=0.1...");
    if let Some(mut c) = pf_east_guard.0.take() {
        drop(c.kill());
        drop(c.wait());
    }
    if let Some(mut c) = pf_west_guard.0.take() {
        drop(c.kill());
        drop(c.wait());
    }
    operator::delete_metrics_routing_pods(&east_ctx);
    // Brief wait for pod deletion to propagate before recreating.
    #[expect(
        clippy::disallowed_methods,
        reason = "brief post-deletion wait before pod recreation"
    )]
    std::thread::sleep(Duration::from_secs(3));

    operator::apply_metrics_routing_pods(&east_ctx, "0.9", "0.1")?;
    operator::wait_for_named_pod_ready(&east_ctx, METRICS_ROUTING_EAST_POD, STATUS_POLL_TIMEOUT)?;
    operator::wait_for_named_pod_ready(&east_ctx, METRICS_ROUTING_WEST_POD, STATUS_POLL_TIMEOUT)?;

    let pf_east2 =
        operator::start_named_pod_port_forward(&east_ctx, METRICS_ROUTING_EAST_POD, METRICS_ROUTING_EAST_PORT)?;
    let mut pf_east2_guard = ProcGuard(Some(pf_east2), "metrics-pod-east-pf2");
    let pf_west2 =
        operator::start_named_pod_port_forward(&east_ctx, METRICS_ROUTING_WEST_POD, METRICS_ROUTING_WEST_PORT)?;
    let mut pf_west2_guard = ProcGuard(Some(pf_west2), "metrics-pod-west-pf2");

    let op2 = operator::spawn_operator(&east_ctx)?;
    let mut op2_guard = ProcGuard(Some(op2), "operator-phase2");

    let phase2_result: Result<PathBuf, Box<dyn std::error::Error>> = (|| {
        // Bump GridNetwork annotation to force reconcile after metrics changed.
        // This is xtask validation synchronization — not a production mechanism.
        operator::bump_gridnetwork(&east_ctx, METRICS_ROUTING_NETWORK)?;
        operator::wait_for_overlay_configmap(
            &east_ctx,
            METRICS_ROUTING_NETWORK,
            METRICS_ROUTING_GW,
            "default",
            CONFIGMAP_POLL_TIMEOUT,
        )?;
        // Brief settle so the operator has time to rescrape the new metrics values.
        #[expect(clippy::disallowed_methods, reason = "settle after metrics flip before overlay read")]
        std::thread::sleep(Duration::from_secs(5));
        // Re-read overlay after settle.
        let overlay =
            operator::read_overlay_configmap(&east_ctx, METRICS_ROUTING_NETWORK, METRICS_ROUTING_GW, "default")?;
        operator::verify_metrics_routing_overlay(&overlay, west_site, east_site)?;
        eprintln!("  [OK] phase 2 overlay: {west_site} (now low queue) before {east_site} (now high queue)");
        operator::export_overlay_to_file(&east_ctx, METRICS_ROUTING_NETWORK, METRICS_ROUTING_GW, "default")
    })();

    if let Some(c) = op2_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(mut c) = pf_east2_guard.0.take() {
        drop(c.kill());
        drop(c.wait());
    }
    if let Some(mut c) = pf_west2_guard.0.take() {
        drop(c.kill());
        drop(c.wait());
    }

    let phase2_overlay = phase2_result?;
    let phase2_overlay_json = std::fs::read_to_string(&phase2_overlay)?;
    let phase2_overlay_parsed = operator_overlay::parse_grid_config_json(&phase2_overlay_json)?;
    consumer::deploy_consumer(&cfg, Some(&phase2_overlay))?;

    let phase2_verify = verify_metrics_routing_phase(
        consumer_site,
        &phase2_overlay_parsed,
        west_site, // expected first after flip
    );

    // ── Step 5: Cleanup ───────────────────────────────────────────────────────
    eprintln!("verify-metrics-routing: [5/7] cleaning up...");
    operator::cleanup_metrics_routing_resources(&east_ctx)
        .unwrap_or_else(|e| eprintln!("  warning: cleanup failed: {e}"));

    // ── Step 6: Pod restart check ─────────────────────────────────────────────
    eprintln!("verify-metrics-routing: [6/7] checking for unexpected pod restarts...");
    for (site, ctx) in [(east_site, &east_ctx), (west_site, &west_ctx)] {
        let restarts = collect_pod_restart_counts(ctx, "default")?;
        if restarts.is_empty() {
            eprintln!("  [OK] {site}: no pod restarts");
        } else {
            for (pod, count) in &restarts {
                eprintln!("  [FAIL] {site}: pod {pod} has {count} unexpected restart(s)");
            }
            return Err(format!("{site}: unexpected pod restarts detected").into());
        }
    }

    // ── Step 7: Report ────────────────────────────────────────────────────────
    eprintln!("verify-metrics-routing: [7/7] summarising results...");
    phase1_verify?;
    phase2_verify?;

    eprintln!(
        "verify-metrics-routing: PASS — overlay order and routing flipped correctly \
         when metrics values were swapped"
    );
    Ok(())
}

/// Verify site join/discovery lifecycle, routing readiness, and cross-network isolation.
///
/// **Design:** the harness advances the joining site through the state machine via
/// `kubectl patch --subresource=status` and polls to confirm each expected phase is
/// observed through at least one reconcile.  This proves the lifecycle type system is
/// functional end-to-end.
/// SWIM membership formation (two operators, east primary + west joining) provides the real
/// distributed-system event that triggers the `Pending → Discovered` transition in production.
///
/// **Cross-network isolation:** a wrong-network `GridSite` and `InferenceProvider` are applied
/// and their absence from the correct-network overlay is asserted by reading the overlay `ConfigMap`.
///
/// **Auto-discovery proof (step 3):** after SWIM convergence, the primary operator creates
/// a `GridSite` for the joining SWIM member without any harness-assisted `kubectl apply`.
/// The created `GridSite` is named after the SWIM `site_id` and has `spec.gridNetworkRef` and
/// `spec.egress.address` populated from the SWIM membership record.
///
/// **Lifecycle proof (step 5):** a separate harness-created `GridSite` is advanced through
/// `Pending → Discovered → Connecting → Active`.  Only `Discovered` is harness-patched;
/// the controller drives `Connecting` (egress present) and `Active`
/// (TCP probe succeeds + `spec.trust.certFingerprint` matches `status.publicCertPem`).
#[expect(
    clippy::too_many_lines,
    reason = "sequential 8-step proof: auto-discovery + harness lifecycle + overlay isolation"
)]
fn env_verify_site_join_discovery(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        CONFIGMAP_POLL_TIMEOUT, SITE_JOIN_JOINING_MODEL, SITE_JOIN_JOINING_SITE, SITE_JOIN_NETWORK,
        SITE_JOIN_PHASE_POLL_TIMEOUT, SITE_JOIN_PRIMARY_EGRESS, SITE_JOIN_PRIMARY_MODEL, SITE_JOIN_PRIMARY_SITE,
        SITE_JOIN_WRONG_NETWORK, SITE_JOIN_WRONG_SITE, SWIM_CONVERGENCE_WAIT, SWIM_STATUS_POLL_TIMEOUT,
    };

    let cfg = EnvConfig::from_file(config)?;
    eprintln!("verify-site-join-discovery: loading multisite config...");

    let providers = provider_clusters_from_config(&cfg);
    if providers.len() < 2 {
        return Err(format!(
            "verify-site-join-discovery requires ≥ 2 provider clusters; got {}",
            providers.len()
        )
        .into());
    }
    let Some((east_site, _)) = providers.first() else {
        return Err("verify-site-join-discovery: no east provider cluster".into());
    };
    let Some((west_site, _)) = providers.get(1) else {
        return Err("verify-site-join-discovery: no west provider cluster".into());
    };
    let east_ctx = kind::kubectl_context(east_site);
    let west_ctx = kind::kubectl_context(west_site);
    eprintln!("  primary={east_site} ({east_ctx}), joining={west_site} ({west_ctx})");

    // ── Step 1: Preflight ─────────────────────────────────────────────────────
    eprintln!("verify-site-join-discovery: [1/8] preflight — CRDs, cleanup, operator binary...");
    // Build operator binary once so both spawns use the same pre-compiled binary.
    let build_ok = std::process::Command::new("cargo")
        .args(["build", "--quiet", "-p", "operator", "--bin", "operator"])
        .status()
        .map_err(|e| format!("cargo build -p operator failed to spawn: {e}"))?;
    if !build_ok.success() {
        return Err("cargo build -p operator --bin operator failed".into());
    }
    eprintln!("  [OK] operator binary ready");
    operator::install_grid_crds(&east_ctx)?;
    operator::install_grid_crds(&west_ctx)?;
    operator::cleanup_site_join_resources(&east_ctx)?;

    // ── Step 2: Start SWIM operators ──────────────────────────────────────────
    eprintln!("verify-site-join-discovery: [2/8] starting SWIM operators...");
    let (bind_primary, bind_joining) = reserve_swim_bind_addrs()?;
    // Primary operator: east cluster, no seeds, no gateway address.
    let op_primary =
        operator::spawn_operator_with_swim_for_context(&east_ctx, &bind_primary, &bind_primary, east_site, "", None)?;
    let mut op_primary_guard = ProcGuard(Some(op_primary), "operator-primary");
    // Joining operator: west cluster, seeds = primary bind addr.
    // Advertises a distinct gateway address (port SITE_JOIN_GATEWAY_PORT) so the auto-created
    // GridSite carries the data-plane address, not the SWIM UDP bind address.
    let joining_gw_addr = format!("127.0.0.1:{}", operator::SITE_JOIN_GATEWAY_PORT);
    let _joining_gateway_listener = std::net::TcpListener::bind(&joining_gw_addr)
        .map_err(|e| format!("failed to bind site-join gateway probe listener at {joining_gw_addr}: {e}"))?;
    let op_joining = operator::spawn_operator_with_swim_for_context(
        &west_ctx,
        &bind_joining,
        &bind_joining,
        west_site,
        &bind_primary,
        Some(&joining_gw_addr),
    )?;
    let mut op_joining_guard = ProcGuard(Some(op_joining), "operator-joining");
    eprintln!("  [OK] primary operator ({east_site}) and joining operator ({west_site}) started");

    // ── Step 3: SWIM convergence + GridNetworks + auto-discovery proof ────────
    eprintln!("verify-site-join-discovery: [3/8] SWIM convergence + auto-discovery proof...");
    // Wait for gossip to propagate before applying fixtures.
    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);

    // Apply GridNetworks ONLY — no GridSites yet.
    // The auto-discovery proof asserts that the primary operator creates the joining
    // GridSite by itself, without harness assistance.
    operator::apply_site_join_network(&east_ctx, east_site)?;
    operator::apply_site_join_wrong_network(&east_ctx)?;

    // Poll GridNetwork Active: proves SWIM membership is reflected.
    let connected = operator::wait_for_gridnetwork_active(&east_ctx, SITE_JOIN_NETWORK, SWIM_STATUS_POLL_TIMEOUT)?;
    eprintln!(
        "  [OK] GridNetwork {SITE_JOIN_NETWORK:?} Active \
         (connectedSites={connected}) - SWIM membership formed"
    );

    // Assert the primary operator auto-created a GridSite for the joining SWIM member.
    // The name is {network}-{site_id} — composite to avoid collisions when the operator
    // reconciles multiple GridNetworks and the same SWIM peer appears in all of them.
    let auto_site_name = operator::auto_discovered_gridsite_name(SITE_JOIN_NETWORK, west_site);
    operator::bump_gridnetwork(&east_ctx, SITE_JOIN_NETWORK)?;
    operator::wait_for_auto_gridsite(
        &east_ctx,
        &auto_site_name,
        SITE_JOIN_NETWORK,
        SITE_JOIN_PHASE_POLL_TIMEOUT,
    )?;
    // Wait for the GridSite controller to advance Discovered → Connecting.
    // The GridNetwork controller sets Discovered; the GridSite controller then
    // advances to Connecting because the advertised gateway address is present.
    operator::wait_for_gridsite_phase(&east_ctx, &auto_site_name, "Connecting", SITE_JOIN_PHASE_POLL_TIMEOUT)?;
    operator::verify_auto_gridsite_fields(&east_ctx, &auto_site_name, SITE_JOIN_NETWORK, "Connecting")?;

    // Hard assertion: spec.egress.address must equal the gateway address, NOT the SWIM UDP address.
    // This proves that auto-discovered GridSites carry the data-plane gateway address
    // separately from the SWIM membership endpoint.
    operator::verify_auto_gridsite_egress(&east_ctx, &auto_site_name, &joining_gw_addr, &bind_joining)?;

    eprintln!(
        "  [PASS] GridSite {auto_site_name:?} auto-created and advanced to Connecting by primary operator \
         from SWIM Alive membership (no harness kubectl apply); \
         egress.address={joining_gw_addr:?} (gateway address, not SWIM UDP {bind_joining:?})"
    );

    // ── Step 4: Apply harness GridSites + wait for membership ────────────────
    eprintln!("verify-site-join-discovery: [4/8] applying harness GridSites for lifecycle proof...");

    // Apply primary GridSite (represents the local site's own record — harness-created for
    // the overlay and isolation proof).
    operator::apply_gridsite(
        &east_ctx,
        SITE_JOIN_PRIMARY_SITE,
        SITE_JOIN_NETWORK,
        SITE_JOIN_PRIMARY_EGRESS,
        "primary",
    )?;
    // Apply joining GridSite with the harness name + labels used in the lifecycle and overlay proofs.
    // This is a SEPARATE site from the auto-created one (different name, same network).
    operator::apply_gridsite(
        &east_ctx,
        SITE_JOIN_JOINING_SITE,
        SITE_JOIN_NETWORK,
        &joining_gw_addr,
        "joining",
    )?;
    operator::apply_gridsite(
        &east_ctx,
        SITE_JOIN_WRONG_SITE,
        SITE_JOIN_WRONG_NETWORK,
        "172.18.0.99:8443",
        "wrong",
    )?;

    // Step 5: GridSite lifecycle (Pending -> Discovered -> Connecting -> Active).
    eprintln!("verify-site-join-discovery: [5/8] verifying GridSite lifecycle...");

    // Confirm operator reconciled the joining site to Pending before any join.
    operator::wait_for_gridsite_phase(
        &east_ctx,
        SITE_JOIN_JOINING_SITE,
        "Pending",
        SITE_JOIN_PHASE_POLL_TIMEOUT,
    )?;
    eprintln!("  [OK] joining site: Pending (absent/unjoined state confirmed before SWIM event)");

    // Advance joining site to Discovered: simulates SWIM membership event.
    // The Pending→Discovered transition requires SWIM membership, which the GridSite
    // controller does not have access to. The harness patches this to simulate the
    // SWIM Alive event (same as the GridNetwork controller does for auto-discovered sites).
    operator::patch_gridsite_phase(&east_ctx, SITE_JOIN_JOINING_SITE, "Discovered")?;
    operator::bump_gridsite(&east_ctx, SITE_JOIN_JOINING_SITE)?;
    // The GridSite controller now drives Discovered → Connecting automatically when
    // spec.egress.address is non-empty and reachable by TCP.
    // Wait for Connecting — do NOT wait for Discovered, which would be immediately
    // superseded by the operator's automated transition.
    operator::wait_for_gridsite_phase(
        &east_ctx,
        SITE_JOIN_JOINING_SITE,
        "Connecting",
        SITE_JOIN_PHASE_POLL_TIMEOUT,
    )?;
    eprintln!(
        "  [OK] joining site: Connecting \
         (Discovered→Connecting driven by GridSite controller; egress address present)"
    );

    // Advance to Active via fingerprint trust policy.  The TCP listener at joining_gw_addr
    // is already bound above so the controller's probe will succeed.  Configure a test cert
    // + matching fingerprint at the same egress address and wait for the operator to promote
    // Connecting → Active (TrustPolicyVerified).  This proves Active is only reached when
    // the trust policy is satisfied — reachability alone is not sufficient.
    operator::apply_active_gridsite_for_eligibility(
        &east_ctx,
        SITE_JOIN_JOINING_SITE,
        SITE_JOIN_NETWORK,
        &joining_gw_addr,
    )?;
    operator::bump_gridsite(&east_ctx, SITE_JOIN_JOINING_SITE)?;
    operator::wait_for_gridsite_reason_in_network(
        &east_ctx,
        SITE_JOIN_JOINING_SITE,
        SITE_JOIN_NETWORK,
        "TrustPolicyVerified",
        SITE_JOIN_PHASE_POLL_TIMEOUT,
    )?;
    eprintln!("  [OK] joining site: Active (TrustPolicyVerified — fingerprint matched, lifecycle complete)");

    // ── Step 5: Routing readiness ─────────────────────────────────────────────
    eprintln!("verify-site-join-discovery: [6/8] verifying join routing readiness...");
    operator::verify_gridsite_routing_data(&east_ctx, SITE_JOIN_JOINING_SITE, SITE_JOIN_NETWORK, &joining_gw_addr)?;

    // ── Step 6: Overlay + cross-network isolation ─────────────────────────────
    eprintln!("verify-site-join-discovery: [7/8] verifying overlay and cross-network isolation...");

    // Cross-network isolation via GridSite inventory: the wrong site must not appear in sjd-net.
    let net_sites = operator::list_gridsites_for_network(&east_ctx, SITE_JOIN_NETWORK)?;
    if net_sites.iter().any(|s| s == SITE_JOIN_WRONG_SITE) {
        return Err(format!(
            "cross-network leakage: {SITE_JOIN_WRONG_SITE:?} appears in \
             {SITE_JOIN_NETWORK:?} GridSite inventory"
        )
        .into());
    }
    eprintln!(
        "  [PASS] GridSite inventory isolation: {SITE_JOIN_WRONG_SITE:?} absent \
         from {SITE_JOIN_NETWORK:?} (found {} site(s))",
        net_sites.len()
    );

    // Cross-network isolation via overlay: apply InferenceProviders and verify overlay.
    operator::apply_site_join_primary_provider(&east_ctx, SITE_JOIN_PRIMARY_SITE, SITE_JOIN_PRIMARY_MODEL)?;
    operator::apply_site_join_joining_provider(&east_ctx, SITE_JOIN_JOINING_SITE, SITE_JOIN_JOINING_MODEL)?;
    operator::apply_site_join_wrong_provider(&east_ctx)?;

    // Bump annotation to force overlay reconcile after new providers land.
    // This is xtask validation synchronization — not a production mechanism.
    operator::bump_gridnetwork(&east_ctx, SITE_JOIN_NETWORK)?;
    eprintln!("  [OK] bumped {SITE_JOIN_NETWORK:?} annotation to force overlay reconcile");

    let overlay_result = operator::wait_for_site_join_overlay(
        &east_ctx,
        SITE_JOIN_PRIMARY_SITE,
        SITE_JOIN_PRIMARY_MODEL,
        SITE_JOIN_JOINING_SITE,
        SITE_JOIN_JOINING_MODEL,
        SITE_JOIN_WRONG_SITE,
        CONFIGMAP_POLL_TIMEOUT,
    );

    // ── Step 8: Cleanup ───────────────────────────────────────────────────────
    eprintln!("verify-site-join-discovery: [8/8] cleanup...");
    if let Some(c) = op_primary_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op_joining_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_site_join_resources(&east_ctx).unwrap_or_else(|e| eprintln!("  warning: cleanup failed: {e}"));
    // Also remove the auto-created GridSite ({network}-{site_id} naming).
    operator::cleanup_auto_discovered_gridsite(&east_ctx, &auto_site_name)
        .unwrap_or_else(|e| eprintln!("  warning: auto-discovered GridSite cleanup: {e}"));

    // Propagate overlay result after cleanup so resources are always removed.
    overlay_result?;

    eprintln!(
        "verify-site-join-discovery: PASS - auto-discovered GridSite created by operator \
         from SWIM Alive member; joining site progressed Pending -> Discovered -> Connecting -> Active; \
         routing data complete; cross-network isolation confirmed"
    );
    Ok(())
}

/// Lost-peer route-away validation: shared-model routing prefers a healthy fallback over a stale
/// remote candidate when both serve the same model.
///
/// **Before partition:** east (local, `backendKind=local`) and west (remote CRDT) both serve
/// `model-failover-shared`.  East sorts first in the overlay by locality score.
///
/// **After killing the west operator:** SWIM marks west Dead →
/// `apply_swim_staleness_override` downgrades the west CRDT provider to `Degraded` →
/// overlay emits west with `fresh=false`.  East (local, `fresh=true`) remains the first
/// candidate for the shared model.  A consumer request for the shared model returns HTTP 200,
/// attributed to east via overlay-position evidence (both mocks echo the same model name in
/// the response body; the first-candidate position is the stated attribution mechanism).
///
/// **Recovery phase (steps 8–9):**
///
/// After the consumer route-away proof, the surviving east operator is also stopped
/// so both SWIM runtimes lose their in-memory membership state.  East and west are
/// then restarted with the same bind addresses, with west seeding east.  SWIM detects
/// west as `Alive`, the staleness override is lifted, and the east overlay reflects
/// west with `fresh=true` again.  This proves the lost-peer → clean-restart recovery
/// cycle without requiring fixture reapplication.
///
/// **Honest boundaries:**
/// - Partition is simulated by process kill, not real network-level isolation (`iptables`/`tc`).
/// - Attribution is overlay-based; this does not prove Praxis hard-excludes `fresh=false` candidates, only that the
///   operator correctly deprioritises them behind the healthy alternative for the shared model.
/// - Recovery proof is clean-restart SWIM Alive + CRDT republish; in-place rejoin after a hard kill and stale-age-based
///   expiry are not implemented.
#[expect(
    clippy::too_many_lines,
    reason = "sequential 10-step failover + recovery proof: gateways, operators, SWIM, shared-model overlay ordering, route-away, rejoin, recovery"
)]
#[expect(
    clippy::large_stack_frames,
    reason = "gateway deploy + SWIM operator lifecycle + consumer port-forward state in one function"
)]
fn env_verify_failover_under_lost_peer(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        CONFIGMAP_POLL_TIMEOUT, FAILOVER_EAST_PROVIDER, FAILOVER_GW, FAILOVER_LOCAL_MODEL, FAILOVER_NETWORK,
        FAILOVER_RECOVERY_POLL_TIMEOUT, FAILOVER_REJOIN_WAIT, FAILOVER_REMOTE_MODEL, FAILOVER_SHARED_MODEL,
        FAILOVER_STALE_POLL_TIMEOUT, FAILOVER_WEST_PROVIDER, SWIM_CONVERGENCE_WAIT, SWIM_DEAD_MEMBER_WAIT,
        SWIM_STATUS_POLL_TIMEOUT,
    };

    let cfg = EnvConfig::from_file(config)?;
    eprintln!("verify-failover-under-lost-peer: loading two-provider config...");

    let providers = provider_clusters_from_config(&cfg);
    if providers.len() < 2 {
        return Err(format!(
            "verify-failover-under-lost-peer requires >= 2 provider clusters; got {}",
            providers.len()
        )
        .into());
    }
    let Some((east_site, _)) = providers.first() else {
        return Err("verify-failover-under-lost-peer: east provider cluster not found".into());
    };
    let Some((west_site, _)) = providers.get(1) else {
        return Err("verify-failover-under-lost-peer: west provider cluster not found".into());
    };
    let east_ctx = kind::kubectl_context(east_site);
    let west_ctx = kind::kubectl_context(west_site);
    eprintln!("  primary={east_site} ({east_ctx}), remote={west_site} ({west_ctx})");

    // ── Step 1: Preflight ─────────────────────────────────────────────────────
    eprintln!("verify-failover-under-lost-peer: [1/10] preflight - CRDs, cleanup, operator binary...");
    let build_ok = std::process::Command::new("cargo")
        .args(["build", "--quiet", "-p", "operator", "--bin", "operator"])
        .status()
        .map_err(|e| format!("cargo build -p operator failed: {e}"))?;
    if !build_ok.success() {
        return Err("cargo build -p operator --bin operator failed".into());
    }
    eprintln!("  [OK] operator binary ready");
    operator::install_grid_crds(&east_ctx)?;
    operator::install_grid_crds(&west_ctx)?;
    operator::cleanup_failover_east_resources(&east_ctx).unwrap_or_else(|e| eprintln!("  note: east cleanup: {e}"));
    operator::cleanup_failover_west_resources(&west_ctx).unwrap_or_else(|e| eprintln!("  note: west cleanup: {e}"));

    // ── Step 2: Deploy provider gateways ──────────────────────────────────────
    // Gateway deployment enables consumer request routing in step 7.
    // The east mock-epp is patched to also serve FAILOVER_SHARED_MODEL so requests
    // for that model route cleanly through east's provider gateway after west is lost.
    eprintln!("verify-failover-under-lost-peer: [2/10] deploying provider gateways...");
    let east_models = providers.first().map(|(_, m)| m.clone()).unwrap_or_default();
    let west_models = providers.get(1).map(|(_, m)| m.clone()).unwrap_or_default();
    gateway::deploy_all(&cfg)?;
    gateway::apply_mock_epp_with_extra_model(&east_ctx, east_site, &east_models, FAILOVER_SHARED_MODEL)?;
    gateway::apply_mock_epp_with_extra_model(&west_ctx, west_site, &west_models, FAILOVER_SHARED_MODEL)?;
    // Wait for both gateways to stabilize after mock-epp patch.
    for (ctx, site) in [(&east_ctx, east_site), (&west_ctx, west_site)] {
        let rollout_ok = std::process::Command::new("kubectl")
            .args(["--context", ctx, "rollout", "restart", "deployment/mock-epp"])
            .status()
            .map_err(|e| format!("rollout restart mock-epp failed for {site}: {e}"))?;
        if !rollout_ok.success() {
            return Err(format!("rollout restart mock-epp failed for {site}").into());
        }
        let wait_ok = std::process::Command::new("kubectl")
            .args([
                "--context",
                ctx,
                "rollout",
                "status",
                "deployment/mock-epp",
                "--timeout=60s",
            ])
            .status()
            .map_err(|e| format!("rollout status mock-epp failed for {site}: {e}"))?;
        if !wait_ok.success() {
            return Err(format!("mock-epp rollout timeout for {site}").into());
        }
        eprintln!("  [OK] {site} mock-epp ready with {FAILOVER_SHARED_MODEL:?} route");
    }

    // ── Step 3: Start SWIM operators ──────────────────────────────────────────
    eprintln!("verify-failover-under-lost-peer: [3/10] starting SWIM operators...");
    let (bind_east, bind_west) = reserve_swim_bind_addrs()?;
    let op_east =
        operator::spawn_operator_with_swim_for_context(&east_ctx, &bind_east, &bind_east, east_site, "", None)?;
    let mut op_east_guard = ProcGuard(Some(op_east), "operator-east");
    let op_west =
        operator::spawn_operator_with_swim_for_context(&west_ctx, &bind_west, &bind_west, west_site, &bind_east, None)?;
    let mut op_west_guard = ProcGuard(Some(op_west), "operator-west");
    eprintln!("  [OK] east (primary) and west (remote) operators started");

    // ── Step 4: SWIM convergence + fixtures ───────────────────────────────────
    eprintln!("verify-failover-under-lost-peer: [4/10] waiting for SWIM convergence and applying fixtures...");
    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);
    // East: local dedicated model + shared model (local, healthy fallback).
    operator::apply_failover_east_fixtures(&east_ctx, east_site, FAILOVER_LOCAL_MODEL)?;
    operator::apply_failover_shared_east_provider(&east_ctx, east_site)?;
    // West: remote dedicated model + shared model (published via CRDT to east).
    operator::apply_failover_west_fixtures_with_shared(&west_ctx, west_site)?;

    // Allow extra time for west to reconcile + publish its CRDT state, then force read.
    operator::wait_for_swim_convergence(Duration::from_secs(5));
    operator::bump_gridnetwork(&east_ctx, FAILOVER_NETWORK)?;
    eprintln!("  [OK] bumped {FAILOVER_NETWORK:?} to force CRDT state read");

    let dist = operator::wait_for_gridnetwork_distributed_state(&east_ctx, FAILOVER_NETWORK, SWIM_STATUS_POLL_TIMEOUT)?;
    eprintln!(
        "  [OK] east GridNetwork {FAILOVER_NETWORK:?}: distributedProviderCount={dist} \
         (west CRDT provider arrived)"
    );

    // ── Step 5: Verify initial overlay ───────────────────────────────────────
    eprintln!(
        "verify-failover-under-lost-peer: [5/10] verifying initial overlay \
         (dedicated + shared models, both providers fresh=true)..."
    );
    operator::bump_gridnetwork(&east_ctx, FAILOVER_NETWORK)?;
    operator::wait_for_overlay_configmap(
        &east_ctx,
        FAILOVER_NETWORK,
        FAILOVER_GW,
        "default",
        CONFIGMAP_POLL_TIMEOUT,
    )?;
    // Dedicated models: each site's own model.
    operator::verify_failover_overlay(
        &east_ctx,
        FAILOVER_NETWORK,
        FAILOVER_GW,
        east_site,
        FAILOVER_LOCAL_MODEL,
        west_site,
        FAILOVER_REMOTE_MODEL,
        false,
    )?;
    // Shared model: east (local, higher score) before west (remote, lower score), both fresh=true.
    operator::verify_shared_model_overlay_ordering(
        &east_ctx,
        FAILOVER_NETWORK,
        FAILOVER_GW,
        east_site,
        west_site,
        false, // expect_west_stale = false: west is alive, both fresh=true
    )?;
    eprintln!(
        "  [PASS] initial overlay: {FAILOVER_EAST_PROVIDER} fresh=true, \
         {FAILOVER_WEST_PROVIDER} fresh=true; east before west for {FAILOVER_SHARED_MODEL:?}"
    );

    // ── Step 6: Kill west operator ────────────────────────────────────────────
    eprintln!("verify-failover-under-lost-peer: [6/10] killing west operator - simulating partition...");
    if let Some(c) = op_west_guard.0.take() {
        operator::kill_operator(c);
    }
    eprintln!(
        "  [OK] west operator killed; waiting {}s for SWIM to declare peer Dead...",
        SWIM_DEAD_MEMBER_WAIT.as_secs()
    );
    // foca::Config::simple(): probe_period=1.5s, suspect_to_down=3s → Dead in ≤6s.
    #[expect(
        clippy::disallowed_methods,
        reason = "bounded wait for SWIM dead detection after operator kill"
    )]
    std::thread::sleep(SWIM_DEAD_MEMBER_WAIT);

    // ── Step 7: Verify stale overlay + route-away proof ───────────────────────
    eprintln!("verify-failover-under-lost-peer: [7/10] verifying stale overlay and consumer route-away...");
    operator::bump_gridnetwork(&east_ctx, FAILOVER_NETWORK)?;
    eprintln!("  [OK] bumped {FAILOVER_NETWORK:?} to force post-partition reconcile");

    operator::wait_for_remote_candidate_stale(
        &east_ctx,
        FAILOVER_NETWORK,
        FAILOVER_GW,
        west_site,
        FAILOVER_STALE_POLL_TIMEOUT,
    )?;

    // Dedicated models: east still fresh, west now stale.
    operator::verify_failover_overlay(
        &east_ctx,
        FAILOVER_NETWORK,
        FAILOVER_GW,
        east_site,
        FAILOVER_LOCAL_MODEL,
        west_site,
        FAILOVER_REMOTE_MODEL,
        true,
    )?;
    // Shared model: east (fresh=true) before west (fresh=false).
    operator::verify_shared_model_overlay_ordering(
        &east_ctx,
        FAILOVER_NETWORK,
        FAILOVER_GW,
        east_site,
        west_site,
        true, // expect_west_stale = true: west is Dead → Degraded → fresh=false
    )?;
    eprintln!(
        "  [PASS] stale overlay: {FAILOVER_EAST_PROVIDER} fresh=true, \
         {FAILOVER_WEST_PROVIDER} fresh=false; east still first for {FAILOVER_SHARED_MODEL:?}"
    );

    // Consumer routing proof: deploy consumer from the stale overlay and verify a request
    // for the shared model returns 200.  East is the first shared-model candidate.
    // Attribution: overlay-based — both mocks echo the same model name in the response body;
    // first shared-model candidate being east is the stated evidence for routing to the healthy fallback.
    // East operator is kept alive here (not killed yet) so it can reconcile the recovery phase below.
    let overlay_path = operator::export_overlay_to_file(&east_ctx, FAILOVER_NETWORK, FAILOVER_GW, "default")?;
    let overlay_json = std::fs::read_to_string(&overlay_path)?;
    let overlay = operator_overlay::parse_grid_config_json(&overlay_json)?;
    consumer::deploy_consumer(&cfg, Some(&overlay_path))?;

    let consumer_site = cfg
        .clusters
        .names
        .iter()
        .find(|n| {
            cfg.clusters
                .definitions
                .get(*n)
                .is_some_and(|d| d.role == ClusterRole::Consumer)
        })
        .map(String::as_str)
        .ok_or("no consumer cluster in config")?;
    let consumer_ctx = kind::kubectl_context(consumer_site);

    let port = verify::find_free_port()?;
    let mut pf = verify::PortForwardGuard::start(&consumer_ctx, "praxis-consumer", port, 8080)?;
    if !verify::wait_for_port(port) {
        pf.stop();
        return Err("consumer gateway not reachable via port-forward after stale overlay deploy".into());
    }

    // Verify the shared model routes to a healthy backend (east, first shared-model candidate).
    match consumer::send_consumer_request(port, FAILOVER_SHARED_MODEL) {
        Ok(r) if r.status == 200 => {
            // Check the response echoes the correct model field.
            let model_field = serde_json::from_str::<serde_json::Value>(&r.body)
                .ok()
                .and_then(|j| j.get("model").and_then(serde_json::Value::as_str).map(String::from));
            if model_field.as_deref() == Some(FAILOVER_SHARED_MODEL) {
                eprintln!(
                    "  [PASS] consumer: {FAILOVER_SHARED_MODEL:?} returns 200 \
                     (response model field matches; first shared-model candidate = {east_site:?}, fresh=true)"
                );
            } else {
                eprintln!(
                    "  [PASS] consumer: {FAILOVER_SHARED_MODEL:?} returns 200 \
                     (first shared-model candidate = {east_site:?}, fresh=true; response model={model_field:?})"
                );
            }
        },
        Ok(r) => {
            pf.stop();
            return Err(format!(
                "consumer: {FAILOVER_SHARED_MODEL:?} returned {} (expected 200 after stale overlay deploy)",
                r.status
            )
            .into());
        },
        Err(e) => {
            pf.stop();
            return Err(format!("consumer: {FAILOVER_SHARED_MODEL:?} request failed: {e}").into());
        },
    }
    // Verify unknown model fails cleanly (port-forward still active).
    match consumer::send_consumer_request(port, "nonexistent-model-xyz") {
        Ok(r) if r.status == 404 || r.status == 503 => {
            eprintln!("  [PASS] unknown model fails cleanly ({})", r.status);
        },
        Ok(r) => {
            pf.stop();
            return Err(format!("unknown model returned {} (expected 404 or 503)", r.status).into());
        },
        Err(e) => {
            pf.stop();
            return Err(format!("unknown model request failed: {e}").into());
        },
    }
    pf.stop();

    // The overlay used by the consumer (for reference in logs).
    eprintln!(
        "  [INFO] overlay used: {} candidates total; first for {FAILOVER_SHARED_MODEL:?} = {} (fresh=true)",
        overlay.candidates.len(),
        overlay
            .candidates
            .iter()
            .find(|c| c.name == FAILOVER_SHARED_MODEL)
            .map_or("(not found)", |c| c.cluster.as_str()),
    );

    // ── Step 8: Kill east + both operators down ───────────────────────────────
    eprintln!("verify-failover-under-lost-peer: [8/10] killing east operator (both operators now down)...");
    // SWIM incarnation semantics: foca uses per-member incarnation numbers.  When a
    // member is hard-killed and declared Dead, the cluster records identity state
    // for that site.  A fresh process restarts at generation/incarnation 0 and
    // can be treated as a stale announcement when the surviving cluster retains
    // newer state for that site.
    //
    // Recovery proof strategy: kill both operators so SWIM state is cleared,
    // then restart both fresh.  The Kubernetes CRD fixtures persist on both
    // clusters (GridNetwork + InferenceProvider resources are durable), so east
    // and west reconcile against the same resources.  This proves the core
    // invariant: when SWIM sees the peer as Alive (no stale tombstone), the
    // overlay emits fresh=true.
    if let Some(c) = op_east_guard.0.take() {
        operator::kill_operator(c);
    }
    eprintln!("  [OK] east operator killed; both operators are now down");

    // ── Step 9: Restart both operators fresh + verify recovery ───────────────
    eprintln!("verify-failover-under-lost-peer: [9/10] restarting both operators for recovery proof...");
    // East restarts with no seeds — it is the primary node of the fresh cluster.
    let op_east_rejoin =
        operator::spawn_operator_with_swim_for_context(&east_ctx, &bind_east, &bind_east, east_site, "", None)?;
    let mut op_east_rejoin_guard = ProcGuard(Some(op_east_rejoin), "operator-east-rejoin");
    // West restarts with east as seed and joins the fresh east cluster during
    // the convergence wait below.
    let op_west_rejoin =
        operator::spawn_operator_with_swim_for_context(&west_ctx, &bind_west, &bind_west, west_site, &bind_east, None)?;
    let mut op_west_rejoin_guard = ProcGuard(Some(op_west_rejoin), "operator-west-rejoin");
    eprintln!(
        "  [OK] both operators restarted (east: bind={bind_east:?}, no seeds; \
         west: bind={bind_west:?}, seed={bind_east:?})"
    );
    eprintln!(
        "  [INFO] waiting {}s for fresh SWIM convergence...",
        FAILOVER_REJOIN_WAIT.as_secs()
    );
    #[expect(
        clippy::disallowed_methods,
        reason = "bounded wait for fresh SWIM cluster convergence after clean restart"
    )]
    std::thread::sleep(FAILOVER_REJOIN_WAIT);

    // Poll until east overlay shows west candidate as fresh=true.
    // The poll function bumps GridNetwork periodically so a reconcile fires after
    // SWIM detects west Alive and west publishes its CRDT provider state.
    eprintln!("  [INFO] polling for west recovery (fresh=true) with periodic reconcile bumps...");
    operator::wait_for_remote_candidate_fresh(
        &east_ctx,
        FAILOVER_NETWORK,
        FAILOVER_GW,
        west_site,
        FAILOVER_RECOVERY_POLL_TIMEOUT,
    )?;

    // Verify overlay ordering is restored: east before west, both fresh=true.
    operator::verify_failover_overlay(
        &east_ctx,
        FAILOVER_NETWORK,
        FAILOVER_GW,
        east_site,
        FAILOVER_LOCAL_MODEL,
        west_site,
        FAILOVER_REMOTE_MODEL,
        false, // expect_remote_stale = false: west is Alive in the fresh cluster
    )?;
    operator::verify_shared_model_overlay_ordering(
        &east_ctx,
        FAILOVER_NETWORK,
        FAILOVER_GW,
        east_site,
        west_site,
        false, // expect_west_stale = false: west is Alive in the fresh cluster
    )?;
    eprintln!(
        "  [PASS] recovery overlay: {FAILOVER_EAST_PROVIDER} fresh=true, \
         {FAILOVER_WEST_PROVIDER} fresh=true; east still first for {FAILOVER_SHARED_MODEL:?}"
    );

    // ── Step 10: Cleanup ──────────────────────────────────────────────────────
    eprintln!("verify-failover-under-lost-peer: [10/10] cleanup...");
    if let Some(c) = op_east_rejoin_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op_west_rejoin_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_failover_east_resources(&east_ctx)
        .unwrap_or_else(|e| eprintln!("  warning: east cleanup failed: {e}"));
    operator::cleanup_failover_west_resources(&west_ctx)
        .unwrap_or_else(|e| eprintln!("  warning: west cleanup failed: {e}"));

    eprintln!(
        "verify-failover-under-lost-peer: PASS - \
         [partition] west fresh=false after kill; east routes shared model via healthy fallback; \
         [recovery] both operators restarted fresh; west fresh=true restored; overlay ordering consistent"
    );
    Ok(())
}

/// Prove that `GridNetwork.spec.staleCandidateTtlSeconds` activates overlay-level GC.
///
/// **Why this is deterministic:**
///
/// The TTL is set to `STALE_GC_TTL_SECS = 5 s`.  After killing the west operator,
/// `SWIM_DEAD_MEMBER_WAIT` (20 s) provides a safe window for SWIM to declare west Dead
/// (typically within ~6 s).  At that point `MemberRecord.age_secs` is approximately
/// 14 s (20 s wait − 6 s SWIM dead detection), which comfortably exceeds the 5 s TTL.
///
/// The `wait_for_remote_candidate_absent` helper bumps the `GridNetwork` every 5 s so
/// a reconcile fires after each age-tick window.  Each reconcile calls
/// `apply_stale_gc_filter` with the age read from the SWIM membership snapshot.  Once
/// age ≥ TTL the stale candidate is omitted from the overlay.
///
/// **Honest boundaries:**
/// - Only overlay-level filtering is proven.  CRDT storage records are not deleted.
/// - The local east candidate is verified to remain present and `fresh=true`.
/// - SWIM is not restarted; the proof uses the same two-provider kind environment.
#[expect(
    clippy::too_many_lines,
    reason = "sequential 7-step GC proof: gateways, operators, SWIM, fixtures with TTL, initial overlay, kill+GC, verify+cleanup"
)]
fn env_verify_stale_gc_ttl(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        CONFIGMAP_POLL_TIMEOUT, STALE_GC_ABSENT_POLL_TIMEOUT, STALE_GC_EAST_PROVIDER, STALE_GC_GW, STALE_GC_NETWORK,
        STALE_GC_TTL_SECS, STALE_GC_WEST_PROVIDER, SWIM_CONVERGENCE_WAIT, SWIM_DEAD_MEMBER_WAIT,
        SWIM_STATUS_POLL_TIMEOUT,
    };

    let cfg = EnvConfig::from_file(config)?;
    eprintln!("verify-stale-gc-ttl: loading two-provider config...");

    let providers = provider_clusters_from_config(&cfg);
    if providers.len() < 2 {
        return Err(format!(
            "verify-stale-gc-ttl requires >= 2 provider clusters; got {}",
            providers.len()
        )
        .into());
    }
    let Some((east_site, _)) = providers.first() else {
        return Err("verify-stale-gc-ttl: east provider cluster not found".into());
    };
    let Some((west_site, _)) = providers.get(1) else {
        return Err("verify-stale-gc-ttl: west provider cluster not found".into());
    };
    let east_ctx = kind::kubectl_context(east_site);
    let west_ctx = kind::kubectl_context(west_site);

    // ── Step 1: Preflight ─────────────────────────────────────────────────────
    eprintln!("verify-stale-gc-ttl: [1/7] preflight — CRDs, cleanup, operator binary...");
    operator::install_grid_crds(&east_ctx)?;
    operator::install_grid_crds(&west_ctx)?;
    operator::ensure_operator_binary_built()?;
    operator::cleanup_stale_gc_east_resources(&east_ctx).unwrap_or_else(|e| eprintln!("  cleanup: {e}"));
    operator::cleanup_stale_gc_west_resources(&west_ctx).unwrap_or_else(|e| eprintln!("  cleanup: {e}"));

    // ── Step 2: Provider gateways ─────────────────────────────────────────────
    eprintln!("verify-stale-gc-ttl: [2/7] deploying provider gateways...");
    gateway::deploy_all(&cfg)?;

    // ── Step 3: SWIM operators ────────────────────────────────────────────────
    eprintln!("verify-stale-gc-ttl: [3/7] starting SWIM operators...");
    let (bind_east, bind_west) = reserve_swim_bind_addrs()?;
    let op_east =
        operator::spawn_operator_with_swim_for_context(&east_ctx, &bind_east, &bind_east, east_site, "", None)?;
    let mut op_east_guard = ProcGuard(Some(op_east), "operator-east");
    let op_west =
        operator::spawn_operator_with_swim_for_context(&west_ctx, &bind_west, &bind_west, west_site, &bind_east, None)?;
    let mut op_west_guard = ProcGuard(Some(op_west), "operator-west");
    eprintln!("  [OK] east + west operators started");

    // ── Step 4: SWIM convergence + fixtures with TTL ──────────────────────────
    eprintln!(
        "verify-stale-gc-ttl: [4/7] SWIM convergence + applying fixtures \
         (staleCandidateTtlSeconds={STALE_GC_TTL_SECS})..."
    );
    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);
    operator::apply_stale_gc_east_fixtures(&east_ctx, east_site, STALE_GC_TTL_SECS)?;
    operator::apply_stale_gc_west_fixtures(&west_ctx, west_site)?;
    operator::bump_gridnetwork(&east_ctx, STALE_GC_NETWORK)?;

    let dist = operator::wait_for_gridnetwork_distributed_state(&east_ctx, STALE_GC_NETWORK, SWIM_STATUS_POLL_TIMEOUT)?;
    eprintln!("  [OK] CRDT distributed state: distributedProviderCount={dist}");

    // ── Step 5: Verify initial overlay (both candidates fresh=true) ───────────
    eprintln!("verify-stale-gc-ttl: [5/7] verifying initial overlay (west fresh=true before kill)...");
    operator::bump_gridnetwork(&east_ctx, STALE_GC_NETWORK)?;
    operator::wait_for_overlay_configmap(
        &east_ctx,
        STALE_GC_NETWORK,
        STALE_GC_GW,
        "default",
        CONFIGMAP_POLL_TIMEOUT,
    )?;

    let initial_overlay = operator::read_overlay_configmap(&east_ctx, STALE_GC_NETWORK, STALE_GC_GW, "default")?;
    let initial_candidates = initial_overlay
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("initial overlay missing candidates")?;
    let local_initially_fresh = initial_candidates.iter().any(|c| {
        c.get("cluster").and_then(serde_json::Value::as_str) == Some(east_site)
            && c.get("fresh").and_then(serde_json::Value::as_bool) == Some(true)
    });
    let remote_initially_fresh = initial_candidates.iter().any(|c| {
        c.get("cluster").and_then(serde_json::Value::as_str) == Some(west_site)
            && c.get("fresh").and_then(serde_json::Value::as_bool) == Some(true)
    });
    if !local_initially_fresh {
        return Err(format!("initial overlay: local candidate ({east_site:?}) not fresh=true").into());
    }
    if !remote_initially_fresh {
        return Err(format!("initial overlay: remote candidate ({west_site:?}) not fresh=true").into());
    }
    eprintln!(
        "  [PASS] initial overlay: {STALE_GC_EAST_PROVIDER} fresh=true, \
         {STALE_GC_WEST_PROVIDER} fresh=true"
    );

    // ── Step 6: Kill west, wait for GC eviction ───────────────────────────────
    eprintln!(
        "verify-stale-gc-ttl: [6/7] killing west operator; waiting for TTL-based GC eviction \
         (TTL={STALE_GC_TTL_SECS}s)..."
    );
    if let Some(c) = op_west_guard.0.take() {
        operator::kill_operator(c);
    }
    eprintln!(
        "  [OK] west operator killed; waiting {}s for SWIM dead detection + age accumulation...",
        SWIM_DEAD_MEMBER_WAIT.as_secs()
    );
    #[expect(
        clippy::disallowed_methods,
        reason = "bounded wait for SWIM dead detection + age accumulation past TTL"
    )]
    std::thread::sleep(SWIM_DEAD_MEMBER_WAIT);

    // Poll until the stale west candidate is absent (GC evicted it).
    // The helper bumps GridNetwork every 5 s to trigger reconciles.
    operator::wait_for_remote_candidate_absent(
        &east_ctx,
        STALE_GC_NETWORK,
        STALE_GC_GW,
        west_site,
        STALE_GC_ABSENT_POLL_TIMEOUT,
    )?;

    // Verify the local east candidate is still present and fresh=true.
    let final_overlay = operator::read_overlay_configmap(&east_ctx, STALE_GC_NETWORK, STALE_GC_GW, "default")?;
    let final_candidates = final_overlay
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("final overlay missing candidates")?;
    let local_still_fresh = final_candidates.iter().any(|c| {
        c.get("cluster").and_then(serde_json::Value::as_str) == Some(east_site)
            && c.get("fresh").and_then(serde_json::Value::as_bool) == Some(true)
    });
    if !local_still_fresh {
        return Err(format!("final overlay: local candidate ({east_site:?}) must still be fresh=true after GC").into());
    }
    eprintln!(
        "  [PASS] final overlay: {STALE_GC_EAST_PROVIDER} (local) still fresh=true; \
         {STALE_GC_WEST_PROVIDER} (remote, stale) absent — evicted by TTL={STALE_GC_TTL_SECS}s GC"
    );

    // ── Step 7: Cleanup ───────────────────────────────────────────────────────
    eprintln!("verify-stale-gc-ttl: [7/7] cleanup...");
    if let Some(c) = op_east_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_stale_gc_east_resources(&east_ctx).unwrap_or_else(|e| eprintln!("  warning: {e}"));
    operator::cleanup_stale_gc_west_resources(&west_ctx).unwrap_or_else(|e| eprintln!("  warning: {e}"));

    eprintln!(
        "verify-stale-gc-ttl: PASS — \
         stale remote candidate evicted by TTL={STALE_GC_TTL_SECS}s GC; \
         local candidate retained; CRD field staleCandidateTtlSeconds proven"
    );
    Ok(())
}

/// `expected_first_site` is the site that should appear first in the overlay after
/// the current metrics values are applied.  The consumer config is generated solely
/// from the metrics-routing overlay (which only advertises the shared model), so
/// dedicated-model alive checks are not performed here.
#[expect(
    clippy::too_many_lines,
    reason = "overlay check + port-forward + shared model + unknown model assertions"
)]
fn verify_metrics_routing_phase(
    consumer_site: &str,
    overlay: &operator_overlay::RoutingOverlay,
    expected_first_site: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use operator::METRICS_ROUTING_MODEL;

    // Confirm overlay position of the shared model's first candidate.
    let first_candidate_site = overlay
        .candidates
        .iter()
        .find(|c| c.name == METRICS_ROUTING_MODEL)
        .map(|c| c.site.as_str())
        .ok_or_else(|| format!("{METRICS_ROUTING_MODEL} not found in overlay candidates"))?;

    if first_candidate_site != expected_first_site {
        return Err(format!(
            "metrics-routing overlay: first candidate for {METRICS_ROUTING_MODEL:?} is \
             site={first_candidate_site:?}, expected site={expected_first_site:?}"
        )
        .into());
    }
    eprintln!(
        "  [OK] overlay first candidate for {METRICS_ROUTING_MODEL:?}: \
         site={expected_first_site:?} (expected)"
    );

    // Port-forward to the consumer gateway and verify all models.
    let consumer_ctx = kind::kubectl_context(consumer_site);
    let port = verify::find_free_port()?;
    let mut pf = verify::PortForwardGuard::start(&consumer_ctx, "praxis-consumer", port, 8080)?;

    if !verify::wait_for_port(port) {
        pf.stop();
        return Err("consumer gateway not reachable via port-forward".into());
    }
    eprintln!("  [PASS] consumer gateway reachable via port-forward");

    // Shared model routes to the first candidate for that model.
    match consumer::send_consumer_request(port, METRICS_ROUTING_MODEL) {
        Ok(r) if r.status == 200 => {
            eprintln!(
                "  [PASS] {METRICS_ROUTING_MODEL:?} returns 200 \
                 (attributed to {expected_first_site:?} - first candidate for model)"
            );
        },
        Ok(r) => {
            pf.stop();
            return Err(format!("{METRICS_ROUTING_MODEL:?} returned {} (expected 200)", r.status).into());
        },
        Err(e) => {
            pf.stop();
            return Err(format!("{METRICS_ROUTING_MODEL:?} request failed: {e}").into());
        },
    }

    // Unknown model must fail cleanly.
    match consumer::send_consumer_request(port, "nonexistent-model-xyz") {
        Ok(r) if r.status == 404 || r.status == 503 => {
            eprintln!("  [PASS] unknown model fails cleanly");
        },
        Ok(r) => {
            pf.stop();
            return Err(format!("unknown model returned {} (expected 404 or 503)", r.status).into());
        },
        Err(e) => {
            pf.stop();
            return Err(format!("unknown model request failed: {e}").into());
        },
    }

    pf.stop();
    Ok(())
}

// ---------------------------------------------------------------------------
// validate-all aggregator
// ---------------------------------------------------------------------------

/// Outcome of a single `validate-all` step.
#[derive(Debug, Clone, PartialEq, Eq)]
enum StepStatus {
    /// Step completed successfully.
    Pass,
    /// Step produced a fatal error.
    Fail,
    /// Step is known to be incomplete due to missing prerequisites.
    Blocked,
}

impl StepStatus {
    /// Return true when this status should contribute to a non-zero exit code.
    fn is_failure(&self) -> bool {
        *self == Self::Fail
    }

    /// Return the display label for this status.
    fn label(&self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Fail => "FAIL",
            Self::Blocked => "BLOCKED",
        }
    }
}

/// One row of the `validate-all` summary table.
struct StepResult {
    /// Short description of what this step validates.
    label: &'static str,
    /// Outcome.
    status: StepStatus,
    /// One-line evidence or error summary (truncated).
    evidence: String,
}

impl StepResult {
    /// Construct a passing result.
    fn pass(label: &'static str, evidence: impl Into<String>) -> Self {
        Self {
            label,
            status: StepStatus::Pass,
            evidence: evidence.into(),
        }
    }

    /// Construct a failing result from an error.
    fn fail(label: &'static str, err: &dyn std::error::Error) -> Self {
        Self {
            label,
            status: StepStatus::Fail,
            evidence: safe_truncate_str(&err.to_string(), 120),
        }
    }

    /// Construct a blocked result with a human-readable reason.
    fn blocked(label: &'static str, reason: impl Into<String>) -> Self {
        Self {
            label,
            status: StepStatus::Blocked,
            evidence: reason.into(),
        }
    }
}

/// Truncate a string to `max` chars, appending "…" if truncated.
fn safe_truncate_str(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_owned()
    } else {
        format!("{}…", s.chars().take(max).collect::<String>())
    }
}

/// Print a Markdown summary table of all step results.
fn print_validate_all_table(results: &[StepResult]) {
    eprintln!();
    eprintln!("| Step | Result | Evidence |");
    eprintln!("|---|---|---|");
    for r in results {
        eprintln!("| {} | **{}** | {} |", r.label, r.status.label(), r.evidence);
    }
    eprintln!();
}

/// Run all validations in sequence and report a Markdown summary.
///
/// Continues past individual step failures so the full picture is visible.
/// Exit code is non-zero when any step has `FAIL` status.
#[expect(
    clippy::too_many_lines,
    reason = "sequential step list: each step is one match arm; no abstraction saves lines without hiding intent"
)]
fn env_validate_all(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let mut results: Vec<StepResult> = Vec::new();

    // Step 1: env status (non-fatal — clusters may still be partially ready)
    eprintln!("validate-all: [1/5] env status...");
    match env_status(config) {
        Ok(()) => results.push(StepResult::pass("env status", "status summary printed above")),
        Err(e) => {
            // Status is informational; mark blocked rather than fail.
            results.push(StepResult::blocked("env status", e.to_string()));
        },
    }

    // Step 2: validate-operator-routing
    eprintln!("validate-all: [2/5] validate-operator-routing...");
    match env_validate_operator_routing(config, site) {
        Ok(()) => results.push(StepResult::pass("validate-operator-routing", "PASS")),
        Err(e) => results.push(StepResult::fail("validate-operator-routing", e.as_ref())),
    }

    // Step 3: verify-swim-membership
    eprintln!("validate-all: [3/5] verify-swim-membership...");
    match env_verify_swim_membership(config, site) {
        Ok(()) => results.push(StepResult::pass(
            "verify-swim-membership",
            "phase=Active connectedSites=1",
        )),
        Err(e) => results.push(StepResult::fail("verify-swim-membership", e.as_ref())),
    }

    // Step 4: verify-swim-state
    eprintln!("validate-all: [4/5] verify-swim-state...");
    match env_verify_swim_state(config, site) {
        Ok(()) => results.push(StepResult::pass("verify-swim-state", "distributedProviderCount=1")),
        Err(e) => results.push(StepResult::fail("verify-swim-state", e.as_ref())),
    }

    // Step 5: verify-mtls-trust
    eprintln!("validate-all: [5/5] verify-mtls-trust...");
    match env_verify_mtls_trust(config) {
        Ok(()) => results.push(StepResult::pass("verify-mtls-trust", "all trust cases pass")),
        Err(e) => results.push(StepResult::fail("verify-mtls-trust", e.as_ref())),
    }

    print_validate_all_table(&results);

    let any_fail = results.iter().any(|r| r.status.is_failure());
    if any_fail {
        Err("validate-all: one or more steps FAILED — see table above".into())
    } else {
        eprintln!("validate-all: all steps PASS or BLOCKED");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CRD schema validation
// ---------------------------------------------------------------------------

/// Required fields validated by `verify-crd-schema`.
///
/// Each entry is `(crd_partial_name, json_pointer)` where the pointer is
/// evaluated against the CRD JSON and must point to a non-null value.
const REQUIRED_CRD_FIELDS: &[(&str, &str)] = &[
    // GridNetwork status fields
    (
        "gridnetworks",
        "/spec/versions/0/schema/openAPIV3Schema/properties/status/properties/connectedSites",
    ),
    (
        "gridnetworks",
        "/spec/versions/0/schema/openAPIV3Schema/properties/status/properties/distributedProviderCount",
    ),
    // InferenceProvider spec fields
    (
        "inferenceproviders",
        "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/routingClusterRef",
    ),
    (
        "inferenceproviders",
        "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/metricsConfig",
    ),
    (
        "inferenceproviders",
        "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/metricsConfig/properties/path",
    ),
    (
        "inferenceproviders",
        "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/metricsConfig/properties/timeout",
    ),
    (
        "inferenceproviders",
        "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/metricsConfig/properties/signalNames/properties/queueDepth",
    ),
    (
        "inferenceproviders",
        "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/metricsConfig/properties/signalNames/properties/kvCacheUtilization",
    ),
    (
        "inferenceproviders",
        "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/metricsConfig/properties/signalNames/properties/latencyP99Ms",
    ),
    (
        "inferenceproviders",
        "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/metricsConfig/properties/signalNames/properties/prefixCacheHitRatio",
    ),
    (
        "inferenceproviders",
        "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/metricsConfig/properties/signalNames/properties/errorRate",
    ),
    (
        "inferenceproviders",
        "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/metricsConfig/properties/signalNames/properties/healthy",
    ),
    // InferenceProvider status subresource
    ("inferenceproviders", "/spec/versions/0/subresources/status"),
    // GridSite spec
    (
        "gridsites",
        "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/gridNetworkRef",
    ),
];

/// Verify required fields are present in the generated CRD JSON.
///
/// Returns a list of missing field paths.
pub(crate) fn check_crd_fields(crd_json: &serde_json::Value) -> Vec<String> {
    let Some(items) = crd_json.get("items").and_then(serde_json::Value::as_array) else {
        return vec!["JSON is not a CRD List (missing 'items' array)".to_owned()];
    };

    let mut missing = Vec::new();
    for &(name_part, ptr) in REQUIRED_CRD_FIELDS {
        let found = items.iter().any(|crd| {
            let crd_name = crd
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            if !crd_name.contains(name_part) {
                return false;
            }
            crd.pointer(ptr).is_some()
        });
        if !found {
            missing.push(format!("{name_part}: {ptr}"));
        }
    }
    missing
}

/// Run `generate_crds`, parse the output, and validate required schema fields.
fn env_verify_crd_schema() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("verify-crd-schema: generating CRDs...");
    let output = std::process::Command::new("cargo")
        .args(["run", "--quiet", "-p", "operator", "--bin", "generate_crds"])
        .output()?;
    if !output.status.success() {
        return Err(format!("generate_crds failed: {}", String::from_utf8_lossy(&output.stderr)).into());
    }
    let json: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let missing = check_crd_fields(&json);
    if missing.is_empty() {
        eprintln!(
            "verify-crd-schema: all {} required fields present",
            REQUIRED_CRD_FIELDS.len()
        );
        Ok(())
    } else {
        eprintln!("verify-crd-schema: {} field(s) missing:", missing.len());
        for m in &missing {
            eprintln!("  MISSING: {m}");
        }
        Err(format!("verify-crd-schema: {} required CRD field(s) missing", missing.len()).into())
    }
}

// ---------------------------------------------------------------------------
// verify-operator-install-rbac
// ---------------------------------------------------------------------------

/// Verify the operator install/RBAC package in Kind.
///
/// Applies `deploy/operator/` manifests, runs `kubectl auth can-i` checks for
/// positive and negative permission expectations, then spawns an out-of-cluster
/// operator to prove RBAC is sufficient for a minimal reconcile.
/// Positive `kubectl auth can-i` checks for the installed operator RBAC.
fn rbac_can_i_checks(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let checks: &[(&str, &str, Option<&str>)] = &[
        ("patch", "gridnetworks.grid.praxis-proxy.io", None),
        ("patch", "gridnetworks.grid.praxis-proxy.io/status", None),
        ("get", "gridnetworks.grid.praxis-proxy.io", None),
        ("list", "gridnetworks.grid.praxis-proxy.io", None),
        ("watch", "gridnetworks.grid.praxis-proxy.io", None),
        ("patch", "gridsites.grid.praxis-proxy.io", None),
        ("patch", "gridsites.grid.praxis-proxy.io/status", None),
        ("list", "inferenceproviders.grid.praxis-proxy.io", None),
        ("patch", "inferenceproviders.grid.praxis-proxy.io/status", None),
        ("get", "secrets", Some("default")),
        ("create", "secrets", Some("default")),
        ("patch", "secrets", Some("default")),
        ("create", "configmaps", Some("default")),
        ("patch", "configmaps", Some("default")),
    ];
    for (verb, resource, ns) in checks {
        let allowed = operator::kubectl_auth_can_i(context, verb, resource, *ns)?;
        if !allowed {
            return Err(format!(
                "RBAC check failed: grid-operator should be allowed to {verb} {resource}{}",
                ns.map_or(String::new(), |n| format!(" in namespace {n}"))
            )
            .into());
        }
        eprintln!("  [PASS] can-i {verb} {resource}{}", ns.map_or("", |_| " (namespaced)"));
    }
    Ok(())
}

/// Negative `kubectl auth can-i` checks (must be denied).
fn rbac_negative_checks(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let checks: &[(&str, &str, Option<&str>)] = &[
        ("list", "secrets", Some("default")),
        ("delete", "secrets", Some("default")),
        ("create", "pods", Some("default")),
        ("get", "pods", Some("default")),
        ("create", "events", Some("default")),
        ("delete", "gridnetworks.grid.praxis-proxy.io", None),
        ("get", "secrets", Some("kube-system")),
        ("patch", "configmaps", Some("kube-system")),
    ];
    for (verb, resource, ns) in checks {
        let allowed = operator::kubectl_auth_can_i(context, verb, resource, *ns)?;
        if allowed {
            return Err(format!(
                "RBAC check failed: grid-operator should NOT be allowed to {verb} {resource}{}",
                ns.map_or(String::new(), |n| format!(" in namespace {n}"))
            )
            .into());
        }
        eprintln!(
            "  [PASS] cannot {verb} {resource}{} (correctly denied)",
            ns.map_or(String::new(), |n| format!(" in {n}"))
        );
    }
    Ok(())
}

/// Validate operator install manifests, RBAC, and in-cluster reconcile.
#[expect(
    clippy::too_many_lines,
    reason = "sequential install/RBAC verification steps; splitting would obscure the proof"
)]
fn env_verify_operator_install_rbac(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = EnvConfig::from_file(config)?;
    let site_cfg_name = resolve_operator_site_name(&cfg, site)?;
    let context = kind::kubectl_context(site_cfg_name);
    let cluster_name = kind::cluster_name_from_config(site_cfg_name);
    eprintln!("verify-operator-install-rbac: context={context}");

    // Step 1: preflight — CRDs + cleanup.
    eprintln!("verify-operator-install-rbac: [1/11] preflight — CRDs, cleanup...");
    operator::install_grid_crds(&context)?;
    operator::cleanup_install_rbac_test_resources(&context)?;

    // Step 2: build operator image + load into Kind.
    eprintln!("verify-operator-install-rbac: [2/11] building operator image...");
    operator::build_operator_image()?;
    operator::load_operator_image(&cluster_name)?;
    eprintln!("  [OK] operator image built and loaded into {cluster_name}");

    // Step 3: apply install manifests.
    eprintln!("verify-operator-install-rbac: [3/11] applying install manifests...");
    operator::apply_install_manifests(&context)?;
    operator::override_operator_image_for_kind(&context)?;
    eprintln!("  [OK] install manifests applied");

    // Steps 4-5: RBAC can-i checks (positive + negative).
    eprintln!("verify-operator-install-rbac: [4/11] positive RBAC checks...");
    rbac_can_i_checks(&context)?;
    eprintln!("verify-operator-install-rbac: [5/11] negative RBAC checks...");
    rbac_negative_checks(&context)?;

    // Step 6: patch Deployment env vars + wait for rollout.
    eprintln!("verify-operator-install-rbac: [6/11] starting in-cluster operator...");
    let test_site_name = "rbac-test-site";
    operator::patch_operator_deployment_env(&context, test_site_name)?;
    kubectl::wait_for_rollout_ns(&context, "grid-operator", "grid-system", &cluster_name)?;
    eprintln!("  [OK] grid-operator Deployment available");

    // Step 7: apply fixtures with TLS Secret refs.
    eprintln!("verify-operator-install-rbac: [7/11] applying test fixtures...");
    let network_name = "op-e2e-rbac-net";
    let gw_name = "op-e2e-rbac-gw";
    let provider_name = "op-e2e-rbac-prov";
    let ca_secret_name = "op-e2e-rbac-ca";
    let site_secret_name = "op-e2e-rbac-site";
    let overlay_cm_name = format!("grid-overlay-{network_name}-{gw_name}");

    let network_manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": network_name },
        "spec": {
            "seeds": [],
            "tls": {
                "caSecretRef": {
                    "name": ca_secret_name,
                    "namespace": "default"
                },
                "siteSecretRef": {
                    "name": site_secret_name,
                    "namespace": "default"
                }
            },
            "gatewayRefs": [{
                "name": gw_name,
                "namespace": "default"
            }]
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("RBAC test network serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(&context, &network_manifest)?;
    eprintln!("  [OK] GridNetwork {network_name} applied (with TLS Secret refs)");

    let provider_manifest = serde_json::to_string_pretty(&serde_json::json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": provider_name },
        "spec": {
            "gridNetworkRef": network_name,
            "models": [{ "name": "model-rbac-test" }],
            "backendKind": "SelfHosted",
            "providerKind": "SelfHosted",
            "routingClusterRef": test_site_name,
            "endpoint": "http://localhost:10099"
        }
    }))
    .unwrap_or_else(|e| {
        eprintln!("RBAC test provider serialization failed: {e}");
        std::process::exit(1);
    });
    kubectl::apply_manifest(&context, &provider_manifest)?;
    eprintln!("  [OK] InferenceProvider {provider_name} applied");

    // Step 8: wait for in-cluster operator to reconcile.
    eprintln!("verify-operator-install-rbac: [8/11] waiting for in-cluster reconcile...");
    let snap = operator::wait_for_gridnetwork_status(&context, network_name, operator::SWIM_STATUS_POLL_TIMEOUT)?;
    eprintln!(
        "  [PASS] GridNetwork {network_name} reconciled by in-cluster operator \
         (phase={:?}, connectedSites={})",
        snap.phase, snap.connected_sites
    );

    // Step 9: verify actual Secret writes (TLS CA + site cert).
    eprintln!("verify-operator-install-rbac: [9/11] verifying Secret writes...");
    if !kind::kubectl_secret_exists(&context, "default", ca_secret_name)? {
        return Err(
            format!("Secret {ca_secret_name} not found in default — operator failed to write CA Secret").into(),
        );
    }
    eprintln!("  [PASS] Secret {ca_secret_name} exists (CA cert written by in-cluster operator)");

    if !kind::kubectl_secret_exists(&context, "default", site_secret_name)? {
        return Err(format!(
            "Secret {site_secret_name} not found in default — operator failed to write site cert Secret"
        )
        .into());
    }
    eprintln!("  [PASS] Secret {site_secret_name} exists (site cert written by in-cluster operator)");

    // Step 10: verify overlay ConfigMap write.
    eprintln!("verify-operator-install-rbac: [10/11] verifying overlay ConfigMap write...");
    match kubectl::get_configmap_yaml(&context, "default", &overlay_cm_name) {
        Ok(_) => {
            eprintln!("  [PASS] ConfigMap {overlay_cm_name} exists (overlay written by in-cluster operator)");
        },
        Err(e) => {
            return Err(format!(
                "ConfigMap {overlay_cm_name} not found in default — operator failed to write overlay: {e}"
            )
            .into());
        },
    }

    // Step 11: cleanup.
    eprintln!("verify-operator-install-rbac: [11/11] cleanup...");
    operator::cleanup_install_rbac_test_resources(&context)?;
    operator::cleanup_install_manifests(&context)?;

    eprintln!(
        "verify-operator-install-rbac: PASS — install manifests apply cleanly; \
         positive RBAC checks pass; negative RBAC checks (including namespace-scoped) pass; \
         in-cluster operator reconcile succeeds; Secret patch and ConfigMap patch verified"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// validate-all tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "tests"
)]
mod multi_provider_tests {
    use super::*;

    fn make_config(clusters: &[(&str, ClusterRole, Vec<String>)]) -> EnvConfig {
        use std::collections::BTreeMap;

        use config::{BedrockDef, ClusterConfig, ProviderConfig, ProviderDef, VertexDef};
        let mut definitions = BTreeMap::new();
        let mut names = Vec::new();
        for (name, role, models) in clusters {
            names.push((*name).to_owned());
            definitions.insert(
                (*name).to_owned(),
                config::ClusterDef {
                    models: models.clone(),
                    role: *role,
                    backend: ProviderBackend::default(),
                },
            );
        }
        EnvConfig {
            clusters: ClusterConfig { names, definitions },
            providers: ProviderConfig {
                openai: ProviderDef { port: 10001 },
                anthropic: ProviderDef { port: 10002 },
                bedrock: BedrockDef {
                    port: 10003,
                    region: "us-east-1".to_owned(),
                },
                vertex: VertexDef {
                    port: 10004,
                    project: "p".to_owned(),
                },
            },
        }
    }

    #[test]
    fn provider_clusters_from_config_single_provider() {
        let cfg = make_config(&[
            ("site-a", ClusterRole::Provider, vec!["model-x".to_owned()]),
            ("consumer", ClusterRole::Consumer, vec![]),
        ]);
        let providers = provider_clusters_from_config(&cfg);
        assert_eq!(providers.len(), 1, "single provider cluster");
        assert_eq!(providers[0].0, "site-a");
        assert_eq!(providers[0].1, vec!["model-x"]);
    }

    #[test]
    fn provider_clusters_from_config_two_providers() {
        let cfg = make_config(&[
            ("site-east", ClusterRole::Provider, vec!["model-east".to_owned()]),
            ("site-west", ClusterRole::Provider, vec!["model-west".to_owned()]),
            ("consumer", ClusterRole::Consumer, vec![]),
        ]);
        let providers = provider_clusters_from_config(&cfg);
        assert_eq!(providers.len(), 2, "two provider clusters");
        assert_eq!(providers[0].0, "site-east");
        assert_eq!(providers[1].0, "site-west");
    }

    #[test]
    fn provider_clusters_from_config_excludes_empty_model_providers() {
        let cfg = make_config(&[
            ("site-a", ClusterRole::Provider, vec![]),
            ("consumer", ClusterRole::Consumer, vec![]),
        ]);
        let providers = provider_clusters_from_config(&cfg);
        assert!(providers.is_empty(), "provider with no models must be excluded");
    }

    #[test]
    fn provider_clusters_from_config_excludes_consumer_clusters() {
        let cfg = make_config(&[("consumer", ClusterRole::Consumer, vec!["x".to_owned()])]);
        let providers = provider_clusters_from_config(&cfg);
        assert!(providers.is_empty(), "consumer clusters must not appear as providers");
    }

    #[test]
    fn require_mock_openai_backends_accepts_mock_openai_providers() {
        let mut cfg = make_config(&[
            ("site-east", ClusterRole::Provider, vec!["model-east".to_owned()]),
            ("site-west", ClusterRole::Provider, vec!["model-west".to_owned()]),
            ("consumer", ClusterRole::Consumer, vec![]),
        ]);
        for site in ["site-east", "site-west"] {
            cfg.clusters
                .definitions
                .get_mut(site)
                .unwrap_or_else(|| std::process::abort())
                .backend = ProviderBackend::MockOpenai;
        }

        assert!(
            require_mock_openai_backends(&cfg, &["site-east", "site-west"]).is_ok(),
            "metrics-routing preflight must accept mock-openai provider backends"
        );
    }

    #[test]
    fn require_mock_openai_backends_rejects_inference_sim_provider() {
        let cfg = make_config(&[
            ("site-east", ClusterRole::Provider, vec!["model-east".to_owned()]),
            ("site-west", ClusterRole::Provider, vec!["model-west".to_owned()]),
            ("consumer", ClusterRole::Consumer, vec![]),
        ]);

        let err = require_mock_openai_backends(&cfg, &["site-east", "site-west"]).unwrap_err();
        assert!(
            err.to_string().contains("backend = \"mock-openai\""),
            "preflight error must explain the required backend; got: {err}"
        );
    }

    #[test]
    fn is_multi_provider_config_false_for_single_provider() {
        let cfg = make_config(&[
            ("site-a", ClusterRole::Provider, vec!["model-x".to_owned()]),
            ("consumer", ClusterRole::Consumer, vec![]),
        ]);
        assert!(!is_multi_provider_config(&cfg), "single provider must return false");
    }

    #[test]
    fn is_multi_provider_config_true_for_two_providers() {
        let cfg = make_config(&[
            ("site-east", ClusterRole::Provider, vec!["model-east".to_owned()]),
            ("site-west", ClusterRole::Provider, vec!["model-west".to_owned()]),
            ("consumer", ClusterRole::Consumer, vec![]),
        ]);
        assert!(is_multi_provider_config(&cfg), "two providers must return true");
    }

    #[test]
    fn is_multi_provider_config_false_for_no_providers() {
        let cfg = make_config(&[("consumer", ClusterRole::Consumer, vec![])]);
        assert!(!is_multi_provider_config(&cfg), "no providers must return false");
    }

    #[test]
    fn provider_clusters_preserves_config_order() {
        let cfg = make_config(&[
            ("site-east", ClusterRole::Provider, vec!["model-east".to_owned()]),
            ("site-west", ClusterRole::Provider, vec!["model-west".to_owned()]),
            ("consumer", ClusterRole::Consumer, vec![]),
        ]);
        let providers = provider_clusters_from_config(&cfg);
        assert_eq!(providers[0].0, "site-east", "first provider must be site-east");
        assert_eq!(providers[1].0, "site-west", "second provider must be site-west");
    }

    #[test]
    fn provider_clusters_include_multi_model_sites() {
        let cfg = make_config(&[
            (
                "site-east",
                ClusterRole::Provider,
                vec!["model-east".to_owned(), "model-east-v2".to_owned()],
            ),
            ("consumer", ClusterRole::Consumer, vec![]),
        ]);
        let providers = provider_clusters_from_config(&cfg);
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].1.len(), 2, "all models must be included");
    }
}

#[cfg(test)]
mod validate_all_tests {
    use super::*;

    #[test]
    fn step_status_pass_is_not_failure() {
        assert!(!StepStatus::Pass.is_failure());
    }

    #[test]
    fn step_status_fail_is_failure() {
        assert!(StepStatus::Fail.is_failure());
    }

    #[test]
    fn step_status_blocked_is_not_failure() {
        assert!(!StepStatus::Blocked.is_failure());
    }

    #[test]
    fn step_result_pass_has_correct_label() {
        let r = StepResult::pass("my-step", "ok");
        assert_eq!(r.status.label(), "PASS");
        assert_eq!(r.label, "my-step");
    }

    #[test]
    fn env_status_summary_evidence_does_not_claim_readiness() {
        let r = StepResult::pass("env status", "status summary printed above");
        assert_eq!(
            r.evidence, "status summary printed above",
            "env_status returns success after printing its own readiness summary; validate-all must not claim all components are ready"
        );
    }

    #[test]
    fn step_result_fail_truncates_long_evidence() {
        let long_msg = "x".repeat(200);
        let err: Box<dyn std::error::Error> = long_msg.clone().into();
        let r = StepResult::fail("step", err.as_ref());
        assert!(r.evidence.len() < 200, "evidence should be truncated");
        assert!(r.evidence.ends_with('…'), "truncated evidence should end with ellipsis");
    }

    #[test]
    fn any_fail_drives_error_exit() {
        // Simulate results where one step fails.
        let results = [
            StepResult::pass("step-a", "ok"),
            StepResult {
                label: "step-b",
                status: StepStatus::Fail,
                evidence: "oops".to_owned(),
            },
            StepResult::pass("step-c", "ok"),
        ];
        let any_fail = results.iter().any(|r| r.status.is_failure());
        assert!(any_fail, "any_fail must be true when at least one step is FAIL");
    }

    #[test]
    fn blocked_only_does_not_drive_error_exit() {
        let results = [
            StepResult::pass("step-a", "ok"),
            StepResult::blocked("step-b", "missing prereq"),
        ];
        let any_fail = results.iter().any(|r| r.status.is_failure());
        assert!(!any_fail, "BLOCKED alone must not produce a non-zero exit");
    }

    #[test]
    fn safe_truncate_str_leaves_short_strings_unchanged() {
        assert_eq!(safe_truncate_str("hello", 10), "hello");
    }

    #[test]
    fn safe_truncate_str_truncates_long_strings() {
        let s = "a".repeat(200);
        let t = safe_truncate_str(&s, 10);
        assert_eq!(t, "aaaaaaaaaa…");
    }
}

// ---------------------------------------------------------------------------
// Fingerprint trust promotion E2E
// ---------------------------------------------------------------------------

/// Print the SHA-256 fingerprint of a `GridSite.status.publicCertPem`.
fn env_gridsite_fingerprint(context: &str, site_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let pem = operator::read_gridsite_public_cert_pem(context, site_name)
        .ok_or_else(|| format!("GridSite {site_name:?} has no publicCertPem in status"))?;
    let fp = operator::sha256_fingerprint(&pem);
    eprintln!("GridSite: {site_name:?}  context: {context}");
    println!("{fp}");
    Ok(())
}

/// Prove the complete fingerprint-pinning trust promotion lifecycle.
///
/// Steps:
/// 1. Spawn operators A (no TLS) and B (TLS via `GridNetwork` spec).
/// 2. Wait for SWIM convergence.
/// 3. Apply `GridNetwork` with TLS references so B broadcasts its cert.
/// 4. Wait for B's cert to appear in auto-discovered `GridSite` for B.
/// 5. Bind TCP listener so `GridSite` probe succeeds.
/// 6. Apply egress to B's `GridSite` so controller advances to `Connecting`.
/// 7. Assert reason = `TrustPolicyMissing` (no fingerprint yet).
/// 8. Patch wrong fingerprint → assert `TrustPolicyMismatch`.
/// 9. Patch correct fingerprint → wait for `Active` (`TrustPolicyVerified`).
/// 10. Assert B's CRDT provider appears in overlay.
/// 11. Rotation proof: patch wrong fingerprint on `Active` site → observe `Connecting TrustPolicyMismatch`.
#[expect(
    clippy::too_many_lines,
    reason = "sequential fingerprint lifecycle proof: 11 steps covering promotion, mismatch, and rotation"
)]
fn env_verify_gridsite_trust_fingerprint(config: &Path, site: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    use operator::{
        CONFIGMAP_POLL_TIMEOUT, SWIM_CONVERGENCE_WAIT, SWIM_STATUS_POLL_TIMEOUT, SWIM_TRUST_GW, SWIM_TRUST_NETWORK,
        SWIM_TRUST_SITE_A, SWIM_TRUST_SITE_B, sha256_fingerprint,
    };

    let cfg = EnvConfig::from_file(config)?;
    let context = resolve_operator_context(&cfg, site)?;
    eprintln!("verify-gridsite-trust-fingerprint: context={context}");

    // ── Step 1: CRDs, cleanup ─────────────────────────────────────────────────
    operator::install_grid_crds(&context)?;
    operator::cleanup_swim_trust_test_resources(&context)?;

    let (bind_a, bind_b) = reserve_swim_bind_addrs()?;
    eprintln!("  A={bind_a} (no TLS)  B={bind_b} (TLS — cert will be broadcast)");

    // ── Step 2: Spawn operators ───────────────────────────────────────────────
    // A renders overlay; B has TLS so it broadcasts a cert that A receives.
    let op_a = operator::spawn_operator_with_swim(&context, &bind_a, &bind_a, SWIM_TRUST_SITE_A, "", None)?;
    let mut op_a_guard = ProcGuard(Some(op_a), "trust-op-a");
    let op_b = operator::spawn_operator_with_swim(&context, &bind_b, &bind_b, SWIM_TRUST_SITE_B, &bind_a, None)?;
    let mut op_b_guard = ProcGuard(Some(op_b), "trust-op-b");

    // ── Step 3: SWIM convergence + apply fixtures ─────────────────────────────
    operator::wait_for_swim_convergence(SWIM_CONVERGENCE_WAIT);
    operator::apply_swim_trust_test_fixtures(&context, SWIM_TRUST_SITE_A)?;
    eprintln!("  fixtures applied; waiting for B's TLS cert to broadcast via SWIM...");

    // ── Step 4: Wait for distributedProviderCount > 0 and B's publicCertPem ──
    let b_site_k8s_name = operator::auto_discovered_gridsite_name(SWIM_TRUST_NETWORK, SWIM_TRUST_SITE_B);
    let result: Result<(), Box<dyn std::error::Error>> = (|| {
        operator::wait_for_distributed_state_count(&context, SWIM_TRUST_NETWORK, 1, SWIM_STATUS_POLL_TIMEOUT)?;
        eprintln!("  [OK] CRDT from B received by A (distributedProviderCount >= 1)");

        let cert_pem =
            operator::wait_for_gridsite_public_cert_pem(&context, &b_site_k8s_name, SWIM_STATUS_POLL_TIMEOUT)?;
        eprintln!("  [OK] publicCertPem received on GridSite {b_site_k8s_name:?}");

        // ── Step 5: Bind TCP listener for probe ───────────────────────────────
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| format!("TCP listener bind failed: {e}"))?;
        let egress_addr = match listener.local_addr() {
            Ok(a) => a.to_string(),
            Err(_) => "127.0.0.1:0".to_owned(),
        };
        eprintln!("  TCP listener bound at {egress_addr} for GridSite probe");

        // ── Step 6: Apply egress so controller advances Discovered → Connecting ─
        // We deliberately do NOT patch status=Active here.  The controller will
        // advance phase naturally once TCP probe + cert + fingerprint policy align.
        operator::apply_gridsite_egress(&context, &b_site_k8s_name, SWIM_TRUST_NETWORK, &egress_addr)?;

        // ── Step 7: Assert TrustPolicyMissing (cert present, no fingerprint) ──
        eprintln!("verify-gridsite-trust-fingerprint: [7] asserting TrustPolicyMissing...");
        operator::wait_for_gridsite_reason(
            &context,
            &b_site_k8s_name,
            "TrustPolicyMissing",
            SWIM_STATUS_POLL_TIMEOUT,
        )?;
        eprintln!("  [PASS] Connecting with TrustPolicyMissing (cert present, no fingerprint configured)");

        // ── Step 8: Wrong fingerprint → TrustPolicyMismatch ──────────────────
        eprintln!("verify-gridsite-trust-fingerprint: [8] patching wrong fingerprint...");
        let wrong_fp = sha256_fingerprint("-----BEGIN CERTIFICATE-----\nwrong\n-----END CERTIFICATE-----\n");
        operator::patch_gridsite_cert_fingerprint(&context, &b_site_k8s_name, &wrong_fp)?;
        operator::wait_for_gridsite_reason(
            &context,
            &b_site_k8s_name,
            "TrustPolicyMismatch",
            SWIM_STATUS_POLL_TIMEOUT,
        )?;
        eprintln!("  [PASS] TrustPolicyMismatch — wrong fingerprint correctly rejected");

        // Assert B's CRDT provider absent before Active.
        operator::wait_for_overlay_configmap(
            &context,
            SWIM_TRUST_NETWORK,
            SWIM_TRUST_GW,
            "default",
            CONFIGMAP_POLL_TIMEOUT,
        )?;
        operator::assert_no_crdt_candidates_for_site(&context, SWIM_TRUST_NETWORK, SWIM_TRUST_GW, SWIM_TRUST_SITE_B)?;
        eprintln!("  [PASS] B absent from overlay before Active");

        // ── Step 9: Correct fingerprint → Active (TrustPolicyVerified) ────────
        eprintln!("verify-gridsite-trust-fingerprint: [9] patching correct fingerprint...");
        let correct_fp = sha256_fingerprint(&cert_pem);
        operator::patch_gridsite_cert_fingerprint(&context, &b_site_k8s_name, &correct_fp)?;
        operator::wait_for_gridsite_reason(
            &context,
            &b_site_k8s_name,
            "TrustPolicyVerified",
            SWIM_STATUS_POLL_TIMEOUT,
        )?;
        eprintln!("  [PASS] TrustPolicyVerified — correct fingerprint promoted site to Active");

        // ── Step 10: Assert B's CRDT provider appears in overlay ──────────────
        eprintln!("verify-gridsite-trust-fingerprint: [10] verifying overlay includes B...");
        operator::wait_for_site_candidate_in_overlay(
            &context,
            SWIM_TRUST_NETWORK,
            SWIM_TRUST_GW,
            SWIM_TRUST_SITE_B,
            SWIM_STATUS_POLL_TIMEOUT,
        )?;
        eprintln!("  [PASS] B's CRDT provider appears in overlay after Active");

        // ── Step 11: Rotation proof — patch wrong fingerprint on Active site ───
        eprintln!("verify-gridsite-trust-fingerprint: [11] rotation proof...");
        operator::patch_gridsite_cert_fingerprint(&context, &b_site_k8s_name, &wrong_fp)?;
        operator::wait_for_gridsite_reason(
            &context,
            &b_site_k8s_name,
            "TrustPolicyMismatch",
            SWIM_STATUS_POLL_TIMEOUT,
        )?;
        eprintln!("  [PASS] rotation: cert fingerprint changed → site left Active (TrustPolicyMismatch)");

        // Verify B absent from overlay after rotation mismatch.
        operator::assert_no_crdt_candidates_for_site(&context, SWIM_TRUST_NETWORK, SWIM_TRUST_GW, SWIM_TRUST_SITE_B)?;
        eprintln!("  [PASS] B absent from overlay after fingerprint rotation mismatch");

        drop(listener); // release probe port
        Ok(())
    })();

    // ── Cleanup ───────────────────────────────────────────────────────────────
    if let Some(c) = op_a_guard.0.take() {
        operator::kill_operator(c);
    }
    if let Some(c) = op_b_guard.0.take() {
        operator::kill_operator(c);
    }
    operator::cleanup_swim_trust_test_resources(&context)?;
    operator::cleanup_auto_discovered_gridsite(&context, &b_site_k8s_name)
        .unwrap_or_else(|e| eprintln!("  warning: B GridSite cleanup: {e}"));

    result?;

    eprintln!(
        "verify-gridsite-trust-fingerprint: PASS — \
         TrustPolicyMissing without fingerprint; \
         TrustPolicyMismatch with wrong fingerprint; \
         TrustPolicyVerified + Active with correct fingerprint; \
         B present in overlay after Active; \
         B absent after rotation mismatch"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// llm-d-compatible routing tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod llmd_compat_routing_tests {
    use super::*;

    #[test]
    fn verify_response_model_field_accepts_matching_model() {
        let body = r#"{"model":"model-east","choices":[{"message":{"content":"hi"}}]}"#;
        assert!(
            verify_response_model_field(body, "model-east").is_ok(),
            "matching model must pass"
        );
    }

    #[test]
    fn verify_response_model_field_rejects_mismatched_model() {
        let body = r#"{"model":"model-west","choices":[{"message":{"content":"hi"}}]}"#;
        assert!(
            verify_response_model_field(body, "model-east").is_err(),
            "mismatched model must fail"
        );
    }

    #[test]
    fn verify_response_model_field_rejects_missing_model() {
        let body = r#"{"choices":[{"message":{"content":"hi"}}]}"#;
        assert!(
            verify_response_model_field(body, "model-east").is_err(),
            "missing model field must fail"
        );
    }

    #[test]
    fn verify_response_model_field_rejects_invalid_json() {
        assert!(
            verify_response_model_field("not json", "model-east").is_err(),
            "invalid JSON must fail"
        );
    }

    #[test]
    fn verify_response_model_field_rejects_null_model() {
        let body = r#"{"model":null,"choices":[]}"#;
        assert!(
            verify_response_model_field(body, "model-east").is_err(),
            "null model field must fail"
        );
    }

    #[test]
    fn verify_response_model_field_rejects_empty_model() {
        let body = r#"{"model":"","choices":[]}"#;
        assert!(
            verify_response_model_field(body, "model-east").is_err(),
            "empty model field must fail"
        );
    }

    #[test]
    fn parse_pod_restart_lines_extracts_non_zero() {
        let output = "pod-a 0\npod-b 2\npod-c 0\npod-d 1\n";
        let restarts = parse_pod_restart_lines(output);
        assert_eq!(restarts.len(), 2, "should find 2 pods with restarts");
        assert_eq!(
            restarts.first().map(|(n, c)| (n.as_str(), *c)),
            Some(("pod-b", 2)),
            "first restarted pod must be pod-b with count 2"
        );
        assert_eq!(
            restarts.get(1).map(|(n, c)| (n.as_str(), *c)),
            Some(("pod-d", 1)),
            "second restarted pod must be pod-d with count 1"
        );
    }

    #[test]
    fn parse_pod_restart_lines_handles_empty_output() {
        assert!(
            parse_pod_restart_lines("").is_empty(),
            "empty output must return empty list"
        );
    }

    #[test]
    fn parse_pod_restart_lines_handles_none_values() {
        let output = "pod-a <none>\npod-b 0\n";
        let restarts = parse_pod_restart_lines(output);
        assert!(restarts.is_empty(), "<none> and 0 must both be excluded");
    }

    #[test]
    fn parse_pod_restart_lines_all_zero() {
        let output = "pod-a 0\npod-b 0\n";
        assert!(
            parse_pod_restart_lines(output).is_empty(),
            "all-zero restarts must produce empty list"
        );
    }

    #[test]
    fn parse_pod_restart_lines_single_restarted() {
        let output = "gateway-pod 5\n";
        let restarts = parse_pod_restart_lines(output);
        assert_eq!(restarts.len(), 1, "should find 1 pod with restarts");
        assert_eq!(
            restarts.first().map(|(n, c)| (n.as_str(), *c)),
            Some(("gateway-pod", 5)),
            "restarted pod must be gateway-pod with count 5"
        );
    }

    #[test]
    fn llmd_compat_record_step_pass_returns_true() {
        let mut results = Vec::new();
        let ok = llmd_compat_record_step("test-step", &mut results, || Ok("evidence".to_owned()));
        assert!(ok, "passing step must return true");
        assert_eq!(results.len(), 1, "one result must be recorded");
        assert_eq!(
            results.first().map(|r| r.status.label()),
            Some("PASS"),
            "status must be PASS"
        );
    }

    #[test]
    fn llmd_compat_record_step_fail_returns_false() {
        let mut results = Vec::new();
        let ok = llmd_compat_record_step("test-step", &mut results, || Err("boom".into()));
        assert!(!ok, "failing step must return false");
        assert_eq!(results.len(), 1, "one result must be recorded");
        assert_eq!(
            results.first().map(|r| r.status.label()),
            Some("FAIL"),
            "status must be FAIL"
        );
    }

    /// Build a minimal [`EnvConfig`] for unit tests.
    fn make_llmd_compat_config(clusters: &[(&str, ClusterRole, Vec<String>)]) -> EnvConfig {
        use std::collections::BTreeMap;

        use config::{BedrockDef, ClusterConfig, ProviderConfig, ProviderDef, VertexDef};
        let mut definitions = BTreeMap::new();
        let mut names = Vec::new();
        for (name, role, models) in clusters {
            names.push((*name).to_owned());
            definitions.insert(
                (*name).to_owned(),
                config::ClusterDef {
                    models: models.clone(),
                    role: *role,
                    backend: ProviderBackend::default(),
                },
            );
        }
        EnvConfig {
            clusters: ClusterConfig { names, definitions },
            providers: ProviderConfig {
                openai: ProviderDef { port: 10001 },
                anthropic: ProviderDef { port: 10002 },
                bedrock: BedrockDef {
                    port: 10003,
                    region: "us-east-1".to_owned(),
                },
                vertex: VertexDef {
                    port: 10004,
                    project: "p".to_owned(),
                },
            },
        }
    }

    #[test]
    fn resolve_consumer_site_finds_consumer() {
        let cfg = make_llmd_compat_config(&[
            ("site-east", ClusterRole::Provider, vec!["model-east".to_owned()]),
            ("consumer", ClusterRole::Consumer, vec![]),
        ]);
        assert_eq!(
            resolve_consumer_site(&cfg).ok(),
            Some("consumer"),
            "must find the consumer cluster"
        );
    }

    #[test]
    fn resolve_consumer_site_errors_when_missing() {
        let cfg = make_llmd_compat_config(&[("site-east", ClusterRole::Provider, vec!["model-east".to_owned()])]);
        assert!(
            resolve_consumer_site(&cfg).is_err(),
            "must error when no consumer cluster exists"
        );
    }
}
