# Grid evaluation and end-to-end scenarios

Use this guide to choose a disposable Grid scenario by the behavior it exercises
and the environment it needs. The commands below are contributor harnesses and
scenario demonstrations. They are not all production installation tutorials
or evidence that a run has passed.

For a smaller first evaluation, start with the
[two-site Kind quickstart](../getting-started/quickstart.md). For clusters you
already operate, use the
[existing-cluster Helm installation](../installation/existing-clusters.md).

## Before you run a scenario

Most rows use the pinned Forge runner to create Kind clusters. Plan for Docker
or Podman, Kind, kubectl, Helm, the repository Rust toolchain, and the pinned
Forge installation described in the
[Forge guide](forge.md). Many runs also need locally built or pulled Grid,
Praxis AI, simulator, or Endpoint Picker images. Cluster count and image
requirements vary by scenario; read its run guide before creating resources.

Use an isolated checkout, check the named clusters and contexts in the run
guide, and do not run qualifications that share cluster names or Forge state
at the same time. Follow each guide's evidence and cleanup steps. The topology
READMEs are under <code>tests/</code>, which the documentation website does not
import, so the links below go directly to their source in the Grid repository.

## Choose a scenario

| Scenario | What it demonstrates | Routing path or important limitation | Run guide |
|---|---|---|---|
| Operator-derived local routing | Operator-configured local dials, request attribution, backend failure and recovery, and endpoint replacement. | One Kind cluster and the embedded Grid Gateway. This experimental integration qualification covers local routing, not cross-site selection. Pass <code>--teardown</code> to delete the run's cluster. | <code>cargo xtask env run-grid-operator-routing-qualification</code>; [topology and run guide](https://github.com/praxis-proxy/grid/blob/main/tests/e2e/topologies/grid-operator-routing/README.md) · 1 cluster |
| Provider traffic across sites | Three provider gateways and a 60-request round-robin selection proof. Full mode adds generated consumer configuration, the embedded Grid Gateway, regional policy, and provider-withdrawal coverage. | Three Kind clusters. Quick mode runs the baseline six scenarios; full is the default. The overlay-backed consumer uses Praxis AI <code>intelligent_route</code>; full mode also exercises <code>grid_site_route</code>. | <code>cargo xtask env run-grid-provider-traffic-qualification</code>; [topology and run guide](https://github.com/praxis-proxy/grid/blob/main/tests/e2e/topologies/grid-provider-traffic/README.md) · 3 clusters |
| Static weighted provider selection | Relative provider weights, traffic distribution within the active group, configuration updates, and recovery. | Three Kind clusters. The CLI accepts <code>--quick</code> and <code>--full</code>, but current source runs the same proof sequence for either value and records the selected label in evidence. Static placement weights are not live queue or KV-cache measurements; weights do not override eligibility or group priority. | <code>cargo xtask env run-grid-static-weighted-qualification</code>; [topology and run guide](https://github.com/praxis-proxy/grid/blob/main/tests/e2e/topologies/grid-static-weighted/README.md) · 3 clusters |
| Combined-site lifecycle | Consumer and provider gateway roles colocated across three sites, local preference, discovery, and provider lifecycle changes. | Three Kind clusters. Full mode is the default; <code>--quick</code> runs the focused scenario set. This is a site and lifecycle scenario, not worker scheduling inside an inference cluster. | <code>cargo xtask env run-grid-combined-site-demo</code>; [topology and run guide](https://github.com/praxis-proxy/grid/blob/main/tests/e2e/topologies/grid-combined-site/README.md) · 3 clusters |
| Global load-balancing demo | Two edge gateways, two provider sites, a GTM emulator, provider routing, trust boundaries, and recovery. | Five Kind clusters by default. Full mode is the default; <code>--quick</code> skips the longer lifecycle proofs. <code>--no-ingress</code> selects the four-cluster workload topology without the GTM emulator; this is not a public DNS or latency qualification. | <code>cargo xtask env run-grid-glb-demo</code>; [topology and run guide](https://github.com/praxis-proxy/grid/blob/main/tests/e2e/topologies/grid-glb-demo/README.md) · 5 clusters |
| llm-d pool metrics | Endpoint Picker metrics, Grid signal relay, score changes, cross-site routing, and recovery. | Two Kind clusters. This is an experimental qualification. Full mode is the default; quick checks the scrape and routing path, while full also checks the embedded <code>grid_site_route</code> path, signal relay, and recovery without a gateway restart. Simulator metrics do not establish GPU performance. | <code>cargo xtask env run-grid-llmd-pool-metrics-demo</code>; [topology and run guide](https://github.com/praxis-proxy/grid/blob/main/tests/e2e/topologies/grid-llmd-pool-metrics/README.md) · 2 clusters |
| Single-cluster multi-gateway | Multiple consumer and provider gateways share one GridSite and apply shared routing state independently. | One Kind cluster. This checks gateway replicas and shared site state, not WAN or cross-site routing. The runner cleans up by default; <code>--keep</code> leaves the cluster for diagnosis. | <code>cargo xtask env run-grid-single-cluster-multi-gateway-qualification</code>; [topology and run guide](https://github.com/praxis-proxy/grid/blob/main/tests/e2e/topologies/grid-single-cluster-multi-gateway/README.md) · 1 cluster |
| Token-rate-limit qualification · experimental | Two consumer gateways share a Valkey-backed, rule-scoped token budget before admitted requests use provider selection. | Three Kind clusters. This experimental integration qualification's identity contract is rule-scoped; it does not qualify general authenticated-principal propagation. Its optional AI token-rate-limit filter needs a compatible image; check the guide before using a published image. The runner cleans up by default; <code>--keep</code> retains clusters. | <code>cargo xtask env run-grid-token-rate-limit-qualification</code>; [topology and run guide](https://github.com/praxis-proxy/grid/blob/main/tests/e2e/topologies/grid-token-rate-limit/README.md) · 3 clusters |

The command names, cluster counts, and summaries above are checked against Grid
source and topology guides. Read the linked guide before running a scenario:
image requirements, supported flags, and cleanup differ. The
[integration qualification checklist](release.md#integration-qualification-checklist)
summarizes image overrides and the
[evidence and cleanup rules](release.md#evidence-cleanup-and-release-gates)
for the broader runner set. Several runners default to full mode; pass
<code>--quick</code> only when the topology supports it and a shorter proof is
appropriate. Add <code>--teardown</code> where supported if you want the runner
to remove its Kind resources after completion.

## Other entry points

These harnesses are separate from the <code>cargo xtask env run-grid-*</code>
table:

- **Hub and site Helm harness:** run
  <code>./scripts/e2e-hub-site.sh</code> for the two-cluster installation,
  enrollment, membership, trust, and gateway checks. Set <code>HA=1</code> for
  the high-availability topology and its restart, drain, and failure scenarios.
  See the [hub and site topology guide](https://github.com/praxis-proxy/grid/blob/main/tests/e2e/topologies/grid-hub-site/README.md)
  and [HA topology guide](https://github.com/praxis-proxy/grid/blob/main/tests/e2e/topologies/grid-hub-site-ha/README.md).
  This shell harness installs the charts directly; it is not an xtask.
- **Standalone Gateway Forge qualification:** run
  <code>make praxis-gateway-e2e</code> for the one-cluster Praxis Gateway chart
  lifecycle and metrics checks. It does not create an AGN network. See the
  [standalone Gateway guide](https://github.com/praxis-proxy/grid/blob/main/tests/e2e/topologies/praxis-gateway-standalone/README.md).

For focused checks instead of a full scenario, choose from these groups:

- **Membership and state propagation:** <code>verify-swim-membership</code>,
  <code>verify-swim-state</code>, and <code>verify-swim-dns-hostnames</code>.
- **Trust and credential boundaries:** <code>verify-swim-encryption</code> and
  <code>verify-api-fallback-native</code>.
- **Signals and recovery:** <code>verify-metrics-routing</code>,
  <code>verify-stale-gc-ttl</code>, and
  <code>verify-failover-under-lost-peer</code>.

The [xtask command source](https://github.com/praxis-proxy/grid/blob/main/crates/xtask/src/env.rs)
defines the current commands and their environment configuration. The
scenario-specific topology guides link the related fixtures. These focused
checks isolate one concern and are not substitutes for the complete runners in
the table.

## What checked-in workflows run

The scenario table is a source inventory, not a CI schedule or a passing-test
report.

- The [Tests workflow](https://github.com/praxis-proxy/grid/blob/main/.github/workflows/tests.yaml)
  runs on pull requests, pushes to <code>main</code>, and merge queues. Its
  documentation-only pull-request path skips the Rust jobs. It does not invoke
  every scenario in the table.
- The reusable
  [Triggered Integration Test workflow](https://github.com/praxis-proxy/grid/blob/main/.github/workflows/triggered-integration-test.yaml)
  can be dispatched manually for a selected Grid PR or branch. Its supported
  choices include GLB quick/full, static weighted, combined-site, llm-d
  pool-metrics quick/full, and operator routing. Scheduled workflows select
  GLB quick, static weighted, llm-d pool-metrics quick, operator routing, and
  combined-site full. Provider traffic, single-cluster multi-gateway, and
  token-rate-limit are not choices in that workflow's current schedule/manual
  list.
- The [Helm workflow](https://github.com/praxis-proxy/grid/blob/main/.github/workflows/helm.yaml)
  separately runs the hub/site shell harness and standalone Gateway Forge
  qualification on its pull-request path.
- The current [Release workflow](https://github.com/praxis-proxy/grid/blob/main/.github/workflows/release.yaml)
  does not call the Triggered Integration Test workflow. A release workflow
  result is not evidence that every Kind scenario above was run.

Workflow triggers and qualification choices can change. Check the linked
workflow files for their current trigger and source-selection rules before
using an automated result as evidence.
