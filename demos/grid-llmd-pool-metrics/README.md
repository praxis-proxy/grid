# llm-d Pool Metrics Routing Demo

Proves that simulated inference telemetry drives real EPP aggregation,
Grid scoring, overlay publication, and Praxis routing decisions across
two Kind clusters.

> **Honest claim.** The inference simulators emit controlled
> `--fake-metrics` values, not traffic-derived GPU measurements. Every
> other component in the pipeline is production code running its normal
> path: llm-d EPP aggregates the simulator gauges into pool-level
> summaries, the Grid operator scrapes those summaries and scores
> backends with its production scoring engine, the overlay ConfigMap is
> published with per-candidate scores and rankings, and Praxis consumes
> that overlay to route requests. No scoring logic is reimplemented in
> the demo harness.

## What This Demo Shows

- **llm-d supplies pool-level inference telemetry.** Two inference simulators
  per cluster expose vLLM-compatible queue and KV-cache gauges. The local llm-d
  EPP aggregates those simulator values into one metrics view for the pool.
- **Grid consumes EPP metrics rather than individual simulator metrics.** Each
  `InferenceProvider` points to its pool's EPP metrics endpoint and identifies
  the expected pool label and queue capacity.
- **Raw telemetry becomes comparable routing signals.** Grid maps the EPP
  metric names, normalizes the waiting-request count by the configured queue
  capacity, and combines queue and KV-cache pressure with the other scoring
  inputs.
- **Metrics can override locality when explicitly enabled.** The demo uses the
  opt-in `scoreFirst` policy. As Pool A becomes busy, Pool B's higher score
  moves it ahead of the initially preferred local pool.
- **The complete routing transition is observable.** The narrated scorecard
  prints both pools' raw metrics, weighted scores, ranks, and selected pool at
  baseline, under pressure, and after recovery.
- **The data plane follows the published decision without restarting.** Grid
  publishes a content-addressed overlay, `grid-overlay-sync` validates and
  delivers it, and Praxis hot-reloads it. Fresh inference requests prove the
  A-to-B-to-A transition through gateway and provider attribution.
- **The telemetry stimulus is controlled, but the decision path is real.** The
  simulator values are generated predictably so the demonstration is
  repeatable. EPP aggregation, Grid scraping and scoring, overlay delivery,
  Praxis routing, and inference requests all use their normal runtime paths.

## Topology

```
┌─────────────────────────────────┐    ┌─────────────────────────────────┐
│          Kind: pool-a           │    │          Kind: pool-b           │
│                                 │    │                                 │
│  sim-1 ──┐                      │    │  sim-1 ──┐                      │
│           ├─► llm-d EPP ◄── Grid│    │           ├─► llm-d EPP ◄── Grid│
│  sim-2 ──┘    :9090      Operator│   │  sim-2 ──┘    :9090      Operator│
│                   │              │    │                   │              │
│       provider-gateway (mTLS)    │    │       provider-gateway (mTLS)    │
│       consumer-gateway ◄─ overlay│    │       consumer-gateway ◄─ overlay│
└───────────────┬─────────────────┘    └───────────────┬─────────────────┘
                │          SWIM mesh (UDP)              │
                └──────────────────────────────────────┘
```

Each cluster runs:

| Component | Count | Purpose |
|-----------|-------|---------|
| `llm-d-inference-sim` | 2 | vLLM-compatible simulator with `--fake-metrics` |
| `llm-d EPP` | 1 | Aggregates per-model metrics into pool-level Prometheus gauges |
| Grid operator | 1 | Scrapes EPP, scores backends, publishes overlay ConfigMap |
| Praxis provider gateway | 1 | mTLS ingress for cross-cluster inference |
| Praxis consumer gateway | 1 | Routes requests using the Grid overlay |

Praxis does **not** scrape metrics. The Grid operator scrapes each
cluster's EPP and publishes scored routing candidates into the overlay
ConfigMap (`grid-overlay-grid-llmd-pool-metrics-consumer-gateway`).
Praxis consumes that overlay to make routing decisions.

## Telemetry Stimulus

The simulators use `--fake-metrics` generators configured in
`resources/pool-{a,b}/sim-config.yaml`:

| Pool | `waiting-requests` | `kv-cache-usage` |
|------|--------------------|------------------|
| A | `rampreset:0:16:120s` (0 to 16 over 120 s, then resets) | `rampreset:0.10:0.90:120s` (0.10 to 0.90 over 120 s) |
| B | `1` (static) | `0.15` (static) |

Pool A ramps linearly from idle to saturated over a 120-second cycle,
then resets to zero and repeats. Pool B holds steady at low utilization.

## Queue Normalization

The raw `waiting-requests` count is normalized to the 0.0-1.0 range
using the configured queue capacity:

```
queue_depth = waiting_requests / queue_capacity
```

`queue_capacity` is set to **16** in the `InferenceProvider` CR
(`metricsConfig.queueCapacity: 16`), matching the simulator's
`max-waiting-queue-length: 16`.

## Scoring Engine

The Grid operator scores each backend with a six-signal weighted sum:

| Signal | Weight | Source |
|--------|--------|--------|
| `locality` | 3.0 | Config: Local=1.0, SameRegion=0.7, CrossRegion=0.4, Cloud=0.2, API=0.1 |
| `queue_depth` | 3.0 | EPP: `1.0 - normalized_queue_depth` |
| `kv_cache` | 2.0 | EPP: `1.0 - kv_cache_utilization` |
| `prefix_cache` | 2.0 | EPP (not derived in this demo; defaults to 0.5) |
| `latency` | 2.0 | Local measurement (not derived in this demo; defaults to 0.5) |
| `cost` | 1.0 | Config (defaults to 0.5) |

Maximum possible score: **13.0** (all signals at 1.0). Missing signals
default to **0.5** (neutral).

## Candidate Ordering Algorithm

The GridNetwork CRD has a `routingPolicy` field that controls how
candidates are ranked.  This demo uses `scoreFirst`.

### `scoreFirst` (this demo)

1. **Admission state** -- `NewAndExisting` before `ExistingOnly`
   (queue_depth > 0.85 or kv_cache > 0.90 triggers `ExistingOnly`)
2. **Freshness** -- `fresh=true` before `fresh=false`
3. **Score** -- descending; metrics pressure can outrank locality
4. **Geography tier** -- same-site preferred as tiebreak
5. **Deterministic tiebreak** -- `(site, name, cluster)` lexicographic

### `geographyFirst` (production default)

1. **Admission state**
2. **Geography tier** -- same-site preferred above score
3. **Score** -- descending
4. **Freshness**
5. **Deterministic tiebreak**

When `routingPolicy` is absent, `geographyFirst` is used.

Scores and per-signal breakdowns are published on each candidate in
the overlay JSON, making routing decisions explainable without
reimplementing the formula.

## Demo Sequence

### Quick mode (`--quick`)

1. **Provenance** -- Verifies EPP metrics endpoints are live and
   simulator configs match expected `--fake-metrics` generators.
2. **Baseline** -- Reads the overlay ConfigMap and prints a scorecard
   with production scores from both pools.

### Full mode (`--full`)

Runs the quick proofs, then continues:

3. **Pressure and flip** -- Polls the overlay until Pool A's ramp
   drives its score below Pool B's, causing a rank change (preference
   flip). Prints the scorecard at the crossover point.
4. **Recovery** -- Polls until the ramp resets and Pool A's score
   recovers above Pool B's, restoring original preference.

### Scorecard Output

Each proof prints a narrated scorecard. All values — queue size,
KV-cache utilization, pressure, score, and rank — are derived from
the same overlay ConfigMap revision so that displayed metrics are
consistent with the scores that drove the routing decision.

Queue size and KV-cache utilization are back-computed from the
overlay's per-signal score breakdown:

```
queue_size = capacity × (1 − queue_depth_score / queue_weight)
kv_cache   = 1 − kv_cache_score / kv_weight
```

Example output:

```
LLM-D POOL ROUTING DECISION
State: BASELINE

                 Queue  Capacity  Pressure  KV Cache   Score  Rank
     Cluster A     0.0        16      0.00      0.11   10.77     0
     Cluster B     1.0        16      0.06      0.15    9.01     1
Score breakdown (pool-a): locality=3.00 queue=3.00 kv=1.77 prefix=1.00 latency=1.00 cost=1.00
Score breakdown (pool-b): locality=1.50 queue=2.81 kv=1.70 prefix=1.00 latency=1.00 cost=1.00

Grid preference: CLUSTER A
```

No values are computed by the demo harness. The harness reads the
overlay ConfigMap and back-computes the raw metrics from the score
breakdown for display.

## Commands

```bash
# Quick proof (baseline only, ~3 min cold / ~30 s warm)
cargo run -p xtask -- env run-grid-llmd-pool-metrics-demo --quick

# Full proof with pressure flip and recovery (~6 min cold / ~4 min warm)
cargo run -p xtask -- env run-grid-llmd-pool-metrics-demo --full

# Keep clusters on failure for debugging
cargo run -p xtask -- env run-grid-llmd-pool-metrics-demo --full --keep-on-failure

# Custom evidence directory
cargo run -p xtask -- env run-grid-llmd-pool-metrics-demo --full --evidence-dir ./my-evidence

# Full proof with automatic teardown on success
cargo run -p xtask -- env run-grid-llmd-pool-metrics-demo --full --teardown
```

See [e2e-demo-output.txt](e2e-demo-output.txt) for example narrated output
from a full cold run.

## Prerequisites

### Local Repositories and Images

The demo requires locally built container images from three
repositories, all tagged `llmd-pool-metrics-demo`:

| Image | Source Repository |
|-------|-------------------|
| `praxis-ai:llmd-pool-metrics-demo` | `../ai/` (Praxis AI gateway) |
| `grid-operator:llmd-pool-metrics-demo` | This repository (`grid/`) |
| `llm-d-epp:llmd-pool-metrics-demo` | llm-d EPP |
| `llm-d-inference-sim:llmd-pool-metrics-demo` | llm-d inference simulator |
| `grid-overlay-sync:llmd-pool-metrics-demo` | This repository (`grid/overlay-sync/`) |

All images use `imagePullPolicy: Never` and are loaded directly into
Kind nodes.

### System Requirements

- Docker or Podman
- Kind (Kubernetes in Docker)
- ~4 GB RAM for two Kind clusters
- ~2 CPU cores available

## Overlay Delivery Architecture

The consumer gateway uses a sidecar-based overlay delivery mechanism
that is compatible with Kubernetes 1.26+ (no native sidecar support
required).

### Container Startup Sequence

```
initContainers:
  overlay-sync-init --once    # Fetches first valid operator overlay, then exits
containers:
  overlay-sync                # Watches ConfigMap, validates, atomically writes
  praxis                      # Reads overlay from shared emptyDir (read-only)
```

1. **`overlay-sync-init`** runs as an init container with `--once`.
   It polls the Kubernetes API until a valid operator-produced overlay
   ConfigMap exists, validates the content-addressed envelope (schema
   version, scope, SHA-256 digest), writes it atomically to the shared
   `emptyDir`, then exits. Praxis cannot start until this completes.

2. **`overlay-sync`** runs as a regular container alongside Praxis.
   It opens a kube-rs watch on the same ConfigMap and applies each
   valid update via atomic write (temp file, fsync, rename). Invalid
   payloads (malformed JSON, digest mismatch, scope mismatch) are
   rejected without touching the file. On restart, it restores status
   from the existing file without fabricating state.

3. **`praxis`** reads the overlay file from the shared volume
   (mounted read-only). It hot-reloads when the file changes.

### Shared Volume Layout

```
emptyDir (sizeLimit: 10Mi)
  └── routing-overlay.json    # Atomically written by overlay-sync
```

### Security Boundaries

- Only `overlay-sync-init` and `overlay-sync` mount the projected
  service account token (1-hour expiry, `kube-root-ca.crt`, downward
  API namespace). Praxis has **no** Kubernetes API access.
- RBAC: namespaced Role with `get`, `list`, `watch` on exactly one
  named ConfigMap. The ServiceAccount is dedicated to overlay-sync.
- `automountServiceAccountToken: false` on the pod spec; the projected
  volume is mounted explicitly only into overlay-sync containers.

### Compatibility: Projected ConfigMap Mode

When `overlay.sidecar.enabled` is `false`, the chart mounts the
ConfigMap directly as a projected volume (no sidecar, no emptyDir,
no service account). This provides a simpler deployment path when
content-addressed validation and atomic delivery are not required.

The demo controls vLLM-compatible telemetry through llm-d
inference-sim. The EPP aggregation, Grid scraping and scoring, overlay
publication, sidecar delivery, Praxis hot reload, and inference
routing are exercised as deployed runtime components.

## Evidence Output

The demo writes evidence files to the evidence directory
(default: `evidence/`):

- Per-proof PASS/FAIL results with timestamps
- Scorecard snapshots at each proof stage
- EPP metric samples

Evidence files do not contain credentials, prompts, message bodies,
or raw session identifiers.

## Security Boundaries

- No credentials, API keys, or authorization headers appear in
  evidence, logs, or metric labels.
- No prompts or message bodies are logged or recorded.
- Raw session identifiers are not exposed in evidence output.
- mTLS certificates are generated per-environment and scoped to the
  demo's Kind clusters.
- Cluster names use the `grid-llmd-pm-` prefix to avoid collisions
  with unrelated environments.

## Teardown

```bash
cargo xtask env run-grid-llmd-pool-metrics-demo --teardown
```

This deletes both Kind clusters (`grid-llmd-pm-pool-a` and
`grid-llmd-pm-pool-b`) and their associated Docker networks.

If clusters are left behind after a failure, delete them manually:

```bash
kind delete cluster --name grid-llmd-pm-pool-a
kind delete cluster --name grid-llmd-pm-pool-b
```

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| EPP metrics return empty | Simulators not ready | Wait for `deployment/sim-{1,2}` to be Available |
| Score never crosses over | Ramp cycle longer than poll window | Re-run; the ramp resets every 120 s |
| Overlay ConfigMap missing | Operator not reconciling | Check `grid-operator` logs in `grid-system` namespace |
| Image pull errors | Images not loaded into Kind | Rebuild and `kind load docker-image` into both clusters |
| SWIM mesh not forming | MetalLB not assigning IPs | Check `metallb-system` namespace for controller readiness |
| Consumer gateway 503 | Overlay not yet populated | Wait for operator to complete first reconciliation cycle |

## Current Limitations

- **No P99 latency derivation.** The `latency` signal defaults to 0.5
  (neutral) because the inference simulators do not expose P99 response
  time metrics.
- **No prefix-cache derivation.** The `prefix_cache` signal defaults to
  0.5 because llm-d EPP does not yet expose a pool-level prefix-cache
  utilization gauge.
- **Simulated telemetry only.** The `--fake-metrics` generators produce
  controlled ramp patterns, not measurements from real GPU inference
  workloads.
- **Two-pool topology.** The demo uses two clusters with `local`
  backend kinds. Each cluster's own provider scores with full locality
  (1.0) while the remote peer scores at 0.5 — so the local provider
  is preferred when metrics are otherwise equal.
- **No cost signal.** The `cost` signal defaults to 0.5 because the
  demo does not configure per-provider cost annotations.
- **Missing telemetry scores neutrally.** When a provider's metrics
  scrape fails or returns no data, all signals default to 0.5. Under
  `scoreFirst`, an unobservable provider can outrank one with known
  high pressure because neutral scores (0.5) exceed saturated scores
  (near 0.0). The `fresh` flag is based on provider phase, not scrape
  success — a provider can be `fresh=true` with zero metric data.
- **No metrics sample age between sites.** Remote providers advertise
  metrics via CRDT but the snapshot carries no timestamp. The scoring
  engine cannot distinguish a 1-second-old reading from a
  5-minute-old one. Stale remote metrics may misrepresent actual
  capacity.
- **No hysteresis or minimum switch margin.** A 0.01-point score
  difference triggers a rank change. In production, metric jitter or
  scrape timing can cause rapid oscillation between providers. A
  configurable dwell time or score margin is needed before this is
  suitable for production traffic.
