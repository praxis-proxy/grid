# grid-llmd-pool-metrics: Deterministic llm-d pool-metrics qualification

Internal test topology for the AGN llm-d pool-metrics E2E scenario.

## xtask command

```console
cargo xtask env run-grid-llmd-pool-metrics-demo \
  --forge-config tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml \
  --quick --teardown
```

Quick mode proves the local EPP scrape, score changes, overlay propagation,
and routing recovery. Full mode also deploys the source-built Grid Gateway and
proves that each site's EPP signals cross the operator mTLS poll relay, reach
the Gateway through its local operator, and change live site selection without
a Gateway restart. It also checks that a stale local signal is excluded while
SWIM remains connected and that a peer with the wrong pinned identity cannot
replace an accepted signal.

Build the Grid operator and Gateway from this checkout before a full local run:

```console
SOURCE_TAG=$(git rev-parse --short HEAD)
docker build -f deploy/operator/Containerfile \
  -t "grid-operator:llmd-source-${SOURCE_TAG}" .
docker build -f deploy/gateway/Containerfile \
  -t "grid-gateway:llmd-source-${SOURCE_TAG}" .
export GRID_XTASK_OPERATOR_IMAGE="grid-operator:llmd-source-${SOURCE_TAG}"
export GRID_XTASK_GRID_GATEWAY_IMAGE="grid-gateway:llmd-source-${SOURCE_TAG}"
export GRID_XTASK_IMAGE_PULL_POLICY=Never
cargo xtask env run-grid-llmd-pool-metrics-demo \
  --forge-config tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml \
  --full --teardown --evidence-dir "$EVIDENCE_DIR"
```

Full mode requires source-built Grid images because it exercises the operator's
signal relay and uses the `gridServing` chart mode with the `grid_site_route`
filter. The run resolves a unique Kind cluster prefix and a Forge state
directory under `EVIDENCE_DIR`; do not share Forge state between simultaneous
runs.

Full mode also runs a site saturation proof after the cross-site proof. It
requires each llm-d site's Gateway weight to equal its EPP's ready endpoints and
to follow a pool-B replica change. Pool-B's EPP runs flow control, so the
Gateway must read pool-B by the EPP's published pool saturation; pool-A's does
not, so the Gateway must read pool-A by its own comparator, and that value must
match the one computed from pool-A's EPP gauges. A spent cache with nothing
waiting must read above 1 yet stay not full and unshed; a queue past depth 5
must make both sites full and recover when it drains. Neither site may publish
a learned ceiling. The proof restores simulator load and replica counts on
success and failure.

For pressure-driven routing, the full-mode cross-site proof checks the live
Gateway queue-score order and compares attributed responses with its measured
two-site capacity-weighted share. This preserves the routing assertion after
Grid's selector began weighting capacity and smoothed saturation.

The cross-site proof uses two waiting requests per simulator endpoint against
the topology's queue capacity of four. That changes the EPP queue score while
leaving both providers eligible, so the proof exercises the weighted two-site
selector rather than the single-eligible-provider fallback. For this proof only,
the embedded test Gateway sets `availability.queue_full` to three: the queue of
two stays below the inclusive backlog threshold and below EPP capacity. The
normal threshold of one is restored afterwards.

In the cross-site proof, both scoring flavors report KV-cache usage of `0.6`
for the pressured site's endpoints. EPP queue depth remains the Grid score/rank
stimulus; the KV reading gives the Gateway's capacity-weighted selector a
measurable `rho` of `0.75`, below the `0.8` share at which a pool reads full, so
both sites remain eligible and the attributed request sample still has to match
the Gateway's observed weighted share.

The qualification uses upstream `llm-d-inference-sim` as the inference
backend. Each simulator's startup `fake-metrics` values are mounted from its own
ConfigMap. The runner updates those persistent configurations and normally
rolls the simulator pods, so a restart cannot lose the requested values.
The local scoring proofs use the deterministic state sequence `0 -> 9 -> 0`
for queue depth and `0.0 -> 0.95 -> 0.0` for KV-cache pressure. Pool-B's pinned
EPP enables its `flowControl` feature gate, so it publishes its own pool
saturation; pool-A's does not, so the Gateway derives pool-A's.

The permanent **Triggered Integration Test** workflow offers this topology as
`llmd-pool-metrics-quick` and `llmd-pool-metrics-full`. Full mode builds the
Grid Gateway from the tested source revision; the Praxis AI gateway, simulator,
and EPP images are pinned and recorded in the run artifact. The workflow's
scheduled quick run calls the same reusable runner as manual dispatch; full
mode remains manual-only.

AGN still performs real EPP metric scraping, score/rank computation, overlay
publication, overlay-sync projection, Praxis configuration loading, and
request routing. It does not generate pressure by sending request floods; the
separate real EPP/VCR smoke coverage remains useful for availability and
provider-boundary checks.

The default gateway image is
`ghcr.io/praxis-proxy/ai:0.7.0`, which contains the provider-side filters used
by this topology. For local development, set
`GRID_XTASK_GATEWAY_IMAGE` to an AI image containing
[`provider_route`](https://github.com/praxis-proxy/ai/pull/386) and set
`GRID_XTASK_IMAGE_PULL_POLICY=Never` explicitly.

### Flags

- `--metrics-mtls`: protect EPP metrics scraping with an nginx mTLS proxy
  instead of scraping directly over HTTP.
- `--kv-cache`: drive routing off llm-d's kv-cache-utilization signal
  (`GridNetwork.spec.scoringPolicy.strategy: kvCachePressure`) instead of the
  default queue-depth signal (`strategy: queueDepth`). Both signals are
  always shown in the live scorecard; this flag only changes which one
  actually produces the `score`/`rank` that drives the A→B failover.

## What this tests

- Two-cluster llm-d pool topology with EPP telemetry
- Score-first routing based on live queue-depth and KV-cache utilization
- A-to-B-to-A capacity failover from deterministic simulator metrics, using
  queue depth by default or KV-cache pressure with `--kv-cache`
- mTLS metrics scraping through the nginx TLS proxy
- Provider boundary and credential isolation
- Full-mode local EPP publication, operator-to-operator mTLS polling, and
  Gateway polling of its local operator
- Live cross-site pressure selection, stale-signal handling, recovery, and
  wrong-peer-identity rejection; when a provider's EPP signal disappears but
  its serving pair remains published, all eight measured Gateway gauges must
  clear, including `full` and `reading_age_seconds`
- Full-mode network partition with both sites' operators and the Gateway
  running, traffic probes during loss, stale remote-route exclusion, and
  reconvergence after the run-owned network link is restored

## Coverage boundaries

The full-mode proof separately interrupts signal polling with a wrong peer
certificate pin while SWIM remains connected, then partitions only the
run-owned Docker network link while both operators and the Gateway keep
running. The partition proof sends requests during the interruption and
requires stale remote-route exclusion before restoring the original network
ID, node addresses, and aliases. The standalone
`verify-failover-under-lost-peer` operator check still stops an operator
process and is not a substitute for this deployed boundary.

Geography-first routing after peer auto-discovery is also not proved here.
Auto-discovered `GridSite` resources currently receive identity and egress
fields from SWIM but do not receive region or zone metadata. A deployed
geography-tier proof needs that metadata propagation; the test must not patch
the discovered resources to imitate it.

## Public quickstarts

User-facing demos with full documentation are maintained in the
[Praxis demos repository](https://github.com/praxis-proxy/demos).
