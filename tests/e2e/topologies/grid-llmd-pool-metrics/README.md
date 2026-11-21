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

The saturation proof also sends three bounded samples of 1,024 ordinary
requests through the local `grid_site_route` Gateway: an idle baseline, pool-B
with synthetic simulator-reported KV utilization of `0.9` and no waiting
requests, and an idle recovery. The pressured batch requires fresh Gateway
`rho >= 1`, `full = 0`, successful provider/site/backend attribution, and the
two-site weighted selector path for every request. Each sample runs in batches
of 32, and route metrics must remain stable across the request window. The
observed pool-B share must match the equal-capacity two-site selector formula,
where each site's contribution is proportional to `1 / (1 + rho)`, within five
percentage points. Baseline and recovery must remain within
five points of the equal-share expectation; pressure must lower pool-B share by
at least ten points while keeping it above ten percent. At 1,024 requests, the
five-point share tolerance is about 3.2 binomial standard errors at an equal
share, while the ten-point reduction exceeds four standard errors for two
independent equal-share samples. The selector's mixed-turn sequence is
deterministic, so these standard errors are a scale reference rather than a
formal confidence interval; each sample also checks the signal-derived share
formula and matching metric window. The real-selector closed-loop simulation
remains the quantitative acceptance test. The fake KV metric is a synthetic
simulator stimulus, not naturally accumulated GPU saturation or a throughput
benchmark.

The same full-mode proof separately qualifies a site losing half its effective
capacity while aggregate synthetic work stays fixed. Pool-B starts with two
ready simulator endpoints; the run removes `vcr-2`, verifies its ready-unit
weight changes from 2 to 1, and then restores it. Pool-A stays at two units and
keeps `waiting=0`, `running=2`, and KV `0.45` per endpoint. Pool-B carries the
same aggregate running work and KV use before and during the loss: each of two
endpoints reports `running=2` and KV `0.45`, then its one remaining endpoint
reports `running=4` and KV `0.9`. Waiting and held requests stay zero. The
Gateway must report `full=0` throughout; pool-B's non-full `rho` rises above 1
under the synthetic KV stimulus, where the soft preference still applies.
Each phase sends exactly 1,024 requests. The observed pool-B share must match
the live capacity-and-pressure formula, which predicts about
`0.50 -> 0.269 -> 0.50` from the measured weights and rho values. The run
requires both sites to receive traffic in every sample and verifies
per-response provider-marker/backend matching. This is a controlled two-site
ready-unit loss, not a throughput or real hardware-capacity benchmark.

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
ConfigMap. During qualification, the runner changes the queue, running, and
KV-cache scalars through the simulator's `GET` and `POST /admin/config` runtime
API. It verifies the returned configuration, waits for the corresponding EPP
metrics, and checks that Pod UIDs and restart counts stay unchanged across each
update. The original values are saved at the first mutation and restored at the
end of the full proof. Replica scaling in the separate half-capacity case is an
explicit capacity change, not a metric-update mechanism.
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
