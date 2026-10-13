# grid-llmd-pool-metrics — Deterministic llm-d pool-metrics qualification

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

Build the Grid Gateway from this checkout before a full local run:

```console
docker build -f deploy/gateway/Containerfile \
  -t grid-gateway:llmd-pool-metrics-demo .
export GRID_XTASK_GRID_GATEWAY_IMAGE=grid-gateway:llmd-pool-metrics-demo
export GRID_XTASK_IMAGE_PULL_POLICY=Never
cargo xtask env run-grid-llmd-pool-metrics-demo \
  --forge-config tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml \
  --full --teardown --evidence-dir "$EVIDENCE_DIR"
```

Full mode requires the source-built image because it uses the `gridServing`
chart mode and the `grid_site_route` filter. The run resolves a unique Kind
cluster prefix and a Forge state directory under `EVIDENCE_DIR`; do not share
Forge state between simultaneous runs.

The qualification uses upstream `llm-d-inference-sim` as the inference
backend. Its startup `fake-metrics.waiting-requests` value is mounted from a
ConfigMap. The runner updates that persistent configuration and rolls the
simulator pods, so a restart cannot lose the requested value. The deterministic
state sequence is `0 -> 9 -> 0` for queue depth and `0.0 -> 0.95 -> 0.0`
for KV-cache pressure.

AGN still performs real EPP metric scraping, score/rank computation, overlay
publication, overlay-sync projection, Praxis configuration loading, and
request routing. It does not generate pressure by sending request floods; the
separate real EPP/VCR smoke coverage remains useful for availability and
provider-boundary checks.

The default gateway image is
`ghcr.io/praxis-proxy/ai:0.6.0`, which contains the provider-side filters used
by this topology. For local development, set
`GRID_XTASK_GATEWAY_IMAGE` to an AI image containing
[`provider_route`](https://github.com/praxis-proxy/ai/pull/386) and set
`GRID_XTASK_IMAGE_PULL_POLICY=Never` explicitly.

### Flags

- `--metrics-mtls` — protect EPP metrics scraping with an nginx mTLS proxy
  instead of scraping directly over HTTP.
- `--kv-cache` — drive routing off llm-d's kv-cache-utilization signal
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
  wrong-peer-identity rejection
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
