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

Full mode also runs a separate learned-ceiling lifecycle proof after the
cross-site signal, stale-signal, wrong-pin, and partition proofs have restored
their state. It teaches a high `(pool-b, llmd-pool-b-provider)` ceiling from
running-request metrics only while EPP and operator evidence reports zero
waiting and held work. It then proves the ceiling persists while idle, removes
that exact pair from the operator-published and Gateway-mounted serving
topology and verifies the Gateway accepted the removal, then restores the same
pair under lower load and requires a newly learned ceiling below the old one.
The proof records the serving revisions and mounted JSON, EPP and relayed
operator signals, live Gateway metrics and pod UID, actual image identities,
and attributed responses. It restores the provider policy, simulator replica
counts and settings, and run-owned reconcile annotations on both success and
failure.

For pressure-driven routing, the full-mode cross-site proof checks the live
Gateway queue-score order and compares attributed responses with its measured
two-site capacity-weighted share. This preserves the routing assertion after
Grid's selector began weighting capacity and smoothed saturation.

It also raises the returned pair's queue-free soft ceiling and records a
2,048-request low-KV routing baseline. It lowers pool-B running load to two
requests per endpoint, keeps waiting and held work at zero, and sets endpoint
KV-cache values to `1.0` and `0.0`. The pinned EPP must publish
their mean (`0.5`) and sample standard deviation (`0.71`). The deployed operator
signal path must deliver those values to the Gateway, which must report the
resulting saturation (`1.0`) and combined route `rho` near full while its hard
ceiling and counted-saturation gauge remain absent. The effective ceiling must
remain the soft value. A matched 2,048-request sample must reduce pool-B's traffic share by
at least eight percentage points, proving that provider KV saturation affects
routing without queue pressure or a count-derived hard bound.

The proof then configures the pinned simulator for eight running requests and
one waiting request per endpoint, with zero flow-control-held work and the same
asymmetric KV-cache values. In-flight counts must match the simulator stimulus:
18 across two ready endpoints. A hard ceiling at that saturated count must fall
below the queue-free soft ceiling, and the effective ceiling and
counted-saturation gauges must use the new hard bound; retaining the old soft
ceiling as the effective bound fails. Pool B must receive at least 75% of the
soft-baseline traffic, then at most 65% under the hard bound, with a reduction
of at least 15 percentage points. This checks a real routing shift while
allowing Grid's weighted selection to continue sending some traffic to a site
whose smoothed `rho` is still below 1; it does not require every individual
request to move to pool A.

While the KV-cache saturation and one-request-per-endpoint backlog remain
active, full mode scales down `vcr-2`, the zero-cache endpoint, leaving the
high-cache `vcr-1` endpoint. It requires the pinned EPP readiness count,
mean/spread, and both operator signal views to reflect the smaller pool, then
waits through the Gateway load window. The Gateway must clear the old
queue-free soft ceiling and relearn the hard/effective ceiling as 9 for the
reduced endpoint count. The runner restores the original simulator replica
counts, both endpoint ConfigMaps, and pod-template annotations on success and
failure.

Full mode then removes the exact provider pair again, confirms the Gateway has
cleared both learned bounds, and publishes a full KV-cache signal with eight
running requests, one waiting request, and no flow-control-held work while the
pair is absent. Re-adding the pair under that sustained signal must teach a
finite hard/effective ceiling while the soft ceiling remains absent. This
covers the hard-bound path for a site that has never supplied a queue-free
capacity sample. The separate low-count KV-routing phase remains queue-free,
so the test distinguishes provider-reported saturation from the backlog needed
to learn a hard request-count bound.

The qualification uses upstream `llm-d-inference-sim` as the inference
backend. Each simulator's startup `fake-metrics` values are mounted from its own
ConfigMap. The runner updates those persistent configurations and rolls the
simulator pods, so
a restart cannot lose the requested values. Existing quick and full proofs use
the deterministic state sequence `0 -> 9 -> 0` for queue depth and
`0.0 -> 0.95 -> 0.0` for KV-cache pressure. The new full-mode lifecycle proof
also changes `running-requests` while keeping waiting requests and KV-cache
pressure at zero. Pool-B's pinned EPP enables its `flowControl` feature gate so
the proof can require an explicit zero held-capacity observation; a missing
held-work metric fails closed.

The permanent **Triggered Integration Test** workflow offers this topology as
`llmd-pool-metrics` in both quick and full modes. Full mode builds the Grid
Gateway from the tested source revision; the Praxis AI gateway, simulator, and
EPP images are pinned and recorded in the run artifact.

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
- Full-mode learned-ceiling persistence while idle, exact published-pair
  removal and Gateway acceptance, and lower relearning after that same pair
  returns
- Full-mode provider-reported KV-cache saturation teaching a hard ceiling below
  a previously measured queue-free soft ceiling, including EPP, relay, Gateway
  metric, and attributed-response evidence
- Full-mode reduction of one simulator endpoint while KV pressure remains live,
  proving the old soft ceiling retires and the hard/effective ceiling is measured
  against the reduced ready-endpoint count

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
