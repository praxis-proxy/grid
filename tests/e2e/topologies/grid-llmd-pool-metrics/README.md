# grid-llmd-pool-metrics — Deterministic llm-d pool-metrics qualification

Internal test topology for the AGN llm-d pool-metrics E2E scenario.

## xtask command

```console
cargo xtask env run-grid-llmd-pool-metrics-demo \
  --forge-config tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml \
  --quick --teardown
```

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
`ghcr.io/praxis-proxy/ai:0.4.0`, which contains the provider-side filters used
by this topology. For local development, set
`GRID_XTASK_GATEWAY_IMAGE` to an AI image containing
[`provider_route`](https://github.com/praxis-proxy/ai/pull/386) and set
`GRID_XTASK_IMAGE_PULL_POLICY=Never` explicitly.

### Experimental poll-mode pressure weighting

The score-based command above exercises the legacy metrics-to-score path. It
does not qualify weighted traffic distribution. The separate experimental
command below materializes `signalTransport.mode: poll` together with the
explicit `pressureWeighted` placement policy, then checks the full
baseline-to-pressure-to-recovery path through EPP metrics, local and polled
signals, published overlay weights, Praxis accepted/serving revisions, and
provider-attributed requests. Poll mode alone does not enable pressure-based
weights.

Each phase samples 1,600 new unbound requests. The qualification uses a
predeclared 99% Pearson chi-square threshold of 6.635 for the two-provider
distribution, requires at least a 12 percentage-point measured shift from
baseline under pressure, and requires pool A to recover to at least 45% of
requests. The sample size gives the recovery floor roughly a one-sided 99%
normal-approximation margin when the published recovery share is 48.1%; none
of the acceptance thresholds were relaxed. HTTP and attribution failures are
recorded, not retried.

The dynamic qualification also binds 32 affinity sessions before the pressure
transition and replays them after the overlay weight change; every session must
stay with its original eligible provider. It restarts both run-owned operators
while pressure remains asserted and requires polling, weights, and served
revisions to reconverge. Finally, it pauses those operators, injects malformed
overlay JSON into the run-owned ConfigMap, and verifies overlay-sync rejects the
update, the gateway snapshot and serving revision remain unchanged, and a new
request still succeeds. The original ConfigMap and operator replicas are restored
before recovery begins.

```bash
export GRID_XTASK_GATEWAY_IMAGE='praxis-ai:dynamic-weighted-<run-id>'
export GRID_XTASK_GATEWAY_REVISION='<full-ai-worktree-sha>'
export GRID_XTASK_GATEWAY_CONTENT_SHA256='<full-ai-worktree-diff-sha256>'
export GRID_XTASK_IMAGE_PULL_POLICY=Never
export GRID_XTASK_SIM_IMAGE=ghcr.io/llm-d/llm-d-inference-sim:v0.10.2
cargo xtask env run-grid-dynamic-weighted-qualification \
  --forge-config tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml \
  --full --teardown \
  --evidence-dir tests/e2e/topologies/grid-llmd-pool-metrics/evidence/dynamic-run-1
```

For this source-built qualification, the gateway image must carry the supplied
SHA in `org.opencontainers.image.revision`. Before teardown, the xtask compares
that revision, the first eight hexadecimal characters of the worktree-diff
SHA-256 (an explicit prefix check) against the image version suffix, and the
Docker-save OCI config digest with every running Praxis gateway container's
requested image and runtime image ID. It records generated resolved Forge files
separately from the source-content hash; their contents are hashed into
`generated-artifacts.json` and are not treated as source.

This is a distinct experimental qualification; the existing score-based and
static-weighted qualifications remain separate compatibility checks.

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

## Public quickstarts

User-facing demos with full documentation are maintained in the
[Praxis demos repository](https://github.com/praxis-proxy/demos).
