# grid-llmd-pool-metrics — Deterministic llm-d pool-metrics qualification

Internal test fixture for the Grid llm-d pool-metrics E2E scenario.

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

Grid still performs real EPP metric scraping, score/rank computation, overlay
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

User-facing Grid demos with full documentation are maintained in the
[Praxis demos repository](https://github.com/praxis-proxy/demos).
