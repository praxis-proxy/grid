# grid-llmd-pool-metrics — Internal E2E Topology

Internal test fixture for the Grid llm-d pool-metrics E2E scenario.

## xtask command

```console
cargo xtask env run-grid-llmd-pool-metrics-demo \
  --forge-config tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml \
  --quick --teardown
```

The default configuration pulls
`ghcr.io/praxis-proxy/grid-ai-rollup:v0.1.3`, which contains the provider-side
filters used by this topology. For local development, set
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
- A-to-B-to-A capacity failover under simulated pressure ramp, with either
  the queue-depth (default) or kv-cache-pressure (`--kv-cache`) scoring
  strategy
- mTLS metrics scraping through the nginx TLS proxy
- Provider boundary and credential isolation

## Public quickstarts

User-facing Grid demos with full documentation are maintained in the
[Praxis demos repository](https://github.com/praxis-proxy/demos).
