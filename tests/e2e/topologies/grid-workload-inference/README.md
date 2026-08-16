# grid-workload-inference — Internal E2E Topology

Internal test fixture for the Grid workload-inference E2E scenario.

This topology reuses the `grid-glb-demo` Forge configuration with the
`--no-ingress` flag, which strips the GTM emulator cluster and produces
a four-cluster topology for cluster-local workload entry.

## xtask command

```console
cargo xtask env run-grid-glb-demo \
  --forge-config tests/e2e/topologies/grid-glb-demo/forge.yaml \
  --no-ingress --quick --teardown
```

The reused GLB configuration defaults to
`ghcr.io/praxis-proxy/grid-ai-rollup:v0.1.3`. For local development, set
`GRID_XTASK_GATEWAY_IMAGE` to an AI image containing
[`provider_route`](https://github.com/praxis-proxy/ai/pull/386) and set
`GRID_XTASK_IMAGE_PULL_POLICY=Never` explicitly.

## What this tests

- Four-cluster workload-inference topology (no GTM emulator)
- In-cluster Job-based workload routing
- Provider selection without public ingress

## Public quickstarts

User-facing Grid demos with full documentation are maintained in the
[Praxis demos repository](https://github.com/praxis-proxy/demos).
