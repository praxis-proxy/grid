# grid-combined-site — Internal E2E Topology

Internal test fixture for the Grid combined-site E2E scenario.

## xtask command

```console
cargo xtask env run-grid-combined-site-demo \
  --forge-config tests/e2e/topologies/grid-combined-site/forge.yaml \
  --quick --teardown
```

The default configuration pulls
`ghcr.io/praxis-proxy/grid-ai-rollup:v0.1.3`, which contains the provider-side
filters used by this topology. For local development, set
`GRID_XTASK_GATEWAY_IMAGE` to an AI image containing
[`provider_route`](https://github.com/praxis-proxy/ai/pull/386) and set
`GRID_XTASK_IMAGE_PULL_POLICY=Never` explicitly.

## What this tests

- Three-cluster combined topology (consumer and provider roles colocated)
- SWIM auto-discovery across all sites
- Local-preference routing (each site prefers its own provider)
- Dynamic provider lifecycle (secondary provider add/remove)
- Provider boundary with organization-only trust
- External provider integration (optional)

## Public quickstarts

User-facing Grid demos with full documentation are maintained in the
[Praxis demos repository](https://github.com/praxis-proxy/demos).
