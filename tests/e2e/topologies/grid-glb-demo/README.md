# grid-glb-demo — Internal E2E Topology

Internal test fixture for the Grid global-ingress E2E scenario.

## xtask command

```console
cargo xtask env run-grid-glb-demo \
  --forge-config tests/e2e/topologies/grid-glb-demo/forge.yaml \
  --quick --teardown
```

The default configuration pulls
`ghcr.io/praxis-proxy/grid-ai-rollup:v0.1.3`, which contains the
`intelligent_route`, `provider_route`, and `credential_inject` filters used by
this topology. A local AI build must be selected explicitly:

```console
GRID_XTASK_GATEWAY_IMAGE=praxis-ai:dev \
GRID_XTASK_OPERATOR_IMAGE=grid-operator:dev \
GRID_XTASK_IMAGE_PULL_POLICY=Never \
cargo xtask env run-grid-glb-demo \
  --forge-config tests/e2e/topologies/grid-glb-demo/forge.yaml \
  --quick --teardown
```

The local gateway image must be built from a `praxis-proxy/ai` revision that
contains [`provider_route`](https://github.com/praxis-proxy/ai/pull/386).

## What this tests

- Five-cluster global-ingress topology (2 edges, 2 providers, 1 GTM emulator)
- SWIM discovery and overlay convergence
- Active/active routing through the local GTM emulator
- Secure provider boundary (mTLS, peer identity, credential replacement)
- Edge withdrawal and recovery
- Hot-reload failure safety

## Public quickstarts

User-facing Grid demos with full documentation are maintained in the
[Praxis demos repository](https://github.com/praxis-proxy/demos).
