# Grid

Grid is the Kubernetes control plane for multi-site AI routing with
[Praxis](https://github.com/praxis-proxy/praxis) as the request data plane.

## What Grid does

- Reconciles `GridNetwork`, `GridSite`, and provider CRDs.
- Forms site membership with SWIM and propagates provider state with CRDTs.
- Manages Grid trust material for mTLS between sites.
- Scrapes configured provider metrics and scores routing candidates.
- Renders Praxis routing overlay `ConfigMap`s consumed by gateway deployments.

## What Grid does not do

Grid does not proxy model traffic, translate provider APIs, or run Praxis HTTP
filters. Praxis handles request routing, filter execution, mTLS termination,
credential handling, and backend proxying.

## Getting started

```sh
# Validate operator routing overlay generation in kind
cargo xtask env validate-operator-routing -c tests/env/operator-routing.toml

# Validate two-provider llm-d-style model routing in kind
cargo xtask env verify-demo1-llmd-routing -c tests/env/operator-routing-two-provider.toml

# Validate full-grid routing across local, remote, cloud mock, and API mock
cargo xtask env verify-full-grid-routing -c tests/env/operator-routing-two-provider.toml

# Validate API-provider fallback and Secret-backed credential projection
cargo xtask env verify-api-fallback -c tests/env/operator-routing.toml

# Validate SWIM membership from env-var startup seeds
cargo xtask env verify-swim-membership -c tests/env/operator-routing.toml

# Validate SWIM membership from GridNetwork.spec.seeds
cargo xtask env verify-swim-crd-seeds -c tests/env/operator-routing.toml

# Validate CRDT provider-state propagation over SWIM
cargo xtask env verify-swim-state -c tests/env/operator-routing.toml
```

## Documentation

- [Documentation index](docs/README.md)
- [Architecture overview](docs/architecture/overview.md)
- [Custom resources](docs/architecture/crds.md)
- [Routing](docs/architecture/routing.md)
- [Scoring](docs/architecture/scoring.md)
- [Auth and policy](docs/architecture/auth.md)
- [Operations](docs/architecture/operations.md)

## Development

- [Development guide](docs/development.md)
- [Conventions](docs/conventions.md)
