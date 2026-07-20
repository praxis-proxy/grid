# Grid

Grid is the Kubernetes control plane for multi-site AI routing with
[Praxis](https://github.com/praxis-proxy/praxis) as the request data plane.

## What Grid does

- Reconciles `GridNetwork`, `GridSite`, and provider CRDs.
- Forms site membership with SWIM and propagates provider state with CRDTs.
- Manages Grid trust material for mTLS between sites.
- Scrapes configured provider metrics and scores routing candidates.
- Renders Praxis routing overlay `ConfigMap`s consumed by gateway deployments.
- Projects provider credential references into overlays without writing token
  values into Grid routing data.

## What Grid does not do

Grid does not proxy model traffic, translate provider APIs, or run Praxis HTTP
filters. The Praxis gateway stack handles TLS, proxying, and backend I/O;
Praxis AI supplies the AI-specific routing and credential filters.

## Getting started

Some Kind validations require Praxis AI and Praxis Core features that are still
pending merge or project-owned image publication. Until those images are
available, use the documented xtask image override environment variables for
local development runs.

```sh
# Validate operator routing overlay generation in kind
cargo xtask env validate-operator-routing -c tests/env/operator-routing.toml

# Validate generated CRD schema contains required fields
cargo xtask env verify-crd-schema

# Validate in-cluster operator install/RBAC behavior
cargo xtask env verify-operator-install-rbac -c tests/env/operator-routing-multisite.toml

# Validate the dedicated llm-d-compatible provider-gateway path
# Uses Praxis AI ext_proc with mock EPP test image
cargo xtask env verify-llmd-compatible-routing -c tests/env/operator-routing-multisite.toml

# Validate /v1/responses request parsing and Grid overlay routing
cargo xtask env verify-responses-routing -c tests/env/operator-routing-multisite.toml

# Validate full-grid routing across local, remote, cloud mock, and API mock
cargo xtask env verify-full-grid-routing -c tests/env/operator-routing-two-provider.toml

# Validate API-provider fallback with static header injection
cargo xtask env verify-api-fallback -c tests/env/operator-routing.toml

# Validate native grid_route → grid_credential_inject credential injection.
# Tokens are read from a mounted Secret file and stay out of Praxis ConfigMaps.
cargo xtask env verify-api-fallback-native -c tests/env/operator-routing.toml

# Validate SWIM membership from env-var startup seeds
cargo xtask env verify-swim-membership -c tests/env/operator-routing.toml

# Validate SWIM membership from GridNetwork.spec.seeds
cargo xtask env verify-swim-crd-seeds -c tests/env/operator-routing.toml

# Validate CRDT provider-state propagation over SWIM
cargo xtask env verify-swim-state -c tests/env/operator-routing.toml

# Validate CRDT-origin overlay rendering over SWIM
cargo xtask env verify-swim-overlay -c tests/env/operator-routing-multisite.toml

# Validate CRDT-origin routing over SWIM
cargo xtask env verify-swim-routing -c tests/env/operator-routing-two-provider.toml

# Validate encrypted SWIM transport behavior and failure cases
cargo xtask env verify-swim-encryption -c tests/env/operator-routing-multisite.toml

# Validate transitive three-node SWIM mesh propagation and routing eligibility
cargo xtask env verify-swim-mesh-three-node -c tests/env/operator-routing-multisite.toml

# Validate GridSite trust fingerprint promotion and fail-closed rotation
cargo xtask env verify-gridsite-trust-fingerprint -c tests/env/operator-routing-multisite.toml

# Validate metrics-driven candidate ordering and routing
cargo xtask env verify-metrics-routing -c tests/env/operator-routing-two-provider.toml

# Validate operator-created GridSite discovery and join lifecycle
cargo xtask env verify-site-join-discovery -c tests/env/operator-routing-multisite.toml

# Validate route-away behavior when a SWIM peer is lost
cargo xtask env verify-failover-under-lost-peer -c tests/env/operator-routing-two-provider.toml

# Validate stale remote candidate eviction from rendered overlays
cargo xtask env verify-stale-gc-ttl -c tests/env/operator-routing-two-provider.toml
```

## Documentation

- [Documentation index](docs/README.md)
- [Architecture overview](docs/architecture/overview.md)
- [Custom resources](docs/architecture/crds.md)
- [Routing](docs/architecture/routing.md)
- [Scoring](docs/architecture/scoring.md)
- [Auth and policy](docs/architecture/auth.md)
- [Operations](docs/architecture/operations.md)
- [Consumer config](docs/architecture/consumer-config.md)
- [CI Kind E2E strategy](docs/architecture/ci-kind-e2e.md)

## Development

- [Development guide](docs/development.md)
- [Conventions](docs/conventions.md)
