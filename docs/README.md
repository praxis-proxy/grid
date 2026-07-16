# Grid Documentation

## Architecture

- [Overview](architecture/overview.md) — operator responsibilities, data-plane
  boundaries, workspace crates, and terminology.
- [Custom Resource Definitions](architecture/crds.md) — `GridNetwork`,
  `GridSite`, `InferenceProvider`, and provider status shape.
- [Routing](architecture/routing.md) — overlay rendering, candidate ordering,
  `grid_route`, `peer_identity_trust`, and provider-side request forwarding.
- [Scoring](architecture/scoring.md) — operator-side candidate scoring,
  metrics input, and request-time scoring boundaries.
- [Auth and Policy](architecture/auth.md) — provider authentication strategies,
  access policy, and trust model.
- [Consumer Config](architecture/consumer-config.md) — design for
  operator-owned consumer gateway configuration and Secret lifecycle.

## Operations

- [Operations](architecture/operations.md) — local environment setup,
  validation commands, and operator workflows.
- [CI Kind E2E Strategy](architecture/ci-kind-e2e.md) — CI stage definitions,
  sequencing requirements, and gating recommendations.

## Development

- [Development](development.md) — build, test, format, lint, and coverage.
- [Conventions](conventions.md) — coding style, testing requirements,
  documentation rules, and commit attribution.
