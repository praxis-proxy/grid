# Grid Documentation

## Architecture

- [Overview](architecture/overview.md) — operator responsibilities, data-plane
  boundaries, workspace crates, and terminology.
- [Custom Resource Definitions](architecture/crds.md) — `GridNetwork`,
  `GridSite`, `InferenceProvider`, and provider status shape.
- [Routing](architecture/routing.md) — overlay rendering, candidate ordering,
  `grid_route`, `grid_ingress_trust`, and provider-side handoff.
- [Scoring](architecture/scoring.md) — operator-side candidate scoring,
  metrics input, and request-time scoring boundaries.
- [Auth and Policy](architecture/auth.md) — provider authentication strategies,
  access policy, and trust model.

## Operations

- [Operations](architecture/operations.md) — local environment setup,
  validation commands, and operator workflows.

## Development

- [Development](development.md) — build, test, format, lint, and coverage.
- [Conventions](conventions.md) — coding style, testing requirements,
  documentation rules, and commit attribution.
