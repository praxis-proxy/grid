# Grid Documentation

## Architecture

- [Overview](architecture/overview.md) — operator responsibilities, data-plane
  boundaries, workspace crates, and terminology.
- [Custom Resource Definitions](architecture/crds.md) — `GridNetwork`,
  `GridSite`, `InferenceProvider`, and provider status shape.
- [Routing](architecture/routing.md) — versioned overlay contract, revision
  lifecycle, candidate ordering, `intelligent_route`, `peer_identity_trust`, and
  provider-side request forwarding.
- [Provider Selection and Load Balancing](architecture/provider-selection-and-load-balancing.md) —
  eligibility, routing groups, scoring, selection modes, affinity, and
  overlay lifecycle.
- [Scoring](architecture/scoring.md) — operator-side candidate scoring,
  metrics input, and request-time scoring boundaries.
- [Auth and Policy](architecture/auth.md) — provider authentication strategies,
  access policy, and trust model.
- [Consumer Config](architecture/consumer-config.md) — operator-generated
  consumer Praxis `ConfigMap` and the `GatewayRef.consumerConfig` API.
- [External Client Ingress](architecture/external-ingress.md) — GTM/GLB edge
  selection, Grid provider routing, trust boundaries, affinity, snapshot
  delivery, and provider-boundary ownership.

## Operations

- [Adding an Inference Provider](adding-provider.md) — step-by-step
  workflow for in-cluster, existing-service, and external HTTPS providers.
- [Operations](architecture/operations.md) — local environment setup,
  validation commands, and operator workflows.
- [CI Kind E2E](architecture/ci-kind-e2e.md) — validation tiers, gate sequence,
  sequencing requirements, and environment dependencies.

## Demos

- [Grid QuickStarts](https://github.com/praxis-proxy/demos) — deployable
  demonstrations with automated runtime proofs of routing, failover, security
  boundaries, and provider lifecycle.

## Installation

- [Existing-Cluster Helm Installation](installation/existing-clusters.md) —
  install Grid and Praxis on running Kubernetes clusters with Helm.

## Development

- [Development](development.md) — build, test, format, lint, and coverage.
- [Conventions](conventions.md) — coding style, testing requirements,
  documentation rules, and commit attribution.
- [Developing: Conventions](developing/conventions.md) — shared Praxis coding,
  tracing, testing, and review conventions.
- [Developing: Type Design](developing/type-design.md) — shared Praxis guidance
  for serde, enums, newtypes, and representable states.
